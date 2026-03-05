package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"tailscale.com/tsnet"
)

type bridge struct {
	ts          *tsnet.Server
	targetAddr  string
	targetURL   *url.URL
	activeConns atomic.Int64
	log         *slog.Logger
}

const maxBodySize = 100 * 1024 * 1024 // 100MB

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// Config from env
	hostname := envOr("TS_HOSTNAME", "prela-bridge")
	authKey := requireEnv(log, "TS_AUTH_KEY")
	targetAddr := requireEnv(log, "TARGET_ADDR")
	listenPort := envOr("LISTEN_PORT", "8123")
	healthPort := envOr("HEALTH_PORT", "8124")
	stateDir := envOr("TS_STATE_DIR", "/tmp/prela-bridge")

	targetURL, err := url.Parse("http://" + targetAddr)
	if err != nil {
		log.Error("invalid TARGET_ADDR", "addr", targetAddr, "err", err)
		os.Exit(1)
	}

	ts := &tsnet.Server{
		Hostname:  hostname,
		AuthKey:   authKey,
		Ephemeral: true,
		Dir:       stateDir,
		UserLogf: func(format string, v ...any) {
			log.Info(fmt.Sprintf(format, v...))
		},
	}

	log.Info("starting prela-bridge",
		"hostname", hostname,
		"listen", ":"+listenPort,
		"health", ":"+healthPort,
		"target", targetAddr,
		"mode", "raw-conn",
	)

	if err := ts.Start(); err != nil {
		log.Error("tailscale start failed", "err", err)
		os.Exit(1)
	}
	defer ts.Close()

	log.Info("tailscale connected")

	b := &bridge{
		ts:         ts,
		targetAddr: targetAddr,
		targetURL:  targetURL,
		log:        log,
	}

	// Main proxy server — raw connection approach.
	// Bypasses http.Transport entirely to avoid gvisor/chunked-encoding
	// interactions that cause body reads to stall on larger responses.
	// Each request: dial tsnet -> write HTTP request -> read HTTP response
	// -> buffer body -> forward to client.
	proxyServer := &http.Server{
		Addr: ":" + listenPort,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			active := b.activeConns.Add(1)
			defer b.activeConns.Add(-1)

			start := time.Now()
			log.Info("proxying request",
				"method", r.Method,
				"path", r.URL.Path,
				"query", r.URL.RawQuery,
				"contentLength", r.ContentLength,
				"remote", r.RemoteAddr,
				"active", active,
			)

			statusCode, headers, body, proxyErr := b.rawProxy(r)
			if proxyErr != nil {
				log.Error("proxy error",
					"method", r.Method,
					"path", r.URL.Path,
					"err", proxyErr,
					"duration", time.Since(start).String(),
				)
				http.Error(w, "Bad Gateway", http.StatusBadGateway)
				return
			}

			// Copy response headers
			for k, vv := range headers {
				for _, v := range vv {
					w.Header().Add(k, v)
				}
			}
			// Replace chunked encoding with explicit Content-Length
			w.Header().Del("Transfer-Encoding")
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))

			w.WriteHeader(statusCode)
			w.Write(body)

			log.Info("request completed",
				"method", r.Method,
				"path", r.URL.Path,
				"status", statusCode,
				"bodySize", len(body),
				"duration", time.Since(start).String(),
			)
		}),
		ReadTimeout:  300 * time.Second,
		WriteTimeout: 300 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Health check server on separate port
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/healthz", b.healthHandler)
	healthServer := &http.Server{
		Addr:    ":" + healthPort,
		Handler: healthMux,
	}
	go func() {
		if err := healthServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Error("health server error", "err", err)
		}
	}()

	// Graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	go func() {
		<-ctx.Done()
		log.Info("shutting down", "active", b.activeConns.Load())
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		proxyServer.Shutdown(shutdownCtx)
		healthServer.Close()
	}()

	log.Info("listening", "port", listenPort)
	if err := proxyServer.ListenAndServe(); err != http.ErrServerClosed {
		log.Error("server error", "err", err)
		os.Exit(1)
	}
}

// rawProxy bypasses http.Transport entirely. It dials a fresh tsnet connection,
// writes the HTTP request manually, reads the response with per-read deadline
// extensions, and returns the buffered result. This avoids all gvisor/http.Transport
// interactions that cause body reads to stall on chunked responses.
func (b *bridge) rawProxy(r *http.Request) (int, http.Header, []byte, error) {
	ctx, cancel := context.WithTimeout(r.Context(), 120*time.Second)
	defer cancel()

	// Dial upstream via Tailscale
	conn, err := b.ts.Dial(ctx, "tcp", b.targetAddr)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	// Build the outbound request
	outURL := *r.URL
	outURL.Scheme = b.targetURL.Scheme
	outURL.Host = b.targetURL.Host

	outReq, err := http.NewRequestWithContext(ctx, r.Method, outURL.String(), nil)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("new request: %w", err)
	}

	// Copy headers from the incoming request
	for k, vv := range r.Header {
		for _, v := range vv {
			outReq.Header.Add(k, v)
		}
	}
	outReq.Header.Set("Host", b.targetURL.Host)
	// Keep-alive so ClickHouse doesn't FIN before we read the body
	outReq.Header.Set("Connection", "keep-alive")

	// Buffer and attach request body
	if r.Body != nil {
		reqBody, err := io.ReadAll(io.LimitReader(r.Body, maxBodySize))
		if err != nil {
			return 0, nil, nil, fmt.Errorf("read request body: %w", err)
		}
		r.Body.Close()
		if len(reqBody) > 0 {
			outReq.Body = io.NopCloser(bytes.NewReader(reqBody))
			outReq.ContentLength = int64(len(reqBody))
		}
	}

	// Set initial write deadline and write the request to the raw connection
	conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
	err = outReq.Write(conn)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("write request: %w", err)
	}

	// Read response with per-read deadline extensions
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	bufReader := bufio.NewReaderSize(conn, 256*1024) // 256KB buffer
	resp, err := http.ReadResponse(bufReader, outReq)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("read response headers: %w", err)
	}
	defer resp.Body.Close()

	// Read the body with periodic deadline extensions.
	// Each successful read resets the deadline, so we only timeout
	// if no data arrives for 30 seconds.
	body, err := readWithDeadlineRefresh(conn, resp.Body, maxBodySize, 30*time.Second)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("read body (%d bytes so far): %w", len(body), err)
	}

	b.log.Info("upstream response buffered",
		"status", resp.StatusCode,
		"bodySize", len(body),
	)

	return resp.StatusCode, resp.Header, body, nil
}

// readWithDeadlineRefresh reads from r, extending conn's read deadline
// each time data arrives. This prevents stalls on gvisor connections
// while allowing large responses to complete as long as data keeps flowing.
func readWithDeadlineRefresh(conn net.Conn, r io.Reader, maxSize int64, timeout time.Duration) ([]byte, error) {
	var buf bytes.Buffer
	tmp := make([]byte, 64*1024) // 64KB read chunks
	limited := io.LimitReader(r, maxSize)
	for {
		conn.SetReadDeadline(time.Now().Add(timeout))
		n, err := limited.Read(tmp)
		if n > 0 {
			buf.Write(tmp[:n])
		}
		if err == io.EOF {
			return buf.Bytes(), nil
		}
		if err != nil {
			return buf.Bytes(), err
		}
	}
}

func (b *bridge) healthHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	resp := map[string]any{
		"connections": b.activeConns.Load(),
		"target":      b.targetAddr,
	}

	conn, err := b.ts.Dial(ctx, "tcp", b.targetAddr)
	if err != nil {
		resp["status"] = "unhealthy"
		resp["error"] = err.Error()
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		conn.Close()
		resp["status"] = "healthy"
		w.WriteHeader(http.StatusOK)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func requireEnv(log *slog.Logger, key string) string {
	v := os.Getenv(key)
	if v == "" {
		aliases := map[string]string{
			"TS_AUTH_KEY": "TS_AUTHKEY",
		}
		if alias, ok := aliases[key]; ok {
			v = os.Getenv(alias)
		}
	}
	if v == "" {
		log.Error("required env var not set", "key", key)
		os.Exit(1)
	}
	return v
}
