package main

import (
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

// deadlineConn wraps a net.Conn and refreshes the read deadline before
// every Read call. This works around gvisor userspace TCP connections
// (from tsnet.Dial) that stall when a single long-lived deadline is set.
type deadlineConn struct {
	net.Conn
	readTimeout time.Duration
}

func (c *deadlineConn) Read(p []byte) (int, error) {
	c.Conn.SetReadDeadline(time.Now().Add(c.readTimeout))
	return c.Conn.Read(p)
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
		"mode", "buffer-and-forward",
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

	// Transport using Tailscale dialer with per-read deadline refresh.
	// NOT using DisableKeepAlives — that sends "Connection: close" which
	// causes ClickHouse to FIN the connection, and gvisor's userspace TCP
	// stalls reading buffered data after receiving FIN. Instead, we use
	// aggressive idle connection cleanup to prevent stale reuse.
	tsTransport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			log.Info("dialing tailscale", "target", targetAddr)
			conn, err := ts.Dial(ctx, "tcp", targetAddr)
			if err != nil {
				return nil, err
			}
			return &deadlineConn{Conn: conn, readTimeout: 30 * time.Second}, nil
		},
		MaxIdleConns:          1,
		MaxIdleConnsPerHost:   1,
		IdleConnTimeout:       5 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
		DisableCompression:    true,
	}

	// Main proxy server — buffer-and-forward to avoid gvisor/io.Copy deadlock.
	// httputil.ReverseProxy streams response bodies via io.Copy, which deadlocks
	// when the downstream write blocks and gvisor's userspace TCP can't process
	// upstream ACKs in the same goroutine. Buffering decouples the two sides.
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

			// Build upstream request
			outReq := r.Clone(r.Context())
			outReq.URL.Scheme = targetURL.Scheme
			outReq.URL.Host = targetURL.Host
			outReq.Host = targetURL.Host
			outReq.RequestURI = "" // Required for http.Transport

			// Buffer request body before sending through tsnet
			if r.Body != nil {
				reqBody, err := io.ReadAll(io.LimitReader(r.Body, maxBodySize))
				if err != nil {
					log.Error("request body read error", "err", err)
					http.Error(w, "Bad Request", http.StatusBadRequest)
					return
				}
				r.Body.Close()
				outReq.Body = io.NopCloser(bytes.NewReader(reqBody))
				outReq.ContentLength = int64(len(reqBody))
			}

			// RoundTrip to upstream via tsnet
			resp, err := tsTransport.RoundTrip(outReq)
			if err != nil {
				log.Error("upstream error",
					"method", r.Method,
					"path", r.URL.Path,
					"err", err,
					"duration", time.Since(start).String(),
				)
				http.Error(w, "Bad Gateway", http.StatusBadGateway)
				return
			}
			defer resp.Body.Close()

			// Buffer the entire response body — decouples tsnet read from downstream write
			body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodySize))
			if err != nil {
				log.Error("upstream body read error",
					"method", r.Method,
					"path", r.URL.Path,
					"err", err,
					"duration", time.Since(start).String(),
				)
				http.Error(w, "Bad Gateway", http.StatusBadGateway)
				return
			}

			// Close idle connections to prevent stale gvisor connections from being reused
			tsTransport.CloseIdleConnections()

			log.Info("upstream response buffered",
				"status", resp.StatusCode,
				"bodySize", len(body),
				"duration", time.Since(start).String(),
			)

			// Copy response headers
			for k, vv := range resp.Header {
				for _, v := range vv {
					w.Header().Add(k, v)
				}
			}
			// Replace chunked encoding with explicit Content-Length
			w.Header().Del("Transfer-Encoding")
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))

			w.WriteHeader(resp.StatusCode)
			w.Write(body)

			log.Info("request completed",
				"method", r.Method,
				"path", r.URL.Path,
				"status", resp.StatusCode,
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
