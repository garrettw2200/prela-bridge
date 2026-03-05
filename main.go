package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
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
		"mode", "http-reverse-proxy",
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

	// HTTP reverse proxy using Tailscale dialer
	// DisableKeepAlives forces a fresh Tailscale connection per request.
	// tsnet connections have issues with HTTP keep-alive — the second
	// request on a reused connection hangs during chunked body transfer.
	tsTransport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			log.Info("dialing tailscale", "target", targetAddr)
			return ts.Dial(ctx, "tcp", targetAddr)
		},
		DisableKeepAlives:     true, // Fresh connection per request
		ResponseHeaderTimeout: 300 * time.Second,
		DisableCompression:    true,
	}

	proxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = targetURL.Scheme
			req.URL.Host = targetURL.Host
			req.Host = targetURL.Host
		},
		Transport: tsTransport,
		ModifyResponse: func(resp *http.Response) error {
			log.Info("upstream response",
				"status", resp.StatusCode,
				"contentLength", resp.ContentLength,
				"transferEncoding", fmt.Sprintf("%v", resp.TransferEncoding),
			)
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			log.Error("proxy error", "method", r.Method, "path", r.URL.Path, "err", err)
			http.Error(w, "Bad Gateway", http.StatusBadGateway)
		},
		FlushInterval: -1, // Flush immediately — don't buffer response
	}

	// Main proxy server
	proxyServer := &http.Server{
		Addr: ":" + listenPort,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			active := b.activeConns.Add(1)
			defer b.activeConns.Add(-1)
			log.Info("proxying request",
				"method", r.Method,
				"path", r.URL.Path,
				"query", r.URL.RawQuery,
				"contentLength", r.ContentLength,
				"remote", r.RemoteAddr,
				"active", active,
			)
			start := time.Now()
			proxy.ServeHTTP(w, r)
			log.Info("request completed",
				"method", r.Method,
				"path", r.URL.Path,
				"duration", time.Since(start).String(),
				"remote", r.RemoteAddr,
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
