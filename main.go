package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
	"tailscale.com/tsnet"
)

type bridge struct {
	ts          *tsnet.Server
	targetAddr  string
	dialTimeout time.Duration
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
	)

	if err := ts.Start(); err != nil {
		log.Error("tailscale start failed", "err", err)
		os.Exit(1)
	}
	defer ts.Close()

	log.Info("tailscale connected")

	b := &bridge{
		ts:          ts,
		targetAddr:  targetAddr,
		dialTimeout: 15 * time.Second,
		log:         log,
	}

	// Health check server
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

	// TCP listener
	listener, err := net.Listen("tcp", "[::]:"+listenPort)
	if err != nil {
		log.Error("listen failed", "err", err)
		os.Exit(1)
	}

	// Graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	var wg sync.WaitGroup

	go func() {
		<-ctx.Done()
		log.Info("shutting down, draining connections", "active", b.activeConns.Load())
		listener.Close()
		healthServer.Close()

		// Wait for active connections with timeout
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
			log.Info("all connections drained")
		case <-time.After(30 * time.Second):
			log.Warn("drain timeout, forcing shutdown", "remaining", b.activeConns.Load())
		}
	}()

	// Accept loop
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				break // shutting down
			}
			log.Error("accept failed", "err", err)
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			b.handleConn(conn)
		}()
	}
}

func (b *bridge) handleConn(clientConn net.Conn) {
	defer clientConn.Close()

	active := b.activeConns.Add(1)
	remoteAddr := clientConn.RemoteAddr().String()
	b.log.Info("connection accepted", "remote", remoteAddr, "active", active)

	defer func() {
		remaining := b.activeConns.Add(-1)
		b.log.Info("connection closed", "remote", remoteAddr, "active", remaining)
	}()

	// TCP keepalive on client side
	if tc, ok := clientConn.(*net.TCPConn); ok {
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(30 * time.Second)
	}

	// Dial target through Tailscale with timeout
	ctx, cancel := context.WithTimeout(context.Background(), b.dialTimeout)
	defer cancel()

	tsConn, err := b.ts.Dial(ctx, "tcp", b.targetAddr)
	if err != nil {
		b.log.Error("dial failed", "target", b.targetAddr, "err", err)
		return
	}
	defer tsConn.Close()

	// TCP keepalive on Tailscale side
	if tc, ok := tsConn.(*net.TCPConn); ok {
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(30 * time.Second)
	}

	// Bidirectional copy with half-close
	var g errgroup.Group

	// client -> tailscale
	g.Go(func() error {
		defer func() {
			if tc, ok := tsConn.(*net.TCPConn); ok {
				tc.CloseWrite()
			}
		}()
		buf := make([]byte, 256*1024)
		_, err := io.CopyBuffer(tsConn, clientConn, buf)
		return err
	})

	// tailscale -> client
	g.Go(func() error {
		defer func() {
			if tc, ok := clientConn.(*net.TCPConn); ok {
				tc.CloseWrite()
			}
		}()
		buf := make([]byte, 256*1024)
		_, err := io.CopyBuffer(clientConn, tsConn, buf)
		return err
	})

	if err := g.Wait(); err != nil {
		// Don't log "use of closed network connection" — it's normal on half-close
		b.log.Debug("transfer completed with error", "remote", remoteAddr, "err", err)
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
		// Also check common aliases
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
