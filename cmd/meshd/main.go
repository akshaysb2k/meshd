// Command meshd is the data plane: an L7 reverse proxy with health checking,
// outlier ejection, circuit breaking, budgeted retries and hedging.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/meshd/meshd/internal/config"
	"github.com/meshd/meshd/internal/proxy"
	"github.com/meshd/meshd/internal/xds"
)

func main() {
	var (
		addr         = flag.String("addr", ":8080", "data plane listen address")
		adminAddr    = flag.String("admin-addr", ":9901", "admin listen address")
		configPath   = flag.String("config", "", "path to a config snapshot; watched for changes")
		controlPlane = flag.String("control-plane", "", "control plane stream URL, e.g. http://meshcp:8081/v1/config/stream")
		node         = flag.String("node", hostname(), "node id reported to the control plane")
		drain        = flag.Duration("drain-timeout", 20*time.Second, "how long to let in-flight requests finish")
		readyDelay   = flag.Duration("ready-delay", 2*time.Second, "delay between failing readiness and closing the listener")
		logLevel     = flag.String("log-level", "info", "debug, info, warn or error")
	)
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: parseLevel(*logLevel)}))
	slog.SetDefault(log)

	if *configPath == "" && *controlPlane == "" {
		log.Error("either -config or -control-plane is required")
		os.Exit(2)
	}

	p := proxy.New(proxy.Options{Logger: log})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if *configPath != "" {
		snap, err := config.Load(*configPath)
		if err != nil {
			log.Error("initial config load failed", "path", *configPath, "error", err)
			os.Exit(1)
		}
		if err := p.Apply(ctx, snap); err != nil {
			log.Error("initial config apply failed", "error", err)
			os.Exit(1)
		}
		go watchFile(ctx, *configPath, log, p)
	}

	if *controlPlane != "" {
		client := &xds.Client{URL: *controlPlane, Node: *node, Log: log, Applier: p}
		go func() { _ = client.Run(ctx) }()
	}

	if err := p.Run(ctx, proxy.RunOptions{
		Addr:         *addr,
		AdminAddr:    *adminAddr,
		DrainTimeout: *drain,
		ReadyDelay:   *readyDelay,
	}); err != nil {
		log.Error("server error", "error", err)
		os.Exit(1)
	}
}

// watchFile reapplies the snapshot when the file changes. Polling mtime keeps
// this dependency-free; inotify would be better but not by much at one-second
// granularity for a config file.
func watchFile(ctx context.Context, path string, log *slog.Logger, p *proxy.Proxy) {
	var lastMod time.Time
	var lastSize int64
	if st, err := os.Stat(path); err == nil {
		lastMod, lastSize = st.ModTime(), st.Size()
	}
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		st, err := os.Stat(path)
		if err != nil {
			continue
		}
		if st.ModTime().Equal(lastMod) && st.Size() == lastSize {
			continue
		}
		lastMod, lastSize = st.ModTime(), st.Size()
		snap, err := config.Load(path)
		if err != nil {
			log.Error("config reload rejected, keeping running config", "error", err)
			continue
		}
		if err := p.Apply(ctx, snap); err != nil {
			log.Error("config reload failed, keeping running config", "error", err)
			continue
		}
		log.Info("config reloaded from file", "version", snap.Version)
	}
}

func parseLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return h
}
