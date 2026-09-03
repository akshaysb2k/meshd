// Command meshcp is the control plane. It watches a config file and broadcasts
// validated snapshots to every connected data plane.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/meshd/meshd/internal/config"
	"github.com/meshd/meshd/internal/xds"
)

func main() {
	var (
		addr       = flag.String("addr", ":8081", "control plane listen address")
		configPath = flag.String("config", "examples/config.json", "snapshot to serve")
	)
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	srv := xds.NewServer(log)

	snap, err := config.Load(*configPath)
	if err != nil {
		log.Error("initial config load failed", "error", err)
		os.Exit(1)
	}
	if err := srv.Publish(snap); err != nil {
		log.Error("initial publish failed", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go watch(ctx, *configPath, log, srv)

	httpSrv := &http.Server{Addr: *addr, Handler: srv.Handler()}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(sctx)
	}()

	log.Info("control plane listening", "addr", *addr, "config", *configPath)
	if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error("server error", "error", err)
		os.Exit(1)
	}
}

func watch(ctx context.Context, path string, log *slog.Logger, srv *xds.Server) {
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
			log.Error("reload rejected, continuing to serve previous snapshot", "error", err)
			continue
		}
		if err := srv.Publish(snap); err != nil {
			log.Error("publish rejected", "error", err)
		}
	}
}
