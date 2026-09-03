package health

import (
	"context"
	"io"
	"log/slog"
	"math/rand/v2"
	"sync"

	"github.com/meshd/meshd/internal/clock"
	"github.com/meshd/meshd/internal/cluster"
	"github.com/meshd/meshd/internal/metrics"
)

// Supervisor owns the prober and detector goroutines and keeps them in step
// with the live cluster set. Sync is called after every config push.
type Supervisor struct {
	clk clock.Clock
	log *slog.Logger
	rng *rand.Rand

	m *Metrics

	mu      sync.Mutex
	running map[string]context.CancelFunc
}

// NewSupervisor registers health metrics and returns an idle Supervisor.
func NewSupervisor(clk clock.Clock, log *slog.Logger, reg *metrics.Registry) *Supervisor {
	if clk == nil {
		clk = clock.Real{}
	}
	if log == nil {
		log = slog.Default()
	}
	return &Supervisor{
		clk:     clk,
		log:     log,
		running: map[string]context.CancelFunc{},
		m:       NewMetrics(reg),
	}
}

// WithRand fixes the jitter source so probe scheduling is reproducible.
func (s *Supervisor) WithRand(r *rand.Rand) *Supervisor {
	s.rng = r
	return s
}

// Sync starts workers for newly configured endpoints and stops workers whose
// endpoint or cluster has gone away.
func (s *Supervisor) Sync(ctx context.Context, m *cluster.Manager) {
	wanted := map[string]func(context.Context){}

	for _, c := range m.All() {
		cfg := c.Config()
		if cfg.Outlier != nil {
			cl := c
			oc := *cfg.Outlier
			wanted["outlier|"+cl.Name] = func(ctx context.Context) {
				NewDetector(cl, oc, s.clk, s.log, s.m).Run(ctx)
			}
		}
		if cfg.HealthCheck == nil {
			continue
		}
		hc := *cfg.HealthCheck
		for _, e := range c.Endpoints() {
			ep := e
			name := c.Name
			wanted["probe|"+name+"|"+ep.Addr] = func(ctx context.Context) {
				pr := NewProber(name, ep, hc, s.clk, s.log, s.m)
				pr.rng = s.rng
				pr.Run(ctx)
			}
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for key, cancel := range s.running {
		if _, ok := wanted[key]; !ok {
			cancel()
			delete(s.running, key)
		}
	}
	for key, start := range wanted {
		if _, ok := s.running[key]; ok {
			continue
		}
		wctx, cancel := context.WithCancel(ctx)
		s.running[key] = cancel
		go start(wctx)
	}
}

// Stop cancels every worker.
func (s *Supervisor) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, cancel := range s.running {
		cancel()
		delete(s.running, key)
	}
}

func discard(r io.Reader) (int64, error) { return io.Copy(io.Discard, r) }
