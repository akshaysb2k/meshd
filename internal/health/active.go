// Package health implements active health checking and passive outlier
// detection. The two are complementary: active probing catches a backend that
// is down but receiving no traffic, passive detection catches one that answers
// probes fine but fails real requests.
package health

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/meshd/meshd/internal/clock"
	"github.com/meshd/meshd/internal/cluster"
	"github.com/meshd/meshd/internal/config"
)

// Prober actively probes one endpoint. Run drives it on an interval; ProbeOnce
// performs a single round and is what the simulation harness calls, so a
// scenario can advance health state without any goroutines or wall clock.
type Prober struct {
	cluster string
	ep      *cluster.Endpoint
	cfg     config.HealthCheck
	clk     clock.Clock
	log     *slog.Logger
	rng     *rand.Rand
	m       *Metrics
}

// NewProber builds a prober for one endpoint.
func NewProber(clusterName string, ep *cluster.Endpoint, cfg config.HealthCheck, clk clock.Clock, log *slog.Logger, m *Metrics) *Prober {
	return &Prober{cluster: clusterName, ep: ep, cfg: cfg, clk: clk, log: log, m: m}
}

// Endpoint returns the endpoint this prober watches.
func (p *Prober) Endpoint() *cluster.Endpoint { return p.ep }

// Run probes on a jittered interval until ctx is cancelled.
func (p *Prober) Run(ctx context.Context) {
	interval := p.cfg.Interval.Or(2 * time.Second)

	// Stagger the first probe. Without this every endpoint in the fleet is
	// probed on the same tick, which produces a synchronised load spike and,
	// worse, makes every proxy replica agree on a false negative at once.
	select {
	case <-p.clk.After(p.jitter(interval)):
	case <-ctx.Done():
		return
	}
	for {
		p.ProbeOnce(ctx)
		select {
		case <-p.clk.After(p.jitter(interval)):
		case <-ctx.Done():
			return
		}
	}
}

// ProbeOnce performs a single probe and applies the hysteresis thresholds.
func (p *Prober) ProbeOnce(ctx context.Context) bool {
	timeout := p.cfg.Timeout.Or(time.Second)
	want := p.cfg.ExpectedStatus
	if want == 0 {
		want = http.StatusOK
	}
	path := p.cfg.Path
	if path == "" {
		path = "/healthz"
	}

	client := &http.Client{Transport: p.ep.Transport(), Timeout: timeout}
	start := p.clk.Now()
	ok := p.probe(ctx, client, path, want, timeout)
	elapsed := p.clk.Now().Sub(start)
	if elapsed < 0 {
		elapsed = 0
	}
	p.m.Duration.With(p.cluster).Observe(elapsed.Seconds())

	result := "failure"
	if ok {
		result = "success"
	}
	p.m.Checks.With(p.cluster, p.ep.Addr, result).Inc()

	if changed := p.ep.RecordProbe(ok, p.cfg.HealthyThreshold, p.cfg.UnhealthyThreshold, p.clk.Now()); changed {
		p.log.Warn("endpoint health changed",
			"cluster", p.cluster, "endpoint", p.ep.Addr, "healthy", p.ep.Healthy())
	}
	if p.ep.Healthy() {
		p.m.Status.With(p.cluster, p.ep.Addr).Set(1)
	} else {
		p.m.Status.With(p.cluster, p.ep.Addr).Set(0)
	}
	return ok
}

func (p *Prober) probe(ctx context.Context, client *http.Client, path string, want int, timeout time.Duration) bool {
	rctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	u := *p.ep.URL
	u.Path = path
	req, err := http.NewRequestWithContext(rctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return false
	}
	req.Header.Set("User-Agent", "meshd-healthcheck/1")
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	// Drain the body so the connection returns to the pool instead of being
	// torn down and redialled on every single probe.
	_, _ = discard(resp.Body)
	return resp.StatusCode == want
}

// jitter spreads probe times by +/- JitterPercent of the interval.
func (p *Prober) jitter(d time.Duration) time.Duration {
	pct := p.cfg.JitterPercent
	if pct <= 0 {
		pct = 20
	}
	if pct > 100 {
		pct = 100
	}
	span := float64(d) * pct / 100.0
	var r float64
	if p.rng != nil {
		r = p.rng.Float64()
	} else {
		r = rand.Float64()
	}
	out := time.Duration(float64(d) - span/2 + r*span)
	if out < time.Millisecond {
		out = time.Millisecond
	}
	return out
}
