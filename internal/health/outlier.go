package health

import (
	"context"
	"log/slog"
	"math"
	"time"

	"github.com/meshd/meshd/internal/clock"
	"github.com/meshd/meshd/internal/cluster"
	"github.com/meshd/meshd/internal/config"
)

// detector runs passive outlier detection over one cluster.
//
// Two safety valves matter more than the detection rule itself. The ejection
// window grows with each ejection, so an endpoint that keeps failing stays out
// longer instead of flapping back in every few seconds. And the total ejected
// fraction is capped, so a bad deploy that breaks every backend cannot cause
// the detector to eject the entire fleet and take the service to zero.
type Detector struct {
	cl  *cluster.Cluster
	cfg config.Outlier
	clk clock.Clock
	log *slog.Logger
	m   *Metrics
}

// NewDetector builds an outlier detector for one cluster.
func NewDetector(cl *cluster.Cluster, cfg config.Outlier, clk clock.Clock, log *slog.Logger, m *Metrics) *Detector {
	return &Detector{cl: cl, cfg: cfg, clk: clk, log: log, m: m}
}

// Run sweeps on an interval until ctx is cancelled.
func (d *Detector) Run(ctx context.Context) {
	interval := d.cfg.Interval.Or(500 * time.Millisecond)
	for {
		select {
		case <-d.clk.After(interval):
		case <-ctx.Done():
			return
		}
		d.Sweep()
	}
}

// Sweep performs one detection pass: uneject anything whose window expired,
// then eject qualifying endpoints within the ejection budget.
func (d *Detector) Sweep() {
	now := d.clk.Now()
	eps := d.cl.Endpoints()
	if len(eps) == 0 {
		return
	}

	// Return expired endpoints to rotation first, so the ejection budget
	// reflects the state after recovery rather than before it.
	for _, e := range eps {
		if until := e.EjectedUntil(); !until.IsZero() && !now.Before(until) {
			e.Uneject(now)
			d.log.Info("endpoint unejected", "cluster", d.cl.Name, "endpoint", e.Addr)
		}
	}

	ejected := 0
	eligible := 0
	for _, e := range eps {
		if e.Draining() {
			continue
		}
		eligible++
		if e.Ejected(now) {
			ejected++
		}
	}
	if eligible == 0 {
		return
	}

	maxPct := d.cfg.MaxEjectionPercent
	if maxPct <= 0 {
		maxPct = 50
	}
	maxEjected := int(math.Floor(float64(eligible) * maxPct / 100.0))
	if maxEjected < 1 && maxPct > 0 {
		maxEjected = 1
	}

	c5 := d.cfg.Consecutive5xx
	if c5 <= 0 {
		c5 = 5
	}
	cg := d.cfg.ConsecutiveGatewayErrors
	if cg <= 0 {
		cg = 3
	}
	base := d.cfg.BaseEjectionTime.Or(5 * time.Second)
	maxEject := d.cfg.MaxEjectionTime.Or(5 * time.Minute)

	for _, e := range eps {
		if e.Draining() || e.Ejected(now) {
			continue
		}
		var reason string
		switch {
		case e.ConsecutiveGatewayErrors() >= int64(cg):
			reason = "consecutive_gateway_errors"
		case e.Consecutive5xx() >= int64(c5):
			reason = "consecutive_5xx"
		default:
			continue
		}
		if ejected >= maxEjected {
			// The endpoint qualifies but the cluster has no ejection budget
			// left. Leaving it in rotation degraded is strictly better than
			// having nowhere to send traffic.
			d.log.Warn("ejection suppressed by budget",
				"cluster", d.cl.Name, "endpoint", e.Addr,
				"ejected", ejected, "max", maxEjected)
			continue
		}
		window := backoffWindow(base, maxEject, e.EjectionCount())
		e.Eject(now.Add(window))
		ejected++
		d.m.Ejects.With(d.cl.Name, e.Addr, reason).Inc()
		d.log.Warn("endpoint ejected",
			"cluster", d.cl.Name, "endpoint", e.Addr,
			"reason", reason, "duration", window.String())
	}
	d.m.Ejected.With(d.cl.Name).Set(float64(ejected))
}

// backoffWindow grows the ejection duration linearly with prior ejections,
// capped at max.
func backoffWindow(base, max time.Duration, priorEjections int64) time.Duration {
	n := priorEjections + 1
	w := time.Duration(n) * base
	if w > max || w <= 0 {
		return max
	}
	return w
}
