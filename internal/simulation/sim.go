package simulation

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"time"

	"github.com/meshd/meshd/internal/clock"
	"github.com/meshd/meshd/internal/cluster"
	"github.com/meshd/meshd/internal/config"
	"github.com/meshd/meshd/internal/health"
	"github.com/meshd/meshd/internal/metrics"
	"github.com/meshd/meshd/internal/proxy"
)

// Epoch is the fixed start time of every simulation.
var Epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// Sim drives a whole proxy deterministically.
type Sim struct {
	Seed  uint64
	Clock *clock.Fake
	Net   *Network
	Proxy *proxy.Proxy

	probers   []*health.Prober
	detectors []*health.Detector
	clusters  []*cluster.Cluster

	trace []string
	step  int
}

// New builds a simulation over the given snapshot. Health workers are stepped
// by hand rather than run as goroutines, which is what keeps ordering fixed.
func New(seed uint64, snap *config.Snapshot) (*Sim, error) {
	net := NewNetwork(seed)
	clk := clock.NewFake(Epoch)
	reg := metrics.New()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	p := proxy.New(proxy.Options{
		Logger:               log,
		Clock:                simClock{clk},
		Registry:             reg,
		Dialer:               net.Dialer(),
		DisableHealthWorkers: true,
		Rand:                 rand.New(rand.NewPCG(seed, 0x5deece66d)),
	})
	if err := p.Apply(context.Background(), snap); err != nil {
		return nil, err
	}

	s := &Sim{Seed: seed, Clock: clk, Net: net, Proxy: p}
	hm := health.NewMetrics(reg)
	for _, c := range p.Clusters().All() {
		s.clusters = append(s.clusters, c)
		cfg := c.Config()
		if cfg.Outlier != nil {
			s.detectors = append(s.detectors, health.NewDetector(c, *cfg.Outlier, clk, log, hm))
		}
		if cfg.HealthCheck != nil {
			for _, e := range c.Endpoints() {
				s.probers = append(s.probers, health.NewProber(c.Name, e, *cfg.HealthCheck, clk, log, hm))
			}
		}
	}
	// Fixed ordering. Map iteration order is the one remaining source of
	// nondeterminism once the clock and the network are pinned.
	sort.Slice(s.probers, func(i, j int) bool {
		return s.probers[i].Endpoint().Addr < s.probers[j].Endpoint().Addr
	})
	sort.Slice(s.clusters, func(i, j int) bool { return s.clusters[i].Name < s.clusters[j].Name })
	return s, nil
}

// Result is the outcome of one simulated client request.
type Result struct {
	Status  int
	Backend string
	Reason  string
}

// Request issues one request synchronously.
func (s *Sim) Request(method, path string) Result {
	req := httptest.NewRequest(method, path, nil)
	req.RemoteAddr = "198.51.100.1:1000"
	rec := httptest.NewRecorder()
	s.Proxy.ServeHTTP(rec, req)
	res := rec.Result()
	_ = res.Body.Close()
	return Result{
		Status:  res.StatusCode,
		Backend: res.Header.Get("X-Backend"),
		Reason:  res.Header.Get("X-Proxy-Reason"),
	}
}

// Tick advances the clock, runs one round of health probes and one detector
// sweep, in that fixed order.
func (s *Sim) Tick(d time.Duration) {
	s.step++
	s.Clock.Advance(d)
	ctx := context.Background()
	for _, pr := range s.probers {
		pr.ProbeOnce(ctx)
	}
	for _, det := range s.detectors {
		det.Sweep()
	}
}

// Record appends a snapshot of endpoint state to the trace.
func (s *Sim) Record(note string) {
	now := s.Clock.Now()
	var parts []string
	for _, c := range s.clusters {
		for _, e := range c.Endpoints() {
			state := "up"
			switch {
			case e.Draining():
				state = "drain"
			case e.Ejected(now):
				state = "eject"
			case !e.Healthy():
				state = "unhealthy"
			}
			parts = append(parts, fmt.Sprintf("%s=%s", short(e.Addr), state))
		}
	}
	s.trace = append(s.trace, fmt.Sprintf("t=%s step=%d %s | %s",
		now.Sub(Epoch), s.step, note, strings.Join(parts, " ")))
}

// Trace returns the recorded event log. Two runs with the same seed must
// produce identical traces; that equality is the harness's core assertion.
func (s *Sim) Trace() []string { return append([]string(nil), s.trace...) }

// TraceString renders the trace for a failure message.
func (s *Sim) TraceString() string { return strings.Join(s.trace, "\n") }

// EjectedCount reports how many endpoints are currently out of rotation.
func (s *Sim) EjectedCount() int {
	now := s.Clock.Now()
	n := 0
	for _, c := range s.clusters {
		for _, e := range c.Endpoints() {
			if e.Ejected(now) {
				n++
			}
		}
	}
	return n
}

// Endpoint looks up a live endpoint by address.
func (s *Sim) Endpoint(addr string) *cluster.Endpoint {
	for _, c := range s.clusters {
		for _, e := range c.Endpoints() {
			if e.Addr == addr {
				return e
			}
		}
	}
	return nil
}

// EndpointCount reports the total endpoints across all clusters.
func (s *Sim) EndpointCount() int {
	n := 0
	for _, c := range s.clusters {
		n += len(c.Endpoints())
	}
	return n
}

// Addrs returns every endpoint address, sorted.
func (s *Sim) Addrs() []string {
	var out []string
	for _, c := range s.clusters {
		for _, e := range c.Endpoints() {
			out = append(out, e.Addr)
		}
	}
	sort.Strings(out)
	return out
}

// UpstreamAttempts totals real requests seen by the network.
func (s *Sim) UpstreamAttempts() int {
	n := 0
	for _, v := range s.Net.Hits() {
		n += v
	}
	return n
}

// Metrics renders the proxy's metrics, for assertions and debugging.
func (s *Sim) Metrics() string {
	var b strings.Builder
	_ = s.Proxy.Registry().WriteText(&b)
	return b.String()
}

// Snapshot builds a standard simulation config: one cluster, n endpoints, with
// health checking, outlier detection and budgeted retries all enabled.
func Snapshot(n int, tune func(*config.Cluster, *config.Route)) *config.Snapshot {
	eps := make([]string, n)
	for i := range eps {
		eps[i] = fmt.Sprintf("http://sim-%02d:80", i)
	}
	c := config.Cluster{
		Name: "api", Policy: "round_robin", Endpoints: eps,
		PanicThreshold: 0.5,
		HealthCheck: &config.HealthCheck{
			Path: "/healthz", Interval: config.Duration(time.Second),
			HealthyThreshold: 2, UnhealthyThreshold: 3,
		},
		Outlier: &config.Outlier{
			Consecutive5xx:           3,
			ConsecutiveGatewayErrors: 3,
			Interval:                 config.Duration(500 * time.Millisecond),
			BaseEjectionTime:         config.Duration(5 * time.Second),
			MaxEjectionTime:          config.Duration(30 * time.Second),
			MaxEjectionPercent:       50,
		},
	}
	r := config.Route{
		Name: "api", Cluster: "api",
		Match:   config.RouteMatch{PathPrefix: "/"},
		Timeout: config.Duration(2 * time.Second),
		Retry: &config.RetryPolicy{
			On: []string{"5xx", "connect-failure"}, MaxAttempts: n,
			BudgetPercent: 100, MinRetryConcurrency: int64(n * 4),
		},
	}
	if tune != nil {
		tune(&c, &r)
	}
	return &config.Snapshot{Version: "sim", Clusters: []config.Cluster{c}, Routes: []config.Route{r}}
}

var _ = http.MethodGet
