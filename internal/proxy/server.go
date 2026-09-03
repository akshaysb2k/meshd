// Package proxy is the data plane: listener, routing, balancing and upstream
// forwarding, plus the admin surface.
package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/meshd/meshd/internal/clock"
	"github.com/meshd/meshd/internal/cluster"
	"github.com/meshd/meshd/internal/config"
	"github.com/meshd/meshd/internal/health"
	"github.com/meshd/meshd/internal/metrics"
	"github.com/meshd/meshd/internal/retry"
	"github.com/meshd/meshd/internal/router"
)

// Proxy is the data plane instance.
type Proxy struct {
	log      *slog.Logger
	clock    clock.Clock
	registry *metrics.Registry

	clusters *cluster.Manager
	health   *health.Supervisor

	// table is swapped atomically on every config push. In-flight requests keep
	// the table they matched against; new requests see the new one.
	table    atomic.Pointer[router.Table]
	draining atomic.Bool

	manualHealth bool

	rngMu sync.Mutex
	rng   *rand.Rand

	requests        *metrics.Counter
	duration        *metrics.Histogram
	attempts        *metrics.Counter
	retries         *metrics.Counter
	retriesDenied   *metrics.Counter
	overflows       *metrics.Counter
	hedges          *metrics.Counter
	upstreamLatency *metrics.Histogram
	panicGauge      *metrics.Gauge
	configVersion   *metrics.Gauge
	configApplied   *metrics.Counter
}

// Options configures a Proxy.
type Options struct {
	Logger   *slog.Logger
	Clock    clock.Clock
	Registry *metrics.Registry

	// Dialer replaces the real connection pool on every endpoint. Used by the
	// simulation harness to run the proxy against a scripted network.
	Dialer cluster.Dialer
	// DisableHealthWorkers stops Apply from spawning prober and detector
	// goroutines, so a test or simulation can step them by hand instead.
	DisableHealthWorkers bool
	// Rand seeds retry backoff jitter. Leave nil in production to use the
	// global source; the simulation supplies a seeded one so that a scenario
	// replays exactly.
	Rand *rand.Rand
}

// New builds an unconfigured Proxy. Call Apply before serving traffic.
func New(o Options) *Proxy {
	log := o.Logger
	if log == nil {
		log = slog.Default()
	}
	clk := o.Clock
	if clk == nil {
		clk = clock.Real{}
	}
	reg := o.Registry
	if reg == nil {
		reg = metrics.New()
	}
	p := &Proxy{
		log:      log,
		clock:    clk,
		registry: reg,
		clusters: cluster.NewManager(log),
		requests: reg.NewCounter("meshd_requests_total",
			"Client requests by response code.", "route", "cluster", "code"),
		duration: reg.NewHistogram("meshd_request_duration_seconds",
			"End to end request latency as seen by the client.", nil, "route", "cluster"),
		attempts: reg.NewCounter("meshd_upstream_attempts_total",
			"Upstream attempts by outcome.", "cluster", "endpoint", "outcome"),
		retries: reg.NewCounter("meshd_retries_total",
			"Retry attempts dispatched.", "cluster"),
		retriesDenied: reg.NewCounter("meshd_retries_denied_total",
			"Retries suppressed, by the gate that suppressed them.", "cluster", "gate"),
		overflows: reg.NewCounter("meshd_breaker_overflow_total",
			"Requests shed by a circuit breaker limit.", "cluster", "limit"),
		hedges: reg.NewCounter("meshd_hedged_attempts_total",
			"Hedge attempts dispatched.", "cluster"),
		upstreamLatency: reg.NewHistogram("meshd_upstream_attempt_duration_seconds",
			"Per-attempt upstream latency.", nil, "cluster"),
		panicGauge: reg.NewGauge("meshd_cluster_panic_mode",
			"1 when a cluster is ignoring health status.", "cluster"),
		configVersion: reg.NewGauge("meshd_config_last_applied_timestamp_seconds",
			"Unix time of the last successful config push.", "version"),
		configApplied: reg.NewCounter("meshd_config_pushes_total",
			"Config pushes by result.", "result"),
	}
	p.health = health.NewSupervisor(clk, log, reg)
	p.manualHealth = o.DisableHealthWorkers
	p.rng = o.Rand
	if o.Dialer != nil {
		p.clusters.SetDialer(o.Dialer)
	}
	p.table.Store(router.Build(&config.Snapshot{}))
	return p
}

// Registry exposes the metrics registry.
func (p *Proxy) Registry() *metrics.Registry { return p.registry }

// Clusters exposes the cluster manager.
func (p *Proxy) Clusters() *cluster.Manager { return p.clusters }

// Apply validates and installs a new configuration snapshot.
//
// Ordering matters here. The snapshot is validated first, then the new routing
// table is built, then clusters are reconciled, and only then is the table
// swapped. A push that fails validation leaves the running config untouched --
// a broken config file should never be able to take the proxy down.
func (p *Proxy) Apply(ctx context.Context, snap *config.Snapshot) error {
	if err := snap.Validate(); err != nil {
		p.configApplied.With("rejected").Inc()
		p.log.Error("config rejected", "error", err)
		return err
	}
	table := router.Build(snap)
	if _, err := p.clusters.Apply(ctx, snap, p.clock.Now()); err != nil {
		p.configApplied.With("failed").Inc()
		return err
	}
	p.table.Store(table)
	if !p.manualHealth {
		p.health.Sync(ctx, p.clusters)
	}
	p.configApplied.With("applied").Inc()
	p.configVersion.With(snap.Version).Set(float64(p.clock.Now().Unix()))
	return nil
}

// Handler returns the data plane HTTP handler.
func (p *Proxy) Handler() http.Handler { return p }

// AdminHandler returns the operational surface: metrics, readiness and a live
// dump of cluster state. The state dump is what makes an incident debuggable at
// three in the morning without attaching a debugger.
func (p *Proxy) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_ = p.registry.WriteText(w)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		// Reporting not-ready as soon as draining starts gives the upstream
		// load balancer time to stop sending traffic before the listener
		// actually closes. Skipping this is why "graceful" deploys still
		// produce a spike of connection resets.
		if p.draining.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("draining\n"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})
	mux.HandleFunc("/clusters", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(p.dumpState())
	})
	mux.HandleFunc("/routes", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		type routeView struct {
			Name    string `json:"name"`
			Cluster string `json:"cluster"`
			Timeout string `json:"timeout"`
			Retries int    `json:"max_attempts"`
			Hedged  bool   `json:"hedged"`
		}
		var out []routeView
		for _, rt := range p.table.Load().Routes() {
			v := routeView{Name: rt.Name, Cluster: rt.Cluster, Timeout: rt.Timeout.String(), Retries: 1, Hedged: rt.Hedge != nil}
			if rt.Retry != nil {
				v.Retries = rt.Retry.MaxAttempts
			}
			out = append(out, v)
		}
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(out)
	})
	return mux
}

type endpointView struct {
	Addr         string `json:"addr"`
	Healthy      bool   `json:"healthy"`
	Ejected      bool   `json:"ejected"`
	EjectedUntil string `json:"ejected_until,omitempty"`
	Ejections    int64  `json:"ejection_count"`
	Draining     bool   `json:"draining"`
	Active       int64  `json:"active_requests"`
	Total        int64  `json:"total_requests"`
	Weight       string `json:"weight"`
}

type clusterView struct {
	Name      string         `json:"name"`
	Policy    string         `json:"policy"`
	PanicMode bool           `json:"panic_mode"`
	Healthy   int            `json:"healthy"`
	Total     int            `json:"total"`
	Retry     retryView      `json:"retry_budget"`
	Endpoints []endpointView `json:"endpoints"`
}

type retryView struct {
	ActiveRequests int64 `json:"active_requests"`
	ActiveRetries  int64 `json:"active_retries"`
	Allowed        int64 `json:"allowed"`
	Granted        int64 `json:"granted"`
	Denied         int64 `json:"denied"`
}

type stateView struct {
	ConfigVersion string        `json:"config_version"`
	Draining      bool          `json:"draining"`
	Clusters      []clusterView `json:"clusters"`
}

func (p *Proxy) dumpState() stateView {
	now := p.clock.Now()
	out := stateView{ConfigVersion: p.clusters.Version(), Draining: p.draining.Load()}
	for _, c := range p.clusters.All() {
		cfg := c.Config()
		s := c.Summary(now)
		active, retries, allowed, granted, denied := c.Budget().Stats()
		cv := clusterView{
			Name: c.Name, Policy: c.Policy(), PanicMode: s.PanicMode,
			Healthy: s.Healthy, Total: s.Total,
			Retry: retryView{active, retries, allowed, granted, denied},
		}
		for _, e := range c.Endpoints() {
			ev := endpointView{
				Addr:      e.Addr,
				Healthy:   e.Healthy(),
				Ejected:   e.Ejected(now),
				Ejections: e.EjectionCount(),
				Draining:  e.Draining(),
				Active:    e.Active(),
				Total:     e.Total(),
				Weight:    formatWeight(e.Weight(now, cfg.SlowStart.D(), cfg.SlowStartMinWeight)),
			}
			if u := e.EjectedUntil(); !u.IsZero() {
				ev.EjectedUntil = u.Format(time.RFC3339Nano)
			}
			cv.Endpoints = append(cv.Endpoints, ev)
		}
		out.Clusters = append(out.Clusters, cv)
	}
	return out
}

func formatWeight(w float64) string {
	b, _ := json.Marshal(w)
	return string(b)
}

// RunOptions configures the listeners.
type RunOptions struct {
	Addr         string
	AdminAddr    string
	DrainTimeout time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
	ReadyDelay   time.Duration
}

// Run serves until ctx is cancelled, then drains.
func (p *Proxy) Run(ctx context.Context, o RunOptions) error {
	if o.DrainTimeout == 0 {
		o.DrainTimeout = 20 * time.Second
	}
	srv := &http.Server{
		Addr:         o.Addr,
		Handler:      p.Handler(),
		ReadTimeout:  orDur(o.ReadTimeout, 30*time.Second),
		WriteTimeout: orDur(o.WriteTimeout, 60*time.Second),
		IdleTimeout:  orDur(o.IdleTimeout, 120*time.Second),
	}
	admin := &http.Server{Addr: o.AdminAddr, Handler: p.AdminHandler()}

	errCh := make(chan error, 2)
	go func() {
		p.log.Info("data plane listening", "addr", o.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	go func() {
		p.log.Info("admin listening", "addr", o.AdminAddr)
		if err := admin.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	// Shutdown sequence, in order:
	//   1. flip readiness so the upstream LB stops sending new connections
	//   2. wait out ReadyDelay for that to propagate
	//   3. stop accepting and let in-flight requests finish
	//   4. drain upstream pools
	p.log.Info("draining", "timeout", o.DrainTimeout.String())
	p.draining.Store(true)
	if o.ReadyDelay > 0 {
		time.Sleep(o.ReadyDelay)
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), o.DrainTimeout)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		p.log.Warn("data plane shutdown incomplete", "error", err)
	}
	p.health.Stop()
	p.clusters.Shutdown(shutCtx, o.DrainTimeout)
	_ = admin.Shutdown(shutCtx)
	p.log.Info("drained")
	return nil
}

// backoff draws a retry delay, using the seeded source when one was supplied.
// The lock is only taken in simulation; production leaves rng nil and uses the
// lock-free global source.
func (p *Proxy) backoff(pol *retry.Policy, attempt int) time.Duration {
	if p.rng == nil {
		return pol.Backoff(attempt, nil)
	}
	p.rngMu.Lock()
	defer p.rngMu.Unlock()
	return pol.Backoff(attempt, p.rng)
}

func orDur(v, def time.Duration) time.Duration {
	if v == 0 {
		return def
	}
	return v
}
