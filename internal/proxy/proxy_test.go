package proxy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/meshd/meshd/internal/config"
)

func quiet() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// backend is a controllable upstream used by these tests.
type backend struct {
	srv     *httptest.Server
	hits    atomic.Int64
	status  atomic.Int64
	latency atomic.Int64 // milliseconds
	lastReq atomic.Pointer[http.Header]
}

func newBackend(t *testing.T, name string) *backend {
	t.Helper()
	b := &backend{}
	b.status.Store(200)
	b.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := r.Header.Clone()
		b.lastReq.Store(&h)
		b.hits.Add(1)
		if d := b.latency.Load(); d > 0 {
			select {
			case <-time.After(time.Duration(d) * time.Millisecond):
			case <-r.Context().Done():
				return
			}
		}
		code := int(b.status.Load())
		w.Header().Set("X-Backend", name)
		w.WriteHeader(code)
		_, _ = fmt.Fprintf(w, "%s\n", name)
	}))
	t.Cleanup(b.srv.Close)
	return b
}

func buildProxy(t *testing.T, snap *config.Snapshot) *Proxy {
	t.Helper()
	p := New(Options{Logger: quiet()})
	if err := p.Apply(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	return p
}

func baseSnapshot(cluster config.Cluster, route config.Route) *config.Snapshot {
	route.Cluster = cluster.Name
	if route.Match.PathPrefix == "" {
		route.Match.PathPrefix = "/"
	}
	if route.Timeout == 0 {
		route.Timeout = config.Duration(5 * time.Second)
	}
	if route.Name == "" {
		route.Name = "r"
	}
	return &config.Snapshot{Version: "t", Clusters: []config.Cluster{cluster}, Routes: []config.Route{route}}
}

func get(t *testing.T, h http.Handler, path string) *http.Response {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.RemoteAddr = "203.0.113.7:1234"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Result()
}

// TestRetriesMaskAFailingBackend is the headline behaviour: one of three
// backends is hard down, and the client never sees it.
func TestRetriesMaskAFailingBackend(t *testing.T) {
	good1, good2, bad := newBackend(t, "good1"), newBackend(t, "good2"), newBackend(t, "bad")
	bad.status.Store(503)

	p := buildProxy(t, baseSnapshot(
		config.Cluster{
			Name: "api", Policy: "round_robin",
			Endpoints:      []string{good1.srv.URL, good2.srv.URL, bad.srv.URL},
			PanicThreshold: 0.34,
		},
		config.Route{Retry: &config.RetryPolicy{
			On: []string{"5xx"}, MaxAttempts: 3,
			BaseBackoff:   config.Duration(time.Millisecond),
			MaxBackoff:    config.Duration(2 * time.Millisecond),
			BudgetPercent: 100, MinRetryConcurrency: 100,
		}},
	))

	failures := 0
	for i := 0; i < 300; i++ {
		resp := get(t, p, "/x")
		_ = resp.Body.Close()
		if resp.StatusCode != 200 {
			failures++
		}
	}
	if failures != 0 {
		t.Fatalf("%d/300 requests failed despite two healthy backends", failures)
	}
	if bad.hits.Load() == 0 {
		t.Fatal("the failing backend never received traffic, so retries were not exercised")
	}
	t.Logf("bad backend absorbed %d attempts, all recovered by retry", bad.hits.Load())
}

// TestRetryBudgetCapsAmplificationUnderTotalOutage is the retry storm test.
func TestRetryBudgetCapsAmplificationUnderTotalOutage(t *testing.T) {
	b1, b2 := newBackend(t, "b1"), newBackend(t, "b2")
	b1.status.Store(503)
	b2.status.Store(503)

	p := buildProxy(t, baseSnapshot(
		config.Cluster{
			Name: "api", Policy: "round_robin",
			Endpoints: []string{b1.srv.URL, b2.srv.URL},
		},
		config.Route{Retry: &config.RetryPolicy{
			On: []string{"5xx"}, MaxAttempts: 4,
			BaseBackoff:   config.Duration(time.Millisecond),
			MaxBackoff:    config.Duration(time.Millisecond),
			BudgetPercent: 20, MinRetryConcurrency: 1,
		}},
	))

	const n = 200
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp := get(t, p, "/x")
			_ = resp.Body.Close()
		}()
	}
	wg.Wait()

	attempts := b1.hits.Load() + b2.hits.Load()
	// Without a budget this would approach n*4 = 800. The budget must hold the
	// amplification factor well below that.
	if attempts >= n*4 {
		t.Fatalf("%d upstream attempts for %d requests: no amplification control at all", attempts, n)
	}
	amp := float64(attempts) / float64(n)
	if amp > 2.5 {
		t.Fatalf("amplification factor %.2fx is too high; the budget is not clamping", amp)
	}
	t.Logf("total outage: %d requests produced %d upstream attempts (%.2fx amplification, cap would be 4x)", n, attempts, amp)
}

func TestOutlierEjectionRemovesFailingBackendFromRotation(t *testing.T) {
	good1, good2, bad := newBackend(t, "good1"), newBackend(t, "good2"), newBackend(t, "bad")
	bad.status.Store(503)

	p := buildProxy(t, baseSnapshot(
		config.Cluster{
			Name: "api", Policy: "round_robin",
			Endpoints:      []string{good1.srv.URL, good2.srv.URL, bad.srv.URL},
			PanicThreshold: 0.34,
			Outlier: &config.Outlier{
				Consecutive5xx:     3,
				Interval:           config.Duration(20 * time.Millisecond),
				BaseEjectionTime:   config.Duration(2 * time.Second),
				MaxEjectionPercent: 50,
			},
		},
		config.Route{Retry: &config.RetryPolicy{
			On: []string{"5xx"}, MaxAttempts: 3,
			BaseBackoff:   config.Duration(time.Millisecond),
			BudgetPercent: 100, MinRetryConcurrency: 100,
		}},
	))

	for i := 0; i < 100; i++ {
		resp := get(t, p, "/x")
		_ = resp.Body.Close()
	}
	deadline := time.Now().Add(3 * time.Second)
	cl, _ := p.Clusters().Get("api")
	var badEp = func() interface{ EjectionCount() int64 } {
		for _, e := range cl.Endpoints() {
			if e.Addr == bad.srv.URL {
				return e
			}
		}
		return nil
	}()
	for badEp.EjectionCount() == 0 && time.Now().Before(deadline) {
		resp := get(t, p, "/x")
		_ = resp.Body.Close()
		time.Sleep(5 * time.Millisecond)
	}
	if badEp.EjectionCount() == 0 {
		t.Fatal("the consistently failing backend was never ejected")
	}

	// Once ejected it should stop receiving traffic entirely.
	before := bad.hits.Load()
	for i := 0; i < 100; i++ {
		resp := get(t, p, "/x")
		_ = resp.Body.Close()
	}
	if got := bad.hits.Load() - before; got > 5 {
		t.Fatalf("ejected backend still took %d requests", got)
	}
	t.Logf("ejected after %d ejections; took %d further requests out of 100", badEp.EjectionCount(), bad.hits.Load()-before)
}

// TestConfigPushUnderLoadDropsNoRequests is the deploy-safety test.
func TestConfigPushUnderLoadDropsNoRequests(t *testing.T) {
	b1, b2, b3 := newBackend(t, "b1"), newBackend(t, "b2"), newBackend(t, "b3")
	for _, b := range []*backend{b1, b2, b3} {
		b.latency.Store(2)
	}
	cluster := config.Cluster{
		Name: "api", Policy: "least_request",
		Endpoints:    []string{b1.srv.URL, b2.srv.URL, b3.srv.URL},
		DrainTimeout: config.Duration(time.Second),
	}
	p := buildProxy(t, baseSnapshot(cluster, config.Route{}))

	stop := make(chan struct{})
	var failures, total atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				resp := get(t, p, "/x")
				_ = resp.Body.Close()
				total.Add(1)
				if resp.StatusCode != 200 {
					failures.Add(1)
				}
			}
		}()
	}

	time.Sleep(100 * time.Millisecond)
	// Remove one endpoint and add nothing, mid-flight.
	shrunk := cluster
	shrunk.Endpoints = []string{b1.srv.URL, b2.srv.URL}
	if err := p.Apply(context.Background(), baseSnapshot(shrunk, config.Route{})); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	// Add it back plus a new one.
	grown := cluster
	grown.Endpoints = []string{b1.srv.URL, b2.srv.URL, b3.srv.URL}
	if err := p.Apply(context.Background(), baseSnapshot(grown, config.Route{})); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	close(stop)
	wg.Wait()

	if failures.Load() != 0 {
		t.Fatalf("%d of %d requests failed across two config pushes", failures.Load(), total.Load())
	}
	t.Logf("%d requests spanning two config pushes, zero failures", total.Load())
}

func TestCircuitBreakerShedsLoadInsteadOfQueueing(t *testing.T) {
	b := newBackend(t, "slow")
	b.latency.Store(200)

	p := buildProxy(t, baseSnapshot(
		config.Cluster{
			Name: "api", Policy: "round_robin", Endpoints: []string{b.srv.URL},
			CircuitBreaker: &config.CircuitBreaker{MaxRequests: 4},
		},
		config.Route{},
	))

	var shed, ok atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp := get(t, p, "/x")
			_ = resp.Body.Close()
			if resp.StatusCode == 503 {
				shed.Add(1)
			} else {
				ok.Add(1)
			}
		}()
	}
	wg.Wait()

	if shed.Load() == 0 {
		t.Fatal("no requests were shed despite a max_requests limit of 4")
	}
	if ok.Load() == 0 {
		t.Fatal("every request was shed; the limit is not letting anything through")
	}
	if b.hits.Load() > 8 {
		t.Fatalf("backend saw %d concurrent-ish requests against a limit of 4", b.hits.Load())
	}
	t.Logf("40 concurrent requests against a limit of 4: %d served, %d shed fast", ok.Load(), shed.Load())
}

func TestHedgingCutsTailLatency(t *testing.T) {
	fast, slow := newBackend(t, "fast"), newBackend(t, "slow")
	fast.latency.Store(5)
	slow.latency.Store(400)

	snap := baseSnapshot(
		config.Cluster{Name: "api", Policy: "round_robin", Endpoints: []string{fast.srv.URL, slow.srv.URL}},
		config.Route{
			Hedge: &config.HedgePolicy{Delay: config.Duration(40 * time.Millisecond), MaxHedges: 1},
			Retry: &config.RetryPolicy{On: []string{"5xx"}, MaxAttempts: 1, BudgetPercent: 100, MinRetryConcurrency: 100},
		},
	)
	p := buildProxy(t, snap)

	var latencies []time.Duration
	for i := 0; i < 40; i++ {
		t0 := time.Now()
		resp := get(t, p, "/x")
		_ = resp.Body.Close()
		latencies = append(latencies, time.Since(t0))
		if resp.StatusCode != 200 {
			t.Fatalf("request %d returned %d", i, resp.StatusCode)
		}
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p95 := latencies[int(float64(len(latencies)-1)*0.95)]

	// Round robin sends half the requests to the 400ms backend. Without
	// hedging p95 would be ~400ms; with a 40ms hedge it should be far lower.
	if p95 > 150*time.Millisecond {
		t.Fatalf("p95 is %s; hedging is not cutting the tail", p95)
	}
	t.Logf("p50=%s p95=%s max=%s against a 400ms backend serving half the traffic",
		latencies[len(latencies)/2], p95, latencies[len(latencies)-1])
}

func TestHopByHopHeadersStrippedAndForwardedHeadersSet(t *testing.T) {
	b := newBackend(t, "b")
	p := buildProxy(t, baseSnapshot(
		config.Cluster{Name: "api", Policy: "round_robin", Endpoints: []string{b.srv.URL}},
		config.Route{},
	))

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.RemoteAddr = "203.0.113.7:1234"
	req.Host = "api.example.com"
	req.Header.Set("Connection", "X-Custom-Hop")
	req.Header.Set("X-Custom-Hop", "should-be-dropped")
	req.Header.Set("Keep-Alive", "timeout=5")
	req.Header.Set("X-Keep-Me", "yes")
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)

	h := b.lastReq.Load()
	if h == nil {
		t.Fatal("backend received no request")
	}
	for _, drop := range []string{"X-Custom-Hop", "Keep-Alive", "Connection"} {
		if h.Get(drop) != "" {
			t.Fatalf("hop-by-hop header %s was forwarded upstream", drop)
		}
	}
	if h.Get("X-Keep-Me") != "yes" {
		t.Fatal("an ordinary header was dropped")
	}
	if h.Get("X-Forwarded-For") != "203.0.113.7" {
		t.Fatalf("X-Forwarded-For is %q", h.Get("X-Forwarded-For"))
	}
	if h.Get("X-Forwarded-Host") != "api.example.com" {
		t.Fatalf("X-Forwarded-Host is %q", h.Get("X-Forwarded-Host"))
	}
	if h.Get("X-Request-Id") == "" {
		t.Fatal("no correlation id was attached")
	}
}

func TestNoHealthyEndpointsReturns503NotPanic(t *testing.T) {
	b := newBackend(t, "b")
	b.srv.Close() // nothing is listening
	p := buildProxy(t, baseSnapshot(
		config.Cluster{Name: "api", Policy: "round_robin", Endpoints: []string{b.srv.URL}},
		config.Route{},
	))
	resp := get(t, p, "/x")
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway && resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 502 or 503", resp.StatusCode)
	}
	if resp.Header.Get("X-Request-Id") == "" {
		t.Fatal("error responses must still carry a correlation id")
	}
}

func TestUnroutablePathReturns404(t *testing.T) {
	b := newBackend(t, "b")
	snap := baseSnapshot(
		config.Cluster{Name: "api", Policy: "round_robin", Endpoints: []string{b.srv.URL}},
		config.Route{Match: config.RouteMatch{PathPrefix: "/api"}},
	)
	p := buildProxy(t, snap)
	resp := get(t, p, "/nope")
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("got %d, want 404", resp.StatusCode)
	}
	if resp.Header.Get("X-Proxy-Reason") != "no_route" {
		t.Fatalf("missing diagnostic reason header, got %q", resp.Header.Get("X-Proxy-Reason"))
	}
}

func TestInvalidConfigPushLeavesRunningConfigIntact(t *testing.T) {
	b := newBackend(t, "b")
	good := baseSnapshot(
		config.Cluster{Name: "api", Policy: "round_robin", Endpoints: []string{b.srv.URL}},
		config.Route{},
	)
	p := buildProxy(t, good)

	bad := &config.Snapshot{Version: "v2", Clusters: []config.Cluster{{Name: "api"}}}
	if err := p.Apply(context.Background(), bad); err == nil {
		t.Fatal("an invalid snapshot was accepted")
	}
	resp := get(t, p, "/x")
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("a rejected config push broke the running proxy: got %d", resp.StatusCode)
	}
}

func TestRingHashGivesStableSessionAffinity(t *testing.T) {
	b1, b2, b3 := newBackend(t, "b1"), newBackend(t, "b2"), newBackend(t, "b3")
	p := buildProxy(t, baseSnapshot(
		config.Cluster{
			Name: "api", Policy: "ring_hash", RingReplicas: 100,
			Endpoints: []string{b1.srv.URL, b2.srv.URL, b3.srv.URL},
		},
		config.Route{HashOn: "X-Session-Id"},
	))

	first := map[string]string{}
	for i := 0; i < 30; i++ {
		sess := fmt.Sprintf("session-%d", i)
		for attempt := 0; attempt < 5; attempt++ {
			req := httptest.NewRequest(http.MethodGet, "/x", nil)
			req.RemoteAddr = "203.0.113.7:1234"
			req.Header.Set("X-Session-Id", sess)
			rec := httptest.NewRecorder()
			p.ServeHTTP(rec, req)
			got := rec.Header().Get("X-Backend")
			if prev, ok := first[sess]; ok && prev != got {
				t.Fatalf("session %s moved from %s to %s", sess, prev, got)
			}
			first[sess] = got
		}
	}
	distinct := map[string]bool{}
	for _, v := range first {
		distinct[v] = true
	}
	if len(distinct) < 2 {
		t.Fatalf("all sessions landed on %d backend(s); the ring is degenerate", len(distinct))
	}
}
