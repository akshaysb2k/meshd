package cluster

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/meshd/meshd/internal/config"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func snapshot(version string, endpoints ...string) *config.Snapshot {
	return &config.Snapshot{
		Version: version,
		Clusters: []config.Cluster{{
			Name: "api", Policy: "round_robin", Endpoints: endpoints,
			DrainTimeout: config.Duration(50 * time.Millisecond),
		}},
		Routes: []config.Route{{Name: "r", Cluster: "api", Match: config.RouteMatch{PathPrefix: "/"}}},
	}
}

// TestConfigPushPreservesEndpointIdentity is the test that protects the whole
// point of reconciliation. If a push rebuilds every Endpoint, the proxy throws
// away warm connection pools and health state, and a routine config change
// shows up as a latency spike and a burst of 502s.
func TestConfigPushPreservesEndpointIdentity(t *testing.T) {
	m := NewManager(quiet())
	now := time.Now()
	ctx := context.Background()

	if _, err := m.Apply(ctx, snapshot("v1", "http://a:80", "http://b:80", "http://c:80"), now); err != nil {
		t.Fatal(err)
	}
	cl, _ := m.Get("api")
	before := map[string]*Endpoint{}
	for _, e := range cl.Endpoints() {
		before[e.Addr] = e
	}

	// Mark one endpoint unhealthy and give another some traffic history, so we
	// can prove state survives the push and is not silently reset.
	before["http://b:80"].SetHealthy(false, now)
	rel := before["http://a:80"].Acquire()
	rel()

	stats, err := m.Apply(ctx, snapshot("v2", "http://a:80", "http://b:80", "http://d:80"), now)
	if err != nil {
		t.Fatal(err)
	}
	if stats.EndpointsRetained != 2 || stats.EndpointsAdded != 1 || stats.EndpointsRemoved != 1 {
		t.Fatalf("reconciliation stats wrong: %+v", stats)
	}

	cl, _ = m.Get("api")
	after := map[string]*Endpoint{}
	for _, e := range cl.Endpoints() {
		after[e.Addr] = e
	}

	for _, addr := range []string{"http://a:80", "http://b:80"} {
		if before[addr] != after[addr] {
			t.Fatalf("%s was rebuilt across the push; its connection pool and health state were discarded", addr)
		}
	}
	if after["http://b:80"].Healthy() {
		t.Fatal("health state was reset by an unrelated config push")
	}
	if after["http://a:80"].Total() != 1 {
		t.Fatal("traffic counters were reset by an unrelated config push")
	}
	if _, ok := after["http://d:80"]; !ok {
		t.Fatal("new endpoint was not added")
	}
	if _, ok := after["http://c:80"]; ok {
		t.Fatal("removed endpoint is still in rotation")
	}

	// The removed endpoint should be draining, not abruptly dropped.
	if !before["http://c:80"].Draining() {
		t.Fatal("removed endpoint was not put into draining state")
	}
}

func TestRemovedClusterIsDrained(t *testing.T) {
	m := NewManager(quiet())
	ctx := context.Background()
	now := time.Now()

	snap := snapshot("v1", "http://a:80")
	snap.Clusters = append(snap.Clusters, config.Cluster{
		Name: "legacy", Policy: "round_robin", Endpoints: []string{"http://z:80"},
		DrainTimeout: config.Duration(50 * time.Millisecond),
	})
	if _, err := m.Apply(ctx, snap, now); err != nil {
		t.Fatal(err)
	}
	legacy, _ := m.Get("legacy")
	zEndpoint := legacy.Endpoints()[0]

	if _, err := m.Apply(ctx, snapshot("v2", "http://a:80"), now); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.Get("legacy"); ok {
		t.Fatal("removed cluster is still live")
	}
	deadline := time.Now().Add(time.Second)
	for !zEndpoint.Draining() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !zEndpoint.Draining() {
		t.Fatal("endpoint of a removed cluster was never drained")
	}
}

func TestPanicThresholdKeepsClusterRoutable(t *testing.T) {
	m := NewManager(quiet())
	now := time.Now()
	snap := snapshot("v1", "http://a:80", "http://b:80", "http://c:80", "http://d:80")
	snap.Clusters[0].PanicThreshold = 0.5
	if _, err := m.Apply(context.Background(), snap, now); err != nil {
		t.Fatal(err)
	}
	cl, _ := m.Get("api")
	eps := cl.Endpoints()

	// Two of four unhealthy: exactly at the threshold, not panicking, and only
	// healthy endpoints are selected.
	eps[0].SetHealthy(false, now)
	eps[1].SetHealthy(false, now)
	for i := 0; i < 20; i++ {
		res, err := cl.Pick("", now, nil)
		if err != nil {
			t.Fatal(err)
		}
		if res.Panic {
			t.Fatal("entered panic mode at exactly the threshold")
		}
		if !res.Endpoint.Healthy() {
			t.Fatal("selected an unhealthy endpoint outside panic mode")
		}
	}

	// Three of four unhealthy: below the threshold. Rather than funnel all load
	// onto the single survivor, spread it across everything.
	eps[2].SetHealthy(false, now)
	seen := map[string]bool{}
	for i := 0; i < 40; i++ {
		res, err := cl.Pick("", now, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !res.Panic {
			t.Fatal("did not enter panic mode below the threshold")
		}
		seen[res.Endpoint.Addr] = true
	}
	if len(seen) < 4 {
		t.Fatalf("panic mode spread load over %d endpoints, want all 4", len(seen))
	}
}

func TestPickExcludesAlreadyTriedEndpoints(t *testing.T) {
	m := NewManager(quiet())
	now := time.Now()
	if _, err := m.Apply(context.Background(), snapshot("v1", "http://a:80", "http://b:80", "http://c:80"), now); err != nil {
		t.Fatal(err)
	}
	cl, _ := m.Get("api")

	exclude := map[string]bool{"http://a:80": true, "http://b:80": true}
	for i := 0; i < 20; i++ {
		res, err := cl.Pick("", now, exclude)
		if err != nil {
			t.Fatal(err)
		}
		if res.Endpoint.Addr != "http://c:80" {
			t.Fatalf("retry landed on an already-tried endpoint %s", res.Endpoint.Addr)
		}
	}

	// When every endpoint has been tried, fall back to the full set rather than
	// failing the request outright.
	all := map[string]bool{"http://a:80": true, "http://b:80": true, "http://c:80": true}
	if _, err := cl.Pick("", now, all); err != nil {
		t.Fatalf("exhausting the exclusion set made the cluster unroutable: %v", err)
	}
}

func TestSlowStartRampsWeightOverWindow(t *testing.T) {
	m := NewManager(quiet())
	now := time.Now()
	snap := snapshot("v1", "http://a:80")
	snap.Clusters[0].SlowStart = config.Duration(10 * time.Second)
	snap.Clusters[0].SlowStartMinWeight = 0.1
	if _, err := m.Apply(context.Background(), snap, now); err != nil {
		t.Fatal(err)
	}
	e := func() *Endpoint { cl, _ := m.Get("api"); return cl.Endpoints()[0] }()

	if w := e.Weight(now, 10*time.Second, 0.1); w != 0.1 {
		t.Fatalf("weight at t=0 is %.2f, want the 0.1 floor", w)
	}
	if w := e.Weight(now.Add(5*time.Second), 10*time.Second, 0.1); w < 0.49 || w > 0.51 {
		t.Fatalf("weight at half the window is %.2f, want about 0.5", w)
	}
	if w := e.Weight(now.Add(11*time.Second), 10*time.Second, 0.1); w != 1 {
		t.Fatalf("weight past the window is %.2f, want 1", w)
	}
}
