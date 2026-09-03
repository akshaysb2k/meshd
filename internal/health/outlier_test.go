package health

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/meshd/meshd/internal/clock"
	"github.com/meshd/meshd/internal/cluster"
	"github.com/meshd/meshd/internal/config"
	"github.com/meshd/meshd/internal/metrics"
)

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newTestCluster builds a cluster with n endpoints and no real backends behind
// them. Nothing here touches the network: the detector's input is the counters
// on each endpoint, so the whole scenario is pure state.
func newTestCluster(t *testing.T, n int, outlier config.Outlier) (*cluster.Cluster, *Detector, *clock.Fake) {
	t.Helper()
	eps := make([]string, n)
	for i := range eps {
		eps[i] = "http://10.0.0." + string(rune('1'+i)) + ":80"
	}
	snap := &config.Snapshot{
		Version: "test",
		Clusters: []config.Cluster{{
			Name: "api", Policy: "round_robin", Endpoints: eps,
			Outlier: &outlier,
		}},
		Routes: []config.Route{{Name: "r", Cluster: "api", Match: config.RouteMatch{PathPrefix: "/"}}},
	}
	if err := snap.Validate(); err != nil {
		t.Fatal(err)
	}
	clk := clock.NewFake(epoch)
	m := cluster.NewManager(quietLogger())
	if _, err := m.Apply(context.Background(), snap, clk.Now()); err != nil {
		t.Fatal(err)
	}
	cl, _ := m.Get("api")
	d := NewDetector(cl, outlier, clk, quietLogger(), NewMetrics(metrics.New()))
	return cl, d, clk
}

func failN(e *cluster.Endpoint, n int) {
	for i := 0; i < n; i++ {
		e.RecordResult(503, false)
	}
}

func ejectedCount(cl *cluster.Cluster, now time.Time) int {
	n := 0
	for _, e := range cl.Endpoints() {
		if e.Ejected(now) {
			n++
		}
	}
	return n
}

func TestOutlierEjectsAfterConsecutiveFailures(t *testing.T) {
	cl, d, clk := newTestCluster(t, 3, config.Outlier{
		Consecutive5xx:     3,
		BaseEjectionTime:   config.Duration(5 * time.Second),
		MaxEjectionPercent: 50,
	})
	eps := cl.Endpoints()

	// Two failures is below the threshold: nothing should happen.
	failN(eps[0], 2)
	d.Sweep()
	if ejectedCount(cl, clk.Now()) != 0 {
		t.Fatal("ejected below the consecutive failure threshold")
	}

	// A success resets the run, proving the counter is consecutive and not
	// cumulative.
	eps[0].RecordResult(200, false)
	failN(eps[0], 2)
	d.Sweep()
	if ejectedCount(cl, clk.Now()) != 0 {
		t.Fatal("a success failed to reset the consecutive failure counter")
	}

	failN(eps[0], 3)
	d.Sweep()
	if !eps[0].Ejected(clk.Now()) {
		t.Fatal("endpoint was not ejected after reaching the threshold")
	}
	if eps[0].Available(clk.Now()) {
		t.Fatal("an ejected endpoint is still available for balancing")
	}
}

func TestOutlierEjectionBudgetProtectsTheCluster(t *testing.T) {
	// Every endpoint is failing, which is what a bad deploy looks like. The
	// detector must refuse to eject the whole fleet.
	cl, d, clk := newTestCluster(t, 4, config.Outlier{
		Consecutive5xx:     3,
		BaseEjectionTime:   config.Duration(5 * time.Second),
		MaxEjectionPercent: 50,
	})
	for _, e := range cl.Endpoints() {
		failN(e, 5)
	}
	d.Sweep()

	got := ejectedCount(cl, clk.Now())
	if got != 2 {
		t.Fatalf("ejected %d of 4 endpoints, want 2 (50%% cap)", got)
	}
	// The survivors must still be routable, degraded or not.
	if _, err := cl.Pick("", clk.Now(), nil); err != nil {
		t.Fatalf("cluster became unroutable under total failure: %v", err)
	}
}

func TestOutlierUnejectsAfterWindowAndRestartsSlowStart(t *testing.T) {
	cl, d, clk := newTestCluster(t, 3, config.Outlier{
		Consecutive5xx:     3,
		BaseEjectionTime:   config.Duration(5 * time.Second),
		MaxEjectionPercent: 50,
	})
	eps := cl.Endpoints()
	failN(eps[0], 3)
	d.Sweep()
	if !eps[0].Ejected(clk.Now()) {
		t.Fatal("not ejected")
	}

	clk.Advance(4 * time.Second)
	d.Sweep()
	if !eps[0].Ejected(clk.Now()) {
		t.Fatal("unejected before the ejection window expired")
	}

	clk.Advance(2 * time.Second)
	d.Sweep()
	if eps[0].Ejected(clk.Now()) {
		t.Fatal("still ejected after the window expired")
	}
	// Coming back must restart the slow start ramp, not resume at full weight.
	if w := eps[0].Weight(clk.Now(), 10*time.Second, 0.05); w >= 1 {
		t.Fatalf("recovered endpoint returned at full weight %.2f, slow start skipped", w)
	}
}

func TestOutlierEjectionWindowGrowsForRepeatOffenders(t *testing.T) {
	cl, d, clk := newTestCluster(t, 3, config.Outlier{
		Consecutive5xx:     3,
		BaseEjectionTime:   config.Duration(2 * time.Second),
		MaxEjectionTime:    config.Duration(30 * time.Second),
		MaxEjectionPercent: 50,
	})
	e := cl.Endpoints()[0]

	var windows []time.Duration
	for round := 0; round < 3; round++ {
		failN(e, 3)
		d.Sweep()
		until := e.EjectedUntil()
		if until.IsZero() {
			t.Fatalf("round %d: not ejected", round)
		}
		windows = append(windows, until.Sub(clk.Now()))
		clk.Advance(until.Sub(clk.Now()) + time.Second)
		d.Sweep()
	}
	for i := 1; i < len(windows); i++ {
		if windows[i] <= windows[i-1] {
			t.Fatalf("ejection windows did not grow: %v", windows)
		}
	}
	t.Logf("ejection windows across repeat offences: %v", windows)
}

func TestScenarioIsDeterministic(t *testing.T) {
	// The same scenario, replayed, must produce byte-identical state. This is
	// the property that makes a failing seed reproducible.
	run := func() []string {
		cl, d, clk := newTestCluster(t, 4, config.Outlier{
			Consecutive5xx:     3,
			BaseEjectionTime:   config.Duration(5 * time.Second),
			MaxEjectionPercent: 50,
		})
		var trace []string
		for step := 0; step < 20; step++ {
			for i, e := range cl.Endpoints() {
				if (step+i)%3 == 0 {
					e.RecordResult(503, false)
				} else {
					e.RecordResult(200, false)
				}
			}
			clk.Advance(500 * time.Millisecond)
			d.Sweep()
			for _, e := range cl.Endpoints() {
				trace = append(trace, e.Addr+":"+boolStr(e.Ejected(clk.Now())))
			}
		}
		return trace
	}
	a, b := run(), run()
	if len(a) != len(b) {
		t.Fatal("trace length differs between runs")
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("replay diverged at step %d: %q vs %q", i, a[i], b[i])
		}
	}
}

func boolStr(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

func TestDetectorRunLoopRespectsContextCancellation(t *testing.T) {
	cl, d, clk := newTestCluster(t, 3, config.Outlier{
		Consecutive5xx:   3,
		Interval:         config.Duration(time.Second),
		BaseEjectionTime: config.Duration(5 * time.Second),
	})
	_ = cl
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { d.Run(ctx); close(done) }()

	clk.Advance(2 * time.Second)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("detector did not exit on context cancellation")
	}
}
