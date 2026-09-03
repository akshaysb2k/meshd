package simulation

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/meshd/meshd/internal/config"
)

const endpointCount = 5

// chaosRun executes one fully deterministic scenario.
//
// One endpoint is designated the anchor and stays healthy for the whole run.
// That makes the central invariant unambiguous: however the other four fail,
// there is always one good endpoint reachable within the attempt budget, so a
// client-visible 5xx means the proxy lost a request it did not have to lose.
func chaosRun(t *testing.T, seed uint64, steps int) (*Sim, []Result) {
	t.Helper()
	snap := Snapshot(endpointCount, nil)
	s, err := New(seed, snap)
	if err != nil {
		t.Fatal(err)
	}
	addrs := s.Addrs()
	anchor := addrs[0]

	rng := rand.New(rand.NewPCG(seed, 0xdeadbeef))
	var results []Result

	for step := 0; step < steps; step++ {
		// Mutate one non-anchor host.
		if rng.Float64() < 0.35 {
			victim := addrs[1+rng.IntN(len(addrs)-1)]
			switch rng.IntN(3) {
			case 0:
				s.Net.Set(victim, func(h *HostState) { h.Down = false; h.ErrorRate = 0 })
			case 1:
				s.Net.Set(victim, func(h *HostState) { h.Down = false; h.ErrorRate = 1 })
			case 2:
				s.Net.Set(victim, func(h *HostState) { h.Down = true; h.ErrorRate = 0 })
			}
		}
		for i := 0; i < 8; i++ {
			results = append(results, s.Request("GET", "/x"))
		}
		s.Record(fmt.Sprintf("ejected=%d", s.EjectedCount()))
		s.Tick(500 * time.Millisecond)
	}
	_ = anchor
	return s, results
}

func TestSameSeedReplaysIdentically(t *testing.T) {
	for _, seed := range []uint64{1, 42, 4471} {
		a, ra := chaosRun(t, seed, 40)
		b, rb := chaosRun(t, seed, 40)

		ta, tb := a.Trace(), b.Trace()
		if len(ta) != len(tb) {
			t.Fatalf("seed %d: trace lengths differ", seed)
		}
		for i := range ta {
			if ta[i] != tb[i] {
				t.Fatalf("seed %d diverged at trace line %d:\n  run A: %s\n  run B: %s", seed, i, ta[i], tb[i])
			}
		}
		if len(ra) != len(rb) {
			t.Fatalf("seed %d: result counts differ", seed)
		}
		for i := range ra {
			if ra[i] != rb[i] {
				t.Fatalf("seed %d: request %d differed: %+v vs %+v", seed, i, ra[i], rb[i])
			}
		}
		// Even the network's hit distribution must match exactly.
		ha, hb := a.Net.Hits(), b.Net.Hits()
		for _, k := range sortedKeys(ha) {
			if ha[k] != hb[k] {
				t.Fatalf("seed %d: host %s took %d hits in run A and %d in run B", seed, k, ha[k], hb[k])
			}
		}
	}
}

func TestDifferentSeedsProduceDifferentScenarios(t *testing.T) {
	a, _ := chaosRun(t, 1, 40)
	b, _ := chaosRun(t, 2, 40)
	if a.TraceString() == b.TraceString() {
		t.Fatal("two different seeds produced identical scenarios; the harness is not exploring anything")
	}
}

// TestInvariantNoLostRequestsWhileAGoodEndpointExists is the headline property.
func TestInvariantNoLostRequestsWhileAGoodEndpointExists(t *testing.T) {
	for seed := uint64(1); seed <= 60; seed++ {
		s, results := chaosRun(t, seed, 30)
		for i, r := range results {
			if r.Status != 200 {
				t.Fatalf("seed %d: request %d returned %d (%s) despite a permanently healthy endpoint\n%s",
					seed, i, r.Status, r.Reason, s.TraceString())
			}
		}
	}
}

// TestInvariantEjectionNeverExceedsBudget checks the safety valve that stops a
// bad deploy from taking a cluster to zero.
func TestInvariantEjectionNeverExceedsBudget(t *testing.T) {
	maxAllowed := endpointCount * 50 / 100
	for seed := uint64(1); seed <= 60; seed++ {
		snap := Snapshot(endpointCount, nil)
		s, err := New(seed, snap)
		if err != nil {
			t.Fatal(err)
		}
		addrs := s.Addrs()
		// Break everything at once; this is the deploy-gone-wrong case.
		for _, a := range addrs {
			s.Net.Set(a, func(h *HostState) { h.ErrorRate = 1 })
		}
		for step := 0; step < 40; step++ {
			for i := 0; i < 8; i++ {
				s.Request("GET", "/x")
			}
			s.Tick(500 * time.Millisecond)
			if got := s.EjectedCount(); got > maxAllowed {
				t.Fatalf("seed %d step %d: %d of %d endpoints ejected, cap is %d",
					seed, step, got, endpointCount, maxAllowed)
			}
		}
	}
}

// TestAttemptCapBoundsAmplificationWithoutConcurrency records something the
// simulation makes obvious and an integration test does not: the retry budget
// is a concurrency gate, not a rate limit.
//
// Requests here are issued one at a time, so a cluster's active-request count
// is always one and the budget never binds -- each request simply gets its
// slot, uses it, and hands it back. Amplification is therefore capped only by
// max_attempts, and lands exactly on it. Under real concurrent load the budget
// is what does the work: the integration test measures 1.23x against the same
// 4x attempt cap. Both facts matter, and neither test shows both.
func TestAttemptCapBoundsAmplificationWithoutConcurrency(t *testing.T) {
	for seed := uint64(1); seed <= 20; seed++ {
		snap := Snapshot(endpointCount, func(c *config.Cluster, r *config.Route) {
			r.Retry.BudgetPercent = 20
			r.Retry.MinRetryConcurrency = 1
		})
		s, err := New(seed, snap)
		if err != nil {
			t.Fatal(err)
		}
		for _, a := range s.Addrs() {
			s.Net.Set(a, func(h *HostState) { h.ErrorRate = 1 })
		}
		const requests = 200
		for i := 0; i < requests; i++ {
			s.Request("GET", "/x")
		}
		amp := float64(s.UpstreamAttempts()) / requests
		if amp > float64(endpointCount) {
			t.Fatalf("seed %d: amplification %.2fx exceeds the attempt cap", seed, amp)
		}
		if seed == 1 {
			t.Logf("sequential total outage: %d requests, %d upstream attempts (%.2fx against a %dx attempt cap); the budget cannot bind without concurrency",
				requests, s.UpstreamAttempts(), amp, endpointCount)
		}
	}
}

// TestLyingHealthEndpointIsCaughtOnlyByOutlierDetection is the scenario that
// justifies running both detection mechanisms.
func TestLyingHealthEndpointIsCaughtOnlyByOutlierDetection(t *testing.T) {
	snap := Snapshot(4, nil)
	s, err := New(7, snap)
	if err != nil {
		t.Fatal(err)
	}
	addrs := s.Addrs()
	liar := addrs[1]
	// /healthz returns 200; every real request fails.
	s.Net.Set(liar, func(h *HostState) { h.ErrorRate = 1; h.HealthLies = true })

	var liarEp interface {
		Healthy() bool
		Ejected(time.Time) bool
		EjectionCount() int64
	}
	for _, c := range s.clusters {
		for _, e := range c.Endpoints() {
			if e.Addr == liar {
				liarEp = e
			}
		}
	}

	for step := 0; step < 20; step++ {
		for i := 0; i < 8; i++ {
			s.Request("GET", "/x")
		}
		s.Tick(500 * time.Millisecond)
	}

	if !liarEp.Healthy() {
		t.Fatal("active health checking somehow caught a backend whose probe endpoint lies")
	}
	if liarEp.EjectionCount() == 0 {
		t.Fatal("outlier detection failed to eject a backend that passes probes but fails real traffic")
	}
	probes := s.Net.Probes()[short(liar)]
	t.Logf("liar answered %d probes with 200 and stayed 'healthy', but was ejected %d time(s) by real traffic",
		probes, liarEp.EjectionCount())
}

// TestRecoveredEndpointRampsBackViaSlowStart checks that a backend returning to
// service is not immediately handed a full share.
func TestRecoveredEndpointRampsBackViaSlowStart(t *testing.T) {
	snap := Snapshot(3, func(c *config.Cluster, r *config.Route) {
		c.SlowStart = config.Duration(20 * time.Second)
		c.SlowStartMinWeight = 0.05
		c.Outlier.BaseEjectionTime = config.Duration(2 * time.Second)
	})
	s, err := New(11, snap)
	if err != nil {
		t.Fatal(err)
	}
	victim := s.Addrs()[2]
	ep := s.Endpoint(victim)

	s.Net.Set(victim, func(h *HostState) { h.ErrorRate = 1 })
	for step := 0; step < 6; step++ {
		for i := 0; i < 8; i++ {
			s.Request("GET", "/x")
		}
		s.Tick(500 * time.Millisecond)
	}
	// Assert on the ejection counter, not on current state: by this point the
	// first ejection window may already have expired and the endpoint returned
	// to rotation, which is correct behaviour and not what is being tested.
	if ep.EjectionCount() == 0 {
		t.Fatalf("the failing endpoint was never ejected\n%s", s.TraceString())
	}

	// Recover it and tick until it is genuinely back in rotation: the ejection
	// window must expire AND active health checking must see healthy_threshold
	// consecutive good probes. Measuring before both happen would give a share
	// of zero and pass this test for entirely the wrong reason.
	s.Net.Set(victim, func(h *HostState) { h.ErrorRate = 0 })
	for step := 0; step < 40 && !ep.Available(s.Clock.Now()); step++ {
		s.Tick(500 * time.Millisecond)
	}
	if !ep.Available(s.Clock.Now()) {
		t.Fatalf("the recovered endpoint never returned to rotation (healthy=%v ejected=%v)\n%s",
			ep.Healthy(), ep.Ejected(s.Clock.Now()), s.TraceString())
	}
	// Compare against relative weight, not absolute. Every endpoint begins its
	// slow start ramp when it is created, so early in a run the untouched
	// endpoints are themselves partway up the ramp rather than at 1.0. That is
	// correct -- they all ramp together, so the distribution between them is
	// unaffected -- but it means the expected share is the victim's weight over
	// the sum of all weights, not over the endpoint count.
	now := s.Clock.Now()
	cfg := s.clusters[0].Config()
	var total float64
	for _, e := range s.clusters[0].Endpoints() {
		total += e.Weight(now, cfg.SlowStart.D(), cfg.SlowStartMinWeight)
	}
	weight := ep.Weight(now, cfg.SlowStart.D(), cfg.SlowStartMinWeight)

	const requests = 300
	expected := float64(requests) * weight / total
	even := float64(requests) / 3

	before := s.Net.Hits()[short(victim)]
	for i := 0; i < requests; i++ {
		s.Request("GET", "/x")
	}
	share := float64(s.Net.Hits()[short(victim)] - before)

	if share == 0 {
		t.Fatal("recovered endpoint received no traffic at all; it is not actually back in rotation")
	}
	if share >= even {
		t.Fatalf("recovered endpoint took %.0f of %d requests, an even share or more; slow start is not ramping", share, requests)
	}
	if share < expected*0.6 || share > expected*1.4 {
		t.Fatalf("recovered endpoint took %.0f of %d requests; its weight of %.3f (of %.3f total) predicts %.1f",
			share, requests, weight, total, expected)
	}
	t.Logf("recovered endpoint: weight %.3f of %.3f total, took %.0f of %d requests (predicted %.1f, even split %.0f)",
		weight, total, share, requests, expected, even)
}
