package balancer

import (
	"fmt"
	"math"
	"math/rand/v2"
	"testing"
)

func cands(n int, weight float64) []Candidate {
	out := make([]Candidate, n)
	for i := range out {
		out[i] = Candidate{Key: fmt.Sprintf("ep-%d", i), Weight: weight}
	}
	return out
}

func TestRoundRobinEqualWeightsIsUniform(t *testing.T) {
	rr := NewRoundRobin()
	c := cands(4, 1)
	counts := map[string]int{}
	for i := 0; i < 4000; i++ {
		idx, err := rr.Pick(c, "")
		if err != nil {
			t.Fatal(err)
		}
		counts[c[idx].Key]++
	}
	for k, v := range counts {
		if v != 1000 {
			t.Fatalf("endpoint %s got %d picks, want exactly 1000", k, v)
		}
	}
}

func TestRoundRobinHonoursSlowStartWeight(t *testing.T) {
	rr := NewRoundRobin()
	c := cands(3, 1)
	c[2].Weight = 0.1 // ramping endpoint

	counts := map[string]int{}
	for i := 0; i < 2100; i++ {
		idx, _ := rr.Pick(c, "")
		counts[c[idx].Key]++
	}
	ramping := counts["ep-2"]
	want := 2100 * 0.1 / 2.1
	if math.Abs(float64(ramping)-want) > want*0.1 {
		t.Fatalf("ramping endpoint got %d picks, want about %.0f", ramping, want)
	}
}

func TestRoundRobinIsSmoothNotBursty(t *testing.T) {
	// A ramping endpoint must not receive its whole share as one burst; that
	// would defeat the point of slow start.
	rr := NewRoundRobin()
	c := cands(2, 1)
	c[1].Weight = 0.25

	var maxRun int
	run := 0
	for i := 0; i < 200; i++ {
		idx, _ := rr.Pick(c, "")
		if idx == 1 {
			run++
			if run > maxRun {
				maxRun = run
			}
		} else {
			run = 0
		}
	}
	if maxRun > 1 {
		t.Fatalf("low-weight endpoint received a run of %d consecutive picks", maxRun)
	}
}

func TestLeastRequestPrefersIdleEndpoint(t *testing.T) {
	lr := NewLeastRequest(2).WithRand(rand.New(rand.NewPCG(1, 2)))
	c := []Candidate{
		{Key: "busy", Active: 100, Weight: 1},
		{Key: "idle", Active: 0, Weight: 1},
	}
	idle := 0
	for i := 0; i < 1000; i++ {
		idx, _ := lr.Pick(c, "")
		if c[idx].Key == "idle" {
			idle++
		}
	}
	// With two candidates, both are sampled almost every round, so the idle one
	// should win nearly always.
	if idle < 950 {
		t.Fatalf("idle endpoint won only %d/1000 times", idle)
	}
}

func TestLeastRequestScalesLoadByWeight(t *testing.T) {
	lr := NewLeastRequest(2).WithRand(rand.New(rand.NewPCG(7, 7)))
	// The ramping endpoint is idle but weighted at 0.05, so its effective load
	// (1/0.05 = 20) should lose to a warm endpoint carrying 5 requests (6/1).
	c := []Candidate{
		{Key: "warm", Active: 5, Weight: 1},
		{Key: "ramping", Active: 0, Weight: 0.05},
	}
	ramping := 0
	for i := 0; i < 1000; i++ {
		idx, _ := lr.Pick(c, "")
		if c[idx].Key == "ramping" {
			ramping++
		}
	}
	if ramping > 50 {
		t.Fatalf("ramping endpoint won %d/1000 times, slow start weight ignored", ramping)
	}
}

func TestRingHashIsStableAndAffine(t *testing.T) {
	rh := NewRingHash(100)
	c := cands(5, 1)

	first := map[string]string{}
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("session-%d", i)
		idx, err := rh.Pick(c, key)
		if err != nil {
			t.Fatal(err)
		}
		first[key] = c[idx].Key
	}
	// Same key, same endpoint, every time.
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("session-%d", i)
		idx, _ := rh.Pick(c, key)
		if c[idx].Key != first[key] {
			t.Fatalf("key %s moved from %s to %s with no membership change", key, first[key], c[idx].Key)
		}
	}
}

func TestRingHashMinimisesRemapOnEndpointLoss(t *testing.T) {
	rh := NewRingHash(200)
	full := cands(5, 1)

	before := map[string]string{}
	for i := 0; i < 5000; i++ {
		key := fmt.Sprintf("session-%d", i)
		idx, _ := rh.Pick(full, key)
		before[key] = full[idx].Key
	}

	reduced := full[:4] // ep-4 leaves
	moved := 0
	for i := 0; i < 5000; i++ {
		key := fmt.Sprintf("session-%d", i)
		idx, _ := rh.Pick(reduced, key)
		if reduced[idx].Key != before[key] {
			moved++
		}
	}
	// Removing 1 of 5 should move roughly a fifth of keys. Anything near 100%
	// means the ring degenerated into modulo hashing.
	frac := float64(moved) / 5000
	if frac > 0.35 {
		t.Fatalf("removing one of five endpoints remapped %.1f%% of keys", frac*100)
	}
	t.Logf("remapped %.1f%% of keys after losing 1 of 5 endpoints", frac*100)
}

func TestPickersRejectEmptyCandidateSet(t *testing.T) {
	for _, p := range []Picker{NewRoundRobin(), NewLeastRequest(2), NewRingHash(10)} {
		if _, err := p.Pick(nil, "k"); err != ErrNoEndpoints {
			t.Fatalf("%s: got %v, want ErrNoEndpoints", p.Name(), err)
		}
	}
}
