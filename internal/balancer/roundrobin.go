package balancer

import "sync"

// RoundRobin implements smooth weighted round robin (the nginx algorithm).
//
// Naive weighted round robin bunches all of a heavy endpoint's turns together,
// which defeats the point of slow start: a ramping endpoint would still receive
// a burst. Smooth WRR interleaves selections so a 0.1-weight endpoint gets one
// request in ten spread evenly, not ten in a row every hundred.
type RoundRobin struct {
	mu      sync.Mutex
	current map[string]float64
}

// NewRoundRobin returns a smooth weighted round robin picker.
func NewRoundRobin() *RoundRobin {
	return &RoundRobin{current: map[string]float64{}}
}

// Name identifies the policy.
func (r *RoundRobin) Name() string { return "round_robin" }

// Pick selects the endpoint with the highest accumulated current weight, then
// subtracts the total weight from it.
func (r *RoundRobin) Pick(cands []Candidate, _ string) (int, error) {
	if len(cands) == 0 {
		return 0, ErrNoEndpoints
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	live := make(map[string]struct{}, len(cands))
	var total float64
	best, bestVal := -1, 0.0
	for i, c := range cands {
		live[c.Key] = struct{}{}
		w := c.Weight
		if w <= 0 {
			w = 0.0001
		}
		total += w
		cur := r.current[c.Key] + w
		r.current[c.Key] = cur
		if best == -1 || cur > bestVal {
			best, bestVal = i, cur
		}
	}
	// Drop state for endpoints that have left the cluster, so a flapping
	// endpoint does not return with a stale credit and steal a burst.
	for k := range r.current {
		if _, ok := live[k]; !ok {
			delete(r.current, k)
		}
	}
	r.current[cands[best].Key] = bestVal - total
	return best, nil
}
