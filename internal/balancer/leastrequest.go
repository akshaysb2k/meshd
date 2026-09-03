package balancer

import "math/rand/v2"

// LeastRequest implements power-of-N-choices least-request balancing.
//
// Scanning every endpoint for the true minimum is O(n) and, worse, is a
// herding hazard: every proxy instance computes the same minimum and stampedes
// the same backend. Sampling two at random and taking the lighter one lands
// within a few percent of optimal, costs O(1), and needs no coordination.
type LeastRequest struct {
	choices int
	rng     *rand.Rand
}

// NewLeastRequest returns a picker sampling n candidates per decision.
func NewLeastRequest(n int) *LeastRequest {
	if n < 2 {
		n = 2
	}
	return &LeastRequest{choices: n}
}

// WithRand fixes the random source, making selection reproducible in tests.
func (l *LeastRequest) WithRand(r *rand.Rand) *LeastRequest {
	l.rng = r
	return l
}

// Name identifies the policy.
func (l *LeastRequest) Name() string { return "least_request" }

// Pick samples candidates and returns the one with the lowest weighted load.
func (l *LeastRequest) Pick(cands []Candidate, _ string) (int, error) {
	n := len(cands)
	if n == 0 {
		return 0, ErrNoEndpoints
	}
	if n == 1 {
		return 0, nil
	}
	// Sampling must be without replacement. Drawing k independent indices means
	// that with n=2 and k=2 the same candidate is picked twice 50% of the time,
	// so the loaded endpoint still wins a quarter of all decisions -- the
	// algorithm silently degrades toward random.
	picks := l.sample(n)

	best := -1
	bestLoad := 0.0
	for _, idx := range picks {
		c := cands[idx]
		w := c.Weight
		if w <= 0 {
			w = 0.0001
		}
		// Dividing by weight is what makes slow start work here: a ramping
		// endpoint looks proportionally more loaded than it is.
		load := float64(c.Active+1) / w
		if best == -1 || load < bestLoad {
			best, bestLoad = idx, load
		}
	}
	return best, nil
}

// sample draws min(choices, n) distinct indices in [0,n).
func (l *LeastRequest) sample(n int) []int {
	k := l.choices
	if k > n {
		k = n
	}
	out := make([]int, 0, k)
	for len(out) < k {
		idx := l.intn(n)
		dup := false
		for _, v := range out {
			if v == idx {
				dup = true
				break
			}
		}
		if !dup {
			out = append(out, idx)
		}
	}
	return out
}

func (l *LeastRequest) intn(n int) int {
	if l.rng != nil {
		return l.rng.IntN(n)
	}
	return rand.IntN(n)
}
