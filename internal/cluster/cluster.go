package cluster

import (
	"sync"
	"time"

	"github.com/meshd/meshd/internal/balancer"
	"github.com/meshd/meshd/internal/breaker"
	"github.com/meshd/meshd/internal/config"
	"github.com/meshd/meshd/internal/retry"
)

// Cluster is a named group of endpoints plus the policy for reaching them.
//
// Everything mutable lives under mu. A config push runs concurrently with live
// traffic, so cfg, picker, breaker and endpoints must be swapped as one
// consistent unit -- a request that read the new endpoint list against the old
// picker would be a subtle and very hard to reproduce bug.
type Cluster struct {
	Name string

	mu        sync.RWMutex
	cfg       config.Cluster
	picker    balancer.Picker
	breaker   *breaker.Breaker
	budget    *retry.Budget
	endpoints []*Endpoint

	// panicMode records whether the last Pick ran with health status ignored.
	panicMode atomicBool
}

type atomicBool struct {
	mu sync.RWMutex
	v  bool
}

func (a *atomicBool) Store(v bool) { a.mu.Lock(); a.v = v; a.mu.Unlock() }
func (a *atomicBool) Load() bool   { a.mu.RLock(); defer a.mu.RUnlock(); return a.v }

// Breaker returns the cluster's resource limits.
func (c *Cluster) Breaker() *breaker.Breaker {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.breaker
}

// Budget returns the cluster's retry budget.
func (c *Cluster) Budget() *retry.Budget {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.budget
}

// Config returns a copy of the cluster's configuration.
func (c *Cluster) Config() config.Cluster {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cfg
}

// Policy returns the active load balancing policy name.
func (c *Cluster) Policy() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.picker.Name()
}

// PanicMode reports whether the cluster is currently ignoring health status.
func (c *Cluster) PanicMode() bool { return c.panicMode.Load() }

// Endpoints returns a snapshot of the cluster's endpoints.
func (c *Cluster) Endpoints() []*Endpoint {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return append([]*Endpoint(nil), c.endpoints...)
}

// PickResult is one balancing decision.
type PickResult struct {
	Endpoint *Endpoint
	// Panic is true when health status was ignored to make this decision.
	Panic bool
}

// Pick chooses an endpoint for a request.
//
// The panic threshold is the subtle part. When the healthy fraction falls below
// it, health status is discarded and every endpoint becomes a candidate. That
// looks wrong until you consider the alternative: if 90% of a fleet is marked
// unhealthy, the health signal is more likely broken than the fleet is, and
// funnelling all traffic onto the remaining 10% guarantees those die too.
// Spreading load over degraded backends beats concentrating it on doomed ones.
func (c *Cluster) Pick(hashKey string, now time.Time, exclude map[string]bool) (PickResult, error) {
	// One read lock covers endpoints, config and picker, so a decision is always
	// made against a single coherent version of the cluster.
	c.mu.RLock()
	eps := c.endpoints
	slowStart := c.cfg.SlowStart.D()
	minWeight := c.cfg.SlowStartMinWeight
	threshold := c.cfg.PanicThreshold
	picker := c.picker
	c.mu.RUnlock()

	if len(eps) == 0 {
		return PickResult{}, balancer.ErrNoEndpoints
	}

	var available, all []*Endpoint
	var totalNonDraining int
	for _, e := range eps {
		if e.Draining() {
			continue
		}
		totalNonDraining++
		all = append(all, e)
		if e.Available(now) {
			available = append(available, e)
		}
	}
	if totalNonDraining == 0 {
		return PickResult{}, balancer.ErrNoEndpoints
	}

	if threshold == 0 {
		threshold = 0.5
	}
	panicking := float64(len(available))/float64(totalNonDraining) < threshold
	c.panicMode.Store(panicking)

	pool := available
	if panicking {
		pool = all
	}
	if len(pool) == 0 {
		return PickResult{}, balancer.ErrNoEndpoints
	}

	// Endpoints already tried by this request are excluded so a retry lands
	// somewhere new. If that empties the pool, fall back to the full set rather
	// than failing outright.
	if len(exclude) > 0 {
		var filtered []*Endpoint
		for _, e := range pool {
			if !exclude[e.Addr] {
				filtered = append(filtered, e)
			}
		}
		if len(filtered) > 0 {
			pool = filtered
		}
	}

	cands := make([]balancer.Candidate, len(pool))
	for i, e := range pool {
		cands[i] = balancer.Candidate{
			Key:    e.Addr,
			Active: e.Active(),
			Weight: e.Weight(now, slowStart, minWeight),
		}
	}
	idx, err := picker.Pick(cands, hashKey)
	if err != nil {
		return PickResult{}, err
	}
	return PickResult{Endpoint: pool[idx], Panic: panicking}, nil
}

// HealthSummary counts endpoints by state.
type HealthSummary struct {
	Total     int
	Healthy   int
	Ejected   int
	Draining  int
	PanicMode bool
}

// Summary reports the cluster's current health breakdown.
func (c *Cluster) Summary(now time.Time) HealthSummary {
	s := HealthSummary{PanicMode: c.panicMode.Load()}
	for _, e := range c.Endpoints() {
		s.Total++
		switch {
		case e.Draining():
			s.Draining++
		case e.Ejected(now):
			s.Ejected++
		case e.Healthy():
			s.Healthy++
		}
	}
	return s
}

// reconcileStats describes what one cluster's reconcile changed.
type reconcileStats struct {
	added    int
	retained int
	removed  []*Endpoint
}

// reconcile atomically installs new config and a new endpoint set, reusing the
// Endpoint object for every address that survives. It returns the endpoints
// that were removed so the caller can drain them.
func (c *Cluster) reconcile(cc config.Cluster, now time.Time, dial Dialer) (reconcileStats, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var st reconcileStats

	// Rebuild the picker only when the policy actually changed, so a round
	// robin cursor or a hash ring is not reset by an unrelated endpoint edit.
	if c.picker == nil || c.cfg.Policy != cc.Policy || c.cfg.RingReplicas != cc.RingReplicas {
		c.picker = balancer.New(cc.Policy, cc.RingReplicas)
	}
	if c.breaker == nil || !sameBreaker(c.cfg.CircuitBreaker, cc.CircuitBreaker) {
		c.breaker = breaker.New(cc.CircuitBreaker)
	}
	if c.budget == nil {
		c.budget = retry.NewBudget(defaultBudgetPercent, defaultBudgetFloor)
	}
	c.cfg = cc

	byAddr := make(map[string]*Endpoint, len(c.endpoints))
	for _, e := range c.endpoints {
		byAddr[e.Addr] = e
	}

	wanted := make(map[string]bool, len(cc.Endpoints))
	eps := make([]*Endpoint, 0, len(cc.Endpoints))
	for _, addr := range cc.Endpoints {
		wanted[addr] = true
		if e, ok := byAddr[addr]; ok {
			st.retained++
			eps = append(eps, e)
			continue
		}
		e, err := newEndpoint(addr, &cc, now, dial)
		if err != nil {
			return st, err
		}
		st.added++
		eps = append(eps, e)
	}
	for addr, e := range byAddr {
		if !wanted[addr] {
			st.removed = append(st.removed, e)
		}
	}
	c.endpoints = eps
	return st, nil
}

// defaultBudgetPercent and defaultBudgetFloor apply when a route does not set
// its own. Twenty percent is generous in steady state and hard-limiting during
// an outage, which is exactly the shape a retry budget should have.
const (
	defaultBudgetPercent = 20.0
	defaultBudgetFloor   = 3
)

func sameBreaker(a, b *config.CircuitBreaker) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}
