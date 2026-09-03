// Package breaker implements circuit breaking as resource limits. Exceeding a
// limit sheds the request immediately rather than queueing it, which is what
// actually stops one slow dependency from consuming the whole proxy.
//
// This is deliberately not the closed/open/half-open state machine people
// usually mean by "circuit breaker" -- that behaviour is per-endpoint and lives
// in the outlier detector. These are per-cluster concurrency ceilings.
package breaker

import (
	"errors"
	"sync/atomic"

	"github.com/meshd/meshd/internal/config"
)

// ErrOverflow is returned when a limit would be exceeded.
var ErrOverflow = errors.New("circuit breaker: limit exceeded")

// Breaker holds the concurrency ceilings for one cluster.
type Breaker struct {
	maxPending  int64
	maxRequests int64
	maxRetries  int64

	pending  atomic.Int64
	requests atomic.Int64
	retries  atomic.Int64

	overflows atomic.Int64
}

// New builds a Breaker from config. A zero or negative limit means unlimited.
func New(c *config.CircuitBreaker) *Breaker {
	if c == nil {
		return &Breaker{}
	}
	return &Breaker{
		maxPending:  c.MaxPendingRequests,
		maxRequests: c.MaxRequests,
		maxRetries:  c.MaxRetries,
	}
}

// AcquirePending reserves a slot for a request that has been admitted but has
// not yet been dispatched to an endpoint.
func (b *Breaker) AcquirePending() (release func(), err error) {
	if !tryAcquire(&b.pending, b.maxPending) {
		b.overflows.Add(1)
		return nil, ErrOverflow
	}
	return releaseOnce(&b.pending), nil
}

// AcquireRequest reserves a slot for an in-flight upstream request.
func (b *Breaker) AcquireRequest() (release func(), err error) {
	if !tryAcquire(&b.requests, b.maxRequests) {
		b.overflows.Add(1)
		return nil, ErrOverflow
	}
	return releaseOnce(&b.requests), nil
}

// AcquireRetry reserves a slot for a retry attempt. Retries are capped
// separately so a cluster cannot spend its entire request budget on them.
func (b *Breaker) AcquireRetry() (release func(), ok bool) {
	if !tryAcquire(&b.retries, b.maxRetries) {
		b.overflows.Add(1)
		return nil, false
	}
	return releaseOnce(&b.retries), true
}

// Stats reports current occupancy for the admin endpoint and metrics.
func (b *Breaker) Stats() (pending, requests, retries, overflows int64) {
	return b.pending.Load(), b.requests.Load(), b.retries.Load(), b.overflows.Load()
}

func tryAcquire(counter *atomic.Int64, limit int64) bool {
	if limit <= 0 {
		counter.Add(1)
		return true
	}
	for {
		cur := counter.Load()
		if cur >= limit {
			return false
		}
		if counter.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

func releaseOnce(counter *atomic.Int64) func() {
	var done atomic.Bool
	return func() {
		if done.CompareAndSwap(false, true) {
			counter.Add(-1)
		}
	}
}
