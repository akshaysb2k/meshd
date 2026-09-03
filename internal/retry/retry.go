// Package retry implements retry policy evaluation, jittered backoff and the
// retry budget.
//
// The budget is the important part. A fixed max_attempts turns a partial outage
// into a total one: backends slow down, every client retries, load multiplies,
// and the remaining capacity dies. A budget expressed as a percentage of the
// cluster's active requests self-limits -- generous when things are healthy,
// clamped hard when they are not.
package retry

import (
	"math/rand/v2"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/meshd/meshd/internal/config"
)

// Policy is the resolved retry configuration for a route.
type Policy struct {
	On                 map[string]bool
	MaxAttempts        int
	PerTryTimeout      time.Duration
	BaseBackoff        time.Duration
	MaxBackoff         time.Duration
	RetryNonIdempotent bool
}

// NewPolicy resolves defaults over a route's retry config. A nil config yields
// a nil Policy, meaning no retries.
func NewPolicy(c *config.RetryPolicy) *Policy {
	if c == nil {
		return nil
	}
	on := map[string]bool{}
	for _, o := range c.On {
		on[strings.ToLower(strings.TrimSpace(o))] = true
	}
	if len(on) == 0 {
		on["5xx"] = true
		on["connect-failure"] = true
	}
	max := c.MaxAttempts
	if max < 1 {
		max = 1
	}
	return &Policy{
		On:                 on,
		MaxAttempts:        max,
		PerTryTimeout:      c.PerTryTimeout.Or(0),
		BaseBackoff:        c.BaseBackoff.Or(10 * time.Millisecond),
		MaxBackoff:         c.MaxBackoff.Or(250 * time.Millisecond),
		RetryNonIdempotent: c.RetryNonIdempotent,
	}
}

// Idempotent reports whether a method is safe to replay by default. POST and
// PATCH are excluded unless the route explicitly opts in.
func Idempotent(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace, http.MethodPut, http.MethodDelete:
		return true
	}
	return false
}

// Retryable decides whether a completed attempt should be repeated.
func (p *Policy) Retryable(method string, status int, transportErr bool) bool {
	if p == nil {
		return false
	}
	if !Idempotent(method) && !p.RetryNonIdempotent {
		return false
	}
	if transportErr {
		return p.On["connect-failure"] || p.On["reset"] || p.On["gateway-error"] || p.On["5xx"]
	}
	if status >= 500 && p.On["5xx"] {
		return true
	}
	if p.On["gateway-error"] && (status == 502 || status == 503 || status == 504) {
		return true
	}
	if p.On["retriable-4xx"] && status == 409 {
		return true
	}
	return false
}

// Backoff returns the delay before attempt n (1-indexed) using full jitter:
// a uniform draw from [0, min(max, base*2^(n-1))]. Full jitter beats
// equal-jitter here because the point is to decorrelate clients, not to
// guarantee a minimum wait.
func (p *Policy) Backoff(attempt int, rng *rand.Rand) time.Duration {
	if p == nil || attempt < 1 {
		return 0
	}
	ceiling := p.BaseBackoff << uint(attempt-1)
	if ceiling > p.MaxBackoff || ceiling <= 0 {
		ceiling = p.MaxBackoff
	}
	if ceiling <= 0 {
		return 0
	}
	if rng == nil {
		return time.Duration(rand.Int64N(int64(ceiling)))
	}
	return time.Duration(rng.Int64N(int64(ceiling)))
}

// Budget bounds concurrent retries against a cluster as a fraction of its
// active requests.
type Budget struct {
	percent float64
	min     int64

	active  atomic.Int64
	retries atomic.Int64

	granted atomic.Int64
	denied  atomic.Int64
}

// NewBudget builds a Budget. A zero percent disables budgeting and falls back to
// the per-request attempt cap alone.
func NewBudget(percent float64, min int64) *Budget {
	if min <= 0 {
		min = 3
	}
	return &Budget{percent: percent, min: min}
}

// TrackRequest marks a request as active for the lifetime of the returned
// release function. The budget scales with this count.
func (b *Budget) TrackRequest() func() {
	b.active.Add(1)
	var done atomic.Bool
	return func() {
		if done.CompareAndSwap(false, true) {
			b.active.Add(-1)
		}
	}
}

// Allowed is the current ceiling on concurrent retries.
func (b *Budget) Allowed() int64 {
	if b.percent <= 0 {
		return 1 << 30
	}
	scaled := int64(float64(b.active.Load()) * b.percent / 100.0)
	if scaled < b.min {
		return b.min
	}
	return scaled
}

// TryAcquire reserves a retry slot, returning false when the budget is spent.
func (b *Budget) TryAcquire() (release func(), ok bool) {
	limit := b.Allowed()
	for {
		cur := b.retries.Load()
		if cur >= limit {
			b.denied.Add(1)
			return nil, false
		}
		if b.retries.CompareAndSwap(cur, cur+1) {
			b.granted.Add(1)
			var done atomic.Bool
			return func() {
				if done.CompareAndSwap(false, true) {
					b.retries.Add(-1)
				}
			}, true
		}
	}
}

// Stats reports budget occupancy.
func (b *Budget) Stats() (active, retries, allowed, granted, denied int64) {
	return b.active.Load(), b.retries.Load(), b.Allowed(), b.granted.Load(), b.denied.Load()
}
