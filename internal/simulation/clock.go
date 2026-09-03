package simulation

import (
	"time"

	"github.com/meshd/meshd/internal/clock"
)

// simClock is the fake clock with one change: waiting advances time instead of
// blocking on it.
//
// In a synchronous simulation a real wait is a deadlock. A request that hits a
// retry backoff calls After and parks, but the only thing that could advance
// the clock is the scenario driver, which is itself blocked inside that
// request. Advancing on demand is also the more faithful model: in discrete
// event terms the delay genuinely happens, it simply costs no wall clock.
//
// The one behaviour this cannot represent is genuine concurrency between a
// sleeping request and other work, so hedging (which races two attempts against
// a timer) is not meaningful under simulation and is exercised by the
// integration tests instead.
type simClock struct{ *clock.Fake }

// After advances simulated time by d and returns an already-fired channel.
func (c simClock) After(d time.Duration) <-chan time.Time {
	if d > 0 {
		c.Fake.Advance(d)
	}
	ch := make(chan time.Time, 1)
	ch <- c.Fake.Now()
	return ch
}

// Sleep advances simulated time by d.
func (c simClock) Sleep(d time.Duration) {
	if d > 0 {
		c.Fake.Advance(d)
	}
}
