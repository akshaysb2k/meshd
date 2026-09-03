// Package clock abstracts time so that health checking, outlier ejection and
// retry backoff can be driven deterministically in tests.
package clock

import (
	"sort"
	"sync"
	"time"
)

// Clock is the subset of the time package the proxy depends on.
type Clock interface {
	Now() time.Time
	NewTicker(d time.Duration) Ticker
	After(d time.Duration) <-chan time.Time
	Sleep(d time.Duration)
}

// Ticker mirrors time.Ticker behind an interface.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// Real is a Clock backed by the wall clock.
type Real struct{}

func (Real) Now() time.Time                         { return time.Now() }
func (Real) After(d time.Duration) <-chan time.Time { return time.After(d) }
func (Real) Sleep(d time.Duration)                  { time.Sleep(d) }
func (Real) NewTicker(d time.Duration) Ticker       { return realTicker{time.NewTicker(d)} }

type realTicker struct{ t *time.Ticker }

func (r realTicker) C() <-chan time.Time { return r.t.C }
func (r realTicker) Stop()               { r.t.Stop() }

// Fake is a manually advanced Clock. Advance fires every timer and ticker whose
// deadline falls inside the elapsed window, in deadline order, so a scenario
// replays identically on every run.
type Fake struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
	nextID int
}

type fakeTimer struct {
	id       int
	deadline time.Time
	period   time.Duration // zero for one-shot
	ch       chan time.Time
	stopped  bool
}

// NewFake returns a Fake clock started at the given instant.
func NewFake(start time.Time) *Fake { return &Fake{now: start} }

func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *Fake) After(d time.Duration) <-chan time.Time { return f.addTimer(d, 0).ch }

func (f *Fake) Sleep(d time.Duration) { <-f.After(d) }

func (f *Fake) NewTicker(d time.Duration) Ticker {
	return &fakeTickerHandle{f: f, t: f.addTimer(d, d)}
}

func (f *Fake) addTimer(d, period time.Duration) *fakeTimer {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	t := &fakeTimer{
		id:       f.nextID,
		deadline: f.now.Add(d),
		period:   period,
		ch:       make(chan time.Time, 1),
	}
	// A non-positive one-shot delay fires immediately. Queueing it would make
	// any code that sleeps for zero -- a retry with no backoff, for instance --
	// block until something else advanced the clock, which in a synchronous
	// simulation is a deadlock rather than a delay.
	if d <= 0 && period == 0 {
		t.stopped = true
		t.ch <- f.now
		return t
	}
	f.timers = append(f.timers, t)
	return t
}

// Advance moves the clock forward, firing due timers in deadline order.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	target := f.now.Add(d)
	for {
		live := f.timers[:0]
		for _, t := range f.timers {
			if !t.stopped {
				live = append(live, t)
			}
		}
		f.timers = live
		sort.SliceStable(f.timers, func(i, j int) bool {
			if f.timers[i].deadline.Equal(f.timers[j].deadline) {
				return f.timers[i].id < f.timers[j].id
			}
			return f.timers[i].deadline.Before(f.timers[j].deadline)
		})
		var due *fakeTimer
		for _, t := range f.timers {
			if !t.deadline.After(target) {
				due = t
				break
			}
		}
		if due == nil {
			break
		}
		f.now = due.deadline
		if due.period > 0 {
			due.deadline = due.deadline.Add(due.period)
		} else {
			due.stopped = true
		}
		fireAt := f.now
		f.mu.Unlock()
		select {
		case due.ch <- fireAt:
		default:
		}
		f.mu.Lock()
	}
	f.now = target
	f.mu.Unlock()
}

type fakeTickerHandle struct {
	f *Fake
	t *fakeTimer
}

func (h *fakeTickerHandle) C() <-chan time.Time { return h.t.ch }
func (h *fakeTickerHandle) Stop() {
	h.f.mu.Lock()
	h.t.stopped = true
	h.f.mu.Unlock()
}
