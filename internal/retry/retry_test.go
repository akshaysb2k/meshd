package retry

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/meshd/meshd/internal/config"
)

func TestBudgetClampsRetriesUnderWidespreadFailure(t *testing.T) {
	// This is the retry storm scenario. 1000 requests are in flight and every
	// one of them wants to retry. A fixed max_attempts of 3 would triple the
	// offered load onto an already failing cluster; a 20% budget allows 200.
	b := NewBudget(20, 3)
	for i := 0; i < 1000; i++ {
		b.TrackRequest()
	}

	granted := 0
	for i := 0; i < 1000; i++ {
		if _, ok := b.TryAcquire(); ok {
			granted++
		}
	}
	if granted != 200 {
		t.Fatalf("granted %d retries against 1000 active requests, want 200", granted)
	}
	_, _, allowed, _, denied := b.Stats()
	if allowed != 200 || denied != 800 {
		t.Fatalf("allowed=%d denied=%d, want 200/800", allowed, denied)
	}
}

func TestBudgetReleasesSlotsBackForReuse(t *testing.T) {
	b := NewBudget(50, 1)
	for i := 0; i < 10; i++ {
		b.TrackRequest()
	}
	var releases []func()
	for i := 0; i < 5; i++ {
		rel, ok := b.TryAcquire()
		if !ok {
			t.Fatalf("retry %d denied inside the budget", i)
		}
		releases = append(releases, rel)
	}
	if _, ok := b.TryAcquire(); ok {
		t.Fatal("granted a retry past the ceiling")
	}
	releases[0]()
	if _, ok := b.TryAcquire(); !ok {
		t.Fatal("a released slot was not reusable")
	}
}

func TestBudgetFloorLetsAnIdleClusterRetry(t *testing.T) {
	// With almost no traffic, a percentage-only budget rounds to zero and the
	// cluster loses retries entirely. The floor exists for exactly this case.
	b := NewBudget(20, 3)
	b.TrackRequest()
	granted := 0
	for i := 0; i < 10; i++ {
		if _, ok := b.TryAcquire(); ok {
			granted++
		}
	}
	if granted != 3 {
		t.Fatalf("granted %d retries on an idle cluster, want the floor of 3", granted)
	}
}

func TestNonIdempotentMethodsAreNotRetriedByDefault(t *testing.T) {
	p := NewPolicy(&config.RetryPolicy{On: []string{"5xx"}, MaxAttempts: 3})
	if p.Retryable("POST", 503, false) {
		t.Fatal("POST was retried without an explicit opt-in")
	}
	if !p.Retryable("GET", 503, false) {
		t.Fatal("GET on 503 should be retryable")
	}
	opt := NewPolicy(&config.RetryPolicy{On: []string{"5xx"}, MaxAttempts: 3, RetryNonIdempotent: true})
	if !opt.Retryable("POST", 503, false) {
		t.Fatal("explicit opt-in did not enable POST retries")
	}
}

func TestRetryConditionsAreHonoured(t *testing.T) {
	p := NewPolicy(&config.RetryPolicy{On: []string{"gateway-error"}, MaxAttempts: 3})
	if p.Retryable("GET", 500, false) {
		t.Fatal("500 retried under a gateway-error-only policy")
	}
	if !p.Retryable("GET", 503, false) {
		t.Fatal("503 not retried under a gateway-error policy")
	}
	if p.Retryable("GET", 404, false) {
		t.Fatal("404 should never be retried")
	}
}

func TestBackoffIsBoundedAndJittered(t *testing.T) {
	p := NewPolicy(&config.RetryPolicy{
		MaxAttempts: 5,
		BaseBackoff: config.Duration(10 * time.Millisecond),
		MaxBackoff:  config.Duration(100 * time.Millisecond),
	})
	rng := rand.New(rand.NewPCG(42, 42))
	seen := map[time.Duration]bool{}
	for i := 0; i < 200; i++ {
		d := p.Backoff(3, rng)
		if d < 0 || d > 100*time.Millisecond {
			t.Fatalf("backoff %v outside [0, max]", d)
		}
		seen[d] = true
	}
	// Full jitter must actually produce a spread; identical delays across
	// clients is what synchronises a retry storm in the first place.
	if len(seen) < 50 {
		t.Fatalf("only %d distinct backoff values in 200 draws, jitter is too weak", len(seen))
	}
}

func TestBackoffCeilingGrowsThenSaturates(t *testing.T) {
	p := NewPolicy(&config.RetryPolicy{
		MaxAttempts: 10,
		BaseBackoff: config.Duration(10 * time.Millisecond),
		MaxBackoff:  config.Duration(80 * time.Millisecond),
	})
	rng := rand.New(rand.NewPCG(1, 1))
	maxSeen := func(attempt int) time.Duration {
		var m time.Duration
		for i := 0; i < 500; i++ {
			if d := p.Backoff(attempt, rng); d > m {
				m = d
			}
		}
		return m
	}
	a1, a3, a9 := maxSeen(1), maxSeen(3), maxSeen(9)
	if !(a1 < a3) {
		t.Fatalf("ceiling did not grow: attempt1=%v attempt3=%v", a1, a3)
	}
	if a9 > 80*time.Millisecond {
		t.Fatalf("ceiling exceeded max_backoff at attempt 9: %v", a9)
	}
}

func TestNilPolicyNeverRetries(t *testing.T) {
	var p *Policy
	if p.Retryable("GET", 503, true) {
		t.Fatal("a nil policy must never retry")
	}
	if p.Backoff(1, nil) != 0 {
		t.Fatal("a nil policy must not delay")
	}
}
