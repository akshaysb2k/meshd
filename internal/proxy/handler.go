package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/meshd/meshd/internal/balancer"
	"github.com/meshd/meshd/internal/breaker"
	"github.com/meshd/meshd/internal/cluster"
	"github.com/meshd/meshd/internal/retry"
	"github.com/meshd/meshd/internal/router"
)

// maxBufferedBody bounds how much of a request body is held for replay.
const maxBufferedBody = 1 << 20 // 1 MiB

// excludeSet tracks endpoints an in-flight request has already tried, so a
// retry -- or a concurrent hedge -- lands somewhere new. It is mutex guarded
// because hedged attempts run in parallel, and a hedge that races onto the same
// slow endpoint as the original is worse than no hedge at all.
type excludeSet struct {
	mu sync.Mutex
	m  map[string]bool
}

func newExcludeSet() *excludeSet { return &excludeSet{m: map[string]bool{}} }

func (e *excludeSet) add(addr string) {
	e.mu.Lock()
	e.m[addr] = true
	e.mu.Unlock()
}

func (e *excludeSet) snapshot() map[string]bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[string]bool, len(e.m))
	for k := range e.m {
		out[k] = true
	}
	return out
}

// attempt is the outcome of one upstream try.
type attempt struct {
	resp     *http.Response
	err      error
	endpoint *cluster.Endpoint
	release  func()
	latency  time.Duration
	hedged   bool
}

func (a *attempt) status() int {
	if a.resp != nil {
		return a.resp.StatusCode
	}
	return 0
}

func (a *attempt) close() {
	if a.resp != nil && a.resp.Body != nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(a.resp.Body, 4096))
		_ = a.resp.Body.Close()
	}
	if a.release != nil {
		a.release()
	}
}

// ServeHTTP is the data plane request path.
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := p.clock.Now()
	reqID := r.Header.Get("X-Request-Id")
	if reqID == "" {
		reqID = newRequestID()
	}

	table := p.table.Load()
	rt, ok := table.Match(r)
	if !ok {
		p.finish(w, r, start, "none", "none", http.StatusNotFound, reqID, "no_route")
		return
	}
	cl, ok := p.clusters.Get(rt.Cluster)
	if !ok {
		p.finish(w, r, start, rt.Name, rt.Cluster, http.StatusServiceUnavailable, reqID, "unknown_cluster")
		return
	}

	// Pending admission. Rejecting here, before any upstream work, is the
	// difference between shedding load and queueing it until everything times
	// out at once.
	relPending, err := cl.Breaker().AcquirePending()
	if err != nil {
		p.overflows.With(cl.Name, "pending").Inc()
		p.finish(w, r, start, rt.Name, cl.Name, http.StatusServiceUnavailable, reqID, "breaker_pending")
		return
	}
	defer relPending()

	body, err := bufferBody(r, maxBufferedBody)
	if err != nil {
		p.finish(w, r, start, rt.Name, cl.Name, http.StatusBadRequest, reqID, "body_read")
		return
	}

	relBudget := cl.Budget().TrackRequest()
	defer relBudget()

	ctx, cancel := context.WithTimeout(r.Context(), rt.Timeout)
	defer cancel()
	r = r.WithContext(ctx)

	var res *attempt
	if p.shouldHedge(rt, r, body) {
		res = p.runHedged(ctx, rt, cl, r, body, reqID)
	} else {
		res = p.runSequential(ctx, rt, cl, r, body, reqID)
	}
	if res == nil {
		p.finish(w, r, start, rt.Name, cl.Name, http.StatusServiceUnavailable, reqID, "no_attempt")
		return
	}
	defer func() {
		if res.release != nil {
			res.release()
		}
	}()

	if res.err != nil || res.resp == nil {
		code := http.StatusBadGateway
		reason := "upstream_error"
		switch {
		case errors.Is(res.err, breaker.ErrOverflow):
			// Shedding is not an upstream failure, and reporting it as 502
			// would make a working proxy look like a broken backend on every
			// dashboard.
			code = http.StatusServiceUnavailable
			reason = "breaker_requests"
		case errors.Is(res.err, context.DeadlineExceeded):
			code = http.StatusGatewayTimeout
			reason = "timeout"
		case errors.Is(res.err, balancer.ErrNoEndpoints):
			code = http.StatusServiceUnavailable
			reason = "no_healthy_endpoints"
		}
		p.finish(w, r, start, rt.Name, cl.Name, code, reqID, reason)
		return
	}

	// Commit point: nothing has been written to the client until now, which is
	// what makes retrying a failed attempt safe.
	copyResponseHeaders(w.Header(), res.resp.Header)
	w.Header().Set("X-Request-Id", reqID)
	w.Header().Set("X-Upstream-Endpoint", res.endpoint.Addr)
	if res.hedged {
		w.Header().Set("X-Hedged", "1")
	}
	w.WriteHeader(res.resp.StatusCode)

	var dst io.Writer = w
	if res.resp.Header.Get("Content-Length") == "" {
		dst = flushWriter{w: w, rc: http.NewResponseController(w)}
	}
	_, _ = io.Copy(dst, res.resp.Body)
	_ = res.resp.Body.Close()

	p.record(rt.Name, cl.Name, res.resp.StatusCode, p.clock.Now().Sub(start))
	p.log.Debug("request complete",
		"request_id", reqID, "route", rt.Name, "cluster", cl.Name,
		"endpoint", res.endpoint.Addr, "status", res.resp.StatusCode,
		"duration_ms", p.clock.Now().Sub(start).Milliseconds(), "hedged", res.hedged)
}

// runSequential performs attempt-then-retry with budget enforcement.
func (p *Proxy) runSequential(ctx context.Context, rt *router.Route, cl *cluster.Cluster, r *http.Request, body *bufferedBody, reqID string) *attempt {
	exclude := newExcludeSet()
	maxAttempts := 1
	if rt.Retry != nil && body.replayable {
		maxAttempts = rt.Retry.MaxAttempts
	}

	var last *attempt
	for n := 1; n <= maxAttempts; n++ {
		if n > 1 {
			// Three independent gates must all allow a retry: the per-cluster
			// retry budget, the circuit breaker's retry ceiling, and the
			// backoff timer. Any one of them alone is insufficient.
			relRetry, ok := cl.Budget().TryAcquire()
			if !ok {
				p.retriesDenied.With(cl.Name, "budget").Inc()
				break
			}
			relBreaker, ok := cl.Breaker().AcquireRetry()
			if !ok {
				relRetry()
				p.retriesDenied.With(cl.Name, "breaker").Inc()
				p.overflows.With(cl.Name, "retries").Inc()
				break
			}
			delay := p.backoff(rt.Retry, n-1)
			select {
			case <-p.clock.After(delay):
			case <-ctx.Done():
				relRetry()
				relBreaker()
				if last != nil {
					return last
				}
				return &attempt{err: ctx.Err()}
			}
			p.retries.With(cl.Name).Inc()
			res := p.doAttempt(ctx, rt, cl, r, body, exclude, reqID, false)
			relRetry()
			relBreaker()
			if last != nil {
				last.close()
			}
			last = res
		} else {
			last = p.doAttempt(ctx, rt, cl, r, body, exclude, reqID, false)
		}

		if !p.retryable(rt, r, last) {
			return last
		}
		if ctx.Err() != nil {
			return last
		}
	}
	return last
}

// runHedged races a second attempt against a slow first one.
//
// Hedging trades a small amount of extra load for a large cut in tail latency:
// p99 stops being "whichever backend had a GC pause" and becomes "the faster of
// two backends". It is only safe for idempotent requests, it is budgeted
// exactly like a retry so it cannot amplify load during an incident, and the
// hedge is steered to a different endpoint than the original.
func (p *Proxy) runHedged(ctx context.Context, rt *router.Route, cl *cluster.Cluster, r *http.Request, body *bufferedBody, reqID string) *attempt {
	results := make(chan *attempt, rt.Hedge.MaxHedges+1)
	exclude := newExcludeSet()

	launched := 0
	launch := func(hedged bool) {
		launched++
		go func() {
			results <- p.doAttempt(ctx, rt, cl, r, body, exclude, reqID, hedged)
		}()
	}

	// reap returns the winner and disposes of everything else in the
	// background. Each attempt owns a cancellable context, so closing a loser
	// aborts only that attempt -- the winner keeps streaming its body.
	received := 0
	reap := func(winner, loser *attempt) *attempt {
		pending := launched - received
		go func() {
			if loser != nil && loser != winner {
				loser.close()
			}
			for i := 0; i < pending; i++ {
				select {
				case a := <-results:
					if a != winner {
						a.close()
					}
				case <-time.After(10 * time.Second):
					return
				}
			}
		}()
		return winner
	}

	launch(false)
	timer := p.clock.After(rt.Hedge.Delay)
	var lastFail *attempt

	for {
		select {
		case a := <-results:
			received++
			if a.err == nil && a.resp != nil && a.resp.StatusCode < 500 {
				return reap(a, lastFail)
			}
			if lastFail != nil {
				lastFail.close()
			}
			lastFail = a
			if received == launched {
				if launched <= rt.Hedge.MaxHedges {
					p.hedges.With(cl.Name).Inc()
					launch(true)
					continue
				}
				return reap(lastFail, nil)
			}
		case <-timer:
			timer = nil
			if launched > rt.Hedge.MaxHedges {
				continue
			}
			// A hedge costs an extra upstream request, so it draws from the
			// same budget a retry would. Under a real incident the budget is
			// already spent and hedging silently switches itself off.
			rel, ok := cl.Budget().TryAcquire()
			if !ok {
				p.retriesDenied.With(cl.Name, "budget").Inc()
				continue
			}
			rel()
			p.hedges.With(cl.Name).Inc()
			launch(true)
		case <-ctx.Done():
			if lastFail != nil {
				return reap(lastFail, nil)
			}
			return reap(&attempt{err: ctx.Err()}, nil)
		}
	}
}

// doAttempt performs one upstream request end to end.
func (p *Proxy) doAttempt(ctx context.Context, rt *router.Route, cl *cluster.Cluster, r *http.Request, body *bufferedBody, exclude *excludeSet, reqID string, hedged bool) *attempt {
	now := p.clock.Now()
	pick, err := cl.Pick(p.hashKey(rt, r), now, exclude.snapshot())
	if err != nil {
		return &attempt{err: err, hedged: hedged}
	}
	ep := pick.Endpoint
	exclude.add(ep.Addr)
	if pick.Panic {
		p.panicGauge.With(cl.Name).Set(1)
	} else {
		p.panicGauge.With(cl.Name).Set(0)
	}

	relBreaker, err := cl.Breaker().AcquireRequest()
	if err != nil {
		p.overflows.With(cl.Name, "requests").Inc()
		return &attempt{err: err, endpoint: ep, hedged: hedged}
	}
	relEndpoint := ep.Acquire()
	release := func() {
		relEndpoint()
		relBreaker()
	}

	// Every attempt gets its own cancellable context. This is what lets a
	// losing hedge be torn down without disturbing the winner, which is still
	// streaming its response body off the same parent context.
	var actx context.Context
	var cancel context.CancelFunc
	if rt.Retry != nil && rt.Retry.PerTryTimeout > 0 {
		actx, cancel = context.WithTimeout(ctx, rt.Retry.PerTryTimeout)
	} else {
		actx, cancel = context.WithCancel(ctx)
	}

	path := rt.RewritePath(r.URL.Path)
	out, err := outboundRequest(r.WithContext(actx), ep.URL.Scheme+"://"+ep.URL.Host, path, body, reqID)
	if err != nil {
		cancel()
		release()
		return &attempt{err: err, endpoint: ep, hedged: hedged}
	}

	started := p.clock.Now()
	resp, err := ep.Transport().RoundTrip(out)
	latency := p.clock.Now().Sub(started)

	outcome := "success"
	if err != nil {
		outcome = "error"
	} else if resp.StatusCode >= 500 {
		outcome = "5xx"
	}
	p.attempts.With(cl.Name, ep.Addr, outcome).Inc()
	p.upstreamLatency.With(cl.Name).Observe(latency.Seconds())
	ep.RecordResult(statusOf(resp), err != nil)

	fullRelease := func() {
		release()
		cancel()
	}
	if err != nil {
		fullRelease()
		return &attempt{err: err, endpoint: ep, latency: latency, hedged: hedged}
	}
	return &attempt{resp: resp, endpoint: ep, release: fullRelease, latency: latency, hedged: hedged}
}

func (p *Proxy) retryable(rt *router.Route, r *http.Request, a *attempt) bool {
	if a == nil || rt.Retry == nil {
		return false
	}
	// Neither of these is worth retrying. Retrying a shed request is
	// self-defeating -- the whole point of shedding was to reduce load -- and
	// retrying when there are no endpoints just burns the attempt budget.
	if errors.Is(a.err, balancer.ErrNoEndpoints) || errors.Is(a.err, breaker.ErrOverflow) {
		return false
	}
	return rt.Retry.Retryable(r.Method, a.status(), a.err != nil)
}

func (p *Proxy) shouldHedge(rt *router.Route, r *http.Request, body *bufferedBody) bool {
	return rt.Hedge != nil && rt.Hedge.Delay > 0 && body.replayable && retry.Idempotent(r.Method)
}

// hashKey resolves the session affinity key for ring_hash clusters.
func (p *Proxy) hashKey(rt *router.Route, r *http.Request) string {
	if rt.HashOn != "" {
		if v := r.Header.Get(rt.HashOn); v != "" {
			return v
		}
		if c, err := r.Cookie(rt.HashOn); err == nil {
			return c.Value
		}
	}
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return xff
	}
	return r.RemoteAddr
}

func statusOf(resp *http.Response) int {
	if resp == nil {
		return 0
	}
	return resp.StatusCode
}

// finish writes a proxy-generated error response and records it.
func (p *Proxy) finish(w http.ResponseWriter, r *http.Request, start time.Time, route, cl string, code int, reqID, reason string) {
	w.Header().Set("X-Request-Id", reqID)
	w.Header().Set("X-Proxy-Reason", reason)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(code)
	_, _ = io.WriteString(w, http.StatusText(code)+"\n")
	p.record(route, cl, code, p.clock.Now().Sub(start))
	p.log.Warn("request failed",
		"request_id", reqID, "route", route, "cluster", cl,
		"status", code, "reason", reason, "path", r.URL.Path)
}

func (p *Proxy) record(route, cl string, code int, d time.Duration) {
	p.requests.With(route, cl, strconv.Itoa(code)).Inc()
	p.duration.With(route, cl).Observe(d.Seconds())
}
