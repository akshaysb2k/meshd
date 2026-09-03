package cluster

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/meshd/meshd/internal/config"
)

// Endpoint is a single upstream host together with everything the data plane
// tracks about it: its own connection pool, its health state, and the counters
// that drive outlier ejection and least-request balancing.
//
// Endpoints deliberately outlive config snapshots. When a new snapshot arrives
// the manager reuses the Endpoint object for any address that still exists, so
// warm connections and accumulated health state survive a config push.
type Endpoint struct {
	Addr string
	URL  *url.URL

	// rt is what requests actually go through. pool is the same object when the
	// endpoint owns a real connection pool, and nil when a test or simulation
	// supplied its own RoundTripper. Keeping both lets drain() close idle
	// connections without the rest of the code caring which case it is in.
	rt   http.RoundTripper
	pool *http.Transport

	active atomic.Int64 // in-flight requests, drives least_request
	total  atomic.Int64

	hcHealthy    atomic.Bool
	hcSuccesses  atomic.Int64
	hcFailures   atomic.Int64
	healthySince atomic.Int64 // unix nanos, drives slow start

	ejectedUntil  atomic.Int64 // unix nanos; zero means not ejected
	ejectionCount atomic.Int64

	consec5xx     atomic.Int64
	consecGateway atomic.Int64

	draining atomic.Bool
}

// Dialer builds the RoundTripper for an endpoint. A nil Dialer means the
// endpoint gets a real pooled HTTP transport.
type Dialer func(addr string, c *config.Cluster) http.RoundTripper

func newEndpoint(addr string, c *config.Cluster, now time.Time, dial Dialer) (*Endpoint, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}
	if dial != nil {
		ep := &Endpoint{Addr: addr, URL: u, rt: dial(addr, c)}
		ep.hcHealthy.Store(true)
		ep.healthySince.Store(now.UnixNano())
		return ep, nil
	}
	idle := c.MaxIdleConnsPerHost
	if idle <= 0 {
		idle = 64
	}
	dialer := &net.Dialer{
		Timeout:   c.ConnectTimeout.Or(2 * time.Second),
		KeepAlive: 30 * time.Second,
	}
	pool := &http.Transport{
		DialContext:           dialer.DialContext,
		MaxIdleConns:          idle,
		MaxIdleConnsPerHost:   idle,
		MaxConnsPerHost:       0,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: time.Second,
		ForceAttemptHTTP2:     true,
	}
	ep := &Endpoint{Addr: addr, URL: u, rt: pool, pool: pool}
	// An endpoint starts healthy. Assuming otherwise means a proxy restart
	// black-holes traffic until the first probe lands.
	ep.hcHealthy.Store(true)
	ep.healthySince.Store(now.UnixNano())
	return ep, nil
}

// Transport returns the RoundTripper for this endpoint.
func (e *Endpoint) Transport() http.RoundTripper { return e.rt }

// CloseIdleConnections releases pooled connections, if this endpoint owns a
// pool at all.
func (e *Endpoint) CloseIdleConnections() {
	if e.pool != nil {
		e.pool.CloseIdleConnections()
	}
}

// Active is the number of requests currently in flight to this endpoint.
func (e *Endpoint) Active() int64 { return e.active.Load() }

// Total is the number of requests ever dispatched to this endpoint.
func (e *Endpoint) Total() int64 { return e.total.Load() }

// Acquire marks the start of a request and returns the release function.
func (e *Endpoint) Acquire() func() {
	e.active.Add(1)
	e.total.Add(1)
	var once atomic.Bool
	return func() {
		if once.CompareAndSwap(false, true) {
			e.active.Add(-1)
		}
	}
}

// Healthy reports the active health checker's view.
func (e *Endpoint) Healthy() bool { return e.hcHealthy.Load() }

// Ejected reports whether passive outlier detection has taken this endpoint
// out of rotation as of now.
func (e *Endpoint) Ejected(now time.Time) bool {
	until := e.ejectedUntil.Load()
	return until != 0 && now.UnixNano() < until
}

// EjectedUntil returns the ejection deadline, or the zero time.
func (e *Endpoint) EjectedUntil() time.Time {
	until := e.ejectedUntil.Load()
	if until == 0 {
		return time.Time{}
	}
	return time.Unix(0, until)
}

// EjectionCount is how many times this endpoint has been ejected, used to grow
// the ejection window on repeat offenders.
func (e *Endpoint) EjectionCount() int64 { return e.ejectionCount.Load() }

// Draining reports whether the endpoint has been removed by a config push and
// is finishing its in-flight requests.
func (e *Endpoint) Draining() bool { return e.draining.Load() }

// Available reports whether the endpoint may receive new traffic.
func (e *Endpoint) Available(now time.Time) bool {
	return e.Healthy() && !e.Ejected(now) && !e.Draining()
}

// Weight returns the endpoint's share of traffic in [minWeight, 1], ramping
// linearly across the slow start window after it most recently became healthy.
func (e *Endpoint) Weight(now time.Time, slowStart time.Duration, minWeight float64) float64 {
	if slowStart <= 0 {
		return 1
	}
	since := e.healthySince.Load()
	if since == 0 {
		return 1
	}
	elapsed := now.Sub(time.Unix(0, since))
	if elapsed >= slowStart {
		return 1
	}
	if minWeight <= 0 {
		minWeight = 0.05
	}
	w := float64(elapsed) / float64(slowStart)
	if w < minWeight {
		return minWeight
	}
	return w
}

// RecordProbe feeds an active health check result in and applies hysteresis:
// unhealthyThreshold consecutive failures to take an endpoint out, and
// healthyThreshold consecutive successes to put it back. Without hysteresis a
// single dropped probe flaps the whole cluster.
func (e *Endpoint) RecordProbe(ok bool, healthyThreshold, unhealthyThreshold int, now time.Time) (changed bool) {
	if healthyThreshold < 1 {
		healthyThreshold = 2
	}
	if unhealthyThreshold < 1 {
		unhealthyThreshold = 3
	}
	if ok {
		e.hcFailures.Store(0)
		n := e.hcSuccesses.Add(1)
		if !e.hcHealthy.Load() && n >= int64(healthyThreshold) {
			return e.SetHealthy(true, now)
		}
		return false
	}
	e.hcSuccesses.Store(0)
	n := e.hcFailures.Add(1)
	if e.hcHealthy.Load() && n >= int64(unhealthyThreshold) {
		return e.SetHealthy(false, now)
	}
	return false
}

// SetHealthy transitions health state, restarting the slow start ramp on any
// unhealthy-to-healthy edge. Exported for the health subsystem.
func (e *Endpoint) SetHealthy(healthy bool, now time.Time) (changed bool) {
	prev := e.hcHealthy.Swap(healthy)
	if prev == healthy {
		return false
	}
	if healthy {
		e.healthySince.Store(now.UnixNano())
	}
	return true
}

// Consecutive5xx is the current run of 5xx responses, read by the outlier
// detector.
func (e *Endpoint) Consecutive5xx() int64 { return e.consec5xx.Load() }

// ConsecutiveGatewayErrors is the current run of 502/503/504 responses and
// transport failures.
func (e *Endpoint) ConsecutiveGatewayErrors() int64 { return e.consecGateway.Load() }

// RecordResult feeds a completed attempt back into outlier detection.
func (e *Endpoint) RecordResult(status int, transportErr bool) {
	switch {
	case transportErr:
		e.consecGateway.Add(1)
		e.consec5xx.Add(1)
	case status >= 500:
		e.consec5xx.Add(1)
		if status == 502 || status == 503 || status == 504 {
			e.consecGateway.Add(1)
		} else {
			e.consecGateway.Store(0)
		}
	default:
		e.consec5xx.Store(0)
		e.consecGateway.Store(0)
	}
}

// Eject takes the endpoint out of rotation until the given deadline.
func (e *Endpoint) Eject(until time.Time) {
	e.ejectedUntil.Store(until.UnixNano())
	e.ejectionCount.Add(1)
	e.consec5xx.Store(0)
	e.consecGateway.Store(0)
}

// Uneject returns the endpoint to rotation and restarts its slow start ramp, so
// a recovered backend is not handed a full share the instant it comes back.
func (e *Endpoint) Uneject(now time.Time) {
	e.ejectedUntil.Store(0)
	e.healthySince.Store(now.UnixNano())
}

// markDraining takes the endpoint out of rotation immediately.
//
// This is deliberately separate from drain and is called synchronously during a
// config push. If the flag were set inside drain's goroutine there would be a
// window, however brief, in which a removed endpoint still received new
// requests -- exactly the 502s that draining exists to prevent.
func (e *Endpoint) markDraining() { e.draining.Store(true) }

// drain waits for in-flight requests to finish under the given deadline, then
// closes the endpoint's idle connections. Callers must have called markDraining
// first.
func (e *Endpoint) drain(ctx context.Context, timeout time.Duration) {
	e.markDraining()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		if e.active.Load() == 0 {
			break
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			goto done
		case <-ctx.Done():
			goto done
		}
	}
done:
	e.CloseIdleConnections()
}
