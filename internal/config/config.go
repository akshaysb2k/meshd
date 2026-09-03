// Package config defines the immutable, versioned configuration snapshot that
// the data plane consumes. Snapshots are never mutated in place: the control
// plane hands over a whole new one and the proxy swaps it atomically.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"
)

// Duration is a time.Duration that marshals as a Go duration string.
type Duration time.Duration

// UnmarshalJSON parses "250ms", "5s" and friends.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	if s == "" {
		*d = 0
		return nil
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

// MarshalJSON renders the duration as a string.
func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(d.D().String()) }

// D converts back to time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

// Or returns the duration, or def when unset.
func (d Duration) Or(def time.Duration) time.Duration {
	if d == 0 {
		return def
	}
	return time.Duration(d)
}

// Snapshot is a complete, self-consistent view of proxy configuration.
type Snapshot struct {
	Version  string    `json:"version"`
	Routes   []Route   `json:"routes"`
	Clusters []Cluster `json:"clusters"`
}

// Route maps matching requests onto a cluster.
type Route struct {
	Name    string       `json:"name"`
	Match   RouteMatch   `json:"match"`
	Cluster string       `json:"cluster"`
	Timeout Duration     `json:"timeout"`
	Retry   *RetryPolicy `json:"retry,omitempty"`
	Hedge   *HedgePolicy `json:"hedge,omitempty"`
	// HashOn names a header whose value drives session affinity when the
	// cluster uses the ring_hash policy. Empty means hash on client IP.
	HashOn string `json:"hash_on,omitempty"`
	// PrefixRewrite replaces the matched path prefix on the outbound request.
	PrefixRewrite string `json:"prefix_rewrite,omitempty"`
}

// RouteMatch is the predicate a request must satisfy to select a Route.
type RouteMatch struct {
	Host       string            `json:"host,omitempty"`
	PathPrefix string            `json:"path_prefix,omitempty"`
	Methods    []string          `json:"methods,omitempty"`
	Headers    map[string]string `json:"headers,omitempty"`
}

// RetryPolicy controls when and how often an attempt is repeated.
type RetryPolicy struct {
	// On lists trigger conditions: 5xx, gateway-error, reset, connect-failure.
	On []string `json:"on"`
	// MaxAttempts caps attempts per request, including the original.
	MaxAttempts   int      `json:"max_attempts"`
	PerTryTimeout Duration `json:"per_try_timeout"`
	BaseBackoff   Duration `json:"base_backoff"`
	MaxBackoff    Duration `json:"max_backoff"`
	// BudgetPercent bounds concurrent retries as a percentage of the cluster's
	// active requests. This is what stops a partial outage becoming a retry
	// storm; a fixed max_attempts alone does not.
	BudgetPercent float64 `json:"budget_percent"`
	// MinRetryConcurrency is the floor so a nearly idle cluster can still retry.
	MinRetryConcurrency int64 `json:"min_retry_concurrency"`
	// RetryNonIdempotent opts a route into retrying POST/PATCH. Off by default.
	RetryNonIdempotent bool `json:"retry_non_idempotent"`
}

// HedgePolicy fires a second attempt when the first is slow, taking whichever
// response arrives first. Idempotent requests only.
type HedgePolicy struct {
	Delay         Duration `json:"delay"`
	MaxHedges     int      `json:"max_hedges"`
	BudgetPercent float64  `json:"budget_percent"`
}

// Cluster is a named group of endpoints plus the policy for reaching them.
type Cluster struct {
	Name      string   `json:"name"`
	Policy    string   `json:"policy"` // round_robin | least_request | ring_hash
	Endpoints []string `json:"endpoints"`

	HealthCheck    *HealthCheck    `json:"health_check,omitempty"`
	Outlier        *Outlier        `json:"outlier_detection,omitempty"`
	CircuitBreaker *CircuitBreaker `json:"circuit_breaker,omitempty"`

	// PanicThreshold is the healthy fraction below which health status is
	// ignored and traffic is spread over every endpoint. When most of the fleet
	// looks unhealthy the signal is probably wrong, and piling all load onto the
	// few survivors guarantees they fall over too.
	PanicThreshold float64 `json:"panic_threshold"`
	// SlowStart ramps a newly healthy endpoint from MinWeight to full weight,
	// so a cold backend is not handed a full share the instant it recovers.
	SlowStart          Duration `json:"slow_start"`
	SlowStartMinWeight float64  `json:"slow_start_min_weight"`

	// RingReplicas is the virtual nodes per endpoint for ring_hash.
	RingReplicas int `json:"ring_replicas"`

	MaxIdleConnsPerHost int      `json:"max_idle_conns_per_host"`
	ConnectTimeout      Duration `json:"connect_timeout"`
	DrainTimeout        Duration `json:"drain_timeout"`
}

// HealthCheck configures active probing of each endpoint.
type HealthCheck struct {
	Path               string   `json:"path"`
	Interval           Duration `json:"interval"`
	Timeout            Duration `json:"timeout"`
	HealthyThreshold   int      `json:"healthy_threshold"`
	UnhealthyThreshold int      `json:"unhealthy_threshold"`
	// JitterPercent spreads probes so the whole fleet is not hit in lockstep.
	JitterPercent float64 `json:"jitter_percent"`
	// ExpectedStatus defaults to 200 when zero.
	ExpectedStatus int `json:"expected_status"`
}

// Outlier configures passive ejection driven by real traffic.
type Outlier struct {
	Consecutive5xx           int      `json:"consecutive_5xx"`
	ConsecutiveGatewayErrors int      `json:"consecutive_gateway_errors"`
	Interval                 Duration `json:"interval"`
	BaseEjectionTime         Duration `json:"base_ejection_time"`
	MaxEjectionTime          Duration `json:"max_ejection_time"`
	// MaxEjectionPercent caps how much of a cluster may be ejected at once so a
	// bad deploy cannot eject the entire fleet.
	MaxEjectionPercent float64 `json:"max_ejection_percent"`
}

// CircuitBreaker holds the resource limits that shed load before a cluster
// collapses. These are limits, not the classic open/half-open state machine;
// that behaviour lives in outlier detection.
type CircuitBreaker struct {
	MaxConnections     int64 `json:"max_connections"`
	MaxPendingRequests int64 `json:"max_pending_requests"`
	MaxRequests        int64 `json:"max_requests"`
	MaxRetries         int64 `json:"max_retries"`
}

// Load reads and validates a snapshot from a JSON file.
func Load(path string) (*Snapshot, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(b)
}

// Parse decodes and validates a snapshot.
func Parse(b []byte) (*Snapshot, error) {
	var s Snapshot
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&s); err != nil {
		return nil, fmt.Errorf("decode snapshot: %w", err)
	}
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return &s, nil
}

// Validate rejects a snapshot that would produce an unroutable proxy. A bad
// push must fail here, before the atomic swap, so the running config survives.
func (s *Snapshot) Validate() error {
	if s.Version == "" {
		return errors.New("snapshot: version is required")
	}
	if len(s.Clusters) == 0 {
		return errors.New("snapshot: at least one cluster is required")
	}
	seen := map[string]bool{}
	for i := range s.Clusters {
		c := &s.Clusters[i]
		if c.Name == "" {
			return fmt.Errorf("cluster %d: name is required", i)
		}
		if seen[c.Name] {
			return fmt.Errorf("cluster %q: duplicate name", c.Name)
		}
		seen[c.Name] = true
		if len(c.Endpoints) == 0 {
			return fmt.Errorf("cluster %q: at least one endpoint is required", c.Name)
		}
		for _, ep := range c.Endpoints {
			u, err := url.Parse(ep)
			if err != nil || u.Scheme == "" || u.Host == "" {
				return fmt.Errorf("cluster %q: endpoint %q must be an absolute URL", c.Name, ep)
			}
		}
		switch c.Policy {
		case "", "round_robin", "least_request", "ring_hash":
		default:
			return fmt.Errorf("cluster %q: unknown policy %q", c.Name, c.Policy)
		}
		if c.PanicThreshold < 0 || c.PanicThreshold > 1 {
			return fmt.Errorf("cluster %q: panic_threshold must be in [0,1]", c.Name)
		}
		if o := c.Outlier; o != nil {
			if o.MaxEjectionPercent < 0 || o.MaxEjectionPercent > 100 {
				return fmt.Errorf("cluster %q: max_ejection_percent must be in [0,100]", c.Name)
			}
		}
	}
	if len(s.Routes) == 0 {
		return errors.New("snapshot: at least one route is required")
	}
	for i := range s.Routes {
		r := &s.Routes[i]
		if r.Cluster == "" {
			return fmt.Errorf("route %d: cluster is required", i)
		}
		if !seen[r.Cluster] {
			return fmt.Errorf("route %q: references unknown cluster %q", r.Name, r.Cluster)
		}
		if r.Retry != nil && r.Retry.MaxAttempts < 1 {
			return fmt.Errorf("route %q: retry.max_attempts must be >= 1", r.Name)
		}
	}
	return nil
}
