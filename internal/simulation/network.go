// Package simulation runs the whole data plane against a scripted network with
// no sockets, no goroutines and no wall clock.
//
// The point is reproducibility. A proxy's interesting behaviour lives in the
// interaction between health checking, ejection, retries and load balancing
// under partial failure, and that interaction is exactly what a normal
// integration test cannot pin down: it depends on timing, on which goroutine
// woke first, and on the scheduler. Bugs found that way come with a stack trace
// and no way to reproduce them.
//
// Here, every source of nondeterminism is replaced. Time is a manually advanced
// fake clock, the network is a scripted RoundTripper, failures are drawn from a
// seeded PRNG, and requests are issued synchronously. A scenario is therefore a
// pure function of its seed: "seed 4471 fails on step 38" is a complete and
// permanent bug report.
package simulation

import (
	"bytes"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/meshd/meshd/internal/cluster"
	"github.com/meshd/meshd/internal/config"
)

// HostState is the scripted behaviour of one simulated upstream.
type HostState struct {
	// Down refuses connections, as a dead process would.
	Down bool
	// ErrorRate is the probability of a 503 on real traffic, in [0,1].
	ErrorRate float64
	// Blackhole accepts the request and never answers, as a hung process would.
	Blackhole bool
	// HealthLies makes the health endpoint report 200 while real traffic fails.
	// This is the case active probing cannot catch and outlier detection can,
	// and it is the reason a proxy needs both.
	HealthLies bool
}

// Network is a scripted set of upstreams plus an event log.
type Network struct {
	mu     sync.Mutex
	rng    *rand.Rand
	hosts  map[string]*HostState
	hits   map[string]int
	probes map[string]int
	events []string
}

// NewNetwork returns a network whose failure draws come from the given seed.
func NewNetwork(seed uint64) *Network {
	return &Network{
		rng:    rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
		hosts:  map[string]*HostState{},
		hits:   map[string]int{},
		probes: map[string]int{},
	}
}

// Host returns the mutable state for an address, creating it if absent.
func (n *Network) Host(addr string) *HostState {
	n.mu.Lock()
	defer n.mu.Unlock()
	h, ok := n.hosts[addr]
	if !ok {
		h = &HostState{}
		n.hosts[addr] = h
	}
	return h
}

// Set applies a mutation to a host and records it in the event log.
func (n *Network) Set(addr string, fn func(*HostState)) {
	h := n.Host(addr)
	n.mu.Lock()
	fn(h)
	n.events = append(n.events, fmt.Sprintf("net %s -> down=%v err=%.2f blackhole=%v lies=%v",
		short(addr), h.Down, h.ErrorRate, h.Blackhole, h.HealthLies))
	n.mu.Unlock()
}

// Hits reports how many real requests each host received.
func (n *Network) Hits() map[string]int {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := map[string]int{}
	for k, v := range n.hits {
		out[short(k)] = v
	}
	return out
}

// Probes reports how many health probes each host received.
func (n *Network) Probes() map[string]int {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := map[string]int{}
	for k, v := range n.probes {
		out[short(k)] = v
	}
	return out
}

// Events returns the network's mutation log.
func (n *Network) Events() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]string(nil), n.events...)
}

// Dialer returns a cluster.Dialer that wires endpoints to this network.
func (n *Network) Dialer() cluster.Dialer {
	return func(addr string, _ *config.Cluster) http.RoundTripper {
		return &hostTransport{n: n, addr: addr}
	}
}

type hostTransport struct {
	n    *Network
	addr string
}

// ErrRefused is what a simulated dead process returns.
type ErrRefused struct{ Addr string }

func (e *ErrRefused) Error() string { return "dial " + e.Addr + ": connection refused" }

func (t *hostTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	isProbe := strings.HasSuffix(req.URL.Path, "/healthz")

	t.n.mu.Lock()
	h, ok := t.n.hosts[t.addr]
	if !ok {
		h = &HostState{}
		t.n.hosts[t.addr] = h
	}
	if isProbe {
		t.n.probes[t.addr]++
	} else {
		t.n.hits[t.addr]++
	}
	down, blackhole, lies, rate := h.Down, h.Blackhole, h.HealthLies, h.ErrorRate
	// The draw happens under the same lock as the counter bump, so the sequence
	// of random values is tied to the sequence of requests and nothing else.
	draw := t.n.rng.Float64()
	t.n.mu.Unlock()

	if down {
		return nil, &ErrRefused{Addr: t.addr}
	}
	if blackhole {
		<-req.Context().Done()
		return nil, req.Context().Err()
	}
	if isProbe {
		if lies {
			return response(200, "ok"), nil
		}
		if rate >= 1 {
			return response(503, "unhealthy"), nil
		}
		return response(200, "ok"), nil
	}
	if draw < rate {
		return response(503, "injected failure"), nil
	}
	return response(200, short(t.addr)), nil
}

func response(code int, body string) *http.Response {
	return &http.Response{
		StatusCode: code,
		Status:     http.StatusText(code),
		Proto:      "HTTP/1.1",
		ProtoMajor: 1, ProtoMinor: 1,
		Header: http.Header{
			"Content-Type": []string{"text/plain"},
			"X-Backend":    []string{body},
		},
		Body:          io.NopCloser(bytes.NewBufferString(body)),
		ContentLength: int64(len(body)),
	}
}

// short trims the scheme and host prefix so traces stay readable.
func short(addr string) string {
	s := strings.TrimPrefix(strings.TrimPrefix(addr, "http://"), "https://")
	return s
}

func sortedKeys(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
