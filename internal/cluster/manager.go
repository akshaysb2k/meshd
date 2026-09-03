package cluster

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/meshd/meshd/internal/config"
)

// Manager owns the live cluster set and reconciles it against config snapshots.
//
// Reconciliation, not replacement, is the point of this type. A naive
// implementation rebuilds every cluster on each push, which throws away warm
// connection pools and health state and produces a latency spike and a burst of
// 502s on every config change. Here, an endpoint that appears in both the old
// and new snapshot keeps its exact Endpoint object -- same pool, same health,
// same ejection history -- and only genuinely removed endpoints are drained.
type Manager struct {
	log *slog.Logger

	// dial, when set, replaces the real connection pool on every endpoint. The
	// simulation harness uses this to run the entire proxy against a scripted
	// network with no sockets involved.
	dial Dialer

	mu       sync.RWMutex
	clusters map[string]*Cluster
	version  string
}

// NewManager returns an empty Manager.
func NewManager(log *slog.Logger) *Manager {
	if log == nil {
		log = slog.Default()
	}
	return &Manager{log: log, clusters: map[string]*Cluster{}}
}

// SetDialer installs a custom RoundTripper factory. It must be called before
// the first Apply.
func (m *Manager) SetDialer(d Dialer) {
	m.mu.Lock()
	m.dial = d
	m.mu.Unlock()
}

// Get looks up a cluster by name.
func (m *Manager) Get(name string) (*Cluster, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c, ok := m.clusters[name]
	return c, ok
}

// All returns every live cluster.
func (m *Manager) All() []*Cluster {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Cluster, 0, len(m.clusters))
	for _, c := range m.clusters {
		out = append(out, c)
	}
	return out
}

// Version reports the currently applied snapshot version.
func (m *Manager) Version() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.version
}

// ApplyStats describes what a config push actually changed.
type ApplyStats struct {
	ClustersAdded     int
	ClustersRemoved   int
	ClustersUpdated   int
	EndpointsAdded    int
	EndpointsRemoved  int
	EndpointsRetained int
}

// Apply reconciles the manager against a snapshot. It never partially applies:
// the snapshot is validated by the caller first, and every cluster is built
// before anything is swapped in.
func (m *Manager) Apply(ctx context.Context, snap *config.Snapshot, now time.Time) (ApplyStats, error) {
	var stats ApplyStats

	m.mu.Lock()
	prev := m.clusters
	next := make(map[string]*Cluster, len(snap.Clusters))
	var toDrain []*Endpoint
	var drainTimeout time.Duration

	for i := range snap.Clusters {
		cc := snap.Clusters[i]
		c, found := prev[cc.Name]
		if found {
			stats.ClustersUpdated++
		} else {
			c = &Cluster{Name: cc.Name}
			stats.ClustersAdded++
		}

		st, err := c.reconcile(cc, now, m.dial)
		if err != nil {
			m.mu.Unlock()
			return stats, err
		}
		stats.EndpointsAdded += st.added
		stats.EndpointsRetained += st.retained
		stats.EndpointsRemoved += len(st.removed)
		toDrain = append(toDrain, st.removed...)

		if d := cc.DrainTimeout.Or(15 * time.Second); d > drainTimeout {
			drainTimeout = d
		}
		next[cc.Name] = c
	}

	for name, c := range prev {
		if _, ok := next[name]; ok {
			continue
		}
		stats.ClustersRemoved++
		toDrain = append(toDrain, c.Endpoints()...)
	}

	// Take removed endpoints out of rotation synchronously, before the swap, so
	// no request can be routed to one after it has been retired.
	for _, e := range toDrain {
		e.markDraining()
	}

	// The atomic swap. Requests already in flight finish against the old
	// cluster objects they captured; new requests see the new map.
	m.clusters = next
	m.version = snap.Version
	m.mu.Unlock()

	if drainTimeout == 0 {
		drainTimeout = 15 * time.Second
	}
	for _, e := range toDrain {
		go e.drain(ctx, drainTimeout)
	}

	m.log.Info("config applied",
		"version", snap.Version,
		"clusters_added", stats.ClustersAdded,
		"clusters_updated", stats.ClustersUpdated,
		"clusters_removed", stats.ClustersRemoved,
		"endpoints_added", stats.EndpointsAdded,
		"endpoints_retained", stats.EndpointsRetained,
		"endpoints_removed", stats.EndpointsRemoved,
	)
	return stats, nil
}

// Shutdown drains every endpoint in every cluster.
func (m *Manager) Shutdown(ctx context.Context, timeout time.Duration) {
	var wg sync.WaitGroup
	for _, c := range m.All() {
		for _, e := range c.Endpoints() {
			e.markDraining()
			wg.Add(1)
			go func(e *Endpoint) {
				defer wg.Done()
				e.drain(ctx, timeout)
			}(e)
		}
	}
	wg.Wait()
}
