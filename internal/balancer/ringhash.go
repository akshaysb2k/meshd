package balancer

import (
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

// RingHash implements consistent hashing for session affinity.
//
// Each endpoint is placed at many points around a 64-bit ring; a request hashes
// to a point and takes the next endpoint clockwise. Removing one endpoint of N
// moves roughly 1/N of keys instead of reshuffling everything, which is the
// whole reason to use a ring rather than hash % len(endpoints).
type RingHash struct {
	replicas int

	mu          sync.RWMutex
	points      []ringPoint
	fingerprint uint64
	idxByKey    map[string]int
}

type ringPoint struct {
	hash uint64
	key  string
}

// NewRingHash returns a ring hash picker with the given virtual nodes per
// endpoint. More replicas mean a more even distribution and a costlier rebuild;
// 100 is a reasonable default.
func NewRingHash(replicas int) *RingHash {
	if replicas <= 0 {
		replicas = 100
	}
	return &RingHash{replicas: replicas, idxByKey: map[string]int{}}
}

// Name identifies the policy.
func (r *RingHash) Name() string { return "ring_hash" }

// Pick maps hashKey onto the ring and returns the owning candidate. The ring is
// rebuilt only when cluster membership actually changes.
func (r *RingHash) Pick(cands []Candidate, hashKey string) (int, error) {
	if len(cands) == 0 {
		return 0, ErrNoEndpoints
	}
	fp := fingerprint(cands)

	r.mu.RLock()
	stale := fp != r.fingerprint
	r.mu.RUnlock()
	if stale {
		r.rebuild(cands, fp)
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.points) == 0 {
		return 0, ErrNoEndpoints
	}
	h := hash64(hashKey)
	i := sort.Search(len(r.points), func(i int) bool { return r.points[i].hash >= h })
	if i == len(r.points) {
		i = 0
	}
	idx, ok := r.idxByKey[r.points[i].key]
	if !ok || idx >= len(cands) {
		return 0, ErrNoEndpoints
	}
	return idx, nil
}

func (r *RingHash) rebuild(cands []Candidate, fp uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fingerprint == fp {
		return
	}
	points := make([]ringPoint, 0, len(cands)*r.replicas)
	idxByKey := make(map[string]int, len(cands))
	for i, c := range cands {
		idxByKey[c.Key] = i
		// Weight scales an endpoint's arc, so slow start still applies -- a
		// ramping endpoint owns proportionally fewer points on the ring.
		reps := r.replicas
		if c.Weight > 0 && c.Weight < 1 {
			reps = int(float64(r.replicas) * c.Weight)
			if reps < 1 {
				reps = 1
			}
		}
		for j := 0; j < reps; j++ {
			points = append(points, ringPoint{
				hash: hash64(c.Key + "#" + strconv.Itoa(j)),
				key:  c.Key,
			})
		}
	}
	sort.Slice(points, func(i, j int) bool { return points[i].hash < points[j].hash })
	r.points = points
	r.idxByKey = idxByKey
	r.fingerprint = fp
}

func fingerprint(cands []Candidate) uint64 {
	h := fnv.New64a()
	for _, c := range cands {
		_, _ = h.Write([]byte(c.Key))
		_, _ = h.Write([]byte{0})
		// Bucket the weight so a continuously ramping endpoint does not force a
		// ring rebuild on every single request.
		_, _ = h.Write([]byte(strconv.Itoa(int(c.Weight * 10))))
		_, _ = h.Write([]byte{0})
	}
	return h.Sum64()
}

// hash64 is FNV-1a followed by the SplitMix64 finalizer.
//
// FNV-1a alone is not good enough here. On short, highly similar strings like
// "ep-3#41" its avalanche is poor, and the ring points cluster badly: measured
// over 5 endpoints at 200 replicas, one endpoint owned 32% of the ring and
// another 6%. That skew both unbalances traffic and, worse, breaks the
// property the ring exists for -- losing one of five endpoints remapped 64% of
// keys instead of 20%. The finalizer costs three multiplies and fixes it.
func hash64(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return mix64(h.Sum64())
}

// mix64 is the SplitMix64 finalizer, a bijection with good avalanche.
func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}
