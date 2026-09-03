// Package balancer implements endpoint selection policies.
//
// Pickers operate on a slice of Candidates and return an index rather than an
// endpoint object. That keeps this package free of any dependency on the
// cluster package, so both can be tested in isolation.
package balancer

import "errors"

// ErrNoEndpoints is returned when the candidate set is empty.
var ErrNoEndpoints = errors.New("balancer: no available endpoints")

// Candidate is one selectable endpoint as seen by a picker.
type Candidate struct {
	// Key is a stable identity, used for consistent hashing and for carrying
	// per-endpoint balancer state across calls.
	Key string
	// Active is the number of in-flight requests, used by least_request.
	Active int64
	// Weight is the endpoint's share in (0,1], reduced during slow start.
	Weight float64
}

// Picker chooses one candidate. hashKey is the affinity key and is ignored by
// policies that do not hash.
type Picker interface {
	Pick(cands []Candidate, hashKey string) (int, error)
	Name() string
}

// New builds a picker by policy name. An unknown or empty name yields
// least_request, which is the right default: it costs O(1) and tracks real
// backend load instead of assuming every request is equal.
func New(policy string, ringReplicas int) Picker {
	switch policy {
	case "round_robin":
		return NewRoundRobin()
	case "ring_hash":
		return NewRingHash(ringReplicas)
	case "least_request", "":
		return NewLeastRequest(2)
	default:
		return NewLeastRequest(2)
	}
}
