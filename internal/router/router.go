// Package router matches requests to routes. The table is immutable once built;
// a config push produces a whole new table that is swapped in atomically.
package router

import (
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/meshd/meshd/internal/config"
	"github.com/meshd/meshd/internal/retry"
)

// Route is a compiled routing rule.
type Route struct {
	Name          string
	Cluster       string
	Timeout       time.Duration
	Retry         *retry.Policy
	Hedge         *HedgePolicy
	HashOn        string
	PrefixRewrite string

	host       string
	hostSuffix string // set when the match is a *.example.com wildcard
	pathPrefix string
	methods    map[string]bool
	headers    map[string]string
}

// HedgePolicy is the compiled hedging configuration for a route.
type HedgePolicy struct {
	Delay     time.Duration
	MaxHedges int
}

// Table is an ordered set of compiled routes.
type Table struct {
	routes []*Route
}

// Build compiles a routing table from a snapshot.
//
// Routes are ordered by descending path prefix length so that a specific rule
// like /api/v2/payments wins over a catch-all /, regardless of the order they
// appear in the config file. Relying on file order is a classic source of
// "my new route silently does nothing" incidents.
func Build(snap *config.Snapshot) *Table {
	routes := make([]*Route, 0, len(snap.Routes))
	for i := range snap.Routes {
		rc := snap.Routes[i]
		r := &Route{
			Name:          rc.Name,
			Cluster:       rc.Cluster,
			Timeout:       rc.Timeout.Or(15 * time.Second),
			Retry:         retry.NewPolicy(rc.Retry),
			HashOn:        rc.HashOn,
			PrefixRewrite: rc.PrefixRewrite,
			pathPrefix:    rc.Match.PathPrefix,
			headers:       map[string]string{},
		}
		if h := strings.ToLower(rc.Match.Host); h != "" {
			if strings.HasPrefix(h, "*.") {
				r.hostSuffix = h[1:]
			} else {
				r.host = h
			}
		}
		if len(rc.Match.Methods) > 0 {
			r.methods = map[string]bool{}
			for _, m := range rc.Match.Methods {
				r.methods[strings.ToUpper(m)] = true
			}
		}
		for k, v := range rc.Match.Headers {
			r.headers[http.CanonicalHeaderKey(k)] = v
		}
		if rc.Hedge != nil {
			r.Hedge = &HedgePolicy{
				Delay:     rc.Hedge.Delay.Or(0),
				MaxHedges: rc.Hedge.MaxHedges,
			}
			if r.Hedge.MaxHedges < 1 {
				r.Hedge.MaxHedges = 1
			}
		}
		routes = append(routes, r)
	}
	sort.SliceStable(routes, func(i, j int) bool {
		li, lj := len(routes[i].pathPrefix), len(routes[j].pathPrefix)
		if li != lj {
			return li > lj
		}
		// A host-qualified route is more specific than a host-agnostic one.
		si := routes[i].host != "" || routes[i].hostSuffix != ""
		sj := routes[j].host != "" || routes[j].hostSuffix != ""
		return si && !sj
	})
	return &Table{routes: routes}
}

// Match returns the first route satisfying the request.
func (t *Table) Match(r *http.Request) (*Route, bool) {
	if t == nil {
		return nil, false
	}
	host := stripPort(strings.ToLower(r.Host))
	for _, rt := range t.routes {
		if rt.host != "" && rt.host != host {
			continue
		}
		if rt.hostSuffix != "" && !strings.HasSuffix(host, rt.hostSuffix) {
			continue
		}
		if rt.pathPrefix != "" && !strings.HasPrefix(r.URL.Path, rt.pathPrefix) {
			continue
		}
		if rt.methods != nil && !rt.methods[r.Method] {
			continue
		}
		ok := true
		for k, v := range rt.headers {
			if r.Header.Get(k) != v {
				ok = false
				break
			}
		}
		if !ok {
			continue
		}
		return rt, true
	}
	return nil, false
}

// Routes returns the compiled routes in match order.
func (t *Table) Routes() []*Route {
	if t == nil {
		return nil
	}
	return append([]*Route(nil), t.routes...)
}

// RewritePath applies the route's prefix rewrite to a path.
func (r *Route) RewritePath(path string) string {
	if r.PrefixRewrite == "" || r.pathPrefix == "" {
		return path
	}
	if !strings.HasPrefix(path, r.pathPrefix) {
		return path
	}
	// Trim the rewrite's trailing slash before joining, or rewriting /api/v1 to
	// "/" turns /api/v1/users into //users, which many upstream frameworks
	// treat as a different (and usually 404) route.
	out := strings.TrimSuffix(r.PrefixRewrite, "/") + strings.TrimPrefix(path, r.pathPrefix)
	if out == "" {
		return "/"
	}
	if !strings.HasPrefix(out, "/") {
		out = "/" + out
	}
	return out
}

func stripPort(host string) string {
	if i := strings.LastIndex(host, ":"); i > -1 && !strings.Contains(host[i:], "]") {
		return host[:i]
	}
	return host
}
