package router

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/meshd/meshd/internal/config"
)

func table(t *testing.T, routes ...config.Route) *Table {
	t.Helper()
	return Build(&config.Snapshot{Version: "t", Routes: routes})
}

func req(method, target, host string) *http.Request {
	r := httptest.NewRequest(method, target, nil)
	if host != "" {
		r.Host = host
	}
	return r
}

func TestMostSpecificPrefixWinsRegardlessOfFileOrder(t *testing.T) {
	// The catch-all is listed first, as it often is in a hand-edited config.
	// Matching in file order would make the specific route dead config.
	tb := table(t,
		config.Route{Name: "catchall", Cluster: "c", Match: config.RouteMatch{PathPrefix: "/"}},
		config.Route{Name: "payments", Cluster: "c", Match: config.RouteMatch{PathPrefix: "/api/v2/payments"}},
		config.Route{Name: "api", Cluster: "c", Match: config.RouteMatch{PathPrefix: "/api"}},
	)
	cases := map[string]string{
		"/api/v2/payments/charge": "payments",
		"/api/v2/users":           "api",
		"/static/logo.png":        "catchall",
	}
	for path, want := range cases {
		got, ok := tb.Match(req("GET", path, ""))
		if !ok || got.Name != want {
			t.Fatalf("%s matched %v, want %s", path, got, want)
		}
	}
}

func TestHostAndWildcardMatching(t *testing.T) {
	tb := table(t,
		config.Route{Name: "exact", Cluster: "c", Match: config.RouteMatch{Host: "api.example.com", PathPrefix: "/"}},
		config.Route{Name: "wild", Cluster: "c", Match: config.RouteMatch{Host: "*.example.com", PathPrefix: "/"}},
		config.Route{Name: "any", Cluster: "c", Match: config.RouteMatch{PathPrefix: "/"}},
	)
	for host, want := range map[string]string{
		"api.example.com":      "exact",
		"staging.example.com":  "wild",
		"api.example.com:8080": "exact",
		"other.org":            "any",
	} {
		got, ok := tb.Match(req("GET", "/x", host))
		if !ok || got.Name != want {
			t.Fatalf("host %s matched %v, want %s", host, got, want)
		}
	}
}

func TestMethodAndHeaderMatching(t *testing.T) {
	tb := table(t,
		config.Route{Name: "canary", Cluster: "c", Match: config.RouteMatch{
			PathPrefix: "/", Methods: []string{"GET"}, Headers: map[string]string{"x-canary": "true"},
		}},
		config.Route{Name: "stable", Cluster: "c", Match: config.RouteMatch{PathPrefix: "/"}},
	)
	r := req("GET", "/x", "")
	r.Header.Set("X-Canary", "true")
	if got, _ := tb.Match(r); got.Name != "canary" {
		t.Fatalf("canary header did not select the canary route, got %s", got.Name)
	}
	if got, _ := tb.Match(req("GET", "/x", "")); got.Name != "stable" {
		t.Fatalf("missing header should fall through to stable, got %s", got.Name)
	}
	p := req("POST", "/x", "")
	p.Header.Set("X-Canary", "true")
	if got, _ := tb.Match(p); got.Name != "stable" {
		t.Fatalf("method mismatch should fall through, got %s", got.Name)
	}
}

func TestPrefixRewrite(t *testing.T) {
	tb := table(t, config.Route{
		Name: "api", Cluster: "c",
		Match:         config.RouteMatch{PathPrefix: "/api/v1"},
		PrefixRewrite: "/",
	})
	rt, ok := tb.Match(req("GET", "/api/v1/users/42", ""))
	if !ok {
		t.Fatal("no match")
	}
	if got := rt.RewritePath("/api/v1/users/42"); got != "/users/42" {
		t.Fatalf("rewrote to %q, want /users/42", got)
	}
	if got := rt.RewritePath("/api/v1"); got != "/" {
		t.Fatalf("rewrote bare prefix to %q, want /", got)
	}
}

func TestNoMatchIsReported(t *testing.T) {
	tb := table(t, config.Route{Name: "api", Cluster: "c", Match: config.RouteMatch{PathPrefix: "/api"}})
	if _, ok := tb.Match(req("GET", "/other", "")); ok {
		t.Fatal("unrelated path matched")
	}
}
