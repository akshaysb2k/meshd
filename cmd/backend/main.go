// Command backend is a controllable upstream for demos and chaos testing. Its
// failure behaviour can be changed at runtime, which is what makes the ejection
// and recovery demo reproducible.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

type state struct {
	name      string
	errorRate atomic.Uint64 // parts per million
	latencyMS atomic.Int64
	jitterMS  atomic.Int64
	healthy   atomic.Bool
	blackhole atomic.Bool
	served    atomic.Int64
	failed    atomic.Int64
}

func main() {
	addr := flag.String("addr", ":9001", "listen address")
	name := flag.String("name", "backend", "instance name echoed in responses")
	latency := flag.Int64("latency-ms", 5, "base response latency")
	jitter := flag.Int64("jitter-ms", 3, "uniform jitter added to latency")
	errRate := flag.Float64("error-rate", 0, "fraction of requests answered with 503")
	flag.Parse()

	if v := os.Getenv("BACKEND_NAME"); v != "" {
		*name = v
	}

	s := &state{name: *name}
	s.latencyMS.Store(*latency)
	s.jitterMS.Store(*jitter)
	s.errorRate.Store(uint64(*errRate * 1e6))
	s.healthy.Store(true)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if !s.healthy.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("unhealthy\n"))
			return
		}
		_, _ = w.Write([]byte("ok\n"))
	})

	// _control mutates behaviour at runtime: flip a backend to failing, add
	// latency, or black-hole it entirely to simulate a hung process.
	mux.HandleFunc("/_control", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if v := q.Get("error_rate"); v != "" {
			f, _ := strconv.ParseFloat(v, 64)
			s.errorRate.Store(uint64(f * 1e6))
		}
		if v := q.Get("latency_ms"); v != "" {
			n, _ := strconv.ParseInt(v, 10, 64)
			s.latencyMS.Store(n)
		}
		if v := q.Get("healthy"); v != "" {
			s.healthy.Store(v == "true" || v == "1")
		}
		if v := q.Get("blackhole"); v != "" {
			s.blackhole.Store(v == "true" || v == "1")
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"name":       s.name,
			"error_rate": float64(s.errorRate.Load()) / 1e6,
			"latency_ms": s.latencyMS.Load(),
			"healthy":    s.healthy.Load(),
			"blackhole":  s.blackhole.Load(),
			"served":     s.served.Load(),
			"failed":     s.failed.Load(),
		})
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if s.blackhole.Load() {
			// Never respond. The client's timeout must be what ends this
			// request, which is exactly the case per-try timeouts exist for.
			<-r.Context().Done()
			return
		}
		d := time.Duration(s.latencyMS.Load()) * time.Millisecond
		if j := s.jitterMS.Load(); j > 0 {
			d += time.Duration(rand.Int64N(j)) * time.Millisecond
		}
		select {
		case <-time.After(d):
		case <-r.Context().Done():
			return
		}
		w.Header().Set("X-Backend", s.name)
		if rand.Uint64N(1e6) < s.errorRate.Load() {
			s.failed.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = fmt.Fprintf(w, "%s: injected failure\n", s.name)
			return
		}
		s.served.Add(1)
		_, _ = fmt.Fprintf(w, "%s: ok path=%s\n", s.name, r.URL.Path)
	})

	log.Printf("backend %s listening on %s", *name, *addr)
	srv := &http.Server{Addr: *addr, Handler: mux}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
