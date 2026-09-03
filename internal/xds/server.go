package xds

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/meshd/meshd/internal/config"
)

// Server broadcasts config snapshots to every connected data plane.
type Server struct {
	Log *slog.Logger

	mu          sync.RWMutex
	current     []byte
	version     string
	subscribers map[chan []byte]struct{}
}

// NewServer returns an empty broadcast server.
func NewServer(log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{Log: log, subscribers: map[chan []byte]struct{}{}}
}

// Publish validates and broadcasts a snapshot. Validation happens here as well
// as in the data plane so an operator sees the error at push time rather than
// discovering it in a proxy log.
func (s *Server) Publish(snap *config.Snapshot) error {
	if err := snap.Validate(); err != nil {
		return err
	}
	b, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	b = append(b, '\n')

	s.mu.Lock()
	s.current = b
	s.version = snap.Version
	subs := make([]chan []byte, 0, len(s.subscribers))
	for ch := range s.subscribers {
		subs = append(subs, ch)
	}
	s.mu.Unlock()

	for _, ch := range subs {
		select {
		case ch <- b:
		default:
			// A subscriber too slow to keep up will get the current snapshot on
			// reconnect; blocking the publisher on it would stall every other
			// data plane instance.
		}
	}
	s.Log.Info("snapshot published", "version", snap.Version, "subscribers", len(subs))
	return nil
}

// Version returns the currently published version.
func (s *Server) Version() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.version
}

// Handler serves the snapshot stream.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/config/stream", s.stream)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, r *http.Request) {
		s.mu.RLock()
		cur := s.current
		s.mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(cur)
	})
	return mux
}

func (s *Server) stream(w http.ResponseWriter, r *http.Request) {
	node := r.Header.Get("X-Node-Id")
	ch := make(chan []byte, 4)

	s.mu.Lock()
	s.subscribers[ch] = struct{}{}
	cur := s.current
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.subscribers, ch)
		s.mu.Unlock()
	}()

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	rc := http.NewResponseController(w)

	// Send the current snapshot immediately so a reconnecting proxy converges
	// without waiting for the next change.
	if len(cur) > 0 {
		if _, err := w.Write(cur); err != nil {
			return
		}
		_ = rc.Flush()
	}
	s.Log.Info("subscriber connected", "node", node)

	keepalive := time.NewTicker(20 * time.Second)
	defer keepalive.Stop()
	for {
		select {
		case b := <-ch:
			if _, err := w.Write(b); err != nil {
				return
			}
			_ = rc.Flush()
		case <-keepalive.C:
			if _, err := w.Write([]byte("\n")); err != nil {
				return
			}
			_ = rc.Flush()
		case <-r.Context().Done():
			s.Log.Info("subscriber disconnected", "node", node)
			return
		}
	}
}
