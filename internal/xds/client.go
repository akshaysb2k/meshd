// Package xds carries config snapshots from the control plane to the data
// plane over a long-lived stream of newline-delimited JSON.
//
// A stream rather than polling, because the useful property is "how stale can
// config be" and polling makes that equal to the poll interval even when
// nothing changes. Reconnects use jittered backoff so a control plane restart
// does not get hit by every data plane instance simultaneously.
package xds

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/meshd/meshd/internal/config"
)

// Applier consumes validated snapshots.
type Applier interface {
	Apply(ctx context.Context, snap *config.Snapshot) error
}

// Client streams snapshots from a control plane endpoint.
type Client struct {
	URL     string
	Node    string
	Log     *slog.Logger
	Applier Applier

	MinBackoff time.Duration
	MaxBackoff time.Duration
}

// Run streams until ctx is cancelled, reconnecting on failure.
func (c *Client) Run(ctx context.Context) error {
	log := c.Log
	if log == nil {
		log = slog.Default()
	}
	min := c.MinBackoff
	if min == 0 {
		min = 250 * time.Millisecond
	}
	max := c.MaxBackoff
	if max == 0 {
		max = 15 * time.Second
	}

	backoff := min
	for {
		err := c.stream(ctx, log)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			log.Warn("control plane stream ended", "error", err, "retry_in", backoff.String())
		}
		// Full jitter on reconnect. Without it, every proxy that lost the
		// control plane reconnects in lockstep and knocks it over again.
		wait := time.Duration(rand.Int64N(int64(backoff) + 1))
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return nil
		}
		backoff *= 2
		if backoff > max {
			backoff = max
		}
	}
}

func (c *Client) stream(ctx context.Context, log *slog.Logger) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.URL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Node-Id", c.Node)
	client := &http.Client{Timeout: 0}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("control plane returned %s", resp.Status)
	}
	log.Info("control plane stream established", "url", c.URL)

	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var snap config.Snapshot
		if err := json.Unmarshal(line, &snap); err != nil {
			log.Error("malformed snapshot from control plane", "error", err)
			continue
		}
		// A bad push must not take the proxy down: log it, keep the running
		// config, and stay connected so the operator can push a fix.
		if err := c.Applier.Apply(ctx, &snap); err != nil {
			log.Error("snapshot rejected", "version", snap.Version, "error", err)
			continue
		}
		log.Info("snapshot applied", "version", snap.Version)
	}
	if err := sc.Err(); err != nil {
		return err
	}
	return errors.New("stream closed by peer")
}
