// Command loadgen drives constant-rate load and reports latency percentiles
// plus a per-second timeline, so a failover can be measured rather than
// eyeballed.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"
)

type sample struct {
	at     time.Duration
	rtt    time.Duration
	status int
	err    bool
}

func main() {
	var (
		target  = flag.String("url", "http://localhost:8080/", "target URL")
		qps     = flag.Int("qps", 200, "requests per second")
		dur     = flag.Duration("duration", 30*time.Second, "test duration")
		conns   = flag.Int("connections", 64, "max idle connections")
		csvPath = flag.String("csv", "", "write the per-second timeline to this file")
		host    = flag.String("host", "", "override the Host header")
	)
	flag.Parse()

	tr := &http.Transport{MaxIdleConns: *conns, MaxIdleConnsPerHost: *conns, IdleConnTimeout: 90 * time.Second}
	client := &http.Client{Transport: tr, Timeout: 10 * time.Second}

	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()

	var mu sync.Mutex
	var samples []sample
	start := time.Now()

	// A fixed-rate ticker rather than a fixed worker pool: closed-loop load
	// generators hide latency regressions because slow responses reduce the
	// offered rate, which is the opposite of what happens in production.
	interval := time.Second / time.Duration(*qps)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var wg sync.WaitGroup
	for {
		select {
		case <-ctx.Done():
			goto done
		case <-ticker.C:
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			t0 := time.Now()
			req, _ := http.NewRequest(http.MethodGet, *target, nil)
			if *host != "" {
				req.Host = *host
			}
			resp, err := client.Do(req)
			s := sample{at: t0.Sub(start), rtt: time.Since(t0)}
			if err != nil {
				s.err = true
			} else {
				s.status = resp.StatusCode
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
			mu.Lock()
			samples = append(samples, s)
			mu.Unlock()
		}()
	}
done:
	wg.Wait()
	report(samples, *csvPath)
}

func report(samples []sample, csvPath string) {
	if len(samples) == 0 {
		fmt.Println("no samples")
		return
	}
	rtts := make([]time.Duration, 0, len(samples))
	byCode := map[int]int{}
	errs := 0
	for _, s := range samples {
		rtts = append(rtts, s.rtt)
		if s.err {
			errs++
			continue
		}
		byCode[s.status]++
	}
	sort.Slice(rtts, func(i, j int) bool { return rtts[i] < rtts[j] })

	ok := 0
	for c, n := range byCode {
		if c < 500 {
			ok += n
		}
	}
	fmt.Printf("requests   %d\n", len(samples))
	fmt.Printf("success    %d (%.3f%%)\n", ok, 100*float64(ok)/float64(len(samples)))
	fmt.Printf("errors     %d transport, %d 5xx\n", errs, len(samples)-ok-errs)
	fmt.Printf("p50        %s\n", pct(rtts, 0.50))
	fmt.Printf("p90        %s\n", pct(rtts, 0.90))
	fmt.Printf("p99        %s\n", pct(rtts, 0.99))
	fmt.Printf("p999       %s\n", pct(rtts, 0.999))
	fmt.Printf("max        %s\n", rtts[len(rtts)-1])
	fmt.Print("codes      ")
	codes := make([]int, 0, len(byCode))
	for c := range byCode {
		codes = append(codes, c)
	}
	sort.Ints(codes)
	for _, c := range codes {
		fmt.Printf("%d=%d ", c, byCode[c])
	}
	fmt.Println()

	if csvPath == "" {
		return
	}
	buckets := map[int64]*struct {
		n, fail int
		rtts    []time.Duration
	}{}
	for _, s := range samples {
		k := int64(s.at.Seconds())
		b := buckets[k]
		if b == nil {
			b = &struct {
				n, fail int
				rtts    []time.Duration
			}{}
			buckets[k] = b
		}
		b.n++
		if s.err || s.status >= 500 {
			b.fail++
		}
		b.rtts = append(b.rtts, s.rtt)
	}
	keys := make([]int64, 0, len(buckets))
	for k := range buckets {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	f, err := os.Create(csvPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	defer func() { _ = f.Close() }()
	fmt.Fprintln(f, "second,requests,failures,p50_ms,p99_ms")
	for _, k := range keys {
		b := buckets[k]
		sort.Slice(b.rtts, func(i, j int) bool { return b.rtts[i] < b.rtts[j] })
		fmt.Fprintf(f, "%d,%d,%d,%.2f,%.2f\n", k, b.n, b.fail,
			float64(pct(b.rtts, 0.5).Microseconds())/1000.0,
			float64(pct(b.rtts, 0.99).Microseconds())/1000.0)
	}
	fmt.Printf("timeline   %s\n", csvPath)
}

func pct(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)-1) * q)
	return sorted[i]
}
