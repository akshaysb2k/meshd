// Package metrics is a dependency-free Prometheus-compatible registry. It
// supports counters, gauges and histograms with a fixed label schema per
// metric, and renders the text exposition format.
package metrics

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Registry holds every metric family exposed on /metrics.
type Registry struct {
	mu       sync.RWMutex
	families map[string]*family
	ordered  []*family
}

type family struct {
	name    string
	help    string
	typ     string
	labels  []string
	buckets []float64

	mu     sync.RWMutex
	series map[string]*seriesEntry
	order  []*seriesEntry
}

type seriesEntry struct {
	labelValues []string
	counter     atomic.Int64 // fixed point, scaled by 1000
	gauge       atomic.Int64 // fixed point, scaled by 1000
	bucketHits  []atomic.Int64
	sum         atomic.Int64 // fixed point, scaled by 1000
	count       atomic.Int64
}

const scale = 1000.0

// New returns an empty Registry.
func New() *Registry { return &Registry{families: map[string]*family{}} }

func (r *Registry) family(name, help, typ string, labels []string, buckets []float64) *family {
	r.mu.Lock()
	defer r.mu.Unlock()
	if f, ok := r.families[name]; ok {
		return f
	}
	f := &family{
		name: name, help: help, typ: typ,
		labels: labels, buckets: buckets,
		series: map[string]*seriesEntry{},
	}
	r.families[name] = f
	r.ordered = append(r.ordered, f)
	return f
}

func (f *family) entry(values []string) *seriesEntry {
	key := strings.Join(values, "\x00")
	f.mu.RLock()
	e, ok := f.series[key]
	f.mu.RUnlock()
	if ok {
		return e
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if e, ok := f.series[key]; ok {
		return e
	}
	e = &seriesEntry{labelValues: append([]string(nil), values...)}
	if len(f.buckets) > 0 {
		e.bucketHits = make([]atomic.Int64, len(f.buckets)+1)
	}
	f.series[key] = e
	f.order = append(f.order, e)
	return e
}

// Counter is a monotonically increasing metric family.
type Counter struct{ f *family }

// NewCounter registers a counter family with the given label names.
func (r *Registry) NewCounter(name, help string, labels ...string) *Counter {
	return &Counter{r.family(name, help, "counter", labels, nil)}
}

// With selects the series for a set of label values, in schema order.
func (c *Counter) With(values ...string) *CounterSeries {
	return &CounterSeries{c.f.entry(values)}
}

// CounterSeries is one label combination of a Counter.
type CounterSeries struct{ e *seriesEntry }

// Inc adds one.
func (c *CounterSeries) Inc() { c.e.counter.Add(int64(scale)) }

// Add increases the counter by v.
func (c *CounterSeries) Add(v float64) { c.e.counter.Add(int64(v * scale)) }

// Gauge is a metric family that can go up and down.
type Gauge struct{ f *family }

// NewGauge registers a gauge family.
func (r *Registry) NewGauge(name, help string, labels ...string) *Gauge {
	return &Gauge{r.family(name, help, "gauge", labels, nil)}
}

// With selects the series for a set of label values.
func (g *Gauge) With(values ...string) *GaugeSeries { return &GaugeSeries{g.f.entry(values)} }

// GaugeSeries is one label combination of a Gauge.
type GaugeSeries struct{ e *seriesEntry }

// Set overwrites the current value.
func (g *GaugeSeries) Set(v float64) { g.e.gauge.Store(int64(v * scale)) }

// Add adds a delta, which may be negative.
func (g *GaugeSeries) Add(v float64) { g.e.gauge.Add(int64(v * scale)) }

// Histogram is a cumulative-bucket latency family.
type Histogram struct{ f *family }

// DefaultBuckets covers sub-millisecond through multi-second proxy latency.
var DefaultBuckets = []float64{
	0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
}

// NewHistogram registers a histogram family with explicit upper bounds.
func (r *Registry) NewHistogram(name, help string, buckets []float64, labels ...string) *Histogram {
	if len(buckets) == 0 {
		buckets = DefaultBuckets
	}
	sorted := append([]float64(nil), buckets...)
	sort.Float64s(sorted)
	return &Histogram{r.family(name, help, "histogram", labels, sorted)}
}

// With selects the series for a set of label values.
func (h *Histogram) With(values ...string) *HistogramSeries {
	return &HistogramSeries{e: h.f.entry(values), buckets: h.f.buckets}
}

// HistogramSeries is one label combination of a Histogram.
type HistogramSeries struct {
	e       *seriesEntry
	buckets []float64
}

// Observe records a single sample, in seconds.
func (h *HistogramSeries) Observe(v float64) {
	idx := sort.SearchFloat64s(h.buckets, v)
	h.e.bucketHits[idx].Add(1)
	h.e.sum.Add(int64(v * scale))
	h.e.count.Add(1)
}

// WriteText renders the registry in Prometheus text exposition format.
func (r *Registry) WriteText(w io.Writer) error {
	r.mu.RLock()
	fams := append([]*family(nil), r.ordered...)
	r.mu.RUnlock()

	var b strings.Builder
	for _, f := range fams {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", f.name, f.help, f.name, f.typ)
		f.mu.RLock()
		entries := append([]*seriesEntry(nil), f.order...)
		f.mu.RUnlock()
		sort.Slice(entries, func(i, j int) bool {
			return strings.Join(entries[i].labelValues, "\x00") < strings.Join(entries[j].labelValues, "\x00")
		})
		for _, e := range entries {
			lbl := renderLabels(f.labels, e.labelValues, "", "")
			switch f.typ {
			case "counter":
				fmt.Fprintf(&b, "%s%s %s\n", f.name, lbl, fmtFloat(float64(e.counter.Load())/scale))
			case "gauge":
				fmt.Fprintf(&b, "%s%s %s\n", f.name, lbl, fmtFloat(float64(e.gauge.Load())/scale))
			case "histogram":
				var cum int64
				for i, ub := range f.buckets {
					cum += e.bucketHits[i].Load()
					bl := renderLabels(f.labels, e.labelValues, "le", fmtFloat(ub))
					fmt.Fprintf(&b, "%s_bucket%s %d\n", f.name, bl, cum)
				}
				cum += e.bucketHits[len(f.buckets)].Load()
				bl := renderLabels(f.labels, e.labelValues, "le", "+Inf")
				fmt.Fprintf(&b, "%s_bucket%s %d\n", f.name, bl, cum)
				fmt.Fprintf(&b, "%s_sum%s %s\n", f.name, lbl, fmtFloat(float64(e.sum.Load())/scale))
				fmt.Fprintf(&b, "%s_count%s %d\n", f.name, lbl, e.count.Load())
			}
		}
	}
	_, err := io.WriteString(w, b.String())
	return err
}

func renderLabels(names, values []string, extraName, extraValue string) string {
	if len(names) == 0 && extraName == "" {
		return ""
	}
	var parts []string
	for i, n := range names {
		v := ""
		if i < len(values) {
			v = values[i]
		}
		parts = append(parts, n+`="`+escape(v)+`"`)
	}
	if extraName != "" {
		parts = append(parts, extraName+`="`+extraValue+`"`)
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func escape(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	return strings.ReplaceAll(s, "\n", `\n`)
}

func fmtFloat(f float64) string {
	if math.IsInf(f, 1) {
		return "+Inf"
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}
