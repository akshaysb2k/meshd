package health

import "github.com/meshd/meshd/internal/metrics"

// Metrics holds the health subsystem's metric families. They are registered
// once and shared by every prober and detector.
type Metrics struct {
	Checks   *metrics.Counter
	Status   *metrics.Gauge
	Duration *metrics.Histogram
	Ejects   *metrics.Counter
	Ejected  *metrics.Gauge
}

// NewMetrics registers the health families on a registry.
func NewMetrics(reg *metrics.Registry) *Metrics {
	return &Metrics{
		Checks: reg.NewCounter("meshd_health_checks_total",
			"Active health check attempts.", "cluster", "endpoint", "result"),
		Status: reg.NewGauge("meshd_endpoint_healthy",
			"1 when the endpoint passes active health checks.", "cluster", "endpoint"),
		Duration: reg.NewHistogram("meshd_health_check_duration_seconds",
			"Active health check latency.", nil, "cluster"),
		Ejects: reg.NewCounter("meshd_outlier_ejections_total",
			"Endpoints ejected by passive outlier detection.", "cluster", "endpoint", "reason"),
		Ejected: reg.NewGauge("meshd_endpoints_ejected",
			"Endpoints currently ejected.", "cluster"),
	}
}
