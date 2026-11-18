package monitoring

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

type PrometheusMetricsService struct {
	registry             *prometheus.Registry
	httpRequestsTotal    *prometheus.CounterVec
	httpRequestsDuration *prometheus.HistogramVec
}

func NewPrometheusMetricsService(namespace, subsystem, podName string) *PrometheusMetricsService {
	constLabels := map[string]string{"podName": podName}

	httpRequestsTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			ConstLabels: constLabels,
			Name:        "requests_total",
			Help:        "Total number of HTTP requests received.",
		},
		[]string{"method", "endpoint", "status_code"},
	)

	httpRequestDuration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			ConstLabels: constLabels,
			Name:        "request_duration_seconds",
			Help:        "Histogram of HTTP request durations.",
			Buckets:     prometheus.DefBuckets,
		},
		[]string{"method", "endpoint", "status_code"},
	)

	p := &PrometheusMetricsService{
		registry:             prometheus.NewRegistry(),
		httpRequestsTotal:    httpRequestsTotal,
		httpRequestsDuration: httpRequestDuration,
	}
	p.registerMetrics()

	return p
}

func (p *PrometheusMetricsService) registerMetrics() {
	p.registry.MustRegister(
		p.httpRequestsTotal,
		p.httpRequestsDuration,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		collectors.NewBuildInfoCollector(),
	)
}

func (p *PrometheusMetricsService) Registry() *prometheus.Registry {
	return p.registry
}

func (p *PrometheusMetricsService) IncrementRequestsTotal(method, endpoint, statusCode string) {
	p.httpRequestsTotal.WithLabelValues(method, endpoint, statusCode).Inc()
}

func (p *PrometheusMetricsService) ObserveRequestDuration(method, endpoint, statusCode string, durationSeconds float64) {
	p.httpRequestsDuration.WithLabelValues(method, endpoint, statusCode).Observe(durationSeconds)
}
