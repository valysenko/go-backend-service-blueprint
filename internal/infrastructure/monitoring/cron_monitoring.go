package monitoring

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

// CronPrometheusMetricsService pushes instead of exposing: a cron binary exits
// before Prometheus could scrape it, so the run's numbers go to a Pushgateway.
type CronPrometheusMetricsService struct {
	registry         *prometheus.Registry
	pusher           *push.Pusher
	runDuration      *prometheus.GaugeVec
	processedTotal   *prometheus.CounterVec
	pushgatewayIsSet bool
}

func NewCronPrometheusMetricsService(namespace, subsystem, podName, pushgatewayURL string) *CronPrometheusMetricsService {
	constLabels := map[string]string{"podName": podName}
	registry := prometheus.NewRegistry()

	runDuration := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			ConstLabels: constLabels,
			Name:        "run_duration_seconds",
			Help:        "Duration of the last cron run.",
		},
		[]string{"run_id"},
	)

	processedTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			ConstLabels: constLabels,
			Name:        "operations_processed_total",
			Help:        "Operations settled by the cron, by type and outcome.",
		},
		[]string{"operation_type", "outcome"},
	)

	registry.MustRegister(runDuration, processedTotal)

	service := &CronPrometheusMetricsService{
		registry:       registry,
		runDuration:    runDuration,
		processedTotal: processedTotal,
	}

	if pushgatewayURL != "" {
		service.pusher = push.New(pushgatewayURL, subsystem).Gatherer(registry).Grouping("pod", podName)
		service.pushgatewayIsSet = true
	}

	return service
}

func (s *CronPrometheusMetricsService) ObserveRunDuration(duration time.Duration, runID string) {
	s.runDuration.WithLabelValues(runID).Set(duration.Seconds())
}

func (s *CronPrometheusMetricsService) IncrementProcessed(operationType, outcome string) {
	s.processedTotal.WithLabelValues(operationType, outcome).Inc()
}

func (s *CronPrometheusMetricsService) Push() error {
	if !s.pushgatewayIsSet {
		return nil
	}

	if err := s.pusher.Push(); err != nil {
		return fmt.Errorf("push cron metrics: %w", err)
	}

	return nil
}
