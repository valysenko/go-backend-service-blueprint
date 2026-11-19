package monitoring

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

type KafkaMetrics struct {
	registry *prometheus.Registry
	events   *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

func NewKafkaMetrics() *KafkaMetrics {
	m := &KafkaMetrics{
		registry: prometheus.NewRegistry(),
		events: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_consumer_events_total",
			Help: "Kafka consumer processing outcomes.",
		}, []string{"topic", "status"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "kafka_consumer_event_duration_seconds",
			Help: "Kafka consumer processing duration.",
		}, []string{"topic"}),
	}
	m.registry.MustRegister(
		m.events,
		m.duration,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		collectors.NewBuildInfoCollector(),
	)
	return m
}
func (m *KafkaMetrics) Registry() *prometheus.Registry { return m.registry }
func (m *KafkaMetrics) IncrementEventsTotal(topic, status string) {
	m.events.WithLabelValues(topic, status).Inc()
}
func (m *KafkaMetrics) ObserveEventProcessingDuration(topic string, duration float64) {
	m.duration.WithLabelValues(topic).Observe(duration)
}
