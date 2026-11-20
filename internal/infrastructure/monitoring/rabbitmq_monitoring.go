package monitoring

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

type RabbitMQMetrics struct {
	registry *prometheus.Registry
	messages *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

func NewRabbitMQMetrics() *RabbitMQMetrics {
	m := &RabbitMQMetrics{
		registry: prometheus.NewRegistry(),
		messages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "rabbitmq_consumer_messages_total",
			Help: "RabbitMQ consumer outcomes.",
		}, []string{"queue", "outcome"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "rabbitmq_consumer_duration_seconds",
			Help: "RabbitMQ message handling duration.",
		}, []string{"queue", "outcome"}),
	}
	m.registry.MustRegister(m.messages, m.duration, collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}

func (m *RabbitMQMetrics) Registry() *prometheus.Registry { return m.registry }
func (m *RabbitMQMetrics) ObserveMessage(queue, outcome string, duration time.Duration) {
	m.messages.WithLabelValues(queue, outcome).Inc()
	if duration > 0 {
		m.duration.WithLabelValues(queue, outcome).Observe(duration.Seconds())
	}
}
