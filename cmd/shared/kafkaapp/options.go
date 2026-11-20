package kafkaapp

import (
	"errors"
	"io"
	"log/slog"
	"time"

	"go-backend-service-blueprint/internal/app/healthcheck"
	"go-backend-service-blueprint/pkg/kafka/reader/handler"
	"go-backend-service-blueprint/pkg/redis"

	"github.com/prometheus/client_golang/prometheus"
)

type Options struct {
	// Name identifies the app
	Name string

	Logger  *slog.Logger
	Metrics MetricsService

	// Redis backs the deduplication and dead-letter counters of every handler
	// chain, so it is required even for an app that owns no tables.
	Redis *redis.AppRedis

	Brokers []string

	// Validator is nil when no schema registry is configured, which is legal
	// only while every entry uses SchemaVersion 0 — ValidationHandler enforces
	// that at message time. Declared as the handler.Validator interface and
	// never as *schemaregistry.Validator: a nil concrete pointer stored in an
	// interface field reads as non-nil and would silently defeat that guard.
	Validator handler.Validator

	Monitoring MonitoringOptions

	// Consumers are the [topic + consumer group] pairs this app subscribes to
	Consumers []TopicConsumer

	// App-level closers. Which might be shared between readers (f.e some Publisher in kafka consumer)
	Closers []io.Closer

	// Checkers are the /healthcheck targets
	Checkers []healthcheck.Checker
}

type MonitoringOptions struct {
	ServerPort   string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

func (o Options) validate() error {
	switch {
	case o.Name == "":
		return errors.New("kafkaapp: Name must not be empty")
	case o.Logger == nil:
		return errors.New("kafkaapp: Logger must not be nil")
	case o.Metrics == nil:
		return errors.New("kafkaapp: Metrics must not be nil")
	case o.Redis == nil:
		return errors.New("kafkaapp: Redis must not be nil")
	case len(o.Brokers) == 0:
		return errors.New("kafkaapp: Brokers must not be empty")
	case len(o.Consumers) == 0:
		return errors.New("kafkaapp: Consumers must not be empty")
	case o.Monitoring.ServerPort == "":
		return errors.New("kafkaapp: Monitoring.ServerPort must not be empty")
	}

	return validateConsumerSet(o.Consumers)
}

// MetricsService is the shell's view of a metrics service: the per-message
// recorder the handler chain needs, plus the registry /metrics serves.
// *monitoring.KafkaPrometheusMetricsService satisfies it.
type MetricsService interface {
	handler.MetricsRecorder
	Registry() *prometheus.Registry
}
