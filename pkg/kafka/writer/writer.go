package writer

import (
	"context"
	"go-backend-service-blueprint/pkg/kafka/config"
	"go-backend-service-blueprint/pkg/kafka/helper"
	"time"

	"log/slog"

	kafkaPkg "github.com/segmentio/kafka-go"
)

type Writer interface {
	WriteMessages(ctx context.Context, msgs ...kafkaPkg.Message) error
	Close() error
}

func NewWriter(con config.KafkaConfig, topic string, logger *slog.Logger) (Writer, error) {
	if err := helper.CheckTopicExists(con.Brokers, topic, logger); err != nil {
		return nil, err
	}

	return newRequestIDWriter(&kafkaPkg.Writer{
		Addr:  kafkaPkg.TCP(con.Brokers...),
		Topic: topic,

		// Balancer determines how messages are distributed across partitions.
		// Hash balancer ensures messages with the same key go to the same partition.
		Balancer: &kafkaPkg.Hash{},

		// Batching configuration for optimal throughput.
		BatchSize:    100,                   // Maximum messages per batch
		BatchBytes:   1048576,               // Maximum batch size in bytes (1MB)
		BatchTimeout: 10 * time.Millisecond, // Maximum time to wait for batch to fill

		// Reliability settings.
		RequiredAcks: kafkaPkg.RequireAll, // Wait for all ISR replicas
		MaxAttempts:  3,                   // Retry failed writes up to 3 times

		// Compression reduces network bandwidth and storage.
		Compression: kafkaPkg.Snappy,
	}), nil
}
