package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go-backend-service-blueprint/config"
	"go-backend-service-blueprint/internal/infrastructure/monitoring"
	rabbitinfra "go-backend-service-blueprint/internal/infrastructure/rabbitmq"
	presentation "go-backend-service-blueprint/internal/presentation/rabbitmq"
	"go-backend-service-blueprint/pkg/rabbitmq"
)

var queueSpecs = []rabbitmq.QueueSpec{
	{Name: "example.direct", Kind: rabbitmq.KindDirect},
	{Name: "example.delayed", Kind: rabbitmq.KindDelayed},
}

func connectionConfig(cfg *config.RabbitMQAppConfig) rabbitmq.Config {
	return rabbitmq.Config{
		Host:           cfg.Host,
		Port:           cfg.Port,
		User:           cfg.User,
		Password:       cfg.Password,
		VHost:          cfg.VHost,
		ConnectionName: "blueprint-rabbitmq",
	}
}

func newConsumers(cfg rabbitmq.Config, metrics *monitoring.RabbitMQMetrics, logger *slog.Logger) (*rabbitmq.ConsumerSet, error) {
	configs := make([]rabbitmq.ConsumerConfig, 0, len(queueSpecs))
	for _, spec := range queueSpecs {
		configs = append(configs, rabbitmq.ConsumerConfig{
			Spec:    spec,
			Workers: 2,
			Retry: rabbitmq.RetryPolicy{
				MaxAttempts:    3,
				InitialBackoff: time.Second,
				MaxBackoff:     5 * time.Second,
			},
			Handler: &presentation.ExampleHandler{Logger: logger, Queue: spec.Name},
		})
	}
	return rabbitmq.NewConsumerSet(cfg, configs, metrics, logger)
}

func findQueue(name string) (rabbitmq.QueueSpec, error) {
	for _, spec := range queueSpecs {
		if spec.Name == name {
			return spec, nil
		}
	}
	return rabbitmq.QueueSpec{}, fmt.Errorf("unknown queue %q", name)
}

func publishExample(ctx context.Context, conn rabbitmq.Config, queue, message string, delay time.Duration) error {
	spec, err := findQueue(queue)
	if err != nil {
		return err
	}
	publisher := rabbitmq.NewPublisher(conn, spec)
	defer func() { _ = publisher.Close() }()
	example := rabbitinfra.ExamplePublisher{Publisher: publisher}
	return example.Publish(ctx, "", presentation.ExampleMessage{Text: message}, delay)
}

func replayDeadLetters(ctx context.Context, conn rabbitmq.Config, queue string) (int, error) {
	spec, err := findQueue(queue)
	if err != nil {
		return 0, err
	}
	return rabbitmq.ReplayDeadLetters(ctx, conn, spec, 0)
}
