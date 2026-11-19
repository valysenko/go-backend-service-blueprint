package main

import (
	"context"
	"log"

	"go-backend-service-blueprint/cmd/shared"
	"go-backend-service-blueprint/cmd/shared/kafkaapp"
	"go-backend-service-blueprint/config"
	"go-backend-service-blueprint/internal/infrastructure/monitoring"
	kafkaPresentation "go-backend-service-blueprint/internal/presentation/kafka"
	"go-backend-service-blueprint/pkg/kafka/reader/handler"
	"go-backend-service-blueprint/pkg/kafka/storage"
)

const (
	eventsTopic   = "example_events_topic"
	eventsGroup   = "blueprint-events"
	eventsDLTopic = "example_events_topic.dl"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	cfg, err := config.LoadKafkaAppConfig()
	if err != nil {
		return err
	}
	logger := shared.NewLogger()
	redis, err := shared.NewRedis(cfg.RedisConfig)
	if err != nil {
		return err
	}
	defer func() {
		if err := redis.Close(); err != nil {
			logger.Error("close redis", "error", err)
		}
	}()

	processor := &kafkaPresentation.EventHandler{Logger: logger}
	source := storage.DLSource{Topic: eventsTopic, ConsumerGroup: eventsGroup}
	consumers := []kafkaapp.TopicConsumer{
		kafkaapp.MustBaseFlowTopicConsumer(kafkaapp.BaseFlowConfig{
			Topic:         eventsTopic,
			ConsumerGroup: eventsGroup,
			Processor:     processor,
			DLTopic:       eventsDLTopic,
			DLSource:      source,
			Retry:         handler.MustRetryConfig(3, 100, 2),
		}),
		kafkaapp.MustDLFlowTopicConsumer(kafkaapp.DLFlowConfig{
			Topic:         eventsDLTopic,
			ConsumerGroup: "blueprint-events-dl",
			Processor:     processor,
			DLSource:      source,
			MaxRetries:    3,
		}),
	}
	app, err := kafkaapp.New(kafkaapp.Options{
		Name:       "blueprint-kafka-events",
		Logger:     logger,
		Metrics:    monitoring.NewKafkaMetrics(),
		Redis:      redis,
		Brokers:    cfg.Brokers,
		Monitoring: kafkaapp.MonitoringOptions{ServerPort: cfg.MonitoringPort},
		Consumers:  consumers,
	})
	if err != nil {
		return err
	}
	return app.Run(context.Background())
}
