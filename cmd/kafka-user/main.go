package main

import (
	"context"
	"errors"
	"io"
	"log"

	"go-backend-service-blueprint/cmd/shared"
	"go-backend-service-blueprint/cmd/shared/kafkaapp"
	"go-backend-service-blueprint/config"
	"go-backend-service-blueprint/internal/app/kafkaexample"
	kafkaInfra "go-backend-service-blueprint/internal/infrastructure/kafka"
	"go-backend-service-blueprint/internal/infrastructure/monitoring"
	kafkaPresentation "go-backend-service-blueprint/internal/presentation/kafka"
	kafkaconfig "go-backend-service-blueprint/pkg/kafka/config"
	"go-backend-service-blueprint/pkg/kafka/reader/handler"
	"go-backend-service-blueprint/pkg/kafka/schemaregistry"
	"go-backend-service-blueprint/pkg/kafka/storage"
	"go-backend-service-blueprint/pkg/kafka/writer"
)

const (
	userTopic   = "example_user_topic"
	userGroup   = "blueprint-user"
	userDLTopic = "example_user_topic.dl"
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
	if cfg.SchemaRegistryURL == "" {
		return errors.New("SCHEMA_REGISTRY_URL is required for the user schema")
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

	registry, err := schemaregistry.NewClient(cfg.SchemaRegistryURL)
	if err != nil {
		return err
	}
	w, err := writer.NewWriter(kafkaconfig.KafkaConfig{Brokers: cfg.Brokers}, "example_events_topic", logger)
	if err != nil {
		return err
	}
	processor := &kafkaPresentation.UserHandler{
		UseCase: kafkaexample.NewUserUseCase(logger, &kafkaInfra.Publisher{Writer: w}),
	}
	source := storage.DLSource{Topic: userTopic, ConsumerGroup: userGroup}
	consumers := []kafkaapp.TopicConsumer{
		kafkaapp.MustBaseFlowTopicConsumer(kafkaapp.BaseFlowConfig{
			Topic:         userTopic,
			ConsumerGroup: userGroup,
			Processor:     processor,
			SchemaSubject: "example_user_topic-value",
			SchemaVersion: 1,
			DLTopic:       userDLTopic,
			DLSource:      source,
			Retry:         handler.MustRetryConfig(3, 100, 2),
		}),
		kafkaapp.MustDLFlowTopicConsumer(kafkaapp.DLFlowConfig{
			Topic:         userDLTopic,
			ConsumerGroup: "blueprint-user-dl",
			Processor:     processor,
			DLSource:      source,
			SchemaSubject: "example_user_topic-value",
			SchemaVersion: 1,
			MaxRetries:    3,
		}),
	}
	app, err := kafkaapp.New(kafkaapp.Options{
		Name:       "blueprint-kafka-user",
		Logger:     logger,
		Metrics:    monitoring.NewKafkaMetrics(),
		Redis:      redis,
		Brokers:    cfg.Brokers,
		Validator:  schemaregistry.NewValidator(registry),
		Monitoring: kafkaapp.MonitoringOptions{ServerPort: cfg.MonitoringPort},
		Consumers:  consumers,
		Closers:    []io.Closer{w},
	})
	if err != nil {
		if closeErr := w.Close(); closeErr != nil {
			logger.Error("close kafka writer", "error", closeErr)
		}
		return err
	}
	return app.Run(context.Background())
}
