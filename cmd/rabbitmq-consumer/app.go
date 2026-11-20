package main

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"go-backend-service-blueprint/config"
	"go-backend-service-blueprint/internal/app/healthcheck"
	"go-backend-service-blueprint/internal/infrastructure/monitoring"
	presentationhc "go-backend-service-blueprint/internal/presentation/http/healthcheck"
	"go-backend-service-blueprint/pkg/rabbitmq"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/adaptor"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type RabbitMQApp struct {
	consumers *rabbitmq.ConsumerSet
	server    *fiber.App
	port      string
}

func NewRabbitMQApp(cfg *config.RabbitMQAppConfig, logger *slog.Logger) (*RabbitMQApp, error) {
	metrics := monitoring.NewRabbitMQMetrics()
	consumers, err := newConsumers(connectionConfig(cfg), metrics, logger)
	if err != nil {
		return nil, err
	}
	checkers := make([]healthcheck.Checker, 0, len(consumers.Consumers()))
	for _, consumer := range consumers.Consumers() {
		checkers = append(checkers, consumer)
	}
	hc := presentationhc.NewHandler(healthcheck.NewHealthcheck(checkers...))
	server := fiber.New(fiber.Config{ReadTimeout: 5 * time.Second})
	server.Get("/healthcheck", hc.Check)
	server.Get("/metrics", adaptor.HTTPHandler(promhttp.HandlerFor(metrics.Registry(), promhttp.HandlerOpts{})))
	return &RabbitMQApp{
		consumers: consumers,
		server:    server,
		port:      cfg.MonitoringPort,
	}, nil
}

func (app *RabbitMQApp) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- app.server.Listen(":"+app.port, fiber.ListenConfig{DisableStartupMessage: true})
	}()
	consumerErrors := make(chan error, 1)
	go func() { consumerErrors <- app.consumers.Run(ctx) }()

	var runErr error
	select {
	case runErr = <-serverErrors:
		cancel()
		runErr = errors.Join(runErr, <-consumerErrors)
	case runErr = <-consumerErrors:
		cancel()
	case <-ctx.Done():
		cancel()
		runErr = <-consumerErrors
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	return errors.Join(runErr, app.server.ShutdownWithContext(shutdownCtx))
}
