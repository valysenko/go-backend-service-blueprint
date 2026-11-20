package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"go-backend-service-blueprint/config"
	"go-backend-service-blueprint/internal/app/healthcheck"
	"go-backend-service-blueprint/internal/infrastructure/monitoring"
	rabbitinfra "go-backend-service-blueprint/internal/infrastructure/rabbitmq"
	presentationhc "go-backend-service-blueprint/internal/presentation/http/healthcheck"
	"go-backend-service-blueprint/pkg/rabbitmq"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/adaptor"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// shutdownTimeout bounds the whole stop sequence so it stays inside the grace
// period a container runtime allows before it sends SIGKILL.
const shutdownTimeout = 5 * time.Second

// consumerRunner is the part of rabbitmq.ConsumerSet that Run needs, so the
// shutdown deadline can be exercised with a set that refuses to stop.
type consumerRunner interface {
	Run(ctx context.Context) error
}

type RabbitMQApp struct {
	consumers consumerRunner
	server    *fiber.App
	port      string
	closers   []io.Closer
}

func NewRabbitMQApp(cfg *config.RabbitMQAppConfig, logger *slog.Logger, store rabbitmq.DeduplicationStore, extraCheckers ...healthcheck.Checker) (*RabbitMQApp, error) {
	metrics := monitoring.NewRabbitMQMetrics()
	publisher := rabbitmq.NewPublisher(connectionConfig(cfg), outputQueue)
	consumers, err := newConsumers(connectionConfig(cfg), metrics, logger, &rabbitinfra.ExamplePublisher{Publisher: publisher}, store)
	if err != nil {
		return nil, err
	}
	checkers := make([]healthcheck.Checker, 0, len(consumers.Consumers()))
	for _, consumer := range consumers.Consumers() {
		checkers = append(checkers, consumer)
	}
	checkers = append(checkers, extraCheckers...)
	hc := presentationhc.NewHandler(healthcheck.NewHealthcheck(checkers...))
	server := fiber.New(fiber.Config{ReadTimeout: 5 * time.Second})
	server.Get("/healthcheck", hc.Check)
	server.Get("/metrics", adaptor.HTTPHandler(promhttp.HandlerFor(metrics.Registry(), promhttp.HandlerOpts{})))
	return &RabbitMQApp{
		consumers: consumers,
		server:    server,
		port:      cfg.MonitoringPort,
		closers:   []io.Closer{publisher},
	}, nil
}

func (app *RabbitMQApp) Run(ctx context.Context) (runErr error) {
	defer func() {
		for i := len(app.closers) - 1; i >= 0; i-- {
			runErr = errors.Join(runErr, app.closers[i].Close())
		}
	}()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- app.server.Listen(":"+app.port, fiber.ListenConfig{DisableStartupMessage: true})
	}()
	consumerErrors := make(chan error, 1)
	go func() { consumerErrors <- app.consumers.Run(ctx) }()

	var errs []error
	consumersStopped := false
	select {
	case err := <-serverErrors:
		errs = append(errs, err)
	case err := <-consumerErrors:
		errs, consumersStopped = append(errs, err), true
	case <-ctx.Done():
	}
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()

	serverStopped := make(chan error, 1)
	go func() { serverStopped <- app.server.ShutdownWithContext(shutdownCtx) }()

	// Abandoning a consumer costs a redelivery, not a message: the broker
	// requeues everything this process never acknowledged once it exits.
	// Waiting for one without a deadline turns a slow shutdown into a SIGKILL.
	if !consumersStopped {
		select {
		case err := <-consumerErrors:
			errs = append(errs, err)
		case <-shutdownCtx.Done():
			errs = append(errs, fmt.Errorf("rabbitmq consumers did not stop within %s", shutdownTimeout))
		}
	}

	return errors.Join(append(errs, <-serverStopped)...)
}
