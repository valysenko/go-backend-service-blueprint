package kafkaapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"go-backend-service-blueprint/pkg/kafka/reader"

	"github.com/gofiber/fiber/v3"
	"golang.org/x/sync/errgroup"
)

// RESOURCE OWNERSHIP & CLEANUP
// Rule: a resource is closed by the component whose shutdown hook matches its
// lifetime — not necessarily by whichever code allocated it.
//
//   - main:     db and redis. They outlive the App (the healthcheck reads them,
//     processors hold them) and main is where they are created, so
//     main defers their Close.
//   - App:      Options.Closers and the monitoring server. Closers are things
//     whose lifetime spans every reader — a processor's kafka writer,
//     an AMQP connection. Closed once errgroup.Wait returns.
//   - Reader:   kafka client + injected closers bound to that one reader (e.g.
//     the BaseFlow DL writer, built in newHandler, used by DLFallback).
//     Closed by reader.Close, which reader.Run defers.
//   - Handlers: stateless from a lifecycle perspective. They use injected
//     resources (DL writer, dedup/DL storages) but never own their close.
//
// Shutdown order (graceful):
//  1. ctx canceled → reader.Run loops exit
//  2. each reader closes its kafka client + DL writer
//  3. errgroup.Wait returns
//  4. App closes Options.Closers (LIFO), then the monitoring server
//  5. main's defers close redis, then db
//
// CONCURRENCY MODEL (in-pod, not a separate scaling axis)
//   - One reader == one [topic + consumer group] pair, each running in its own
//     goroutine inside the pod. Pairs are listed in Options.Consumers.
//   - No concurrency inside a single consumer goroutine — messages are
//     processed sequentially to preserve Kafka's per-partition ordering.
//
// SCALING — two independent dimensions
// 1) Domain separation via consumer groups
//   - The app can subscribe to multiple [topic + consumer group] pairs.
//   - Messages of one topic may carry attributes (e.g. event type, merchant) that route
//     to different domain processors. Splitting domains into separate
//     consumer groups on the same topic isolates them — each group only
//     processes the subset it cares about, others are skipped.
//   - Example: topic some_command with events {recreate, promote} → groups
//     some_command-recreate-group and some_command-promote-group.
//
// 2) Partition scaling via K8s pod replicas
//   - Within a consumer group, the broker distributes partitions across
//     active pod instances. More pods → more partitions read in parallel,
//     up to the partition count.
type App struct {
	opts    Options
	logger  *slog.Logger
	server  *fiber.App
	readers []*reader.KafkaTopicReader
}

func New(opts Options) (*App, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}

	app := &App{opts: opts, logger: opts.Logger}
	app.setupMonitoringServer()

	readers, err := app.newReaders()
	if err != nil {
		return nil, err
	}
	app.readers = readers

	return app, nil
}

// newReaders builds every reader up front. A reader holds a kafka client and
// (on BaseFlow) a DL writer from the moment it is constructed, so a failure
// partway through the set must close the ones already built — nothing will ever
// Run them, and reader.Run's defer is what would otherwise have released them.
func (a *App) newReaders() ([]*reader.KafkaTopicReader, error) {
	readers := make([]*reader.KafkaTopicReader, 0, len(a.opts.Consumers))

	for _, c := range a.opts.Consumers {
		r, err := a.newTopicReader(c)
		if err != nil {
			for _, built := range readers {
				if cerr := built.Close(); cerr != nil {
					a.logger.Error("close reader after construction failure", "error", cerr)
				}
			}
			return nil, fmt.Errorf("kafkaapp: create reader for topic %q: %w", c.Topic, err)
		}
		readers = append(readers, r)
	}

	return readers, nil
}

func (a *App) Run(ctx context.Context) error {
	a.logger.Info("starting Kafka consumer application", "app", a.opts.Name)

	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()
	defer a.close()

	go a.serveMonitoring(ctx, cancel)

	// If any reader returns an error, restart the pod.
	// The handler chain absorbs business errors (Retry/DL).
	// Reader errors are limited to Kafka client failures (broker unreachable,
	// coordinator issues) or infra failures escaping the chain (Redis/DL writer
	// blip). On pod restart readers resume from the last consumer group offset.
	//
	// Run each [topic + consumer group] reader in its own goroutine. NO
	// concurrency inside a reader, to preserve the ordering guarantee.
	g, gctx := errgroup.WithContext(ctx)
	for _, r := range a.readers {
		g.Go(func() error { return r.Run(gctx) })
	}

	if err := g.Wait(); err != nil {
		if ctx.Err() != nil && errors.Is(err, context.Canceled) {
			return nil
		}
		a.logger.Error("error running Kafka topic readers", "error", err)
		return fmt.Errorf("kafka topic readers failed: %w", err)
	}

	return nil
}

// serveMonitoring serves /healthcheck and /metrics
func (a *App) serveMonitoring(ctx context.Context, cancel context.CancelFunc) {
	addr := fmt.Sprintf(":%s", a.opts.Monitoring.ServerPort)
	a.logger.Info("monitoring server listening", "addr", addr)

	err := a.server.Listen(addr, fiber.ListenConfig{DisableStartupMessage: true})
	if err == nil || ctx.Err() != nil {
		return // nil, or Shutdown raced us during a shutdown already under way
	}

	a.logger.Error("monitoring server failed, shutting down", "error", err)
	cancel()
}

// close releases what the App owns: the binary's app-scoped resources first
// (LIFO, mirroring construction order), then the monitoring server, so the pod
// stops reporting healthy only once there is nothing left to serve.
func (a *App) close() {
	for i := len(a.opts.Closers) - 1; i >= 0; i-- {
		if err := a.opts.Closers[i].Close(); err != nil {
			a.logger.Error("error closing app resource", "error", err)
		}
	}

	if err := a.server.Shutdown(); err != nil {
		a.logger.Error("monitoring server shutdown failed", "error", err)
		return
	}
	a.logger.Info("monitoring server shutdown complete")
}
