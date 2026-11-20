package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os/signal"
	"syscall"

	"go-backend-service-blueprint/cmd/shared"
	"go-backend-service-blueprint/config"
	"go-backend-service-blueprint/pkg/rabbitmq/storage"
)

func main() {
	replay := flag.String("replay-dead-letters", "", "queue to replay (example.direct or example.delayed)")
	queue := flag.String("queue", "example.direct", "queue for a sample message")
	message := flag.String("publish", "", "publish a sample message and exit")
	delay := flag.Duration("delay", 0, "delivery delay for the delayed queue (for example 5s)")
	flag.Parse()

	cfg, err := config.LoadRabbitMQAppConfig()
	if err != nil {
		log.Fatal(err)
	}
	if *replay != "" && *message != "" {
		log.Fatal(errors.New("choose replay or publish, not both"))
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	conn := connectionConfig(cfg)

	switch {
	case *message != "":
		err = publishExample(ctx, conn, *queue, *message, *delay)
	case *replay != "":
		var count int
		count, err = replayDeadLetters(ctx, conn, *replay)
		if err == nil {
			log.Printf("replayed %d messages from %s", count, *replay)
		}
	default:
		err = runConsumer(ctx, cfg)
	}
	if err != nil {
		log.Fatal(err)
	}
}

func runConsumer(ctx context.Context, cfg *config.RabbitMQAppConfig) (runErr error) {
	redis, err := shared.NewRedis(cfg.RedisConfig)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, redis.Close()) }()
	app, err := NewRabbitMQApp(cfg, shared.NewLogger(), storage.NewDeduplicationStorage(redis), redis)
	if err != nil {
		return err
	}
	return app.Run(ctx)
}
