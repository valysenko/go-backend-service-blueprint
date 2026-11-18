package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"go-backend-service-blueprint/config"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	app, err := NewCronApp(config.InitCronAppConfig())
	if err != nil {
		log.Fatalf("failed to initialize cron app: %v", err)
	}
	defer app.Close()

	if err := app.Run(ctx); err != nil {
		log.Fatalf("operations cron exited with error: %v", err)
	}
}
