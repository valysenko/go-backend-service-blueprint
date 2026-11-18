package main

import (
	"log"

	"go-backend-service-blueprint/cmd/shared"
	"go-backend-service-blueprint/config"
)

// @title           Go Backend Service Blueprint API
// @version         1.0
// @description     Reference HTTP API of the service blueprint.
// @BasePath        /
func main() {
	cfg := config.InitHTTPAppConfig()

	db, err := shared.NewPostgres(cfg.PostgreSQLConfig)
	if err != nil {
		log.Fatalf("failed to initialize database: %v", err)
	}

	appRedis, err := shared.NewRedis(cfg.RedisConfig)
	if err != nil {
		log.Fatalf("failed to initialize redis: %v", err)
	}

	app := NewHTTPApp(cfg, Dependencies{DB: db, Redis: appRedis})
	defer app.Close()

	if err := app.Run(); err != nil {
		log.Fatalf("http app exited with error: %v", err)
	}
}
