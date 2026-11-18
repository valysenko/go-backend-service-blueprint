package shared

import (
	"fmt"
	"log/slog"
	"os"

	"go-backend-service-blueprint/config"
	"go-backend-service-blueprint/pkg/database"
	"go-backend-service-blueprint/pkg/redis"
)

func NewLogger() *slog.Logger {
	jsonHandler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(requestIDHandler{Handler: jsonHandler})
	slog.SetDefault(logger)

	return logger
}

func NewPostgres(cfg config.PostgreSQLConfig) (*database.AppDB, error) {
	db, err := database.InitDB(&database.DBConfig{
		Host:           cfg.Host,
		Port:           cfg.Port,
		Username:       cfg.Username,
		Password:       cfg.Password,
		DBName:         cfg.DBName,
		SSLMode:        cfg.SSLMode,
		MaxConns:       cfg.MaxConns,
		MinConns:       cfg.MinConns,
		ConnectTimeout: cfg.ConnectTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("initialize database: %w", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()

		return nil, fmt.Errorf("ping database: %w", err)
	}

	if err := db.RunMigrations(cfg.MigrationsDir); err != nil {
		db.Close()

		return nil, err
	}

	return db, nil
}

func NewRedis(cfg config.RedisConfig) (*redis.AppRedis, error) {
	appRedis, err := redis.InitRedis(&redis.RedisConfig{
		RedisURL:      cfg.RedisURL,
		RedisPassword: cfg.RedisPassword,
		RedisDB:       cfg.RedisDB,
	})
	if err != nil {
		return nil, fmt.Errorf("initialize redis: %w", err)
	}

	return appRedis, nil
}
