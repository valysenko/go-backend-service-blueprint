package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	initTimeout = 3 * time.Second
	pingTimeout = time.Second
)

type RedisConfig struct {
	RedisURL      string
	RedisPassword string
	RedisDB       int
}

type AppRedis struct {
	Client *redis.Client
}

func InitRedis(redisConfig *RedisConfig) (*AppRedis, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     redisConfig.RedisURL,
		Password: redisConfig.RedisPassword,
		DB:       redisConfig.RedisDB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), initTimeout)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, errors.Join(fmt.Errorf("ping redis: %w", err), client.Close())
	}

	return &AppRedis{Client: client}, nil
}

func (ar *AppRedis) Close() error {
	return ar.Client.Close()
}

func (ar *AppRedis) Name() string {
	return "redis"
}

func (ar *AppRedis) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()

	return ar.Client.Ping(ctx).Err()
}
