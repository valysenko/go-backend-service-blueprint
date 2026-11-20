package storage_test

import (
	"context"
	"testing"
	"time"

	"go-backend-service-blueprint/internal/testutils"
	"go-backend-service-blueprint/pkg/rabbitmq/storage"
	appredis "go-backend-service-blueprint/pkg/redis"

	"github.com/stretchr/testify/require"
)

func TestRabbitDeduplicationWithRedis(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	addr, stop, err := testutils.RunRedisContainer(ctx)
	require.NoError(t, err)
	t.Cleanup(stop)
	redis, err := appredis.InitRedis(&appredis.RedisConfig{RedisURL: addr})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, redis.Close()) })
	store := storage.NewDeduplicationStorage(redis)
	processed, err := store.IsProcessed(ctx, "queue", "message")
	require.NoError(t, err)
	require.False(t, processed)
	require.NoError(t, store.MarkProcessed(ctx, "queue", "message"))
	processed, err = store.IsProcessed(ctx, "queue", "message")
	require.NoError(t, err)
	require.True(t, processed)
	processed, err = store.IsProcessed(ctx, "other-queue", "message")
	require.NoError(t, err)
	require.False(t, processed)
}
