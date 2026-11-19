package storage_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"go-backend-service-blueprint/pkg/kafka/reader"
	"go-backend-service-blueprint/pkg/kafka/storage"
	appredis "go-backend-service-blueprint/pkg/redis"

	"github.com/go-redis/redismock/v9"
	goredis "github.com/redis/go-redis/v9"
	kafkaPkg "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

const (
	testConsumerGroup = "test-cg"
	testTopic         = "test-topic"
	testID            = "msg-123"
)

func newTestMessage(headers ...kafkaPkg.Header) *reader.Message {
	return reader.NewMessage(kafkaPkg.Message{
		Topic:   testTopic,
		Headers: headers,
	})
}

func idHeader(value string) kafkaPkg.Header {
	return kafkaPkg.Header{Key: "id", Value: []byte(value)}
}

const (
	testKey  = "01a0f0b5-bfd7-7475-ae30-bf954620707d"
	testBody = `{"userUuid":"01a0f0b5-bfd7-7475-ae30-bf954620707d","eventType":"UPDATED"}`
)

func newKeyedTestMessage(headers ...kafkaPkg.Header) *reader.Message {
	return reader.NewMessage(kafkaPkg.Message{
		Topic:   testTopic,
		Key:     []byte(testKey),
		Value:   []byte(testBody),
		Headers: headers,
	})
}

func keyBodyHash(key, body string) string {
	hash := sha256.New()
	hash.Write([]byte(key))
	hash.Write([]byte{0})
	hash.Write([]byte(body))
	return hex.EncodeToString(hash.Sum(nil))
}

func expectedHashDedupKey() string {
	return "dedup_key_" + testTopic + "_" + testConsumerGroup + "_content_" + keyBodyHash(testKey, testBody)
}

func expectedDedupKey() string {
	return "dedup_key_" + testTopic + "_" + testConsumerGroup + "_" + testID
}

func TestDeduplicationStorage_IsAcked(t *testing.T) {
	ctx := context.Background()

	t.Run("returns true when key exists", func(t *testing.T) {
		client, mock := redismock.NewClientMock()
		s := storage.NewDeduplicationStorage(&appredis.AppRedis{Client: client}, testConsumerGroup)
		mock.ExpectExists(expectedDedupKey()).SetVal(1)

		acked, err := s.IsAcked(ctx, newTestMessage(idHeader(testID)))

		assert.NoError(t, err)
		assert.True(t, acked)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns false when key does not exist", func(t *testing.T) {
		client, mock := redismock.NewClientMock()
		s := storage.NewDeduplicationStorage(&appredis.AppRedis{Client: client}, testConsumerGroup)
		mock.ExpectExists(expectedDedupKey()).SetVal(0)

		acked, err := s.IsAcked(ctx, newTestMessage(idHeader(testID)))

		assert.NoError(t, err)
		assert.False(t, acked)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns error on redis failure", func(t *testing.T) {
		client, mock := redismock.NewClientMock()
		s := storage.NewDeduplicationStorage(&appredis.AppRedis{Client: client}, testConsumerGroup)
		expectedErr := errors.New("redis down")
		mock.ExpectExists(expectedDedupKey()).SetErr(expectedErr)

		acked, err := s.IsAcked(ctx, newTestMessage(idHeader(testID)))

		assert.ErrorIs(t, err, expectedErr)
		assert.False(t, acked)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("falls back to key+body hash when id header is missing", func(t *testing.T) {
		client, mock := redismock.NewClientMock()
		s := storage.NewDeduplicationStorage(&appredis.AppRedis{Client: client}, testConsumerGroup)
		mock.ExpectExists(expectedHashDedupKey()).SetVal(1)

		acked, err := s.IsAcked(ctx, newKeyedTestMessage())

		assert.NoError(t, err)
		assert.True(t, acked)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("falls back to key+body hash when id header is empty", func(t *testing.T) {
		client, mock := redismock.NewClientMock()
		s := storage.NewDeduplicationStorage(&appredis.AppRedis{Client: client}, testConsumerGroup)
		mock.ExpectExists(expectedHashDedupKey()).SetVal(0)

		acked, err := s.IsAcked(ctx, newKeyedTestMessage(idHeader("")))

		assert.NoError(t, err)
		assert.False(t, acked)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("hash separates key from body", func(t *testing.T) {
		client, mock := redismock.NewClientMock()
		s := storage.NewDeduplicationStorage(&appredis.AppRedis{Client: client}, testConsumerGroup)
		shiftedKey, shiftedBody := testKey+testBody[:1], testBody[1:]
		shiftedDedupKey := "dedup_key_" + testTopic + "_" + testConsumerGroup + "_content_" + keyBodyHash(shiftedKey, shiftedBody)
		mock.ExpectExists(shiftedDedupKey).SetVal(0)

		_, err := s.IsAcked(ctx, reader.NewMessage(kafkaPkg.Message{
			Topic: testTopic,
			Key:   []byte(shiftedKey),
			Value: []byte(shiftedBody),
		}))

		assert.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
		assert.NotEqual(t, expectedHashDedupKey(), shiftedDedupKey)
	})
}

func TestDeduplicationStorage_Ack(t *testing.T) {
	ctx := context.Background()

	t.Run("sets key with NX mode and 24h TTL", func(t *testing.T) {
		client, mock := redismock.NewClientMock()
		s := storage.NewDeduplicationStorage(&appredis.AppRedis{Client: client}, testConsumerGroup)
		mock.ExpectSetArgs(expectedDedupKey(), true, goredis.SetArgs{
			Mode: "NX",
			TTL:  24 * time.Hour,
		}).SetVal("OK")

		err := s.Ack(ctx, newTestMessage(idHeader(testID)))

		assert.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns nil when key already exists (redis.Nil)", func(t *testing.T) {
		client, mock := redismock.NewClientMock()
		s := storage.NewDeduplicationStorage(&appredis.AppRedis{Client: client}, testConsumerGroup)
		mock.ExpectSetArgs(expectedDedupKey(), true, goredis.SetArgs{
			Mode: "NX",
			TTL:  24 * time.Hour,
		}).SetErr(goredis.Nil)

		err := s.Ack(ctx, newTestMessage(idHeader(testID)))

		assert.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns wrapped error on redis failure", func(t *testing.T) {
		client, mock := redismock.NewClientMock()
		s := storage.NewDeduplicationStorage(&appredis.AppRedis{Client: client}, testConsumerGroup)
		expectedErr := errors.New("redis down")
		mock.ExpectSetArgs(expectedDedupKey(), true, goredis.SetArgs{
			Mode: "NX",
			TTL:  24 * time.Hour,
		}).SetErr(expectedErr)

		err := s.Ack(ctx, newTestMessage(idHeader(testID)))

		assert.ErrorIs(t, err, expectedErr)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns error when id header is missing", func(t *testing.T) {
		client, _ := redismock.NewClientMock()
		s := storage.NewDeduplicationStorage(&appredis.AppRedis{Client: client}, testConsumerGroup)

		err := s.Ack(ctx, newTestMessage())

		assert.Error(t, err)
	})
}
