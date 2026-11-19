package storage_test

import (
	"context"
	"errors"
	"go-backend-service-blueprint/pkg/kafka/reader"
	"go-backend-service-blueprint/pkg/kafka/storage"
	appredis "go-backend-service-blueprint/pkg/redis"

	"testing"

	"github.com/go-redis/redismock/v9"
	kafkaPkg "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

const (
	sourceTopic         = "source-topic"
	sourceConsumerGroup = "source-group"
	partitionKey        = "test-pk"
)

func newTestMessageWithPK() *reader.Message {
	return reader.NewMessage(kafkaPkg.Message{
		Topic: testTopic,
		Key:   []byte(partitionKey),
	})
}

func expectedKey() string {
	return "dl_counter_" + sourceTopic + "_" + sourceConsumerGroup + "_" + partitionKey
}

func TestGetDlCounter(t *testing.T) {
	ctx := context.Background()

	t.Run("returns value", func(t *testing.T) {
		client, mock := redismock.NewClientMock()
		s := storage.NewDlStorage(&appredis.AppRedis{Client: client}, storage.DLSource{Topic: sourceTopic, ConsumerGroup: sourceConsumerGroup})

		mock.ExpectGet(expectedKey()).SetVal("12")

		val, err := s.GetDlCounter(ctx, newTestMessageWithPK())

		assert.NoError(t, err)
		assert.Equal(t, 12, val)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("value does not exist", func(t *testing.T) {
		client, mock := redismock.NewClientMock()
		s := storage.NewDlStorage(&appredis.AppRedis{Client: client}, storage.DLSource{Topic: sourceTopic, ConsumerGroup: sourceConsumerGroup})

		mock.ExpectGet(expectedKey()).RedisNil()

		val, err := s.GetDlCounter(ctx, newTestMessageWithPK())

		assert.NoError(t, err)
		assert.Equal(t, 0, val)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns error on redis failure", func(t *testing.T) {
		client, mock := redismock.NewClientMock()
		s := storage.NewDlStorage(&appredis.AppRedis{Client: client}, storage.DLSource{Topic: sourceTopic, ConsumerGroup: sourceConsumerGroup})
		expectedErr := errors.New("redis down")
		mock.ExpectGet(expectedKey()).SetErr(expectedErr)

		val, err := s.GetDlCounter(ctx, newTestMessageWithPK())

		assert.ErrorIs(t, err, expectedErr)
		assert.Zero(t, val)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

}

func TestIncrementCounter(t *testing.T) {
	ctx := context.Background()

	t.Run("increments counter", func(t *testing.T) {
		client, mock := redismock.NewClientMock()
		s := storage.NewDlStorage(&appredis.AppRedis{Client: client}, storage.DLSource{Topic: sourceTopic, ConsumerGroup: sourceConsumerGroup})

		mock.ExpectIncr(expectedKey()).SetVal(1)

		err := s.IncrementCounter(ctx, newTestMessageWithPK())

		assert.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns error on redis failure", func(t *testing.T) {
		client, mock := redismock.NewClientMock()
		s := storage.NewDlStorage(&appredis.AppRedis{Client: client}, storage.DLSource{Topic: sourceTopic, ConsumerGroup: sourceConsumerGroup})
		expectedErr := errors.New("redis down")
		mock.ExpectIncr(expectedKey()).SetErr(expectedErr)

		err := s.IncrementCounter(ctx, newTestMessageWithPK())

		assert.ErrorIs(t, err, expectedErr)
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestDecrementCounter(t *testing.T) {
	ctx := context.Background()

	const decrementScript = `
local v = tonumber(redis.call('GET', KEYS[1]) or '0')
if v > 0 then return redis.call('DECR', KEYS[1]) end
return v
`

	t.Run("decrements counter", func(t *testing.T) {
		client, mock := redismock.NewClientMock()
		s := storage.NewDlStorage(&appredis.AppRedis{Client: client}, storage.DLSource{Topic: sourceTopic, ConsumerGroup: sourceConsumerGroup})

		mock.ExpectEval(decrementScript, []string{expectedKey()}).SetVal(int64(0))

		err := s.DecrementCounter(ctx, newTestMessageWithPK())

		assert.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns error on redis failure", func(t *testing.T) {
		client, mock := redismock.NewClientMock()
		s := storage.NewDlStorage(&appredis.AppRedis{Client: client}, storage.DLSource{Topic: sourceTopic, ConsumerGroup: sourceConsumerGroup})
		expectedErr := errors.New("redis down")
		mock.ExpectEval(decrementScript, []string{expectedKey()}).SetErr(expectedErr)

		err := s.DecrementCounter(ctx, newTestMessageWithPK())

		assert.ErrorIs(t, err, expectedErr)
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}
