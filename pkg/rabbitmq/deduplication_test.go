package rabbitmq

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type testDeduplicationStore struct {
	queue     string
	id        string
	processed bool
}

func (s *testDeduplicationStore) IsProcessed(_ context.Context, queue, id string) (bool, error) {
	s.queue, s.id = queue, id
	return s.processed, nil
}

func (s *testDeduplicationStore) MarkProcessed(_ context.Context, queue, id string) error {
	s.queue, s.id, s.processed = queue, id, true
	return nil
}

func TestDeduplicatingHandler(t *testing.T) {
	store := &testDeduplicationStore{}
	calls := 0
	failure := errors.New("processing failed")
	handler, err := NewDeduplicatingHandler("queue", testHandler(func(_ context.Context, d Delivery) error {
		calls++
		require.Equal(t, "message-1", d.MessageID)
		if calls == 1 {
			return failure
		}
		return nil
	}), store)
	require.NoError(t, err)
	var permanent *PermanentError
	require.ErrorAs(t, handler.Handle(context.Background(), Delivery{}), &permanent)
	require.Equal(t, 0, calls)
	delivery := Delivery{MessageID: "message-1"}
	require.ErrorIs(t, handler.Handle(context.Background(), delivery), failure)
	require.False(t, store.processed)
	require.NoError(t, handler.Handle(context.Background(), delivery))
	require.True(t, store.processed)
	require.NoError(t, handler.Handle(context.Background(), delivery))
	require.Equal(t, "queue", store.queue)
	require.Equal(t, "message-1", store.id)
	require.Equal(t, 2, calls)
}
