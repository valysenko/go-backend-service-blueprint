package writer_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	"go-backend-service-blueprint/pkg/kafka/schemaregistry"
	"go-backend-service-blueprint/pkg/kafka/writer"
	"go-backend-service-blueprint/pkg/kafka/writer/mocks"

	kafkaPkg "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var testLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

// captureLogger returns a logger that writes JSON records into buf so tests
// can assert on emitted log lines.
func captureLogger(buf *bytes.Buffer) *slog.Logger {
	return captureLoggerAt(buf, slog.LevelDebug)
}

// captureLoggerAt pins the handler level, which is what separates "the payload is
// not at Info" from "the payload is nowhere".
func captureLoggerAt(buf *bytes.Buffer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: level}))
}

func recordsAtLevel(t *testing.T, raw, level string) []map[string]any {
	t.Helper()

	out := []map[string]any{}
	for _, line := range nonEmptyLines(raw) {
		rec := map[string]any{}
		require.NoError(t, json.Unmarshal([]byte(line), &rec))
		if rec["level"] == level {
			out = append(out, rec)
		}
	}

	return out
}

const (
	vwSubject = "some-topic-value"
	vwVersion = 1
)

func msgWith(key, value string) kafkaPkg.Message {
	return kafkaPkg.Message{Key: []byte(key), Value: []byte(value)}
}

func TestValidatingWriter_WriteMessages(t *testing.T) {
	ctx := context.Background()

	t.Run("all messages valid: inner called with all msgs, no error", func(t *testing.T) {
		validator := mocks.NewMockValidator(t)
		inner := mocks.NewMockWriter(t)
		w := writer.NewValidatingWriter(inner, validator, testLogger, vwSubject, vwVersion)
		m1 := msgWith("k1", `{"x":1}`)
		m2 := msgWith("k2", `{"x":2}`)

		validator.EXPECT().Validate(ctx, vwSubject, vwVersion, m1.Value).Return(nil).Once()
		validator.EXPECT().Validate(ctx, vwSubject, vwVersion, m2.Value).Return(nil).Once()
		inner.EXPECT().WriteMessages(ctx, mock.MatchedBy(func(msgs []kafkaPkg.Message) bool {
			return len(msgs) == 2 && string(msgs[0].Key) == "k1" && string(msgs[1].Key) == "k2"
		})).Return(nil).Once()

		err := w.WriteMessages(ctx, m1, m2)

		assert.NoError(t, err)
	})

	t.Run("one message invalid: stops at first failure, inner not called, error returned", func(t *testing.T) {
		validator := mocks.NewMockValidator(t)
		inner := mocks.NewMockWriter(t)
		w := writer.NewValidatingWriter(inner, validator, testLogger, vwSubject, vwVersion)
		m1 := msgWith("k1", `{"x":1}`)
		m2 := msgWith("k2", `bad`)
		validateErr := errors.New("schema mismatch")

		validator.EXPECT().Validate(ctx, vwSubject, vwVersion, m1.Value).Return(nil).Once()
		validator.EXPECT().Validate(ctx, vwSubject, vwVersion, m2.Value).Return(validateErr).Once()

		err := w.WriteMessages(ctx, m1, m2)

		inner.AssertNotCalled(t, "WriteMessages")
		assert.ErrorIs(t, err, validateErr)
	})

	t.Run("empty Value (tombstone) is skipped: validator not called for that msg", func(t *testing.T) {
		validator := mocks.NewMockValidator(t)
		inner := mocks.NewMockWriter(t)
		w := writer.NewValidatingWriter(inner, validator, testLogger, vwSubject, vwVersion)
		tombstone := kafkaPkg.Message{Key: []byte("k1"), Value: nil}
		m2 := msgWith("k2", `{"x":2}`)

		validator.EXPECT().Validate(ctx, vwSubject, vwVersion, m2.Value).Return(nil).Once()
		inner.EXPECT().WriteMessages(ctx, mock.Anything).Return(nil).Once()

		err := w.WriteMessages(ctx, tombstone, m2)

		assert.NoError(t, err)
	})

	t.Run("validator transient error: inner not called, error propagates", func(t *testing.T) {
		validator := mocks.NewMockValidator(t)
		inner := mocks.NewMockWriter(t)
		w := writer.NewValidatingWriter(inner, validator, testLogger, vwSubject, vwVersion)
		m := msgWith("k1", `{"x":1}`)
		registryErr := errors.New("schema registry: connection refused")

		validator.EXPECT().Validate(ctx, vwSubject, vwVersion, m.Value).Return(registryErr).Once()

		err := w.WriteMessages(ctx, m)

		inner.AssertNotCalled(t, "WriteMessages")
		assert.ErrorIs(t, err, registryErr)
	})

	t.Run("schemaVersion == 0: validation skipped, inner called", func(t *testing.T) {
		validator := mocks.NewMockValidator(t)
		inner := mocks.NewMockWriter(t)
		w := writer.NewValidatingWriter(inner, validator, testLogger, "", 0)
		m1 := msgWith("k1", `anything`)
		m2 := msgWith("k2", `even garbage`)

		inner.EXPECT().WriteMessages(ctx, mock.Anything).Return(nil).Once()

		err := w.WriteMessages(ctx, m1, m2)

		validator.AssertNotCalled(t, "Validate")
		assert.NoError(t, err)
	})

	t.Run("inner error propagates unchanged", func(t *testing.T) {
		validator := mocks.NewMockValidator(t)
		inner := mocks.NewMockWriter(t)
		w := writer.NewValidatingWriter(inner, validator, testLogger, vwSubject, vwVersion)
		m := msgWith("k1", `{"x":1}`)
		writeErr := errors.New("kafka unavailable")

		validator.EXPECT().Validate(ctx, vwSubject, vwVersion, m.Value).Return(nil).Once()
		inner.EXPECT().WriteMessages(ctx, mock.Anything).Return(writeErr).Once()

		err := w.WriteMessages(ctx, m)

		assert.ErrorIs(t, err, writeErr)
	})
}

func TestValidatingWriter_Close(t *testing.T) {
	t.Run("delegates to inner", func(t *testing.T) {
		validator := mocks.NewMockValidator(t)
		inner := mocks.NewMockWriter(t)
		w := writer.NewValidatingWriter(inner, validator, testLogger, vwSubject, vwVersion)

		inner.EXPECT().Close().Return(nil).Once()

		err := w.Close()

		assert.NoError(t, err)
	})

	t.Run("propagates inner close error", func(t *testing.T) {
		validator := mocks.NewMockValidator(t)
		inner := mocks.NewMockWriter(t)
		w := writer.NewValidatingWriter(inner, validator, testLogger, vwSubject, vwVersion)
		closeErr := errors.New("close failed")

		inner.EXPECT().Close().Return(closeErr).Once()

		err := w.Close()

		assert.ErrorIs(t, err, closeErr)
	})
}

func TestValidatingWriter_PublishLogging(t *testing.T) {
	ctx := context.Background()

	// One line per call, not per message: a 50k-row bulk run used to emit 50k Info
	// records, each carrying a customer-linkable subscription id.
	t.Run("one Info line per call, carrying the message count", func(t *testing.T) {
		var buf bytes.Buffer
		validator := mocks.NewMockValidator(t)
		inner := mocks.NewMockWriter(t)
		w := writer.NewValidatingWriter(inner, validator, captureLoggerAt(&buf, slog.LevelInfo), vwSubject, vwVersion)

		validator.EXPECT().Validate(ctx, vwSubject, vwVersion, mock.Anything).Return(nil).Times(3)
		inner.EXPECT().WriteMessages(ctx, mock.Anything).Return(nil).Once()

		err := w.WriteMessages(ctx, msgWith("k1", `{"x":1}`), msgWith("k2", `{"x":2}`), msgWith("k3", `{"x":3}`))
		assert.NoError(t, err)

		infos := recordsAtLevel(t, buf.String(), "INFO")
		require.Len(t, infos, 1, "expected exactly one Info record regardless of message count")
		assert.Equal(t, "kafka messages published", infos[0]["msg"])
		assert.Equal(t, vwSubject, infos[0]["schema_subject"])
		assert.EqualValues(t, vwVersion, infos[0]["schema_version"])
		assert.EqualValues(t, 3, infos[0]["count"])
	})

	// Asserted negatively — that is what proves the exposure is gone rather than moved.
	t.Run("no payload and no key at Info", func(t *testing.T) {
		var buf bytes.Buffer
		validator := mocks.NewMockValidator(t)
		inner := mocks.NewMockWriter(t)
		w := writer.NewValidatingWriter(inner, validator, captureLoggerAt(&buf, slog.LevelInfo), vwSubject, vwVersion)

		validator.EXPECT().Validate(ctx, vwSubject, vwVersion, mock.Anything).Return(nil).Twice()
		inner.EXPECT().WriteMessages(ctx, mock.Anything).Return(nil).Once()

		err := w.WriteMessages(ctx, msgWith("sub-1111", `{"merchant":{"subscriptionId":"sub-1111"}}`), msgWith("sub-2222", `{"x":2}`))
		assert.NoError(t, err)

		infos := recordsAtLevel(t, buf.String(), "INFO")
		// Guard the negative assertions: without this the loop would pass vacuously
		// if the Info record vanished altogether.
		require.NotEmpty(t, infos, "there must be an Info record to assert about")
		for _, rec := range infos {
			assert.NotContains(t, rec, "value")
			assert.NotContains(t, rec, "key")
		}
		assert.NotContains(t, buf.String(), "sub-1111", "no Info record may carry a subscription id")
	})

	t.Run("key and payload remain available at Debug", func(t *testing.T) {
		var buf bytes.Buffer
		validator := mocks.NewMockValidator(t)
		inner := mocks.NewMockWriter(t)
		w := writer.NewValidatingWriter(inner, validator, captureLoggerAt(&buf, slog.LevelDebug), vwSubject, vwVersion)

		validator.EXPECT().Validate(ctx, vwSubject, vwVersion, mock.Anything).Return(nil).Twice()
		inner.EXPECT().WriteMessages(ctx, mock.Anything).Return(nil).Once()

		err := w.WriteMessages(ctx, msgWith("k1", `{"x":1}`), msgWith("k2", `{"x":22}`))
		assert.NoError(t, err)

		debugs := recordsAtLevel(t, buf.String(), "DEBUG")
		require.Len(t, debugs, 2, "expected one Debug record per message")
		assert.Equal(t, "k1", debugs[0]["key"])
		assert.Equal(t, `{"x":1}`, debugs[0]["value"])
		assert.Equal(t, "k2", debugs[1]["key"])
		assert.Equal(t, `{"x":22}`, debugs[1]["value"])
	})

	t.Run("does not log when validation fails", func(t *testing.T) {
		var buf bytes.Buffer
		validator := mocks.NewMockValidator(t)
		inner := mocks.NewMockWriter(t)
		w := writer.NewValidatingWriter(inner, validator, captureLogger(&buf), vwSubject, vwVersion)
		m1 := msgWith("k1", `{"x":1}`)

		validator.EXPECT().Validate(ctx, vwSubject, vwVersion, m1.Value).Return(errors.New("schema mismatch")).Once()

		err := w.WriteMessages(ctx, m1)
		assert.Error(t, err)
		assert.Empty(t, nonEmptyLines(buf.String()), "must not log when validation fails")
	})

	t.Run("does not log when inner write fails", func(t *testing.T) {
		var buf bytes.Buffer
		validator := mocks.NewMockValidator(t)
		inner := mocks.NewMockWriter(t)
		w := writer.NewValidatingWriter(inner, validator, captureLogger(&buf), vwSubject, vwVersion)
		m1 := msgWith("k1", `{"x":1}`)

		validator.EXPECT().Validate(ctx, vwSubject, vwVersion, m1.Value).Return(nil).Once()
		inner.EXPECT().WriteMessages(ctx, mock.Anything).Return(errors.New("broker down")).Once()

		err := w.WriteMessages(ctx, m1)
		assert.Error(t, err)
		assert.Empty(t, nonEmptyLines(buf.String()), "must not log when inner write fails")
	})
}

// The validation error names which message failed, but not its key — the key is the
// subscription id. The wrapped sentinel must survive, because the publisher branches
// on errors.Is to decide whether to isolate schema-invalid rows.
func TestValidatingWriter_ValidationErrorOmitsKey(t *testing.T) {
	ctx := context.Background()

	var buf bytes.Buffer
	validator := mocks.NewMockValidator(t)
	inner := mocks.NewMockWriter(t)
	w := writer.NewValidatingWriter(inner, validator, captureLogger(&buf), vwSubject, vwVersion)
	bad := msgWith("sub-1111", `{"x":1}`)

	validator.EXPECT().
		Validate(ctx, vwSubject, vwVersion, bad.Value).
		Return(fmt.Errorf("%w: schema %s v%d", schemaregistry.ErrPayloadInvalid, vwSubject, vwVersion)).
		Once()

	err := w.WriteMessages(ctx, bad)

	require.Error(t, err)
	assert.ErrorIs(t, err, schemaregistry.ErrPayloadInvalid, "the publisher's isolation branch depends on this")
	assert.Contains(t, err.Error(), "msg[0]", "an operator needs to know which message failed")
	assert.NotContains(t, err.Error(), "sub-1111", "the key must not reach the error text")
	assert.NotContains(t, err.Error(), `{"x":1}`, "the payload must not reach the error text")
}

// Regression pin, not a change: every message is validated before anything is written.
// The bulk rails depend on that ordering to rule out duplicate commands — a batch-level
// ErrPayloadInvalid must mean zero messages reached the broker, never some of them.
func TestValidatingWriter_ValidatesEveryMessageBeforeWriting(t *testing.T) {
	ctx := context.Background()

	var buf bytes.Buffer
	validator := mocks.NewMockValidator(t)
	// No expectations on inner: any call fails the test.
	inner := mocks.NewMockWriter(t)
	w := writer.NewValidatingWriter(inner, validator, captureLogger(&buf), vwSubject, vwVersion)
	ok1, ok2 := msgWith("k1", `{"x":1}`), msgWith("k2", `{"x":2}`)
	bad := msgWith("k3", `{"x":3}`)

	validator.EXPECT().Validate(ctx, vwSubject, vwVersion, ok1.Value).Return(nil).Once()
	validator.EXPECT().Validate(ctx, vwSubject, vwVersion, ok2.Value).Return(nil).Once()
	validator.EXPECT().
		Validate(ctx, vwSubject, vwVersion, bad.Value).
		Return(fmt.Errorf("%w: bad", schemaregistry.ErrPayloadInvalid)).
		Once()

	err := w.WriteMessages(ctx, ok1, ok2, bad)

	assert.ErrorIs(t, err, schemaregistry.ErrPayloadInvalid)
	assert.Empty(t, nonEmptyLines(buf.String()), "nothing published, so nothing logged")
}

func nonEmptyLines(s string) []string {
	out := []string{}
	for _, line := range strings.Split(s, "\n") {
		if line != "" {
			out = append(out, line)
		}
	}
	return out
}
