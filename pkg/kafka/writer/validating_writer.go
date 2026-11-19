package writer

import (
	"context"
	"fmt"
	"log/slog"

	kafkaPkg "github.com/segmentio/kafka-go"
)

type Validator interface {
	Validate(ctx context.Context, subject string, version int, payload []byte) error
}

type ValidatingWriter struct {
	inner         Writer
	validator     Validator
	logger        *slog.Logger
	schemaSubject string
	schemaVersion int
}

func NewValidatingWriter(inner Writer, validator Validator, logger *slog.Logger, schemaSubject string, schemaVersion int) *ValidatingWriter {
	return &ValidatingWriter{
		inner:         inner,
		validator:     validator,
		logger:        logger,
		schemaSubject: schemaSubject,
		schemaVersion: schemaVersion,
	}
}

func (w *ValidatingWriter) WriteMessages(ctx context.Context, msgs ...kafkaPkg.Message) error {
	if w.validator == nil {
		return fmt.Errorf("validator is required when schemaVersion != 0")
	}
	if w.schemaVersion != 0 {
		for i, m := range msgs {
			if len(m.Value) == 0 {
				continue // tombstone / log-compaction marker — schema not applicable
			}
			if err := w.validator.Validate(ctx, w.schemaSubject, w.schemaVersion, m.Value); err != nil {
				// Index, not key: the key is a customer-linkable id and callers log this.
				return fmt.Errorf("validate msg[%d]: %w", i, err)
			}
		}
	}

	if err := w.inner.WriteMessages(ctx, msgs...); err != nil {
		return err
	}

	// One line per call, not per message: a key or payload can carry a customer-linkable
	// id. Note the Debug block below is NOT currently a protection — the shipped logger
	// is built at Debug with no override, so it runs in production. A configurable level
	// is what would close that.
	w.logger.InfoContext(ctx, "kafka messages published",
		"schema_subject", w.schemaSubject,
		"schema_version", w.schemaVersion,
		"count", len(msgs),
	)

	if w.logger.Enabled(ctx, slog.LevelDebug) {
		for _, m := range msgs {
			w.logger.DebugContext(ctx, "kafka message published",
				"schema_subject", w.schemaSubject,
				"schema_version", w.schemaVersion,
				"key", string(m.Key),
				"value", string(m.Value),
			)
		}
	}

	return nil
}

func (w *ValidatingWriter) Close() error { return w.inner.Close() }
