package handler

import (
	"context"
	"errors"
	"go-backend-service-blueprint/pkg/kafka/reader"
	"go-backend-service-blueprint/pkg/kafka/schemaregistry"
)

type Validator interface {
	Validate(ctx context.Context, subject string, version int, payload []byte) error
}

// Schema registry message payload validation
type ValidationHandler struct {
	inner         reader.MessageHandler
	v             Validator
	schemaSubject string
	schemaVersion int
}

func NewValidationHandler(v Validator, inner reader.MessageHandler, schemaSubject string, schemaVersion int) *ValidationHandler {
	return &ValidationHandler{
		inner:         inner,
		v:             v,
		schemaSubject: schemaSubject,
		schemaVersion: schemaVersion,
	}
}

func (h *ValidationHandler) Handle(ctx context.Context, msg *reader.Message) error {
	if h.schemaVersion > 0 {
		if h.v == nil {
			return reader.NewReaderFailure(errors.New("schema validator is not configured"), "schema validation")
		}
		if err := h.v.Validate(ctx, h.schemaSubject, h.schemaVersion, msg.Body()); err != nil {
			// Classify the validator error:
			//   - ErrPayloadInvalid: payload will never validate → mark unrecoverable
			//     so DLFallback routes it to DL.
			//   - everything else (registry blip, ctx cancellation, schema parse/compile):
			//     transient → propagate as-is so Retry / upstream handlers can react.
			if errors.Is(err, schemaregistry.ErrPayloadInvalid) {
				return reader.NewUnrecoverableError(err, "schema validation")
			}
			return err
		}
	}

	return h.inner.Handle(ctx, msg)
}
