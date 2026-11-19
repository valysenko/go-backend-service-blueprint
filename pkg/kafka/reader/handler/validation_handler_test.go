package handler_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"go-backend-service-blueprint/pkg/kafka/reader"
	"go-backend-service-blueprint/pkg/kafka/reader/handler"
	handlerMocks "go-backend-service-blueprint/pkg/kafka/reader/handler/mocks"
	"go-backend-service-blueprint/pkg/kafka/reader/mocks"
	"go-backend-service-blueprint/pkg/kafka/schemaregistry"

	"github.com/stretchr/testify/assert"
)

const (
	validationSubject = "test-topic-value"
	validationVersion = 1
)

func TestValidationHandle(t *testing.T) {
	ctx := context.Background()

	t.Run("schemaVersion 0: validation skipped, inner called", func(t *testing.T) {
		validator := handlerMocks.NewMockValidator(t)
		inner := mocks.NewMockMessageHandler(t)
		h := handler.NewValidationHandler(validator, inner, "schema-subject", 0)
		msg := newFallbackTestMessage()

		inner.EXPECT().Handle(ctx, msg).Return(nil).Once()
		err := h.Handle(ctx, msg)
		validator.AssertNotCalled(t, "Validate")
		assert.NoError(t, err)
	})

	t.Run("payload valid: inner called, no error", func(t *testing.T) {
		validator := handlerMocks.NewMockValidator(t)
		inner := mocks.NewMockMessageHandler(t)
		h := handler.NewValidationHandler(validator, inner, validationSubject, validationVersion)
		msg := newFallbackTestMessage()

		validator.EXPECT().Validate(ctx, validationSubject, validationVersion, msg.Body()).Return(nil).Once()
		inner.EXPECT().Handle(ctx, msg).Return(nil).Once()

		err := h.Handle(ctx, msg)
		assert.NoError(t, err)
	})

	t.Run("schema invalid: returns UnrecoverableError, inner not called", func(t *testing.T) {
		validator := handlerMocks.NewMockValidator(t)
		inner := mocks.NewMockMessageHandler(t)
		h := handler.NewValidationHandler(validator, inner, validationSubject, validationVersion)
		msg := newFallbackTestMessage()
		validateErr := fmt.Errorf("%w: schema mismatch", schemaregistry.ErrPayloadInvalid)

		validator.EXPECT().Validate(ctx, validationSubject, validationVersion, msg.Body()).Return(validateErr).Once()

		err := h.Handle(ctx, msg)

		var unrecErr *reader.UnrecoverableError
		assert.ErrorAs(t, err, &unrecErr)
		assert.ErrorIs(t, err, schemaregistry.ErrPayloadInvalid)
		inner.AssertNotCalled(t, "Handle")
	})

	t.Run("transient validator error: returns plain error, inner not called", func(t *testing.T) {
		validator := handlerMocks.NewMockValidator(t)
		inner := mocks.NewMockMessageHandler(t)
		h := handler.NewValidationHandler(validator, inner, validationSubject, validationVersion)
		msg := newFallbackTestMessage()
		registryErr := errors.New("schema registry: connection refused")

		validator.EXPECT().Validate(ctx, validationSubject, validationVersion, msg.Body()).Return(registryErr).Once()

		err := h.Handle(ctx, msg)
		assert.ErrorIs(t, err, registryErr)
		var unrecErr *reader.UnrecoverableError
		assert.False(t, errors.As(err, &unrecErr))
		inner.AssertNotCalled(t, "Handle")
	})

	t.Run("inner error propagates unchanged", func(t *testing.T) {
		validator := handlerMocks.NewMockValidator(t)
		inner := mocks.NewMockMessageHandler(t)
		h := handler.NewValidationHandler(validator, inner, validationSubject, validationVersion)
		msg := newFallbackTestMessage()
		innerErr := errors.New("processing failed")

		validator.EXPECT().Validate(ctx, validationSubject, validationVersion, msg.Body()).Return(nil).Once()
		inner.EXPECT().Handle(ctx, msg).Return(innerErr).Once()

		err := h.Handle(ctx, msg)
		assert.ErrorIs(t, err, innerErr)
	})
}
