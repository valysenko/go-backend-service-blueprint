package handler

import (
	"go-backend-service-blueprint/pkg/kafka/reader"
	"go-backend-service-blueprint/pkg/kafka/writer"
	"log/slog"
)

// Handler chain ordering (outermost → innermost):
//
//	BaseFlow:  Dedup → DLFallback → Retry → Validation → Single → Processor
//	DLFlow:    Dedup → DLTopic    → Validation → Single → Processor
//
// DLFallback / DLTopic wrap Validation so validation errors are routed to DL
// instead of escaping to the reader.
// Consumer Validation is skipped when schemaVersion == 0.

// NewBaseFlowHandler builds the BaseFlow chain for primary topics.
//
// Failed messages traverse Retry (exponential backoff). On exhaustion or
// UnrecoverableError, DLFallback writes the message to dlTopic.
func NewBaseFlowHandler(
	processor MessageProcessor,
	retryConfig RetryConfig,
	dedupStorage DeduplicationStorage,
	dlStorage DlIncrementStorage,
	writer writer.Writer,
	logger *slog.Logger,
	metrics MetricsRecorder,
	dlTopic string,
	validator Validator,
	schemaSubject string,
	schemaVersion int,
) reader.MessageHandler {
	single := NewSingleMessageHandler(processor, metrics)
	validation := NewValidationHandler(validator, single, schemaSubject, schemaVersion)
	retry := NewRetryMessageHandler(validation, retryConfig, logger)
	fallback := NewDLFallbackMessageHandler(retry, dlStorage, writer, logger, dlTopic)
	return NewDeduplicationMessageHandler(dedupStorage, fallback)
}

// NewDLFlowHandler builds the DLFlow chain for dead-letter topics.
//
// Failed messages are retried in-place up to maxAttempts (0 = forever).
// On exhaustion or DiscardError, the DL counter is decremented so the
// matching BaseFlow consumer can resume processing the original key.
func NewDLFlowHandler(
	processor MessageProcessor,
	dedupStorage DeduplicationStorage,
	dlStorage DlDecrementStorage,
	logger *slog.Logger,
	metrics MetricsRecorder,
	maxAttempts int,
	validator Validator,
	schemaSubject string,
	schemaVersion int,
) reader.MessageHandler {
	single := NewSingleMessageHandler(processor, metrics)
	validation := NewValidationHandler(validator, single, schemaSubject, schemaVersion)
	dlTopic := NewDLTopicMessageHandler(validation, dlStorage, logger, maxAttempts)
	return NewDeduplicationMessageHandler(dedupStorage, dlTopic)
}
