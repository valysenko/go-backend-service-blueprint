package handler

import (
	"context"
	"errors"
	"fmt"
	"go-backend-service-blueprint/pkg/kafka/reader"
	"log/slog"
	"math"
	"time"
)

type RetryConfig struct {
	totalAttempts      int
	backoffBaseMs      int
	backoffCoefficient int
}

func NewRetryConfig(totalAttempts, backoffBaseMs, backoffCoefficient int) (RetryConfig, error) {
	if totalAttempts < 1 {
		return RetryConfig{}, fmt.Errorf("totalAttempts must be >= 1, got %d", totalAttempts)
	}
	if backoffBaseMs < 1 {
		return RetryConfig{}, fmt.Errorf("backoffBaseMs must be >= 1, got %d", backoffBaseMs)
	}
	if backoffCoefficient < 1 {
		return RetryConfig{}, fmt.Errorf("backoffCoefficient must be >= 1, got %d", backoffCoefficient)
	}
	return RetryConfig{
		totalAttempts:      totalAttempts,
		backoffBaseMs:      backoffBaseMs,
		backoffCoefficient: backoffCoefficient,
	}, nil
}

func MustRetryConfig(totalAttempts, backoffBaseMs, backoffCoefficient int) RetryConfig {
	cfg, err := NewRetryConfig(totalAttempts, backoffBaseMs, backoffCoefficient)
	if err != nil {
		panic(err)
	}
	return cfg
}

// RetryMessageHandler retries its inner handler with exponential backoff up to
// totalAttempts. Used in BaseFlow between DLFallback and Validation handlers.
type RetryMessageHandler struct {
	innerHandler reader.MessageHandler
	logger       *slog.Logger
	config       RetryConfig
}

func NewRetryMessageHandler(innerHandler reader.MessageHandler, config RetryConfig, logger *slog.Logger) *RetryMessageHandler {
	return &RetryMessageHandler{
		innerHandler: innerHandler,
		config:       config,
		logger:       logger,
	}
}

func (h *RetryMessageHandler) Handle(ctx context.Context, msg *reader.Message) error {
	attempt := 0
	var err error

	for attempt < h.config.totalAttempts {
		err = h.innerHandler.Handle(ctx, msg)
		if err == nil {
			if attempt > 0 {
				h.logger.InfoContext(ctx, "retry succeeded", "attempts", attempt+1)
			}
			return nil
		}

		var unrecErr *reader.UnrecoverableError
		if errors.As(err, &unrecErr) {
			h.logger.WarnContext(ctx, "retry: skipping on unrecoverable error", "error", err)
			return err
		}
		var discardErr *reader.DiscardError
		if errors.As(err, &discardErr) {
			h.logger.WarnContext(ctx, "retry: skipping on discard error", "error", err)
			return err
		}
		var rf *reader.ReaderFailure
		if errors.As(err, &rf) {
			h.logger.WarnContext(ctx, "retry: skipping on reader failure", "error", err)
			return err
		}

		attempt++
		h.logger.WarnContext(ctx, "attempt failed", "attempt", attempt, "error", err)
		if attempt >= h.config.totalAttempts {
			break
		}
		select {
		case <-ctx.Done(): // if context is cancelled, return immediately
			return ctx.Err()
		case <-time.After(time.Duration(h.calculateSleepMs(attempt)) * time.Millisecond):
		}
	}

	return err
}

// Deterministic backoff (no jitter). Many consumers failing in lockstep
// will retry simultaneously and can re-spike a recovering downstream;
// if that becomes a problem, wrap with rand.IntN(base+1) for full jitter.
func (h *RetryMessageHandler) calculateSleepMs(attempt int) int {
	return h.config.backoffBaseMs * int(math.Pow(float64(h.config.backoffCoefficient), float64(attempt)))
}
