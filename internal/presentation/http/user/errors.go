package user

import (
	"errors"
	"log/slog"
	"math"
	"strconv"

	appuser "go-backend-service-blueprint/internal/app/user"
	domainuser "go-backend-service-blueprint/internal/domain/user"
	"go-backend-service-blueprint/internal/presentation"

	"github.com/gofiber/fiber/v3"
)

func writeDomainError(c fiber.Ctx, err error) error {
	var rateLimited *appuser.RateLimitedError
	if errors.As(err, &rateLimited) {
		c.Set(fiber.HeaderRetryAfter, strconv.Itoa(int(math.Ceil(rateLimited.RetryAfter.Seconds()))))

		return c.Status(fiber.StatusTooManyRequests).JSON(presentation.ErrorResponse{Error: "too many requests"})
	}

	switch {
	case errors.Is(err, domainuser.ErrInvalid):
		return c.Status(fiber.StatusBadRequest).JSON(presentation.ErrorResponse{Error: err.Error()})
	case errors.Is(err, domainuser.ErrNotFound):
		return c.Status(fiber.StatusNotFound).JSON(presentation.ErrorResponse{Error: "user not found"})
	case errors.Is(err, domainuser.ErrAlreadyExists):
		return c.Status(fiber.StatusConflict).JSON(presentation.ErrorResponse{Error: "user already exists"})
	default:
		slog.ErrorContext(c.Context(), "unhandled domain error", "error", err)

		return c.Status(fiber.StatusInternalServerError).JSON(presentation.ErrorResponse{Error: "internal server error"})
	}
}
