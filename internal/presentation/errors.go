package presentation

import (
	"errors"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/gofiber/fiber/v3"
)

type ErrorResponse struct {
	Error string `json:"error"`
}

func BadRequest(c fiber.Ctx, msg string) error {
	return c.Status(fiber.StatusBadRequest).JSON(ErrorResponse{Error: msg})
}

// BindErrorMessage renders a binding error into a client-facing message. Fiber
// joins its own binding failures with the validator's, so both shapes have to
// be unwrapped.
func BindErrorMessage(err error) string {
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		msgs := make([]string, 0, len(joined.Unwrap()))
		for _, member := range joined.Unwrap() {
			msgs = append(msgs, validationMessages(member)...)
		}
		if len(msgs) > 0 {
			return strings.Join(msgs, "; ")
		}

		return "invalid request body"
	}

	if msgs := validationMessages(err); len(msgs) > 0 {
		return strings.Join(msgs, "; ")
	}

	return "invalid request body"
}

func validationMessages(err error) []string {
	var validationErrs validator.ValidationErrors
	if !errors.As(err, &validationErrs) {
		return nil
	}

	msgs := make([]string, 0, len(validationErrs))
	for _, fieldErr := range validationErrs {
		msgs = append(msgs, fieldErr.Field()+" fails the "+fieldErr.Tag()+" rule")
	}

	return msgs
}
