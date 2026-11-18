package middleware

import (
	"go-backend-service-blueprint/pkg/ctxutils"

	"github.com/gofiber/fiber/v3"
)

func RequestID() fiber.Handler {
	return func(fiberCtx fiber.Ctx) error {
		ctx := ctxutils.WithRequestID(fiberCtx.Context(), fiberCtx.Get(ctxutils.RequestIDHTTPHeader))

		fiberCtx.SetContext(ctx)
		fiberCtx.Set(ctxutils.RequestIDHTTPHeader, ctxutils.GetFromContext(ctx))

		return fiberCtx.Next()
	}
}
