package shared

import (
	"context"
	"log/slog"

	"go-backend-service-blueprint/pkg/ctxutils"
)

type requestIDHandler struct {
	slog.Handler
}

func (handler requestIDHandler) Handle(ctx context.Context, record slog.Record) error {
	record.AddAttrs(slog.String(ctxutils.RequestID, ctxutils.GetFromContext(ctx)))

	return handler.Handler.Handle(ctx, record)
}

func (handler requestIDHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return requestIDHandler{Handler: handler.Handler.WithAttrs(attrs)}
}

func (handler requestIDHandler) WithGroup(name string) slog.Handler {
	return requestIDHandler{Handler: handler.Handler.WithGroup(name)}
}
