package ctxutils

import (
	"context"
	"regexp"

	"github.com/google/uuid"
)

const (
	RequestID           = "request_id"
	RequestIDHTTPHeader = "X-Request-Id"
)

var requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

type contextKey string

const RequestIDContext contextKey = RequestID

func WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, RequestIDContext, requestIDOrNew(requestID))
}

func NewRequestID() string {
	return uuid.NewString()
}

func GetFromContext(ctx context.Context) string {
	if ctx == nil {
		return NewRequestID()
	}

	if value, ok := ctx.Value(RequestIDContext).(string); ok {
		return requestIDOrNew(value)
	}

	return NewRequestID()
}

func requestIDOrNew(requestID string) string {
	if !requestIDPattern.MatchString(requestID) {
		return NewRequestID()
	}

	return requestID
}
