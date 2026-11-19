package writer

import (
	"context"

	"go-backend-service-blueprint/pkg/ctxutils"

	kafkaPkg "github.com/segmentio/kafka-go"
)

type requestIDWriter struct {
	inner Writer
}

func newRequestIDWriter(inner Writer) Writer {
	return &requestIDWriter{inner: inner}
}

func (requestWriter *requestIDWriter) WriteMessages(ctx context.Context, msgs ...kafkaPkg.Message) error {
	requestID := ctxutils.GetFromContext(ctx)

	for messageIndex := range msgs {
		msgs[messageIndex].Headers = upsertHeader(msgs[messageIndex].Headers, ctxutils.RequestID, requestID)
	}

	return requestWriter.inner.WriteMessages(ctx, msgs...)
}

func (requestWriter *requestIDWriter) Close() error {
	return requestWriter.inner.Close()
}

func upsertHeader(headers []kafkaPkg.Header, key, value string) []kafkaPkg.Header {
	for headerIndex := range headers {
		if headers[headerIndex].Key == key {
			headers[headerIndex].Value = []byte(value)
			return headers
		}
	}

	return append(headers, kafkaPkg.Header{Key: key, Value: []byte(value)})
}
