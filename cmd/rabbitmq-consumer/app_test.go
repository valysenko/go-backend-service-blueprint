package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go-backend-service-blueprint/config"

	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/require"
)

type testDeduplicationStore struct{}

func (testDeduplicationStore) IsProcessed(context.Context, string, string) (bool, error) {
	return false, nil
}
func (testDeduplicationStore) MarkProcessed(context.Context, string, string) error { return nil }

type blockedConsumers struct{ released chan struct{} }

func (c blockedConsumers) Run(context.Context) error {
	<-c.released
	return nil
}

func TestRunStopsWhenConsumersIgnoreTheDeadline(t *testing.T) {
	consumers := blockedConsumers{released: make(chan struct{})}
	t.Cleanup(func() { close(consumers.released) })
	app := &RabbitMQApp{
		consumers: consumers,
		server:    fiber.New(fiber.Config{ReadTimeout: time.Second}),
		port:      "0",
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	started := time.Now()
	go func() { done <- app.Run(ctx) }()

	select {
	case err := <-done:
		require.ErrorContains(t, err, "did not stop within")
		require.Less(t, time.Since(started), shutdownTimeout+5*time.Second)
	case <-time.After(shutdownTimeout + 10*time.Second):
		t.Fatal("Run waited for the consumers forever")
	}
}

func TestMonitoringRoutes(t *testing.T) {
	app, err := NewRabbitMQApp(&config.RabbitMQAppConfig{}, slog.New(slog.NewTextHandler(io.Discard, nil)), testDeduplicationStore{})
	require.NoError(t, err)

	response, err := app.server.Test(httptest.NewRequest(http.MethodGet, "/healthcheck", nil))
	require.NoError(t, err)
	defer func() { _ = response.Body.Close() }()
	require.Equal(t, http.StatusInternalServerError, response.StatusCode)

	response, err = app.server.Test(httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.NoError(t, err)
	defer func() { _ = response.Body.Close() }()
	require.Equal(t, http.StatusOK, response.StatusCode)
}
