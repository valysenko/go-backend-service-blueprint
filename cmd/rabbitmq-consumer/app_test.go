package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"go-backend-service-blueprint/config"

	"github.com/stretchr/testify/require"
)

type testDeduplicationStore struct{}

func (testDeduplicationStore) IsProcessed(context.Context, string, string) (bool, error) {
	return false, nil
}
func (testDeduplicationStore) MarkProcessed(context.Context, string, string) error { return nil }

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
