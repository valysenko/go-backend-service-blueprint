package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"go-backend-service-blueprint/config"
	"go-backend-service-blueprint/internal/app/healthcheck"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestApp() *HTTPApp {
	return NewHTTPApp(&config.HTTPAppConfig{
		AppEnv:    config.EnvTest,
		PodName:   "test",
		Namespace: "blueprint",
		ServerConfig: config.ServerConfig{
			ServerPort:   "8096",
			ReadTimeout:  5,
			WriteTimeout: 5,
			BodyLimit:    1024,
		},
	})
}

func doRequest(t *testing.T, app *HTTPApp, path string) (*http.Response, []byte) {
	t.Helper()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
	require.NoError(t, err)

	resp, err := app.server.Test(req)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp, body
}

func TestHelloEndpoint(t *testing.T) {
	resp, body := doRequest(t, newTestApp(), "/hello")

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.JSONEq(t, `{"message":"hello world"}`, string(body))
}

func TestHealthcheckEndpointWithoutCheckers(t *testing.T) {
	resp, body := doRequest(t, newTestApp(), "/healthcheck/")

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result healthcheck.HealthcheckResult
	require.NoError(t, json.Unmarshal(body, &result))
	assert.Empty(t, result.HealthcheckResults)
}

func TestRequestIDHeaderIsEchoed(t *testing.T) {
	app := newTestApp()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "/hello", nil)
	require.NoError(t, err)
	req.Header.Set("X-Request-Id", "test-request-id")

	resp, err := app.server.Test(req)
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()

	assert.Equal(t, "test-request-id", resp.Header.Get("X-Request-Id"))
}

func TestMetricsEndpointExposesRequestCounter(t *testing.T) {
	app := newTestApp()
	doRequest(t, app, "/hello")

	resp, body := doRequest(t, app, "/metrics")

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), "blueprint_http_server_app_requests_total")
	assert.Contains(t, string(body), "go_goroutines")
}
