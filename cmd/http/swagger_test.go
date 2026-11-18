package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	_ "go-backend-service-blueprint/docs"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/adaptor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	httpSwagger "github.com/swaggo/http-swagger/v2"
)

func TestSwaggerSpecServed(t *testing.T) {
	app := fiber.New()
	app.Get("/swagger/*", adaptor.HTTPHandler(httpSwagger.WrapHandler))

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "/swagger/doc.json", nil)
	require.NoError(t, err)

	resp, err := app.Test(req)
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.True(t, json.Valid(body), "doc.json must be valid JSON")

	var spec struct {
		Info struct {
			Title string `json:"title"`
		} `json:"info"`
		Paths map[string]any `json:"paths"`
	}
	require.NoError(t, json.Unmarshal(body, &spec))

	assert.Equal(t, "Go Backend Service Blueprint API", spec.Info.Title)
	assert.Contains(t, spec.Paths, "/hello")
	assert.Contains(t, spec.Paths, "/healthcheck/")
}
