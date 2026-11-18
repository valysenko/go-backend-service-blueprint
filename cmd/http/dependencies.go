package main

import (
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"go-backend-service-blueprint/config"
	"go-backend-service-blueprint/internal/app/healthcheck"
	inframonitoring "go-backend-service-blueprint/internal/infrastructure/monitoring"
	"go-backend-service-blueprint/internal/presentation/http/middleware"
	"go-backend-service-blueprint/pkg/ctxutils"

	"github.com/go-playground/validator/v10"
	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/logger"
)

const (
	maxLoggedBodyBytes   = 2048
	maxLoggedSecretChars = 22
)

var secretHeaders = map[string]struct{}{
	fiber.HeaderAuthorization:      {},
	fiber.HeaderProxyAuthorization: {},
	fiber.HeaderCookie:             {},
	"X-Api-Key":                    {},
}

type structValidator struct {
	validate *validator.Validate
}

func (v *structValidator) Validate(out any) error {
	return v.validate.Struct(out)
}

func NewFiber(cfg *config.HTTPAppConfig) *fiber.App {
	server := fiber.New(fiber.Config{
		ReadTimeout:     time.Duration(cfg.ReadTimeout) * time.Second,
		WriteTimeout:    time.Duration(cfg.WriteTimeout) * time.Second,
		BodyLimit:       cfg.BodyLimit,
		StructValidator: &structValidator{validate: validator.New()},
	})

	server.Use(middleware.RequestID())
	if cfg.AppEnv != config.EnvTest {
		server.Use(logger.New(RequestLoggerConfig()))
	}

	return server
}

func RequestLoggerConfig() logger.Config {
	return logger.Config{
		Stream:        os.Stderr,
		DisableColors: true,
		Format: "${status} - ${method} ${url} ${latency} " +
			"request_id=${respHeader:" + ctxutils.RequestIDHTTPHeader + "} " +
			"headers=${reqHeaders} body=${body} ${error}\n",
		Skip: func(c fiber.Ctx) bool {
			path := c.Path()
			return strings.HasPrefix(path, "/healthcheck") || strings.HasPrefix(path, "/metrics")
		},
		CustomTags: map[string]logger.LogFunc{
			logger.TagBody:       logBody,
			logger.TagReqHeaders: logReqHeaders,
		},
	}
}

func logReqHeaders(output logger.Buffer, c fiber.Ctx, _ *logger.Data, _ string) (int, error) {
	headers := make(map[string][]string)
	if err := c.Bind().Header(&headers); err != nil {
		return 0, err
	}

	rendered := make([]string, 0, len(headers))
	for name, values := range headers {
		value := strings.Join(values, ",")
		if _, secret := secretHeaders[http.CanonicalHeaderKey(name)]; secret {
			value = maskSecret(value)
		}
		rendered = append(rendered, name+"="+value)
	}

	return output.WriteString(strings.Join(rendered, "&"))
}

// maskSecret keeps the auth scheme and at most half of the secret, so a short
// API key does not end up readable in the log either.
func maskSecret(value string) string {
	scheme, secret, hasScheme := strings.Cut(value, " ")
	if !hasScheme {
		scheme, secret = "", value
	}

	secret = secret[:min(len(secret)/2, maxLoggedSecretChars)] + "..."
	if scheme == "" {
		return secret
	}

	return scheme + " " + secret
}

func logBody(output logger.Buffer, c fiber.Ctx, _ *logger.Data, _ string) (int, error) {
	body := c.Body()
	if len(body) == 0 {
		return 0, nil
	}

	contentType := c.Get(fiber.HeaderContentType)
	if !strings.HasPrefix(contentType, fiber.MIMEApplicationJSON) {
		return output.WriteString(contentType + " body, " + strconv.Itoa(len(body)) + " bytes")
	}

	if len(body) > maxLoggedBodyBytes {
		return output.WriteString(strconv.Quote(string(body[:maxLoggedBodyBytes]) + "...(truncated)"))
	}

	return output.WriteString(strconv.Quote(string(body)))
}

func NewMetricsService(namespace, subsystem, podName string) *inframonitoring.PrometheusMetricsService {
	return inframonitoring.NewPrometheusMetricsService(namespace, subsystem, podName)
}

func NewHealthcheck(checkers ...healthcheck.Checker) *healthcheck.Healthcheck {
	return healthcheck.NewHealthcheck(checkers...)
}
