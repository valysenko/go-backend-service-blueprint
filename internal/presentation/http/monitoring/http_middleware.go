package monitoring

import (
	"strconv"
	"time"

	appmon "go-backend-service-blueprint/internal/app/monitoring"

	"github.com/gofiber/fiber/v3"
)

func HTTPMonitoringMiddleware(ms appmon.HTTPMetricsRecorder) fiber.Handler {
	return func(c fiber.Ctx) error {
		start := time.Now()
		err := c.Next()

		duration := time.Since(start).Seconds()
		statusCode := strconv.Itoa(c.Response().StatusCode())

		ms.ObserveRequestDuration(c.Method(), c.Route().Path, statusCode, duration)
		ms.IncrementRequestsTotal(c.Method(), c.Route().Path, statusCode)

		return err
	}
}
