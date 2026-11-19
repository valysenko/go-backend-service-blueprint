package kafkaapp

import (
	"go-backend-service-blueprint/internal/app/healthcheck"
	presentationhc "go-backend-service-blueprint/internal/presentation/http/healthcheck"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/adaptor"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func (a *App) setupMonitoringServer() {
	a.server = newFiber(a.opts.Monitoring)

	hcHandler := presentationhc.NewHandler(healthcheck.NewHealthcheck(a.checkers()...))
	a.server.Get("/healthcheck", hcHandler.Check)

	promMetrics := a.server.Group("/metrics")
	promMetrics.Get("", adaptor.HTTPHandler(promhttp.HandlerFor(a.opts.Metrics.Registry(), promhttp.HandlerOpts{})))
}

// checkers falls back to Redis when the binary named no targets of its own:
// it is the one dependency every consumer has, since the dedup and dead-letter
// counters live there. See Options.Checkers for why the set is deliberate and
// not simply "everything we connected to".
func (a *App) checkers() []healthcheck.Checker {
	if len(a.opts.Checkers) > 0 {
		return a.opts.Checkers
	}
	return []healthcheck.Checker{a.opts.Redis}
}

func newFiber(cfg MonitoringOptions) *fiber.App {
	return fiber.New(fiber.Config{
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	})
}
