package main

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"go-backend-service-blueprint/cmd/shared"
	"go-backend-service-blueprint/config"
	_ "go-backend-service-blueprint/docs"
	inframonitoring "go-backend-service-blueprint/internal/infrastructure/monitoring"
	presentationhc "go-backend-service-blueprint/internal/presentation/http/healthcheck"
	"go-backend-service-blueprint/internal/presentation/http/hello"
	presmonitoring "go-backend-service-blueprint/internal/presentation/http/monitoring"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/adaptor"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	httpSwagger "github.com/swaggo/http-swagger/v2"
)

const appName = "http_server_app"

type HTTPApp struct {
	server *fiber.App
	cfg    *config.HTTPAppConfig
	logger *slog.Logger
	ms     *inframonitoring.PrometheusMetricsService
}

func NewHTTPApp(cfg *config.HTTPAppConfig) *HTTPApp {
	app := &HTTPApp{
		server: NewFiber(cfg),
		cfg:    cfg,
		logger: shared.NewLogger(),
		ms:     NewMetricsService(cfg.Namespace, appName, cfg.PodName),
	}
	app.setupRoutes()

	return app
}

func (app *HTTPApp) setupRoutes() {
	hc := NewHealthcheck()
	healthcheckHandler := presentationhc.NewHandler(hc)

	healthcheckGroup := app.server.Group("/healthcheck")
	healthcheckGroup.Use(presmonitoring.HTTPMonitoringMiddleware(app.ms))
	healthcheckGroup.Get("/", healthcheckHandler.Check)

	helloHandler := hello.NewHandler()
	helloGroup := app.server.Group("/hello")
	helloGroup.Use(presmonitoring.HTTPMonitoringMiddleware(app.ms))
	helloGroup.Get("", helloHandler.Hello)

	promMetrics := app.server.Group("/metrics")
	promMetrics.Get("", adaptor.HTTPHandler(promhttp.HandlerFor(app.ms.Registry(), promhttp.HandlerOpts{})))

	app.server.Get("/swagger/*", adaptor.HTTPHandler(httpSwagger.WrapHandler))
}

func (app *HTTPApp) Run() error {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(quit)

	errCh := make(chan error, 1)
	go func() {
		app.logger.Info("starting server", "port", app.cfg.ServerPort)
		errCh <- app.server.Listen(fmt.Sprintf(":%s", app.cfg.ServerPort))
	}()

	select {
	case err := <-errCh:
		if err != nil {
			app.logger.Error("unable to start server", "error", err)
		}

		return err
	case <-quit:
		app.logger.Info("shutting down server")

		return app.server.Shutdown()
	}
}

func (app *HTTPApp) Close() {}
