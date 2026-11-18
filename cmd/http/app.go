package main

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go-backend-service-blueprint/cmd/shared"
	"go-backend-service-blueprint/config"
	_ "go-backend-service-blueprint/docs"
	"go-backend-service-blueprint/internal/app/healthcheck"
	appuser "go-backend-service-blueprint/internal/app/user"
	inframonitoring "go-backend-service-blueprint/internal/infrastructure/monitoring"
	"go-backend-service-blueprint/internal/infrastructure/postgres"
	infraredis "go-backend-service-blueprint/internal/infrastructure/redis"
	presentationhc "go-backend-service-blueprint/internal/presentation/http/healthcheck"
	"go-backend-service-blueprint/internal/presentation/http/hello"
	presmonitoring "go-backend-service-blueprint/internal/presentation/http/monitoring"
	presentationuser "go-backend-service-blueprint/internal/presentation/http/user"
	"go-backend-service-blueprint/pkg/database"
	"go-backend-service-blueprint/pkg/redis"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/adaptor"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	httpSwagger "github.com/swaggo/http-swagger/v2"
)

const appName = "http_server_app"

type Dependencies struct {
	DB    *database.AppDB
	Redis *redis.AppRedis
}

type HTTPApp struct {
	server *fiber.App
	cfg    *config.HTTPAppConfig
	logger *slog.Logger
	ms     *inframonitoring.PrometheusMetricsService
	db     *database.AppDB
	redis  *redis.AppRedis
}

func NewHTTPApp(cfg *config.HTTPAppConfig, deps Dependencies) *HTTPApp {
	app := &HTTPApp{
		server: NewFiber(cfg),
		cfg:    cfg,
		logger: shared.NewLogger(),
		ms:     NewMetricsService(cfg.Namespace, appName, cfg.PodName),
		db:     deps.DB,
		redis:  deps.Redis,
	}
	app.setupRoutes()

	return app
}

func (app *HTTPApp) setupRoutes() {
	healthcheckHandler := presentationhc.NewHandler(NewHealthcheck(app.checkers()...))

	healthcheckGroup := app.server.Group("/healthcheck")
	healthcheckGroup.Use(presmonitoring.HTTPMonitoringMiddleware(app.ms))
	healthcheckGroup.Get("/", healthcheckHandler.Check)

	helloHandler := hello.NewHandler()
	helloGroup := app.server.Group("/hello")
	helloGroup.Use(presmonitoring.HTTPMonitoringMiddleware(app.ms))
	helloGroup.Get("", helloHandler.Hello)

	if app.db != nil {
		userHandler := presentationuser.NewHandler(
			appuser.NewUseCases(
				postgres.NewUserRepository(app.db),
				app.rateLimiter(),
				app.cfg.UserCreatePerMinute,
				app.logger,
			),
			time.Duration(app.cfg.RequestTimeout)*time.Second,
		)

		users := app.server.Group("/api/v1/users")
		users.Use(presmonitoring.HTTPMonitoringMiddleware(app.ms))
		users.Post("", userHandler.Create)
		users.Get("", userHandler.List)
		users.Get("/:id", userHandler.Get)
	} else {
		app.logger.Warn("database is not configured, user endpoints are disabled")
	}

	promMetrics := app.server.Group("/metrics")
	promMetrics.Get("", adaptor.HTTPHandler(promhttp.HandlerFor(app.ms.Registry(), promhttp.HandlerOpts{})))

	app.server.Get("/swagger/*", adaptor.HTTPHandler(httpSwagger.WrapHandler))
}

func (app *HTTPApp) rateLimiter() appuser.RateLimiter {
	if app.redis == nil {
		app.logger.Warn("redis is not configured, rate limiting is disabled")

		return appuser.NewUnlimited()
	}

	return infraredis.NewRateLimiter(app.redis)
}

func (app *HTTPApp) checkers() []healthcheck.Checker {
	checkers := make([]healthcheck.Checker, 0, 2)
	if app.db != nil {
		checkers = append(checkers, app.db)
	}
	if app.redis != nil {
		checkers = append(checkers, app.redis)
	}

	return checkers
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

func (app *HTTPApp) Close() {
	if app.redis != nil {
		if err := app.redis.Close(); err != nil {
			app.logger.Error("failed to close redis", "error", err)
		}
	}
	if app.db != nil {
		app.db.Close()
	}
}
