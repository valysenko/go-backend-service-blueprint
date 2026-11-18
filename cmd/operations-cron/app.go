package main

import (
	"context"
	"log/slog"
	"time"

	"go-backend-service-blueprint/cmd/shared"
	"go-backend-service-blueprint/config"
	appoperation "go-backend-service-blueprint/internal/app/operation"
	domainoperation "go-backend-service-blueprint/internal/domain/operation"
	inframonitoring "go-backend-service-blueprint/internal/infrastructure/monitoring"
	"go-backend-service-blueprint/internal/infrastructure/notification"
	"go-backend-service-blueprint/internal/infrastructure/postgres"
	"go-backend-service-blueprint/pkg/database"
)

const appName = "operations_cron"

type CronApp struct {
	cfg    *config.CronAppConfig
	db     *database.AppDB
	logger *slog.Logger
	runner *appoperation.Runner
}

func NewCronApp(cfg *config.CronAppConfig) (*CronApp, error) {
	logger := shared.NewLogger()

	db, err := shared.NewPostgres(cfg.PostgreSQLConfig)
	if err != nil {
		return nil, err
	}

	operations := postgres.NewOperationRepository(db)
	registry := appoperation.NewRegistry(map[domainoperation.OperationType]appoperation.Executor{
		domainoperation.OperationTypeSendWelcomeEmail: appoperation.NewWelcomeEmailExecutor(
			postgres.NewUserRepository(db),
			notification.NewLogEmailSender(logger),
		),
		domainoperation.OperationTypeRecalculateStats: appoperation.NewRecalculateStatsExecutor(),
	})

	metrics := inframonitoring.NewCronPrometheusMetricsService(cfg.Namespace, appName, cfg.PodName, cfg.PushgatewayURL)
	processor := appoperation.NewProcessor(operations, registry, postgres.NewTxManager(db), logger)

	return &CronApp{
		cfg:    cfg,
		db:     db,
		logger: logger,
		runner: appoperation.NewRunner(operations, processor, metrics, logger, cfg.BatchSize),
	}, nil
}

func (app *CronApp) Run(ctx context.Context) error {
	_, err := app.runner.Run(ctx, time.Now().UTC())

	return err
}

func (app *CronApp) Close() {
	if app.db != nil {
		app.db.Close()
	}
}
