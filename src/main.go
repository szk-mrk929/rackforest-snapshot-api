package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"rackforest-snapshot-api/src/api"
	"rackforest-snapshot-api/src/app"
	"rackforest-snapshot-api/src/backend"
	"rackforest-snapshot-api/src/config"
	"rackforest-snapshot-api/src/logger"
	"rackforest-snapshot-api/src/store"
	"rackforest-snapshot-api/src/worker"
)

func main() {
	conf, err := config.Load()
	if err != nil {
		slog.Error("environment loading failed", "error", err)
		os.Exit(1)
	}

	log := logger.New(conf.LogLevel, os.Stdout)
	slog.SetDefault(log)

	if err := conf.Validate(); err != nil {
		log.WithGroup("main").Error("environment validation failed", "error", err)
		os.Exit(1)
	}

	log.WithGroup("main").Info("service started", "config", conf)

	if err := run(conf, log); err != nil {
		log.WithGroup("main").Error("service stopped", "error", err)
		os.Exit(1)
	}
	log.WithGroup("main").Info("service stopped")
}

func run(conf config.Config, log *slog.Logger) error {
	st := store.NewMemory()
	be := backend.NewMock(backend.MockConfig{
		MinDelay:  conf.StorageMinDelay,
		MaxDelay:  conf.StorageMaxDelay,
		ErrorRate: conf.StorageErrorRate,
	})
	jobs, err := worker.New(worker.Config{
		WorkerCount:     conf.WorkerCount,
		WorkerAttempts:  conf.WorkerAttempts,
		WorkerQueueSize: conf.WorkerQueueSize,
		StorageTimeout:  conf.StorageTimeout,
		RetryBaseDelay:  conf.RetryBaseDelay,
	}, st, be, log)
	if err != nil {
		return err
	}

	application, err := app.New(conf, log, jobs)
	if err != nil {
		return err
	}
	snapshots, err := api.NewServer(st, jobs, conf.TenantSnapshotQuota, log)
	if err != nil {
		return err
	}
	snapshots.Mount(application.Mux())

	// SIGINT and SIGTERM cancel this context. Run then drains on its own
	// budget, ShutdownTimeout, instead of the already-canceled signal context.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return application.Run(ctx)
}
