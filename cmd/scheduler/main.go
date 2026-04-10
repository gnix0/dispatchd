package main

import (
	"context"
	"log/slog"
	"os"

	schedulerapp "github.com/gnix0/task-orchestrator/internal/application/scheduler"
	"github.com/gnix0/task-orchestrator/internal/platform/config"
	runtimeapp "github.com/gnix0/task-orchestrator/internal/platform/runtime"
	"github.com/gnix0/task-orchestrator/internal/platform/store"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := config.Load("scheduler")

	runtimeapp.LogBootstrap(logger, serviceConfig)

	ctx, stop := runtimeapp.SignalContext()
	defer stop()

	sharedStore, err := store.Open(context.Background(), serviceConfig)
	if err != nil {
		logger.Error("failed to initialize shared state", slog.Any("error", err))
		os.Exit(1)
	}
	defer sharedStore.Close()

	schedulerService := schedulerapp.NewService(
		logger,
		sharedStore,
		serviceConfig.InstanceID,
		serviceConfig.SchedulerPollInterval,
		serviceConfig.LeaseDuration,
		serviceConfig.WorkerHeartbeatTTL,
	)

	if err := schedulerService.Run(ctx); err != nil {
		logger.Error("scheduler exited with error", slog.Any("error", err))
		os.Exit(1)
	}

	runtimeapp.LogStopping(logger, serviceConfig)
}
