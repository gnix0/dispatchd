package main

import (
	"log/slog"
	"os"
	"time"

	"github.com/gnix0/task-orchestrator/internal/application/jobs"
	schedulerapp "github.com/gnix0/task-orchestrator/internal/application/scheduler"
	"github.com/gnix0/task-orchestrator/internal/platform/config"
	runtimeapp "github.com/gnix0/task-orchestrator/internal/platform/runtime"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := config.Load("scheduler")
	jobStore := jobs.NewInMemoryService()
	schedulerService := schedulerapp.NewService(logger, jobStore, 2*time.Second, 30*time.Second)

	runtimeapp.LogBootstrap(logger, serviceConfig)

	ctx, stop := runtimeapp.SignalContext()
	defer stop()

	if err := schedulerService.Run(ctx); err != nil {
		logger.Error("scheduler exited with error", slog.Any("error", err))
		os.Exit(1)
	}

	runtimeapp.LogStopping(logger, serviceConfig)
}
