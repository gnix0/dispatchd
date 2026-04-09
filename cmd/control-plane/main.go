package main

import (
	"log/slog"
	"os"

	"github.com/gnix0/task-orchestrator/internal/application/jobs"
	"github.com/gnix0/task-orchestrator/internal/platform/config"
	"github.com/gnix0/task-orchestrator/internal/platform/grpcserver"
	runtimeapp "github.com/gnix0/task-orchestrator/internal/platform/runtime"
	"github.com/gnix0/task-orchestrator/internal/transport/grpcapi"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := config.Load("control-plane")
	jobApplication := jobs.NewInMemoryService()

	runtimeapp.LogBootstrap(logger, serviceConfig)

	ctx, stop := runtimeapp.SignalContext()
	defer stop()

	if err := grpcserver.Run(ctx, logger, serviceConfig, grpcapi.RegisterControlPlane(jobApplication)); err != nil {
		logger.Error("control-plane exited with error", slog.Any("error", err))
		os.Exit(1)
	}

	runtimeapp.LogStopping(logger, serviceConfig)
}
