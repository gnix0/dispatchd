package main

import (
	"log/slog"
	"os"

	"github.com/gnix0/task-orchestrator/internal/application/workers"
	"github.com/gnix0/task-orchestrator/internal/platform/config"
	"github.com/gnix0/task-orchestrator/internal/platform/grpcserver"
	runtimeapp "github.com/gnix0/task-orchestrator/internal/platform/runtime"
	"github.com/gnix0/task-orchestrator/internal/transport/grpcapi"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := config.Load("worker-gateway")
	workerApplication := workers.NewInMemoryService()

	runtimeapp.LogBootstrap(logger, serviceConfig)

	ctx, stop := runtimeapp.SignalContext()
	defer stop()

	if err := grpcserver.Run(ctx, logger, serviceConfig, grpcapi.RegisterWorkerGateway(workerApplication)); err != nil {
		logger.Error("worker-gateway exited with error", slog.Any("error", err))
		os.Exit(1)
	}

	runtimeapp.LogStopping(logger, serviceConfig)
}
