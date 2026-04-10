package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/gnix0/task-orchestrator/internal/platform/config"
	"github.com/gnix0/task-orchestrator/internal/platform/grpcserver"
	runtimeapp "github.com/gnix0/task-orchestrator/internal/platform/runtime"
	"github.com/gnix0/task-orchestrator/internal/platform/store"
	"github.com/gnix0/task-orchestrator/internal/transport/grpcapi"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := config.Load("worker-gateway")

	runtimeapp.LogBootstrap(logger, serviceConfig)

	ctx, stop := runtimeapp.SignalContext()
	defer stop()

	sharedStore, err := store.Open(context.Background(), serviceConfig)
	if err != nil {
		logger.Error("failed to initialize shared state", slog.Any("error", err))
		os.Exit(1)
	}
	defer sharedStore.Close()

	if err := grpcserver.Run(ctx, logger, serviceConfig, grpcapi.RegisterWorkerGateway(sharedStore, sharedStore, serviceConfig.LeaseDuration)); err != nil {
		logger.Error("worker-gateway exited with error", slog.Any("error", err))
		os.Exit(1)
	}

	runtimeapp.LogStopping(logger, serviceConfig)
}
