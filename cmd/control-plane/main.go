package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/gnix0/task-orchestrator/internal/platform/config"
	"github.com/gnix0/task-orchestrator/internal/platform/grpcserver"
	"github.com/gnix0/task-orchestrator/internal/platform/observability"
	runtimeapp "github.com/gnix0/task-orchestrator/internal/platform/runtime"
	"github.com/gnix0/task-orchestrator/internal/platform/store"
	"github.com/gnix0/task-orchestrator/internal/transport/grpcapi"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	serviceConfig := config.Load("control-plane")

	runtimeapp.LogBootstrap(logger, serviceConfig)

	ctx, stop := runtimeapp.SignalContext()
	defer stop()

	observabilityHandle, err := observability.Start(context.Background(), logger, serviceConfig)
	if err != nil {
		logger.Error("failed to initialize observability", slog.Any("error", err))
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), serviceConfig.ShutdownTimeout)
		defer cancel()
		if err := observabilityHandle.Shutdown(shutdownCtx); err != nil {
			logger.Warn("observability shutdown failed", slog.Any("error", err))
		}
	}()

	sharedStore, err := store.Open(context.Background(), serviceConfig)
	if err != nil {
		logger.Error("failed to initialize shared state", slog.Any("error", err))
		os.Exit(1)
	}
	defer sharedStore.Close()

	if err := grpcserver.Run(ctx, logger, serviceConfig, grpcapi.RegisterControlPlane(sharedStore)); err != nil {
		logger.Error("control-plane exited with error", slog.Any("error", err))
		os.Exit(1)
	}

	runtimeapp.LogStopping(logger, serviceConfig)
}
