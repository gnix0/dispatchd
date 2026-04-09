package runtime

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/gnix0/task-orchestrator/internal/platform/config"
	"github.com/gnix0/task-orchestrator/internal/version"
)

func Run(logger *slog.Logger, serviceConfig config.Service) {
	logger.Info(
		"service bootstrapped",
		slog.String("service", serviceConfig.Name),
		slog.String("environment", serviceConfig.Environment),
		slog.String("version", version.Version),
		slog.String("commit", version.Commit),
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), serviceConfig.ShutdownTimeout)
	defer cancel()

	logger.Info(
		"service stopping",
		slog.String("service", serviceConfig.Name),
		slog.String("timeout", serviceConfig.ShutdownTimeout.String()),
	)

	<-shutdownCtx.Done()
}
