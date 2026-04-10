package runtime

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/gnix0/dispatchd/internal/platform/config"
	"github.com/gnix0/dispatchd/internal/version"
)

func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

func LogBootstrap(logger *slog.Logger, serviceConfig config.Service) {
	logger.Info(
		"service bootstrapped",
		slog.String("service", serviceConfig.Name),
		slog.String("environment", serviceConfig.Environment),
		slog.String("version", version.Version),
		slog.String("commit", version.Commit),
	)
}

func LogStopping(logger *slog.Logger, serviceConfig config.Service) {
	logger.Info(
		"service stopping",
		slog.String("service", serviceConfig.Name),
		slog.String("timeout", serviceConfig.ShutdownTimeout.String()),
	)
}

func Run(logger *slog.Logger, serviceConfig config.Service) {
	LogBootstrap(logger, serviceConfig)

	ctx, stop := SignalContext()
	defer stop()

	<-ctx.Done()

	LogStopping(logger, serviceConfig)
}
