package grpcserver

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"time"

	"github.com/gnix0/task-orchestrator/internal/platform/config"
	"github.com/gnix0/task-orchestrator/internal/platform/security"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

type Registrar func(*grpc.Server)

func Run(ctx context.Context, logger *slog.Logger, serviceConfig config.Service, registrars ...Registrar) error {
	listener, err := net.Listen("tcp", serviceConfig.GRPCAddress())
	if err != nil {
		return err
	}

	serverOptions, err := security.BuildServerOptions(logger, serviceConfig)
	if err != nil {
		return err
	}

	server := grpc.NewServer(serverOptions...)
	healthServer := health.NewServer()

	healthpb.RegisterHealthServer(server, healthServer)
	reflection.Register(server)

	for _, registrar := range registrars {
		registrar(server)
	}

	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	logger.Info("gRPC server listening", slog.String("service", serviceConfig.Name), slog.String("address", serviceConfig.GRPCAddress()))

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		healthServer.Shutdown()
		shutdown(server, serviceConfig.ShutdownTimeout)
		return nil
	case err := <-serveErr:
		if errors.Is(err, grpc.ErrServerStopped) {
			return nil
		}

		return err
	}
}

func shutdown(server *grpc.Server, timeout time.Duration) {
	done := make(chan struct{})

	go func() {
		server.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		server.Stop()
	}
}
