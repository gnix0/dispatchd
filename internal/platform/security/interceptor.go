package security

import (
	"context"
	"log/slog"

	"github.com/gnix0/task-orchestrator/internal/platform/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func BuildServerOptions(logger *slog.Logger, cfg config.Service) ([]grpc.ServerOption, error) {
	options := make([]grpc.ServerOption, 0, 3)

	transportCredentials, err := loadTransportCredentials(cfg)
	if err != nil {
		return nil, err
	}
	if transportCredentials != nil {
		options = append(options, grpc.Creds(transportCredentials))
	}

	authenticator, err := NewAuthenticator(cfg)
	if err != nil {
		return nil, err
	}

	options = append(options,
		grpc.ChainUnaryInterceptor(unaryAuthInterceptor(logger, authenticator, cfg)),
		grpc.ChainStreamInterceptor(streamAuthInterceptor(logger, authenticator, cfg)),
	)

	return options, nil
}

func unaryAuthInterceptor(logger *slog.Logger, authenticator *Authenticator, cfg config.Service) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		nextCtx, principal, err := authenticateAndAuthorize(ctx, authenticator, info.FullMethod)
		if err != nil {
			logAudit(logger, cfg, info.FullMethod, principal, err)
			return nil, err
		}

		response, handlerErr := handler(nextCtx, req)
		logAudit(logger, cfg, info.FullMethod, principal, handlerErr)
		return response, handlerErr
	}
}

func streamAuthInterceptor(logger *slog.Logger, authenticator *Authenticator, cfg config.Service) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		nextCtx, principal, err := authenticateAndAuthorize(stream.Context(), authenticator, info.FullMethod)
		if err != nil {
			logAudit(logger, cfg, info.FullMethod, principal, err)
			return err
		}

		wrapped := &serverStreamWithContext{
			ServerStream: stream,
			ctx:          nextCtx,
		}

		handlerErr := handler(srv, wrapped)
		logAudit(logger, cfg, info.FullMethod, principal, handlerErr)
		return handlerErr
	}
}

func authenticateAndAuthorize(ctx context.Context, authenticator *Authenticator, fullMethod string) (context.Context, *Principal, error) {
	if isPublicMethod(fullMethod) {
		return ctx, nil, nil
	}
	if authenticator == nil || !authenticator.Enabled() {
		return ctx, nil, nil
	}

	nextCtx, principal, err := authenticator.Authenticate(ctx)
	if err != nil {
		return ctx, principal, err
	}

	if !isAuthorized(fullMethod, principal) {
		return nextCtx, principal, status.Error(codes.PermissionDenied, "principal is not authorized for this method")
	}

	return nextCtx, principal, nil
}

func isPublicMethod(fullMethod string) bool {
	switch fullMethod {
	case "/grpc.health.v1.Health/Check",
		"/grpc.health.v1.Health/Watch":
		return true
	default:
		return false
	}
}

func isAuthorized(fullMethod string, principal *Principal) bool {
	switch fullMethod {
	case "/taskorchestrator.v1.JobService/SubmitJob":
		return HasAnyRole(principal, "submitter", "operator", "admin")
	case "/taskorchestrator.v1.JobService/CancelJob":
		return HasAnyRole(principal, "operator", "admin")
	case "/taskorchestrator.v1.JobService/GetJob",
		"/taskorchestrator.v1.JobService/ListExecutions":
		return HasAnyRole(principal, "viewer", "submitter", "operator", "admin")
	case "/taskorchestrator.v1.WorkerService/Connect":
		return HasAnyRole(principal, "worker", "service", "operator", "admin")
	default:
		return HasAnyRole(principal, "admin")
	}
}

func logAudit(logger *slog.Logger, cfg config.Service, method string, principal *Principal, err error) {
	if logger == nil || !cfg.AuditLogEnabled || isPublicMethod(method) {
		return
	}

	subject := "anonymous"
	roles := []string{}
	if principal != nil {
		subject = principal.Subject
		for role := range principal.Roles {
			roles = append(roles, role)
		}
	}

	level := slog.LevelInfo
	outcome := "success"
	if err != nil {
		level = slog.LevelWarn
		outcome = status.Code(err).String()
	}

	logger.Log(context.Background(), level, "audit event",
		slog.String("method", method),
		slog.String("subject", subject),
		slog.Any("roles", roles),
		slog.String("outcome", outcome),
	)
}

type serverStreamWithContext struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *serverStreamWithContext) Context() context.Context {
	return s.ctx
}
