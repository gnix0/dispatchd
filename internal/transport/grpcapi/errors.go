package grpcapi

import (
	"errors"

	"github.com/gnix0/task-orchestrator/internal/application/jobs"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func toStatusError(err error) error {
	switch {
	case errors.Is(err, jobs.ErrInvalidArgument):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, jobs.ErrJobNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, jobs.ErrIdempotencyConflict):
		return status.Error(codes.AlreadyExists, err.Error())
	default:
		return status.Error(codes.Internal, "internal server error")
	}
}
