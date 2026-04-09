package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/gnix0/task-orchestrator/internal/application/jobs"
)

type Service struct {
	logger        *slog.Logger
	store         jobs.SchedulerStore
	pollInterval  time.Duration
	leaseDuration time.Duration
}

func NewService(logger *slog.Logger, store jobs.SchedulerStore, pollInterval, leaseDuration time.Duration) *Service {
	if pollInterval <= 0 {
		pollInterval = 2 * time.Second
	}
	if leaseDuration <= 0 {
		leaseDuration = 30 * time.Second
	}

	return &Service{
		logger:        logger,
		store:         store,
		pollInterval:  pollInterval,
		leaseDuration: leaseDuration,
	}
}

func (s *Service) Run(ctx context.Context) error {
	if _, err := s.Tick(ctx); err != nil {
		return err
	}

	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if _, err := s.Tick(ctx); err != nil {
				return err
			}
		}
	}
}

func (s *Service) Tick(ctx context.Context) (bool, error) {
	execution, job, ok, err := s.store.ClaimNextRunnable(ctx, s.leaseDuration)
	if err != nil {
		return false, err
	}

	if !ok {
		s.logger.Debug("scheduler tick found no runnable executions")
		return false, nil
	}

	s.logger.Info(
		"execution claimed for dispatch",
		slog.String("job_id", job.ID),
		slog.String("execution_id", execution.ID),
		slog.Int64("attempt", int64(execution.Attempt)),
	)

	return true, nil
}

func (s *Service) HandleDispatchFailure(ctx context.Context, executionID, reason string) error {
	failedExecution, nextExecution, job, err := s.store.MarkExecutionFailed(ctx, executionID, reason)
	if err != nil {
		return err
	}

	if nextExecution != nil {
		s.logger.Info(
			"execution failed and was rescheduled",
			slog.String("job_id", job.ID),
			slog.String("failed_execution_id", failedExecution.ID),
			slog.String("next_execution_id", nextExecution.ID),
			slog.Int64("next_attempt", int64(nextExecution.Attempt)),
		)
		return nil
	}

	s.logger.Info(
		"execution failed terminally",
		slog.String("job_id", job.ID),
		slog.String("execution_id", failedExecution.ID),
		slog.String("status", failedExecution.Status),
	)
	return nil
}
