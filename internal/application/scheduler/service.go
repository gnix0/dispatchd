package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/gnix0/dispatchd/internal/application/jobs"
	"github.com/gnix0/dispatchd/internal/platform/observability"
	"go.opentelemetry.io/otel/attribute"
)

type Service struct {
	logger        *slog.Logger
	store         jobs.SchedulerStore
	instanceID    string
	pollInterval  time.Duration
	leaseDuration time.Duration
	workerTTL     time.Duration
}

func NewService(logger *slog.Logger, store jobs.SchedulerStore, instanceID string, pollInterval, leaseDuration, workerTTL time.Duration) *Service {
	if pollInterval <= 0 {
		pollInterval = 2 * time.Second
	}
	if leaseDuration <= 0 {
		leaseDuration = 30 * time.Second
	}
	if workerTTL <= 0 {
		workerTTL = 45 * time.Second
	}

	return &Service{
		logger:        logger,
		store:         store,
		instanceID:    instanceID,
		pollInterval:  pollInterval,
		leaseDuration: leaseDuration,
		workerTTL:     workerTTL,
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

func (s *Service) Tick(ctx context.Context) (isLeader bool, err error) {
	started := time.Now()
	requeued := 0
	enqueued := 0

	ctx, span := observability.StartSpan(ctx, "scheduler.tick",
		attribute.String("scheduler.instance_id", s.instanceID),
	)
	defer func() {
		span.End()
		observability.RecordSchedulerTick(time.Since(started), isLeader, requeued, enqueued, err)
	}()

	isLeader, err = s.store.TryAcquireLeadership(ctx, s.instanceID, s.pollInterval*3)
	if err != nil {
		return false, err
	}
	if !isLeader {
		s.logger.Debug("scheduler tick skipped because instance is not leader")
		return false, nil
	}

	requeued, err = s.store.RequeueExpiredExecutions(ctx, s.workerTTL, 256)
	if err != nil {
		return false, err
	}

	enqueued, err = s.store.EnqueueRunnableExecutions(ctx, 256)
	if err != nil {
		return false, err
	}

	if requeued == 0 && enqueued == 0 {
		s.logger.Debug("scheduler tick found no runnable or expired executions")
		return false, nil
	}

	s.logger.Info(
		"scheduler reconciled execution queues",
		slog.Int("requeued_expired", requeued),
		slog.Int("enqueued_runnable", enqueued),
	)

	return true, nil
}
