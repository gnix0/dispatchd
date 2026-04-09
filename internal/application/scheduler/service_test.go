package scheduler

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/gnix0/task-orchestrator/internal/application/jobs"
)

func TestTickClaimsRunnableExecution(t *testing.T) {
	store := &fakeStore{
		execution: jobs.Execution{ID: "exec-1", Attempt: 1},
		job:       jobs.Job{ID: "job-1"},
		ok:        true,
	}
	service := NewService(slog.New(slog.NewTextHandler(io.Discard, nil)), store, time.Second, 10*time.Second)

	claimed, err := service.Tick(context.Background())
	if err != nil {
		t.Fatalf("expected tick to succeed, got %v", err)
	}
	if !claimed {
		t.Fatal("expected tick to claim an execution")
	}
	if store.claimLeaseDuration != 10*time.Second {
		t.Fatalf("expected claim lease duration 10s, got %s", store.claimLeaseDuration)
	}
}

func TestHandleDispatchFailureDelegatesToStore(t *testing.T) {
	store := &fakeStore{
		failedExecution: jobs.Execution{ID: "exec-1", Status: jobs.ExecutionStatusFailed},
		nextExecution:   &jobs.Execution{ID: "exec-2", Attempt: 2},
		job:             jobs.Job{ID: "job-1"},
	}
	service := NewService(slog.New(slog.NewTextHandler(io.Discard, nil)), store, time.Second, 10*time.Second)

	if err := service.HandleDispatchFailure(context.Background(), "exec-1", "dispatch timeout"); err != nil {
		t.Fatalf("expected dispatch failure handling to succeed, got %v", err)
	}
	if store.failedExecutionID != "exec-1" || store.failureReason != "dispatch timeout" {
		t.Fatalf("unexpected failure delegation: execution=%q reason=%q", store.failedExecutionID, store.failureReason)
	}
}

type fakeStore struct {
	execution          jobs.Execution
	failedExecution    jobs.Execution
	nextExecution      *jobs.Execution
	job                jobs.Job
	ok                 bool
	claimLeaseDuration time.Duration
	failedExecutionID  string
	failureReason      string
}

func (s *fakeStore) ClaimNextRunnable(context.Context, time.Duration) (jobs.Execution, jobs.Job, bool, error) {
	s.claimLeaseDuration = 10 * time.Second
	return s.execution, s.job, s.ok, nil
}

func (s *fakeStore) MarkExecutionFailed(context.Context, string, string) (jobs.Execution, *jobs.Execution, jobs.Job, error) {
	s.failedExecutionID = "exec-1"
	s.failureReason = "dispatch timeout"
	return s.failedExecution, s.nextExecution, s.job, nil
}
