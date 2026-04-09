package jobs

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSubmitJobStoresPendingJobAndInitialExecution(t *testing.T) {
	service := NewInMemoryService()
	service.now = func() time.Time { return time.Date(2026, 4, 9, 18, 0, 0, 0, time.UTC) }
	service.newJobID = func() string { return "job_fixed" }
	service.newExecutionID = func() string { return "exec_initial" }

	job, err := service.SubmitJob(context.Background(), SubmitJobInput{
		JobType:        "email.send",
		Payload:        []byte(`{"user_id":"123"}`),
		Priority:       2,
		IdempotencyKey: "submit-1",
		Metadata:       map[string]string{"source": "api"},
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if job.ID != "job_fixed" {
		t.Fatalf("expected stable job id, got %q", job.ID)
	}

	if job.Status != StatusPending {
		t.Fatalf("expected pending status, got %q", job.Status)
	}

	executions, err := service.ListExecutions(context.Background(), "job_fixed")
	if err != nil {
		t.Fatalf("expected list executions to succeed, got %v", err)
	}

	if len(executions) != 1 {
		t.Fatalf("expected exactly one initial execution, got %d", len(executions))
	}

	if executions[0].ID != "exec_initial" || executions[0].Status != ExecutionStatusQueued || executions[0].Attempt != 1 {
		t.Fatalf("unexpected initial execution: %#v", executions[0])
	}
}

func TestSubmitJobReturnsExistingJobForSameIdempotencyRequest(t *testing.T) {
	service := NewInMemoryService()
	service.newJobID = func() string { return "job_same" }
	service.newExecutionID = func() string { return "exec_same" }

	first, err := service.SubmitJob(context.Background(), SubmitJobInput{
		JobType:        "report.generate",
		Payload:        []byte("payload"),
		IdempotencyKey: "idem-1",
	})
	if err != nil {
		t.Fatalf("expected first submit to succeed, got %v", err)
	}

	second, err := service.SubmitJob(context.Background(), SubmitJobInput{
		JobType:        "report.generate",
		Payload:        []byte("payload"),
		IdempotencyKey: "idem-1",
	})
	if err != nil {
		t.Fatalf("expected second submit to succeed, got %v", err)
	}

	if first.ID != second.ID {
		t.Fatalf("expected idempotent submit to return same job id, got %q and %q", first.ID, second.ID)
	}
}

func TestSubmitJobRejectsIdempotencyConflict(t *testing.T) {
	service := NewInMemoryService()
	service.newJobID = func() string { return "job_same" }
	service.newExecutionID = func() string { return "exec_same" }

	_, err := service.SubmitJob(context.Background(), SubmitJobInput{
		JobType:        "report.generate",
		Payload:        []byte("payload"),
		IdempotencyKey: "idem-1",
	})
	if err != nil {
		t.Fatalf("expected first submit to succeed, got %v", err)
	}

	_, err = service.SubmitJob(context.Background(), SubmitJobInput{
		JobType:        "report.generate",
		Payload:        []byte("different"),
		IdempotencyKey: "idem-1",
	})
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("expected idempotency conflict, got %v", err)
	}
}

func TestCancelJobUpdatesStatus(t *testing.T) {
	service := NewInMemoryService()
	service.newJobID = func() string { return "job_cancel" }
	service.newExecutionID = func() string { return "exec_cancel" }

	job, err := service.SubmitJob(context.Background(), SubmitJobInput{
		JobType: "cleanup",
		Payload: []byte("payload"),
	})
	if err != nil {
		t.Fatalf("expected submit to succeed, got %v", err)
	}

	canceled, err := service.CancelJob(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("expected cancel to succeed, got %v", err)
	}

	if canceled.Status != StatusCanceled {
		t.Fatalf("expected canceled status, got %q", canceled.Status)
	}
}

func TestListExecutionsReturnsNotFoundForUnknownJob(t *testing.T) {
	service := NewInMemoryService()

	_, err := service.ListExecutions(context.Background(), "missing")
	if !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("expected job not found, got %v", err)
	}
}

func TestClaimNextRunnablePrefersHigherPriorityJobs(t *testing.T) {
	service := NewInMemoryService()
	now := time.Date(2026, 4, 9, 18, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }

	jobIDs := []string{"job_low", "job_high"}
	executionIDs := []string{"exec_low", "exec_high"}
	service.newJobID = func() string {
		id := jobIDs[0]
		jobIDs = jobIDs[1:]
		return id
	}
	service.newExecutionID = func() string {
		id := executionIDs[0]
		executionIDs = executionIDs[1:]
		return id
	}

	_, err := service.SubmitJob(context.Background(), SubmitJobInput{
		JobType:  "low",
		Payload:  []byte("payload"),
		Priority: 1,
	})
	if err != nil {
		t.Fatalf("expected low priority submit to succeed, got %v", err)
	}

	_, err = service.SubmitJob(context.Background(), SubmitJobInput{
		JobType:  "high",
		Payload:  []byte("payload"),
		Priority: 9,
	})
	if err != nil {
		t.Fatalf("expected high priority submit to succeed, got %v", err)
	}

	execution, job, ok, err := service.ClaimNextRunnable(context.Background(), 30*time.Second)
	if err != nil {
		t.Fatalf("expected claim to succeed, got %v", err)
	}
	if !ok {
		t.Fatal("expected a runnable execution to be claimed")
	}

	if job.ID != "job_high" || execution.ID != "exec_high" {
		t.Fatalf("expected high priority job to be claimed first, got job=%q execution=%q", job.ID, execution.ID)
	}

	if job.Status != StatusDispatching || execution.Status != ExecutionStatusClaimed {
		t.Fatalf("unexpected claimed state: job=%q execution=%q", job.Status, execution.Status)
	}
}

func TestMarkExecutionFailedSchedulesRetryWithBackoff(t *testing.T) {
	service := NewInMemoryService()
	baseTime := time.Date(2026, 4, 9, 18, 0, 0, 0, time.UTC)
	currentTime := baseTime
	service.now = func() time.Time { return currentTime }

	jobIDs := []string{"job_retry"}
	executionIDs := []string{"exec_first", "exec_second"}
	service.newJobID = func() string {
		id := jobIDs[0]
		jobIDs = jobIDs[1:]
		return id
	}
	service.newExecutionID = func() string {
		id := executionIDs[0]
		executionIDs = executionIDs[1:]
		return id
	}

	job, err := service.SubmitJob(context.Background(), SubmitJobInput{
		JobType: "report.generate",
		Payload: []byte("payload"),
		RetryPolicy: RetryPolicy{
			MaxAttempts:    3,
			InitialBackoff: 10 * time.Second,
			MaxBackoff:     30 * time.Second,
		},
	})
	if err != nil {
		t.Fatalf("expected submit to succeed, got %v", err)
	}

	claimedExecution, _, ok, err := service.ClaimNextRunnable(context.Background(), 30*time.Second)
	if err != nil {
		t.Fatalf("expected claim to succeed, got %v", err)
	}
	if !ok {
		t.Fatal("expected claimed execution")
	}

	currentTime = baseTime.Add(3 * time.Second)
	failedExecution, nextExecution, updatedJob, err := service.MarkExecutionFailed(context.Background(), claimedExecution.ID, "dispatch timeout")
	if err != nil {
		t.Fatalf("expected failure handling to succeed, got %v", err)
	}

	if failedExecution.Status != ExecutionStatusFailed {
		t.Fatalf("expected failed execution status, got %q", failedExecution.Status)
	}

	if nextExecution == nil {
		t.Fatal("expected retry execution to be created")
	}

	if nextExecution.Attempt != 2 || nextExecution.Status != ExecutionStatusQueued {
		t.Fatalf("unexpected retry execution: %#v", *nextExecution)
	}

	expectedAvailableAt := currentTime.Add(10 * time.Second)
	if !nextExecution.AvailableAt.Equal(expectedAvailableAt) {
		t.Fatalf("expected retry available at %s, got %s", expectedAvailableAt, nextExecution.AvailableAt)
	}

	if updatedJob.Status != StatusPending {
		t.Fatalf("expected job to return to pending, got %q", updatedJob.Status)
	}

	executions, err := service.ListExecutions(context.Background(), job.ID)
	if err != nil {
		t.Fatalf("expected list executions to succeed, got %v", err)
	}

	if len(executions) != 2 {
		t.Fatalf("expected two execution records, got %d", len(executions))
	}
}

func TestMarkExecutionFailedDeadLettersAtMaxAttempts(t *testing.T) {
	service := NewInMemoryService()
	service.now = func() time.Time { return time.Date(2026, 4, 9, 18, 0, 0, 0, time.UTC) }
	service.newJobID = func() string { return "job_terminal" }
	service.newExecutionID = func() string { return "exec_terminal" }

	_, err := service.SubmitJob(context.Background(), SubmitJobInput{
		JobType: "report.generate",
		Payload: []byte("payload"),
		RetryPolicy: RetryPolicy{
			MaxAttempts:    1,
			InitialBackoff: 5 * time.Second,
			MaxBackoff:     5 * time.Second,
		},
	})
	if err != nil {
		t.Fatalf("expected submit to succeed, got %v", err)
	}

	claimedExecution, _, ok, err := service.ClaimNextRunnable(context.Background(), 30*time.Second)
	if err != nil {
		t.Fatalf("expected claim to succeed, got %v", err)
	}
	if !ok {
		t.Fatal("expected claimed execution")
	}

	failedExecution, nextExecution, updatedJob, err := service.MarkExecutionFailed(context.Background(), claimedExecution.ID, "final failure")
	if err != nil {
		t.Fatalf("expected failure handling to succeed, got %v", err)
	}

	if nextExecution != nil {
		t.Fatalf("expected no retry execution, got %#v", *nextExecution)
	}

	if failedExecution.Status != ExecutionStatusDeadLettered {
		t.Fatalf("expected dead lettered status, got %q", failedExecution.Status)
	}

	if updatedJob.Status != StatusFailed {
		t.Fatalf("expected failed job status, got %q", updatedJob.Status)
	}
}
