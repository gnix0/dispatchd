package jobs

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSubmitJobStoresAndReturnsPendingJob(t *testing.T) {
	service := NewInMemoryService()
	service.now = func() time.Time { return time.Date(2026, 4, 9, 18, 0, 0, 0, time.UTC) }
	service.newID = func() string { return "job_fixed" }

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

	if job.RetryPolicy.MaxAttempts != 3 {
		t.Fatalf("expected default retry attempts 3, got %d", job.RetryPolicy.MaxAttempts)
	}

	got, err := service.GetJob(context.Background(), "job_fixed")
	if err != nil {
		t.Fatalf("expected get job to succeed, got %v", err)
	}

	if got.JobType != "email.send" {
		t.Fatalf("expected stored job type email.send, got %q", got.JobType)
	}
}

func TestSubmitJobReturnsExistingJobForSameIdempotencyRequest(t *testing.T) {
	service := NewInMemoryService()
	service.newID = func() string { return "job_same" }

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
	service.newID = func() string { return "job_same" }

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
	service.newID = func() string { return "job_cancel" }

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
