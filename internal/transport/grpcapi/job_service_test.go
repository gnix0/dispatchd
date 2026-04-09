package grpcapi

import (
	"context"
	"testing"

	taskorchestratorv1 "github.com/gnix0/task-orchestrator/gen/go/taskorchestrator/v1"
	"github.com/gnix0/task-orchestrator/internal/application/jobs"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSubmitJobReturnsCreatedJob(t *testing.T) {
	jobApplication := jobs.NewInMemoryService()
	service := &JobService{jobApplication: jobApplication}

	response, err := service.SubmitJob(context.Background(), &taskorchestratorv1.SubmitJobRequest{
		JobType:        "email.send",
		Payload:        []byte("payload"),
		IdempotencyKey: "idem-1",
	})
	if err != nil {
		t.Fatalf("expected submit to succeed, got %v", err)
	}

	if response.GetJob().GetJobId() == "" {
		t.Fatal("expected generated job id to be set")
	}

	if response.GetJob().GetStatus() != taskorchestratorv1.JobStatus_JOB_STATUS_PENDING {
		t.Fatalf("expected pending status, got %v", response.GetJob().GetStatus())
	}

	got, err := service.GetJob(context.Background(), &taskorchestratorv1.GetJobRequest{JobId: response.GetJob().GetJobId()})
	if err != nil {
		t.Fatalf("expected get job to succeed, got %v", err)
	}

	if got.GetJob().GetIdempotencyKey() != "idem-1" {
		t.Fatalf("expected persisted idempotency key idem-1, got %q", got.GetJob().GetIdempotencyKey())
	}
}

func TestSubmitJobMapsValidationErrors(t *testing.T) {
	service := &JobService{jobApplication: jobs.NewInMemoryService()}

	_, err := service.SubmitJob(context.Background(), &taskorchestratorv1.SubmitJobRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", status.Code(err))
	}
}

func TestGetJobMapsNotFound(t *testing.T) {
	service := &JobService{jobApplication: jobs.NewInMemoryService()}

	_, err := service.GetJob(context.Background(), &taskorchestratorv1.GetJobRequest{JobId: "missing"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", status.Code(err))
	}
}

func TestIdempotencyConflictMapsToAlreadyExists(t *testing.T) {
	jobApplication := jobs.NewInMemoryService()
	service := &JobService{jobApplication: jobApplication}

	_, err := service.SubmitJob(context.Background(), &taskorchestratorv1.SubmitJobRequest{
		JobType:        "email.send",
		Payload:        []byte("payload"),
		IdempotencyKey: "idem-1",
	})
	if err != nil {
		t.Fatalf("expected first submit to succeed, got %v", err)
	}

	_, err = service.SubmitJob(context.Background(), &taskorchestratorv1.SubmitJobRequest{
		JobType:        "email.send",
		Payload:        []byte("different"),
		IdempotencyKey: "idem-1",
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("expected AlreadyExists, got %v", status.Code(err))
	}
}

func TestListExecutionsReturnsPersistedExecution(t *testing.T) {
	jobApplication := jobs.NewInMemoryService()
	service := &JobService{jobApplication: jobApplication}

	submitResponse, err := service.SubmitJob(context.Background(), &taskorchestratorv1.SubmitJobRequest{
		JobType: "email.send",
		Payload: []byte("payload"),
	})
	if err != nil {
		t.Fatalf("expected submit to succeed, got %v", err)
	}

	listResponse, err := service.ListExecutions(context.Background(), &taskorchestratorv1.ListExecutionsRequest{
		JobId: submitResponse.GetJob().GetJobId(),
	})
	if err != nil {
		t.Fatalf("expected list executions to succeed, got %v", err)
	}

	if len(listResponse.GetExecutions()) != 1 {
		t.Fatalf("expected one execution, got %d", len(listResponse.GetExecutions()))
	}

	if listResponse.GetExecutions()[0].GetStatus() != taskorchestratorv1.ExecutionStatus_EXECUTION_STATUS_QUEUED {
		t.Fatalf("expected queued execution status, got %v", listResponse.GetExecutions()[0].GetStatus())
	}
}
