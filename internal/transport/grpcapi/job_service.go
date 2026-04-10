package grpcapi

import (
	"context"
	"time"

	taskorchestratorv1 "github.com/gnix0/task-orchestrator/gen/go/taskorchestrator/v1"
	"github.com/gnix0/task-orchestrator/internal/application/jobs"
	"github.com/gnix0/task-orchestrator/internal/platform/observability"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type JobService struct {
	jobApplication jobs.Service
	taskorchestratorv1.UnimplementedJobServiceServer
}

var _ taskorchestratorv1.JobServiceServer = (*JobService)(nil)

func RegisterControlPlane(jobApplication jobs.Service) func(*grpc.Server) {
	return func(server *grpc.Server) {
		taskorchestratorv1.RegisterJobServiceServer(server, &JobService{jobApplication: jobApplication})
	}
}

func (s *JobService) SubmitJob(ctx context.Context, request *taskorchestratorv1.SubmitJobRequest) (_ *taskorchestratorv1.SubmitJobResponse, err error) {
	started := time.Now()
	ctx, span := observability.StartSpan(ctx, "controlplane.submit_job")
	defer func() {
		span.End()
		observability.RecordGRPCRequest("unary", "/taskorchestrator.v1.JobService/SubmitJob", started, err)
		observability.RecordJobOperation("submit", err)
	}()

	job, submitErr := s.jobApplication.SubmitJob(ctx, jobs.SubmitJobInput{
		JobType:        request.GetJobType(),
		Payload:        request.GetPayload(),
		Priority:       request.GetPriority(),
		IdempotencyKey: request.GetIdempotencyKey(),
		Metadata:       request.GetMetadata(),
		RetryPolicy: jobs.RetryPolicy{
			MaxAttempts:    request.GetRetryPolicy().GetMaxAttempts(),
			InitialBackoff: request.GetRetryPolicy().GetInitialBackoff().AsDuration(),
			MaxBackoff:     request.GetRetryPolicy().GetMaxBackoff().AsDuration(),
		},
	})
	if submitErr != nil {
		err = toStatusError(submitErr)
		return nil, err
	}

	return &taskorchestratorv1.SubmitJobResponse{Job: toProtoJob(job)}, nil
}

func (s *JobService) CancelJob(ctx context.Context, request *taskorchestratorv1.CancelJobRequest) (_ *taskorchestratorv1.CancelJobResponse, err error) {
	started := time.Now()
	ctx, span := observability.StartSpan(ctx, "controlplane.cancel_job")
	defer func() {
		span.End()
		observability.RecordGRPCRequest("unary", "/taskorchestrator.v1.JobService/CancelJob", started, err)
		observability.RecordJobOperation("cancel", err)
	}()

	job, cancelErr := s.jobApplication.CancelJob(ctx, request.GetJobId())
	if cancelErr != nil {
		err = toStatusError(cancelErr)
		return nil, err
	}

	return &taskorchestratorv1.CancelJobResponse{Job: toProtoJob(job)}, nil
}

func (s *JobService) GetJob(ctx context.Context, request *taskorchestratorv1.GetJobRequest) (_ *taskorchestratorv1.GetJobResponse, err error) {
	started := time.Now()
	ctx, span := observability.StartSpan(ctx, "controlplane.get_job")
	defer func() {
		span.End()
		observability.RecordGRPCRequest("unary", "/taskorchestrator.v1.JobService/GetJob", started, err)
		observability.RecordJobOperation("get", err)
	}()

	job, getErr := s.jobApplication.GetJob(ctx, request.GetJobId())
	if getErr != nil {
		err = toStatusError(getErr)
		return nil, err
	}

	return &taskorchestratorv1.GetJobResponse{Job: toProtoJob(job)}, nil
}

func (s *JobService) ListExecutions(ctx context.Context, request *taskorchestratorv1.ListExecutionsRequest) (_ *taskorchestratorv1.ListExecutionsResponse, err error) {
	started := time.Now()
	ctx, span := observability.StartSpan(ctx, "controlplane.list_executions")
	defer func() {
		span.End()
		observability.RecordGRPCRequest("unary", "/taskorchestrator.v1.JobService/ListExecutions", started, err)
		observability.RecordJobOperation("list_executions", err)
	}()

	executions, listErr := s.jobApplication.ListExecutions(ctx, request.GetJobId())
	if listErr != nil {
		err = toStatusError(listErr)
		return nil, err
	}

	response := &taskorchestratorv1.ListExecutionsResponse{
		Executions: make([]*taskorchestratorv1.Execution, 0, len(executions)),
	}

	for _, execution := range executions {
		response.Executions = append(response.Executions, toProtoExecution(execution))
	}

	return response, nil
}

func toProtoJob(job jobs.Job) *taskorchestratorv1.Job {
	return &taskorchestratorv1.Job{
		JobId:          job.ID,
		JobType:        job.JobType,
		Payload:        append([]byte(nil), job.Payload...),
		Status:         toProtoJobStatus(job.Status),
		Priority:       job.Priority,
		IdempotencyKey: job.IdempotencyKey,
		Metadata:       cloneMetadata(job.Metadata),
		RetryPolicy: &taskorchestratorv1.RetryPolicy{
			MaxAttempts:    job.RetryPolicy.MaxAttempts,
			InitialBackoff: durationpb.New(job.RetryPolicy.InitialBackoff),
			MaxBackoff:     durationpb.New(job.RetryPolicy.MaxBackoff),
		},
		CreatedAt: timestamppb.New(job.CreatedAt),
		UpdatedAt: timestamppb.New(job.UpdatedAt),
	}
}

func toProtoExecution(execution jobs.Execution) *taskorchestratorv1.Execution {
	result := &taskorchestratorv1.Execution{
		ExecutionId:  execution.ID,
		JobId:        execution.JobID,
		Attempt:      execution.Attempt,
		WorkerId:     execution.WorkerID,
		ErrorMessage: execution.ErrorMessage,
	}

	switch execution.Status {
	case jobs.ExecutionStatusQueued:
		result.Status = taskorchestratorv1.ExecutionStatus_EXECUTION_STATUS_QUEUED
	case jobs.ExecutionStatusClaimed:
		result.Status = taskorchestratorv1.ExecutionStatus_EXECUTION_STATUS_CLAIMED
	case jobs.ExecutionStatusRunning:
		result.Status = taskorchestratorv1.ExecutionStatus_EXECUTION_STATUS_RUNNING
	case jobs.ExecutionStatusSucceeded:
		result.Status = taskorchestratorv1.ExecutionStatus_EXECUTION_STATUS_SUCCEEDED
	case jobs.ExecutionStatusFailed:
		result.Status = taskorchestratorv1.ExecutionStatus_EXECUTION_STATUS_FAILED
	case jobs.ExecutionStatusDeadLettered:
		result.Status = taskorchestratorv1.ExecutionStatus_EXECUTION_STATUS_DEAD_LETTERED
	default:
		result.Status = taskorchestratorv1.ExecutionStatus_EXECUTION_STATUS_UNSPECIFIED
	}

	if execution.ClaimedAt != nil {
		result.ClaimedAt = timestamppb.New(*execution.ClaimedAt)
	}
	if execution.StartedAt != nil {
		result.StartedAt = timestamppb.New(*execution.StartedAt)
	}
	if execution.FinishedAt != nil {
		result.FinishedAt = timestamppb.New(*execution.FinishedAt)
	}

	return result
}

func toProtoJobStatus(status jobs.Status) taskorchestratorv1.JobStatus {
	switch status {
	case jobs.StatusPending:
		return taskorchestratorv1.JobStatus_JOB_STATUS_PENDING
	case jobs.StatusDispatching:
		return taskorchestratorv1.JobStatus_JOB_STATUS_DISPATCHING
	case jobs.StatusSucceeded:
		return taskorchestratorv1.JobStatus_JOB_STATUS_SUCCEEDED
	case jobs.StatusFailed:
		return taskorchestratorv1.JobStatus_JOB_STATUS_FAILED
	case jobs.StatusCanceled:
		return taskorchestratorv1.JobStatus_JOB_STATUS_CANCELED
	default:
		return taskorchestratorv1.JobStatus_JOB_STATUS_UNSPECIFIED
	}
}

func cloneMetadata(metadata map[string]string) map[string]string {
	if len(metadata) == 0 {
		return map[string]string{}
	}

	cloned := make(map[string]string, len(metadata))
	for key, value := range metadata {
		cloned[key] = value
	}

	return cloned
}
