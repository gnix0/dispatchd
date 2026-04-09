package grpcapi

import (
	"context"

	taskorchestratorv1 "github.com/gnix0/task-orchestrator/gen/go/taskorchestrator/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type JobService struct {
	taskorchestratorv1.UnimplementedJobServiceServer
}

var _ taskorchestratorv1.JobServiceServer = (*JobService)(nil)

func RegisterControlPlane(server *grpc.Server) {
	taskorchestratorv1.RegisterJobServiceServer(server, &JobService{})
}

func (s *JobService) SubmitJob(context.Context, *taskorchestratorv1.SubmitJobRequest) (*taskorchestratorv1.SubmitJobResponse, error) {
	return nil, status.Error(codes.Unimplemented, "SubmitJob not implemented")
}

func (s *JobService) CancelJob(context.Context, *taskorchestratorv1.CancelJobRequest) (*taskorchestratorv1.CancelJobResponse, error) {
	return nil, status.Error(codes.Unimplemented, "CancelJob not implemented")
}

func (s *JobService) GetJob(context.Context, *taskorchestratorv1.GetJobRequest) (*taskorchestratorv1.GetJobResponse, error) {
	return nil, status.Error(codes.Unimplemented, "GetJob not implemented")
}

func (s *JobService) ListExecutions(context.Context, *taskorchestratorv1.ListExecutionsRequest) (*taskorchestratorv1.ListExecutionsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ListExecutions not implemented")
}
