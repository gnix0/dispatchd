package grpcapi

import (
	taskorchestratorv1 "github.com/gnix0/task-orchestrator/gen/go/taskorchestrator/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type WorkerService struct {
	taskorchestratorv1.UnimplementedWorkerServiceServer
}

var _ taskorchestratorv1.WorkerServiceServer = (*WorkerService)(nil)

func RegisterWorkerGateway(server *grpc.Server) {
	taskorchestratorv1.RegisterWorkerServiceServer(server, &WorkerService{})
}

func (s *WorkerService) Connect(taskorchestratorv1.WorkerService_ConnectServer) error {
	return status.Error(codes.Unimplemented, "Connect not implemented")
}
