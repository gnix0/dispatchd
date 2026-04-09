package grpcapi

import (
	"io"

	taskorchestratorv1 "github.com/gnix0/task-orchestrator/gen/go/taskorchestrator/v1"
	"github.com/gnix0/task-orchestrator/internal/application/workers"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type WorkerService struct {
	workerApplication workers.Service
	taskorchestratorv1.UnimplementedWorkerServiceServer
}

var _ taskorchestratorv1.WorkerServiceServer = (*WorkerService)(nil)

func RegisterWorkerGateway(workerApplication workers.Service) func(*grpc.Server) {
	return func(server *grpc.Server) {
		taskorchestratorv1.RegisterWorkerServiceServer(server, &WorkerService{workerApplication: workerApplication})
	}
}

func (s *WorkerService) Connect(stream taskorchestratorv1.WorkerService_ConnectServer) error {
	var streamWorkerID string

	for {
		request, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		switch payload := request.GetPayload().(type) {
		case *taskorchestratorv1.ConnectRequest_Registration:
			worker, err := s.workerApplication.Register(stream.Context(), workers.RegisterInput{
				WorkerID:       payload.Registration.GetWorkerId(),
				Capabilities:   payload.Registration.GetCapabilities(),
				MaxConcurrency: payload.Registration.GetMaxConcurrency(),
				Labels:         payload.Registration.GetLabels(),
			})
			if err != nil {
				return toStatusError(err)
			}

			streamWorkerID = worker.ID
			if err := stream.Send(newWorkerAck(worker.ID, "worker registered")); err != nil {
				return err
			}
		case *taskorchestratorv1.ConnectRequest_Heartbeat:
			workerID := payload.Heartbeat.GetWorkerId()
			if workerID == "" {
				workerID = streamWorkerID
			}

			worker, err := s.workerApplication.Heartbeat(stream.Context(), workers.HeartbeatInput{
				WorkerID:           workerID,
				Status:             fromProtoWorkerStatus(payload.Heartbeat.GetStatus()),
				InflightExecutions: payload.Heartbeat.GetInflightExecutions(),
			})
			if err != nil {
				return toStatusError(err)
			}

			streamWorkerID = worker.ID
			if err := stream.Send(newWorkerAck(worker.ID, "heartbeat accepted")); err != nil {
				return err
			}
		case *taskorchestratorv1.ConnectRequest_Result:
			return status.Error(codes.Unimplemented, "task results are not implemented yet")
		case *taskorchestratorv1.ConnectRequest_LogChunk:
			return status.Error(codes.Unimplemented, "task log streaming is not implemented yet")
		default:
			return status.Error(codes.InvalidArgument, "connect payload is required")
		}
	}
}

func newWorkerAck(workerID, message string) *taskorchestratorv1.ConnectResponse {
	return &taskorchestratorv1.ConnectResponse{
		Payload: &taskorchestratorv1.ConnectResponse_Ack{
			Ack: &taskorchestratorv1.WorkerAck{
				WorkerId: workerID,
				Message:  message,
			},
		},
	}
}

func fromProtoWorkerStatus(status taskorchestratorv1.WorkerStatus) workers.Status {
	switch status {
	case taskorchestratorv1.WorkerStatus_WORKER_STATUS_READY:
		return workers.StatusReady
	case taskorchestratorv1.WorkerStatus_WORKER_STATUS_BUSY:
		return workers.StatusBusy
	case taskorchestratorv1.WorkerStatus_WORKER_STATUS_DRAINING:
		return workers.StatusDraining
	case taskorchestratorv1.WorkerStatus_WORKER_STATUS_OFFLINE:
		return workers.StatusOffline
	default:
		return workers.StatusUnspecified
	}
}
