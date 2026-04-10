package grpcapi

import (
	"io"
	"time"

	taskorchestratorv1 "github.com/gnix0/task-orchestrator/gen/go/taskorchestrator/v1"
	"github.com/gnix0/task-orchestrator/internal/application/jobs"
	"github.com/gnix0/task-orchestrator/internal/application/workers"
	"github.com/gnix0/task-orchestrator/internal/platform/security"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type WorkerService struct {
	workerApplication workers.Service
	dispatchService   jobs.DispatchService
	leaseDuration     time.Duration
	taskorchestratorv1.UnimplementedWorkerServiceServer
}

var _ taskorchestratorv1.WorkerServiceServer = (*WorkerService)(nil)

func RegisterWorkerGateway(workerApplication workers.Service, dispatchService jobs.DispatchService, leaseDuration time.Duration) func(*grpc.Server) {
	return func(server *grpc.Server) {
		taskorchestratorv1.RegisterWorkerServiceServer(server, &WorkerService{
			workerApplication: workerApplication,
			dispatchService:   dispatchService,
			leaseDuration:     leaseDuration,
		})
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
			if err := security.ValidateWorkerIdentity(stream.Context(), payload.Registration.GetWorkerId()); err != nil {
				return err
			}
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
			if err := s.sendAssignments(stream, worker); err != nil {
				return err
			}
		case *taskorchestratorv1.ConnectRequest_Heartbeat:
			workerID := payload.Heartbeat.GetWorkerId()
			if workerID == "" {
				workerID = streamWorkerID
			}
			if err := security.ValidateWorkerIdentity(stream.Context(), workerID); err != nil {
				return err
			}

			worker, err := s.workerApplication.Heartbeat(stream.Context(), workers.HeartbeatInput{
				WorkerID:           workerID,
				Status:             fromProtoWorkerStatus(payload.Heartbeat.GetStatus()),
				InflightExecutions: payload.Heartbeat.GetInflightExecutions(),
			})
			if err != nil {
				return toStatusError(err)
			}
			if err := s.dispatchService.RenewLeasesForWorker(stream.Context(), worker.ID, s.leaseDuration); err != nil {
				return toStatusError(err)
			}

			streamWorkerID = worker.ID
			if err := stream.Send(newWorkerAck(worker.ID, "heartbeat accepted")); err != nil {
				return err
			}
			if err := s.sendAssignments(stream, worker); err != nil {
				return err
			}
		case *taskorchestratorv1.ConnectRequest_Result:
			workerID := streamWorkerID
			if workerID == "" {
				workerID = payload.Result.GetMetadata()["worker_id"]
			}
			if err := security.ValidateWorkerIdentity(stream.Context(), workerID); err != nil {
				return err
			}

			if payload.Result.GetSuccess() {
				if _, _, err := s.dispatchService.CompleteExecution(stream.Context(), jobs.CompleteExecutionInput{
					ExecutionID: payload.Result.GetExecutionId(),
					WorkerID:    workerID,
					Metadata:    payload.Result.GetMetadata(),
				}); err != nil {
					return toStatusError(err)
				}
			} else {
				if _, _, _, err := s.dispatchService.FailExecution(stream.Context(), jobs.FailExecutionInput{
					ExecutionID:  payload.Result.GetExecutionId(),
					WorkerID:     workerID,
					ErrorMessage: payload.Result.GetErrorMessage(),
					Metadata:     payload.Result.GetMetadata(),
				}); err != nil {
					return toStatusError(err)
				}
			}
			if err := stream.Send(newExecutionAck(payload.Result.GetExecutionId(), workerID, "result accepted")); err != nil {
				return err
			}
		case *taskorchestratorv1.ConnectRequest_LogChunk:
			if err := security.ValidateWorkerIdentity(stream.Context(), streamWorkerID); err != nil {
				return err
			}
			if err := stream.Send(newExecutionAck(payload.LogChunk.GetExecutionId(), streamWorkerID, "log accepted")); err != nil {
				return err
			}
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

func newExecutionAck(executionID, workerID, message string) *taskorchestratorv1.ConnectResponse {
	return &taskorchestratorv1.ConnectResponse{
		Payload: &taskorchestratorv1.ConnectResponse_Ack{
			Ack: &taskorchestratorv1.WorkerAck{
				ExecutionId: executionID,
				WorkerId:    workerID,
				Message:     message,
			},
		},
	}
}

func (s *WorkerService) sendAssignments(stream taskorchestratorv1.WorkerService_ConnectServer, worker workers.Worker) error {
	availableSlots := int(worker.MaxConcurrency - worker.InflightExecutions)
	if availableSlots <= 0 || worker.Status == workers.StatusDraining || worker.Status == workers.StatusOffline {
		return nil
	}

	for i := 0; i < availableSlots; i++ {
		assignment, err := s.dispatchService.ClaimNextForWorker(stream.Context(), jobs.ClaimForWorkerInput{
			WorkerID:           worker.ID,
			Capabilities:       worker.Capabilities,
			MaxConcurrency:     worker.MaxConcurrency,
			InflightExecutions: worker.InflightExecutions + int32(i),
		}, s.leaseDuration)
		if err != nil {
			return toStatusError(err)
		}
		if assignment == nil {
			return nil
		}

		if err := stream.Send(toAssignmentResponse(assignment)); err != nil {
			releaseErr := s.dispatchService.ReleaseExecution(stream.Context(), assignment.ExecutionID, "assignment delivery failed")
			if releaseErr != nil {
				return releaseErr
			}
			return err
		}
	}

	return nil
}

func toAssignmentResponse(assignment *jobs.Assignment) *taskorchestratorv1.ConnectResponse {
	return &taskorchestratorv1.ConnectResponse{
		Payload: &taskorchestratorv1.ConnectResponse_Assignment{
			Assignment: &taskorchestratorv1.TaskAssignment{
				ExecutionId:    assignment.ExecutionID,
				JobType:        assignment.JobType,
				Payload:        append([]byte(nil), assignment.Payload...),
				Attempt:        assignment.Attempt,
				LeaseExpiresAt: timestamppb.New(assignment.LeaseExpiresAt),
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
