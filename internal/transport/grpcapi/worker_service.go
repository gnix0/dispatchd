package grpcapi

import (
	"io"
	"time"

	dispatchdv1 "github.com/gnix0/dispatchd/gen/go/dispatchd/v1"
	"github.com/gnix0/dispatchd/internal/application/jobs"
	"github.com/gnix0/dispatchd/internal/application/workers"
	"github.com/gnix0/dispatchd/internal/platform/observability"
	"github.com/gnix0/dispatchd/internal/platform/security"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type WorkerService struct {
	workerApplication workers.Service
	dispatchService   jobs.DispatchService
	leaseDuration     time.Duration
	dispatchdv1.UnimplementedWorkerServiceServer
}

var _ dispatchdv1.WorkerServiceServer = (*WorkerService)(nil)

func RegisterWorkerGateway(workerApplication workers.Service, dispatchService jobs.DispatchService, leaseDuration time.Duration) func(*grpc.Server) {
	return func(server *grpc.Server) {
		dispatchdv1.RegisterWorkerServiceServer(server, &WorkerService{
			workerApplication: workerApplication,
			dispatchService:   dispatchService,
			leaseDuration:     leaseDuration,
		})
	}
}

func (s *WorkerService) Connect(stream dispatchdv1.WorkerService_ConnectServer) (err error) {
	var streamWorkerID string
	var streamWorker workers.Worker
	started := time.Now()
	ctx, span := observability.StartSpan(stream.Context(), "worker_gateway.connect")
	defer func() {
		span.End()
		observability.RecordGRPCRequest("stream", "/dispatchd.v1.WorkerService/Connect", started, err)
	}()

	for {
		request, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		switch payload := request.GetPayload().(type) {
		case *dispatchdv1.ConnectRequest_Registration:
			spanCtx, eventSpan := observability.StartSpan(ctx, "worker_gateway.registration",
				attribute.String("worker.id", payload.Registration.GetWorkerId()),
			)
			eventErr := func() error {
				if err := security.ValidateWorkerIdentity(spanCtx, payload.Registration.GetWorkerId()); err != nil {
					return err
				}
				worker, err := s.workerApplication.Register(spanCtx, workers.RegisterInput{
					WorkerID:       payload.Registration.GetWorkerId(),
					Capabilities:   payload.Registration.GetCapabilities(),
					MaxConcurrency: payload.Registration.GetMaxConcurrency(),
					Labels:         payload.Registration.GetLabels(),
				})
				if err != nil {
					return toStatusError(err)
				}

				streamWorkerID = worker.ID
				streamWorker = worker
				if err := stream.Send(newWorkerAck(worker.ID, "worker registered")); err != nil {
					return err
				}
				return s.sendAssignments(stream, worker)
			}()
			eventSpan.End()
			observability.RecordWorkerEvent("registration", eventErr)
			if eventErr != nil {
				return eventErr
			}
		case *dispatchdv1.ConnectRequest_Heartbeat:
			workerID := payload.Heartbeat.GetWorkerId()
			if workerID == "" {
				workerID = streamWorkerID
			}
			spanCtx, eventSpan := observability.StartSpan(ctx, "worker_gateway.heartbeat",
				attribute.String("worker.id", workerID),
			)
			eventErr := func() error {
				if err := security.ValidateWorkerIdentity(spanCtx, workerID); err != nil {
					return err
				}

				worker, err := s.workerApplication.Heartbeat(spanCtx, workers.HeartbeatInput{
					WorkerID:           workerID,
					Status:             fromProtoWorkerStatus(payload.Heartbeat.GetStatus()),
					InflightExecutions: payload.Heartbeat.GetInflightExecutions(),
				})
				if err != nil {
					return toStatusError(err)
				}
				if err := s.dispatchService.RenewLeasesForWorker(spanCtx, worker.ID, s.leaseDuration); err != nil {
					observability.RecordDispatchEvent("lease_renewal", err)
					return toStatusError(err)
				}
				observability.RecordDispatchEvent("lease_renewal", nil)

				streamWorkerID = worker.ID
				streamWorker = worker
				if err := stream.Send(newWorkerAck(worker.ID, "heartbeat accepted")); err != nil {
					return err
				}
				return s.sendAssignments(stream, worker)
			}()
			eventSpan.End()
			observability.RecordWorkerEvent("heartbeat", eventErr)
			if eventErr != nil {
				return eventErr
			}
		case *dispatchdv1.ConnectRequest_Result:
			workerID := streamWorkerID
			if workerID == "" {
				workerID = payload.Result.GetMetadata()["worker_id"]
			}
			spanCtx, eventSpan := observability.StartSpan(ctx, "worker_gateway.result",
				attribute.String("worker.id", workerID),
				attribute.String("execution.id", payload.Result.GetExecutionId()),
			)
			eventErr := func() error {
				if err := security.ValidateWorkerIdentity(spanCtx, workerID); err != nil {
					return err
				}

				if payload.Result.GetSuccess() {
					if _, _, err := s.dispatchService.CompleteExecution(spanCtx, jobs.CompleteExecutionInput{
						ExecutionID: payload.Result.GetExecutionId(),
						WorkerID:    workerID,
						Metadata:    payload.Result.GetMetadata(),
					}); err != nil {
						observability.RecordDispatchEvent("complete_execution", err)
						return toStatusError(err)
					}
					observability.RecordDispatchEvent("complete_execution", nil)
				} else {
					if _, _, _, err := s.dispatchService.FailExecution(spanCtx, jobs.FailExecutionInput{
						ExecutionID:  payload.Result.GetExecutionId(),
						WorkerID:     workerID,
						ErrorMessage: payload.Result.GetErrorMessage(),
						Metadata:     payload.Result.GetMetadata(),
					}); err != nil {
						observability.RecordDispatchEvent("fail_execution", err)
						return toStatusError(err)
					}
					observability.RecordDispatchEvent("fail_execution", nil)
				}
				if err := stream.Send(newExecutionAck(payload.Result.GetExecutionId(), workerID, "result accepted")); err != nil {
					return err
				}
				if streamWorker.ID == "" {
					return nil
				}
				return s.sendAssignmentSlots(stream, streamWorker, 1)
			}()
			eventSpan.End()
			observability.RecordWorkerEvent("result", eventErr)
			if eventErr != nil {
				return eventErr
			}
		case *dispatchdv1.ConnectRequest_LogChunk:
			spanCtx, eventSpan := observability.StartSpan(ctx, "worker_gateway.log_chunk",
				attribute.String("worker.id", streamWorkerID),
				attribute.String("execution.id", payload.LogChunk.GetExecutionId()),
			)
			eventErr := func() error {
				if err := security.ValidateWorkerIdentity(spanCtx, streamWorkerID); err != nil {
					return err
				}
				return stream.Send(newExecutionAck(payload.LogChunk.GetExecutionId(), streamWorkerID, "log accepted"))
			}()
			eventSpan.End()
			observability.RecordWorkerEvent("log_chunk", eventErr)
			if eventErr != nil {
				return eventErr
			}
		default:
			err := status.Error(codes.InvalidArgument, "connect payload is required")
			observability.RecordWorkerEvent("unknown_payload", err)
			return err
		}
	}
}

func newWorkerAck(workerID, message string) *dispatchdv1.ConnectResponse {
	return &dispatchdv1.ConnectResponse{
		Payload: &dispatchdv1.ConnectResponse_Ack{
			Ack: &dispatchdv1.WorkerAck{
				WorkerId: workerID,
				Message:  message,
			},
		},
	}
}

func newExecutionAck(executionID, workerID, message string) *dispatchdv1.ConnectResponse {
	return &dispatchdv1.ConnectResponse{
		Payload: &dispatchdv1.ConnectResponse_Ack{
			Ack: &dispatchdv1.WorkerAck{
				ExecutionId: executionID,
				WorkerId:    workerID,
				Message:     message,
			},
		},
	}
}

func (s *WorkerService) sendAssignments(stream dispatchdv1.WorkerService_ConnectServer, worker workers.Worker) error {
	availableSlots := int(worker.MaxConcurrency - worker.InflightExecutions)
	return s.sendAssignmentSlots(stream, worker, availableSlots)
}

func (s *WorkerService) sendAssignmentSlots(stream dispatchdv1.WorkerService_ConnectServer, worker workers.Worker, availableSlots int) error {
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
			observability.RecordDispatchEvent("claim_assignment", err)
			return toStatusError(err)
		}
		if assignment == nil {
			return nil
		}
		observability.RecordDispatchEvent("claim_assignment", nil)

		if err := stream.Send(toAssignmentResponse(assignment)); err != nil {
			observability.RecordDispatchEvent("assignment_delivery", err)
			releaseErr := s.dispatchService.ReleaseExecution(stream.Context(), assignment.ExecutionID, "assignment delivery failed")
			if releaseErr != nil {
				observability.RecordDispatchEvent("release_execution", releaseErr)
				return releaseErr
			}
			return err
		}
		observability.RecordDispatchEvent("assignment_delivery", nil)
	}

	return nil
}

func toAssignmentResponse(assignment *jobs.Assignment) *dispatchdv1.ConnectResponse {
	return &dispatchdv1.ConnectResponse{
		Payload: &dispatchdv1.ConnectResponse_Assignment{
			Assignment: &dispatchdv1.TaskAssignment{
				ExecutionId:    assignment.ExecutionID,
				JobType:        assignment.JobType,
				Payload:        append([]byte(nil), assignment.Payload...),
				Attempt:        assignment.Attempt,
				LeaseExpiresAt: timestamppb.New(assignment.LeaseExpiresAt),
			},
		},
	}
}

func fromProtoWorkerStatus(status dispatchdv1.WorkerStatus) workers.Status {
	switch status {
	case dispatchdv1.WorkerStatus_WORKER_STATUS_READY:
		return workers.StatusReady
	case dispatchdv1.WorkerStatus_WORKER_STATUS_BUSY:
		return workers.StatusBusy
	case dispatchdv1.WorkerStatus_WORKER_STATUS_DRAINING:
		return workers.StatusDraining
	case dispatchdv1.WorkerStatus_WORKER_STATUS_OFFLINE:
		return workers.StatusOffline
	default:
		return workers.StatusReady
	}
}
