package grpcapi

import (
	"context"
	"io"
	"testing"
	"time"

	taskorchestratorv1 "github.com/gnix0/task-orchestrator/gen/go/taskorchestrator/v1"
	"github.com/gnix0/task-orchestrator/internal/application/jobs"
	"github.com/gnix0/task-orchestrator/internal/application/workers"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestConnectRegistersWorkerAndAcceptsHeartbeat(t *testing.T) {
	workerApplication := workers.NewInMemoryService()
	service := &WorkerService{
		workerApplication: workerApplication,
		dispatchService:   &fakeDispatchService{},
		leaseDuration:     30 * time.Second,
	}
	stream := &fakeWorkerConnectStream{
		ctx: context.Background(),
		recvMessages: []*taskorchestratorv1.ConnectRequest{
			{
				Payload: &taskorchestratorv1.ConnectRequest_Registration{
					Registration: &taskorchestratorv1.WorkerRegistration{
						WorkerId:       "worker-1",
						Capabilities:   []string{"email"},
						MaxConcurrency: 4,
					},
				},
			},
			{
				Payload: &taskorchestratorv1.ConnectRequest_Heartbeat{
					Heartbeat: &taskorchestratorv1.WorkerHeartbeat{
						Status:             taskorchestratorv1.WorkerStatus_WORKER_STATUS_BUSY,
						InflightExecutions: 2,
					},
				},
			},
		},
	}

	err := service.Connect(stream)
	if err != nil {
		t.Fatalf("expected connect to succeed, got %v", err)
	}

	if len(stream.sentMessages) != 2 {
		t.Fatalf("expected 2 ack messages, got %d", len(stream.sentMessages))
	}

	firstAck := stream.sentMessages[0].GetAck()
	if firstAck.GetWorkerId() != "worker-1" || firstAck.GetMessage() != "worker registered" {
		t.Fatalf("unexpected first ack: %#v", firstAck)
	}

	secondAck := stream.sentMessages[1].GetAck()
	if secondAck.GetWorkerId() != "worker-1" || secondAck.GetMessage() != "heartbeat accepted" {
		t.Fatalf("unexpected second ack: %#v", secondAck)
	}

	worker, ok := workerApplication.GetWorker("worker-1")
	if !ok {
		t.Fatal("expected worker to be stored in memory")
	}

	if worker.Status != workers.StatusBusy {
		t.Fatalf("expected worker busy status, got %q", worker.Status)
	}
}

func TestConnectRejectsMissingPayload(t *testing.T) {
	service := &WorkerService{
		workerApplication: workers.NewInMemoryService(),
		dispatchService:   &fakeDispatchService{},
		leaseDuration:     30 * time.Second,
	}
	stream := &fakeWorkerConnectStream{
		ctx:          context.Background(),
		recvMessages: []*taskorchestratorv1.ConnectRequest{{}},
	}

	err := service.Connect(stream)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", status.Code(err))
	}
}

func TestConnectAcceptsTaskResult(t *testing.T) {
	dispatcher := &fakeDispatchService{}
	service := &WorkerService{
		workerApplication: workers.NewInMemoryService(),
		dispatchService:   dispatcher,
		leaseDuration:     30 * time.Second,
	}
	stream := &fakeWorkerConnectStream{
		ctx: context.Background(),
		recvMessages: []*taskorchestratorv1.ConnectRequest{
			{
				Payload: &taskorchestratorv1.ConnectRequest_Result{
					Result: &taskorchestratorv1.TaskResult{ExecutionId: "exec-1", Success: true},
				},
			},
		},
	}

	err := service.Connect(stream)
	if err != nil {
		t.Fatalf("expected task result to succeed, got %v", err)
	}
	if dispatcher.completedExecutionID != "exec-1" {
		t.Fatalf("expected completion to be delegated, got %q", dispatcher.completedExecutionID)
	}
}

type fakeDispatchService struct {
	completedExecutionID string
}

func (f *fakeDispatchService) ClaimNextForWorker(context.Context, jobs.ClaimForWorkerInput, time.Duration) (*jobs.Assignment, error) {
	return nil, nil
}

func (f *fakeDispatchService) RenewLeasesForWorker(context.Context, string, time.Duration) error {
	return nil
}

func (f *fakeDispatchService) CompleteExecution(_ context.Context, input jobs.CompleteExecutionInput) (jobs.Execution, jobs.Job, error) {
	f.completedExecutionID = input.ExecutionID
	return jobs.Execution{ID: input.ExecutionID, Status: jobs.ExecutionStatusSucceeded}, jobs.Job{ID: "job-1", Status: jobs.StatusSucceeded}, nil
}

func (f *fakeDispatchService) FailExecution(_ context.Context, input jobs.FailExecutionInput) (jobs.Execution, *jobs.Execution, jobs.Job, error) {
	return jobs.Execution{ID: input.ExecutionID, Status: jobs.ExecutionStatusFailed}, nil, jobs.Job{ID: "job-1", Status: jobs.StatusFailed}, nil
}

func (f *fakeDispatchService) ReleaseExecution(context.Context, string, string) error {
	return nil
}

type fakeWorkerConnectStream struct {
	taskorchestratorv1.WorkerService_ConnectServer
	ctx          context.Context
	recvMessages []*taskorchestratorv1.ConnectRequest
	sentMessages []*taskorchestratorv1.ConnectResponse
	recvIndex    int
}

func (s *fakeWorkerConnectStream) Context() context.Context {
	return s.ctx
}

func (s *fakeWorkerConnectStream) Send(response *taskorchestratorv1.ConnectResponse) error {
	s.sentMessages = append(s.sentMessages, response)
	return nil
}

func (s *fakeWorkerConnectStream) Recv() (*taskorchestratorv1.ConnectRequest, error) {
	if s.recvIndex >= len(s.recvMessages) {
		return nil, io.EOF
	}

	message := s.recvMessages[s.recvIndex]
	s.recvIndex++
	return message, nil
}

func (s *fakeWorkerConnectStream) SetHeader(metadata.MD) error {
	return nil
}

func (s *fakeWorkerConnectStream) SendHeader(metadata.MD) error {
	return nil
}

func (s *fakeWorkerConnectStream) SetTrailer(metadata.MD) {}

func (s *fakeWorkerConnectStream) SendMsg(any) error {
	return nil
}

func (s *fakeWorkerConnectStream) RecvMsg(any) error {
	return nil
}
