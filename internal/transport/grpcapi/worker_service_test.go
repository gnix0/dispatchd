package grpcapi

import (
	"context"
	"io"
	"testing"
	"time"

	dispatchdv1 "github.com/gnix0/dispatchd/gen/go/dispatchd/v1"
	"github.com/gnix0/dispatchd/internal/application/jobs"
	"github.com/gnix0/dispatchd/internal/application/workers"
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
		recvMessages: []*dispatchdv1.ConnectRequest{
			{
				Payload: &dispatchdv1.ConnectRequest_Registration{
					Registration: &dispatchdv1.WorkerRegistration{
						WorkerId:       "worker-1",
						Capabilities:   []string{"email"},
						MaxConcurrency: 4,
					},
				},
			},
			{
				Payload: &dispatchdv1.ConnectRequest_Heartbeat{
					Heartbeat: &dispatchdv1.WorkerHeartbeat{
						Status:             dispatchdv1.WorkerStatus_WORKER_STATUS_BUSY,
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

func TestConnectHeartbeatDefaultsToReadyWhenStatusIsOmitted(t *testing.T) {
	workerApplication := workers.NewInMemoryService()
	service := &WorkerService{
		workerApplication: workerApplication,
		dispatchService:   &fakeDispatchService{},
		leaseDuration:     30 * time.Second,
	}
	stream := &fakeWorkerConnectStream{
		ctx: context.Background(),
		recvMessages: []*dispatchdv1.ConnectRequest{
			{
				Payload: &dispatchdv1.ConnectRequest_Registration{
					Registration: &dispatchdv1.WorkerRegistration{
						WorkerId:       "worker-1",
						Capabilities:   []string{"email"},
						MaxConcurrency: 4,
					},
				},
			},
			{
				Payload: &dispatchdv1.ConnectRequest_Heartbeat{
					Heartbeat: &dispatchdv1.WorkerHeartbeat{
						InflightExecutions: 0,
					},
				},
			},
		},
	}

	err := service.Connect(stream)
	if err != nil {
		t.Fatalf("expected connect to succeed, got %v", err)
	}

	worker, ok := workerApplication.GetWorker("worker-1")
	if !ok {
		t.Fatal("expected worker to be stored in memory")
	}

	if worker.Status != workers.StatusReady {
		t.Fatalf("expected omitted heartbeat status to default to ready, got %q", worker.Status)
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
		recvMessages: []*dispatchdv1.ConnectRequest{{}},
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
		recvMessages: []*dispatchdv1.ConnectRequest{
			{
				Payload: &dispatchdv1.ConnectRequest_Result{
					Result: &dispatchdv1.TaskResult{ExecutionId: "exec-1", Success: true},
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

func TestConnectRefillsOneAssignmentAfterTaskResult(t *testing.T) {
	dispatcher := &fakeDispatchService{
		assignments: []*jobs.Assignment{
			{
				ExecutionID: "exec-1",
				JobType:     "email.send",
				Payload:     []byte("payload-1"),
				Attempt:     1,
			},
			{
				ExecutionID: "exec-2",
				JobType:     "email.send",
				Payload:     []byte("payload-2"),
				Attempt:     1,
			},
		},
	}
	service := &WorkerService{
		workerApplication: workers.NewInMemoryService(),
		dispatchService:   dispatcher,
		leaseDuration:     30 * time.Second,
	}
	stream := &fakeWorkerConnectStream{
		ctx: context.Background(),
		recvMessages: []*dispatchdv1.ConnectRequest{
			{
				Payload: &dispatchdv1.ConnectRequest_Registration{
					Registration: &dispatchdv1.WorkerRegistration{
						WorkerId:       "worker-1",
						Capabilities:   []string{"email"},
						MaxConcurrency: 1,
					},
				},
			},
			{
				Payload: &dispatchdv1.ConnectRequest_Result{
					Result: &dispatchdv1.TaskResult{ExecutionId: "exec-1", Success: true},
				},
			},
		},
	}

	err := service.Connect(stream)
	if err != nil {
		t.Fatalf("expected stream to succeed, got %v", err)
	}

	if dispatcher.claimCalls != 2 {
		t.Fatalf("expected exactly 2 assignment claims, got %d", dispatcher.claimCalls)
	}
	if dispatcher.completedExecutionID != "exec-1" {
		t.Fatalf("expected completed execution exec-1, got %q", dispatcher.completedExecutionID)
	}
	if len(stream.sentMessages) != 4 {
		t.Fatalf("expected ack, assignment, result ack, refill assignment; got %d messages", len(stream.sentMessages))
	}
	if stream.sentMessages[1].GetAssignment().GetExecutionId() != "exec-1" {
		t.Fatalf("expected first assignment exec-1, got %#v", stream.sentMessages[1])
	}
	if stream.sentMessages[3].GetAssignment().GetExecutionId() != "exec-2" {
		t.Fatalf("expected refill assignment exec-2, got %#v", stream.sentMessages[3])
	}
}

func TestConnectReleasesAssignmentWhenDeliveryFails(t *testing.T) {
	dispatcher := &fakeDispatchService{
		assignment: &jobs.Assignment{
			ExecutionID: "exec-assign-1",
			JobType:     "email.send",
			Payload:     []byte("payload"),
			Attempt:     1,
		},
	}
	service := &WorkerService{
		workerApplication: workers.NewInMemoryService(),
		dispatchService:   dispatcher,
		leaseDuration:     30 * time.Second,
	}
	stream := &fakeWorkerConnectStream{
		ctx: context.Background(),
		recvMessages: []*dispatchdv1.ConnectRequest{
			{
				Payload: &dispatchdv1.ConnectRequest_Registration{
					Registration: &dispatchdv1.WorkerRegistration{
						WorkerId:       "worker-1",
						Capabilities:   []string{"email"},
						MaxConcurrency: 1,
					},
				},
			},
		},
		failSendAt: 2,
	}

	err := service.Connect(stream)
	if err == nil {
		t.Fatal("expected assignment delivery to fail")
	}
	if dispatcher.releasedExecutionID != "exec-assign-1" {
		t.Fatalf("expected released execution exec-assign-1, got %q", dispatcher.releasedExecutionID)
	}
}

type fakeDispatchService struct {
	completedExecutionID string
	releasedExecutionID  string
	assignment           *jobs.Assignment
	assignments          []*jobs.Assignment
	claimCalls           int
}

func (f *fakeDispatchService) ClaimNextForWorker(context.Context, jobs.ClaimForWorkerInput, time.Duration) (*jobs.Assignment, error) {
	f.claimCalls++
	if len(f.assignments) > 0 {
		assignment := f.assignments[0]
		f.assignments = f.assignments[1:]
		return assignment, nil
	}
	return f.assignment, nil
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

func (f *fakeDispatchService) ReleaseExecution(_ context.Context, executionID, _ string) error {
	f.releasedExecutionID = executionID
	return nil
}

type fakeWorkerConnectStream struct {
	dispatchdv1.WorkerService_ConnectServer
	ctx          context.Context
	recvMessages []*dispatchdv1.ConnectRequest
	sentMessages []*dispatchdv1.ConnectResponse
	recvIndex    int
	failSendAt   int
}

func (s *fakeWorkerConnectStream) Context() context.Context {
	return s.ctx
}

func (s *fakeWorkerConnectStream) Send(response *dispatchdv1.ConnectResponse) error {
	if s.failSendAt > 0 && len(s.sentMessages)+1 == s.failSendAt {
		return io.ErrClosedPipe
	}
	s.sentMessages = append(s.sentMessages, response)
	return nil
}

func (s *fakeWorkerConnectStream) Recv() (*dispatchdv1.ConnectRequest, error) {
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
