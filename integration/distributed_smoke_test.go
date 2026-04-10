package integration

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	dispatchdv1 "github.com/gnix0/dispatchd/gen/go/dispatchd/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

func TestDistributedSubmitAssignCompleteFlow(t *testing.T) {
	if os.Getenv("DISPATCHD_INTEGRATION") != "1" {
		t.Skip("set DISPATCHD_INTEGRATION=1 to run integration smoke tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	controlConn, err := grpc.NewClient(
		"127.0.0.1:8080",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial control-plane: %v", err)
	}
	defer func() {
		if err := controlConn.Close(); err != nil {
			t.Fatalf("close control-plane connection: %v", err)
		}
	}()
	waitForReady(t, ctx, controlConn, "control-plane")

	workerConn, err := grpc.NewClient(
		"127.0.0.1:8081",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial worker-gateway: %v", err)
	}
	defer func() {
		if err := workerConn.Close(); err != nil {
			t.Fatalf("close worker-gateway connection: %v", err)
		}
	}()
	waitForReady(t, ctx, workerConn, "worker-gateway")

	jobClient := dispatchdv1.NewJobServiceClient(controlConn)
	workerClient := dispatchdv1.NewWorkerServiceClient(workerConn)

	submitResp, err := jobClient.SubmitJob(ctx, &dispatchdv1.SubmitJobRequest{
		JobType:        "email.send",
		Payload:        []byte(`{"user_id":"integration-smoke"}`),
		IdempotencyKey: "integration-smoke-" + time.Now().UTC().Format("20060102150405.000000000"),
		Priority:       5,
	})
	if err != nil {
		t.Fatalf("submit job: %v", err)
	}

	stream, err := workerClient.Connect(ctx)
	if err != nil {
		t.Fatalf("open worker stream: %v", err)
	}

	messages := make(chan *dispatchdv1.ConnectResponse, 16)
	streamErr := make(chan error, 1)
	go func() {
		for {
			response, err := stream.Recv()
			if err != nil {
				streamErr <- err
				close(messages)
				return
			}
			messages <- response
		}
	}()

	if err := stream.Send(&dispatchdv1.ConnectRequest{
		Payload: &dispatchdv1.ConnectRequest_Registration{
			Registration: &dispatchdv1.WorkerRegistration{
				WorkerId:       "integration-worker-1",
				Capabilities:   []string{"email"},
				MaxConcurrency: 1,
			},
		},
	}); err != nil {
		t.Fatalf("send registration: %v", err)
	}

	if _, err := waitForAck(messages, streamErr, 2*time.Second); err != nil {
		t.Fatalf("registration ack: %v", err)
	}

	var assignment *dispatchdv1.TaskAssignment
	for i := 0; i < 8 && assignment == nil; i++ {
		if err := stream.Send(&dispatchdv1.ConnectRequest{
			Payload: &dispatchdv1.ConnectRequest_Heartbeat{
				Heartbeat: &dispatchdv1.WorkerHeartbeat{
					WorkerId:           "integration-worker-1",
					Status:             dispatchdv1.WorkerStatus_WORKER_STATUS_READY,
					InflightExecutions: 0,
				},
			},
		}); err != nil {
			t.Fatalf("send heartbeat: %v", err)
		}

		gotAssignment, err := waitForAssignment(messages, streamErr, 3*time.Second)
		if err != nil {
			t.Fatalf("wait for assignment: %v", err)
		}
		assignment = gotAssignment
	}

	if assignment == nil {
		t.Fatal("expected assignment to be delivered to connected worker")
	}

	if err := stream.Send(&dispatchdv1.ConnectRequest{
		Payload: &dispatchdv1.ConnectRequest_Result{
			Result: &dispatchdv1.TaskResult{
				ExecutionId: assignment.GetExecutionId(),
				Success:     true,
				Metadata: map[string]string{
					"source": "integration",
				},
			},
		},
	}); err != nil {
		t.Fatalf("send result: %v", err)
	}

	if _, err := waitForAck(messages, streamErr, 2*time.Second); err != nil {
		t.Fatalf("result ack: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		getResp, err := jobClient.GetJob(ctx, &dispatchdv1.GetJobRequest{JobId: submitResp.GetJob().GetJobId()})
		if err != nil {
			t.Fatalf("get job: %v", err)
		}
		if getResp.GetJob().GetStatus() == dispatchdv1.JobStatus_JOB_STATUS_SUCCEEDED {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}

	t.Fatalf("job %s did not reach succeeded state before deadline", submitResp.GetJob().GetJobId())
}

func waitForAck(messages <-chan *dispatchdv1.ConnectResponse, streamErr <-chan error, timeout time.Duration) (*dispatchdv1.WorkerAck, error) {
	deadline := time.After(timeout)
	for {
		select {
		case err := <-streamErr:
			if err == nil || err == io.EOF {
				return nil, io.EOF
			}
			return nil, err
		case response := <-messages:
			if response == nil {
				continue
			}
			if ack := response.GetAck(); ack != nil {
				return ack, nil
			}
		case <-deadline:
			return nil, context.DeadlineExceeded
		}
	}
}

func waitForAssignment(messages <-chan *dispatchdv1.ConnectResponse, streamErr <-chan error, timeout time.Duration) (*dispatchdv1.TaskAssignment, error) {
	deadline := time.After(timeout)
	for {
		select {
		case err := <-streamErr:
			if err == nil || err == io.EOF {
				return nil, io.EOF
			}
			return nil, err
		case response := <-messages:
			if response == nil {
				continue
			}
			if assignment := response.GetAssignment(); assignment != nil {
				return assignment, nil
			}
		case <-deadline:
			return nil, nil
		}
	}
}

func waitForReady(t *testing.T, ctx context.Context, conn *grpc.ClientConn, service string) {
	t.Helper()

	conn.Connect()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return
		}
		if !conn.WaitForStateChange(ctx, state) {
			t.Fatalf("%s connection did not become ready before deadline", service)
		}
	}
}
