package workers

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRegisterNormalizesWorkerAndStoresIt(t *testing.T) {
	service := NewInMemoryService()
	service.now = func() time.Time { return time.Date(2026, 4, 9, 19, 0, 0, 0, time.UTC) }

	worker, err := service.Register(context.Background(), RegisterInput{
		WorkerID:       " worker-1 ",
		Capabilities:   []string{"email", " email ", "report"},
		MaxConcurrency: 0,
		Labels:         map[string]string{"zone": "sa-east-1"},
	})
	if err != nil {
		t.Fatalf("expected register to succeed, got %v", err)
	}

	if worker.ID != "worker-1" {
		t.Fatalf("expected trimmed worker id, got %q", worker.ID)
	}

	if worker.MaxConcurrency != 1 {
		t.Fatalf("expected default max concurrency 1, got %d", worker.MaxConcurrency)
	}

	if len(worker.Capabilities) != 2 || worker.Capabilities[0] != "email" || worker.Capabilities[1] != "report" {
		t.Fatalf("expected normalized capabilities, got %#v", worker.Capabilities)
	}

	if worker.Status != StatusReady {
		t.Fatalf("expected ready status, got %q", worker.Status)
	}
}

func TestHeartbeatUpdatesExistingWorker(t *testing.T) {
	service := NewInMemoryService()
	service.now = func() time.Time { return time.Date(2026, 4, 9, 19, 0, 0, 0, time.UTC) }

	_, err := service.Register(context.Background(), RegisterInput{WorkerID: "worker-1"})
	if err != nil {
		t.Fatalf("expected register to succeed, got %v", err)
	}

	service.now = func() time.Time { return time.Date(2026, 4, 9, 19, 1, 0, 0, time.UTC) }
	worker, err := service.Heartbeat(context.Background(), HeartbeatInput{
		WorkerID:           "worker-1",
		Status:             StatusBusy,
		InflightExecutions: 2,
	})
	if err != nil {
		t.Fatalf("expected heartbeat to succeed, got %v", err)
	}

	if worker.Status != StatusBusy {
		t.Fatalf("expected busy status, got %q", worker.Status)
	}

	if worker.InflightExecutions != 2 {
		t.Fatalf("expected inflight executions 2, got %d", worker.InflightExecutions)
	}

	if worker.LastHeartbeatAt.IsZero() {
		t.Fatal("expected last heartbeat to be recorded")
	}
}

func TestHeartbeatRejectsUnknownWorker(t *testing.T) {
	service := NewInMemoryService()

	_, err := service.Heartbeat(context.Background(), HeartbeatInput{WorkerID: "missing"})
	if !errors.Is(err, ErrWorkerNotFound) {
		t.Fatalf("expected worker not found, got %v", err)
	}
}
