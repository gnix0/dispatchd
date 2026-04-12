package scheduler

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestTickClaimsRunnableExecution(t *testing.T) {
	store := &fakeStore{
		leader:   true,
		requeued: 1,
		enqueued: 2,
	}
	service := NewService(slog.New(slog.NewTextHandler(io.Discard, nil)), store, "scheduler-a", "region-a", "region-a", true, time.Second, 10*time.Second, 45*time.Second)

	claimed, err := service.Tick(context.Background())
	if err != nil {
		t.Fatalf("expected tick to succeed, got %v", err)
	}
	if !claimed {
		t.Fatal("expected tick to reconcile work")
	}
	if store.leaderInstanceID != "scheduler-a" {
		t.Fatalf("expected leader instance id scheduler-a, got %q", store.leaderInstanceID)
	}
	if store.leaderTTL != 10*time.Second {
		t.Fatalf("expected leader ttl 10s, got %s", store.leaderTTL)
	}
}

func TestTickReturnsFalseForStandbyInstance(t *testing.T) {
	store := &fakeStore{leader: false}
	service := NewService(slog.New(slog.NewTextHandler(io.Discard, nil)), store, "scheduler-b", "region-a", "region-a", true, time.Second, 10*time.Second, 45*time.Second)

	claimed, err := service.Tick(context.Background())
	if err != nil {
		t.Fatalf("expected tick to succeed, got %v", err)
	}
	if claimed {
		t.Fatal("expected standby instance to skip reconciliation")
	}
}

func TestTickReturnsFalseForPassiveRegion(t *testing.T) {
	store := &fakeStore{leader: true}
	service := NewService(slog.New(slog.NewTextHandler(io.Discard, nil)), store, "scheduler-b", "region-b", "region-a", false, time.Second, 10*time.Second, 45*time.Second)

	claimed, err := service.Tick(context.Background())
	if err != nil {
		t.Fatalf("expected passive region tick to succeed, got %v", err)
	}
	if claimed {
		t.Fatal("expected passive region to skip reconciliation")
	}
	if store.leaderInstanceID != "" {
		t.Fatalf("expected passive region to skip leadership acquisition, got %q", store.leaderInstanceID)
	}
}

type fakeStore struct {
	leader           bool
	requeued         int
	enqueued         int
	leaderInstanceID string
	leaderTTL        time.Duration
}

func (s *fakeStore) TryAcquireLeadership(_ context.Context, instanceID string, ttl time.Duration) (bool, error) {
	s.leaderInstanceID = instanceID
	s.leaderTTL = ttl
	return s.leader, nil
}

func (s *fakeStore) RequeueExpiredExecutions(context.Context, time.Duration, int) (int, error) {
	return s.requeued, nil
}

func (s *fakeStore) EnqueueRunnableExecutions(context.Context, int) (int, error) {
	return s.enqueued, nil
}
