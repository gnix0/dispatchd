package workers

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	ErrInvalidArgument = errors.New("invalid argument")
	ErrWorkerNotFound  = errors.New("worker not found")
)

type Status string

const (
	StatusUnspecified Status = "unspecified"
	StatusReady       Status = "ready"
	StatusBusy        Status = "busy"
	StatusDraining    Status = "draining"
	StatusOffline     Status = "offline"
)

type Worker struct {
	ID                 string
	Capabilities       []string
	MaxConcurrency     int32
	Labels             map[string]string
	Status             Status
	InflightExecutions int32
	RegisteredAt       time.Time
	LastHeartbeatAt    time.Time
	UpdatedAt          time.Time
}

type RegisterInput struct {
	WorkerID       string
	Capabilities   []string
	MaxConcurrency int32
	Labels         map[string]string
}

type HeartbeatInput struct {
	WorkerID           string
	Status             Status
	InflightExecutions int32
}

type Service interface {
	Register(context.Context, RegisterInput) (Worker, error)
	Heartbeat(context.Context, HeartbeatInput) (Worker, error)
}

type InMemoryService struct {
	mu          sync.RWMutex
	workersByID map[string]Worker
	now         func() time.Time
}

func NewInMemoryService() *InMemoryService {
	return &InMemoryService{
		workersByID: make(map[string]Worker),
		now:         time.Now,
	}
}

func (s *InMemoryService) Register(_ context.Context, input RegisterInput) (Worker, error) {
	normalized, err := normalizeRegisterInput(input)
	if err != nil {
		return Worker{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now().UTC()

	existing, exists := s.workersByID[normalized.WorkerID]
	registeredAt := now
	if exists {
		registeredAt = existing.RegisteredAt
	}

	worker := Worker{
		ID:                 normalized.WorkerID,
		Capabilities:       append([]string(nil), normalized.Capabilities...),
		MaxConcurrency:     normalized.MaxConcurrency,
		Labels:             cloneLabels(normalized.Labels),
		Status:             StatusReady,
		InflightExecutions: 0,
		RegisteredAt:       registeredAt,
		LastHeartbeatAt:    now,
		UpdatedAt:          now,
	}

	s.workersByID[worker.ID] = worker
	return cloneWorker(worker), nil
}

func (s *InMemoryService) Heartbeat(_ context.Context, input HeartbeatInput) (Worker, error) {
	normalized, err := normalizeHeartbeatInput(input)
	if err != nil {
		return Worker{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	worker, ok := s.workersByID[normalized.WorkerID]
	if !ok {
		return Worker{}, ErrWorkerNotFound
	}

	now := s.now().UTC()
	worker.Status = normalized.Status
	worker.InflightExecutions = normalized.InflightExecutions
	worker.LastHeartbeatAt = now
	worker.UpdatedAt = now

	s.workersByID[worker.ID] = worker
	return cloneWorker(worker), nil
}

func (s *InMemoryService) GetWorker(workerID string) (Worker, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	worker, ok := s.workersByID[strings.TrimSpace(workerID)]
	if !ok {
		return Worker{}, false
	}

	return cloneWorker(worker), true
}

func normalizeRegisterInput(input RegisterInput) (RegisterInput, error) {
	input.WorkerID = strings.TrimSpace(input.WorkerID)
	if input.WorkerID == "" {
		return RegisterInput{}, fmt.Errorf("%w: worker_id is required", ErrInvalidArgument)
	}

	if input.MaxConcurrency <= 0 {
		input.MaxConcurrency = 1
	}

	input.Capabilities = normalizeCapabilities(input.Capabilities)
	input.Labels = cloneLabels(input.Labels)
	return input, nil
}

func normalizeHeartbeatInput(input HeartbeatInput) (HeartbeatInput, error) {
	input.WorkerID = strings.TrimSpace(input.WorkerID)
	if input.WorkerID == "" {
		return HeartbeatInput{}, fmt.Errorf("%w: worker_id is required", ErrInvalidArgument)
	}

	if input.InflightExecutions < 0 {
		return HeartbeatInput{}, fmt.Errorf("%w: inflight_executions must be greater than or equal to zero", ErrInvalidArgument)
	}

	if input.Status == "" {
		input.Status = StatusReady
	}

	return input, nil
}

func normalizeCapabilities(capabilities []string) []string {
	if len(capabilities) == 0 {
		return []string{}
	}

	seen := make(map[string]struct{}, len(capabilities))
	normalized := make([]string, 0, len(capabilities))

	for _, capability := range capabilities {
		trimmed := strings.TrimSpace(capability)
		if trimmed == "" {
			continue
		}

		if _, ok := seen[trimmed]; ok {
			continue
		}

		seen[trimmed] = struct{}{}
		normalized = append(normalized, trimmed)
	}

	sort.Strings(normalized)
	return normalized
}

func cloneLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return map[string]string{}
	}

	cloned := make(map[string]string, len(labels))
	for key, value := range labels {
		cloned[key] = value
	}

	return cloned
}

func cloneWorker(worker Worker) Worker {
	worker.Capabilities = append([]string(nil), worker.Capabilities...)
	worker.Labels = cloneLabels(worker.Labels)
	return worker
}
