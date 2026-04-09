package jobs

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	StatusPending  Status = "pending"
	StatusCanceled Status = "canceled"
)

var (
	ErrInvalidArgument     = errors.New("invalid argument")
	ErrJobNotFound         = errors.New("job not found")
	ErrIdempotencyConflict = errors.New("idempotency key already used with different request")
)

type Status string

type RetryPolicy struct {
	MaxAttempts    int32
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

type Job struct {
	ID                 string
	JobType            string
	Payload            []byte
	Status             Status
	Priority           int32
	IdempotencyKey     string
	Metadata           map[string]string
	RetryPolicy        RetryPolicy
	CreatedAt          time.Time
	UpdatedAt          time.Time
	requestFingerprint string
}

type Execution struct {
	ID           string
	JobID        string
	Attempt      int32
	Status       string
	WorkerID     string
	ErrorMessage string
	ClaimedAt    *time.Time
	StartedAt    *time.Time
	FinishedAt   *time.Time
}

type SubmitJobInput struct {
	JobType        string
	Payload        []byte
	Priority       int32
	IdempotencyKey string
	Metadata       map[string]string
	RetryPolicy    RetryPolicy
}

type Service interface {
	SubmitJob(context.Context, SubmitJobInput) (Job, error)
	GetJob(context.Context, string) (Job, error)
	CancelJob(context.Context, string) (Job, error)
	ListExecutions(context.Context, string) ([]Execution, error)
}

type InMemoryService struct {
	mu             sync.RWMutex
	jobsByID       map[string]Job
	jobIDByIdemKey map[string]string
	now            func() time.Time
	newID          func() string
}

func NewInMemoryService() *InMemoryService {
	return &InMemoryService{
		jobsByID:       make(map[string]Job),
		jobIDByIdemKey: make(map[string]string),
		now:            time.Now,
		newID:          newJobID,
	}
}

func (s *InMemoryService) SubmitJob(_ context.Context, input SubmitJobInput) (Job, error) {
	normalized, err := normalizeSubmitInput(input)
	if err != nil {
		return Job{}, err
	}

	fingerprint := buildFingerprint(normalized)

	s.mu.Lock()
	defer s.mu.Unlock()

	if normalized.IdempotencyKey != "" {
		if existingID, ok := s.jobIDByIdemKey[normalized.IdempotencyKey]; ok {
			existing := s.jobsByID[existingID]
			if existing.requestFingerprint != fingerprint {
				return Job{}, ErrIdempotencyConflict
			}

			return cloneJob(existing), nil
		}
	}

	now := s.now().UTC()
	job := Job{
		ID:                 s.newID(),
		JobType:            normalized.JobType,
		Payload:            append([]byte(nil), normalized.Payload...),
		Status:             StatusPending,
		Priority:           normalized.Priority,
		IdempotencyKey:     normalized.IdempotencyKey,
		Metadata:           cloneMetadata(normalized.Metadata),
		RetryPolicy:        normalized.RetryPolicy,
		CreatedAt:          now,
		UpdatedAt:          now,
		requestFingerprint: fingerprint,
	}

	s.jobsByID[job.ID] = job
	if job.IdempotencyKey != "" {
		s.jobIDByIdemKey[job.IdempotencyKey] = job.ID
	}

	return cloneJob(job), nil
}

func (s *InMemoryService) GetJob(_ context.Context, jobID string) (Job, error) {
	trimmedJobID := strings.TrimSpace(jobID)
	if trimmedJobID == "" {
		return Job{}, fmt.Errorf("%w: job_id is required", ErrInvalidArgument)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	job, ok := s.jobsByID[trimmedJobID]
	if !ok {
		return Job{}, ErrJobNotFound
	}

	return cloneJob(job), nil
}

func (s *InMemoryService) CancelJob(_ context.Context, jobID string) (Job, error) {
	trimmedJobID := strings.TrimSpace(jobID)
	if trimmedJobID == "" {
		return Job{}, fmt.Errorf("%w: job_id is required", ErrInvalidArgument)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobsByID[trimmedJobID]
	if !ok {
		return Job{}, ErrJobNotFound
	}

	if job.Status != StatusCanceled {
		job.Status = StatusCanceled
		job.UpdatedAt = s.now().UTC()
		s.jobsByID[trimmedJobID] = job
	}

	return cloneJob(job), nil
}

func (s *InMemoryService) ListExecutions(_ context.Context, jobID string) ([]Execution, error) {
	trimmedJobID := strings.TrimSpace(jobID)
	if trimmedJobID == "" {
		return nil, fmt.Errorf("%w: job_id is required", ErrInvalidArgument)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, ok := s.jobsByID[trimmedJobID]; !ok {
		return nil, ErrJobNotFound
	}

	return []Execution{}, nil
}

func normalizeSubmitInput(input SubmitJobInput) (SubmitJobInput, error) {
	input.JobType = strings.TrimSpace(input.JobType)
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)

	if input.JobType == "" {
		return SubmitJobInput{}, fmt.Errorf("%w: job_type is required", ErrInvalidArgument)
	}

	if len(input.Payload) == 0 {
		return SubmitJobInput{}, fmt.Errorf("%w: payload is required", ErrInvalidArgument)
	}

	if input.Priority < 0 {
		return SubmitJobInput{}, fmt.Errorf("%w: priority must be greater than or equal to zero", ErrInvalidArgument)
	}

	retryPolicy, err := normalizeRetryPolicy(input.RetryPolicy)
	if err != nil {
		return SubmitJobInput{}, err
	}

	input.Payload = append([]byte(nil), input.Payload...)
	input.Metadata = cloneMetadata(input.Metadata)
	input.RetryPolicy = retryPolicy

	return input, nil
}

func normalizeRetryPolicy(policy RetryPolicy) (RetryPolicy, error) {
	if policy.MaxAttempts == 0 && policy.InitialBackoff == 0 && policy.MaxBackoff == 0 {
		return RetryPolicy{
			MaxAttempts:    3,
			InitialBackoff: 5 * time.Second,
			MaxBackoff:     1 * time.Minute,
		}, nil
	}

	if policy.MaxAttempts <= 0 {
		return RetryPolicy{}, fmt.Errorf("%w: retry_policy.max_attempts must be greater than zero", ErrInvalidArgument)
	}

	if policy.InitialBackoff < 0 || policy.MaxBackoff < 0 {
		return RetryPolicy{}, fmt.Errorf("%w: retry_policy backoff values must be non-negative", ErrInvalidArgument)
	}

	if policy.MaxBackoff > 0 && policy.InitialBackoff > policy.MaxBackoff {
		return RetryPolicy{}, fmt.Errorf("%w: retry_policy.max_backoff must be greater than or equal to initial_backoff", ErrInvalidArgument)
	}

	if policy.InitialBackoff == 0 {
		policy.InitialBackoff = 5 * time.Second
	}

	if policy.MaxBackoff == 0 {
		policy.MaxBackoff = policy.InitialBackoff
	}

	return policy, nil
}

func buildFingerprint(input SubmitJobInput) string {
	hash := sha256.New()
	hash.Write([]byte(input.JobType))
	hash.Write(input.Payload)
	_, _ = fmt.Fprintf(hash, "|%d|%s|%d|%d|", input.Priority, input.IdempotencyKey, input.RetryPolicy.MaxAttempts, input.RetryPolicy.InitialBackoff.Nanoseconds())
	_, _ = fmt.Fprintf(hash, "%d|", input.RetryPolicy.MaxBackoff.Nanoseconds())

	keys := make([]string, 0, len(input.Metadata))
	for key := range input.Metadata {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		hash.Write([]byte(key))
		hash.Write([]byte("="))
		hash.Write([]byte(input.Metadata[key]))
		hash.Write([]byte("|"))
	}

	return hex.EncodeToString(hash.Sum(nil))
}

func cloneJob(job Job) Job {
	job.Payload = append([]byte(nil), job.Payload...)
	job.Metadata = cloneMetadata(job.Metadata)
	return job
}

func cloneMetadata(metadata map[string]string) map[string]string {
	if len(metadata) == 0 {
		return map[string]string{}
	}

	cloned := make(map[string]string, len(metadata))
	for key, value := range metadata {
		cloned[key] = value
	}

	return cloned
}

func newJobID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		panic(err)
	}

	return "job_" + hex.EncodeToString(raw[:])
}
