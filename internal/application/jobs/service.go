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
	StatusPending     Status = "pending"
	StatusDispatching Status = "dispatching"
	StatusSucceeded   Status = "succeeded"
	StatusFailed      Status = "failed"
	StatusCanceled    Status = "canceled"
)

const (
	ExecutionStatusQueued       = "queued"
	ExecutionStatusClaimed      = "claimed"
	ExecutionStatusRunning      = "running"
	ExecutionStatusSucceeded    = "succeeded"
	ExecutionStatusFailed       = "failed"
	ExecutionStatusDeadLettered = "dead_lettered"
)

var (
	ErrInvalidArgument     = errors.New("invalid argument")
	ErrJobNotFound         = errors.New("job not found")
	ErrExecutionNotFound   = errors.New("execution not found")
	ErrIdempotencyConflict = errors.New("idempotency key already used with different request")
	ErrMutationsDisabled   = errors.New("mutations are disabled in this region")
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
	ID             string
	JobID          string
	Attempt        int32
	Status         string
	WorkerID       string
	ErrorMessage   string
	ClaimedAt      *time.Time
	StartedAt      *time.Time
	FinishedAt     *time.Time
	AvailableAt    time.Time
	LeaseExpiresAt *time.Time
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

type SchedulerStore interface {
	TryAcquireLeadership(context.Context, string, time.Duration) (bool, error)
	RequeueExpiredExecutions(context.Context, time.Duration, int) (int, error)
	EnqueueRunnableExecutions(context.Context, int) (int, error)
}

type DispatchService interface {
	ClaimNextForWorker(context.Context, ClaimForWorkerInput, time.Duration) (*Assignment, error)
	RenewLeasesForWorker(context.Context, string, time.Duration) error
	CompleteExecution(context.Context, CompleteExecutionInput) (Execution, Job, error)
	FailExecution(context.Context, FailExecutionInput) (Execution, *Execution, Job, error)
	ReleaseExecution(context.Context, string, string) error
}

type ClaimForWorkerInput struct {
	WorkerID           string
	Capabilities       []string
	MaxConcurrency     int32
	InflightExecutions int32
}

type Assignment struct {
	ExecutionID    string
	JobID          string
	JobType        string
	Payload        []byte
	Attempt        int32
	LeaseExpiresAt time.Time
	IdempotencyKey string
}

type CompleteExecutionInput struct {
	ExecutionID string
	WorkerID    string
	Metadata    map[string]string
}

type FailExecutionInput struct {
	ExecutionID  string
	WorkerID     string
	ErrorMessage string
	Metadata     map[string]string
}

type InMemoryService struct {
	mu                  sync.RWMutex
	jobsByID            map[string]Job
	jobIDByIdemKey      map[string]string
	executionsByID      map[string]Execution
	executionIDsByJobID map[string][]string
	now                 func() time.Time
	newJobID            func() string
	newExecutionID      func() string
}

func NewInMemoryService() *InMemoryService {
	return &InMemoryService{
		jobsByID:            make(map[string]Job),
		jobIDByIdemKey:      make(map[string]string),
		executionsByID:      make(map[string]Execution),
		executionIDsByJobID: make(map[string][]string),
		now:                 time.Now,
		newJobID:            newJobID,
		newExecutionID:      newExecutionID,
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
		ID:                 s.newJobID(),
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

	initialExecution := Execution{
		ID:          s.newExecutionID(),
		JobID:       job.ID,
		Attempt:     1,
		Status:      ExecutionStatusQueued,
		AvailableAt: now,
	}

	s.jobsByID[job.ID] = job
	if job.IdempotencyKey != "" {
		s.jobIDByIdemKey[job.IdempotencyKey] = job.ID
	}
	s.executionsByID[initialExecution.ID] = initialExecution
	s.executionIDsByJobID[job.ID] = append(s.executionIDsByJobID[job.ID], initialExecution.ID)

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

	executionIDs := s.executionIDsByJobID[trimmedJobID]
	executions := make([]Execution, 0, len(executionIDs))
	for _, executionID := range executionIDs {
		executions = append(executions, cloneExecution(s.executionsByID[executionID]))
	}

	sort.Slice(executions, func(i, j int) bool {
		if executions[i].Attempt == executions[j].Attempt {
			return executions[i].AvailableAt.Before(executions[j].AvailableAt)
		}

		return executions[i].Attempt < executions[j].Attempt
	})

	return executions, nil
}

func (s *InMemoryService) ClaimNextRunnable(_ context.Context, leaseDuration time.Duration) (Execution, Job, bool, error) {
	if leaseDuration <= 0 {
		return Execution{}, Job{}, false, fmt.Errorf("%w: lease duration must be greater than zero", ErrInvalidArgument)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now().UTC()
	var candidateExecution Execution
	var candidateJob Job
	found := false

	for _, execution := range s.executionsByID {
		if execution.Status != ExecutionStatusQueued {
			continue
		}

		if execution.AvailableAt.After(now) {
			continue
		}

		job := s.jobsByID[execution.JobID]
		if job.Status == StatusCanceled || job.Status == StatusFailed || job.Status == StatusSucceeded {
			continue
		}

		if !found || isHigherPriorityCandidate(job, execution, candidateJob, candidateExecution) {
			candidateExecution = execution
			candidateJob = job
			found = true
		}
	}

	if !found {
		return Execution{}, Job{}, false, nil
	}

	claimedAt := now
	leaseExpiresAt := now.Add(leaseDuration)

	candidateExecution.Status = ExecutionStatusClaimed
	candidateExecution.ClaimedAt = &claimedAt
	candidateExecution.StartedAt = &claimedAt
	candidateExecution.LeaseExpiresAt = &leaseExpiresAt

	candidateJob.Status = StatusDispatching
	candidateJob.UpdatedAt = now

	s.executionsByID[candidateExecution.ID] = candidateExecution
	s.jobsByID[candidateJob.ID] = candidateJob

	return cloneExecution(candidateExecution), cloneJob(candidateJob), true, nil
}

func (s *InMemoryService) MarkExecutionFailed(_ context.Context, executionID, errorMessage string) (Execution, *Execution, Job, error) {
	trimmedExecutionID := strings.TrimSpace(executionID)
	if trimmedExecutionID == "" {
		return Execution{}, nil, Job{}, fmt.Errorf("%w: execution_id is required", ErrInvalidArgument)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	execution, ok := s.executionsByID[trimmedExecutionID]
	if !ok {
		return Execution{}, nil, Job{}, ErrExecutionNotFound
	}

	if execution.Status != ExecutionStatusClaimed {
		return Execution{}, nil, Job{}, fmt.Errorf("%w: execution must be claimed before failure handling", ErrInvalidArgument)
	}

	job := s.jobsByID[execution.JobID]
	now := s.now().UTC()
	trimmedError := strings.TrimSpace(errorMessage)
	if trimmedError == "" {
		trimmedError = "execution failed"
	}

	execution.ErrorMessage = trimmedError
	execution.FinishedAt = &now

	var nextExecution *Execution
	if execution.Attempt >= job.RetryPolicy.MaxAttempts {
		execution.Status = ExecutionStatusDeadLettered
		job.Status = StatusFailed
	} else {
		execution.Status = ExecutionStatusFailed
		retryExecution := Execution{
			ID:          s.newExecutionID(),
			JobID:       job.ID,
			Attempt:     execution.Attempt + 1,
			Status:      ExecutionStatusQueued,
			AvailableAt: now.Add(retryDelay(job.RetryPolicy, execution.Attempt+1)),
		}
		s.executionsByID[retryExecution.ID] = retryExecution
		s.executionIDsByJobID[job.ID] = append(s.executionIDsByJobID[job.ID], retryExecution.ID)
		nextExecution = &retryExecution
		job.Status = StatusPending
	}

	job.UpdatedAt = now
	s.executionsByID[execution.ID] = execution
	s.jobsByID[job.ID] = job

	clonedJob := cloneJob(job)
	if nextExecution == nil {
		return cloneExecution(execution), nil, clonedJob, nil
	}

	clonedNextExecution := cloneExecution(*nextExecution)
	return cloneExecution(execution), &clonedNextExecution, clonedJob, nil
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

func retryDelay(policy RetryPolicy, nextAttempt int32) time.Duration {
	delay := policy.InitialBackoff
	for attempt := int32(2); attempt < nextAttempt; attempt++ {
		if delay >= policy.MaxBackoff {
			return policy.MaxBackoff
		}

		delay *= 2
		if delay > policy.MaxBackoff {
			return policy.MaxBackoff
		}
	}

	return delay
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

func isHigherPriorityCandidate(job Job, execution Execution, currentJob Job, currentExecution Execution) bool {
	if job.Priority != currentJob.Priority {
		return job.Priority > currentJob.Priority
	}

	if !execution.AvailableAt.Equal(currentExecution.AvailableAt) {
		return execution.AvailableAt.Before(currentExecution.AvailableAt)
	}

	if !job.CreatedAt.Equal(currentJob.CreatedAt) {
		return job.CreatedAt.Before(currentJob.CreatedAt)
	}

	return execution.Attempt < currentExecution.Attempt
}

func cloneJob(job Job) Job {
	job.Payload = append([]byte(nil), job.Payload...)
	job.Metadata = cloneMetadata(job.Metadata)
	return job
}

func cloneExecution(execution Execution) Execution {
	if execution.ClaimedAt != nil {
		claimedAt := *execution.ClaimedAt
		execution.ClaimedAt = &claimedAt
	}
	if execution.StartedAt != nil {
		startedAt := *execution.StartedAt
		execution.StartedAt = &startedAt
	}
	if execution.FinishedAt != nil {
		finishedAt := *execution.FinishedAt
		execution.FinishedAt = &finishedAt
	}
	if execution.LeaseExpiresAt != nil {
		leaseExpiresAt := *execution.LeaseExpiresAt
		execution.LeaseExpiresAt = &leaseExpiresAt
	}

	return execution
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

func newExecutionID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		panic(err)
	}

	return "exec_" + hex.EncodeToString(raw[:])
}
