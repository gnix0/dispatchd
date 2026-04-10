package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/gnix0/task-orchestrator/internal/application/jobs"
	"github.com/gnix0/task-orchestrator/internal/application/workers"
	"github.com/gnix0/task-orchestrator/internal/platform/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

//go:embed schema.sql
var schema string

type Store struct {
	pg    *pgxpool.Pool
	redis *redis.Client
	cfg   config.Service
}

var (
	_ jobs.Service         = (*Store)(nil)
	_ jobs.SchedulerStore  = (*Store)(nil)
	_ jobs.DispatchService = (*Store)(nil)
	_ workers.Service      = (*Store)(nil)
)

func Open(ctx context.Context, cfg config.Service) (*Store, error) {
	pgPool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}

	if err := pgPool.Ping(ctx); err != nil {
		pgPool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddress,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		pgPool.Close()
		_ = redisClient.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}

	store := &Store{
		pg:    pgPool,
		redis: redisClient,
		cfg:   cfg,
	}

	if err := store.Migrate(ctx); err != nil {
		store.Close()
		return nil, err
	}

	return store, nil
}

func (s *Store) Close() {
	if s.redis != nil {
		_ = s.redis.Close()
	}
	if s.pg != nil {
		s.pg.Close()
	}
}

func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.pg.Exec(ctx, schema); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	return nil
}

func (s *Store) SubmitJob(ctx context.Context, input jobs.SubmitJobInput) (jobs.Job, error) {
	normalized, err := normalizeSubmitInput(input)
	if err != nil {
		return jobs.Job{}, err
	}

	fingerprint := buildFingerprint(normalized)
	if normalized.IdempotencyKey != "" {
		existing, existingFingerprint, err := s.getJobByIdempotencyKey(ctx, normalized.IdempotencyKey)
		if err == nil {
			if existingFingerprint != fingerprint {
				return jobs.Job{}, jobs.ErrIdempotencyConflict
			}
			return existing, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return jobs.Job{}, err
		}
	}

	now := time.Now().UTC()
	job := jobs.Job{
		ID:             newJobID(),
		JobType:        normalized.JobType,
		Payload:        append([]byte(nil), normalized.Payload...),
		Status:         jobs.StatusPending,
		Priority:       normalized.Priority,
		IdempotencyKey: normalized.IdempotencyKey,
		Metadata:       cloneMetadata(normalized.Metadata),
		RetryPolicy:    normalized.RetryPolicy,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	execution := jobs.Execution{
		ID:          newExecutionID(),
		JobID:       job.ID,
		Attempt:     1,
		Status:      jobs.ExecutionStatusQueued,
		AvailableAt: now,
	}

	tx, err := s.pg.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return jobs.Job{}, fmt.Errorf("begin submit tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	metadataJSON, err := marshalJSON(job.Metadata)
	if err != nil {
		return jobs.Job{}, err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO jobs (
		  id, job_type, payload, status, priority, idempotency_key, metadata,
		  retry_max_attempts, retry_initial_backoff_seconds, retry_max_backoff_seconds,
		  request_fingerprint, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''), $7, $8, $9, $10, $11, $12, $13)
	`,
		job.ID, job.JobType, job.Payload, string(job.Status), job.Priority, job.IdempotencyKey, metadataJSON,
		job.RetryPolicy.MaxAttempts, int64(job.RetryPolicy.InitialBackoff.Seconds()), int64(job.RetryPolicy.MaxBackoff.Seconds()),
		fingerprint, job.CreatedAt, job.UpdatedAt,
	)
	if err != nil {
		if isUniqueViolation(err) && normalized.IdempotencyKey != "" {
			existing, existingFingerprint, lookupErr := s.getJobByIdempotencyKey(ctx, normalized.IdempotencyKey)
			if lookupErr != nil {
				return jobs.Job{}, lookupErr
			}
			if existingFingerprint != fingerprint {
				return jobs.Job{}, jobs.ErrIdempotencyConflict
			}
			return existing, nil
		}
		return jobs.Job{}, fmt.Errorf("insert job: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO executions (
		  id, job_id, attempt, status, worker_id, error_message,
		  available_at, result_metadata, created_at, updated_at
		) VALUES ($1, $2, $3, $4, '', '', $5, '{}'::jsonb, $6, $6)
	`, execution.ID, execution.JobID, execution.Attempt, execution.Status, execution.AvailableAt, now)
	if err != nil {
		return jobs.Job{}, fmt.Errorf("insert initial execution: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return jobs.Job{}, fmt.Errorf("commit submit tx: %w", err)
	}

	return job, nil
}

func (s *Store) GetJob(ctx context.Context, jobID string) (jobs.Job, error) {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return jobs.Job{}, fmt.Errorf("%w: job_id is required", jobs.ErrInvalidArgument)
	}

	job, _, err := s.getJobByID(ctx, jobID)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Job{}, jobs.ErrJobNotFound
	}
	return job, err
}

func (s *Store) CancelJob(ctx context.Context, jobID string) (jobs.Job, error) {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return jobs.Job{}, fmt.Errorf("%w: job_id is required", jobs.ErrInvalidArgument)
	}

	now := time.Now().UTC()
	row := s.pg.QueryRow(ctx, `
		UPDATE jobs
		SET status = $2, updated_at = $3
		WHERE id = $1
		RETURNING id, job_type, payload, status, priority, COALESCE(idempotency_key, ''),
		  metadata, retry_max_attempts, retry_initial_backoff_seconds, retry_max_backoff_seconds,
		  request_fingerprint, created_at, updated_at
	`, jobID, string(jobs.StatusCanceled), now)

	job, _, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Job{}, jobs.ErrJobNotFound
	}
	return job, err
}

func (s *Store) ListExecutions(ctx context.Context, jobID string) ([]jobs.Execution, error) {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return nil, fmt.Errorf("%w: job_id is required", jobs.ErrInvalidArgument)
	}

	if _, _, err := s.getJobByID(ctx, jobID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, jobs.ErrJobNotFound
		}
		return nil, err
	}

	rows, err := s.pg.Query(ctx, `
		SELECT id, job_id, attempt, status, worker_id, error_message,
		  claimed_at, started_at, finished_at, available_at, lease_expires_at
		FROM executions
		WHERE job_id = $1
		ORDER BY attempt ASC, available_at ASC
	`, jobID)
	if err != nil {
		return nil, fmt.Errorf("list executions: %w", err)
	}
	defer rows.Close()

	executions := make([]jobs.Execution, 0)
	for rows.Next() {
		execution, err := scanExecution(rows)
		if err != nil {
			return nil, err
		}
		executions = append(executions, execution)
	}

	return executions, rows.Err()
}

func (s *Store) TryAcquireLeadership(ctx context.Context, instanceID string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		ttl = 10 * time.Second
	}

	status := s.redis.SetArgs(ctx, s.cfg.SchedulerLeaderKey, instanceID, redis.SetArgs{
		TTL:  ttl,
		Mode: "NX",
	})
	acquired, err := status.Result()
	if err == redis.Nil {
		err = nil
	}
	if err != nil {
		return false, fmt.Errorf("acquire leader lock: %w", err)
	}
	if acquired == "OK" {
		return true, nil
	}

	currentOwner, err := s.redis.Get(ctx, s.cfg.SchedulerLeaderKey).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read leader lock: %w", err)
	}

	if currentOwner != instanceID {
		return false, nil
	}

	if err := s.redis.Expire(ctx, s.cfg.SchedulerLeaderKey, ttl).Err(); err != nil {
		return false, fmt.Errorf("renew leader lock: %w", err)
	}

	return true, nil
}

func (s *Store) RequeueExpiredExecutions(ctx context.Context, workerTTL time.Duration, limit int) (int, error) {
	if limit <= 0 {
		limit = 256
	}

	now := time.Now().UTC()
	if workerTTL > 0 {
		_, err := s.pg.Exec(ctx, `
			UPDATE workers
			SET status = $1, updated_at = $2
			WHERE status <> $1 AND last_heartbeat_at < $3
		`, string(workers.StatusOffline), now, now.Add(-workerTTL))
		if err != nil {
			return 0, fmt.Errorf("expire stale workers: %w", err)
		}
	}

	rows, err := s.pg.Query(ctx, `
		WITH candidate AS (
		  SELECT e.id, e.job_id
		  FROM executions e
		  WHERE e.status = $1
		    AND e.lease_expires_at IS NOT NULL
		    AND e.lease_expires_at < $2
		  ORDER BY e.lease_expires_at ASC
		  LIMIT $3
		), updated AS (
		  UPDATE executions e
		  SET status = $4,
		      worker_id = '',
		      error_message = 'lease expired',
		      claimed_at = NULL,
		      started_at = NULL,
		      lease_expires_at = NULL,
		      available_at = $2,
		      enqueued_at = NULL,
		      updated_at = $2
		  FROM candidate c
		  WHERE e.id = c.id
		  RETURNING e.id, e.job_id
		)
		SELECT id, job_id FROM updated
	`, jobs.ExecutionStatusClaimed, now, limit, jobs.ExecutionStatusQueued)
	if err != nil {
		return 0, fmt.Errorf("requeue expired executions: %w", err)
	}
	defer rows.Close()

	jobIDs := make(map[string]struct{})
	count := 0
	for rows.Next() {
		var executionID string
		var jobID string
		if err := rows.Scan(&executionID, &jobID); err != nil {
			return 0, fmt.Errorf("scan requeued execution: %w", err)
		}
		jobIDs[jobID] = struct{}{}
		count++
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	for jobID := range jobIDs {
		if _, err := s.pg.Exec(ctx, `
			UPDATE jobs
			SET status = $2, updated_at = $3
			WHERE id = $1 AND status NOT IN ($4, $5, $6)
		`, jobID, string(jobs.StatusPending), now, string(jobs.StatusCanceled), string(jobs.StatusFailed), string(jobs.StatusSucceeded)); err != nil {
			return 0, fmt.Errorf("reset job to pending: %w", err)
		}
	}

	return count, nil
}

func (s *Store) EnqueueRunnableExecutions(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		limit = 256
	}

	now := time.Now().UTC()
	rows, err := s.pg.Query(ctx, `
		WITH candidate AS (
		  SELECT e.id, j.job_type
		  FROM executions e
		  JOIN jobs j ON j.id = e.job_id
		  WHERE e.status = $1
		    AND e.available_at <= $2
		    AND e.enqueued_at IS NULL
		    AND j.status = $3
		  ORDER BY j.priority DESC, e.available_at ASC, j.created_at ASC, e.attempt ASC
		  LIMIT $4
		), updated AS (
		  UPDATE executions e
		  SET enqueued_at = $2, updated_at = $2
		  FROM candidate c
		  WHERE e.id = c.id
		  RETURNING e.id, c.job_type
		)
		SELECT id, job_type FROM updated
	`, jobs.ExecutionStatusQueued, now, string(jobs.StatusPending), limit)
	if err != nil {
		return 0, fmt.Errorf("enqueue runnable executions: %w", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var executionID string
		var jobType string
		if err := rows.Scan(&executionID, &jobType); err != nil {
			return 0, fmt.Errorf("scan runnable execution: %w", err)
		}
		if err := s.redis.RPush(ctx, s.readyQueueKey(jobType), executionID).Err(); err != nil {
			return 0, fmt.Errorf("push execution to redis: %w", err)
		}
		count++
	}

	return count, rows.Err()
}

func (s *Store) ClaimNextForWorker(ctx context.Context, input jobs.ClaimForWorkerInput, leaseDuration time.Duration) (*jobs.Assignment, error) {
	normalized := normalizeClaimInput(input)
	if normalized.WorkerID == "" || normalized.InflightExecutions >= normalized.MaxConcurrency {
		return nil, nil
	}

	for _, capability := range normalized.Capabilities {
		for {
			executionID, err := s.redis.LPop(ctx, s.capabilityQueueKey(capability)).Result()
			if err == redis.Nil {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("pop ready execution: %w", err)
			}

			assignment, err := s.claimExecution(ctx, executionID, normalized.WorkerID, leaseDuration)
			if err != nil {
				return nil, err
			}
			if assignment != nil {
				return assignment, nil
			}
		}
	}

	return nil, nil
}

func (s *Store) RenewLeasesForWorker(ctx context.Context, workerID string, leaseDuration time.Duration) error {
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return fmt.Errorf("%w: worker_id is required", workers.ErrInvalidArgument)
	}

	now := time.Now().UTC()
	_, err := s.pg.Exec(ctx, `
		UPDATE executions
		SET lease_expires_at = $2, updated_at = $3
		WHERE worker_id = $1 AND status = $4
	`, workerID, now.Add(leaseDuration), now, jobs.ExecutionStatusClaimed)
	if err != nil {
		return fmt.Errorf("renew worker leases: %w", err)
	}
	return nil
}

func (s *Store) CompleteExecution(ctx context.Context, input jobs.CompleteExecutionInput) (jobs.Execution, jobs.Job, error) {
	input.ExecutionID = strings.TrimSpace(input.ExecutionID)
	input.WorkerID = strings.TrimSpace(input.WorkerID)
	if input.ExecutionID == "" {
		return jobs.Execution{}, jobs.Job{}, fmt.Errorf("%w: execution_id is required", jobs.ErrInvalidArgument)
	}

	tx, err := s.pg.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return jobs.Execution{}, jobs.Job{}, fmt.Errorf("begin completion tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	execution, job, _, err := s.loadExecutionAndJobForUpdate(ctx, tx, input.ExecutionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Execution{}, jobs.Job{}, jobs.ErrExecutionNotFound
	}
	if err != nil {
		return jobs.Execution{}, jobs.Job{}, err
	}
	if execution.WorkerID != "" && input.WorkerID != "" && execution.WorkerID != input.WorkerID {
		return jobs.Execution{}, jobs.Job{}, fmt.Errorf("%w: execution is owned by a different worker", jobs.ErrInvalidArgument)
	}
	if execution.Status == jobs.ExecutionStatusSucceeded {
		if err := tx.Commit(ctx); err != nil {
			return jobs.Execution{}, jobs.Job{}, fmt.Errorf("commit completion tx: %w", err)
		}
		return execution, job, nil
	}
	if execution.Status != jobs.ExecutionStatusClaimed && execution.Status != jobs.ExecutionStatusRunning {
		return jobs.Execution{}, jobs.Job{}, fmt.Errorf("%w: execution must be claimed before completion", jobs.ErrInvalidArgument)
	}

	now := time.Now().UTC()
	metadataJSON, err := marshalJSON(input.Metadata)
	if err != nil {
		return jobs.Execution{}, jobs.Job{}, err
	}

	_, err = tx.Exec(ctx, `
		UPDATE executions
		SET status = $2,
		    finished_at = $3,
		    lease_expires_at = NULL,
		    result_metadata = $4,
		    updated_at = $3
		WHERE id = $1
	`, input.ExecutionID, jobs.ExecutionStatusSucceeded, now, metadataJSON)
	if err != nil {
		return jobs.Execution{}, jobs.Job{}, fmt.Errorf("update successful execution: %w", err)
	}

	_, err = tx.Exec(ctx, `
		UPDATE jobs
		SET status = $2, updated_at = $3
		WHERE id = $1
	`, job.ID, string(jobs.StatusSucceeded), now)
	if err != nil {
		return jobs.Execution{}, jobs.Job{}, fmt.Errorf("update successful job: %w", err)
	}

	execution.Status = jobs.ExecutionStatusSucceeded
	execution.FinishedAt = &now
	execution.LeaseExpiresAt = nil
	job.Status = jobs.StatusSucceeded
	job.UpdatedAt = now

	if err := tx.Commit(ctx); err != nil {
		return jobs.Execution{}, jobs.Job{}, fmt.Errorf("commit completion tx: %w", err)
	}

	return execution, job, nil
}

func (s *Store) FailExecution(ctx context.Context, input jobs.FailExecutionInput) (jobs.Execution, *jobs.Execution, jobs.Job, error) {
	input.ExecutionID = strings.TrimSpace(input.ExecutionID)
	input.WorkerID = strings.TrimSpace(input.WorkerID)
	input.ErrorMessage = strings.TrimSpace(input.ErrorMessage)
	if input.ExecutionID == "" {
		return jobs.Execution{}, nil, jobs.Job{}, fmt.Errorf("%w: execution_id is required", jobs.ErrInvalidArgument)
	}
	if input.ErrorMessage == "" {
		input.ErrorMessage = "execution failed"
	}

	tx, err := s.pg.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return jobs.Execution{}, nil, jobs.Job{}, fmt.Errorf("begin failure tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	execution, job, fingerprint, err := s.loadExecutionAndJobForUpdate(ctx, tx, input.ExecutionID)
	_ = fingerprint
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Execution{}, nil, jobs.Job{}, jobs.ErrExecutionNotFound
	}
	if err != nil {
		return jobs.Execution{}, nil, jobs.Job{}, err
	}
	if execution.WorkerID != "" && input.WorkerID != "" && execution.WorkerID != input.WorkerID {
		return jobs.Execution{}, nil, jobs.Job{}, fmt.Errorf("%w: execution is owned by a different worker", jobs.ErrInvalidArgument)
	}
	if execution.Status != jobs.ExecutionStatusClaimed && execution.Status != jobs.ExecutionStatusRunning {
		return jobs.Execution{}, nil, jobs.Job{}, fmt.Errorf("%w: execution must be claimed before failure handling", jobs.ErrInvalidArgument)
	}

	now := time.Now().UTC()
	metadataJSON, err := marshalJSON(input.Metadata)
	if err != nil {
		return jobs.Execution{}, nil, jobs.Job{}, err
	}

	execution.ErrorMessage = input.ErrorMessage
	execution.FinishedAt = &now
	execution.LeaseExpiresAt = nil

	var nextExecution *jobs.Execution
	if execution.Attempt >= job.RetryPolicy.MaxAttempts {
		execution.Status = jobs.ExecutionStatusDeadLettered
		job.Status = jobs.StatusFailed
	} else {
		execution.Status = jobs.ExecutionStatusFailed
		retry := jobs.Execution{
			ID:          newExecutionID(),
			JobID:       job.ID,
			Attempt:     execution.Attempt + 1,
			Status:      jobs.ExecutionStatusQueued,
			AvailableAt: now.Add(retryDelay(job.RetryPolicy, execution.Attempt+1)),
		}
		nextExecution = &retry
		job.Status = jobs.StatusPending
	}
	job.UpdatedAt = now

	_, err = tx.Exec(ctx, `
		UPDATE executions
		SET status = $2,
		    error_message = $3,
		    finished_at = $4,
		    lease_expires_at = NULL,
		    result_metadata = $5,
		    updated_at = $4
		WHERE id = $1
	`, execution.ID, execution.Status, execution.ErrorMessage, now, metadataJSON)
	if err != nil {
		return jobs.Execution{}, nil, jobs.Job{}, fmt.Errorf("update failed execution: %w", err)
	}

	if nextExecution != nil {
		_, err = tx.Exec(ctx, `
			INSERT INTO executions (
			  id, job_id, attempt, status, worker_id, error_message,
			  available_at, enqueued_at, result_metadata, created_at, updated_at
			) VALUES ($1, $2, $3, $4, '', '', $5, $6, '{}'::jsonb, $6, $6)
		`, nextExecution.ID, nextExecution.JobID, nextExecution.Attempt, nextExecution.Status, nextExecution.AvailableAt, now)
		if err != nil {
			return jobs.Execution{}, nil, jobs.Job{}, fmt.Errorf("insert retry execution: %w", err)
		}
	}

	_, err = tx.Exec(ctx, `
		UPDATE jobs
		SET status = $2, updated_at = $3
		WHERE id = $1
	`, job.ID, string(job.Status), now)
	if err != nil {
		return jobs.Execution{}, nil, jobs.Job{}, fmt.Errorf("update job after failure: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return jobs.Execution{}, nil, jobs.Job{}, fmt.Errorf("commit failure tx: %w", err)
	}

	if nextExecution != nil {
		if err := s.redis.RPush(ctx, s.readyQueueKey(job.JobType), nextExecution.ID).Err(); err != nil {
			return jobs.Execution{}, nil, jobs.Job{}, fmt.Errorf("enqueue retry execution: %w", err)
		}
	}

	return execution, nextExecution, job, nil
}

func (s *Store) ReleaseExecution(ctx context.Context, executionID, reason string) error {
	executionID = strings.TrimSpace(executionID)
	if executionID == "" {
		return fmt.Errorf("%w: execution_id is required", jobs.ErrInvalidArgument)
	}

	tx, err := s.pg.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin release tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	execution, job, _, err := s.loadExecutionAndJobForUpdate(ctx, tx, executionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.ErrExecutionNotFound
	}
	if err != nil {
		return err
	}
	if execution.Status != jobs.ExecutionStatusClaimed {
		return fmt.Errorf("%w: execution must be claimed before release", jobs.ErrInvalidArgument)
	}

	now := time.Now().UTC()
	trimmedReason := strings.TrimSpace(reason)
	if trimmedReason == "" {
		trimmedReason = "assignment delivery failed"
	}

	_, err = tx.Exec(ctx, `
		UPDATE executions
		SET status = $2,
		    worker_id = '',
		    error_message = $3,
		    claimed_at = NULL,
		    started_at = NULL,
		    lease_expires_at = NULL,
		    available_at = $4,
		    enqueued_at = $4,
		    updated_at = $4
		WHERE id = $1
	`, execution.ID, jobs.ExecutionStatusQueued, trimmedReason, now)
	if err != nil {
		return fmt.Errorf("release execution: %w", err)
	}

	_, err = tx.Exec(ctx, `
		UPDATE jobs
		SET status = $2, updated_at = $3
		WHERE id = $1 AND status NOT IN ($4, $5, $6)
	`, job.ID, string(jobs.StatusPending), now, string(jobs.StatusCanceled), string(jobs.StatusFailed), string(jobs.StatusSucceeded))
	if err != nil {
		return fmt.Errorf("reset job after release: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit release tx: %w", err)
	}

	if err := s.redis.RPush(ctx, s.readyQueueKey(job.JobType), execution.ID).Err(); err != nil {
		return fmt.Errorf("requeue released execution: %w", err)
	}

	return nil
}

func (s *Store) Register(ctx context.Context, input workers.RegisterInput) (workers.Worker, error) {
	normalized, err := normalizeRegisterInput(input)
	if err != nil {
		return workers.Worker{}, err
	}

	now := time.Now().UTC()
	capabilitiesJSON, err := marshalJSON(normalized.Capabilities)
	if err != nil {
		return workers.Worker{}, err
	}
	labelsJSON, err := marshalJSON(normalized.Labels)
	if err != nil {
		return workers.Worker{}, err
	}

	row := s.pg.QueryRow(ctx, `
		INSERT INTO workers (
		  id, capabilities, max_concurrency, labels, status, inflight_executions,
		  registered_at, last_heartbeat_at, updated_at, gateway_instance_id
		) VALUES ($1, $2, $3, $4, $5, 0, $6, $6, $6, $7)
		ON CONFLICT (id) DO UPDATE SET
		  capabilities = EXCLUDED.capabilities,
		  max_concurrency = EXCLUDED.max_concurrency,
		  labels = EXCLUDED.labels,
		  status = EXCLUDED.status,
		  inflight_executions = 0,
		  last_heartbeat_at = EXCLUDED.last_heartbeat_at,
		  updated_at = EXCLUDED.updated_at,
		  gateway_instance_id = EXCLUDED.gateway_instance_id
		RETURNING id, capabilities, max_concurrency, labels, status, inflight_executions,
		  registered_at, last_heartbeat_at, updated_at
	`, normalized.WorkerID, capabilitiesJSON, normalized.MaxConcurrency, labelsJSON, string(workers.StatusReady), now, s.cfg.InstanceID)

	return scanWorker(row)
}

func (s *Store) Heartbeat(ctx context.Context, input workers.HeartbeatInput) (workers.Worker, error) {
	normalized, err := normalizeHeartbeatInput(input)
	if err != nil {
		return workers.Worker{}, err
	}

	now := time.Now().UTC()
	row := s.pg.QueryRow(ctx, `
		UPDATE workers
		SET status = $2,
		    inflight_executions = $3,
		    last_heartbeat_at = $4,
		    updated_at = $4,
		    gateway_instance_id = $5
		WHERE id = $1
		RETURNING id, capabilities, max_concurrency, labels, status, inflight_executions,
		  registered_at, last_heartbeat_at, updated_at
	`, normalized.WorkerID, string(normalized.Status), normalized.InflightExecutions, now, s.cfg.InstanceID)

	worker, err := scanWorker(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return workers.Worker{}, workers.ErrWorkerNotFound
	}
	return worker, err
}

func (s *Store) GetWorker(ctx context.Context, workerID string) (workers.Worker, bool, error) {
	row := s.pg.QueryRow(ctx, `
		SELECT id, capabilities, max_concurrency, labels, status, inflight_executions,
		  registered_at, last_heartbeat_at, updated_at
		FROM workers
		WHERE id = $1
	`, strings.TrimSpace(workerID))
	worker, err := scanWorker(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return workers.Worker{}, false, nil
	}
	if err != nil {
		return workers.Worker{}, false, err
	}
	return worker, true, nil
}

func (s *Store) getJobByIdempotencyKey(ctx context.Context, idempotencyKey string) (jobs.Job, string, error) {
	row := s.pg.QueryRow(ctx, `
		SELECT id, job_type, payload, status, priority, COALESCE(idempotency_key, ''),
		  metadata, retry_max_attempts, retry_initial_backoff_seconds, retry_max_backoff_seconds,
		  request_fingerprint, created_at, updated_at
		FROM jobs
		WHERE idempotency_key = $1
	`, idempotencyKey)
	return scanJob(row)
}

func (s *Store) getJobByID(ctx context.Context, jobID string) (jobs.Job, string, error) {
	row := s.pg.QueryRow(ctx, `
		SELECT id, job_type, payload, status, priority, COALESCE(idempotency_key, ''),
		  metadata, retry_max_attempts, retry_initial_backoff_seconds, retry_max_backoff_seconds,
		  request_fingerprint, created_at, updated_at
		FROM jobs
		WHERE id = $1
	`, jobID)
	return scanJob(row)
}

func (s *Store) claimExecution(ctx context.Context, executionID, workerID string, leaseDuration time.Duration) (*jobs.Assignment, error) {
	tx, err := s.pg.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin claim tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	execution, job, _, err := s.loadExecutionAndJobForUpdate(ctx, tx, executionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	if execution.Status != jobs.ExecutionStatusQueued || execution.AvailableAt.After(now) {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit skipped claim tx: %w", err)
		}
		return nil, nil
	}
	if job.Status != jobs.StatusPending {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit skipped claim tx: %w", err)
		}
		return nil, nil
	}

	leaseExpiresAt := now.Add(leaseDuration)
	result, err := tx.Exec(ctx, `
		UPDATE executions
		SET status = $2,
		    worker_id = $3,
		    claimed_at = $4,
		    started_at = $4,
		    lease_expires_at = $5,
		    updated_at = $4
		WHERE id = $1 AND status = $6
	`, executionID, jobs.ExecutionStatusClaimed, workerID, now, leaseExpiresAt, jobs.ExecutionStatusQueued)
	if err != nil {
		return nil, fmt.Errorf("claim execution: %w", err)
	}
	if result.RowsAffected() == 0 {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit stale claim tx: %w", err)
		}
		return nil, nil
	}

	_, err = tx.Exec(ctx, `
		UPDATE jobs
		SET status = $2, updated_at = $3
		WHERE id = $1 AND status = $4
	`, job.ID, string(jobs.StatusDispatching), now, string(jobs.StatusPending))
	if err != nil {
		return nil, fmt.Errorf("update dispatching job: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit claim tx: %w", err)
	}

	return &jobs.Assignment{
		ExecutionID:    execution.ID,
		JobID:          job.ID,
		JobType:        job.JobType,
		Payload:        append([]byte(nil), job.Payload...),
		Attempt:        execution.Attempt,
		LeaseExpiresAt: leaseExpiresAt,
		IdempotencyKey: job.IdempotencyKey,
	}, nil
}

func (s *Store) loadExecutionAndJobForUpdate(ctx context.Context, tx pgx.Tx, executionID string) (jobs.Execution, jobs.Job, string, error) {
	row := tx.QueryRow(ctx, `
		SELECT e.id, e.job_id, e.attempt, e.status, e.worker_id, e.error_message,
		  e.claimed_at, e.started_at, e.finished_at, e.available_at, e.lease_expires_at,
		  j.id, j.job_type, j.payload, j.status, j.priority, COALESCE(j.idempotency_key, ''),
		  j.metadata, j.retry_max_attempts, j.retry_initial_backoff_seconds, j.retry_max_backoff_seconds,
		  j.request_fingerprint, j.created_at, j.updated_at
		FROM executions e
		JOIN jobs j ON j.id = e.job_id
		WHERE e.id = $1
		FOR UPDATE OF e, j
	`, executionID)

	var (
		execution           jobs.Execution
		job                 jobs.Job
		metadata            []byte
		fingerprint         string
		retryInitialSeconds int64
		retryMaxSeconds     int64
		claimedAt           *time.Time
		startedAt           *time.Time
		finishedAt          *time.Time
		leaseExpiresAt      *time.Time
	)
	if err := row.Scan(
		&execution.ID, &execution.JobID, &execution.Attempt, &execution.Status, &execution.WorkerID, &execution.ErrorMessage,
		&claimedAt, &startedAt, &finishedAt, &execution.AvailableAt, &leaseExpiresAt,
		&job.ID, &job.JobType, &job.Payload, &job.Status, &job.Priority, &job.IdempotencyKey,
		&metadata, &job.RetryPolicy.MaxAttempts, &retryInitialSeconds, &retryMaxSeconds,
		&fingerprint, &job.CreatedAt, &job.UpdatedAt,
	); err != nil {
		return jobs.Execution{}, jobs.Job{}, "", err
	}
	if err := json.Unmarshal(metadata, &job.Metadata); err != nil {
		return jobs.Execution{}, jobs.Job{}, "", fmt.Errorf("decode job metadata: %w", err)
	}
	job.RetryPolicy.InitialBackoff = time.Duration(retryInitialSeconds) * time.Second
	job.RetryPolicy.MaxBackoff = time.Duration(retryMaxSeconds) * time.Second
	execution.ClaimedAt = claimedAt
	execution.StartedAt = startedAt
	execution.FinishedAt = finishedAt
	execution.LeaseExpiresAt = leaseExpiresAt
	return execution, job, fingerprint, nil
}

func scanJob(row pgx.Row) (jobs.Job, string, error) {
	var (
		job                jobs.Job
		metadata           []byte
		requestFingerprint string
		retryInitialSecs   int64
		retryMaxSecs       int64
	)

	if err := row.Scan(
		&job.ID, &job.JobType, &job.Payload, &job.Status, &job.Priority, &job.IdempotencyKey,
		&metadata, &job.RetryPolicy.MaxAttempts, &retryInitialSecs, &retryMaxSecs,
		&requestFingerprint, &job.CreatedAt, &job.UpdatedAt,
	); err != nil {
		return jobs.Job{}, "", err
	}

	if err := json.Unmarshal(metadata, &job.Metadata); err != nil {
		return jobs.Job{}, "", fmt.Errorf("decode job metadata: %w", err)
	}

	job.RetryPolicy.InitialBackoff = time.Duration(retryInitialSecs) * time.Second
	job.RetryPolicy.MaxBackoff = time.Duration(retryMaxSecs) * time.Second
	job.Payload = append([]byte(nil), job.Payload...)
	return job, requestFingerprint, nil
}

func scanExecution(row interface{ Scan(...any) error }) (jobs.Execution, error) {
	var (
		execution      jobs.Execution
		claimedAt      *time.Time
		startedAt      *time.Time
		finishedAt     *time.Time
		leaseExpiresAt *time.Time
	)

	if err := row.Scan(
		&execution.ID, &execution.JobID, &execution.Attempt, &execution.Status, &execution.WorkerID, &execution.ErrorMessage,
		&claimedAt, &startedAt, &finishedAt, &execution.AvailableAt, &leaseExpiresAt,
	); err != nil {
		return jobs.Execution{}, err
	}

	execution.ClaimedAt = claimedAt
	execution.StartedAt = startedAt
	execution.FinishedAt = finishedAt
	execution.LeaseExpiresAt = leaseExpiresAt
	return execution, nil
}

func scanWorker(row pgx.Row) (workers.Worker, error) {
	var (
		worker       workers.Worker
		capabilities []byte
		labels       []byte
		status       string
	)

	if err := row.Scan(
		&worker.ID, &capabilities, &worker.MaxConcurrency, &labels, &status, &worker.InflightExecutions,
		&worker.RegisteredAt, &worker.LastHeartbeatAt, &worker.UpdatedAt,
	); err != nil {
		return workers.Worker{}, err
	}

	if err := json.Unmarshal(capabilities, &worker.Capabilities); err != nil {
		return workers.Worker{}, fmt.Errorf("decode worker capabilities: %w", err)
	}
	if err := json.Unmarshal(labels, &worker.Labels); err != nil {
		return workers.Worker{}, fmt.Errorf("decode worker labels: %w", err)
	}
	worker.Status = workers.Status(status)
	return worker, nil
}

func (s *Store) readyQueueKey(jobType string) string {
	return s.capabilityQueueKey(jobNamespace(jobType))
}

func (s *Store) capabilityQueueKey(capability string) string {
	return fmt.Sprintf("%s:%s", s.cfg.ReadyQueuePrefix, jobNamespace(capability))
}

func normalizeSubmitInput(input jobs.SubmitJobInput) (jobs.SubmitJobInput, error) {
	input.JobType = strings.TrimSpace(input.JobType)
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)

	if input.JobType == "" {
		return jobs.SubmitJobInput{}, fmt.Errorf("%w: job_type is required", jobs.ErrInvalidArgument)
	}
	if len(input.Payload) == 0 {
		return jobs.SubmitJobInput{}, fmt.Errorf("%w: payload is required", jobs.ErrInvalidArgument)
	}
	if input.Priority < 0 {
		return jobs.SubmitJobInput{}, fmt.Errorf("%w: priority must be greater than or equal to zero", jobs.ErrInvalidArgument)
	}

	retryPolicy, err := normalizeRetryPolicy(input.RetryPolicy)
	if err != nil {
		return jobs.SubmitJobInput{}, err
	}

	input.Payload = append([]byte(nil), input.Payload...)
	input.Metadata = cloneMetadata(input.Metadata)
	input.RetryPolicy = retryPolicy
	return input, nil
}

func normalizeRetryPolicy(policy jobs.RetryPolicy) (jobs.RetryPolicy, error) {
	if policy.MaxAttempts == 0 && policy.InitialBackoff == 0 && policy.MaxBackoff == 0 {
		return jobs.RetryPolicy{
			MaxAttempts:    3,
			InitialBackoff: 5 * time.Second,
			MaxBackoff:     time.Minute,
		}, nil
	}
	if policy.MaxAttempts <= 0 {
		return jobs.RetryPolicy{}, fmt.Errorf("%w: retry_policy.max_attempts must be greater than zero", jobs.ErrInvalidArgument)
	}
	if policy.InitialBackoff < 0 || policy.MaxBackoff < 0 {
		return jobs.RetryPolicy{}, fmt.Errorf("%w: retry_policy backoff values must be non-negative", jobs.ErrInvalidArgument)
	}
	if policy.MaxBackoff > 0 && policy.InitialBackoff > policy.MaxBackoff {
		return jobs.RetryPolicy{}, fmt.Errorf("%w: retry_policy.max_backoff must be greater than or equal to initial_backoff", jobs.ErrInvalidArgument)
	}
	if policy.InitialBackoff == 0 {
		policy.InitialBackoff = 5 * time.Second
	}
	if policy.MaxBackoff == 0 {
		policy.MaxBackoff = policy.InitialBackoff
	}
	return policy, nil
}

func normalizeRegisterInput(input workers.RegisterInput) (workers.RegisterInput, error) {
	input.WorkerID = strings.TrimSpace(input.WorkerID)
	if input.WorkerID == "" {
		return workers.RegisterInput{}, fmt.Errorf("%w: worker_id is required", workers.ErrInvalidArgument)
	}
	if input.MaxConcurrency <= 0 {
		input.MaxConcurrency = 1
	}
	input.Capabilities = normalizeCapabilities(input.Capabilities)
	input.Labels = cloneMetadata(input.Labels)
	return input, nil
}

func normalizeHeartbeatInput(input workers.HeartbeatInput) (workers.HeartbeatInput, error) {
	input.WorkerID = strings.TrimSpace(input.WorkerID)
	if input.WorkerID == "" {
		return workers.HeartbeatInput{}, fmt.Errorf("%w: worker_id is required", workers.ErrInvalidArgument)
	}
	if input.InflightExecutions < 0 {
		return workers.HeartbeatInput{}, fmt.Errorf("%w: inflight_executions must be greater than or equal to zero", workers.ErrInvalidArgument)
	}
	if input.Status == "" {
		input.Status = workers.StatusUnspecified
	}
	return input, nil
}

func normalizeClaimInput(input jobs.ClaimForWorkerInput) jobs.ClaimForWorkerInput {
	input.WorkerID = strings.TrimSpace(input.WorkerID)
	if input.MaxConcurrency <= 0 {
		input.MaxConcurrency = 1
	}
	if input.InflightExecutions < 0 {
		input.InflightExecutions = 0
	}
	input.Capabilities = normalizeCapabilities(input.Capabilities)
	return input
}

func normalizeCapabilities(capabilities []string) []string {
	if len(capabilities) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(capabilities))
	normalized := make([]string, 0, len(capabilities))
	for _, capability := range capabilities {
		namespace := jobNamespace(capability)
		if namespace == "" {
			continue
		}
		if _, ok := seen[namespace]; ok {
			continue
		}
		seen[namespace] = struct{}{}
		normalized = append(normalized, namespace)
	}
	sort.Strings(normalized)
	return normalized
}

func jobNamespace(jobType string) string {
	jobType = strings.TrimSpace(jobType)
	if jobType == "" {
		return ""
	}
	if index := strings.Index(jobType, "."); index > 0 {
		return jobType[:index]
	}
	return jobType
}

func retryDelay(policy jobs.RetryPolicy, nextAttempt int32) time.Duration {
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

func buildFingerprint(input jobs.SubmitJobInput) string {
	hash := sha256.New()
	hash.Write([]byte(input.JobType))
	hash.Write(input.Payload)
	_, _ = fmt.Fprintf(hash, "|%d|%s|%d|%d|%d|",
		input.Priority,
		input.IdempotencyKey,
		input.RetryPolicy.MaxAttempts,
		input.RetryPolicy.InitialBackoff.Nanoseconds(),
		input.RetryPolicy.MaxBackoff.Nanoseconds(),
	)

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

func marshalJSON(value any) ([]byte, error) {
	if value == nil {
		return []byte("{}"), nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal json: %w", err)
	}
	return data, nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func newJobID() string {
	return "job_" + randomHex(16)
}

func newExecutionID() string {
	return "exec_" + randomHex(16)
}

func randomHex(size int) string {
	raw := make([]byte, size)
	if _, err := rand.Read(raw); err != nil {
		panic(err)
	}
	return hex.EncodeToString(raw)
}
