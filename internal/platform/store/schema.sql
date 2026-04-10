CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  job_type TEXT NOT NULL,
  payload BYTEA NOT NULL,
  status TEXT NOT NULL,
  priority INTEGER NOT NULL,
  idempotency_key TEXT UNIQUE,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  retry_max_attempts INTEGER NOT NULL,
  retry_initial_backoff_seconds BIGINT NOT NULL,
  retry_max_backoff_seconds BIGINT NOT NULL,
  request_fingerprint TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_jobs_status_priority_created_at
  ON jobs (status, priority DESC, created_at ASC);

CREATE TABLE IF NOT EXISTS executions (
  id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
  attempt INTEGER NOT NULL,
  status TEXT NOT NULL,
  worker_id TEXT NOT NULL DEFAULT '',
  error_message TEXT NOT NULL DEFAULT '',
  claimed_at TIMESTAMPTZ,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  available_at TIMESTAMPTZ NOT NULL,
  lease_expires_at TIMESTAMPTZ,
  enqueued_at TIMESTAMPTZ,
  result_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  UNIQUE (job_id, attempt)
);

CREATE INDEX IF NOT EXISTS idx_executions_status_available_at
  ON executions (status, available_at ASC);

CREATE INDEX IF NOT EXISTS idx_executions_worker_status
  ON executions (worker_id, status, lease_expires_at);

CREATE TABLE IF NOT EXISTS workers (
  id TEXT PRIMARY KEY,
  capabilities JSONB NOT NULL DEFAULT '[]'::jsonb,
  max_concurrency INTEGER NOT NULL,
  labels JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL,
  inflight_executions INTEGER NOT NULL DEFAULT 0,
  registered_at TIMESTAMPTZ NOT NULL,
  last_heartbeat_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  gateway_instance_id TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_workers_status_heartbeat
  ON workers (status, last_heartbeat_at);
