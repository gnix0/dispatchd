# task-orchestrator

Distributed task orchestration platform in Go built around gRPC contracts, containerized protobuf tooling, and a Kubernetes-ready delivery path.

## What It Is

The system is designed around three runtime roles:

- `control-plane`: accepts job submissions and exposes query APIs
- `scheduler`: claims runnable work, applies retry policy, and coordinates dispatch
- `worker-gateway`: manages bidirectional worker streams for assignment, heartbeats, and task results

The current implementation focuses on the control-plane path so the API surface and transport layer can be exercised before persistence and scheduling are introduced.

## Current Behavior

Implemented today:

- gRPC contracts for jobs, executions, retry policy, and worker streaming
- generated Go protobuf/grpc stubs checked into `gen/go`
- gRPC server bootstrap with reflection and standard health endpoints
- in-memory control-plane flow for:
  - `SubmitJob`
  - `GetJob`
  - `CancelJob`
  - `ListExecutions` returning in-memory execution history
- in-memory worker registration and heartbeat flow over the gRPC stream
- in-memory scheduler polling and retry/dead-letter behavior
- request validation for required fields and retry policy shape
- idempotency-key handling on job submission

Current submit-flow semantics:

- `job_type` and `payload` are required
- `priority` must be `>= 0`
- if no retry policy is supplied, defaults are applied
- the same idempotency key with the same request returns the existing job
- the same idempotency key with a different request returns a conflict

## How It Works

### Contracts

The protobuf module under `proto/` defines the external system contract. The control-plane uses `JobService`, while worker lifecycle behavior is defined on `WorkerService`.

Key message families:

- `Job`: submitted work plus retry policy and metadata
- `Execution`: runtime attempt records for a job
- `RetryPolicy`: max attempts and backoff configuration
- `ConnectRequest` / `ConnectResponse`: streaming worker protocol

### Transport

The gRPC transport layer lives under `internal/transport/grpcapi`.

- request messages are mapped into application inputs
- application errors are translated into gRPC status codes
- health and reflection are registered centrally in the shared gRPC server bootstrap

### Application Layer

The first real use case is the in-memory job service under `internal/application/jobs`.

- jobs are stored in-process
- submission normalizes and validates input
- a request fingerprint is used to enforce idempotency-key consistency
- query and cancellation operate against the same in-memory store

This gives the control-plane a real execution path without forcing early database choices.

The worker gateway uses the in-memory worker service under `internal/application/workers`.

- registration records worker identity, capabilities, labels, and concurrency
- heartbeats update worker status and inflight execution count
- the stream returns acknowledgement frames for successful registration and heartbeat messages

The scheduler logic lives under `internal/application/scheduler` and works against the in-memory job store.

- job submission creates an initial queued execution
- the scheduler polling loop claims runnable executions using a lease duration
- execution failure either schedules a new attempt with exponential backoff or transitions to a terminal dead-lettered state
- execution history is visible through `ListExecutions`

## Repository Layout

```text
cmd/                  service entrypoints
gen/go/               generated protobuf and gRPC stubs
internal/application/ application use cases
internal/platform/    shared runtime, config, and gRPC server helpers
internal/transport/   gRPC handlers and protocol mapping
proto/                protobuf contracts
scripts/              developer commands
tools/proto/          Dockerized protobuf toolchain
```

## Running It

Core local checks:

```bash
make fmt-check
make test
make build
make lint
```

Protobuf workflow:

```bash
make proto
make proto-check
```

Optional compatibility check against the current `main` branch schema baseline:

```bash
make proto-breaking
```

The protobuf toolchain runs in Docker so local `protoc` or `buf` installation is not required.
