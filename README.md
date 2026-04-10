# dispatchd

Distributed task orchestration platform in Go built around gRPC contracts, shared Postgres and Redis state, Kustomize-based GitOps delivery, and an Argo CD deployment path.

## Versioning

`dispatchd` uses Semantic Versioning for repository releases.

- `0.x.y` covers the current hardening phase, where the public API and operational model can still evolve.
- patch releases are for backward-compatible fixes
- minor releases are for backward-compatible capabilities and hardening milestones
- `1.0.0` is reserved for the point where the documented public API and operating model are stable enough for real production adoption

Released tags are immutable. Once a version is published, any correction ships as a new version instead of mutating the existing tag.

## Public API Surface

The release contract for `dispatchd` currently includes:

- the protobuf and gRPC contract under `proto/dispatchd/v1`
- the generated Go client/server stubs under `gen/go/dispatchd`
- the documented operator-facing environment and deployment shape used by Docker Compose, Kustomize, Argo CD, and GitHub Actions
- the published image naming scheme under `ghcr.io/gnix0/dispatchd-*`

Until `1.0.0`, these surfaces may still change, but any change that affects compatibility should be called out in release notes and pull requests.

## Architecture

The runtime is organized around three service roles:

- `control-plane`: accepts job submissions, serves query APIs, and persists job state
- `scheduler`: reconciles runnable and expired executions, maintains queue readiness, and holds the active scheduling lease
- `worker-gateway`: manages bidirectional worker streams for registration, heartbeats, assignment delivery, and result feedback

The core domain is centered on durable `Job` and `Execution` records:

- a submitted job creates an initial queued execution
- the scheduler moves runnable executions into Redis-backed ready queues
- workers receive assignments through the gRPC stream and renew execution leases through heartbeats
- failed executions are retried with bounded exponential backoff
- retry exhaustion produces a terminal dead-lettered execution

## Runtime Model

### Contracts

The protobuf module under `proto/` defines the external system contract.

- `JobService` exposes submission, cancellation, lookup, and execution history
- `WorkerService` defines the worker control stream used for registration, heartbeat, assignment delivery, results, and acknowledgements
- generated protobuf and gRPC stubs are committed under `gen/go`

Key message families:

- `Job`: submitted work, metadata, priority, and retry policy
- `Execution`: an individual attempt for a job
- `RetryPolicy`: max attempts plus backoff configuration
- `ConnectRequest` / `ConnectResponse`: worker control-plane stream messages

### Shared State

Shared orchestration state is split across two storage layers:

- `Postgres`: source of truth for jobs, executions, retries, idempotency, worker registry data, and execution metadata
- `Redis`: scheduler leadership lock and ready queues keyed by capability namespace

This allows the control-plane, scheduler, and worker-gateway to coordinate as separate processes instead of relying on process-local memory.

### Transport

The gRPC transport layer lives under `internal/transport/grpcapi`.

- request payloads are mapped into application-layer inputs
- application errors are translated into explicit gRPC status codes
- unary and stream interceptors can enforce JWT-based authentication and role-based authorization
- worker stream messages validate worker identity against the authenticated principal when security is enabled
- reflection and standard gRPC health services are registered centrally

### Application Services

The application layer lives under `internal/application/`.

- `jobs`: job creation, idempotency, execution tracking, completion handling, retry scheduling, and dead-letter decisions
- `scheduler`: single-leader reconciliation of expired leases and runnable execution queueing
- `workers`: worker registration, heartbeat state, capabilities, labels, concurrency metadata, and dispatch eligibility

## Security

The repository includes a zero-trust-ready foundation that can be enabled without changing the public API shape:

- JWT authentication and role-based authorization at the gRPC interceptor layer
- worker identity validation on the bidirectional worker stream
- audit logging for authenticated gRPC operations
- TLS and client-certificate validation boundaries in the server bootstrap path
- Kubernetes secret mounts for JWT material and TLS certificates

Default local development keeps:

- `AUTH_ENABLED=false`
- `TLS_ENABLED=false`

Security-relevant role mapping is:

- `submitter`: submit jobs
- `viewer`: query jobs and execution history
- `operator`: submit, query, cancel, and operate service paths
- `worker` / `service`: connect through the worker stream
- `admin`: unrestricted gRPC access

## Platform And Delivery

### Containers

- a single multi-stage [Dockerfile](/home/gnix0/developer/dispatchd/Dockerfile) builds any service binary through the `SERVICE` build argument
- [docker-compose.yml](/home/gnix0/developer/dispatchd/docker-compose.yml) packages the three services together with Postgres and Redis for local distributed execution

### Kubernetes

- [deploy/base](/home/gnix0/developer/dispatchd/deploy/base) contains reusable Deployments, Services, ConfigMap, Secrets, ServiceAccount, and NetworkPolicy resources
- [deploy/overlays/dev](/home/gnix0/developer/dispatchd/deploy/overlays/dev) defines the development overlay used by local `kind` clusters and GitOps updates
- [deploy/overlays/staging](/home/gnix0/developer/dispatchd/deploy/overlays/staging) defines a promotion-oriented staging overlay
- [deploy/overlays/prod](/home/gnix0/developer/dispatchd/deploy/overlays/prod) defines a production-oriented overlay and PodDisruptionBudgets
- [deploy/kind/cluster.yaml](/home/gnix0/developer/dispatchd/deploy/kind/cluster.yaml) maps NodePorts for local access to the control-plane and worker-gateway

### GitOps

- image tags are managed through the Kustomize `images` section in the dev overlay
- [scripts/update-dev-image-tags.sh](/home/gnix0/developer/dispatchd/scripts/update-dev-image-tags.sh) updates the dev overlay tags for automated GitOps PRs
- the release workflow publishes images from semantic version tags and opens a PR with updated deployment tags instead of mutating manifests directly on the default branch

### Argo CD

- [deploy/argocd/dev-application.yaml](/home/gnix0/developer/dispatchd/deploy/argocd/dev-application.yaml) defines the dev `Application`
- [deploy/argocd/project.yaml](/home/gnix0/developer/dispatchd/deploy/argocd/project.yaml) defines the Argo CD project boundaries
- Argo CD targets the `deploy/overlays/dev` path and can self-heal and prune once the repo is connected

If you fork or rename the repository, update the Argo CD `repoURL` fields to match the canonical Git URL for your deployment source.

## Governance

- contribution expectations are documented in [CONTRIBUTING.md](/home/gnix0/developer/dispatchd/CONTRIBUTING.md)
- security intake is documented in [.github/SECURITY.md](/home/gnix0/developer/dispatchd/.github/SECURITY.md)
- release history is tracked in [CHANGELOG.md](/home/gnix0/developer/dispatchd/CHANGELOG.md)
- repository ownership defaults are declared in [.github/CODEOWNERS](/home/gnix0/developer/dispatchd/.github/CODEOWNERS)

## High Availability And DR

The repository now carries the first HA/DR-oriented operational assets:

- scheduler leadership is coordinated through Redis so only one active scheduler instance reconciles work per environment
- staging and production Kustomize overlays separate promotion targets from development
- production PodDisruptionBudgets keep the core services available during voluntary disruptions
- Postgres backup and restore helpers live in [backup-postgres.sh](/home/gnix0/developer/dispatchd/scripts/backup-postgres.sh) and [restore-postgres.sh](/home/gnix0/developer/dispatchd/scripts/restore-postgres.sh)
- a scheduler restart drill lives in [failover-smoke.sh](/home/gnix0/developer/dispatchd/scripts/failover-smoke.sh)

This repository models single-region active leadership today. The config boundaries and overlays are structured so failover and environment promotion can be exercised explicitly rather than hidden in manual operator steps.

## Performance & Reliability

The repository includes a Dockerized performance and observability workflow built around:

- Prometheus metrics scraping
- Grafana dashboards
- Jaeger trace collection through OTLP
- `k6` gRPC load generation for the control-plane unary path
- a dedicated `perf-worker` load client for worker registration and heartbeat pressure

Reference evidence for the current branch lives under [assets/perf](/home/gnix0/developer/dispatchd/assets/perf) and [perf/results](/home/gnix0/developer/dispatchd/perf/results).

Supported SLOs for the current evidence set:

- control-plane unary gRPC availability: `>= 99.9%`
- control-plane unary gRPC latency: `p95 < 50 ms` in the reference Docker Compose environment
- worker heartbeat acknowledgement latency: `p95 < 50 ms`
- worker heartbeat error rate: `< 0.1%`

Measured SLIs from the captured runs:

- control-plane smoke run: `5548` iterations, `100%` checks passed, `grpc_req_duration avg 5.16 ms`, `p95 13.30 ms`
- control-plane stress run: `266330` iterations, `100%` checks passed, `443.81 iterations/s`, `grpc_req_duration avg 16.71 ms`, `p95 43.74 ms`
- worker heartbeat smoke run: `180` heartbeats, `0` errors, `avg ack 9.67 ms`, `p95 12.85 ms`, `p99 14.33 ms`
- worker heartbeat stress run: `112801` heartbeats, `0` errors, `avg ack 10.34 ms`, `p95 28.27 ms`, `p99 54.04 ms`

Evidence currently checked into the repository includes:

- Grafana request-rate and latency captures such as [grafana_grpc_req_rate.png](/home/gnix0/developer/dispatchd/assets/perf/grafana_grpc_req_rate.png) and [grafana_grpc_p95_latency.png](/home/gnix0/developer/dispatchd/assets/perf/grafana_grpc_p95_latency.png)
- worker and scheduler activity captures such as [grafana_worker_stream_events.png](/home/gnix0/developer/dispatchd/assets/perf/grafana_worker_stream_events.png), [grafana_dispatch_events.png](/home/gnix0/developer/dispatchd/assets/perf/grafana_dispatch_events.png), and [grafana_scheduler_ticks_avg_duration.png](/home/gnix0/developer/dispatchd/assets/perf/grafana_scheduler_ticks_avg_duration.png)

The currently published SLI set is intentionally limited to the portions of the system that were directly measured in the checked-in evidence. It does not claim submit-to-assignment latency, end-to-end execution latency, or retry/dead-letter timing because those were not benchmarked explicitly in this evidence set.

## DevSecOps

The repository includes a dedicated delivery and security layer:

- `SAST`: CodeQL for static analysis of the Go codebase
- `SCA`: `govulncheck`, Dependabot, and repository-level Trivy filesystem scanning
- `Container Scanning`: Trivy image scanning across the service images
- `Secret Leakage Detection`: Gitleaks plus Trivy secret scanning
- `DAST`: OWASP ZAP baseline scans against an explicitly provided environment URL

Because the core services expose gRPC rather than a browser-oriented HTTP application surface, DAST is modeled as an environment-level scan against an ingress, gateway, or externally exposed endpoint rather than as a local repo-only check.

## Repository Layout

```text
cmd/                  service entrypoints
deploy/               Kubernetes base, overlays, kind, and Argo CD manifests
gen/go/               generated protobuf and gRPC stubs
integration/          distributed smoke and future environment-level tests
internal/application/ application use cases
internal/platform/    shared runtime, config, security, store, and gRPC helpers
internal/transport/   gRPC handlers and protocol mapping
proto/                protobuf contracts
scripts/              developer automation helpers
tools/proto/          Dockerized protobuf toolchain
```

## Local Workflows

Core validation:

```bash
make fmt-check
make test
make build
make lint
make proto-check
```

Distributed smoke:

```bash
DISPATCHD_INTEGRATION=1 go test ./integration -run TestDistributedSubmitAssignCompleteFlow -count=1 -v
```

Protobuf workflows:

```bash
make proto
make proto-breaking
```

Container workflows:

```bash
make compose-config
make compose-up
make compose-down
make backup-postgres
make failover-smoke
```

Observability and performance workflows:

```bash
make perf-stack-up
make perf-k6-smoke
make perf-worker-smoke
make perf-stack-down
```

Kubernetes workflows:

```bash
make k8s-render
make k8s-render-staging
make k8s-render-prod
make k8s-validate
make argocd-render
make kind-up
make kind-down
```

GitOps tag update helper:

```bash
make gitops-update-dev IMAGE_TAG=sha-1234567
```

The protobuf toolchain runs in Docker, so local `protoc` or `buf` installation is not required.
