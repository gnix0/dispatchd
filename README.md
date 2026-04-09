# task-orchestrator

Distributed task orchestration platform in Go built around gRPC contracts, containerized delivery, Kustomize-based GitOps, and an Argo CD deployment path.

## Architecture

The system is organized around three runtime roles:

- `control-plane`: accepts job submissions and exposes query APIs
- `scheduler`: claims runnable executions, applies retry policy, and decides terminal failure
- `worker-gateway`: manages bidirectional worker streams for registration, heartbeats, and task feedback

The core domain model is centered on `Job` and `Execution` records:

- a submitted job creates an initial queued execution
- the scheduler claims runnable executions with a lease window
- failed executions are retried with bounded exponential backoff
- jobs transition to a terminal dead-letter path after the retry policy is exhausted

## Runtime Model

### Contracts

The protobuf module under `proto/` defines the external system contract.

- `JobService` exposes submission, cancellation, lookup, and execution history
- `WorkerService` defines the worker stream protocol
- generated protobuf and gRPC stubs are committed under `gen/go`

Key message families:

- `Job`: submitted work, metadata, priority, and retry policy
- `Execution`: an individual attempt for a job
- `RetryPolicy`: max attempts plus backoff configuration
- `ConnectRequest` / `ConnectResponse`: worker registration, heartbeat, and acknowledgement stream frames

### Transport

The gRPC transport layer lives under `internal/transport/grpcapi`.

- request payloads are mapped into application-layer inputs
- application errors are translated into explicit gRPC status codes
- reflection and standard gRPC health services are registered centrally

### Application Services

The application layer lives under `internal/application/`.

- `jobs`: job creation, idempotency, execution tracking, and scheduler-facing execution state transitions
- `scheduler`: polling, claiming, retry scheduling, and dead-letter decisions
- `workers`: worker registration, heartbeat state, capabilities, labels, and concurrency metadata

The current reference runtime uses in-memory state so the orchestration rules stay isolated from persistence concerns. The deployment topology, CI/CD flow, and security controls are production-shaped; replacing the in-memory stores with shared persistence is the next infrastructure-level evolution, not an architectural rewrite.

## Platform And Delivery

### Containers

- a single multi-stage [Dockerfile](/home/gnix0/developer/task-orchestrator/Dockerfile) builds any service binary through the `SERVICE` build argument
- [docker-compose.yml](/home/gnix0/developer/task-orchestrator/docker-compose.yml) packages the three services for local container execution

### Kubernetes

- [deploy/base](/home/gnix0/developer/task-orchestrator/deploy/base) contains reusable Deployments, Services, ConfigMap, ServiceAccount, and NetworkPolicy resources
- [deploy/overlays/dev](/home/gnix0/developer/task-orchestrator/deploy/overlays/dev) defines the development overlay used by local `kind` clusters and future GitOps updates
- [deploy/kind/cluster.yaml](/home/gnix0/developer/task-orchestrator/deploy/kind/cluster.yaml) maps NodePorts for local access to the control-plane and worker-gateway

### GitOps

- image tags are managed through the Kustomize `images` section in the dev overlay
- [scripts/update-dev-image-tags.sh](/home/gnix0/developer/task-orchestrator/scripts/update-dev-image-tags.sh) updates the overlay tags for automated GitOps PRs
- the release workflow publishes images and opens a PR with updated deployment tags instead of mutating manifests directly on the default branch

### Argo CD

- [deploy/argocd/dev-application.yaml](/home/gnix0/developer/task-orchestrator/deploy/argocd/dev-application.yaml) defines the dev `Application`
- [deploy/argocd/project.yaml](/home/gnix0/developer/task-orchestrator/deploy/argocd/project.yaml) defines the Argo CD project boundaries
- Argo CD targets the `deploy/overlays/dev` path and can self-heal/prune once the repo is connected

If you fork or rename the repository, update the Argo CD `repoURL` fields to match the canonical Git URL for your deployment source.

## DevSecOps

The repository includes a dedicated delivery/security layer:

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
internal/application/ application use cases
internal/platform/    shared runtime, config, and gRPC server helpers
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
```

Kubernetes workflows:

```bash
make k8s-render
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
