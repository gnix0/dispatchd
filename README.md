# task-orchestrator

Distributed task orchestration platform built in Go with gRPC, Docker, Kubernetes, and GitHub Actions. This repository is structured to grow through small, reviewable branches instead of large one-shot drops.

## Current Scope

The current foundation covers:

- the Go module and service entrypoints
- a shared runtime/config foundation
- refined protobuf contracts for jobs, executions, retries, and worker streams
- generated Go stubs checked into `gen/go`
- gRPC server setup with standard health and reflection enabled
- local development commands through `make`
- the first GitHub Actions CI workflow

Business behavior, persistence, scheduler loops, real worker orchestration, and Kubernetes deployment manifests land in follow-up branches.

## Planned Services

- `control-plane`: public gRPC API for job submission and query flows
- `scheduler`: execution claiming, retry policy, and dispatch orchestration
- `worker-gateway`: bidirectional worker stream lifecycle and task delivery

## Repository Layout

```text
cmd/                  service entrypoints
internal/platform/    shared runtime and configuration helpers
internal/transport/   gRPC transport handlers and registration
internal/version/     build metadata
gen/go/               generated protobuf and gRPC stubs
proto/                protobuf contracts
scripts/              local developer commands
tools/proto/          Dockerized protobuf toolchain
```

## Quick Start

```bash
make fmt-check
make test
make build
```

Generate protobuf code with Docker:

```bash
make proto
make proto-check
```

If you want to compare schema changes against the current `main` branch baseline explicitly, run:

```bash
make proto-breaking
```

The module path is currently `github.com/gnix0/task-orchestrator`. If you publish under a different GitHub namespace later, update `go.mod` and the `go_package` option in the protobuf files before committing generated stubs.

## Branching Strategy

The intended workflow is one focused branch per slice, for example:

1. `feat/bootstrap-foundation`
2. `feat/protobuf-contracts`
3. `feat/control-plane-submit-flow`
4. `feat/worker-stream-foundation`
5. `feat/scheduler-retry-engine`
6. `feat/local-platform-k8s`
7. `feat/ci-release-automation`

Each branch should produce one coherent PR with passing tests and updated docs where needed.
