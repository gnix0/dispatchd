SHELL := /bin/sh

CONTROL_PLANE_IMAGE := ghcr.io/gnix0/task-orchestrator-control-plane:dev
SCHEDULER_IMAGE := ghcr.io/gnix0/task-orchestrator-scheduler:dev
WORKER_GATEWAY_IMAGE := ghcr.io/gnix0/task-orchestrator-worker-gateway:dev
KIND_CLUSTER_NAME ?= task-orchestrator
IMAGE_TAG ?= dev

.PHONY: fmt fmt-check lint test build proto proto-check proto-breaking \
	docker-build-control-plane docker-build-scheduler docker-build-worker-gateway \
	compose-config compose-up compose-down k8s-render k8s-validate argocd-render \
	k8s-render-staging k8s-render-prod \
	kind-up kind-down gitops-update-dev backup-postgres restore-postgres failover-smoke \
	perf-stack-up perf-stack-down perf-k6-smoke perf-k6-average perf-k6-stress \
	perf-worker-smoke perf-worker-average perf-worker-stress

fmt:
	go fmt ./...

fmt-check:
	./scripts/check-gofmt.sh

lint:
	golangci-lint run ./...

test:
	go test ./...

build:
	go build ./...

proto:
	./scripts/proto.sh generate

proto-check:
	./scripts/proto.sh check

proto-breaking:
	./scripts/proto.sh breaking

docker-build-control-plane:
	docker build --build-arg SERVICE=control-plane -t "$(CONTROL_PLANE_IMAGE)" .

docker-build-scheduler:
	docker build --build-arg SERVICE=scheduler -t "$(SCHEDULER_IMAGE)" .

docker-build-worker-gateway:
	docker build --build-arg SERVICE=worker-gateway -t "$(WORKER_GATEWAY_IMAGE)" .

compose-config:
	docker compose config

compose-up:
	docker compose up --build -d

compose-down:
	docker compose down --remove-orphans

k8s-render:
	kubectl kustomize deploy/overlays/dev

k8s-render-staging:
	kubectl kustomize deploy/overlays/staging

k8s-render-prod:
	kubectl kustomize deploy/overlays/prod

k8s-validate:
	kubectl kustomize deploy/overlays/dev >/dev/null

argocd-render:
	kubectl kustomize deploy/argocd

gitops-update-dev:
	./scripts/update-dev-image-tags.sh "$(IMAGE_TAG)"

kind-up:
	./scripts/kind-up.sh

kind-down:
	./scripts/kind-down.sh

backup-postgres:
	./scripts/backup-postgres.sh

restore-postgres:
	./scripts/restore-postgres.sh

failover-smoke:
	./scripts/failover-smoke.sh

perf-stack-up:
	docker compose up --build -d

perf-stack-down:
	docker compose down --remove-orphans

perf-k6-smoke:
	./scripts/perf-k6.sh smoke

perf-k6-average:
	./scripts/perf-k6.sh average

perf-k6-stress:
	./scripts/perf-k6.sh stress

perf-worker-smoke:
	./scripts/perf-worker-load.sh smoke

perf-worker-average:
	./scripts/perf-worker-load.sh average

perf-worker-stress:
	./scripts/perf-worker-load.sh stress
