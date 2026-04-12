#!/bin/sh

set -eu

REPO_ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
OUTPUT_DIR="${REPO_ROOT}/evidence/drills"
OUTPUT_PATH="${OUTPUT_DIR}/scheduler-restart-drill.json"
STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

mkdir -p "${OUTPUT_DIR}"

printf 'restarting scheduler to exercise leader reacquisition...\n'
docker compose restart scheduler >/dev/null

printf 'running post-restart end-to-end drill...\n'
go run ./cmd/perf-e2e \
  -profile restart \
  -jobs "${DRILL_JOBS:-20}" \
  -timeout "${DRILL_TIMEOUT:-90s}" \
  -output "${OUTPUT_PATH}"

printf 'scheduler restart drill started at %s and wrote %s\n' "${STARTED_AT}" "${OUTPUT_PATH}"
