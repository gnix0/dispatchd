#!/bin/sh

set -eu

PROFILE="${1:-smoke}"
REPO_ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
RESULTS_DIR="${REPO_ROOT}/perf/results"
OUTPUT_PATH="${RESULTS_DIR}/worker-heartbeats-${PROFILE}.json"

mkdir -p "${RESULTS_DIR}"

case "${PROFILE}" in
  smoke)
    WORKERS=12
    DURATION=30s
    INTERVAL=2s
    ;;
  average)
    WORKERS=48
    DURATION=5m
    INTERVAL=1s
    ;;
  stress)
    WORKERS=96
    DURATION=10m
    INTERVAL=500ms
    ;;
  *)
    echo "unknown worker perf profile: ${PROFILE}" >&2
    exit 1
    ;;
esac

docker build --build-arg SERVICE=perf-worker -t task-orchestrator-perf-worker:local "${REPO_ROOT}"

docker run --rm \
  --network host \
  --user "$(id -u):$(id -g)" \
  -v "${REPO_ROOT}:/workspace" \
  -w /workspace \
  task-orchestrator-perf-worker:local \
  -profile "${PROFILE}" \
  -target "${WORKER_GATEWAY_TARGET:-127.0.0.1:8081}" \
  -workers "${PERF_WORKERS:-${WORKERS}}" \
  -duration "${PERF_DURATION:-${DURATION}}" \
  -heartbeat-interval "${PERF_HEARTBEAT_INTERVAL:-${INTERVAL}}" \
  -output "/workspace/perf/results/worker-heartbeats-${PROFILE}.json"

printf 'wrote %s\n' "${OUTPUT_PATH}"
