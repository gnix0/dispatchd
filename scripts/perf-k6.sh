#!/bin/sh

set -eu

PROFILE="${1:-smoke}"
REPO_ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
RESULTS_DIR="${REPO_ROOT}/perf/results"
OUTPUT_PATH="${RESULTS_DIR}/control-plane-${PROFILE}.json"

mkdir -p "${RESULTS_DIR}"

case "${PROFILE}" in
  smoke)
    VUS=12
    DURATION=30s
    ;;
  average)
    VUS=40
    DURATION=5m
    ;;
  stress)
    VUS=80
    DURATION=10m
    ;;
  *)
    echo "unknown k6 profile: ${PROFILE}" >&2
    exit 1
    ;;
esac

docker run --rm \
  --network host \
  --user "$(id -u):$(id -g)" \
  -v "${REPO_ROOT}:/workspace" \
  -w /workspace \
  -e CONTROL_PLANE_TARGET="${CONTROL_PLANE_TARGET:-127.0.0.1:8080}" \
  -e K6_VUS="${K6_VUS:-${VUS}}" \
  -e K6_DURATION="${K6_DURATION:-${DURATION}}" \
  -e K6_SLEEP_SECONDS="${K6_SLEEP_SECONDS:-0.05}" \
  grafana/k6:0.52.0 \
  run --summary-export "/workspace/perf/results/control-plane-${PROFILE}.json" /workspace/perf/k6/control_plane.js

printf 'wrote %s\n' "${OUTPUT_PATH}"
