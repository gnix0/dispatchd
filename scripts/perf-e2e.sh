#!/bin/sh

set -eu

PROFILE="${1:-smoke}"
REPO_ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
OUTPUT_DIR="${REPO_ROOT}/evidence/performance"
OUTPUT_PATH="${OUTPUT_DIR}/end-to-end-${PROFILE}.json"

mkdir -p "${OUTPUT_DIR}"

case "${PROFILE}" in
  smoke)
    JOBS=20
    TIMEOUT=45s
    ;;
  average)
    JOBS=80
    TIMEOUT=90s
    ;;
  restart)
    JOBS=20
    TIMEOUT=90s
    ;;
  *)
    echo "unknown e2e profile: ${PROFILE}" >&2
    exit 1
    ;;
esac

go run ./cmd/perf-e2e \
  -profile "${PROFILE}" \
  -jobs "${PERF_E2E_JOBS:-${JOBS}}" \
  -timeout "${PERF_E2E_TIMEOUT:-${TIMEOUT}}" \
  -output "${OUTPUT_PATH}"

printf 'wrote %s\n' "${OUTPUT_PATH}"
