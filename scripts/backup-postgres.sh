#!/bin/sh

set -eu

OUTPUT_DIR="${1:-backups}"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"

mkdir -p "${OUTPUT_DIR}"

docker compose exec -T postgres pg_dump -U postgres task_orchestrator > "${OUTPUT_DIR}/task_orchestrator_${TIMESTAMP}.sql"

printf 'postgres backup written to %s\n' "${OUTPUT_DIR}/task_orchestrator_${TIMESTAMP}.sql"
