#!/bin/sh

set -eu

if [ "${#}" -ne 1 ]; then
  printf 'usage: %s <backup-file>\n' "$0" >&2
  exit 1
fi

BACKUP_FILE="$1"

if [ ! -f "${BACKUP_FILE}" ]; then
  printf 'backup file not found: %s\n' "${BACKUP_FILE}" >&2
  exit 1
fi

docker compose exec -T postgres psql -U postgres -d task_orchestrator < "${BACKUP_FILE}"
