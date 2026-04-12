#!/bin/sh

set -eu

REPO_ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
OUTPUT_DIR="${1:-${REPO_ROOT}/evidence/drills}"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
BACKUP_PATH="${OUTPUT_DIR}/dispatchd_${TIMESTAMP}.sql"
SUMMARY_PATH="${OUTPUT_DIR}/backup-restore-validation.json"
RESTORE_DB="dispatchd_restore_validation"

mkdir -p "${OUTPUT_DIR}"

docker compose exec -T postgres pg_dump -U postgres dispatchd > "${BACKUP_PATH}"

ORIGINAL_COUNTS="$(docker compose exec -T postgres psql -U postgres -d dispatchd -At -F ',' -c "select (select count(*) from jobs), (select count(*) from executions), (select count(*) from workers);")"

docker compose exec -T postgres psql -U postgres -d postgres -c "DROP DATABASE IF EXISTS ${RESTORE_DB};" >/dev/null
docker compose exec -T postgres psql -U postgres -d postgres -c "CREATE DATABASE ${RESTORE_DB};" >/dev/null
docker compose exec -T postgres psql -U postgres -d "${RESTORE_DB}" < "${BACKUP_PATH}" >/dev/null

RESTORED_COUNTS="$(docker compose exec -T postgres psql -U postgres -d "${RESTORE_DB}" -At -F ',' -c "select (select count(*) from jobs), (select count(*) from executions), (select count(*) from workers);")"
docker compose exec -T postgres psql -U postgres -d postgres -c "DROP DATABASE IF EXISTS ${RESTORE_DB};" >/dev/null

IFS=',' read -r ORIGINAL_JOBS ORIGINAL_EXECUTIONS ORIGINAL_WORKERS <<EOF
${ORIGINAL_COUNTS}
EOF

IFS=',' read -r RESTORED_JOBS RESTORED_EXECUTIONS RESTORED_WORKERS <<EOF
${RESTORED_COUNTS}
EOF

MATCHED="false"
if [ "${ORIGINAL_COUNTS}" = "${RESTORED_COUNTS}" ]; then
  MATCHED="true"
fi

cat > "${SUMMARY_PATH}" <<EOF
{
  "backup_file": "${BACKUP_PATH}",
  "original": {
    "jobs": ${ORIGINAL_JOBS},
    "executions": ${ORIGINAL_EXECUTIONS},
    "workers": ${ORIGINAL_WORKERS}
  },
  "restored": {
    "jobs": ${RESTORED_JOBS},
    "executions": ${RESTORED_EXECUTIONS},
    "workers": ${RESTORED_WORKERS}
  },
  "matched": ${MATCHED}
}
EOF

printf 'backup written to %s\n' "${BACKUP_PATH}"
printf 'validation summary written to %s\n' "${SUMMARY_PATH}"
