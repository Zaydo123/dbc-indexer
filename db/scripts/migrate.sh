#!/usr/bin/env bash
set -euo pipefail

DB_HOST=${DB_HOST:-localhost}
DB_PORT=${DB_PORT:-5432}
DB_NAME=${DB_NAME:-grpc}
DB_USER=${DB_USER:-postgres}

PSQL="psql "
CONN_STR="postgresql://${DB_USER}:${POSTGRES_PASSWORD:-postgres}@${DB_HOST}:${DB_PORT}/${DB_NAME}"

wait_for_db() {
  echo "Waiting for DB ${DB_HOST}:${DB_PORT}..."
  for i in {1..60}; do
    if PGPASSWORD=${POSTGRES_PASSWORD:-postgres} ${PSQL} -d "$CONN_STR" -v ON_ERROR_STOP=1 -c 'SELECT 1;' >/dev/null 2>&1; then
      echo "DB is up"; return 0; fi
    sleep 2
  done
  echo "Timed out waiting for DB" >&2
  exit 1
}

run_sql_file() {
  local file="$1"
  echo "Applying: ${file}"
  PGPASSWORD=${POSTGRES_PASSWORD:-postgres} ${PSQL} -d "$CONN_STR" -v ON_ERROR_STOP=1 -f "$file"
}

main() {
  wait_for_db
  # Re-apply idempotent init scripts (safe due to IF NOT EXISTS and guards)
  for f in db/init/01_extensions.sql db/init/02_schema.sql db/init/03_hypertables_and_indexes.sql; do
    [ -f "$f" ] && run_sql_file "$f"
  done
  # Guarded ensures for compression policies and caggs/policies
  for f in db/migrate/ensure_compression_and_policies.sql db/migrate/ensure_caggs.sql; do
    [ -f "$f" ] && run_sql_file "$f"
  done
  echo "Migration check complete."
}

main "$@"
