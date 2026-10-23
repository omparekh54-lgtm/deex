#!/bin/sh
set -eu

SNAPSHOT="${DEEX_SNAPSHOT_PATH:-/app/snapshot/pms.dump}"
PGDATA="${DEEX_PGDATA:-/tmp/deex-pgdata}"
PGSOCKET="${DEEX_PGSOCKET:-/tmp/deex-pgsocket}"
PGPORT="${DEEX_PGPORT:-55432}"
PG_BIN="${DEEX_PG_BIN:-/usr/lib/postgresql/17/bin}"

start_app() {
  exec uvicorn app.main:app --host 0.0.0.0 --port "${PORT:-10000}" --workers 1 --no-access-log --no-server-header --limit-concurrency 64 --timeout-keep-alive 5
}

mkdir -p "$(dirname "$SNAPSHOT")"

if [ ! -s "$SNAPSHOT" ] && [ -n "${SNAPSHOT_SOURCE_URL:-}" ]; then
  echo "Creating private read-only Design Experience snapshot..."
  export PGOPTIONS='-c default_transaction_read_only=on -c timezone=UTC -c datestyle=ISO,YMD'
  "$PG_BIN/pg_dump" --format=custom --compress=6 --no-owner --no-privileges \
    --dbname "$SNAPSHOT_SOURCE_URL" \
    --schema=pms \
    --table=public.benchmarks \
    --table=public.corporate_events \
    --table=public.financial_facts \
    --table=public.market_bars \
    --table=public.securities \
    --table=public.security_benchmarks \
    --table=public.sources \
    --file "$SNAPSHOT.tmp"
  mv "$SNAPSHOT.tmp" "$SNAPSHOT"
  unset PGOPTIONS
  echo "Private snapshot created locally."
fi

if [ ! -s "$SNAPSHOT" ]; then
  echo "DEEX local snapshot not present; using configured external PMS_DATABASE_URL if available."
  start_app
fi

mkdir -p "$PGDATA" "$PGSOCKET"
chmod 700 "$PGDATA"

if [ ! -s "$PGDATA/PG_VERSION" ]; then
  echo "Initializing local read-only DEEX PostgreSQL snapshot..."
  "$PG_BIN/initdb" -D "$PGDATA" --auth=trust --username=deex >/dev/null
  {
    echo "listen_addresses='127.0.0.1'"
    echo "port=$PGPORT"
    echo "unix_socket_directories='$PGSOCKET'"
    echo "fsync=off"
    echo "synchronous_commit=off"
    echo "full_page_writes=off"
    echo "max_connections=12"
  } >> "$PGDATA/postgresql.conf"
  "$PG_BIN/pg_ctl" -D "$PGDATA" -o "-k $PGSOCKET -p $PGPORT" -w start >/dev/null
  "$PG_BIN/createdb" -h "$PGSOCKET" -p "$PGPORT" -U deex pms
  "$PG_BIN/pg_restore" -h "$PGSOCKET" -p "$PGPORT" -U deex -d pms --no-owner --no-privileges --exit-on-error "$SNAPSHOT"
  "$PG_BIN/psql" -h "$PGSOCKET" -p "$PGPORT" -U deex -d pms -v ON_ERROR_STOP=1 <<'SQL'
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
ALTER DATABASE pms SET default_transaction_read_only=on;
SQL
else
  "$PG_BIN/pg_ctl" -D "$PGDATA" -o "-k $PGSOCKET -p $PGPORT" -w start >/dev/null
fi

export PMS_DATABASE_URL="postgresql://deex@127.0.0.1:${PGPORT}/pms"
unset SNAPSHOT_SOURCE_URL || true

echo "DEEX is using the local Design Experience snapshot."
start_app
