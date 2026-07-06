#!/usr/bin/env bash
# Run the OLTP query suite (scripts/zfs/oltp/*.sql) against a lane's Postgres.
# Mirrors the B3 shapes from the tiering evaluation so results stay comparable.
# Run AFTER measure.sh: the bench sample tables add bytes to the database.
#
# Usage: bench.sh <lane>
#
# Environment:
#   COMPOSE_FILE   compose file with the running postgres service
#                  (default: <repo>/docker/zfs/docker-compose.yml)
#   OUT            output dir (default: <repo>/results/zfs/<lane>)
#   BENCH_SECS     seconds per timed shape (default: 30)
#   BENCH_CLIENTS  pgbench clients/threads (default: 4)

set -euo pipefail

LANE="${1:?usage: bench.sh <lane>}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$REPO_ROOT/docker/zfs/docker-compose.yml}"
OUT="${OUT:-$REPO_ROOT/results/zfs/$LANE}"
BENCH_SECS="${BENCH_SECS:-30}"
BENCH_CLIENTS="${BENCH_CLIENTS:-4}"

mkdir -p "$OUT"

pg() {
  docker compose -f "$COMPOSE_FILE" exec -T postgres "$@"
}
psql_ta() {
  pg psql -U tidx -d tidx -tA -c "$1"
}

echo "building bench sample tables..."
TX_COUNT=$(psql_ta "SELECT count(*) FROM txs")
LOG_COUNT=$(psql_ta "SELECT count(*) FROM logs")
TX_FRAC=$(awk -v n="$TX_COUNT" 'BEGIN{f=(n>0)?40000/n:1; print (f>1)?1:f}')
LOG_FRAC=$(awk -v n="$LOG_COUNT" 'BEGIN{f=(n>0)?40000/n:1; print (f>1)?1:f}')
pg psql -U tidx -d tidx -v ON_ERROR_STOP=1 \
  -v tx_frac="$TX_FRAC" -v log_frac="$LOG_FRAC" \
  -f /oltp/00-samples.sql >/dev/null

MIN_BLOCK=$(psql_ta "SELECT min(num) FROM blocks")
MAX_BLOCK=$(psql_ta "SELECT max(num) FROM blocks")
N_TXH=$(psql_ta "SELECT count(*) FROM bench.tx_hashes")
N_ADDR=$(psql_ta "SELECT count(*) FROM bench.addresses")
[ "$N_TXH" -gt 0 ] || { echo "error: no tx hash samples (empty txs table?)" >&2; exit 1; }
[ "$N_ADDR" -gt 0 ] || { echo "error: no address samples (empty logs table?)" >&2; exit 1; }

SHAPES=(
  keyset_txs
  keyset_logs
  block_by_number
  tx_by_hash
  tx_by_hash_bounded
  logs_by_address
  receipt_by_tx
  recent_range
)

DARGS=(-D "min_block=$MIN_BLOCK" -D "max_block=$MAX_BLOCK" -D "n_txh=$N_TXH" -D "n_addr=$N_ADDR")

echo -e "shape\tlatency_avg_ms\ttps" >"$OUT/oltp.tsv"
for shape in "${SHAPES[@]}"; do
  echo "shape: $shape (warmup + ${BENCH_SECS}s x ${BENCH_CLIENTS} clients)"
  # warmup (NB: pgbench's -d is "debug", not dbname — dbname is positional)
  pg pgbench -U tidx -n -c 1 -t 20 "${DARGS[@]}" -f "/oltp/$shape.sql" tidx >/dev/null 2>&1 || {
    echo "error: shape $shape failed; running once verbosely:" >&2
    pg pgbench -U tidx -n -c 1 -t 1 "${DARGS[@]}" -f "/oltp/$shape.sql" tidx >&2 || true
    exit 1
  }
  out=$(pg pgbench -U tidx -n -c "$BENCH_CLIENTS" -j "$BENCH_CLIENTS" -T "$BENCH_SECS" "${DARGS[@]}" -f "/oltp/$shape.sql" tidx)
  lat=$(echo "$out" | awk '/latency average/ {print $(NF-1)}')
  tps=$(echo "$out" | awk '/^tps/ {print $3; exit}')
  echo -e "$shape\t${lat:-?}\t${tps:-?}" >>"$OUT/oltp.tsv"
done

{
  echo "# Lane $LANE — OLTP suite (${BENCH_SECS}s/shape, $BENCH_CLIENTS clients)"
  echo
  echo "| shape | avg latency (ms) | tps |"
  echo "|---|---:|---:|"
  tail -n +2 "$OUT/oltp.tsv" | while IFS=$'\t' read -r s l t; do
    echo "| $s | $l | $t |"
  done
} >"$OUT/oltp.md"

cat "$OUT/oltp.md"
