#!/usr/bin/env bash
# Collect storage measurements for a ZFS-experiment lane.
# Run AFTER the sync reaches its target and tidx is stopped, and BEFORE
# bench.sh (the bench sample tables would pollute the size numbers).
#
# Usage: measure.sh <lane>
#
# Environment:
#   COMPOSE_FILE  compose file with the running postgres service
#                 (default: <repo>/docker/zfs/docker-compose.yml)
#   TIDX_PGDATA   host path of the data dir (required, du/zfs lookups)
#   TIDX_PGWAL    host path of the WAL dir (required)
#   OUT           output dir (default: <repo>/results/zfs/<lane>)

set -euo pipefail

LANE="${1:?usage: measure.sh <lane>}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$REPO_ROOT/docker/zfs/docker-compose.yml}"
OUT="${OUT:-$REPO_ROOT/results/zfs/$LANE}"
: "${TIDX_PGDATA:?TIDX_PGDATA required}"
: "${TIDX_PGWAL:?TIDX_PGWAL required}"

mkdir -p "$OUT"

psql_ta() {
  docker compose -f "$COMPOSE_FILE" exec -T postgres psql -U tidx -d tidx -tA -c "$1"
}

echo "measuring lane $LANE -> $OUT"

# --- logical sizes and row counts (Postgres view; comparable across lanes/fs) ---
psql_ta "
SELECT relname,
       pg_table_size(c.oid),
       pg_indexes_size(c.oid),
       pg_total_relation_size(c.oid)
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
  AND c.relkind = 'r'
  AND relname IN ('blocks','txs','logs','receipts')
ORDER BY pg_total_relation_size(c.oid) DESC;
" >"$OUT/tables.tsv"

BLOCKS=$(psql_ta "SELECT count(*) FROM blocks")
MIN_BLOCK=$(psql_ta "SELECT coalesce(min(num),0) FROM blocks")
MAX_BLOCK=$(psql_ta "SELECT coalesce(max(num),0) FROM blocks")
TXS=$(psql_ta "SELECT count(*) FROM txs")
LOGS=$(psql_ta "SELECT count(*) FROM logs")
RECEIPTS=$(psql_ta "SELECT count(*) FROM receipts")
DB_LOGICAL=$(psql_ta "SELECT pg_database_size('tidx')")
WAL_BYTES=$(psql_ta "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::int8")

# --- physical sizes (what the disk actually holds) ---
du_bytes() {
  # Allocated bytes, NOT apparent size (du -b would report uncompressed bytes
  # on ZFS). GNU: --block-size=1 counts allocation; BSD (macOS): -k * 1024.
  du -s --block-size=1 "$1" 2>/dev/null | cut -f1 && return
  du -sk "$1" 2>/dev/null | awk '{print $1 * 1024}'
}
DATA_PHYS=$(du_bytes "$TIDX_PGDATA" || echo 0)
WAL_PHYS=$(du_bytes "$TIDX_PGWAL" || echo 0)

# --- ZFS view (skipped on non-ZFS lanes/hosts) ---
ZFS_DATA_LINE="" ZFS_WAL_LINE=""
if command -v zfs >/dev/null 2>&1; then
  ZFS_DATA_LINE=$(zfs list -Hp -o name,used,logicalused,compressratio,recordsize,compression 2>/dev/null |
    awk -v mp="$TIDX_PGDATA" '$1 != "" {print}' | while read -r name rest; do
      m=$(zfs get -H -o value mountpoint "$name" 2>/dev/null)
      [ "$m" = "$TIDX_PGDATA" ] && echo "$name $rest"
    done | head -1) || true
  ZFS_WAL_LINE=$(zfs list -Hp -o name,used,logicalused,compressratio,recordsize,compression 2>/dev/null |
    awk '$1 != "" {print}' | while read -r name rest; do
      m=$(zfs get -H -o value mountpoint "$name" 2>/dev/null)
      [ "$m" = "$TIDX_PGWAL" ] && echo "$name $rest"
    done | head -1) || true
fi

# --- machine-readable dump ---
{
  echo "LANE=$LANE"
  echo "MEASURED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "BLOCKS=$BLOCKS"
  echo "MIN_BLOCK=$MIN_BLOCK"
  echo "MAX_BLOCK=$MAX_BLOCK"
  echo "TXS=$TXS"
  echo "LOGS=$LOGS"
  echo "RECEIPTS=$RECEIPTS"
  echo "DB_LOGICAL_BYTES=$DB_LOGICAL"
  echo "WAL_LSN_BYTES=$WAL_BYTES"
  echo "DATA_PHYSICAL_BYTES=$DATA_PHYS"
  echo "WAL_PHYSICAL_BYTES=$WAL_PHYS"
  echo "ZFS_DATA=${ZFS_DATA_LINE:-n/a}"
  echo "ZFS_WAL=${ZFS_WAL_LINE:-n/a}"
} >"$OUT/raw.env"

# --- human summary ---
# Prefer the authoritative ZFS accounting (used/logicalused/compressratio)
# when the data dir is a ZFS dataset; fall back to du + pg_database_size.
if [ -n "$ZFS_DATA_LINE" ]; then
  DATA_PHYS=$(echo "$ZFS_DATA_LINE" | awk '{print $2}')
  RATIO=$(echo "$ZFS_DATA_LINE" | awk '{print $4 "x (zfs compressratio)"}')
else
  RATIO="n/a"
  if [ "$DATA_PHYS" -gt 0 ]; then
    RATIO=$(awk -v l="$DB_LOGICAL" -v p="$DATA_PHYS" 'BEGIN{printf "%.2fx (db-logical/du)", l/p}')
  fi
fi
gb() { awk -v b="$1" 'BEGIN{printf "%.2f", b/1e9}'; }
PER_M="n/a"
if [ "$BLOCKS" -gt 0 ]; then
  PER_M=$(awk -v b="$DATA_PHYS" -v n="$BLOCKS" 'BEGIN{printf "%.2f", b/n*1e6/1e9}')
fi

{
  echo "# Lane $LANE — storage measurement"
  echo
  echo "| metric | value |"
  echo "|---|---|"
  echo "| blocks (rows) | $BLOCKS ($MIN_BLOCK..$MAX_BLOCK) |"
  echo "| txs / logs / receipts | $TXS / $LOGS / $RECEIPTS |"
  echo "| logical DB size | $(gb "$DB_LOGICAL") GB |"
  echo "| physical data dir | $(gb "$DATA_PHYS") GB |"
  echo "| physical WAL dir | $(gb "$WAL_PHYS") GB |"
  echo "| WAL written (lsn) | $(gb "$WAL_BYTES") GB |"
  echo "| **compression ratio** | **${RATIO}** |"
  echo "| **physical GB per 1M blocks** | **$PER_M** |"
  echo "| zfs data dataset | ${ZFS_DATA_LINE:-n/a} |"
  echo "| zfs wal dataset | ${ZFS_WAL_LINE:-n/a} |"
  echo
  echo "Per-table logical bytes (table / indexes / total):"
  echo
  echo '```'
  cat "$OUT/tables.tsv"
  echo '```'
} >"$OUT/summary.md"

cat "$OUT/summary.md"
