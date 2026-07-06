#!/usr/bin/env bash
# Run one lane of the ZFS zstd storage experiment end-to-end:
# provision datasets -> start postgres+tidx -> sync Moderato to the target
# block count -> measure sizes -> run the OLTP suite -> tear down.
#
# See tasks/zfs-zstd-plan.md and docker/zfs/README.md.
#
# Usage: sudo -E run-lane.sh <l0|l1|l2|l3|l4>
#
# Lane presets:
#   l0  ext4 baseline (L0_DIR)          PG: stock flags
#   l1  ZFS recordsize=16k zstd-3       PG: fpw/init_zero/recycle off
#   l2  ZFS recordsize=32k zstd-3       PG: same as l1
#   l3  ZFS recordsize=16k lz4          PG: same as l1
#   l4  ZFS recordsize=$L4_RS zstd-9    PG: same as l1
#
# Environment:
#   TARGET_BLOCKS  stop after this many blocks are indexed (default: 1000000)
#   POOL           ZFS pool for zfs lanes (default: tidx)
#   L0_DIR         base dir for the ext4 baseline lane (default: /var/lib/tidx-zfs-l0)
#   L4_RS          recordsize for l4 (default: 32k)
#   BATCH_SIZE / CONCURRENCY   tidx sync settings (default: 500 / 8)
#   TIDX_RPC_URL   Moderato RPC (default: https://rpc.testnet.tempo.xyz)
#   TIDX_CHAIN_ID / TIDX_CHAIN_NAME   chain override (default: 42431 / moderato;
#                  used by the smoke test against a local dev node)
#   TEMPO_RPC_AUTH_MODERATO    basic-auth for the RPC (recommended; avoids 429s)
#   POLL_SECS      progress poll interval (default: 30)
#   SKIP_BENCH=1   skip the OLTP suite

set -euo pipefail

LANE="${1:?usage: run-lane.sh <l0|l1|l2|l3|l4>}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPT_DIR="$REPO_ROOT/scripts/zfs"
COMPOSE_FILE="$REPO_ROOT/docker/zfs/docker-compose.yml"
export COMPOSE_FILE

TARGET_BLOCKS="${TARGET_BLOCKS:-1000000}"
POOL="${POOL:-tidx}"
L0_DIR="${L0_DIR:-/var/lib/tidx-zfs-l0}"
POLL_SECS="${POLL_SECS:-30}"
TIDX_HTTP_PORT="${TIDX_HTTP_PORT:-8080}"
export TIDX_HTTP_PORT

OUT="$REPO_ROOT/results/zfs/$LANE"
if [ -e "$OUT/raw.env" ]; then
  OUT="$OUT-$(date +%Y%m%d-%H%M%S)"
fi
export OUT
mkdir -p "$OUT"

compose() { docker compose -f "$COMPOSE_FILE" "$@"; }
psql_ta() { compose exec -T postgres psql -U tidx -d tidx -tA -c "$1" 2>/dev/null; }

# --- lane preset -------------------------------------------------------------
RS="" COMP=""
case "$LANE" in
  l0) ;;
  l1) RS=16k COMP=zstd-3 ;;
  l2) RS=32k COMP=zstd-3 ;;
  l3) RS=16k COMP=lz4 ;;
  l4) RS="${L4_RS:-32k}" COMP=zstd-9 ;;
  *) echo "error: unknown lane '$LANE'" >&2; exit 1 ;;
esac

if [ "$LANE" = "l0" ]; then
  export PG_FULL_PAGE_WRITES=on PG_WAL_INIT_ZERO=on PG_WAL_RECYCLE=on
  mkdir -p "$L0_DIR/pgdata" "$L0_DIR/pgwal"
  chown 999:999 "$L0_DIR/pgdata" "$L0_DIR/pgwal" 2>/dev/null || true
  export TIDX_PGDATA="$L0_DIR/pgdata" TIDX_PGWAL="$L0_DIR/pgwal"
else
  [ "$(id -u)" = "0" ] || { echo "error: ZFS lanes need root (sudo -E)" >&2; exit 1; }
  export PG_FULL_PAGE_WRITES=off PG_WAL_INIT_ZERO=off PG_WAL_RECYCLE=off
  # provision.sh prints "export TIDX_PGDATA=..." lines; adopt them.
  eval "$(POOL="$POOL" "$SCRIPT_DIR/provision.sh" dataset "$LANE" "$RS" "$COMP" | grep '^export ')"
fi

[ -n "$(ls -A "$TIDX_PGDATA" 2>/dev/null)" ] && {
  echo "error: $TIDX_PGDATA is not empty; refusing to reuse a dirty data dir" >&2
  exit 1
}

echo "lane=$LANE recordsize=${RS:-ext4} compression=${COMP:-none}"
echo "pgdata=$TIDX_PGDATA pgwal=$TIDX_PGWAL target=$TARGET_BLOCKS blocks"
echo "results=$OUT"

# --- config ------------------------------------------------------------------
CONF="$REPO_ROOT/docker/zfs/config.gen.toml"
{
  echo '[http]'
  echo 'enabled = true'
  echo 'port = 8080'
  echo 'bind = "0.0.0.0"'
  echo
  echo '[prometheus]'
  echo 'enabled = true'
  echo 'port = 9090'
  echo
  echo '[[chains]]'
  echo "name = \"${TIDX_CHAIN_NAME:-moderato}\""
  echo "chain_id = ${TIDX_CHAIN_ID:-42431}"
  echo "rpc_url = \"${TIDX_RPC_URL:-https://rpc.testnet.tempo.xyz}\""
  if [ -n "${TEMPO_RPC_AUTH_MODERATO:-}" ]; then
    echo 'rpc_auth_env = "TEMPO_RPC_AUTH_MODERATO"'
  fi
  echo 'pg_url = "postgres://tidx:tidx@postgres:5432/tidx"'
  echo 'backfill = true'
  echo "batch_size = ${BATCH_SIZE:-500}"
  echo "concurrency = ${CONCURRENCY:-8}"
} >"$CONF"

if [ -z "${TEMPO_RPC_AUTH_MODERATO:-}" ]; then
  echo "warning: TEMPO_RPC_AUTH_MODERATO not set — public RPC rate limits (429) may throttle the sync" >&2
fi

# --- sync --------------------------------------------------------------------
cleanup() {
  echo "collecting container logs..."
  compose logs --no-color >"$OUT/compose.log" 2>&1 || true
  compose down >/dev/null 2>&1 || true
}
trap cleanup EXIT

compose up -d --build --wait postgres
compose up -d --build tidx

T0=$(date +%s)
LAST_COUNT=0
STALL=0
echo "syncing... (poll every ${POLL_SECS}s)"
while :; do
  sleep "$POLL_SECS"
  COUNT=$(psql_ta "SELECT count(*) FROM blocks" || echo 0)
  COUNT=${COUNT:-0}
  NOW=$(date +%s)
  RATE=$(awk -v c="$COUNT" -v t=$((NOW - T0)) 'BEGIN{ if (t>0) printf "%.0f", c/t; else print 0 }')
  echo "  blocks=$COUNT rate=${RATE}/s elapsed=$((NOW - T0))s"
  curl -sf "http://localhost:${TIDX_HTTP_PORT}/status" >"$OUT/status.json" 2>/dev/null || true

  if [ "$COUNT" -ge "$TARGET_BLOCKS" ]; then break; fi
  if [ "$COUNT" -le "$LAST_COUNT" ]; then
    STALL=$((STALL + 1))
    if [ "$STALL" -ge 40 ]; then
      echo "error: sync stalled for $((STALL * POLL_SECS))s; aborting lane" >&2
      exit 1
    fi
  else
    STALL=0
  fi
  LAST_COUNT=$COUNT
done
T1=$(date +%s)
WALL=$((T1 - T0))

echo "target reached: $COUNT blocks in ${WALL}s — stopping tidx"
compose stop tidx
psql_ta "CHECKPOINT" >/dev/null || true
sleep 10 # let the final ZFS txg commit settle before du

{
  echo "SYNC_WALL_SECS=$WALL"
  echo "SYNC_BLOCKS=$COUNT"
  echo "SYNC_AVG_BLOCKS_PER_SEC=$(awk -v c="$COUNT" -v t="$WALL" 'BEGIN{printf "%.1f", c/t}')"
} >"$OUT/sync.env"

# --- measure + bench ---------------------------------------------------------
"$SCRIPT_DIR/measure.sh" "$LANE"
if [ "${SKIP_BENCH:-0}" != "1" ]; then
  "$SCRIPT_DIR/bench.sh" "$LANE"
fi

echo "lane $LANE complete -> $OUT"
