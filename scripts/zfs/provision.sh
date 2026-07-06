#!/usr/bin/env bash
# Provision ZFS storage for tidx's Postgres (transparent compression).
# Use with docker/prod/docker-compose.zfs.yml. Requires root + OpenZFS.
#
# Usage:
#   provision.sh pool /dev/nvme1n1p3                 # dedicated device/partition
#   provision.sh pool --file /var/lib/tidx-zpool.img 200G   # file-backed vdev
#   provision.sh datasets [recordsize] [compression] # default: 16k lz4
#   provision.sh destroy                             # remove the datasets
#
# Environment:
#   POOL   pool name (default: tidx)
#
# Measured on Moderato: 16k/lz4 = 2.4x smaller at near-parity reads;
# 32k/zstd-3 = 3.8x smaller for archive boxes.

set -euo pipefail

POOL="${POOL:-tidx}"
PG_UID=999 # postgres uid/gid in the postgres:16 image
PG_GID=999

die() {
  echo "error: $*" >&2
  exit 1
}

[ "$(uname -s)" = "Linux" ] || die "requires a Linux host with OpenZFS"
[ "$(id -u)" = "0" ] || die "must run as root (zpool/zfs/chown)"
command -v zpool >/dev/null || die "zpool not found; install OpenZFS (e.g. apt install zfsutils-linux)"

cmd_pool() {
  local vdev="${1:-}"
  [ -n "$vdev" ] || die "usage: provision.sh pool <vdev> | pool --file <path> <size>"

  if [ "$vdev" = "--file" ]; then
    local img="${2:?file path required}" size="${3:?size required (e.g. 200G)}"
    [ -e "$img" ] && die "$img already exists"
    truncate -s "$size" "$img"
    vdev="$img"
  fi

  zpool list "$POOL" >/dev/null 2>&1 && die "pool '$POOL' already exists"
  zpool create -o ashift=12 -O atime=off -O xattr=sa -O compression=off "$POOL" "$vdev"
  echo "created pool '$POOL' on $vdev"
}

cmd_datasets() {
  local rs="${1:-16k}" comp="${2:-lz4}"
  local data="$POOL/pgdata" wal="$POOL/pgwal"

  zfs create -o recordsize="$rs" -o compression="$comp" -o logbias=latency "$data"
  # WAL is sequential and capped by max_wal_size; cheap lz4 is always fine.
  zfs create -o recordsize=128k -o compression=lz4 -o logbias=latency "$wal"

  local data_mp wal_mp
  data_mp="$(zfs get -H -o value mountpoint "$data")"
  wal_mp="$(zfs get -H -o value mountpoint "$wal")"
  chown "$PG_UID:$PG_GID" "$data_mp" "$wal_mp"
  chmod 700 "$data_mp" "$wal_mp"

  echo "created $data ($rs/$comp) and $wal (128k/lz4)"
  echo "export TIDX_PGDATA=$data_mp"
  echo "export TIDX_PGWAL=$wal_mp"
}

cmd_destroy() {
  zfs destroy -r "$POOL/pgdata"
  zfs destroy -r "$POOL/pgwal"
  echo "destroyed $POOL/pgdata and $POOL/pgwal"
}

case "${1:-}" in
  pool) shift; cmd_pool "$@" ;;
  datasets) shift; cmd_datasets "$@" ;;
  destroy) shift; cmd_destroy "$@" ;;
  *) die "usage: provision.sh {pool|datasets|destroy} ..." ;;
esac
