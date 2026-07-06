#!/usr/bin/env bash
# Provision the ZFS pool and per-lane datasets for the storage experiment.
# See tasks/zfs-zstd-plan.md. Must run as root on a Linux host with OpenZFS.
#
# Usage:
#   provision.sh pool /dev/nvme1n1p3          # dedicated device/partition
#   provision.sh pool --file /var/tmp/tidx-zpool.img 100G   # file-backed vdev
#   provision.sh dataset <lane> <recordsize> <compression>  # e.g. dataset l1 16k zstd-3
#   provision.sh destroy <lane>
#
# Environment:
#   POOL   pool name (default: tidx)

set -euo pipefail

POOL="${POOL:-tidx}"
PG_UID=999 # postgres uid/gid in the postgres:16 image
PG_GID=999

die() {
  echo "error: $*" >&2
  exit 1
}

[ "$(uname -s)" = "Linux" ] || die "ZFS lanes require a Linux host (see docker/zfs/README.md for macOS notes)"
[ "$(id -u)" = "0" ] || die "must run as root (zpool/zfs/chown)"
command -v zpool >/dev/null || die "zpool not found; install OpenZFS (e.g. apt install zfsutils-linux)"

cmd_pool() {
  local vdev="${1:-}"
  [ -n "$vdev" ] || die "usage: provision.sh pool <vdev> | pool --file <path> <size>"

  if [ "$vdev" = "--file" ]; then
    local img="${2:?file path required}" size="${3:?size required (e.g. 100G)}"
    [ -e "$img" ] && die "$img already exists"
    truncate -s "$size" "$img"
    vdev="$img"
    echo "note: file-backed vdev — compression ratios are accurate, perf numbers are approximate"
  fi

  zpool list "$POOL" >/dev/null 2>&1 && die "pool '$POOL' already exists"
  zpool create -o ashift=12 -O atime=off -O xattr=sa -O compression=off "$POOL" "$vdev"
  echo "created pool '$POOL' on $vdev"
}

cmd_dataset() {
  local lane="${1:?lane required (e.g. l1)}" rs="${2:?recordsize required}" comp="${3:?compression required}"
  local data="$POOL/${lane}-pgdata" wal="$POOL/${lane}-pgwal"

  zfs create -o recordsize="$rs" -o compression="$comp" -o logbias=latency "$data"
  # WAL is sequential, capped by max_wal_size, and not part of the size result:
  # cheap lz4 + default recordsize is fine for every lane.
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
  local lane="${1:?lane required}"
  zfs destroy -r "$POOL/${lane}-pgdata"
  zfs destroy -r "$POOL/${lane}-pgwal"
  echo "destroyed datasets for lane $lane"
}

case "${1:-}" in
  pool) shift; cmd_pool "$@" ;;
  dataset) shift; cmd_dataset "$@" ;;
  destroy) shift; cmd_destroy "$@" ;;
  *) die "usage: provision.sh {pool|dataset|destroy} ..." ;;
esac
