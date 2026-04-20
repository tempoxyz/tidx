#!/usr/bin/env bash
set -euo pipefail

RPC_URL="${RPC_URL:-http://localhost:8500}"
API_URL="${API_URL:-http://localhost:8080}"
CHAIN_ID="${CHAIN_ID:-42431}"
CONTRACT_ADDR="${CONTRACT_ADDR:-0x1000000000000000000000000000000000000001}"
SENDER="${SENDER:-0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266}"
VIRTUAL_ADDR="${VIRTUAL_ADDR:-0x01020304fdfdfdfdfdfdfdfdfdfd0a0b0c0d0e0f}"
MASTER="${MASTER:-0x1111111111111111111111111111111111111111}"
AMOUNT="${AMOUNT:-123}"
TIMEOUT_SECS="${TIMEOUT_SECS:-60}"
POLL_SECS="${POLL_SECS:-2}"

for bin in forge cast jq curl; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    echo "Missing required dependency: $bin"
    exit 1
  fi
done

WORKDIR="/tmp/veproj"
SOL="/tmp/VirtualEmitter.sol"
ARTIFACT="$WORKDIR/out/VirtualEmitter.sol/VirtualEmitter.json"

cat >"$SOL" <<'EOF'
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

contract VirtualEmitter {
    event Transfer(address indexed from, address indexed to, uint256 value);

    function emitPair(address sender, address virtualAddr, address master, uint256 amount) external {
        emit Transfer(sender, virtualAddr, amount);
        emit Transfer(virtualAddr, master, amount);
    }
}
EOF

rm -rf "$WORKDIR"
mkdir -p "$WORKDIR/src"
cp "$SOL" "$WORKDIR/src/VirtualEmitter.sol"
(
  cd "$WORKDIR"
  forge build >/dev/null
)

RUNTIME=$(jq -r '.deployedBytecode.object' "$ARTIFACT")
echo "Injecting runtime bytecode into $CONTRACT_ADDR"
cast rpc --rpc-url "$RPC_URL" anvil_setCode "$CONTRACT_ADDR" "$RUNTIME" >/dev/null

DATA=$(cast calldata 'emitPair(address,address,address,uint256)' \
  "$SENDER" "$VIRTUAL_ADDR" "$MASTER" "$AMOUNT")

echo "Sending synthetic virtual-address tx"
TX_HASH=$(curl -s -X POST "$RPC_URL" \
  -H 'content-type: application/json' \
  --data "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_sendTransaction\",\"params\":[{\"from\":\"$SENDER\",\"to\":\"$CONTRACT_ADDR\",\"data\":\"$DATA\"}]}" \
  | jq -r '.result')

if [[ -z "$TX_HASH" || "$TX_HASH" == "null" ]]; then
  echo "Failed to send tx"
  exit 1
fi

echo "TX_HASH=$TX_HASH"

SQL="SELECT block_num, tx_hash, log_idx, format_address(address) AS log_address, format_address(abi_address(topic1)) AS from_addr, format_address(abi_address(topic2)) AS to_addr, is_virtual_forward FROM logs WHERE tx_hash = '$TX_HASH' ORDER BY log_idx"
ENC_SQL=$(printf '%s' "$SQL" | jq -sRr @uri)

DEADLINE=$(( $(date +%s) + TIMEOUT_SECS ))
while true; do
  RESP=$(curl -s "$API_URL/query?chainId=$CHAIN_ID&sql=$ENC_SQL")
  ROW_COUNT=$(printf '%s' "$RESP" | jq -r '.row_count // 0')
  if [[ "$ROW_COUNT" != "0" ]]; then
    echo "$RESP" | jq .
    break
  fi
  if (( $(date +%s) >= DEADLINE )); then
    echo "Timed out waiting for tidx to index tx $TX_HASH"
    echo "$RESP" | jq .
    exit 1
  fi
  sleep "$POLL_SECS"
done

MATCH=$(printf '%s' "$RESP" | jq --arg contract "$CONTRACT_ADDR" --arg sender "$SENDER" --arg vaddr "$VIRTUAL_ADDR" --arg master "$MASTER" '
  [ .rows[]
    | select(.[3] == $contract)
    | {from: .[4], to: .[5], forward: .[6]} ] as $rows
  | ($rows | length) >= 2
    and ([ $rows[] | select(.forward == true) ] | length) == 1
    and any($rows[]; .from == $sender and .to == $vaddr and .forward == false)
    and any($rows[]; .from == $vaddr and .to == $master and .forward == true)
')

if [[ "$MATCH" != "true" ]]; then
  echo "E2E verification failed: expected exact virtual pair shape not found"
  exit 1
fi

echo
printf 'E2E verification passed for %s\n' "$TX_HASH"
