#!/usr/bin/env bash
# start_cluster.sh – Launch a 5-node adaptive Raft cluster locally.
# Each node gets its own port (9001-9005) and unique ID (1-5).
# Press Ctrl-C to tear down all nodes.

set -euo pipefail

PIDS=()

cleanup() {
  echo ""
  echo "Stopping cluster..."
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait
  echo "All nodes stopped."
}
trap cleanup EXIT INT TERM

PEERS_FOR() {
  local self=$1
  local parts=()
  for i in 1 2 3 4 5; do
    if [[ $i -ne $self ]]; then
      parts+=("${i}=127.0.0.1:900${i}")
    fi
  done
  echo "$(
    IFS=,
    echo "${parts[*]}"
  )"
}

echo "Building..."
go build -o adaptive-raft .

echo "Starting 5-node cluster..."
for i in 1 2 3 4 5; do
  PEERS=$(PEERS_FOR $i)
  METRICS_PORT=$((9100 + i))
  ./adaptive-raft -id "$i" -port "900${i}" -peers "$PEERS" -metric-port "$METRICS_PORT" &
  PIDS+=($!)
  echo "  Node $i (PID ${PIDS[-1]}) on port 900${i}"
done

echo ""
echo "Cluster running. Press Ctrl-C to stop."
wait
