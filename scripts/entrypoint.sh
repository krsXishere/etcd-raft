#!/bin/bash
# entrypoint.sh – Apply Linux Traffic Control (tc netem) rules based on
# environment variables, then start the adaptive-raft node.
#
# TC parameters are fully configurable per-node via env vars:
#   TC_DELAY        – base delay          (e.g. "10ms",  default "0ms")
#   TC_JITTER       – delay variation     (e.g. "3ms",   default "0ms")
#   TC_LOSS         – packet loss rate    (e.g. "0.1%",  default "0%")
#   TC_CORRELATION  – jitter correlation  (e.g. "25%",   default "0%")
#   TC_DUPLICATE    – packet duplication  (e.g. "0.1%",  default "0%")
#   TC_REORDER      – packet reordering   (e.g. "0.1%",  default "0%")
#
# Set TC_DELAY="0ms" (or leave empty) to skip TC entirely.
#
# Suggested presets (pass via .env or command-line):
#   Jakarta-Surabaya (Low Latency):
#     Hub   → TC_DELAY=2ms  TC_JITTER=1ms  TC_LOSS=0.1% TC_CORRELATION=25%
#     Remote→ TC_DELAY=10ms TC_JITTER=3ms  TC_LOSS=0.1% TC_CORRELATION=25%
#   Jakarta-Papua (High Latency / Unstable):
#     Hub   → TC_DELAY=2ms   TC_JITTER=1ms   TC_LOSS=0.1% TC_CORRELATION=25%
#     Remote→ TC_DELAY=150ms TC_JITTER=50ms  TC_LOSS=2%   TC_CORRELATION=25%

set -e

TC_DELAY="${TC_DELAY:-0ms}"
TC_JITTER="${TC_JITTER:-0ms}"
TC_LOSS="${TC_LOSS:-0%}"
TC_CORRELATION="${TC_CORRELATION:-0%}"
TC_DUPLICATE="${TC_DUPLICATE:-0%}"
TC_REORDER="${TC_REORDER:-0%}"

if [ "$TC_DELAY" != "0ms" ] && [ -n "$TC_DELAY" ]; then
    echo "[TC] node=${NODE_ID} delay=${TC_DELAY} jitter=${TC_JITTER} loss=${TC_LOSS} correlation=${TC_CORRELATION} duplicate=${TC_DUPLICATE} reorder=${TC_REORDER}"
    tc qdisc add dev eth0 root netem \
        delay ${TC_DELAY} ${TC_JITTER} ${TC_CORRELATION} \
        loss ${TC_LOSS} \
        duplicate ${TC_DUPLICATE} \
        reorder ${TC_REORDER} \
        2>/dev/null || echo "[TC] WARNING: tc command failed (may need NET_ADMIN capability)"
else
    echo "[TC] No traffic control applied (TC_DELAY=${TC_DELAY})"
fi

ADAPTIVE_MODE="${ADAPTIVE_MODE:-true}"
BASELINE_RTT="${BASELINE_RTT:-50ms}"
T_BASE="${T_BASE:-1s}"
T_MIN="${T_MIN:-500ms}"
T_MAX="${T_MAX:-5s}"
KP="${KP:-1.5}"
KI="${KI:-0.2}"
KD="${KD:-0.1}"

echo "[entrypoint] Starting adaptive-raft node..."

if [ "$ADAPTIVE_MODE" = "true" ]; then
    echo "[MODE] ADAPTIVE (PID) node=${NODE_ID} baseline-rtt=${BASELINE_RTT} t-base=${T_BASE} t-min=${T_MIN} t-max=${T_MAX} kp=${KP} ki=${KI} kd=${KD}"
    exec /app/adaptive-raft \
        -id "$NODE_ID" \
        -port "$NODE_PORT" \
        -metric-port "$METRICS_PORT" \
        -peers "$PEERS" \
        -adaptive \
        -baseline-rtt "$BASELINE_RTT" \
        -t-base "$T_BASE" \
        -t-min "$T_MIN" \
        -t-max "$T_MAX" \
        -kp "$KP" \
        -ki "$KI" \
        -kd "$KD" \
        "$@"
else
    echo "[MODE] STATIC node=${NODE_ID} t-base=${T_BASE} (fixed election timeout, PID disabled)"
    exec /app/adaptive-raft \
        -id "$NODE_ID" \
        -port "$NODE_PORT" \
        -metric-port "$METRICS_PORT" \
        -peers "$PEERS" \
        -t-base "$T_BASE" \
        -t-min "$T_MIN" \
        -t-max "$T_MAX" \
        "$@"
fi
