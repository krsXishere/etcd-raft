#!/bin/bash
# entrypoint.sh – Apply Linux Traffic Control (tc netem) rules based on
# the NETWORK_PROFILE and NODE_LOCATION environment variables, then
# start the adaptive-raft node.
#
# Profiles:
#   low-latency  (Jakarta-Surabaya)  – moderate delay, low jitter & loss
#   high-latency (Jakarta-Papua)     – high delay, significant jitter & loss
#   none                             – no artificial latency
#
# Node locations:
#   jakarta   – low-latency hub
#   surabaya  – moderate-latency peer
#   papua     – high-latency / unstable peer

set -e

PROFILE="${NETWORK_PROFILE:-low-latency}"
LOCATION="${NODE_LOCATION:-jakarta}"

apply_tc() {
    local delay="$1" jitter="$2" loss="$3" correlation="$4"
    echo "[TC] profile=${PROFILE} location=${LOCATION}"
    echo "[TC] delay=${delay} jitter=${jitter} loss=${loss} correlation=${correlation}"
    tc qdisc add dev eth0 root netem \
        delay ${delay} ${jitter} ${correlation} \
        loss ${loss} \
        2>/dev/null || echo "[TC] WARNING: tc command failed (may need NET_ADMIN capability)"
}

case "$PROFILE" in
    low-latency)
        # Jakarta-Surabaya profile: low overall latency
        case "$LOCATION" in
            jakarta)    apply_tc "2ms"  "1ms"  "0.1%"  "25%" ;;
            surabaya)   apply_tc "10ms" "3ms"  "0.1%"  "25%" ;;
            *)          apply_tc "5ms"  "2ms"  "0.1%"  "25%" ;;
        esac
        ;;
    high-latency)
        # Jakarta-Papua profile: heterogeneous latency with instability
        case "$LOCATION" in
            jakarta)    apply_tc "2ms"   "1ms"   "0.1%"  "25%" ;;
            papua)      apply_tc "150ms" "50ms"  "2%"    "25%" ;;
            *)          apply_tc "50ms"  "20ms"  "1%"    "25%" ;;
        esac
        ;;
    none)
        echo "[TC] No traffic control applied"
        ;;
    *)
        echo "[TC] Unknown profile: $PROFILE – no TC applied"
        ;;
esac

echo "[entrypoint] Starting adaptive-raft node..."
exec /app/adaptive-raft \
    -id "$NODE_ID" \
    -port "$NODE_PORT" \
    -metric-port "$METRICS_PORT" \
    -peers "$PEERS" \
    "$@"
