#!/usr/bin/env bash
###############################################################################
# run_experiments.sh — Structured experiment runner for Adaptive Raft
#
# Runs 4 scenarios (stable / latency-spike / partition / overhead),
# each in STATIC then ADAPTIVE mode, using the built-in benchmark tool.
#
# Result structure:
#   results/
#   └── <YYYYMMDD_HHMMSS>/           ← one run
#       ├── s1_stable/
#       │   ├── static/
#       │   │   ├── benchmark.json    ← benchmark tool output
#       │   │   └── benchmark.log     ← full console log
#       │   └── adaptive/
#       │       ├── benchmark.json
#       │       └── benchmark.log
#       ├── s2_latency_spike/
#       │   ├── static/
#       │   │   ├── benchmark.json
#       │   │   ├── benchmark.log
#       │   │   └── timeline.json     ← spike inject/restore timestamps
#       │   └── adaptive/
#       │       └── ...
#       ├── s3_partition/
#       │   ├── static/
#       │   │   ├── benchmark.json
#       │   │   ├── benchmark.log
#       │   │   ├── timeline.json     ← partition create/heal timestamps
#       │   │   └── recovery.json     ← recovery metrics from all nodes
#       │   └── adaptive/
#       │       └── ...
#       ├── s4_overhead/
#       │   ├── static/
#       │   │   ├── benchmark.json
#       │   │   ├── benchmark.log
#       │   │   ├── before.json       ← CPU/mem snapshot before
#       │   │   ├── after.json        ← CPU/mem snapshot after
#       │   │   └── summary.json      ← overhead % calculation
#       │   └── adaptive/
#       │       └── ...
#       └── summary.json              ← combined summary of all scenarios
#
# Usage:
#   ./scripts/run_experiments.sh                  # all scenarios, both modes
#   ./scripts/run_experiments.sh s1               # only scenario 1, both modes
#   ./scripts/run_experiments.sh s2 adaptive      # only scenario 2, adaptive
#   ./scripts/run_experiments.sh all static       # all scenarios, static only
#
# Prerequisites: docker compose v2+, curl, jq
###############################################################################
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RUN_DIR="$PROJECT_DIR/results/$TIMESTAMP"

# ── Tunables ────────────────────────────────────────────────────────
WARMUP=15s                # cluster warm-up after start
BENCH_RATE=50             # proposals/sec
BENCH_BURST_RATE=300      # burst rate (for bursty pattern)

S1_DURATION=60s           # scenario 1 benchmark length

S2_DURATION=120s          # scenario 2 total benchmark length
S2_SPIKE_AT=30            # seconds into benchmark to inject spike
S2_RESTORE_AT=75          # seconds into benchmark to restore normal

S3_DURATION=90s           # scenario 3 total benchmark length
S3_PARTITION_AT=20        # seconds into benchmark to create partition
S3_HEAL_AT=50             # seconds into benchmark to heal partition

S4_DURATION=60s           # scenario 4 benchmark length

# ── Node ports (host-side) ─────────────────────────────────────────
ALL_PORTS=(9201 9202 9203 9204 9205)
HUB_PORTS=(9201 9202 9203)
REMOTE_PORTS=(9204 9205)

# ── TC presets (JSON for POST /tc) ─────────────────────────────────
TC_HUB_NORMAL='{"delay":"2ms","jitter":"0.05ms","correlation":"90%","loss":"0%","duplicate":"0%","reorder":"0%"}'
TC_NODE4_NORMAL='{"delay":"9.8ms","jitter":"0.05ms","correlation":"90%","loss":"0%","duplicate":"0%","reorder":"0%"}'
TC_NODE5_NORMAL='{"delay":"59.75ms","jitter":"0.05ms","correlation":"90%","loss":"0%","duplicate":"0%","reorder":"0%"}'
TC_HUB_SPIKE='{"delay":"75ms","jitter":"25ms","correlation":"25%","loss":"1%","duplicate":"0%","reorder":"0%"}'
TC_REMOTE_SPIKE='{"delay":"150ms","jitter":"50ms","correlation":"25%","loss":"2%","duplicate":"0%","reorder":"0%"}'

# ── Colors & logging ───────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; DIM='\033[2m'; NC='\033[0m'

_ts()     { date +%H:%M:%S; }
log()     { echo -e "${CYAN}[$(_ts)]${NC} $*"; }
ok()      { echo -e "${GREEN}[$(_ts)] ✓${NC} $*"; }
warn()    { echo -e "${YELLOW}[$(_ts)] !${NC} $*"; }
fail()    { echo -e "${RED}[$(_ts)] ✗${NC} $*"; }
header()  { echo -e "\n${BOLD}━━━ $* ━━━${NC}"; }
divider() { echo -e "${DIM}──────────────────────────────────────────────────${NC}"; }

###############################################################################
# HELPERS
###############################################################################

wait_healthy() {
    local timeout=${1:-120}
    local deadline=$(( SECONDS + timeout ))
    while (( SECONDS < deadline )); do
        local up=0
        for p in "${ALL_PORTS[@]}"; do
            curl -sf "http://localhost:$p/health" >/dev/null 2>&1 && (( up++ )) || true
        done
        if (( up == ${#ALL_PORTS[@]} )); then
            ok "All ${#ALL_PORTS[@]} nodes healthy"
            return 0
        fi
        printf "\r  ${DIM}Waiting… %d/%d healthy${NC}  " "$up" "${#ALL_PORTS[@]}"
        sleep 2
    done
    echo
    fail "Cluster health timeout (${timeout}s)"
    return 1
}

find_leader() {
    # prints "port:node_id" of current leader, or empty string
    for p in "${ALL_PORTS[@]}"; do
        local resp
        resp=$(curl -sf "http://localhost:$p/status" 2>/dev/null) || continue
        local role id
        role=$(echo "$resp" | jq -r '.role // empty' 2>/dev/null) || continue
        id=$(echo "$resp" | jq -r '.id // empty' 2>/dev/null) || continue
        if [[ "$role" == "leader" ]]; then
            echo "$p:$id"
            return 0
        fi
    done
    echo ""
    return 1
}

wait_leader() {
    local timeout=${1:-60}
    local deadline=$(( SECONDS + timeout ))
    while (( SECONDS < deadline )); do
        local info
        info=$(find_leader) || true
        if [[ -n "$info" ]]; then
            local port=${info%%:*} nid=${info##*:}
            ok "Leader: node $nid on port :$port"
            return 0
        fi
        sleep 1
    done
    fail "No leader found within ${timeout}s"
    return 1
}

tc_apply() {
    local port=$1 payload=$2
    curl -sf -X POST "http://localhost:$port/tc" \
        -H "Content-Type: application/json" \
        -d "$payload" >/dev/null 2>&1 || warn "TC failed on :$port"
}

tc_restore_all() {
    log "Restoring normal TC on all nodes"
    for p in "${HUB_PORTS[@]}"; do tc_apply "$p" "$TC_HUB_NORMAL"; done
    tc_apply 9204 "$TC_NODE4_NORMAL"
    tc_apply 9205 "$TC_NODE5_NORMAL"
    ok "TC restored"
}

tc_inject_spike() {
    log "Injecting latency spike on all nodes"
    for p in "${HUB_PORTS[@]}"; do tc_apply "$p" "$TC_HUB_SPIKE"; done
    for p in "${REMOTE_PORTS[@]}"; do tc_apply "$p" "$TC_REMOTE_SPIKE"; done
    ok "Spike active: hub 75ms±25ms, remote 150ms±50ms, loss 1-2%"
}

collect_metrics_snapshot() {
    # Collects CPU/mem/tuning metrics from a node, writes to $2
    local port=$1 outfile=$2
    local raw
    raw=$(curl -sf "http://localhost:$port/metrics" 2>/dev/null) || raw=""

    local cpu mem_alloc mem_sys goroutines tuning_sum tuning_count
    cpu=$(echo "$raw"          | grep '^process_cpu_seconds_total '                    | awk '{print $2}') || cpu=0
    mem_alloc=$(echo "$raw"    | grep '^go_memstats_alloc_bytes '                      | awk '{print $2}') || mem_alloc=0
    mem_sys=$(echo "$raw"      | grep '^go_memstats_sys_bytes '                        | awk '{print $2}') || mem_sys=0
    goroutines=$(echo "$raw"   | grep '^go_goroutines '                                | awk '{print $2}') || goroutines=0
    tuning_sum=$(echo "$raw"   | grep '^raft_overhead_tuning_duration_seconds_sum{'    | awk '{print $2}') || tuning_sum=0
    tuning_count=$(echo "$raw" | grep '^raft_overhead_tuning_duration_seconds_count{'  | awk '{print $2}') || tuning_count=0

    cat > "$outfile" <<EOF
{
  "timestamp": "$(date -Iseconds)",
  "port": $port,
  "process_cpu_seconds_total": ${cpu:-0},
  "go_memstats_alloc_bytes": ${mem_alloc:-0},
  "go_memstats_sys_bytes": ${mem_sys:-0},
  "go_goroutines": ${goroutines:-0},
  "tuning_duration_sum_s": ${tuning_sum:-0},
  "tuning_duration_count": ${tuning_count:-0}
}
EOF
}

###############################################################################
# CLUSTER LIFECYCLE
###############################################################################

start_cluster() {
    local mode=$1  # static | adaptive
    local adaptive_flag="false"
    [[ "$mode" == "adaptive" ]] && adaptive_flag="true"

    header "Starting cluster — ${mode^^} mode"
    cd "$PROJECT_DIR"

    docker compose down --remove-orphans --timeout 5 2>/dev/null || true
    sleep 2

    ADAPTIVE_MODE="$adaptive_flag" docker compose up -d --build \
        node1 node2 node3 node4 node5 prometheus 2>&1 | tail -5

    wait_healthy 120
    log "Warm-up ${WARMUP}…"
    sleep "${WARMUP%s}"
    wait_leader 60 || true
}

stop_cluster() {
    log "Stopping cluster"
    cd "$PROJECT_DIR"
    docker compose down --remove-orphans --timeout 5 2>/dev/null || true
    sleep 3
}

###############################################################################
# BENCHMARK RUNNER
#
# Runs the benchmark tool inside Docker (same network as the cluster).
# After it finishes, copies the benchmark JSON from the container volume
# into the structured output directory.
###############################################################################

# run_benchmark <out_dir> <duration> <pattern> <rate>
#   Blocking — waits for benchmark to finish.
run_benchmark() {
    local out_dir=$1 duration=$2 pattern=$3 rate=$4

    mkdir -p "$out_dir"

    log "Benchmark: duration=$duration pattern=$pattern rate=$rate"
    cd "$PROJECT_DIR"

    # Run benchmark container; output goes to stdout (tee'd to log file).
    # The container also writes JSON to /app/results/ (mounted volume).
    docker compose run --rm \
        -e TARGETS="node1:9200,node2:9200,node3:9200,node4:9200,node5:9200" \
        -e PATTERN="$pattern" \
        -e DURATION="$duration" \
        -e RATE="$rate" \
        -e BURST_RATE="$BENCH_BURST_RATE" \
        -e WARMUP="5s" \
        benchmark 2>&1 | tee "$out_dir/benchmark.log"

    # Find the latest benchmark JSON and copy it into our output dir.
    local latest
    latest=$(ls -t "$PROJECT_DIR/results/benchmark_"*.json 2>/dev/null | head -1) || true
    if [[ -n "$latest" && -f "$latest" ]]; then
        cp "$latest" "$out_dir/benchmark.json"
        ok "Results → $out_dir/benchmark.json"
    else
        warn "No benchmark JSON found in results/"
    fi

    # Find and copy the corresponding timeline JSON
    local timeline
    if [[ -n "$latest" ]]; then
        # Extract timestamp from benchmark filename (e.g., benchmark_20260312_021311.json)
        local timestamp
        timestamp=$(basename "$latest" | sed 's/benchmark_\(.*\)\.json/\1/')
        timeline="$PROJECT_DIR/results/timeline_${timestamp}.json"
        if [[ -f "$timeline" ]]; then
            cp "$timeline" "$out_dir/timeline.json"
            ok "Timeline → $out_dir/timeline.json"
        fi
    fi
}

# run_benchmark_bg <out_dir> <duration> <pattern> <rate>
#   Non-blocking — starts benchmark in background.
#   Sets BG_BENCH_PID and BG_BENCH_DIR for wait_benchmark.
BG_BENCH_PID=""
BG_BENCH_DIR=""
BG_BENCH_CONTAINER=""

run_benchmark_bg() {
    local out_dir=$1 duration=$2 pattern=$3 rate=$4
    local container_name="raft-bench-bg-$$"

    mkdir -p "$out_dir"
    BG_BENCH_DIR="$out_dir"
    BG_BENCH_CONTAINER="$container_name"

    log "Benchmark (background): duration=$duration pattern=$pattern rate=$rate"
    cd "$PROJECT_DIR"

    docker compose run --rm --name "$container_name" \
        -e TARGETS="node1:9200,node2:9200,node3:9200,node4:9200,node5:9200" \
        -e PATTERN="$pattern" \
        -e DURATION="$duration" \
        -e RATE="$rate" \
        -e BURST_RATE="$BENCH_BURST_RATE" \
        -e WARMUP="5s" \
        benchmark > "$out_dir/benchmark.log" 2>&1 &
    BG_BENCH_PID=$!

    ok "Benchmark PID=$BG_BENCH_PID (container=$container_name)"
}

wait_benchmark() {
    log "Waiting for background benchmark to finish…"
    if [[ -n "$BG_BENCH_PID" ]]; then
        wait "$BG_BENCH_PID" 2>/dev/null || true
    fi

    # Copy benchmark JSON
    local latest
    latest=$(ls -t "$PROJECT_DIR/results/benchmark_"*.json 2>/dev/null | head -1) || true
    if [[ -n "$latest" && -f "$latest" ]]; then
        cp "$latest" "$BG_BENCH_DIR/benchmark.json"
        ok "Results → $BG_BENCH_DIR/benchmark.json"

        # Copy timeline data as well
        local timeline
        local timestamp
        timestamp=$(basename "$latest" | sed 's/benchmark_\(.*\)\.json/\1/')
        timeline="$PROJECT_DIR/results/timeline_${timestamp}.json"
        if [[ -f "$timeline" ]]; then
            cp "$timeline" "$BG_BENCH_DIR/timeline.json"
            ok "Timeline → $BG_BENCH_DIR/timeline.json"
        fi
    else
        warn "No benchmark JSON found in background results/"
    fi

    BG_BENCH_PID=""
    BG_BENCH_DIR=""
    BG_BENCH_CONTAINER=""
}

###############################################################################
# SCENARIO 1: Stable Network
#
# Goal : Prove adaptive doesn't hurt performance on a good network.
# Method: Run steady workload, no injections. Compare static vs adaptive.
###############################################################################

run_s1() {
    local mode=$1
    local out_dir="$RUN_DIR/s1_stable/$mode"

    header "SCENARIO 1 — Stable Network [$mode]"
    divider

    run_benchmark "$out_dir" "$S1_DURATION" "steady" "$BENCH_RATE"

    ok "Scenario 1 [$mode] complete → $out_dir/"
}

###############################################################################
# SCENARIO 2: High Latency Spike
#
# Goal : Show static fails (timeouts/re-elections) vs adaptive (adjusts).
# Method:
#   t=0          benchmark starts
#   t=SPIKE_AT   inject 150ms spike via /tc
#   t=RESTORE_AT restore normal via /tc
#   t=DURATION   benchmark ends
###############################################################################

run_s2() {
    local mode=$1
    local out_dir="$RUN_DIR/s2_latency_spike/$mode"

    header "SCENARIO 2 — High Latency Spike [$mode]"
    divider

    # Start benchmark in background
    run_benchmark_bg "$out_dir" "$S2_DURATION" "steady" "$BENCH_RATE"

    # Phase 1: Baseline
    log "[Phase 1] Baseline — normal latency for ${S2_SPIKE_AT}s"
    sleep "$S2_SPIKE_AT"

    # Phase 2: Inject spike
    local spike_start
    spike_start=$(date -Iseconds)
    log "[Phase 2] Injecting latency spike"
    tc_inject_spike

    local spike_hold=$(( S2_RESTORE_AT - S2_SPIKE_AT ))
    log "[Phase 2] Spike active for ${spike_hold}s"
    sleep "$spike_hold"

    # Phase 3: Restore
    local restore_time
    restore_time=$(date -Iseconds)
    log "[Phase 3] Restoring normal latency"
    tc_restore_all

    # Wait for benchmark to finish
    wait_benchmark

    # Save timeline metadata
    cat > "$out_dir/timeline.json" <<EOF
{
  "scenario": "s2_latency_spike",
  "mode": "$mode",
  "total_duration": "$S2_DURATION",
  "phases": [
    {
      "name": "baseline",
      "start_s": 0,
      "end_s": $S2_SPIKE_AT,
      "tc": "normal"
    },
    {
      "name": "spike",
      "start_s": $S2_SPIKE_AT,
      "end_s": $S2_RESTORE_AT,
      "spike_injected_at": "$spike_start",
      "tc_hub": $TC_HUB_SPIKE,
      "tc_remote": $TC_REMOTE_SPIKE
    },
    {
      "name": "recovery",
      "start_s": $S2_RESTORE_AT,
      "end_s": ${S2_DURATION%s},
      "restored_at": "$restore_time",
      "tc_hub": $TC_HUB_NORMAL,
      "tc_node4": $TC_NODE4_NORMAL,
      "tc_node5": $TC_NODE5_NORMAL
    }
  ]
}
EOF

    ok "Scenario 2 [$mode] complete → $out_dir/"
}

###############################################################################
# SCENARIO 3: Network Partition
#
# Goal : Simulate split-brain, measure recovery time.
# Method:
#   t=0             benchmark starts
#   t=PARTITION_AT  isolate leader from nodes 4 & 5 via /partition
#   t=HEAL_AT       heal via DELETE /partition (auto-measures recovery)
#   t=DURATION      benchmark ends (collects partition_recovery_ms)
###############################################################################

run_s3() {
    local mode=$1
    local out_dir="$RUN_DIR/s3_partition/$mode"

    header "SCENARIO 3 — Network Partition [$mode]"
    divider

    # Start benchmark in background
    run_benchmark_bg "$out_dir" "$S3_DURATION" "steady" "$BENCH_RATE"

    # Phase 1: Normal operation
    log "[Phase 1] Normal operation for ${S3_PARTITION_AT}s"
    sleep "$S3_PARTITION_AT"

    # Find leader
    local leader_info leader_port leader_id
    leader_info=$(find_leader) || leader_info=""
    if [[ -n "$leader_info" ]]; then
        leader_port=${leader_info%%:*}
        leader_id=${leader_info##*:}
    else
        warn "No leader found, defaulting to node 1 (:9201)"
        leader_port=9201
        leader_id=1
    fi
    log "Leader: node $leader_id (port :$leader_port)"

    # Phase 2: Create partition
    local partition_time
    partition_time=$(date -Iseconds)
    log "[Phase 2] Creating partition — isolating node $leader_id from [4, 5]"
    local part_resp
    part_resp=$(curl -sf -X POST "http://localhost:$leader_port/partition" \
        -H "Content-Type: application/json" \
        -d '{"isolate":[4,5]}' 2>/dev/null) || part_resp='{"success":false}'
    echo "$part_resp" | jq . 2>/dev/null || echo "$part_resp"

    local hold_time=$(( S3_HEAL_AT - S3_PARTITION_AT ))
    log "[Phase 2] Partition active for ${hold_time}s"
    sleep "$hold_time"

    # Phase 3: Heal
    local heal_time
    heal_time=$(date -Iseconds)
    log "[Phase 3] Healing partition"
    local heal_resp
    heal_resp=$(curl -sf -X DELETE "http://localhost:$leader_port/partition" 2>/dev/null) || heal_resp='{}'
    echo "$heal_resp" | jq . 2>/dev/null || echo "$heal_resp"

    # Wait for benchmark to finish (it auto-collects partition metrics)
    wait_benchmark

    # Collect recovery metrics from all nodes
    log "Collecting recovery metrics from all nodes"
    {
        echo '['
        local first=true
        for port in "${ALL_PORTS[@]}"; do
            $first || echo ','
            first=false

            local raw nid recovery_ms duration_ms part_status
            raw=$(curl -sf "http://localhost:$port/metrics" 2>/dev/null) || raw=""
            nid=$(curl -sf "http://localhost:$port/status" 2>/dev/null | jq -r '.id // 0') || nid=0
            recovery_ms=$(echo "$raw" | grep '^raft_partition_recovery_milliseconds{' | awk '{print $2}') || recovery_ms=0
            duration_ms=$(echo "$raw" | grep '^raft_partition_duration_milliseconds{' | awk '{print $2}') || duration_ms=0
            part_status=$(curl -sf "http://localhost:$port/partition" 2>/dev/null) || part_status='{}'

            cat <<ITEM
  {
    "node_id": ${nid:-0},
    "port": $port,
    "recovery_ms": ${recovery_ms:-0},
    "partition_duration_ms": ${duration_ms:-0},
    "partition_status": $part_status
  }
ITEM
        done
        echo ']'
    } | jq . > "$out_dir/recovery.json" 2>/dev/null || true

    # Save timeline metadata
    cat > "$out_dir/timeline.json" <<EOF
{
  "scenario": "s3_partition",
  "mode": "$mode",
  "total_duration": "$S3_DURATION",
  "leader_node_id": $leader_id,
  "leader_port": $leader_port,
  "isolated_peers": [4, 5],
  "phases": [
    {
      "name": "normal",
      "start_s": 0,
      "end_s": $S3_PARTITION_AT
    },
    {
      "name": "partitioned",
      "start_s": $S3_PARTITION_AT,
      "end_s": $S3_HEAL_AT,
      "partition_created_at": "$partition_time"
    },
    {
      "name": "recovery",
      "start_s": $S3_HEAL_AT,
      "end_s": ${S3_DURATION%s},
      "partition_healed_at": "$heal_time"
    }
  ]
}
EOF

    ok "Scenario 3 [$mode] complete → $out_dir/"
}

###############################################################################
# SCENARIO 4: Overhead Analysis
#
# Goal : Show PID controller overhead < 5% CPU.
# Method:
#   1. Snapshot CPU/mem from /metrics (before)
#   2. Run benchmark workload
#   3. Snapshot CPU/mem from /metrics (after)
#   4. Compute: overhead% = tuning_cpu_delta / total_cpu_delta × 100
###############################################################################

run_s4() {
    local mode=$1
    local out_dir="$RUN_DIR/s4_overhead/$mode"
    mkdir -p "$out_dir"

    header "SCENARIO 4 — Overhead Analysis [$mode]"
    divider

    # Find leader for consistent measurement
    local leader_info leader_port
    leader_info=$(find_leader) || leader_info=""
    leader_port=${leader_info%%:*}
    [[ -z "$leader_port" ]] && leader_port=9201

    # Snapshot BEFORE
    log "Collecting pre-benchmark metrics snapshot"
    collect_metrics_snapshot "$leader_port" "$out_dir/before.json"

    # Run benchmark
    run_benchmark "$out_dir" "$S4_DURATION" "steady" "$BENCH_RATE"

    # Snapshot AFTER (re-find leader in case it changed)
    leader_info=$(find_leader) || true
    leader_port=${leader_info%%:*}
    [[ -z "$leader_port" ]] && leader_port=9201

    log "Collecting post-benchmark metrics snapshot"
    collect_metrics_snapshot "$leader_port" "$out_dir/after.json"

    # Compute overhead
    if [[ -f "$out_dir/before.json" && -f "$out_dir/after.json" ]]; then
        local cpu_b cpu_a tuning_b tuning_a mem_b mem_a
        cpu_b=$(jq -r '.process_cpu_seconds_total' "$out_dir/before.json")
        cpu_a=$(jq -r '.process_cpu_seconds_total' "$out_dir/after.json")
        tuning_b=$(jq -r '.tuning_duration_sum_s' "$out_dir/before.json")
        tuning_a=$(jq -r '.tuning_duration_sum_s' "$out_dir/after.json")
        mem_b=$(jq -r '.go_memstats_alloc_bytes' "$out_dir/before.json")
        mem_a=$(jq -r '.go_memstats_alloc_bytes' "$out_dir/after.json")

        local cpu_delta tuning_delta overhead_pct verdict
        cpu_delta=$(echo "$cpu_a - $cpu_b" | bc 2>/dev/null || echo "0")
        tuning_delta=$(echo "$tuning_a - $tuning_b" | bc 2>/dev/null || echo "0")

        if [[ -n "$cpu_delta" ]] && (( $(echo "$cpu_delta > 0" | bc 2>/dev/null || echo 0) )); then
            overhead_pct=$(echo "scale=4; ($tuning_delta / $cpu_delta) * 100" | bc 2>/dev/null || echo "0")
        else
            overhead_pct="0"
        fi

        if (( $(echo "$overhead_pct < 5" | bc 2>/dev/null || echo 0) )); then
            verdict="PASS"
        else
            verdict="FAIL"
        fi

        cat > "$out_dir/summary.json" <<EOF
{
  "scenario": "s4_overhead",
  "mode": "$mode",
  "cpu_seconds_before": $cpu_b,
  "cpu_seconds_after": $cpu_a,
  "cpu_seconds_delta": $cpu_delta,
  "tuning_seconds_before": $tuning_b,
  "tuning_seconds_after": $tuning_a,
  "tuning_seconds_delta": $tuning_delta,
  "overhead_percent": $overhead_pct,
  "threshold_percent": 5,
  "verdict": "$verdict",
  "memory_alloc_before_bytes": $mem_b,
  "memory_alloc_after_bytes": $mem_a
}
EOF

        divider
        echo -e "  ${BOLD}CPU delta     :${NC} ${cpu_delta}s"
        echo -e "  ${BOLD}Tuning delta  :${NC} ${tuning_delta}s"
        if [[ "$verdict" == "PASS" ]]; then
            echo -e "  ${BOLD}Overhead      :${NC} ${GREEN}${overhead_pct}%${NC} — PASS (< 5%)"
        else
            echo -e "  ${BOLD}Overhead      :${NC} ${RED}${overhead_pct}%${NC} — FAIL (≥ 5%)"
        fi
        divider
    fi

    ok "Scenario 4 [$mode] complete → $out_dir/"
}

###############################################################################
# SUMMARY GENERATOR
###############################################################################

generate_summary() {
    log "Generating combined summary"
    local out="$RUN_DIR/summary.json"

    # Helper: extract a field from a benchmark.json if it exists
    _bval() {
        local file="$1" field="$2"
        if [[ -f "$file" ]]; then
            jq -r ".$field // 0" "$file" 2>/dev/null || echo "0"
        else
            echo "0"
        fi
    }

    cat > "$out" <<EOF
{
  "run_timestamp": "$TIMESTAMP",
  "scenarios": {
    "s1_stable": {
      "static": {
        "throughput": $(_bval "$RUN_DIR/s1_stable/static/benchmark.json" "throughput"),
        "latency_p50_ms": $(_bval "$RUN_DIR/s1_stable/static/benchmark.json" "latency_p50_ms"),
        "latency_p99_ms": $(_bval "$RUN_DIR/s1_stable/static/benchmark.json" "latency_p99_ms"),
        "elections_total": $(_bval "$RUN_DIR/s1_stable/static/benchmark.json" "elections_total")
      },
      "adaptive": {
        "throughput": $(_bval "$RUN_DIR/s1_stable/adaptive/benchmark.json" "throughput"),
        "latency_p50_ms": $(_bval "$RUN_DIR/s1_stable/adaptive/benchmark.json" "latency_p50_ms"),
        "latency_p99_ms": $(_bval "$RUN_DIR/s1_stable/adaptive/benchmark.json" "latency_p99_ms"),
        "elections_total": $(_bval "$RUN_DIR/s1_stable/adaptive/benchmark.json" "elections_total")
      }
    },
    "s2_latency_spike": {
      "static": {
        "throughput": $(_bval "$RUN_DIR/s2_latency_spike/static/benchmark.json" "throughput"),
        "latency_p50_ms": $(_bval "$RUN_DIR/s2_latency_spike/static/benchmark.json" "latency_p50_ms"),
        "latency_p99_ms": $(_bval "$RUN_DIR/s2_latency_spike/static/benchmark.json" "latency_p99_ms"),
        "elections_total": $(_bval "$RUN_DIR/s2_latency_spike/static/benchmark.json" "elections_total"),
        "failures": $(_bval "$RUN_DIR/s2_latency_spike/static/benchmark.json" "failures")
      },
      "adaptive": {
        "throughput": $(_bval "$RUN_DIR/s2_latency_spike/adaptive/benchmark.json" "throughput"),
        "latency_p50_ms": $(_bval "$RUN_DIR/s2_latency_spike/adaptive/benchmark.json" "latency_p50_ms"),
        "latency_p99_ms": $(_bval "$RUN_DIR/s2_latency_spike/adaptive/benchmark.json" "latency_p99_ms"),
        "elections_total": $(_bval "$RUN_DIR/s2_latency_spike/adaptive/benchmark.json" "elections_total"),
        "failures": $(_bval "$RUN_DIR/s2_latency_spike/adaptive/benchmark.json" "failures")
      }
    },
    "s3_partition": {
      "static": {
        "throughput": $(_bval "$RUN_DIR/s3_partition/static/benchmark.json" "throughput"),
        "elections_total": $(_bval "$RUN_DIR/s3_partition/static/benchmark.json" "elections_total"),
        "partition_recovery_ms": $(_bval "$RUN_DIR/s3_partition/static/benchmark.json" "partition_recovery_ms"),
        "partition_duration_ms": $(_bval "$RUN_DIR/s3_partition/static/benchmark.json" "partition_duration_ms")
      },
      "adaptive": {
        "throughput": $(_bval "$RUN_DIR/s3_partition/adaptive/benchmark.json" "throughput"),
        "elections_total": $(_bval "$RUN_DIR/s3_partition/adaptive/benchmark.json" "elections_total"),
        "partition_recovery_ms": $(_bval "$RUN_DIR/s3_partition/adaptive/benchmark.json" "partition_recovery_ms"),
        "partition_duration_ms": $(_bval "$RUN_DIR/s3_partition/adaptive/benchmark.json" "partition_duration_ms")
      }
    },
    "s4_overhead": {
      "static": {
        "overhead_percent": $(_bval "$RUN_DIR/s4_overhead/static/summary.json" "overhead_percent"),
        "verdict": "$(_bval "$RUN_DIR/s4_overhead/static/summary.json" "verdict")"
      },
      "adaptive": {
        "overhead_percent": $(_bval "$RUN_DIR/s4_overhead/adaptive/summary.json" "overhead_percent"),
        "verdict": "$(_bval "$RUN_DIR/s4_overhead/adaptive/summary.json" "verdict")"
      }
    }
  }
}
EOF

    # Pretty-print it
    local tmp
    tmp=$(jq . "$out" 2>/dev/null) && echo "$tmp" > "$out"

    ok "Summary → $out"
    echo
    cat "$out"
}

###############################################################################
# MAIN ENTRY POINT
###############################################################################

print_banner() {
    echo
    echo "╔══════════════════════════════════════════════════════════════════╗"
    echo "║           ADAPTIVE RAFT — EXPERIMENT RUNNER                    ║"
    echo "╠══════════════════════════════════════════════════════════════════╣"
    echo "║  Timestamp : $TIMESTAMP                                 ║"
    echo "║  Output    : results/$TIMESTAMP/                        ║"
    echo "╠══════════════════════════════════════════════════════════════════╣"
    echo "║  Structure per scenario:                                       ║"
    echo "║    <scenario>/<mode>/benchmark.json   ← metrics                ║"
    echo "║    <scenario>/<mode>/benchmark.log    ← full log               ║"
    echo "║    <scenario>/<mode>/timeline.json    ← event timestamps       ║"
    echo "╚══════════════════════════════════════════════════════════════════╝"
    echo
}

run_scenario_both_modes() {
    local scenario=$1
    for mode in static adaptive; do
        start_cluster "$mode"
        "$scenario" "$mode"
        stop_cluster
    done
}

main() {
    local scenario_arg="${1:-all}"
    local mode_arg="${2:-all}"

    print_banner
    mkdir -p "$RUN_DIR"

    case "$scenario_arg" in
        # ── Single scenario ───────────────────────────────────────
        s1|s2|s3|s4)
            local run_fn="run_${scenario_arg}"
            if [[ "$mode_arg" == "all" ]]; then
                for mode in static adaptive; do
                    start_cluster "$mode"
                    "$run_fn" "$mode"
                    stop_cluster
                done
            else
                start_cluster "$mode_arg"
                "$run_fn" "$mode_arg"
                stop_cluster
            fi
            ;;

        # ── All scenarios ─────────────────────────────────────────
        all)
            if [[ "$mode_arg" == "all" ]]; then
                for mode in static adaptive; do
                    echo
                    echo "┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓"
                    printf "┃  MODE: %-51s ┃\n" "${mode^^}"
                    echo "┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛"

                    start_cluster "$mode"
                    run_s1 "$mode"; sleep 5
                    run_s2 "$mode"; sleep 5
                    run_s3 "$mode"; sleep 5
                    run_s4 "$mode"
                    stop_cluster
                done
            else
                start_cluster "$mode_arg"
                run_s1 "$mode_arg"; sleep 5
                run_s2 "$mode_arg"; sleep 5
                run_s3 "$mode_arg"; sleep 5
                run_s4 "$mode_arg"
                stop_cluster
            fi
            ;;

        *)
            fail "Unknown scenario: $scenario_arg"
            echo "Usage: $0 [s1|s2|s3|s4|all] [static|adaptive|all]"
            echo
            echo "Examples:"
            echo "  $0              # all scenarios, both modes"
            echo "  $0 s1           # scenario 1 only, both modes"
            echo "  $0 s2 adaptive  # scenario 2, adaptive only"
            echo "  $0 all static   # all scenarios, static only"
            exit 1
            ;;
    esac

    # Generate combined summary
    generate_summary

    echo
    echo "╔══════════════════════════════════════════════════════════════════╗"
    echo "║                    ALL EXPERIMENTS COMPLETE                     ║"
    echo "╚══════════════════════════════════════════════════════════════════╝"
    echo
    echo "Results directory:"
    echo "  $RUN_DIR/"
    echo
    if command -v tree &>/dev/null; then
        tree "$RUN_DIR" --dirsfirst -C 2>/dev/null || find "$RUN_DIR" -type f | sort
    else
        find "$RUN_DIR" -type f | sort | sed "s|$RUN_DIR/|  |"
    fi
    echo
}

main "$@"
