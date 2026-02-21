# Adaptive Raft – PI-Controlled Election Timeout

A 5-node Raft consensus cluster in Go where each node runs a PI (Proportional-Integral) controller that adaptively tunes the election timeout based on observed round-trip time (RTT) to peers.

## Overview

This project demonstrates an **observability-driven approach** to Raft in geo-dispersed or high-variance latency environments. Instead of fixed timeouts, each node independently monitors RTT to its peers and feeds the moving average into a lightweight PI controller that adjusts the election timeout in real time.

### Key Features

- **Full Raft Implementation**: Leader election, log replication, term logic, majority voting, safety guarantees untouched
- **Adaptive Control**: PI controller computes `electionTimeout = T_base + Kp·error + Ki·∫error`, clamped to `[T_min, T_max]`
- **Heartbeat Derivation**: `heartbeatInterval = electionTimeout / R` (default R=5), always derived—never independently tuned
- **RTT Measurement**: Measured from actual HTTP RPC latency; per-peer sliding window computes moving average
- **Back-Calculation Anti-Windup**: Prevents integral term from accumulating unsustainable error when output is saturated
- **Pluggable Metrics**: `metrics.Observer` interface supports OpenTelemetry, Prometheus, or custom backends
- **Synchronous RPC**: HTTP+JSON with measured round-trip for accurate RTT feedback

## Architecture

```
etcd-raft/
├── main.go               # CLI entry point, flag parsing
├── start_cluster.sh      # Launch script for all 5 nodes
├── controller/
│   └── controller.go     # PIController: Update, Reset, GetElectionTimeout
├── metrics/
│   └── metrics.go        # Observer interface, RTTWindow, LogObserver
├── raft/
│   └── node.go           # Raft state machine (follower/candidate/leader)
└── transport/
    └── transport.go      # HTTP/JSON RPC layer
```

## Requirements

- **Go 1.16+**
- macOS, Linux, or Windows with network stack

## Building

```bash
go build ./...
```

Or build the binary:

```bash
go build -o adaptive-raft .
```

## Running

### Quick Start (All 5 Nodes Locally)

```bash
./start_cluster.sh
```

This will:

1. Compile the binary
2. Launch 5 nodes on ports `9001–9005` with IDs `1–5`
3. Establish a full-mesh network
4. Begin leader election

Press `Ctrl-C` to stop all nodes gracefully.

### Manual (Single Node)

Start one node:

```bash
go run main.go \
  -id 1 \
  -port 9001 \
  -peers "2=127.0.0.1:9002,3=127.0.0.1:9003,4=127.0.0.1:9004,5=127.0.0.1:9005"
```

Then launch others in separate terminals with appropriate `-id` and `-port` values.

## Configuration

### Flags

| Flag            | Default  | Description                                           |
| --------------- | -------- | ----------------------------------------------------- |
| `-id`           | —        | Unique node ID (1–5, required)                        |
| `-port`         | —        | Listen port (required)                                |
| `-peers`        | —        | Peer list: `id=host:port,id=host:port,...` (required) |
| `-baseline-rtt` | `5ms`    | Expected "normal" round-trip time                     |
| `-t-base`       | `300ms`  | Base election timeout (before correction)             |
| `-t-min`        | `150ms`  | Minimum election timeout bound                        |
| `-t-max`        | `3000ms` | Maximum election timeout bound                        |
| `-kp`           | `2.0`    | Proportional gain                                     |
| `-ki`           | `0.5`    | Integral gain                                         |
| `-ratio`        | `5.0`    | Heartbeat ratio (R)                                   |
| `-rtt-window`   | `20`     | RTT sliding window size (samples)                     |

### Example: Different Baseline RTT

For a geo-distributed cluster with higher latency:

```bash
go run main.go \
  -id 1 \
  -port 9001 \
  -peers "2=10.0.1.2:9001,3=10.0.2.2:9001,4=10.0.3.2:9001,5=10.0.4.2:9001" \
  -baseline-rtt 50ms \
  -t-base 1s \
  -t-min 500ms \
  -t-max 5s
```

## How It Works

### 1. RTT Sampling

Every time a node sends an RPC (heartbeat or vote request) to a peer, the transport layer measures the HTTP round-trip time and records it into a per-peer RTT sliding window.

### 2. Aggregation

The node computes a moving average across all peer RTT windows and feeds this into the PI controller.

### 3. PI Control

```
error        = moving_avg_RTT − baseline_RTT
raw_output   = T_base + Kp·error + Ki·∫error
clamped      = clamp(raw_output, T_min, T_max)
integral     = back_calculate_for_antiwindup(clamped, error)
```

### 4. Timeout Update

The clamped output becomes the new `electionTimeout`. The `heartbeatInterval` is always derived as `electionTimeout / R`.

### 5. Safety

- Election timeout is applied with jitter: `[T, T·1.5)`
- Raft term logic, voting, and log consistency checks are **untouched**
- The controller only tunes timeouts; it does not modify consensus logic

## Observability

### Logging

By default, the node logs to stdout with structured entries:

```
[node=1] controller role=leader term=1 rtt=5.12ms baseline=5ms err=-0.12ms electionTimeout=300ms heartbeat=60ms
[node=2] rtt peer=1 rtt=4.98ms
[node=1] role_change role=leader term=1
```

### Custom Metrics Backend

Implement the `metrics.Observer` interface to plug in OpenTelemetry, Prometheus, or any other backend:

```go
type Observer interface {
    RecordRTT(peerID uint64, rtt time.Duration)
    RecordControllerOutput(snapshot ControllerSnapshot)
    RecordRoleChange(role RaftRole, term uint64)
}
```

Pass your observer to `raft.Config`:

```go
nodeCfg := raft.Config{
    // ... other fields ...
    Observer: myCustomObserver,
}
```

## Design Principles

1. **Simplicity**: No external ML. Just a straightforward PI controller with anti-windup.
2. **Safety First**: Raft invariants (term, voting, quorum) are never modified by the controller.
3. **Local Control**: Each node runs its own controller; no centralized tuning.
4. **Production-Ready Foundations**: Proper locking, graceful shutdown, structured logging.
5. **Observable**: Easy to add metrics, tracing, and debugging output.

## Limitations & Simplifications

- **In-memory log**: No persistence or snapshots
- **Single entry per RPC**: Log replication sends one entry at a time (not optimized for bulk)
- **No cluster membership changes**: Fixed 5-node cluster
- **Synchronous replication**: No pipelining
- **HTTP transport**: Not optimized for cross-datacenter (use gRPC for production)

These are intentional to keep the focus on the adaptive timeout mechanism.

## Testing the Cluster

### Leader Election

Once all 5 nodes are running, one should become leader within ~1 second. Watch the logs:

```
[raft] node=3 became leader term=1
```

### Heartbeats

The leader will send heartbeats at the rate of `heartbeatInterval`:

```
[transport] node=1 listening on 0.0.0.0:9001
```

### Adaptive Adjustment

Introduce artificial latency (on macOS with `Network Utility` or `tc` on Linux) and observe the election timeout increase as error rises:

```bash
# Increase RTT by 50ms
sudo tc qdisc add dev lo root netem delay 50ms
```

Watch the logs—`electionTimeout` should grow. When latency is removed, it should recover.

## Performance Notes

- **Memory**: ~1MB per node (minimal log overhead)
- **CPU**: Low (event-driven, mostly idle on followers)
- **Network**: ~1 heartbeat per `heartbeatInterval` per peer

For a 5-node cluster with default settings:

- Heartbeat period: ~60ms (300ms / 5)
- Traffic per leader: ~5 heartbeats/sec × 5 peers = ~25 HTTP requests/sec
- Typical latency: <10ms (local), <100ms (geo-distributed)

## Future Enhancements

- [ ] Persistent log + snapshots
- [ ] Client request API (read/write to committed log)
- [ ] gRPC transport option
- [ ] OpenTelemetry integration (example)
- [ ] Non-uniform peer latencies (per-peer PI controllers)
- [ ] Dynamic cluster membership

## License

MIT

## References

- [Raft Consensus Algorithm](https://raft.github.io/)
- [PI Controller Tuning](https://en.wikipedia.org/wiki/Proportional%E2%80%93integral_controller)
- [Anti-Windup Techniques](https://en.wikipedia.org/wiki/Integral_windup)
