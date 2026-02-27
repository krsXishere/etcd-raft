package metrics

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// PrometheusObserver implements Observer interface and exports metrics to Prometheus.
type PrometheusObserver struct {
	nodeID    uint64
	nodeLabel string
}

// ============================================================================
// Cluster 1 — Adaptive Parameter Tuning (8 metrik)
//
// Sumber data  : RecordRTT(), RecordControllerOutput()
// Fungsi       : Mengukur bagaimana PID controller menyesuaikan election
//                timeout & heartbeat berdasarkan kondisi jaringan (RTT).
// ============================================================================
var (
	// Gauge: RTT mentah ke setiap peer (ms)
	// ← RecordRTT(peerID, rtt) — parameter rtt
	rttGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Subsystem: "adaptive",
		Name:      "rtt_milliseconds",
		Help:      "Round-trip time to peer in milliseconds",
	}, []string{"node_id", "peer_id"})

	// Histogram: distribusi RTT (detik) — untuk P50/P95/P99
	// ← RecordRTT(peerID, rtt) — parameter rtt
	rttHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "raft",
		Subsystem: "adaptive",
		Name:      "rtt_duration_seconds",
		Help:      "RTT duration distribution in seconds",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5},
	}, []string{"node_id", "peer_id"})

	// Gauge: RTT rata-rata dari controller snapshot (ms)
	// ← RecordControllerOutput(s) — parameter s.CurrentRTT
	rttAvgGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Subsystem: "adaptive",
		Name:      "rtt_avg_milliseconds",
		Help:      "Average RTT from controller snapshot in milliseconds",
	}, []string{"node_id"})

	// Gauge: RTT baseline sebagai acuan PID (ms)
	// ← RecordControllerOutput(s) — parameter s.BaselineRTT
	baselineRTTGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Subsystem: "adaptive",
		Name:      "baseline_rtt_milliseconds",
		Help:      "Configured baseline RTT in milliseconds",
	}, []string{"node_id"})

	// Gauge: PID error = currentRTT − baselineRTT (ms)
	// ← RecordControllerOutput(s) — parameter s.Error
	controllerErrorGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Subsystem: "adaptive",
		Name:      "controller_error_milliseconds",
		Help:      "PID controller error (RTT - baseline) in milliseconds",
	}, []string{"node_id"})

	// Gauge: PID derivative term dError/dt
	// ← RecordControllerOutput(s) — parameter s.Derivative
	controllerDerivativeGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Subsystem: "adaptive",
		Name:      "controller_derivative",
		Help:      "PID controller derivative term (dError/dt in seconds per second)",
	}, []string{"node_id"})

	// Gauge: election timeout yang dihasilkan PID (ms)
	// ← RecordControllerOutput(s) — parameter s.ElectionTimeout
	electionTimeoutGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Subsystem: "adaptive",
		Name:      "election_timeout_milliseconds",
		Help:      "Current election timeout in milliseconds",
	}, []string{"node_id"})

	// Gauge: heartbeat interval yang dihasilkan PID (ms)
	// ← RecordControllerOutput(s) — parameter s.HeartbeatInterval
	heartbeatIntervalGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Subsystem: "adaptive",
		Name:      "heartbeat_interval_milliseconds",
		Help:      "Current heartbeat interval in milliseconds",
	}, []string{"node_id"})
)

// ============================================================================
// Cluster 2 — System Throughput (1 metrik)
//
// Sumber data  : RecordCommit()
// Fungsi       : Mengukur jumlah log entries yang berhasil di-commit.
// Catatan      : Untuk proposal/sec dan success rate, gunakan recording rule
//                dari histogram Cluster 3 (_count suffix).
//
// Grafana      : rate(raft_throughput_committed_entries_total[1m])
// ============================================================================
var (
	// Counter: total entri yang di-commit oleh mayoritas
	// ← RecordCommit(term, index) — increment per entry
	committedEntriesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "raft",
		Subsystem: "throughput",
		Name:      "committed_entries_total",
		Help:      "Total log entries committed (reached majority consensus)",
	}, []string{"node_id"})
)

// ============================================================================
// Cluster 3 — Consensus Latency (1 metrik)
//
// Sumber data  : RecordProposalLatency()
// Fungsi       : Mengukur waktu proposal → commit. Label status membedakan
//                sukses/gagal. P95/P99 dihitung via recording rule.
// Bonus        : _count suffix otomatis jadi proposals_committed / proposals_failed
//                sehingga tidak perlu counter terpisah.
//
// Grafana      : histogram_quantile(0.99, rate(raft_consensus_proposal_latency_seconds_bucket{status="success"}[5m]))
// ============================================================================
var (
	// Histogram: latensi proposal (proposal → commit/fail)
	// ← RecordProposalLatency(latency, success) — parameter latency, label status
	proposalLatencyHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "raft",
		Subsystem: "consensus",
		Name:      "proposal_latency_seconds",
		Help:      "Distribution of proposal latency in seconds",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
	}, []string{"node_id", "status"})
)

// ============================================================================
// Cluster 4 — Control Overhead (1 metrik)
//
// Sumber data  : RecordTuningDuration()
// Fungsi       : Membuktikan bahwa PID controller ringan (low overhead).
// Built-in     : process_cpu_seconds_total dan go_memstats_alloc_bytes
//                sudah otomatis tersedia dari promhttp.Handler().
//
// Grafana      : histogram_quantile(0.99, rate(raft_overhead_tuning_duration_seconds_bucket[5m]))
// ============================================================================
var (
	// Histogram: waktu CPU untuk satu siklus PID computation
	// ← RecordTuningDuration(d) — parameter d
	tuningDurationHist = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "raft",
		Subsystem: "overhead",
		Name:      "tuning_duration_seconds",
		Help:      "CPU time spent computing one PID controller cycle",
		Buckets:   []float64{0.000001, 0.000005, 0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005},
	}, []string{"node_id"})
)

// ============================================================================
// Cluster 5 — Role & State Tracking (4 metrik)
//
// Sumber data  : RecordRoleChange(), RecordElectionDuration(),
//                RecordControllerOutput() (untuk term & role)
// Fungsi       : Melacak role, term, frekuensi pergantian, dan durasi election.
// ============================================================================
var (
	// Gauge: Raft term saat ini
	// ← RecordControllerOutput(s) — parameter s.Term
	// ← RecordRoleChange(role, term) — parameter term
	currentTermGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Subsystem: "state",
		Name:      "current_term",
		Help:      "Current Raft term number",
	}, []string{"node_id"})

	// Gauge: role berlabel (1 = aktif untuk role tersebut)
	// ← RecordControllerOutput(s) — parameter s.Role
	// ← RecordRoleChange(role, term) — parameter role
	nodeRoleGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Subsystem: "state",
		Name:      "node_role",
		Help:      "Node role indicator (1 if active for this role)",
	}, []string{"node_id", "role"})

	// Counter: total pergantian role
	// ← RecordRoleChange(role, term) — parameter role → label to_role
	roleChangeCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "raft",
		Subsystem: "state",
		Name:      "role_changes_total",
		Help:      "Total number of role changes",
	}, []string{"node_id", "to_role"})

	// Counter: berapa kali node menjadi leader
	// ← RecordRoleChange(role=leader, term)
	leaderElectionCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "raft",
		Subsystem: "state",
		Name:      "leader_elections_total",
		Help:      "Total number of times this node became leader",
	}, []string{"node_id"})

	// Histogram: durasi proses leader election (detik)
	// ← RecordElectionDuration(duration) — parameter duration
	leaderElectionDurationHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "raft",
		Subsystem: "state",
		Name:      "leader_election_duration_seconds",
		Help:      "Distribution of leader election duration in seconds",
		Buckets:   []float64{0.05, 0.1, 0.15, 0.2, 0.3, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0, 5.0},
	}, []string{"node_id"})
)

// ============================================================================
// Constructor
// ============================================================================

func NewPrometheusObserver(nodeID uint64) *PrometheusObserver {
	return &PrometheusObserver{
		nodeID:    nodeID,
		nodeLabel: strconv.FormatUint(nodeID, 10),
	}
}

// ── Cluster 1 — Adaptive Parameter Tuning ────────────────────────────────

func (p *PrometheusObserver) RecordRTT(peerID uint64, rtt time.Duration) {
	peerLabel := strconv.FormatUint(peerID, 10)
	rttGauge.WithLabelValues(p.nodeLabel, peerLabel).Set(float64(rtt.Milliseconds()))
	rttHistogram.WithLabelValues(p.nodeLabel, peerLabel).Observe(rtt.Seconds())
}

func (p *PrometheusObserver) RecordControllerOutput(s ControllerSnapshot) {
	rttAvgGauge.WithLabelValues(p.nodeLabel).Set(float64(s.CurrentRTT.Milliseconds()))
	baselineRTTGauge.WithLabelValues(p.nodeLabel).Set(float64(s.BaselineRTT.Milliseconds()))
	controllerErrorGauge.WithLabelValues(p.nodeLabel).Set(float64(s.Error.Milliseconds()))
	controllerDerivativeGauge.WithLabelValues(p.nodeLabel).Set(s.Derivative)
	electionTimeoutGauge.WithLabelValues(p.nodeLabel).Set(float64(s.ElectionTimeout.Milliseconds()))
	heartbeatIntervalGauge.WithLabelValues(p.nodeLabel).Set(float64(s.HeartbeatInterval.Milliseconds()))
	currentTermGauge.WithLabelValues(p.nodeLabel).Set(float64(s.Term))
	p.updateRoleMetrics(s.Role)
}

// ── Cluster 2 — System Throughput ────────────────────────────────────────

func (p *PrometheusObserver) RecordCommit(term uint64, index uint64) {
	committedEntriesTotal.WithLabelValues(p.nodeLabel).Inc()
}

// ── Cluster 3 — Consensus Latency ───────────────────────────────────────

func (p *PrometheusObserver) RecordProposalLatency(latency time.Duration, success bool) {
	status := "success"
	if !success {
		status = "failure"
	}
	proposalLatencyHistogram.WithLabelValues(p.nodeLabel, status).Observe(latency.Seconds())
}

// ── Cluster 4 — Control Overhead ────────────────────────────────────────

func (p *PrometheusObserver) RecordTuningDuration(d time.Duration) {
	tuningDurationHist.WithLabelValues(p.nodeLabel).Observe(d.Seconds())
}

// ── Cluster 5 — Role & State Tracking ───────────────────────────────────

func (p *PrometheusObserver) RecordRoleChange(role RaftRole, term uint64) {
	roleChangeCounter.WithLabelValues(p.nodeLabel, string(role)).Inc()
	if role == RoleLeader {
		leaderElectionCounter.WithLabelValues(p.nodeLabel).Inc()
	}
	currentTermGauge.WithLabelValues(p.nodeLabel).Set(float64(term))
	p.updateRoleMetrics(role)
}

func (p *PrometheusObserver) RecordElectionDuration(duration time.Duration) {
	leaderElectionDurationHistogram.WithLabelValues(p.nodeLabel).Observe(duration.Seconds())
}

// updateRoleMetrics sets the node_role gauge labels.
func (p *PrometheusObserver) updateRoleMetrics(role RaftRole) {
	nodeRoleGauge.WithLabelValues(p.nodeLabel, string(RoleFollower)).Set(0)
	nodeRoleGauge.WithLabelValues(p.nodeLabel, string(RoleCandidate)).Set(0)
	nodeRoleGauge.WithLabelValues(p.nodeLabel, string(RoleLeader)).Set(0)
	nodeRoleGauge.WithLabelValues(p.nodeLabel, string(role)).Set(1)
}
