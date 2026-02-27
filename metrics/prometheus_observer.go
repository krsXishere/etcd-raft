package metrics

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// PrometheusObserver implements Observer interface and exports metrics to Prometheus.
// This enables monitoring via Prometheus/Grafana for the observability plane.
type PrometheusObserver struct {
	nodeID    uint64
	nodeLabel string
}

// ============================================================================
// Cluster 1 — Adaptive Parameter Tuning
// Mengukur bagaimana PID controller menyesuaikan election timeout & heartbeat
// berdasarkan kondisi jaringan (RTT). Ini adalah metrik utama "adaptive" system.
// ============================================================================
var (
	// Gauge: RTT mentah ke setiap peer (ms)
	rttGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Subsystem: "adaptive",
		Name:      "rtt_milliseconds",
		Help:      "Round-trip time to peer in milliseconds",
	}, []string{"node_id", "peer_id"})

	// Gauge: RTT rata-rata dari controller snapshot (ms)
	rttAvgGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Subsystem: "adaptive",
		Name:      "rtt_avg_milliseconds",
		Help:      "Average RTT from controller snapshot in milliseconds",
	}, []string{"node_id"})

	// Gauge: RTT baseline sebagai acuan PID (ms)
	baselineRTTGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Subsystem: "adaptive",
		Name:      "baseline_rtt_milliseconds",
		Help:      "Configured baseline RTT in milliseconds",
	}, []string{"node_id"})

	// Gauge: PID error = currentRTT − baselineRTT (ms)
	controllerErrorGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Subsystem: "adaptive",
		Name:      "controller_error_milliseconds",
		Help:      "PID controller error (RTT - baseline) in milliseconds",
	}, []string{"node_id"})

	// Gauge: PID derivative term dError/dt
	controllerDerivativeGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Subsystem: "adaptive",
		Name:      "controller_derivative",
		Help:      "PID controller derivative term (dError/dt in seconds per second)",
	}, []string{"node_id"})

	// Gauge: election timeout yang dihasilkan PID (ms)
	electionTimeoutGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Subsystem: "adaptive",
		Name:      "election_timeout_milliseconds",
		Help:      "Current election timeout in milliseconds",
	}, []string{"node_id"})

	// Gauge: heartbeat interval yang dihasilkan PID (ms)
	heartbeatIntervalGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Subsystem: "adaptive",
		Name:      "heartbeat_interval_milliseconds",
		Help:      "Current heartbeat interval in milliseconds",
	}, []string{"node_id"})

	// Histogram: distribusi RTT (detik) — untuk analisis P95/P99
	rttHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "raft",
		Subsystem: "adaptive",
		Name:      "rtt_duration_seconds",
		Help:      "RTT duration distribution in seconds",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5},
	}, []string{"node_id", "peer_id"})

	// Histogram: distribusi nilai election timeout (detik)
	electionTimeoutHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "raft",
		Subsystem: "adaptive",
		Name:      "election_timeout_duration_seconds",
		Help:      "Distribution of election timeout values",
		Buckets:   []float64{0.1, 0.15, 0.2, 0.3, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0},
	}, []string{"node_id"})

	// Proposal latency histogram (for P95, P99 consensus latency)
	proposalLatencyHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "raft",
		Name:      "proposal_latency_seconds",
		Help:      "Distribution of proposal commit latency in seconds (consensus latency)",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
	}, []string{"node_id", "status"})

	// Leader election duration histogram
	leaderElectionDurationHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "raft",
		Name:      "leader_election_duration_seconds",
		Help:      "Distribution of leader election duration in seconds",
		Buckets:   []float64{0.05, 0.1, 0.15, 0.2, 0.3, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0, 5.0},
	}, []string{"node_id"})

	// Proposals committed total
	proposalsCommittedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "raft",
		Name:      "proposals_committed_total",
		Help:      "Total number of proposals committed",
	}, []string{"node_id"})

	// Proposals failed total
	proposalsFailedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "raft",
		Name:      "proposals_failed_total",
		Help:      "Total number of proposals that failed",
	}, []string{"node_id"})
)

// ============================================================================
// Cluster 2 — System Throughput
// Mengukur jumlah log entries yang berhasil di-commit per satuan waktu.
// Grafana query: rate(raft_throughput_committed_entries_total[1m])
// ============================================================================
var (
	// Counter: total entri yang berhasil committed oleh mayoritas
	committedEntriesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "raft",
		Subsystem: "throughput",
		Name:      "committed_entries_total",
		Help:      "Total log entries committed (reached majority consensus)",
	}, []string{"node_id"})
)

// ============================================================================
// Cluster 3 — Consensus Latency (P95, P99)
// Mengukur waktu dari Leader menerima request hingga entry di-commit mayoritas.
// Grafana query: histogram_quantile(0.99, rate(raft_consensus_replication_latency_seconds_bucket[5m]))
// ============================================================================
var (
	// Histogram: waktu siklus replikasi log (proposal → commit)
	replicationLatencyHist = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "raft",
		Subsystem: "consensus",
		Name:      "replication_latency_seconds",
		Help:      "Time from leader receiving a request until commit by majority",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0},
	}, []string{"node_id"})
)

// ============================================================================
// Cluster 4 — Control Overhead
// Membuktikan bahwa modul PID controller itu ringan (low overhead).
// Grafana query: histogram_quantile(0.99, rate(raft_overhead_tuning_duration_seconds_bucket[5m]))
// Catatan: go_cpu_user_seconds_total dan go_memstats_alloc_bytes sudah
// otomatis tersedia dari promhttp.Handler() untuk overhead keseluruhan proses.
// ============================================================================
var (
	// Histogram: waktu CPU untuk satu siklus PID computation
	tuningDurationHist = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "raft",
		Subsystem: "overhead",
		Name:      "tuning_duration_seconds",
		Help:      "CPU time spent computing one PID controller cycle",
		Buckets:   []float64{0.000001, 0.000005, 0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005},
	}, []string{"node_id"})

	// Histogram: waktu controller update keseluruhan (termasuk I/O observer)
	controllerUpdateHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "raft",
		Subsystem: "overhead",
		Name:      "controller_update_duration_seconds",
		Help:      "Time distribution of full controller update cycles (PID + observer I/O)",
		Buckets:   prometheus.DefBuckets,
	}, []string{"node_id"})
)

// ============================================================================
// Cluster 5 — Role & State Tracking
// Melacak role setiap node (follower/candidate/leader), term, dan frekuensi
// pergantian role. Berguna untuk analisis stabilitas cluster.
// ============================================================================
var (
	// Gauge: Raft term saat ini
	currentTermGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Subsystem: "state",
		Name:      "current_term",
		Help:      "Current Raft term number",
	}, []string{"node_id"})

	// Gauge: state numerik (0=follower, 1=candidate, 2=leader)
	nodeStateGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Subsystem: "state",
		Name:      "node_state",
		Help:      "Current node state (0=follower, 1=candidate, 2=leader)",
	}, []string{"node_id"})

	// Gauge: role berlabel — lebih mudah di-query di Grafana
	nodeRoleGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Subsystem: "state",
		Name:      "node_role",
		Help:      "Node role indicator (1 if active for this role)",
	}, []string{"node_id", "role"})

	// Counter: total pergantian role
	roleChangeCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "raft",
		Subsystem: "state",
		Name:      "role_changes_total",
		Help:      "Total number of role changes",
	}, []string{"node_id", "to_role"})

	// Counter: berapa kali node ini menjadi leader
	leaderElectionCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "raft",
		Subsystem: "state",
		Name:      "leader_elections_total",
		Help:      "Total number of times this node became leader",
	}, []string{"node_id"})
)

// NewPrometheusObserver creates a new Prometheus metrics observer for the given node.
func NewPrometheusObserver(nodeID uint64) *PrometheusObserver {
	return &PrometheusObserver{
		nodeID:    nodeID,
		nodeLabel: strconv.FormatUint(nodeID, 10),
	}
}

// --------------------------------------------------------------------------
// Cluster 1 — Adaptive Parameter Tuning
// --------------------------------------------------------------------------

// RecordRTT records RTT measurement to a specific peer.
func (p *PrometheusObserver) RecordRTT(peerID uint64, rtt time.Duration) {
	peerLabel := strconv.FormatUint(peerID, 10)
	rttGauge.WithLabelValues(p.nodeLabel, peerLabel).Set(float64(rtt.Milliseconds()))
	rttHistogram.WithLabelValues(p.nodeLabel, peerLabel).Observe(rtt.Seconds())
}

// RecordControllerOutput records all PID controller output metrics.
func (p *PrometheusObserver) RecordControllerOutput(s ControllerSnapshot) {
	// RTT & PID signals
	rttAvgGauge.WithLabelValues(p.nodeLabel).Set(float64(s.CurrentRTT.Milliseconds()))
	baselineRTTGauge.WithLabelValues(p.nodeLabel).Set(float64(s.BaselineRTT.Milliseconds()))
	controllerErrorGauge.WithLabelValues(p.nodeLabel).Set(float64(s.Error.Milliseconds()))
	controllerDerivativeGauge.WithLabelValues(p.nodeLabel).Set(s.Derivative)

	// Adaptive timing parameters
	electionTimeoutGauge.WithLabelValues(p.nodeLabel).Set(float64(s.ElectionTimeout.Milliseconds()))
	heartbeatIntervalGauge.WithLabelValues(p.nodeLabel).Set(float64(s.HeartbeatInterval.Milliseconds()))
	electionTimeoutHistogram.WithLabelValues(p.nodeLabel).Observe(s.ElectionTimeout.Seconds())

	// Term & role
	currentTermGauge.WithLabelValues(p.nodeLabel).Set(float64(s.Term))
	p.updateRoleMetrics(s.Role)
}

// --------------------------------------------------------------------------
// Cluster 2 — System Throughput
// --------------------------------------------------------------------------

// RecordCommit records a single committed log entry.
func (p *PrometheusObserver) RecordCommit(term uint64, index uint64) {
	committedEntriesTotal.WithLabelValues(p.nodeLabel).Inc()
}

// --------------------------------------------------------------------------
// Cluster 3 — Consensus Latency
// --------------------------------------------------------------------------

// RecordReplicationLatency records the time from proposal to commit.
func (p *PrometheusObserver) RecordReplicationLatency(d time.Duration) {
	replicationLatencyHist.WithLabelValues(p.nodeLabel).Observe(d.Seconds())
}

// --------------------------------------------------------------------------
// Cluster 4 — Control Overhead
// --------------------------------------------------------------------------

// RecordTuningDuration records the CPU time for one PID computation cycle.
func (p *PrometheusObserver) RecordTuningDuration(d time.Duration) {
	tuningDurationHist.WithLabelValues(p.nodeLabel).Observe(d.Seconds())
}

// --------------------------------------------------------------------------
// Cluster 5 — Role & State Tracking
// --------------------------------------------------------------------------

// RecordRoleChange records when the node's role changes.
func (p *PrometheusObserver) RecordRoleChange(role RaftRole, term uint64) {
	roleChangeCounter.WithLabelValues(p.nodeLabel, string(role)).Inc()
	if role == RoleLeader {
		leaderElectionCounter.WithLabelValues(p.nodeLabel).Inc()
	}
	currentTermGauge.WithLabelValues(p.nodeLabel).Set(float64(term))
	p.updateRoleMetrics(role)
}

// updateRoleMetrics updates the node state gauge and role indicators.
func (p *PrometheusObserver) updateRoleMetrics(role RaftRole) {
	var stateValue float64
	switch role {
	case RoleFollower:
		stateValue = 0
	case RoleCandidate:
		stateValue = 1
	case RoleLeader:
		stateValue = 2
	}
	nodeStateGauge.WithLabelValues(p.nodeLabel).Set(stateValue)

	nodeRoleGauge.WithLabelValues(p.nodeLabel, string(RoleFollower)).Set(0)
	nodeRoleGauge.WithLabelValues(p.nodeLabel, string(RoleCandidate)).Set(0)
	nodeRoleGauge.WithLabelValues(p.nodeLabel, string(RoleLeader)).Set(0)
	nodeRoleGauge.WithLabelValues(p.nodeLabel, string(role)).Set(1)
}

// RecordProposalLatency records the latency of a proposal (consensus latency).
func (p *PrometheusObserver) RecordProposalLatency(latency time.Duration, success bool) {
	status := "success"
	if !success {
		status = "failure"
	}
	proposalLatencyHistogram.WithLabelValues(p.nodeLabel, status).Observe(latency.Seconds())
	if success {
		proposalsCommittedCounter.WithLabelValues(p.nodeLabel).Inc()
	} else {
		proposalsFailedCounter.WithLabelValues(p.nodeLabel).Inc()
	}
}

// RecordElectionDuration records the time it took to complete a leader election.
func (p *PrometheusObserver) RecordElectionDuration(duration time.Duration) {
	leaderElectionDurationHistogram.WithLabelValues(p.nodeLabel).Observe(duration.Seconds())
}
