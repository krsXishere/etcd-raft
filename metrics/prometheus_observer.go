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

// Prometheus metrics for Raft observability
var (
	// RTT metrics - Round-trip time to peers
	rttGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Name:      "rtt_milliseconds",
		Help:      "Round-trip time to peer in milliseconds",
	}, []string{"node_id", "peer_id"})

	rttAvgGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Name:      "rtt_avg_milliseconds",
		Help:      "Average RTT from controller snapshot in milliseconds",
	}, []string{"node_id"})

	// Baseline RTT for comparison
	baselineRTTGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Name:      "baseline_rtt_milliseconds",
		Help:      "Configured baseline RTT in milliseconds",
	}, []string{"node_id"})

	// Controller error (deviation from baseline)
	controllerErrorGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Name:      "controller_error_milliseconds",
		Help:      "PI controller error (RTT - baseline) in milliseconds",
	}, []string{"node_id"})

	// Election timeout - key adaptive parameter
	electionTimeoutGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Name:      "election_timeout_milliseconds",
		Help:      "Current election timeout in milliseconds",
	}, []string{"node_id"})

	// Heartbeat interval
	heartbeatIntervalGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Name:      "heartbeat_interval_milliseconds",
		Help:      "Current heartbeat interval in milliseconds",
	}, []string{"node_id"})

	// Current term
	currentTermGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Name:      "current_term",
		Help:      "Current Raft term number",
	}, []string{"node_id"})

	// Node state: 0=follower, 1=candidate, 2=leader
	nodeStateGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Name:      "node_state",
		Help:      "Current node state (0=follower, 1=candidate, 2=leader)",
	}, []string{"node_id"})

	// Role as labeled metric (for easier querying)
	nodeRoleGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Name:      "node_role",
		Help:      "Node role indicator (1 if active for this role)",
	}, []string{"node_id", "role"})

	// Counters for events
	roleChangeCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "raft",
		Name:      "role_changes_total",
		Help:      "Total number of role changes",
	}, []string{"node_id", "to_role"})

	leaderElectionCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "raft",
		Name:      "leader_elections_total",
		Help:      "Total number of times this node became leader",
	}, []string{"node_id"})

	// Histogram for RTT distribution (useful for P95/P99 analysis)
	rttHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "raft",
		Name:      "rtt_duration_seconds",
		Help:      "RTT duration distribution in seconds",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5},
	}, []string{"node_id", "peer_id"})

	// Controller update histogram
	controllerUpdateHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "raft",
		Name:      "controller_update_duration_seconds",
		Help:      "Time distribution of controller updates",
		Buckets:   prometheus.DefBuckets,
	}, []string{"node_id"})

	// Election timeout histogram for analysis
	electionTimeoutHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "raft",
		Name:      "election_timeout_duration_seconds",
		Help:      "Distribution of election timeout values",
		Buckets:   []float64{0.1, 0.15, 0.2, 0.3, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0},
	}, []string{"node_id"})
)

// NewPrometheusObserver creates a new Prometheus metrics observer for the given node.
func NewPrometheusObserver(nodeID uint64) *PrometheusObserver {
	return &PrometheusObserver{
		nodeID:    nodeID,
		nodeLabel: strconv.FormatUint(nodeID, 10),
	}
}

// RecordRTT records RTT measurement to a specific peer.
func (p *PrometheusObserver) RecordRTT(peerID uint64, rtt time.Duration) {
	peerLabel := strconv.FormatUint(peerID, 10)

	// Record as gauge (current value)
	rttGauge.WithLabelValues(p.nodeLabel, peerLabel).Set(float64(rtt.Milliseconds()))

	// Record in histogram for percentile analysis
	rttHistogram.WithLabelValues(p.nodeLabel, peerLabel).Observe(rtt.Seconds())
}

// RecordControllerOutput records all controller output metrics.
func (p *PrometheusObserver) RecordControllerOutput(s ControllerSnapshot) {
	// RTT metrics
	rttAvgGauge.WithLabelValues(p.nodeLabel).Set(float64(s.CurrentRTT.Milliseconds()))
	baselineRTTGauge.WithLabelValues(p.nodeLabel).Set(float64(s.BaselineRTT.Milliseconds()))
	controllerErrorGauge.WithLabelValues(p.nodeLabel).Set(float64(s.Error.Milliseconds()))

	// Timing parameters
	electionTimeoutGauge.WithLabelValues(p.nodeLabel).Set(float64(s.ElectionTimeout.Milliseconds()))
	heartbeatIntervalGauge.WithLabelValues(p.nodeLabel).Set(float64(s.HeartbeatInterval.Milliseconds()))

	// Term
	currentTermGauge.WithLabelValues(p.nodeLabel).Set(float64(s.Term))

	// Record in histograms
	electionTimeoutHistogram.WithLabelValues(p.nodeLabel).Observe(s.ElectionTimeout.Seconds())

	// Update node state based on role
	p.updateRoleMetrics(s.Role)
}

// RecordRoleChange records when the node's role changes.
func (p *PrometheusObserver) RecordRoleChange(role RaftRole, term uint64) {
	// Increment role change counter
	roleChangeCounter.WithLabelValues(p.nodeLabel, string(role)).Inc()

	// Track leader elections specifically
	if role == RoleLeader {
		leaderElectionCounter.WithLabelValues(p.nodeLabel).Inc()
	}

	// Update current term
	currentTermGauge.WithLabelValues(p.nodeLabel).Set(float64(term))

	// Update role metrics
	p.updateRoleMetrics(role)
}

// updateRoleMetrics updates the node state gauge and role indicators.
func (p *PrometheusObserver) updateRoleMetrics(role RaftRole) {
	// Numeric state for simple queries
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

	// Labeled role indicators (easier for Grafana)
	nodeRoleGauge.WithLabelValues(p.nodeLabel, string(RoleFollower)).Set(0)
	nodeRoleGauge.WithLabelValues(p.nodeLabel, string(RoleCandidate)).Set(0)
	nodeRoleGauge.WithLabelValues(p.nodeLabel, string(RoleLeader)).Set(0)
	nodeRoleGauge.WithLabelValues(p.nodeLabel, string(role)).Set(1)
}
