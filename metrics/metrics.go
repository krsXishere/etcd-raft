// Package metrics provides an observability layer for the adaptive Raft node.
// It is designed as a pluggable interface so that OpenTelemetry or any other
// backend can be swapped in without touching the core logic.
package metrics

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// RaftRole represents the current role of a Raft node.
type RaftRole string

const (
	RoleFollower  RaftRole = "follower"
	RoleCandidate RaftRole = "candidate"
	RoleLeader    RaftRole = "leader"
)

// Observer is the interface any metrics backend must satisfy.
// Implement this with OpenTelemetry, Prometheus, or plain logging.
type Observer interface {
	// RecordRTT is called whenever a new RTT sample is obtained from a peer.
	RecordRTT(peerID uint64, rtt time.Duration)

	// RecordControllerOutput is called after every PID controller update.
	RecordControllerOutput(snapshot ControllerSnapshot)

	// RecordRoleChange is called when the node's Raft role changes.
	RecordRoleChange(role RaftRole, term uint64)
}

// ControllerSnapshot holds all the values the PID controller computed during
// a single update cycle.  Structured so it maps cleanly to OTel attributes.
type ControllerSnapshot struct {
	Timestamp         time.Time
	NodeID            uint64
	Role              RaftRole
	CurrentRTT        time.Duration
	BaselineRTT       time.Duration
	Error             time.Duration
	Derivative        float64 // dError/dt (seconds per second)
	ElectionTimeout   time.Duration
	HeartbeatInterval time.Duration
	Term              uint64
}

// --------------------------------------------------------------------------
// Default implementation: structured log output
// --------------------------------------------------------------------------

// LogObserver is the default Observer that writes to the standard logger.
type LogObserver struct {
	nodeID uint64
	mu     sync.Mutex
}

// NewLogObserver returns an observer that logs to stdout.
func NewLogObserver(nodeID uint64) *LogObserver {
	return &LogObserver{nodeID: nodeID}
}

func (o *LogObserver) RecordRTT(peerID uint64, rtt time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	log.Printf("[node=%d] rtt peer=%d rtt=%s", o.nodeID, peerID, rtt)
}

func (o *LogObserver) RecordControllerOutput(s ControllerSnapshot) {
	o.mu.Lock()
	defer o.mu.Unlock()
	log.Printf("[node=%d] controller role=%s term=%d rtt=%s baseline=%s err=%s dErr=%.6f electionTimeout=%s heartbeat=%s",
		s.NodeID, s.Role, s.Term,
		s.CurrentRTT, s.BaselineRTT, s.Error,
		s.Derivative,
		s.ElectionTimeout, s.HeartbeatInterval,
	)
}

func (o *LogObserver) RecordRoleChange(role RaftRole, term uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	log.Printf("[node=%d] role_change role=%s term=%d", o.nodeID, role, term)
}

// --------------------------------------------------------------------------
// RTT sliding window
// --------------------------------------------------------------------------

// RTTWindow maintains a fixed-size sliding window of RTT samples and exposes
// a moving average.
type RTTWindow struct {
	mu      sync.Mutex
	samples []time.Duration
	pos     int
	full    bool
	size    int
}

// NewRTTWindow creates a window that holds at most `size` samples.
func NewRTTWindow(size int) *RTTWindow {
	if size <= 0 {
		size = 20
	}
	return &RTTWindow{
		samples: make([]time.Duration, size),
		size:    size,
	}
}

// Add records a new RTT sample into the window.
func (w *RTTWindow) Add(d time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.samples[w.pos] = d
	w.pos = (w.pos + 1) % w.size
	if w.pos == 0 {
		w.full = true
	}
}

// Average returns the current moving average. If no samples exist it returns 0.
func (w *RTTWindow) Average() time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := w.size
	if !w.full {
		n = w.pos
	}
	if n == 0 {
		return 0
	}
	var total time.Duration
	for i := 0; i < n; i++ {
		total += w.samples[i]
	}
	return total / time.Duration(n)
}

// String returns a human-readable representation of the window.
func (w *RTTWindow) String() string {
	return fmt.Sprintf("avg=%s", w.Average())
}
