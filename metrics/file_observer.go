package metrics

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// FileObserver writes structured metrics to a timestamped log file.
// A new file is created each time the server starts.
type FileObserver struct {
	nodeID uint64
	mu     sync.Mutex
	file   *os.File
}

// NewFileObserver creates a FileObserver that writes to a file inside `dir`.
// The file name includes the node ID and the server start timestamp, e.g.
//
//	logs/node1_2026-02-22_14-30-45.log
//
// The directory is created automatically if it does not exist.
func NewFileObserver(nodeID uint64, dir string) (*FileObserver, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("metrics: create log dir: %w", err)
	}

	ts := time.Now().Format("2006-01-02_15-04-05")
	name := fmt.Sprintf("node%d_%s.log", nodeID, ts)
	path := filepath.Join(dir, name)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("metrics: open log file: %w", err)
	}

	// Write a header line so the file is self-describing.
	fmt.Fprintf(f, "# Adaptive Raft metrics – node %d – started %s\n", nodeID, time.Now().Format(time.RFC3339))

	return &FileObserver{nodeID: nodeID, file: f}, nil
}

// Close flushes and closes the underlying file.
func (o *FileObserver) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.file.Close()
}

// FilePath returns the path to the log file being written.
func (o *FileObserver) FilePath() string {
	return o.file.Name()
}

func (o *FileObserver) RecordRTT(peerID uint64, rtt time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	fmt.Fprintf(o.file, "%s [node=%d] rtt peer=%d rtt=%s\n",
		time.Now().Format("2006/01/02 15:04:05.000000"), o.nodeID, peerID, rtt)
}

func (o *FileObserver) RecordControllerOutput(s ControllerSnapshot) {
	o.mu.Lock()
	defer o.mu.Unlock()
	fmt.Fprintf(o.file, "%s [node=%d] controller role=%s term=%d rtt=%s baseline=%s err=%s dErr=%.6f electionTimeout=%s heartbeat=%s\n",
		time.Now().Format("2006/01/02 15:04:05.000000"),
		s.NodeID, s.Role, s.Term,
		s.CurrentRTT, s.BaselineRTT, s.Error,
		s.Derivative,
		s.ElectionTimeout, s.HeartbeatInterval,
	)
}

func (o *FileObserver) RecordCommit(term uint64, index uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	fmt.Fprintf(o.file, "%s [node=%d] commit term=%d index=%d\n",
		time.Now().Format("2006/01/02 15:04:05.000000"), o.nodeID, term, index)
}

func (o *FileObserver) RecordReplicationLatency(d time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	fmt.Fprintf(o.file, "%s [node=%d] replication_latency duration=%s\n",
		time.Now().Format("2006/01/02 15:04:05.000000"), o.nodeID, d)
}

func (o *FileObserver) RecordTuningDuration(d time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	fmt.Fprintf(o.file, "%s [node=%d] tuning_duration duration=%s\n",
		time.Now().Format("2006/01/02 15:04:05.000000"), o.nodeID, d)
}

func (o *FileObserver) RecordRoleChange(role RaftRole, term uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	fmt.Fprintf(o.file, "%s [node=%d] role_change role=%s term=%d\n",
		time.Now().Format("2006/01/02 15:04:05.000000"), o.nodeID, role, term)
}

func (o *FileObserver) RecordProposalLatency(latency time.Duration, success bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	fmt.Fprintf(o.file, "%s [node=%d] proposal latency=%s success=%v\n",
		time.Now().Format("2006/01/02 15:04:05.000000"), o.nodeID, latency, success)
}

func (o *FileObserver) RecordElectionDuration(duration time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	fmt.Fprintf(o.file, "%s [node=%d] election_duration duration=%s\n",
		time.Now().Format("2006/01/02 15:04:05.000000"), o.nodeID, duration)
}

// --------------------------------------------------------------------------
// MultiObserver – fan-out to multiple backends
// --------------------------------------------------------------------------

// MultiObserver dispatches every metric call to all wrapped observers.
type MultiObserver struct {
	observers []Observer
}

// NewMultiObserver returns an observer that writes to all given backends.
func NewMultiObserver(observers ...Observer) *MultiObserver {
	return &MultiObserver{observers: observers}
}

func (m *MultiObserver) RecordRTT(peerID uint64, rtt time.Duration) {
	for _, o := range m.observers {
		o.RecordRTT(peerID, rtt)
	}
}

func (m *MultiObserver) RecordControllerOutput(s ControllerSnapshot) {
	for _, o := range m.observers {
		o.RecordControllerOutput(s)
	}
}

func (m *MultiObserver) RecordCommit(term uint64, index uint64) {
	for _, o := range m.observers {
		o.RecordCommit(term, index)
	}
}

func (m *MultiObserver) RecordReplicationLatency(d time.Duration) {
	for _, o := range m.observers {
		o.RecordReplicationLatency(d)
	}
}

func (m *MultiObserver) RecordTuningDuration(d time.Duration) {
	for _, o := range m.observers {
		o.RecordTuningDuration(d)
	}
}

func (m *MultiObserver) RecordRoleChange(role RaftRole, term uint64) {
	for _, o := range m.observers {
		o.RecordRoleChange(role, term)
	}
}

func (m *MultiObserver) RecordProposalLatency(latency time.Duration, success bool) {
	for _, o := range m.observers {
		o.RecordProposalLatency(latency, success)
	}
}

func (m *MultiObserver) RecordElectionDuration(duration time.Duration) {
	for _, o := range m.observers {
		o.RecordElectionDuration(duration)
	}
}
