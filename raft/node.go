// Package raft implements a simplified Raft consensus node with adaptive
// election timeout driven by a PI controller.
//
// Intentionally simplified: single-entry AppendEntries, in-memory log,
// no snapshots, no cluster membership changes.  The focus is on
// demonstrating the adaptive timeout mechanism.
package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/adaptive-raft/controller"
	"github.com/adaptive-raft/metrics"
	"github.com/adaptive-raft/transport"
)

// --------------------------------------------------------------------
// Role
// --------------------------------------------------------------------

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

func roleToMetrics(r Role) metrics.RaftRole {
	switch r {
	case Leader:
		return metrics.RoleLeader
	case Candidate:
		return metrics.RoleCandidate
	default:
		return metrics.RoleFollower
	}
}

// --------------------------------------------------------------------
// Node
// --------------------------------------------------------------------

// Node is one participant in the Raft cluster.
type Node struct {
	mu sync.Mutex

	// Identity
	id    uint64
	peers []uint64 // peer IDs (not including self)

	// Persistent state (simplified – in-memory only)
	currentTerm uint64
	votedFor    uint64 // 0 = none
	log         []transport.LogEntry

	// Volatile state
	commitIndex uint64
	lastApplied uint64
	role        Role

	// Leader-only volatile state
	nextIndex  map[uint64]uint64
	matchIndex map[uint64]uint64

	// Election bookkeeping
	votesReceived map[uint64]bool

	// Adaptive controller + metrics
	ctrl     *controller.PIController
	observer metrics.Observer
	rttWin   map[uint64]*metrics.RTTWindow // per-peer RTT windows

	// Transport
	transport *transport.SyncTransport

	// Timers
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	// Lifecycle
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// Config bundles everything needed to create a Node.
type Config struct {
	ID            uint64
	Peers         []uint64
	ListenAddr    string
	PeerAddrs     map[uint64]string
	ControllerCfg controller.Config
	Observer      metrics.Observer
	RTTWindowSize int
}

// NewNode creates a new Raft node.  Call Start() to begin operation.
func NewNode(cfg Config) *Node {
	ctrl := controller.New(cfg.ControllerCfg)

	rttWin := make(map[uint64]*metrics.RTTWindow)
	for _, p := range cfg.Peers {
		rttWin[p] = metrics.NewRTTWindow(cfg.RTTWindowSize)
	}

	n := &Node{
		id:          cfg.ID,
		peers:       cfg.Peers,
		currentTerm: 0,
		votedFor:    0,
		log:         []transport.LogEntry{{Term: 0, Index: 0}}, // sentinel
		role:        Follower,
		nextIndex:   make(map[uint64]uint64),
		matchIndex:  make(map[uint64]uint64),
		ctrl:        ctrl,
		observer:    cfg.Observer,
		rttWin:      rttWin,
		transport:   transport.NewSync(cfg.ID, cfg.ListenAddr, cfg.PeerAddrs),
		stopCh:      make(chan struct{}),
	}

	return n
}

// Start boots the transport and starts the ticker goroutines.
func (n *Node) Start() error {
	n.transport.RegisterSyncHandler(n.handleMessage)

	if err := n.transport.Start(); err != nil {
		return err
	}

	n.resetElectionTimer()

	n.wg.Add(1)
	go n.tickerLoop()

	log.Printf("[raft] node=%d started as follower", n.id)
	return nil
}

// Stop shuts down the node gracefully.
func (n *Node) Stop() {
	close(n.stopCh)
	n.transport.Stop()
	n.wg.Wait()
	log.Printf("[raft] node=%d stopped", n.id)
}

// --------------------------------------------------------------------
// Timer helpers
// --------------------------------------------------------------------

func (n *Node) electionTimeout() time.Duration {
	base := n.ctrl.GetElectionTimeout()
	// Add jitter: [base, base*1.5)
	jitter := time.Duration(rand.Int63n(int64(base / 2)))
	return base + jitter
}

func (n *Node) resetElectionTimer() {
	timeout := n.electionTimeout()
	if n.electionTimer == nil {
		n.electionTimer = time.NewTimer(timeout)
	} else {
		if !n.electionTimer.Stop() {
			select {
			case <-n.electionTimer.C:
			default:
			}
		}
		n.electionTimer.Reset(timeout)
	}
}

func (n *Node) resetHeartbeatTimer() {
	interval := n.ctrl.GetHeartbeatInterval()
	if n.heartbeatTimer == nil {
		n.heartbeatTimer = time.NewTimer(interval)
	} else {
		if !n.heartbeatTimer.Stop() {
			select {
			case <-n.heartbeatTimer.C:
			default:
			}
		}
		n.heartbeatTimer.Reset(interval)
	}
}

// --------------------------------------------------------------------
// Main event loop
// --------------------------------------------------------------------

func (n *Node) tickerLoop() {
	defer n.wg.Done()

	for {
		select {
		case <-n.stopCh:
			return

		case <-n.electionTimer.C:
			n.mu.Lock()
			if n.role != Leader {
				n.startElection()
			}
			n.mu.Unlock()

		case <-func() <-chan time.Time {
			n.mu.Lock()
			defer n.mu.Unlock()
			if n.heartbeatTimer != nil {
				return n.heartbeatTimer.C
			}
			// Return a channel that never fires.
			return make(chan time.Time)
		}():
			n.mu.Lock()
			if n.role == Leader {
				n.broadcastAppendEntries()
				n.resetHeartbeatTimer()
			}
			n.mu.Unlock()
		}
	}
}

// --------------------------------------------------------------------
// Election
// --------------------------------------------------------------------

func (n *Node) startElection() {
	n.currentTerm++
	n.role = Candidate
	n.votedFor = n.id
	n.votesReceived = map[uint64]bool{n.id: true}

	n.observer.RecordRoleChange(metrics.RoleCandidate, n.currentTerm)
	log.Printf("[raft] node=%d starting election term=%d", n.id, n.currentTerm)

	lastIdx, lastTerm := n.lastLogInfo()

	msg := transport.Message{
		Type:         transport.MsgRequestVote,
		From:         n.id,
		Term:         n.currentTerm,
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
	}

	// Send vote requests (releases lock while doing network I/O)
	term := n.currentTerm
	n.mu.Unlock()
	rtts := n.transport.Broadcast(msg)
	n.mu.Lock()

	// Feed RTT samples into controller.
	n.feedRTTs(rtts)

	// If term changed while we were broadcasting, abort.
	if n.currentTerm != term {
		return
	}

	// Check if we already won (replies processed in handleMessage).
	n.checkElectionWon()

	// Reset election timer regardless.
	n.resetElectionTimer()
}

func (n *Node) checkElectionWon() {
	if n.role != Candidate {
		return
	}
	votes := 0
	for _, v := range n.votesReceived {
		if v {
			votes++
		}
	}
	majority := (len(n.peers)+1)/2 + 1
	if votes >= majority {
		n.becomeLeader()
	}
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.observer.RecordRoleChange(metrics.RoleLeader, n.currentTerm)
	log.Printf("[raft] node=%d became leader term=%d", n.id, n.currentTerm)

	// Reinitialise leader state.
	lastIdx := n.log[len(n.log)-1].Index
	for _, p := range n.peers {
		n.nextIndex[p] = lastIdx + 1
		n.matchIndex[p] = 0
	}

	n.ctrl.Reset()
	n.resetHeartbeatTimer()
	// Immediately send heartbeat.
	n.broadcastAppendEntries()
}

func (n *Node) becomeFollower(term uint64) {
	if n.role != Follower {
		n.observer.RecordRoleChange(metrics.RoleFollower, term)
		log.Printf("[raft] node=%d became follower term=%d", n.id, term)
	}
	n.role = Follower
	n.currentTerm = term
	n.votedFor = 0
	n.resetElectionTimer()
}

// --------------------------------------------------------------------
// AppendEntries (heartbeats + replication)
// --------------------------------------------------------------------

func (n *Node) broadcastAppendEntries() {
	for _, peerID := range n.peers {
		n.sendAppendEntries(peerID)
	}
}

func (n *Node) sendAppendEntries(peerID uint64) {
	prevIdx := n.nextIndex[peerID] - 1
	var prevTerm uint64
	if prevIdx < uint64(len(n.log)) {
		prevTerm = n.log[prevIdx].Term
	}

	var entries []transport.LogEntry
	if n.nextIndex[peerID] < uint64(len(n.log)) {
		entries = n.log[n.nextIndex[peerID]:]
	}

	msg := transport.Message{
		Type:         transport.MsgAppendEntries,
		From:         n.id,
		To:           peerID,
		Term:         n.currentTerm,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}

	term := n.currentTerm
	n.mu.Unlock()
	rtt, err := n.transport.Send(msg)
	n.mu.Lock()

	if err != nil {
		return
	}

	// Record RTT.
	n.recordRTT(peerID, rtt)

	// If our term changed, don't process stale state.
	if n.currentTerm != term || n.role != Leader {
		return
	}
}

// --------------------------------------------------------------------
// Message handler (called by transport on inbound RPC)
// --------------------------------------------------------------------

func (n *Node) handleMessage(msg transport.Message) *transport.Message {
	n.mu.Lock()
	defer n.mu.Unlock()

	// If message term > current term, step down.
	if msg.Term > n.currentTerm {
		n.becomeFollower(msg.Term)
	}

	switch msg.Type {
	case transport.MsgRequestVote:
		return n.handleRequestVote(msg)
	case transport.MsgRequestVoteReply:
		n.handleRequestVoteReply(msg)
		return nil
	case transport.MsgAppendEntries:
		return n.handleAppendEntries(msg)
	case transport.MsgAppendEntriesReply:
		n.handleAppendEntriesReply(msg)
		return nil
	}
	return nil
}

func (n *Node) handleRequestVote(msg transport.Message) *transport.Message {
	reply := &transport.Message{
		Type:        transport.MsgRequestVoteReply,
		From:        n.id,
		To:          msg.From,
		Term:        n.currentTerm,
		VoteGranted: false,
	}

	if msg.Term < n.currentTerm {
		return reply
	}

	lastIdx, lastTerm := n.lastLogInfo()
	logOK := msg.LastLogTerm > lastTerm ||
		(msg.LastLogTerm == lastTerm && msg.LastLogIndex >= lastIdx)

	if (n.votedFor == 0 || n.votedFor == msg.From) && logOK {
		n.votedFor = msg.From
		reply.VoteGranted = true
		n.resetElectionTimer()
		log.Printf("[raft] node=%d voted for %d term=%d", n.id, msg.From, n.currentTerm)
	}

	return reply
}

func (n *Node) handleRequestVoteReply(msg transport.Message) {
	if n.role != Candidate || msg.Term != n.currentTerm {
		return
	}
	if msg.VoteGranted {
		n.votesReceived[msg.From] = true
		n.checkElectionWon()
	}
}

func (n *Node) handleAppendEntries(msg transport.Message) *transport.Message {
	reply := &transport.Message{
		Type:    transport.MsgAppendEntriesReply,
		From:    n.id,
		To:      msg.From,
		Term:    n.currentTerm,
		Success: false,
	}

	if msg.Term < n.currentTerm {
		return reply
	}

	// Valid AppendEntries from current leader — reset election timer.
	n.resetElectionTimer()
	if n.role == Candidate {
		n.becomeFollower(msg.Term)
	}

	// Check log consistency.
	if msg.PrevLogIndex > 0 {
		if msg.PrevLogIndex >= uint64(len(n.log)) {
			return reply
		}
		if n.log[msg.PrevLogIndex].Term != msg.PrevLogTerm {
			// Truncate conflicting suffix.
			n.log = n.log[:msg.PrevLogIndex]
			return reply
		}
	}

	// Append new entries.
	for i, entry := range msg.Entries {
		idx := msg.PrevLogIndex + 1 + uint64(i)
		if idx < uint64(len(n.log)) {
			if n.log[idx].Term != entry.Term {
				n.log = n.log[:idx]
				n.log = append(n.log, entry)
			}
		} else {
			n.log = append(n.log, entry)
		}
	}

	// Update commit index.
	if msg.LeaderCommit > n.commitIndex {
		lastNew := msg.PrevLogIndex + uint64(len(msg.Entries))
		if msg.LeaderCommit < lastNew {
			n.commitIndex = msg.LeaderCommit
		} else {
			n.commitIndex = lastNew
		}
	}

	reply.Success = true
	reply.MatchIndex = n.log[len(n.log)-1].Index
	return reply
}

func (n *Node) handleAppendEntriesReply(msg transport.Message) {
	if n.role != Leader || msg.Term != n.currentTerm {
		return
	}

	if msg.Success {
		if msg.MatchIndex > n.matchIndex[msg.From] {
			n.matchIndex[msg.From] = msg.MatchIndex
			n.nextIndex[msg.From] = msg.MatchIndex + 1
		}
		n.advanceCommitIndex()
	} else {
		// Decrement nextIndex and retry.
		if n.nextIndex[msg.From] > 1 {
			n.nextIndex[msg.From]--
		}
	}
}

// advanceCommitIndex checks if there's an N > commitIndex such that a majority
// of matchIndex[i] >= N and log[N].term == currentTerm.
func (n *Node) advanceCommitIndex() {
	for idx := n.commitIndex + 1; idx < uint64(len(n.log)); idx++ {
		if n.log[idx].Term != n.currentTerm {
			continue
		}
		matches := 1 // count self
		for _, p := range n.peers {
			if n.matchIndex[p] >= idx {
				matches++
			}
		}
		majority := (len(n.peers)+1)/2 + 1
		if matches >= majority {
			n.commitIndex = idx
		}
	}
}

// --------------------------------------------------------------------
// RTT / Controller integration
// --------------------------------------------------------------------

func (n *Node) recordRTT(peerID uint64, rtt time.Duration) {
	if w, ok := n.rttWin[peerID]; ok {
		w.Add(rtt)
	}
	n.observer.RecordRTT(peerID, rtt)

	avgRTT := n.aggregateRTT()
	n.ctrl.Update(avgRTT)

	n.observer.RecordControllerOutput(metrics.ControllerSnapshot{
		Timestamp:         time.Now(),
		NodeID:            n.id,
		Role:              roleToMetrics(n.role),
		CurrentRTT:        avgRTT,
		BaselineRTT:       n.ctrl.Config().BaselineRTT,
		Error:             avgRTT - n.ctrl.Config().BaselineRTT,
		ElectionTimeout:   n.ctrl.GetElectionTimeout(),
		HeartbeatInterval: n.ctrl.GetHeartbeatInterval(),
		Term:              n.currentTerm,
	})
}

func (n *Node) feedRTTs(rtts map[uint64]time.Duration) {
	for peerID, rtt := range rtts {
		n.recordRTT(peerID, rtt)
	}
}

func (n *Node) aggregateRTT() time.Duration {
	var total time.Duration
	count := 0
	for _, w := range n.rttWin {
		avg := w.Average()
		if avg > 0 {
			total += avg
			count++
		}
	}
	if count == 0 {
		return n.ctrl.Config().BaselineRTT
	}
	return total / time.Duration(count)
}

// --------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------

func (n *Node) lastLogInfo() (index uint64, term uint64) {
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}
