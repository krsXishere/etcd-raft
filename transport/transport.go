// Package transport provides the RPC layer for inter-node communication.
// It uses net/http + encoding/json for simplicity. The wire format is
// intentionally kept trivial so the focus stays on the Raft + controller logic.
package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

// -------------------------------------------------------------------
// Message types exchanged between nodes
// -------------------------------------------------------------------

// MsgType distinguishes the different RPC kinds.
type MsgType int

const (
	MsgRequestVote MsgType = iota + 1
	MsgRequestVoteReply
	MsgAppendEntries
	MsgAppendEntriesReply
)

// LogEntry is a single replicated log entry.
type LogEntry struct {
	Term    uint64 `json:"term"`
	Index   uint64 `json:"index"`
	Command string `json:"command,omitempty"`
}

// Message is the envelope for every RPC between nodes.
type Message struct {
	Type MsgType `json:"type"`
	From uint64  `json:"from"`
	To   uint64  `json:"to"`
	Term uint64  `json:"term"`

	// RequestVote fields
	LastLogIndex uint64 `json:"lastLogIndex,omitempty"`
	LastLogTerm  uint64 `json:"lastLogTerm,omitempty"`

	// AppendEntries fields
	PrevLogIndex uint64     `json:"prevLogIndex,omitempty"`
	PrevLogTerm  uint64     `json:"prevLogTerm,omitempty"`
	Entries      []LogEntry `json:"entries,omitempty"`
	LeaderCommit uint64     `json:"leaderCommit,omitempty"`

	// Reply fields
	VoteGranted bool   `json:"voteGranted,omitempty"`
	Success     bool   `json:"success,omitempty"`
	MatchIndex  uint64 `json:"matchIndex,omitempty"`
}

// -------------------------------------------------------------------
// Transport
// -------------------------------------------------------------------

// MessageHandler is the callback that the Raft layer registers to receive
// inbound messages.
type MessageHandler func(msg Message)

// Transport manages outbound HTTP calls and hosts the inbound HTTP server.
type Transport struct {
	nodeID  uint64
	addr    string            // this node's listen address
	peers   map[uint64]string // peerID → address
	handler MessageHandler

	server *http.Server
	client *http.Client

	mu      sync.RWMutex
	running bool
}

// New creates a Transport.  `peers` maps peerID → "host:port".
func New(nodeID uint64, listenAddr string, peers map[uint64]string) *Transport {
	return &Transport{
		nodeID: nodeID,
		addr:   listenAddr,
		peers:  peers,
		client: &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 4,
				IdleConnTimeout:     60 * time.Second,
			},
		},
	}
}

// RegisterHandler sets the callback for inbound messages.
func (t *Transport) RegisterHandler(h MessageHandler) {
	t.handler = h
}

// Start begins listening for inbound RPCs.
func (t *Transport) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/raft", t.handleRaft)

	t.server = &http.Server{
		Addr:    t.addr,
		Handler: mux,
	}

	ln, err := net.Listen("tcp", t.addr)
	if err != nil {
		return fmt.Errorf("transport: listen %s: %w", t.addr, err)
	}

	t.mu.Lock()
	t.running = true
	t.mu.Unlock()

	go func() {
		if err := t.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("[transport] serve error: %v", err)
		}
	}()

	log.Printf("[transport] node=%d listening on %s", t.nodeID, t.addr)
	return nil
}

// Stop gracefully shuts down the HTTP server.
func (t *Transport) Stop() {
	t.mu.Lock()
	t.running = false
	t.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = t.server.Shutdown(ctx)
}

// Send transmits a message to a specific peer.  It returns the RTT of the
// HTTP round-trip so the caller can feed it into the PI controller.
func (t *Transport) Send(msg Message) (rtt time.Duration, err error) {
	addr, ok := t.peers[msg.To]
	if !ok {
		return 0, fmt.Errorf("transport: unknown peer %d", msg.To)
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return 0, err
	}

	url := fmt.Sprintf("http://%s/raft", addr)
	start := time.Now()
	resp, err := t.client.Post(url, "application/json", bytes.NewReader(body))
	rtt = time.Since(start)
	if err != nil {
		return rtt, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return rtt, fmt.Errorf("transport: peer %d returned %d", msg.To, resp.StatusCode)
	}

	// If the peer returned a reply body, decode it and dispatch.
	if resp.ContentLength > 0 || resp.Header.Get("Content-Type") == "application/json" {
		var reply Message
		data, _ := io.ReadAll(resp.Body)
		if len(data) > 0 {
			if jsonErr := json.Unmarshal(data, &reply); jsonErr == nil {
				if t.handler != nil {
					t.handler(reply)
				}
			}
		}
	}

	return rtt, nil
}

// Broadcast sends a message to all peers (used by leaders / candidates).
// RTT samples are returned per-peer.
func (t *Transport) Broadcast(msg Message) map[uint64]time.Duration {
	rtts := make(map[uint64]time.Duration)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for id := range t.peers {
		wg.Add(1)
		go func(peerID uint64) {
			defer wg.Done()
			m := msg
			m.To = peerID
			rtt, err := t.Send(m)
			if err != nil {
				log.Printf("[transport] send to %d: %v", peerID, err)
				return
			}
			mu.Lock()
			rtts[peerID] = rtt
			mu.Unlock()
		}(id)
	}
	wg.Wait()
	return rtts
}

// handleRaft processes an inbound HTTP request.
func (t *Transport) handleRaft(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var msg Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	// Synchronously handle and produce a reply on the same HTTP connection,
	// so the sender can measure the true RTT.
	if t.handler == nil {
		w.WriteHeader(http.StatusOK)
		return
	}

	replyCh := make(chan Message, 1)
	t.handler(msg) // dispatch to raft; raft will call SendReply

	// We use a lightweight trick: the handler is expected to put a reply
	// into the message's channel.  But for simplicity we handle the reply
	// inline inside the raft package and return it via a ResponseWriter.
	//
	// For this HTTP-based transport we use the handleReply pattern below.
	// The raft node's handleMessage produces a reply synchronously.
	select {
	case reply := <-replyCh:
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(reply)
	default:
		w.WriteHeader(http.StatusOK)
	}
}

// -------------------------------------------------------------------
// Synchronous RPC variant (better RTT measurement)
// -------------------------------------------------------------------

// SyncTransport wraps Transport and provides a synchronous RPC interface
// where the handler directly returns a reply.
type SyncTransport struct {
	*Transport
	syncHandler func(msg Message) *Message
}

// NewSync creates a SyncTransport.
func NewSync(nodeID uint64, listenAddr string, peers map[uint64]string) *SyncTransport {
	st := &SyncTransport{
		Transport: New(nodeID, listenAddr, peers),
	}
	return st
}

// RegisterSyncHandler sets a handler that returns a reply directly.
func (st *SyncTransport) RegisterSyncHandler(h func(msg Message) *Message) {
	st.syncHandler = h
	// Also wire up the HTTP handler.
}

// Start overrides Transport.Start to use the sync handler in HTTP.
func (st *SyncTransport) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/raft", st.handleRaftSync)

	st.server = &http.Server{
		Addr:    st.addr,
		Handler: mux,
	}

	ln, err := net.Listen("tcp", st.addr)
	if err != nil {
		return fmt.Errorf("transport: listen %s: %w", st.addr, err)
	}

	st.mu.Lock()
	st.running = true
	st.mu.Unlock()

	go func() {
		if err := st.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("[transport] serve error: %v", err)
		}
	}()

	log.Printf("[transport] node=%d listening on %s", st.nodeID, st.addr)
	return nil
}

func (st *SyncTransport) handleRaftSync(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var msg Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if st.syncHandler == nil {
		w.WriteHeader(http.StatusOK)
		return
	}
	reply := st.syncHandler(msg)
	if reply != nil {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(reply)
	} else {
		w.WriteHeader(http.StatusOK)
	}
}
