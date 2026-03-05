// Network Partition Simulator
//
// Uses iptables to simulate network partitions (split-brain scenarios)
// within a Docker container.  Each node can isolate itself from specific
// peers by dropping all traffic to/from those peers' IP addresses.
//
// Endpoints:
//
//	POST   /partition          — create a partition (isolate peers)
//	GET    /partition          — show current partition state
//	DELETE /partition          — heal (remove all iptables rules)
//
// Example:
//
//	POST /partition
//	{
//	  "isolate": [4, 5]       // block traffic to/from nodes 4 and 5
//	}
//
//	DELETE /partition          // restore full connectivity
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ============================================================================
// Cluster 8 — Network Partition Metrics (3 metrik)
//
// Sumber data  : PartitionManager
// Fungsi       : Melacak simulasi network partition untuk Scenario 3
// Grafana      : raft_partition_*
// ============================================================================
var (
	// Gauge: apakah node ini sedang dalam kondisi partisi (1=yes, 0=no)
	partitionActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Subsystem: "partition",
		Name:      "active",
		Help:      "Whether this node is currently partitioned (1=yes, 0=no)",
	}, []string{"node_id"})

	// Gauge: jumlah peer yang diisolasi saat ini
	partitionIsolatedPeers = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Subsystem: "partition",
		Name:      "isolated_peers",
		Help:      "Number of peers currently isolated by iptables",
	}, []string{"node_id"})

	// Counter: total jumlah event partisi yang telah terjadi
	partitionEventsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "raft",
		Subsystem: "partition",
		Name:      "events_total",
		Help:      "Total number of partition events (isolate + heal)",
	}, []string{"node_id", "action"}) // action = "isolate" | "heal"
)

// PartitionManager handles iptables-based network partition simulation.
type PartitionManager struct {
	mu        sync.Mutex
	nodeID    uint64
	nodeLabel string
	peerAddrs map[uint64]string // peerID → "host:port"

	isolated    map[uint64]bool // peer IDs currently isolated
	partitioned bool
	startTime   time.Time // when partition was created
}

// NewPartitionManager creates a new partition manager.
func NewPartitionManager(nodeID uint64, peerAddrs map[uint64]string) *PartitionManager {
	label := fmt.Sprintf("%d", nodeID)

	// Pre-initialize gauges.
	partitionActive.WithLabelValues(label).Set(0)
	partitionIsolatedPeers.WithLabelValues(label).Set(0)
	partitionEventsTotal.WithLabelValues(label, "isolate")
	partitionEventsTotal.WithLabelValues(label, "heal")

	return &PartitionManager{
		nodeID:    nodeID,
		nodeLabel: label,
		peerAddrs: peerAddrs,
		isolated:  make(map[uint64]bool),
	}
}

// resolveHost extracts the hostname from "host:port" and resolves it to an IP.
func resolveHost(addr string) (string, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr // assume it's just a hostname
	}

	// Try DNS resolution (works inside Docker compose network).
	ips, err := net.LookupHost(host)
	if err != nil {
		return "", fmt.Errorf("cannot resolve %q: %v", host, err)
	}
	if len(ips) == 0 {
		return "", fmt.Errorf("no IPs found for %q", host)
	}
	return ips[0], nil
}

// Isolate blocks traffic to/from the specified peer nodes using iptables.
func (pm *PartitionManager) Isolate(peerIDs []uint64) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for _, pid := range peerIDs {
		if pm.isolated[pid] {
			continue // already isolated
		}

		addr, ok := pm.peerAddrs[pid]
		if !ok {
			return fmt.Errorf("unknown peer ID %d", pid)
		}

		ip, err := resolveHost(addr)
		if err != nil {
			return fmt.Errorf("peer %d: %v", pid, err)
		}

		// Block outgoing traffic to peer.
		if err := iptablesRule("-A", "OUTPUT", ip); err != nil {
			return fmt.Errorf("block OUTPUT to peer %d (%s): %v", pid, ip, err)
		}

		// Block incoming traffic from peer.
		if err := iptablesRule("-A", "INPUT", ip); err != nil {
			// Rollback the OUTPUT rule.
			_ = iptablesRule("-D", "OUTPUT", ip)
			return fmt.Errorf("block INPUT from peer %d (%s): %v", pid, ip, err)
		}

		pm.isolated[pid] = true
		log.Printf("[PARTITION] node=%d isolated peer %d (ip=%s)", pm.nodeID, pid, ip)
	}

	pm.partitioned = len(pm.isolated) > 0
	if pm.partitioned {
		pm.startTime = time.Now()
	}

	// Update Prometheus.
	if pm.partitioned {
		partitionActive.WithLabelValues(pm.nodeLabel).Set(1)
	} else {
		partitionActive.WithLabelValues(pm.nodeLabel).Set(0)
	}
	partitionIsolatedPeers.WithLabelValues(pm.nodeLabel).Set(float64(len(pm.isolated)))
	partitionEventsTotal.WithLabelValues(pm.nodeLabel, "isolate").Inc()

	return nil
}

// Heal removes all iptables rules and restores full connectivity.
func (pm *PartitionManager) Heal() error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	var errors []string
	for pid := range pm.isolated {
		addr, ok := pm.peerAddrs[pid]
		if !ok {
			continue
		}

		ip, err := resolveHost(addr)
		if err != nil {
			errors = append(errors, fmt.Sprintf("peer %d resolve: %v", pid, err))
			continue
		}

		// Remove OUTPUT rule.
		if err := iptablesRule("-D", "OUTPUT", ip); err != nil {
			errors = append(errors, fmt.Sprintf("unblock OUTPUT peer %d: %v", pid, err))
		}

		// Remove INPUT rule.
		if err := iptablesRule("-D", "INPUT", ip); err != nil {
			errors = append(errors, fmt.Sprintf("unblock INPUT peer %d: %v", pid, err))
		}

		log.Printf("[PARTITION] node=%d healed peer %d (ip=%s)", pm.nodeID, pid, ip)
	}

	// Also flush the entire chain as a safety net.
	_ = exec.Command("iptables", "-F", "INPUT").Run()
	_ = exec.Command("iptables", "-F", "OUTPUT").Run()

	duration := time.Duration(0)
	if pm.partitioned {
		duration = time.Since(pm.startTime)
	}

	pm.isolated = make(map[uint64]bool)
	pm.partitioned = false

	// Update Prometheus.
	partitionActive.WithLabelValues(pm.nodeLabel).Set(0)
	partitionIsolatedPeers.WithLabelValues(pm.nodeLabel).Set(0)
	partitionEventsTotal.WithLabelValues(pm.nodeLabel, "heal").Inc()

	if len(errors) > 0 {
		log.Printf("[PARTITION] node=%d heal completed with errors (duration=%s): %v",
			pm.nodeID, duration, errors)
		return fmt.Errorf("partial heal: %s", strings.Join(errors, "; "))
	}

	log.Printf("[PARTITION] node=%d fully healed (partition lasted %s)", pm.nodeID, duration)
	return nil
}

// Status returns the current partition state.
func (pm *PartitionManager) Status() map[string]interface{} {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	isolated := make([]uint64, 0, len(pm.isolated))
	for pid := range pm.isolated {
		isolated = append(isolated, pid)
	}

	status := map[string]interface{}{
		"node_id":     pm.nodeID,
		"partitioned": pm.partitioned,
		"isolated":    isolated,
	}

	if pm.partitioned {
		status["duration_s"] = time.Since(pm.startTime).Seconds()
	}

	return status
}

// iptablesRule adds/removes an iptables rule to DROP traffic to/from an IP.
func iptablesRule(action, chain, ip string) error {
	args := []string{action, chain, "-s", ip, "-j", "DROP"}
	if chain == "OUTPUT" {
		args = []string{action, chain, "-d", ip, "-j", "DROP"}
	}

	log.Printf("[PARTITION] iptables %s", strings.Join(args, " "))
	cmd := exec.Command("iptables", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("iptables %s: %v – %s", strings.Join(args, " "), err, string(out))
	}
	return nil
}

// ============================================================================
// HTTP handler
// ============================================================================

// HandlePartition handles GET / POST / DELETE on /partition.
func (pm *PartitionManager) HandlePartition(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodGet:
		_ = json.NewEncoder(w).Encode(pm.Status())

	case http.MethodPost:
		var req struct {
			Isolate []uint64 `json:"isolate"` // peer IDs to isolate
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"bad request: `+err.Error()+`"}`, http.StatusBadRequest)
			return
		}
		if len(req.Isolate) == 0 {
			http.Error(w, `{"error":"'isolate' array is required (list of peer IDs)"}`, http.StatusBadRequest)
			return
		}

		if err := pm.Isolate(req.Isolate); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"error":   err.Error(),
			})
			return
		}

		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"success":  true,
			"node_id":  pm.nodeID,
			"isolated": req.Isolate,
			"message":  fmt.Sprintf("traffic blocked to/from peers %v", req.Isolate),
		})

	case http.MethodDelete:
		if err := pm.Heal(); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"error":   err.Error(),
			})
			return
		}

		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"node_id": pm.nodeID,
			"message": "all partitions healed, full connectivity restored",
		})

	default:
		http.Error(w, `{"error":"method not allowed, use GET / POST / DELETE"}`, http.StatusMethodNotAllowed)
	}
}
