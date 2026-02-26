// Adaptive Raft – 5-node cluster with PID-controlled election timeout.
//
// Usage:
//
//	go run main.go -id 1 -port 9001 -peers "2=127.0.0.1:9002,3=127.0.0.1:9003,4=127.0.0.1:9004,5=127.0.0.1:9005"
//
// Or launch all 5 with the helper script (see README).
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/adaptive-raft/controller"
	"github.com/adaptive-raft/metrics"
	"github.com/adaptive-raft/raft"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	var (
		id          uint64
		port        int
		peersFlag   string
		baselineRTT time.Duration
		tBase       time.Duration
		tMin        time.Duration
		tMax        time.Duration
		kp          float64
		ki          float64
		kd          float64
		ratio       float64
		rttWindow   int
		metricsPort int = 9200 // Port untuk Prometheus metrics endpoint
	)

	flag.Uint64Var(&id, "id", 0, "Unique node ID (1-5)")
	flag.IntVar(&port, "port", 0, "Listen port for this node")
	flag.StringVar(&peersFlag, "peers", "", "Comma-separated peer list: id=host:port,...")
	flag.IntVar(&metricsPort, "metric-port", 9200, "Port for Prometheus metrics endpoint")
	flag.DurationVar(&baselineRTT, "baseline-rtt", 5*time.Millisecond, "Expected baseline RTT")
	flag.DurationVar(&tBase, "t-base", 300*time.Millisecond, "Base election timeout")
	flag.DurationVar(&tMin, "t-min", 150*time.Millisecond, "Minimum election timeout")
	flag.DurationVar(&tMax, "t-max", 3*time.Second, "Maximum election timeout")
	flag.Float64Var(&kp, "kp", 2.0, "Proportional gain")
	flag.Float64Var(&ki, "ki", 0.5, "Integral gain")
	flag.Float64Var(&kd, "kd", 0.1, "Derivative gain")
	flag.Float64Var(&ratio, "ratio", 5.0, "Heartbeat ratio (electionTimeout / R)")
	flag.IntVar(&rttWindow, "rtt-window", 20, "RTT sliding window size")

	flag.Parse()

	if id == 0 || port == 0 || peersFlag == "" {
		fmt.Fprintln(os.Stderr, "Usage: -id <1-5> -port <port> -peers 'id=host:port,...'")
		flag.PrintDefaults()
		os.Exit(1)
	}

	// Parse peer addresses.
	peerAddrs := make(map[uint64]string)
	var peerIDs []uint64
	for _, entry := range strings.Split(peersFlag, ",") {
		parts := strings.SplitN(strings.TrimSpace(entry), "=", 2)
		if len(parts) != 2 {
			log.Fatalf("bad peer entry: %q", entry)
		}
		pid, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			log.Fatalf("bad peer id: %q", parts[0])
		}
		peerAddrs[pid] = parts[1]
		peerIDs = append(peerIDs, pid)
	}

	listenAddr := fmt.Sprintf("0.0.0.0:%d", port)

	ctrlCfg := controller.Config{
		BaselineRTT:    baselineRTT,
		Kp:             kp,
		Ki:             ki,
		Kd:             kd,
		TBase:          tBase,
		TMin:           tMin,
		TMax:           tMax,
		HeartbeatRatio: ratio,
	}

	// Log to console (stdout).
	logObs := metrics.NewLogObserver(id)

	// Log to file: logs/node<id>_<timestamp>.log
	fileObs, err := metrics.NewFileObserver(id, "logs")
	if err != nil {
		log.Fatalf("failed to create file observer: %v", err)
	}
	defer fileObs.Close()
	log.Printf("[main] metrics log file: %s", fileObs.FilePath())

	// Prometheus observer for Grafana/Prometheus monitoring.
	promObs := metrics.NewPrometheusObserver(id)

	// Combine all observers so every metric goes to console + file + Prometheus.
	observer := metrics.NewMultiObserver(logObs, fileObs, promObs)

	nodeCfg := raft.Config{
		ID:            id,
		Peers:         peerIDs,
		ListenAddr:    listenAddr,
		PeerAddrs:     peerAddrs,
		ControllerCfg: ctrlCfg,
		Observer:      observer,
		RTTWindowSize: rttWindow,
	}

	node := raft.NewNode(nodeCfg)
	if err := node.Start(); err != nil {
		log.Fatalf("node start: %v", err)
	}

	// ── HTTP API + Prometheus metrics server ─────────────────────────
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		status := map[string]interface{}{
			"id":        id,
			"role":      node.RoleString(),
			"leader_id": node.LeaderID(),
			"term":      node.CurrentTerm(),
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(status)
	})

	mux.HandleFunc("/propose", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			Command string `json:"command"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		start := time.Now()
		propErr := node.Propose(req.Command)
		latency := time.Since(start)

		resp := map[string]interface{}{
			"latency_ms": float64(latency.Microseconds()) / 1000.0,
		}

		if propErr != nil {
			resp["success"] = false
			resp["error"] = propErr.Error()
			resp["leader_id"] = node.LeaderID()
			observer.RecordProposalLatency(latency, false)
		} else {
			resp["success"] = true
			observer.RecordProposalLatency(latency, true)
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})

	go func() {
		addr := fmt.Sprintf(":%d", metricsPort)
		log.Printf("[main] API + metrics endpoint on %s", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Fatalf("http server: %v", err)
		}
	}()

	log.Printf("[main] node %d running on port %d, peers=%v", id, port, peerIDs)

	// Wait for interrupt.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Printf("[main] shutting down node %d...", id)
	node.Stop()
}
