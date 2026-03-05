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
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/adaptive-raft/controller"
	"github.com/adaptive-raft/metrics"
	"github.com/adaptive-raft/raft"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ── Traffic Control (tc netem) runtime management ────────────────────────────

// TCConfig holds the current netem parameters applied to the container.
type TCConfig struct {
	Delay       string `json:"delay"`
	Jitter      string `json:"jitter"`
	Loss        string `json:"loss"`
	Correlation string `json:"correlation"`
	Duplicate   string `json:"duplicate"`
	Reorder     string `json:"reorder"`
	Interface   string `json:"interface"`
}

// TCManager allows querying / changing netem rules at runtime.
type TCManager struct {
	mu      sync.Mutex
	current TCConfig
	nodeID  uint64
}

func NewTCManager(nodeID uint64) *TCManager {
	return &TCManager{
		nodeID: nodeID,
		current: TCConfig{
			Delay:       os.Getenv("TC_DELAY"),
			Jitter:      os.Getenv("TC_JITTER"),
			Loss:        os.Getenv("TC_LOSS"),
			Correlation: os.Getenv("TC_CORRELATION"),
			Duplicate:   os.Getenv("TC_DUPLICATE"),
			Reorder:     os.Getenv("TC_REORDER"),
			Interface:   "eth0",
		},
	}
}

// Apply sets new netem rules using `tc qdisc replace`.
func (tm *TCManager) Apply(cfg TCConfig) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if cfg.Interface == "" {
		cfg.Interface = "eth0"
	}
	if cfg.Delay == "" {
		cfg.Delay = "0ms"
	}
	if cfg.Jitter == "" {
		cfg.Jitter = "0ms"
	}
	if cfg.Loss == "" {
		cfg.Loss = "0%"
	}
	if cfg.Correlation == "" {
		cfg.Correlation = "0%"
	}
	if cfg.Duplicate == "" {
		cfg.Duplicate = "0%"
	}
	if cfg.Reorder == "" {
		cfg.Reorder = "0%"
	}

	// tc qdisc replace works whether a qdisc exists or not.
	args := []string{
		"qdisc", "replace", "dev", cfg.Interface, "root", "netem",
		"delay", cfg.Delay, cfg.Jitter, cfg.Correlation,
		"loss", cfg.Loss,
		"duplicate", cfg.Duplicate,
		"reorder", cfg.Reorder,
	}

	log.Printf("[TC] node=%d applying: tc %s", tm.nodeID, strings.Join(args, " "))
	cmd := exec.Command("tc", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("[TC] node=%d ERROR: %v – %s", tm.nodeID, err, string(out))
		return fmt.Errorf("tc command failed: %v – %s", err, string(out))
	}

	tm.current = cfg
	log.Printf("[TC] node=%d applied: delay=%s jitter=%s loss=%s corr=%s dup=%s reorder=%s",
		tm.nodeID, cfg.Delay, cfg.Jitter, cfg.Loss, cfg.Correlation, cfg.Duplicate, cfg.Reorder)
	return nil
}

// Remove deletes all netem rules from the interface.
func (tm *TCManager) Remove() error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	iface := tm.current.Interface
	if iface == "" {
		iface = "eth0"
	}

	cmd := exec.Command("tc", "qdisc", "del", "dev", iface, "root")
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("[TC] node=%d remove WARNING: %v – %s", tm.nodeID, err, string(out))
		return fmt.Errorf("tc delete failed: %v – %s", err, string(out))
	}

	tm.current = TCConfig{Interface: iface}
	log.Printf("[TC] node=%d all netem rules removed on %s", tm.nodeID, iface)
	return nil
}

// Current returns a copy of the active TC config.
func (tm *TCManager) Current() TCConfig {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	return tm.current
}

// ShowRaw returns the raw `tc qdisc show` output.
func (tm *TCManager) ShowRaw() (string, error) {
	iface := tm.current.Interface
	if iface == "" {
		iface = "eth0"
	}
	cmd := exec.Command("tc", "qdisc", "show", "dev", iface)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("tc show failed: %v – %s", err, string(out))
	}
	return strings.TrimSpace(string(out)), nil
}

func main() {
	var (
		id          uint64
		port        int
		peersFlag   string
		adaptive    bool
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
	flag.BoolVar(&adaptive, "adaptive", false, "Enable adaptive PID-controlled election timeout")
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

	// ── Mode selection: adaptive (PID) vs static (fixed timeout) ────
	if !adaptive {
		// Static mode: zero out PID gains so controller always outputs T_base.
		log.Printf("[main] STATIC MODE: election timeout fixed at %v (PID disabled)", tBase)
		kp = 0
		ki = 0
		kd = 0
	} else {
		log.Printf("[main] ADAPTIVE MODE: PID enabled (Kp=%.2f Ki=%.2f Kd=%.2f baseline=%v)",
			kp, ki, kd, baselineRTT)
	}

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

	// ── Traffic Control runtime endpoint ─────────────────────────────
	tcMgr := NewTCManager(id)

	// ── FOPDT Step Response Test runner ──────────────────────────────
	stepRunner := NewStepTestRunner(id, node, tcMgr)

	// Start background RTT sampler so raft_fopdt_rtt_sample_milliseconds
	// is always up-to-date (every 200ms), not only during step tests.
	stepRunner.StartRTTSampler(200*time.Millisecond, node.StopChan())

	// Also initialize the TC input metric with the current TC delay.
	if curDelay := tcMgr.Current().Delay; curDelay != "" {
		stepRunner.UpdateTCInput(curDelay)
	}

	mux.HandleFunc("/tc", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.Method {
		case http.MethodGet:
			// Show current TC config + raw tc output.
			raw, _ := tcMgr.ShowRaw()
			resp := map[string]interface{}{
				"node_id": id,
				"config":  tcMgr.Current(),
				"raw":     raw,
			}
			_ = json.NewEncoder(w).Encode(resp)

		case http.MethodPost:
			// Apply new TC rules.
			var cfg TCConfig
			if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
				http.Error(w, `{"error":"bad request: `+err.Error()+`"}`, http.StatusBadRequest)
				return
			}
			if err := tcMgr.Apply(cfg); err != nil {
				resp := map[string]interface{}{
					"success": false,
					"error":   err.Error(),
				}
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(resp)
				return
			}
			// Update FOPDT input metric so Prometheus tracks TC changes in real-time.
			applied := tcMgr.Current()
			stepRunner.UpdateTCInput(applied.Delay)

			resp := map[string]interface{}{
				"success": true,
				"node_id": id,
				"applied": applied,
			}
			_ = json.NewEncoder(w).Encode(resp)

		case http.MethodDelete:
			// Remove all TC rules.
			if err := tcMgr.Remove(); err != nil {
				resp := map[string]interface{}{
					"success": false,
					"error":   err.Error(),
				}
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(resp)
				return
			}
			resp := map[string]interface{}{
				"success": true,
				"node_id": id,
				"message": "all netem rules removed",
			}
			_ = json.NewEncoder(w).Encode(resp)

		default:
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		}
	})

	// ── FOPDT Step Response Test endpoint ────────────────────────────
	mux.HandleFunc("/step-test", stepRunner.HandleStepTest)
	mux.HandleFunc("/step-test/samples", stepRunner.HandleStepTestSamples)

	// ── Network Partition simulation endpoint ────────────────────────
	partMgr := NewPartitionManager(id, peerAddrs, node)
	mux.HandleFunc("/partition", partMgr.HandlePartition)

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		mode := "static"
		if adaptive {
			mode = "adaptive"
		}
		status := map[string]interface{}{
			"id":        id,
			"mode":      mode,
			"role":      node.RoleString(),
			"leader_id": node.LeaderID(),
			"term":      node.CurrentTerm(),
			"partition": partMgr.Status(),
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
