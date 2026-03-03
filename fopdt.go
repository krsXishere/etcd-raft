// FOPDT Step Response Test Runner
//
// Automates the First-Order Plus Dead-Time identification protocol:
//  1. Baseline phase  – apply pre_delay TC, record RTT for N seconds
//  2. Step phase      – apply post_delay TC, record RTT for M seconds
//  3. Compute K, θ, τ from the recorded step response
//
// Usage (static mode first, then adaptive):
//
//	POST /step-test
//	{
//	  "pre_delay": "2ms",
//	  "post_delay": "12ms",
//	  "baseline_secs": 30,
//	  "step_secs": 90,
//	  "sample_interval_ms": 100
//	}
//
//	GET  /step-test          → status + FOPDT result
//	GET  /step-test/samples  → all recorded RTT samples (JSON array)
//	DELETE /step-test        → reset / abort
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/adaptive-raft/raft"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ============================================================================
// Cluster 7 — FOPDT Step Response Identification (10 metrik)
//
// Sumber data  : StepTestRunner
// Fungsi       : Mengukur parameter FOPDT (K, θ, τ) dari step response test.
//
//	Test dilakukan di static mode (baseline) terlebih dahulu,
//	kemudian di adaptive mode untuk perbandingan.
//
// Grafana      : raft_fopdt_*
// ============================================================================
var (
	// Gauge: apakah step test sedang berjalan (1=yes, 0=no)
	fopdtStepActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Subsystem: "fopdt",
		Name:      "step_active",
		Help:      "Whether a step test is currently running (1=yes, 0=no)",
	}, []string{"node_id"})

	// Gauge: fase step test (0=idle, 1=baseline, 2=step, 3=computing, 4=complete, -1=error)
	fopdtStepPhase = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Subsystem: "fopdt",
		Name:      "step_phase",
		Help:      "Current step test phase (0=idle, 1=baseline, 2=step, 3=computing, 4=complete, -1=error)",
	}, []string{"node_id"})

	// Gauge: TC delay input saat ini (ms) — sinyal input ke plant
	fopdtStepInputMs = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Subsystem: "fopdt",
		Name:      "step_input_milliseconds",
		Help:      "Current TC delay input being applied during step test (ms)",
	}, []string{"node_id"})

	// Gauge: besarnya step (post_delay - pre_delay) dalam ms
	fopdtStepMagnitudeMs = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Subsystem: "fopdt",
		Name:      "step_magnitude_milliseconds",
		Help:      "Step magnitude (post_delay - pre_delay) in ms",
	}, []string{"node_id"})

	// Gauge: RTT sample terbaru selama step test (ms) — output plant
	fopdtRTTSampleMs = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Subsystem: "fopdt",
		Name:      "rtt_sample_milliseconds",
		Help:      "Latest RTT sample during step test (ms)",
	}, []string{"node_id"})

	// Gauge: FOPDT process gain K = ΔRTT / ΔTC_delay
	fopdtGainK = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Subsystem: "fopdt",
		Name:      "gain_k",
		Help:      "Computed FOPDT process gain K = delta_RTT / step_magnitude",
	}, []string{"node_id"})

	// Gauge: FOPDT dead time θ (ms)
	fopdtDeadTimeMs = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Subsystem: "fopdt",
		Name:      "dead_time_milliseconds",
		Help:      "Computed FOPDT dead time theta (ms)",
	}, []string{"node_id"})

	// Gauge: FOPDT time constant τ (ms)
	fopdtTimeConstantMs = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Subsystem: "fopdt",
		Name:      "time_constant_milliseconds",
		Help:      "Computed FOPDT time constant tau (ms)",
	}, []string{"node_id"})

	// Gauge: RTT sebelum step (rata-rata baseline) dalam ms
	fopdtRTTInitialMs = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Subsystem: "fopdt",
		Name:      "rtt_initial_milliseconds",
		Help:      "RTT before step (baseline average) in ms",
	}, []string{"node_id"})

	// Gauge: RTT sesudah step (rata-rata settled) dalam ms
	fopdtRTTFinalMs = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "raft",
		Subsystem: "fopdt",
		Name:      "rtt_final_milliseconds",
		Help:      "RTT after step (settled average) in ms",
	}, []string{"node_id"})
)

// ============================================================================
// Data structures
// ============================================================================

// StepTestConfig holds the parameters for a FOPDT step response test.
type StepTestConfig struct {
	PreDelay         string `json:"pre_delay"`          // TC delay before step, e.g. "2ms"
	PostDelay        string `json:"post_delay"`         // TC delay after step, e.g. "12ms"
	BaselineSecs     int    `json:"baseline_secs"`      // Seconds to record baseline (default 30)
	StepSecs         int    `json:"step_secs"`          // Seconds to record after step (default 90)
	SampleIntervalMs int    `json:"sample_interval_ms"` // Sampling interval in ms (default 100)
}

// StepSample is one RTT observation during the step test.
type StepSample struct {
	Timestamp time.Time `json:"timestamp"`
	Phase     string    `json:"phase"`      // "baseline" or "step"
	ElapsedMs float64   `json:"elapsed_ms"` // ms since test start
	RTTMs     float64   `json:"rtt_ms"`     // measured RTT in ms
	InputMs   float64   `json:"input_ms"`   // TC delay input in ms
}

// FOPDTResult holds the computed FOPDT model parameters.
type FOPDTResult struct {
	K               float64 `json:"k"`                 // Process gain (ΔRTT / Δinput)
	DeadTimeMs      float64 `json:"dead_time_ms"`      // θ in ms
	TimeConstantMs  float64 `json:"time_constant_ms"`  // τ in ms
	RTTInitialMs    float64 `json:"rtt_initial_ms"`    // Baseline average RTT
	RTTFinalMs      float64 `json:"rtt_final_ms"`      // Settled average RTT
	StepMagnitudeMs float64 `json:"step_magnitude_ms"` // Step size (post - pre)
}

// ============================================================================
// StepTestRunner
// ============================================================================

// StepTestRunner manages FOPDT step response experiments.
type StepTestRunner struct {
	mu        sync.Mutex
	nodeID    uint64
	nodeLabel string
	node      *raft.Node
	tcMgr     *TCManager

	running   bool
	phase     string // "idle","baseline","step","computing","complete","error"
	config    StepTestConfig
	samples   []StepSample
	result    *FOPDTResult
	startTime time.Time
	errMsg    string
	stopCh    chan struct{} // signal to abort a running test
}

// NewStepTestRunner creates a new step test runner.
func NewStepTestRunner(nodeID uint64, node *raft.Node, tcMgr *TCManager) *StepTestRunner {
	label := strconv.FormatUint(nodeID, 10)

	// Pre-initialize all FOPDT gauges so they appear in /metrics immediately
	// (Prometheus GaugeVec only emits a time series after WithLabelValues is called).
	fopdtStepActive.WithLabelValues(label).Set(0)
	fopdtStepPhase.WithLabelValues(label).Set(0)
	fopdtStepInputMs.WithLabelValues(label).Set(0)
	fopdtStepMagnitudeMs.WithLabelValues(label).Set(0)
	fopdtRTTSampleMs.WithLabelValues(label).Set(0)
	fopdtGainK.WithLabelValues(label).Set(0)
	fopdtDeadTimeMs.WithLabelValues(label).Set(0)
	fopdtTimeConstantMs.WithLabelValues(label).Set(0)
	fopdtRTTInitialMs.WithLabelValues(label).Set(0)
	fopdtRTTFinalMs.WithLabelValues(label).Set(0)

	return &StepTestRunner{
		nodeID:    nodeID,
		nodeLabel: label,
		node:      node,
		tcMgr:     tcMgr,
		phase:     "idle",
	}
}

// delayToMs parses a Go duration string and returns the value in milliseconds.
func delayToMs(s string) (float64, error) {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, err
	}
	return float64(d.Nanoseconds()) / 1e6, nil
}

// ============================================================================
// Real-time tracking (independent of step test)
// ============================================================================

// UpdateTCInput updates the raft_fopdt_step_input_milliseconds gauge
// whenever TC delay is applied via /tc endpoint.  This allows the
// metric to reflect the current TC delay even without a formal step test.
func (r *StepTestRunner) UpdateTCInput(delayStr string) {
	ms, err := delayToMs(delayStr)
	if err != nil {
		log.Printf("[FOPDT] node=%d cannot parse TC delay %q: %v", r.nodeID, delayStr, err)
		return
	}
	fopdtStepInputMs.WithLabelValues(r.nodeLabel).Set(ms)
}

// StartRTTSampler launches a background goroutine that continuously
// pushes the node's average RTT to raft_fopdt_rtt_sample_milliseconds.
// This runs forever (until stopCh is closed) so the metric is always
// up-to-date, not only during a step test.
func (r *StepTestRunner) StartRTTSampler(interval time.Duration, stopCh <-chan struct{}) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				rtt := r.node.GetAverageRTT()
				rttMs := float64(rtt.Nanoseconds()) / 1e6
				fopdtRTTSampleMs.WithLabelValues(r.nodeLabel).Set(rttMs)
			}
		}
	}()
}

// ============================================================================
// Run / Abort
// ============================================================================

// Run starts the step test in a background goroutine.
func (r *StepTestRunner) Run(cfg StepTestConfig) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("step test already running (phase=%s)", r.phase)
	}

	// Validate & apply defaults.
	if cfg.PreDelay == "" || cfg.PostDelay == "" {
		r.mu.Unlock()
		return fmt.Errorf("pre_delay and post_delay are required")
	}
	if cfg.BaselineSecs <= 0 {
		cfg.BaselineSecs = 30
	}
	if cfg.StepSecs <= 0 {
		cfg.StepSecs = 90
	}
	if cfg.SampleIntervalMs <= 0 {
		cfg.SampleIntervalMs = 100
	}

	preMs, err := delayToMs(cfg.PreDelay)
	if err != nil {
		r.mu.Unlock()
		return fmt.Errorf("invalid pre_delay %q: %v", cfg.PreDelay, err)
	}
	postMs, err := delayToMs(cfg.PostDelay)
	if err != nil {
		r.mu.Unlock()
		return fmt.Errorf("invalid post_delay %q: %v", cfg.PostDelay, err)
	}

	magnitude := postMs - preMs

	r.running = true
	r.phase = "baseline"
	r.config = cfg
	r.samples = nil
	r.result = nil
	r.errMsg = ""
	r.startTime = time.Now()
	r.stopCh = make(chan struct{})

	// Prometheus: mark active.
	fopdtStepActive.WithLabelValues(r.nodeLabel).Set(1)
	fopdtStepPhase.WithLabelValues(r.nodeLabel).Set(1) // baseline
	fopdtStepMagnitudeMs.WithLabelValues(r.nodeLabel).Set(magnitude)
	fopdtStepInputMs.WithLabelValues(r.nodeLabel).Set(preMs)

	r.mu.Unlock()

	go r.execute(cfg, preMs, postMs, magnitude)
	return nil
}

// Abort cancels a running step test.
func (r *StepTestRunner) Abort() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.running && r.stopCh != nil {
		close(r.stopCh)
	}
}

// Reset clears results and returns to idle state (only when not running).
func (r *StepTestRunner) Reset() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.running {
		return fmt.Errorf("cannot reset while test is running")
	}
	r.phase = "idle"
	r.samples = nil
	r.result = nil
	r.errMsg = ""
	fopdtStepPhase.WithLabelValues(r.nodeLabel).Set(0)
	fopdtStepActive.WithLabelValues(r.nodeLabel).Set(0)
	return nil
}

// ============================================================================
// Core test loop
// ============================================================================

func (r *StepTestRunner) execute(cfg StepTestConfig, preMs, postMs, magnitude float64) {
	defer func() {
		r.mu.Lock()
		r.running = false
		fopdtStepActive.WithLabelValues(r.nodeLabel).Set(0)
		r.mu.Unlock()
	}()

	interval := time.Duration(cfg.SampleIntervalMs) * time.Millisecond

	// ── Phase 1: Baseline ───────────────────────────────────────────
	log.Printf("[FOPDT] node=%d ▶ baseline phase (%ds, pre_delay=%s)",
		r.nodeID, cfg.BaselineSecs, cfg.PreDelay)

	if err := r.tcMgr.Apply(TCConfig{Delay: cfg.PreDelay}); err != nil {
		r.fail(fmt.Sprintf("apply pre_delay: %v", err))
		return
	}

	baselineEnd := time.Now().Add(time.Duration(cfg.BaselineSecs) * time.Second)
	if aborted := r.sampleUntil(baselineEnd, interval, "baseline", preMs); aborted {
		return
	}

	// ── Phase 2: Step ───────────────────────────────────────────────
	log.Printf("[FOPDT] node=%d ▶ step phase: %s → %s (Δ=%.1fms, recording %ds)",
		r.nodeID, cfg.PreDelay, cfg.PostDelay, magnitude, cfg.StepSecs)

	r.mu.Lock()
	r.phase = "step"
	r.mu.Unlock()
	fopdtStepPhase.WithLabelValues(r.nodeLabel).Set(2)
	fopdtStepInputMs.WithLabelValues(r.nodeLabel).Set(postMs)

	stepTime := time.Now()
	if err := r.tcMgr.Apply(TCConfig{Delay: cfg.PostDelay}); err != nil {
		r.fail(fmt.Sprintf("apply post_delay: %v", err))
		return
	}

	stepEnd := stepTime.Add(time.Duration(cfg.StepSecs) * time.Second)
	if aborted := r.sampleUntil(stepEnd, interval, "step", postMs); aborted {
		return
	}

	// ── Phase 3: Compute ────────────────────────────────────────────
	r.mu.Lock()
	r.phase = "computing"
	r.mu.Unlock()
	fopdtStepPhase.WithLabelValues(r.nodeLabel).Set(3)

	log.Printf("[FOPDT] node=%d computing FOPDT from %d samples…", r.nodeID, len(r.samples))

	result := r.computeFOPDT(stepTime, magnitude)

	r.mu.Lock()
	r.result = result
	r.phase = "complete"
	r.mu.Unlock()
	fopdtStepPhase.WithLabelValues(r.nodeLabel).Set(4)

	// Push FOPDT results to Prometheus.
	fopdtGainK.WithLabelValues(r.nodeLabel).Set(result.K)
	fopdtDeadTimeMs.WithLabelValues(r.nodeLabel).Set(result.DeadTimeMs)
	fopdtTimeConstantMs.WithLabelValues(r.nodeLabel).Set(result.TimeConstantMs)
	fopdtRTTInitialMs.WithLabelValues(r.nodeLabel).Set(result.RTTInitialMs)
	fopdtRTTFinalMs.WithLabelValues(r.nodeLabel).Set(result.RTTFinalMs)

	log.Printf("[FOPDT] node=%d ✓ COMPLETE  K=%.4f  θ=%.1fms  τ=%.1fms  (RTT %.1f→%.1fms, step=%.1fms)",
		r.nodeID, result.K, result.DeadTimeMs, result.TimeConstantMs,
		result.RTTInitialMs, result.RTTFinalMs, result.StepMagnitudeMs)
}

// sampleUntil records RTT samples until `deadline`. Returns true if aborted.
func (r *StepTestRunner) sampleUntil(deadline time.Time, interval time.Duration, phase string, inputMs float64) bool {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			r.fail("aborted by user")
			return true
		case <-ticker.C:
			if time.Now().After(deadline) {
				return false
			}
			rtt := r.node.GetAverageRTT()
			rttMs := float64(rtt.Nanoseconds()) / 1e6
			s := StepSample{
				Timestamp: time.Now(),
				Phase:     phase,
				ElapsedMs: float64(time.Since(r.startTime).Nanoseconds()) / 1e6,
				RTTMs:     rttMs,
				InputMs:   inputMs,
			}
			r.mu.Lock()
			r.samples = append(r.samples, s)
			r.mu.Unlock()

			fopdtRTTSampleMs.WithLabelValues(r.nodeLabel).Set(rttMs)
		}
	}
}

func (r *StepTestRunner) fail(msg string) {
	log.Printf("[FOPDT] node=%d ERROR: %s", r.nodeID, msg)
	r.mu.Lock()
	r.errMsg = msg
	r.phase = "error"
	r.mu.Unlock()
	fopdtStepPhase.WithLabelValues(r.nodeLabel).Set(-1)
}

// ============================================================================
// FOPDT computation
// ============================================================================

func (r *StepTestRunner) computeFOPDT(stepTime time.Time, magnitude float64) *FOPDTResult {
	r.mu.Lock()
	samples := make([]StepSample, len(r.samples))
	copy(samples, r.samples)
	r.mu.Unlock()

	// Split phases.
	var baseline, step []StepSample
	for _, s := range samples {
		switch s.Phase {
		case "baseline":
			baseline = append(baseline, s)
		case "step":
			step = append(step, s)
		}
	}

	// RTT_initial: baseline average.
	rttInitial := avgRTT(baseline)

	// RTT_final: average of last 20 % of step samples (settled region).
	tailCount := len(step) / 5
	if tailCount < 5 {
		tailCount = 5
	}
	if tailCount > len(step) {
		tailCount = len(step)
	}
	rttFinal := avgRTT(step[len(step)-tailCount:])

	deltaRTT := rttFinal - rttInitial

	// K = ΔRTT / step_magnitude (process gain).
	var K float64
	if magnitude != 0 {
		K = deltaRTT / magnitude
	}

	// θ (dead time): first sample where RTT exceeds baseline + 10 % of ΔRTT.
	absDelta := math.Abs(deltaRTT)
	threshold10 := rttInitial + 0.10*absDelta
	var deadTimeMs float64
	for _, s := range step {
		if s.RTTMs > threshold10 {
			deadTimeMs = float64(s.Timestamp.Sub(stepTime).Nanoseconds()) / 1e6
			break
		}
	}

	// τ (time constant): time to reach 63.2 % of final change, minus θ.
	threshold63 := rttInitial + 0.632*absDelta
	var tauPlusTheta float64
	for _, s := range step {
		if s.RTTMs > threshold63 {
			tauPlusTheta = float64(s.Timestamp.Sub(stepTime).Nanoseconds()) / 1e6
			break
		}
	}
	timeConstantMs := tauPlusTheta - deadTimeMs
	if timeConstantMs < 0 {
		timeConstantMs = 0
	}

	return &FOPDTResult{
		K:               K,
		DeadTimeMs:      deadTimeMs,
		TimeConstantMs:  timeConstantMs,
		RTTInitialMs:    rttInitial,
		RTTFinalMs:      rttFinal,
		StepMagnitudeMs: magnitude,
	}
}

func avgRTT(samples []StepSample) float64 {
	if len(samples) == 0 {
		return 0
	}
	sum := 0.0
	for _, s := range samples {
		sum += s.RTTMs
	}
	return sum / float64(len(samples))
}

// ============================================================================
// Status / Samples accessors
// ============================================================================

// Status returns the current step test state as a JSON-serialisable map.
func (r *StepTestRunner) Status() map[string]interface{} {
	r.mu.Lock()
	defer r.mu.Unlock()

	status := map[string]interface{}{
		"node_id":      r.nodeID,
		"phase":        r.phase,
		"running":      r.running,
		"sample_count": len(r.samples),
	}

	if r.running {
		status["config"] = r.config
		status["elapsed_secs"] = time.Since(r.startTime).Seconds()
	}
	if r.result != nil {
		status["result"] = r.result
	}
	if r.errMsg != "" {
		status["error"] = r.errMsg
	}

	// Warn if this node is not the leader (RTT not measured on followers).
	if r.node.RoleString() != "leader" {
		status["warning"] = "This node is NOT the leader. RTT samples may be 0. " +
			"Run the step test on the leader node for accurate FOPDT results, " +
			"or correlate with the leader's raft_adaptive_rtt_milliseconds in Prometheus."
	}

	return status
}

// Samples returns a copy of all recorded samples.
func (r *StepTestRunner) Samples() []StepSample {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]StepSample, len(r.samples))
	copy(out, r.samples)
	return out
}

// ============================================================================
// HTTP handlers
// ============================================================================

// HandleStepTest handles GET / POST / DELETE on /step-test.
func (r *StepTestRunner) HandleStepTest(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch req.Method {
	case http.MethodGet:
		_ = json.NewEncoder(w).Encode(r.Status())

	case http.MethodPost:
		var cfg StepTestConfig
		if err := json.NewDecoder(req.Body).Decode(&cfg); err != nil {
			http.Error(w, `{"error":"bad request: `+err.Error()+`"}`, http.StatusBadRequest)
			return
		}
		if err := r.Run(cfg); err != nil {
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"error":   err.Error(),
			})
			return
		}
		totalSecs := cfg.BaselineSecs + cfg.StepSecs
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"node_id": r.nodeID,
			"message": fmt.Sprintf("step test started (total ≈ %ds: baseline %ds + step %ds)",
				totalSecs, cfg.BaselineSecs, cfg.StepSecs),
			"config": cfg,
		})

	case http.MethodDelete:
		r.Abort()
		if err := r.Reset(); err != nil {
			// Still running, just abort was sent. Wait briefly.
			time.Sleep(200 * time.Millisecond)
			_ = r.Reset()
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"node_id": r.nodeID,
			"message": "step test aborted / reset",
		})

	default:
		http.Error(w, `{"error":"method not allowed, use GET / POST / DELETE"}`, http.StatusMethodNotAllowed)
	}
}

// HandleStepTestSamples serves GET /step-test/samples — exports all recorded data points.
func (r *StepTestRunner) HandleStepTestSamples(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(r.Samples())
}
