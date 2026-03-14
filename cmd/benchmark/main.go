// Benchmark tool for the Adaptive Raft cluster.
//
// Supports two load patterns:
//   - steady:  constant request rate
//   - bursty:  alternating burst / calm phases
//
// Reports:
//   - Leader Election Time (observed via /status polling)
//   - System Throughput (ops/sec)
//   - Consensus Latency (P50, P95, P99)
//   - Error rate
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ─────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────

type config struct {
	Targets       []string
	Pattern       string        // "steady" or "bursty"
	Duration      time.Duration // total benchmark duration
	Rate          int           // ops/sec (steady rate, or calm-phase rate)
	BurstRate     int           // ops/sec during burst phase
	BurstDuration time.Duration // length of one burst phase
	CalmDuration  time.Duration // length of one calm phase
	WarmUp        time.Duration // initial wait for cluster to elect leader
}

// ─────────────────────────────────────────────────────────────────────
// Metrics collection
// ─────────────────────────────────────────────────────────────────────

type result struct {
	latency   time.Duration
	success   bool
	timestamp time.Time
}

type stats struct {
	mu            sync.Mutex
	latencies     []time.Duration
	results       []result // track all results with timestamps
	successes     int64
	failures      int64
	lastSnapshotS int64 // track successes at last snapshot
	lastSnapshotF int64 // track failures at last snapshot
}

func (s *stats) record(r result) {
	if r.success {
		atomic.AddInt64(&s.successes, 1)
	} else {
		atomic.AddInt64(&s.failures, 1)
	}
	s.mu.Lock()
	s.results = append(s.results, r)
	if r.success {
		s.latencies = append(s.latencies, r.latency)
	}
	s.mu.Unlock()
}

func (s *stats) getResultsInWindow(start, end time.Time) []result {
	s.mu.Lock()
	defer s.mu.Unlock()
	var filtered []result
	for _, r := range s.results {
		if !r.timestamp.Before(start) && r.timestamp.Before(end) {
			filtered = append(filtered, r)
		}
	}
	return filtered
}

// ─────────────────────────────────────────────────────────────────────
// Per-second timeline metrics
// ─────────────────────────────────────────────────────────────────────

type timelineEntry struct {
	Timestamp              int64   `json:"timestamp_unix"`
	ElapsedSeconds         float64 `json:"elapsed_s"`
	SuccessesPerSecond     int64   `json:"successes_ps"`
	FailuresPerSecond      int64   `json:"failures_ps"`
	ThroughputOpsPerSecond float64 `json:"throughput_ops_s"`
	LatencyP50Ms           float64 `json:"latency_p50_ms"`
	LatencyP95Ms           float64 `json:"latency_p95_ms"`
	LatencyP99Ms           float64 `json:"latency_p99_ms"`
	LatencyMeanMs          float64 `json:"latency_mean_ms"`
	LatencyMinMs           float64 `json:"latency_min_ms"`
	LatencyMaxMs           float64 `json:"latency_max_ms"`
	CumulativeSuccesses    int64   `json:"cumulative_successes"`
	CumulativeFailures     int64   `json:"cumulative_failures"`
}

type timeline struct {
	mu      sync.Mutex
	entries []timelineEntry
}

func (t *timeline) record(entry timelineEntry) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.entries = append(t.entries, entry)
}

func (t *timeline) getEntries() []timelineEntry {
	t.mu.Lock()
	defer t.mu.Unlock()
	result := make([]timelineEntry, len(t.entries))
	copy(result, t.entries)
	return result
}

// ─────────────────────────────────────────────────────────────────────
// HTTP helpers
// ─────────────────────────────────────────────────────────────────────

var httpClient = &http.Client{Timeout: 15 * time.Second}

type proposeResponse struct {
	Success   bool    `json:"success"`
	Error     string  `json:"error"`
	LeaderID  uint64  `json:"leader_id"`
	LatencyMs float64 `json:"latency_ms"`
}

type statusResponse struct {
	ID       uint64 `json:"id"`
	Role     string `json:"role"`
	LeaderID uint64 `json:"leader_id"`
	Term     uint64 `json:"term"`
}

func propose(target, command string) (time.Duration, error) {
	body, _ := json.Marshal(map[string]string{"command": command})
	start := time.Now()
	resp, err := httpClient.Post(
		fmt.Sprintf("http://%s/propose", target),
		"application/json",
		bytes.NewReader(body),
	)
	latency := time.Since(start)
	if err != nil {
		return latency, err
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)
	var pr proposeResponse
	if err := json.Unmarshal(data, &pr); err != nil {
		return latency, fmt.Errorf("decode: %w", err)
	}
	if !pr.Success {
		return latency, fmt.Errorf("propose: %s", pr.Error)
	}
	return latency, nil
}

func getStatus(target string) (*statusResponse, error) {
	resp, err := httpClient.Get(fmt.Sprintf("http://%s/status", target))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var sr statusResponse
	if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
		return nil, err
	}
	return &sr, nil
}

// getLeaderElectionCount queries Prometheus /metrics for raft_state_leader_elections_total
func getLeaderElectionCount(target string) int64 {
	resp, err := httpClient.Get(fmt.Sprintf("http://%s/metrics", target))
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)

	// Parse Prometheus text format for raft_state_leader_elections_total
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "raft_state_leader_elections_total{") {
			// Format: raft_state_leader_elections_total{node_id="1"} 3
			parts := strings.Split(line, " ")
			if len(parts) >= 2 {
				var count int64
				fmt.Sscanf(parts[len(parts)-1], "%d", &count)
				return count
			}
		}
	}
	return 0
}

// getTotalLeaderElections sums leader_elections_total across all nodes
func getTotalLeaderElections(targets []string) int64 {
	var total int64
	for _, t := range targets {
		total += getLeaderElectionCount(t)
	}
	return total
}

// getPartitionRecoveryMs reads raft_partition_recovery_milliseconds for a target.
func getPartitionRecoveryMs(target string) float64 {
	resp, err := httpClient.Get(fmt.Sprintf("http://%s/metrics", target))
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)

	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "raft_partition_recovery_milliseconds{") {
			parts := strings.Split(line, " ")
			if len(parts) >= 2 {
				var val float64
				fmt.Sscanf(parts[len(parts)-1], "%f", &val)
				return val
			}
		}
	}
	return 0
}

// getPartitionDurationMs reads raft_partition_duration_milliseconds for a target.
func getPartitionDurationMs(target string) float64 {
	resp, err := httpClient.Get(fmt.Sprintf("http://%s/metrics", target))
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)

	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "raft_partition_duration_milliseconds{") {
			parts := strings.Split(line, " ")
			if len(parts) >= 2 {
				var val float64
				fmt.Sscanf(parts[len(parts)-1], "%f", &val)
				return val
			}
		}
	}
	return 0
}

// collectPartitionMetrics gathers partition recovery and duration from all nodes.
func collectPartitionMetrics(targets []string) (maxRecoveryMs, maxDurationMs float64) {
	for _, t := range targets {
		if r := getPartitionRecoveryMs(t); r > maxRecoveryMs {
			maxRecoveryMs = r
		}
		if d := getPartitionDurationMs(t); d > maxDurationMs {
			maxDurationMs = d
		}
	}
	return
}

// discoverLeader polls all targets until one reports role == "leader".
func discoverLeader(targets []string) string {
	for _, t := range targets {
		st, err := getStatus(t)
		if err == nil && st.Role == "leader" {
			return t
		}
	}
	return ""
}

// ─────────────────────────────────────────────────────────────────────
// Wait for cluster to be healthy
// ─────────────────────────────────────────────────────────────────────

func waitForCluster(targets []string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ready := 0
		for _, t := range targets {
			resp, err := httpClient.Get(fmt.Sprintf("http://%s/health", t))
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode == 200 {
					ready++
				}
			}
		}
		if ready == len(targets) {
			log.Printf("[bench] all %d nodes healthy", ready)
			return
		}
		log.Printf("[bench] %d/%d nodes healthy, waiting...", ready, len(targets))
		time.Sleep(2 * time.Second)
	}
	log.Println("[bench] WARNING: timeout waiting for cluster, proceeding anyway")
}

// ─────────────────────────────────────────────────────────────────────
// Leader election observation
// ─────────────────────────────────────────────────────────────────────

type electionObserver struct {
	mu            sync.Mutex
	targets       []string
	lastLeader    string
	lastLeaderID  uint64
	lastTerm      uint64
	electionStart time.Time
	durations     []time.Duration
	stopCh        chan struct{}
}

func newElectionObserver(targets []string) *electionObserver {
	return &electionObserver{
		targets: targets,
		stopCh:  make(chan struct{}),
	}
}

func (eo *electionObserver) start() {
	go func() {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-eo.stopCh:
				return
			case <-ticker.C:
				eo.poll()
			}
		}
	}()
}

func (eo *electionObserver) poll() {
	for _, t := range eo.targets {
		st, err := getStatus(t)
		if err != nil {
			continue
		}
		eo.mu.Lock()
		if st.Role == "leader" && (st.ID != eo.lastLeaderID || st.Term != eo.lastTerm) {
			if !eo.electionStart.IsZero() {
				dur := time.Since(eo.electionStart)
				eo.durations = append(eo.durations, dur)
				log.Printf("[bench] leader election observed: node=%d term=%d duration=%s",
					st.ID, st.Term, dur)
			}
			eo.lastLeaderID = st.ID
			eo.lastTerm = st.Term
			eo.lastLeader = t
			eo.electionStart = time.Now()
		}
		eo.mu.Unlock()
	}
}

func (eo *electionObserver) stop() []time.Duration {
	close(eo.stopCh)
	eo.mu.Lock()
	defer eo.mu.Unlock()
	return eo.durations
}

// ─────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────

func main() {
	cfg := parseConfig()

	log.SetFlags(log.Ltime | log.Lmicroseconds)
	log.Printf("[bench] Adaptive Raft Benchmark")
	log.Printf("[bench] pattern=%s duration=%s rate=%d burst_rate=%d targets=%v",
		cfg.Pattern, cfg.Duration, cfg.Rate, cfg.BurstRate, cfg.Targets)

	// Wait for cluster
	waitForCluster(cfg.Targets, 120*time.Second)

	// Wait for leader election to complete
	log.Printf("[bench] warming up (%s)...", cfg.WarmUp)
	time.Sleep(cfg.WarmUp)

	leader := discoverLeader(cfg.Targets)
	if leader == "" {
		log.Println("[bench] WARNING: no leader found, will try all nodes")
	} else {
		log.Printf("[bench] leader found: %s", leader)
	}

	// Snapshot leader election count BEFORE benchmark
	electionsBefore := getTotalLeaderElections(cfg.Targets)
	log.Printf("[bench] leader elections before: %d", electionsBefore)

	// Start election observer
	eo := newElectionObserver(cfg.Targets)
	eo.start()

	// Run benchmark with timeline collection
	st := &stats{}
	tl := &timeline{}
	start := time.Now()
	runBenchmarkWithTimeline(cfg, st, tl, start)
	totalDur := time.Since(start)

	// Stop election observer
	electionDurations := eo.stop()

	// Snapshot leader election count AFTER benchmark
	electionsAfter := getTotalLeaderElections(cfg.Targets)
	log.Printf("[bench] leader elections after: %d (delta=%d)", electionsAfter, electionsAfter-electionsBefore)

	// Collect partition recovery metrics
	recoveryMs, partDurationMs := collectPartitionMetrics(cfg.Targets)
	if recoveryMs > 0 {
		log.Printf("[bench] partition recovery_ms=%.1f partition_duration_ms=%.1f", recoveryMs, partDurationMs)
	}

	// Report
	report(st, totalDur, electionDurations, electionsBefore, electionsAfter, recoveryMs, partDurationMs, tl)
}

func parseConfig() config {
	var (
		targets  string
		pattern  string
		duration time.Duration
		rate     int
		burst    int
		burstDur time.Duration
		calmDur  time.Duration
		warmup   time.Duration
	)

	flag.StringVar(&targets, "targets", "", "Comma-separated node addresses (host:metricsPort)")
	flag.StringVar(&pattern, "pattern", "steady", "Load pattern: steady or bursty")
	flag.DurationVar(&duration, "duration", 60*time.Second, "Benchmark duration")
	flag.IntVar(&rate, "rate", 100, "Steady-state ops/sec")
	flag.IntVar(&burst, "burst-rate", 500, "Burst-phase ops/sec")
	flag.DurationVar(&burstDur, "burst-duration", 10*time.Second, "Burst phase length")
	flag.DurationVar(&calmDur, "calm-duration", 20*time.Second, "Calm phase length")
	flag.DurationVar(&warmup, "warmup", 10*time.Second, "Warm-up wait before benchmark")
	flag.Parse()

	// Override from environment variables
	if e := os.Getenv("TARGETS"); e != "" {
		targets = e
	}
	if e := os.Getenv("PATTERN"); e != "" {
		pattern = e
	}
	if e := os.Getenv("DURATION"); e != "" {
		if d, err := time.ParseDuration(e); err == nil {
			duration = d
		}
	}
	if e := os.Getenv("RATE"); e != "" {
		fmt.Sscanf(e, "%d", &rate)
	}
	if e := os.Getenv("BURST_RATE"); e != "" {
		fmt.Sscanf(e, "%d", &burst)
	}
	if e := os.Getenv("WARMUP"); e != "" {
		if d, err := time.ParseDuration(e); err == nil {
			warmup = d
		}
	}

	if targets == "" {
		log.Fatal("[bench] -targets or TARGETS env var is required")
	}

	return config{
		Targets:       strings.Split(targets, ","),
		Pattern:       pattern,
		Duration:      duration,
		Rate:          rate,
		BurstRate:     burst,
		BurstDuration: burstDur,
		CalmDuration:  calmDur,
		WarmUp:        warmup,
	}
}

func runBenchmark(cfg config, st *stats) {
	start := time.Now()
	deadline := start.Add(cfg.Duration)

	var wg sync.WaitGroup
	sem := make(chan struct{}, 200) // concurrency limiter

	for time.Now().Before(deadline) {
		currentRate := cfg.Rate

		if cfg.Pattern == "bursty" {
			elapsed := time.Since(start)
			cycleLen := cfg.BurstDuration + cfg.CalmDuration
			phase := elapsed % cycleLen
			if phase < cfg.BurstDuration {
				currentRate = cfg.BurstRate
			}
		}

		if currentRate <= 0 {
			currentRate = 1
		}
		interval := time.Second / time.Duration(currentRate)

		// Pick a target; prefer leader but fall back to random node.
		target := pickTarget(cfg.Targets)

		sem <- struct{}{} // acquire
		wg.Add(1)
		go func(t string) {
			defer wg.Done()
			defer func() { <-sem }() // release

			cmd := fmt.Sprintf("SET key_%d=%d", rand.Intn(10000), time.Now().UnixNano())
			latency, err := propose(t, cmd)
			st.record(result{latency: latency, success: err == nil, timestamp: time.Now()})
		}(target)

		time.Sleep(interval)
	}

	wg.Wait()
}

func pickTarget(targets []string) string {
	// Try to find the leader first (cached attempt).
	leader := discoverLeader(targets)
	if leader != "" {
		return leader
	}
	return targets[rand.Intn(len(targets))]
}

// ─────────────────────────────────────────────────────────────────────
// Report
// ─────────────────────────────────────────────────────────────────────

func report(st *stats, totalDur time.Duration, electionDurations []time.Duration, electionsBefore, electionsAfter int64, recoveryMs, partDurationMs float64, tl *timeline) {
	st.mu.Lock()
	latencies := make([]time.Duration, len(st.latencies))
	copy(latencies, st.latencies)
	st.mu.Unlock()

	successes := atomic.LoadInt64(&st.successes)
	failures := atomic.LoadInt64(&st.failures)
	total := successes + failures

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════╗")
	fmt.Println("║           ADAPTIVE RAFT BENCHMARK RESULTS              ║")
	fmt.Println("╠══════════════════════════════════════════════════════════╣")
	fmt.Printf("║  Duration         : %-36s ║\n", totalDur.Round(time.Millisecond))
	fmt.Printf("║  Total Ops        : %-36d ║\n", total)
	fmt.Printf("║  Successes        : %-36d ║\n", successes)
	fmt.Printf("║  Failures         : %-36d ║\n", failures)
	if totalDur.Seconds() > 0 {
		throughput := float64(successes) / totalDur.Seconds()
		fmt.Printf("║  Throughput       : %-33.2f ops/s ║\n", throughput)
	}
	fmt.Println("╠══════════════════════════════════════════════════════════╣")
	fmt.Println("║  CONSENSUS LATENCY                                     ║")
	fmt.Println("╠══════════════════════════════════════════════════════════╣")
	if len(latencies) > 0 {
		fmt.Printf("║  Min              : %-36s ║\n", latencies[0])
		fmt.Printf("║  Median (P50)     : %-36s ║\n", percentile(latencies, 50))
		fmt.Printf("║  P90              : %-36s ║\n", percentile(latencies, 90))
		fmt.Printf("║  P95              : %-36s ║\n", percentile(latencies, 95))
		fmt.Printf("║  P99              : %-36s ║\n", percentile(latencies, 99))
		fmt.Printf("║  Max              : %-36s ║\n", latencies[len(latencies)-1])
		fmt.Printf("║  Mean             : %-36s ║\n", mean(latencies))
	} else {
		fmt.Println("║  (no successful proposals)                              ║")
	}
	fmt.Println("╠══════════════════════════════════════════════════════════╣")
	fmt.Println("║  LEADER ELECTION                                       ║")
	fmt.Println("╠══════════════════════════════════════════════════════════╣")
	elecDelta := electionsAfter - electionsBefore
	fmt.Printf("║  Elections (Prom) : %-36d ║\n", elecDelta)
	if len(electionDurations) > 0 {
		sort.Slice(electionDurations, func(i, j int) bool {
			return electionDurations[i] < electionDurations[j]
		})
		fmt.Printf("║  Observed (poll)  : %-36d ║\n", len(electionDurations))
		fmt.Printf("║  Min duration     : %-36s ║\n", electionDurations[0])
		fmt.Printf("║  Max duration     : %-36s ║\n", electionDurations[len(electionDurations)-1])
		fmt.Printf("║  Mean duration    : %-36s ║\n", mean(electionDurations))
	} else {
		fmt.Println("║  No election durations measured (no leader change)      ║")
	}
	if recoveryMs > 0 || partDurationMs > 0 {
		fmt.Println("╠══════════════════════════════════════════════════════════╣")
		fmt.Println("║  PARTITION RECOVERY                                     ║")
		fmt.Println("╠══════════════════════════════════════════════════════════╣")
		fmt.Printf("║  Partition Duration : %-33.1f ms  ║\n", partDurationMs)
		fmt.Printf("║  Recovery Time      : %-33.1f ms  ║\n", recoveryMs)
	}
	fmt.Println("╚══════════════════════════════════════════════════════════╝")
	fmt.Println()

	// Also write machine-readable JSON
	timestamp := time.Now().Format("20060102_150405")
	jsonReport := map[string]interface{}{
		"timestamp":          timestamp,
		"duration_s":         totalDur.Seconds(),
		"total_ops":          total,
		"successes":          successes,
		"failures":           failures,
		"throughput":         float64(successes) / math.Max(totalDur.Seconds(), 0.001),
		"elections_total":    elecDelta,
		"elections_observed": len(electionDurations),
	}
	if len(latencies) > 0 {
		jsonReport["latency_p50_ms"] = float64(percentile(latencies, 50).Microseconds()) / 1000.0
		jsonReport["latency_p95_ms"] = float64(percentile(latencies, 95).Microseconds()) / 1000.0
		jsonReport["latency_p99_ms"] = float64(percentile(latencies, 99).Microseconds()) / 1000.0
		jsonReport["latency_mean_ms"] = float64(mean(latencies).Microseconds()) / 1000.0
	}
	if len(electionDurations) > 0 {
		jsonReport["election_min_ms"] = float64(electionDurations[0].Microseconds()) / 1000.0
		jsonReport["election_max_ms"] = float64(electionDurations[len(electionDurations)-1].Microseconds()) / 1000.0
		jsonReport["election_mean_ms"] = float64(mean(electionDurations).Microseconds()) / 1000.0
	}
	if recoveryMs > 0 {
		jsonReport["partition_recovery_ms"] = recoveryMs
		jsonReport["partition_duration_ms"] = partDurationMs
	}
	data, _ := json.MarshalIndent(jsonReport, "", "  ")
	filename := fmt.Sprintf("/app/results/benchmark_%s.json", timestamp)
	_ = os.WriteFile(filename, data, 0644)
	fmt.Printf("JSON results written to %s\n", filename)

	// Write timeline data
	timelineData := map[string]interface{}{
		"timestamp":  timestamp,
		"duration_s": totalDur.Seconds(),
		"entries":    tl.getEntries(),
	}
	timelineJSON, _ := json.MarshalIndent(timelineData, "", "  ")
	timelineFilename := fmt.Sprintf("/app/results/timeline_%s.json", timestamp)
	_ = os.WriteFile(timelineFilename, timelineJSON, 0644)
	fmt.Printf("Timeline data written to %s\n", timelineFilename)
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p/100.0*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func mean(vals []time.Duration) time.Duration {
	if len(vals) == 0 {
		return 0
	}
	var total time.Duration
	for _, v := range vals {
		total += v
	}
	return total / time.Duration(len(vals))
}
