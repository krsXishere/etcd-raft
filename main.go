// Adaptive Raft – 5-node cluster with PI-controlled election timeout.
//
// Usage:
//
//	go run main.go -id 1 -port 9001 -peers "2=127.0.0.1:9002,3=127.0.0.1:9003,4=127.0.0.1:9004,5=127.0.0.1:9005"
//
// Or launch all 5 with the helper script (see README).
package main

import (
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
	"github.com/prometheus/client_golang/prometheus/promhttp" // Biasanya ini juga butuh
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
		ratio       float64
		rttWindow   int
		metricsPort int = 9100 // Port untuk Prometheus metrics endpoint
	)

	flag.Uint64Var(&id, "id", 0, "Unique node ID (1-5)")
	flag.IntVar(&port, "port", 0, "Listen port for this node")
	flag.StringVar(&peersFlag, "peers", "", "Comma-separated peer list: id=host:port,...")
	flag.IntVar(&metricsPort, "metric-port", 9100, "Port for Prometheus metrics endpoint")
	flag.DurationVar(&baselineRTT, "baseline-rtt", 5*time.Millisecond, "Expected baseline RTT")
	flag.DurationVar(&tBase, "t-base", 300*time.Millisecond, "Base election timeout")
	flag.DurationVar(&tMin, "t-min", 150*time.Millisecond, "Minimum election timeout")
	flag.DurationVar(&tMax, "t-max", 3*time.Second, "Maximum election timeout")
	flag.Float64Var(&kp, "kp", 2.0, "Proportional gain")
	flag.Float64Var(&ki, "ki", 0.5, "Integral gain")
	flag.Float64Var(&ratio, "ratio", 5.0, "Heartbeat ratio (electionTimeout / R)")
	flag.IntVar(&rttWindow, "rtt-window", 20, "RTT sliding window size")

	flag.Parse()

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		addr := fmt.Sprintf(":%d", metricsPort)
		log.Printf("Prometheus metrics endpoint listening on %s/metrics", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Fatalf("metrics http server: %v", err)
		}
	}()

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
	log.Printf("[mAin] metrics log file: %s", fileObs.FilePath())

	// Combine both observers so every metric goes to console + file.
	observer := metrics.NewMultiObserver(logObs, fileObs)

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

	log.Printf("[main] node %d running on port %d, peers=%v", id, port, peerIDs)

	// Wait for interrupt.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Printf("[main] shutting down node %d...", id)
	node.Stop()
}
