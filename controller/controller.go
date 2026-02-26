// Package controller implements a Proportional-Integral-Derivative (PID)
// controller that adaptively tunes the Raft election timeout based on
// observed RTT.
//
// Control law:
//
//	error        = observedRTT − baselineRTT
//	rawOutput    = T_base + Kp·error + Ki·∫error + Kd·(Δerror/Δt)
//	electionTimeout = clamp(rawOutput, T_min, T_max)
//
// Integral windup is prevented with back-calculation clamping: whenever
// the raw output would exceed the bounds, the integral term is adjusted
// so that the output sits exactly at the violated bound.  This keeps the
// integrator from accumulating error that it can never "unwind", which
// would otherwise cause sluggish recovery when RTT returns to normal.
//
// The derivative term is computed as (error − prevError) / Δt and is
// optionally smoothed with a low-pass filter (alpha = 0.3) to avoid
// amplifying high-frequency measurement noise.
package controller

import (
	"sync"
	"time"
)

// Config holds the tuning knobs for the PID controller.
type Config struct {
	// BaselineRTT is the expected "normal" round-trip time.
	BaselineRTT time.Duration

	// Kp is the proportional gain.
	Kp float64

	// Ki is the integral gain.
	Ki float64

	// Kd is the derivative gain.
	Kd float64

	// TBase is the base election timeout before any correction.
	TBase time.Duration

	// TMin / TMax clamp the resulting election timeout.
	TMin time.Duration
	TMax time.Duration

	// HeartbeatRatio (R): heartbeatInterval = electionTimeout / R.
	HeartbeatRatio float64
}

// DefaultConfig returns sensible defaults for a local / low-latency cluster.
func DefaultConfig() Config {
	return Config{
		BaselineRTT:    5 * time.Millisecond,
		Kp:             2.0,
		Ki:             0.5,
		Kd:             0.1,
		TBase:          300 * time.Millisecond,
		TMin:           150 * time.Millisecond,
		TMax:           3000 * time.Millisecond,
		HeartbeatRatio: 5.0,
	}
}

// PIController is a lightweight PID controller that outputs an election
// timeout value based on observed RTT error.
type PIController struct {
	mu sync.RWMutex

	cfg      Config
	integral float64 // accumulated error (seconds)
	lastErr  float64 // most recent error (seconds), for observability
	prevErr  float64 // previous error for derivative calculation (seconds)

	// Derivative state
	filteredDeriv float64   // low-pass filtered derivative (seconds/second)
	lastUpdate    time.Time // timestamp of previous Update call
	firstUpdate   bool      // true until the second call to Update

	electionTimeout   time.Duration
	heartbeatInterval time.Duration
}

// New creates a PIController with the given configuration.
// The initial output is T_base with a zero integral.
func New(cfg Config) *PIController {
	pi := &PIController{
		cfg:         cfg,
		firstUpdate: true,
	}
	pi.electionTimeout = cfg.TBase
	pi.heartbeatInterval = deriveHeartbeat(cfg.TBase, cfg.HeartbeatRatio)
	return pi
}

// Update accepts a new RTT observation, computes the error relative to
// the configured baseline, updates the integral and derivative terms,
// and produces a clamped election timeout.
//
// Internally all arithmetic is in float64 seconds; the public result is
// returned (and stored) as time.Duration.
func (pi *PIController) Update(observedRTT time.Duration) {
	pi.mu.Lock()
	defer pi.mu.Unlock()

	now := time.Now()

	// ── 1. Compute error ────────────────────────────────────────────
	errSec := (observedRTT - pi.cfg.BaselineRTT).Seconds()
	pi.lastErr = errSec

	// ── 2. Tentatively accumulate integral ──────────────────────────
	candidate := pi.integral + errSec

	// ── 3. Compute derivative term ─────────────────────────────────
	var dTerm float64
	if pi.firstUpdate {
		// No previous sample: derivative is zero on the first call.
		pi.firstUpdate = false
	} else {
		dt := now.Sub(pi.lastUpdate).Seconds()
		if dt > 0 {
			rawDeriv := (errSec - pi.prevErr) / dt
			// Low-pass filter (α = 0.3) to suppress measurement noise.
			const alpha = 0.3
			pi.filteredDeriv = alpha*rawDeriv + (1-alpha)*pi.filteredDeriv
			dTerm = pi.cfg.Kd * pi.filteredDeriv
		}
	}
	pi.prevErr = errSec
	pi.lastUpdate = now

	// ── 4. Compute raw (unclamped) PID output ───────────────────────
	pTerm := pi.cfg.Kp * errSec
	iTerm := pi.cfg.Ki * candidate
	raw := pi.cfg.TBase.Seconds() + pTerm + iTerm + dTerm

	// ── 5. Clamp output to [TMin, TMax] ─────────────────────────────
	tMin := pi.cfg.TMin.Seconds()
	tMax := pi.cfg.TMax.Seconds()

	clamped := raw
	if clamped < tMin {
		clamped = tMin
	}
	if clamped > tMax {
		clamped = tMax
	}

	// ── 6. Anti-windup via back-calculation ─────────────────────────
	// If the output was clamped and Ki != 0, solve for the integral
	// value that would place the output exactly at the bound:
	//    bound = TBase + Kp·e + Ki·integral + Kd·d
	//    →  integral = (bound − TBase − Kp·e − Kd·d) / Ki
	// This prevents the integrator from winding up while the output
	// is saturated.
	if raw != clamped && pi.cfg.Ki != 0 {
		pi.integral = (clamped - pi.cfg.TBase.Seconds() - pTerm - dTerm) / pi.cfg.Ki
	} else {
		pi.integral = candidate
	}

	// ── 7. Store results ────────────────────────────────────────────
	pi.electionTimeout = secToDuration(clamped)
	pi.heartbeatInterval = deriveHeartbeat(pi.electionTimeout, pi.cfg.HeartbeatRatio)
}

// Reset clears the integral and derivative terms and reverts to base values.
func (pi *PIController) Reset() {
	pi.mu.Lock()
	defer pi.mu.Unlock()
	pi.integral = 0
	pi.lastErr = 0
	pi.prevErr = 0
	pi.filteredDeriv = 0
	pi.firstUpdate = true
	pi.electionTimeout = pi.cfg.TBase
	pi.heartbeatInterval = deriveHeartbeat(pi.cfg.TBase, pi.cfg.HeartbeatRatio)
}

// GetElectionTimeout returns the current election timeout.
func (pi *PIController) GetElectionTimeout() time.Duration {
	pi.mu.RLock()
	defer pi.mu.RUnlock()
	return pi.electionTimeout
}

// GetHeartbeatInterval returns the current heartbeat interval,
// always derived as electionTimeout / R.
func (pi *PIController) GetHeartbeatInterval() time.Duration {
	pi.mu.RLock()
	defer pi.mu.RUnlock()
	return pi.heartbeatInterval
}

// GetLastError returns the most recent error value (observed − baseline)
// as a time.Duration, useful for logging / metrics.
func (pi *PIController) GetLastError() time.Duration {
	pi.mu.RLock()
	defer pi.mu.RUnlock()
	return secToDuration(pi.lastErr)
}

// GetDerivative returns the most recent filtered derivative value
// (dError/dt in seconds per second), useful for logging / metrics.
func (pi *PIController) GetDerivative() float64 {
	pi.mu.RLock()
	defer pi.mu.RUnlock()
	return pi.filteredDeriv
}

// Config returns a copy of the controller's configuration.
func (pi *PIController) Config() Config {
	pi.mu.RLock()
	defer pi.mu.RUnlock()
	return pi.cfg
}

// ──────────────────────────────────────────────────────────────────────
// Internal helpers
// ──────────────────────────────────────────────────────────────────────

// secToDuration converts a float64 seconds value to time.Duration.
func secToDuration(sec float64) time.Duration {
	return time.Duration(sec * float64(time.Second))
}

// deriveHeartbeat computes heartbeatInterval = electionTimeout / R.
func deriveHeartbeat(electionTimeout time.Duration, ratio float64) time.Duration {
	if ratio <= 0 {
		ratio = 5.0
	}
	return time.Duration(float64(electionTimeout) / ratio)
}
