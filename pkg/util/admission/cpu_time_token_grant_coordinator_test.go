// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestCPUTimeTokenACEnableAndDisable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var ambientCtx log.AmbientContext
	settings := cluster.MakeTestingClusterSettings()
	registry := metric.NewRegistry()
	var opts Options
	knobs := &TestingKnobs{DisableCPUTimeTokenFillerGoroutine: true}
	coords := NewGrantCoordinators(ambientCtx, settings, opts, registry, &noopOnLogEntryAdmitted{}, knobs)
	defer coords.Close()
	cpuCoords := coords.RegularCPU

	defer func(prev bool) {
		cpuTimeTokenACEnabled.Override(context.Background(), &settings.SV, prev)
	}(cpuTimeTokenACEnabled.Get(&settings.SV))

	// Test that if setting is disabled, WorkQueues uses slots, else they
	// use CPU time tokens.
	cpuTimeTokenACEnabled.Override(context.Background(), &settings.SV, false)
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */).mode)
	require.Equal(t, usesSlots, cpuCoords.GetKVWorkQueue(true /* isSystemTenant */).mode)
	// If CPU time token AC is disabled, we use one WorkQueue for both
	// system & app tenant work.
	require.Equal(t, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */), cpuCoords.GetKVWorkQueue(true /* isSystemTenant */))

	cpuTimeTokenACEnabled.Override(context.Background(), &settings.SV, true)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */).mode)
	require.Equal(t, usesCPUTimeTokens, cpuCoords.GetKVWorkQueue(true /* isSystemTenant */).mode)
	// If CPU time token AC is enabled, we use one WorkQueue for system
	// tenant work & a second WorkQueue for app tenant work.
	require.NotEqual(t, cpuCoords.GetKVWorkQueue(false /* isSystemTenant */), cpuCoords.GetKVWorkQueue(true /* isSystemTenant */))
}

// simulationCPUMetricsProvider is a stub CPUMetricsProvider for integration
// testing. The cumulative CPU time is updated externally based on the actual
// concurrency of admitted requests.
type simulationCPUMetricsProvider struct {
	mu            syncutil.Mutex
	cumulativeCPU time.Duration
	cpuCapacity   float64
}

func (p *simulationCPUMetricsProvider) GetCPUUsage() (time.Duration, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cumulativeCPU, nil
}

func (p *simulationCPUMetricsProvider) GetCPUCapacity() float64 {
	return p.cpuCapacity
}

// simPhase describes a phase of the simulation with fixed parameters.
type simPhase struct {
	duration      time.Duration
	concurrency   int
	cpuMultiplier float64       // multiplied by actual concurrency for CPU usage reporting
	cpuTimeMean   time.Duration // mean CPU time per request
	cpuTimeStddev time.Duration // stddev of CPU time per request
	ioWaitMean    time.Duration // mean non-CPU wait time (e.g. blocking on I/O)
	ioWaitStddev  time.Duration // stddev of non-CPU wait time
}

// simTestCase describes a full simulation test case.
type simTestCase struct {
	name        string
	cpuCapacity float64
	phases      []simPhase
}

// TestCPUTimeTokenACIntegration is a simulation-style integration test of
// CPU time token admission control. It uses a manual time source and a
// stubbed CPU metrics provider, but otherwise exercises the real AC
// machinery (filler, allocator, model, granter, work queue).
//
// The simulation launches a fixed number of concurrent requests (the
// phase's concurrency parameter). Each request calls Admit, waits for
// its chosen CPU time to elapse (simulated), then calls AdmittedWorkDone.
// The CPU metrics stub reports CPU usage as actual concurrency multiplied
// by the phase's cpuMultiplier, simulating untracked CPU work.
//
// The test prints ASCII line graphs of:
//  1. Intended concurrency over time.
//  2. AC wait time (per-second average).
//  3. CPU usage / actual concurrency over time (per-second average).
//  4. Token-to-CPU-time multiplier over time.
func TestCPUTimeTokenACIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []simTestCase{
		{
			name:        "baseline",
			cpuCapacity: 10.0,
			phases: []simPhase{
				{
					duration:      60 * time.Second,
					concurrency:   3,
					cpuMultiplier: 1.0,
					cpuTimeMean:   5 * time.Millisecond,
					cpuTimeStddev: 20 * time.Millisecond,
					ioWaitMean:    1 * time.Millisecond,
					ioWaitStddev:  10 * time.Microsecond,
				},
				{
					duration:    60 * time.Second,
					concurrency: 100,
					// TODO(josh): Debug why this being 4 leads to low CPU usage.
					cpuMultiplier: 4.0,
					cpuTimeMean:   2 * time.Millisecond,
					cpuTimeStddev: 10 * time.Millisecond,
					ioWaitMean:    2 * time.Millisecond,
					ioWaitStddev:  40 * time.Microsecond,
				},
				{
					duration:      60 * time.Second,
					concurrency:   4,
					cpuMultiplier: 2.0,
					cpuTimeMean:   4 * time.Millisecond,
					cpuTimeStddev: 10 * time.Millisecond,
					ioWaitMean:    2 * time.Millisecond,
					ioWaitStddev:  80 * time.Microsecond,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			runSimTestCase(t, tc)
		})
	}
}

func runSimTestCase(t *testing.T, tc simTestCase) {
	rng := rand.New(rand.NewSource(42))

	// Compute total duration from phases.
	var totalDuration time.Duration
	for _, p := range tc.phases {
		totalDuration += p.duration
	}
	numMs := int(totalDuration / time.Millisecond)
	numSeconds := int(totalDuration / time.Second)

	// Manual time source.
	timeSource := timeutil.NewManualTime(time.Unix(0, 0))

	// Stub CPU metrics provider.
	cpuMetrics := &simulationCPUMetricsProvider{
		cpuCapacity: tc.cpuCapacity,
	}

	// Create coordinator with manual time source and disabled filler
	// goroutine (we start it manually after setting up the tick channel).
	var ambientCtx log.AmbientContext
	settings := cluster.MakeTestingClusterSettings()
	registry := metric.NewRegistry()
	opts := Options{CPUMetricsProvider: cpuMetrics}
	knobs := &TestingKnobs{
		TimeSource:                         timeSource,
		DisableCPUTimeTokenFillerGoroutine: true,
	}
	coord := makeCPUTimeTokenGrantCoordinator(ambientCtx, opts, settings, registry, knobs)
	defer coord.close()

	// Set up tick synchronization: the filler goroutine sends on tickCh
	// after processing each tick, so the main loop can advance time in
	// lockstep.
	tickCh := make(chan struct{})
	coord.filler.tickCh = &tickCh

	// Start filler manually.
	coord.filler.start(context.Background())

	// Get app tenant work queue.
	wq := coord.getWorkQueue(appTenant)

	// Get model reference for observing the multiplier.
	model := coord.filler.allocator.(*cpuTimeTokenAllocator).model.(*cpuTimeTokenLinearModel)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Ongoing request tracking.
	type ongoingReq struct {
		cpuTime    time.Duration
		ioWaitTime time.Duration
		startTime  time.Time // when Admit was called
		admitTime  time.Time // when Admit returned
		admitted   atomic.Bool
		cpuDone    bool // set by main loop when CPU phase ends
		resp       AdmitResponse
		done       chan struct{} // closed to signal the goroutine to call AdmittedWorkDone
		finished   chan struct{} // closed by the goroutine after AdmittedWorkDone
	}
	ongoing := make(map[int]*ongoingReq)
	nextID := 0
	var actualConcurrency atomic.Int64
	var wg sync.WaitGroup

	// Recording.
	cpuUsageSamples := make([]float64, 0, numMs)
	waitTimesPerSecond := make([][]time.Duration, numSeconds)
	multiplierPerSecond := make([]float64, numSeconds)
	intendedConcurrencyPerSecond := make([]float64, numSeconds)

	// Phase tracking.
	phaseIdx := 0
	phaseEndMs := int(tc.phases[0].duration / time.Millisecond)

	// Simulation loop: one iteration per millisecond of simulated time.
	for ms := 0; ms < numMs; ms++ {
		// Advance phase if needed.
		if ms >= phaseEndMs && phaseIdx < len(tc.phases)-1 {
			phaseIdx++
			phaseEndMs += int(tc.phases[phaseIdx].duration / time.Millisecond)
		}
		phase := tc.phases[phaseIdx]

		// Update cumulative CPU based on actual concurrency, scaled
		// by the phase's cpuMultiplier to simulate untracked CPU work.
		cpuMetrics.mu.Lock()
		cpuMetrics.cumulativeCPU += time.Duration(
			float64(actualConcurrency.Load()) * phase.cpuMultiplier * float64(time.Millisecond))
		cpuMetrics.mu.Unlock()

		// Advance simulated time by 1ms. This triggers the filler's
		// ticker, which allocates tokens and potentially grants
		// waiting requests.
		timeSource.Advance(time.Millisecond)
		<-tickCh

		// Record multiplier at the end of each second.
		if (ms+1)%1000 == 0 {
			sec := ms / 1000
			multiplierPerSecond[sec] = model.tokenToCPUTimeMultiplier
		}

		// Check for requests transitioning from CPU-active to
		// I/O-waiting, and for fully completed requests.
		now := timeSource.Now()
		for id, req := range ongoing {
			if !req.admitted.Load() {
				continue
			}
			elapsed := now.Sub(req.admitTime)

			// Transition from CPU-active to I/O-waiting: stop
			// counting toward CPU usage but keep in ongoing.
			if !req.cpuDone && elapsed >= req.cpuTime {
				req.cpuDone = true
				actualConcurrency.Add(-1)
			}

			// Request fully complete (CPU + I/O).
			if elapsed < req.cpuTime+req.ioWaitTime {
				continue
			}
			// Signal the goroutine to call AdmittedWorkDone.
			close(req.done)
			<-req.finished

			// Record wait time.
			waitTime := req.admitTime.Sub(req.startTime)
			sec := ms / 1000
			if sec < numSeconds {
				waitTimesPerSecond[sec] = append(waitTimesPerSecond[sec], waitTime)
			}
			delete(ongoing, id)
		}

		// Start new requests up to the phase's concurrency target.
		for len(ongoing) < phase.concurrency {
			meanMs := float64(phase.cpuTimeMean) / float64(time.Millisecond)
			stddevMs := float64(phase.cpuTimeStddev) / float64(time.Millisecond)
			cpuTimeMs := rng.NormFloat64()*stddevMs + meanMs
			if cpuTimeMs < 1 {
				cpuTimeMs = 1
			}
			if cpuTimeMs > 100 {
				cpuTimeMs = 100
			}
			cpuTime := time.Duration(cpuTimeMs * float64(time.Millisecond))

			ioMeanMs := float64(phase.ioWaitMean) / float64(time.Millisecond)
			ioStddevMs := float64(phase.ioWaitStddev) / float64(time.Millisecond)
			var ioWaitTime time.Duration
			if ioMeanMs > 0 {
				ioWaitMs := rng.NormFloat64()*ioStddevMs + ioMeanMs
				if ioWaitMs < 0 {
					ioWaitMs = 0
				}
				ioWaitTime = time.Duration(ioWaitMs * float64(time.Millisecond))
			}

			req := &ongoingReq{
				cpuTime:    cpuTime,
				ioWaitTime: ioWaitTime,
				done:       make(chan struct{}),
				finished:   make(chan struct{}),
			}
			ongoing[nextID] = req
			nextID++

			wg.Add(1)
			go func(r *ongoingReq) {
				defer wg.Done()
				r.startTime = timeSource.Now()
				resp, err := wq.Admit(ctx, WorkInfo{
					TenantID:   roachpb.MustMakeTenantID(2),
					Priority:   admissionpb.NormalPri,
					CreateTime: timeSource.Now().UnixNano(),
				})
				if err != nil {
					// Context canceled during cleanup.
					close(r.finished)
					return
				}
				r.resp = resp
				r.admitTime = timeSource.Now()
				r.admitted.Store(true)
				actualConcurrency.Add(1)

				<-r.done
				wq.AdmittedWorkDone(r.resp, r.cpuTime)
				close(r.finished)
			}(req)
		}

		// Record CPU usage sample (actual concurrency * cpuMultiplier).
		cpuUsageSamples = append(cpuUsageSamples, float64(actualConcurrency.Load())*phase.cpuMultiplier)
	}

	// Cleanup: cancel remaining requests and wait for all goroutines.
	cancel()
	for _, req := range ongoing {
		select {
		case <-req.done:
		default:
			close(req.done)
		}
	}
	wg.Wait()

	// Compute per-second aggregates.
	avgCPUUsage := make([]float64, numSeconds)
	avgWaitTimeMs := make([]float64, numSeconds)

	// Rebuild phase index for per-second intended concurrency.
	pIdx := 0
	pEndSec := int(tc.phases[0].duration / time.Second)
	for s := 0; s < numSeconds; s++ {
		if s >= pEndSec && pIdx < len(tc.phases)-1 {
			pIdx++
			pEndSec += int(tc.phases[pIdx].duration / time.Second)
		}
		intendedConcurrencyPerSecond[s] = float64(tc.phases[pIdx].concurrency)

		// Average CPU usage for this second.
		start := s * 1000
		end := start + 1000
		if end > len(cpuUsageSamples) {
			end = len(cpuUsageSamples)
		}
		var sum float64
		count := 0
		for i := start; i < end; i++ {
			sum += cpuUsageSamples[i]
			count++
		}
		if count > 0 {
			avgCPUUsage[s] = sum / float64(count)
		}

		// Average AC wait time for requests completing in this second.
		if len(waitTimesPerSecond[s]) > 0 {
			var totalWait time.Duration
			for _, w := range waitTimesPerSecond[s] {
				totalWait += w
			}
			avgWaitTimeMs[s] = float64(totalWait.Milliseconds()) / float64(len(waitTimesPerSecond[s]))
		}
	}

	// Compute CPU usage as fraction of capacity.
	cpuUsageFraction := make([]float64, numSeconds)
	for s := 0; s < numSeconds; s++ {
		cpuUsageFraction[s] = avgCPUUsage[s] / tc.cpuCapacity
	}

	// Print ASCII line graphs.
	printASCIILineGraph(t, "Intended Concurrency", intendedConcurrencyPerSecond)
	printASCIILineGraph(t, "AC Wait Time (ms)", avgWaitTimeMs)
	printASCIILineGraph(t, "CPU Usage (Cores)", avgCPUUsage)
	printASCIILineGraph(t, "CPU Usage (Fraction of Capacity)", cpuUsageFraction)
	printASCIILineGraph(t, "Token-to-CPU-Time Multiplier", multiplierPerSecond)
}

// printASCIILineGraph prints a line graph with time on the x-axis.
// Each element in data corresponds to one second.
func printASCIILineGraph(t *testing.T, title string, data []float64) {
	const height = 15
	n := len(data)
	if n == 0 {
		return
	}

	// Find min/max.
	minVal, maxVal := data[0], data[0]
	for _, v := range data {
		if v < minVal {
			minVal = v
		}
		if v > maxVal {
			maxVal = v
		}
	}
	if maxVal == minVal {
		minVal -= 1
		maxVal += 1
	}
	// Small visual margin.
	yRange := maxVal - minVal
	minVal -= yRange * 0.05
	maxVal += yRange * 0.05
	yRange = maxVal - minVal

	// Map a value to a row index (0 = top = maxVal).
	toRow := func(v float64) int {
		row := int(math.Round((maxVal - v) / yRange * float64(height-1)))
		if row < 0 {
			row = 0
		}
		if row >= height {
			row = height - 1
		}
		return row
	}

	// Create grid.
	grid := make([][]byte, height)
	for i := range grid {
		grid[i] = make([]byte, n)
		for j := range grid[i] {
			grid[i][j] = ' '
		}
	}

	// Plot data points and connect consecutive points vertically.
	for col := 0; col < n; col++ {
		row := toRow(data[col])
		grid[row][col] = '*'

		if col > 0 {
			prevRow := toRow(data[col-1])
			lo, hi := prevRow, row
			if lo > hi {
				lo, hi = hi, lo
			}
			for r := lo + 1; r < hi; r++ {
				grid[r][col] = ':'
			}
		}
	}

	// Print title.
	t.Logf("")
	t.Logf("== %s ==", title)

	// Print rows with y-axis labels every 2 rows.
	for row := 0; row < height; row++ {
		yVal := maxVal - float64(row)/float64(height-1)*yRange
		if row%2 == 0 {
			t.Logf("%7.2f |%s", yVal, string(grid[row]))
		} else {
			t.Logf("        |%s", string(grid[row]))
		}
	}

	// X-axis line.
	t.Logf("        +%s", strings.Repeat("-", n))

	// X-axis labels every 10 seconds.
	labelLine := make([]byte, n)
	for i := range labelLine {
		labelLine[i] = ' '
	}
	for i := 0; i < n; i += 10 {
		label := fmt.Sprintf("%d", i)
		for j := 0; j < len(label) && i+j < n; j++ {
			labelLine[i+j] = label[j]
		}
	}
	t.Logf("         %s", string(labelLine))
	t.Logf("         %*s", n/2+4, "time (s)")
}
