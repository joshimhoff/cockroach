// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func BenchmarkAllocateTokens(b *testing.B) {
	granter, allocator, _ := setup()
	ctx := context.Background()
	allocator.resetInterval(ctx)

	b.ResetTimer()
	i := 0
	for b.Loop() {
		remainingTicks := int64(1000 - (i % 1000))
		if remainingTicks == 0 {
			remainingTicks = 1
		}
		allocator.allocateTokens(remainingTicks)

		if i%1000 == 999 {
			granter.mu.Lock()
			granter.mu.buckets[0][0] = tokenBucket{0}
			granter.mu.buckets[0][1] = tokenBucket{0}
			granter.mu.buckets[1][0] = tokenBucket{0}
			granter.mu.buckets[1][1] = tokenBucket{0}
			granter.mu.Unlock()
			allocator.resetInterval(ctx)
		}

		i++
	}
}

func BenchmarkResetInterval(b *testing.B) {
	granter, allocator, _ := setup()
	ctx := context.Background()
	allocator.resetInterval(ctx)

	b.ResetTimer()
	for b.Loop() {
		granter.mu.Lock()
		granter.mu.buckets[0][0] = tokenBucket{0}
		granter.mu.buckets[0][1] = tokenBucket{0}
		granter.mu.buckets[1][0] = tokenBucket{0}
		granter.mu.buckets[1][1] = tokenBucket{0}
		granter.mu.Unlock()
		allocator.resetInterval(ctx)
	}
}

func BenchmarkFiller(b *testing.B) {
	granter, allocator, testTime := setup()
	ctx := context.Background()

	filler := &cpuTimeTokenFiller{
		allocator:  allocator,
		timeSource: testTime,
		closeCh:    make(chan struct{}),
	}

	allocator.resetInterval(ctx)
	filler.intervalStart = testTime.Now()
	filler.lastRemainingTicks = int64(time.Second / timePerTick)

	b.ResetTimer()
	i := 0
	for b.Loop() {
		testTime.Advance(timePerTick)
		tickTime := testTime.Now()

		if i%1000 == 999 {
			granter.mu.Lock()
			granter.mu.buckets[0][0] = tokenBucket{0}
			granter.mu.buckets[0][1] = tokenBucket{0}
			granter.mu.buckets[1][0] = tokenBucket{0}
			granter.mu.buckets[1][1] = tokenBucket{0}
			granter.mu.Unlock()
		}

		filler.tick(ctx, tickTime)
		i++
	}
}

func setup() (*cpuTimeTokenGranter, *cpuTimeTokenAllocator, *timeutil.ManualTime) {
	granter := &cpuTimeTokenGranter{}
	tier0Granter := &cpuTimeTokenChildGranter{
		tier:   testTier0,
		parent: granter,
	}
	tier1Granter := &cpuTimeTokenChildGranter{
		tier:   testTier1,
		parent: granter,
	}
	granter.requester[testTier0] = &testRequester{
		additionalID: "tier0",
		granter:      tier0Granter,
	}
	granter.requester[testTier1] = &testRequester{
		additionalID: "tier1",
		granter:      tier1Granter,
	}

	testTime := timeutil.NewManualTime(time.Now())
	cpuProvider := &benchCPUMetricsProvider{
		capacity:     8.0,
		totalCPUTime: 0,
		deltaCPUTime: 5 * time.Second,
	}

	model := &cpuTimeTokenLinearModel{
		timeSource:         testTime,
		granter:            granter,
		cpuMetricsProvider: cpuProvider,
	}

	allocator := &cpuTimeTokenAllocator{
		granter:  granter,
		settings: cluster.MakeClusterSettings(),
		model:    model,
	}

	return granter, allocator, testTime
}

type benchCPUMetricsProvider struct {
	capacity     float64
	totalCPUTime time.Duration
	deltaCPUTime time.Duration
}

func (p *benchCPUMetricsProvider) GetCPUUsage() (totalCPUTime time.Duration, err error) {
	p.totalCPUTime += p.deltaCPUTime
	return p.totalCPUTime, nil
}

func (p *benchCPUMetricsProvider) GetCPUCapacity() (cpuCapacity float64) {
	return p.capacity
}
