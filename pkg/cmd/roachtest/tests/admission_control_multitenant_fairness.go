// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	rpgrafana "github.com/cockroachdb/cockroach/pkg/cmd/roachprod/grafana"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/grafana"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
)

// This file contains two test families for CPU time token admission control:
//
// == Multi-Tenant Fairness Tests (runMultiTenantFairness) ==
//
// Cluster: 5 nodes (4 vCPUs each) — n1 = KV node, n2-n5 = one SQL-only virtual
// cluster each (4 tenants total).
//
// Flow: Load data (concurrency 25) → sleep 2min → run workload (20 min) →
// collect stats.
//
// Assertions: Throughput and latency across the 4 tenants should be within 30%
// of the mean (soft — logged but not fatal).
//
//	Test                             Workload                                Concurrency
//	read-heavy/even                  95% reads, 5B blocks, batch=100         250/tenant (equal), old AC
//	read-heavy/skewed                same                                    i*250 (1x-4x), old AC
//	write-heavy/even                 5% reads, 50KB blocks, batch=1          50/tenant (equal), old AC
//	write-heavy/skewed               same                                    i*50 (1x-4x), old AC
//	read-heavy/even/cpu-time-tokens    same as read-heavy/even                 250/tenant, cpu_time_tokens.enabled=true
//	write-heavy/even/cpu-time-tokens   same as write-heavy/even                50/tenant, cpu_time_tokens.enabled=true
//	read-heavy/skewed/cpu-time-tokens  same as read-heavy/skewed               i*250, cpu_time_tokens.enabled=true
//	write-heavy/skewed/cpu-time-tokens same as write-heavy/skewed              i*50, cpu_time_tokens.enabled=true
//
// == Noisy Neighbor Tests (runNoisyNeighborFairness) ==
//
// Cluster: 3 nodes — n1 = KV node (4 vCPUs), n2 = quiet tenant SQL (32 vCPUs),
// n3 = noisy tenant SQL (32 vCPUs). CPU time tokens always enabled.
//
// Flow: Load data → sleep 2min → Phase 1 (5 min): quiet tenant only (baseline
// latency) → Phase 2 (15 min): both tenants concurrently → compare latencies.
//
// Assertions:
//
//  1. CPU utilization is 80% ±15%.
//
//  2. Quiet tenant's phase 2 latency must be < 2x the baseline (latency
//     isolation).
//
//     Test                        Workload                            Quiet conc.  Noisy conc.
//     read-heavy/noisy-neighbor   95% reads, 20B blocks, batch=100    25           3500 (140x)
//     write-heavy/noisy-neighbor  5% reads, 50KB blocks, batch=1      25           1500 (60x)
//     tpcc/noisy-neighbor         TPCC (1 vs 50 warehouses)           10           500 (50x)
//
// == Logging ==
//
// Both test families log results to test.log on success and failure.
//
// runMultiTenantFairness logs:
//   - max-throughput-delta, average-throughput, total-ops-per-tenant (always)
//   - max-latency-delta, mean-latency-per-tenant (always)
//   - "throughput not within expectations" / "latency not within expectations"
//     if the 30% threshold is exceeded (soft, not fatal)
//   - Writes a stats.json to perf artifacts with tput/latency min/max/delta
//
// runNoisyNeighborFairness logs:
//   - "phase 1 baseline latency: <val>" (always, in seconds)
//   - "phase 2 latency: <val> (baseline: <val>, ratio: <val>)" (always)
//   - "average CPU utilization: <val>%, target: 80%, tolerance: 15%" (always,
//     from verifyCPUUtilization)
//   - Fatal if CPU utilization is outside 80% ±15%
//   - Fatal if phase 2 / baseline latency ratio >= 2.0x
//
// Key takeaway: all measured values are logged even on success, so test.log
// is sufficient for post-hoc analysis without needing Grafana.
//
// [1]: Co-locating the SQL pod and the workload generator is a bit funky, but
// it works fine enough as written and saves us from using another 4 nodes
// per test.
//
// TODO(sumeer): Now that we are counting actual CPU for inter-tenant
// fairness, alter the read-heavy workloads to perform different sized work,
// and evaluate fairness.
func registerMultiTenantFairness(r registry.Registry) {
	specs := []multiTenantFairnessSpec{
		{
			name:        "read-heavy/even",
			concurrency: func(int) int { return 250 },
			blockSize:   5,
			readPercent: 95,
			duration:    20 * time.Minute,
			batch:       100,
			maxOps:      100_000,
			query:       "SELECT k, v FROM kv",
		},
		{
			name:        "read-heavy/skewed",
			concurrency: func(i int) int { return i * 250 },
			blockSize:   5,
			readPercent: 95,
			duration:    20 * time.Minute,
			batch:       100,
			maxOps:      100_000,
			query:       "SELECT k, v FROM kv",
		},
		{
			name:        "write-heavy/even",
			concurrency: func(i int) int { return 50 },
			blockSize:   50_000,
			readPercent: 5,
			duration:    20 * time.Minute,
			batch:       1,
			maxOps:      1000,
			query:       "UPSERT INTO kv(k, v)",
		},
		{
			name:        "write-heavy/skewed",
			concurrency: func(i int) int { return i * 50 },
			blockSize:   50_000,
			readPercent: 5,
			duration:    20 * time.Minute,
			batch:       1,
			maxOps:      1000,
			query:       "UPSERT INTO kv(k, v)",
		},
		{
			name:           "read-heavy/even/cpu-time-tokens",
			concurrency:    func(int) int { return 250 },
			blockSize:      5,
			readPercent:    95,
			duration:       20 * time.Minute,
			batch:          100,
			maxOps:         100_000,
			query:          "SELECT k, v FROM kv",
			cpuTimeTokenAC: true,
		},
		{
			name:           "write-heavy/even/cpu-time-tokens",
			concurrency:    func(i int) int { return 50 },
			blockSize:      50_000,
			readPercent:    5,
			duration:       20 * time.Minute,
			batch:          1,
			maxOps:         1000,
			query:          "UPSERT INTO kv(k, v)",
			cpuTimeTokenAC: true,
		},
		{
			name:           "read-heavy/skewed/cpu-time-tokens",
			concurrency:    func(i int) int { return i * 250 },
			blockSize:      5,
			readPercent:    95,
			duration:       20 * time.Minute,
			batch:          100,
			maxOps:         100_000,
			query:          "SELECT k, v FROM kv",
			cpuTimeTokenAC: true,
		},
		{
			name:           "write-heavy/skewed/cpu-time-tokens",
			concurrency:    func(i int) int { return i * 50 },
			blockSize:      50_000,
			readPercent:    5,
			duration:       20 * time.Minute,
			batch:          1,
			maxOps:         1000,
			query:          "UPSERT INTO kv(k, v)",
			cpuTimeTokenAC: true,
		},
	}

	for _, s := range specs {
		s := s
		r.Add(registry.TestSpec{
			Name:             fmt.Sprintf("admission-control/multitenant-fairness/%s", s.name),
			Cluster:          r.MakeClusterSpec(5),
			Owner:            registry.OwnerAdmissionControl,
			Benchmark:        true,
			Leases:           registry.MetamorphicLeases,
			CompatibleClouds: registry.CloudsWithServiceRegistration,
			Suites:           registry.Suites(registry.Weekly),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runMultiTenantFairness(ctx, t, c, s)
			},
		})
	}

	noisyNeighborSpecs := []noisyNeighborSpec{
		{
			name:             "kv-read/noisy-neighbor",
			query:            "SELECT k, v FROM kv",
			readPercent:      95,
			blockSize:        20,
			batch:            100,
			maxOps:           100_000,
			quietConcurrency: 25,
			noisyConcurrency: 3500,
		},
		{
			name:             "kv-tiny/noisy-neighbor",
			query:            "SELECT k, v FROM kv",
			readPercent:      95,
			blockSize:        20,
			batch:            100,
			maxOps:           100_000,
			quietConcurrency: 1,
			noisyConcurrency: 3500,
		},
		{
			name:            "tpcc/noisy-neighbor",
			workload:        "tpcc",
			quietWarehouses: 1,
			noisyWarehouses: 200,
		},
	}
	for _, s := range noisyNeighborSpecs {
		s := s
		timeout := 40 * time.Minute
		if s.isTPCC() {
			timeout = 60 * time.Minute
		}
		r.Add(registry.TestSpec{
			Name: fmt.Sprintf("admission-control/multitenant-fairness/%s", s.name),
			Cluster: r.MakeClusterSpec(
				3, spec.CPU(4), spec.VolumeSize(4096), spec.DisableLocalSSD(),
				spec.WorkloadNodeCount(2), spec.WorkloadNodeCPU(32),
			),
			Owner:            registry.OwnerAdmissionControl,
			Benchmark:        true,
			CompatibleClouds: registry.CloudsWithServiceRegistration,
			Suites:           registry.Suites(registry.Weekly),
			Timeout:          timeout,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runNoisyNeighborFairness(ctx, t, c, s)
			},
		})
	}
}

type multiTenantFairnessSpec struct {
	name  string
	query string // query for which we'll check statistics for

	readPercent    int           // --read-percent
	blockSize      int           // --min-block-bytes, --max-block-bytes
	duration       time.Duration // --duration
	concurrency    func(int) int // --concurrency
	batch          int           // --batch
	maxOps         int           // --max-ops
	cpuTimeTokenAC bool
}

// annotatePhase adds a Grafana annotation marking a test phase transition.
// Errors are logged but not fatal since annotations are observability aids.
func annotatePhase(ctx context.Context, c cluster.Cluster, t test.Test, text string) {
	if err := c.AddGrafanaAnnotation(
		ctx, t.L(), rpgrafana.AddAnnotationRequest{Text: text},
	); err != nil {
		t.L().Printf("annotation error: %v", err)
	}
}

func runMultiTenantFairness(
	ctx context.Context, t test.Test, c cluster.Cluster, s multiTenantFairnessSpec,
) {
	crdbNode := c.Node(1)
	if c.IsLocal() {
		s.duration = 30 * time.Second
		s.concurrency = func(i int) int { return 4 }
		if s.maxOps > 10000 {
			s.maxOps = 10000
		}
		if s.batch > 10 {
			s.batch = 10
		}
		if s.blockSize > 2 {
			s.batch = 2
		}
	}

	t.L().Printf("starting cockroach (<%s)", time.Minute)
	c.Start(ctx, t.L(),
		option.NewStartOpts(option.NoBackupSchedule),
		install.MakeClusterSettings(),
		crdbNode,
	)

	promNode := c.Node(c.Spec().NodeCount)
	promCfg := &prometheus.Config{}
	promCfg.WithPrometheusNode(promNode.InstallNodes()[0])
	promCfg.WithNodeExporter(crdbNode.InstallNodes())
	promCfg.WithCluster(crdbNode.InstallNodes())
	promCfg.WithGrafanaDashboardJSON(grafana.MultiTenantFairnessGrafanaJSON)

	systemConn := c.Conn(ctx, t.L(), crdbNode[0])
	defer systemConn.Close()

	const rateLimit = 1_000_000

	if _, err := systemConn.ExecContext(
		ctx, fmt.Sprintf("SET CLUSTER SETTING kv.tenant_rate_limiter.rate_limit = '%d'", rateLimit),
	); err != nil {
		t.Fatalf("failed to set tenant rate limiter limit: %v", err)
	}

	t.L().Printf("enabling child metrics (<%s)", 30*time.Second)
	_, err := systemConn.ExecContext(ctx, `SET CLUSTER SETTING server.child_metrics.enabled = true`)
	require.NoError(t, err)

	virtualClusters := map[string]option.NodeListOption{
		"app-fairness-n2": c.Node(2),
		"app-fairness-n3": c.Node(3),
		"app-fairness-n4": c.Node(4),
		"app-fairness-n5": c.Node(5),
	}

	virtualClusterNames := maps.Keys(virtualClusters)
	sort.Strings(virtualClusterNames)

	t.L().Printf("initializing %d virtual clusters (<%s)", len(virtualClusters), 5*time.Minute)
	for j, name := range virtualClusterNames {
		node := virtualClusters[name]
		c.StartServiceForVirtualCluster(
			ctx, t.L(),
			option.StartVirtualClusterOpts(name, node, option.NoBackupSchedule),
			install.MakeClusterSettings(),
		)

		t.L().Printf("virtual cluster %q started on n%d", name, node[0])
		_, err := systemConn.ExecContext(
			ctx, fmt.Sprintf("SELECT crdb_internal.update_tenant_resource_limits('%s', 1000000000, 10000, 1000000)", name),
		)
		require.NoError(t, err)

		promCfg.WithTenantPod(node.InstallNodes()[0], j+1)
		promCfg.WithScrapeConfigs(
			prometheus.MakeWorkloadScrapeConfig(fmt.Sprintf("workload-tenant-%d", j+1),
				"/", makeWorkloadScrapeNodes(
					node.InstallNodes()[0],
					[]workloadInstance{
						{
							nodes:          node,
							prometheusPort: 2112,
						},
					})),
		)

		initKV := fmt.Sprintf(
			"%s workload init kv {pgurl:%d:%s}",
			test.DefaultCockroachPath, node[0], name,
		)

		c.Run(ctx, option.WithNodes(node), initKV)
	}

	if s.cpuTimeTokenAC {
		if _, err := systemConn.ExecContext(
			ctx, `SET CLUSTER SETTING admission.cpu_time_tokens.enabled = true`,
		); err != nil {
			t.Fatalf("failed to enable cpu time tokens: %v", err)
		}
	}

	t.L().Printf("loading per-tenant data (<%s)", 10*time.Minute)
	annotatePhase(ctx, c, t, "loading data")
	m1 := c.NewDeprecatedMonitor(ctx, c.All())
	for name, node := range virtualClusters {
		pgurl := fmt.Sprintf("{pgurl:%d:%s}", node[0], name)
		name := name
		node := node
		m1.Go(func(ctx context.Context) error {
			// TODO(irfansharif): Occasionally we see SQL liveness errors of the
			// following form. See #78691, #97448.
			//
			// 	ERROR: liveness session expired 571.043163ms before transaction
			//
			// Why do these errors occur? We started using high-pri for tenant
			// sql liveness work as of #98785, so this TODO might be stale. If
			// it persists, consider extending the default lease duration from
			// 40s to something higher, or retrying internally if the sql
			// session gets renewed shortly (within some jitter). We don't want
			// to --tolerate-errors here and below because we'd see total
			// throughput collapse.
			cmd := roachtestutil.NewCommand("%s workload run kv", test.DefaultCockroachPath).
				Option("secure").
				Flag("min-block-bytes", s.blockSize).
				Flag("max-block-bytes", s.blockSize).
				Flag("batch", s.batch).
				Flag("max-ops", s.maxOps).
				Flag("concurrency", 25).
				Arg("%s", pgurl)

			if err := c.RunE(ctx, option.WithNodes(node), cmd.String()); err != nil {
				return err
			}

			t.L().Printf("loaded data for virtual cluster %q", name)
			return nil
		})
	}
	m1.Wait()

	waitDur := 2 * time.Minute
	t.L().Printf("loaded data for all tenants, sleeping (<%s)", waitDur)
	annotatePhase(ctx, c, t, "data loaded")
	time.Sleep(waitDur)

	t.L().Printf("running virtual cluster workloads (<%s)", s.duration+time.Minute)
	annotatePhase(ctx, c, t, "running workloads")
	m2 := c.NewDeprecatedMonitor(ctx, crdbNode)
	var n int
	for name, node := range virtualClusters {
		pgurl := fmt.Sprintf("{pgurl:%d:%s}", node[0], name)
		n++

		name := name
		node := node
		m2.Go(func(ctx context.Context) error {
			cmd := roachtestutil.NewCommand("%s workload run kv", test.DefaultCockroachPath).
				Option("secure").
				Flag("write-seq", fmt.Sprintf("R%d", s.maxOps*s.batch)).
				Flag("min-block-bytes", s.blockSize).
				Flag("max-block-bytes", s.blockSize).
				Flag("batch", s.batch).
				Flag("duration", s.duration).
				Flag("read-percent", s.readPercent).
				Flag("concurrency", s.concurrency(n)).
				Arg("%s", pgurl)

			if err := c.RunE(ctx, option.WithNodes(node), cmd.String()); err != nil {
				return err
			}

			t.L().Printf("ran workload for virtual cluster %q", name)
			return nil
		})
	}
	m2.Wait()

	// Pull workload performance from crdb_internal.statement_statistics. We
	// could alternatively get these from the workload itself but this was
	// easier.
	//
	// TODO(irfansharif): Worth using clusterstats for this directly against a
	// prometheus instance pointed to each tenant's workload generator.
	// TODO(irfansharif): Make sure that count of failed queries is small/zero.
	// TODO(irfansharif): Aren't these stats getting polluted by the data-load
	// step?
	t.L().Printf("computing workload statistics (%s)", 30*time.Second)
	counts := make([]float64, len(virtualClusters))
	meanLatencies := make([]float64, len(virtualClusters))
	for j, name := range virtualClusterNames {
		node := virtualClusters[name]

		vcdb := c.Conn(ctx, t.L(), node[0], option.VirtualClusterName(name))

		_, err := vcdb.ExecContext(ctx, "USE kv")
		// Retry once, since this can fail sometimes due the cluster running hot.
		if err != nil {
			_, err = vcdb.ExecContext(ctx, "USE kv")
		}
		require.NoError(t, err)

		// TODO(aaditya): We no longer have the ability to filter for stats by
		// successful queries, and include ones for failed queries. Maybe consider
		// finding a way to do this?
		// See https://github.com/cockroachdb/cockroach/pull/121120.
		rows, err := vcdb.QueryContext(ctx, `
			SELECT
				sum((statistics -> 'statistics' -> 'cnt')::INT),
				avg((statistics -> 'statistics' -> 'runLat' -> 'mean')::FLOAT)
			FROM crdb_internal.statement_statistics
			WHERE metadata @> '{"db":"kv"}' AND metadata @> $1`,
			fmt.Sprintf(`{"querySummary": "%s"}`, s.query))
		require.NoError(t, err)

		if rows.Next() {
			var cnt, lat float64
			err := rows.Scan(&cnt, &lat)
			require.NoError(t, err)
			counts[j] = cnt
			meanLatencies[j] = lat
		} else {
			t.Fatal("no query results")
		}
		require.NoError(t, rows.Err())
		vcdb.Close()
	}

	failThreshold := .3
	throughput := make([]float64, len(virtualClusters))
	ok, maxThroughputDelta := floatsWithinPercentage(counts, failThreshold)
	for i, count := range counts {
		throughput[i] = count / s.duration.Seconds()
	}
	t.L().Printf("max-throughput-delta=%d%% average-throughput=%f total-ops-per-tenant=%v\n", int(maxThroughputDelta*100), averageFloat(throughput), counts)
	if !ok {
		// TODO(irfansharif): This is a weak assertion. Variation occurs when
		// there are workload differences during periods where AC is not
		// inducing any queuing. Remove?
		t.L().Printf("throughput not within expectations: %f > %f %v", maxThroughputDelta, failThreshold, throughput)
	}

	ok, maxLatencyDelta := floatsWithinPercentage(meanLatencies, failThreshold)
	t.L().Printf("max-latency-delta=%d%% mean-latency-per-tenant=%v\n", int(maxLatencyDelta*100), meanLatencies)
	if !ok {
		// TODO(irfansharif): Same as above -- this is a weak assertion.
		t.L().Printf("latency not within expectations: %f > %f %v", maxLatencyDelta, failThreshold, meanLatencies)
	}

	c.Run(ctx, option.WithNodes(crdbNode), "mkdir", "-p", t.PerfArtifactsDir())
	results := fmt.Sprintf(`{ "max_tput_delta": %f, "max_tput": %f, "min_tput": %f, "max_latency": %f, "min_latency": %f}`,
		maxThroughputDelta, maxFloat(throughput), minFloat(throughput), maxFloat(meanLatencies), minFloat(meanLatencies))
	c.Run(ctx, option.WithNodes(crdbNode), fmt.Sprintf(`echo '%s' > %s/stats.json`, results, t.PerfArtifactsDir()))
}

type noisyNeighborSpec struct {
	name             string
	query            string
	readPercent      int
	blockSize        int
	batch            int
	maxOps           int
	quietConcurrency int
	noisyConcurrency int

	// TPCC-specific fields (ignored for KV workload).
	workload        string // "kv" (default when empty) or "tpcc"
	quietWarehouses int
	noisyWarehouses int
}

func (s noisyNeighborSpec) isTPCC() bool {
	return s.workload == "tpcc"
}

// runNoisyNeighborFairness tests that CPU time token AC provides latency
// isolation for a "quiet" tenant when a "noisy" neighbor drives heavy load.
// It measures the quiet tenant's baseline latency in isolation, then measures
// it again while the noisy neighbor is active, and asserts that the degradation
// is bounded.
func runNoisyNeighborFairness(
	ctx context.Context, t test.Test, c cluster.Cluster, s noisyNeighborSpec,
) {
	crdbNode := c.Node(1)
	quietNode := c.Node(2)
	noisyNode := c.Node(3)

	t.L().Printf("starting cockroach")
	c.Start(ctx, t.L(),
		option.NewStartOpts(option.NoBackupSchedule),
		install.MakeClusterSettings(),
		crdbNode,
	)

	systemConn := c.Conn(ctx, t.L(), crdbNode[0])
	defer systemConn.Close()

	// Set effectively unlimited rate limiter so CPU time tokens are the
	// binding constraint.
	if _, err := systemConn.ExecContext(
		ctx, `SET CLUSTER SETTING kv.tenant_rate_limiter.rate_limit = '1000000'`,
	); err != nil {
		t.Fatalf("failed to set tenant rate limiter limit: %v", err)
	}
	if _, err := systemConn.ExecContext(
		ctx, `SET CLUSTER SETTING admission.cpu_time_tokens.enabled = true`,
	); err != nil {
		t.Fatalf("failed to enable cpu time tokens: %v", err)
	}
	if _, err := systemConn.ExecContext(
		ctx, `SET CLUSTER SETTING server.child_metrics.enabled = true`,
	); err != nil {
		t.Fatalf("failed to enable child metrics: %v", err)
	}

	// Start virtual clusters.
	quietName := "app-quiet"
	noisyName := "app-noisy"
	for _, vc := range []struct {
		name       string
		node       option.NodeListOption
		warehouses int // TPCC only
	}{
		{quietName, quietNode, s.quietWarehouses},
		{noisyName, noisyNode, s.noisyWarehouses},
	} {
		c.StartServiceForVirtualCluster(
			ctx, t.L(),
			option.StartVirtualClusterOpts(vc.name, vc.node, option.NoBackupSchedule),
			install.MakeClusterSettings(),
		)
		t.L().Printf("virtual cluster %q started on n%d", vc.name, vc.node[0])

		// Set generous resource limits.
		_, err := systemConn.ExecContext(
			ctx, fmt.Sprintf(
				"SELECT crdb_internal.update_tenant_resource_limits('%s', 1000000000, 10000, 1000000)",
				vc.name,
			),
		)
		require.NoError(t, err)

		// Initialize workload data.
		if s.isTPCC() {
			initCmd := fmt.Sprintf(
				"%s workload init tpcc --warehouses=%d {pgurl:%d:%s}",
				test.DefaultCockroachPath, vc.warehouses, vc.node[0], vc.name,
			)
			c.Run(ctx, option.WithNodes(vc.node), initCmd)
		} else {
			initCmd := fmt.Sprintf(
				"%s workload init kv {pgurl:%d:%s}",
				test.DefaultCockroachPath, vc.node[0], vc.name,
			)
			c.Run(ctx, option.WithNodes(vc.node), initCmd)
		}
	}

	// Load initial data for both tenants (TPCC init already loads data).
	annotatePhase(ctx, c, t, "loading data")
	if !s.isTPCC() {
		t.L().Printf("loading data for both tenants")
		m1 := c.NewDeprecatedMonitor(ctx, c.All())
		for _, vc := range []struct {
			name string
			node option.NodeListOption
		}{
			{quietName, quietNode},
			{noisyName, noisyNode},
		} {
			pgurl := fmt.Sprintf("{pgurl:%d:%s}", vc.node[0], vc.name)
			node := vc.node
			m1.Go(func(ctx context.Context) error {
				cmd := roachtestutil.NewCommand("%s workload run kv", test.DefaultCockroachPath).
					Option("secure").
					Flag("min-block-bytes", s.blockSize).
					Flag("max-block-bytes", s.blockSize).
					Flag("batch", s.batch).
					Flag("max-ops", s.maxOps).
					Flag("concurrency", 25).
					Arg("%s", pgurl)
				return c.RunE(ctx, option.WithNodes(node), cmd.String())
			})
		}
		m1.Wait()
	}

	waitDur := 2 * time.Minute
	t.L().Printf("loaded data, sleeping %s", waitDur)
	annotatePhase(ctx, c, t, "data loaded")
	time.Sleep(waitDur)

	// Reset SQL stats on quiet tenant before baseline measurement.
	quietConn := c.Conn(ctx, t.L(), quietNode[0], option.VirtualClusterName(quietName))
	defer quietConn.Close()
	_, err := quietConn.ExecContext(ctx, "SELECT crdb_internal.reset_sql_stats()")
	require.NoError(t, err)

	// Phase 1: run only the quiet tenant workload to establish a baseline.
	phase1Duration := 5 * time.Minute
	t.L().Printf("phase 1: running quiet tenant only (%s)", phase1Duration)
	annotatePhase(ctx, c, t, "phase 1: quiet tenant only")
	quietPgurl := fmt.Sprintf("{pgurl:%d:%s}", quietNode[0], quietName)
	if s.isTPCC() {
		quietCmd := roachtestutil.NewCommand("%s workload run tpcc", test.DefaultCockroachPath).
			Option("secure").
			Flag("warehouses", s.quietWarehouses).
			Flag("duration", phase1Duration).
			Arg("%s", quietPgurl)
		c.Run(ctx, option.WithNodes(quietNode), quietCmd.String())
	} else {
		quietCmd := roachtestutil.NewCommand("%s workload run kv", test.DefaultCockroachPath).
			Option("secure").
			Flag("write-seq", fmt.Sprintf("R%d", s.maxOps*s.batch)).
			Flag("min-block-bytes", s.blockSize).
			Flag("max-block-bytes", s.blockSize).
			Flag("batch", s.batch).
			Flag("duration", phase1Duration).
			Flag("read-percent", s.readPercent).
			Flag("concurrency", s.quietConcurrency).
			Arg("%s", quietPgurl)
		c.Run(ctx, option.WithNodes(quietNode), quietCmd.String())
	}

	// Collect baseline latency from statement statistics.
	dbName := "kv"
	if s.isTPCC() {
		dbName = "tpcc"
	}
	_, err = quietConn.ExecContext(ctx, "USE "+dbName)
	require.NoError(t, err)
	var baselineLatency float64
	if s.isTPCC() {
		err = quietConn.QueryRowContext(ctx, `
			SELECT avg((statistics -> 'statistics' -> 'runLat' -> 'mean')::FLOAT)
			FROM crdb_internal.statement_statistics
			WHERE metadata @> '{"db":"tpcc"}'`,
		).Scan(&baselineLatency)
	} else {
		err = quietConn.QueryRowContext(ctx, `
			SELECT avg((statistics -> 'statistics' -> 'runLat' -> 'mean')::FLOAT)
			FROM crdb_internal.statement_statistics
			WHERE metadata @> '{"db":"kv"}' AND metadata @> $1`,
			fmt.Sprintf(`{"querySummary": "%s"}`, s.query),
		).Scan(&baselineLatency)
	}
	require.NoError(t, err)
	t.L().Printf("phase 1 baseline latency: %f", baselineLatency)

	// Reset SQL stats on quiet tenant before phase 2.
	_, err = quietConn.ExecContext(ctx, "SELECT crdb_internal.reset_sql_stats()")
	require.NoError(t, err)

	// Phase 2: run both tenants concurrently.
	phase2Duration := 15 * time.Minute
	t.L().Printf("phase 2: running both tenants (%s)", phase2Duration)
	annotatePhase(ctx, c, t, "phase 2: both tenants")
	workloadStart := timeutil.Now()
	m2 := c.NewDeprecatedMonitor(ctx, crdbNode)

	// Quiet tenant workload.
	m2.Go(func(ctx context.Context) error {
		var cmd *roachtestutil.Command
		if s.isTPCC() {
			cmd = roachtestutil.NewCommand("%s workload run tpcc", test.DefaultCockroachPath).
				Option("secure").
				Flag("warehouses", s.quietWarehouses).
				Flag("duration", phase2Duration).
				Arg("%s", quietPgurl)
		} else {
			cmd = roachtestutil.NewCommand("%s workload run kv", test.DefaultCockroachPath).
				Option("secure").
				Flag("write-seq", fmt.Sprintf("R%d", s.maxOps*s.batch)).
				Flag("min-block-bytes", s.blockSize).
				Flag("max-block-bytes", s.blockSize).
				Flag("batch", s.batch).
				Flag("duration", phase2Duration).
				Flag("read-percent", s.readPercent).
				Flag("concurrency", s.quietConcurrency).
				Arg("%s", quietPgurl)
		}
		return c.RunE(ctx, option.WithNodes(quietNode), cmd.String())
	})

	// Noisy tenant workload (tolerate errors from AC throttling).
	noisyPgurl := fmt.Sprintf("{pgurl:%d:%s}", noisyNode[0], noisyName)
	m2.Go(func(ctx context.Context) error {
		var cmd *roachtestutil.Command
		if s.isTPCC() {
			cmd = roachtestutil.NewCommand("%s workload run tpcc", test.DefaultCockroachPath).
				Option("secure").
				Option("tolerate-errors").
				Flag("warehouses", s.noisyWarehouses).
				Flag("wait", 0).
				Flag("duration", phase2Duration).
				Arg("%s", noisyPgurl)
		} else {
			cmd = roachtestutil.NewCommand("%s workload run kv", test.DefaultCockroachPath).
				Option("secure").
				Option("tolerate-errors").
				Flag("write-seq", fmt.Sprintf("R%d", s.maxOps*s.batch)).
				Flag("min-block-bytes", s.blockSize).
				Flag("max-block-bytes", s.blockSize).
				Flag("batch", s.batch).
				Flag("duration", phase2Duration).
				Flag("read-percent", s.readPercent).
				Flag("concurrency", s.noisyConcurrency).
				Arg("%s", noisyPgurl)
		}
		return c.RunE(ctx, option.WithNodes(noisyNode), cmd.String())
	})

	m2.Wait()
	workloadEnd := timeutil.Now()
	annotatePhase(ctx, c, t, "workload complete")

	// Verify CPU utilization is near the target.
	verifyCPUUtilization(
		ctx, c, t, crdbNode, workloadStart, workloadEnd,
		0.8 /* targetCPU */, 0.15 /* tolerance */, nil, /* sources */
	)

	// Collect phase 2 latency from statement statistics.
	var phase2Latency float64
	if s.isTPCC() {
		err = quietConn.QueryRowContext(ctx, `
			SELECT avg((statistics -> 'statistics' -> 'runLat' -> 'mean')::FLOAT)
			FROM crdb_internal.statement_statistics
			WHERE metadata @> '{"db":"tpcc"}'`,
		).Scan(&phase2Latency)
	} else {
		err = quietConn.QueryRowContext(ctx, `
			SELECT avg((statistics -> 'statistics' -> 'runLat' -> 'mean')::FLOAT)
			FROM crdb_internal.statement_statistics
			WHERE metadata @> '{"db":"kv"}' AND metadata @> $1`,
			fmt.Sprintf(`{"querySummary": "%s"}`, s.query),
		).Scan(&phase2Latency)
	}
	require.NoError(t, err)
	t.L().Printf("phase 2 latency: %f (baseline: %f, ratio: %.2f)",
		phase2Latency, baselineLatency, phase2Latency/baselineLatency)

	// Assert latency isolation: the quiet tenant's latency under noisy
	// neighbor load should not exceed 2x the baseline.
	if ratio := phase2Latency / baselineLatency; ratio >= 2.0 {
		t.Fatalf(
			"quiet tenant latency degraded %.2fx (from %f to %f), expected < 2.0x",
			ratio, baselineLatency, phase2Latency,
		)
	}
}

func averageFloat(values []float64) float64 {
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func minFloat(values []float64) float64 {
	min := values[0]
	for _, v := range values {
		min = math.Min(v, min)
	}
	return min
}

func maxFloat(values []float64) float64 {
	max := values[0]
	for _, v := range values {
		max = math.Max(v, max)
	}
	return max
}

func floatsWithinPercentage(values []float64, percent float64) (bool, float64) {
	avg := averageFloat(values)
	limit := avg * percent
	maxDelta := 0.0
	for _, v := range values {
		delta := math.Abs(avg - v)
		if delta > limit {
			return false, 1.0 - (avg-delta)/avg
		}
		if delta > maxDelta {
			maxDelta = delta
		}
	}
	maxDelta = 1.0 - (avg-maxDelta)/avg // make a percentage
	return true, maxDelta
}
