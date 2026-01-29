// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { AxisUnits } from "@cockroachlabs/cluster-ui";
import React from "react";

import LineGraph from "src/views/cluster/components/linegraph";
import { Metric, Axis } from "src/views/shared/components/metricQuery";

import { GraphDashboardProps, nodeDisplayName } from "./dashboardUtils";

export default function (props: GraphDashboardProps) {
  const { nodeIDs, nodeSources, nodeDisplayNameByID, tenantSource } = props;

  return [
    <LineGraph
      title="CPU Time Token Multiplier"
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
      tooltip={`The token-to-CPU-time multiplier used in CPU time token admission control. A value greater than 1 indicates that tracked CPU time underestimates actual CPU usage.`}
    >
      <Axis label="Multiplier">
        {nodeIDs.map(nid => (
          <Metric
            key={nid}
            name="cr.node.admission.cpu_time_tokens.multiplier"
            title={nodeDisplayName(nodeDisplayNameByID, nid)}
            sources={[nid]}
          />
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Bucket Tokens – Tier 0 (System Tenant)"
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
      tooltip={`Current token count in tier 0 (system tenant) buckets. Positive values indicate available capacity; negative values indicate the bucket is exhausted and requests must wait.`}
    >
      <Axis label="Tokens">
        {nodeIDs.map(nid => (
          <>
            <Metric
              key={nid + "-burst"}
              name="cr.node.admission.cpu_time_tokens.bucket_tokens.tier0.canBurst"
              title={"Burstable " + nodeDisplayName(nodeDisplayNameByID, nid)}
              sources={[nid]}
            />
            <Metric
              key={nid + "-noburst"}
              name="cr.node.admission.cpu_time_tokens.bucket_tokens.tier0.noBurst"
              title={"Non-Burstable " + nodeDisplayName(nodeDisplayNameByID, nid)}
              sources={[nid]}
            />
          </>
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Bucket Tokens – Tier 1 (App Tenant)"
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
      tooltip={`Current token count in tier 1 (app tenant) buckets. Positive values indicate available capacity; negative values indicate the bucket is exhausted and requests must wait.`}
    >
      <Axis label="Tokens">
        {nodeIDs.map(nid => (
          <>
            <Metric
              key={nid + "-burst"}
              name="cr.node.admission.cpu_time_tokens.bucket_tokens.tier1.canBurst"
              title={"Burstable " + nodeDisplayName(nodeDisplayNameByID, nid)}
              sources={[nid]}
            />
            <Metric
              key={nid + "-noburst"}
              name="cr.node.admission.cpu_time_tokens.bucket_tokens.tier1.noBurst"
              title={"Non-Burstable " + nodeDisplayName(nodeDisplayNameByID, nid)}
              sources={[nid]}
            />
          </>
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Token Usage Rate – Tier 0 (System Tenant)"
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
      tooltip={`Rate of token consumption from tier 0 (system tenant) buckets. This shows the CPU time being consumed by system tenant work.`}
    >
      <Axis units={AxisUnits.Duration} label="Tokens/sec">
        {nodeIDs.map(nid => (
          <>
            <Metric
              key={nid + "-burst"}
              name="cr.node.admission.cpu_time_tokens.tokens_used.tier0.canBurst"
              title={"Burstable " + nodeDisplayName(nodeDisplayNameByID, nid)}
              sources={[nid]}
              nonNegativeRate
            />
            <Metric
              key={nid + "-noburst"}
              name="cr.node.admission.cpu_time_tokens.tokens_used.tier0.noBurst"
              title={"Non-Burstable " + nodeDisplayName(nodeDisplayNameByID, nid)}
              sources={[nid]}
              nonNegativeRate
            />
          </>
        ))}
      </Axis>
    </LineGraph>,

    <LineGraph
      title="Token Usage Rate – Tier 1 (App Tenant)"
      sources={nodeSources}
      tenantSource={tenantSource}
      showMetricsInTooltip={true}
      tooltip={`Rate of token consumption from tier 1 (app tenant) buckets. This shows the CPU time being consumed by app tenant work.`}
    >
      <Axis units={AxisUnits.Duration} label="Tokens/sec">
        {nodeIDs.map(nid => (
          <>
            <Metric
              key={nid + "-burst"}
              name="cr.node.admission.cpu_time_tokens.tokens_used.tier1.canBurst"
              title={"Burstable " + nodeDisplayName(nodeDisplayNameByID, nid)}
              sources={[nid]}
              nonNegativeRate
            />
            <Metric
              key={nid + "-noburst"}
              name="cr.node.admission.cpu_time_tokens.tokens_used.tier1.noBurst"
              title={"Non-Burstable " + nodeDisplayName(nodeDisplayNameByID, nid)}
              sources={[nid]}
              nonNegativeRate
            />
          </>
        ))}
      </Axis>
    </LineGraph>,
  ];
}
