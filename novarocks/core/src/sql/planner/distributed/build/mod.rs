#![allow(dead_code)]
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

mod fragment_cut;
mod lowering;
mod runtime_filter_binding;

use crate::sql::planner::distributed::DistributedPlan;
use crate::sql::planner::distributed::fragment::DistributedPlanDraft;
use crate::sql::planner::physical::PhysicalPlanNode;

pub(crate) fn build_distributed_plan(plan: &PhysicalPlanNode) -> Result<DistributedPlan, String> {
    let draft = build_distributed_plan_draft(plan)?;
    super::seal::seal_draft(draft).map_err(|error| error.to_string())
}

/// Build a normal distributed plan whose root has an internal statistics sink.
/// The fragment graph, placement and exchange shape remain exactly those of a
/// regular query; only the terminal capability changes before sealing. This is
/// intentionally not a post-seal mutation, so topology/output contracts see
/// the statistics terminal from the start.
pub(crate) fn build_statistics_distributed_plan(
    plan: &PhysicalPlanNode,
    metrics: novarocks_spi::connector::StatisticsMetricRequest,
) -> Result<DistributedPlan, String> {
    let mut draft = build_distributed_plan_draft(plan)?;
    let root_id = draft
        .root_fragment_id
        .ok_or_else(|| "statistics distributed plan is missing a root fragment".to_string())?;
    let root = draft
        .fragments
        .iter_mut()
        .find(|fragment| fragment.fragment_id == root_id)
        .ok_or_else(|| format!("statistics distributed plan root fragment {root_id} is absent"))?;
    root.sink = super::fragment::DataSink::Statistics(metrics);
    super::seal::seal_draft(draft).map_err(|error| error.to_string())
}

pub(in crate::sql::planner::distributed) fn build_distributed_plan_draft(
    plan: &PhysicalPlanNode,
) -> Result<DistributedPlanDraft, String> {
    let mut cut = fragment_cut::cut(plan)?;
    runtime_filter_binding::populate_runtime_filter_graph(
        &mut cut.plan.fragments,
        &mut cut.plan.runtime_filter_graph,
        &cut.bindings,
    )?;
    Ok(cut.plan)
}

pub(crate) fn union_distinct_must_be_rewritten_error() -> &'static str {
    "UNION DISTINCT must be rewritten by UnionDistinctToAggregate before distributed build"
}

#[cfg(test)]
mod tests;
