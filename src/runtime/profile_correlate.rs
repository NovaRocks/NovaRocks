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

use std::collections::HashMap;

use crate::runtime::profile::Profiler;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ActualMetrics {
    pub(crate) output_rows: i64,
    pub(crate) total_time_ns: i64,
    pub(crate) peak_mem_bytes: i64,
}

pub(crate) fn collect_actuals_by_plan_node_id(profiler: &Profiler) -> HashMap<i32, ActualMetrics> {
    collect_actuals_by_plan_node_id_multi(std::slice::from_ref(profiler))
}

pub(crate) fn collect_actuals_by_plan_node_id_multi(
    profilers: &[Profiler],
) -> HashMap<i32, ActualMetrics> {
    let mut actuals = HashMap::new();
    for profiler in profilers {
        let merged = crate::service::fe_report::merge_pipeline_profiles_for_fe(profiler);
        collect_rec(&merged, &mut actuals);
    }
    actuals
}

fn collect_rec(node: &Profiler, actuals: &mut HashMap<i32, ActualMetrics>) {
    if let Some(node_id) = parse_plan_node_id(&node.name()) {
        if let Some(common) = node.get_child("CommonMetrics") {
            let metrics = actuals.entry(node_id).or_default();
            metrics.output_rows = metrics.output_rows.max(counter(&common, "PullRowNum"));
            metrics.total_time_ns = metrics
                .total_time_ns
                .max(counter(&common, "OperatorTotalTime"));
            metrics.peak_mem_bytes = metrics
                .peak_mem_bytes
                .max(counter(&common, "OperatorPeakMemoryUsage"));
        }
    }

    for child in node.children() {
        collect_rec(&child, actuals);
    }
}

fn counter(common: &Profiler, name: &str) -> i64 {
    common.counter_value(name).unwrap_or(0)
}

fn parse_plan_node_id(name: &str) -> Option<i32> {
    let key = if name.contains("plan_node_id=") {
        "plan_node_id="
    } else {
        "(id="
    };
    let start = name.find(key)? + key.len();
    let rest = &name[start..];
    let end = rest
        .find(|c: char| !c.is_ascii_digit() && c != '-')
        .unwrap_or(rest.len());
    rest[..end].parse().ok()
}

#[cfg(test)]
mod tests {
    use super::{
        ActualMetrics, collect_actuals_by_plan_node_id, collect_actuals_by_plan_node_id_multi,
    };
    use crate::metrics;
    use crate::runtime::profile::Profiler;

    fn add_operator_metrics(
        parent: &Profiler,
        name: &str,
        output_rows: i64,
        total_time_ns: i64,
        peak_mem_bytes: i64,
    ) {
        let common = parent.child(name).child("CommonMetrics");
        common.counter_set("PullRowNum", metrics::TUnit::UNIT, output_rows);
        common.counter_set("OperatorTotalTime", metrics::TUnit::TIME_NS, total_time_ns);
        common.counter_set(
            "OperatorPeakMemoryUsage",
            metrics::TUnit::BYTES,
            peak_mem_bytes,
        );
    }

    #[test]
    fn collects_single_operator_profile() {
        let profiler = Profiler::new("query");
        let driver = profiler
            .child("Pipeline (id=0)")
            .child("PipelineDriver (id=0)");
        add_operator_metrics(&driver, "SCAN (plan_node_id=2)", 10, 5, 64);

        let actuals = collect_actuals_by_plan_node_id(&profiler);

        assert_eq!(
            actuals.get(&2).copied(),
            Some(ActualMetrics {
                output_rows: 10,
                total_time_ns: 5,
                peak_mem_bytes: 64,
            })
        );
    }

    #[test]
    fn merges_driver_instances_before_collecting() {
        let profiler = Profiler::new("query");
        let pipeline = profiler.child("Pipeline (id=0)");
        let driver0 = pipeline.child("PipelineDriver (id=0)");
        add_operator_metrics(&driver0, "SCAN (plan_node_id=2)", 10, 5, 64);
        let driver1 = pipeline.child("PipelineDriver (id=1)");
        add_operator_metrics(&driver1, "SCAN (plan_node_id=2)", 20, 5, 32);

        let actuals = collect_actuals_by_plan_node_id(&profiler);

        assert_eq!(
            actuals.get(&2).copied(),
            Some(ActualMetrics {
                output_rows: 30,
                total_time_ns: 5,
                peak_mem_bytes: 96,
            })
        );
    }

    #[test]
    fn collects_runtime_operator_id_names_before_fe_normalization() {
        let profiler = Profiler::new("query");
        let driver = profiler
            .child("Pipeline (id=0)")
            .child("PipelineDriver (id=0)");
        add_operator_metrics(&driver, "AGGREGATE (id=3)", 1, 12_000, 128);

        let actuals = collect_actuals_by_plan_node_id(&profiler);

        assert_eq!(
            actuals.get(&3).copied(),
            Some(ActualMetrics {
                output_rows: 1,
                total_time_ns: 12_000,
                peak_mem_bytes: 128,
            })
        );
    }

    #[test]
    fn merges_profiles_across_fragments() {
        let scan_fragment = Profiler::new("fragment-0");
        let scan_driver = scan_fragment
            .child("Pipeline (id=0)")
            .child("PipelineDriver (id=0)");
        add_operator_metrics(&scan_driver, "SCAN (plan_node_id=2)", 100, 20, 256);

        let agg_fragment = Profiler::new("fragment-1");
        let agg_driver = agg_fragment
            .child("Pipeline (id=0)")
            .child("PipelineDriver (id=0)");
        add_operator_metrics(&agg_driver, "AGGREGATE (plan_node_id=5)", 1, 30, 512);

        let actuals = collect_actuals_by_plan_node_id_multi(&[scan_fragment, agg_fragment]);

        assert_eq!(
            actuals.get(&2).copied(),
            Some(ActualMetrics {
                output_rows: 100,
                total_time_ns: 20,
                peak_mem_bytes: 256,
            })
        );
        assert_eq!(
            actuals.get(&5).copied(),
            Some(ActualMetrics {
                output_rows: 1,
                total_time_ns: 30,
                peak_mem_bytes: 512,
            })
        );
    }
}
