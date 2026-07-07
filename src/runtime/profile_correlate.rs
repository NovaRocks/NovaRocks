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

use crate::runtime::profile::{ProfileNode, Profiler, RuntimeProfileTree};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ActualMetrics {
    pub(crate) output_rows: i64,
    pub(crate) total_time_ns: i64,
    pub(crate) peak_mem_bytes: i64,
    pub(crate) total_time_max_ns: i64,
    pub(crate) total_time_min_ns: i64,
    pub(crate) build_ht_ns: i64,
    pub(crate) search_ns: i64,
    pub(crate) out_build_ns: i64,
    pub(crate) out_probe_ns: i64,
    pub(crate) dict_input_rows: i64,
    pub(crate) dict_input_columns: i64,
    pub(crate) dict_kept_rows: i64,
    pub(crate) dict_kept_columns: i64,
    pub(crate) dict_hydrated_rows: i64,
    pub(crate) dict_hydrated_columns: i64,
    pub(crate) dict_unsupported_columns: i64,
}

const COMMON_METRICS: &str = "CommonMetrics";
const UNIQUE_METRICS: &str = "UniqueMetrics";
const DICT_INPUT_ROWS: &str = "DictInputRows";
const DICT_INPUT_COLUMNS: &str = "DictInputColumns";
const DICT_KEPT_ROWS: &str = "DictKeptRows";
const DICT_KEPT_COLUMNS: &str = "DictKeptColumns";
const DICT_HYDRATED_ROWS: &str = "DictHydratedRows";
const DICT_HYDRATED_COLUMNS: &str = "DictHydratedColumns";
const DICT_UNSUPPORTED_COLUMNS: &str = "DictUnsupportedColumns";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct DistributedProfileSummary {
    pub(crate) fragment_instance_count: usize,
    pub(crate) fragment_wall_max_ns: i64,
    pub(crate) fragment_wall_sum_ns: i64,
    pub(crate) driver_total_time_ns: i64,
    pub(crate) operator_active_time_ns: i64,
    pub(crate) driver_blocked_time_ns: i64,
    pub(crate) source_wait_time_ns: i64,
    pub(crate) sink_wait_time_ns: i64,
    pub(crate) dependency_wait_time_ns: i64,
    pub(crate) exchange_wait_time_ns: i64,
    pub(crate) exchange_process_time_ns: i64,
    pub(crate) network_time_ns: i64,
    pub(crate) scan_io_time_ns: i64,
}

pub(crate) fn collect_actuals_by_plan_node_id(profiler: &Profiler) -> HashMap<i32, ActualMetrics> {
    collect_actuals_by_plan_node_id_multi(std::slice::from_ref(profiler))
}

pub(crate) fn collect_actuals_by_plan_node_id_multi(
    profilers: &[Profiler],
) -> HashMap<i32, ActualMetrics> {
    let trees = profilers
        .iter()
        .map(|profiler| crate::service::fe_report::merge_pipeline_profiles_for_fe(profiler))
        .map(|profiler| profiler.to_native_tree())
        .collect::<Vec<_>>();
    collect_actuals_by_plan_node_id_from_profile_trees(&trees)
}

pub(crate) fn collect_actuals_by_plan_node_id_from_profile_trees(
    trees: &[RuntimeProfileTree],
) -> HashMap<i32, ActualMetrics> {
    let mut actuals = HashMap::new();
    for tree in trees {
        collect_native_tree_rec(&tree.root, &mut actuals);
    }
    actuals
}

pub(crate) fn collect_distributed_profile_summary_from_profile_trees(
    trees: &[RuntimeProfileTree],
) -> DistributedProfileSummary {
    let mut summary = DistributedProfileSummary::default();
    for tree in trees {
        merge_summary(&mut summary, &summarize_one_tree(tree));
    }
    summary
}

pub(crate) fn sum_profile_counters_by_name_from_profile_trees<'a>(
    trees: &[RuntimeProfileTree],
    names: &[&'a str],
) -> HashMap<&'a str, i64> {
    let mut sums = names
        .iter()
        .copied()
        .map(|name| (name, 0_i64))
        .collect::<HashMap<_, _>>();
    for tree in trees {
        sum_profile_counters_by_name_rec(&tree.root, &mut sums);
    }
    sums
}

fn sum_profile_counters_by_name_rec<'a>(node: &ProfileNode, sums: &mut HashMap<&'a str, i64>) {
    for counter in &node.counters {
        if let Some(total) = sums.get_mut(counter.name.as_str()) {
            *total = total.saturating_add(counter.value);
        }
    }
    for child in &node.children {
        sum_profile_counters_by_name_rec(child, sums);
    }
}

pub(crate) fn format_counter_sums_from_profile_trees(
    trees: &[RuntimeProfileTree],
    names: &[&str],
    label: &str,
) -> Option<String> {
    let sums = sum_profile_counters_by_name_from_profile_trees(trees, names);
    if !sums.values().any(|value| *value != 0) {
        return None;
    }
    let parts = names
        .iter()
        .map(|name| format!("{name}={}", sums.get(name).copied().unwrap_or(0)))
        .collect::<Vec<_>>();
    Some(format!("{label}: {}", parts.join(" ")))
}

/// Per-fragment attribution (W0'b): group each fragment-instance profile tree by the fragment's
/// root (output) plan-node id (see `fragment_root_plan_node_id`), merging instances of the same
/// fragment. The renderer maps each `PLAN FRAGMENT` to the same id via `fragment.root.node_id` and
/// prints the matching summary. Reuses `summarize_one_tree` so the math matches the query-level
/// summary exactly.
pub(crate) fn collect_per_fragment_profile_summaries(
    trees: &[RuntimeProfileTree],
) -> HashMap<i32, DistributedProfileSummary> {
    let mut by_fragment: HashMap<i32, DistributedProfileSummary> = HashMap::new();
    for tree in trees {
        let Some(fragment_key) = fragment_root_plan_node_id(tree) else {
            continue;
        };
        let one = summarize_one_tree(tree);
        merge_summary(by_fragment.entry(fragment_key).or_default(), &one);
    }
    by_fragment
}

/// The fragment's root (output) plan-node id — the unambiguous per-fragment key. It is encoded in
/// the fragment profiler's root node name `execute_fragment (plan_node_id=N)` (see
/// `src/lower/compat/fragment.rs`), where `N = fragment.plan.nodes.first().node_id`, and it equals the
/// `DistributedPlan` `fragment.root.node_id` the renderer keys by. This is unique per fragment and
/// is never a cross-fragment-shared exchange node id (the root is the fragment's output operator),
/// so it avoids the collision that a min-over-nodes representative hits on shared exchange ids.
/// Falls back to the smallest operator id only if the tree root carries no plan-node id (e.g. some
/// synthetic test trees) — real fragment trees always have the `execute_fragment` root.
fn fragment_root_plan_node_id(tree: &RuntimeProfileTree) -> Option<i32> {
    if let Some(id) = parse_plan_node_id(&tree.root.name) {
        return Some(id);
    }
    let mut operators: HashMap<i32, ActualMetrics> = HashMap::new();
    collect_native_tree_rec(&tree.root, &mut operators);
    operators.keys().copied().min()
}

/// Summarize one fragment-instance profile tree into a single-instance summary.
fn summarize_one_tree(tree: &RuntimeProfileTree) -> DistributedProfileSummary {
    let fragment_wall_ns = native_counter_in_tree(tree, "FragmentWallTime");
    DistributedProfileSummary {
        fragment_instance_count: 1,
        fragment_wall_max_ns: fragment_wall_ns,
        fragment_wall_sum_ns: fragment_wall_ns,
        driver_total_time_ns: native_counter_in_tree(tree, "DriverTotalTime"),
        operator_active_time_ns: native_counter_in_tree(tree, "OperatorTotalTime"),
        driver_blocked_time_ns: native_counter_in_tree(tree, "DriverBlockedTime"),
        source_wait_time_ns: native_counter_in_tree(tree, "DriverInputEmptyTime"),
        sink_wait_time_ns: native_counter_in_tree(tree, "DriverOutputFullTime"),
        dependency_wait_time_ns: native_counter_in_tree(tree, "DriverDependencyWaitTime"),
        exchange_wait_time_ns: native_counter_in_tree(tree, "WaitTime"),
        exchange_process_time_ns: native_counter_in_tree(tree, "ReceiverProcessTotalTime"),
        network_time_ns: native_counter_in_tree(tree, "NetworkTime"),
        scan_io_time_ns: native_counter_in_tree(tree, "IOTaskExecTime"),
    }
}

fn collect_native_tree_rec(node: &ProfileNode, actuals: &mut HashMap<i32, ActualMetrics>) {
    if let Some(node_id) = parse_plan_node_id(&node.name) {
        let common = node
            .children
            .iter()
            .find(|child| child.name == COMMON_METRICS);
        let unique = node
            .children
            .iter()
            .find(|child| child.name == UNIQUE_METRICS);
        if let Some(common) = common {
            let (total_time_ns, total_time_min_ns, total_time_max_ns) =
                native_counter_value_min_max(common, "OperatorTotalTime");
            merge_actual_metrics(
                actuals,
                node_id,
                ActualMetrics {
                    output_rows: native_counter(common, "PullRowNum"),
                    total_time_ns,
                    peak_mem_bytes: native_counter(common, "OperatorPeakMemoryUsage"),
                    total_time_max_ns,
                    total_time_min_ns,
                    build_ht_ns: native_counter(common, "BuildHashTableTime"),
                    search_ns: native_counter(common, "SearchHashTableTime"),
                    out_build_ns: native_counter(common, "OutputBuildColumnTime"),
                    out_probe_ns: native_counter(common, "OutputProbeColumnTime"),
                    dict_input_rows: native_optional_counter(unique, DICT_INPUT_ROWS),
                    dict_input_columns: native_optional_counter(unique, DICT_INPUT_COLUMNS),
                    dict_kept_rows: native_optional_counter(unique, DICT_KEPT_ROWS),
                    dict_kept_columns: native_optional_counter(unique, DICT_KEPT_COLUMNS),
                    dict_hydrated_rows: native_optional_counter(unique, DICT_HYDRATED_ROWS),
                    dict_hydrated_columns: native_optional_counter(unique, DICT_HYDRATED_COLUMNS),
                    dict_unsupported_columns: native_optional_counter(
                        unique,
                        DICT_UNSUPPORTED_COLUMNS,
                    ),
                },
            );
        }
    }

    for child in &node.children {
        collect_native_tree_rec(child, actuals);
    }
}

fn native_counter(node: &ProfileNode, name: &str) -> i64 {
    node.counters
        .iter()
        .filter(|counter| counter.name == name)
        .map(|counter| counter.value)
        .fold(0_i64, i64::saturating_add)
}

fn native_optional_counter(node: Option<&ProfileNode>, name: &str) -> i64 {
    node.map_or(0, |node| native_counter(node, name))
}

fn native_counter_value_min_max(node: &ProfileNode, name: &str) -> (i64, i64, i64) {
    let mut value = 0_i64;
    let mut max_of_max = None;
    let mut min_of_min = None;
    for counter in node.counters.iter().filter(|counter| counter.name == name) {
        value = value.saturating_add(counter.value);
        if let Some(max) = counter.max_value {
            max_of_max = Some(max_of_max.map_or(max, |current: i64| current.max(max)));
        }
        if let Some(min) = counter.min_value {
            min_of_min = Some(min_of_min.map_or(min, |current: i64| current.min(min)));
        }
    }
    (
        value,
        min_of_min.unwrap_or(value),
        max_of_max.unwrap_or(value),
    )
}

fn native_counter_in_tree(tree: &RuntimeProfileTree, name: &str) -> i64 {
    native_counter_in_node(&tree.root, name)
}

fn native_counter_in_node(node: &ProfileNode, name: &str) -> i64 {
    let local = native_counter(node, name);
    node.children
        .iter()
        .map(|child| native_counter_in_node(child, name))
        .fold(local, i64::saturating_add)
}

/// Fold `other` into `into`: counts/times sum, fragment wall takes max — identical to the
/// query-level aggregation so per-fragment and query-level numbers reconcile.
fn merge_summary(into: &mut DistributedProfileSummary, other: &DistributedProfileSummary) {
    into.fragment_instance_count += other.fragment_instance_count;
    into.fragment_wall_max_ns = into.fragment_wall_max_ns.max(other.fragment_wall_max_ns);
    into.fragment_wall_sum_ns = into
        .fragment_wall_sum_ns
        .saturating_add(other.fragment_wall_sum_ns);
    into.driver_total_time_ns = into
        .driver_total_time_ns
        .saturating_add(other.driver_total_time_ns);
    into.operator_active_time_ns = into
        .operator_active_time_ns
        .saturating_add(other.operator_active_time_ns);
    into.driver_blocked_time_ns = into
        .driver_blocked_time_ns
        .saturating_add(other.driver_blocked_time_ns);
    into.source_wait_time_ns = into
        .source_wait_time_ns
        .saturating_add(other.source_wait_time_ns);
    into.sink_wait_time_ns = into
        .sink_wait_time_ns
        .saturating_add(other.sink_wait_time_ns);
    into.dependency_wait_time_ns = into
        .dependency_wait_time_ns
        .saturating_add(other.dependency_wait_time_ns);
    into.exchange_wait_time_ns = into
        .exchange_wait_time_ns
        .saturating_add(other.exchange_wait_time_ns);
    into.exchange_process_time_ns = into
        .exchange_process_time_ns
        .saturating_add(other.exchange_process_time_ns);
    into.network_time_ns = into.network_time_ns.saturating_add(other.network_time_ns);
    into.scan_io_time_ns = into.scan_io_time_ns.saturating_add(other.scan_io_time_ns);
}

fn merge_actual_metrics(
    actuals: &mut HashMap<i32, ActualMetrics>,
    node_id: i32,
    metrics: ActualMetrics,
) {
    let metrics = sanitize_operator_total_time(metrics);
    let entry = actuals.entry(node_id).or_default();
    entry.output_rows = entry.output_rows.saturating_add(metrics.output_rows);
    entry.peak_mem_bytes = entry.peak_mem_bytes.saturating_add(metrics.peak_mem_bytes);
    entry.dict_input_rows = entry
        .dict_input_rows
        .saturating_add(metrics.dict_input_rows);
    entry.dict_input_columns = entry
        .dict_input_columns
        .saturating_add(metrics.dict_input_columns);
    entry.dict_kept_rows = entry.dict_kept_rows.saturating_add(metrics.dict_kept_rows);
    entry.dict_kept_columns = entry
        .dict_kept_columns
        .saturating_add(metrics.dict_kept_columns);
    entry.dict_hydrated_rows = entry
        .dict_hydrated_rows
        .saturating_add(metrics.dict_hydrated_rows);
    entry.dict_hydrated_columns = entry
        .dict_hydrated_columns
        .saturating_add(metrics.dict_hydrated_columns);
    entry.dict_unsupported_columns = entry
        .dict_unsupported_columns
        .saturating_add(metrics.dict_unsupported_columns);
    match metrics.total_time_ns.cmp(&entry.total_time_ns) {
        std::cmp::Ordering::Greater => {
            entry.total_time_ns = metrics.total_time_ns;
            entry.total_time_min_ns = metrics.total_time_min_ns;
        }
        std::cmp::Ordering::Equal => {
            entry.total_time_min_ns = match (entry.total_time_min_ns, metrics.total_time_min_ns) {
                (0, incoming) => incoming,
                (current, 0) => current,
                (current, incoming) => current.min(incoming),
            };
        }
        std::cmp::Ordering::Less => {}
    }
    entry.total_time_max_ns = entry.total_time_max_ns.max(metrics.total_time_max_ns);
    entry.build_ht_ns = entry.build_ht_ns.max(metrics.build_ht_ns);
    entry.search_ns = entry.search_ns.max(metrics.search_ns);
    entry.out_build_ns = entry.out_build_ns.max(metrics.out_build_ns);
    entry.out_probe_ns = entry.out_probe_ns.max(metrics.out_probe_ns);
}

fn sanitize_operator_total_time(mut metrics: ActualMetrics) -> ActualMetrics {
    metrics.total_time_ns = metrics.total_time_ns.max(0);
    metrics.total_time_min_ns = metrics.total_time_min_ns.max(0);
    metrics.total_time_max_ns = metrics.total_time_max_ns.max(0);
    metrics
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
        ActualMetrics, COMMON_METRICS, DICT_HYDRATED_COLUMNS, DICT_HYDRATED_ROWS,
        DICT_INPUT_COLUMNS, DICT_INPUT_ROWS, DICT_KEPT_COLUMNS, DICT_KEPT_ROWS,
        DICT_UNSUPPORTED_COLUMNS, UNIQUE_METRICS, collect_actuals_by_plan_node_id,
        collect_actuals_by_plan_node_id_from_profile_trees, collect_actuals_by_plan_node_id_multi,
        collect_distributed_profile_summary_from_profile_trees,
        collect_per_fragment_profile_summaries, merge_actual_metrics,
    };
    use crate::runtime::profile::{ProfileUnit, Profiler};

    fn add_operator_metrics(
        parent: &Profiler,
        name: &str,
        output_rows: i64,
        total_time_ns: i64,
        peak_mem_bytes: i64,
    ) {
        let common = parent.child(name).child(COMMON_METRICS);
        common.counter_set("PullRowNum", ProfileUnit::Unit, output_rows);
        common.counter_set("OperatorTotalTime", ProfileUnit::TimeNs, total_time_ns);
        common.counter_set(
            "OperatorPeakMemoryUsage",
            ProfileUnit::Bytes,
            peak_mem_bytes,
        );
    }

    fn add_dictionary_metrics(
        parent: &Profiler,
        name: &str,
        input_rows: i64,
        input_columns: i64,
        kept_rows: i64,
        kept_columns: i64,
        hydrated_rows: i64,
        hydrated_columns: i64,
        unsupported_columns: i64,
    ) {
        let unique = parent.child(name).child(UNIQUE_METRICS);
        unique.counter_set(DICT_INPUT_ROWS, ProfileUnit::Unit, input_rows);
        unique.counter_set(DICT_INPUT_COLUMNS, ProfileUnit::Unit, input_columns);
        unique.counter_set(DICT_KEPT_ROWS, ProfileUnit::Unit, kept_rows);
        unique.counter_set(DICT_KEPT_COLUMNS, ProfileUnit::Unit, kept_columns);
        unique.counter_set(DICT_HYDRATED_ROWS, ProfileUnit::Unit, hydrated_rows);
        unique.counter_set(DICT_HYDRATED_COLUMNS, ProfileUnit::Unit, hydrated_columns);
        unique.counter_set(
            DICT_UNSUPPORTED_COLUMNS,
            ProfileUnit::Unit,
            unsupported_columns,
        );
    }

    #[test]
    fn collect_actuals_reads_dictionary_unique_metrics() {
        let profiler = Profiler::new("query");
        let driver = profiler
            .child("Pipeline (id=0)")
            .child("PipelineDriver (id=0)");
        add_operator_metrics(&driver, "SCAN (plan_node_id=2)", 10, 5, 64);
        add_dictionary_metrics(&driver, "SCAN (plan_node_id=2)", 100, 3, 80, 2, 20, 1, 4);

        let actuals = collect_actuals_by_plan_node_id(&profiler);
        let metrics = actuals.get(&2).expect("node 2 metrics");

        assert_eq!(metrics.output_rows, 10);
        assert_eq!(metrics.dict_input_rows, 100);
        assert_eq!(metrics.dict_input_columns, 3);
        assert_eq!(metrics.dict_kept_rows, 80);
        assert_eq!(metrics.dict_kept_columns, 2);
        assert_eq!(metrics.dict_hydrated_rows, 20);
        assert_eq!(metrics.dict_hydrated_columns, 1);
        assert_eq!(metrics.dict_unsupported_columns, 4);
    }

    #[test]
    fn collect_actuals_reads_dictionary_unique_metrics_from_native_tree() {
        let profiler = Profiler::new("query");
        let driver = profiler
            .child("Pipeline (id=0)")
            .child("PipelineDriver (id=0)");
        add_operator_metrics(&driver, "SCAN (plan_node_id=2)", 10, 5, 64);
        add_dictionary_metrics(&driver, "SCAN (plan_node_id=2)", 101, 4, 81, 3, 21, 2, 5);
        let tree =
            crate::service::fe_report::merge_pipeline_profiles_for_fe(&profiler).to_native_tree();

        let actuals = collect_actuals_by_plan_node_id_from_profile_trees(&[tree]);
        let metrics = actuals.get(&2).expect("node 2 metrics");

        assert_eq!(metrics.output_rows, 10);
        assert_eq!(metrics.dict_input_rows, 101);
        assert_eq!(metrics.dict_input_columns, 4);
        assert_eq!(metrics.dict_kept_rows, 81);
        assert_eq!(metrics.dict_kept_columns, 3);
        assert_eq!(metrics.dict_hydrated_rows, 21);
        assert_eq!(metrics.dict_hydrated_columns, 2);
        assert_eq!(metrics.dict_unsupported_columns, 5);
    }

    #[test]
    fn merge_actual_metrics_sums_dictionary_counters() {
        let mut actuals = std::collections::HashMap::new();
        merge_actual_metrics(
            &mut actuals,
            7,
            ActualMetrics {
                dict_input_rows: 100,
                dict_input_columns: 3,
                dict_kept_rows: 90,
                dict_kept_columns: 2,
                dict_hydrated_rows: 10,
                dict_hydrated_columns: 1,
                dict_unsupported_columns: 4,
                ..ActualMetrics::default()
            },
        );
        merge_actual_metrics(
            &mut actuals,
            7,
            ActualMetrics {
                dict_input_rows: 200,
                dict_input_columns: 5,
                dict_kept_rows: 180,
                dict_kept_columns: 4,
                dict_hydrated_rows: 20,
                dict_hydrated_columns: 2,
                dict_unsupported_columns: 6,
                ..ActualMetrics::default()
            },
        );

        let metrics = actuals.get(&7).expect("node 7 metrics");
        assert_eq!(metrics.dict_input_rows, 300);
        assert_eq!(metrics.dict_input_columns, 8);
        assert_eq!(metrics.dict_kept_rows, 270);
        assert_eq!(metrics.dict_kept_columns, 6);
        assert_eq!(metrics.dict_hydrated_rows, 30);
        assert_eq!(metrics.dict_hydrated_columns, 3);
        assert_eq!(metrics.dict_unsupported_columns, 10);
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
                total_time_max_ns: 5,
                total_time_min_ns: 5,
                ..ActualMetrics::default()
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
                total_time_max_ns: 5,
                total_time_min_ns: 5,
                ..ActualMetrics::default()
            })
        );
    }

    #[test]
    fn preserves_operator_total_time_minmax_after_driver_merge() {
        let profiler = Profiler::new("query");
        let pipeline = profiler.child("Pipeline (id=0)");
        let driver0 = pipeline.child("PipelineDriver (id=0)");
        add_operator_metrics(&driver0, "HASH JOIN (plan_node_id=9)", 10, 10, 64);
        let common0 = driver0
            .child("HASH JOIN (plan_node_id=9)")
            .child("CommonMetrics");
        common0.add_timer("BuildHashTableTime").set(2);
        common0.add_timer("SearchHashTableTime").set(4);
        common0.add_timer("OutputBuildColumnTime").set(2);
        common0.add_timer("OutputProbeColumnTime").set(4);

        let driver1 = pipeline.child("PipelineDriver (id=1)");
        add_operator_metrics(&driver1, "HASH JOIN (plan_node_id=9)", 20, 30, 32);
        let common1 = driver1
            .child("HASH JOIN (plan_node_id=9)")
            .child("CommonMetrics");
        common1.add_timer("BuildHashTableTime").set(6);
        common1.add_timer("SearchHashTableTime").set(8);
        common1.add_timer("OutputBuildColumnTime").set(6);
        common1.add_timer("OutputProbeColumnTime").set(8);

        let actuals = collect_actuals_by_plan_node_id(&profiler);

        assert_eq!(
            actuals.get(&9).copied(),
            Some(ActualMetrics {
                output_rows: 30,
                total_time_ns: 20,
                peak_mem_bytes: 96,
                total_time_max_ns: 30,
                total_time_min_ns: 10,
                build_ht_ns: 4,
                search_ns: 6,
                out_build_ns: 4,
                out_probe_ns: 6,
                ..ActualMetrics::default()
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
                total_time_max_ns: 12_000,
                total_time_min_ns: 12_000,
                ..ActualMetrics::default()
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
                total_time_max_ns: 20,
                total_time_min_ns: 20,
                ..ActualMetrics::default()
            })
        );
        assert_eq!(
            actuals.get(&5).copied(),
            Some(ActualMetrics {
                output_rows: 1,
                total_time_ns: 30,
                peak_mem_bytes: 512,
                total_time_max_ns: 30,
                total_time_min_ns: 30,
                ..ActualMetrics::default()
            })
        );
    }

    #[test]
    fn collects_actuals_from_native_profile_trees_after_pipeline_merge() {
        let profiler = Profiler::new("query");
        let driver = profiler
            .child("Pipeline (id=0)")
            .child("PipelineDriver (id=0)");
        add_operator_metrics(&driver, "HASH JOIN (plan_node_id=4)", 8, 900_000, 4096);
        let tree =
            crate::service::fe_report::merge_pipeline_profiles_for_fe(&profiler).to_native_tree();

        let actuals = collect_actuals_by_plan_node_id_from_profile_trees(&[tree]);

        assert_eq!(
            actuals.get(&4).copied(),
            Some(ActualMetrics {
                output_rows: 8,
                total_time_ns: 900_000,
                peak_mem_bytes: 4096,
                total_time_max_ns: 900_000,
                total_time_min_ns: 900_000,
                ..ActualMetrics::default()
            })
        );
    }

    #[test]
    fn collects_actuals_from_native_profile_trees() {
        let profiler = Profiler::new("query");
        let driver = profiler
            .child("Pipeline (id=0)")
            .child("PipelineDriver (id=0)");
        add_operator_metrics(&driver, "HASH JOIN (plan_node_id=4)", 8, 900_000, 4096);
        let tree =
            crate::service::fe_report::merge_pipeline_profiles_for_fe(&profiler).to_native_tree();

        let actuals = collect_actuals_by_plan_node_id_from_profile_trees(&[tree]);

        assert_eq!(
            actuals.get(&4).copied(),
            Some(ActualMetrics {
                output_rows: 8,
                total_time_ns: 900_000,
                peak_mem_bytes: 4096,
                total_time_max_ns: 900_000,
                total_time_min_ns: 900_000,
                ..ActualMetrics::default()
            })
        );
    }

    #[test]
    fn summarizes_distributed_profile_attribution_from_native_trees() {
        let profiler = Profiler::new("fragment");
        profiler.counter_set("FragmentWallTime", ProfileUnit::TimeNs, 20_000);
        let driver = profiler
            .child("Pipeline (id=0)")
            .child("PipelineDriver (id=0)");
        driver.counter_set("DriverTotalTime", ProfileUnit::TimeNs, 10_000);
        driver.counter_set("DriverBlockedTime", ProfileUnit::TimeNs, 4_500);
        driver.counter_set("DriverInputEmptyTime", ProfileUnit::TimeNs, 2_000);
        driver.counter_set("DriverOutputFullTime", ProfileUnit::TimeNs, 1_500);
        driver.counter_set("DriverDependencyWaitTime", ProfileUnit::TimeNs, 1_000);
        add_operator_metrics(&driver, "EXCHANGE_SOURCE (plan_node_id=2)", 8, 3_000, 512);
        let exchange_unique = driver
            .child("EXCHANGE_SOURCE (plan_node_id=2)")
            .child("UniqueMetrics");
        exchange_unique.counter_set("WaitTime", ProfileUnit::TimeNs, 700);
        exchange_unique.counter_set("ReceiverProcessTotalTime", ProfileUnit::TimeNs, 300);
        add_operator_metrics(&driver, "DATA_STREAM_SINK (plan_node_id=3)", 8, 2_000, 256);
        let sink_unique = driver
            .child("DATA_STREAM_SINK (plan_node_id=3)")
            .child("UniqueMetrics");
        sink_unique.counter_set("NetworkTime", ProfileUnit::TimeNs, 900);
        let scan_unique = driver.child("SCAN (plan_node_id=4)").child("UniqueMetrics");
        scan_unique.counter_set("IOTaskExecTime", ProfileUnit::TimeNs, 1_100);

        let summary =
            collect_distributed_profile_summary_from_profile_trees(&[profiler.to_native_tree()]);

        assert_eq!(summary.fragment_instance_count, 1);
        assert_eq!(summary.fragment_wall_max_ns, 20_000);
        assert_eq!(summary.fragment_wall_sum_ns, 20_000);
        assert_eq!(summary.driver_total_time_ns, 10_000);
        assert_eq!(summary.operator_active_time_ns, 5_000);
        assert_eq!(summary.driver_blocked_time_ns, 4_500);
        assert_eq!(summary.source_wait_time_ns, 2_000);
        assert_eq!(summary.sink_wait_time_ns, 1_500);
        assert_eq!(summary.dependency_wait_time_ns, 1_000);
        assert_eq!(summary.exchange_wait_time_ns, 700);
        assert_eq!(summary.exchange_process_time_ns, 300);
        assert_eq!(summary.network_time_ns, 900);
        assert_eq!(summary.scan_io_time_ns, 1_100);
    }

    #[test]
    fn sums_named_profile_counters_from_native_trees() {
        let make_tree = |files_pruned: i64, unsupported: i64| {
            let profiler = Profiler::new("fragment");
            let common = profiler
                .child("Pipeline (id=0)")
                .child("PipelineDriver (id=0)")
                .child("SCAN (plan_node_id=1)")
                .child("CommonMetrics");
            common.counter_set(
                "IcebergRuntimeFilePruning/FilesPruned",
                ProfileUnit::Unit,
                files_pruned,
            );
            common.counter_set(
                "IcebergRuntimeFilePruning/Unsupported",
                ProfileUnit::Unit,
                unsupported,
            );
            profiler.to_native_tree()
        };
        let names = [
            "IcebergRuntimeFilePruning/FilesPruned",
            "IcebergRuntimeFilePruning/Unsupported",
        ];

        let sums = super::sum_profile_counters_by_name_from_profile_trees(
            &[make_tree(1, 0), make_tree(2, 3)],
            &names,
        );

        assert_eq!(sums["IcebergRuntimeFilePruning/FilesPruned"], 3);
        assert_eq!(sums["IcebergRuntimeFilePruning/Unsupported"], 3);
    }

    #[test]
    fn collects_phase_timers_and_minmax_from_native_tree() {
        let profiler = Profiler::new("Fragment");
        let op = profiler.child("HASH JOIN (plan_node_id=9)");
        let common = op.child("CommonMetrics");
        common.add_counter("PullRowNum", ProfileUnit::Unit).set(100);
        let total = common.add_timer("OperatorTotalTime");
        total.set(44_000);
        total.set_min(43_000);
        total.set_max(46_000);
        common
            .add_counter("OperatorPeakMemoryUsage", ProfileUnit::Bytes)
            .set(640);
        common.add_timer("BuildHashTableTime").set(0);
        common.add_timer("SearchHashTableTime").set(20_000);
        common.add_timer("OutputBuildColumnTime").set(6_000);
        common.add_timer("OutputProbeColumnTime").set(9_000);

        let tree = profiler.to_native_tree();
        let actuals = collect_actuals_by_plan_node_id_from_profile_trees(&[tree]);
        let m = actuals.get(&9).expect("node 9 metrics");
        assert_eq!(m.output_rows, 100);
        assert_eq!(m.total_time_ns, 44_000);
        assert_eq!(m.total_time_max_ns, 46_000);
        assert_eq!(m.total_time_min_ns, 43_000);
        assert_eq!(m.search_ns, 20_000);
        assert_eq!(m.out_build_ns, 6_000);
        assert_eq!(m.out_probe_ns, 9_000);
        assert_eq!(m.build_ht_ns, 0);
    }

    #[test]
    fn collects_per_fragment_profile_summaries_keyed_by_fragment_root_node() {
        // Fragment A: keyed by the root (output) plan-node id = 9, read from the
        // `execute_fragment (plan_node_id=9)` tree root; two instances merge.
        // The `Pipeline (id=0)` / `PipelineDriver (id=0)` wrapper nodes carry counters (as in real
        // trees) so they are serialized — guarding against a regression to a min-over-nodes key,
        // which would mis-key on the wrapper `(id=0)` instead of the real fragment root.
        let make_a = |active: i64, blocked: i64| {
            let p = Profiler::new("execute_fragment (plan_node_id=9)");
            let pipeline = p.child("Pipeline (id=0)");
            pipeline.counter_set("DriverTotalTime", ProfileUnit::TimeNs, 1);
            let driver = pipeline.child("PipelineDriver (id=0)");
            driver.counter_set("DriverBlockedTime", ProfileUnit::TimeNs, blocked);
            add_operator_metrics(&driver, "HASH JOIN (plan_node_id=9)", 1, active, 0);
            add_operator_metrics(&driver, "SCAN (plan_node_id=4)", 1, 0, 0);
            p.to_native_tree()
        };
        // Fragment B: root (output) plan-node id = 2.
        let make_b = || {
            let p = Profiler::new("execute_fragment (plan_node_id=2)");
            let pipeline = p.child("Pipeline (id=0)");
            pipeline.counter_set("DriverTotalTime", ProfileUnit::TimeNs, 1);
            let driver = pipeline.child("PipelineDriver (id=0)");
            driver.counter_set("DriverBlockedTime", ProfileUnit::TimeNs, 300);
            add_operator_metrics(&driver, "SCAN (plan_node_id=2)", 1, 5_000, 0);
            p.to_native_tree()
        };

        let trees = vec![make_a(10_000, 100), make_a(20_000, 200), make_b()];
        let by_fragment = collect_per_fragment_profile_summaries(&trees);

        assert_eq!(by_fragment.len(), 2);
        let a = by_fragment.get(&9).expect("fragment root node 9");
        assert_eq!(a.fragment_instance_count, 2);
        assert_eq!(a.operator_active_time_ns, 30_000);
        assert_eq!(a.driver_blocked_time_ns, 300);
        let b = by_fragment.get(&2).expect("fragment root node 2");
        assert_eq!(b.fragment_instance_count, 1);
        assert_eq!(b.operator_active_time_ns, 5_000);
        assert_eq!(b.driver_blocked_time_ns, 300);
    }

    #[test]
    fn collects_actuals_clamps_negative_operator_total_time() {
        let profiler = Profiler::new("Fragment");
        let op = profiler.child("SCAN (plan_node_id=2)");
        let common = op.child("CommonMetrics");
        let total = common.add_timer("OperatorTotalTime");
        total.set(-10_000);
        total.set_min(-5_000);
        total.set_max(-1_000);

        let tree = profiler.to_native_tree();
        let actuals = collect_actuals_by_plan_node_id_from_profile_trees(&[tree]);
        let m = actuals.get(&2).expect("node 2 metrics");
        assert_eq!(m.total_time_ns, 0);
        assert_eq!(m.total_time_min_ns, 0);
        assert_eq!(m.total_time_max_ns, 0);
    }

    #[test]
    fn merge_actual_metrics_keeps_min_from_winning_total_time_source() {
        let mut actuals = std::collections::HashMap::new();
        merge_actual_metrics(
            &mut actuals,
            9,
            ActualMetrics {
                output_rows: 100,
                total_time_ns: 100,
                peak_mem_bytes: 64,
                total_time_max_ns: 100,
                total_time_min_ns: 80,
                build_ht_ns: 20,
                search_ns: 30,
                out_build_ns: 35,
                out_probe_ns: 40,
                ..ActualMetrics::default()
            },
        );
        merge_actual_metrics(
            &mut actuals,
            9,
            ActualMetrics {
                output_rows: 3,
                total_time_ns: 10,
                peak_mem_bytes: 8,
                total_time_max_ns: 120,
                total_time_min_ns: 1,
                build_ht_ns: 25,
                search_ns: 10,
                out_build_ns: 45,
                out_probe_ns: 15,
                ..ActualMetrics::default()
            },
        );

        assert_eq!(
            actuals.get(&9).copied(),
            Some(ActualMetrics {
                output_rows: 103,
                total_time_ns: 100,
                peak_mem_bytes: 72,
                total_time_max_ns: 120,
                total_time_min_ns: 80,
                build_ht_ns: 25,
                search_ns: 30,
                out_build_ns: 45,
                out_probe_ns: 40,
                ..ActualMetrics::default()
            })
        );
    }
}
