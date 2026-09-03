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
//! Projection from a runtime profile tree to task-protocol operator statistics.
//!
//! This lives beside the profile model rather than in either role, because the
//! backend produces the projection and the frontend renders it: a copy on each
//! side would be two authorities over the same counter conventions.
//!
//! The counter names read here are the ones the pipeline driver writes and the
//! frontend's own profile reader already keys by, so the task protocol reports
//! the same facts the pre-task profile path reported rather than inventing a
//! parallel vocabulary.

use std::collections::BTreeMap;
use std::time::Duration;

use crate::runtime::profile::{ProfileNode, RuntimeProfileTree};
use crate::task_execution::status::{OperatorStatistics, SafeDetail};

/// The child profile that carries every operator's shared counters.
///
/// Its presence is also what identifies an operator node: `Pipeline (id=N)`
/// and `PipelineDriver (id=N)` carry a driver id in the same `(id=…)` shape a
/// plan node uses, and only the absence of this child separates them from a
/// real operator.
const COMMON_METRICS: &str = "CommonMetrics";

/// Rows pushed into an operator.
const INPUT_ROWS_COUNTER: &str = "PushRowNum";

/// Rows pulled out of an operator.
const OUTPUT_ROWS_COUNTER: &str = "PullRowNum";

/// Time an operator spent working, excluding IO, network and receiver waits.
///
/// This is the only per-operator time the pipeline records, and it is the one
/// the frontend's profile reader already treats as the operator's time.
const WALL_TIME_COUNTER: &str = "OperatorTotalTime";

/// Operator statistics projected from one profile tree, with what the tree
/// could not attribute.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OperatorStatisticsProjection {
    statistics: Vec<OperatorStatistics>,
    unattributed_operators: usize,
}

impl OperatorStatisticsProjection {
    pub fn statistics(&self) -> &[OperatorStatistics] {
        &self.statistics
    }

    pub fn into_statistics(self) -> Vec<OperatorStatistics> {
        self.statistics
    }

    /// How many operator profile nodes carried no plan-node id.
    ///
    /// Such a node is dropped rather than attributed to a neighbour or to node
    /// zero, so this count is the only trace it leaves. The result-buffer sink
    /// is the ordinary case: it is a runtime bridge that names itself
    /// `plan_node_id=-1` because it belongs to no plan node at all.
    pub const fn unattributed_operators(&self) -> usize {
        self.unattributed_operators
    }

    pub const fn is_empty(&self) -> bool {
        self.statistics.is_empty()
    }
}

/// Projects one fragment-instance profile tree onto per-operator statistics.
///
/// Entries are keyed by plan-node id *and* operator name: one plan node can
/// own several operators (a hash join's build and probe halves are two), and
/// merging them would report a sum no single operator ever observed. Copies of
/// the same operator across parallel drivers do merge, because they are the
/// same operator running at a degree the plan chose.
///
/// The caller bounds the result: `TaskStatusReporter::record_operator_statistics`
/// owns the `FINAL_TASK_INFO_MAX_OPERATORS` truncation and its explicit marker,
/// so this returns every attributable operator in a stable order and lets that
/// one owner decide what fits.
pub fn project_operator_statistics(tree: &RuntimeProfileTree) -> OperatorStatisticsProjection {
    let mut merged: BTreeMap<OperatorKey, MergedOperator> = BTreeMap::new();
    let mut unattributed_operators = 0_usize;
    visit(&tree.root, &mut merged, &mut unattributed_operators);

    let statistics = merged
        .into_iter()
        .map(|(key, merged)| merged.finish(key))
        .collect();
    OperatorStatisticsProjection {
        statistics,
        unattributed_operators,
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct OperatorKey {
    plan_node_id: i32,
    operator: String,
}

#[derive(Clone, Debug, Default)]
struct MergedOperator {
    input_rows: Option<u64>,
    output_rows: Option<u64>,
    wall_time_nanos: Option<u64>,
}

impl MergedOperator {
    fn absorb(&mut self, common: &ProfileNode) {
        // Parallel drivers each run their own copy of the operator over a
        // disjoint slice of the input, so their row counts add up.
        merge_sum(
            &mut self.input_rows,
            counter_sum(common, INPUT_ROWS_COUNTER),
        );
        merge_sum(
            &mut self.output_rows,
            counter_sum(common, OUTPUT_ROWS_COUNTER),
        );
        // Those same drivers run concurrently, so the elapsed time the
        // operator cost is the longest one, not their total or their average.
        merge_max(
            &mut self.wall_time_nanos,
            counter_max(common, WALL_TIME_COUNTER),
        );
    }

    fn finish(self, key: OperatorKey) -> OperatorStatistics {
        let mut statistics = OperatorStatistics::new(
            key.plan_node_id,
            // Operator names are short kind labels; the bound only bites on a
            // name no operator in this engine produces, and losing its tail is
            // better than losing the whole entry.
            SafeDetail::truncating(&key.operator),
        );
        if let Some(input_rows) = self.input_rows {
            statistics = statistics.with_input_rows(input_rows);
        }
        if let Some(output_rows) = self.output_rows {
            statistics = statistics.with_output_rows(output_rows);
        }
        if let Some(nanos) = self.wall_time_nanos {
            statistics = statistics.with_wall_time(Duration::from_nanos(nanos));
        }
        statistics
    }
}

fn visit(
    node: &ProfileNode,
    merged: &mut BTreeMap<OperatorKey, MergedOperator>,
    unattributed_operators: &mut usize,
) {
    if let Some(common) = node
        .children
        .iter()
        .find(|child| child.name == COMMON_METRICS)
    {
        match parse_plan_node_id(&node.name) {
            Some(plan_node_id) => {
                let key = OperatorKey {
                    plan_node_id,
                    operator: operator_label(&node.name),
                };
                merged.entry(key).or_default().absorb(common);
            }
            // Refused rather than folded: no neighbouring plan node owns this
            // operator's rows, and reporting it under node zero would invent a
            // plan node the renderer would then key real facts by.
            None => *unattributed_operators += 1,
        }
    }

    for child in &node.children {
        visit(child, merged, unattributed_operators);
    }
}

/// The plan-node id a profile node's name encodes, when it has one.
///
/// Both spellings are live: scans and sinks name themselves
/// `NAME (plan_node_id=N)`, set operators name themselves `NAME (id=N)`. A
/// negative value is the explicit "this operator has no plan node" sentinel
/// rather than an id, so it yields `None` like an unnamed node does.
fn parse_plan_node_id(name: &str) -> Option<i32> {
    let (_, digits) = split_plan_node_id(name)?;
    let node_id: i32 = digits.parse().ok()?;
    (node_id >= 0).then_some(node_id)
}

/// The operator's kind, with the id suffix removed.
///
/// The id travels in its own typed field, so repeating it inside the label
/// would state the same fact twice with no way to tell which copy a consumer
/// should trust. A name that is nothing but its suffix keeps the whole name,
/// because an empty label identifies nothing.
fn operator_label(name: &str) -> String {
    let Some((prefix, _)) = split_plan_node_id(name) else {
        return name.to_owned();
    };
    let trimmed = prefix.trim_end();
    if trimmed.is_empty() {
        name.to_owned()
    } else {
        trimmed.to_owned()
    }
}

/// Splits a profile node name around its id suffix, returning the text before
/// the suffix and the suffix's digits.
fn split_plan_node_id(name: &str) -> Option<(&str, &str)> {
    let key = if name.contains("plan_node_id=") {
        "plan_node_id="
    } else {
        "(id="
    };
    let key_start = name.find(key)?;
    let digits_start = key_start + key.len();
    let rest = &name[digits_start..];
    let end = rest
        .find(|c: char| !c.is_ascii_digit() && c != '-')
        .unwrap_or(rest.len());
    // The suffix always opens with `(`. `(id=` matches it directly; the
    // `plan_node_id=` spelling matches one byte after it. Only that one byte
    // is reclaimed, so an earlier parenthesis in the name is left alone.
    let prefix_end = if name[..key_start].ends_with('(') {
        key_start - 1
    } else {
        key_start
    };
    Some((&name[..prefix_end], &rest[..end]))
}

/// The total of one counter across its occurrences on a node.
///
/// `None` means the counter is not there. An operator that reported no rows
/// and an operator that processed zero rows are different statements, and a
/// negative count is neither: it contributes nothing rather than being clamped
/// to a zero the profile never observed.
fn counter_sum(node: &ProfileNode, name: &str) -> Option<u64> {
    let mut total: Option<u64> = None;
    for counter in node.counters.iter().filter(|counter| counter.name == name) {
        if let Ok(value) = u64::try_from(counter.value) {
            total = Some(total.unwrap_or(0).saturating_add(value));
        }
    }
    total
}

fn counter_max(node: &ProfileNode, name: &str) -> Option<u64> {
    let mut largest: Option<u64> = None;
    for counter in node.counters.iter().filter(|counter| counter.name == name) {
        if let Ok(value) = u64::try_from(counter.value) {
            largest = Some(largest.map_or(value, |current: u64| current.max(value)));
        }
    }
    largest
}

fn merge_sum(into: &mut Option<u64>, value: Option<u64>) {
    if let Some(value) = value {
        *into = Some(into.unwrap_or(0).saturating_add(value));
    }
}

fn merge_max(into: &mut Option<u64>, value: Option<u64>) {
    if let Some(value) = value {
        *into = Some(into.map_or(value, |current| current.max(value)));
    }
}

#[cfg(test)]
mod tests {
    use super::{
        INPUT_ROWS_COUNTER, OUTPUT_ROWS_COUNTER, WALL_TIME_COUNTER, project_operator_statistics,
    };
    use crate::runtime::profile::{ProfileUnit, RuntimeProfile};
    use std::time::Duration;

    /// Builds the operator profile shape the pipeline driver produces.
    fn add_operator(parent: &RuntimeProfile, name: &str, pushed: i64, pulled: i64, active_ns: i64) {
        let common = parent.child(name.to_string()).child("CommonMetrics");
        common.counter_set(INPUT_ROWS_COUNTER, ProfileUnit::Unit, pushed);
        common.counter_set(OUTPUT_ROWS_COUNTER, ProfileUnit::Unit, pulled);
        common.counter_set(WALL_TIME_COUNTER, ProfileUnit::TimeNs, active_ns);
    }

    /// Catches a producer that reads the wrong counter names, reports the id
    /// suffix as part of the operator label, or loses the plan-node id.
    #[test]
    fn projects_driver_counters_onto_the_plan_node_they_name() {
        let fragment = RuntimeProfile::new("execute_fragment_native (plan_node_id=3)");
        let driver = fragment
            .child("Pipeline (id=0)")
            .child("PipelineDriver (id=0)");
        add_operator(&driver, "SCAN (plan_node_id=2)", 0, 500, 7_000);

        let projection = project_operator_statistics(&fragment.to_native_tree());

        assert_eq!(projection.unattributed_operators(), 0);
        let entries = projection.statistics();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].plan_node_id(), 2);
        assert_eq!(entries[0].operator().as_str(), "SCAN");
        assert_eq!(entries[0].input_rows(), Some(0));
        assert_eq!(entries[0].output_rows(), Some(500));
        assert_eq!(entries[0].wall_time(), Some(Duration::from_nanos(7_000)));
    }

    /// Catches a producer that treats a missing counter as zero. An operator
    /// that reported no rows and one that processed zero rows are different
    /// statements, and only the second may be rendered as a count.
    #[test]
    fn absent_counters_stay_absent_and_zero_counters_stay_zero() {
        let fragment = RuntimeProfile::new("execute_fragment_native (plan_node_id=3)");
        let driver = fragment
            .child("Pipeline (id=0)")
            .child("PipelineDriver (id=0)");
        let silent = driver.child("SCAN (plan_node_id=2)").child("CommonMetrics");
        silent.counter_set(OUTPUT_ROWS_COUNTER, ProfileUnit::Unit, 0);

        let projection = project_operator_statistics(&fragment.to_native_tree());

        let entries = projection.statistics();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].input_rows(), None);
        assert_eq!(entries[0].output_rows(), Some(0));
        assert_eq!(entries[0].wall_time(), None);
    }

    /// Catches a producer that sums parallel drivers' elapsed time, or that
    /// keeps one driver's rows and drops the rest. Drivers run concurrently
    /// over disjoint input: rows add, elapsed time does not.
    #[test]
    fn parallel_driver_copies_merge_rows_by_sum_and_time_by_max() {
        let fragment = RuntimeProfile::new("execute_fragment_native (plan_node_id=3)");
        let pipeline = fragment.child("Pipeline (id=0)");
        add_operator(
            &pipeline.child("PipelineDriver (id=0)"),
            "SCAN (plan_node_id=2)",
            0,
            300,
            9_000,
        );
        add_operator(
            &pipeline.child("PipelineDriver (id=1)"),
            "SCAN (plan_node_id=2)",
            0,
            200,
            4_000,
        );

        let projection = project_operator_statistics(&fragment.to_native_tree());

        let entries = projection.statistics();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].output_rows(), Some(500));
        assert_eq!(entries[0].wall_time(), Some(Duration::from_nanos(9_000)));
    }

    /// Catches a producer that keys only by plan-node id. One plan node owns
    /// several operators — a hash join's build and probe halves are two — and
    /// merging them reports a row count no operator ever observed.
    #[test]
    fn operators_sharing_one_plan_node_stay_separate() {
        let fragment = RuntimeProfile::new("execute_fragment_native (plan_node_id=3)");
        let driver = fragment
            .child("Pipeline (id=0)")
            .child("PipelineDriver (id=0)");
        add_operator(&driver, "HASH_JOIN_BUILD (plan_node_id=9)", 100, 0, 5_000);
        add_operator(&driver, "HASH_JOIN_PROBE (plan_node_id=9)", 40, 30, 2_000);

        let projection = project_operator_statistics(&fragment.to_native_tree());

        let entries = projection.statistics();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].plan_node_id(), 9);
        assert_eq!(entries[0].operator().as_str(), "HASH_JOIN_BUILD");
        assert_eq!(entries[0].input_rows(), Some(100));
        assert_eq!(entries[1].operator().as_str(), "HASH_JOIN_PROBE");
        assert_eq!(entries[1].input_rows(), Some(40));
    }

    /// Catches a producer that reads `Pipeline (id=N)` or
    /// `PipelineDriver (id=N)` as a plan node. Those ids are scheduling ids in
    /// the same textual shape, and attributing them would report a driver's
    /// counters under an unrelated plan node.
    #[test]
    fn pipeline_and_driver_ids_are_not_plan_node_ids() {
        let fragment = RuntimeProfile::new("execute_fragment_native (plan_node_id=3)");
        let pipeline = fragment.child("Pipeline (id=7)");
        let driver = pipeline.child("PipelineDriver (id=11)");
        let _ = driver.add_timer("DriverTotalTime");
        add_operator(&driver, "SCAN (plan_node_id=2)", 0, 5, 1_000);

        let projection = project_operator_statistics(&fragment.to_native_tree());

        let ids: Vec<i32> = projection
            .statistics()
            .iter()
            .map(|entry| entry.plan_node_id())
            .collect();
        assert_eq!(ids, vec![2]);
    }

    /// Catches a producer that folds a plan-node-less operator onto a
    /// neighbour or onto node zero. The result-buffer sink is a runtime bridge
    /// that belongs to no plan node, and inventing one for it would let a
    /// renderer key real facts by a node the plan never had.
    #[test]
    fn operators_without_a_plan_node_are_refused_and_counted() {
        let fragment = RuntimeProfile::new("execute_fragment_native (plan_node_id=3)");
        let driver = fragment
            .child("Pipeline (id=0)")
            .child("PipelineDriver (id=0)");
        add_operator(&driver, "SCAN (plan_node_id=2)", 0, 5, 1_000);
        add_operator(&driver, "RESULT_BUFFER_SINK (plan_node_id=-1)", 5, 0, 500);

        let projection = project_operator_statistics(&fragment.to_native_tree());

        assert_eq!(projection.unattributed_operators(), 1);
        let entries = projection.statistics();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].plan_node_id(), 2);
    }

    /// Catches a producer that walks only the fragment root's own counters, or
    /// that mistakes the fragment profile itself for an operator: the root
    /// names a plan node but has no `CommonMetrics` of its own.
    #[test]
    fn fragment_root_is_not_reported_as_an_operator() {
        let fragment = RuntimeProfile::new("execute_fragment_native (plan_node_id=3)");
        let _ = fragment.add_timer("FragmentWallTime");
        add_operator(
            &fragment
                .child("Pipeline (id=0)")
                .child("PipelineDriver (id=0)"),
            "AGGREGATE (id=3)",
            10,
            1,
            2_000,
        );

        let projection = project_operator_statistics(&fragment.to_native_tree());

        let entries = projection.statistics();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].operator().as_str(), "AGGREGATE");
        assert_eq!(entries[0].plan_node_id(), 3);
    }

    /// Catches a producer whose entry order depends on hash iteration. The
    /// bound that truncates this list keeps a prefix, so an unstable order
    /// would silently change which operators survive.
    #[test]
    fn entries_are_ordered_by_plan_node_then_operator() {
        let fragment = RuntimeProfile::new("execute_fragment_native (plan_node_id=3)");
        let driver = fragment
            .child("Pipeline (id=0)")
            .child("PipelineDriver (id=0)");
        add_operator(&driver, "PROJECT (id=8)", 1, 1, 10);
        add_operator(&driver, "SCAN (plan_node_id=2)", 0, 1, 10);
        add_operator(&driver, "AGGREGATE (id=8)", 1, 1, 10);

        let projection = project_operator_statistics(&fragment.to_native_tree());

        let order: Vec<(i32, &str)> = projection
            .statistics()
            .iter()
            .map(|entry| (entry.plan_node_id(), entry.operator().as_str()))
            .collect();
        assert_eq!(order, vec![(2, "SCAN"), (8, "AGGREGATE"), (8, "PROJECT")]);
    }

    /// Catches a producer that clamps a negative counter to zero, which would
    /// turn a corrupt reading into a confident claim that nothing flowed.
    #[test]
    fn negative_counter_values_are_not_reported_as_zero() {
        let fragment = RuntimeProfile::new("execute_fragment_native (plan_node_id=3)");
        let driver = fragment
            .child("Pipeline (id=0)")
            .child("PipelineDriver (id=0)");
        add_operator(&driver, "SCAN (plan_node_id=2)", -1, 4, 1_000);

        let projection = project_operator_statistics(&fragment.to_native_tree());

        let entries = projection.statistics();
        assert_eq!(entries[0].input_rows(), None);
        assert_eq!(entries[0].output_rows(), Some(4));
    }
}
