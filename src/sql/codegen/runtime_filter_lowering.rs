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

use crate::sql::analysis::{ExprKind, TypedExpr};
use crate::sql::codegen::FragmentId;
use crate::sql::column_id::ColumnId;
use crate::thrift::exprs;

// ---------------------------------------------------------------------------
// Scan/join ownership metadata (used by RF planning)
// ---------------------------------------------------------------------------

/// Probe-side target recorded while visiting a node that carries a planner-side
/// runtime-filter probe annotation. The build-side hash join (visited AFTER its
/// probe descendants) looks this up by `filter_id` to wire the RF's
/// `plan_node_id_to_target_expr` and the probe-side prober params.
#[derive(Clone, Debug)]
pub(in crate::sql::codegen) struct RfProbeTarget {
    /// Thrift node id of the node that consumes the probe (scan or the
    /// root thrift node of an intermediate operator's subtree).
    pub(in crate::sql::codegen) thrift_node_id: i32,
    /// Probe key expression compiled against the target node's output scope.
    pub(in crate::sql::codegen) probe_texpr: exprs::TExpr,
    /// Fragment that owns the probe target node.
    pub(in crate::sql::codegen) fragment_id: FragmentId,
}

/// Standalone-mode pipeline DOP used for the RF layout's
/// `num_drivers_per_instance`. Mirrors the historical post-pass computation.
pub(in crate::sql::codegen) fn rf_pipeline_dop() -> i32 {
    std::thread::available_parallelism()
        .map(|p| p.get().min(4))
        .unwrap_or(4) as i32
}

/// For each probe target node, collect the build plan-node ids whose RF it
/// consumes and that live in the same fragment. Cross-fragment RFs are remote
/// waits and must not enter the local wait set.
pub(in crate::sql::codegen) fn local_rf_waiting_sets(
    builds: &[(i32, i32, FragmentId)],
    probes: &[(i32, i32, FragmentId)],
) -> std::collections::HashMap<i32, Vec<i32>> {
    use std::collections::HashMap;

    let mut by_probe_node: HashMap<i32, Vec<i32>> = HashMap::new();
    for (probe_filter_id, probe_node_id, probe_fragment_id) in probes {
        for (build_filter_id, build_node_id, build_fragment_id) in builds {
            if probe_filter_id == build_filter_id && probe_fragment_id == build_fragment_id {
                by_probe_node
                    .entry(*probe_node_id)
                    .or_default()
                    .push(*build_node_id);
            }
        }
    }
    for build_nodes in by_probe_node.values_mut() {
        build_nodes.sort_unstable();
        build_nodes.dedup();
    }
    by_probe_node
}

/// Remap a runtime filter's `expr_order` from the join's PRE-demote
/// `op.eq_conditions` index space to the POST-demote `eq_join_conjuncts`
/// index space that BE lowering indexes (`src/lower/compat/node/hash_join.rs`).
///
/// `surviving_eq_origin[j]` is the original `op.eq_conditions` index of the
/// `j`-th surviving (non-demoted) `eq_join_conjuncts` entry — built in
/// `visit_hash_join` as eq conditions are compiled and kept. Demoted
/// conditions never get an entry, so the vec is the post-demote conjunct list
/// keyed by its source index.
///
/// Returns:
/// - `Some(j)` when the RF's original conjunct survived demotion, where `j`
///   is its post-demote index.
/// - `None` when the RF's conjunct was demoted to `other_join_conjuncts` (it
///   is no longer an equi-join key at execution) — the caller MUST drop the RF.
pub(in crate::sql::codegen) fn remap_rf_expr_order(
    surviving_eq_origin: &[usize],
    pre_demote_expr_order: usize,
) -> Option<usize> {
    surviving_eq_origin
        .iter()
        .position(|&origin| origin == pre_demote_expr_order)
}

fn collect_rf_column_refs(expr: &TypedExpr, refs: &mut Vec<(ColumnId, Option<String>, String)>) {
    match &expr.kind {
        ExprKind::ColumnRef {
            column_id,
            qualifier,
            column,
        } => refs.push((*column_id, qualifier.clone(), column.clone())),
        ExprKind::BinaryOp { left, right, .. } => {
            collect_rf_column_refs(left, refs);
            collect_rf_column_refs(right, refs);
        }
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. }
        | ExprKind::Nested(expr)
        | ExprKind::LambdaFunction { body: expr, .. }
        | ExprKind::Lambda { body: expr, .. } => collect_rf_column_refs(expr, refs),
        ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => {
            for arg in args {
                collect_rf_column_refs(arg, refs);
            }
        }
        ExprKind::InList { expr, list, .. } => {
            collect_rf_column_refs(expr, refs);
            for item in list {
                collect_rf_column_refs(item, refs);
            }
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            collect_rf_column_refs(expr, refs);
            collect_rf_column_refs(low, refs);
            collect_rf_column_refs(high, refs);
        }
        ExprKind::Like { expr, pattern, .. } => {
            collect_rf_column_refs(expr, refs);
            collect_rf_column_refs(pattern, refs);
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
            ..
        } => {
            if let Some(operand) = operand {
                collect_rf_column_refs(operand, refs);
            }
            for (when, then) in when_then {
                collect_rf_column_refs(when, refs);
                collect_rf_column_refs(then, refs);
            }
            if let Some(else_expr) = else_expr {
                collect_rf_column_refs(else_expr, refs);
            }
        }
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args {
                collect_rf_column_refs(arg, refs);
            }
            for partition_expr in partition_by {
                collect_rf_column_refs(partition_expr, refs);
            }
            for item in order_by {
                collect_rf_column_refs(&item.expr, refs);
            }
        }
        ExprKind::Literal(_)
        | ExprKind::LambdaParamRef { .. }
        | ExprKind::SubqueryPlaceholder { .. } => {}
    }
}

pub(in crate::sql::codegen) fn rf_build_expr_matches_join_build_expr(
    candidate: &TypedExpr,
    expected: &TypedExpr,
) -> bool {
    let mut candidate_refs = Vec::new();
    let mut expected_refs = Vec::new();
    collect_rf_column_refs(candidate, &mut candidate_refs);
    collect_rf_column_refs(expected, &mut expected_refs);
    !expected_refs.is_empty() && candidate_refs == expected_refs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_rf_waiting_sets_include_only_same_fragment_builds() {
        let builds = vec![(7, 300, 0), (7, 100, 0), (7, 100, 0), (8, 200, 1)];
        let probes = vec![(7, 10, 0), (8, 20, 0)];

        let sets = local_rf_waiting_sets(&builds, &probes);

        assert_eq!(sets.get(&10).map(Vec::as_slice), Some(&[100, 300][..]));
        assert!(
            !sets.contains_key(&20),
            "cross-fragment RF must not be a local wait"
        );
    }
}
