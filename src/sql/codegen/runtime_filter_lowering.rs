use crate::sql::analysis::{ExprKind, TypedExpr};
use crate::sql::codegen::FragmentId;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::JoinDistribution;
use crate::sql::optimizer::physical_plan::JoinExecutionDistribution;
use crate::thrift::exprs;
use crate::thrift::plan_nodes;

// ---------------------------------------------------------------------------
// Scan/join ownership metadata (used by RF planning)
// ---------------------------------------------------------------------------

/// Probe-side target recorded while visiting a node that carries a
/// `RuntimeFilterProbe` annotation. The build-side hash join (visited AFTER
/// its probe descendants) looks this up by `filter_id` to wire the RF's
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

/// Remap a runtime filter's `expr_order` from the join's PRE-demote
/// `op.eq_conditions` index space to the POST-demote `eq_join_conjuncts`
/// index space that BE lowering indexes (`src/lower/node/hash_join.rs`).
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

pub(in crate::sql::codegen) fn join_distribution_mode_from_execution(
    execution_distribution: Option<JoinExecutionDistribution>,
    fallback: &JoinDistribution,
) -> plan_nodes::TJoinDistributionMode {
    match execution_distribution {
        Some(JoinExecutionDistribution::Broadcast) => plan_nodes::TJoinDistributionMode::BROADCAST,
        Some(JoinExecutionDistribution::Partitioned) => {
            plan_nodes::TJoinDistributionMode::PARTITIONED
        }
        Some(JoinExecutionDistribution::Colocate) => plan_nodes::TJoinDistributionMode::COLOCATE,
        None => match fallback {
            JoinDistribution::Broadcast => plan_nodes::TJoinDistributionMode::BROADCAST,
            JoinDistribution::Shuffle => plan_nodes::TJoinDistributionMode::PARTITIONED,
            JoinDistribution::Colocate => plan_nodes::TJoinDistributionMode::COLOCATE,
            JoinDistribution::Unknown => plan_nodes::TJoinDistributionMode::BROADCAST,
        },
    }
}

pub(in crate::sql::codegen) fn legacy_rf_distribution_to_execution(
    distribution: &JoinDistribution,
) -> JoinExecutionDistribution {
    match distribution {
        JoinDistribution::Broadcast | JoinDistribution::Unknown => {
            JoinExecutionDistribution::Broadcast
        }
        JoinDistribution::Shuffle => JoinExecutionDistribution::Partitioned,
        JoinDistribution::Colocate => JoinExecutionDistribution::Colocate,
    }
}

/// Map execution join distribution to the thrift RF
/// `(build_join_mode, local_layout, global_layout)` triple.
pub(in crate::sql::codegen) fn rf_layout_for_execution_distribution(
    distribution: JoinExecutionDistribution,
) -> (
    crate::thrift::runtime_filter::TRuntimeFilterBuildJoinMode,
    crate::thrift::runtime_filter::TRuntimeFilterLayoutMode,
    crate::thrift::runtime_filter::TRuntimeFilterLayoutMode,
) {
    use crate::thrift::runtime_filter::{TRuntimeFilterBuildJoinMode, TRuntimeFilterLayoutMode};

    match distribution {
        JoinExecutionDistribution::Broadcast => (
            TRuntimeFilterBuildJoinMode::BORADCAST,
            TRuntimeFilterLayoutMode::SINGLETON,
            TRuntimeFilterLayoutMode::SINGLETON,
        ),
        JoinExecutionDistribution::Partitioned => (
            TRuntimeFilterBuildJoinMode::PARTITIONED,
            TRuntimeFilterLayoutMode::SINGLETON,
            TRuntimeFilterLayoutMode::GLOBAL_SHUFFLE_1L,
        ),
        JoinExecutionDistribution::Colocate => (
            TRuntimeFilterBuildJoinMode::COLOCATE,
            TRuntimeFilterLayoutMode::SINGLETON,
            TRuntimeFilterLayoutMode::GLOBAL_BUCKET_1L,
        ),
    }
}
