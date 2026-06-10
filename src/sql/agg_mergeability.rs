//! Single source of truth for two-phase (Local/Global) aggregate split
//! eligibility. Shared by `SplitAggregateRule`. A function is `TwoPhase` only
//! when it has a well-defined local-update + global-merge decomposition whose
//! parallel-partition result equals the single-pass result.
//!
//! Conservative by default: distinct, ordered, order-sensitive, and unknown
//! functions stay `SinglePhaseOnly`. Distinct goes through `SplitDistinctAgg`.

use crate::sql::planner::plan::AggregateCall;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AggMergeability {
    /// Local emits intermediate state, Global merges. Safe two-phase split.
    TwoPhase,
    /// Cannot be safely two-phased.
    SinglePhaseOnly,
}

/// Order-sensitive aggregates whose parallel-partition merge would change
/// concatenation/array ordering. These stay single-phase.
fn is_order_sensitive(name: &str) -> bool {
    matches!(
        name,
        "group_concat" | "string_agg" | "array_agg" | "array_agg_distinct"
    )
}

/// Functions with an exact, deterministically-verifiable local-update +
/// global-merge decomposition. Part 1 scope: the existing whitelist plus
/// `avg`. Float/sketch families (stddev/variance/percentile/approx/bitmap/hll)
/// are added in a follow-up round with tolerance/sketch-equality tests.
fn has_two_phase_merge(name: &str) -> bool {
    matches!(name, "sum" | "min" | "max" | "count" | "avg")
}

pub(crate) fn aggregate_mergeability(call: &AggregateCall) -> AggMergeability {
    let name = call.name.to_ascii_lowercase();
    if call.distinct
        || !call.order_by.is_empty()
        || is_order_sensitive(&name)
        || !has_two_phase_merge(&name)
    {
        AggMergeability::SinglePhaseOnly
    } else {
        AggMergeability::TwoPhase
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, SortItem, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use arrow::datatypes::DataType;

    fn arg(ty: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(1),
                qualifier: None,
                column: "v".into(),
            },
            data_type: ty,
            nullable: true,
        }
    }

    fn call(name: &str, distinct: bool, ordered: bool) -> AggregateCall {
        AggregateCall {
            name: name.into(),
            args: vec![arg(DataType::Int64)],
            distinct,
            result_type: DataType::Float64,
            order_by: if ordered {
                vec![SortItem {
                    expr: arg(DataType::Int64),
                    asc: true,
                    nulls_first: false,
                }]
            } else {
                vec![]
            },
            output_column_id: ColumnId::UNSET,
        }
    }

    #[test]
    fn avg_and_existing_whitelist_are_two_phase() {
        for name in ["sum", "min", "max", "count", "avg"] {
            assert_eq!(
                aggregate_mergeability(&call(name, false, false)),
                AggMergeability::TwoPhase,
                "{name} should be TwoPhase"
            );
        }
    }

    #[test]
    fn distinct_ordered_and_order_sensitive_are_single_phase() {
        assert_eq!(
            aggregate_mergeability(&call("avg", true, false)),
            AggMergeability::SinglePhaseOnly
        );
        assert_eq!(
            aggregate_mergeability(&call("sum", false, true)),
            AggMergeability::SinglePhaseOnly
        );
        assert_eq!(
            aggregate_mergeability(&call("group_concat", false, false)),
            AggMergeability::SinglePhaseOnly
        );
    }

    #[test]
    fn unknown_function_is_single_phase() {
        assert_eq!(
            aggregate_mergeability(&call("my_udaf", false, false)),
            AggMergeability::SinglePhaseOnly
        );
    }

    #[test]
    fn two_phase_functions_have_planning_layer_intermediate_type() {
        use crate::sql::codegen::expr_compiler::infer_agg_function_types;
        use arrow::datatypes::DataType;

        // Every name the oracle calls TwoPhase must be inferrable with a defined
        // intermediate type by the planning layer. `count` takes no args; the rest
        // are exercised with a single Int64 arg.
        //
        // NOTE: this is a NECESSARY-not-sufficient guard. `infer_agg_function_types`
        // returns `Some(intermediate)` for almost every name (including its catch-all
        // arm), so a green result does NOT prove the execution layer can correctly
        // merge a newly-added function. Before adding stddev/variance/percentile/
        // sketch families to `has_two_phase_merge`, verify merge correctness with
        // result-equality (tolerance/sketch) tests, not just this guard.
        for name in ["sum", "min", "max", "count", "avg"] {
            let args: &[DataType] = if name == "count" {
                &[]
            } else {
                &[DataType::Int64]
            };
            let inferred = infer_agg_function_types(name, args, false);
            assert!(
                matches!(inferred, Ok((_, Some(_)))),
                "{name} must infer (output, Some(intermediate)); got {inferred:?}"
            );
        }
    }
}
