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

//! Single source of truth for two-phase (Local/Global) aggregate split
//! eligibility. Shared by `SplitAggregateRule`. A function is `TwoPhase` only
//! when it has a well-defined local-update + global-merge decomposition whose
//! parallel-partition result equals the single-pass result.
//!
//! Conservative by default: distinct, ordered, order-sensitive, and unknown
//! functions stay `SinglePhaseOnly`. Distinct goes through `SplitDistinctAgg`.

use crate::optimizer::operator::ScalarAggregateSpec;
#[cfg(test)]
use crate::planner::payload::AggregateCall;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AggMergeability {
    /// Local emits intermediate state, Global merges. Safe two-phase split.
    TwoPhase,
    /// Cannot be safely two-phased.
    SinglePhaseOnly,
}

// The executable selection owns decomposition. A second overload of an
// existing SQL name does not inherit the selected singleton's state contract.
fn has_two_phase_merge(binding: &crate::binding::SqlFunctionBinding) -> bool {
    matches!(
        (
            binding.function_id.as_str(),
            binding.selected.overload.as_str()
        ),
        (
            "builtin.aggregate/sum/v1",
            "builtin.aggregate/sum/derived-v1"
        ) | (
            "builtin.aggregate/min/v1",
            "builtin.aggregate/min/derived-v1"
        ) | (
            "builtin.aggregate/max/v1",
            "builtin.aggregate/max/derived-v1"
        ) | (
            "builtin.aggregate/count/v1",
            "builtin.aggregate/count/derived-v1"
        ) | (
            "builtin.aggregate/avg/v1",
            "builtin.aggregate/avg/derived-v1"
        )
    )
}

#[cfg(test)]
pub(crate) fn aggregate_mergeability(call: &AggregateCall) -> AggMergeability {
    aggregate_mergeability_from_parts(
        call.source.binding(),
        call.distinct,
        call.source.order_by().is_empty(),
    )
}

pub(crate) fn scalar_aggregate_mergeability(call: &ScalarAggregateSpec) -> AggMergeability {
    aggregate_mergeability_from_parts(
        call.source.binding(),
        call.distinct,
        call.source.order_by().is_empty(),
    )
}

fn aggregate_mergeability_from_parts(
    binding: &crate::binding::SqlFunctionBinding,
    distinct: bool,
    order_by_is_empty: bool,
) -> AggMergeability {
    if distinct || !order_by_is_empty || !has_two_phase_merge(binding) {
        AggMergeability::SinglePhaseOnly
    } else {
        AggMergeability::TwoPhase
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::{ExprKind, SortItem, TypedExpr};
    use crate::column_id::ColumnId;
    use arrow::datatypes::DataType;

    fn arg(ty: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(1),
                qualifier: None,
                column: "v".into(),
            },
            value_type: novarocks_type_contract::FunctionValueType::new(ty, true),
        }
    }

    fn call(name: &str, distinct: bool, ordered: bool) -> AggregateCall {
        let args = if matches!(name, "group_concat" | "string_agg") {
            vec![arg(DataType::Int64), arg(DataType::Utf8)]
        } else {
            vec![arg(DataType::Int64)]
        };
        let argument_types = args
            .iter()
            .map(|arg| arg.value_type.data_type().clone())
            .collect::<Vec<_>>();
        AggregateCall {
            name: name.into(),
            distinct,
            result_type: novarocks_type_contract::FunctionValueType::new(DataType::Float64, true),
            source: crate::binding::AggregateArgumentSource::uncertified(
                args,
                if ordered {
                    vec![SortItem {
                        expr: arg(DataType::Int64),
                        asc: true,
                        nulls_first: false,
                    }]
                } else {
                    vec![]
                },
                crate::functions::test_resolved_aggregate(name, &argument_types, distinct),
            ),
            output_column_id: ColumnId::UNSET,
        }
    }

    #[test]
    fn top_n_overloads_do_not_inherit_scalar_extrema_decomposition() {
        for name in ["min", "max"] {
            let mut call = call(name, false, false);
            let types = [DataType::Int64, DataType::Int64];
            call.source = crate::binding::AggregateArgumentSource::uncertified(
                vec![arg(DataType::Int64), arg(DataType::Int64)],
                vec![],
                crate::functions::test_resolved_aggregate(name, &types, false),
            );
            assert_eq!(
                aggregate_mergeability(&call),
                AggMergeability::SinglePhaseOnly
            );
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
    fn unknown_function_cannot_enter_the_aggregate_plan() {
        let error = crate::functions::builtin_sql_function_catalog()
            .resolve_aggregate_trusted(
                "my_udaf",
                &[DataType::Int64],
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .expect_err("unregistered aggregate must fail exact resolution");
        assert!(matches!(
            error,
            novarocks_functions::FunctionResolutionError::UnknownFunction
        ));
    }

    #[test]
    fn two_phase_functions_have_planning_layer_intermediate_type() {
        use arrow::datatypes::DataType;
        use novarocks_functions::EngineFunctionCatalog;

        // Every name the oracle calls TwoPhase must resolve to an exact catalog
        // overload with a concrete intermediate carrier. This remains a
        // necessary-not-sufficient guard: adding another family still requires
        // result-equality tests for its merge semantics.
        let catalog: EngineFunctionCatalog =
            crate::functions::build_builtin_engine_function_catalog().expect("builtin catalog");
        for name in ["sum", "min", "max", "count", "avg"] {
            let args: &[DataType] = if name == "count" {
                &[]
            } else {
                &[DataType::Int64]
            };
            let resolved = crate::compiler::SqlFunctionCatalog::resolve_aggregate_signature(
                &catalog,
                name,
                args,
                &crate::compiler::SqlCompileControl::unbounded(),
            );
            assert!(
                resolved.is_ok(),
                "{name} must resolve exactly; got {resolved:?}"
            );
        }
    }
}
