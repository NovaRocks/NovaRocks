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

use novarocks_type_contract::FunctionValueType;

use crate::column_id::ColumnId;
use crate::common::OutputColumn;
use crate::optimizer::memo::{MExpr, Memo};
use crate::optimizer::operator::{
    AggStage, AggregateOutputLayout, LogicalAggregateOp, Operator, ScalarAggregateSpec,
};
use crate::optimizer::rule::{NewExpr, Rule, RuleType};
use crate::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};
use crate::optimizer::scalar_expr;

pub(crate) struct SplitAggregateRule;

impl Rule for SplitAggregateRule {
    fn name(&self) -> &str {
        "SplitAggregateRule"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Transformation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalAggregate(_))
    }

    fn apply(
        &self,
        expr: &MExpr,
        memo: &mut Memo,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Vec<NewExpr>, crate::compiler::SqlCompileError> {
        let _ = control;

        let Operator::LogicalAggregate(agg) = &expr.op else {
            return Ok(Vec::new());
        };
        if !is_eligible(agg) {
            return Ok(Vec::new());
        }

        let local_output_columns = local_output_columns(agg, &memo.scalars);
        let local_output_layout = AggregateOutputLayout::new(
            local_output_columns
                .iter()
                .take(agg.group_by.len())
                .cloned()
                .collect(),
            local_output_columns
                .iter()
                .skip(agg.group_by.len())
                .cloned()
                .collect(),
        );
        remember_group_key_output_displays(&mut memo.scalars, &agg.group_by, &local_output_columns);
        let local_group_by = aggregate_group_key_output_ref(
            &mut memo.scalars,
            &local_output_columns,
            agg.group_by.len(),
            control,
        )?;
        remember_group_key_output_displays(
            &mut memo.scalars,
            &local_group_by,
            &agg.output_layout.group_key_columns,
        );
        let global_output_layout = agg.output_layout.clone();
        let local = LogicalAggregateOp::staged(
            AggStage::Local,
            agg.group_by.clone(),
            agg.aggregates.clone(),
            local_output_layout,
            local_output_columns,
            vec![false; agg.aggregates.len()],
            true,
        );
        let local_op = Operator::LogicalAggregate(local);
        let local_group = find_existing_logical_group(memo, &local_op, &expr.children)
            .unwrap_or_else(|| {
                let local_id = memo.next_expr_id();
                memo.new_group(MExpr {
                    id: local_id,
                    op: local_op,
                    children: expr.children.clone(),
                })
            });
        let global = LogicalAggregateOp::staged(
            AggStage::Global,
            local_group_by,
            agg.aggregates.clone(),
            global_output_layout,
            agg.output_columns.clone(),
            vec![true; agg.aggregates.len()],
            true,
        );
        Ok(vec![NewExpr {
            op: Operator::LogicalAggregate(global),
            children: vec![local_group],
        }])
    }
}

fn remember_group_key_output_displays(
    scalars: &mut ScalarArena,
    group_by: &[ScalarId],
    output_columns: &[OutputColumn],
) {
    for (scalar_id, output) in group_by.iter().zip(output_columns.iter()) {
        scalars.remember_column_display_from_scalar(output.column_id, *scalar_id);
    }
}

fn is_eligible(agg: &LogicalAggregateOp) -> bool {
    agg.stage == AggStage::Single
        && !agg.is_split
        && agg.is_merge.iter().all(|flag| !*flag)
        && (!agg.aggregates.is_empty() || !agg.group_by.is_empty())
        && agg.aggregates.iter().all(is_splittable_aggregate)
}

fn is_splittable_aggregate(call: &ScalarAggregateSpec) -> bool {
    use crate::agg_mergeability::{AggMergeability, scalar_aggregate_mergeability};
    scalar_aggregate_mergeability(call) == AggMergeability::TwoPhase
}

fn local_output_columns(agg: &LogicalAggregateOp, arena: &ScalarArena) -> Vec<OutputColumn> {
    let mut columns = Vec::with_capacity(agg.group_by.len() + agg.aggregates.len());
    columns.extend(agg.group_by.iter().enumerate().map(|(idx, expr)| {
        let layout_column = agg
            .output_layout
            .group_key_columns
            .get(idx)
            .filter(|output| output.column_id != ColumnId::UNSET);
        let name = layout_column
            .map(|output| output.name.clone())
            .unwrap_or_else(|| scalar_expr::scalar_display_name(arena, *expr));
        let column_id = layout_column
            .map(|output| output.column_id)
            .unwrap_or_else(|| {
                group_key_output_column_id(
                    arena,
                    *expr,
                    &name,
                    &agg.output_layout.group_key_columns,
                )
            });
        OutputColumn {
            column_id,
            name,
            value_type: arena.value_type(*expr).clone(),

            is_internal: layout_column
                .map(|output| output.is_internal)
                .unwrap_or(false),
        }
    }));
    columns.extend(agg.aggregates.iter().enumerate().map(|(idx, call)| {
        let name = scalar_expr::aggregate_display_name(
            arena,
            &call.name,
            call.source.arguments(),
            call.distinct,
            call.source.order_by(),
        );
        let source_output = aggregate_output_column(agg, idx);
        OutputColumn {
            column_id: aggregate_output_column_id(&name, source_output),
            name,
            value_type: local_aggregate_intermediate_type(call),

            is_internal: true,
        }
    }));
    columns
}

fn local_aggregate_intermediate_type(call: &ScalarAggregateSpec) -> FunctionValueType {
    let mut value_type = crate::functions::aggregate_selection(call.source.binding())
        .intermediate_type
        .clone();
    // Preserve the existing phase-output root nullability widening.
    value_type = value_type
        .with_nullable(true)
        .expect("validated root NULL contract");
    value_type
}

pub(crate) fn group_key_output_column_id(
    arena: &ScalarArena,
    expr: ScalarId,
    display_name: &str,
    existing_outputs: &[OutputColumn],
) -> ColumnId {
    match arena.node(expr) {
        ScalarNode::ColumnRef(column_id) => *column_id,
        _ => existing_outputs
            .iter()
            .find(|output| output.name == display_name)
            .map(|output| output.column_id)
            .unwrap_or(ColumnId::UNSET),
    }
}

fn aggregate_output_column_id(
    display_name: &str,
    source_output: Option<&OutputColumn>,
) -> ColumnId {
    source_output
        .filter(|output| output.name == display_name || output.column_id != ColumnId::UNSET)
        .map(|output| output.column_id)
        .unwrap_or(ColumnId::UNSET)
}

fn aggregate_output_column(
    agg: &LogicalAggregateOp,
    aggregate_idx: usize,
) -> Option<&OutputColumn> {
    agg.output_layout.aggregate_columns.get(aggregate_idx)
}

pub(crate) fn aggregate_group_key_output_ref(
    arena: &mut ScalarArena,
    local_output_columns: &[OutputColumn],
    group_by_len: usize,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Vec<ScalarId>, crate::compiler::SqlCompileError> {
    local_output_columns
        .iter()
        .take(group_by_len)
        .map(|output| {
            arena.remember_project_output_display(output.column_id, None, output.name.clone());
            arena.intern_observed(
                ScalarNode::ColumnRef(output.column_id),
                output.value_type.clone(),
                control,
            )
        })
        .collect()
}

pub(crate) fn find_existing_logical_group(
    memo: &Memo,
    op: &Operator,
    children: &[usize],
) -> Option<usize> {
    let op_debug = format!("{op:?}");
    memo.groups.iter().position(|group| {
        group
            .logical_exprs
            .iter()
            .any(|expr| expr.children == children && format!("{:?}", expr.op) == op_debug)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::{ExprKind, OutputColumn, TypedExpr};
    use crate::column_id::ColumnId;
    use crate::optimizer::operator::{AggStage, LogicalAggregateOp, ValuesOp};
    use crate::planner::optimizer_bridge::scalar::materialize;
    use crate::planner::optimizer_bridge::scalar::{intern_aggregate_calls, intern_exprs};
    use crate::planner::payload::AggregateCall;
    use arrow::datatypes::DataType;

    fn output_column(id: u32, name: &str) -> OutputColumn {
        typed_output_column(id, name, DataType::Int64)
    }

    fn typed_output_column(id: u32, name: &str, data_type: DataType) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(data_type, false),

            is_internal: false,
        }
    }

    fn col_ref(id: u32, name: &str) -> TypedExpr {
        nullable_col_ref(id, name, false)
    }

    fn nullable_col_ref(id: u32, name: &str, nullable: bool) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: Some("t".to_string()),
                column: name.to_string(),
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, nullable),
        }
    }

    fn count_call(distinct: bool) -> AggregateCall {
        AggregateCall {
            name: "count".to_string(),
            distinct,
            result_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
            output_column_id: ColumnId::new_for_test(3),
            source: crate::binding::AggregateArgumentSource::uncertified(
                vec![col_ref(2, "v")],
                vec![],
                crate::functions::test_resolved_aggregate("count", &[DataType::Int64], distinct),
            ),
        }
    }

    fn aggregate_output_layout(
        group_by: &[TypedExpr],
        aggregates: &[AggregateCall],
        output_columns: &[OutputColumn],
    ) -> AggregateOutputLayout {
        let group_key_columns = group_by
            .iter()
            .enumerate()
            .map(|(idx, expr)| {
                if let ExprKind::ColumnRef {
                    column_id, column, ..
                } = &expr.kind
                {
                    output_columns
                        .iter()
                        .find(|output| output.column_id == *column_id)
                        .cloned()
                        .unwrap_or_else(|| OutputColumn {
                            column_id: *column_id,
                            name: column.clone(),
                            value_type: expr.value_type.clone(),

                            is_internal: false,
                        })
                } else {
                    output_columns
                        .get(idx)
                        .cloned()
                        .unwrap_or_else(|| OutputColumn {
                            column_id: ColumnId::new_for_test(9000 + idx as u32),
                            name: format!("group_{idx}"),
                            value_type: expr.value_type.clone(),

                            is_internal: false,
                        })
                }
            })
            .collect();
        let aggregate_columns = aggregates
            .iter()
            .enumerate()
            .map(|(idx, aggregate)| {
                let mut column = output_columns
                    .iter()
                    .find(|output| output.column_id == aggregate.output_column_id)
                    .cloned()
                    .or_else(|| output_columns.get(group_by.len() + idx).cloned())
                    .unwrap_or_else(|| OutputColumn {
                        column_id: aggregate.output_column_id,
                        name: aggregate.name.clone(),
                        value_type: aggregate.result_type.clone(),

                        is_internal: false,
                    });
                column.column_id = aggregate.output_column_id;
                column
            })
            .collect();
        AggregateOutputLayout::new(group_key_columns, aggregate_columns)
    }

    fn single_agg(
        memo: &mut Memo,
        group_by: Vec<TypedExpr>,
        aggregates: Vec<AggregateCall>,
        output_columns: Vec<OutputColumn>,
    ) -> LogicalAggregateOp {
        let output_layout = aggregate_output_layout(&group_by, &aggregates, &output_columns);
        let group_by = intern_exprs(
            &mut memo.scalars,
            &group_by,
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();
        let aggregates = intern_aggregate_calls(
            &mut memo.scalars,
            &aggregates,
            crate::optimizer::test_optimizer_control(),
        )
        .unwrap();
        LogicalAggregateOp::single(group_by, aggregates, output_layout, output_columns)
    }

    fn staged_agg(
        memo: &mut Memo,
        stage: AggStage,
        group_by: Vec<TypedExpr>,
        aggregates: Vec<AggregateCall>,
        output_columns: Vec<OutputColumn>,
        is_merge: Vec<bool>,
        is_split: bool,
    ) -> LogicalAggregateOp {
        let output_layout = aggregate_output_layout(&group_by, &aggregates, &output_columns);
        let group_by = intern_exprs(
            &mut memo.scalars,
            &group_by,
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();
        let aggregates = intern_aggregate_calls(
            &mut memo.scalars,
            &aggregates,
            crate::optimizer::test_optimizer_control(),
        )
        .unwrap();
        LogicalAggregateOp::staged(
            stage,
            group_by,
            aggregates,
            output_layout,
            output_columns,
            is_merge,
            is_split,
        )
    }

    fn values_group(memo: &mut Memo) -> usize {
        let id = memo.next_expr_id();
        memo.new_group(MExpr {
            id,
            op: Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        })
    }

    fn single_grouped_expr(memo: &mut Memo) -> MExpr {
        let child = values_group(memo);
        MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(single_agg(
                memo,
                vec![nullable_col_ref(1, "k", true)],
                vec![count_call(false)],
                vec![output_column(1, "k"), output_column(3, "count(v)")],
            )),
            children: vec![child],
        }
    }

    fn select_order_grouped_expr(memo: &mut Memo) -> MExpr {
        let child = values_group(memo);
        MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(single_agg(
                memo,
                vec![col_ref(1, "k")],
                vec![count_call(false)],
                vec![output_column(3, "count(v)"), output_column(1, "k")],
            )),
            children: vec![child],
        }
    }

    fn single_scalar_expr(memo: &mut Memo) -> MExpr {
        let child = values_group(memo);
        MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(single_agg(
                memo,
                vec![],
                vec![count_call(false)],
                vec![output_column(3, "count(v)")],
            )),
            children: vec![child],
        }
    }

    #[test]
    fn splits_grouped_aggregate_into_global_over_local() {
        let mut memo = Memo::new();
        let expr = single_grouped_expr(&mut memo);
        let out = SplitAggregateRule
            .apply(
                &expr,
                &mut memo,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap();
        assert_eq!(out.len(), 1);
        let Operator::LogicalAggregate(global) = &out[0].op else {
            panic!("expected global aggregate");
        };
        assert_eq!(global.stage, AggStage::Global);
        assert_eq!(global.is_merge, vec![true]);
        assert!(global.is_split);
        assert_eq!(global.group_by.len(), 1);
        assert!(
            materialize(&memo.scalars, global.group_by[0])
                .value_type
                .nullable()
        );
        assert_eq!(out[0].children.len(), 1);
        let local_group_id = out[0].children[0];
        let local_group = &memo.groups[local_group_id];
        assert_eq!(local_group.logical_exprs.len(), 1);
        let Operator::LogicalAggregate(local) = &local_group.logical_exprs[0].op else {
            panic!("expected local aggregate child");
        };
        assert_eq!(local.stage, AggStage::Local);
        assert_eq!(local.is_merge, vec![false]);
        assert!(local.is_split);
        assert_eq!(
            local.output_columns[local.group_by.len()].column_id,
            ColumnId::new_for_test(3)
        );
    }

    #[test]
    fn split_global_group_by_uses_local_group_key_layout_not_select_order_output() {
        let mut memo = Memo::new();
        let expr = select_order_grouped_expr(&mut memo);
        let out = SplitAggregateRule
            .apply(
                &expr,
                &mut memo,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap();
        assert_eq!(out.len(), 1);
        let Operator::LogicalAggregate(global) = &out[0].op else {
            panic!("expected global aggregate");
        };
        assert_eq!(global.group_by.len(), 1);
        let group_by = materialize(&memo.scalars, global.group_by[0]);
        let ExprKind::ColumnRef { column_id, .. } = &group_by.kind else {
            panic!("expected global group key column ref");
        };
        assert_eq!(*column_id, ColumnId::new_for_test(1));
    }

    #[test]
    fn split_aggregate_preserves_layout_when_visible_group_key_is_pruned() {
        let mut memo = Memo::new();
        let group_output_id = ColumnId::new_for_test(101);
        let sum_output_id = ColumnId::new_for_test(201);
        let group = intern_exprs(
            &mut memo.scalars,
            &[col_ref(1, "k")],
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap()[0];
        let arg = intern_exprs(
            &mut memo.scalars,
            &[col_ref(2, "v")],
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap()[0];
        let agg = LogicalAggregateOp::single(
            vec![group],
            vec![ScalarAggregateSpec {
                output_column_id: sum_output_id,
                name: "sum".to_string(),
                distinct: false,
                source: crate::binding::AggregateArgumentSource::uncertified(
                    vec![arg],
                    vec![],
                    crate::functions::test_resolved_aggregate("sum", &[DataType::Int64], false),
                ),
            }],
            AggregateOutputLayout::new(
                vec![OutputColumn {
                    column_id: group_output_id,
                    name: "k".to_string(),
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        DataType::Int64,
                        false,
                    ),

                    is_internal: false,
                }],
                vec![OutputColumn {
                    column_id: sum_output_id,
                    name: "sum(v)".to_string(),
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        DataType::Int64,
                        true,
                    ),

                    is_internal: false,
                }],
            ),
            vec![OutputColumn {
                column_id: sum_output_id,
                name: "sum(v)".to_string(),
                value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),

                is_internal: false,
            }],
        );
        let expr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(agg),
            children: vec![values_group(&mut memo)],
        };

        let out = SplitAggregateRule
            .apply(
                &expr,
                &mut memo,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap();

        let Operator::LogicalAggregate(global) = &out[0].op else {
            panic!("expected global aggregate");
        };
        assert_eq!(global.output_columns.len(), 1);
        assert_eq!(global.output_columns[0].column_id, sum_output_id);
        assert_eq!(
            global.output_layout.group_key_columns[0].column_id,
            group_output_id
        );
    }

    #[test]
    fn repeated_apply_reuses_existing_local_group() {
        let mut memo = Memo::new();
        let expr = single_grouped_expr(&mut memo);
        let first = SplitAggregateRule
            .apply(
                &expr,
                &mut memo,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap();
        assert_eq!(first.len(), 1);
        let first_local_group = first[0].children[0];
        let group_count_after_first = memo.groups.len();

        let second = SplitAggregateRule
            .apply(
                &expr,
                &mut memo,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].children[0], first_local_group);
        assert_eq!(memo.groups.len(), group_count_after_first);
    }

    #[test]
    fn splits_scalar_aggregate() {
        let mut memo = Memo::new();
        let expr = single_scalar_expr(&mut memo);
        let out = SplitAggregateRule
            .apply(
                &expr,
                &mut memo,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap();
        assert_eq!(out.len(), 1);
        let Operator::LogicalAggregate(global) = &out[0].op else {
            panic!("expected global aggregate");
        };
        assert_eq!(global.stage, AggStage::Global);
        assert!(global.group_by.is_empty());
        let local_group_id = out[0].children[0];
        let local_group = &memo.groups[local_group_id];
        let Operator::LogicalAggregate(local) = &local_group.logical_exprs[0].op else {
            panic!("expected local aggregate child");
        };
        assert_eq!(local.stage, AggStage::Local);
        assert!(local.group_by.is_empty());
        assert_eq!(local.output_columns[0].column_id, ColumnId::new_for_test(3));
    }

    fn avg_call() -> AggregateCall {
        AggregateCall {
            name: "avg".to_string(),
            distinct: false,
            result_type: novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Float64,
                true,
            ),
            output_column_id: ColumnId::new_for_test(3),
            source: crate::binding::AggregateArgumentSource::uncertified(
                vec![col_ref(2, "v")],
                vec![],
                crate::functions::test_resolved_aggregate("avg", &[DataType::Int64], false),
            ),
        }
    }

    #[test]
    fn split_preserves_actual_selected_value_domains_through_local_and_global() {
        use arrow::datatypes::Field;
        use novarocks_functions::FunctionArgument;
        use novarocks_type_contract::ValueLogicalType;
        use std::sync::Arc;

        let sources = [
            (
                "min",
                FunctionValueType::try_with_logical_type(
                    DataType::Utf8,
                    false,
                    ValueLogicalType::Json,
                )
                .unwrap(),
            ),
            (
                "min",
                FunctionValueType::new(
                    DataType::List(Arc::new(
                        Field::new("item", DataType::Utf8, false).with_metadata(
                            std::collections::HashMap::<String, String>::from([
                                ("nr_logical_type".into(), "json".into()),
                                ("provider.field-id".into(), "71".into()),
                            ]),
                        ),
                    )),
                    false,
                ),
            ),
            (
                "sum",
                FunctionValueType::try_with_logical_type(
                    DataType::FixedSizeBinary(16),
                    false,
                    ValueLogicalType::LargeInt,
                )
                .unwrap(),
            ),
        ];
        for (name, source) in sources {
            let arguments = [FunctionArgument::Value {
                value_type: source.clone(),
                constant: None,
            }];
            let resolved = crate::functions::builtin_sql_function_catalog()
                .resolve_aggregate_binding(
                    name,
                    1,
                    &arguments,
                    &crate::compiler::SqlCompileControl::unbounded(),
                )
                .unwrap();
            let resolved = crate::binding::SqlFunctionBinding::new(
                resolved,
                novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            );
            let result = crate::functions::aggregate_result_type(&resolved).clone();
            let mut state = crate::functions::aggregate_selection(&resolved)
                .intermediate_type
                .clone();
            state = state.with_nullable(true).unwrap();
            let input = OutputColumn {
                column_id: ColumnId(2),
                name: "v".into(),
                value_type: source.clone(),
                is_internal: false,
            };
            let group = OutputColumn {
                column_id: ColumnId(1),
                name: "g".into(),
                value_type: source.clone(),
                is_internal: false,
            };
            let output = OutputColumn {
                column_id: ColumnId(3),
                name: format!("{name}(v)"),
                value_type: result.clone(),
                is_internal: false,
            };
            let typed = |column: &OutputColumn| TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: column.column_id,
                    qualifier: None,
                    column: column.name.clone(),
                },
                value_type: column.value_type.clone(),
            };
            let call = AggregateCall {
                name: name.into(),
                distinct: false,
                result_type: result.clone(),
                output_column_id: output.column_id,
                source: crate::binding::AggregateArgumentSource::uncertified(
                    vec![typed(&input)],
                    vec![],
                    resolved.clone(),
                ),
            };
            let mut memo = Memo::new();
            let id = memo.next_expr_id();
            let child = memo.new_group(MExpr {
                id,
                op: Operator::LogicalValues(ValuesOp {
                    rows: vec![],
                    columns: vec![group.clone(), input],
                }),
                children: vec![],
            });
            let aggregate = single_agg(
                &mut memo,
                vec![typed(&group)],
                vec![call],
                vec![group.clone(), output.clone()],
            );
            let expr = MExpr {
                id: memo.next_expr_id(),
                op: Operator::LogicalAggregate(aggregate),
                children: vec![child],
            };
            let alternative = SplitAggregateRule
                .apply(
                    &expr,
                    &mut memo,
                    &crate::compiler::SqlCompileControl::unbounded(),
                )
                .unwrap();
            assert_eq!(
                alternative.len(),
                1,
                "actual selected {name} domain {source:?}"
            );
            let Operator::LogicalAggregate(global) = &alternative[0].op else {
                panic!("global aggregate");
            };
            let Operator::LogicalAggregate(local) =
                &memo.groups[alternative[0].children[0]].logical_exprs[0].op
            else {
                panic!("local aggregate");
            };
            assert_eq!(local.output_layout.aggregate_columns[0].value_type, state);
            assert_eq!(local.output_layout.group_key_columns[0].value_type, source);
            assert_eq!(global.output_layout.aggregate_columns[0].value_type, result);
            assert_eq!(global.output_columns[1].value_type, output.value_type);
            assert_eq!(memo.scalars.value_type(global.group_by[0]), &source);
            assert!(std::ptr::eq(
                local.aggregates[0].source.binding().resolved(),
                resolved.resolved()
            ));
            assert!(std::ptr::eq(
                global.aggregates[0].source.binding().resolved(),
                resolved.resolved()
            ));
        }
    }

    #[test]
    fn splits_grouped_avg_aggregate() {
        let mut memo = Memo::new();
        let child = values_group(&mut memo);
        let expr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(single_agg(
                &mut memo,
                vec![nullable_col_ref(1, "k", true)],
                vec![avg_call()],
                vec![
                    output_column(1, "k"),
                    typed_output_column(3, "avg(v)", DataType::Float64),
                ],
            )),
            children: vec![child],
        };
        let out = SplitAggregateRule
            .apply(
                &expr,
                &mut memo,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap();
        assert_eq!(out.len(), 1, "avg must now produce a split alternative");
        let Operator::LogicalAggregate(global) = &out[0].op else {
            panic!("expected global aggregate");
        };
        assert_eq!(global.stage, AggStage::Global);
        assert_eq!(global.is_merge, vec![true]);
        let local_group_id = out[0].children[0];
        let local_group = &memo.groups[local_group_id];
        let Operator::LogicalAggregate(local) = &local_group.logical_exprs[0].op else {
            panic!("expected local aggregate child");
        };
        assert_eq!(local.stage, AggStage::Local);
        assert_eq!(
            local.output_layout.aggregate_columns[0]
                .value_type
                .data_type()
                .clone(),
            DataType::Utf8
        );
        assert_eq!(
            local.output_columns[1].value_type.data_type().clone(),
            DataType::Utf8
        );
    }

    #[test]
    fn rejects_distinct_and_already_split_aggregate() {
        let mut memo = Memo::new();
        let child = values_group(&mut memo);
        let distinct = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(single_agg(
                &mut memo,
                vec![col_ref(1, "k")],
                vec![count_call(true)],
                vec![output_column(1, "k"), output_column(3, "count(v)")],
            )),
            children: vec![child],
        };
        assert!(
            SplitAggregateRule
                .apply(
                    &distinct,
                    &mut memo,
                    &crate::compiler::SqlCompileControl::unbounded()
                )
                .unwrap()
                .is_empty()
        );

        let already_split = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(staged_agg(
                &mut memo,
                AggStage::Local,
                vec![col_ref(1, "k")],
                vec![count_call(false)],
                vec![output_column(1, "k"), output_column(3, "count(v)")],
                vec![false],
                true,
            )),
            children: vec![child],
        };
        assert!(
            SplitAggregateRule
                .apply(
                    &already_split,
                    &mut memo,
                    &crate::compiler::SqlCompileControl::unbounded()
                )
                .unwrap()
                .is_empty()
        );
    }
}
