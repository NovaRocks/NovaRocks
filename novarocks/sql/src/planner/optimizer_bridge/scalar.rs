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

//! Transitional helpers between analyzer/planner expression wrappers and
//! memo-native `ScalarId` wrappers.

use std::collections::HashSet;

use arrow::datatypes::DataType;

use crate::analysis::{ExprKind, OutputColumn, ProjectItem, SortItem, TypedExpr};
use crate::column_id::ColumnId;
use crate::optimizer::operator::{
    AggregateOutputLayout, ScalarAggregateSpec, ScalarProjectItem, ScalarWindowSpec,
};
use crate::optimizer::scalar::{
    ColumnDisplay, HashableLiteral, ScalarArena, ScalarId, ScalarNode, SortKey,
};
use crate::planner::payload::{AggregateCall, WindowExpr};

pub(crate) fn intern_exprs(arena: &mut ScalarArena, exprs: &[TypedExpr]) -> Vec<ScalarId> {
    exprs.iter().map(|expr| intern_typed(arena, expr)).collect()
}

pub(crate) fn materialize_exprs(arena: &ScalarArena, exprs: &[ScalarId]) -> Vec<TypedExpr> {
    exprs.iter().map(|expr| materialize(arena, *expr)).collect()
}

pub(crate) fn intern_sort_item(arena: &mut ScalarArena, item: &SortItem) -> SortKey {
    SortKey {
        expr: intern_typed(arena, &item.expr),
        asc: item.asc,
        nulls_first: item.nulls_first,
        display: column_display_from_expr(&item.expr),
    }
}

pub(crate) fn intern_sort_items(arena: &mut ScalarArena, items: &[SortItem]) -> Vec<SortKey> {
    items
        .iter()
        .map(|item| intern_sort_item(arena, item))
        .collect()
}

pub(crate) fn materialize_sort_key(arena: &ScalarArena, key: &SortKey) -> SortItem {
    let mut expr = materialize(arena, key.expr);
    apply_column_display(&mut expr, key.display.as_ref());
    SortItem {
        expr,
        asc: key.asc,
        nulls_first: key.nulls_first,
    }
}

pub(crate) fn materialize_sort_keys(arena: &ScalarArena, keys: &[SortKey]) -> Vec<SortItem> {
    keys.iter()
        .map(|key| materialize_sort_key(arena, key))
        .collect()
}

pub(crate) fn intern_project_item(
    arena: &mut ScalarArena,
    item: &ProjectItem,
) -> ScalarProjectItem {
    let scalar_item = ScalarProjectItem {
        expr: intern_typed(arena, &item.expr),
        output_name: item.output_name.clone(),
        output_column_id: item.output_column_id,
        expr_display: column_display_from_expr(&item.expr),
    };
    arena.remember_project_output_display(item.output_column_id, None, item.output_name.clone());
    scalar_item
}

pub(crate) fn intern_project_items(
    arena: &mut ScalarArena,
    items: &[ProjectItem],
) -> Vec<ScalarProjectItem> {
    items
        .iter()
        .map(|item| intern_project_item(arena, item))
        .collect()
}

pub(crate) fn materialize_project_item(
    arena: &ScalarArena,
    item: &ScalarProjectItem,
) -> ProjectItem {
    let mut expr = materialize(arena, item.expr);
    apply_column_display(&mut expr, item.expr_display.as_ref());
    ProjectItem {
        expr,
        output_name: item.output_name.clone(),
        output_column_id: item.output_column_id,
    }
}

pub(crate) fn materialize_project_items(
    arena: &ScalarArena,
    items: &[ScalarProjectItem],
) -> Vec<ProjectItem> {
    items
        .iter()
        .map(|item| materialize_project_item(arena, item))
        .collect()
}

pub(crate) fn intern_aggregate_call(
    arena: &mut ScalarArena,
    call: &AggregateCall,
) -> ScalarAggregateSpec {
    assert!(
        call.output_column_id != ColumnId::UNSET,
        "AggregateCall {} must carry output_column_id before optimizer bridge",
        call.name
    );
    ScalarAggregateSpec {
        output_column_id: call.output_column_id,
        name: call.name.clone(),
        args: intern_exprs(arena, &call.args),
        distinct: call.distinct,
        order_by: intern_sort_items(arena, &call.order_by),
        resolved: call.resolved.clone(),
    }
}

pub(crate) fn intern_aggregate_calls(
    arena: &mut ScalarArena,
    calls: &[AggregateCall],
) -> Vec<ScalarAggregateSpec> {
    calls
        .iter()
        .map(|call| intern_aggregate_call(arena, call))
        .collect()
}

fn aggregate_output_column<'a>(
    call: &ScalarAggregateSpec,
    output_layout: &'a AggregateOutputLayout,
) -> &'a OutputColumn {
    assert!(
        call.output_column_id != ColumnId::UNSET,
        "ScalarAggregateSpec {} must carry output_column_id before materialization",
        call.name
    );
    let mut matches = output_layout
        .aggregate_columns
        .iter()
        .filter(|column| column.column_id == call.output_column_id);
    let Some(output_column) = matches.next() else {
        panic!(
            "aggregate output column id {} missing from AggregateOutputLayout.aggregate_columns",
            call.output_column_id.0
        );
    };
    assert!(
        matches.next().is_none(),
        "duplicate aggregate output column id {}",
        call.output_column_id.0
    );
    output_column
}

pub(crate) fn materialize_aggregate_call(
    arena: &ScalarArena,
    call: &ScalarAggregateSpec,
    output_layout: &AggregateOutputLayout,
) -> AggregateCall {
    let output_column = aggregate_output_column(call, output_layout);
    AggregateCall {
        name: call.name.clone(),
        args: materialize_exprs(arena, &call.args),
        distinct: call.distinct,
        result_type: output_column.data_type.clone(),
        order_by: materialize_sort_keys(arena, &call.order_by),
        output_column_id: call.output_column_id,
        resolved: call.resolved.clone(),
    }
}

pub(crate) fn materialize_aggregate_calls(
    arena: &ScalarArena,
    calls: &[ScalarAggregateSpec],
    output_layout: &AggregateOutputLayout,
) -> Vec<AggregateCall> {
    let mut seen = std::collections::HashSet::new();
    calls
        .iter()
        .map(|call| {
            assert!(
                seen.insert(call.output_column_id),
                "duplicate ScalarAggregateSpec output_column_id {}",
                call.output_column_id.0
            );
            materialize_aggregate_call(arena, call, output_layout)
        })
        .collect()
}

pub(crate) fn intern_window_expr(arena: &mut ScalarArena, expr: &WindowExpr) -> ScalarWindowSpec {
    assert!(
        expr.output_column_id != ColumnId::UNSET,
        "WindowExpr {} must carry output_column_id before optimizer bridge",
        expr.output_name
    );
    ScalarWindowSpec {
        output_column_id: expr.output_column_id,
        name: expr.name.clone(),
        args: intern_exprs(arena, &expr.args),
        distinct: expr.distinct,
        function_order_by: intern_sort_items(arena, &expr.function_order_by),
        aggregate_binding: expr.aggregate_binding.clone(),
        partition_by: intern_exprs(arena, &expr.partition_by),
        order_by: intern_sort_items(arena, &expr.order_by),
        window_frame: expr.window_frame.clone(),
        ignore_nulls: expr.ignore_nulls,
    }
}

pub(crate) fn intern_window_exprs(
    arena: &mut ScalarArena,
    exprs: &[WindowExpr],
) -> Vec<ScalarWindowSpec> {
    exprs
        .iter()
        .map(|expr| intern_window_expr(arena, expr))
        .collect()
}

fn window_output_column<'a>(
    expr: &ScalarWindowSpec,
    output_columns: &'a [OutputColumn],
) -> &'a OutputColumn {
    assert!(
        expr.output_column_id != ColumnId::UNSET,
        "ScalarWindowSpec {} must carry output_column_id before materialization",
        expr.name
    );
    let mut matches = output_columns
        .iter()
        .filter(|column| column.column_id == expr.output_column_id);
    let output_column = matches.next().unwrap_or_else(|| {
        panic!(
            "window output column id {} missing from WindowOp.output_columns",
            expr.output_column_id.0
        )
    });
    assert!(
        matches.next().is_none(),
        "duplicate window output column id {}",
        expr.output_column_id.0
    );
    output_column
}

pub(crate) fn materialize_window_expr(
    arena: &ScalarArena,
    expr: &ScalarWindowSpec,
    output_columns: &[OutputColumn],
) -> WindowExpr {
    let output_column = window_output_column(expr, output_columns);
    WindowExpr {
        name: expr.name.clone(),
        args: materialize_exprs(arena, &expr.args),
        distinct: expr.distinct,
        function_order_by: materialize_sort_keys(arena, &expr.function_order_by),
        aggregate_binding: expr.aggregate_binding.clone(),
        partition_by: materialize_exprs(arena, &expr.partition_by),
        order_by: materialize_sort_keys(arena, &expr.order_by),
        window_frame: expr.window_frame.clone(),
        result_type: output_column.data_type.clone(),
        output_name: output_column.name.clone(),
        output_column_id: expr.output_column_id,
        ignore_nulls: expr.ignore_nulls,
    }
}

pub(crate) fn materialize_window_exprs(
    arena: &ScalarArena,
    exprs: &[ScalarWindowSpec],
    output_columns: &[OutputColumn],
) -> Vec<WindowExpr> {
    let mut seen = HashSet::new();
    exprs
        .iter()
        .map(|expr| {
            assert!(
                seen.insert(expr.output_column_id),
                "duplicate ScalarWindowSpec output_column_id {}",
                expr.output_column_id.0
            );
            materialize_window_expr(arena, expr, output_columns)
        })
        .collect()
}

#[allow(
    dead_code,
    reason = "Column sort-key interning is retained for optimizer bridge callers enabled in feature-specific targets."
)]
pub(crate) fn intern_column_sort_key(
    arena: &mut ScalarArena,
    key: &crate::optimizer::property::SortKey,
) -> SortKey {
    let expr = TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: key.column,
            qualifier: None,
            column: format!("{}", key.column),
        },
        data_type: DataType::Null,
        nullable: true,
    };
    SortKey {
        expr: intern_typed(arena, &expr),
        asc: key.asc,
        nulls_first: key.nulls_first,
        display: None,
    }
}

#[cfg(test)]
#[expect(
    clippy::items_after_test_module,
    reason = "The scalar conversion helpers stay adjacent to their callers; moving the large test module would obscure that local relationship."
)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    fn window_expr(output_column_id: ColumnId, output_name: &str) -> WindowExpr {
        WindowExpr {
            name: "row_number".to_string(),
            args: vec![],
            distinct: false,
            function_order_by: vec![],
            aggregate_binding: None,
            partition_by: vec![],
            order_by: vec![],
            window_frame: None,
            result_type: DataType::Int64,
            output_name: output_name.to_string(),
            output_column_id,
            ignore_nulls: false,
        }
    }

    fn aggregate_call(output_column_id: ColumnId, name: &str) -> AggregateCall {
        AggregateCall {
            name: name.to_string(),
            args: vec![],
            distinct: false,
            result_type: DataType::Int64,
            order_by: vec![],
            resolved: crate::functions::test_resolved_aggregate(
                name,
                if name == "count" {
                    &[]
                } else {
                    &[DataType::Int64]
                },
                false,
            ),
            output_column_id,
        }
    }

    fn aggregate_spec(output_column_id: ColumnId, name: &str) -> ScalarAggregateSpec {
        ScalarAggregateSpec {
            output_column_id,
            name: name.to_string(),
            args: vec![],
            distinct: false,
            order_by: vec![],
            resolved: crate::functions::test_resolved_aggregate(
                name,
                if name == "count" {
                    &[]
                } else {
                    &[DataType::Int64]
                },
                false,
            ),
        }
    }

    fn aggregate_output_column(id: ColumnId, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: id,
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            is_internal: false,
        }
    }

    fn output_column(id: ColumnId, name: &str, data_type: DataType) -> OutputColumn {
        OutputColumn {
            column_id: id,
            name: name.to_string(),
            data_type,
            nullable: true,
            is_internal: false,
        }
    }

    fn window_spec(output_column_id: ColumnId, name: &str) -> ScalarWindowSpec {
        ScalarWindowSpec {
            output_column_id,
            name: name.to_string(),
            args: vec![],
            distinct: false,
            function_order_by: vec![],
            aggregate_binding: None,
            partition_by: vec![],
            order_by: vec![],
            window_frame: None,
            ignore_nulls: false,
        }
    }

    #[test]
    fn intern_aggregate_call_preserves_output_column_id() {
        let output_id = ColumnId::new_for_test(701);
        let mut arena = ScalarArena::new();

        let spec = intern_aggregate_call(&mut arena, &aggregate_call(output_id, "sum"));

        assert_eq!(spec.output_column_id, output_id);
        assert_eq!(spec.name, "sum");
    }

    #[test]
    #[should_panic(
        expected = "AggregateCall sum must carry output_column_id before optimizer bridge"
    )]
    fn intern_aggregate_call_rejects_unset_output_column_id() {
        let mut arena = ScalarArena::new();

        let _ = intern_aggregate_call(&mut arena, &aggregate_call(ColumnId::UNSET, "sum"));
    }

    #[test]
    fn materialize_aggregate_calls_matches_outputs_by_id_not_position() {
        let id_sum = ColumnId::new_for_test(801);
        let id_count = ColumnId::new_for_test(802);
        let layout = AggregateOutputLayout::new(
            vec![],
            vec![
                aggregate_output_column(id_count, "count_b"),
                aggregate_output_column(id_sum, "sum_a"),
            ],
        );
        let arena = ScalarArena::new();

        let calls = materialize_aggregate_calls(
            &arena,
            &[
                aggregate_spec(id_sum, "sum"),
                aggregate_spec(id_count, "count"),
            ],
            &layout,
        );

        assert_eq!(calls[0].output_column_id, id_sum);
        assert_eq!(calls[0].result_type, DataType::Int64);
        assert_eq!(calls[1].output_column_id, id_count);
    }

    #[test]
    #[should_panic(
        expected = "aggregate output column id 803 missing from AggregateOutputLayout.aggregate_columns"
    )]
    fn materialize_aggregate_calls_rejects_missing_output_id() {
        let missing_id = ColumnId::new_for_test(803);
        let layout = AggregateOutputLayout::new(vec![], vec![]);
        let arena = ScalarArena::new();

        let _ = materialize_aggregate_calls(&arena, &[aggregate_spec(missing_id, "sum")], &layout);
    }

    #[test]
    #[should_panic(expected = "duplicate aggregate output column id 804")]
    fn materialize_aggregate_calls_rejects_duplicate_output_id() {
        let duplicate_id = ColumnId::new_for_test(804);
        let layout = AggregateOutputLayout::new(
            vec![],
            vec![
                aggregate_output_column(duplicate_id, "sum_a"),
                aggregate_output_column(duplicate_id, "sum_a_duplicate"),
            ],
        );
        let arena = ScalarArena::new();

        let _ =
            materialize_aggregate_calls(&arena, &[aggregate_spec(duplicate_id, "sum")], &layout);
    }

    #[test]
    #[should_panic(expected = "duplicate ScalarAggregateSpec output_column_id 806")]
    fn materialize_aggregate_calls_rejects_duplicate_spec_output_id() {
        let duplicate_id = ColumnId::new_for_test(806);
        let layout = AggregateOutputLayout::new(
            vec![],
            vec![aggregate_output_column(duplicate_id, "sum_a")],
        );
        let arena = ScalarArena::new();

        let _ = materialize_aggregate_calls(
            &arena,
            &[
                aggregate_spec(duplicate_id, "sum"),
                aggregate_spec(duplicate_id, "count"),
            ],
            &layout,
        );
    }

    #[test]
    #[should_panic(
        expected = "ScalarAggregateSpec sum must carry output_column_id before materialization"
    )]
    fn materialize_aggregate_calls_rejects_unset_output_id() {
        let layout = AggregateOutputLayout::new(vec![], vec![]);
        let arena = ScalarArena::new();

        let _ =
            materialize_aggregate_calls(&arena, &[aggregate_spec(ColumnId::UNSET, "sum")], &layout);
    }

    #[test]
    fn intern_window_expr_preserves_output_column_id() {
        let output_id = ColumnId::new_for_test(701);
        let mut arena = ScalarArena::new();

        let spec = intern_window_expr(&mut arena, &window_expr(output_id, "rn"));

        assert_eq!(spec.output_column_id, output_id);
    }

    #[test]
    fn window_bridge_preserves_function_order_and_aggregate_binding() {
        let output_id = ColumnId::new_for_test(702);
        let argument = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(7),
                qualifier: None,
                column: "value".to_string(),
            },
            data_type: DataType::Int64,
            nullable: true,
        };
        let aggregate_binding =
            crate::functions::test_resolved_aggregate("array_agg", &[DataType::Int64], false);
        let source = WindowExpr {
            name: "array_agg".to_string(),
            args: vec![argument.clone()],
            distinct: false,
            function_order_by: vec![SortItem {
                expr: argument,
                asc: false,
                nulls_first: true,
            }],
            aggregate_binding: Some(aggregate_binding.clone()),
            partition_by: vec![],
            order_by: vec![],
            window_frame: None,
            result_type: aggregate_binding.output_type.clone(),
            output_name: "ordered_values".to_string(),
            output_column_id: output_id,
            ignore_nulls: false,
        };
        let mut arena = ScalarArena::new();

        let spec = intern_window_expr(&mut arena, &source);
        assert_eq!(spec.aggregate_binding.as_ref(), Some(&aggregate_binding));
        assert_eq!(spec.function_order_by.len(), 1);
        let materialized = materialize_window_expr(
            &arena,
            &spec,
            &[output_column(
                output_id,
                "ordered_values",
                aggregate_binding.output_type.clone(),
            )],
        );

        assert_eq!(materialized.aggregate_binding, Some(aggregate_binding));
        assert_eq!(materialized.function_order_by.len(), 1);
        assert!(!materialized.function_order_by[0].asc);
        assert!(materialized.function_order_by[0].nulls_first);
    }

    #[test]
    #[should_panic(expected = "WindowExpr rn must carry output_column_id before optimizer bridge")]
    fn intern_window_expr_rejects_unset_output_id() {
        let mut arena = ScalarArena::new();

        let _ = intern_window_expr(&mut arena, &window_expr(ColumnId::UNSET, "rn"));
    }

    #[test]
    fn materialize_window_exprs_matches_outputs_by_id_not_position() {
        let arena = ScalarArena::new();
        let id_rn = ColumnId::new_for_test(801);
        let id_rank = ColumnId::new_for_test(802);
        let output_columns = vec![
            output_column(ColumnId::new_for_test(1), "a", DataType::Int32),
            output_column(id_rank, "rk", DataType::Int64),
            output_column(ColumnId::new_for_test(2), "b", DataType::Int32),
            output_column(id_rn, "rn", DataType::UInt64),
        ];

        let exprs = materialize_window_exprs(
            &arena,
            &[
                window_spec(id_rn, "row_number"),
                window_spec(id_rank, "rank"),
            ],
            &output_columns,
        );

        assert_eq!(exprs[0].output_column_id, id_rn);
        assert_eq!(exprs[0].output_name, "rn");
        assert_eq!(exprs[0].result_type, DataType::UInt64);
        assert_eq!(exprs[1].output_column_id, id_rank);
        assert_eq!(exprs[1].output_name, "rk");
        assert_eq!(exprs[1].result_type, DataType::Int64);
    }

    #[test]
    #[should_panic(expected = "window output column id 803 missing from WindowOp.output_columns")]
    fn materialize_window_exprs_rejects_missing_output_id() {
        let arena = ScalarArena::new();
        let missing_id = ColumnId::new_for_test(803);

        let _ = materialize_window_exprs(&arena, &[window_spec(missing_id, "row_number")], &[]);
    }

    #[test]
    #[should_panic(expected = "duplicate window output column id 804")]
    fn materialize_window_exprs_rejects_duplicate_output_id() {
        let arena = ScalarArena::new();
        let duplicate_id = ColumnId::new_for_test(804);
        let output_columns = vec![
            output_column(duplicate_id, "rn_a", DataType::Int64),
            output_column(duplicate_id, "rn_b", DataType::Int64),
        ];

        let _ = materialize_window_exprs(
            &arena,
            &[window_spec(duplicate_id, "row_number")],
            &output_columns,
        );
    }

    #[test]
    #[should_panic(expected = "duplicate ScalarWindowSpec output_column_id 805")]
    fn materialize_window_exprs_rejects_duplicate_expr_output_id() {
        let arena = ScalarArena::new();
        let duplicate_id = ColumnId::new_for_test(805);
        let output_columns = vec![output_column(duplicate_id, "rn", DataType::Int64)];

        let _ = materialize_window_exprs(
            &arena,
            &[
                window_spec(duplicate_id, "row_number"),
                window_spec(duplicate_id, "rank"),
            ],
            &output_columns,
        );
    }
}

fn apply_column_display(expr: &mut TypedExpr, display: Option<&ColumnDisplay>) {
    if let (
        Some(display),
        ExprKind::ColumnRef {
            qualifier, column, ..
        },
    ) = (display, &mut expr.kind)
    {
        *qualifier = display.qualifier.clone();
        *column = display.column.clone();
    }
}

fn column_display_from_expr(expr: &TypedExpr) -> Option<ColumnDisplay> {
    match &expr.kind {
        ExprKind::ColumnRef {
            qualifier, column, ..
        } => Some(ColumnDisplay::new(qualifier.clone(), column.clone())),
        _ => None,
    }
}

/// Recursively intern an analyzer `TypedExpr` into the arena, returning its id.
pub(crate) fn intern_typed(arena: &mut ScalarArena, expr: &TypedExpr) -> ScalarId {
    let node = match &expr.kind {
        ExprKind::ColumnRef {
            column_id,
            qualifier,
            column,
        } => {
            if *column_id == ColumnId::UNSET {
                let display_name = qualifier
                    .as_deref()
                    .map(|qualifier| format!("{qualifier}.{column}"))
                    .unwrap_or_else(|| column.clone());
                panic!(
                    "ColumnId::UNSET cannot be interned into ScalarArena; resolve column '{display_name}' before optimizer scalar interning"
                );
            }
            arena.remember_source_column_display(*column_id, qualifier.clone(), column.clone());
            ScalarNode::ColumnRef(*column_id)
        }
        ExprKind::LambdaParamRef { name, slot_id } => ScalarNode::LambdaParamRef {
            name: name.clone(),
            slot_id: *slot_id,
        },
        ExprKind::Literal(v) => ScalarNode::Literal(HashableLiteral(v.clone())),
        ExprKind::BinaryOp { left, op, right } => {
            let l = intern_typed(arena, left);
            let r = intern_typed(arena, right);
            ScalarNode::BinaryOp {
                op: *op,
                left: l,
                right: r,
            }
        }
        ExprKind::UnaryOp { op, expr } => ScalarNode::UnaryOp {
            op: *op,
            child: intern_typed(arena, expr),
        },
        ExprKind::FunctionCall {
            name,
            args,
            distinct,
            volatility,
        } => {
            let arg_ids: Vec<ScalarId> = args.iter().map(|a| intern_typed(arena, a)).collect();
            ScalarNode::FunctionCall {
                name: name.clone(),
                args: arg_ids,
                distinct: *distinct,
                volatility: *volatility,
            }
        }
        ExprKind::LambdaFunction { params, body } => ScalarNode::LambdaFunction {
            params: params.clone(),
            body: intern_typed(arena, body),
        },
        ExprKind::AggregateCall {
            name,
            args,
            distinct,
            order_by,
            resolved,
        } => ScalarNode::AggregateCall {
            name: name.clone(),
            args: args.iter().map(|a| intern_typed(arena, a)).collect(),
            distinct: *distinct,
            order_by: order_by
                .iter()
                .map(|item| intern_sort_item(arena, item))
                .collect(),
            resolved: resolved.clone(),
        },
        ExprKind::Cast { expr, target } => ScalarNode::Cast {
            child: intern_typed(arena, expr),
            target: target.clone(),
        },
        ExprKind::IsNull { expr, negated } => ScalarNode::IsNull {
            child: intern_typed(arena, expr),
            negated: *negated,
        },
        ExprKind::InList {
            expr,
            list,
            negated,
        } => ScalarNode::InList {
            child: intern_typed(arena, expr),
            list: list.iter().map(|item| intern_typed(arena, item)).collect(),
            negated: *negated,
        },
        ExprKind::Between {
            expr,
            low,
            high,
            negated,
        } => ScalarNode::Between {
            child: intern_typed(arena, expr),
            low: intern_typed(arena, low),
            high: intern_typed(arena, high),
            negated: *negated,
        },
        ExprKind::Like {
            expr,
            pattern,
            negated,
        } => ScalarNode::Like {
            child: intern_typed(arena, expr),
            pattern: intern_typed(arena, pattern),
            negated: *negated,
        },
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => ScalarNode::Case {
            operand: operand.as_ref().map(|item| intern_typed(arena, item)),
            when_then: when_then
                .iter()
                .map(|(when, then)| (intern_typed(arena, when), intern_typed(arena, then)))
                .collect(),
            else_expr: else_expr.as_ref().map(|item| intern_typed(arena, item)),
        },
        ExprKind::IsTruthValue {
            expr,
            value,
            negated,
        } => ScalarNode::IsTruthValue {
            child: intern_typed(arena, expr),
            value: *value,
            negated: *negated,
        },
        ExprKind::Nested(expr) => ScalarNode::Nested(intern_typed(arena, expr)),
        ExprKind::WindowCall {
            name,
            args,
            distinct,
            function_order_by,
            aggregate_binding,
            partition_by,
            order_by,
            window_frame,
            ignore_nulls,
        } => ScalarNode::WindowCall {
            name: name.clone(),
            args: args.iter().map(|a| intern_typed(arena, a)).collect(),
            distinct: *distinct,
            function_order_by: function_order_by
                .iter()
                .map(|item| intern_sort_item(arena, item))
                .collect(),
            aggregate_binding: aggregate_binding.clone(),
            partition_by: partition_by
                .iter()
                .map(|expr| intern_typed(arena, expr))
                .collect(),
            order_by: order_by
                .iter()
                .map(|item| intern_sort_item(arena, item))
                .collect(),
            window_frame: window_frame.clone(),
            ignore_nulls: *ignore_nulls,
        },
        ExprKind::SubqueryPlaceholder { .. } => {
            unreachable!("SubqueryPlaceholder must be rewritten before the optimizer")
        }
        ExprKind::Lambda { params, body } => ScalarNode::Lambda {
            params: params.clone(),
            body: intern_typed(arena, body),
        },
    };
    arena.intern(node, expr.data_type.clone(), expr.nullable)
}

/// Rebuild an analyzer `TypedExpr` from an interned id.
pub(crate) fn materialize(arena: &ScalarArena, id: ScalarId) -> TypedExpr {
    let kind = match arena.node(id) {
        ScalarNode::ColumnRef(cid) => {
            let display = arena.column_display(*cid);
            ExprKind::ColumnRef {
                column_id: *cid,
                qualifier: display.and_then(|item| item.qualifier.clone()),
                column: display
                    .map(|item| item.column.clone())
                    .unwrap_or_else(|| format!("col{}", cid.0)),
            }
        }
        ScalarNode::LambdaParamRef { name, slot_id } => ExprKind::LambdaParamRef {
            name: name.clone(),
            slot_id: *slot_id,
        },
        ScalarNode::Literal(HashableLiteral(v)) => ExprKind::Literal(v.clone()),
        ScalarNode::BinaryOp { op, left, right } => ExprKind::BinaryOp {
            left: Box::new(materialize(arena, *left)),
            op: *op,
            right: Box::new(materialize(arena, *right)),
        },
        ScalarNode::UnaryOp { op, child } => ExprKind::UnaryOp {
            op: *op,
            expr: Box::new(materialize(arena, *child)),
        },
        ScalarNode::FunctionCall {
            name,
            args,
            distinct,
            volatility,
        } => ExprKind::FunctionCall {
            name: name.clone(),
            args: args.iter().map(|a| materialize(arena, *a)).collect(),
            distinct: *distinct,
            volatility: *volatility,
        },
        ScalarNode::LambdaFunction { params, body } => ExprKind::LambdaFunction {
            params: params.clone(),
            body: Box::new(materialize(arena, *body)),
        },
        ScalarNode::AggregateCall {
            name,
            args,
            distinct,
            order_by,
            resolved,
        } => ExprKind::AggregateCall {
            name: name.clone(),
            args: args.iter().map(|a| materialize(arena, *a)).collect(),
            distinct: *distinct,
            order_by: order_by
                .iter()
                .map(|key| materialize_sort_key(arena, key))
                .collect(),
            resolved: resolved.clone(),
        },
        ScalarNode::Cast { child, target } => ExprKind::Cast {
            expr: Box::new(materialize(arena, *child)),
            target: target.clone(),
        },
        ScalarNode::IsNull { child, negated } => ExprKind::IsNull {
            expr: Box::new(materialize(arena, *child)),
            negated: *negated,
        },
        ScalarNode::InList {
            child,
            list,
            negated,
        } => ExprKind::InList {
            expr: Box::new(materialize(arena, *child)),
            list: list.iter().map(|item| materialize(arena, *item)).collect(),
            negated: *negated,
        },
        ScalarNode::Between {
            child,
            low,
            high,
            negated,
        } => ExprKind::Between {
            expr: Box::new(materialize(arena, *child)),
            low: Box::new(materialize(arena, *low)),
            high: Box::new(materialize(arena, *high)),
            negated: *negated,
        },
        ScalarNode::Like {
            child,
            pattern,
            negated,
        } => ExprKind::Like {
            expr: Box::new(materialize(arena, *child)),
            pattern: Box::new(materialize(arena, *pattern)),
            negated: *negated,
        },
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => ExprKind::Case {
            operand: operand.map(|item| Box::new(materialize(arena, item))),
            when_then: when_then
                .iter()
                .map(|(when, then)| (materialize(arena, *when), materialize(arena, *then)))
                .collect(),
            else_expr: else_expr.map(|item| Box::new(materialize(arena, item))),
        },
        ScalarNode::IsTruthValue {
            child,
            value,
            negated,
        } => ExprKind::IsTruthValue {
            expr: Box::new(materialize(arena, *child)),
            value: *value,
            negated: *negated,
        },
        ScalarNode::Nested(child) => ExprKind::Nested(Box::new(materialize(arena, *child))),
        ScalarNode::WindowCall {
            name,
            args,
            distinct,
            function_order_by,
            aggregate_binding,
            partition_by,
            order_by,
            window_frame,
            ignore_nulls,
        } => ExprKind::WindowCall {
            name: name.clone(),
            args: args.iter().map(|a| materialize(arena, *a)).collect(),
            distinct: *distinct,
            function_order_by: function_order_by
                .iter()
                .map(|key| materialize_sort_key(arena, key))
                .collect(),
            aggregate_binding: aggregate_binding.clone(),
            partition_by: partition_by
                .iter()
                .map(|expr| materialize(arena, *expr))
                .collect(),
            order_by: order_by
                .iter()
                .map(|key| materialize_sort_key(arena, key))
                .collect(),
            window_frame: window_frame.clone(),
            ignore_nulls: *ignore_nulls,
        },
        ScalarNode::Lambda { params, body } => ExprKind::Lambda {
            params: params.clone(),
            body: Box::new(materialize(arena, *body)),
        },
    };
    TypedExpr {
        kind,
        data_type: arena.data_type(id).clone(),
        nullable: arena.nullable(id),
    }
}

#[allow(
    dead_code,
    reason = "Column-id expression materialization remains available to optimizer bridge fixture and rewrite paths."
)]
pub(crate) fn column_id_expr(id: ColumnId, data_type: DataType, nullable: bool) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: id,
            qualifier: None,
            column: format!("col{}", id.0),
        },
        data_type,
        nullable,
    }
}
