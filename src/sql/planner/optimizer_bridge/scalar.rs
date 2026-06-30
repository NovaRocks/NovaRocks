//! Transitional helpers between analyzer/planner expression wrappers and
//! memo-native `ScalarId` wrappers.

use arrow::datatypes::DataType;

use crate::sql::analysis::{ExprKind, OutputColumn, ProjectItem, SortItem, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::{ScalarAggregateSpec, ScalarProjectItem, ScalarWindowSpec};
use crate::sql::optimizer::scalar::{
    ColumnDisplay, HashableLiteral, ScalarArena, ScalarId, ScalarNode, SortKey,
};
use crate::sql::planner::plan::{AggregateCall, WindowExpr};

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
    ScalarAggregateSpec {
        name: call.name.clone(),
        args: intern_exprs(arena, &call.args),
        distinct: call.distinct,
        order_by: intern_sort_items(arena, &call.order_by),
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

pub(crate) fn materialize_aggregate_call(
    arena: &ScalarArena,
    call: &ScalarAggregateSpec,
    output_column: Option<&OutputColumn>,
) -> AggregateCall {
    let output_column =
        output_column.expect("aggregate ScalarId bridge requires output column metadata");
    AggregateCall {
        name: call.name.clone(),
        args: materialize_exprs(arena, &call.args),
        distinct: call.distinct,
        result_type: output_column.data_type.clone(),
        order_by: materialize_sort_keys(arena, &call.order_by),
        output_column_id: output_column.column_id,
    }
}

pub(crate) fn materialize_aggregate_calls(
    arena: &ScalarArena,
    calls: &[ScalarAggregateSpec],
    group_by_len: usize,
    output_columns: &[OutputColumn],
) -> Vec<AggregateCall> {
    assert!(
        output_columns.len() >= group_by_len + calls.len(),
        "aggregate output layout must be [group_by..., aggregates...]"
    );
    calls
        .iter()
        .enumerate()
        .map(|(idx, call)| {
            materialize_aggregate_call(arena, call, output_columns.get(group_by_len + idx))
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

pub(crate) fn materialize_window_expr(
    arena: &ScalarArena,
    expr: &ScalarWindowSpec,
    output_column: Option<&OutputColumn>,
) -> WindowExpr {
    let output_column =
        output_column.expect("window ScalarId bridge requires output column metadata");
    WindowExpr {
        name: expr.name.clone(),
        args: materialize_exprs(arena, &expr.args),
        distinct: expr.distinct,
        partition_by: materialize_exprs(arena, &expr.partition_by),
        order_by: materialize_sort_keys(arena, &expr.order_by),
        window_frame: expr.window_frame.clone(),
        result_type: output_column.data_type.clone(),
        output_name: output_column.name.clone(),
        output_column_id: output_column.column_id,
        ignore_nulls: expr.ignore_nulls,
    }
}

pub(crate) fn materialize_window_exprs(
    arena: &ScalarArena,
    exprs: &[ScalarWindowSpec],
    output_columns: &[OutputColumn],
) -> Vec<WindowExpr> {
    assert!(
        output_columns.len() >= exprs.len(),
        "window output layout must include window result columns"
    );
    let window_output_start = output_columns.len() - exprs.len();
    exprs
        .iter()
        .enumerate()
        .map(|(idx, expr)| {
            materialize_window_expr(arena, expr, output_columns.get(window_output_start + idx))
        })
        .collect()
}

pub(crate) fn intern_column_sort_key(
    arena: &mut ScalarArena,
    key: &crate::sql::optimizer::property::SortKey,
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
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    fn window_expr(output_column_id: ColumnId, output_name: &str) -> WindowExpr {
        WindowExpr {
            name: "row_number".to_string(),
            args: vec![],
            distinct: false,
            partition_by: vec![],
            order_by: vec![],
            window_frame: None,
            result_type: DataType::Int64,
            output_name: output_name.to_string(),
            output_column_id,
            ignore_nulls: false,
        }
    }

    #[test]
    fn intern_window_expr_preserves_output_column_id() {
        let output_id = ColumnId::new_for_test(701);
        let mut arena = ScalarArena::new();

        let spec = intern_window_expr(&mut arena, &window_expr(output_id, "rn"));

        assert_eq!(spec.output_column_id, output_id);
    }

    #[test]
    #[should_panic(expected = "WindowExpr rn must carry output_column_id before optimizer bridge")]
    fn intern_window_expr_rejects_unset_output_id() {
        let mut arena = ScalarArena::new();

        let _ = intern_window_expr(&mut arena, &window_expr(ColumnId::UNSET, "rn"));
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
        } => {
            let arg_ids: Vec<ScalarId> = args.iter().map(|a| intern_typed(arena, a)).collect();
            ScalarNode::FunctionCall {
                name: name.clone(),
                args: arg_ids,
                distinct: *distinct,
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
        } => ScalarNode::AggregateCall {
            name: name.clone(),
            args: args.iter().map(|a| intern_typed(arena, a)).collect(),
            distinct: *distinct,
            order_by: order_by
                .iter()
                .map(|item| intern_sort_item(arena, item))
                .collect(),
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
            partition_by,
            order_by,
            window_frame,
            ignore_nulls,
        } => ScalarNode::WindowCall {
            name: name.clone(),
            args: args.iter().map(|a| intern_typed(arena, a)).collect(),
            distinct: *distinct,
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
        } => ExprKind::FunctionCall {
            name: name.clone(),
            args: args.iter().map(|a| materialize(arena, *a)).collect(),
            distinct: *distinct,
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
        } => ExprKind::AggregateCall {
            name: name.clone(),
            args: args.iter().map(|a| materialize(arena, *a)).collect(),
            distinct: *distinct,
            order_by: order_by
                .iter()
                .map(|key| materialize_sort_key(arena, key))
                .collect(),
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
            partition_by,
            order_by,
            window_frame,
            ignore_nulls,
        } => ExprKind::WindowCall {
            name: name.clone(),
            args: args.iter().map(|a| materialize(arena, *a)).collect(),
            distinct: *distinct,
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
