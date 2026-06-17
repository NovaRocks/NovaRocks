//! Transitional helpers between analyzer/planner expression wrappers and
//! memo-native `ScalarId` wrappers.

use arrow::datatypes::DataType;

use crate::sql::analysis::{ExprKind, OutputColumn, ProjectItem, SortItem, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::{ScalarAggregateSpec, ScalarProjectItem, ScalarWindowSpec};
use crate::sql::optimizer::scalar::{
    ColumnDisplay, ScalarArena, ScalarId, SortKey, intern_typed, materialize,
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
        display: ColumnDisplay::from_expr(&item.expr),
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
        expr_display: ColumnDisplay::from_expr(&item.expr),
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
    ScalarWindowSpec {
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
