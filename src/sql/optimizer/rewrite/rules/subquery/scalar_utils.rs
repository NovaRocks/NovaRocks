#![allow(dead_code)] // Staged while subquery rules migrate to OptExpr one by one.

use std::collections::{HashMap, HashSet};

use arrow::datatypes::DataType;

use crate::sql::analysis::{BinOp, LiteralValue};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::{
    FilterOp, LogicalJoinOp, Operator, ScalarAggregateSpec, ScalarProjectItem, ScalarWindowSpec,
};
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::scalar::{HashableLiteral, ScalarArena, ScalarId, ScalarNode, SortKey};
use crate::sql::planner::plan::ApplyKind;
use crate::sql::{analysis::JoinKind, analysis::OutputColumn};

pub(super) fn opt_output_columns(
    expr: &OptExpr,
    arena: &ScalarArena,
) -> Result<Vec<OutputColumn>, String> {
    match &expr.op {
        Operator::LogicalScan(scan) => Ok(scan.columns.clone()),
        Operator::LogicalProject(project) => Ok(project
            .items
            .iter()
            .map(|item| OutputColumn {
                column_id: item.output_column_id,
                name: item.output_name.clone(),
                data_type: arena.data_type(item.expr).clone(),
                nullable: arena.nullable(item.expr),
                is_internal: false,
            })
            .collect()),
        Operator::LogicalAggregate(aggregate) => Ok(aggregate.output_columns.clone()),
        Operator::LogicalWindow(window) => Ok(window.output_columns.clone()),
        Operator::LogicalUnion(union) => Ok(union.output_columns.clone()),
        Operator::LogicalIntersect(intersect) => Ok(intersect.output_columns.clone()),
        Operator::LogicalExcept(except) => Ok(except.output_columns.clone()),
        Operator::LogicalValues(values) => Ok(values.columns.clone()),
        Operator::LogicalTableFunction(table_fn) => {
            let mut out = opt_output_columns(expr.unary_input(), arena)?;
            out.extend(table_fn.output_columns.clone());
            Ok(out)
        }
        Operator::LogicalGenerateSeries(series) => Ok(vec![OutputColumn {
            column_id: series.output_column_id,
            name: series.column_name.clone(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }]),
        Operator::LogicalCTEProduce(produce) => Ok(produce.output_columns.clone()),
        Operator::LogicalCTEConsume(consume) => Ok(consume.output_columns.clone()),
        Operator::LogicalDecode(decode) => Ok(decode.output_columns.clone()),
        Operator::LogicalAggregateStateMerge(merge) => Ok(merge.output_columns.clone()),
        Operator::LogicalFilter(_)
        | Operator::LogicalSort(_)
        | Operator::LogicalLimit(_)
        | Operator::LogicalTopN(_)
        | Operator::LogicalRepeat(_)
        | Operator::LogicalAssertOneRow(_) => opt_output_columns(expr.unary_input(), arena),
        Operator::LogicalJoin(join) => {
            let left = opt_output_columns(expr.left(), arena)?;
            let right = opt_output_columns(expr.right(), arena)?;
            Ok(join_output_columns(join.join_type, left, right))
        }
        Operator::LogicalCTEAnchor(_) => opt_output_columns(expr.child(1), arena),
        Operator::LogicalApply(apply) => {
            let mut out = opt_output_columns(expr.left(), arena)?;
            out.push(apply.output_column.clone());
            Ok(out)
        }
        Operator::LogicalImvDelta(_) | Operator::LogicalImvVersion(_) => {
            opt_output_columns(expr.unary_input(), arena)
        }
        other if other.is_physical() => Err(format!(
            "subquery rewrite received physical operator {:?}",
            other
        )),
        other => Err(format!(
            "subquery rewrite cannot derive output columns for {:?}",
            other
        )),
    }
}

fn join_output_columns(
    join_type: JoinKind,
    left: Vec<OutputColumn>,
    right: Vec<OutputColumn>,
) -> Vec<OutputColumn> {
    match join_type {
        JoinKind::LeftSemi | JoinKind::LeftAnti | JoinKind::NullAwareLeftAnti => left,
        _ => {
            let mut out = left;
            out.extend(right);
            out
        }
    }
}

pub(super) fn column_ref(arena: &mut ScalarArena, column: &OutputColumn) -> ScalarId {
    arena.remember_project_output_display(column.column_id, None, column.name.clone());
    arena.intern(
        ScalarNode::ColumnRef(column.column_id),
        column.data_type.clone(),
        column.nullable,
    )
}

pub(super) fn project_item_for_column(
    arena: &mut ScalarArena,
    column: &OutputColumn,
) -> ScalarProjectItem {
    ScalarProjectItem {
        expr: column_ref(arena, column),
        output_name: column.name.clone(),
        output_column_id: column.column_id,
        expr_display: None,
    }
}

pub(super) fn bool_literal(arena: &mut ScalarArena, value: bool) -> ScalarId {
    arena.intern(
        ScalarNode::Literal(HashableLiteral(LiteralValue::Bool(value))),
        DataType::Boolean,
        false,
    )
}

pub(super) fn int_literal(arena: &mut ScalarArena, value: i64) -> ScalarId {
    arena.intern(
        ScalarNode::Literal(HashableLiteral(LiteralValue::Int(value))),
        DataType::Int64,
        false,
    )
}

pub(super) fn string_literal(arena: &mut ScalarArena, value: impl Into<String>) -> ScalarId {
    arena.intern(
        ScalarNode::Literal(HashableLiteral(LiteralValue::String(value.into()))),
        DataType::Utf8,
        false,
    )
}

pub(super) fn binary_op(
    arena: &mut ScalarArena,
    op: BinOp,
    left: ScalarId,
    right: ScalarId,
    data_type: DataType,
    nullable: bool,
) -> ScalarId {
    arena.intern(
        ScalarNode::BinaryOp { op, left, right },
        data_type,
        nullable,
    )
}

pub(super) fn eq(arena: &mut ScalarArena, left: ScalarId, right: ScalarId) -> ScalarId {
    binary_op(arena, BinOp::Eq, left, right, DataType::Boolean, false)
}

pub(super) fn combine_and(arena: &mut ScalarArena, exprs: Vec<ScalarId>) -> Option<ScalarId> {
    crate::sql::optimizer::scalar_expr::combine_conjuncts(arena, exprs)
}

pub(super) fn split_and(arena: &ScalarArena, expr: ScalarId) -> Vec<ScalarId> {
    let mut out = Vec::new();
    crate::sql::optimizer::scalar_expr::split_conjuncts(arena, expr, &mut out);
    out
}

pub(super) fn collect_column_ids(arena: &ScalarArena, expr: ScalarId) -> HashSet<ColumnId> {
    crate::sql::optimizer::scalar_expr::collect_column_ids_strict(arena, expr).unwrap_or_default()
}

pub(super) fn scalar_refs_any(
    arena: &ScalarArena,
    expr: ScalarId,
    columns: &HashSet<ColumnId>,
) -> bool {
    !collect_column_ids(arena, expr).is_disjoint(columns)
}

pub(super) fn scalar_refs_only(
    arena: &ScalarArena,
    expr: ScalarId,
    columns: &HashSet<ColumnId>,
) -> bool {
    let refs = collect_column_ids(arena, expr);
    !refs.is_empty() && refs.iter().all(|column_id| columns.contains(column_id))
}

pub(super) fn orient_eq(
    arena: &ScalarArena,
    conjunct: ScalarId,
    corr_ids: &HashSet<ColumnId>,
) -> Option<(ScalarId, ScalarId)> {
    let ScalarNode::BinaryOp {
        left,
        op: BinOp::Eq,
        right,
    } = arena.node(conjunct)
    else {
        return None;
    };
    let left_outer = scalar_refs_any(arena, *left, corr_ids);
    let right_outer = scalar_refs_any(arena, *right, corr_ids);
    match (left_outer, right_outer) {
        (true, false) => Some((*left, *right)),
        (false, true) => Some((*right, *left)),
        _ => None,
    }
}

pub(super) fn is_column_ref(arena: &ScalarArena, expr: ScalarId) -> Option<ColumnId> {
    match arena.node(expr) {
        ScalarNode::ColumnRef(column_id) if *column_id != ColumnId::UNSET => Some(*column_id),
        ScalarNode::Nested(inner) => is_column_ref(arena, *inner),
        _ => None,
    }
}

pub(super) fn find_output_column(
    columns: &[OutputColumn],
    column_id: ColumnId,
) -> Option<&OutputColumn> {
    columns.iter().find(|column| column.column_id == column_id)
}

pub(super) fn find_column_type(
    expr: &OptExpr,
    arena: &ScalarArena,
    column_id: ColumnId,
) -> Option<DataType> {
    find_output_column(&opt_output_columns(expr, arena).ok()?, column_id)
        .map(|column| column.data_type.clone())
}

pub(super) fn find_column_nullable(
    expr: &OptExpr,
    arena: &ScalarArena,
    column_id: ColumnId,
) -> Option<bool> {
    find_output_column(&opt_output_columns(expr, arena).ok()?, column_id)
        .map(|column| column.nullable)
}

pub(super) fn is_count_aggregate_result(
    expr: &OptExpr,
    arena: &ScalarArena,
    column_id: ColumnId,
) -> bool {
    match &expr.op {
        Operator::LogicalAggregate(aggregate) => aggregate
            .aggregates
            .iter()
            .zip(
                aggregate
                    .output_columns
                    .iter()
                    .skip(aggregate.group_by.len()),
            )
            .any(|(call, output)| {
                output.column_id == column_id && call.name.eq_ignore_ascii_case("count")
            }),
        Operator::LogicalProject(project) => {
            let Some(inner_id) = project.items.iter().find_map(|item| {
                if item.output_column_id == column_id {
                    is_column_ref(arena, item.expr)
                } else {
                    None
                }
            }) else {
                return false;
            };
            is_count_aggregate_result(expr.unary_input(), arena, inner_id)
        }
        Operator::LogicalFilter(_) | Operator::LogicalAssertOneRow(_) => {
            is_count_aggregate_result(expr.unary_input(), arena, column_id)
        }
        _ => false,
    }
}

pub(super) fn replace_column_ref(
    arena: &mut ScalarArena,
    expr: ScalarId,
    target: ColumnId,
    replacement: ScalarId,
) -> ScalarId {
    if matches!(arena.node(expr), ScalarNode::ColumnRef(column_id) if *column_id == target) {
        return replacement;
    }
    rewrite_scalar_children(arena, expr, &mut |arena, child| {
        replace_column_ref(arena, child, target, replacement)
    })
}

pub(super) fn remap_column_refs<F>(
    arena: &mut ScalarArena,
    expr: ScalarId,
    remap: &mut F,
) -> Option<ScalarId>
where
    F: FnMut(&mut ScalarArena, ColumnId) -> Option<Option<ScalarId>>,
{
    if let ScalarNode::ColumnRef(column_id) = arena.node(expr)
        && let Some(mapped) = remap(arena, *column_id)?
    {
        return Some(mapped);
    }
    rewrite_scalar_children_result(arena, expr, &mut |arena, child| {
        remap_column_refs(arena, child, remap)
    })
}

fn rewrite_scalar_children<F>(arena: &mut ScalarArena, expr: ScalarId, rewrite: &mut F) -> ScalarId
where
    F: FnMut(&mut ScalarArena, ScalarId) -> ScalarId,
{
    rewrite_scalar_children_result(arena, expr, &mut |arena, child| Some(rewrite(arena, child)))
        .unwrap_or(expr)
}

fn rewrite_scalar_children_result<F>(
    arena: &mut ScalarArena,
    expr: ScalarId,
    rewrite: &mut F,
) -> Option<ScalarId>
where
    F: FnMut(&mut ScalarArena, ScalarId) -> Option<ScalarId>,
{
    let node = arena.node(expr).clone();
    let data_type = arena.data_type(expr).clone();
    let nullable = arena.nullable(expr);
    let rebuilt = match node {
        ScalarNode::BinaryOp { op, left, right } => ScalarNode::BinaryOp {
            op,
            left: rewrite(arena, left)?,
            right: rewrite(arena, right)?,
        },
        ScalarNode::UnaryOp { op, child } => ScalarNode::UnaryOp {
            op,
            child: rewrite(arena, child)?,
        },
        ScalarNode::FunctionCall {
            name,
            args,
            distinct,
        } => ScalarNode::FunctionCall {
            name,
            args: rewrite_vec(arena, args, rewrite)?,
            distinct,
        },
        ScalarNode::LambdaFunction { params, body } => ScalarNode::LambdaFunction {
            params,
            body: rewrite(arena, body)?,
        },
        ScalarNode::AggregateCall {
            name,
            args,
            distinct,
            order_by,
        } => ScalarNode::AggregateCall {
            name,
            args: rewrite_vec(arena, args, rewrite)?,
            distinct,
            order_by: rewrite_sort_keys(arena, order_by, rewrite)?,
        },
        ScalarNode::Cast { child, target } => ScalarNode::Cast {
            child: rewrite(arena, child)?,
            target,
        },
        ScalarNode::IsNull { child, negated } => ScalarNode::IsNull {
            child: rewrite(arena, child)?,
            negated,
        },
        ScalarNode::InList {
            child,
            list,
            negated,
        } => ScalarNode::InList {
            child: rewrite(arena, child)?,
            list: rewrite_vec(arena, list, rewrite)?,
            negated,
        },
        ScalarNode::Between {
            child,
            low,
            high,
            negated,
        } => ScalarNode::Between {
            child: rewrite(arena, child)?,
            low: rewrite(arena, low)?,
            high: rewrite(arena, high)?,
            negated,
        },
        ScalarNode::Like {
            child,
            pattern,
            negated,
        } => ScalarNode::Like {
            child: rewrite(arena, child)?,
            pattern: rewrite(arena, pattern)?,
            negated,
        },
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => ScalarNode::Case {
            operand: match operand {
                Some(item) => Some(rewrite(arena, item)?),
                None => None,
            },
            when_then: when_then
                .into_iter()
                .map(|(when, then)| Some((rewrite(arena, when)?, rewrite(arena, then)?)))
                .collect::<Option<Vec<_>>>()?,
            else_expr: match else_expr {
                Some(item) => Some(rewrite(arena, item)?),
                None => None,
            },
        },
        ScalarNode::IsTruthValue {
            child,
            value,
            negated,
        } => ScalarNode::IsTruthValue {
            child: rewrite(arena, child)?,
            value,
            negated,
        },
        ScalarNode::Nested(child) => ScalarNode::Nested(rewrite(arena, child)?),
        ScalarNode::WindowCall {
            name,
            args,
            distinct,
            partition_by,
            order_by,
            window_frame,
            ignore_nulls,
        } => ScalarNode::WindowCall {
            name,
            args: rewrite_vec(arena, args, rewrite)?,
            distinct,
            partition_by: rewrite_vec(arena, partition_by, rewrite)?,
            order_by: rewrite_sort_keys(arena, order_by, rewrite)?,
            window_frame,
            ignore_nulls,
        },
        ScalarNode::Lambda { params, body } => ScalarNode::Lambda {
            params,
            body: rewrite(arena, body)?,
        },
        ScalarNode::ColumnRef(_) | ScalarNode::LambdaParamRef { .. } | ScalarNode::Literal(_) => {
            return Some(expr);
        }
    };
    Some(arena.intern(rebuilt, data_type, nullable))
}

fn rewrite_vec<F>(
    arena: &mut ScalarArena,
    exprs: Vec<ScalarId>,
    rewrite: &mut F,
) -> Option<Vec<ScalarId>>
where
    F: FnMut(&mut ScalarArena, ScalarId) -> Option<ScalarId>,
{
    exprs.into_iter().map(|expr| rewrite(arena, expr)).collect()
}

fn rewrite_sort_keys<F>(
    arena: &mut ScalarArena,
    keys: Vec<SortKey>,
    rewrite: &mut F,
) -> Option<Vec<SortKey>>
where
    F: FnMut(&mut ScalarArena, ScalarId) -> Option<ScalarId>,
{
    keys.into_iter()
        .map(|key| {
            Some(SortKey {
                expr: rewrite(arena, key.expr)?,
                asc: key.asc,
                nulls_first: key.nulls_first,
                display: key.display,
            })
        })
        .collect()
}

pub(super) fn coalesce_false(arena: &mut ScalarArena, pred: ScalarId) -> ScalarId {
    let false_lit = bool_literal(arena, false);
    arena.intern(
        ScalarNode::FunctionCall {
            name: "coalesce".to_string(),
            args: vec![pred, false_lit],
            distinct: false,
        },
        DataType::Boolean,
        false,
    )
}

pub(super) fn ifnull_zero(
    arena: &mut ScalarArena,
    value: ScalarId,
    result_type: DataType,
) -> ScalarId {
    let zero = int_literal(arena, 0);
    arena.intern(
        ScalarNode::FunctionCall {
            name: "ifnull".to_string(),
            args: vec![value, zero],
            distinct: false,
        },
        result_type,
        false,
    )
}

pub(super) fn assert_true(
    arena: &mut ScalarArena,
    condition: ScalarId,
    message: impl Into<String>,
) -> ScalarId {
    let message = string_literal(arena, message);
    arena.intern(
        ScalarNode::FunctionCall {
            name: "assert_true".to_string(),
            args: vec![condition, message],
            distinct: false,
        },
        DataType::Boolean,
        false,
    )
}

pub(super) fn count_one_spec(arena: &mut ScalarArena) -> ScalarAggregateSpec {
    ScalarAggregateSpec {
        name: "count".to_string(),
        args: vec![int_literal(arena, 1)],
        distinct: false,
        order_by: vec![],
    }
}

pub(super) fn any_value_spec(arg: ScalarId) -> ScalarAggregateSpec {
    ScalarAggregateSpec {
        name: "any_value".to_string(),
        args: vec![arg],
        distinct: false,
        order_by: vec![],
    }
}

pub(super) fn sort_key(expr: ScalarId) -> SortKey {
    SortKey {
        expr,
        asc: true,
        nulls_first: true,
        display: None,
    }
}

pub(super) fn output_for_scalar(
    arena: &ScalarArena,
    column_id: ColumnId,
    name: impl Into<String>,
    scalar: ScalarId,
    is_internal: bool,
) -> OutputColumn {
    OutputColumn {
        column_id,
        name: name.into(),
        data_type: arena.data_type(scalar).clone(),
        nullable: arena.nullable(scalar),
        is_internal,
    }
}

pub(super) fn left_project_items(
    left: &OptExpr,
    arena: &mut ScalarArena,
) -> Result<Vec<ScalarProjectItem>, String> {
    let columns = opt_output_columns(left, arena)?;
    Ok(columns
        .iter()
        .map(|column| project_item_for_column(arena, column))
        .collect())
}

pub(super) fn simple_project(child: OptExpr, items: Vec<ScalarProjectItem>) -> OptExpr {
    OptExpr::new(
        Operator::LogicalProject(crate::sql::optimizer::operator::ProjectOp {
            items,
            output_qualifier: None,
        }),
        vec![child],
    )
}

pub(super) fn filter(child: OptExpr, predicate: ScalarId) -> OptExpr {
    OptExpr::new(Operator::LogicalFilter(FilterOp { predicate }), vec![child])
}

pub(super) fn join(
    left: OptExpr,
    right: OptExpr,
    join_type: JoinKind,
    condition: Option<ScalarId>,
) -> OptExpr {
    OptExpr::new(
        Operator::LogicalJoin(LogicalJoinOp {
            join_type,
            condition,
        }),
        vec![left, right],
    )
}

pub(super) fn apply_kind_is_scalar(kind: &ApplyKind) -> bool {
    *kind == ApplyKind::Scalar
}

pub(super) fn window_spec_from_aggregate(
    name: String,
    args: Vec<ScalarId>,
    _result_type: DataType,
) -> ScalarWindowSpec {
    ScalarWindowSpec {
        name,
        args,
        distinct: false,
        partition_by: vec![],
        order_by: vec![],
        window_frame: None,
        ignore_nulls: false,
    }
}

pub(super) fn scan_column_map(expr: &OptExpr) -> HashMap<ColumnId, (String, String)> {
    let mut map = HashMap::new();
    scan_column_map_inner(expr, &mut map);
    map
}

fn scan_column_map_inner(expr: &OptExpr, map: &mut HashMap<ColumnId, (String, String)>) {
    if let Operator::LogicalScan(scan) = &expr.op {
        let table = scan.table.name.clone();
        for column in &scan.columns {
            map.insert(column.column_id, (table.clone(), column.name.clone()));
        }
        return;
    }
    for child in &expr.children {
        scan_column_map_inner(child, map);
    }
}
