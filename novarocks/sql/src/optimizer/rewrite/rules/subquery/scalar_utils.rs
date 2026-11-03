#![allow(dead_code)] // Staged while subquery rules migrate to OptExpr one by one.
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

use std::collections::{HashMap, HashSet};

use arrow::datatypes::DataType;

use crate::column_id::ColumnId;
use crate::common::ApplyKind;
use crate::common::{BinOp, LiteralValue};
use crate::common::{JoinKind, OutputColumn};
use crate::optimizer::operator::{
    FilterOp, LogicalJoinOp, Operator, ScalarAggregateSpec, ScalarProjectItem,
};
use crate::optimizer::opt_expr::OptExpr;
use crate::optimizer::scalar::{HashableLiteral, ScalarArena, ScalarId, ScalarNode, SortKey};

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
                value_type: arena.value_type(item.expr).clone(),

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
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),

            is_internal: false,
        }]),
        Operator::LogicalCTEProduce(produce) => Ok(produce.output_columns.clone()),
        Operator::LogicalCTEConsume(consume) => Ok(consume.output_columns.clone()),
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
        JoinKind::LeftMark { .. } => {
            let mut output = left;
            output.push(join_type.mark_output().expect("mark output"));
            output
        }
        JoinKind::LeftSemi | JoinKind::LeftAnti | JoinKind::NullAwareLeftAnti => left,
        JoinKind::RightSemi | JoinKind::RightAnti => right,
        JoinKind::LeftOuter => {
            let mut out = left;
            out.extend(make_nullable(right));
            out
        }
        JoinKind::RightOuter => {
            let mut out = make_nullable(left);
            out.extend(right);
            out
        }
        JoinKind::FullOuter => {
            let mut out = make_nullable(left);
            out.extend(make_nullable(right));
            out
        }
        JoinKind::Inner | JoinKind::Cross => {
            let mut out = left;
            out.extend(right);
            out
        }
    }
}

fn make_nullable(mut columns: Vec<OutputColumn>) -> Vec<OutputColumn> {
    for column in &mut columns {
        column.value_type = column
            .value_type
            .with_nullable(true)
            .expect("validated root NULL contract");
    }
    columns
}

pub(super) fn column_ref(
    arena: &mut ScalarArena,
    column: &OutputColumn,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<ScalarId, crate::compiler::SqlCompileError> {
    arena.remember_project_output_display(column.column_id, None, column.name.clone());
    arena.intern_observed(
        ScalarNode::ColumnRef(column.column_id),
        column.value_type.clone(),
        control,
    )
}

pub(super) fn project_item_for_column(
    arena: &mut ScalarArena,
    column: &OutputColumn,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<ScalarProjectItem, crate::compiler::SqlCompileError> {
    Ok(ScalarProjectItem {
        expr: column_ref(arena, column, control)?,
        output_name: column.name.clone(),
        output_column_id: column.column_id,
        expr_display: None,
    })
}

pub(super) fn bool_literal(
    arena: &mut ScalarArena,
    value: bool,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<ScalarId, crate::compiler::SqlCompileError> {
    arena.intern_observed(
        ScalarNode::Literal(HashableLiteral(LiteralValue::Bool(value))),
        novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),
        control,
    )
}

pub(super) fn int_literal(
    arena: &mut ScalarArena,
    value: i64,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<ScalarId, crate::compiler::SqlCompileError> {
    arena.intern_observed(
        ScalarNode::Literal(HashableLiteral(LiteralValue::Int(value))),
        novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
        control,
    )
}

pub(super) fn string_literal(
    arena: &mut ScalarArena,
    value: impl Into<String>,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<ScalarId, crate::compiler::SqlCompileError> {
    arena.intern_observed(
        ScalarNode::Literal(HashableLiteral(LiteralValue::String(value.into()))),
        novarocks_type_contract::FunctionValueType::new(DataType::Utf8, false),
        control,
    )
}

pub(super) fn binary_op(
    arena: &mut ScalarArena,
    op: BinOp,
    left: ScalarId,
    right: ScalarId,
    data_type: DataType,
    nullable: bool,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<ScalarId, crate::compiler::SqlCompileError> {
    arena.intern_observed(
        ScalarNode::BinaryOp {
            op,
            left,
            right,
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        },
        novarocks_type_contract::FunctionValueType::new(data_type, nullable),
        control,
    )
}

pub(super) fn eq(
    arena: &mut ScalarArena,
    left: ScalarId,
    right: ScalarId,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<ScalarId, crate::compiler::SqlCompileError> {
    let nullable = arena.nullable(left) || arena.nullable(right);
    binary_op(
        arena,
        BinOp::Eq,
        left,
        right,
        DataType::Boolean,
        nullable,
        control,
    )
}

pub(super) fn combine_and(
    arena: &mut ScalarArena,
    exprs: Vec<ScalarId>,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Option<ScalarId>, crate::compiler::SqlCompileError> {
    crate::optimizer::scalar_expr::combine_conjuncts(arena, exprs, control)
}

pub(super) fn split_and(arena: &ScalarArena, expr: ScalarId) -> Vec<ScalarId> {
    let mut out = Vec::new();
    crate::optimizer::scalar_expr::split_conjuncts(arena, expr, &mut out);
    out
}

pub(super) fn collect_column_ids(arena: &ScalarArena, expr: ScalarId) -> HashSet<ColumnId> {
    crate::optimizer::scalar_expr::collect_column_ids_strict(arena, expr).unwrap_or_default()
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
        ..
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

pub(super) fn is_count_aggregate_result(
    expr: &OptExpr,
    arena: &ScalarArena,
    column_id: ColumnId,
) -> bool {
    match &expr.op {
        Operator::LogicalAggregate(aggregate) => aggregate
            .aggregates
            .iter()
            .zip(aggregate.output_layout.aggregate_columns.iter())
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

// A rejected candidate short-circuits separately from a typed control failure.
macro_rules! candidate_or_none {
    ($candidate:expr) => {
        match $candidate {
            Some(value) => value,
            None => return Ok(None),
        }
    };
}

pub(super) fn replace_column_ref(
    arena: &mut ScalarArena,
    expr: ScalarId,
    target: ColumnId,
    replacement: ScalarId,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<ScalarId, crate::compiler::SqlCompileError> {
    if matches!(arena.node(expr), ScalarNode::ColumnRef(column_id) if *column_id == target) {
        return Ok(replacement);
    }
    rewrite_scalar_children(
        arena,
        expr,
        &mut |arena, child| replace_column_ref(arena, child, target, replacement, control),
        control,
    )
}
pub(super) fn remap_column_refs<F>(
    arena: &mut ScalarArena,
    expr: ScalarId,
    remap: &mut F,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Option<ScalarId>, crate::compiler::SqlCompileError>
where
    F: FnMut(
        &mut ScalarArena,
        ColumnId,
    ) -> Result<Option<Option<ScalarId>>, crate::compiler::SqlCompileError>,
{
    if let ScalarNode::ColumnRef(column_id) = arena.node(expr) {
        let Some(mapped) = remap(arena, *column_id)? else {
            return Ok(None);
        };
        if let Some(mapped) = mapped {
            return Ok(Some(mapped));
        }
    }
    rewrite_scalar_children_result(
        arena,
        expr,
        &mut |arena, child| remap_column_refs(arena, child, remap, control),
        control,
    )
}
fn rewrite_scalar_children<F>(
    arena: &mut ScalarArena,
    expr: ScalarId,
    rewrite: &mut F,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<ScalarId, crate::compiler::SqlCompileError>
where
    F: FnMut(&mut ScalarArena, ScalarId) -> Result<ScalarId, crate::compiler::SqlCompileError>,
{
    Ok(rewrite_scalar_children_result(
        arena,
        expr,
        &mut |arena, child| Ok(Some(rewrite(arena, child)?)),
        control,
    )?
    .unwrap_or(expr))
}
fn rewrite_scalar_children_result<F>(
    arena: &mut ScalarArena,
    expr: ScalarId,
    rewrite: &mut F,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Option<ScalarId>, crate::compiler::SqlCompileError>
where
    F: FnMut(
        &mut ScalarArena,
        ScalarId,
    ) -> Result<Option<ScalarId>, crate::compiler::SqlCompileError>,
{
    let node = arena.node(expr).clone();
    let value_type = arena.value_type(expr).clone();
    let rebuilt = match node {
        ScalarNode::BinaryOp {
            op,
            left,
            right,
            decimal_overflow_policy,
        } => ScalarNode::BinaryOp {
            op,
            left: candidate_or_none!(rewrite(arena, left)?),
            right: candidate_or_none!(rewrite(arena, right)?),
            decimal_overflow_policy,
        },
        ScalarNode::UnaryOp { op, child } => ScalarNode::UnaryOp {
            op,
            child: candidate_or_none!(rewrite(arena, child)?),
        },
        ScalarNode::FunctionCall {
            name,
            args,
            distinct,
            binding,
            volatility,
        } => ScalarNode::FunctionCall {
            name,
            args: candidate_or_none!(rewrite_vec(arena, args, rewrite, control)?),
            distinct,
            binding,
            volatility,
        },
        ScalarNode::LambdaFunction { params, body } => ScalarNode::LambdaFunction {
            params,
            body: candidate_or_none!(rewrite(arena, body)?),
        },
        ScalarNode::AggregateCall {
            name,
            args,
            distinct,
            order_by,
            resolved,
        } => ScalarNode::AggregateCall {
            name,
            args: candidate_or_none!(rewrite_vec(arena, args, rewrite, control)?),
            distinct,
            order_by: candidate_or_none!(rewrite_sort_keys(arena, order_by, rewrite, control)?),
            resolved,
        },
        ScalarNode::Cast {
            child,
            target,
            decimal_overflow_policy,
        } => ScalarNode::Cast {
            child: candidate_or_none!(rewrite(arena, child)?),
            target,
            decimal_overflow_policy,
        },
        ScalarNode::IsNull { child, negated } => ScalarNode::IsNull {
            child: candidate_or_none!(rewrite(arena, child)?),
            negated,
        },
        ScalarNode::InList {
            child,
            list,
            negated,
        } => ScalarNode::InList {
            child: candidate_or_none!(rewrite(arena, child)?),
            list: candidate_or_none!(rewrite_vec(arena, list, rewrite, control)?),
            negated,
        },
        ScalarNode::Between {
            child,
            low,
            high,
            negated,
        } => ScalarNode::Between {
            child: candidate_or_none!(rewrite(arena, child)?),
            low: candidate_or_none!(rewrite(arena, low)?),
            high: candidate_or_none!(rewrite(arena, high)?),
            negated,
        },
        ScalarNode::Like {
            child,
            pattern,
            negated,
        } => ScalarNode::Like {
            child: candidate_or_none!(rewrite(arena, child)?),
            pattern: candidate_or_none!(rewrite(arena, pattern)?),
            negated,
        },
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => ScalarNode::Case {
            operand: match operand {
                Some(item) => Some(candidate_or_none!(rewrite(arena, item)?)),
                None => None,
            },
            when_then: {
                let mut pairs = Vec::with_capacity(when_then.len());
                for (when, then) in when_then {
                    let when = candidate_or_none!(rewrite(arena, when)?);
                    let then = candidate_or_none!(rewrite(arena, then)?);
                    pairs.push((when, then));
                }
                pairs
            },
            else_expr: match else_expr {
                Some(item) => Some(candidate_or_none!(rewrite(arena, item)?)),
                None => None,
            },
        },
        ScalarNode::IsTruthValue {
            child,
            value,
            negated,
        } => ScalarNode::IsTruthValue {
            child: candidate_or_none!(rewrite(arena, child)?),
            value,
            negated,
        },
        ScalarNode::Nested(child) => ScalarNode::Nested(candidate_or_none!(rewrite(arena, child)?)),
        ScalarNode::WindowCall {
            name,
            args,
            distinct,
            binding,
            function_order_by,
            aggregate_binding,
            partition_by,
            order_by,
            window_frame,
            ignore_nulls,
        } => ScalarNode::WindowCall {
            name,
            args: candidate_or_none!(rewrite_vec(arena, args, rewrite, control)?),
            distinct,
            binding,
            function_order_by: candidate_or_none!(rewrite_sort_keys(
                arena,
                function_order_by,
                rewrite,
                control
            )?),
            aggregate_binding,
            partition_by: candidate_or_none!(rewrite_vec(arena, partition_by, rewrite, control)?),
            order_by: candidate_or_none!(rewrite_sort_keys(arena, order_by, rewrite, control)?),
            window_frame,
            ignore_nulls,
        },
        ScalarNode::Lambda { params, body } => ScalarNode::Lambda {
            params,
            body: candidate_or_none!(rewrite(arena, body)?),
        },
        ScalarNode::ColumnRef(_)
        | ScalarNode::LambdaParamRef { .. }
        | ScalarNode::Literal(_)
        | ScalarNode::Constant(_) => {
            return Ok(Some(expr));
        }
    };
    Ok(Some(arena.intern_observed(rebuilt, value_type, control)?))
}

fn rewrite_vec<F>(
    arena: &mut ScalarArena,
    exprs: Vec<ScalarId>,
    rewrite: &mut F,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Option<Vec<ScalarId>>, crate::compiler::SqlCompileError>
where
    F: FnMut(
        &mut ScalarArena,
        ScalarId,
    ) -> Result<Option<ScalarId>, crate::compiler::SqlCompileError>,
{
    let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
        control,
        novarocks_type_contract::CompilePhase::Validate,
    )?;
    let mut out = Vec::with_capacity(exprs.len());
    for expr in exprs {
        work.step()?;
        let Some(expr) = rewrite(arena, expr)? else {
            work.finish()?;
            return Ok(None);
        };
        out.push(expr);
    }
    work.finish()?;
    Ok(Some(out))
}

fn rewrite_sort_keys<F>(
    arena: &mut ScalarArena,
    keys: Vec<SortKey>,
    rewrite: &mut F,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Option<Vec<SortKey>>, crate::compiler::SqlCompileError>
where
    F: FnMut(
        &mut ScalarArena,
        ScalarId,
    ) -> Result<Option<ScalarId>, crate::compiler::SqlCompileError>,
{
    let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
        control,
        novarocks_type_contract::CompilePhase::Validate,
    )?;
    let mut out = Vec::with_capacity(keys.len());
    for key in keys {
        work.step()?;
        let Some(expr) = rewrite(arena, key.expr)? else {
            work.finish()?;
            return Ok(None);
        };
        out.push(SortKey {
            expr,
            asc: key.asc,
            nulls_first: key.nulls_first,
            display: key.display,
        });
    }
    work.finish()?;
    Ok(Some(out))
}

pub(super) fn coalesce_false(
    function_catalog: &dyn crate::compiler::SqlFunctionCatalog,
    arena: &mut ScalarArena,
    pred: ScalarId,
    policy: novarocks_type_contract::DecimalOverflowPolicy,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<ScalarId, crate::compiler::SqlCompileError> {
    let false_lit = bool_literal(arena, false, control)?;
    let args = vec![pred, false_lit];
    let binding = crate::optimizer::scalar::resolve_function_binding(
        function_catalog,
        arena,
        "coalesce",
        &args,
        policy,
        control,
    )?;
    arena.intern_observed(
        ScalarNode::FunctionCall {
            volatility: crate::functions::FunctionVolatility::Immutable,
            name: "coalesce".to_string(),
            args,
            distinct: false,
            binding,
        },
        novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),
        control,
    )
}

pub(super) fn ifnull_zero(
    function_catalog: &dyn crate::compiler::SqlFunctionCatalog,
    arena: &mut ScalarArena,
    value: ScalarId,
    result_type: DataType,
    policy: novarocks_type_contract::DecimalOverflowPolicy,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<ScalarId, crate::compiler::SqlCompileError> {
    let zero = int_literal(arena, 0, control)?;
    let args = vec![value, zero];
    let binding = crate::optimizer::scalar::resolve_function_binding(
        function_catalog,
        arena,
        "ifnull",
        &args,
        policy,
        control,
    )?;
    arena.intern_observed(
        ScalarNode::FunctionCall {
            volatility: crate::functions::FunctionVolatility::Immutable,
            name: "ifnull".to_string(),
            args,
            distinct: false,
            binding,
        },
        novarocks_type_contract::FunctionValueType::new(result_type, false),
        control,
    )
}

pub(super) fn assert_true(
    function_catalog: &dyn crate::compiler::SqlFunctionCatalog,
    arena: &mut ScalarArena,
    condition: ScalarId,
    message: impl Into<String>,
    policy: novarocks_type_contract::DecimalOverflowPolicy,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<ScalarId, crate::compiler::SqlCompileError> {
    let message = string_literal(arena, message, control)?;
    let args = vec![condition, message];
    let binding = crate::optimizer::scalar::resolve_function_binding(
        function_catalog,
        arena,
        "assert_true",
        &args,
        policy,
        control,
    )?;
    arena.intern_observed(
        ScalarNode::FunctionCall {
            volatility: crate::functions::FunctionVolatility::Immutable,
            name: "assert_true".to_string(),
            args,
            distinct: false,
            binding,
        },
        novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),
        control,
    )
}

pub(super) fn count_one_spec(
    arena: &mut ScalarArena,
    output_column_id: ColumnId,
    resolved: crate::binding::SqlFunctionBinding,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<ScalarAggregateSpec, crate::compiler::SqlCompileError> {
    Ok(ScalarAggregateSpec {
        output_column_id,
        name: "count".to_string(),
        distinct: false,
        source: crate::binding::AggregateArgumentSource::logical_update(
            vec![int_literal(arena, 1, control)?],
            vec![],
            resolved,
        ),
    })
}

pub(super) fn any_value_spec(
    arg: ScalarId,
    output_column_id: ColumnId,
    resolved: crate::binding::SqlFunctionBinding,
) -> ScalarAggregateSpec {
    ScalarAggregateSpec {
        output_column_id,
        name: "any_value".to_string(),
        distinct: false,
        source: crate::binding::AggregateArgumentSource::logical_update(
            vec![arg],
            vec![],
            resolved,
        ),
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
        value_type: arena.value_type(scalar).clone(),

        is_internal,
    }
}

pub(super) fn left_project_items(
    left: &OptExpr,
    arena: &mut ScalarArena,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Vec<ScalarProjectItem>, crate::compiler::SqlCompileError> {
    let columns =
        opt_output_columns(left, arena).map_err(crate::compiler::SqlCompileError::Compilation)?;
    columns
        .iter()
        .map(|column| project_item_for_column(arena, column, control))
        .collect()
}

pub(super) fn simple_project(child: OptExpr, items: Vec<ScalarProjectItem>) -> OptExpr {
    OptExpr::new(
        Operator::LogicalProject(crate::optimizer::operator::ProjectOp {
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
            naa_author: None,
            join_type,
            condition,
        }),
        vec![left, right],
    )
}

pub(super) fn apply_kind_is_scalar(kind: &ApplyKind) -> bool {
    *kind == ApplyKind::Scalar
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer::operator::{AggregateOutputLayout, LogicalAggregateOp};

    fn output_column(column_id: ColumnId, name: &str) -> OutputColumn {
        OutputColumn {
            column_id,
            name: name.to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),

            is_internal: false,
        }
    }

    #[test]
    fn is_count_aggregate_result_uses_layout_when_group_key_is_hidden() {
        let mut arena = ScalarArena::new();
        let group_id = ColumnId::new_for_test(1);
        let count_id = ColumnId::new_for_test(2);
        let group = arena.intern(
            ScalarNode::ColumnRef(group_id),
            novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
        );
        let aggregate = OptExpr::new(
            Operator::LogicalAggregate(LogicalAggregateOp::single(
                vec![group],
                vec![ScalarAggregateSpec {
                    output_column_id: count_id,
                    name: "count".to_string(),
                    distinct: false,
                    source: crate::binding::AggregateArgumentSource::uncertified(
                        vec![],
                        vec![],
                        crate::functions::test_resolved_aggregate("count", &[], false),
                    ),
                }],
                AggregateOutputLayout::new(
                    vec![output_column(group_id, "k")],
                    vec![output_column(count_id, "count(1)")],
                ),
                vec![output_column(count_id, "count(1)")],
            )),
            vec![],
        );

        assert!(is_count_aggregate_result(&aggregate, &arena, count_id));
    }

    #[test]
    fn opt_output_columns_widens_outer_join_nullable_side() {
        let arena = ScalarArena::new();
        let left_col = output_column(ColumnId::new_for_test(1), "l");
        let right_col = output_column(ColumnId::new_for_test(2), "r");
        let left = OptExpr::new(
            Operator::LogicalValues(crate::optimizer::operator::ValuesOp {
                columns: vec![left_col.clone()],
                rows: vec![],
            }),
            vec![],
        );
        let right = OptExpr::new(
            Operator::LogicalValues(crate::optimizer::operator::ValuesOp {
                columns: vec![right_col.clone()],
                rows: vec![],
            }),
            vec![],
        );

        let left_outer = opt_output_columns(
            &join(left.clone(), right.clone(), JoinKind::LeftOuter, None),
            &arena,
        )
        .expect("left outer output columns");
        assert!(!left_outer[0].value_type.nullable());
        assert!(left_outer[1].value_type.nullable());

        let right_outer = opt_output_columns(
            &join(left.clone(), right.clone(), JoinKind::RightOuter, None),
            &arena,
        )
        .expect("right outer output columns");
        assert!(right_outer[0].value_type.nullable());
        assert!(!right_outer[1].value_type.nullable());

        let full_outer = opt_output_columns(&join(left, right, JoinKind::FullOuter, None), &arena)
            .expect("full outer output columns");
        assert!(full_outer[0].value_type.nullable());
        assert!(full_outer[1].value_type.nullable());
    }
    struct CheckControl {
        checks: std::sync::Mutex<Vec<u32>>,
        fail_check: Option<usize>,
        fail_after_work: Option<u64>,
        reason: novarocks_type_contract::CompileControlError,
    }
    impl novarocks_type_contract::PureCompileControl for CheckControl {
        fn checkpoint(
            &self,
            _: novarocks_type_contract::CompilePhase,
            units: u32,
        ) -> Result<(), novarocks_type_contract::CompileControlError> {
            assert!(units <= novarocks_type_contract::MAX_UNOBSERVED_COMPILE_WORK);
            let mut checks = self.checks.lock().unwrap();
            checks.push(units);
            let work: u64 = checks.iter().map(|units| u64::from(*units)).sum();
            if self.fail_check == Some(checks.len())
                || self.fail_after_work.is_some_and(|limit| work >= limit)
            {
                return Err(self.reason.clone());
            }
            Ok(())
        }
    }
    fn checked_control(
        fail_check: Option<usize>,
        fail_after_work: Option<u64>,
        reason: novarocks_type_contract::CompileControlError,
    ) -> CheckControl {
        CheckControl {
            checks: Default::default(),
            fail_check,
            fail_after_work,
            reason,
        }
    }
    fn assert_original_control(
        error: crate::compiler::SqlCompileError,
        reason: novarocks_type_contract::CompileControlError,
    ) {
        match reason {
            novarocks_type_contract::CompileControlError::Source(source) => {
                assert_eq!(error.source_failure(), Some(&source));
            }
            novarocks_type_contract::CompileControlError::Cancelled => {
                assert!(matches!(error, crate::compiler::SqlCompileError::Cancelled))
            }
            novarocks_type_contract::CompileControlError::DeadlineExceeded => assert!(matches!(
                error,
                crate::compiler::SqlCompileError::DeadlineExceeded
            )),
            novarocks_type_contract::CompileControlError::ResourceExhausted => assert!(matches!(
                error,
                crate::compiler::SqlCompileError::ResourceExhausted
            )),
        }
    }
    #[test]
    fn observed_projection_interning_keeps_original_control_at_entry_interior_and_tail() {
        use novarocks_type_contract::CompileControlError as Reason;
        let columns: Vec<_> = (1..=320)
            .map(|id| output_column(ColumnId::new_for_test(id), &format!("c{id}")))
            .collect();
        let left = OptExpr::new(
            Operator::LogicalValues(crate::optimizer::operator::ValuesOp {
                columns,
                rows: vec![],
            }),
            vec![],
        );
        let baseline = checked_control(None, None, Reason::Cancelled);
        assert_eq!(
            left_project_items(&left, &mut ScalarArena::new(), &baseline)
                .unwrap()
                .len(),
            320
        );
        let check_count = baseline.checks.lock().unwrap().len();
        assert!(
            baseline
                .checks
                .lock()
                .unwrap()
                .iter()
                .map(|units| u64::from(*units))
                .sum::<u64>()
                > 256
        );
        for reason in [
            Reason::Cancelled,
            Reason::DeadlineExceeded,
            Reason::ResourceExhausted,
        ] {
            for (check, work) in [
                (Some(1), None),
                (None, Some(256)),
                (Some(check_count), None),
            ] {
                let control = checked_control(check, work, reason.clone());
                let error =
                    left_project_items(&left, &mut ScalarArena::new(), &control).unwrap_err();
                assert_original_control(error, reason.clone());
            }
        }
    }
    #[test]
    fn observed_recursive_remap_preserves_full_json_domain_and_first_none_short_circuit() {
        use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
        let json =
            FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
                .unwrap();
        let mut arena = ScalarArena::new();
        let source = arena.intern(
            ScalarNode::ColumnRef(ColumnId::new_for_test(1)),
            json.clone(),
        );
        let replacement = arena.intern(
            ScalarNode::ColumnRef(ColumnId::new_for_test(2)),
            json.clone(),
        );
        let nested = arena.intern(ScalarNode::Nested(source), json.clone());
        let control = crate::optimizer::rewrite::context::unbounded_rewrite_test_control();
        let rebuilt = replace_column_ref(
            &mut arena,
            nested,
            ColumnId::new_for_test(1),
            replacement,
            control,
        )
        .unwrap();
        assert_eq!(arena.value_type(rebuilt), &json);
        assert!(matches!(arena.node(rebuilt),ScalarNode::Nested(id) if *id==replacement));
        let pair = arena.intern(
            ScalarNode::BinaryOp {
                left: source,
                right: replacement,
                op: BinOp::Eq,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            FunctionValueType::new(DataType::Boolean, false),
        );
        let mut calls = 0;
        let rejected = remap_column_refs(
            &mut arena,
            pair,
            &mut |_, _| {
                calls += 1;
                Ok(None)
            },
            control,
        )
        .unwrap();
        assert!(rejected.is_none());
        assert_eq!(calls, 1);
        for reason in [
            novarocks_type_contract::CompileControlError::Cancelled,
            novarocks_type_contract::CompileControlError::DeadlineExceeded,
            novarocks_type_contract::CompileControlError::ResourceExhausted,
        ] {
            let error = remap_column_refs(
                &mut arena,
                pair,
                &mut |_, _| Err(reason.clone().into()),
                control,
            )
            .unwrap_err();
            assert_original_control(error, reason);
        }
    }
}
