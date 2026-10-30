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

//! Logical-property derivation for optimizer Memo groups.

use std::collections::HashMap;

use super::memo::{GroupId, LogicalProperties, MExpr, Memo};
use super::operator::Operator;
use super::property::ColumnIdSet;
use super::statistics::{ColumnStatistic, Confidence};
use crate::column_id::ColumnId;
use crate::common::{BinOp, JoinKind, OutputColumn};
use crate::optimizer::scalar::{HashableLiteral, ScalarArena, ScalarId, ScalarNode};
use arrow::datatypes::DataType;

pub(crate) fn derive_for_group(
    memo: &Memo,
    group_idx: GroupId,
    output_columns: Vec<OutputColumn>,
    row_count: f64,
    row_count_confidence: Confidence,
    column_statistics: HashMap<ColumnId, ColumnStatistic>,
) -> LogicalProperties {
    let group = &memo.groups[group_idx];
    let expr = group.logical_exprs.first().or(group.physical_exprs.first());
    let Some(expr) = expr else {
        let mut props = LogicalProperties::new(output_columns, row_count);
        props.row_count_confidence = row_count_confidence;
        props.column_statistics = column_statistics;
        return props;
    };
    derive_for_expr(
        expr,
        memo,
        output_columns,
        row_count,
        row_count_confidence,
        column_statistics,
    )
}

pub(crate) fn derive_for_expr(
    expr: &MExpr,
    memo: &Memo,
    output_columns: Vec<OutputColumn>,
    row_count: f64,
    row_count_confidence: Confidence,
    column_statistics: HashMap<ColumnId, ColumnStatistic>,
) -> LogicalProperties {
    let output_ids = output_id_set(&output_columns);
    let mut props = LogicalProperties::new(output_columns, row_count);
    props.row_count_confidence = row_count_confidence;
    props.column_statistics = column_statistics;

    match &expr.op {
        Operator::LogicalFilter(filter) => {
            inherit_from_child(memo, expr, 0, &output_ids, &mut props);
            for (left, right) in collect_strict_column_equalities(&memo.scalars, filter.predicate) {
                props.equivalence_classes.merge_pair(left, right);
            }
        }
        Operator::PhysicalFilter(filter) => {
            inherit_from_child(memo, expr, 0, &output_ids, &mut props);
            for (left, right) in collect_strict_column_equalities(&memo.scalars, filter.predicate) {
                props.equivalence_classes.merge_pair(left, right);
            }
        }
        Operator::LogicalJoin(join) => {
            if join.join_type == JoinKind::Inner {
                inherit_from_child(memo, expr, 0, &output_ids, &mut props);
                inherit_from_child(memo, expr, 1, &output_ids, &mut props);
                if let Some(condition) = &join.condition {
                    for (left, right) in collect_strict_column_equalities(&memo.scalars, *condition)
                    {
                        props.equivalence_classes.merge_pair(left, right);
                    }
                }
            }
            props.equivalence_classes.retain_subset_of(&output_ids);
        }
        Operator::PhysicalHashJoin(join) => {
            if join.join_type == JoinKind::Inner {
                inherit_from_child(memo, expr, 0, &output_ids, &mut props);
                inherit_from_child(memo, expr, 1, &output_ids, &mut props);
                for eq in join.condition.keys() {
                    if eq.null_safe {
                        continue;
                    }
                    if let (Some(left), Some(right)) = (
                        column_id_from_scalar(&memo.scalars, eq.left),
                        column_id_from_scalar(&memo.scalars, eq.right),
                    ) {
                        props.equivalence_classes.merge_pair(left, right);
                    }
                }
                if let Some(condition) = join.condition.residual() {
                    for (left, right) in collect_strict_column_equalities(&memo.scalars, *condition)
                    {
                        props.equivalence_classes.merge_pair(left, right);
                    }
                }
            }
            props.equivalence_classes.retain_subset_of(&output_ids);
        }
        Operator::LogicalAggregate(agg) => {
            let key = ColumnIdSet::from_columns(
                agg.output_columns
                    .iter()
                    .take(agg.group_by.len())
                    .map(|column| column.column_id),
            );
            if !key.is_empty() {
                props.unique_columns.push(key);
            }
        }
        Operator::PhysicalHashAggregate(agg) => {
            let key = ColumnIdSet::from_columns(
                agg.output_columns
                    .iter()
                    .take(agg.group_by.len())
                    .map(|column| column.column_id),
            );
            if !key.is_empty() {
                props.unique_columns.push(key);
            }
        }
        Operator::LogicalProject(_)
        | Operator::PhysicalProject(_)
        | Operator::LogicalSort(_)
        | Operator::PhysicalSort(_)
        | Operator::LogicalLimit(_)
        | Operator::PhysicalLimit(_)
        | Operator::LogicalTopN(_)
        | Operator::PhysicalTopN(_)
        | Operator::LogicalWindow(_)
        | Operator::PhysicalWindow(_)
        | Operator::LogicalTableFunction(_)
        | Operator::PhysicalTableFunction(_)
        | Operator::LogicalCTEProduce(_)
        | Operator::PhysicalCTEProduce(_)
        | Operator::PhysicalDistribution(_)
        | Operator::LogicalAssertOneRow(_)
        | Operator::PhysicalAssertOneRow(_) => {
            inherit_from_child(memo, expr, 0, &output_ids, &mut props);
        }
        _ => {}
    }

    props.equivalence_classes.retain_subset_of(&output_ids);
    props
        .unique_columns
        .retain(|key| !key.is_empty() && key.is_subset(&output_ids));
    props
}

fn inherit_from_child(
    memo: &Memo,
    expr: &MExpr,
    child_slot: usize,
    output_ids: &ColumnIdSet,
    props: &mut LogicalProperties,
) {
    let Some(child_group_id) = expr.children.get(child_slot).copied() else {
        return;
    };
    let Some(child_props) = memo.groups[child_group_id].logical_props.as_ref() else {
        return;
    };
    props
        .equivalence_classes
        .extend_from(&child_props.equivalence_classes);
    for key in &child_props.unique_columns {
        if key.is_subset(output_ids) {
            props.unique_columns.push(key.clone());
        }
    }
    props.equivalence_classes.retain_subset_of(output_ids);
}

fn output_id_set(output_columns: &[OutputColumn]) -> ColumnIdSet {
    ColumnIdSet::from_columns(output_columns.iter().map(|column| column.column_id))
}

pub(crate) fn column_id_from_scalar(scalars: &ScalarArena, expr: ScalarId) -> Option<ColumnId> {
    match scalars.node(expr) {
        ScalarNode::ColumnRef(column_id) if *column_id != ColumnId::UNSET => Some(*column_id),
        ScalarNode::Nested(inner) => column_id_from_scalar(scalars, *inner),
        _ => None,
    }
}

pub(crate) fn collect_strict_column_equalities(
    scalars: &ScalarArena,
    expr: ScalarId,
) -> Vec<(ColumnId, ColumnId)> {
    let mut out = Vec::new();
    collect_strict_column_equalities_inner(scalars, expr, &mut out);
    out
}

fn collect_strict_column_equalities_inner(
    scalars: &ScalarArena,
    expr: ScalarId,
    out: &mut Vec<(ColumnId, ColumnId)>,
) {
    match scalars.node(expr) {
        ScalarNode::Nested(inner) => collect_strict_column_equalities_inner(scalars, *inner, out),
        ScalarNode::BinaryOp {
            left,
            op: BinOp::And,
            right,
            ..
        } => {
            collect_strict_column_equalities_inner(scalars, *left, out);
            collect_strict_column_equalities_inner(scalars, *right, out);
        }
        ScalarNode::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
            ..
        } => {
            if let (Some(left_id), Some(right_id)) = (
                column_id_from_scalar(scalars, *left),
                column_id_from_scalar(scalars, *right),
            ) {
                out.push((left_id, right_id));
            }
        }
        _ => {}
    }
}

#[derive(Clone, Debug)]
pub(crate) struct LiteralEquality {
    pub(crate) column_id: ColumnId,
    pub(crate) literal: ScalarId,
}

pub(crate) fn collect_literal_equalities(
    scalars: &ScalarArena,
    expr: ScalarId,
) -> Vec<LiteralEquality> {
    let mut out = Vec::new();
    collect_literal_equalities_inner(scalars, expr, &mut out);
    out
}

fn collect_literal_equalities_inner(
    scalars: &ScalarArena,
    expr: ScalarId,
    out: &mut Vec<LiteralEquality>,
) {
    match scalars.node(expr) {
        ScalarNode::Nested(inner) => collect_literal_equalities_inner(scalars, *inner, out),
        ScalarNode::BinaryOp {
            left,
            op: BinOp::And,
            right,
            ..
        } => {
            collect_literal_equalities_inner(scalars, *left, out);
            collect_literal_equalities_inner(scalars, *right, out);
        }
        ScalarNode::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
            ..
        } => match (scalars.node(*left), scalars.node(*right)) {
            (
                ScalarNode::ColumnRef(column_id),
                ScalarNode::Literal(_) | ScalarNode::Constant(_),
            ) if *column_id != ColumnId::UNSET => {
                out.push(LiteralEquality {
                    column_id: *column_id,
                    literal: *right,
                });
            }
            (
                ScalarNode::Literal(_) | ScalarNode::Constant(_),
                ScalarNode::ColumnRef(column_id),
            ) if *column_id != ColumnId::UNSET => {
                out.push(LiteralEquality {
                    column_id: *column_id,
                    literal: *left,
                });
            }
            _ => {}
        },
        _ => {}
    }
}

pub(crate) fn make_column_ref_expr(
    arena: &mut ScalarArena,
    column: &OutputColumn,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<ScalarId, crate::compiler::SqlCompileError> {
    let result = arena.intern_observed(
        ScalarNode::ColumnRef(column.column_id),
        column.value_type.clone(),
        control,
    )?;
    arena.remember_source_column_display(column.column_id, None, column.name.clone());
    Ok(result)
}

pub(crate) fn make_eq_literal_predicate(
    arena: &mut ScalarArena,
    column: &OutputColumn,
    literal: ScalarId,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<ScalarId, crate::compiler::SqlCompileError> {
    let left = make_column_ref_expr(arena, column, control)?;
    arena.intern_observed(
        ScalarNode::BinaryOp {
            left,
            op: BinOp::Eq,
            right: literal,
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        },
        novarocks_type_contract::FunctionValueType::new(
            DataType::Boolean,
            column.value_type.nullable() || arena.nullable(literal),
        ),
        control,
    )
}

pub(crate) fn combine_with_and(
    arena: &mut ScalarArena,
    predicates: Vec<ScalarId>,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Option<ScalarId>, crate::compiler::SqlCompileError> {
    control.checkpoint(novarocks_type_contract::CompilePhase::Validate, 0)?;
    let mut predicates = predicates.into_iter();
    let Some(mut left) = predicates.next() else {
        return Ok(None);
    };
    for right in predicates {
        left = arena.intern_observed(
            ScalarNode::BinaryOp {
                left,
                op: BinOp::And,
                right,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            novarocks_type_contract::FunctionValueType::new(
                DataType::Boolean,
                arena.nullable(left) || arena.nullable(right),
            ),
            control,
        )?;
    }
    control.checkpoint(novarocks_type_contract::CompilePhase::Validate, 0)?;
    Ok(Some(left))
}

/// Compare literal leaves without rendering payloads or source pool identities.
/// Syntax leaves retain their existing representation-sensitive rules; they do
/// not implicitly equal materialized constants during this migration stage.
pub(crate) fn literal_equal_observed(
    left_arena: &ScalarArena,
    left: ScalarId,
    right_arena: &ScalarArena,
    right: ScalarId,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<bool, crate::compiler::SqlCompileError> {
    use crate::compiler::SqlCompileError;
    use novarocks_type_contract::{CompileCheckpoints, CompilePhase};
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let result = (|| {
        let same_type = left_arena
            .value_type(left)
            .exactly_equals_observed::<novarocks_functions::ConstantError>(
                right_arena.value_type(right),
                || work.step().map_err(Into::into),
            )
            .map_err(SqlCompileError::from)?;
        if !same_type {
            return Ok(false);
        }
        let equal = match (left_arena.node(left), right_arena.node(right)) {
            (ScalarNode::Constant(left), ScalarNode::Constant(right)) => {
                work.flush()?;
                left.equals_observed(right, CompilePhase::Validate, work.control())?
            }
            (
                ScalarNode::Literal(HashableLiteral(left)),
                ScalarNode::Literal(HashableLiteral(right)),
            ) => {
                use crate::common::LiteralValue;
                match (left, right) {
                    (LiteralValue::Float(left), LiteralValue::Float(right)) => {
                        // Old syntax signatures rendered every NaN as NaN, but
                        // retained the sign of zero. This is not CV equality.
                        (left.is_nan() && right.is_nan()) || left.to_bits() == right.to_bits()
                    }
                    (LiteralValue::String(left), LiteralValue::String(right))
                    | (LiteralValue::Decimal(left), LiteralValue::Decimal(right)) => {
                        equal_literal_bytes(left.as_bytes(), right.as_bytes(), &mut work)?
                    }
                    (LiteralValue::Binary(left), LiteralValue::Binary(right)) => {
                        equal_literal_bytes(left, right, &mut work)?
                    }
                    (left, right) => left == right,
                }
            }
            _ => false,
        };
        work.step()?;
        Ok(equal)
    })();
    if matches!(
        &result,
        Err(SqlCompileError::Cancelled
            | SqlCompileError::DeadlineExceeded
            | SqlCompileError::ResourceExhausted)
    ) {
        return result;
    }
    work.finish()?;
    result
}

fn equal_literal_bytes(
    left: &[u8],
    right: &[u8],
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<bool, crate::compiler::SqlCompileError> {
    let same_len = left.len() == right.len();
    work.step()?;
    if !same_len {
        return Ok(false);
    }
    for (left, right) in left.chunks(1024).zip(right.chunks(1024)) {
        let equal = left == right;
        work.step()?;
        if !equal {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::LiteralValue;
    use crate::optimizer::memo::MExpr;
    use crate::optimizer::operator::{
        FilterOp, JoinDistribution, LogicalJoinOp, PhysicalHashJoinEqCondition, PhysicalHashJoinOp,
        ScanOp,
    };
    use crate::planner::table::TableDef;

    fn col(arena: &mut ScalarArena, id: u32) -> ScalarId {
        arena.intern(
            ScalarNode::ColumnRef(ColumnId(id)),
            novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
        )
    }

    fn lit(arena: &mut ScalarArena, value: i64) -> ScalarId {
        arena.intern(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Int(value))),
            novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
        )
    }

    fn binary(arena: &mut ScalarArena, op: BinOp, left: ScalarId, right: ScalarId) -> ScalarId {
        arena.intern(
            ScalarNode::BinaryOp {
                left,
                op,
                right,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),
        )
    }

    fn eq(arena: &mut ScalarArena, left: ScalarId, right: ScalarId) -> ScalarId {
        binary(arena, BinOp::Eq, left, right)
    }

    fn eq_for_null(arena: &mut ScalarArena, left: ScalarId, right: ScalarId) -> ScalarId {
        binary(arena, BinOp::EqForNull, left, right)
    }

    fn and(arena: &mut ScalarArena, left: ScalarId, right: ScalarId) -> ScalarId {
        binary(arena, BinOp::And, left, right)
    }

    fn eq_cols(arena: &mut ScalarArena, left: u32, right: u32) -> ScalarId {
        let left = col(arena, left);
        let right = col(arena, right);
        eq(arena, left, right)
    }

    fn null_safe_eq_cols(arena: &mut ScalarArena, left: u32, right: u32) -> ScalarId {
        let left = col(arena, left);
        let right = col(arena, right);
        eq_for_null(arena, left, right)
    }

    fn eq_col_lit(arena: &mut ScalarArena, column: u32, value: i64) -> ScalarId {
        let column = col(arena, column);
        let value = lit(arena, value);
        eq(arena, column, value)
    }

    fn output(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),

            is_internal: false,
        }
    }

    fn scan_group(memo: &mut Memo, id: u32, name: &str) -> GroupId {
        memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalScan(ScanOp {
                database: "db".to_string(),
                table: TableDef {
                    name: format!("t{id}"),
                    columns: Vec::new(),
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source: crate::compiler::mv_rewrite::test_scan_source(
                        crate::planner::table::SqlScanKind::ConnectorRead,
                    ),
                },
                alias: None,
                stats_ref: None,
                columns: vec![output(id, name)],
                predicates: Vec::new(),
                required_columns: None,
                variant_columns: Vec::new(),
                mv_rewritten_from: None,
            }),
            children: Vec::new(),
        })
    }

    #[test]
    fn collect_strict_column_equalities_reads_top_level_and() {
        let mut arena = ScalarArena::new();
        let col_eq = eq_cols(&mut arena, 1, 2);
        let literal_eq = eq_col_lit(&mut arena, 1, 10);
        let predicate = and(&mut arena, col_eq, literal_eq);
        assert_eq!(
            collect_strict_column_equalities(&arena, predicate),
            vec![(ColumnId(1), ColumnId(2))]
        );
        assert_eq!(collect_literal_equalities(&arena, predicate).len(), 1);
    }

    #[test]
    fn collect_strict_column_equalities_ignores_null_safe_equality() {
        let mut arena = ScalarArena::new();
        let strict_eq = eq_cols(&mut arena, 1, 2);
        let null_safe_eq = null_safe_eq_cols(&mut arena, 3, 4);
        let predicate = and(&mut arena, strict_eq, null_safe_eq);
        assert_eq!(
            collect_strict_column_equalities(&arena, predicate),
            vec![(ColumnId(1), ColumnId(2))]
        );
    }

    #[test]
    fn filter_derivation_merges_column_equality() {
        let mut memo = Memo::new();
        let child = scan_group(&mut memo, 1, "a");
        memo.groups[child].logical_props = Some(LogicalProperties::new(
            vec![output(1, "a"), output(2, "b")],
            100.0,
        ));
        let predicate = eq_cols(&mut memo.scalars, 1, 2);
        let filter = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalFilter(FilterOp { predicate }),
            children: vec![child],
        });
        let props = derive_for_group(
            &memo,
            filter,
            vec![output(1, "a"), output(2, "b")],
            50.0,
            Confidence::Estimated,
            std::collections::HashMap::new(),
        );
        let class = props
            .equivalence_classes
            .class_containing(ColumnId(1))
            .expect("filter equivalence class");
        assert_eq!(
            class.iter().collect::<Vec<_>>(),
            vec![ColumnId(1), ColumnId(2)]
        );
    }

    #[test]
    fn filter_derivation_ignores_null_safe_column_equality() {
        let mut memo = Memo::new();
        let child = scan_group(&mut memo, 1, "a");
        memo.groups[child].logical_props = Some(LogicalProperties::new(
            vec![output(1, "a"), output(2, "b")],
            100.0,
        ));
        let predicate = null_safe_eq_cols(&mut memo.scalars, 1, 2);
        let filter = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalFilter(FilterOp { predicate }),
            children: vec![child],
        });
        let props = derive_for_group(
            &memo,
            filter,
            vec![output(1, "a"), output(2, "b")],
            50.0,
            Confidence::Estimated,
            std::collections::HashMap::new(),
        );
        assert!(
            props
                .equivalence_classes
                .class_containing(ColumnId(1))
                .is_none(),
            "null-safe equality must not populate the strict equivalence store"
        );
    }

    #[test]
    fn inner_join_derivation_merges_cross_side_equality() {
        let mut memo = Memo::new();
        let left = scan_group(&mut memo, 1, "lk");
        let right = scan_group(&mut memo, 2, "rk");
        memo.groups[left].logical_props = Some(LogicalProperties::new(vec![output(1, "lk")], 10.0));
        memo.groups[right].logical_props =
            Some(LogicalProperties::new(vec![output(2, "rk")], 10.0));
        let condition = eq_cols(&mut memo.scalars, 1, 2);
        let join = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalJoin(LogicalJoinOp {
                naa_author: None,
                join_type: JoinKind::Inner,
                condition: Some(condition),
            }),
            children: vec![left, right],
        });
        let props = derive_for_group(
            &memo,
            join,
            vec![output(1, "lk"), output(2, "rk")],
            10.0,
            Confidence::Estimated,
            std::collections::HashMap::new(),
        );
        let class = props
            .equivalence_classes
            .class_containing(ColumnId(2))
            .expect("join equivalence class");
        assert_eq!(
            class.iter().collect::<Vec<_>>(),
            vec![ColumnId(1), ColumnId(2)]
        );
    }

    #[test]
    fn left_join_derivation_does_not_merge_cross_side_equality() {
        let mut memo = Memo::new();
        let left = scan_group(&mut memo, 1, "lk");
        let right = scan_group(&mut memo, 2, "rk");
        memo.groups[left].logical_props = Some(LogicalProperties::new(vec![output(1, "lk")], 10.0));
        memo.groups[right].logical_props =
            Some(LogicalProperties::new(vec![output(2, "rk")], 10.0));
        let condition = eq_cols(&mut memo.scalars, 1, 2);
        let join = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalJoin(LogicalJoinOp {
                naa_author: None,
                join_type: JoinKind::LeftOuter,
                condition: Some(condition),
            }),
            children: vec![left, right],
        });
        let props = derive_for_group(
            &memo,
            join,
            vec![output(1, "lk"), output(2, "rk")],
            10.0,
            Confidence::Estimated,
            std::collections::HashMap::new(),
        );
        assert!(
            props
                .equivalence_classes
                .class_containing(ColumnId(1))
                .is_none()
        );
        assert!(
            props
                .equivalence_classes
                .class_containing(ColumnId(2))
                .is_none()
        );
    }

    #[test]
    fn physical_hash_join_derivation_skips_null_safe_hash_key() {
        let mut memo = Memo::new();
        let left = scan_group(&mut memo, 1, "lk");
        let right = scan_group(&mut memo, 2, "rk");
        memo.groups[left].logical_props = Some(LogicalProperties::new(vec![output(1, "lk")], 10.0));
        memo.groups[right].logical_props =
            Some(LogicalProperties::new(vec![output(2, "rk")], 10.0));
        let left_key = col(&mut memo.scalars, 1);
        let right_key = col(&mut memo.scalars, 2);
        let join = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                build_side: crate::optimizer::operator::HashJoinBuildSide::Right,
                distribution: JoinDistribution::Broadcast,
                condition: novarocks_physical_plan::HashJoinCondition::Ordinary {
                    keys: vec![PhysicalHashJoinEqCondition {
                        left: left_key,
                        right: right_key,
                        null_safe: true,
                    }],
                    residual: None,
                },
            }),
            children: vec![left, right],
        });
        let props = derive_for_group(
            &memo,
            join,
            vec![output(1, "lk"), output(2, "rk")],
            10.0,
            Confidence::Estimated,
            std::collections::HashMap::new(),
        );
        assert!(
            props
                .equivalence_classes
                .class_containing(ColumnId(1))
                .is_none(),
            "null-safe hash join key must not populate the strict equivalence store"
        );
    }

    #[test]
    fn physical_hash_join_derivation_keeps_strict_hash_key() {
        let mut memo = Memo::new();
        let left = scan_group(&mut memo, 1, "lk");
        let right = scan_group(&mut memo, 2, "rk");
        memo.groups[left].logical_props = Some(LogicalProperties::new(vec![output(1, "lk")], 10.0));
        memo.groups[right].logical_props =
            Some(LogicalProperties::new(vec![output(2, "rk")], 10.0));
        let left_key = col(&mut memo.scalars, 1);
        let right_key = col(&mut memo.scalars, 2);
        let join = memo.new_group(MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                build_side: crate::optimizer::operator::HashJoinBuildSide::Right,
                distribution: JoinDistribution::Broadcast,
                condition: novarocks_physical_plan::HashJoinCondition::Ordinary {
                    keys: vec![PhysicalHashJoinEqCondition {
                        left: left_key,
                        right: right_key,
                        null_safe: false,
                    }],
                    residual: None,
                },
            }),
            children: vec![left, right],
        });
        let props = derive_for_group(
            &memo,
            join,
            vec![output(1, "lk"), output(2, "rk")],
            10.0,
            Confidence::Estimated,
            std::collections::HashMap::new(),
        );
        let class = props
            .equivalence_classes
            .class_containing(ColumnId(1))
            .expect("strict hash join key equivalence class");
        assert_eq!(
            class.iter().collect::<Vec<_>>(),
            vec![ColumnId(1), ColumnId(2)]
        );
    }

    fn cv_node(
        array: std::sync::Arc<dyn arrow::array::Array>,
        ordinal: u32,
        logical: novarocks_type_contract::ValueLogicalType,
        nullable: bool,
        metadata: &str,
    ) -> (ScalarArena, ScalarId, novarocks_functions::ConstantValue) {
        let ty = novarocks_type_contract::FunctionValueType::try_from_legacy_declared_projection(
            array.data_type().clone(),
            nullable,
            logical,
        )
        .expect("valid temporary legacy value projection");
        let field = arrow::datatypes::Field::new("selected", ty.data_type().clone(), nullable)
            .with_metadata(HashMap::from([("provider".into(), metadata.into())]));
        let value = novarocks_functions::ConstantPool::try_new(
            std::sync::Arc::new(field),
            ty.clone(),
            array.to_data(),
            crate::constant::test_constant_policy(),
            novarocks_type_contract::CompilePhase::Validate,
            crate::optimizer::test_optimizer_control(),
        )
        .unwrap()
        .value(ordinal)
        .unwrap();
        let mut arena = ScalarArena::new();
        let id = arena
            .intern_observed(
                ScalarNode::Constant(value.clone()),
                ty,
                crate::optimizer::test_optimizer_control(),
            )
            .unwrap();
        (arena, id, value)
    }

    #[test]
    fn literal_identity_compares_selected_cv_and_complete_source_without_pool_or_scalar_id_keys() {
        use arrow::array::{Int64Array, StringArray};
        use novarocks_type_contract::ValueLogicalType;
        use std::sync::Arc;
        let (left, l, lv) = cv_node(
            Arc::new(Int64Array::from(vec![99, 7])),
            1,
            ValueLogicalType::Physical,
            false,
            "same",
        );
        let (right, r, rv) = cv_node(
            Arc::new(Int64Array::from(vec![7, -20, 8])),
            0,
            ValueLogicalType::Physical,
            false,
            "same",
        );
        assert_ne!(lv.pool().backing_identity(), rv.pool().backing_identity());
        assert_ne!(lv.ordinal(), rv.ordinal());
        assert!(
            literal_equal_observed(
                &left,
                l,
                &right,
                r,
                crate::optimizer::test_optimizer_control()
            )
            .unwrap()
        );
        let mut different = ScalarArena::new();
        let different_id = different
            .intern_observed(
                ScalarNode::Constant(rv.pool().value(2).unwrap()),
                rv.value_type().clone(),
                crate::optimizer::test_optimizer_control(),
            )
            .unwrap();
        assert_eq!(
            l, different_id,
            "same arena-local number does not imply the same selected value"
        );
        assert!(
            !literal_equal_observed(
                &left,
                l,
                &different,
                different_id,
                crate::optimizer::test_optimizer_control()
            )
            .unwrap()
        );
        let mut same_pool = ScalarArena::new();
        let selected = same_pool
            .intern_observed(
                ScalarNode::Constant(rv.clone()),
                rv.value_type().clone(),
                crate::optimizer::test_optimizer_control(),
            )
            .unwrap();
        let other = same_pool
            .intern_observed(
                ScalarNode::Constant(rv.pool().value(2).unwrap()),
                rv.value_type().clone(),
                crate::optimizer::test_optimizer_control(),
            )
            .unwrap();
        assert!(
            !literal_equal_observed(
                &same_pool,
                selected,
                &same_pool,
                other,
                crate::optimizer::test_optimizer_control()
            )
            .unwrap()
        );
        for (other_nullable, other_fact, expected_equal) in
            [(true, "same", false), (false, "different", true)]
        {
            let (other_arena, other_id, _) = cv_node(
                Arc::new(Int64Array::from(vec![7])),
                0,
                ValueLogicalType::Physical,
                other_nullable,
                other_fact,
            );
            assert_eq!(
                literal_equal_observed(
                    &left,
                    l,
                    &other_arena,
                    other_id,
                    crate::optimizer::test_optimizer_control()
                )
                .unwrap(),
                expected_equal
            );
        }
        let (text, text_id, _) = cv_node(
            Arc::new(StringArray::from(vec!["\"same\""])),
            0,
            ValueLogicalType::Physical,
            false,
            "same",
        );
        let (json, json_id, _) = cv_node(
            Arc::new(StringArray::from(vec!["\"same\""])),
            0,
            ValueLogicalType::Json,
            false,
            "same",
        );
        assert!(
            !literal_equal_observed(
                &text,
                text_id,
                &json,
                json_id,
                crate::optimizer::test_optimizer_control()
            )
            .unwrap()
        );
    }

    #[test]
    fn literal_identity_keeps_legacy_syntax_nan_zero_rules_without_implicit_cv_interchange() {
        use arrow::array::{Float64Array, Int64Array};
        use novarocks_type_contract::ValueLogicalType;
        use std::sync::Arc;
        let mut left = ScalarArena::new();
        let mut right = ScalarArena::new();
        let ty = novarocks_type_contract::FunctionValueType::new(DataType::Float64, false);
        let a = left.intern(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Float(f64::from_bits(
                0x7ff8000000000001,
            )))),
            ty.clone(),
        );
        let b = right.intern(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Float(f64::from_bits(
                0x7ff8000000000002,
            )))),
            ty.clone(),
        );
        assert!(
            literal_equal_observed(
                &left,
                a,
                &right,
                b,
                crate::optimizer::test_optimizer_control()
            )
            .unwrap()
        );
        let zero = left.intern(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Float(0.0))),
            ty.clone(),
        );
        let negative_zero = right.intern(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Float(-0.0))),
            ty,
        );
        assert!(
            !literal_equal_observed(
                &left,
                zero,
                &right,
                negative_zero,
                crate::optimizer::test_optimizer_control()
            )
            .unwrap()
        );
        let (first, first_id, _) = cv_node(
            Arc::new(Float64Array::from(vec![f64::from_bits(0x7ff8000000000001)])),
            0,
            ValueLogicalType::Physical,
            false,
            "same",
        );
        let (second, second_id, _) = cv_node(
            Arc::new(Float64Array::from(vec![f64::from_bits(0x7ff8000000000002)])),
            0,
            ValueLogicalType::Physical,
            false,
            "same",
        );
        assert!(
            !literal_equal_observed(
                &first,
                first_id,
                &second,
                second_id,
                crate::optimizer::test_optimizer_control()
            )
            .unwrap()
        );
        let syntax = lit(&mut left, 7);
        let (cv_arena, cv_id, _) = cv_node(
            Arc::new(Int64Array::from(vec![7])),
            0,
            ValueLogicalType::Physical,
            false,
            "same",
        );
        assert!(
            !literal_equal_observed(
                &left,
                syntax,
                &cv_arena,
                cv_id,
                crate::optimizer::test_optimizer_control()
            )
            .unwrap()
        );
        assert!(
            !literal_equal_observed(
                &cv_arena,
                cv_id,
                &left,
                syntax,
                crate::optimizer::test_optimizer_control()
            )
            .unwrap()
        );
    }
}
