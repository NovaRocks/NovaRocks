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

//! Implementation rules: logical operator -> physical operator(s).
//!
//! Each struct implements the `Rule` trait. The `apply` method constructs the
//! physical variant of the matched logical operator, preserving child GroupIds.

use std::collections::HashSet;

use arrow::datatypes::DataType;

use crate::column_id::ColumnId;
use crate::common::{BinOp, JoinKind, OutputColumn};
use crate::optimizer::memo::{GroupId, MExpr, Memo};
use crate::optimizer::operator::*;
use crate::optimizer::rule::{NewExpr, Rule, RuleType};
use crate::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};
use crate::optimizer::scalar_expr;
#[cfg(test)]
use crate::planner::payload::WindowExpr;

pub(super) fn get_group_column_ids(memo: &Memo, group_id: GroupId) -> HashSet<ColumnId> {
    memo.groups
        .get(group_id)
        .and_then(|g| g.logical_props.as_ref())
        .map(|props| {
            props
                .output_columns
                .iter()
                .map(|c| c.column_id)
                .filter(|id| *id != ColumnId::UNSET)
                .collect()
        })
        .unwrap_or_default()
}

/// True if `expr` contains at least one ColumnRef. Used to decide whether a
/// side of an `Eq` predicate could be a join key (vs. a constant filter).
pub(super) fn expr_has_column_ref(arena: &ScalarArena, expr: ScalarId) -> bool {
    scalar_expr::collect_column_ids_strict(arena, expr).is_some_and(|ids| !ids.is_empty())
}

#[derive(Clone, Debug)]
struct ScalarHashJoinEqCondition {
    left: ScalarId,
    right: ScalarId,
    null_safe: bool,
    match_relation: novarocks_type_contract::JoinKeyMatch,
    origin: ScalarId,
    bound_comparison: bool,
}

/// Orient an eq pair so that the first element references the left child's
/// columns and the second references the right. Returns `None` when either side
/// has unresolved ids, cannot be assigned exclusively to one child, or both
/// expressions reference the same child.
fn orient_eq_pair(
    pair: ScalarHashJoinEqCondition,
    arena: &ScalarArena,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
) -> Option<ScalarHashJoinEqCondition> {
    let (a, b) = (pair.left, pair.right);
    let a_ids = scalar_expr::collect_column_ids_strict(arena, a)?;
    let b_ids = scalar_expr::collect_column_ids_strict(arena, b)?;
    if a_ids.is_empty() || b_ids.is_empty() {
        return None;
    }

    let a_in_left = a_ids
        .iter()
        .all(|id| left_ids.contains(id) && !right_ids.contains(id));
    let a_in_right = a_ids
        .iter()
        .all(|id| right_ids.contains(id) && !left_ids.contains(id));
    let b_in_left = b_ids
        .iter()
        .all(|id| left_ids.contains(id) && !right_ids.contains(id));
    let b_in_right = b_ids
        .iter()
        .all(|id| right_ids.contains(id) && !left_ids.contains(id));

    // Unambiguous exclusive assignment: a from left only, b from right only.
    if a_in_left && !a_in_right && b_in_right && !b_in_left {
        return Some(ScalarHashJoinEqCondition {
            left: a,
            right: b,
            ..pair
        });
    }
    // Unambiguous exclusive swap: a from right only, b from left only.
    if a_in_right && !a_in_left && b_in_left && !b_in_right {
        return Some(ScalarHashJoinEqCondition {
            left: b,
            right: a,
            ..pair
        });
    }
    None
}

fn hash_join_key_type_is_supported(data_type: &DataType) -> bool {
    !matches!(
        data_type,
        DataType::Null
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Struct(_)
            | DataType::Union(_, _)
            | DataType::Dictionary(_, _)
            | DataType::Map(_, _)
            | DataType::RunEndEncoded(_, _)
    )
}

fn place_hash_join_key(
    arena: &ScalarArena,
    eq: ScalarHashJoinEqCondition,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Option<ScalarHashJoinEqCondition>, crate::compiler::SqlCompileError> {
    if !safe_key_operand(arena, eq.origin)
        || !safe_key_operand(arena, eq.left)
        || !safe_key_operand(arena, eq.right)
    {
        return Ok(None);
    }
    let Some(eq) = orient_eq_pair(eq, arena, left_ids, right_ids) else {
        return Ok(None);
    };
    let left = arena.value_type(eq.left);
    let right = arena.value_type(eq.right);
    let Ok(common) = crate::compiler::selected_scalar_call::comparison_type(left, right) else {
        return Ok(None);
    };
    if !hash_join_key_type_is_supported(common.data_type())
        || !eq
            .match_relation
            .supports(common.data_type(), common.logical_type(), eq.null_safe)
        || (eq.bound_comparison
            && (left.value_type().kind() != right.value_type().kind()
                || left.data_type() != right.data_type()
                || left.logical_type() != right.logical_type()))
        || !arena.comparison_casts_can_share(left, right, control)?
    {
        return Ok(None);
    }
    Ok(Some(eq))
}

fn comparison_key(arena: &ScalarArena, expr: ScalarId) -> Option<ScalarHashJoinEqCondition> {
    use novarocks_type_contract::{JoinKeyMatch, OperatorFunction};

    let (left, right, null_safe, bound_comparison) = if let Some((operator, [left, right])) =
        crate::common::binary_operator::scalar_operands(arena, expr)
    {
        let null_safe = match operator {
            OperatorFunction::Equal => false,
            OperatorFunction::Identical => true,
            _ => return None,
        };
        let ScalarNode::FunctionCall {
            binding,
            distinct: false,
            ..
        } = arena.node(expr)
        else {
            return None;
        };
        use novarocks_functions::FunctionResultType;
        use novarocks_type_contract::{FunctionArgumentType, FunctionKind};
        let (
            [
                FunctionArgumentType::Value(selected_left),
                FunctionArgumentType::Value(selected_right),
            ],
            FunctionResultType::Scalar(result),
        ) = (
            binding.selected.argument_types.as_ref(),
            &binding.selected.result_type,
        )
        else {
            return None;
        };
        if binding.kind != FunctionKind::Scalar
            || binding.logical_argument_count != 2
            || Some(binding.selected.overload.as_str()) != operator.comparison_overload_text()
            || selected_left != arena.value_type(left)
            || selected_right != arena.value_type(right)
            || result != arena.value_type(expr)
        {
            return None;
        }
        (left, right, null_safe, true)
    } else {
        let ScalarNode::BinaryOp {
            left, op, right, ..
        } = arena.node(expr)
        else {
            return None;
        };
        let null_safe = match op {
            BinOp::Eq => false,
            BinOp::EqForNull => true,
            _ => return None,
        };
        (*left, *right, null_safe, false)
    };
    let match_relation = if bound_comparison
        && !null_safe
        && matches!(arena.data_type(left), DataType::Float32 | DataType::Float64)
    {
        JoinKeyMatch::IeeeFloat
    } else {
        JoinKeyMatch::Grouping
    };
    Some(ScalarHashJoinEqCondition {
        left,
        right,
        null_safe,
        match_relation,
        origin: expr,
        bound_comparison,
    })
}

pub(crate) fn strict_comparison_operands(
    arena: &ScalarArena,
    expr: ScalarId,
) -> Option<[ScalarId; 2]> {
    let key = comparison_key(arena, expr)?;
    (!key.null_safe).then_some([key.left, key.right])
}

/// Recursively flatten top-level AND nodes and classify each conjunct as
/// either an equality pair or a residual predicate.
fn collect_conjuncts(
    arena: &ScalarArena,
    expr: ScalarId,
    eq_pairs: &mut Vec<ScalarHashJoinEqCondition>,
    others: &mut Vec<ScalarId>,
) {
    match arena.node(expr) {
        // Unwrap parenthesized expressions transparently.
        ScalarNode::Nested(inner) => {
            collect_conjuncts(arena, *inner, eq_pairs, others);
        }
        ScalarNode::BinaryOp {
            left,
            op: BinOp::And,
            right,
            ..
        } => {
            collect_conjuncts(arena, *left, eq_pairs, others);
            collect_conjuncts(arena, *right, eq_pairs, others);
        }
        _ => match comparison_key(arena, expr) {
            Some(pair)
                if expr_has_column_ref(arena, pair.left)
                    && expr_has_column_ref(arena, pair.right) =>
            {
                eq_pairs.push(pair)
            }
            _ => others.push(expr),
        },
    }
}

/// The interned definition includes the original comparison binding,
/// ordered operands, and every operand and result type.
fn eq_pair_matches(a: &ScalarHashJoinEqCondition, b: &ScalarHashJoinEqCondition) -> bool {
    a.origin == b.origin
}

/// Try to extract common equality conditions from an OR expression.
///
/// Given `(A AND x=y AND B) OR (C AND x=y AND D)`, extracts `(x, y)` as
/// a common eq pair and rewrites the expression to `(A AND B) OR (C AND D)`.
///
/// Returns `(common_eq_pairs, rewritten_or_condition)`.
fn try_extract_common_eq_from_or(
    arena: &mut ScalarArena,
    expr: ScalarId,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<(Vec<ScalarHashJoinEqCondition>, Option<ScalarId>), crate::compiler::SqlCompileError> {
    let mut branches = Vec::new();
    scalar_expr::split_disjuncts(arena, expr, &mut branches);
    if branches.len() < 2 {
        return Ok((vec![], Some(expr)));
    }

    // For each branch, extract eq pairs and residual.
    let mut branch_eqs: Vec<Vec<ScalarHashJoinEqCondition>> = Vec::new();
    let mut branch_others: Vec<Vec<ScalarId>> = Vec::new();
    for branch in &branches {
        let mut eqs = Vec::new();
        let mut others = Vec::new();
        collect_conjuncts(arena, *branch, &mut eqs, &mut others);
        branch_eqs.push(eqs);
        branch_others.push(others);
    }

    // Find eq pairs that appear in ALL branches.
    let first_eqs = &branch_eqs[0];
    let mut common: Vec<ScalarHashJoinEqCondition> = Vec::new();
    for eq in first_eqs {
        if branch_eqs[1..]
            .iter()
            .all(|branch| branch.iter().any(|b| eq_pair_matches(eq, b)))
        {
            common.push(eq.clone());
        }
    }

    if common.is_empty() {
        return Ok((vec![], Some(expr)));
    }

    // Rewrite each branch: remove the common eq pairs, recombine.
    let mut rewritten_branches = Vec::new();
    for (eqs, others) in branch_eqs.iter().zip(branch_others.iter()) {
        let mut remaining_parts: Vec<ScalarId> = others.clone();
        for eq in eqs {
            if !common.iter().any(|c| eq_pair_matches(c, eq)) {
                // Keep non-common eq pairs as regular conjuncts.
                remaining_parts.push(eq.origin);
            }
        }
        if let Some(branch_expr) = scalar_expr::combine_conjuncts(arena, remaining_parts, control)?
        {
            rewritten_branches.push(branch_expr);
        }
        // If a branch becomes empty (only common eqs), skip it — it
        // effectively becomes TRUE, making the whole OR always true for
        // matched eq keys.  We represent this by omitting the branch.
    }

    let rewritten = if rewritten_branches.len() == branches.len() {
        scalar_expr::combine_disjuncts(arena, rewritten_branches, control)?
    } else {
        // Some branches were pure eq-only; the entire OR condition is
        // satisfied whenever the common equalities hold.
        None
    };

    Ok((common, rewritten))
}

// ===========================================================================
// Implementation rule structs
// ===========================================================================

// ---------------------------------------------------------------------------
// 1. ScanToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct ScanToPhysical;

impl Rule for ScanToPhysical {
    fn name(&self) -> &str {
        "ScanToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalScan(_))
    }
    fn apply(
        &self,
        expr: &MExpr,
        _memo: &mut Memo,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Vec<NewExpr>, crate::compiler::SqlCompileError> {
        let _ = control;

        let Operator::LogicalScan(op) = &expr.op else {
            return Ok(vec![]);
        };
        Ok(vec![NewExpr {
            op: Operator::PhysicalScan(ScanOp {
                database: op.database.clone(),
                table: op.table.clone(),
                alias: op.alias.clone(),
                stats_ref: op.stats_ref,
                columns: op.columns.clone(),
                predicates: op.predicates.clone(),
                required_columns: op.required_columns.clone(),
                // Propagated from the logical scan. Populated by
                // `VariantPathPushdown`; empty otherwise.
                variant_columns: op.variant_columns.clone(),
                // Propagated from the logical scan so that MvRewrite-injected
                // scans carry the annotation through to the physical plan.
                mv_rewritten_from: op.mv_rewritten_from.clone(),
            }),
            children: expr.children.clone(),
        }])
    }
}

// ---------------------------------------------------------------------------
// 2. FilterToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct FilterToPhysical;

impl Rule for FilterToPhysical {
    fn name(&self) -> &str {
        "FilterToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalFilter(_))
    }
    fn apply(
        &self,
        expr: &MExpr,
        _memo: &mut Memo,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Vec<NewExpr>, crate::compiler::SqlCompileError> {
        let _ = control;

        let Operator::LogicalFilter(op) = &expr.op else {
            return Ok(vec![]);
        };
        Ok(vec![NewExpr {
            op: Operator::PhysicalFilter(FilterOp {
                predicate: op.predicate,
            }),
            children: expr.children.clone(),
        }])
    }
}

// ---------------------------------------------------------------------------
// 3. ProjectToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct ProjectToPhysical;

impl Rule for ProjectToPhysical {
    fn name(&self) -> &str {
        "ProjectToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalProject(_))
    }
    fn apply(
        &self,
        expr: &MExpr,
        _memo: &mut Memo,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Vec<NewExpr>, crate::compiler::SqlCompileError> {
        let _ = control;

        let Operator::LogicalProject(op) = &expr.op else {
            return Ok(vec![]);
        };
        Ok(vec![NewExpr {
            op: Operator::PhysicalProject(ProjectOp {
                items: op.items.clone(),
                output_qualifier: op.output_qualifier.clone(),
            }),
            children: expr.children.clone(),
        }])
    }
}

// ---------------------------------------------------------------------------
// 4. JoinToHashJoin
// ---------------------------------------------------------------------------

pub(crate) struct JoinToHashJoin;

type ScalarJoinCondition =
    novarocks_physical_plan::HashJoinCondition<Vec<PhysicalHashJoinEqCondition>, ScalarId>;

pub(crate) fn safe_key_operand(arena: &ScalarArena, expression: ScalarId) -> bool {
    arena.effects(expression).can_share()
}

/// Admit only the original NOT IN author's complete C, with its Value-demand
/// prefix intact. A merely orientable equality in E is never a value key.
fn null_aware_hash_condition(
    op: &LogicalJoinOp,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
    arena: &mut ScalarArena,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Option<ScalarJoinCondition>, crate::compiler::SqlCompileError> {
    let Some(author) = &op.naa_author else {
        return Ok(None);
    };
    let Some(predicate) = op.condition else {
        return Ok(None);
    };
    let mut prefix = predicate;
    loop {
        if prefix == author.comparison {
            break;
        }
        match arena.node(prefix) {
            ScalarNode::Nested(inner) => prefix = *inner,
            ScalarNode::BinaryOp {
                left,
                op: BinOp::And,
                ..
            } => prefix = *left,
            _ => return Ok(None),
        }
    }
    let mut comparisons = Vec::new();
    let mut comparison_rest = Vec::new();
    collect_conjuncts(
        arena,
        author.comparison,
        &mut comparisons,
        &mut comparison_rest,
    );
    if comparisons.is_empty() || !comparison_rest.is_empty() {
        return Ok(None);
    }
    let mut value_equal_keys = Vec::new();
    for pair in comparisons {
        let Some(pair) = place_hash_join_key(arena, pair, left_ids, right_ids, control)? else {
            return Ok(None);
        };
        value_equal_keys.push(PhysicalHashJoinEqCondition {
            left: pair.left,
            right: pair.right,
            null_safe: pair.null_safe,
            match_relation: pair.match_relation,
        });
    }
    let mut scope_keys = Vec::new();
    let mut remaining = Vec::new();
    if let Some(eligibility) = author.eligibility {
        let total = safe_key_operand(arena, eligibility);
        let mut conjuncts = Vec::new();
        scalar_expr::split_conjuncts(arena, eligibility, &mut conjuncts);
        let mut safe_prefix = true;
        for conjunct in conjuncts {
            // A nullable Value-demand prefix cannot suppress required work
            // after UNKNOWN. A nonnullable safe prefix, or a total E, can.
            let candidate = comparison_key(arena, conjunct).filter(|pair| {
                total
                    || (safe_prefix
                        && (pair.null_safe
                            || (!arena.nullable(conjunct)
                                && !arena.nullable(pair.left)
                                && !arena.nullable(pair.right))))
            });
            let extracted = match candidate {
                Some(pair) => place_hash_join_key(arena, pair, left_ids, right_ids, control)?,
                None => None,
            };
            if let Some(pair) = extracted {
                scope_keys.push(PhysicalHashJoinEqCondition {
                    left: pair.left,
                    right: pair.right,
                    null_safe: pair.null_safe,
                    match_relation: pair.match_relation,
                });
            } else {
                remaining.push(conjunct);
                safe_prefix = false;
            }
        }
    }
    let eligibility = if scope_keys.is_empty() {
        author.eligibility
    } else {
        scalar_expr::combine_conjuncts(arena, remaining, control)?
    };
    Ok(Some(
        novarocks_physical_plan::HashJoinCondition::NullAwareEqual {
            value_equal_keys,
            scope_keys,
            eligibility,
        },
    ))
}

/// Prove a placement from the original ordinary TruthOnly condition.
fn ordinary_hash_condition(
    condition: ScalarId,
    arena: &mut ScalarArena,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Option<ScalarJoinCondition>, crate::compiler::SqlCompileError> {
    let effects = arena.effects(condition);
    // Hash changes physical pair traversal. Stateful work or invocation Data
    // needs a separate pair-domain proof, even if the equality is total.
    if !effects.expression.permits_boolean_reordering() || !effects.permits_filter_row_elimination {
        return Ok(None);
    }

    let mut conjuncts = Vec::new();
    scalar_expr::split_conjuncts(arena, condition, &mut conjuncts);
    let mut keys = Vec::new();
    let mut residual = Vec::new();
    let mut can_prune_pairs = true;
    for conjunct in conjuncts {
        control.checkpoint(novarocks_type_contract::CompilePhase::Validate, 0)?;
        let pair = if can_prune_pairs {
            match comparison_key(arena, conjunct) {
                Some(pair) => place_hash_join_key(arena, pair, left_ids, right_ids, control)?,
                None => None,
            }
        } else {
            None
        };
        if let Some(pair) = pair {
            keys.push(pair);
        } else {
            // Hash rejects pairs before evaluating the residual. A later key
            // cannot hide a required error or effect of an earlier conjunct.
            can_prune_pairs &= arena.effects(conjunct).can_skip_for_earlier_filter();
            residual.push(conjunct);
        }
    }
    // Retain the original total AND/OR path. Factoring inside a condition
    // that can raise is not licensed by the outer TruthOnly proof.
    if keys.is_empty() && effects.can_share() {
        let mut rewritten = Vec::new();
        for part in residual {
            let (common, remaining) = try_extract_common_eq_from_or(arena, part, control)?;
            let mut placed = Vec::new();
            for pair in common {
                let Some(pair) = place_hash_join_key(arena, pair, left_ids, right_ids, control)?
                else {
                    placed.clear();
                    break;
                };
                placed.push(pair);
            }
            if placed.is_empty() {
                rewritten.push(part);
            } else {
                keys.extend(placed);
                rewritten.extend(remaining);
            }
        }
        residual = rewritten;
    }
    if keys.is_empty() {
        return Ok(None);
    }
    Ok(Some(novarocks_physical_plan::HashJoinCondition::Ordinary {
        keys: keys
            .into_iter()
            .map(|eq| PhysicalHashJoinEqCondition {
                left: eq.left,
                right: eq.right,
                null_safe: eq.null_safe,
                match_relation: eq.match_relation,
            })
            .collect(),
        residual: scalar_expr::combine_conjuncts(arena, residual, control)?,
    }))
}

fn choose_hash_join(
    op: &LogicalJoinOp,
    children: &[GroupId],
    memo: &mut Memo,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Option<PhysicalHashJoinOp>, crate::compiler::SqlCompileError> {
    let ([left, right], Some(condition), Some(build_side)) = (
        children,
        op.condition,
        crate::optimizer::operator::exact_hash_join_build_side(op.join_type),
    ) else {
        return Ok(None);
    };
    let left_ids = get_group_column_ids(memo, *left);
    let right_ids = get_group_column_ids(memo, *right);
    let condition = if matches!(
        op.join_type,
        JoinKind::NullAwareLeftAnti | JoinKind::LeftMark { .. }
    ) {
        null_aware_hash_condition(op, &left_ids, &right_ids, &mut memo.scalars, control)?
    } else {
        ordinary_hash_condition(condition, &mut memo.scalars, &left_ids, &right_ids, control)?
    };
    Ok(condition.map(|condition| PhysicalHashJoinOp {
        join_type: op.join_type,
        build_side,
        distribution: JoinDistribution::Unknown,
        condition,
    }))
}

impl Rule for JoinToHashJoin {
    fn name(&self) -> &str {
        "JoinToHashJoin"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalJoin(_))
    }
    fn apply(
        &self,
        expr: &MExpr,
        memo: &mut Memo,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Vec<NewExpr>, crate::compiler::SqlCompileError> {
        let Operator::LogicalJoin(op) = &expr.op else {
            return Ok(vec![]);
        };
        Ok(choose_hash_join(op, &expr.children, memo, control)?
            .map(|hash| NewExpr {
                op: Operator::PhysicalHashJoin(hash),
                children: expr.children.clone(),
            })
            .into_iter()
            .collect())
    }
}

pub(crate) struct JoinToNestLoop;

impl Rule for JoinToNestLoop {
    fn name(&self) -> &str {
        "JoinToNestLoop"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalJoin(_))
    }
    fn apply(
        &self,
        expr: &MExpr,
        memo: &mut Memo,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Vec<NewExpr>, crate::compiler::SqlCompileError> {
        let Operator::LogicalJoin(op) = &expr.op else {
            return Ok(vec![]);
        };
        if matches!(op.join_type, JoinKind::LeftMark { .. })
            && (op.naa_author.is_some() || op.condition.is_none())
        {
            // Relation marks consume the complete predicate. Authored hash
            // marks have a distinct Value-demand comparison placement.
            return Ok(vec![]);
        }
        let hash_available = memo.hash_join_enabled || op.join_type == JoinKind::NullAwareLeftAnti;
        if hash_available && choose_hash_join(op, &expr.children, memo, control)?.is_some() {
            return Ok(vec![]);
        }
        Ok(vec![NewExpr {
            op: Operator::PhysicalNestLoopJoin(PhysicalNestLoopJoinOp {
                join_type: op.join_type,
                condition: op.condition,
            }),
            children: expr.children.clone(),
        }])
    }
}

// ---------------------------------------------------------------------------
// 6. AggToHashAgg
// ---------------------------------------------------------------------------

pub(crate) struct AggToHashAgg;

impl Rule for AggToHashAgg {
    fn name(&self) -> &str {
        "AggToHashAgg"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalAggregate(_))
    }
    fn apply(
        &self,
        expr: &MExpr,
        _memo: &mut Memo,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Vec<NewExpr>, crate::compiler::SqlCompileError> {
        let _ = control;

        let Operator::LogicalAggregate(op) = &expr.op else {
            return Ok(Vec::new());
        };
        Ok(vec![NewExpr {
            op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: op.stage.to_physical_mode(),
                group_by: op.group_by.clone(),
                aggregates: op.aggregates.clone(),
                output_layout: op.output_layout.clone(),
                output_columns: op.output_columns.clone(),
                is_merge: op.is_merge.clone(),
            }),
            children: expr.children.clone(),
        }])
    }
}

// ---------------------------------------------------------------------------
// 7. SortToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct SortToPhysical;

impl Rule for SortToPhysical {
    fn name(&self) -> &str {
        "SortToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalSort(_))
    }
    fn apply(
        &self,
        expr: &MExpr,
        _memo: &mut Memo,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Vec<NewExpr>, crate::compiler::SqlCompileError> {
        let _ = control;

        let Operator::LogicalSort(op) = &expr.op else {
            return Ok(vec![]);
        };
        Ok(vec![NewExpr {
            op: Operator::PhysicalSort(SortOp {
                items: op.items.clone(),
                // Propagate the analytic-partition tag through Logical→Physical
                // so the optimizer's required-distribution logic can see it.
                analytic_partition_exprs: op.analytic_partition_exprs.clone(),
                partition_limit: op.partition_limit,
                topn_type: op.topn_type,
            }),
            children: expr.children.clone(),
        }])
    }
}

// ---------------------------------------------------------------------------
// 8. LimitToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct LimitToPhysical;

impl Rule for LimitToPhysical {
    fn name(&self) -> &str {
        "LimitToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalLimit(_))
    }
    fn apply(
        &self,
        expr: &MExpr,
        memo: &mut Memo,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Vec<NewExpr>, crate::compiler::SqlCompileError> {
        let _ = control;

        let Operator::LogicalLimit(op) = &expr.op else {
            return Ok(vec![]);
        };
        // If the Limit has a Sort directly underneath, SortLimitToTopN has
        // already added an equivalent LogicalTopN to this same group; defer
        // exclusively to that path. Producing both PhysicalLimit (here) and
        // PhysicalTopN (via TopNToPhysical) is unsafe at large cost scales:
        // the underlying join/agg cost dominates and f64 precision collapses
        // the difference, so the search arbitrarily picks whichever
        // alternative was inserted first. Fragment builder also asserts that
        // PhysicalLimit never sits directly on a SORT_NODE; this skip keeps
        // the assertion satisfied.
        if op.limit.is_some() && expr.children.len() == 1 {
            let child_group = &memo.groups[expr.children[0]];
            let child_has_sort = child_group
                .logical_exprs
                .iter()
                .any(|m| matches!(m.op, Operator::LogicalSort(_)));
            if child_has_sort {
                return Ok(vec![]);
            }
        }

        Ok(vec![NewExpr {
            op: Operator::PhysicalLimit(LimitOp {
                limit: op.limit,
                offset: op.offset,
            }),
            children: expr.children.clone(),
        }])
    }
}

// ---------------------------------------------------------------------------
// AssertOneRowToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct AssertOneRowToPhysical;

impl Rule for AssertOneRowToPhysical {
    fn name(&self) -> &str {
        "AssertOneRowToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalAssertOneRow(_))
    }
    fn apply(
        &self,
        expr: &MExpr,
        _memo: &mut Memo,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Vec<NewExpr>, crate::compiler::SqlCompileError> {
        let _ = control;

        let Operator::LogicalAssertOneRow(op) = &expr.op else {
            return Ok(vec![]);
        };
        Ok(vec![NewExpr {
            op: Operator::PhysicalAssertOneRow(AssertOneRowOp {
                subquery_text: op.subquery_text.clone(),
            }),
            children: expr.children.clone(),
        }])
    }
}

// ---------------------------------------------------------------------------
// 8b. TopNToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct TopNToPhysical;

impl Rule for TopNToPhysical {
    fn name(&self) -> &str {
        "TopNToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalTopN(_))
    }
    fn apply(
        &self,
        expr: &MExpr,
        _memo: &mut Memo,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Vec<NewExpr>, crate::compiler::SqlCompileError> {
        let _ = control;

        let Operator::LogicalTopN(op) = &expr.op else {
            return Ok(vec![]);
        };
        Ok(vec![NewExpr {
            op: Operator::PhysicalTopN(TopNOp {
                items: op.items.clone(),
                limit: op.limit,
                offset: op.offset,
                phase: op.phase,
                is_split: op.is_split,
            }),
            children: expr.children.clone(),
        }])
    }
}

// ---------------------------------------------------------------------------
// 9. WindowToPhysical
// ---------------------------------------------------------------------------

/// Split a LogicalWindow's expressions into groups sharing the same
/// (partition_by, order_by, frame) signature. Preserves first-seen order.
#[cfg(test)]
#[allow(dead_code)]
fn split_window_exprs_by_signature(exprs: &[WindowExpr]) -> Vec<Vec<WindowExpr>> {
    let signature = |expr: &WindowExpr| {
        format!(
            "{:?}|{:?}|{:?}",
            expr.partition_by
                .iter()
                .map(|item| format!("{:?}", item.kind))
                .collect::<Vec<_>>(),
            expr.order_by
                .iter()
                .map(|item| format!("{:?}:{}", item.expr.kind, item.asc))
                .collect::<Vec<_>>(),
            expr.window_frame,
        )
    };
    let mut groups: Vec<(String, Vec<WindowExpr>)> = Vec::new();
    for expr in exprs {
        let key = signature(expr);
        if let Some((_, group)) = groups.iter_mut().find(|(existing, _)| *existing == key) {
            group.push(expr.clone());
        } else {
            groups.push((key, vec![expr.clone()]));
        }
    }
    groups.into_iter().map(|(_, group)| group).collect()
}

/// Derive sort items for a window's partition_by + order_by.
/// Window sort ordering is: partition_by columns first (ASC, NULLS FIRST),
/// then order_by columns with their own direction.
#[cfg(test)]
#[allow(dead_code)]
fn sort_items_for_window(
    win: &crate::planner::payload::WindowExpr,
) -> Vec<crate::analysis::SortItem> {
    let mut items = Vec::new();
    for expr in &win.partition_by {
        items.push(crate::analysis::SortItem {
            expr: expr.clone(),
            asc: true,
            nulls_first: true,
        });
    }
    for item in &win.order_by {
        items.push(item.clone());
    }
    items
}

pub(crate) struct WindowToPhysical;

impl Rule for WindowToPhysical {
    fn name(&self) -> &str {
        "WindowToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalWindow(_))
    }
    fn apply(
        &self,
        expr: &MExpr,
        _memo: &mut Memo,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Vec<NewExpr>, crate::compiler::SqlCompileError> {
        let _ = control;

        let Operator::LogicalWindow(op) = &expr.op else {
            return Ok(vec![]);
        };
        if expr.children.len() != 1 {
            return Ok(vec![]);
        }
        let child_group = expr.children[0];
        Ok(
            // Emit a single PhysicalWindow with all window expressions.
            // The fragment builder groups expressions by (partition_by, order_by)
            // signature internally and emits one Sort+Analytic node per group —
            // all within the same fragment, without cross-group exchanges.
            // Cascades-level splitting (one PhysicalWindow per signature group)
            // would cause the CBO to insert distribution enforcers (HASH EXCHANGE)
            // between window nodes when their partition key sets differ, which
            // breaks pipelined analytic execution.
            vec![NewExpr {
                op: Operator::PhysicalWindow(WindowOp {
                    window_exprs: op.window_exprs.clone(),
                    output_columns: op.output_columns.clone(),
                }),
                children: vec![child_group],
            }],
        )
    }
}

// ---------------------------------------------------------------------------
// 10. CTEAnchorToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct CTEAnchorToPhysical;

impl Rule for CTEAnchorToPhysical {
    fn name(&self) -> &str {
        "CTEAnchorToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalCTEAnchor(_))
    }
    fn apply(
        &self,
        expr: &MExpr,
        _memo: &mut Memo,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Vec<NewExpr>, crate::compiler::SqlCompileError> {
        let _ = control;

        let Operator::LogicalCTEAnchor(op) = &expr.op else {
            return Ok(vec![]);
        };
        Ok(vec![NewExpr {
            op: Operator::PhysicalCTEAnchor(CTEAnchorOp { cte_id: op.cte_id }),
            children: expr.children.clone(),
        }])
    }
}

// ---------------------------------------------------------------------------
// 11. CTEProduceToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct CTEProduceToPhysical;

impl Rule for CTEProduceToPhysical {
    fn name(&self) -> &str {
        "CTEProduceToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalCTEProduce(_))
    }
    fn apply(
        &self,
        expr: &MExpr,
        _memo: &mut Memo,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Vec<NewExpr>, crate::compiler::SqlCompileError> {
        let _ = control;

        let Operator::LogicalCTEProduce(op) = &expr.op else {
            return Ok(vec![]);
        };
        Ok(vec![NewExpr {
            op: Operator::PhysicalCTEProduce(CTEProduceOp {
                cte_id: op.cte_id,
                output_columns: op.output_columns.clone(),
            }),
            children: expr.children.clone(),
        }])
    }
}

// ---------------------------------------------------------------------------
// 12. CTEConsumeToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct CTEConsumeToPhysical;

impl Rule for CTEConsumeToPhysical {
    fn name(&self) -> &str {
        "CTEConsumeToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalCTEConsume(_))
    }
    fn apply(
        &self,
        expr: &MExpr,
        _memo: &mut Memo,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Vec<NewExpr>, crate::compiler::SqlCompileError> {
        let _ = control;

        let Operator::LogicalCTEConsume(op) = &expr.op else {
            return Ok(vec![]);
        };
        Ok(vec![NewExpr {
            op: Operator::PhysicalCTEConsume(CTEConsumeOp {
                cte_id: op.cte_id,
                alias: op.alias.clone(),
                output_columns: op.output_columns.clone(),
                producer_column_ids: op.producer_column_ids.clone(),
            }),
            children: expr.children.clone(),
        }])
    }
}

// ---------------------------------------------------------------------------
// 13. RepeatToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct RepeatToPhysical;

impl Rule for RepeatToPhysical {
    fn name(&self) -> &str {
        "RepeatToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalRepeat(_))
    }
    fn apply(
        &self,
        expr: &MExpr,
        _memo: &mut Memo,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Vec<NewExpr>, crate::compiler::SqlCompileError> {
        let _ = control;

        let Operator::LogicalRepeat(op) = &expr.op else {
            return Ok(vec![]);
        };
        Ok(vec![NewExpr {
            op: Operator::PhysicalRepeat(RepeatOp {
                repeat_column_ref_list: op.repeat_column_ref_list.clone(),
                repeat_column_ref_ids: op.repeat_column_ref_ids.clone(),
                grouping_ids: op.grouping_ids.clone(),
                all_rollup_columns: op.all_rollup_columns.clone(),
                all_rollup_column_ids: op.all_rollup_column_ids.clone(),
                grouping_key_aliases: op.grouping_key_aliases.clone(),
                grouping_fn_args: op.grouping_fn_args.clone(),
                grouping_fn_arg_ids: op.grouping_fn_arg_ids.clone(),
                grouping_fn_ids: op.grouping_fn_ids.clone(),
            }),
            children: expr.children.clone(),
        }])
    }
}

// ---------------------------------------------------------------------------
// 14. ChangeEventExpandToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct ChangeEventExpandToPhysical;

impl Rule for ChangeEventExpandToPhysical {
    fn name(&self) -> &str {
        "ChangeEventExpandToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalChangeEventExpand(_))
    }
    fn apply(
        &self,
        expr: &MExpr,
        _memo: &mut Memo,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Vec<NewExpr>, crate::compiler::SqlCompileError> {
        let _ = control;

        let Operator::LogicalChangeEventExpand(op) = &expr.op else {
            return Ok(vec![]);
        };
        Ok(vec![NewExpr {
            op: Operator::PhysicalChangeEventExpand(op.clone()),
            children: expr.children.clone(),
        }])
    }
}

// ---------------------------------------------------------------------------
// Content application has one exact two-input placement.
pub(crate) struct ContentApplyToPhysical;

impl Rule for ContentApplyToPhysical {
    fn name(&self) -> &str {
        "ContentApplyToPhysical"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalContentApply(_))
    }

    fn apply(
        &self,
        expr: &MExpr,
        _memo: &mut Memo,
        _control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Vec<NewExpr>, crate::compiler::SqlCompileError> {
        let Operator::LogicalContentApply(op) = &expr.op else {
            return Ok(Vec::new());
        };
        Ok(vec![NewExpr {
            op: Operator::PhysicalContentApply(op.clone()),
            children: expr.children.clone(),
        }])
    }
}

// 15. UnionToPhysical
// ---------------------------------------------------------------------------

fn refresh_set_op_child_output_columns(
    memo: &Memo,
    children: &[GroupId],
    output_len: usize,
    fallback: &[Vec<OutputColumn>],
) -> Vec<Vec<OutputColumn>> {
    children
        .iter()
        .enumerate()
        .map(|(idx, child)| {
            let current = memo
                .groups
                .get(*child)
                .and_then(|group| group.logical_props.as_ref())
                .map(|props| props.output_columns.clone());
            match current {
                Some(columns) if columns.len() == output_len => columns,
                _ => fallback.get(idx).cloned().unwrap_or_default(),
            }
        })
        .collect()
}

pub(crate) struct UnionToPhysical;

impl Rule for UnionToPhysical {
    fn name(&self) -> &str {
        "UnionToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalUnion(_))
    }
    fn apply(
        &self,
        expr: &MExpr,
        memo: &mut Memo,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Vec<NewExpr>, crate::compiler::SqlCompileError> {
        let _ = control;

        let Operator::LogicalUnion(op) = &expr.op else {
            return Ok(vec![]);
        };
        let child_output_columns = refresh_set_op_child_output_columns(
            memo,
            &expr.children,
            op.output_columns.len(),
            &op.child_output_columns,
        );
        Ok(vec![NewExpr {
            op: Operator::PhysicalUnion(UnionOp {
                all: op.all,
                output_columns: op.output_columns.clone(),
                child_output_columns,
            }),
            children: expr.children.clone(),
        }])
    }
}

// ---------------------------------------------------------------------------
// 15. IntersectToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct IntersectToPhysical;

impl Rule for IntersectToPhysical {
    fn name(&self) -> &str {
        "IntersectToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalIntersect(_))
    }
    fn apply(
        &self,
        expr: &MExpr,
        memo: &mut Memo,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Vec<NewExpr>, crate::compiler::SqlCompileError> {
        let _ = control;

        let Operator::LogicalIntersect(op) = &expr.op else {
            return Ok(vec![]);
        };
        let child_output_columns = refresh_set_op_child_output_columns(
            memo,
            &expr.children,
            op.output_columns.len(),
            &op.child_output_columns,
        );
        Ok(vec![NewExpr {
            op: Operator::PhysicalIntersect(IntersectOp {
                output_columns: op.output_columns.clone(),
                child_output_columns,
            }),
            children: expr.children.clone(),
        }])
    }
}

// ---------------------------------------------------------------------------
// 15. ExceptToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct ExceptToPhysical;

impl Rule for ExceptToPhysical {
    fn name(&self) -> &str {
        "ExceptToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalExcept(_))
    }
    fn apply(
        &self,
        expr: &MExpr,
        memo: &mut Memo,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Vec<NewExpr>, crate::compiler::SqlCompileError> {
        let _ = control;

        let Operator::LogicalExcept(op) = &expr.op else {
            return Ok(vec![]);
        };
        let child_output_columns = refresh_set_op_child_output_columns(
            memo,
            &expr.children,
            op.output_columns.len(),
            &op.child_output_columns,
        );
        Ok(vec![NewExpr {
            op: Operator::PhysicalExcept(ExceptOp {
                output_columns: op.output_columns.clone(),
                child_output_columns,
            }),
            children: expr.children.clone(),
        }])
    }
}

// ---------------------------------------------------------------------------
// 16. ValuesToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct ValuesToPhysical;

impl Rule for ValuesToPhysical {
    fn name(&self) -> &str {
        "ValuesToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalValues(_))
    }
    fn apply(
        &self,
        expr: &MExpr,
        _memo: &mut Memo,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Vec<NewExpr>, crate::compiler::SqlCompileError> {
        let _ = control;

        let Operator::LogicalValues(op) = &expr.op else {
            return Ok(vec![]);
        };
        Ok(vec![NewExpr {
            op: Operator::PhysicalValues(ValuesOp {
                rows: op.rows.clone(),
                columns: op.columns.clone(),
            }),
            children: expr.children.clone(),
        }])
    }
}

/// Declaration-only metadata leaf; provider completion supplies data later.
pub(crate) struct SystemTableToPhysical;
impl Rule for SystemTableToPhysical {
    fn name(&self) -> &str {
        "SystemTableToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalSystemTable(_))
    }
    fn apply(
        &self,
        expr: &MExpr,
        _memo: &mut Memo,
        _control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Vec<NewExpr>, crate::compiler::SqlCompileError> {
        let Operator::LogicalSystemTable(table) = &expr.op else {
            return Ok(vec![]);
        };
        Ok(vec![NewExpr {
            op: Operator::PhysicalSystemTable(table.clone()),
            children: expr.children.clone(),
        }])
    }
}

// ---------------------------------------------------------------------------
// 17. GenerateSeriesToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct GenerateSeriesToPhysical;

impl Rule for GenerateSeriesToPhysical {
    fn name(&self) -> &str {
        "GenerateSeriesToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalGenerateSeries(_))
    }
    fn apply(
        &self,
        expr: &MExpr,
        _memo: &mut Memo,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Vec<NewExpr>, crate::compiler::SqlCompileError> {
        let _ = control;

        let Operator::LogicalGenerateSeries(op) = &expr.op else {
            return Ok(vec![]);
        };
        Ok(vec![NewExpr {
            op: Operator::PhysicalGenerateSeries(GenerateSeriesOp {
                start: op.start,
                end: op.end,
                step: op.step,
                column_name: op.column_name.clone(),
                alias: op.alias.clone(),
                output_column_id: op.output_column_id,
            }),
            children: expr.children.clone(),
        }])
    }
}

// ---------------------------------------------------------------------------
// 18. TableFunctionToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct TableFunctionToPhysical;

impl Rule for TableFunctionToPhysical {
    fn name(&self) -> &str {
        "TableFunctionToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalTableFunction(_))
    }
    fn apply(
        &self,
        expr: &MExpr,
        _memo: &mut Memo,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Vec<NewExpr>, crate::compiler::SqlCompileError> {
        let _ = control;

        let Operator::LogicalTableFunction(op) = &expr.op else {
            return Ok(vec![]);
        };
        Ok(vec![NewExpr {
            op: Operator::PhysicalTableFunction(TableFunctionOp {
                function_name: op.function_name.clone(),
                args: op.args.clone(),
                binding: op.binding.clone(),
                output_columns: op.output_columns.clone(),
                alias: op.alias.clone(),
                is_left_join: op.is_left_join,
            }),
            children: expr.children.clone(),
        }])
    }
}

#[cfg(test)]
mod top_n_tests {
    use super::*;
    use crate::optimizer::memo::{MExpr, Memo};
    use crate::optimizer::operator::{GenerateSeriesOp, TopNOp, TopNPhase};

    #[test]
    fn top_n_to_physical_produces_physical_top_n() {
        let mut memo = Memo::new();
        let values_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        };
        let dummy_child = memo.new_group(values_mexpr);

        let expr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(TopNOp {
                items: vec![],
                limit: Some(50),
                offset: Some(10),
                phase: TopNPhase::Final,
                is_split: false,
            }),
            children: vec![dummy_child],
        };
        let rule = TopNToPhysical;
        let out = rule
            .apply(
                &expr,
                &mut memo,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap();
        assert_eq!(out.len(), 1);
        match &out[0].op {
            Operator::PhysicalTopN(p) => {
                assert_eq!(p.limit, Some(50));
                assert_eq!(p.offset, Some(10));
            }
            other => panic!("expected PhysicalTopN, got {:?}", other),
        }
        assert_eq!(out[0].children, vec![dummy_child]);
    }

    #[test]
    fn generate_series_to_physical_preserves_output_column_id() {
        let mut memo = Memo::new();
        let output_column_id = ColumnId::new_for_test(8101);
        let expr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalGenerateSeries(GenerateSeriesOp {
                start: 1,
                end: 10,
                step: 2,
                column_name: "x".to_string(),
                alias: Some("gs".to_string()),
                output_column_id,
            }),
            children: vec![],
        };

        let out = GenerateSeriesToPhysical
            .apply(
                &expr,
                &mut memo,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap();
        assert_eq!(out.len(), 1);
        let Operator::PhysicalGenerateSeries(p) = &out[0].op else {
            panic!("expected PhysicalGenerateSeries");
        };
        assert_eq!(p.output_column_id, output_column_id);
        assert_eq!(p.column_name, "x");
        assert_eq!(p.alias.as_deref(), Some("gs"));
    }
}

#[cfg(test)]
mod set_op_tests {
    use super::*;
    use crate::analysis::OutputColumn;
    use crate::column_id::ColumnId;
    use crate::optimizer::memo::{LogicalProperties, MExpr, Memo};
    use crate::optimizer::operator::ValuesOp;
    use arrow::datatypes::DataType;

    fn output_column(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),

            is_internal: false,
        }
    }

    fn values_group_with_outputs(memo: &mut Memo, columns: Vec<OutputColumn>) -> usize {
        let id = memo.next_expr_id();
        let group = memo.new_group(MExpr {
            id,
            op: Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: columns.clone(),
            }),
            children: vec![],
        });
        memo.groups[group].logical_props = Some(LogicalProperties::new(columns, 0.0));
        group
    }

    #[test]
    fn union_to_physical_refreshes_child_outputs_from_child_groups() {
        let mut memo = Memo::new();
        let left_current = output_column(101, "total");
        let right_current = output_column(201, "total");
        let left_group = values_group_with_outputs(&mut memo, vec![left_current.clone()]);
        let right_group = values_group_with_outputs(&mut memo, vec![right_current.clone()]);

        let logical = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalUnion(UnionOp {
                all: true,
                output_columns: vec![output_column(301, "total")],
                child_output_columns: vec![
                    vec![output_column(11, "total")],
                    vec![output_column(21, "total")],
                ],
            }),
            children: vec![left_group, right_group],
        };

        let out = UnionToPhysical
            .apply(
                &logical,
                &mut memo,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap();
        assert_eq!(out.len(), 1);
        let Operator::PhysicalUnion(union) = &out[0].op else {
            panic!("expected PhysicalUnion");
        };
        assert_eq!(union.child_output_columns.len(), 2);
        assert_eq!(union.child_output_columns[0].len(), 1);
        assert_eq!(union.child_output_columns[1].len(), 1);
        assert_eq!(
            union.child_output_columns[0][0].column_id,
            left_current.column_id
        );
        assert_eq!(union.child_output_columns[0][0].name, left_current.name);
        assert_eq!(
            union.child_output_columns[1][0].column_id,
            right_current.column_id
        );
        assert_eq!(union.child_output_columns[1][0].name, right_current.name);
    }
}

#[cfg(test)]
mod eq_pair_tests {
    use super::*;
    use crate::analysis::{ExprKind, TypedExpr};
    use crate::column_id::ColumnId;
    use crate::optimizer::scalar::{ScalarArena, ScalarId};

    use crate::planner::optimizer_bridge::scalar::{intern_typed, materialize};
    use arrow::datatypes::DataType;

    fn col_id(name: &str, id: u32) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(id),
                qualifier: None,
                column: name.into(),
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int32, false),
        }
    }

    fn ids(values: &[u32]) -> HashSet<ColumnId> {
        values.iter().copied().map(ColumnId).collect()
    }

    fn eq_pair(
        arena: &mut ScalarArena,
        left: TypedExpr,
        right: TypedExpr,
    ) -> ScalarHashJoinEqCondition {
        let expression = TypedExpr {
            value_type: novarocks_type_contract::FunctionValueType::new(
                DataType::Boolean,
                left.value_type.nullable() || right.value_type.nullable(),
            ),
            kind: ExprKind::BinaryOp {
                left: novarocks_parser::ast::AstBox::new(left),
                op: BinOp::Eq,
                right: novarocks_parser::ast::AstBox::new(right),
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
        };
        let origin = intern_typed(
            arena,
            &expression,
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();
        comparison_key(arena, origin).expect("fixture equality")
    }

    fn assert_column_name(arena: &ScalarArena, expr: ScalarId, expected: &str) {
        let expr = materialize(arena, expr);
        match &expr.kind {
            ExprKind::ColumnRef { column, .. } => assert_eq!(column, expected),
            _ => panic!("expected ColumnRef"),
        }
    }

    fn assert_column_id(arena: &ScalarArena, expr: ScalarId, expected: ColumnId) {
        let expr = materialize(arena, expr);
        match &expr.kind {
            ExprKind::ColumnRef { column_id, .. } => assert_eq!(*column_id, expected),
            _ => panic!("expected ColumnRef"),
        }
    }

    #[test]
    fn orient_natural_order_keeps_order() {
        let mut arena = ScalarArena::new();
        let left_ids = ids(&[10]);
        let right_ids = ids(&[20]);
        let pair = eq_pair(&mut arena, col_id("a_id", 10), col_id("b_id", 20));
        let out = orient_eq_pair(pair, &arena, &left_ids, &right_ids).expect("should orient");
        assert_column_name(&arena, out.left, "a_id");
        assert_column_name(&arena, out.right, "b_id");
    }

    #[test]
    fn orient_swapped_pair_returns_swapped() {
        let mut arena = ScalarArena::new();
        let left_ids = ids(&[10]);
        let right_ids = ids(&[20]);
        let pair = eq_pair(&mut arena, col_id("b_id", 20), col_id("a_id", 10));
        let out = orient_eq_pair(pair, &arena, &left_ids, &right_ids).expect("should orient");
        assert_column_name(&arena, out.left, "a_id");
        assert_column_name(&arena, out.right, "b_id");
    }

    #[test]
    fn orient_single_side_pair_returns_none() {
        let mut arena = ScalarArena::new();
        let left_ids = ids(&[10, 11]);
        let right_ids = ids(&[20]);
        let pair = eq_pair(&mut arena, col_id("a_id", 10), col_id("a_name", 11));
        assert!(orient_eq_pair(pair, &arena, &left_ids, &right_ids).is_none());
    }

    #[test]
    fn orient_uses_column_ids_when_names_are_ambiguous() {
        let mut arena = ScalarArena::new();
        let left_ids = ids(&[10]);
        let right_ids = ids(&[20]);
        let pair = eq_pair(&mut arena, col_id("id", 20), col_id("id", 10));
        let out = orient_eq_pair(pair, &arena, &left_ids, &right_ids)
            .expect("column ids should disambiguate the join sides");

        assert_column_id(&arena, out.left, ColumnId(10));
        assert_column_id(&arena, out.right, ColumnId(20));
    }
}

#[cfg(test)]
mod join_demotion_tests;

#[cfg(test)]
mod window_split_tests {
    use super::*;
    use crate::analysis::{ExprKind, OutputColumn, TypedExpr};
    use crate::column_id::ColumnId;
    use crate::planner::optimizer_bridge::scalar::intern_window_exprs;
    use crate::planner::payload::WindowExpr;
    use arrow::datatypes::DataType;

    fn window_output_id(name: &str) -> ColumnId {
        let id = match name {
            "w1" => 1001,
            "w2" => 1002,
            "w3" => 1003,
            "w" => 1004,
            other => panic!("unexpected window fixture name {other}"),
        };
        ColumnId::new_for_test(id)
    }

    fn mk_window_expr(name: &str, partition: Vec<TypedExpr>) -> WindowExpr {
        WindowExpr {
            name: name.into(),
            args: vec![],
            distinct: false,
            binding: crate::analysis::test_window_binding(name, &[], DataType::Int64, true),
            function_order_by: vec![],
            aggregate_binding: None,
            partition_by: partition,
            order_by: vec![],
            window_frame: None,
            result_type: DataType::Int64,
            output_name: name.into(),
            output_column_id: window_output_id(name),
            ignore_nulls: false,
        }
    }

    fn window_output_column(name: &str) -> OutputColumn {
        OutputColumn {
            column_id: window_output_id(name),
            name: name.into(),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),

            is_internal: false,
        }
    }

    fn col(name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::UNSET,
                qualifier: None,
                column: name.into(),
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int32, false),
        }
    }

    #[test]
    fn split_groups_same_signature_together() {
        let exprs = vec![
            mk_window_expr("w1", vec![col("a")]),
            mk_window_expr("w2", vec![col("a")]),
        ];
        let groups = split_window_exprs_by_signature(&exprs);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].len(), 2);
    }

    #[test]
    fn split_separates_different_signatures() {
        let a1 = mk_window_expr("w1", vec![col("a")]);
        let b = mk_window_expr("w2", vec![col("b")]);
        let a2 = mk_window_expr("w3", vec![col("a")]);
        let groups = split_window_exprs_by_signature(&[a1, b, a2]);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].len(), 2);
        assert_eq!(groups[0][0].name, "w1");
        assert_eq!(groups[0][1].name, "w3");
        assert_eq!(groups[1].len(), 1);
        assert_eq!(groups[1][0].name, "w2");
    }

    #[test]
    fn sort_items_for_window_combines_partition_and_order() {
        use crate::analysis::SortItem;
        let win = WindowExpr {
            name: "w".into(),
            args: vec![],
            distinct: false,
            binding: crate::analysis::test_window_binding("w", &[], DataType::Int64, true),
            function_order_by: vec![],
            aggregate_binding: None,
            partition_by: vec![col("a"), col("b")],
            order_by: vec![SortItem {
                expr: col("c"),
                asc: false,
                nulls_first: false,
            }],
            window_frame: None,
            result_type: DataType::Int64,
            output_name: "w".into(),
            output_column_id: window_output_id("w"),
            ignore_nulls: false,
        };
        let items = sort_items_for_window(&win);
        assert_eq!(items.len(), 3);
        // partition_by items are ASC NULLS FIRST
        assert!(items[0].asc);
        assert!(items[0].nulls_first);
        assert!(items[1].asc);
        assert!(items[1].nulls_first);
        // order_by item preserves its own direction
        assert!(!items[2].asc);
        assert!(!items[2].nulls_first);
    }

    // -----------------------------------------------------------------------
    // Integration tests: WindowToPhysical.apply
    // -----------------------------------------------------------------------
    //
    // Note: a previous revision of this module decomposed multi-signature
    // LogicalWindow operators into a chain of single-signature PhysicalWindow
    // nodes separated by PhysicalSort. That feature was reverted in the Phase
    // 2 hardening commit because the chain triggered cascades search
    // recursion into newly-allocated groups whose physical_exprs were not yet
    // implemented. The deleted test
    // `window_to_physical_builds_chain_for_multi_group` asserted chain shape
    // and is therefore obsolete. Multi-group decomposition still happens in
    // `build_distributed_plan` window lowering
    // (planner/distributed/build/lowering.rs), rather than at the cascades rule level.

    /// Single window expression with empty partition_by and empty order_by.
    /// The signature is empty → single group → no PhysicalSort inserted.
    /// The rule should return a single NewExpr whose child is the original child_group.
    #[test]
    fn window_to_physical_skips_sort_when_empty_signature() {
        let mut memo = Memo::new();

        let values_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        };
        let child_group = memo.new_group(values_mexpr);

        // Single window with no partition and no order => single group, no sort.
        let window_exprs = intern_window_exprs(
            &mut memo.scalars,
            &[mk_window_expr("w1", vec![])],
            crate::optimizer::test_optimizer_control(),
        )
        .unwrap();
        let logical_window_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalWindow(WindowOp {
                window_exprs,
                output_columns: vec![window_output_column("w1")],
            }),
            children: vec![child_group],
        };

        let rule = WindowToPhysical;
        let out = rule
            .apply(
                &logical_window_mexpr,
                &mut memo,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap();

        assert_eq!(out.len(), 1);
        let terminal = &out[0];
        match &terminal.op {
            Operator::PhysicalWindow(p) => {
                assert_eq!(
                    p.window_exprs.len(),
                    1,
                    "should have exactly one window expr"
                );
            }
            other => panic!("expected PhysicalWindow, got {:?}", other),
        }
        // Single-group path with empty signature: child is the original child_group,
        // no PhysicalSort inserted.
        assert_eq!(
            terminal.children,
            vec![child_group],
            "no sort should be inserted for empty signature"
        );
    }
}

#[cfg(test)]
mod two_phase_agg_tests {
    use super::*;
    use crate::analysis::{ExprKind, OutputColumn, TypedExpr};
    use crate::column_id::ColumnId;
    use crate::optimizer::memo::{MExpr, Memo};
    use crate::planner::optimizer_bridge::scalar::{intern_aggregate_calls, intern_exprs};
    use crate::planner::payload::AggregateCall;
    use arrow::datatypes::DataType;

    fn test_col_id(name: &str) -> ColumnId {
        match name {
            "k" => ColumnId::new_for_test(1),
            "v" => ColumnId::new_for_test(2),
            "city" => ColumnId::new_for_test(4),
            "id" => ColumnId::new_for_test(5),
            _ => ColumnId::new_for_test(100),
        }
    }

    fn col(name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: test_col_id(name),
                qualifier: None,
                column: name.into(),
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int32, false),
        }
    }

    fn output_column(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.into(),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),

            is_internal: false,
        }
    }

    fn count_call(arg: &str, distinct: bool) -> AggregateCall {
        AggregateCall {
            name: "count".into(),
            distinct,
            result_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
            output_column_id: ColumnId::new_for_test(3),
            source: crate::binding::AggregateArgumentSource::uncertified(
                vec![col(arg)],
                vec![],
                crate::functions::test_resolved_aggregate("count", &[DataType::Int64], distinct),
            ),
        }
    }

    fn single_agg(
        memo: &mut Memo,
        group_by: Vec<TypedExpr>,
        aggregates: Vec<AggregateCall>,
        output_columns: Vec<OutputColumn>,
    ) -> LogicalAggregateOp {
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
        let output_layout = AggregateOutputLayout::new(
            output_columns
                .iter()
                .take(group_by.len())
                .cloned()
                .collect(),
            output_columns
                .iter()
                .skip(group_by.len())
                .cloned()
                .collect(),
        );
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
        let output_layout = AggregateOutputLayout::new(
            output_columns
                .iter()
                .take(group_by.len())
                .cloned()
                .collect(),
            output_columns
                .iter()
                .skip(group_by.len())
                .cloned()
                .collect(),
        );
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

    #[test]
    fn agg_to_hash_agg_lowers_single_to_one_physical_single() {
        let mut memo = Memo::new();
        let child_group = values_group(&mut memo);
        let expr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(single_agg(
                &mut memo,
                vec![col("k")],
                vec![count_call("v", false)],
                vec![output_column(1, "k"), output_column(3, "count(v)")],
            )),
            children: vec![child_group],
        };

        let out = AggToHashAgg
            .apply(
                &expr,
                &mut memo,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap();
        assert_eq!(out.len(), 1);
        let Operator::PhysicalHashAggregate(op) = &out[0].op else {
            panic!("expected physical hash aggregate");
        };
        assert_eq!(op.mode, AggMode::Single);
        assert_eq!(op.is_merge, vec![false]);
        assert_eq!(out[0].children, vec![child_group]);
    }

    #[test]
    fn agg_to_hash_agg_lowers_split_stages_without_creating_extra_alternatives() {
        let mut memo = Memo::new();
        let child_group = values_group(&mut memo);
        let local_expr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(staged_agg(
                &mut memo,
                AggStage::Local,
                vec![col("k")],
                vec![count_call("v", false)],
                vec![output_column(1, "k"), output_column(3, "count(v)")],
                vec![false],
                true,
            )),
            children: vec![child_group],
        };
        let local_out = AggToHashAgg
            .apply(
                &local_expr,
                &mut memo,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap();
        assert_eq!(local_out.len(), 1);
        let Operator::PhysicalHashAggregate(local) = &local_out[0].op else {
            panic!("expected local physical aggregate");
        };
        assert_eq!(local.mode, AggMode::Local);
        assert_eq!(local.is_merge, vec![false]);
        assert_eq!(local_out[0].children, vec![child_group]);

        let local_group = values_group(&mut memo);
        let global_expr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(staged_agg(
                &mut memo,
                AggStage::Global,
                vec![col("k")],
                vec![count_call("v", false)],
                vec![output_column(1, "k"), output_column(3, "count(v)")],
                vec![true],
                true,
            )),
            children: vec![local_group],
        };
        let global_out = AggToHashAgg
            .apply(
                &global_expr,
                &mut memo,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap();
        assert_eq!(global_out.len(), 1);
        let Operator::PhysicalHashAggregate(global) = &global_out[0].op else {
            panic!("expected global physical aggregate");
        };
        assert_eq!(global.mode, AggMode::Global);
        assert_eq!(global.is_merge, vec![true]);
        assert_eq!(global_out[0].children, vec![local_group]);
    }

    #[test]
    fn agg_to_hash_agg_skips_two_phase_for_distinct() {
        let mut memo = Memo::new();
        let child_group = values_group(&mut memo);

        let expr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(single_agg(
                &mut memo,
                vec![col("city")],
                vec![AggregateCall {
                    name: "count".into(),
                    distinct: true,
                    result_type: novarocks_type_contract::FunctionValueType::new(
                        DataType::Int64,
                        false,
                    ),
                    output_column_id: ColumnId::new_for_test(6),
                    source: crate::binding::AggregateArgumentSource::uncertified(
                        vec![col("id")],
                        vec![],
                        crate::functions::test_resolved_aggregate(
                            "count",
                            &[DataType::Int64],
                            true,
                        ),
                    ),
                }],
                vec![
                    OutputColumn {
                        column_id: test_col_id("city"),
                        name: "city".into(),
                        value_type: novarocks_type_contract::FunctionValueType::new(
                            DataType::Int32,
                            false,
                        ),

                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: ColumnId::new_for_test(6),
                        name: "count(distinct id)".into(),
                        value_type: novarocks_type_contract::FunctionValueType::new(
                            DataType::Int64,
                            true,
                        ),

                        is_internal: false,
                    },
                ],
            )),
            children: vec![child_group],
        };

        let rule = AggToHashAgg;
        let out = rule
            .apply(
                &expr,
                &mut memo,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap();

        assert_eq!(out.len(), 1, "DISTINCT agg should only produce Single");
        match &out[0].op {
            Operator::PhysicalHashAggregate(p) => {
                assert!(matches!(p.mode, AggMode::Single));
            }
            other => panic!("expected PhysicalHashAggregate(Single), got {:?}", other),
        }
    }
}

#[cfg(test)]
#[path = "naa_admission_tests.rs"]
mod naa_admission_tests;
