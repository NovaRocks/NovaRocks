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

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use arrow::datatypes::DataType;

use crate::column_id::ColumnId;
use crate::common::{BinOp, LiteralValue};
use crate::optimizer::rewrite::rules::predicate_pushdown::predicate_group::{
    PredicateDerivedKind, PredicateGroup, PredicateOrigin, dedupe_groups,
};
use crate::optimizer::scalar::{HashableLiteral, ScalarArena, ScalarId, ScalarNode};
use crate::optimizer::scalar_expr;

pub(crate) fn derive_inner_join_predicates(
    arena: &mut ScalarArena,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
    join_groups: &[PredicateGroup],
    filter_groups: &[PredicateGroup],
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Vec<PredicateGroup>, crate::compiler::SqlCompileError> {
    if join_groups
        .iter()
        .chain(filter_groups.iter())
        .any(|group| !group.deterministic || group.can_fail)
    {
        return Ok(Vec::new());
    }

    let mut equality_pairs = Vec::new();
    for group in join_groups.iter().chain(filter_groups.iter()) {
        let mut conjuncts = Vec::new();
        scalar_expr::split_conjuncts(arena, group.expr, &mut conjuncts);
        for conjunct in conjuncts {
            if let Some(pair) = extract_column_pair_equality(arena, conjunct) {
                equality_pairs.push(pair);
            }
        }
    }

    let mut constraints = Vec::new();
    for group in join_groups.iter().chain(filter_groups.iter()) {
        let mut conjuncts = Vec::new();
        scalar_expr::split_conjuncts(arena, group.expr, &mut conjuncts);
        for conjunct in conjuncts {
            if let Some(constraint) = extract_column_literal_constraint(arena, conjunct) {
                constraints.push(constraint);
            }
        }
    }

    let mut derived = Vec::new();
    for (left, right) in equality_pairs {
        if !is_cross_side_pair(arena, left, right, left_ids, right_ids) {
            continue;
        }
        for constraint in &constraints {
            let target = if same_column(arena, constraint.column, left) {
                transfer_comparison_operand(arena, constraint.column, left, right, control)?
            } else if same_column(arena, constraint.column, right) {
                transfer_comparison_operand(arena, constraint.column, right, left, control)?
            } else {
                None
            };
            if let Some(target) = target
                && let Some(group) = substitute_constraint_column(
                    arena,
                    constraint,
                    target,
                    derived_kind_for_constraint(&constraint.kind),
                    control,
                )?
            {
                derived.push(group);
            }
        }
    }

    for group in filter_groups {
        derived.extend(derive_or_branch_side_filters(
            arena, left_ids, right_ids, group, control,
        )?);
    }

    Ok(dedupe_groups(derived))
}

fn extract_column_pair_equality(
    arena: &ScalarArena,
    expr: ScalarId,
) -> Option<(ScalarId, ScalarId)> {
    match arena.node(expr) {
        ScalarNode::Nested(inner) => extract_column_pair_equality(arena, *inner),
        ScalarNode::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
            ..
        } if is_column_ref(arena, *left) && is_column_ref(arena, *right) => Some((*left, *right)),
        _ => None,
    }
}

fn extract_column_literal_constraint(
    arena: &ScalarArena,
    expr: ScalarId,
) -> Option<ColumnConstraint> {
    match arena.node(expr) {
        ScalarNode::Nested(inner) => extract_column_literal_constraint(arena, *inner),
        ScalarNode::BinaryOp {
            left, op, right, ..
        } => {
            if is_constraint_operand(arena, *left) && is_literal(arena, *right) {
                return build_binary_constraint(*left, *op, *right);
            }
            if is_literal(arena, *left) && is_constraint_operand(arena, *right) {
                return build_binary_constraint(*right, reverse_comparison(*op)?, *left);
            }
            None
        }
        ScalarNode::InList {
            child,
            list,
            negated: false,
        } if is_constraint_operand(arena, *child)
            && list.iter().all(|item| is_literal(arena, *item)) =>
        {
            Some(ColumnConstraint {
                column: *child,
                kind: ConstraintKind::InList(list.clone()),
            })
        }
        ScalarNode::Between {
            child,
            low,
            high,
            negated: false,
        } if is_constraint_operand(arena, *child)
            && is_literal(arena, *low)
            && is_literal(arena, *high) =>
        {
            Some(ColumnConstraint {
                column: *child,
                kind: ConstraintKind::Between {
                    low: *low,
                    high: *high,
                },
            })
        }
        _ => None,
    }
}

fn substitute_constraint_column(
    arena: &mut ScalarArena,
    constraint: &ColumnConstraint,
    target_column: ScalarId,
    derived: PredicateDerivedKind,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Option<PredicateGroup>, crate::compiler::SqlCompileError> {
    if !is_constraint_operand(arena, target_column) {
        return Ok(None);
    }
    if arena.data_type(constraint.column) != arena.data_type(target_column) {
        return Ok(None);
    }
    // New cast/CV paths need the original complete comparison binding. Legacy
    // direct-column syntax algorithms retain their existing admission below.
    if (widening_source(arena, constraint.column).is_some()
        || widening_source(arena, target_column).is_some()
        || constraint_has_checked_bound(arena, constraint))
        && (!same_complete_type(arena, constraint.column, target_column, control)?
            || !bounds_match_operand(arena, constraint, control)?)
    {
        return Ok(None);
    }
    let expr = match &constraint.kind {
        ConstraintKind::Eq(value) => binary_bool(arena, target_column, BinOp::Eq, *value, control)?,
        ConstraintKind::InList(list) => {
            let nullable =
                arena.nullable(target_column) || list.iter().any(|expr| arena.nullable(*expr));
            arena.intern_observed(
                ScalarNode::InList {
                    child: target_column,
                    list: list.clone(),
                    negated: false,
                },
                novarocks_type_contract::FunctionValueType::new(DataType::Boolean, nullable),
                control,
            )?
        }
        ConstraintKind::Lower { op, value } | ConstraintKind::Upper { op, value } => {
            binary_bool(arena, target_column, *op, *value, control)?
        }
        ConstraintKind::Between { low, high } => {
            let nullable =
                arena.nullable(target_column) || arena.nullable(*low) || arena.nullable(*high);
            arena.intern_observed(
                ScalarNode::Between {
                    child: target_column,
                    low: *low,
                    high: *high,
                    negated: false,
                },
                novarocks_type_contract::FunctionValueType::new(DataType::Boolean, nullable),
                control,
            )?
        }
    };
    Ok(Some(PredicateGroup::new(
        arena,
        expr,
        PredicateOrigin::Derived,
        derived,
    )))
}

fn derive_or_branch_side_filters(
    arena: &mut ScalarArena,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
    group: &PredicateGroup,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Vec<PredicateGroup>, crate::compiler::SqlCompileError> {
    if !group.deterministic || group.can_fail {
        return Ok(Vec::new());
    }

    let mut branches = Vec::new();
    scalar_expr::split_disjuncts(arena, group.expr, &mut branches);
    if branches.len() < 2 {
        return Ok(Vec::new());
    }

    let mut per_branch = Vec::with_capacity(branches.len());
    for branch in branches {
        let mut pairs = Vec::new();
        let mut constraints = Vec::new();
        let mut conjuncts = Vec::new();
        scalar_expr::split_conjuncts(arena, branch, &mut conjuncts);
        for conjunct in conjuncts {
            if let Some(pair) = extract_column_pair_equality(arena, conjunct)
                && is_cross_side_pair(arena, pair.0, pair.1, left_ids, right_ids)
            {
                pairs.push(pair);
            }
            if let Some(constraint) = extract_column_literal_constraint(arena, conjunct) {
                constraints.push(constraint);
            }
        }

        let mut candidates = constraints.clone();
        for (left, right) in pairs {
            for constraint in &constraints {
                let target = if same_column(arena, constraint.column, left) {
                    transfer_comparison_operand(arena, constraint.column, left, right, control)?
                } else if same_column(arena, constraint.column, right) {
                    transfer_comparison_operand(arena, constraint.column, right, left, control)?
                } else {
                    None
                };
                if let Some(target) = target {
                    candidates.push(ColumnConstraint {
                        column: target,
                        kind: constraint.kind.clone(),
                    });
                }
            }
        }

        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        let mut by_comparison: HashMap<ScalarId, Vec<ColumnConstraint>> = HashMap::new();
        let mut by_side: HashMap<Side, Vec<ColumnConstraint>> = HashMap::new();
        for candidate in candidates {
            if let Some(side) = column_side(arena, candidate.column, left_ids, right_ids) {
                // ScalarId includes the complete typed structural expression;
                // two casts of one base column cannot merge comparison domains.
                by_comparison
                    .entry(candidate.column)
                    .or_default()
                    .push(candidate.clone());
                by_side.entry(side).or_default().push(candidate);
            }
        }
        if by_comparison.is_empty() {
            return Ok(Vec::new());
        }
        per_branch.push(BranchCandidates {
            by_comparison,
            by_side,
        });
    }

    let mut derived = Vec::new();
    let mut specialized_sides = HashSet::new();
    let first_comparisons: Vec<ScalarId> = per_branch[0].by_comparison.keys().copied().collect();
    for comparison in first_comparisons {
        if !per_branch
            .iter()
            .all(|branch| branch.by_comparison.contains_key(&comparison))
        {
            continue;
        }

        if let Some(group) = derive_or_in_list(arena, comparison, &per_branch, control)? {
            if let Some(side) = single_column_side(arena, group.expr, left_ids, right_ids) {
                specialized_sides.insert(side);
            }
            derived.push(group);
        } else if let Some(group) =
            derive_or_range_envelope(arena, comparison, &per_branch, control)?
        {
            if let Some(side) = single_column_side(arena, group.expr, left_ids, right_ids) {
                specialized_sides.insert(side);
            }
            derived.push(group);
        }
    }

    for side in [Side::Left, Side::Right] {
        if specialized_sides.contains(&side) {
            continue;
        }
        if let Some(group) = derive_or_side_fallback(arena, side, &per_branch, control)? {
            derived.push(group);
        }
    }

    Ok(dedupe_groups(derived))
}

#[derive(Clone, Debug)]
struct BranchCandidates {
    by_comparison: HashMap<ScalarId, Vec<ColumnConstraint>>,
    by_side: HashMap<Side, Vec<ColumnConstraint>>,
}

#[derive(Clone, Debug)]
struct ColumnConstraint {
    column: ScalarId,
    kind: ConstraintKind,
}

#[derive(Clone, Debug)]
enum ConstraintKind {
    Eq(ScalarId),
    InList(Vec<ScalarId>),
    Lower { op: BinOp, value: ScalarId },
    Upper { op: BinOp, value: ScalarId },
    Between { low: ScalarId, high: ScalarId },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Side {
    Left,
    Right,
}

fn build_binary_constraint(
    column: ScalarId,
    op: BinOp,
    value: ScalarId,
) -> Option<ColumnConstraint> {
    let kind = match op {
        BinOp::Eq => ConstraintKind::Eq(value),
        BinOp::Gt | BinOp::Ge => ConstraintKind::Lower { op, value },
        BinOp::Lt | BinOp::Le => ConstraintKind::Upper { op, value },
        _ => return None,
    };
    Some(ColumnConstraint { column, kind })
}

fn reverse_comparison(op: BinOp) -> Option<BinOp> {
    match op {
        BinOp::Eq => Some(BinOp::Eq),
        BinOp::Gt => Some(BinOp::Lt),
        BinOp::Ge => Some(BinOp::Le),
        BinOp::Lt => Some(BinOp::Gt),
        BinOp::Le => Some(BinOp::Ge),
        _ => None,
    }
}

fn derived_kind_for_constraint(kind: &ConstraintKind) -> PredicateDerivedKind {
    match kind {
        ConstraintKind::Eq(_) | ConstraintKind::InList(_) => PredicateDerivedKind::Equivalence,
        ConstraintKind::Lower { .. }
        | ConstraintKind::Upper { .. }
        | ConstraintKind::Between { .. } => PredicateDerivedKind::Range,
    }
}

fn derive_or_in_list(
    arena: &mut ScalarArena,
    comparison: ScalarId,
    per_branch: &[BranchCandidates],
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Option<PredicateGroup>, crate::compiler::SqlCompileError> {
    let Some(constraint) = (|| {
        let first = per_branch[0]
            .by_comparison
            .get(&comparison)?
            .first()?
            .column;
        let mut values = Vec::new();
        for branch in per_branch {
            let constraints = branch.by_comparison.get(&comparison)?;
            let eq = constraints
                .iter()
                .find_map(|constraint| match &constraint.kind {
                    ConstraintKind::Eq(value) => Some(*value),
                    _ => None,
                })?;
            values.push(eq);
        }

        Some(ColumnConstraint {
            column: first,
            kind: ConstraintKind::InList(values),
        })
    })() else {
        return Ok(None);
    };
    substitute_constraint_column(
        arena,
        &constraint,
        constraint.column,
        PredicateDerivedKind::OrSideFilter,
        control,
    )
}

fn derive_or_range_envelope(
    arena: &mut ScalarArena,
    comparison: ScalarId,
    per_branch: &[BranchCandidates],
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Option<PredicateGroup>, crate::compiler::SqlCompileError> {
    for branch in per_branch {
        if let Some(constraints) = branch.by_comparison.get(&comparison) {
            for constraint in constraints {
                if (widening_source(arena, constraint.column).is_some()
                    || constraint_has_checked_bound(arena, constraint))
                    && !bounds_match_operand(arena, constraint, control)?
                {
                    return Ok(None);
                }
            }
        }
    }
    let Some(constraint) = (|| {
        let first = per_branch[0]
            .by_comparison
            .get(&comparison)?
            .first()?
            .column;
        let mut low: Option<ScalarId> = None;
        let mut high: Option<ScalarId> = None;

        for branch in per_branch {
            let constraints = branch.by_comparison.get(&comparison)?;
            let (branch_low, branch_high) = branch_range(arena, constraints)?;
            low = Some(match low {
                Some(current)
                    if compare_literals(arena, current, branch_low)? != Ordering::Greater =>
                {
                    current
                }
                _ => branch_low,
            });
            high = Some(match high {
                Some(current)
                    if compare_literals(arena, current, branch_high)? != Ordering::Less =>
                {
                    current
                }
                _ => branch_high,
            });
        }

        Some(ColumnConstraint {
            column: first,
            kind: ConstraintKind::Between {
                low: low?,
                high: high?,
            },
        })
    })() else {
        return Ok(None);
    };
    substitute_constraint_column(
        arena,
        &constraint,
        constraint.column,
        PredicateDerivedKind::RangeEnvelope,
        control,
    )
}

fn branch_range(
    arena: &ScalarArena,
    constraints: &[ColumnConstraint],
) -> Option<(ScalarId, ScalarId)> {
    if let Some((low, high)) = constraints
        .iter()
        .find_map(|constraint| match &constraint.kind {
            ConstraintKind::Between { low, high } => Some((*low, *high)),
            _ => None,
        })
    {
        ensure_comparable_literal(arena, low)?;
        ensure_comparable_literal(arena, high)?;
        return Some((low, high));
    }

    let mut low = None;
    let mut high = None;
    for constraint in constraints {
        match &constraint.kind {
            ConstraintKind::Lower { value, .. } => {
                ensure_comparable_literal(arena, *value)?;
                low = Some(*value);
            }
            ConstraintKind::Upper { value, .. } => {
                ensure_comparable_literal(arena, *value)?;
                high = Some(*value);
            }
            _ => {}
        }
    }
    Some((low?, high?))
}

fn ensure_comparable_literal(arena: &ScalarArena, expr: ScalarId) -> Option<()> {
    compare_literals(arena, expr, expr).map(|_| ())
}

fn compare_literals(arena: &ScalarArena, left: ScalarId, right: ScalarId) -> Option<Ordering> {
    // Only the checked signed family is new. Keep every legacy literal-only
    // numeric algorithm below unchanged, including exact decimal strings.
    if matches!(arena.node(left), ScalarNode::Constant(_))
        || matches!(arena.node(right), ScalarNode::Constant(_))
    {
        return Some(checked_signed_bound(arena, left)?.cmp(&checked_signed_bound(arena, right)?));
    }
    match (arena.node(left), arena.node(right)) {
        (
            ScalarNode::Literal(HashableLiteral(LiteralValue::Int(left))),
            ScalarNode::Literal(HashableLiteral(LiteralValue::Int(right))),
        ) => Some(left.cmp(right)),
        (
            ScalarNode::Literal(HashableLiteral(LiteralValue::LargeInt(left))),
            ScalarNode::Literal(HashableLiteral(LiteralValue::LargeInt(right))),
        ) => Some(left.cmp(right)),
        (
            ScalarNode::Literal(HashableLiteral(LiteralValue::Int(left))),
            ScalarNode::Literal(HashableLiteral(LiteralValue::LargeInt(right))),
        ) => Some(i128::from(*left).cmp(right)),
        (
            ScalarNode::Literal(HashableLiteral(LiteralValue::LargeInt(left))),
            ScalarNode::Literal(HashableLiteral(LiteralValue::Int(right))),
        ) => Some(left.cmp(&i128::from(*right))),
        (
            ScalarNode::Literal(HashableLiteral(LiteralValue::Float(left))),
            ScalarNode::Literal(HashableLiteral(LiteralValue::Float(right))),
        ) if left.is_finite() && right.is_finite() => left.partial_cmp(right),
        (
            ScalarNode::Literal(HashableLiteral(LiteralValue::Decimal(left))),
            ScalarNode::Literal(HashableLiteral(LiteralValue::Decimal(right))),
        ) => compare_decimal_strings(left, right),
        _ => None,
    }
}

fn compare_decimal_strings(left: &str, right: &str) -> Option<Ordering> {
    let left = DecimalLiteral::parse(left)?;
    let right = DecimalLiteral::parse(right)?;
    Some(left.cmp(&right))
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DecimalLiteral {
    negative: bool,
    int: String,
    frac: String,
}

impl DecimalLiteral {
    fn parse(input: &str) -> Option<Self> {
        let (negative, unsigned) = match input.as_bytes().first().copied() {
            Some(b'-') => (true, &input[1..]),
            Some(b'+') => (false, &input[1..]),
            _ => (false, input),
        };
        if unsigned.is_empty() {
            return None;
        }
        let mut parts = unsigned.split('.');
        let int_part = parts.next()?;
        let frac_part = parts.next();
        if parts.next().is_some() {
            return None;
        }
        let frac_part = frac_part.unwrap_or("");
        if int_part.is_empty() && frac_part.is_empty() {
            return None;
        }
        if !int_part.bytes().all(|byte| byte.is_ascii_digit())
            || !frac_part.bytes().all(|byte| byte.is_ascii_digit())
        {
            return None;
        }

        let int = int_part.trim_start_matches('0');
        let frac = frac_part.trim_end_matches('0');
        let int = if int.is_empty() { "0" } else { int }.to_string();
        let frac = frac.to_string();
        let is_zero = int == "0" && frac.is_empty();
        Some(Self {
            negative: negative && !is_zero,
            int,
            frac,
        })
    }

    fn cmp_abs(&self, other: &Self) -> Ordering {
        match self.int.len().cmp(&other.int.len()) {
            Ordering::Equal => {}
            ordering => return ordering,
        }
        match self.int.cmp(&other.int) {
            Ordering::Equal => {}
            ordering => return ordering,
        }
        let max_frac_len = self.frac.len().max(other.frac.len());
        for idx in 0..max_frac_len {
            let left = self.frac.as_bytes().get(idx).copied().unwrap_or(b'0');
            let right = other.frac.as_bytes().get(idx).copied().unwrap_or(b'0');
            match left.cmp(&right) {
                Ordering::Equal => {}
                ordering => return ordering,
            }
        }
        Ordering::Equal
    }
}

impl Ord for DecimalLiteral {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.negative, other.negative) {
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => self.cmp_abs(other),
            (true, true) => other.cmp_abs(self),
        }
    }
}

impl PartialOrd for DecimalLiteral {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn is_cross_side_pair(
    arena: &ScalarArena,
    left: ScalarId,
    right: ScalarId,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
) -> bool {
    matches!(
        (
            column_side(arena, left, left_ids, right_ids),
            column_side(arena, right, left_ids, right_ids)
        ),
        (Some(Side::Left), Some(Side::Right)) | (Some(Side::Right), Some(Side::Left))
    )
}

fn derive_or_side_fallback(
    arena: &mut ScalarArena,
    side: Side,
    per_branch: &[BranchCandidates],
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Option<PredicateGroup>, crate::compiler::SqlCompileError> {
    let mut branch_exprs = Vec::new();
    for branch in per_branch {
        let Some(constraints) = branch.by_side.get(&side) else {
            return Ok(None);
        };
        if constraints.is_empty() {
            return Ok(None);
        }
        let mut exprs = Vec::with_capacity(constraints.len());
        for constraint in constraints {
            let Some(expr) = constraint_to_expr(arena, constraint, control)? else {
                return Ok(None);
            };
            exprs.push(expr);
        }
        let Some(expr) = scalar_expr::combine_conjuncts(arena, exprs, control)? else {
            return Ok(None);
        };
        branch_exprs.push(expr);
    }
    let Some(expr) = scalar_expr::combine_disjuncts(arena, branch_exprs, control)? else {
        return Ok(None);
    };
    Ok(Some(PredicateGroup::new(
        arena,
        expr,
        PredicateOrigin::Derived,
        PredicateDerivedKind::OrSideFilter,
    )))
}

fn column_side(
    arena: &ScalarArena,
    expr: ScalarId,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
) -> Option<Side> {
    let column_id = column_id_of(arena, expr)?;
    match (
        left_ids.contains(&column_id),
        right_ids.contains(&column_id),
    ) {
        (true, false) => Some(Side::Left),
        (false, true) => Some(Side::Right),
        _ => None,
    }
}

fn single_column_side(
    arena: &ScalarArena,
    expr: ScalarId,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
) -> Option<Side> {
    let mut side = None;
    for id in scalar_expr::collect_column_ids_strict(arena, expr)? {
        let current = match (left_ids.contains(&id), right_ids.contains(&id)) {
            (true, false) => Side::Left,
            (false, true) => Side::Right,
            _ => return None,
        };
        if side.is_some_and(|existing| existing != current) {
            return None;
        }
        side = Some(current);
    }
    side
}

fn same_column(arena: &ScalarArena, left: ScalarId, right: ScalarId) -> bool {
    match (column_id_of(arena, left), column_id_of(arena, right)) {
        (Some(left), Some(right)) => left == right,
        _ => false,
    }
}

fn column_id_of(arena: &ScalarArena, expr: ScalarId) -> Option<ColumnId> {
    match arena.node(expr) {
        ScalarNode::ColumnRef(column_id) if *column_id != ColumnId::UNSET => Some(*column_id),
        _ => widening_source(arena, expr).and_then(|source| match arena.node(source) {
            ScalarNode::ColumnRef(column_id) => Some(*column_id),
            _ => None,
        }),
    }
}

fn is_column_ref(arena: &ScalarArena, expr: ScalarId) -> bool {
    matches!(arena.node(expr), ScalarNode::ColumnRef(id) if *id != ColumnId::UNSET)
}

fn is_constraint_operand(arena: &ScalarArena, expr: ScalarId) -> bool {
    column_id_of(arena, expr).is_some()
}

/// Source dependency only: callers must retain the original comparison operand.
/// Six signed widenings are total, NULL-preserving and strictly order-preserving.
fn widening_source(arena: &ScalarArena, expr: ScalarId) -> Option<ScalarId> {
    use novarocks_type_contract::ValueLogicalType;
    let ScalarNode::Cast { child, target, .. } = arena.node(expr) else {
        return None;
    };
    if !is_column_ref(arena, *child) {
        return None;
    }
    let source = arena.value_type(*child);
    let result = arena.value_type(expr);
    if source.logical_type() != ValueLogicalType::Physical
        || result.logical_type() != ValueLogicalType::Physical
        || source.nullable() != result.nullable()
        || result.data_type() != target
    {
        return None;
    }
    matches!(
        (source.data_type(), target),
        (
            DataType::Int8,
            DataType::Int16 | DataType::Int32 | DataType::Int64
        ) | (DataType::Int16, DataType::Int32 | DataType::Int64)
            | (DataType::Int32, DataType::Int64)
    )
    .then_some(*child)
}

fn same_complete_type(
    arena: &ScalarArena,
    left: ScalarId,
    right: ScalarId,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<bool, crate::compiler::SqlCompileError> {
    let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
        control,
        novarocks_type_contract::CompilePhase::Validate,
    )?;
    let equal = arena
        .value_type(left)
        .exactly_equals_checked_observed(arena.value_type(right), || work.step())?;
    work.finish()?;
    Ok(equal)
}

fn transfer_comparison_operand(
    arena: &mut ScalarArena,
    operand: ScalarId,
    source: ScalarId,
    target: ScalarId,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Option<ScalarId>, crate::compiler::SqlCompileError> {
    let Some(base) = widening_source(arena, operand) else {
        return Ok((is_column_ref(arena, operand)
            && arena.data_type(operand) == arena.data_type(target))
        .then_some(target));
    };
    if !is_column_ref(arena, source)
        || !is_column_ref(arena, target)
        || column_id_of(arena, base) != column_id_of(arena, source)
        || !same_complete_type(arena, base, source, control)?
        || !same_complete_type(arena, base, target, control)?
    {
        return Ok(None);
    }
    let ScalarNode::Cast {
        target: cast_target,
        decimal_overflow_policy,
        ..
    } = arena.node(operand)
    else {
        unreachable!()
    };
    let node = ScalarNode::Cast {
        child: target,
        target: cast_target.clone(),
        decimal_overflow_policy: *decimal_overflow_policy,
    };
    let value_type = arena.value_type(operand).clone();
    Ok(Some(arena.intern_observed(node, value_type, control)?))
}

fn constraint_has_checked_bound(arena: &ScalarArena, constraint: &ColumnConstraint) -> bool {
    let checked = |id| matches!(arena.node(id), ScalarNode::Constant(_));
    match &constraint.kind {
        ConstraintKind::Eq(id)
        | ConstraintKind::Lower { value: id, .. }
        | ConstraintKind::Upper { value: id, .. } => checked(*id),
        ConstraintKind::InList(ids) => ids.iter().any(|id| checked(*id)),
        ConstraintKind::Between { low, high } => checked(*low) || checked(*high),
    }
}

fn bounds_match_operand(
    arena: &ScalarArena,
    constraint: &ColumnConstraint,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<bool, crate::compiler::SqlCompileError> {
    let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
        control,
        novarocks_type_contract::CompilePhase::Validate,
    )?;
    let mut same = |id| {
        arena
            .value_type(constraint.column)
            .same_physical_binding_ignoring_root_nullability_observed(arena.value_type(id), || {
                work.step()
            })
    };
    let equal = match &constraint.kind {
        ConstraintKind::Eq(id)
        | ConstraintKind::Lower { value: id, .. }
        | ConstraintKind::Upper { value: id, .. } => same(*id)?,
        ConstraintKind::InList(ids) => {
            let mut equal = true;
            for id in ids {
                if !same(*id)? {
                    equal = false;
                    break;
                }
            }
            equal
        }
        ConstraintKind::Between { low, high } => same(*low)? && same(*high)?,
    };
    work.finish()?;
    Ok(equal)
}

fn checked_signed_bound(arena: &ScalarArena, id: ScalarId) -> Option<i64> {
    let value_type = arena.value_type(id);
    if value_type.logical_type() != novarocks_type_contract::ValueLogicalType::Physical
        || !matches!(
            value_type.data_type(),
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        )
    {
        return None;
    }
    match arena.node(id) {
        ScalarNode::Constant(value) => value.try_i64().ok().flatten(),
        ScalarNode::Literal(HashableLiteral(LiteralValue::Int(value))) => Some(*value),
        _ => None,
    }
}

fn is_literal(arena: &ScalarArena, expr: ScalarId) -> bool {
    matches!(
        arena.node(expr),
        ScalarNode::Literal(_) | ScalarNode::Constant(_)
    )
}

fn binary_bool(
    arena: &mut ScalarArena,
    left: ScalarId,
    op: BinOp,
    right: ScalarId,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<ScalarId, crate::compiler::SqlCompileError> {
    let nullable = arena.nullable(left) || arena.nullable(right);
    arena.intern_observed(
        ScalarNode::BinaryOp {
            op,
            left,
            right,
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        },
        novarocks_type_contract::FunctionValueType::new(DataType::Boolean, nullable),
        control,
    )
}

fn constraint_to_expr(
    arena: &mut ScalarArena,
    constraint: &ColumnConstraint,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Option<ScalarId>, crate::compiler::SqlCompileError> {
    Ok(substitute_constraint_column(
        arena,
        constraint,
        constraint.column,
        PredicateDerivedKind::OrSideFilter,
        control,
    )?
    .map(|group| group.expr))
}

#[cfg(test)]
mod comparison_domain_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr};
    use crate::column_id::ColumnId;
    use crate::optimizer::rewrite::rules::predicate_pushdown::predicate_group::{
        PredicateDerivedKind, PredicateGroup, PredicateOrigin,
    };
    use crate::optimizer::scalar::ScalarArena;

    use crate::planner::optimizer_bridge::scalar::{intern_typed, materialize};
    use arrow::datatypes::DataType;
    use std::collections::HashSet;

    fn col(alias: &str, name: &str, id: u32) -> TypedExpr {
        col_ty(alias, name, id, DataType::Int32)
    }

    fn col_ty(alias: &str, name: &str, id: u32, data_type: DataType) -> TypedExpr {
        col_meta(alias, name, id, data_type, true)
    }

    fn col_meta(
        alias: &str,
        name: &str,
        id: u32,
        data_type: DataType,
        nullable: bool,
    ) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: Some(alias.to_string()),
                column: name.to_string(),
            },
            value_type: novarocks_type_contract::FunctionValueType::new(data_type, nullable),
        }
    }

    fn int_lit(v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(v)),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
        }
    }

    fn large_int_lit(v: i128) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::LargeInt(v)),
            value_type: novarocks_type_contract::FunctionValueType::new(
                DataType::Decimal128(38, 0),
                false,
            ),
        }
    }

    fn decimal_lit(v: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Decimal(v.to_string())),
            value_type: novarocks_type_contract::FunctionValueType::new(
                DataType::Decimal128(38, 2),
                false,
            ),
        }
    }

    fn string_lit(v: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::String(v.to_string())),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Utf8, false),
        }
    }

    fn bool_expr(left: TypedExpr, op: BinOp, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Boolean, true),
        }
    }

    fn group(arena: &mut ScalarArena, expr: TypedExpr) -> PredicateGroup {
        let expr = intern_typed(
            arena,
            &expr,
            crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
        )
        .unwrap();
        PredicateGroup::new(
            arena,
            expr,
            PredicateOrigin::Filter,
            PredicateDerivedKind::None,
        )
    }

    fn nondeterministic_group(arena: &mut ScalarArena) -> PredicateGroup {
        group(
            arena,
            TypedExpr {
                kind: ExprKind::FunctionCall {
                    binding: crate::analysis::test_function_binding(
                        "rand",
                        &[],
                        DataType::Float64,
                        false,
                        crate::functions::FunctionVolatility::Volatile,
                    ),
                    volatility: crate::functions::FunctionVolatility::Volatile,
                    name: "rand".to_string(),
                    args: vec![],
                    distinct: false,
                },
                value_type: novarocks_type_contract::FunctionValueType::new(
                    DataType::Float64,
                    false,
                ),
            },
        )
    }

    fn ids(values: &[u32]) -> HashSet<ColumnId> {
        values.iter().copied().map(ColumnId::new_for_test).collect()
    }

    fn rendered_exprs(arena: &ScalarArena, groups: &[PredicateGroup]) -> String {
        groups
            .iter()
            .map(|group| format!("{:?}", materialize(arena, group.expr).kind))
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn derives_equality_across_join_key() {
        let mut arena = ScalarArena::new();
        let join_eq = group(
            &mut arena,
            bool_expr(col("l", "a", 1), BinOp::Eq, col("r", "b", 2)),
        );
        let left_filter = group(
            &mut arena,
            bool_expr(col("l", "a", 1), BinOp::Eq, int_lit(7)),
        );

        let derived = derive_inner_join_predicates(
            &mut arena,
            &ids(&[1]),
            &ids(&[2]),
            &[join_eq],
            &[left_filter],
            crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
        )
        .unwrap();

        let rendered = rendered_exprs(&arena, &derived);
        assert!(rendered.contains("\"b\""));
        assert!(rendered.contains("Int(7)"));
    }

    #[test]
    fn derives_equality_from_filter_group_join_key() {
        let mut arena = ScalarArena::new();
        let filter_eq = group(
            &mut arena,
            bool_expr(col("l", "a", 1), BinOp::Eq, col("r", "b", 2)),
        );
        let left_filter = group(
            &mut arena,
            bool_expr(col("l", "a", 1), BinOp::Eq, int_lit(7)),
        );

        let derived = derive_inner_join_predicates(
            &mut arena,
            &ids(&[1]),
            &ids(&[2]),
            &[],
            &[filter_eq, left_filter],
            crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
        )
        .unwrap();

        let rendered = rendered_exprs(&arena, &derived);
        assert!(rendered.contains("\"b\""));
        assert!(rendered.contains("Int(7)"));
    }

    #[test]
    fn skips_equivalence_derivation_for_incompatible_key_types() {
        let mut arena = ScalarArena::new();
        let join_eq = group(
            &mut arena,
            bool_expr(
                col_ty("l", "a", 1, DataType::Int32),
                BinOp::Eq,
                col_ty("r", "b", 2, DataType::Utf8),
            ),
        );
        let left_filter = group(
            &mut arena,
            bool_expr(col_ty("l", "a", 1, DataType::Int32), BinOp::Eq, int_lit(7)),
        );

        let derived = derive_inner_join_predicates(
            &mut arena,
            &ids(&[1]),
            &ids(&[2]),
            &[join_eq],
            &[left_filter],
            crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
        )
        .unwrap();

        assert!(
            derived.is_empty(),
            "incompatible key types must not derive predicates: {:?}",
            derived
        );
    }

    #[test]
    fn derives_equality_when_same_column_has_different_scalar_metadata() {
        let mut arena = ScalarArena::new();
        let join_eq = group(
            &mut arena,
            bool_expr(
                col_meta("l", "a", 1, DataType::Int32, true),
                BinOp::Eq,
                col_meta("r", "b", 2, DataType::Int32, true),
            ),
        );
        let left_filter = group(
            &mut arena,
            bool_expr(
                col_meta("l", "a", 1, DataType::Int32, false),
                BinOp::Eq,
                int_lit(7),
            ),
        );

        let derived = derive_inner_join_predicates(
            &mut arena,
            &ids(&[1]),
            &ids(&[2]),
            &[join_eq],
            &[left_filter],
            crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
        )
        .unwrap();

        let rendered = rendered_exprs(&arena, &derived);
        assert!(rendered.contains("\"b\""));
        assert!(rendered.contains("Int(7)"));
    }

    #[test]
    fn derives_or_side_filter_from_branch_equalities() {
        let mut arena = ScalarArena::new();
        let or_pred = bool_expr(
            bool_expr(
                bool_expr(col("l", "a", 1), BinOp::Eq, col("r", "b", 2)),
                BinOp::And,
                bool_expr(col("l", "a", 1), BinOp::Eq, int_lit(1)),
            ),
            BinOp::Or,
            bool_expr(
                bool_expr(col("l", "a", 1), BinOp::Eq, col("r", "b", 2)),
                BinOp::And,
                bool_expr(col("l", "a", 1), BinOp::Eq, int_lit(2)),
            ),
        );

        let or_group = group(&mut arena, or_pred);
        let derived = derive_inner_join_predicates(
            &mut arena,
            &ids(&[1]),
            &ids(&[2]),
            &[],
            &[or_group],
            crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
        )
        .unwrap();

        let rendered = rendered_exprs(&arena, &derived);
        assert!(rendered.contains("\"b\""));
        assert!(rendered.contains("InList") || rendered.contains("Or"));
        assert!(rendered.contains("Int(1)"));
        assert!(rendered.contains("Int(2)"));
    }

    #[test]
    fn derives_or_side_filter_for_different_columns_on_same_side() {
        let mut arena = ScalarArena::new();
        let or_pred = bool_expr(
            bool_expr(
                bool_expr(col("l", "a", 1), BinOp::Eq, col("r", "b", 2)),
                BinOp::And,
                bool_expr(col("l", "a", 1), BinOp::Eq, int_lit(1)),
            ),
            BinOp::Or,
            bool_expr(
                bool_expr(col("l", "c", 3), BinOp::Eq, col("r", "d", 4)),
                BinOp::And,
                bool_expr(col("l", "c", 3), BinOp::Eq, int_lit(2)),
            ),
        );

        let or_group = group(&mut arena, or_pred);
        let derived = derive_inner_join_predicates(
            &mut arena,
            &ids(&[1, 3]),
            &ids(&[2, 4]),
            &[],
            &[or_group],
            crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
        )
        .unwrap();

        let rendered = rendered_exprs(&arena, &derived);
        assert!(rendered.contains("\"b\""));
        assert!(rendered.contains("\"d\""));
        assert!(rendered.contains("Int(1)"));
        assert!(rendered.contains("Int(2)"));
        assert!(rendered.contains("Or"));
    }

    #[test]
    fn nondeterministic_group_suppresses_all_derivation() {
        let mut arena = ScalarArena::new();
        let join_eq = group(
            &mut arena,
            bool_expr(col("l", "a", 1), BinOp::Eq, col("r", "b", 2)),
        );
        let left_filter = group(
            &mut arena,
            bool_expr(col("l", "a", 1), BinOp::Eq, int_lit(7)),
        );
        let nondeterministic = nondeterministic_group(&mut arena);
        let derived = derive_inner_join_predicates(
            &mut arena,
            &ids(&[1]),
            &ids(&[2]),
            &[join_eq],
            &[left_filter, nondeterministic],
            crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
        )
        .unwrap();

        assert!(derived.is_empty());
    }

    #[test]
    fn or_branch_missing_same_side_constraint_derives_nothing() {
        let mut arena = ScalarArena::new();
        let or_pred = bool_expr(
            bool_expr(
                bool_expr(col("l", "a", 1), BinOp::Eq, col("r", "b", 2)),
                BinOp::And,
                bool_expr(col("l", "a", 1), BinOp::Eq, int_lit(1)),
            ),
            BinOp::Or,
            bool_expr(col("l", "a", 1), BinOp::Eq, col("r", "b", 2)),
        );

        let or_group = group(&mut arena, or_pred);
        let derived = derive_inner_join_predicates(
            &mut arena,
            &ids(&[1]),
            &ids(&[2]),
            &[],
            &[or_group],
            crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
        )
        .unwrap();

        assert!(
            derived.is_empty(),
            "missing branch side constraint must not derive: {:?}",
            derived
        );
    }

    #[test]
    fn derives_range_envelope_from_or_branches() {
        let mut arena = ScalarArena::new();
        let or_pred = bool_expr(
            TypedExpr {
                kind: ExprKind::Between {
                    expr: Box::new(col("s", "price", 3)),
                    low: Box::new(int_lit(100)),
                    high: Box::new(int_lit(150)),
                    negated: false,
                },
                value_type: novarocks_type_contract::FunctionValueType::new(
                    DataType::Boolean,
                    true,
                ),
            },
            BinOp::Or,
            TypedExpr {
                kind: ExprKind::Between {
                    expr: Box::new(col("s", "price", 3)),
                    low: Box::new(int_lit(50)),
                    high: Box::new(int_lit(200)),
                    negated: false,
                },
                value_type: novarocks_type_contract::FunctionValueType::new(
                    DataType::Boolean,
                    true,
                ),
            },
        );

        let or_group = group(&mut arena, or_pred);
        let derived = derive_inner_join_predicates(
            &mut arena,
            &ids(&[1]),
            &ids(&[3]),
            &[],
            &[or_group],
            crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
        )
        .unwrap();

        let rendered = rendered_exprs(&arena, &derived);
        assert!(rendered.contains("\"price\""));
        assert!(rendered.contains("Int(50)"));
        assert!(rendered.contains("Int(200)"));
    }

    #[test]
    fn range_envelope_compares_large_int_bounds_exactly() {
        let mut arena = ScalarArena::new();
        let or_pred = bool_expr(
            TypedExpr {
                kind: ExprKind::Between {
                    expr: Box::new(col("s", "price", 3)),
                    low: Box::new(large_int_lit(9_007_199_254_740_993)),
                    high: Box::new(large_int_lit(9_007_199_254_741_500)),
                    negated: false,
                },
                value_type: novarocks_type_contract::FunctionValueType::new(
                    DataType::Boolean,
                    true,
                ),
            },
            BinOp::Or,
            TypedExpr {
                kind: ExprKind::Between {
                    expr: Box::new(col("s", "price", 3)),
                    low: Box::new(large_int_lit(9_007_199_254_740_992)),
                    high: Box::new(large_int_lit(9_007_199_254_741_600)),
                    negated: false,
                },
                value_type: novarocks_type_contract::FunctionValueType::new(
                    DataType::Boolean,
                    true,
                ),
            },
        );

        let or_group = group(&mut arena, or_pred);
        let derived = derive_inner_join_predicates(
            &mut arena,
            &ids(&[1]),
            &ids(&[3]),
            &[],
            &[or_group],
            crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
        )
        .unwrap();

        let rendered = rendered_exprs(&arena, &derived);
        assert!(rendered.contains("LargeInt(9007199254740992)"));
        assert!(!rendered.contains("LargeInt(9007199254740993)"));
    }

    #[test]
    fn range_envelope_compares_decimal_bounds_exactly() {
        let mut arena = ScalarArena::new();
        let or_pred = bool_expr(
            TypedExpr {
                kind: ExprKind::Between {
                    expr: Box::new(col("s", "price", 3)),
                    low: Box::new(decimal_lit("10.25")),
                    high: Box::new(decimal_lit("20.00")),
                    negated: false,
                },
                value_type: novarocks_type_contract::FunctionValueType::new(
                    DataType::Boolean,
                    true,
                ),
            },
            BinOp::Or,
            TypedExpr {
                kind: ExprKind::Between {
                    expr: Box::new(col("s", "price", 3)),
                    low: Box::new(decimal_lit("9.50")),
                    high: Box::new(decimal_lit("30.00")),
                    negated: false,
                },
                value_type: novarocks_type_contract::FunctionValueType::new(
                    DataType::Boolean,
                    true,
                ),
            },
        );

        let or_group = group(&mut arena, or_pred);
        let derived = derive_inner_join_predicates(
            &mut arena,
            &ids(&[1]),
            &ids(&[3]),
            &[],
            &[or_group],
            crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
        )
        .unwrap();

        let rendered = rendered_exprs(&arena, &derived);
        assert!(rendered.contains("Decimal(\"9.50\")"));
    }

    #[test]
    fn unsupported_range_literal_skips_range_envelope() {
        let mut arena = ScalarArena::new();
        let or_pred = bool_expr(
            TypedExpr {
                kind: ExprKind::Between {
                    expr: Box::new(col("s", "price", 3)),
                    low: Box::new(string_lit("a")),
                    high: Box::new(string_lit("z")),
                    negated: false,
                },
                value_type: novarocks_type_contract::FunctionValueType::new(
                    DataType::Boolean,
                    true,
                ),
            },
            BinOp::Or,
            TypedExpr {
                kind: ExprKind::Between {
                    expr: Box::new(col("s", "price", 3)),
                    low: Box::new(string_lit("b")),
                    high: Box::new(string_lit("y")),
                    negated: false,
                },
                value_type: novarocks_type_contract::FunctionValueType::new(
                    DataType::Boolean,
                    true,
                ),
            },
        );

        let or_group = group(&mut arena, or_pred);
        let derived = derive_inner_join_predicates(
            &mut arena,
            &ids(&[1]),
            &ids(&[3]),
            &[],
            &[or_group],
            crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
        )
        .unwrap();

        assert!(
            derived
                .iter()
                .all(|group| group.derived != PredicateDerivedKind::RangeEnvelope),
            "unsupported literals must not produce range envelope: {:?}",
            derived
        );
    }
}

#[cfg(test)]
mod checked_integer_tests;
