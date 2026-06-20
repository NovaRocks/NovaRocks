use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use arrow::datatypes::DataType;

use crate::sql::analysis::{BinOp, LiteralValue};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::rules::predicate_pushdown::predicate_group::{
    PredicateDerivedKind, PredicateGroup, PredicateOrigin, dedupe_groups,
};
use crate::sql::optimizer::scalar::{HashableLiteral, ScalarArena, ScalarId, ScalarNode};
use crate::sql::optimizer::scalar_expr;

pub(crate) fn derive_inner_join_predicates(
    arena: &mut ScalarArena,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
    join_groups: &[PredicateGroup],
    filter_groups: &[PredicateGroup],
) -> Vec<PredicateGroup> {
    if join_groups
        .iter()
        .chain(filter_groups.iter())
        .any(|group| !group.deterministic)
    {
        return Vec::new();
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
            if same_column(arena, constraint.column, left) {
                if let Some(group) = substitute_constraint_column(
                    arena,
                    constraint,
                    right,
                    derived_kind_for_constraint(&constraint.kind),
                ) {
                    derived.push(group);
                }
            } else if same_column(arena, constraint.column, right) {
                if let Some(group) = substitute_constraint_column(
                    arena,
                    constraint,
                    left,
                    derived_kind_for_constraint(&constraint.kind),
                ) {
                    derived.push(group);
                }
            }
        }
    }

    for group in filter_groups {
        derived.extend(derive_or_branch_side_filters(
            arena, left_ids, right_ids, group,
        ));
    }

    dedupe_groups(derived)
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
        ScalarNode::BinaryOp { left, op, right } => {
            if is_column_ref(arena, *left) && is_literal(arena, *right) {
                return build_binary_constraint(*left, *op, *right);
            }
            if is_literal(arena, *left) && is_column_ref(arena, *right) {
                return build_binary_constraint(*right, reverse_comparison(*op)?, *left);
            }
            None
        }
        ScalarNode::InList {
            child,
            list,
            negated: false,
        } if is_column_ref(arena, *child) && list.iter().all(|item| is_literal(arena, *item)) => {
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
        } if is_column_ref(arena, *child)
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
) -> Option<PredicateGroup> {
    if !is_column_ref(arena, target_column) {
        return None;
    }
    if arena.data_type(constraint.column) != arena.data_type(target_column) {
        return None;
    }
    let expr = match &constraint.kind {
        ConstraintKind::Eq(value) => binary_bool(arena, target_column, BinOp::Eq, *value),
        ConstraintKind::InList(list) => {
            let nullable =
                arena.nullable(target_column) || list.iter().any(|expr| arena.nullable(*expr));
            arena.intern(
                ScalarNode::InList {
                    child: target_column,
                    list: list.clone(),
                    negated: false,
                },
                DataType::Boolean,
                nullable,
            )
        }
        ConstraintKind::Lower { op, value } | ConstraintKind::Upper { op, value } => {
            binary_bool(arena, target_column, *op, *value)
        }
        ConstraintKind::Between { low, high } => {
            let nullable =
                arena.nullable(target_column) || arena.nullable(*low) || arena.nullable(*high);
            arena.intern(
                ScalarNode::Between {
                    child: target_column,
                    low: *low,
                    high: *high,
                    negated: false,
                },
                DataType::Boolean,
                nullable,
            )
        }
    };
    Some(PredicateGroup::new(
        arena,
        expr,
        PredicateOrigin::Derived,
        derived,
    ))
}

fn derive_or_branch_side_filters(
    arena: &mut ScalarArena,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
    group: &PredicateGroup,
) -> Vec<PredicateGroup> {
    if !group.deterministic {
        return Vec::new();
    }

    let mut branches = Vec::new();
    scalar_expr::split_disjuncts(arena, group.expr, &mut branches);
    if branches.len() < 2 {
        return Vec::new();
    }

    let mut per_branch = Vec::with_capacity(branches.len());
    for branch in branches {
        let mut pairs = Vec::new();
        let mut constraints = Vec::new();
        let mut conjuncts = Vec::new();
        scalar_expr::split_conjuncts(arena, branch, &mut conjuncts);
        for conjunct in conjuncts {
            if let Some(pair) = extract_column_pair_equality(arena, conjunct) {
                if is_cross_side_pair(arena, pair.0, pair.1, left_ids, right_ids) {
                    pairs.push(pair);
                }
            }
            if let Some(constraint) = extract_column_literal_constraint(arena, conjunct) {
                constraints.push(constraint);
            }
        }

        let mut candidates = constraints.clone();
        for (left, right) in pairs {
            for constraint in &constraints {
                if same_column(arena, constraint.column, left) {
                    if arena.data_type(constraint.column) != arena.data_type(right) {
                        continue;
                    }
                    candidates.push(ColumnConstraint {
                        column: right,
                        kind: constraint.kind.clone(),
                    });
                } else if same_column(arena, constraint.column, right) {
                    if arena.data_type(constraint.column) != arena.data_type(left) {
                        continue;
                    }
                    candidates.push(ColumnConstraint {
                        column: left,
                        kind: constraint.kind.clone(),
                    });
                }
            }
        }

        if candidates.is_empty() {
            return Vec::new();
        }

        let mut by_column: HashMap<ColumnId, Vec<ColumnConstraint>> = HashMap::new();
        let mut by_side: HashMap<Side, Vec<ColumnConstraint>> = HashMap::new();
        for candidate in candidates {
            if let Some(side) = column_side(arena, candidate.column, left_ids, right_ids) {
                let Some(column_id) = column_id_of(arena, candidate.column) else {
                    continue;
                };
                by_column
                    .entry(column_id)
                    .or_default()
                    .push(candidate.clone());
                by_side.entry(side).or_default().push(candidate);
            }
        }
        if by_column.is_empty() {
            return Vec::new();
        }
        per_branch.push(BranchCandidates { by_column, by_side });
    }

    let mut derived = Vec::new();
    let mut specialized_sides = HashSet::new();
    let first_columns: Vec<ColumnId> = per_branch[0].by_column.keys().copied().collect();
    for column_id in first_columns {
        if !per_branch
            .iter()
            .all(|branch| branch.by_column.contains_key(&column_id))
        {
            continue;
        }

        if let Some(group) = derive_or_in_list(arena, column_id, &per_branch) {
            if let Some(side) = single_column_side(arena, group.expr, left_ids, right_ids) {
                specialized_sides.insert(side);
            }
            derived.push(group);
        } else if let Some(group) = derive_or_range_envelope(arena, column_id, &per_branch) {
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
        if let Some(group) = derive_or_side_fallback(arena, side, &per_branch) {
            derived.push(group);
        }
    }

    dedupe_groups(derived)
}

#[derive(Clone, Debug)]
struct BranchCandidates {
    by_column: HashMap<ColumnId, Vec<ColumnConstraint>>,
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
    column_id: ColumnId,
    per_branch: &[BranchCandidates],
) -> Option<PredicateGroup> {
    let first = per_branch[0].by_column.get(&column_id)?.first()?.column;
    let mut values = Vec::new();
    for branch in per_branch {
        let constraints = branch.by_column.get(&column_id)?;
        let eq = constraints
            .iter()
            .find_map(|constraint| match &constraint.kind {
                ConstraintKind::Eq(value) => Some(*value),
                _ => None,
            })?;
        values.push(eq);
    }

    substitute_constraint_column(
        arena,
        &ColumnConstraint {
            column: first,
            kind: ConstraintKind::InList(values),
        },
        first,
        PredicateDerivedKind::OrSideFilter,
    )
}

fn derive_or_range_envelope(
    arena: &mut ScalarArena,
    column_id: ColumnId,
    per_branch: &[BranchCandidates],
) -> Option<PredicateGroup> {
    let first = per_branch[0].by_column.get(&column_id)?.first()?.column;
    let mut low: Option<ScalarId> = None;
    let mut high: Option<ScalarId> = None;

    for branch in per_branch {
        let constraints = branch.by_column.get(&column_id)?;
        let (branch_low, branch_high) = branch_range(arena, constraints)?;
        low = Some(match low {
            Some(current) if compare_literals(arena, current, branch_low)? != Ordering::Greater => {
                current
            }
            _ => branch_low,
        });
        high = Some(match high {
            Some(current) if compare_literals(arena, current, branch_high)? != Ordering::Less => {
                current
            }
            _ => branch_high,
        });
    }

    substitute_constraint_column(
        arena,
        &ColumnConstraint {
            column: first,
            kind: ConstraintKind::Between {
                low: low?,
                high: high?,
            },
        },
        first,
        PredicateDerivedKind::RangeEnvelope,
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
) -> Option<PredicateGroup> {
    let mut branch_exprs = Vec::new();
    for branch in per_branch {
        let constraints = branch.by_side.get(&side)?;
        if constraints.is_empty() {
            return None;
        }
        let exprs: Option<Vec<ScalarId>> = constraints
            .iter()
            .map(|constraint| constraint_to_expr(arena, constraint))
            .collect();
        branch_exprs.push(scalar_expr::combine_conjuncts(arena, exprs?)?);
    }

    let expr = scalar_expr::combine_disjuncts(arena, branch_exprs)?;
    Some(PredicateGroup::new(
        arena,
        expr,
        PredicateOrigin::Derived,
        PredicateDerivedKind::OrSideFilter,
    ))
}

fn column_side(
    arena: &ScalarArena,
    expr: ScalarId,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
) -> Option<Side> {
    let ScalarNode::ColumnRef(column_id) = arena.node(expr) else {
        return None;
    };
    match (left_ids.contains(column_id), right_ids.contains(column_id)) {
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
        _ => None,
    }
}

fn is_column_ref(arena: &ScalarArena, expr: ScalarId) -> bool {
    matches!(arena.node(expr), ScalarNode::ColumnRef(id) if *id != ColumnId::UNSET)
}

fn is_literal(arena: &ScalarArena, expr: ScalarId) -> bool {
    matches!(arena.node(expr), ScalarNode::Literal(_))
}

fn binary_bool(arena: &mut ScalarArena, left: ScalarId, op: BinOp, right: ScalarId) -> ScalarId {
    let nullable = arena.nullable(left) || arena.nullable(right);
    arena.intern(
        ScalarNode::BinaryOp { op, left, right },
        DataType::Boolean,
        nullable,
    )
}

fn constraint_to_expr(arena: &mut ScalarArena, constraint: &ColumnConstraint) -> Option<ScalarId> {
    substitute_constraint_column(
        arena,
        constraint,
        constraint.column,
        PredicateDerivedKind::OrSideFilter,
    )
    .map(|group| group.expr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::rules::predicate_pushdown::predicate_group::{
        PredicateDerivedKind, PredicateGroup, PredicateOrigin,
    };
    use crate::sql::optimizer::scalar::{ScalarArena, intern_typed, materialize};
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
            data_type,
            nullable,
        }
    }

    fn int_lit(v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(v)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn large_int_lit(v: i128) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::LargeInt(v)),
            data_type: DataType::Decimal128(38, 0),
            nullable: false,
        }
    }

    fn decimal_lit(v: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Decimal(v.to_string())),
            data_type: DataType::Decimal128(38, 2),
            nullable: false,
        }
    }

    fn string_lit(v: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::String(v.to_string())),
            data_type: DataType::Utf8,
            nullable: false,
        }
    }

    fn bool_expr(left: TypedExpr, op: BinOp, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: true,
        }
    }

    fn group(arena: &mut ScalarArena, expr: TypedExpr) -> PredicateGroup {
        let expr = intern_typed(arena, &expr);
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
                    name: "rand".to_string(),
                    args: vec![],
                    distinct: false,
                },
                data_type: DataType::Float64,
                nullable: false,
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
        );

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
        );

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
        );

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
        );

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
        let derived =
            derive_inner_join_predicates(&mut arena, &ids(&[1]), &ids(&[2]), &[], &[or_group]);

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
        );

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
        );

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
        let derived =
            derive_inner_join_predicates(&mut arena, &ids(&[1]), &ids(&[2]), &[], &[or_group]);

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
                data_type: DataType::Boolean,
                nullable: true,
            },
            BinOp::Or,
            TypedExpr {
                kind: ExprKind::Between {
                    expr: Box::new(col("s", "price", 3)),
                    low: Box::new(int_lit(50)),
                    high: Box::new(int_lit(200)),
                    negated: false,
                },
                data_type: DataType::Boolean,
                nullable: true,
            },
        );

        let or_group = group(&mut arena, or_pred);
        let derived =
            derive_inner_join_predicates(&mut arena, &ids(&[1]), &ids(&[3]), &[], &[or_group]);

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
                data_type: DataType::Boolean,
                nullable: true,
            },
            BinOp::Or,
            TypedExpr {
                kind: ExprKind::Between {
                    expr: Box::new(col("s", "price", 3)),
                    low: Box::new(large_int_lit(9_007_199_254_740_992)),
                    high: Box::new(large_int_lit(9_007_199_254_741_600)),
                    negated: false,
                },
                data_type: DataType::Boolean,
                nullable: true,
            },
        );

        let or_group = group(&mut arena, or_pred);
        let derived =
            derive_inner_join_predicates(&mut arena, &ids(&[1]), &ids(&[3]), &[], &[or_group]);

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
                data_type: DataType::Boolean,
                nullable: true,
            },
            BinOp::Or,
            TypedExpr {
                kind: ExprKind::Between {
                    expr: Box::new(col("s", "price", 3)),
                    low: Box::new(decimal_lit("9.50")),
                    high: Box::new(decimal_lit("30.00")),
                    negated: false,
                },
                data_type: DataType::Boolean,
                nullable: true,
            },
        );

        let or_group = group(&mut arena, or_pred);
        let derived =
            derive_inner_join_predicates(&mut arena, &ids(&[1]), &ids(&[3]), &[], &[or_group]);

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
                data_type: DataType::Boolean,
                nullable: true,
            },
            BinOp::Or,
            TypedExpr {
                kind: ExprKind::Between {
                    expr: Box::new(col("s", "price", 3)),
                    low: Box::new(string_lit("b")),
                    high: Box::new(string_lit("y")),
                    negated: false,
                },
                data_type: DataType::Boolean,
                nullable: true,
            },
        );

        let or_group = group(&mut arena, or_pred);
        let derived =
            derive_inner_join_predicates(&mut arena, &ids(&[1]), &ids(&[3]), &[], &[or_group]);

        assert!(
            derived
                .iter()
                .all(|group| group.derived != PredicateDerivedKind::RangeEnvelope),
            "unsupported literals must not produce range envelope: {:?}",
            derived
        );
    }
}
