use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::rules::predicate_pushdown::predicate_group::{
    PredicateDerivedKind, PredicateGroup, PredicateOrigin, dedupe_groups, split_and_refs,
    split_or_refs,
};
use arrow::datatypes::DataType;

pub(crate) fn derive_inner_join_predicates(
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
        for conjunct in split_and_refs(&group.expr) {
            if let Some(pair) = extract_column_pair_equality(conjunct) {
                equality_pairs.push(pair);
            }
        }
    }

    let mut constraints = Vec::new();
    for group in join_groups.iter().chain(filter_groups.iter()) {
        for conjunct in split_and_refs(&group.expr) {
            if let Some(constraint) = extract_column_literal_constraint(conjunct) {
                constraints.push(constraint);
            }
        }
    }

    let mut derived = Vec::new();
    for (left, right) in equality_pairs {
        if !is_cross_side_pair(&left, &right, left_ids, right_ids) {
            continue;
        }
        for constraint in &constraints {
            if same_column(&constraint.column, &left) {
                if let Some(group) = substitute_constraint_column(
                    constraint,
                    &right,
                    derived_kind_for_constraint(&constraint.kind),
                ) {
                    derived.push(group);
                }
            } else if same_column(&constraint.column, &right) {
                if let Some(group) = substitute_constraint_column(
                    constraint,
                    &left,
                    derived_kind_for_constraint(&constraint.kind),
                ) {
                    derived.push(group);
                }
            }
        }
    }

    for group in filter_groups {
        derived.extend(derive_or_branch_side_filters(left_ids, right_ids, group));
    }

    dedupe_groups(derived)
}

fn extract_column_pair_equality(expr: &TypedExpr) -> Option<(TypedExpr, TypedExpr)> {
    match &expr.kind {
        ExprKind::Nested(inner) => extract_column_pair_equality(inner),
        ExprKind::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } if is_column_ref(left) && is_column_ref(right) => {
            Some(((**left).clone(), (**right).clone()))
        }
        _ => None,
    }
}

fn extract_column_literal_constraint(expr: &TypedExpr) -> Option<ColumnConstraint> {
    match &expr.kind {
        ExprKind::Nested(inner) => extract_column_literal_constraint(inner),
        ExprKind::BinaryOp { left, op, right } => {
            if is_column_ref(left) && is_literal(right) {
                return build_binary_constraint((**left).clone(), *op, (**right).clone());
            }
            if is_literal(left) && is_column_ref(right) {
                return build_binary_constraint(
                    (**right).clone(),
                    reverse_comparison(*op)?,
                    (**left).clone(),
                );
            }
            None
        }
        ExprKind::InList {
            expr,
            list,
            negated: false,
        } if is_column_ref(expr) && list.iter().all(is_literal) => Some(ColumnConstraint {
            column: (**expr).clone(),
            kind: ConstraintKind::InList(list.clone()),
        }),
        ExprKind::Between {
            expr,
            low,
            high,
            negated: false,
        } if is_column_ref(expr) && is_literal(low) && is_literal(high) => Some(ColumnConstraint {
            column: (**expr).clone(),
            kind: ConstraintKind::Between {
                low: (**low).clone(),
                high: (**high).clone(),
            },
        }),
        _ => None,
    }
}

fn substitute_constraint_column(
    constraint: &ColumnConstraint,
    target_column: &TypedExpr,
    derived: PredicateDerivedKind,
) -> Option<PredicateGroup> {
    if !is_column_ref(target_column) {
        return None;
    }
    if constraint.column.data_type != target_column.data_type {
        return None;
    }
    let expr = match &constraint.kind {
        ConstraintKind::Eq(value) => binary_bool(target_column.clone(), BinOp::Eq, value.clone()),
        ConstraintKind::InList(list) => TypedExpr {
            kind: ExprKind::InList {
                expr: Box::new(target_column.clone()),
                list: list.clone(),
                negated: false,
            },
            data_type: DataType::Boolean,
            nullable: target_column.nullable || list.iter().any(|expr| expr.nullable),
        },
        ConstraintKind::Lower { op, value } | ConstraintKind::Upper { op, value } => {
            binary_bool(target_column.clone(), *op, value.clone())
        }
        ConstraintKind::Between { low, high } => TypedExpr {
            kind: ExprKind::Between {
                expr: Box::new(target_column.clone()),
                low: Box::new(low.clone()),
                high: Box::new(high.clone()),
                negated: false,
            },
            data_type: DataType::Boolean,
            nullable: target_column.nullable || low.nullable || high.nullable,
        },
    };
    Some(PredicateGroup::new(expr, PredicateOrigin::Derived, derived))
}

fn derive_or_branch_side_filters(
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
    group: &PredicateGroup,
) -> Vec<PredicateGroup> {
    if !group.deterministic {
        return Vec::new();
    }

    let branches = split_or_refs(&group.expr);
    if branches.len() < 2 {
        return Vec::new();
    }

    let mut per_branch = Vec::with_capacity(branches.len());
    for branch in branches {
        let mut pairs = Vec::new();
        let mut constraints = Vec::new();
        for conjunct in split_and_refs(branch) {
            if let Some(pair) = extract_column_pair_equality(conjunct) {
                if is_cross_side_pair(&pair.0, &pair.1, left_ids, right_ids) {
                    pairs.push(pair);
                }
            }
            if let Some(constraint) = extract_column_literal_constraint(conjunct) {
                constraints.push(constraint);
            }
        }

        let mut candidates = constraints.clone();
        for (left, right) in pairs {
            for constraint in &constraints {
                if same_column(&constraint.column, &left) {
                    if constraint.column.data_type != right.data_type {
                        continue;
                    }
                    candidates.push(ColumnConstraint {
                        column: right.clone(),
                        kind: constraint.kind.clone(),
                    });
                } else if same_column(&constraint.column, &right) {
                    if constraint.column.data_type != left.data_type {
                        continue;
                    }
                    candidates.push(ColumnConstraint {
                        column: left.clone(),
                        kind: constraint.kind.clone(),
                    });
                }
            }
        }

        if candidates.is_empty() {
            return Vec::new();
        }

        let mut by_column: HashMap<String, Vec<ColumnConstraint>> = HashMap::new();
        let mut by_side: HashMap<Side, Vec<ColumnConstraint>> = HashMap::new();
        for candidate in candidates {
            if let Some(side) = column_side(&candidate.column, left_ids, right_ids) {
                by_column
                    .entry(column_key(&candidate.column))
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
    let first_columns: Vec<String> = per_branch[0].by_column.keys().cloned().collect();
    for column_key in first_columns {
        if !per_branch
            .iter()
            .all(|branch| branch.by_column.contains_key(&column_key))
        {
            continue;
        }

        if let Some(group) = derive_or_in_list(&column_key, &per_branch) {
            if let Some(side) = single_column_side(&group.expr, left_ids, right_ids) {
                specialized_sides.insert(side);
            }
            derived.push(group);
        } else if let Some(group) = derive_or_range_envelope(&column_key, &per_branch) {
            if let Some(side) = single_column_side(&group.expr, left_ids, right_ids) {
                specialized_sides.insert(side);
            }
            derived.push(group);
        }
    }

    for side in [Side::Left, Side::Right] {
        if specialized_sides.contains(&side) {
            continue;
        }
        if let Some(group) = derive_or_side_fallback(side, &per_branch) {
            derived.push(group);
        }
    }

    dedupe_groups(derived)
}

#[derive(Clone, Debug)]
struct BranchCandidates {
    by_column: HashMap<String, Vec<ColumnConstraint>>,
    by_side: HashMap<Side, Vec<ColumnConstraint>>,
}

#[derive(Clone, Debug)]
struct ColumnConstraint {
    column: TypedExpr,
    kind: ConstraintKind,
}

#[derive(Clone, Debug)]
enum ConstraintKind {
    Eq(TypedExpr),
    InList(Vec<TypedExpr>),
    Lower { op: BinOp, value: TypedExpr },
    Upper { op: BinOp, value: TypedExpr },
    Between { low: TypedExpr, high: TypedExpr },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Side {
    Left,
    Right,
}

fn build_binary_constraint(
    column: TypedExpr,
    op: BinOp,
    value: TypedExpr,
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

fn derive_or_in_list(column_key: &str, per_branch: &[BranchCandidates]) -> Option<PredicateGroup> {
    let first = per_branch[0]
        .by_column
        .get(column_key)?
        .first()?
        .column
        .clone();
    let mut values = Vec::new();
    for branch in per_branch {
        let constraints = branch.by_column.get(column_key)?;
        let eq = constraints
            .iter()
            .find_map(|constraint| match &constraint.kind {
                ConstraintKind::Eq(value) => Some(value.clone()),
                _ => None,
            })?;
        values.push(eq);
    }

    substitute_constraint_column(
        &ColumnConstraint {
            column: first.clone(),
            kind: ConstraintKind::InList(values),
        },
        &first,
        PredicateDerivedKind::OrSideFilter,
    )
}

fn derive_or_range_envelope(
    column_key: &str,
    per_branch: &[BranchCandidates],
) -> Option<PredicateGroup> {
    let first = per_branch[0]
        .by_column
        .get(column_key)?
        .first()?
        .column
        .clone();
    let mut low: Option<TypedExpr> = None;
    let mut high: Option<TypedExpr> = None;

    for branch in per_branch {
        let constraints = branch.by_column.get(column_key)?;
        let (branch_low, branch_high) = branch_range(constraints)?;
        low = Some(match low {
            Some(current) if compare_literals(&current, &branch_low)? != Ordering::Greater => {
                current
            }
            _ => branch_low,
        });
        high = Some(match high {
            Some(current) if compare_literals(&current, &branch_high)? != Ordering::Less => current,
            _ => branch_high,
        });
    }

    substitute_constraint_column(
        &ColumnConstraint {
            column: first.clone(),
            kind: ConstraintKind::Between {
                low: low?,
                high: high?,
            },
        },
        &first,
        PredicateDerivedKind::RangeEnvelope,
    )
}

fn branch_range(constraints: &[ColumnConstraint]) -> Option<(TypedExpr, TypedExpr)> {
    if let Some((low, high)) = constraints
        .iter()
        .find_map(|constraint| match &constraint.kind {
            ConstraintKind::Between { low, high } => Some((low.clone(), high.clone())),
            _ => None,
        })
    {
        ensure_comparable_literal(&low)?;
        ensure_comparable_literal(&high)?;
        return Some((low, high));
    }

    let mut low = None;
    let mut high = None;
    for constraint in constraints {
        match &constraint.kind {
            ConstraintKind::Lower { value, .. } => {
                ensure_comparable_literal(value)?;
                low = Some(value.clone());
            }
            ConstraintKind::Upper { value, .. } => {
                ensure_comparable_literal(value)?;
                high = Some(value.clone());
            }
            _ => {}
        }
    }
    Some((low?, high?))
}

fn ensure_comparable_literal(expr: &TypedExpr) -> Option<()> {
    compare_literals(expr, expr).map(|_| ())
}

fn compare_literals(left: &TypedExpr, right: &TypedExpr) -> Option<Ordering> {
    match (&left.kind, &right.kind) {
        (
            ExprKind::Literal(LiteralValue::Int(left)),
            ExprKind::Literal(LiteralValue::Int(right)),
        ) => Some(left.cmp(right)),
        (
            ExprKind::Literal(LiteralValue::LargeInt(left)),
            ExprKind::Literal(LiteralValue::LargeInt(right)),
        ) => Some(left.cmp(right)),
        (
            ExprKind::Literal(LiteralValue::Int(left)),
            ExprKind::Literal(LiteralValue::LargeInt(right)),
        ) => Some(i128::from(*left).cmp(right)),
        (
            ExprKind::Literal(LiteralValue::LargeInt(left)),
            ExprKind::Literal(LiteralValue::Int(right)),
        ) => Some(left.cmp(&i128::from(*right))),
        (
            ExprKind::Literal(LiteralValue::Float(left)),
            ExprKind::Literal(LiteralValue::Float(right)),
        ) if left.is_finite() && right.is_finite() => left.partial_cmp(right),
        (
            ExprKind::Literal(LiteralValue::Decimal(left)),
            ExprKind::Literal(LiteralValue::Decimal(right)),
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
    left: &TypedExpr,
    right: &TypedExpr,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
) -> bool {
    matches!(
        (
            column_side(left, left_ids, right_ids),
            column_side(right, left_ids, right_ids)
        ),
        (Some(Side::Left), Some(Side::Right)) | (Some(Side::Right), Some(Side::Left))
    )
}

fn derive_or_side_fallback(side: Side, per_branch: &[BranchCandidates]) -> Option<PredicateGroup> {
    let mut branch_exprs = Vec::new();
    for branch in per_branch {
        let constraints = branch.by_side.get(&side)?;
        if constraints.is_empty() {
            return None;
        }
        let exprs: Option<Vec<TypedExpr>> = constraints.iter().map(constraint_to_expr).collect();
        branch_exprs.push(combine_bool_exprs(exprs?, BinOp::And)?);
    }

    let expr = combine_bool_exprs(branch_exprs, BinOp::Or)?;
    Some(PredicateGroup::new(
        expr,
        PredicateOrigin::Derived,
        PredicateDerivedKind::OrSideFilter,
    ))
}

fn column_side(
    expr: &TypedExpr,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
) -> Option<Side> {
    let ExprKind::ColumnRef { column_id, .. } = &expr.kind else {
        return None;
    };
    match (left_ids.contains(column_id), right_ids.contains(column_id)) {
        (true, false) => Some(Side::Left),
        (false, true) => Some(Side::Right),
        _ => None,
    }
}

fn single_column_side(
    expr: &TypedExpr,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
) -> Option<Side> {
    let mut side = None;
    for id in crate::sql::optimizer::rewrite::rules::utils::collect_column_id_refs(expr) {
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

fn same_column(left: &TypedExpr, right: &TypedExpr) -> bool {
    column_key(left) == column_key(right)
}

fn column_key(expr: &TypedExpr) -> String {
    format!("{:?}", expr.kind)
}

fn is_column_ref(expr: &TypedExpr) -> bool {
    matches!(expr.kind, ExprKind::ColumnRef { .. })
}

fn is_literal(expr: &TypedExpr) -> bool {
    matches!(expr.kind, ExprKind::Literal(_))
}

fn binary_bool(left: TypedExpr, op: BinOp, right: TypedExpr) -> TypedExpr {
    TypedExpr {
        data_type: DataType::Boolean,
        nullable: left.nullable || right.nullable,
        kind: ExprKind::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        },
    }
}

fn constraint_to_expr(constraint: &ColumnConstraint) -> Option<TypedExpr> {
    substitute_constraint_column(
        constraint,
        &constraint.column,
        PredicateDerivedKind::OrSideFilter,
    )
    .map(|group| group.expr)
}

fn combine_bool_exprs(mut exprs: Vec<TypedExpr>, op: BinOp) -> Option<TypedExpr> {
    if exprs.is_empty() {
        return None;
    }
    let mut result = exprs.pop()?;
    while let Some(left) = exprs.pop() {
        result = TypedExpr {
            data_type: DataType::Boolean,
            nullable: left.nullable || result.nullable,
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(result),
            },
        };
    }
    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::rules::predicate_pushdown::predicate_group::{
        PredicateDerivedKind, PredicateGroup, PredicateOrigin,
    };
    use arrow::datatypes::DataType;
    use std::collections::HashSet;

    fn col(alias: &str, name: &str, id: u32) -> TypedExpr {
        col_ty(alias, name, id, DataType::Int32)
    }

    fn col_ty(alias: &str, name: &str, id: u32, data_type: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: Some(alias.to_string()),
                column: name.to_string(),
            },
            data_type,
            nullable: true,
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

    fn group(expr: TypedExpr) -> PredicateGroup {
        PredicateGroup::new(expr, PredicateOrigin::Filter, PredicateDerivedKind::None)
    }

    fn nondeterministic_group() -> PredicateGroup {
        group(TypedExpr {
            kind: ExprKind::FunctionCall {
                name: "rand".to_string(),
                args: vec![],
                distinct: false,
            },
            data_type: DataType::Float64,
            nullable: false,
        })
    }

    fn ids(values: &[u32]) -> HashSet<ColumnId> {
        values.iter().copied().map(ColumnId::new_for_test).collect()
    }

    #[test]
    fn derives_equality_across_join_key() {
        let join_eq = group(bool_expr(col("l", "a", 1), BinOp::Eq, col("r", "b", 2)));
        let left_filter = group(bool_expr(col("l", "a", 1), BinOp::Eq, int_lit(7)));

        let derived =
            derive_inner_join_predicates(&ids(&[1]), &ids(&[2]), &[join_eq], &[left_filter]);

        let rendered = format!("{:?}", derived);
        assert!(rendered.contains("\"b\""));
        assert!(rendered.contains("Int(7)"));
    }

    #[test]
    fn derives_equality_from_filter_group_join_key() {
        let filter_eq = group(bool_expr(col("l", "a", 1), BinOp::Eq, col("r", "b", 2)));
        let left_filter = group(bool_expr(col("l", "a", 1), BinOp::Eq, int_lit(7)));

        let derived =
            derive_inner_join_predicates(&ids(&[1]), &ids(&[2]), &[], &[filter_eq, left_filter]);

        let rendered = format!("{:?}", derived);
        assert!(rendered.contains("\"b\""));
        assert!(rendered.contains("Int(7)"));
    }

    #[test]
    fn skips_equivalence_derivation_for_incompatible_key_types() {
        let join_eq = group(bool_expr(
            col_ty("l", "a", 1, DataType::Int32),
            BinOp::Eq,
            col_ty("r", "b", 2, DataType::Utf8),
        ));
        let left_filter = group(bool_expr(
            col_ty("l", "a", 1, DataType::Int32),
            BinOp::Eq,
            int_lit(7),
        ));

        let derived =
            derive_inner_join_predicates(&ids(&[1]), &ids(&[2]), &[join_eq], &[left_filter]);

        assert!(
            derived.is_empty(),
            "incompatible key types must not derive predicates: {:?}",
            derived
        );
    }

    #[test]
    fn derives_or_side_filter_from_branch_equalities() {
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

        let derived = derive_inner_join_predicates(&ids(&[1]), &ids(&[2]), &[], &[group(or_pred)]);

        let rendered = format!("{:?}", derived);
        assert!(rendered.contains("\"b\""));
        assert!(rendered.contains("InList") || rendered.contains("Or"));
        assert!(rendered.contains("Int(1)"));
        assert!(rendered.contains("Int(2)"));
    }

    #[test]
    fn derives_or_side_filter_for_different_columns_on_same_side() {
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

        let derived =
            derive_inner_join_predicates(&ids(&[1, 3]), &ids(&[2, 4]), &[], &[group(or_pred)]);

        let rendered = format!("{:?}", derived);
        assert!(rendered.contains("\"b\""));
        assert!(rendered.contains("\"d\""));
        assert!(rendered.contains("Int(1)"));
        assert!(rendered.contains("Int(2)"));
        assert!(rendered.contains("Or"));
    }

    #[test]
    fn nondeterministic_group_suppresses_all_derivation() {
        let join_eq = group(bool_expr(col("l", "a", 1), BinOp::Eq, col("r", "b", 2)));
        let left_filter = group(bool_expr(col("l", "a", 1), BinOp::Eq, int_lit(7)));
        let derived = derive_inner_join_predicates(
            &ids(&[1]),
            &ids(&[2]),
            &[join_eq],
            &[left_filter, nondeterministic_group()],
        );

        assert!(derived.is_empty());
    }

    #[test]
    fn or_branch_missing_same_side_constraint_derives_nothing() {
        let or_pred = bool_expr(
            bool_expr(
                bool_expr(col("l", "a", 1), BinOp::Eq, col("r", "b", 2)),
                BinOp::And,
                bool_expr(col("l", "a", 1), BinOp::Eq, int_lit(1)),
            ),
            BinOp::Or,
            bool_expr(col("l", "a", 1), BinOp::Eq, col("r", "b", 2)),
        );

        let derived = derive_inner_join_predicates(&ids(&[1]), &ids(&[2]), &[], &[group(or_pred)]);

        assert!(
            derived.is_empty(),
            "missing branch side constraint must not derive: {:?}",
            derived
        );
    }

    #[test]
    fn derives_range_envelope_from_or_branches() {
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

        let derived = derive_inner_join_predicates(&ids(&[1]), &ids(&[3]), &[], &[group(or_pred)]);

        let rendered = format!("{:?}", derived);
        assert!(rendered.contains("\"price\""));
        assert!(rendered.contains("Int(50)"));
        assert!(rendered.contains("Int(200)"));
    }

    #[test]
    fn range_envelope_compares_large_int_bounds_exactly() {
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

        let derived = derive_inner_join_predicates(&ids(&[1]), &ids(&[3]), &[], &[group(or_pred)]);

        let rendered = format!("{:?}", derived);
        assert!(rendered.contains("LargeInt(9007199254740992)"));
        assert!(!rendered.contains("LargeInt(9007199254740993)"));
    }

    #[test]
    fn range_envelope_compares_decimal_bounds_exactly() {
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

        let derived = derive_inner_join_predicates(&ids(&[1]), &ids(&[3]), &[], &[group(or_pred)]);

        let rendered = format!("{:?}", derived);
        assert!(rendered.contains("Decimal(\"9.50\")"));
    }

    #[test]
    fn unsupported_range_literal_skips_range_envelope() {
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

        let derived = derive_inner_join_predicates(&ids(&[1]), &ids(&[3]), &[], &[group(or_pred)]);

        assert!(
            derived
                .iter()
                .all(|group| group.derived != PredicateDerivedKind::RangeEnvelope),
            "unsupported literals must not produce range envelope: {:?}",
            derived
        );
    }
}
