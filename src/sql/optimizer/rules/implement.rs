//! Implementation rules: logical operator -> physical operator(s).
//!
//! Each struct implements the `Rule` trait. The `apply` method constructs the
//! physical variant of the matched logical operator, preserving child GroupIds.

use std::collections::HashSet;

use arrow::datatypes::DataType;

use crate::sql::analysis::{BinOp, ExprKind, JoinKind, TypedExpr};
use crate::sql::codegen::helpers::typed_expr_display_name;
use crate::sql::optimizer::memo::{GroupId, MExpr, Memo};
use crate::sql::optimizer::operator::*;
use crate::sql::optimizer::rule::{NewExpr, Rule, RuleType};

/// Get lowercase column names from a memo group's output columns.
pub(super) fn get_group_column_names(memo: &Memo, group_id: GroupId) -> HashSet<String> {
    memo.groups
        .get(group_id)
        .and_then(|g| g.logical_props.as_ref())
        .map(|props| {
            props
                .output_columns
                .iter()
                .map(|c| c.name.to_lowercase())
                .collect()
        })
        .unwrap_or_default()
}

/// Walk a TypedExpr and return the set of lowercase column names it references.
pub(super) fn collect_column_refs_lowercase(expr: &TypedExpr) -> HashSet<String> {
    let mut out = HashSet::new();
    walk_column_refs(expr, &mut out);
    out
}

fn walk_column_refs(expr: &TypedExpr, out: &mut HashSet<String>) {
    match &expr.kind {
        ExprKind::ColumnRef { column, .. } => {
            out.insert(column.to_lowercase());
        }
        ExprKind::BinaryOp { left, right, .. } => {
            walk_column_refs(left, out);
            walk_column_refs(right, out);
        }
        ExprKind::UnaryOp { expr, .. } => {
            walk_column_refs(expr, out);
        }
        ExprKind::FunctionCall { args, .. } => {
            for a in args {
                walk_column_refs(a, out);
            }
        }
        ExprKind::AggregateCall { args, order_by, .. } => {
            for a in args {
                walk_column_refs(a, out);
            }
            for item in order_by {
                walk_column_refs(&item.expr, out);
            }
        }
        ExprKind::Cast { expr, .. } => {
            walk_column_refs(expr, out);
        }
        ExprKind::IsNull { expr, .. } => {
            walk_column_refs(expr, out);
        }
        ExprKind::InList { expr, list, .. } => {
            walk_column_refs(expr, out);
            for item in list {
                walk_column_refs(item, out);
            }
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            walk_column_refs(expr, out);
            walk_column_refs(low, out);
            walk_column_refs(high, out);
        }
        ExprKind::Like { expr, pattern, .. } => {
            walk_column_refs(expr, out);
            walk_column_refs(pattern, out);
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(op) = operand {
                walk_column_refs(op, out);
            }
            for (cond, val) in when_then {
                walk_column_refs(cond, out);
                walk_column_refs(val, out);
            }
            if let Some(e) = else_expr {
                walk_column_refs(e, out);
            }
        }
        ExprKind::IsTruthValue { expr, .. } => {
            walk_column_refs(expr, out);
        }
        ExprKind::Nested(inner) => {
            walk_column_refs(inner, out);
        }
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for a in args {
                walk_column_refs(a, out);
            }
            for p in partition_by {
                walk_column_refs(p, out);
            }
            for item in order_by {
                walk_column_refs(&item.expr, out);
            }
        }
        ExprKind::Literal(_) | ExprKind::SubqueryPlaceholder { .. } => {}
    }
}

/// Orient an eq pair so that the first element references the left child's
/// columns and the second references the right. Returns:
///   - `Some((a, b))` if natural order is confirmed or plausible.
///   - `Some((b, a))` if swapping is unambiguously correct.
///   - `None` only when we are certain BOTH sides reference the same single
///     child (e.g. `t.a = t.b` where both come exclusively from left).
///     The caller demotes such pairs into the residual "other" predicate.
///
/// This is a best-effort heuristic used to set `output_properties` correctly
/// for the CBO search.  The fragment builder has a try-natural-then-swap
/// fallback that handles any cases where this function's guess is wrong
/// (e.g. self-joins where the same column name appears in both children, or
/// when logical_props is missing for a child group).
fn orient_eq_pair(
    pair: PhysicalHashJoinEqCondition,
    left_cols: &HashSet<String>,
    right_cols: &HashSet<String>,
) -> Option<PhysicalHashJoinEqCondition> {
    let PhysicalHashJoinEqCondition {
        left: a,
        right: b,
        null_safe,
    } = pair;
    let a_cols = collect_column_refs_lowercase(&a);
    let b_cols = collect_column_refs_lowercase(&b);

    let a_in_left = !a_cols.is_empty() && a_cols.iter().all(|c| left_cols.contains(c));
    let a_in_right = !a_cols.is_empty() && a_cols.iter().all(|c| right_cols.contains(c));
    let b_in_left = !b_cols.is_empty() && b_cols.iter().all(|c| left_cols.contains(c));
    let b_in_right = !b_cols.is_empty() && b_cols.iter().all(|c| right_cols.contains(c));

    // Unambiguous exclusive assignment: a from left only, b from right only.
    if a_in_left && !a_in_right && b_in_right && !b_in_left {
        return Some(PhysicalHashJoinEqCondition {
            left: a,
            right: b,
            null_safe,
        });
    }
    // Unambiguous exclusive swap: a from right only, b from left only.
    if a_in_right && !a_in_left && b_in_left && !b_in_right {
        return Some(PhysicalHashJoinEqCondition {
            left: b,
            right: a,
            null_safe,
        });
    }
    // Ambiguous or unknown: preserve natural order.  The fragment builder's
    // try-swap fallback will handle any incorrect orientation at compile time.
    // We only demote to None when BOTH sides are exclusively from the same
    // child (proven intra-child predicate, never a valid equi-join key).
    let both_exclusively_left = a_in_left && !a_in_right && b_in_left && !b_in_right;
    let both_exclusively_right = a_in_right && !a_in_left && b_in_right && !b_in_left;
    if both_exclusively_left || both_exclusively_right {
        return None;
    }
    Some(PhysicalHashJoinEqCondition {
        left: a,
        right: b,
        null_safe,
    })
}

// ---------------------------------------------------------------------------
// Helper: extract equality conditions from a join predicate
// ---------------------------------------------------------------------------

/// Walk a join condition and split top-level AND-connected `a = b` pairs from
/// the remaining predicate. Returns `(eq_pairs, remaining_condition)`.
///
/// Also handles OR-connected disjuncts: if the top-level condition (or a
/// top-level conjunct) is `(A AND eq) OR (B AND eq) OR …`, the equality
/// pairs that appear in *every* OR branch are extracted as hash join keys.
///
/// For cross joins (condition is `None`) or when no equalities are found,
/// `eq_pairs` will be empty.
fn extract_eq_conditions(
    condition: &Option<TypedExpr>,
    _join_type: &JoinKind,
) -> (Vec<PhysicalHashJoinEqCondition>, Option<TypedExpr>) {
    let Some(cond) = condition else {
        return (vec![], None);
    };
    let mut eq_pairs = Vec::new();
    let mut others = Vec::new();
    collect_conjuncts(cond, &mut eq_pairs, &mut others);

    // If no equalities were found from top-level AND, try to extract common
    // equalities from OR branches among the "other" predicates.
    if eq_pairs.is_empty() {
        let mut new_others = Vec::new();
        for part in others {
            let (common, rewritten) = try_extract_common_eq_from_or(&part);
            eq_pairs.extend(common);
            if let Some(r) = rewritten {
                new_others.push(r);
            }
        }
        others = new_others;
    }

    let remaining = combine_conjuncts(others);
    (eq_pairs, remaining)
}

/// Recursively flatten top-level AND nodes and classify each conjunct as
/// either an equality pair or a residual predicate.
fn collect_conjuncts(
    expr: &TypedExpr,
    eq_pairs: &mut Vec<PhysicalHashJoinEqCondition>,
    others: &mut Vec<TypedExpr>,
) {
    match &expr.kind {
        // Unwrap parenthesized expressions transparently.
        ExprKind::Nested(inner) => {
            collect_conjuncts(inner, eq_pairs, others);
        }
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            collect_conjuncts(left, eq_pairs, others);
            collect_conjuncts(right, eq_pairs, others);
        }
        ExprKind::BinaryOp { left, op, right } if matches!(op, BinOp::Eq | BinOp::EqForNull) => {
            // Only treat as equi-join key if BOTH sides are column refs.
            // Expressions like `d_year = 2002` (column = constant) are filters,
            // not equi-join keys.
            let left_is_col = matches!(left.kind, ExprKind::ColumnRef { .. });
            let right_is_col = matches!(right.kind, ExprKind::ColumnRef { .. });
            if left_is_col && right_is_col {
                eq_pairs.push(PhysicalHashJoinEqCondition {
                    left: *left.clone(),
                    right: *right.clone(),
                    null_safe: matches!(op, BinOp::EqForNull),
                });
            } else {
                others.push(expr.clone());
            }
        }
        _ => {
            others.push(expr.clone());
        }
    }
}

/// Split a top-level OR expression into its disjuncts.
fn split_or(expr: &TypedExpr) -> Vec<TypedExpr> {
    match &expr.kind {
        ExprKind::Nested(inner) => split_or(inner),
        ExprKind::BinaryOp {
            left,
            op: BinOp::Or,
            right,
        } => {
            let mut parts = split_or(left);
            parts.extend(split_or(right));
            parts
        }
        _ => vec![expr.clone()],
    }
}

/// Combine a list of disjuncts back into a single OR-connected expression.
fn combine_disjuncts(mut parts: Vec<TypedExpr>) -> Option<TypedExpr> {
    if parts.is_empty() {
        return None;
    }
    let mut result = parts.pop().unwrap();
    while let Some(p) = parts.pop() {
        result = TypedExpr {
            data_type: arrow::datatypes::DataType::Boolean,
            nullable: p.nullable || result.nullable,
            kind: ExprKind::BinaryOp {
                left: Box::new(p),
                op: BinOp::Or,
                right: Box::new(result),
            },
        };
    }
    Some(result)
}

/// Structural equality for TypedExpr using Debug representation.
fn typed_expr_eq(a: &TypedExpr, b: &TypedExpr) -> bool {
    format!("{:?}", a) == format!("{:?}", b)
}

/// Check if two eq pairs are structurally equal (possibly with swapped sides).
fn eq_pair_matches(a: &PhysicalHashJoinEqCondition, b: &PhysicalHashJoinEqCondition) -> bool {
    a.null_safe == b.null_safe
        && ((typed_expr_eq(&a.left, &b.left) && typed_expr_eq(&a.right, &b.right))
            || (typed_expr_eq(&a.left, &b.right) && typed_expr_eq(&a.right, &b.left)))
}

/// Try to extract common equality conditions from an OR expression.
///
/// Given `(A AND x=y AND B) OR (C AND x=y AND D)`, extracts `(x, y)` as
/// a common eq pair and rewrites the expression to `(A AND B) OR (C AND D)`.
///
/// Returns `(common_eq_pairs, rewritten_or_condition)`.
fn try_extract_common_eq_from_or(
    expr: &TypedExpr,
) -> (Vec<PhysicalHashJoinEqCondition>, Option<TypedExpr>) {
    let branches = split_or(expr);
    if branches.len() < 2 {
        return (vec![], Some(expr.clone()));
    }

    // For each branch, extract eq pairs and residual.
    let mut branch_eqs: Vec<Vec<PhysicalHashJoinEqCondition>> = Vec::new();
    let mut branch_others: Vec<Vec<TypedExpr>> = Vec::new();
    for branch in &branches {
        let mut eqs = Vec::new();
        let mut others = Vec::new();
        collect_conjuncts(branch, &mut eqs, &mut others);
        branch_eqs.push(eqs);
        branch_others.push(others);
    }

    // Find eq pairs that appear in ALL branches.
    let first_eqs = &branch_eqs[0];
    let mut common: Vec<PhysicalHashJoinEqCondition> = Vec::new();
    for eq in first_eqs {
        if branch_eqs[1..]
            .iter()
            .all(|branch| branch.iter().any(|b| eq_pair_matches(eq, b)))
        {
            common.push(eq.clone());
        }
    }

    if common.is_empty() {
        return (vec![], Some(expr.clone()));
    }

    // Rewrite each branch: remove the common eq pairs, recombine.
    let mut rewritten_branches = Vec::new();
    for (eqs, others) in branch_eqs.iter().zip(branch_others.iter()) {
        let mut remaining_parts: Vec<TypedExpr> = others.clone();
        for eq in eqs {
            if !common.iter().any(|c| eq_pair_matches(c, eq)) {
                // Keep non-common eq pairs as regular conjuncts.
                remaining_parts.push(TypedExpr {
                    data_type: DataType::Boolean,
                    nullable: eq.left.nullable || eq.right.nullable,
                    kind: ExprKind::BinaryOp {
                        left: Box::new(eq.left.clone()),
                        op: if eq.null_safe {
                            BinOp::EqForNull
                        } else {
                            BinOp::Eq
                        },
                        right: Box::new(eq.right.clone()),
                    },
                });
            }
        }
        if let Some(branch_expr) = combine_conjuncts(remaining_parts) {
            rewritten_branches.push(branch_expr);
        }
        // If a branch becomes empty (only common eqs), skip it — it
        // effectively becomes TRUE, making the whole OR always true for
        // matched eq keys.  We represent this by omitting the branch.
    }

    let rewritten = if rewritten_branches.len() == branches.len() {
        combine_disjuncts(rewritten_branches)
    } else {
        // Some branches were pure eq-only; the entire OR condition is
        // satisfied whenever the common equalities hold.
        None
    };

    (common, rewritten)
}

/// Combine a list of residual predicates back into a single AND-connected
/// expression. Returns `None` if the list is empty.
fn combine_conjuncts(mut parts: Vec<TypedExpr>) -> Option<TypedExpr> {
    if parts.is_empty() {
        return None;
    }
    let mut result = parts.pop().unwrap();
    while let Some(p) = parts.pop() {
        result = TypedExpr {
            data_type: arrow::datatypes::DataType::Boolean,
            nullable: p.nullable || result.nullable,
            kind: ExprKind::BinaryOp {
                left: Box::new(p),
                op: BinOp::And,
                right: Box::new(result),
            },
        };
    }
    Some(result)
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
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalScan(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalScan(PhysicalScanOp {
                database: op.database.clone(),
                table: op.table.clone(),
                alias: op.alias.clone(),
                columns: op.columns.clone(),
                predicates: op.predicates.clone(),
                required_columns: op.required_columns.clone(),
            }),
            children: expr.children.clone(),
        }]
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
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalFilter(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalFilter(PhysicalFilterOp {
                predicate: op.predicate.clone(),
            }),
            children: expr.children.clone(),
        }]
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
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalProject(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalProject(PhysicalProjectOp {
                items: op.items.clone(),
            }),
            children: expr.children.clone(),
        }]
    }
}

// ---------------------------------------------------------------------------
// 4. JoinToHashJoin
// ---------------------------------------------------------------------------

pub(crate) struct JoinToHashJoin;

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
    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalJoin(op) = &expr.op else {
            return vec![];
        };
        let (raw_eq_conds, mut other) = extract_eq_conditions(&op.condition, &op.join_type);

        // Orient eq_conditions so that pair.0 references the left child's
        // columns and pair.1 references the right child's columns.  Pairs
        // that reference only one side (e.g., inner predicates in a SEMI
        // JOIN condition) are demoted into other_condition.
        let mut eq_conds = Vec::new();
        if expr.children.len() == 2 {
            let left_cols = get_group_column_names(memo, expr.children[0]);
            let right_cols = get_group_column_names(memo, expr.children[1]);
            for pair in raw_eq_conds {
                let a = pair.left.clone();
                let b = pair.right.clone();
                let null_safe = pair.null_safe;
                match orient_eq_pair(pair, &left_cols, &right_cols) {
                    Some(oriented) => eq_conds.push(oriented),
                    None => {
                        let demoted = TypedExpr {
                            data_type: DataType::Boolean,
                            nullable: false,
                            kind: ExprKind::BinaryOp {
                                left: Box::new(a),
                                op: if null_safe {
                                    BinOp::EqForNull
                                } else {
                                    BinOp::Eq
                                },
                                right: Box::new(b),
                            },
                        };
                        other = Some(match other {
                            Some(existing) => TypedExpr {
                                data_type: DataType::Boolean,
                                nullable: false,
                                kind: ExprKind::BinaryOp {
                                    left: Box::new(existing),
                                    op: BinOp::And,
                                    right: Box::new(demoted),
                                },
                            },
                            None => demoted,
                        });
                    }
                }
            }
        } else {
            eq_conds = raw_eq_conds;
        }

        if eq_conds.is_empty() {
            // No equality conditions — JoinToNestLoop should handle this.
            return vec![];
        }
        vec![
            NewExpr {
                op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                    join_type: op.join_type,
                    eq_conditions: eq_conds.clone(),
                    other_condition: other.clone(),
                    distribution: JoinDistribution::Shuffle,
                }),
                children: expr.children.clone(),
            },
            NewExpr {
                op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                    join_type: op.join_type,
                    eq_conditions: eq_conds,
                    other_condition: other,
                    distribution: JoinDistribution::Broadcast,
                }),
                children: expr.children.clone(),
            },
        ]
    }
}

// ---------------------------------------------------------------------------
// 5. JoinToNestLoop
// ---------------------------------------------------------------------------

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
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalJoin(op) = &expr.op else {
            return vec![];
        };
        // NestLoop is used for cross joins or joins without equality conditions.
        let (eq_conds, _) = extract_eq_conditions(&op.condition, &op.join_type);
        if !eq_conds.is_empty() && op.join_type != JoinKind::Cross {
            // Has equality conditions — JoinToHashJoin should handle this.
            return vec![];
        }
        vec![NewExpr {
            op: Operator::PhysicalNestLoopJoin(PhysicalNestLoopJoinOp {
                join_type: op.join_type,
                condition: op.condition.clone(),
            }),
            children: expr.children.clone(),
        }]
    }
}

// ---------------------------------------------------------------------------
// 6. AggToHashAgg
// ---------------------------------------------------------------------------

pub(crate) struct AggToHashAgg;

fn aggregate_group_key_output_ref(expr: &TypedExpr) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::ColumnRef {
            qualifier: None,
            column: typed_expr_display_name(expr),
        },
        data_type: expr.data_type.clone(),
        nullable: expr.nullable,
    }
}

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
    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalAggregate(op) = &expr.op else {
            return vec![];
        };

        // Alternative 1: Single-phase aggregation (always applicable).
        let single = NewExpr {
            op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: AggMode::Single,
                group_by: op.group_by.clone(),
                aggregates: op.aggregates.clone(),
                output_columns: op.output_columns.clone(),
                is_merge: vec![false; op.aggregates.len()],
            }),
            children: expr.children.clone(),
        };

        // Two-phase Local+Global: skip when any aggregate is DISTINCT or
        // when there are no group-by keys (scalar agg — deferred).
        let has_distinct = op.aggregates.iter().any(|a| a.distinct);
        if has_distinct || op.group_by.is_empty() {
            return vec![single];
        }

        // Alternative 2: Local pre-agg → (hash exchange inserted by enforcer) → Global merge.
        let local_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: AggMode::Local,
                group_by: op.group_by.clone(),
                aggregates: op.aggregates.clone(),
                output_columns: op.output_columns.clone(),
                is_merge: vec![false; op.aggregates.len()],
            }),
            children: expr.children.clone(),
        };
        let local_group_id = memo.new_group(local_mexpr);
        let global_group_by = op
            .group_by
            .iter()
            .map(aggregate_group_key_output_ref)
            .collect();

        let global = NewExpr {
            op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: AggMode::Global,
                group_by: global_group_by,
                aggregates: op.aggregates.clone(),
                output_columns: op.output_columns.clone(),
                is_merge: vec![true; op.aggregates.len()],
            }),
            children: vec![local_group_id],
        };

        vec![single, global]
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
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalSort(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalSort(PhysicalSortOp {
                items: op.items.clone(),
            }),
            children: expr.children.clone(),
        }]
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
    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalLimit(op) = &expr.op else {
            return vec![];
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
                return vec![];
            }
        }

        vec![NewExpr {
            op: Operator::PhysicalLimit(PhysicalLimitOp {
                limit: op.limit,
                offset: op.offset,
            }),
            children: expr.children.clone(),
        }]
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
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalTopN(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalTopN(PhysicalTopNOp {
                items: op.items.clone(),
                limit: op.limit,
                offset: op.offset,
                phase: op.phase,
                is_split: op.is_split,
            }),
            children: expr.children.clone(),
        }]
    }
}

// ---------------------------------------------------------------------------
// 9. WindowToPhysical
// ---------------------------------------------------------------------------

/// Split a LogicalWindow's expressions into groups sharing the same
/// (partition_by, order_by) signature. Preserves first-seen order.
#[allow(dead_code)]
fn split_window_exprs_by_signature(
    exprs: &[crate::sql::planner::plan::WindowExpr],
) -> Vec<Vec<crate::sql::planner::plan::WindowExpr>> {
    let index_groups = crate::sql::codegen::helpers::group_win_exprs_by_sig(exprs);
    index_groups
        .into_iter()
        .map(|idxs| idxs.into_iter().map(|i| exprs[i].clone()).collect())
        .collect()
}

/// Derive sort items for a window's partition_by + order_by.
/// Window sort ordering is: partition_by columns first (ASC, NULLS FIRST),
/// then order_by columns with their own direction.
#[allow(dead_code)]
fn sort_items_for_window(
    win: &crate::sql::planner::plan::WindowExpr,
) -> Vec<crate::sql::analysis::SortItem> {
    let mut items = Vec::new();
    for expr in &win.partition_by {
        items.push(crate::sql::analysis::SortItem {
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
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalWindow(op) = &expr.op else {
            return vec![];
        };
        if expr.children.len() != 1 {
            return vec![];
        }
        let child_group = expr.children[0];

        // Emit a single PhysicalWindow with all window expressions.
        // The fragment builder groups expressions by (partition_by, order_by)
        // signature internally and emits one Sort+Analytic node per group —
        // all within the same fragment, without cross-group exchanges.
        // Cascades-level splitting (one PhysicalWindow per signature group)
        // would cause the CBO to insert distribution enforcers (HASH EXCHANGE)
        // between window nodes when their partition key sets differ, which
        // breaks pipelined analytic execution.
        vec![NewExpr {
            op: Operator::PhysicalWindow(PhysicalWindowOp {
                window_exprs: op.window_exprs.clone(),
                output_columns: op.output_columns.clone(),
            }),
            children: vec![child_group],
        }]
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
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalCTEAnchor(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalCTEAnchor(PhysicalCTEAnchorOp { cte_id: op.cte_id }),
            children: expr.children.clone(),
        }]
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
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalCTEProduce(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalCTEProduce(PhysicalCTEProduceOp {
                cte_id: op.cte_id,
                output_columns: op.output_columns.clone(),
            }),
            children: expr.children.clone(),
        }]
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
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalCTEConsume(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalCTEConsume(PhysicalCTEConsumeOp {
                cte_id: op.cte_id,
                alias: op.alias.clone(),
                output_columns: op.output_columns.clone(),
            }),
            children: expr.children.clone(),
        }]
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
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalRepeat(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalRepeat(PhysicalRepeatOp {
                repeat_column_ref_list: op.repeat_column_ref_list.clone(),
                grouping_ids: op.grouping_ids.clone(),
                all_rollup_columns: op.all_rollup_columns.clone(),
                grouping_key_aliases: op.grouping_key_aliases.clone(),
                grouping_fn_args: op.grouping_fn_args.clone(),
            }),
            children: expr.children.clone(),
        }]
    }
}

// ---------------------------------------------------------------------------
// 14. UnionToPhysical
// ---------------------------------------------------------------------------

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
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalUnion(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalUnion(PhysicalUnionOp { all: op.all }),
            children: expr.children.clone(),
        }]
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
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        vec![NewExpr {
            op: Operator::PhysicalIntersect(PhysicalIntersectOp),
            children: expr.children.clone(),
        }]
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
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        vec![NewExpr {
            op: Operator::PhysicalExcept(PhysicalExceptOp),
            children: expr.children.clone(),
        }]
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
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalValues(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalValues(PhysicalValuesOp {
                rows: op.rows.clone(),
                columns: op.columns.clone(),
            }),
            children: expr.children.clone(),
        }]
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
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalGenerateSeries(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalGenerateSeries(PhysicalGenerateSeriesOp {
                start: op.start,
                end: op.end,
                step: op.step,
                column_name: op.column_name.clone(),
                alias: op.alias.clone(),
            }),
            children: expr.children.clone(),
        }]
    }
}

// ---------------------------------------------------------------------------
// 18. SubqueryAliasToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct SubqueryAliasToPhysical;

impl Rule for SubqueryAliasToPhysical {
    fn name(&self) -> &str {
        "SubqueryAliasToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalSubqueryAlias(_))
    }
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalSubqueryAlias(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalSubqueryAlias(PhysicalSubqueryAliasOp {
                alias: op.alias.clone(),
                output_columns: op.output_columns.clone(),
            }),
            children: expr.children.clone(),
        }]
    }
}

#[cfg(test)]
mod top_n_tests {
    use super::*;
    use crate::sql::optimizer::memo::{MExpr, Memo};
    use crate::sql::optimizer::operator::{LogicalTopNOp, TopNPhase};

    #[test]
    fn top_n_to_physical_produces_physical_top_n() {
        let mut memo = Memo::new();
        let values_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(LogicalValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        };
        let dummy_child = memo.new_group(values_mexpr);

        let expr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(LogicalTopNOp {
                items: vec![],
                limit: Some(50),
                offset: Some(10),
                phase: TopNPhase::Final,
                is_split: false,
            }),
            children: vec![dummy_child],
        };
        let rule = TopNToPhysical;
        let out = rule.apply(&expr, &mut memo);
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
}

#[cfg(test)]
mod eq_pair_tests {
    use super::*;
    use arrow::datatypes::DataType;

    fn col(name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: name.into(),
            },
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    fn cols(names: &[&str]) -> HashSet<String> {
        names.iter().map(|s| s.to_lowercase()).collect()
    }

    fn eq_pair(left: TypedExpr, right: TypedExpr) -> PhysicalHashJoinEqCondition {
        PhysicalHashJoinEqCondition {
            left,
            right,
            null_safe: false,
        }
    }

    #[test]
    fn orient_natural_order_keeps_order() {
        let left = cols(&["a_id"]);
        let right = cols(&["b_id"]);
        let pair = eq_pair(col("a_id"), col("b_id"));
        let out = orient_eq_pair(pair, &left, &right).expect("should orient");
        match &out.left.kind {
            ExprKind::ColumnRef { column, .. } => assert_eq!(column, "a_id"),
            _ => panic!("expected ColumnRef"),
        }
        match &out.right.kind {
            ExprKind::ColumnRef { column, .. } => assert_eq!(column, "b_id"),
            _ => panic!("expected ColumnRef"),
        }
    }

    #[test]
    fn orient_swapped_pair_returns_swapped() {
        let left = cols(&["a_id"]);
        let right = cols(&["b_id"]);
        let pair = eq_pair(col("b_id"), col("a_id"));
        let out = orient_eq_pair(pair, &left, &right).expect("should orient");
        match &out.left.kind {
            ExprKind::ColumnRef { column, .. } => assert_eq!(column, "a_id"),
            _ => panic!("expected ColumnRef"),
        }
        match &out.right.kind {
            ExprKind::ColumnRef { column, .. } => assert_eq!(column, "b_id"),
            _ => panic!("expected ColumnRef"),
        }
    }

    #[test]
    fn orient_single_side_pair_returns_none() {
        let left = cols(&["a_id", "a_name"]);
        let right = cols(&["b_id"]);
        let pair = eq_pair(col("a_id"), col("a_name"));
        assert!(orient_eq_pair(pair, &left, &right).is_none());
    }
}

#[cfg(test)]
mod join_demotion_tests {
    use super::*;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::catalog::{TableDef, TableStorage};
    use crate::sql::optimizer::memo::{LogicalProperties, MExpr, Memo};
    use arrow::datatypes::DataType;

    fn col(name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: name.into(),
            },
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    /// Create a scan group whose logical_props report the given output columns.
    fn mk_scan_group(memo: &mut Memo, col_names: &[&str]) -> usize {
        let output_columns: Vec<OutputColumn> = col_names
            .iter()
            .map(|name| OutputColumn {
                name: (*name).into(),
                data_type: DataType::Int32,
                nullable: false,
            })
            .collect();
        let scan_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalScan(LogicalScanOp {
                database: "db".into(),
                table: TableDef {
                    name: "t".into(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    iceberg_table: None,
                    storage: TableStorage::LocalParquetFile {
                        path: std::path::PathBuf::from("/tmp/t.parquet"),
                    },
                },
                alias: None,
                columns: output_columns.clone(),
                predicates: vec![],
                required_columns: None,
            }),
            children: vec![],
        };
        let gid = memo.new_group(scan_mexpr);
        // Inject logical_props so get_group_column_names returns the column names.
        memo.groups[gid].logical_props = Some(LogicalProperties {
            output_columns,
            row_count: 100.0,
        });
        gid
    }

    /// Build `left op right` as a TypedExpr.
    fn bin(left: TypedExpr, op: BinOp, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    /// The full demotion path: a same-side eq pair must land in other_condition
    /// while an orientable pair lands (correctly oriented) in eq_conditions.
    #[test]
    fn demoted_single_side_pair_ends_in_other_condition() {
        let mut memo = Memo::new();

        // Left side: columns [a_id, a_name].  Right side: column [b_id].
        let left_group = mk_scan_group(&mut memo, &["a_id", "a_name"]);
        let right_group = mk_scan_group(&mut memo, &["b_id"]);

        // Condition: (a_id = b_id) AND (a_id = a_name)
        //   • First pair  (a_id, b_id)  — orientable (a_id left, b_id right).
        //   • Second pair (a_id, a_name) — same-side (both left) → must demote.
        let first_eq = bin(col("a_id"), BinOp::Eq, col("b_id"));
        let second_eq = bin(col("a_id"), BinOp::Eq, col("a_name"));
        let condition = bin(first_eq, BinOp::And, second_eq);

        let join_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: Some(condition),
            }),
            children: vec![left_group, right_group],
        };

        let rule = JoinToHashJoin;
        let alternatives = rule.apply(&join_mexpr, &mut memo);

        // Expect two alternatives (Shuffle + Broadcast).
        assert!(
            !alternatives.is_empty(),
            "expected at least one alternative from JoinToHashJoin"
        );

        // Both alternatives must have the same eq_conditions / other_condition shape;
        // spot-check the first one.
        let alt = &alternatives[0];
        let Operator::PhysicalHashJoin(phys) = &alt.op else {
            panic!("expected PhysicalHashJoin, got {:?}", alt.op);
        };

        // ── eq_conditions: exactly one pair, (a_id, b_id) ──────────────────
        assert_eq!(
            phys.eq_conditions.len(),
            1,
            "expected 1 eq pair in eq_conditions, got {:?}",
            phys.eq_conditions
        );
        let eq = &phys.eq_conditions[0];
        assert!(
            !eq.null_safe,
            "regular equality should not be marked null-safe"
        );
        let (lhs, rhs) = (&eq.left, &eq.right);
        match &lhs.kind {
            ExprKind::ColumnRef { column, .. } => {
                assert_eq!(column, "a_id", "left side of eq_condition should be a_id")
            }
            other => panic!("expected ColumnRef on left of eq pair, got {:?}", other),
        }
        match &rhs.kind {
            ExprKind::ColumnRef { column, .. } => {
                assert_eq!(column, "b_id", "right side of eq_condition should be b_id")
            }
            other => panic!("expected ColumnRef on right of eq pair, got {:?}", other),
        }

        // ── other_condition: the demoted (a_id = a_name) pair ───────────────
        let other = phys
            .other_condition
            .as_ref()
            .expect("demoted same-side pair must appear in other_condition");
        match &other.kind {
            ExprKind::BinaryOp { left, op, right } => {
                assert!(
                    matches!(op, BinOp::Eq),
                    "demoted condition should be BinaryOp::Eq, got {:?}",
                    op
                );
                match (&left.kind, &right.kind) {
                    (
                        ExprKind::ColumnRef { column: l, .. },
                        ExprKind::ColumnRef { column: r, .. },
                    ) => {
                        assert!(
                            (l == "a_id" && r == "a_name") || (l == "a_name" && r == "a_id"),
                            "expected (a_id, a_name) in demoted eq, got ({}, {})",
                            l,
                            r
                        );
                    }
                    other => panic!(
                        "expected two ColumnRef nodes inside demoted eq, got {:?}",
                        other
                    ),
                }
            }
            other => panic!("expected BinaryOp::Eq in other_condition, got {:?}", other),
        }
    }

    #[test]
    fn null_safe_join_pair_stays_hash_join_key() {
        let mut memo = Memo::new();
        let left_group = mk_scan_group(&mut memo, &["a_id"]);
        let right_group = mk_scan_group(&mut memo, &["b_id"]);

        let condition = bin(col("a_id"), BinOp::EqForNull, col("b_id"));
        let join_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type: JoinKind::Inner,
                condition: Some(condition),
            }),
            children: vec![left_group, right_group],
        };

        let rule = JoinToHashJoin;
        let alternatives = rule.apply(&join_mexpr, &mut memo);
        assert!(
            !alternatives.is_empty(),
            "expected null-safe equality to produce hash join alternatives"
        );
        let Operator::PhysicalHashJoin(phys) = &alternatives[0].op else {
            panic!("expected PhysicalHashJoin, got {:?}", alternatives[0].op);
        };
        assert_eq!(phys.eq_conditions.len(), 1);
        assert!(
            phys.eq_conditions[0].null_safe,
            "<=> hash join key must retain null-safe semantics"
        );
        assert!(
            phys.other_condition.is_none(),
            "<=> should not be left as a residual-only predicate"
        );
    }
}

#[cfg(test)]
mod window_split_tests {
    use super::*;
    use crate::sql::planner::plan::WindowExpr;
    use arrow::datatypes::DataType;

    fn mk_window_expr(name: &str, partition: Vec<TypedExpr>) -> WindowExpr {
        WindowExpr {
            name: name.into(),
            args: vec![],
            distinct: false,
            partition_by: partition,
            order_by: vec![],
            window_frame: None,
            result_type: DataType::Int64,
            output_name: name.into(),
            ignore_nulls: false,
        }
    }

    fn col(name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: name.into(),
            },
            data_type: DataType::Int32,
            nullable: false,
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
        let exprs = vec![
            mk_window_expr("w1", vec![col("a")]),
            mk_window_expr("w2", vec![col("b")]),
            mk_window_expr("w3", vec![col("a")]),
        ];
        let groups = split_window_exprs_by_signature(&exprs);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].len(), 2);
        assert_eq!(groups[0][0].name, "w1");
        assert_eq!(groups[0][1].name, "w3");
        assert_eq!(groups[1].len(), 1);
        assert_eq!(groups[1][0].name, "w2");
    }

    #[test]
    fn sort_items_for_window_combines_partition_and_order() {
        use crate::sql::analysis::SortItem;
        let win = WindowExpr {
            name: "w".into(),
            args: vec![],
            distinct: false,
            partition_by: vec![col("a"), col("b")],
            order_by: vec![SortItem {
                expr: col("c"),
                asc: false,
                nulls_first: false,
            }],
            window_frame: None,
            result_type: DataType::Int64,
            output_name: "w".into(),
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
    // and is therefore obsolete. Multi-group decomposition still happens, but
    // at the fragment_builder level (visit_window_multi_group) rather than at
    // the cascades rule level.

    /// Single window expression with empty partition_by and empty order_by.
    /// The signature is empty → single group → no PhysicalSort inserted.
    /// The rule should return a single NewExpr whose child is the original child_group.
    #[test]
    fn window_to_physical_skips_sort_when_empty_signature() {
        let mut memo = Memo::new();

        let values_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(LogicalValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        };
        let child_group = memo.new_group(values_mexpr);

        // Single window with no partition and no order => single group, no sort.
        let logical_window_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalWindow(LogicalWindowOp {
                window_exprs: vec![mk_window_expr("w1", vec![])],
                output_columns: vec![],
            }),
            children: vec![child_group],
        };

        let rule = WindowToPhysical;
        let out = rule.apply(&logical_window_mexpr, &mut memo);

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
    use crate::sql::analysis::OutputColumn;
    use crate::sql::optimizer::memo::{MExpr, Memo};
    use crate::sql::planner::plan::AggregateCall;
    use arrow::datatypes::DataType;

    fn col(name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: name.into(),
            },
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    fn modulo(left: TypedExpr, right: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::Mod,
                right: Box::new(TypedExpr {
                    kind: ExprKind::Literal(crate::sql::analysis::LiteralValue::Int(right)),
                    data_type: DataType::Int64,
                    nullable: false,
                }),
            },
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    #[test]
    fn agg_to_hash_agg_produces_single_and_two_phase() {
        let mut memo = Memo::new();
        let child_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(LogicalValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        };
        let child_group = memo.new_group(child_mexpr);

        let expr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(LogicalAggregateOp {
                group_by: vec![col("city")],
                aggregates: vec![AggregateCall {
                    name: "sum".into(),
                    args: vec![col("amount")],
                    distinct: false,
                    result_type: DataType::Int64,
                    order_by: vec![],
                }],
                output_columns: vec![
                    OutputColumn {
                        name: "city".into(),
                        data_type: DataType::Int32,
                        nullable: false,
                    },
                    OutputColumn {
                        name: "sum(amount)".into(),
                        data_type: DataType::Int64,
                        nullable: true,
                    },
                ],
            }),
            children: vec![child_group],
        };

        let rule = AggToHashAgg;
        let out = rule.apply(&expr, &mut memo);

        // Should produce 2 alternatives: Single and Global.
        assert_eq!(out.len(), 2, "expected Single + Global alternatives");

        // Alternative 1: Single
        match &out[0].op {
            Operator::PhysicalHashAggregate(p) => {
                assert!(matches!(p.mode, AggMode::Single));
            }
            other => panic!("expected PhysicalHashAggregate(Single), got {:?}", other),
        }
        assert_eq!(out[0].children, vec![child_group]);

        // Alternative 2: Global (child is a new group containing Local)
        match &out[1].op {
            Operator::PhysicalHashAggregate(p) => {
                assert!(matches!(p.mode, AggMode::Global));
            }
            other => panic!("expected PhysicalHashAggregate(Global), got {:?}", other),
        }
        let local_group_id = out[1].children[0];
        assert_ne!(local_group_id, child_group);

        // The new group should contain a Local physical expr
        let local_group = &memo.groups[local_group_id];
        assert_eq!(local_group.physical_exprs.len(), 1);
        match &local_group.physical_exprs[0].op {
            Operator::PhysicalHashAggregate(p) => {
                assert!(matches!(p.mode, AggMode::Local));
            }
            other => panic!("expected PhysicalHashAggregate(Local), got {:?}", other),
        }
        assert_eq!(local_group.physical_exprs[0].children, vec![child_group]);
    }

    #[test]
    fn agg_to_hash_agg_skips_two_phase_for_distinct() {
        let mut memo = Memo::new();
        let child_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(LogicalValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        };
        let child_group = memo.new_group(child_mexpr);

        let expr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(LogicalAggregateOp {
                group_by: vec![col("city")],
                aggregates: vec![AggregateCall {
                    name: "count".into(),
                    args: vec![col("id")],
                    distinct: true,
                    result_type: DataType::Int64,
                    order_by: vec![],
                }],
                output_columns: vec![
                    OutputColumn {
                        name: "city".into(),
                        data_type: DataType::Int32,
                        nullable: false,
                    },
                    OutputColumn {
                        name: "count(distinct id)".into(),
                        data_type: DataType::Int64,
                        nullable: true,
                    },
                ],
            }),
            children: vec![child_group],
        };

        let rule = AggToHashAgg;
        let out = rule.apply(&expr, &mut memo);

        assert_eq!(out.len(), 1, "DISTINCT agg should only produce Single");
        match &out[0].op {
            Operator::PhysicalHashAggregate(p) => {
                assert!(matches!(p.mode, AggMode::Single));
            }
            other => panic!("expected PhysicalHashAggregate(Single), got {:?}", other),
        }
    }

    #[test]
    fn agg_to_hash_agg_global_reads_local_group_key_slots() {
        let mut memo = Memo::new();
        let child_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(LogicalValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        };
        let child_group = memo.new_group(child_mexpr);
        let group_expr = modulo(col("city"), 2);

        let expr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalAggregate(LogicalAggregateOp {
                group_by: vec![group_expr.clone()],
                aggregates: vec![AggregateCall {
                    name: "min".into(),
                    args: vec![col("amount")],
                    distinct: false,
                    result_type: DataType::Int32,
                    order_by: vec![],
                }],
                output_columns: vec![
                    OutputColumn {
                        name: "city % 2".into(),
                        data_type: DataType::Int32,
                        nullable: false,
                    },
                    OutputColumn {
                        name: "min(amount)".into(),
                        data_type: DataType::Int32,
                        nullable: true,
                    },
                ],
            }),
            children: vec![child_group],
        };

        let rule = AggToHashAgg;
        let out = rule.apply(&expr, &mut memo);

        let global = match &out[1].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected PhysicalHashAggregate(Global), got {:?}", other),
        };
        assert!(matches!(global.mode, AggMode::Global));
        match &global.group_by[0].kind {
            ExprKind::ColumnRef { qualifier, column } => {
                assert!(qualifier.is_none());
                assert_eq!(column, "city % 2");
            }
            other => panic!("expected Global group key to read Local output slot, got {other:?}"),
        }

        let local_group_id = out[1].children[0];
        let local_group = &memo.groups[local_group_id];
        let local = match &local_group.physical_exprs[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected PhysicalHashAggregate(Local), got {:?}", other),
        };
        assert!(
            matches!(local.group_by[0].kind, ExprKind::BinaryOp { .. }),
            "Local aggregate should keep the original expression group key"
        );
    }
}
