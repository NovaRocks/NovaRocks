//! Implementation rules: logical operator -> physical operator(s).
//!
//! Each struct implements the `Rule` trait. The `apply` method constructs the
//! physical variant of the matched logical operator, preserving child GroupIds.

use std::collections::HashSet;

use arrow::datatypes::DataType;

use crate::sql::analysis::{BinOp, JoinKind};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::memo::{GroupId, MExpr, Memo};
use crate::sql::optimizer::operator::*;
use crate::sql::optimizer::rule::{NewExpr, Rule, RuleType};
use crate::sql::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};
use crate::sql::optimizer::scalar_expr;
use crate::sql::types::wider_type;

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
    let ScalarHashJoinEqCondition {
        left: a,
        right: b,
        null_safe,
    } = pair;
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
            null_safe,
        });
    }
    // Unambiguous exclusive swap: a from right only, b from left only.
    if a_in_right && !a_in_left && b_in_left && !b_in_right {
        return Some(ScalarHashJoinEqCondition {
            left: b,
            right: a,
            null_safe,
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

fn hash_join_eq_condition_is_hashable(arena: &ScalarArena, eq: &ScalarHashJoinEqCondition) -> bool {
    let left_type = arena.data_type(eq.left);
    let right_type = arena.data_type(eq.right);
    if left_type == right_type {
        return hash_join_key_type_is_supported(left_type);
    }
    let common_type = wider_type(left_type, right_type);
    hash_join_key_type_is_supported(&common_type)
}

fn coerce_hash_join_eq_condition(
    arena: &ScalarArena,
    eq: ScalarHashJoinEqCondition,
) -> Option<ScalarHashJoinEqCondition> {
    if hash_join_eq_condition_is_hashable(arena, &eq) {
        return Some(eq);
    }
    None
}

fn eq_condition_to_expr(arena: &mut ScalarArena, eq: ScalarHashJoinEqCondition) -> ScalarId {
    arena.intern(
        ScalarNode::BinaryOp {
            left: eq.left,
            op: if eq.null_safe {
                BinOp::EqForNull
            } else {
                BinOp::Eq
            },
            right: eq.right,
        },
        DataType::Boolean,
        if eq.null_safe {
            false
        } else {
            arena.nullable(eq.left) || arena.nullable(eq.right)
        },
    )
}

fn append_residual_condition(
    arena: &mut ScalarArena,
    other: &mut Option<ScalarId>,
    residual: ScalarId,
) {
    *other = Some(match other.take() {
        Some(existing) => arena.intern(
            ScalarNode::BinaryOp {
                left: existing,
                op: BinOp::And,
                right: residual,
            },
            DataType::Boolean,
            false,
        ),
        None => residual,
    });
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
    condition: Option<ScalarId>,
    _join_type: &JoinKind,
    arena: &mut ScalarArena,
) -> (Vec<ScalarHashJoinEqCondition>, Option<ScalarId>) {
    let Some(cond) = condition else {
        return (vec![], None);
    };
    let mut eq_pairs = Vec::new();
    let mut others = Vec::new();
    collect_conjuncts(arena, cond, &mut eq_pairs, &mut others);

    // If no equalities were found from top-level AND, try to extract common
    // equalities from OR branches among the "other" predicates.
    if eq_pairs.is_empty() {
        let mut new_others = Vec::new();
        for part in others {
            let (common, rewritten) = try_extract_common_eq_from_or(arena, part);
            eq_pairs.extend(common);
            if let Some(r) = rewritten {
                new_others.push(r);
            }
        }
        others = new_others;
    }

    let remaining = scalar_expr::combine_conjuncts(arena, others);
    (eq_pairs, remaining)
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
        } => {
            collect_conjuncts(arena, *left, eq_pairs, others);
            collect_conjuncts(arena, *right, eq_pairs, others);
        }
        ScalarNode::BinaryOp { left, op, right } if matches!(op, BinOp::Eq | BinOp::EqForNull) => {
            // Treat as equi-join key when BOTH sides reference at least
            // one column. A side that's purely literal/constant
            // (`col = 2002`) is a filter, not an equi-key; let it fall
            // through to `others` so the optimizer pushes it down to a
            // scan filter. Expression keys (`-tt1.c_int = tt2.c_int`,
            // `lower(a.x) = b.x`, …) are valid join keys — the
            // exec-layer hash join hashes the lowered expression, and
            // `orient_eq_pair` below collects column refs from each
            // side to determine left/right orientation.
            let left_has_col = expr_has_column_ref(arena, *left);
            let right_has_col = expr_has_column_ref(arena, *right);
            if left_has_col && right_has_col {
                eq_pairs.push(ScalarHashJoinEqCondition {
                    left: *left,
                    right: *right,
                    null_safe: matches!(op, BinOp::EqForNull),
                });
            } else {
                others.push(expr);
            }
        }
        _ => {
            others.push(expr);
        }
    }
}

/// Split a top-level OR expression into its disjuncts.
/// Check if two eq pairs are structurally equal (possibly with swapped sides).
fn eq_pair_matches(a: &ScalarHashJoinEqCondition, b: &ScalarHashJoinEqCondition) -> bool {
    a.null_safe == b.null_safe
        && ((a.left == b.left && a.right == b.right) || (a.left == b.right && a.right == b.left))
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
) -> (Vec<ScalarHashJoinEqCondition>, Option<ScalarId>) {
    let mut branches = Vec::new();
    scalar_expr::split_disjuncts(arena, expr, &mut branches);
    if branches.len() < 2 {
        return (vec![], Some(expr));
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
        return (vec![], Some(expr));
    }

    // Rewrite each branch: remove the common eq pairs, recombine.
    let mut rewritten_branches = Vec::new();
    for (eqs, others) in branch_eqs.iter().zip(branch_others.iter()) {
        let mut remaining_parts: Vec<ScalarId> = others.clone();
        for eq in eqs {
            if !common.iter().any(|c| eq_pair_matches(c, eq)) {
                // Keep non-common eq pairs as regular conjuncts.
                remaining_parts.push(eq_condition_to_expr(arena, eq.clone()));
            }
        }
        if let Some(branch_expr) = scalar_expr::combine_conjuncts(arena, remaining_parts) {
            rewritten_branches.push(branch_expr);
        }
        // If a branch becomes empty (only common eqs), skip it — it
        // effectively becomes TRUE, making the whole OR always true for
        // matched eq keys.  We represent this by omitting the branch.
    }

    let rewritten = if rewritten_branches.len() == branches.len() {
        scalar_expr::combine_disjuncts(arena, rewritten_branches)
    } else {
        // Some branches were pure eq-only; the entire OR condition is
        // satisfied whenever the common equalities hold.
        None
    };

    (common, rewritten)
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
            op: Operator::PhysicalScan(ScanOp {
                database: op.database.clone(),
                table: op.table.clone(),
                alias: op.alias.clone(),
                columns: op.columns.clone(),
                predicates: op.predicates.clone(),
                required_columns: op.required_columns.clone(),
                // Propagated from the logical scan. Populated by Task 7
                // `LowCardinalityDictionaryRewrite`; empty otherwise.
                dict_columns: op.dict_columns.clone(),
                // Propagated from the logical scan. Populated by
                // `VariantPathPushdown`; empty otherwise.
                variant_columns: op.variant_columns.clone(),
                // Propagated from the logical scan so that MvRewrite-injected
                // scans carry the annotation through to the physical plan.
                mv_rewritten_from: op.mv_rewritten_from.clone(),
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
            op: Operator::PhysicalFilter(FilterOp {
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
            op: Operator::PhysicalProject(ProjectOp {
                items: op.items.clone(),
                output_qualifier: op.output_qualifier.clone(),
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
        let (raw_eq_conds, mut other) =
            extract_eq_conditions(op.condition, &op.join_type, &mut memo.scalars);

        // Orient eq_conditions so that pair.0 references the left child's
        // columns and pair.1 references the right child's columns.  Pairs
        // that reference only one side (e.g., inner predicates in a SEMI
        // JOIN condition) are demoted into other_condition.
        let mut eq_conds = Vec::new();
        if expr.children.len() == 2 {
            let left_ids = get_group_column_ids(memo, expr.children[0]);
            let right_ids = get_group_column_ids(memo, expr.children[1]);
            for pair in raw_eq_conds {
                let a = pair.left;
                let b = pair.right;
                let null_safe = pair.null_safe;
                match orient_eq_pair(pair, &memo.scalars, &left_ids, &right_ids) {
                    Some(oriented) => {
                        if let Some(coerced) =
                            coerce_hash_join_eq_condition(&memo.scalars, oriented.clone())
                        {
                            eq_conds.push(coerced);
                        } else {
                            let residual = eq_condition_to_expr(&mut memo.scalars, oriented);
                            append_residual_condition(&mut memo.scalars, &mut other, residual);
                        }
                    }
                    None => {
                        let residual = eq_condition_to_expr(
                            &mut memo.scalars,
                            ScalarHashJoinEqCondition {
                                left: a,
                                right: b,
                                null_safe,
                            },
                        );
                        append_residual_condition(&mut memo.scalars, &mut other, residual);
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
        let eq_conditions = eq_conds
            .into_iter()
            .map(|eq| PhysicalHashJoinEqCondition {
                left: eq.left,
                right: eq.right,
                null_safe: eq.null_safe,
            })
            .collect();
        let other_condition = other;
        vec![NewExpr {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: op.join_type,
                eq_conditions,
                other_condition,
                distribution: JoinDistribution::Unknown,
            }),
            children: expr.children.clone(),
        }]
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
    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalJoin(op) = &expr.op else {
            return vec![];
        };
        // NestLoop is used for cross joins or joins without equality
        // conditions. We must check feasibility after `orient_eq_pair`,
        // not just the raw equality count — a `=` whose two sides each
        // reference columns from BOTH children (e.g. CROSS JOIN with
        // `abs(t0.x + t1.y) = abs(t0.u + t1.v)`) cannot be used as a hash
        // key, so JoinToHashJoin will demote it and bail out with no
        // physical alternatives. Without this guard, the memo group has no
        // feasible implementation and the optimizer surfaces "no feasible
        // plan for group N".
        let (eq_conds, _) = extract_eq_conditions(op.condition, &op.join_type, &mut memo.scalars);
        if !eq_conds.is_empty() && op.join_type != JoinKind::Cross && expr.children.len() == 2 {
            let left_ids = get_group_column_ids(memo, expr.children[0]);
            let right_ids = get_group_column_ids(memo, expr.children[1]);
            let has_orientable_pair = eq_conds
                .iter()
                .filter_map(|p| orient_eq_pair(p.clone(), &memo.scalars, &left_ids, &right_ids))
                .any(|p| coerce_hash_join_eq_condition(&memo.scalars, p).is_some());
            if has_orientable_pair {
                // Has at least one usable equi-key — JoinToHashJoin handles this.
                return vec![];
            }
        } else if !eq_conds.is_empty() && op.join_type != JoinKind::Cross {
            // 1-child join (shouldn't happen for binary joins) — defer.
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
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalAggregate(op) = &expr.op else {
            return Vec::new();
        };
        vec![NewExpr {
            op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: op.stage.to_physical_mode(),
                group_by: op.group_by.clone(),
                aggregates: op.aggregates.clone(),
                output_columns: op.output_columns.clone(),
                is_merge: op.is_merge.clone(),
            }),
            children: expr.children.clone(),
        }]
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
            op: Operator::PhysicalSort(SortOp {
                items: op.items.clone(),
                // Propagate the analytic-partition tag through Logical→Physical
                // so the optimizer's required-distribution logic can see it.
                analytic_partition_exprs: op.analytic_partition_exprs.clone(),
                partition_limit: op.partition_limit,
                topn_type: op.topn_type,
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
            op: Operator::PhysicalLimit(LimitOp {
                limit: op.limit,
                offset: op.offset,
            }),
            children: expr.children.clone(),
        }]
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
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalAssertOneRow(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalAssertOneRow(AssertOneRowOp {
                subquery_text: op.subquery_text.clone(),
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
            op: Operator::PhysicalTopN(TopNOp {
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
#[cfg(test)]
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
#[cfg(test)]
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
            op: Operator::PhysicalWindow(WindowOp {
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
            op: Operator::PhysicalCTEAnchor(CTEAnchorOp { cte_id: op.cte_id }),
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
            op: Operator::PhysicalCTEProduce(CTEProduceOp {
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
            op: Operator::PhysicalCTEConsume(CTEConsumeOp {
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
            op: Operator::PhysicalUnion(UnionOp {
                all: op.all,
                output_columns: op.output_columns.clone(),
                child_output_columns: op.child_output_columns.clone(),
            }),
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
        let Operator::LogicalIntersect(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalIntersect(IntersectOp {
                output_columns: op.output_columns.clone(),
                child_output_columns: op.child_output_columns.clone(),
            }),
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
        let Operator::LogicalExcept(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalExcept(ExceptOp {
                output_columns: op.output_columns.clone(),
                child_output_columns: op.child_output_columns.clone(),
            }),
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
            op: Operator::PhysicalValues(ValuesOp {
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
            op: Operator::PhysicalGenerateSeries(GenerateSeriesOp {
                start: op.start,
                end: op.end,
                step: op.step,
                column_name: op.column_name.clone(),
                alias: op.alias.clone(),
                output_column_id: op.output_column_id,
            }),
            children: expr.children.clone(),
        }]
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
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalTableFunction(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalTableFunction(TableFunctionOp {
                function_name: op.function_name.clone(),
                args: op.args.clone(),
                output_columns: op.output_columns.clone(),
                alias: op.alias.clone(),
                is_left_join: op.is_left_join,
            }),
            children: expr.children.clone(),
        }]
    }
}

// ---------------------------------------------------------------------------
// 19. DecodeToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct DecodeToPhysical;

impl Rule for DecodeToPhysical {
    fn name(&self) -> &str {
        "DecodeToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalDecode(_))
    }
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalDecode(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalDecode(DecodeOp {
                mappings: op.mappings.clone(),
                output_columns: op.output_columns.clone(),
            }),
            children: expr.children.clone(),
        }]
    }
}

// ---------------------------------------------------------------------------
// 20. AggregateStateMergeToPhysical
// ---------------------------------------------------------------------------

pub(crate) struct AggregateStateMergeToPhysical;

impl Rule for AggregateStateMergeToPhysical {
    fn name(&self) -> &str {
        "AggregateStateMergeToPhysical"
    }
    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }
    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalAggregateStateMerge(_))
    }
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalAggregateStateMerge(op) = &expr.op else {
            return vec![];
        };
        vec![NewExpr {
            op: Operator::PhysicalAggregateStateMerge(op.clone()),
            children: expr.children.clone(),
        }]
    }
}

#[cfg(test)]
mod decode_tests {
    use super::*;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::memo::{MExpr, Memo};
    use crate::sql::optimizer::operator::{DecodeOp, ValuesOp};
    use crate::sql::planner::plan::DecodeMapping;
    use arrow::datatypes::DataType;

    #[test]
    fn decode_to_physical_emits_physical_decode_with_same_mappings() {
        let mut memo = Memo::new();
        // Dummy child group so the rule has a valid child slot to forward.
        let child_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        };
        let child_group = memo.new_group(child_mexpr);

        let mappings = vec![DecodeMapping {
            source_column_id: ColumnId::new_for_test(1),
            output_column_id: ColumnId::new_for_test(2),
            dict_column: "a".into(),
            string_column: "a_str".into(),
        }];
        let logical_decode = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalDecode(DecodeOp {
                mappings: mappings.clone(),
                output_columns: vec![],
            }),
            children: vec![child_group],
        };

        let rule = DecodeToPhysical;
        assert!(rule.matches(&logical_decode.op));
        let out = rule.apply(&logical_decode, &mut memo);

        assert_eq!(out.len(), 1);
        match &out[0].op {
            Operator::PhysicalDecode(p) => assert_eq!(p.mappings, mappings),
            other => panic!("expected PhysicalDecode, got {:?}", other),
        }
        assert_eq!(out[0].children, vec![child_group]);
    }

    #[test]
    fn decode_to_physical_preserves_output_columns() {
        let mut memo = Memo::new();
        let child_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        };
        let child_group = memo.new_group(child_mexpr);

        let mappings = vec![DecodeMapping {
            source_column_id: ColumnId::new_for_test(1),
            output_column_id: ColumnId::new_for_test(2),
            dict_column: "dict_col".into(),
            string_column: "string_col".into(),
        }];
        // Logical output_columns reflects the post-rename names — i.e.
        // string_col, not dict_col. The physical operator must surface
        // the same set verbatim.
        let logical_outputs = vec![OutputColumn {
            column_id: ColumnId::UNSET,
            name: "string_col".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            is_internal: false,
        }];
        let logical_decode = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalDecode(DecodeOp {
                mappings,
                output_columns: logical_outputs.clone(),
            }),
            children: vec![child_group],
        };

        let out = DecodeToPhysical.apply(&logical_decode, &mut memo);
        assert_eq!(out.len(), 1);
        let Operator::PhysicalDecode(p) = &out[0].op else {
            panic!("expected PhysicalDecode");
        };
        assert_eq!(p.output_columns.len(), logical_outputs.len());
        assert_eq!(p.output_columns[0].name, logical_outputs[0].name);
        assert_eq!(p.output_columns[0].column_id, logical_outputs[0].column_id);
        assert_eq!(p.output_columns[0].data_type, logical_outputs[0].data_type);
        assert_eq!(p.output_columns[0].nullable, logical_outputs[0].nullable);
    }
}

#[cfg(test)]
mod top_n_tests {
    use super::*;
    use crate::sql::optimizer::memo::{MExpr, Memo};
    use crate::sql::optimizer::operator::{GenerateSeriesOp, TopNOp, TopNPhase};

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

        let out = GenerateSeriesToPhysical.apply(&expr, &mut memo);
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
mod eq_pair_tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::scalar::{ScalarArena, ScalarId, intern_typed, materialize};
    use arrow::datatypes::DataType;

    fn col_id(name: &str, id: u32) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(id),
                qualifier: None,
                column: name.into(),
            },
            data_type: DataType::Int32,
            nullable: false,
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
        ScalarHashJoinEqCondition {
            left: intern_typed(arena, &left),
            right: intern_typed(arena, &right),
            null_safe: false,
        }
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
mod join_demotion_tests {
    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, JoinKind, OutputColumn, TypedExpr};
    use crate::sql::catalog::{ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::memo::{LogicalProperties, MExpr, Memo};
    use crate::sql::optimizer::operator::{LogicalJoinOp, ScanOp};
    use crate::sql::optimizer::scalar::{ScalarId, intern_typed, materialize};
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    fn test_col_id(name: &str) -> ColumnId {
        match name {
            "a_id" => ColumnId::new_for_test(10),
            "a_name" => ColumnId::new_for_test(11),
            "a_arr" => ColumnId::new_for_test(12),
            "b_id" => ColumnId::new_for_test(20),
            "b_arr" => ColumnId::new_for_test(21),
            _ => ColumnId::new_for_test(100),
        }
    }

    fn col(name: &str) -> TypedExpr {
        col_typed(name, DataType::Int32)
    }

    fn col_typed(name: &str, data_type: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: test_col_id(name),
                qualifier: None,
                column: name.into(),
            },
            data_type,
            nullable: false,
        }
    }

    fn list_type(item_type: DataType) -> DataType {
        DataType::List(Arc::new(Field::new("item", item_type, true)))
    }

    /// Create a scan group whose logical_props report the given output columns.
    fn mk_scan_group(memo: &mut Memo, col_names: &[&str]) -> usize {
        let output_columns: Vec<OutputColumn> = col_names
            .iter()
            .map(|name| OutputColumn {
                column_id: test_col_id(name),
                name: (*name).into(),
                data_type: DataType::Int32,
                nullable: false,
                is_internal: false,
            })
            .collect();
        let scan_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalScan(ScanOp {
                database: "db".into(),
                table: TableDef {
                    name: "t".into(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                },
                alias: None,
                columns: output_columns.clone(),
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: vec![],
        };
        let gid = memo.new_group(scan_mexpr);
        // Inject logical_props so side ownership can be derived from ColumnId.
        memo.groups[gid].logical_props = Some(LogicalProperties::new(output_columns, 100.0));
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

    fn logical_join_expr(
        memo: &mut Memo,
        join_type: JoinKind,
        condition: TypedExpr,
        children: Vec<GroupId>,
    ) -> MExpr {
        let id = memo.next_expr_id();
        let condition = Some(intern_typed(&mut memo.scalars, &condition));
        MExpr {
            id,
            op: Operator::LogicalJoin(LogicalJoinOp {
                join_type,
                condition,
            }),
            children,
        }
    }

    fn mat(memo: &Memo, expr: ScalarId) -> TypedExpr {
        materialize(&memo.scalars, expr)
    }

    fn assert_col_id(expr: &TypedExpr, expected: ColumnId, message: &str) {
        match &expr.kind {
            ExprKind::ColumnRef { column_id, .. } => assert_eq!(*column_id, expected, "{message}"),
            other => panic!("expected ColumnRef for {message}, got {:?}", other),
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

        let join_mexpr = logical_join_expr(
            &mut memo,
            JoinKind::Inner,
            condition,
            vec![left_group, right_group],
        );

        let rule = JoinToHashJoin;
        let alternatives = rule.apply(&join_mexpr, &mut memo);

        assert_eq!(alternatives.len(), 1);

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
        let lhs = mat(&memo, eq.left);
        let rhs = mat(&memo, eq.right);
        assert_col_id(
            &lhs,
            test_col_id("a_id"),
            "left side of eq_condition should be a_id",
        );
        assert_col_id(
            &rhs,
            test_col_id("b_id"),
            "right side of eq_condition should be b_id",
        );

        // ── other_condition: the demoted (a_id = a_name) pair ───────────────
        let other = phys
            .other_condition
            .as_ref()
            .expect("demoted same-side pair must appear in other_condition");
        let other = mat(&memo, *other);
        match &other.kind {
            ExprKind::BinaryOp { left, op, right } => {
                assert!(
                    matches!(op, BinOp::Eq),
                    "demoted condition should be BinaryOp::Eq, got {:?}",
                    op
                );
                match (&left.kind, &right.kind) {
                    (
                        ExprKind::ColumnRef { column_id: l, .. },
                        ExprKind::ColumnRef { column_id: r, .. },
                    ) => {
                        let a_id = test_col_id("a_id");
                        let a_name = test_col_id("a_name");
                        assert!(
                            (*l == a_id && *r == a_name) || (*l == a_name && *r == a_id),
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
        let join_mexpr = logical_join_expr(
            &mut memo,
            JoinKind::Inner,
            condition,
            vec![left_group, right_group],
        );

        let rule = JoinToHashJoin;
        let alternatives = rule.apply(&join_mexpr, &mut memo);
        assert_eq!(alternatives.len(), 1);
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

    #[test]
    fn mixed_integer_eq_pair_keeps_raw_hash_key_for_distribution() {
        let mut memo = Memo::new();
        let left_group = mk_scan_group(&mut memo, &["a_id"]);
        let right_group = mk_scan_group(&mut memo, &["b_id"]);

        let condition = bin(
            col_typed("a_id", DataType::Int64),
            BinOp::Eq,
            col_typed("b_id", DataType::Int32),
        );
        let join_mexpr = logical_join_expr(
            &mut memo,
            JoinKind::Inner,
            condition,
            vec![left_group, right_group],
        );

        let alternatives = JoinToHashJoin.apply(&join_mexpr, &mut memo);
        assert_eq!(
            alternatives.len(),
            1,
            "mixed integer equality should remain hash-joinable"
        );
        let Operator::PhysicalHashJoin(phys) = &alternatives[0].op else {
            panic!("expected PhysicalHashJoin, got {:?}", alternatives[0].op);
        };
        assert_eq!(phys.eq_conditions.len(), 1);
        let left_key = mat(&memo, phys.eq_conditions[0].left);
        let right_key = mat(&memo, phys.eq_conditions[0].right);
        assert_eq!(left_key.data_type, DataType::Int64);
        assert_eq!(right_key.data_type, DataType::Int32);
        assert!(
            matches!(left_key.kind, ExprKind::ColumnRef { .. }),
            "optimizer hash key should keep raw column refs so distribution can enforce both sides"
        );
        assert!(
            matches!(right_key.kind, ExprKind::ColumnRef { .. }),
            "optimizer hash key should keep raw column refs so distribution can enforce both sides"
        );
        assert!(
            phys.other_condition.is_none(),
            "compatible scalar equality should not be demoted"
        );
    }

    #[test]
    fn cross_type_array_pair_has_no_hash_join_alternative() {
        let mut memo = Memo::new();
        let left_group = mk_scan_group(&mut memo, &["a_arr"]);
        let right_group = mk_scan_group(&mut memo, &["b_arr"]);
        let condition = bin(
            col_typed("a_arr", list_type(DataType::Utf8)),
            BinOp::Eq,
            col_typed("b_arr", list_type(DataType::Int64)),
        );
        let join_mexpr = logical_join_expr(
            &mut memo,
            JoinKind::Inner,
            condition,
            vec![left_group, right_group],
        );

        let hash_alternatives = JoinToHashJoin.apply(&join_mexpr, &mut memo);
        assert!(
            hash_alternatives.is_empty(),
            "cross-type complex equality must not become a raw hash key"
        );

        let nested_alternatives = JoinToNestLoop.apply(&join_mexpr, &mut memo);
        assert_eq!(
            nested_alternatives.len(),
            1,
            "nested loop must remain available for complex-only equality"
        );
        assert!(matches!(
            nested_alternatives[0].op,
            Operator::PhysicalNestLoopJoin(_)
        ));
    }

    #[test]
    fn complex_pair_demotes_when_scalar_hash_key_remains() {
        let mut memo = Memo::new();
        let left_group = mk_scan_group(&mut memo, &["a_id", "a_arr"]);
        let right_group = mk_scan_group(&mut memo, &["b_id", "b_arr"]);
        let scalar_eq = bin(col("a_id"), BinOp::Eq, col("b_id"));
        let complex_eq = bin(
            col_typed("a_arr", list_type(DataType::Utf8)),
            BinOp::Eq,
            col_typed("b_arr", list_type(DataType::Utf8)),
        );
        let condition = bin(scalar_eq, BinOp::And, complex_eq);
        let join_mexpr = logical_join_expr(
            &mut memo,
            JoinKind::Inner,
            condition,
            vec![left_group, right_group],
        );

        let alternatives = JoinToHashJoin.apply(&join_mexpr, &mut memo);
        assert_eq!(alternatives.len(), 1);
        let Operator::PhysicalHashJoin(phys) = &alternatives[0].op else {
            panic!("expected PhysicalHashJoin, got {:?}", alternatives[0].op);
        };
        assert_eq!(
            phys.eq_conditions.len(),
            1,
            "only the scalar equality should remain as a hash key"
        );
        let left_key = mat(&memo, phys.eq_conditions[0].left);
        assert_col_id(&left_key, test_col_id("a_id"), "scalar hash key on left");
        assert!(
            phys.other_condition.is_some(),
            "complex equality must survive as a residual predicate"
        );
    }

    #[test]
    fn join_to_hash_join_emits_one_property_driven_hash_join() {
        let mut memo = Memo::new();
        let left_group = mk_scan_group(&mut memo, &["a_id"]);
        let right_group = mk_scan_group(&mut memo, &["b_id"]);
        let condition = bin(col("a_id"), BinOp::Eq, col("b_id"));
        let expr = logical_join_expr(
            &mut memo,
            JoinKind::Inner,
            condition,
            vec![left_group, right_group],
        );
        let rule = JoinToHashJoin;
        let alternatives = rule.apply(&expr, &mut memo);

        assert_eq!(alternatives.len(), 1);
        let Operator::PhysicalHashJoin(phys) = &alternatives[0].op else {
            panic!("expected PhysicalHashJoin, got {:?}", alternatives[0].op);
        };
        assert!(matches!(phys.distribution, JoinDistribution::Unknown));
    }
}

#[cfg(test)]
mod window_split_tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::scalar_bridge::intern_window_exprs;
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
            output_column_id: ColumnId::UNSET,
            ignore_nulls: false,
        }
    }

    fn col(name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::UNSET,
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
            output_column_id: ColumnId::UNSET,
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
            op: Operator::LogicalValues(ValuesOp {
                rows: vec![],
                columns: vec![],
            }),
            children: vec![],
        };
        let child_group = memo.new_group(values_mexpr);

        // Single window with no partition and no order => single group, no sort.
        let window_exprs = intern_window_exprs(&mut memo.scalars, &[mk_window_expr("w1", vec![])]);
        let logical_window_mexpr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalWindow(WindowOp {
                window_exprs,
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
    use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::memo::{MExpr, Memo};
    use crate::sql::optimizer::scalar_bridge::{intern_aggregate_calls, intern_exprs};
    use crate::sql::planner::plan::AggregateCall;
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
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    fn output_column(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.into(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    fn count_call(arg: &str, distinct: bool) -> AggregateCall {
        AggregateCall {
            name: "count".into(),
            args: vec![col(arg)],
            distinct,
            result_type: DataType::Int64,
            order_by: vec![],
            output_column_id: ColumnId::UNSET,
        }
    }

    fn single_agg(
        memo: &mut Memo,
        group_by: Vec<TypedExpr>,
        aggregates: Vec<AggregateCall>,
        output_columns: Vec<OutputColumn>,
    ) -> LogicalAggregateOp {
        let group_by = intern_exprs(&mut memo.scalars, &group_by);
        let aggregates = intern_aggregate_calls(&mut memo.scalars, &aggregates);
        LogicalAggregateOp::single(group_by, aggregates, output_columns)
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
        let group_by = intern_exprs(&mut memo.scalars, &group_by);
        let aggregates = intern_aggregate_calls(&mut memo.scalars, &aggregates);
        LogicalAggregateOp::staged(
            stage,
            group_by,
            aggregates,
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

        let out = AggToHashAgg.apply(&expr, &mut memo);
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
        let local_out = AggToHashAgg.apply(&local_expr, &mut memo);
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
        let global_out = AggToHashAgg.apply(&global_expr, &mut memo);
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
                    args: vec![col("id")],
                    distinct: true,
                    result_type: DataType::Int64,
                    order_by: vec![],
                    output_column_id: ColumnId::UNSET,
                }],
                vec![
                    OutputColumn {
                        column_id: test_col_id("city"),
                        name: "city".into(),
                        data_type: DataType::Int32,
                        nullable: false,
                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: ColumnId::new_for_test(6),
                        name: "count(distinct id)".into(),
                        data_type: DataType::Int64,
                        nullable: true,
                        is_internal: false,
                    },
                ],
            )),
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
}
