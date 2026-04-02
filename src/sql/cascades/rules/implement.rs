//! Implementation rules: logical operator -> physical operator(s).
//!
//! Each struct implements the `Rule` trait. The `apply` method constructs the
//! physical variant of the matched logical operator, preserving child GroupIds.

use crate::sql::cascades::memo::{MExpr, Memo};
use crate::sql::cascades::operator::*;
use crate::sql::cascades::rule::{NewExpr, Rule, RuleType};
use crate::sql::ir::{BinOp, ExprKind, JoinKind, TypedExpr};

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
) -> (Vec<(TypedExpr, TypedExpr)>, Option<TypedExpr>) {
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
    eq_pairs: &mut Vec<(TypedExpr, TypedExpr)>,
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
        ExprKind::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => {
            eq_pairs.push((*left.clone(), *right.clone()));
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
fn eq_pair_matches(a: &(TypedExpr, TypedExpr), b: &(TypedExpr, TypedExpr)) -> bool {
    (typed_expr_eq(&a.0, &b.0) && typed_expr_eq(&a.1, &b.1))
        || (typed_expr_eq(&a.0, &b.1) && typed_expr_eq(&a.1, &b.0))
}

/// Try to extract common equality conditions from an OR expression.
///
/// Given `(A AND x=y AND B) OR (C AND x=y AND D)`, extracts `(x, y)` as
/// a common eq pair and rewrites the expression to `(A AND B) OR (C AND D)`.
///
/// Returns `(common_eq_pairs, rewritten_or_condition)`.
fn try_extract_common_eq_from_or(
    expr: &TypedExpr,
) -> (Vec<(TypedExpr, TypedExpr)>, Option<TypedExpr>) {
    let branches = split_or(expr);
    if branches.len() < 2 {
        return (vec![], Some(expr.clone()));
    }

    // For each branch, extract eq pairs and residual.
    let mut branch_eqs: Vec<Vec<(TypedExpr, TypedExpr)>> = Vec::new();
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
    let mut common: Vec<(TypedExpr, TypedExpr)> = Vec::new();
    for eq in first_eqs {
        if branch_eqs[1..].iter().all(|branch| branch.iter().any(|b| eq_pair_matches(eq, b))) {
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
                    data_type: arrow::datatypes::DataType::Boolean,
                    nullable: eq.0.nullable || eq.1.nullable,
                    kind: ExprKind::BinaryOp {
                        left: Box::new(eq.0.clone()),
                        op: BinOp::Eq,
                        right: Box::new(eq.1.clone()),
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
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalJoin(op) = &expr.op else {
            return vec![];
        };
        let (eq_conds, other) = extract_eq_conditions(&op.condition, &op.join_type);
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
            }),
            children: expr.children.clone(),
        };

        // Two-phase Local+Global aggregation is deferred — the Global
        // aggregate's input expressions must reference the Local output
        // columns (e.g., `sum(sum(x))`), which requires expression
        // rewriting not yet implemented.  Single-phase only for now.
        vec![single]
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
    fn apply(&self, expr: &MExpr, _memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalLimit(op) = &expr.op else {
            return vec![];
        };
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
// 9. WindowToPhysical
// ---------------------------------------------------------------------------

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
        vec![NewExpr {
            op: Operator::PhysicalWindow(PhysicalWindowOp {
                window_exprs: op.window_exprs.clone(),
                output_columns: op.output_columns.clone(),
            }),
            children: expr.children.clone(),
        }]
    }
}

// ---------------------------------------------------------------------------
// 10. CTEProduceToPhysical
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
// 11. CTEConsumeToPhysical
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
// 12. RepeatToPhysical
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
                grouping_fn_args: op.grouping_fn_args.clone(),
            }),
            children: expr.children.clone(),
        }]
    }
}

// ---------------------------------------------------------------------------
// 13. UnionToPhysical
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
// 14. IntersectToPhysical
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
