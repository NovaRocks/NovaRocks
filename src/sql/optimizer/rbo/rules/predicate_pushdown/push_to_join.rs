//! PushDownPredicateJoin — `Filter(Join)` rewrite.
//!
//! Classifies conjuncts of the filter predicate by which side of the join
//! they reference, pushes single-side predicates below the join (respecting
//! OUTER/SEMI/ANTI null-preservation), and merges genuine cross-side
//! conjuncts into the join condition. Also performs single-step OR-factoring
//! to extract common equi-joins from OR branches. Upgrades a CROSS join to
//! INNER when a predicate promotes it.
//!
//! Mirrors legacy `push_predicates_through_join` + `factor_common_eq_from_or`
//! from `src/sql/optimizer/predicate_pushdown.rs`. One step per apply — the
//! driver's fixed-point handles repeated firing when a newly-formed shape
//! exposes further opportunities.

use std::collections::HashSet;

use super::super::super::rule::RewriteRule;
use crate::sql::analysis::{BinOp, ExprKind, JoinKind, LiteralValue, TypedExpr};
use crate::sql::optimizer::rbo::utils::{
    collect_column_refs, collect_output_columns, collect_qualified_column_refs,
    collect_qualified_output_columns, combine_and, split_and, wrap_remaining_filter,
};
use crate::sql::planner::plan::*;
use arrow::datatypes::DataType;

pub(crate) struct PushDownPredicateJoin;

impl RewriteRule for PushDownPredicateJoin {
    fn name(&self) -> &'static str {
        "PushDownPredicateJoin"
    }

    fn matches(&self, plan: &LogicalPlan) -> bool {
        matches!(
            plan,
            LogicalPlan::Filter(f) if matches!(*f.input, LogicalPlan::Join(_))
        )
    }

    fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan> {
        let LogicalPlan::Filter(filter) = plan else {
            return None;
        };
        let LogicalPlan::Join(join) = *filter.input else {
            return None;
        };
        let (rewritten, pushed_any) = push_predicates_through_join(filter.predicate, join);
        if pushed_any { Some(rewritten) } else { None }
    }
}

// ============================================================
// Port of legacy helpers from src/sql/optimizer/predicate_pushdown.rs
// ============================================================

fn push_predicates_through_join(predicate: TypedExpr, join: JoinNode) -> (LogicalPlan, bool) {
    let conjuncts = split_and(predicate);
    let left_cols = collect_output_columns(&join.left);
    let right_cols = collect_output_columns(&join.right);

    // Qualified output columns for precise self-join disambiguation.
    let left_qcols = collect_qualified_output_columns(&join.left);
    let right_qcols = collect_qualified_output_columns(&join.right);

    let mut left_preds = Vec::new();
    let mut right_preds = Vec::new();
    let mut join_preds = Vec::new();
    let mut remaining = Vec::new();

    // For LEFT joins (OUTER/SEMI/ANTI), subquery rewrites can produce a right
    // child whose output columns share names with the left child. A predicate
    // that references only left-child columns may look like "both-sides" due to
    // this name overlap. Detect this and treat such predicates as left-only.
    let is_left_join_variant = matches!(
        join.join_type,
        JoinKind::LeftOuter | JoinKind::LeftSemi | JoinKind::LeftAnti
    );

    for conj in conjuncts {
        let refs = collect_column_refs(&conj);
        let in_left = refs.iter().any(|c| left_cols.contains(&c.to_lowercase()));
        let in_right = refs.iter().any(|c| right_cols.contains(&c.to_lowercase()));

        match (in_left, in_right) {
            (true, false) => left_preds.push(conj),
            (false, true) => {
                // For LEFT OUTER / LEFT SEMI / LEFT ANTI / FULL OUTER joins,
                // right-side predicates affect NULL preservation semantics and
                // must NOT be pushed below the join.
                // For RIGHT OUTER, left-side predicates have the same issue
                // (handled below), but right-side predicates are safe to push.
                match join.join_type {
                    JoinKind::Inner
                    | JoinKind::Cross
                    | JoinKind::RightOuter
                    | JoinKind::RightSemi
                    | JoinKind::RightAnti => {
                        right_preds.push(conj);
                    }
                    _ => remaining.push(conj),
                }
            }
            (true, true) => {
                // Bare-name matching says "both sides". Re-check with qualified
                // column references to handle self-joins (e.g. nation n1, nation n2)
                // where both sides share the same bare column names.
                let qrefs = collect_qualified_column_refs(&conj);
                let q_in_left = qrefs.iter().any(|r| left_qcols.contains(r));
                let q_in_right = qrefs.iter().any(|r| right_qcols.contains(r));

                // If ALL qualified refs are present in left (and not exclusively
                // right), treat as left-only. Vice versa for right-only.
                // Only if qualified refs genuinely span both sides treat as
                // "both-sides".
                let all_in_left = qrefs.iter().all(|r| left_qcols.contains(r));
                let all_in_right = qrefs.iter().all(|r| right_qcols.contains(r));

                // Guard against false "left-only" or "right-only" classification
                // when unqualified refs are ambiguous (present in both sides).
                // E.g., `d_week_seq = __sq_2.d_week_seq` has refs:
                //   (None, "d_week_seq")  — in both left & right qcols
                //   (Some("__sq_2"), "d_week_seq") — only in right qcols
                // `all_in_right` would be true, but the unqualified ref is
                // genuinely a left-side column. Treat as join predicate.
                let any_ambiguous_in_both = qrefs
                    .iter()
                    .any(|r| left_qcols.contains(r) && right_qcols.contains(r));

                if all_in_left && !all_in_right && !any_ambiguous_in_both {
                    // Qualified analysis shows left-only
                    left_preds.push(conj);
                } else if all_in_right && !all_in_left && !any_ambiguous_in_both {
                    // Qualified analysis shows right-only
                    match join.join_type {
                        JoinKind::Inner
                        | JoinKind::Cross
                        | JoinKind::RightOuter
                        | JoinKind::RightSemi
                        | JoinKind::RightAnti => {
                            right_preds.push(conj);
                        }
                        _ => remaining.push(conj),
                    }
                } else if q_in_left && q_in_right {
                    // Genuinely references both sides.
                    //
                    // For self-joins (left and right share the same bare column
                    // names), keep the predicate as a remaining filter above the
                    // join.  Merging it into the join condition can cause the
                    // execution layer to mis-resolve ambiguous column references
                    // when both sides originate from the same table.
                    let is_self_join_overlap = {
                        let bare_refs: Vec<String> =
                            refs.iter().map(|c| c.to_lowercase()).collect();
                        bare_refs
                            .iter()
                            .all(|c| left_cols.contains(c) && right_cols.contains(c))
                    };
                    if is_self_join_overlap
                        && q_in_left
                        && q_in_right
                        && !is_left_join_variant
                        && matches!(join.join_type, JoinKind::Cross | JoinKind::Inner)
                    {
                        // Shared column names but qualified refs confirm both sides
                        // are referenced (e.g., CTE.d_week_seq = date_dim.d_week_seq).
                        // Only push for CROSS/INNER joins to enable CROSS → INNER
                        // upgrade. Do NOT push for SEMI/ANTI/OUTER which have
                        // different scope semantics.
                        join_preds.push(conj);
                    } else if is_self_join_overlap {
                        remaining.push(conj);
                    } else if is_left_join_variant
                        && refs.iter().all(|c| left_cols.contains(&c.to_lowercase()))
                    {
                        left_preds.push(conj);
                    } else if is_left_join_variant {
                        remaining.push(conj);
                    } else {
                        // For OR predicates, try to extract common equi-join
                        // conditions shared by all OR branches. This handles:
                        //   (cd_demo_sk=ss_cdemo_sk AND ...) OR (cd_demo_sk=ss_cdemo_sk AND ...)
                        // → factor out cd_demo_sk=ss_cdemo_sk as join_pred,
                        //   keep remaining OR as other condition.
                        let (factored, or_remaining) =
                            factor_common_eq_from_or(&conj, &left_cols, &right_cols);
                        if !factored.is_empty() {
                            join_preds.extend(factored);
                            if let Some(rem) = or_remaining {
                                remaining.push(rem);
                            }
                        } else {
                            join_preds.push(conj);
                        }
                    }
                } else {
                    // Fallback: keep above the join as remaining filter.
                    // This handles cases where qualified matching cannot
                    // disambiguate (e.g. mixed qualified/unqualified refs).
                    remaining.push(conj);
                }
            }
            (false, false) => {
                // Constant predicates — push to left side
                left_preds.push(conj);
            }
        }
    }

    // For RIGHT OUTER joins, left-side predicates cannot be pushed below
    // (left side is the nullable side). Move them to remaining.
    if matches!(
        join.join_type,
        JoinKind::RightOuter | JoinKind::RightSemi | JoinKind::RightAnti
    ) {
        remaining.append(&mut left_preds);
    }

    // For FULL OUTER joins, neither side can receive pushed predicates.
    if matches!(join.join_type, JoinKind::FullOuter) {
        remaining.append(&mut left_preds);
        remaining.append(&mut right_preds);
    }

    // Determine whether anything was actually pushed (after outer-join drain-back).
    let pushed_any = !left_preds.is_empty() || !right_preds.is_empty() || !join_preds.is_empty();

    // Build the new left child, applying pushed predicates then wrapping in
    // a Filter. The RBO driver's fixed-point handles continued pushdown on
    // subsequent iterations.
    let new_left = if left_preds.is_empty() {
        *join.left
    } else {
        let pushed = combine_and(left_preds);
        LogicalPlan::Filter(FilterNode {
            input: join.left,
            predicate: pushed,
        })
    };

    // Build the new right child.
    let new_right = if right_preds.is_empty() {
        *join.right
    } else {
        let pushed = combine_and(right_preds);
        LogicalPlan::Filter(FilterNode {
            input: join.right,
            predicate: pushed,
        })
    };

    // Merge new join predicates with the existing join condition.
    let new_condition = merge_join_conditions(join.condition, join_preds);

    // When a CROSS JOIN gets join predicates extracted from the filter above,
    // upgrade it to INNER JOIN so the physical emitter can use hash join.
    let new_join_type = if join.join_type == JoinKind::Cross && new_condition.is_some() {
        JoinKind::Inner
    } else {
        join.join_type
    };

    let new_join = LogicalPlan::Join(JoinNode {
        left: Box::new(new_left),
        right: Box::new(new_right),
        join_type: new_join_type,
        condition: new_condition,
    });

    (wrap_remaining_filter(new_join, remaining), pushed_any)
}

/// Extract common equi-join conditions from all branches of an OR predicate.
/// Returns (extracted_join_preds, remaining_or_predicate).
///
/// For: `(A=B AND X) OR (A=B AND Y)` where A is from left and B from right,
/// extracts `A=B` as a join predicate and returns `X OR Y` as remaining.
fn factor_common_eq_from_or(
    expr: &TypedExpr,
    left_cols: &HashSet<String>,
    right_cols: &HashSet<String>,
) -> (Vec<TypedExpr>, Option<TypedExpr>) {
    // Split OR into branches
    let branches = split_or_branches(expr);
    if branches.len() < 2 {
        return (vec![], None);
    }

    // Collect AND conjuncts per branch
    let branch_conjuncts: Vec<Vec<&TypedExpr>> =
        branches.iter().map(|b| split_and_refs(b)).collect();

    // Find equi-join predicates (col=col where one side left, one right)
    // that appear in ALL branches
    let mut common_eqs: Vec<TypedExpr> = Vec::new();
    if let Some(first) = branch_conjuncts.first() {
        for candidate in first {
            if !is_cross_side_eq(candidate, left_cols, right_cols) {
                continue;
            }
            let in_all = branch_conjuncts[1..]
                .iter()
                .all(|conjs| conjs.iter().any(|c| expr_eq(c, candidate)));
            if in_all {
                common_eqs.push((*candidate).clone());
            }
        }
    }

    if common_eqs.is_empty() {
        return (vec![], None);
    }

    // Build remaining OR: remove common eqs from each branch
    let mut new_branches: Vec<TypedExpr> = Vec::new();
    for branch in &branch_conjuncts {
        let remaining: Vec<TypedExpr> = branch
            .iter()
            .filter(|c| !common_eqs.iter().any(|eq| expr_eq(c, eq)))
            .map(|c| (*c).clone())
            .collect();
        if remaining.is_empty() {
            // Branch was only the common eq → TRUE
            new_branches.push(TypedExpr {
                data_type: DataType::Boolean,
                nullable: false,
                kind: ExprKind::Literal(LiteralValue::Bool(true)),
            });
        } else {
            new_branches.push(combine_and(remaining));
        }
    }

    let or_remaining = if new_branches
        .iter()
        .all(|b| matches!(b.kind, ExprKind::Literal(LiteralValue::Bool(true))))
    {
        None // All branches were just the common eq
    } else {
        let mut result = new_branches.remove(0);
        for branch in new_branches {
            result = TypedExpr {
                data_type: DataType::Boolean,
                nullable: false,
                kind: ExprKind::BinaryOp {
                    left: Box::new(result),
                    op: BinOp::Or,
                    right: Box::new(branch),
                },
            };
        }
        Some(result)
    };

    (common_eqs, or_remaining)
}

fn split_or_branches(expr: &TypedExpr) -> Vec<&TypedExpr> {
    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::Or,
            right,
        } => {
            let mut v = split_or_branches(left);
            v.extend(split_or_branches(right));
            v
        }
        ExprKind::Nested(inner) => split_or_branches(inner),
        _ => vec![expr],
    }
}

fn split_and_refs(expr: &TypedExpr) -> Vec<&TypedExpr> {
    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            let mut v = split_and_refs(left);
            v.extend(split_and_refs(right));
            v
        }
        ExprKind::Nested(inner) => split_and_refs(inner),
        _ => vec![expr],
    }
}

fn is_cross_side_eq(
    expr: &TypedExpr,
    left_cols: &HashSet<String>,
    right_cols: &HashSet<String>,
) -> bool {
    if let ExprKind::BinaryOp {
        left,
        op: BinOp::Eq,
        right,
    } = &expr.kind
    {
        let l_name = match &left.kind {
            ExprKind::ColumnRef { column, .. } => Some(column.to_lowercase()),
            _ => None,
        };
        let r_name = match &right.kind {
            ExprKind::ColumnRef { column, .. } => Some(column.to_lowercase()),
            _ => None,
        };
        match (l_name, r_name) {
            (Some(l), Some(r)) => {
                (left_cols.contains(&l) && right_cols.contains(&r))
                    || (left_cols.contains(&r) && right_cols.contains(&l))
            }
            _ => false,
        }
    } else {
        false
    }
}

fn expr_eq(a: &TypedExpr, b: &TypedExpr) -> bool {
    format!("{:?}", a.kind) == format!("{:?}", b.kind)
}

/// Merge new predicates into an existing (optional) join condition.
fn merge_join_conditions(
    existing: Option<TypedExpr>,
    new_preds: Vec<TypedExpr>,
) -> Option<TypedExpr> {
    let mut all = Vec::new();
    if let Some(cond) = existing {
        all.push(cond);
    }
    all.extend(new_preds);
    if all.is_empty() {
        None
    } else {
        Some(combine_and(all))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::sql::catalog::{ColumnDef, TableDef, TableStorage};
    use arrow::datatypes::DataType;

    fn col(name: &str) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Int64,
            nullable: true,
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: name.into(),
            },
        }
    }

    fn int_lit(v: i64) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Int64,
            nullable: false,
            kind: ExprKind::Literal(LiteralValue::Int(v)),
        }
    }

    fn eq(a: TypedExpr, b: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::BinaryOp {
                left: Box::new(a),
                op: BinOp::Eq,
                right: Box::new(b),
            },
        }
    }

    /// Build a scan for a named table with an alias (the alias is used by
    /// `collect_qualified_output_columns` when disambiguating self-joins).
    fn scan(table_name: &str, cols: &[&str]) -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "db".into(),
            table: TableDef {
                name: table_name.into(),
                columns: cols
                    .iter()
                    .map(|n| ColumnDef {
                        name: (*n).into(),
                        data_type: DataType::Int64,
                        nullable: true,
                    })
                    .collect(),
                iceberg_row_lineage_metadata_columns: vec![],
                storage: TableStorage::LocalParquetFile {
                    path: std::path::PathBuf::from("/tmp/t.parquet"),
                },
            },
            alias: Some(table_name.into()),
            columns: cols
                .iter()
                .map(|n| OutputColumn {
                    name: (*n).into(),
                    data_type: DataType::Int64,
                    nullable: true,
                })
                .collect(),
            predicates: vec![],
            required_columns: None,
        })
    }

    fn inner_join(
        left: LogicalPlan,
        right: LogicalPlan,
        condition: Option<TypedExpr>,
    ) -> LogicalPlan {
        LogicalPlan::Join(JoinNode {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinKind::Inner,
            condition,
        })
    }

    fn cross_join(left: LogicalPlan, right: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Join(JoinNode {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinKind::Cross,
            condition: None,
        })
    }

    // Test 1: t1 INNER t2 WHERE t1.x = 1
    // x belongs only to t1 → pushed below left child.
    // Expected: Join(Filter(t1), t2) with no remaining filter above.
    #[test]
    fn pushes_left_only_predicate_below_inner_join() {
        let t1 = scan("t1", &["x", "y"]);
        let t2 = scan("t2", &["a", "b"]);
        let join = inner_join(t1, t2, None);
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(join),
            predicate: eq(col("x"), int_lit(1)),
        });

        let rule = PushDownPredicateJoin;
        assert!(rule.matches(&filter));
        let out = rule.apply(filter).expect("should rewrite");

        // Expected shape: Join(Filter(t1), t2) — no outer Filter
        match out {
            LogicalPlan::Join(j) => {
                assert_eq!(j.join_type, JoinKind::Inner);
                match *j.left {
                    LogicalPlan::Filter(f) => match *f.input {
                        LogicalPlan::Scan(_) => {}
                        other => panic!("expected Scan under left Filter, got {:?}", other),
                    },
                    other => panic!("expected Filter on left child, got {:?}", other),
                }
                // Right child must be unmodified scan
                assert!(matches!(*j.right, LogicalPlan::Scan(_)));
            }
            other => panic!("expected bare Join at top, got {:?}", other),
        }
    }

    // Test 2: t1 INNER t2 WHERE t2.a = 1
    // a belongs only to t2 → pushed below right child.
    // Expected: Join(t1, Filter(t2)) with no remaining filter above.
    #[test]
    fn pushes_right_only_below_inner_join() {
        let t1 = scan("t1", &["x", "y"]);
        let t2 = scan("t2", &["a", "b"]);
        let join = inner_join(t1, t2, None);
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(join),
            predicate: eq(col("a"), int_lit(1)),
        });

        let rule = PushDownPredicateJoin;
        assert!(rule.matches(&filter));
        let out = rule.apply(filter).expect("should rewrite");

        match out {
            LogicalPlan::Join(j) => {
                assert_eq!(j.join_type, JoinKind::Inner);
                // Left child must be unmodified scan
                assert!(matches!(*j.left, LogicalPlan::Scan(_)));
                match *j.right {
                    LogicalPlan::Filter(f) => match *f.input {
                        LogicalPlan::Scan(_) => {}
                        other => panic!("expected Scan under right Filter, got {:?}", other),
                    },
                    other => panic!("expected Filter on right child, got {:?}", other),
                }
            }
            other => panic!("expected bare Join at top, got {:?}", other),
        }
    }

    // Test 3: CROSS(t1, t2) WHERE t1.x = t2.a
    // x is left-only, a is right-only → cross-side equi-join condition.
    // Expected: INNER join with condition (x=a), no outer Filter.
    #[test]
    fn merges_cross_side_predicate_into_join_condition() {
        let t1 = scan("t1", &["x", "y"]);
        let t2 = scan("t2", &["a", "b"]);
        let join = cross_join(t1, t2);
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(join),
            predicate: eq(col("x"), col("a")),
        });

        let rule = PushDownPredicateJoin;
        assert!(rule.matches(&filter));
        let out = rule.apply(filter).expect("should rewrite");

        // Expected: INNER join with condition — no outer Filter
        match out {
            LogicalPlan::Join(j) => {
                assert_eq!(
                    j.join_type,
                    JoinKind::Inner,
                    "CROSS should be upgraded to INNER"
                );
                assert!(j.condition.is_some(), "join condition must be set");
                // Children should be bare scans (no pushed filters)
                assert!(matches!(*j.left, LogicalPlan::Scan(_)));
                assert!(matches!(*j.right, LogicalPlan::Scan(_)));
            }
            other => panic!("expected bare Join at top, got {:?}", other),
        }
    }

    // Test 4: RIGHT OUTER JOIN(t1, t2) WHERE t1.x = 1
    // t1 is the left (nullable) side of a RIGHT OUTER join — predicates on
    // the nullable side must NOT be pushed below. The rule must either return
    // None (if the entire predicate ends up in `remaining` which reconstructs
    // the original shape) or keep the filter above the join.
    #[test]
    fn does_not_push_left_side_below_right_outer() {
        let t1 = scan("t1", &["x", "y"]);
        let t2 = scan("t2", &["a", "b"]);
        let join = LogicalPlan::Join(JoinNode {
            left: Box::new(t1),
            right: Box::new(t2),
            join_type: JoinKind::RightOuter,
            condition: None,
        });
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(join),
            predicate: eq(col("x"), int_lit(1)),
        });

        let rule = PushDownPredicateJoin;
        assert!(rule.matches(&filter));
        // The predicate references only the left (nullable) side — it cannot
        // be pushed, so the rule detects no change and returns None.
        let out = rule.apply(filter);
        assert!(
            out.is_none(),
            "left-side predicate must not be pushed below a RIGHT OUTER join; got {:?}",
            out
        );
    }
}
