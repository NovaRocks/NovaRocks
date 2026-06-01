//! OQ-2: derive `IS NOT NULL` predicates on equi-join keys for null-rejecting
//! join types (Inner / LeftSemi / RightSemi), mirroring StarRocks
//! `JoinPredicatePushdown.deriveIsNotNullPredicate`. The derived `Filter` is
//! pushed to the scan by the existing PredicatePushdownPostJoin pushdown rules
//! running in the same fixed-point loop.

use std::collections::HashSet;

use arrow::datatypes::DataType;

use crate::sql::analysis::{ExprKind, JoinKind, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::rule::PlanRewriteRule;
use crate::sql::optimizer::rewrite::rules::utils::{combine_and, join_equi_keys, split_and};
use crate::sql::planner::plan::*;

pub(crate) struct DeriveJoinNotNullPredicate;

/// `(derive_left, derive_right)`: which sides' equi-keys may receive IS NOT NULL.
/// Mirrors StarRocks: inner -> both; left-semi -> right; right-semi -> left.
/// Anti / null-aware-anti / outer / cross -> neither (see spec §3.1).
fn safe_sides(join_type: JoinKind) -> (bool, bool) {
    match join_type {
        JoinKind::Inner => (true, true),
        JoinKind::LeftSemi => (false, true),
        JoinKind::RightSemi => (true, false),
        JoinKind::LeftAnti
        | JoinKind::RightAnti
        | JoinKind::NullAwareLeftAnti
        | JoinKind::LeftOuter
        | JoinKind::RightOuter
        | JoinKind::FullOuter
        | JoinKind::Cross => (false, false),
    }
}

impl PlanRewriteRule for DeriveJoinNotNullPredicate {
    fn name(&self) -> &'static str {
        "DeriveJoinNotNullPredicate"
    }

    fn matches(&self, plan: &LogicalPlan) -> bool {
        matches!(plan, LogicalPlan::Join(_))
    }

    fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan> {
        let LogicalPlan::Join(join) = plan else {
            return None;
        };
        let (derive_left, derive_right) = safe_sides(join.join_type);
        if !derive_left && !derive_right {
            return None;
        }
        let keys = join_equi_keys(&join);
        if keys.is_empty() {
            return None;
        }

        let left_preds = if derive_left {
            eligible_not_null(&join.left, keys.iter().map(|k| &k.left))
        } else {
            Vec::new()
        };
        let right_preds = if derive_right {
            eligible_not_null(&join.right, keys.iter().map(|k| &k.right))
        } else {
            Vec::new()
        };
        if left_preds.is_empty() && right_preds.is_empty() {
            return None;
        }

        let JoinNode {
            left,
            right,
            join_type,
            condition,
            required_output_columns,
        } = join;
        Some(LogicalPlan::Join(JoinNode {
            left: Box::new(wrap_not_null(*left, left_preds)),
            right: Box::new(wrap_not_null(*right, right_preds)),
            join_type,
            condition,
            required_output_columns,
        }))
    }
}

/// For each candidate key operand (a ColumnRef from `child`), build the
/// `IS NOT NULL` predicates to add: keep only operands that are (a) nullable and
/// (b) not already guaranteed non-null by `child`'s predicate spine. Dedupe by
/// column identity within the side.
fn eligible_not_null<'a>(
    child: &LogicalPlan,
    operands: impl Iterator<Item = &'a TypedExpr>,
) -> Vec<TypedExpr> {
    let (guaranteed_ids, guaranteed_names) = spine_not_null(child);
    let mut seen_ids: HashSet<ColumnId> = HashSet::new();
    let mut seen_names: HashSet<String> = HashSet::new();
    let mut preds = Vec::new();
    for operand in operands {
        if !operand.nullable {
            continue;
        }
        // Keys from join_equi_keys are always bare ColumnRef (Cast/Nested
        // already peeled); this guard is defensive.
        let ExprKind::ColumnRef {
            column_id, column, ..
        } = &operand.kind
        else {
            continue;
        };
        let name = column.to_lowercase();
        if (*column_id != ColumnId::UNSET && guaranteed_ids.contains(column_id))
            || guaranteed_names.contains(&name)
        {
            continue; // already guaranteed non-null -> idempotency
        }
        let fresh = if *column_id != ColumnId::UNSET {
            seen_ids.insert(*column_id)
        } else {
            seen_names.insert(name.clone())
        };
        if fresh {
            preds.push(is_not_null(operand.clone()));
        }
    }
    preds
}

fn is_not_null(operand: TypedExpr) -> TypedExpr {
    TypedExpr {
        data_type: DataType::Boolean,
        nullable: false,
        kind: ExprKind::IsNull {
            expr: Box::new(operand),
            negated: true,
        },
    }
}

fn wrap_not_null(child: LogicalPlan, preds: Vec<TypedExpr>) -> LogicalPlan {
    if preds.is_empty() {
        return child;
    }
    LogicalPlan::Filter(FilterNode {
        input: Box::new(child),
        predicate: combine_and(preds),
        required_output_columns: None,
    })
}

/// Walk `plan`'s predicate spine (passthrough single-input nodes down to the
/// root scan) collecting column identities already guaranteed non-null by an
/// `IS NOT NULL` conjunct. Used for idempotency / redundant-filter avoidance.
fn spine_not_null(plan: &LogicalPlan) -> (HashSet<ColumnId>, HashSet<String>) {
    let mut ids = HashSet::new();
    let mut names = HashSet::new();
    spine_not_null_inner(plan, &mut ids, &mut names);
    (ids, names)
}

fn spine_not_null_inner(
    plan: &LogicalPlan,
    ids: &mut HashSet<ColumnId>,
    names: &mut HashSet<String>,
) {
    match plan {
        LogicalPlan::Filter(f) => {
            for conj in split_and(f.predicate.clone()) {
                record_not_null(&conj, ids, names);
            }
            spine_not_null_inner(&f.input, ids, names);
        }
        LogicalPlan::Scan(s) => {
            for p in &s.predicates {
                record_not_null(p, ids, names);
            }
        }
        // Project may rename columns; descending is intentionally conservative.
        // A post-Project name match can only cause a (safe) skip of a possibly
        // redundant filter — never an incorrectly-omitted one.
        LogicalPlan::Project(p) => spine_not_null_inner(&p.input, ids, names),
        LogicalPlan::Sort(s) => spine_not_null_inner(&s.input, ids, names),
        LogicalPlan::Limit(l) => spine_not_null_inner(&l.input, ids, names),
        _ => {}
    }
}

fn record_not_null(expr: &TypedExpr, ids: &mut HashSet<ColumnId>, names: &mut HashSet<String>) {
    if let ExprKind::IsNull {
        expr: inner,
        negated: true,
    } = &expr.kind
    {
        if let ExprKind::ColumnRef {
            column_id, column, ..
        } = &inner.kind
        {
            if *column_id != ColumnId::UNSET {
                ids.insert(*column_id);
            }
            names.insert(column.to_lowercase());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{BinOp, OutputColumn};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};

    fn scan(alias: &str, table: &str, cols: &[(&str, u32, bool)]) -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "default".to_string(),
            table: TableDef {
                name: table.to_string(),
                columns: cols
                    .iter()
                    .map(|(name, _, nullable)| ColumnDef {
                        name: name.to_string(),
                        data_type: DataType::Int32,
                        nullable: *nullable,
                        write_default: None,
                        logical_type: None,
                    })
                    .collect(),
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::StarRocks {
                    db_id: 0,
                    table_id: 0,
                },
            },
            alias: Some(alias.to_string()),
            columns: cols
                .iter()
                .map(|(name, id, nullable)| OutputColumn {
                    column_id: ColumnId::new_for_test(*id),
                    name: name.to_string(),
                    data_type: DataType::Int32,
                    nullable: *nullable,
                    is_internal: false,
                })
                .collect(),
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            required_output_columns: None,
        })
    }

    fn col(qualifier: &str, name: &str, id: u32, nullable: bool) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: Some(qualifier.to_string()),
                column: name.to_string(),
            },
            data_type: DataType::Int32,
            nullable,
        }
    }

    fn eq(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::Eq,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: true,
        }
    }

    fn and(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::And,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: true,
        }
    }

    fn join(
        jt: JoinKind,
        left: LogicalPlan,
        right: LogicalPlan,
        cond: Option<TypedExpr>,
    ) -> LogicalPlan {
        LogicalPlan::Join(JoinNode {
            left: Box::new(left),
            right: Box::new(right),
            join_type: jt,
            condition: cond,
            required_output_columns: None,
        })
    }

    /// (left_child_is_filter, right_child_is_filter) for the rule's output.
    fn side_filters(out: Option<LogicalPlan>) -> (bool, bool) {
        match out {
            None => (false, false),
            Some(LogicalPlan::Join(j)) => (
                matches!(*j.left, LogicalPlan::Filter(_)),
                matches!(*j.right, LogicalPlan::Filter(_)),
            ),
            Some(_) => panic!("rule must return a Join"),
        }
    }

    /// Count IS NOT NULL conjuncts in a Filter's predicate (0 if not a Filter).
    fn not_null_count(plan: &LogicalPlan) -> usize {
        let LogicalPlan::Filter(f) = plan else {
            return 0;
        };
        split_and(f.predicate.clone())
            .iter()
            .filter(|e| matches!(&e.kind, ExprKind::IsNull { negated: true, .. }))
            .count()
    }

    fn inner_eq_join(left_nullable: bool, right_nullable: bool) -> LogicalPlan {
        join(
            JoinKind::Inner,
            scan("l", "tl", &[("a", 1, left_nullable)]),
            scan("r", "tr", &[("b", 2, right_nullable)]),
            Some(eq(
                col("l", "a", 1, left_nullable),
                col("r", "b", 2, right_nullable),
            )),
        )
    }

    #[test]
    fn join_type_safety_table() {
        let cases = [
            (JoinKind::Inner, true, true),
            (JoinKind::LeftSemi, false, true),
            (JoinKind::RightSemi, true, false),
            (JoinKind::LeftAnti, false, false),
            (JoinKind::RightAnti, false, false),
            (JoinKind::NullAwareLeftAnti, false, false),
            (JoinKind::LeftOuter, false, false),
            (JoinKind::RightOuter, false, false),
            (JoinKind::FullOuter, false, false),
            (JoinKind::Cross, false, false),
        ];
        for (jt, exp_l, exp_r) in cases {
            let cond = if matches!(jt, JoinKind::Cross) {
                None
            } else {
                Some(eq(col("l", "a", 1, true), col("r", "b", 2, true)))
            };
            let plan = join(
                jt,
                scan("l", "tl", &[("a", 1, true)]),
                scan("r", "tr", &[("b", 2, true)]),
                cond,
            );
            assert_eq!(
                side_filters(DeriveJoinNotNullPredicate.apply(plan)),
                (exp_l, exp_r),
                "join type {jt:?}"
            );
        }
    }

    #[test]
    fn non_nullable_keys_are_skipped() {
        assert_eq!(
            side_filters(DeriveJoinNotNullPredicate.apply(inner_eq_join(false, false))),
            (false, false)
        );
        // Only the nullable side gets a filter.
        assert_eq!(
            side_filters(DeriveJoinNotNullPredicate.apply(inner_eq_join(true, false))),
            (true, false)
        );
    }

    #[test]
    fn composite_inner_key_derives_all_columns_on_each_side() {
        let plan = join(
            JoinKind::Inner,
            scan("l", "tl", &[("a", 1, true), ("a2", 3, true)]),
            scan("r", "tr", &[("b", 2, true), ("b2", 4, true)]),
            Some(and(
                eq(col("l", "a", 1, true), col("r", "b", 2, true)),
                eq(col("l", "a2", 3, true), col("r", "b2", 4, true)),
            )),
        );
        let Some(LogicalPlan::Join(j)) = DeriveJoinNotNullPredicate.apply(plan) else {
            panic!("expected join");
        };
        assert_eq!(not_null_count(&j.left), 2);
        assert_eq!(not_null_count(&j.right), 2);
    }

    #[test]
    fn idempotent_second_apply_is_noop() {
        let once = DeriveJoinNotNullPredicate
            .apply(inner_eq_join(true, true))
            .expect("first applies");
        // Second application over the already-derived plan must not change it.
        assert!(DeriveJoinNotNullPredicate.apply(once).is_none());
    }

    #[test]
    fn idempotent_when_not_null_already_in_scan_predicates() {
        // State B: IS NOT NULL already pushed into scan.predicates (as
        // PushDownPredicateScan would do). The rule must NOT re-derive.
        fn not_null(operand: TypedExpr) -> TypedExpr {
            TypedExpr {
                kind: ExprKind::IsNull {
                    expr: Box::new(operand),
                    negated: true,
                },
                data_type: DataType::Boolean,
                nullable: false,
            }
        }
        let mut left = scan("l", "tl", &[("a", 1, true)]);
        let mut right = scan("r", "tr", &[("b", 2, true)]);
        if let LogicalPlan::Scan(s) = &mut left {
            s.predicates.push(not_null(col("l", "a", 1, true)));
        }
        if let LogicalPlan::Scan(s) = &mut right {
            s.predicates.push(not_null(col("r", "b", 2, true)));
        }
        let plan = join(
            JoinKind::Inner,
            left,
            right,
            Some(eq(col("l", "a", 1, true), col("r", "b", 2, true))),
        );
        assert!(DeriveJoinNotNullPredicate.apply(plan).is_none());
    }

    #[test]
    fn non_equi_and_missing_condition_skipped() {
        assert!(
            DeriveJoinNotNullPredicate
                .apply(join(
                    JoinKind::Inner,
                    scan("l", "tl", &[("a", 1, true)]),
                    scan("r", "tr", &[("b", 2, true)]),
                    None
                ))
                .is_none()
        );
        let gt = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col("l", "a", 1, true)),
                op: BinOp::Gt,
                right: Box::new(col("r", "b", 2, true)),
            },
            data_type: DataType::Boolean,
            nullable: true,
        };
        assert!(
            DeriveJoinNotNullPredicate
                .apply(join(
                    JoinKind::Inner,
                    scan("l", "tl", &[("a", 1, true)]),
                    scan("r", "tr", &[("b", 2, true)]),
                    Some(gt)
                ))
                .is_none()
        );
    }
}
