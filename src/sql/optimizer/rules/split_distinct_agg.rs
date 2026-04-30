//! Implementation rule: multi-phase DISTINCT aggregation.
//!
//! Matches a `LogicalAggregate` with at least one DISTINCT aggregate call,
//! where all DISTINCT calls share a single simple column as their argument.
//! Emits one alternative physical chain:
//!   - 3-phase (LOCAL -> DISTINCT_GLOBAL -> GLOBAL) when `group_by` is non-empty.
//!   - 4-phase (LOCAL -> DISTINCT_GLOBAL -> DISTINCT_LOCAL -> GLOBAL) when scalar.
//!
//! Mirrors StarRocks's `SplitAggregateRule` / `AggType.java` convention.

use crate::sql::analysis::{ExprKind, TypedExpr};
use crate::sql::optimizer::memo::{MExpr, Memo};
use crate::sql::optimizer::operator::{
    AggMode, LogicalAggregateOp, Operator, PhysicalHashAggregateOp,
};
use crate::sql::optimizer::rule::{NewExpr, Rule, RuleType};
use crate::sql::planner::plan::AggregateCall;

pub(crate) struct SplitDistinctAgg;

impl Rule for SplitDistinctAgg {
    fn name(&self) -> &str {
        "SplitDistinctAgg"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Implementation
    }

    fn matches(&self, op: &Operator) -> bool {
        matches!(op, Operator::LogicalAggregate(a) if a.aggregates.iter().any(|c| c.distinct))
    }

    fn apply(&self, expr: &MExpr, memo: &mut Memo) -> Vec<NewExpr> {
        let Operator::LogicalAggregate(agg) = &expr.op else {
            return vec![];
        };

        // Ordered aggregates need all order-by inputs available at the update
        // phase. The current split-distinct lowering only preserves the
        // shared DISTINCT column across phase boundaries, so ordered DISTINCT
        // aggregates like `array_agg(distinct x order by y)` lose `y` in the
        // GLOBAL phase. Fall back to the single-stage aggregate for semantic
        // correctness until multi-phase ordered DISTINCT is implemented.
        if agg.aggregates.iter().any(|call| !call.order_by.is_empty()) {
            return vec![];
        }

        // Validate single-DISTINCT-column precondition.
        let distinct_col = match extract_single_distinct_col(&agg.aggregates) {
            Some(c) => c,
            None => return vec![], // multi-column DISTINCT, or multiple different DISTINCT cols
        };

        // Partition aggregates into DISTINCT-bearing (which are deduped away at LOCAL)
        // and non-DISTINCT (which flow as merge states through the phases).
        let non_distinct: Vec<AggregateCall> = agg
            .aggregates
            .iter()
            .filter(|c| !c.distinct)
            .cloned()
            .collect();

        // Stateful sketch/bitmap aggregates preserve null/empty-state semantics
        // across the current split-distinct phase boundaries poorly. Fall back
        // to the single-stage plan for correctness until their merge path is
        // aligned with StarRocks FE.
        if non_distinct
            .iter()
            .any(|call| split_distinct_sensitive_agg(call.name.as_str()))
        {
            return vec![];
        }

        if agg.group_by.is_empty() {
            apply_four_phase(expr, memo, agg, &distinct_col, &non_distinct)
        } else {
            apply_three_phase(expr, memo, agg, &distinct_col, &non_distinct)
        }
    }
}

/// Return the shared DISTINCT column if every DISTINCT aggregate takes exactly
/// one argument and all such arguments are the same simple `ColumnRef`.
/// Returns `None` for:
///   - no DISTINCT calls at all (shouldn't happen -- `matches` filters this)
///   - multi-arg DISTINCT (`count(distinct a, b)`)
///   - multiple distinct columns (`count(distinct a), count(distinct b)`)
///   - DISTINCT arg that is not a plain ColumnRef
fn extract_single_distinct_col(calls: &[AggregateCall]) -> Option<TypedExpr> {
    let mut distinct_calls = calls.iter().filter(|c| c.distinct);
    let first = distinct_calls.next()?;
    if first.args.len() != 1 {
        return None;
    }
    if !matches!(first.args[0].kind, ExprKind::ColumnRef { .. }) {
        return None;
    }
    for c in distinct_calls {
        if c.args.len() != 1 {
            return None;
        }
        if !typed_exprs_structurally_equal(&c.args[0], &first.args[0]) {
            return None;
        }
    }
    Some(first.args[0].clone())
}

fn typed_exprs_structurally_equal(a: &TypedExpr, b: &TypedExpr) -> bool {
    match (&a.kind, &b.kind) {
        (
            ExprKind::ColumnRef {
                qualifier: qa,
                column: ca,
            },
            ExprKind::ColumnRef {
                qualifier: qb,
                column: cb,
            },
        ) => qa == qb && ca == cb,
        _ => false,
    }
}

fn split_distinct_sensitive_agg(name: &str) -> bool {
    matches!(
        name,
        "approx_count_distinct_hll_sketch"
            | "bitmap_agg"
            | "bitmap_union"
            | "bitmap_union_count"
            | "bitmap_union_int"
            | "ds_hll_count_distinct"
            | "ds_hll_count_distinct_merge"
            | "ds_hll_count_distinct_union"
            | "hll_raw_agg"
            | "hll_union"
            | "hll_union_agg"
    )
}

fn apply_three_phase(
    expr: &MExpr,
    memo: &mut Memo,
    agg: &LogicalAggregateOp,
    distinct_col: &TypedExpr,
    non_distinct: &[AggregateCall],
) -> Vec<NewExpr> {
    // Group-by for LOCAL and DISTINCT_GLOBAL: original group_by + distinct_col.
    let mut gb_with_distinct = agg.group_by.clone();
    gb_with_distinct.push(distinct_col.clone());

    // LOCAL: group_by = g + x; non_distinct aggs computed with update semantics.
    let local_id = memo.next_expr_id();
    let local = MExpr {
        id: local_id,
        op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Local,
            group_by: gb_with_distinct.clone(),
            aggregates: non_distinct.to_vec(),
            output_columns: vec![],
            is_merge: vec![false; non_distinct.len()],
        }),
        children: expr.children.clone(),
    };
    let local_group = memo.new_group(local);

    // DISTINCT_GLOBAL: same group_by; merge non_distinct states.
    let dg_id = memo.next_expr_id();
    let dg = MExpr {
        id: dg_id,
        op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::DistinctGlobal,
            group_by: gb_with_distinct,
            aggregates: non_distinct.to_vec(),
            output_columns: vec![],
            is_merge: vec![true; non_distinct.len()],
        }),
        children: vec![local_group],
    };
    let dg_group = memo.new_group(dg);

    // GLOBAL: group_by = original g; aggregates = [count(x) update, then each
    // non_distinct merged].
    //
    // Preserve the original distinct aggregate call (first one from agg.aggregates)
    // so that agg_call_display_name matches what the PROJECT node expects.
    // (e.g. "count(distinct x)" not "count(x)").
    let first_distinct = agg
        .aggregates
        .iter()
        .find(|c| c.distinct)
        .cloned()
        .unwrap_or_else(|| AggregateCall {
            name: "count".into(),
            args: vec![distinct_col.clone()],
            distinct: true,
            result_type: arrow::datatypes::DataType::Int64,
            order_by: vec![],
        });
    let mut global_aggs = Vec::with_capacity(1 + non_distinct.len());
    global_aggs.push(first_distinct);
    global_aggs.extend(non_distinct.iter().cloned());
    let mut global_merge = Vec::with_capacity(1 + non_distinct.len());
    global_merge.push(false); // distinct agg is an update in the GLOBAL phase
    global_merge.extend(std::iter::repeat_n(true, non_distinct.len()));

    vec![NewExpr {
        op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Global,
            group_by: agg.group_by.clone(),
            aggregates: global_aggs,
            output_columns: agg.output_columns.clone(),
            is_merge: global_merge,
        }),
        children: vec![dg_group],
    }]
}

fn apply_four_phase(
    expr: &MExpr,
    memo: &mut Memo,
    agg: &crate::sql::optimizer::operator::LogicalAggregateOp,
    distinct_col: &TypedExpr,
    non_distinct: &[AggregateCall],
) -> Vec<NewExpr> {
    // LOCAL: group_by = [x]; non_distinct aggs with update semantics.
    let local_id = memo.next_expr_id();
    let local = MExpr {
        id: local_id,
        op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Local,
            group_by: vec![distinct_col.clone()],
            aggregates: non_distinct.to_vec(),
            output_columns: vec![],
            is_merge: vec![false; non_distinct.len()],
        }),
        children: expr.children.clone(),
    };
    let local_group = memo.new_group(local);

    // DISTINCT_GLOBAL: group_by = [x]; merge non_distinct states.
    let dg_id = memo.next_expr_id();
    let dg = MExpr {
        id: dg_id,
        op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::DistinctGlobal,
            group_by: vec![distinct_col.clone()],
            aggregates: non_distinct.to_vec(),
            output_columns: vec![],
            is_merge: vec![true; non_distinct.len()],
        }),
        children: vec![local_group],
    };
    let dg_group = memo.new_group(dg);

    // Build the phase-boundary aggregate list shared by DISTINCT_LOCAL and GLOBAL:
    // [first_distinct_agg first, then each non_distinct]. Fragment builder applies
    // per-call is_merge dispatch from op.is_merge.
    //
    // Use the original distinct aggregate call so that agg_call_display_name
    // (e.g. "count(distinct x)") matches what the PROJECT node expects.
    let first_distinct = agg
        .aggregates
        .iter()
        .find(|c| c.distinct)
        .cloned()
        .unwrap_or_else(|| AggregateCall {
            name: "count".into(),
            args: vec![distinct_col.clone()],
            distinct: true,
            result_type: arrow::datatypes::DataType::Int64,
            order_by: vec![],
        });
    let mut phase_aggs = Vec::with_capacity(1 + non_distinct.len());
    phase_aggs.push(first_distinct);
    phase_aggs.extend(non_distinct.iter().cloned());

    // DISTINCT_LOCAL: scalar; [count(x) update, non_distinct merge...].
    let mut dl_merge = Vec::with_capacity(1 + non_distinct.len());
    dl_merge.push(false);
    dl_merge.extend(std::iter::repeat_n(true, non_distinct.len()));
    let dl_id = memo.next_expr_id();
    let dl = MExpr {
        id: dl_id,
        op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::DistinctLocal,
            group_by: vec![],
            aggregates: phase_aggs.clone(),
            output_columns: vec![],
            is_merge: dl_merge,
        }),
        children: vec![dg_group],
    };
    let dl_group = memo.new_group(dl);

    // GLOBAL: scalar; aggregates all MERGES.
    //
    // Correctness note: when the preserved distinct aggregate is count(distinct x),
    // the physical expression becomes multi_distinct_count with is_merge_agg=true,
    // which merges bitmap states across DISTINCT_LOCAL instances. This is correct
    // because DISTINCT_GLOBAL partitions data by x, guaranteeing each DISTINCT_LOCAL
    // instance sees a disjoint subset of distinct x values. Bitmap union over
    // disjoint sets is equivalent to sum of partial counts.
    let global_merge = vec![true; 1 + non_distinct.len()];

    vec![NewExpr {
        op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Global,
            group_by: vec![],
            aggregates: phase_aggs,
            output_columns: agg.output_columns.clone(),
            is_merge: global_merge,
        }),
        children: vec![dl_group],
    }]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
    use crate::sql::optimizer::memo::Memo;
    use crate::sql::optimizer::operator::{AggMode, LogicalAggregateOp, LogicalScanOp};
    use arrow::datatypes::DataType;
    use std::sync::Arc;

    fn col(name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: name.into(),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn scan_group(memo: &mut Memo) -> usize {
        let m = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalScan(LogicalScanOp {
                database: "db".into(),
                table: crate::sql::catalog::TableDef {
                    name: "t".into(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    storage: crate::sql::catalog::TableStorage::LocalParquetFile {
                        path: std::path::PathBuf::from("/tmp/t.parquet"),
                    },
                },
                alias: None,
                columns: vec![],
                predicates: vec![],
                required_columns: None,
            }),
            children: vec![],
        };
        memo.new_group(m)
    }

    fn count_distinct(arg_name: &str) -> AggregateCall {
        AggregateCall {
            name: "count".into(),
            args: vec![col(arg_name)],
            distinct: true,
            result_type: DataType::Int64,
            order_by: vec![],
        }
    }

    fn sum_non_distinct(arg_name: &str) -> AggregateCall {
        AggregateCall {
            name: "sum".into(),
            args: vec![col(arg_name)],
            distinct: false,
            result_type: DataType::Int64,
            order_by: vec![],
        }
    }

    #[test]
    fn matches_when_any_distinct() {
        let op = Operator::LogicalAggregate(LogicalAggregateOp {
            group_by: vec![],
            aggregates: vec![count_distinct("x"), sum_non_distinct("a")],
            output_columns: vec![],
        });
        assert!(SplitDistinctAgg.matches(&op));
    }

    #[test]
    fn does_not_match_when_no_distinct() {
        let op = Operator::LogicalAggregate(LogicalAggregateOp {
            group_by: vec![],
            aggregates: vec![sum_non_distinct("a")],
            output_columns: vec![],
        });
        assert!(!SplitDistinctAgg.matches(&op));
    }

    #[test]
    fn apply_skips_multi_arg_distinct() {
        let mut memo = Memo::new();
        let sg = scan_group(&mut memo);
        let two_arg = AggregateCall {
            name: "count".into(),
            args: vec![col("a"), col("b")],
            distinct: true,
            result_type: DataType::Int64,
            order_by: vec![],
        };
        let id = memo.next_expr_id();
        let mexpr = MExpr {
            id,
            op: Operator::LogicalAggregate(LogicalAggregateOp {
                group_by: vec![],
                aggregates: vec![two_arg],
                output_columns: vec![],
            }),
            children: vec![sg],
        };
        assert!(SplitDistinctAgg.apply(&mexpr, &mut memo).is_empty());
    }

    #[test]
    fn apply_skips_distinct_on_different_cols() {
        let mut memo = Memo::new();
        let sg = scan_group(&mut memo);
        let id = memo.next_expr_id();
        let mexpr = MExpr {
            id,
            op: Operator::LogicalAggregate(LogicalAggregateOp {
                group_by: vec![],
                aggregates: vec![count_distinct("a"), count_distinct("b")],
                output_columns: vec![],
            }),
            children: vec![sg],
        };
        assert!(SplitDistinctAgg.apply(&mexpr, &mut memo).is_empty());
    }

    #[test]
    fn apply_skips_non_distinct_order_sensitive_aggregate() {
        let mut memo = Memo::new();
        let sg = scan_group(&mut memo);
        let id = memo.next_expr_id();
        let mexpr = MExpr {
            id,
            op: Operator::LogicalAggregate(LogicalAggregateOp {
                group_by: vec![col("g")],
                aggregates: vec![
                    AggregateCall {
                        name: "array_agg".into(),
                        args: vec![col("name")],
                        distinct: false,
                        result_type: DataType::List(Arc::new(arrow::datatypes::Field::new(
                            "item",
                            DataType::Int64,
                            true,
                        ))),
                        order_by: vec![crate::sql::analysis::SortItem {
                            expr: col("id"),
                            asc: true,
                            nulls_first: true,
                        }],
                    },
                    count_distinct("name"),
                ],
                output_columns: vec![],
            }),
            children: vec![sg],
        };
        assert!(SplitDistinctAgg.apply(&mexpr, &mut memo).is_empty());
    }

    #[test]
    fn apply_skips_stateful_sketch_non_distinct_aggregate() {
        let mut memo = Memo::new();
        let sg = scan_group(&mut memo);
        let id = memo.next_expr_id();
        let mexpr = MExpr {
            id,
            op: Operator::LogicalAggregate(LogicalAggregateOp {
                group_by: vec![],
                aggregates: vec![
                    count_distinct("x"),
                    AggregateCall {
                        name: "ds_hll_count_distinct".into(),
                        args: vec![col("x")],
                        distinct: false,
                        result_type: DataType::Int64,
                        order_by: vec![],
                    },
                ],
                output_columns: vec![],
            }),
            children: vec![sg],
        };
        assert!(SplitDistinctAgg.apply(&mexpr, &mut memo).is_empty());
    }

    #[test]
    fn apply_skips_distinct_order_sensitive_aggregate() {
        let mut memo = Memo::new();
        let sg = scan_group(&mut memo);
        let id = memo.next_expr_id();
        let mexpr = MExpr {
            id,
            op: Operator::LogicalAggregate(LogicalAggregateOp {
                group_by: vec![col("g")],
                aggregates: vec![AggregateCall {
                    name: "array_agg".into(),
                    args: vec![col("name")],
                    distinct: true,
                    result_type: DataType::List(Arc::new(arrow::datatypes::Field::new(
                        "item",
                        DataType::Int64,
                        true,
                    ))),
                    order_by: vec![crate::sql::analysis::SortItem {
                        expr: col("id"),
                        asc: true,
                        nulls_first: true,
                    }],
                }],
                output_columns: vec![],
            }),
            children: vec![sg],
        };
        assert!(SplitDistinctAgg.apply(&mexpr, &mut memo).is_empty());
    }

    #[test]
    fn extracts_distinct_col_for_same_col_multi_distinct() {
        // count(distinct x) + sum(distinct x) -- same col. Accepts both.
        let sum_distinct_x = AggregateCall {
            name: "sum".into(),
            args: vec![col("x")],
            distinct: true,
            result_type: DataType::Int64,
            order_by: vec![],
        };
        let col_out = extract_single_distinct_col(&[count_distinct("x"), sum_distinct_x]);
        assert!(
            col_out.is_some(),
            "expected Some for same-column multi-DISTINCT"
        );
        let ExprKind::ColumnRef { column, .. } = &col_out.unwrap().kind else {
            panic!("expected ColumnRef");
        };
        assert_eq!(column, "x");
    }

    #[test]
    fn three_phase_chain_with_group_by() {
        let mut memo = Memo::new();
        let sg = scan_group(&mut memo);
        let id = memo.next_expr_id();
        let mexpr = MExpr {
            id,
            op: Operator::LogicalAggregate(LogicalAggregateOp {
                group_by: vec![col("g")],
                aggregates: vec![count_distinct("x"), sum_non_distinct("a")],
                output_columns: vec![
                    OutputColumn {
                        name: "g".into(),
                        data_type: DataType::Int64,
                        nullable: false,
                    },
                    OutputColumn {
                        name: "count(distinct x)".into(),
                        data_type: DataType::Int64,
                        nullable: true,
                    },
                    OutputColumn {
                        name: "sum(a)".into(),
                        data_type: DataType::Int64,
                        nullable: true,
                    },
                ],
            }),
            children: vec![sg],
        };
        let out = SplitDistinctAgg.apply(&mexpr, &mut memo);
        assert_eq!(out.len(), 1, "expected one multi-phase alternative");

        // Top: GLOBAL, group_by=[g], aggregates[0] = count(distinct x), aggregates[1] = sum(a) (merge)
        let top = match &out[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected GLOBAL PhysicalHashAggregate, got {:?}", other),
        };
        assert!(matches!(top.mode, AggMode::Global));
        assert_eq!(top.group_by.len(), 1, "GLOBAL group_by is just [g]");
        assert_eq!(top.aggregates.len(), 2);
        assert_eq!(top.aggregates[0].name, "count");
        // distinct=true is preserved so the display name matches what PROJECT looks up.
        assert!(top.aggregates[0].distinct);
        assert_eq!(top.is_merge, vec![false, true]);

        // Follow chain: GLOBAL -> DISTINCT_GLOBAL -> LOCAL -> scan
        assert_eq!(out[0].children.len(), 1);
        let dg_group = &memo.groups[out[0].children[0]];
        let dg = match &dg_group.physical_exprs[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected DISTINCT_GLOBAL, got {:?}", other),
        };
        assert!(matches!(dg.mode, AggMode::DistinctGlobal));
        assert_eq!(dg.group_by.len(), 2, "DG group_by is [g, x]");
        assert_eq!(dg.aggregates.len(), 1); // only sum(a); count(distinct x) is folded into grouping
        assert_eq!(dg.is_merge, vec![true]);
        assert_eq!(dg_group.physical_exprs[0].children.len(), 1);

        let local_group = &memo.groups[dg_group.physical_exprs[0].children[0]];
        let local = match &local_group.physical_exprs[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected LOCAL, got {:?}", other),
        };
        assert!(matches!(local.mode, AggMode::Local));
        assert_eq!(local.group_by.len(), 2, "LOCAL group_by is [g, x]");
        assert_eq!(local.aggregates.len(), 1);
        assert_eq!(local.is_merge, vec![false]);
        assert_eq!(local_group.physical_exprs[0].children, vec![sg]);
    }

    #[test]
    fn four_phase_chain_when_scalar() {
        let mut memo = Memo::new();
        let sg = scan_group(&mut memo);
        let id = memo.next_expr_id();
        let mexpr = MExpr {
            id,
            op: Operator::LogicalAggregate(LogicalAggregateOp {
                group_by: vec![],
                aggregates: vec![count_distinct("x"), sum_non_distinct("a")],
                output_columns: vec![
                    OutputColumn {
                        name: "count(distinct x)".into(),
                        data_type: DataType::Int64,
                        nullable: true,
                    },
                    OutputColumn {
                        name: "sum(a)".into(),
                        data_type: DataType::Int64,
                        nullable: true,
                    },
                ],
            }),
            children: vec![sg],
        };
        let out = SplitDistinctAgg.apply(&mexpr, &mut memo);
        assert_eq!(out.len(), 1);

        // Top: GLOBAL, scalar, [count(x) merge, sum(a) merge]
        let top = match &out[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected GLOBAL, got {:?}", other),
        };
        assert!(matches!(top.mode, AggMode::Global));
        assert_eq!(top.group_by.len(), 0);
        assert_eq!(top.aggregates.len(), 2);
        assert_eq!(top.is_merge, vec![true, true]);

        // DISTINCT_LOCAL: scalar, [count(x) update, sum(a) merge]
        let dl_group = &memo.groups[out[0].children[0]];
        let dl = match &dl_group.physical_exprs[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected DISTINCT_LOCAL, got {:?}", other),
        };
        assert!(matches!(dl.mode, AggMode::DistinctLocal));
        assert_eq!(dl.group_by.len(), 0);
        assert_eq!(dl.is_merge, vec![false, true]);

        // DISTINCT_GLOBAL: group_by=[x], [sum(a) merge]
        let dg_group = &memo.groups[dl_group.physical_exprs[0].children[0]];
        let dg = match &dg_group.physical_exprs[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected DISTINCT_GLOBAL, got {:?}", other),
        };
        assert!(matches!(dg.mode, AggMode::DistinctGlobal));
        assert_eq!(dg.group_by.len(), 1);
        assert_eq!(dg.is_merge, vec![true]);

        // LOCAL: group_by=[x], [sum(a) update]
        let local_group = &memo.groups[dg_group.physical_exprs[0].children[0]];
        let local = match &local_group.physical_exprs[0].op {
            Operator::PhysicalHashAggregate(p) => p,
            other => panic!("expected LOCAL, got {:?}", other),
        };
        assert!(matches!(local.mode, AggMode::Local));
        assert_eq!(local.group_by.len(), 1);
        assert_eq!(local.is_merge, vec![false]);
        assert_eq!(local_group.physical_exprs[0].children, vec![sg]);
    }
}
