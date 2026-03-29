//! Join reorder optimization pass.
//!
//! For hash joins the left child is the **probe** side and the right child is
//! the **build** side.  Building a hash table on a huge relation is
//! catastrophically expensive, so we want the *smaller* side on the right
//! (build) and the *larger* side on the left (probe).
//!
//! This pass estimates the byte-size of each subtree and, for join types whose
//! semantics are commutative (INNER, CROSS), swaps the children when the right
//! (build) side is significantly larger than the left (probe) side.
//!
//! Left-preserving joins (LEFT OUTER, LEFT SEMI, LEFT ANTI) and
//! right-preserving joins (RIGHT OUTER, RIGHT SEMI, RIGHT ANTI) as well as
//! FULL OUTER are never swapped because their semantics depend on which side is
//! left vs. right.

use crate::sql::catalog::TableStorage;
use crate::sql::ir::{BinOp, ExprKind, JoinKind, TypedExpr};
use crate::sql::plan::*;

/// Count the number of AND-conjuncts in a predicate expression.
fn count_conjuncts(expr: &TypedExpr) -> usize {
    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => count_conjuncts(left) + count_conjuncts(right),
        _ => 1,
    }
}

/// Estimate the output "size" of a plan subtree in bytes.
///
/// This is a rough heuristic that does not require table statistics.  It is
/// good enough to distinguish a 6 million-row fact table from a 30 thousand-row
/// dimension table, which is the primary goal.
fn estimate_size(plan: &LogicalPlan) -> u64 {
    match plan {
        LogicalPlan::Scan(s) => {
            let raw_size = match &s.table.storage {
                TableStorage::S3ParquetFiles { files, .. } => {
                    let total: u64 = files.iter().map(|f| f.size.max(0) as u64).sum();
                    total.max(1)
                }
                TableStorage::LocalParquetFile { path } => std::fs::metadata(path)
                    .map(|m| m.len())
                    .unwrap_or(1_000_000),
            };
            // Apply selectivity for pushed-down predicates on the scan
            let num_predicates = s.predicates.len();
            if num_predicates == 0 {
                raw_size
            } else {
                // Each predicate applies ~30% selectivity, multiplicatively
                // 1 pred: 30%, 2 preds: 9%, 3 preds: 2.7%, 4+: ~1%
                let factor = match num_predicates {
                    1 => 30,
                    2 => 9,
                    3 => 3,
                    _ => 1,
                };
                (raw_size * factor / 100).max(1)
            }
        }
        LogicalPlan::Filter(f) => {
            // Count conjuncts in the filter predicate for better selectivity estimate
            let num_conjuncts = count_conjuncts(&f.predicate);
            let input_size = estimate_size(&f.input);
            let factor = match num_conjuncts {
                1 => 30,
                2 => 9,
                3 => 3,
                _ => 1,
            };
            (input_size * factor / 100).max(1)
        }
        LogicalPlan::Join(j) => {
            // For an inner join the output is roughly bounded by the smaller
            // input (assuming a PK-FK join).
            let left = estimate_size(&j.left);
            let right = estimate_size(&j.right);
            left.min(right)
        }
        LogicalPlan::Aggregate(a) => {
            // Aggregation significantly reduces row count.
            estimate_size(&a.input) / 10
        }
        LogicalPlan::Project(p) => estimate_size(&p.input),
        LogicalPlan::Sort(s) => estimate_size(&s.input),
        LogicalPlan::Limit(l) => {
            let input = estimate_size(&l.input);
            // LIMIT drastically caps the output.
            input.min(10_000)
        }
        LogicalPlan::Window(w) => estimate_size(&w.input),
        // Leaf / set-op nodes without better info: default 1 MB.
        _ => 1_000_000,
    }
}

/// Reorder join children so that the larger relation is on the left (probe)
/// and the smaller relation is on the right (build).
///
/// The pass is applied bottom-up: children are reordered first so that size
/// estimates of intermediate joins are based on already-reordered subtrees.
pub(crate) fn reorder_joins(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Join(mut j) => {
            // Recurse into children first (bottom-up).
            j.left = Box::new(reorder_joins(*j.left));
            j.right = Box::new(reorder_joins(*j.right));

            match j.join_type {
                // INNER and CROSS are commutative — safe to swap.
                JoinKind::Inner | JoinKind::Cross => {
                    let left_size = estimate_size(&j.left);
                    let right_size = estimate_size(&j.right);

                    if right_size > left_size * 2 {
                        // The build side (right) is much larger than the probe
                        // side (left).  Swap so the big relation probes and the
                        // small relation builds.
                        //
                        // For INNER JOIN the join condition is symmetric (both
                        // sides reference columns by qualified name), so
                        // swapping children does not require rewriting the
                        // condition expression.
                        //
                        // For CROSS JOIN there is no condition at all.
                        tracing::debug!(
                            left_bytes = left_size,
                            right_bytes = right_size,
                            "join_reorder: swapping join sides"
                        );
                        std::mem::swap(&mut j.left, &mut j.right);
                    }
                }
                // Left/Right outer, semi, anti, and full outer joins have
                // asymmetric semantics — the preserved/probing side is fixed.
                JoinKind::LeftOuter
                | JoinKind::RightOuter
                | JoinKind::FullOuter
                | JoinKind::LeftSemi
                | JoinKind::RightSemi
                | JoinKind::LeftAnti
                | JoinKind::RightAnti => {}
            }

            LogicalPlan::Join(j)
        }
        // --- Recurse through all other node types --------------------------------
        LogicalPlan::Filter(mut f) => {
            f.input = Box::new(reorder_joins(*f.input));
            LogicalPlan::Filter(f)
        }
        LogicalPlan::Project(mut p) => {
            p.input = Box::new(reorder_joins(*p.input));
            LogicalPlan::Project(p)
        }
        LogicalPlan::Aggregate(mut a) => {
            a.input = Box::new(reorder_joins(*a.input));
            LogicalPlan::Aggregate(a)
        }
        LogicalPlan::Sort(mut s) => {
            s.input = Box::new(reorder_joins(*s.input));
            LogicalPlan::Sort(s)
        }
        LogicalPlan::Limit(mut l) => {
            l.input = Box::new(reorder_joins(*l.input));
            LogicalPlan::Limit(l)
        }
        LogicalPlan::Window(mut w) => {
            w.input = Box::new(reorder_joins(*w.input));
            LogicalPlan::Window(w)
        }
        LogicalPlan::Union(mut u) => {
            u.inputs = u.inputs.into_iter().map(reorder_joins).collect();
            LogicalPlan::Union(u)
        }
        LogicalPlan::Intersect(mut i) => {
            i.inputs = i.inputs.into_iter().map(reorder_joins).collect();
            LogicalPlan::Intersect(i)
        }
        LogicalPlan::Except(mut e) => {
            e.inputs = e.inputs.into_iter().map(reorder_joins).collect();
            LogicalPlan::Except(e)
        }
        LogicalPlan::SubqueryAlias(mut s) => {
            s.input = Box::new(reorder_joins(*s.input));
            LogicalPlan::SubqueryAlias(s)
        }
        // Leaf nodes: Scan, Values, GenerateSeries — nothing to reorder.
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::catalog::{ColumnDef, S3FileInfo, TableDef, TableStorage};
    use crate::sql::ir::{BinOp, ExprKind, JoinKind, OutputColumn, TypedExpr};
    use arrow::datatypes::DataType;

    /// Helper: build a `TableDef` backed by S3 parquet files with the given
    /// total byte size.
    fn s3_table(name: &str, total_bytes: i64) -> TableDef {
        TableDef {
            name: name.to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            }],
            storage: TableStorage::S3ParquetFiles {
                files: vec![S3FileInfo {
                    path: format!("s3://bucket/{}.parquet", name),
                    size: total_bytes,
                }],
                cloud_properties: Default::default(),
            },
        }
    }

    fn scan_for(table: &TableDef) -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: table.clone(),
            alias: None,
            columns: vec![OutputColumn {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            }],
            predicates: vec![],
            required_columns: None,
        })
    }

    fn eq_condition() -> Option<TypedExpr> {
        Some(TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(TypedExpr {
                    kind: ExprKind::ColumnRef {
                        qualifier: Some("a".to_string()),
                        column: "id".to_string(),
                    },
                    data_type: DataType::Int32,
                    nullable: false,
                }),
                op: BinOp::Eq,
                right: Box::new(TypedExpr {
                    kind: ExprKind::ColumnRef {
                        qualifier: Some("b".to_string()),
                        column: "id".to_string(),
                    },
                    data_type: DataType::Int32,
                    nullable: false,
                }),
            },
            data_type: DataType::Boolean,
            nullable: false,
        })
    }

    #[test]
    fn inner_join_swaps_when_build_side_is_larger() {
        // Left = small (1 KB), Right = large (10 MB) => should swap.
        let small = s3_table("small", 1_000);
        let large = s3_table("large", 10_000_000);

        let plan = LogicalPlan::Join(JoinNode {
            left: Box::new(scan_for(&small)),
            right: Box::new(scan_for(&large)),
            join_type: JoinKind::Inner,
            condition: eq_condition(),
        });

        let reordered = reorder_joins(plan);

        match reordered {
            LogicalPlan::Join(j) => {
                // After reorder: left (probe) = large, right (build) = small.
                let left_name = match j.left.as_ref() {
                    LogicalPlan::Scan(s) => s.table.name.clone(),
                    other => panic!("expected Scan, got {:?}", std::mem::discriminant(other)),
                };
                let right_name = match j.right.as_ref() {
                    LogicalPlan::Scan(s) => s.table.name.clone(),
                    other => panic!("expected Scan, got {:?}", std::mem::discriminant(other)),
                };
                assert_eq!(left_name, "large", "probe side should be the large table");
                assert_eq!(right_name, "small", "build side should be the small table");
            }
            other => panic!("expected Join, got {:?}", std::mem::discriminant(&other)),
        }
    }

    #[test]
    fn inner_join_no_swap_when_already_correct() {
        // Left = large (10 MB), Right = small (1 KB) => already correct.
        let small = s3_table("small", 1_000);
        let large = s3_table("large", 10_000_000);

        let plan = LogicalPlan::Join(JoinNode {
            left: Box::new(scan_for(&large)),
            right: Box::new(scan_for(&small)),
            join_type: JoinKind::Inner,
            condition: eq_condition(),
        });

        let reordered = reorder_joins(plan);

        match reordered {
            LogicalPlan::Join(j) => {
                let left_name = match j.left.as_ref() {
                    LogicalPlan::Scan(s) => s.table.name.clone(),
                    other => panic!("expected Scan, got {:?}", std::mem::discriminant(other)),
                };
                assert_eq!(
                    left_name, "large",
                    "probe side should remain the large table"
                );
            }
            other => panic!("expected Join, got {:?}", std::mem::discriminant(&other)),
        }
    }

    #[test]
    fn left_outer_join_never_swaps() {
        // Even though right is larger, LEFT OUTER cannot swap.
        let small = s3_table("small", 1_000);
        let large = s3_table("large", 10_000_000);

        let plan = LogicalPlan::Join(JoinNode {
            left: Box::new(scan_for(&small)),
            right: Box::new(scan_for(&large)),
            join_type: JoinKind::LeftOuter,
            condition: eq_condition(),
        });

        let reordered = reorder_joins(plan);

        match reordered {
            LogicalPlan::Join(j) => {
                let left_name = match j.left.as_ref() {
                    LogicalPlan::Scan(s) => s.table.name.clone(),
                    other => panic!("expected Scan, got {:?}", std::mem::discriminant(other)),
                };
                assert_eq!(left_name, "small", "LEFT OUTER must preserve left side");
            }
            other => panic!("expected Join, got {:?}", std::mem::discriminant(&other)),
        }
    }

    #[test]
    fn left_semi_join_never_swaps() {
        let small = s3_table("small", 1_000);
        let large = s3_table("large", 10_000_000);

        let plan = LogicalPlan::Join(JoinNode {
            left: Box::new(scan_for(&small)),
            right: Box::new(scan_for(&large)),
            join_type: JoinKind::LeftSemi,
            condition: eq_condition(),
        });

        let reordered = reorder_joins(plan);

        match reordered {
            LogicalPlan::Join(j) => {
                let left_name = match j.left.as_ref() {
                    LogicalPlan::Scan(s) => s.table.name.clone(),
                    other => panic!("expected Scan, got {:?}", std::mem::discriminant(other)),
                };
                assert_eq!(left_name, "small", "LEFT SEMI must preserve left side");
            }
            other => panic!("expected Join, got {:?}", std::mem::discriminant(&other)),
        }
    }

    #[test]
    fn cross_join_swaps_when_build_side_is_larger() {
        let small = s3_table("small", 1_000);
        let large = s3_table("large", 10_000_000);

        let plan = LogicalPlan::Join(JoinNode {
            left: Box::new(scan_for(&small)),
            right: Box::new(scan_for(&large)),
            join_type: JoinKind::Cross,
            condition: None,
        });

        let reordered = reorder_joins(plan);

        match reordered {
            LogicalPlan::Join(j) => {
                let left_name = match j.left.as_ref() {
                    LogicalPlan::Scan(s) => s.table.name.clone(),
                    other => panic!("expected Scan, got {:?}", std::mem::discriminant(other)),
                };
                assert_eq!(left_name, "large", "probe side should be the large table");
            }
            other => panic!("expected Join, got {:?}", std::mem::discriminant(&other)),
        }
    }

    #[test]
    fn nested_joins_reordered_bottom_up() {
        // Simulate TPC-H q3 shape:
        //   (customer JOIN orders) JOIN lineitem
        // customer = 3 KB, orders = 70 KB, lineitem = 600 KB
        let customer = s3_table("customer", 3_000);
        let orders = s3_table("orders", 70_000);
        let lineitem = s3_table("lineitem", 600_000);

        let inner_join = LogicalPlan::Join(JoinNode {
            left: Box::new(scan_for(&customer)),
            right: Box::new(scan_for(&orders)),
            join_type: JoinKind::Inner,
            condition: eq_condition(),
        });

        let outer_join = LogicalPlan::Join(JoinNode {
            left: Box::new(inner_join),
            right: Box::new(scan_for(&lineitem)),
            join_type: JoinKind::Inner,
            condition: eq_condition(),
        });

        let reordered = reorder_joins(outer_join);

        // After reorder:
        //   The inner join: customer(3K) vs orders(70K)
        //     => orders is >2x customer, so swap: left=orders, right=customer
        //   The outer join: inner_join result (~3K estimated) vs lineitem(600K)
        //     => lineitem is >2x inner result, so swap: left=lineitem, right=inner_join
        match reordered {
            LogicalPlan::Join(outer) => {
                // Outer left should be lineitem (the large fact table).
                match outer.left.as_ref() {
                    LogicalPlan::Scan(s) => {
                        assert_eq!(
                            s.table.name, "lineitem",
                            "lineitem should be probe of outer join"
                        );
                    }
                    other => panic!(
                        "expected Scan(lineitem) as outer left, got {:?}",
                        std::mem::discriminant(other)
                    ),
                }

                // Outer right should be the inner join.
                match outer.right.as_ref() {
                    LogicalPlan::Join(inner) => {
                        // Inner join: left=orders, right=customer
                        let inner_left = match inner.left.as_ref() {
                            LogicalPlan::Scan(s) => s.table.name.clone(),
                            other => {
                                panic!("expected Scan, got {:?}", std::mem::discriminant(other))
                            }
                        };
                        let inner_right = match inner.right.as_ref() {
                            LogicalPlan::Scan(s) => s.table.name.clone(),
                            other => {
                                panic!("expected Scan, got {:?}", std::mem::discriminant(other))
                            }
                        };
                        assert_eq!(inner_left, "orders", "orders should be probe of inner join");
                        assert_eq!(
                            inner_right, "customer",
                            "customer should be build of inner join"
                        );
                    }
                    other => panic!(
                        "expected Join as outer right, got {:?}",
                        std::mem::discriminant(other)
                    ),
                }
            }
            other => panic!("expected Join, got {:?}", std::mem::discriminant(&other)),
        }
    }
}
