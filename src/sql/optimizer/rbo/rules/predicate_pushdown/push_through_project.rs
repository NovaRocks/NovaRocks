//! PushDownPredicateProject — `Filter(Project)` rewrite.
//!
//! Pushes conjuncts that reference only pass-through (i.e. bare
//! `ColumnRef`) projection items below the Project, leaving conjuncts
//! that touch computed expressions as a residual Filter above. One step
//! only — the driver's bottom-up walker will push further at the next
//! round.
//!
//! Mirrors the `LogicalPlan::Project(proj)` arm of legacy
//! `predicate_pushdown::push_filter_into`, with the difference that this
//! rule does NOT recurse (driver owns traversal).

use std::collections::HashSet;

use super::super::super::rule::RewriteRule;
use crate::sql::analysis::ExprKind;
use crate::sql::optimizer::rbo::utils::{
    collect_column_refs, combine_and, split_and, wrap_remaining_filter,
};
use crate::sql::planner::plan::*;

pub(crate) struct PushDownPredicateProject;

impl RewriteRule for PushDownPredicateProject {
    fn name(&self) -> &'static str {
        "PushDownPredicateProject"
    }

    fn matches(&self, plan: &LogicalPlan) -> bool {
        matches!(
            plan,
            LogicalPlan::Filter(f) if matches!(*f.input, LogicalPlan::Project(_))
        )
    }

    fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan> {
        let LogicalPlan::Filter(filter) = plan else {
            return None;
        };
        let LogicalPlan::Project(proj) = *filter.input else {
            return None;
        };

        let passthrough_columns: HashSet<String> = proj
            .items
            .iter()
            .filter_map(|item| {
                if let ExprKind::ColumnRef { column, .. } = &item.expr.kind {
                    Some(column.to_lowercase())
                } else {
                    None
                }
            })
            .collect();

        let conjuncts = split_and(filter.predicate);
        let mut pushable = Vec::new();
        let mut remaining = Vec::new();
        for conj in conjuncts {
            let refs = collect_column_refs(&conj);
            if refs
                .iter()
                .all(|r| passthrough_columns.contains(&r.to_lowercase()))
            {
                pushable.push(conj);
            } else {
                remaining.push(conj);
            }
        }

        if pushable.is_empty() {
            return None;
        }

        // Build Filter(child) below the Project.
        let pushed = combine_and(pushable);
        let new_child = LogicalPlan::Filter(FilterNode {
            input: proj.input,
            predicate: pushed,
        });
        let new_project = LogicalPlan::Project(ProjectNode {
            input: Box::new(new_child),
            items: proj.items,
        });
        Some(wrap_remaining_filter(new_project, remaining))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{
        BinOp, ExprKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr,
    };
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

    fn and(a: TypedExpr, b: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::BinaryOp {
                left: Box::new(a),
                op: BinOp::And,
                right: Box::new(b),
            },
        }
    }

    fn scan_with_cols(cols: &[&str]) -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "db".into(),
            table: TableDef {
                name: "t".into(),
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
            alias: None,
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

    /// Build a pass-through Project that forwards the named columns unchanged.
    fn passthrough_project(cols: &[&str], input: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Project(ProjectNode {
            input: Box::new(input),
            items: cols
                .iter()
                .map(|n| ProjectItem {
                    expr: col(n),
                    output_name: (*n).into(),
                })
                .collect(),
        })
    }

    // Test 1: SELECT a, b FROM (SELECT a, b FROM t) WHERE a = 1
    // Expected: Project(Filter(Scan)) — the predicate is pushed below the project.
    #[test]
    fn pushes_through_passthrough_project() {
        let scan = scan_with_cols(&["a", "b"]);
        let project = passthrough_project(&["a", "b"], scan);
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(project),
            predicate: eq(col("a"), int_lit(1)),
        });

        let rule = PushDownPredicateProject;
        assert!(rule.matches(&filter));
        let out = rule.apply(filter).expect("should rewrite");

        // Expected shape: Project(Filter(Scan))
        match out {
            LogicalPlan::Project(p) => match *p.input {
                LogicalPlan::Filter(f) => match *f.input {
                    LogicalPlan::Scan(_) => {}
                    other => panic!("expected Scan under Filter, got {:?}", other),
                },
                other => panic!("expected Filter under Project, got {:?}", other),
            },
            other => panic!("expected Project at top, got {:?}", other),
        }
    }

    // Test 2: SELECT a+1 AS x FROM t WHERE x = 5
    // The projection item for x is computed (BinaryOp), not a bare ColumnRef.
    // No conjuncts are pushable; rule must return None.
    #[test]
    fn does_not_push_through_computed_projection() {
        let scan = scan_with_cols(&["a"]);
        // Build: Project(Scan) with item x = a + 1
        let computed_expr = TypedExpr {
            data_type: DataType::Int64,
            nullable: true,
            kind: ExprKind::BinaryOp {
                left: Box::new(col("a")),
                op: BinOp::Add,
                right: Box::new(int_lit(1)),
            },
        };
        let project = LogicalPlan::Project(ProjectNode {
            input: Box::new(scan),
            items: vec![ProjectItem {
                expr: computed_expr,
                output_name: "x".into(),
            }],
        });
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(project),
            predicate: eq(col("x"), int_lit(5)),
        });

        let rule = PushDownPredicateProject;
        assert!(rule.matches(&filter));
        // x is computed — nothing pushable; must return None
        assert!(
            rule.apply(filter).is_none(),
            "should not push through a computed projection"
        );
    }

    // Test 4: WHERE 1=1 (constant predicate, no column refs).
    // Legacy push_filter_into Project arm pushes it via vacuous-truth of all()
    // on an empty iterator. The new rule must match exactly.
    // Expected shape: Project(Filter(Scan))
    #[test]
    fn pushes_constant_predicate_through_project() {
        // WHERE 1=1 (no column refs): legacy behavior is to push vacuously;
        // new rule must match exactly.
        let scan = scan_with_cols(&["a"]);
        let project = passthrough_project(&["a"], scan);
        let one_eq_one = eq(int_lit(1), int_lit(1));
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(project),
            predicate: one_eq_one,
        });
        let rule = PushDownPredicateProject;
        let out = rule.apply(filter).expect("should push vacuous constant");
        match out {
            LogicalPlan::Project(p) => {
                assert!(matches!(*p.input, LogicalPlan::Filter(_)));
            }
            other => panic!(
                "expected Project(Filter(Scan)) for pushed constant, got {:?}",
                other
            ),
        }
    }

    // Test 3: AND of a pass-through ref (a = 1) and a computed-expr ref (x = 5)
    // where only a is a bare pass-through column.
    // Expected shape: Filter(Project(Filter(Scan)))
    //   — a=1 is pushed below the Project, x=5 remains above.
    #[test]
    fn partial_pushdown_through_project() {
        let scan = scan_with_cols(&["a"]);
        // Project: a is pass-through, x = a+1 is computed.
        let computed_expr = TypedExpr {
            data_type: DataType::Int64,
            nullable: true,
            kind: ExprKind::BinaryOp {
                left: Box::new(col("a")),
                op: BinOp::Add,
                right: Box::new(int_lit(1)),
            },
        };
        let project = LogicalPlan::Project(ProjectNode {
            input: Box::new(scan),
            items: vec![
                ProjectItem {
                    expr: col("a"),
                    output_name: "a".into(),
                },
                ProjectItem {
                    expr: computed_expr,
                    output_name: "x".into(),
                },
            ],
        });
        // Filter: a=1 AND x=5
        let pred = and(eq(col("a"), int_lit(1)), eq(col("x"), int_lit(5)));
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(project),
            predicate: pred,
        });

        let rule = PushDownPredicateProject;
        let out = rule.apply(filter).expect("should produce partial rewrite");

        // Expected: Filter(Project(Filter(Scan)))
        match out {
            LogicalPlan::Filter(outer_f) => match *outer_f.input {
                LogicalPlan::Project(p) => match *p.input {
                    LogicalPlan::Filter(inner_f) => match *inner_f.input {
                        LogicalPlan::Scan(_) => {}
                        other => panic!("expected Scan at bottom, got {:?}", other),
                    },
                    other => panic!("expected Filter under Project, got {:?}", other),
                },
                other => panic!("expected Project under outer Filter, got {:?}", other),
            },
            other => panic!("expected outer Filter at top, got {:?}", other),
        }
    }
}
