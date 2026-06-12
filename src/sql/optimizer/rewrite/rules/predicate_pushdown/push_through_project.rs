//! PushDownPredicateProject — `Filter(Project)` rewrite.
//!
//! Pushes conjuncts that reference only pass-through (i.e. bare
//! `ColumnRef`) projection items below the Project, leaving conjuncts
//! that touch computed expressions as a residual Filter above. One step
//! only — the rewrite pipeline's bottom-up walker will push further at the next
//! round.
//!
//! Mirrors the `LogicalPlan::Project(proj)` arm of legacy
//! `predicate_pushdown::push_filter_into`, with the difference that this
//! rule does NOT recurse; the rewrite framework owns traversal.

use crate::sql::analysis::{ExprKind, SortItem, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::rule::PlanRewriteRule as RewriteRule;
use crate::sql::optimizer::rewrite::rules::utils::{combine_and, split_and, wrap_remaining_filter};
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

        let conjuncts = split_and(filter.predicate);
        let mut pushable = Vec::new();
        let mut remaining = Vec::new();
        for conj in conjuncts {
            match rewrite_predicate_through_project(&conj, &proj) {
                Some(rewritten) => pushable.push(rewritten),
                None => remaining.push(conj),
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
            required_output_columns: None,
        });
        let new_project = LogicalPlan::Project(ProjectNode {
            input: Box::new(new_child),
            items: proj.items,
            output_qualifier: proj.output_qualifier,
            required_output_columns: proj.required_output_columns,
        });
        Some(wrap_remaining_filter(new_project, remaining))
    }
}

fn rewrite_predicate_through_project(expr: &TypedExpr, proj: &ProjectNode) -> Option<TypedExpr> {
    match &expr.kind {
        ExprKind::ColumnRef {
            column_id,
            qualifier,
            column,
        } => lookup_passthrough_projection(*column_id, qualifier.as_deref(), column, proj),
        ExprKind::LambdaParamRef { .. } | ExprKind::Literal(_) => Some(expr.clone()),
        ExprKind::BinaryOp { left, op, right } => Some(TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::BinaryOp {
                left: Box::new(rewrite_predicate_through_project(left, proj)?),
                op: *op,
                right: Box::new(rewrite_predicate_through_project(right, proj)?),
            },
        }),
        ExprKind::UnaryOp { op, expr: inner } => Some(TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::UnaryOp {
                op: *op,
                expr: Box::new(rewrite_predicate_through_project(inner, proj)?),
            },
        }),
        ExprKind::FunctionCall {
            name,
            args,
            distinct,
        } => Some(TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::FunctionCall {
                name: name.clone(),
                args: rewrite_expr_list_through_project(args, proj)?,
                distinct: *distinct,
            },
        }),
        ExprKind::LambdaFunction { params, body } => Some(TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::LambdaFunction {
                params: params.clone(),
                body: Box::new(rewrite_predicate_through_project(body, proj)?),
            },
        }),
        ExprKind::AggregateCall {
            name,
            args,
            distinct,
            order_by,
        } => Some(TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::AggregateCall {
                name: name.clone(),
                args: rewrite_expr_list_through_project(args, proj)?,
                distinct: *distinct,
                order_by: order_by
                    .iter()
                    .map(|item| {
                        rewrite_predicate_through_project(&item.expr, proj).map(|expr| SortItem {
                            expr,
                            asc: item.asc,
                            nulls_first: item.nulls_first,
                        })
                    })
                    .collect::<Option<Vec<_>>>()?,
            },
        }),
        ExprKind::Cast {
            expr: inner,
            target,
        } => Some(TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::Cast {
                expr: Box::new(rewrite_predicate_through_project(inner, proj)?),
                target: target.clone(),
            },
        }),
        ExprKind::IsNull {
            expr: inner,
            negated,
        } => Some(TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::IsNull {
                expr: Box::new(rewrite_predicate_through_project(inner, proj)?),
                negated: *negated,
            },
        }),
        ExprKind::InList {
            expr: inner,
            list,
            negated,
        } => Some(TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::InList {
                expr: Box::new(rewrite_predicate_through_project(inner, proj)?),
                list: rewrite_expr_list_through_project(list, proj)?,
                negated: *negated,
            },
        }),
        ExprKind::Between {
            expr: inner,
            low,
            high,
            negated,
        } => Some(TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::Between {
                expr: Box::new(rewrite_predicate_through_project(inner, proj)?),
                low: Box::new(rewrite_predicate_through_project(low, proj)?),
                high: Box::new(rewrite_predicate_through_project(high, proj)?),
                negated: *negated,
            },
        }),
        ExprKind::Like {
            expr: inner,
            pattern,
            negated,
        } => Some(TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::Like {
                expr: Box::new(rewrite_predicate_through_project(inner, proj)?),
                pattern: Box::new(rewrite_predicate_through_project(pattern, proj)?),
                negated: *negated,
            },
        }),
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => Some(TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::Case {
                operand: match operand {
                    Some(operand) => {
                        Some(Box::new(rewrite_predicate_through_project(operand, proj)?))
                    }
                    None => None,
                },
                when_then: when_then
                    .iter()
                    .map(|(when, then)| {
                        Some((
                            rewrite_predicate_through_project(when, proj)?,
                            rewrite_predicate_through_project(then, proj)?,
                        ))
                    })
                    .collect::<Option<Vec<_>>>()?,
                else_expr: match else_expr {
                    Some(else_expr) => Some(Box::new(rewrite_predicate_through_project(
                        else_expr, proj,
                    )?)),
                    None => None,
                },
            },
        }),
        ExprKind::IsTruthValue {
            expr: inner,
            value,
            negated,
        } => Some(TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::IsTruthValue {
                expr: Box::new(rewrite_predicate_through_project(inner, proj)?),
                value: *value,
                negated: *negated,
            },
        }),
        ExprKind::Nested(inner) => Some(TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::Nested(Box::new(rewrite_predicate_through_project(inner, proj)?)),
        }),
        ExprKind::WindowCall {
            name,
            args,
            distinct,
            partition_by,
            order_by,
            window_frame,
            ignore_nulls,
        } => Some(TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::WindowCall {
                name: name.clone(),
                args: rewrite_expr_list_through_project(args, proj)?,
                distinct: *distinct,
                partition_by: rewrite_expr_list_through_project(partition_by, proj)?,
                order_by: order_by
                    .iter()
                    .map(|item| {
                        rewrite_predicate_through_project(&item.expr, proj).map(|expr| SortItem {
                            expr,
                            asc: item.asc,
                            nulls_first: item.nulls_first,
                        })
                    })
                    .collect::<Option<Vec<_>>>()?,
                window_frame: window_frame.clone(),
                ignore_nulls: *ignore_nulls,
            },
        }),
        ExprKind::SubqueryPlaceholder { .. } => Some(expr.clone()),
        ExprKind::Lambda { params, body } => Some(TypedExpr {
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
            kind: ExprKind::Lambda {
                params: params.clone(),
                body: Box::new(rewrite_predicate_through_project(body, proj)?),
            },
        }),
    }
}

fn rewrite_expr_list_through_project(
    exprs: &[TypedExpr],
    proj: &ProjectNode,
) -> Option<Vec<TypedExpr>> {
    exprs
        .iter()
        .map(|expr| rewrite_predicate_through_project(expr, proj))
        .collect()
}

fn lookup_passthrough_projection(
    column_id: ColumnId,
    qualifier: Option<&str>,
    column: &str,
    proj: &ProjectNode,
) -> Option<TypedExpr> {
    for item in &proj.items {
        if !matches!(item.expr.kind, ExprKind::ColumnRef { .. }) {
            continue;
        }
        if column_id != ColumnId::UNSET && item.output_column_id == column_id {
            return Some(item.expr.clone());
        }
        if let Some(ref output_qualifier) = proj.output_qualifier
            && !qualifier
                .map(|q| q.eq_ignore_ascii_case(output_qualifier))
                .unwrap_or(true)
        {
            continue;
        }
        if item.output_name.eq_ignore_ascii_case(column) {
            return Some(item.expr.clone());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{
        BinOp, ExprKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr,
    };
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use arrow::datatypes::DataType;

    fn col(name: &str) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Int64,
            nullable: true,
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::UNSET,
                qualifier: None,
                column: name.into(),
            },
        }
    }

    fn qualified_col(qualifier: &str, name: &str) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Int64,
            nullable: true,
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::UNSET,
                qualifier: Some(qualifier.into()),
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

    fn is_not_null(expr: TypedExpr) -> TypedExpr {
        TypedExpr {
            data_type: DataType::Boolean,
            nullable: false,
            kind: ExprKind::IsNull {
                expr: Box::new(expr),
                negated: true,
            },
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
            alias: None,
            columns: cols
                .iter()
                .map(|n| OutputColumn {
                    column_id: ColumnId::UNSET,
                    name: (*n).into(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: false,
                })
                .collect(),
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            required_output_columns: None,
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
                    output_column_id: crate::sql::column_id::ColumnId::UNSET,
                })
                .collect(),
            output_qualifier: None,
            required_output_columns: None,
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
            required_output_columns: None,
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

    #[test]
    fn rewrites_qualified_alias_predicate_before_pushdown() {
        let scan = scan_with_cols(&["item_sk"]);
        let mut project = passthrough_project(&["item_sk"], scan);
        if let LogicalPlan::Project(ref mut p) = project {
            p.output_qualifier = Some("asceding".into());
        }
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(project),
            predicate: is_not_null(qualified_col("asceding", "item_sk")),
            required_output_columns: None,
        });

        let rule = PushDownPredicateProject;
        let out = rule.apply(filter).expect("should rewrite");

        let LogicalPlan::Project(project) = out else {
            panic!("expected Project at top");
        };
        let LogicalPlan::Filter(inner_filter) = *project.input else {
            panic!("expected pushed Filter below Project");
        };
        let ExprKind::IsNull { expr, negated } = inner_filter.predicate.kind else {
            panic!("expected pushed IS NOT NULL predicate");
        };
        assert!(negated);
        let ExprKind::ColumnRef {
            qualifier, column, ..
        } = expr.kind
        else {
            panic!("expected pushed predicate to reference the Project input column");
        };
        assert_eq!(qualifier, None);
        assert_eq!(column, "item_sk");
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
                output_column_id: crate::sql::column_id::ColumnId::UNSET,
            }],
            output_qualifier: None,
            required_output_columns: None,
        });
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(project),
            predicate: eq(col("x"), int_lit(5)),
            required_output_columns: None,
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
            required_output_columns: None,
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
                    output_column_id: crate::sql::column_id::ColumnId::UNSET,
                },
                ProjectItem {
                    expr: computed_expr,
                    output_name: "x".into(),
                    output_column_id: crate::sql::column_id::ColumnId::UNSET,
                },
            ],
            output_qualifier: None,
            required_output_columns: None,
        });
        // Filter: a=1 AND x=5
        let pred = and(eq(col("a"), int_lit(1)), eq(col("x"), int_lit(5)));
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(project),
            predicate: pred,
            required_output_columns: None,
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
