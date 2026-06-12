//! Aggregate pushdown rewriter — phase 2 of the rule.

use crate::sql::analysis::{ExprKind, OutputColumn, ProjectItem, TypedExpr};
use crate::sql::codegen::helpers::{agg_call_display_name, typed_expr_display_name};
use crate::sql::column_id::{ColumnId, ColumnRefFactory};
use crate::sql::planner::plan::{AggregateCall, AggregateNode, LogicalPlan, ProjectNode};

use super::context::PushPlan;

/// Construct the final LogicalPlan: a top-level Aggregate (with
/// already_pushed=true) whose input is the original Join with one side
/// wrapped by a partial Aggregate.
pub(crate) fn rewrite(
    original: &AggregateNode,
    plan: PushPlan,
    column_ref_factory: &mut ColumnRefFactory,
) -> LogicalPlan {
    // Capture the side before plan is consumed by the moves below.
    let plan_side = plan.side;

    // 1. Build partial AggregateCalls. For SUM/MIN/MAX function name is
    //    unchanged at the partial stage; for COUNT it stays COUNT at
    //    partial and becomes SUM at final.
    let partial_calls: Vec<AggregateCall> = plan
        .partial_aggregates
        .iter()
        .map(|c| {
            let mut call = AggregateCall {
                name: partial_fn_name(&c.name),
                args: c.args.clone(),
                distinct: false,
                result_type: c.result_type.clone(),
                order_by: vec![],
                output_column_id: ColumnId::UNSET,
            };
            let name = agg_call_display_name(&call);
            call.output_column_id =
                column_ref_factory.create(None, name, call.result_type.clone(), true);
            call
        })
        .collect();

    // 2. Synthetic output columns for each partial call.
    let partial_output_cols: Vec<OutputColumn> = partial_calls
        .iter()
        .map(|call| OutputColumn {
            column_id: call.output_column_id,
            name: agg_call_display_name(call),
            data_type: call.result_type.clone(),
            nullable: true,
            is_internal: false,
        })
        .collect();

    // 3. Partial group-by output columns (column-ref pass-through).
    let partial_groupby_outputs: Vec<OutputColumn> = plan
        .partial_groupby
        .iter()
        .filter_map(|gb| match &gb.kind {
            ExprKind::ColumnRef {
                column_id, column, ..
            } => Some(OutputColumn {
                column_id: *column_id,
                name: column.clone(),
                data_type: gb.data_type.clone(),
                nullable: gb.nullable,
                is_internal: false,
            }),
            _ => None,
        })
        .collect();

    let mut partial_outputs = partial_groupby_outputs;
    partial_outputs.extend(partial_output_cols.clone());

    let partial_aggregate = AggregateNode {
        input: Box::new(plan.target_subtree),
        group_by: plan.partial_groupby,
        aggregates: partial_calls,
        output_columns: partial_outputs,
        already_pushed: false, // partial isn't itself a final
        required_output_columns: None,
    };

    // 4. Splice partial into the chosen side of the join. v1 invariant
    //    (enforced by the collector): original.input is a Join, and
    //    PushPlan.side identifies which side gets wrapped.
    let new_input = {
        let mut join = match (*original.input).clone() {
            LogicalPlan::Join(j) => j,
            _ => unreachable!("collector guarantees original.input is a Join"),
        };
        let wrapped = Box::new(LogicalPlan::Aggregate(partial_aggregate));
        match plan_side {
            super::context::Side::Left => join.left = wrapped,
            super::context::Side::Right => join.right = wrapped,
        }
        LogicalPlan::Join(join)
    };

    // 5. Rewrite top-level aggregate calls to reference partial outputs.
    let final_aggs: Vec<AggregateCall> = original
        .aggregates
        .iter()
        .zip(partial_output_cols.iter())
        .map(|(orig, pc)| AggregateCall {
            name: final_fn_name(&orig.name),
            args: vec![TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: pc.column_id,
                    qualifier: None,
                    column: pc.name.clone(),
                },
                data_type: pc.data_type.clone(),
                nullable: pc.nullable,
            }],
            distinct: false,
            result_type: orig.result_type.clone(),
            order_by: orig.order_by.clone(),
            output_column_id: orig.output_column_id,
        })
        .collect();

    let final_aggregate = LogicalPlan::Aggregate(AggregateNode {
        input: Box::new(new_input),
        group_by: original.group_by.clone(),
        aggregates: final_aggs.clone(),
        output_columns: final_aggregate_output_columns(original, &final_aggs),
        already_pushed: true,
        required_output_columns: None,
    });

    LogicalPlan::Project(ProjectNode {
        input: Box::new(final_aggregate),
        items: exposure_project_items(original, &final_aggs),
        output_qualifier: None,
        required_output_columns: None,
    })
}

fn partial_fn_name(name: &str) -> String {
    name.to_ascii_lowercase()
}

fn final_fn_name(name: &str) -> String {
    match name.to_ascii_lowercase().as_str() {
        "count" => "sum".to_string(),
        other => other.to_string(),
    }
}

fn final_aggregate_output_columns(
    original: &AggregateNode,
    final_aggs: &[AggregateCall],
) -> Vec<OutputColumn> {
    let mut output_columns = original
        .group_by
        .iter()
        .map(group_by_output_column)
        .collect::<Vec<_>>();
    output_columns.extend(final_aggs.iter().map(|call| OutputColumn {
        column_id: ColumnId::UNSET,
        name: agg_call_display_name(call),
        data_type: call.result_type.clone(),
        nullable: true,
        is_internal: false,
    }));
    output_columns
}

fn group_by_output_column(expr: &TypedExpr) -> OutputColumn {
    let column_id = match &expr.kind {
        ExprKind::ColumnRef { column_id, .. } => *column_id,
        _ => ColumnId::UNSET,
    };
    OutputColumn {
        column_id,
        name: typed_expr_display_name(expr),
        data_type: expr.data_type.clone(),
        nullable: expr.nullable,
        is_internal: false,
    }
}

fn exposure_project_items(
    original: &AggregateNode,
    final_aggs: &[AggregateCall],
) -> Vec<ProjectItem> {
    let mut items = original
        .group_by
        .iter()
        .map(group_by_project_item)
        .collect::<Vec<_>>();
    items.extend(original.aggregates.iter().zip(final_aggs.iter()).map(
        |(original_call, final_call)| ProjectItem {
            // The final Aggregate already computes this aggregate (e.g.
            // sum(partial)); expose its result as a ColumnRef to the final
            // aggregate's output. Repeating the AggregateCall here would
            // reference the partial-input column, which lives below the final
            // aggregate and is not in its output scope ("not produced by child
            // scope"). The output id reuses the original aggregate's id so the
            // SELECT Project above resolves against it.
            expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: final_call.output_column_id,
                    qualifier: None,
                    column: agg_call_display_name(final_call),
                },
                data_type: final_call.result_type.clone(),
                nullable: true,
            },
            output_name: agg_call_display_name(original_call),
            output_column_id: original_call.output_column_id,
        },
    ));
    items
}

fn group_by_project_item(expr: &TypedExpr) -> ProjectItem {
    let output_name = typed_expr_display_name(expr);
    let column_id = match &expr.kind {
        ExprKind::ColumnRef { column_id, .. } => *column_id,
        _ => ColumnId::UNSET,
    };
    ProjectItem {
        expr: TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id,
                qualifier: None,
                column: output_name.clone(),
            },
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
        },
        output_name,
        output_column_id: column_id,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{BinOp, JoinKind, OutputColumn};
    use crate::sql::catalog::{ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};
    use crate::sql::optimizer::rewrite::result::RewriteResult;
    use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
    use crate::sql::planner::plan::*;
    use arrow::datatypes::DataType;

    fn col_ref(name: &str, ty: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::UNSET,
                qualifier: None,
                column: name.into(),
            },
            data_type: ty,
            nullable: true,
        }
    }

    fn scan(name: &str, cols: &[(&str, DataType)]) -> LogicalPlan {
        scan_with_alias_and_ids(
            name,
            None,
            &cols
                .iter()
                .map(|(col, ty)| (*col, ColumnId::UNSET, ty.clone()))
                .collect::<Vec<_>>(),
        )
    }

    fn scan_with_alias_and_ids(
        name: &str,
        alias: Option<&str>,
        cols: &[(&str, ColumnId, DataType)],
    ) -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "db".into(),
            table: TableDef {
                name: name.into(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::StarRocks {
                    db_id: 0,
                    table_id: 0,
                },
            },
            alias: alias.map(str::to_string),
            columns: cols
                .iter()
                .map(|(n, id, ty)| OutputColumn {
                    column_id: *id,
                    name: (*n).into(),
                    data_type: ty.clone(),
                    nullable: false,
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

    fn eq(a: &str, b: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_ref(a, DataType::Int64)),
                op: BinOp::Eq,
                right: Box::new(col_ref(b, DataType::Int64)),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn qualified_col_ref(
        qualifier: &str,
        name: &str,
        column_id: ColumnId,
        ty: DataType,
    ) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id,
                qualifier: Some(qualifier.into()),
                column: name.into(),
            },
            data_type: ty,
            nullable: true,
        }
    }

    fn unwrap_exposure_project(plan: LogicalPlan) -> (Vec<ProjectItem>, AggregateNode) {
        let LogicalPlan::Project(project) = plan else {
            panic!("expected exposure Project")
        };
        let LogicalPlan::Aggregate(aggregate) = *project.input else {
            panic!("expected final Aggregate under exposure Project")
        };
        (project.items, aggregate)
    }

    #[test]
    fn rewrites_count_to_sum_at_final() {
        let a = scan("a", &[("k", DataType::Int64), ("v", DataType::Int64)]);
        let b = scan("b", &[("k", DataType::Int64)]);
        let join = LogicalPlan::Join(JoinNode {
            left: Box::new(a),
            right: Box::new(b),
            join_type: JoinKind::Inner,
            condition: Some(eq("k", "k")),
            required_output_columns: None,
        });
        let count_call = AggregateCall {
            name: "count".into(),
            args: vec![col_ref("v", DataType::Int64)],
            distinct: false,
            result_type: DataType::Int64,
            order_by: vec![],
            output_column_id: ColumnId::UNSET,
        };
        let original = AggregateNode {
            input: Box::new(join),
            group_by: vec![col_ref("k", DataType::Int64)],
            aggregates: vec![count_call],
            output_columns: vec![OutputColumn {
                column_id: ColumnId::UNSET,
                name: "k".into(),
                data_type: DataType::Int64,
                nullable: true,
                is_internal: false,
            }],
            already_pushed: false,
            required_output_columns: None,
        };
        let push = PushPlan {
            side: super::super::context::Side::Left,
            target_subtree: scan("a", &[("k", DataType::Int64), ("v", DataType::Int64)]),
            partial_groupby: vec![col_ref("k", DataType::Int64)],
            partial_aggregates: original.aggregates.clone(),
        };
        let mut factory = ColumnRefFactory::new();
        let out = rewrite(&original, push, &mut factory);
        let (_, top) = unwrap_exposure_project(out);
        assert!(top.already_pushed);
        assert_eq!(top.aggregates[0].name, "sum");
        let LogicalPlan::Join(j) = *top.input else {
            panic!("input must be Join")
        };
        let LogicalPlan::Aggregate(partial) = *j.left else {
            panic!("partial on left")
        };
        assert!(!partial.already_pushed);
        assert_eq!(partial.aggregates[0].name, "count");
    }

    #[test]
    fn rewrites_sum_stays_sum() {
        let a = scan("a", &[("k", DataType::Int64), ("v", DataType::Int64)]);
        let b = scan("b", &[("k", DataType::Int64)]);
        let join = LogicalPlan::Join(JoinNode {
            left: Box::new(a),
            right: Box::new(b),
            join_type: JoinKind::Inner,
            condition: Some(eq("k", "k")),
            required_output_columns: None,
        });
        let original = AggregateNode {
            input: Box::new(join),
            group_by: vec![col_ref("k", DataType::Int64)],
            aggregates: vec![AggregateCall {
                name: "sum".into(),
                args: vec![col_ref("v", DataType::Int64)],
                distinct: false,
                result_type: DataType::Int64,
                order_by: vec![],
                output_column_id: ColumnId::UNSET,
            }],
            output_columns: vec![],
            already_pushed: false,
            required_output_columns: None,
        };
        let push = PushPlan {
            side: super::super::context::Side::Left,
            target_subtree: scan("a", &[("k", DataType::Int64), ("v", DataType::Int64)]),
            partial_groupby: vec![col_ref("k", DataType::Int64)],
            partial_aggregates: original.aggregates.clone(),
        };
        let mut factory = ColumnRefFactory::new();
        let out = rewrite(&original, push, &mut factory);
        let (_, top) = unwrap_exposure_project(out);
        assert_eq!(top.aggregates[0].name, "sum");
        match &top.aggregates[0].args[0].kind {
            ExprKind::ColumnRef { column, .. } => {
                assert_eq!(column, "sum(v)");
            }
            _ => panic!("final SUM arg must be a ColumnRef"),
        }
    }

    #[test]
    fn rewriter_exposure_project_preserves_group_and_original_aggregate_names() {
        let a = scan("a", &[("k", DataType::Int64), ("v", DataType::Int64)]);
        let b = scan("b", &[("k", DataType::Int64)]);
        let join = LogicalPlan::Join(JoinNode {
            left: Box::new(a.clone()),
            right: Box::new(b),
            join_type: JoinKind::Inner,
            condition: Some(eq("k", "k")),
            required_output_columns: None,
        });
        let original = AggregateNode {
            input: Box::new(join),
            group_by: vec![col_ref("k", DataType::Int64)],
            aggregates: vec![AggregateCall {
                name: "sum".into(),
                args: vec![col_ref("v", DataType::Int64)],
                distinct: false,
                result_type: DataType::Int64,
                order_by: vec![],
                output_column_id: ColumnId::UNSET,
            }],
            output_columns: vec![
                OutputColumn {
                    column_id: ColumnId::UNSET,
                    name: "k".into(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: false,
                },
                OutputColumn {
                    column_id: ColumnId::UNSET,
                    name: "total".into(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: false,
                },
            ],
            already_pushed: false,
            required_output_columns: None,
        };
        let push = PushPlan {
            side: super::super::context::Side::Left,
            target_subtree: a,
            partial_groupby: original.group_by.clone(),
            partial_aggregates: original.aggregates.clone(),
        };
        let mut factory = ColumnRefFactory::new();
        let out = rewrite(&original, push, &mut factory);
        let (items, top) = unwrap_exposure_project(out);
        assert_eq!(top.output_columns.len(), 2);
        assert_eq!(top.output_columns[0].name, "k");
        assert_eq!(top.output_columns[1].name, "sum(sum(v))");
        assert!(items.iter().any(|item| item.output_name == "k"));
        assert!(items.iter().any(|item| item.output_name == "sum(v)"));
    }

    #[test]
    fn rewrite_keeps_partial_source_columns_visible_to_required_column_tagging() {
        let mut factory = ColumnRefFactory::new();
        let c_key = factory.create(Some("t1".into()), "c_key".into(), DataType::Int32, false);
        let c_bigint = factory.create(Some("t1".into()), "c_bigint".into(), DataType::Int64, true);
        let c_int = factory.create(Some("t2".into()), "c_int".into(), DataType::Int32, true);
        let sum_out = factory.create(None, "sum(t1.c_key)".into(), DataType::Int64, true);

        let left = scan_with_alias_and_ids(
            "t1",
            Some("t1"),
            &[
                ("c_key", c_key, DataType::Int32),
                ("c_bigint", c_bigint, DataType::Int64),
            ],
        );
        let right = scan_with_alias_and_ids("t2", Some("t2"), &[("c_int", c_int, DataType::Int32)]);
        let original = AggregateNode {
            input: Box::new(LogicalPlan::Join(JoinNode {
                left: Box::new(left.clone()),
                right: Box::new(right),
                join_type: JoinKind::Inner,
                condition: Some(TypedExpr {
                    kind: ExprKind::BinaryOp {
                        left: Box::new(qualified_col_ref(
                            "t1",
                            "c_bigint",
                            c_bigint,
                            DataType::Int64,
                        )),
                        op: BinOp::Eq,
                        right: Box::new(qualified_col_ref("t2", "c_int", c_int, DataType::Int32)),
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                }),
                required_output_columns: None,
            })),
            group_by: vec![qualified_col_ref(
                "t1",
                "c_bigint",
                c_bigint,
                DataType::Int64,
            )],
            aggregates: vec![AggregateCall {
                name: "sum".into(),
                args: vec![qualified_col_ref("t1", "c_key", c_key, DataType::Int32)],
                distinct: false,
                result_type: DataType::Int64,
                order_by: vec![],
                output_column_id: ColumnId::UNSET,
            }],
            output_columns: vec![
                OutputColumn {
                    column_id: sum_out,
                    name: "sum(t1.c_key)".into(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: false,
                },
                OutputColumn {
                    column_id: c_bigint,
                    name: "c_bigint".into(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: false,
                },
            ],
            already_pushed: false,
            required_output_columns: None,
        };
        let push = PushPlan {
            side: super::super::context::Side::Left,
            target_subtree: left,
            partial_groupby: original.group_by.clone(),
            partial_aggregates: original.aggregates.clone(),
        };

        let rewritten = rewrite(&original, push, &mut factory);
        let mut ctx = RewriteContext::new(RewriteConsumer::Query);
        let tagged = match crate::sql::optimizer::rewrite::required_columns::TagRequiredColumns
            .apply(rewritten, &mut ctx)
            .unwrap()
        {
            RewriteResult::Changed(plan) => plan,
            RewriteResult::Unchanged => panic!("tagging should change the plan"),
            RewriteResult::Rejected(_) => panic!("tagging should not reject"),
        };

        let LogicalPlan::Project(project) = tagged else {
            panic!("expected exposure project")
        };
        let LogicalPlan::Aggregate(top) = *project.input else {
            panic!("expected final aggregate")
        };
        let LogicalPlan::Join(join) = *top.input else {
            panic!("expected rewritten join")
        };
        let LogicalPlan::Aggregate(partial) = *join.left else {
            panic!("expected partial aggregate on left")
        };
        let LogicalPlan::Scan(scan) = *partial.input else {
            panic!("expected scan under partial aggregate")
        };
        let required = scan
            .required_output_columns
            .expect("scan should be tagged with required columns");
        assert!(required.contains(&c_key), "partial SUM input must be kept");
        assert!(
            required.contains(&c_bigint),
            "partial group-by key must be kept"
        );
    }

    #[test]
    fn rewrite_exposes_original_count_display_after_final_sum_merge() {
        let mut factory = ColumnRefFactory::new();
        let c_key = factory.create(Some("t1".into()), "c_key".into(), DataType::Int32, false);
        let c_bigint = factory.create(Some("t1".into()), "c_bigint".into(), DataType::Int64, true);
        let c_int = factory.create(Some("t2".into()), "c_int".into(), DataType::Int32, true);

        let left = scan_with_alias_and_ids(
            "t1",
            Some("t1"),
            &[
                ("c_key", c_key, DataType::Int32),
                ("c_bigint", c_bigint, DataType::Int64),
            ],
        );
        let right = scan_with_alias_and_ids("t2", Some("t2"), &[("c_int", c_int, DataType::Int32)]);
        let count_call = AggregateCall {
            name: "count".into(),
            args: vec![qualified_col_ref("t1", "c_key", c_key, DataType::Int32)],
            distinct: false,
            result_type: DataType::Int64,
            order_by: vec![],
            output_column_id: ColumnId::UNSET,
        };
        let original = AggregateNode {
            input: Box::new(LogicalPlan::Join(JoinNode {
                left: Box::new(left.clone()),
                right: Box::new(right),
                join_type: JoinKind::Inner,
                condition: Some(TypedExpr {
                    kind: ExprKind::BinaryOp {
                        left: Box::new(qualified_col_ref(
                            "t1",
                            "c_bigint",
                            c_bigint,
                            DataType::Int64,
                        )),
                        op: BinOp::Eq,
                        right: Box::new(qualified_col_ref("t2", "c_int", c_int, DataType::Int32)),
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                }),
                required_output_columns: None,
            })),
            group_by: vec![qualified_col_ref(
                "t1",
                "c_bigint",
                c_bigint,
                DataType::Int64,
            )],
            aggregates: vec![count_call.clone()],
            output_columns: vec![],
            already_pushed: false,
            required_output_columns: None,
        };
        let push = PushPlan {
            side: super::super::context::Side::Left,
            target_subtree: left,
            partial_groupby: original.group_by.clone(),
            partial_aggregates: original.aggregates.clone(),
        };

        let rewritten = rewrite(&original, push, &mut factory);
        let (items, top) = unwrap_exposure_project(rewritten);
        assert_eq!(top.aggregates[0].name, "sum");
        assert!(
            items
                .iter()
                .any(|item| item.output_name == agg_call_display_name(&count_call))
        );
    }
}
