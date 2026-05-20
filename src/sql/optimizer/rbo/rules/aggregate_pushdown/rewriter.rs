//! Aggregate pushdown rewriter — phase 2 of the rule.

use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
use crate::sql::planner::plan::{AggregateCall, AggregateNode, LogicalPlan};

use super::context::PushPlan;

const PARTIAL_OUTPUT_PREFIX: &str = "__nr_agg_pd_";

/// Construct the final LogicalPlan: a top-level Aggregate (with
/// already_pushed=true) whose input is the original Join with one side
/// wrapped by a partial Aggregate.
pub(crate) fn rewrite(original: &AggregateNode, plan: PushPlan) -> LogicalPlan {
    // Capture the side before plan is consumed by the moves below.
    let plan_side = plan.side;

    // 1. Build partial AggregateCalls. For SUM/MIN/MAX function name is
    //    unchanged at the partial stage; for COUNT it stays COUNT at
    //    partial and becomes SUM at final.
    let partial_calls: Vec<AggregateCall> = plan
        .partial_aggregates
        .iter()
        .map(|c| AggregateCall {
            name: partial_fn_name(&c.name),
            args: c.args.clone(),
            distinct: false,
            result_type: c.result_type.clone(),
            order_by: vec![],
        })
        .collect();

    // 2. Synthetic output columns for each partial call.
    let partial_output_cols: Vec<OutputColumn> = partial_calls
        .iter()
        .enumerate()
        .map(|(i, call)| OutputColumn {
            name: format!("{}{}", PARTIAL_OUTPUT_PREFIX, i),
            data_type: call.result_type.clone(),
            nullable: true,
        })
        .collect();

    // 3. Partial group-by output columns (column-ref pass-through).
    let partial_groupby_outputs: Vec<OutputColumn> = plan
        .partial_groupby
        .iter()
        .filter_map(|gb| match &gb.kind {
            ExprKind::ColumnRef { column, .. } => Some(OutputColumn {
                name: column.clone(),
                data_type: gb.data_type.clone(),
                nullable: gb.nullable,
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
                    qualifier: None,
                    column: pc.name.clone(),
                },
                data_type: pc.data_type.clone(),
                nullable: pc.nullable,
            }],
            distinct: false,
            result_type: orig.result_type.clone(),
            order_by: orig.order_by.clone(),
        })
        .collect();

    LogicalPlan::Aggregate(AggregateNode {
        input: Box::new(new_input),
        group_by: original.group_by.clone(),
        aggregates: final_aggs,
        output_columns: original.output_columns.clone(),
        already_pushed: true,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{BinOp, JoinKind, OutputColumn};
    use crate::sql::catalog::{TableDef, TableStorage};
    use crate::sql::planner::plan::*;
    use arrow::datatypes::DataType;

    fn col_ref(name: &str, ty: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: name.into(),
            },
            data_type: ty,
            nullable: true,
        }
    }

    fn scan(name: &str, cols: &[(&str, DataType)]) -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "db".into(),
            table: TableDef {
                name: name.into(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                iceberg_table: None,
                storage: TableStorage::LocalParquetFile {
                    path: std::path::PathBuf::from("/tmp/t.parquet"),
                },
            },
            alias: None,
            columns: cols
                .iter()
                .map(|(n, ty)| OutputColumn {
                    name: (*n).into(),
                    data_type: ty.clone(),
                    nullable: false,
                })
                .collect(),
            predicates: vec![],
            required_columns: None,
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

    #[test]
    fn rewrites_count_to_sum_at_final() {
        let a = scan("a", &[("k", DataType::Int64), ("v", DataType::Int64)]);
        let b = scan("b", &[("k", DataType::Int64)]);
        let join = LogicalPlan::Join(JoinNode {
            left: Box::new(a),
            right: Box::new(b),
            join_type: JoinKind::Inner,
            condition: Some(eq("k", "k")),
        });
        let count_call = AggregateCall {
            name: "count".into(),
            args: vec![col_ref("v", DataType::Int64)],
            distinct: false,
            result_type: DataType::Int64,
            order_by: vec![],
        };
        let original = AggregateNode {
            input: Box::new(join),
            group_by: vec![col_ref("k", DataType::Int64)],
            aggregates: vec![count_call],
            output_columns: vec![OutputColumn {
                name: "k".into(),
                data_type: DataType::Int64,
                nullable: true,
            }],
            already_pushed: false,
        };
        let push = PushPlan {
            side: super::super::context::Side::Left,
            target_subtree: scan("a", &[("k", DataType::Int64), ("v", DataType::Int64)]),
            partial_groupby: vec![col_ref("k", DataType::Int64)],
            partial_aggregates: original.aggregates.clone(),
        };
        let out = rewrite(&original, push);
        let LogicalPlan::Aggregate(top) = out else {
            panic!("top must be Aggregate")
        };
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
            }],
            output_columns: vec![],
            already_pushed: false,
        };
        let push = PushPlan {
            side: super::super::context::Side::Left,
            target_subtree: scan("a", &[("k", DataType::Int64), ("v", DataType::Int64)]),
            partial_groupby: vec![col_ref("k", DataType::Int64)],
            partial_aggregates: original.aggregates.clone(),
        };
        let out = rewrite(&original, push);
        let LogicalPlan::Aggregate(top) = out else {
            panic!()
        };
        assert_eq!(top.aggregates[0].name, "sum");
        match &top.aggregates[0].args[0].kind {
            ExprKind::ColumnRef { column, .. } => {
                assert!(
                    column.starts_with("__nr_agg_pd_"),
                    "expected partial column prefix, got: {column}"
                );
            }
            _ => panic!("final SUM arg must be a ColumnRef"),
        }
    }

    #[test]
    fn rewriter_output_preserves_top_output_columns() {
        let a = scan("a", &[("k", DataType::Int64), ("v", DataType::Int64)]);
        let b = scan("b", &[("k", DataType::Int64)]);
        let join = LogicalPlan::Join(JoinNode {
            left: Box::new(a.clone()),
            right: Box::new(b),
            join_type: JoinKind::Inner,
            condition: Some(eq("k", "k")),
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
            }],
            output_columns: vec![
                OutputColumn {
                    name: "k".into(),
                    data_type: DataType::Int64,
                    nullable: true,
                },
                OutputColumn {
                    name: "total".into(),
                    data_type: DataType::Int64,
                    nullable: true,
                },
            ],
            already_pushed: false,
        };
        let push = PushPlan {
            side: super::super::context::Side::Left,
            target_subtree: a,
            partial_groupby: original.group_by.clone(),
            partial_aggregates: original.aggregates.clone(),
        };
        let out = rewrite(&original, push);
        let LogicalPlan::Aggregate(top) = out else {
            panic!()
        };
        assert_eq!(top.output_columns.len(), 2);
        assert_eq!(top.output_columns[0].name, "k");
        assert_eq!(top.output_columns[1].name, "total");
    }
}
