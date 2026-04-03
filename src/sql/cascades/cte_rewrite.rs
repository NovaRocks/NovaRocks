use crate::sql::cte::CteId;
use crate::sql::plan::*;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, Default)]
pub(crate) struct CTEContext {
    pub produces: HashSet<CteId>,
    pub consume_count: HashMap<CteId, usize>,
}

pub(crate) fn collect_cte_counts(plan: &LogicalPlan) -> CTEContext {
    fn visit(plan: &LogicalPlan, ctx: &mut CTEContext) {
        match plan {
            LogicalPlan::Scan(_) | LogicalPlan::Values(_) | LogicalPlan::GenerateSeries(_) => {}
            LogicalPlan::Filter(node) => visit(&node.input, ctx),
            LogicalPlan::Project(node) => visit(&node.input, ctx),
            LogicalPlan::Aggregate(node) => visit(&node.input, ctx),
            LogicalPlan::Sort(node) => visit(&node.input, ctx),
            LogicalPlan::Limit(node) => visit(&node.input, ctx),
            LogicalPlan::Window(node) => visit(&node.input, ctx),
            LogicalPlan::SubqueryAlias(node) => visit(&node.input, ctx),
            LogicalPlan::Repeat(node) => visit(&node.input, ctx),
            LogicalPlan::Join(node) => {
                visit(&node.left, ctx);
                visit(&node.right, ctx);
            }
            LogicalPlan::Union(node) => {
                for input in &node.inputs {
                    visit(input, ctx);
                }
            }
            LogicalPlan::Intersect(node) => {
                for input in &node.inputs {
                    visit(input, ctx);
                }
            }
            LogicalPlan::Except(node) => {
                for input in &node.inputs {
                    visit(input, ctx);
                }
            }
            LogicalPlan::CTEAnchor(node) => {
                ctx.produces.insert(node.cte_id);
                visit(&node.produce, ctx);
                visit(&node.consumer, ctx);
            }
            LogicalPlan::CTEProduce(node) => {
                // produces.insert is already done in CTEAnchor above.
                visit(&node.input, ctx);
            }
            LogicalPlan::CTEConsume(node) => {
                *ctx.consume_count.entry(node.cte_id).or_insert(0) += 1;
            }
        }
    }

    let mut ctx = CTEContext::default();
    visit(plan, &mut ctx);
    ctx
}

pub(crate) fn inline_single_use_ctes(plan: LogicalPlan, ctx: &CTEContext) -> LogicalPlan {
    match plan {
        LogicalPlan::Scan(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::GenerateSeries(_)
        | LogicalPlan::CTEConsume(_) => plan,
        LogicalPlan::Filter(node) => LogicalPlan::Filter(FilterNode {
            input: Box::new(inline_single_use_ctes(*node.input, ctx)),
            predicate: node.predicate,
        }),
        LogicalPlan::Project(node) => LogicalPlan::Project(ProjectNode {
            input: Box::new(inline_single_use_ctes(*node.input, ctx)),
            items: node.items,
        }),
        LogicalPlan::Aggregate(node) => LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(inline_single_use_ctes(*node.input, ctx)),
            group_by: node.group_by,
            aggregates: node.aggregates,
            output_columns: node.output_columns,
        }),
        LogicalPlan::Join(node) => LogicalPlan::Join(JoinNode {
            left: Box::new(inline_single_use_ctes(*node.left, ctx)),
            right: Box::new(inline_single_use_ctes(*node.right, ctx)),
            join_type: node.join_type,
            condition: node.condition,
        }),
        LogicalPlan::Sort(node) => LogicalPlan::Sort(SortNode {
            input: Box::new(inline_single_use_ctes(*node.input, ctx)),
            items: node.items,
        }),
        LogicalPlan::Limit(node) => LogicalPlan::Limit(LimitNode {
            input: Box::new(inline_single_use_ctes(*node.input, ctx)),
            limit: node.limit,
            offset: node.offset,
        }),
        LogicalPlan::Union(node) => LogicalPlan::Union(UnionNode {
            inputs: node
                .inputs
                .into_iter()
                .map(|input| inline_single_use_ctes(input, ctx))
                .collect(),
            all: node.all,
        }),
        LogicalPlan::Intersect(node) => LogicalPlan::Intersect(IntersectNode {
            inputs: node
                .inputs
                .into_iter()
                .map(|input| inline_single_use_ctes(input, ctx))
                .collect(),
        }),
        LogicalPlan::Except(node) => LogicalPlan::Except(ExceptNode {
            inputs: node
                .inputs
                .into_iter()
                .map(|input| inline_single_use_ctes(input, ctx))
                .collect(),
        }),
        LogicalPlan::Window(node) => LogicalPlan::Window(WindowNode {
            input: Box::new(inline_single_use_ctes(*node.input, ctx)),
            window_exprs: node.window_exprs,
            output_columns: node.output_columns,
        }),
        LogicalPlan::SubqueryAlias(node) => LogicalPlan::SubqueryAlias(SubqueryAliasNode {
            input: Box::new(inline_single_use_ctes(*node.input, ctx)),
            alias: node.alias,
            output_columns: node.output_columns,
        }),
        LogicalPlan::Repeat(node) => LogicalPlan::Repeat(RepeatPlanNode {
            input: Box::new(inline_single_use_ctes(*node.input, ctx)),
            repeat_column_ref_list: node.repeat_column_ref_list,
            grouping_ids: node.grouping_ids,
            all_rollup_columns: node.all_rollup_columns,
            grouping_fn_args: node.grouping_fn_args,
        }),
        LogicalPlan::CTEProduce(node) => LogicalPlan::CTEProduce(CTEProduceNode {
            cte_id: node.cte_id,
            input: Box::new(inline_single_use_ctes(*node.input, ctx)),
            output_columns: node.output_columns,
        }),
        LogicalPlan::CTEAnchor(node) => {
            let produce = inline_single_use_ctes(*node.produce, ctx);
            let consumer = inline_single_use_ctes(*node.consumer, ctx);
            let consume_count = ctx.consume_count.get(&node.cte_id).copied().unwrap_or(0);

            if ctx.produces.contains(&node.cte_id) && consume_count <= 1 {
                let produce_input = match produce {
                    LogicalPlan::CTEProduce(produce_node) if produce_node.cte_id == node.cte_id => {
                        *produce_node.input
                    }
                    other => other,
                };
                replace_cte_consume(consumer, node.cte_id, &produce_input)
            } else {
                LogicalPlan::CTEAnchor(CTEAnchorNode {
                    cte_id: node.cte_id,
                    produce: Box::new(produce),
                    consumer: Box::new(consumer),
                })
            }
        }
    }
}

fn replace_cte_consume(plan: LogicalPlan, cte_id: CteId, replacement: &LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::CTEConsume(node) if node.cte_id == cte_id => {
            LogicalPlan::SubqueryAlias(SubqueryAliasNode {
                input: Box::new(replacement.clone()),
                alias: node.alias,
                output_columns: node.output_columns,
            })
        }
        LogicalPlan::Scan(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::GenerateSeries(_)
        | LogicalPlan::CTEConsume(_) => plan,
        LogicalPlan::Filter(node) => LogicalPlan::Filter(FilterNode {
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)),
            predicate: node.predicate,
        }),
        LogicalPlan::Project(node) => LogicalPlan::Project(ProjectNode {
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)),
            items: node.items,
        }),
        LogicalPlan::Aggregate(node) => LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)),
            group_by: node.group_by,
            aggregates: node.aggregates,
            output_columns: node.output_columns,
        }),
        LogicalPlan::Join(node) => LogicalPlan::Join(JoinNode {
            left: Box::new(replace_cte_consume(*node.left, cte_id, replacement)),
            right: Box::new(replace_cte_consume(*node.right, cte_id, replacement)),
            join_type: node.join_type,
            condition: node.condition,
        }),
        LogicalPlan::Sort(node) => LogicalPlan::Sort(SortNode {
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)),
            items: node.items,
        }),
        LogicalPlan::Limit(node) => LogicalPlan::Limit(LimitNode {
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)),
            limit: node.limit,
            offset: node.offset,
        }),
        LogicalPlan::Union(node) => LogicalPlan::Union(UnionNode {
            inputs: node
                .inputs
                .into_iter()
                .map(|input| replace_cte_consume(input, cte_id, replacement))
                .collect(),
            all: node.all,
        }),
        LogicalPlan::Intersect(node) => LogicalPlan::Intersect(IntersectNode {
            inputs: node
                .inputs
                .into_iter()
                .map(|input| replace_cte_consume(input, cte_id, replacement))
                .collect(),
        }),
        LogicalPlan::Except(node) => LogicalPlan::Except(ExceptNode {
            inputs: node
                .inputs
                .into_iter()
                .map(|input| replace_cte_consume(input, cte_id, replacement))
                .collect(),
        }),
        LogicalPlan::Window(node) => LogicalPlan::Window(WindowNode {
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)),
            window_exprs: node.window_exprs,
            output_columns: node.output_columns,
        }),
        LogicalPlan::SubqueryAlias(node) => LogicalPlan::SubqueryAlias(SubqueryAliasNode {
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)),
            alias: node.alias,
            output_columns: node.output_columns,
        }),
        LogicalPlan::Repeat(node) => LogicalPlan::Repeat(RepeatPlanNode {
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)),
            repeat_column_ref_list: node.repeat_column_ref_list,
            grouping_ids: node.grouping_ids,
            all_rollup_columns: node.all_rollup_columns,
            grouping_fn_args: node.grouping_fn_args,
        }),
        LogicalPlan::CTEProduce(node) => LogicalPlan::CTEProduce(CTEProduceNode {
            cte_id: node.cte_id,
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)),
            output_columns: node.output_columns,
        }),
        LogicalPlan::CTEAnchor(node) => LogicalPlan::CTEAnchor(CTEAnchorNode {
            cte_id: node.cte_id,
            produce: Box::new(replace_cte_consume(*node.produce, cte_id, replacement)),
            consumer: Box::new(replace_cte_consume(*node.consumer, cte_id, replacement)),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::catalog::{ColumnDef, TableDef, TableStorage};
    use crate::sql::ir::OutputColumn;
    use arrow::datatypes::DataType;
    use std::path::PathBuf;

    fn scan_plan() -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: "t1".to_string(),
                columns: vec![ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                }],
                storage: TableStorage::LocalParquetFile {
                    path: PathBuf::from("/tmp/t1.parquet"),
                },
            },
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

    fn output_columns() -> Vec<OutputColumn> {
        vec![OutputColumn {
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
        }]
    }

    fn consume_plan(cte_id: CteId, alias: &str) -> LogicalPlan {
        LogicalPlan::CTEConsume(CTEConsumeNode {
            cte_id,
            alias: alias.to_string(),
            output_columns: output_columns(),
        })
    }

    fn assert_output_columns_match(columns: &[OutputColumn]) {
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[0].data_type, DataType::Int32);
        assert!(!columns[0].nullable);
    }

    #[test]
    fn test_collect_cte_counts_counts_consumes() {
        let plan = LogicalPlan::CTEAnchor(CTEAnchorNode {
            cte_id: 1,
            produce: Box::new(LogicalPlan::CTEProduce(CTEProduceNode {
                cte_id: 1,
                input: Box::new(scan_plan()),
                output_columns: output_columns(),
            })),
            consumer: Box::new(LogicalPlan::CTEConsume(CTEConsumeNode {
                cte_id: 1,
                alias: "t".to_string(),
                output_columns: output_columns(),
            })),
        });

        let ctx = collect_cte_counts(&plan);
        assert!(ctx.produces.contains(&1));
        assert_eq!(ctx.consume_count.get(&1), Some(&1));
    }

    #[test]
    fn test_inline_single_use_cte_removes_anchor() {
        let plan = LogicalPlan::CTEAnchor(CTEAnchorNode {
            cte_id: 1,
            produce: Box::new(LogicalPlan::CTEProduce(CTEProduceNode {
                cte_id: 1,
                input: Box::new(scan_plan()),
                output_columns: output_columns(),
            })),
            consumer: Box::new(consume_plan(1, "t")),
        });

        let ctx = collect_cte_counts(&plan);
        let rewritten = inline_single_use_ctes(plan, &ctx);
        assert!(matches!(rewritten, LogicalPlan::SubqueryAlias(_)));
    }

    #[test]
    fn test_inline_single_use_cte_preserves_alias_namespace() {
        let plan = LogicalPlan::CTEAnchor(CTEAnchorNode {
            cte_id: 1,
            produce: Box::new(LogicalPlan::CTEProduce(CTEProduceNode {
                cte_id: 1,
                input: Box::new(scan_plan()),
                output_columns: output_columns(),
            })),
            consumer: Box::new(consume_plan(1, "x")),
        });

        let ctx = collect_cte_counts(&plan);
        let rewritten = inline_single_use_ctes(plan, &ctx);

        match rewritten {
            LogicalPlan::SubqueryAlias(node) => {
                assert_eq!(node.alias, "x");
                assert_output_columns_match(&node.output_columns);
                assert!(matches!(*node.input, LogicalPlan::Scan(_)));
            }
            other => panic!("expected SubqueryAlias, got {other:?}"),
        }
    }

    #[test]
    fn test_inline_single_use_cte_keeps_multi_use_anchor() {
        let plan = LogicalPlan::CTEAnchor(CTEAnchorNode {
            cte_id: 1,
            produce: Box::new(LogicalPlan::CTEProduce(CTEProduceNode {
                cte_id: 1,
                input: Box::new(scan_plan()),
                output_columns: output_columns(),
            })),
            consumer: Box::new(LogicalPlan::Union(UnionNode {
                inputs: vec![consume_plan(1, "t1"), consume_plan(1, "t2")],
                all: true,
            })),
        });

        let ctx = collect_cte_counts(&plan);
        assert_eq!(ctx.consume_count.get(&1), Some(&2));

        let rewritten = inline_single_use_ctes(plan, &ctx);
        assert!(matches!(rewritten, LogicalPlan::CTEAnchor(_)));
    }

    #[test]
    fn test_inline_single_use_cte_inlines_nested_cte_inside_later_produce() {
        let plan = LogicalPlan::CTEAnchor(CTEAnchorNode {
            cte_id: 1,
            produce: Box::new(LogicalPlan::CTEProduce(CTEProduceNode {
                cte_id: 1,
                input: Box::new(scan_plan()),
                output_columns: output_columns(),
            })),
            consumer: Box::new(LogicalPlan::CTEAnchor(CTEAnchorNode {
                cte_id: 2,
                produce: Box::new(LogicalPlan::CTEProduce(CTEProduceNode {
                    cte_id: 2,
                    input: Box::new(LogicalPlan::CTEAnchor(CTEAnchorNode {
                        cte_id: 1,
                        produce: Box::new(LogicalPlan::CTEProduce(CTEProduceNode {
                            cte_id: 1,
                            input: Box::new(scan_plan()),
                            output_columns: output_columns(),
                        })),
                        consumer: Box::new(consume_plan(1, "a")),
                    })),
                    output_columns: output_columns(),
                })),
                consumer: Box::new(LogicalPlan::Union(UnionNode {
                    inputs: vec![consume_plan(2, "b1"), consume_plan(2, "b2")],
                    all: true,
                })),
            })),
        });

        let ctx = collect_cte_counts(&plan);
        assert_eq!(ctx.consume_count.get(&1), Some(&1));
        assert_eq!(ctx.consume_count.get(&2), Some(&2));

        let rewritten = inline_single_use_ctes(plan, &ctx);

        match rewritten {
            LogicalPlan::CTEAnchor(anchor) => {
                assert_eq!(anchor.cte_id, 2);
                match *anchor.produce {
                    LogicalPlan::CTEProduce(produce) => match *produce.input {
                        LogicalPlan::SubqueryAlias(alias) => {
                            assert_eq!(alias.alias, "a");
                            assert!(matches!(*alias.input, LogicalPlan::Scan(_)));
                        }
                        other => panic!("expected nested alias replacement, got {other:?}"),
                    },
                    other => panic!("expected CTEProduce for b, got {other:?}"),
                }
                assert!(matches!(*anchor.consumer, LogicalPlan::Union(_)));
            }
            other => panic!("expected surviving anchor for b, got {other:?}"),
        }
    }

    #[test]
    fn test_replace_cte_consume_only_rewrites_targeted_cte_id() {
        let plan = LogicalPlan::CTEAnchor(CTEAnchorNode {
            cte_id: 2,
            produce: Box::new(LogicalPlan::CTEProduce(CTEProduceNode {
                cte_id: 2,
                input: Box::new(scan_plan()),
                output_columns: output_columns(),
            })),
            consumer: Box::new(LogicalPlan::Union(UnionNode {
                inputs: vec![consume_plan(1, "target"), consume_plan(2, "shadow")],
                all: true,
            })),
        });

        let rewritten = replace_cte_consume(plan, 1, &scan_plan());

        match rewritten {
            LogicalPlan::CTEAnchor(anchor) => match *anchor.consumer {
                LogicalPlan::Union(union) => {
                    match &union.inputs[0] {
                        LogicalPlan::SubqueryAlias(alias) => {
                            assert_eq!(alias.alias, "target");
                            assert!(matches!(*alias.input.clone(), LogicalPlan::Scan(_)));
                        }
                        other => panic!("expected targeted consume to be rewritten, got {other:?}"),
                    }
                    assert!(matches!(union.inputs[1], LogicalPlan::CTEConsume(_)));
                }
                other => panic!("expected union consumer, got {other:?}"),
            },
            other => panic!("expected outer anchor, got {other:?}"),
        }
    }
}
