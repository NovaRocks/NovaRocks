use crate::sql::analysis::cte::CteId;
use crate::sql::planner::plan::*;
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
            LogicalPlan::TableFunction(node) => visit(&node.input, ctx),
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
            LogicalPlan::Decode(node) => visit(&node.input, ctx),
            LogicalPlan::ImvDelta(_) | LogicalPlan::ImvVersion(_) => {
                panic!("imv marker leaked into non-IMV plan");
            }
        }
    }

    let mut ctx = CTEContext::default();
    visit(plan, &mut ctx);
    ctx
}

pub(crate) fn inline_single_use_ctes(
    plan: LogicalPlan,
    ctx: &CTEContext,
) -> Result<LogicalPlan, String> {
    match plan {
        LogicalPlan::Scan(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::GenerateSeries(_)
        | LogicalPlan::CTEConsume(_) => Ok(plan),
        LogicalPlan::TableFunction(node) => Ok(LogicalPlan::TableFunction(TableFunctionNode {
            input: Box::new(inline_single_use_ctes(*node.input, ctx)?),
            function_name: node.function_name,
            args: node.args,
            output_columns: node.output_columns,
            alias: node.alias,
            is_left_join: node.is_left_join,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::Filter(node) => Ok(LogicalPlan::Filter(FilterNode {
            input: Box::new(inline_single_use_ctes(*node.input, ctx)?),
            predicate: node.predicate,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::Project(node) => Ok(LogicalPlan::Project(ProjectNode {
            input: Box::new(inline_single_use_ctes(*node.input, ctx)?),
            items: node.items,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::Aggregate(node) => Ok(LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(inline_single_use_ctes(*node.input, ctx)?),
            group_by: node.group_by,
            aggregates: node.aggregates,
            output_columns: node.output_columns,
            already_pushed: node.already_pushed,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::Join(node) => Ok(LogicalPlan::Join(JoinNode {
            left: Box::new(inline_single_use_ctes(*node.left, ctx)?),
            right: Box::new(inline_single_use_ctes(*node.right, ctx)?),
            join_type: node.join_type,
            condition: node.condition,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::Sort(node) => Ok(LogicalPlan::Sort(SortNode {
            input: Box::new(inline_single_use_ctes(*node.input, ctx)?),
            items: node.items,
            analytic_partition_by: node.analytic_partition_by,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::Limit(node) => Ok(LogicalPlan::Limit(LimitNode {
            input: Box::new(inline_single_use_ctes(*node.input, ctx)?),
            limit: node.limit,
            offset: node.offset,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::Union(node) => Ok(LogicalPlan::Union(UnionNode {
            inputs: node
                .inputs
                .into_iter()
                .map(|input| inline_single_use_ctes(input, ctx))
                .collect::<Result<Vec<_>, _>>()?,
            all: node.all,
            output_columns: node.output_columns,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::Intersect(node) => Ok(LogicalPlan::Intersect(IntersectNode {
            inputs: node
                .inputs
                .into_iter()
                .map(|input| inline_single_use_ctes(input, ctx))
                .collect::<Result<Vec<_>, _>>()?,
            output_columns: node.output_columns,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::Except(node) => Ok(LogicalPlan::Except(ExceptNode {
            inputs: node
                .inputs
                .into_iter()
                .map(|input| inline_single_use_ctes(input, ctx))
                .collect::<Result<Vec<_>, _>>()?,
            output_columns: node.output_columns,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::Window(node) => Ok(LogicalPlan::Window(WindowNode {
            input: Box::new(inline_single_use_ctes(*node.input, ctx)?),
            window_exprs: node.window_exprs,
            output_columns: node.output_columns,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::Repeat(node) => Ok(LogicalPlan::Repeat(RepeatPlanNode {
            input: Box::new(inline_single_use_ctes(*node.input, ctx)?),
            repeat_column_ref_list: node.repeat_column_ref_list,
            grouping_ids: node.grouping_ids,
            all_rollup_columns: node.all_rollup_columns,
            grouping_key_aliases: node.grouping_key_aliases,
            grouping_fn_args: node.grouping_fn_args,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::CTEProduce(node) => Ok(LogicalPlan::CTEProduce(CTEProduceNode {
            cte_id: node.cte_id,
            input: Box::new(inline_single_use_ctes(*node.input, ctx)?),
            output_columns: node.output_columns,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::CTEAnchor(node) => {
            let produce = inline_single_use_ctes(*node.produce, ctx)?;
            let consumer = inline_single_use_ctes(*node.consumer, ctx)?;
            let consume_count = ctx.consume_count.get(&node.cte_id).copied().unwrap_or(0);

            // Inline single-use CTEs. Multi-consume CTEs use the CTE
            // Produce/Consume path with MultiCast exchange.
            if ctx.produces.contains(&node.cte_id) && consume_count <= 1 {
                let produce_input = match produce {
                    LogicalPlan::CTEProduce(produce_node) if produce_node.cte_id == node.cte_id => {
                        *produce_node.input
                    }
                    other => other,
                };
                replace_cte_consume(consumer, node.cte_id, &produce_input)
            } else {
                Ok(LogicalPlan::CTEAnchor(CTEAnchorNode {
                    cte_id: node.cte_id,
                    produce: Box::new(produce),
                    consumer: Box::new(consumer),
                    required_output_columns: node.required_output_columns,
                }))
            }
        }
        LogicalPlan::Decode(node) => Ok(LogicalPlan::Decode(DecodeNode {
            input: Box::new(inline_single_use_ctes(*node.input, ctx)?),
            mappings: node.mappings,
            output_columns: node.output_columns,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::ImvDelta(_) | LogicalPlan::ImvVersion(_) => {
            panic!("imv marker leaked into non-IMV plan");
        }
    }
}

fn replace_cte_consume(
    plan: LogicalPlan,
    cte_id: CteId,
    replacement: &LogicalPlan,
) -> Result<LogicalPlan, String> {
    match plan {
        LogicalPlan::CTEConsume(node) if node.cte_id == cte_id => {
            crate::sql::planner::adapt_plan_output(replacement.clone(), &node.output_columns)
        }
        LogicalPlan::Scan(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::GenerateSeries(_)
        | LogicalPlan::CTEConsume(_) => Ok(plan),
        LogicalPlan::TableFunction(node) => Ok(LogicalPlan::TableFunction(TableFunctionNode {
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)?),
            function_name: node.function_name,
            args: node.args,
            output_columns: node.output_columns,
            alias: node.alias,
            is_left_join: node.is_left_join,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::Filter(node) => Ok(LogicalPlan::Filter(FilterNode {
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)?),
            predicate: node.predicate,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::Project(node) => Ok(LogicalPlan::Project(ProjectNode {
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)?),
            items: node.items,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::Aggregate(node) => Ok(LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)?),
            group_by: node.group_by,
            aggregates: node.aggregates,
            output_columns: node.output_columns,
            already_pushed: node.already_pushed,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::Join(node) => Ok(LogicalPlan::Join(JoinNode {
            left: Box::new(replace_cte_consume(*node.left, cte_id, replacement)?),
            right: Box::new(replace_cte_consume(*node.right, cte_id, replacement)?),
            join_type: node.join_type,
            condition: node.condition,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::Sort(node) => Ok(LogicalPlan::Sort(SortNode {
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)?),
            items: node.items,
            analytic_partition_by: node.analytic_partition_by,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::Limit(node) => Ok(LogicalPlan::Limit(LimitNode {
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)?),
            limit: node.limit,
            offset: node.offset,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::Union(node) => Ok(LogicalPlan::Union(UnionNode {
            inputs: node
                .inputs
                .into_iter()
                .map(|input| replace_cte_consume(input, cte_id, replacement))
                .collect::<Result<Vec<_>, _>>()?,
            all: node.all,
            output_columns: node.output_columns,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::Intersect(node) => Ok(LogicalPlan::Intersect(IntersectNode {
            inputs: node
                .inputs
                .into_iter()
                .map(|input| replace_cte_consume(input, cte_id, replacement))
                .collect::<Result<Vec<_>, _>>()?,
            output_columns: node.output_columns,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::Except(node) => Ok(LogicalPlan::Except(ExceptNode {
            inputs: node
                .inputs
                .into_iter()
                .map(|input| replace_cte_consume(input, cte_id, replacement))
                .collect::<Result<Vec<_>, _>>()?,
            output_columns: node.output_columns,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::Window(node) => Ok(LogicalPlan::Window(WindowNode {
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)?),
            window_exprs: node.window_exprs,
            output_columns: node.output_columns,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::Repeat(node) => Ok(LogicalPlan::Repeat(RepeatPlanNode {
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)?),
            repeat_column_ref_list: node.repeat_column_ref_list,
            grouping_ids: node.grouping_ids,
            all_rollup_columns: node.all_rollup_columns,
            grouping_key_aliases: node.grouping_key_aliases,
            grouping_fn_args: node.grouping_fn_args,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::CTEProduce(node) => Ok(LogicalPlan::CTEProduce(CTEProduceNode {
            cte_id: node.cte_id,
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)?),
            output_columns: node.output_columns,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::CTEAnchor(node) => Ok(LogicalPlan::CTEAnchor(CTEAnchorNode {
            cte_id: node.cte_id,
            produce: Box::new(replace_cte_consume(*node.produce, cte_id, replacement)?),
            consumer: Box::new(replace_cte_consume(*node.consumer, cte_id, replacement)?),
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::Decode(node) => Ok(LogicalPlan::Decode(DecodeNode {
            input: Box::new(replace_cte_consume(*node.input, cte_id, replacement)?),
            mappings: node.mappings,
            output_columns: node.output_columns,
            required_output_columns: node.required_output_columns,
        })),
        LogicalPlan::ImvDelta(_) | LogicalPlan::ImvVersion(_) => {
            panic!("imv marker leaked into non-IMV plan");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use arrow::datatypes::DataType;

    fn scan_plan() -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: "t1".to_string(),
                columns: vec![ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                }],
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::StarRocks {
                    db_id: 0,
                    table_id: 0,
                },
            },
            alias: None,
            columns: vec![OutputColumn {
                column_id: ColumnId::UNSET,
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                is_internal: false,
            }],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            required_output_columns: None,
        })
    }

    fn output_columns() -> Vec<OutputColumn> {
        vec![OutputColumn {
            column_id: ColumnId::UNSET,
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
            is_internal: false,
        }]
    }

    fn output_columns_with_id_and_name(column_id: ColumnId, name: &str) -> Vec<OutputColumn> {
        vec![OutputColumn {
            column_id,
            name: name.to_string(),
            data_type: DataType::Int32,
            nullable: false,
            is_internal: false,
        }]
    }

    fn consume_plan(cte_id: CteId, alias: &str) -> LogicalPlan {
        LogicalPlan::CTEConsume(CTEConsumeNode {
            cte_id,
            alias: alias.to_string(),
            output_columns: output_columns(),
            required_output_columns: None,
        })
    }

    fn consume_plan_with_output_columns(
        cte_id: CteId,
        alias: &str,
        output_columns: Vec<OutputColumn>,
    ) -> LogicalPlan {
        LogicalPlan::CTEConsume(CTEConsumeNode {
            cte_id,
            alias: alias.to_string(),
            output_columns,
            required_output_columns: None,
        })
    }

    #[test]
    fn test_collect_cte_counts_counts_consumes() {
        let plan = LogicalPlan::CTEAnchor(CTEAnchorNode {
            cte_id: 1,
            produce: Box::new(LogicalPlan::CTEProduce(CTEProduceNode {
                cte_id: 1,
                input: Box::new(scan_plan()),
                output_columns: output_columns(),
                required_output_columns: None,
            })),
            consumer: Box::new(LogicalPlan::CTEConsume(CTEConsumeNode {
                cte_id: 1,
                alias: "t".to_string(),
                output_columns: output_columns(),
                required_output_columns: None,
            })),
            required_output_columns: None,
        });

        let ctx = collect_cte_counts(&plan);
        assert!(ctx.produces.contains(&1));
        assert_eq!(ctx.consume_count.get(&1), Some(&1));
    }

    #[test]
    fn test_inline_single_use_cte_removes_anchor_without_alias_node() {
        let plan = LogicalPlan::CTEAnchor(CTEAnchorNode {
            cte_id: 1,
            produce: Box::new(LogicalPlan::CTEProduce(CTEProduceNode {
                cte_id: 1,
                input: Box::new(scan_plan()),
                output_columns: output_columns(),
                required_output_columns: None,
            })),
            consumer: Box::new(consume_plan(1, "t")),
            required_output_columns: None,
        });

        let ctx = collect_cte_counts(&plan);
        let rewritten = inline_single_use_ctes(plan, &ctx).expect("inline should succeed");
        assert!(matches!(
            rewritten,
            LogicalPlan::Scan(_) | LogicalPlan::Project(_)
        ));
    }

    #[test]
    fn test_inline_single_use_cte_preserves_consumer_output_columns_with_project() {
        let consume_output_id = ColumnId::new_for_test(42);
        let consume_output_columns = output_columns_with_id_and_name(consume_output_id, "x_id");
        let plan = LogicalPlan::CTEAnchor(CTEAnchorNode {
            cte_id: 1,
            produce: Box::new(LogicalPlan::CTEProduce(CTEProduceNode {
                cte_id: 1,
                input: Box::new(scan_plan()),
                output_columns: output_columns(),
                required_output_columns: None,
            })),
            consumer: Box::new(consume_plan_with_output_columns(
                1,
                "x",
                consume_output_columns.clone(),
            )),
            required_output_columns: None,
        });

        let ctx = collect_cte_counts(&plan);
        let rewritten = inline_single_use_ctes(plan, &ctx).expect("inline should succeed");

        let output = crate::sql::planner::plan_output_columns(&rewritten)
            .expect("rewritten output columns should be derivable");
        assert_eq!(output.len(), consume_output_columns.len());
        assert_eq!(output[0].column_id, consume_output_columns[0].column_id);
        assert_eq!(output[0].name, consume_output_columns[0].name);
        assert_eq!(output[0].data_type, consume_output_columns[0].data_type);
        assert_eq!(output[0].nullable, consume_output_columns[0].nullable);
        let LogicalPlan::Project(project) = rewritten else {
            panic!("expected Project adapter");
        };
        assert_eq!(project.items[0].output_name, "x_id");
        assert_eq!(project.items[0].output_column_id, consume_output_id);
    }

    #[test]
    fn test_inline_single_use_cte_keeps_multi_use_anchor() {
        let plan = LogicalPlan::CTEAnchor(CTEAnchorNode {
            cte_id: 1,
            produce: Box::new(LogicalPlan::CTEProduce(CTEProduceNode {
                cte_id: 1,
                input: Box::new(scan_plan()),
                output_columns: output_columns(),
                required_output_columns: None,
            })),
            consumer: Box::new(LogicalPlan::Union(UnionNode {
                inputs: vec![consume_plan(1, "t1"), consume_plan(1, "t2")],
                all: true,
                output_columns: vec![],
                required_output_columns: None,
            })),
            required_output_columns: None,
        });

        let ctx = collect_cte_counts(&plan);
        assert_eq!(ctx.consume_count.get(&1), Some(&2));

        let rewritten = inline_single_use_ctes(plan, &ctx).expect("inline should succeed");
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
                required_output_columns: None,
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
                            required_output_columns: None,
                        })),
                        consumer: Box::new(consume_plan(1, "a")),
                        required_output_columns: None,
                    })),
                    output_columns: output_columns(),
                    required_output_columns: None,
                })),
                consumer: Box::new(LogicalPlan::Union(UnionNode {
                    inputs: vec![consume_plan(2, "b1"), consume_plan(2, "b2")],
                    all: true,
                    output_columns: vec![],
                    required_output_columns: None,
                })),
                required_output_columns: None,
            })),
            required_output_columns: None,
        });

        let ctx = collect_cte_counts(&plan);
        assert_eq!(ctx.consume_count.get(&1), Some(&1));
        assert_eq!(ctx.consume_count.get(&2), Some(&2));

        let rewritten = inline_single_use_ctes(plan, &ctx).expect("inline should succeed");

        match rewritten {
            LogicalPlan::CTEAnchor(anchor) => {
                assert_eq!(anchor.cte_id, 2);
                match *anchor.produce {
                    LogicalPlan::CTEProduce(produce) => match *produce.input {
                        LogicalPlan::Scan(_) | LogicalPlan::Project(_) => {}
                        other => panic!("expected nested inline replacement, got {other:?}"),
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
                required_output_columns: None,
            })),
            consumer: Box::new(LogicalPlan::Union(UnionNode {
                inputs: vec![consume_plan(1, "target"), consume_plan(2, "shadow")],
                all: true,
                output_columns: vec![],
                required_output_columns: None,
            })),
            required_output_columns: None,
        });

        let rewritten = replace_cte_consume(plan, 1, &scan_plan()).expect("replace should succeed");

        match rewritten {
            LogicalPlan::CTEAnchor(anchor) => match *anchor.consumer {
                LogicalPlan::Union(union) => {
                    match &union.inputs[0] {
                        LogicalPlan::Scan(_) | LogicalPlan::Project(_) => {}
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
