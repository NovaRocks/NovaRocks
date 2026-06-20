use crate::sql::common::CteId;
use crate::sql::common::{JoinKind, OutputColumn};
use crate::sql::optimizer::operator::{Operator, ProjectOp, ScalarProjectItem};
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::scalar::{ColumnDisplay, ScalarArena, ScalarNode};
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, Default)]
pub(crate) struct CTEContext {
    pub produces: HashSet<CteId>,
    pub consume_count: HashMap<CteId, usize>,
}

pub(crate) fn collect_cte_counts(expr: &OptExpr) -> CTEContext {
    fn visit(expr: &OptExpr, ctx: &mut CTEContext) {
        match &expr.op {
            Operator::LogicalCTEAnchor(node) => {
                ctx.produces.insert(node.cte_id);
                for child in &expr.children {
                    visit(child, ctx);
                }
            }
            Operator::LogicalCTEConsume(node) => {
                *ctx.consume_count.entry(node.cte_id).or_insert(0) += 1;
            }
            Operator::LogicalImvDelta(_) | Operator::LogicalImvVersion(_) => {
                panic!("imv marker leaked into non-IMV plan");
            }
            _ => {
                for child in &expr.children {
                    visit(child, ctx);
                }
            }
        }
    }

    let mut ctx = CTEContext::default();
    visit(expr, &mut ctx);
    ctx
}

pub(crate) fn inline_single_use_ctes(
    mut expr: OptExpr,
    ctx: &CTEContext,
    scalars: &mut ScalarArena,
) -> Result<OptExpr, String> {
    match &expr.op {
        Operator::LogicalCTEAnchor(node) => {
            let cte_id = node.cte_id;
            let mut children = std::mem::take(&mut expr.children);
            let produce = inline_single_use_ctes(children.remove(0), ctx, scalars)?;
            let consumer = inline_single_use_ctes(children.remove(0), ctx, scalars)?;
            let consume_count = ctx.consume_count.get(&cte_id).copied().unwrap_or(0);

            // Inline single-use CTEs. Multi-consume CTEs use the CTE
            // Produce/Consume path with MultiCast exchange.
            if ctx.produces.contains(&cte_id) && consume_count <= 1 {
                let produce_input = if matches!(
                    &produce.op,
                    Operator::LogicalCTEProduce(produce_node) if produce_node.cte_id == cte_id
                ) {
                    into_single_child(produce)
                } else {
                    produce
                };
                replace_cte_consume(consumer, cte_id, &produce_input, scalars)
            } else {
                expr.children = vec![produce, consumer];
                Ok(expr)
            }
        }
        Operator::LogicalImvDelta(_) | Operator::LogicalImvVersion(_) => {
            panic!("imv marker leaked into non-IMV plan");
        }
        _ => {
            expr.children = expr
                .children
                .into_iter()
                .map(|child| inline_single_use_ctes(child, ctx, scalars))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(expr)
        }
    }
}

fn replace_cte_consume(
    mut expr: OptExpr,
    cte_id: CteId,
    replacement: &OptExpr,
    scalars: &mut ScalarArena,
) -> Result<OptExpr, String> {
    match &expr.op {
        Operator::LogicalCTEConsume(node) if node.cte_id == cte_id => {
            adapt_opt_expr_output_with_qualifier(
                replacement.clone(),
                &node.output_columns,
                Some(&node.alias),
                scalars,
            )
        }
        Operator::LogicalCTEConsume(_) => Ok(expr),
        Operator::LogicalImvDelta(_) | Operator::LogicalImvVersion(_) => {
            panic!("imv marker leaked into non-IMV plan");
        }
        _ => {
            expr.children = expr
                .children
                .into_iter()
                .map(|child| replace_cte_consume(child, cte_id, replacement, scalars))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(expr)
        }
    }
}

fn into_single_child(mut expr: OptExpr) -> OptExpr {
    assert_eq!(expr.children.len(), 1, "expected one logical plan child");
    expr.children.remove(0)
}

fn adapt_opt_expr_output_with_qualifier(
    input: OptExpr,
    target_output_columns: &[OutputColumn],
    output_qualifier: Option<&str>,
    scalars: &mut ScalarArena,
) -> Result<OptExpr, String> {
    let source_output_columns = opt_expr_output_columns(&input, scalars)?;
    if source_output_columns.len() != target_output_columns.len() {
        return Err(format!(
            "output column count mismatch while adapting subquery/CTE output: child has {}, target has {}",
            source_output_columns.len(),
            target_output_columns.len()
        ));
    }

    if source_output_columns
        .iter()
        .zip(target_output_columns.iter())
        .all(|(source, target)| output_column_metadata_equal(source, target))
        && output_qualifier.is_none()
    {
        return Ok(input);
    }

    let mut items = Vec::with_capacity(target_output_columns.len());
    for (source, target) in source_output_columns
        .iter()
        .zip(target_output_columns.iter())
    {
        if source.data_type != target.data_type {
            return Err(format!(
                "output type mismatch while adapting subquery/CTE column '{}': child={:?}, target={:?}",
                target.name, source.data_type, target.data_type
            ));
        }
        if source.nullable && !target.nullable {
            return Err(format!(
                "output nullability mismatch while adapting subquery/CTE column '{}': child={}, target={}",
                target.name, source.nullable, target.nullable
            ));
        }
        scalars.remember_source_column_display(source.column_id, None, source.name.clone());
        let expr = scalars.intern(
            ScalarNode::ColumnRef(source.column_id),
            source.data_type.clone(),
            target.nullable,
        );
        let expr_display = Some(ColumnDisplay {
            qualifier: None,
            column: source.name.clone(),
        });
        scalars.remember_project_output_display(target.column_id, None, target.name.clone());
        items.push(ScalarProjectItem {
            expr,
            output_name: target.name.clone(),
            output_column_id: target.column_id,
            expr_display,
        });
    }

    Ok(OptExpr::new(
        Operator::LogicalProject(ProjectOp {
            items,
            output_qualifier: output_qualifier.map(str::to_string),
        }),
        vec![input],
    ))
}

fn opt_expr_output_columns(
    expr: &OptExpr,
    scalars: &ScalarArena,
) -> Result<Vec<OutputColumn>, String> {
    match &expr.op {
        Operator::LogicalScan(node) => Ok(node.columns.clone()),
        Operator::LogicalFilter(_)
        | Operator::LogicalSort(_)
        | Operator::LogicalLimit(_)
        | Operator::LogicalTopN(_)
        | Operator::LogicalRepeat(_)
        | Operator::LogicalAssertOneRow(_) => opt_expr_output_columns(expr.unary_input(), scalars),
        Operator::LogicalProject(node) => Ok(node
            .items
            .iter()
            .map(|item| OutputColumn {
                column_id: item.output_column_id,
                name: item.output_name.clone(),
                data_type: scalars.data_type(item.expr).clone(),
                nullable: scalars.nullable(item.expr),
                is_internal: false,
            })
            .collect()),
        Operator::LogicalAggregate(node) => Ok(node.output_columns.clone()),
        Operator::LogicalJoin(node) => {
            let left = opt_expr_output_columns(expr.left(), scalars)?;
            let right = opt_expr_output_columns(expr.right(), scalars)?;
            Ok(join_output_columns(node.join_type, left, right))
        }
        Operator::LogicalUnion(node) => Ok(node.output_columns.clone()),
        Operator::LogicalIntersect(node) => Ok(node.output_columns.clone()),
        Operator::LogicalExcept(node) => Ok(node.output_columns.clone()),
        Operator::LogicalValues(node) => Ok(node.columns.clone()),
        Operator::LogicalGenerateSeries(node) => Ok(vec![OutputColumn {
            column_id: node.output_column_id,
            name: node.column_name.clone(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            is_internal: false,
        }]),
        Operator::LogicalTableFunction(node) => {
            let mut columns = opt_expr_output_columns(expr.unary_input(), scalars)?;
            columns.extend(node.output_columns.clone());
            Ok(columns)
        }
        Operator::LogicalWindow(node) => Ok(node.output_columns.clone()),
        Operator::LogicalCTEAnchor(_) => opt_expr_output_columns(expr.child(1), scalars),
        Operator::LogicalCTEProduce(node) => Ok(node.output_columns.clone()),
        Operator::LogicalCTEConsume(node) => Ok(node.output_columns.clone()),
        Operator::LogicalDecode(node) => Ok(node.output_columns.clone()),
        Operator::LogicalAggregateStateMerge(node) => Ok(node.output_columns.clone()),
        Operator::LogicalApply(node) => {
            let mut columns = opt_expr_output_columns(expr.left(), scalars)?;
            columns.push(node.output_column.clone());
            Ok(columns)
        }
        Operator::LogicalImvDelta(_) | Operator::LogicalImvVersion(_) => {
            Err("imv marker leaked into non-IMV planner output adaptation".to_string())
        }
        other => Err(format!(
            "physical operator leaked into CTE output adaptation: {other:?}"
        )),
    }
}

fn output_column_metadata_equal(left: &OutputColumn, right: &OutputColumn) -> bool {
    left.column_id == right.column_id
        && left.name == right.name
        && left.data_type == right.data_type
        && left.nullable == right.nullable
        && left.is_internal == right.is_internal
}

fn join_output_columns(
    join_type: JoinKind,
    left: Vec<OutputColumn>,
    right: Vec<OutputColumn>,
) -> Vec<OutputColumn> {
    match join_type {
        JoinKind::LeftSemi | JoinKind::LeftAnti | JoinKind::NullAwareLeftAnti => left,
        JoinKind::RightSemi | JoinKind::RightAnti => right,
        JoinKind::LeftOuter => {
            let mut out = left;
            out.extend(make_nullable(right));
            out
        }
        JoinKind::RightOuter => {
            let mut out = make_nullable(left);
            out.extend(right);
            out
        }
        JoinKind::FullOuter => {
            let mut out = make_nullable(left);
            out.extend(make_nullable(right));
            out
        }
        JoinKind::Inner | JoinKind::Cross => {
            let mut out = left;
            out.extend(right);
            out
        }
    }
}

fn make_nullable(mut columns: Vec<OutputColumn>) -> Vec<OutputColumn> {
    for column in &mut columns {
        column.nullable = true;
    }
    columns
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{
        CTEAnchorOp, CTEConsumeOp, CTEProduceOp, Operator, ScanOp, UnionOp,
    };
    use crate::sql::optimizer::opt_expr::OptExpr;
    use crate::sql::optimizer::scalar::{self, ScalarArena};
    use arrow::datatypes::DataType;

    fn scan_plan() -> OptExpr {
        OptExpr::leaf(Operator::LogicalScan(ScanOp {
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
                column_id: ColumnId::new_for_test(1),
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                is_internal: false,
            }],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            mv_rewritten_from: None,
        }))
    }

    fn output_columns() -> Vec<OutputColumn> {
        vec![OutputColumn {
            column_id: ColumnId::new_for_test(1),
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

    fn consume_plan(cte_id: CteId, alias: &str) -> OptExpr {
        OptExpr::leaf(Operator::LogicalCTEConsume(CTEConsumeOp {
            cte_id: cte_id,
            alias: alias.to_string(),
            output_columns: output_columns(),
        }))
    }

    fn consume_plan_with_output_columns(
        cte_id: CteId,
        alias: &str,
        output_columns: Vec<OutputColumn>,
    ) -> OptExpr {
        OptExpr::leaf(Operator::LogicalCTEConsume(CTEConsumeOp {
            cte_id: cte_id,
            alias: alias.to_string(),
            output_columns: output_columns,
        }))
    }

    fn cte_produce(cte_id: CteId, input: OptExpr) -> OptExpr {
        OptExpr::new(
            Operator::LogicalCTEProduce(CTEProduceOp {
                cte_id,
                output_columns: output_columns(),
            }),
            vec![input],
        )
    }

    fn cte_anchor(cte_id: CteId, produce: OptExpr, consumer: OptExpr) -> OptExpr {
        OptExpr::new(
            Operator::LogicalCTEAnchor(CTEAnchorOp { cte_id }),
            vec![produce, consumer],
        )
    }

    fn union(children: Vec<OptExpr>) -> OptExpr {
        OptExpr::new(
            Operator::LogicalUnion(UnionOp {
                all: true,
                output_columns: vec![],
                child_output_columns: vec![],
            }),
            children,
        )
    }

    fn scalar_arena() -> ScalarArena {
        ScalarArena::new()
    }

    fn opt_output_columns(
        plan: &OptExpr,
        arena: &ScalarArena,
    ) -> Result<Vec<OutputColumn>, String> {
        let mut memo = crate::sql::optimizer::Memo::new();
        memo.scalars = arena.clone();
        let root_group = crate::sql::optimizer::convert::opt_expr_to_memo(plan, &mut memo);
        crate::sql::optimizer::stats::derive_group_statistics(&mut memo, &HashMap::new());
        Ok(memo.groups[root_group]
            .logical_props
            .as_ref()
            .expect("logical properties should be derived")
            .output_columns
            .clone())
    }

    fn column_ref(column: &OutputColumn) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: column.column_id,
                qualifier: None,
                column: column.name.clone(),
            },
            data_type: column.data_type.clone(),
            nullable: column.nullable,
        }
    }

    #[test]
    fn test_collect_cte_counts_counts_consumes() {
        let plan = cte_anchor(1, cte_produce(1, scan_plan()), consume_plan(1, "t"));

        let ctx = collect_cte_counts(&plan);
        assert!(ctx.produces.contains(&1));
        assert_eq!(ctx.consume_count.get(&1), Some(&1));
    }

    #[test]
    fn test_inline_single_use_cte_removes_anchor_without_alias_node() {
        let plan = cte_anchor(1, cte_produce(1, scan_plan()), consume_plan(1, "t"));

        let ctx = collect_cte_counts(&plan);
        let mut arena = scalar_arena();
        let rewritten =
            inline_single_use_ctes(plan, &ctx, &mut arena).expect("inline should succeed");
        assert!(matches!(
            &rewritten.op,
            Operator::LogicalScan(_) | Operator::LogicalProject(_)
        ));
    }

    #[test]
    fn test_inline_single_use_cte_preserves_consumer_output_columns_with_project() {
        let consume_output_id = ColumnId::new_for_test(42);
        let consume_output_columns = output_columns_with_id_and_name(consume_output_id, "x_id");
        let plan = cte_anchor(
            1,
            cte_produce(1, scan_plan()),
            consume_plan_with_output_columns(1, "x", consume_output_columns.clone()),
        );

        let ctx = collect_cte_counts(&plan);
        let mut arena = scalar_arena();
        let rewritten =
            inline_single_use_ctes(plan, &ctx, &mut arena).expect("inline should succeed");

        let output = opt_output_columns(&rewritten, &arena)
            .expect("rewritten output columns should be derivable");
        assert_eq!(output.len(), consume_output_columns.len());
        assert_eq!(output[0].column_id, consume_output_columns[0].column_id);
        assert_eq!(output[0].name, consume_output_columns[0].name);
        assert_eq!(output[0].data_type, consume_output_columns[0].data_type);
        assert_eq!(output[0].nullable, consume_output_columns[0].nullable);
        let Operator::LogicalProject(project) = &rewritten.op else {
            panic!("expected Project adapter");
        };
        assert_eq!(project.items[0].output_name, "x_id");
        assert_eq!(project.items[0].output_column_id, consume_output_id);
        let materialized = crate::sql::planner::optimizer_bridge::scalar::materialize(
            &arena,
            project.items[0].expr,
        );
        let expected = column_ref(&output_columns()[0]);
        assert_eq!(materialized.data_type, expected.data_type);
        assert_eq!(materialized.nullable, expected.nullable);
        let ExprKind::ColumnRef {
            column_id,
            qualifier,
            column,
        } = materialized.kind
        else {
            panic!("expected ColumnRef project expression");
        };
        assert_eq!(column_id, output_columns()[0].column_id);
        assert!(qualifier.is_none());
        assert_eq!(column, output_columns()[0].name);
    }

    #[test]
    fn test_inline_single_use_cte_keeps_multi_use_anchor() {
        let plan = cte_anchor(
            1,
            cte_produce(1, scan_plan()),
            union(vec![consume_plan(1, "t1"), consume_plan(1, "t2")]),
        );

        let ctx = collect_cte_counts(&plan);
        assert_eq!(ctx.consume_count.get(&1), Some(&2));

        let mut arena = scalar_arena();
        let rewritten =
            inline_single_use_ctes(plan, &ctx, &mut arena).expect("inline should succeed");
        assert!(matches!(&rewritten.op, Operator::LogicalCTEAnchor(_)));
    }

    #[test]
    fn test_inline_single_use_cte_inlines_nested_cte_inside_later_produce() {
        let plan = cte_anchor(
            1,
            cte_produce(1, scan_plan()),
            cte_anchor(
                2,
                cte_produce(
                    2,
                    cte_anchor(1, cte_produce(1, scan_plan()), consume_plan(1, "a")),
                ),
                union(vec![consume_plan(2, "b1"), consume_plan(2, "b2")]),
            ),
        );

        let ctx = collect_cte_counts(&plan);
        assert_eq!(ctx.consume_count.get(&1), Some(&1));
        assert_eq!(ctx.consume_count.get(&2), Some(&2));

        let mut arena = scalar_arena();
        let rewritten =
            inline_single_use_ctes(plan, &ctx, &mut arena).expect("inline should succeed");

        match &rewritten.op {
            Operator::LogicalCTEAnchor(anchor) => {
                assert_eq!(anchor.cte_id, 2);
                let produce_plan = rewritten.child(0);
                match &produce_plan.op {
                    Operator::LogicalCTEProduce(_) => match &produce_plan.unary_input().op {
                        Operator::LogicalScan(_) | Operator::LogicalProject(_) => {}
                        other => panic!("expected nested inline replacement, got {other:?}"),
                    },
                    other => panic!("expected CTEProduce for b, got {other:?}"),
                }
                assert!(matches!(&rewritten.child(1).op, Operator::LogicalUnion(_)));
            }
            other => panic!("expected surviving anchor for b, got {other:?}"),
        }
    }

    #[test]
    fn test_replace_cte_consume_only_rewrites_targeted_cte_id() {
        let plan = cte_anchor(
            2,
            cte_produce(2, scan_plan()),
            union(vec![consume_plan(1, "target"), consume_plan(2, "shadow")]),
        );

        let mut arena = scalar_arena();
        let rewritten =
            replace_cte_consume(plan, 1, &scan_plan(), &mut arena).expect("replace should succeed");

        match &rewritten.op {
            Operator::LogicalCTEAnchor(_) => match &rewritten.child(1).op {
                Operator::LogicalUnion(_) => {
                    let union_plan = rewritten.child(1);
                    match &union_plan.child(0).op {
                        Operator::LogicalScan(_) | Operator::LogicalProject(_) => {}
                        other => panic!("expected targeted consume to be rewritten, got {other:?}"),
                    }
                    assert!(matches!(
                        &union_plan.child(1).op,
                        Operator::LogicalCTEConsume(_)
                    ));
                }
                other => panic!("expected union consumer, got {other:?}"),
            },
            other => panic!("expected outer anchor, got {other:?}"),
        }
    }
}
