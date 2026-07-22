// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::aggregate::*;
use super::output::*;
use super::query::*;
use crate::sql::analysis::cte::CTERegistry;
use crate::sql::analysis::*;
use crate::sql::catalog::PlannerTableProvider;
use crate::sql::column_id::{ColumnId, ColumnRefFactory};
use crate::sql::planner::logical::*;
use crate::sql::planner::payload::*;
use crate::sql::planner::table::{ScanSource, TableDef};
use arrow::datatypes::DataType;
use novarocks_catalog::schema::ColumnDef;

struct TestCatalog;

impl TestCatalog {
    fn get_table(&self, _db: &str, table: &str) -> Result<TableDef, String> {
        match table {
            "orders" => Ok(TableDef {
                name: "orders".to_string(),
                columns: vec![
                    ColumnDef {
                        name: "o_orderkey".to_string(),
                        data_type: arrow::datatypes::DataType::Int64,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    },
                    ColumnDef {
                        name: "o_custkey".to_string(),
                        data_type: arrow::datatypes::DataType::Int64,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    },
                ],
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::StarRocks {
                    db_id: 0,
                    table_id: 0,
                },
            }),
            "maps" => Ok(TableDef {
                name: "maps".to_string(),
                columns: vec![ColumnDef {
                    name: "m".to_string(),
                    data_type: arrow::datatypes::DataType::Map(
                        std::sync::Arc::new(arrow::datatypes::Field::new(
                            "entries",
                            arrow::datatypes::DataType::Struct(
                                vec![
                                    std::sync::Arc::new(arrow::datatypes::Field::new(
                                        "key",
                                        arrow::datatypes::DataType::Int32,
                                        true,
                                    )),
                                    std::sync::Arc::new(arrow::datatypes::Field::new(
                                        "value",
                                        arrow::datatypes::DataType::Int32,
                                        true,
                                    )),
                                ]
                                .into(),
                            ),
                            false,
                        )),
                        false,
                    ),
                    nullable: true,
                    write_default: None,
                    logical_type: None,
                }],
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::StarRocks {
                    db_id: 0,
                    table_id: 0,
                },
            }),
            "t" => Ok(TableDef {
                name: "t".to_string(),
                columns: vec![
                    ColumnDef {
                        name: "a".to_string(),
                        data_type: arrow::datatypes::DataType::Int64,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    },
                    ColumnDef {
                        name: "b".to_string(),
                        data_type: arrow::datatypes::DataType::Int64,
                        nullable: true,
                        write_default: None,
                        logical_type: None,
                    },
                ],
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::StarRocks {
                    db_id: 0,
                    table_id: 0,
                },
            }),
            "iv_orders" => Ok(TableDef {
                name: "iv_orders".to_string(),
                columns: vec![
                    ColumnDef {
                        name: "o_orderkey".to_string(),
                        data_type: arrow::datatypes::DataType::Int64,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    },
                    ColumnDef {
                        name: "o_custkey".to_string(),
                        data_type: arrow::datatypes::DataType::Int64,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    },
                ],
                iceberg_row_lineage_metadata_columns: vec![
                    ColumnDef {
                        name: "_row_id".to_string(),
                        data_type: arrow::datatypes::DataType::Int64,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    },
                    ColumnDef {
                        name: "_last_updated_sequence_number".to_string(),
                        data_type: arrow::datatypes::DataType::Int64,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    },
                ],
                source: ScanSource::StarRocks {
                    db_id: 0,
                    table_id: 0,
                },
            }),
            "t1" | "t2" => {
                let value_col = if table == "t1" { "v1" } else { "v2" };
                Ok(TableDef {
                    name: table.to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "k1".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "k2".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: value_col.to_string(),
                            data_type: arrow::datatypes::DataType::Utf8,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                    ],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                })
            }
            other => Err(format!("unknown test table: {other}")),
        }
    }
}

impl PlannerTableProvider for TestCatalog {
    fn resolve_table_for_analysis(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<crate::sql::catalog::ResolvedAnalyzerTable, String> {
        let planner = self.get_table(database, table)?;
        Ok(crate::sql::catalog::ResolvedAnalyzerTable::from_planner(
            catalog, database, planner,
        ))
    }
}

fn parse_analyze_and_plan(sql: &str) -> Result<LogicalPlanNode, String> {
    let (resolved, cte_registry, mut factory) = parse_analyze_query(sql)?;
    plan_query(resolved, cte_registry, &mut factory)
}

fn parse_analyze_query(
    sql: &str,
) -> Result<(ResolvedQuery, CTERegistry, ColumnRefFactory), String> {
    let dialect = crate::sql::parser::dialect::StarRocksDialect;
    let mut ast = sqlparser::parser::Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())?;
    let stmt = ast
        .pop()
        .ok_or_else(|| "expected a statement".to_string())?;
    let query = match stmt {
        sqlparser::ast::Statement::Query(q) => q,
        _ => return Err("expected query".into()),
    };
    crate::sql::analyzer::analyze(&query, &TestCatalog, "default")
}

fn parse_analyze_query_apply(
    sql: &str,
) -> Result<(ResolvedQuery, CTERegistry, ColumnRefFactory), String> {
    parse_analyze_query(sql)
}

/// Analyze and plan `sql` with the Apply subquery framework.
fn parse_analyze_and_plan_apply(sql: &str) -> Result<LogicalPlanNode, String> {
    let (resolved, cte_registry, mut factory) = parse_analyze_query_apply(sql)?;
    plan_query(resolved, cte_registry, &mut factory)
}

fn plan_test_query(sql: &str) -> LogicalPlanNode {
    parse_analyze_and_plan(sql).expect("planner should succeed")
}

fn first_aggregate_calls(plan: &LogicalPlanNode) -> Vec<AggregateCall> {
    fn visit(plan: &LogicalPlanNode) -> Option<Vec<AggregateCall>> {
        match &plan.kind {
            LogicalPlanKind::Aggregate(node) => Some(node.aggregates.clone()),
            _ => plan.children.iter().find_map(visit),
        }
    }

    visit(plan).unwrap_or_default()
}

fn first_aggregate_node(plan: &LogicalPlanNode) -> Option<&LogicalAggregateNode> {
    match &plan.kind {
        LogicalPlanKind::Aggregate(node) => Some(node),
        _ => plan.children.iter().find_map(first_aggregate_node),
    }
}

fn root_project_over_aggregate(
    plan: &LogicalPlanNode,
) -> (&PlanProjectNode, &LogicalAggregateNode) {
    let LogicalPlanKind::Project(project) = &plan.kind else {
        panic!("expected Project root, got {plan:?}");
    };
    let LogicalPlanKind::Aggregate(aggregate) = &plan.unary_input().kind else {
        panic!(
            "expected Aggregate under Project, got {:?}",
            plan.unary_input()
        );
    };
    (project, aggregate)
}

fn root_project_filter_aggregate(
    plan: &LogicalPlanNode,
) -> (&PlanProjectNode, &PlanFilterNode, &LogicalAggregateNode) {
    let LogicalPlanKind::Project(project) = &plan.kind else {
        panic!("expected Project root, got {plan:?}");
    };
    let filter_plan = plan.unary_input();
    let LogicalPlanKind::Filter(filter) = &filter_plan.kind else {
        panic!("expected Filter under Project, got {:?}", filter_plan);
    };
    let LogicalPlanKind::Aggregate(aggregate) = &filter_plan.unary_input().kind else {
        panic!(
            "expected Aggregate under Filter, got {:?}",
            filter_plan.unary_input()
        );
    };
    (project, filter, aggregate)
}

fn first_repeat_node(plan: &LogicalPlanNode) -> (&LogicalPlanNode, &PlanRepeatNode) {
    fn visit(plan: &LogicalPlanNode) -> Option<(&LogicalPlanNode, &PlanRepeatNode)> {
        match &plan.kind {
            LogicalPlanKind::Repeat(node) => Some((plan, node)),
            _ => plan.children.iter().find_map(visit),
        }
    }

    visit(plan).unwrap_or_else(|| panic!("missing Repeat node in {plan:?}"))
}

#[test]
fn planner_deduplicates_repeated_group_by_but_keeps_repeated_projection_outputs() {
    let plan = parse_analyze_and_plan(
        "SELECT o_orderkey, o_orderkey, count(DISTINCT o_orderkey) \
             FROM orders GROUP BY o_orderkey, o_orderkey",
    )
    .expect("planner should succeed");
    let (project, aggregate) = root_project_over_aggregate(&plan);

    assert_eq!(aggregate.group_by.len(), 1);
    assert_eq!(
        aggregate
            .output_columns
            .iter()
            .map(|column| column.column_id)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        aggregate.output_columns.len()
    );
    assert_eq!(project.items.len(), 3);
    assert_ne!(
        project.items[0].output_column_id,
        project.items[1].output_column_id
    );
    assert_eq!(
        column_ref_id(&project.items[0].expr),
        aggregate.output_columns[0].column_id
    );
    assert_eq!(
        column_ref_id(&project.items[1].expr),
        aggregate.output_columns[0].column_id
    );
}

fn column_ref_id(expr: &TypedExpr) -> ColumnId {
    let ExprKind::ColumnRef { column_id, .. } = &expr.kind else {
        panic!("expected ColumnRef, got {:?}", expr.kind);
    };
    *column_id
}

#[test]
fn planner_group_by_targets_ignore_aggregate_public_output_order() {
    fn col(id: u32, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(id),
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn output(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    let aggregate = LogicalAggregateNode {
        group_by: vec![col(1, "k"), col(2, "region")],
        aggregates: vec![AggregateCall {
            name: "sum".to_string(),
            args: Vec::new(),
            distinct: false,
            result_type: DataType::Int64,
            order_by: Vec::new(),
            output_column_id: ColumnId(30),
        }],
        output_columns: vec![output(30, "sum(v)"), output(1, "k"), output(2, "region")],
        already_pushed: false,
    };

    let targets = planner_aggregate_group_by_targets(&aggregate);

    assert_eq!(
        targets
            .iter()
            .map(|target| target.column_id)
            .collect::<Vec<_>>(),
        vec![ColumnId(1), ColumnId(2)]
    );
}

fn root_project_over_window(plan: &LogicalPlanNode) -> (&PlanProjectNode, &PlanWindowNode) {
    let LogicalPlanKind::Project(project) = &plan.kind else {
        panic!("expected Project root, got {plan:?}");
    };
    let LogicalPlanKind::Window(window) = &plan.unary_input().kind else {
        panic!(
            "expected Window under Project, got {:?}",
            plan.unary_input()
        );
    };
    (project, window)
}

fn root_strip_sort_inner_project(
    plan: &LogicalPlanNode,
) -> (&PlanProjectNode, &PlanSortNode, &PlanProjectNode) {
    let LogicalPlanKind::Project(outer_proj) = &plan.kind else {
        panic!("expected outer strip Project, got {plan:?}");
    };
    let sort_plan = plan.unary_input();
    let LogicalPlanKind::Sort(sort) = &sort_plan.kind else {
        panic!("expected Sort under outer Project, got {:?}", sort_plan);
    };
    let LogicalPlanKind::Project(inner_proj) = &sort_plan.unary_input().kind else {
        panic!(
            "expected inner Project under Sort, got {:?}",
            sort_plan.unary_input()
        );
    };
    (outer_proj, sort, inner_proj)
}

fn binary_left_column_id(expr: &TypedExpr) -> ColumnId {
    let ExprKind::BinaryOp { left, .. } = &expr.kind else {
        panic!("expected BinaryOp, got {:?}", expr.kind);
    };
    let ExprKind::ColumnRef { column_id, .. } = &left.kind else {
        panic!(
            "expected BinaryOp left side to be ColumnRef, got {:?}",
            left.kind
        );
    };
    *column_id
}

fn first_window_exprs(plan: &LogicalPlanNode) -> Vec<WindowExpr> {
    fn visit(plan: &LogicalPlanNode) -> Option<Vec<WindowExpr>> {
        match &plan.kind {
            LogicalPlanKind::Window(node) => Some(node.window_exprs.clone()),
            _ => plan.children.iter().find_map(visit),
        }
    }

    visit(plan).unwrap_or_default()
}

fn first_window_output_columns(plan: &LogicalPlanNode) -> Vec<OutputColumn> {
    fn visit(plan: &LogicalPlanNode) -> Option<Vec<OutputColumn>> {
        match &plan.kind {
            LogicalPlanKind::Window(node) => Some(node.output_columns.clone()),
            _ => plan.children.iter().find_map(visit),
        }
    }

    visit(plan).unwrap_or_default()
}

fn assert_window_expr_ids_are_real_unique_and_backed_by_output_columns(plan: &LogicalPlanNode) {
    let wins = first_window_exprs(plan);
    let output_columns = first_window_output_columns(plan);
    assert!(!wins.is_empty(), "expected at least one WindowExpr");

    let output_ids = output_columns
        .iter()
        .map(|col| col.column_id)
        .collect::<std::collections::HashSet<_>>();
    let mut window_ids = std::collections::HashSet::new();
    for w in &wins {
        assert_ne!(
            w.output_column_id,
            crate::sql::column_id::ColumnId::UNSET,
            "WindowExpr {} must carry a real output_column_id",
            w.output_name
        );
        assert!(
            window_ids.insert(w.output_column_id),
            "WindowExpr {} reuses output_column_id {}",
            w.output_name,
            w.output_column_id
        );
        assert!(
            output_ids.contains(&w.output_column_id),
            "WindowExpr {} output_column_id {} missing from PlanWindowNode.output_columns",
            w.output_name,
            w.output_column_id
        );
    }
}

fn window_expr_by_function_name<'a>(wins: &'a [WindowExpr], name: &str) -> &'a WindowExpr {
    wins.iter()
        .find(|w| w.name.eq_ignore_ascii_case(name))
        .unwrap_or_else(|| panic!("missing WindowExpr function {name}"))
}

fn visible_output_column_by_name<'a>(
    output_columns: &'a [OutputColumn],
    name: &str,
) -> &'a OutputColumn {
    output_columns
        .iter()
        .find(|col| !col.is_internal && col.name == name)
        .unwrap_or_else(|| panic!("missing visible Window output column {name}"))
}

fn strip_project_sort_limit(plan: &LogicalPlanNode) -> &LogicalPlanNode {
    match &plan.kind {
        LogicalPlanKind::Project(_) | LogicalPlanKind::Sort(_) | LogicalPlanKind::Limit(_) => {
            strip_project_sort_limit(plan.unary_input())
        }
        _ => plan,
    }
}

fn unwrap_project_input(plan: &LogicalPlanNode) -> &LogicalPlanNode {
    // Peel any chain of Project adapters to reach the underlying logical
    // node. Besides the outer identity adapter, a subquery alias is now
    // represented as a Project carrying `output_qualifier` (added by the
    // predicate-pushdown work), so more than one Project layer may sit
    // above the set-op.
    let mut current = plan;
    while let LogicalPlanKind::Project(_) = &current.kind {
        current = current.unary_input();
    }
    current
}

fn contains_identity_project_adapter(
    plan: &LogicalPlanNode,
    source_column: &str,
    output_name: &str,
) -> bool {
    match &plan.kind {
        LogicalPlanKind::Project(project) => {
            project.items.iter().any(|item| {
                item.output_name == output_name
                    && matches!(
                        &item.expr.kind,
                        ExprKind::ColumnRef { column_id, column, .. }
                            if column == source_column && item.output_column_id == *column_id
                    )
            }) || plan
                .children
                .iter()
                .any(|child| contains_identity_project_adapter(child, source_column, output_name))
        }
        _ => plan
            .children
            .iter()
            .any(|child| contains_identity_project_adapter(child, source_column, output_name)),
    }
}

#[test]
fn adapt_plan_output_passthrough_when_outputs_match() {
    let source_id = ColumnId::new_for_test(10);
    let input = LogicalPlanNode::new(
        LogicalPlanKind::Values(PlanValuesNode {
            rows: vec![],
            columns: vec![OutputColumn {
                column_id: source_id,
                name: "k".to_string(),
                data_type: arrow::datatypes::DataType::Int64,
                nullable: false,
                is_internal: false,
            }],
        }),
        vec![],
        None,
    );
    let target = vec![OutputColumn {
        column_id: source_id,
        name: "k".to_string(),
        data_type: arrow::datatypes::DataType::Int64,
        nullable: false,
        is_internal: false,
    }];

    let adapted = adapt_plan_output(input, &target).expect("adapter should succeed");
    assert!(matches!(&adapted.kind, LogicalPlanKind::Values(_)));
}

#[test]
fn adapt_plan_output_renames_and_rebinds_with_project() {
    let source_id = ColumnId::new_for_test(10);
    let target_id = ColumnId::new_for_test(20);
    let input = LogicalPlanNode::new(
        LogicalPlanKind::Values(PlanValuesNode {
            rows: vec![],
            columns: vec![OutputColumn {
                column_id: source_id,
                name: "k".to_string(),
                data_type: arrow::datatypes::DataType::Int64,
                nullable: false,
                is_internal: false,
            }],
        }),
        vec![],
        None,
    );
    let target = vec![OutputColumn {
        column_id: target_id,
        name: "alias_k".to_string(),
        data_type: arrow::datatypes::DataType::Int64,
        nullable: false,
        is_internal: false,
    }];

    let adapted = adapt_plan_output(input, &target).expect("adapter should succeed");
    let LogicalPlanKind::Project(project) = &adapted.kind else {
        panic!("expected Project adapter");
    };
    assert_eq!(project.items.len(), 1);
    assert_eq!(project.items[0].output_name, "alias_k");
    assert_eq!(project.items[0].output_column_id, target_id);
    let ExprKind::ColumnRef {
        column_id, column, ..
    } = &project.items[0].expr.kind
    else {
        panic!("expected adapter item to read child column");
    };
    assert_eq!(*column_id, source_id);
    assert_eq!(column, "k");
}

#[test]
fn adapt_plan_output_with_qualifier_preserves_cte_alias_lookup() {
    let source_id = ColumnId::new_for_test(10);
    let target_id = ColumnId::new_for_test(20);
    let input = LogicalPlanNode::new(
        LogicalPlanKind::Values(PlanValuesNode {
            rows: vec![],
            columns: vec![OutputColumn {
                column_id: source_id,
                name: "k1".to_string(),
                data_type: arrow::datatypes::DataType::Int64,
                nullable: false,
                is_internal: false,
            }],
        }),
        vec![],
        None,
    );
    let target = vec![OutputColumn {
        column_id: target_id,
        name: "k1".to_string(),
        data_type: arrow::datatypes::DataType::Int64,
        nullable: false,
        is_internal: false,
    }];

    let adapted = adapt_plan_output_with_qualifier(input, &target, Some("w1"))
        .expect("adapter should succeed");
    let LogicalPlanKind::Project(project) = &adapted.kind else {
        panic!("expected Project adapter");
    };
    assert_eq!(project.items[0].output_column_id, target_id);
    assert_eq!(project.output_qualifier.as_deref(), Some("w1"));
    let ExprKind::ColumnRef {
        column_id,
        qualifier,
        column,
    } = &project.items[0].expr.kind
    else {
        panic!("expected adapter item to read child column");
    };
    assert_eq!(*column_id, source_id);
    assert_eq!(qualifier.as_deref(), None);
    assert_eq!(column, "k1");
}

#[test]
fn adapt_plan_output_with_qualifier_inserts_project_when_outputs_match() {
    let source_id = ColumnId::new_for_test(10);
    let input = LogicalPlanNode::new(
        LogicalPlanKind::Values(PlanValuesNode {
            rows: vec![],
            columns: vec![OutputColumn {
                column_id: source_id,
                name: "rnk".to_string(),
                data_type: arrow::datatypes::DataType::Int64,
                nullable: false,
                is_internal: false,
            }],
        }),
        vec![],
        None,
    );
    let target = vec![OutputColumn {
        column_id: source_id,
        name: "rnk".to_string(),
        data_type: arrow::datatypes::DataType::Int64,
        nullable: false,
        is_internal: false,
    }];

    let adapted = adapt_plan_output_with_qualifier(input, &target, Some("asceding"))
        .expect("adapter should insert alias project");
    let LogicalPlanKind::Project(project) = &adapted.kind else {
        panic!("expected Project adapter for qualified subquery output");
    };
    assert_eq!(project.items[0].output_name, "rnk");
    assert_eq!(project.output_qualifier.as_deref(), Some("asceding"));
    let ExprKind::ColumnRef {
        column_id,
        qualifier,
        column,
    } = &project.items[0].expr.kind
    else {
        panic!("expected adapter item to read child column");
    };
    assert_eq!(*column_id, source_id);
    assert_eq!(qualifier.as_deref(), None);
    assert_eq!(column, "rnk");
}

#[test]
fn adapt_plan_output_allows_nullable_widening() {
    let source_id = ColumnId::new_for_test(10);
    let target_id = ColumnId::new_for_test(20);
    let input = LogicalPlanNode::new(
        LogicalPlanKind::Values(PlanValuesNode {
            rows: vec![],
            columns: vec![OutputColumn {
                column_id: source_id,
                name: "k".to_string(),
                data_type: arrow::datatypes::DataType::Int64,
                nullable: false,
                is_internal: false,
            }],
        }),
        vec![],
        None,
    );
    let target = vec![OutputColumn {
        column_id: target_id,
        name: "nullable_k".to_string(),
        data_type: arrow::datatypes::DataType::Int64,
        nullable: true,
        is_internal: false,
    }];

    let adapted = adapt_plan_output(input, &target).expect("adapter should widen nullability");
    let LogicalPlanKind::Project(project) = &adapted.kind else {
        panic!("expected Project adapter");
    };
    assert_eq!(project.items.len(), 1);
    assert!(project.items[0].expr.nullable);
    assert_eq!(project.items[0].output_column_id, target_id);
}

#[test]
fn adapt_plan_output_rejects_nullable_narrowing() {
    let input = LogicalPlanNode::new(
        LogicalPlanKind::Values(PlanValuesNode {
            rows: vec![],
            columns: vec![OutputColumn {
                column_id: ColumnId::new_for_test(10),
                name: "k".to_string(),
                data_type: arrow::datatypes::DataType::Int64,
                nullable: true,
                is_internal: false,
            }],
        }),
        vec![],
        None,
    );
    let target = vec![OutputColumn {
        column_id: ColumnId::new_for_test(20),
        name: "not_null_k".to_string(),
        data_type: arrow::datatypes::DataType::Int64,
        nullable: false,
        is_internal: false,
    }];

    let err =
        adapt_plan_output(input, &target).expect_err("adapter should reject nullable narrowing");
    assert!(
        err.contains("output nullability mismatch"),
        "unexpected error: {err}"
    );
}

#[test]
fn adapt_plan_output_rejects_shape_mismatch() {
    let input = LogicalPlanNode::new(
        LogicalPlanKind::Values(PlanValuesNode {
            rows: vec![],
            columns: vec![],
        }),
        vec![],
        None,
    );
    let target = vec![OutputColumn {
        column_id: ColumnId::new_for_test(20),
        name: "alias_k".to_string(),
        data_type: arrow::datatypes::DataType::Int64,
        nullable: false,
        is_internal: false,
    }];

    let err = adapt_plan_output(input, &target).expect_err("adapter should reject arity mismatch");
    assert!(
        err.contains("output column count mismatch"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_plan_query_wraps_single_cte_in_anchor() {
    let plan =
        parse_analyze_and_plan("WITH t AS (SELECT o_orderkey AS ok FROM orders) SELECT ok FROM t")
            .expect("planner should succeed");

    match &plan.kind {
        LogicalPlanKind::CTEAnchor(anchor) => {
            assert_eq!(anchor.cte_id, 0);
            assert!(matches!(
                &plan.child(0).kind,
                LogicalPlanKind::CTEProduce(_)
            ));
        }
        other => panic!("expected CTEAnchor, got {other:?}"),
    }
}

#[test]
fn p1_aggregate_call_gets_output_column_id() {
    let plan = plan_test_query("SELECT a, sum(b) AS s FROM t GROUP BY a");
    let aggs = first_aggregate_calls(&plan);
    assert!(!aggs.is_empty(), "expected at least one AggregateCall");
    for call in &aggs {
        assert_ne!(
            call.output_column_id,
            crate::sql::column_id::ColumnId::UNSET,
            "AggregateCall {} must carry a real output_column_id",
            call.name
        );
    }
}

#[test]
fn p1_aggregate_call_ids_deduplicate_repeated_calls() {
    let plan = plan_test_query("SELECT sum(b) AS s1, count(b) AS c, sum(b) AS s2 FROM t");
    let aggs = first_aggregate_calls(&plan);
    assert_eq!(aggs.len(), 2, "expected repeated sum(b) to deduplicate");
    assert_eq!(
        aggs.iter()
            .filter(|call| call.name.eq_ignore_ascii_case("sum"))
            .count(),
        1,
        "expected exactly one sum(b) AggregateCall"
    );
    assert_eq!(
        aggs.iter()
            .filter(|call| call.name.eq_ignore_ascii_case("count"))
            .count(),
        1,
        "expected one count(b) AggregateCall"
    );

    let ids = aggs
        .iter()
        .map(|call| {
            assert_ne!(
                call.output_column_id,
                crate::sql::column_id::ColumnId::UNSET,
                "AggregateCall {} must carry a real output_column_id",
                call.name
            );
            call.output_column_id
        })
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(
        ids.len(),
        aggs.len(),
        "distinct AggregateCalls must carry distinct output ids"
    );
}

#[test]
fn p2_aggregate_projection_rewrites_agg_call_to_output_id_ref() {
    let plan = plan_test_query("SELECT sum(b) + 1 AS s1 FROM t");
    let (project, aggregate) = root_project_over_aggregate(&plan);
    let sum_id = aggregate
        .aggregates
        .iter()
        .find(|call| call.name.eq_ignore_ascii_case("sum"))
        .expect("expected sum AggregateCall")
        .output_column_id;
    assert_ne!(
        sum_id,
        ColumnId::UNSET,
        "AggregateCall must have a real output id"
    );

    let ExprKind::BinaryOp { left, .. } = &project.items[0].expr.kind else {
        panic!(
            "expected sum(b)+1 to remain a BinaryOp over the aggregate output, got {:?}",
            project.items[0].expr.kind
        );
    };
    let ExprKind::ColumnRef {
        column_id, column, ..
    } = &left.kind
    else {
        panic!(
            "aggregate child in sum(b)+1 must be rewritten to ColumnRef, got {:?}",
            left.kind
        );
    };
    assert_eq!(
        *column_id, sum_id,
        "project expression must reference the AggregateCall output id"
    );
    assert_eq!(
        column, "sum(b)",
        "project aggregate ColumnRef must preserve the display name for the P2 fallback"
    );
}

#[test]
fn p2_computed_group_key_rewrites_to_group_output_id() {
    let plan = plan_test_query("SELECT a + 1 AS k, sum(b) AS s FROM t GROUP BY a + 1");
    let (project, aggregate) = root_project_over_aggregate(&plan);
    let group_output_id = aggregate
        .output_columns
        .iter()
        .find(|col| col.name == "k")
        .expect("expected aggregate output column for computed key")
        .column_id;
    assert_ne!(
        group_output_id,
        ColumnId::UNSET,
        "computed group output column must have a real id"
    );

    let ExprKind::ColumnRef {
        column_id, column, ..
    } = &project.items[0].expr.kind
    else {
        panic!(
            "computed group key projection must be rewritten to ColumnRef, got {:?}",
            project.items[0].expr.kind
        );
    };
    assert_eq!(
        *column_id, group_output_id,
        "computed group key projection must reference the Aggregate output id"
    );
    assert_eq!(
        column, "a + 1",
        "computed group key ColumnRef must preserve the group expression display name"
    );
}

#[test]
fn p2_having_rewrites_agg_call_to_output_id_ref() {
    let plan = plan_test_query("SELECT sum(b) AS s FROM t HAVING sum(b) > 10");
    let (_project, filter, aggregate) = root_project_filter_aggregate(&plan);
    let sum_id = aggregate
        .aggregates
        .iter()
        .find(|call| call.name.eq_ignore_ascii_case("sum"))
        .expect("expected sum AggregateCall")
        .output_column_id;
    assert_ne!(
        sum_id,
        ColumnId::UNSET,
        "AggregateCall must have a real output id"
    );

    let ExprKind::BinaryOp { left, .. } = &filter.predicate.kind else {
        panic!(
            "expected HAVING sum(b)>10 to remain a BinaryOp over the aggregate output, got {:?}",
            filter.predicate.kind
        );
    };
    let ExprKind::ColumnRef {
        column_id, column, ..
    } = &left.kind
    else {
        panic!(
            "aggregate child in HAVING must be rewritten to ColumnRef, got {:?}",
            left.kind
        );
    };
    assert_eq!(
        *column_id, sum_id,
        "HAVING predicate must reference the AggregateCall output id"
    );
    assert_eq!(
        column, "sum(b)",
        "HAVING aggregate ColumnRef must preserve the display name for the P2 fallback"
    );
}

#[test]
fn order_by_only_aggregates_are_added_to_aggregate_outputs() {
    let plan = plan_test_query(
        "SELECT min(a) AS v1 FROM t GROUP BY b ORDER BY round(count(a) / min(a)), min(a)",
    );
    let aggregate = first_aggregate_node(&plan).expect("expected Aggregate in plan");

    assert_eq!(
        aggregate.output_columns.len(),
        aggregate.group_by.len() + aggregate.aggregates.len(),
        "Aggregate output columns must include ORDER BY-only aggregate calls"
    );
    for call in &aggregate.aggregates {
        assert_ne!(
            call.output_column_id,
            ColumnId::UNSET,
            "AggregateCall {} must have a real output id",
            call.name
        );
        assert!(
            aggregate
                .output_columns
                .iter()
                .any(|col| col.column_id == call.output_column_id),
            "Aggregate output columns must contain {} with id {}",
            call.name,
            call.output_column_id
        );
    }
}

#[test]
fn p2_having_computed_group_key_does_not_append_leaf_group_by() {
    let plan =
        plan_test_query("SELECT abs(a) AS k, sum(b) AS s FROM t GROUP BY abs(a) HAVING abs(a) > 1");
    let (project, filter, aggregate) = root_project_filter_aggregate(&plan);
    assert_eq!(
        aggregate.group_by.len(),
        1,
        "HAVING group expression must not append its leaf column as an extra group key"
    );
    let group_output_id = aggregate
        .output_columns
        .iter()
        .find(|col| col.name == "k")
        .expect("expected aggregate output column for computed key")
        .column_id;
    assert_ne!(
        group_output_id,
        ColumnId::UNSET,
        "computed group output column must have a real id"
    );
    let ExprKind::ColumnRef {
        column_id, column, ..
    } = &project.items[0].expr.kind
    else {
        panic!(
            "computed group key projection must be rewritten to ColumnRef, got {:?}",
            project.items[0].expr.kind
        );
    };
    assert_eq!(
        *column_id, group_output_id,
        "computed group key projection must reference the Aggregate output id"
    );
    assert_eq!(
        column, "abs(a)",
        "computed group key ColumnRef must preserve the group expression display name"
    );

    let ExprKind::BinaryOp { left, .. } = &filter.predicate.kind else {
        panic!(
            "expected HAVING abs(a)>1 to remain a BinaryOp over the group key output, got {:?}",
            filter.predicate.kind
        );
    };
    let ExprKind::ColumnRef {
        column_id, column, ..
    } = &left.kind
    else {
        panic!(
            "computed group key in HAVING must be rewritten to ColumnRef, got {:?}",
            left.kind
        );
    };
    assert_eq!(
        *column_id, group_output_id,
        "HAVING computed group key must reference the Aggregate output id"
    );
    assert_eq!(
        column, "abs(a)",
        "HAVING computed group key ColumnRef must preserve the group expression display name"
    );
}

#[test]
fn p2_repeat_grouping_aggregate_outputs_follow_group_by_order() {
    let plan = plan_test_query(
        "SELECT grouping(a + 1) AS g, a + 1 AS k, count(*) AS cnt \
             FROM t GROUP BY ROLLUP(a + 1)",
    );
    let (project, aggregate) = root_project_over_aggregate(&plan);
    assert_eq!(
        aggregate.group_by.len(),
        2,
        "ROLLUP with GROUPING() should group by repeat key and grouping marker"
    );
    let group_ids = aggregate
        .group_by
        .iter()
        .map(column_ref_id)
        .collect::<Vec<_>>();
    let output_prefix_ids = aggregate
        .output_columns
        .iter()
        .take(aggregate.group_by.len())
        .map(|col| col.column_id)
        .collect::<Vec<_>>();
    assert_eq!(
        output_prefix_ids, group_ids,
        "Aggregate output_columns prefix must match group_by physical output order"
    );

    let g_id = column_ref_id(&project.items[0].expr);
    let k_id = column_ref_id(&project.items[1].expr);
    assert_eq!(
        g_id, group_ids[1],
        "GROUPING() projection must bind to the grouping marker output"
    );
    assert_eq!(
        k_id, group_ids[0],
        "rollup key projection must bind to the repeat key output"
    );
}

#[test]
fn p3_cube_without_grouping_survives_optimizer_id_binding() {
    let sql = "WITH t AS ( \
                   SELECT 1 AS a, 'x' AS b \
                   UNION ALL SELECT 1, 'y' \
                   UNION ALL SELECT 2, 'z' \
                   ) \
                   SELECT a, b FROM t GROUP BY CUBE(a, b) ORDER BY a, b";
    let (resolved, cte_registry, mut factory) =
        parse_analyze_query(sql).expect("analyzer should succeed");
    let logical_plan =
        plan_query(resolved, cte_registry, &mut factory).expect("planner should succeed");
    let mut scalar_arena = crate::sql::optimizer::scalar::ScalarArena::new();
    let optimizer_expr = crate::sql::planner::optimizer_bridge::logical::try_to_optimizer_expr(
        &logical_plan,
        &mut scalar_arena,
    )
    .expect("logical to opt expr");
    let optimized_tree = crate::sql::optimizer::optimize_with_legacy_table_stats_for_migration(
        optimizer_expr,
        scalar_arena,
        &std::collections::HashMap::new(),
        factory,
        Vec::new(),
    )
    .expect("optimizer should produce a physical plan");

    crate::sql::planner::optimizer_bridge::id_binding::verify_optimized_tree_id_binding(
        &optimized_tree,
    )
    .expect("CUBE synthetic grouping output must survive optimizer extraction");
}

#[test]
fn p3_rollup_order_by_only_key_survives_optimizer_id_binding() {
    let sql = "SELECT array_agg(DISTINCT b ORDER BY b) \
                   FROM t GROUP BY ROLLUP(a) ORDER BY a";
    let (resolved, cte_registry, mut factory) =
        parse_analyze_query(sql).expect("analyzer should succeed");
    let logical_plan =
        plan_query(resolved, cte_registry, &mut factory).expect("planner should succeed");
    let mut scalar_arena = crate::sql::optimizer::scalar::ScalarArena::new();
    let optimizer_expr = crate::sql::planner::optimizer_bridge::logical::try_to_optimizer_expr(
        &logical_plan,
        &mut scalar_arena,
    )
    .expect("logical to opt expr");
    let optimized_tree = crate::sql::optimizer::optimize_with_legacy_table_stats_for_migration(
        optimizer_expr,
        scalar_arena,
        &std::collections::HashMap::new(),
        factory,
        Vec::new(),
    )
    .expect("optimizer should produce a physical plan");

    crate::sql::planner::optimizer_bridge::id_binding::verify_optimized_tree_id_binding(
        &optimized_tree,
    )
    .expect("ROLLUP ORDER BY-only key must bind to aggregate repeat-key output");
}

#[test]
fn p3_rollup_window_order_by_alias_extra_survives_optimizer_id_binding() {
    let sql = "SELECT sum(b) AS total_sum, \
                          a, \
                          b, \
                          grouping(a) + grouping(b) AS lochierarchy, \
                          rank() OVER ( \
                            PARTITION BY grouping(a) + grouping(b), \
                                         CASE WHEN grouping(b) = 0 THEN a END \
                            ORDER BY sum(b) DESC \
                          ) AS rank_within_parent \
                   FROM t \
                   GROUP BY ROLLUP(a, b) \
                   ORDER BY lochierarchy DESC, \
                            CASE WHEN lochierarchy = 0 THEN a END, \
                            rank_within_parent \
                   LIMIT 10";
    let (resolved, cte_registry, mut factory) =
        parse_analyze_query(sql).expect("analyzer should succeed");
    let logical_plan =
        plan_query(resolved, cte_registry, &mut factory).expect("planner should succeed");
    let mut scalar_arena = crate::sql::optimizer::scalar::ScalarArena::new();
    let optimizer_expr = crate::sql::planner::optimizer_bridge::logical::try_to_optimizer_expr(
        &logical_plan,
        &mut scalar_arena,
    )
    .expect("logical to opt expr");
    let optimized_tree = crate::sql::optimizer::optimize_with_legacy_table_stats_for_migration(
        optimizer_expr,
        scalar_arena,
        &std::collections::HashMap::new(),
        factory,
        Vec::new(),
    )
    .expect("optimizer should produce a physical plan");

    crate::sql::planner::optimizer_bridge::id_binding::verify_optimized_tree_id_binding(
        &optimized_tree,
    )
    .expect("ROLLUP window ORDER BY alias extras must bind to child/window outputs");
}

#[test]
fn p3_aggregate_order_by_alias_topn_survives_optimizer_id_binding() {
    let sql = "SELECT a, count(*) AS total_cnt \
                   FROM t \
                   GROUP BY a \
                   ORDER BY total_cnt DESC, a \
                   LIMIT 10";
    let (resolved, cte_registry, mut factory) =
        parse_analyze_query(sql).expect("analyzer should succeed");
    let logical_plan =
        plan_query(resolved, cte_registry, &mut factory).expect("planner should succeed");
    let mut scalar_arena = crate::sql::optimizer::scalar::ScalarArena::new();
    let optimizer_expr = crate::sql::planner::optimizer_bridge::logical::try_to_optimizer_expr(
        &logical_plan,
        &mut scalar_arena,
    )
    .expect("logical to opt expr");
    let optimized_tree = crate::sql::optimizer::optimize_with_legacy_table_stats_for_migration(
        optimizer_expr,
        scalar_arena,
        &std::collections::HashMap::new(),
        factory,
        Vec::new(),
    )
    .expect("optimizer should produce a physical plan");

    crate::sql::planner::optimizer_bridge::id_binding::verify_optimized_tree_id_binding(
        &optimized_tree,
    )
    .expect("aggregate ORDER BY alias TopN must bind to aggregate project output");
}

#[test]
fn p1_window_expr_gets_output_column_id() {
    let plan =
        plan_test_query("SELECT a, row_number() OVER (PARTITION BY a ORDER BY b) AS rn FROM t");
    assert_window_expr_ids_are_real_unique_and_backed_by_output_columns(&plan);
    let wins = first_window_exprs(&plan);
    let output_columns = first_window_output_columns(&plan);
    let rn = window_expr_by_function_name(&wins, "row_number");
    let visible_rn = visible_output_column_by_name(&output_columns, "rn");
    assert_eq!(
        rn.output_column_id, visible_rn.column_id,
        "single visible window projection must reuse the visible rn output id"
    );
}

#[test]
fn p1_compound_window_exprs_get_distinct_output_column_ids() {
    let plan = plan_test_query(
        "SELECT row_number() OVER (ORDER BY a) + rank() OVER (ORDER BY b) AS x FROM t",
    );
    let wins = first_window_exprs(&plan);
    let output_columns = first_window_output_columns(&plan);
    assert_eq!(wins.len(), 2, "expected two extracted WindowExprs");
    assert_window_expr_ids_are_real_unique_and_backed_by_output_columns(&plan);
    for w in &wins {
        let output_column = output_columns
            .iter()
            .find(|col| col.column_id == w.output_column_id)
            .expect("window output id should be present");
        assert!(
            output_column.is_internal,
            "compound WindowExpr {} should use an internal output column",
            w.output_name
        );
    }
    let visible = plan_output_columns(&plan).expect("plan output should be known");
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].name, "x");
    assert!(!visible[0].is_internal);
}

#[test]
fn p1_multiple_projection_window_exprs_get_distinct_output_column_ids() {
    let plan = plan_test_query(
        "SELECT row_number() OVER (ORDER BY a) AS rn, rank() OVER (ORDER BY b) AS rk FROM t",
    );
    let wins = first_window_exprs(&plan);
    let output_columns = first_window_output_columns(&plan);
    assert_eq!(wins.len(), 2, "expected two extracted WindowExprs");
    assert_window_expr_ids_are_real_unique_and_backed_by_output_columns(&plan);
    let rn = window_expr_by_function_name(&wins, "row_number");
    let rk = window_expr_by_function_name(&wins, "rank");
    let visible_rn = visible_output_column_by_name(&output_columns, "rn");
    let visible_rk = visible_output_column_by_name(&output_columns, "rk");
    assert_eq!(
        rn.output_column_id, visible_rn.column_id,
        "single visible window projection must reuse the visible rn output id"
    );
    assert_eq!(
        rk.output_column_id, visible_rk.column_id,
        "single visible window projection must reuse the visible rk output id"
    );
}

#[test]
fn p2_window_output_columns_preserve_passthrough_input_ids() {
    let plan =
        plan_test_query("SELECT a, row_number() OVER (PARTITION BY a ORDER BY b) AS rn FROM t");
    let (project, window) = root_project_over_window(&plan);
    let passthrough_id = column_ref_id(&project.items[0].expr);
    assert!(
        window
            .output_columns
            .iter()
            .any(|col| col.column_id == passthrough_id),
        "PlanWindowNode output_columns must expose child passthrough ColumnIds"
    );
    let rn = window_expr_by_function_name(&window.window_exprs, "row_number");
    assert!(
        window
            .output_columns
            .iter()
            .any(|col| col.column_id == rn.output_column_id),
        "PlanWindowNode output_columns must include window result ColumnIds"
    );
}

#[test]
fn p2_window_call_rewrites_to_window_output_id() {
    let plan =
        plan_test_query("SELECT row_number() OVER (PARTITION BY a ORDER BY b) + 1 AS rn1 FROM t");
    let (project, window) = root_project_over_window(&plan);
    let rn = window_expr_by_function_name(&window.window_exprs, "row_number");
    assert_ne!(
        rn.output_column_id,
        ColumnId::UNSET,
        "WindowExpr must have a real output id"
    );

    let ExprKind::BinaryOp { left, .. } = &project.items[0].expr.kind else {
        panic!(
            "expected row_number()+1 to remain a BinaryOp over the window output, got {:?}",
            project.items[0].expr.kind
        );
    };
    let ExprKind::ColumnRef {
        column_id, column, ..
    } = &left.kind
    else {
        panic!(
            "window child in row_number()+1 must be rewritten to ColumnRef, got {:?}",
            left.kind
        );
    };
    assert_eq!(
        *column_id, rn.output_column_id,
        "project expression must reference the WindowExpr output id"
    );
    assert_eq!(
        column, "rn1",
        "window ColumnRef must preserve the P2 display name"
    );
}

#[test]
fn test_plan_query_builds_nested_anchor_chain() {
    let plan = parse_analyze_and_plan(
        "WITH a AS (SELECT o_orderkey AS ok FROM orders), \
                  b AS (SELECT ok FROM a) \
             SELECT ok FROM b",
    )
    .expect("planner should succeed");

    match &plan.kind {
        LogicalPlanKind::CTEAnchor(anchor_a) => match &plan.child(1).kind {
            LogicalPlanKind::CTEAnchor(anchor_b) => {
                assert_eq!(anchor_a.cte_id, 0);
                assert_eq!(anchor_b.cte_id, 1);
            }
            other => panic!("expected nested CTEAnchor, got {other:?}"),
        },
        other => panic!("expected outer CTEAnchor, got {other:?}"),
    }
}

#[test]
fn test_sum_map_subscript_plans_as_aggregate() {
    let plan =
        parse_analyze_and_plan("SELECT sum_map(m)[1] FROM maps").expect("planner should succeed");

    match &plan.kind {
        LogicalPlanKind::Project(_) => match &plan.unary_input().kind {
            LogicalPlanKind::Aggregate(agg) => {
                assert_eq!(agg.aggregates.len(), 1);
                assert_eq!(agg.aggregates[0].name, "sum_map");
            }
            other => panic!("expected Aggregate under Project, got {other:?}"),
        },
        other => panic!("expected Project root, got {other:?}"),
    }
}

#[test]
fn group_by_alias_expression_projects_aggregate_group_key() {
    let plan = parse_analyze_and_plan(
        "SELECT o_orderkey % 2 AS g, count(*) FROM orders GROUP BY g ORDER BY g",
    )
    .expect("planner should succeed");

    let LogicalPlanKind::Sort(sort) = &plan.kind else {
        panic!("expected Sort root");
    };
    let LogicalPlanKind::Project(project) = &plan.unary_input().kind else {
        panic!("expected Project under Sort");
    };
    let ExprKind::ColumnRef {
        qualifier, column, ..
    } = &project.items[0].expr.kind
    else {
        panic!(
            "expected group key projection to be a ColumnRef, got {:?}",
            project.items[0].expr
        );
    };
    assert!(qualifier.is_none());
    assert_eq!(column, "o_orderkey % 2");
}

#[test]
fn derived_table_plans_without_alias_operator() {
    let plan = parse_analyze_and_plan("SELECT s.o_orderkey FROM (SELECT o_orderkey FROM orders) s")
        .expect("planner should succeed");

    let debug = format!("{plan:?}");
    assert!(
        !debug.contains("alias operator"),
        "derived table must not create alias operator: {debug}"
    );
}

#[test]
fn derived_table_column_alias_uses_project_adapter() {
    let plan = parse_analyze_and_plan("SELECT s.ok FROM (SELECT o_orderkey FROM orders) s(ok)")
        .expect("planner should succeed");

    let debug = format!("{plan:?}");
    assert!(
        !debug.contains("alias operator"),
        "column alias derived table must not create alias operator: {debug}"
    );

    assert!(
        contains_identity_project_adapter(&plan, "o_orderkey", "ok"),
        "expected identity Project adapter to expose column alias ok: {plan:?}"
    );
}

#[test]
fn test_nested_with_in_derived_table_stays_inside_subquery_scope() {
    let plan = parse_analyze_and_plan(
        "WITH outer_t AS (SELECT o_orderkey AS ok FROM orders) \
             SELECT ok FROM (WITH inner_t AS (SELECT o_custkey AS ok FROM orders) \
                             SELECT ok FROM inner_t) s",
    )
    .expect("planner should succeed");

    match &plan.kind {
        LogicalPlanKind::CTEAnchor(outer_anchor) => {
            assert_eq!(outer_anchor.cte_id, 0);
            let subquery_input = strip_project_sort_limit(plan.child(1));
            match &subquery_input.kind {
                LogicalPlanKind::CTEAnchor(inner_anchor) => {
                    assert_eq!(inner_anchor.cte_id, 1);
                }
                other => panic!("expected inner CTEAnchor inside subquery, got {other:?}"),
            }
        }
        other => panic!("expected outer CTEAnchor, got {other:?}"),
    }
}

#[test]
fn test_nested_with_in_cte_definition_stays_inside_produce_subtree() {
    let plan = parse_analyze_and_plan(
        "WITH outer_cte AS (WITH inner_cte AS (SELECT o_orderkey AS ok FROM orders) \
                                SELECT ok FROM inner_cte) \
             SELECT ok FROM outer_cte",
    )
    .expect("planner should succeed");

    match &plan.kind {
        LogicalPlanKind::CTEAnchor(outer_anchor) => {
            assert_eq!(outer_anchor.cte_id, 1);
            match &plan.child(0).kind {
                LogicalPlanKind::CTEProduce(_) => match &plan.child(0).unary_input().kind {
                    LogicalPlanKind::CTEAnchor(inner_anchor) => {
                        assert_eq!(inner_anchor.cte_id, 0);
                    }
                    other => {
                        panic!("expected inner CTEAnchor inside produce input, got {other:?}")
                    }
                },
                other => panic!("expected outer CTEProduce, got {other:?}"),
            }
        }
        other => panic!("expected outer CTEAnchor, got {other:?}"),
    }
}

#[test]
fn test_explain_keeps_nested_cte_anchor_inside_subquery() {
    let plan = parse_analyze_and_plan(
        "WITH outer_t AS (SELECT o_orderkey AS ok FROM orders) \
             SELECT ok FROM (WITH inner_t AS (SELECT o_custkey AS ok FROM orders) \
                             SELECT ok FROM inner_t) s",
    )
    .expect("planner should succeed");

    let lines = crate::sql::explain::explain_plan(&plan, crate::sql::explain::ExplainLevel::Normal);
    let outer_anchor_idx = lines
        .iter()
        .position(|line| line.contains("CTE_ANCHOR(cte_id=0)"))
        .expect("expected outer anchor line");
    let inner_anchor_idx = lines
        .iter()
        .position(|line| line.contains("CTE_ANCHOR(cte_id=1)"))
        .expect("expected nested inner anchor line");

    assert!(
        inner_anchor_idx > outer_anchor_idx,
        "nested inner anchor should remain inside derived-table subtree: {lines:?}"
    );
}

#[test]
fn window_reuses_ordering_through_derived_table() {
    let plan = parse_analyze_and_plan(
        "SELECT sum(o_custkey) OVER \
                    (ORDER BY o_orderkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) \
                    AS running_sum \
             FROM (SELECT o_orderkey, o_custkey FROM orders ORDER BY o_orderkey) s",
    )
    .expect("planner should succeed");

    let lines =
        crate::sql::explain::explain_plan(&plan, crate::sql::explain::ExplainLevel::Verbose);
    let sort_count = lines
        .iter()
        .filter(|line| line.contains("SORT BY [o_orderkey ASC NULLS FIRST]"))
        .count();
    assert_eq!(
        sort_count, 1,
        "window should reuse the derived table ordering: {lines:?}"
    );
}

#[test]
fn window_reuses_ordering_through_derived_table_column_alias_project() {
    let plan = parse_analyze_and_plan(
        "SELECT sum(ok) OVER \
                    (ORDER BY ok ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) \
                    AS running_sum \
             FROM (SELECT o_orderkey FROM orders ORDER BY o_orderkey) s(ok)",
    )
    .expect("planner should succeed");

    let lines =
        crate::sql::explain::explain_plan(&plan, crate::sql::explain::ExplainLevel::Verbose);
    let sort_lines = lines
        .iter()
        .filter(|line| line.contains("SORT BY ["))
        .collect::<Vec<_>>();
    assert_eq!(
        sort_lines.len(),
        1,
        "window should reuse derived table ordering through identity Project: {lines:?}"
    );
    assert!(
        sort_lines[0].contains("SORT BY [o_orderkey ASC NULLS FIRST]"),
        "expected the preserved derived-table ordering, got {sort_lines:?}"
    );
}

#[test]
fn test_parenthesized_set_op_branch_keeps_local_cte_anchor_in_branch() {
    let plan = parse_analyze_and_plan(
        "SELECT o_orderkey AS ok FROM orders \
             UNION ALL \
             (WITH t AS (SELECT o_custkey AS ok FROM orders) SELECT ok FROM t)",
    )
    .expect("planner should succeed");

    match &plan.kind {
        LogicalPlanKind::Union(_) => {
            assert_eq!(plan.children.len(), 2);
            match &plan.child(1).kind {
                LogicalPlanKind::CTEAnchor(anchor) => assert_eq!(anchor.cte_id, 0),
                other => {
                    panic!("expected branch-local CTEAnchor in union input, got {other:?}")
                }
            }
        }
        other => panic!("expected UNION plan, got {other:?}"),
    }
}

#[test]
fn test_explain_keeps_parenthesized_set_op_branch_anchor_in_branch() {
    let plan = parse_analyze_and_plan(
        "SELECT o_orderkey AS ok FROM orders \
             UNION ALL \
             (WITH t AS (SELECT o_custkey AS ok FROM orders) SELECT ok FROM t)",
    )
    .expect("planner should succeed");

    let lines = crate::sql::explain::explain_plan(&plan, crate::sql::explain::ExplainLevel::Normal);
    let union_idx = lines
        .iter()
        .position(|line| line.contains("UNION ALL"))
        .expect("expected union line");
    let anchor_idx = lines
        .iter()
        .position(|line| line.contains("CTE_ANCHOR(cte_id=0)"))
        .expect("expected branch-local anchor line");

    assert!(
        anchor_idx > union_idx,
        "branch-local anchor should appear under union: {lines:?}"
    );
}

/// Regression test for the ColumnId-correctness bug where LogicalUnionNode.output_columns
/// carried left-branch ColumnIds instead of the fresh set-op output ColumnIds.
#[test]
fn output_columns_carry_fresh_set_op_ids() {
    // Plan a derived table that wraps a UNION ALL. The Union node must carry
    // the analyzer-visible output IDs directly, without an alias wrapper.
    let plan = parse_analyze_and_plan(
        "SELECT o_orderkey, o_custkey \
             FROM (SELECT o_orderkey, o_custkey FROM orders \
                   UNION ALL \
                   SELECT o_orderkey, o_custkey FROM orders) sub",
    )
    .expect("planner should succeed");

    let debug = format!("{plan:?}");
    assert!(
        !debug.contains("alias operator"),
        "set-op derived table must not create alias operator: {debug}"
    );

    let union_node = match &unwrap_project_input(&plan).kind {
        LogicalPlanKind::Union(n) => n,
        other => panic!("expected Union below adapter, got {other:?}"),
    };
    let visible_columns = plan_output_columns(&plan).expect("plan output should be known");

    // Core assertion: fresh IDs must match position-by-position.
    assert_eq!(
        visible_columns.len(),
        union_node.output_columns.len(),
        "visible output and Union output_columns length must match"
    );
    for (i, (visible_col, union_col)) in visible_columns
        .iter()
        .zip(union_node.output_columns.iter())
        .enumerate()
    {
        assert_eq!(
            visible_col.column_id, union_col.column_id,
            "output_columns[{i}]: visible column_id {:?} != Union column_id {:?} \
                 (Union must carry the fresh set-op IDs, not left-branch IDs)",
            visible_col.column_id, union_col.column_id
        );
    }
}

/// Same correctness guarantee for INTERSECT and EXCEPT set operations.
#[test]
fn intersect_except_output_columns_carry_fresh_set_op_ids() {
    for sql in [
        "SELECT o_orderkey FROM (SELECT o_orderkey FROM orders \
             INTERSECT SELECT o_orderkey FROM orders) sub",
        "SELECT o_orderkey FROM (SELECT o_orderkey FROM orders \
             EXCEPT SELECT o_orderkey FROM orders) sub",
    ] {
        let plan = parse_analyze_and_plan(sql).expect("planner should succeed");

        let debug = format!("{plan:?}");
        assert!(
            !debug.contains("alias operator"),
            "set-op derived table must not create alias operator: {debug}"
        );

        let visible_columns = plan_output_columns(&plan).expect("plan output should be known");
        let set_op_cols = match &unwrap_project_input(&plan).kind {
            LogicalPlanKind::Intersect(n) => &n.output_columns,
            LogicalPlanKind::Except(n) => &n.output_columns,
            other => panic!("expected Intersect/Except below adapter, got {other:?}"),
        };

        assert_eq!(visible_columns.len(), set_op_cols.len());
        for (i, (visible_col, set_op_col)) in
            visible_columns.iter().zip(set_op_cols.iter()).enumerate()
        {
            assert_eq!(
                visible_col.column_id, set_op_col.column_id,
                "output_columns[{i}]: visible {:?} != set-op {:?} for SQL: {sql}",
                visible_col.column_id, set_op_col.column_id
            );
        }
    }
}

// -----------------------------------------------------------------------
// Bug B regression: build_distinct must preserve item.output_column_id
// -----------------------------------------------------------------------

/// Bug B: build_distinct previously called expr_column_id() for every item
/// in the projection, minting a fresh ColumnId for non-ColumnRef exprs (e.g.
/// the synthetic `Literal(1)` produced by IN/EXISTS subquery rewriting when
/// the item had a meaningful pre-assigned `output_column_id`). This broke
/// downstream references that already held the original id.
///
/// Fix: when item.output_column_id != UNSET, use it directly instead of
/// calling expr_column_id.
///
/// This test verifies that SELECT DISTINCT over a query with pre-assigned
/// output ids produces an Aggregate whose group-by ColumnRefs carry the same
/// ids as the inner projection's output_column_ids.
#[test]
fn build_distinct_preserves_output_column_id_from_projection() {
    // Use build_distinct indirectly via the planner: SELECT DISTINCT
    // o_orderkey FROM orders.  The inner Project item will have a
    // non-UNSET output_column_id (assigned by the analyzer), and the outer
    // DISTINCT Aggregate's group-by ColumnRef must carry the same id.
    let plan = parse_analyze_and_plan("SELECT DISTINCT o_orderkey FROM orders")
        .expect("planner should succeed");

    // Expected shape: Aggregate(group_by=[ColumnRef(cid)]) <- Project(item.output_column_id=cid)
    let (agg_group_by_cid, inner_proj_output_cid) = match &plan.kind {
        LogicalPlanKind::Aggregate(agg) => {
            let gb_cid = match &agg.group_by[0].kind {
                ExprKind::ColumnRef { column_id, .. } => *column_id,
                other => panic!("expected ColumnRef group_by, got {other:?}"),
            };
            let inner_proj = match &plan.unary_input().kind {
                LogicalPlanKind::Project(p) => p,
                other => panic!("expected Project under Aggregate, got {other:?}"),
            };
            let item_cid = inner_proj.items[0].output_column_id;
            (gb_cid, item_cid)
        }
        other => panic!("expected Aggregate root for SELECT DISTINCT, got {other:?}"),
    };

    assert_ne!(
        agg_group_by_cid,
        ColumnId::UNSET,
        "Aggregate group-by ColumnRef must not be UNSET"
    );
    assert_eq!(
        agg_group_by_cid, inner_proj_output_cid,
        "build_distinct must reuse inner Project item's output_column_id, \
             not mint a fresh id (Bug B)"
    );
}

// -----------------------------------------------------------------------
// Bug C regression: apply_query_modifiers strip-project must reuse inner ids
// -----------------------------------------------------------------------

/// Bug C: apply_query_modifiers built a strip-projection by calling
/// factory.create(...) for each item, minting fresh ColumnIds that
/// disconnected the outer Project from the inner Project's output ids.
/// The Phase-1 tagging pass then saw a double-Project barrier where the
/// outer Project's items used ids that didn't match anything the inner
/// Project produced, causing it to compute child_needed = {} and drop all
/// inner columns.
///
/// Fix: reuse the inner project item's existing output_column_id for the
/// strip-project item instead of minting a fresh one.
///
/// This test uses a query with an ORDER BY column that is NOT in the SELECT
/// output (triggering extra_items and the strip-projection path), then
/// verifies that the outer Project's items carry the same ColumnIds as the
/// inner Project's items at the corresponding positions.
#[test]
fn apply_query_modifiers_strip_project_reuses_inner_output_column_ids() {
    // ORDER BY o_custkey is not in the SELECT output (only o_orderkey is),
    // so collect_extra_sort_items returns o_custkey as an extra.
    // apply_query_modifiers then builds:
    //   outer strip-Project (items: [o_orderkey]) <-- Sort <-- inner Project (items: [o_orderkey__nr_sel_0, o_custkey_extra])
    let plan = parse_analyze_and_plan("SELECT o_orderkey FROM orders ORDER BY o_custkey")
        .expect("planner should succeed");

    // Walk down to find the outer and inner Projects.
    // Shape: outer-Project? <- Sort <- inner-Project <- Scan
    let outer_proj = match &plan.kind {
        LogicalPlanKind::Project(p) => p,
        other => {
            // If there is no outer Project (no extra items triggered), skip.
            // The test is only meaningful when the strip-projection was built.
            let shape = format!("{other:?}");
            if shape.contains("Sort") {
                return; // no extra items path — test not applicable
            }
            panic!("expected Project or Sort root, got {shape}");
        }
    };

    let sort_plan = plan.unary_input();
    let inner_proj = match &sort_plan.kind {
        LogicalPlanKind::Sort(_) => match &sort_plan.unary_input().kind {
            LogicalPlanKind::Project(p) => p,
            other => panic!("expected inner Project under Sort, got {other:?}"),
        },
        other => panic!("expected Sort under outer Project, got {other:?}"),
    };

    // The outer strip-project has one user-visible item (o_orderkey).
    // Its output_column_id and expr ColumnRef column_id must match the
    // corresponding inner project item's output_column_id.
    assert!(
        !outer_proj.items.is_empty(),
        "outer strip-project must have at least one item"
    );
    let outer_item = &outer_proj.items[0];
    let outer_expr_cid = match &outer_item.expr.kind {
        ExprKind::ColumnRef { column_id, .. } => *column_id,
        other => panic!("outer strip-project item must be ColumnRef, got {other:?}"),
    };

    // Find the inner project item that was renamed to __nr_sel_0 and
    // corresponds to position 0.
    let inner_item_0 = &inner_proj.items[0];
    let inner_output_cid = inner_item_0.output_column_id;

    assert_ne!(
        outer_expr_cid,
        ColumnId::UNSET,
        "outer strip-project ColumnRef must not be UNSET"
    );
    assert_eq!(
        outer_expr_cid, inner_output_cid,
        "outer strip-project item's ColumnRef column_id must equal inner project item's \
             output_column_id at the same position (Bug C: no fresh id minting)"
    );
    assert_eq!(
        outer_item.output_column_id, inner_output_cid,
        "outer strip-project item's output_column_id must equal inner project item's \
             output_column_id at the same position (Bug C: no fresh id minting)"
    );
}

#[test]
fn sort_only_expression_extra_uses_traceable_output_column_id() {
    let plan = parse_analyze_and_plan(
        "SELECT o_orderkey FROM orders ORDER BY abs(o_custkey - o_orderkey)",
    )
    .expect("planner should succeed");

    let outer_proj = match &plan.kind {
        LogicalPlanKind::Project(p) => p,
        other => panic!("expected outer strip Project, got {other:?}"),
    };
    let sort_plan = plan.unary_input();
    let sort = match &sort_plan.kind {
        LogicalPlanKind::Sort(s) => s,
        other => panic!("expected Sort under outer Project, got {other:?}"),
    };
    let inner_proj = match &sort_plan.unary_input().kind {
        LogicalPlanKind::Project(p) => p,
        other => panic!("expected inner Project under Sort, got {other:?}"),
    };

    let extra_item = inner_proj
        .items
        .iter()
        .find(|item| item.output_name.starts_with("abs("))
        .expect("expected sort-only expression item");
    assert_ne!(
        extra_item.output_column_id,
        ColumnId::UNSET,
        "sort-only expression extra must have a real output ColumnId so pruning can track it"
    );

    let ExprKind::ColumnRef {
        column_id: sort_key_id,
        ..
    } = sort.items[0].expr.kind
    else {
        panic!("sort key should be rewritten to a ColumnRef");
    };
    assert_eq!(
        sort_key_id, extra_item.output_column_id,
        "sort key must reference the sort-only expression extra by ColumnId"
    );
}

#[test]
fn order_by_computed_select_alias_reuses_project_output_column_id() {
    let plan =
        parse_analyze_and_plan("SELECT o_orderkey * 2 AS revenue FROM orders ORDER BY revenue")
            .expect("planner should succeed");

    let sort = match &plan.kind {
        LogicalPlanKind::Sort(s) => s,
        other => panic!("expected Sort root, got {other:?}"),
    };
    let project = match &plan.unary_input().kind {
        LogicalPlanKind::Project(p) => p,
        other => panic!("expected Project under Sort, got {other:?}"),
    };
    let project_output_id = project.items[0].output_column_id;
    let ExprKind::ColumnRef {
        column_id: sort_key_id,
        ..
    } = sort.items[0].expr.kind
    else {
        panic!("sort key should be a ColumnRef to the select alias");
    };

    assert_ne!(
        project_output_id,
        ColumnId::UNSET,
        "computed select alias must have a real output ColumnId"
    );
    assert_eq!(
        sort_key_id, project_output_id,
        "ORDER BY select alias must point at the Project output ColumnId"
    );
}

#[test]
fn p2_order_by_select_alias_extra_path_preserves_inner_output_id() {
    let plan = parse_analyze_and_plan("SELECT o_orderkey AS x FROM orders ORDER BY x, o_custkey")
        .expect("planner should succeed");

    let outer_proj = match &plan.kind {
        LogicalPlanKind::Project(p) => p,
        other => panic!("expected outer strip Project, got {other:?}"),
    };
    let sort_plan = plan.unary_input();
    let sort = match &sort_plan.kind {
        LogicalPlanKind::Sort(s) => s,
        other => panic!("expected Sort under outer Project, got {other:?}"),
    };
    let inner_proj = match &sort_plan.unary_input().kind {
        LogicalPlanKind::Project(p) => p,
        other => panic!("expected inner Project under Sort, got {other:?}"),
    };

    let inner_output_id = inner_proj.items[0].output_column_id;
    assert_ne!(
        inner_output_id,
        ColumnId::UNSET,
        "inner select alias output must have a real ColumnId"
    );
    let ExprKind::ColumnRef {
        column_id, column, ..
    } = &sort.items[0].expr.kind
    else {
        panic!("sort key should be a ColumnRef to the remapped select alias");
    };
    assert_eq!(
        column, "__nr_sel_0",
        "ORDER BY alias should remap to the synthetic inner Project label"
    );
    assert_eq!(
        *column_id, inner_output_id,
        "ORDER BY alias remap must preserve the inner Project output ColumnId"
    );
}

#[test]
fn order_by_positions_preserve_duplicate_output_column_ids() {
    let plan = parse_analyze_and_plan(
        "SELECT l.a, r.a FROM t l FULL JOIN t r ON l.a != r.a ORDER BY 1, 2",
    )
    .expect("planner should succeed");

    let sort = match &plan.kind {
        LogicalPlanKind::Sort(sort) => sort,
        other => panic!("expected Sort root, got {other:?}"),
    };
    let project = match &plan.unary_input().kind {
        LogicalPlanKind::Project(project) => project,
        other => panic!("expected Project under Sort, got {other:?}"),
    };
    assert_eq!(project.items.len(), 2);
    assert_eq!(project.items[0].output_name, "a");
    assert_eq!(project.items[1].output_name, "a");

    let first_output_id = project.items[0].output_column_id;
    let second_output_id = project.items[1].output_column_id;
    assert_ne!(first_output_id, ColumnId::UNSET);
    assert_ne!(second_output_id, ColumnId::UNSET);
    assert_ne!(first_output_id, second_output_id);

    let sort_ids = sort
        .items
        .iter()
        .map(|item| match &item.expr.kind {
            ExprKind::ColumnRef { column_id, .. } => *column_id,
            other => panic!("expected sort key ColumnRef, got {other:?}"),
        })
        .collect::<Vec<_>>();

    assert_eq!(sort_ids, vec![first_output_id, second_output_id]);
}

#[test]
fn p2_order_by_derived_values_extra_preserves_source_column_id() {
    let plan = parse_analyze_and_plan("SELECT 1 FROM (VALUES (1, 2)) AS v(a, b) ORDER BY v.b + 1")
        .expect("planner should succeed");

    let (_, _, inner_proj) = root_strip_sort_inner_project(&plan);
    let inner_proj_plan = plan.unary_input().unary_input();
    let child_output_columns = plan_output_columns(inner_proj_plan.unary_input())
        .expect("VALUES child output should be known");
    assert_eq!(
        child_output_columns.len(),
        2,
        "derived VALUES child should expose both columns"
    );
    let b_output_id = child_output_columns[1].column_id;
    assert_ne!(
        b_output_id,
        ColumnId::UNSET,
        "derived VALUES b output must have a real ColumnId"
    );

    let extra_item = inner_proj
        .items
        .last()
        .expect("sort-extra ProjectItem should be appended");
    assert_eq!(
        binary_left_column_id(&extra_item.expr),
        b_output_id,
        "ORDER BY v.b + 1 extra must reference the derived VALUES source ColumnId"
    );
}

#[test]
fn p2_order_by_generate_series_extra_preserves_source_column_id() {
    let plan = parse_analyze_and_plan(
        "SELECT 1 FROM TABLE(generate_series(1, 3, 1)) AS gs(x) ORDER BY gs.x + 1",
    )
    .expect("planner should succeed");

    let (_, _, inner_proj) = root_strip_sort_inner_project(&plan);
    let inner_proj_plan = plan.unary_input().unary_input();
    let child_output_columns = plan_output_columns(inner_proj_plan.unary_input())
        .expect("GenerateSeries child output should be known");
    assert_eq!(
        child_output_columns.len(),
        1,
        "GenerateSeries child should expose one column"
    );
    let source_output_id = child_output_columns[0].column_id;
    assert_ne!(
        source_output_id,
        ColumnId::UNSET,
        "GenerateSeries output must have a real ColumnId"
    );

    let extra_item = inner_proj
        .items
        .last()
        .expect("sort-extra ProjectItem should be appended");
    assert_eq!(
        binary_left_column_id(&extra_item.expr),
        source_output_id,
        "ORDER BY gs.x + 1 extra must reference the GenerateSeries source ColumnId"
    );
}

#[test]
fn p2_values_output_uses_single_column_id() {
    let (resolved, cte_registry, mut factory) =
        parse_analyze_query("VALUES (1, 2), (3, 4)").expect("analyzer should succeed");
    let analyzer_output_columns = resolved.output_columns.clone();
    let plan = plan_query(resolved, cte_registry, &mut factory).expect("planner should succeed");
    let LogicalPlanKind::Values(values) = &plan.kind else {
        panic!("expected Values root");
    };
    assert_eq!(
        values.columns.len(),
        analyzer_output_columns.len(),
        "PlanValuesNode should expose the analyzer output columns"
    );
    for (value_column, analyzer_column) in values.columns.iter().zip(analyzer_output_columns.iter())
    {
        assert_ne!(
            value_column.column_id,
            ColumnId::UNSET,
            "VALUES output column must have a real ColumnId"
        );
        assert_eq!(
            value_column.column_id, analyzer_column.column_id,
            "PlanValuesNode column id must reuse the analyzer query output id"
        );
    }
}

#[test]
fn p2_generate_series_output_has_column_id_through_planner() {
    let plan = parse_analyze_and_plan("SELECT x FROM TABLE(generate_series(1, 3, 1)) AS gs(x)")
        .expect("planner should succeed");
    let LogicalPlanKind::Project(project) = &plan.kind else {
        panic!("expected Project root, got {plan:?}");
    };
    let child_output_columns =
        plan_output_columns(plan.unary_input()).expect("generate_series output should be known");
    assert_eq!(
        child_output_columns.len(),
        1,
        "generate_series should expose one output column"
    );
    let child_output_id = child_output_columns[0].column_id;
    assert_ne!(
        child_output_id,
        ColumnId::UNSET,
        "GenerateSeries output must have a real ColumnId"
    );
    let ExprKind::ColumnRef { column_id, .. } = project.items[0].expr.kind else {
        panic!("project over generate_series should read a ColumnRef");
    };
    assert_eq!(
        column_id, project.items[0].output_column_id,
        "Project item should preserve the generate_series ColumnRef id"
    );
    assert_eq!(
        column_id, child_output_id,
        "GenerateSeries child output id must match the parent Project ColumnRef"
    );
}

#[test]
fn p2_base_scan_row_lineage_metadata_preserves_column_id_through_planner() {
    let plan = parse_analyze_and_plan("SELECT _row_id FROM iv_orders AS t")
        .expect("planner should succeed");
    let LogicalPlanKind::Project(project) = &plan.kind else {
        panic!("expected Project root, got {plan:?}");
    };
    let LogicalPlanKind::Scan(scan) = &plan.unary_input().kind else {
        panic!("expected Scan under Project, got {:?}", plan.unary_input());
    };

    let row_id_output = scan
        .columns
        .iter()
        .find(|col| col.name == "_row_id")
        .expect("PlanScanNode must expose _row_id metadata output");
    assert_ne!(row_id_output.column_id, ColumnId::UNSET);

    let ExprKind::ColumnRef { column_id, .. } = project.items[0].expr.kind else {
        panic!("Project over _row_id should read a ColumnRef");
    };
    assert_eq!(
        column_id, row_id_output.column_id,
        "Project must read the _row_id ColumnId exposed by the PlanScanNode"
    );
    assert_eq!(
        project.items[0].output_column_id, row_id_output.column_id,
        "visible _row_id output should preserve the PlanScanNode metadata ColumnId"
    );
}

#[test]
fn p2_rollup_materialized_key_has_real_id() {
    let plan = parse_analyze_and_plan("SELECT a + 1 AS k FROM t GROUP BY ROLLUP(a + 1)")
        .expect("planner should succeed");
    let (_project, aggregate) = root_project_over_aggregate(&plan);
    let (repeat_plan, repeat) = first_repeat_node(&plan);
    let LogicalPlanKind::Project(repeat_input_project) = &repeat_plan.unary_input().kind else {
        panic!(
            "expected Repeat input Project, got {:?}",
            repeat_plan.unary_input()
        );
    };
    let repeat_key = repeat_input_project
        .items
        .iter()
        .find(|item| item.output_name == "__repeat_group_key_0")
        .expect("computed rollup key should be materialized before Repeat");
    assert_ne!(
        repeat_key.output_column_id,
        ColumnId::UNSET,
        "computed ROLLUP key materialization must have a real ColumnId"
    );
    assert_eq!(
        column_ref_id(&aggregate.group_by[0]),
        repeat_key.output_column_id,
        "Aggregate over Repeat must group by the materialized key ColumnId"
    );
}

#[test]
fn p2_rollup_column_key_uses_distinct_repeat_materialization_id() {
    let plan = parse_analyze_and_plan("SELECT a, count(*) FROM t GROUP BY ROLLUP(a)")
        .expect("planner should succeed");
    let (_project, aggregate) = root_project_over_aggregate(&plan);
    let (repeat_plan, repeat) = first_repeat_node(&plan);
    let LogicalPlanKind::Project(repeat_input_project) = &repeat_plan.unary_input().kind else {
        panic!(
            "expected Repeat input Project, got {:?}",
            repeat_plan.unary_input()
        );
    };
    let source_a = repeat_input_project
        .items
        .iter()
        .find(|item| item.output_name == "a")
        .expect("Repeat input should preserve original source column");
    let repeat_key = repeat_input_project
        .items
        .iter()
        .find(|item| item.output_name == "__repeat_group_key_0")
        .expect("ROLLUP key should be materialized before Repeat");

    assert_ne!(
        source_a.output_column_id, repeat_key.output_column_id,
        "Repeat grouping key must not reuse the source ColumnId"
    );
    assert_eq!(
        repeat.all_rollup_column_ids,
        vec![repeat_key.output_column_id],
        "Repeat metadata must point at the materialized key id"
    );
    assert_eq!(
        column_ref_id(&aggregate.group_by[0]),
        repeat_key.output_column_id,
        "Aggregate over Repeat must group by the nullified materialized key"
    );
}

#[test]
fn p2_repeat_output_columns_include_grouping_function_slots() {
    let plan =
        parse_analyze_and_plan("SELECT grouping(a) AS g, a, count(*) FROM t GROUP BY ROLLUP(a)")
            .expect("planner should succeed");
    let (repeat_plan, repeat) = first_repeat_node(&plan);
    let grouping_id = repeat
        .grouping_fn_ids
        .iter()
        .find(|(name, _)| name == "__grouping_fn_0")
        .map(|(_, id)| *id)
        .expect("ROLLUP should produce grouping function metadata");

    let repeat_outputs = plan_output_columns(repeat_plan).expect("Repeat output columns");

    assert!(
        repeat_outputs
            .iter()
            .any(|column| column.column_id == grouping_id && column.name == "__grouping_fn_0"),
        "Repeat output columns must expose generated GROUPING() ColumnId"
    );
}

#[test]
fn p2_subquery_alias_reexposes_producing_id() {
    let plan = parse_analyze_and_plan("SELECT x FROM (SELECT a AS x FROM t) s WHERE x > 1")
        .expect("planner should succeed");
    let LogicalPlanKind::Project(project) = &plan.kind else {
        panic!("expected Project root, got {plan:?}");
    };
    let filter_plan = plan.unary_input();
    let LogicalPlanKind::Filter(filter) = &filter_plan.kind else {
        panic!("expected Filter under Project, got {:?}", filter_plan);
    };
    let child_output =
        plan_output_columns(filter_plan.unary_input()).expect("child output columns");
    let producing_id = child_output
        .iter()
        .find(|col| col.name == "x")
        .expect("subquery child should expose x")
        .column_id;
    assert_ne!(
        producing_id,
        ColumnId::UNSET,
        "subquery alias producer must expose a real ColumnId"
    );
    let ExprKind::BinaryOp { left, .. } = &filter.predicate.kind else {
        panic!(
            "expected WHERE x > 1 binary predicate, got {:?}",
            filter.predicate.kind
        );
    };
    assert_eq!(
        column_ref_id(left),
        producing_id,
        "outer WHERE x must reuse the subquery producer ColumnId"
    );
    assert_eq!(
        column_ref_id(&project.items[0].expr),
        producing_id,
        "outer SELECT x must reuse the subquery producer ColumnId"
    );
}

#[test]
fn p2_full_outer_using_order_by_uses_project_output_id() {
    let plan = parse_analyze_and_plan(
        "SELECT a AS merged FROM t l FULL OUTER JOIN t r USING(a) ORDER BY merged",
    )
    .expect("planner should succeed");
    let LogicalPlanKind::Sort(sort) = &plan.kind else {
        panic!("expected Sort root, got {plan:?}");
    };
    let LogicalPlanKind::Project(project) = &plan.unary_input().kind else {
        panic!("expected Project under Sort, got {:?}", plan.unary_input());
    };
    let merged_output_id = project.items[0].output_column_id;
    assert_ne!(
        merged_output_id,
        ColumnId::UNSET,
        "FULL OUTER USING merged projection must have a real output ColumnId"
    );
    assert_eq!(
        column_ref_id(&sort.items[0].expr),
        merged_output_id,
        "ORDER BY merged must reference the FULL OUTER USING project output ColumnId"
    );
}

#[test]
fn qualified_order_by_selected_column_does_not_create_sort_extra() {
    let plan = parse_analyze_and_plan(
        "SELECT s.o_orderkey FROM (SELECT o_orderkey FROM orders) s ORDER BY s.o_orderkey",
    )
    .expect("planner should succeed");

    let sort = match &plan.kind {
        LogicalPlanKind::Sort(s) => s,
        other => {
            panic!("qualified ORDER BY selected column must not add strip Project: {other:?}")
        }
    };
    let project = match &plan.unary_input().kind {
        LogicalPlanKind::Project(p) => p,
        other => panic!("expected SELECT Project under Sort, got {other:?}"),
    };
    assert_eq!(
        project.items.len(),
        1,
        "selected ORDER BY column must not be appended as a sort-only extra"
    );
    let ExprKind::ColumnRef {
        column_id: sort_key_id,
        ..
    } = sort.items[0].expr.kind
    else {
        panic!("sort key should remain a ColumnRef");
    };
    assert_eq!(
        sort_key_id, project.items[0].output_column_id,
        "sort key should reference the selected output column by ColumnId"
    );
}

#[test]
fn apply_output_columns_extend_left_with_output_column() {
    use std::collections::HashSet;

    use arrow::datatypes::DataType;

    use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::common::ApplyKind;
    use crate::sql::planner::logical::{LogicalApplyNode, LogicalPlanKind};
    use crate::sql::planner::payload::PlanValuesNode;

    let left_col = OutputColumn {
        column_id: ColumnId(11),
        name: "l1".to_string(),
        data_type: DataType::Int64,
        nullable: false,
        is_internal: false,
    };
    let out_col = OutputColumn {
        column_id: ColumnId(12),
        name: "__sq_1".to_string(),
        data_type: DataType::Int64,
        nullable: true,
        is_internal: true,
    };
    let plan = LogicalPlanNode::new(
        LogicalPlanKind::Apply(LogicalApplyNode {
            kind: ApplyKind::Scalar,
            subquery_expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: ColumnId(12),
                    qualifier: None,
                    column: "__sq_1".to_string(),
                },
                data_type: DataType::Int64,
                nullable: true,
            },
            output_column: out_col.clone(),
            inner_output_column_id: out_col.column_id,
            correlation_column_ids: vec![],
            correlation_conjuncts: vec![],
            residual_predicate: None,
            need_check_max_rows: true,
            use_semi_anti: false,
            uncorrelated_outer_predicate_columns: HashSet::new(),
        }),
        vec![
            LogicalPlanNode::new(
                LogicalPlanKind::Values(PlanValuesNode {
                    rows: vec![],
                    columns: vec![left_col.clone()],
                }),
                vec![],
                None,
            ),
            LogicalPlanNode::new(
                LogicalPlanKind::Values(PlanValuesNode {
                    rows: vec![],
                    columns: vec![],
                }),
                vec![],
                None,
            ),
        ],
        None,
    );

    let columns = plan_output_columns(&plan).expect("apply output columns");
    assert_eq!(columns.len(), 2);
    assert_eq!(columns[0].column_id, left_col.column_id);
    assert_eq!(columns[1].column_id, out_col.column_id);
}

#[test]
fn assert_one_row_output_columns_pass_through() {
    use arrow::datatypes::DataType;

    use crate::sql::analysis::OutputColumn;
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::logical::LogicalPlanKind;
    use crate::sql::planner::payload::{PlanAssertOneRowNode, PlanValuesNode};

    let col = OutputColumn {
        column_id: ColumnId(21),
        name: "c1".to_string(),
        data_type: DataType::Int64,
        nullable: false,
        is_internal: false,
    };
    let plan = LogicalPlanNode::new(
        LogicalPlanKind::AssertOneRow(PlanAssertOneRowNode::global_at_most_one("select 1")),
        vec![LogicalPlanNode::new(
            LogicalPlanKind::Values(PlanValuesNode {
                rows: vec![],
                columns: vec![col.clone()],
            }),
            vec![],
            None,
        )],
        None,
    );

    let columns = plan_output_columns(&plan).expect("assert output columns");
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0].column_id, col.column_id);
}

// -------------------------------------------------------------------
// Apply spec placement tests (Task 4)
// -------------------------------------------------------------------

/// Recursive helper: returns true if `e` (or any sub-expression) contains a
/// `ColumnRef` with the given `ColumnId`.  Used by placement tests to verify
/// that filter/projection predicates reference the Apply output column.
fn expr_references_col(e: &TypedExpr, id: ColumnId) -> bool {
    match &e.kind {
        ExprKind::ColumnRef { column_id, .. } => *column_id == id,
        ExprKind::BinaryOp { left, right, .. } => {
            expr_references_col(left, id) || expr_references_col(right, id)
        }
        ExprKind::UnaryOp { expr, .. } => expr_references_col(expr, id),
        ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => {
            args.iter().any(|a| expr_references_col(a, id))
        }
        ExprKind::Cast { expr, .. } => expr_references_col(expr, id),
        _ => false,
    }
}

/// WHERE-clause scalar subquery in Apply framework: the plan must contain an
/// Apply node between the FROM Scan and the WHERE Filter, and the Apply's
/// output_column must appear in the plan's output column set.
#[test]
fn apply_where_spec_emits_apply_below_where_filter() {
    // t1.k1 = (SELECT max(k2) FROM t2 WHERE t2.k1 = t1.k1)
    let sql = "SELECT k1 FROM t1 WHERE k1 = (SELECT max(k2) FROM t2 WHERE t2.k1 = t1.k1)";
    let plan = parse_analyze_and_plan_apply(sql).expect("Apply framework plan must succeed");

    // Root shape: Project → Filter(WHERE) → Apply → Scan
    let LogicalPlanKind::Project(project) = &plan.kind else {
        panic!("expected Project root, got {plan:?}");
    };
    let filter_plan = plan.unary_input();
    let LogicalPlanKind::Filter(filter) = &filter_plan.kind else {
        panic!("expected Filter under Project, got {:?}", filter_plan);
    };
    // The Apply must be directly under the WHERE Filter.
    let apply_plan = filter_plan.unary_input();
    let LogicalPlanKind::Apply(apply) = &apply_plan.kind else {
        panic!("expected Apply under WHERE Filter, got {:?}", apply_plan);
    };
    assert_eq!(
        apply.kind,
        crate::sql::common::ApplyKind::Scalar,
        "Apply kind must be Scalar"
    );
    // Apply.left must be the FROM Scan.
    assert!(
        matches!(&apply_plan.left().kind, LogicalPlanKind::Scan(_)),
        "Apply.left must be the FROM Scan, got {:?}",
        apply_plan.left()
    );
    // The WHERE Filter's predicate must reference the Apply output column
    // so that the filter can consume the scalar value.
    let apply_col_id = apply.output_column.column_id;
    assert!(
        expr_references_col(&filter.predicate, apply_col_id),
        "WHERE predicate must reference the Apply output column {:?}",
        apply_col_id
    );
}

/// HAVING-clause scalar subquery in Apply framework: the Apply must appear
/// between the Aggregate and the HAVING Filter.
#[test]
fn apply_having_spec_emits_apply_above_aggregate() {
    let sql = "SELECT k1, max(k2) FROM t1 GROUP BY k1 \
                   HAVING max(k2) > (SELECT max(k2) FROM t2 WHERE t2.k1 = t1.k1)";
    let plan =
        parse_analyze_and_plan_apply(sql).expect("Apply framework plan must succeed for HAVING");

    // Walk down: Project → Filter(HAVING) → Apply → Aggregate → ...
    let LogicalPlanKind::Project(project) = &plan.kind else {
        panic!("expected Project root, got {plan:?}");
    };
    let having_filter_plan = plan.unary_input();
    let LogicalPlanKind::Filter(having_filter) = &having_filter_plan.kind else {
        panic!(
            "expected HAVING Filter under Project, got {:?}",
            having_filter_plan
        );
    };
    let apply_plan = having_filter_plan.unary_input();
    let LogicalPlanKind::Apply(apply) = &apply_plan.kind else {
        panic!(
            "expected Apply directly under HAVING Filter, got {:?}",
            apply_plan
        );
    };
    assert_eq!(apply.kind, crate::sql::common::ApplyKind::Scalar);
    // Apply.left must be the Aggregate.
    assert!(
        matches!(&apply_plan.left().kind, LogicalPlanKind::Aggregate(_)),
        "Apply.left for HAVING spec must be the Aggregate, got {:?}",
        apply_plan.left()
    );
    // The HAVING Filter's predicate must reference the Apply output column.
    let apply_col_id = apply.output_column.column_id;
    assert!(
        expr_references_col(&having_filter.predicate, apply_col_id),
        "HAVING predicate must reference Apply output column {:?}",
        apply_col_id
    );
}

/// Projection-clause scalar subquery in Apply framework: the Apply must appear
/// below the Project node (Project is above Apply).
#[test]
fn apply_projection_spec_emits_apply_below_project() {
    let sql = "SELECT k1, (SELECT max(k2) FROM t2 WHERE t2.k1 = t1.k1) AS sub FROM t1";
    let plan = parse_analyze_and_plan_apply(sql)
        .expect("Apply framework plan must succeed for Projection");

    // Root must be Project; its input must be Apply.
    let LogicalPlanKind::Project(project) = &plan.kind else {
        panic!("expected Project root, got {plan:?}");
    };
    let apply_plan = plan.unary_input();
    let LogicalPlanKind::Apply(apply) = &apply_plan.kind else {
        panic!(
            "expected Apply directly under Project, got {:?}",
            apply_plan
        );
    };
    assert_eq!(apply.kind, crate::sql::common::ApplyKind::Scalar);
    // Apply.left must be the FROM Scan.
    assert!(
        matches!(&apply_plan.left().kind, LogicalPlanKind::Scan(_)),
        "Apply.left for Projection spec must be FROM Scan, got {:?}",
        apply_plan.left()
    );
    // The Apply's output_column must appear in the Project's items.
    let apply_col_id = apply.output_column.column_id;
    let projected = project.items.iter().any(|item| {
        matches!(
            &item.expr.kind,
            ExprKind::ColumnRef { column_id, .. } if *column_id == apply_col_id
        )
    });
    assert!(
        projected,
        "Projection must reference the Apply output column"
    );
}

fn plan_with_single_predicate_apply_spec(
    sql: &str,
) -> (LogicalPlanNode, crate::sql::analysis::ApplyPredicateSpec) {
    use crate::sql::analysis::QueryBody;

    let (resolved, cte_registry, mut factory) =
        parse_analyze_query_apply(sql).expect("Apply framework analyze must succeed");
    let QueryBody::Select(select) = &resolved.body else {
        panic!("expected SELECT body");
    };
    assert_eq!(
        select.predicate_apply_specs.len(),
        1,
        "test query must record exactly one predicate apply spec"
    );
    let spec = select.predicate_apply_specs[0].clone();
    let plan = plan_query(resolved, cte_registry, &mut factory)
        .expect("planner must consume predicate apply spec");
    (plan, spec)
}

fn direct_where_apply(plan: &LogicalPlanNode) -> &LogicalApplyNode {
    let LogicalPlanKind::Project(_) = &plan.kind else {
        panic!("expected Project root, got {plan:?}");
    };
    let project_input = plan.unary_input();
    match &project_input.kind {
        LogicalPlanKind::Filter(_) => {
            let apply_plan = project_input.unary_input();
            let LogicalPlanKind::Apply(apply) = &apply_plan.kind else {
                panic!(
                    "expected Apply directly below WHERE Filter, got {:?}",
                    apply_plan
                );
            };
            apply
        }
        LogicalPlanKind::Apply(apply) => apply,
        other => panic!("expected Filter->Apply or Apply below Project, got {other:?}"),
    }
}

fn assert_same_column_ref_expr(actual: &TypedExpr, expected: &TypedExpr) {
    assert_eq!(actual.data_type, expected.data_type);
    assert_eq!(actual.nullable, expected.nullable);
    let ExprKind::ColumnRef {
        column_id: actual_id,
        qualifier: actual_qualifier,
        column: actual_column,
    } = &actual.kind
    else {
        panic!("actual expression must be a ColumnRef, got {actual:?}");
    };
    let ExprKind::ColumnRef {
        column_id: expected_id,
        qualifier: expected_qualifier,
        column: expected_column,
    } = &expected.kind
    else {
        panic!("expected expression must be a ColumnRef, got {expected:?}");
    };
    assert_eq!(actual_id, expected_id);
    assert_eq!(actual_qualifier, expected_qualifier);
    assert_eq!(actual_column, expected_column);
}

#[test]
fn plan_exists_builds_apply_exists() {
    let sql = "SELECT k1 FROM t1 WHERE k1 > 0 \
                   AND EXISTS (SELECT 1 FROM t2 WHERE t2.k1 = t1.k1)";
    let (plan, spec) = plan_with_single_predicate_apply_spec(sql);
    assert!(
        !spec.correlation_column_ids.is_empty(),
        "test query must record a correlated EXISTS predicate spec"
    );

    let LogicalPlanKind::Project(project) = &plan.kind else {
        panic!("expected Project root, got {plan:?}");
    };
    let filter_plan = plan.unary_input();
    let LogicalPlanKind::Filter(filter) = &filter_plan.kind else {
        panic!(
            "expected residual WHERE Filter under Project, got {:?}",
            filter_plan
        );
    };
    let apply_plan = filter_plan.unary_input();
    let LogicalPlanKind::Apply(apply) = &apply_plan.kind else {
        panic!(
            "expected Apply directly below WHERE Filter, got {:?}",
            apply_plan
        );
    };
    assert_eq!(
        apply.kind,
        crate::sql::common::ApplyKind::Exists { negated: false }
    );
    assert_eq!(apply.correlation_column_ids, spec.correlation_column_ids);
    assert!(apply.use_semi_anti);
    assert!(!apply.need_check_max_rows);
    assert!(apply.correlation_conjuncts.is_empty());
}

#[test]
fn plan_not_in_builds_apply_in_negated() {
    let sql = "SELECT k1 FROM t1 WHERE t1.k1 NOT IN (SELECT t2.k2 FROM t2)";
    let (plan, spec) = plan_with_single_predicate_apply_spec(sql);
    assert!(
        spec.correlation_column_ids.is_empty(),
        "test query must record an uncorrelated NOT IN predicate spec"
    );
    let apply = direct_where_apply(&plan);

    assert_eq!(
        apply.kind,
        crate::sql::common::ApplyKind::In { negated: true }
    );
    assert!(apply.correlation_column_ids.is_empty());
    let expected_lhs = spec
        .in_lhs
        .expect("IN predicate apply spec must carry analyzed LHS");
    assert_same_column_ref_expr(&apply.subquery_expr, &expected_lhs);
}

#[test]
fn plan_exists_subquery_expr_is_boolean_colref() {
    let sql = "SELECT k1 FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.k1 = t1.k1)";
    let (plan, spec) = plan_with_single_predicate_apply_spec(sql);
    let apply = direct_where_apply(&plan);

    assert_eq!(
        apply.subquery_expr.data_type,
        arrow::datatypes::DataType::Boolean
    );
    assert_eq!(
        apply.subquery_expr.nullable, spec.output_column.nullable,
        "EXISTS subquery_expr must mirror the Boolean predicate output nullability"
    );
    let ExprKind::ColumnRef { column_id, .. } = apply.subquery_expr.kind else {
        panic!(
            "EXISTS subquery_expr must be a Boolean ColumnRef, got {:?}",
            apply.subquery_expr
        );
    };
    assert_eq!(
        column_id, spec.output_column.column_id,
        "EXISTS subquery_expr must reference the predicate output column"
    );
}
