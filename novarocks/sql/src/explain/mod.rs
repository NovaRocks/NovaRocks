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

//! EXPLAIN plan formatter for logical plans and shared expression formatting.

pub(crate) mod completed;
pub(crate) mod completed_tree;
mod logical;

use crate::planner::logical::LogicalPlanNode;

/// Detail level for EXPLAIN output.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExplainLevel {
    Normal,
    Verbose,
    Costs,
    /// Node-level Verbose output with an application-owned runtime header.
    Analyze,
    /// The frozen plan contract rather than its operators.
    Contract,
}

/// Render a logical tree without materializing an unbounded intermediate line.
#[allow(dead_code)]
pub(crate) fn explain_plan_checked(
    plan: &LogicalPlanNode,
    level: ExplainLevel,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Vec<String>, crate::compiler::SqlCompileError> {
    logical::render_observed(
        plan,
        level,
        completed::ExplainRenderBudget::default(),
        control,
    )
}

#[allow(dead_code)]
pub(crate) fn explain_plan(plan: &LogicalPlanNode, level: ExplainLevel) -> Vec<String> {
    explain_plan_checked(
        plan,
        level,
        &crate::compiler::SqlCompileControl::unbounded(),
    )
    .expect("invalid logical plan stage")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PlanNodeExplainStage {
    Logical,
    #[cfg(test)]
    Distributed,
}

// These old convenience APIs have no production callers. Keep only thin test
// wrappers around the same borrowed renderer, rather than a second formatter.
#[cfg(test)]
fn format_shared_plan_node_header(
    kind: &crate::planner::logical::LogicalPlanKind,
    stage: PlanNodeExplainStage,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Option<String>, crate::compiler::SqlCompileError> {
    use crate::planner::logical::LogicalPlanKind;
    match kind {
        LogicalPlanKind::Scan(_)
        | LogicalPlanKind::Filter(_)
        | LogicalPlanKind::Project(_)
        | LogicalPlanKind::Sort(_)
        | LogicalPlanKind::Window(_)
        | LogicalPlanKind::Values(_)
        | LogicalPlanKind::Repeat(_)
        | LogicalPlanKind::GenerateSeries(_)
        | LogicalPlanKind::TableFunction(_)
        | LogicalPlanKind::AssertOneRow(_) => {
            let diagnostic = completed::RenderDiagnostic::new(control);
            let mut output = completed::ExplainRenderOutput::new_observed(
                completed::ExplainRenderBudget::default(),
                control,
            )?;
            let result = output.push(format_args!(
                "{}",
                logical::Header(kind, stage, Some(&diagnostic))
            ));
            if let Some(error) = diagnostic.take_error() {
                return Err(error);
            }
            result?;
            Ok(output.finish_observed()?.pop())
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
fn format_expr(
    expr: &crate::analysis::TypedExpr,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<String, crate::compiler::SqlCompileError> {
    let diagnostic = completed::RenderDiagnostic::new(control);
    let mut output = completed::ExplainRenderOutput::new_observed(
        completed::ExplainRenderBudget::default(),
        control,
    )?;
    let result = output.push(format_args!(
        "{}",
        logical::Expression(expr, Some(&diagnostic))
    ));
    if let Some(error) = diagnostic.take_error() {
        return Err(error);
    }
    result?;
    Ok(output.finish_observed()?.pop().expect("one test line"))
}

#[cfg(test)]
fn format_project_item(
    item: &crate::analysis::ProjectItem,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<String, crate::compiler::SqlCompileError> {
    let diagnostic = completed::RenderDiagnostic::new(control);
    let mut output = completed::ExplainRenderOutput::new_observed(
        completed::ExplainRenderBudget::default(),
        control,
    )?;
    let result = output.push(format_args!(
        "{}",
        logical::Project(item, Some(&diagnostic))
    ));
    if let Some(error) = diagnostic.take_error() {
        return Err(error);
    }
    result?;
    Ok(output.finish_observed()?.pop().expect("one test line"))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::num::{NonZeroU32, NonZeroU64};

    use arrow::datatypes::DataType;

    use super::{
        ExplainLevel, PlanNodeExplainStage, explain_plan, format_expr, format_project_item,
        format_shared_plan_node_header,
    };
    use crate::analysis::{
        BinOp, ExprKind, LiteralValue, OutputColumn, ProjectItem, SortItem, TypedExpr,
    };
    use crate::binding::{SqlTableBindingId, SqlTableBindingScopeId};
    use crate::column_id::ColumnId;
    use crate::common::ApplyKind;
    use crate::planner::logical::{LogicalApplyNode, LogicalPlanKind, LogicalPlanNode};
    use crate::planner::payload::{
        PlanAssertOneRowNode, PlanFilterNode, PlanProjectNode, PlanScanNode, PlanValuesNode,
        PlanWindowNode, WindowExpr,
    };
    use crate::planner::table::{
        ScanSource, SqlMvTargetLocatorScan, SqlScanKind, SqlScanSource, SqlTableIdentity, TableDef,
    };
    use novarocks_types::schema::ColumnDef;

    fn empty_values_for_test() -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Values(PlanValuesNode {
                rows: vec![],
                columns: vec![],
            }),
            vec![],
            None,
        )
    }

    fn output_column(id: u32, name: &str, data_type: DataType, nullable: bool) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(data_type, nullable),

            is_internal: false,
        }
    }

    fn column_def(name: &str, data_type: DataType, nullable: bool) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type,
            nullable,
            write_default: None,
            logical_type: None,
        }
    }

    fn test_table_def() -> TableDef {
        TableDef {
            name: "t".to_string(),
            columns: vec![column_def("k", DataType::Int64, false)],
            iceberg_row_lineage_metadata_columns: vec![],
            source: crate::compiler::mv_rewrite::test_scan_source(
                crate::planner::table::SqlScanKind::ConnectorRead,
            ),
        }
    }

    fn sql_delta_source(from_snapshot_id: i64, to_snapshot_id: i64) -> ScanSource {
        let binding = SqlTableBindingId::new(
            SqlTableBindingScopeId::new(NonZeroU64::new(1).expect("scope")),
            NonZeroU32::new(1).expect("ordinal"),
        );
        ScanSource::Sql(SqlScanSource::new(
            binding,
            SqlTableIdentity {
                catalog: "ice".to_string(),
                namespace: "db".to_string(),
                table: "orders".to_string(),
            },
            SqlScanKind::Delta {
                from_snapshot_id,
                to_snapshot_id,
            },
        ))
    }

    fn sql_snapshot_source(snapshot_id: i64) -> ScanSource {
        let binding = SqlTableBindingId::new(
            SqlTableBindingScopeId::new(NonZeroU64::new(1).expect("scope")),
            NonZeroU32::new(1).expect("ordinal"),
        );
        ScanSource::Sql(SqlScanSource::new(
            binding,
            SqlTableIdentity {
                catalog: "ice".to_string(),
                namespace: "db".to_string(),
                table: "orders".to_string(),
            },
            SqlScanKind::FrozenInputSet {
                version: crate::planner::table::SqlTableVersionSelector::Snapshot(snapshot_id),
            },
        ))
    }

    fn sql_target_locator_source() -> ScanSource {
        let binding = SqlTableBindingId::new(
            SqlTableBindingScopeId::new(NonZeroU64::new(1).expect("scope")),
            NonZeroU32::new(1).expect("ordinal"),
        );
        ScanSource::Sql(SqlScanSource::new(
            binding,
            SqlTableIdentity {
                catalog: "ice".to_string(),
                namespace: "db".to_string(),
                table: "pf_mv".to_string(),
            },
            SqlScanKind::MvTargetLocator {
                facts: SqlMvTargetLocatorScan {
                    target_table_uuid: "uuid-pf-mv".to_string(),
                    target_snapshot_id: Some(99),
                    apply_key_column: "__nova_base_row_id".to_string(),
                    branch_id_column: Some("__branch_id".to_string()),
                },
            },
        ))
    }

    fn scan_plan_with_source(table_name: &str, source: ScanSource) -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::scan(PlanScanNode {
                database: "db".to_string(),
                table: TableDef {
                    name: table_name.to_string(),
                    columns: vec![column_def("k", DataType::Int64, false)],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source,
                },
                alias: None,
                columns: vec![output_column(1, "k", DataType::Int64, false)],
                predicates: vec![],
                required_columns: None,
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        )
    }

    fn column_expr(id: u32, qualifier: Option<&str>, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: qualifier.map(str::to_string),
                column: name.to_string(),
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
        }
    }

    fn int_literal(value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(value)),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
        }
    }

    #[test]
    fn logical_explain_verbose_prints_refresh_scan_sources() {
        let delta_plan = scan_plan_with_source("orders", sql_delta_source(101, 200));

        let delta_normal = explain_plan(&delta_plan, ExplainLevel::Normal).join("\n");
        assert!(!delta_normal.contains("source:"), "{delta_normal}");
        let delta_verbose = explain_plan(&delta_plan, ExplainLevel::Verbose).join("\n");
        assert!(
            delta_verbose
                .contains("source: IcebergDeltaTable from_snapshot_id=101 to_snapshot_id=200"),
            "{delta_verbose}"
        );

        let version_plan = scan_plan_with_source("orders", sql_snapshot_source(200));
        let version_verbose = explain_plan(&version_plan, ExplainLevel::Verbose).join("\n");
        assert!(
            version_verbose.contains("source: IcebergVersionTable snapshot_id=200"),
            "{version_verbose}"
        );

        let locator_plan = scan_plan_with_source("pf_mv", sql_target_locator_source());

        let locator_normal = explain_plan(&locator_plan, ExplainLevel::Normal).join("\n");
        assert!(!locator_normal.contains("source:"), "{locator_normal}");
        let locator_verbose = explain_plan(&locator_plan, ExplainLevel::Verbose).join("\n");
        assert!(
            locator_verbose.contains(
                "source: IcebergMvTargetLocator target=ice.db.pf_mv apply_key=__nova_base_row_id branch_id=__branch_id"
            ),
            "{locator_verbose}"
        );
    }

    #[test]
    fn logical_explain_formats_apply_and_assert_one_row() {
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::apply(LogicalApplyNode {
                kind: ApplyKind::Exists { negated: true },
                subquery_expr: TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: ColumnId(5),
                        qualifier: None,
                        column: "sq".to_string(),
                    },
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        DataType::Boolean,
                        false,
                    ),
                },
                output_column: OutputColumn {
                    column_id: ColumnId(5),
                    name: "sq".to_string(),
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        DataType::Boolean,
                        false,
                    ),

                    is_internal: true,
                },
                inner_output_column_id: ColumnId(5),
                correlation_column_ids: vec![ColumnId(1)],
                correlation_conjuncts: vec![],
                residual_predicate: None,
                need_check_max_rows: false,
                use_semi_anti: true,
                uncorrelated_outer_predicate_columns: HashSet::new(),
            }),
            vec![
                empty_values_for_test(),
                LogicalPlanNode::new(
                    LogicalPlanKind::AssertOneRow(PlanAssertOneRowNode::global_at_most_one(
                        "select 1",
                    )),
                    vec![empty_values_for_test()],
                    None,
                ),
            ],
            None,
        );

        let out = explain_plan(&plan, ExplainLevel::Normal).join("\n");

        assert!(
            out.contains("APPLY (NOT EXISTS, correlated=true, use_semi_anti=true)"),
            "missing APPLY line: {out}"
        );
        assert!(
            out.contains("ASSERT ONE ROW"),
            "missing ASSERT ONE ROW line: {out}"
        );
    }

    #[test]
    fn shared_plan_node_header_formats_unified_pass_through_nodes() {
        let values = LogicalPlanKind::Values(PlanValuesNode {
            rows: vec![vec![], vec![]],
            columns: vec![],
        });
        let assert =
            LogicalPlanKind::AssertOneRow(PlanAssertOneRowNode::global_at_most_one("select 1"));

        assert_eq!(
            format_shared_plan_node_header(
                &values,
                PlanNodeExplainStage::Logical,
                &crate::compiler::SqlCompileControl::unbounded()
            )
            .unwrap(),
            Some("VALUES (2 rows)".to_string())
        );
        assert_eq!(
            format_shared_plan_node_header(
                &values,
                PlanNodeExplainStage::Distributed,
                &crate::compiler::SqlCompileControl::unbounded()
            )
            .unwrap(),
            Some("VALUES (2 rows)".to_string())
        );
        assert_eq!(
            format_shared_plan_node_header(
                &assert,
                PlanNodeExplainStage::Logical,
                &crate::compiler::SqlCompileControl::unbounded()
            )
            .unwrap(),
            Some("ASSERT ONE ROW".to_string())
        );
        assert_eq!(
            format_shared_plan_node_header(
                &assert,
                PlanNodeExplainStage::Distributed,
                &crate::compiler::SqlCompileControl::unbounded()
            )
            .unwrap(),
            Some("ASSERT NUM ROWS (<= 1)".to_string())
        );

        let keyed = LogicalPlanKind::AssertOneRow(PlanAssertOneRowNode::per_key_at_most_one(
            "DML change-stream matched row uniqueness",
            vec![
                crate::column_id::ColumnId::new_for_test(7),
                crate::column_id::ColumnId::new_for_test(8),
            ],
            vec!["_file".to_string(), "_pos".to_string()],
            "MOR UPDATE matched target row",
            None,
        ));
        assert_eq!(
            format_shared_plan_node_header(
                &keyed,
                PlanNodeExplainStage::Distributed,
                &crate::compiler::SqlCompileControl::unbounded()
            )
            .unwrap(),
            Some("ASSERT NUM ROWS (PER KEY <= 1 BY [_file, _pos])".to_string())
        );
    }

    #[test]
    fn shared_logical_formatter_path_covers_scan_filter_project_and_window() {
        let scan_columns = vec![output_column(1, "k", DataType::Int64, false)];
        let scan = LogicalPlanNode::new(
            LogicalPlanKind::scan(PlanScanNode {
                database: "test_db".to_string(),
                table: test_table_def(),
                alias: Some("t".to_string()),
                columns: scan_columns,
                predicates: vec![],
                required_columns: None,
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        );
        let predicate = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(column_expr(1, Some("t"), "k")),
                op: BinOp::Gt,
                right: Box::new(int_literal(10)),
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),
        };
        let filter = LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode { predicate }),
            vec![scan],
            None,
        );
        let project = LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: column_expr(1, Some("t"), "k"),
                    output_name: "k".to_string(),
                    output_column_id: ColumnId::new_for_test(1),
                }],
                output_qualifier: None,
            }),
            vec![filter],
            None,
        );
        let window = LogicalPlanNode::new(
            LogicalPlanKind::Window(PlanWindowNode {
                window_exprs: vec![WindowExpr {
                    name: "row_number".to_string(),
                    args: vec![],
                    distinct: false,
                    binding: crate::analysis::test_window_binding(
                        "row_number",
                        &[],
                        DataType::Int64,
                        false,
                    ),
                    function_order_by: vec![],
                    aggregate_binding: None,
                    partition_by: vec![column_expr(1, None, "k")],
                    order_by: vec![SortItem {
                        expr: column_expr(1, None, "k"),
                        asc: true,
                        nulls_first: false,
                    }],
                    window_frame: None,
                    result_type: DataType::Int64,
                    output_name: "rn".to_string(),
                    output_column_id: ColumnId::new_for_test(2),
                    ignore_nulls: false,
                }],
                output_columns: vec![
                    output_column(1, "k", DataType::Int64, false),
                    output_column(2, "rn", DataType::Int64, false),
                ],
            }),
            vec![project],
            None,
        );

        assert_eq!(
            format_shared_plan_node_header(
                &window.kind,
                PlanNodeExplainStage::Logical,
                &crate::compiler::SqlCompileControl::unbounded()
            )
            .unwrap(),
            Some("WINDOW [row_number() OVER (PARTITION BY k ORDER BY k ASC)]".to_string())
        );
        assert_eq!(
            explain_plan(&window, ExplainLevel::Normal),
            vec![
                "WINDOW [row_number() OVER (PARTITION BY k ORDER BY k ASC)]".to_string(),
                "  PROJECT [t.k AS k]".to_string(),
                "    FILTER".to_string(),
                "      predicate: t.k > 10".to_string(),
                "      0:SCAN test_db.t (alias=t)".to_string(),
            ]
        );
    }

    #[test]
    fn format_expr_prints_column_before_literal_for_equality() {
        let expr = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(10)),
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        DataType::Int64,
                        false,
                    ),
                }),
                op: BinOp::Eq,
                right: Box::new(TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: ColumnId(42),
                        qualifier: Some("r".to_string()),
                        column: "rk".to_string(),
                    },
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        DataType::Int64,
                        false,
                    ),
                }),
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),
        };

        assert_eq!(
            format_expr(&expr, &crate::compiler::SqlCompileControl::unbounded()).unwrap(),
            "r.rk = 10"
        );
    }

    #[test]
    fn format_project_item_keeps_qualified_column_alias() {
        let item = ProjectItem {
            expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: ColumnId(1),
                    qualifier: Some("a".to_string()),
                    column: "k".to_string(),
                },
                value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
            },
            output_name: "k".to_string(),
            output_column_id: ColumnId(1),
        };

        assert_eq!(
            format_project_item(&item, &crate::compiler::SqlCompileControl::unbounded()).unwrap(),
            "a.k AS k"
        );
    }

    #[test]
    fn format_project_item_keeps_real_column_alias() {
        let item = ProjectItem {
            expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: ColumnId(1),
                    qualifier: None,
                    column: "id".to_string(),
                },
                value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
            },
            output_name: "alias_id".to_string(),
            output_column_id: ColumnId(1),
        };

        assert_eq!(
            format_project_item(&item, &crate::compiler::SqlCompileControl::unbounded()).unwrap(),
            "id AS alias_id"
        );
    }

    fn render_with_bound(
        plan: &LogicalPlanNode,
        lines: usize,
        bytes: usize,
    ) -> Result<Vec<String>, String> {
        super::logical::render(
            plan,
            ExplainLevel::Verbose,
            super::completed::ExplainRenderBudget::try_new(lines, bytes)
                .expect("test render bound"),
        )
    }

    #[test]
    fn logical_borrowed_expression_bytes_preserve_literals_cases_and_aliases() {
        let control = crate::compiler::SqlCompileControl::unbounded();
        let format_expr = |expr: &TypedExpr| super::format_expr(expr, &control).unwrap();
        let format_project_item =
            |item: &ProjectItem| super::format_project_item(item, &control).unwrap();
        let typed = |kind| TypedExpr {
            kind,
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
        };
        let expression = typed(ExprKind::Case {
            operand: Some(Box::new(column_expr(1, None, "key"))),
            when_then: vec![(
                int_literal(1),
                typed(ExprKind::Literal(LiteralValue::String("a'b".into()))),
            )],
            else_expr: Some(Box::new(typed(ExprKind::Literal(LiteralValue::Binary(
                vec![0, 171, 255],
            ))))),
        });
        assert_eq!(
            format_expr(&expression),
            "CASE key WHEN 1 THEN 'a'b' ELSE X'00ABFF' END"
        );
        let lambda = typed(ExprKind::Lambda {
            params: vec!["x".into(), "y".into()],
            body: Box::new(column_expr(1, None, "x")),
        });
        assert_eq!(format_expr(&lambda), "(x, y) -> x");
        assert_eq!(
            format_expr(&typed(ExprKind::Literal(LiteralValue::Decimal(
                "1.2300".into()
            )))),
            "1.2300"
        );
        let item = |expr, name: &str| ProjectItem {
            expr,
            output_name: name.into(),
            output_column_id: ColumnId::new_for_test(1),
        };
        assert_eq!(
            format_project_item(&item(column_expr(1, None, "名称"), "名称")),
            "名称"
        );
        assert_eq!(
            format_project_item(&item(column_expr(1, Some("a"), "名称"), "名称")),
            "a.名称 AS 名称"
        );
        assert_eq!(format_project_item(&item(int_literal(10), "10")), "10");
        assert_eq!(format_project_item(&item(int_literal(10), "1")), "10 AS 1");
    }

    #[test]
    fn logical_scan_source_current_has_no_verbose_label() {
        let source = match sql_snapshot_source(1) {
            ScanSource::Sql(mut source) => {
                source.kind = SqlScanKind::FrozenInputSet {
                    version: crate::planner::table::SqlTableVersionSelector::Current,
                };
                ScanSource::Sql(source)
            }
        };
        assert_eq!(
            explain_plan(
                &scan_plan_with_source("orders", source),
                ExplainLevel::Verbose
            ),
            ["0:SCAN db.orders"],
        );
    }

    #[test]
    fn logical_output_refuses_large_names_literals_and_argument_lists() {
        let named = scan_plan_with_source(&"n".repeat(32 * 1024), sql_snapshot_source(1));
        assert!(render_with_bound(&named, 16, 128).is_err());
        for literal in [
            LiteralValue::String("s".repeat(32 * 1024)),
            LiteralValue::Binary(vec![0xab; 32 * 1024]),
            LiteralValue::Decimal("1".repeat(32 * 1024)),
        ] {
            let plan = LogicalPlanNode::new(
                LogicalPlanKind::Filter(PlanFilterNode {
                    predicate: TypedExpr {
                        kind: ExprKind::Literal(literal),
                        value_type: novarocks_type_contract::FunctionValueType::new(
                            DataType::Utf8,
                            false,
                        ),
                    },
                }),
                vec![empty_values_for_test()],
                None,
            );
            assert!(render_with_bound(&plan, 16, 128).is_err());
        }
        let many = LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode {
                predicate: TypedExpr {
                    kind: ExprKind::InList {
                        expr: Box::new(column_expr(1, None, "k")),
                        list: (0..1024).map(int_literal).collect(),
                        negated: true,
                    },
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        DataType::Boolean,
                        false,
                    ),
                },
            }),
            vec![empty_values_for_test()],
            None,
        );
        assert!(render_with_bound(&many, 16, 128).is_err());
    }

    #[test]
    fn logical_exact_line_and_byte_bounds_are_applied_during_render() {
        let plan = empty_values_for_test();
        // The independent literal oracle has no newline for one line.
        assert_eq!(
            render_with_bound(&plan, 1, 15).unwrap(),
            ["VALUES (0 rows)"]
        );
        assert!(render_with_bound(&plan, 1, 14).is_err());
        let filter = LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode {
                predicate: int_literal(1),
            }),
            vec![plan],
            None,
        );
        assert_eq!(
            render_with_bound(&filter, 3, 39).unwrap(),
            ["FILTER", "  predicate: 1", "  VALUES (0 rows)",]
        );
        assert!(render_with_bound(&filter, 2, 128).is_err());
        assert!(render_with_bound(&filter, 3, 38).is_err());
    }

    #[test]
    fn logical_depth_refusals_are_checked_before_unbounded_recursive_descent() {
        let mut plan = empty_values_for_test();
        for _ in 0..63 {
            plan = LogicalPlanNode::new(
                LogicalPlanKind::Filter(PlanFilterNode {
                    predicate: int_literal(1),
                }),
                vec![plan],
                None,
            );
        }
        assert!(
            super::explain_plan_checked(
                &plan,
                ExplainLevel::Normal,
                &crate::compiler::SqlCompileControl::unbounded()
            )
            .is_ok()
        );
        let too_deep = LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode {
                predicate: int_literal(1),
            }),
            vec![plan],
            None,
        );
        let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            super::explain_plan_checked(
                &too_deep,
                ExplainLevel::Normal,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
        }));
        assert!(
            failure
                .expect("depth refusal must not panic")
                .unwrap_err()
                .to_string()
                .contains("depth")
        );
        let mut expr = int_literal(1);
        for _ in 0..64 {
            expr = TypedExpr {
                kind: ExprKind::Nested(Box::new(expr)),
                value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
            };
        }
        let too_deep = LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode { predicate: expr }),
            vec![empty_values_for_test()],
            None,
        );
        assert!(
            super::explain_plan_checked(
                &too_deep,
                ExplainLevel::Normal,
                &crate::compiler::SqlCompileControl::unbounded()
            )
            .is_err()
        );
    }

    #[test]
    fn logical_target_state_source_keeps_keys_branch_and_partition_constraints() {
        use crate::planner::table::{
            BranchScope, SqlMvTargetStatePartitionConstraint, SqlMvTargetStateRowFilter,
            SqlMvTargetStateScan,
        };
        let ScanSource::Sql(mut source) = sql_snapshot_source(1);
        source.kind = SqlScanKind::MvTargetState {
            facts: SqlMvTargetStateScan {
                target_table_uuid: "uuid-1".into(),
                target_snapshot_id: Some(9),
                aggregate_state_layout_version: 2,
                columns: vec![],
                group_key_names: vec!["a".into(), "b".into()],
                aggregate_state_names: vec!["sum".into(), "count".into()],
                physical_column_names: vec![],
                row_id_column_name: "row_id".into(),
                row_filter: SqlMvTargetStateRowFilter::DeltaInputRowIds {
                    row_id_column_name: "row_id".into(),
                    branch_scope: Some(BranchScope {
                        branch_id_column_name: "branch".into(),
                        branch_id: 7,
                    }),
                },
                partition_constraint:
                    SqlMvTargetStatePartitionConstraint::AffectedPartitionAllowListRequired,
            },
        };
        let plan = scan_plan_with_source("mv", ScanSource::Sql(source));
        assert_eq!(
            explain_plan(&plan, ExplainLevel::Verbose),
            [
                "0:SCAN db.mv",
                "     source: IcebergMvTargetState target=ice.db.orders keys=[a,b] states=[sum,count] uuid=uuid-1 snapshot=9 layout=2 row_filter=delta_input_row_ids(row_id, branch=7) partition=affected_allow_list_required",
            ]
        );
    }
}

#[cfg(test)]
#[path = "cv_tests.rs"]
mod cv_tests;
