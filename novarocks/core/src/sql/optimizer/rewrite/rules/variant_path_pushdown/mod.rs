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

mod rule;

pub(crate) use rule::VariantPathPushdownRule;

#[cfg(test)]
mod tests {
    use crate::sql::planner::logical::*;
    use crate::sql::planner::payload::*;
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    use std::rc::Rc;

    use arrow::datatypes::DataType;

    use super::VariantPathPushdownRule;
    use crate::connector::iceberg::scan_model::{
        IcebergDataFileBinding, IcebergSchemaDef, IcebergTableInfo,
    };
    use crate::sql::analysis::{
        BinOp, ExprKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr,
    };
    use crate::sql::column_id::{ColumnId, ColumnRefFactory};
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::tree::rewrite_with_rule;
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::planner::logical::{LogicalPlanKind, LogicalPlanNode};
    use crate::sql::planner::optimizer_bridge::logical::{to_logical_plan, to_optimizer_expr};
    use crate::sql::planner::payload::{PlanFilterNode, PlanProjectNode, PlanScanNode};
    use crate::sql::planner::table::{ScanSource, TableDef};
    use novarocks_catalog::schema::ColumnDef;

    fn add_column(
        factory: &Rc<RefCell<ColumnRefFactory>>,
        name: &str,
        data_type: DataType,
        nullable: bool,
        is_internal: bool,
    ) -> OutputColumn {
        let column_id =
            factory
                .borrow_mut()
                .create(None, name.to_string(), data_type.clone(), nullable);
        OutputColumn {
            column_id,
            name: name.to_string(),
            data_type,
            nullable,
            is_internal,
        }
    }

    fn iceberg_info() -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "t".to_string(),
            table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
            current_snapshot_id: Some(1),
            schema_id: 1,
            location: "file:///tmp/t".to_string(),
            schema: IcebergSchemaDef { fields: vec![] },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    fn table_def(source: ScanSource, source_type: DataType) -> TableDef {
        TableDef {
            name: "t".to_string(),
            columns: vec![ColumnDef {
                name: "v".to_string(),
                data_type: source_type,
                nullable: true,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source,
        }
    }

    fn iceberg_source() -> ScanSource {
        ScanSource::IcebergDataFiles {
            table: iceberg_info(),
            files: vec![],
            cloud_properties: BTreeMap::new(),
            binding: IcebergDataFileBinding::CurrentSnapshot,
        }
    }

    fn starrocks_source() -> ScanSource {
        ScanSource::StarRocks {
            db_id: 1,
            table_id: 2,
        }
    }

    fn scan_with_source(
        factory: &Rc<RefCell<ColumnRefFactory>>,
        source: ScanSource,
        source_type: DataType,
    ) -> (LogicalPlanNode, OutputColumn) {
        let source_column = add_column(factory, "v", source_type.clone(), true, false);
        (
            LogicalPlanNode::new(
                LogicalPlanKind::Scan(PlanScanNode {
                    database: "db".to_string(),
                    table: table_def(source, source_type),
                    alias: None,
                    columns: vec![source_column.clone()],
                    predicates: vec![],
                    required_columns: None,
                    variant_columns: vec![],
                    mv_rewritten_from: None,
                }),
                vec![],
                None,
            ),
            source_column,
        )
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

    fn string_literal(value: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::String(value.to_string())),
            data_type: DataType::Utf8,
            nullable: false,
        }
    }

    fn int_literal(value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(value)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn bool_literal(value: bool) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Bool(value)),
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn variant_get(name: &str, source_column: &OutputColumn, path: &str, ty: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::FunctionCall {
                name: name.to_string(),
                args: vec![
                    column_ref(source_column),
                    string_literal(path),
                    string_literal(ty),
                ],
                distinct: false,
            },
            data_type: DataType::Int64,
            nullable: true,
        }
    }

    fn variant_get_with_args(name: &str, args: Vec<TypedExpr>) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::FunctionCall {
                name: name.to_string(),
                args,
                distinct: false,
            },
            data_type: DataType::Int64,
            nullable: true,
        }
    }

    fn equality_with_ten(expr: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(expr),
                op: BinOp::Eq,
                right: Box::new(int_literal(10)),
            },
            data_type: DataType::Boolean,
            nullable: true,
        }
    }

    fn rewrite(
        plan: LogicalPlanNode,
        factory: Rc<RefCell<ColumnRefFactory>>,
    ) -> (LogicalPlanNode, bool) {
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_column_ref_factory(factory);
        let mut scalars = ScalarArena::new();
        let opt_plan = to_optimizer_expr(&plan, &mut scalars);
        let arena_rc = Rc::new(RefCell::new(scalars));
        ctx.set_scalar_arena(arena_rc.clone());
        let (opt_result, changed) =
            rewrite_with_rule(opt_plan, &VariantPathPushdownRule, &mut ctx).unwrap();
        let arena = arena_rc.borrow();
        (to_logical_plan(opt_result, &arena), changed)
    }

    fn scan_from_plan(plan: &LogicalPlanNode) -> &PlanScanNode {
        match &plan.kind {
            LogicalPlanKind::Scan(scan) => scan,
            LogicalPlanKind::Filter(_) | LogicalPlanKind::Project(_) => {
                scan_from_plan(plan.unary_input())
            }
            other => panic!("expected plan with scan leaf, got {other:?}"),
        }
    }

    fn column_ref_id(expr: &TypedExpr) -> ColumnId {
        match &expr.kind {
            ExprKind::ColumnRef { column_id, .. } => *column_id,
            other => panic!("expected ColumnRef, got {other:?}"),
        }
    }

    /// Return whichever side of a binary predicate is a ColumnRef.
    /// ScalarArena normalizes commutative ops (including Eq) by ScalarId ordering,
    /// so after a round-trip through the arena the left/right positions may swap.
    fn binary_column_ref_side(expr: &TypedExpr) -> &TypedExpr {
        match &expr.kind {
            ExprKind::BinaryOp { left, right, .. } => {
                if matches!(left.kind, ExprKind::ColumnRef { .. }) {
                    left
                } else if matches!(right.kind, ExprKind::ColumnRef { .. }) {
                    right
                } else {
                    panic!("expected one side to be ColumnRef, got left={left:?}, right={right:?}")
                }
            }
            other => panic!("expected BinaryOp, got {other:?}"),
        }
    }

    #[test]
    fn filter_scan_rewrites_variant_get_predicate_to_synthetic_column() {
        let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
        let (scan, source_column) =
            scan_with_source(&factory, iceberg_source(), DataType::LargeBinary);
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode {
                predicate: equality_with_ten(variant_get(
                    "variant_get",
                    &source_column,
                    "$.a",
                    "bigint",
                )),
            }),
            vec![scan],
            None,
        );

        let (rewritten, changed) = rewrite(plan, Rc::clone(&factory));

        assert!(changed);
        let LogicalPlanKind::Filter(filter) = &rewritten.kind else {
            panic!("expected filter");
        };
        let scan = scan_from_plan(&rewritten);
        assert_eq!(scan.variant_columns.len(), 1);
        let descriptor = &scan.variant_columns[0];
        assert_eq!(descriptor.source_column_id, source_column.column_id);
        assert_eq!(descriptor.source_column, "v");
        assert_eq!(descriptor.synthetic_column, "__nr_var_v_0");
        assert_eq!(descriptor.canonical_path, "$.a");
        assert_eq!(descriptor.requested_type, DataType::Int64);
        assert!(descriptor.strict);
        assert_ne!(descriptor.synthetic_column_id, source_column.column_id);

        let synthetic_output = scan
            .columns
            .iter()
            .find(|column| column.column_id == descriptor.synthetic_column_id)
            .expect("synthetic scan output");
        assert_eq!(synthetic_output.name, descriptor.synthetic_column);
        assert_eq!(synthetic_output.data_type, DataType::Int64);
        assert!(synthetic_output.nullable);
        assert!(synthetic_output.is_internal);

        let rewritten_left = binary_column_ref_side(&filter.predicate);
        assert_eq!(
            column_ref_id(rewritten_left),
            descriptor.synthetic_column_id
        );
        assert_eq!(rewritten_left.data_type, DataType::Int64);
        assert!(rewritten_left.nullable);
    }

    #[test]
    fn project_scan_rewrites_variant_get_item_to_synthetic_column() {
        let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
        let (scan, source_column) =
            scan_with_source(&factory, iceberg_source(), DataType::LargeBinary);
        let project_output = add_column(&factory, "a", DataType::Int64, true, false).column_id;
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: variant_get("variant_get", &source_column, "$.a", "bigint"),
                    output_name: "a".to_string(),
                    output_column_id: project_output,
                }],
                output_qualifier: None,
            }),
            vec![scan],
            None,
        );

        let (rewritten, changed) = rewrite(plan, Rc::clone(&factory));

        assert!(changed);
        let LogicalPlanKind::Project(project) = &rewritten.kind else {
            panic!("expected project");
        };
        let scan = scan_from_plan(&rewritten);
        assert_eq!(scan.variant_columns.len(), 1);
        let descriptor = &scan.variant_columns[0];
        assert_eq!(descriptor.synthetic_column, "__nr_var_v_0");
        assert_eq!(
            column_ref_id(&project.items[0].expr),
            descriptor.synthetic_column_id
        );
        assert_eq!(project.items[0].output_column_id, project_output);
    }

    #[test]
    fn strict_project_above_unrelated_filter_is_not_rewritten() {
        let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
        let (scan, source_column) =
            scan_with_source(&factory, iceberg_source(), DataType::LargeBinary);
        let filter = LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode {
                predicate: bool_literal(true),
            }),
            vec![scan],
            None,
        );
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: variant_get("variant_get", &source_column, "$.a", "bigint"),
                    output_name: "a".to_string(),
                    output_column_id: add_column(&factory, "a", DataType::Int64, true, false)
                        .column_id,
                }],
                output_qualifier: None,
            }),
            vec![filter],
            None,
        );
        let before = format!("{plan:?}");

        let (rewritten, changed) = rewrite(plan, Rc::clone(&factory));

        assert!(!changed);
        assert_eq!(format!("{rewritten:?}"), before);
        assert!(scan_from_plan(&rewritten).variant_columns.is_empty());
    }

    #[test]
    fn strict_project_above_scan_with_unrelated_predicate_is_not_rewritten() {
        let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
        let (mut scan, source_column) =
            scan_with_source(&factory, iceberg_source(), DataType::LargeBinary);
        let LogicalPlanKind::Scan(scan_node) = &mut scan.kind else {
            panic!("expected scan");
        };
        scan_node.predicates.push(bool_literal(true));
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: variant_get("variant_get", &source_column, "$.a", "bigint"),
                    output_name: "a".to_string(),
                    output_column_id: add_column(&factory, "a", DataType::Int64, true, false)
                        .column_id,
                }],
                output_qualifier: None,
            }),
            vec![scan],
            None,
        );
        let before = format!("{plan:?}");

        let (rewritten, changed) = rewrite(plan, Rc::clone(&factory));

        assert!(!changed);
        assert_eq!(format!("{rewritten:?}"), before);
        assert!(scan_from_plan(&rewritten).variant_columns.is_empty());
    }

    #[test]
    fn predicate_and_projection_deduplicate_identical_variant_requests() {
        let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
        let (scan, source_column) =
            scan_with_source(&factory, iceberg_source(), DataType::LargeBinary);
        let filter = LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode {
                predicate: equality_with_ten(variant_get(
                    "variant_get",
                    &source_column,
                    "$.a",
                    "bigint",
                )),
            }),
            vec![scan],
            None,
        );
        let project_output = add_column(&factory, "a", DataType::Int64, true, false).column_id;
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: variant_get("variant_get", &source_column, "$.a", "bigint"),
                    output_name: "a".to_string(),
                    output_column_id: project_output,
                }],
                output_qualifier: None,
            }),
            vec![filter],
            None,
        );

        let (rewritten, changed) = rewrite(plan, Rc::clone(&factory));

        assert!(changed);
        let LogicalPlanKind::Project(project) = &rewritten.kind else {
            panic!("expected project");
        };
        let LogicalPlanKind::Filter(filter) = &rewritten.unary_input().kind else {
            panic!("expected filter child");
        };
        let scan = scan_from_plan(&rewritten);
        assert_eq!(scan.variant_columns.len(), 1);
        let synthetic_id = scan.variant_columns[0].synthetic_column_id;
        assert_eq!(column_ref_id(&project.items[0].expr), synthetic_id);
        assert_eq!(
            column_ref_id(binary_column_ref_side(&filter.predicate)),
            synthetic_id
        );
    }

    #[test]
    fn rewrite_is_idempotent_after_variant_slot_is_allocated() {
        let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
        let (scan, source_column) =
            scan_with_source(&factory, iceberg_source(), DataType::LargeBinary);
        let filter = LogicalPlanNode::new(
            LogicalPlanKind::Filter(PlanFilterNode {
                predicate: equality_with_ten(variant_get(
                    "variant_get",
                    &source_column,
                    "$.a",
                    "bigint",
                )),
            }),
            vec![scan],
            None,
        );
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: variant_get("variant_get", &source_column, "$.a", "bigint"),
                    output_name: "a".to_string(),
                    output_column_id: add_column(&factory, "a", DataType::Int64, true, false)
                        .column_id,
                }],
                output_qualifier: None,
            }),
            vec![filter],
            None,
        );

        let (rewritten_once, changed_once) = rewrite(plan, Rc::clone(&factory));
        assert!(changed_once);
        let scan_once = scan_from_plan(&rewritten_once);
        assert_eq!(scan_once.variant_columns.len(), 1);
        let synthetic_id = scan_once.variant_columns[0].synthetic_column_id;
        let before_second = format!("{rewritten_once:?}");

        let (rewritten_twice, changed_twice) = rewrite(rewritten_once, Rc::clone(&factory));

        assert!(!changed_twice);
        assert_eq!(format!("{rewritten_twice:?}"), before_second);
        let scan_twice = scan_from_plan(&rewritten_twice);
        assert_eq!(scan_twice.variant_columns.len(), 1);
        assert_eq!(
            scan_twice.variant_columns[0].synthetic_column_id,
            synthetic_id
        );
    }

    #[test]
    fn try_variant_get_records_non_strict_descriptor() {
        let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
        let (scan, source_column) =
            scan_with_source(&factory, iceberg_source(), DataType::LargeBinary);
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: variant_get("try_variant_get", &source_column, "$.a", "bigint"),
                    output_name: "a".to_string(),
                    output_column_id: add_column(&factory, "a", DataType::Int64, true, false)
                        .column_id,
                }],
                output_qualifier: None,
            }),
            vec![scan],
            None,
        );

        let (rewritten, changed) = rewrite(plan, Rc::clone(&factory));

        assert!(changed);
        let scan = scan_from_plan(&rewritten);
        assert_eq!(scan.variant_columns.len(), 1);
        assert!(!scan.variant_columns[0].strict);
    }

    #[test]
    fn already_pushed_scan_predicate_is_rewritten() {
        let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
        let (mut scan, source_column) =
            scan_with_source(&factory, iceberg_source(), DataType::LargeBinary);
        let LogicalPlanKind::Scan(scan_node) = &mut scan.kind else {
            panic!("expected scan");
        };
        scan_node.predicates.push(equality_with_ten(variant_get(
            "variant_get",
            &source_column,
            "$.a",
            "bigint",
        )));

        let (rewritten, changed) = rewrite(scan, Rc::clone(&factory));

        assert!(changed);
        let LogicalPlanKind::Scan(scan) = &rewritten.kind else {
            panic!("expected scan");
        };
        assert_eq!(scan.variant_columns.len(), 1);
        assert_eq!(
            column_ref_id(binary_column_ref_side(&scan.predicates[0])),
            scan.variant_columns[0].synthetic_column_id
        );
    }

    #[test]
    fn unset_source_column_id_is_not_rewritten() {
        // ColumnId::UNSET cannot be interned via intern_typed (it panics), so this
        // test builds the OptExpr tree directly using arena.intern() to bypass that
        // guard, then verifies the rule returns Unchanged for UNSET source columns.
        use crate::sql::optimizer::operator::{Operator, ProjectOp, ScalarProjectItem, ScanOp};
        use crate::sql::optimizer::opt_expr::OptExpr;
        use crate::sql::optimizer::scalar::{ScalarArena, ScalarNode};

        let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
        let output_column_id = add_column(&factory, "a", DataType::Int64, true, false).column_id;
        let source_column = OutputColumn {
            column_id: ColumnId::UNSET,
            name: "v".to_string(),
            data_type: DataType::LargeBinary,
            nullable: true,
            is_internal: false,
        };

        let mut scalars = ScalarArena::new();

        // Build the variant_get(UNSET_col, "$.a", "bigint") call directly.
        // arena.intern() has no UNSET guard — only intern_typed does.
        let unset_col_id = scalars.intern(
            ScalarNode::ColumnRef(ColumnId::UNSET),
            DataType::LargeBinary,
            true,
        );
        let path_id = scalars.intern(
            ScalarNode::Literal(crate::sql::optimizer::scalar::HashableLiteral(
                crate::sql::analysis::LiteralValue::String("$.a".to_string()),
            )),
            DataType::Utf8,
            false,
        );
        let ty_id = scalars.intern(
            ScalarNode::Literal(crate::sql::optimizer::scalar::HashableLiteral(
                crate::sql::analysis::LiteralValue::String("bigint".to_string()),
            )),
            DataType::Utf8,
            false,
        );
        let call_id = scalars.intern(
            ScalarNode::FunctionCall {
                name: "variant_get".to_string(),
                args: vec![unset_col_id, path_id, ty_id],
                distinct: false,
            },
            DataType::Int64,
            true,
        );

        let scan_op = OptExpr::leaf(Operator::LogicalScan(ScanOp {
            database: "db".to_string(),
            table: table_def(iceberg_source(), DataType::LargeBinary),
            alias: None,
            stats_ref: None,
            columns: vec![source_column],
            predicates: vec![],
            required_columns: None,
            variant_columns: vec![],
            mv_rewritten_from: None,
        }));
        let project_op = OptExpr::new(
            Operator::LogicalProject(ProjectOp {
                items: vec![ScalarProjectItem {
                    expr: call_id,
                    output_name: "a".to_string(),
                    output_column_id,
                    expr_display: None,
                }],
                output_qualifier: None,
            }),
            vec![scan_op],
        );

        let arena_rc = Rc::new(RefCell::new(scalars));
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_column_ref_factory(Rc::clone(&factory));
        ctx.set_scalar_arena(arena_rc);
        let (_, changed) =
            rewrite_with_rule(project_op, &VariantPathPushdownRule, &mut ctx).unwrap();

        assert!(!changed);
    }

    #[test]
    fn root_variant_path_is_not_rewritten() {
        let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
        let (scan, source_column) =
            scan_with_source(&factory, iceberg_source(), DataType::LargeBinary);
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: variant_get("variant_get", &source_column, "$", "bigint"),
                    output_name: "root_path".to_string(),
                    output_column_id: add_column(
                        &factory,
                        "root_path",
                        DataType::Int64,
                        true,
                        false,
                    )
                    .column_id,
                }],
                output_qualifier: None,
            }),
            vec![scan],
            None,
        );
        let before = format!("{plan:?}");

        let (rewritten, changed) = rewrite(plan, Rc::clone(&factory));

        assert!(!changed);
        assert_eq!(format!("{rewritten:?}"), before);
    }

    #[test]
    fn empty_variant_path_is_not_rewritten() {
        let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
        let (scan, source_column) =
            scan_with_source(&factory, iceberg_source(), DataType::LargeBinary);
        let plan = LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: variant_get("variant_get", &source_column, "", "bigint"),
                    output_name: "empty_path".to_string(),
                    output_column_id: add_column(
                        &factory,
                        "empty_path",
                        DataType::Int64,
                        true,
                        false,
                    )
                    .column_id,
                }],
                output_qualifier: None,
            }),
            vec![scan],
            None,
        );
        let before = format!("{plan:?}");

        let (rewritten, changed) = rewrite(plan, Rc::clone(&factory));

        assert!(!changed);
        assert_eq!(format!("{rewritten:?}"), before);
    }

    #[test]
    fn unsupported_variant_requests_stay_unchanged() {
        for name in [
            "non_column_source",
            "non_literal_path",
            "non_literal_type",
            "two_arg_variant_get",
            "array_index_path",
            "unsupported_requested_type",
            "non_iceberg_scan",
            "source_not_variant_binary",
        ] {
            let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
            let source = if name == "non_iceberg_scan" {
                starrocks_source()
            } else {
                iceberg_source()
            };
            let source_type = if name == "source_not_variant_binary" {
                DataType::Utf8
            } else {
                DataType::LargeBinary
            };
            let (scan, _) = scan_with_source(&factory, source, source_type);
            let source_column = scan_from_plan(&scan).columns[0].clone();
            let expr = match name {
                "non_column_source" => variant_get_with_args(
                    "variant_get",
                    vec![
                        string_literal("not a column"),
                        string_literal("$.a"),
                        string_literal("bigint"),
                    ],
                ),
                "non_literal_path" => variant_get_with_args(
                    "variant_get",
                    vec![
                        column_ref(&source_column),
                        column_ref(&add_column(&factory, "path", DataType::Utf8, false, false)),
                        string_literal("bigint"),
                    ],
                ),
                "non_literal_type" => variant_get_with_args(
                    "variant_get",
                    vec![
                        column_ref(&source_column),
                        string_literal("$.a"),
                        column_ref(&add_column(&factory, "ty", DataType::Utf8, false, false)),
                    ],
                ),
                "two_arg_variant_get" => variant_get_with_args(
                    "variant_get",
                    vec![column_ref(&source_column), string_literal("$.a")],
                ),
                "array_index_path" => {
                    variant_get("variant_get", &source_column, "$.a[0]", "bigint")
                }
                "unsupported_requested_type" => {
                    variant_get("variant_get", &source_column, "$.a", "int")
                }
                "non_iceberg_scan" | "source_not_variant_binary" => {
                    variant_get("variant_get", &source_column, "$.a", "bigint")
                }
                _ => unreachable!(),
            };
            let plan = LogicalPlanNode::new(
                LogicalPlanKind::Project(PlanProjectNode {
                    items: vec![ProjectItem {
                        expr,
                        output_name: name.to_string(),
                        output_column_id: add_column(&factory, name, DataType::Int64, true, false)
                            .column_id,
                    }],
                    output_qualifier: None,
                }),
                vec![scan],
                None,
            );
            let before = format!("{plan:?}");

            let (rewritten, changed) = rewrite(plan, Rc::clone(&factory));

            assert!(!changed, "{name}");
            assert_eq!(format!("{rewritten:?}"), before, "{name}");
        }
    }
}
