mod rule;

pub(crate) use rule::VariantPathPushdownRule;

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    use std::rc::Rc;

    use arrow::datatypes::DataType;

    use super::VariantPathPushdownRule;
    use crate::sql::analysis::{
        BinOp, ExprKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr,
    };
    use crate::sql::catalog::{
        ColumnDef, IcebergDataFileBinding, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef,
    };
    use crate::sql::column_id::{ColumnId, ColumnRefFactory};
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::tree::rewrite_with_rule;
    use crate::sql::planner::plan::{FilterNode, LogicalPlan, ProjectNode, ScanNode};

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
    ) -> (LogicalPlan, OutputColumn) {
        let source_column = add_column(factory, "v", source_type.clone(), true, false);
        (
            LogicalPlan::Scan(ScanNode {
                database: "db".to_string(),
                table: table_def(source, source_type),
                alias: None,
                columns: vec![source_column.clone()],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                required_output_columns: None,
            }),
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

    fn rewrite(plan: LogicalPlan, factory: Rc<RefCell<ColumnRefFactory>>) -> (LogicalPlan, bool) {
        let mut ctx = RewriteContext::for_query(Vec::<String>::new());
        ctx.set_column_ref_factory(factory);
        rewrite_with_rule(plan, &VariantPathPushdownRule, &mut ctx).unwrap()
    }

    fn scan_from_plan(plan: &LogicalPlan) -> &ScanNode {
        match plan {
            LogicalPlan::Scan(scan) => scan,
            LogicalPlan::Filter(filter) => scan_from_plan(&filter.input),
            LogicalPlan::Project(project) => scan_from_plan(&project.input),
            other => panic!("expected plan with scan leaf, got {other:?}"),
        }
    }

    fn column_ref_id(expr: &TypedExpr) -> ColumnId {
        match &expr.kind {
            ExprKind::ColumnRef { column_id, .. } => *column_id,
            other => panic!("expected ColumnRef, got {other:?}"),
        }
    }

    fn binary_left(expr: &TypedExpr) -> &TypedExpr {
        match &expr.kind {
            ExprKind::BinaryOp { left, .. } => left,
            other => panic!("expected BinaryOp, got {other:?}"),
        }
    }

    #[test]
    fn filter_scan_rewrites_variant_get_predicate_to_synthetic_column() {
        let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
        let (scan, source_column) =
            scan_with_source(&factory, iceberg_source(), DataType::LargeBinary);
        let plan = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: equality_with_ten(variant_get(
                "variant_get",
                &source_column,
                "$.a",
                "bigint",
            )),
            required_output_columns: None,
        });

        let (rewritten, changed) = rewrite(plan, Rc::clone(&factory));

        assert!(changed);
        let LogicalPlan::Filter(filter) = &rewritten else {
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

        let rewritten_left = binary_left(&filter.predicate);
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
        let plan = LogicalPlan::Project(ProjectNode {
            input: Box::new(scan),
            items: vec![ProjectItem {
                expr: variant_get("variant_get", &source_column, "$.a", "bigint"),
                output_name: "a".to_string(),
                output_column_id: project_output,
            }],
            output_qualifier: None,
            required_output_columns: None,
        });

        let (rewritten, changed) = rewrite(plan, Rc::clone(&factory));

        assert!(changed);
        let LogicalPlan::Project(project) = &rewritten else {
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
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: bool_literal(true),
            required_output_columns: None,
        });
        let plan = LogicalPlan::Project(ProjectNode {
            input: Box::new(filter),
            items: vec![ProjectItem {
                expr: variant_get("variant_get", &source_column, "$.a", "bigint"),
                output_name: "a".to_string(),
                output_column_id: add_column(&factory, "a", DataType::Int64, true, false).column_id,
            }],
            output_qualifier: None,
            required_output_columns: None,
        });
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
        let LogicalPlan::Scan(scan_node) = &mut scan else {
            panic!("expected scan");
        };
        scan_node.predicates.push(bool_literal(true));
        let plan = LogicalPlan::Project(ProjectNode {
            input: Box::new(scan),
            items: vec![ProjectItem {
                expr: variant_get("variant_get", &source_column, "$.a", "bigint"),
                output_name: "a".to_string(),
                output_column_id: add_column(&factory, "a", DataType::Int64, true, false).column_id,
            }],
            output_qualifier: None,
            required_output_columns: None,
        });
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
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: equality_with_ten(variant_get(
                "variant_get",
                &source_column,
                "$.a",
                "bigint",
            )),
            required_output_columns: None,
        });
        let project_output = add_column(&factory, "a", DataType::Int64, true, false).column_id;
        let plan = LogicalPlan::Project(ProjectNode {
            input: Box::new(filter),
            items: vec![ProjectItem {
                expr: variant_get("variant_get", &source_column, "$.a", "bigint"),
                output_name: "a".to_string(),
                output_column_id: project_output,
            }],
            output_qualifier: None,
            required_output_columns: None,
        });

        let (rewritten, changed) = rewrite(plan, Rc::clone(&factory));

        assert!(changed);
        let LogicalPlan::Project(project) = &rewritten else {
            panic!("expected project");
        };
        let LogicalPlan::Filter(filter) = project.input.as_ref() else {
            panic!("expected filter child");
        };
        let scan = scan_from_plan(&rewritten);
        assert_eq!(scan.variant_columns.len(), 1);
        let synthetic_id = scan.variant_columns[0].synthetic_column_id;
        assert_eq!(column_ref_id(&project.items[0].expr), synthetic_id);
        assert_eq!(column_ref_id(binary_left(&filter.predicate)), synthetic_id);
    }

    #[test]
    fn rewrite_is_idempotent_after_variant_slot_is_allocated() {
        let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
        let (scan, source_column) =
            scan_with_source(&factory, iceberg_source(), DataType::LargeBinary);
        let filter = LogicalPlan::Filter(FilterNode {
            input: Box::new(scan),
            predicate: equality_with_ten(variant_get(
                "variant_get",
                &source_column,
                "$.a",
                "bigint",
            )),
            required_output_columns: None,
        });
        let plan = LogicalPlan::Project(ProjectNode {
            input: Box::new(filter),
            items: vec![ProjectItem {
                expr: variant_get("variant_get", &source_column, "$.a", "bigint"),
                output_name: "a".to_string(),
                output_column_id: add_column(&factory, "a", DataType::Int64, true, false).column_id,
            }],
            output_qualifier: None,
            required_output_columns: None,
        });

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
        let plan = LogicalPlan::Project(ProjectNode {
            input: Box::new(scan),
            items: vec![ProjectItem {
                expr: variant_get("try_variant_get", &source_column, "$.a", "bigint"),
                output_name: "a".to_string(),
                output_column_id: add_column(&factory, "a", DataType::Int64, true, false).column_id,
            }],
            output_qualifier: None,
            required_output_columns: None,
        });

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
        let LogicalPlan::Scan(scan_node) = &mut scan else {
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
        let LogicalPlan::Scan(scan) = &rewritten else {
            panic!("expected scan");
        };
        assert_eq!(scan.variant_columns.len(), 1);
        assert_eq!(
            column_ref_id(binary_left(&scan.predicates[0])),
            scan.variant_columns[0].synthetic_column_id
        );
    }

    #[test]
    fn unset_source_column_id_is_not_rewritten() {
        let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
        let source_column = OutputColumn {
            column_id: ColumnId::UNSET,
            name: "v".to_string(),
            data_type: DataType::LargeBinary,
            nullable: true,
            is_internal: false,
        };
        let scan = LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: table_def(iceberg_source(), DataType::LargeBinary),
            alias: None,
            columns: vec![source_column.clone()],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            required_output_columns: None,
        });
        let plan = LogicalPlan::Project(ProjectNode {
            input: Box::new(scan),
            items: vec![ProjectItem {
                expr: variant_get("variant_get", &source_column, "$.a", "bigint"),
                output_name: "a".to_string(),
                output_column_id: add_column(&factory, "a", DataType::Int64, true, false).column_id,
            }],
            output_qualifier: None,
            required_output_columns: None,
        });
        let before = format!("{plan:?}");

        let (rewritten, changed) = rewrite(plan, Rc::clone(&factory));

        assert!(!changed);
        assert_eq!(format!("{rewritten:?}"), before);
    }

    #[test]
    fn root_variant_path_is_not_rewritten() {
        let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
        let (scan, source_column) =
            scan_with_source(&factory, iceberg_source(), DataType::LargeBinary);
        let plan = LogicalPlan::Project(ProjectNode {
            input: Box::new(scan),
            items: vec![ProjectItem {
                expr: variant_get("variant_get", &source_column, "$", "bigint"),
                output_name: "root_path".to_string(),
                output_column_id: add_column(&factory, "root_path", DataType::Int64, true, false)
                    .column_id,
            }],
            output_qualifier: None,
            required_output_columns: None,
        });
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
        let plan = LogicalPlan::Project(ProjectNode {
            input: Box::new(scan),
            items: vec![ProjectItem {
                expr: variant_get("variant_get", &source_column, "", "bigint"),
                output_name: "empty_path".to_string(),
                output_column_id: add_column(&factory, "empty_path", DataType::Int64, true, false)
                    .column_id,
            }],
            output_qualifier: None,
            required_output_columns: None,
        });
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
            let plan = LogicalPlan::Project(ProjectNode {
                input: Box::new(scan),
                items: vec![ProjectItem {
                    expr,
                    output_name: name.to_string(),
                    output_column_id: add_column(&factory, name, DataType::Int64, true, false)
                        .column_id,
                }],
                output_qualifier: None,
                required_output_columns: None,
            });
            let before = format!("{plan:?}");

            let (rewritten, changed) = rewrite(plan, Rc::clone(&factory));

            assert!(!changed, "{name}");
            assert_eq!(format!("{rewritten:?}"), before, "{name}");
        }
    }
}
