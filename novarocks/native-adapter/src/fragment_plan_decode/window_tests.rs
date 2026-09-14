#[expect(
    clippy::items_after_test_module,
    reason = "Focused decode tests remain adjacent to their helpers."
)]
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use super::super::{NativePlanDecodeContext, decode_node};
    use novarocks_execution::exec::expr::ExprArena;
    use novarocks_execution::exec::node::ExecNodeKind;
    use novarocks_execution::exec::node::analytic::WindowFunctionKind;
    use novarocks_plan_codec::encode_native_type as encode_type;
    use novarocks_proto_models::{common, expr, plan};
    use novarocks_types::SlotId;

    fn type_desc(data_type: &DataType) -> common::TypeDesc {
        encode_type(data_type).expect("encode type")
    }

    fn output_column(column_id: u32, name: &str, data_type: DataType) -> common::OutputColumn {
        common::OutputColumn {
            column_id,
            name: name.to_string(),
            r#type: Some(type_desc(&data_type)),
            nullable: true,
            is_internal: false,
        }
    }

    fn int_literal(value: i64) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&DataType::Int64)),
            nullable: false,
            kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::IntValue(value)),
                }),
            })),
        }
    }

    fn string_literal(value: &str) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&DataType::Utf8)),
            nullable: false,
            kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::StringValue(value.to_string())),
                }),
            })),
        }
    }

    fn function_catalog() -> Arc<novarocks_functions::EngineFunctionCatalog> {
        Arc::new(
            novarocks_sql::compiler::build_builtin_engine_function_catalog()
                .expect("builtin function catalog"),
        )
    }

    fn aggregate_binding(
        name: &str,
        argument_types: &[DataType],
    ) -> Option<plan::ResolvedAggregateSignature> {
        let selected = function_catalog()
            .resolve_aggregate_trusted(name, argument_types)
            .expect("resolved aggregate binding");
        Some(plan::ResolvedAggregateSignature {
            overload_identity: selected.overload.as_str().to_string(),
            argument_types: selected.argument_types.iter().map(type_desc).collect(),
            intermediate_type: Some(type_desc(&selected.intermediate_type)),
            output_type: Some(type_desc(&selected.output_type)),
            state_format_identity: selected.state_format.as_str().to_string(),
        })
    }

    fn aggregate_update_binding(
        name: &str,
        logical_argument_types: &[DataType],
        update_argument_types: &[DataType],
    ) -> Option<plan::ResolvedAggregateSignature> {
        let selected = function_catalog()
            .resolve_aggregate_update_trusted(name, logical_argument_types, update_argument_types)
            .expect("resolved aggregate update binding");
        Some(plan::ResolvedAggregateSignature {
            overload_identity: selected.overload.as_str().to_string(),
            argument_types: selected.argument_types.iter().map(type_desc).collect(),
            intermediate_type: Some(type_desc(&selected.intermediate_type)),
            output_type: Some(type_desc(&selected.output_type)),
            state_format_identity: selected.state_format.as_str().to_string(),
        })
    }

    fn column_ref(column_id: u32, data_type: DataType) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&data_type)),
            nullable: true,
            kind: Some(expr::expr::Kind::ColumnRef(expr::ColumnRef {
                column_id,
                qualifier: None,
                column: None,
            })),
        }
    }

    fn sort_item(column_id: u32) -> expr::SortItem {
        expr::SortItem {
            expr: Some(column_ref(column_id, DataType::Int64)),
            asc: true,
            nulls_first: false,
        }
    }

    fn physical_node(
        node_id: i32,
        kind: plan::plan_node::Kind,
        output_columns: Vec<common::OutputColumn>,
        children: Vec<plan::DistributedNode>,
    ) -> plan::DistributedNode {
        plan::DistributedNode {
            node_id,
            fragment_id: 1,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children,
            payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                output_columns,
                kind: Some(kind),
            })),
        }
    }

    fn one_col_values_node(node_id: i32) -> plan::DistributedNode {
        let columns = vec![output_column(1, "id", DataType::Int64)];
        physical_node(
            node_id,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![plan::ExprList {
                    values: vec![int_literal(10)],
                }],
                columns: columns.clone(),
            }),
            columns,
            Vec::new(),
        )
    }

    fn value_key_values_node(node_id: i32) -> plan::DistributedNode {
        let columns = vec![
            output_column(1, "value", DataType::Utf8),
            output_column(2, "key", DataType::Int64),
        ];
        physical_node(
            node_id,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![plan::ExprList {
                    values: vec![string_literal("v"), int_literal(1)],
                }],
                columns: columns.clone(),
            }),
            columns,
            Vec::new(),
        )
    }

    fn lower(node: &plan::DistributedNode) -> super::super::DecodedNode {
        let mut arena = ExprArena::default();
        decode_node(node, &mut arena, &NativePlanDecodeContext::default()).expect("lower node")
    }

    fn lower_aggregate_window(
        node: &plan::DistributedNode,
    ) -> Result<(super::super::DecodedNode, ExprArena), String> {
        let mut arena = ExprArena::default();
        decode_node(
            node,
            &mut arena,
            &NativePlanDecodeContext::default().with_function_catalog(function_catalog()),
        )
        .map(|decoded| (decoded, arena))
        .map_err(|error| error.to_string())
    }

    fn aggregate_window_node(
        name: &str,
        args: Vec<expr::Expr>,
        result_type: DataType,
        binding: Option<plan::ResolvedAggregateSignature>,
    ) -> plan::DistributedNode {
        let output_columns = vec![
            output_column(1, "value", DataType::Utf8),
            output_column(2, "key", DataType::Int64),
            output_column(3, "window_value", result_type.clone()),
        ];
        physical_node(
            90,
            plan::plan_node::Kind::Window(plan::WindowNode {
                window_exprs: vec![plan::WindowExpr {
                    name: name.to_string(),
                    args,
                    distinct: false,
                    function_order_by: Vec::new(),
                    aggregate_binding: binding,
                    partition_by: Vec::new(),
                    order_by: Vec::new(),
                    window_frame: None,
                    result_type: Some(type_desc(&result_type)),
                    output_name: "window_value".to_string(),
                    output_column_id: 3,
                    ignore_nulls: false,
                }],
                output_columns: output_columns.clone(),
            }),
            output_columns,
            vec![value_key_values_node(89)],
        )
    }

    #[test]
    fn lowers_window_node_to_analytic_exec_node() {
        let output_columns = vec![
            output_column(1, "id", DataType::Int64),
            output_column(2, "rn", DataType::Int64),
        ];
        let window = physical_node(
            80,
            plan::plan_node::Kind::Window(plan::WindowNode {
                window_exprs: vec![plan::WindowExpr {
                    name: "row_number".to_string(),
                    args: Vec::new(),
                    distinct: false,
                    function_order_by: Vec::new(),
                    aggregate_binding: None,
                    partition_by: Vec::new(),
                    order_by: vec![sort_item(1)],
                    window_frame: Some(expr::WindowFrame {
                        frame_type: expr::WindowFrameType::Rows as i32,
                        start: Some(expr::WindowBound {
                            bound: Some(expr::window_bound::Bound::UnboundedPreceding(true)),
                        }),
                        end: Some(expr::WindowBound {
                            bound: Some(expr::window_bound::Bound::CurrentRow(true)),
                        }),
                    }),
                    result_type: Some(type_desc(&DataType::Int64)),
                    output_name: "rn".to_string(),
                    output_column_id: 2,
                    ignore_nulls: false,
                }],
                output_columns: output_columns.clone(),
            }),
            output_columns,
            vec![one_col_values_node(10)],
        );

        let lowered = lower(&window);
        let ExecNodeKind::Analytic(analytic) = lowered.node.kind else {
            panic!("expected Analytic");
        };
        assert_eq!(analytic.node_id, 80);
        assert_eq!(analytic.functions.len(), 1);
        assert!(matches!(
            analytic.functions[0].kind,
            novarocks_execution::exec::node::analytic::WindowFunctionKind::RowNumber
        ));
        assert_eq!(analytic.order_by_exprs.len(), 1);
        assert_eq!(
            analytic.output_chunk_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(2)]
        );
        assert_eq!(lowered.layout.order(), &[SlotId::new(1), SlotId::new(2)]);
    }

    #[test]
    fn lowers_window_node_with_multiple_specs_as_analytic_chain() {
        let output_columns = vec![
            output_column(1, "id", DataType::Int64),
            output_column(2, "rn", DataType::Int64),
            output_column(3, "rnk", DataType::Int64),
        ];
        let mut descending_id = sort_item(1);
        descending_id.asc = false;
        let window = physical_node(
            81,
            plan::plan_node::Kind::Window(plan::WindowNode {
                window_exprs: vec![
                    plan::WindowExpr {
                        name: "row_number".to_string(),
                        args: Vec::new(),
                        distinct: false,
                        function_order_by: Vec::new(),
                        aggregate_binding: None,
                        partition_by: Vec::new(),
                        order_by: vec![sort_item(1)],
                        window_frame: Some(expr::WindowFrame {
                            frame_type: expr::WindowFrameType::Rows as i32,
                            start: Some(expr::WindowBound {
                                bound: Some(expr::window_bound::Bound::UnboundedPreceding(true)),
                            }),
                            end: Some(expr::WindowBound {
                                bound: Some(expr::window_bound::Bound::CurrentRow(true)),
                            }),
                        }),
                        result_type: Some(type_desc(&DataType::Int64)),
                        output_name: "rn".to_string(),
                        output_column_id: 2,
                        ignore_nulls: false,
                    },
                    plan::WindowExpr {
                        name: "rank".to_string(),
                        args: Vec::new(),
                        distinct: false,
                        function_order_by: Vec::new(),
                        aggregate_binding: None,
                        partition_by: Vec::new(),
                        order_by: vec![descending_id],
                        window_frame: Some(expr::WindowFrame {
                            frame_type: expr::WindowFrameType::Rows as i32,
                            start: Some(expr::WindowBound {
                                bound: Some(expr::window_bound::Bound::UnboundedPreceding(true)),
                            }),
                            end: Some(expr::WindowBound {
                                bound: Some(expr::window_bound::Bound::CurrentRow(true)),
                            }),
                        }),
                        result_type: Some(type_desc(&DataType::Int64)),
                        output_name: "rnk".to_string(),
                        output_column_id: 3,
                        ignore_nulls: false,
                    },
                ],
                output_columns: output_columns.clone(),
            }),
            output_columns,
            vec![one_col_values_node(10)],
        );

        let lowered = lower(&window);
        let ExecNodeKind::Analytic(second) = lowered.node.kind else {
            panic!("expected final Analytic");
        };
        assert_eq!(second.node_id, 83);
        assert_eq!(second.functions.len(), 1);
        assert!(matches!(
            second.functions[0].kind,
            novarocks_execution::exec::node::analytic::WindowFunctionKind::Rank
        ));
        assert_eq!(
            second.output_chunk_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(2), SlotId::new(3)]
        );

        let ExecNodeKind::Sort(sort) = second.input.kind else {
            panic!("expected Sort under final Analytic");
        };
        assert_eq!(sort.node_id, 82);
        assert_eq!(sort.order_by.len(), 1);
        assert!(!sort.order_by[0].asc);

        let ExecNodeKind::Analytic(first) = sort.input.kind else {
            panic!("expected first Analytic under Sort");
        };
        assert_eq!(first.node_id, 81);
        assert_eq!(first.functions.len(), 1);
        assert!(matches!(
            first.functions[0].kind,
            novarocks_execution::exec::node::analytic::WindowFunctionKind::RowNumber
        ));
        assert_eq!(
            first.output_chunk_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(2)]
        );
        assert_eq!(
            lowered.layout.order(),
            &[SlotId::new(1), SlotId::new(2), SlotId::new(3)]
        );
    }

    #[test]
    fn aggregate_window_requires_exact_binding_and_window_only_rejects_one() {
        let missing = aggregate_window_node(
            "max_by",
            vec![
                column_ref(1, DataType::Utf8),
                column_ref(2, DataType::Int64),
            ],
            DataType::Utf8,
            None,
        );
        let error = lower_aggregate_window(&missing).expect_err("binding must be mandatory");
        assert!(
            error.contains("exact resolved signature missing"),
            "{error}"
        );

        let extra = aggregate_window_node(
            "row_number",
            Vec::new(),
            DataType::Int64,
            aggregate_binding("count", &[]),
        );
        let error = lower_aggregate_window(&extra)
            .expect_err("window-only function must reject aggregate binding");
        assert!(
            error.contains("must not carry an aggregate binding"),
            "{error}"
        );
    }

    #[test]
    fn aggregate_window_rejects_expression_signature_drift_and_unregistered_v2() {
        let drift = aggregate_window_node(
            "sum",
            vec![column_ref(2, DataType::Int64)],
            DataType::Int64,
            aggregate_binding("sum", &[DataType::Int32]),
        );
        let error = lower_aggregate_window(&drift).expect_err("logical arg drift must fail");
        assert!(error.contains("argument type drift"), "{error}");

        let mut forged_binding = aggregate_binding("sum", &[DataType::Int64]);
        forged_binding
            .as_mut()
            .expect("sum binding")
            .state_format_identity = "forged/window-state/v99".to_string();
        let forged = aggregate_window_node(
            "sum",
            vec![column_ref(2, DataType::Int64)],
            DataType::Int64,
            forged_binding,
        );
        let error = lower_aggregate_window(&forged).expect_err("catalog drift must fail");
        assert!(error.contains("resolved signature drift"), "{error}");

        let v2 = aggregate_window_node(
            "max_by_v2",
            vec![
                column_ref(1, DataType::Utf8),
                column_ref(2, DataType::Int64),
            ],
            DataType::Utf8,
            None,
        );
        let error = lower_aggregate_window(&v2).expect_err("v2 is not registered");
        assert!(
            error.contains("unsupported window function: max_by_v2"),
            "{error}"
        );
    }

    #[test]
    fn max_min_by_keep_logical_two_arg_binding_and_packed_struct_input() {
        for name in ["max_by", "min_by"] {
            let node = aggregate_window_node(
                name,
                vec![
                    column_ref(1, DataType::Utf8),
                    column_ref(2, DataType::Int64),
                ],
                DataType::Utf8,
                aggregate_binding(name, &[DataType::Utf8, DataType::Int64]),
            );

            let (decoded, arena) = lower_aggregate_window(&node).expect("lower max/min_by");
            let ExecNodeKind::Analytic(analytic) = decoded.node.kind else {
                panic!("expected analytic node");
            };
            let function = &analytic.functions[0];
            let binding = function
                .aggregate_binding
                .as_ref()
                .expect("aggregate binding");
            assert_eq!(binding.function_name, name);
            assert_eq!(
                binding.resolved.argument_types,
                [DataType::Utf8, DataType::Int64]
            );
            let [physical] = function.args.as_slice() else {
                panic!("expected one packed physical argument");
            };
            let Some(DataType::Struct(fields)) = arena.data_type(*physical) else {
                panic!("expected packed Struct physical input");
            };
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].data_type(), &DataType::Utf8);
            assert_eq!(fields[1].data_type(), &DataType::Int64);
        }
    }

    #[test]
    fn array_agg_function_order_is_separate_from_over_order() {
        let list_type = DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            DataType::Utf8,
            true,
        )));
        let output_columns = vec![
            output_column(1, "value", DataType::Utf8),
            output_column(2, "key", DataType::Int64),
            output_column(3, "window_value", list_type.clone()),
        ];
        let mut function_order = sort_item(2);
        function_order.asc = false;
        function_order.nulls_first = true;
        let node = physical_node(
            92,
            plan::plan_node::Kind::Window(plan::WindowNode {
                window_exprs: vec![plan::WindowExpr {
                    name: "array_agg".to_string(),
                    args: vec![column_ref(1, DataType::Utf8)],
                    distinct: false,
                    function_order_by: vec![function_order],
                    aggregate_binding: aggregate_update_binding(
                        "array_agg",
                        &[DataType::Utf8],
                        &[DataType::Utf8, DataType::Int64],
                    ),
                    partition_by: Vec::new(),
                    order_by: vec![sort_item(2)],
                    window_frame: None,
                    result_type: Some(type_desc(&list_type)),
                    output_name: "window_value".to_string(),
                    output_column_id: 3,
                    ignore_nulls: false,
                }],
                output_columns: output_columns.clone(),
            }),
            output_columns,
            vec![value_key_values_node(91)],
        );

        let (decoded, arena) = lower_aggregate_window(&node).expect("lower ordered array_agg");
        let ExecNodeKind::Analytic(analytic) = decoded.node.kind else {
            panic!("expected analytic node");
        };
        assert_eq!(analytic.order_by_exprs.len(), 1);
        let function = &analytic.functions[0];
        let WindowFunctionKind::ArrayAgg {
            is_asc_order,
            nulls_first,
            ..
        } = &function.kind
        else {
            panic!("expected array_agg");
        };
        assert_eq!(is_asc_order, &[false]);
        assert_eq!(nulls_first, &[true]);
        assert_eq!(
            function
                .aggregate_binding
                .as_ref()
                .expect("array_agg aggregate binding")
                .resolved
                .argument_types,
            [DataType::Utf8, DataType::Int64]
        );
        let [physical] = function.args.as_slice() else {
            panic!("expected packed value and function-order input");
        };
        let Some(DataType::Struct(fields)) = arena.data_type(*physical) else {
            panic!("expected packed Struct input");
        };
        assert_eq!(fields.len(), 2);
    }
}
