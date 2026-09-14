//! Native aggregate decoder integration coverage.

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use std::sync::Arc;

    use super::super::tests::*;

    fn aggregate_decode_context() -> NativePlanDecodeContext {
        NativePlanDecodeContext::default().with_function_catalog(Arc::new(
            novarocks_sql::compiler::build_builtin_engine_function_catalog()
                .expect("builtin function catalog"),
        ))
    }
    use super::super::{NativePlanDecodeContext, decode_node};
    use novarocks_execution::exec::expr::ExprArena;
    use novarocks_execution::exec::node::ExecNodeKind;
    use novarocks_proto_models::plan;
    use novarocks_types::SlotId;

    #[test]
    fn hash_aggregate_derives_output_columns_from_layout_sidecar() {
        let group_column = output_column(1, "id", DataType::Int64);
        let aggregate = physical_node(
            20,
            plan::plan_node::Kind::HashAggregate(plan::HashAggregateNode {
                mode: plan::AggMode::Single as i32,
                group_by: vec![column_ref(1, DataType::Int64)],
                aggregates: Vec::new(),
                is_merge: Vec::new(),
                output_layout: Some(plan::AggregateOutputLayout {
                    group_key_columns: vec![group_column],
                    aggregate_columns: Vec::new(),
                }),
                output_columns: Vec::new(),
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );

        let lowered = lower(&aggregate);
        let ExecNodeKind::Aggregate(aggregate) = lowered.node.kind else {
            panic!("expected Aggregate");
        };
        assert_eq!(aggregate.group_by.len(), 1);
        assert!(aggregate.functions.is_empty());
        assert_eq!(aggregate.output_chunk_schema.slot_ids(), &[SlotId::new(1)]);
        assert_eq!(lowered.layout.order(), &[SlotId::new(1)]);
    }

    #[test]
    fn hash_aggregate_rejects_group_key_output_type_drift() {
        let aggregate = physical_node(
            20,
            plan::plan_node::Kind::HashAggregate(plan::HashAggregateNode {
                mode: plan::AggMode::Single as i32,
                group_by: vec![column_ref(1, DataType::Int64)],
                aggregates: Vec::new(),
                is_merge: Vec::new(),
                output_layout: Some(plan::AggregateOutputLayout {
                    group_key_columns: vec![output_column(1, "id", DataType::Utf8)],
                    aggregate_columns: Vec::new(),
                }),
                output_columns: Vec::new(),
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );

        let error = decode_node(
            &aggregate,
            &mut ExprArena::default(),
            &aggregate_decode_context(),
        )
        .expect_err("group key output type drift must fail");
        assert!(
            error.to_string().contains("group key 0 output type drift"),
            "{error}"
        );
    }

    #[test]
    fn hash_aggregate_rejects_a_missing_exact_resolved_signature() {
        let output_columns = vec![output_column(2, "sum_id", DataType::Int64)];
        let aggregate = physical_node(
            20,
            plan::plan_node::Kind::HashAggregate(plan::HashAggregateNode {
                mode: plan::AggMode::Single as i32,
                group_by: Vec::new(),
                aggregates: vec![plan::PlanAggregateCall {
                    name: "sum".to_string(),
                    args: vec![column_ref(1, DataType::Int64)],
                    distinct: false,
                    result_type: Some(type_desc(&DataType::Int64)),
                    order_by: Vec::new(),
                    output_column_id: 2,
                    resolved_signature: None,
                }],
                is_merge: vec![false],
                output_layout: Some(plan::AggregateOutputLayout {
                    group_key_columns: Vec::new(),
                    aggregate_columns: output_columns.clone(),
                }),
                output_columns: output_columns.clone(),
            }),
            output_columns,
            vec![one_col_values_node(10)],
        );
        let error = decode_node(
            &aggregate,
            &mut ExprArena::default(),
            &aggregate_decode_context(),
        )
        .expect_err("missing resolved signature must fail");
        assert!(
            error
                .to_string()
                .contains("exact resolved signature missing"),
            "{error}"
        );
    }

    #[test]
    fn hash_aggregate_rejects_state_format_drift_from_the_process_catalog() {
        let output_columns = vec![output_column(2, "sum_id", DataType::Int64)];
        let mut resolved = resolved_aggregate_signature("sum", &[DataType::Int64])
            .expect("resolved aggregate signature");
        resolved.state_format_identity = "novarocks/sum/state-v2".to_string();
        let aggregate = physical_node(
            20,
            plan::plan_node::Kind::HashAggregate(plan::HashAggregateNode {
                mode: plan::AggMode::Single as i32,
                group_by: Vec::new(),
                aggregates: vec![plan::PlanAggregateCall {
                    name: "sum".to_string(),
                    args: vec![column_ref(1, DataType::Int64)],
                    distinct: false,
                    result_type: Some(type_desc(&DataType::Int64)),
                    order_by: Vec::new(),
                    output_column_id: 2,
                    resolved_signature: Some(resolved),
                }],
                is_merge: vec![false],
                output_layout: Some(plan::AggregateOutputLayout {
                    group_key_columns: Vec::new(),
                    aggregate_columns: output_columns.clone(),
                }),
                output_columns: output_columns.clone(),
            }),
            output_columns,
            vec![one_col_values_node(10)],
        );
        let error = decode_node(
            &aggregate,
            &mut ExprArena::default(),
            &aggregate_decode_context(),
        )
        .expect_err("state format drift must fail");
        assert!(
            error.to_string().contains("resolved signature drift"),
            "{error}"
        );
    }

    #[test]
    fn hash_aggregate_projects_visible_subset_after_full_layout_output() {
        let group_a = output_column(1, "a", DataType::Int64);
        let group_c = output_column(3, "c", DataType::Int64);
        let sum_b = output_column(4, "sum_b", DataType::Int64);
        let visible_output = vec![sum_b.clone()];
        let aggregate = physical_node(
            20,
            plan::plan_node::Kind::HashAggregate(plan::HashAggregateNode {
                mode: plan::AggMode::Single as i32,
                group_by: vec![
                    column_ref(1, DataType::Int64),
                    column_ref(3, DataType::Int64),
                ],
                aggregates: vec![plan::PlanAggregateCall {
                    name: "sum".to_string(),
                    args: vec![column_ref(2, DataType::Int64)],
                    distinct: false,
                    result_type: Some(type_desc(&DataType::Int64)),
                    order_by: Vec::new(),
                    output_column_id: 4,
                    resolved_signature: resolved_aggregate_signature("sum", &[DataType::Int64]),
                }],
                is_merge: vec![false],
                output_layout: Some(plan::AggregateOutputLayout {
                    group_key_columns: vec![group_a, group_c],
                    aggregate_columns: vec![sum_b],
                }),
                output_columns: visible_output.clone(),
            }),
            visible_output,
            vec![three_col_values_node(10)],
        );

        let lowered = lower(&aggregate);
        assert_eq!(lowered.layout.order(), &[SlotId::new(4)]);
        let ExecNodeKind::Project(project) = lowered.node.kind else {
            panic!("expected visible-output projection");
        };
        assert!(project.is_subordinate);
        assert_eq!(project.expr_slot_ids, vec![SlotId::new(4)]);
        assert_eq!(project.output_chunk_schema.slot_ids(), &[SlotId::new(4)]);
        let ExecNodeKind::Aggregate(aggregate) = project.input.kind else {
            panic!("expected Aggregate below visible-output projection");
        };
        assert_eq!(aggregate.group_by.len(), 2);
        assert_eq!(aggregate.functions.len(), 1);
        assert_eq!(
            aggregate.output_chunk_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(3), SlotId::new(4)]
        );
    }

    #[test]
    fn hash_aggregate_uses_inferred_intermediate_type() {
        let output_columns = vec![output_column(2, "avg_id", DataType::Float64)];
        let aggregate = physical_node(
            20,
            plan::plan_node::Kind::HashAggregate(plan::HashAggregateNode {
                mode: plan::AggMode::Single as i32,
                group_by: Vec::new(),
                aggregates: vec![plan::PlanAggregateCall {
                    name: "avg".to_string(),
                    args: vec![column_ref(1, DataType::Int64)],
                    distinct: false,
                    result_type: Some(type_desc(&DataType::Float64)),
                    order_by: Vec::new(),
                    output_column_id: 2,
                    resolved_signature: resolved_aggregate_signature("avg", &[DataType::Int64]),
                }],
                is_merge: vec![false],
                output_layout: Some(plan::AggregateOutputLayout {
                    group_key_columns: Vec::new(),
                    aggregate_columns: output_columns.clone(),
                }),
                output_columns: output_columns.clone(),
            }),
            output_columns,
            vec![one_col_values_node(10)],
        );
        let lowered = lower(&aggregate);
        let ExecNodeKind::Aggregate(aggregate) = lowered.node.kind else {
            panic!("expected Aggregate");
        };
        let types = aggregate.functions[0]
            .types
            .as_ref()
            .expect("aggregate type signature");
        assert_eq!(types.intermediate_type, Some(DataType::Utf8));
        assert_eq!(types.output_type, Some(DataType::Float64));
        assert_eq!(types.input_arg_type, Some(DataType::Int64));
    }

    #[test]
    fn hash_aggregate_local_avg_signature_keeps_final_output_type() {
        let output_columns = vec![output_column(2, "avg_id", DataType::Utf8)];
        let aggregate = physical_node(
            20,
            plan::plan_node::Kind::HashAggregate(plan::HashAggregateNode {
                mode: plan::AggMode::Local as i32,
                group_by: Vec::new(),
                aggregates: vec![plan::PlanAggregateCall {
                    name: "avg".to_string(),
                    args: vec![column_ref(1, DataType::Int64)],
                    distinct: false,
                    result_type: Some(type_desc(&DataType::Float64)),
                    order_by: Vec::new(),
                    output_column_id: 2,
                    resolved_signature: resolved_aggregate_signature("avg", &[DataType::Int64]),
                }],
                is_merge: vec![false],
                output_layout: Some(plan::AggregateOutputLayout {
                    group_key_columns: Vec::new(),
                    aggregate_columns: output_columns.clone(),
                }),
                output_columns: output_columns.clone(),
            }),
            output_columns,
            vec![one_col_values_node(10)],
        );
        let lowered = lower(&aggregate);
        let ExecNodeKind::Aggregate(aggregate) = lowered.node.kind else {
            panic!("expected Aggregate");
        };
        let types = aggregate.functions[0]
            .types
            .as_ref()
            .expect("aggregate type signature");
        assert_eq!(types.intermediate_type, Some(DataType::Utf8));
        assert_eq!(types.output_type, Some(DataType::Float64));
        assert_eq!(types.input_arg_type, Some(DataType::Int64));
        assert_eq!(
            aggregate
                .output_chunk_schema
                .field(0)
                .expect("avg output field")
                .data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn hash_aggregate_rejects_sql_result_type_drift() {
        let output_columns = vec![output_column(2, "avg_id", DataType::Utf8)];
        let aggregate = physical_node(
            20,
            plan::plan_node::Kind::HashAggregate(plan::HashAggregateNode {
                mode: plan::AggMode::Local as i32,
                group_by: Vec::new(),
                aggregates: vec![plan::PlanAggregateCall {
                    name: "avg".to_string(),
                    args: vec![column_ref(1, DataType::Int64)],
                    distinct: false,
                    result_type: Some(type_desc(&DataType::Utf8)),
                    order_by: Vec::new(),
                    output_column_id: 2,
                    resolved_signature: resolved_aggregate_signature("avg", &[DataType::Int64]),
                }],
                is_merge: vec![false],
                output_layout: Some(plan::AggregateOutputLayout {
                    group_key_columns: Vec::new(),
                    aggregate_columns: output_columns.clone(),
                }),
                output_columns: output_columns.clone(),
            }),
            output_columns,
            vec![one_col_values_node(10)],
        );

        let error = decode_node(
            &aggregate,
            &mut ExprArena::default(),
            &aggregate_decode_context(),
        )
        .expect_err("SQL result type drift must fail");
        assert!(
            error.to_string().contains("SQL result type drift"),
            "{error}"
        );
    }

    #[test]
    fn hash_aggregate_rejects_phase_output_layout_type_drift() {
        let output_columns = vec![output_column(2, "avg_id", DataType::Float64)];
        let aggregate = physical_node(
            20,
            plan::plan_node::Kind::HashAggregate(plan::HashAggregateNode {
                mode: plan::AggMode::Local as i32,
                group_by: Vec::new(),
                aggregates: vec![plan::PlanAggregateCall {
                    name: "avg".to_string(),
                    args: vec![column_ref(1, DataType::Int64)],
                    distinct: false,
                    result_type: Some(type_desc(&DataType::Float64)),
                    order_by: Vec::new(),
                    output_column_id: 2,
                    resolved_signature: resolved_aggregate_signature("avg", &[DataType::Int64]),
                }],
                is_merge: vec![false],
                output_layout: Some(plan::AggregateOutputLayout {
                    group_key_columns: Vec::new(),
                    aggregate_columns: output_columns.clone(),
                }),
                output_columns: output_columns.clone(),
            }),
            output_columns,
            vec![one_col_values_node(10)],
        );

        let error = decode_node(
            &aggregate,
            &mut ExprArena::default(),
            &aggregate_decode_context(),
        )
        .expect_err("phase output layout type drift must fail");
        assert!(
            error.to_string().contains("phase output type drift"),
            "{error}"
        );
    }

    #[test]
    fn hash_aggregate_merge_reuses_the_frozen_update_signature() {
        let output_columns = vec![output_column(3, "avg_id", DataType::Float64)];
        let aggregate = physical_node(
            20,
            plan::plan_node::Kind::HashAggregate(plan::HashAggregateNode {
                mode: plan::AggMode::Global as i32,
                group_by: Vec::new(),
                aggregates: vec![plan::PlanAggregateCall {
                    name: "avg".to_string(),
                    args: vec![column_ref(2, DataType::Utf8)],
                    distinct: false,
                    result_type: Some(type_desc(&DataType::Float64)),
                    order_by: Vec::new(),
                    output_column_id: 3,
                    resolved_signature: resolved_aggregate_signature("avg", &[DataType::Int64]),
                }],
                is_merge: vec![true],
                output_layout: Some(plan::AggregateOutputLayout {
                    group_key_columns: Vec::new(),
                    aggregate_columns: output_columns.clone(),
                }),
                output_columns: output_columns.clone(),
            }),
            output_columns,
            vec![values_node(10)],
        );

        let lowered = lower(&aggregate);
        let ExecNodeKind::Aggregate(aggregate) = lowered.node.kind else {
            panic!("expected Aggregate");
        };
        assert_eq!(
            aggregate.resolved_aggregates[0].argument_types,
            [DataType::Int64]
        );
        assert_eq!(
            aggregate.functions[0]
                .types
                .as_ref()
                .expect("aggregate type signature")
                .input_arg_type,
            Some(DataType::Int64)
        );
    }

    #[test]
    fn hash_aggregate_ordered_inputs_pack_order_by_exprs() {
        let resolved_signature = resolved_aggregate_update_signature(
            "group_concat",
            &[DataType::Utf8, DataType::Utf8],
            &[DataType::Utf8, DataType::Utf8, DataType::Int64],
        );
        let intermediate_type = novarocks_plan_codec::native_type::decode_type(
            resolved_signature
                .as_ref()
                .and_then(|signature| signature.intermediate_type.as_ref())
                .expect("catalog intermediate type"),
        )
        .expect("decoded intermediate type");
        let output_columns = vec![output_column(3, "gc", intermediate_type.clone())];
        let aggregate = physical_node(
            20,
            plan::plan_node::Kind::HashAggregate(plan::HashAggregateNode {
                mode: plan::AggMode::Local as i32,
                group_by: Vec::new(),
                aggregates: vec![plan::PlanAggregateCall {
                    name: "group_concat".to_string(),
                    args: vec![column_ref(2, DataType::Utf8), string_literal("|")],
                    distinct: true,
                    result_type: Some(type_desc(&DataType::Utf8)),
                    order_by: vec![sort_item(1)],
                    output_column_id: 3,
                    resolved_signature,
                }],
                is_merge: vec![false],
                output_layout: Some(plan::AggregateOutputLayout {
                    group_key_columns: Vec::new(),
                    aggregate_columns: output_columns.clone(),
                }),
                output_columns: output_columns.clone(),
            }),
            output_columns,
            vec![values_node(10)],
        );

        let mut arena = ExprArena::default();
        let lowered = decode_node(&aggregate, &mut arena, &aggregate_decode_context())
            .expect("lower ordered aggregate");
        let ExecNodeKind::Aggregate(aggregate) = lowered.node.kind else {
            panic!("expected Aggregate");
        };
        assert_eq!(aggregate.functions[0].inputs.len(), 1);
        let input_type = arena
            .data_type(aggregate.functions[0].inputs[0])
            .expect("packed input type");
        let DataType::Struct(fields) = input_type else {
            panic!("expected packed struct input, got {input_type:?}");
        };

        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0].data_type(), &DataType::Utf8);
        assert_eq!(fields[1].data_type(), &DataType::Utf8);
        assert_eq!(fields[2].data_type(), &DataType::Int64);
        assert_eq!(aggregate.functions[0].order.is_asc_order, vec![true]);
        assert_eq!(aggregate.functions[0].order.nulls_first, vec![false]);
        assert!(aggregate.functions[0].order.is_distinct);
        assert_eq!(
            aggregate.resolved_aggregates[0].argument_types,
            [DataType::Utf8, DataType::Utf8, DataType::Int64]
        );
        let DataType::Struct(intermediate_fields) =
            &aggregate.resolved_aggregates[0].intermediate_type
        else {
            panic!("expected group_concat Struct intermediate");
        };
        assert_eq!(intermediate_fields.len(), 3);
    }

    #[test]
    fn hash_aggregate_rejects_count_if_order_by_before_input_selection() {
        let output_columns = vec![output_column(3, "cnt", DataType::Int64)];
        let aggregate = physical_node(
            21,
            plan::plan_node::Kind::HashAggregate(plan::HashAggregateNode {
                mode: plan::AggMode::Local as i32,
                group_by: Vec::new(),
                aggregates: vec![plan::PlanAggregateCall {
                    name: "count_if".to_string(),
                    args: vec![bool_literal(true)],
                    distinct: false,
                    result_type: Some(type_desc(&DataType::Int64)),
                    order_by: vec![sort_item(1)],
                    output_column_id: 3,
                    resolved_signature: resolved_aggregate_signature(
                        "count_if",
                        &[DataType::Boolean],
                    ),
                }],
                is_merge: vec![false],
                output_layout: Some(plan::AggregateOutputLayout {
                    group_key_columns: Vec::new(),
                    aggregate_columns: output_columns.clone(),
                }),
                output_columns: output_columns.clone(),
            }),
            output_columns,
            vec![values_node(10)],
        );

        let mut arena = ExprArena::default();
        let err = decode_node(&aggregate, &mut arena, &aggregate_decode_context())
            .expect_err("count_if ORDER BY should be rejected before input selection");
        assert!(
            err.contains("count_if does not support ORDER BY"),
            "unexpected error: {err}"
        );
    }
}
