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

use super::encode_type;
use super::output::encode_output_columns;
use super::scan::encode_scan_node;
use super::scan_facts::NativeScanFacts;
use super::type_mapping::{
    encode_agg_mode, encode_join_distribution, encode_join_execution_mode, encode_join_kind,
    encode_redistribute_mode, encode_row_mutation_effect, encode_set_op_kind,
    encode_sort_topn_type, encode_topn_phase, usize_to_u64,
};
use super::{NativePlanEncodeContext, encode_exprs};
use crate::expr::{encode_expr, encode_sort_items, encode_window_frame};
use novarocks_proto_codec::{FieldPath, arrow_physical};
use novarocks_proto_models::plan;
use novarocks_sql::plan_read::{
    PhysicalPlanKind, PlanRowCountAssertion, SqlPhysicalPlanRead, physical_plan_read,
};

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Retained for target-specific frontend integration and regression coverage."
)]
pub(super) fn encoded_physical_variant_names_for_test() -> &'static [&'static str] {
    &[
        "Scan",
        "Filter",
        "Project",
        "Unpivot",
        "Sort",
        "Limit",
        "Values",
        "Repeat",
        "Window",
        "GenerateSeries",
        "TableFunction",
        "AssertOneRow",
        "TopN",
        "HashAggregate",
        "HashJoin",
        "NestLoopJoin",
        "SetOp",
        "ChangeEventExpand",
        "CTEAnchor",
        "CTEProduce",
        "CTEConsume",
        "Redistribute",
    ]
}

pub(super) fn encode_physical_node<'a, F: NativeScanFacts<'a>>(
    src: &PhysicalPlanKind,
    node_id: i32,
    ctx: &NativePlanEncodeContext<'a, F>,
) -> Result<plan::PlanNode, String> {
    use plan::plan_node::Kind;

    let (output_columns, kind) = match physical_plan_read(src) {
        SqlPhysicalPlanRead::Scan(node) => (
            encode_output_columns(&node.columns)?,
            Kind::Scan(encode_scan_node(&node, node_id, ctx)?),
        ),
        SqlPhysicalPlanRead::Filter { predicate } => (
            Vec::new(),
            Kind::Filter(plan::FilterNode {
                predicate: Some(encode_expr(&predicate)?),
            }),
        ),
        SqlPhysicalPlanRead::Project(node) => (
            Vec::new(),
            Kind::Project(plan::ProjectNode {
                items: node
                    .items
                    .iter()
                    .map(|item| {
                        Ok(plan::ProjectItem {
                            expr: Some(encode_expr(&item.expr)?),
                            output_name: item.output_name.clone(),
                            output_column_id: item.output_column_id.0,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
                output_qualifier: node.output_qualifier.clone(),
            }),
        ),
        SqlPhysicalPlanRead::Unpivot(node) => (
            Vec::new(),
            Kind::Unpivot(plan::UnpivotNode {
                passthrough_columns: node
                    .passthrough_columns
                    .iter()
                    .map(|mapping| plan::UnpivotPassthroughColumn {
                        input_column_id: mapping.input_column_id.0,
                        output_column_id: mapping.output_column_id.0,
                    })
                    .collect(),
                value_output_column_id: node.value_output_column_id.0,
                literal_output_column_ids: node
                    .literal_output_column_ids
                    .iter()
                    .map(|id| id.0)
                    .collect(),
                value_mappings: node
                    .value_mappings
                    .iter()
                    .map(|mapping| {
                        Ok(plan::UnpivotValueMapping {
                            input_value_column_id: mapping.input_value_column_id.0,
                            constants: mapping
                                .constants
                                .iter()
                                .map(encode_unpivot_constant)
                                .collect::<Result<Vec<_>, String>>()?,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
                max_output_rows: usize_to_u64(node.max_output_rows),
                max_output_bytes: usize_to_u64(node.max_output_bytes),
                output_schema: Some(encode_unpivot_output_schema(&node.output_columns)?),
            }),
        ),
        SqlPhysicalPlanRead::Sort(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::Sort(plan::SortNode {
                items: encode_sort_items(&node.items)?,
                analytic_partition_by: encode_exprs(&node.analytic_partition_by)?,
                output_columns: encode_output_columns(&node.output_columns)?,
                offset: node.offset,
                partition_limit: node.partition_limit.map(usize_to_u64),
                topn_type: node.topn_type.map(encode_sort_topn_type),
            }),
        ),
        SqlPhysicalPlanRead::Limit { limit, offset } => {
            (Vec::new(), Kind::Limit(plan::LimitNode { limit, offset }))
        }
        SqlPhysicalPlanRead::Values(node) => (
            encode_output_columns(&node.columns)?,
            Kind::Values(plan::ValuesNode {
                rows: node
                    .rows
                    .iter()
                    .map(|row| {
                        Ok(plan::ExprList {
                            values: encode_exprs(row)?,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
                columns: encode_output_columns(&node.columns)?,
            }),
        ),
        SqlPhysicalPlanRead::Repeat(node) => (
            Vec::new(),
            Kind::Repeat(plan::RepeatNode {
                repeat_column_ref_list: node
                    .repeat_column_ref_list
                    .iter()
                    .map(|values| plan::StringList {
                        values: values.clone(),
                    })
                    .collect(),
                repeat_column_ref_ids: node
                    .repeat_column_ref_ids
                    .iter()
                    .map(|values| plan::UInt32List {
                        values: values.iter().map(|id| id.0).collect(),
                    })
                    .collect(),
                grouping_ids: node.grouping_ids.clone(),
                all_rollup_columns: node.all_rollup_columns.clone(),
                all_rollup_column_ids: node.all_rollup_column_ids.iter().map(|id| id.0).collect(),
                grouping_key_aliases: node
                    .grouping_key_aliases
                    .iter()
                    .map(|(first, second)| plan::StringPair {
                        first: first.clone(),
                        second: second.clone(),
                    })
                    .collect(),
                grouping_fn_args: node
                    .grouping_fn_args
                    .iter()
                    .map(|(name, values)| plan::NamedStringList {
                        name: name.clone(),
                        values: values.clone(),
                    })
                    .collect(),
                grouping_fn_arg_ids: node
                    .grouping_fn_arg_ids
                    .iter()
                    .map(|values| plan::UInt32List {
                        values: values.iter().map(|id| id.0).collect(),
                    })
                    .collect(),
                grouping_fn_ids: node
                    .grouping_fn_ids
                    .iter()
                    .map(|(name, value)| plan::NamedUInt32 {
                        name: name.clone(),
                        value: value.0,
                    })
                    .collect(),
                virtual_tuple_id: node.virtual_tuple_id,
            }),
        ),
        SqlPhysicalPlanRead::Window(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::Window(plan::WindowNode {
                window_exprs: node
                    .window_exprs
                    .iter()
                    .map(|expr| {
                        Ok(plan::WindowExpr {
                            name: expr.name.clone(),
                            args: encode_exprs(&expr.args)?,
                            distinct: expr.distinct,
                            function_order_by: encode_sort_items(&expr.function_order_by)?,
                            aggregate_binding: expr
                                .aggregate_binding
                                .as_ref()
                                .map(encode_resolved_aggregate_signature)
                                .transpose()?,
                            partition_by: encode_exprs(&expr.partition_by)?,
                            order_by: encode_sort_items(&expr.order_by)?,
                            window_frame: expr
                                .window_frame
                                .as_ref()
                                .map(encode_window_frame)
                                .transpose()?,
                            result_type: Some(encode_type(&expr.result_type)?),
                            output_name: expr.output_name.clone(),
                            output_column_id: expr.output_column_id.0,
                            ignore_nulls: expr.ignore_nulls,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
                output_columns: encode_output_columns(&node.output_columns)?,
            }),
        ),
        SqlPhysicalPlanRead::GenerateSeries(node) => (
            Vec::new(),
            Kind::GenerateSeries(plan::GenerateSeriesNode {
                start: node.start,
                end: node.end,
                step: node.step,
                column_name: node.column_name.clone(),
                alias: node.alias.clone(),
                output_column_id: node.output_column_id.0,
            }),
        ),
        SqlPhysicalPlanRead::TableFunction(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::TableFunction(plan::TableFunctionNode {
                function_name: node.function_name.clone(),
                args: encode_exprs(&node.args)?,
                output_columns: encode_output_columns(&node.output_columns)?,
                alias: node.alias.clone(),
                is_left_join: node.is_left_join,
            }),
        ),
        SqlPhysicalPlanRead::AssertOneRow(node) => (
            Vec::new(),
            Kind::AssertOneRow(plan::AssertOneRowNode {
                subquery_text: node.subquery_text.clone(),
                desired_num_rows: node.desired_num_rows,
                assertion: encode_row_count_assertion(node.assertion),
                group_key_column_ids: node
                    .group_key_column_ids
                    .iter()
                    .map(|column_id| column_id.0)
                    .collect(),
                group_key_labels: node.group_key_labels.clone(),
                keyed_message_prefix: node.keyed_message_prefix.clone(),
            }),
        ),
        SqlPhysicalPlanRead::TopN(node) => (
            Vec::new(),
            Kind::Topn(plan::TopNNode {
                items: encode_sort_items(&node.items)?,
                limit: node.limit,
                offset: node.offset,
                phase: encode_topn_phase(node.phase),
                is_split: node.is_split,
            }),
        ),
        SqlPhysicalPlanRead::HashAggregate(node) => {
            // Baseline raw layout/output columns straight from the physical payload.
            // In a sealed plan `apply_sealed_node_output_columns` overwrites both the
            // node output columns and this `output_layout`/`output_columns` from the
            // finalized aggregate contract (which applies the per-mode intermediate
            // aggregate-state types). This raw form only stands in the bare-node
            // encoder unit tests that have no sealed plan; the intermediate-type
            // determination is owned by the planner (`finalize_hash_aggregate_wire`).
            let raw_output_columns = if node.output_columns.is_empty() {
                node.output_layout
                    .group_key_columns
                    .iter()
                    .chain(node.output_layout.aggregate_columns.iter())
                    .cloned()
                    .collect()
            } else {
                node.output_columns.clone()
            };
            (
                encode_output_columns(&raw_output_columns)?,
                Kind::HashAggregate(plan::HashAggregateNode {
                    mode: encode_agg_mode(node.mode),
                    group_by: encode_exprs(&node.group_by)?,
                    aggregates: node
                        .aggregates
                        .iter()
                        .map(|call| {
                            Ok(plan::PlanAggregateCall {
                                name: call.name.clone(),
                                args: encode_exprs(&call.args)?,
                                distinct: call.distinct,
                                // AggregateCall.result_type follows the physical
                                // phase layout after optimizer materialization.
                                // The wire field is the final SQL result type;
                                // phase carriers are sealed independently in
                                // output_layout.aggregate_columns.
                                result_type: Some(encode_type(&call.resolved.output_type)?),
                                order_by: encode_sort_items(&call.order_by)?,
                                output_column_id: call.output_column_id.0,
                                resolved_signature: Some(encode_resolved_aggregate_signature(
                                    &call.resolved,
                                )?),
                            })
                        })
                        .collect::<Result<Vec<_>, String>>()?,
                    is_merge: node.is_merge.clone(),
                    output_layout: Some(plan::AggregateOutputLayout {
                        group_key_columns: encode_output_columns(
                            &node.output_layout.group_key_columns,
                        )?,
                        aggregate_columns: encode_output_columns(
                            &node.output_layout.aggregate_columns,
                        )?,
                    }),
                    output_columns: encode_output_columns(&raw_output_columns)?,
                }),
            )
        }
        SqlPhysicalPlanRead::HashJoin(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::HashJoin(plan::HashJoinNode {
                join_type: encode_join_kind(node.join_type),
                eq_conditions: node
                    .eq_conditions
                    .iter()
                    .map(|cond| {
                        Ok(plan::HashJoinEqCondition {
                            left: Some(encode_expr(&cond.left)?),
                            right: Some(encode_expr(&cond.right)?),
                            null_safe: cond.null_safe,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
                other_condition: node.other_condition.as_ref().map(encode_expr).transpose()?,
                distribution: encode_join_distribution(&node.distribution),
                execution_mode: node.execution_mode.map(encode_join_execution_mode),
            }),
        ),
        SqlPhysicalPlanRead::NestLoopJoin(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::NestLoopJoin(plan::NestLoopJoinNode {
                join_type: encode_join_kind(node.join_type),
                condition: node.condition.as_ref().map(encode_expr).transpose()?,
            }),
        ),
        SqlPhysicalPlanRead::SetOp(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::SetOp(plan::SetOpNode {
                kind: encode_set_op_kind(node.kind),
                output_columns: encode_output_columns(&node.output_columns)?,
                child_output_columns: node
                    .child_output_columns
                    .iter()
                    .map(|columns| {
                        Ok(plan::OutputColumnList {
                            columns: encode_output_columns(columns)?,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
            }),
        ),
        SqlPhysicalPlanRead::ChangeEventExpand(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::ChangeEventExpand(plan::ChangeEventExpandNode {
                events: node
                    .events
                    .iter()
                    .map(|event| {
                        Ok(plan::DistributedChangeEventSpec {
                            predicate: event.predicate.as_ref().map(encode_expr).transpose()?,
                            effect: encode_row_mutation_effect(event.effect),
                            assignments: event
                                .assignments
                                .iter()
                                .map(|assignment| {
                                    Ok(plan::DistributedChangeEventOutputExpr {
                                        output_column_id: assignment.output_column_id.0,
                                        expr: assignment
                                            .expr
                                            .as_ref()
                                            .map(encode_expr)
                                            .transpose()?,
                                    })
                                })
                                .collect::<Result<Vec<_>, String>>()?,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
                output_columns: encode_output_columns(&node.output_columns)?,
                effect_column_id: node.effect_column_id.0,
            }),
        ),
        SqlPhysicalPlanRead::CTEAnchor { cte_id } => {
            (Vec::new(), Kind::CteAnchor(plan::CteAnchorNode { cte_id }))
        }
        SqlPhysicalPlanRead::CTEProduce(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::CteProduce(plan::CteProduceNode {
                cte_id: node.cte_id,
                output_columns: encode_output_columns(&node.output_columns)?,
            }),
        ),
        SqlPhysicalPlanRead::CTEConsume(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::CteConsume(plan::CteConsumeNode {
                cte_id: node.cte_id,
                alias: node.alias.clone(),
                output_columns: encode_output_columns(&node.output_columns)?,
                producer_column_ids: node.producer_column_ids.iter().map(|id| id.0).collect(),
            }),
        ),
        SqlPhysicalPlanRead::Redistribute(node) => (
            encode_output_columns(&node.output_columns)?,
            Kind::Redistribute(plan::RedistributeNode {
                mode: Some(encode_redistribute_mode(&node.mode)),
                partition_exprs: encode_exprs(&node.partition_exprs)?,
                output_columns: encode_output_columns(&node.output_columns)?,
            }),
        ),
    };

    Ok(plan::PlanNode {
        output_columns,
        kind: Some(kind),
    })
}

fn encode_unpivot_output_schema(
    columns: &[novarocks_sql::plan_read::OutputColumn],
) -> Result<plan::ArrowPhysicalSchema, String> {
    let schema = arrow::datatypes::Schema::new(
        columns
            .iter()
            .map(|column| {
                arrow::datatypes::Field::new(
                    &column.name,
                    column.data_type.clone(),
                    column.nullable,
                )
            })
            .collect::<Vec<_>>(),
    );
    let slot_ids = columns
        .iter()
        .map(|column| column.column_id.0)
        .collect::<Vec<_>>();
    let internal = columns
        .iter()
        .map(|column| column.is_internal)
        .collect::<Vec<_>>();
    let (columns, schema_metadata) = arrow_physical::encode_schema_with_internal(
        &schema,
        &slot_ids,
        &internal,
        FieldPath::root("unpivot.output_schema"),
    )
    .map_err(|error| error.to_string())?;
    Ok(plan::ArrowPhysicalSchema {
        columns,
        schema_metadata,
    })
}

pub(super) fn encode_unpivot_constant(
    constant: &novarocks_sql::plan_read::UnpivotConstant,
) -> Result<plan::UnpivotConstant, String> {
    use plan::unpivot_constant::Value;
    let value = match constant {
        novarocks_sql::plan_read::UnpivotConstant::Scalar(expression) => {
            if !matches!(
                expression.kind,
                novarocks_sql::plan_read::ExprKind::Literal(_)
            ) {
                return Err("Unpivot scalar constant is not a literal expression".to_string());
            }
            Value::ScalarLiteral(encode_expr(expression)?)
        }
        novarocks_sql::plan_read::UnpivotConstant::Int32List(values) => {
            Value::Int32List(plan::Int32List {
                values: values.clone(),
            })
        }
        novarocks_sql::plan_read::UnpivotConstant::Utf8Map(entries) => {
            Value::Utf8Map(plan::Utf8Map {
                entries: entries
                    .iter()
                    .map(|(key, value)| plan::Utf8MapEntry {
                        key: key.clone(),
                        value: value.clone(),
                    })
                    .collect(),
            })
        }
    };
    Ok(plan::UnpivotConstant { value: Some(value) })
}

pub(super) fn encode_resolved_aggregate_signature(
    binding: &novarocks_functions::ResolvedAggregateSignature,
) -> Result<plan::ResolvedAggregateSignature, String> {
    Ok(plan::ResolvedAggregateSignature {
        overload_identity: binding.overload.as_str().to_string(),
        argument_types: binding
            .argument_types
            .iter()
            .map(encode_type)
            .collect::<Result<Vec<_>, _>>()?,
        intermediate_type: Some(encode_type(&binding.intermediate_type)?),
        output_type: Some(encode_type(&binding.output_type)?),
        state_format_identity: binding.state_format.as_str().to_string(),
    })
}

fn encode_row_count_assertion(assertion: PlanRowCountAssertion) -> i32 {
    match assertion {
        PlanRowCountAssertion::Eq => plan::RowCountAssertion::Eq as i32,
        PlanRowCountAssertion::Ne => plan::RowCountAssertion::Ne as i32,
        PlanRowCountAssertion::Lt => plan::RowCountAssertion::Lt as i32,
        PlanRowCountAssertion::Le => plan::RowCountAssertion::Le as i32,
        PlanRowCountAssertion::Gt => plan::RowCountAssertion::Gt as i32,
        PlanRowCountAssertion::Ge => plan::RowCountAssertion::Ge as i32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn find_hash_aggregate(node: &plan::DistributedNode) -> Option<&plan::HashAggregateNode> {
        if let Some(plan::distributed_node::Payload::Physical(physical)) = node.payload.as_ref()
            && let Some(plan::plan_node::Kind::HashAggregate(aggregate)) = physical.kind.as_ref()
        {
            return Some(aggregate);
        }
        node.children.iter().find_map(find_hash_aggregate)
    }

    #[test]
    fn encodes_complete_exact_aggregate_binding() {
        let binding = novarocks_functions::ResolvedAggregateSignature {
            overload: novarocks_functions::AggregateOverloadIdentity::try_new(
                "builtin/max_by/utf8-int64/v1",
            )
            .unwrap(),
            argument_types: vec![
                arrow::datatypes::DataType::Utf8,
                arrow::datatypes::DataType::Int64,
            ],
            intermediate_type: arrow::datatypes::DataType::Binary,
            output_type: arrow::datatypes::DataType::Utf8,
            state_format: novarocks_functions::AggregateStateFormatIdentity::try_new(
                "novarocks/max_by/state-v1",
            )
            .unwrap(),
        };

        let encoded = encode_resolved_aggregate_signature(&binding).unwrap();

        assert_eq!(encoded.overload_identity, binding.overload.as_str());
        assert_eq!(encoded.argument_types.len(), 2);
        assert!(encoded.intermediate_type.is_some());
        assert!(encoded.output_type.is_some());
        assert_eq!(encoded.state_format_identity, binding.state_format.as_str());
    }

    #[test]
    fn local_aggregate_wire_separates_final_result_from_phase_carrier() {
        let source = novarocks_sql::test_support::native_encoder_plan(
            novarocks_sql::test_support::NativeEncoderPlanFixture::LocalAverageStreamEdge,
        )
        .expect("sealed local average fixture");
        let encoded = super::super::encode_distributed_plan_with_context(
            &source,
            NativePlanEncodeContext {
                scan_facts: None,
                node_outputs: None,
                fragment_edge_outputs: None,
                write_contracts: None,
                write_targets: None,
            },
        )
        .expect("encode local average fixture");
        let aggregate = encoded
            .fragments
            .iter()
            .filter_map(|fragment| fragment.root.as_ref())
            .find_map(find_hash_aggregate)
            .expect("encoded local aggregate");
        let call = aggregate.aggregates.first().expect("average call");
        assert_eq!(
            call.result_type,
            Some(encode_type(&arrow::datatypes::DataType::Float64).expect("final type"))
        );
        let phase_column = aggregate
            .output_layout
            .as_ref()
            .and_then(|layout| layout.aggregate_columns.first())
            .and_then(|column| column.r#type.as_ref())
            .expect("phase output type");
        assert_eq!(
            phase_column,
            &encode_type(&arrow::datatypes::DataType::Utf8).expect("intermediate type")
        );
    }

    #[test]
    fn encodes_typed_unpivot_contract() {
        let physical = novarocks_sql::plan_read::unpivot_physical_plan_for_test();
        let encoded = encode_physical_node(
            &physical,
            7,
            &NativePlanEncodeContext::<super::super::scan_facts::NoScanFacts> {
                scan_facts: None,
                node_outputs: None,
                fragment_edge_outputs: None,
                write_contracts: None,
                write_targets: None,
            },
        )
        .unwrap();
        let Some(plan::plan_node::Kind::Unpivot(unpivot)) = encoded.kind.as_ref() else {
            panic!("expected Unpivot");
        };
        assert_eq!(unpivot.passthrough_columns.len(), 1);
        assert_eq!(unpivot.value_mappings.len(), 2);
        assert_eq!(unpivot.literal_output_column_ids, vec![12]);
        assert_eq!(unpivot.max_output_rows, 128);
        assert_eq!(unpivot.max_output_bytes, 4096);
        assert!(encoded.output_columns.is_empty());
        let output_schema = unpivot.output_schema.as_ref().expect("exact output schema");
        assert_eq!(output_schema.columns.len(), 3);
        assert_eq!(
            output_schema
                .columns
                .iter()
                .map(|column| column.slot_id)
                .collect::<Vec<_>>(),
            vec![11, 12, 13]
        );
    }
}
