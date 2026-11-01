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

//! End-to-end pure graph boundaries for independent write stream roles.

use super::*;
use crate::{
    DataStreamPartitionType, ExchangeDeliveryRequirement, KernelAbiVersion, ProgramOutputRole,
    StaticExprKind, StaticExprNode, StaticRouterRoute, StaticStreamBranch, StaticStreamContract,
    StaticStreamRouting,
};
use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
    ValueTypeStorageLifetime,
};
use std::{collections::HashMap, num::NonZeroUsize, sync::Arc};

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
fn layout(fields: &[(&str, DataType, u32)]) -> StaticLayout {
    let schema = Arc::new(Schema::new(
        fields
            .iter()
            .map(|(name, ty, _)| Field::new(*name, ty.clone(), false))
            .collect::<Vec<_>>(),
    ));
    StaticLayout::try_new_typed_for_compile(
        schema,
        fields
            .iter()
            .map(|(_, _, slot)| SlotId::new(*slot))
            .collect(),
        fields
            .iter()
            .map(|(_, ty, _)| {
                (
                    FunctionValueType::new(ty.clone(), false)
                        .typed_binding()
                        .clone(),
                    None,
                )
            })
            .collect(),
        ValueTypeStorageLifetime::pure(),
        |_| Ok(()),
        &Control,
    )
    .unwrap()
}
fn expressions() -> Arc<ImmutableExpressions> {
    Arc::new(ImmutableExpressions::try_new(vec![], false, HashMap::new(), None).unwrap())
}
fn profile(layout: &StaticLayout) -> CompileProfile {
    CompileProfile::new(
        NonZeroUsize::new(1).unwrap(),
        None,
        layout.identity().unwrap(),
        KernelAbiVersion::CURRENT,
    )
}
fn values(layout: &StaticLayout, arrays: Vec<ArrayRef>) -> StaticValues {
    StaticValues::try_new(
        RecordBatch::try_new(layout.schema().clone(), arrays).unwrap(),
        layout.clone(),
    )
    .unwrap()
}
fn exchange() -> ProgramNodeKind {
    ProgramNodeKind::ExchangeSource {
        runtime_filters: vec![].into(),
        hash_partition_exprs: vec![],
    }
}

#[derive(Clone, Copy)]
enum RouteProjection {
    Data,
    ExplicitEffect,
    ImplicitWholeRoot,
}
fn router_graph(projection: RouteProjection) -> Result<LocalProgramGraph, LocalProgramError> {
    let input_layout = layout(&[("value", DataType::Int64, 2)]);
    let root_layout = layout(&[("effect", DataType::Int8, 1), ("value", DataType::Int64, 2)]);
    let summary_layout = layout(&[
        ("inserted", DataType::Int64, 10),
        ("replaced", DataType::Int64, 11),
        ("deleted", DataType::Int64, 12),
    ]);
    let (columns, route_layout) = match projection {
        RouteProjection::Data => (vec![SlotId::new(2)], input_layout.clone()),
        RouteProjection::ExplicitEffect => (
            vec![SlotId::new(1)],
            layout(&[("effect", DataType::Int8, 1)]),
        ),
        RouteProjection::ImplicitWholeRoot => (vec![], root_layout.clone()),
    };
    let branch = |node, columns, role| {
        StaticStreamBranch::try_new(
            node,
            DataStreamPartitionType::Unpartitioned,
            vec![],
            columns,
            None,
            StaticStreamContract {
                role,
                delivery: ExchangeDeliveryRequirement::MustDrain,
                routing: StaticStreamRouting::Ordinary,
            },
        )
        .unwrap()
    };
    let target = WriteTargetOrdinal::try_new(0).unwrap();
    let sink = StaticSinkProgram::try_change_stream_router(
        vec![
            branch(20, columns, ProgramOutputRole::ProviderRoute { target }),
            branch(
                21,
                summary_layout.slots().to_vec(),
                ProgramOutputRole::LogicalSummary,
            ),
        ],
        expressions(),
        SlotId::new(1),
        vec![StaticRouterRoute {
            layout: crate::ProgramWriteLayoutPreference::None,
            branch: 0,
            accepted_effects: Arc::from([ConnectorRowMutationEffect::Insert]),
        }],
        1,
        summary_layout.clone(),
    )
    .unwrap();
    let main = Arc::new(
        ImmutableExpressions::try_new(
            vec![StaticExprNode::new(
                StaticExprKind::SlotId(SlotId::new(2)),
                DataType::Int64,
                None,
            )],
            false,
            HashMap::new(),
            None,
        )
        .unwrap(),
    );
    let nodes = vec![
        ProgramNode::new(
            0,
            ProgramNodeKind::Values {
                values: values(&input_layout, vec![Arc::new(Int64Array::from(vec![7]))]),
            },
            input_layout,
        ),
        ProgramNode::new(
            1,
            ProgramNodeKind::ChangeEventExpand {
                input: ProgramNodeId::new(0),
                events: vec![ChangeEventSpec {
                    predicate: None,
                    effect: ConnectorRowMutationEffect::Insert,
                    assignments: vec![ChangeEventOutputExpr {
                        output_slot_id: SlotId::new(2),
                        expr: Some(ProgramExprId::new(0)),
                    }],
                }],
                output_slot_ids: vec![SlotId::new(1), SlotId::new(2)],
                effect_slot_id: SlotId::new(1),
            },
            root_layout.clone(),
        ),
    ];
    LocalProgramGraph::try_new_with_sink(
        nodes,
        ProgramNodeId::new(1),
        main,
        profile(&root_layout),
        BindingRequirements::try_new(vec![
            BindingRequirement::ExchangeOutput {
                branch: 0,
                layout: route_layout,
                role: ProgramOutputRole::ProviderRoute { target },
                delivery: ExchangeDeliveryRequirement::MustDrain,
            },
            BindingRequirement::ExchangeOutput {
                branch: 1,
                layout: summary_layout,
                role: ProgramOutputRole::LogicalSummary,
                delivery: ExchangeDeliveryRequirement::MustDrain,
            },
        ])
        .unwrap(),
        Some(sink),
    )
}

#[test]
fn router_provider_projection_rejects_explicit_and_implicit_control_columns() {
    router_graph(RouteProjection::Data)
        .expect("an actual expansion routes data beside an independent summary");
    for projection in [
        RouteProjection::ExplicitEffect,
        RouteProjection::ImplicitWholeRoot,
    ] {
        assert!(
            matches!(
                router_graph(projection),
                Err(LocalProgramError::InvalidSink)
            ),
            "a control discriminator must never become provider data"
        );
    }
}

#[derive(Clone, Copy)]
enum SummaryInput {
    Valid,
    RelationalRole,
    WrongDelivery,
    ValuesCarrier,
    SharedByTwoFinishes,
}
fn finish_graph(shape: SummaryInput) -> Result<LocalProgramGraph, LocalProgramError> {
    let writer_layout = layout(&[("writer_row", DataType::Int64, 2)]);
    let summary_layout = layout(&[
        ("inserted", DataType::Int64, 10),
        ("replaced", DataType::Int64, 11),
        ("deleted", DataType::Int64, 12),
    ]);
    let root_layout = layout(&[("affected_rows", DataType::Int64, 20)]);
    let summary_kind = if matches!(shape, SummaryInput::ValuesCarrier) {
        ProgramNodeKind::Values {
            values: values(
                &summary_layout,
                vec![
                    Arc::new(Int64Array::from(vec![1])),
                    Arc::new(Int64Array::from(vec![0])),
                    Arc::new(Int64Array::from(vec![0])),
                ],
            ),
        }
    } else {
        exchange()
    };
    let make_finish = |writer| ProgramNodeKind::TableFinish {
        inputs: vec![ProgramNodeId::new(writer), ProgramNodeId::new(1)],
        count_mode: novarocks_result_contract::FinishCountMode::LogicalChanges,
        input_roles: novarocks_result_contract::FinishInputs {
            writer_results: 0,
            logical_summary: Some(1),
        },
        logical_summary_layout: Some(summary_layout.clone()),
        expected_targets: vec![WriteTargetOrdinal::try_new(0).unwrap()],
        writer_multiplex_layout: writer_layout.clone(),
        root_result_layout: root_layout.clone(),
        final_aggregates: WriterFinalAggregatePlan {
            prepared: None,
            calls: vec![],
            unpivot: None,
        },
    };
    let mut nodes = vec![
        ProgramNode::new(0, exchange(), writer_layout.clone()),
        ProgramNode::new(1, summary_kind, summary_layout.clone()),
        ProgramNode::new(2, make_finish(0), root_layout.clone()),
    ];
    let mut requirements = vec![
        BindingRequirement::ExchangeInput {
            node: ProgramNodeId::new(0),
            layout: writer_layout.clone(),
            role: ProgramOutputRole::WriterResults,
            delivery: ExchangeDeliveryRequirement::MustDrain,
        },
        BindingRequirement::TableFinish {
            node: ProgramNodeId::new(2),
            layout: root_layout.clone(),
        },
    ];
    if !matches!(shape, SummaryInput::ValuesCarrier) {
        let (role, delivery) = if matches!(shape, SummaryInput::RelationalRole) {
            (
                ProgramOutputRole::Relational,
                ExchangeDeliveryRequirement::DemandDriven,
            )
        } else {
            (
                ProgramOutputRole::LogicalSummary,
                if matches!(shape, SummaryInput::WrongDelivery) {
                    ExchangeDeliveryRequirement::DemandDriven
                } else {
                    ExchangeDeliveryRequirement::MustDrain
                },
            )
        };
        requirements.push(BindingRequirement::ExchangeInput {
            node: ProgramNodeId::new(1),
            layout: summary_layout.clone(),
            role,
            delivery,
        });
    }
    let root = if matches!(shape, SummaryInput::SharedByTwoFinishes) {
        nodes.push(ProgramNode::new(3, exchange(), writer_layout.clone()));
        requirements.push(BindingRequirement::ExchangeInput {
            node: ProgramNodeId::new(3),
            layout: writer_layout.clone(),
            role: ProgramOutputRole::WriterResults,
            delivery: ExchangeDeliveryRequirement::MustDrain,
        });
        nodes.push(ProgramNode::new(4, make_finish(3), root_layout.clone()));
        requirements.push(BindingRequirement::TableFinish {
            node: ProgramNodeId::new(4),
            layout: root_layout.clone(),
        });
        nodes.push(ProgramNode::new(
            5,
            ProgramNodeKind::UnionAll {
                inputs: vec![ProgramNodeId::new(2), ProgramNodeId::new(4)],
            },
            root_layout.clone(),
        ));
        ProgramNodeId::new(5)
    } else {
        ProgramNodeId::new(2)
    };
    LocalProgramGraph::try_new(
        nodes,
        root,
        expressions(),
        profile(&root_layout),
        BindingRequirements::try_new(requirements).unwrap(),
    )
}

#[test]
fn logical_finish_requires_a_real_summary_role_with_one_consumer() {
    finish_graph(SummaryInput::Valid).expect("the exact independent summary exchange is valid");
    for shape in [
        SummaryInput::RelationalRole,
        SummaryInput::WrongDelivery,
        SummaryInput::ValuesCarrier,
        SummaryInput::SharedByTwoFinishes,
    ] {
        assert!(
            finish_graph(shape).is_err(),
            "same-shaped data cannot impersonate or multiply consume a summary"
        );
    }
}
