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

use super::*;
use crate::resolved_calls::relational_tests as fixture;
use crate::*;
use arrow_array::{RecordBatch, new_null_array};
use arrow_schema::{DataType, Field, Schema};
use novarocks_functions::{AggregateKernelPhase, FunctionKind, PureKernelAbi};
use novarocks_type_contract::{FunctionArgumentType, MAX_CONTROL_USE_REFERENCES, ValueLogicalType};
use std::{collections::HashMap, num::NonZeroUsize, sync::Mutex};

#[derive(Default)]
struct Control {
    failure: Option<(u32, CompileControlError)>,
    seen: Mutex<Vec<u32>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, work: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::LowerProgram);
        assert!(work <= 256);
        self.seen.lock().unwrap().push(work);
        if let Some((at, error)) = self.failure.clone()
            && work == at
        {
            Err(error)
        } else {
            Ok(())
        }
    }
}
fn i64_type(nullable: bool) -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, nullable)
}
fn site(node: usize, role: ProgramChannelLayoutRole, ordinal: u32) -> ProgramChannelSite {
    ProgramChannelSite::Layout {
        node: ProgramNodeId::new(node),
        role,
        ordinal,
    }
}
fn output(node: usize) -> ProgramChannelSite {
    site(node, ProgramChannelLayoutRole::NodeOutput, 0)
}
fn expression_types(calls: ProgramResolvedCalls, nullable: bool) -> ProgramTypedExpressions {
    // Every definition in these real relational fixtures is an Int64 literal.
    let types = calls
        .snapshot()
        .roots()
        .arenas()
        .iter()
        .map(|(arena, definitions)| {
            assert!(
                definitions
                    .nodes()
                    .iter()
                    .all(|node| node.data_type() == &DataType::Int64)
            );
            (
                *arena,
                vec![FunctionArgumentType::Value(i64_type(nullable)); definitions.nodes().len()],
            )
        })
        .collect();
    ProgramTypedExpressions::try_new(calls, types, &Control::default()).unwrap()
}
fn aggregate(phase: AggregateKernelPhase, nullable: bool) -> ProgramTypedExpressions {
    let owner = Arc::new(fixture::Owner::new(
        FunctionKind::Aggregate,
        &[PureKernelAbi::AggregateWindowV1],
    ));
    let catalog = fixture::aggregate_catalog(owner.clone());
    let call = fixture::Call::new(&owner, 0);
    let token = fixture::prepare_aggregate_token(&catalog, &call, phase);
    let p = fixture::aggregate_node(
        &owner,
        !phase.consumes_logical_arguments(),
        phase.produces_final_result(),
        StaticAggregateOrder::default(),
        0,
    );
    let calls = ProgramResolvedCalls::try_new(
        fixture::snapshot(p),
        vec![(fixture::aggregate_site(), token)],
        &Control::default(),
    )
    .unwrap();
    expression_types(calls, nullable)
}
fn writer() -> ProgramTypedExpressions {
    let owner = Arc::new(fixture::Owner::new(
        FunctionKind::Aggregate,
        &[PureKernelAbi::AggregateWindowV1],
    ));
    let catalog = fixture::aggregate_catalog(owner.clone());
    let calls = ProgramResolvedCalls::try_new(
        fixture::snapshot(fixture::writer_program(&owner, 1, 2)),
        fixture::writer_tokens(&catalog, &owner),
        &Control::default(),
    )
    .unwrap();
    expression_types(calls, false)
}
fn writer_channels() -> Vec<(ProgramChannelSite, FunctionValueType)> {
    use ProgramChannelLayoutRole as Role;
    vec![
        output(0),
        output(1),
        output(2),
        site(1, Role::WriterProjection, 0),
        site(1, Role::WriterMultiplex, 0),
        site(2, Role::WriterMultiplex, 0),
        site(2, Role::WriterRootResult, 0),
        ProgramChannelSite::WriterFinalOutput {
            node: ProgramNodeId::new(2),
            call: 0,
        },
    ]
    .into_iter()
    .map(|key| (key, i64_type(false)))
    .collect()
}
fn table() -> ProgramTypedExpressions {
    let (owner, catalog) = fixture::table_fixture();
    let call = fixture::Call::new(&owner, 0);
    let token = fixture::prepare_table_token(&catalog, &call);
    let calls = ProgramResolvedCalls::try_new(
        fixture::snapshot(fixture::table_program(DataType::Int64, DataType::Int64, 1)),
        vec![(fixture::table_site(), token)],
        &Control::default(),
    )
    .unwrap();
    expression_types(calls, false)
}
fn table_channels() -> Vec<(ProgramChannelSite, FunctionValueType)> {
    vec![
        (output(0), i64_type(false)),
        (output(1), i64_type(false)),
        (
            ProgramChannelSite::TableResult {
                node: ProgramNodeId::new(1),
                result: 0,
            },
            i64_type(false),
        ),
    ]
}
fn rebuild(original: &LocalProgramGraph, nodes: Vec<ProgramNode>) -> LocalProgramGraph {
    let root = original.root();
    let profile = CompileProfile::new(
        NonZeroUsize::new(1).unwrap(),
        None,
        nodes[root.index()].output_layout().identity().unwrap(),
        KernelAbiVersion::CURRENT,
    );
    LocalProgramGraph::try_new(
        nodes,
        root,
        original.expressions().clone(),
        profile,
        original.requirements().clone(),
    )
    .unwrap()
}
fn retyped_table(
    mut change: impl FnMut(&mut ProgramNodeKind, &mut StaticLayout),
) -> ProgramTypedExpressions {
    let original = fixture::table_program(DataType::Int64, DataType::Int64, 1);
    let nodes = original
        .nodes()
        .iter()
        .enumerate()
        .map(|(index, node)| {
            let mut kind = node.kind().clone();
            let mut layout = node.output_layout().clone();
            if index == 1 {
                change(&mut kind, &mut layout)
            }
            ProgramNode::new(
                node.legacy_native_node_id().expect("legacy fixture node"),
                kind,
                layout,
            )
        })
        .collect();
    let p = rebuild(&original, nodes);
    let (owner, catalog) = fixture::table_fixture();
    let call = fixture::Call::new(&owner, 0);
    let token = fixture::prepare_table_token(&catalog, &call);
    let calls = ProgramResolvedCalls::try_new(
        fixture::snapshot(p),
        vec![(fixture::table_site(), token)],
        &Control::default(),
    )
    .unwrap();
    expression_types(calls, false)
}

#[test]
fn actual_four_phase_inputs_and_emissions_are_typed_without_copying_the_prepared_call() {
    for phase in [
        AggregateKernelPhase::Single,
        AggregateKernelPhase::Partial,
        AggregateKernelPhase::Intermediate,
        AggregateKernelPhase::Final,
    ] {
        let expressions = aggregate(phase, false);
        let call = expressions.resolved_calls().calls()[&fixture::aggregate_site()].call_contract()
            as *const _;
        let typed = ProgramTypedChannels::try_new(
            expressions,
            vec![(output(0), i64_type(false)), (output(1), i64_type(false))],
            &Control::default(),
        )
        .unwrap();
        assert_eq!(
            typed.expressions().resolved_calls().calls()[&fixture::aggregate_site()].call_contract()
                as *const _,
            call
        );
        assert_eq!(typed.channel_slot(output(1)), Some(SlotId::new(1)));
        let actual = typed.channel_type(output(1)).unwrap();
        assert!(std::ptr::eq(
            actual,
            typed
                .slot_type(
                    ProgramNodeId::new(1),
                    ProgramChannelLayoutRole::NodeOutput,
                    SlotId::new(1)
                )
                .unwrap()
        ));
        let ProgramStateTemplate::Aggregate { kernel, .. } =
            typed.expressions().resolved_calls().calls()[&fixture::aggregate_site()]
                .state_template()
        else {
            panic!("aggregate handle")
        };
        fixture::run_aggregate(kernel, phase);
    }
}
#[test]
fn nullable_actual_argument_cannot_enter_a_nonnullable_selected_aggregate_channel() {
    for phase in [
        AggregateKernelPhase::Partial,
        AggregateKernelPhase::Intermediate,
    ] {
        assert_eq!(
            ProgramTypedChannels::try_new(
                aggregate(phase, true),
                vec![(output(0), i64_type(false)), (output(1), i64_type(false))],
                &Control::default()
            )
            .unwrap_err(),
            ProgramChannelTypeError::TypeMismatch
        );
    }
}
#[test]
fn writer_internal_final_slot_has_an_explicit_output_type_outside_input_multiplex() {
    let typed =
        ProgramTypedChannels::try_new(writer(), writer_channels(), &Control::default()).unwrap();
    let internal = ProgramChannelSite::WriterFinalOutput {
        node: ProgramNodeId::new(2),
        call: 0,
    };
    assert_eq!(typed.channel_slot(internal), Some(SlotId::new(999)));
    assert!(
        typed
            .slot_type(
                ProgramNodeId::new(2),
                ProgramChannelLayoutRole::WriterMultiplex,
                SlotId::new(999)
            )
            .is_none()
    );
    assert_eq!(typed.channel_type(internal), Some(&i64_type(false)));
    assert!(std::ptr::eq(
        typed
            .channel_layout(
                ProgramNodeId::new(1),
                ProgramChannelLayoutRole::WriterProjection
            )
            .unwrap(),
        match typed
            .expressions()
            .resolved_calls()
            .snapshot()
            .program()
            .nodes()[1]
            .kind()
        {
            ProgramNodeKind::TableWriter { projection, .. } => &projection.layout,
            _ => unreachable!(),
        }
    ));
    let mut missing = writer_channels();
    missing.retain(|(key, _)| *key != internal);
    assert_eq!(
        ProgramTypedChannels::try_new(writer(), missing, &Control::default()).unwrap_err(),
        ProgramChannelTypeError::MissingSite(internal)
    );
}
#[test]
fn mandatory_channel_coverage_has_no_default_and_duplicate_keys_never_replace_facts() {
    let source = table();
    let mut channels = table_channels();
    channels.push(channels[0].clone());
    assert_eq!(
        ProgramTypedChannels::try_new(source.clone(), channels, &Control::default()).unwrap_err(),
        ProgramChannelTypeError::DuplicateSite
    );
    let mut missing = table_channels();
    missing.remove(1);
    assert_eq!(
        ProgramTypedChannels::try_new(source.clone(), missing, &Control::default()).unwrap_err(),
        ProgramChannelTypeError::MissingSite(output(1))
    );
    let mut extra = table_channels();
    extra.push((
        site(0, ProgramChannelLayoutRole::WriterProjection, 0),
        i64_type(false),
    ));
    assert_eq!(
        ProgramTypedChannels::try_new(source, extra, &Control::default()).unwrap_err(),
        ProgramChannelTypeError::InvalidSite
    );
}
#[test]
fn unprojected_table_results_remain_mandatory_independent_of_outer_projection() {
    let source = retyped_table(|kind, layout| {
        let ProgramNodeKind::TableFunction {
            fn_result_required,
            output_slot_sources,
            outer_slots,
            ..
        } = kind
        else {
            unreachable!()
        };
        *fn_result_required = false;
        *outer_slots = vec![SlotId::new(1)];
        *output_slot_sources = vec![TableFunctionOutputSlot::Outer {
            slot: SlotId::new(1),
        }];
        *layout = StaticLayout::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "outer",
                DataType::Int64,
                false,
            )])),
            Arc::from([SlotId::new(1)]),
        )
        .unwrap();
    });
    let typed =
        ProgramTypedChannels::try_new(source.clone(), table_channels(), &Control::default())
            .unwrap();
    assert_eq!(typed.channel_slot(output(1)), Some(SlotId::new(1)));
    assert_eq!(
        typed.channel_type(ProgramChannelSite::TableResult {
            node: ProgramNodeId::new(1),
            result: 0
        }),
        Some(&i64_type(false))
    );
    let mut missing = table_channels();
    missing.pop();
    assert_eq!(
        ProgramTypedChannels::try_new(source, missing, &Control::default()).unwrap_err(),
        ProgramChannelTypeError::MissingSite(ProgramChannelSite::TableResult {
            node: ProgramNodeId::new(1),
            result: 0
        })
    );
}
#[test]
fn left_outer_table_projection_widens_only_projected_results_without_changing_function_relation() {
    let source = retyped_table(|kind, layout| {
        let ProgramNodeKind::TableFunction { is_left_join, .. } = kind else {
            unreachable!()
        };
        *is_left_join = true;
        *layout = StaticLayout::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "result",
                DataType::Int64,
                true,
            )])),
            Arc::from([SlotId::new(2)]),
        )
        .unwrap();
    });
    let mut channels = table_channels();
    channels[1].1 = channels[1].1.with_nullable(true).unwrap();
    let typed = ProgramTypedChannels::try_new(source, channels, &Control::default()).unwrap();
    assert!(typed.channel_type(output(1)).unwrap().nullable());
    assert!(
        !typed
            .channel_type(ProgramChannelSite::TableResult {
                node: ProgramNodeId::new(1),
                result: 0
            })
            .unwrap()
            .nullable()
    );
}

fn values_program(fields: Vec<Field>) -> ProgramTypedExpressions {
    let schema = Arc::new(Schema::new(fields));
    let arrays = schema
        .fields()
        .iter()
        .map(|field| new_null_array(field.data_type(), 1))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    let slots = (0..schema.fields().len())
        .map(|ordinal| SlotId::new(u32::try_from(ordinal + 1).unwrap()))
        .collect::<Vec<_>>();
    let layout = StaticLayout::try_new(schema, slots.into()).unwrap();
    let profile = CompileProfile::new(
        NonZeroUsize::new(1).unwrap(),
        None,
        layout.identity().unwrap(),
        KernelAbiVersion::CURRENT,
    );
    let arena = Arc::new(
        ImmutableExpressions::try_new(
            vec![StaticExprNode::new(
                StaticExprKind::Literal(StaticLiteral::Int64(7)),
                DataType::Int64,
                None,
            )],
            false,
            HashMap::new(),
            None,
        )
        .unwrap(),
    );
    let program = LocalProgramGraph::try_new(
        vec![ProgramNode::new(
            10,
            ProgramNodeKind::Values {
                values: StaticValues::try_new(batch, layout.clone()).unwrap(),
            },
            layout,
        )],
        ProgramNodeId::new(0),
        arena,
        profile,
        BindingRequirements::try_new(vec![]).unwrap(),
    )
    .unwrap();
    let calls =
        ProgramResolvedCalls::try_new(fixture::snapshot(program), vec![], &Control::default())
            .unwrap();
    expression_types(calls, false)
}
#[test]
fn explicit_json_root_identity_is_required_without_inferring_it_from_plain_utf8() {
    let json =
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap();
    let field = Field::new("json", DataType::Utf8, true).with_metadata(HashMap::from([(
        novarocks_type_contract::NR_LOGICAL_TYPE_KEY.into(),
        "json".into(),
    )]));
    let source = values_program(vec![field]);
    ProgramTypedChannels::try_new(source.clone(), vec![(output(0), json)], &Control::default())
        .unwrap();
    assert_eq!(
        ProgramTypedChannels::try_new(
            source,
            vec![(output(0), FunctionValueType::new(DataType::Utf8, true))],
            &Control::default()
        )
        .unwrap_err(),
        ProgramChannelTypeError::TypeMismatch
    );
    // An unlabeled carrier permits an explicitly supplied logical fact; no
    // fallback fabricates that fact or replaces the caller's typed table.
    let source = values_program(vec![Field::new("unlabeled", DataType::Utf8, true)]);
    let json =
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap();
    let checked =
        ProgramTypedChannels::try_new(source, vec![(output(0), json)], &Control::default())
            .unwrap();
    assert_eq!(
        checked.channel_type(output(0)).unwrap().logical_type(),
        ValueLogicalType::Json
    );
}
#[test]
fn nested_nullability_is_semantic_but_dictionary_ids_and_source_attributes_are_not() {
    #[allow(deprecated)]
    let nested = |id, nullable, annotation: &str| {
        DataType::Struct(
            vec![Arc::new(
                Field::new_dict(
                    "dictionary",
                    DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                    nullable,
                    id,
                    true,
                )
                .with_metadata(HashMap::from([(
                    "fixture.annotation".into(),
                    annotation.into(),
                )])),
            )]
            .into(),
        )
    };
    let actual = nested(1, true, "first");
    let source = values_program(vec![Field::new("struct", actual.clone(), true)]);
    ProgramTypedChannels::try_new(
        source.clone(),
        vec![(output(0), FunctionValueType::new(actual, true))],
        &Control::default(),
    )
    .unwrap();
    for changed in [nested(2, true, "first"), nested(1, true, "changed")] {
        ProgramTypedChannels::try_new(
            source.clone(),
            vec![(output(0), FunctionValueType::new(changed, true))],
            &Control::default(),
        )
        .unwrap();
    }
    assert_eq!(
        ProgramTypedChannels::try_new(
            source,
            vec![(
                output(0),
                FunctionValueType::new(nested(1, false, "first"), true)
            )],
            &Control::default(),
        )
        .unwrap_err(),
        ProgramChannelTypeError::TypeMismatch
    );
}
struct OwnerControl;
impl PureCompileControl for OwnerControl {
    fn checkpoint(&self, _: CompilePhase, work: u32) -> Result<(), CompileControlError> {
        assert!(work <= 256);
        Ok(())
    }
}
#[test]
fn full_window_argument_and_output_types_borrow_the_same_frozen_partition_owner() {
    let owner = Arc::new(fixture::Owner::new(
        FunctionKind::Window,
        &[PureKernelAbi::WindowV1],
    ));
    let mut builder = novarocks_functions::EngineFunctionCatalogBuilder::new();
    builder
        .register(
            novarocks_functions::FunctionDefinition::try_new_pure_window(
                "channel_window",
                novarocks_functions::FunctionVisibility::Public,
                owner.clone(),
            )
            .unwrap(),
        )
        .unwrap();
    let catalogue = builder.seal_pure(owner.manifest()).unwrap();
    let call = fixture::Call::new(&owner, 0);
    let token = catalogue
        .prepare_frozen(
            call.input(),
            call.selected.clone(),
            &owner.frozen(&call.selected),
            novarocks_functions::PureCallPreparation::Window {
                arguments: call.children(),
                options: fixture::window_options(),
            },
            &OwnerControl,
        )
        .unwrap();
    let calls = ProgramResolvedCalls::try_new(
        fixture::snapshot(fixture::window_program(
            &owner,
            false,
            Some(fixture::local_frame()),
            false,
        )),
        vec![(fixture::window_site(), token)],
        &Control::default(),
    )
    .unwrap();
    let channels = vec![(output(0), i64_type(false)), (output(1), i64_type(false))];
    let typed = ProgramTypedChannels::try_new(
        expression_types(calls.clone(), false),
        channels.clone(),
        &Control::default(),
    )
    .unwrap();
    let ProgramStateTemplate::WindowPartition { kernel, .. } =
        typed.expressions().resolved_calls().calls()[&fixture::window_site()].state_template()
    else {
        panic!("actual window handle")
    };
    fixture::run_window(kernel.clone(), &[3, 5]);
    assert_eq!(
        ProgramTypedChannels::try_new(expression_types(calls, true), channels, &Control::default())
            .unwrap_err(),
        ProgramChannelTypeError::TypeMismatch
    );
}
#[test]
fn typed_channel_entry_and_positive_work_preserve_control_categories() {
    let source = values_program(
        (0..300)
            .map(|ordinal| Field::new(format!("column_{ordinal}"), DataType::Int64, true))
            .collect(),
    );
    let entries = (0..300)
        .map(|ordinal| {
            (
                site(0, ProgramChannelLayoutRole::NodeOutput, ordinal),
                i64_type(true),
            )
        })
        .collect::<Vec<_>>();
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in [0, 256] {
            let control = Control {
                failure: Some((at, error.clone())),
                seen: Mutex::default(),
            };
            assert_eq!(
                ProgramTypedChannels::try_new(source.clone(), entries.clone(), &control)
                    .unwrap_err(),
                ProgramChannelTypeError::Control(error.clone())
            );
            assert!(control.seen.lock().unwrap().contains(&at));
        }
    }
}
#[test]
fn schema_channel_positions_use_an_independent_checked_definition_budget() {
    let mut count = MAX_PROGRAM_TYPED_CHANNELS - 1;
    add_count(&mut count, 1).unwrap();
    assert_eq!(count, MAX_PROGRAM_TYPED_CHANNELS);
    assert_eq!(
        add_count(&mut count, 1),
        Err(ProgramChannelTypeError::TooManyChannels)
    );
    let mut overflow = usize::MAX;
    assert_eq!(
        add_count(&mut overflow, 1),
        Err(ProgramChannelTypeError::TooManyChannels)
    );
    // Actual schema width beyond the invocation budget remains legal. Each
    // entry is a channel position, not an expression use or selected call.
    let width = MAX_CONTROL_USE_REFERENCES + 1;
    let source = values_program(
        (0..width)
            .map(|ordinal| Field::new(format!("column_{ordinal}"), DataType::Null, true))
            .collect(),
    );
    let entries = (0..width)
        .map(|ordinal| {
            (
                site(
                    0,
                    ProgramChannelLayoutRole::NodeOutput,
                    u32::try_from(ordinal).unwrap(),
                ),
                FunctionValueType::new(DataType::Null, true),
            )
        })
        .collect();
    let typed = ProgramTypedChannels::try_new(source, entries, &Control::default()).unwrap();
    assert_eq!(typed.channels().len(), width);
    assert!(typed.expressions().resolved_calls().calls().is_empty());
}

#[test]
fn actual_join_side_and_scope_layouts_keep_separate_explicit_logical_domains() {
    use ProgramChannelLayoutRole as Role;
    let left = StaticLayout::try_new(
        Arc::new(Schema::new(vec![Field::new("left", DataType::Utf8, true)])),
        Arc::from([SlotId::new(1)]),
    )
    .unwrap();
    let right = StaticLayout::try_new(
        Arc::new(Schema::new(vec![Field::new("right", DataType::Utf8, true)])),
        Arc::from([SlotId::new(2)]),
    )
    .unwrap();
    let joined = StaticLayout::try_new(
        Arc::new(Schema::new(vec![
            Field::new("left", DataType::Utf8, true),
            Field::new("right", DataType::Utf8, true),
        ])),
        Arc::from([SlotId::new(1), SlotId::new(2)]),
    )
    .unwrap();
    let values = |layout: &StaticLayout| {
        let batch = RecordBatch::try_new(
            layout.schema().clone(),
            vec![new_null_array(&DataType::Utf8, 1)],
        )
        .unwrap();
        StaticValues::try_new(batch, layout.clone()).unwrap()
    };
    let nodes = vec![
        ProgramNode::new(
            1,
            ProgramNodeKind::Values {
                values: values(&left),
            },
            left.clone(),
        ),
        ProgramNode::new(
            2,
            ProgramNodeKind::Values {
                values: values(&right),
            },
            right.clone(),
        ),
        ProgramNode::new(
            3,
            ProgramNodeKind::NestedLoopJoin {
                left: ProgramNodeId::new(0),
                right: ProgramNodeId::new(1),
                join_type: NestedLoopJoinType::Inner,
                join_conjunct: None,
                left_layout: left,
                right_layout: right,
                join_scope_layout: joined.clone(),
            },
            joined.clone(),
        ),
    ];
    let arena = Arc::new(
        ImmutableExpressions::try_new(
            vec![StaticExprNode::new(
                StaticExprKind::Literal(StaticLiteral::Int64(7)),
                DataType::Int64,
                None,
            )],
            false,
            HashMap::new(),
            None,
        )
        .unwrap(),
    );
    let profile = CompileProfile::new(
        NonZeroUsize::new(1).unwrap(),
        None,
        joined.identity().unwrap(),
        KernelAbiVersion::CURRENT,
    );
    let program = LocalProgramGraph::try_new(
        nodes,
        ProgramNodeId::new(2),
        arena,
        profile,
        BindingRequirements::try_new(vec![]).unwrap(),
    )
    .unwrap();
    let source = expression_types(
        ProgramResolvedCalls::try_new(fixture::snapshot(program), vec![], &Control::default())
            .unwrap(),
        false,
    );
    let json =
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap();
    let plain = FunctionValueType::new(DataType::Utf8, true);
    let channels = vec![
        (output(0), json.clone()),
        (output(1), plain.clone()),
        (output(2), json.clone()),
        (site(2, Role::NodeOutput, 1), plain.clone()),
        (site(2, Role::JoinLeft, 0), json.clone()),
        (site(2, Role::JoinRight, 0), plain.clone()),
        (site(2, Role::JoinScope, 0), json),
        (site(2, Role::JoinScope, 1), plain.clone()),
    ];
    let checked =
        ProgramTypedChannels::try_new(source.clone(), channels.clone(), &Control::default())
            .unwrap();
    assert_eq!(
        checked
            .slot_type(ProgramNodeId::new(2), Role::JoinLeft, SlotId::new(1))
            .unwrap()
            .logical_type(),
        ValueLogicalType::Json
    );
    assert_eq!(
        checked
            .slot_type(ProgramNodeId::new(2), Role::JoinRight, SlotId::new(2))
            .unwrap()
            .logical_type(),
        ValueLogicalType::Physical
    );
    let mut missing = channels.clone();
    missing.retain(|(key, _)| *key != site(2, Role::JoinScope, 1));
    assert_eq!(
        ProgramTypedChannels::try_new(source.clone(), missing, &Control::default()).unwrap_err(),
        ProgramChannelTypeError::MissingSite(site(2, Role::JoinScope, 1))
    );
    let mut wrong_side = channels;
    wrong_side[4].1 = plain;
    assert_eq!(
        ProgramTypedChannels::try_new(source, wrong_side, &Control::default()).unwrap_err(),
        ProgramChannelTypeError::TypeMismatch
    );
}

struct OrderedOwner {
    inner: fixture::Owner,
    declaration: novarocks_functions::FunctionBindingDeclaration,
    selected: Arc<novarocks_functions::FunctionBindingSelection>,
}
impl OrderedOwner {
    fn new() -> Self {
        use novarocks_functions::*;
        let inner = fixture::Owner::new(FunctionKind::Aggregate, &[PureKernelAbi::AggregateV1]);
        let mut selected = inner.selections[0].as_ref().clone();
        selected.argument_types = vec![
            FunctionArgumentType::Value(i64_type(false)),
            FunctionArgumentType::Value(FunctionValueType::new(DataType::Int32, false)),
        ]
        .into();
        let base = inner
            .declaration
            .effect_declaration(&selected.overload)
            .unwrap()
            .clone();
        let state = selected.aggregate.as_ref().unwrap();
        let declaration = FunctionBindingDeclaration::try_new_complete(
            inner.declaration.function_id().clone(),
            FunctionKind::Aggregate,
            vec![FunctionOverloadDeclaration::from_effects(
                selected.overload.clone(),
                "Int64, Int32",
                "Int64",
                Some(AggregateBindingDeclaration {
                    state_argument_contract:
                        novarocks_type_contract::AggregateStateArgumentContract::ExactSignature,
                    intermediate_pattern: "Int64".into(),
                    state_format: state.state_format.clone(),
                }),
                base,
            )],
        )
        .unwrap();
        Self {
            inner,
            declaration,
            selected: Arc::new(selected),
        }
    }
    fn signature(&self) -> novarocks_functions::ResolvedAggregateSignature {
        novarocks_functions::ResolvedAggregateSignature {
            overload: novarocks_functions::AggregateOverloadIdentity::try_new(
                self.selected.overload.as_str(),
            )
            .unwrap(),
            argument_types: vec![DataType::Int64, DataType::Int32],
            intermediate_type: DataType::Int64,
            output_type: DataType::Int64,
            state_format: self
                .selected
                .aggregate
                .as_ref()
                .unwrap()
                .state_format
                .clone(),
        }
    }
}
impl novarocks_functions::PureFunctionMetadataOwner for OrderedOwner {
    fn binding_declaration(&self) -> &novarocks_functions::FunctionBindingDeclaration {
        &self.declaration
    }
    fn implementation_declarations(&self) -> &[novarocks_functions::PureImplementationDeclaration] {
        &self.inner.implementations
    }
}
impl novarocks_functions::FunctionBindingResolver for OrderedOwner {
    fn resolve(
        &self,
        request: novarocks_functions::FunctionBindingRequest<'_>,
        _control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::FunctionBindingSelection,
        novarocks_functions::FunctionBindingError,
    > {
        self.validate_selected(&self.selected, request, _control)?;
        Ok(self.selected.as_ref().clone())
    }
    fn validate_selected(
        &self,
        selected: &novarocks_functions::FunctionBindingSelection,
        request: novarocks_functions::FunctionBindingRequest<'_>,
        _control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<(), novarocks_functions::FunctionBindingError> {
        if selected != self.selected.as_ref()
            || request.logical_argument_count != 1
            || request.arguments.len() != 2
            || request
                .arguments
                .iter()
                .map(novarocks_functions::FunctionArgument::argument_type)
                .collect::<Vec<_>>()
                .as_slice()
                != selected.argument_types.as_ref()
        {
            Err(novarocks_functions::FunctionBindingError::NoMatchingOverload)
        } else {
            Ok(())
        }
    }
}
impl novarocks_functions::FunctionEffectOwner for OrderedOwner {
    type Error = novarocks_functions::FunctionBindingError;
    fn declaration(
        &self,
        function: &novarocks_functions::FunctionId,
        selected: &novarocks_functions::FunctionBindingSelection,
    ) -> Result<&novarocks_type_contract::FunctionEffectDeclaration, Self::Error> {
        if function != self.declaration.function_id() {
            return Err(novarocks_functions::FunctionBindingError::UnknownFunction);
        };
        self.declaration.effect_declaration(&selected.overload)
    }
    fn validate_and_refine(
        &self,
        input: novarocks_functions::CallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<
        novarocks_type_contract::CallEffects,
        novarocks_functions::FunctionEffectOwnerError<Self::Error>,
    > {
        use novarocks_functions::FunctionBindingResolver;
        control
            .checkpoint(CompilePhase::FunctionSpecialization, 1)
            .map_err(novarocks_functions::FunctionEffectOwnerError::Control)?;
        self.validate_selected(input.selected, input.request, control)?;
        Ok(self.inner.frozen(input.selected))
    }
}
impl novarocks_functions::AggregateSignatureResolver for OrderedOwner {
    fn resolve_aggregate(
        &self,
        args: &[DataType],
    ) -> Result<
        novarocks_functions::ResolvedAggregateSignature,
        novarocks_functions::FunctionResolutionError,
    > {
        if args == [DataType::Int64, DataType::Int32] {
            Ok(self.signature())
        } else {
            Err(
                novarocks_functions::FunctionResolutionError::NoMatchingSignature {
                    candidates: 1,
                    binding_enforced: true,
                },
            )
        }
    }
    fn produces_null(&self) -> bool {
        false
    }
}
impl novarocks_functions::PureAggregateImplementation for OrderedOwner {
    type Kernel = fixture::Sum;
    fn prepare_aggregate(
        &self,
        input: novarocks_functions::CallEffectInput<'_>,
        contract: Arc<novarocks_functions::AggregateCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<Self::Kernel>, novarocks_functions::KernelFailure> {
        novarocks_functions::PureAggregateImplementation::prepare_aggregate(
            &self.inner,
            input,
            contract,
            control,
        )
    }
}
struct RuntimeControl;
impl novarocks_functions::KernelEvaluationControl for RuntimeControl {
    fn checkpoint(&self, work: u32) -> Result<(), KernelFailure> {
        assert!(work <= 256);
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        Ok(())
    }
}
#[test]
fn independent_order_tail_has_its_actual_complete_type_and_is_materialized_as_a_separate_channel() {
    use novarocks_functions::*;
    use novarocks_type_contract::{
        CallProofScope, DecimalOverflowPolicy, EvaluationDemand, EvaluationDomainId,
        ExpressionEffectContext, ExpressionUseId, SemanticParameters,
    };
    let owner = Arc::new(OrderedOwner::new());
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(
            FunctionDefinition::try_new_pure_aggregate(
                "typed_order_sum",
                FunctionVisibility::Public,
                owner.clone(),
            )
            .unwrap(),
        )
        .unwrap();
    let manifest = owner.inner.manifest();
    let catalogue = builder.seal_pure(manifest).unwrap();
    let arguments = vec![
        FunctionArgument::Value {
            value_type: i64_type(false),
            constant: None,
        },
        FunctionArgument::Value {
            value_type: FunctionValueType::new(DataType::Int32, false),
            constant: None,
        },
    ];
    let uses = [Some(ExpressionUseId::new(1)), Some(ExpressionUseId::new(2))];
    let parameters = SemanticParameters::default();
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(u32::MAX),
        domain: EvaluationDomainId::new(1),
        demand: EvaluationDemand::Value,
    };
    let input = CallEffectInput {
        context,
        argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(&uses),
        function_id: owner.declaration.function_id(),
        kind: FunctionKind::Aggregate,
        selected: &owner.selected,
        request: FunctionBindingRequest {
            expected_result_type: None,
            arguments: &arguments,
            logical_argument_count: 1,
        },
        environment: &[],
        parameters: &parameters,
        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
        proof_scope: CallProofScope::Unconditional,
    };
    let token = catalogue
        .prepare_frozen(
            input,
            owner.selected.clone(),
            &owner.inner.frozen(&owner.selected),
            PureCallPreparation::Aggregate {
                arguments: ScopedExpressionEffects::pure_value(context),
                options: AggregatePreparationOptions {
                    state_interpretation: None,
                    phase: AggregateKernelPhase::Single,
                    distinct: false,
                    order_keys: Arc::from([AggregateOrderKey {
                        ascending: false,
                        nulls_first: false,
                    }]),
                    state_input_type: None,
                },
            },
            &OwnerControl,
        )
        .unwrap();
    let original = fixture::aggregate_node(
        &owner.inner,
        false,
        true,
        StaticAggregateOrder::default(),
        0,
    );
    let node = &original.nodes()[1];
    let mut kind = node.kind().clone();
    let ProgramNodeKind::Aggregate { functions, .. } = &mut kind else {
        unreachable!()
    };
    functions[0].inputs = vec![ProgramExprId::new(0), ProgramExprId::new(1)];
    functions[0].resolved = owner.signature();
    // The adapted call has an independently declared order channel. Publish
    // the actual ordered owner's complete selection, replacing the original
    // one-argument fixture owner together with the revised signature.
    functions[0].frozen = Some(Arc::new(
        crate::FrozenAggregateSelection::try_new(
            owner.declaration.function_id().clone(),
            owner.selected.clone(),
            1,
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        )
        .unwrap(),
    ));
    functions[0].order = StaticAggregateOrder {
        is_asc_order: vec![false],
        nulls_first: vec![false],
        ..Default::default()
    };
    let arena = Arc::new(
        ImmutableExpressions::try_new(
            vec![
                StaticExprNode::new(
                    StaticExprKind::Literal(StaticLiteral::Int64(7)),
                    DataType::Int64,
                    None,
                ),
                StaticExprNode::new(
                    StaticExprKind::Literal(StaticLiteral::Int32(9)),
                    DataType::Int32,
                    None,
                ),
            ],
            false,
            HashMap::new(),
            None,
        )
        .unwrap(),
    );
    let program = LocalProgramGraph::try_new(
        vec![
            original.nodes()[0].clone(),
            ProgramNode::new(
                node.legacy_native_node_id().expect("legacy fixture node"),
                kind,
                node.output_layout().clone(),
            ),
        ],
        original.root(),
        arena,
        original.profile(),
        original.requirements().clone(),
    )
    .unwrap();
    let calls = ProgramResolvedCalls::try_new(
        fixture::snapshot(program),
        vec![(fixture::aggregate_site(), token)],
        &Control::default(),
    )
    .unwrap();
    let types = |nullable| {
        BTreeMap::from([(
            ProgramExpressionArena::Main,
            vec![
                FunctionArgumentType::Value(i64_type(false)),
                FunctionArgumentType::Value(FunctionValueType::new(DataType::Int32, nullable)),
            ],
        )])
    };
    let entries = vec![(output(0), i64_type(false)), (output(1), i64_type(false))];
    let typed = ProgramTypedChannels::try_new(
        ProgramTypedExpressions::try_new(calls.clone(), types(false), &Control::default()).unwrap(),
        entries.clone(),
        &Control::default(),
    )
    .unwrap();
    let ProgramStateTemplate::Aggregate { kernel, .. } =
        typed.expressions().resolved_calls().calls()[&fixture::aggregate_site()].state_template()
    else {
        panic!("actual ordered kernel")
    };
    assert_eq!(kernel.contract().call().logical_argument_count(), 1);
    assert_eq!(
        kernel
            .contract()
            .order_argument_types()
            .next()
            .unwrap()
            .data_type(),
        &DataType::Int32
    );
    #[repr(C, align(64))]
    struct Storage([std::mem::MaybeUninit<u8>; 128]);
    let mut storage = Storage([std::mem::MaybeUninit::uninit(); 128]);
    let mut states = [kernel
        .initialize_in(&mut storage.0, &RuntimeControl)
        .unwrap()];
    let logical: arrow_array::ArrayRef = Arc::new(arrow_array::Int64Array::from(vec![3, 999, 5]));
    let order: arrow_array::ArrayRef = Arc::new(arrow_array::Int32Array::from(vec![30, 20, 10]));
    let logical_args = [EvaluatedArgument::Column(&logical)];
    let order_args = [EvaluatedArgument::Column(&order)];
    let rows = [0, 2];
    let checked = SelectedAggregateUpdateInput::try_new(
        kernel.contract(),
        Selection::try_sparse(3, &rows).unwrap(),
        &logical_args,
        &order_args,
        &RuntimeControl,
    )
    .unwrap();
    kernel
        .prepare_update_batch(&mut states, &[0, 0], checked, &RuntimeControl)
        .unwrap()
        .run(&RuntimeControl)
        .unwrap();
    let result = kernel.emit(&states, &[0], 1, &RuntimeControl).unwrap();
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0),
        8
    );
    assert_eq!(
        ProgramTypedChannels::try_new(
            ProgramTypedExpressions::try_new(calls, types(true), &Control::default()).unwrap(),
            entries,
            &Control::default()
        )
        .unwrap_err(),
        ProgramChannelTypeError::TypeMismatch
    );
}

// The supplied owner is lifetime-only. Its observer records actual public
// requests and locked opaque operation bounds, without manufacturing a grant.
struct CreationHold(Arc<std::sync::atomic::AtomicUsize>);
impl novarocks_type_contract::ValueTypeStorageHold for CreationHold {}
impl Drop for CreationHold {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CreationEvent {
    Layout(usize, usize),
    Opaque(usize),
}
#[derive(Debug, Eq, PartialEq)]
struct CallerRefusal {
    operation: usize,
    original: u64,
}
fn creation_storage(drops: &Arc<std::sync::atomic::AtomicUsize>) -> ValueTypeStorageLifetime {
    ValueTypeStorageLifetime::held(Arc::new(CreationHold(drops.clone())))
}

#[test]
fn created_channel_maps_and_independent_type_clones_retain_only_their_actual_owners() {
    use novarocks_type_contract::{ValueKind, ValueLayout, ValueTypeRef};
    let original_drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let new_drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let source_storage = creation_storage(&original_drops);
    let ty = FunctionValueType::try_from_value(
        ValueTypeRef::try_new(
            ValueKind::Scalar(DataType::Int64),
            false,
            source_storage.clone(),
        )
        .unwrap(),
        ValueLayout::try_new(&DataType::Int64, source_storage.clone()).unwrap(),
    )
    .unwrap();
    let source = writer();
    let channels = writer_channels()
        .into_iter()
        .map(|(site, _)| (site, ty.clone()))
        .collect::<Vec<_>>();
    let expected =
        ProgramTypedChannels::try_new(source.clone(), channels.clone(), &Control::default())
            .unwrap();
    let mut request = |_: std::alloc::Layout| -> Result<(), CallerRefusal> { Ok(()) };
    let mut opaque = |_: usize| -> Result<(), CallerRefusal> { Ok(()) };
    let mut creation =
        ValueSchemaCreation::new(creation_storage(&new_drops), &mut request, &mut opaque);
    let checked = ProgramTypedChannels::try_new_with_creator_in(
        source,
        channels,
        &mut creation,
        &Control::default(),
    )
    .unwrap();
    assert_eq!(checked.channels(), expected.channels());
    let independent_type = checked.channel_type(output(0)).unwrap().clone();
    let late_map = checked.slots.clone();
    drop(checked);
    drop(creation);
    drop(expected);
    drop(ty);
    drop(source_storage);
    assert_eq!(new_drops.load(std::sync::atomic::Ordering::SeqCst), 0);
    drop(late_map);
    assert_eq!(new_drops.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(original_drops.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(independent_type, i64_type(false));
    drop(independent_type);
    assert_eq!(original_drops.load(std::sync::atomic::Ordering::SeqCst), 1);
}
#[test]
fn every_channel_quote_preserves_first_caller_payload_and_original_type_failure() {
    let source = writer();
    let channels = writer_channels();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let run = |fail_at: Option<usize>| {
        seen.lock().unwrap().clear();
        let events = seen.clone();
        let mut request = move |layout: std::alloc::Layout| {
            let mut events = events.lock().unwrap();
            let operation = events.len();
            events.push(CreationEvent::Layout(layout.size(), layout.align()));
            if fail_at == Some(operation) {
                Err(CallerRefusal {
                    operation,
                    original: 99,
                })
            } else {
                Ok(())
            }
        };
        let events = seen.clone();
        let mut opaque = move |bytes| {
            let mut events = events.lock().unwrap();
            let operation = events.len();
            events.push(CreationEvent::Opaque(bytes));
            if fail_at == Some(operation) {
                Err(CallerRefusal {
                    operation,
                    original: 99,
                })
            } else {
                Ok(())
            }
        };
        let mut creation =
            ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut request, &mut opaque);
        ProgramTypedChannels::try_new_with_creator_in(
            source.clone(),
            channels.clone(),
            &mut creation,
            &Control::default(),
        )
    };
    drop(run(None).unwrap());
    let original = seen.lock().unwrap().clone();
    assert_eq!(
        original
            .iter()
            .filter(|e| matches!(e, CreationEvent::Layout(..)))
            .count(),
        2
    );
    for operation in 0..original.len() {
        assert!(
            matches!(run(Some(operation)),Err(ProgramCreationFailure::Creator(CallerRefusal{operation:at,original:99})) if at==operation)
        );
        assert_eq!(*seen.lock().unwrap(), original[..=operation]);
    }
    let mut wrong = channels;
    wrong[0].1 = i64_type(true);
    let mut request = |_| -> Result<(), CallerRefusal> { Ok(()) };
    let mut opaque = |_| -> Result<(), CallerRefusal> { Ok(()) };
    let mut creation =
        ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut request, &mut opaque);
    assert!(matches!(
        ProgramTypedChannels::try_new_with_creator_in(
            source,
            wrong,
            &mut creation,
            &Control::default()
        ),
        Err(ProgramCreationFailure::Owner(
            ProgramChannelTypeError::TypeMismatch
        ))
    ));
}

#[test]
fn canonical_map64_carrier_consistency_keeps_type_and_control_refusals() {
    let map = |mut data_type: DataType| {
        for _ in 1..64 {
            data_type = DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new("key", DataType::Int32, false)),
                            Arc::new(Field::new("value", data_type, false)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            );
        }
        data_type
    };
    let left = map(DataType::Utf8);
    let right = map(DataType::LargeUtf8);
    let good = Control::default();
    let mut work = CompileCheckpoints::try_new(&good, CompilePhase::LowerProgram).unwrap();
    same_carrier(&left, &left, &mut work).unwrap();
    assert_eq!(
        same_carrier(&left, &right, &mut work),
        Err(ProgramChannelTypeError::TypeMismatch)
    );
    work.finish().unwrap();
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let control = Control {
            failure: Some((256, cause.clone())),
            ..Control::default()
        };
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
        assert_eq!(
            same_carrier(&left, &left, &mut work),
            Err(ProgramChannelTypeError::Control(cause))
        );
    }
}
