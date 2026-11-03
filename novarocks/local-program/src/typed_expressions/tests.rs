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
use crate::{
    BindingRequirement, BindingRequirements, CompileProfile, ControlShape, ImmutableExpressions,
    KernelAbiVersion, LocalProgramGraph, ProgramControlFlow, ProgramEvaluationDomain,
    ProgramExpressionRootSite, ProgramExpressionUse, ProgramNode, ProgramNodeExpressionRole,
    ProgramNodeId, ProgramNodeKind, ProgramRootControlBindings, ProgramRootUseBinding,
    StaticExprNode, StaticLayout, StaticLiteral, StaticSinkProgram, StaticStreamBranch,
    StaticValues,
};
use arrow_array::{BooleanArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use novarocks_execution_contract::DataStreamPartitionType;
use novarocks_type_contract::{EvaluationDomainId, ValueLogicalType};
use novarocks_types::SlotId;
use std::{collections::HashMap, num::NonZeroUsize};

#[derive(Default)]
struct Control {
    at: Option<u32>,
    failure: Option<CompileControlError>,
}

fn shared_truth_value_fixture(ty: DataType) -> ProgramResolvedCalls {
    let nodes = arena(vec![StaticExprNode::new(
        StaticExprKind::Literal(StaticLiteral::Null),
        ty.clone(),
        None,
    )]);
    let source_schema = Arc::new(Schema::new(vec![Field::new(
        "source",
        DataType::Boolean,
        false,
    )]));
    let source_layout =
        StaticLayout::try_new(source_schema.clone(), Arc::from([SlotId::new(1)])).unwrap();
    let values = StaticValues::try_new(
        RecordBatch::try_new(
            source_schema,
            vec![Arc::new(BooleanArray::from(vec![true]))],
        )
        .unwrap(),
        source_layout.clone(),
    )
    .unwrap();
    let output = StaticLayout::try_new(
        Arc::new(Schema::new(vec![Field::new("result", ty, true)])),
        Arc::from([SlotId::new(2)]),
    )
    .unwrap();
    let program = LocalProgramGraph::try_new(
        vec![
            ProgramNode::new(0, ProgramNodeKind::Values { values }, source_layout.clone()),
            ProgramNode::new(
                1,
                ProgramNodeKind::Filter {
                    input: ProgramNodeId::new(0),
                    predicates: vec![ProgramExprId::new(0)].into_boxed_slice(),
                },
                source_layout,
            ),
            ProgramNode::new(
                2,
                ProgramNodeKind::Project {
                    input: ProgramNodeId::new(1),
                    is_subordinate: false,
                    validate_final_result_input: false,
                    exprs: vec![ProgramExprId::new(0)],
                    expr_slot_ids: vec![SlotId::new(2)],
                    expr_slot_schemas: None,
                    output_indices: None,
                },
                output.clone(),
            ),
        ],
        ProgramNodeId::new(2),
        nodes,
        CompileProfile::new(
            NonZeroUsize::new(1).unwrap(),
            None,
            output.identity().unwrap(),
            KernelAbiVersion::CURRENT,
        ),
        BindingRequirements::try_new(vec![]).unwrap(),
    )
    .unwrap();
    let flow = ProgramControlFlow::try_new(
        vec![ProgramEvaluationDomain {
            id: EvaluationDomainId::new(0),
            parent: None,
            guard: None,
        }],
        [EvaluationDemand::TruthOnly, EvaluationDemand::Value]
            .into_iter()
            .enumerate()
            .map(|(index, demand)| ProgramExpressionUse {
                context: novarocks_type_contract::ExpressionEffectContext {
                    use_id: novarocks_type_contract::ExpressionUseId::new(index as u32),
                    domain: EvaluationDomainId::new(0),
                    demand,
                },
                definition: ProgramExprId::new(0),
                control: ControlShape::Eager,
                arguments: Box::default(),
            })
            .collect(),
        1,
        &Control::default(),
    )
    .unwrap();
    let bindings = [
        ProgramNodeExpressionRole::FilterPredicate { predicate: 0 },
        ProgramNodeExpressionRole::ProjectOutput { expression: 0 },
    ]
    .into_iter()
    .enumerate()
    .map(|(index, role)| ProgramRootUseBinding {
        site: ProgramExpressionRootSite::Node {
            node: ProgramNodeId::new(index + 1),
            role,
        },
        use_id: novarocks_type_contract::ExpressionUseId::new(index as u32),
    })
    .collect();
    let snapshot = ProgramRootControlBindings::try_new(
        program,
        BTreeMap::from([(ProgramExpressionArena::Main, flow)]),
        bindings,
        &Control::default(),
    )
    .unwrap();
    ProgramResolvedCalls::try_new(snapshot, vec![], &Control::default()).unwrap()
}

#[test]
fn actual_truth_only_uses_require_boolean_without_erasing_shared_value_nullability() {
    let typed = ProgramTypedExpressions::try_new(
        shared_truth_value_fixture(DataType::Boolean),
        main_types(vec![value(DataType::Boolean, true)]),
        &Control::default(),
    )
    .unwrap();
    assert_eq!(
        typed.definition_type(ProgramExpressionArena::Main, ProgramExprId::new(0)),
        Some(&value(DataType::Boolean, true))
    );
    let uses = typed.resolved_calls().snapshot().flows()[&ProgramExpressionArena::Main].uses();
    assert_eq!(
        uses[&novarocks_type_contract::ExpressionUseId::new(0)]
            .context
            .demand,
        EvaluationDemand::TruthOnly
    );
    assert_eq!(
        uses[&novarocks_type_contract::ExpressionUseId::new(1)]
            .context
            .demand,
        EvaluationDemand::Value
    );
    for ty in [DataType::Utf8, DataType::Int64, DataType::Null] {
        assert_eq!(
            ProgramTypedExpressions::try_new(
                shared_truth_value_fixture(ty.clone()),
                main_types(vec![value(ty, true)]),
                &Control::default()
            )
            .unwrap_err(),
            ProgramExpressionTypeError::WrongDemand
        );
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::LowerProgram);
        assert!(units <= 256);
        if self.at == Some(units) {
            Err(self.failure.clone().unwrap())
        } else {
            Ok(())
        }
    }
}
fn arena(nodes: Vec<StaticExprNode>) -> Arc<ImmutableExpressions> {
    Arc::new(ImmutableExpressions::try_new(nodes, false, HashMap::new(), None).unwrap())
}
fn bool_node() -> StaticExprNode {
    StaticExprNode::new(
        StaticExprKind::Literal(StaticLiteral::Bool(true)),
        DataType::Boolean,
        None,
    )
}
fn value(ty: DataType, nullable: bool) -> FunctionArgumentType {
    FunctionArgumentType::Value(FunctionValueType::new(ty, nullable))
}
fn empty_flow(count: usize) -> ProgramControlFlow {
    ProgramControlFlow::try_new(
        vec![ProgramEvaluationDomain {
            id: EvaluationDomainId::new(u32::MAX),
            parent: None,
            guard: None,
        }],
        vec![],
        count,
        &Control::default(),
    )
    .unwrap()
}
fn resolved(
    nodes: Vec<StaticExprNode>,
    sink_nodes: Option<Vec<StaticExprNode>>,
) -> ProgramResolvedCalls {
    let main = arena(nodes);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "source",
        DataType::Boolean,
        false,
    )]));
    let layout = StaticLayout::try_new(schema.clone(), Arc::from([SlotId::new(1)])).unwrap();
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(BooleanArray::from(vec![true]))]).unwrap();
    let values = StaticValues::try_new(batch, layout.clone()).unwrap();
    let profile = CompileProfile::new(
        NonZeroUsize::new(1).unwrap(),
        None,
        layout.identity().unwrap(),
        KernelAbiVersion::CURRENT,
    );
    let mut flows =
        BTreeMap::from([(ProgramExpressionArena::Main, empty_flow(main.nodes().len()))]);
    let (requirements, sink) = match sink_nodes {
        Some(nodes) => {
            let sink_arena = arena(nodes);
            flows.insert(
                ProgramExpressionArena::Sink,
                empty_flow(sink_arena.nodes().len()),
            );
            let branch = StaticStreamBranch::try_new(
                10,
                DataStreamPartitionType::Unpartitioned,
                vec![],
                vec![],
                None,
                crate::StaticStreamContract {
                    role: crate::ProgramOutputRole::Relational,
                    delivery:
                        novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
                    routing: crate::StaticStreamRouting::Ordinary,
                },
            )
            .unwrap();
            (
                vec![BindingRequirement::ExchangeOutput {
                    role: crate::ProgramOutputRole::Relational,
                    delivery:
                        novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
                    branch: 0,
                    layout: layout.clone(),
                }],
                Some(StaticSinkProgram::try_data_stream(branch, sink_arena).unwrap()),
            )
        }
        None => (vec![], None),
    };
    let program = LocalProgramGraph::try_new_with_sink(
        vec![ProgramNode::new(
            0,
            ProgramNodeKind::Values { values },
            layout,
        )],
        ProgramNodeId::new(0),
        main,
        profile,
        BindingRequirements::try_new(requirements).unwrap(),
        sink,
    )
    .unwrap();
    let snapshot =
        ProgramRootControlBindings::try_new(program, flows, vec![], &Control::default()).unwrap();
    ProgramResolvedCalls::try_new(snapshot, vec![], &Control::default()).unwrap()
}
fn main_types(
    types: Vec<FunctionArgumentType>,
) -> BTreeMap<ProgramExpressionArena, Vec<FunctionArgumentType>> {
    BTreeMap::from([(ProgramExpressionArena::Main, types)])
}

#[test]
fn unused_definitions_still_have_explicit_full_logical_and_nullable_types() {
    let source = resolved(
        vec![StaticExprNode::new(
            StaticExprKind::Literal(StaticLiteral::Utf8("null".into())),
            DataType::Utf8,
            None,
        )],
        None,
    );
    let json = FunctionArgumentType::Value(
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap(),
    );
    let typed = ProgramTypedExpressions::try_new(
        source,
        main_types(vec![json.clone()]),
        &Control::default(),
    )
    .unwrap();
    assert_eq!(
        typed.definition_type(ProgramExpressionArena::Main, ProgramExprId::new(0)),
        Some(&json)
    );
    assert!(
        typed
            .definition_type(ProgramExpressionArena::Sink, ProgramExprId::new(0))
            .is_none()
    );
    assert!(
        typed
            .definition_type(ProgramExpressionArena::Main, ProgramExprId::new(1))
            .is_none()
    );
    assert!(typed.resolved_calls().calls().is_empty());
}

#[test]
fn missing_extra_arena_and_definition_positions_are_not_optional() {
    let source = resolved(vec![bool_node()], None);
    for entries in [
        BTreeMap::new(),
        main_types(vec![]),
        main_types(vec![value(DataType::Boolean, true); 2]),
        BTreeMap::from([(
            ProgramExpressionArena::Sink,
            vec![value(DataType::Boolean, true)],
        )]),
    ] {
        assert_eq!(
            ProgramTypedExpressions::try_new(source.clone(), entries, &Control::default())
                .unwrap_err(),
            ProgramExpressionTypeError::IncompleteCoverage
        );
    }
}

#[test]
fn lambda_parameter_shape_and_body_type_remain_explicit() {
    let source = resolved(
        vec![
            bool_node(),
            StaticExprNode::new(
                StaticExprKind::LambdaFunction {
                    body: ProgramExprId::new(0),
                    arg_slots: vec![SlotId::new(7)],
                    common_sub_exprs: vec![],
                    is_nondeterministic: false,
                },
                DataType::Boolean,
                None,
            ),
        ],
        None,
    );
    let lambda = FunctionArgumentType::Lambda {
        parameter_types: novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
            vec![FunctionValueType::new(DataType::Int64, false)].into_boxed_slice(),
        ),
        result_type: FunctionValueType::new(DataType::Boolean, true),
    };
    ProgramTypedExpressions::try_new(
        source.clone(),
        main_types(vec![value(DataType::Boolean, true), lambda.clone()]),
        &Control::default(),
    )
    .unwrap();
    assert_eq!(
        ProgramTypedExpressions::try_new(
            source.clone(),
            main_types(vec![value(DataType::Boolean, true); 2]),
            &Control::default()
        )
        .unwrap_err(),
        ProgramExpressionTypeError::WrongKind
    );
    let FunctionArgumentType::Lambda { result_type, .. } = lambda.clone() else {
        unreachable!()
    };
    let wrong = FunctionArgumentType::Lambda {
        parameter_types: novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
            Box::default(),
        ),
        result_type,
    };
    assert_eq!(
        ProgramTypedExpressions::try_new(
            source.clone(),
            main_types(vec![value(DataType::Boolean, true), wrong]),
            &Control::default()
        )
        .unwrap_err(),
        ProgramExpressionTypeError::WrongLambda
    );
    assert_eq!(
        ProgramTypedExpressions::try_new(
            source,
            main_types(vec![value(DataType::Boolean, false), lambda]),
            &Control::default()
        )
        .unwrap_err(),
        ProgramExpressionTypeError::TypeMismatch
    );
}

#[test]
fn source_dictionary_ids_do_not_change_static_value_binding_and_invalid_nominals_fail_at_author() {
    #[allow(deprecated)]
    let field = |id| {
        Arc::new(Field::new_dict(
            "dictionary",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            false,
            id,
            true,
        ))
    };
    let left = DataType::Struct(vec![field(1)].into());
    let right = DataType::Struct(vec![field(2)].into());
    assert_eq!(left, right); // Arrow logical equality is deliberately coarser.
    let source = resolved(
        vec![StaticExprNode::new(
            StaticExprKind::Literal(StaticLiteral::Null),
            FunctionValueType::new(left.clone(), true)
                .data_type()
                .clone(),
            None,
        )],
        None,
    );
    ProgramTypedExpressions::try_new(
        source.clone(),
        main_types(vec![value(left, true)]),
        &Control::default(),
    )
    .unwrap();
    ProgramTypedExpressions::try_new(
        source,
        main_types(vec![value(right, true)]),
        &Control::default(),
    )
    .unwrap();
    assert!(
        FunctionValueType::try_with_logical_type(DataType::Boolean, true, ValueLogicalType::Json)
            .is_err()
    );
}

#[test]
fn combined_definition_budget_counts_distinct_arenas_even_for_unused_constants() {
    for main_count in [
        MAX_STATIC_EXPRESSIONS - 2,
        MAX_STATIC_EXPRESSIONS - 1,
        MAX_STATIC_EXPRESSIONS,
    ] {
        let source = resolved(vec![bool_node(); main_count], Some(vec![bool_node()]));
        let types = BTreeMap::from([
            (
                ProgramExpressionArena::Main,
                vec![value(DataType::Boolean, false); main_count],
            ),
            (
                ProgramExpressionArena::Sink,
                vec![value(DataType::Boolean, false)],
            ),
        ]);
        let result = ProgramTypedExpressions::try_new(source, types, &Control::default());
        if main_count == MAX_STATIC_EXPRESSIONS {
            assert_eq!(
                result.unwrap_err(),
                ProgramExpressionTypeError::TooManyDefinitions
            );
        } else {
            result.unwrap();
        }
    }
}

#[test]
fn signature_type_work_uses_one_scope_and_preserves_all_three_control_failures() {
    let source = resolved(vec![bool_node(); 300], None);
    for failure in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in [0, 256] {
            assert_eq!(
                ProgramTypedExpressions::try_new(
                    source.clone(),
                    main_types(vec![value(DataType::Boolean, false); 300]),
                    &Control {
                        at: Some(at),
                        failure: Some(failure.clone())
                    }
                )
                .unwrap_err(),
                ProgramExpressionTypeError::Control(failure.clone())
            );
        }
    }
}

fn cv_policy() -> novarocks_functions::ConstantPolicy {
    novarocks_functions::ConstantPolicy {
        max_rows: 1_000_000,
        max_array_nodes: 4096,
        max_logical_elements: 16_000_000,
        max_retained_buffer_bytes: 64 * 1024 * 1024,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 64,
        max_metadata_bytes: 1024 * 1024,
        max_library_validation_work: 1024 * 1024 * 1024,
        max_library_validation_bytes: 64 * 1024 * 1024,
    }
}
fn cv(
    array: arrow_array::ArrayRef,
    ty: FunctionValueType,
    ordinal: u32,
) -> novarocks_functions::ConstantValue {
    novarocks_functions::ConstantPool::try_new(
        Arc::new(Field::new(
            "selected",
            array.data_type().clone(),
            ty.nullable(),
        )),
        ty,
        array.to_data(),
        cv_policy(),
        CompilePhase::LowerProgram,
        &Control::default(),
    )
    .unwrap()
    .value(ordinal)
    .unwrap()
}
fn cv_node(value: novarocks_functions::ConstantValue) -> StaticExprNode {
    let ty = value.value_type().data_type().clone();
    StaticExprNode::new(StaticExprKind::Constant(value), ty, None)
}

#[test]
fn shared_constants_keep_selected_rows_float_bits_and_exact_complete_types() {
    use arrow_array::{Array, Float32Array, StringArray, StructArray};
    let floats = [0x8000_0000u32, 0x7fc0_0042];
    let ty = FunctionValueType::new(DataType::Float32, false);
    let source = cv(
        Arc::new(Float32Array::from(floats.map(f32::from_bits).to_vec())),
        ty.clone(),
        1,
    );
    let typed = ProgramTypedExpressions::try_new(
        resolved(vec![cv_node(source.clone())], None),
        main_types(vec![FunctionArgumentType::Value(ty)]),
        &Control::default(),
    )
    .unwrap();
    let arena = &typed.resolved_calls().snapshot().roots().arenas()[&ProgramExpressionArena::Main];
    let StaticExprKind::Constant(stored) = arena.nodes()[0].kind() else {
        panic!("missing checked constant")
    };
    assert_eq!(stored.ordinal(), 1);
    assert!(Arc::ptr_eq(stored.pool().array(), source.pool().array()));
    assert_eq!(
        stored
            .pool()
            .array()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(stored.ordinal() as usize)
            .to_bits(),
        floats[1]
    );

    let child = Arc::new(
        Field::new("json", DataType::Utf8, true).with_metadata(HashMap::from([
            (
                novarocks_type_contract::NR_LOGICAL_TYPE_KEY.to_owned(),
                "json".to_owned(),
            ),
            ("provider".to_owned(), "source".to_owned()),
        ])),
    );
    let array = Arc::new(StructArray::new(
        vec![child.clone()].into(),
        vec![Arc::new(StringArray::from(vec![Some("null"), Some("{}")]))],
        None,
    ));
    let exact = crate::test_value_types::string_structure_from_source(
        &[("json", true, true)],
        false,
        array.data_type(),
    );
    let nested = cv(array, exact.clone(), 1);
    ProgramTypedExpressions::try_new(
        resolved(vec![cv_node(nested.clone())], None),
        main_types(vec![FunctionArgumentType::Value(exact)]),
        &Control::default(),
    )
    .unwrap();
    let changed_source = DataType::Struct(
        vec![Arc::new(child.as_ref().clone().with_metadata(
            HashMap::from([
                (
                    novarocks_type_contract::NR_LOGICAL_TYPE_KEY.to_owned(),
                    "json".to_owned(),
                ),
                ("provider".to_owned(), "different".to_owned()),
            ]),
        ))]
        .into(),
    );
    let source_attributes_only = crate::test_value_types::string_structure_from_source(
        &[("json", true, true)],
        false,
        &changed_source,
    );
    ProgramTypedExpressions::try_new(
        resolved(vec![cv_node(nested.clone())], None),
        main_types(vec![FunctionArgumentType::Value(source_attributes_only)]),
        &Control::default(),
    )
    .unwrap();
    let wrong = crate::test_value_types::string_structure(&[("json", true, false)], false);
    assert_eq!(
        ProgramTypedExpressions::try_new(
            resolved(vec![cv_node(nested)], None),
            main_types(vec![FunctionArgumentType::Value(wrong)]),
            &Control::default(),
        )
        .unwrap_err(),
        ProgramExpressionTypeError::TypeMismatch
    );
}

#[test]
fn shared_constants_reject_logical_nullable_and_carrier_retagging() {
    use arrow_array::{Float32Array, StringArray};
    let json_type =
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap();
    let json = cv(
        Arc::new(StringArray::from(vec![None::<&str>, Some("null")])),
        json_type.clone(),
        0,
    );
    assert!(
        json.is_null_observed(CompilePhase::LowerProgram, &Control::default())
            .unwrap()
    );
    ProgramTypedExpressions::try_new(
        resolved(vec![cv_node(json.clone())], None),
        main_types(vec![FunctionArgumentType::Value(json_type.clone())]),
        &Control::default(),
    )
    .unwrap();
    for wrong in [
        FunctionValueType::new(DataType::Utf8, true),
        FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
            .unwrap(),
    ] {
        assert_eq!(
            ProgramTypedExpressions::try_new(
                resolved(vec![cv_node(json.clone())], None),
                main_types(vec![FunctionArgumentType::Value(wrong)]),
                &Control::default(),
            )
            .unwrap_err(),
            ProgramExpressionTypeError::TypeMismatch
        );
    }
    let float = cv(
        Arc::new(Float32Array::from(vec![1.0])),
        FunctionValueType::new(DataType::Float32, false),
        0,
    );
    assert_eq!(
        ImmutableExpressions::try_new(
            vec![StaticExprNode::new(
                StaticExprKind::Constant(float),
                DataType::Float64,
                None
            )],
            false,
            HashMap::new(),
            None,
        )
        .unwrap_err(),
        crate::StaticExpressionError::ConstantTypeMismatch
    );
}

#[test]
fn shared_constant_backing_is_bounded_as_retained_storage_and_deduplicated() {
    use arrow_array::BinaryArray;
    let payload = vec![7u8; 9 * 1024 * 1024];
    let make = || {
        cv(
            Arc::new(BinaryArray::from(vec![payload.as_slice()])),
            FunctionValueType::new(DataType::Binary, false),
            0,
        )
    };
    let same = make();
    arena(vec![
        cv_node(same.clone()),
        cv_node(same.clone()),
        cv_node(same),
    ]);
    assert_eq!(
        ImmutableExpressions::try_new(
            vec![cv_node(make()), cv_node(make())],
            false,
            HashMap::new(),
            None,
        )
        .unwrap_err(),
        crate::StaticExpressionError::TooManyBytes
    );
}

#[test]
fn shared_constant_definition_control_never_becomes_a_partial_typed_program() {
    let constant = cv(
        Arc::new(BooleanArray::from(vec![true])),
        FunctionValueType::new(DataType::Boolean, false),
        0,
    );
    let source = resolved(vec![cv_node(constant); 320], None);
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in [0, 256] {
            assert_eq!(
                ProgramTypedExpressions::try_new(
                    source.clone(),
                    main_types(vec![value(DataType::Boolean, false); 320]),
                    &Control {
                        at: Some(at),
                        failure: Some(error.clone())
                    },
                )
                .unwrap_err(),
                ProgramExpressionTypeError::Control(error.clone())
            );
        }
    }
}

#[test]
fn created_type_table_late_slice_retains_only_its_original_arena_owner() {
    struct Hold;
    impl novarocks_type_contract::ValueTypeStorageHold for Hold {}
    let temporary_hold = Arc::new(Hold);
    let table_hold = Arc::new(Hold);
    let arena_hold = Arc::new(Hold);
    let temporary_weak = Arc::downgrade(&temporary_hold);
    let table_weak = Arc::downgrade(&table_hold);
    let arena_weak = Arc::downgrade(&arena_hold);
    let mut request = |_: std::alloc::Layout| Ok::<_, ()>(());
    let mut opaque = |_: usize| Ok::<_, ()>(());
    let mut retained_request = |_: std::alloc::Layout| Ok::<_, ()>(());
    let mut retained_opaque = |_: usize| Ok::<_, ()>(());
    let mut temporary = ValueSchemaCreation::new(
        ValueTypeStorageLifetime::held(temporary_hold.clone()),
        &mut request,
        &mut opaque,
    );
    let mut retained = ValueSchemaCreation::new(
        ValueTypeStorageLifetime::held(table_hold.clone()),
        &mut retained_request,
        &mut retained_opaque,
    );
    // The real producer owns this collection from its first Vec allocation;
    // the downstream constructor never borrows the table's scope for it.
    let source_value = value(DataType::Boolean, true);
    let source_requests = std::sync::atomic::AtomicUsize::new(0);
    let mut source_request = |_: std::alloc::Layout| {
        source_requests.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok::<_, novarocks_type_contract::owned_resources::layout::LayoutResourceError>(())
    };
    source_request(std::alloc::Layout::array::<FunctionArgumentType>(1).unwrap()).unwrap();
    let entries = vec![source_value];
    let entries = FunctionArgumentTypes::try_from_boxed_observed(
        entries.into_boxed_slice(),
        ValueTypeStorageLifetime::held(arena_hold.clone()),
        &mut source_request,
    )
    .unwrap();
    assert_eq!(
        source_requests.load(std::sync::atomic::Ordering::Relaxed),
        2
    );
    let typed = ProgramTypedExpressions::try_new_with_creator_in(
        shared_truth_value_fixture(DataType::Boolean),
        BTreeMap::from([(ProgramExpressionArena::Main, entries)]),
        &mut temporary,
        &mut retained,
        &Control::default(),
    )
    .unwrap();
    let late = typed.types()[&ProgramExpressionArena::Main].clone();
    assert_eq!(late.as_slice(), &[value(DataType::Boolean, true)]);
    drop(typed);
    drop(temporary);
    drop(retained);
    drop(temporary_hold);
    drop(table_hold);
    drop(arena_hold);
    assert!(temporary_weak.upgrade().is_none());
    assert!(table_weak.upgrade().is_none());
    assert!(arena_weak.upgrade().is_some());
    drop(late);
    assert!(arena_weak.upgrade().is_none());
}

#[test]
fn created_type_table_refuses_each_actual_final_request_without_control_tail() {
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    struct Trace(Mutex<Vec<(CompilePhase, u32)>>);
    impl PureCompileControl for Trace {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            self.0.lock().unwrap().push((phase, units));
            Ok(())
        }
    }
    let run = |fail_at: Option<usize>| {
        let calls = shared_truth_value_fixture(DataType::Boolean);
        let control = Trace(Mutex::new(Vec::new()));
        let next = AtomicUsize::new(0);
        let refused = AtomicUsize::new(usize::MAX);
        let cause = Arc::new(());
        let observe = || {
            let at = next.fetch_add(1, Ordering::Relaxed);
            if Some(at) == fail_at {
                refused.store(control.0.lock().unwrap().len(), Ordering::Relaxed);
                Err(cause.clone())
            } else {
                Ok(())
            }
        };
        let mut request = |_: std::alloc::Layout| observe();
        let mut opaque = |_: usize| observe();
        let mut temporary_request = |_: std::alloc::Layout| Ok::<_, Arc<()>>(());
        let mut temporary_opaque = |_: usize| Ok::<_, Arc<()>>(());
        let mut temporary = ValueSchemaCreation::new(
            ValueTypeStorageLifetime::pure(),
            &mut temporary_request,
            &mut temporary_opaque,
        );
        let mut retained =
            ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut request, &mut opaque);
        let result = ProgramTypedExpressions::try_new_with_creator_in(
            calls,
            BTreeMap::from([(
                ProgramExpressionArena::Main,
                FunctionArgumentTypes::from_pure_boxed(
                    vec![value(DataType::Boolean, true)].into_boxed_slice(),
                ),
            )]),
            &mut temporary,
            &mut retained,
            &control,
        );
        if let Some(at) = fail_at {
            assert!(
                matches!(result, Err(ProgramCreationFailure::Creator(actual))
                if Arc::ptr_eq(&actual, &cause))
            );
            assert_eq!(next.load(Ordering::Relaxed), at + 1);
            assert_eq!(
                control.0.lock().unwrap().len(),
                refused.load(Ordering::Relaxed)
            );
        } else {
            assert!(result.is_ok());
        }
        next.load(Ordering::Relaxed)
    };
    let total = run(None);
    assert!(
        total >= 2,
        "actual final tree and table Arc must be observed"
    );
    for at in 0..total {
        run(Some(at));
    }
}

#[test]
fn pure_guarded_type_table_shares_the_original_arena_collection() {
    let entries = FunctionArgumentTypes::from_pure_boxed(
        vec![value(DataType::Boolean, true)].into_boxed_slice(),
    );
    let original = entries.clone();
    let typed = ProgramTypedExpressions::try_new_guarded(
        shared_truth_value_fixture(DataType::Boolean),
        BTreeMap::from([(ProgramExpressionArena::Main, entries)]),
        &Control::default(),
    )
    .unwrap();
    let published = &typed.types()[&ProgramExpressionArena::Main];
    assert_eq!(published.as_slice(), original.as_slice());
    assert_eq!(published.as_slice().as_ptr(), original.as_slice().as_ptr());
    assert_eq!(
        typed.definition_type(ProgramExpressionArena::Main, ProgramExprId::new(0)),
        Some(&value(DataType::Boolean, true))
    );
}

#[test]
fn complete_map64_definition_uses_canonical_depth_and_keeps_layout_mismatch() {
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
    let expected = map(DataType::Utf8);
    let source = || {
        resolved(
            vec![StaticExprNode::new(
                StaticExprKind::Literal(StaticLiteral::Null),
                expected.clone(),
                None,
            )],
            None,
        )
    };
    ProgramTypedExpressions::try_new(
        source(),
        main_types(vec![value(expected.clone(), true)]),
        &Control::default(),
    )
    .unwrap();
    assert_eq!(
        ProgramTypedExpressions::try_new(
            source(),
            main_types(vec![value(map(DataType::LargeUtf8), true)]),
            &Control::default()
        )
        .unwrap_err(),
        ProgramExpressionTypeError::TypeMismatch
    );
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let control = Control {
            at: Some(256),
            failure: Some(cause.clone()),
        };
        assert_eq!(
            ProgramTypedExpressions::try_new(
                source(),
                main_types(vec![value(expected.clone(), true)]),
                &control
            )
            .unwrap_err(),
            ProgramExpressionTypeError::Control(cause)
        );
    }
}
