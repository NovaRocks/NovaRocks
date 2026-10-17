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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Real FE encoding and default Native lowering/evaluation, with the original
//! checked request and actual BE construction owner.
use super::*;
use arrow::array::{ArrayRef, Int16Array, Int32Array, StringArray};
use novarocks_functions::{
    FunctionArgument, FunctionBindingRequest, FunctionKind, FunctionResultType,
};
use novarocks_physical_plan::{
    BoundFunction, ConstantPool, ConstantPoolId, ConstantReference, LegacyBindingMetadata,
    PhysicalCallDefinition, PhysicalCallRequest, StaticFunctionArgument,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, PureCompileControl, ValueLogicalType,
};

fn policy() -> novarocks_functions::ConstantPolicy {
    novarocks_functions::ConstantPolicy {
        max_rows: 16,
        max_array_nodes: 128,
        max_logical_elements: 1024,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 64,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 1 << 20,
        max_library_validation_bytes: 1 << 20,
    }
}

fn encoded_constant_call(name: &str, argument: ValueType, array: ArrayRef) -> plan::PlanFragment {
    encoded_constant_call_with_coercion(name, argument, array, None)
}
fn encoded_constant_call_with_coercion(
    name: &str,
    argument: ValueType,
    array: ArrayRef,
    coerced: Option<ValueType>,
) -> plan::PlanFragment {
    encoded_constant_call_with_target(name, argument, array, coerced, None)
}
fn encoded_constant_call_with_target(
    name: &str,
    argument: ValueType,
    array: ArrayRef,
    coerced: Option<ValueType>,
    expected_target: Option<ValueType>,
) -> plan::PlanFragment {
    let catalog = novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap();
    let pool = ConstantPool::try_new(
        Arc::new(arrow::datatypes::Field::new(
            "original literal",
            argument.data_type().clone(),
            argument.nullable(),
        )),
        argument.clone(),
        array.to_data(),
        policy(),
        CompilePhase::Encode,
        compile_control(),
    )
    .unwrap();
    let reference = ConstantReference {
        pool: ConstantPoolId::new(37),
        ordinal: 0,
    };
    let original_value = pool.value(0).unwrap();
    let call_argument = coerced.as_ref().unwrap_or(&argument).clone();
    let original_constant = if coerced.is_some() {
        None
    } else {
        Some(reference)
    };
    let arguments = [FunctionArgument::Value {
        value_type: call_argument.clone(),
        constant: if coerced.is_some() {
            None
        } else {
            Some(original_value)
        },
    }];
    let request = FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count: 1,
        expected_result_type: expected_target.as_ref(),
    };
    let resolved = if expected_target.is_some() {
        // The original SQL converter author is a hidden trusted declaration;
        // its exact target is frozen in the original request, not guessed in BE.
        catalog.resolve_bound_trusted(name, FunctionKind::Scalar, request, compile_control())
    } else {
        catalog.resolve_bound_user(name, FunctionKind::Scalar, request, compile_control())
    }
    .unwrap();
    let FunctionResultType::Scalar(result) = &resolved.selected.result_type else {
        panic!("real scalar result")
    };
    let result = result.clone();
    let function = BoundFunction {
        function_id: resolved.function_id,
        overload: resolved.selected.overload,
        kind: resolved.kind,
        argument_types: resolved.selected.argument_types,
        result_type: result.clone(),
        legacy_metadata: Some(LegacyBindingMetadata {
            volatility: resolved.semantics.volatility,
            argument_evaluation: resolved.semantics.argument_evaluation,
            failure_behavior: resolved.semantics.failure_behavior,
            intrinsic_row_error: resolved.semantics.intrinsic_row_error,
            semantic_parameters: Box::default(),
        }),
    };
    let mut builder = FragmentBuilder::new(FragmentId::new(73));
    let node = builder.reserve_node_id().unwrap();
    let literal = builder
        .add_expression(node, argument.clone(), ExprKind::Constant(reference))
        .unwrap();
    let allow = novarocks_type_contract::SemanticParameterRef {
        id: novarocks_type_contract::SemanticParameterId::new(11),
        expected_key: novarocks_type_contract::SemanticParameterKey::AllowThrowException,
    };
    let literal = if let Some(target) = &coerced {
        builder
            .add_expression(
                node,
                target.clone(),
                ExprKind::Cast {
                    expr: literal,
                    target: target.data_type().clone(),
                    decimal_overflow_policy:
                        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                    allow_throw_exception: allow,
                },
            )
            .unwrap()
    } else {
        literal
    };
    let call = builder
        .add_expression(
            node,
            result.clone(),
            ExprKind::FunctionCall {
                function,
                args: Box::from([literal]),
            },
        )
        .unwrap();
    let output = builder
        .add_value(
            result.clone(),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: singleton_properties(),
            output: OutputPort {
                node,
                columns: Box::from([output]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([call])]),
            },
        })
        .unwrap();
    let fragment = builder
        .finish_definition(
            node,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap()
        .with_call_requests_observed(
            vec![(
                PhysicalCallDefinition::Expression(call),
                PhysicalCallRequest {
                    arguments: Box::from([StaticFunctionArgument::Value {
                        value_type: call_argument,
                        constant: original_constant,
                    }]),
                    logical_argument_count: 1,
                    expected_result_type: expected_target,
                    constant_policy: policy(),
                },
            )],
            compile_control(),
        )
        .unwrap();
    let mut builder = PlanBuilder::new(PlanVersionId::try_new([73; 16]).unwrap());
    if coerced.is_some() {
        builder = builder.with_semantic_parameters(
            novarocks_type_contract::SemanticParameters::try_new([(
                allow.id,
                novarocks_type_contract::SemanticParameterValue::AllowThrowException(false),
            )])
            .unwrap(),
        );
    }
    builder.insert_constant_pool(reference.pool, pool).unwrap();
    let plan = finish_scalar_fixture(builder, fragment, node, output);
    let mut encoded = novarocks_plan_codec::encode_physical_plan_typed_pure(
        &plan,
        &catalog,
        &novarocks_plan_codec::NoPhysicalV1PrivateFacts,
        false,
        compile_control(),
    )
    .unwrap();
    assert_eq!(encoded.fragments.len(), 1);
    encoded.fragments.remove(0)
}

fn call_expression(source: &plan::PlanFragment) -> &novarocks_proto_models::expr::Expr {
    let Some(plan::distributed_node::Payload::Physical(node)) =
        &source.root.as_ref().unwrap().payload
    else {
        panic!("actual physical source")
    };
    let Some(plan::plan_node::Kind::Values(values)) = &node.kind else {
        panic!("actual Values source")
    };
    &values.rows[0].values[0]
}
fn call_expression_mut(source: &mut plan::PlanFragment) -> &mut novarocks_proto_models::expr::Expr {
    let Some(plan::distributed_node::Payload::Physical(node)) =
        &mut source.root.as_mut().unwrap().payload
    else {
        panic!("actual physical source")
    };
    let Some(plan::plan_node::Kind::Values(values)) = &mut node.kind else {
        panic!("actual Values source")
    };
    &mut values.rows[0].values[0]
}
fn empty_input() -> Chunk {
    let options = arrow::array::RecordBatchOptions::new().with_row_count(Some(1));
    let batch = arrow::array::RecordBatch::try_new_with_options(
        Arc::new(arrow::datatypes::Schema::empty()),
        Vec::new(),
        &options,
    )
    .unwrap();
    Chunk::try_new_with_chunk_schema(
        batch,
        Arc::new(novarocks_execution::exec::chunk::ChunkSchema::empty()),
    )
    .unwrap()
}

#[test]
fn default_scalar_frozen_fe_constant_request_reaches_original_eval() {
    let source = encoded_constant_call(
        "abs",
        ValueType::new(DataType::Int32, false),
        Arc::new(Int32Array::from(vec![-7])),
    );
    let mut arena = ExprArena::default();
    let id = with_fragment_types(&source, |types| {
        crate::fragment_plan_node::decode_zero_input_expression(
            call_expression(&source),
            novarocks_proto_codec::FieldPath::root("expr"),
            &mut arena,
            types,
        )
    })
    .unwrap();
    let value = arena.eval(id, &empty_input()).unwrap();
    assert_eq!(
        value
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        7
    );
    assert_eq!(
        arena.value_type(id).unwrap(),
        &ValueType::new(DataType::Int64, false)
    );

    let source = encoded_constant_call(
        "parse_json",
        ValueType::new(DataType::Utf8, false),
        Arc::new(StringArray::from(vec![" { \"a\" : 1 } "])),
    );
    let mut arena = ExprArena::default();
    let id = with_fragment_types(&source, |types| {
        crate::fragment_plan_node::decode_zero_input_expression(
            call_expression(&source),
            novarocks_proto_codec::FieldPath::root("expr"),
            &mut arena,
            types,
        )
    })
    .unwrap();
    assert_eq!(
        arena.value_type(id).unwrap().logical_type(),
        ValueLogicalType::Json
    );
    let value = arena.eval(id, &empty_input()).unwrap();
    assert_eq!(
        value
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "{\"a\": 1}"
    );
}

#[test]
fn default_scalar_frozen_same_carrier_wrong_nominal_is_rejected() {
    let mut source = encoded_constant_call(
        "parse_json",
        ValueType::new(DataType::Utf8, false),
        Arc::new(StringArray::from(vec!["{\"a\":1}"])),
    );
    let expression = call_expression_mut(&mut source);
    let Some(novarocks_proto_models::expr::expr::Kind::FunctionCall(call)) = &mut expression.kind
    else {
        panic!("real scalar source")
    };
    let plain = call.args[0]
        .r#type
        .as_ref()
        .unwrap()
        .value_binding_id
        .unwrap();
    assert_ne!(
        expression.r#type.as_ref().unwrap().value_binding_id,
        Some(plain)
    );
    call.frozen_binding.as_mut().unwrap().result = Some(novarocks_proto_models::physical_package_v2::function_binding_definition::Result::ScalarValueTypeId(plain));
    let mut arena = ExprArena::default();
    let failure = with_fragment_types(&source, |types| {
        crate::fragment_plan_node::decode_zero_input_expression(
            call_expression(&source),
            novarocks_proto_codec::FieldPath::root("expr"),
            &mut arena,
            types,
        )
    })
    .unwrap_err();
    assert!(
        failure.to_string().contains("complete frozen declaration"),
        "{failure}"
    );
}

#[derive(Default)]
struct TraceControl(Mutex<Vec<(CompilePhase, u32)>>);
impl PureCompileControl for TraceControl {
    fn checkpoint(&self, phase: CompilePhase, work: u32) -> Result<(), CompileControlError> {
        self.0.lock().unwrap().push((phase, work));
        Ok(())
    }
}
#[test]
fn default_scalar_first_real_header_refusal_rolls_back_without_tail() {
    let source = encoded_constant_call(
        "abs",
        ValueType::new(DataType::Int32, false),
        Arc::new(Int32Array::from(vec![-7])),
    );
    let tracker = novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
        "ordinary scalar first header",
    );
    let control = TraceControl::default();
    let catalog = novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap();
    let creation = crate::native_type_construction::NativeTypeConstruction::try_with_control(
        Arc::clone(&tracker),
        &control,
    )
    .unwrap();
    let mut resources =
        crate::fragment_type_decode::NativeDecodeResources::try_new(&creation).unwrap();
    resources.bind_function_catalog(&catalog);
    let types =
        crate::fragment_type_decode::decode_fragment_type_owner(&source, &mut resources).unwrap();
    let original =
        crate::fragment_original_sources::decode_original_requests(&source, &types, &mut resources)
            .unwrap();
    resources.work.flush().unwrap();
    let baseline = tracker.current();
    assert!(baseline > 0);
    tracker.install_limit_once(baseline + 1).unwrap();
    let mut arena = ExprArena::default();
    let mut decoder = crate::fragment_type_decode::NativeTypeDecoder::Strong {
        types: &types.execution,
        original_requests: Some(original.as_ref()),
        resources: &mut resources,
    };
    let failure = crate::fragment_plan_node::decode_zero_input_expression(
        call_expression(&source),
        novarocks_proto_codec::FieldPath::root("expr"),
        &mut arena,
        &mut decoder,
    )
    .unwrap_err();
    assert!(
        matches!(
            failure.construction_cause(),
            Some(crate::native_type_construction::NativeTypeConstructionError::Capacity(_))
        ),
        "{failure}"
    );
    assert_eq!(
        tracker.current(),
        baseline,
        "the failed actual request rolls back its prospective bytes"
    );
    let stopped = control.0.lock().unwrap().len();
    drop(decoder);
    drop(resources); // Error exit performs no finish/footer.
    assert_eq!(control.0.lock().unwrap().len(), stopped);
    drop(arena);
    drop(original);
    drop(types);
    drop(creation);
    assert_eq!(
        tracker.current(),
        0,
        "last actual immutable type/constant reference releases its finite responsibility"
    );
}

#[test]
fn default_scalar_frozen_original_request_keeps_explicit_coercion() {
    let source = encoded_constant_call_with_coercion(
        "abs",
        ValueType::new(DataType::Int16, false),
        Arc::new(Int16Array::from(vec![-12])),
        Some(ValueType::new(DataType::Int32, false)),
    );
    let expression = call_expression(&source);
    let Some(novarocks_proto_models::expr::expr::Kind::FunctionCall(call)) = &expression.kind
    else {
        panic!("ordinary actual call")
    };
    assert!(matches!(
        call.args[0].kind,
        Some(novarocks_proto_models::expr::expr::Kind::Cast(_))
    ));
    let mut arena = ExprArena::default();
    let id = with_fragment_types(&source, |types| {
        crate::fragment_plan_node::decode_zero_input_expression(
            expression,
            novarocks_proto_codec::FieldPath::root("expr"),
            &mut arena,
            types,
        )
    })
    .unwrap();
    assert_eq!(
        arena.value_type(id).unwrap(),
        &ValueType::new(DataType::Int64, false)
    );
    let value = arena.eval(id, &empty_input()).unwrap();
    assert_eq!(
        value
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        12
    );
}

fn encoded_original_lambda_call() -> plan::PlanFragment {
    use arrow::array::ListArray;
    let catalog = novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap();
    let values: ArrayRef = Arc::new(ListArray::from_iter_primitive::<
        arrow::datatypes::Int32Type,
        _,
        _,
    >([Some([Some(3), None, Some(8)])]));
    let argument = ValueType::new(values.data_type().clone(), false);
    let item = ValueType::new(DataType::Int32, true);
    let parameter_types =
        novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(Box::from([item.clone()]));
    let pool = ConstantPool::try_new(
        Arc::new(arrow::datatypes::Field::new(
            "original list",
            argument.data_type().clone(),
            argument.nullable(),
        )),
        argument.clone(),
        values.to_data(),
        policy(),
        CompilePhase::Encode,
        compile_control(),
    )
    .unwrap();
    let reference = ConstantReference {
        pool: ConstantPoolId::new(38),
        ordinal: 0,
    };
    let arguments = [
        FunctionArgument::Lambda {
            parameter_types: parameter_types.clone(),
            result_type: item.clone(),
        },
        FunctionArgument::Value {
            value_type: argument.clone(),
            constant: Some(pool.value(0).unwrap()),
        },
    ];
    let resolved = catalog
        .resolve_bound_user(
            "array_map",
            FunctionKind::Scalar,
            FunctionBindingRequest {
                arguments: &arguments,
                logical_argument_count: 2,
                expected_result_type: None,
            },
            compile_control(),
        )
        .unwrap();
    let FunctionResultType::Scalar(result) = &resolved.selected.result_type else {
        panic!("real scalar result")
    };
    let result = result.clone();
    let function = BoundFunction {
        function_id: resolved.function_id,
        overload: resolved.selected.overload,
        kind: resolved.kind,
        argument_types: resolved.selected.argument_types,
        result_type: result.clone(),
        legacy_metadata: Some(LegacyBindingMetadata {
            volatility: resolved.semantics.volatility,
            argument_evaluation: resolved.semantics.argument_evaluation,
            failure_behavior: resolved.semantics.failure_behavior,
            intrinsic_row_error: resolved.semantics.intrinsic_row_error,
            semantic_parameters: Box::default(),
        }),
    };
    let mut builder = FragmentBuilder::new(FragmentId::new(74));
    let node = builder.reserve_node_id().unwrap();
    let lambda = builder.reserve_expression_id().unwrap();
    let parameter = builder.reserve_expression_id().unwrap();
    builder
        .insert_expression(novarocks_physical_plan::ExprNode {
            id: parameter,
            owner: node,
            lambda_scope: Some(lambda),
            ty: item.clone(),
            kind: ExprKind::LambdaParameter { lambda, ordinal: 0 },
        })
        .unwrap();
    builder
        .insert_expression(novarocks_physical_plan::ExprNode {
            id: lambda,
            owner: node,
            lambda_scope: None,
            ty: item.clone(),
            kind: ExprKind::Lambda {
                parameter_types: Box::from([item.clone()]),
                body: parameter,
            },
        })
        .unwrap();
    let literal = builder
        .add_expression(node, argument.clone(), ExprKind::Constant(reference))
        .unwrap();
    let call = builder
        .add_expression(
            node,
            result.clone(),
            ExprKind::FunctionCall {
                function,
                args: Box::from([lambda, literal]),
            },
        )
        .unwrap();
    let output = builder
        .add_value(
            result,
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: singleton_properties(),
            output: OutputPort {
                node,
                columns: Box::from([output]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([call])]),
            },
        })
        .unwrap();
    let fragment = builder
        .finish_definition(
            node,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap()
        .with_call_requests_observed(
            vec![(
                PhysicalCallDefinition::Expression(call),
                PhysicalCallRequest {
                    arguments: Box::from([
                        StaticFunctionArgument::Lambda {
                            parameter_types,
                            result_type: item,
                        },
                        StaticFunctionArgument::Value {
                            value_type: argument,
                            constant: Some(reference),
                        },
                    ]),
                    logical_argument_count: 2,
                    expected_result_type: None,
                    constant_policy: policy(),
                },
            )],
            compile_control(),
        )
        .unwrap();
    let mut builder = PlanBuilder::new(PlanVersionId::try_new([74; 16]).unwrap());
    builder.insert_constant_pool(reference.pool, pool).unwrap();
    let plan = finish_scalar_fixture(builder, fragment, node, output);
    let mut encoded = novarocks_plan_codec::encode_physical_plan_typed_pure(
        &plan,
        &catalog,
        &novarocks_plan_codec::NoPhysicalV1PrivateFacts,
        false,
        compile_control(),
    )
    .unwrap();
    assert_eq!(encoded.fragments.len(), 1);
    encoded.fragments.remove(0)
}
#[test]
fn default_scalar_frozen_original_lambda_types_and_null_values_reach_original_eval() {
    let source = encoded_original_lambda_call();
    let mut arena = ExprArena::default();
    let id = with_fragment_types(&source, |types| {
        crate::fragment_plan_node::decode_zero_input_expression(
            call_expression(&source),
            novarocks_proto_codec::FieldPath::root("expr"),
            &mut arena,
            types,
        )
    })
    .unwrap();
    let output = arena.eval(id, &empty_input()).unwrap();
    let output = output
        .as_any()
        .downcast_ref::<arrow::array::ListArray>()
        .unwrap();
    let items = output.value(0);
    let items = items.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(items.iter().collect::<Vec<_>>(), [Some(3), None, Some(8)]);
    assert!(
        arena
            .value_type(id)
            .unwrap()
            .value_type()
            .child_type_at(0)
            .unwrap()
            .nullable()
    );

    // An actual lambda child declaration cannot be replaced by an equal
    // carrier with a different child NULL law from the original request.
    let mut changed = source.clone();
    let expression = call_expression_mut(&mut changed);
    let Some(novarocks_proto_models::expr::expr::Kind::FunctionCall(call)) = &mut expression.kind
    else {
        panic!("real scalar source")
    };
    let Some(novarocks_proto_models::expr::expr::Kind::Lambda(lambda)) = &mut call.args[0].kind
    else {
        panic!("real original lambda")
    };
    lambda.params[0].nullable = false;
    let mut arena = ExprArena::default();
    let failure = with_fragment_types(&changed, |types| {
        crate::fragment_plan_node::decode_zero_input_expression(
            call_expression(&changed),
            novarocks_proto_codec::FieldPath::root("expr"),
            &mut arena,
            types,
        )
    })
    .unwrap_err();
    assert!(
        failure.to_string().contains("lambda parameter"),
        "{failure}"
    );
}

// These fixtures author one scalar result from their real resolved value, not
// the Arrow carrier or a sink default. Encoding binds its actual output slot.
fn fixture_result_field(
    value: &novarocks_type_contract::ValueTypeRef,
) -> novarocks_result_contract::ScalarField {
    use novarocks_result_contract::ScalarValueType as S;
    use novarocks_type_contract::ValueKind as V;
    let value_type = match value.kind() {
        V::Scalar(DataType::Int32) => S::SignedInteger(32),
        V::Scalar(DataType::Int64) => S::SignedInteger(64),
        V::String => S::String,
        V::Binary => S::Binary,
        V::Nominal(ValueLogicalType::Json) => S::Json,
        V::List(item) => S::List(Box::new(fixture_result_field(item))),
        V::Map { key, value } => S::Map {
            key: Box::new(fixture_result_field(key)),
            value: Box::new(fixture_result_field(value)),
        },
        other => panic!("fixture has no declared scalar purpose for {other:?}"),
    };
    novarocks_result_contract::ScalarField {
        nullable: value.nullable(),
        value_type,
    }
}
fn finish_scalar_fixture(
    mut builder: PlanBuilder,
    fragment: novarocks_physical_plan::Fragment,
    node: novarocks_physical_plan::NodeId,
    output: ValueId,
) -> PhysicalPlan {
    use novarocks_result_contract::{
        FrozenRootOutput, RootOutputContract, RootProfileId, ScalarSchema,
    };
    let result = fragment.values()[&output].ty.clone();
    let schema = ScalarSchema::try_new(fixture_result_field(result.value_type())).unwrap();
    let domain = match result.logical_type() {
        ValueLogicalType::Json => novarocks_physical_plan::ResultValueDomain::Json,
        ValueLogicalType::Physical => novarocks_physical_plan::ResultValueDomain::Plain,
        other => panic!("fixture root has no declared result domain for {other:?}"),
    };
    let fragment_id = fragment.id();
    builder.add_fragment(fragment).unwrap();
    builder
        .set_result_port(ResultPort {
            fragment: fragment_id,
            output: OutputPort {
                node,
                columns: Box::from([output]),
            },
            fields: Box::from([ResultField {
                domain,
                name: "scalar".into(),
                alias: None,
                value: output,
                ty: result,
            }]),
            scalar_schema: Some(schema.clone()),
        })
        .unwrap();
    builder
        .finish_observed(compile_control())
        .unwrap()
        .with_root_output(RootOutputContract::new(
            RootProfileId::V1,
            FrozenRootOutput::ScalarValue(schema),
        ))
        .unwrap()
}

#[test]
fn default_array_literal_frozen_json_child_keeps_original_constructor() {
    let json =
        ValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json).unwrap();
    let source = encoded_constant_call(
        "__array_literal",
        json,
        Arc::new(StringArray::from(vec!["{\"a\":1}"])),
    );
    let mut arena = ExprArena::default();
    let id = with_fragment_types(&source, |types| {
        crate::fragment_plan_node::decode_zero_input_expression(
            call_expression(&source),
            novarocks_proto_codec::FieldPath::root("expr"),
            &mut arena,
            types,
        )
    })
    .unwrap();
    assert!(matches!(
        arena.node(id),
        Some(novarocks_execution::exec::expr::ExprNode::ArrayExpr { .. })
    ));
    assert_eq!(
        arena
            .value_type(id)
            .unwrap()
            .value_type()
            .child_type_at(0)
            .unwrap()
            .kind(),
        &novarocks_type_contract::ValueKind::Nominal(ValueLogicalType::Json)
    );
    let value = arena.eval(id, &empty_input()).unwrap();
    let value = value
        .as_any()
        .downcast_ref::<arrow::array::ListArray>()
        .unwrap()
        .value(0);
    assert_eq!(
        value
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "{\"a\":1}"
    );
}

fn encoded_original_map_call() -> plan::PlanFragment {
    encoded_original_map_call_with_sources(
        ValueType::new(DataType::Int32, false),
        ValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json).unwrap(),
        [
            Arc::new(Int32Array::from(vec![7])),
            Arc::new(StringArray::from(vec!["{\"a\":1}"])),
        ],
    )
}

fn encoded_original_map_call_with_sources(
    key: ValueType,
    json: ValueType,
    arrays: [ArrayRef; 2],
) -> plan::PlanFragment {
    let catalog = novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap();
    let mut pools = Vec::new();
    let mut arguments = Vec::new();
    let references = [
        ConstantReference {
            pool: ConstantPoolId::new(40),
            ordinal: 0,
        },
        ConstantReference {
            pool: ConstantPoolId::new(41),
            ordinal: 0,
        },
    ];
    for (ty, array) in [&key, &json].into_iter().zip(arrays) {
        let pool = ConstantPool::try_new(
            Arc::new(arrow::datatypes::Field::new(
                "original pair",
                ty.data_type().clone(),
                ty.nullable(),
            )),
            ty.clone(),
            array.to_data(),
            policy(),
            CompilePhase::Encode,
            compile_control(),
        )
        .unwrap();
        arguments.push(FunctionArgument::Value {
            value_type: ty.clone(),
            constant: Some(pool.value(0).unwrap()),
        });
        pools.push(pool);
    }
    let resolved = catalog
        .resolve_bound_user(
            "map",
            FunctionKind::Scalar,
            FunctionBindingRequest {
                arguments: &arguments,
                logical_argument_count: 2,
                expected_result_type: None,
            },
            compile_control(),
        )
        .unwrap();
    let FunctionResultType::Scalar(result) = &resolved.selected.result_type else {
        panic!("real MAP result")
    };
    let result = result.clone();
    let function = BoundFunction {
        function_id: resolved.function_id,
        overload: resolved.selected.overload,
        kind: resolved.kind,
        argument_types: resolved.selected.argument_types,
        result_type: result.clone(),
        legacy_metadata: Some(LegacyBindingMetadata {
            volatility: resolved.semantics.volatility,
            argument_evaluation: resolved.semantics.argument_evaluation,
            failure_behavior: resolved.semantics.failure_behavior,
            intrinsic_row_error: resolved.semantics.intrinsic_row_error,
            semantic_parameters: Box::default(),
        }),
    };
    let mut builder = FragmentBuilder::new(FragmentId::new(75));
    let node = builder.reserve_node_id().unwrap();
    let k = builder
        .add_expression(node, key.clone(), ExprKind::Constant(references[0]))
        .unwrap();
    let v = builder
        .add_expression(node, json.clone(), ExprKind::Constant(references[1]))
        .unwrap();
    let call = builder
        .add_expression(
            node,
            result.clone(),
            ExprKind::FunctionCall {
                function,
                args: Box::from([k, v]),
            },
        )
        .unwrap();
    let output = builder
        .add_value(
            result,
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: singleton_properties(),
            output: OutputPort {
                node,
                columns: Box::from([output]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([call])]),
            },
        })
        .unwrap();
    let fragment = builder
        .finish_definition(
            node,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap()
        .with_call_requests_observed(
            vec![(
                PhysicalCallDefinition::Expression(call),
                PhysicalCallRequest {
                    arguments: Box::from([
                        StaticFunctionArgument::Value {
                            value_type: key,
                            constant: Some(references[0]),
                        },
                        StaticFunctionArgument::Value {
                            value_type: json,
                            constant: Some(references[1]),
                        },
                    ]),
                    logical_argument_count: 2,
                    expected_result_type: None,
                    constant_policy: policy(),
                },
            )],
            compile_control(),
        )
        .unwrap();
    let mut builder = PlanBuilder::new(PlanVersionId::try_new([75; 16]).unwrap());
    for (reference, pool) in references.into_iter().zip(pools) {
        builder.insert_constant_pool(reference.pool, pool).unwrap();
    }
    let plan = finish_scalar_fixture(builder, fragment, node, output);
    let mut encoded = novarocks_plan_codec::encode_physical_plan_typed_pure(
        &plan,
        &catalog,
        &novarocks_plan_codec::NoPhysicalV1PrivateFacts,
        false,
        compile_control(),
    )
    .unwrap();
    assert_eq!(encoded.fragments.len(), 1);
    encoded.fragments.remove(0)
}
#[test]
fn default_map_frozen_json_value_keeps_original_constructor_and_checks_original_constant() {
    let source = encoded_original_map_call();
    let mut arena = ExprArena::default();
    let id = with_fragment_types(&source, |types| {
        crate::fragment_plan_node::decode_zero_input_expression(
            call_expression(&source),
            novarocks_proto_codec::FieldPath::root("expr"),
            &mut arena,
            types,
        )
    })
    .unwrap();
    assert!(matches!(
        arena.node(id),
        Some(novarocks_execution::exec::expr::ExprNode::FunctionCall {
            kind: novarocks_execution::exec::expr::function::FunctionKind::Map("map"),
            ..
        })
    ));
    assert_eq!(
        arena
            .value_type(id)
            .unwrap()
            .value_type()
            .child_type_at(1)
            .unwrap()
            .kind(),
        &novarocks_type_contract::ValueKind::Nominal(ValueLogicalType::Json)
    );
    let value = arena.eval(id, &empty_input()).unwrap();
    let value = value
        .as_any()
        .downcast_ref::<arrow::array::MapArray>()
        .unwrap();
    assert_eq!(
        value
            .keys()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        7
    );
    assert_eq!(
        value
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "{\"a\":1}"
    );

    let mut changed = source.clone();
    let Some(novarocks_proto_models::expr::expr::Kind::FunctionCall(call)) =
        &mut call_expression_mut(&mut changed).kind
    else {
        panic!("real MAP source")
    };
    let Some(novarocks_proto_models::expr::expr::Kind::ConstantReference(reference)) =
        &mut call.args[1].kind
    else {
        panic!("real original constant")
    };
    reference.row_ordinal = 1;
    let mut arena = ExprArena::default();
    let failure = with_fragment_types(&changed, |types| {
        crate::fragment_plan_node::decode_zero_input_expression(
            call_expression(&changed),
            novarocks_proto_codec::FieldPath::root("expr"),
            &mut arena,
            types,
        )
    })
    .unwrap_err();
    assert!(
        failure.to_string().contains("constant address differs"),
        "{failure}"
    );
}

#[test]
fn default_scalar_frozen_percentile_binary_target_reaches_original_keep_leaf() {
    use arrow::array::BinaryArray;
    use novarocks_functions::builtin::value_conversion::VALUE_CONVERSION_NAME;
    let source_type =
        ValueType::try_with_logical_type(DataType::Binary, false, ValueLogicalType::Percentile)
            .unwrap();
    let target = ValueType::new(DataType::Binary, false);
    let source = encoded_constant_call_with_target(
        VALUE_CONVERSION_NAME,
        source_type.clone(),
        Arc::new(BinaryArray::from_vec(vec![b"original opaque percentile"])),
        None,
        Some(target.clone()),
    );
    let expression = call_expression(&source);
    let Some(novarocks_proto_models::expr::expr::Kind::FunctionCall(call)) = &expression.kind
    else {
        panic!("exact converter call")
    };
    assert!(call.frozen_binding.is_some());
    assert!(call.original_request_id.is_some());
    let mut arena = ExprArena::default();
    let id = with_fragment_types(&source, |types| {
        crate::fragment_plan_node::decode_zero_input_expression(
            expression,
            novarocks_proto_codec::FieldPath::root("expr"),
            &mut arena,
            types,
        )
    })
    .unwrap();
    let Some(novarocks_execution::exec::expr::ExprNode::FunctionCall { kind, args }) =
        arena.node(id)
    else {
        panic!("original default FunctionCall actor")
    };
    assert_eq!(
        *kind,
        novarocks_execution::exec::expr::function::FunctionKind::PercentileToBinary
    );
    assert_eq!(arena.value_type(args[0]), Some(&source_type));
    assert_eq!(arena.value_type(id), Some(&target));
    let result = arena.eval(id, &empty_input()).unwrap();
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        b"original opaque percentile"
    );

    // The same opaque overload has other Candidate owners. Complete original
    // HLL semantics cannot be admitted as this default Percentile actor merely
    // because it shares Binary and the same nullable contract.
    let hll = encoded_constant_call_with_target(
        VALUE_CONVERSION_NAME,
        ValueType::try_with_logical_type(DataType::Binary, false, ValueLogicalType::Hll).unwrap(),
        Arc::new(BinaryArray::from_vec(vec![b"original hll"])),
        None,
        Some(target),
    );
    let mut arena = ExprArena::default();
    let error = with_fragment_types(&hll, |types| {
        crate::fragment_plan_node::decode_zero_input_expression(
            call_expression(&hll),
            novarocks_proto_codec::FieldPath::root("expr"),
            &mut arena,
            types,
        )
    })
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("default host does not support this frozen value-domain conversion"),
        "{error}"
    );
}

fn assert_real_sql_optimized_constant_keeps_original_none(sql: &str, expected: Option<&str>) {
    use novarocks_physical_plan::ScanReadBudget;
    use novarocks_sql::compiler::{
        DEFAULT_COMPLETION_LIMITS, SessionOptimizerSettings, SqlCompileControl, SqlCompileIntent,
        SqlCompiler, SqlFinalPlanCompileRequest, SqlFunctionCatalog, SqlPhysicalEmissionMode,
        SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput,
        build_builtin_engine_function_catalog, noop_constant_evaluator,
    };

    let control = SqlCompileControl::unbounded();
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let request = SqlFinalPlanCompileRequest::new(
        PlanVersionId::try_new([83; 16]).unwrap(),
        SqlStatementInput::sql(sql),
        SqlCompileIntent::Query,
        SqlSessionContext {
            sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings::default(),
            current_catalog: None,
            current_database: "fixture".into(),
            optimizer_settings: SessionOptimizerSettings {
                // Keep real SQL Nested sources until the canonical request is captured.
                // The existing lowering then unwraps them without changing that request.
                disabled_rules: vec!["FoldConstant".into()],
                enable_materialized_view_rewrite: Some(false),
                enable_common_subexpr_reuse: Some(false),
                ..SessionOptimizerSettings::default()
            },
        },
        SqlPlanningEnvironment::Distributed,
        catalog.snapshot(),
        noop_constant_evaluator(),
        policy(),
        SqlPhysicalEmissionMode::OriginalNativeV1,
        control.clone(),
        PipelineDopDomain {
            min: 1,
            max: 8,
            requires_power_of_two: true,
        },
        ScanReadBudget {
            max_batch_rows: 64,
            max_batch_bytes: 1 << 20,
        },
        DEFAULT_COMPLETION_LIMITS,
    )
    .try_into_completion()
    .unwrap();
    let owner = SqlCompiler::start(request, &control)
        .expect("public SQL compilation")
        .into_complete()
        .expect("table-free SQL has no catalog observation need")
        .into_plan();
    let calls = owner
        .plan()
        .fragments()
        .values()
        .flat_map(|fragment| {
            fragment
                .expressions()
                .iter()
                .filter_map(move |(_, expression)| {
                    matches!(&expression.kind, ExprKind::FunctionCall { function, .. }
                if function.function_id.as_str().contains("/lower/"))
                    .then_some((fragment, expression))
                })
        })
        .collect::<Vec<_>>();
    assert_eq!(
        calls.len(),
        1,
        "the authored SQL must retain one LOWER call: {sql}"
    );
    let (fragment, call) = calls[0];
    let ExprKind::FunctionCall { args, .. } = &call.kind else {
        unreachable!()
    };
    assert_eq!(args.len(), 1);
    let actual = fragment.expressions().get(args[0]).unwrap();
    let ExprKind::Constant(reference) = &actual.kind else {
        panic!(
            "the SQL physical argument must already be a constant: {:?}",
            actual.kind
        );
    };
    let actual_value = owner
        .plan()
        .constants()
        .entries()
        .get(&reference.pool)
        .unwrap()
        .value(reference.ordinal)
        .unwrap();
    assert_eq!(actual_value.value_type(), &actual.ty);
    let original = fragment
        .call_requests()
        .get(PhysicalCallDefinition::Expression(call.id))
        .expect("original SQL request is frozen independently of physical optimization");
    let [
        StaticFunctionArgument::Value {
            value_type,
            constant,
        },
    ] = original.arguments.as_ref()
    else {
        panic!("one original value argument");
    };
    assert!(
        constant.is_none(),
        "Nested original source must not acquire an address: {sql}; request={original:?}"
    );
    assert_eq!(
        value_type, &actual.ty,
        "optimization preserves the full argument type"
    );

    let mut encoded = novarocks_plan_codec::encode_physical_plan_typed_pure(
        owner.plan(),
        &catalog,
        &novarocks_plan_codec::NoPhysicalV1PrivateFacts,
        false,
        compile_control(),
    )
    .expect("real public SQL plan encoding");
    let matching = encoded
        .fragments
        .iter()
        .filter(|source| source.fragment_id == fragment.id().get())
        .count();
    assert_eq!(
        matching, 1,
        "the authored call fragment has one exact wire identity"
    );
    let source = encoded.fragments.swap_remove(
        encoded
            .fragments
            .iter()
            .position(|source| source.fragment_id == fragment.id().get())
            .unwrap(),
    );
    let mut arena = ExprArena::default();
    let _decoded = decode_node(
        &source,
        source.root.as_ref().unwrap(),
        &mut arena,
        &NativePlanDecodeContext::default(),
    )
    .unwrap_or_else(|error| {
        panic!("Strong decode must preserve original None and validate actual Constant: {error}")
    });
    let mut lower = Vec::new();
    let mut index = 0;
    while let Some(node) = arena.node(novarocks_execution::exec::expr::ExprId(index)) {
        if matches!(
            node,
            novarocks_execution::exec::expr::ExprNode::FunctionCall {
                kind: novarocks_execution::exec::expr::function::FunctionKind::String("lower"),
                ..
            }
        ) {
            lower.push(novarocks_execution::exec::expr::ExprId(index));
        }
        index += 1;
    }
    assert_eq!(
        lower.len(),
        1,
        "original LOWER remains the actual execution owner"
    );
    let output = arena
        .eval(lower[0], &empty_input())
        .expect("original scalar evaluation");
    let output = output.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(output.iter().collect::<Vec<_>>(), vec![expected]);
}

#[test]
fn default_real_sql_nested_constant_preserves_original_none_through_strong_decode() {
    assert_real_sql_optimized_constant_keeps_original_none(
        "SELECT lower(('MiXeD'))",
        Some("mixed"),
    );
}

#[test]
fn default_real_sql_nested_typed_null_preserves_original_none_through_strong_decode() {
    assert_real_sql_optimized_constant_keeps_original_none(
        "SELECT lower((CAST(NULL AS VARCHAR)))",
        None,
    );
}

#[test]
fn default_map_original_some_rejects_equal_value_foreign_pool_and_ordinal() {
    let ty = ValueType::new(DataType::Utf8, false);
    // Both pools have the same exact type and two identical values. Their
    // source identities and ordinals remain independent facts.
    let source = encoded_original_map_call_with_sources(
        ty.clone(),
        ty,
        [
            Arc::new(StringArray::from(vec!["same", "same"])),
            Arc::new(StringArray::from(vec!["same", "same"])),
        ],
    );
    let mut arena = ExprArena::default();
    with_fragment_types(&source, |types| {
        crate::fragment_plan_node::decode_zero_input_expression(
            call_expression(&source),
            novarocks_proto_codec::FieldPath::root("expr"),
            &mut arena,
            types,
        )
    })
    .expect("the original exact two-pool source is valid");
    for (pool, ordinal) in [(40, 0), (41, 1)] {
        let mut changed = source.clone();
        let Some(novarocks_proto_models::expr::expr::Kind::FunctionCall(call)) =
            &mut call_expression_mut(&mut changed).kind
        else {
            panic!("real MAP source");
        };
        let Some(novarocks_proto_models::expr::expr::Kind::ConstantReference(reference)) =
            &mut call.args[1].kind
        else {
            panic!("real original constant");
        };
        assert_eq!(reference.pool_id, Some(41));
        assert_eq!(reference.row_ordinal, 0);
        reference.pool_id = Some(pool);
        reference.row_ordinal = ordinal;
        let mut arena = ExprArena::default();
        let failure = with_fragment_types(&changed, |types| {
            crate::fragment_plan_node::decode_zero_input_expression(
                call_expression(&changed),
                novarocks_proto_codec::FieldPath::root("expr"),
                &mut arena,
                types,
            )
        })
        .unwrap_err();
        assert!(
            failure.to_string().contains("constant address differs"),
            "{failure}"
        );
    }
}

#[test]
fn default_real_sql_utf8_to_nominal_largeint_keeps_original_i128_null_parse_policy() {
    use novarocks_physical_plan::ScanReadBudget;
    use novarocks_sql::compiler::*;
    for (sql, expected) in [
        ("SELECT CAST('-' AS LARGEINT)", None),
        ("SELECT CAST('+' AS LARGEINT)", None),
        ("SELECT CAST('  9000 ' AS LARGEINT)", Some(9000i128)),
        (
            "SELECT CAST('9223372036854775808' AS LARGEINT)",
            Some(9223372036854775808i128),
        ),
        (
            "SELECT CAST('170141183460469231731687303715884105727' AS LARGEINT)",
            Some(i128::MAX),
        ),
        (
            "SELECT CAST('-170141183460469231731687303715884105728' AS LARGEINT)",
            Some(i128::MIN),
        ),
        (
            "SELECT CAST('170141183460469231731687303715884105728' AS LARGEINT)",
            None,
        ),
    ] {
        let catalog = build_builtin_engine_function_catalog().unwrap();
        let control = SqlCompileControl::unbounded();
        let request = SqlFinalPlanCompileRequest::new(
            PlanVersionId::try_new([89; 16]).unwrap(),
            SqlStatementInput::sql(sql),
            SqlCompileIntent::Query,
            SqlSessionContext {
                sql_semantics: Default::default(),
                current_catalog: None,
                current_database: "fixture".into(),
                optimizer_settings: SessionOptimizerSettings {
                    disabled_rules: vec!["FoldConstant".into()],
                    enable_materialized_view_rewrite: Some(false),
                    enable_common_subexpr_reuse: Some(false),
                    ..Default::default()
                },
            },
            SqlPlanningEnvironment::Distributed,
            catalog.snapshot(),
            noop_constant_evaluator(),
            policy(),
            SqlPhysicalEmissionMode::OriginalNativeV1,
            control.clone(),
            PipelineDopDomain {
                min: 1,
                max: 8,
                requires_power_of_two: true,
            },
            ScanReadBudget {
                max_batch_rows: 64,
                max_batch_bytes: 1 << 20,
            },
            DEFAULT_COMPLETION_LIMITS,
        )
        .try_into_completion()
        .unwrap();
        let owner = SqlCompiler::start(request, &control)
            .unwrap()
            .into_complete()
            .unwrap()
            .into_plan();
        let calls=owner.plan().fragments().values().flat_map(|fragment|fragment.expressions().iter().filter_map(move|(_,expression)|matches!(&expression.kind,ExprKind::FunctionCall {function,..} if function.function_id.as_str()==novarocks_functions::builtin::value_conversion::VALUE_CONVERSION_FUNCTION_ID).then_some((fragment,expression)))).collect::<Vec<_>>();
        assert_eq!(calls.len(), 1, "{sql}");
        let (fragment, call) = calls[0];
        let ExprKind::FunctionCall { function, .. } = &call.kind else {
            unreachable!()
        };
        assert_eq!(
            function.overload.as_str(),
            "builtin.scalar/value_domain_conversion/utf8_to_largeint_null_invalid/v1"
        );
        assert_eq!(call.ty.logical_type(), ValueLogicalType::LargeInt);
        assert_eq!(call.ty.data_type(), &DataType::FixedSizeBinary(16));
        assert!(call.ty.nullable());
        let encoded = novarocks_plan_codec::encode_physical_plan_typed_pure(
            owner.plan(),
            &catalog,
            &novarocks_plan_codec::NoPhysicalV1PrivateFacts,
            false,
            compile_control(),
        )
        .unwrap();
        let sources = encoded
            .fragments
            .iter()
            .filter(|source| source.fragment_id == fragment.id().get())
            .collect::<Vec<_>>();
        assert_eq!(sources.len(), 1);
        let source = sources[0];
        let mut arena = ExprArena::default();
        decode_node(
            source,
            source.root.as_ref().unwrap(),
            &mut arena,
            &NativePlanDecodeContext::default(),
        )
        .unwrap();
        let mut roots = Vec::new();
        let mut index = 0;
        while let Some(node) = arena.node(novarocks_execution::exec::expr::ExprId(index)) {
            let id = novarocks_execution::exec::expr::ExprId(index);
            if matches!(node, novarocks_execution::exec::expr::ExprNode::Cast(..))
                && arena
                    .value_type(id)
                    .is_some_and(|ty| ty.logical_type() == ValueLogicalType::LargeInt)
            {
                roots.push(id);
            }
            index += 1;
        }
        assert_eq!(roots.len(), 1, "one exact validated default CAST author");
        let output = arena.eval(roots[0], &empty_input()).unwrap();
        let output = output
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .unwrap();
        let actual = if output.is_null(0) {
            None
        } else {
            Some(novarocks_functions::largeint::value_at(output, 0).unwrap())
        };
        assert_eq!(actual, expected, "{sql}");
    }
}

#[test]
fn default_regexp_count_real_sql_disabled_folding_keeps_exact_emitted_source_through_strong_decode()
{
    use novarocks_physical_plan::ScanReadBudget;
    use novarocks_sql::compiler::*;
    use novarocks_type_contract::RegexpCountPatternSource as Source;
    for (sql, expected_source, expected, fails) in [
        (
            "SELECT regexp_count('test string', '[0-9')",
            Source::NativeV1Utf8LiteralWhenPresent,
            None,
            true,
        ),
        (
            "SELECT regexp_count('aaa', 'a')",
            Source::NativeV1Utf8LiteralWhenPresent,
            Some(3),
            false,
        ),
        (
            "SELECT regexp_count(CAST(NULL AS VARCHAR), '[0-9')",
            Source::NativeV1Utf8LiteralWhenPresent,
            None,
            false,
        ),
        (
            "SELECT regexp_count('aaa', lower('[0-9'))",
            Source::Dynamic,
            None,
            false,
        ),
    ] {
        let catalog = build_builtin_engine_function_catalog().unwrap();
        let control = SqlCompileControl::unbounded();
        let request = SqlFinalPlanCompileRequest::new(
            PlanVersionId::try_new([88; 16]).unwrap(),
            SqlStatementInput::sql(sql),
            SqlCompileIntent::Query,
            SqlSessionContext {
                sql_semantics: Default::default(),
                current_catalog: None,
                current_database: "fixture".into(),
                optimizer_settings: SessionOptimizerSettings {
                    disabled_rules: vec!["FoldConstant".into()],
                    enable_materialized_view_rewrite: Some(false),
                    enable_common_subexpr_reuse: Some(false),
                    ..Default::default()
                },
            },
            SqlPlanningEnvironment::Distributed,
            catalog.snapshot(),
            noop_constant_evaluator(),
            policy(),
            SqlPhysicalEmissionMode::OriginalNativeV1,
            control.clone(),
            PipelineDopDomain {
                min: 1,
                max: 8,
                requires_power_of_two: true,
            },
            ScanReadBudget {
                max_batch_rows: 64,
                max_batch_bytes: 1 << 20,
            },
            DEFAULT_COMPLETION_LIMITS,
        )
        .try_into_completion()
        .unwrap();
        let owner = SqlCompiler::start(request, &control)
            .unwrap()
            .into_complete()
            .unwrap()
            .into_plan();
        let calls=owner.plan().fragments().values().flat_map(|fragment| fragment.expressions().iter().filter_map(move |(_,expression)| matches!(&expression.kind,ExprKind::FunctionCall {function,..} if function.function_id.as_str()=="builtin.scalar/regexp_count/v1").then_some((fragment,expression)))).collect::<Vec<_>>();
        assert_eq!(calls.len(), 1, "{sql}");
        let (fragment, call) = calls[0];
        let ExprKind::FunctionCall { args, .. } = &call.kind else {
            unreachable!()
        };
        let actual_pattern = fragment.expressions().get(args[1]).unwrap();
        assert_eq!(
            matches!(actual_pattern.kind, ExprKind::Constant(_)),
            expected_source.invalid_pattern_is_error(),
            "actual emitted definition, never equal-value inference: {sql}"
        );
        let encoded = novarocks_plan_codec::encode_physical_plan_typed_pure(
            owner.plan(),
            &catalog,
            &novarocks_plan_codec::NoPhysicalV1PrivateFacts,
            false,
            compile_control(),
        )
        .unwrap();
        let sources = encoded
            .fragments
            .iter()
            .filter(|source| source.fragment_id == fragment.id().get())
            .collect::<Vec<_>>();
        assert_eq!(sources.len(), 1);
        let source = sources[0];
        let mut arena = ExprArena::default();
        decode_node(
            source,
            source.root.as_ref().unwrap(),
            &mut arena,
            &NativePlanDecodeContext::default(),
        )
        .unwrap();
        let mut roots = Vec::new();
        let mut index = 0;
        while let Some(node) = arena.node(novarocks_execution::exec::expr::ExprId(index)) {
            if matches!(
                node,
                novarocks_execution::exec::expr::ExprNode::FunctionCall {
                    kind: novarocks_execution::exec::expr::function::FunctionKind::String(
                        "regexp_count"
                    ),
                    ..
                }
            ) {
                roots.push(novarocks_execution::exec::expr::ExprId(index));
            }
            index += 1;
        }
        assert_eq!(roots.len(), 1);
        assert_eq!(
            arena.regexp_count_pattern_source(roots[0]),
            Some(expected_source)
        );
        let output = arena.eval(roots[0], &empty_input());
        if fails {
            assert!(
                output
                    .unwrap_err()
                    .starts_with("Invalid regex expression: [0-9."),
                "{sql}"
            );
        } else {
            let output = output.unwrap();
            let output = output
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            assert_eq!(
                (!output.is_null(0)).then(|| output.value(0)),
                expected,
                "{sql}"
            );
        }
    }
}

#[test]
fn default_real_sql_nominal_largeint_datetime_uses_original_checked_i64_then_calendar() {
    use novarocks_physical_plan::ScanReadBudget;
    use novarocks_sql::compiler::*;
    for (sql, expected) in [
        ("SELECT CAST(-18446744073709551494 AS DATETIME)", None),
        ("SELECT CAST(18446744073709551615 AS DATETIME)", None),
        (
            "SELECT CAST(CAST(20240229123456 AS LARGEINT) AS DATETIME)",
            Some(1709210096000000i64),
        ),
    ] {
        let catalog = build_builtin_engine_function_catalog().unwrap();
        let control = SqlCompileControl::unbounded();
        let request = SqlFinalPlanCompileRequest::new(
            PlanVersionId::try_new([90; 16]).unwrap(),
            SqlStatementInput::sql(sql),
            SqlCompileIntent::Query,
            SqlSessionContext {
                sql_semantics: Default::default(),
                current_catalog: None,
                current_database: "fixture".into(),
                optimizer_settings: SessionOptimizerSettings {
                    disabled_rules: vec!["FoldConstant".into()],
                    enable_materialized_view_rewrite: Some(false),
                    enable_common_subexpr_reuse: Some(false),
                    ..Default::default()
                },
            },
            SqlPlanningEnvironment::Distributed,
            catalog.snapshot(),
            noop_constant_evaluator(),
            policy(),
            SqlPhysicalEmissionMode::OriginalNativeV1,
            control.clone(),
            PipelineDopDomain {
                min: 1,
                max: 8,
                requires_power_of_two: true,
            },
            ScanReadBudget {
                max_batch_rows: 64,
                max_batch_bytes: 1 << 20,
            },
            DEFAULT_COMPLETION_LIMITS,
        )
        .try_into_completion()
        .unwrap();
        let owner = SqlCompiler::start(request, &control)
            .unwrap()
            .into_complete()
            .unwrap()
            .into_plan();
        let calls=owner.plan().fragments().values().flat_map(|fragment|fragment.expressions().iter().filter_map(move|(_,expression)|matches!(&expression.kind,ExprKind::FunctionCall {function,..} if function.overload.as_str()=="builtin.scalar/value_domain_conversion/largeint_to_signed_null_overflow/v1").then_some((fragment,expression)))).collect::<Vec<_>>();
        assert_eq!(calls.len(), 1, "{sql}");
        let (fragment, call) = calls[0];
        let ExprKind::FunctionCall { function, .. } = &call.kind else {
            unreachable!()
        };
        assert_eq!(function.result_type.data_type(), &DataType::Int64);
        assert!(function.result_type.nullable());
        assert_eq!(
            function.result_type.logical_type(),
            ValueLogicalType::Physical
        );
        let [novarocks_type_contract::FunctionArgumentType::Value(source_type)] =
            function.argument_types.as_ref()
        else {
            panic!("one exact nominal argument")
        };
        assert_eq!(source_type.logical_type(), ValueLogicalType::LargeInt);
        assert_eq!(source_type.data_type(), &DataType::FixedSizeBinary(16));
        let encoded = novarocks_plan_codec::encode_physical_plan_typed_pure(
            owner.plan(),
            &catalog,
            &novarocks_plan_codec::NoPhysicalV1PrivateFacts,
            false,
            compile_control(),
        )
        .unwrap();
        let sources = encoded
            .fragments
            .iter()
            .filter(|source| source.fragment_id == fragment.id().get())
            .collect::<Vec<_>>();
        assert_eq!(sources.len(), 1);
        let source = sources[0];
        let mut arena = ExprArena::default();
        decode_node(
            source,
            source.root.as_ref().unwrap(),
            &mut arena,
            &NativePlanDecodeContext::default(),
        )
        .unwrap();
        let mut roots = Vec::new();
        let mut index = 0;
        while let Some(node) = arena.node(novarocks_execution::exec::expr::ExprId(index)) {
            let id = novarocks_execution::exec::expr::ExprId(index);
            if matches!(node, novarocks_execution::exec::expr::ExprNode::Cast(..))
                && arena.value_type(id).is_some_and(|ty| {
                    ty.data_type()
                        == &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
                })
            {
                roots.push(id);
            }
            index += 1;
        }
        assert_eq!(roots.len(), 1, "one original timestamp cast");
        let Some(novarocks_execution::exec::expr::ExprNode::Cast(child, _)) = arena.node(roots[0])
        else {
            unreachable!()
        };
        assert_eq!(
            arena.value_type(*child).unwrap().data_type(),
            &DataType::Int64
        );
        assert!(arena.value_type(*child).unwrap().nullable());
        let output = arena.eval(roots[0], &empty_input()).unwrap();
        let output = output
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(
            (!output.is_null(0)).then(|| output.value(0)),
            expected,
            "{sql}"
        );
    }
}

#[test]
fn default_real_sql_variant_get_parses_utf8_text_without_signing_variant_input() {
    use novarocks_physical_plan::ScanReadBudget;
    use novarocks_sql::compiler::{
        DEFAULT_COMPLETION_LIMITS, SessionOptimizerSettings, SqlCompileControl, SqlCompileIntent,
        SqlCompiler, SqlFinalPlanCompileRequest, SqlFunctionCatalog, SqlPhysicalEmissionMode,
        SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput,
        build_builtin_engine_function_catalog, noop_constant_evaluator,
    };
    let cases = [
        (
            r#"SELECT try_variant_get('{"a":99}', '$.a', 'bigint')"#,
            "try_variant_get",
            ValueLogicalType::Physical,
            Some(99),
        ),
        (
            r#"SELECT variant_get(parse_json('{"a":1.5}'), '$.a', 'bigint')"#,
            "variant_get",
            ValueLogicalType::Json,
            Some(1),
        ),
        (
            r#"SELECT variant_get('not-json', '$.a', 'bigint')"#,
            "variant_get",
            ValueLogicalType::Physical,
            None,
        ),
        (
            r#"SELECT try_variant_get('not-json', '$.a', 'bigint')"#,
            "try_variant_get",
            ValueLogicalType::Physical,
            None,
        ),
    ];
    for (sql, name, input_domain, expected) in cases {
        let control = SqlCompileControl::unbounded();
        let catalog = build_builtin_engine_function_catalog().unwrap();
        let request = SqlFinalPlanCompileRequest::new(
            PlanVersionId::try_new([97; 16]).unwrap(),
            SqlStatementInput::sql(sql),
            SqlCompileIntent::Query,
            SqlSessionContext {
                sql_semantics: Default::default(),
                current_catalog: None,
                current_database: "fixture".into(),
                optimizer_settings: SessionOptimizerSettings {
                    disabled_rules: vec!["FoldConstant".into()],
                    enable_materialized_view_rewrite: Some(false),
                    enable_common_subexpr_reuse: Some(false),
                    ..Default::default()
                },
            },
            SqlPlanningEnvironment::Distributed,
            catalog.snapshot(),
            noop_constant_evaluator(),
            policy(),
            SqlPhysicalEmissionMode::OriginalNativeV1,
            control.clone(),
            PipelineDopDomain {
                min: 1,
                max: 8,
                requires_power_of_two: true,
            },
            ScanReadBudget {
                max_batch_rows: 64,
                max_batch_bytes: 1 << 20,
            },
            DEFAULT_COMPLETION_LIMITS,
        )
        .try_into_completion()
        .unwrap();
        let owner = SqlCompiler::start(request, &control)
            .unwrap()
            .into_complete()
            .unwrap()
            .into_plan();
        let identity = format!("builtin.scalar/{name}/v1");
        let identity = identity.as_str();
        let calls = owner.plan().fragments().values().flat_map(|fragment| {
            fragment.expressions().iter().filter_map(move |(_, expression)| {
                matches!(&expression.kind, ExprKind::FunctionCall { function, .. } if function.function_id.as_str() == identity)
                    .then_some((fragment, expression))
            })
        }).collect::<Vec<_>>();
        assert_eq!(calls.len(), 1, "one original {name} call: {sql}");
        let (fragment, expression) = calls[0];
        let ExprKind::FunctionCall { function, args } = &expression.kind else {
            unreachable!()
        };
        let novarocks_type_contract::FunctionArgumentType::Value(input) =
            &function.argument_types[0]
        else {
            panic!("actual value")
        };
        assert_eq!(input.data_type(), &DataType::Utf8);
        assert_eq!(input.logical_type(), input_domain);
        assert_eq!(&fragment.expressions().get(args[0]).unwrap().ty, input);
        let original = fragment
            .call_requests()
            .get(PhysicalCallDefinition::Expression(expression.id))
            .unwrap();
        let StaticFunctionArgument::Value { value_type, .. } = &original.arguments[0] else {
            panic!("original argument")
        };
        assert_eq!(value_type, input);
        let encoded = novarocks_plan_codec::encode_physical_plan_typed_pure(
            owner.plan(),
            &catalog,
            &novarocks_plan_codec::NoPhysicalV1PrivateFacts,
            false,
            compile_control(),
        )
        .unwrap();
        let source = encoded
            .fragments
            .iter()
            .find(|source| source.fragment_id == fragment.id().get())
            .unwrap();
        let mut arena = ExprArena::default();
        decode_node(
            source,
            source.root.as_ref().unwrap(),
            &mut arena,
            &NativePlanDecodeContext::default(),
        )
        .unwrap();
        let mut roots = Vec::new();
        let mut index = 0;
        while let Some(node) = arena.node(novarocks_execution::exec::expr::ExprId(index)) {
            if matches!(node, novarocks_execution::exec::expr::ExprNode::FunctionCall {
                kind: novarocks_execution::exec::expr::function::FunctionKind::Variant(actual), ..
            } if *actual == name)
            {
                roots.push(novarocks_execution::exec::expr::ExprId(index));
            }
            index += 1;
        }
        assert_eq!(
            roots.len(),
            1,
            "complete frozen validation selects the actual default actor"
        );
        let result = arena.eval(roots[0], &empty_input());
        if let Some(expected) = expected {
            let result = result.unwrap();
            let result = result
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            assert_eq!(result.value(0), expected);
        } else {
            let error = result.unwrap_err();
            assert!(
                error.starts_with(&format!("{name}: invalid JSON input:")),
                "{error}"
            );
        }
    }
}

#[test]
fn default_real_sql_variant_typeof_keeps_declared_json_and_original_variant_profiles() {
    use novarocks_physical_plan::ScanReadBudget;
    use novarocks_sql::compiler::*;
    for (sql, expected) in [
        (r#"SELECT variant_typeof(parse_json('{}'))"#, Some("Object")),
        (r#"SELECT variant_typeof(parse_json('[]'))"#, Some("Array")),
        (r#"SELECT variant_typeof(parse_json('null'))"#, Some("Null")),
        (r#"SELECT variant_typeof(parse_json('128'))"#, Some("Int16")),
        (
            r#"SELECT variant_typeof(parse_json('2147483648'))"#,
            Some("Int64"),
        ),
        (
            r#"SELECT variant_typeof(parse_json('true'))"#,
            Some("Boolean(true)"),
        ),
        (
            r#"SELECT variant_typeof(parse_json(CAST(NULL AS VARCHAR)))"#,
            None,
        ),
        (r#"SELECT variant_typeof(CAST(NULL AS VARIANT))"#, None),
        (
            r#"SELECT variant_typeof(variant_get('{"a":129}', '$.a'))"#,
            Some("Int16"),
        ),
    ] {
        let catalog = build_builtin_engine_function_catalog().unwrap();
        let control = SqlCompileControl::unbounded();
        let request = SqlFinalPlanCompileRequest::new(
            PlanVersionId::try_new([98; 16]).unwrap(),
            SqlStatementInput::sql(sql),
            SqlCompileIntent::Query,
            SqlSessionContext {
                sql_semantics: Default::default(),
                current_catalog: None,
                current_database: "fixture".into(),
                optimizer_settings: SessionOptimizerSettings {
                    disabled_rules: vec!["FoldConstant".into()],
                    enable_materialized_view_rewrite: Some(false),
                    enable_common_subexpr_reuse: Some(false),
                    ..Default::default()
                },
            },
            SqlPlanningEnvironment::Distributed,
            catalog.snapshot(),
            noop_constant_evaluator(),
            policy(),
            SqlPhysicalEmissionMode::OriginalNativeV1,
            control.clone(),
            PipelineDopDomain {
                min: 1,
                max: 8,
                requires_power_of_two: true,
            },
            ScanReadBudget {
                max_batch_rows: 64,
                max_batch_bytes: 1 << 20,
            },
            DEFAULT_COMPLETION_LIMITS,
        )
        .try_into_completion()
        .unwrap();
        let owner = SqlCompiler::start(request, &control)
            .unwrap()
            .into_complete()
            .unwrap()
            .into_plan();
        let calls = owner
            .plan()
            .fragments()
            .values()
            .flat_map(|fragment| {
                fragment
                    .expressions()
                    .iter()
                    .filter_map(move |(_, expression)| {
                        matches!(&expression.kind, ExprKind::FunctionCall { function, .. }
                    if function.function_id.as_str() == "builtin.scalar/variant_typeof/v1")
                        .then_some((fragment, expression))
                    })
            })
            .collect::<Vec<_>>();
        assert_eq!(calls.len(), 1, "{sql}");
        let (fragment, expression) = calls[0];
        let ExprKind::FunctionCall { function, args } = &expression.kind else {
            unreachable!()
        };
        let novarocks_type_contract::FunctionArgumentType::Value(argument_type) =
            &function.argument_types[0]
        else {
            panic!("exact value argument")
        };
        assert!(matches!(
            argument_type.logical_type(),
            ValueLogicalType::Json | ValueLogicalType::Variant
        ));
        assert_eq!(
            &fragment.expressions().get(args[0]).unwrap().ty,
            argument_type
        );
        let encoded = novarocks_plan_codec::encode_physical_plan_typed_pure(
            owner.plan(),
            &catalog,
            &novarocks_plan_codec::NoPhysicalV1PrivateFacts,
            false,
            compile_control(),
        )
        .unwrap();
        let source = encoded
            .fragments
            .iter()
            .find(|source| source.fragment_id == fragment.id().get())
            .unwrap();
        let mut arena = ExprArena::default();
        decode_node(
            source,
            source.root.as_ref().unwrap(),
            &mut arena,
            &NativePlanDecodeContext::default(),
        )
        .unwrap();
        let mut roots = vec![];
        let mut index = 0;
        while let Some(node) = arena.node(novarocks_execution::exec::expr::ExprId(index)) {
            if matches!(
                node,
                novarocks_execution::exec::expr::ExprNode::FunctionCall {
                    kind: novarocks_execution::exec::expr::function::FunctionKind::Variant(
                        "variant_typeof"
                    ),
                    ..
                }
            ) {
                roots.push(novarocks_execution::exec::expr::ExprId(index));
            }
            index += 1;
        }
        assert_eq!(roots.len(), 1);
        let result = arena.eval(roots[0], &empty_input()).unwrap();
        let result = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            (!result.is_null(0)).then(|| result.value(0)),
            expected,
            "{sql}"
        );
    }
}

#[test]
fn default_real_sql_union_of_json_and_typed_variant_null_completes() {
    use novarocks_physical_plan::ScanReadBudget;
    use novarocks_sql::compiler::*;
    for sql in [
        r#"WITH source_values AS (
            SELECT 1 AS id, parse_json('{"a":1,"b":"x"}') AS v
            UNION ALL SELECT 2, parse_json('{"a":99,"b":"y"}')
            UNION ALL SELECT 3, parse_json('{"b":"no-a"}')
            UNION ALL SELECT 4, parse_json('{"a":1.5}')
            UNION ALL SELECT 5, CAST(NULL AS VARIANT)
            UNION ALL SELECT 6, parse_json('{"a":"abc"}')
        ) SELECT id, try_variant_get(v, '$.a', 'bigint') FROM source_values ORDER BY id"#,
        r#"SELECT parse_json('{"a":1}') AS v UNION ALL SELECT CAST(NULL AS VARIANT)"#,
    ] {
        let catalog = build_builtin_engine_function_catalog().unwrap();
        let control = SqlCompileControl::unbounded();
        let request = SqlFinalPlanCompileRequest::new(
            PlanVersionId::try_new([98; 16]).unwrap(),
            SqlStatementInput::sql(sql),
            SqlCompileIntent::Query,
            SqlSessionContext {
                sql_semantics: Default::default(),
                current_catalog: None,
                current_database: "fixture".into(),
                optimizer_settings: SessionOptimizerSettings {
                    disabled_rules: vec!["FoldConstant".into()],
                    enable_materialized_view_rewrite: Some(false),
                    enable_common_subexpr_reuse: Some(false),
                    ..Default::default()
                },
            },
            SqlPlanningEnvironment::Distributed,
            catalog.snapshot(),
            noop_constant_evaluator(),
            policy(),
            SqlPhysicalEmissionMode::OriginalNativeV1,
            control.clone(),
            PipelineDopDomain {
                min: 1,
                max: 8,
                requires_power_of_two: true,
            },
            ScanReadBudget {
                max_batch_rows: 64,
                max_batch_bytes: 1 << 20,
            },
            DEFAULT_COMPLETION_LIMITS,
        )
        .try_into_completion()
        .unwrap();
        let owner = SqlCompiler::start(request, &control)
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
            .into_complete()
            .unwrap()
            .into_plan();
        assert!(!owner.plan().fragments().is_empty(), "{sql}");
    }
}

#[test]
fn default_real_sql_map_null_values_keep_valid_null_storage() {
    use novarocks_physical_plan::ScanReadBudget;
    use novarocks_sql::compiler::*;
    for sql in [
        "SELECT map{1:NULL}[1]",
        "SELECT map{1:NULL}[2]",
        "SELECT element_at(map{1:NULL}, 1)",
    ] {
        let catalog = build_builtin_engine_function_catalog().unwrap();
        let control = SqlCompileControl::unbounded();
        let request = SqlFinalPlanCompileRequest::new(
            PlanVersionId::try_new([98; 16]).unwrap(),
            SqlStatementInput::sql(sql),
            SqlCompileIntent::Query,
            SqlSessionContext {
                sql_semantics: Default::default(),
                current_catalog: None,
                current_database: "fixture".into(),
                optimizer_settings: SessionOptimizerSettings {
                    disabled_rules: vec!["FoldConstant".into()],
                    enable_materialized_view_rewrite: Some(false),
                    enable_common_subexpr_reuse: Some(false),
                    ..Default::default()
                },
            },
            SqlPlanningEnvironment::Distributed,
            catalog.snapshot(),
            noop_constant_evaluator(),
            policy(),
            SqlPhysicalEmissionMode::OriginalNativeV1,
            control.clone(),
            PipelineDopDomain {
                min: 1,
                max: 8,
                requires_power_of_two: true,
            },
            ScanReadBudget {
                max_batch_rows: 64,
                max_batch_bytes: 1 << 20,
            },
            DEFAULT_COMPLETION_LIMITS,
        )
        .try_into_completion()
        .unwrap();
        let owner = SqlCompiler::start(request, &control)
            .unwrap()
            .into_complete()
            .unwrap()
            .into_plan();
        let encoded = novarocks_plan_codec::encode_physical_plan_typed_pure(
            owner.plan(),
            &catalog,
            &novarocks_plan_codec::NoPhysicalV1PrivateFacts,
            false,
            compile_control(),
        )
        .unwrap();
        let mut lookups = 0;
        for source in &encoded.fragments {
            let fragment = owner
                .plan()
                .fragments()
                .values()
                .find(|fragment| fragment.id().get() == source.fragment_id)
                .unwrap();
            if !fragment.expressions().iter().any(|(_, expression)| {
                matches!(&expression.kind, novarocks_physical_plan::ExprKind::FunctionCall { function, .. }
                    if function.function_id.as_str().contains("map_element_at"))
            }) {
                continue;
            }
            let mut arena = ExprArena::default();
            decode_node(
                source,
                source.root.as_ref().unwrap(),
                &mut arena,
                &NativePlanDecodeContext::default(),
            )
            .unwrap();
            let mut index = 0;
            while let Some(node) = arena.node(novarocks_execution::exec::expr::ExprId(index)) {
                if matches!(node, novarocks_execution::exec::expr::ExprNode::FunctionCall {
                    kind: novarocks_execution::exec::expr::function::FunctionKind::Map(name), ..
                } if name.ends_with("element_at"))
                {
                    let value = arena
                        .eval(
                            novarocks_execution::exec::expr::ExprId(index),
                            &empty_input(),
                        )
                        .unwrap();
                    assert_eq!(value.data_type(), &DataType::Null, "{sql}");
                    assert_eq!(value.len(), 1, "{sql}");
                    assert_eq!(value.logical_null_count(), 1, "{sql}");
                    assert!(value.to_data().nulls().is_none(), "{sql}");
                    lookups += 1;
                }
                index += 1;
            }
        }
        assert_eq!(lookups, 1, "{sql}");
    }
}

#[test]
fn default_real_sql_largeint_to_decimal_keeps_exact_numeric_admission() {
    use novarocks_physical_plan::ScanReadBudget;
    use novarocks_sql::compiler::*;
    for (sql, expected_coefficient) in [
        (
            "SELECT CAST(CAST('9223372036854775808' AS LARGEINT) AS DECIMAL(38,0))",
            "9223372036854775808",
        ),
        (
            "SELECT CAST(CAST('170141183460469231731687303715884105727' AS LARGEINT) AS DECIMAL(41,2))",
            "17014118346046923173168730371588410572700",
        ),
        (
            "SELECT field(CAST('170141183460469231731687303715884105727' AS LARGEINT), CAST('170141183460469231731687303715884105727.00' AS DECIMAL(41,2)))",
            "17014118346046923173168730371588410572700",
        ),
    ] {
        let catalog = build_builtin_engine_function_catalog().unwrap();
        let control = SqlCompileControl::unbounded();
        let request = SqlFinalPlanCompileRequest::new(
            PlanVersionId::try_new([98; 16]).unwrap(),
            SqlStatementInput::sql(sql),
            SqlCompileIntent::Query,
            SqlSessionContext {
                sql_semantics: Default::default(),
                current_catalog: None,
                current_database: "fixture".into(),
                optimizer_settings: SessionOptimizerSettings {
                    disabled_rules: vec!["FoldConstant".into()],
                    enable_materialized_view_rewrite: Some(false),
                    enable_common_subexpr_reuse: Some(false),
                    ..Default::default()
                },
            },
            SqlPlanningEnvironment::Distributed,
            catalog.snapshot(),
            noop_constant_evaluator(),
            policy(),
            SqlPhysicalEmissionMode::OriginalNativeV1,
            control.clone(),
            PipelineDopDomain {
                min: 1,
                max: 8,
                requires_power_of_two: true,
            },
            ScanReadBudget {
                max_batch_rows: 64,
                max_batch_bytes: 1 << 20,
            },
            DEFAULT_COMPLETION_LIMITS,
        )
        .try_into_completion()
        .unwrap();
        let owner = SqlCompiler::start(request, &control)
            .unwrap()
            .into_complete()
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
            .into_plan();
        let calls = owner
            .plan()
            .fragments()
            .values()
            .flat_map(|fragment| {
                fragment
                    .expressions()
                    .iter()
                    .filter_map(move |(_, expression)| {
                        matches!(&expression.kind, ExprKind::FunctionCall { function, .. }
                    if function.overload.as_str().contains("/largeint_to_decimal"))
                        .then_some((fragment, expression))
                    })
            })
            .collect::<Vec<_>>();
        assert_eq!(calls.len(), 1, "{sql}");
        let (fragment, expression) = calls[0];
        let ExprKind::FunctionCall { function, .. } = &expression.kind else {
            unreachable!()
        };
        assert_eq!(
            function.overload.as_str(),
            "builtin.scalar/value_domain_conversion/largeint_to_decimal_exact/v1"
        );
        let novarocks_type_contract::FunctionArgumentType::Value(source_type) =
            &function.argument_types[0]
        else {
            panic!("one exact value source")
        };
        assert_eq!(source_type.logical_type(), ValueLogicalType::LargeInt);
        assert_eq!(source_type.data_type(), &DataType::FixedSizeBinary(16));
        assert_eq!(expression.ty.logical_type(), ValueLogicalType::Physical);
        assert!(matches!(
            expression.ty.data_type(),
            DataType::Decimal128(..) | DataType::Decimal256(..)
        ));
        let encoded = novarocks_plan_codec::encode_physical_plan_typed_pure(
            owner.plan(),
            &catalog,
            &novarocks_plan_codec::NoPhysicalV1PrivateFacts,
            false,
            compile_control(),
        )
        .unwrap();
        let source = encoded
            .fragments
            .iter()
            .find(|source| source.fragment_id == fragment.id().get())
            .unwrap();
        let mut arena = ExprArena::default();
        decode_node(
            source,
            source.root.as_ref().unwrap(),
            &mut arena,
            &NativePlanDecodeContext::default(),
        )
        .unwrap();
        let mut roots = Vec::new();
        let mut index = 0;
        while let Some(node) = arena.node(novarocks_execution::exec::expr::ExprId(index)) {
            let id = novarocks_execution::exec::expr::ExprId(index);
            if matches!(node, novarocks_execution::exec::expr::ExprNode::Cast(..))
                && arena.value_type(id).is_some_and(|ty| ty == &expression.ty)
            {
                roots.push(id);
            }
            index += 1;
        }
        // FIELD also has a text-to-decimal candidate; only the LARGEINT source
        // identifies the conversion whose exact coefficient is asserted here.
        let roots = roots
            .into_iter()
            .filter(|id| {
                let Some(novarocks_execution::exec::expr::ExprNode::Cast(child, _)) =
                    arena.node(*id)
                else {
                    return false;
                };
                arena
                    .value_type(*child)
                    .is_some_and(|ty| ty.logical_type() == ValueLogicalType::LargeInt)
            })
            .collect::<Vec<_>>();
        assert_eq!(roots.len(), 1, "{sql}");
        let value = arena.eval(roots[0], &empty_input()).unwrap();
        assert_eq!(value.len(), 1);
        assert!(!value.is_null(0), "{sql}");
        let coefficient = match value.data_type() {
            DataType::Decimal128(..) => value
                .as_any()
                .downcast_ref::<arrow::array::Decimal128Array>()
                .unwrap()
                .value(0)
                .to_string(),
            DataType::Decimal256(..) => value
                .as_any()
                .downcast_ref::<arrow::array::Decimal256Array>()
                .unwrap()
                .value(0)
                .to_string(),
            _ => panic!("exact selected decimal carrier"),
        };
        assert_eq!(coefficient, expected_coefficient, "{sql}");
    }
}
