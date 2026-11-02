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

//! Permanent source probes on the real builders, without owner preparation.

use super::*;
use std::sync::Mutex;

use arrow::array::StringArray;
use novarocks_physical_plan::BoundFunction;
use novarocks_plan_codec::physical_package_v2::{
    FragmentDefinitionSource, visit_fragment_definitions_observed,
};
use novarocks_plan_codec::physical_type_v2::{
    PackageTypeProjectionLimits, TypeProjectionLimits, decode_type_table,
    encode_borrowed_type_table_writer_sources_in,
};
use novarocks_type_contract::{CompileControlError, PureCompileControl, ValueLogicalType};

struct SourceFixture {
    fragment: Fragment,
    pools: ConstantPools,
    first: ExprId,
    later: ExprId,
    source: ConstantReference,
}

fn scalar_binding(name: &str, request: &[FunctionArgument]) -> BoundFunction {
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let bound = actual
        .resolve_bound_user(
            name,
            FunctionKind::Scalar,
            FunctionBindingRequest {
                arguments: request,
                logical_argument_count: request.len(),
                expected_result_type: None,
            },
            &FixtureControl,
        )
        .unwrap();
    let FunctionResultType::Scalar(result) = bound.selected.result_type else {
        panic!("actual scalar resolver must select a scalar result")
    };
    let mut function = BoundFunction::from_exact_signature(
        bound.function_id,
        bound.selected.overload,
        FunctionKind::Scalar,
        bound.selected.argument_types,
        result,
    );
    function.legacy_metadata = Some(LegacyBindingMetadata {
        volatility: bound.semantics.volatility,
        argument_evaluation: bound.semantics.argument_evaluation,
        failure_behavior: bound.semantics.failure_behavior,
        intrinsic_row_error: bound.semantics.intrinsic_row_error,
        semantic_parameters: Box::default(),
    });
    function
}

fn source_fixture() -> SourceFixture {
    let mut builder = FragmentBuilder::new(SOLE);
    let input = builder.reserve_node_id().unwrap();
    let mut pools = Pools::new();
    let (key, _lists) = values(
        &mut builder,
        &mut pools,
        input,
        &[(1, vec![Some(vec![Some(7), None])])],
        1,
    );
    let project = builder.reserve_node_id().unwrap();
    let utf8 = FunctionValueType::new(DataType::Utf8, true);
    let pool = ConstantPool::try_new(
        Arc::new(
            Field::new("original_payload", DataType::Utf8, true).with_metadata(
                std::collections::HashMap::<String, String>::from([(
                    "provider.field_id".into(),
                    "41".into(),
                )]),
            ),
        ),
        utf8.clone(),
        arrow::array::Array::to_data(&StringArray::from(vec![None, Some("key")])),
        constant_policy(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap();
    let pool_id = ConstantPoolId::new(900);
    let source = ConstantReference {
        pool: pool_id,
        ordinal: 0,
    };
    let key_source = ConstantReference {
        pool: pool_id,
        ordinal: 1,
    };
    let arguments = [
        FunctionArgument::Value {
            value_type: utf8.clone(),
            constant: Some(pool.value(0).unwrap()),
        },
        FunctionArgument::Value {
            value_type: utf8.clone(),
            constant: Some(pool.value(1).unwrap()),
        },
    ];
    let function = scalar_binding("aes_encrypt", &arguments);
    let first_type = function.result_type.clone();
    pools.0.insert(pool_id, pool).unwrap();
    let plaintext = builder
        .add_expression(project, utf8.clone(), ExprKind::Constant(source))
        .unwrap();
    let cipher_key = builder
        .add_expression(project, utf8.clone(), ExprKind::Constant(key_source))
        .unwrap();
    let first = builder
        .add_expression(
            project,
            function.result_type.clone(),
            ExprKind::FunctionCall {
                function,
                args: Box::from([plaintext, cipher_key]),
            },
        )
        .unwrap();
    let numeric_request = [FunctionArgument::Value {
        value_type: int64(false),
        constant: None,
    }];
    let function = scalar_binding("abs", &numeric_request);
    let later_type = function.result_type.clone();
    let read = builder
        .add_expression(project, int64(false), ExprKind::Value(key))
        .unwrap();
    let later = builder
        .add_expression(
            project,
            function.result_type.clone(),
            ExprKind::FunctionCall {
                function,
                args: Box::from([read]),
            },
        )
        .unwrap();

    let first_output = builder
        .add_value(
            first_type,
            ValueOrigin::Expr {
                node: project,
                expr: first,
            },
        )
        .unwrap();
    let later_output = builder
        .add_value(
            later_type,
            ValueOrigin::Expr {
                node: project,
                expr: later,
            },
        )
        .unwrap();
    builder
        .add_project(
            project,
            input,
            Box::from([(first, first_output), (later, later_output)]),
            Box::from([first_output, later_output]),
        )
        .unwrap();
    let fragment = builder
        .finish_definition(
            project,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let requests = vec![
        (
            PhysicalCallDefinition::Expression(first),
            PhysicalCallRequest {
                arguments: Box::from([
                    StaticFunctionArgument::Value {
                        value_type: utf8.clone(),
                        constant: Some(source),
                    },
                    StaticFunctionArgument::Value {
                        value_type: utf8,
                        constant: Some(key_source),
                    },
                ]),
                logical_argument_count: 2,
                expected_result_type: None,
                constant_policy: constant_policy(),
            },
        ),
        (
            PhysicalCallDefinition::Expression(later),
            PhysicalCallRequest {
                arguments: Box::from([StaticFunctionArgument::Value {
                    value_type: int64(false),
                    constant: None,
                }]),
                logical_argument_count: 1,
                expected_result_type: None,
                constant_policy: constant_policy(),
            },
        ),
    ];
    SourceFixture {
        fragment: fragment
            .with_call_requests_observed(requests, &FixtureControl)
            .unwrap(),
        pools: pools.0,
        first,
        later,
        source,
    }
}

fn source_count(fragment: &Fragment) -> usize {
    fragment.values().len()
        + fragment.expressions().iter().count()
        + fragment.call_requests().entries().len()
}

#[test]
fn all_definition_source_preserves_actual_project_bindings_requests_and_typed_null_pool() {
    let fixture = source_fixture();
    let fragment = &fixture.fragment;
    let mut work = CompileCheckpoints::try_new(&FixtureControl, CompilePhase::Encode).unwrap();
    let mut calls = Vec::new();
    let mut definitions = Vec::new();
    visit_fragment_definitions_observed::<CompileControlError>(
        fragment,
        &mut work,
        |source, work| {
            match source {
                FragmentDefinitionSource::Value { id, value } => {
                    assert!(std::ptr::eq(value, fragment.values().get(&id).unwrap()));
                }
                FragmentDefinitionSource::Expression { id, expression } => {
                    assert!(std::ptr::eq(
                        expression,
                        fragment.expressions().get(id).unwrap()
                    ));
                    if let ExprKind::FunctionCall { function, .. } = &expression.kind {
                        calls.push((
                            id,
                            function.function_id.clone(),
                            function.overload.clone(),
                            function.argument_types.clone(),
                            function.result_type.clone(),
                        ));
                    }
                }
                FragmentDefinitionSource::Request {
                    definition,
                    request,
                } => {
                    assert!(std::ptr::eq(
                        request,
                        fragment.call_requests().get(definition).unwrap()
                    ));
                    definitions.push(definition);
                    assert!(request.expected_result_type.is_none());
                    if definition == PhysicalCallDefinition::Expression(fixture.first) {
                        assert_eq!(request.logical_argument_count, 2);
                        let StaticFunctionArgument::Value {
                            constant,
                            value_type,
                        } = &request.arguments[0]
                        else {
                            panic!("actual scalar argument")
                        };
                        assert_eq!(*constant, Some(fixture.source));
                        assert_eq!(*value_type, FunctionValueType::new(DataType::Utf8, true));
                    }
                    if definition == PhysicalCallDefinition::Expression(fixture.later) {
                        assert!(matches!(
                            &request.arguments[0],
                            StaticFunctionArgument::Value { constant: None, .. }
                        ));
                    }
                }
            }
            work.step()
        },
    )
    .unwrap();
    assert_eq!(
        calls.iter().map(|call| call.0).collect::<Vec<_>>(),
        [fixture.first, fixture.later]
    );
    for (id, function, overload, arguments, result) in calls {
        let ExprKind::FunctionCall {
            function: original, ..
        } = &fragment.expressions().get(id).unwrap().kind
        else {
            unreachable!()
        };
        assert_eq!(function, original.function_id);
        assert_eq!(overload, original.overload);
        assert_eq!(arguments, original.argument_types);
        assert_eq!(result, original.result_type);
    }
    assert_eq!(
        definitions,
        [
            PhysicalCallDefinition::Expression(fixture.first),
            PhysicalCallDefinition::Expression(fixture.later),
        ]
    );
    let constant = fixture
        .pools
        .resolve_source_observed(fixture.source, &mut work)
        .unwrap();
    assert_eq!(constant.ordinal(), 0);
    assert!(
        constant
            .is_null_observed(CompilePhase::Encode, &FixtureControl)
            .unwrap()
    );
    assert_eq!(constant.field().name(), "original_payload");
    assert_eq!(
        constant
            .field()
            .metadata()
            .get("provider.field_id")
            .unwrap(),
        "41"
    );
    work.finish().unwrap();
}

#[derive(Default)]
struct Meter {
    events: Mutex<Vec<u32>>,
    fail: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Meter {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Encode);
        assert!(units <= 256);
        let mut events = self.events.lock().unwrap();
        let ordinal = events.len();
        events.push(units);
        if let Some((at, cause)) = self.fail.clone()
            && at == ordinal
        {
            return Err(cause);
        }
        Ok(())
    }
}

#[test]
fn all_definition_source_does_not_add_step_scope_or_success_footer() {
    let fixture = source_fixture();
    let meter = Meter::default();
    let mut work = CompileCheckpoints::try_new(&meter, CompilePhase::Encode).unwrap();
    let mut captures = 0;
    visit_fragment_definitions_observed::<CompileControlError>(
        &fixture.fragment,
        &mut work,
        |_, work| {
            captures += 1;
            work.step()
        },
    )
    .unwrap();
    assert_eq!(captures, source_count(&fixture.fragment));
    assert_eq!(*meter.events.lock().unwrap(), [0]);
    work.finish().unwrap();
    assert_eq!(*meter.events.lock().unwrap(), [0, captures as u32]);
}

#[test]
fn all_definition_source_first_control_cause_stops_every_callback_prefix() {
    let fixture = source_fixture();
    let total = source_count(&fixture.fragment);
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for stop in 1..=total {
            let meter = Meter {
                events: Mutex::new(Vec::new()),
                fail: Some((stop, cause.clone())),
            };
            let mut work = CompileCheckpoints::try_new(&meter, CompilePhase::Encode).unwrap();
            let mut captures = 0;
            let result = visit_fragment_definitions_observed::<CompileControlError>(
                &fixture.fragment,
                &mut work,
                |_, work| {
                    captures += 1;
                    work.step()?;
                    work.flush()
                },
            );
            assert_eq!(result, Err(cause.clone()));
            assert_eq!(captures, stop);
            let events = meter.events.lock().unwrap().clone();
            assert_eq!(events.len(), stop + 1);
            assert_eq!(work.flush(), Err(cause.clone()));
            assert_eq!(*meter.events.lock().unwrap(), events);
        }
    }
}

#[test]
fn all_definition_source_uses_real_unnest_relational_binding_author() {
    let mut builder = FragmentBuilder::new(SOLE);
    let mut pools = Pools::new();
    let input = builder.reserve_node_id().unwrap();
    let (key, lists) = values(
        &mut builder,
        &mut pools,
        input,
        &[(1, vec![Some(vec![Some(2)])])],
        1,
    );
    let (table, _) = add_table_function(&mut builder, input, key, &lists, &Spec::unnest(false));
    let fragment = finish(builder, table, FragmentSink::Result, 1);
    let mut work = CompileCheckpoints::try_new(&FixtureControl, CompilePhase::Encode).unwrap();
    let mut requests = Vec::new();
    visit_fragment_definitions_observed::<CompileControlError>(
        &fragment,
        &mut work,
        |source, work| {
            if let FragmentDefinitionSource::Request {
                definition,
                request,
            } = source
            {
                requests.push((definition, request.logical_argument_count));
            }
            work.step()
        },
    )
    .unwrap();
    let mut bindings = Vec::new();
    novarocks_physical_plan::visit_relational_calls_observed::<FrozenCallError>(
        &fragment,
        &mut work,
        |site, binding, work| {
            let PhysicalCallBinding::Table(function) = binding else {
                panic!("real UNNEST table binding")
            };
            bindings.push((
                PhysicalCallDefinition::Relational(site),
                function.argument_types.len(),
                function.function_id.clone(),
                function.overload.clone(),
            ));
            work.step()?;
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(bindings.len(), 1);
    assert_eq!(requests[0], (bindings[0].0, bindings[0].1));
    let NodeKind::TableFunction { function, .. } = &fragment.nodes().get(&table).unwrap().kind
    else {
        unreachable!()
    };
    assert_eq!(bindings[0].2, function.function_id);
    assert_eq!(bindings[0].3, function.overload);
    work.finish().unwrap();
}

#[test]
#[allow(deprecated)]
fn all_definition_source_borrowed_type_encoder_keeps_nested_provider_and_nominal_identity() {
    // Provider attributes and dictionary descriptors use SourceFieldTable.
    // Their exact original (-37, ordered=true) source round-trip is covered by
    // physical_type_v2::source_fields_tests::original_nested_provider_source_attributes_and_dictionary_identity.
    let dictionary = Arc::new(Field::new("dictionary", DataType::Utf8, false));
    let nested = Arc::new(Field::new(
        "nested",
        DataType::List(Arc::new(Field::new("", DataType::Utf8, true))),
        true,
    ));
    let root = Arc::new(Field::new(
        "original_root",
        DataType::Struct(vec![dictionary.clone(), nested].into()),
        false,
    ));
    let physical = FunctionValueType::new(root.data_type().clone(), false);
    let nominal = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    let values = [(5, &physical), (900, &nominal)];
    let fields = [(17, &root), (9, &dictionary)];
    let limits = PackageTypeProjectionLimits {
        max_definitions: 64,
        max_expanded_nodes: 128,
        max_string_bytes: 65536,
        max_allocation_requests: 16384,
        max_allocation_request_bytes: 16 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 32 * 1024 * 1024,
        max_work: 128 * 1024 * 1024,
    };
    let mut work = CompileCheckpoints::try_new(&FixtureControl, CompilePhase::Encode).unwrap();
    let mut last_admission = None;
    let encoded = encode_borrowed_type_table_writer_sources_in(
        &values,
        &fields,
        &[],
        64 * 1024,
        limits,
        &mut |facts| {
            last_admission = Some(*facts);
            Ok(())
        },
        &mut work,
    )
    .unwrap_or_else(|error| {
        panic!("borrowed type projection failed: {error:?}; last admission: {last_admission:?}")
    });
    work.finish().unwrap();
    let facts = last_admission.expect("original encoder must admit its completed projection");
    // The old >64 count included retired inline source descriptors. Keep
    // the real recursive expansion and the same numeric receiving ceiling.
    assert!(facts.expanded_node_count > values.len() + fields.len());
    assert!(facts.expanded_node_count <= limits.max_expanded_nodes);
    let decoded = decode_type_table(
        encoded.as_wire(),
        TypeProjectionLimits {
            max_definitions: 1024,
            max_expanded_nodes: 4096,
            max_string_bytes: 65536,
        },
        &FixtureControl,
    )
    .unwrap();
    assert_eq!(decoded.value_type(5).unwrap(), &physical);
    assert_eq!(decoded.value_type(900).unwrap(), &nominal);
    assert!(novarocks_type_contract::arrow_fields_exact(
        &root,
        decoded.field(17).unwrap()
    ));
    assert!(novarocks_type_contract::arrow_fields_exact(
        &dictionary,
        decoded.field(9).unwrap()
    ));
    assert_eq!(decoded.field(9).unwrap().dict_id(), None);
    assert_eq!(decoded.field(9).unwrap().dict_is_ordered(), None);
    assert!(decoded.field(17).unwrap().metadata().as_arc().is_none());
    assert!(decoded.field(9).unwrap().metadata().as_arc().is_none());
    assert_eq!(
        decoded.value_type(900).unwrap().logical_type(),
        ValueLogicalType::LargeInt
    );
    assert!(!decoded.field(17).unwrap().is_nullable());
    let DataType::Struct(children) = decoded.field(17).unwrap().data_type() else {
        panic!("Struct")
    };
    assert_eq!(children[0].name(), "dictionary");
    assert!(!children[0].is_nullable());
    assert_eq!(children[1].name(), "nested");
    assert!(children[1].is_nullable());
}

#[test]
fn all_definition_source_original_publication_rejects_unreachable_definition() {
    let mut builder = FragmentBuilder::new(SOLE);
    let mut pools = Pools::new();
    let input = builder.reserve_node_id().unwrap();
    values(
        &mut builder,
        &mut pools,
        input,
        &[(1, vec![Some(vec![Some(2)])])],
        1,
    );
    builder
        .add_expression(
            input,
            int64(false),
            ExprKind::Literal(LiteralValue::Int64(99)),
        )
        .unwrap();
    let error = builder
        .finish_definition(
            input,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("expression arena contains definitions unreachable from operator roots")
    );
}
