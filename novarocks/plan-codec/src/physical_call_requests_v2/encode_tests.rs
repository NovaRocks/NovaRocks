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
use crate::physical_type_v2::{TypeProjectionLimits, encode_type_table_sources};
use arrow::array::{Array, Int64Array};
use arrow::datatypes::{DataType, Field};
use novarocks_physical_plan as p;
use novarocks_type_contract::{
    FunctionArgumentEvaluation, FunctionArgumentType, FunctionFailureBehavior, FunctionId,
    FunctionIntrinsicRowError, FunctionKind, FunctionOverloadId, FunctionVolatility,
};
use std::sync::{Arc, Mutex};

const SOURCE: usize = 1 << 20;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Mutex<Option<(usize, CompileControlError)>>,
    active: Mutex<bool>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        if !*self.active.lock().unwrap() {
            return Ok(());
        }
        assert_eq!(phase, CompilePhase::Encode);
        let stop = self.stop.lock().unwrap().clone();
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((end, _)) = stop {
            assert!(at <= end, "callback after first refusal");
        }
        trace.push((phase, units));
        if let Some((end, cause)) = stop
            && end == at
        {
            return Err(cause);
        }
        Ok(())
    }
}
impl Control {
    fn arm(&self, stop: Option<(usize, CompileControlError)>) {
        self.trace.lock().unwrap().clear();
        *self.stop.lock().unwrap() = stop;
        *self.active.lock().unwrap() = true;
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
fn admission() -> p::ConstantPolicy {
    p::ConstantPolicy {
        max_rows: 4096,
        max_array_nodes: 4096,
        max_logical_elements: 1 << 20,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 1 << 28,
        max_library_validation_bytes: 1 << 28,
    }
}
fn data_policy() -> p::ConstantPolicy {
    p::ConstantPolicy {
        max_rows: 0,
        max_array_nodes: u64::MAX,
        max_logical_elements: 2,
        max_retained_buffer_bytes: 3,
        max_type_depth: 0,
        max_type_nodes: 5,
        max_dictionary_depth: u32::MAX,
        max_metadata_bytes: 7,
        max_library_validation_work: 8,
        max_library_validation_bytes: 9,
    }
}
fn limits() -> CallRequestProjectionLimits {
    CallRequestProjectionLimits {
        max_definitions: 4096,
        max_type_references: 32768,
        max_request_bytes: 16 << 20,
        max_allocation_requests: 16384,
        max_coexisting_source_and_request_bytes: 32 << 20,
        max_work: 1 << 30,
    }
}
fn types_limits() -> TypeProjectionLimits {
    TypeProjectionLimits {
        max_definitions: 64,
        max_expanded_nodes: 4096,
        max_string_bytes: 1 << 20,
    }
}
fn int() -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, true)
}
fn nested(name: &str) -> FunctionValueType {
    FunctionValueType::new(
        DataType::Struct(
            vec![Arc::new(
                Field::new(name, DataType::Int64, false).with_metadata(
                    [("origin".to_owned(), "full-field".to_owned())]
                        .into_iter()
                        .collect::<std::collections::HashMap<String, String>>(),
                ),
            )]
            .into(),
        ),
        false,
    )
}
fn binding(args: Vec<FunctionArgumentType>) -> p::BoundFunction {
    p::BoundFunction {
        function_id: FunctionId::try_new("test/codec-original-request").unwrap(),
        overload: FunctionOverloadId::try_new("test/codec-original-request/v1").unwrap(),
        kind: FunctionKind::Scalar,
        argument_types: novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
            args.into_boxed_slice(),
        ),
        result_type: int(),

        legacy_metadata: Some(novarocks_physical_plan::LegacyBindingMetadata {
            volatility: FunctionVolatility::Immutable,
            argument_evaluation: FunctionArgumentEvaluation::Eager,
            failure_behavior: FunctionFailureBehavior::Propagate,
            intrinsic_row_error: FunctionIntrinsicRowError::NoRowError,
            semantic_parameters: Box::default(),
        }),
    }
}
/// Real checked structural source; no installed function/provenance claim.
pub(crate) struct Fixture {
    pub(crate) fragment: p::Fragment,
    pub(crate) roots: Vec<(u32, FunctionValueType)>,
    pub(crate) pools: ConstantPools,
}
pub(crate) fn fixture() -> Fixture {
    fixture_with_reference(p::ConstantReference {
        pool: p::ConstantPoolId::new(u32::MAX),
        ordinal: 1,
    })
}
fn fixture_with_reference(reference: p::ConstantReference) -> Fixture {
    fixture_with_parameters(reference, 1)
}
fn fixture_with_parameters(reference: p::ConstantReference, parameter_count: usize) -> Fixture {
    let control = Control::default();
    let mut pools = ConstantPools::empty();
    let pool = p::ConstantPool::try_new(
        Arc::new(
            Field::new("request-only", DataType::Int64, true).with_metadata(
                [("source".to_owned(), "original".to_owned())]
                    .into_iter()
                    .collect::<std::collections::HashMap<String, String>>(),
            ),
        ),
        int(),
        Arc::new(Int64Array::from(vec![Some(11), None, Some(33)])).to_data(),
        admission(),
        CompilePhase::Validate,
        &control,
    )
    .unwrap();
    pools
        .insert(p::ConstantPoolId::new(u32::MAX), pool)
        .unwrap();
    let owner = p::NodeId::new(42);
    let mut builder = p::FragmentBuilder::new(p::FragmentId::new(0));
    builder
        .add_values(
            p::NodeId::new(99),
            Box::from([Box::default()]),
            Box::default(),
        )
        .unwrap();
    // Sparse call ID0, independent scoped Lambda body, original order retained.
    let input = p::ExprId::new(10);
    let lambda = p::ExprId::new(20);
    let body = p::ExprId::new(21);
    let call = p::ExprId::new(0);
    for node in [
        p::ExprNode {
            id: input,
            owner,
            ty: int(),
            lambda_scope: None,
            kind: p::ExprKind::Literal(p::LiteralValue::Int64(5)),
        },
        p::ExprNode {
            id: body,
            owner,
            ty: int(),
            lambda_scope: Some(lambda),
            kind: p::ExprKind::Literal(p::LiteralValue::Int64(9)),
        },
        p::ExprNode {
            id: lambda,
            owner,
            ty: int(),
            lambda_scope: None,
            kind: p::ExprKind::Lambda {
                parameter_types: vec![nested("payload"); parameter_count].into_boxed_slice(),
                body,
            },
        },
        p::ExprNode {
            id: call,
            owner,
            ty: int(),
            lambda_scope: None,
            kind: p::ExprKind::FunctionCall {
                function: binding(vec![
                    FunctionArgumentType::Value(int()),
                    FunctionArgumentType::Value(int()),
                    FunctionArgumentType::Lambda {
                        parameter_types:
                            novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
                                vec![nested("payload"); parameter_count].into_boxed_slice(),
                            ),
                        result_type: int(),
                    },
                ]),
                args: Box::from([input, input, lambda]),
            },
        },
    ] {
        builder.insert_expression(node).unwrap();
    }
    let output = builder
        .add_value(
            int(),
            p::ValueOrigin::Expr {
                node: owner,
                expr: call,
            },
        )
        .unwrap();
    builder
        .add_project(
            owner,
            p::NodeId::new(99),
            Box::from([(call, output)]),
            Box::from([output]),
        )
        .unwrap();
    let fragment = builder
        .finish_definition(
            owner,
            p::FragmentSink::Noop,
            p::PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap()
        .with_call_requests_observed(
            vec![(
                PhysicalCallDefinition::Expression(call),
                PhysicalCallRequest {
                    arguments: Box::from([
                        Argument::Value {
                            value_type: int(),
                            constant: None,
                        },
                        Argument::Value {
                            value_type: int(),
                            constant: Some(reference),
                        },
                        Argument::Lambda {
                            parameter_types:
                                novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
                                    vec![nested("payload"); parameter_count].into_boxed_slice(),
                                ),
                            result_type: int(),
                        },
                    ]),
                    logical_argument_count: 3,
                    expected_result_type: Some(int()),
                    constant_policy: data_policy(),
                },
            )],
            &control,
        )
        .unwrap();
    Fixture {
        fragment,
        roots: vec![
            (0, int()),
            (u32::MAX, nested("payload")),
            (7, nested("foreign")),
        ],
        pools,
    }
}
fn project<'a>(
    parameter_ids: &'a [u32],
    arguments: &'a [ArgumentTypeIds<'a>],
) -> [CallRequestTypeIds<'a>; 1] {
    let _ = parameter_ids;
    [CallRequestTypeIds {
        definition: PhysicalCallDefinition::Expression(p::ExprId::new(0)),
        arguments,
        expected_result_type: Some(0),
    }]
}
fn run(
    f: &Fixture,
    control: &Control,
    limits: CallRequestProjectionLimits,
    invoice: usize,
    bad_type: bool,
) -> Result<(wire::FragmentCallRequests, CallRequestProjectionFacts), Error> {
    let types = encode_type_table_sources(&f.roots, &[], types_limits(), control).unwrap();
    let params = [if bad_type { 7 } else { u32::MAX }];
    let args = [
        ArgumentTypeIds::Value(0),
        ArgumentTypeIds::Value(0),
        ArgumentTypeIds::Lambda {
            parameters: &params,
            result: 0,
        },
    ];
    let ids = project(&params, &args);
    control.arm(None);
    let prepared = prepare_call_requests_encode(
        f.fragment.call_requests(),
        &types,
        &ids,
        &f.pools,
        invoice,
        limits,
        control,
    )?;
    let facts = *prepared.facts();
    Ok((encode_call_requests(prepared)?, facts))
}
#[test]
fn original_value_none_typed_null_lambda_constraint_and_all_policy_fields_have_independent_wire_oracle()
 {
    let f = fixture();
    let c = Control::default();
    let (actual, facts) = run(&f, &c, limits(), SOURCE, false).unwrap();
    let expected = wire::FragmentCallRequests {
        entries: vec![wire::OriginalCallRequest {
            definition: Some(wire::CallRequestDefinition {
                kind: Some(wire::call_request_definition::Kind::ExpressionDefinitionId(
                    0,
                )),
            }),
            arguments: vec![
                wire::OriginalFunctionArgument {
                    kind: Some(wire::original_function_argument::Kind::Value(
                        wire::OriginalValueArgument {
                            value_type_id: Some(0),
                            constant: None,
                        },
                    )),
                },
                wire::OriginalFunctionArgument {
                    kind: Some(wire::original_function_argument::Kind::Value(
                        wire::OriginalValueArgument {
                            value_type_id: Some(0),
                            constant: Some(wire::ConstantReference {
                                pool_id: Some(u32::MAX),
                                row_ordinal: 1,
                            }),
                        },
                    )),
                },
                wire::OriginalFunctionArgument {
                    kind: Some(wire::original_function_argument::Kind::Lambda(
                        wire::LambdaArgumentType {
                            parameter_value_type_ids: vec![u32::MAX],
                            result_value_type_id: Some(0),
                        },
                    )),
                },
            ],
            logical_argument_count: Some(3),
            expected_result_value_type_id: Some(0),
            constant_policy: Some(wire::SourceConstantPolicy {
                max_rows: Some(0),
                max_array_nodes: Some(u64::MAX),
                max_logical_elements: Some(2),
                max_retained_buffer_bytes: Some(3),
                max_type_depth: Some(0),
                max_type_nodes: Some(5),
                max_dictionary_depth: Some(u32::MAX),
                max_metadata_bytes: Some(7),
                max_library_validation_work: Some(8),
                max_library_validation_bytes: Some(9),
            }),
        }],
    };
    assert_eq!(actual, expected);
    assert_eq!(facts.definition_count, 1);
    assert_eq!(facts.type_reference_count, 5);
    assert_eq!(facts.allocation_requests_upper_bound, 3);
    assert_eq!(
        facts.request_bytes_upper_bound,
        size_of::<wire::OriginalCallRequest>()
            + 3 * size_of::<wire::OriginalFunctionArgument>()
            + size_of::<u32>()
    );
    let source = f
        .pools
        .entries()
        .get(&p::ConstantPoolId::new(u32::MAX))
        .unwrap();
    assert!(
        source
            .value(1)
            .unwrap()
            .is_null_observed(CompilePhase::Validate, &Control::default())
            .unwrap()
    );
}
#[test]
fn exact_six_axis_envelopes_accept_and_each_one_under_refuses_without_relaxing_source() {
    let f = fixture();
    let (_, facts) = run(&f, &Control::default(), limits(), SOURCE, false).unwrap();
    let exact = CallRequestProjectionLimits {
        max_definitions: facts.definition_count,
        max_type_references: facts.type_reference_count,
        max_request_bytes: facts.request_bytes_upper_bound,
        max_allocation_requests: facts.allocation_requests_upper_bound,
        max_coexisting_source_and_request_bytes: facts
            .coexisting_source_and_request_bytes_upper_bound,
        max_work: facts.cumulative_work_upper_bound,
    };
    run(&f, &Control::default(), exact, SOURCE, false).unwrap();
    for axis in 0..6 {
        let mut under = exact;
        match axis {
            0 => under.max_definitions -= 1,
            1 => under.max_type_references -= 1,
            2 => under.max_request_bytes -= 1,
            3 => under.max_allocation_requests -= 1,
            4 => under.max_coexisting_source_and_request_bytes -= 1,
            _ => under.max_work -= 1,
        }
        assert!(
            run(&f, &Control::default(), under, SOURCE, false).is_err(),
            "axis {axis}"
        );
    }
    assert!(matches!(
        run(&f, &Control::default(), limits(), 0, false),
        Err(Error::InvalidShape(
            "request source invoice omits original backing"
        ))
    ));
}
#[test]
fn wrong_complete_type_missing_pool_and_selected_ordinal_fail_before_emission() {
    let mut foreign = fixture();
    let c = Control::default();
    let foreign_type = FunctionValueType::new(DataType::Int64, false);
    let pool = p::ConstantPool::try_new(
        Arc::new(Field::new("foreign.request", DataType::Int64, false)),
        foreign_type,
        Arc::new(Int64Array::from(vec![11, 22, 33])).to_data(),
        admission(),
        CompilePhase::Validate,
        &c,
    )
    .unwrap();
    foreign.pools = ConstantPools::empty();
    foreign
        .pools
        .insert(p::ConstantPoolId::new(u32::MAX), pool)
        .unwrap();
    assert!(matches!(run(&foreign, &c, limits(), SOURCE, false),
        Err(Error::Constant(p::ConstantReferenceError::SourceTypeMismatch(reference)))
            if reference.pool == p::ConstantPoolId::new(u32::MAX) && reference.ordinal == 1));

    assert!(matches!(
        run(&fixture(), &Control::default(), limits(), SOURCE, true),
        Err(Error::InvalidShape(
            "request complete type differs from its supplied type root"
        ))
    ));
    for (pool, ordinal) in [(0, 1), (u32::MAX, 3)] {
        let f = fixture_with_reference(p::ConstantReference {
            pool: p::ConstantPoolId::new(pool),
            ordinal,
        });
        assert!(matches!(
            run(&f, &Control::default(), limits(), SOURCE, false),
            Err(Error::Constant(_))
        ));
    }
}
fn prefixes(f: &Fixture, bad: bool) {
    let baseline = Control::default();
    let types = encode_type_table_sources(&f.roots, &[], types_limits(), &baseline).unwrap();
    let params = [if bad { 7 } else { u32::MAX }];
    let args = [
        ArgumentTypeIds::Value(0),
        ArgumentTypeIds::Value(0),
        ArgumentTypeIds::Lambda {
            parameters: &params,
            result: 0,
        },
    ];
    let ids = project(&params, &args);
    baseline.arm(None);
    let outcome = prepare_call_requests_encode(
        f.fragment.call_requests(),
        &types,
        &ids,
        &f.pools,
        SOURCE,
        limits(),
        &baseline,
    )
    .and_then(encode_call_requests);
    assert_eq!(outcome.is_err(), bad);
    let trace = baseline.trace();
    assert!(!trace.is_empty());
    for at in 0..trace.len() {
        for cause in CAUSES {
            let c = Control::default();
            c.arm(Some((at, cause.clone())));
            let result = prepare_call_requests_encode(
                f.fragment.call_requests(),
                &types,
                &ids,
                &f.pools,
                SOURCE,
                limits(),
                &c,
            )
            .and_then(encode_call_requests);
            assert!(matches!(result,Err(Error::Control(actual)) if actual==cause));
            assert_eq!(c.trace(), trace[..=at]);
        }
    }
}
#[test]
fn original_control_every_actual_success_and_ordinary_refusal_prefix_keeps_first_cause() {
    prefixes(&fixture(), false);
    prefixes(&fixture(), true);
}
#[test]
fn ordered_type_source_shape_and_constraint_presence_cannot_be_guessed() {
    let f = fixture();
    let c = Control::default();
    let types = encode_type_table_sources(&f.roots, &[], types_limits(), &c).unwrap();
    let params = [u32::MAX];
    let args = [
        ArgumentTypeIds::Value(0),
        ArgumentTypeIds::Value(0),
        ArgumentTypeIds::Lambda {
            parameters: &params,
            result: 0,
        },
    ];
    for variant in 0..4 {
        let mut ids = project(&params, &args);
        let empty = [];
        match variant {
            0 => ids[0].definition = PhysicalCallDefinition::Expression(p::ExprId::new(u32::MAX)),
            1 => ids[0].expected_result_type = None,
            2 => ids[0].arguments = &empty,
            _ => ids[0].arguments = &args[..2],
        }
        assert!(matches!(
            prepare_call_requests_encode(
                f.fragment.call_requests(),
                &types,
                &ids,
                &f.pools,
                SOURCE,
                limits(),
                &c
            ),
            Err(Error::InvalidShape(_))
        ));
    }
}
#[test]
fn empty_explicit_table_emits_empty_and_wide_actual_value_channels_observe_real_quantum() {
    let c = Control::default();
    let mut b = p::FragmentBuilder::new(p::FragmentId::new(0));
    b.add_values(p::NodeId::new(0), Box::default(), Box::default())
        .unwrap();
    let empty = b
        .finish_definition(
            p::NodeId::new(0),
            p::FragmentSink::Noop,
            p::PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap()
        .with_call_requests_observed(vec![], &c)
        .unwrap();
    let roots = [];
    let types = encode_type_table_sources(&roots, &[], types_limits(), &c).unwrap();
    c.arm(None);
    let pools = ConstantPools::empty();
    let token = prepare_call_requests_encode(
        empty.call_requests(),
        &types,
        &[],
        &pools,
        SOURCE,
        limits(),
        &c,
    )
    .unwrap();
    assert!(encode_call_requests(token).unwrap().entries.is_empty());
    // The actual 320-parameter Lambda exists in the checked source and is
    // projected through the complete prepare/emission port. The sampled 256
    // belongs to owned parameter loops, not opaque Arrow/type internals.
    let wide = fixture_with_parameters(
        p::ConstantReference {
            pool: p::ConstantPoolId::new(u32::MAX),
            ordinal: 1,
        },
        320,
    );
    let setup = Control::default();
    let types = encode_type_table_sources(&wide.roots, &[], types_limits(), &setup).unwrap();
    let params = vec![u32::MAX; 320];
    let args = [
        ArgumentTypeIds::Value(0),
        ArgumentTypeIds::Value(0),
        ArgumentTypeIds::Lambda {
            parameters: &params,
            result: 0,
        },
    ];
    let ids = project(&params, &args);
    let mut wide_limits = limits();
    wide_limits.max_work = 1usize << 34;
    setup.arm(None);
    let token = prepare_call_requests_encode(
        wide.fragment.call_requests(),
        &types,
        &ids,
        &wide.pools,
        SOURCE,
        wide_limits,
        &setup,
    )
    .unwrap();
    // Isolate the actual emission trace, with a fresh unrefused preparation
    // for each sampled primary cause (same original control per instance).
    setup.arm(None);
    let output = encode_call_requests(token).unwrap();
    assert!(matches!(&output.entries[0].arguments[2].kind,
        Some(wire::original_function_argument::Kind::Lambda(value)) if value.parameter_value_type_ids==params));
    let baseline = setup.trace();
    let quantum = baseline
        .iter()
        .position(|(_, units)| *units == 256)
        .expect("actual owned Lambda emission quantum");
    for at in [0, quantum, baseline.len() - 1] {
        for cause in CAUSES {
            let c = Control::default();
            let token = prepare_call_requests_encode(
                wide.fragment.call_requests(),
                &types,
                &ids,
                &wide.pools,
                SOURCE,
                wide_limits,
                &c,
            )
            .unwrap();
            c.arm(Some((at, cause.clone())));
            assert!(
                matches!(encode_call_requests(token),Err(Error::Control(actual)) if actual==cause)
            );
            assert_eq!(c.trace(), baseline[..=at]);
        }
    }
}

#[test]
fn two_way_request_component_returns_to_original_fragment_coverage_and_pool_namespace() {
    use crate::physical_call_requests_v2::{decode_call_requests, prepare_call_requests_decode};
    use crate::physical_type_v2::decode_type_table;
    for has_constraint in [false, true] {
        let mut f = fixture();
        let control = Control::default();
        if !has_constraint {
            let mut entries: Vec<_> = f
                .fragment
                .call_requests()
                .entries()
                .iter()
                .map(|(key, request)| (*key, request.clone()))
                .collect();
            entries[0].1.expected_result_type = None;
            f.fragment = f
                .fragment
                .with_call_requests_observed(entries, &control)
                .unwrap();
        }
        let types = encode_type_table_sources(&f.roots, &[], types_limits(), &control).unwrap();
        let params = [u32::MAX];
        let args = [
            ArgumentTypeIds::Value(0),
            ArgumentTypeIds::Value(0),
            ArgumentTypeIds::Lambda {
                parameters: &params,
                result: 0,
            },
        ];
        let mut ids = project(&params, &args);
        ids[0].expected_result_type = has_constraint.then_some(0);
        let wire = encode_call_requests(
            prepare_call_requests_encode(
                f.fragment.call_requests(),
                &types,
                &ids,
                &f.pools,
                SOURCE,
                limits(),
                &control,
            )
            .unwrap(),
        )
        .unwrap();
        // Independent data checks precede the two-way correspondence check.
        assert_eq!(
            wire.entries[0].expected_result_value_type_id,
            has_constraint.then_some(0)
        );
        let decoded_types = decode_type_table(types.as_wire(), types_limits(), &control).unwrap();
        let entries = decode_call_requests(
            prepare_call_requests_decode(
                Some(&wire),
                &decoded_types,
                &f.pools,
                SOURCE,
                limits(),
                &control,
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].0,
            PhysicalCallDefinition::Expression(p::ExprId::new(0))
        );
        assert!(matches!(
            &entries[0].1.arguments[0],
            Argument::Value { constant: None, .. }
        ));
        let Argument::Value {
            constant: Some(reference),
            ..
        } = &entries[0].1.arguments[1]
        else {
            panic!("the original typed NULL reference must remain Some");
        };
        assert_eq!(reference.pool.get(), u32::MAX);
        assert_eq!(reference.ordinal, 1);
        assert_eq!(entries[0].1.constant_policy, data_policy());
        let checked = f
            .fragment
            .clone()
            .with_call_requests_observed(entries, &control)
            .unwrap();
        assert_eq!(checked.call_requests(), f.fragment.call_requests());
        // A shape-valid request component cannot authorize a foreign definition.
        let mut foreign = wire.clone();
        foreign.entries[0].definition.as_mut().unwrap().kind = Some(
            wire::call_request_definition::Kind::ExpressionDefinitionId(7),
        );
        let entries = decode_call_requests(
            prepare_call_requests_decode(
                Some(&foreign),
                &decoded_types,
                &f.pools,
                SOURCE,
                limits(),
                &control,
            )
            .unwrap(),
        )
        .unwrap();
        assert!(
            matches!(f.fragment.clone().with_call_requests_observed(entries, &control),
            Err(p::CallRequestError::MissingDefinition(PhysicalCallDefinition::Expression(id))) if id.get() == 0)
        );
    }
}

#[path = "sender_owned_tests.rs"]
mod owned;
