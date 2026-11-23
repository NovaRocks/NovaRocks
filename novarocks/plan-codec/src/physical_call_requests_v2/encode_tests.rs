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
use novarocks_expression_contract::{
    BoundExprSet, BoundExprSetBuilder, DefinitionControl, ExprId, ExprKind, ExpressionCallClaim,
    RootScope, UnitLimits, UnitParts,
};
use novarocks_physical_plan as p;
use novarocks_type_contract::{
    ArgumentControl, CallProofScope, ControlShape, EvaluationDemand, FunctionInstanceState,
    FunctionNullBehavior, ObservableEffects,
};
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
fn binding(args: Vec<FunctionArgumentType>) -> novarocks_expression_contract::BoundFunction {
    novarocks_expression_contract::BoundFunction {
        function_id: FunctionId::try_new("test/codec-original-request").unwrap(),
        overload: FunctionOverloadId::try_new("test/codec-original-request/v1").unwrap(),
        kind: FunctionKind::Scalar,
        argument_types: novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
            args.into_boxed_slice(),
        ),
        result_type: int(),

        legacy_metadata: Some(novarocks_expression_contract::LegacyBindingMetadata {
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
    pub(crate) unit: BoundExprSet,
    pub(crate) roots: Vec<(u32, FunctionValueType)>,
    pub(crate) pools: ConstantPools,
}
fn unit_limits() -> UnitLimits {
    UnitLimits {
        expression_semantic_depth: p::PlanLimits::FROZEN.expression_semantic_depth,
    }
}
impl Fixture {
    fn receiving_parts(&self) -> UnitParts {
        UnitParts::new(
            self.unit.ports().into(),
            self.unit.definitions().into(),
            self.unit.roots().into(),
            self.unit.root_use_ids().into(),
            self.unit.control_flow().clone(),
            self.unit.claims().clone(),
            self.unit.requests().clone(),
        )
    }
    fn with_request(mut self, request: PhysicalCallRequest) -> Self {
        let mut parts = self.unit.into_parts();
        parts.requests.insert(ExprId::new(0), request);
        let c = Control::default();
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Validate).unwrap();
        self.unit = BoundExprSet::try_from_parts_in(parts, unit_limits(), &mut work).unwrap();
        work.finish().unwrap();
        self
    }
}
// Loan the real checked unit to the same generic preflight/emission authors.
// Only tests isolating a native emission quantum need these split helpers;
// complete caller-borrowed projections below use encode_unit_requests_in.
fn prepare_unit_token<'a, 's>(
    unit: &'a BoundExprSet,
    types: &'a EncodedTypeTable<'s>,
    ids: &'a [UnitRequestTypeIds<'a>],
    pools: &'a ConstantPools,
    invoice: usize,
    limits: CallRequestProjectionLimits,
    control: &'a dyn PureCompileControl,
) -> Result<PreparedCallRequestsEncode<'a, 's, ExprId>, Error> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = preflight::<std::convert::Infallible, _>(
        unit.requests(),
        types,
        ids,
        pools,
        invoice,
        limits,
        None,
        &mut work,
    )
    .map_err(ProjectionFailure::without_host)
    .map(|facts| PreparedCallRequestsEncode {
        source: unit.requests(),
        types,
        type_ids: ids,
        pools,
        control,
        facts,
    });
    finish(work, result)
}
fn emit_unit_token(
    token: PreparedCallRequestsEncode<'_, '_, ExprId>,
) -> Result<Vec<wire::UnitCallRequest>, Error> {
    let mut work = CompileCheckpoints::try_new(token.control, CompilePhase::Encode)?;
    let result = emit_core(token, &mut work);
    finish(work, result)
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
    let mut builder = BoundExprSetBuilder::new(unit_limits());
    let call = builder.reserve().unwrap();
    let input = builder
        .add(
            None,
            int(),
            crate::test_constants::insert(&mut pools, crate::test_constants::i64(5)(&int())),
        )
        .unwrap();
    let lambda = builder.reserve().unwrap();
    let body = builder
        .add(
            Some(lambda),
            int(),
            crate::test_constants::insert(&mut pools, crate::test_constants::i64(9)(&int())),
        )
        .unwrap();
    let parameters = vec![nested("payload"); parameter_count];
    builder
        .insert(
            lambda,
            None,
            int(),
            ExprKind::Lambda {
                parameter_types: parameters.clone().into_boxed_slice(),
                body,
            },
        )
        .unwrap();
    builder
        .insert(
            call,
            None,
            int(),
            ExprKind::FunctionCall {
                function: binding(vec![
                    FunctionArgumentType::Value(int()),
                    FunctionArgumentType::Value(int()),
                    FunctionArgumentType::Lambda {
                        parameter_types:
                            novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
                                parameters.clone().into_boxed_slice(),
                            ),
                        result_type: int(),
                    },
                ]),
                args: Box::new([input, input, lambda]),
            },
        )
        .unwrap();
    builder
        .definition_control(
            call,
            DefinitionControl {
                shape: ControlShape::HigherOrder {
                    body_ordinal: 2,
                    body_demand: EvaluationDemand::Value,
                },
                temporal_sources: None,
            },
        )
        .unwrap();
    builder
        .request(
            call,
            PhysicalCallRequest {
                arguments: Box::new([
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
                                parameters.into_boxed_slice(),
                            ),
                        result_type: int(),
                    },
                ]),
                logical_argument_count: 3,
                expected_result_type: Some(int()),
                constant_policy: data_policy(),
            },
        )
        .unwrap();
    builder
        .root(call, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let sealed = builder.seal(vec![], &mut work).unwrap();
    let claims = sealed
        .uses()
        .iter()
        .filter(|(_, i)| i.definition == call)
        .map(|(&id, i)| {
            (
                id,
                ExpressionCallClaim {
                    context: i.context,
                    effects: novarocks_type_contract::CallEffects {
                        value_stability: FunctionVolatility::Immutable,
                        own_row_error: FunctionIntrinsicRowError::NoRowError,
                        failure_behavior: FunctionFailureBehavior::Propagate,
                        null_behavior: FunctionNullBehavior::CalledOnNull,
                        argument_control: ArgumentControl::HigherOrder {
                            body_ordinal: 2,
                            body_demand: EvaluationDemand::Value,
                        },
                        instance_state: FunctionInstanceState::None,
                        observable_effects: ObservableEffects::NONE,
                        environment: Box::default(),
                        proof_scope: CallProofScope::Region(i.context.region),
                    },
                    decimal_overflow_policy:
                        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                    temporal_source: None,
                },
            )
        })
        .collect();
    let unit = sealed.finish(claims, &mut work).unwrap();
    work.finish().unwrap();
    Fixture {
        unit,
        roots: vec![
            (0, int()),
            (u32::MAX, nested("payload")),
            (7, nested("foreign")),
        ],
        pools,
    }
}
fn relation_fixture() -> p::Fragment {
    relation_fixture_with_result_constraint(None)
}
fn relation_fixture_with_result_constraint(
    expected_result_type: Option<FunctionValueType>,
) -> p::Fragment {
    let control = Control::default();
    let mut builder = p::FragmentBuilder::new(p::FragmentId::new(0));
    let input = p::NodeId::new(99);
    let node = p::NodeId::new(42);
    builder
        .add_values(input, Box::new([Box::default()]), Box::default())
        .unwrap();
    let output = builder
        .add_value(
            int(),
            p::ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let props = p::PhysicalProperties {
        distribution: p::Distribution::Singleton,
        row_multiplicity: p::RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    };
    let mut function = binding(vec![]);
    function.kind = FunctionKind::Window;
    function
        .legacy_metadata
        .as_mut()
        .unwrap()
        .intrinsic_row_error = FunctionIntrinsicRowError::NotRowEvaluated;
    builder
        .insert_node_unchecked(p::PhysicalNode {
            id: node,
            inputs: Box::new([input]),
            required_inputs: Box::new([props.clone()]),
            output_properties: props,
            output: p::OutputPort {
                node,
                columns: Box::new([output]),
            },
            kind: p::NodeKind::Window(p::WindowSpec {
                partition_by: Box::default(),
                order_by: Box::default(),
                calls: Box::new([p::WindowCallSpec {
                    binding: function,
                    distinct: false,
                    arguments: Box::default(),
                    order_by: Box::default(),
                    frame: None,
                    ignore_nulls: false,
                    aggregate_binding: None,
                    output,
                }]),
            }),
            expressions: None,
        })
        .unwrap();
    let fragment = builder
        .finish_structure(
            node,
            p::FragmentSink::Noop,
            p::PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            p::PlanLimits::FROZEN,
            &control,
            p::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let site = PhysicalCallSite::Window { node, call: 0 };
    let fragment = fragment
        .with_call_requests_observed(
            vec![(
                site,
                PhysicalCallRequest {
                    arguments: Box::default(),
                    logical_argument_count: 0,
                    expected_result_type,
                    constant_policy: data_policy(),
                },
            )],
            &control,
        )
        .unwrap();
    let context = novarocks_type_contract::RelationEffectContext::call(
        novarocks_type_contract::RelationCallId::new(0),
    );
    fragment
        .with_calls_observed(
            vec![p::FrozenPhysicalCall {
                site,
                context,
                effects: novarocks_type_contract::CallEffects {
                    value_stability: FunctionVolatility::Immutable,
                    own_row_error: FunctionIntrinsicRowError::NotRowEvaluated,
                    failure_behavior: FunctionFailureBehavior::Propagate,
                    null_behavior: FunctionNullBehavior::CalledOnNull,
                    argument_control: ArgumentControl::Window,
                    instance_state: FunctionInstanceState::WindowPartition,
                    observable_effects: ObservableEffects::NONE,
                    environment: Box::default(),
                    proof_scope: CallProofScope::Relation(context.identity),
                },
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                materialized_writer_input: None,
            }],
            &control,
        )
        .unwrap()
}
fn project<'a>(
    parameter_ids: &'a [u32],
    arguments: &'a [ArgumentTypeIds<'a>],
) -> [UnitRequestTypeIds<'a>; 1] {
    let _ = parameter_ids;
    [UnitRequestTypeIds {
        definition: ExprId::new(0),
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
) -> Result<(Vec<wire::UnitCallRequest>, CallRequestProjectionFacts), Error> {
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
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = encode_unit_requests_in(
        &f.unit,
        &types,
        &ids,
        &f.pools,
        invoice,
        limits,
        &mut |_| Ok(()),
        &mut work,
    );
    finish(work, result)
}
#[test]
fn original_value_none_typed_null_lambda_constraint_and_all_policy_fields_have_independent_wire_oracle()
 {
    let f = fixture();
    let c = Control::default();
    let (actual, facts) = run(&f, &c, limits(), SOURCE, false).unwrap();
    let expected = vec![wire::UnitCallRequest {
        definition_id: Some(0),
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
    }];
    assert_eq!(actual, expected);
    assert_eq!(facts.definition_count, 1);
    assert_eq!(facts.type_reference_count, 5);
    assert_eq!(facts.allocation_requests_upper_bound, 3);
    assert_eq!(
        facts.request_bytes_upper_bound,
        size_of::<wire::UnitCallRequest>()
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
        Err(Error::Constant(p::ConstantReferenceError::Table(novarocks_constant_contract::ConstantTableError::SourceTypeMismatch(reference))))
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
    let outcome = prepare_unit_token(&f.unit, &types, &ids, &f.pools, SOURCE, limits(), &baseline)
        .and_then(emit_unit_token);
    assert_eq!(outcome.is_err(), bad);
    let trace = baseline.trace();
    assert!(!trace.is_empty());
    for at in 0..trace.len() {
        for cause in CAUSES {
            let c = Control::default();
            c.arm(Some((at, cause.clone())));
            let result = prepare_unit_token(&f.unit, &types, &ids, &f.pools, SOURCE, limits(), &c)
                .and_then(emit_unit_token);
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
            0 => ids[0].definition = ExprId::new(u32::MAX),
            1 => ids[0].expected_result_type = None,
            2 => ids[0].arguments = &empty,
            _ => ids[0].arguments = &args[..2],
        }
        assert!(matches!(
            prepare_unit_token(&f.unit, &types, &ids, &f.pools, SOURCE, limits(), &c),
            Err(Error::InvalidShape(_))
        ));
    }
}
#[test]
fn empty_explicit_table_emits_empty_and_wide_actual_value_channels_observe_real_quantum() {
    let c = Control::default();
    let builder = BoundExprSetBuilder::new(unit_limits());
    let mut setup = CompileCheckpoints::try_new(&c, CompilePhase::Validate).unwrap();
    let empty = builder
        .seal(vec![], &mut setup)
        .unwrap()
        .finish(std::collections::BTreeMap::new(), &mut setup)
        .unwrap();
    setup.finish().unwrap();
    let roots = [];
    let types = encode_type_table_sources(&roots, &[], types_limits(), &c).unwrap();
    c.arm(None);
    let pools = ConstantPools::empty();
    let token = prepare_unit_token(&empty, &types, &[], &pools, SOURCE, limits(), &c).unwrap();
    assert!(emit_unit_token(token).unwrap().is_empty());
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
    let token = prepare_unit_token(
        &wide.unit,
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
    let output = emit_unit_token(token).unwrap();
    assert!(matches!(&output[0].arguments[2].kind,
        Some(wire::original_function_argument::Kind::Lambda(value)) if value.parameter_value_type_ids==params));
    let baseline = setup.trace();
    let quantum = baseline
        .iter()
        .position(|(_, units)| *units == 256)
        .expect("actual owned Lambda emission quantum");
    for at in [0, quantum, baseline.len() - 1] {
        for cause in CAUSES {
            let c = Control::default();
            let token = prepare_unit_token(
                &wide.unit,
                &types,
                &ids,
                &wide.pools,
                SOURCE,
                wide_limits,
                &c,
            )
            .unwrap();
            c.arm(Some((at, cause.clone())));
            assert!(matches!(emit_unit_token(token),Err(Error::Control(actual)) if actual==cause));
            assert_eq!(c.trace(), baseline[..=at]);
        }
    }
}

#[test]
fn two_way_request_component_returns_to_original_fragment_coverage_and_pool_namespace() {
    use crate::physical_type_v2::decode_type_table;
    for has_constraint in [false, true] {
        let f = fixture();
        let mut request = f.unit.requests()[&ExprId::new(0)].clone();
        request.expected_result_type = has_constraint.then(int);
        let f = f.with_request(request);
        let c = Control::default();
        let types = encode_type_table_sources(&f.roots, &[], types_limits(), &c).unwrap();
        let params = [u32::MAX];
        let arguments = [
            ArgumentTypeIds::Value(0),
            ArgumentTypeIds::Value(0),
            ArgumentTypeIds::Lambda {
                parameters: &params,
                result: 0,
            },
        ];
        let mut ids = project(&params, &arguments);
        ids[0].expected_result_type = has_constraint.then_some(0);
        let wire = emit_unit_token(
            prepare_unit_token(&f.unit, &types, &ids, &f.pools, SOURCE, limits(), &c).unwrap(),
        )
        .unwrap();
        assert_eq!(
            wire[0].expected_result_value_type_id,
            has_constraint.then_some(0)
        );
        let decoded_types = decode_type_table(types.as_wire(), types_limits(), &c).unwrap();
        let decode = |raw: &Vec<wire::UnitCallRequest>| {
            let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
            let (requests, _, _) =
                super::super::read::decode_unit_requests_in::<std::convert::Infallible>(
                    raw,
                    &decoded_types,
                    &f.pools,
                    SOURCE,
                    limits(),
                    &mut |_| Ok(()),
                    None,
                    &mut work,
                )
                .map_err(ProjectionFailure::without_host)
                .unwrap();
            work.finish().unwrap();
            requests
        };
        let entries = decode(&wire);
        assert_eq!(entries.len(), 1);
        let request = &entries[&ExprId::new(0)];
        assert!(matches!(
            &request.arguments[0],
            Argument::Value { constant: None, .. }
        ));
        let Argument::Value {
            constant: Some(reference),
            ..
        } = &request.arguments[1]
        else {
            panic!("the original typed NULL reference must remain Some")
        };
        assert_eq!(reference.pool.get(), u32::MAX);
        assert_eq!(reference.ordinal, 1);
        assert_eq!(request.constant_policy, data_policy());
        let mut parts = f.receiving_parts();
        parts.requests = entries;
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Validate).unwrap();
        let checked = BoundExprSet::try_from_parts_in(parts, unit_limits(), &mut work).unwrap();
        work.finish().unwrap();
        assert_eq!(checked.requests(), f.unit.requests());
        let mut foreign = wire.clone();
        foreign[0].definition_id = Some(7);
        let mut parts = f.receiving_parts();
        parts.requests = decode(&foreign);
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Validate).unwrap();
        let error = BoundExprSet::try_from_parts_in(parts, unit_limits(), &mut work).unwrap_err();
        assert!(
            matches!(error,novarocks_expression_contract::UnitError::Invalid{definition:Some(id),message,..} if id==ExprId::new(0)&&message=="function definition lacks its original request")
        );
        work.finish().unwrap();
    }
}

#[path = "sender_owned_tests.rs"]
mod owned;
