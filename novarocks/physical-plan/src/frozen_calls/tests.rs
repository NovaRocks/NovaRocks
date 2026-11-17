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
#[path = "dynamic_items_owned_tests.rs"]
mod dynamic_items_owned_tests;
#[path = "property_formula_tests.rs"]
mod property_formula_tests;
#[path = "property_proof_tests.rs"]
mod property_proof_tests;
use crate::*;
use arrow_schema::DataType;
use novarocks_type_contract::{
    AggregateStateFormatId, ControlShape, EvaluationRegionId, ExpressionControlFlow,
    ExpressionEvaluationRegion, ExpressionInvocation, FunctionArgumentEvaluation,
    FunctionFailureBehavior, FunctionId, FunctionInstanceState, FunctionIntrinsicRowError,
    FunctionNullBehavior, FunctionOverloadId, FunctionVolatility, GuardKind, ObservableEffects,
    RegionGuard, SemanticParameterId, SemanticParameterKey,
};
use std::sync::Mutex;

#[path = "replica_tests.rs"]
mod replica_tests;

#[derive(Default)]
struct Control {
    stop: Option<CompileControlError>,
    positive_only: bool,
    observations: Mutex<Vec<(CompilePhase, u32)>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        self.observations.lock().unwrap().push((phase, units));
        if let Some(error) = self.stop.clone()
            && (!self.positive_only || units > 0)
        {
            return Err(error);
        }
        Ok(())
    }
}
fn properties() -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Singleton,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}
fn dop() -> PipelineDopDomain {
    PipelineDopDomain {
        min: 1,
        max: 1,
        requires_power_of_two: false,
    }
}
fn boolean() -> ValueType {
    ValueType::new(DataType::Boolean, true)
}
fn integer() -> ValueType {
    ValueType::new(DataType::Int64, false)
}
fn function(kind: FunctionKind, result_type: ValueType) -> BoundFunction {
    BoundFunction {
        function_id: FunctionId::try_new("fixture/frozen-calls/exact-id").unwrap(),
        overload: FunctionOverloadId::try_new("fixture/frozen-calls/exact-overload").unwrap(),
        kind,
        argument_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::default()),
        result_type,
        legacy_metadata: Some(crate::LegacyBindingMetadata {
            volatility: FunctionVolatility::Immutable,
            argument_evaluation: FunctionArgumentEvaluation::Eager,
            failure_behavior: FunctionFailureBehavior::Propagate,
            intrinsic_row_error: match kind {
                FunctionKind::Scalar | FunctionKind::Table => FunctionIntrinsicRowError::NoRowError,
                FunctionKind::Aggregate | FunctionKind::Window => {
                    FunctionIntrinsicRowError::NotRowEvaluated
                }
            },
            semantic_parameters: Box::default(),
        }),
    }
}
fn effects(kind: FunctionKind, domain: EvaluationRegionId) -> CallEffects {
    CallEffects {
        value_stability: FunctionVolatility::Immutable,
        own_row_error: match kind {
            FunctionKind::Scalar | FunctionKind::Table => FunctionIntrinsicRowError::NoRowError,
            FunctionKind::Aggregate | FunctionKind::Window => {
                FunctionIntrinsicRowError::NotRowEvaluated
            }
        },
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: FunctionNullBehavior::CalledOnNull,
        argument_control: match kind {
            FunctionKind::Scalar => ArgumentControl::Eager,
            FunctionKind::Aggregate => ArgumentControl::Aggregate,
            FunctionKind::Window => ArgumentControl::Window,
            FunctionKind::Table => ArgumentControl::Table,
        },
        instance_state: match kind {
            FunctionKind::Scalar => FunctionInstanceState::None,
            FunctionKind::Aggregate => FunctionInstanceState::AggregateInstance,
            FunctionKind::Window => FunctionInstanceState::WindowPartition,
            FunctionKind::Table => FunctionInstanceState::TableInstance,
        },
        observable_effects: ObservableEffects::NONE,
        environment: Box::default(),
        proof_scope: CallProofScope::Region(domain),
    }
}
fn context(id: u32, domain: u32, demand: EvaluationDemand) -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(id),
        region: EvaluationRegionId::new(domain),
        demand,
    }
}
fn domain(id: u32) -> ExpressionEvaluationRegion {
    ExpressionEvaluationRegion {
        id: EvaluationRegionId::new(id),
        parent: None,
        guard: None,
    }
}
fn install_node(
    builder: &mut FragmentBuilder,
    node: NodeId,
    inputs: Vec<NodeId>,
    columns: Vec<ValueId>,
    kind: NodeKind,
) {
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            required_inputs: vec![properties(); inputs.len()].into_boxed_slice(),
            inputs: inputs.into_boxed_slice(),
            output_properties: properties(),
            output: OutputPort {
                node,
                columns: columns.into_boxed_slice(),
            },
            kind,
        })
        .unwrap();
}
// The zero-argument fixture is requested without a result constraint. This
// source record is authored before emission, independently of selected metadata.
fn zero_argument_request() -> PhysicalCallRequest {
    PhysicalCallRequest {
        arguments: Box::default(),
        logical_argument_count: 0,
        expected_result_type: None,
        constant_policy: ConstantPolicy {
            max_rows: 16,
            max_array_nodes: 32,
            max_logical_elements: 128,
            max_retained_buffer_bytes: 64 * 1024,
            max_type_depth: 16,
            max_type_nodes: 128,
            max_dictionary_depth: 8,
            max_metadata_bytes: 4096,
            max_library_validation_work: 1_000_000,
            max_library_validation_bytes: 1024 * 1024,
        },
    }
}
fn add_values(
    builder: &mut FragmentBuilder,
    count: usize,
    scalar: bool,
    fixture_constants: &mut crate::tests::constant_fixture::ConstantFixture,
) -> (NodeId, ExprId) {
    let node = builder.reserve_node_id().unwrap();
    let ty = if scalar { boolean() } else { integer() };
    let expression = builder
        .add_expression(
            node,
            (ty.clone()).clone(),
            if scalar {
                ExprKind::FunctionCall {
                    function: function(FunctionKind::Scalar, ty.clone()),
                    args: Box::default(),
                }
            } else {
                fixture_constants.i64(17, &(ty.clone()))
            },
        )
        .unwrap();
    let value = builder
        .add_value(
            ty,
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    install_node(
        builder,
        node,
        vec![],
        vec![value],
        NodeKind::Values {
            rows: vec![Box::from([expression]); count].into_boxed_slice(),
        },
    );
    (node, expression)
}
fn leaf_roots(fragment: &Fragment) -> PhysicalRootUses {
    let roots = PhysicalExpressionRoots::try_new(fragment, &Control::default()).unwrap();
    let mut bindings = Vec::new();
    let mut invocations = Vec::new();
    for (ordinal, (site, root)) in roots.sites().iter().enumerate() {
        // These fixtures explicitly use literals or zero-argument calls.
        // This is not a production inference of function control semantics.
        match &fragment.expressions().get(root.expr).unwrap().kind {
            ExprKind::Constant(_) => {}
            ExprKind::FunctionCall { args, .. } | ExprKind::WindowCall { args, .. } => {
                assert!(args.is_empty());
            }
            _ => panic!("fixture root requires explicit invocation construction"),
        }
        let id = match ordinal {
            0 => 0,
            1 => u32::MAX,
            other => other as u32 - 1,
        };
        let current = context(id, if ordinal % 2 == 0 { 0 } else { u32::MAX }, root.demand);
        bindings.push((*site, current.use_id));
        invocations.push(ExpressionInvocation {
            context: current,
            definition: root.expr,
            control: ControlShape::Eager,
            arguments: Box::default(),
        });
    }
    let flow = ExpressionControlFlow::try_new(
        vec![domain(0), domain(u32::MAX)],
        invocations,
        fragment.expressions(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    PhysicalRootUses::try_new(fragment, flow, bindings, &Control::default()).unwrap()
}
struct Fixture {
    fragment: Fragment,
    uses: PhysicalRootUses,
    calls: Vec<FrozenPhysicalCall>,
}
impl Fixture {
    fn checked(&self) -> Result<FrozenFragmentCalls, FrozenCallError> {
        FrozenFragmentCalls::try_new(
            &self.fragment,
            &self.uses,
            self.calls.clone(),
            &Control::default(),
        )
    }
}
// A genuine zero-row, zero-column source has no unreachable expression.
// Empty call/request tables are explicit and checked by their original owners.
fn empty_fixture() -> Fixture {
    let mut builder = FragmentBuilder::new(FragmentId::new(u32::MAX));
    let node = builder.reserve_node_id().unwrap();
    install_node(
        &mut builder,
        node,
        vec![],
        vec![],
        NodeKind::Values {
            rows: Box::default(),
        },
    );
    let fragment = builder
        .finish_definition(
            node,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap()
        .with_call_requests_observed(vec![], &Control::default())
        .unwrap();
    let uses = leaf_roots(&fragment);
    Fixture {
        fragment,
        uses,
        calls: vec![],
    }
}

fn scalar_fixture(
    count: usize,
    fragment_id: u32,
    fixture_constants: &mut crate::tests::constant_fixture::ConstantFixture,
) -> Fixture {
    let mut builder = FragmentBuilder::new(FragmentId::new(fragment_id));
    let request = zero_argument_request();
    let (node, expression) = add_values(&mut builder, count, true, fixture_constants);
    let fragment = builder
        .finish_definition(
            node,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap()
        .with_call_requests_observed(
            vec![(PhysicalCallDefinition::Expression(expression), request)],
            &Control::default(),
        )
        .unwrap();
    validate_fragment_definition(&fragment).unwrap();
    let uses = leaf_roots(&fragment);
    let calls = uses
        .flow()
        .uses()
        .values()
        .map(|invocation| FrozenPhysicalCall {
            materialized_writer_input: None,
            regexp_count_pattern_source: None,
            to_base64_byte_source: None,
            temporal_source: None,
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            site: PhysicalCallSite::Expression(invocation.context.use_id),
            context: invocation.context,
            effects: effects(FunctionKind::Scalar, invocation.context.region),
        })
        .collect();
    Fixture {
        fragment,
        uses,
        calls,
    }
}

#[test]
fn repeated_definition_preserves_complete_per_use_claims_and_exact_binding_borrow() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut fixture = scalar_fixture(2, 0, &mut fixture_constants);
    let reference = SemanticParameterRef {
        id: SemanticParameterId::new(u32::MAX),
        expected_key: SemanticParameterKey::TimeZone,
    };
    let rich = &mut fixture.calls[0].effects;
    rich.value_stability = FunctionVolatility::Volatile;
    rich.own_row_error = FunctionIntrinsicRowError::MayRaise;
    rich.failure_behavior = FunctionFailureBehavior::ReturnsNull;
    rich.null_behavior = FunctionNullBehavior::Strict;
    rich.instance_state = FunctionInstanceState::ScalarInstance;
    rich.observable_effects = ObservableEffects {
        rng_sampling: true,
        warnings: true,
        controlled_wait: true,
    };
    rich.environment = Box::from([reference]);
    fixture.calls[1].effects.proof_scope = CallProofScope::Unconditional;
    let checked = fixture.checked().unwrap();
    assert_eq!(checked.fragment(), FragmentId::new(0));
    assert_eq!(checked.entries().len(), 2);
    for call in &fixture.calls {
        assert_eq!(&checked.entries()[&call.site], call);
    }
    assert_eq!(
        checked.parameter_references().collect::<Vec<_>>(),
        vec![reference]
    );
    let first = &fixture.uses.flow().uses()[&ExpressionUseId::new(0)];
    let second = &fixture.uses.flow().uses()[&ExpressionUseId::new(u32::MAX)];
    assert_eq!(first.definition, second.definition);
    assert_ne!(first.context.region, second.context.region);
    let ExprKind::FunctionCall { function, .. } = &fixture
        .fragment
        .expressions()
        .get(first.definition)
        .unwrap()
        .kind
    else {
        panic!("expected actual function binding")
    };
    for call in &fixture.calls {
        let Some(PhysicalCallBinding::Scalar(actual)) =
            checked.binding(&fixture.fragment, &fixture.uses, call.site)
        else {
            panic!("missing exact binding")
        };
        assert!(std::ptr::eq(actual, function));
    }
    // Public shape claims intentionally do not authenticate an installed owner.
    assert_ne!(
        fixture.calls[0].effects.value_stability,
        function.legacy_metadata.as_ref().unwrap().volatility
    );
}

#[test]
fn missing_extra_duplicate_foreign_fragment_context_scope_and_control_fail_closed() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fixture = scalar_fixture(2, 7, &mut fixture_constants);
    let check = |calls| {
        FrozenFragmentCalls::try_new(&fixture.fragment, &fixture.uses, calls, &Control::default())
    };
    assert_eq!(
        check(vec![]),
        Err(FrozenCallError::MissingSite(PhysicalCallSite::Expression(
            ExpressionUseId::new(0)
        )))
    );
    let mut calls = fixture.calls.clone();
    calls.push(calls[0].clone());
    assert_eq!(check(calls), Err(FrozenCallError::DuplicateSite));
    let mut calls = fixture.calls.clone();
    let mut extra = calls[0].clone();
    extra.site = PhysicalCallSite::Expression(ExpressionUseId::new(123));
    calls.push(extra);
    assert_eq!(check(calls), Err(FrozenCallError::InvalidSite));
    let mut calls = fixture.calls.clone();
    calls[0].context.demand = EvaluationDemand::TruthOnly;
    assert_eq!(check(calls), Err(FrozenCallError::WrongContext));
    let mut calls = fixture.calls.clone();
    calls[0].context.region = EvaluationRegionId::new(123);
    assert_eq!(check(calls), Err(FrozenCallError::WrongContext));
    let mut calls = fixture.calls.clone();
    calls[0].effects.proof_scope = CallProofScope::Region(EvaluationRegionId::new(123));
    assert_eq!(check(calls), Err(FrozenCallError::WrongProofScope));
    let mut calls = fixture.calls.clone();
    calls[0].effects.argument_control = ArgumentControl::TypeOnly;
    assert_eq!(check(calls), Err(FrozenCallError::WrongControl));
    let checked = fixture.checked().unwrap();
    let foreign = scalar_fixture(2, 8, &mut fixture_constants);
    assert_eq!(
        checked.validate_fragment(&foreign.fragment, &foreign.uses, &Control::default()),
        Err(FrozenCallError::WrongFragment)
    );
    assert!(
        checked
            .binding(&foreign.fragment, &foreign.uses, fixture.calls[0].site)
            .is_none()
    );
}

#[test]
fn malformed_full_effect_shape_and_duplicate_environment_are_rejected() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fixture = scalar_fixture(1, 11, &mut fixture_constants);
    let check = |effects| {
        let mut calls = fixture.calls.clone();
        calls[0].effects = effects;
        FrozenFragmentCalls::try_new(&fixture.fragment, &fixture.uses, calls, &Control::default())
    };
    let mut invalid = fixture.calls[0].effects.clone();
    invalid.instance_state = FunctionInstanceState::TableInstance;
    assert_eq!(
        check(invalid),
        Err(FrozenCallError::InvalidEffects(
            EffectContractError::KindMismatch
        ))
    );
    let mut invalid = fixture.calls[0].effects.clone();
    invalid.own_row_error = FunctionIntrinsicRowError::NotRowEvaluated;
    assert_eq!(
        check(invalid),
        Err(FrozenCallError::InvalidEffects(
            EffectContractError::KindMismatch
        ))
    );
    let mut invalid = fixture.calls[0].effects.clone();
    invalid.environment = Box::from([
        SemanticParameterRef {
            id: SemanticParameterId::new(0),
            expected_key: SemanticParameterKey::TimeZone,
        },
        SemanticParameterRef {
            id: SemanticParameterId::new(u32::MAX),
            expected_key: SemanticParameterKey::TimeZone,
        },
    ]);
    assert_eq!(
        check(invalid),
        Err(FrozenCallError::InvalidEffects(
            EffectContractError::InvalidEnvironmentReference
        ))
    );
}

fn case_fixture(
    fixture_constants: &mut crate::tests::constant_fixture::ConstantFixture,
) -> Fixture {
    let mut builder = FragmentBuilder::new(FragmentId::new(31));
    let node = builder.reserve_node_id().unwrap();
    let function = builder
        .add_expression(
            node,
            boolean(),
            ExprKind::FunctionCall {
                function: function(FunctionKind::Scalar, boolean()),
                args: Box::default(),
            },
        )
        .unwrap();
    let otherwise = builder
        .add_expression(
            node,
            (boolean()).clone(),
            fixture_constants.boolean(false, &(boolean())),
        )
        .unwrap();
    let case = builder
        .add_expression(
            node,
            boolean(),
            ExprKind::Case {
                operand: None,
                when_then: Box::from([(function, function)]),
                else_expr: Some(otherwise),
            },
        )
        .unwrap();
    let output = builder
        .add_value(
            boolean(),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    install_node(
        &mut builder,
        node,
        vec![],
        vec![output],
        NodeKind::Values {
            rows: Box::from([Box::from([case])]),
        },
    );
    let fragment = builder
        .finish_definition(
            node,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let domains = vec![
        domain(0),
        ExpressionEvaluationRegion {
            id: EvaluationRegionId::new(12),
            parent: Some(EvaluationRegionId::new(0)),
            guard: Some(RegionGuard {
                owner: ExpressionUseId::new(9),
                kind: GuardKind::CaseWhen { arm: 0 },
            }),
        },
        ExpressionEvaluationRegion {
            id: EvaluationRegionId::new(10),
            parent: Some(EvaluationRegionId::new(0)),
            guard: Some(RegionGuard {
                owner: ExpressionUseId::new(9),
                kind: GuardKind::CaseThen { arm: 0 },
            }),
        },
        ExpressionEvaluationRegion {
            id: EvaluationRegionId::new(11),
            parent: Some(EvaluationRegionId::new(0)),
            guard: Some(RegionGuard {
                owner: ExpressionUseId::new(9),
                kind: GuardKind::CaseElse,
            }),
        },
    ];
    let invocations = vec![
        ExpressionInvocation {
            context: context(9, 0, EvaluationDemand::Value),
            definition: case,
            control: ControlShape::Case {
                simple: false,
                arms: 1,
                has_else: true,
            },
            arguments: Box::from([
                ExpressionUseId::new(0),
                ExpressionUseId::new(u32::MAX),
                ExpressionUseId::new(2),
            ]),
        },
        ExpressionInvocation {
            context: context(0, 12, EvaluationDemand::TruthOnly),
            definition: function,
            control: ControlShape::Eager,
            arguments: Box::default(),
        },
        ExpressionInvocation {
            context: context(u32::MAX, 10, EvaluationDemand::Value),
            definition: function,
            control: ControlShape::Eager,
            arguments: Box::default(),
        },
        ExpressionInvocation {
            context: context(2, 11, EvaluationDemand::Value),
            definition: otherwise,
            control: ControlShape::Eager,
            arguments: Box::default(),
        },
    ];
    let flow = ExpressionControlFlow::try_new(
        domains,
        invocations,
        fragment.expressions(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    let uses = PhysicalRootUses::try_new(
        &fragment,
        flow,
        vec![(
            ExpressionRootSite {
                node,
                role: ExpressionRootRole::ValuesCell { row: 0, column: 0 },
            },
            ExpressionUseId::new(9),
        )],
        &Control::default(),
    )
    .unwrap();
    let calls = [0, u32::MAX]
        .map(|id| {
            let context = uses.flow().uses()[&ExpressionUseId::new(id)].context;
            FrozenPhysicalCall {
                materialized_writer_input: None,
                regexp_count_pattern_source: None,
                to_base64_byte_source: None,
                temporal_source: None,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                site: PhysicalCallSite::Expression(context.use_id),
                context,
                effects: effects(FunctionKind::Scalar, context.region),
            }
        })
        .to_vec();
    Fixture {
        fragment,
        uses,
        calls,
    }
}

#[test]
fn one_scalar_definition_has_independent_truth_and_guarded_value_occurrences() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut fixture = case_fixture(&mut fixture_constants);
    fixture.calls[1].effects.own_row_error = FunctionIntrinsicRowError::MayRaise;
    let checked = fixture.checked().unwrap();
    let first = &checked.entries()[&PhysicalCallSite::Expression(ExpressionUseId::new(0))];
    let then = &checked.entries()[&PhysicalCallSite::Expression(ExpressionUseId::new(u32::MAX))];
    assert_eq!(first.context.demand, EvaluationDemand::TruthOnly);
    assert_eq!(then.context.demand, EvaluationDemand::Value);
    assert_ne!(first.context.region, then.context.region);
    assert_eq!(
        then.effects.proof_scope,
        CallProofScope::Region(then.context.region)
    );
    assert_eq!(
        then.effects.own_row_error,
        FunctionIntrinsicRowError::MayRaise
    );
    assert_eq!(
        fixture.uses.flow().uses()[&first.context.use_id].definition,
        fixture.uses.flow().uses()[&then.context.use_id].definition
    );
    let mut wrong = fixture.calls.clone();
    wrong[1].effects.proof_scope = first.effects.proof_scope;
    assert_eq!(
        FrozenFragmentCalls::try_new(&fixture.fragment, &fixture.uses, wrong, &Control::default()),
        Err(FrozenCallError::WrongProofScope)
    );
}

fn special_fixture(
    fixture_constants: &mut crate::tests::constant_fixture::ConstantFixture,
) -> Fixture {
    special_fixture_with_rows(1, fixture_constants)
}

fn special_fixture_with_rows(
    input_rows: usize,
    fixture_constants: &mut crate::tests::constant_fixture::ConstantFixture,
) -> Fixture {
    let mut builder = FragmentBuilder::new(FragmentId::new(41));
    let (input, _) = add_values(&mut builder, input_rows, false, fixture_constants);
    let aggregate = builder.reserve_node_id().unwrap();
    let mut outputs = Vec::new();
    let mut aggregate_calls = Vec::new();
    for id in [0, u32::MAX] {
        let id = AggregateCallId::new(id);
        let output = builder
            .add_value(integer(), ValueOrigin::AggregateResult { call: id })
            .unwrap();
        outputs.push(output);
        aggregate_calls.push(AggregateCall {
            id,
            binding: AggregateBinding {
                state_interpretation: None,
                state_argument_contract:
                    novarocks_type_contract::AggregateStateArgumentContract::ExactSignature,
                function: function(FunctionKind::Aggregate, integer()),
                phase: AggregatePhase::Single,
                logical_argument_count: 0,
                intermediate_type: ValueType::new(DataType::Binary, false),
                state_format: AggregateStateFormatId::try_new("fixture/count/state-v1").unwrap(),
            },
            arguments: Box::default(),
            distinct: false,
            order_by: Box::default(),
            output,
        });
    }
    install_node(
        &mut builder,
        aggregate,
        vec![input],
        outputs.clone(),
        NodeKind::Aggregate {
            group_by: Box::default(),
            calls: aggregate_calls.into_boxed_slice(),
            grouping: AggregateGrouping::Complete,
        },
    );
    let window = builder.reserve_node_id().unwrap();
    let expression = builder
        .add_expression(
            window,
            integer(),
            ExprKind::WindowCall {
                function: function(FunctionKind::Window, integer()),
                distinct: false,
                args: Box::default(),
                function_order_by: Box::default(),
                frame: None,
                ignore_nulls: false,
                aggregate_binding: None,
            },
        )
        .unwrap();
    let output = builder
        .add_value(
            integer(),
            ValueOrigin::Expr {
                node: window,
                expr: expression,
            },
        )
        .unwrap();
    outputs.push(output);
    install_node(
        &mut builder,
        window,
        vec![aggregate],
        outputs,
        NodeKind::Window(WindowSpec {
            partition_by: Box::default(),
            order_by: Box::default(),
            expressions: Box::from([WindowExpression { expression, output }]),
        }),
    );
    let table = builder.reserve_node_id().unwrap();
    let output = builder
        .add_value(
            integer(),
            ValueOrigin::NodeOutput {
                node: table,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let scalar_fields = function(FunctionKind::Table, integer());
    install_node(
        &mut builder,
        table,
        vec![window],
        vec![output],
        NodeKind::TableFunction {
            function: BoundTableFunction {
                function_id: scalar_fields.function_id,
                overload: scalar_fields.overload,
                argument_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                    Box::default(),
                ),
                result_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from(
                    [integer()],
                )),
                legacy_metadata: Some(crate::LegacyBindingMetadata {
                    volatility: scalar_fields.legacy_metadata.as_ref().unwrap().volatility,
                    argument_evaluation: scalar_fields
                        .legacy_metadata
                        .as_ref()
                        .unwrap()
                        .argument_evaluation,
                    failure_behavior: scalar_fields
                        .legacy_metadata
                        .as_ref()
                        .unwrap()
                        .failure_behavior,
                    intrinsic_row_error: scalar_fields
                        .legacy_metadata
                        .as_ref()
                        .unwrap()
                        .intrinsic_row_error,
                    semantic_parameters: Box::default(),
                }),
            },
            arguments: Box::default(),
            outputs: Box::from([TableFunctionOutput::FunctionResult {
                result_ordinal: 0,
                value: output,
            }]),
            left_outer: false,
        },
    );
    let fragment = builder
        .finish_definition(
            table,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    validate_fragment_definition(&fragment).unwrap();
    let uses = leaf_roots(&fragment);
    let mut calls = vec![];
    // Wide Values roots consume sequential low IDs plus MAX. Relational IDs
    // are independent fresh occurrences, never reused graph roots.
    let special_base = if input_rows == 1 {
        1
    } else {
        u32::try_from(MAX_CONTROL_USE_REFERENCES).unwrap() - 2
    };
    for (call, use_id) in [(0, special_base), (1, special_base + 1)] {
        let context = context(use_id, 0, EvaluationDemand::Value);
        calls.push(FrozenPhysicalCall {
            materialized_writer_input: None,
            regexp_count_pattern_source: None,
            to_base64_byte_source: None,
            temporal_source: None,
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            site: PhysicalCallSite::Aggregate {
                node: aggregate,
                call,
            },
            context,
            effects: effects(FunctionKind::Aggregate, context.region),
        });
    }
    let window_use = uses
        .flow()
        .uses()
        .values()
        .find(|invocation| invocation.definition == expression)
        .unwrap()
        .context;
    calls.push(FrozenPhysicalCall {
        materialized_writer_input: None,
        regexp_count_pattern_source: None,
        to_base64_byte_source: None,
        temporal_source: None,
        decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        site: PhysicalCallSite::Expression(window_use.use_id),
        context: window_use,
        effects: effects(FunctionKind::Window, window_use.region),
    });
    let context = context(special_base + 2, u32::MAX, EvaluationDemand::Value);
    calls.push(FrozenPhysicalCall {
        materialized_writer_input: None,
        regexp_count_pattern_source: None,
        to_base64_byte_source: None,
        temporal_source: None,
        decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        site: PhysicalCallSite::Table { node: table },
        context,
        effects: effects(FunctionKind::Table, context.region),
    });
    Fixture {
        fragment,
        uses,
        calls,
    }
}

#[test]
fn real_aggregate_window_table_positions_borrow_selected_bindings() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fixture = special_fixture(&mut fixture_constants);
    let checked = fixture.checked().unwrap();
    assert_eq!(checked.entries().len(), 4);
    for call in &fixture.calls {
        let binding = checked
            .binding(&fixture.fragment, &fixture.uses, call.site)
            .unwrap();
        match (call.site, binding) {
            (
                PhysicalCallSite::Aggregate { node, call },
                PhysicalCallBinding::Aggregate(actual),
            ) => {
                let NodeKind::Aggregate {
                    calls, grouping, ..
                } = &fixture.fragment.nodes()[&node].kind
                else {
                    panic!("actual aggregate")
                };
                assert_eq!(*grouping, AggregateGrouping::Complete);
                assert!(std::ptr::eq(actual, &calls[call as usize].binding));
                assert_eq!(actual.phase, AggregatePhase::Single);
                assert_eq!(actual.logical_argument_count, 0);
            }
            (
                PhysicalCallSite::Expression(id),
                PhysicalCallBinding::Window {
                    function: actual,
                    aggregate,
                },
            ) => {
                let ExprKind::WindowCall { function, .. } = &fixture
                    .fragment
                    .expressions()
                    .get(fixture.uses.flow().uses()[&id].definition)
                    .unwrap()
                    .kind
                else {
                    panic!("actual window")
                };
                assert!(std::ptr::eq(actual, function));
                assert!(aggregate.is_none());
            }
            (PhysicalCallSite::Table { node }, PhysicalCallBinding::Table(actual)) => {
                let NodeKind::TableFunction { function, .. } =
                    &fixture.fragment.nodes()[&node].kind
                else {
                    panic!("actual table")
                };
                assert!(std::ptr::eq(actual, function));
                assert_eq!(
                    actual.result_types,
                    novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from(
                        [integer()]
                    ))
                );
            }
            _ => panic!("wrong binding family"),
        }
    }
}

#[test]
fn relational_occurrences_require_fresh_ids_value_demand_actual_domain_and_kind() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fixture = special_fixture(&mut fixture_constants);
    let check = |calls| {
        FrozenFragmentCalls::try_new(&fixture.fragment, &fixture.uses, calls, &Control::default())
    };
    let mut wrong = fixture.calls.clone();
    wrong[0].context.use_id = ExpressionUseId::new(0);
    assert_eq!(check(wrong), Err(FrozenCallError::SharedUse));
    let mut wrong = fixture.calls.clone();
    wrong[1].context.use_id = wrong[0].context.use_id;
    assert_eq!(check(wrong), Err(FrozenCallError::SharedUse));
    let mut wrong = fixture.calls.clone();
    wrong[0].context.demand = EvaluationDemand::TruthOnly;
    assert_eq!(check(wrong), Err(FrozenCallError::WrongContext));
    let mut wrong = fixture.calls.clone();
    wrong[0].context.region = EvaluationRegionId::new(123);
    assert_eq!(check(wrong), Err(FrozenCallError::InvalidRegion));
    let mut wrong = fixture.calls.clone();
    wrong[0].effects.argument_control = ArgumentControl::Eager;
    assert_eq!(
        check(wrong),
        Err(FrozenCallError::InvalidEffects(
            EffectContractError::KindMismatch
        ))
    );
    let mut wrong = fixture.calls.clone();
    wrong.pop();
    assert_eq!(
        check(wrong),
        Err(FrozenCallError::MissingSite(
            fixture.calls.last().unwrap().site
        ))
    );
    let mut wrong = fixture.calls.clone();
    wrong[0].site = PhysicalCallSite::WriterPartial {
        node: fixture.fragment.root(),
        call: 0,
    };
    assert_eq!(
        check(wrong),
        Err(FrozenCallError::MissingSite(fixture.calls[0].site))
    );
}

#[test]
fn combined_expression_and_relational_occurrences_share_one_near_over_budget() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    for (rows, accepted) in [
        (MAX_CONTROL_USE_REFERENCES - 4, true),
        (MAX_CONTROL_USE_REFERENCES - 3, false),
    ] {
        let fixture = special_fixture_with_rows(rows, &mut fixture_constants);
        // Actual Values rows plus one Window call are expression uses; two
        // Aggregate calls and one Table call add three distinct occurrences.
        assert_eq!(fixture.uses.flow().use_reference_count(), rows + 1);
        assert_eq!(fixture.calls.len(), 4);
        let mut special_ids = BTreeSet::new();
        for call in &fixture.calls {
            if !matches!(call.site, PhysicalCallSite::Expression(_)) {
                assert!(
                    !fixture
                        .uses
                        .flow()
                        .uses()
                        .contains_key(&call.context.use_id)
                );
                assert!(special_ids.insert(call.context.use_id));
            }
        }
        assert_eq!(special_ids.len(), 3);
        assert_eq!(
            rows + 4,
            MAX_CONTROL_USE_REFERENCES + usize::from(!accepted)
        );
        let result = fixture.checked();
        if accepted {
            let checked = result.expect("the combined exact boundary must be admitted");
            assert_eq!(checked.entries().len(), 4);
            checked
                .validate_fragment(&fixture.fragment, &fixture.uses, &Control::default())
                .unwrap();
        } else {
            assert_eq!(result, Err(FrozenCallError::TooManyItems));
        }
    }
}

#[test]
fn entry_and_mid_quantum_compile_failures_keep_exact_outer_categories() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fixture = scalar_fixture(300, 71, &mut fixture_constants);
    let checked = fixture.checked().unwrap();
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for positive_only in [false, true] {
            let control = Control {
                stop: Some(error.clone()),
                positive_only,
                observations: Mutex::default(),
            };
            assert_eq!(
                FrozenFragmentCalls::try_new(
                    &fixture.fragment,
                    &fixture.uses,
                    fixture.calls.clone(),
                    &control
                ),
                Err(FrozenCallError::Control(error.clone()))
            );
            let observations = control.observations.lock().unwrap();
            assert_eq!(observations[0], (CompilePhase::Validate, 0));
            if positive_only {
                assert_eq!(observations.last(), Some(&(CompilePhase::Validate, 256)));
            }
            drop(observations);
            let control = Control {
                stop: Some(error.clone()),
                positive_only,
                observations: Mutex::default(),
            };
            assert_eq!(
                checked.validate_fragment(&fixture.fragment, &fixture.uses, &control),
                Err(FrozenCallError::Control(error.clone()))
            );
            let observations = control.observations.lock().unwrap();
            if positive_only {
                assert_eq!(observations.last(), Some(&(CompilePhase::Validate, 256)));
            }
        }
    }
}

#[test]
fn call_count_preflight_rejects_over_budget_without_visiting_entries() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fixture = scalar_fixture(1, 81, &mut fixture_constants);
    let calls = vec![fixture.calls[0].clone(); MAX_CONTROL_USE_REFERENCES + 1];
    let control = Control::default();
    assert_eq!(
        FrozenFragmentCalls::try_new(&fixture.fragment, &fixture.uses, calls, &control),
        Err(FrozenCallError::TooManyItems)
    );
    assert_eq!(
        *control.observations.lock().unwrap(),
        vec![(CompilePhase::Validate, 0), (CompilePhase::Validate, 0)]
    );
}

#[test]
fn type_only_parent_retains_static_child_binding_without_runtime_child_claim() {
    let mut builder = FragmentBuilder::new(FragmentId::new(91));
    let node = builder.reserve_node_id().unwrap();
    let child = builder
        .add_expression(
            node,
            boolean(),
            ExprKind::FunctionCall {
                function: function(FunctionKind::Scalar, boolean()),
                args: Box::default(),
            },
        )
        .unwrap();
    let mut selected_parent = function(FunctionKind::Scalar, boolean());
    selected_parent.function_id = FunctionId::try_new("fixture/frozen-calls/inspect-type").unwrap();
    selected_parent.argument_types =
        novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from([
            FunctionArgumentType::Value(boolean()),
        ]));
    let parent = builder
        .add_expression(
            node,
            boolean(),
            ExprKind::FunctionCall {
                function: selected_parent,
                args: Box::from([child]),
            },
        )
        .unwrap();
    let output = builder
        .add_value(
            boolean(),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    install_node(
        &mut builder,
        node,
        vec![],
        vec![output],
        NodeKind::Values {
            rows: Box::from([Box::from([parent])]),
        },
    );
    let fragment = builder
        .finish_definition(
            node,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let current = context(0, u32::MAX, EvaluationDemand::Value);
    let flow = ExpressionControlFlow::try_new(
        vec![domain(u32::MAX)],
        vec![ExpressionInvocation {
            context: current,
            definition: parent,
            control: ControlShape::TypeOnly,
            arguments: Box::default(),
        }],
        fragment.expressions(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    let uses = PhysicalRootUses::try_new(
        &fragment,
        flow,
        vec![(
            ExpressionRootSite {
                node,
                role: ExpressionRootRole::ValuesCell { row: 0, column: 0 },
            },
            current.use_id,
        )],
        &Control::default(),
    )
    .unwrap();
    let mut claimed = effects(FunctionKind::Scalar, current.region);
    claimed.argument_control = ArgumentControl::TypeOnly;
    let parent_call = FrozenPhysicalCall {
        materialized_writer_input: None,
        regexp_count_pattern_source: None,
        to_base64_byte_source: None,
        temporal_source: None,
        decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        site: PhysicalCallSite::Expression(current.use_id),
        context: current,
        effects: claimed,
    };
    let checked = FrozenFragmentCalls::try_new(
        &fragment,
        &uses,
        vec![parent_call.clone()],
        &Control::default(),
    )
    .unwrap();
    assert_eq!(fragment.expressions().len(), 2);
    assert_eq!(uses.flow().uses().len(), 1);
    assert!(uses.flow().uses()[&current.use_id].arguments.is_empty());
    assert_eq!(checked.entries().len(), 1);
    let Some(PhysicalCallBinding::Scalar(actual)) =
        checked.binding(&fragment, &uses, parent_call.site)
    else {
        panic!("type-only parent binding is required");
    };
    assert_eq!(
        actual.argument_types,
        novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from([
            FunctionArgumentType::Value(boolean())
        ]))
    );
    let ExprKind::FunctionCall { args, .. } = &fragment.expressions().get(parent).unwrap().kind
    else {
        panic!("actual parent definition is required");
    };
    assert_eq!(args.as_ref(), &[child]);
    let child_site = PhysicalCallSite::Expression(ExpressionUseId::new(u32::MAX));
    assert!(checked.binding(&fragment, &uses, child_site).is_none());
    let mut child_claim = parent_call.clone();
    child_claim.site = child_site;
    child_claim.context.use_id = ExpressionUseId::new(u32::MAX);
    child_claim.effects.argument_control = ArgumentControl::Eager;
    assert_eq!(
        FrozenFragmentCalls::try_new(
            &fragment,
            &uses,
            vec![parent_call, child_claim],
            &Control::default(),
        ),
        Err(FrozenCallError::InvalidSite),
    );
}

#[test]
fn decimal_policy_is_independent_of_equal_effects_and_call_context() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    use novarocks_type_contract::DecimalOverflowPolicy::{OutputNull, ReportError};

    let mut fixture = scalar_fixture(1, 83, &mut fixture_constants);
    let output_null = fixture.checked().unwrap();
    let site = fixture.calls[0].site;
    fixture.calls[0].decimal_overflow_policy = ReportError;
    let report_error = fixture.checked().unwrap();
    let before = &output_null.entries()[&site];
    let after = &report_error.entries()[&site];
    assert_eq!(before.context, after.context);
    assert_eq!(before.effects, after.effects);
    assert_eq!(before.decimal_overflow_policy, OutputNull);
    assert_eq!(after.decimal_overflow_policy, ReportError);
    assert_ne!(before, after);
    assert_ne!(output_null, report_error);
    // Rechecking this exact snapshot validates shape, not an invented policy
    // derived from the selected signature or its unchanged NoRowError claim.
    report_error
        .validate_fragment(&fixture.fragment, &fixture.uses, &Control::default())
        .unwrap();
    assert_eq!(report_error.entries()[&site], fixture.calls[0]);
}

#[test]
fn shared_definition_occurrences_keep_distinct_decimal_policies_in_one_table() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    use novarocks_type_contract::DecimalOverflowPolicy::{OutputNull, ReportError};

    let mut fixture = scalar_fixture(2, 84, &mut fixture_constants);
    for call in &mut fixture.calls {
        call.effects.proof_scope = CallProofScope::Unconditional;
    }
    fixture.calls[0].decimal_overflow_policy = ReportError;
    fixture.calls[1].decimal_overflow_policy = OutputNull;
    assert_eq!(fixture.calls[0].effects, fixture.calls[1].effects);
    let checked = fixture.checked().unwrap();
    let first = fixture.uses.flow().uses()[&fixture.calls[0].context.use_id].definition;
    let second = fixture.uses.flow().uses()[&fixture.calls[1].context.use_id].definition;
    assert_eq!(first, second);
    let Some(PhysicalCallBinding::Scalar(first_binding)) =
        checked.binding(&fixture.fragment, &fixture.uses, fixture.calls[0].site)
    else {
        panic!("missing first exact scalar binding")
    };
    let Some(PhysicalCallBinding::Scalar(second_binding)) =
        checked.binding(&fixture.fragment, &fixture.uses, fixture.calls[1].site)
    else {
        panic!("missing second exact scalar binding")
    };
    assert!(std::ptr::eq(first_binding, second_binding));
    for (call, policy) in fixture.calls.iter().zip([ReportError, OutputNull]) {
        assert_eq!(
            checked.entries()[&call.site].decimal_overflow_policy,
            policy
        );
    }
    checked
        .validate_fragment(&fixture.fragment, &fixture.uses, &Control::default())
        .unwrap();
    assert_eq!(checked, fixture.checked().unwrap());
}

#[test]
fn relational_and_window_sites_preserve_their_own_explicit_decimal_policy() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    use novarocks_type_contract::DecimalOverflowPolicy::{OutputNull, ReportError};

    let mut fixture = special_fixture(&mut fixture_constants);
    let policies = [ReportError, OutputNull, ReportError, OutputNull];
    for (call, policy) in fixture.calls.iter_mut().zip(policies) {
        call.decimal_overflow_policy = policy;
    }
    let checked = fixture.checked().unwrap();
    assert!(matches!(
        fixture.calls[0].site,
        PhysicalCallSite::Aggregate { call: 0, .. }
    ));
    assert!(matches!(
        fixture.calls[1].site,
        PhysicalCallSite::Aggregate { call: 1, .. }
    ));
    assert!(matches!(
        fixture.calls[2].site,
        PhysicalCallSite::Expression(_)
    ));
    assert!(matches!(
        fixture.calls[3].site,
        PhysicalCallSite::Table { .. }
    ));
    for (call, policy) in fixture.calls.iter().zip(policies) {
        assert_eq!(
            checked.entries()[&call.site].decimal_overflow_policy,
            policy
        );
        assert!(
            checked
                .binding(&fixture.fragment, &fixture.uses, call.site)
                .is_some()
        );
    }
    checked
        .validate_fragment(&fixture.fragment, &fixture.uses, &Control::default())
        .unwrap();
    assert_eq!(checked, fixture.checked().unwrap());
}

#[path = "visit_tests.rs"]
mod visit_tests;

#[path = "property_derivation_tests.rs"]
mod property_derivation_tests;

#[path = "all_property_derivation_tests.rs"]
mod all_property_derivation_tests;

#[path = "owned_property_finalization_tests.rs"]
mod owned_property_finalization_tests;
