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
use arrow::datatypes::DataType;
use novarocks_expression_contract::{BoundFunction, ExprId, ExprKind};
use novarocks_physical_plan::*;
use novarocks_type_contract::{
    AggregateStateFormatId, CompileControlError, CompilePhase, ControlShape, EvaluationDemand,
    ExpressionControlFlow, ExpressionEvaluationRegion, ExpressionInvocation,
    FunctionArgumentEvaluation, FunctionId, FunctionKind, FunctionOverloadId, SemanticParameterId,
    SemanticParameterKey, SemanticParameterRef,
};
use std::sync::Mutex;

#[derive(Default)]
struct TestControl {
    failure: Option<(usize, CompileControlError)>,
    events: Mutex<Vec<(CompilePhase, u32)>>,
}
impl PureCompileControl for TestControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut events = self.events.lock().unwrap();
        events.push((phase, units));
        if let Some((at, error)) = self.failure.clone()
            && at == events.len()
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

        legacy_metadata: Some(novarocks_expression_contract::LegacyBindingMetadata {
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
fn effects(kind: FunctionKind, proof_scope: CallProofScope) -> CallEffects {
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
        proof_scope,
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
#[derive(Debug)]
enum ClaimError {
    Codec(E),
    Unit(novarocks_expression_contract::UnitError),
}
impl From<E> for ClaimError {
    fn from(value: E) -> Self {
        Self::Codec(value)
    }
}
impl From<CompileControlError> for ClaimError {
    fn from(value: CompileControlError) -> Self {
        Self::Codec(E::Control(value))
    }
}
impl From<novarocks_expression_contract::UnitError> for ClaimError {
    fn from(value: novarocks_expression_contract::UnitError) -> Self {
        match value {
            novarocks_expression_contract::UnitError::Control(cause) => {
                Self::Codec(E::Control(cause))
            }
            other => Self::Unit(other),
        }
    }
}
fn unit_limits() -> novarocks_expression_contract::UnitLimits {
    novarocks_expression_contract::UnitLimits {
        expression_semantic_depth: PlanLimits::FROZEN.expression_semantic_depth,
    }
}
fn projection_limits() -> NodeProjectionLimits {
    NodeProjectionLimits {
        max_input_nodes: usize::MAX,
        max_value_references: usize::MAX,
        max_list_items: usize::MAX,
        max_allocation_requests: usize::MAX,
        max_allocation_request_bytes: usize::MAX,
        max_coexisting_source_and_request_bytes: usize::MAX,
        max_work: usize::MAX,
        properties: crate::physical_properties_v2::PhysicalPropertyProjectionLimits {
            max_value_references: usize::MAX,
            max_allocation_requests: usize::MAX,
            max_allocation_request_bytes: usize::MAX,
            max_coexisting_source_and_request_bytes: usize::MAX,
            max_work: usize::MAX,
        },
    }
}
struct Fixture {
    unit: novarocks_expression_contract::BoundExprSet,
    calls: Vec<novarocks_expression_contract::ExpressionCallClaim>,
}
impl Fixture {
    /// Prepare the receiving structural model from this explicitly pure
    /// fixture before entering its controlled decoder. No finite creator
    /// storage is copied or erased; those laws use the real writer fixture.
    fn receiving_parts(&self) -> novarocks_expression_contract::UnitParts {
        novarocks_expression_contract::UnitParts::new(
            self.unit.ports().to_vec().into_boxed_slice(),
            self.unit.definitions().to_vec().into_boxed_slice(),
            self.unit.roots().to_vec().into_boxed_slice(),
            self.unit.root_use_ids().to_vec().into_boxed_slice(),
            self.unit.control_flow().clone(),
            self.unit.claims().clone(),
            self.unit.requests().clone(),
        )
    }
    fn checked(
        &self,
    ) -> Result<novarocks_expression_contract::BoundExprSet, novarocks_expression_contract::UnitError>
    {
        let mut parts = self.receiving_parts();
        parts.claims = self
            .calls
            .iter()
            .cloned()
            .map(|claim| (claim.context.use_id, claim))
            .collect();
        let control = TestControl::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        let result = novarocks_expression_contract::BoundExprSet::try_from_parts_in(
            parts,
            unit_limits(),
            &mut work,
        )?;
        work.finish().unwrap();
        Ok(result)
    }
}
fn scalar_fixture(count: usize, _retired_fragment_id: u32) -> Fixture {
    use novarocks_expression_contract::{BoundExprSetBuilder, DefinitionControl, RootScope};
    let mut builder = BoundExprSetBuilder::new(unit_limits());
    let definition = builder
        .add(
            None,
            boolean(),
            ExprKind::FunctionCall {
                function: function(FunctionKind::Scalar, boolean()),
                args: Box::default(),
            },
        )
        .unwrap();
    builder
        .definition_control(
            definition,
            DefinitionControl {
                shape: ControlShape::Eager,
                temporal_sources: None,
            },
        )
        .unwrap();
    builder
        .request(
            definition,
            novarocks_expression_contract::PhysicalCallRequest {
                arguments: Box::default(),
                logical_argument_count: 0,
                expected_result_type: None,
            },
        )
        .unwrap();
    for _ in 0..count {
        builder
            .root(definition, EvaluationDemand::Value, RootScope::AllPorts)
            .unwrap();
    }
    let control = TestControl::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let sealed = builder.seal(vec![], &mut work).unwrap();
    let claims = sealed
        .uses()
        .iter()
        .map(|(&id, invocation)| {
            (
                id,
                novarocks_expression_contract::ExpressionCallClaim {
                    context: invocation.context,
                    effects: effects(
                        FunctionKind::Scalar,
                        CallProofScope::Region(invocation.context.region),
                    ),
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                    temporal_source: None,
                },
            )
        })
        .collect();
    let unit = sealed.finish(claims, &mut work).unwrap();
    work.finish().unwrap();
    // Keep the old independent sparse use/region inputs at the new unit owner.
    let mut parts = unit.into_parts();
    let root_uses = (0..count)
        .map(|ordinal| {
            ExpressionUseId::new(match ordinal {
                0 => 0,
                1 => u32::MAX,
                other => other as u32 - 1,
            })
        })
        .collect::<Vec<_>>();
    let uses = root_uses
        .iter()
        .enumerate()
        .map(|(ordinal, &id)| ExpressionInvocation {
            context: context(
                id.get(),
                if ordinal % 2 == 0 { 0 } else { u32::MAX },
                EvaluationDemand::Value,
            ),
            definition,
            control: ControlShape::Eager,
            arguments: Box::default(),
        })
        .collect();
    struct Membership(usize);
    impl novarocks_type_contract::ExpressionDefinitionMembership<ExprId> for Membership {
        fn definition_count(&self) -> usize {
            self.0
        }
        fn contains_definition(&self, id: ExprId) -> bool {
            (id.get() as usize) < self.0
        }
    }
    parts.control = ExpressionControlFlow::try_new(
        if count > 1 {
            vec![domain(0), domain(u32::MAX)]
        } else {
            vec![domain(0)]
        },
        uses,
        &Membership(parts.definitions.len()),
        CompilePhase::Validate,
        &control,
    )
    .unwrap();
    parts.root_uses = root_uses.into_boxed_slice();
    parts.claims = parts
        .control
        .uses()
        .iter()
        .map(|(&id, invocation)| {
            (
                id,
                novarocks_expression_contract::ExpressionCallClaim {
                    context: invocation.context,
                    effects: effects(
                        FunctionKind::Scalar,
                        CallProofScope::Region(invocation.context.region),
                    ),
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                    temporal_source: None,
                },
            )
        })
        .collect();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let unit = novarocks_expression_contract::BoundExprSet::try_from_parts_in(
        parts,
        unit_limits(),
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    let calls = unit.claims().values().cloned().collect();
    Fixture { unit, calls }
}
fn encode_checked(
    input: &novarocks_expression_contract::BoundExprSet,
    control: &dyn PureCompileControl,
) -> Result<wire::ExpressionClaims, E> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = encode_expression_claims_in(
        input,
        64 * 1024 * 1024,
        projection_limits(),
        &mut |_| Ok(()),
        &mut work,
    )
    .map(|(wire, _)| wire);
    if matches!(&result, Err(E::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn decode_checked(
    fixture: &Fixture,
    input: &wire::ExpressionClaims,
    control: &dyn PureCompileControl,
) -> Result<novarocks_expression_contract::BoundExprSet, ClaimError> {
    let mut parts = fixture.receiving_parts();
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = (|| {
        let (claims, _) = decode_expression_claims_in::<std::convert::Infallible>(
            input,
            64 * 1024 * 1024,
            projection_limits(),
            &mut |_| Ok(()),
            None,
            &mut work,
        )
        .map_err(crate::host_projection_v2::ProjectionFailure::without_host)?;
        parts.claims = claims;
        Ok(
            novarocks_expression_contract::BoundExprSet::try_from_parts_in(
                parts,
                unit_limits(),
                &mut work,
            )?,
        )
    })();
    if matches!(&result, Err(ClaimError::Codec(E::Control(_)))) {
        return result;
    }
    work.finish()?;
    result
}
fn assert_unit_invalid(
    result: Result<novarocks_expression_contract::BoundExprSet, ClaimError>,
    text: &str,
) {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    match result.unwrap_err() {
        ClaimError::Unit(error) => {
            assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
            assert!(
                matches!(error,novarocks_expression_contract::UnitError::Invalid{ref message,..} if message.contains(text)),
                "{error:?}"
            );
        }
        other => panic!("expected actual unit refusal, got {other:?}"),
    }
}
struct RelationFixture {
    fragment: Fragment,
    calls: Vec<FrozenPhysicalCall>,
}
impl RelationFixture {
    fn checked(&self) -> Result<FrozenFragmentCalls, FrozenCallError> {
        FrozenFragmentCalls::try_new(&self.fragment, self.calls.clone(), &TestControl::default())
    }
}
fn relation_fixture(count: usize, id: u32) -> RelationFixture {
    let mut builder = novarocks_physical_plan::FragmentBuilder::new(FragmentId::new(id));
    let input = NodeId::new(0);
    let window = NodeId::new(u32::MAX);
    builder
        .add_values(input, Box::from([Box::default()]), Box::default())
        .unwrap();
    let mut output = Vec::new();
    let mut calls = Vec::new();
    for ordinal in 0..count {
        let value = builder
            .add_value(
                integer(),
                ValueOrigin::NodeOutput {
                    node: window,
                    output_ordinal: ordinal as u32,
                },
            )
            .unwrap();
        output.push(value);
        calls.push(WindowCallSpec {
            binding: function(FunctionKind::Window, integer()),
            distinct: false,
            arguments: Box::default(),
            order_by: Box::default(),
            frame: None,
            ignore_nulls: false,
            aggregate_binding: None,
            output: value,
        });
    }
    builder
        .insert_node_unchecked(PhysicalNode {
            id: window,
            inputs: Box::from([input]),
            required_inputs: Box::from([properties()]),
            output_properties: properties(),
            output: OutputPort {
                node: window,
                columns: output.into_boxed_slice(),
            },
            kind: NodeKind::Window(WindowSpec {
                partition_by: Box::default(),
                order_by: Box::default(),
                calls: calls.into_boxed_slice(),
            }),
            expressions: None,
        })
        .unwrap();
    let fragment = builder
        .finish_structure(
            window,
            FragmentSink::Noop,
            dop(),
            PlanLimits::FROZEN,
            &TestControl::default(),
            StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let requests = (0..count)
        .map(|ordinal| {
            (
                PhysicalCallSite::Window {
                    node: window,
                    call: ordinal as u32,
                },
                novarocks_expression_contract::PhysicalCallRequest {
                    arguments: Box::default(),
                    logical_argument_count: 0,
                    expected_result_type: None,
                },
            )
        })
        .collect();
    let fragment = fragment
        .with_call_requests_observed(requests, &TestControl::default())
        .unwrap();
    let calls = (0..count)
        .map(|ordinal| {
            relation_call(
                PhysicalCallSite::Window {
                    node: window,
                    call: ordinal as u32,
                },
                ordinal as u32,
                FunctionKind::Window,
            )
        })
        .collect();
    RelationFixture { fragment, calls }
}
fn relation_call(site: PhysicalCallSite, id: u32, kind: FunctionKind) -> FrozenPhysicalCall {
    let context = novarocks_type_contract::RelationEffectContext::call(
        novarocks_type_contract::RelationCallId::new(id),
    );
    FrozenPhysicalCall {
        site,
        context,
        materialized_writer_input: None,
        effects: effects(kind, CallProofScope::Relation(context.identity)),
        decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
    }
}
fn encode_relation_checked(
    input: &FrozenFragmentCalls,
    control: &dyn PureCompileControl,
) -> Result<wire::FrozenCalls, E> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = encode_calls(input, &mut work);
    if matches!(&result, Err(E::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn decode_relation_checked(
    fixture: &RelationFixture,
    input: &wire::FrozenCalls,
    control: &dyn PureCompileControl,
) -> Result<FrozenFragmentCalls, E> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = decode_calls(&fixture.fragment, input, &mut work, control);
    if matches!(&result, Err(E::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn special_fixture() -> RelationFixture {
    special_fixture_with_rows(1)
}
fn special_fixture_with_rows(input_rows: usize) -> RelationFixture {
    let mut builder = novarocks_physical_plan::FragmentBuilder::new(FragmentId::new(0));
    let source = NodeId::new(0);
    let aggregate = NodeId::new(7);
    let window = NodeId::new(8);
    let table = NodeId::new(u32::MAX);
    builder
        .add_values(
            source,
            (0..input_rows).map(|_| Box::default()).collect(),
            Box::default(),
        )
        .unwrap();
    let mut outputs = Vec::new();
    let mut aggregates = Vec::new();
    for ordinal in 0..2 {
        let id = AggregateCallId::new(if ordinal == 0 { 0 } else { u32::MAX });
        let output = builder
            .add_value(integer(), ValueOrigin::AggregateResult { call: id })
            .unwrap();
        outputs.push(output);
        aggregates.push(AggregateCall {
            filter_root: None,
            id,
            binding: AggregateBinding {
                function: function(FunctionKind::Aggregate, integer()),
                phase: AggregatePhase::Single,
                logical_argument_count: 0,
                intermediate_type: ValueType::new(DataType::Binary, false),
                state_format: AggregateStateFormatId::try_new("fixture/semantics-state-v1")
                    .unwrap(),
                state_argument_contract:
                    novarocks_type_contract::AggregateStateArgumentContract::ExactSignature,
                state_interpretation: None,
            },
            arguments: Box::default(),
            distinct: false,
            order_by: Box::default(),
            output,
        });
    }
    builder
        .insert_node_unchecked(PhysicalNode {
            id: aggregate,
            inputs: Box::from([source]),
            required_inputs: Box::from([properties()]),
            output_properties: properties(),
            output: OutputPort {
                node: aggregate,
                columns: outputs.clone().into_boxed_slice(),
            },
            kind: NodeKind::Aggregate {
                group_by: Box::default(),
                calls: aggregates.into_boxed_slice(),
                grouping: AggregateGrouping::Complete,
            },
            expressions: None,
        })
        .unwrap();
    let w = builder
        .add_value(
            integer(),
            ValueOrigin::NodeOutput {
                node: window,
                output_ordinal: 2,
            },
        )
        .unwrap();
    outputs.push(w);
    builder
        .insert_node_unchecked(PhysicalNode {
            id: window,
            inputs: Box::from([aggregate]),
            required_inputs: Box::from([properties()]),
            output_properties: properties(),
            output: OutputPort {
                node: window,
                columns: outputs.clone().into_boxed_slice(),
            },
            kind: NodeKind::Window(WindowSpec {
                partition_by: Box::default(),
                order_by: Box::default(),
                calls: Box::from([WindowCallSpec {
                    binding: function(FunctionKind::Window, integer()),
                    distinct: false,
                    arguments: Box::default(),
                    order_by: Box::default(),
                    frame: None,
                    ignore_nulls: false,
                    aggregate_binding: None,
                    output: w,
                }]),
            }),
            expressions: None,
        })
        .unwrap();
    let t = builder
        .add_value(
            integer(),
            ValueOrigin::NodeOutput {
                node: table,
                output_ordinal: 3,
            },
        )
        .unwrap();
    let mut result = outputs
        .iter()
        .copied()
        .map(TableFunctionOutput::PassThrough)
        .collect::<Vec<_>>();
    result.push(TableFunctionOutput::FunctionResult {
        result_ordinal: 0,
        value: t,
    });
    outputs.push(t);
    builder
        .insert_node_unchecked(PhysicalNode {
            id: table,
            inputs: Box::from([window]),
            required_inputs: Box::from([properties()]),
            output_properties: properties(),
            output: OutputPort {
                node: table,
                columns: outputs.into_boxed_slice(),
            },
            kind: NodeKind::TableFunction {
                function: BoundTableFunction::from_exact_signature(
                    FunctionId::try_new("fixture/table-semantics").unwrap(),
                    FunctionOverloadId::try_new("fixture/table-semantics-v1").unwrap(),
                    novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(Box::default()),
                    novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(Box::from([
                        integer(),
                    ])),
                ),
                arguments: Box::default(),
                outputs: result.into_boxed_slice(),
                left_outer: false,
            },
            expressions: None,
        })
        .unwrap();
    let fragment = builder
        .finish_structure(
            table,
            FragmentSink::Noop,
            dop(),
            PlanLimits::FROZEN,
            &TestControl::default(),
            StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let sites = [
        (
            PhysicalCallSite::Aggregate {
                node: aggregate,
                call: 0,
            },
            0,
            FunctionKind::Aggregate,
        ),
        (
            PhysicalCallSite::Aggregate {
                node: aggregate,
                call: 1,
            },
            u32::MAX,
            FunctionKind::Aggregate,
        ),
        (
            PhysicalCallSite::Window {
                node: window,
                call: 0,
            },
            1,
            FunctionKind::Window,
        ),
        (
            PhysicalCallSite::Table { node: table },
            2,
            FunctionKind::Table,
        ),
    ];
    let requests = sites
        .iter()
        .map(|(site, _, _)| {
            (
                *site,
                novarocks_expression_contract::PhysicalCallRequest {
                    arguments: Box::default(),
                    logical_argument_count: 0,
                    expected_result_type: None,
                },
            )
        })
        .collect();
    let fragment = fragment
        .with_call_requests_observed(requests, &TestControl::default())
        .unwrap();
    RelationFixture {
        fragment,
        calls: sites
            .into_iter()
            .map(|(site, id, kind)| relation_call(site, id, kind))
            .collect(),
    }
}

fn rich_effects(domain: EvaluationRegionId) -> CallEffects {
    CallEffects {
        value_stability: FunctionVolatility::Stable,
        own_row_error: FunctionIntrinsicRowError::MayRaise,
        failure_behavior: FunctionFailureBehavior::ReturnsNull,
        null_behavior: FunctionNullBehavior::CalledOnNull,
        argument_control: ArgumentControl::Eager,
        instance_state: FunctionInstanceState::ScalarInstance,
        observable_effects: ObservableEffects {
            rng_sampling: true,
            warnings: true,
            controlled_wait: true,
        },
        environment: Box::from([
            SemanticParameterRef {
                id: SemanticParameterId::new(u32::MAX),
                expected_key: SemanticParameterKey::TimeZone,
            },
            SemanticParameterRef {
                id: SemanticParameterId::new(0),
                expected_key: SemanticParameterKey::AllowThrowException,
            },
        ]),
        proof_scope: CallProofScope::Region(domain),
    }
}

#[test]
fn actual_scalar_occurrences_preserve_all_nine_fields_ordered_refs_and_independent_policies() {
    let mut fixture = scalar_fixture(2, u32::MAX);
    for (ordinal, call) in fixture.calls.iter_mut().enumerate() {
        call.effects = rich_effects(call.context.region);
        call.decimal_overflow_policy = if ordinal == 0 {
            DecimalOverflowPolicy::ReportError
        } else {
            DecimalOverflowPolicy::OutputNull
        };
    }
    let original = fixture.checked().unwrap();
    let dto = encode_checked(&original, &TestControl::default()).unwrap();
    let checked = decode_checked(&fixture, &dto, &TestControl::default()).unwrap();
    assert_eq!(checked, original);
    assert_eq!(
        dto.entries[0].decimal_overflow_policy,
        Some(wire::DecimalOverflowPolicy::ReportError as i32)
    );
    assert_eq!(
        dto.entries[1].decimal_overflow_policy,
        Some(wire::DecimalOverflowPolicy::OutputNull as i32)
    );
    assert_eq!(
        dto.entries[0]
            .effects
            .as_ref()
            .unwrap()
            .environment
            .iter()
            .map(|r| r.id.unwrap())
            .collect::<Vec<_>>(),
        [u32::MAX, 0]
    );
    assert_eq!(
        fixture.unit.control_flow().uses()[&ExpressionUseId::new(0)].definition,
        fixture.unit.control_flow().uses()[&ExpressionUseId::new(u32::MAX)].definition
    );
}

#[test]
fn positional_null_effects_transfer_canonical_ordinals_and_reject_malformed_claims() {
    let control = TestControl::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let mut selected = rich_effects(EvaluationRegionId::new(0));
    selected.argument_control = ArgumentControl::OrderedNull;
    selected.null_behavior = FunctionNullBehavior::StrictPositions(Box::new([0, 2]));
    let mut resources = Projection::plain();
    let wire = encode_effects(&selected, &mut resources, &mut work).unwrap();
    assert_eq!(wire.strict_argument_ordinals, [0, 2]);
    assert_eq!(
        decode_effects(&wire, &mut resources, &mut work).unwrap(),
        selected
    );
    for invalid in [vec![], vec![2, 2], vec![2, 0], vec![u32::MAX]] {
        let mut malformed = wire.clone();
        malformed.strict_argument_ordinals = invalid;
        assert!(matches!(
            decode_effects(&malformed, &mut resources, &mut work),
            Err(E::InvalidShape(_))
        ));
    }
    let mut malformed = wire;
    malformed.null_behavior = wire::NullBehavior::CalledOnNull as i32;
    assert!(matches!(
        decode_effects(&malformed, &mut resources, &mut work),
        Err(E::InvalidShape(_))
    ));
    work.finish().unwrap();
}

#[test]
fn policy_absence_unspecified_and_unknown_are_never_defaulted() {
    let fixture = scalar_fixture(1, 0);
    let dto = encode_checked(&fixture.checked().unwrap(), &TestControl::default()).unwrap();
    for policy in [None, Some(0), Some(-1), Some(i32::MAX)] {
        let mut wrong = dto.clone();
        wrong.entries[0].decimal_overflow_policy = policy;
        assert!(matches!(
            decode_checked(&fixture, &wrong, &TestControl::default()),
            Err(ClaimError::Codec(E::InvalidShape(_)))
        ));
    }
}

#[test]
fn all_six_sites_preserve_zero_max_ids_and_actual_call_ordinals() {
    for id in [0, u32::MAX] {
        for site in [
            PhysicalCallSite::Window {
                node: NodeId::new(id),
                call: u32::MAX,
            },
            PhysicalCallSite::Aggregate {
                node: NodeId::new(id),
                call: u32::MAX,
            },
            PhysicalCallSite::TopNState {
                node: NodeId::new(id),
                call: 0,
            },
            PhysicalCallSite::WriterPartial {
                node: NodeId::new(id),
                call: u32::MAX,
            },
            PhysicalCallSite::WriterFinal {
                node: NodeId::new(id),
                call: 0,
            },
            PhysicalCallSite::Table {
                node: NodeId::new(id),
            },
        ] {
            assert_eq!(decode_site(&encode_site(site)).unwrap(), site);
        }
    }
    assert!(decode_site(&wire::CallSite { kind: None }).is_err());
    let constructors: [fn(wire::NodeCallSite) -> wire::call_site::Kind; 5] = [
        wire::call_site::Kind::Window,
        wire::call_site::Kind::Aggregate,
        wire::call_site::Kind::TopNState,
        wire::call_site::Kind::WriterPartial,
        wire::call_site::Kind::WriterFinal,
    ];
    for kind in constructors {
        assert!(
            decode_site(&wire::CallSite {
                kind: Some(kind(wire::NodeCallSite {
                    node_id: None,
                    call: 0
                }))
            })
            .is_err()
        );
    }
}

#[test]
fn contexts_require_both_ids_and_closed_demand_without_rejecting_zero_max() {
    for id in [0, u32::MAX] {
        for demand in [EvaluationDemand::Value, EvaluationDemand::TruthOnly] {
            let value = context(id, id, demand);
            assert_eq!(decode_context(&encode_context(&value)).unwrap(), value);
        }
    }
    let dto = encode_context(&context(0, u32::MAX, EvaluationDemand::TruthOnly));
    let mut bad = dto;
    bad.use_id = None;
    assert!(decode_context(&bad).is_err());
    bad = dto;
    bad.region_id = None;
    assert!(decode_context(&bad).is_err());
    for demand in [0, -1, i32::MAX] {
        bad = dto;
        bad.demand = demand;
        assert!(decode_context(&bad).is_err());
    }
}

#[test]
fn every_control_and_proof_variant_projects_exactly_including_higher_order() {
    for value in [
        ArgumentControl::Eager,
        ArgumentControl::NoArguments,
        ArgumentControl::TypeOnly,
        ArgumentControl::If,
        ArgumentControl::Coalesce,
        ArgumentControl::Try,
        ArgumentControl::OrderedRegexp,
        ArgumentControl::SimpleCase,
        ArgumentControl::SearchedCase,
        ArgumentControl::Aggregate,
        ArgumentControl::Window,
        ArgumentControl::Table,
        ArgumentControl::HigherOrder {
            body_ordinal: u32::MAX,
            body_demand: EvaluationDemand::TruthOnly,
        },
        ArgumentControl::HigherOrder {
            body_ordinal: 0,
            body_demand: EvaluationDemand::Value,
        },
    ] {
        assert_eq!(
            decode_argument_control(&encode_argument_control(value)).unwrap(),
            value
        );
    }
    for value in [
        CallProofScope::Unconditional,
        CallProofScope::Relation(
            novarocks_type_contract::RelationEffectContext::call(
                novarocks_type_contract::RelationCallId::new(0),
            )
            .identity,
        ),
        CallProofScope::Relation(
            novarocks_type_contract::RelationEffectContext::materialized_input(
                novarocks_type_contract::RelationCallId::new(u32::MAX),
            )
            .identity,
        ),
        CallProofScope::Region(EvaluationRegionId::new(0)),
        CallProofScope::Region(EvaluationRegionId::new(u32::MAX)),
    ] {
        assert_eq!(decode_proof(&encode_proof(value)).unwrap(), value);
    }
    assert!(decode_argument_control(&wire::ArgumentControl { kind: None }).is_err());
    for value in [0, -1, i32::MAX] {
        assert!(
            decode_argument_control(&wire::ArgumentControl {
                kind: Some(wire::argument_control::Kind::Simple(value))
            })
            .is_err()
        );
    }
    assert!(
        decode_argument_control(&wire::ArgumentControl {
            kind: Some(wire::argument_control::Kind::HigherOrder(
                control_wire::HigherOrderControl {
                    body_ordinal: 0,
                    body_demand: 0
                }
            ))
        })
        .is_err()
    );
    assert!(decode_proof(&wire::ProofScope { kind: None }).is_err());
}

#[test]
fn every_closed_effect_enum_rejects_unspecified_and_unknown_values() {
    for bad in [0, -1, i32::MAX] {
        assert!(decode_stability(bad).is_err());
        assert!(decode_row_error(bad).is_err());
        assert!(decode_failure(bad).is_err());
        assert!(decode_null(bad).is_err());
        assert!(decode_state(bad).is_err());
    }
    for v in [
        FunctionVolatility::Immutable,
        FunctionVolatility::Stable,
        FunctionVolatility::Volatile,
    ] {
        assert_eq!(decode_stability(encode_stability(v)).unwrap(), v);
    }
    for v in [
        FunctionIntrinsicRowError::NoRowError,
        FunctionIntrinsicRowError::MayRaise,
        FunctionIntrinsicRowError::NotRowEvaluated,
    ] {
        assert_eq!(decode_row_error(encode_row_error(v)).unwrap(), v);
    }
    for v in [
        FunctionFailureBehavior::Propagate,
        FunctionFailureBehavior::ReturnsNull,
    ] {
        assert_eq!(decode_failure(encode_failure(v)).unwrap(), v);
    }
    for v in [
        FunctionNullBehavior::Strict,
        FunctionNullBehavior::CalledOnNull,
        FunctionNullBehavior::ControlDefined,
    ] {
        assert_eq!(decode_null(encode_null(&v)).unwrap(), v);
    }
    for v in [
        FunctionInstanceState::None,
        FunctionInstanceState::ScalarInstance,
        FunctionInstanceState::AggregateInstance,
        FunctionInstanceState::WindowPartition,
        FunctionInstanceState::TableInstance,
    ] {
        assert_eq!(decode_state(encode_state(v)).unwrap(), v);
    }
}

#[test]
fn mandatory_call_and_effect_messages_are_not_reconstructed_from_legacy_metadata() {
    let fixture = scalar_fixture(1, 0);
    let dto = encode_checked(&fixture.checked().unwrap(), &TestControl::default()).unwrap();
    for field in 0..7 {
        let mut bad = dto.clone();
        match field {
            0 => bad.entries[0].use_id = None,
            1 => bad.entries[0].context = None,
            2 => bad.entries[0].effects = None,
            3 => bad.entries[0].effects.as_mut().unwrap().argument_control = None,
            4 => bad.entries[0].effects.as_mut().unwrap().observable_effects = None,
            5 => bad.entries[0].effects.as_mut().unwrap().proof_scope = None,
            _ => {
                bad.entries[0].effects.as_mut().unwrap().proof_scope =
                    Some(wire::ProofScope { kind: None })
            }
        }
        assert!(matches!(
            decode_checked(&fixture, &bad, &TestControl::default()),
            Err(ClaimError::Codec(E::InvalidShape(_)))
        ));
    }
}

#[test]
fn actual_snapshot_constructor_checks_coverage_context_proof_and_shared_occurrence() {
    let fixture = scalar_fixture(2, 0);
    let dto = encode_checked(&fixture.checked().unwrap(), &TestControl::default()).unwrap();
    let mut bad = dto.clone();
    bad.entries.pop();
    assert_unit_invalid(
        decode_checked(&fixture, &bad, &TestControl::default()),
        "lacks a call claim",
    );
    let mut bad = dto.clone();
    bad.entries.push(bad.entries[0].clone());
    assert!(matches!(
        decode_checked(&fixture, &bad, &TestControl::default()),
        Err(ClaimError::Codec(E::InvalidShape("duplicate scalar claim")))
    ));
    let mut bad = dto.clone();
    bad.entries[0].context = bad.entries[1].context;
    assert!(matches!(
        decode_checked(&fixture, &bad, &TestControl::default()),
        Err(ClaimError::Codec(E::InvalidShape(
            "scalar claim key differs from its context"
        )))
    ));
    let mut bad = dto.clone();
    bad.entries[0].effects.as_mut().unwrap().proof_scope = Some(encode_proof(
        CallProofScope::Region(EvaluationRegionId::new(u32::MAX)),
    ));
    assert_unit_invalid(
        decode_checked(&fixture, &bad, &TestControl::default()),
        "foreign region",
    );
    let mut bad = dto.clone();
    bad.entries[0].use_id = Some(71);
    bad.entries[0].context.as_mut().unwrap().use_id = Some(71);
    assert_unit_invalid(
        decode_checked(&fixture, &bad, &TestControl::default()),
        "lacks a call claim",
    );
    let mut bad = dto;
    bad.entries[0].effects.as_mut().unwrap().argument_control =
        Some(encode_argument_control(ArgumentControl::TypeOnly));
    assert_unit_invalid(
        decode_checked(&fixture, &bad, &TestControl::default()),
        "context/control differs",
    );
}

#[test]
fn reference_presence_closed_key_uniqueness_and_same_snapshot_are_checked() {
    let mut fixture = scalar_fixture(1, 0);
    fixture.calls[0].effects = rich_effects(fixture.calls[0].context.region);
    let dto = encode_checked(&fixture.checked().unwrap(), &TestControl::default()).unwrap();
    for missing in [true, false] {
        let mut bad = dto.clone();
        let reference = &mut bad.entries[0].effects.as_mut().unwrap().environment[0];
        if missing {
            reference.id = None;
        } else {
            reference.expected_key = 0;
        }
        assert!(matches!(
            decode_checked(&fixture, &bad, &TestControl::default()),
            Err(ClaimError::Codec(E::InvalidShape(_)))
        ));
    }
    let mut bad = dto.clone();
    let environment = &mut bad.entries[0].effects.as_mut().unwrap().environment;
    environment[1] = environment[0];
    assert!(matches!(
        decode_checked(&fixture, &bad, &TestControl::default()),
        Err(ClaimError::Unit(
            novarocks_expression_contract::UnitError::Effects(
                novarocks_type_contract::EffectContractError::InvalidEnvironmentReference
            )
        ))
    ));
    // The foreign root-bank API retired. A real unit with two roots must not
    // accept the one-root claim set; the actual unit owner proves coverage.
    let foreign = scalar_fixture(2, u32::MAX);
    assert_unit_invalid(
        decode_checked(&foreign, &dto, &TestControl::default()),
        "lacks a call claim",
    );
}

#[test]
fn original_control_survives_projection_and_nested_constructor_entry_256_and_tails() {
    let fixture = scalar_fixture(320, u32::MAX);
    let checked = fixture.checked().unwrap();
    let good_encode = TestControl::default();
    let dto = encode_checked(&checked, &good_encode).unwrap();
    let encode_trace = good_encode.events.into_inner().unwrap();
    let good_decode = TestControl::default();
    decode_checked(&fixture, &dto, &good_decode).unwrap();
    let decode_trace = good_decode.events.into_inner().unwrap();
    for trace in [&encode_trace, &decode_trace] {
        assert_eq!(trace[0].1, 0);
        assert!(trace.iter().any(|(_, units)| *units == 256));
        assert!(trace.iter().any(|(_, units)| *units > 0 && *units < 256));
    }
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 1..=encode_trace.len() {
            let stop = TestControl {
                failure: Some((at, error.clone())),
                events: Mutex::default(),
            };
            assert_eq!(
                encode_checked(&checked, &stop),
                Err(E::Control(error.clone()))
            );
            assert_eq!(stop.events.lock().unwrap().len(), at);
        }
        for at in 1..=decode_trace.len() {
            let stop = TestControl {
                failure: Some((at, error.clone())),
                events: Mutex::default(),
            };
            assert!(
                matches!(decode_checked(&fixture,&dto,&stop),Err(ClaimError::Codec(E::Control(ref actual))) if *actual==error)
            );
            assert_eq!(stop.events.lock().unwrap().len(), at);
        }
    }
}

#[test]
fn real_relational_and_window_calls_preserve_lifecycle_context_and_require_exact_coverage() {
    let mut fixture = special_fixture();
    for call in &mut fixture.calls {
        call.decimal_overflow_policy = DecimalOverflowPolicy::ReportError;
    }
    let checked = fixture.checked().unwrap();
    let dto = encode_relation_checked(&checked, &TestControl::default()).unwrap();
    let result = decode_relation_checked(&fixture, &dto, &TestControl::default()).unwrap();
    assert_eq!(result, checked);
    assert_eq!(result.entries().len(), 4);
    assert_eq!(
        result
            .entries()
            .keys()
            .filter(|site| matches!(site, PhysicalCallSite::Aggregate { .. }))
            .count(),
        2
    );
    assert_eq!(
        result
            .entries()
            .keys()
            .filter(|site| matches!(site, PhysicalCallSite::Table { .. }))
            .count(),
        1
    );
    let mut missing = dto.clone();
    let index = missing
        .entries
        .iter()
        .position(|call| {
            matches!(
                call.site.as_ref().unwrap().kind,
                Some(wire::call_site::Kind::Aggregate(_))
            )
        })
        .unwrap();
    missing.entries.remove(index);
    assert!(matches!(
        decode_relation_checked(&fixture, &missing, &TestControl::default()),
        Err(E::Calls(FrozenCallError::MissingSite(
            PhysicalCallSite::Aggregate { .. }
        )))
    ));
    let mut bad = dto.clone();
    let table = bad
        .entries
        .iter_mut()
        .find(|call| {
            matches!(
                call.site.as_ref().unwrap().kind,
                Some(wire::call_site::Kind::TableNodeId(_))
            )
        })
        .unwrap();
    table.relation_context.as_mut().unwrap().demand = encode_demand(EvaluationDemand::TruthOnly);
    assert_eq!(
        decode_relation_checked(&fixture, &bad, &TestControl::default()),
        Err(E::Calls(FrozenCallError::WrongContext))
    );
    let mut bad = dto;
    let table = bad
        .entries
        .iter_mut()
        .find(|call| {
            matches!(
                call.site.as_ref().unwrap().kind,
                Some(wire::call_site::Kind::TableNodeId(_))
            )
        })
        .unwrap();
    table
        .relation_context
        .as_mut()
        .unwrap()
        .identity
        .as_mut()
        .unwrap()
        .call_id = Some(0);
    assert_eq!(
        decode_relation_checked(&fixture, &bad, &TestControl::default()),
        Err(E::Calls(FrozenCallError::DuplicateIdentity))
    );
}

#[test]
fn canonical_calls_with_environment_references_can_exceed_control_reference_count() {
    // 32769 actual zero-argument Window relations keep independent call ids.
    // Their original one-reference environments exceed 65536 dynamic items
    // without inventing scalar roots or resetting the fragment call bound.
    let mut fixture = relation_fixture(MAX_CONTROL_USE_REFERENCES / 2 + 1, u32::MAX);
    for call in &mut fixture.calls {
        call.effects.environment = Box::from([SemanticParameterRef {
            id: SemanticParameterId::new(0),
            expected_key: SemanticParameterKey::TimeZone,
        }]);
    }
    let checked = fixture.checked().unwrap();
    assert_eq!(checked.entries().len(), 32769);
    let dto = encode_relation_checked(&checked, &TestControl::default()).unwrap();
    assert_eq!(dto.entries.len(), 32769);
    let decoded = decode_relation_checked(&fixture, &dto, &TestControl::default()).unwrap();
    assert_eq!(decoded, checked);
}

#[test]
fn cumulative_calls_and_environment_bounds_precede_output_allocation() {
    assert_eq!(
        count_references(MAX_CONTROL_USE_REFERENCES, 1).unwrap(),
        MAX_CONTROL_USE_REFERENCES + 1
    );
    assert_eq!(
        count_references(MAX_PLAN_DERIVED_CUT_ITEMS - 1, 1).unwrap(),
        MAX_PLAN_DERIVED_CUT_ITEMS
    );
    assert!(count_references(MAX_PLAN_DERIVED_CUT_ITEMS, 1).is_err());
    assert!(count_references(usize::MAX, 1).is_err());
    assert!(count_references(0, MAX_SEMANTIC_PARAMETERS + 1).is_err());
    let fixture = relation_fixture(1, 0);
    let dto =
        encode_relation_checked(&fixture.checked().unwrap(), &TestControl::default()).unwrap();
    let too_many_calls = wire::FrozenCalls {
        entries: vec![dto.entries[0].clone(); MAX_CONTROL_USE_REFERENCES + 1],
    };
    assert!(matches!(
        decode_relation_checked(&fixture, &too_many_calls, &TestControl::default()),
        Err(E::InvalidShape(_))
    ));
    // A small table with duplicate occurrences reaches the canonical
    // structural rejection rather than a fabricated reference-budget error.
    let duplicate = wire::FrozenCalls {
        entries: vec![dto.entries[0].clone(); 2],
    };
    assert_eq!(
        decode_relation_checked(&fixture, &duplicate, &TestControl::default()),
        Err(E::Calls(FrozenCallError::DuplicateSite))
    );
    let mut many_refs = dto;
    many_refs.entries[0].effects.as_mut().unwrap().environment = vec![
        wire::SemanticParameterRef {
            id: Some(0),
            expected_key: wire::SemanticParameterKey::TimeZone as i32
        };
        MAX_SEMANTIC_PARAMETERS + 1
    ];
    assert!(matches!(
        decode_relation_checked(&fixture, &many_refs, &TestControl::default()),
        Err(E::InvalidShape(_))
    ));
}
mod observed_resources_tests {
    include!("../owned_resources/tests.rs");
}

#[test]
fn ordered_null_argument_effect_roundtrips_as_its_own_wire_variant() {
    let encoded = encode_argument_control(ArgumentControl::OrderedNull);
    assert_eq!(
        decode_argument_control(&encoded).unwrap(),
        ArgumentControl::OrderedNull
    );
    assert_ne!(
        encoded,
        encode_argument_control(ArgumentControl::OrderedRegexp)
    );
    assert_ne!(encoded, encode_argument_control(ArgumentControl::Eager));
}
