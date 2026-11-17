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
use novarocks_expression_contract::RootOrdinal;
use novarocks_type_contract::{
    AggregateStateFormatId, ArgumentControl, ControlShape, EvaluationRegionId,
    ExpressionControlFlow, ExpressionEvaluationRegion, ExpressionInvocation,
    FunctionArgumentEvaluation, FunctionFailureBehavior, FunctionId, FunctionInstanceState,
    FunctionIntrinsicRowError, FunctionNullBehavior, FunctionOverloadId, FunctionVolatility,
    GuardKind, ObservableEffects, RegionGuard, SemanticParameterId, SemanticParameterKey,
};
use novarocks_type_contract::{
    CompileCheckpoints, EvaluationDemand, ExpressionEffectContext, ExpressionUseId,
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
fn declared_effects(kind: FunctionKind, scope: CallProofScope) -> CallEffects {
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
        proof_scope: scope,
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
            expressions: None,
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

// Offline replacement foundation for frozen_calls/tests.rs. Not compiled.
// Keep only Fragment as the fact authority. NodeId below identifies the fixed
// fixture's scalar source, not another expression or invocation namespace.
struct Fixture {
    fragment: Fragment,
    primary: NodeId,
}
impl Fixture {
    fn unit(&self) -> &novarocks_expression_contract::BoundExprSet {
        self.fragment.unit(self.primary).unwrap()
    }
    fn claims(
        &self,
    ) -> &BTreeMap<ExpressionUseId, novarocks_expression_contract::ExpressionCallClaim> {
        self.unit().claims()
    }
    fn relations(&self) -> &BTreeMap<PhysicalCallSite, FrozenPhysicalCall> {
        self.fragment.calls().entries()
    }
    fn checked(&self) -> Result<&FrozenFragmentCalls, FrozenCallError> {
        self.fragment
            .calls()
            .validate_fragment(&self.fragment, &Control::default())?;
        Ok(self.fragment.calls())
    }
    // Only these private fixtures create their unit with a pure construction
    // lifetime. Recursive type/CV handles below retain their actual source.
    // This is not a clone API for host-created production units.
    fn candidate_unit(
        &self,
        edit: impl FnOnce(&mut novarocks_expression_contract::UnitParts),
    ) -> Result<novarocks_expression_contract::BoundExprSet, novarocks_expression_contract::UnitError>
    {
        let source = self.unit();
        let mut parts = novarocks_expression_contract::UnitParts::new(
            source.ports().into(),
            source.definitions().into(),
            source.roots().into(),
            source.root_use_ids().into(),
            source.control_flow().clone(),
            source.claims().clone(),
            source.requests().clone(),
        );
        edit(&mut parts);
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate)?;
        let result = novarocks_expression_contract::BoundExprSet::try_from_parts_in(
            parts,
            unit_limits(),
            &mut work,
        );
        if matches!(
            result,
            Err(novarocks_expression_contract::UnitError::Control(_))
        ) {
            return result;
        }
        work.finish()?;
        result
    }
    fn edit_unit(&mut self, edit: impl FnOnce(&mut novarocks_expression_contract::UnitParts)) {
        let unit = self
            .candidate_unit(edit)
            .expect("fixture's intended valid claim edit");
        let mut parts = self.fragment.clone().into_parts();
        parts.nodes.get_mut(&self.primary).unwrap().expressions = Some(std::sync::Arc::new(unit));
        self.fragment = parts.into();
    }
    fn candidate_relations(
        &self,
        edit: impl FnOnce(&mut Vec<FrozenPhysicalCall>),
    ) -> Result<Fragment, FrozenCallError> {
        let mut calls = self.relations().values().cloned().collect();
        edit(&mut calls);
        self.fragment
            .clone()
            .with_calls_observed(calls, &Control::default())
    }
}
fn unit_limits() -> novarocks_expression_contract::UnitLimits {
    novarocks_expression_contract::UnitLimits {
        expression_semantic_depth: PlanLimits::FROZEN.expression_semantic_depth,
    }
}
// A borrowed membership view for already-authored dense local definitions.
// It is not an arena, identity mint, source store or global adapter.
struct FixtureDefinitions<'a>(&'a [novarocks_expression_contract::ExprNode]);
impl novarocks_type_contract::ExpressionDefinitionMembership<ExprId> for FixtureDefinitions<'_> {
    fn definition_count(&self) -> usize {
        self.0.len()
    }
    fn contains_definition(&self, id: ExprId) -> bool {
        self.0
            .get(id.get() as usize)
            .is_some_and(|node| node.id == id)
    }
}
fn scalar_claim(
    current: ExpressionEffectContext,
) -> novarocks_expression_contract::ExpressionCallClaim {
    novarocks_expression_contract::ExpressionCallClaim {
        context: current,
        effects: declared_effects(FunctionKind::Scalar, CallProofScope::Region(current.region)),
        decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        temporal_source: None,
        regexp_count_pattern_source: None,
        to_base64_byte_source: None,
    }
}
fn make_unit(
    definitions: Vec<novarocks_expression_contract::ExprNode>,
    roots: Vec<novarocks_expression_contract::ExprRoot>,
    root_uses: Vec<ExpressionUseId>,
    regions: Vec<ExpressionEvaluationRegion>,
    invocations: Vec<ExpressionInvocation<ExprId>>,
    claims: BTreeMap<ExpressionUseId, novarocks_expression_contract::ExpressionCallClaim>,
    requests: BTreeMap<ExprId, novarocks_expression_contract::PhysicalCallRequest>,
) -> novarocks_expression_contract::BoundExprSet {
    let control = Control::default();
    let flow = ExpressionControlFlow::try_new(
        regions,
        invocations,
        &FixtureDefinitions(&definitions),
        CompilePhase::Validate,
        &control,
    )
    .unwrap();
    let parts = novarocks_expression_contract::UnitParts::new(
        Box::default(),
        definitions.into_boxed_slice(),
        roots.into_boxed_slice(),
        root_uses.into_boxed_slice(),
        flow,
        claims,
        requests,
    );
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let unit = novarocks_expression_contract::BoundExprSet::try_from_parts_in(
        parts,
        unit_limits(),
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    unit
}
fn leaf_unit(
    count: usize,
    scalar: bool,
    fixture_constants: &mut crate::tests::constant_fixture::ConstantFixture,
) -> novarocks_expression_contract::BoundExprSet {
    assert_ne!(count, 0, "zero rows use the genuinely empty source fixture");
    let ty = if scalar { boolean() } else { integer() };
    let definition = ExprId::new(0);
    let definitions = vec![novarocks_expression_contract::ExprNode {
        id: definition,
        lambda_scope: None,
        ty: ty.clone(),
        kind: if scalar {
            ExprKind::FunctionCall {
                function: function(FunctionKind::Scalar, ty),
                args: Box::default(),
            }
        } else {
            fixture_constants.i64(17, &ty)
        },
    }];
    let mut roots = Vec::new();
    let mut root_uses = Vec::new();
    let mut invocations = Vec::new();
    let mut claims = BTreeMap::new();
    for ordinal in 0..count {
        let id = match ordinal {
            0 => 0,
            1 => u32::MAX,
            other => u32::try_from(other - 1).unwrap(),
        };
        let current = context(
            id,
            if ordinal % 2 == 0 { 0 } else { u32::MAX },
            EvaluationDemand::Value,
        );
        roots.push(novarocks_expression_contract::ExprRoot {
            definition,
            demand: EvaluationDemand::Value,
            scope: novarocks_expression_contract::RootScope::AllPorts,
        });
        root_uses.push(current.use_id);
        invocations.push(ExpressionInvocation {
            context: current,
            definition,
            control: ControlShape::Eager,
            arguments: Box::default(),
        });
        if scalar {
            claims.insert(current.use_id, scalar_claim(current));
        }
    }
    let regions = if count == 1 {
        vec![domain(0)]
    } else {
        vec![domain(0), domain(u32::MAX)]
    };
    let requests = if scalar {
        BTreeMap::from([(definition, zero_argument_request())])
    } else {
        BTreeMap::new()
    };
    make_unit(
        definitions,
        roots,
        root_uses,
        regions,
        invocations,
        claims,
        requests,
    )
}
fn add_values(
    builder: &mut FragmentBuilder,
    count: usize,
    scalar: bool,
    fixture_constants: &mut crate::tests::constant_fixture::ConstantFixture,
) -> NodeId {
    let node = builder.reserve_node_id().unwrap();
    let ty = if scalar { boolean() } else { integer() };
    let value = builder
        .add_value(
            ty,
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let unit = leaf_unit(count, scalar, fixture_constants);
    install_node(
        builder,
        node,
        vec![],
        vec![value],
        NodeKind::Values {
            rows: (0..count)
                .map(|ordinal| Box::from([RootOrdinal::new(u32::try_from(ordinal).unwrap())]))
                .collect(),
        },
    );
    builder
        .install_unit(node, std::sync::Arc::new(unit))
        .unwrap();
    node
}
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
            StagePlacementPolicy::Singleton,
        )
        .unwrap()
        .with_call_requests_observed(vec![], &Control::default())
        .unwrap()
        .with_calls_observed(vec![], &Control::default())
        .unwrap();
    Fixture {
        fragment,
        primary: node,
    }
}
fn scalar_fixture(
    count: usize,
    fragment_id: u32,
    constants: &mut crate::tests::constant_fixture::ConstantFixture,
) -> Fixture {
    let mut builder = FragmentBuilder::new(FragmentId::new(fragment_id));
    let node = add_values(&mut builder, count, true, constants);
    let fragment = builder
        .finish_definition(
            node,
            FragmentSink::Noop,
            dop(),
            StagePlacementPolicy::Singleton,
        )
        .unwrap()
        .with_call_requests_observed(vec![], &Control::default())
        .unwrap()
        .with_calls_observed(vec![], &Control::default())
        .unwrap();
    Fixture {
        fragment,
        primary: node,
    }
}

fn case_fixture(constants: &mut crate::tests::constant_fixture::ConstantFixture) -> Fixture {
    let mut builder = FragmentBuilder::new(FragmentId::new(31));
    let node = builder.reserve_node_id().unwrap();
    let definition = ExprId::new(0);
    let otherwise = ExprId::new(1);
    let case = ExprId::new(2);
    let definitions = vec![
        novarocks_expression_contract::ExprNode {
            id: definition,
            lambda_scope: None,
            ty: boolean(),
            kind: ExprKind::FunctionCall {
                function: function(FunctionKind::Scalar, boolean()),
                args: Box::default(),
            },
        },
        novarocks_expression_contract::ExprNode {
            id: otherwise,
            lambda_scope: None,
            ty: boolean(),
            kind: constants.boolean(false, &boolean()),
        },
        novarocks_expression_contract::ExprNode {
            id: case,
            lambda_scope: None,
            ty: boolean(),
            kind: ExprKind::Case {
                operand: None,
                when_then: Box::from([(definition, definition)]),
                else_expr: Some(otherwise),
            },
        },
    ];
    let regions = vec![
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
    let first = context(0, 12, EvaluationDemand::TruthOnly);
    let then = context(u32::MAX, 10, EvaluationDemand::Value);
    let unit = make_unit(
        definitions,
        vec![novarocks_expression_contract::ExprRoot {
            definition: case,
            demand: EvaluationDemand::Value,
            scope: novarocks_expression_contract::RootScope::AllPorts,
        }],
        vec![ExpressionUseId::new(9)],
        regions,
        vec![
            ExpressionInvocation {
                context: context(9, 0, EvaluationDemand::Value),
                definition: case,
                control: ControlShape::Case {
                    simple: false,
                    arms: 1,
                    has_else: true,
                },
                arguments: Box::from([first.use_id, then.use_id, ExpressionUseId::new(2)]),
            },
            ExpressionInvocation {
                context: first,
                definition,
                control: ControlShape::Eager,
                arguments: Box::default(),
            },
            ExpressionInvocation {
                context: then,
                definition,
                control: ControlShape::Eager,
                arguments: Box::default(),
            },
            ExpressionInvocation {
                context: context(2, 11, EvaluationDemand::Value),
                definition: otherwise,
                control: ControlShape::Eager,
                arguments: Box::default(),
            },
        ],
        BTreeMap::from([
            (first.use_id, scalar_claim(first)),
            (then.use_id, scalar_claim(then)),
        ]),
        BTreeMap::from([(definition, zero_argument_request())]),
    );
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
            rows: Box::from([Box::from([RootOrdinal::new(0)])]),
        },
    );
    builder
        .install_unit(node, std::sync::Arc::new(unit))
        .unwrap();
    let fragment = builder
        .finish_definition(
            node,
            FragmentSink::Noop,
            dop(),
            StagePlacementPolicy::Singleton,
        )
        .unwrap()
        .with_call_requests_observed(vec![], &Control::default())
        .unwrap()
        .with_calls_observed(vec![], &Control::default())
        .unwrap();
    Fixture {
        fragment,
        primary: node,
    }
}

fn special_fixture(constants: &mut crate::tests::constant_fixture::ConstantFixture) -> Fixture {
    special_fixture_with_rows(1, constants).unwrap()
}
// The rejection belongs at real relation installation. Return it instead of
// storing a parallel invalid table in an otherwise published Fixture.
fn special_fixture_with_rows(
    input_rows: usize,
    constants: &mut crate::tests::constant_fixture::ConstantFixture,
) -> Result<Fixture, FrozenCallError> {
    let mut builder = FragmentBuilder::new(FragmentId::new(41));
    let input = add_values(&mut builder, input_rows, false, constants);
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
    let output = builder
        .add_value(
            integer(),
            ValueOrigin::NodeOutput {
                node: window,
                output_ordinal: u32::try_from(outputs.len()).unwrap(),
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
            calls: Box::from([WindowCallSpec {
                binding: function(FunctionKind::Window, integer()),
                distinct: false,
                arguments: Box::default(),
                order_by: Box::default(),
                frame: None,
                ignore_nulls: false,
                aggregate_binding: None,
                output,
            }]),
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
    let fields = function(FunctionKind::Table, integer());
    install_node(
        &mut builder,
        table,
        vec![window],
        vec![output],
        NodeKind::TableFunction {
            function: BoundTableFunction {
                function_id: fields.function_id,
                overload: fields.overload,
                argument_types: fields.argument_types,
                result_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from(
                    [integer()],
                )),
                legacy_metadata: fields.legacy_metadata,
            },
            arguments: Box::default(),
            outputs: Box::from([TableFunctionOutput::FunctionResult {
                result_ordinal: 0,
                value: output,
            }]),
            left_outer: false,
        },
    );
    // This is the original public construction layer for pending relational
    // declarations; finish_definition retains its full local property gate.
    let fragment = builder
        .finish_structure(
            table,
            FragmentSink::Noop,
            dop(),
            PlanLimits::FROZEN,
            &Control::default(),
            StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let sites = [
        (
            PhysicalCallSite::Aggregate {
                node: aggregate,
                call: 0,
            },
            FunctionKind::Aggregate,
            0,
        ),
        (
            PhysicalCallSite::Aggregate {
                node: aggregate,
                call: 1,
            },
            FunctionKind::Aggregate,
            u32::MAX,
        ),
        (
            PhysicalCallSite::Window {
                node: window,
                call: 0,
            },
            FunctionKind::Window,
            71,
        ),
        (
            PhysicalCallSite::Table { node: table },
            FunctionKind::Table,
            89,
        ),
    ];
    let requests = sites
        .iter()
        .map(|(site, _, _)| (*site, zero_argument_request()))
        .collect();
    let fragment = fragment
        .with_call_requests_observed(requests, &Control::default())
        .unwrap();
    let calls = sites
        .into_iter()
        .map(|(site, kind, id)| {
            let current = novarocks_type_contract::RelationEffectContext::call(
                novarocks_type_contract::RelationCallId::new(id),
            );
            FrozenPhysicalCall {
                site,
                context: current,
                materialized_writer_input: None,
                effects: declared_effects(kind, CallProofScope::Relation(current.identity)),
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            }
        })
        .collect();
    let fragment = fragment.with_calls_observed(calls, &Control::default())?;
    Ok(Fixture {
        fragment,
        primary: input,
    })
}

fn wide_relation_fixture(count: usize) -> Fixture {
    let mut builder = FragmentBuilder::new(FragmentId::new(71));
    let input = builder.reserve_node_id().unwrap();
    install_node(
        &mut builder,
        input,
        vec![],
        vec![],
        NodeKind::Values {
            rows: Box::from([Box::default()]),
        },
    );
    let aggregate = builder.reserve_node_id().unwrap();
    let mut outputs = Vec::new();
    let mut specifications = Vec::new();
    let mut requests = Vec::new();
    let mut calls = Vec::new();
    for ordinal in 0..count {
        let id = u32::try_from(ordinal).unwrap();
        let call = AggregateCallId::new(id);
        let output = builder
            .add_value(integer(), ValueOrigin::AggregateResult { call })
            .unwrap();
        outputs.push(output);
        specifications.push(AggregateCall {
            id: call,
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
        let site = PhysicalCallSite::Aggregate {
            node: aggregate,
            call: id,
        };
        requests.push((site, zero_argument_request()));
        let context = novarocks_type_contract::RelationEffectContext::call(
            novarocks_type_contract::RelationCallId::new(id),
        );
        calls.push(FrozenPhysicalCall {
            site,
            context,
            materialized_writer_input: None,
            effects: declared_effects(
                FunctionKind::Aggregate,
                CallProofScope::Relation(context.identity),
            ),
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        });
    }
    install_node(
        &mut builder,
        aggregate,
        vec![input],
        outputs,
        NodeKind::Aggregate {
            group_by: Box::default(),
            calls: specifications.into_boxed_slice(),
            grouping: AggregateGrouping::Complete,
        },
    );
    let fragment = builder
        .finish_structure(
            aggregate,
            FragmentSink::Noop,
            dop(),
            PlanLimits::FROZEN,
            &Control::default(),
            StagePlacementPolicy::Singleton,
        )
        .unwrap()
        .with_call_requests_observed(requests, &Control::default())
        .unwrap()
        .with_calls_observed(calls, &Control::default())
        .unwrap();
    Fixture {
        fragment,
        primary: input,
    }
}

#[test]
fn repeated_definition_preserves_complete_per_use_claims_and_exact_binding_borrow() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut fixture = scalar_fixture(2, 0, &mut constants);
    let reference = SemanticParameterRef {
        id: SemanticParameterId::new(u32::MAX),
        expected_key: SemanticParameterKey::TimeZone,
    };
    fixture.edit_unit(|parts| {
        let rich = &mut parts
            .claims
            .get_mut(&ExpressionUseId::new(0))
            .unwrap()
            .effects;
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
        parts
            .claims
            .get_mut(&ExpressionUseId::new(u32::MAX))
            .unwrap()
            .effects
            .proof_scope = CallProofScope::Unconditional;
    });
    let checked = fixture.checked().unwrap();
    assert_eq!(checked.fragment(), FragmentId::new(0));
    assert!(
        checked.entries().is_empty(),
        "scalar claims belong only to the node unit"
    );
    assert_eq!(fixture.claims().len(), 2);
    let actual_refs = fixture
        .claims()
        .values()
        .flat_map(|call| call.effects.environment.iter().copied())
        .collect::<Vec<_>>();
    assert_eq!(actual_refs, vec![reference]);
    let unit = fixture.unit();
    let first = &unit.control_flow().uses()[&ExpressionUseId::new(0)];
    let second = &unit.control_flow().uses()[&ExpressionUseId::new(u32::MAX)];
    assert_eq!(first.definition, second.definition);
    assert_ne!(first.context.region, second.context.region);
    let ExprKind::FunctionCall { function, .. } = &unit.definition(first.definition).unwrap().kind
    else {
        panic!("actual scalar binding")
    };
    for call in unit.claims().values() {
        let actual = &unit
            .definition(unit.control_flow().uses()[&call.context.use_id].definition)
            .unwrap()
            .kind;
        let ExprKind::FunctionCall {
            function: actual, ..
        } = actual
        else {
            panic!("exact definition")
        };
        assert!(std::ptr::eq(actual, function));
        assert_eq!(
            call.context,
            unit.control_flow().uses()[&call.context.use_id].context
        );
    }
    assert_ne!(
        unit.claims()[&first.context.use_id].effects.value_stability,
        function.legacy_metadata.as_ref().unwrap().volatility
    );
}

// Pure unit rejection scenarios moved, without dropping their assertions, to
// expression-contract-claims.candidate.rs in this offline proposal:
// missing_extra_duplicate_foreign_fragment_context_scope_and_control_fail_closed
// malformed_full_effect_shape_and_duplicate_environment_are_rejected
// type_only_parent_retains_static_child_binding_without_runtime_child_claim
// The fragment/snapshot part is also retained by property_proof_tests.

#[test]
fn one_scalar_definition_has_independent_truth_and_guarded_value_occurrences() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut fixture = case_fixture(&mut constants);
    fixture.edit_unit(|parts| {
        parts
            .claims
            .get_mut(&ExpressionUseId::new(u32::MAX))
            .unwrap()
            .effects
            .own_row_error = FunctionIntrinsicRowError::MayRaise;
    });
    fixture.checked().unwrap();
    let first = &fixture.claims()[&ExpressionUseId::new(0)];
    let then = &fixture.claims()[&ExpressionUseId::new(u32::MAX)];
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
        fixture.unit().control_flow().uses()[&first.context.use_id].definition,
        fixture.unit().control_flow().uses()[&then.context.use_id].definition
    );
    // Cross-region scalar proof refusal is tested at its real unit constructor
    // in scalar_claim_cannot_borrow_another_guarded_occurrences_proof.
}

#[test]
fn real_aggregate_window_table_positions_borrow_selected_bindings() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fixture = special_fixture(&mut constants);
    let checked = fixture.checked().unwrap();
    assert_eq!(checked.entries().len(), 4);
    for call in fixture.relations().values() {
        match (call.site, checked.binding(&fixture.fragment, call.site)) {
            (
                PhysicalCallSite::Aggregate {
                    node,
                    call: ordinal,
                },
                Some(PhysicalCallBinding::Aggregate(actual)),
            ) => {
                let NodeKind::Aggregate { calls, .. } = &fixture.fragment.nodes()[&node].kind
                else {
                    unreachable!()
                };
                let selected = &calls[ordinal as usize];
                assert!(std::ptr::eq(actual, &selected.binding));
                assert_eq!(
                    selected.id,
                    AggregateCallId::new(if ordinal == 0 { 0 } else { u32::MAX })
                );
                if ordinal != 0 {
                    assert_ne!(selected.id.get(), ordinal);
                }
            }
            (
                PhysicalCallSite::Window {
                    node,
                    call: ordinal,
                },
                Some(PhysicalCallBinding::Window {
                    function: actual,
                    aggregate,
                }),
            ) => {
                let NodeKind::Window(spec) = &fixture.fragment.nodes()[&node].kind else {
                    unreachable!()
                };
                assert!(std::ptr::eq(actual, &spec.calls[ordinal as usize].binding));
                assert!(aggregate.is_none());
            }
            (PhysicalCallSite::Table { node }, Some(PhysicalCallBinding::Table(actual))) => {
                let NodeKind::TableFunction { function, .. } =
                    &fixture.fragment.nodes()[&node].kind
                else {
                    unreachable!()
                };
                assert!(std::ptr::eq(actual, function));
                assert_eq!(actual.result_types.as_slice(), &[integer()]);
            }
            _ => panic!("wrong actual binding family"),
        }
    }
}

#[test]
fn relational_occurrences_require_fresh_ids_value_demand_actual_domain_and_kind() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fixture = special_fixture(&mut constants);
    let original = fixture.relations().values().cloned().collect::<Vec<_>>();
    // Same integer in an unrelated expression namespace is valid. Real
    // relation identities must instead be unique among actual relation sites.
    assert_eq!(original[0].context.identity.call.get(), 0);
    assert!(
        fixture
            .unit()
            .control_flow()
            .uses()
            .contains_key(&ExpressionUseId::new(0))
    );
    assert!(matches!(
        fixture.candidate_relations(
            |calls| calls[1].context.identity.call = calls[0].context.identity.call
        ),
        Err(FrozenCallError::DuplicateIdentity)
    ));
    assert!(matches!(
        fixture.candidate_relations(|calls| calls[0].context.demand = EvaluationDemand::TruthOnly),
        Err(FrozenCallError::WrongContext)
    ));
    assert!(matches!(
        fixture.candidate_relations(|calls| calls[0].context.identity.role =
            novarocks_type_contract::RelationEffectRole::MaterializedInput),
        Err(FrozenCallError::WrongContext)
    ));
    assert!(matches!(
        fixture.candidate_relations(|calls| {
            calls[0].effects.proof_scope = CallProofScope::Relation(
                novarocks_type_contract::RelationEffectContext::call(
                    novarocks_type_contract::RelationCallId::new(123),
                )
                .identity,
            );
        }),
        Err(FrozenCallError::WrongProofScope)
    ));
    assert!(matches!(
            fixture.candidate_relations(
                |calls| calls[0].effects.argument_control = ArgumentControl::Eager
            ),
            Err(FrozenCallError::InvalidEffects(
                EffectContractError::KindMismatch
            ))
        ));
    assert!(
        matches!(fixture.candidate_relations(|calls| { calls.pop(); }), Err(FrozenCallError::MissingSite(site)) if site == original.last().unwrap().site)
    );
    assert!(
        matches!(fixture.candidate_relations(|calls| calls[0].site = PhysicalCallSite::WriterPartial { node: fixture.fragment.root(), call: 0 }), Err(FrozenCallError::MissingSite(site)) if site == original[0].site)
    );
    let foreign = wide_relation_fixture(2);
    let checked = fixture.checked().unwrap();
    assert_eq!(
        checked.validate_fragment(&foreign.fragment, &Control::default()),
        Err(FrozenCallError::WrongFragment)
    );
    assert!(
        checked
            .binding(&foreign.fragment, original[0].site)
            .is_none()
    );
}

#[test]
fn combined_expression_and_relational_occurrences_share_one_near_over_budget() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    for (rows, accepted) in [
        (MAX_CONTROL_USE_REFERENCES - 4, true),
        (MAX_CONTROL_USE_REFERENCES - 3, false),
    ] {
        let result = special_fixture_with_rows(rows, &mut constants);
        assert_eq!(
            rows + 4,
            MAX_CONTROL_USE_REFERENCES + usize::from(!accepted)
        );
        if accepted {
            let fixture = result.expect("exact original mixed boundary");
            assert_eq!(fixture.unit().control_flow().use_reference_count(), rows);
            assert_eq!(fixture.checked().unwrap().entries().len(), 4);
        } else {
            assert!(matches!(result, Err(FrozenCallError::TooManyItems)));
        }
    }
}

#[test]
fn entry_and_mid_quantum_compile_failures_keep_exact_outer_categories() {
    let fixture = wide_relation_fixture(300);
    let calls = fixture.relations().values().cloned().collect::<Vec<_>>();
    let checked = fixture.checked().unwrap();
    let baseline = Control::default();
    checked
        .validate_fragment(&fixture.fragment, &baseline)
        .unwrap();
    let baseline = baseline.observations.lock().unwrap().clone();
    assert!(baseline.iter().all(|(_, units)| *units <= 256));
    let first_positive = baseline.iter().find(|(_, units)| *units > 0).unwrap().1;
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for positive_only in [false, true] {
            let control = Control {
                stop: Some(cause.clone()),
                positive_only,
                observations: Mutex::default(),
            };
            assert!(
                matches!(FrozenFragmentCalls::try_new(&fixture.fragment, calls.clone(), &control), Err(FrozenCallError::Control(actual)) if actual == cause)
            );
            let observations = control.observations.lock().unwrap();
            assert_eq!(observations[0], (CompilePhase::Validate, 0));
            if positive_only {
                assert_eq!(observations.last(), Some(&(CompilePhase::Validate, 256)));
            }
            drop(observations);
            let control = Control {
                stop: Some(cause.clone()),
                positive_only,
                observations: Mutex::default(),
            };
            assert_eq!(
                checked.validate_fragment(&fixture.fragment, &control),
                Err(FrozenCallError::Control(cause.clone()))
            );
            if positive_only {
                assert_eq!(
                    control.observations.lock().unwrap().last(),
                    Some(&(CompilePhase::Validate, first_positive))
                );
            }
        }
    }
    // Actual scalar source construction retains its own 300-use primary-cause
    // test at the unit owner; an empty relation table cannot prove that work.
}

#[test]
fn call_count_preflight_rejects_over_budget_without_visiting_entries() {
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let fixture = special_fixture(&mut constants);
    let calls =
        vec![fixture.relations().values().next().unwrap().clone(); MAX_CONTROL_USE_REFERENCES + 1];
    let control = Control::default();
    assert!(matches!(
        FrozenFragmentCalls::try_new(&fixture.fragment, calls, &control),
        Err(FrozenCallError::TooManyItems)
    ));
    assert_eq!(
        *control.observations.lock().unwrap(),
        vec![(CompilePhase::Validate, 0), (CompilePhase::Validate, 0)]
    );
}

#[test]
fn decimal_policy_is_independent_of_equal_effects_and_call_context() {
    use novarocks_type_contract::DecimalOverflowPolicy::{OutputNull, ReportError};
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut fixture = scalar_fixture(1, 83, &mut constants);
    let before = fixture.claims()[&ExpressionUseId::new(0)].clone();
    fixture.edit_unit(|parts| {
        parts
            .claims
            .get_mut(&ExpressionUseId::new(0))
            .unwrap()
            .decimal_overflow_policy = ReportError
    });
    fixture.checked().unwrap();
    let after = &fixture.claims()[&ExpressionUseId::new(0)];
    assert_eq!(before.context, after.context);
    assert_eq!(before.effects, after.effects);
    assert_eq!(before.decimal_overflow_policy, OutputNull);
    assert_eq!(after.decimal_overflow_policy, ReportError);
    assert_ne!(&before, after);
    let repeated = fixture.candidate_unit(|_| {}).unwrap();
    assert_eq!(&repeated, fixture.unit());
}

#[test]
fn shared_definition_occurrences_keep_distinct_decimal_policies_in_one_table() {
    use novarocks_type_contract::DecimalOverflowPolicy::{OutputNull, ReportError};
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut fixture = scalar_fixture(2, 84, &mut constants);
    fixture.edit_unit(|parts| {
        for call in parts.claims.values_mut() {
            call.effects.proof_scope = CallProofScope::Unconditional;
        }
        parts
            .claims
            .get_mut(&ExpressionUseId::new(0))
            .unwrap()
            .decimal_overflow_policy = ReportError;
        parts
            .claims
            .get_mut(&ExpressionUseId::new(u32::MAX))
            .unwrap()
            .decimal_overflow_policy = OutputNull;
    });
    fixture.checked().unwrap();
    let first = &fixture.claims()[&ExpressionUseId::new(0)];
    let second = &fixture.claims()[&ExpressionUseId::new(u32::MAX)];
    assert_eq!(first.effects, second.effects);
    let first_id = fixture.unit().control_flow().uses()[&first.context.use_id].definition;
    let second_id = fixture.unit().control_flow().uses()[&second.context.use_id].definition;
    assert_eq!(first_id, second_id);
    let ExprKind::FunctionCall {
        function: first_binding,
        ..
    } = &fixture.unit().definition(first_id).unwrap().kind
    else {
        panic!("actual binding")
    };
    let ExprKind::FunctionCall {
        function: second_binding,
        ..
    } = &fixture.unit().definition(second_id).unwrap().kind
    else {
        panic!("actual binding")
    };
    assert!(std::ptr::eq(first_binding, second_binding));
    assert_eq!(first.decimal_overflow_policy, ReportError);
    assert_eq!(second.decimal_overflow_policy, OutputNull);
}

#[test]
fn relational_and_window_sites_preserve_their_own_explicit_decimal_policy() {
    use novarocks_type_contract::DecimalOverflowPolicy::{OutputNull, ReportError};
    let mut constants = crate::tests::constant_fixture::ConstantFixture::new();
    let mut fixture = special_fixture(&mut constants);
    let policies = [ReportError, OutputNull, ReportError, OutputNull];
    fixture.fragment = fixture
        .candidate_relations(|calls| {
            for (call, policy) in calls.iter_mut().zip(policies) {
                call.decimal_overflow_policy = policy;
            }
        })
        .unwrap();
    let checked = fixture.checked().unwrap();
    let entries = checked.entries().values().collect::<Vec<_>>();
    assert!(matches!(
        entries[0].site,
        PhysicalCallSite::Aggregate { call: 0, .. }
    ));
    assert!(matches!(
        entries[1].site,
        PhysicalCallSite::Aggregate { call: 1, .. }
    ));
    assert!(matches!(
        entries[2].site,
        PhysicalCallSite::Window { call: 0, .. }
    ));
    assert!(matches!(entries[3].site, PhysicalCallSite::Table { .. }));
    for (call, policy) in entries.into_iter().zip(policies) {
        assert_eq!(call.decimal_overflow_policy, policy);
        assert!(checked.binding(&fixture.fragment, call.site).is_some());
    }
    checked
        .validate_fragment(&fixture.fragment, &Control::default())
        .unwrap();
    assert!(std::ptr::eq(checked, fixture.checked().unwrap()));
}

#[path = "visit_tests.rs"]
mod visit_tests;

#[path = "property_derivation_tests.rs"]
mod property_derivation_tests;

#[path = "all_property_derivation_tests.rs"]
mod all_property_derivation_tests;

#[path = "owned_property_finalization_tests.rs"]
mod owned_property_finalization_tests;
