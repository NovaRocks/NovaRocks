// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::*;
use novarocks_type_contract::{
    CompilePhase, PureCompileControl,
    owned_resources::{btree, copy::copy_string},
};
use std::{alloc::Layout, collections::BTreeMap, mem::size_of, sync::Mutex};

const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl Control {
    fn trace(&self) -> Vec<u32> {
        self.trace.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Decode, "nested type scope");
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "callback after refusal");
        }
        trace.push(units);
        match self.refusal.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn ty() -> ValueType {
    ValueType::new(DataType::Int64, false)
}
fn function(kind: FunctionKind, lambda: bool) -> BoundFunction {
    let arguments = if lambda {
        Box::from([
            FunctionArgumentType::Value(ty()),
            FunctionArgumentType::Lambda {
                parameter_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                    Box::from([ty(), ty()]),
                ),
                result_type: ty(),
            },
        ])
    } else {
        Box::from([FunctionArgumentType::Value(ty())])
    };
    BoundFunction::from_exact_signature(
        novarocks_type_contract::FunctionId::try_new("test.resource.signature").unwrap(),
        novarocks_type_contract::FunctionOverloadId::try_new("test.resource.overload").unwrap(),
        kind,
        novarocks_type_contract::TypedFactSlice::from_pure_boxed(arguments),
        ty(),
    )
}
fn binding() -> AggregateBinding {
    AggregateBinding {
        state_interpretation: None,
        state_argument_contract:
            novarocks_type_contract::AggregateStateArgumentContract::ExactSignature,
        function: function(FunctionKind::Aggregate, false),
        phase: AggregatePhase::Single,
        logical_argument_count: 1,
        intermediate_type: ty(),
        state_format: novarocks_type_contract::AggregateStateFormatId::try_new(
            "test.resource.state",
        )
        .unwrap(),
    }
}
fn properties() -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Singleton,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}
fn node(id: u32, kind: NodeKind) -> PhysicalNode {
    let id = NodeId::new(id);
    PhysicalNode {
        expressions: None,
        id,
        inputs: Box::default(),
        required_inputs: Box::default(),
        output: OutputPort {
            node: id,
            columns: Box::default(),
        },
        output_properties: properties(),
        kind,
    }
}
fn schema() -> WriterRelationSchema {
    WriterRelationSchema {
        revision: 1,
        fields: Box::from([WriterRelationField {
            value: ValueId::new(0),
            name: "rows".into(),
            ty: ty(),
            role: WriterRelationFieldRole::RowCount,
        }]),
    }
}
// This is an uninstalled resource fixture, never an implementation grant.
struct UnitControl;
impl PureCompileControl for UnitControl {
    fn checkpoint(&self, _phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        Ok(())
    }
}
fn fixture_policy() -> novarocks_constant_contract::ConstantPolicy {
    novarocks_constant_contract::ConstantPolicy {
        max_rows: 1024,
        max_array_nodes: 4096,
        max_logical_elements: 1_000_000,
        max_retained_buffer_bytes: 16_777_216,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 1_048_576,
        max_library_validation_work: 67_108_864,
        max_library_validation_bytes: 67_108_864,
    }
}
// The original fixture producer records the retained control constructor's
// actual public requests. This is a quote of source geometry, not an SDK
// capacity inference or a runtime grant. Temporary requests are kept separate.
#[derive(Default)]
struct SourceQuote {
    bytes: usize,
}
impl novarocks_type_contract::ValueSchemaCreationScope<std::convert::Infallible> for SourceQuote {
    fn storage_lifetime(&self) -> novarocks_type_contract::ValueTypeStorageLifetime {
        novarocks_type_contract::ValueTypeStorageLifetime::pure()
    }
    fn request_layout(&mut self, layout: Layout) -> Result<(), std::convert::Infallible> {
        self.bytes = self.bytes.checked_add(layout.size()).unwrap();
        Ok(())
    }
    fn observe_opaque_upper_bound(&mut self, bytes: usize) -> Result<(), std::convert::Infallible> {
        self.bytes = self.bytes.checked_add(bytes).unwrap();
        Ok(())
    }
}
struct Definitions(usize);
impl novarocks_type_contract::ExpressionDefinitionMembership<ExprId> for Definitions {
    fn definition_count(&self) -> usize {
        self.0
    }
    fn contains_definition(&self, id: ExprId) -> bool {
        (id.get() as usize) < self.0
    }
}
fn checked_scalar_unit() -> (
    std::sync::Arc<novarocks_expression_contract::BoundExprSet>,
    ConstantPools,
    usize,
) {
    use novarocks_expression_contract::{
        BoundExprSet, BoundExprSetBuilder, DefinitionControl, ExpressionCallClaim,
        PhysicalCallRequest, RootScope, UnitLimits,
    };
    use novarocks_type_contract::{
        ArgumentControl, CallEffects, CallProofScope, ControlShape, EvaluationDemand,
        FunctionFailureBehavior, FunctionInstanceState, FunctionIntrinsicRowError,
        FunctionNullBehavior, FunctionVolatility, ObservableEffects,
    };
    let policy = fixture_policy();
    let value = novarocks_constant_contract::ConstantValue::from_i64(
        std::sync::Arc::new(arrow_schema::Field::new("source", DataType::Int64, false)),
        ty(),
        11,
        policy,
        CompilePhase::Validate,
        &UnitControl,
    )
    .unwrap();
    let reference = ConstantReference {
        pool: ConstantPoolId::new(77),
        ordinal: value.ordinal(),
    };
    let mut pools = ConstantPools::empty();
    pools.insert(reference.pool, value.pool().clone()).unwrap();
    let mut builder = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: novarocks_type_contract::MAX_CONTROL_DEPTH,
    });
    let lambda = builder.reserve().unwrap();
    let body = builder
        .add(
            Some(lambda),
            ty(),
            ExprKind::LambdaParameter { lambda, ordinal: 0 },
        )
        .unwrap();
    builder
        .insert(
            lambda,
            None,
            ty(),
            ExprKind::Lambda {
                parameter_types: Box::new([ty(), ty()]),
                body,
            },
        )
        .unwrap();
    let argument = builder
        .add(None, ty(), ExprKind::Constant(reference))
        .unwrap();
    let call_function = function(FunctionKind::Scalar, true);
    let mut source = function_heap(&call_function) + 2 * size_of::<ValueType>();
    let original = PhysicalCallRequest {
        arguments: Box::new([
            novarocks_function_contract::FunctionArgument::Value {
                value_type: ty(),
                constant: Some(reference),
            },
            novarocks_function_contract::FunctionArgument::Lambda {
                parameter_types: match &call_function.argument_types[1] {
                    FunctionArgumentType::Lambda {
                        parameter_types, ..
                    } => parameter_types.clone(),
                    _ => unreachable!(),
                },
                result_type: ty(),
            },
        ]),
        logical_argument_count: 2,
        expected_result_type: None,
    };
    let call = builder
        .add(
            None,
            ty(),
            ExprKind::FunctionCall {
                function: call_function,
                args: Box::new([argument, lambda]),
            },
        )
        .unwrap();
    builder.request(call, original).unwrap();
    builder
        .definition_control(
            call,
            DefinitionControl {
                shape: ControlShape::HigherOrder {
                    body_ordinal: 1,
                    body_demand: EvaluationDemand::Value,
                },
                temporal_sources: None,
            },
        )
        .unwrap();
    let cast = builder
        .add(
            None,
            ValueType::new(DataType::Int32, false),
            ExprKind::Cast {
                cast_mode: novarocks_type_contract::TrinoCastMode::Implicit,
                expr: call,
                target: DataType::Int32,
            },
        )
        .unwrap();
    builder
        .root(call, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    builder
        .root(cast, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    let mut work = CompileCheckpoints::try_new(&UnitControl, CompilePhase::Validate).unwrap();
    let sealed = builder.seal(vec![], &mut work).unwrap();
    let claims = sealed
        .uses()
        .iter()
        .filter(|(_, invocation)| invocation.definition == call)
        .map(|(&use_id, invocation)| {
            (
                use_id,
                ExpressionCallClaim {
                    context: invocation.context,
                    effects: CallEffects {
                        value_stability: FunctionVolatility::Immutable,
                        own_row_error: FunctionIntrinsicRowError::NoRowError,
                        failure_behavior: FunctionFailureBehavior::Propagate,
                        null_behavior: FunctionNullBehavior::ControlDefined,
                        argument_control: ArgumentControl::HigherOrder {
                            body_ordinal: 1,
                            body_demand: EvaluationDemand::Value,
                        },
                        instance_state: FunctionInstanceState::None,
                        observable_effects: ObservableEffects::NONE,
                        environment: Box::default(),
                        proof_scope: CallProofScope::Region(invocation.context.region),
                    },
                    decimal_overflow_policy:
                        novarocks_type_contract::DecimalOverflowPolicy::ReportError,
                    temporal_source: None,
                },
            )
        })
        .collect();
    let unit = sealed.finish(claims, &mut work).unwrap();
    work.finish().unwrap();
    assert_eq!(unit.definitions().len(), 5);
    assert_eq!(unit.roots().len(), 2);
    assert_eq!(unit.requests().len(), 1);
    assert_eq!(unit.claims().len(), 2);
    // Replace only the pure producer's control container, through that same
    // real control constructor. Its retained requests quote private headers;
    // no private layout is reconstructed in this consumer fixture.
    let mut parts = unit.into_parts();
    let mut temporary = SourceQuote::default();
    let mut retained = SourceQuote::default();
    let mut work = CompileCheckpoints::try_new(&UnitControl, CompilePhase::Validate).unwrap();
    parts.control = novarocks_type_contract::ExpressionControlFlow::try_new_with_creator_in(
        parts.control.regions().values().copied().collect(),
        parts.control.uses().values().cloned().collect(),
        &Definitions(parts.definitions.len()),
        &mut temporary,
        &mut retained,
        &mut work,
    )
    .unwrap();
    source += retained.bytes;
    // Actual owned argument boxes were supplied to the control constructor.
    source += parts
        .control
        .uses()
        .values()
        .map(|u| u.arguments.len() * size_of::<novarocks_type_contract::ExpressionUseId>())
        .sum::<usize>();
    source += parts.definitions.len() * size_of::<ExprNode>()
        + parts.roots.len() * size_of::<novarocks_expression_contract::ExprRoot>()
        + parts.root_uses.len() * size_of::<novarocks_type_contract::ExpressionUseId>();
    source +=
        btree::insertion_only::<novarocks_type_contract::ExpressionUseId, ExpressionCallClaim>(
            parts.claims.len(),
        )
        .unwrap()
        .request_bytes_upper_bound;
    source += btree::insertion_only::<ExprId, PhysicalCallRequest>(parts.requests.len())
        .unwrap()
        .request_bytes_upper_bound;
    source += 2 * size_of::<novarocks_function_contract::FunctionArgument<ConstantReference>>();
    source +=
        novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<BoundExprSet>())
            .unwrap()
            .size();
    let unit = BoundExprSet::try_from_parts_in(
        parts,
        UnitLimits {
            expression_semantic_depth: novarocks_type_contract::MAX_CONTROL_DEPTH,
        },
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    (std::sync::Arc::new(unit), pools, source)
}

fn fragment(width: u32, rich: bool, bad: bool) -> (Fragment, ConstantPools, usize) {
    let mut values = BTreeMap::new();
    for ordinal in 0..width {
        let id = ValueId::new(ordinal);
        values.insert(
            id,
            ValueDef {
                id,
                ty: if bad && ordinal == 0 {
                    ValueType::new(DataType::Binary, false)
                } else {
                    ty()
                },
                origin: ValueOrigin::NodeOutput {
                    node: NodeId::new(0),
                    output_ordinal: ordinal,
                },
            },
        );
    }
    let mut nodes = BTreeMap::new();
    nodes.insert(
        NodeId::new(0),
        node(
            0,
            NodeKind::Values {
                rows: Box::default(),
            },
        ),
    );
    let mut heap = 0;
    let mut pools = ConstantPools::empty();
    if rich {
        // Only the outer relation fixture isolates resource laws; every
        // installed expression unit is genuinely sealed and type-correct.
        let (unit, actual_pools, unit_source) = checked_scalar_unit();
        pools = actual_pools;
        heap += unit_source;
        nodes.get_mut(&NodeId::new(0)).unwrap().expressions = Some(unit);
        let aggregate = binding();
        heap += binding_heap(&aggregate) + size_of::<AggregateCall>();
        nodes.insert(
            NodeId::new(1),
            node(
                1,
                NodeKind::Aggregate {
                    group_by: Box::default(),
                    calls: Box::from([AggregateCall {
                        filter_root: None,
                        id: AggregateCallId::new(0),
                        binding: aggregate,
                        arguments: Box::default(),
                        distinct: false,
                        order_by: Box::default(),
                        output: ValueId::new(0),
                    }]),
                    grouping: AggregateGrouping::Complete,
                },
            ),
        );
        let table = BoundTableFunction::from_exact_signature(
            novarocks_type_contract::FunctionId::try_new("test.resource.table").unwrap(),
            novarocks_type_contract::FunctionOverloadId::try_new("test.resource.table.overload")
                .unwrap(),
            novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from([
                FunctionArgumentType::Lambda {
                    parameter_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                        Box::from([ty(), ty()]),
                    ),
                    result_type: ty(),
                },
            ])),
            novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from([ty(), ty()])),
        );
        heap += table.function_id.as_str().len()
            + table.overload.as_str().len()
            + size_of::<FunctionArgumentType>()
            + 4 * size_of::<ValueType>();
        nodes.insert(
            NodeId::new(2),
            node(
                2,
                NodeKind::TableFunction {
                    function: table,
                    arguments: Box::default(),
                    outputs: Box::default(),
                    left_outer: true,
                },
            ),
        );
        let aggregate = binding();
        heap += binding_heap(&aggregate)
            + size_of::<WriterAggregateCall>()
            + 2 * (size_of::<WriterRelationField>() + "rows".len());
        nodes.insert(
            NodeId::new(3),
            node(
                3,
                NodeKind::TableFinish(WriterFinishSpec {
                    count_mode: crate::FinishCountMode::WriterReported,
                    inputs: crate::FinishInputs {
                        writer_results: 0,
                        summaries: Default::default(),
                    },
                    summaries: Default::default(),
                    expected_target_ordinals: Box::default(),
                    input_schema: schema(),
                    output_schema: schema(),
                    final_aggregates: Box::from([WriterAggregateCall {
                        input: ValueId::new(0),
                        binding: aggregate,
                        output: ValueId::new(0),
                    }]),
                    grouped_unpivot: None,
                }),
            ),
        );
        // Aggregate OVER is an actual WindowCallSpec, never a scalar ExprKind.
        let aggregate = binding();
        let call = aggregate.function.clone();
        // The binding's shared signature metadata is one source; the window
        // owns its independent boxed aggregate descriptor and call array.
        heap +=
            binding_heap(&aggregate) + size_of::<AggregateBinding>() + size_of::<WindowCallSpec>();
        nodes.insert(
            NodeId::new(4),
            node(
                4,
                NodeKind::Window(WindowSpec {
                    partition_by: Box::default(),
                    order_by: Box::default(),
                    calls: Box::new([WindowCallSpec {
                        binding: call,
                        distinct: false,
                        arguments: Box::default(),
                        order_by: Box::default(),
                        frame: None,
                        ignore_nulls: false,
                        aggregate_binding: Some(Box::new(aggregate)),
                        output: ValueId::new(0),
                    }]),
                }),
            ),
        );
    }

    // These maps are actually fresh insertion-only sources. The sole locked
    // BTree bound funds their backing, not a private retained-capacity guess.
    let source = size_of::<Fragment>()
        + novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<
            BTreeMap<PhysicalCallSite, PhysicalCallRequest>,
        >())
        .unwrap()
        .size()
        + novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<
            BTreeMap<PhysicalCallSite, FrozenPhysicalCall>,
        >())
        .unwrap()
        .size()
        + heap
        + btree::insertion_only::<ValueId, ValueDef>(values.len())
            .unwrap()
            .request_bytes_upper_bound
        + btree::insertion_only::<NodeId, PhysicalNode>(nodes.len())
            .unwrap()
            .request_bytes_upper_bound;
    let fragment = Fragment::from(crate::plan::FragmentParts {
        summaries: Box::default(),
        stage_placement: crate::StagePlacementPolicy::Singleton,
        id: FragmentId::new(7),
        root: NodeId::new(0),
        values,
        nodes,
        sink: FragmentSink::Noop,
        dop_domain: PipelineDopDomain {
            min: 1,
            max: 1,
            requires_power_of_two: false,
        },
        runtime_filters: Box::default(),
        call_requests: FragmentCallRequests::unpublished_empty(FragmentId::new(7)),
        calls: FrozenFragmentCalls::unpublished_empty(FragmentId::new(7)),
    });
    (fragment, pools, source)
}
fn function_heap(function: &BoundFunction) -> usize {
    function.function_id.as_str().len()
        + function.overload.as_str().len()
        + function.argument_types.len() * size_of::<FunctionArgumentType>()
        + function
            .argument_types
            .iter()
            .map(|argument| match argument {
                FunctionArgumentType::Value(_) => 0,
                FunctionArgumentType::Lambda {
                    parameter_types, ..
                } => parameter_types.len() * size_of::<ValueType>(),
            })
            .sum::<usize>()
}
fn binding_heap(binding: &AggregateBinding) -> usize {
    function_heap(&binding.function) + binding.state_format.as_str().len()
}
#[derive(Debug)]
struct Outcome {
    items: usize,
    bytes: usize,
    errors: Vec<ValidationError>,
    facts: ControlOwnedResourceFacts,
}
fn run_fragment(
    fragment: &Fragment,
    source: usize,
    control: &Control,
) -> Result<Outcome, ControlResourceError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let mut counter = ControlResourceCounter::default();
    let mut cut = CutResourcePreflight::new();
    let mut errors = ValidationContext::new();
    cut.add_fragment_in(
        fragment,
        &mut errors,
        source,
        &mut counter,
        &mut |_| Ok(()),
        &mut work,
    )?;
    work.finish()?;
    Ok(Outcome {
        items: cut.usage.items,
        bytes: cut.usage.bytes,
        errors: errors.into_vec(),
        facts: counter.facts(),
    })
}
fn assert_plain(fragment: &Fragment, actual: &Outcome) {
    let mut errors = ValidationContext::new();
    let usage = fragment_usage(fragment, &mut errors);
    assert_eq!((actual.items, actual.bytes), (usage.items, usage.bytes));
    assert_eq!(actual.errors, errors.into_vec());
}
fn assert_primitives(facts: ControlOwnedResourceFacts, data_roots: usize, logical_roots: usize) {
    assert_eq!(Layout::new::<TypePending<'_>>().size(), 32);
    assert_eq!(facts.allocation_requests_upper_bound, data_roots);
    assert_eq!(
        facts.allocation_request_bytes_upper_bound,
        Layout::new::<TypePending<'_>>().size() * data_roots
    );
    assert!(facts.cumulative_work_upper_bound >= logical_roots);
}

#[test]
fn actual_fragment_call_lambda_cast_window_aggregate_table_and_writer_types_keep_occurrences() {
    let (fragment, _pools, source) = fragment(1, true, false);
    let actual = run_fragment(&fragment, source, &Control::default()).unwrap();
    assert_plain(&fragment, &actual);
    assert!(actual.errors.is_empty());
    // Value1 + checked unit13 + Window5 + Aggregate3 + Table5 + Finish5.
    // Unit: Lambda3 + real body1 + actual constant1 + scalar6 + Cast2.
    // WindowCallSpec has no fake expression-result FVT; its binding has two
    // occurrences and aggregate binding has three. Cast target is Arrow only.
    assert_primitives(actual.facts, 32, 31);
}

#[test]
fn actual_wide_fragment_preserves_all_320_original_value_type_occurrences() {
    let (fragment, _pools, source) = fragment(320, false, false);
    let actual = run_fragment(&fragment, source, &Control::default()).unwrap();
    assert_plain(&fragment, &actual);
    assert_primitives(actual.facts, 320, 320);
    assert_eq!(actual.items, 641); // 320 definitions, one Values node, 320 carrier nodes.
}

fn partitioning() -> EdgePartitioning {
    EdgePartitioning {
        source: Distribution::Singleton,
        destination: Distribution::Singleton,
        source_multiplicity: RowMultiplicity::SingleCopy,
        destination_multiplicity: RowMultiplicity::SingleCopy,
        routing_only: Box::default(),
    }
}
fn filter() -> RuntimeFilter {
    RuntimeFilter {
        id: RuntimeFilterId::new(0),
        kind: RuntimeFilterKind::Bloom,
        domain: RuntimeFilterDomain::Membership {
            ty: ty(),
            null_semantics: RuntimeFilterNullSemantics::NullSafeEqual,
        },
        lifecycle: RuntimeFilterLifecycle::CompleteOnce,
        reduction: RuntimeFilterReduction::SetUnion,
        availability_coverage: RuntimeFilterCoverage {
            nodes: Box::default(),
            root: 0,
        },
        terminal_coverage: RuntimeFilterCoverage {
            nodes: Box::default(),
            root: 0,
        },
        equality_witnesses: Box::default(),
        producers: Box::default(),
        consumers: Box::default(),
        policy: RuntimeFilterPolicy {
            max_contribution_bytes: 1,
            max_artifact_bytes: 1,
            deadline_ms: 1,
            max_retries: 0,
        },
    }
}
fn cuts() -> FragmentCuts {
    let cut_value = CutValue {
        value: ValueId::new(0),
        ty: ty(),
    };
    let import = CutImport {
        source: cut_value.clone(),
        destination: ValueId::new(0),
    };
    let writer = WriterResultCut {
        write_target_ordinal: novarocks_connector_contract::WriteTargetOrdinal::try_new(0).unwrap(),
        schema_revision: 1,
        fields: Box::from([WriterResultCutField {
            source: ValueId::new(0),
            destination: ValueId::new(0),
            name: "rows".into(),
            ty: ty(),
            role: WriterRelationFieldRole::RowCount,
        }]),
    };
    FragmentCuts {
        inbound: Box::from([InboundFragmentCut {
            edge: EdgeId::new(0),
            kind: EdgeKind::Stream,
            source_fragment: FragmentId::new(1),
            destination_node: NodeId::new(0),
            imports: Box::from([import.clone(), import.clone()]),
            partitioning: partitioning(),
            source_port: crate::FragmentOutputPort::Root,
            delivery: crate::DeliveryRequirement::MustDrain,
            payload: crate::ExchangePayloadContract::WriterResults(writer.clone()),
        }]),
        outbound: Box::from([OutboundFragmentCut {
            edge: EdgeId::new(1),
            kind: EdgeKind::Stream,
            destination_fragment: FragmentId::new(2),
            destination_node: NodeId::new(2),
            projection: Box::from([cut_value.clone(), cut_value]),
            destination_imports: Box::from([import]),
            partitioning: partitioning(),
            source_port: crate::FragmentOutputPort::Root,
            delivery: crate::DeliveryRequirement::MustDrain,
            payload: crate::ExchangePayloadContract::WriterResults(writer),
        }]),
        runtime_filters: Box::from([filter()]),
        runtime_filter_bindings: Box::default(),
    }
}
fn cuts_source(cuts: &FragmentCuts) -> usize {
    // Each fixture owns disjoint boxed arrays and literal-name copies; there is
    // no sharing to deduplicate and no private container backing to infer.
    size_of::<FragmentCuts>()
        + size_of::<InboundFragmentCut>()
        + size_of::<OutboundFragmentCut>()
        + 3 * size_of::<CutImport>()
        + 2 * size_of::<CutValue>()
        + 2 * (size_of::<WriterResultCutField>() + "rows".len())
        + cuts.runtime_filters.len() * size_of::<RuntimeFilter>()
}
#[test]
fn actual_cuts_keep_inbound_writer_and_both_outbound_type_occurrences_and_filter() {
    let (fragment, _pools, fragment_source) = fragment(0, false, false);
    let cuts = cuts();
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let mut counter = ControlResourceCounter::default();
    let mut errors = ValidationContext::new();
    let mut actual = CutResourcePreflight::new();
    actual
        .add_cuts_in(
            &fragment,
            &cuts,
            &mut errors,
            fragment_source + cuts_source(&cuts),
            &mut counter,
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap();
    work.finish().unwrap();
    let mut plain_errors = ValidationContext::new();
    let plain = fragment_cut_usage(&fragment, &cuts, &mut plain_errors);
    assert_eq!(
        (actual.usage.items, actual.usage.bytes),
        (plain.items, plain.bytes)
    );
    assert_eq!(errors.into_vec(), plain_errors.into_vec());
    assert_primitives(counter.facts(), 8, 8); // inbound2+writer1, outbound2+import1+writer1, RF1.
}

#[test]
fn each_actual_small_fragment_type_variant_callback_keeps_caller_cause() {
    for bad in [false, true] {
        let (fragment, _pools, source) = fragment(1, false, bad);
        let control = Control::default();
        let actual = run_fragment(&fragment, source, &control).unwrap();
        assert_plain(&fragment, &actual);
        assert!(actual.errors.is_empty());
        let trace = control.trace();
        assert_eq!(trace.first(), Some(&0));
        assert!(trace.last().is_some());
        for cause in CAUSES {
            for stop in 0..trace.len() {
                let control = Control {
                    trace: Mutex::new(Vec::new()),
                    refusal: Some((stop, cause.clone())),
                };
                let error = run_fragment(&fragment, source, &control).unwrap_err();
                assert!(matches!(error, ControlResourceError::Control(actual) if actual == cause));
                assert_eq!(control.trace(), trace[..=stop]);
            }
        }
    }
}

#[test]
fn original_type_request_in_fragment_cuts_and_filter_wins_before_actual_pending_late_controls() {
    let (fragment, _pools, fragment_source) = fragment(1, false, false);
    let cuts = cuts();
    let filter = filter();
    for route in 0..3 {
        for cause in CAUSES {
            let control = Control {
                trace: Mutex::new(Vec::new()),
                refusal: Some((3, cause)),
            };
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            // Real original UTF-8 copy ends with 255 units pending in this caller.
            copy_string::<ControlResourceError>(&"x".repeat(255), &mut work).unwrap();
            assert_eq!(control.trace(), [0, 0, 1]);
            let mut counter = ControlResourceCounter::default();
            let mut errors = ValidationContext::new();
            let mut actual = CutResourcePreflight::new();
            let mut refuse = |facts: &ControlOwnedResourceFacts| {
                if facts.allocation_requests_upper_bound > 0 {
                    Err(CompileControlError::ResourceExhausted)
                } else {
                    Ok(())
                }
            };
            let result = match route {
                0 => actual.add_fragment_in(
                    &fragment,
                    &mut errors,
                    fragment_source,
                    &mut counter,
                    &mut refuse,
                    &mut work,
                ),
                1 => actual.add_cuts_in(
                    &fragment,
                    &cuts,
                    &mut errors,
                    fragment_source + cuts_source(&cuts),
                    &mut counter,
                    &mut refuse,
                    &mut work,
                ),
                _ => actual.add_filter_in(
                    &filter,
                    "filter",
                    &mut errors,
                    size_of::<RuntimeFilter>(),
                    &mut counter,
                    &mut refuse,
                    &mut work,
                ),
            };
            assert!(matches!(
                result,
                Err(ControlResourceError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(control.trace(), [0, 0, 1]);
        }
    }
}

#[test]
fn actual_scan_relation_schema_and_ordered_runtime_filter_use_the_same_type_author() {
    use novarocks_connector_contract::{
        CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
        ConnectorEnvelopeHeader, ConnectorInstanceDescriptor, ConnectorInstanceId,
        ConnectorProviderId, ConnectorReadBinding, ConnectorReadRelationKind,
        ConnectorReadRelationPayload,
    };
    use novarocks_type_contract::owned_resources::layout::{arc_layout, bytes_shared_upper};
    let provider = ConnectorProviderId::parse("iceberg").unwrap();
    let instance = ConnectorInstanceId::parse("resource-fixture").unwrap();
    let binding = ConnectorReadBinding::new(
        ConnectorInstanceDescriptor {
            provider_id: provider.clone(),
            instance_id: instance.clone(),
        },
        CatalogHandle::new(instance, CatalogVersion::from_bytes([7; 32])),
    );
    let encoded = |category| {
        ConnectorEncodedPayload::new(
            ConnectorEnvelopeHeader::new(
                provider.clone(),
                binding.catalog_handle().clone(),
                category,
                ConnectorCodecRevision::try_new(1).unwrap(),
            ),
            vec![1].into(),
        )
    };
    let relation = Relation::Data(DataRelation {
        read: ProviderReadReference {
            binding: binding.clone(),
            input_version: ExactInputVersion::try_new(vec![1]).unwrap(),
            relation: ConnectorReadRelationPayload::new(
                ConnectorReadRelationKind::Table,
                encoded(ConnectorCodecCategory::ReadTable),
                encoded(ConnectorCodecCategory::ReadView),
            ),
        },

        selection_digest: [1; 32],
        schema: Box::from([RelationField {
            column: ProviderColumnReference {
                column_payload: encoded(ConnectorCodecCategory::ReadColumn),
            },
            ty: ty(),
        }]),
        predicate_guarantees: Box::default(),
        provided_properties: properties(),
    });
    let (base, _pools, base_source) = fragment(0, false, false);
    let mut parts = base.into_parts();
    parts.nodes.insert(
        NodeId::new(0),
        node(
            0,
            NodeKind::Scan {
                occurrence: ProviderReadOccurrenceId::new(0),
                relation: Box::new(relation),
                provider_outputs: Box::default(),
                residuals: Box::default(),
                derived_values: Box::default(),
            },
        ),
    );
    let fragment = Fragment::from(parts);
    // Only two identity Arcs, one input-version Arc and three one-byte Bytes
    // sources were authored. Cloned header/column handles do not mint sources.
    let source = base_source
        + size_of::<Relation>()
        + size_of::<RelationField>()
        + arc_layout(Layout::array::<u8>("iceberg".len()).unwrap())
            .unwrap()
            .size()
        + arc_layout(Layout::array::<u8>("resource-fixture".len()).unwrap())
            .unwrap()
            .size()
        + arc_layout(Layout::array::<u8>(1).unwrap()).unwrap().size()
        + 3 * (1 + bytes_shared_upper().unwrap());
    let actual = run_fragment(&fragment, source, &Control::default()).unwrap();
    assert_plain(&fragment, &actual);
    assert_primitives(actual.facts, 1, 1);

    let mut filter = filter();
    filter.kind = RuntimeFilterKind::MinMax;
    filter.domain = RuntimeFilterDomain::Ordered {
        key: RuntimeFilterOrderKey {
            ty: ty(),
            direction: SortDirection::Descending,
            null_ordering: NullOrdering::Last,
        },
        inclusive: false,
        comparator: novarocks_type_contract::OrderedComparisonAlgorithm::NativeScalarOrderV1,
    };
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let mut counter = ControlResourceCounter::default();
    let mut errors = ValidationContext::new();
    let mut actual = CutResourcePreflight::new();
    actual
        .add_filter_in(
            &filter,
            "filter",
            &mut errors,
            size_of::<RuntimeFilter>(),
            &mut counter,
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap();
    work.finish().unwrap();
    let mut plain_errors = ValidationContext::new();
    let mut plain = CutResourcePreflight::new();
    plain.add_filter(&filter, "filter", &mut plain_errors);
    assert_eq!(
        (actual.usage.items, actual.usage.bytes),
        (plain.usage.items, plain.usage.bytes)
    );
    assert_eq!(errors.into_vec(), plain_errors.into_vec());
    assert_primitives(counter.facts(), 1, 1);
}

#[test]
fn actual_fragment_three_numeric_axes_accept_exact_snapshot_and_refuse_one_under() {
    let (fragment, _pools, source) = fragment(1, true, false);
    let complete = run_fragment(&fragment, source, &Control::default())
        .unwrap()
        .facts;
    for axis in 0..3 {
        for under in [false, true] {
            let cap = match axis {
                0 => complete.allocation_requests_upper_bound,
                1 => complete.allocation_request_bytes_upper_bound,
                _ => complete.cumulative_work_upper_bound,
            } - usize::from(under);
            let control = Control::default();
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            let mut counter = ControlResourceCounter::default();
            let mut errors = ValidationContext::new();
            let mut actual = CutResourcePreflight::new();
            let result = actual.add_fragment_in(
                &fragment,
                &mut errors,
                source,
                &mut counter,
                &mut |facts| {
                    let value = match axis {
                        0 => facts.allocation_requests_upper_bound,
                        1 => facts.allocation_request_bytes_upper_bound,
                        _ => facts.cumulative_work_upper_bound,
                    };
                    if value > cap {
                        Err(CompileControlError::ResourceExhausted)
                    } else {
                        Ok(())
                    }
                },
                &mut work,
            );
            if under {
                assert!(matches!(
                    result,
                    Err(ControlResourceError::Control(
                        CompileControlError::ResourceExhausted
                    ))
                ));
                // A typed refusal owns its tail; no caller finish is issued.
            } else {
                result.unwrap();
                work.finish().unwrap();
                assert_eq!(counter.facts(), complete);
            }
        }
    }
}
