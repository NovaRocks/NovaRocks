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

//! Physical-plan authoring of real aggregate calls for compiled-path tests.
//!
//! Every binding comes from the real builtin resolver, never a forged one.
//! Each aggregate call site carries what the FE would freeze for it:
//! - its original request: the logical argument types in every phase, never
//!   a state type;
//! - its own relational context, a use and an unguarded domain disjoint from
//!   every expression occurrence;
//! - its frozen effects, obtained from a fresh preparation by the installed
//!   owner with the same inputs the BE compiler later uses.
//!
//! Expression roots reuse the original Scan fixture's eager use-tree author,
//! including binary residuals. Relational calls retain their separate domains.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_functions::{
    AggregateKernelPhase, AggregatePreparationOptions, AggregateStateFormatIdentity,
    CallEffectInput, EngineFunctionCatalogBuilder, FunctionArgument, FunctionBindingRequest,
    FunctionBindingSelection, FunctionId, FunctionKind, FunctionOverloadId, FunctionResultType,
    InstalledPureKernel, PureCallPreparation, PureEngineFunctionCatalog,
    PureImplementationDeclaration, PureImplementationId, PureKernelAbi, ScopedExpressionEffects,
};
use novarocks_local_compiler::{
    LocalCompileOptions, compile_fragment, validate_fragment_providers,
};
use novarocks_local_program::{KernelAbiVersion, LocalProgram};
use novarocks_physical_plan::{
    AggregateBinding, AggregateCall, AggregateCallId, AggregateGrouping, AggregatePhase,
    BoundFunction, Distribution, Edge, EdgeDestination, EdgeId, EdgeKind, EdgePartitioning,
    EdgeSource, ExprKind, ExpressionRootRole, ExpressionRootSite, Fragment, FragmentBuilder,
    FragmentId, FragmentPackage, FragmentPackageAdmission, FragmentSink, FrozenCallError,
    FrozenFragmentCalls, FrozenFragmentPruning, FrozenPhysicalCall, FunctionArgumentType,
    HashDefinition, HashPartitionScheme, LiteralValue, NodeId, NodeKind, OutputPort,
    PartitionCountDomain, PartitionCountParameter, PartitionCountParameterId, PartitionSpaceId,
    PhysicalCallBinding, PhysicalCallDefinition, PhysicalCallRequest, PhysicalCallSite,
    PhysicalNode, PhysicalPlan, PhysicalProperties, PhysicalRootUses, PipelineDopDomain,
    PlanLimits, PropertyProofProjectionLimits, RowMultiplicity, StaticFunctionArgument, ValueId,
    ValueOrigin, extract_fragment_packages,
};
use novarocks_type_contract::{
    CallProofScope, CompileCheckpoints, CompilePhase, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationDomainId, ExpressionControlFlow, ExpressionEffectContext, ExpressionEffects,
    ExpressionEvaluationDomain, ExpressionUseId, FunctionValueType, SemanticParameters,
};
use novarocks_types::UniqueId;

use super::family_fixture::{FixtureControl, constant_policy};
use crate::exec::chunk::Chunk;
use crate::exec::fragment::program::FragmentNodeId;
use crate::exec::operators::{ResultSinkBuffer, ResultSinkFactory};
use crate::exec::pipeline::binding::{ExchangeBindings, ScanBindings};
use crate::exec::pipeline::executor::prepare_compiled_program_pipeline_execution_with_profiler;
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::endpoint::{TaskDestination, RuntimeEndpoint};
use crate::runtime::exchange::online_unit;
use crate::runtime::runtime_state::RuntimeState;
use crate::runtime::task::exchange::{
    CompiledExchangeReceivers, materialize_compiled_exchange_receivers,
};
use crate::runtime::task::instance::{
    ExchangeInputAssignment, ExchangeInputAssignments, TaskSinkAssignment,
};
use crate::runtime::task::io::{ExchangeFrameTransmitter, ExchangeReceiverPort, NoopTaskEventSink};
use crate::runtime::task::sink::materialize_compiled_sink;

/// The frozen decimal policy every fixture call authors explicitly.
pub(crate) const POLICY: DecimalOverflowPolicy = DecimalOverflowPolicy::ReportError;
/// Relational use IDs start far above every expression use of a fixture.
const RELATIONAL_USE_BASE: u32 = 1_000_000;

/// Direct operator fixtures install the same original tracker/control host as
/// pipeline preparation; the representation profile has one production owner.
pub(crate) fn runtime_construction_factory(
    tracker: Arc<crate::runtime::mem_tracker::MemTracker>,
    error: Arc<crate::runtime::runtime_state::RuntimeErrorState>,
) -> Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory> {
    crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        tracker,
        error,
    )
    .expect("original operator construction host is installed")
}

/// A sealed subset of the real builtin catalogue holding exactly the named
/// aggregate owners with their installed ABIs.
pub(crate) fn aggregate_catalog(owners: &[(&str, PureKernelAbi)]) -> PureEngineFunctionCatalog {
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    for (name, _) in owners {
        builder
            .register(
                actual
                    .definition(name, FunctionKind::Aggregate)
                    .unwrap()
                    .clone(),
            )
            .unwrap();
    }
    // Independent records of the actually installed implementations.
    builder
        .seal_pure(owners.iter().map(|(name, abi)| {
            InstalledPureKernel {
                function: FunctionId::try_new(format!("builtin.aggregate/{name}/v1")).unwrap(),
                kind: FunctionKind::Aggregate,
                implementation: PureImplementationDeclaration {
                    overload: FunctionOverloadId::try_new(format!(
                        "builtin.aggregate/{name}/derived-v1"
                    ))
                    .unwrap(),
                    implementation: PureImplementationId::try_new(format!(
                        "builtin.aggregate/{name}/selected-v1"
                    ))
                    .unwrap(),
                    abi: *abi,
                },
                // SUM's exact state is its second format.
                aggregate_state_format: Some(
                    AggregateStateFormatIdentity::try_new(if *name == "sum" {
                        "novarocks/sum/state-v2".to_owned()
                    } else {
                        format!("novarocks/{name}/state-v1")
                    })
                    .unwrap(),
                ),
            }
        }))
        .unwrap()
}

/// COUNT, MIN and MAX: the owners Stage 1 runs.
pub(crate) fn extrema_catalog() -> PureEngineFunctionCatalog {
    aggregate_catalog(&[
        ("count", PureKernelAbi::AggregateWindowV1),
        ("min", PureKernelAbi::AggregateWindowV1),
        ("max", PureKernelAbi::AggregateWindowV1),
    ])
}

/// One resolved aggregate selection, from the real resolver.
#[derive(Clone, Debug)]
pub(crate) struct Bound {
    pub function_id: FunctionId,
    pub selected: FunctionBindingSelection,
    pub semantics: novarocks_functions::FunctionSemantics,
    pub arguments: Vec<FunctionValueType>,
    pub state_interpretation: Option<Arc<novarocks_type_contract::AggregateStateInterpretation>>,
}

impl Bound {
    pub(crate) fn result_type(&self) -> FunctionValueType {
        match &self.selected.result_type {
            FunctionResultType::Scalar(value) => value.clone(),
            other => panic!("aggregate result is scalar, got {other:?}"),
        }
    }
    pub(crate) fn intermediate_type(&self) -> FunctionValueType {
        self.selected
            .aggregate
            .as_ref()
            .expect("aggregate selection")
            .intermediate_type
            .clone()
    }
    /// The value type a call in `phase` emits.
    pub(crate) fn output_type(&self, phase: AggregatePhase) -> FunctionValueType {
        if phase.produces_final_result() {
            self.result_type()
        } else {
            self.intermediate_type()
        }
    }
    pub(crate) fn binding(&self, phase: AggregatePhase) -> AggregateBinding {
        let aggregate = self
            .selected
            .aggregate
            .as_ref()
            .expect("aggregate selection");
        // The plan is published through its original producer facts, as the
        // FE does; the package's frozen calls stay the only effect authority.
        let mut function = BoundFunction::from_exact_signature(
            self.function_id.clone(),
            self.selected.overload.clone(),
            FunctionKind::Aggregate,
            self.selected.argument_types.clone(),
            self.result_type(),
        );
        function.legacy_metadata = Some(novarocks_physical_plan::LegacyBindingMetadata {
            volatility: self.semantics.volatility,
            argument_evaluation: self.semantics.argument_evaluation,
            failure_behavior: self.semantics.failure_behavior,
            intrinsic_row_error: self.semantics.intrinsic_row_error,
            semantic_parameters: Box::default(),
        });
        AggregateBinding {
            // The physical port owns this small facts projection. Its guarded
            // order collection shares the original immutable backing.
            state_interpretation: self.state_interpretation.as_deref().cloned(),
            state_argument_contract: aggregate.state_argument_contract,
            function,
            phase,
            logical_argument_count: u32::try_from(self.arguments.len()).unwrap(),
            intermediate_type: aggregate.intermediate_type.clone(),
            state_format: aggregate.state_format.clone(),
        }
    }
}

pub(crate) fn bind(
    catalog: &PureEngineFunctionCatalog,
    name: &str,
    arguments: &[FunctionValueType],
) -> Bound {
    let request = arguments
        .iter()
        .map(|value_type| FunctionArgument::Value {
            value_type: value_type.clone(),
            constant: None,
        })
        .collect::<Vec<_>>();
    let bound = catalog
        .metadata()
        .resolve_bound_user(
            name,
            FunctionKind::Aggregate,
            FunctionBindingRequest {
                arguments: &request,
                logical_argument_count: request.len(),
                expected_result_type: None,
            },
            &FixtureControl,
        )
        .unwrap_or_else(|error| panic!("{name} binds: {error}"));
    Bound {
        function_id: bound.function_id,
        selected: bound.selected,
        semantics: bound.semantics,
        arguments: arguments.to_vec(),
        state_interpretation: None,
    }
}

pub(crate) fn kernel_phase(phase: AggregatePhase) -> AggregateKernelPhase {
    match phase {
        AggregatePhase::Single => AggregateKernelPhase::Single,
        AggregatePhase::Partial { .. } => AggregateKernelPhase::Partial,
        AggregatePhase::Intermediate { .. } => AggregateKernelPhase::Intermediate,
        AggregatePhase::Final { .. } => AggregateKernelPhase::Final,
    }
}

/// One aggregate call of a fixture node. An update phase reads its logical
/// argument values; a merge phase reads exactly one state value.
pub(crate) struct CallSpec<'a> {
    pub bound: &'a Bound,
    pub phase: AggregatePhase,
    pub id: AggregateCallId,
    pub arguments: Vec<ValueId>,
    pub distinct: bool,
}

/// The output distribution the physical contract derives for an aggregate.
fn aggregate_distribution(input: &Distribution, output: &[ValueId]) -> Distribution {
    match input {
        Distribution::Singleton => Distribution::Singleton,
        Distribution::Hash { keys, .. } | Distribution::BucketShuffle { keys, .. }
            if keys.iter().all(|key| output.contains(key)) =>
        {
            input.clone()
        }
        _ => Distribution::Unconstrained,
    }
}

/// Add an Aggregate over `input`: the group values pass through as outputs,
/// followed by one output per call.
pub(crate) fn add_aggregate(
    builder: &mut FragmentBuilder,
    input: NodeId,
    groups: &[ValueId],
    calls: &[CallSpec<'_>],
    grouping: AggregateGrouping,
) -> (NodeId, Vec<ValueId>) {
    let node = builder.reserve_node_id().unwrap();
    let input_properties = builder.node_output_properties(input).unwrap().clone();
    let group_by = groups
        .iter()
        .map(|value| {
            let ty = builder.value(*value).unwrap().ty.clone();
            let expression = builder
                .add_expression(node, ty, ExprKind::Value(*value))
                .unwrap();
            (expression, *value)
        })
        .collect::<Vec<_>>();
    let mut columns = groups.to_vec();
    let mut physical_calls = Vec::new();
    for call in calls {
        let arguments = call
            .arguments
            .iter()
            .map(|value| {
                let ty = builder.value(*value).unwrap().ty.clone();
                builder
                    .add_expression(node, ty, ExprKind::Value(*value))
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let origin = if call.phase.produces_final_result() {
            ValueOrigin::AggregateResult { call: call.id }
        } else {
            ValueOrigin::AggregateState {
                call: call.id,
                phase: call.phase,
            }
        };
        let output = builder
            .add_value(call.bound.output_type(call.phase), origin)
            .unwrap();
        columns.push(output);
        physical_calls.push(AggregateCall {
            id: call.id,
            binding: call.bound.binding(call.phase),
            arguments: arguments.into_boxed_slice(),
            distinct: call.distinct,
            order_by: Box::default(),
            output,
        });
    }
    let distribution = aggregate_distribution(&input_properties.distribution, &columns);
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::from([input]),
            required_inputs: Box::from([input_properties]),
            output_properties: PhysicalProperties {
                distribution,
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            },
            output: OutputPort {
                node,
                columns: columns.clone().into_boxed_slice(),
            },
            kind: NodeKind::Aggregate {
                group_by: group_by.into_boxed_slice(),
                calls: physical_calls.into_boxed_slice(),
                grouping,
            },
        })
        .unwrap();
    (node, columns)
}

/// A Values leaf whose columns have the given complete types and whose rows
/// are literal cells of exactly those types.
pub(crate) fn values(
    builder: &mut FragmentBuilder,
    node: NodeId,
    types: &[FunctionValueType],
    rows: &[Vec<LiteralValue>],
) -> Vec<ValueId> {
    super::family_fixture::values(builder, node, types, rows)
}

pub(crate) fn dop(max: u32) -> PipelineDopDomain {
    PipelineDopDomain {
        min: 1,
        max,
        requires_power_of_two: false,
    }
}

/// A native-exchange hash scheme over `keys`.
pub(crate) fn hash(keys: &[ValueId]) -> Distribution {
    Distribution::Hash {
        keys: Box::from(keys),
        scheme: HashPartitionScheme {
            space: PartitionSpaceId::try_new([61; 32]).unwrap(),
            count: PartitionCountParameter {
                id: PartitionCountParameterId::try_new([62; 32]).unwrap(),
                admissible: PartitionCountDomain {
                    min: 1,
                    max: 64,
                    requires_power_of_two: true,
                },
            },
            definition: HashDefinition::native_exchange(),
        },
    }
}

/// Imports `sources` of `edge` as an ExchangeSource at `node`.
pub(crate) fn receive(
    builder: &mut FragmentBuilder,
    node: NodeId,
    edge: EdgeId,
    sources: &[(ValueId, FunctionValueType)],
    distribution: impl Fn(&[ValueId]) -> Distribution,
) -> Vec<ValueId> {
    let imports = sources
        .iter()
        .map(|(source, ty)| {
            builder
                .add_value(
                    ty.clone(),
                    ValueOrigin::ExchangeImport {
                        edge,
                        source_value: *source,
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    builder
        .add_exchange_source(
            node,
            edge,
            sources
                .iter()
                .map(|(source, _)| *source)
                .zip(imports.iter().copied())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            imports.clone().into_boxed_slice(),
            distribution(&imports),
            RowMultiplicity::SingleCopy,
        )
        .unwrap();
    imports
}

pub(crate) fn edge(
    id: EdgeId,
    from: (FragmentId, &[ValueId]),
    to: (FragmentId, NodeId, &[ValueId]),
    source: Distribution,
    destination: Distribution,
) -> Edge {
    Edge {
        role: novarocks_physical_plan::OutputRole::Relational,
        delivery: novarocks_physical_plan::DeliveryRequirement::DemandDriven,
        id,
        kind: EdgeKind::Stream,
        source: EdgeSource {
            port: novarocks_physical_plan::FragmentOutputPort::Root,
            fragment: from.0,
            projection: Box::from(from.1),
        },
        destination: EdgeDestination {
            fragment: to.0,
            node: to.1,
            receive_mapping: from
                .1
                .iter()
                .copied()
                .zip(to.2.iter().copied())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        },
        partitioning: EdgePartitioning {
            source,
            source_multiplicity: RowMultiplicity::SingleCopy,
            destination,
            destination_multiplicity: RowMultiplicity::SingleCopy,
        },
    }
}

/// Finish a fragment and attach one original request per relational call:
/// its logical argument types, in every phase.
pub(crate) fn finish(
    builder: FragmentBuilder,
    root: NodeId,
    sink: FragmentSink,
    max_dop: u32,
) -> Fragment {
    let fragment = builder
        .finish_definition(
            root,
            sink,
            dop(max_dop),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let mut entries = fragment
        .call_requests()
        .entries()
        .iter()
        .map(|(definition, request)| (*definition, request.clone()))
        .collect::<Vec<_>>();
    let mut work = CompileCheckpoints::try_new(&FixtureControl, CompilePhase::Validate).unwrap();
    novarocks_physical_plan::visit_relational_calls_observed::<FrozenCallError>(
        &fragment,
        &mut work,
        |site, binding, _| {
            let PhysicalCallBinding::Aggregate(binding) = binding else {
                panic!("fixture relational calls are aggregates")
            };
            let arguments = binding
                .function
                .argument_types
                .iter()
                .map(|argument| match argument {
                    FunctionArgumentType::Value(value_type) => StaticFunctionArgument::Value {
                        value_type: value_type.clone(),
                        constant: None,
                    },
                    FunctionArgumentType::Lambda { .. } => panic!("aggregate value channel"),
                })
                .collect();
            entries.push((
                PhysicalCallDefinition::Relational(site),
                PhysicalCallRequest {
                    arguments,
                    logical_argument_count: binding.logical_argument_count as usize,
                    expected_result_type: None,
                    constant_policy: constant_policy(),
                },
            ));
            Ok(())
        },
    )
    .unwrap();
    work.finish().unwrap();
    fragment
        .with_call_requests_observed(entries, &FixtureControl)
        .unwrap()
}

/// Root uses and frozen calls of one fragment. Expression roots share domain
/// 0; aggregate call `k` owns domain `k + 1` and a relational use of its own.
fn freeze(
    fragment: &Fragment,
    catalog: &PureEngineFunctionCatalog,
    parameters: &SemanticParameters,
) -> (PhysicalRootUses, FrozenFragmentCalls) {
    let expression_roots = crate::runtime::task::scan::compiled_fixture::root_uses(fragment);
    let expression_domain = EvaluationDomainId::new(0);
    let uses = expression_roots
        .flow()
        .uses()
        .values()
        .cloned()
        .collect::<Vec<_>>();
    let bindings = expression_roots
        .bindings()
        .iter()
        .map(|(site, id)| (*site, *id))
        .collect();
    let mut domains = vec![ExpressionEvaluationDomain {
        id: expression_domain,
        parent: None,
        guard: None,
    }];
    let mut sites = Vec::new();
    for node in fragment.nodes().values() {
        let (calls, topn) = match &node.kind {
            NodeKind::Aggregate { calls, .. } => (calls, false),
            NodeKind::TopN {
                reduction: novarocks_physical_plan::TopNReduction::GroupedStates { calls, .. },
                ..
            } => (calls, true),
            _ => continue,
        };
        for ordinal in 0..calls.len() {
            let k = u32::try_from(sites.len()).unwrap();
            let context = ExpressionEffectContext {
                use_id: ExpressionUseId::new(RELATIONAL_USE_BASE + k),
                domain: EvaluationDomainId::new(k + 1),
                demand: EvaluationDemand::Value,
            };
            domains.push(ExpressionEvaluationDomain {
                id: context.domain,
                parent: None,
                guard: None,
            });
            let call = u32::try_from(ordinal).unwrap();
            let site = if topn {
                PhysicalCallSite::TopNState {
                    node: node.id,
                    call,
                }
            } else {
                PhysicalCallSite::Aggregate {
                    node: node.id,
                    call,
                }
            };
            sites.push((node.id, call, context, site));
        }
    }
    let flow = ExpressionControlFlow::try_new(
        domains,
        uses,
        fragment.expressions(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap();
    let root_uses = PhysicalRootUses::try_new(fragment, flow, bindings, &FixtureControl).unwrap();
    let mut frozen = Vec::new();
    for (node, ordinal, context, call_site) in sites {
        let calls = match &fragment.nodes()[&node].kind {
            NodeKind::Aggregate { calls, .. }
            | NodeKind::TopN {
                reduction: novarocks_physical_plan::TopNReduction::GroupedStates { calls, .. },
                ..
            } => calls,
            _ => unreachable!(),
        };
        let call = &calls[ordinal as usize];
        let binding = &call.binding;
        let selection = Arc::new(FunctionBindingSelection {
            overload: binding.function.overload.clone(),
            argument_types: binding.function.argument_types.clone(),
            result_type: FunctionResultType::Scalar(binding.function.result_type.clone()),
            aggregate: Some(novarocks_functions::AggregateBindingSelection {
                state_argument_contract: binding.state_argument_contract,
                intermediate_type: binding.intermediate_type.clone(),
                state_format: binding.state_format.clone(),
            }),
        });
        let request = binding
            .function
            .argument_types
            .iter()
            .map(|argument| match argument {
                FunctionArgumentType::Value(value_type) => FunctionArgument::Value {
                    value_type: value_type.clone(),
                    constant: None,
                },
                FunctionArgumentType::Lambda { .. } => panic!("aggregate value channel"),
            })
            .collect::<Vec<_>>();
        let argument_uses = (0..call.arguments.len())
            .map(|argument| {
                let site = ExpressionRootSite {
                    node,
                    role: if matches!(call_site, PhysicalCallSite::TopNState { .. }) {
                        ExpressionRootRole::TopNStateArgument {
                            call: ordinal,
                            argument: u32::try_from(argument).unwrap(),
                        }
                    } else {
                        ExpressionRootRole::AggregateArgument {
                            call: ordinal,
                            argument: u32::try_from(argument).unwrap(),
                        }
                    },
                };
                Some(root_uses.bindings()[&site])
            })
            .collect::<Vec<_>>();
        let phase = kernel_phase(binding.phase);
        let state_type;
        let (uses, options) = if phase.consumes_logical_arguments() {
            (
                novarocks_functions::CallArgumentUses::SelectedChannels(&argument_uses),
                // A call's frozen effects do not depend on DISTINCT; whether
                // the owner implements DISTINCT is the compiler's preparation.
                AggregatePreparationOptions {
                    state_interpretation: binding.state_interpretation.clone().map(Arc::new),
                    phase,
                    distinct: false,
                    order_keys: Arc::from([]),
                    state_input_type: None,
                },
            )
        } else {
            let state_use = argument_uses[0].unwrap();
            state_type = fragment
                .expressions()
                .get(call.arguments[0])
                .unwrap()
                .ty
                .clone();
            (
                novarocks_functions::CallArgumentUses::AggregateMerge {
                    phase,
                    state_context: root_uses.flow().uses()[&state_use].context,
                    state_input_type: &state_type,
                },
                AggregatePreparationOptions {
                    state_interpretation: binding.state_interpretation.clone().map(Arc::new),
                    phase,
                    distinct: false,
                    order_keys: Arc::from([]),
                    state_input_type: Some(state_type.clone()),
                },
            )
        };
        let token = catalog
            .prepare_fresh(
                CallEffectInput {
                    context,
                    argument_uses: uses,
                    function_id: &binding.function.function_id,
                    kind: FunctionKind::Aggregate,
                    selected: selection.as_ref(),
                    request: FunctionBindingRequest {
                        arguments: &request,
                        logical_argument_count: binding.logical_argument_count as usize,
                        expected_result_type: None,
                    },
                    environment: &[],
                    parameters,
                    decimal_overflow_policy: POLICY,
                    proof_scope: CallProofScope::Domain(context.domain),
                },
                Arc::clone(&selection),
                PureCallPreparation::Aggregate {
                    arguments: ScopedExpressionEffects::primitive(
                        context,
                        ExpressionEffects::PURE_VALUE,
                    ),
                    options,
                },
                &FixtureControl,
            )
            .unwrap_or_else(|error| panic!("fixture aggregate call prepares: {error}"));
        frozen.push(FrozenPhysicalCall {
            materialized_writer_input: None,
            regexp_count_pattern_source: None,
            to_base64_byte_source: None,
            temporal_source: None,
            site: call_site,
            context,
            effects: token.call_contract().effects().clone(),
            decimal_overflow_policy: POLICY,
        });
    }
    let calls = FrozenFragmentCalls::try_new(fragment, &root_uses, frozen, &FixtureControl)
        .unwrap_or_else(|error| panic!("frozen aggregate calls validate: {error}"));
    (root_uses, calls)
}

// Explicit small-fixture admission; these are test inputs, not defaults.
fn admission() -> FragmentPackageAdmission {
    FragmentPackageAdmission {
        plan_limits: PlanLimits::FROZEN,
        source_retained_bytes: 64 * 1024 * 1024,
        property_projection_limits: PropertyProofProjectionLimits {
            max_request_bytes: 16 * 1024 * 1024,
            max_coexisting_bytes: 256 * 1024 * 1024,
            max_projection_work: 16 * 1024 * 1024,
        },
    }
}

/// Every fragment of `plan` as a checked package with its frozen calls.
pub(crate) fn packages(
    plan: &PhysicalPlan,
    catalog: &PureEngineFunctionCatalog,
) -> BTreeMap<FragmentId, FragmentPackage> {
    packages_with_reads(plan, catalog, &BTreeMap::new())
}

/// The same aggregate root/call author with explicitly frozen provider reads.
pub(crate) fn packages_with_reads(
    plan: &PhysicalPlan,
    catalog: &PureEngineFunctionCatalog,
    reads: &BTreeMap<
        novarocks_physical_plan::ProviderReadOccurrenceId,
        novarocks_connector_contract::FrozenConnectorRead,
    >,
) -> BTreeMap<FragmentId, FragmentPackage> {
    let mut uses = BTreeMap::new();
    let mut calls = BTreeMap::new();
    for (id, fragment) in plan.fragments() {
        let (root_uses, frozen) = freeze(fragment, catalog, plan.parameters());
        uses.insert(*id, root_uses);
        calls.insert(*id, frozen);
    }
    let pruning = plan
        .fragments()
        .keys()
        .map(|id| {
            (
                *id,
                FrozenFragmentPruning::try_new(*id, vec![], &FixtureControl).unwrap(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let admissions = plan
        .fragments()
        .keys()
        .map(|id| (*id, admission()))
        .collect::<BTreeMap<_, _>>();
    extract_fragment_packages(
        plan,
        reads,
        &BTreeMap::new(),
        &uses,
        &calls,
        &pruning,
        &admissions,
        &FixtureControl,
    )
    .unwrap_or_else(|error| panic!("aggregate packages extract: {error:?}"))
}

pub(crate) fn try_compile(
    package: FragmentPackage,
    catalog: &PureEngineFunctionCatalog,
    pipeline_dop: usize,
    result: bool,
) -> Result<Arc<LocalProgram>, String> {
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &FixtureControl)
            .unwrap();
    try_compile_with_providers(package, catalog, pipeline_dop, result, &providers)
}

fn try_compile_with_providers<E: std::error::Error + 'static>(
    package: FragmentPackage,
    catalog: &PureEngineFunctionCatalog,
    pipeline_dop: usize,
    result: bool,
    providers: &PureProviderProgramCatalog<E>,
) -> Result<Arc<LocalProgram>, String> {
    let validated = validate_fragment_providers(Arc::new(package), providers, &FixtureControl)
        .map_err(|error| error.to_string())?;
    let options = LocalCompileOptions {
        pipeline_dop: NonZeroUsize::new(pipeline_dop).unwrap(),
        // Only a Result root has a frozen placement; a stream sink has none.
        root_sink_dop: result.then(|| NonZeroUsize::new(1).unwrap()),
        kernel_abi: KernelAbiVersion::CURRENT,
        constants: constant_policy(),
    };
    compile_fragment(validated, catalog, options, &FixtureControl)
        .map(Arc::new)
        .map_err(|error| error.to_string())
}

pub(crate) fn compile(
    package: FragmentPackage,
    catalog: &PureEngineFunctionCatalog,
    pipeline_dop: usize,
    result: bool,
) -> Arc<LocalProgram> {
    try_compile(package, catalog, pipeline_dop, result)
        .unwrap_or_else(|error| panic!("aggregate fragment compiles: {error}"))
}

/// Preserve the original compile/options author while admitting the fixture's
/// actual frozen Scan through its existing pure provider catalogue.
pub(crate) fn compile_with_providers<E: std::error::Error + 'static>(
    package: FragmentPackage,
    catalog: &PureEngineFunctionCatalog,
    pipeline_dop: usize,
    result: bool,
    providers: &PureProviderProgramCatalog<E>,
) -> Arc<LocalProgram> {
    try_compile_with_providers(package, catalog, pipeline_dop, result, providers)
        .unwrap_or_else(|error| panic!("aggregate scan fragment compiles: {error}"))
}

/// Test-only observational decoder bound before delivery to the exact source
/// branch. Actual Unit custody still belongs exclusively to the loopback port.
pub(crate) struct PreparedUnitRecording {
    schema: crate::exec::chunk::ChunkSchemaRef,
    recipe: Arc<online_unit::ExchangeRecipe>,
    factory: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
}

impl PreparedUnitRecording {
    /// Freeze the same source declaration and ordered branch projection as the
    /// actual sender. Retain finite creator storage, never the source Program.
    pub(crate) fn new(
        program: &LocalProgram,
        factory: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
    ) -> (i32, Self) {
        let Some(novarocks_local_program::StaticSinkProgram::DataStream { branch, .. }) =
            program.graph().sink()
        else {
            panic!("recording fixture requires its actual DataStream branch");
        };
        let output = program.graph().nodes()[program.graph().root().index()].output_layout();
        let owner = factory.begin_compiled_layout_construction(output).unwrap();
        // Empty columns retain the sender's complete-layout convention,
        // including an actually empty layout carrying count-only rows.
        let projected = output
            .project_by_slots_typed_with_creator(
                branch.output_columns(),
                owner.storage_lifetime(),
                |layout| {
                    owner
                        .request(layout)
                        .map_err(novarocks_local_program::LayoutCreationFailure::Creator)
                },
                &FixtureControl,
            )
            .unwrap();
        let schema = factory.create_compiled_chunk_schema(&projected).unwrap();
        let recipe = online_unit::prepare_recipe(&schema, &[], factory.as_ref()).unwrap();
        (
            branch.dest_node_id(),
            Self {
                schema,
                recipe,
                factory,
            },
        )
    }

    pub(crate) fn chunks(&self, payload: &[u8]) -> Vec<Chunk> {
        online_unit::decode(&self.recipe, payload, &self.schema, self.factory.as_ref())
            .expect("recorded unit must match its frozen source recipe")
    }
}

/// Delivers every encoded frame to the receiver port and counts the rows sent.
pub(crate) struct LoopbackTransmitter {
    unit: Arc<crate::runtime::task::io::exchange::test_support::UnitLoopback>,
    rows: Mutex<BTreeMap<(UniqueId, UniqueId), usize>>,
    recordings: Mutex<BTreeMap<(UniqueId, i32), Arc<PreparedUnitRecording>>>,
}

impl LoopbackTransmitter {
    pub(crate) fn new(
        unit: Arc<crate::runtime::task::io::exchange::test_support::UnitLoopback>,
    ) -> Arc<Self> {
        Arc::new(Self {
            unit,
            rows: Mutex::default(),
            recordings: Mutex::default(),
        })
    }
    /// Rows `sender` sent to `destination`.
    pub(crate) fn rows(&self, sender: UniqueId, destination: UniqueId) -> usize {
        self.rows
            .lock()
            .unwrap()
            .get(&(sender, destination))
            .copied()
            .unwrap_or_default()
    }
}

impl ExchangeFrameTransmitter for LoopbackTransmitter {
    fn deliver_unit(
        &self,
        frame: crate::runtime::task::io::exchange_unit::ExchangeUnitFrame,
        stop: tokio_util::sync::CancellationToken,
        activity: crate::runtime::task::io::exchange_unit::ExchangeCallActivity,
        done: crate::runtime::task::io::exchange_unit::ExchangeDeliveryCallback,
    ) {
        let carried = if frame.receiver.payload.is_empty() {
            0
        } else {
            let key = (
                frame.receiver.stream.source().task_key(),
                frame.receiver.stream.destination_node().get(),
            );
            let recording = self
                .recordings
                .lock()
                .unwrap()
                .get(&key)
                .cloned()
                .expect("sender installed its exact branch before Unit delivery");
            recording
                .chunks(&frame.receiver.payload)
                .iter()
                .map(Chunk::len)
                .sum()
        };
        *self
            .rows
            .lock()
            .unwrap()
            .entry((
                frame.receiver.stream.source().task_key(),
                frame.receiver.stream.destination_task_key(),
            ))
            .or_default() += carried;
        self.unit.deliver_unit(frame, stop, activity, done);
    }
    fn register_unit_demands(
        &self,
        destination: RuntimeEndpoint,
        entries: Vec<crate::runtime::task::io::exchange_unit::ExchangeDemandEntry>,
        stop: tokio_util::sync::CancellationToken,
        done: crate::runtime::task::io::exchange_unit::ExchangeDemandCallback,
    ) {
        self.unit
            .register_unit_demands(destination, entries, stop, done)
    }
    fn watch_unit_facts(
        &self,
        destination: RuntimeEndpoint,
        stream: novarocks_execution_contract::ExchangeStreamIdentity,
        wake: crate::runtime::task::io::exchange_unit::ExchangeGrantWake,
    ) -> Result<(), novarocks_failure_contract::FailureFact> {
        self.unit.watch_unit_facts(destination, stream, wake)
    }
    fn forget_unit_facts(&self, stream: novarocks_execution_contract::ExchangeStreamIdentity) {
        self.unit.forget_unit_facts(stream)
    }
    fn schedule_unit_retry(
        &self,
        delay: Duration,
        task: Box<dyn FnOnce() + Send + 'static>,
    ) -> Result<(), novarocks_failure_contract::FailureFact> {
        self.unit.schedule_unit_retry(delay, task)
    }
}

fn runtime_state() -> Arc<RuntimeState> {
    Arc::new(RuntimeState::for_local_execution(
        None,
        None,
        None,
        None,
        None,
        Some(crate::runtime::mem_tracker::MemTracker::new_root(
            "compiled aggregate fixture",
        )),
        Some(crate::runtime::execution_runtime::test_execution_runtime()),
    ))
}

/// Prepare and run one instance of `program`; a failure is returned as text.
pub(crate) fn try_run(
    program: &Arc<LocalProgram>,
    sink: Box<dyn OperatorFactory>,
    bindings: ExchangeBindings,
    instance: UniqueId,
) -> Result<(), String> {
    try_run_in(
        program,
        sink,
        bindings,
        ScanBindings::default(),
        instance,
        runtime_state(),
        false,
    )
}

/// Run a stream-connected instance on the same runtime and original Task as
/// its frozen receiver roster and loopback transport.
pub(crate) fn try_run_in_fixture(
    fixture: &crate::runtime::task::io::exchange::test_support::Fixture,
    program: &Arc<LocalProgram>,
    sink: Box<dyn OperatorFactory>,
    bindings: ExchangeBindings,
    instance: UniqueId,
) -> Result<(), String> {
    try_run_in(
        program,
        sink,
        bindings,
        ScanBindings::default(),
        instance,
        fixture.state(instance),
        false,
    )
}

/// The same original local execution lane with an explicit declared query
/// tracker. This is test input wiring, not a new source, control or executor.
pub(crate) fn try_run_with_tracker(
    fixture: &crate::runtime::task::io::exchange::test_support::Fixture,
    program: &Arc<LocalProgram>,
    sink: Box<dyn OperatorFactory>,
    bindings: ExchangeBindings,
    instance: UniqueId,
    tracker: Arc<crate::runtime::mem_tracker::MemTracker>,
) -> Result<(), String> {
    let original = fixture.state(instance);
    let state = Arc::new(RuntimeState::new(
        None,
        None,
        None,
        Some(instance),
        None,
        Some(tracker),
        Some(fixture.runtime.clone()),
        original.error_state().clone(),
    ));
    try_run_in(
        program,
        sink,
        bindings,
        ScanBindings::default(),
        instance,
        state,
        true,
    )
}
/// Run the original compiled pipeline with this instance's exact scan operations.
pub(crate) fn try_run_with_scans(
    fixture: &crate::runtime::task::io::exchange::test_support::Fixture,
    program: &Arc<LocalProgram>,
    sink: Box<dyn OperatorFactory>,
    bindings: ExchangeBindings,
    scans: ScanBindings,
    instance: UniqueId,
) -> Result<(), String> {
    try_run_in(
        program,
        sink,
        bindings,
        scans,
        instance,
        fixture.state(instance),
        true,
    )
}

fn try_run_in(
    program: &Arc<LocalProgram>,
    sink: Box<dyn OperatorFactory>,
    bindings: ExchangeBindings,
    scans: ScanBindings,
    instance: UniqueId,
    state: Arc<RuntimeState>,
    tracked: bool,
) -> Result<(), String> {
    let dop = i32::try_from(program.graph().profile().pipeline_dop().get()).unwrap();
    let running = prepare_compiled_program_pipeline_execution_with_profiler(
        Arc::clone(program),
        Duration::from_millis(10),
        sink,
        bindings,
        scans,
        crate::runtime::task::CompiledWriterBindings::default(),
        Some((instance.high(), instance.low())),
        None,
        dop,
        state,
        Arc::new(NoopTaskEventSink),
    )
    .map_err(|error| error.to_string())?
    .start();
    running.join().map_err(|error| error.to_string())?;
    if tracked {
        assert!(
            running.stopped_fact().is_some(),
            "successful join proves actual driver exit"
        );
    }
    Ok(())
}

/// The single receiver of `program`, registered for `instance` with
/// `senders` expected senders.
pub(crate) fn register(
    fixture: &crate::runtime::task::io::exchange::test_support::Fixture,
    program: &LocalProgram,
    instance: UniqueId,
    sources: &[UniqueId],
    port: &Arc<dyn ExchangeReceiverPort>,
) -> ExchangeBindings {
    let (_, input) = program
        .exchange_inputs()
        .iter()
        .next()
        .expect("one compiled exchange input");
    let receiver = FragmentNodeId::new(i32::try_from(input.receiver_node).unwrap());
    let CompiledExchangeReceivers {
        registrations,
        bindings,
    } = materialize_compiled_exchange_receivers(
        program,
        instance,
        &ExchangeInputAssignments::new(BTreeMap::from([(
            receiver,
            ExchangeInputAssignment::new(NonZeroUsize::new(sources.len()).unwrap()),
        )])),
        Arc::clone(port),
        Some(&fixture.creation(instance)),
    )
    .expect("compiled receivers");
    for registration in registrations {
        let key = registration.key;
        port.register(registration).expect("register receiver");
        fixture.bind(key, sources);
    }
    bindings
}

pub(crate) fn destination(
    instance: UniqueId,
    sender: UniqueId,
    ordinal: u32,
    count: u32,
) -> TaskDestination {
    TaskDestination::new(
        instance,
        RuntimeEndpoint::new("127.0.0.1", 9030).expect("endpoint"),
        sender,
        ordinal,
        count,
    )
    .expect("destination")
}

pub(crate) fn stream_sink(
    fixture: &crate::runtime::task::io::exchange::test_support::Fixture,
    program: &Arc<LocalProgram>,
    instance: UniqueId,
    destinations: Vec<TaskDestination>,
    transmitter: &Arc<LoopbackTransmitter>,
) -> Box<dyn OperatorFactory> {
    let (node, recording) = PreparedUnitRecording::new(program, fixture.creation(instance));
    assert!(
        transmitter
            .recordings
            .lock()
            .unwrap()
            .insert((instance, node), Arc::new(recording))
            .is_none(),
        "each source branch has one original recording declaration"
    );
    fixture
        .sink(
            program,
            &TaskSinkAssignment::StreamDestinations {
                destinations,
                sender_id: None,
            },
            instance,
            Arc::clone(transmitter) as Arc<dyn ExchangeFrameTransmitter>,
            None,
            None,
        )
        .expect("compiled stream sink")
}

pub(crate) fn result_sink(
    program: &Arc<LocalProgram>,
    instance: UniqueId,
    output: &ResultSinkBuffer,
) -> Box<dyn OperatorFactory> {
    materialize_compiled_sink(
        program,
        &TaskSinkAssignment::None,
        instance,
        crate::runtime::task::io::exchange::discard_exchange_transmitter(),
        Some(Box::new(ResultSinkFactory::new(output.clone()))),
        None,
    )
    .expect("compiled result sink")
}
