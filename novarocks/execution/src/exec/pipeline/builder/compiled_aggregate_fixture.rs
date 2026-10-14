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
//! Every expression root here is a leaf (literal or value read), so each root
//! gets one eager use in one shared expression domain.

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
    PhysicalExpressionRoots, PhysicalNode, PhysicalPlan, PhysicalProperties, PhysicalRootUses,
    PipelineDopDomain, PlanLimits, PropertyProofProjectionLimits, RowMultiplicity,
    StaticFunctionArgument, ValueId, ValueOrigin, extract_fragment_packages,
};
use novarocks_type_contract::{
    CallProofScope, CompileCheckpoints, CompilePhase, ControlShape, DecimalOverflowPolicy,
    EvaluationDemand, EvaluationDomainId, ExpressionControlFlow, ExpressionEffectContext,
    ExpressionEffects, ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId,
    FunctionValueType, SemanticParameters,
};
use novarocks_types::UniqueId;

use super::family_fixture::{FixtureControl, constant_policy};
use crate::exec::chunk::Chunk;
use crate::exec::fragment::program::FragmentNodeId;
use crate::exec::operators::{ResultSinkFactory, ResultSinkHandle};
use crate::exec::pipeline::binding::ExchangeBindings;
use crate::exec::pipeline::executor::prepare_compiled_program_pipeline_execution;
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::endpoint::{FragmentDestination, RuntimeEndpoint};
use crate::runtime::exchange::decode_chunks;
use crate::runtime::fragment::exchange::{
    CompiledExchangeReceivers, materialize_compiled_exchange_receivers,
};
use crate::runtime::fragment::instance::{
    ExchangeInputAssignment, ExchangeInputAssignments, FragmentSinkAssignment,
};
use crate::runtime::fragment::io::{
    ExchangeFrame, ExchangeFrameTransmitter, ExchangeReceiverFrame, ExchangeReceiverKey,
    ExchangeReceiverPort, ExchangeTransmitRejection, FragmentIoError, FragmentIoErrorKind,
    FragmentIoOperation, NoopFragmentEventSink,
};
use crate::runtime::fragment::sink::materialize_compiled_sink;
use crate::runtime::runtime_state::RuntimeState;

/// The frozen decimal policy every fixture call authors explicitly.
pub(super) const POLICY: DecimalOverflowPolicy = DecimalOverflowPolicy::ReportError;
/// Relational use IDs start far above every expression use of a fixture.
const RELATIONAL_USE_BASE: u32 = 1_000_000;

/// A sealed subset of the real builtin catalogue holding exactly the named
/// aggregate owners with their installed ABIs.
pub(super) fn aggregate_catalog(owners: &[(&str, PureKernelAbi)]) -> PureEngineFunctionCatalog {
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
pub(super) fn extrema_catalog() -> PureEngineFunctionCatalog {
    aggregate_catalog(&[
        ("count", PureKernelAbi::AggregateWindowV1),
        ("min", PureKernelAbi::AggregateWindowV1),
        ("max", PureKernelAbi::AggregateWindowV1),
    ])
}

/// One resolved aggregate selection, from the real resolver.
#[derive(Clone, Debug)]
pub(super) struct Bound {
    pub function_id: FunctionId,
    pub selected: FunctionBindingSelection,
    pub semantics: novarocks_functions::FunctionSemantics,
    pub arguments: Vec<FunctionValueType>,
}

impl Bound {
    pub(super) fn result_type(&self) -> FunctionValueType {
        match &self.selected.result_type {
            FunctionResultType::Scalar(value) => value.clone(),
            other => panic!("aggregate result is scalar, got {other:?}"),
        }
    }
    pub(super) fn intermediate_type(&self) -> FunctionValueType {
        self.selected
            .aggregate
            .as_ref()
            .expect("aggregate selection")
            .intermediate_type
            .clone()
    }
    /// The value type a call in `phase` emits.
    pub(super) fn output_type(&self, phase: AggregatePhase) -> FunctionValueType {
        if phase.produces_final_result() {
            self.result_type()
        } else {
            self.intermediate_type()
        }
    }
    pub(super) fn binding(&self, phase: AggregatePhase) -> AggregateBinding {
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
            state_interpretation: None,
            state_argument_contract: aggregate.state_argument_contract,
            function,
            phase,
            logical_argument_count: u32::try_from(self.arguments.len()).unwrap(),
            intermediate_type: aggregate.intermediate_type.clone(),
            state_format: aggregate.state_format.clone(),
        }
    }
}

pub(super) fn bind(
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
    }
}

pub(super) fn kernel_phase(phase: AggregatePhase) -> AggregateKernelPhase {
    match phase {
        AggregatePhase::Single => AggregateKernelPhase::Single,
        AggregatePhase::Partial { .. } => AggregateKernelPhase::Partial,
        AggregatePhase::Intermediate { .. } => AggregateKernelPhase::Intermediate,
        AggregatePhase::Final { .. } => AggregateKernelPhase::Final,
    }
}

/// One aggregate call of a fixture node. An update phase reads its logical
/// argument values; a merge phase reads exactly one state value.
pub(super) struct CallSpec<'a> {
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
pub(super) fn add_aggregate(
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
pub(super) fn values(
    builder: &mut FragmentBuilder,
    node: NodeId,
    types: &[FunctionValueType],
    rows: &[Vec<LiteralValue>],
) -> Vec<ValueId> {
    super::family_fixture::values(builder, node, types, rows)
}

pub(super) fn dop(max: u32) -> PipelineDopDomain {
    PipelineDopDomain {
        min: 1,
        max,
        requires_power_of_two: false,
    }
}

/// A native-exchange hash scheme over `keys`.
pub(super) fn hash(keys: &[ValueId]) -> Distribution {
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
pub(super) fn receive(
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

pub(super) fn edge(
    id: EdgeId,
    from: (FragmentId, &[ValueId]),
    to: (FragmentId, NodeId, &[ValueId]),
    source: Distribution,
    destination: Distribution,
) -> Edge {
    Edge {
        id,
        kind: EdgeKind::Stream,
        source: EdgeSource {
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
pub(super) fn finish(
    builder: FragmentBuilder,
    root: NodeId,
    sink: FragmentSink,
    max_dop: u32,
) -> Fragment {
    let fragment = builder.finish_definition(root, sink, dop(max_dop)).unwrap();
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
) -> (PhysicalRootUses, FrozenFragmentCalls) {
    let roots = PhysicalExpressionRoots::try_new(fragment, &FixtureControl).unwrap();
    let expression_domain = EvaluationDomainId::new(0);
    let mut uses = Vec::new();
    let mut bindings = Vec::new();
    for (ordinal, (site, root)) in roots.sites().iter().enumerate() {
        let id = ExpressionUseId::new(u32::try_from(ordinal).unwrap());
        uses.push(ExpressionInvocation {
            context: ExpressionEffectContext {
                use_id: id,
                domain: expression_domain,
                demand: root.demand,
            },
            definition: root.expr,
            control: ControlShape::Eager,
            arguments: Box::default(),
        });
        bindings.push((*site, id));
    }
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
    let parameters = SemanticParameters::try_new([]).unwrap();
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
                    state_interpretation: None,
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
                    state_interpretation: None,
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
                    parameters: &parameters,
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
pub(super) fn packages(
    plan: &PhysicalPlan,
    catalog: &PureEngineFunctionCatalog,
) -> BTreeMap<FragmentId, FragmentPackage> {
    let mut uses = BTreeMap::new();
    let mut calls = BTreeMap::new();
    for (id, fragment) in plan.fragments() {
        let (root_uses, frozen) = freeze(fragment, catalog);
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
        &BTreeMap::new(),
        &BTreeMap::new(),
        &uses,
        &calls,
        &pruning,
        &admissions,
        &FixtureControl,
    )
    .unwrap_or_else(|error| panic!("aggregate packages extract: {error:?}"))
}

pub(super) fn try_compile(
    package: FragmentPackage,
    catalog: &PureEngineFunctionCatalog,
    pipeline_dop: usize,
    result: bool,
) -> Result<Arc<LocalProgram>, String> {
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &FixtureControl)
            .unwrap();
    let validated =
        validate_fragment_providers(Arc::new(package), &providers, &FixtureControl).unwrap();
    let options = LocalCompileOptions {
        pipeline_dop: NonZeroUsize::new(pipeline_dop).unwrap(),
        // Only a Result root has a frozen placement; a stream sink has none.
        root_sink_dop: result.then(|| NonZeroUsize::new(1).unwrap()),
        kernel_abi: KernelAbiVersion::CURRENT,
        constants: constant_policy(),
        exchange_wait: Duration::from_secs(120),
    };
    compile_fragment(validated, catalog, options, &FixtureControl)
        .map(Arc::new)
        .map_err(|error| error.to_string())
}

pub(super) fn compile(
    package: FragmentPackage,
    catalog: &PureEngineFunctionCatalog,
    pipeline_dop: usize,
    result: bool,
) -> Arc<LocalProgram> {
    try_compile(package, catalog, pipeline_dop, result)
        .unwrap_or_else(|error| panic!("aggregate fragment compiles: {error}"))
}

/// Delivers every encoded frame to the receiver port, as the native data
/// plane does after its wire hop, and counts the rows each sender sent.
pub(super) struct LoopbackTransmitter {
    port: Arc<dyn ExchangeReceiverPort>,
    rows: Mutex<BTreeMap<(UniqueId, UniqueId), usize>>,
}

impl LoopbackTransmitter {
    pub(super) fn new(port: Arc<dyn ExchangeReceiverPort>) -> Arc<Self> {
        Arc::new(Self {
            port,
            rows: Mutex::default(),
        })
    }
    /// Rows `sender` sent to `destination`.
    pub(super) fn rows(&self, sender: UniqueId, destination: UniqueId) -> usize {
        self.rows
            .lock()
            .unwrap()
            .get(&(sender, destination))
            .copied()
            .unwrap_or_default()
    }
}

impl ExchangeFrameTransmitter for LoopbackTransmitter {
    fn transmit(&self, frame: ExchangeFrame) -> Result<(), ExchangeTransmitRejection> {
        let payload = frame.payload;
        let carried = if payload.is_empty() {
            0
        } else {
            decode_chunks(&payload)
                .expect("exchange payload")
                .iter()
                .map(Chunk::len)
                .sum()
        };
        *self
            .rows
            .lock()
            .unwrap()
            .entry((
                frame.sender_fragment_instance_id,
                frame.destination_fragment_instance_id,
            ))
            .or_default() += carried;
        self.port
            .push(
                ExchangeReceiverKey {
                    fragment_instance_id: frame.destination_fragment_instance_id,
                    node_id: frame.destination_node_id,
                },
                ExchangeReceiverFrame {
                    source_fragment_instance_id: frame.sender_fragment_instance_id,
                    sender_ordinal: frame.sender_ordinal,
                    sender_count: frame.sender_count,
                    sender_id: frame.sender_id,
                    backend_number: frame.backend_number,
                    sequence: frame.sequence,
                    eos: frame.eos,
                    payload,
                },
            )
            .map_err(|error| {
                ExchangeTransmitRejection::Failed(FragmentIoError::new(
                    FragmentIoOperation::ExchangeTransmit,
                    FragmentIoErrorKind::RemoteRejected,
                    error,
                ))
            })
    }
}

fn runtime_state() -> Arc<RuntimeState> {
    Arc::new(RuntimeState::new(
        None,
        None,
        None,
        None,
        None,
        None,
        Some(crate::runtime::execution_runtime::test_execution_runtime()),
    ))
}

/// Prepare and run one instance of `program`; a failure is returned as text.
pub(super) fn try_run(
    program: &Arc<LocalProgram>,
    sink: Box<dyn OperatorFactory>,
    bindings: ExchangeBindings,
    instance: UniqueId,
) -> Result<(), String> {
    try_run_in(program, sink, bindings, instance, runtime_state(), false)
}

/// The same original local execution lane with an explicit declared query
/// tracker. This is test input wiring, not a new source, control or executor.
pub(super) fn try_run_with_tracker(
    program: &Arc<LocalProgram>,
    sink: Box<dyn OperatorFactory>,
    bindings: ExchangeBindings,
    instance: UniqueId,
    tracker: Arc<crate::runtime::mem_tracker::MemTracker>,
) -> Result<(), String> {
    let state = Arc::new(RuntimeState::new(
        None,
        None,
        None,
        Some(instance),
        None,
        Some(tracker),
        Some(crate::runtime::execution_runtime::test_execution_runtime()),
    ));
    try_run_in(program, sink, bindings, instance, state, true)
}
fn try_run_in(
    program: &Arc<LocalProgram>,
    sink: Box<dyn OperatorFactory>,
    bindings: ExchangeBindings,
    instance: UniqueId,
    state: Arc<RuntimeState>,
    tracked: bool,
) -> Result<(), String> {
    let dop = i32::try_from(program.graph().profile().pipeline_dop().get()).unwrap();
    let running = prepare_compiled_program_pipeline_execution(
        Arc::clone(program),
        Duration::from_millis(10),
        sink,
        bindings,
        Some((instance.high(), instance.low())),
        dop,
        state,
        Arc::new(NoopFragmentEventSink),
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
pub(super) fn register(
    program: &LocalProgram,
    instance: UniqueId,
    senders: usize,
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
            ExchangeInputAssignment::new(NonZeroUsize::new(senders).unwrap()),
        )])),
        Arc::clone(port),
    )
    .expect("compiled receivers");
    for registration in registrations {
        port.register(registration).expect("register receiver");
    }
    bindings
}

pub(super) fn destination(
    instance: UniqueId,
    sender: UniqueId,
    ordinal: u32,
    count: u32,
) -> FragmentDestination {
    FragmentDestination::new(
        instance,
        RuntimeEndpoint::new("127.0.0.1", 9030).expect("endpoint"),
        sender,
        ordinal,
        count,
    )
    .expect("destination")
}

pub(super) fn stream_sink(
    program: &Arc<LocalProgram>,
    instance: UniqueId,
    destinations: Vec<FragmentDestination>,
    transmitter: &Arc<LoopbackTransmitter>,
) -> Box<dyn OperatorFactory> {
    materialize_compiled_sink(
        program,
        &FragmentSinkAssignment::StreamDestinations {
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

pub(super) fn result_sink(
    program: &Arc<LocalProgram>,
    instance: UniqueId,
    output: &ResultSinkHandle,
) -> Box<dyn OperatorFactory> {
    materialize_compiled_sink(
        program,
        &FragmentSinkAssignment::None,
        instance,
        crate::runtime::fragment::io::exchange::discard_exchange_transmitter(),
        Some(Box::new(ResultSinkFactory::new(output.clone()))),
        None,
    )
    .expect("compiled result sink")
}
