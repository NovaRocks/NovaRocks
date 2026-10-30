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

//! Physical package lowering into the mandatory checked local owner.

use crate::project_metadata::{
    DirectProjectMetadata, ProjectMetadataMode, ProjectMetadataOutput, ProjectMetadataScope,
    project_output_requests,
};
use crate::{
    ProviderValidatedFragment,
    aggregate::lower_aggregate_with_creation,
    assert_rows::lower_assert_rows,
    change_events::lower_change_events_with_creation,
    channels::{ChannelLoweringError, resolve_tree_channels_with_creation},
    exchange::lower_exchange_source_with_creation,
    expressions::{
        ExpressionLoweringError, lower_expressions_with_unions,
        lower_expressions_with_unions_with_creator, prepare_calls_with_aggregates,
    },
    repeat::{RepeatLoweringError, lower_repeat_with_creation},
    scan::{admit_scan, lower_scan_with_creation},
    sort::lower_sort,
    stream_sink::{lower_router_sink_with_creation, lower_stream_sink_with_creation},
    topn::lower_topn,
    unpivot::{UnpivotLoweringError, UnpivotLoweringInput, lower_unpivot_with_creation},
    values::{lower_values_with_creation, retired_values_uses},
};
use novarocks_functions::{ConstantPolicy, PureEngineFunctionCatalog};
use novarocks_local_program::*;
use novarocks_physical_plan::{
    Distribution, EdgeKind, ExpressionRootRole, FragmentSink, JoinSide, NodeId, NodeKind,
    RowMultiplicity,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};

/// Host-admitted values are explicit; the compiler authors the actual layout
/// digest. Neither topology nor resource defaults are inferred from the plan.
#[derive(Clone, Copy, Debug)]
pub struct LocalCompileOptions {
    pub pipeline_dop: NonZeroUsize,
    pub root_sink_dop: Option<NonZeroUsize>,
    pub kernel_abi: KernelAbiVersion,
    pub constants: ConstantPolicy,
    /// Host-admitted receive wait copied into every compiled ExchangeSource.
    /// The compiler never defaults it or derives it from the plan.
    pub exchange_wait: Duration,
}

#[derive(Debug)]
pub enum FragmentCompileError {
    ArrayProjection(novarocks_functions::array_projection::ArrayProjectionFailure),
    ProjectMetadataRequest(novarocks_type_contract::MetadataRequestError),
    ProjectMetadataHost {
        error: novarocks_failure_contract::SharedFailureCause,
    },
    Control(CompileControlError),
    Unsupported {
        node: Option<NodeId>,
        feature: &'static str,
    },
    Invalid(&'static str),
    Owner {
        phase: &'static str,
        error: novarocks_failure_contract::SharedFailureCause,
    },
}
impl FragmentCompileError {
    pub(crate) fn is_primary_construction(&self) -> bool {
        matches!(self, Self::Owner { error, .. }
            if error.downcast_ref::<ExpressionLoweringError>()
                .is_some_and(ExpressionLoweringError::is_primary_construction))
    }

    /// Capability admission is a typed fact, independent of diagnostic text.
    pub fn is_unsupported(&self) -> bool {
        match self {
            Self::Unsupported { .. } => true,
            Self::Owner { error, .. } => {
                error
                    .downcast_ref::<ExpressionLoweringError>()
                    .is_some_and(ExpressionLoweringError::is_unsupported)
                    || error
                        .downcast_ref::<LocalProgramCompileError>()
                        .is_some_and(|error| {
                            matches!(
                                error,
                                LocalProgramCompileError::Primitive(
                                    novarocks_local_program::ProgramPrimitiveError::Cast(
                                        novarocks_functions::CastPrepareError::Unsupported
                                    ) | novarocks_local_program::ProgramPrimitiveError::Arithmetic(
                                        novarocks_functions::ArithmeticPrepareError::Unsupported
                                    ) | novarocks_local_program::ProgramPrimitiveError::Comparison(
                                        novarocks_functions::ComparisonPrepareError::Unsupported
                                    )
                                )
                            )
                        })
            }
            _ => false,
        }
    }
}
impl fmt::Display for FragmentCompileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ProjectMetadataRequest(error) => write!(f, "Project metadata request: {error}"),
            Self::ProjectMetadataHost { error } => write!(f, "Project metadata host: {error}"),
            Self::Control(error) => error.fmt(f),
            Self::ArrayProjection(error) => error.fmt(f),
            Self::Unsupported { node, feature } => {
                write!(f, "unsupported local lowering at {node:?}: {feature}")
            }
            Self::Invalid(message) => write!(f, "invalid local lowering: {message}"),
            Self::Owner { phase, error } => write!(f, "local lowering {phase}: {error}"),
        }
    }
}
impl Error for FragmentCompileError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ProjectMetadataRequest(error) => Some(error),
            Self::ProjectMetadataHost { error } => Some(error),
            Self::Control(error) => Some(error),
            Self::ArrayProjection(error) => Some(error),
            Self::Owner { error, .. } => Some(error),
            _ => None,
        }
    }
}
impl From<novarocks_functions::array_projection::ArrayProjectionFailure> for FragmentCompileError {
    fn from(source: novarocks_functions::array_projection::ArrayProjectionFailure) -> Self {
        Self::ArrayProjection(source)
    }
}
impl From<CompileControlError> for FragmentCompileError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<novarocks_type_contract::ValueTypeError> for FragmentCompileError {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        Self::Owner {
            phase: "value type",
            error: novarocks_failure_contract::SharedFailureCause::new(error),
        }
    }
}
impl From<novarocks_functions::ConstantError> for FragmentCompileError {
    fn from(error: novarocks_functions::ConstantError) -> Self {
        match error {
            novarocks_functions::ConstantError::Control(cause) => Self::Control(cause),
            original => Self::Owner {
                phase: "constant",
                error: novarocks_failure_contract::SharedFailureCause::new(original),
            },
        }
    }
}
impl From<novarocks_physical_plan::ConstantReferenceError> for FragmentCompileError {
    fn from(error: novarocks_physical_plan::ConstantReferenceError) -> Self {
        match error {
            novarocks_physical_plan::ConstantReferenceError::Control(cause) => Self::Control(cause),
            original => Self::Owner {
                phase: "constant reference",
                error: novarocks_failure_contract::SharedFailureCause::new(original),
            },
        }
    }
}
macro_rules! owner_error {
    ($ty:ident, $phase:literal) => {
        impl From<$ty> for FragmentCompileError {
            fn from(error: $ty) -> Self {
                match error {
                    $ty::Control(error) => Self::Control(error),
                    error => Self::Owner {
                        phase: $phase,
                        error: novarocks_failure_contract::SharedFailureCause::new(error),
                    },
                }
            }
        }
    };
}
impl From<ChannelLoweringError> for FragmentCompileError {
    fn from(error: ChannelLoweringError) -> Self {
        match error {
            ChannelLoweringError::Control(cause) => Self::Control(cause),
            ChannelLoweringError::Invalid(message) => Self::Invalid(message),
            ChannelLoweringError::Join(error) => error,
            error => Self::Owner {
                phase: "input channels",
                error: novarocks_failure_contract::SharedFailureCause::new(error),
            },
        }
    }
}
owner_error!(ExpressionLoweringError, "expressions");
impl From<RepeatLoweringError> for FragmentCompileError {
    fn from(error: RepeatLoweringError) -> Self {
        match error {
            RepeatLoweringError::Control(cause) => Self::Control(cause),
            RepeatLoweringError::Invalid(message) => Self::Invalid(message),
            error => Self::Owner {
                phase: "Repeat",
                error: novarocks_failure_contract::SharedFailureCause::new(error),
            },
        }
    }
}
impl From<UnpivotLoweringError> for FragmentCompileError {
    fn from(error: UnpivotLoweringError) -> Self {
        match error {
            UnpivotLoweringError::Control(c) => Self::Control(c),
            UnpivotLoweringError::Invalid(m) => Self::Invalid(m),
            e => Self::Owner {
                phase: "Unpivot",
                error: novarocks_failure_contract::SharedFailureCause::new(e),
            },
        }
    }
}
owner_error!(LayoutCompileError, "layout");
owner_error!(ValuesCompileError, "values");
owner_error!(BindingRequirementsCompileError, "requirements");
owner_error!(ProgramCompileError, "graph");
owner_error!(ProgramControlFlowError, "control flow");
impl From<ProgramRootBindingError> for FragmentCompileError {
    fn from(error: ProgramRootBindingError) -> Self {
        match error {
            ProgramRootBindingError::Control(cause)
            | ProgramRootBindingError::Roots(ProgramExpressionRootError::Control(cause)) => {
                Self::Control(cause)
            }
            error => Self::Owner {
                phase: "root correspondence",
                error: novarocks_failure_contract::SharedFailureCause::new(error),
            },
        }
    }
}
impl From<RootAttachmentError> for FragmentCompileError {
    fn from(error: RootAttachmentError) -> Self {
        match error {
            RootAttachmentError::Control(cause) => Self::Control(cause),
            RootAttachmentError::Roots(error) => error.into(),
            RootAttachmentError::Calls(error) => error.into(),
            RootAttachmentError::Expressions(error) => error.into(),
            RootAttachmentError::Channels(error) => error.into(),
        }
    }
}
owner_error!(ProgramResolvedCallsError, "resolved calls");
owner_error!(ProgramExpressionTypeError, "definition types");
owner_error!(ProgramChannelTypeError, "channel types");
owner_error!(ProgramLexicalBindingError, "lexical bindings");
owner_error!(LocalProgramCompileError, "final owner");

/// This entry consumes the provider intermediate rather than retaining a
/// second package beside the resulting graph. Unsupported families remain
/// explicit while the compiler is integrated; they cannot enter legacy decode.
pub fn compile_fragment_with_creation<
    H,
    F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
>(
    input: ProviderValidatedFragment,
    functions: &PureEngineFunctionCatalog,
    options: LocalCompileOptions,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    temporary: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
    factory: &F,
    control: &dyn PureCompileControl,
) -> Result<LocalProgram, crate::FragmentCreationError<H>> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = lower_with_creation(
        input,
        functions,
        options,
        &mut work,
        creation,
        Some(temporary),
        Some(factory),
        &mut ProjectMetadataMode::<DirectProjectMetadata>::Direct,
    );
    crate::creation::finish_attempt(result, work)
}

pub fn compile_fragment_with_creation_and_project_metadata_host<
    H,
    F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
    P: ProjectMetadataScope,
>(
    input: ProviderValidatedFragment,
    functions: &PureEngineFunctionCatalog,
    options: LocalCompileOptions,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    temporary: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
    factory: &F,
    control: &dyn PureCompileControl,
    host: &mut P,
) -> Result<LocalProgram, crate::FragmentCreationError<H>> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = lower_with_creation(
        input,
        functions,
        options,
        &mut work,
        creation,
        Some(temporary),
        Some(factory),
        &mut ProjectMetadataMode::Funded(host),
    );
    crate::creation::finish_attempt(result, work)
}

fn lower_with_creation<
    H,
    F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
    P: ProjectMetadataScope,
>(
    input: ProviderValidatedFragment,
    functions: &PureEngineFunctionCatalog,
    options: LocalCompileOptions,
    work: &mut CompileCheckpoints<'_>,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    mut temporary: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
    factory: Option<&F>,
    metadata: &mut ProjectMetadataMode<'_, P>,
) -> Result<LocalProgram, crate::FragmentCreationError<H>> {
    // Each validated provider recipe moves into its one lowered owner.
    let (package, mut reads, mut writes) = input.into_parts();
    let package = &package;
    let physical = package.fragment();
    let dop = u32::try_from(options.pipeline_dop.get())
        .map_err(|_| FragmentCompileError::Invalid("DOP exceeds physical domain"))?;
    let domain = physical.dop_domain();
    work.step()?;
    if dop < domain.min
        || dop > domain.max
        || (domain.requires_power_of_two && !dop.is_power_of_two())
    {
        return Err((FragmentCompileError::Invalid("DOP is outside physical domain")).into());
    }
    if options.root_sink_dop.is_some_and(|dop| dop.get() != 1) {
        return Err((FragmentCompileError::Unsupported {
            node: None,
            feature: "result sink width for singleton source",
        })
        .into());
    }
    let outbound = &package.cuts().outbound;
    let is_router = matches!(physical.sink(), FragmentSink::Router { .. });
    // A Result sink publishes the result port; a Stream sink publishes its
    // one exact outbound cut. Every other sink family remains explicit.
    let stream_cut = match physical.sink() {
        FragmentSink::Result | FragmentSink::RootResult(_) => {
            if !outbound.is_empty() {
                return Err((FragmentCompileError::Invalid(
                    "result sink has an outbound exchange cut",
                ))
                .into());
            }
            None
        }
        FragmentSink::Stream { edge } => {
            let [cut] = &outbound[..] else {
                return Err((FragmentCompileError::Invalid(
                    "stream sink requires exactly one outbound cut",
                ))
                .into());
            };
            if cut.edge != *edge {
                return Err((FragmentCompileError::Invalid(
                    "stream sink edge differs from its outbound cut",
                ))
                .into());
            }
            if cut.kind != EdgeKind::Stream {
                return Err((FragmentCompileError::Unsupported {
                    node: None,
                    feature: "outbound CTE or change-stream edge",
                })
                .into());
            }
            // A writer relation streams only from its root writer, and a root
            // writer streams exactly its relation.
            let writer_rooted = physical
                .nodes()
                .get(&physical.root())
                .is_some_and(|root| matches!(root.kind, NodeKind::TableWriter { .. }));
            if cut.writer_result().is_some() || writer_rooted {
                crate::writer::admit_writer_result_cut(package, cut, work)?;
            }
            if matches!(cut.partitioning.source, Distribution::Unconstrained) {
                return Err((FragmentCompileError::Unsupported {
                    node: None,
                    feature: "unconstrained stream partitioning",
                })
                .into());
            }
            Some(cut)
        }
        FragmentSink::Router { .. } => None,
        FragmentSink::Multicast { .. } | FragmentSink::Noop => {
            return Err((FragmentCompileError::Unsupported {
                node: None,
                feature: "multicast or noop sink",
            })
            .into());
        }
    };
    for cut in package.cuts().inbound.iter() {
        work.step()?;
        if cut.kind != EdgeKind::Stream && cut.kind != EdgeKind::ChangeStreamRouter {
            return Err((FragmentCompileError::Unsupported {
                node: Some(cut.destination_node),
                feature: "inbound CTE edge",
            })
            .into());
        }
    }
    // Provider reads and writes are admitted per scan and writer below; each
    // local runtime-filter endpoint is admitted here and later lowered by its
    // own scan or join.
    let mut runtime_filters = crate::runtime_filter::plan_runtime_filters(
        package,
        &reads,
        factory,
        crate::creation::borrow_scope(&mut temporary),
        work,
    )?;
    // The result port exists exactly for a Result sink; a stream producer
    // has no result labels and never borrows another fragment's port.
    let result = package.result();
    if !is_router && stream_cut.is_none() && result.is_none() {
        return Err((FragmentCompileError::Invalid("missing result port")).into());
    }
    if (is_router || stream_cut.is_some()) && result.is_some() {
        return Err((FragmentCompileError::Invalid("stream sink carries a result port")).into());
    }
    // Borrowed input order determines the bounded postorder. Each physical
    // node has one execution owner; shared subgraphs remain unsupported.
    let mut order = Vec::new();
    let mut visited = BTreeSet::new();
    let mut scans = BTreeSet::new();
    let mut writers = BTreeSet::new();
    let mut stack = Vec::new();
    let mut expanded_nodes = physical.nodes().len();
    let mut derived_definitions = 0usize;
    let mut channel_count = 0usize;
    for node in physical.nodes().values() {
        let pieces = if matches!(
            node.kind,
            NodeKind::SetOp {
                kind: novarocks_physical_plan::SetOperationKind::UnionAll,
                ..
            }
        ) {
            node.inputs
                .len()
                .checked_add(1)
                .ok_or(CompileControlError::ResourceExhausted)?
        } else {
            1
        };
        channel_count = channel_count
            .checked_add(
                pieces
                    .checked_mul(node.output.columns.len())
                    .ok_or(CompileControlError::ResourceExhausted)?,
            )
            .ok_or(CompileControlError::ResourceExhausted)?;
        // A writer-family node also owns its non-output relation roles.
        channel_count = crate::writer::extra_channels(package, node)
            .and_then(|extra| channel_count.checked_add(extra))
            .ok_or(CompileControlError::ResourceExhausted)?;
        // A join also owns its side and scope roles, and may publish its
        // output through one selection Project of derived slot reads.
        if let Some((pieces, channels, definitions)) = crate::join::resource_bound(physical, node)?
        {
            expanded_nodes = expanded_nodes
                .checked_add(pieces)
                .ok_or(CompileControlError::ResourceExhausted)?;
            channel_count = channel_count
                .checked_add(channels)
                .ok_or(CompileControlError::ResourceExhausted)?;
            derived_definitions = derived_definitions
                .checked_add(definitions)
                .ok_or(CompileControlError::ResourceExhausted)?;
        }
        if let Some((pieces, channels)) = crate::generate_series::resource_bound(node) {
            expanded_nodes = expanded_nodes
                .checked_add(pieces)
                .ok_or(CompileControlError::ResourceExhausted)?;
            channel_count = channel_count
                .checked_add(channels)
                .ok_or(CompileControlError::ResourceExhausted)?;
        }
        // A table function also owns its argument Project, that Project's
        // channels and pass-through reads, and its relation result channels.
        if let Some((pieces, channels, definitions)) = crate::table_function::resource_bound(node)?
        {
            expanded_nodes = expanded_nodes
                .checked_add(pieces)
                .ok_or(CompileControlError::ResourceExhausted)?;
            channel_count = channel_count
                .checked_add(channels)
                .ok_or(CompileControlError::ResourceExhausted)?;
            derived_definitions = derived_definitions
                .checked_add(definitions)
                .ok_or(CompileControlError::ResourceExhausted)?;
        }
        if matches!(
            node.kind,
            NodeKind::SetOp {
                kind: novarocks_physical_plan::SetOperationKind::UnionAll,
                ..
            }
        ) {
            expanded_nodes = expanded_nodes
                .checked_add(node.inputs.len())
                .ok_or(CompileControlError::ResourceExhausted)?;
            derived_definitions = derived_definitions
                .checked_add(
                    node.inputs
                        .len()
                        .checked_mul(node.output.columns.len())
                        .ok_or(CompileControlError::ResourceExhausted)?,
                )
                .ok_or(CompileControlError::ResourceExhausted)?;
        }
        work.step()?;
    }
    if let FragmentSink::Router { routes, .. } = physical.sink() {
        let root_width = physical
            .nodes()
            .get(&physical.root())
            .ok_or(FragmentCompileError::Invalid("missing router root"))?
            .output
            .columns
            .len();
        let width = routes
            .iter()
            .try_fold(root_width, |width, route| {
                width.checked_add(route.input_mapping.len())
            })
            .ok_or(CompileControlError::ResourceExhausted)?;
        expanded_nodes = expanded_nodes
            .checked_add(1)
            .ok_or(CompileControlError::ResourceExhausted)?;
        derived_definitions = derived_definitions
            .checked_add(width)
            .ok_or(CompileControlError::ResourceExhausted)?;
        channel_count = channel_count
            .checked_add(width)
            .ok_or(CompileControlError::ResourceExhausted)?;
    }
    // Each scan runtime-filter consumer key is one derived slot read.
    derived_definitions = derived_definitions
        .checked_add(runtime_filters.consumer_count()?)
        .ok_or(CompileControlError::ResourceExhausted)?;
    let derived_roots = derived_definitions
        .checked_add(runtime_filters.join_consumer_count()?)
        .ok_or(CompileControlError::ResourceExhausted)?;
    let original_flow = package.expression_uses().flow();
    let mut references = original_flow
        .uses()
        .len()
        .checked_add(derived_roots)
        .ok_or(CompileControlError::ResourceExhausted)?;
    for invocation in original_flow.uses().values() {
        references = references
            .checked_add(invocation.arguments.len())
            .ok_or(CompileControlError::ResourceExhausted)?;
        work.step()?;
    }
    if channel_count > MAX_PROGRAM_TYPED_CHANNELS
        || expanded_nodes
            .checked_mul(2)
            .is_none_or(|n| n > MAX_PROFILE_REFERENCES)
        || expanded_nodes > MAX_PROGRAM_NODES
        || expanded_nodes > MAX_PROGRAM_EXPANDED_OCCURRENCES
        || physical
            .expressions()
            .len()
            .checked_add(derived_definitions)
            .is_none_or(|n| n > novarocks_type_contract::MAX_CONTROL_DEFINITIONS)
        || original_flow
            .domains()
            .len()
            .checked_add(derived_roots)
            .is_none_or(|n| n > novarocks_type_contract::MAX_CONTROL_DEFINITIONS)
        || original_flow
            .uses()
            .len()
            .checked_add(derived_roots)
            .is_none_or(|n| n > novarocks_type_contract::MAX_CONTROL_DEFINITIONS)
        || references > novarocks_type_contract::MAX_CONTROL_USE_REFERENCES
    {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    std::alloc::Layout::array::<(NodeId, bool, usize)>(expanded_nodes)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    work.flush()?;
    stack
        .try_reserve_exact(expanded_nodes)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    order
        .try_reserve_exact(physical.nodes().len())
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    // Only a join's own build receiver may arrive replicated.
    let replicated_builds = crate::join::replicated_build_inputs(physical, work)?;
    stack.push((physical.root(), false, 1usize));
    while let Some((id, exiting, depth)) = stack.pop() {
        work.step()?;
        if exiting {
            order.push(id);
            continue;
        }
        if depth > MAX_PROGRAM_NODE_DEPTH {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        if !visited.insert(id) {
            return Err((FragmentCompileError::Unsupported {
                node: Some(id),
                feature: "shared or cyclic physical input",
            })
            .into());
        }
        let node = physical
            .nodes()
            .get(&id)
            .ok_or(FragmentCompileError::Invalid("missing physical node"))?;
        // A partitioned single-copy layout only says how rows are placed
        // across instances. Families with a whole-relation meaning (global
        // Sort, Single/Final TopN, Limit, global row-count assertion) reach
        // here only over the Singleton input the checked physical contract
        // requires, and a per-key assertion only over a key-colocated one.
        // An analytic Sort and its Window consume instance-level partition
        // co-location: each gathers its instance input to one driver. A
        // Partial row-count TopN prunes each instance's own rows wherever they
        // are placed; its gather-and-Final sequence is a checked plan fact. A
        // family that consumes per-driver key co-location must author its own
        // local partitioning instead of relying on this. Copied rows and
        // broadcast placement stay refused, except the broadcast receiver a
        // join consumes as its complete build side.
        if !replicated_builds.contains(&id)
            && (!matches!(
                node.output_properties.distribution,
                Distribution::Singleton
                    | Distribution::Unconstrained
                    | Distribution::RoundRobin
                    | Distribution::Hash { .. }
                    | Distribution::ConditionalKey { .. }
                    | Distribution::BucketShuffle { .. }
            ) || node.output_properties.row_multiplicity != RowMultiplicity::SingleCopy)
        {
            return Err((FragmentCompileError::Unsupported {
                node: Some(id),
                feature: "replicated or broadcast source-tree properties",
            })
            .into());
        }
        let union = matches!(
            node.kind,
            NodeKind::SetOp {
                kind: novarocks_physical_plan::SetOperationKind::UnionAll,
                ..
            }
        );
        let supported = match &node.kind {
            NodeKind::GenerateSeries { .. } => {
                crate::generate_series::admit(package, node, work)?;
                if depth
                    .checked_add(1)
                    .is_none_or(|depth| depth > MAX_PROGRAM_NODE_DEPTH)
                {
                    return Err(CompileControlError::ResourceExhausted.into());
                }
                true
            }
            NodeKind::Values { .. } | NodeKind::ExchangeSource { .. } | NodeKind::Scan { .. } => {
                node.inputs.is_empty()
            }
            NodeKind::Project { .. }
            | NodeKind::Limit { .. }
            | NodeKind::AssertOneRow(_)
            | NodeKind::Repeat { .. }
            | NodeKind::Unpivot { .. }
            | NodeKind::ChangeEventExpand { .. }
            | NodeKind::Aggregate { .. } => node.inputs.len() == 1,
            NodeKind::Filter { predicates } => !predicates.is_empty() && node.inputs.len() == 1,
            // Every row-count TopN phase; a grouped-state reduction has no
            // local owner.
            NodeKind::Sort {
                mode:
                    novarocks_physical_plan::SortMode::Global
                    | novarocks_physical_plan::SortMode::Analytic { .. },
                ..
            }
            | NodeKind::TopN {
                reduction: novarocks_physical_plan::TopNReduction::Rows,
                ..
            } => node.inputs.len() == 1,
            NodeKind::TopN {
                reduction: novarocks_physical_plan::TopNReduction::GroupedStates { .. },
                phase: novarocks_physical_plan::TopNPhase::Partial { .. },
                offset: 0,
                ..
            } => node.inputs.len() == 1,
            // A window, under its own admission: one input and an installed
            // pure window kernel for each call shape.
            NodeKind::Window(_) => {
                crate::window::admit_window(package, node, functions, work)?;
                true
            }
            NodeKind::SetOp {
                kind: novarocks_physical_plan::SetOperationKind::UnionAll,
                ..
            } => node.inputs.len() >= 2,
            // The writer family, under its own admission below.
            NodeKind::TableWriter { .. } => node.inputs.len() == 1,
            NodeKind::TableFinish(spec) => {
                node.inputs.len()
                    == if spec.count_mode
                        == novarocks_physical_plan::FinishCountMode::LogicalChanges
                    {
                        2
                    } else {
                        1
                    }
            }
            // Both join families, under their own admission below.
            NodeKind::HashJoin { .. } | NodeKind::NestLoopJoin { .. } => node.inputs.len() == 2,
            // A table function, under its own admission: exactly one outer
            // input and an installed pure TableV1 kernel.
            NodeKind::TableFunction { .. } => {
                crate::table_function::admit_table_function(node, functions, work)?;
                true
            }
            _ => false,
        };
        if !supported {
            return Err((FragmentCompileError::Unsupported {
                node: Some(id),
                feature: unsupported_node_shape(&node.kind),
            })
            .into());
        }
        if matches!(node.kind, NodeKind::Scan { .. }) {
            admit_scan(node, reads.get(&id), work)?;
            scans.insert(id);
        }
        if matches!(
            node.kind,
            NodeKind::TableWriter { .. } | NodeKind::TableFinish(_)
        ) {
            crate::writer::admit_writer_family(
                package,
                node,
                writes.get(&id),
                options.pipeline_dop,
                work,
            )?;
            if matches!(node.kind, NodeKind::TableWriter { .. }) {
                writers.insert(id);
            }
        }
        // A join may lower to itself and a following selection Project.
        let join = matches!(
            node.kind,
            NodeKind::HashJoin { .. } | NodeKind::NestLoopJoin { .. }
        );
        if join {
            crate::join::orient_join(node)?;
        }
        stack.push((id, true, depth));
        for &child in node.inputs.iter().rev() {
            // A join's selection, a union's normalizer and a table function's
            // argument Project each add one local node above the child.
            let derived = union || join || matches!(node.kind, NodeKind::TableFunction { .. });
            stack.push((child, false, depth + if derived { 2 } else { 1 }));
            work.step()?;
        }
    }
    order.reverse();
    if order.len() != physical.nodes().len() {
        return Err((FragmentCompileError::Invalid("unrepresented physical nodes")).into());
    }
    // Each admitted scan found its recipe; a recipe for any other node is
    // never silently left behind.
    work.step()?;
    if reads.len() != scans.len() {
        return Err((FragmentCompileError::Invalid(
            "provider read recipe names no admitted scan node",
        ))
        .into());
    }
    if writes.len() != writers.len() || writes.keys().any(|node| !writers.contains(node)) {
        return Err((FragmentCompileError::Invalid(
            "provider write recipe names no admitted table writer",
        ))
        .into());
    }
    // Expansion conservatively loses distribution knowledge. It still has
    // the exact input placement at every admitted driver width; descendants of this
    // actual expansion can consume that uncertainty. A runtime-split scan is
    // an unconstrained source in its own right: its rows land on any instance
    // and driver, and only its transparent Project/Filter/Limit descendants
    // and a Partial row-count TopN inherit that placement. The partial keeps
    // a subset of each instance's rows where they already are; its Final
    // reads them only through a checked gather. A Partial-grouping aggregate
    // is such a source too: it merges only the rows each driver already holds,
    // so its output is placed wherever its input was, and the physical
    // contract lets only an exchange-fed later phase complete its groups.
    let mut properties = BTreeMap::<NodeId, (bool, bool, bool)>::new();
    for &id in order.iter().rev() {
        let node = &physical.nodes()[&id];
        let mut expanded = false;
        for child in &node.inputs {
            expanded |= properties.get(child).is_some_and(|p| p.0);
            work.step()?;
        }
        let sorted = node.inputs.len() == 1 && properties.get(&node.inputs[0]).is_some_and(|p| p.1);
        let changes = matches!(node.kind, NodeKind::ChangeEventExpand { .. });
        // A table function keeps each outer row on its driver and emits its
        // rows in outer order; the property law keeps exactly the input
        // ordering prefix its pass-through values carry.
        let transparent = matches!(
            node.kind,
            NodeKind::Project { .. }
                | NodeKind::Filter { .. }
                | NodeKind::Limit { .. }
                | NodeKind::TableFunction { .. }
        );
        let partial_rows = matches!(
            node.kind,
            NodeKind::TopN {
                phase: novarocks_physical_plan::TopNPhase::Partial { .. },
                ..
            }
        );
        let partial_groups = matches!(
            node.kind,
            NodeKind::Aggregate {
                grouping: novarocks_physical_plan::AggregateGrouping::Partial,
                ..
            }
        );
        // A join's rows are placed where its probe rows are: every instance
        // holds the whole build it needs, so a join probing a runtime-split
        // scan inherits that scan's placement.
        let probe_input = match &node.kind {
            NodeKind::HashJoin { build_side, .. } if node.inputs.len() == 2 => {
                Some(node.inputs[usize::from(*build_side == JoinSide::Left)])
            }
            NodeKind::NestLoopJoin { .. } if node.inputs.len() == 2 => Some(node.inputs[0]),
            _ => None,
        };
        // A UnionAll concatenates the rows each driver already holds, so it is
        // placed wherever all of its inputs are.
        let union_placed = matches!(
            node.kind,
            NodeKind::SetOp {
                kind: novarocks_physical_plan::SetOperationKind::UnionAll,
                ..
            }
        ) && !node.inputs.is_empty()
            && node
                .inputs
                .iter()
                .all(|input| properties.get(input).is_some_and(|p| p.2));
        // Projection may drop a checked hash key and therefore publish
        // Unconstrained distribution. Its rows still run in the exact input
        // instances; losing the key does not create a new placement source.
        let projected_placement = matches!(node.kind, NodeKind::Project { .. })
            && node.inputs.len() == 1
            && physical.nodes().get(&node.inputs[0]).is_some_and(|input| {
                input.output_properties.row_multiplicity
                    == novarocks_physical_plan::RowMultiplicity::SingleCopy
                    && matches!(
                        input.output_properties.distribution,
                        Distribution::Hash { .. }
                            | Distribution::ConditionalKey { .. }
                            | Distribution::BucketShuffle { .. }
                    )
            });
        // Repeat emits its frozen grouping sets on the exact input instances.
        // Its property author may drop partition keys after nullifying them,
        // but that does not create a new placement or ordering source.
        let repeat_placed = matches!(node.kind, NodeKind::Repeat { .. })
            && node.inputs.len() == 1
            && physical.nodes().get(&node.inputs[0]).is_some_and(|input| {
                input.output_properties.row_multiplicity
                    == novarocks_physical_plan::RowMultiplicity::SingleCopy
                    && (properties.get(&input.id).is_some_and(|p| p.2)
                        || matches!(
                            input.output_properties.distribution,
                            Distribution::Singleton
                                | Distribution::RoundRobin
                                | Distribution::Hash { .. }
                                | Distribution::ConditionalKey { .. }
                                | Distribution::BucketShuffle { .. }
                        ))
            });
        let scan_rooted = matches!(node.kind, NodeKind::Scan { .. })
            || projected_placement
            || repeat_placed
            || partial_groups
            || union_placed
            || ((transparent || partial_rows)
                && node.inputs.len() == 1
                && properties.get(&node.inputs[0]).is_some_and(|p| p.2))
            || probe_input.is_some_and(|probe| properties.get(&probe).is_some_and(|p| p.2));
        let unknown = node.output_properties.distribution == Distribution::Unconstrained;
        // A writer's relation is per-driver summaries of what it wrote, and
        // the writer is its fragment's root: its rows have no placement a
        // descendant could consume.
        let writer_rooted = matches!(node.kind, NodeKind::TableWriter { .. });
        work.step()?;
        if unknown && !expanded && !changes && !scan_rooted && !writer_rooted {
            return Err((FragmentCompileError::Unsupported {
                node: Some(id),
                feature: "unconstrained distribution without a runtime-split scan placement",
            })
            .into());
        }
        // Every row-count TopN phase emits its instance's window as one
        // ordered stream on one driver, so its declared ordering holds for
        // the whole instance output, as the property law states it. An
        // analytic Sort does the same over its partition keys and then its
        // order keys.
        let global = matches!(
            node.kind,
            NodeKind::Sort {
                mode: novarocks_physical_plan::SortMode::Global
                    | novarocks_physical_plan::SortMode::Analytic { .. },
                ..
            } | NodeKind::TopN { .. }
        );
        // A window emits its gathered input on one driver in arrival order,
        // so it preserves exactly the ordering it reads. One that partitions
        // or orders reads that ordering from an actual sorted source.
        let window = matches!(node.kind, NodeKind::Window(_));
        if let NodeKind::Window(spec) = &node.kind
            && !(spec.partition_by.is_empty() && spec.order_by.is_empty())
            && !sorted
        {
            return Err((FragmentCompileError::Unsupported {
                node: Some(id),
                feature: "window input ordering lacks a supported sort source",
            })
            .into());
        }
        let ordered = global || (sorted && (transparent || window));
        if !(node.output_properties.ordering.is_empty() || ordered) {
            return Err((FragmentCompileError::Unsupported {
                node: Some(id),
                feature: "ordering lacks supported global-sort source",
            })
            .into());
        }
        properties.insert(id, (expanded || changes, ordered, scan_rooted));
    }
    work.flush()?;
    let channels_plan =
        resolve_tree_channels_with_creation(package, &order, work.control(), creation)?;
    let runtime_filter_keys = runtime_filters.consumer_keys(
        &channels_plan.nodes,
        crate::creation::borrow_scope(&mut temporary),
        work,
    )?;
    work.flush()?;
    let expressions = match factory {
        Some(f) => {
            let mut type_scope = f
                .begin_scope()
                .map_err(crate::FragmentCreationError::Creator)?;
            lower_expressions_with_unions_with_creator(
                package,
                options.constants,
                &channels_plan.inputs,
                &channels_plan.unions,
                &runtime_filter_keys,
                &mut type_scope,
                f,
                work.control(),
            )?
        }
        None => lower_expressions_with_unions(
            package,
            options.constants,
            &channels_plan.inputs,
            &channels_plan.unions,
            &runtime_filter_keys,
            work.control(),
        )?,
    };
    work.flush()?;
    let local_nodes = channels_plan
        .nodes
        .iter()
        .map(|(source, planned)| (*source, planned.local))
        .collect::<BTreeMap<_, _>>();
    work.flush()?;
    let tokens = match factory {
        Some(factory) => crate::expressions::prepare_calls_with_aggregates_with_creator(
            package,
            &expressions,
            functions,
            &local_nodes,
            &options.constants,
            factory,
            work.control(),
        )?,
        None => prepare_calls_with_aggregates(
            package,
            &expressions,
            functions,
            &local_nodes,
            work.control(),
        )?,
    };
    let mut nodes: Vec<ProgramNode> = Vec::new();
    let mut local_ids = BTreeMap::new();
    let mut channels = Vec::new();
    let mut operators = Vec::new();
    let mut allowed = BTreeSet::new();
    let mut union_roots = Vec::new();
    let mut filter_roots = Vec::new();
    let mut source_requirements = Vec::new();
    let mut exchange_inputs = BTreeMap::new();
    let mut scan_inputs = BTreeMap::new();
    let mut aggregates = BTreeMap::new();
    let mut program_writes = BTreeMap::new();
    let mut writer_flows = Vec::new();
    // Local nodes whose layout is a provider scan layout unchanged.
    let mut scan_layouts = BTreeSet::new();
    crate::assert_rows::reserve_vec(&mut nodes, expanded_nodes, work)?;
    crate::assert_rows::reserve_vec(&mut operators, expanded_nodes, work)?;
    crate::assert_rows::reserve_vec(&mut channels, channel_count, work)?;
    crate::assert_rows::reserve_vec(&mut union_roots, derived_definitions, work)?;
    for &source in order.iter().rev() {
        work.step()?;
        let node = &physical.nodes()[&source];
        let planned = channels_plan
            .nodes
            .get(&source)
            .ok_or(FragmentCompileError::Invalid(
                "missing planned node channels",
            ))?;
        let id = planned.local;
        // A join lowers to its join node and, when its physical output is not
        // canonical, one selection Project whose slot reads were authored
        // like a union normalizer's.
        if let Some(join) = channels_plan.joins.get(&source) {
            let input = |child: NodeId| {
                local_ids
                    .get(&child)
                    .copied()
                    .ok_or(FragmentCompileError::Invalid("missing lowered join input"))
            };
            let (probe, build) = (input(join.probe)?, input(join.build)?);
            let selection = expressions
                .union_ids
                .get(&source)
                .and_then(|rows| rows.first())
                .map(Vec::as_slice);
            let filters =
                runtime_filters.take_join(source, package, join, &expressions.ids, work)?;
            filter_roots.extend(filters.roots);
            source_requirements.extend(filters.requirements);
            work.flush()?;
            let lowered = crate::join::lower_join_with_creation(
                package,
                node,
                join,
                &planned.slots,
                crate::join::JoinInput {
                    node: probe,
                    layout: nodes[probe.index()].output_layout(),
                },
                crate::join::JoinInput {
                    node: build,
                    layout: nodes[build.index()].output_layout(),
                },
                &expressions.ids,
                selection,
                filters.producers,
                filters.consumers,
                work.control(),
                creation,
            )?;
            for emitted in lowered.nodes {
                let same = emitted
                    .local_id()
                    .is_some_and(|emitted| emitted.index() == nodes.len());
                work.step()?;
                if !same {
                    return Err((FragmentCompileError::Invalid("join schedule differs")).into());
                }
                nodes.push(emitted);
            }
            channels.extend(lowered.channels);
            operators.extend(lowered.operators);
            union_roots.extend(lowered.selection_roots);
            allowed.insert(DiagnosticSourceNodeId::new(source.get()));
            local_ids.insert(source, id);
            continue;
        }
        if let Some(series) = channels_plan.series.get(&source) {
            let lowered = crate::generate_series::lower_with_creation(
                package,
                node,
                series,
                &planned.slots,
                &expressions.ids,
                work,
                creation,
            )?;
            for emitted in lowered.nodes {
                if emitted
                    .local_id()
                    .is_none_or(|id| id.index() != nodes.len())
                {
                    return Err(
                        (FragmentCompileError::Invalid("series node schedule differs")).into(),
                    );
                }
                nodes.push(emitted);
                work.step()?;
            }
            channels.extend(lowered.channels);
            operators.extend(lowered.operators);
            allowed.insert(DiagnosticSourceNodeId::new(source.get()));
            local_ids.insert(source, id);
            continue;
        }
        // A table function lowers to its argument Project and itself.
        if let Some(table) = channels_plan.table_functions.get(&source) {
            let child = *local_ids
                .get(&node.inputs[0])
                .ok_or(FragmentCompileError::Invalid(
                    "missing lowered table function input",
                ))?;
            let reads = expressions
                .union_ids
                .get(&source)
                .and_then(|rows| rows.first())
                .ok_or(FragmentCompileError::Invalid(
                    "missing table function pass-through definitions",
                ))?;
            work.flush()?;
            let lowered = crate::table_function::lower_table_function_with_creation(
                package,
                node,
                table,
                &planned.slots,
                child,
                &expressions.ids,
                reads,
                work.control(),
                creation,
            )?;
            for emitted in lowered.nodes {
                let same = emitted
                    .local_id()
                    .is_some_and(|emitted| emitted.index() == nodes.len());
                work.step()?;
                if !same {
                    return Err(
                        (FragmentCompileError::Invalid("table function schedule differs")).into(),
                    );
                }
                nodes.push(emitted);
            }
            channels.extend(lowered.channels);
            operators.extend(lowered.operators);
            union_roots.extend(lowered.pass_through_roots);
            allowed.insert(DiagnosticSourceNodeId::new(source.get()));
            local_ids.insert(source, id);
            continue;
        }
        if matches!(
            node.kind,
            NodeKind::SetOp {
                kind: novarocks_physical_plan::SetOperationKind::UnionAll,
                ..
            }
        ) {
            let branches = channels_plan
                .unions
                .get(&source)
                .ok_or(FragmentCompileError::Invalid("missing UnionAll branches"))?;
            let branches = &branches[..node.inputs.len()];
            let definitions =
                expressions
                    .union_ids
                    .get(&source)
                    .ok_or(FragmentCompileError::Invalid(
                        "missing UnionAll definitions",
                    ))?;
            let definitions = &definitions[..node.inputs.len()];
            work.flush()?;
            let emitted = crate::union::lower_union_with_creation(
                package,
                node,
                id,
                branches,
                definitions,
                &planned.slots,
                work.control(),
                creation,
            )?;
            let owner = LocalOperatorId::new(
                u32::try_from(id.index()).map_err(|_| CompileControlError::ResourceExhausted)?,
            );
            for (branch, definitions) in branches.iter().zip(definitions) {
                for (ordinal, (input, &definition)) in
                    branch.sources.iter().zip(definitions).enumerate()
                {
                    union_roots.push(crate::union_flow::UnionRoot {
                        node: branch.normalizer,
                        ordinal: u32::try_from(ordinal)
                            .map_err(|_| CompileControlError::ResourceExhausted)?,
                        definition,
                        source: input.input.source,
                    });
                    work.step()?;
                }
            }
            let source_id = DiagnosticSourceNodeId::new(source.get());
            allowed.insert(source_id);
            local_ids.insert(source, id);
            for (piece, emitted_node) in emitted.into_iter().enumerate() {
                let emitted_id = emitted_node
                    .local_id()
                    .ok_or(FragmentCompileError::Invalid(
                        "missing UnionAll local node identity",
                    ))?;
                if emitted_id.index() != nodes.len() {
                    return Err((FragmentCompileError::Invalid("UnionAll schedule differs")).into());
                }
                for (ordinal, value) in node.output.columns.iter().enumerate() {
                    work.flush()?;
                    channels.push((
                        ProgramChannelSite::Layout {
                            node: emitted_id,
                            role: ProgramChannelLayoutRole::NodeOutput,
                            ordinal: u32::try_from(ordinal)
                                .map_err(|_| CompileControlError::ResourceExhausted)?,
                        },
                        physical.values()[value].ty.clone(),
                    ));
                    work.step()?;
                }
                operators.push(LocalOperatorProvenance {
                    id: LocalOperatorId::new(
                        u32::try_from(emitted_id.index())
                            .map_err(|_| CompileControlError::ResourceExhausted)?,
                    ),
                    lowered_nodes: Box::from([emitted_id]),
                    sources: Box::from([source_id]),
                    origin: LocalOperatorOrigin::Split {
                        piece: u32::try_from(piece)
                            .map_err(|_| CompileControlError::ResourceExhausted)?,
                    },
                    cost_owner: owner,
                    metrics: OperatorMetricAggregation {
                        cpu_time: MetricAggregation::Sum,
                        wall_time: MetricAggregation::Maximum,
                        peak_retained_bytes: MetricAggregation::Maximum,
                    },
                });
                nodes.push(emitted_node);
                work.step()?;
            }
            continue;
        }
        if id.index() != nodes.len() {
            return Err((FragmentCompileError::Invalid(
                "channel schedule differs from node schedule",
            ))
            .into());
        }
        local_ids.insert(source, id);
        let source_id = DiagnosticSourceNodeId::new(source.get());
        allowed.insert(source_id);
        let (kind, layout) = match &node.kind {
            NodeKind::Values { .. } => {
                work.flush()?;
                lower_values_with_creation(
                    package,
                    node,
                    &expressions,
                    &planned.slots,
                    work.control(),
                    creation,
                )?
            }
            NodeKind::ExchangeSource { .. } => {
                work.flush()?;
                let lowered = lower_exchange_source_with_creation(
                    package,
                    node,
                    &planned.slots,
                    options.exchange_wait,
                    work.control(),
                    creation,
                )?;
                source_requirements.push(BindingRequirement::ExchangeInput {
                    node: id,
                    role: lowered.input.role,
                    delivery: lowered.input.delivery,
                    layout: lowered.layout.clone(),
                });
                exchange_inputs.insert(id, lowered.input);
                (lowered.kind, lowered.layout)
            }
            NodeKind::Scan { .. } => {
                let recipe = reads.remove(&source).ok_or(FragmentCompileError::Invalid(
                    "missing provider read recipe",
                ))?;
                let filters = runtime_filters.take_scan(
                    source,
                    id,
                    expressions.runtime_filter_key_ids.get(&source),
                    factory,
                    crate::creation::borrow_scope(&mut temporary),
                    work,
                )?;
                work.flush()?;
                let lowered = lower_scan_with_creation(
                    node,
                    id,
                    recipe,
                    filters.filters,
                    &planned.slots,
                    &expressions.ids,
                    work.control(),
                    creation,
                )?;
                if filters.ordered {
                    let count = filters
                        .requirements
                        .len()
                        .checked_add(1)
                        .ok_or(CompileControlError::ResourceExhausted)?;
                    crate::creation::reserve_vec(
                        &mut source_requirements,
                        count,
                        crate::creation::borrow_scope(&mut temporary),
                        work,
                    )?;
                    crate::creation::reserve_vec(
                        &mut filter_roots,
                        filters.roots.len(),
                        crate::creation::borrow_scope(&mut temporary),
                        work,
                    )?;
                }
                source_requirements.push(lowered.requirement);
                source_requirements.extend(filters.requirements);
                filter_roots.extend(filters.roots);
                scan_inputs.insert(id, lowered.input);
                scan_layouts.insert(id);
                (lowered.kind, lowered.layout)
            }
            NodeKind::Sort { .. } => {
                let child = *local_ids
                    .get(&node.inputs[0])
                    .ok_or(FragmentCompileError::Invalid("missing lowered sort child"))?;
                work.flush()?;
                lower_sort(
                    node,
                    child,
                    nodes[child.index()].output_layout(),
                    &expressions.ids,
                    work.control(),
                )?
            }
            NodeKind::Window(_) => {
                let child =
                    *local_ids
                        .get(&node.inputs[0])
                        .ok_or(FragmentCompileError::Invalid(
                            "missing lowered window child",
                        ))?;
                work.flush()?;
                crate::window::lower_window_with_creation(
                    package,
                    node,
                    child,
                    nodes[child.index()].output_layout(),
                    &expressions.ids,
                    &planned.slots,
                    work.control(),
                    creation,
                    metadata,
                )?
            }
            NodeKind::Aggregate { .. } => {
                let child =
                    *local_ids
                        .get(&node.inputs[0])
                        .ok_or(FragmentCompileError::Invalid(
                            "missing lowered aggregate child",
                        ))?;
                let filters = runtime_filters.take_aggregate(
                    source,
                    &expressions.ids,
                    factory,
                    crate::creation::borrow_scope(&mut temporary),
                    work,
                )?;
                if !filters.filters.is_empty() {
                    crate::creation::reserve_vec(
                        &mut source_requirements,
                        filters.requirements.len(),
                        crate::creation::borrow_scope(&mut temporary),
                        work,
                    )?;
                }
                source_requirements.extend(filters.requirements);
                work.flush()?;
                let lowered = lower_aggregate_with_creation(
                    package,
                    node,
                    child,
                    &expressions.ids,
                    &planned.slots,
                    filters.filters,
                    work.control(),
                    creation,
                )?;
                aggregates.insert(id, lowered.fact);
                (lowered.kind, lowered.layout)
            }
            NodeKind::TopN { .. } => {
                let child = *local_ids
                    .get(&node.inputs[0])
                    .ok_or(FragmentCompileError::Invalid("missing lowered TopN child"))?;
                work.flush()?;
                match &node.kind {
                    NodeKind::TopN {
                        reduction: novarocks_physical_plan::TopNReduction::GroupedStates { .. },
                        ..
                    } => crate::topn::lower_grouped_topn_with_creation(
                        package,
                        node,
                        child,
                        &expressions.ids,
                        &planned.slots,
                        work.control(),
                        creation,
                    )?,
                    _ => lower_topn(
                        node,
                        child,
                        nodes[child.index()].output_layout(),
                        &expressions.ids,
                        work.control(),
                    )?,
                }
            }
            NodeKind::Filter { predicates } => {
                let child = *local_ids
                    .get(&node.inputs[0])
                    .ok_or(FragmentCompileError::Invalid("missing lowered child"))?;
                let mut local_predicates = Vec::with_capacity(predicates.len());
                for predicate in predicates {
                    local_predicates.push(*expressions.ids.get(predicate).ok_or(
                        FragmentCompileError::Invalid("missing predicate definition"),
                    )?);
                    work.step()?;
                }
                work.flush()?;
                let local_predicates = local_predicates.into_boxed_slice();
                work.flush()?;
                (
                    ProgramNodeKind::Filter {
                        input: child,
                        predicates: local_predicates,
                    },
                    nodes[child.index()].output_layout().clone(),
                )
            }
            NodeKind::Limit { limit, offset } => {
                let child = *local_ids
                    .get(&node.inputs[0])
                    .ok_or(FragmentCompileError::Invalid("missing lowered child"))?;
                let limit = limit
                    .map(usize::try_from)
                    .transpose()
                    .map_err(|_| FragmentCompileError::Invalid("limit exceeds host range"))?;
                let offset = usize::try_from(*offset)
                    .map_err(|_| FragmentCompileError::Invalid("offset exceeds host range"))?;
                (
                    ProgramNodeKind::Limit {
                        input: child,
                        limit,
                        offset,
                    },
                    nodes[child.index()].output_layout().clone(),
                )
            }
            NodeKind::Unpivot { .. } => {
                let child =
                    *local_ids
                        .get(&node.inputs[0])
                        .ok_or(FragmentCompileError::Invalid(
                            "missing lowered Unpivot child",
                        ))?;
                let sources = channels_plan.unpivot_sources.get(&source).ok_or(
                    FragmentCompileError::Invalid("missing planned Unpivot input sources"),
                )?;
                work.flush()?;
                lower_unpivot_with_creation(
                    package,
                    node,
                    id,
                    UnpivotLoweringInput {
                        node: child,
                        layout: nodes[child.index()].output_layout(),
                        sources,
                        expressions: &expressions.ids,
                    },
                    &planned.slots,
                    work.control(),
                    creation,
                )?
            }
            NodeKind::ChangeEventExpand { .. } => {
                let child =
                    *local_ids
                        .get(&node.inputs[0])
                        .ok_or(FragmentCompileError::Invalid(
                            "missing lowered change-event child",
                        ))?;
                work.flush()?;
                lower_change_events_with_creation(
                    package,
                    node,
                    id,
                    child,
                    &planned.slots,
                    &expressions.ids,
                    work.control(),
                    creation,
                )?
            }
            NodeKind::AssertOneRow(_) => {
                let child =
                    *local_ids
                        .get(&node.inputs[0])
                        .ok_or(FragmentCompileError::Invalid(
                            "missing lowered assertion child",
                        ))?;
                let keys = channels_plan.assertion_keys.get(&source).ok_or(
                    FragmentCompileError::Invalid("missing planned assertion keys"),
                )?;
                work.flush()?;
                lower_assert_rows(
                    node,
                    child,
                    nodes[child.index()].output_layout(),
                    keys,
                    *channels_plan.assertion_applicability.get(&source).ok_or(
                        FragmentCompileError::Invalid("missing planned assertion applicability"),
                    )?,
                    work.control(),
                )?
            }
            NodeKind::Repeat { .. } => {
                let child =
                    *local_ids
                        .get(&node.inputs[0])
                        .ok_or(FragmentCompileError::Invalid(
                            "missing lowered Repeat child",
                        ))?;
                work.flush()?;
                lower_repeat_with_creation(
                    package,
                    node,
                    id,
                    (child, nodes[child.index()].output_layout()),
                    &planned.slots,
                    work.control(),
                    creation,
                )?
            }
            NodeKind::Project {
                expressions: projected,
            } => {
                let child = *local_ids
                    .get(&node.inputs[0])
                    .ok_or(FragmentCompileError::Invalid("missing lowered child"))?;
                let mut is_result_output = result
                    .is_some_and(|result| node.output.columns.len() == result.output.columns.len());
                if let Some(result) = result.filter(|_| is_result_output) {
                    for (actual, expected) in node.output.columns.iter().zip(&result.output.columns)
                    {
                        work.step()?;
                        if actual != expected {
                            is_result_output = false;
                            break;
                        }
                    }
                }
                let requests = if matches!(metadata, ProjectMetadataMode::Funded(_)) {
                    Some(project_output_requests(
                        node.id,
                        id.index(),
                        projected.len(),
                        projected.iter().enumerate().map(|(ordinal, (_, value))| {
                            let ty = &physical
                                .values()
                                .get(value)
                                .ok_or(FragmentCompileError::Invalid("missing projection value"))?
                                .ty;
                            let name = if is_result_output {
                                let field = result
                                    .and_then(|result| result.fields.get(ordinal))
                                    .ok_or(FragmentCompileError::Invalid("missing result field"))?;
                                Some(field.alias.as_deref().unwrap_or(&field.name))
                            } else {
                                None
                            };
                            Ok((ty, name))
                        }),
                        work,
                    )?)
                } else {
                    None
                };
                let mut build =
                    || -> Result<ProjectMetadataOutput, crate::FragmentCreationError<H>> {
                        let mut typed_layout =
                            crate::typed_layout::TypedLayoutBuilder::new_with_creation(
                                projected.len(),
                                work,
                                creation,
                            )?;
                        let mut slots = Vec::new();
                        let mut exprs = Vec::new();
                        for (ordinal, (expr, value)) in projected.iter().enumerate() {
                            work.step()?;
                            let definition = physical.expressions().get(*expr).ok_or(
                                FragmentCompileError::Invalid("missing projection definition"),
                            )?;
                            let value_type = &physical
                                .values()
                                .get(value)
                                .ok_or(FragmentCompileError::Invalid("missing projection value"))?
                                .ty;
                            work.flush()?;
                            if !definition
                                .ty
                                .exactly_equals_observed::<crate::FragmentCreationError<H>>(
                                    value_type,
                                    || work.step().map_err(Into::into),
                                )?
                            {
                                return Err((FragmentCompileError::Invalid(
                                    "projection output type differs from definition",
                                ))
                                .into());
                            }
                            // Full result labels are authoritative only when the entire
                            // ordered output matches, including repeated occurrences.
                            let name = if is_result_output {
                                let field = result
                                    .and_then(|result| result.fields.get(ordinal))
                                    .ok_or(FragmentCompileError::Invalid("missing result field"))?;
                                crate::typed_layout::copy_name_with_creation(
                                    field.alias.as_deref().unwrap_or(&field.name),
                                    work,
                                    creation,
                                )?
                            } else {
                                crate::typed_layout::local_name_with_creation(
                                    "local",
                                    id.index(),
                                    ordinal,
                                    work,
                                    creation,
                                )?
                            };
                            typed_layout.push_with_creation(value_type, name, work, creation)?;
                            work.flush()?;
                            slots.push(*planned.slots.get(ordinal).ok_or(
                                FragmentCompileError::Invalid("missing planned output occurrence"),
                            )?);
                            exprs.push(*expressions.ids.get(expr).ok_or(
                                FragmentCompileError::Invalid("missing projection expression"),
                            )?);
                        }
                        work.flush()?;
                        let layout = typed_layout.finish_with_creation(&slots, work, creation)?;
                        let expr_slot_ids = layout.slots().to_vec();
                        Ok(ProjectMetadataOutput {
                            layout,
                            exprs,
                            expr_slot_ids,
                        })
                    };
                let ProjectMetadataOutput {
                    layout,
                    exprs,
                    expr_slot_ids,
                } = match requests {
                    Some(requests) => metadata.materialize(&requests, build)?,
                    None => build()?,
                };
                (
                    ProgramNodeKind::Project {
                        input: child,
                        is_subordinate: false,
                        // The expression Project computes its original roots. The
                        // terminal RootResult boundary validates the actual output.
                        validate_final_result_input: false,
                        exprs,
                        expr_slot_ids,
                        expr_slot_schemas: None,
                        output_indices: None,
                    },
                    layout,
                )
            }
            NodeKind::TableWriter { .. } => {
                let child_source = node.inputs[0];
                let child = *local_ids
                    .get(&child_source)
                    .ok_or(FragmentCompileError::Invalid(
                        "missing lowered table writer child",
                    ))?;
                let recipe = writes.remove(&source).ok_or(FragmentCompileError::Invalid(
                    "missing provider write recipe",
                ))?;
                let projection_slots = channels_plan.writer_projections.get(&source).ok_or(
                    FragmentCompileError::Invalid("missing planned writer projection"),
                )?;
                work.flush()?;
                let lowered = crate::writer::lower_writer_with_creation(
                    package,
                    node,
                    id,
                    &recipe,
                    crate::writer::WriterLoweringInput {
                        child,
                        child_node: &physical.nodes()[&child_source],
                        child_layout: nodes[child.index()].output_layout(),
                        output_slots: &planned.slots,
                        projection_slots,
                    },
                    work.control(),
                    creation,
                    crate::creation::borrow_scope(&mut temporary),
                    factory,
                )?;
                source_requirements.push(lowered.requirement);
                channels.extend(lowered.channels);
                writer_flows.push(lowered.flow);
                program_writes.insert(id, recipe);
                (lowered.kind, lowered.layout)
            }
            NodeKind::TableFinish(spec) => {
                let child = *local_ids
                    .get(&node.inputs[spec.inputs.writer_results as usize])
                    .ok_or(FragmentCompileError::Invalid(
                        "missing lowered table finish child",
                    ))?;
                let statistics_slots = channels_plan.finish_statistics.get(&source).ok_or(
                    FragmentCompileError::Invalid("missing planned finish statistics"),
                )?;
                work.flush()?;
                let lowered = crate::writer::lower_finish_with_creation(
                    package,
                    node,
                    id,
                    crate::writer::FinishLoweringInput {
                        child,
                        child_layout: nodes[child.index()].output_layout(),
                        summary: spec.inputs.logical_summary.map(|port| {
                            let id = local_ids[&node.inputs[port as usize]];
                            (id, nodes[id.index()].output_layout())
                        }),
                        slots: &planned.slots,
                        statistics_slots,
                        expressions: &expressions.ids,
                    },
                    work.control(),
                    creation,
                )?;
                source_requirements.push(lowered.requirement);
                channels.extend(lowered.channels);
                (lowered.kind, lowered.layout)
            }
            _ => {
                return Err(
                    (FragmentCompileError::Invalid("validated node family changed")).into(),
                );
            }
        };
        for (actual, expected) in layout.slots().iter().zip(planned.slots.iter()) {
            work.step()?;
            if actual != expected {
                return Err((FragmentCompileError::Invalid(
                    "materialized channel order differs from plan",
                ))
                .into());
            }
        }
        if layout.slots().len() != planned.slots.len()
            || layout.slots().len() != node.output.columns.len()
        {
            return Err((FragmentCompileError::Invalid("output occurrence width changed")).into());
        }
        // A one-input family that reuses the child's provider schema object
        // still publishes the provider's field names, not SQL labels.
        if let [child] = node.inputs.as_ref() {
            let inherited = local_ids.get(child).is_some_and(|child| {
                scan_layouts.contains(child)
                    && Arc::ptr_eq(
                        layout.schema(),
                        nodes[child.index()].output_layout().schema(),
                    )
            });
            work.step()?;
            if inherited {
                scan_layouts.insert(id);
            }
        }
        for (ordinal, value) in node.output.columns.iter().enumerate() {
            work.step()?;
            let ty: FunctionValueType = physical
                .values()
                .get(value)
                .ok_or(FragmentCompileError::Invalid("missing channel value"))?
                .ty
                .clone();
            channels.push((
                ProgramChannelSite::Layout {
                    node: id,
                    role: ProgramChannelLayoutRole::NodeOutput,
                    ordinal: u32::try_from(ordinal)
                        .map_err(|_| FragmentCompileError::Invalid("channel ordinal exhausted"))?,
                },
                ty,
            ));
        }
        let operator = LocalOperatorId::new(
            u32::try_from(id.index())
                .map_err(|_| FragmentCompileError::Invalid("operator identity exhausted"))?,
        );
        operators.push(LocalOperatorProvenance {
            id: operator,
            lowered_nodes: Box::from([id]),
            sources: Box::from([source_id]),
            origin: LocalOperatorOrigin::Direct,
            cost_owner: operator,
            metrics: OperatorMetricAggregation {
                cpu_time: MetricAggregation::Sum,
                wall_time: MetricAggregation::Maximum,
                peak_retained_bytes: MetricAggregation::Maximum,
            },
        });
        nodes.push(ProgramNode::new_local(id, vec![source_id], kind, layout));
    }
    // Each inbound cut is consumed by exactly one lowered receiver, and each
    // provider recipe by exactly one lowered scan.
    if exchange_inputs.len() != package.cuts().inbound.len() {
        return Err((FragmentCompileError::Invalid(
            "inbound exchange cut has no lowered receiver",
        ))
        .into());
    }
    if !reads.is_empty() {
        return Err(
            (FragmentCompileError::Invalid("provider read recipe has no lowered scan")).into(),
        );
    }
    if !writes.is_empty() {
        return Err((FragmentCompileError::Invalid(
            "provider write recipe has no lowered table writer",
        ))
        .into());
    }
    runtime_filters.finish()?;
    let mut root = local_ids[&physical.root()];
    if let Some(projection) = &channels_plan.sink_projection {
        let branch = channels_plan
            .unions
            .get(&physical.root())
            .and_then(|branches| branches.last())
            .ok_or(FragmentCompileError::Invalid(
                "missing router projection source",
            ))?;
        let definitions = expressions
            .union_ids
            .get(&physical.root())
            .and_then(|branches| branches.last())
            .ok_or(FragmentCompileError::Invalid(
                "missing router projection definitions",
            ))?;
        if projection.node.index() != nodes.len() || definitions.len() != projection.slots.len() {
            return Err(FragmentCompileError::Invalid("router projection schedule differs").into());
        }
        let mut typed = crate::typed_layout::TypedLayoutBuilder::new_with_creation(
            branch.sources.len(),
            work,
            creation,
        )?;
        for (ordinal, (source, definition)) in branch.sources.iter().zip(definitions).enumerate() {
            let source_ordinal = match source.input.source {
                ProgramChannelSite::Layout {
                    node,
                    role: ProgramChannelLayoutRole::NodeOutput,
                    ordinal,
                } if node == root => ordinal as usize,
                _ => {
                    return Err(FragmentCompileError::Invalid(
                        "router alias reads a foreign input",
                    )
                    .into());
                }
            };
            let name = nodes[root.index()]
                .output_layout()
                .schema()
                .field(source_ordinal)
                .name();
            let name = crate::typed_layout::copy_name_with_creation(name, work, creation)?;
            typed.push_with_creation(&source.ty, name, work, creation)?;
            channels.push((
                ProgramChannelSite::Layout {
                    node: projection.node,
                    role: ProgramChannelLayoutRole::NodeOutput,
                    ordinal: u32::try_from(ordinal)
                        .map_err(|_| CompileControlError::ResourceExhausted)?,
                },
                source.ty.clone_observed(|| work.step())?,
            ));
            union_roots.push(crate::union_flow::UnionRoot {
                node: projection.node,
                ordinal: u32::try_from(ordinal)
                    .map_err(|_| CompileControlError::ResourceExhausted)?,
                definition: *definition,
                source: source.input.source,
            });
            work.step()?;
        }
        let layout = typed.finish_with_creation(&projection.slots, work, creation)?;
        let diagnostic = DiagnosticSourceNodeId::new(physical.root().get());
        let operator = LocalOperatorId::new(
            u32::try_from(projection.node.index())
                .map_err(|_| CompileControlError::ResourceExhausted)?,
        );
        operators.push(LocalOperatorProvenance {
            id: operator,
            lowered_nodes: Box::from([projection.node]),
            sources: Box::from([diagnostic]),
            origin: LocalOperatorOrigin::Synthetic {
                reason: SyntheticOperatorReason::ExpressionControl,
            },
            cost_owner: LocalOperatorId::new(
                u32::try_from(root.index()).map_err(|_| CompileControlError::ResourceExhausted)?,
            ),
            metrics: OperatorMetricAggregation {
                cpu_time: MetricAggregation::Sum,
                wall_time: MetricAggregation::Maximum,
                peak_retained_bytes: MetricAggregation::Maximum,
            },
        });
        nodes.push(ProgramNode::new_local(
            projection.node,
            vec![diagnostic],
            ProgramNodeKind::Project {
                input: root,
                is_subordinate: true,
                validate_final_result_input: false,
                exprs: definitions.clone(),
                expr_slot_ids: projection.slots.to_vec(),
                expr_slot_schemas: None,
                output_indices: None,
            },
            layout,
        ));
        root = projection.node;
    }
    let root_layout = nodes[root.index()].output_layout();
    work.flush()?;
    let profile = CompileProfile::new(
        options.pipeline_dop,
        options.root_sink_dop,
        root_layout.identity_for_compile(work.control())?,
        options.kernel_abi,
    );
    let mut requirement_entries = source_requirements;
    let (sink, stream) = match stream_cut {
        Some(cut) => {
            work.flush()?;
            let lowered = lower_stream_sink_with_creation(
                package,
                cut,
                root,
                root_layout,
                work.control(),
                creation,
                crate::creation::borrow_scope(&mut temporary),
                factory,
            )?;
            requirement_entries.extend(lowered.requirements);
            (lowered.sink, Some(lowered.flow))
        }
        None if is_router => {
            let projection = channels_plan
                .sink_projection
                .as_ref()
                .ok_or(FragmentCompileError::Invalid("missing router projection"))?;
            let summary_slots =
                channels_plan
                    .summary_slots
                    .as_ref()
                    .ok_or(FragmentCompileError::Invalid(
                        "missing router summary slots",
                    ))?;
            let lowered = lower_router_sink_with_creation(
                package,
                root,
                root_layout,
                &projection.route_slots,
                summary_slots,
                work.control(),
                creation,
                crate::creation::borrow_scope(&mut temporary),
                factory,
            )?;
            requirement_entries.extend(lowered.requirements);
            (lowered.sink, Some(lowered.flow))
        }
        None => {
            // The published layout's names are the result's: the frontend
            // checks them against the declared schema. Any root other than a
            // provider scan publishes a layout some node labeled from the
            // result port. A provider layout cannot be relabeled -- its fields
            // must equal the public schema -- so its result labels must be
            // exactly the provider names.
            let result = result.ok_or(FragmentCompileError::Invalid("missing result port"))?;
            let fields = root_layout.schema().fields();
            if result.fields.len() != fields.len() {
                return Err((FragmentCompileError::Invalid(
                    "result width differs from its root layout",
                ))
                .into());
            }
            for (label, field) in result.fields.iter().zip(fields.iter()) {
                let same = label.alias.as_deref().unwrap_or(&label.name) == field.name();
                work.step()?;
                if !same {
                    return Err((FragmentCompileError::Unsupported {
                        node: Some(physical.root()),
                        feature: if scan_layouts.contains(&root) {
                            "result labels differ from the provider scan layout"
                        } else {
                            "result labels differ from the published root layout"
                        },
                    })
                    .into());
                }
            }
            requirement_entries.push(BindingRequirement::ResultSink {
                layout: root_layout.clone(),
            });
            let sink = match physical.sink() {
                FragmentSink::RootResult(contract) => {
                    let mut slots = Vec::with_capacity(root_layout.slots().len());
                    for slot in root_layout.slots() {
                        slots.push(slot.as_u32());
                        work.step()?;
                    }
                    work.flush()?;
                    let bound = contract
                        .as_ref()
                        .clone()
                        .bind_native_slots(&slots)
                        .map_err(|_| {
                            FragmentCompileError::Invalid(
                                "root result slots differ from its actual local output",
                            )
                        })?;
                    work.flush()?;
                    StaticSinkProgram::RootResult(Arc::new(bound))
                }
                FragmentSink::Result => StaticSinkProgram::Result,
                _ => {
                    return Err((FragmentCompileError::Invalid(
                        "terminal result sink changed during lowering",
                    ))
                    .into());
                }
            };
            (sink, None)
        }
    };
    work.flush()?;
    let requirements =
        BindingRequirements::try_new_for_compile(requirement_entries, work.control())?;
    work.flush()?;
    let graph = LocalProgramGraph::try_new_with_sink_for_compile(
        nodes,
        root,
        expressions.arena.clone(),
        profile,
        requirements,
        Some(sink),
        work.control(),
    )?;
    // A materialized constant Values cell is backing, not a runtime root.
    // Retire only those constant-cell source occurrences; every dynamic cell
    // keeps its root and argument uses as a ValuesCell root.
    let mut retired = retired_values_uses(package, &expressions, work)?;
    // A window call occurrence and its frame offsets leave the flow; its
    // argument uses are re-rooted on the Analytic node.
    let window_roots = crate::window::window_roots(package, &local_ids, work)?;
    retired.extend(window_roots.retired.iter().copied());
    // Argument backing and published maps belong to one actual Main flow
    // lifetime, separate from package construction and validation scratch.
    let mut main_flow_scope = match factory {
        Some(f) => {
            work.flush()?;
            Some(
                f.begin_scope()
                    .map_err(crate::FragmentCreationError::Creator)?,
            )
        }
        None => None,
    };
    let mut domains = Vec::new();
    for domain in package.expression_uses().flow().domains().values() {
        work.step()?;
        crate::creation::before_vec_push(
            &domains,
            crate::creation::borrow_scope(&mut temporary),
            work,
        )?;
        domains.push(*domain);
    }
    let mut uses = Vec::new();
    for invocation in package.expression_uses().flow().uses().values() {
        work.step()?;
        if retired.contains(&invocation.context.use_id) {
            continue;
        }
        let definition =
            *expressions
                .ids
                .get(&invocation.definition)
                .ok_or(FragmentCompileError::Invalid(
                    "missing invocation definition",
                ))?;
        let mut arguments = Vec::new();
        for argument in &invocation.arguments {
            work.step()?;
            if let Some(scope) = main_flow_scope.as_mut() {
                crate::creation::before_vec_push(&arguments, Some(scope), work)?;
            }
            arguments.push(*argument);
        }
        crate::creation::before_vec_push(
            &uses,
            crate::creation::borrow_scope(&mut temporary),
            work,
        )?;
        uses.push(ProgramExpressionUse {
            context: invocation.context,
            definition,
            control: invocation.control,
            arguments: {
                if arguments.capacity()!=arguments.len() && !arguments.is_empty()
                    && let Some(scope)=main_flow_scope.as_mut() {
                        crate::creation::request(std::alloc::Layout::array::<novarocks_type_contract::ExpressionUseId>(arguments.len()).map_err(|_| novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic)?,scope,work)?;
                    }
                arguments.into_boxed_slice()
            },
        });
    }
    let mut roots = Vec::new();
    let mut union_slot_bindings = Vec::new();
    let minted = crate::union_flow::package_use_ids(package, work)?;
    work.flush()?;
    match factory {
        Some(_) => {
            crate::union_flow::append_union_roots_with_creator(
                &union_roots,
                &minted,
                &mut domains,
                &mut uses,
                &mut roots,
                &mut union_slot_bindings,
                crate::creation::borrow_scope(&mut temporary)
                    .expect("strong entry supplies temporary scope"),
                work.control(),
            )?;
        }
        None => {
            crate::union_flow::append_union_roots(
                &union_roots,
                &minted,
                &mut domains,
                &mut uses,
                &mut roots,
                &mut union_slot_bindings,
                work.control(),
            )?;
        }
    }

    work.flush()?;
    match factory {
        Some(_) => {
            crate::union_flow::append_union_roots_with_creator(
                &filter_roots,
                &minted,
                &mut domains,
                &mut uses,
                &mut roots,
                &mut union_slot_bindings,
                crate::creation::borrow_scope(&mut temporary)
                    .expect("strong entry supplies temporary scope"),
                work.control(),
            )?;
        }
        None => {
            crate::union_flow::append_union_roots(
                &filter_roots,
                &minted,
                &mut domains,
                &mut uses,
                &mut roots,
                &mut union_slot_bindings,
                work.control(),
            )?;
        }
    }

    for (site, use_id) in package.expression_uses().bindings() {
        work.step()?;
        if retired.contains(use_id) {
            continue;
        }
        // A join's roots belong to its join node, never to its selection,
        // with each key oriented to its local probe or build side.
        if let Some(join) = channels_plan.joins.get(&site.node) {
            let (role, _) = crate::join::root_role(join, site.role)?;
            crate::creation::before_vec_push(
                &roots,
                crate::creation::borrow_scope(&mut temporary),
                work,
            )?;
            roots.push(ProgramRootUseBinding {
                site: ProgramExpressionRootSite::Node {
                    node: join.join,
                    role,
                },
                use_id: *use_id,
            });
            continue;
        }
        if let Some(root) = crate::generate_series::argument_root(
            &channels_plan.series,
            site.node,
            site.role,
            *use_id,
        )? {
            crate::creation::before_vec_push(
                &roots,
                crate::creation::borrow_scope(&mut temporary),
                work,
            )?;
            roots.push(root);
            continue;
        }
        // A table function's argument roots belong to its argument Project.
        if let ExpressionRootRole::TableFunctionArgument { argument } = site.role {
            crate::creation::before_vec_push(
                &roots,
                crate::creation::borrow_scope(&mut temporary),
                work,
            )?;
            roots.push(crate::table_function::argument_root(
                &channels_plan.table_functions,
                site.node,
                argument,
                *use_id,
            )?);
            continue;
        }
        let node = *local_ids
            .get(&site.node)
            .ok_or(FragmentCompileError::Invalid("missing root node"))?;
        let role = match site.role {
            ExpressionRootRole::ChangePredicate { event } => {
                ProgramNodeExpressionRole::ChangePredicate { event }
            }
            ExpressionRootRole::ChangeAssignment { event, assignment } => {
                ProgramNodeExpressionRole::ChangeAssignment { event, assignment }
            }
            ExpressionRootRole::UnpivotConstant { mapping, constant } => {
                ProgramNodeExpressionRole::UnpivotConstant { mapping, constant }
            }
            // A grouped Unpivot keeps the frozen mapping order, so a mapping
            // ordinal names the same mapping on both sides.
            ExpressionRootRole::FinishUnpivotConstant { mapping, constant } => {
                ProgramNodeExpressionRole::FinishUnpivotConstant { mapping, constant }
            }
            ExpressionRootRole::FilterPredicate { predicate } => {
                ProgramNodeExpressionRole::FilterPredicate { predicate }
            }
            ExpressionRootRole::ScanResidual { predicate } => {
                ProgramNodeExpressionRole::ScanResidual { predicate }
            }
            ExpressionRootRole::SortOrder { key } => ProgramNodeExpressionRole::SortOrder { key },
            ExpressionRootRole::SortPartition { key } => {
                ProgramNodeExpressionRole::SortPartition { key }
            }
            ExpressionRootRole::WindowPartition { key } => {
                ProgramNodeExpressionRole::WindowPartition { key }
            }
            ExpressionRootRole::WindowOrder { key } => {
                ProgramNodeExpressionRole::WindowOrder { key }
            }
            ExpressionRootRole::TopNOrder { key } => match &physical.nodes()[&site.node].kind {
                NodeKind::TopN {
                    reduction: novarocks_physical_plan::TopNReduction::GroupedStates { .. },
                    ..
                } => ProgramNodeExpressionRole::TopNOrder { key },
                _ => ProgramNodeExpressionRole::SortOrder { key },
            },
            ExpressionRootRole::TopNGroup { group } => {
                ProgramNodeExpressionRole::TopNGroup { group }
            }
            ExpressionRootRole::TopNStateArgument { call, argument } => {
                ProgramNodeExpressionRole::TopNStateArgument { call, argument }
            }
            ExpressionRootRole::ProjectOutput { expression } => {
                ProgramNodeExpressionRole::ProjectOutput { expression }
            }
            ExpressionRootRole::ValuesCell { row, column } => {
                ProgramNodeExpressionRole::ValuesCell { row, column }
            }
            ExpressionRootRole::AggregateGroup { group } => {
                ProgramNodeExpressionRole::AggregateGroup { group }
            }
            ExpressionRootRole::AggregateArgument { call, argument } => {
                ProgramNodeExpressionRole::AggregateInput { call, argument }
            }
            // Function ORDER channels follow the call's logical arguments.
            ExpressionRootRole::AggregateOrder { call, key } => {
                let NodeKind::Aggregate { calls, .. } = &physical.nodes()[&site.node].kind else {
                    return Err((FragmentCompileError::Invalid(
                        "aggregate order root outside an Aggregate",
                    ))
                    .into());
                };
                let logical = calls
                    .get(call as usize)
                    .ok_or(FragmentCompileError::Invalid(
                        "missing aggregate order call",
                    ))?
                    .arguments
                    .len();
                let argument = u32::try_from(logical)
                    .ok()
                    .and_then(|logical| logical.checked_add(key))
                    .ok_or(FragmentCompileError::Invalid(
                        "aggregate order channel exhausted",
                    ))?;
                ProgramNodeExpressionRole::AggregateInput { call, argument }
            }
            _ => {
                return Err((FragmentCompileError::Unsupported {
                    node: Some(site.node),
                    feature: "expression root role",
                })
                .into());
            }
        };
        crate::creation::before_vec_push(
            &roots,
            crate::creation::borrow_scope(&mut temporary),
            work,
        )?;
        roots.push(ProgramRootUseBinding {
            site: ProgramExpressionRootSite::Node { node, role },
            use_id: *use_id,
        });
    }
    crate::creation::reserve_vec(
        &mut roots,
        window_roots.inputs.len(),
        crate::creation::borrow_scope(&mut temporary),
        work,
    )?;
    roots.extend(window_roots.inputs);
    work.flush()?;
    let flow = match main_flow_scope.as_mut() {
        Some(retained) => ProgramControlFlow::try_new_with_creator_in(
            domains,
            uses,
            expressions.arena.nodes().len(),
            crate::creation::borrow_scope(&mut temporary)
                .expect("strong entry supplies temporary scope"),
            retained,
            work.control(),
        )?,
        None => ProgramControlFlow::try_new(
            domains,
            uses,
            expressions.arena.nodes().len(),
            work.control(),
        )?,
    };
    crate::creation::before_tree_insert::<ProgramExpressionArena, ProgramControlFlow, H>(
        0,
        crate::creation::borrow_scope(&mut temporary),
        work,
    )?;
    let mut flows = BTreeMap::from([(ProgramExpressionArena::Main, flow)]);
    crate::creation::before_tree_insert::<
        ProgramExpressionArena,
        novarocks_type_contract::FunctionArgumentTypes,
        H,
    >(0, crate::creation::borrow_scope(&mut temporary), work)?;
    let mut types = BTreeMap::from([(ProgramExpressionArena::Main, expressions.types)]);
    let mut sink_slot_bindings = Vec::new();
    // A stream sink always owns a Sink arena, so its flow and types are
    // supplied even when the arena is empty (Gather and Broadcast).
    if let Some(stream) = stream {
        crate::creation::reserve_vec(
            &mut roots,
            stream.roots.len(),
            crate::creation::borrow_scope(&mut temporary),
            work,
        )?;
        roots.extend(stream.roots);
        crate::creation::before_tree_insert::<ProgramExpressionArena, ProgramControlFlow, H>(
            flows.len(),
            crate::creation::borrow_scope(&mut temporary),
            work,
        )?;
        flows.insert(ProgramExpressionArena::Sink, stream.flow);
        crate::creation::before_tree_insert::<
            ProgramExpressionArena,
            novarocks_type_contract::FunctionArgumentTypes,
            H,
        >(
            types.len(),
            crate::creation::borrow_scope(&mut temporary),
            work,
        )?;
        types.insert(ProgramExpressionArena::Sink, stream.types);
        sink_slot_bindings = stream.slots;
    }
    // Each writer owns its WriterProjection arena with its own flow, types,
    // roots and input occurrences.
    for writer in writer_flows {
        crate::creation::reserve_vec(
            &mut roots,
            writer.roots.len(),
            crate::creation::borrow_scope(&mut temporary),
            work,
        )?;
        roots.extend(writer.roots);
        crate::assert_rows::reserve_vec(&mut sink_slot_bindings, writer.slots.len(), work)?;
        sink_slot_bindings.extend(writer.slots);
        crate::creation::before_tree_insert::<ProgramExpressionArena, ProgramControlFlow, H>(
            flows.len(),
            crate::creation::borrow_scope(&mut temporary),
            work,
        )?;
        flows.insert(writer.arena, writer.flow);
        crate::creation::before_tree_insert::<
            ProgramExpressionArena,
            novarocks_type_contract::FunctionArgumentTypes,
            H,
        >(
            types.len(),
            crate::creation::borrow_scope(&mut temporary),
            work,
        )?;
        types.insert(writer.arena, writer.types);
    }
    let channels = match factory {
        Some(f) => attach_typed_root_channels_with_factory_in(
            graph,
            flows,
            roots,
            tokens,
            types,
            channels,
            crate::creation::borrow_scope(&mut temporary)
                .expect("strong entry supplies temporary scope"),
            f,
            work,
        )?,
        None => attach_typed_root_channels_created_in(
            graph, flows, roots, tokens, types, channels, work,
        )
        .map_err(FragmentCompileError::from)?,
    };
    work.flush()?;
    let mut slot_bindings = union_slot_bindings;
    if !sink_slot_bindings.is_empty() {
        crate::assert_rows::reserve_vec(&mut slot_bindings, sink_slot_bindings.len(), work)?;
        slot_bindings.extend(sink_slot_bindings);
    }
    // A join-owned read takes the layout its own root reads: a key root its
    // side, a residual or nested-loop predicate the join scope.
    work.flush()?;
    let join_uses = crate::join::join_use_roles(package, &channels_plan.joins, work.control())?;
    work.flush()?;
    for invocation in package.expression_uses().flow().uses().values() {
        work.step()?;
        if let Some(input) = channels_plan.inputs.get(&invocation.definition) {
            let source = match channels_plan.join_values.get(&invocation.definition) {
                Some(value) => {
                    let (join, role) = *join_uses.get(&invocation.context.use_id).ok_or(
                        FragmentCompileError::Invalid("join value read outside its join's roots"),
                    )?;
                    value.for_root(join, role)?
                }
                None => input.source,
            };
            slot_bindings.push(ProgramSlotBinding {
                occurrence: ProgramUseRef {
                    arena: ProgramExpressionArena::Main,
                    use_id: invocation.context.use_id,
                },
                source: ProgramLexicalSource::Input(source),
            });
        }
    }
    work.flush()?;
    let lexical = match factory {
        Some(factory) => {
            let mut retained = factory
                .begin_scope()
                .map_err(crate::creation::FragmentCreationError::Creator)?;
            ProgramLexicalBindings::try_new_with_creator_in(
                channels,
                vec![],
                slot_bindings,
                // This compiler authors no Lambda capture allocation.
                novarocks_type_contract::ValueTypeStorageLifetime::pure(),
                crate::creation::borrow_scope(&mut temporary)
                    .expect("strong entry supplies the real temporary scope"),
                &mut retained,
                work.control(),
            )?
        }
        None => ProgramLexicalBindings::try_new(channels, vec![], slot_bindings, work.control())?,
    };
    work.flush()?;
    let facts = CompiledProgramFacts {
        writes: program_writes,
        exchange_inputs,
        scan_inputs,
        aggregates,
    };
    match factory {
        Some(factory) => LocalProgram::try_new_with_creator_in(
            lexical,
            operators,
            &allowed,
            facts,
            crate::creation::borrow_scope(&mut temporary)
                .expect("strong entry supplies the real temporary scope"),
            factory,
            work.control(),
        )
        .map_err(Into::into),
        None => LocalProgram::try_new(lexical, operators, &allowed, facts, work.control())
            .map_err(Into::into),
    }
}

/// Which node shape has no local owner, so a refusal names it.
fn unsupported_node_shape(kind: &NodeKind) -> &'static str {
    match kind {
        NodeKind::Filter { .. } => "filter with other than one predicate or input",
        NodeKind::Sort { .. } => "sort mode without a local owner",
        NodeKind::TopN { .. } => "grouped-state TopN reduction",
        NodeKind::SetOp { .. } => "INTERSECT or EXCEPT set operation",
        NodeKind::GenerateSeries { .. } => "generate_series source",
        _ => "node family or occurrence shape",
    }
}

/// Explicit pure FE preparation uses ordinary references, original structural
/// control and the same mandatory-creator body. Runtime callers supply their
/// actual creator through `compile_fragment_with_creation`.
pub fn compile_fragment(
    input: ProviderValidatedFragment,
    functions: &PureEngineFunctionCatalog,
    options: LocalCompileOptions,
    control: &dyn PureCompileControl,
) -> Result<LocalProgram, FragmentCompileError> {
    crate::creation::pure(|creation| {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
        let result = lower_with_creation::<_, crate::creation::NoFactory, DirectProjectMetadata>(
            input,
            functions,
            options,
            &mut work,
            creation,
            None,
            None,
            &mut ProjectMetadataMode::<DirectProjectMetadata>::Direct,
        );
        crate::creation::finish_attempt(result, work)
    })
}

/// Explicit funding observer around the same pure typed Project author.
pub fn compile_fragment_with_project_metadata_host<P: ProjectMetadataScope>(
    input: ProviderValidatedFragment,
    functions: &PureEngineFunctionCatalog,
    options: LocalCompileOptions,
    control: &dyn PureCompileControl,
    host: &mut P,
) -> Result<LocalProgram, FragmentCompileError> {
    if input.package().original_metadata_namespace().is_none() {
        return compile_fragment(input, functions, options, control);
    }
    crate::creation::pure(|creation| {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
        let result = lower_with_creation::<_, crate::creation::NoFactory, P>(
            input,
            functions,
            options,
            &mut work,
            creation,
            None,
            None,
            &mut ProjectMetadataMode::Funded(host),
        );
        crate::creation::finish_attempt(result, work)
    })
}

#[cfg(test)]
mod capability_admission_tests {
    use super::*;
    use novarocks_functions::{
        FunctionBindingError, FunctionOverloadId, FunctionSpecializationFailure,
    };
    use novarocks_physical_plan::ExprId;

    #[test]
    fn capability_identity_survives_private_expression_ownership() {
        let overload = FunctionOverloadId::try_new("fixture/missing/pure-v1").unwrap();
        for error in [
            ExpressionLoweringError::UnsupportedExpression(ExprId::new(7)),
            ExpressionLoweringError::Binding(FunctionBindingError::UnavailableImplementation(
                overload.clone(),
            )),
            ExpressionLoweringError::Specialization(
                FunctionSpecializationFailure::MissingPureImplementation(overload),
            ),
        ] {
            let error = FragmentCompileError::from(error);
            assert!(matches!(error, FragmentCompileError::Owner { .. }));
            assert!(error.is_unsupported(), "{error}");
        }
        assert!(
            FragmentCompileError::Unsupported {
                node: None,
                feature: "FULL OUTER hash join"
            }
            .is_unsupported()
        );
        for error in [
            FragmentCompileError::from(ExpressionLoweringError::Invalid(
                "unsupported is diagnostic text",
            )),
            FragmentCompileError::from(ExpressionLoweringError::Binding(
                FunctionBindingError::UnknownFunction,
            )),
            FragmentCompileError::Invalid("unsupported is diagnostic text"),
            FragmentCompileError::Control(CompileControlError::Cancelled),
            FragmentCompileError::Control(CompileControlError::ResourceExhausted),
        ] {
            assert!(!error.is_unsupported(), "{error}");
        }
    }
}
