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
    expressions::{ExpressionLoweringError, LoweredExpressions},
    repeat::{RepeatLoweringError, lower_repeat_with_creation},
    scan::{admit_scan, lower_scan_with_creation},
    sort::lower_sort,
    stream_sink::{
        admit_multicast_cut, lower_multicast_sink_with_creation, lower_router_sink_with_creation,
        lower_stream_sink_with_creation, lower_summary_outputs_with_creation,
        plan_multicast_output,
    },
    topn::lower_topn,
    unpivot::{UnpivotLoweringError, UnpivotLoweringInput, lower_unpivot_with_creation},
    values::lower_values_with_creation,
};
use novarocks_functions::{ConstantValidation, PureEngineFunctionCatalog};
use novarocks_local_program::*;
use novarocks_physical_plan::{
    Distribution, EdgeKind, FragmentSink, JoinSide, NodeId, NodeKind, RowMultiplicity,
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
};

/// Host-admitted values are explicit; the compiler authors the actual layout
/// digest. Neither topology nor resource defaults are inferred from the plan.
#[derive(Clone, Copy, Debug)]
pub struct LocalCompileOptions {
    pub pipeline_dop: NonZeroUsize,
    pub root_sink_dop: Option<NonZeroUsize>,
    pub kernel_abi: KernelAbiVersion,
    pub constants: ConstantValidation,
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
            Self::Owner { error, .. } => error
                .downcast_ref::<ExpressionLoweringError>()
                .is_some_and(ExpressionLoweringError::is_unsupported),
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
owner_error!(ProgramResolvedCallsError, "relation calls");
owner_error!(ProgramChannelTypeError, "typed channels");
owner_error!(LocalProgramCompileError, "local program");

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
    let (package, mut reads, mut writes, mut layouts) = input.into_parts();
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
    let is_result = matches!(
        physical.sink(),
        FragmentSink::Result | FragmentSink::RootResult(_)
    );
    let (stream_cut, multicast) = match physical.sink() {
        FragmentSink::Result | FragmentSink::RootResult(_) => {
            if outbound.len() != physical.summaries().len() {
                return Err((FragmentCompileError::Invalid(
                    "result sink has an outbound exchange cut",
                ))
                .into());
            }
            (None, None)
        }
        FragmentSink::Stream { edge } => {
            let cut = outbound
                .iter()
                .find(|cut| cut.edge == *edge)
                .ok_or(FragmentCompileError::Invalid("stream sink cut is absent"))?;
            if outbound.len() != physical.summaries().len() + 1 || cut.edge != *edge {
                return Err((FragmentCompileError::Invalid(
                    "stream sink edge differs from its outbound cut",
                ))
                .into());
            }
            if cut.kind == EdgeKind::CteMulticast {
                return Err(FragmentCompileError::Invalid(
                    "multicast cut requires a multicast sink",
                )
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
            (Some(cut), None)
        }
        FragmentSink::Router { .. } => (None, None),
        FragmentSink::Multicast { edges } => (
            None,
            Some(plan_multicast_output(
                package,
                edges,
                crate::creation::borrow_scope(&mut temporary),
                work,
            )?),
        ),
        FragmentSink::Noop => {
            return Err((FragmentCompileError::Unsupported {
                node: None,
                feature: "noop sink",
            })
            .into());
        }
    };
    let mut routing_receivers = BTreeSet::new();
    for cut in package.cuts().inbound.iter() {
        work.step()?;
        if cut.kind == EdgeKind::CteMulticast {
            admit_multicast_cut(
                cut.kind,
                cut.delivery,
                cut.source_port,
                &cut.payload,
                &cut.partitioning,
            )?;
        }
        if !cut.partitioning.routing_only.is_empty() {
            routing_receivers.insert(cut.destination_node);
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
    if is_result && result.is_none() {
        return Err((FragmentCompileError::Invalid("missing result port")).into());
    }
    if !is_result && result.is_some() {
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
        let pieces = if matches!(node.kind, NodeKind::SetOp { .. }) {
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
        if matches!(node.kind, NodeKind::SetOp { .. }) {
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
    let multicast_aliases = multicast
        .as_ref()
        .map_or(&[][..], |output| output.alias_sources.as_slice());
    let root_width = physical.nodes()[&physical.root()].output.columns.len();
    let sink_project_width = match physical.sink() {
        FragmentSink::Router { routes, .. } => {
            Some(routes.iter().try_fold(root_width, |width, route| {
                work.step()?;
                width
                    .checked_add(route.input_mapping.len())
                    .ok_or(CompileControlError::ResourceExhausted)
            })?)
        }
        FragmentSink::Multicast { .. } if !multicast_aliases.is_empty() => Some(
            root_width
                .checked_add(multicast_aliases.len())
                .ok_or(CompileControlError::ResourceExhausted)?,
        ),
        _ => None,
    };
    if let Some(width) = sink_project_width {
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
    let mut definition_count = 0usize;
    let mut region_count = 0usize;
    let mut use_count = 0usize;
    let mut references = derived_roots;
    for node in physical.nodes().values() {
        work.step()?;
        if let Some(unit) = &node.expressions {
            definition_count = definition_count
                .checked_add(unit.definitions().len())
                .ok_or(CompileControlError::ResourceExhausted)?;
            region_count = region_count
                .checked_add(unit.control_flow().regions().len())
                .ok_or(CompileControlError::ResourceExhausted)?;
            use_count = use_count
                .checked_add(unit.control_flow().uses().len())
                .ok_or(CompileControlError::ResourceExhausted)?;
            references = references
                .checked_add(unit.control_flow().use_reference_count())
                .ok_or(CompileControlError::ResourceExhausted)?;
        }
    }
    if channel_count > MAX_PROGRAM_TYPED_CHANNELS
        || expanded_nodes
            .checked_mul(2)
            .is_none_or(|n| n > MAX_PROFILE_REFERENCES)
        || expanded_nodes > MAX_PROGRAM_NODES
        || expanded_nodes > MAX_PROGRAM_EXPANDED_OCCURRENCES
        || definition_count
            .checked_add(derived_definitions)
            .is_none_or(|n| n > novarocks_type_contract::MAX_CONTROL_DEFINITIONS)
        || region_count
            .checked_add(derived_roots)
            .is_none_or(|n| n > novarocks_type_contract::MAX_CONTROL_DEFINITIONS)
        || use_count
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
    // A join may consume one replica of a complete build subtree.
    let replicated_builds = crate::join::replicated_build_inputs(physical, work)?;
    stack.push((physical.root(), false, 0usize));
    while let Some((id, exiting, depth)) = stack.pop() {
        work.step()?;
        if exiting {
            order.push(id);
            continue;
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
        // Project and Aggregate lower to the linear worklist. Their outgoing
        // chains keep the same recursive depth as the current physical node.
        let recursive_depth = depth
            + usize::from(!matches!(
                node.kind,
                NodeKind::Project { .. } | NodeKind::Aggregate { .. }
            ));
        if recursive_depth > MAX_RECURSIVE_PIPELINE_BUILD_DEPTH {
            return Err(CompileControlError::ResourceExhausted.into());
        }
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
        // broadcast placement are allowed only in a complete build subtree.
        if replicated_builds.get(&id) != Some(&true)
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
        let supported = match &node.kind {
            NodeKind::GenerateSeries { .. } => {
                crate::generate_series::admit(package, node, work)?;
                if recursive_depth
                    .checked_add(1)
                    .is_none_or(|depth| depth > MAX_RECURSIVE_PIPELINE_BUILD_DEPTH)
                {
                    return Err(CompileControlError::ResourceExhausted.into());
                }
                true
            }
            NodeKind::SystemRelation { .. }
            | NodeKind::Values { .. }
            | NodeKind::ExchangeSource { .. }
            | NodeKind::Scan { .. } => node.inputs.is_empty(),
            NodeKind::Project { .. }
            | NodeKind::Limit { .. }
            | NodeKind::AssertOneRow(_)
            | NodeKind::Repeat { .. }
            | NodeKind::Unpivot { .. }
            | NodeKind::ChangeEventExpand { .. }
            | NodeKind::Aggregate { .. } => node.inputs.len() == 1,
            NodeKind::WriteLayoutKey { .. } => node.id == physical.root() && node.inputs.len() == 1,
            NodeKind::PositionShard(spec) => {
                node.id == physical.root()
                    && !spec.inputs.is_empty()
                    && node.inputs.len() == spec.inputs.len()
            }
            NodeKind::Filter { predicates } => !predicates.is_empty() && node.inputs.len() == 1,
            // Every frozen sort mode and row-count TopN phase has one local owner.
            NodeKind::Sort { .. }
            | NodeKind::TopN {
                reduction: novarocks_physical_plan::TopNReduction::Rows,
                ..
            } => node.inputs.len() == 1,
            NodeKind::TopN {
                reduction: novarocks_physical_plan::TopNReduction::GroupedStates { .. },
                ..
            } => {
                crate::topn::admit_grouped_topn_shape(node)?;
                true
            }
            // A window, under its own admission: one input and an installed
            // pure window kernel for each call shape.
            NodeKind::Window(_) => {
                crate::window::admit_window(package, node, functions, work)?;
                true
            }
            NodeKind::SetOp { .. } => node.inputs.len() >= 2,
            // The writer family, under its own admission below.
            NodeKind::TableWriter { .. } => node.inputs.len() == 1,
            NodeKind::TableFinish(spec) => {
                node.inputs.len() == 1 + spec.inputs.summaries.iter().count()
            }
            // Both join families, under their own admission below.
            NodeKind::HashJoin { .. }
            | NodeKind::NestLoopJoin { .. }
            | NodeKind::ContentApply(_) => node.inputs.len() == 2,
            // A table function, under its own admission: exactly one outer
            // input and an installed pure TableV1 kernel.
            NodeKind::TableFunction { .. } => {
                crate::table_function::admit_table_function(node, functions, work)?;
                true
            }
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
            // The derived selection, normalization and argument Projects
            // introduced by lowering also use the linear worklist.
            stack.push((child, false, recursive_depth));
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
    // actual expansion can consume that uncertainty. A scan with runtime
    // splits, a single-copy routed receiver and a singleton stage each prove
    // placement without needing an output partition key. A partial TopN or
    // partial aggregation works only with the rows the instance already has.
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
        // A table function and a write layout key keep each outer row on its
        // driver and preserve the ordering of the pass-through occurrences.
        let transparent = matches!(
            node.kind,
            NodeKind::Project { .. }
                | NodeKind::Filter { .. }
                | NodeKind::Limit { .. }
                | NodeKind::TableFunction { .. }
                | NodeKind::WriteLayoutKey { .. }
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
        // Checked single-copy inputs already name where their rows run, even
        // when a later operator cannot retain any partition key in its output.
        let checked_input_placement = |input: &NodeId| {
            physical.nodes().get(input).is_some_and(|node| {
                node.output_properties.row_multiplicity
                    == novarocks_physical_plan::RowMultiplicity::SingleCopy
                    && (properties.get(input).is_some_and(|p| p.2)
                        || matches!(
                            node.output_properties.distribution,
                            Distribution::Singleton
                                | Distribution::RoundRobin
                                | Distribution::Hash { .. }
                                | Distribution::ConditionalKey { .. }
                                | Distribution::BucketShuffle { .. }
                        ))
            })
        };
        // A UnionAll preserves the exact instance placement of all of its
        // inputs, even when their layouts cannot yield one shared key.
        let union_placed = matches!(
            node.kind,
            NodeKind::SetOp {
                kind: novarocks_physical_plan::SetOperationKind::UnionAll,
                ..
            }
        ) && node.inputs.iter().all(|input| {
            properties.get(input).is_some_and(|p| p.2)
                || physical
                    .nodes()
                    .get(input)
                    .is_some_and(|node| has_single_copy_partitioning(&node.output_properties))
        });
        // Repeat emits its frozen grouping sets on the exact input instances.
        // Its property author may drop partition keys after nullifying them,
        // but that does not create a new placement or ordering source.
        let repeat_placed = matches!(node.kind, NodeKind::Repeat { .. })
            && node.inputs.len() == 1
            && checked_input_placement(&node.inputs[0]);
        // A legal build-preserving join may lose both inputs' partition keys
        // (FULL OUTER), while both inputs retain their checked instance
        // placement. Keep that placement fact, not a guessed distribution.
        // Network join admissibility was checked by orient_join above.
        let join_placed = probe_input.is_some() && node.inputs.iter().all(checked_input_placement);
        // ContentApply drops the output partition key. Its two inputs still
        // prove the same single-copy owner in the checked physical plan.
        let content_placed = matches!(node.kind, NodeKind::ContentApply(_))
            && node.inputs.iter().all(checked_input_placement);
        // The cut can place each row exactly once with or without a visible
        // routing key after the exchange.
        let exchange_placed = matches!(node.kind, NodeKind::ExchangeSource { .. })
            && (routing_receivers.contains(&id)
                || has_single_copy_partitioning(&node.output_properties));
        let singleton_placed = *physical.stage_placement()
            == novarocks_physical_plan::StagePlacementPolicy::Singleton
            && node.output_properties.row_multiplicity == RowMultiplicity::SingleCopy;
        let placed = singleton_placed
            || matches!(node.kind, NodeKind::Scan { .. })
            || exchange_placed
            || projected_placement
            || repeat_placed
            || join_placed
            || content_placed
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
        if unknown && !expanded && !changes && !placed && !writer_rooted {
            return Err((FragmentCompileError::Unsupported {
                node: Some(id),
                feature: "unconstrained distribution without a runtime-split scan placement",
            })
            .into());
        }
        // Every row-count TopN phase emits its instance's window as one
        // ordered stream on one driver, so its declared ordering holds for
        // the whole instance output, as the property law states it. An
        // analytic Sort and partition TopN do the same over partition keys
        // followed by order keys; pruning preserves that ordering.
        let global = matches!(
            node.kind,
            NodeKind::Sort {
                mode: novarocks_physical_plan::SortMode::Global
                    | novarocks_physical_plan::SortMode::Analytic { .. }
                    | novarocks_physical_plan::SortMode::PartitionTopN { .. },
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
        let global_assertion = matches!(
            node.kind,
            NodeKind::AssertOneRow(novarocks_physical_plan::RowCountAssertionSpec::Global { .. })
        );
        let ordered = global || (sorted && (transparent || window || global_assertion));
        if !(node.output_properties.ordering.is_empty() || ordered) {
            return Err((FragmentCompileError::Unsupported {
                node: Some(id),
                feature: "ordering lacks supported global-sort source",
            })
            .into());
        }
        properties.insert(id, (expanded || changes, ordered, placed));
    }
    work.flush()?;
    let channels_plan = resolve_tree_channels_with_creation(
        package,
        &order,
        multicast_aliases,
        work.control(),
        creation,
    )?;
    let runtime_filter_keys = runtime_filters.consumer_keys(
        &channels_plan.nodes,
        crate::creation::borrow_scope(&mut temporary),
        work,
    )?;
    work.flush()?;
    let mut expressions =
        LoweredExpressions::new(package, crate::creation::borrow_scope(&mut temporary), work)?;
    let mut local_nodes = BTreeMap::new();
    let mut physical_nodes = BTreeMap::new();
    for (source, planned) in &channels_plan.nodes {
        crate::creation::before_tree_insert::<NodeId, ProgramNodeId, H>(
            local_nodes.len(),
            crate::creation::borrow_scope(&mut temporary),
            work,
        )?;
        local_nodes.insert(*source, planned.local);
        crate::creation::before_tree_insert::<ProgramNodeId, NodeId, H>(
            physical_nodes.len(),
            crate::creation::borrow_scope(&mut temporary),
            work,
        )?;
        physical_nodes.insert(planned.local, *source);
        work.step()?;
    }
    let mut nodes: Vec<ProgramNode> = Vec::new();
    let mut local_ids = BTreeMap::new();
    let mut channels = Vec::new();
    let mut operators = Vec::new();
    let mut allowed = BTreeSet::new();
    let mut source_requirements = Vec::new();
    let mut exchange_inputs = BTreeMap::new();
    let mut scan_inputs = BTreeMap::new();
    let mut aggregates = BTreeMap::new();
    let mut program_writes = BTreeMap::new();
    // Local nodes whose layout is a provider scan layout unchanged.
    let mut scan_layouts = BTreeSet::new();
    crate::assert_rows::reserve_vec(&mut nodes, expanded_nodes, work)?;
    crate::assert_rows::reserve_vec(&mut operators, expanded_nodes, work)?;
    crate::assert_rows::reserve_vec(&mut channels, channel_count, work)?;
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
        if !matches!(
            node.kind,
            NodeKind::Scan { .. } | NodeKind::HashJoin { .. } | NodeKind::NestLoopJoin { .. }
        ) {
            let mut inputs = Vec::new();
            crate::creation::reserve_vec(
                &mut inputs,
                node.inputs.len(),
                crate::creation::borrow_scope(&mut temporary),
                work,
            )?;
            for source in &node.inputs {
                let layout = local_ids
                    .get(source)
                    .and_then(|id: &ProgramNodeId| nodes.get(id.index()))
                    .map(ProgramNode::output_layout)
                    .ok_or(FragmentCompileError::Invalid(
                        "expression input runtime layout is absent",
                    ))?;
                work.step()?;
                inputs.push(layout);
            }
            crate::expressions::compile_node_unit(
                package,
                node,
                &inputs,
                None,
                &mut expressions,
                functions,
                options.constants,
                creation,
                crate::creation::borrow_scope(&mut temporary),
                factory,
                work,
            )?;
        }
        if !matches!(
            node.kind,
            NodeKind::HashJoin { .. } | NodeKind::NestLoopJoin { .. }
        ) && let Some(branches) = channels_plan.unions.get(&source)
        {
            let mut roots = Vec::new();
            crate::creation::reserve_vec(
                &mut roots,
                branches.len(),
                crate::creation::borrow_scope(&mut temporary),
                work,
            )?;
            for branch in branches {
                if channels_plan
                    .sink_projection
                    .as_ref()
                    .is_some_and(|projection| projection.node == branch.normalizer)
                {
                    continue;
                }
                let child =
                    *physical_nodes
                        .get(&branch.input)
                        .ok_or(FragmentCompileError::Invalid(
                            "synthesized read input node is absent",
                        ))?;
                roots.push(crate::union_flow::compile_branch_reads(
                    package,
                    &mut expressions,
                    functions,
                    options.constants,
                    nodes[branch.input.index()].output_layout(),
                    child,
                    branch,
                    creation,
                    crate::creation::borrow_scope(&mut temporary),
                    factory,
                    work,
                )?);
            }
            crate::creation::before_tree_insert::<
                NodeId,
                Vec<Vec<novarocks_local_program::ProgramRootRef>>,
                H,
            >(
                expressions.union_ids.len(),
                crate::creation::borrow_scope(&mut temporary),
                work,
            )?;
            expressions.union_ids.insert(source, roots);
        }
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
            let filters = runtime_filters.take_join(
                source,
                package,
                join,
                expressions.roots(source)?,
                work,
            )?;
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
                &mut expressions,
                functions,
                options.constants,
                crate::creation::borrow_scope(&mut temporary),
                factory,
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
                expressions.roots(source)?,
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
                expressions.roots(source)?,
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
            allowed.insert(DiagnosticSourceNodeId::new(source.get()));
            local_ids.insert(source, id);
            continue;
        }
        if matches!(node.kind, NodeKind::SetOp { .. }) {
            let branches =
                channels_plan
                    .unions
                    .get(&source)
                    .ok_or(FragmentCompileError::Invalid(
                        "missing set operation branches",
                    ))?;
            let branches = &branches[..node.inputs.len()];
            let definitions =
                expressions
                    .union_ids
                    .get(&source)
                    .ok_or(FragmentCompileError::Invalid(
                        "missing set operation definitions",
                    ))?;
            let definitions = &definitions[..node.inputs.len()];
            work.flush()?;
            let emitted = crate::union::lower_set_operation_with_creation(
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
            let source_id = DiagnosticSourceNodeId::new(source.get());
            allowed.insert(source_id);
            local_ids.insert(source, id);
            for (piece, emitted_node) in emitted.into_iter().enumerate() {
                let emitted_id = emitted_node
                    .local_id()
                    .ok_or(FragmentCompileError::Invalid(
                        "missing set operation local node identity",
                    ))?;
                if emitted_id.index() != nodes.len() {
                    return Err(
                        (FragmentCompileError::Invalid("set operation schedule differs")).into(),
                    );
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
            NodeKind::SystemRelation { .. } => {
                crate::system_relation::lower(package, node, &planned.slots, creation, work)?
            }
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
                let mut lowered = lower_scan_with_creation(
                    node,
                    id,
                    recipe,
                    ScanFilterConsumers::default(),
                    &planned.slots,
                    expressions.roots(source)?,
                    work.control(),
                    creation,
                )?;
                crate::expressions::compile_node_unit(
                    package,
                    node,
                    &[&lowered.layout],
                    None,
                    &mut expressions,
                    functions,
                    options.constants,
                    creation,
                    crate::creation::borrow_scope(&mut temporary),
                    factory,
                    work,
                )?;
                if let Some(keys) = runtime_filter_keys.get(&source) {
                    let mut columns = Vec::new();
                    crate::creation::reserve_vec(
                        &mut columns,
                        node.output.columns.len(),
                        crate::creation::borrow_scope(&mut temporary),
                        work,
                    )?;
                    for value in &node.output.columns {
                        work.step()?;
                        columns.push((*value, physical.values()[value].ty.clone()));
                    }
                    let mut reads = Vec::new();
                    crate::creation::reserve_vec(
                        &mut reads,
                        keys.len(),
                        crate::creation::borrow_scope(&mut temporary),
                        work,
                    )?;
                    for key in keys {
                        let ProgramChannelSite::Layout { ordinal, .. } = key.input.source else {
                            return Err(FragmentCompileError::Invalid(
                                "scan filter reads a nonlayout channel",
                            )
                            .into());
                        };
                        work.step()?;
                        reads.push((
                            ordinal as usize,
                            novarocks_type_contract::EvaluationDemand::Value,
                        ));
                    }
                    let roots = crate::union_flow::compile_reads(
                        package,
                        &mut expressions,
                        functions,
                        options.constants,
                        &lowered.layout,
                        &columns,
                        &reads,
                        creation,
                        crate::creation::borrow_scope(&mut temporary),
                        factory,
                        work,
                    )?;
                    crate::creation::before_tree_insert::<
                        NodeId,
                        Vec<novarocks_local_program::ProgramRootRef>,
                        H,
                    >(
                        expressions.runtime_filter_key_ids.len(),
                        crate::creation::borrow_scope(&mut temporary),
                        work,
                    )?;
                    expressions.runtime_filter_key_ids.insert(source, roots);
                }
                let filters = runtime_filters.take_scan(
                    source,
                    id,
                    expressions.runtime_filter_key_ids.get(&source),
                    factory,
                    crate::creation::borrow_scope(&mut temporary),
                    work,
                )?;
                if let ProgramNodeKind::Scan {
                    runtime_filters, ..
                } = &mut lowered.kind
                {
                    *runtime_filters = filters.filters;
                }
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
                }
                source_requirements.push(lowered.requirement);
                source_requirements.extend(filters.requirements);
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
                    expressions.roots(source)?,
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
                    expressions.roots(source)?,
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
                    expressions.roots(source)?,
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
                    expressions.roots(source)?,
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
                        expressions.roots(source)?,
                        &planned.slots,
                        work.control(),
                        creation,
                    )?,
                    _ => lower_topn(
                        node,
                        child,
                        nodes[child.index()].output_layout(),
                        expressions.roots(source)?,
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
                    local_predicates.push(expressions.root(source, *predicate)?);
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
                        expressions: expressions.roots(source)?,
                    },
                    &planned.slots,
                    work.control(),
                    creation,
                )?
            }
            NodeKind::WriteLayoutKey {
                key_inputs,
                output_key,
            } => {
                let child =
                    *local_ids
                        .get(&node.inputs[0])
                        .ok_or(FragmentCompileError::Invalid(
                            "missing lowered layout input",
                        ))?;
                let recipe = layouts
                    .remove(&source)
                    .ok_or(FragmentCompileError::Invalid(
                        "missing sealed write layout recipe",
                    ))?;
                lower_write_layout_key(
                    package,
                    node,
                    (key_inputs, *output_key),
                    child,
                    nodes[child.index()].output_layout(),
                    &planned.slots,
                    recipe,
                    work,
                    creation,
                )?
            }
            NodeKind::PositionShard(spec) => lower_position_shard(
                physical,
                node,
                spec,
                planned,
                &channels_plan.nodes,
                work,
                creation,
            )?,
            NodeKind::ContentApply(spec) => lower_content_apply(
                physical,
                node,
                spec,
                planned,
                &channels_plan,
                runtime_filters.take_content_demand(source, &mut source_requirements, work)?,
                work,
                creation,
            )?,
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
                    expressions.roots(source)?,
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
                            let definition =
                                crate::expressions::bound_root(package, source, *expr)?;
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
                            exprs.push(expressions.root(source, *expr)?);
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
                    &mut expressions,
                    functions,
                    options.constants,
                    work.control(),
                    creation,
                    crate::creation::borrow_scope(&mut temporary),
                    factory,
                )?;
                source_requirements.push(lowered.requirement);
                channels.extend(lowered.channels);
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
                        summaries: spec.inputs.summaries.map(|_, port| {
                            let id = local_ids[&node.inputs[port as usize]];
                            (id, nodes[id.index()].output_layout())
                        }),
                        slots: &planned.slots,
                        statistics_slots,
                        expressions: expressions.roots(source)?,
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
        let data_output = node.data_output_columns();
        if layout.slots().len() != planned.slots.len() || layout.slots().len() != data_output.len()
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
        for (ordinal, value) in data_output.iter().enumerate() {
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
        let branches =
            channels_plan
                .unions
                .get(&physical.root())
                .ok_or(FragmentCompileError::Invalid(
                    "sink projection sources are absent",
                ))?;
        let branch = branches
            .last()
            .filter(|branch| branch.normalizer == projection.node)
            .ok_or(FragmentCompileError::Invalid(
                "sink projection source is absent",
            ))?;
        let definitions = crate::union_flow::compile_branch_reads(
            package,
            &mut expressions,
            functions,
            options.constants,
            nodes[root.index()].output_layout(),
            physical.root(),
            branch,
            creation,
            crate::creation::borrow_scope(&mut temporary),
            factory,
            work,
        )?;
        if projection.node.index() != nodes.len() || definitions.len() != projection.slots.len() {
            return Err(FragmentCompileError::Invalid("sink projection schedule differs").into());
        }
        let mut typed = crate::typed_layout::TypedLayoutBuilder::new_with_creation(
            branch.sources.len(),
            work,
            creation,
        )?;
        for (ordinal, source) in branch.sources.iter().enumerate() {
            let source_ordinal = match source.input.source {
                ProgramChannelSite::Layout {
                    node,
                    role: ProgramChannelLayoutRole::NodeOutput,
                    ordinal,
                } if node == root => ordinal as usize,
                _ => {
                    return Err(
                        FragmentCompileError::Invalid("sink alias reads a foreign input").into(),
                    );
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
            work.step()?;
        }
        let layout = typed.finish_with_creation(&projection.slots, work, creation)?;
        let mut expr_slot_ids = Vec::new();
        crate::creation::reserve_vec(
            &mut expr_slot_ids,
            projection.slots.len(),
            Some(creation),
            work,
        )?;
        for &slot in projection.slots.iter() {
            expr_slot_ids.push(slot);
            work.step()?;
        }
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
                exprs: definitions,
                expr_slot_ids,
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
    let (sink, _) = match (stream_cut, multicast.as_ref()) {
        (Some(cut), _) => {
            work.flush()?;
            let lowered = lower_stream_sink_with_creation(
                package,
                cut,
                root,
                root_layout,
                &mut expressions,
                functions,
                options.constants,
                work.control(),
                creation,
                crate::creation::borrow_scope(&mut temporary),
                factory,
            )?;
            requirement_entries.extend(lowered.requirements);
            (lowered.sink, ())
        }
        (None, None) if is_router => {
            let projection = channels_plan
                .sink_projection
                .as_ref()
                .ok_or(FragmentCompileError::Invalid("missing router projection"))?;
            let lowered = lower_router_sink_with_creation(
                package,
                root,
                root_layout,
                &mut expressions,
                functions,
                options.constants,
                &projection.route_slots,
                work.control(),
                creation,
                crate::creation::borrow_scope(&mut temporary),
                factory,
            )?;
            requirement_entries.extend(lowered.requirements);
            (lowered.sink, ())
        }
        (None, Some(multicast)) => {
            work.flush()?;
            let lowered = lower_multicast_sink_with_creation(
                package,
                multicast,
                root,
                root_layout,
                &mut expressions,
                functions,
                options.constants,
                work.control(),
                creation,
                crate::creation::borrow_scope(&mut temporary),
                factory,
            )?;
            requirement_entries.extend(lowered.requirements);
            (lowered.sink, ())
        }
        (None, None) => {
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
            (sink, ())
        }
    };
    work.flush()?;
    let (summaries, summary_requirements) = lower_summary_outputs_with_creation(
        package,
        &channels_plan.summary_slots,
        &local_ids,
        sink.branches().len(),
        work.control(),
        creation,
    )?;
    requirement_entries.extend(summary_requirements);
    work.flush()?;
    let requirements =
        BindingRequirements::try_new_for_compile(requirement_entries, work.control())?;
    work.flush()?;
    let tokens = crate::expressions::prepare_calls_with_aggregates(
        package,
        &expressions,
        functions,
        &local_nodes,
        work,
        factory.map(|factory| (factory, &options.constants)),
        crate::creation::borrow_scope(&mut temporary),
    )?;
    let units = expressions.finish(crate::creation::borrow_scope(&mut temporary), work)?;
    let graph = LocalProgramGraph::try_new_with_outputs_for_compile(
        nodes,
        root,
        &units,
        profile,
        requirements,
        Some(sink),
        summaries,
        work.control(),
    )?;
    let facts = CompiledProgramFacts {
        input_completion: match physical.input_completion() {
            novarocks_physical_plan::FragmentInputCompletion::MayShortCircuit => {
                novarocks_local_program::InputCompletion::MayShortCircuit
            }
            novarocks_physical_plan::FragmentInputCompletion::MustDrain => {
                novarocks_local_program::InputCompletion::MustDrain
            }
        },
        writes: program_writes,
        exchange_inputs,
        scan_inputs,
        aggregates,
    };
    match factory {
        Some(factory) => LocalProgram::try_new_with_creator_in(
            graph,
            units,
            tokens,
            channels,
            operators,
            &allowed,
            facts,
            crate::creation::borrow_scope(&mut temporary)
                .expect("strong entry supplies the original temporary scope"),
            factory,
            work.control(),
        )
        .map_err(Into::into),
        None => LocalProgram::try_new(
            graph,
            units,
            tokens,
            channels,
            operators,
            &allowed,
            facts,
            work.control(),
        )
        .map_err(Into::into),
    }
}

fn lower_write_layout_key<H>(
    package: &novarocks_physical_plan::FragmentPackage,
    node: &novarocks_physical_plan::PhysicalNode,
    (key_inputs, output_key): (
        &[novarocks_physical_plan::ValueId],
        novarocks_physical_plan::ValueId,
    ),
    input: ProgramNodeId,
    input_layout: &novarocks_local_program::StaticLayout,
    slots: &[novarocks_types::SlotId],
    recipe: novarocks_connector_contract::ConnectorWriteLayoutRecipe,
    work: &mut CompileCheckpoints<'_>,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
) -> Result<(ProgramNodeKind, novarocks_local_program::StaticLayout), crate::FragmentCreationError<H>>
{
    use novarocks_physical_plan::ValueId;
    let fragment = package.fragment();
    let child = &fragment.nodes()[&node.inputs[0]];
    let local_slot = |value: ValueId, work: &mut CompileCheckpoints<'_>| {
        for (ordinal, candidate) in child.output.columns.iter().enumerate() {
            work.step()?;
            if *candidate == value {
                return Ok(input_layout.slots()[ordinal]);
            }
        }
        Err(FragmentCompileError::Invalid(
            "layout key reads outside its input",
        ))
    };
    let mut key_slots = Vec::new();
    crate::assert_rows::reserve_vec(&mut key_slots, key_inputs.len(), work)?;
    for value in key_inputs {
        key_slots.push(local_slot(*value, work)?);
    }
    let cut = package.cuts().outbound.iter().find(|cut| {
        matches!(cut.partitioning.routing_only.as_ref(), [(key, _)] if *key == output_key)
    }).ok_or(FragmentCompileError::Invalid("layout key lacks its exact output edge"))?;
    let (branch, effects) = match fragment.sink() {
        FragmentSink::Stream { edge } if *edge == cut.edge => (0, None),
        FragmentSink::Router { effect, routes } => {
            let (branch, route) = routes
                .iter()
                .enumerate()
                .find(|(_, route)| route.edge == cut.edge)
                .ok_or(FragmentCompileError::Invalid(
                    "layout key lacks the router branch",
                ))?;
            // The original ChangeEventExpand produces one effect per page.
            if !matches!(child.kind, NodeKind::ChangeEventExpand { effect_output: value, .. } if value == *effect)
            {
                return Err(FragmentCompileError::Unsupported {
                    node: Some(node.id),
                    feature: "write layout router input other than its original change-event expansion",
                }.into());
            }
            (
                branch,
                Some(novarocks_local_program::ProgramWriteLayoutEffects {
                    slot: local_slot(*effect, work)?,
                    accepted: Arc::from(route.accepted_effects.clone()),
                }),
            )
        }
        _ => return Err(FragmentCompileError::Invalid("layout key has an invalid sink").into()),
    };
    let mut layout =
        crate::typed_layout::TypedLayoutBuilder::new_with_creation(slots.len(), work, creation)?;
    for (ordinal, value) in node.output.columns.iter().enumerate() {
        let name = if ordinal < input_layout.slots().len() {
            crate::typed_layout::copy_name_with_creation(
                input_layout.schema().field(ordinal).name(),
                work,
                creation,
            )?
        } else {
            crate::typed_layout::copy_name_with_creation("write_layout_key", work, creation)?
        };
        layout.push_with_creation(&fragment.values()[value].ty, name, work, creation)?;
    }
    Ok((
        ProgramNodeKind::WriteLayoutKey {
            input,
            key_inputs: key_slots.into_boxed_slice(),
            output_key: *slots
                .last()
                .ok_or(FragmentCompileError::Invalid("missing layout key slot"))?,
            recipe,
            branch,
            effects,
        },
        layout.finish_with_creation(slots, work, creation)?,
    ))
}

fn lower_position_shard<H>(
    fragment: &novarocks_physical_plan::Fragment,
    node: &novarocks_physical_plan::PhysicalNode,
    spec: &novarocks_physical_plan::PositionShardAggregation,
    planned: &crate::channels::NodeChannels,
    channels: &BTreeMap<NodeId, crate::channels::NodeChannels>,
    work: &mut CompileCheckpoints<'_>,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
) -> Result<(ProgramNodeKind, StaticLayout), crate::FragmentCreationError<H>> {
    let invalid = |message| FragmentCompileError::Invalid(message);
    let output_slots: [novarocks_types::SlotId; 9] = planned
        .slots
        .as_ref()
        .try_into()
        .map_err(|_| invalid("position shard output width differs"))?;
    if node.output.columns.as_ref() != spec.output.columns()
        || node.inputs.len() != spec.inputs.len()
        || spec.inputs.is_empty()
    {
        return Err(invalid("position shard ports differ from its frozen layout").into());
    }
    let mut inputs = Vec::new();
    crate::assert_rows::reserve_vec(&mut inputs, spec.inputs.len(), work)?;
    for (&id, input) in node.inputs.iter().zip(&spec.inputs) {
        work.step()?;
        let child = fragment
            .nodes()
            .get(&id)
            .ok_or_else(|| invalid("missing position contribution node"))?;
        let child_channels = channels
            .get(&id)
            .ok_or_else(|| invalid("missing position contribution channels"))?;
        let &[file, position, spec_id, partition] = child_channels.slots.as_ref() else {
            return Err(invalid("position contribution channel width differs").into());
        };
        if !matches!(child.kind, NodeKind::ExchangeSource { .. })
            || child.output.columns.as_ref() != input.columns()
        {
            return Err(
                invalid("position contribution must be its exact received relation").into(),
            );
        }
        inputs.push(ProgramPositionShardInput {
            input: child_channels.local,
            file,
            position,
            spec_id,
            partition,
        });
    }
    let mut layout = crate::typed_layout::TypedLayoutBuilder::new_with_creation(
        output_slots.len(),
        work,
        creation,
    )?;
    for (ordinal, value) in spec.output.columns().into_iter().enumerate() {
        let ty = &fragment
            .values()
            .get(&value)
            .ok_or_else(|| invalid("missing position shard output value"))?
            .ty;
        let name = crate::typed_layout::local_name_with_creation(
            "local",
            planned.local.index(),
            ordinal,
            work,
            creation,
        )?;
        layout.push_with_creation(ty, name, work, creation)?;
    }
    Ok((
        ProgramNodeKind::PositionShard {
            group: spec.group,
            inputs: inputs.into_boxed_slice(),
            output_slots,
        },
        layout.finish_with_creation(&output_slots, work, creation)?,
    ))
}

fn lower_content_apply<H>(
    fragment: &novarocks_physical_plan::Fragment,
    node: &novarocks_physical_plan::PhysicalNode,
    spec: &novarocks_physical_plan::ContentApplySpec,
    planned: &crate::channels::NodeChannels,
    channels: &crate::channels::PlannedChannels,
    demand_filters: Box<[(usize, novarocks_local_program::StaticFilterProducer)]>,
    work: &mut CompileCheckpoints<'_>,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
) -> Result<(ProgramNodeKind, StaticLayout), crate::FragmentCreationError<H>> {
    use novarocks_local_program::{ProgramContentApply, ProgramContentApplyTarget, SummaryKind};

    let [weighted, target] = node.inputs.as_ref() else {
        return Err(FragmentCompileError::Invalid("content application inputs differ").into());
    };
    let input_id = |id| {
        channels
            .nodes
            .get(id)
            .map(|input| input.local)
            .ok_or(FragmentCompileError::Invalid(
                "missing content application input channels",
            ))
    };
    let content =
        |input, values: &[novarocks_physical_plan::ValueId], work: &mut CompileCheckpoints<'_>| {
            let mut slots = Vec::new();
            crate::assert_rows::reserve_vec(&mut slots, values.len(), work)?;
            for &value in values {
                slots.push(channels.data_slot(input, value)?);
                work.step()?;
            }
            Ok::<_, crate::FragmentCreationError<H>>(slots.into_boxed_slice())
        };
    let mut route_keys = Vec::new();
    crate::assert_rows::reserve_vec(&mut route_keys, spec.route_keys.len(), work)?;
    for &key in &spec.route_keys {
        route_keys.push(key);
        work.step()?;
    }
    let summary_slots = channels
        .summary_slots
        .get(SummaryKind::DeleteVerification)
        .and_then(|slots| slots.as_ref().try_into().ok())
        .ok_or(FragmentCompileError::Invalid(
            "content application summary layout differs",
        ))?;
    let data = node.data_output_columns();
    let mut layout =
        crate::typed_layout::TypedLayoutBuilder::new_with_creation(data.len(), work, creation)?;
    for (ordinal, value) in data.iter().enumerate() {
        let ty = &fragment
            .values()
            .get(value)
            .ok_or(FragmentCompileError::Invalid(
                "missing content application output value",
            ))?
            .ty;
        let name = crate::typed_layout::local_name_with_creation(
            "local",
            planned.local.index(),
            ordinal,
            work,
            creation,
        )?;
        layout.push_with_creation(ty, name, work, creation)?;
    }
    Ok((
        ProgramNodeKind::ContentApply(ProgramContentApply {
            weighted_input: input_id(weighted)?,
            target_input: input_id(target)?,
            weighted_content: content(*weighted, &spec.weighted_content, work)?,
            target_content: content(*target, &spec.target_content, work)?,
            route_keys: route_keys.into_boxed_slice(),
            weight: channels.data_slot(*weighted, spec.weight)?,
            target: ProgramContentApplyTarget {
                file: channels.data_slot(*target, spec.target.file)?,
                position: channels.data_slot(*target, spec.target.position)?,
                spec_id: channels.data_slot(*target, spec.target.spec_id)?,
                partition: channels.data_slot(*target, spec.target.partition)?,
            },
            demand_filters,
            summary_slots,
        }),
        layout.finish_with_creation(&planned.slots, work, creation)?,
    ))
}

fn has_single_copy_partitioning(properties: &novarocks_physical_plan::PhysicalProperties) -> bool {
    properties.row_multiplicity == RowMultiplicity::SingleCopy
        && matches!(
            properties.distribution,
            Distribution::RoundRobin
                | Distribution::Hash { .. }
                | Distribution::ConditionalKey { .. }
                | Distribution::BucketShuffle { .. }
        )
}

/// Which node shape has no local owner, so a refusal names it.
fn unsupported_node_shape(kind: &NodeKind) -> &'static str {
    match kind {
        NodeKind::Filter { .. } => "filter with other than one predicate or input",
        NodeKind::Sort { .. } => "sort with other than one input",
        NodeKind::TopN { .. } => "TopN with other than one input",
        NodeKind::SetOp { .. } => "set operation with fewer than two inputs",
        NodeKind::GenerateSeries { .. } => "generate_series source",
        NodeKind::PositionShard(_) => "position shard aggregation",
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

impl From<novarocks_constant_contract::ConstantTableError> for FragmentCompileError {
    fn from(error: novarocks_constant_contract::ConstantTableError) -> Self {
        Self::from(novarocks_physical_plan::ConstantReferenceError::from(error))
    }
}

#[cfg(test)]
mod capability_admission_tests {
    use super::*;
    use novarocks_expression_contract::ExprId;
    use novarocks_functions::{
        FunctionBindingError, FunctionOverloadId, FunctionSpecializationFailure,
    };

    #[test]
    fn capability_identity_survives_private_expression_ownership() {
        let overload = FunctionOverloadId::try_new("fixture/missing/pure-v1").unwrap();
        for error in [
            ExpressionLoweringError::from(
                novarocks_expression::ExprCompileError::UnsupportedExpression(ExprId::new(7)),
            ),
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
