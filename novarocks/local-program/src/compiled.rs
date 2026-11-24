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

//! Final pure compilation product. It directly owns one immutable graph and
//! its compiled unit table. Relation calls, typed channels, provider recipes and
//! provenance are validated against those exact inputs before publication.

use crate::{
    BindingRequirement, CompiledOriginsError, DiagnosticSourceNodeId, LocalOperatorProvenance,
    LocalProgramGraph, ProgramCallSite, ProgramNodeId, ProgramNodeKind, ProgramProvenance,
    ProgramResolvedCalls, ProgramStateTemplate, ProgramTypedChannels, ProviderLinkError,
};
use novarocks_connector_contract::ConnectorWriteRecipe;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use novarocks_types::SlotId;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

/// Compiled-only receiver address of one actual `ExchangeSource` node. A
/// compiled node has no legacy native identity, so the program carries the
/// physical routing facts explicitly instead of deriving them from a node ID.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompiledExchangeRouting {
    Unpartitioned,
    RoundRobin,
    Hash {
        algorithm: novarocks_type_contract::PartitionHashAlgorithm,
    },
    ConditionalKey {
        applies: SlotId,
        algorithm: novarocks_type_contract::PartitionHashAlgorithm,
    },
    BucketShuffle {
        algorithm: novarocks_type_contract::PartitionHashAlgorithm,
    },
}

#[derive(Clone, Debug)]
pub struct CompiledExchangeInput {
    /// Complete cut-source declarations in wire ordinal order, mapped to local slots.
    pub wire_layout: crate::StaticLayout,
    /// Local decoded transport layout when the exchange carries an additional
    /// write-layout key. The local writer shuffle consumes and removes the key.
    pub routing_layout: Option<crate::StaticLayout>,
    pub role: crate::ProgramOutputRole,
    pub delivery: novarocks_execution_contract::ExchangeDeliveryRequirement,
    pub routing: CompiledExchangeRouting,
    /// Physical NodeId of the receiving ExchangeSource; senders address it.
    pub receiver_node: u32,
    /// Physical EdgeId of the inbound stream edge.
    pub edge: u32,
    /// Physical FragmentId of the sending fragment.
    pub source_fragment: u32,
    /// Exact destination hash keys, in the inbound cut's key order, projected
    /// onto this receiver's input slots. Unpartitioned cuts carry no keys.
    pub hash_partition_slots: Box<[SlotId]>,
}

/// Compiled-only physical address of one actual provider `Scan` node. A
/// compiled node has no legacy native identity, so the program names the
/// physical node that keys every relation's task-local work-unit queue, range scope and
/// scan assignment instead of deriving it from a node ID.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompiledScanInput {
    /// Physical NodeId of the scan; runtime splits are addressed to it.
    pub scan_node: u32,
}

/// Whether one compiled aggregate's emitted groups are complete for its task.
/// This is the physical grouping guarantee, which a node's finalization flag
/// does not imply: a merging distinct phase finalizes nothing yet must still
/// emit each group once.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompiledAggregateGrouping {
    /// A group may be emitted by several drivers, batches or instances; a
    /// later phase merges them.
    Partial,
    /// Each group is emitted at most once by the whole task, so every driver
    /// of the task that feeds this aggregate must reach one group owner.
    Complete,
}

/// Compiled-only facts of one actual `Aggregate` node.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompiledAggregate {
    pub grouping: CompiledAggregateGrouping,
}

/// Every compiled-only fact the program carries beside its checked graph.
/// Each map addresses exactly the graph nodes of its family; the final owner
/// validates all of them against that one graph.
#[derive(Clone, Debug, Default)]
pub struct CompiledProgramFacts {
    pub writes: BTreeMap<ProgramNodeId, ConnectorWriteRecipe>,
    pub exchange_inputs: BTreeMap<ProgramNodeId, CompiledExchangeInput>,
    pub scan_inputs: BTreeMap<ProgramNodeId, CompiledScanInput>,
    pub aggregates: BTreeMap<ProgramNodeId, CompiledAggregate>,
}

#[derive(Debug)]
pub struct LocalProgram {
    graph: LocalProgramGraph,
    units: Box<[std::sync::Arc<novarocks_expression::CompiledExprSet>]>,
    calls: ProgramResolvedCalls,
    channels: ProgramTypedChannels,
    _unit_storage: novarocks_type_contract::ValueTypeStorageLifetime,
    provenance: ProgramProvenance,
    writes: BTreeMap<ProgramNodeId, ConnectorWriteRecipe>,
    exchange_inputs: BTreeMap<ProgramNodeId, CompiledExchangeInput>,
    scan_inputs: BTreeMap<ProgramNodeId, CompiledScanInput>,
    aggregates: BTreeMap<ProgramNodeId, CompiledAggregate>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LocalProgramCompileError {
    Control(CompileControlError),
    MissingSink,
    /// The compiled exchange addresses, the actual `ExchangeSource` nodes and
    /// their `ExchangeInput` requirements are not the same node set.
    ExchangeInputMismatch(ProgramNodeId),
    /// Two compiled exchange inputs name the same physical receiver.
    DuplicateExchangeReceiver(u32),
    /// The compiled scan addresses, the actual `Scan` nodes and their `Scan`
    /// requirements are not the same node set.
    ScanInputMismatch(ProgramNodeId),
    /// Two compiled scan inputs name the same physical scan node.
    DuplicateScanNode(u32),
    /// The compiled aggregate facts and the actual `Aggregate` nodes are not
    /// the same node set.
    AggregateMismatch(ProgramNodeId),
    /// A finalizing aggregate declares only a partial grouping guarantee.
    PartialFinalization(ProgramNodeId),
    Origins(CompiledOriginsError),
    Provider(ProviderLinkError),
    Calls(crate::ProgramResolvedCallsError),
    Channels(crate::ProgramChannelTypeError),
    Sink(crate::StaticSinkError),
    InvalidExpression,
    TooManyDefinitions,
    TooManyConstantBytes,
}
impl From<crate::StaticSinkError> for LocalProgramCompileError {
    fn from(cause: crate::StaticSinkError) -> Self {
        Self::Sink(cause)
    }
}
impl From<CompileControlError> for LocalProgramCompileError {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}
impl From<crate::ProgramResolvedCallsError> for LocalProgramCompileError {
    fn from(error: crate::ProgramResolvedCallsError) -> Self {
        match error {
            crate::ProgramResolvedCallsError::Control(cause) => Self::Control(cause),
            other => Self::Calls(other),
        }
    }
}
impl From<crate::ProgramChannelTypeError> for LocalProgramCompileError {
    fn from(error: crate::ProgramChannelTypeError) -> Self {
        match error {
            crate::ProgramChannelTypeError::Control(cause) => Self::Control(cause),
            other => Self::Channels(other),
        }
    }
}
impl From<CompiledOriginsError> for LocalProgramCompileError {
    fn from(error: CompiledOriginsError) -> Self {
        match error {
            CompiledOriginsError::Control(cause) => Self::Control(cause),
            other => Self::Origins(other),
        }
    }
}
impl From<ProviderLinkError> for LocalProgramCompileError {
    fn from(error: ProviderLinkError) -> Self {
        match error {
            ProviderLinkError::Control(cause) => Self::Control(cause),
            other => Self::Provider(other),
        }
    }
}
impl fmt::Display for LocalProgramCompileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(f),
            Self::Origins(error) => error.fmt(f),
            Self::Provider(error) => error.fmt(f),
            Self::Calls(error) => error.fmt(f),
            Self::Channels(error) => error.fmt(f),
            Self::Sink(error) => error.fmt(f),
            Self::TooManyDefinitions => {
                f.write_str("compiled unit table exceeds the shared definition limit")
            }
            Self::TooManyConstantBytes => {
                f.write_str("compiled unit table exceeds the shared constant backing limit")
            }
            Self::InvalidExpression => {
                f.write_str("local expression root is absent from its program unit")
            }
            Self::MissingSink => f.write_str("compiled local program requires an exact sink"),
            Self::ExchangeInputMismatch(node) => write!(
                f,
                "compiled exchange input addresses differ from the exchange sources at local node {}",
                node.index()
            ),
            Self::DuplicateExchangeReceiver(receiver) => write!(
                f,
                "compiled exchange inputs share physical receiver node {receiver}"
            ),
            Self::ScanInputMismatch(node) => write!(
                f,
                "compiled scan input addresses differ from the scan nodes at local node {}",
                node.index()
            ),
            Self::DuplicateScanNode(scan) => {
                write!(f, "compiled scan inputs share physical scan node {scan}")
            }
            Self::AggregateMismatch(node) => write!(
                f,
                "compiled aggregate facts differ from the aggregate nodes at local node {}",
                node.index()
            ),
            Self::PartialFinalization(node) => write!(
                f,
                "finalizing aggregate at local node {} declares a partial grouping",
                node.index()
            ),
        }
    }
}
impl std::error::Error for LocalProgramCompileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(error) => Some(error),
            Self::Origins(error) => Some(error),
            Self::Provider(error) => Some(error),
            Self::Calls(error) => Some(error),
            Self::Channels(error) => Some(error),
            Self::Sink(error) => Some(error),
            Self::TooManyDefinitions
            | Self::TooManyConstantBytes
            | Self::InvalidExpression
            | Self::MissingSink
            | Self::ExchangeInputMismatch(_)
            | Self::DuplicateExchangeReceiver(_)
            | Self::ScanInputMismatch(_)
            | Self::DuplicateScanNode(_)
            | Self::AggregateMismatch(_)
            | Self::PartialFinalization(_) => None,
        }
    }
}
impl<H> From<LocalProgramCompileError>
    for crate::ProgramCreationFailure<LocalProgramCompileError, H>
{
    fn from(error: LocalProgramCompileError) -> Self {
        match error {
            LocalProgramCompileError::Control(error) => Self::Control(error),
            other => Self::Owner(other),
        }
    }
}
impl LocalProgram {
    /// Build the final product from one graph and its actual compiled units.
    /// Relation and channel authors borrow those exact inputs before publication.
    pub fn try_new(
        graph: LocalProgramGraph,
        units: Vec<std::sync::Arc<novarocks_expression::CompiledExprSet>>,
        calls: Vec<(ProgramCallSite, novarocks_functions::PureCallSpecialization)>,
        channels: Vec<(
            crate::ProgramChannelSite,
            novarocks_type_contract::FunctionValueType,
        )>,
        operators: Vec<LocalOperatorProvenance>,
        allowed_sources: &BTreeSet<DiagnosticSourceNodeId>,
        facts: CompiledProgramFacts,
        control: &dyn PureCompileControl,
    ) -> Result<Self, LocalProgramCompileError> {
        Self::try_new_core::<std::convert::Infallible, crate::creation::NoFactory>(
            graph,
            units,
            calls,
            channels,
            operators,
            allowed_sources,
            facts,
            None,
            None,
            control,
        )
        .map_err(|error| match error {
            crate::ProgramCreationFailure::Owner(error) => error,
            crate::ProgramCreationFailure::Control(error) => {
                LocalProgramCompileError::Control(error)
            }
            crate::ProgramCreationFailure::Resource(_)
            | crate::ProgramCreationFailure::Capacity(_) => {
                LocalProgramCompileError::Control(CompileControlError::ResourceExhausted)
            }
            crate::ProgramCreationFailure::Creator(never) => match never {},
        })
    }
    pub fn try_new_with_creator_in<H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>(
        graph: LocalProgramGraph,
        units: Vec<std::sync::Arc<novarocks_expression::CompiledExprSet>>,
        calls: Vec<(ProgramCallSite, novarocks_functions::PureCallSpecialization)>,
        channels: Vec<(
            crate::ProgramChannelSite,
            novarocks_type_contract::FunctionValueType,
        )>,
        operators: Vec<LocalOperatorProvenance>,
        allowed_sources: &BTreeSet<DiagnosticSourceNodeId>,
        facts: CompiledProgramFacts,
        temporary: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        retained_factory: &F,
        control: &dyn PureCompileControl,
    ) -> Result<Self, crate::ProgramCreationFailure<LocalProgramCompileError, H>> {
        Self::try_new_core(
            graph,
            units,
            calls,
            channels,
            operators,
            allowed_sources,
            facts,
            Some(temporary),
            Some(retained_factory),
            control,
        )
    }
    fn try_new_core<H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>(
        graph: LocalProgramGraph,
        units: Vec<std::sync::Arc<novarocks_expression::CompiledExprSet>>,
        calls: Vec<(ProgramCallSite, novarocks_functions::PureCallSpecialization)>,
        channels: Vec<(
            crate::ProgramChannelSite,
            novarocks_type_contract::FunctionValueType,
        )>,
        operators: Vec<LocalOperatorProvenance>,
        allowed_sources: &BTreeSet<DiagnosticSourceNodeId>,
        facts: CompiledProgramFacts,
        mut temporary: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
        retained_factory: Option<&F>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, crate::ProgramCreationFailure<LocalProgramCompileError, H>> {
        let CompiledProgramFacts {
            writes,
            exchange_inputs,
            scan_inputs,
            aggregates,
        } = facts;
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
        work.step()?;
        work.flush()?;
        if graph.sink().is_none() {
            return Err(LocalProgramCompileError::MissingSink.into());
        }
        {
            let creator = temporary.as_mut().map(|scope| {
                &mut **scope as &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>
            });
            let mut creation = crate::creation::ProgramCreation { creator };
            validate_unit_budgets(&units, &mut creation, &mut work)?;
        }
        for node in graph.nodes() {
            node.kind().try_for_each_expression(|reference, demand| {
                work.step()?;
                if reference.is_some_and(|r| {
                    crate::unit_refs::root_type(&units, r).is_none()
                        || units[r.unit.index()]
                            .root(r.root)
                            .is_none_or(|root| root.demand != demand)
                }) {
                    return Err(LocalProgramCompileError::InvalidExpression);
                }
                Ok(())
            })?;
        }
        let sink = graph
            .sink()
            .expect("the actual graph sink was required above");
        sink.validate_core::<LocalProgramCompileError>(&units, &mut || {
            work.step().map_err(LocalProgramCompileError::Control)
        })?;
        // The previous checked root author never admitted executable split
        // predicates. Table migration does not expand this compiled capability.
        if !sink.split_exprs().is_empty() {
            return Err(LocalProgramCompileError::InvalidExpression.into());
        }
        crate::unit_inputs::validate(
            graph.nodes(),
            graph.root(),
            graph.sink(),
            &units,
            || work.step().map_err(LocalProgramCompileError::Control),
            || LocalProgramCompileError::InvalidExpression,
        )?;
        work.flush()?;
        let (calls, channels, units, unit_storage) = match retained_factory {
            Some(factory) => {
                let temporary = temporary.expect("strong entry borrows its actual temporary scope");
                let mut retained = factory
                    .begin_scope()
                    .map_err(crate::ProgramCreationFailure::Creator)?;
                let calls = ProgramResolvedCalls::try_new_with_creator_in(
                    &graph,
                    &units,
                    calls,
                    temporary,
                    &mut retained,
                    control,
                )
                .map_err(|e| e.map_owner(LocalProgramCompileError::from))?;
                let channels = ProgramTypedChannels::try_new_with_creator_in(
                    &graph,
                    &units,
                    &calls,
                    channels,
                    &mut retained,
                    control,
                )
                .map_err(|e| e.map_owner(LocalProgramCompileError::from))?;
                let mut creation = crate::creation::ProgramCreation {
                    creator: Some(&mut retained),
                };
                let storage = creation.storage();
                let mut published =
                    creation.reserve::<LocalProgramCompileError, _>(units.len(), &mut work)?;
                for unit in units {
                    work.step()?;
                    published.push(unit);
                }
                (calls, channels, published.into_boxed_slice(), storage)
            }
            None => {
                let calls = ProgramResolvedCalls::try_new(&graph, &units, calls, control)
                    .map_err(LocalProgramCompileError::from)?;
                let channels =
                    ProgramTypedChannels::try_new(&graph, &units, &calls, channels, control)
                        .map_err(LocalProgramCompileError::from)?;
                (
                    calls,
                    channels,
                    units.into_boxed_slice(),
                    novarocks_type_contract::ValueTypeStorageLifetime::pure(),
                )
            }
        };
        let provenance =
            crate::compiled_origins::compile_origins(&graph, operators, allowed_sources, control)
                .map_err(LocalProgramCompileError::from)?;
        crate::provider_links::validate_provider_links(&graph, &channels, &writes, control)
            .map_err(LocalProgramCompileError::from)?;
        validate_exchange_inputs(&graph, &exchange_inputs, control)?;
        validate_scan_inputs(&graph, &scan_inputs, control)?;
        validate_aggregates(&graph, &aggregates, control)?;
        Ok(Self {
            graph,
            units,
            _unit_storage: unit_storage,
            calls,
            channels,
            provenance,
            writes,
            exchange_inputs,
            scan_inputs,
            aggregates,
        })
    }
    pub fn graph(&self) -> &LocalProgramGraph {
        &self.graph
    }
    pub fn units(&self) -> &[std::sync::Arc<novarocks_expression::CompiledExprSet>] {
        &self.units
    }
    pub fn unit(
        &self,
        id: crate::ProgramUnitId,
    ) -> Option<&std::sync::Arc<novarocks_expression::CompiledExprSet>> {
        self.units.get(id.index())
    }
    pub fn resolved_calls(&self) -> &ProgramResolvedCalls {
        &self.calls
    }
    pub fn channels(&self) -> &ProgramTypedChannels {
        &self.channels
    }
    pub const fn provenance(&self) -> &ProgramProvenance {
        &self.provenance
    }
    pub fn write_recipes(&self) -> &BTreeMap<ProgramNodeId, ConnectorWriteRecipe> {
        &self.writes
    }
    pub fn exchange_inputs(&self) -> &BTreeMap<ProgramNodeId, CompiledExchangeInput> {
        &self.exchange_inputs
    }
    pub fn scan_inputs(&self) -> &BTreeMap<ProgramNodeId, CompiledScanInput> {
        &self.scan_inputs
    }
    pub fn aggregates(&self) -> &BTreeMap<ProgramNodeId, CompiledAggregate> {
        &self.aggregates
    }
    pub fn state_template(&self, site: ProgramCallSite) -> Option<ProgramStateTemplate<'_>> {
        self.calls.calls().get(&site).map(|c| c.state_template())
    }
}

/// Preserve the previous fragment-wide definition and constant-backing axes.
/// Unit table positions are definitions even when their Arc is shared. Constant
/// storage uses the original immutable pool's array identity and public facts.
fn validate_unit_budgets<H>(
    units: &[std::sync::Arc<novarocks_expression::CompiledExprSet>],
    temporary: &mut crate::creation::ProgramCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), crate::ProgramCreationFailure<LocalProgramCompileError, H>> {
    let mut definitions = 0usize;
    for unit in units {
        work.step()?;
        definitions = definitions
            .checked_add(unit.definitions().nodes().len())
            .filter(|count| *count <= novarocks_expression::MAX_STATIC_EXPRESSIONS)
            .ok_or(LocalProgramCompileError::TooManyDefinitions)?;
    }
    let mut arrays = BTreeSet::new();
    let mut bytes = 0usize;
    for unit in units {
        work.step()?;
        for definition in unit.definitions().nodes() {
            work.step()?;
            if let novarocks_expression::StaticExprKind::Constant(value) = definition.kind() {
                let array = std::sync::Arc::as_ptr(value.pool().array()) as *const () as usize;
                work.step()?;
                if !arrays.contains(&array) {
                    temporary.before_tree_insert::<LocalProgramCompileError, usize, ()>(
                        arrays.len(),
                        work,
                    )?;
                    arrays.insert(array);
                    let facts = value.pool().resource_facts();
                    let retained = facts
                        .retained_buffer_capacity_bytes
                        .checked_add(facts.metadata_bytes)
                        .and_then(|bytes| usize::try_from(bytes).ok())
                        .ok_or(LocalProgramCompileError::TooManyConstantBytes)?;
                    bytes = bytes
                        .checked_add(retained)
                        .filter(|bytes| {
                            *bytes <= novarocks_expression::MAX_STATIC_EXPRESSION_DYNAMIC_BYTES
                        })
                        .ok_or(LocalProgramCompileError::TooManyConstantBytes)?;
                    work.step()?;
                }
            }
        }
    }
    work.flush()?;
    Ok(())
}

/// One compiled-only source address family: which nodes it addresses, which
/// requirement declares such a node, the physical identity that must be unique
/// and the errors naming a coverage gap or a shared physical identity.
struct AddressFamily<A> {
    addressed: fn(&ProgramNodeKind) -> bool,
    declared: fn(&BindingRequirement) -> Option<ProgramNodeId>,
    physical: fn(&A) -> u32,
    mismatch: fn(ProgramNodeId) -> LocalProgramCompileError,
    duplicate: fn(u32) -> LocalProgramCompileError,
}

/// Require one address per actual exchange receiver and per declared exchange
/// input, and no address elsewhere. A first control refusal stays primary.
fn validate_exchange_inputs(
    graph: &LocalProgramGraph,
    exchange_inputs: &BTreeMap<ProgramNodeId, CompiledExchangeInput>,
    control: &dyn PureCompileControl,
) -> Result<(), LocalProgramCompileError> {
    validate_addresses(
        graph,
        exchange_inputs,
        AddressFamily {
            addressed: |kind| matches!(kind, ProgramNodeKind::ExchangeSource { .. }),
            declared: |requirement| match requirement {
                BindingRequirement::ExchangeInput { node, .. } => Some(*node),
                _ => None,
            },
            physical: |input| input.receiver_node,
            mismatch: LocalProgramCompileError::ExchangeInputMismatch,
            duplicate: LocalProgramCompileError::DuplicateExchangeReceiver,
        },
        control,
    )?;
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)
        .map_err(LocalProgramCompileError::Control)?;
    let result = (|| {
        let mut declarations = BTreeMap::new();
        for declared in graph.requirements().entries() {
            if let BindingRequirement::ExchangeInput {
                node,
                role,
                delivery,
                ..
            } = declared
            {
                declarations.insert(*node, (*role, *delivery));
            }
            work.step().map_err(LocalProgramCompileError::Control)?;
        }
        for (id, input) in exchange_inputs {
            let layout = graph.nodes()[id.index()].output_layout();
            let transport = input.routing_layout.as_ref().unwrap_or(layout);
            let slots = transport.slots();
            validate_exchange_wire_layout(*id, &input.wire_layout, transport, &mut work)?;
            if input.routing_layout.is_some() {
                let [key] = input.hash_partition_slots.as_ref() else {
                    return Err(LocalProgramCompileError::ExchangeInputMismatch(*id));
                };
                let prefix = layout.slots().len();
                let exact = slots.len() == prefix + 1
                    && slots.starts_with(layout.slots())
                    && slots.last() == Some(key)
                    && matches!(input.routing, CompiledExchangeRouting::Hash {
                        algorithm: novarocks_type_contract::PartitionHashAlgorithm::NativeHierarchicalV1
                    });
                if !exact
                    || !matches!(graph.nodes()[graph.root().index()].kind(),
                    ProgramNodeKind::TableWriter { input, .. } if input == id)
                {
                    return Err(LocalProgramCompileError::ExchangeInputMismatch(*id));
                }
                validate_exchange_layout_prefix(*id, transport, layout, &mut work)?;
            }
            let requirement = declarations.get(id).copied();
            if requirement != Some((input.role, input.delivery))
                || (input.role != crate::ProgramOutputRole::Relational
                    && input.delivery
                        != novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain)
            {
                return Err(LocalProgramCompileError::ExchangeInputMismatch(*id));
            }
            let valid_routing = match input.routing {
                CompiledExchangeRouting::Unpartitioned | CompiledExchangeRouting::RoundRobin => {
                    input.hash_partition_slots.is_empty()
                }
                CompiledExchangeRouting::Hash { algorithm } => {
                    !input.hash_partition_slots.is_empty()
                        && matches!(
                        algorithm,
                        novarocks_type_contract::PartitionHashAlgorithm::NativeExchangeV1
                            | novarocks_type_contract::PartitionHashAlgorithm::NativeExchangeV2
                            | novarocks_type_contract::PartitionHashAlgorithm::NativeHierarchicalV1
                    )
                }
                CompiledExchangeRouting::BucketShuffle { algorithm } => {
                    !input.hash_partition_slots.is_empty()
                        && algorithm
                            == novarocks_type_contract::PartitionHashAlgorithm::NativeBucketCrc32V1
                }
                CompiledExchangeRouting::ConditionalKey { applies, algorithm } => {
                    let mut ordinal = None;
                    for (index, slot) in slots.iter().enumerate() {
                        if *slot == applies {
                            ordinal = Some(index);
                        }
                        work.step().map_err(LocalProgramCompileError::Control)?;
                    }
                    !input.hash_partition_slots.is_empty()
                        && algorithm
                            == novarocks_type_contract::PartitionHashAlgorithm::NativeHierarchicalV1
                        && ordinal.is_some_and(|i| {
                            layout.schema().field(i).data_type() == &arrow_schema::DataType::Boolean
                        })
                }
            };
            work.step().map_err(LocalProgramCompileError::Control)?;
            if !valid_routing {
                return Err(LocalProgramCompileError::ExchangeInputMismatch(*id));
            }
            let algorithm = match input.routing {
                CompiledExchangeRouting::Hash { algorithm }
                | CompiledExchangeRouting::ConditionalKey { algorithm, .. }
                | CompiledExchangeRouting::BucketShuffle { algorithm } => Some(algorithm),
                _ => None,
            };
            if let crate::ProgramOutputRole::Summary(kind) = input.role {
                let valid = matches!(input.routing, CompiledExchangeRouting::Unpartitioned)
                    && crate::validate_summary_layout(kind, layout).is_ok();
                work.step().map_err(LocalProgramCompileError::Control)?;
                if !valid {
                    return Err(LocalProgramCompileError::ExchangeInputMismatch(*id));
                }
            }
            for key in &input.hash_partition_slots {
                let mut occurrences = 0usize;
                let mut supported = false;
                for (ordinal, slot) in slots.iter().enumerate() {
                    if slot == key {
                        occurrences += 1;
                        supported = algorithm.is_some_and(|algorithm| {
                            let data_type = transport.schema().field(ordinal).data_type();
                            if let Some((semantic, _)) = transport.slot_metadata_at(ordinal) {
                                algorithm.supports_partition_binding(semantic, data_type)
                            } else {
                                algorithm.supports_partition_key(data_type)
                            }
                        });
                    }
                    work.step().map_err(LocalProgramCompileError::Control)?;
                }
                if occurrences != 1 || !supported {
                    return Err(LocalProgramCompileError::ExchangeInputMismatch(*id));
                }
            }
        }
        Ok(())
    })();
    if matches!(result, Err(LocalProgramCompileError::Control(_))) {
        return result;
    }
    work.finish().map_err(LocalProgramCompileError::Control)?;
    result
}

/// The wire retains the source's complete binding; only its root NULL promise
/// may widen at the receiver. Names and source-fragment slot IDs are not wire identity.
fn validate_exchange_wire_layout(
    id: ProgramNodeId,
    wire: &crate::StaticLayout,
    local: &crate::StaticLayout,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), LocalProgramCompileError> {
    if !wire.is_typed() || !local.is_typed() || wire.slots() != local.slots() {
        return Err(LocalProgramCompileError::ExchangeInputMismatch(id));
    }
    validate_exchange_layout_prefix(id, wire, local, work)
}

fn validate_exchange_layout_prefix(
    id: ProgramNodeId,
    wire: &crate::StaticLayout,
    local: &crate::StaticLayout,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), LocalProgramCompileError> {
    for ordinal in 0..local.slots().len() {
        work.step().map_err(LocalProgramCompileError::Control)?;
        let source = wire
            .value_binding_at(ordinal)
            .ok_or(LocalProgramCompileError::ExchangeInputMismatch(id))?;
        let destination = local
            .value_binding_at(ordinal)
            .ok_or(LocalProgramCompileError::ExchangeInputMismatch(id))?;
        if (source.nullable() && !destination.nullable())
            || !source
                .borrowed()
                .same_physical_binding_ignoring_root_nullability_observed(
                    destination.borrowed(),
                    || work.step().map_err(LocalProgramCompileError::Control),
                )?
        {
            return Err(LocalProgramCompileError::ExchangeInputMismatch(id));
        }
    }
    Ok(())
}

/// Require one address per actual scan and per declared scan requirement, and
/// no address elsewhere. A first control refusal stays primary.
fn validate_scan_inputs(
    graph: &LocalProgramGraph,
    scan_inputs: &BTreeMap<ProgramNodeId, CompiledScanInput>,
    control: &dyn PureCompileControl,
) -> Result<(), LocalProgramCompileError> {
    validate_addresses(
        graph,
        scan_inputs,
        AddressFamily {
            addressed: |kind| matches!(kind, ProgramNodeKind::Scan { .. }),
            declared: |requirement| match requirement {
                BindingRequirement::Scan { node, .. } => Some(*node),
                _ => None,
            },
            physical: |input| input.scan_node,
            mismatch: LocalProgramCompileError::ScanInputMismatch,
            duplicate: LocalProgramCompileError::DuplicateScanNode,
        },
        control,
    )
}

/// Require one fact per actual `Aggregate` node and no fact elsewhere. A node
/// with finalizing calls emits final results, which the physical contract
/// admits only under a complete grouping. A first control refusal stays
/// primary.
fn validate_aggregates(
    graph: &LocalProgramGraph,
    aggregates: &BTreeMap<ProgramNodeId, CompiledAggregate>,
    control: &dyn PureCompileControl,
) -> Result<(), LocalProgramCompileError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)
        .map_err(LocalProgramCompileError::Control)?;
    let result = validate_aggregates_core(graph, aggregates, &mut work);
    if matches!(result, Err(LocalProgramCompileError::Control(_))) {
        return result;
    }
    work.finish().map_err(LocalProgramCompileError::Control)?;
    result
}

fn validate_aggregates_core(
    graph: &LocalProgramGraph,
    aggregates: &BTreeMap<ProgramNodeId, CompiledAggregate>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), LocalProgramCompileError> {
    for (index, node) in graph.nodes().iter().enumerate() {
        let id = ProgramNodeId::new(index);
        let fact = aggregates.get(&id);
        work.step().map_err(LocalProgramCompileError::Control)?;
        match (node.kind(), fact) {
            (
                ProgramNodeKind::Aggregate {
                    functions,
                    need_finalize,
                    ..
                },
                Some(fact),
            ) => {
                if *need_finalize
                    && !functions.is_empty()
                    && fact.grouping == CompiledAggregateGrouping::Partial
                {
                    return Err(LocalProgramCompileError::PartialFinalization(id));
                }
            }
            (ProgramNodeKind::Aggregate { .. }, None) | (_, Some(_)) => {
                return Err(LocalProgramCompileError::AggregateMismatch(id));
            }
            (_, None) => {}
        }
    }
    for id in aggregates.keys() {
        let present = id.index() < graph.nodes().len();
        work.step().map_err(LocalProgramCompileError::Control)?;
        if !present {
            return Err(LocalProgramCompileError::AggregateMismatch(*id));
        }
    }
    Ok(())
}

fn validate_addresses<A>(
    graph: &LocalProgramGraph,
    addresses: &BTreeMap<ProgramNodeId, A>,
    family: AddressFamily<A>,
    control: &dyn PureCompileControl,
) -> Result<(), LocalProgramCompileError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)
        .map_err(LocalProgramCompileError::Control)?;
    let result = validate_addresses_core(graph, addresses, &family, &mut work);
    if matches!(result, Err(LocalProgramCompileError::Control(_))) {
        return result;
    }
    work.finish().map_err(LocalProgramCompileError::Control)?;
    result
}

fn validate_addresses_core<A>(
    graph: &LocalProgramGraph,
    addresses: &BTreeMap<ProgramNodeId, A>,
    family: &AddressFamily<A>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), LocalProgramCompileError> {
    let mut required = BTreeSet::new();
    for requirement in graph.requirements().entries() {
        if let Some(node) = (family.declared)(requirement) {
            required.insert(node);
        }
        work.step().map_err(LocalProgramCompileError::Control)?;
    }
    // A requirement naming no actual node cannot be hidden by the node walk.
    for node in &required {
        let present = node.index() < graph.nodes().len();
        work.step().map_err(LocalProgramCompileError::Control)?;
        if !present {
            return Err((family.mismatch)(*node));
        }
    }
    let mut physical = BTreeSet::new();
    for (index, node) in graph.nodes().iter().enumerate() {
        let id = ProgramNodeId::new(index);
        let addressed = (family.addressed)(node.kind());
        let declared = required.contains(&id);
        let address = addresses.get(&id);
        work.step().map_err(LocalProgramCompileError::Control)?;
        match (addressed, declared, address) {
            (true, true, Some(input)) => {
                let identity = (family.physical)(input);
                let unique = physical.insert(identity);
                work.step().map_err(LocalProgramCompileError::Control)?;
                if !unique {
                    return Err((family.duplicate)(identity));
                }
            }
            (false, false, None) => {}
            _ => return Err((family.mismatch)(id)),
        }
    }
    // An address for a node outside the graph cannot be hidden either.
    for id in addresses.keys() {
        let present = id.index() < graph.nodes().len();
        work.step().map_err(LocalProgramCompileError::Control)?;
        if !present {
            return Err((family.mismatch)(*id));
        }
    }
    Ok(())
}

#[cfg(test)]
mod kernel_tests;
#[cfg(test)]
mod tests;
