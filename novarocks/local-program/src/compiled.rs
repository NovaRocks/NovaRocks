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

//! Final pure compilation product. The graph is owned exactly once by the
//! mandatory checked expression/control/type/channel/lexical chain. Provider
//! recipes and provenance are checked against that same graph before exposure.
//! Execution's construction bridge consumes LocalProgramGraph separately and
//! must be retired when the production compiler is connected.

use crate::{
    BindingRequirement, CompiledOriginsError, DiagnosticSourceNodeId, LocalOperatorProvenance,
    LocalProgramGraph, ProgramCallSite, ProgramComparisonSite, ProgramLexicalBindings,
    ProgramNodeId, ProgramNodeKind, ProgramPrimitiveError, ProgramProvenance, ProgramStateTemplate,
    ProviderLinkError,
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
    expressions: std::sync::Arc<crate::ExpressionProgram>,
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
    Primitive(ProgramPrimitiveError),
}
impl From<ProgramPrimitiveError> for LocalProgramCompileError {
    fn from(error: ProgramPrimitiveError) -> Self {
        match error {
            ProgramPrimitiveError::Control(cause) => Self::Control(cause),
            other => Self::Primitive(other),
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
            Self::Primitive(error) => error.fmt(f),
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
            Self::Primitive(error) => Some(error),
            Self::MissingSink
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
    /// Accept only the complete checked chain, not an AST and optional receipts.
    /// Provenance is freshly authored against its actual local graph. Compiler
    /// lowering remains responsible for physical-token/source correspondence,
    /// output guarantees and the existence of synthetic non-node entities.
    /// `exchange_inputs` addresses exactly the graph's `ExchangeSource` nodes,
    /// which are exactly its `ExchangeInput` requirements; receivers are unique.
    /// `scan_inputs` likewise addresses exactly its `Scan` nodes, which are
    /// exactly its `Scan` requirements; physical scan nodes are unique.
    /// `aggregates` addresses exactly its `Aggregate` nodes, and a node whose
    /// calls finalize declares a complete grouping.
    pub fn try_new(
        checked: ProgramLexicalBindings,
        operators: Vec<LocalOperatorProvenance>,
        allowed_sources: &BTreeSet<DiagnosticSourceNodeId>,
        facts: CompiledProgramFacts,
        control: &dyn PureCompileControl,
    ) -> Result<Self, LocalProgramCompileError> {
        Self::try_new_core::<std::convert::Infallible, crate::creation::NoFactory>(
            checked,
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
    pub fn try_new_with_creator_in<H, F>(
        checked: ProgramLexicalBindings,
        operators: Vec<LocalOperatorProvenance>,
        allowed_sources: &BTreeSet<DiagnosticSourceNodeId>,
        facts: CompiledProgramFacts,
        temporary: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        retained_factory: &F,
        control: &dyn PureCompileControl,
    ) -> Result<Self, crate::ProgramCreationFailure<LocalProgramCompileError, H>>
    where
        F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
    {
        Self::try_new_core(
            checked,
            operators,
            allowed_sources,
            facts,
            Some(temporary),
            Some(retained_factory),
            control,
        )
    }
    fn try_new_core<H, F>(
        checked: ProgramLexicalBindings,
        operators: Vec<LocalOperatorProvenance>,
        allowed_sources: &BTreeSet<DiagnosticSourceNodeId>,
        facts: CompiledProgramFacts,
        temporary: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
        retained_factory: Option<&F>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, crate::ProgramCreationFailure<LocalProgramCompileError, H>>
    where
        F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
    {
        let CompiledProgramFacts {
            writes,
            exchange_inputs,
            scan_inputs,
            aggregates,
        } = facts;
        let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
            control,
            novarocks_type_contract::CompilePhase::LowerProgram,
        )
        .map_err(LocalProgramCompileError::Control)?;
        let graph = checked
            .channels()
            .expressions()
            .resolved_calls()
            .snapshot()
            .program();
        let has_sink = graph.sink().is_some();
        work.step().map_err(LocalProgramCompileError::Control)?;
        work.flush().map_err(LocalProgramCompileError::Control)?;
        if !has_sink {
            return Err(LocalProgramCompileError::MissingSink.into());
        }
        let provenance =
            crate::compiled_origins::compile_origins(graph, operators, allowed_sources, control)
                .map_err(LocalProgramCompileError::from)?;
        crate::provider_links::validate_provider_links(&checked, &writes, control)
            .map_err(LocalProgramCompileError::from)?;
        validate_exchange_inputs(graph, &exchange_inputs, control)?;
        validate_scan_inputs(graph, &scan_inputs, control)?;
        validate_aggregates(graph, &aggregates, control)?;
        let expressions = match retained_factory {
            Some(factory) => crate::ExpressionProgram::try_new_with_creator_in(
                checked,
                temporary.expect("strong entry supplies the real temporary scope"),
                factory,
                control,
            )
            .map_err(|error| error.map_owner(LocalProgramCompileError::from))?,
            None => crate::ExpressionProgram::try_new(checked, control)
                .map_err(LocalProgramCompileError::from)?,
        };
        // Each delegated author finishes its completed work and propagates a
        // first control refusal directly; no control object enters the product.
        Ok(Self {
            expressions,
            provenance,
            writes,
            exchange_inputs,
            scan_inputs,
            aggregates,
        })
    }
    pub fn checked(&self) -> &ProgramLexicalBindings {
        self.expressions.checked()
    }
    pub fn graph(&self) -> &LocalProgramGraph {
        self.expressions.graph()
    }
    pub const fn provenance(&self) -> &ProgramProvenance {
        &self.provenance
    }
    pub fn write_recipes(&self) -> &BTreeMap<ProgramNodeId, ConnectorWriteRecipe> {
        &self.writes
    }
    /// Exact receiver address of every `ExchangeSource` node in this program.
    pub fn exchange_inputs(&self) -> &BTreeMap<ProgramNodeId, CompiledExchangeInput> {
        &self.exchange_inputs
    }
    /// Exact physical scan address of every `Scan` node in this program.
    pub fn scan_inputs(&self) -> &BTreeMap<ProgramNodeId, CompiledScanInput> {
        &self.scan_inputs
    }
    /// Exact grouping guarantee of every `Aggregate` node in this program.
    pub fn aggregates(&self) -> &BTreeMap<ProgramNodeId, CompiledAggregate> {
        &self.aggregates
    }
    pub fn expressions(&self) -> &std::sync::Arc<crate::ExpressionProgram> {
        &self.expressions
    }
    pub fn comparison_recipe(
        &self,
        site: ProgramComparisonSite,
    ) -> Option<&novarocks_functions::PreparedComparisonRecipe> {
        self.expressions.comparison_recipe(site)
    }
    pub fn null_safe_comparison_recipe(
        &self,
        site: crate::ProgramUseRef,
    ) -> Option<&novarocks_functions::PreparedNullSafeComparisonRecipe> {
        self.expressions.null_safe_comparison_recipe(site)
    }
    pub fn native_like_recipe(
        &self,
        site: crate::ProgramUseRef,
    ) -> Option<&novarocks_functions::PreparedNativeLikeRecipe> {
        self.expressions.native_like_recipe(site)
    }
    pub fn native_inlist_recipe(
        &self,
        site: crate::ProgramUseRef,
    ) -> Option<&novarocks_functions::PreparedNativeInListRecipe> {
        self.expressions.native_inlist_recipe(site)
    }
    pub fn native_negate_recipe(
        &self,
        site: crate::ProgramUseRef,
    ) -> Option<&novarocks_functions::PreparedNativeNegateRecipe> {
        self.expressions.native_negate_recipe(site)
    }
    pub fn native_bitnot_recipe(
        &self,
        site: crate::ProgramUseRef,
    ) -> Option<&novarocks_functions::PreparedNativeBitNotRecipe> {
        self.expressions.native_bitnot_recipe(site)
    }
    pub fn arithmetic_recipe(
        &self,
        site: crate::ProgramUseRef,
    ) -> Option<&novarocks_functions::PreparedArithmeticRecipe> {
        self.expressions.arithmetic_recipe(site)
    }
    pub fn cast_recipe(
        &self,
        site: crate::ProgramUseRef,
    ) -> Option<&novarocks_functions::PreparedCastRecipe> {
        self.expressions.cast_recipe(site)
    }
    /// Borrow the same expression owner's exact selected lifecycle.
    pub fn state_template(&self, site: ProgramCallSite) -> Option<ProgramStateTemplate<'_>> {
        self.expressions.state_template(site)
    }
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
            let slots = layout.slots();
            validate_exchange_wire_layout(*id, &input.wire_layout, layout, &mut work)?;
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
                            algorithm
                                .supports_partition_key(layout.schema().field(ordinal).data_type())
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
mod read_tests;
#[cfg(test)]
mod tests;
