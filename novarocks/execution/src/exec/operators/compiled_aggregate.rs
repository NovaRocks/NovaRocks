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

//! Compiled hash aggregation.
//!
//! One driver owns one processor: an instance per group and argument root, one
//! group-key table and one owned state column per call. Every arriving row's
//! group keys and arguments are evaluated once by the driver's compiled roots;
//! a row data error of any root is a required error. Each call runs through
//! the exact prepared aggregate handle the compiler attached to its site, so
//! no implementation is selected by name and no legacy expression is read.
//!
//! Group-key equivalence has one owner, the shared `KeyIndex`: NULL keys
//! form one group, every NaN of a float key is one group, and `-0.0` and
//! `+0.0` are one group. The table's first stored key represents its group.
//!
//! Grouping is full hash aggregation that emits every group once at finish,
//! in group creation order, into exactly the node's frozen output layout.
//! A group-less aggregate owns one group from its first input, or from
//! finishing on empty input, so each driver emits exactly one row. The
//! builder places a Complete aggregate on one driver; a Partial aggregate
//! runs per driver and leaves merging to its later phase.
//!
//! State backing comes from a host allocator charged to the operator's memory
//! tracker. Owner-reported retained heap inside a state is not reconciled yet;
//! the formal memory grant remains an open host obligation.

use crate::exec::fragment::error::{
    FragmentBindingErrorKind as PlanBindingKind, program_binding_failure,
};
use crate::exec::pipeline::operator::OperatorLifecycle;
use std::alloc::Layout;
use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow::array::ArrayRef;
#[cfg(test)]
use arrow::array::RecordBatch;
#[cfg(test)]
use arrow::datatypes::DataType;
use novarocks_functions::{
    AggregateCallContract, AggregateStateAllocator, AggregateStateColumn, EvaluatedArgument,
    FunctionValueType, KernelDiagnostic, KernelFailure, PreparedAggregateHandle,
    SelectedAggregateMergeInput, SelectedAggregateUpdateInput, Selection,
};
use novarocks_local_program::{
    CompiledAggregateGrouping, LocalProgram, ProgramCallSite, ProgramExpressionRootSite,
    ProgramNodeExpressionRole, ProgramNodeId, ProgramNodeKind, ProgramStateTemplate,
};

use super::compiled_aggregate_topn::{CompiledAggregateTopN, CompiledAggregateTopNFactory};
use super::compiled_expression::{RuntimeKernelControl, evaluate_all, instances, root_value_type};
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
use crate::exec::expr::compiled_program::CompiledExpressionInstance;
use crate::exec::hash_table::allocation::AggregateAllocator;
use crate::exec::hash_table::key_index::{
    IndexPurpose, KeyIndex, KeyInput, KeyMatch, KeySpec, NullPolicy, PreparedKeyBatch, hash_batch,
};
use crate::exec::pipeline::operator::{
    ConstructionFailure, InputConstructionFactory, InputConstructionOwner, InputEncodingPolicy,
    Operator,
};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::{
    input::PreparedInput,
    shape::{Transform, TransformKernel},
    step::{NextStep, StepResult},
    step_context::StepContext,
};
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};
use crate::runtime_filter::{
    OrderedTopNAllocationHost, RuntimeFilterProducerFailure, RuntimeFilterSessionRef,
};
use novarocks_value_arrays::{
    ValueFailure, batch::hash::HashScratch, decode::ValueConversionContext,
    reachability::ReachableRows,
};

/// Rows per emitted chunk; the whole result is materialized at finish.
const OUTPUT_CHUNK_ROWS: usize = 4096;
/// Target bytes of one state backing block for a grouped aggregate.
const STATE_BLOCK_BYTES: usize = 64 * 1024;
const MAX_STATES_PER_BLOCK: usize = 4096;

#[derive(Debug)]
struct AggregateGroupContractFailure(novarocks_failure_contract::BoundedDetail);
impl std::fmt::Display for AggregateGroupContractFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for AggregateGroupContractFailure {}
impl novarocks_failure_contract::FailureProjection for AggregateGroupContractFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            crate::runtime::fragment::ExecutionReason::AggregateGroupContract.reason(),
        )
    }
}
fn group_contract(detail: impl std::fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from_domain(AggregateGroupContractFailure(
        novarocks_failure_contract::BoundedDetail::truncating_display(detail),
    ))
}

fn aggregate_vector<T>(
    owner: &dyn InputConstructionOwner,
    count: usize,
) -> ExecutionResult<Vec<T>> {
    let layout = Layout::array::<T>(count).map_err(|_| {
        ConstructionFailure::Resource(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
        )
    })?;
    if layout.size() != 0 {
        owner.request(layout)?;
    }
    let mut values = Vec::new();
    values.try_reserve_exact(count).map_err(|source| {
        ExecutionFailure::from_domain(novarocks_value_arrays::ValueError::Allocation {
            requested: layout.size(),
            source,
        })
    })?;
    Ok(values)
}
fn aggregate_value_failure<E: Into<ExecutionFailure>>(cause: ValueFailure<E>) -> ExecutionFailure {
    match cause {
        ValueFailure::Value(cause) => ExecutionFailure::from_domain(cause),
        ValueFailure::Caller(cause) => cause.into(),
    }
}

use crate::runtime::tracked_state_allocator::TrackedStateAllocator;

/// One call's exact prepared handle and the roots that feed it: the logical
/// arguments of an update phase, or the one state input of a merge phase.
#[derive(Clone)]
struct CallPlan {
    handle: PreparedAggregateHandle,
    /// The handle's own contract allocation; a frame input borrows it while
    /// the call's state column is mutably borrowed.
    contract: Arc<AggregateCallContract>,
    merge: bool,
    /// Offset of this call's roots in the processor's root list.
    first_root: usize,
    roots: usize,
}

/// The source layout owns names/IDs and complete final values. Local partial
/// storage is explicitly authored by the same installed contracts, never by a
/// physical carrier or a format field marker.
#[derive(Clone, Copy)]
enum AggregateSchemaRole {
    Output,
    Intermediate,
}

fn prepare_aggregate_schema(
    factory: &dyn InputConstructionFactory,
    layout: &novarocks_local_program::StaticLayout,
    groups: usize,
    calls: &[CallPlan],
    role: AggregateSchemaRole,
) -> ExecutionResult<ChunkSchemaRef> {
    if matches!(role, AggregateSchemaRole::Output) {
        return factory
            .create_compiled_chunk_schema(layout)
            .map_err(Into::into);
    }
    let owner = factory.begin_compiled_layout_construction(layout)?;
    ChunkSchema::from_compiled_layout_with_bindings_owned_observed::<ConstructionFailure>(
        layout,
        owner.storage_lifetime(),
        |index| {
            if index < groups {
                layout.value_binding_at(index)
            } else {
                calls
                    .get(index - groups)
                    .map(|call| call.contract.intermediate_type().typed_binding())
            }
        },
        |layout| owner.request(layout),
        |units| {
            owner.observe(
                0,
                &[],
                crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units },
            )
        },
    )
    .map_err(Into::into)
}

pub struct CompiledAggregateProcessorFactory {
    name: String,
    node: ProgramNodeId,
    owns_ordered_endpoint: bool,
    program: Arc<LocalProgram>,
    sites: Vec<ProgramExpressionRootSite>,
    groups: usize,
    calls: Vec<CallPlan>,
    key_types: Vec<FunctionValueType>,
    output_layout: novarocks_local_program::StaticLayout,
    output_role: AggregateSchemaRole,
    grouping: CompiledAggregateGrouping,
    direct_intermediate: bool,
    topn: Option<CompiledAggregateTopNFactory>,
    error: Arc<RuntimeErrorState>,
}

impl CompiledAggregateProcessorFactory {
    pub(crate) fn try_new(
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        error: Arc<RuntimeErrorState>,
    ) -> ExecutionResult<Self> {
        Self::try_new_parts(program, node, error, false)
    }

    #[cfg(test)]
    pub(crate) fn try_new_ordered(
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        error: Arc<RuntimeErrorState>,
        session: RuntimeFilterSessionRef,
        actual_dop: i32,
        host: &OrderedTopNAllocationHost,
        control: &RuntimeKernelControl,
    ) -> ExecutionResult<Self> {
        let mut factory = Self::prepare_ordered(program, node, error)?;
        factory.install_ordered(session, actual_dop, host, control)?;
        Ok(factory)
    }

    /// Prepare the original aggregate lifecycle without opening or retaining a
    /// producer. Local split/gather must settle before an endpoint is attached.
    pub(crate) fn prepare_ordered(
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        error: Arc<RuntimeErrorState>,
    ) -> ExecutionResult<Self> {
        Self::try_new_parts(program, node, error, true)
    }

    pub(crate) fn install_ordered(
        &mut self,
        session: RuntimeFilterSessionRef,
        actual_dop: i32,
        host: &OrderedTopNAllocationHost,
        control: &RuntimeKernelControl,
    ) -> ExecutionResult<()> {
        if !self.owns_ordered_endpoint {
            return Err(KernelFailure::InvalidProgram(KernelDiagnostic::new(
                "compiled aggregate does not own an ordered producer endpoint",
            ))
            .into());
        }
        if self.topn.is_some() {
            return Err(KernelFailure::InvalidProgram(KernelDiagnostic::new(
                "compiled Aggregate TopN producer is already installed",
            ))
            .into());
        }
        if self.grouping == CompiledAggregateGrouping::Partial
            && (self.calls.is_empty()
                || self
                    .calls
                    .iter()
                    .any(|call| call.merge || call.contract.distinct()))
        {
            return Err(KernelFailure::InvalidProgram(KernelDiagnostic::new(
                "compiled ordered Aggregate TopN requires a non-distinct logical-input Partial aggregate",
            )).into());
        }
        // Preserve an already published task failure at this installation
        // boundary before the cooperative kernel sees only a stopped state.
        control.checkpoint_execution()?;
        self.topn = Some(CompiledAggregateTopNFactory::try_new(
            &self.program,
            self.node,
            session,
            actual_dop,
            host,
            control,
        )?);
        Ok(())
    }

    fn try_new_parts(
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        error: Arc<RuntimeErrorState>,
        allow_ordered: bool,
    ) -> ExecutionResult<Self> {
        let at = node.index();
        let graph_node = program.graph().nodes().get(at).ok_or_else(|| {
            program_binding_failure(
                PlanBindingKind::MissingAssignment,
                format!("compiled Aggregate node {at} is absent"),
            )
        })?;
        let ProgramNodeKind::Aggregate {
            group_by,
            functions,
            topn_filters,
            streaming_preaggregation_mode,
            ..
        } = graph_node.kind()
        else {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                format!("compiled local node {at} is not an Aggregate"),
            ));
        };
        let owns_ordered_endpoint = !topn_filters.is_empty();
        if owns_ordered_endpoint && !allow_ordered {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                format!(
                    "compiled Aggregate at local node {at} with TopN runtime filters is not executable"
                ),
            ));
        }
        if streaming_preaggregation_mode.is_some() {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                format!(
                    "compiled Aggregate at local node {at} with streaming pre-aggregation is not executable"
                ),
            ));
        }
        let grouping = program
            .aggregates()
            .get(&node)
            .ok_or_else(|| {
                program_binding_failure(
                    PlanBindingKind::MissingAssignment,
                    format!("compiled Aggregate at local node {at} has no grouping fact"),
                )
            })?
            .grouping;
        let mut sites = Vec::new();
        for group in 0..group_by.len() {
            let group = novarocks_local_program::checked_program_ordinal(group)
                .map_err(crate::runtime::fragment::ExecutionFailure::from_domain)?;
            sites.push(ProgramExpressionRootSite::Node {
                node,
                role: ProgramNodeExpressionRole::AggregateGroup { group },
            });
        }
        let mut calls = Vec::with_capacity(functions.len());
        for (ordinal, function) in functions.iter().enumerate() {
            let call = novarocks_local_program::checked_program_ordinal(ordinal)
                .map_err(crate::runtime::fragment::ExecutionFailure::from_domain)?;
            let Some(ProgramStateTemplate::Aggregate { kernel, .. }) =
                program.state_template(ProgramCallSite::Aggregate { node, call })
            else {
                return Err(program_binding_failure(
                    PlanBindingKind::InvalidAssignment,
                    format!(
                        "compiled Aggregate call {call} at local node {at} has no prepared aggregate"
                    ),
                ));
            };
            let handle = kernel.clone();
            let contract = Arc::clone(handle.contract());
            let merge = !contract.phase().consumes_logical_arguments();
            let roots = if merge {
                1
            } else {
                contract.logical_argument_types().len() + contract.order_keys().len()
            };
            if function.inputs.len() != roots {
                return Err(program_binding_failure(
                    PlanBindingKind::InvalidAssignment,
                    format!(
                        "compiled Aggregate call {call} at local node {at} has {} roots for {roots} channels",
                        function.inputs.len()
                    ),
                ));
            }
            let first_root = sites.len();
            for argument in 0..roots {
                let argument = u32::try_from(argument).map_err(|_| {
                    program_binding_failure(
                        PlanBindingKind::InvalidAssignment,
                        format!(
                            "compiled Aggregate call {call} at local node {at} has too many inputs"
                        ),
                    )
                })?;
                sites.push(ProgramExpressionRootSite::Node {
                    node,
                    role: ProgramNodeExpressionRole::AggregateInput { call, argument },
                });
            }
            calls.push(CallPlan {
                handle,
                contract,
                merge,
                first_root,
                roots,
            });
        }
        let output_layout = graph_node.output_layout().clone();
        if !output_layout.is_typed() {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                "compiled Aggregate has no complete output declaration",
            ));
        }
        let fields = graph_node.output_layout().schema().fields();
        if fields.len() != group_by.len() + calls.len() {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                format!(
                    "compiled Aggregate at local node {at} output is not its keys followed by its calls"
                ),
            ));
        }
        let key_types = sites[..group_by.len()]
            .iter()
            .map(|&site| root_value_type(&program, site).map_err(ExecutionFailure::from_domain))
            .collect::<ExecutionResult<Vec<_>>>()?;
        let groups = group_by.len();
        Ok(Self {
            name: format!("COMPILED_AGGREGATE (node={at})"),
            node,
            owns_ordered_endpoint,
            program,
            sites,
            groups,
            calls,
            key_types,
            output_layout,
            output_role: AggregateSchemaRole::Output,
            grouping,
            direct_intermediate: false,
            topn: None,
            error,
        })
    }

    /// Exactly the original local two-stage condition except for upstream
    /// DOP, which the pipeline builder observes after building the input.
    pub(crate) fn requires_local_update_stages(&self) -> bool {
        self.completes_groups()
            && self.calls.iter().all(|call| {
                call.contract.phase() == novarocks_functions::AggregateKernelPhase::Single
            })
    }
    /// Consume the already prepared local lifecycle. The intermediate schema
    /// is authored by each exact call's existing full intermediate type.
    pub(crate) fn into_local_update_stages(self) -> ExecutionResult<(Self, Self)> {
        if self.topn.is_some() {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                "compiled aggregate must split before installing its ordered producer",
            ));
        }
        if !self.requires_local_update_stages() {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                "compiled aggregate local stages differ from its actual update node",
            ));
        }
        let mut partial_calls = Vec::with_capacity(self.calls.len());
        let mut final_calls = Vec::with_capacity(self.calls.len());
        for (ordinal, original) in self.calls.iter().enumerate() {
            let stages = original.handle.local_stages().ok_or_else(|| {
                program_binding_failure(
                    PlanBindingKind::InvalidAssignment,
                    format_args!(
                        "compiled aggregate call {ordinal} has no prepared local lifecycle"
                    ),
                )
            })?;
            if !Arc::ptr_eq(stages.source(), &original.contract) {
                return Err(program_binding_failure(
                    PlanBindingKind::InvalidAssignment,
                    "compiled aggregate local stages have a different original call",
                ));
            }
            let handle = stages.partial().clone();
            partial_calls.push(CallPlan {
                contract: Arc::clone(handle.contract()),
                handle,
                merge: false,
                first_root: original.first_root,
                roots: original.roots,
            });
            let handle = stages.final_stage().clone();
            final_calls.push(CallPlan {
                contract: Arc::clone(handle.contract()),
                handle,
                merge: true,
                first_root: self.groups + ordinal,
                roots: 1,
            });
        }
        let partial = Self {
            name: format!("{} LOCAL_PARTIAL", self.name),
            node: self.node,
            owns_ordered_endpoint: false,
            program: Arc::clone(&self.program),
            sites: self.sites.clone(),
            groups: self.groups,
            calls: partial_calls,
            key_types: self.key_types.clone(),
            output_layout: self.output_layout.clone(),
            output_role: AggregateSchemaRole::Intermediate,
            grouping: CompiledAggregateGrouping::Partial,
            direct_intermediate: false,
            topn: None,
            error: Arc::clone(&self.error),
        };
        let final_stage = Self {
            name: format!("{} LOCAL_FINAL", self.name),
            node: self.node,
            owns_ordered_endpoint: self.owns_ordered_endpoint,
            program: self.program,
            sites: Vec::new(),
            groups: self.groups,
            calls: final_calls,
            key_types: self.key_types,
            output_layout: self.output_layout,
            output_role: AggregateSchemaRole::Output,
            grouping: CompiledAggregateGrouping::Complete,
            direct_intermediate: true,
            topn: None,
            error: self.error,
        };
        Ok((partial, final_stage))
    }
    /// The actual output key slots also prefix the prepared partial schema.
    pub(crate) fn local_group_partition_slots(&self) -> Vec<novarocks_types::SlotId> {
        self.output_layout.slots()[..self.groups].to_vec()
    }
    /// Whether every group of this aggregate must be owned by one driver.
    pub(crate) fn completes_groups(&self) -> bool {
        self.grouping == CompiledAggregateGrouping::Complete
    }
}

impl OperatorFactory for CompiledAggregateProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let prepared = if self.owns_ordered_endpoint && self.topn.is_none() {
            Err(KernelFailure::InvalidProgram(KernelDiagnostic::new(
                "compiled aggregate ordered producer endpoint is not installed",
            ))
            .into())
        } else {
            self.topn
                .as_ref()
                .map(|factory| factory.create_for_driver(&self.program, dop, driver_id))
                .transpose()
        };
        let (topn, failed) = match prepared {
            Ok(topn) => (topn, None),
            Err(error) => (None, Some(error)),
        };
        Box::new(Transform::hydrating(CompiledAggregateProcessor {
            name: self.name.clone(),
            program: Arc::clone(&self.program),
            sites: self.sites.clone(),
            group_roots: self.groups,
            calls: self.calls.clone(),
            key_types: self.key_types.clone(),
            output_layout: self.output_layout.clone(),
            output_role: self.output_role,
            output: None,
            direct_intermediate: self.direct_intermediate,
            direct_input: None,
            construction: None,
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            tracker: None,
            instances: None,
            key_table: None,
            states: None,
            groups: 0,
            emission: None,
            failed,
            topn,
        }))
    }
}

struct CompiledAggregateProcessor {
    name: String,
    program: Arc<LocalProgram>,
    sites: Vec<ProgramExpressionRootSite>,
    group_roots: usize,
    calls: Vec<CallPlan>,
    key_types: Vec<FunctionValueType>,
    output_layout: novarocks_local_program::StaticLayout,
    output_role: AggregateSchemaRole,
    output: Option<ChunkSchemaRef>,
    direct_intermediate: bool,
    direct_input: Option<ChunkSchemaRef>,
    construction: Option<Arc<dyn InputConstructionFactory>>,
    control: RuntimeKernelControl,
    tracker: Option<Arc<MemTracker>>,
    instances: Option<Vec<CompiledExpressionInstance>>,
    /// Present exactly when the aggregate has group keys.
    key_table: Option<KeyIndex>,
    /// One column per call, created on first use.
    states: Option<Vec<AggregateStateColumn>>,
    /// Groups created so far; every state column holds exactly this many.
    groups: usize,
    emission: Option<AggregateOutputCursor>,
    /// The first failure owns its original typed category and diagnostic.
    failed: Option<ExecutionFailure>,
    topn: Option<CompiledAggregateTopN>,
}

struct AggregateOutputCursor {
    output: Chunk,
    offset: usize,
}

impl CompiledAggregateProcessor {
    fn prepared_output(&self) -> ExecutionResult<&ChunkSchemaRef> {
        self.output
            .as_ref()
            .ok_or_else(|| ConstructionFailure::MissingOwner.into())
    }

    /// Dropping the actual owners destroys initialized states before their
    /// allocator returns backing; a status change alone cannot release them.
    fn release_owned(&mut self) {
        self.states = None;
        self.key_table = None;
        self.instances = None;
        self.topn = None;
        self.emission = None;
        self.groups = 0;
    }

    fn failure_result<T>(&mut self, result: ExecutionResult<T>) -> ExecutionResult<T> {
        if let Some(error) = &self.failed {
            return Err(error.clone());
        }
        match result {
            Ok(value) => Ok(value),
            Err(error) => {
                if self.failed.is_none() {
                    self.failed = Some(error);
                    if let Some(topn) = &mut self.topn {
                        let _ = topn.fail(RuntimeFilterProducerFailure::ExecutionFailed);
                    }
                    // A failed update may have changed a prefix of the calls,
                    // and a failed emission may have built a prefix of output.
                    // Neither prefix can be resumed or exposed after failure.
                    self.release_owned();
                }
                Err(self
                    .failed
                    .as_ref()
                    .expect("first failure was retained")
                    .clone())
            }
        }
    }

    fn tracker(&self) -> ExecutionResult<Arc<MemTracker>> {
        if let Some(tracker) = self.tracker.as_ref() {
            return Ok(Arc::clone(tracker));
        }
        #[cfg(test)]
        {
            Ok(MemTracker::new_child(
                "CompiledAggregateTest",
                &crate::runtime::mem_tracker::process_mem_tracker(),
            ))
        }
        #[cfg(not(test))]
        {
            Err(crate::runtime::runtime_state::RuntimeBindingFailure::MissingOperatorMemoryTracker {
                operator: novarocks_failure_contract::BoundedDetail::truncating("compiled aggregate"),
            }.into())
        }
    }

    /// Create the key table and the state columns on first use.
    fn ensure_state(&mut self) -> ExecutionResult<()> {
        if self.states.is_some() {
            return Ok(());
        }
        let tracker = self.tracker()?;
        if self.group_roots > 0 {
            let schema = Arc::clone(self.prepared_output()?);
            let owner = self.control.construction_owner(schema.as_ref())?;
            let checkpoint = || self.control.checkpoint_execution();
            let mut nulls = aggregate_vector(owner.as_ref(), self.key_types.len())?;
            for _ in &self.key_types {
                checkpoint()?;
                nulls.push(NullPolicy::Equal);
            }
            let spec = KeySpec::new(&self.key_types, &nulls, owner.as_ref(), &checkpoint)?;
            self.key_table = Some(KeyIndex::new(
                spec,
                IndexPurpose::KeyIds,
                MemTracker::new_child("CompiledAggregateKeys", &tracker),
                owner.as_ref(),
            )?);
        }
        let allocator: Arc<dyn AggregateStateAllocator> = Arc::new(TrackedStateAllocator::new(
            MemTracker::new_child("CompiledAggregateStates", &tracker),
        ));
        let mut states = Vec::with_capacity(self.calls.len());
        for call in &self.calls {
            let stride = call.handle.state_layout().pad_to_align().size().max(1);
            // A group-less aggregate owns exactly one state per call.
            let per_block = if self.group_roots == 0 {
                1
            } else {
                (STATE_BLOCK_BYTES / stride).clamp(1, MAX_STATES_PER_BLOCK)
            };
            states.push(AggregateStateColumn::try_new(
                call.handle.clone(),
                Arc::clone(&allocator),
                NonZeroUsize::new(per_block).expect("clamped to at least one state"),
            )?);
        }
        self.states = Some(states);
        Ok(())
    }

    /// Initialize the state of every group created since the last batch.
    fn grow_states(&mut self) -> ExecutionResult<()> {
        let states = self.states.as_mut().expect("states were created");
        for column in states.iter_mut() {
            while column.len() < self.groups {
                column.push(&self.control)?;
            }
        }
        Ok(())
    }

    /// Assign original occurrences through the shared key author. Aggregate
    /// states and TopN publication keep their original lifecycle and owners.
    fn assign_groups(
        &mut self,
        keys: &[ArrayRef],
        bindings: &[FunctionValueType],
        rows: usize,
        owner: &dyn InputConstructionOwner,
    ) -> ExecutionResult<Vec<usize>> {
        // The caller keeps this same actual creator alive through the returned
        // mapping's final use. A different construction scope is not a substitute.
        let control = &self.control;
        let checkpoint = || control.checkpoint_execution();
        let mut mapping = aggregate_vector(owner, rows)?;
        if self.group_roots == 0 {
            self.groups = 1;
            for _ in 0..rows {
                checkpoint()?;
                mapping.push(0);
            }
            return Ok(mapping);
        }
        let tracker = self.tracker()?;
        let table = self
            .key_table
            .as_mut()
            .expect("grouped aggregate has an index");
        if keys.len() != self.group_roots || bindings.len() != self.group_roots {
            return Err(group_contract(
                "compiled aggregate group columns differ from their complete bindings",
            ));
        }
        let readable = ReachableRows::all(rows)
            .materialize_observed(
                &mut |layout| owner.request(layout).map_err(ExecutionFailure::from),
                &mut |_| checkpoint(),
            )
            .map_err(aggregate_value_failure)?;
        let mut occurrences = aggregate_vector(owner, rows)?;
        for row in 0..rows {
            checkpoint()?;
            occurrences.push(row);
        }
        let input = KeyInput::new_for_spec(
            table.spec(),
            keys,
            bindings,
            &readable,
            &occurrences,
            &mut ValueConversionContext::default(),
            owner,
            &checkpoint,
        )?;
        let prepared = PreparedKeyBatch::from_captured(
            table.spec(),
            &input,
            AggregateAllocator::new(tracker),
            &checkpoint,
        )?;
        let hashes = hash_batch(
            &prepared,
            table.policy(),
            &mut HashScratch::default(),
            owner,
            &checkpoint,
        )?;
        let interned = table
            .intern_batch(&prepared, &hashes, owner, control)
            .map_err(|cause| control.project_required_failure(cause))?;
        let mut next = self.groups;
        let observe_topn = self.topn.is_some();
        let mut new_ids = aggregate_vector(
            owner,
            if observe_topn {
                interned.new_keys().len()
            } else {
                0
            },
        )?;
        for new in interned.new_keys() {
            checkpoint()?;
            if new.id.index() != next {
                return Err(group_contract(
                    "compiled aggregate index created a group out of order",
                ));
            }
            if observe_topn {
                new_ids.push(new.id);
            }
            next = next
                .checked_add(1)
                .ok_or_else(|| group_contract("compiled aggregate group count overflow"))?;
        }
        if next != table.len() {
            return Err(group_contract(
                "compiled aggregate index and state group counts differ",
            ));
        }
        for &found in interned.matches() {
            checkpoint()?;
            match found {
                KeyMatch::Found(id) => mapping.push(id.index()),
                _ => {
                    return Err(group_contract(
                        "GROUP BY equality input did not receive a group",
                    ));
                }
            }
        }
        if mapping.len() != rows {
            return Err(group_contract(
                "compiled aggregate group id count differs from its rows",
            ));
        }
        if let Some(topn) = &mut self.topn {
            let read = table.read_keys(&new_ids, owner, control)?;
            topn.observe_new_groups(&self.program, &read, &new_ids)?;
        }
        self.groups = next;
        Ok(mapping)
    }

    fn consume(&mut self, chunk: &Chunk) -> ExecutionResult<()> {
        let values = if self.direct_intermediate {
            let expected = self
                .direct_input
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?;
            if chunk.chunk_schema() != expected.as_ref() {
                return Err(group_contract(
                    "compiled local aggregate state chunk differs from its actual intermediate schema",
                ));
            }
            self.ensure_state()?;
            chunk.record_batch().columns().to_vec()
        } else {
            instances(
                &mut self.instances,
                &self.program,
                &self.sites,
                &self.control,
            )?;
            self.ensure_state()?;
            // Keys first, then each call's original roots in call order.
            let instances = self.instances.as_mut().expect("instances were created");
            let mut values = Vec::with_capacity(self.sites.len());
            for (instance, site) in instances.iter_mut().zip(&self.sites) {
                values.push(evaluate_all(
                    instance,
                    *site,
                    chunk.record_batch(),
                    &self.control,
                )?);
            }
            values
        };
        let rows = chunk.len();
        let schema = Arc::clone(self.prepared_output()?);
        let owner = self.control.construction_owner(schema.as_ref())?;
        // Static root facts come from the same checked-expression author as
        // evaluation; local merge uses its actual prepared input declarations.
        let mut bindings = aggregate_vector(owner.as_ref(), values.len())?;
        if self.direct_intermediate {
            for slot in chunk.chunk_schema().slots() {
                self.control.checkpoint_execution()?;
                bindings.push(
                    FunctionValueType::try_from_typed_binding(
                        slot.typed_binding()
                            .clone_observed(|| self.control.checkpoint_execution())?,
                    )
                    .map_err(KernelFailure::from)?,
                );
            }
        } else {
            for &site in &self.sites {
                self.control.checkpoint_execution()?;
                bindings.push(
                    root_value_type(&self.program, site).map_err(ExecutionFailure::from_domain)?,
                );
            }
        }
        let mapping = self
            .assign_groups(
                &values[..self.group_roots],
                &bindings[..self.group_roots],
                rows,
                owner.as_ref(),
            )
            .map_err(|cause| self.control.project_required_failure(cause))?;
        self.grow_states()?;
        let selection = Selection::all(rows);
        let states = self.states.as_mut().expect("states were created");
        for (call, column) in self.calls.iter().zip(states.iter_mut()) {
            let arguments = values[call.first_root..call.first_root + call.roots]
                .iter()
                .map(EvaluatedArgument::Column)
                .collect::<Vec<_>>();
            let mut frame = if call.merge {
                let input = SelectedAggregateMergeInput::try_new(
                    call.contract.as_ref(),
                    selection,
                    arguments[0],
                    &self.control,
                )?;
                column.prepare_merge_batch_evaluation(&mapping, input, &self.control)?
            } else {
                // State construction retained this exact installed handle. Its
                // phase and input capability belong to the BE implementation.
                // Borrowing the shared handle avoids a self-borrow of `column`
                // while the update frame subsequently borrows it mutably.
                if !call.handle.same_installed_owner(column.handle()) {
                    return Err(KernelFailure::InvalidProgram(KernelDiagnostic::new(
                        "aggregate state and input use different installed call owners",
                    ))
                    .into());
                }
                let contract = call.handle.contract().as_ref();
                let logical_count = contract.call().logical_argument_count();
                let input = SelectedAggregateUpdateInput::try_new_with_typed_source_lookup(
                    contract,
                    selection,
                    &arguments[..logical_count],
                    &arguments[logical_count..],
                    |ordinal| {
                        bindings.get(call.first_root + ordinal).ok_or_else(|| {
                            KernelFailure::InvalidProgram(KernelDiagnostic::new(
                                "aggregate input root binding is missing",
                            ))
                        })
                    },
                    |ordinal| call.handle.argument_encoding(ordinal),
                    &self.control,
                )?;
                column.prepare_update_batch_evaluation(&mapping, input, &self.control)?
            };
            frame.run(&self.control)?;
        }
        Ok(())
    }

    /// Every group's keys and call emissions, in group creation order.
    fn build_output(&mut self) -> ExecutionResult<Option<Chunk>> {
        let output_schema = Arc::clone(self.prepared_output()?);
        self.ensure_state()?;
        if self.group_roots == 0 && self.groups == 0 {
            // A group-less aggregate emits its initial state on empty input.
            self.groups = 1;
            self.grow_states()?;
        }
        let groups = self.groups;
        if groups == 0 {
            return Ok(None);
        }
        let fields = output_schema.arrow_schema_ref();
        let owner = self.control.construction_owner(output_schema.as_ref())?;
        let columns_layout = Layout::array::<ArrayRef>(fields.fields().len()).map_err(|_| {
            crate::exec::pipeline::operator::ConstructionFailure::Resource(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            )
        })?;
        if columns_layout.size() != 0 {
            owner.request(columns_layout)?;
        }
        let mut columns: Vec<ArrayRef> = Vec::new();
        columns
            .try_reserve_exact(fields.fields().len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        if let Some(table) = self.key_table.as_ref() {
            let mut targets = aggregate_vector(owner.as_ref(), self.group_roots)?;
            for slot in &output_schema.slots()[..self.group_roots] {
                self.control.checkpoint_execution()?;
                targets.push(
                    FunctionValueType::try_from_typed_binding(
                        slot.typed_binding()
                            .clone_observed(|| self.control.checkpoint_execution())?,
                    )
                    .map_err(KernelFailure::from)?,
                );
            }
            let keys = table
                .export_key_range(0..groups, &targets, owner.as_ref(), &self.control)
                .map_err(|cause| self.control.project_required_failure(cause))?;
            for array in keys.arrays() {
                self.control.checkpoint_execution()?;
                columns.push(Arc::clone(array));
            }
        }
        // This temporary releases its actual allocation at the end of emit;
        // its capacity never becomes part of a late output schema's lifetime.
        let tracker = self
            .tracker
            .as_ref()
            .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?;
        let index_allocator =
            crate::exec::hash_table::allocation::AggregateAllocator::new(Arc::clone(tracker));
        let mut indices =
            crate::exec::hash_table::allocation::AggregateVec::new_in(index_allocator);
        indices
            .try_reserve_exact(groups)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        for group in 0..groups {
            indices.push(group);
            if (group + 1) % novarocks_type_contract::MAX_UNOBSERVED_COMPILE_WORK as usize == 0 {
                owner.observe(
                    0,
                    &[],
                    crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork {
                        units: novarocks_type_contract::MAX_UNOBSERVED_COMPILE_WORK as usize,
                    },
                )?;
            }
        }
        owner.observe(
            0,
            &[],
            crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork {
                units: groups % novarocks_type_contract::MAX_UNOBSERVED_COMPILE_WORK as usize,
            },
        )?;
        let states = self.states.as_ref().expect("states were created");
        for column in states {
            columns.push(column.emit_evaluation(&indices, groups, &self.control)?);
        }
        let output = Chunk::try_new_typed_with_columns_owned_observed::<
            crate::exec::pipeline::operator::ConstructionFailure,
            _,
        >(
            Arc::clone(&output_schema),
            columns,
            groups,
            owner.storage_lifetime(),
            |step| owner.observe(0, &[], step),
        )?;
        Ok(Some(output))
    }
}

impl Drop for CompiledAggregateProcessor {
    fn drop(&mut self) {
        self.release_owned();
    }
}

impl OperatorLifecycle for CompiledAggregateProcessor {
    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_construction_factory(
        &mut self,
        factory: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
    ) {
        self.control.bind_construction_factory(Arc::clone(&factory));
        self.construction = Some(factory);
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        if let Some(error) = &self.failed {
            return Err(error.clone());
        }
        if self.output.is_some() {
            return Ok(());
        }
        let result = (|| {
            let factory = self
                .construction
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?;
            let output = prepare_aggregate_schema(
                factory.as_ref(),
                &self.output_layout,
                self.group_roots,
                &self.calls,
                self.output_role,
            )?;
            let direct_input = if self.direct_intermediate {
                Some(prepare_aggregate_schema(
                    factory.as_ref(),
                    &self.output_layout,
                    self.group_roots,
                    &self.calls,
                    AggregateSchemaRole::Intermediate,
                )?)
            } else {
                None
            };
            // Publish only a complete preparation. A second-schema refusal
            // drops the first temporary before the failure latch is visible.
            self.output = Some(output);
            self.direct_input = direct_input;
            Ok(())
        })();
        self.failure_result(result)
    }
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.control.bind_runtime_memory(state);
        let result = self
            .topn
            .as_mut()
            .map_or(Ok(()), CompiledAggregateTopN::bind);
        self.failure_result(result)
    }
    fn cancel(&mut self) {
        if let Some(topn) = &mut self.topn {
            let _ = topn.fail(RuntimeFilterProducerFailure::Cancelled);
        }
        self.release_owned();
    }
    fn on_driver_failure(&mut self) {
        if let Some(topn) = &mut self.topn {
            let _ = topn.fail(RuntimeFilterProducerFailure::ExecutionFailed);
        }
        self.release_owned();
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.control.bind_mem_tracker(Arc::clone(&tracker));
        self.tracker = Some(tracker);
    }
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        let result = self.topn.as_mut().map_or(Ok(()), |topn| {
            topn.fail(RuntimeFilterProducerFailure::ExecutionFailed)
        });
        self.release_owned();
        result
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.begin_exit()
    }
}

impl InputEncodingPolicy for CompiledAggregateProcessor {}
impl TransformKernel for CompiledAggregateProcessor {
    fn apply(
        &mut self,
        ctx: &mut StepContext<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<StepResult> {
        let chunk = input.into_uncovered_chunk()?;
        if let Some(error) = &self.failed {
            return Err(error.clone());
        }
        let rows = u64::try_from(chunk.len()).expect("supported row count fits u64");
        let result = if let Err(error) = self.prepared_output() {
            Err(error)
        } else if chunk.is_empty() {
            Ok(())
        } else {
            self.consume(&chunk).and_then(|()| {
                self.topn
                    .as_mut()
                    .map_or(Ok(()), |topn| topn.after_successful_input(chunk.len()))
            })
        };
        if let Err(error) = &result {
            ctx.report_failure(error.clone());
        }
        self.failure_result(result)?;
        Ok(StepResult::new(None, NextStep::NeedInput, rows))
    }
    fn drive(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        if let Some(error) = &self.failed {
            return Err(error.clone());
        }
        let Some(cursor) = self.emission.as_ref() else {
            return Ok(StepResult::new(None, NextStep::NeedInput, 0));
        };
        if !ctx.work().try_consume(1) {
            return Ok(StepResult::new(None, NextStep::Yield, 0));
        }
        let rows = OUTPUT_CHUNK_ROWS.min(cursor.output.len() - cursor.offset);
        let result = (|| -> ExecutionResult<Chunk> {
            let owner = self
                .control
                .construction_owner(cursor.output.chunk_schema())?;
            Ok(cursor
                .output
                .slice_typed_observed::<ConstructionFailure, _>(
                    cursor.offset,
                    rows,
                    owner.storage_lifetime(),
                    |step| owner.observe(0, &[], step),
                )?)
        })();
        if let Err(error) = &result {
            ctx.report_failure(error.clone());
        }
        let output = self.failure_result(result)?;
        let cursor = self
            .emission
            .as_mut()
            .expect("aggregate output remains installed");
        cursor.offset += rows;
        let next = if cursor.offset == cursor.output.len() {
            self.emission = None;
            NextStep::Finished
        } else {
            NextStep::Runnable
        };
        Ok(StepResult::new(Some(output), next, 0))
    }
    fn finish(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        if let Some(error) = &self.failed {
            return Err(error.clone());
        }
        let result = self.build_output().and_then(|output| {
            if let Some(topn) = &mut self.topn {
                topn.finish()?;
            }
            Ok(output)
        });
        if let Err(error) = &result {
            ctx.report_failure(error.clone());
        }
        let output = self.failure_result(result)?;
        self.topn = None;
        self.states = None;
        self.key_table = None;
        self.instances = None;
        match output {
            Some(output) if output.len() > OUTPUT_CHUNK_ROWS => {
                self.emission = Some(AggregateOutputCursor { output, offset: 0 });
                Ok(StepResult::new(None, NextStep::Runnable, 0))
            }
            output => Ok(StepResult::new(output, NextStep::Finished, 0)),
        }
    }
}

#[cfg(test)]
use super::{aggregate_fixture, family_fixture};

#[cfg(test)]
mod failure_latch_tests {
    use super::aggregate_fixture::{
        CallSpec, add_aggregate, aggregate_catalog, bind, compile, extrema_catalog, finish,
        packages, values,
    };
    use super::family_fixture::{FixtureControl, int64};
    use super::*;
    use crate::runtime::fragment::{
        ExecutionFailureCause, PipelineOperation, RequiredExpressionRowError,
    };
    use arrow::array::{Array, Int64Array};
    use novarocks_functions::PureKernelAbi;
    use novarocks_physical_plan::{
        AggregateCallId, AggregateGrouping, AggregatePhase, FragmentBuilder, FragmentId,
        FragmentSink, LiteralValue, PlanBuilder, PlanVersionId, ResultField, ResultPort,
    };

    fn processor() -> (
        CompiledAggregateProcessor,
        Chunk,
        Arc<RuntimeErrorState>,
        Arc<MemTracker>,
    ) {
        processor_for_count(false)
    }
    fn processor_for_count(
        distinct: bool,
    ) -> (
        CompiledAggregateProcessor,
        Chunk,
        Arc<RuntimeErrorState>,
        Arc<MemTracker>,
    ) {
        processor_for_key_count(
            distinct,
            int64(false),
            &[vec![LiteralValue::Int64(1)], vec![LiteralValue::Int64(2)]],
        )
    }
    fn processor_for_key_count(
        distinct: bool,
        key_type: FunctionValueType,
        literal_rows: &[Vec<LiteralValue>],
    ) -> (
        CompiledAggregateProcessor,
        Chunk,
        Arc<RuntimeErrorState>,
        Arc<MemTracker>,
    ) {
        let catalog = if distinct {
            aggregate_catalog(&[("multi_distinct_count", PureKernelAbi::AggregateV1)])
        } else {
            extrema_catalog()
        };
        let count = if distinct {
            bind(&catalog, "multi_distinct_count", &[int64(false)])
        } else {
            bind(&catalog, "count", &[])
        };
        let fragment = FragmentId::new(1);
        let mut builder = FragmentBuilder::new(fragment);
        let source = builder.reserve_node_id().unwrap();
        let keys = values(
            &mut builder,
            source,
            std::slice::from_ref(&key_type),
            literal_rows,
        );
        let (node, _) = add_aggregate(
            &mut builder,
            source,
            &keys,
            &[CallSpec {
                bound: &count,
                phase: AggregatePhase::Single,
                id: AggregateCallId::new(1),
                arguments: if distinct { vec![keys[0]] } else { Vec::new() },
                distinct: false,
            }],
            AggregateGrouping::Complete,
        );
        let definition = finish(builder, node, FragmentSink::Result, 1);
        let output = definition.nodes()[&node].output.clone();
        let mut plan = PlanBuilder::new(PlanVersionId::try_new([101; 16]).unwrap());
        plan.add_fragment(definition).unwrap();
        plan.set_result_port(ResultPort {
            scalar_schema: None,
            fragment,
            fields: output
                .columns
                .iter()
                .zip([key_type, count.result_type()])
                .enumerate()
                .map(|(ordinal, (value, ty))| ResultField {
                    domain: crate::test_result_domain::result_value_domain(&ty),
                    name: format!("c{ordinal}").into_boxed_str(),
                    alias: None,
                    value: *value,
                    ty,
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            output,
        })
        .unwrap();
        let plan = plan.finish_observed(&FixtureControl).unwrap();
        let package = packages(&plan, &catalog).remove(&fragment).unwrap();
        let program = compile(package, &catalog, 1, true);
        let source = program
            .graph()
            .nodes()
            .iter()
            .find(|node| matches!(node.kind(), ProgramNodeKind::Values { .. }))
            .unwrap();
        let ProgramNodeKind::Values { values } = source.kind() else {
            unreachable!()
        };
        let input = Chunk::new_with_chunk_schema(
            values.batch().unwrap().clone(),
            ChunkSchema::from_compiled_layout(source.output_layout()).unwrap(),
        );
        let node = program.graph().root();
        let error = Arc::new(RuntimeErrorState::default());
        let factory =
            CompiledAggregateProcessorFactory::try_new(program, node, Arc::clone(&error)).unwrap();
        let tracker = MemTracker::new_root("CompiledAggregateFailureLatchTest");
        let mut processor = CompiledAggregateProcessor {
            name: factory.name,
            program: factory.program,
            sites: factory.sites,
            group_roots: factory.groups,
            calls: factory.calls,
            key_types: factory.key_types,
            output_layout: factory.output_layout,
            output_role: factory.output_role,
            output: None,
            direct_intermediate: factory.direct_intermediate,
            direct_input: None,
            construction: None,
            control: RuntimeKernelControl::new(Arc::clone(&error)),
            tracker: Some(Arc::clone(&tracker)),
            instances: None,
            key_table: None,
            states: None,
            groups: 0,
            emission: None,
            failed: None,
            topn: None,
        };
        processor.set_mem_tracker(Arc::clone(&tracker));
        processor.set_construction_factory(super::aggregate_fixture::runtime_construction_factory(
            Arc::clone(&tracker),
            Arc::clone(&error),
        ));
        processor
            .prepare()
            .expect("prepare actual aggregate schema");
        (processor, input, error, tracker)
    }
    #[test]
    fn compiled_aggregate_k_cross_dictionary_generations_share_dense_states_and_export_declared_offsets()
     {
        use arrow::array::{DictionaryArray, StringArray};
        use arrow::datatypes::Int32Type;
        let ty = FunctionValueType::new(DataType::Utf8, true);
        let (mut processor, _input, _, _tracker) =
            processor_for_key_count(false, ty, &[vec![LiteralValue::Utf8("seed".into())]]);
        processor.ensure_state().unwrap();
        let declared = processor.key_types.clone();
        let schema = Arc::clone(processor.prepared_output().unwrap());
        let owner = processor
            .control
            .construction_owner(schema.as_ref())
            .unwrap();
        let first: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                arrow::array::Int32Array::from(vec![Some(1), Some(0), Some(1), Some(2)]),
                Arc::new(StringArray::from(vec![Some("b"), Some("a"), None])),
            )
            .unwrap(),
        );
        let second: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                arrow::array::Int32Array::from(vec![Some(0), Some(1), Some(2), Some(0), None]),
                Arc::new(StringArray::from(vec![Some("b"), None, Some("a")])),
            )
            .unwrap(),
        );
        for (array, expected) in [(first, vec![0, 1, 0, 2]), (second, vec![1, 2, 0, 1, 2])] {
            let rows = array.len();
            let mapping = processor
                .assign_groups(&[array], &declared, rows, owner.as_ref())
                .unwrap();
            assert_eq!(mapping, expected);
            processor.grow_states().unwrap();
            let call = &processor.calls[0];
            let input = SelectedAggregateUpdateInput::try_new_with_typed_source_lookup(
                call.handle.contract().as_ref(),
                Selection::all(rows),
                &[],
                &[],
                |_| {
                    Err(KernelFailure::InvalidProgram(KernelDiagnostic::new(
                        "COUNT(*) has no argument root",
                    )))
                },
                |ordinal| call.handle.argument_encoding(ordinal),
                &processor.control,
            )
            .unwrap();
            processor.states.as_mut().unwrap()[0]
                .prepare_update_batch_evaluation(&mapping, input, &processor.control)
                .unwrap()
                .run(&processor.control)
                .unwrap();
        }
        let output = processor
            .finalize_for_test(&RuntimeState::default())
            .unwrap()
            .output
            .unwrap();
        assert_eq!(output.columns()[0].data_type(), &DataType::Utf8);
        assert_eq!(
            output.columns()[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some("a"), Some("b"), None]
        );
        assert_eq!(
            output.columns()[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[3, 3, 3]
        );
    }

    #[test]
    fn compiled_aggregate_k_real_root_float_batches_preserve_first_zero_nan_null_and_counts() {
        let nan = f64::from_bits(0x7ff8_0000_0000_0042);
        let ty = FunctionValueType::new(DataType::Float64, true);
        let rows = vec![
            vec![LiteralValue::Float64Bits((-0.0f64).to_bits())],
            vec![LiteralValue::Float64Bits(0.0f64.to_bits())],
            vec![LiteralValue::Float64Bits(nan.to_bits())],
            vec![LiteralValue::Float64Bits(f64::NAN.to_bits())],
            vec![LiteralValue::Null],
            vec![LiteralValue::Float64Bits(1.5f64.to_bits())],
        ];
        let (mut processor, input, _, _tracker) = processor_for_key_count(false, ty, &rows);
        let state = RuntimeState::default();
        processor.apply_for_test(&state, input.clone()).unwrap();
        processor.apply_for_test(&state, input).unwrap();
        let output = processor.finalize_for_test(&state).unwrap().output.unwrap();
        let keys = output.columns()[0]
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        let count = output.columns()[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(keys.len(), 4);
        assert!(keys.value(0) == 0.0 && keys.value(0).is_sign_negative());
        assert!(keys.value(1).is_nan());
        assert!(keys.is_null(2));
        assert_eq!(keys.value(3), 1.5);
        assert_eq!(count.values().as_ref(), &[4, 4, 2, 2]);
    }

    fn with_context<T>(state: &RuntimeState, action: impl FnOnce(&mut StepContext<'_>) -> T) -> T {
        let error = state.error_state();
        let mut work =
            crate::exec::pipeline::step_context::TurnWork::new(std::num::NonZeroU64::MIN, 64);
        action(&mut StepContext::new(
            &error,
            &mut work,
            &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
        ))
    }
    impl CompiledAggregateProcessor {
        fn apply_for_test(
            &mut self,
            state: &RuntimeState,
            input: Chunk,
        ) -> ExecutionResult<StepResult> {
            with_context(state, |ctx| self.apply(ctx, PreparedInput::for_test(input)))
        }
        fn drive_for_test(&mut self, state: &RuntimeState) -> ExecutionResult<StepResult> {
            with_context(state, |ctx| self.drive(ctx))
        }
        fn finalize_for_test(&mut self, state: &RuntimeState) -> ExecutionResult<StepResult> {
            with_context(state, |ctx| self.finish(ctx))
        }
    }
    fn assert_failed_reentry(
        processor: &mut CompiledAggregateProcessor,
        expected: &ExecutionFailure,
        input: &Chunk,
    ) {
        let state = RuntimeState::default();
        assert!(processor.failed.is_some());
        assert!(processor.states.is_none());
        assert!(processor.key_table.is_none());
        assert!(processor.instances.is_none());
        assert!(processor.emission.is_none());
        assert_eq!(
            processor.apply_for_test(&state, input.clone()).unwrap_err(),
            *expected
        );
        assert_eq!(
            processor
                .apply_for_test(&state, Chunk::default())
                .unwrap_err(),
            *expected
        );
        assert_eq!(processor.drive_for_test(&state).unwrap_err(), *expected);
        assert_eq!(processor.finalize_for_test(&state).unwrap_err(), *expected);
    }
    fn pending_prefix(
        processor: &mut CompiledAggregateProcessor,
        input: &Chunk,
    ) -> std::sync::Weak<dyn arrow::array::Array> {
        let column = Arc::new(Int64Array::from(vec![999, 1000])) as ArrayRef;
        let weak = Arc::downgrade(&column);
        let batch = RecordBatch::try_new(input.record_batch().schema(), vec![column]).unwrap();
        processor.emission = Some(AggregateOutputCursor {
            output: Chunk::new_like(batch, input),
            offset: 0,
        });
        weak
    }
    #[test]
    fn compiled_aggregate_failure_latch_count_distinct_cancel_releases_live_state() {
        let (mut processor, input, error, tracker) = processor_for_count(true);
        let creator_bytes = tracker.current();
        let state = RuntimeState::default();
        processor.apply_for_test(&state, input.clone()).unwrap();
        processor.apply_for_test(&state, input.clone()).unwrap();
        assert_eq!(processor.groups, 2);
        assert!(tracker.current() > 0);
        assert!(processor.instances.is_some());
        assert!(
            processor
                .states
                .as_ref()
                .is_some_and(|states| states[0].len() == 2)
        );
        error.set_error(KernelFailure::Cancelled);
        let original = processor.apply_for_test(&state, input.clone()).unwrap_err();
        assert_eq!(
            original.cause(),
            &ExecutionFailureCause::Kernel(KernelFailure::Cancelled)
        );
        assert_eq!(
            tracker.current(),
            creator_bytes,
            "distinct tables and keys must drop"
        );
        assert_failed_reentry(&mut processor, &original, &input);
        assert_eq!(tracker.current(), creator_bytes);
    }
    #[test]
    fn compiled_aggregate_failure_latch_count_distinct_drop_releases_live_state() {
        let (mut processor, input, _, tracker) = processor_for_count(true);
        processor
            .apply_for_test(&RuntimeState::default(), input)
            .unwrap();
        assert!(tracker.current() > 0);
        assert!(
            processor
                .states
                .as_ref()
                .is_some_and(|states| states[0].len() == 2)
        );
        assert!(processor.key_table.is_some());
        assert!(processor.instances.is_some());
        drop(processor);
        assert_eq!(tracker.current(), 0, "last owner releases distinct backing");
    }
    #[test]
    fn compiled_aggregate_failure_latch_push_cannot_replay_accumulated_prefix() {
        let (mut processor, input, error, tracker) = processor();
        let creator_bytes = tracker.current();
        let state = RuntimeState::default();
        processor.apply_for_test(&state, input.clone()).unwrap();
        assert_eq!(processor.groups, 2);
        assert!(tracker.current() > 0);
        let stop = ExecutionFailure::from(KernelFailure::Internal(
            novarocks_functions::KernelDiagnostic::new("originating task stop"),
        ));
        error.set_error(stop.clone());
        let original = processor.apply_for_test(&state, input.clone()).unwrap_err();
        assert_eq!(original.cause(), stop.cause());
        assert_eq!(
            tracker.current(),
            creator_bytes,
            "failed states must actually drop"
        );
        assert_failed_reentry(&mut processor, &original, &input);
        assert_eq!(tracker.current(), creator_bytes);
    }
    #[test]
    fn compiled_aggregate_failure_latch_finish_discards_pending_prefix() {
        let (mut processor, input, error, tracker) = processor();
        let creator_bytes = tracker.current();
        let state = RuntimeState::default();
        processor.apply_for_test(&state, input.clone()).unwrap();
        let prefix = pending_prefix(&mut processor, &input);
        assert!(prefix.upgrade().is_some());
        error.set_error(novarocks_functions::KernelFailure::Internal(
            novarocks_functions::KernelDiagnostic::new("stop before aggregate emission"),
        ));
        let originating = error.error().unwrap();
        let original = processor.finalize_for_test(&state).unwrap_err();
        assert_eq!(original.cause(), originating.cause());
        assert!(
            prefix.upgrade().is_none(),
            "failed output prefix must actually drop"
        );
        assert_eq!(tracker.current(), creator_bytes);
        assert_failed_reentry(&mut processor, &original, &input);
    }
    #[test]
    fn compiled_aggregate_reports_resource_refusal_before_discarding_unpublished_work() {
        struct ObserveDrop {
            report: Arc<RuntimeErrorState>,
            observed: Arc<std::sync::Mutex<Option<ExecutionFailure>>>,
        }
        impl novarocks_type_contract::ValueTypeStorageHold for ObserveDrop {}
        impl Drop for ObserveDrop {
            fn drop(&mut self) {
                *self.observed.lock().unwrap() = self.report.error();
            }
        }
        let (mut processor, input, kernel_control, tracker) = processor_for_count(true);
        let state = RuntimeState::default();
        processor.apply_for_test(&state, input.clone()).unwrap();
        let report = state.error_state();
        let observed = Arc::new(std::sync::Mutex::new(None));
        let marker =
            novarocks_type_contract::ValueTypeStorageLifetime::held(Arc::new(ObserveDrop {
                report: report.clone(),
                observed: observed.clone(),
            }));
        let prefix = input.clone().retaining_array_type_storage_observed::<
            novarocks_type_contract::owned_resources::layout::LayoutResourceError
        >(&marker, |_| Ok(())).unwrap();
        drop(marker);
        processor.emission = Some(AggregateOutputCursor {
            output: prefix,
            offset: 0,
        });
        tracker.install_limit_once(tracker.current() + 1).unwrap();
        let first = with_context(&state, |ctx| {
            let mut step = ctx.operator_step(7, PipelineOperation::Finishing);
            processor.finish(&mut step).unwrap_err()
        });
        use novarocks_failure_contract::FailureProjection;
        assert_eq!(
            first.failure_fact().class(),
            novarocks_failure_contract::FailureClass::Resource
        );
        let seen = observed
            .lock()
            .unwrap()
            .take()
            .expect("known failure reported before prefix destruction");
        assert!(seen.same_occurrence(&first));
        assert_eq!(
            seen.context(),
            Some(crate::runtime::fragment::ExecutionFailureContext {
                operator_ordinal: 7,
                operation: PipelineOperation::Finishing,
            })
        );
        assert!(processor.states.is_none());
        assert!(processor.key_table.is_none());
        assert!(processor.emission.is_none());
        assert_failed_reentry(&mut processor, &first, &input);
        drop(processor);
        drop(input);
        drop(kernel_control);
        drop(first);
        drop(seen);
        drop(report);
        drop(state);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn compiled_aggregate_failure_latch_keeps_first_typed_payload_and_context() {
        let (mut processor, input, _, tracker) = processor();
        let creator_bytes = tracker.current();
        let state = RuntimeState::default();
        processor.apply_for_test(&state, input.clone()).unwrap();
        let row = RequiredExpressionRowError::try_new(
            processor.sites[0],
            Selection::all(2),
            novarocks_functions::RowDataError::new(1, "original required row"),
        )
        .unwrap();
        let original = ExecutionFailure::from(row).at_operator(7, PipelineOperation::Push);
        let result = processor
            .failure_result::<()>(Err(original.clone()))
            .unwrap_err();
        assert_eq!(result, original);
        assert_eq!(tracker.current(), creator_bytes);
        let secondary = ExecutionFailure::from(KernelFailure::ResourceExhausted)
            .at_operator(9, PipelineOperation::Finishing);
        assert_eq!(
            processor.failure_result::<()>(Err(secondary)).unwrap_err(),
            original
        );
        assert_failed_reentry(&mut processor, &original, &input);
    }
    #[test]
    fn compiled_aggregate_failure_latch_drop_releases_live_owners_and_pending() {
        let (mut processor, input, _, tracker) = processor();
        processor
            .apply_for_test(&RuntimeState::default(), input.clone())
            .unwrap();
        let prefix = pending_prefix(&mut processor, &input);
        assert!(tracker.current() > 0);
        assert!(
            processor
                .states
                .as_ref()
                .is_some_and(|states| states[0].len() == 2)
        );
        assert!(processor.key_table.is_some());
        assert!(processor.instances.is_some());
        drop(processor);
        assert_eq!(tracker.current(), 0);
        assert!(prefix.upgrade().is_none());
    }
    #[test]
    fn actual_compiled_aggregate_output_outlives_operator_without_retaining_control() {
        let (mut processor, input, error, tracker) = processor();
        let error_weak = Arc::downgrade(&error);
        let state = RuntimeState::default();
        processor.apply_for_test(&state, input.clone()).unwrap();
        let output = processor.finalize_for_test(&state).unwrap().output.unwrap();
        assert_eq!(output.len(), 2);
        assert!(output.chunk_schema().is_typed());
        assert!(processor.drive_for_test(&state).unwrap().output.is_none());
        let projection = output.into_batch_projection();
        drop(input);
        drop(processor);
        drop(error);
        assert!(error_weak.upgrade().is_none());
        assert!(
            tracker.current() > 0,
            "late output retains actual wrapper responsibility"
        );
        let keys = projection.columns()[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let counts = projection.columns()[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(keys.values().as_ref(), &[1, 2]);
        assert_eq!(counts.values().as_ref(), &[1, 1]);
        let clone = projection.clone();
        assert!(std::ptr::eq(
            projection.record_batch(),
            clone.record_batch()
        ));
        drop(projection);
        assert!(tracker.current() > 0);
        drop(clone);
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn compiled_aggregate_output_cursor_yields_without_allocating_and_emits_each_group_once() {
        use crate::exec::pipeline::step_context::TurnWork;
        let (mut processor, original, error, tracker) = processor();
        let groups = OUTPUT_CHUNK_ROWS + 17;
        let input = Chunk::try_new_with_chunk_schema(
            RecordBatch::try_new(
                original.schema().clone(),
                vec![
                    Arc::new(Int64Array::from((0..groups as i64).collect::<Vec<_>>())) as ArrayRef,
                ],
            )
            .unwrap(),
            original.chunk_schema_ref(),
        )
        .unwrap();
        let state = RuntimeState::default();
        assert_eq!(
            processor
                .apply_for_test(&state, input)
                .unwrap()
                .committed_rows,
            groups as u64
        );
        let finalized = processor.finalize_for_test(&state).unwrap();
        assert!(finalized.output.is_none());
        assert!(matches!(finalized.next, NextStep::Runnable));
        assert!(processor.states.is_none());
        assert!(processor.key_table.is_none());
        let mut work = TurnWork::new(std::num::NonZeroU64::new(41).unwrap(), 1);
        let first = processor
            .drive(&mut StepContext::new(
                &error,
                &mut work,
                &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
            ))
            .unwrap();
        assert!(matches!(first.next, NextStep::Runnable));
        let first = first.output.unwrap();
        assert_eq!(first.len(), OUTPUT_CHUNK_ROWS);
        let allocated = tracker.allocated();
        let cursor = processor.emission.as_ref().unwrap().offset;
        let yielded = processor
            .drive(&mut StepContext::new(
                &error,
                &mut work,
                &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
            ))
            .unwrap();
        assert!(yielded.output.is_none());
        assert!(matches!(yielded.next, NextStep::Yield));
        assert_eq!(tracker.allocated(), allocated);
        assert_eq!(processor.emission.as_ref().unwrap().offset, cursor);
        work.begin_turn(1).unwrap();
        let last = processor
            .drive(&mut StepContext::new(
                &error,
                &mut work,
                &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
            ))
            .unwrap();
        assert!(matches!(last.next, NextStep::Finished));
        let last = last.output.unwrap();
        assert_eq!(last.len(), 17);
        assert!(processor.emission.is_none());
        let mut observed = std::collections::BTreeMap::new();
        for chunk in [&first, &last] {
            let keys = chunk.columns()[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let counts = chunk.columns()[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..chunk.len() {
                assert!(
                    observed
                        .insert(keys.value(row), counts.value(row))
                        .is_none()
                );
            }
        }
        assert_eq!(observed, (0..groups as i64).map(|key| (key, 1)).collect());
        drop(original);
        drop(processor);
        drop(error);
        assert!(
            tracker.current() > 0,
            "returned pages retain their actual construction"
        );
        drop(first);
        assert!(
            tracker.current() > 0,
            "the final page is an independent storage owner"
        );
        drop(last);
        assert_eq!(tracker.current(), 0);
    }
}

#[cfg(test)]
#[path = "ds_hll_local_phase_factory_tests.rs"]
mod ds_hll_local_phase_factory_tests;

#[cfg(test)]
#[path = "copy_root_scratch_host_tests.rs"]
mod copy_root_scratch_host_tests;

#[cfg(test)]
#[path = "copy_child_scratch_host_tests.rs"]
mod copy_child_scratch_host_tests;

#[cfg(test)]
#[path = "copy_joint_take_host_tests.rs"]
mod copy_joint_take_host_tests;

#[cfg(test)]
#[path = "copy_original_concat_baseline_tests.rs"]
mod original_concat_baseline_tests;

#[cfg(test)]
mod schema_prepare_tests {
    use super::aggregate_fixture::{
        CallSpec, add_aggregate, aggregate_catalog, bind, compile, finish, packages, values,
    };
    use super::family_fixture::{FixtureControl, int64};
    use super::*;
    use novarocks_functions::PureKernelAbi;
    use novarocks_physical_plan::{
        AggregateCallId, AggregateGrouping, AggregatePhase, FragmentBuilder, FragmentId,
        FragmentSink, PlanBuilder, PlanVersionId, ResultField, ResultPort,
    };

    fn program(name: &str, argument: FunctionValueType) -> Arc<LocalProgram> {
        let catalogue = if name == "count" {
            aggregate_fixture::extrema_catalog()
        } else {
            aggregate_catalog(&[(name, PureKernelAbi::AggregateV1)])
        };
        let mut bound = bind(&catalogue, name, std::slice::from_ref(&argument));
        // These are the fixture call's original lexical options. The state
        // interpretation is published with the call and merely forwarded by
        // freezing/preparation; consumers never manufacture missing facts.
        bound.state_interpretation = Some(Arc::new(
            novarocks_type_contract::AggregateStateInterpretation {
                distinct: false,
                order_keys: novarocks_type_contract::AggregateStateOrderKeys::from_pure_boxed(
                    Box::new([]),
                ),
            },
        ));
        let fragment = FragmentId::new(1);
        let mut builder = FragmentBuilder::new(fragment);
        let source = builder.reserve_node_id().unwrap();
        let arguments = values(&mut builder, source, &[argument], &[]);
        let (root, _) = add_aggregate(
            &mut builder,
            source,
            &[],
            &[CallSpec {
                bound: &bound,
                phase: AggregatePhase::Single,
                id: AggregateCallId::new(1),
                arguments,
                distinct: false,
            }],
            AggregateGrouping::Complete,
        );
        let fragment = finish(builder, root, FragmentSink::Result, 2);
        let output = fragment.nodes()[&root].output.clone();
        let result = bound.result_type();
        let mut builder = PlanBuilder::new(PlanVersionId::try_new([191; 16]).unwrap());
        builder.add_fragment(fragment).unwrap();
        builder
            .set_result_port(ResultPort {
                scalar_schema: None,
                fragment: FragmentId::new(1),
                fields: vec![ResultField {
                    name: "result".into(),
                    alias: None,
                    value: output.columns[0],
                    domain: crate::test_result_domain::result_value_domain(&result),
                    ty: result,
                }]
                .into_boxed_slice(),
                output,
            })
            .unwrap();
        let plan = builder.finish_observed(&FixtureControl).unwrap();
        let package = packages(&plan, &catalogue)
            .remove(&FragmentId::new(1))
            .unwrap();
        compile(package, &catalogue, 2, true)
    }

    fn factory(program: &Arc<LocalProgram>) -> CompiledAggregateProcessorFactory {
        CompiledAggregateProcessorFactory::try_new(
            program.clone(),
            program.graph().root(),
            Arc::new(RuntimeErrorState::default()),
        )
        .unwrap()
    }

    #[test]
    fn compiled_aggregate_prepare_missing_owner_precedes_empty_input_and_state() {
        let program = program("count", int64(false));
        let factory = factory(&program);
        let mut operator = factory.create(1, 0);
        let error = operator.prepare().unwrap_err();
        assert!(matches!(
            error.cause(),
            crate::runtime::fragment::ExecutionFailureCause::Construction(
                ConstructionFailure::MissingOwner,
            )
        ));
        assert_eq!(operator.prepare().unwrap_err(), error);
        let mut processor = crate::exec::pipeline::test_driver::TestDriver::new(
            operator.as_mut(),
            &RuntimeState::default(),
        );
        assert_eq!(
            processor
                .input_without_output(Chunk::default())
                .unwrap_err(),
            error
        );
        assert_eq!(processor.signal_eos().unwrap_err(), error);
        assert!(processor.drive().unwrap_err().same_occurrence(&error));
    }

    #[test]
    fn compiled_aggregate_prepare_preserves_first_real_memory_refusal() {
        let program = program("count", int64(false));
        let factory = factory(&program);
        let tracker = MemTracker::new_root("aggregate first schema refusal");
        let mut operator = factory.create(1, 0);
        operator.set_construction_factory(aggregate_fixture::runtime_construction_factory(
            tracker.clone(),
            factory.error.clone(),
        ));
        let installed_bytes = tracker.current();
        assert!(installed_bytes > 0);
        tracker.install_limit_once(installed_bytes + 1).unwrap();
        let first = operator.prepare().unwrap_err();
        assert!(matches!(
            first.cause(),
            crate::runtime::fragment::ExecutionFailureCause::Construction(
                ConstructionFailure::Memory(_),
            )
        ));
        assert_eq!(tracker.current(), installed_bytes);
        assert_eq!(operator.prepare().unwrap_err(), first);
        let mut processor = crate::exec::pipeline::test_driver::TestDriver::new(
            operator.as_mut(),
            &RuntimeState::default(),
        );
        assert_eq!(processor.signal_eos().unwrap_err(), first);
        assert_eq!(
            processor
                .input_without_output(Chunk::default())
                .unwrap_err(),
            first
        );
        assert_eq!(tracker.current(), installed_bytes);
        drop(operator);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn compiled_aggregate_prepared_schema_last_slot_keeps_finite_creator_without_program() {
        let program = program("count", int64(false));
        let weak = Arc::downgrade(&program);
        let factory = factory(&program);
        let tracker = MemTracker::new_root("aggregate schema independent last slot");
        let mut operator = factory.create(1, 0);
        operator.set_construction_factory(aggregate_fixture::runtime_construction_factory(
            tracker.clone(),
            factory.error.clone(),
        ));
        operator.set_mem_tracker(tracker.clone());
        operator.prepare().unwrap();
        operator.finish_for_test(&RuntimeState::default()).unwrap();
        let output = crate::exec::pipeline::test_driver::TestDriver::new(
            operator.as_mut(),
            &RuntimeState::default(),
        )
        .advance_to_boundary()
        .map(|step| step.output)
        .unwrap()
        .unwrap();
        let schema = output.chunk_schema_ref();
        let slot = schema.slots()[0].clone();
        drop(operator);
        drop(factory);
        drop(program);
        assert!(weak.upgrade().is_none());
        drop(output);
        assert!(tracker.current() > 0);
        drop(schema);
        assert!(tracker.current() > 0);
        drop(slot);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn compiled_aggregate_local_prepared_intermediate_preserves_nested_json_and_phase_nulls() {
        let json = FunctionValueType::try_with_logical_type(
            DataType::Utf8,
            true,
            novarocks_type_contract::ValueLogicalType::Json,
        )
        .unwrap();
        let program = program("array_agg", json);
        let (partial, final_stage) = factory(&program).into_local_update_stages().unwrap();
        let expected = partial.calls[0].contract.intermediate_type().clone();
        let tracker = MemTracker::new_root("aggregate complete intermediate declaration");
        let mut partial_operator = partial.create(2, 0);
        partial_operator.set_construction_factory(aggregate_fixture::runtime_construction_factory(
            tracker.clone(),
            partial.error.clone(),
        ));
        partial_operator.set_mem_tracker(tracker.clone());
        partial_operator.prepare().unwrap();
        partial_operator
            .finish_for_test(&RuntimeState::default())
            .unwrap();
        let intermediate = crate::exec::pipeline::test_driver::TestDriver::new(
            partial_operator.as_mut(),
            &RuntimeState::default(),
        )
        .advance_to_boundary()
        .map(|step| step.output)
        .unwrap()
        .unwrap();
        let slot = partial.output_layout.slots()[partial.groups];
        let actual = intermediate
            .chunk_schema()
            .slot(slot)
            .unwrap()
            .typed_binding();
        assert_eq!(actual, expected.typed_binding());
        assert_eq!(actual.nullable(), expected.nullable());
        // The actual single-argument ABI is List<Json>; ordered channels would
        // produce a Struct of Lists through a different original declaration.
        let nested = actual.borrowed().child_at(0).unwrap();
        assert_eq!(
            nested.value_type().domain(),
            Some(novarocks_type_contract::ValueLogicalType::Json)
        );
        let mut final_operator = final_stage.create(1, 0);
        final_operator.set_construction_factory(aggregate_fixture::runtime_construction_factory(
            tracker.clone(),
            final_stage.error.clone(),
        ));
        final_operator.set_mem_tracker(tracker.clone());
        final_operator.prepare().unwrap();
        crate::exec::pipeline::test_driver::TestDriver::new(
            final_operator.as_mut(),
            &RuntimeState::default(),
        )
        .input_without_output(intermediate)
        .unwrap();
        final_operator
            .finish_for_test(&RuntimeState::default())
            .unwrap();
        let output = crate::exec::pipeline::test_driver::TestDriver::new(
            final_operator.as_mut(),
            &RuntimeState::default(),
        )
        .advance_to_boundary()
        .map(|step| step.output)
        .unwrap()
        .unwrap();
        assert_eq!(
            output.chunk_schema().slots()[0].typed_binding(),
            final_stage.calls[0].contract.final_type().typed_binding()
        );
        drop(output);
        drop(final_operator);
        drop(partial_operator);
        assert_eq!(tracker.current(), 0);
    }
}
