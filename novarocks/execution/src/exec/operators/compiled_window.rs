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

//! Compiled Analytic (window) evaluation over pure window kernels.
//!
//! One driver owns one processor; the builder gathers the instance input to
//! that one driver. The input arrives sorted by the partition keys and then by
//! the order keys: the checked plan places an analytic or global Sort below
//! every window that partitions or orders, and the compiler admits only that
//! source. The processor relies on that order and never re-sorts.
//!
//! Evaluation boundary: every partition key, order key and call argument is a
//! compiled root (`WindowPartition`, `WindowOrder`, `WindowInput`), evaluated
//! by this driver's own instance exactly once per arriving row, in arrival
//! order; a row data error of any root is a required error. Each call runs
//! through the exact prepared window kernel the compiler attached at its
//! `ProgramCallSite::Window`. No implementation is selected by name and no
//! legacy expression is read.
//!
//! Equality: two rows are in one partition when their normalized partition
//! keys are equal, and peers when their normalized order keys are equal.
//! Equality is that of the sort's total order, through the Arrow row format of
//! the same normalized key arrays the compiled Sort orders by: NULL equals
//! NULL, a float equals only the same bits (one NaN equals itself, and `-0.0`
//! and `+0.0` differ, exactly as the sort separates them).
//!
//! Partitions: rows of the open partition are buffered with their evaluated
//! keys and arguments. A partition closes when a row with different partition
//! keys arrives or at finish; its peer groups and each call's frame table are
//! computed over the complete partition, every call's kernel evaluates it
//! once, and the partition's rows are emitted with their call values. A whole
//! partition is held in memory (no spill, ADR-0162).

use crate::exec::fragment::error::{
    FragmentBindingErrorKind as PlanBindingKind, program_binding_failure,
};
use crate::exec::pipeline::operator::OperatorLifecycle;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::compute::{SortOptions, concat};
use arrow::row::{OwnedRow, Rows};
use novarocks_functions::{
    AggregateStateAllocator, EvaluatedArgument, FullPartitionWindowInput, KernelEvaluationControl,
    KernelFailure, PreparedWindowKernel, Selection, WindowEvaluationPartition,
    WindowInvocationContext, WindowInvocationScratch, WindowPartitionInput, WindowResultCarrier,
    WindowRowRange,
};
use novarocks_local_program::{
    AnalyticOutputColumn, LocalProgram, ProgramCallSite, ProgramExpressionRootSite,
    ProgramNodeExpressionRole, ProgramNodeId, ProgramNodeKind, ProgramStateTemplate, WindowFrame,
    WindowFunctionKind,
};

#[path = "compiled_window_invocation_input.rs"]
mod invocation_input;
use invocation_input::InvocationInputBuffer;

use super::analytic_shared::{
    AnalyticOutputValidationFailure, validate_analytic_output_columns_typed,
};
use super::compiled_expression::{RuntimeKernelControl, evaluate_all, instances};
use super::compiled_window_geometry::{admit_frame, frame_table, frame_table_tracked, peer_groups};
use super::sort::creation::{CreatedSortConverter, SortCreation};
use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::expr::compiled_program::CompiledExpressionInstance;
use crate::exec::pipeline::operator::{ConstructionFailure, InputConstructionOwner};
use crate::exec::pipeline::operator::{InputEncodingPolicy, Operator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::{
    driver_step::DriverStep,
    input::PreparedInput,
    shape::{Transform, TransformKernel},
    step::{NextStep, StepResult},
};
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};
use crate::runtime::task::{ExecutionFailure, ExecutionResult};
use novarocks_type_contract::ValueTypeStorageLifetime;

#[derive(Clone, Copy, Debug)]
enum WindowArrowOperation {
    EncodeKeys,
    ConcatenatePartitions,
}
#[derive(Debug)]
struct WindowArrowFailure {
    operation: WindowArrowOperation,
    source: arrow::error::ArrowError,
}
impl std::fmt::Display for WindowArrowFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "compiled window {:?}: {}", self.operation, self.source)
    }
}
impl std::error::Error for WindowArrowFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl novarocks_failure_contract::FailureProjection for WindowArrowFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        let (class, reason) = match &self.source {
            arrow::error::ArrowError::MemoryError(_) => (
                novarocks_failure_contract::FailureClass::Resource,
                crate::runtime::task::ExecutionReason::WindowArrowAllocation,
            ),
            _ => (
                novarocks_failure_contract::FailureClass::Invariant,
                crate::runtime::task::ExecutionReason::WindowArrow,
            ),
        };
        novarocks_failure_contract::FailureFact::new(class, reason.reason())
    }
}
fn arrow_failure(
    operation: WindowArrowOperation,
    source: arrow::error::ArrowError,
) -> ExecutionFailure {
    ExecutionFailure::from_domain(WindowArrowFailure { operation, source })
}

/// One call's exact prepared kernel, its explicit frame and its roots.
#[derive(Clone)]
struct CallPlan {
    kernel: Arc<dyn PreparedWindowKernel>,
    frame: WindowFrame,
    /// Offset of this call's argument roots in the processor's root list.
    first_root: usize,
    roots: usize,
}

/// Where one output column comes from.
#[derive(Clone, Copy)]
enum OutputSource {
    Input(usize),
    Call(usize),
}

pub struct CompiledWindowProcessorFactory {
    name: String,
    program: Arc<LocalProgram>,
    sites: Vec<ProgramExpressionRootSite>,
    partition_keys: usize,
    order_keys: usize,
    calls: Vec<CallPlan>,
    columns: Vec<OutputSource>,
    output_node: ProgramNodeId,
    error: Arc<RuntimeErrorState>,
    original_function_major: bool,
}

impl CompiledWindowProcessorFactory {
    pub(crate) fn try_new(
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        error: Arc<RuntimeErrorState>,
    ) -> ExecutionResult<Self> {
        let at = node.index();
        let nodes = program.graph().nodes();
        let graph_node = nodes.get(at).ok_or_else(|| {
            program_binding_failure(
                PlanBindingKind::MissingAssignment,
                format!("compiled Analytic node {at} is absent"),
            )
        })?;
        let ProgramNodeKind::Analytic {
            input,
            partition_exprs,
            order_by_exprs,
            functions,
            output_columns,
        } = graph_node.kind()
        else {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                format!("compiled local node {at} is not an Analytic"),
            ));
        };
        let key = |count: usize, role: fn(u32) -> ProgramNodeExpressionRole| {
            (0..count)
                .map(|key| {
                    u32::try_from(key)
                        .map(|key| ProgramExpressionRootSite::Node {
                            node,
                            role: role(key),
                        })
                        .map_err(|_| {
                            program_binding_failure(
                                PlanBindingKind::InvalidAssignment,
                                format!("compiled Analytic at local node {at} has too many keys"),
                            )
                        })
                })
                .collect::<ExecutionResult<Vec<_>>>()
        };
        let mut sites = key(partition_exprs.len(), |key| {
            ProgramNodeExpressionRole::WindowPartition { key }
        })?;
        sites.extend(key(order_by_exprs.len(), |key| {
            ProgramNodeExpressionRole::WindowOrder { key }
        })?);
        let mut calls = Vec::with_capacity(functions.len());
        for (ordinal, function) in functions.iter().enumerate() {
            let call = novarocks_local_program::checked_program_ordinal(ordinal)
                .map_err(crate::runtime::task::ExecutionFailure::from_domain)?;
            if !matches!(function.kind, WindowFunctionKind::Prepared) {
                return Err(program_binding_failure(
                    PlanBindingKind::InvalidAssignment,
                    format!(
                        "compiled Analytic call {call} at local node {at} is not a prepared window call"
                    ),
                ));
            }
            let frame = function.frame.ok_or_else(|| {
                program_binding_failure(
                    PlanBindingKind::MissingAssignment,
                    format!(
                        "compiled Analytic call {call} at local node {at} has no explicit frame"
                    ),
                )
            })?;
            admit_compiled_frame(at, call, &frame)?;
            let Some(ProgramStateTemplate::WindowPartition { kernel, .. }) =
                program.state_template(ProgramCallSite::Window { node, call })
            else {
                return Err(program_binding_failure(
                    PlanBindingKind::InvalidAssignment,
                    format!(
                        "compiled Analytic call {call} at local node {at} has no prepared window kernel"
                    ),
                ));
            };
            let kernel = Arc::clone(kernel);
            let contract = kernel.contract();
            if contract.order_argument_types().len() != 0 {
                return Err(program_binding_failure(
                    PlanBindingKind::InvalidAssignment,
                    format!(
                        "compiled Analytic call {call} at local node {at} with function ORDER BY is not executable"
                    ),
                ));
            }
            let roots = contract.logical_argument_types().len();
            if function.args.len() != roots {
                return Err(program_binding_failure(
                    PlanBindingKind::InvalidAssignment,
                    format!(
                        "compiled Analytic call {call} at local node {at} has {} roots for {roots} channels",
                        function.args.len()
                    ),
                ));
            }
            let first_root = sites.len();
            for argument in 0..roots {
                let argument = u32::try_from(argument).map_err(|_| {
                    program_binding_failure(
                        PlanBindingKind::InvalidAssignment,
                        format!(
                            "compiled Analytic call {call} at local node {at} has too many inputs"
                        ),
                    )
                })?;
                sites.push(ProgramExpressionRootSite::Node {
                    node,
                    role: ProgramNodeExpressionRole::WindowInput { call, argument },
                });
            }
            calls.push(CallPlan {
                kernel,
                frame,
                first_root,
                roots,
            });
        }
        let input_slots = nodes
            .get(input.index())
            .ok_or_else(|| {
                program_binding_failure(
                    PlanBindingKind::MissingAssignment,
                    format!("compiled Analytic at local node {at} has no input node"),
                )
            })?
            .output_layout()
            .slots();
        let mut columns = Vec::with_capacity(output_columns.len());
        for column in output_columns {
            columns.push(match column {
                AnalyticOutputColumn::InputSlotId(slot) => OutputSource::Input(
                    input_slots
                        .iter()
                        .position(|candidate| candidate == slot)
                        .ok_or_else(|| {
                            program_binding_failure(PlanBindingKind::MissingAssignment, format!(
                                "compiled Analytic at local node {at} outputs slot {} its input lacks",
                                slot.as_u32()
                            ))
                        })?,
                ),
                AnalyticOutputColumn::Window(call) if *call < calls.len() => {
                    OutputSource::Call(*call)
                }
                AnalyticOutputColumn::Window(call) => {
                    return Err(program_binding_failure(PlanBindingKind::InvalidAssignment, format!(
                        "compiled Analytic at local node {at} outputs absent call {call}"
                    )));
                }
            });
        }
        if graph_node.output_layout().schema().fields().len() != columns.len() {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                format!(
                    "compiled Analytic at local node {at} output width differs from its columns"
                ),
            ));
        }
        let (partition_keys, order_keys) = (partition_exprs.len(), order_by_exprs.len());
        // Positive immutable owner capability, never a function-name decision.
        let original_function_major = calls
            .iter()
            .any(|call| call.kernel.has_original_invocation_handoff());

        Ok(Self {
            name: format!("COMPILED_ANALYTIC (node={at})"),
            program,
            sites,
            partition_keys,
            order_keys,
            calls,
            columns,
            output_node: node,
            error,
            original_function_major,
        })
    }
}

impl CompiledWindowProcessorFactory {
    #[cfg(test)]
    pub(crate) fn bind_runtime_memory_for_test(
        &self,
        state: &RuntimeState,
    ) -> ExecutionResult<Option<crate::runtime::query_memory::QueryMemoryBinding>> {
        let mut processor = self.create_processor();
        processor.bind_runtime_state(state)?;
        Ok(processor.control.query_memory().cloned())
    }
    fn create_processor(&self) -> CompiledWindowProcessor {
        CompiledWindowProcessor {
            name: self.name.clone(),
            program: Arc::clone(&self.program),
            sites: self.sites.clone(),
            partition_keys: self.partition_keys,
            order_keys: self.order_keys,
            calls: self.calls.clone(),
            columns: self.columns.clone(),
            output_node: self.output_node,
            output: None,
            construction: None,
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            instances: None,
            partition_key_types: None,
            order_key_types: None,
            partition_rows: None,
            order_rows: None,
            last_partition: None,
            open: Vec::new(),
            emission: None,
            failed: None,
            original_function_major: self.original_function_major,
            invocation_input: None,
            partition_ordinal: 0,
        }
    }
}

impl OperatorFactory for CompiledWindowProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(Transform::hydrating(self.create_processor()))
    }
}

/// Rows of the open partition with their evaluated order keys and arguments.
struct Piece {
    batch: Chunk,
    order: Vec<ArrayRef>,
    arguments: Vec<ArrayRef>,
    _key_storage: novarocks_type_contract::ValueTypeStorageLifetime,
}

struct CompiledWindowProcessor {
    name: String,
    program: Arc<LocalProgram>,
    sites: Vec<ProgramExpressionRootSite>,
    partition_keys: usize,
    order_keys: usize,
    calls: Vec<CallPlan>,
    columns: Vec<OutputSource>,
    output_node: ProgramNodeId,
    output: Option<ChunkSchemaRef>,
    construction: Option<Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>>,
    control: RuntimeKernelControl,
    instances: Option<Vec<CompiledExpressionInstance>>,
    /// Row encoders of the normalized partition and order keys, created from
    /// the first evaluated keys; one encoder owns every comparison.
    partition_key_types: Option<novarocks_type_contract::ValueTypeStorageLifetime>,
    order_key_types: Option<novarocks_type_contract::ValueTypeStorageLifetime>,
    partition_rows: Option<CreatedSortConverter>,
    order_rows: Option<CreatedSortConverter>,
    /// The partition keys of the last arrived row.
    last_partition: Option<CreatedPartitionKey>,
    open: Vec<Piece>,
    emission: Option<WindowOutputCursor>,
    failed: Option<ExecutionFailure>,
    original_function_major: bool,
    invocation_input: Option<InvocationInputBuffer>,
    partition_ordinal: usize,
}

/// The complete relation is computed in the original function-major order.
/// Only one original arrival-sized wrapper is constructed per output step.
struct WindowOutputCursor {
    output: Chunk,
    lengths: Vec<usize>,
    index: usize,
    offset: usize,
    _length_storage: ValueTypeStorageLifetime,
}

/// The row encoding owns shared converter fields independently of the converter.
/// Row bytes and offsets remain under the original window payload policy.
struct CreatedWindowRows {
    rows: Rows,
    storage: ValueTypeStorageLifetime,
}
impl std::ops::Deref for CreatedWindowRows {
    type Target = Rows;
    fn deref(&self) -> &Rows {
        &self.rows
    }
}
struct CreatedPartitionKey {
    row: OwnedRow,
    _storage: ValueTypeStorageLifetime,
}

/// Row-encode normalized keys with the processor's one encoder for them.
fn encode(
    converter: &mut Option<CreatedSortConverter>,
    keys: &[ArrayRef],
    type_storage: ValueTypeStorageLifetime,
    creation: &SortCreation<'_>,
) -> ExecutionResult<CreatedWindowRows> {
    if converter.is_none() {
        *converter = Some(creation.row_converter(
            keys,
            type_storage,
            std::iter::repeat_n(SortOptions::default(), keys.len()),
        )?);
    }
    let converter = converter.as_ref().expect("converter was created");
    let rows = converter
        .converter()
        .convert_columns(keys)
        .map_err(|error| arrow_failure(WindowArrowOperation::EncodeKeys, error))?;
    Ok(CreatedWindowRows {
        rows,
        storage: converter.storage_lifetime(),
    })
}

/// Original RecordBatch slice, with its actual wrapper constructors admitted
/// before construction and finite source custody carried to the new wrapper.
/// This does not call a new payload transfer or change SchemeS accounting.
fn slice_body(
    source: &Chunk,
    offset: usize,
    len: usize,
    factory: &dyn crate::exec::pipeline::operator::InputConstructionFactory,
) -> ExecutionResult<Chunk> {
    let owner = factory.begin_input_construction(source)?;
    owner.observe(
        0,
        &[],
        crate::exec::chunk::ArrayNormalizationStep::CreateChunkColumns {
            channels: source.columns().len(),
        },
    )?;
    for array in source.columns() {
        owner.observe(
            0,
            &[],
            crate::exec::chunk::ArrayNormalizationStep::SliceArray {
                source: array,
                offset,
                length: len,
            },
        )?;
    }
    let storage = source.source_storage_lifetime_observed::<ConstructionFailure>(
        owner.storage_lifetime(),
        |layout| owner.request(layout),
    )?;
    let batch = source.record_batch().slice(offset, len);
    Ok(Chunk::try_new_typed_record_batch_owned_observed::<
        ConstructionFailure,
        _,
        _,
    >(
        batch,
        source.chunk_schema_ref(),
        storage,
        owner.storage_lifetime(),
        |layout| owner.request(layout),
        |step| owner.observe(0, &[], step),
    )?)
}

/// Maximal adjacent groups using the ONE original semantic equality author.
/// Every demanded key occurrence observes its real control; no decoded value,
/// guessed dictionary expansion, or second comparison algorithm is introduced.
fn original_peer_groups(
    keys: &[ArrayRef],
    start: usize,
    end: usize,
    host: Arc<dyn AggregateStateAllocator>,
    control: &dyn KernelEvaluationControl,
) -> ExecutionResult<WindowInvocationScratch<WindowRowRange>> {
    let mut groups = WindowInvocationScratch::try_with_capacity(
        if keys.is_empty() {
            usize::from(start != end)
        } else {
            end - start
        },
        host,
        control,
    )?;
    if start == end {
        return Ok(groups);
    }
    let mut first = start;
    for row in start + 1..end {
        let mut equal = true;
        for key in keys {
            control.checkpoint(1)?;
            if !super::analytic_shared::row_equal_on_keys(std::slice::from_ref(key), row - 1, row)?
            {
                equal = false;
                break;
            }
        }
        if !equal {
            groups.try_push(
                WindowRowRange {
                    start: first,
                    end: row,
                },
                control,
            )?;
            first = row;
        }
    }
    groups.try_push(WindowRowRange { start: first, end }, control)?;
    Ok(groups)
}

impl CompiledWindowProcessor {
    fn release_owned(&mut self) {
        self.emission = None;
        self.instances = None;
        self.invocation_input = None;
        self.partition_rows = None;
        self.order_rows = None;
        self.last_partition = None;
        self.partition_key_types = None;
        self.order_key_types = None;
        drop(std::mem::take(&mut self.open));
    }

    fn prepared_output(&self) -> ExecutionResult<&ChunkSchemaRef> {
        self.output.as_ref().ok_or_else(|| {
            crate::exec::pipeline::operator::ConstructionFailure::MissingOwner.into()
        })
    }

    fn failure_result<T>(&mut self, result: ExecutionResult<T>) -> ExecutionResult<T> {
        if let Some(cause) = &self.failed {
            return Err(cause.clone());
        }
        match result {
            Ok(value) => Ok(value),
            Err(cause) => {
                self.failed = Some(cause);
                self.release_owned();
                Err(self
                    .failed
                    .as_ref()
                    .expect("first window failure retained")
                    .clone())
            }
        }
    }

    fn consume_invocation(&mut self, chunk: Chunk) -> ExecutionResult<()> {
        if self.invocation_input.is_none() {
            let host = self.control.allocator().ok_or_else(|| {
                KernelFailure::InvalidProgram(novarocks_functions::KernelDiagnostic::new(
                    "tracked window invocation requires its actual allocator",
                ))
            })?;
            self.invocation_input = Some(InvocationInputBuffer::try_new(host, &self.control)?);
        }
        self.invocation_input
            .as_mut()
            .expect("input owner installed")
            .push(chunk, &self.control)?;
        Ok(())
    }
    fn finish_invocation(&mut self) -> ExecutionResult<()> {
        use novarocks_functions::{
            FullWindowInvocationInput, WindowEvaluationInvocation, WindowInvocationInput,
        };
        // Check the real capability even for an empty source. No default host.
        let host = self.control.allocator().ok_or_else(|| {
            KernelFailure::InvalidProgram(novarocks_functions::KernelDiagnostic::new(
                "tracked window invocation requires its actual allocator",
            ))
        })?;
        if host.opaque_allocation_host().is_none() {
            return Err(
                KernelFailure::InvalidProgram(novarocks_functions::KernelDiagnostic::new(
                    "tracked window invocation requires its actual opaque host",
                ))
                .into(),
            );
        }
        let Some(input_owner) = self.invocation_input.take() else {
            return Ok(());
        };
        let input = input_owner.chunks();
        if input.is_empty() {
            return Ok(());
        }
        // This is the ONE original input-gather author; source backing and its
        // existing Chunk accounting lease survive through final splitting.
        let mut ordered = super::analytic_shared::concat_original_analytic_input(input)?;
        let rows = ordered.len();
        if rows == 0 {
            return Ok(());
        }
        instances(
            &mut self.instances,
            &self.program,
            &self.sites,
            &self.control,
        )?;
        let instances = self.instances.as_mut().expect("instances created");
        let mut keys = WindowInvocationScratch::try_with_capacity(
            self.partition_keys + self.order_keys,
            host.clone(),
            &self.control,
        )?;
        for (root, instance) in instances
            .iter_mut()
            .enumerate()
            .take(self.partition_keys + self.order_keys)
        {
            keys.try_push(
                evaluate_all(
                    instance,
                    self.sites[root],
                    ordered.record_batch(),
                    &self.control,
                )?,
                &self.control,
            )?;
        }
        let regroup = novarocks_functions::window_input_order::should_regroup_partition_only(
            self.order_keys != 0,
            self.calls
                .iter()
                .any(|call| call.kernel.contract().options().frame().is_some()),
            self.calls
                .iter()
                .map(|call| call.kernel.original_partition_regroup_eligible()),
        );
        if rows > 1 && self.partition_keys != 0 && regroup {
            ordered = super::analytic_shared::reorder_chunk_by_partition_keys(
                &ordered,
                &keys[..self.partition_keys],
            )?;
            // Original source really re-invokes these roots even when the
            // permutation happened to be identity. No source CSE is invented.
            for (root, instance) in instances
                .iter_mut()
                .enumerate()
                .take(self.partition_keys + self.order_keys)
            {
                keys.as_mut_slice()[root] = evaluate_all(
                    instance,
                    self.sites[root],
                    ordered.record_batch(),
                    &self.control,
                )?;
            }
        }
        let batch = ordered.record_batch();
        // The original adjacency author compares the actual keys directly.
        // It preserves signed zero / NaN bit identity and original supported
        // key errors, without a new normalized-key or RowConverter allocation.
        let partitions = original_peer_groups(
            &keys[..self.partition_keys],
            0,
            rows,
            host.clone(),
            &self.control,
        )?;
        let order = &keys[self.partition_keys..];
        let mut peers =
            WindowInvocationScratch::try_with_capacity(rows, host.clone(), &self.control)?;
        for part in &partitions {
            for peer in
                original_peer_groups(order, part.start, part.end, host.clone(), &self.control)?
                    .iter()
            {
                peers.try_push(*peer, &self.control)?;
            }
        }
        let mut results = WindowInvocationScratch::try_with_capacity(
            self.calls.len(),
            host.clone(),
            &self.control,
        )?;
        for (call_ordinal, call) in self.calls.iter().enumerate() {
            // Original function-major order: this call's full-input argument
            // uses precede THIS call's all-partition work, not later call roots.
            let mut arguments = WindowInvocationScratch::try_with_capacity(
                call.roots,
                host.clone(),
                &self.control,
            )?;
            for (root, instance) in instances
                .iter_mut()
                .enumerate()
                .skip(call.first_root)
                .take(call.roots)
            {
                arguments.try_push(
                    evaluate_all(
                        instance,
                        self.sites[root],
                        ordered.record_batch(),
                        &self.control,
                    )?,
                    &self.control,
                )?;
            }
            if call.kernel.has_original_invocation_handoff() {
                let mut frames =
                    WindowInvocationScratch::try_with_capacity(rows, host.clone(), &self.control)?;
                for part in &partitions {
                    let mut local_peers = WindowInvocationScratch::try_with_capacity(
                        part.end - part.start,
                        host.clone(),
                        &self.control,
                    )?;
                    for peer in peers
                        .iter()
                        .filter(|peer| peer.start >= part.start && peer.end <= part.end)
                    {
                        local_peers.try_push(
                            WindowRowRange {
                                start: peer.start - part.start,
                                end: peer.end - part.start,
                            },
                            &self.control,
                        )?;
                    }
                    let local_frames = frame_table_tracked(
                        &call.frame,
                        &local_peers,
                        host.clone(),
                        &self.control,
                    )?;
                    for frame in local_frames.iter() {
                        frames.try_push(
                            WindowRowRange {
                                start: part.start + frame.start,
                                end: part.start + frame.end,
                            },
                            &self.control,
                        )?;
                    }
                }
                let mut logical = WindowInvocationScratch::try_with_capacity(
                    arguments.len(),
                    host.clone(),
                    &self.control,
                )?;
                for argument in arguments.iter() {
                    logical.try_push(EvaluatedArgument::Column(argument), &self.control)?;
                }
                let full = FullWindowInvocationInput::try_new(
                    call.kernel.contract().as_ref(),
                    rows,
                    &logical,
                    &[],
                    &self.control,
                )?;
                let source = WindowInvocationInput::try_new(
                    full,
                    &partitions,
                    &peers,
                    &frames,
                    &self.control,
                )?;
                let mut invocation = WindowEvaluationInvocation::begin(
                    Arc::clone(&call.kernel),
                    source,
                    WindowInvocationContext::complete_invocation(call_ordinal),
                    Some(Arc::clone(&host)),
                    &self.control,
                )?;
                let carrier = invocation.complete_carrier(&self.control)?;
                let values = Arc::clone(carrier.values());
                invocation.finish(&self.control)?;
                results.try_push(
                    CallOutput {
                        values,
                        carrier: Some(carrier),
                    },
                    &self.control,
                )?;
            } else {
                // Mixed nodes retain the existing FixedZero partition owner
                // and its errors, while preserving original call priority.
                let mut outputs = Vec::with_capacity(partitions.len());
                for (part_ordinal, part) in partitions.iter().enumerate() {
                    let arguments = slice(&arguments, part.start, part.end - part.start);
                    let logical = arguments
                        .iter()
                        .map(EvaluatedArgument::Column)
                        .collect::<Vec<_>>();
                    let mut local_peers = WindowInvocationScratch::try_with_capacity(
                        part.end - part.start,
                        host.clone(),
                        &self.control,
                    )?;
                    for peer in peers
                        .iter()
                        .filter(|peer| peer.start >= part.start && peer.end <= part.end)
                    {
                        local_peers.try_push(
                            WindowRowRange {
                                start: peer.start - part.start,
                                end: peer.end - part.start,
                            },
                            &self.control,
                        )?;
                    }
                    let local_frames =
                        frame_table(&call.frame, &local_peers).map_err(ExecutionFailure::from)?;
                    outputs.push(
                        evaluate_call(
                            &call.kernel,
                            call_ordinal,
                            part_ordinal,
                            part.end - part.start,
                            &logical,
                            &local_peers,
                            &local_frames,
                            &self.control,
                        )?
                        .values,
                    );
                }
                let values = if outputs.len() == 1 {
                    outputs.pop().expect("one partition")
                } else {
                    let parts = outputs
                        .iter()
                        .map(|array| array.as_ref())
                        .collect::<Vec<&dyn Array>>();
                    concat(&parts).map_err(|error| {
                        arrow_failure(WindowArrowOperation::ConcatenatePartitions, error)
                    })?
                };
                results.try_push(
                    CallOutput {
                        values,
                        carrier: None,
                    },
                    &self.control,
                )?;
            }
        }
        let mut columns = WindowInvocationScratch::try_with_capacity(
            self.columns.len(),
            host.clone(),
            &self.control,
        )?;
        for source in &self.columns {
            columns.try_push(
                match source {
                    OutputSource::Input(ordinal) => Arc::clone(batch.column(*ordinal)),
                    OutputSource::Call(call) => Arc::clone(&results[*call].values),
                },
                &self.control,
            )?;
        }
        validate_complete_output(
            &columns,
            &self.columns,
            &results,
            self.prepared_output()?,
            rows,
            &self.control,
        )?;
        // Keep original arrival chunk boundaries; changing them would alter
        // batch-sensitive downstream legacy semantics independently of BY.
        let factory = self
            .construction
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let owner = factory.begin_input_construction(&ordered)?;
        let creation = SortCreation::new(factory.as_ref(), self.prepared_output()?);
        let mut lengths = creation.vector::<usize>(input.len(), owner.as_ref())?;
        lengths.extend(
            input
                .iter()
                .filter(|chunk| !chunk.is_empty())
                .map(Chunk::len),
        );
        let mut output_columns = creation.vector::<ArrayRef>(columns.len(), owner.as_ref())?;
        output_columns.extend(columns.iter().cloned());
        let storage = Chunk::source_set_storage_lifetime_observed::<ConstructionFailure>(
            input,
            owner.storage_lifetime(),
            |layout| owner.request(layout),
        )?;
        let output = Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
            self.prepared_output()?.clone(),
            output_columns,
            rows,
            storage,
            |step| owner.observe(0, &[], step),
        )?;
        self.emission = Some(WindowOutputCursor {
            output,
            lengths,
            index: 0,
            offset: 0,
            _length_storage: owner.storage_lifetime(),
        });
        self.partition_rows = None;
        self.order_rows = None;
        self.partition_key_types = None;
        self.order_key_types = None;
        Ok(())
    }
    /// Evaluate every root over the arriving rows, then split them at the
    /// partition boundaries; every partition the rows close is evaluated.
    fn consume(&mut self, chunk: &Chunk) -> ExecutionResult<Option<Chunk>> {
        instances(
            &mut self.instances,
            &self.program,
            &self.sites,
            &self.control,
        )?;
        let instances = self.instances.as_mut().expect("instances were created");
        let factory = self
            .construction
            .as_ref()
            .cloned()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let references = factory.begin_input_construction(chunk)?;
        references.request(std::alloc::Layout::array::<ArrayRef>(self.sites.len())
            .map_err(|_| ConstructionFailure::Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))?)?;
        let mut values = Vec::with_capacity(self.sites.len());
        for (instance, site) in instances.iter_mut().zip(&self.sites) {
            values.push(evaluate_all(
                instance,
                *site,
                chunk.record_batch(),
                &self.control,
            )?);
        }
        let keys = self.partition_keys + self.order_keys;
        let key_factory = self
            .construction
            .as_ref()
            .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?;
        let input_schema = chunk.chunk_schema_ref();
        let key_creation =
            super::sort::creation::SortCreation::new(key_factory.as_ref(), &input_schema);
        let key_arrays = key_creation.begin(chunk)?;
        let key_types = key_creation.begin(chunk)?;
        let mut key_type_storage = key_creation.source_type_storage(chunk, key_types.as_ref())?;
        for (ordinal, key) in values.iter_mut().take(keys).enumerate() {
            let declared =
                super::compiled_expression::root_value_type(&self.program, self.sites[ordinal])?;
            key_type_storage = key_type_storage.retaining_observed::<ConstructionFailure>(
                &declared
                    .typed_binding()
                    .layout()
                    .arrow_backing_storage_lifetime(),
                |layout| key_types.request(layout),
            )?;
            *key = key_creation.normalize_key(
                key,
                declared.value_type(),
                key_arrays.as_ref(),
                key_types.as_ref(),
            )?;
        }
        let key_storage = key_arrays
            .storage_lifetime()
            .retaining_observed::<crate::exec::pipeline::operator::ConstructionFailure>(
            &key_type_storage,
            |layout| key_arrays.request(layout),
        )?;
        if self.partition_key_types.is_none() && self.partition_keys != 0 {
            self.partition_key_types = Some(key_type_storage.clone());
        }
        if self.order_key_types.is_none() && self.order_keys != 0 {
            self.order_key_types = Some(key_type_storage.clone());
        }
        let rows = chunk.len();
        // Rows at which a new partition starts; without partition keys every
        // row belongs to the one partition.
        let mut starts = Vec::new();
        if self.partition_keys > 0 {
            let encoded = encode(
                &mut self.partition_rows,
                &values[..self.partition_keys],
                key_type_storage.clone(),
                &key_creation,
            )?;
            if self
                .last_partition
                .as_ref()
                .is_none_or(|last| last.row.row() != encoded.row(0))
            {
                starts.push(0);
            }
            for row in 1..rows {
                if encoded.row(row) != encoded.row(row - 1) {
                    starts.push(row);
                }
            }
            self.last_partition = Some(CreatedPartitionKey {
                row: encoded.row(rows - 1).owned(),
                _storage: encoded.storage.clone(),
            });
        }
        let mut closed = Vec::new();
        let mut cursor = 0;
        for &start in starts.iter().chain(std::iter::once(&rows)) {
            if start > cursor {
                self.open.push(Piece {
                    _key_storage: key_storage.clone(),
                    batch: slice_body(chunk, cursor, start - cursor, factory.as_ref())?,
                    order: slice_observed(
                        &values[self.partition_keys..keys],
                        cursor,
                        start - cursor,
                        key_arrays.as_ref(),
                    )?,
                    arguments: slice_observed(
                        &values[keys..],
                        cursor,
                        start - cursor,
                        key_arrays.as_ref(),
                    )?,
                });
                cursor = start;
            }
            if start < rows && !self.open.is_empty() {
                closed.push(self.close()?);
            }
        }
        self.publish(closed)
    }

    /// Evaluate the complete open partition and return its output rows.
    fn close(&mut self) -> ExecutionResult<Chunk> {
        let pieces = std::mem::take(&mut self.open);
        let factory = self
            .construction
            .as_ref()
            .cloned()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let source = &pieces[0].batch;
        let input_schema = source.chunk_schema_ref();
        let input_creation = SortCreation::new(factory.as_ref(), &input_schema);
        let temporary = factory.begin_input_construction(source)?;
        let retained = factory.begin_input_construction(source)?;
        let batch = if pieces.len() == 1 {
            source.clone()
        } else {
            let mut input = input_creation.vector::<Chunk>(pieces.len(), temporary.as_ref())?;
            input.extend(pieces.iter().map(|piece| piece.batch.clone()));
            let batch = input_creation.concat(&input, retained.as_ref())?;
            let storage = Chunk::source_set_storage_lifetime_observed::<ConstructionFailure>(
                &input,
                retained.storage_lifetime(),
                |layout| retained.request(layout),
            )?;
            input_creation.publish_with_storage(batch, source, storage, retained.as_ref())?
        };
        let rows = batch.len();
        let join = |column: &dyn Fn(&Piece) -> &ArrayRef| -> ExecutionResult<ArrayRef> {
            if pieces.len() == 1 {
                return Ok(Arc::clone(column(&pieces[0])));
            }
            let mut sources =
                input_creation.vector::<&ArrayRef>(pieces.len(), temporary.as_ref())?;
            sources.extend(pieces.iter().map(column));
            input_creation.concat_array(&sources, source, retained.as_ref())
        };
        let mut order = input_creation.vector::<ArrayRef>(self.order_keys, retained.as_ref())?;
        for key in 0..self.order_keys {
            order.push(join(&|piece| &piece.order[key])?);
        }
        let arguments_count = self.sites.len() - self.partition_keys - self.order_keys;
        let mut arguments =
            input_creation.vector::<ArrayRef>(arguments_count, retained.as_ref())?;
        for root in 0..arguments_count {
            arguments.push(join(&|piece| &piece.arguments[root])?);
        }
        let peers = if self.order_keys == 0 {
            peer_groups(rows, |_| true)
        } else {
            let encoded = encode(
                &mut self.order_rows,
                &order,
                self.order_key_types
                    .as_ref()
                    .ok_or(ConstructionFailure::MissingOwner)?
                    .clone(),
                &input_creation,
            )?;
            peer_groups(rows, |row| encoded.row(row) == encoded.row(row - 1))
        };
        let first_argument = self.partition_keys + self.order_keys;
        let mut results = Vec::with_capacity(self.calls.len());
        for (call_ordinal, call) in self.calls.iter().enumerate() {
            let frames = frame_table(&call.frame, &peers).map_err(ExecutionFailure::from)?;
            let start = call.first_root - first_argument;
            let logical = arguments[start..start + call.roots]
                .iter()
                .map(EvaluatedArgument::Column)
                .collect::<Vec<_>>();
            results.push(evaluate_call(
                &call.kernel,
                call_ordinal,
                self.partition_ordinal,
                rows,
                &logical,
                &peers,
                &frames,
                &self.control,
            )?);
        }
        let mut columns =
            input_creation.vector::<ArrayRef>(self.columns.len(), retained.as_ref())?;
        columns.extend(self.columns.iter().map(|source| match source {
            OutputSource::Input(ordinal) => Arc::clone(batch.record_batch().column(*ordinal)),
            OutputSource::Call(call) => Arc::clone(&results[*call].values),
        }));
        // Only the new complete-carrier route invokes the original final
        // analytic validator. Existing FixedZero rows retain their old route.
        if results.iter().any(|result| result.carrier.is_some()) {
            validate_complete_output(
                &columns,
                &self.columns,
                &results,
                self.prepared_output()?,
                rows,
                &self.control,
            )?;
        }
        let storage = batch.source_storage_lifetime_observed::<ConstructionFailure>(
            retained.storage_lifetime(),
            |layout| retained.request(layout),
        )?;
        let result = Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
            self.prepared_output()?.clone(),
            columns,
            rows,
            storage,
            |step| retained.observe(0, &[], step),
        )?;
        self.partition_ordinal = self
            .partition_ordinal
            .checked_add(1)
            .ok_or(KernelFailure::ResourceExhausted)?;
        Ok(result)
    }

    /// Emit the partitions one arrival closed as one chunk, in partition order.
    fn publish(&mut self, closed: Vec<Chunk>) -> ExecutionResult<Option<Chunk>> {
        let output = match closed.len() {
            0 => return Ok(None),
            1 => closed.into_iter().next().expect("one closed partition"),
            _ => {
                let factory = self
                    .construction
                    .as_ref()
                    .ok_or(ConstructionFailure::MissingOwner)?;
                let creation = SortCreation::new(factory.as_ref(), self.prepared_output()?);
                let retained = factory.begin_input_construction(&closed[0])?;
                let batch = creation.concat(&closed, retained.as_ref())?;
                let storage = Chunk::source_set_storage_lifetime_observed::<ConstructionFailure>(
                    &closed,
                    retained.storage_lifetime(),
                    |layout| retained.request(layout),
                )?;
                creation.publish_with_storage(batch, &closed[0], storage, retained.as_ref())?
            }
        };
        Ok(Some(output))
    }
}

fn slice_observed(
    values: &[ArrayRef],
    offset: usize,
    len: usize,
    owner: &dyn InputConstructionOwner,
) -> ExecutionResult<Vec<ArrayRef>> {
    owner.request(
        std::alloc::Layout::array::<ArrayRef>(values.len()).map_err(|_| {
            ConstructionFailure::Resource(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            )
        })?,
    )?;
    let mut output = Vec::with_capacity(values.len());
    for value in values {
        owner.observe(
            0,
            &[],
            crate::exec::chunk::ArrayNormalizationStep::SliceArray {
                source: value,
                offset,
                length: len,
            },
        )?;
        output.push(value.slice(offset, len));
    }
    Ok(output)
}

// The function-major invocation keeps its original payload/scratch author.
fn slice(values: &[ArrayRef], offset: usize, len: usize) -> Vec<ArrayRef> {
    values
        .iter()
        .map(|value| value.slice(offset, len))
        .collect()
}

/// Run one call's exact kernel once over one complete partition. Its whole
/// input is supplied even when no output were demanded; the partition latches
/// any failure, which is returned as a required error.
struct CallOutput {
    values: ArrayRef,
    carrier: Option<WindowResultCarrier>,
}
fn evaluate_call(
    kernel: &Arc<dyn PreparedWindowKernel>,
    call_ordinal: usize,
    partition_ordinal: usize,
    rows: usize,
    logical: &[EvaluatedArgument<'_>],
    peers: &[WindowRowRange],
    frames: &[WindowRowRange],
    control: &RuntimeKernelControl,
) -> ExecutionResult<CallOutput> {
    let contract = Arc::clone(kernel.contract());
    let full = FullPartitionWindowInput::try_new(contract.as_ref(), rows, logical, &[], control)?;
    let input = WindowPartitionInput::try_new(full, peers, frames, control)?;
    let mut partition = WindowEvaluationPartition::begin_evaluated(
        Arc::clone(kernel),
        input,
        WindowInvocationContext::partition(call_ordinal, partition_ordinal),
        control.allocator(),
        control,
    )?;
    if let Some(carrier) = partition.complete_carrier_evaluated(control)? {
        // Original final output-layout validation is still pending. In
        // particular a raw builder panic is never caught or reclassified here.
        let values = Arc::clone(carrier.values());
        partition.finish_evaluated(control)?;
        return Ok(CallOutput {
            values,
            carrier: Some(carrier),
        });
    }
    let output = partition.evaluate_evaluated(Selection::all(rows), rows, control)?;
    partition.finish_evaluated(control)?;
    let (_, values, errors) = output.into_parts();
    validate_unmasked_window_output(
        call_ordinal,
        partition_ordinal,
        rows,
        values.len(),
        errors.len(),
    )?;
    Ok(CallOutput {
        values,
        carrier: None,
    })
}

// Count the ONE original Display without allocating or rendering a second
// diagnostic. The caller requests real host capacity before invoking the
// validator or constructing its full original String.
fn validation_message_bytes(
    error: &AnalyticOutputValidationFailure<'_>,
) -> Result<usize, KernelFailure> {
    struct Count(usize);
    impl std::fmt::Write for Count {
        fn write_str(&mut self, text: &str) -> std::fmt::Result {
            self.0 = self.0.checked_add(text.len()).ok_or(std::fmt::Error)?;
            Ok(())
        }
    }
    use std::fmt::Write;
    let mut count = Count(0);
    write!(&mut count, "{error}").map_err(|_| KernelFailure::ResourceExhausted)?;
    Ok(count.0)
}
fn validate_complete_output(
    columns: &[ArrayRef],
    sources: &[OutputSource],
    results: &[CallOutput],
    schema: &ChunkSchemaRef,
    rows: usize,
    control: &RuntimeKernelControl,
) -> ExecutionResult<()> {
    use novarocks_functions::opaque_memory::OpaqueRetainedCharge;
    use std::fmt::Write;
    control.checkpoint(0)?;
    // The envelope covers every possible diagnostic of this actual complete
    // schema/carrier. No payload value is formatted by the original validator.
    let mut bytes = validation_message_bytes(&AnalyticOutputValidationFailure::ColumnCount {
        expected: schema.slots().len(),
        actual: columns.len(),
    })?;
    for (column, (array, slot)) in columns.iter().zip(schema.slots()).enumerate() {
        bytes = bytes.max(validation_message_bytes(
            &AnalyticOutputValidationFailure::Length {
                column,
                expected_rows: rows,
                actual: array.len(),
            },
        )?);
        bytes = bytes.max(validation_message_bytes(
            &AnalyticOutputValidationFailure::Type {
                column,
                expected: slot.field().data_type(),
                actual: array.data_type(),
            },
        )?);
        control.checkpoint(1)?;
    }
    let host = control.allocator().ok_or_else(|| {
        KernelFailure::InvalidProgram(novarocks_functions::KernelDiagnostic::new(
            "complete window output requires its actual allocator",
        ))
    })?;
    let charge = OpaqueRetainedCharge::try_new(host)?;
    let reservation = charge.reserve_operation(bytes)?;
    // Complete the admitted formatter backing before the original validator
    // can publish Data. Its first Data therefore has no later fallible
    // allocation, host request, checkpoint or error footer.
    let mut message = String::new();
    message
        .try_reserve_exact(bytes)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    if let Err(error) = validate_analytic_output_columns_typed(columns, schema, rows) {
        let ordinal = error.output_ordinal().ok_or_else(|| {
            KernelFailure::Internal(novarocks_functions::KernelDiagnostic::new(
                "compiled window factory violated its exact output count",
            ))
        })?;
        let source = sources.get(ordinal).ok_or_else(|| {
            KernelFailure::Internal(novarocks_functions::KernelDiagnostic::new(
                "compiled window output source ordinal is absent",
            ))
        })?;
        let carrier = match source {
            OutputSource::Call(call) => results
                .get(*call)
                .and_then(|result| result.carrier.as_ref()),
            OutputSource::Input(_) => None,
        }
        .ok_or_else(|| {
            KernelFailure::Internal(novarocks_functions::KernelDiagnostic::new(
                "exact validated input or FixedZero output violated its immutable layout",
            ))
        })?;
        write!(&mut message, "{error}")
            .expect("original analytic Display writes the measured exact text");
        let cause = carrier.original_output_validation_data(ordinal, message, reservation);
        return Err(novarocks_functions::WindowEvaluationFailure::InvocationData(cause).into());
    }
    drop(reservation);
    control.checkpoint(0)?;
    Ok(())
}

impl OperatorLifecycle for CompiledWindowProcessor {
    #[cfg(any(test, novarocks_dev_controls))]
    fn prepared_schema_for_test(&self) -> Option<ChunkSchemaRef> {
        self.output.clone()
    }
    fn compiled_schema_preparation_site(
        &self,
    ) -> Option<(
        &Arc<LocalProgram>,
        crate::runtime::preparation_metadata::CompiledSchemaSite,
    )> {
        Some((
            &self.program,
            crate::runtime::preparation_metadata::CompiledSchemaSite::AnalyticOutput(
                self.output_node,
            ),
        ))
    }
    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_construction_factory(
        &mut self,
        factory: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
    ) {
        self.control.bind_construction_factory(factory.clone());
        self.construction = Some(factory);
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        if let Some(cause) = &self.failed {
            return Err(cause.clone());
        }
        if self.output.is_some() {
            return Ok(());
        }
        let result = (|| {
            use crate::exec::pipeline::operator::ConstructionFailure;
            let layout = self.program.graph().nodes()[self.output_node.index()].output_layout();
            let factory = self
                .construction
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?;
            self.output = Some(factory.create_compiled_chunk_schema(layout)?);
            Ok(())
        })();
        self.failure_result(result)
    }

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.control.bind_runtime_memory(state);
        Ok(())
    }
    fn set_mem_tracker(&mut self, tracker: Arc<crate::runtime::mem_tracker::MemTracker>) {
        self.control.bind_mem_tracker(tracker);
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.release_owned();
        Ok(())
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.begin_exit()
    }
    fn cancel(&mut self) {
        self.release_owned();
    }
    fn on_driver_failure(&mut self) {
        self.release_owned();
    }
}

impl InputEncodingPolicy for CompiledWindowProcessor {}
impl TransformKernel for CompiledWindowProcessor {
    fn apply(
        &mut self,
        ctx: &mut DriverStep<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<StepResult> {
        if let Some(cause) = &self.failed {
            return Err(cause.clone());
        }
        let chunk = input.into_uncovered_chunk()?;
        let rows = u64::try_from(chunk.len()).expect("supported row count fits u64");
        let result = (|| {
            self.prepared_output()?;
            if chunk.is_empty() {
                Ok(None)
            } else if self.original_function_major {
                self.consume_invocation(chunk)?;
                Ok(None)
            } else {
                self.consume(&chunk)
            }
        })();
        if let Err(error) = &result {
            ctx.report_failure(error.clone());
        }
        let output = self.failure_result(result)?;
        Ok(StepResult::new(output, NextStep::NeedInput, rows))
    }

    fn drive(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        if let Some(cause) = &self.failed {
            return Err(cause.clone());
        }
        let Some(cursor) = &self.emission else {
            return Ok(StepResult::new(None, NextStep::NeedInput, 0));
        };
        if !ctx.work().try_consume(1) {
            return Ok(StepResult::new(None, NextStep::Yield, 0));
        }
        let result = (|| {
            let factory = self
                .construction
                .as_ref()
                .ok_or(ConstructionFailure::MissingOwner)?;
            slice_body(
                &cursor.output,
                cursor.offset,
                cursor.lengths[cursor.index],
                factory.as_ref(),
            )
        })();
        if let Err(error) = &result {
            ctx.report_failure(error.clone());
        }
        let output = self.failure_result(result)?;
        let cursor = self.emission.as_mut().expect("live output cursor");
        cursor.offset += cursor.lengths[cursor.index];
        cursor.index += 1;
        let next = if cursor.index == cursor.lengths.len() {
            self.release_owned();
            NextStep::Finished
        } else {
            NextStep::Runnable
        };
        Ok(StepResult::new(Some(output), next, 0))
    }

    fn finish(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        if let Some(cause) = &self.failed {
            return Err(cause.clone());
        }
        let result: ExecutionResult<StepResult> = (|| {
            self.prepared_output()?;
            if self.original_function_major {
                self.finish_invocation()?;
                self.instances = None;
                if self.emission.is_some() {
                    return Ok(StepResult::new(None, NextStep::Runnable, 0));
                }
                self.release_owned();
                Ok(StepResult::new(None, NextStep::Finished, 0))
            } else {
                let output = if self.open.is_empty() {
                    None
                } else {
                    Some(self.close()?)
                };
                self.release_owned();
                Ok(StepResult::new(output, NextStep::Finished, 0))
            }
        })();
        if let Err(error) = &result {
            ctx.report_failure(error.clone());
        }
        self.failure_result(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array};
    use arrow::compute::{SortColumn, SortOptions, lexsort_to_indices, take};

    struct DeclarationControl;
    impl novarocks_type_contract::PureCompileControl for DeclarationControl {
        fn checkpoint(
            &self,
            _: novarocks_type_contract::CompilePhase,
            _: u32,
        ) -> Result<(), novarocks_type_contract::CompileControlError> {
            Ok(())
        }
    }

    /// Peers of `keys` after ordering them as the compiled Sort does.
    fn sorted_peers(
        keys: ArrayRef,
        declared_type: arrow::datatypes::DataType,
    ) -> Vec<WindowRowRange> {
        use novarocks_type_contract::{TypedValueBinding, ValueKind, ValueLayout, ValueTypeRef};
        let storage = ValueTypeStorageLifetime::pure();
        let binding = TypedValueBinding::try_new(
            ValueTypeRef::try_new(
                ValueKind::Scalar(declared_type.clone()),
                true,
                storage.clone(),
            )
            .unwrap(),
            ValueLayout::try_new(&declared_type, storage.clone()).unwrap(),
        )
        .unwrap();
        let layout = novarocks_local_program::StaticLayout::try_new_typed_for_compile(
            Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("key", declared_type, true),
            ])),
            Arc::from([novarocks_types::SlotId::new(1)]),
            vec![(binding.clone(), None)],
            storage,
            |_| Ok(()),
            &DeclarationControl,
        )
        .unwrap();
        let state = RuntimeState::default();
        let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            crate::runtime::mem_tracker::MemTracker::new_root("window-key-equality-scaffolds"),
            state.error_state(),
        ).unwrap();
        let schema = factory.create_compiled_chunk_schema(&layout).unwrap();
        let creation = SortCreation::new(factory.as_ref(), &schema);
        let arrays = factory.begin_typed_construction(&schema).unwrap();
        let types = factory.begin_typed_construction(&schema).unwrap();
        let keys = creation
            .normalize_key(&keys, binding.value_type(), arrays.as_ref(), types.as_ref())
            .unwrap();
        let indices = lexsort_to_indices(
            &[SortColumn {
                values: Arc::clone(&keys),
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: true,
                }),
            }],
            None,
        )
        .unwrap();
        let sorted = take(keys.as_ref(), &indices, None).unwrap();
        let encoded = encode(
            &mut None,
            &[sorted],
            binding.layout().storage_lifetime(),
            &creation,
        )
        .unwrap();
        peer_groups(encoded.num_rows(), |row| {
            encoded.row(row) == encoded.row(row - 1)
        })
    }

    fn ranges(values: &[(usize, usize)]) -> Vec<WindowRowRange> {
        values
            .iter()
            .map(|&(start, end)| WindowRowRange { start, end })
            .collect()
    }

    #[test]
    fn key_equality_is_the_sort_total_order() {
        // NULL equals NULL; one NaN equals itself; -0.0 and +0.0 are distinct
        // adjacent runs, exactly as the sort separates them.
        let floats: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(0.0),
            Some(f64::NAN),
            None,
            Some(-0.0),
            Some(1.5),
            None,
            Some(f64::NAN),
            Some(-0.0),
        ]));
        assert_eq!(
            sorted_peers(floats, arrow::datatypes::DataType::Float64),
            ranges(&[(0, 2), (2, 4), (4, 5), (5, 6), (6, 8)])
        );
        let integers: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(2),
            None,
            Some(2),
            Some(-1),
            None,
        ]));
        assert_eq!(
            sorted_peers(integers, arrow::datatypes::DataType::Int64),
            ranges(&[(0, 2), (2, 3), (3, 5)])
        );
    }
}

/// The real factory retains frozen declarations; prepare owns runtime creation.
#[cfg(novarocks_dev_controls)]
pub fn prepare_window_factory_for_test(
    program: Arc<LocalProgram>,
    node: ProgramNodeId,
    error: Arc<RuntimeErrorState>,
) -> ExecutionResult<CompiledWindowProcessorFactory> {
    CompiledWindowProcessorFactory::try_new(program, node, error)
}

#[derive(Debug)]
struct WindowFrameFailure {
    node: usize,
    call: u32,
    source: super::compiled_window_geometry::WindowGeometryError,
}
impl std::fmt::Display for WindowFrameFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "compiled Analytic call {} at local node {}: {}",
            self.call, self.node, self.source
        )
    }
}
impl std::error::Error for WindowFrameFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl novarocks_failure_contract::FailureProjection for WindowFrameFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(&self.source)
    }
}
fn admit_compiled_frame(node: usize, call: u32, frame: &WindowFrame) -> ExecutionResult<()> {
    admit_frame(frame)
        .map_err(|source| ExecutionFailure::from_domain(WindowFrameFailure { node, call, source }))
}
#[derive(Debug)]
enum WindowOutputViolation {
    RowErrors { count: usize },
    Length { expected: usize, actual: usize },
}
#[derive(Debug)]
struct WindowOutputFailure {
    call: usize,
    partition: usize,
    violation: WindowOutputViolation,
}
impl std::fmt::Display for WindowOutputFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.violation {
            WindowOutputViolation::RowErrors { count } => write!(
                f,
                "compiled window kernel returned {count} row errors on a channel without row masking (call={} partition={})",
                self.call, self.partition
            ),
            WindowOutputViolation::Length { expected, actual } => write!(
                f,
                "compiled window kernel output length differs from its partition rows (call={} partition={} expected={} actual={})",
                self.call, self.partition, expected, actual
            ),
        }
    }
}
impl std::error::Error for WindowOutputFailure {}
impl novarocks_failure_contract::FailureProjection for WindowOutputFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            crate::runtime::task::ExecutionReason::WindowKernelOutputContract.reason(),
        )
    }
}
fn validate_unmasked_window_output(
    call: usize,
    partition: usize,
    expected: usize,
    actual: usize,
    row_errors: usize,
) -> ExecutionResult<()> {
    // WindowCallContract::try_window admits NotRowEvaluated exclusively.
    // A journal here violates that already checked kernel declaration.
    let violation = if row_errors != 0 {
        WindowOutputViolation::RowErrors { count: row_errors }
    } else if expected != actual {
        WindowOutputViolation::Length { expected, actual }
    } else {
        return Ok(());
    };
    Err(ExecutionFailure::from_domain(WindowOutputFailure {
        call,
        partition,
        violation,
    }))
}
#[cfg(test)]
mod frame_source_properties {
    use super::*;
    use novarocks_failure_contract::FailureProjection;
    use std::error::Error;
    #[test]
    fn actual_frame_admission_refusal_preserves_geometry_source_and_local_site() {
        let frame = WindowFrame {
            start: Some(novarocks_local_program::WindowBoundary::Preceding(-1)),
            end: None,
            window_type: novarocks_local_program::WindowType::Rows,
        };
        let original = admit_frame(&frame).unwrap_err();
        let error = admit_compiled_frame(7, 3, &frame).unwrap_err();
        assert_eq!(error.failure_fact(), original.failure_fact());
        let source = error
            .source()
            .unwrap()
            .source()
            .unwrap()
            .downcast_ref::<WindowFrameFailure>()
            .unwrap();
        assert_eq!((source.node, source.call), (7, 3));
        assert_eq!(source.source, original);
        assert!(
            source
                .source()
                .unwrap()
                .is::<super::super::compiled_window_geometry::WindowGeometryError>()
        );
    }

    #[test]
    fn window_output_failure_reports_call_partition_and_observed_shape() {
        let row_errors = validate_unmasked_window_output(7, 11, 3, 3, 2).unwrap_err();
        assert!(
            row_errors
                .detail()
                .contains("2 row errors on a channel without row masking (call=7 partition=11)")
        );

        let length = validate_unmasked_window_output(7, 11, 3, 4, 0).unwrap_err();
        assert!(
            length
                .detail()
                .contains("call=7 partition=11 expected=3 actual=4")
        );
    }
}
