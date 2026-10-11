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

//! Processors that evaluate compiled LocalProgram expression roots.
//!
//! Each driver owns one `CompiledExpressionInstance` per actual root. The
//! input batch is the child's frozen output port; the output batch is exactly
//! the node's frozen output layout. Row data errors of a required root and
//! kernel failures stay typed. There is no legacy ExprArena or name dispatch.
//!
//! The kernel control observes the fragment's runtime error state. A unified
//! absolute evaluation deadline and memory admission are not yet loaned to
//! this host; they remain open host obligations, not implied by this control.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, ArrayRef, BooleanArray};
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use novarocks_functions::{KernelDiagnostic, KernelEvaluationControl, KernelFailure, Selection};
use novarocks_local_program::{
    LocalProgram, ProgramChannelLayoutRole, ProgramExpressionRootSite, ProgramNodeExpressionRole,
    ProgramNodeId, ProgramNodeKind, ProgramRootInput, root_input_layout,
};

use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
use crate::exec::expr::compiled_program::CompiledExpressionInstance;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult, RequiredExpressionRowError};
use crate::runtime::preparation_metadata::{CompiledSchemaMetadataScope, ProjectSchemaSite};
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};

pub(super) use crate::runtime::kernel_memory as runtime_kernel_memory;
#[cfg(test)]
#[path = "runtime_kernel_memory_tests.rs"]
mod runtime_kernel_memory_tests;

/// Kernel control backed by the fragment's runtime error state: a recorded
/// failure or cancellation refuses the next checkpoint, and waits are
/// interruptible by the same state.
pub(crate) struct RuntimeKernelControl {
    error: Arc<RuntimeErrorState>,
    query_memory: Option<crate::runtime::query_memory::QueryMemoryBinding>,
    allocator: Option<Arc<dyn novarocks_functions::AggregateStateAllocator>>,
}
impl RuntimeKernelControl {
    pub(crate) fn new(error: Arc<RuntimeErrorState>) -> Self {
        Self {
            error,
            query_memory: None,
            allocator: None,
        }
    }
    /// Borrow the already-validated task capability at original operator binding.
    /// Arc/account clones establish no funding domain or allocation authorization.
    pub(crate) fn bind_runtime_memory(&mut self, state: &RuntimeState) {
        self.query_memory = state.query_memory().cloned();
    }
    pub(crate) fn query_memory(&self) -> Option<&crate::runtime::query_memory::QueryMemoryBinding> {
        self.query_memory.as_ref()
    }
    pub(super) fn request_kernel_memory(
        &self,
        request: runtime_kernel_memory::KernelMemoryRequest,
    ) -> runtime_kernel_memory::KernelMemoryAdmission {
        runtime_kernel_memory::request(self.query_memory(), request)
    }
    pub(crate) fn bind_mem_tracker(
        &mut self,
        tracker: Arc<crate::runtime::mem_tracker::MemTracker>,
    ) {
        self.allocator = Some(super::compiled_aggregate::expression_allocation_host(
            tracker,
        ));
    }
    pub(crate) fn allocator(
        &self,
    ) -> Option<Arc<dyn novarocks_functions::AggregateStateAllocator>> {
        self.allocator.clone()
    }
}
impl KernelEvaluationControl for RuntimeKernelControl {
    fn checkpoint(&self, _work_units: u32) -> Result<(), KernelFailure> {
        if self.error.error().is_some() {
            return Err(KernelFailure::Cancelled);
        }
        Ok(())
    }
    fn wait(&self, duration: Duration) -> Result<(), KernelFailure> {
        self.error
            .wait_interruptibly(duration)
            .map_err(|_| KernelFailure::Cancelled)
    }
}

fn root(node: ProgramNodeId, role: ProgramNodeExpressionRole) -> ProgramExpressionRootSite {
    ProgramExpressionRootSite::Node { node, role }
}

/// Evaluate one root over every row of `input` and return the full column.
/// A row data error of the root is a required error and is returned typed.
pub(crate) fn evaluate_all(
    instance: &mut CompiledExpressionInstance,
    site: ProgramExpressionRootSite,
    input: &RecordBatch,
    control: &dyn KernelEvaluationControl,
) -> ExecutionResult<ArrayRef> {
    evaluate_selection(
        instance,
        site,
        input,
        Selection::all(input.num_rows()),
        if input.num_rows() == 0 {
            novarocks_functions::ScalarInvocationActivation::ValidateOnly
        } else {
            novarocks_functions::ScalarInvocationActivation::Activated
        },
        control,
    )
}

/// Evaluate one root over exactly the strictly increasing batch rows `rows`
/// and return one value per selected row, in selection order. Unselected rows
/// are never evaluated, so they raise no row error; a selected row's error is
/// a required error reported at its batch row.
pub(crate) fn evaluate_selected(
    instance: &mut CompiledExpressionInstance,
    site: ProgramExpressionRootSite,
    input: &RecordBatch,
    rows: &[usize],
    control: &dyn KernelEvaluationControl,
) -> ExecutionResult<ArrayRef> {
    let selection = Selection::try_sparse(input.num_rows(), rows).map_err(|_| {
        ExecutionFailure::from(KernelFailure::Internal(KernelDiagnostic::new(
            "compiled root selection is not ordered within its batch",
        )))
    })?;
    let activation = if rows.is_empty() {
        novarocks_functions::ScalarInvocationActivation::ValidateOnly
    } else {
        novarocks_functions::ScalarInvocationActivation::Activated
    };
    evaluate_selection(instance, site, input, selection, activation, control)
}

fn evaluate_selection(
    instance: &mut CompiledExpressionInstance,
    site: ProgramExpressionRootSite,
    input: &RecordBatch,
    selection: Selection<'_>,
    activation: novarocks_functions::ScalarInvocationActivation,
    control: &dyn KernelEvaluationControl,
) -> ExecutionResult<ArrayRef> {
    // Required callers supply their actual row-demand policy. Direct Frame
    // callers may activate an empty invocation (e.g. the original IF else).
    let result = instance.evaluate_evaluation(input, selection, activation, control)?;
    let (selection, values, errors) = result.into_parts();
    if let Some(error) = errors.into_vec().into_iter().next() {
        return Err(RequiredExpressionRowError::try_new(site, selection, error)?.into());
    }
    Ok(values)
}

/// Lazily create one instance per root on the driver's first batch, so a
/// refused preparation is reported on the actual operator operation.
pub(crate) fn instances(
    slot: &mut Option<Vec<CompiledExpressionInstance>>,
    program: &Arc<LocalProgram>,
    sites: &[ProgramExpressionRootSite],
    control: &RuntimeKernelControl,
) -> ExecutionResult<()> {
    if slot.is_some() {
        return Ok(());
    }
    let mut created = Vec::with_capacity(sites.len());
    for site in sites {
        created.push(CompiledExpressionInstance::try_new_with_allocator(
            Arc::clone(program),
            *site,
            control,
            control.allocator(),
        )?);
    }
    *slot = Some(created);
    Ok(())
}

/// The static value type the program's checked expressions give one root:
/// the exact type of every array an instance of that root returns.
pub(crate) fn root_value_type(
    program: &LocalProgram,
    site: ProgramExpressionRootSite,
) -> Result<novarocks_type_contract::FunctionValueType, String> {
    let expressions = program.checked().channels().expressions();
    let root = expressions
        .resolved_calls()
        .snapshot()
        .roots()
        .sites()
        .get(&site)
        .ok_or_else(|| format!("compiled program has no expression root {site:?}"))?;
    match expressions.definition_type(site.arena(), root.definition) {
        Some(novarocks_type_contract::FunctionArgumentType::Value(value)) => Ok(value.clone()),
        Some(_) => Err(format!("compiled expression root {site:?} is not a value")),
        None => Err(format!(
            "compiled expression root {site:?} has no static type"
        )),
    }
}

/// Project one compiled node: each output column is one ProjectOutput root.
pub struct CompiledProjectProcessorFactory {
    name: String,
    program: Arc<LocalProgram>,
    sites: Vec<ProgramExpressionRootSite>,
    final_identity_slots: Option<Vec<(novarocks_types::SlotId, novarocks_types::SlotId)>>,
    output: ChunkSchemaRef,
    error: Arc<RuntimeErrorState>,
}
impl CompiledProjectProcessorFactory {
    /// The actual bounded root boundary follows the original computing node.
    /// It borrows that node's published slots and does not author expressions,
    /// uses, bindings, or a second computation of any Project output.
    pub(crate) fn try_new_final_result_boundary(
        program: Arc<LocalProgram>,
        error: Arc<RuntimeErrorState>,
    ) -> Result<Self, String> {
        Self::try_new_final_result_boundary_in(program, error, |_, _, layout| {
            ChunkSchema::from_compiled_layout(layout)
        })
    }

    pub(crate) fn try_new_final_result_boundary_with_metadata_host<
        H: CompiledSchemaMetadataScope,
    >(
        program: Arc<LocalProgram>,
        error: Arc<RuntimeErrorState>,
        host: &mut H,
    ) -> ExecutionResult<Self> {
        Self::try_new_final_result_boundary_in(program, error, |program, site, layout| {
            host.materialize(program, site, layout, || {
                ChunkSchema::from_compiled_layout(layout)
            })
        })
    }

    fn try_new_final_result_boundary_in<E, F>(
        program: Arc<LocalProgram>,
        error: Arc<RuntimeErrorState>,
        mut materialize: F,
    ) -> Result<Self, E>
    where
        E: From<String> + From<&'static str>,
        F: FnMut(
            &Arc<LocalProgram>,
            ProjectSchemaSite,
            &novarocks_local_program::StaticLayout,
        ) -> Result<ChunkSchemaRef, E>,
    {
        if !matches!(
            program.graph().sink(),
            Some(novarocks_local_program::StaticSinkProgram::RootResult(_))
        ) {
            return Err("final result boundary requires its actual RootResult sink".into());
        }
        let root = program.graph().root();
        let node = program
            .graph()
            .nodes()
            .get(root.index())
            .ok_or("compiled final result root is absent")?;
        let output = materialize(
            &program,
            ProjectSchemaSite::FinalResult,
            node.output_layout(),
        )?;
        let pairs = output
            .slot_ids()
            .iter()
            .map(|slot| (*slot, *slot))
            .collect();
        Ok(Self {
            name: format!("COMPILED_FINAL_RESULT (root={})", root.index()),
            program,
            sites: Vec::new(),
            final_identity_slots: Some(pairs),
            output,
            error,
        })
    }

    pub(crate) fn try_new(
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        error: Arc<RuntimeErrorState>,
    ) -> Result<Self, String> {
        Self::try_new_in(program, node, error, |_, _, layout| {
            ChunkSchema::from_compiled_layout(layout)
        })
    }

    pub(crate) fn try_new_with_metadata_host<H: CompiledSchemaMetadataScope>(
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        error: Arc<RuntimeErrorState>,
        host: &mut H,
    ) -> ExecutionResult<Self> {
        Self::try_new_in(program, node, error, |program, site, layout| {
            host.materialize(program, site, layout, || {
                ChunkSchema::from_compiled_layout(layout)
            })
        })
    }

    fn try_new_in<E, F>(
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        error: Arc<RuntimeErrorState>,
        mut materialize: F,
    ) -> Result<Self, E>
    where
        E: From<String> + From<&'static str>,
        F: FnMut(
            &Arc<LocalProgram>,
            ProjectSchemaSite,
            &novarocks_local_program::StaticLayout,
        ) -> Result<ChunkSchemaRef, E>,
    {
        let graph_node = program
            .graph()
            .nodes()
            .get(node.index())
            .ok_or("compiled Project node is absent")?;
        let ProgramNodeKind::Project {
            exprs,
            expr_slot_ids,
            validate_final_result_input,
            output_indices,
            ..
        } = graph_node.kind()
        else {
            return Err("compiled node is not a Project".to_string().into());
        };
        let mut sites = Vec::with_capacity(exprs.len());
        for ordinal in 0..exprs.len() {
            let expression = u32::try_from(ordinal).map_err(|_| "Project width exceeds u32")?;
            sites.push(root(
                node,
                ProgramNodeExpressionRole::ProjectOutput { expression },
            ));
        }
        let final_identity_slots = if *validate_final_result_input {
            if output_indices.is_some() || exprs.len() != expr_slot_ids.len() {
                return Err(
                    "final result input validation requires an identity output layout".into(),
                );
            }
            Some(
                exprs
                    .iter()
                    .zip(expr_slot_ids)
                    .map(|(expr, output)| {
                        match program
                            .graph()
                            .expressions()
                            .node(*expr)
                            .map(|node| node.kind())
                        {
                            Some(novarocks_local_program::StaticExprKind::SlotId(source)) => {
                                Ok((*source, *output))
                            }
                            _ => Err(
                                "final result input validation requires identity expressions"
                                    .to_string(),
                            ),
                        }
                    })
                    .collect::<Result<Vec<_>, String>>()?,
            )
        } else {
            None
        };
        let output = materialize(
            &program,
            ProjectSchemaSite::Project(node),
            graph_node.output_layout(),
        )?;
        Ok(Self {
            name: format!("COMPILED_PROJECT (node={})", node.index()),
            program,
            sites,
            final_identity_slots,
            output,
            error,
        })
    }
}
impl OperatorFactory for CompiledProjectProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }
    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(CompiledProjectProcessor {
            name: self.name.clone(),
            program: Arc::clone(&self.program),
            sites: self.sites.clone(),
            final_identity_slots: self.final_identity_slots.clone(),
            output: Arc::clone(&self.output),
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            instances: None,
            pending: None,
            finishing: false,
            finished: false,
        })
    }
}
struct CompiledProjectProcessor {
    name: String,
    program: Arc<LocalProgram>,
    sites: Vec<ProgramExpressionRootSite>,
    final_identity_slots: Option<Vec<(novarocks_types::SlotId, novarocks_types::SlotId)>>,
    output: ChunkSchemaRef,
    control: RuntimeKernelControl,
    instances: Option<Vec<CompiledExpressionInstance>>,
    pending: Option<Chunk>,
    finishing: bool,
    finished: bool,
}
impl Operator for CompiledProjectProcessor {
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
    fn is_finished(&self) -> bool {
        self.finished
    }
    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }
    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }
}
impl ProcessorOperator for CompiledProjectProcessor {
    fn need_input(&self) -> bool {
        !self.finishing && !self.finished && self.pending.is_none()
    }
    fn has_output(&self) -> bool {
        self.pending.is_some()
    }
    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> ExecutionResult<()> {
        if self.pending.is_some() {
            return Err("compiled Project received input while output is pending".into());
        }
        if let Some(pairs) = &self.final_identity_slots {
            super::project_processor::validate_final_result_identity_input_source(
                &chunk,
                &self.output,
                true,
                pairs.len(),
                pairs.iter().copied().map(Ok),
            )?;
            // No column requires projection. Keep the actual row-count-bearing
            // source and its genuine schema owners after the same validation.
            if pairs.is_empty() {
                self.pending = Some(chunk);
                return Ok(());
            }
            let columns = if chunk.is_empty() {
                self.output
                    .slots()
                    .iter()
                    .map(|slot| arrow::array::new_empty_array(slot.data_type()))
                    .collect()
            } else {
                pairs
                    .iter()
                    .map(|(source, _)| {
                        let ordinal = chunk
                            .slot_id_to_index()
                            .get(source)
                            .copied()
                            .ok_or("final result input source slot is missing")?;
                        chunk
                            .columns()
                            .get(ordinal)
                            .map(Arc::clone)
                            .ok_or_else(|| "final result input array is missing".to_string())
                    })
                    .collect::<Result<Vec<_>, String>>()?
            };
            self.pending = Some(super::project_processor::materialize_project_output(
                columns,
                &self.output,
                true,
            )?);
            return Ok(());
        }
        instances(
            &mut self.instances,
            &self.program,
            &self.sites,
            &self.control,
        )?;
        let instances = self.instances.as_mut().expect("instances were created");
        let mut columns = Vec::with_capacity(self.sites.len());
        for (instance, site) in instances.iter_mut().zip(&self.sites) {
            columns.push(evaluate_all(instance, *site, &chunk.batch, &self.control)?);
        }
        // A Project may publish no column (e.g. under COUNT(*)); its row count
        // is still its input's, which a zero-column batch cannot infer.
        let batch = RecordBatch::try_new_with_options(
            self.output.arrow_schema_ref(),
            columns,
            &arrow::record_batch::RecordBatchOptions::new()
                .with_row_count(Some(chunk.batch.num_rows())),
        )
        .map_err(|error| ExecutionFailure::from(format!("compiled Project output: {error}")))?;
        self.pending = Some(Chunk::try_new_with_chunk_schema(
            batch,
            Arc::clone(&self.output),
        )?);
        Ok(())
    }
    fn pull_chunk(&mut self, _state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
        let output = self.pending.take();
        if self.finishing {
            self.finished = true;
        }
        Ok(output)
    }
    fn set_finishing(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
        self.finishing = true;
        if self.pending.is_none() {
            self.finished = true;
        }
        Ok(())
    }
}

/// Filter by the original ordered Filter or Scan TruthOnly predicate roots.
/// Both preserve the exact
/// input port and keep only rows whose complete predicate is TRUE.
pub struct CompiledFilterProcessorFactory {
    name: String,
    program: Arc<LocalProgram>,
    site: ProgramExpressionRootSite,
    error: Arc<RuntimeErrorState>,
}
impl CompiledFilterProcessorFactory {
    /// `site` identifies the Filter owner through original predicate zero,
    /// or original ScanResidual zero. All roots share the same
    /// checked NodeOutput input port, which is also the filtered output.
    pub(crate) fn try_new(
        program: Arc<LocalProgram>,
        site: ProgramExpressionRootSite,
        error: Arc<RuntimeErrorState>,
    ) -> Result<Self, String> {
        let ProgramExpressionRootSite::Node { node, role } = site else {
            return Err(format!(
                "compiled filter root {site:?} is not a node predicate"
            ));
        };
        let graph_node = program
            .graph()
            .nodes()
            .get(node.index())
            .ok_or_else(|| format!("compiled filter node {} is absent", node.index()))?;
        let label = match (graph_node.kind(), role) {
            (
                ProgramNodeKind::Filter { .. },
                ProgramNodeExpressionRole::FilterPredicate { predicate: 0 },
            ) => "COMPILED_FILTER",
            (
                ProgramNodeKind::Scan { .. },
                ProgramNodeExpressionRole::ScanResidual { predicate: 0 },
            ) => "COMPILED_SCAN_RESIDUAL",
            _ => {
                return Err(format!(
                    "compiled filter root {role:?} at local node {} is neither a Filter predicate nor a Scan residual",
                    node.index()
                ));
            }
        };
        match root_input_layout(program.graph(), site) {
            Ok(ProgramRootInput::Layout {
                role: ProgramChannelLayoutRole::NodeOutput,
                ..
            }) => {}
            _ => {
                return Err(format!(
                    "compiled filter root {role:?} at local node {} has no single node-output input port",
                    node.index()
                ));
            }
        }
        Ok(Self {
            name: format!("{label} (node={})", node.index()),
            program,
            site,
            error,
        })
    }
}
impl OperatorFactory for CompiledFilterProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }
    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(CompiledFilterProcessor {
            name: self.name.clone(),
            program: Arc::clone(&self.program),
            site: self.site,
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            instance: None,
            pending: None,
            finishing: false,
            finished: false,
        })
    }
}
struct CompiledFilterProcessor {
    name: String,
    program: Arc<LocalProgram>,
    site: ProgramExpressionRootSite,
    control: RuntimeKernelControl,
    instance: Option<crate::exec::expr::compiled_program::CompiledFilterConjunctionInstance>,
    pending: Option<Chunk>,
    finishing: bool,
    finished: bool,
}
impl Operator for CompiledFilterProcessor {
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
    fn is_finished(&self) -> bool {
        self.finished
    }
    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }
    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }
}
impl ProcessorOperator for CompiledFilterProcessor {
    fn need_input(&self) -> bool {
        !self.finishing && !self.finished && self.pending.is_none()
    }
    fn has_output(&self) -> bool {
        self.pending.is_some()
    }
    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> ExecutionResult<()> {
        if self.pending.is_some() {
            return Err("compiled Filter received input while output is pending".into());
        }
        if self.instance.is_none() {
            let ProgramExpressionRootSite::Node { node, .. } = self.site else {
                return Err(KernelFailure::InvalidProgram(KernelDiagnostic::new(
                    "compiled Filter root is not a node",
                ))
                .into());
            };
            self.instance = Some(crate::exec::expr::compiled_program::CompiledFilterConjunctionInstance::try_new_with_allocator(
                Arc::clone(&self.program), node, &self.control, self.control.allocator(),
            )?);
        }
        let truth = self
            .instance
            .as_mut()
            .expect("instance was created")
            .evaluate_required(
                &chunk.batch,
                Selection::all(chunk.batch.num_rows()),
                &self.control,
            )?;
        let truth = truth
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                ExecutionFailure::from(KernelFailure::InvalidProgram(KernelDiagnostic::new(
                    "compiled Filter predicate is not Boolean",
                )))
            })?;
        // TruthOnly: NULL is not TRUE. Clear nulls before using the mask.
        let mask = if truth.null_count() == 0 {
            truth.clone()
        } else {
            BooleanArray::from_iter(truth.iter().map(|v| Some(v == Some(true))))
        };
        let batch = filter_record_batch(&chunk.batch, &mask)
            .map_err(|error| ExecutionFailure::from(format!("compiled Filter: {error}")))?;
        self.pending = Some(Chunk::try_new_with_chunk_schema(
            batch,
            chunk.chunk_schema_ref(),
        )?);
        Ok(())
    }
    fn pull_chunk(&mut self, _state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
        let output = self.pending.take();
        if self.finishing {
            self.finished = true;
        }
        Ok(output)
    }
    fn set_finishing(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
        self.finishing = true;
        if self.pending.is_none() {
            self.finished = true;
        }
        Ok(())
    }
}

#[cfg(feature = "test-support")]
pub fn prepare_project_factory_for_test<H: CompiledSchemaMetadataScope>(
    program: Arc<LocalProgram>,
    site: ProjectSchemaSite,
    error: Arc<RuntimeErrorState>,
    host: &mut H,
) -> ExecutionResult<CompiledProjectProcessorFactory> {
    match site {
        ProjectSchemaSite::Project(node) => {
            CompiledProjectProcessorFactory::try_new_with_metadata_host(program, node, error, host)
        }
        ProjectSchemaSite::FinalResult => {
            CompiledProjectProcessorFactory::try_new_final_result_boundary_with_metadata_host(
                program, error, host,
            )
        }
    }
}
