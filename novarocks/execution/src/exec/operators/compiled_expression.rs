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
use crate::exec::expr::compiled_program::{
    CompiledExpressionInstance, input_source::CompiledInputLoan,
};
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult, RequiredExpressionRowError};
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};

pub(super) use crate::runtime::kernel_memory as runtime_kernel_memory;
#[cfg(test)]
#[path = "runtime_kernel_memory_tests.rs"]
mod runtime_kernel_memory_tests;
#[cfg(novarocks_dev_controls)]
#[path = "runtime_scalar_memory_test_support.rs"]
pub(crate) mod runtime_scalar_memory_test_support;

/// Kernel control backed by the fragment's runtime error state: a recorded
/// failure or cancellation refuses the next checkpoint, and waits are
/// interruptible by the same state.
#[derive(Clone)]
pub(crate) struct RuntimeKernelControl {
    error: Arc<RuntimeErrorState>,
    query_memory: Option<crate::runtime::query_memory::QueryMemoryBinding>,
    allocator: Option<Arc<super::compiled_aggregate::TrackedStateAllocator>>,
    construction: Option<Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>>,
}
impl RuntimeKernelControl {
    pub(crate) fn new(error: Arc<RuntimeErrorState>) -> Self {
        Self {
            error,
            query_memory: None,
            allocator: None,
            construction: None,
        }
    }
    /// Borrow the already-validated task capability at original operator binding.
    /// Arc/account clones establish no funding domain or allocation authorization.
    pub(crate) fn bind_runtime_memory(&mut self, state: &RuntimeState) {
        self.query_memory = state.query_memory().cloned();
    }
    pub(crate) fn bind_construction_factory(
        &mut self,
        factory: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
    ) {
        self.construction = Some(factory);
    }
    pub(crate) fn construction_owner(
        &self,
        schema: &ChunkSchema,
    ) -> ExecutionResult<Box<dyn crate::exec::pipeline::operator::InputConstructionOwner>> {
        use crate::exec::pipeline::operator::ConstructionFailure;
        let factory = self
            .construction
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        Ok(factory.begin_typed_construction(schema)?)
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
        self.allocator.as_ref().map(|host| {
            let actual: Arc<dyn novarocks_functions::AggregateStateAllocator> = host.clone();
            actual
        })
    }
    /// Borrow the SAME concrete host whose original dyn Arc created this
    /// instance. Equality checks existing owners; it grants no new authority.
    pub(crate) fn invocation_host(
        &self,
        original: Option<&Arc<dyn novarocks_functions::AggregateStateAllocator>>,
    ) -> Result<
        &super::compiled_aggregate::TrackedStateAllocator,
        novarocks_functions::ScalarResourceError,
    > {
        let Some(host) = &self.allocator else {
            return Err(novarocks_functions::ScalarResourceError::Invariant(
                "whole-invocation runtime has no concrete allocation host",
            ));
        };
        let actual: Arc<dyn novarocks_functions::AggregateStateAllocator> = host.clone();
        if !original.is_some_and(|original| Arc::ptr_eq(original, &actual)) {
            return Err(novarocks_functions::ScalarResourceError::Invariant(
                "whole-invocation instance allocation host differs from its runtime owner",
            ));
        }
        Ok(host)
    }
}
impl crate::exec::expr::ExprEvaluationControl for RuntimeKernelControl {
    fn checkpoint(&self) -> ExecutionResult<()> {
        match self.error.error() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
    fn wait(&self, duration: Duration) -> ExecutionResult<()> {
        self.error.wait_interruptibly(duration)
    }
}
impl KernelEvaluationControl for RuntimeKernelControl {
    fn checkpoint(&self, _work_units: u32) -> Result<(), KernelFailure> {
        if self.error.is_stopped() {
            return Err(KernelFailure::Cancelled);
        }
        Ok(())
    }
    fn wait(&self, duration: Duration) -> Result<(), KernelFailure> {
        if self.error.wait_until_stopped(duration) {
            Err(KernelFailure::Cancelled)
        } else {
            Ok(())
        }
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
    control: &RuntimeKernelControl,
) -> ExecutionResult<ArrayRef> {
    evaluate_selection(
        instance,
        site,
        CompiledInputLoan::RecordBatch(input),
        Selection::all(input.num_rows()),
        if input.num_rows() == 0 {
            novarocks_functions::ScalarInvocationActivation::ValidateOnly
        } else {
            novarocks_functions::ScalarInvocationActivation::Activated
        },
        control,
    )
}

/// The actual Chunk callers borrow its original attached authority. No
/// authority is retained after this synchronous evaluation returns.
pub(crate) fn evaluate_chunk_all(
    instance: &mut CompiledExpressionInstance,
    site: ProgramExpressionRootSite,
    input: &Chunk,
    control: &RuntimeKernelControl,
) -> ExecutionResult<ArrayRef> {
    evaluate_selection(
        instance,
        site,
        CompiledInputLoan::from_chunk(input),
        Selection::all(input.record_batch().num_rows()),
        if input.record_batch().num_rows() == 0 {
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
    control: &RuntimeKernelControl,
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
    evaluate_selection(
        instance,
        site,
        CompiledInputLoan::RecordBatch(input),
        selection,
        activation,
        control,
    )
}

fn evaluate_selection(
    instance: &mut CompiledExpressionInstance,
    site: ProgramExpressionRootSite,
    input: CompiledInputLoan<'_>,
    selection: Selection<'_>,
    activation: novarocks_functions::ScalarInvocationActivation,
    control: &RuntimeKernelControl,
) -> ExecutionResult<ArrayRef> {
    // Required callers supply their actual row-demand policy. Direct Frame
    // callers may activate an empty invocation (e.g. the original IF else).
    let mut journal = crate::runtime::scalar_memory::RuntimeScalarMemoryJournal::default();
    let mut scope =
        crate::runtime::scalar_memory::RuntimeScalarMemoryScope::new(control, &mut journal);
    let result =
        instance.evaluate_input_runtime(input, selection, activation, control, &mut scope)?;
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

fn borrowed_project_root_value_type(
    program: &LocalProgram,
    site: ProgramExpressionRootSite,
) -> Result<
    &novarocks_type_contract::FunctionValueType,
    crate::exec::pipeline::operator::ConstructionFailure,
> {
    use crate::exec::pipeline::operator::ConstructionFailure;
    let expressions = program.checked().channels().expressions();
    let root = expressions
        .resolved_calls()
        .snapshot()
        .roots()
        .sites()
        .get(&site)
        .ok_or_else(|| {
            ConstructionFailure::InvalidSource("compiled project root is absent".into())
        })?;
    match expressions.definition_type(site.arena(), root.definition) {
        Some(novarocks_type_contract::FunctionArgumentType::Value(value)) => Ok(value),
        _ => Err(ConstructionFailure::InvalidSource(
            "compiled project root lacks a complete value declaration".into(),
        )),
    }
}

/// Project one compiled node: each output column is one ProjectOutput root.
pub struct CompiledProjectProcessorFactory {
    name: String,
    program: Arc<LocalProgram>,
    sites: Vec<ProgramExpressionRootSite>,
    final_identity_slots: Option<Vec<(novarocks_types::SlotId, novarocks_types::SlotId)>>,
    layout_node: ProgramNodeId,
    preparation_site: crate::runtime::preparation_metadata::CompiledSchemaSite,
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
        if !node.output_layout().is_typed() {
            return Err("compiled final result requires a complete output layout".into());
        }
        let pairs = node
            .output_layout()
            .slots()
            .iter()
            .map(|slot| (*slot, *slot))
            .collect();
        Ok(Self {
            name: format!("COMPILED_FINAL_RESULT (root={})", root.index()),
            program,
            sites: Vec::new(),
            final_identity_slots: Some(pairs),
            layout_node: root,
            preparation_site: crate::runtime::preparation_metadata::CompiledSchemaSite::FinalResult,
            error,
        })
    }

    pub(crate) fn try_new(
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        error: Arc<RuntimeErrorState>,
    ) -> Result<Self, String> {
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
            return Err("compiled node is not a Project".to_string());
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
        if !graph_node.output_layout().is_typed() {
            return Err("compiled Project requires its complete output layout".into());
        }
        Ok(Self {
            name: format!("COMPILED_PROJECT (node={})", node.index()),
            program,
            sites,
            final_identity_slots,
            layout_node: node,
            preparation_site: crate::runtime::preparation_metadata::CompiledSchemaSite::Project(
                node,
            ),
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
            layout_node: self.layout_node,
            preparation_site: self.preparation_site,
            output: None,
            construction: None,
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
    layout_node: ProgramNodeId,
    preparation_site: crate::runtime::preparation_metadata::CompiledSchemaSite,
    output: Option<ChunkSchemaRef>,
    construction: Option<Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>>,
    control: RuntimeKernelControl,
    instances: Option<Vec<CompiledExpressionInstance>>,
    pending: Option<Chunk>,
    finishing: bool,
    finished: bool,
}
#[cfg(test)]
pub(crate) fn project_chunk_with_source_observer(
    program: Arc<LocalProgram>,
    chunk: Chunk,
    observer: Arc<crate::exec::expr::compiled_program::input_source::SlotSourceObserver>,
) -> ExecutionResult<Chunk> {
    let factory = CompiledProjectProcessorFactory::try_new(
        program,
        ProgramNodeId::new(3),
        Arc::new(RuntimeErrorState::default()),
    )?;
    let mut processor = CompiledProjectProcessor {
        name: factory.name,
        program: factory.program,
        sites: factory.sites,
        final_identity_slots: factory.final_identity_slots,
        layout_node: factory.layout_node,
        preparation_site: factory.preparation_site,
        output: None,
        construction: None,
        control: RuntimeKernelControl::new(factory.error),
        instances: None,
        pending: None,
        finishing: false,
        finished: false,
    };
    let tracker =
        crate::runtime::mem_tracker::MemTracker::new_root("slot source fixture construction");
    tracker.install_limit_once(64 * 1024 * 1024).unwrap();
    processor.control.bind_mem_tracker(tracker.clone());
    processor.set_construction_factory(crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        tracker, Arc::new(RuntimeErrorState::default()),
    )?);
    processor.prepare()?;
    assert!(processor.final_identity_slots.is_none());
    instances(
        &mut processor.instances,
        &processor.program,
        &processor.sites,
        &processor.control,
    )?;
    for instance in processor.instances.as_mut().unwrap() {
        instance.observe_slot_source(Arc::clone(&observer));
    }
    let state = RuntimeState::default();
    processor.push_chunk(&state, chunk)?;
    processor
        .pull_chunk(&state)?
        .ok_or_else(|| "source fixture Project has no output".into())
}
impl Operator for CompiledProjectProcessor {
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
        Some((&self.program, self.preparation_site))
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
        use crate::exec::pipeline::operator::ConstructionFailure;
        let layout = self.program.graph().nodes()[self.layout_node.index()].output_layout();
        let factory = self
            .construction
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let owner = factory.begin_compiled_layout_construction(layout)?;
        self.output = Some(ChunkSchema::from_compiled_layout_owned_observed::<
            ConstructionFailure,
        >(
            layout,
            owner.storage_lifetime(),
            |layout| owner.request(layout),
            |units| {
                owner.observe(
                    0,
                    &[],
                    crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units },
                )
            },
        )?);
        Ok(())
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
    fn accepts_encoded_path(
        &self,
        occurrence: crate::exec::pipeline::operator::EncodedValueOccurrence<'_>,
    ) -> bool {
        let Some(pairs) = &self.final_identity_slots else {
            return false;
        };
        let installed = matches!(occurrence.carrier, arrow::datatypes::DataType::Dictionary(key, value)
            if key.as_ref() == &arrow::datatypes::DataType::Int32
                && matches!(value.as_ref(), arrow::datatypes::DataType::Utf8 | arrow::datatypes::DataType::LargeUtf8));
        installed && pairs.iter().any(|(source, _)| *source == occurrence.slot)
    }
    fn need_input(&self) -> bool {
        !self.finishing && !self.finished && self.pending.is_none()
    }
    fn has_output(&self) -> bool {
        self.pending.is_some()
    }
    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> ExecutionResult<()> {
        use super::project_processor::{
            ProjectEncodingSource, materialize_project_output_owned, project_reserve,
        };
        use crate::exec::pipeline::operator::ConstructionFailure;
        if self.pending.is_some() {
            return Err("compiled Project received input while output is pending".into());
        }
        let output = self
            .output
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?
            .clone();
        let owner = self.control.construction_owner(&output)?;
        let storage = if output.slots().is_empty() {
            owner.storage_lifetime()
        } else {
            chunk.source_storage_lifetime_observed(owner.storage_lifetime(), |layout| {
                owner.request(layout)
            })?
        };
        if let Some(pairs) = &self.final_identity_slots {
            super::project_processor::validate_final_result_identity_input_source(
                &chunk,
                &output,
                true,
                pairs.len(),
                pairs.iter().copied().map(Ok),
            )?;
            let source_at = |ordinal: usize| -> Result<
                &crate::exec::chunk::ChunkSlotSchema,
                ConstructionFailure,
            > {
                chunk.chunk_schema().slot(pairs[ordinal].0).ok_or_else(|| {
                    ConstructionFailure::InvalidSource("final result source is missing".into())
                })
            };
            let mut columns = project_reserve(pairs.len(), owner.as_ref())?;
            for (source, _) in pairs {
                columns.push(chunk.column_by_slot_id(*source)?);
            }
            self.pending = Some(materialize_project_output_owned(
                columns,
                chunk.record_batch().num_rows(),
                output,
                |ordinal| Ok(source_at(ordinal)?.typed_binding()),
                |ordinal| {
                    Ok(ProjectEncodingSource::OriginalField(
                        source_at(ordinal)?.field(),
                    ))
                },
                storage,
                owner.as_ref(),
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
        let mut columns = project_reserve(self.sites.len(), owner.as_ref())?;
        for (instance, site) in instances.iter_mut().zip(&self.sites) {
            columns.push(evaluate_chunk_all(instance, *site, &chunk, &self.control)?);
        }
        self.pending = Some(materialize_project_output_owned(
            columns,
            chunk.record_batch().num_rows(),
            output,
            |ordinal| {
                Ok(
                    borrowed_project_root_value_type(&self.program, self.sites[ordinal])?
                        .typed_binding(),
                )
            },
            |_| Ok(ProjectEncodingSource::Plain),
            storage,
            owner.as_ref(),
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
#[derive(Clone)]
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
/// One synchronous predicate body. Pipeline buffering and source polling
/// stay in their respective shells; both consume the same conjunction state.
pub(crate) struct CompiledFilterBody {
    program: Arc<LocalProgram>,
    site: ProgramExpressionRootSite,
    instance: Option<crate::exec::expr::compiled_program::CompiledFilterConjunctionInstance>,
}
impl CompiledFilterBody {
    pub(crate) fn evaluate(
        &mut self,
        input: CompiledInputLoan<'_>,
        control: &RuntimeKernelControl,
    ) -> ExecutionResult<BooleanArray> {
        if self.instance.is_none() {
            let ProgramExpressionRootSite::Node { node, .. } = self.site else {
                return Err(KernelFailure::InvalidProgram(KernelDiagnostic::new(
                    "compiled Filter root is not a node",
                ))
                .into());
            };
            self.instance = Some(crate::exec::expr::compiled_program::CompiledFilterConjunctionInstance::try_new_with_allocator(
                Arc::clone(&self.program), node, control, control.allocator(),
            )?);
        }
        let mut journal = crate::runtime::scalar_memory::RuntimeScalarMemoryJournal::default();
        let mut scope =
            crate::runtime::scalar_memory::RuntimeScalarMemoryScope::new(control, &mut journal);
        let truth = self
            .instance
            .as_mut()
            .expect("instance was created")
            .evaluate_required_input_runtime(
                input,
                Selection::all(input.batch().num_rows()),
                control,
                &mut scope,
            )?;
        let truth = truth
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                ExecutionFailure::from(KernelFailure::InvalidProgram(KernelDiagnostic::new(
                    "compiled Filter predicate is not Boolean",
                )))
            })?;
        Ok(truth.clone())
    }
}
impl CompiledFilterProcessorFactory {
    pub(crate) fn create_body(&self) -> CompiledFilterBody {
        CompiledFilterBody {
            program: Arc::clone(&self.program),
            site: self.site,
            instance: None,
        }
    }
}
impl OperatorFactory for CompiledFilterProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }
    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(CompiledFilterProcessor {
            name: self.name.clone(),
            body: self.create_body(),
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            pending: None,
            finishing: false,
            finished: false,
        })
    }
}
struct CompiledFilterProcessor {
    name: String,
    body: CompiledFilterBody,
    control: RuntimeKernelControl,
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
        let truth = self.body.evaluate(
            CompiledInputLoan::RecordBatch(chunk.record_batch()),
            &self.control,
        )?;
        let mask = crate::exec::chunk::normalize_filter_mask_observed(&truth, || Ok(()))?;
        let batch = filter_record_batch(chunk.record_batch(), &mask)
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

/// Select the same runtime factory for component fixtures. Typed schema creation
/// still occurs only under the factory installed on the actual operator.
#[cfg(novarocks_dev_controls)]
pub fn prepare_project_factory_for_test(
    program: Arc<LocalProgram>,
    node: Option<ProgramNodeId>,
    error: Arc<RuntimeErrorState>,
) -> ExecutionResult<CompiledProjectProcessorFactory> {
    Ok(match node {
        Some(node) => CompiledProjectProcessorFactory::try_new(program, node, error)?,
        None => CompiledProjectProcessorFactory::try_new_final_result_boundary(program, error)?,
    })
}
