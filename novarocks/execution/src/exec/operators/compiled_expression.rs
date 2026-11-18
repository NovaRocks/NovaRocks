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

use crate::exec::fragment::error::{
    FragmentBindingErrorKind as PlanBindingKind, program_binding_failure,
};
use crate::exec::pipeline::operator::OperatorLifecycle;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, ArrayRef, BooleanArray};
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use novarocks_functions::{KernelDiagnostic, KernelEvaluationControl, KernelFailure, Selection};
use novarocks_local_program::{LocalProgram, ProgramNodeId, ProgramNodeKind, ProgramRootRef};

use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
use crate::exec::expr::compiled_program::{
    CompiledExpressionInstance, input_source::CompiledInputLoan,
};
use crate::exec::pipeline::operator::{InputEncodingPolicy, Operator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::{
    shape::{Transform, TransformKernel},
    step::{NextStep, StepResult},
};
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};
use crate::runtime::task::{ExecutionFailure, ExecutionResult};
use novarocks_expression::{InputColumnProjection, RequiredExpressionRowError};

#[cfg(test)]
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
    observed_stop: std::sync::OnceLock<ExecutionFailure>,
    expression_node: Option<ProgramNodeId>,
    query_memory: Option<crate::runtime::query_memory::QueryMemoryBinding>,
    allocator: Option<Arc<crate::runtime::tracked_state_allocator::TrackedStateAllocator>>,
    construction: Option<Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>>,
}
impl RuntimeKernelControl {
    pub(crate) fn new(error: Arc<RuntimeErrorState>) -> Self {
        Self {
            error,
            observed_stop: std::sync::OnceLock::new(),
            expression_node: None,
            query_memory: None,
            allocator: None,
            construction: None,
        }
    }
    /// The actual operator supplies diagnostic context outside the evaluator.
    /// This preserves the original failure occurrence and grants no resource.
    pub(crate) fn at_expression_node(mut self, node: ProgramNodeId) -> Self {
        self.expression_node = Some(node);
        self
    }
    /// Bind a nested receiver to the same fragment services. Its allocator is
    /// installed separately on that receiver; this creates no memory authority.
    pub(crate) fn for_receiver(&self) -> Self {
        Self {
            error: Arc::clone(&self.error),
            observed_stop: std::sync::OnceLock::new(),
            expression_node: self.expression_node,
            query_memory: self.query_memory.clone(),
            allocator: None,
            construction: self.construction.clone(),
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
    #[cfg(test)]
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
        self.allocator =
            Some(crate::runtime::tracked_state_allocator::expression_allocation_host(tracker));
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
        &crate::runtime::tracked_state_allocator::TrackedStateAllocator,
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
impl RuntimeKernelControl {
    // The hot kernel port carries only a stop signal. Retain the exact
    // originating fact only when that port actually observes its owner stop;
    // an unrelated kernel failure must not adopt a later fragment failure.
    fn observe_stop_cause(&self) {
        if self.observed_stop.get().is_none()
            && let Some(error) = self.error.error()
        {
            let _ = self.observed_stop.set(error);
        }
    }
    pub(crate) fn project_required_failure(&self, failure: ExecutionFailure) -> ExecutionFailure {
        if matches!(
            failure.cause(),
            crate::runtime::task::ExecutionFailureCause::Kernel(KernelFailure::Cancelled)
        ) && let Some(original) = self.observed_stop.get()
        {
            return original.clone();
        }
        match self.expression_node {
            Some(node) => failure.at_expression_node(node),
            None => failure,
        }
    }
    pub(crate) fn checkpoint_execution(&self) -> ExecutionResult<()> {
        match self.error.error() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

impl KernelEvaluationControl for RuntimeKernelControl {
    fn checkpoint(&self, _work_units: u32) -> Result<(), KernelFailure> {
        if self.error.is_stopped() {
            self.observe_stop_cause();
            return Err(KernelFailure::Cancelled);
        }
        Ok(())
    }
    fn wait(&self, duration: Duration) -> Result<(), KernelFailure> {
        if self.error.wait_until_stopped(duration) {
            self.observe_stop_cause();
            Err(KernelFailure::Cancelled)
        } else {
            Ok(())
        }
    }
}

/// Evaluate one root over every row of `input` and return the full column.
/// A row data error of the root is a required error and is returned typed.
pub(crate) fn evaluate_all(
    instance: &mut CompiledExpressionInstance,
    site: ProgramRootRef,
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
    site: ProgramRootRef,
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

/// The same root evaluator over an explicitly mapped, physically compact port.
/// The complete frozen declaration remains independent of its materialized
/// columns. No placeholder Array is constructed for an unreferenced channel.
pub(crate) fn evaluate_projected_chunk_all(
    instance: &mut CompiledExpressionInstance,
    site: ProgramRootRef,
    input: &Chunk,
    declared: &ChunkSchema,
    projection: &Arc<InputColumnProjection>,
    control: &RuntimeKernelControl,
) -> ExecutionResult<ArrayRef> {
    let loan = CompiledInputLoan::from_projected_chunk(input, declared, projection, control)?;
    evaluate_selection(
        instance,
        site,
        loan,
        Selection::all(input.len()),
        if input.is_empty() {
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
pub(crate) fn evaluate_projected_chunk_rows(
    instance: &mut CompiledExpressionInstance,
    site: ProgramRootRef,
    input: &Chunk,
    declared: &ChunkSchema,
    projection: &Arc<InputColumnProjection>,
    rows: &[usize],
    control: &RuntimeKernelControl,
) -> ExecutionResult<ArrayRef> {
    let selection = Selection::try_sparse(input.len(), rows).map_err(|_| {
        ExecutionFailure::from(KernelFailure::Internal(KernelDiagnostic::new(
            "compiled root selection is not ordered within its batch",
        )))
    })?;
    let loan = CompiledInputLoan::from_projected_chunk(input, declared, projection, control)?;
    evaluate_selection(
        instance,
        site,
        loan,
        selection,
        if rows.is_empty() {
            novarocks_functions::ScalarInvocationActivation::ValidateOnly
        } else {
            novarocks_functions::ScalarInvocationActivation::Activated
        },
        control,
    )
}

/// The same original sparse selection author for a complete RecordBatch port.
pub(crate) fn evaluate_selected(
    instance: &mut CompiledExpressionInstance,
    site: ProgramRootRef,
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
    site: ProgramRootRef,
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
    let result = instance
        .evaluate_input_runtime(input, selection, activation, control, &mut scope)
        .map_err(|cause| control.project_required_failure(cause))?;
    let (selection, values, errors) = result.into_parts();
    if let Some(error) = errors.into_vec().into_iter().next() {
        return Err(control.project_required_failure(
            RequiredExpressionRowError::try_new(site.root, selection, error)?.into(),
        ));
    }
    Ok(values)
}

/// Lazily create one instance per root on the driver's first batch, so a
/// refused preparation is reported on the actual operator operation.
pub(crate) fn instances(
    slot: &mut Option<Vec<CompiledExpressionInstance>>,
    program: &Arc<LocalProgram>,
    sites: &[ProgramRootRef],
    control: &RuntimeKernelControl,
) -> ExecutionResult<()> {
    if slot.is_some() {
        return Ok(());
    }
    let mut created = Vec::with_capacity(sites.len());
    for site in sites {
        created.push(
            CompiledExpressionInstance::try_new_with_allocator(
                Arc::clone(program.unit(site.unit).ok_or_else(|| {
                    KernelFailure::InvalidProgram(KernelDiagnostic::new(
                        "compiled expression unit is absent",
                    ))
                })?),
                site.root,
                control,
                control.allocator(),
            )
            .map_err(|cause| control.project_required_failure(cause.into()))?,
        );
    }
    *slot = Some(created);
    Ok(())
}

/// The static value type the program's checked expressions give one root:
/// the exact type of every array an instance of that root returns.
pub(crate) fn root_value_type_ref(
    program: &LocalProgram,
    site: ProgramRootRef,
) -> Option<&novarocks_type_contract::FunctionValueType> {
    let unit = program.unit(site.unit)?;
    let root = unit.root(site.root)?;
    match unit.types().get(root.definition.index())? {
        novarocks_type_contract::FunctionArgumentType::Value(value) => Some(value),
        _ => None,
    }
}

pub(crate) fn root_value_type(
    program: &LocalProgram,
    site: ProgramRootRef,
) -> Result<
    novarocks_type_contract::FunctionValueType,
    crate::exec::fragment::error::FragmentBindingError,
> {
    use crate::exec::fragment::error::{
        FragmentBindingError, FragmentBindingErrorKind, FragmentBindingTarget,
    };
    let unit = program.unit(site.unit).ok_or_else(|| {
        FragmentBindingError::new(
            FragmentBindingTarget::Program,
            FragmentBindingErrorKind::MissingAssignment,
            format!("compiled program has no expression unit {:?}", site.unit),
        )
    })?;
    let root = unit.root(site.root).ok_or_else(|| {
        FragmentBindingError::new(
            FragmentBindingTarget::Program,
            FragmentBindingErrorKind::MissingAssignment,
            format!("compiled program has no expression root {site:?}"),
        )
    })?;
    match unit.types().get(root.definition.index()) {
        Some(novarocks_type_contract::FunctionArgumentType::Value(value)) => Ok(value.clone()),
        Some(_) => Err(FragmentBindingError::new(
            FragmentBindingTarget::Program,
            FragmentBindingErrorKind::WrongAssignmentKind,
            format!("compiled expression root {site:?} is not a value"),
        )),
        None => Err(FragmentBindingError::new(
            FragmentBindingTarget::Program,
            FragmentBindingErrorKind::MissingAssignment,
            format!("compiled expression root {site:?} has no static type"),
        )),
    }
}

fn borrowed_project_root_value_type(
    program: &LocalProgram,
    site: ProgramRootRef,
) -> Result<
    &novarocks_type_contract::FunctionValueType,
    crate::exec::pipeline::operator::ConstructionFailure,
> {
    use crate::exec::pipeline::operator::ConstructionFailure;
    let unit = program.unit(site.unit).ok_or_else(|| {
        ConstructionFailure::InvalidSource("compiled project unit is absent".into())
    })?;
    let root = unit.root(site.root).ok_or_else(|| {
        ConstructionFailure::InvalidSource("compiled project root is absent".into())
    })?;
    match unit.types().get(root.definition.index()) {
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
    sites: Vec<ProgramRootRef>,
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
    ) -> ExecutionResult<Self> {
        if !matches!(
            program.graph().sink(),
            Some(novarocks_local_program::StaticSinkProgram::RootResult(_))
        ) {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                "final result boundary requires its actual RootResult sink",
            ));
        }
        let root = program.graph().root();
        let node = program.graph().nodes().get(root.index()).ok_or_else(|| {
            program_binding_failure(
                PlanBindingKind::MissingAssignment,
                "compiled final result root is absent",
            )
        })?;
        if !node.output_layout().is_typed() {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                "compiled final result requires a complete output layout",
            ));
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
    ) -> ExecutionResult<Self> {
        let graph_node = program.graph().nodes().get(node.index()).ok_or_else(|| {
            program_binding_failure(
                PlanBindingKind::MissingAssignment,
                "compiled Project node is absent",
            )
        })?;
        let ProgramNodeKind::Project {
            input,
            exprs,
            expr_slot_ids,
            validate_final_result_input,
            output_indices,
            ..
        } = graph_node.kind()
        else {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                "compiled node is not a Project",
            ));
        };
        let sites = exprs.to_vec();
        let final_identity_slots = if *validate_final_result_input {
            if output_indices.is_some() || exprs.len() != expr_slot_ids.len() {
                return Err(program_binding_failure(
                    PlanBindingKind::InvalidAssignment,
                    "final result input validation requires an identity output layout",
                ));
            }
            Some(
                exprs
                    .iter()
                    .zip(expr_slot_ids)
                    .map(|(expr, output)| {
                        let source = program.unit(expr.unit).and_then(|unit| {
                            let root = unit.root(expr.root)?;
                            if !matches!(
                                unit.definitions().node(root.definition)?.kind(),
                                novarocks_expression::StaticExprKind::Input
                            ) {
                                return None;
                            }
                            let binding = unit.input(root.use_id)?;
                            if binding.scope != root.scope {
                                return None;
                            }
                            program
                                .graph()
                                .nodes()
                                .get(input.index())?
                                .output_layout()
                                .slots()
                                .get(binding.ordinal as usize)
                                .copied()
                        });
                        source.map(|source| (source, *output)).ok_or_else(|| {
                            program_binding_failure(
                                PlanBindingKind::InvalidAssignment,
                                "final result input validation requires identity expressions",
                            )
                        })
                    })
                    .collect::<ExecutionResult<Vec<_>>>()?,
            )
        } else {
            None
        };
        if !graph_node.output_layout().is_typed() {
            return Err(program_binding_failure(
                PlanBindingKind::InvalidAssignment,
                "compiled Project requires its complete output layout",
            ));
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
        Box::new(Transform::hydrating(CompiledProjectProcessor {
            name: self.name.clone(),
            program: Arc::clone(&self.program),
            sites: self.sites.clone(),
            final_identity_slots: self.final_identity_slots.clone(),
            layout_node: self.layout_node,
            preparation_site: self.preparation_site,
            output: None,
            construction: None,
            control: RuntimeKernelControl::new(Arc::clone(&self.error))
                .at_expression_node(self.layout_node),
            instances: None,
        }))
    }
}
struct CompiledProjectProcessor {
    name: String,
    program: Arc<LocalProgram>,
    sites: Vec<ProgramRootRef>,
    final_identity_slots: Option<Vec<(novarocks_types::SlotId, novarocks_types::SlotId)>>,
    layout_node: ProgramNodeId,
    preparation_site: crate::runtime::preparation_metadata::CompiledSchemaSite,
    output: Option<ChunkSchemaRef>,
    construction: Option<Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>>,
    control: RuntimeKernelControl,
    instances: Option<Vec<CompiledExpressionInstance>>,
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
    let control = RuntimeErrorState::default();
    let mut turn = crate::exec::pipeline::driver_step::TurnWork::new(std::num::NonZeroU64::MIN, 64);
    let mut value_conversions = novarocks_value_arrays::decode::ValueConversionContext::default();
    let mut ctx = crate::exec::pipeline::driver_step::DriverStep::new(
        &control,
        &mut turn,
        &mut value_conversions,
    );
    processor
        .apply(
            &mut ctx,
            crate::exec::pipeline::input::PreparedInput::for_test(chunk),
        )?
        .output
        .ok_or_else(|| {
            novarocks_functions::KernelFailure::Internal(
                novarocks_functions::KernelDiagnostic::new("source fixture Project has no output"),
            )
            .into()
        })
}
impl OperatorLifecycle for CompiledProjectProcessor {
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
}
impl InputEncodingPolicy for CompiledProjectProcessor {
    fn retains_encoded_path(
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
}
impl TransformKernel for CompiledProjectProcessor {
    fn end_input(
        &mut self,
        _ctx: &mut crate::exec::pipeline::driver_step::DriverStep<'_>,
    ) -> ExecutionResult<crate::exec::pipeline::shape::TransformInputEnd> {
        // Every accepted input has already emitted its sole result. This
        // computing boundary owns no EOS payload or further materialization.
        Ok(crate::exec::pipeline::shape::TransformInputEnd::Finished)
    }

    fn apply(
        &mut self,
        _ctx: &mut crate::exec::pipeline::driver_step::DriverStep<'_>,
        chunk: crate::exec::pipeline::input::PreparedInput,
    ) -> ExecutionResult<StepResult> {
        let chunk = chunk.into_uncovered_chunk()?;
        use super::project_processor::{
            ProjectEncodingSource, materialize_project_output_owned, project_reserve,
        };
        use crate::exec::pipeline::operator::ConstructionFailure;
        let rows = u64::try_from(chunk.len()).expect("supported row count fits u64");
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
            let projected = materialize_project_output_owned(
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
            )?;
            return Ok(StepResult::new(Some(projected), NextStep::NeedInput, rows));
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
        let projected = materialize_project_output_owned(
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
        )?;
        Ok(StepResult::new(Some(projected), NextStep::NeedInput, rows))
    }
}

/// Filter by the original ordered Filter or Scan TruthOnly predicate roots.
/// Both preserve the exact
/// input port and keep only rows whose complete predicate is TRUE.
#[derive(Clone)]
pub struct CompiledFilterProcessorFactory {
    name: String,
    roots: Vec<(
        Arc<novarocks_expression::CompiledExprSet>,
        novarocks_expression_contract::RootOrdinal,
    )>,
    node: ProgramNodeId,
    error: Arc<RuntimeErrorState>,
}
impl CompiledFilterProcessorFactory {
    pub(crate) fn try_new(
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        error: Arc<RuntimeErrorState>,
    ) -> ExecutionResult<Self> {
        let graph_node = program.graph().nodes().get(node.index()).ok_or_else(|| {
            program_binding_failure(
                PlanBindingKind::MissingAssignment,
                "compiled filter node is absent",
            )
        })?;
        let (predicates, label): (&[ProgramRootRef], _) = match graph_node.kind() {
            ProgramNodeKind::Filter { predicates, .. } => (predicates, "COMPILED_FILTER"),
            ProgramNodeKind::Scan { residuals, .. } => (residuals, "COMPILED_SCAN_RESIDUAL"),
            _ => {
                return Err(program_binding_failure(
                    PlanBindingKind::InvalidAssignment,
                    "compiled filter requires its actual Filter or Scan node",
                ));
            }
        };
        if predicates.is_empty() {
            return Err(program_binding_failure(
                PlanBindingKind::MissingAssignment,
                "compiled filter has no actual predicate",
            ));
        }
        let mut roots = Vec::with_capacity(predicates.len());
        for predicate in predicates {
            let unit = program.unit(predicate.unit).ok_or_else(|| {
                program_binding_failure(
                    PlanBindingKind::MissingAssignment,
                    "compiled filter unit is absent",
                )
            })?;
            roots.push((Arc::clone(unit), predicate.root));
        }
        Ok(Self {
            name: format!("{label} (node={})", node.index()),
            roots,
            node,
            error,
        })
    }
}
/// One synchronous predicate body. Pipeline buffering and source polling
/// stay in their respective shells; both consume the same conjunction state.
pub(crate) struct CompiledFilterBody {
    roots: Vec<(
        Arc<novarocks_expression::CompiledExprSet>,
        novarocks_expression_contract::RootOrdinal,
    )>,
    node: ProgramNodeId,
    instance: Option<crate::exec::expr::compiled_program::CompiledFilterConjunctionInstance>,
}
impl CompiledFilterBody {
    pub(crate) fn evaluate(
        &mut self,
        input: CompiledInputLoan<'_>,
        control: &RuntimeKernelControl,
    ) -> ExecutionResult<BooleanArray> {
        if self.instance.is_none() {
            self.instance = Some(crate::exec::expr::compiled_program::CompiledFilterConjunctionInstance::try_new_with_allocator(
                &self.roots, control, control.allocator(),
            ).map_err(|cause| control.project_required_failure(cause.into()))?);
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
            )
            .map_err(|cause| {
                control.project_required_failure(cause.at_expression_node(self.node))
            })?;
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
            roots: self.roots.clone(),
            node: self.node,
            instance: None,
        }
    }
}
impl OperatorFactory for CompiledFilterProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }
    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(Transform::hydrating(CompiledFilterProcessor {
            name: self.name.clone(),
            body: self.create_body(),
            control: RuntimeKernelControl::new(Arc::clone(&self.error))
                .at_expression_node(self.node),
        }))
    }
}
#[derive(Debug)]
struct CompiledFilterArrowFailure(arrow::error::ArrowError);

impl std::fmt::Display for CompiledFilterArrowFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "filter kernel: {}", self.0)
    }
}

impl std::error::Error for CompiledFilterArrowFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

impl novarocks_failure_contract::FailureProjection for CompiledFilterArrowFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            crate::runtime::task::ExecutionReason::FilterArrow.reason(),
        )
    }
}

struct CompiledFilterProcessor {
    name: String,
    body: CompiledFilterBody,
    control: RuntimeKernelControl,
}
impl OperatorLifecycle for CompiledFilterProcessor {
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
}
impl crate::exec::pipeline::operator::InputEncodingPolicy for CompiledFilterProcessor {}
impl crate::exec::pipeline::shape::TransformKernel for CompiledFilterProcessor {
    fn end_input(
        &mut self,
        _ctx: &mut crate::exec::pipeline::driver_step::DriverStep<'_>,
    ) -> ExecutionResult<crate::exec::pipeline::shape::TransformInputEnd> {
        // Every accepted input has already emitted its sole result. This
        // computing boundary owns no EOS payload or further materialization.
        Ok(crate::exec::pipeline::shape::TransformInputEnd::Finished)
    }

    fn apply(
        &mut self,
        _ctx: &mut crate::exec::pipeline::driver_step::DriverStep<'_>,
        chunk: crate::exec::pipeline::input::PreparedInput,
    ) -> ExecutionResult<crate::exec::pipeline::step::StepResult> {
        let chunk = chunk.into_uncovered_chunk()?;
        let rows = u64::try_from(chunk.len()).expect("supported row count fits u64");
        let truth = self
            .body
            .evaluate(CompiledInputLoan::from_chunk(&chunk), &self.control)?;
        let mask = crate::exec::chunk::normalize_filter_mask_observed(&truth, || Ok(()))?;
        let batch = filter_record_batch(chunk.record_batch(), &mask)
            .map_err(|error| ExecutionFailure::from_domain(CompiledFilterArrowFailure(error)))?;
        let output = Chunk::try_new_with_chunk_schema(batch, chunk.chunk_schema_ref())?;
        Ok(StepResult::new(Some(output), NextStep::NeedInput, rows))
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

#[cfg(test)]
mod controlled_wait_tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn compiled_kernel_wait_is_registered_and_interrupted_by_the_runtime_error_owner() {
        let error = Arc::new(RuntimeErrorState::default());
        let original = ExecutionFailure::from(KernelFailure::InvalidProgram(
            KernelDiagnostic::new("the original fragment stop"),
        ));
        let control = RuntimeKernelControl::new(Arc::clone(&error));
        let waiter = std::thread::spawn(move || {
            KernelEvaluationControl::wait(&control, Duration::from_secs(30))
        });
        let registration_deadline = Instant::now() + Duration::from_secs(3);
        while error.waiting_count() == 0 && Instant::now() < registration_deadline {
            std::thread::yield_now();
        }
        let registered = error.waiting_count() == 1;
        // Always release the owned waiter, including a failed registration oracle.
        error.set_error(original.clone());
        let result = waiter.join().expect("controlled waiter exited");
        assert!(
            registered,
            "the real runtime wait must register before interruption"
        );
        assert!(matches!(result, Err(KernelFailure::Cancelled)));
        assert_eq!(error.waiting_count(), 0);
        assert_eq!(error.error(), Some(original));
        let control = RuntimeKernelControl::new(Arc::clone(&error));
        assert!(matches!(
            KernelEvaluationControl::wait(&control, Duration::from_secs(30)),
            Err(KernelFailure::Cancelled)
        ));
        assert_eq!(error.waiting_count(), 0);
    }
}

#[cfg(test)]
mod runtime_stop_source_tests {
    use super::*;

    #[test]
    fn required_stop_projection_keeps_only_the_cause_observed_by_its_control_port() {
        for wait in [false, true] {
            let state = Arc::new(RuntimeErrorState::default());
            let control = RuntimeKernelControl::new(state.clone());
            let original = ExecutionFailure::from(KernelFailure::Operational(
                KernelDiagnostic::new("original fragment failure"),
            ));
            let independent = ExecutionFailure::from(KernelFailure::Cancelled);
            state.set_error(original.clone());
            // A kernel refusal before this port observes a stop cannot adopt
            // an already-published or later-published fragment cause.
            assert!(
                control
                    .project_required_failure(independent.clone())
                    .same_occurrence(&independent)
            );
            let signal = if wait {
                KernelEvaluationControl::wait(&control, Duration::ZERO)
            } else {
                KernelEvaluationControl::checkpoint(&control, 1)
            }
            .expect_err("this actual control port observes its owner stop");
            let projected = control.project_required_failure(signal.into());
            assert_eq!(projected, original);
            assert!(projected.same_occurrence(&original));
            let different = ExecutionFailure::from(KernelFailure::Internal(KernelDiagnostic::new(
                "independent kernel invariant",
            )));
            assert!(
                control
                    .project_required_failure(different.clone())
                    .same_occurrence(&different),
                "a different kernel failure keeps its own cause"
            );
        }
    }
}

#[cfg(test)]
mod mixed_unit_filter_tests {
    use super::*;
    use crate::exec::expr::test_support::{HarnessControl, constant_policy};
    use arrow::datatypes::{DataType, Schema};
    use novarocks_expression::{
        CompiledExprSet, EvaluationEnvironment, ScopeColumn, ScopeLayout, ScopeLayouts,
        compile_expr_set_pure,
    };
    use novarocks_expression_contract::{
        BoundExprSetBuilder, ExprKind, ExprPort, RootOrdinal, RootScope, UnitLimits, ValueId,
    };
    use novarocks_local_program::*;
    use novarocks_type_contract::{
        CompileCheckpoints, CompilePhase, EvaluationDemand, FunctionValueType, SemanticParameters,
        ValueTypeStorageLifetime,
    };
    use std::{
        collections::{BTreeMap, BTreeSet},
        num::NonZeroUsize,
    };

    fn predicate_unit(layout: &StaticLayout, ty: &FunctionValueType) -> Arc<CompiledExprSet> {
        let control = HarnessControl;
        let value = ValueId::new(17);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        let mut draft = BoundExprSetBuilder::new(UnitLimits {
            expression_semantic_depth: novarocks_type_contract::MAX_CONTROL_DEPTH,
        });
        let definition = draft.add(None, ty.clone(), ExprKind::Value(value)).unwrap();
        let root = draft
            .root(definition, EvaluationDemand::TruthOnly, RootScope::Port(0))
            .unwrap();
        assert_eq!(root, RootOrdinal::new(0));
        let unit = draft
            .seal(
                vec![ExprPort {
                    values: Box::from([(value, ty.clone())]),
                }],
                &mut work,
            )
            .unwrap()
            .finish(BTreeMap::new(), &mut work)
            .unwrap();
        work.finish().unwrap();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
        let scope = ScopeLayout::try_new(
            layout.schema().clone(),
            Arc::from([ty.clone()]),
            Arc::from([ScopeColumn {
                value,
                ordinal: 0,
                ty: ty.clone(),
            }]),
            ValueTypeStorageLifetime::pure(),
            &mut work,
        )
        .unwrap();
        // This real checked unit contains only a declared input, no call or constant.
        let constants = novarocks_physical_plan::ConstantPools::empty();
        let parameters = SemanticParameters::try_new([]).unwrap();
        let catalog = crate::exec::expr::compiled_program::tests::rng_subset();
        let compiled = compile_expr_set_pure(
            &unit,
            EvaluationEnvironment {
                constants: &constants,
                parameters: &parameters,
            },
            ScopeLayouts {
                ports: &[scope],
                all_ports: None,
            },
            constant_policy(),
            &catalog,
            &mut work,
        )
        .unwrap();
        work.finish().unwrap();
        compiled
    }

    #[test]
    fn filter_factory_preserves_actual_roots_from_distinct_unit_table_entries() {
        let control = HarnessControl;
        let ty = FunctionValueType::new(DataType::Boolean, false);
        let layout = StaticLayout::try_new_typed_for_compile(
            Arc::new(Schema::new(vec![ty.try_to_field("predicate").unwrap()])),
            Arc::from([novarocks_types::SlotId::new(17)]),
            vec![(ty.typed_binding().clone(), None)],
            ValueTypeStorageLifetime::pure(),
            |_| Ok(()),
            &control,
        )
        .unwrap();
        let input = RecordBatch::try_new(
            layout.schema().clone(),
            vec![Arc::new(BooleanArray::from(vec![true, false, true]))],
        )
        .unwrap();
        // Exercise both the same scope pointer and equal independently authored scopes.
        for reuse_actual_arc in [true, false] {
            let first = predicate_unit(&layout, &ty);
            let second = if reuse_actual_arc {
                Arc::clone(&first)
            } else {
                predicate_unit(&layout, &ty)
            };
            assert_eq!(Arc::ptr_eq(&first, &second), reuse_actual_arc);
            let units = vec![first, second];
            let predicates = [0, 1]
                .map(|index| ProgramRootRef::new(ProgramUnitId::new(index), RootOrdinal::new(0)));
            let values = StaticValues::try_new(input.clone(), layout.clone()).unwrap();
            let nodes = vec![
                ProgramNode::new_local(
                    ProgramNodeId::new(0),
                    vec![DiagnosticSourceNodeId::new(0)],
                    ProgramNodeKind::Values { values },
                    layout.clone(),
                ),
                ProgramNode::new_local(
                    ProgramNodeId::new(1),
                    vec![DiagnosticSourceNodeId::new(1)],
                    ProgramNodeKind::Filter {
                        input: ProgramNodeId::new(0),
                        predicates: Box::from(predicates),
                    },
                    layout.clone(),
                ),
            ];
            let graph = LocalProgramGraph::try_new_with_sink(
                nodes,
                ProgramNodeId::new(1),
                &units,
                CompileProfile::new(
                    NonZeroUsize::new(1).unwrap(),
                    None,
                    layout.identity().unwrap(),
                    KernelAbiVersion::CURRENT,
                ),
                BindingRequirements::try_new(vec![BindingRequirement::ResultSink {
                    layout: layout.clone(),
                }])
                .unwrap(),
                Some(StaticSinkProgram::Result),
            )
            .unwrap();
            let channels = [0, 1]
                .map(|node| {
                    (
                        ProgramChannelSite::Layout {
                            node: ProgramNodeId::new(node),
                            role: ProgramChannelLayoutRole::NodeOutput,
                            ordinal: 0,
                        },
                        ty.clone(),
                    )
                })
                .to_vec();
            let operators = [0, 1]
                .map(|index| LocalOperatorProvenance {
                    id: LocalOperatorId::new(index),
                    lowered_nodes: Box::from([ProgramNodeId::new(index as usize)]),
                    sources: Box::from([DiagnosticSourceNodeId::new(index)]),
                    origin: LocalOperatorOrigin::Direct,
                    cost_owner: LocalOperatorId::new(index),
                    metrics: OperatorMetricAggregation {
                        cpu_time: MetricAggregation::Sum,
                        wall_time: MetricAggregation::Maximum,
                        peak_retained_bytes: MetricAggregation::Maximum,
                    },
                })
                .to_vec();
            let program = Arc::new(
                LocalProgram::try_new(
                    graph,
                    units,
                    vec![],
                    channels,
                    operators,
                    &BTreeSet::from([
                        DiagnosticSourceNodeId::new(0),
                        DiagnosticSourceNodeId::new(1),
                    ]),
                    CompiledProgramFacts::default(),
                    &control,
                )
                .unwrap(),
            );
            let ProgramNodeKind::Filter {
                predicates: actual, ..
            } = program.graph().nodes()[1].kind()
            else {
                panic!("the final author must retain the actual Filter node")
            };
            assert_eq!(actual.as_ref(), &predicates);
            let error = Arc::new(RuntimeErrorState::default());
            let factory = CompiledFilterProcessorFactory::try_new(
                program,
                ProgramNodeId::new(1),
                Arc::clone(&error),
            )
            .expect("the checked Filter roots may use distinct unit table entries");
            let mut body = factory.create_body();
            let truth = body
                .evaluate(
                    CompiledInputLoan::RecordBatch(&input),
                    &RuntimeKernelControl::new(error),
                )
                .unwrap();
            assert_eq!(
                truth.iter().collect::<Vec<_>>(),
                vec![Some(true), Some(false), Some(true)]
            );
        }
    }
}
