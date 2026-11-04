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

//! The compiled Values source for Values with dynamic cells.
//!
//! Each dynamic cell is a `ValuesCell { row, column }` root. On its opening
//! turn the source evaluates every dynamic cell exactly once, in row-major
//! order, through the one compiled evaluator over an explicit empty port of
//! one row: the same value, NULL and row-error effects as v1's per-cell
//! evaluation. A cell's row error is a typed required-expression error of its
//! own root, and the first failing cell in row-major order is the one
//! reported. Each column is then assembled exactly in its frozen layout type
//! from its constant cells and its evaluated cells; no cell is cast, coerced
//! or defaulted here. All rows are emitted from the one source driver.

use crate::exec::pipeline::operator::OperatorLifecycle;
use std::sync::Arc;

use crate::exec::pipeline::operator::{ConstructionFailure, InputConstructionOwner};
use arrow::array::{Array, ArrayRef};
use arrow::compute::kernels::interleave::interleave;
use arrow::datatypes::Schema;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use novarocks_functions::{
    KernelDiagnostic, KernelEvaluationControl, KernelFailure, MAX_UNOBSERVED_KERNEL_WORK,
    selected_copy::{self, CopyConstructionError},
};
use novarocks_local_program::{
    LocalProgram, ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeId,
    ProgramNodeKind, StaticValuesBacking,
};
use novarocks_type_contract::{
    ArrowTypeProfile, ValueSchemaCreation, arrow_data_types_exact_with_profile_observed,
};

use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
use crate::exec::expr::compiled_program::CompiledExpressionInstance;
use crate::exec::operators::compiled_expression::{RuntimeKernelControl, evaluate_all};
use crate::exec::pipeline::operator::Operator;
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::shape::{Source, SourceKernel, SourceStart};
use crate::exec::pipeline::step::{NextStep, StepResult};
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use crate::runtime::runtime_state::{RuntimeErrorState, RuntimeState};

fn invalid(message: &str) -> ExecutionFailure {
    KernelFailure::InvalidProgram(KernelDiagnostic::new(message)).into()
}

/// Source factory for one compiled Values node that has dynamic cells.
pub(crate) struct CompiledValuesSourceFactory {
    name: String,
    program: Arc<LocalProgram>,
    node: ProgramNodeId,
    error: Arc<RuntimeErrorState>,
}

impl CompiledValuesSourceFactory {
    pub(crate) fn try_new(
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        error: Arc<RuntimeErrorState>,
    ) -> crate::runtime::fragment::ExecutionResult<Self> {
        let graph_node = program.graph().nodes().get(node.index()).ok_or_else(|| {
            crate::exec::fragment::error::program_binding_failure(
                crate::exec::fragment::error::FragmentBindingErrorKind::MissingAssignment,
                "compiled Values node is absent",
            )
        })?;
        let ProgramNodeKind::Values { values } = graph_node.kind() else {
            return Err(crate::exec::fragment::error::program_binding_failure(
                crate::exec::fragment::error::FragmentBindingErrorKind::InvalidAssignment,
                "compiled node is not a Values source",
            ));
        };
        if !values.layout().is_typed() {
            return Err(crate::exec::fragment::error::program_binding_failure(
                crate::exec::fragment::error::FragmentBindingErrorKind::InvalidAssignment,
                "compiled Values require their complete frozen layout",
            ));
        }
        Ok(Self {
            name: format!("COMPILED_VALUES (node={})", node.index()),
            program,
            node,
            error,
        })
    }
}

impl OperatorFactory for CompiledValuesSourceFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        Box::new(Source::new(CompiledValuesSource {
            name: self.name.clone(),
            program: Arc::clone(&self.program),
            node: self.node,
            output: None,
            construction: None,
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            driver_id,
        }))
    }

    fn is_source(&self) -> bool {
        true
    }
}

struct CompiledValuesSource {
    name: String,
    program: Arc<LocalProgram>,
    node: ProgramNodeId,
    output: Option<ChunkSchemaRef>,
    construction: Option<Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>>,
    control: RuntimeKernelControl,
    driver_id: i32,
}

impl OperatorLifecycle for CompiledValuesSource {
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
        let ProgramNodeKind::Values { values } =
            self.program.graph().nodes()[self.node.index()].kind()
        else {
            return Err(invalid("compiled Values node changed"));
        };
        let factory = self
            .construction
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let owner = factory.begin_compiled_layout_construction(values.layout())?;
        let output = ChunkSchema::from_compiled_layout_owned_observed::<ConstructionFailure>(
            values.layout(),
            owner.storage_lifetime(),
            |layout| owner.request(layout),
            |units| {
                owner.observe(
                    0,
                    &[],
                    crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units },
                )
            },
        )?;
        self.output = Some(output);
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

impl SourceKernel for CompiledValuesSource {
    fn start(&self) -> SourceStart {
        if self.driver_id != 0 {
            SourceStart::Finished
        } else {
            SourceStart::Ready
        }
    }
    fn poll_output(
        &mut self,
        _ctx: &mut crate::exec::pipeline::step_context::StepContext<'_>,
    ) -> ExecutionResult<StepResult> {
        // The source pipeline has one driver; every row comes from it, so the
        // cells are evaluated once and rows keep their frozen order.
        if self.driver_id != 0 {
            return Ok(StepResult::new(None, NextStep::Finished, 0));
        }
        let output = self
            .output
            .as_ref()
            .ok_or_else(|| invalid("compiled Values source was not prepared"))?;
        let chunk = materialize(&self.program, self.node, output, &self.control)?;
        Ok(StepResult::new(Some(chunk), NextStep::Finished, 0))
    }
}

fn reserve<T>(
    count: usize,
    owner: &dyn InputConstructionOwner,
    work: &mut Work<'_>,
) -> ExecutionResult<Vec<T>> {
    let layout = values_array_layout::<T>(count)?;
    work.flush()?;
    owner.request(layout)?;
    let mut values = Vec::new();
    values.try_reserve_exact(count).map_err(|source| {
        ExecutionFailure::from_domain(
            novarocks_functions::allocation_failure::AllocationRefusal::new(
                "reserve compiled Values assembly",
                source,
            ),
        )
    })?;
    work.flush()?;
    Ok(values)
}
fn copy_failure(error: CopyConstructionError<ConstructionFailure>) -> ExecutionFailure {
    match error {
        CopyConstructionError::Creator(cause) => cause.into(),
        CopyConstructionError::Resource(cause) => ConstructionFailure::Resource(cause).into(),
        CopyConstructionError::Copy(cause) => cause.into_kernel().into(),
    }
}

/// Evaluate every dynamic cell once and assemble the frozen layout.
fn materialize(
    program: &Arc<LocalProgram>,
    node: ProgramNodeId,
    output: &ChunkSchemaRef,
    control: &RuntimeKernelControl,
) -> ExecutionResult<Chunk> {
    let ProgramNodeKind::Values { values } = program.graph().nodes()[node.index()].kind() else {
        return Err(invalid("compiled Values node changed"));
    };
    let owner = control.construction_owner(output.as_ref())?;
    let storage = owner
        .storage_lifetime()
        .retaining_observed::<crate::exec::pipeline::operator::ConstructionFailure>(
            &values.storage_lifetime(),
            |layout| owner.request(layout),
        )?;
    if let Some(projection) = values.batch_projection() {
        owner.request(std::alloc::Layout::array::<ArrayRef>(projection.record_batch().num_columns()).map_err(|_|crate::exec::pipeline::operator::ConstructionFailure::Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))?)?;
        let columns = projection.record_batch().columns().to_vec();
        return Ok(Chunk::try_new_typed_with_columns_owned_observed::<
            crate::exec::pipeline::operator::ConstructionFailure,
            _,
        >(
            output.clone(),
            columns,
            projection.record_batch().num_rows(),
            storage,
            |step| owner.observe(0, &[], step),
        )?);
    }
    let StaticValuesBacking::Cells {
        rows,
        constants,
        dynamic,
    } = values.backing()
    else {
        return Err(invalid("compiled Values source has no dynamic cell"));
    };
    let schema = values.layout().schema();
    let mut work = Work::new(control);
    work.flush()?;
    owner.request(
        novarocks_type_contract::owned_resources::layout::arc_layout(std::alloc::Layout::new::<
            Schema,
        >())
        .map_err(ConstructionFailure::Resource)?,
    )?;
    let empty = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        Vec::new(),
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .map_err(|source| {
        ExecutionFailure::from_domain(ValuesConstructionFailure::EmptyPort(source))
    })?;
    // Row-major: the first failing cell is the one v1 would have reported.
    let mut evaluated = reserve(dynamic.len(), owner.as_ref(), &mut work)?;
    for cell in dynamic {
        let site = ProgramExpressionRootSite::Node {
            node,
            role: ProgramNodeExpressionRole::ValuesCell {
                row: cell.row,
                column: cell.column,
            },
        };
        let mut instance = CompiledExpressionInstance::try_new_with_allocator(
            Arc::clone(program),
            site,
            control,
            control.allocator(),
        )?;
        let value = evaluate_all(&mut instance, site, &empty, control)?;
        let field = schema
            .fields()
            .get(cell.column as usize)
            .ok_or_else(|| invalid("Values cell column is outside its layout"))?;
        work.flush()?;
        let exact = value.len() == 1
            && arrow_data_types_exact_with_profile_observed(
                value.data_type(),
                field.data_type(),
                ArrowTypeProfile::CanonicalValue,
                || work.step(),
            )?;
        if !exact {
            return Err(invalid(
                "Values cell result differs from its frozen column type",
            ));
        }
        evaluated.push(value);
        work.step()?;
    }
    let mut columns = reserve(constants.len(), owner.as_ref(), &mut work)?;
    for (ordinal, (constant, field)) in constants.iter().zip(schema.fields()).enumerate() {
        // `sources[0]` holds this column's constant cells in row order; each
        // evaluated cell of the column follows as its own one-row source.
        let mut count = 0usize;
        for cell in dynamic {
            if cell.column as usize == ordinal {
                count += 1;
            }
            work.step()?;
        }
        let mut sources: Vec<ArrayRef> = reserve(
            count
                .checked_add(1)
                .ok_or(KernelFailure::ResourceExhausted)?,
            owner.as_ref(),
            &mut work,
        )?;
        sources.push(Arc::clone(constant));
        let mut dynamic_rows = reserve(count, owner.as_ref(), &mut work)?;
        for (cell, value) in dynamic.iter().zip(&evaluated) {
            if cell.column as usize == ordinal {
                dynamic_rows.push(cell.row as usize);
                sources.push(Arc::clone(value));
            }
            work.step()?;
        }
        if dynamic_rows.is_empty() {
            columns.push(Arc::clone(constant));
            continue;
        }
        let mut choices = reserve(rows, owner.as_ref(), &mut work)?;
        let mut next_constant = 0usize;
        let mut next_dynamic = 0usize;
        for row in 0..rows {
            if dynamic_rows.get(next_dynamic) == Some(&row) {
                next_dynamic += 1;
                choices.push((next_dynamic, 0));
            } else {
                choices.push((0, next_constant));
                next_constant += 1;
            }
            work.step()?;
        }
        if next_constant != constant.len() || next_dynamic != dynamic_rows.len() {
            return Err(invalid("Values cell sources do not cover their column"));
        }
        let mut request = |layout| owner.request(layout);
        let mut opaque = |bytes| owner.observe_opaque_upper_bound(bytes);
        let mut creation = ValueSchemaCreation::new(storage.clone(), &mut request, &mut opaque);
        work.flush()?;
        let layout = values
            .layout()
            .value_binding_at(ordinal)
            .ok_or_else(|| invalid("Values column has no complete typed binding"))?
            .layout();
        let column = match selected_copy::values_copy_kind_for_layout_observed(
            layout,
            field.data_type(),
            || work.step(),
        )
        .map_err(|cause| copy_failure(cause.into()))?
        {
            selected_copy::ValuesCopyKind::Interleave => {
                selected_copy::preflight_interleave_for_layout_construction(
                    layout,
                    field.data_type(),
                    &sources,
                    &choices,
                    &mut creation,
                    control,
                )
                .map_err(copy_failure)?;
                let mut borrowed: Vec<&dyn Array> =
                    reserve(sources.len(), owner.as_ref(), &mut work)?;
                for source in &sources {
                    borrowed.push(source.as_ref());
                    work.step()?;
                }
                work.flush()?;
                let column = interleave(&borrowed, &choices).map_err(|error| {
                    ExecutionFailure::from_domain(ValuesConstructionFailure::Column {
                        ordinal,
                        source: error,
                    })
                })?;
                work.flush()?;
                column
            }
            selected_copy::ValuesCopyKind::Extend => {
                selected_copy::copy_nested_values_for_layout_construction(
                    layout,
                    &sources,
                    &choices,
                    &mut creation,
                    control,
                )
                .map_err(copy_failure)?
            }
        };
        columns.push(column);
    }
    work.flush()?;
    Ok(Chunk::try_new_typed_with_columns_owned_observed::<
        crate::exec::pipeline::operator::ConstructionFailure,
        _,
    >(output.clone(), columns, rows, storage, |step| {
        owner.observe(0, &[], step)
    })?)
}

/// Bounded observation of this source's own assembly work.
struct Work<'a> {
    control: &'a dyn KernelEvaluationControl,
    pending: u32,
}

impl<'a> Work<'a> {
    fn new(control: &'a dyn KernelEvaluationControl) -> Self {
        Self {
            control,
            pending: 0,
        }
    }
    fn step(&mut self) -> Result<(), KernelFailure> {
        self.pending += 1;
        if self.pending == MAX_UNOBSERVED_KERNEL_WORK {
            self.flush()?;
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<(), KernelFailure> {
        self.control.checkpoint(self.pending)?;
        self.pending = 0;
        Ok(())
    }
}

/// The compiled Values author owns failures at its actual Arrow assembly
/// sites. An allocation or kernel refusal takes its original typed path.
#[derive(Debug)]
enum ValuesConstructionFailure {
    ArrayLayout {
        count: usize,
        width: usize,
        source: std::alloc::LayoutError,
    },
    EmptyPort(arrow::error::ArrowError),
    Column {
        ordinal: usize,
        source: arrow::error::ArrowError,
    },
}
impl std::fmt::Display for ValuesConstructionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ArrayLayout {
                count,
                width,
                source,
            } => write!(
                f,
                "compiled Values backing requires {} bytes: {source}",
                *count as u128 * *width as u128
            ),
            Self::EmptyPort(source) => write!(f, "compiled Values empty port: {source}"),
            Self::Column { ordinal, source } => {
                write!(f, "compiled Values column {ordinal}: {source}")
            }
        }
    }
}
impl std::error::Error for ValuesConstructionFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ArrayLayout { source, .. } => Some(source),
            Self::EmptyPort(source) | Self::Column { source, .. } => Some(source),
        }
    }
}
impl novarocks_failure_contract::FailureProjection for ValuesConstructionFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::fragment::ExecutionReason;
        use novarocks_failure_contract::{FailureClass, FailureFact};
        use novarocks_failure_contract::{
            RepresentationBoundary, RepresentationFacts, RepresentationLayout, domain,
        };
        if let Self::ArrayLayout { count, width, .. } = self {
            return FailureFact::new(
                FailureClass::Representation,
                ExecutionReason::ValuesArrayBacking.reason(),
            )
            .with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::SingleOutput,
                layout: RepresentationLayout {
                    domain: domain::EXECUTION,
                    code: std::num::NonZeroU16::new(556).unwrap(),
                },
                required: u64::try_from(*count as u128 * *width as u128).ok(),
                extent_limit: Some(isize::MAX as u64),
                representable_prefix: None,
            });
        }
        let (reason, source) = match self {
            Self::EmptyPort(source) => (ExecutionReason::ValuesEmptyPort, source),
            Self::Column { source, .. } => (ExecutionReason::ValuesColumnAssembly, source),
            Self::ArrayLayout { .. } => unreachable!("layout returned its own representation fact"),
        };
        if matches!(source, arrow::error::ArrowError::MemoryError(_)) {
            FailureFact::new(
                FailureClass::Resource,
                ExecutionReason::ValuesArrowAllocation.reason(),
            )
        } else {
            FailureFact::new(FailureClass::Invariant, reason.reason())
        }
    }
}

fn values_array_layout<T>(count: usize) -> ExecutionResult<std::alloc::Layout> {
    std::alloc::Layout::array::<T>(count).map_err(|source| {
        ExecutionFailure::from_domain(ValuesConstructionFailure::ArrayLayout {
            count,
            width: size_of::<T>(),
            source,
        })
    })
}
#[cfg(test)]
mod allocation_domain_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection, RepresentationBoundary};
    #[test]
    fn values_array_geometry_refuses_before_funding_and_retains_real_layout_error() {
        assert_eq!(values_array_layout::<u64>(0).unwrap().size(), 0);
        assert_eq!(values_array_layout::<u64>(3).unwrap().size(), 24);
        let count = isize::MAX as usize + 1;
        let error = values_array_layout::<u8>(count).unwrap_err();
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Representation);
        assert!(fact.resource().is_none());
        let extent = fact.representation().unwrap();
        assert_eq!(extent.boundary, RepresentationBoundary::SingleOutput);
        assert_eq!(extent.required, Some(count as u64));
        assert_eq!(extent.extent_limit, Some(isize::MAX as u64));
        assert_eq!(extent.representable_prefix, None);
        let crate::runtime::fragment::ExecutionFailureCause::Domain(shared) = error.cause() else {
            panic!("the original Values author");
        };
        let original = shared.downcast_ref::<ValuesConstructionFailure>().unwrap();
        assert!(
            std::error::Error::source(original)
                .unwrap()
                .is::<std::alloc::LayoutError>()
        );
    }
    #[test]
    fn values_arrow_memory_preserves_its_actual_variant_separately_from_frozen_shape() {
        let error = ValuesConstructionFailure::Column {
            ordinal: 7,
            source: arrow::error::ArrowError::MemoryError("original allocator refusal".into()),
        };
        assert_eq!(error.failure_fact().class(), FailureClass::Resource);
        assert!(error.failure_fact().resource().is_none());
        assert!(
            std::error::Error::source(&error)
                .unwrap()
                .is::<arrow::error::ArrowError>()
        );
        let shape = ValuesConstructionFailure::Column {
            ordinal: 7,
            source: arrow::error::ArrowError::SchemaError("wrong frozen field".into()),
        };
        assert_eq!(shape.failure_fact().class(), FailureClass::Invariant);
    }
}
