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

//! Materialize admitted constant cells into the existing static Values owner.
//! Every other cell stays a dynamic `ValuesCell` root that the one compiled
//! evaluator runs at runtime; nothing is evaluated or folded here. This is not
//! an expression evaluator or an opaque Arrow memory grant.

use crate::{
    assert_rows::reserve_vec, expressions::LoweredExpressions, lowering::FragmentCompileError,
};
use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchOptions, UInt64Array, new_empty_array};
use arrow_schema::DataType;
use arrow_select::{
    interleave::interleave,
    take::{TakeOptions, take},
};
use novarocks_functions::{
    ConstantError, ConstantValue, KernelDiagnostic, KernelEvaluationControl, KernelFailure,
    selected_copy::{self, CopyConstructionError, CopyError, CopyIndexView},
};
use novarocks_local_program::{
    ProgramExprId, ProgramNodeKind, StaticExprKind, StaticLayout, StaticValues, StaticValuesCell,
};
use novarocks_physical_plan::{
    ExprId, ExprKind, ExpressionRootRole, FragmentPackage, PhysicalNode,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, ExpressionUseId, PureCompileControl,
    ValueTypeVisit, validate_arrow_carrier_parameters_observed,
    validate_value_type_structure_with_profile_observed,
};
use novarocks_types::SlotId;
use std::{collections::BTreeSet, sync::Arc};

/// One admitted Values cell: a compile-time constant materialized into the
/// backing, or a dynamic cell kept as its own runtime root.
#[derive(Clone, Copy)]
enum Cell<'a> {
    Constant(&'a ConstantValue),
    Dynamic(ProgramExprId),
}

fn cell<'a>(
    package: &FragmentPackage,
    expressions: &'a LoweredExpressions,
    id: ExprId,
) -> Result<Cell<'a>, FragmentCompileError> {
    let source = package
        .fragment()
        .expressions()
        .get(id)
        .ok_or(FragmentCompileError::Invalid(
            "missing Values cell definition",
        ))?;
    let local = *expressions
        .ids
        .get(&id)
        .ok_or(FragmentCompileError::Invalid("Values cell was not lowered"))?;
    if !matches!(source.kind, ExprKind::Literal(_) | ExprKind::Constant(_)) {
        // The compiled evaluator runs it once, over an empty port, with the
        // same value, NULL and row-error effects as any other root.
        return Ok(Cell::Dynamic(local));
    }
    let lowered = expressions
        .arena
        .node(local)
        .ok_or(FragmentCompileError::Invalid(
            "missing lowered Values definition",
        ))?;
    let StaticExprKind::Constant(value) = lowered.kind() else {
        return Err(FragmentCompileError::Invalid(
            "Values constant source changed",
        ));
    };
    Ok(Cell::Constant(value))
}
fn constant<'a>(
    package: &FragmentPackage,
    expressions: &'a LoweredExpressions,
    id: ExprId,
) -> Result<Option<&'a ConstantValue>, FragmentCompileError> {
    Ok(match cell(package, expressions, id)? {
        Cell::Constant(value) => Some(value),
        Cell::Dynamic(_) => None,
    })
}
#[derive(Clone, Copy, Debug)]
#[repr(u16)]
enum ValuesArrayOperation {
    Take = 30,
    Interleave = 31,
    Batch = 32,
}
#[derive(Debug)]
struct ValuesArrayFailure {
    operation: ValuesArrayOperation,
    source: arrow_schema::ArrowError,
}
impl std::fmt::Display for ValuesArrayFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Values {:?}: {}", self.operation, self.source)
    }
}
impl std::error::Error for ValuesArrayFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl novarocks_failure_contract::FailureProjection for ValuesArrayFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::*;
        let class = match &self.source {
            arrow_schema::ArrowError::MemoryError(_) => FailureClass::Resource,
            _ => FailureClass::Unexpected,
        };
        FailureFact::new(
            class,
            FailureReason::new(
                domain::LOCAL_COMPILER,
                std::num::NonZeroU16::new(self.operation as u16).unwrap(),
            ),
        )
    }
}
fn values_array_error(
    operation: ValuesArrayOperation,
    source: arrow_schema::ArrowError,
) -> FragmentCompileError {
    FragmentCompileError::Owner {
        phase: "Values array construction",
        error: novarocks_failure_contract::SharedFailureCause::new(ValuesArrayFailure {
            operation,
            source,
        }),
    }
}

fn copy_error(error: CopyError, node: novarocks_physical_plan::NodeId) -> FragmentCompileError {
    match error {
        CopyError::Control(KernelFailure::CompileControl(cause)) => {
            FragmentCompileError::Control(cause)
        }
        CopyError::Control(KernelFailure::ResourceExhausted) => {
            FragmentCompileError::Control(CompileControlError::ResourceExhausted)
        }
        CopyError::Control(KernelFailure::Cancelled) => {
            FragmentCompileError::Control(CompileControlError::Cancelled)
        }
        CopyError::Control(KernelFailure::DeadlineExceeded) => {
            FragmentCompileError::Control(CompileControlError::DeadlineExceeded)
        }
        CopyError::Unsupported(_) => FragmentCompileError::Unsupported {
            node: Some(node),
            feature: "Values combined copy carrier lacks selected-copy preflight",
        },
        error => FragmentCompileError::Owner {
            phase: "Values copy",
            error: novarocks_failure_contract::SharedFailureCause::new(error),
        },
    }
}
struct CompileCopyControl<'a>(&'a dyn PureCompileControl);
impl KernelEvaluationControl for CompileCopyControl<'_> {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        self.0
            .checkpoint(CompilePhase::LowerProgram, units)
            .map_err(KernelFailure::CompileControl)
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        Err(KernelFailure::InvalidProgram(KernelDiagnostic::new(
            "compiler copy observer cannot wait",
        )))
    }
}
fn copy_construction_error<H>(
    error: CopyConstructionError<H>,
    node: novarocks_physical_plan::NodeId,
) -> crate::FragmentCreationError<H> {
    match error {
        CopyConstructionError::Creator(cause) => crate::FragmentCreationError::Creator(cause),
        CopyConstructionError::Resource(cause) => crate::FragmentCreationError::Resource(cause),
        CopyConstructionError::Copy(cause) => copy_error(cause, node).into(),
    }
}

fn reserve_created<T, H>(
    values: &mut Vec<T>,
    count: usize,
    work: &mut CompileCheckpoints<'_>,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
) -> Result<(), crate::FragmentCreationError<H>> {
    let capacity = values
        .len()
        .checked_add(count)
        .ok_or(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic)?;
    let layout = std::alloc::Layout::array::<T>(capacity).map_err(|_| {
        novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic
    })?;
    crate::creation::request(layout, creation, work)?;
    reserve_vec(values, count, work)?;
    Ok(())
}

pub(crate) fn lower_values_with_creation<H>(
    package: &FragmentPackage,
    node: &PhysicalNode,
    expressions: &LoweredExpressions,
    slots: &[SlotId],
    control: &dyn PureCompileControl,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
) -> Result<(ProgramNodeKind, StaticLayout), crate::FragmentCreationError<H>> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = lower_core_with_creation(package, node, expressions, slots, &mut work, creation);
    crate::creation::finish_attempt(result, work)
}
fn lower_core_with_creation<H>(
    package: &FragmentPackage,
    node: &PhysicalNode,
    expressions: &LoweredExpressions,
    slots: &[SlotId],
    work: &mut CompileCheckpoints<'_>,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
) -> Result<(ProgramNodeKind, StaticLayout), crate::FragmentCreationError<H>> {
    let novarocks_physical_plan::NodeKind::Values { rows } = &node.kind else {
        return Err((FragmentCompileError::Invalid("Values source kind differs")).into());
    };
    if !node.inputs.is_empty() || slots.len() != node.output.columns.len() {
        return Err(
            (FragmentCompileError::Invalid("Values input or channel width differs")).into(),
        );
    }
    for row in rows {
        let same = row.len() == slots.len();
        work.step()?;
        if !same {
            return Err((FragmentCompileError::Invalid("Values row width differs")).into());
        }
    }
    // Validate every source before materializing any column. No dynamic call is
    // evaluated, folded, or removed from its original required occurrence: it
    // stays a ValuesCell root at its exact position.
    let mut constant_cells: Vec<usize> = Vec::new();
    reserve_created(&mut constant_cells, slots.len(), work, creation)?;
    for _ in 0..slots.len() {
        constant_cells.push(0);
        work.step()?;
    }
    let mut dynamic = Vec::new();
    for (row_index, row) in rows.iter().enumerate() {
        for (ordinal, &id) in row.iter().enumerate() {
            let value = cell(package, expressions, id);
            work.step()?;
            let value = value?;
            let source = package
                .fragment()
                .expressions()
                .get(id)
                .ok_or(FragmentCompileError::Invalid("missing Values source"))?;
            let output = package
                .fragment()
                .values()
                .get(&node.output.columns[ordinal])
                .ok_or(FragmentCompileError::Invalid("missing Values output"))?;
            if source.owner != node.id {
                return Err((FragmentCompileError::Invalid("foreign Values cell owner")).into());
            }
            let ty = match value {
                Cell::Constant(value) => value.value_type(),
                Cell::Dynamic(_) => &source.ty,
            };
            if !ty.exactly_equals_observed::<crate::FragmentCreationError<H>>(&output.ty, || {
                work.step().map_err(Into::into)
            })? {
                return Err((FragmentCompileError::Invalid(
                    "Values cell full type differs from output",
                ))
                .into());
            }
            match value {
                Cell::Constant(_) => constant_cells[ordinal] += 1,
                Cell::Dynamic(definition) => {
                    work.flush()?;
                    selected_copy::values_copy_kind_for_layout_observed(
                        output.ty.typed_binding().layout(),
                        output.ty.data_type(),
                        || CompileCopyControl(work.control()).checkpoint(1),
                    )
                    .map_err(|cause| copy_error(cause, node.id))?;
                    work.flush()?;
                    if dynamic.len() == dynamic.capacity() {
                        let additional = dynamic.len().max(4);
                        reserve_created(&mut dynamic, additional, work, creation)?;
                    }
                    dynamic.push(StaticValuesCell {
                        row: u32::try_from(row_index)
                            .map_err(|_| CompileControlError::ResourceExhausted)?,
                        column: u32::try_from(ordinal)
                            .map_err(|_| CompileControlError::ResourceExhausted)?,
                        definition,
                    });
                }
            }
        }
    }
    // A column without a constant cell keeps an empty constant backing.
    // Arrow's sole empty constructor selects the first Union child even for
    // zero rows. The shared type walk also reaches nested empty Unions.
    for (output, count) in node.output.columns.iter().zip(&constant_cells) {
        work.step()?;
        if *count != 0 {
            continue;
        }
        let ty = &package
            .fragment()
            .values()
            .get(output)
            .ok_or(FragmentCompileError::Invalid("missing Values output type"))?
            .ty;
        work.flush()?;
        validate_value_type_structure_with_profile_observed::<crate::FragmentCreationError<H>>(
            ty.data_type(),
            novarocks_type_contract::ArrowTypeProfile::CanonicalValue,
            |visit| {
                if let ValueTypeVisit::TypeNode(data_type) = visit {
                    validate_arrow_carrier_parameters_observed::<ConstantError>(data_type, || {
                        work.step().map_err(Into::into)
                    })
                    .map_err(FragmentCompileError::from)?;
                }
                let unsupported = matches!(visit, ValueTypeVisit::TypeNode(DataType::Union(fields, _)) if fields.is_empty());
                work.step()?;
                if unsupported {
                    return Err((FragmentCompileError::Unsupported {
                        node: Some(node.id),
                        feature: "Values empty Union has no Arrow constructor child",
                    })
                    .into());
                }
                Ok(())
            },
        )?;
        work.flush()?;
    }
    let mut typed_layout =
        crate::typed_layout::TypedLayoutBuilder::new_with_creation(slots.len(), work, creation)?;
    let mut source_storage = creation.storage_lifetime();
    let mut columns: Vec<ArrayRef> = Vec::new();
    reserve_created(&mut columns, slots.len(), work, creation)?;
    for (ordinal, output) in node.output.columns.iter().enumerate() {
        let ty = &package
            .fragment()
            .values()
            .get(output)
            .ok_or(FragmentCompileError::Invalid("missing Values output type"))?
            .ty;
        work.flush()?;
        let name = if package
            .result()
            .is_some_and(|result| result.output.columns == node.output.columns)
        {
            let field = package
                .result()
                .and_then(|result| result.fields.get(ordinal))
                .ok_or(FragmentCompileError::Invalid("missing Values result label"))?;
            crate::typed_layout::copy_name_with_creation(
                field.alias.as_deref().unwrap_or(&field.name),
                work,
                creation,
            )?
        } else {
            crate::typed_layout::local_name_with_creation(
                "local_values",
                node.id.get(),
                ordinal,
                work,
                creation,
            )?
        };
        typed_layout.push_with_creation(ty, name, work, creation)?;
        let column = if constant_cells[ordinal] == 0 {
            work.flush()?;
            selected_copy::preflight_empty_construction(
                ty.typed_binding().layout(),
                creation,
                &CompileCopyControl(work.control()),
            )
            .map_err(|error| copy_construction_error(error, node.id))?;
            let array = new_empty_array(ty.data_type());
            work.flush()?;
            array
        } else {
            // Only this column's constant cells, in row order; a dynamic cell
            // has no placeholder in the backing.
            let count = constant_cells[ordinal];
            let mut sources = Vec::new();
            let mut choices = Vec::new();
            let mut raw_indices = Vec::new();
            reserve_created(&mut sources, count, work, creation)?;
            reserve_created(&mut choices, count, work, creation)?;
            reserve_created(&mut raw_indices, count, work, creation)?;
            let mut first: Option<&ConstantValue> = None;
            let mut same_backing = true;
            for row in rows {
                let value = constant(package, expressions, row[ordinal]);
                work.step()?;
                let Some(value) = value? else {
                    continue;
                };
                let first = *first.get_or_insert(value);
                same_backing &= value.pool().backing_identity() == first.pool().backing_identity();
                let source = value.pool().array_projection();
                source_storage = source_storage
                    .retaining_observed::<crate::FragmentCreationError<H>>(
                        &source.storage_lifetime(),
                        |layout| {
                            creation
                                .request_layout(layout)
                                .map_err(crate::FragmentCreationError::Creator)
                        },
                    )?;
                sources.push(Arc::clone(value.pool().array()));
                choices.push((sources.len() - 1, value.ordinal() as usize));
                raw_indices.push(u64::from(value.ordinal()));
                work.step()?;
            }
            let first = first.ok_or(FragmentCompileError::Invalid(
                "Values constant cell count changed",
            ))?;
            if same_backing {
                work.flush()?;
                let copy_control = CompileCopyControl(work.control());
                selected_copy::preflight_uint64_indices_construction(creation, &copy_control)
                    .map_err(|error| copy_construction_error(error, node.id))?;
                let indices = UInt64Array::from(raw_indices);
                selected_copy::preflight_take_construction(
                    first.pool().array().as_ref(),
                    CopyIndexView::UInt64(&indices),
                    creation,
                    &copy_control,
                )
                .map_err(|error| copy_construction_error(error, node.id))?;
                work.flush()?;
                let copied = take(
                    first.pool().array().as_ref(),
                    &indices,
                    Some(TakeOptions { check_bounds: true }),
                );
                let copied = copied
                    .map_err(|source| values_array_error(ValuesArrayOperation::Take, source))?;
                work.flush()?;
                copied
            } else {
                work.flush()?;
                match selected_copy::values_copy_kind_for_layout_observed(
                    ty.typed_binding().layout(),
                    ty.data_type(),
                    || CompileCopyControl(work.control()).checkpoint(1),
                )
                .map_err(|cause| copy_error(cause, node.id))?
                {
                    selected_copy::ValuesCopyKind::Interleave => {
                        selected_copy::preflight_interleave_for_layout_construction(
                            ty.typed_binding().layout(),
                            ty.data_type(),
                            &sources,
                            &choices,
                            creation,
                            &CompileCopyControl(work.control()),
                        )
                        .map_err(|error| copy_construction_error(error, node.id))?;
                        let mut borrowed: Vec<&dyn Array> = Vec::new();
                        reserve_created(&mut borrowed, sources.len(), work, creation)?;
                        for source in &sources {
                            borrowed.push(source.as_ref());
                            work.step()?;
                        }
                        work.flush()?;
                        let copied = interleave(&borrowed, &choices);
                        let copied = copied.map_err(|source| {
                            values_array_error(ValuesArrayOperation::Interleave, source)
                        })?;
                        work.flush()?;
                        copied
                    }
                    selected_copy::ValuesCopyKind::Extend => {
                        work.flush()?;
                        let copied = selected_copy::copy_nested_values_for_layout_construction(
                            ty.typed_binding().layout(),
                            &sources,
                            &choices,
                            creation,
                            &CompileCopyControl(work.control()),
                        )
                        .map_err(|cause| copy_construction_error(cause, node.id))?;
                        work.flush()?;
                        copied
                    }
                }
            }
        };
        let column = crate::array_projection::normalize_constant_column_with_creation(
            &column, ty, work, creation,
        )?;
        columns.push(column);
        work.step()?;
    }
    work.flush()?;
    let layout = typed_layout.finish_with_creation(slots, work, creation)?;
    work.flush()?;
    if !dynamic.is_empty() {
        let values = StaticValues::try_new_with_cells_with_creator(
            rows.len(),
            columns,
            dynamic,
            layout.clone(),
            source_storage,
            creation,
            work.control(),
        )?;
        work.flush()?;
        return Ok((ProgramNodeKind::Values { values }, layout));
    }
    let batch = RecordBatch::try_new_with_options(
        layout.schema().clone(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(rows.len())),
    );
    work.flush()?;
    let batch = batch.map_err(|source| values_array_error(ValuesArrayOperation::Batch, source))?;
    let values = StaticValues::try_new_with_creator(
        batch,
        layout.clone(),
        source_storage,
        creation,
        work.control(),
    )?;
    work.flush()?;
    Ok((ProgramNodeKind::Values { values }, layout))
}

pub(crate) fn retired_values_uses(
    package: &FragmentPackage,
    expressions: &LoweredExpressions,
    work: &mut CompileCheckpoints<'_>,
) -> Result<BTreeSet<ExpressionUseId>, FragmentCompileError> {
    let mut retired = BTreeSet::new();
    for (site, id) in package.expression_uses().bindings() {
        if let ExpressionRootRole::ValuesCell { .. } = site.role {
            let invocation = package.expression_uses().flow().uses().get(id).ok_or(
                FragmentCompileError::Invalid("missing Values root invocation"),
            )?;
            let cell = constant(package, expressions, invocation.definition);
            work.step()?;
            // A dynamic cell keeps its root use and every argument use.
            if cell?.is_some() {
                if !invocation.arguments.is_empty() {
                    return Err(FragmentCompileError::Invalid(
                        "constant Values root has arguments",
                    ));
                }
                retired.insert(*id);
            }
        }
        work.step()?;
    }
    Ok(retired)
}

#[cfg(test)]
pub(crate) fn lower_values(
    package: &FragmentPackage,
    node: &PhysicalNode,
    expressions: &LoweredExpressions,
    slots: &[SlotId],
    control: &dyn PureCompileControl,
) -> Result<(ProgramNodeKind, StaticLayout), FragmentCompileError> {
    crate::creation::pure(|creation| {
        lower_values_with_creation(package, node, expressions, slots, control, creation)
    })
}

#[cfg(test)]
mod failure_projection_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use std::error::Error;
    #[test]
    fn values_array_operations_preserve_sdk_source_without_text_classification() {
        for operation in [
            ValuesArrayOperation::Take,
            ValuesArrayOperation::Interleave,
            ValuesArrayOperation::Batch,
        ] {
            let memory = values_array_error(
                operation,
                arrow_schema::ArrowError::MemoryError("actual Values memory refusal".into()),
            );
            assert_eq!(memory.failure_fact().class(), FailureClass::Resource);
            assert_eq!(memory.failure_fact().resource(), None);
            assert_eq!(memory.failure_fact().reason().code(), operation as u16);
            assert!(
                memory
                    .source()
                    .unwrap()
                    .source()
                    .unwrap()
                    .source()
                    .unwrap()
                    .is::<arrow_schema::ArrowError>()
            );
            let sdk = values_array_error(
                operation,
                arrow_schema::ArrowError::ComputeError("resource exhausted 403 overflow".into()),
            );
            assert_eq!(sdk.failure_fact().class(), FailureClass::Unexpected);
            assert!(
                sdk.source()
                    .unwrap()
                    .source()
                    .unwrap()
                    .source()
                    .unwrap()
                    .is::<arrow_schema::ArrowError>()
            );
        }
    }
}
