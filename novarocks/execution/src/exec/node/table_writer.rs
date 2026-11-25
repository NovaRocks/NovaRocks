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

//! Native runtime binding and value obligations of a compiled TableWriter.
//!
//! `TableWriter` is an ordinary unary processor *with output*, not a terminal
//! sink. Every pipeline driver opens, owns, appends to, and finishes or aborts
//! its own [`ConnectorBatchWriter`]. Nothing about the writer is shared between
//! drivers, so this node carries no driver count, no writer mutex, and no
//! "last driver finishes" protocol: only an immutable logical recipe plus the
//! attempt-local facts a driver needs to build its own physical context.

use std::num::NonZeroU32;
use std::sync::Arc;

#[cfg(test)]
use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use novarocks_type_contract::{BorrowedTypedValueBinding, TypedValueBinding, ValueKind};
use novarocks_value_arrays::{ValueError, ValueFailure, ValueViolation};

use novarocks_connector_contract::ConnectorMutationContributionGroupId;
use novarocks_execution_contract::descriptor::ExchangeSource;
use novarocks_spi::connector::write_stack::{
    ConnectorPositionShardSource, ConnectorWriteExecution, ConnectorWriterHandle,
    ConnectorWriterPhysicalContext,
};
use novarocks_spi::connector::{ConnectorExecutionResources, ConnectorRequestContext};

use crate::exec::fragment::error::{ExecPlanBuildError, ExecPlanInvariant};
use crate::exec::node::table_write_relation::ConnectorCommitFragmentEncoder;
#[cfg(novarocks_dev_controls)]
use crate::exec::node::table_write_relation::{
    AllowTableWriteAggregates, TableWriteAggregateFaultHook,
};

/// The attempt-local facts every driver of one `TableWriter` shares.
///
/// The driver id is deliberately absent: it is the one field that differs per
/// driver, and it is supplied by `OperatorFactory::create` when that driver
/// opens its own writer. These facts are never a commit authority and never a
/// recovery token.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TableWriterPhysicalContextTemplate {
    execution_query_id: [u8; 16],
    execution_attempt_id: u64,
    task_key: [u8; 16],
    writer_ordinal: u32,
}

impl TableWriterPhysicalContextTemplate {
    pub const fn new(
        execution_query_id: [u8; 16],
        execution_attempt_id: u64,
        task_key: [u8; 16],
        writer_ordinal: u32,
    ) -> Self {
        Self {
            execution_query_id,
            execution_attempt_id,
            task_key,
            writer_ordinal,
        }
    }

    /// Complete this template for the exact driver that is opening a writer.
    pub const fn for_driver(self, driver_id: u32) -> ConnectorWriterPhysicalContext {
        ConnectorWriterPhysicalContext::new(
            self.execution_query_id,
            self.execution_attempt_id,
            self.task_key,
            driver_id,
            self.writer_ordinal,
        )
    }

    pub const fn writer_ordinal(self) -> u32 {
        self.writer_ordinal
    }
}

/// Original target facts and the output declaration after their value check.
/// The output may keep the source's root NULL declaration; nested declarations
/// and all other physical/semantic facts remain those of the target.
pub(crate) struct TargetValueDeclaration<'a> {
    pub(crate) column: usize,
    pub(crate) name: &'a str,
    pub(crate) requirement: &'a TypedValueBinding,
    pub(crate) storage: Option<BorrowedTypedValueBinding<'a>>,
    pub(crate) output: &'a TypedValueBinding,
    pub(crate) input_rows: Option<&'a [usize]>,
}

/// Fulfil the checked assignment using the original construction owner.
/// This preserves licensed byte strings and never relabels its source promise. The checked
/// source and the target requirement remain separate through materialization.
pub(crate) fn materialize_target_value(
    array: &ArrayRef,
    source: BorrowedTypedValueBinding<'_>,
    target: TargetValueDeclaration<'_>,
    context: &mut novarocks_value_arrays::decode::ValueConversionContext,
    owner: &dyn crate::exec::pipeline::operator::InputConstructionOwner,
    work: &mut impl FnMut(usize) -> crate::runtime::task::ExecutionResult<()>,
) -> crate::runtime::task::ExecutionResult<ArrayRef> {
    use crate::exec::chunk::ArrayNormalizationStep;
    use crate::runtime::task::ExecutionFailure;
    use novarocks_value_arrays::hydrate::{HydrateOptions, HydratePlan, HydrateProjection};
    let mut observed_work = |units| {
        work(units)?;
        owner
            .observe(
                target.column,
                &[],
                ArrayNormalizationStep::ValueWork { units },
            )
            .map_err(ExecutionFailure::from)
    };
    let proof = novarocks_type_contract::CheckedWriteInputProjection::try_new_observed(
        source,
        target.requirement.borrowed(),
        target.storage,
        || observed_work(1),
    )?
    .ok_or_else(|| target_value_contract(ValueViolation::Layout))?;
    if !target
        .output
        .borrowed()
        .same_physical_binding_ignoring_root_nullability_observed(
            target.requirement.borrowed(),
            || observed_work(1),
        )?
    {
        return Err(target_value_contract(ValueViolation::Layout));
    }
    let mut growth = |layout| owner.request(layout).map_err(ExecutionFailure::from);
    validate_target_requiredness(array, &target, &mut growth, &mut observed_work)?;
    if array.data_type() == target.output.data_type() {
        return Ok(Arc::clone(array));
    }
    let original = |failure| match failure {
        ValueFailure::Caller(cause) => cause,
        ValueFailure::Value(cause) => ExecutionFailure::from_domain(cause),
    };
    let mut observe = |event: novarocks_value_arrays::hydrate::HydrateEvent<'_>| {
        novarocks_functions::array_projection::observe_value_hydration(event, &mut |step| {
            owner
                .observe(target.column, &[], step)
                .map_err(ExecutionFailure::from)
        })
    };
    let plan = if target.storage.is_some() {
        HydratePlan::plan_assignment_projection_with_storage_observed(
            array,
            target.requirement,
            proof,
            context,
            &owner.storage_lifetime(),
            &mut growth,
            &mut observed_work,
            &mut observe,
        )
    } else {
        HydratePlan::plan_with_options_and_storage_observed(
            array,
            target.output,
            HydrateOptions {
                dictionary_value_layout: None,
                projection: HydrateProjection::FrozenNamesAndNullability,
            },
            context,
            &owner.storage_lifetime(),
            &mut |_, _| false,
            &mut growth,
            &mut observed_work,
            &mut observe,
        )
    }
    .map_err(original)?;
    plan.require_complete()
        .map_err(ExecutionFailure::from_domain)?;
    let output = plan
        .execute_observed(context, &mut growth, &mut observed_work, &mut observe)
        .map_err(original)?;
    let output = if target.storage.is_some() {
        // Hydrate normalizes raw annotations. The exact W layout, rather than
        // a source Field or storage T, authors the published wrapper metadata.
        novarocks_functions::array_projection::rebuild_array_observed::<ExecutionFailure, _>(
            &output,
            target.output.data_type(),
            &mut |step| {
                owner
                    .observe(target.column, &[], step)
                    .map_err(ExecutionFailure::from)
            },
        )?
    } else {
        output
    };
    if output.data_type() != target.output.data_type() {
        return Err(target_value_contract(ValueViolation::Layout));
    }
    Ok(output)
}

fn target_value_contract(violation: ValueViolation) -> crate::runtime::task::ExecutionFailure {
    crate::runtime::task::ExecutionFailure::from_domain(ValueError::Invariant(violation))
}

fn has_required_children(
    binding: BorrowedTypedValueBinding<'_>,
    work: &mut impl FnMut(usize) -> crate::runtime::task::ExecutionResult<()>,
) -> crate::runtime::task::ExecutionResult<bool> {
    work(1)?;
    let mut index = 0;
    while let Some(child) = binding.child_at(index) {
        if !child.value_type().nullable() || has_required_children(child, work)? {
            return Ok(true);
        }
        index += 1;
    }
    Ok(false)
}

struct RequiredValueBranch {
    needed: bool,
    children: std::ops::Range<usize>,
}
struct RequiredValuePlan {
    branches: Vec<RequiredValueBranch>,
    children: Vec<usize>,
}
impl RequiredValuePlan {
    fn new(
        binding: BorrowedTypedValueBinding<'_>,
        growth: &mut impl FnMut(std::alloc::Layout) -> crate::runtime::task::ExecutionResult<()>,
        work: &mut impl FnMut(usize) -> crate::runtime::task::ExecutionResult<()>,
    ) -> crate::runtime::task::ExecutionResult<Self> {
        fn storage<T>(
            count: usize,
            growth: &mut impl FnMut(std::alloc::Layout) -> crate::runtime::task::ExecutionResult<()>,
        ) -> crate::runtime::task::ExecutionResult<Vec<T>> {
            let layout = std::alloc::Layout::array::<T>(count)
                .map_err(|_| target_value_contract(ValueViolation::Bounds))?;
            if count != 0 {
                growth(layout)?;
            }
            let mut output = Vec::new();
            output.try_reserve_exact(count).map_err(|source| {
                crate::runtime::task::ExecutionFailure::from_domain(ValueError::Allocation {
                    requested: layout.size(),
                    source,
                })
            })?;
            Ok(output)
        }
        let nodes = binding.value_type().expanded_nodes();
        let mut plan = Self {
            branches: storage(nodes, growth)?,
            children: storage(nodes.saturating_sub(1), growth)?,
        };
        plan.build(binding, work)?;
        Ok(plan)
    }
    fn build(
        &mut self,
        binding: BorrowedTypedValueBinding<'_>,
        work: &mut impl FnMut(usize) -> crate::runtime::task::ExecutionResult<()>,
    ) -> crate::runtime::task::ExecutionResult<usize> {
        work(1)?;
        let node = self.branches.len();
        self.branches.push(RequiredValueBranch {
            needed: !binding.value_type().nullable(),
            children: 0..0,
        });
        let mut count = 0;
        while binding.child_at(count).is_some() {
            count += 1;
        }
        let start = self.children.len();
        self.children.resize(start + count, 0);
        self.branches[node].children = start..start + count;
        for ordinal in 0..count {
            let child = self.build(
                binding
                    .child_at(ordinal)
                    .ok_or_else(|| target_value_contract(ValueViolation::Layout))?,
                work,
            )?;
            self.children[start + ordinal] = child;
            self.branches[node].needed |= self.branches[child].needed;
        }
        Ok(node)
    }
}

fn logical_null(
    array: &ArrayRef,
    binding: BorrowedTypedValueBinding<'_>,
    row: usize,
) -> crate::runtime::task::ExecutionResult<bool> {
    if row >= array.len() {
        return Err(target_value_contract(ValueViolation::Bounds));
    }
    if matches!(array.data_type(), DataType::Dictionary(..)) {
        let layout = novarocks_value_arrays::ByteLayout::from_data_type(binding.data_type())
            .ok_or_else(|| target_value_contract(ValueViolation::Layout))?;
        let source = novarocks_value_arrays::access::ByteSource::parse(array, layout, None)
            .map_err(crate::runtime::task::ExecutionFailure::from_domain)?;
        return source
            .mapped_row(row)
            .map(|row| row.is_none_or(|row| source.values().is_null(row)))
            .map_err(crate::runtime::task::ExecutionFailure::from_domain);
    }
    Ok(array.is_null(row))
}

#[derive(Clone, Copy)]
enum RequirementPathStep<'a> {
    Field(&'a str),
    Item,
    Key,
    Value,
}
struct RequirementPath<'a> {
    root: &'a str,
    steps: &'a [RequirementPathStep<'a>],
}
impl std::fmt::Display for RequirementPath<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.root)?;
        for step in self.steps {
            match step {
                RequirementPathStep::Field(name) => write!(formatter, ".{name}")?,
                RequirementPathStep::Item => formatter.write_str(".item")?,
                RequirementPathStep::Key => formatter.write_str(".key")?,
                RequirementPathStep::Value => formatter.write_str(".value")?,
            }
        }
        Ok(())
    }
}

fn required_child(
    array: &ArrayRef,
    index: usize,
) -> crate::runtime::task::ExecutionResult<&ArrayRef> {
    use arrow::array::{
        FixedSizeListArray, LargeListArray, ListArray, MapArray, StructArray, UnionArray,
    };
    let invalid = || target_value_contract(ValueViolation::Structure);
    match array.data_type() {
        DataType::Struct(_) => array
            .as_any()
            .downcast_ref::<StructArray>()
            .and_then(|array| array.columns().get(index))
            .ok_or_else(invalid),
        DataType::List(_) => array
            .as_any()
            .downcast_ref::<ListArray>()
            .map(|array| array.values())
            .filter(|_| index == 0)
            .ok_or_else(invalid),
        DataType::LargeList(_) => array
            .as_any()
            .downcast_ref::<LargeListArray>()
            .map(|array| array.values())
            .filter(|_| index == 0)
            .ok_or_else(invalid),
        DataType::FixedSizeList(..) => array
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .map(|array| array.values())
            .filter(|_| index == 0)
            .ok_or_else(invalid),
        DataType::Map(..) => {
            let array = array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(invalid)?;
            match index {
                0 => Ok(array.keys()),
                1 => Ok(array.values()),
                _ => Err(invalid()),
            }
        }
        DataType::Union(fields, _) => {
            let id = fields.iter().nth(index).ok_or_else(invalid)?.0;
            Ok(array
                .as_any()
                .downcast_ref::<UnionArray>()
                .ok_or_else(invalid)?
                .child(id))
        }
        _ => Err(invalid()),
    }
}

fn validate_target_requiredness(
    array: &ArrayRef,
    target: &TargetValueDeclaration<'_>,
    growth: &mut impl FnMut(std::alloc::Layout) -> crate::runtime::task::ExecutionResult<()>,
    work: &mut impl FnMut(usize) -> crate::runtime::task::ExecutionResult<()>,
) -> crate::runtime::task::ExecutionResult<()> {
    if target
        .input_rows
        .is_some_and(|rows| rows.len() != array.len())
    {
        return Err(target_value_contract(ValueViolation::Bounds));
    }
    let nested = has_required_children(target.requirement.borrowed(), work)?;
    if !nested && target.requirement.value_type().nullable() {
        return Ok(());
    }
    if !nested && array.null_count() == 0 && !matches!(array.data_type(), DataType::Dictionary(..))
    {
        return Ok(());
    }
    let plan = nested
        .then(|| RequiredValuePlan::new(target.requirement.borrowed(), growth, work))
        .transpose()?;
    let callback = std::cell::RefCell::new(work);
    let observed_work = |units| (callback.borrow_mut())(units);
    let mut path = [RequirementPathStep::Item; novarocks_type_contract::MAX_VALUE_TYPE_DEPTH];
    for row in 0..array.len() {
        validate_target_row(
            array,
            target.requirement.borrowed(),
            row,
            row,
            target,
            plan.as_ref().map(|plan| (plan, 0)),
            &mut path,
            0,
            &observed_work,
        )?;
    }
    Ok(())
}

fn validate_target_row<'a>(
    array: &ArrayRef,
    binding: BorrowedTypedValueBinding<'a>,
    row: usize,
    root_row: usize,
    target: &TargetValueDeclaration<'_>,
    plan: Option<(&RequiredValuePlan, usize)>,
    path: &mut [RequirementPathStep<'a>],
    depth: usize,
    work: &impl Fn(usize) -> crate::runtime::task::ExecutionResult<()>,
) -> crate::runtime::task::ExecutionResult<()> {
    work(1)?;
    if logical_null(array, binding, row)? {
        if !binding.value_type().nullable() {
            return Err(crate::runtime::task::ExecutionFailure::from_domain(
                WriterRequiredValue {
                    column: target.column,
                    row: Some(target.input_rows.map_or(root_row, |rows| rows[root_row])),
                    field: novarocks_failure_contract::BoundedDetail::truncating_display(
                        RequirementPath {
                            root: target.name,
                            steps: &path[..depth],
                        },
                    ),
                },
            ));
        }
        return Ok(());
    }
    let Some((plan, node)) = plan else {
        return Ok(());
    };
    if plan.branches[node].children.is_empty() {
        return Ok(());
    }
    if depth >= path.len() {
        return Err(target_value_contract(ValueViolation::Depth));
    }
    novarocks_value_arrays::visit_reachable_child_rows_observed(
        array,
        [row],
        &mut |units| work(units),
        &mut |index, rows| {
            let child_binding = binding
                .child_at(index)
                .ok_or_else(|| target_value_contract(ValueViolation::Layout))?;
            let children = &plan.branches[node].children;
            let child_plan = *plan
                .children
                .get(children.start + index)
                .filter(|_| index < children.len())
                .ok_or_else(|| target_value_contract(ValueViolation::Bounds))?;
            if !plan.branches[child_plan].needed {
                return Ok(());
            }
            path[depth] = match binding.value_type().kind() {
                ValueKind::Struct(fields) => RequirementPathStep::Field(
                    fields
                        .get(index)
                        .ok_or_else(|| target_value_contract(ValueViolation::Layout))?
                        .name(),
                ),
                ValueKind::Map { .. } => {
                    if index == 0 {
                        RequirementPathStep::Key
                    } else {
                        RequirementPathStep::Value
                    }
                }
                ValueKind::Union(fields) => RequirementPathStep::Field(
                    fields
                        .get(index)
                        .ok_or_else(|| target_value_contract(ValueViolation::Layout))?
                        .field
                        .name(),
                ),
                _ => RequirementPathStep::Item,
            };
            let child = required_child(array, index)?;
            for child_row in rows {
                validate_target_row(
                    child,
                    child_binding,
                    child_row,
                    root_row,
                    target,
                    Some((plan, child_plan)),
                    path,
                    depth + 1,
                    work,
                )?;
            }
            Ok(())
        },
    )
    .map_err(|failure| match failure {
        ValueFailure::Caller(cause) => cause,
        ValueFailure::Value(cause) => crate::runtime::task::ExecutionFailure::from_domain(cause),
    })
}
#[derive(Debug)]
struct WriterRequiredValue {
    column: usize,
    row: Option<usize>,
    field: novarocks_failure_contract::BoundedDetail,
}
impl std::fmt::Display for WriterRequiredValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "build table writer projected batch: Invalid argument error: Column '{}' (column={}, row={:?}) is declared as non-nullable but contains null values",
            self.field, self.column, self.row
        )
    }
}
impl std::error::Error for WriterRequiredValue {}
impl novarocks_failure_contract::FailureProjection for WriterRequiredValue {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Data,
            crate::runtime::task::ExecutionReason::WriterRequiredValue.reason(),
        )
    }
}

#[cfg(test)]
mod required_value_source_properties {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    fn assignment_owner(
        required: &novarocks_type_contract::FunctionValueType,
    ) -> Box<dyn crate::exec::pipeline::operator::InputConstructionOwner> {
        use arrow::datatypes::{Field, Schema};
        let tracker =
            crate::runtime::mem_tracker::MemTracker::new_root("checked assignment materializer");
        tracker.install_limit_once(64 * 1024 * 1024).unwrap();
        let factory=crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),tracker,
            Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
        ).unwrap();
        let layout = crate::exec::chunk::typed_fixture::layout(
            Arc::new(Schema::new(vec![Field::new(
                "payload",
                required.data_type().clone(),
                required.nullable(),
            )])),
            &[novarocks_types::SlotId(1)],
            std::slice::from_ref(required),
        );
        let schema = factory.create_compiled_chunk_schema(&layout).unwrap();
        factory.begin_typed_construction(&schema).unwrap()
    }

    #[test]
    fn checked_assignment_preserves_fixed_bytes_and_rejects_only_unlicensed_conversion() {
        use arrow::array::{BinaryArray, FixedSizeBinaryArray};
        use novarocks_type_contract::FunctionValueType;
        let source = FunctionValueType::new(DataType::FixedSizeBinary(4), true);
        let input = FunctionValueType::new(DataType::Binary, true);
        let storage = FunctionValueType::new(DataType::FixedSizeBinary(8), true);
        let array: ArrayRef = Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                [Some([0, 255, 128, 1]), None].into_iter(),
                4,
            )
            .unwrap(),
        );
        let owner = assignment_owner(&input);
        let mut context = novarocks_value_arrays::decode::ValueConversionContext::default();
        let output = materialize_target_value(
            &array,
            source.typed_binding().borrowed(),
            TargetValueDeclaration {
                column: 0,
                name: "payload",
                requirement: input.typed_binding(),
                storage: Some(storage.typed_binding().borrowed()),
                output: input.typed_binding(),
                input_rows: None,
            },
            &mut context,
            owner.as_ref(),
            &mut |_| Ok(()),
        )
        .unwrap();
        let bytes = output.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(bytes.value(0), &[0, 255, 128, 1]);
        assert!(bytes.is_null(1));
        assert_eq!(output.data_type(), input.data_type());
        let failure = materialize_target_value(
            &array,
            source.typed_binding().borrowed(),
            TargetValueDeclaration {
                column: 0,
                name: "payload",
                requirement: input.typed_binding(),
                storage: None,
                output: input.typed_binding(),
                input_rows: None,
            },
            &mut context,
            owner.as_ref(),
            &mut |_| Ok(()),
        )
        .unwrap_err();
        assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
    }

    #[test]
    fn checked_assignment_uses_strict_w_before_global_nullable_event_output() {
        use arrow::array::{FixedSizeBinaryArray, StructArray};
        use arrow::buffer::NullBuffer;
        use arrow::datatypes::Field;
        use novarocks_type_contract::FunctionValueType;
        let source = FunctionValueType::new(
            DataType::Struct(vec![Field::new("f", DataType::FixedSizeBinary(4), true)].into()),
            true,
        );
        let input = FunctionValueType::new(
            DataType::Struct(vec![Field::new("f", DataType::Binary, false)].into()),
            true,
        );
        let storage = FunctionValueType::new(
            DataType::Struct(vec![Field::new("f", DataType::FixedSizeBinary(8), false)].into()),
            true,
        );
        let build = |valid| -> ArrayRef {
            Arc::new(
                StructArray::try_new(
                    vec![Field::new("f", DataType::FixedSizeBinary(4), true)].into(),
                    vec![Arc::new(
                        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                            [None::<[u8; 4]>].into_iter(),
                            4,
                        )
                        .unwrap(),
                    )],
                    Some(NullBuffer::from(vec![valid])),
                )
                .unwrap(),
            )
        };
        let owner = assignment_owner(&input);
        let mut context = novarocks_value_arrays::decode::ValueConversionContext::default();
        let failure = materialize_target_value(
            &build(true),
            source.typed_binding().borrowed(),
            TargetValueDeclaration {
                column: 0,
                name: "payload",
                requirement: input.typed_binding(),
                storage: Some(storage.typed_binding().borrowed()),
                output: input.typed_binding(),
                input_rows: Some(&[23]),
            },
            &mut context,
            owner.as_ref(),
            &mut |_| Ok(()),
        )
        .unwrap_err();
        assert_eq!(failure.failure_fact().class(), FailureClass::Data);
        assert_eq!(failure.failure_fact().reason().code(), 498);
        assert!(failure.detail().contains("payload.f"), "{failure}");
        assert!(failure.detail().contains("row=Some(23)"), "{failure}");
        let output = materialize_target_value(
            &build(false),
            source.typed_binding().borrowed(),
            TargetValueDeclaration {
                column: 0,
                name: "payload",
                requirement: input.typed_binding(),
                storage: Some(storage.typed_binding().borrowed()),
                output: input.typed_binding(),
                input_rows: Some(&[23]),
            },
            &mut context,
            owner.as_ref(),
            &mut |_| Ok(()),
        )
        .unwrap();
        let values = output.as_any().downcast_ref::<StructArray>().unwrap();
        assert!(values.is_null(0));
        assert_eq!(values.column(0).null_count(), 0);

        let source = FunctionValueType::new(DataType::FixedSizeBinary(4), true);
        let strict = FunctionValueType::new(DataType::Binary, false);
        let global = FunctionValueType::new(DataType::Binary, true);
        let storage = FunctionValueType::new(DataType::FixedSizeBinary(8), false);
        let null: ArrayRef = Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size([None::<[u8; 4]>].into_iter(), 4)
                .unwrap(),
        );
        let owner = assignment_owner(&global);
        let failure = materialize_target_value(
            &null,
            source.typed_binding().borrowed(),
            TargetValueDeclaration {
                column: 0,
                name: "payload",
                requirement: strict.typed_binding(),
                storage: Some(storage.typed_binding().borrowed()),
                output: global.typed_binding(),
                input_rows: Some(&[23]),
            },
            &mut context,
            owner.as_ref(),
            &mut |_| Ok(()),
        )
        .unwrap_err();
        assert_eq!(failure.failure_fact().class(), FailureClass::Data);
        assert_eq!(failure.failure_fact().reason().code(), 498);
    }

    #[test]
    fn actual_required_value_failure_keeps_its_column_and_row_without_input_owners() {
        use arrow::array::{ArrayRef, Int32Array};
        use arrow::datatypes::{Field, Schema};
        let field = Arc::new(Field::new("ResourceExhausted", DataType::Int32, false));
        let field_owner = Arc::downgrade(&field);
        let schema = Schema::new(vec![field.clone()]);
        let column: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None]));
        let column_owner = Arc::downgrade(&column);
        let required = novarocks_type_contract::FunctionValueType::new(DataType::Int32, false);
        let failure = validate_target_requiredness(
            &column,
            &TargetValueDeclaration {
                column: 0,
                name: field.name(),
                requirement: required.typed_binding(),
                storage: None,
                output: required.typed_binding(),
                input_rows: None,
            },
            &mut |_| Ok(()),
            &mut |_| Ok(()),
        )
        .unwrap_err();
        assert_eq!(failure.failure_fact().class(), FailureClass::Data);
        assert_eq!(failure.failure_fact().reason().code(), 498);
        let crate::runtime::task::ExecutionFailureCause::Domain(source) = failure.cause() else {
            panic!("writer value owner required")
        };
        let source = source.downcast_ref::<WriterRequiredValue>().unwrap();
        assert_eq!(source.column, 0);
        assert_eq!(source.row, Some(1));
        assert!(failure.detail().contains("Column 'ResourceExhausted'"));
        assert!(
            failure
                .detail()
                .contains("is declared as non-nullable but contains null values")
        );
        assert!(failure.detail().contains("column=0"));
        assert!(failure.detail().contains("row=Some(1)"));
        drop(column);
        drop(schema);
        drop(field);
        assert!(column_owner.upgrade().is_none());
        assert!(field_owner.upgrade().is_none());
    }

    fn requiredness(
        array: &ArrayRef,
        required: &novarocks_type_contract::FunctionValueType,
        input_rows: Option<&[usize]>,
    ) -> crate::runtime::task::ExecutionResult<()> {
        validate_target_requiredness(
            array,
            &TargetValueDeclaration {
                column: 2,
                name: "payload",
                requirement: required.typed_binding(),
                storage: None,
                output: required.typed_binding(),
                input_rows,
            },
            &mut |_| Ok(()),
            &mut |_| Ok(()),
        )
    }

    #[test]
    fn nested_required_null_is_data_with_the_first_root_row_and_typed_child_path() {
        use arrow::array::{Int64Array, StructArray};
        use arrow::datatypes::Field;
        let source: ArrayRef = Arc::new(StructArray::new(
            vec![Field::new("child", DataType::Int64, true)].into(),
            vec![Arc::new(Int64Array::from(vec![Some(7), None, None]))],
            None,
        ));
        let required = novarocks_type_contract::FunctionValueType::new(
            DataType::Struct(vec![Field::new("child", DataType::Int64, false)].into()),
            true,
        );
        let failure = requiredness(&source, &required, Some(&[3, 8, 12])).unwrap_err();
        assert_eq!(failure.failure_fact().class(), FailureClass::Data);
        assert_eq!(failure.failure_fact().reason().code(), 498);
        let crate::runtime::task::ExecutionFailureCause::Domain(cause) = failure.cause() else {
            panic!("original required-value cause");
        };
        let cause = cause.downcast_ref::<WriterRequiredValue>().unwrap();
        assert_eq!(cause.row, Some(8));
        assert_eq!(cause.column, 2);
        assert_eq!(cause.field.as_str(), "payload.child");
    }

    #[test]
    fn a_null_parent_masks_its_required_child_value() {
        use arrow::array::{Int64Array, StructArray};
        use arrow::buffer::NullBuffer;
        use arrow::datatypes::Field;
        let source: ArrayRef = Arc::new(StructArray::new(
            vec![Field::new("child", DataType::Int64, true)].into(),
            vec![Arc::new(Int64Array::from(vec![None::<i64>]))],
            Some(NullBuffer::from(vec![false])),
        ));
        let required = novarocks_type_contract::FunctionValueType::new(
            DataType::Struct(vec![Field::new("child", DataType::Int64, false)].into()),
            true,
        );
        requiredness(&source, &required, None).unwrap();
    }

    #[test]
    fn sliced_list_requiredness_ignores_unreferenced_null_elements() {
        use arrow::array::{Int64Array, ListArray};
        use arrow::buffer::{OffsetBuffer, ScalarBuffer};
        use arrow::datatypes::Field;
        let source: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int64, true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 2, 3])),
            Arc::new(Int64Array::from(vec![None, Some(7), None])),
            None,
        ));
        let required = novarocks_type_contract::FunctionValueType::new(
            DataType::List(Arc::new(Field::new("item", DataType::Int64, false))),
            true,
        );
        requiredness(&source.slice(1, 1), &required, None).unwrap();
        let failure = requiredness(&source, &required, None).unwrap_err();
        assert_eq!(failure.failure_fact().class(), FailureClass::Data);
        assert!(failure.detail().contains("payload.item"));
        assert!(failure.detail().contains("row=Some(0)"));
    }

    #[test]
    fn sliced_map_requiredness_ignores_unreferenced_null_values() {
        use arrow::array::{Int64Array, MapArray, StructArray};
        use arrow::buffer::{OffsetBuffer, ScalarBuffer};
        use arrow::datatypes::Field;
        let entries = StructArray::new(
            vec![
                Field::new("key", DataType::Int64, false),
                Field::new("value", DataType::Int64, true),
            ]
            .into(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![None, Some(7), None])),
            ],
            None,
        );
        let source: ArrayRef = Arc::new(MapArray::new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 2, 3])),
            entries,
            None,
            false,
        ));
        let required = novarocks_type_contract::FunctionValueType::new(
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Int64, false),
                            Field::new("value", DataType::Int64, false),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        );
        requiredness(&source.slice(1, 1), &required, None).unwrap();
        let failure = requiredness(&source, &required, None).unwrap_err();
        assert_eq!(failure.failure_fact().class(), FailureClass::Data);
        assert!(failure.detail().contains("payload.value"));
        assert!(failure.detail().contains("row=Some(0)"));
    }
}

/// Provider and attempt capabilities moved out of the transient decoder node.
/// A pure local program never owns any of these values.
pub struct TableWriterRuntimeBinding {
    pub(crate) resources: Option<ConnectorExecutionResources>,
    pub(crate) position_shards: Option<Arc<PositionShardWriterSources>>,
    pub(crate) handle: ConnectorWriterHandle,
    pub(crate) execution: Arc<dyn ConnectorWriteExecution>,
    pub(crate) physical_template: TableWriterPhysicalContextTemplate,
    pub(crate) request_context: ConnectorRequestContext,
    pub(crate) fragment_encoder: Arc<dyn ConnectorCommitFragmentEncoder>,
    #[cfg(novarocks_dev_controls)]
    pub(crate) aggregate_guard: Arc<dyn TableWriteAggregateFaultHook>,
}

impl TableWriterRuntimeBinding {
    /// One Task's write capability for one compiled writer: the decoded
    /// handle, the query-leased execution of the handle's own catalog, the
    /// attempt-local physical template, the request context and the
    /// commit-fragment encoder.
    pub fn try_new(
        handle: ConnectorWriterHandle,
        execution: Arc<dyn ConnectorWriteExecution>,
        physical_template: TableWriterPhysicalContextTemplate,
        request_context: ConnectorRequestContext,
        fragment_encoder: Arc<dyn ConnectorCommitFragmentEncoder>,
    ) -> Result<Self, ExecPlanBuildError> {
        if execution.catalog_handle() != handle.binding().catalog_handle() {
            return Err(ExecPlanBuildError::new(
                ExecPlanInvariant::Node,
                "table writer catalog handle does not match its query-leased write execution",
            ));
        }
        Ok(Self {
            resources: None,
            position_shards: None,
            handle,
            execution,
            physical_template,
            request_context,
            fragment_encoder,
            #[cfg(novarocks_dev_controls)]
            aggregate_guard: Arc::new(AllowTableWriteAggregates),
        })
    }

    pub fn with_position_shard_sources(
        mut self,
        group: ConnectorMutationContributionGroupId,
        receiver_node: i32,
        mut sources: Vec<(ExchangeSource, NonZeroU32)>,
    ) -> Self {
        sources.sort_unstable_by_key(|(source, _)| source.sender_ordinal());
        self.position_shards = Some(Arc::new(PositionShardWriterSources {
            group,
            receiver_node,
            sources,
        }));
        self
    }

    /// Bind the application-owned, query-scoped aggregate rejection guard.
    #[cfg(novarocks_dev_controls)]
    pub fn with_aggregate_guard(mut self, guard: Arc<dyn TableWriteAggregateFaultHook>) -> Self {
        self.aggregate_guard = guard;
        self
    }

    pub const fn handle(&self) -> &ConnectorWriterHandle {
        &self.handle
    }

    pub const fn physical_template(&self) -> TableWriterPhysicalContextTemplate {
        self.physical_template
    }
}

#[derive(Debug)]
pub(crate) struct PositionShardWriterSources {
    group: ConnectorMutationContributionGroupId,
    receiver_node: i32,
    sources: Vec<(ExchangeSource, NonZeroU32)>,
}

/// The receiver mints this sideband after matching the exact authenticated
/// source. Cloning a chunk retains the original roster without allocation.
#[derive(Clone, Debug)]
pub(crate) struct PositionShardChunkSource {
    roster: Arc<PositionShardWriterSources>,
    sender_ordinal: usize,
}

impl PositionShardWriterSources {
    pub(crate) fn matches_receiver(
        &self,
        receiver_node: i32,
        group: ConnectorMutationContributionGroupId,
        expected_senders: usize,
    ) -> bool {
        self.receiver_node == receiver_node
            && self.group == group
            && self.sources.len() == expected_senders
    }

    pub(crate) fn authenticate(
        self: &Arc<Self>,
        source: ExchangeSource,
    ) -> crate::runtime::task::ExecutionResult<PositionShardChunkSource> {
        let sender_ordinal = source.sender_ordinal() as usize;
        if !self
            .sources
            .get(sender_ordinal)
            .is_some_and(|(expected, _)| *expected == source)
        {
            return Err(Self::invalid_source());
        }
        Ok(PositionShardChunkSource {
            roster: Arc::clone(self),
            sender_ordinal,
        })
    }

    pub(crate) fn bind(
        self: &Arc<Self>,
        source: Option<&PositionShardChunkSource>,
    ) -> crate::runtime::task::ExecutionResult<ConnectorPositionShardSource> {
        let &(source, width) = source
            .filter(|source| Arc::ptr_eq(&source.roster, self))
            .and_then(|source| self.sources.get(source.sender_ordinal))
            .ok_or_else(Self::invalid_source)?;
        let key = source.task_key();
        let mut bytes = [0; 16];
        bytes[..8].copy_from_slice(&key.high().to_be_bytes());
        bytes[8..].copy_from_slice(&key.low().to_be_bytes());
        Ok(ConnectorPositionShardSource::new(self.group, bytes, width))
    }

    fn invalid_source() -> crate::runtime::task::ExecutionFailure {
        crate::exec::fragment::error::program_binding_failure(
            crate::exec::fragment::error::FragmentBindingErrorKind::InvalidAssignment,
            "Position shard author is absent from the admitted exchange roster",
        )
    }
}

#[cfg(test)]
mod position_shard_source_tests {
    use super::*;
    use crate::exec::chunk::Chunk;
    use novarocks_execution_contract::identity::TaskIdentity;
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId, UniqueId,
    };

    #[test]
    fn compact_source_retains_exact_roster_through_chunk_slice() {
        assert_eq!(
            std::mem::size_of::<Option<PositionShardChunkSource>>(),
            2 * std::mem::size_of::<usize>()
        );
        let task = |process| {
            TaskIdentity::new(
                QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap(),
                StageId::new(1).unwrap(),
                TaskId::new(1).unwrap(),
                process,
            )
        };
        let key = UniqueId::new(3, 4);
        let source = ExchangeSource::new(task(BackendProcessId::new_v7()), key, 0);
        let other_process = ExchangeSource::new(task(BackendProcessId::new_v7()), key, 0);
        let group = ConnectorMutationContributionGroupId::from_bytes([1; 32]);
        let width = NonZeroU32::new(4).unwrap();
        let roster = || {
            Arc::new(PositionShardWriterSources {
                group,
                receiver_node: 7,
                sources: vec![(source, width)],
            })
        };
        let original = roster();
        assert!(original.matches_receiver(7, group, 1));
        assert!(!original.matches_receiver(8, group, 1));
        assert!(original.authenticate(other_process).is_err());
        assert!(original.bind(None).is_err());

        let mut chunk = Chunk::default();
        chunk.set_position_shard_source(original.authenticate(source).unwrap());
        let slice = chunk.slice(0, 0);
        drop(chunk);
        assert!(roster().bind(slice.position_shard_source()).is_err());
        let admitted = original.bind(slice.position_shard_source()).unwrap();
        assert_eq!(admitted.group(), group);
        assert_eq!(admitted.driver_count(), width);
        assert_eq!(admitted.task_key()[..8], key.high().to_be_bytes());
        assert_eq!(admitted.task_key()[8..], key.low().to_be_bytes());
    }
}

#[cfg(test)]
mod runtime_projection_tests {

    use crate::exec::chunk::Chunk;
    use crate::exec::pipeline::operator::ConstructionFailure;
    use crate::runtime::task::ExecutionFailure;
    use crate::runtime::{mem_tracker::MemTracker, runtime_state::RuntimeErrorState};
    use arrow::array::{Array, ArrayRef, BooleanArray, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
    use std::sync::Arc;

    #[test]
    fn typed_writer_projection_keeps_nominal_input_through_freeze_and_last_output_exit() {
        let tracker = MemTracker::new_root("typed compiled writer projection output");
        let errors = Arc::new(RuntimeErrorState::default());
        let factory=crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),tracker.clone(),errors.clone(),
        ).unwrap();
        let ty = FunctionValueType::try_with_logical_type(
            DataType::LargeBinary,
            false,
            ValueLogicalType::Json,
        )
        .unwrap();
        let provider_schema = Arc::new(Schema::new(vec![Field::new(
            "document",
            DataType::Utf8,
            false,
        )]));
        let (frozen, projection, input_layout) =
            crate::exec::operators::table_writer::tests::compiled_projection(
                ty.clone(),
                provider_schema,
                errors.clone(),
            );
        let mut runtime = projection.create();
        runtime.set_mem_tracker(tracker.clone());
        runtime.prepare(Some(&factory)).unwrap();
        let source_schema = factory.create_compiled_chunk_schema(&input_layout).unwrap();
        assert!(source_schema.is_typed());
        assert_eq!(source_schema.slots()[0].typed_binding(), ty.typed_binding());
        let owner = factory.begin_typed_construction(&source_schema).unwrap();
        owner
            .request(std::alloc::Layout::array::<ArrayRef>(1).unwrap())
            .unwrap();
        let input = Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
            source_schema.clone(),
            vec![Arc::new(StringArray::from(vec!["{}", "[]"])) as ArrayRef],
            2,
            owner.storage_lifetime(),
            |step| owner.observe(0, &[], step),
        )
        .unwrap();
        drop(owner);
        let output = runtime.project(&input).unwrap();
        assert_eq!(
            output.chunk_schema().slots()[0].typed_binding(),
            ty.typed_binding()
        );
        assert!(
            output.chunk_schema().slots()[0]
                .field()
                .metadata()
                .as_arc()
                .is_none()
        );
        assert_eq!(
            output.columns()[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(1),
            "[]"
        );
        let weak = Arc::downgrade(&factory);
        let output = output.into_batch_projection();
        drop((
            runtime,
            projection,
            frozen,
            input_layout,
            source_schema,
            input,
            factory,
            errors,
            ty,
        ));
        assert!(weak.upgrade().is_none());
        assert!(tracker.current() > 0);
        assert_eq!(output.record_batch().num_rows(), 2);
        drop(output);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn static_writer_projection_binds_its_exact_fragment_stop_owner() {
        // A real compiled slot root observes RuntimeKernelControl/Frame entry
        // checkpoints. The retired SLEEP(0) arena was only a cancellation
        // stimulus; no new pure SLEEP owner is claimed here.
        let first = Arc::new(RuntimeErrorState::default());
        let sibling = Arc::new(RuntimeErrorState::default());
        let ty = FunctionValueType::new(DataType::Boolean, false);
        let provider_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Boolean,
            false,
        )]));
        let (_frozen, projection, input_layout) =
            crate::exec::operators::table_writer::tests::compiled_projection(
                ty.clone(),
                provider_schema.clone(),
                first.clone(),
            );
        let (_sibling_frozen, sibling_projection, _) =
            crate::exec::operators::table_writer::tests::compiled_projection(
                ty.clone(),
                provider_schema,
                sibling,
            );
        let tracker = MemTracker::new_root("writer projection runtime fixture");
        let factory=crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),tracker.clone(),Arc::new(RuntimeErrorState::default()),
        ).unwrap();
        let mut projection = projection.create();
        let mut sibling_projection = sibling_projection.create();
        projection.set_mem_tracker(tracker.clone());
        sibling_projection.set_mem_tracker(tracker);
        projection.prepare(Some(&factory)).unwrap();
        sibling_projection.prepare(Some(&factory)).unwrap();
        let batch = arrow::record_batch::RecordBatch::try_new(
            input_layout.schema().clone(),
            vec![Arc::new(BooleanArray::from(vec![true])) as ArrayRef],
        )
        .unwrap();
        let chunk = crate::exec::chunk::typed_fixture::chunk(batch, input_layout.slots(), &[ty]);
        assert!(projection.project(&chunk).is_ok());
        let cancelled = ExecutionFailure::from(novarocks_functions::KernelFailure::Cancelled);
        first.set_error(cancelled.clone());
        let actual = projection.project(&chunk).unwrap_err();
        assert_eq!(actual, cancelled);
        assert!(actual.same_occurrence(&cancelled));
        assert!(sibling_projection.project(&chunk).is_ok());
        // RuntimeBoundArena refusal to re-freeze protected only the removed
        // arena wrapper. It is not a compiled-program contract or this test's
        // fragment-stop/source property.
    }
}
