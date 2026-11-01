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

//! Selected fixed columns share the existing construction-aware Arrow copy
//! author. This owns key values, not join rows or aggregate state.

use super::{KeySpec, PreparedKeyBatch, checkpoint_at, invariant, vector};
use crate::{
    exec::pipeline::operator::{ConstructionFailure, InputConstructionOwner},
    runtime::fragment::{ExecutionFailure, ExecutionResult},
};
use arrow::array::{Array, ArrayRef};
use novarocks_functions::{
    AggregateStateAllocator, KernelEvaluationControl,
    selected_copy::{CopyConstructionError, copy_nested_values_construction},
};
use novarocks_type_contract::{FunctionValueType, ValueSchemaCreation, ValueTypeStorageLifetime};
use novarocks_value_arrays::ValueViolation;
use std::sync::Arc;

pub(super) struct OwnedFixedColumn {
    array: ArrayRef,
    binding: FunctionValueType,
    spec: KeySpec,
    column: usize,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Retains the real fixed-column creator; preserve field order and actual alias lifetime."
        )
    )]
    storage: ValueTypeStorageLifetime,
}
impl OwnedFixedColumn {
    pub(super) fn from_compacted(
        array: ArrayRef,
        binding: FunctionValueType,
        construction: &super::compact::Construction<'_>,
    ) -> ExecutionResult<Self> {
        let checkpoint = || {
            construction
                .control
                .checkpoint(0)
                .map_err(ExecutionFailure::from)
        };
        checkpoint()?;
        super::owned_batch::validate_compacted_binding(
            construction.spec,
            construction.column,
            &binding,
            &checkpoint,
        )?;
        if !super::prepared::is_fixed(binding.data_type())
            || binding.data_type() != array.data_type()
        {
            return Err(invariant(ValueViolation::Carrier));
        }
        if !binding.nullable() && array.logical_null_count() != 0 {
            return Err(invariant(ValueViolation::RequiredNull));
        }
        let storage = construction.owner.storage_lifetime();
        let array = super::store::retain_created_array(
            array,
            &binding,
            &storage,
            Arc::clone(&construction.host),
            construction.control,
        )?;
        checkpoint()?;
        Ok(Self {
            array,
            binding,
            spec: construction.spec.clone(),
            column: construction.column,
            storage,
        })
    }
    pub(super) fn copy_selected(
        input: &PreparedKeyBatch<'_>,
        column: usize,
        rows: &[usize],
        owner: &dyn InputConstructionOwner,
        host: Arc<dyn AggregateStateAllocator>,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<Self> {
        let checkpoint = || control.checkpoint(0).map_err(ExecutionFailure::from);
        checkpoint()?;
        let signed = input
            .spec
            .types()
            .get(column)
            .ok_or_else(|| invariant(ValueViolation::Bounds))?;
        if !super::prepared::is_fixed(signed.data_type()) {
            return Err(invariant(ValueViolation::Carrier));
        }
        let (source, mapping) = input.tuples().physical_component(column)?;
        let mut choices = vector(owner, rows.len())?;
        for (i, &row) in rows.iter().enumerate() {
            checkpoint_at(i, &checkpoint)?;
            if !input.input.selected().contains(row) {
                return Err(invariant(ValueViolation::UndefinedRow));
            }
            choices.push((0, super::pairs::physical_row(mapping, row)?));
        }
        let mut sources = vector(owner, 1)?;
        sources.push(Arc::clone(source));
        let storage = owner.storage_lifetime();
        let mut request = |layout| owner.request(layout);
        let mut opaque = |bytes| owner.observe_opaque_upper_bound(bytes);
        let mut creation = ValueSchemaCreation::new(storage.clone(), &mut request, &mut opaque);
        // The original public author handles top-level FixedSizeBinary with
        // controlled byte blocks; other original Arrow/container operations
        // retain their documented indivisible implementation boundaries.
        let array = copy_nested_values_construction(&sources, &choices, &mut creation, control)
            .map_err(|cause| match cause {
                CopyConstructionError::Creator(cause) => ExecutionFailure::from(cause),
                CopyConstructionError::Resource(cause) => {
                    ConstructionFailure::Resource(cause).into()
                }
                CopyConstructionError::Copy(cause) => cause.into_kernel().into(),
            })?;
        let binding = input.input.bindings()[column].clone_observed(checkpoint)?;
        let array = super::store::retain_created_array(array, &binding, &storage, host, control)?;
        checkpoint()?;
        Ok(Self {
            array,
            binding,
            spec: input.spec.clone(),
            column,
            storage,
        })
    }
    pub(super) fn len(&self) -> usize {
        self.array.len()
    }
    pub(super) fn array_ref(&self) -> &ArrayRef {
        &self.array
    }
    #[cfg(test)]
    pub(super) fn array(&self) -> &dyn Array {
        self.array.as_ref()
    }
    pub(super) fn binding(&self) -> &FunctionValueType {
        &self.binding
    }
    #[cfg(test)]
    pub(super) fn storage(&self) -> &ValueTypeStorageLifetime {
        &self.storage
    }
    pub(super) fn compare_pairs(
        &self,
        input: &PreparedKeyBatch<'_>,
        column: usize,
        input_rows: &[usize],
        page_rows: &[usize],
        output: &mut [u8],
        checkpoint: &dyn Fn() -> ExecutionResult<()>,
    ) -> ExecutionResult<()> {
        checkpoint()?;
        if column != self.column
            || !self.spec.same_instance(&input.spec)
            || input_rows.len() != page_rows.len()
            || output.len() != input_rows.len()
        {
            return Err(invariant(ValueViolation::Layout));
        }
        for (i, ((&input_row, &stored_row), out)) in input_rows
            .iter()
            .zip(page_rows)
            .zip(output.iter_mut())
            .enumerate()
        {
            checkpoint_at(i, checkpoint)?;
            if !input.input.selected().contains(input_row) || stored_row >= self.len() {
                return Err(invariant(ValueViolation::UndefinedRow));
            }
            *out = 1;
        }
        let (source, mapping) = input.tuples().physical_component(column)?;
        super::pairs::compare_fixed_pairs(
            source,
            mapping,
            input_rows,
            &self.array,
            None,
            page_rows,
            self.spec.nulls()[column],
            output,
            checkpoint,
        )?;
        checkpoint()
    }
}
