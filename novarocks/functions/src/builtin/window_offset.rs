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

//! LEAD/LAG selected values from one complete original partition.
//! Offsets come from the exact BIGINT argument or the default of one.

use crate::control_values::{
    AssemblyPlan, assemble_indexed_values_in, assemble_values, normalize_indexed_value_in,
    visit_indexed_value_offsets,
};
use crate::kernel_control::{
    KernelControlObservation, internal, invalid, invalid_function_argument,
};
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::{OpaqueReservation, OpaqueRetainedCharge};
use crate::{
    AggregateStateAllocator, EvaluatedArgument, KernelEvaluationControl, KernelFailure,
    PreparedWindowKernel, SelectedValues, Selection, WindowCallContract, WindowEvaluationFailure,
    WindowInvocationContext, WindowKernelPartition, WindowPartitionInput, WindowPartitionRetention,
};
use arrow_array::{Array, ArrayRef, Int64Array, UInt64Array};
use std::{alloc::Layout, ops::Range, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum OffsetOperation {
    Lead,
    Lag,
}

#[derive(Debug)]
pub(super) struct PreparedOffset {
    pub(super) contract: Arc<WindowCallContract>,
    pub(super) operation: OffsetOperation,
    pub(super) offset: Option<i64>,
}

struct OffsetDefault {
    array: ArrayRef,
    broadcast_row: Option<usize>,
}

impl OffsetDefault {
    fn new(argument: EvaluatedArgument<'_>) -> Self {
        let broadcast_row = match argument {
            EvaluatedArgument::Column(_) | EvaluatedArgument::SelectedColumn(_) => None,
            EvaluatedArgument::Scalar(_)
            | EvaluatedArgument::Constant(_)
            | EvaluatedArgument::ConstantView(_) => Some(argument.value_row(0, 0)),
        };
        Self {
            array: Arc::clone(argument.array()),
            broadcast_row,
        }
    }

    fn value_row(&self, row: usize) -> usize {
        self.broadcast_row.unwrap_or(row)
    }
}

struct OffsetPartition {
    prepared: Arc<PreparedOffset>,
    source: ArrayRef,
    default: Option<OffsetDefault>,
    addresses: Box<[Option<u64>]>,
    allocator: Option<Arc<dyn AggregateStateAllocator>>,
    _address_reservation: Option<OpaqueReservation>,
    retained: usize,
    closed: bool,
}

fn reserve<T>(rows: usize, work: &mut EvaluationCheckpoints<'_>) -> Result<Vec<T>, KernelFailure> {
    Layout::array::<T>(rows).map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(rows)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    Ok(values)
}

fn copy_plan(
    input: &ArrayRef,
    indices: &[Option<u64>],
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    // These are actual delegated take-plan scratch and UInt64 values/bitmap
    // representations. They do not authorize the Arrow copy's host MEM scope.
    Layout::array::<Range<usize>>(indices.len()).map_err(|_| KernelFailure::ResourceExhausted)?;
    Layout::array::<u64>(indices.len()).map_err(|_| KernelFailure::ResourceExhausted)?;
    let bitmap = indices
        .len()
        .checked_add(7)
        .ok_or(KernelFailure::ResourceExhausted)?
        / 8;
    Layout::array::<u8>(bitmap).map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    match crate::selected_copy::preflight_take(input.as_ref(), indices, |opaque| {
        if opaque { work.flush() } else { work.step() }
    }) {
        Ok(()) => work.flush(),
        Err(source) => Err(source.into_kernel()),
    }
}

impl PreparedWindowKernel for PreparedOffset {
    fn contract(&self) -> &Arc<WindowCallContract> {
        &self.contract
    }
    fn partition_retained_upper_bound(&self, rows: usize) -> Result<usize, KernelFailure> {
        let heap = Layout::array::<Option<u64>>(rows)
            .map_err(|_| KernelFailure::ResourceExhausted)?
            .size();
        size_of::<OffsetPartition>()
            .checked_add(heap)
            .ok_or(KernelFailure::ResourceExhausted)
    }
    fn partition_retention(&self, rows: usize) -> Result<WindowPartitionRetention, KernelFailure> {
        if self.supports_owned_partition() {
            Ok(WindowPartitionRetention::AllocationTracked)
        } else {
            self.partition_retained_upper_bound(rows)
                .map(|bytes| WindowPartitionRetention::RowBounded { bytes })
        }
    }
    fn supports_owned_partition(&self) -> bool {
        self.contract.logical_argument_types().len() == 3
    }
    fn begin_owned_partition_evaluated(
        self: Arc<Self>,
        input: WindowPartitionInput<'_>,
        _context: WindowInvocationContext,
        allocator: Arc<dyn AggregateStateAllocator>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn WindowKernelPartition>, WindowEvaluationFailure> {
        Ok(self.begin_offset_partition(input, Some(allocator), control)?)
    }
    fn begin_partition_evaluated<'input>(
        self: Arc<Self>,
        input: WindowPartitionInput<'input>,
        _context: WindowInvocationContext,
        allocator: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn WindowKernelPartition + 'input>, WindowEvaluationFailure> {
        let allocator = if self.supports_owned_partition() {
            Some(allocator.ok_or_else(|| {
                invalid("three-argument LEAD/LAG requires its original host allocator")
            })?)
        } else {
            None
        };
        Ok(self.begin_offset_partition(input, allocator, control)?)
    }
    fn begin_partition<'a>(
        self: Arc<Self>,
        input: WindowPartitionInput<'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn WindowKernelPartition + 'a>, KernelFailure> {
        Ok(self.begin_offset_partition(input, None, control)?)
    }
}

impl PreparedOffset {
    fn begin_offset_partition(
        self: Arc<Self>,
        input: WindowPartitionInput<'_>,
        allocator: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn WindowKernelPartition>, KernelFailure> {
        let three_arguments = self.supports_owned_partition();
        let observation = KernelControlObservation::new(control);
        let control: &dyn KernelEvaluationControl = &observation;
        control.checkpoint(0)?;
        let mut work = EvaluationCheckpoints::new(control);
        let result = (|| {
            let same = std::ptr::eq(input.full_input().contract(), self.contract.as_ref());
            work.step()?;
            if !same {
                return Err(invalid(
                    "window offset partition differs from its original prepared contract",
                ));
            }
            let rows = input.full_input().partition_rows();
            let retained = self.partition_retained_upper_bound(rows)?;
            let charge = allocator
                .as_ref()
                .map(|host| OpaqueRetainedCharge::try_new(Arc::clone(host)))
                .transpose()?;
            let address_reservation = charge
                .as_ref()
                .map(|charge| charge.reserve_operation(retained))
                .transpose()?;
            // Full input validation already evaluated and checked both value
            // channels. Only their row coordinates differ during selection.
            let arguments = input.full_input().logical_arguments();
            let source = arguments[0];
            let mut default = arguments.get(2).copied().map(OffsetDefault::new);
            let mut addresses = reserve::<Option<u64>>(rows, &mut work)?;
            let ignore = self.contract.options().ignore_nulls();
            let ignore_scratch = if ignore {
                let slots = rows
                    .checked_mul(2)
                    .and_then(|count| count.checked_add(1))
                    .ok_or(KernelFailure::ResourceExhausted)?;
                let bytes = Layout::array::<usize>(slots)
                    .map_err(|_| KernelFailure::ResourceExhausted)?
                    .size();
                charge
                    .as_ref()
                    .map(|charge| charge.reserve_operation(bytes))
                    .transpose()?
            } else {
                None
            };
            let mut positions = if ignore {
                reserve::<usize>(rows, &mut work)?
            } else {
                Vec::new()
            };
            let mut prefix = if ignore {
                let count = rows
                    .checked_add(1)
                    .ok_or(KernelFailure::ResourceExhausted)?;
                reserve::<usize>(count, &mut work)?
            } else {
                Vec::new()
            };
            if ignore {
                prefix.push(0);
                work.step()?;
                for row in 0..rows {
                    let address = source.value_row(row, row);
                    // Encoded values use the same logical NULL rule as plain values.
                    if !crate::kernel_input::logical_is_null(
                        source.array().as_ref(),
                        address,
                        1,
                        &mut work,
                    )? {
                        positions.push(row);
                    }
                    prefix.push(positions.len());
                    work.step()?;
                }
            }
            let dynamic_offset = if self.offset.is_none() {
                let argument = input.full_input().logical_arguments()[1];
                let array = argument
                    .array()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        invalid("LEAD/LAG offset requires its selected BIGINT carrier")
                    })?;
                Some((argument, array))
            } else {
                None
            };
            for row in 0..rows {
                let offset = if let Some((argument, array)) = dynamic_offset {
                    let address = argument.value_row(row, row);
                    if array.is_null(address) {
                        return Err(invalid_function_argument(
                            "LEAD/LAG offset must not be NULL",
                        ));
                    }
                    let offset = array.value(address);
                    if offset < 0 {
                        return Err(invalid_function_argument(
                            "LEAD/LAG offset must be non-negative",
                        ));
                    }
                    offset
                } else {
                    self.offset.expect("prepared default or constant offset")
                };
                let offset = usize::try_from(offset).ok();
                let target = if ignore {
                    if offset == Some(0) {
                        Some(row)
                    } else {
                        offset.and_then(|offset| match self.operation {
                            OffsetOperation::Lag => prefix[row]
                                .checked_sub(offset)
                                .and_then(|rank| positions.get(rank).copied()),
                            OffsetOperation::Lead => prefix[row + 1]
                                .checked_add(offset - 1)
                                .and_then(|rank| positions.get(rank).copied()),
                        })
                    }
                } else {
                    offset.and_then(|offset| match self.operation {
                        OffsetOperation::Lag => row.checked_sub(offset),
                        OffsetOperation::Lead => {
                            row.checked_add(offset).filter(|target| *target < rows)
                        }
                    })
                };
                let address = target
                    .map(|row| u64::try_from(source.value_row(row, row)))
                    .transpose()
                    .map_err(|_| KernelFailure::ResourceExhausted)?;
                addresses.push(address);
                work.step()?;
            }
            if default.is_none() {
                // Preserve the existing complete take preflight.
                copy_plan(source.array(), &addresses, &mut work)?;
            }
            drop(positions);
            drop(prefix);
            drop(ignore_scratch);
            work.flush()?;
            let addresses = addresses.into_boxed_slice();
            work.flush()?;
            let source = if let (Some(default), Some(allocator)) = (&mut default, &allocator) {
                let result = self.contract.result_type().data_type();
                let normalized = normalize_indexed_value_in(
                    source.array(),
                    result,
                    Arc::clone(allocator),
                    control,
                )?;
                default.array = normalize_indexed_value_in(
                    &default.array,
                    result,
                    Arc::clone(allocator),
                    control,
                )?;
                normalized
            } else {
                Arc::clone(source.array())
            };
            let partition = Box::new(OffsetPartition {
                prepared: self,
                source,
                default,
                addresses,
                allocator,
                _address_reservation: address_reservation,
                retained,
                closed: false,
            }) as Box<dyn WindowKernelPartition>;
            work.flush()?;
            Ok(partition)
        })();
        if three_arguments {
            observation.finish(work.finish_result(result))
        } else {
            if result.as_ref().is_err_and(KernelFailure::is_interruption) {
                return result;
            }
            work.finish()?;
            observation.finish(result)
        }
    }
}

impl OffsetPartition {
    fn result_source(
        &self,
        row: usize,
    ) -> Result<Option<(usize, &ArrayRef, usize)>, KernelFailure> {
        let address = self
            .addresses
            .get(row)
            .ok_or_else(|| invalid("window offset result row differs from its partition"))?;
        if let Some(index) = address {
            let index = usize::try_from(*index).map_err(|_| KernelFailure::ResourceExhausted)?;
            return Ok(Some((0, &self.source, index)));
        }
        Ok(self
            .default
            .as_ref()
            .map(|default| (1, &default.array, default.value_row(row))))
    }
}

impl WindowKernelPartition for OffsetPartition {
    fn visit_output_row_offsets(
        &self,
        row: usize,
        visit: &mut dyn FnMut(usize, usize) -> Result<(), KernelFailure>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        if let Some((_, source, index)) = self.result_source(row)? {
            visit_indexed_value_offsets(source.as_ref(), index, 0, visit, control)?;
        }
        Ok(())
    }
    fn evaluate<'s>(
        &mut self,
        selection: Selection<'s>,
        row_capacity: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'s>, KernelFailure> {
        if self.closed {
            return Err(KernelFailure::InstanceFailed);
        }
        let observation = KernelControlObservation::new(control);
        let control: &dyn KernelEvaluationControl = &observation;
        let result = (|| {
            control.checkpoint(0)?;
            let mut work = EvaluationCheckpoints::new(control);
            let result = (|| {
                let shape = selection.batch_rows() == self.addresses.len()
                    && selection.len() <= row_capacity;
                work.step()?;
                if !shape {
                    return Err(invalid(
                        "window offset output differs from its partition or host row grant",
                    ));
                }
                let result_type = self.prepared.contract.result_type().data_type();
                let copied = if let Some(default) = &self.default {
                    let choice_bytes = Layout::array::<Option<(usize, usize)>>(selection.len())
                        .map_err(|_| KernelFailure::ResourceExhausted)?
                        .size();
                    let _choice_reservation = self
                        .allocator
                        .as_ref()
                        .map(|host| {
                            OpaqueRetainedCharge::try_new(Arc::clone(host))?
                                .reserve_operation(choice_bytes)
                        })
                        .transpose()?;
                    let mut choices =
                        reserve::<Option<(usize, usize)>>(selection.len(), &mut work)?;
                    for row in selection.iter() {
                        choices.push(
                            self.result_source(row)?
                                .map(|(source, _, index)| (source, index)),
                        );
                        work.step()?;
                    }
                    work.flush()?;
                    let sources = [Some(&self.source), Some(&default.array)];
                    let copied = if let Some(allocator) = &self.allocator {
                        assemble_indexed_values_in(
                            result_type,
                            &sources,
                            &choices,
                            Arc::clone(allocator),
                            control,
                        )?
                    } else {
                        assemble_values(
                            AssemblyPlan::Indexed {
                                result_type,
                                sources: &sources,
                                choices: &choices,
                            },
                            control,
                        )
                        .map_err(|error| error.into_kernel())?
                    };
                    work.flush()?;
                    copied
                } else {
                    let mut indices = reserve::<Option<u64>>(selection.len(), &mut work)?;
                    for row in selection.iter() {
                        indices.push(self.addresses[row]);
                        work.step()?;
                    }
                    copy_plan(&self.source, &indices, &mut work)?;
                    work.flush()?;
                    let indices = UInt64Array::from(indices);
                    work.flush()?;
                    let copied = arrow_select::take::take(self.source.as_ref(), &indices, None);
                    work.flush()?;
                    copied.map_err(|_| internal("window offset Arrow take failed"))?
                };
                SelectedValues::try_new_observed::<KernelFailure>(
                    selection,
                    result_type,
                    copied,
                    Box::default(),
                    || work.step(),
                )
            })();
            if self.default.is_some() {
                work.finish_result(result)
            } else {
                if result.as_ref().is_err_and(KernelFailure::is_interruption) {
                    return result;
                }
                work.finish()?;
                result
            }
        })();
        let result = observation.finish(result);
        if result.is_err() {
            self.closed = true;
        }
        result
    }

    fn finish(&mut self, control: &dyn KernelEvaluationControl) -> Result<(), KernelFailure> {
        if self.closed {
            return Err(KernelFailure::InstanceFailed);
        }
        self.closed = true;
        control.checkpoint(0)?;
        EvaluationCheckpoints::new(control).finish()
    }

    fn retained_bytes(&self) -> usize {
        self.retained
    }
}

#[cfg(test)]
#[path = "window_offset_tests.rs"]
mod tests;
