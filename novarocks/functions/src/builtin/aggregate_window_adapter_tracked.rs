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
//! Owned, checked window partitions for variable-width aggregate results.

use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_scalar::{ExtremaDirection, ScalarWork};
use crate::builtin::aggregate_array::ArrayKernel;
use crate::builtin::aggregate_concat::ListaggKernel;
use crate::builtin::aggregate_concat_core::listagg_emission;
use crate::builtin::aggregate_extrema_dispatch::PreparedExtrema;
use crate::builtin::array_order::{is_orderable_type, stable_positions};
use crate::builtin::array_scalar_output::scratch;
use crate::builtin::array_structure::append_segment;
use crate::comparison::ExtremaOrder;
use crate::control_values::{
    assemble_indexed_values_in, indexed_value_offset_count, normalize_indexed_value_in,
    visit_indexed_value_offsets, window_i32_offset_failure,
};
use crate::kernel_control::{KernelControlObservation, compile_failure, internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::{OpaqueReservation, OpaqueRetainedCharge};
use crate::scalar_output_operation::with_scalar_output_peak;
use crate::selected_copy::{checked_utf8_output_offset, list_from_segments_in};
use crate::*;
use allocator_api2::vec::Vec as HostVec;
use arrow_array::{Array, ArrayRef, StringArray};
use arrow_buffer::{Buffer, NullBufferBuilder, OffsetBuffer};
use arrow_schema::DataType;
use hashbrown::raw::RawTable;
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, KeyComparison, KeyRelation, PureCompileControl,
    WindowFrameExclusion,
};
use novarocks_value_arrays::ValueFailure;
use novarocks_value_arrays::hash_eq::{
    BoundKeyArray, KeyPreparationFailure, KeyRows, PreparedKeyOperations,
};
use std::{cmp::Ordering, hash::Hasher, sync::Arc};

fn observed<T>(
    control: &dyn KernelEvaluationControl,
    body: impl FnOnce(
        &mut EvaluationCheckpoints<'_>,
        &dyn KernelEvaluationControl,
    ) -> Result<T, KernelFailure>,
) -> Result<T, KernelFailure> {
    let observation = KernelControlObservation::new(control);
    observation.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(&observation);
    let result = body(&mut work, &observation);
    observation.finish(work.finish_result(result))
}

#[derive(Debug)]
pub(in crate::builtin) enum TrackedWindowOwner {
    Array(Arc<ArrayKernel>),
    Listagg(Arc<ListaggKernel>),
    Utf8Extrema(Arc<PreparedExtrema>),
}

impl TrackedWindowOwner {
    fn aggregate(&self) -> &Arc<AggregateCallContract> {
        match self {
            Self::Array(kernel) => kernel.contract(),
            Self::Listagg(kernel) => kernel.contract(),
            Self::Utf8Extrema(kernel) => kernel.contract(),
        }
    }

    pub(in crate::builtin) fn prepare(
        self,
        contract: Arc<WindowCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedWindowKernel>, KernelFailure> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(compile_failure)?;
        let result = (|| {
            let aggregate = self.aggregate();
            work.step().map_err(compile_failure)?;
            if !contract
                .aggregate()
                .is_some_and(|original| Arc::ptr_eq(original, aggregate))
                || aggregate.phase() != AggregateKernelPhase::Single
                || aggregate.state_input_type().is_some()
                || contract.options().ignore_nulls()
                || contract
                    .options()
                    .frame()
                    .is_some_and(|frame| frame.exclusion != WindowFrameExclusion::NoOthers)
            {
                return Err(invalid(
                    "tracked OVER requires its exact Single aggregate and full frame",
                ));
            }
            let keys = if aggregate.distinct() {
                aggregate
                    .logical_argument_types()
                    .map(|argument| {
                        PreparedKeyOperations::prepare_observed(
                            argument,
                            KeyRelation::Identical,
                            &mut |units| observe_compile(units, &mut work),
                        )
                        .map_err(|error| match error {
                            KeyPreparationFailure::Caller(error) => error,
                            KeyPreparationFailure::Unsupported(_) => {
                                invalid("window DISTINCT has no exact key recipe")
                            }
                        })
                    })
                    .collect::<Result<Box<[_]>, _>>()?
            } else {
                Box::default()
            };
            let ordering = aggregate
                .order_argument_types()
                .zip(aggregate.order_keys())
                .map(|(arg, key)| {
                    let order = ExtremaOrder::prepare(arg.value_type(), arg.data_type(), || {
                        work.step().map_err(compile_failure)
                    })?
                    .ok_or_else(|| invalid("window ORDER BY has no exact order recipe"))?;
                    Ok::<_, KernelFailure>((order, *key))
                })
                .collect::<Result<Box<[_]>, _>>()?;
            let offsets = indexed_value_offset_count(contract.result_type().data_type())?;
            Ok(Arc::new(PreparedTrackedWindow {
                owner: self,
                contract,
                keys,
                ordering,
                offsets,
            }) as Arc<dyn PreparedWindowKernel>)
        })();
        work.finish_result(result, compile_failure)
    }
}

fn observe_compile(units: usize, work: &mut CompileCheckpoints<'_>) -> Result<(), KernelFailure> {
    for _ in 0..units {
        work.step().map_err(compile_failure)?;
    }
    Ok(())
}

pub(in crate::builtin) fn admit_frame_modifiers(
    selected: &FunctionBindingSelection,
    logical_count: usize,
    distinct: bool,
    ordered: bool,
    control: &dyn PureCompileControl,
) -> Result<(), FunctionBindingError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    if selected.argument_types.len() < logical_count
        || ordered != (selected.argument_types.len() > logical_count)
    {
        return Err(FunctionBindingError::InvalidBinding(
            "aggregate window requires its exact logical and ORDER BY channels".into(),
        ));
    }
    let unavailable = || FunctionBindingError::UnavailableImplementation(selected.overload.clone());
    for (ordinal, argument) in selected.argument_types.iter().enumerate() {
        work.step()?;
        let FunctionArgumentType::Value(argument) = argument else {
            return Err(FunctionBindingError::InvalidBinding(
                "aggregate window requires exact Value channels".into(),
            ));
        };
        if ordinal >= logical_count {
            if !is_orderable_type(argument.value_type(), &mut || work.step())? {
                return Err(FunctionBindingError::InvalidBinding(
                    "aggregate function ORDER BY expression must be orderable".into(),
                ));
            }
            if !ExtremaOrder::supports(argument.value_type(), argument.data_type(), || work.step())?
            {
                return Err(unavailable());
            }
        } else if distinct
            && KeyRelation::Identical
                .validate_type_observed(argument.value_type(), &mut |units| {
                    for _ in 0..units {
                        work.step()?;
                    }
                    Ok::<(), novarocks_type_contract::CompileControlError>(())
                })?
                .is_err()
        {
            return Err(unavailable());
        }
    }
    work.finish()?;
    Ok(())
}

#[derive(Debug)]
struct PreparedTrackedWindow {
    owner: TrackedWindowOwner,
    contract: Arc<WindowCallContract>,
    keys: Box<[PreparedKeyOperations]>,
    ordering: Box<[(ExtremaOrder, AggregateOrderKey)]>,
    offsets: usize,
}

struct WindowChannel {
    array: ArrayRef,
    broadcast: Option<usize>,
}
impl WindowChannel {
    fn new(source: EvaluatedArgument<'_>) -> Self {
        let broadcast = match source {
            EvaluatedArgument::Column(_) | EvaluatedArgument::SelectedColumn(_) => None,
            _ => Some(source.value_row(0, 0)),
        };
        Self {
            array: Arc::clone(source.array()),
            broadcast,
        }
    }
    fn row(&self, row: usize) -> usize {
        self.broadcast.unwrap_or(row)
    }
}
type WindowVec<T> = HostVec<T, HostAggregateAllocator>;

struct TrackedWindowPartition {
    prepared: Arc<PreparedTrackedWindow>,
    frames: WindowVec<WindowRowRange>,
    logical: WindowVec<WindowChannel>,
    order: WindowVec<WindowChannel>,
    geometry: WindowVec<usize>,
    checked: WindowVec<Option<usize>>,
    emission: FrameScratch,
    host: Arc<dyn AggregateStateAllocator>,
    allocator: HostAggregateAllocator,
    body: OpaqueReservation,
    closed: bool,
}

impl PreparedWindowKernel for PreparedTrackedWindow {
    fn contract(&self) -> &Arc<WindowCallContract> {
        &self.contract
    }

    fn partition_retention(&self, _: usize) -> Result<WindowPartitionRetention, KernelFailure> {
        Ok(WindowPartitionRetention::AllocationTracked)
    }

    fn partition_retained_upper_bound(&self, _: usize) -> Result<usize, KernelFailure> {
        Err(invalid(
            "tracked window requires its original host allocator",
        ))
    }

    fn supports_owned_partition(&self) -> bool {
        true
    }

    fn begin_partition<'input>(
        self: Arc<Self>,
        _: WindowPartitionInput<'input>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn WindowKernelPartition + 'input>, KernelFailure> {
        control.checkpoint(0)?;
        Err(invalid(
            "tracked window requires its original host allocator",
        ))
    }

    fn begin_owned_partition_evaluated(
        self: Arc<Self>,
        input: WindowPartitionInput<'_>,
        _: WindowInvocationContext,
        host: Arc<dyn AggregateStateAllocator>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn WindowKernelPartition>, WindowEvaluationFailure> {
        Ok(self.prepare_partition(input, host, control)?)
    }

    fn begin_partition_evaluated<'input>(
        self: Arc<Self>,
        input: WindowPartitionInput<'input>,
        _: WindowInvocationContext,
        allocator: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn WindowKernelPartition + 'input>, WindowEvaluationFailure> {
        let host = allocator
            .ok_or_else(|| invalid("tracked window requires its original host allocator"))?;
        Ok(self.prepare_partition(input, host, control)?)
    }
}

impl PreparedTrackedWindow {
    fn prepare_partition(
        self: Arc<Self>,
        input: WindowPartitionInput<'_>,
        host: Arc<dyn AggregateStateAllocator>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn WindowKernelPartition>, KernelFailure> {
        observed(control, |work, control| {
            let full = input.full_input();
            if !std::ptr::eq(full.contract(), self.contract.as_ref()) {
                return Err(invalid("tracked partition has a foreign window contract"));
            }
            work.flush()?;
            let allocator = HostAggregateAllocator::try_new_with_journal(Arc::clone(&host))?;
            let charge = OpaqueRetainedCharge::try_new(Arc::clone(&host))?;
            let body = charge.reserve_operation(size_of::<TrackedWindowPartition>())?;
            let rows = full.partition_rows();
            let mut logical = scratch(full.logical_arguments().len(), &allocator)?;
            let mut order = scratch(full.order_arguments().len(), &allocator)?;
            for argument in full.logical_arguments() {
                logical.push(WindowChannel::new(*argument));
                work.step()?;
            }
            for argument in full.order_arguments() {
                order.push(WindowChannel::new(*argument));
                work.step()?;
            }
            if let (TrackedWindowOwner::Array(_), DataType::List(item)) =
                (&self.owner, self.contract.result_type().data_type())
            {
                logical[0].array = normalize_indexed_value_in(
                    &logical[0].array,
                    item.data_type(),
                    Arc::clone(&host),
                    control,
                )?;
            }
            let mut frames = scratch(rows, &allocator)?;
            for &frame in input.frames() {
                frames.push(frame);
                work.step()?;
            }
            let count = rows
                .checked_mul(self.offsets)
                .ok_or(KernelFailure::ResourceExhausted)?;
            let checked = scratch(
                if matches!(&self.owner, TrackedWindowOwner::Array(_)) {
                    0
                } else {
                    rows
                },
                &allocator,
            )?;
            let emission = FrameScratch::new(&allocator)?;
            let mut result = TrackedWindowPartition {
                prepared: self,
                frames,
                logical,
                order,
                geometry: scratch(count, &allocator)?,
                checked,
                emission,
                host,
                allocator,
                body,
                closed: false,
            };
            result.check_frames(work, control)?;
            work.flush()?;
            Ok(Box::new(result) as Box<dyn WindowKernelPartition>)
        })
    }
}

#[derive(Clone, Copy)]
struct KeyEntry {
    hash: u64,
    row: usize,
}

struct FrameScratch {
    rows: WindowVec<usize>,
    temporary: WindowVec<usize>,
    index: RawTable<KeyEntry, HostAggregateAllocator>,
    last: Option<WindowRowRange>,
}
impl FrameScratch {
    fn new(allocator: &HostAggregateAllocator) -> Result<Self, KernelFailure> {
        Ok(Self {
            rows: scratch(0, allocator)?,
            temporary: scratch(0, allocator)?,
            index: RawTable::new_in(allocator.clone()),
            last: None,
        })
    }
    fn retained_bytes(&self) -> usize {
        (self.rows.capacity() + self.temporary.capacity()) * size_of::<usize>()
            + self.index.allocation_info().1.size()
    }
}
struct FrameRows<'a> {
    logical: &'a [WindowChannel],
    order: &'a [WindowChannel],
    keys: &'a [PreparedKeyOperations],
    bound_keys: WindowVec<BoundKeyArray<'a>>,
    ordering: &'a [(ExtremaOrder, AggregateOrderKey)],
    state: &'a mut FrameScratch,
    allocator: &'a HostAggregateAllocator,
}

impl<'a> FrameRows<'a> {
    fn new(
        prepared: &'a PreparedTrackedWindow,
        logical: &'a [WindowChannel],
        order: &'a [WindowChannel],
        allocator: &'a HostAggregateAllocator,
        state: &'a mut FrameScratch,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Self, KernelFailure> {
        let keys = &prepared.keys;
        let mut bound_keys = scratch(keys.len(), allocator)?;
        for (key, source) in keys.iter().zip(logical) {
            let bound = key
                .bind_observed(&source.array, KeyRows::All, &mut |units| {
                    observe_eval(units, work)
                })
                .map_err(key_failure)?;
            bound_keys.push(bound);
        }
        Ok(Self {
            logical,
            order,
            keys,
            bound_keys,
            ordering: &prepared.ordering,
            state,
            allocator,
        })
    }

    fn fill(
        &mut self,
        frame: WindowRowRange,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<usize, KernelFailure> {
        work.step()?;
        if self.state.last == Some(frame) {
            return Ok(self.state.rows.len());
        }
        let append = self.state.last.filter(|last| {
            self.ordering.is_empty() && last.start == frame.start && last.end <= frame.end
        });
        let start = if let Some(last) = append {
            last.end
        } else {
            self.state.rows.clear();
            self.state.index.clear();
            frame.start
        };
        let prefix = self.state.rows.len();
        for row in start..frame.end {
            work.step()?;
            if !self.keys.is_empty() && !self.insert_key(row, work)? {
                continue;
            }
            if self.state.rows.len() == self.state.rows.capacity() {
                work.flush()?;
                self.state
                    .rows
                    .try_reserve(1)
                    .map_err(|_| self.allocator.take_failure())?;
            }
            self.state.rows.push(row);
        }
        self.state.last = Some(frame);
        if !self.ordering.is_empty() {
            work.flush()?;
            self.state.temporary.clear();
            self.state
                .temporary
                .try_reserve(self.state.rows.len())
                .map_err(|_| self.allocator.take_failure())?;
            self.state.temporary.resize(self.state.rows.len(), 0);
            let ordering = self.ordering;
            let channels = self.order;
            stable_positions(
                &mut self.state.rows,
                &mut self.state.temporary,
                work,
                |left, right, work| compare_order(ordering, channels, left, right, work),
            )?;
            work.flush()?;
        }
        Ok(prefix)
    }

    fn insert_key(
        &mut self,
        row: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<bool, KernelFailure> {
        let mut hash = std::hash::DefaultHasher::new();
        for ((recipe, values), source) in self.keys.iter().zip(&self.bound_keys).zip(self.logical) {
            hash.write_u64(
                recipe
                    .hash_observed(values, source.row(row), &mut |units| {
                        observe_eval(units, work)
                    })
                    .map_err(key_failure)?,
            );
        }
        let hash = hash.finish();
        let mut failure = None;
        let existing = self
            .state
            .index
            .get(hash, |entry| {
                if entry.hash != hash || failure.is_some() {
                    return false;
                }
                match self.same_key(row, entry.row, work) {
                    Ok(same) => same,
                    Err(error) => {
                        failure = Some(error);
                        false
                    }
                }
            })
            .is_some();
        if let Some(error) = failure {
            return Err(error);
        }
        if existing {
            return Ok(false);
        }
        if self.state.index.len() == self.state.index.capacity() {
            work.flush()?;
            self.state
                .index
                .try_reserve(1, |entry| entry.hash)
                .map_err(|_| self.allocator.take_failure())?;
        }
        self.state
            .index
            .insert(hash, KeyEntry { hash, row }, |entry| entry.hash);
        Ok(true)
    }

    fn same_key(
        &self,
        left: usize,
        right: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<bool, KernelFailure> {
        for ((recipe, values), source) in self.keys.iter().zip(&self.bound_keys).zip(self.logical) {
            let comparison = recipe
                .compare_in_observed(
                    values,
                    values,
                    (source.row(left), source.row(right)),
                    self.allocator,
                    &mut |units| observe_eval(units, work),
                )
                .map_err(key_failure)?;
            match comparison {
                KeyComparison::True => {}
                KeyComparison::False => return Ok(false),
                _ => return Err(internal("IDENTICAL aggregate window key was indeterminate")),
            }
        }
        Ok(true)
    }
}

fn observe_eval(units: usize, work: &mut EvaluationCheckpoints<'_>) -> Result<(), KernelFailure> {
    for _ in 0..units {
        work.step()?;
    }
    Ok(())
}

fn key_failure(failure: ValueFailure<KernelFailure>) -> KernelFailure {
    match failure {
        ValueFailure::Caller(error) => error,
        ValueFailure::Value(error) => KernelFailure::from_domain(error),
    }
}

fn compare_order(
    ordering: &[(ExtremaOrder, AggregateOrderKey)],
    channels: &[WindowChannel],
    left: usize,
    right: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Ordering, KernelFailure> {
    for ((order, key), channel) in ordering.iter().zip(channels) {
        work.step()?;
        let array = channel.array.as_ref();
        let (l, r) = (channel.row(left), channel.row(right));
        let null = |row| array.data_type() == &DataType::Null || array.is_null(row);
        let compared = match (null(l), null(r)) {
            (true, true) => Ordering::Equal,
            (true, false) if key.nulls_first => Ordering::Less,
            (true, false) => Ordering::Greater,
            (false, true) if key.nulls_first => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => {
                let ord = order.compare((array, l), (array, r), ExtremaDirection::Minimum, work)?;
                if key.ascending { ord } else { ord.reverse() }
            }
        };
        if compared != Ordering::Equal {
            return Ok(compared);
        }
    }
    // A peer uses the selected frame's original encounter order.
    Ok(left.cmp(&right))
}

fn listagg_rows<'a>(
    logical: &'a [WindowChannel],
    rows: &'a [usize],
) -> impl Iterator<Item = [(&'a ArrayRef, usize); 5]> {
    rows.iter().map(move |&row| {
        std::array::from_fn(|ordinal| {
            let source = &logical[ordinal];
            (&source.array, source.row(row))
        })
    })
}

impl TrackedWindowPartition {
    fn check_frames(
        &mut self,
        work: &mut EvaluationCheckpoints<'_>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        let mut state = FrameScratch::new(&self.allocator)?;
        let mut rows = FrameRows::new(
            &self.prepared,
            &self.logical,
            &self.order,
            &self.allocator,
            &mut state,
            work,
        )?;
        let mut extents = scratch(self.prepared.offsets, &self.allocator)?;
        extents.resize(self.prepared.offsets, 0);
        let source = &self.logical[0];
        let mut last_checked = None;
        for &frame in &self.frames {
            work.step()?;
            let same_frame = rows.state.last == Some(frame);
            let prefix = rows.fill(frame, work)?;
            match &self.prepared.owner {
                TrackedWindowOwner::Array(_) => {
                    if prefix == 0 {
                        extents.fill(0);
                    }
                    extents[0] = rows.state.rows.len();
                    work.flush()?;
                    for &row in &rows.state.rows[prefix..] {
                        visit_indexed_value_offsets(
                            source.array.as_ref(),
                            source.row(row),
                            1,
                            &mut |ordinal, bytes| {
                                let current = extents.get_mut(ordinal).ok_or_else(|| {
                                    internal("window ARRAY element offset shape changed")
                                })?;
                                *current = current
                                    .checked_add(bytes)
                                    .ok_or(KernelFailure::ResourceExhausted)?;
                                Ok(())
                            },
                            control,
                        )?;
                    }
                }
                TrackedWindowOwner::Listagg(_) => {
                    if !same_frame {
                        last_checked = listagg_emission::emit_window(
                            listagg_rows(&self.logical, &rows.state.rows),
                            &self.allocator,
                            &mut ScalarWork::new(Some(work)),
                            |_, _| Ok(()),
                        )?;
                    }
                    extents[0] = last_checked.unwrap_or(0);
                    self.checked.push(last_checked);
                }
                TrackedWindowOwner::Utf8Extrema(kernel) => {
                    let PreparedExtrema::Bytes(kernel) = kernel.as_ref() else {
                        return Err(internal("VARCHAR window uses a foreign extrema owner"));
                    };
                    let mut winner = if prefix == 0 { None } else { last_checked };
                    for &row in &rows.state.rows[prefix..] {
                        winner = kernel.window_candidate(
                            &source.array,
                            source.row(row),
                            winner,
                            work,
                        )?;
                    }
                    let values = source
                        .array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| internal("VARCHAR window extrema input is not Utf8"))?;
                    extents[0] = winner.map_or(0, |row| values.value(row).len());
                    last_checked = winner;
                    self.checked.push(winner);
                }
            }
            for (ordinal, &units) in extents.iter().enumerate() {
                work.step()?;
                if units > i32::MAX as usize {
                    return Err(window_i32_offset_failure(
                        self.prepared.contract.result_type().data_type(),
                        ordinal,
                        units,
                    ));
                }
                self.geometry.push(units);
            }
        }
        Ok(())
    }

    fn emit_array(
        &mut self,
        selection: Selection<'_>,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<ArrayRef, KernelFailure> {
        let DataType::List(field) = self.prepared.contract.result_type().data_type() else {
            return Err(internal("ARRAY window result is not a list"));
        };
        let mut rows = FrameRows::new(
            &self.prepared,
            &self.logical,
            &self.order,
            &self.allocator,
            &mut self.emission,
            work,
        )?;
        let mut lengths = scratch(selection.len(), &self.allocator)?;
        let mut segments = scratch(0, &self.allocator)?;
        let source = &self.logical[0];
        for row in selection.iter() {
            rows.fill(self.frames[row], work)?;
            let items = rows.state.rows.len();
            lengths.push((items != 0).then_some(items));
            for &item in &rows.state.rows {
                work.step()?;
                append_segment(&mut segments, source.row(item), 1, 0, &self.allocator, work)?;
            }
        }
        list_from_segments_in(
            field,
            std::slice::from_ref(&source.array),
            &segments,
            &lengths,
            &self.host,
            &self.allocator,
            work,
        )
    }

    fn emit_listagg(
        &mut self,
        selection: Selection<'_>,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<ArrayRef, KernelFailure> {
        let mut total = 0usize;
        for row in selection.iter() {
            work.step()?;
            total = total
                .checked_add(self.checked[row].unwrap_or(0))
                .ok_or(KernelFailure::ResourceExhausted)?;
        }
        checked_utf8_output_offset(total)?;
        let peak = crate::scalar_output_resources::flat_utf8_output_peak(total, selection.len())?;
        Ok(with_scalar_output_peak(
            peak,
            Arc::clone(&self.host),
            &self.allocator,
            work,
            |work| {
                let mut rows = FrameRows::new(
                    &self.prepared,
                    &self.logical,
                    &self.order,
                    &self.allocator,
                    &mut self.emission,
                    work,
                )?;
                let mut payload = Vec::new();
                payload
                    .try_reserve_exact(total)
                    .map_err(|_| KernelFailure::ResourceExhausted)?;
                let mut offsets = Vec::new();
                let offsets_count = selection
                    .len()
                    .checked_add(1)
                    .ok_or(KernelFailure::ResourceExhausted)?;
                offsets
                    .try_reserve_exact(offsets_count)
                    .map_err(|_| KernelFailure::ResourceExhausted)?;
                offsets.push(0i32);
                let mut validity = NullBufferBuilder::new(selection.len());
                for row in selection.iter() {
                    rows.fill(self.frames[row], work)?;
                    let length = listagg_emission::emit_window(
                        listagg_rows(&self.logical, &rows.state.rows),
                        &self.allocator,
                        &mut ScalarWork::new(Some(work)),
                        |piece, work| {
                            if piece.len() > total - payload.len() {
                                return Err(internal("LISTAGG exceeded its checked window output"));
                            }
                            for block in piece.as_bytes().chunks(32 * 1024) {
                                payload.extend_from_slice(block);
                                work.step()?;
                            }
                            Ok(())
                        },
                    )?;
                    if length != self.checked[row] {
                        return Err(internal("LISTAGG changed its checked window value layout"));
                    }
                    validity.append(length.is_some());
                    offsets.push(checked_utf8_output_offset(payload.len())?);
                }
                work.flush()?;
                let array = StringArray::try_new(
                    OffsetBuffer::new(offsets.into()),
                    Buffer::from(payload),
                    validity.finish(),
                )
                .map_err(|_| internal("checked LISTAGG window output could not be built"))?;
                Ok(Arc::new(array) as ArrayRef)
            },
        )?
        .values)
    }

    fn emit_extrema(
        &self,
        selection: Selection<'_>,
        work: &mut EvaluationCheckpoints<'_>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure> {
        let mut choices = scratch(selection.len(), &self.allocator)?;
        for row in selection.iter() {
            work.step()?;
            choices.push(self.checked[row].map(|source| (0, source)));
        }
        work.flush()?;
        assemble_indexed_values_in(
            self.prepared.contract.result_type().data_type(),
            &[Some(&self.logical[0].array)],
            &choices,
            Arc::clone(&self.host),
            control,
        )
    }
}

impl WindowKernelPartition for TrackedWindowPartition {
    fn visit_output_row_offsets(
        &self,
        row: usize,
        visit: &mut dyn FnMut(usize, usize) -> Result<(), KernelFailure>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        if row >= self.frames.len() {
            return Err(invalid(
                "tracked window offset request is outside its original partition",
            ));
        }
        let offsets = self.prepared.offsets;
        let start = row * offsets;
        for (ordinal, &bytes) in self.geometry[start..start + offsets].iter().enumerate() {
            control.checkpoint(1)?;
            visit(ordinal, bytes)?;
        }
        Ok(())
    }

    fn evaluate<'selection>(
        &mut self,
        selection: Selection<'selection>,
        row_capacity: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'selection>, KernelFailure> {
        if self.closed {
            return Err(KernelFailure::InstanceFailed);
        }
        let result = observed(control, |work, control| {
            if selection.batch_rows() != self.frames.len() || selection.len() > row_capacity {
                return Err(invalid(
                    "tracked window output differs from its original domain or capacity",
                ));
            }
            let values = match &self.prepared.owner {
                TrackedWindowOwner::Array(_) => self.emit_array(selection, work)?,
                TrackedWindowOwner::Listagg(_) => self.emit_listagg(selection, work)?,
                TrackedWindowOwner::Utf8Extrema(_) => {
                    self.emit_extrema(selection, work, control)?
                }
            };
            SelectedValues::try_new_observed(
                selection,
                self.prepared.contract.result_type().data_type(),
                values,
                Box::default(),
                || work.step(),
            )
        });
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
        control.checkpoint(0)
    }

    fn retained_bytes(&self) -> usize {
        self.allocator.metadata_bytes()
            + self.allocator.journal_backing_bytes()
            + self.body.remaining_bytes()
            + self.frames.capacity() * size_of::<WindowRowRange>()
            + (self.logical.capacity() + self.order.capacity()) * size_of::<WindowChannel>()
            + self.geometry.capacity() * size_of::<usize>()
            + self.checked.capacity() * size_of::<Option<usize>>()
            + self.emission.retained_bytes()
    }
}
