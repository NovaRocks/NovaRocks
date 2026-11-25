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

//! Operand groups for exact leaf owners that accept explicit scalar broadcasts.

use super::{ExprEvaluationFailure, Value, Work, internal, mapped, scalar_invocation};
use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, LargeBinaryArray, LargeListArray, LargeStringArray,
        ListArray, MapArray, StringArray, new_null_array,
    },
    datatypes::DataType,
};
use novarocks_functions::{
    AggregateStateAllocator, EvaluatedArgument, KernelFailure, RowDataError,
    ScalarInvocationActivation, SelectedValues, Selection, opaque_memory::OpaqueReservation,
};
use novarocks_type_contract::{ArgumentControl, FunctionInstanceState};
use std::{
    collections::BTreeMap,
    hash::{DefaultHasher, Hash, Hasher},
    sync::Arc,
};

pub(super) struct Groups {
    positions: Vec<usize>,
    offsets: Vec<usize>,
    arguments: Vec<usize>,
    _hold: Option<OpaqueReservation>,
}

struct Group {
    first: usize,
    count: usize,
    next_collision: Option<usize>,
}

pub(super) type ParentRow<'a> = &'a dyn Fn(usize) -> Result<usize, KernelFailure>;

impl Groups {
    pub(super) fn plan(
        prepared: scalar_invocation::PreparedCall<'_>,
        children: &[Value<'_>],
        active: &[usize],
        selection: Selection<'_>,
        parent: Option<ParentRow<'_>>,
        allocator: Option<&Arc<dyn AggregateStateAllocator>>,
        work: &mut Work<'_, '_, impl Sized>,
    ) -> Result<Option<Self>, KernelFailure> {
        let (scalar_invocation::PreparedCall::Kernel(kernel), Some(parent)) = (prepared, parent)
        else {
            return Ok(None);
        };
        let effects = prepared.contract().effects();
        if effects.argument_control != ArgumentControl::Eager
            || effects.value_stability.is_volatile()
            || effects.instance_state != FunctionInstanceState::None
            || !effects.observable_effects.is_empty()
            || active.len() < 2
        {
            return Ok(None);
        }

        let argument_bytes = mapped::bytes::<usize>(children.len())?;
        let _argument_hold = mapped::reserve(allocator, argument_bytes)?;
        let mut arguments = mapped::buffer(children.len())?;
        for (ordinal, child) in children.iter().enumerate() {
            if kernel.supports_scalar_broadcast(ordinal)
                && let Value::Mapped(value) = child.uncontinued()
            {
                let mut wide = false;
                for &row in active {
                    if let Some((array, row)) = value.value(row)
                        && selected_payload(array, row)
                            > size_of::<Group>() + size_of::<(usize, usize)>()
                    {
                        wide = true;
                        break;
                    }
                    work.step()?;
                }
                if wide {
                    arguments.push(ordinal);
                }
            }
            work.step()?;
        }
        if arguments.is_empty() {
            return Ok(None);
        }

        let group_bytes = mapped::bytes::<Group>(active.len())?
            .checked_add(mapped::bytes::<usize>(active.len())?)
            .ok_or(KernelFailure::ResourceExhausted)?;
        let _group_hold = mapped::reserve(allocator, group_bytes)?;
        let mut group_of = mapped::buffer(active.len())?;
        let mut groups: Vec<Group> = mapped::buffer(active.len())?;
        let mut hashes = mapped::ScopedMap::<u64, usize>::new();
        let parent_at = |ordinal| -> Result<_, KernelFailure> {
            parent(
                selection
                    .row(ordinal)
                    .ok_or_else(|| internal("selected leaf row is absent"))?,
            )
        };
        let address = |argument: usize, ordinal: usize| {
            let Value::Mapped(value) = children[argument].uncontinued() else {
                unreachable!("only mapped arguments are qualified for grouping")
            };
            value.address(ordinal)
        };
        for &ordinal in active {
            let parent_row = parent_at(ordinal)?;
            let mut hasher = DefaultHasher::new();
            parent_row.hash(&mut hasher);
            for &argument in &arguments {
                address(argument, ordinal).hash(&mut hasher);
                work.step()?;
            }
            let key = hasher.finish();
            let head = hashes.get(&key).copied();
            let mut search = head;
            let mut found = None;
            while let Some(index) = search {
                let group = &groups[index];
                let mut same = parent_at(group.first)? == parent_row;
                for &argument in &arguments {
                    same &= address(argument, group.first) == address(argument, ordinal);
                    work.step()?;
                }
                if same {
                    found = Some(index);
                    break;
                }
                search = group.next_collision;
            }
            let group_index = match found {
                Some(index) => index,
                None => {
                    let index = groups.len();
                    groups.push(Group {
                        first: ordinal,
                        count: 0,
                        next_collision: head,
                    });
                    hashes.insert(key, index, allocator, work)?;
                    index
                }
            };
            groups[group_index].count += 1;
            group_of.push(group_index);
            work.step()?;
        }
        if groups.len() == active.len() {
            return Ok(None);
        }

        let offsets_len = groups
            .len()
            .checked_add(1)
            .ok_or(KernelFailure::ResourceExhausted)?;
        let bytes = [
            argument_bytes,
            mapped::bytes::<usize>(offsets_len)?,
            mapped::bytes::<usize>(active.len())?,
        ]
        .into_iter()
        .try_fold(0usize, |total, bytes| {
            total
                .checked_add(bytes)
                .ok_or(KernelFailure::ResourceExhausted)
        })?;
        let hold = mapped::reserve(allocator, bytes)?;
        let mut offsets = mapped::buffer(offsets_len)?;
        offsets.push(0);
        for group in &mut groups {
            offsets.push(offsets.last().copied().unwrap() + group.count);
            group.count = 0;
            work.step()?;
        }
        let mut positions = mapped::buffer(active.len())?;
        positions.resize(active.len(), 0);
        for (&ordinal, group) in active.iter().zip(group_of) {
            positions[offsets[group] + groups[group].count] = ordinal;
            groups[group].count += 1;
            work.step()?;
        }
        Ok(Some(Self {
            positions,
            offsets,
            arguments,
            _hold: hold,
        }))
    }

    pub(super) fn evaluate<'a, R>(
        &self,
        instance: &mut scalar_invocation::CallInstance,
        prepared: scalar_invocation::PreparedCall<'_>,
        children: &[Value<'a>],
        selection: Selection<'a>,
        activation: ScalarInvocationActivation,
        inherited: &mut BTreeMap<usize, RowDataError>,
        allocator: Option<&Arc<dyn AggregateStateAllocator>>,
        work: &mut Work<'_, '_, R>,
    ) -> Result<ArrayRef, ExprEvaluationFailure<R>> {
        let ty = prepared.contract().result_type().data_type();
        let mut output =
            mapped::MappedValues::new(ty, self.offsets.len() - 1, selection.len(), allocator)?;
        let _rows = mapped::reserve(
            allocator,
            mapped::bytes::<Option<(usize, usize)>>(selection.len())?,
        )?;
        let mut result_order = mapped::buffer(selection.len())?;
        result_order.resize(selection.len(), None);
        for bounds in self.offsets.windows(2) {
            let ordinals = &self.positions[bounds[0]..bounds[1]];
            let _group = mapped::reserve(allocator, mapped::bytes::<usize>(ordinals.len())?)?;
            let mut group_rows = mapped::buffer(ordinals.len())?;
            for &ordinal in ordinals {
                group_rows.push(
                    selection
                        .row(ordinal)
                        .ok_or_else(|| internal("scalar group row is absent"))?,
                );
                work.step()?;
            }
            let selected =
                Selection::try_sparse_observed(selection.batch_rows(), &group_rows, || {
                    work.step()
                })?;
            let mut values = Vec::with_capacity(children.len());
            for (index, child) in children.iter().enumerate() {
                let storage = match child.uncontinued() {
                    Value::Mapped(value) if self.arguments.binary_search(&index).is_ok() => {
                        let piece = match value.value(ordinals[0]) {
                            Some((source, offset)) => {
                                mapped::slice_rows(source, offset, 1, allocator, work)?
                            }
                            None => new_null_array(value.data_type(), 1),
                        };
                        Operand::Scalar(piece)
                    }
                    Value::Mapped(value) => Operand::Compact(SelectedValues::try_new_observed(
                        selected,
                        value.data_type(),
                        value.materialize(ordinals.iter().copied(), work)?,
                        Box::default(),
                        || work.step(),
                    )?),
                    Value::Selected(value) => Operand::Compact(SelectedValues::try_new_observed(
                        selected,
                        value.values().data_type(),
                        mapped::take_rows(
                            value.values(),
                            ordinals.iter().copied().map(Some),
                            allocator,
                            work,
                        )?,
                        Box::default(),
                        || work.step(),
                    )?),
                    child => Operand::Original(child.argument()?),
                };
                values.push(storage);
                work.step()?;
            }
            let mut arguments = Vec::with_capacity(values.len());
            for value in &values {
                arguments.push(value.argument());
                work.step()?;
            }
            work.flush()?;
            #[cfg(test)]
            super::lambda_observation::record(|| super::lambda_observation::Event::ScalarLeaf {
                rows: selected.len(),
                broadcast: self.arguments.clone(),
            });
            let result = scalar_invocation::invoke(
                instance, selected, &arguments, activation, allocator, work,
            )?;
            let (_, values, errors) = result.into_parts();
            let source = output.push_source(values)?;
            for (local, &ordinal) in ordinals.iter().enumerate() {
                result_order[ordinal] = Some((source, local));
                work.step()?;
            }
            for error in errors {
                let ordinal = ordinals[error.selected_ordinal()];
                inherited.insert(ordinal, error.with_selected_ordinal(ordinal));
                work.step()?;
            }
        }
        for row in result_order {
            output.push(row)?;
            work.step()?;
        }
        Ok(output.materialize(0..selection.len(), work)?)
    }
}

enum Operand<'a> {
    Scalar(ArrayRef),
    Compact(SelectedValues<'a>),
    Original(EvaluatedArgument<'a>),
}
impl Operand<'_> {
    fn argument(&self) -> EvaluatedArgument<'_> {
        match self {
            Self::Scalar(value) => EvaluatedArgument::Scalar(value),
            Self::Compact(value) => EvaluatedArgument::SelectedColumn(value),
            Self::Original(value) => *value,
        }
    }
}

// Only compares repeated source payload with actual grouping metadata. The
// selected-copy author still computes the exact operation extent.
fn selected_payload(array: &ArrayRef, row: usize) -> usize {
    match array.data_type() {
        DataType::List(field) => array
            .as_any()
            .downcast_ref::<ListArray>()
            .map_or(0, |array| {
                (array.value_length(row) as usize).saturating_mul(
                    field
                        .data_type()
                        .primitive_width()
                        .unwrap_or(size_of::<i32>()),
                )
            }),
        DataType::LargeList(field) => {
            array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .map_or(0, |array| {
                    (array.value_length(row) as usize).saturating_mul(
                        field
                            .data_type()
                            .primitive_width()
                            .unwrap_or(size_of::<i64>()),
                    )
                })
        }
        DataType::Map(..) => array
            .as_any()
            .downcast_ref::<MapArray>()
            .map_or(0, |array| {
                (array.value_length(row) as usize).saturating_mul(size_of::<i32>())
            }),
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map_or(0, |array| array.value_length(row) as usize),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map_or(0, |array| array.value_length(row) as usize),
        DataType::Binary => array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .map_or(0, |array| array.value_length(row) as usize),
        DataType::LargeBinary => array
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .map_or(0, |array| array.value_length(row) as usize),
        _ => 0,
    }
}
