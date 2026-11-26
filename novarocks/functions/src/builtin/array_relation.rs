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

//! Selected array relations over the original, fully typed key operations.

use std::{ops::Range, sync::Arc};

use allocator_api2::vec::Vec as HostVec;
use arrow_array::{
    Array, ArrayRef, ListArray,
    builder::{BooleanBuilder, Int64Builder},
};
use arrow_schema::DataType;
use novarocks_function_contract::kernel_failure::ARRAY_ELEMENT_COMPARISON_INDETERMINATE;
use novarocks_type_contract::{KeyComparison, KeyRelation, key_domains_match_observed, key_leaf};
use novarocks_value_arrays::ValueFailure;
use novarocks_value_arrays::hash_eq::{
    BoundKeyArray, KeyLookup, KeyPreparationFailure, KeyRows, PreparedKeyOperations,
};

use super::array_scalar_output::scratch;
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::arrow_result_custody::{
    custody_type_metadata_upper_bound, retain_result_backing_direct,
};
use crate::kernel_control::{internal, invalid};
use crate::kernel_input::{EvaluationCheckpoints, logical_is_null};
use crate::opaque_memory::OpaqueRetainedCharge;
use crate::selected_copy::{ExtendSegment, list_from_segments_in};
use crate::{
    AggregateStateAllocator, DataFailureReason, EvaluatedArgument, KernelEvaluationControl,
    KernelFailure, RowDataError, ScalarCallContract, ScalarCallInput, SelectedValues,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Operation {
    Contains,
    Position,
    Remove,
    ContainsSequence,
    Overlap,
    Distinct,
    Intersect,
}

impl Operation {
    pub(super) fn relation(self) -> KeyRelation {
        match self {
            Self::Contains | Self::Position | Self::Remove => KeyRelation::Equal,
            Self::ContainsSequence | Self::Overlap | Self::Distinct | Self::Intersect => {
                KeyRelation::Identical
            }
        }
    }

    fn list_result(self) -> bool {
        matches!(self, Self::Remove | Self::Distinct | Self::Intersect)
    }
}

#[derive(Debug)]
pub(super) struct PreparedRelation {
    operation: Operation,
    keys: PreparedKeyOperations,
    right_keys: Option<PreparedKeyOperations>,
    flat: bool,
}

impl PreparedRelation {
    pub(super) fn supports_scalar_broadcast(&self, ordinal: usize) -> bool {
        ordinal < 2 && matches!(self.operation, Operation::Contains | Operation::Position)
    }

    pub(super) fn prepare(
        operation: Operation,
        contract: &ScalarCallContract,
        mut observe: impl FnMut() -> Result<(), KernelFailure>,
    ) -> Result<Self, KernelFailure> {
        let mut arguments = contract.value_argument_types();
        let arity = if operation == Operation::Distinct {
            1
        } else {
            2
        };
        if arguments.len() != arity || contract.selected().argument_types.len() != arity {
            return Err(invalid("array relation has the wrong arity"));
        }
        let left = arguments
            .next()
            .and_then(|list| {
                list.child_at(0)
                    .filter(|_| matches!(list.data_type(), DataType::List(_)))
            })
            .ok_or_else(|| invalid("array relation requires an exact List argument"))?;
        let right = if let Some(right) = arguments.next() {
            let item = if operation.relation() == KeyRelation::Equal {
                right.clone()
            } else {
                right
                    .child_at(0)
                    .filter(|_| matches!(right.data_type(), DataType::List(_)))
                    .ok_or_else(|| {
                        invalid("array relation requires a second exact List argument")
                    })?
            };
            if !key_domains_match_observed::<KernelFailure>(
                left.value_type(),
                item.value_type(),
                &mut |_| observe(),
            )? {
                return Err(invalid(
                    "array relation arguments have different element types",
                ));
            }
            Some(item)
        } else {
            None
        };

        let result = contract.result_type();
        if operation.list_result() {
            let DataType::List(_) = result.data_type() else {
                return Err(invalid("array relation requires an exact List result"));
            };
            let item = result
                .child_at(0)
                .ok_or_else(|| invalid("array relation result has no List element type"))?;
            if !key_domains_match_observed::<KernelFailure>(
                left.value_type(),
                item.value_type(),
                &mut |_| observe(),
            )? || !novarocks_type_contract::arrow_data_types_exact_observed(
                left.data_type(),
                item.data_type(),
                &mut observe,
            )? {
                return Err(invalid(
                    "array relation result differs from its original element type",
                ));
            }
            if operation == Operation::Intersect {
                let right = right.as_ref().expect("required second List argument");
                if !novarocks_type_contract::arrow_data_types_exact_observed(
                    right.data_type(),
                    item.data_type(),
                    &mut observe,
                )? {
                    return Err(invalid(
                        "array intersection result differs from its second element layout",
                    ));
                }
            }
        } else {
            let expected = if operation == Operation::Position {
                DataType::Int64
            } else {
                DataType::Boolean
            };
            if result.data_type() != &expected {
                return Err(invalid("array relation has the wrong scalar result"));
            }
        }

        let flat = key_leaf(left.value_type()).is_ok_and(|leaf| leaf.is_some());
        let mut prepare = |item: &crate::FunctionValueType| {
            PreparedKeyOperations::prepare_observed(item, operation.relation(), &mut |_| observe())
                .map_err(|error| match error {
                    KeyPreparationFailure::Caller(cause) => cause,
                    KeyPreparationFailure::Unsupported(_) => {
                        invalid("array relation has no implementation for its frozen element type")
                    }
                })
        };
        let keys = prepare(&left)?;
        let right_keys = right.as_ref().map(prepare).transpose()?;
        Ok(Self {
            operation,
            keys,
            right_keys,
            flat,
        })
    }

    fn right_keys(&self) -> &PreparedKeyOperations {
        self.right_keys
            .as_ref()
            .expect("operation has a right argument")
    }

    fn ordered_keys(&self, larger: usize) -> (&PreparedKeyOperations, &PreparedKeyOperations) {
        if larger == 0 {
            (&self.keys, self.right_keys())
        } else {
            (self.right_keys(), &self.keys)
        }
    }

    pub(super) fn evaluate<'a>(
        &self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
        host: &Arc<dyn AggregateStateAllocator>,
        allocator: &HostAggregateAllocator,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        control.checkpoint(0)?;
        let arguments = Inputs::new(self.operation, input.arguments())?;
        let selection = input.selection();
        let target = input.contract().result_type().data_type();
        let mut work = EvaluationCheckpoints::new(control);
        let result = (|| {
            let charge = OpaqueRetainedCharge::try_new(Arc::clone(host))?;
            let mut relation = Evaluation {
                prepared: self,
                allocator,
                work: &mut work,
            };
            let mut uncertain = scratch::<usize>(0, allocator)?;
            let values = if self.operation.list_result() {
                let DataType::List(field) = target else {
                    return Err(internal("array relation has a non-List output"));
                };
                relation.work.flush()?;
                let mut rows = scratch(selection.len(), allocator)?;
                let mut segments = scratch::<ExtendSegment>(0, allocator)?;
                for (ordinal, row) in selection.iter().enumerate() {
                    relation.work.step()?;
                    let Some(selected) = arguments.row(ordinal, row, relation.work)? else {
                        rows.push(None);
                        continue;
                    };
                    let original_segments = segments.len();
                    let mut result = RowOutput {
                        segments: &mut segments,
                        initial: original_segments,
                        count: 0,
                    };
                    match relation.list_row(&selected, &mut result)? {
                        true => rows.push(Some(result.count)),
                        false => {
                            segments.truncate(original_segments);
                            rows.push(None);
                            push(&mut uncertain, ordinal, allocator, relation.work)?;
                        }
                    }
                }
                if let RightInput::List(_, right) = arguments.right {
                    let sources = [
                        Arc::clone(arguments.left.values()),
                        Arc::clone(right.values()),
                    ];
                    list_from_segments_in(
                        field,
                        &sources,
                        &segments,
                        &rows,
                        host,
                        allocator,
                        relation.work,
                    )?
                } else {
                    list_from_segments_in(
                        field,
                        std::slice::from_ref(arguments.left.values()),
                        &segments,
                        &rows,
                        host,
                        allocator,
                        relation.work,
                    )?
                }
            } else {
                // The original builders reserve rounded Arrow buffers. The
                // result backing keeps the corresponding host charge.
                let rows = selection.len();
                let bytes = fixed_output_peak(rows, self.operation == Operation::Position)?;
                relation.work.flush()?;
                let mut reservation = charge.reserve_operation(bytes)?;
                let values: ArrayRef = if self.operation == Operation::Position {
                    let mut output = Int64Builder::with_capacity(rows);
                    for (ordinal, row) in selection.iter().enumerate() {
                        relation.work.step()?;
                        let Some(selected) = arguments.row(ordinal, row, relation.work)? else {
                            output.append_null();
                            continue;
                        };
                        match relation.find(&selected)? {
                            Find::Present(position) => output
                                .append_value((position - selected.left.range.start + 1) as i64),
                            Find::Uncertain => {
                                output.append_null();
                                push(&mut uncertain, ordinal, allocator, relation.work)?;
                            }
                            Find::Missing { .. } => output.append_value(0),
                        }
                    }
                    Arc::new(output.finish())
                } else {
                    let mut output = BooleanBuilder::with_capacity(rows);
                    for (ordinal, row) in selection.iter().enumerate() {
                        relation.work.step()?;
                        let Some(selected) = arguments.row(ordinal, row, relation.work)? else {
                            output.append_null();
                            continue;
                        };
                        let value = match self.operation {
                            Operation::Contains => match relation.find(&selected)? {
                                Find::Present(_) => Some(true),
                                Find::Missing { saw_null: false } => Some(false),
                                Find::Missing { saw_null: true } => None,
                                Find::Uncertain => {
                                    push(&mut uncertain, ordinal, allocator, relation.work)?;
                                    None
                                }
                            },
                            Operation::ContainsSequence => {
                                Some(relation.contains_sequence(&selected)?)
                            }
                            Operation::Overlap => relation.overlap(&selected)?,
                            _ => {
                                return Err(internal(
                                    "array relation has the wrong scalar operation",
                                ));
                            }
                        };
                        output.append_option(value);
                    }
                    Arc::new(output.finish())
                };
                let output_charge = OpaqueRetainedCharge::try_new(Arc::clone(host))?;
                retain_result_backing_direct(
                    values,
                    output_charge,
                    &mut reservation,
                    allocator.clone(),
                    relation.work,
                )?
                .values
            };
            let errors = row_errors(self.operation, &uncertain, &charge, relation.work)?;
            SelectedValues::try_new_observed(selection, target, values, errors, || {
                relation.work.step()
            })
        })();
        work.finish_result(result)
    }
}

fn fixed_output_peak(rows: usize, position: bool) -> Result<usize, KernelFailure> {
    let rounded = |size: usize| {
        size.checked_next_multiple_of(64)
            .ok_or(KernelFailure::ResourceExhausted)
    };
    let validity = rounded(rows.div_ceil(8))?;
    let values = if position {
        rounded(
            rows.checked_mul(size_of::<i64>())
                .ok_or(KernelFailure::ResourceExhausted)?,
        )?
    } else {
        validity
    };
    let metadata = custody_type_metadata_upper_bound(1)?;
    values
        .checked_add(validity)
        .and_then(|bytes| bytes.checked_add(metadata))
        .ok_or(KernelFailure::ResourceExhausted)
}

fn row_errors(
    operation: Operation,
    uncertain: &[usize],
    charge: &OpaqueRetainedCharge,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Box<[RowDataError]>, KernelFailure> {
    if uncertain.is_empty() {
        return Ok(Box::default());
    }
    let message = match operation {
        Operation::Contains => "contains does not support indeterminate element comparisons",
        Operation::Position => "array_position does not support indeterminate element comparisons",
        Operation::Remove => "array_remove does not support indeterminate element comparisons",
        _ => return Err(internal("unexpected array relation comparison error")),
    };
    let each = size_of::<RowDataError>()
        .checked_add(message.len())
        .ok_or(KernelFailure::ResourceExhausted)?;
    let peak = each
        .checked_mul(uncertain.len())
        .ok_or(KernelFailure::ResourceExhausted)?;
    work.flush()?;
    let _reservation = charge.reserve_operation(peak)?;
    let mut errors = Vec::new();
    errors
        .try_reserve_exact(uncertain.len())
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    for &ordinal in uncertain {
        work.step()?;
        errors.push(RowDataError::with_reason(
            ordinal,
            DataFailureReason::Domain(ARRAY_ELEMENT_COMPARISON_INDETERMINATE),
            message,
        ));
    }
    Ok(errors.into_boxed_slice())
}

struct Inputs<'a> {
    first: EvaluatedArgument<'a>,
    left: &'a ListArray,
    right: RightInput<'a>,
}

enum RightInput<'a> {
    None,
    Probe(EvaluatedArgument<'a>),
    List(EvaluatedArgument<'a>, &'a ListArray),
}

impl<'a> Inputs<'a> {
    fn new(
        operation: Operation,
        arguments: &[EvaluatedArgument<'a>],
    ) -> Result<Self, KernelFailure> {
        let first = *arguments
            .first()
            .ok_or_else(|| internal("array relation lacks its first argument"))?;
        let left = as_list(first)?;
        let right = match arguments.get(1).copied() {
            None => RightInput::None,
            Some(probe) if operation.relation() == KeyRelation::Equal => RightInput::Probe(probe),
            Some(right) => RightInput::List(right, as_list(right)?),
        };
        Ok(Self { first, left, right })
    }

    fn row(
        &self,
        ordinal: usize,
        logical_row: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Option<SelectedRow<'a>>, KernelFailure> {
        let left_row = self.first.value_row(ordinal, logical_row);
        if self.left.is_null(left_row) {
            return Ok(None);
        }
        let right = match self.right {
            RightInput::None => None,
            RightInput::Probe(argument) => {
                let row = argument.value_row(ordinal, logical_row);
                let array = argument.array();
                if logical_is_null(array.as_ref(), row, 0, work)? {
                    return Ok(None);
                }
                Some(Side {
                    values: array,
                    range: row..row + 1,
                })
            }
            RightInput::List(argument, list) => {
                let row = argument.value_row(ordinal, logical_row);
                if list.is_null(row) {
                    return Ok(None);
                }
                Some(list_side(list, row)?)
            }
        };
        Ok(Some(SelectedRow {
            left: list_side(self.left, left_row)?,
            right,
        }))
    }
}

fn as_list(argument: EvaluatedArgument<'_>) -> Result<&ListArray, KernelFailure> {
    argument
        .array()
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| internal("array relation received a non-List carrier"))
}

fn list_side(list: &ListArray, row: usize) -> Result<Side<'_>, KernelFailure> {
    let offsets = list.value_offsets();
    let start =
        usize::try_from(offsets[row]).map_err(|_| internal("invalid array element range"))?;
    let end =
        usize::try_from(offsets[row + 1]).map_err(|_| internal("invalid array element range"))?;
    if end < start || end > list.values().len() {
        return Err(internal("invalid array element range"));
    }
    Ok(Side {
        values: list.values(),
        range: start..end,
    })
}

struct Side<'a> {
    values: &'a ArrayRef,
    range: Range<usize>,
}

impl Side<'_> {
    fn is_null(
        &self,
        row: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<bool, KernelFailure> {
        logical_is_null(self.values.as_ref(), row, 0, work)
    }
}

struct SelectedRow<'a> {
    left: Side<'a>,
    right: Option<Side<'a>>,
}

impl<'a> SelectedRow<'a> {
    fn right(&self) -> Result<&Side<'a>, KernelFailure> {
        self.right
            .as_ref()
            .ok_or_else(|| internal("array relation lacks its right input"))
    }
}

enum Find {
    Present(usize),
    Missing { saw_null: bool },
    Uncertain,
}

struct Evaluation<'a, 'work> {
    prepared: &'a PreparedRelation,
    allocator: &'a HostAggregateAllocator,
    work: &'a mut EvaluationCheckpoints<'work>,
}

impl Evaluation<'_, '_> {
    fn find(&mut self, row: &SelectedRow<'_>) -> Result<Find, KernelFailure> {
        let right = row.right()?;
        let probe = right.range.start;
        let keys = &self.prepared.keys;
        let left_values = bind(keys, &row.left, self.work)?;
        let right_values = bind(self.prepared.right_keys(), right, self.work)?;
        if self.prepared.flat {
            let result = keys
                .lookup_observed(
                    &left_values,
                    &right_values,
                    probe,
                    &mut |_| {
                        Err(internal(
                            "flat array comparison unexpectedly requested scratch",
                        ))
                    },
                    &mut |steps| observe_steps(self.work, steps),
                )
                .map_err(value_failure)?;
            return Ok(match result {
                KeyLookup::Found(row) => Find::Present(row),
                KeyLookup::Missing => Find::Missing { saw_null: false },
                KeyLookup::Indeterminate => Find::Missing { saw_null: true },
            });
        }
        let mut saw_null = false;
        for index in row.left.range.clone() {
            self.work.step()?;
            if row.left.is_null(index, self.work)? {
                saw_null = true;
                continue;
            }
            match self.compare(&left_values, index, &right_values, probe)? {
                KeyComparison::True => return Ok(Find::Present(index)),
                KeyComparison::False => {}
                KeyComparison::Unknown | KeyComparison::Indeterminate => {
                    return Ok(Find::Uncertain);
                }
            }
        }
        Ok(Find::Missing { saw_null })
    }

    fn compare(
        &mut self,
        left: &BoundKeyArray<'_>,
        left_row: usize,
        right: &BoundKeyArray<'_>,
        right_row: usize,
    ) -> Result<KeyComparison, KernelFailure> {
        let result = self.prepared.keys.compare_in_observed(
            left,
            right,
            (left_row, right_row),
            self.allocator,
            &mut |steps| observe_steps(self.work, steps),
        );
        result.map_err(|error| {
            self.allocator
                .take_recorded_failure()
                .unwrap_or_else(|| value_failure(error))
        })
    }

    fn identical(
        &mut self,
        left: &BoundKeyArray<'_>,
        left_row: usize,
        right: &BoundKeyArray<'_>,
        right_row: usize,
    ) -> Result<bool, KernelFailure> {
        match self.compare(left, left_row, right, right_row)? {
            KeyComparison::True => Ok(true),
            KeyComparison::False => Ok(false),
            KeyComparison::Unknown | KeyComparison::Indeterminate => {
                Err(internal("array element identity must be a total relation"))
            }
        }
    }

    fn hash(&mut self, values: &BoundKeyArray<'_>, row: usize) -> Result<u64, KernelFailure> {
        self.prepared
            .keys
            .hash_observed(values, row, &mut |steps| observe_steps(self.work, steps))
            .map_err(value_failure)
    }

    fn contains_sequence(&mut self, row: &SelectedRow<'_>) -> Result<bool, KernelFailure> {
        let right = row.right()?;
        if right.range.is_empty() {
            return Ok(true);
        }
        let Some(last) = row
            .left
            .range
            .end
            .checked_sub(right.range.len())
            .filter(|&last| last >= row.left.range.start)
        else {
            return Ok(false);
        };
        let left_values = bind(&self.prepared.keys, &row.left, self.work)?;
        let right_values = bind(self.prepared.right_keys(), right, self.work)?;
        'positions: for start in row.left.range.start..=last {
            self.work.step()?;
            for (offset, element) in right.range.clone().enumerate() {
                if !self.identical(&left_values, start + offset, &right_values, element)? {
                    continue 'positions;
                }
            }
            return Ok(true);
        }
        Ok(false)
    }

    fn overlap(&mut self, row: &SelectedRow<'_>) -> Result<Option<bool>, KernelFailure> {
        let (larger, smaller, source) = ordered_sides(row)?;
        if smaller.range.is_empty() {
            return Ok(Some(false));
        }
        let (large_keys, small_keys) = self.prepared.ordered_keys(source);
        let small_values = bind(small_keys, smaller, self.work)?;
        let large_values = bind(large_keys, larger, self.work)?;
        let mut set = SetIndex::new(smaller.range.len(), self.allocator, self.work)?;
        let mut saw_null = false;
        for index in smaller.range.clone() {
            self.work.step()?;
            if smaller.is_null(index, self.work)? {
                saw_null = true;
            } else {
                set.insert(index, &small_values, self)?;
            }
        }
        for index in larger.range.clone() {
            self.work.step()?;
            if larger.is_null(index, self.work)? {
                saw_null = true;
                continue;
            }
            let hash = self.hash(&large_values, index)?;
            if set
                .find(hash, &large_values, index, &small_values, self)?
                .found()
            {
                return Ok(Some(true));
            }
        }
        Ok((!saw_null).then_some(false))
    }

    fn list_row(
        &mut self,
        row: &SelectedRow<'_>,
        output: &mut RowOutput<'_>,
    ) -> Result<bool, KernelFailure> {
        match self.prepared.operation {
            Operation::Remove => {
                let probe = row.right()?;
                let left = bind(&self.prepared.keys, &row.left, self.work)?;
                let right = bind(self.prepared.right_keys(), probe, self.work)?;
                for index in row.left.range.clone() {
                    self.work.step()?;
                    if !row.left.is_null(index, self.work)? {
                        match self.compare(&left, index, &right, probe.range.start)? {
                            KeyComparison::True => continue,
                            KeyComparison::False => {}
                            KeyComparison::Unknown | KeyComparison::Indeterminate => {
                                return Ok(false);
                            }
                        }
                    }
                    output.append(0, index, self.allocator, self.work)?;
                }
            }
            Operation::Distinct => {
                if row.left.range.len() == 1 {
                    output.append(0, row.left.range.start, self.allocator, self.work)?;
                } else if !row.left.range.is_empty() {
                    let values = bind(&self.prepared.keys, &row.left, self.work)?;
                    let mut set = SetIndex::new(row.left.range.len(), self.allocator, self.work)?;
                    for index in row.left.range.clone() {
                        if set.insert(index, &values, self)? {
                            output.append(0, index, self.allocator, self.work)?;
                        }
                    }
                }
            }
            Operation::Intersect => {
                let (larger, smaller, source) = ordered_sides(row)?;
                if smaller.range.is_empty() {
                    return Ok(true);
                }
                let (large_keys, small_keys) = self.prepared.ordered_keys(source);
                let small_values = bind(small_keys, smaller, self.work)?;
                let large_values = bind(large_keys, larger, self.work)?;
                let mut set = SetIndex::new(smaller.range.len(), self.allocator, self.work)?;
                for index in smaller.range.clone() {
                    set.insert(index, &small_values, self)?;
                }
                for index in larger.range.clone() {
                    let hash = self.hash(&large_values, index)?;
                    if let Probe::Found(slot) =
                        set.find(hash, &large_values, index, &small_values, self)?
                    {
                        let matched = &mut set.slots[slot]
                            .as_mut()
                            .expect("found original slot")
                            .matched;
                        if !*matched {
                            *matched = true;
                            output.append(source, index, self.allocator, self.work)?;
                        }
                    }
                }
            }
            _ => return Err(internal("array relation has the wrong List operation")),
        }
        Ok(true)
    }
}

fn ordered_sides<'a, 'row>(
    row: &'row SelectedRow<'a>,
) -> Result<(&'row Side<'a>, &'row Side<'a>, usize), KernelFailure> {
    let right = row.right()?;
    if row.left.range.len() < right.range.len() {
        Ok((right, &row.left, 1))
    } else {
        Ok((&row.left, right, 0))
    }
}

fn bind<'a>(
    keys: &'a PreparedKeyOperations,
    side: &Side<'a>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<BoundKeyArray<'a>, KernelFailure> {
    keys.bind_observed(
        side.values,
        KeyRows::Range(side.range.clone()),
        &mut |steps| observe_steps(work, steps),
    )
    .map_err(value_failure)
}

fn observe_steps(work: &mut EvaluationCheckpoints<'_>, steps: usize) -> Result<(), KernelFailure> {
    for _ in 0..steps {
        work.step()?;
    }
    Ok(())
}

fn value_failure(cause: ValueFailure<KernelFailure>) -> KernelFailure {
    match cause {
        ValueFailure::Caller(error) => error,
        ValueFailure::Value(error) => KernelFailure::from(
            crate::array_projection::ArrayProjectionFailure::Value(error),
        ),
    }
}

fn push<T>(
    target: &mut HostVec<T, HostAggregateAllocator>,
    value: T,
    allocator: &HostAggregateAllocator,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    if target.len() == target.capacity() {
        work.flush()?;
        target
            .try_reserve(1)
            .map_err(|_| allocator.take_failure())?;
    }
    target.push(value);
    Ok(())
}

struct RowOutput<'a> {
    segments: &'a mut HostVec<ExtendSegment, HostAggregateAllocator>,
    initial: usize,
    count: usize,
}

impl RowOutput<'_> {
    fn append(
        &mut self,
        source: usize,
        index: usize,
        allocator: &HostAggregateAllocator,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        if self.segments.len() > self.initial
            && let Some(last) = self.segments.last_mut()
            && last.source == source
            && last.start + last.len == index
        {
            last.len += 1;
        } else {
            push(
                self.segments,
                ExtendSegment {
                    source,
                    start: index,
                    len: 1,
                    repeats: 1,
                    nulls: 0,
                },
                allocator,
                work,
            )?;
        }
        self.count += 1;
        Ok(())
    }
}

// This local index records original positions. Hashes only route candidates;
// an identity check decides every insertion or match even on equal hashes.
struct SetIndex {
    slots: HostVec<Option<Entry>, HostAggregateAllocator>,
}

#[derive(Clone, Copy)]
struct Entry {
    hash: u64,
    position: usize,
    matched: bool,
}

enum Probe {
    Found(usize),
    Vacant(usize),
}

impl Probe {
    fn found(&self) -> bool {
        matches!(self, Self::Found(_))
    }
}

impl SetIndex {
    fn new(
        max_entries: usize,
        allocator: &HostAggregateAllocator,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Self, KernelFailure> {
        let capacity = max_entries
            .checked_mul(2)
            .and_then(usize::checked_next_power_of_two)
            .ok_or(KernelFailure::ResourceExhausted)?;
        work.flush()?;
        let mut slots = scratch(capacity, allocator)?;
        for _ in 0..capacity {
            work.step()?;
            slots.push(None);
        }
        Ok(Self { slots })
    }

    fn find(
        &self,
        hash: u64,
        probe: &BoundKeyArray<'_>,
        position: usize,
        candidates: &BoundKeyArray<'_>,
        cx: &mut Evaluation<'_, '_>,
    ) -> Result<Probe, KernelFailure> {
        let mask = self.slots.len() - 1;
        let mut slot = hash as usize & mask;
        loop {
            cx.work.step()?;
            let Some(entry) = self.slots[slot] else {
                return Ok(Probe::Vacant(slot));
            };
            if entry.hash == hash && cx.identical(probe, position, candidates, entry.position)? {
                return Ok(Probe::Found(slot));
            }
            slot = (slot + 1) & mask;
        }
    }

    fn insert(
        &mut self,
        position: usize,
        values: &BoundKeyArray<'_>,
        cx: &mut Evaluation<'_, '_>,
    ) -> Result<bool, KernelFailure> {
        let hash = cx.hash(values, position)?;
        match self.find(hash, values, position, values, cx)? {
            Probe::Found(_) => Ok(false),
            Probe::Vacant(slot) => {
                self.slots[slot] = Some(Entry {
                    hash,
                    position,
                    matched: false,
                });
                Ok(true)
            }
        }
    }
}

#[cfg(test)]
#[path = "array_relation_tests.rs"]
mod tests;
