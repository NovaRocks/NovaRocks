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

//! Selected, resumable collection evaluation. A request contains only the
//! source positions chosen by this overload's current algorithm frontier.

// Design: ADR-0270 (docs/adr/ADR-0270-collection-lambdas-use-bound-body-requests.md)
use std::{fmt, ops::Range, sync::Arc};

use allocator_api2::vec::Vec as AllocVec;
use arrow_array::{Array, ArrayRef, BooleanArray, ListArray, MapArray, builder::BooleanBuilder};
use arrow_schema::DataType;
use hashbrown::{HashMap, hash_map::DefaultHashBuilder};
use novarocks_failure_contract::kernel_reason::INVALID_FUNCTION_ARGUMENT;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, KeyComparison, KeyRelation};
use novarocks_value_arrays::{
    ValueFailure,
    hash_eq::{KeyPreparationFailure, KeyRows, PreparedKeyOperations},
};

use super::{
    CollectionLambdaSignature, HigherOrderCallContract, LambdaBodyContract, PureCompileControl,
};
use crate::{
    AggregateStateAllocator, DataFailureReason, HigherOrderCall, HigherOrderCallInput,
    HigherOrderKernelInstance, HigherOrderOutput, HigherOrderStep, KernelEvaluationControl,
    KernelFailure, LambdaBodyBatch, LambdaBodyBatchBuilder, LambdaBodyBatchOutput,
    MAX_UNOBSERVED_KERNEL_WORK, PreparedHigherOrderKernel, RequiredLambdaBodyError, RowDataError,
    SelectedValues, Selection,
    aggregate_host_allocator::HostAggregateAllocator,
    arrow_result_custody::{custody_type_metadata_upper_bound, retain_result_backing_direct},
    kernel_control::{compile_failure, internal, invalid},
    kernel_input::{EvaluationCheckpoints, logical_is_null},
    opaque_memory::{OpaqueReservation, OpaqueRetainedCharge},
    selected_copy::{
        ExtendSegment, array_from_segments_in, empty_array_in, list_from_segments_in,
        map_from_struct_children_in,
    },
};

mod sort;

type Scratch<T> = AllocVec<T, HostAggregateAllocator>;
type Index<K, V> = HashMap<K, V, DefaultHashBuilder, HostAggregateAllocator>;

// Use the kernel's existing cooperative work quantum for body handoffs. This
// bounds one set of in-flight indices, not the collection extent or total work.
const BATCH_ROWS: usize = MAX_UNOBSERVED_KERNEL_WORK as usize;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Operation {
    Transform,
    Filter,
    Any,
    All,
    Sort,
    MapFilter,
    TransformKeys,
    TransformValues,
}
impl Operation {
    fn decisive(self) -> Option<bool> {
        match self {
            Self::Any => Some(true),
            Self::All => Some(false),
            _ => None,
        }
    }

    fn transforms(self) -> bool {
        matches!(
            self,
            Self::Transform | Self::TransformKeys | Self::TransformValues
        )
    }
}

pub fn prepare_kernel(
    signature: CollectionLambdaSignature,
    contract: Arc<HigherOrderCallContract>,
    body: Arc<LambdaBodyContract>,
    control: &dyn PureCompileControl,
) -> Result<Arc<dyn PreparedHigherOrderKernel>, KernelFailure> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
        .map_err(compile_failure)?;
    let operation = match signature.name() {
        "transform" => Operation::Transform,
        "filter" => Operation::Filter,
        "any_match" => Operation::Any,
        "all_match" => Operation::All,
        "array_sort" => Operation::Sort,
        "map_filter" => Operation::MapFilter,
        "transform_keys" => Operation::TransformKeys,
        "transform_values" => Operation::TransformValues,
        _ => return Err(internal("unknown collection-lambda owner")),
    };
    let effects = body
        .effects()
        .for_use(body.context())
        .map_err(|_| internal("collection lambda lost its exact body effects"))?;
    let nullable_key = operation == Operation::TransformKeys && body.result_type().nullable();
    let unknown_key =
        operation == Operation::TransformKeys && body.result_type().data_type() == &DataType::Null;
    let keys = if operation == Operation::TransformKeys && !unknown_key {
        let keys = PreparedKeyOperations::prepare_observed(
            body.result_type(),
            KeyRelation::Identical,
            &mut |units| {
                for _ in 0..units {
                    work.step()?;
                }
                Ok(())
            },
        )
        .map_err(|error| match error {
            KeyPreparationFailure::Caller(cause) => compile_failure(cause),
            KeyPreparationFailure::Unsupported(_) => {
                invalid("transform_keys has no prepared key identity")
            }
        })?;
        Some(Arc::new(keys))
    } else {
        None
    };
    work.finish().map_err(compile_failure)?;
    Ok(Arc::new(Prepared {
        contract,
        body,
        recipe: Recipe {
            operation,
            speculative: effects.permits_boolean_reordering(),
            certain_range: operation.decisive().is_none()
                && !effects.may_raise_row_error
                && !effects.may_raise_invocation_data
                && !nullable_key,
            unknown_key,
            keys,
        },
    }))
}

#[derive(Clone)]
struct Recipe {
    operation: Operation,
    speculative: bool,
    certain_range: bool,
    unknown_key: bool,
    keys: Option<Arc<PreparedKeyOperations>>,
}

struct Prepared {
    contract: Arc<HigherOrderCallContract>,
    body: Arc<LambdaBodyContract>,
    recipe: Recipe,
}
impl fmt::Debug for Prepared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedCollectionLambda")
            .field("operation", &self.recipe.operation)
            .finish_non_exhaustive()
    }
}
impl PreparedHigherOrderKernel for Prepared {
    fn contract(&self) -> &Arc<HigherOrderCallContract> {
        &self.contract
    }
    fn body_contract(&self) -> &Arc<LambdaBodyContract> {
        &self.body
    }
    fn instance_retained_upper_bound(&self) -> usize {
        size_of::<Instance>()
    }
    fn create_instance(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn HigherOrderKernelInstance>, KernelFailure> {
        control.checkpoint(0)?;
        Ok(Box::new(Instance {
            recipe: self.recipe.clone(),
            contract: Arc::clone(&self.contract),
        }))
    }
}

struct Instance {
    recipe: Recipe,
    contract: Arc<HigherOrderCallContract>,
}
impl HigherOrderKernelInstance for Instance {
    fn begin<'call, 'input: 'call>(
        &'call mut self,
        input: HigherOrderCallInput<'_, 'input>,
        allocator: &Arc<dyn AggregateStateAllocator>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn HigherOrderCall<'input> + 'call>, KernelFailure> {
        let mut work = EvaluationCheckpoints::new(control);
        let shared = Storage::new(allocator)?;
        let invocation: Box<dyn HigherOrderCall<'input> + 'call> =
            if self.recipe.operation == Operation::Sort {
                Box::new(sort::Sort::new(
                    input,
                    Arc::clone(&self.contract),
                    self.recipe.speculative,
                    shared,
                    &mut work,
                )?)
            } else {
                Box::new(Linear::new(
                    input,
                    Arc::clone(&self.contract),
                    self.recipe.clone(),
                    shared,
                    &mut work,
                )?)
            };
        work.finish()?;
        Ok(invocation)
    }
    fn retained_bytes(&self) -> usize {
        size_of::<Self>()
    }
}

struct Storage {
    host: Arc<dyn AggregateStateAllocator>,
    allocator: HostAggregateAllocator,
}
impl Storage {
    fn new(host: &Arc<dyn AggregateStateAllocator>) -> Result<Self, KernelFailure> {
        Ok(Self {
            host: Arc::clone(host),
            allocator: HostAggregateAllocator::try_new(Arc::clone(host))?,
        })
    }
    fn vec<T>(&self, capacity: usize) -> Result<Scratch<T>, KernelFailure> {
        let mut values = Scratch::new_in(self.allocator.clone());
        values
            .try_reserve_exact(capacity)
            .map_err(|_| self.allocator.take_failure())?;
        Ok(values)
    }

    fn push<T>(&self, vector: &mut Scratch<T>, value: T) -> Result<(), KernelFailure> {
        vector
            .try_reserve(1)
            .map_err(|_| self.allocator.take_failure())?;
        vector.push(value);
        Ok(())
    }

    fn index<K, V>(&self) -> Index<K, V> {
        HashMap::with_hasher_in(DefaultHashBuilder::default(), self.allocator.clone())
    }

    fn reserve(&self, size: usize) -> Result<OpaqueReservation, KernelFailure> {
        OpaqueRetainedCharge::try_new(Arc::clone(&self.host))?.reserve_operation(size)
    }
}

enum Columns {
    List(ArrayRef),
    Map([ArrayRef; 2]),
}
impl Columns {
    fn from_input(input: &ArrayRef) -> Result<Self, KernelFailure> {
        let actual = input.as_any();
        if let Some(list) = actual.downcast_ref::<ListArray>() {
            return Ok(Self::List(Arc::clone(list.values())));
        }
        let map = actual
            .downcast_ref::<MapArray>()
            .ok_or_else(|| invalid("collection lambda has no bound List or MAP carrier"))?;
        Ok(Self::Map([
            Arc::clone(map.keys()),
            Arc::clone(map.values()),
        ]))
    }

    fn batch_sources(&self) -> &[ArrayRef] {
        match self {
            Self::List(value) => std::slice::from_ref(value),
            Self::Map(values) => values,
        }
    }
}

fn source_ranges(input: &ArrayRef) -> Result<&[i32], KernelFailure> {
    match input.data_type() {
        DataType::List(_) => input
            .as_any()
            .downcast_ref::<ListArray>()
            .map(ListArray::value_offsets),
        DataType::Map(_, _) => input
            .as_any()
            .downcast_ref::<MapArray>()
            .map(MapArray::value_offsets),
        _ => None,
    }
    .ok_or_else(|| invalid("collection lambda has no physical offsets"))
}

fn parent_range(
    input: &ArrayRef,
    offsets: &[i32],
    row: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<Range<usize>>, KernelFailure> {
    work.step()?;
    if input.is_null(row) {
        return Ok(None);
    }
    Ok(Some(offsets[row] as usize..offsets[row + 1] as usize))
}

#[derive(Debug)]
enum ParentError<'input> {
    Own(RowDataError),
    Required(RequiredLambdaBodyError<'input>),
}

fn bad_argument(parent: usize, message: &'static str) -> ParentError<'static> {
    ParentError::Own(RowDataError::with_reason(
        parent,
        DataFailureReason::Domain(INVALID_FUNCTION_ARGUMENT),
        message,
    ))
}

fn required_error<'input>(
    response: &LambdaBodyBatchOutput<'input>,
    index: usize,
) -> Result<ParentError<'input>, KernelFailure> {
    response
        .required_parent_error(index)?
        .map(ParentError::Required)
        .ok_or_else(|| internal("collection lambda lost its actual required body error"))
}

struct Piece {
    source: ArrayRef,
    start: usize,
    len: usize,
}

fn emit(
    pieces: &mut Scratch<Piece>,
    source: &ArrayRef,
    row: usize,
    storage: &Storage,
) -> Result<(), KernelFailure> {
    if let Some(last) = pieces.last_mut()
        && Arc::ptr_eq(&last.source, source)
        && last.start + last.len == row
    {
        last.len += 1;
        return Ok(());
    }
    storage.push(
        pieces,
        Piece {
            source: Arc::clone(source),
            start: row,
            len: 1,
        },
    )
}

struct Parent<'input> {
    present: bool,
    next: usize,
    end: usize,
    match_window: usize,
    saw_null: bool,
    decision: Option<bool>,
    length: usize,
    pieces: [Scratch<Piece>; 2],
    error: Option<ParentError<'input>>,
}
impl<'input> Parent<'input> {
    fn new(range: Option<Range<usize>>, storage: &Storage) -> Result<Self, KernelFailure> {
        let present = range.is_some();
        let range = range.unwrap_or_default();
        Ok(Self {
            present,
            next: range.start,
            end: range.end,
            match_window: 1,
            saw_null: false,
            decision: None,
            length: 0,
            pieces: [storage.vec(0)?, storage.vec(0)?],
            error: None,
        })
    }

    fn active(&self) -> bool {
        self.next < self.end
    }

    fn fail(&mut self, error: ParentError<'input>) {
        self.next = self.end;
        self.error = Some(error);
        for pieces in &mut self.pieces {
            *pieces = Scratch::new_in(pieces.allocator().clone());
        }
    }

    fn length(&self) -> Option<usize> {
        (self.present && self.error.is_none()).then_some(self.length)
    }

    fn matched(&self, decisive: bool) -> Option<bool> {
        if !self.present || self.error.is_some() {
            None
        } else {
            self.decision
                .or_else(|| (!self.saw_null).then_some(!decisive))
        }
    }
}

struct Linear<'input> {
    selection: Selection<'input>,
    contract: Arc<HigherOrderCallContract>,
    recipe: Recipe,
    columns: Columns,
    parents: Scratch<Parent<'input>>,
    remaining: usize,
    next_parent: usize,
    storage: Storage,
    _invocation: OpaqueReservation,
}
impl<'input> Linear<'input> {
    fn new(
        input: HigherOrderCallInput<'_, 'input>,
        contract: Arc<HigherOrderCallContract>,
        recipe: Recipe,
        storage: Storage,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Self, KernelFailure> {
        let _invocation = storage.reserve(size_of::<Self>())?;
        let selection = input.selection();
        let [ordinary] = input.arguments() else {
            return Err(invalid(
                "collection lambda requires one collection argument",
            ));
        };
        let source = ordinary.array();
        let columns = Columns::from_input(source)?;
        let offsets = source_ranges(source)?;
        let mut parents = storage.vec(selection.len())?;
        let mut remaining = 0;
        for (parent, row) in selection.iter().enumerate() {
            let mut state = Parent::new(
                parent_range(source, offsets, ordinary.value_row(parent, row), work)?,
                &storage,
            )?;
            if state.active() {
                if recipe.unknown_key {
                    state.fail(bad_argument(parent, "map key cannot be null"));
                } else {
                    remaining += 1;
                }
            }
            parents.push(state);
        }
        Ok(Self {
            selection,
            contract,
            recipe,
            columns,
            parents,
            remaining,
            next_parent: 0,
            storage,
            _invocation,
        })
    }

    fn request(
        &mut self,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<LambdaBodyBatch, KernelFailure> {
        let first_parent = self.next_parent;
        let share = if self.recipe.speculative {
            BATCH_ROWS.div_ceil(self.remaining)
        } else {
            BATCH_ROWS
        };
        let mut capacity = 0;
        let mut runs = self.storage.vec(self.remaining.min(BATCH_ROWS))?;
        for _ in 0..self.parents.len() {
            let parent = self.next_parent;
            self.next_parent = (self.next_parent + 1) % self.parents.len();
            let row = &mut self.parents[parent];
            work.step()?;
            if !row.active() {
                continue;
            }
            let limit = if !self.recipe.speculative && !self.recipe.certain_range {
                1
            } else if self.recipe.operation.decisive().is_some() {
                row.match_window
            } else {
                share
            };
            let count = (row.end - row.next)
                .min(limit)
                .min(share)
                .min(BATCH_ROWS - capacity);
            runs.push((parent, row.next..row.next + count));
            capacity += count;
            if !self.recipe.speculative || capacity == BATCH_ROWS {
                if !self.recipe.speculative {
                    // An effectful row owns its complete evaluation order.
                    self.next_parent = parent;
                }
                break;
            }
        }
        if self.recipe.operation.decisive().is_some() && self.recipe.speculative {
            for (parent, span) in &runs {
                let row = &mut self.parents[*parent];
                // Grow from the actual admitted window. Once the per-request
                // share fills it, the same limit applies on the next handoff.
                row.match_window = span.len().saturating_mul(2).min(BATCH_ROWS);
                work.step()?;
            }
        }
        // The rotating scheduler grants each active row a turn. Publication
        // always groups those rows in the original outer Selection order.
        if let Some(wrap) = parent_wrap(runs.iter().map(|(parent, _)| *parent), first_parent, work)?
        {
            runs.rotate_left(wrap);
            work.flush()?;
        }
        let mut batch = LambdaBodyBatchBuilder::try_new(
            &self.storage.host,
            self.columns.batch_sources(),
            capacity,
        )?;
        for (parent, rows) in runs {
            for position in rows {
                work.step()?;
                let positions = [position, position];
                batch.push(parent, &positions[..self.columns.batch_sources().len()])?;
            }
        }
        Ok(batch.finish())
    }

    fn consume(
        &mut self,
        response: &LambdaBodyBatchOutput<'input>,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        let operation = self.recipe.operation;
        let values = response.values();
        let predicate = if operation.transforms() {
            None
        } else {
            Some(
                values
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        internal("collection predicate differs from its Boolean declaration")
                    })?,
            )
        };
        let mut errors = response.errors().iter().enumerate().peekable();
        for (ordinal, &parent) in response.batch().parent_ordinals().iter().enumerate() {
            work.step()?;
            let row = &mut self.parents[parent];
            let error = errors.next_if(|(_, error)| error.selected_ordinal() == ordinal);
            if !row.active() {
                continue;
            }
            let input_position = row.next;
            row.next += 1;
            if let Some((error_index, _)) = error {
                row.fail(required_error(response, error_index)?);
            } else if let Some(decisive) = operation.decisive() {
                let predicate = predicate.expect("prepared match predicate");
                if predicate.is_null(ordinal) {
                    row.saw_null = true;
                } else if predicate.value(ordinal) == decisive {
                    row.decision = Some(decisive);
                    row.next = row.end;
                }
            } else if operation == Operation::TransformKeys
                && logical_is_null(values.as_ref(), ordinal, 1, work)?
            {
                row.fail(bad_argument(parent, "map key cannot be null"));
            } else if predicate
                .is_none_or(|values| !values.is_null(ordinal) && values.value(ordinal))
            {
                let originals = self.columns.batch_sources();
                let (first, first_row) =
                    if matches!(operation, Operation::Transform | Operation::TransformKeys) {
                        (values, ordinal)
                    } else {
                        (&originals[0], input_position)
                    };
                emit(&mut row.pieces[0], first, first_row, &self.storage)?;
                if let [_, original_value] = originals {
                    let (second, second_row) = if operation == Operation::TransformValues {
                        (values, ordinal)
                    } else {
                        (original_value, input_position)
                    };
                    emit(&mut row.pieces[1], second, second_row, &self.storage)?;
                }
                row.length += 1;
            }
            if !row.active() {
                self.remaining -= 1;
                if row.error.is_none()
                    && let Some(keys) = &self.recipe.keys
                    && duplicate(keys, row, &self.storage, work)?
                {
                    row.fail(bad_argument(parent, "Duplicate map keys are not allowed"));
                }
            }
        }
        Ok(())
    }

    fn finish(
        &mut self,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<HigherOrderOutput<'input>, KernelFailure> {
        let ty = self.contract.result_type().data_type();
        let result = if let Some(decisive) = self.recipe.operation.decisive() {
            boolean_output(
                self.parents.iter().map(|row| row.matched(decisive)),
                &self.storage,
                work,
            )?
        } else {
            let mut lengths = self.storage.vec(self.parents.len())?;
            for row in &self.parents {
                lengths.push(row.length());
                work.step()?;
            }
            match ty {
                DataType::List(field) => {
                    let fallback = match self.recipe.operation {
                        Operation::Transform => self.contract.body_result_type().data_type(),
                        _ => self.columns.batch_sources()[0].data_type(),
                    };
                    let (sources, segments) = pieces_for(
                        self.parents
                            .iter()
                            .filter(|row| row.error.is_none())
                            .map(|row| row.pieces[0].as_slice()),
                        fallback,
                        &self.storage,
                        work,
                    )?;
                    list_from_segments_in(
                        field,
                        &sources,
                        &segments,
                        &lengths,
                        &self.storage.host,
                        &self.storage.allocator,
                        work,
                    )?
                }
                DataType::Map(entries, _) => {
                    let DataType::Struct(fields) = entries.data_type() else {
                        return Err(internal("collection MAP lost its original fields"));
                    };
                    let mut count = 0;
                    for length in &lengths {
                        count = crate::aggregate_scalar::checked_map_output_offset(
                            count,
                            length.unwrap_or(0),
                        )?;
                        work.step()?;
                    }
                    let child = |ordinal: usize, work: &mut EvaluationCheckpoints<'_>| {
                        let (sources, segments) = pieces_for(
                            self.parents
                                .iter()
                                .filter(|row| row.error.is_none())
                                .map(|row| row.pieces[ordinal].as_slice()),
                            fields[ordinal].data_type(),
                            &self.storage,
                            work,
                        )?;
                        array_from_segments_in(
                            &sources,
                            &segments,
                            count as usize,
                            &self.storage.host,
                            &self.storage.allocator,
                            work,
                        )
                    };
                    let children = [child(0, work)?, child(1, work)?];
                    map_from_struct_children_in(
                        ty,
                        children,
                        &lengths,
                        &self.storage.host,
                        &self.storage.allocator,
                        work,
                    )?
                }
                _ => return Err(internal("invalid collection lambda result carrier")),
            }
        };
        finish_output(
            self.selection,
            ty,
            result,
            self.parents.iter_mut().filter_map(|row| row.error.take()),
            &self.storage,
            work,
        )
    }
}

fn parent_wrap(
    parents: impl Iterator<Item = usize>,
    first: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<usize>, KernelFailure> {
    for (index, parent) in parents.enumerate() {
        work.step()?;
        if parent < first {
            return Ok(Some(index));
        }
    }
    Ok(None)
}
impl<'input> HigherOrderCall<'input> for Linear<'input> {
    fn advance(
        &mut self,
        completed: Option<LambdaBodyBatchOutput<'input>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<HigherOrderStep<'input>, KernelFailure> {
        let mut work = EvaluationCheckpoints::new(control);
        if let Some(completed) = completed {
            self.consume(&completed, &mut work)?;
        }
        let step = if self.remaining != 0 {
            HigherOrderStep::Request(self.request(&mut work)?)
        } else {
            HigherOrderStep::Complete(self.finish(&mut work)?)
        };
        work.finish()?;
        Ok(step)
    }

    fn instance_retained_bytes(&self) -> usize {
        size_of::<Instance>()
    }
}

fn pieces_for<'a>(
    pieces: impl Iterator<Item = &'a [Piece]>,
    empty_type: &DataType,
    storage: &Storage,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(Scratch<ArrayRef>, Scratch<ExtendSegment>), KernelFailure> {
    let mut sources = storage.vec(0)?;
    let mut segments = storage.vec(0)?;
    let mut indices = storage.index::<usize, usize>();
    for row in pieces {
        work.step()?;
        for piece in row {
            work.step()?;
            let identity = Arc::as_ptr(&piece.source).cast::<()>() as usize;
            let source = if let Some(index) = indices.get(&identity) {
                *index
            } else {
                indices
                    .try_reserve(1)
                    .map_err(|_| storage.allocator.take_failure())?;
                let index = sources.len();
                storage.push(&mut sources, Arc::clone(&piece.source))?;
                indices.insert(identity, index);
                index
            };
            storage.push(
                &mut segments,
                ExtendSegment {
                    source,
                    start: piece.start,
                    len: piece.len,
                    repeats: 1,
                    nulls: 0,
                },
            )?;
        }
    }
    if sources.is_empty() {
        let empty = empty_array_in(empty_type, &storage.host, &storage.allocator, work)?;
        storage.push(&mut sources, empty)?;
    }
    Ok((sources, segments))
}

fn duplicate(
    operations: &PreparedKeyOperations,
    row: &Parent<'_>,
    storage: &Storage,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<bool, KernelFailure> {
    let pieces = &row.pieces[0];
    let mut heads = storage.index::<u64, usize>();
    heads
        .try_reserve(row.length)
        .map_err(|_| storage.allocator.take_failure())?;
    let mut links = storage.vec::<(usize, usize, Option<usize>)>(row.length)?;
    let mut arrays = storage.vec(pieces.len())?;
    let mut observe = |units| {
        for _ in 0..units {
            work.step()?;
        }
        Ok(())
    };
    for piece in pieces {
        arrays.push(
            operations
                .bind_observed(
                    &piece.source,
                    KeyRows::Range(piece.start..piece.start + piece.len),
                    &mut observe,
                )
                .map_err(key_failure)?,
        );
    }
    for (source, piece) in pieces.iter().enumerate() {
        for position in piece.start..piece.start + piece.len {
            observe(1)?;
            let bound = &arrays[source];
            let hash = operations
                .hash_observed(bound, position, &mut observe)
                .map_err(key_failure)?;
            let head = heads.insert(hash, links.len());
            let mut previous = head;
            while let Some(index) = previous {
                let (other_source, other_position, next) = links[index];
                match operations
                    .compare_in_observed(
                        &arrays[other_source],
                        bound,
                        (other_position, position),
                        &storage.allocator,
                        &mut observe,
                    )
                    .map_err(|error| {
                        storage
                            .allocator
                            .take_recorded_failure()
                            .unwrap_or_else(|| key_failure(error))
                    })? {
                    KeyComparison::True => return Ok(true),
                    KeyComparison::False => {}
                    _ => return Err(internal("transformed key identity is not total")),
                }
                previous = next;
            }
            links.push((source, position, head));
        }
    }
    Ok(false)
}

fn key_failure(error: ValueFailure<KernelFailure>) -> KernelFailure {
    match error {
        ValueFailure::Value(error) => KernelFailure::from_domain(error),
        ValueFailure::Caller(error) => error,
    }
}

fn boolean_output(
    output: impl ExactSizeIterator<Item = Option<bool>>,
    storage: &Storage,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    let rows = output.len();
    let metadata = custody_type_metadata_upper_bound(1)?;
    let size = rows
        .div_ceil(8)
        .checked_next_multiple_of(64)
        .and_then(|bytes| bytes.checked_mul(2))
        .and_then(|bytes| bytes.checked_add(metadata))
        .ok_or(KernelFailure::ResourceExhausted)?;
    work.flush()?;
    let charge = OpaqueRetainedCharge::try_new(Arc::clone(&storage.host))?;
    let mut reservation = charge.reserve_operation(size)?;
    work.flush()?;
    let mut builder = BooleanBuilder::with_capacity(rows);
    for value in output {
        work.step()?;
        builder.append_option(value);
    }
    work.flush()?;
    Ok(retain_result_backing_direct(
        Arc::new(builder.finish()),
        charge,
        &mut reservation,
        storage.allocator.clone(),
        work,
    )?
    .values)
}

fn finish_output<'input>(
    selection: Selection<'input>,
    result_type: &DataType,
    result: ArrayRef,
    journals: impl Iterator<Item = ParentError<'input>>,
    storage: &Storage,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<HigherOrderOutput<'input>, KernelFailure> {
    let mut errors = storage.vec(0)?;
    let mut required = storage.vec(0)?;
    for error in journals {
        work.step()?;
        match error {
            ParentError::Own(error) => storage.push(&mut errors, error)?,
            ParentError::Required(error) => {
                storage.push(&mut errors, error.error().clone())?;
                storage.push(&mut required, error)?;
            }
        }
    }
    let error_size = size_of::<RowDataError>()
        .checked_mul(errors.len())
        .and_then(|bytes| {
            size_of::<RequiredLambdaBodyError<'_>>()
                .checked_mul(required.len())
                .and_then(|evidence_size| bytes.checked_add(evidence_size))
        })
        .ok_or(KernelFailure::ResourceExhausted)?;
    let _journals = storage.reserve(error_size)?;
    let mut final_errors = Vec::new();
    let mut final_required = Vec::new();
    final_errors
        .try_reserve_exact(errors.len())
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    final_required
        .try_reserve_exact(required.len())
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    for error in errors {
        work.step()?;
        final_errors.push(error);
    }
    for error in required {
        work.step()?;
        final_required.push(error);
    }
    let values = SelectedValues::try_new_observed(
        selection,
        result_type,
        result,
        final_errors.into_boxed_slice(),
        || work.step(),
    )?;
    Ok(HigherOrderOutput::new(
        values,
        final_required.into_boxed_slice(),
    ))
}
