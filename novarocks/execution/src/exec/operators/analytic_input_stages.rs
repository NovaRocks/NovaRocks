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
//! ONE original analytic gathering, regrouping and output splitting.
//! Scope ownership is supplied by the caller. Observing a source or a Layout
//! grants no capacity; the legacy adapter supplies only its original lifetime.
use super::analytic_failure;
use super::{ArrayRef, Chunk, ChunkSchemaRef, Ordering, UInt32Builder, compare_at, take};
use crate::runtime::task::{ExecutionFailure, ExecutionResult};
use arrow::array::{Array, RecordBatch, RecordBatchOptions, UInt32Array};
use arrow::compute::concat;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
#[cfg(test)]
use std::collections::VecDeque;
use std::{convert::Infallible, sync::Arc};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AnalyticInputStage {
    Gather,
    Regroup,
    #[cfg(test)]
    OutputSplit,
}

pub(crate) enum AnalyticInputSource<'a> {
    Gather {
        #[cfg(test)]
        chunks: &'a [Chunk],
        #[cfg(not(test))]
        _lifetime: std::marker::PhantomData<&'a ()>,
    },
    Regroup {
        #[cfg(test)]
        chunk: &'a Chunk,
        #[cfg(test)]
        keys: &'a [ArrayRef],
    },
    #[cfg(test)]
    OutputSplit {
        output: &'a ChunkSchemaRef,
        columns: &'a [ArrayRef],
        chunks: &'a [Chunk],
    },
}
#[cfg(test)]
impl AnalyticInputSource<'_> {
    pub(crate) fn stage(&self) -> AnalyticInputStage {
        match self {
            Self::Gather { .. } => AnalyticInputStage::Gather,
            Self::Regroup { .. } => AnalyticInputStage::Regroup,
            Self::OutputSplit { .. } => AnalyticInputStage::OutputSplit,
        }
    }
}
/// Preserve the original Arrow or domain error across the actual copy occurrence.
pub(crate) enum AnalyticCopyFailure<E> {
    OriginalArrow {
        source: ArrowError,
        _marker: std::marker::PhantomData<fn() -> E>,
    },
    #[cfg(test)]
    Control(E),
}
impl<E> AnalyticCopyFailure<E> {
    pub(crate) fn original_arrow(source: ArrowError) -> Self {
        Self::OriginalArrow {
            source,
            _marker: std::marker::PhantomData,
        }
    }
}

/// Keep a constructor failure distinct from a Work refusal. Each retains its
/// own domain facts; the stage adds only operation context.
pub(crate) type AnalyticChunkConstruction<E> = Result<Chunk, Result<ExecutionFailure, E>>;

pub(crate) type AnalyticStageOperationResult<W> = Result<
    AnalyticStageResult<Chunk, <W as AnalyticInputWork>::Scope>,
    AnalyticStageFailure<<W as AnalyticInputWork>::Scope, <W as AnalyticInputWork>::Failure>,
>;

/// Admission precedes the original stage's allocation. A tracked adapter must
/// return its real host lease, not an observation token. Success and original
/// operation failure retain the scope until their owned payload is dropped.
/// These stages have no window function ordinal.
pub(crate) trait AnalyticInputWork {
    type Failure;
    type Scope;
    fn admit(&mut self, source: AnalyticInputSource<'_>) -> Result<Self::Scope, Self::Failure>;
    fn step(&mut self) -> Result<(), Self::Failure>;
    fn boundary(&mut self) -> Result<(), Self::Failure>;

    /// ONE original column-copy occurrence. The tracked adapter may replace
    /// admission/custody, never the concat value operation or batch iteration.
    fn concat_column(
        &mut self,
        _scope: &mut Self::Scope,
        batches: &[&RecordBatch],
        _sources: &[Chunk],
        ordinal: usize,
    ) -> Result<ArrayRef, AnalyticCopyFailure<Self::Failure>> {
        concat(
            &batches
                .iter()
                .map(|batch| batch.column(ordinal).as_ref())
                .collect::<Vec<_>>(),
        )
        .map_err(AnalyticCopyFailure::original_arrow)
    }
    fn take_column(
        &mut self,
        _scope: &mut Self::Scope,
        source: &Chunk,
        ordinal: usize,
        indices: &Arc<UInt32Array>,
    ) -> Result<ArrayRef, AnalyticCopyFailure<Self::Failure>> {
        take(
            source.record_batch().column(ordinal).as_ref(),
            indices.as_ref(),
            None,
        )
        .map_err(AnalyticCopyFailure::original_arrow)
    }
    #[cfg(test)]
    fn slice_column(
        &mut self,
        _scope: &mut Self::Scope,
        source: &ArrayRef,
        offset: usize,
        len: usize,
    ) -> Result<ArrayRef, Self::Failure> {
        Ok(source.slice(offset, len))
    }

    /// ONE original batch-based Chunk construction, with its exact source schema.
    fn construct_batch_chunk(
        &mut self,
        _scope: &mut Self::Scope,
        batch: RecordBatch,
        schema: ChunkSchemaRef,
    ) -> AnalyticChunkConstruction<Self::Failure> {
        Chunk::try_new_with_chunk_schema(batch, schema)
            .map_err(|source| Ok(ExecutionFailure::from(source)))
    }

    /// ONE original actual-column construction, including reconciliation/retag.
    fn construct_columns_chunk(
        &mut self,
        _scope: &mut Self::Scope,
        schema: ChunkSchemaRef,
        columns: Vec<ArrayRef>,
    ) -> AnalyticChunkConstruction<Self::Failure> {
        Chunk::try_new_with_columns(schema, columns)
            .map_err(|source| Ok(ExecutionFailure::from(source)))
    }
}
#[derive(Debug)]
pub(crate) struct AnalyticStageResult<T, S> {
    #[cfg(test)]
    pub stage: AnalyticInputStage,
    pub value: T,
    _scope: S,
}
#[derive(Debug)]
pub(crate) enum AnalyticStageFailure<S, E> {
    Control {
        #[cfg(test)]
        stage: AnalyticInputStage,
        cause: E,
    },
    Operation {
        #[cfg(test)]
        stage: AnalyticInputStage,
        cause: ExecutionFailure,
        _scope: S,
    },
}
fn controlled<S, E>(_stage: AnalyticInputStage, cause: E) -> AnalyticStageFailure<S, E> {
    AnalyticStageFailure::Control {
        #[cfg(test)]
        stage: _stage,
        cause,
    }
}

pub(crate) struct UnscopedWork;
impl AnalyticInputWork for UnscopedWork {
    type Failure = Infallible;
    type Scope = ();
    fn admit(&mut self, _: AnalyticInputSource<'_>) -> Result<(), Infallible> {
        Ok(())
    }
    fn step(&mut self) -> Result<(), Infallible> {
        Ok(())
    }
    fn boundary(&mut self) -> Result<(), Infallible> {
        Ok(())
    }
}
pub(crate) fn unscoped<T>(
    result: Result<AnalyticStageResult<T, ()>, AnalyticStageFailure<(), Infallible>>,
) -> ExecutionResult<T> {
    match result {
        Ok(result) => Ok(result.value),
        Err(AnalyticStageFailure::Operation { cause, .. }) => Err(cause),
        Err(AnalyticStageFailure::Control { cause, .. }) => match cause {},
    }
}

/// The original pinned Arrow 58.2 concat_batches body, parameterized only at
/// its original per-column concat occurrence. Empty-schema row-count, source
/// schema disregard, column order and final RecordBatch validator are unchanged.
fn original_concat_batches<W: AnalyticInputWork>(
    schema: &SchemaRef,
    input_batches: &[&RecordBatch],
    sources: &[Chunk],
    scope: &mut W::Scope,
    work: &mut W,
) -> Result<RecordBatch, AnalyticCopyFailure<W::Failure>> {
    if schema.fields().is_empty() {
        let num_rows: usize = input_batches.iter().map(|batch| batch.num_rows()).sum();
        let mut options = RecordBatchOptions::default();
        options.row_count = Some(num_rows);
        return RecordBatch::try_new_with_options(schema.clone(), vec![], &options)
            .map_err(AnalyticCopyFailure::original_arrow);
    }
    let batches = input_batches;
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }
    let field_num = schema.fields().len();
    let mut arrays = Vec::with_capacity(field_num);
    for i in 0..field_num {
        let array = work.concat_column(scope, batches, sources, i)?;
        arrays.push(array);
    }
    RecordBatch::try_new(schema.clone(), arrays).map_err(AnalyticCopyFailure::original_arrow)
}

pub(crate) fn gather<W: AnalyticInputWork>(
    input: &[Chunk],
    work: &mut W,
) -> AnalyticStageOperationResult<W> {
    let stage = AnalyticInputStage::Gather;
    let mut scope = work
        .admit(AnalyticInputSource::Gather {
            #[cfg(test)]
            chunks: input,
            #[cfg(not(test))]
            _lifetime: std::marker::PhantomData,
        })
        .map_err(|e| controlled(stage, e))?;
    let result = if input.len() == 1 {
        Ok(input[0].clone())
    } else {
        // Original empty-input index panic remains the caller's nonempty proof.
        let input_schema = input[0].schema();
        work.boundary().map_err(|e| controlled(stage, e))?;
        let mut batches = Vec::with_capacity(input.len());
        for chunk in input {
            work.step().map_err(|e| controlled(stage, e))?;
            batches.push(chunk.record_batch());
        }
        work.boundary().map_err(|e| controlled(stage, e))?;
        match original_concat_batches(&input_schema, &batches, input, &mut scope, work) {
            #[cfg(test)]
            Err(AnalyticCopyFailure::Control(cause)) => return Err(controlled(stage, cause)),
            Err(AnalyticCopyFailure::OriginalArrow { source, .. }) => Err(analytic_failure::stage(
                "concat_batches",
                analytic_failure::arrow(source),
            )),
            Ok(batch) => {
                work.boundary().map_err(|e| controlled(stage, e))?;
                match work.construct_batch_chunk(&mut scope, batch, input[0].chunk_schema_ref()) {
                    Ok(value) => Ok(value),
                    Err(Ok(error)) => Err(analytic_failure::stage(
                        "build analytic concat chunk",
                        error,
                    )),
                    Err(Err(cause)) => return Err(controlled(stage, cause)),
                }
            }
        }
    };
    match result {
        Ok(value) => {
            work.boundary().map_err(|e| controlled(stage, e))?;
            Ok(AnalyticStageResult {
                #[cfg(test)]
                stage,
                value,
                _scope: scope,
            })
        }
        Err(cause) => Err(AnalyticStageFailure::Operation {
            #[cfg(test)]
            stage,
            cause,
            _scope: scope,
        }),
    }
}

// ONE original key loop. The parent legacy comparator delegates here too.
// The original comparator, NULL order and key order are unchanged.
pub(crate) fn compare_keys<W: AnalyticInputWork>(
    keys: &[ArrayRef],
    left: usize,
    right: usize,
    work: &mut W,
) -> Result<Ordering, Result<ExecutionFailure, W::Failure>> {
    for array in keys {
        work.step().map_err(Err)?;
        match (array.is_null(left), array.is_null(right)) {
            (true, true) => continue,
            (true, false) => return Ok(Ordering::Less),
            (false, true) => return Ok(Ordering::Greater),
            (false, false) => {
                work.boundary().map_err(Err)?;
                let order = compare_at(array.as_ref(), left, right).map_err(Ok)?;
                work.boundary().map_err(Err)?;
                if !order.is_eq() {
                    return Ok(order);
                }
            }
        }
    }
    Ok(Ordering::Equal)
}
#[cfg(test)]
pub(crate) fn unscoped_compare(
    keys: &[ArrayRef],
    left: usize,
    right: usize,
) -> ExecutionResult<Ordering> {
    match compare_keys(keys, left, right, &mut UnscopedWork) {
        Ok(order) => Ok(order),
        Err(Ok(message)) => Err(message),
        Err(Err(cause)) => match cause {},
    }
}

pub(crate) fn regroup<W: AnalyticInputWork>(
    chunk: &Chunk,
    keys: &[ArrayRef],
    work: &mut W,
) -> AnalyticStageOperationResult<W> {
    let stage = AnalyticInputStage::Regroup;
    let mut scope = work
        .admit(AnalyticInputSource::Regroup {
            #[cfg(test)]
            chunk,
            #[cfg(test)]
            keys,
        })
        .map_err(|e| controlled(stage, e))?;
    let rows = chunk.len();
    if rows <= 1 || keys.is_empty() {
        work.boundary().map_err(|e| controlled(stage, e))?;
        return Ok(AnalyticStageResult {
            #[cfg(test)]
            stage,
            value: chunk.clone(),
            _scope: scope,
        });
    }
    work.boundary().map_err(|e| controlled(stage, e))?;
    let mut perm = Vec::with_capacity(rows);
    for row in 0..rows {
        work.step().map_err(|e| controlled(stage, e))?;
        perm.push(row);
    }
    let mut sort_error: Option<Result<ExecutionFailure, W::Failure>> = None;
    work.boundary().map_err(|e| controlled(stage, e))?;
    perm.sort_by(|left, right| {
        if sort_error.is_some() {
            return Ordering::Equal;
        }
        match compare_keys(keys, *left, *right, work) {
            Ok(order) => {
                if order.is_eq() {
                    left.cmp(right)
                } else {
                    order
                }
            }
            Err(error) => {
                sort_error = Some(error);
                Ordering::Equal
            }
        }
    });
    if let Some(error) = sort_error {
        return Err(match error {
            Ok(cause) => AnalyticStageFailure::Operation {
                #[cfg(test)]
                stage,
                cause,
                _scope: scope,
            },
            Err(cause) => controlled(stage, cause),
        });
    }
    work.boundary().map_err(|e| controlled(stage, e))?;
    let mut unchanged = true;
    for (index, row) in perm.iter().enumerate() {
        work.step().map_err(|e| controlled(stage, e))?;
        if index != *row {
            unchanged = false;
            break;
        }
    }
    if unchanged {
        work.boundary().map_err(|e| controlled(stage, e))?;
        return Ok(AnalyticStageResult {
            #[cfg(test)]
            stage,
            value: chunk.clone(),
            _scope: scope,
        });
    }
    work.boundary().map_err(|e| controlled(stage, e))?;
    let mut indices = UInt32Builder::with_capacity(rows);
    for row in perm {
        work.step().map_err(|e| controlled(stage, e))?;
        let index = match analytic_failure::row_index(row) {
            Ok(index) => index,
            Err(cause) => {
                return Err(AnalyticStageFailure::Operation {
                    #[cfg(test)]
                    stage,
                    cause,
                    _scope: scope,
                });
            }
        };
        indices.append_value(index);
    }
    work.boundary().map_err(|e| controlled(stage, e))?;
    let indices = Arc::new(indices.finish());
    work.boundary().map_err(|e| controlled(stage, e))?;
    let mut columns = Vec::with_capacity(chunk.record_batch().num_columns());
    for ordinal in 0..chunk.record_batch().num_columns() {
        work.boundary().map_err(|e| controlled(stage, e))?;
        let value = match work.take_column(&mut scope, chunk, ordinal, &indices) {
            Ok(value) => value,
            #[cfg(test)]
            Err(AnalyticCopyFailure::Control(cause)) => return Err(controlled(stage, cause)),
            Err(AnalyticCopyFailure::OriginalArrow { source, .. }) => {
                return Err(AnalyticStageFailure::Operation {
                    #[cfg(test)]
                    stage,
                    cause: analytic_failure::arrow(source),
                    _scope: scope,
                });
            }
        };
        work.boundary().map_err(|e| controlled(stage, e))?;
        columns.push(value);
    }
    work.boundary().map_err(|e| controlled(stage, e))?;
    match work.construct_columns_chunk(&mut scope, chunk.chunk_schema_ref(), columns) {
        Ok(value) => {
            work.boundary().map_err(|e| controlled(stage, e))?;
            Ok(AnalyticStageResult {
                #[cfg(test)]
                stage,
                value,
                _scope: scope,
            })
        }
        Err(Ok(error)) => Err(AnalyticStageFailure::Operation {
            #[cfg(test)]
            stage,
            cause: analytic_failure::stage("build reordered analytic batch", error),
            _scope: scope,
        }),
        Err(Err(cause)) => Err(controlled(stage, cause)),
    }
}

#[cfg(test)]
pub(crate) type AnalyticSplitResult<W> = Result<
    AnalyticStageResult<VecDeque<Chunk>, <W as AnalyticInputWork>::Scope>,
    AnalyticStageFailure<<W as AnalyticInputWork>::Scope, <W as AnalyticInputWork>::Failure>,
>;

#[cfg(test)]
pub(crate) fn split<W: AnalyticInputWork>(
    output: ChunkSchemaRef,
    columns: &[ArrayRef],
    input: &[Chunk],
    work: &mut W,
) -> AnalyticSplitResult<W> {
    let stage = AnalyticInputStage::OutputSplit;
    let mut scope = work
        .admit(AnalyticInputSource::OutputSplit {
            output: &output,
            columns,
            chunks: input,
        })
        .map_err(|e| controlled(stage, e))?;
    let mut value = VecDeque::new();
    let mut offset = 0usize;
    for chunk in input {
        work.step().map_err(|e| controlled(stage, e))?;
        let len = chunk.len();
        if len == 0 {
            continue;
        }
        work.boundary().map_err(|e| controlled(stage, e))?;
        let mut slices = Vec::with_capacity(columns.len());
        for column in columns {
            work.boundary().map_err(|e| controlled(stage, e))?;
            slices.push(
                work.slice_column(&mut scope, column, offset, len)
                    .map_err(|cause| controlled(stage, cause))?,
            );
            work.step().map_err(|e| controlled(stage, e))?;
        }
        work.boundary().map_err(|e| controlled(stage, e))?;
        match work.construct_columns_chunk(&mut scope, Arc::clone(&output), slices) {
            Ok(chunk) => {
                work.boundary().map_err(|e| controlled(stage, e))?;
                value.push_back(chunk);
            }
            Err(Ok(error)) => {
                return Err(AnalyticStageFailure::Operation {
                    #[cfg(test)]
                    stage,
                    cause: analytic_failure::stage("build analytic output batch", error),
                    _scope: scope,
                });
            }
            Err(Err(cause)) => return Err(controlled(stage, cause)),
        }
        offset += len;
    }
    work.boundary().map_err(|e| controlled(stage, e))?;
    Ok(AnalyticStageResult {
        #[cfg(test)]
        stage,
        value,
        _scope: scope,
    })
}
