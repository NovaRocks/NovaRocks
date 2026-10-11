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
use super::{ArrayRef, Chunk, ChunkSchemaRef, Ordering, UInt32Builder, compare_at, take};
use arrow::array::{Array, RecordBatch, RecordBatchOptions, UInt32Array};
use arrow::compute::concat;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use std::{collections::VecDeque, convert::Infallible, sync::Arc};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AnalyticInputStage {
    Gather,
    Regroup,
    OutputSplit,
}

pub(crate) enum AnalyticInputSource<'a> {
    Gather {
        chunks: &'a [Chunk],
    },
    Regroup {
        chunk: &'a Chunk,
        keys: &'a [ArrayRef],
    },
    OutputSplit {
        output: &'a ChunkSchemaRef,
        columns: &'a [ArrayRef],
        chunks: &'a [Chunk],
    },
}
impl AnalyticInputSource<'_> {
    pub(crate) fn stage(&self) -> AnalyticInputStage {
        match self {
            Self::Gather { .. } => AnalyticInputStage::Gather,
            Self::Regroup { .. } => AnalyticInputStage::Regroup,
            Self::OutputSplit { .. } => AnalyticInputStage::OutputSplit,
        }
    }
}

/// Preserve the original ArrowError until its original stage formatter. The
/// tracked adapter supplies already-owned original text under its scope grant.
pub(crate) enum AnalyticCopyFailure<E> {
    OriginalArrow(ArrowError),
    OriginalText(String),
    Control(E),
}

/// Keep the original constructor String distinct from a typed Work refusal.
/// The original stage remains the sole author of its diagnostic prefix.
pub(crate) type AnalyticChunkConstruction<E> = Result<Chunk, Result<String, E>>;

/// Admission precedes the original stage's allocation. A tracked adapter must
/// return its real host lease, not an observation token. Success and original
/// whole-stage Data retain the scope until their owned payload is dropped.
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
        .map_err(AnalyticCopyFailure::OriginalArrow)
    }
    fn take_column(
        &mut self,
        _scope: &mut Self::Scope,
        source: &Chunk,
        ordinal: usize,
        indices: &Arc<UInt32Array>,
    ) -> Result<ArrayRef, AnalyticCopyFailure<Self::Failure>> {
        take(
            source.batch.column(ordinal).as_ref(),
            indices.as_ref(),
            None,
        )
        .map_err(AnalyticCopyFailure::OriginalArrow)
    }
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
        Chunk::try_new_with_chunk_schema(batch, schema).map_err(Ok)
    }

    /// ONE original actual-column construction, including reconciliation/retag.
    fn construct_columns_chunk(
        &mut self,
        _scope: &mut Self::Scope,
        schema: ChunkSchemaRef,
        columns: Vec<ArrayRef>,
    ) -> AnalyticChunkConstruction<Self::Failure> {
        Chunk::try_new_with_columns(schema, columns).map_err(Ok)
    }
}
#[derive(Debug)]
pub(crate) struct AnalyticStageResult<T, S> {
    pub stage: AnalyticInputStage,
    // Field order keeps the payload alive under its scope until payload Drop.
    pub value: T,
    pub scope: S,
}
#[derive(Debug)]
pub(crate) enum AnalyticStageFailure<S, E> {
    Control {
        stage: AnalyticInputStage,
        cause: E,
    },
    OriginalData {
        stage: AnalyticInputStage,
        message: String,
        scope: S,
    },
}
fn controlled<S, E>(stage: AnalyticInputStage, cause: E) -> AnalyticStageFailure<S, E> {
    AnalyticStageFailure::Control { stage, cause }
}

pub(crate) struct LegacyWork;
impl AnalyticInputWork for LegacyWork {
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
pub(crate) fn legacy<T>(
    result: Result<AnalyticStageResult<T, ()>, AnalyticStageFailure<(), Infallible>>,
) -> Result<T, String> {
    match result {
        Ok(result) => Ok(result.value),
        Err(AnalyticStageFailure::OriginalData { message, .. }) => Err(message),
        Err(AnalyticStageFailure::Control { cause, .. }) => match cause {},
    }
}

/// The original pinned Arrow 58.2 concat_batches body, parameterized only at
/// its original per-column concat occurrence. Empty-schema row-count, source
/// schema disregard, column order and final RecordBatch validator are unchanged.
fn original_concat_batches<W: AnalyticInputWork>(
    schema: &SchemaRef,
    input_batches: &[RecordBatch],
    sources: &[Chunk],
    scope: &mut W::Scope,
    work: &mut W,
) -> Result<RecordBatch, AnalyticCopyFailure<W::Failure>> {
    if schema.fields().is_empty() {
        let num_rows: usize = input_batches.iter().map(RecordBatch::num_rows).sum();
        let mut options = RecordBatchOptions::default();
        options.row_count = Some(num_rows);
        return RecordBatch::try_new_with_options(schema.clone(), vec![], &options)
            .map_err(AnalyticCopyFailure::OriginalArrow);
    }
    let batches: Vec<&RecordBatch> = input_batches.iter().collect();
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }
    let field_num = schema.fields().len();
    let mut arrays = Vec::with_capacity(field_num);
    for i in 0..field_num {
        let array = work.concat_column(scope, &batches, sources, i)?;
        arrays.push(array);
    }
    RecordBatch::try_new(schema.clone(), arrays).map_err(AnalyticCopyFailure::OriginalArrow)
}

pub(crate) fn gather<W: AnalyticInputWork>(
    input: &[Chunk],
    work: &mut W,
) -> Result<AnalyticStageResult<Chunk, W::Scope>, AnalyticStageFailure<W::Scope, W::Failure>> {
    let stage = AnalyticInputStage::Gather;
    let mut scope = work
        .admit(AnalyticInputSource::Gather { chunks: input })
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
            batches.push(chunk.batch.clone());
        }
        work.boundary().map_err(|e| controlled(stage, e))?;
        match original_concat_batches(&input_schema, &batches, input, &mut scope, work) {
            Err(AnalyticCopyFailure::Control(cause)) => return Err(controlled(stage, cause)),
            Err(AnalyticCopyFailure::OriginalArrow(error)) => {
                Err(format!("concat_batches: {}", error))
            }
            Err(AnalyticCopyFailure::OriginalText(error)) => {
                Err(format!("concat_batches: {}", error))
            }
            Ok(batch) => {
                work.boundary().map_err(|e| controlled(stage, e))?;
                match work.construct_batch_chunk(&mut scope, batch, input[0].chunk_schema_ref()) {
                    Ok(value) => Ok(value),
                    Err(Ok(error)) => Err(format!("build analytic concat chunk: {error}")),
                    Err(Err(cause)) => return Err(controlled(stage, cause)),
                }
            }
        }
    };
    match result {
        Ok(value) => {
            work.boundary().map_err(|e| controlled(stage, e))?;
            Ok(AnalyticStageResult {
                stage,
                value,
                scope,
            })
        }
        Err(message) => Err(AnalyticStageFailure::OriginalData {
            stage,
            message,
            scope,
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
) -> Result<Ordering, Result<String, W::Failure>> {
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
pub(crate) fn legacy_compare(
    keys: &[ArrayRef],
    left: usize,
    right: usize,
) -> Result<Ordering, String> {
    match compare_keys(keys, left, right, &mut LegacyWork) {
        Ok(order) => Ok(order),
        Err(Ok(message)) => Err(message),
        Err(Err(cause)) => match cause {},
    }
}

pub(crate) fn regroup<W: AnalyticInputWork>(
    chunk: &Chunk,
    keys: &[ArrayRef],
    work: &mut W,
) -> Result<AnalyticStageResult<Chunk, W::Scope>, AnalyticStageFailure<W::Scope, W::Failure>> {
    let stage = AnalyticInputStage::Regroup;
    let mut scope = work
        .admit(AnalyticInputSource::Regroup { chunk, keys })
        .map_err(|e| controlled(stage, e))?;
    let rows = chunk.len();
    if rows <= 1 || keys.is_empty() {
        work.boundary().map_err(|e| controlled(stage, e))?;
        return Ok(AnalyticStageResult {
            stage,
            value: chunk.clone(),
            scope,
        });
    }
    work.boundary().map_err(|e| controlled(stage, e))?;
    let mut perm = Vec::with_capacity(rows);
    for row in 0..rows {
        work.step().map_err(|e| controlled(stage, e))?;
        perm.push(row);
    }
    let mut sort_error: Option<Result<String, W::Failure>> = None;
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
            Ok(message) => AnalyticStageFailure::OriginalData {
                stage,
                message,
                scope,
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
            stage,
            value: chunk.clone(),
            scope,
        });
    }
    work.boundary().map_err(|e| controlled(stage, e))?;
    let mut indices = UInt32Builder::with_capacity(rows);
    for row in perm {
        work.step().map_err(|e| controlled(stage, e))?;
        // Preserve the original unchecked UInt32 conversion.
        indices.append_value(row as u32);
    }
    work.boundary().map_err(|e| controlled(stage, e))?;
    let indices = Arc::new(indices.finish());
    work.boundary().map_err(|e| controlled(stage, e))?;
    let mut columns = Vec::with_capacity(chunk.batch.num_columns());
    for ordinal in 0..chunk.batch.num_columns() {
        work.boundary().map_err(|e| controlled(stage, e))?;
        let value = match work.take_column(&mut scope, chunk, ordinal, &indices) {
            Ok(value) => value,
            Err(AnalyticCopyFailure::Control(cause)) => return Err(controlled(stage, cause)),
            Err(AnalyticCopyFailure::OriginalArrow(error)) => {
                return Err(AnalyticStageFailure::OriginalData {
                    stage,
                    message: error.to_string(),
                    scope,
                });
            }
            Err(AnalyticCopyFailure::OriginalText(message)) => {
                return Err(AnalyticStageFailure::OriginalData {
                    stage,
                    message,
                    scope,
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
                stage,
                value,
                scope,
            })
        }
        Err(Ok(error)) => Err(AnalyticStageFailure::OriginalData {
            stage,
            message: format!("build reordered analytic batch: {}", error),
            scope,
        }),
        Err(Err(cause)) => Err(controlled(stage, cause)),
    }
}

pub(crate) fn split<W: AnalyticInputWork>(
    output: ChunkSchemaRef,
    columns: &[ArrayRef],
    input: &[Chunk],
    work: &mut W,
) -> Result<
    AnalyticStageResult<VecDeque<Chunk>, W::Scope>,
    AnalyticStageFailure<W::Scope, W::Failure>,
> {
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
                return Err(AnalyticStageFailure::OriginalData {
                    stage,
                    message: format!("build analytic output batch: {}", error),
                    scope,
                });
            }
            Err(Err(cause)) => return Err(controlled(stage, cause)),
        }
        offset += len;
    }
    work.boundary().map_err(|e| controlled(stage, e))?;
    Ok(AnalyticStageResult {
        stage,
        value,
        scope,
    })
}
