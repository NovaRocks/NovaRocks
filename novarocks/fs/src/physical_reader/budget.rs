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

use std::collections::VecDeque;

use std::sync::Arc;

use arrow::array::{Array, UInt64Array};
use arrow::compute::take;
use arrow::record_batch::RecordBatch;

use crate::{
    FileBatch, FileBatchReader, FileError, FileErrorKind, FileMetricsSnapshot, FileReadBudget,
    FileResult,
};

/// Splits decoded batches to a read budget, keeping each remainder for the
/// next call. Shared by the blocking and the awaited reader.
pub(crate) struct BudgetSplitter {
    budget: FileReadBudget,
    pending: VecDeque<FileBatch>,
}

impl BudgetSplitter {
    pub(crate) fn new(budget: FileReadBudget) -> Self {
        Self {
            budget,
            pending: VecDeque::new(),
        }
    }

    pub(crate) fn take_pending(&mut self) -> Option<FileBatch> {
        self.pending.pop_front()
    }

    pub(crate) fn clear(&mut self) {
        self.pending.clear();
    }

    pub(crate) fn split(&mut self, batch: FileBatch) -> FileResult<FileBatch> {
        let max_rows = self.budget.max_rows.get();
        let max_bytes = self.budget.max_bytes.get();
        let row_count = batch.batch.num_rows();
        if row_count == 0 {
            return Ok(batch);
        }

        let take = if row_count > max_rows {
            max_rows
        } else if batch.batch.get_array_memory_size() <= max_bytes {
            row_count
        } else {
            largest_fitting_prefix(&batch, max_bytes)?
        };

        if take == row_count {
            return Ok(batch);
        }
        let remainder = slice_file_batch(&batch, take, row_count - take)?;
        self.pending.push_front(remainder);
        slice_file_batch(&batch, 0, take)
    }
}

pub(crate) struct BudgetedFileReader {
    inner: Box<dyn FileBatchReader>,
    splitter: BudgetSplitter,
    closed: bool,
}

impl BudgetedFileReader {
    pub(crate) fn new(inner: Box<dyn FileBatchReader>, budget: FileReadBudget) -> Self {
        Self {
            inner,
            splitter: BudgetSplitter::new(budget),
            closed: false,
        }
    }
}

impl FileBatchReader for BudgetedFileReader {
    fn next_batch(&mut self) -> FileResult<Option<FileBatch>> {
        if self.closed {
            return Ok(None);
        }
        loop {
            let batch = match self.splitter.take_pending() {
                Some(batch) => Some(batch),
                None => self.inner.next_batch()?,
            };
            let Some(batch) = batch else {
                self.close()?;
                return Ok(None);
            };
            if batch.batch.num_rows() == 0 {
                continue;
            }
            return self.splitter.split(batch).map(Some);
        }
    }

    fn close(&mut self) -> FileResult<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.splitter.clear();
        self.inner.close()
    }

    fn metrics_snapshot(&self) -> FileMetricsSnapshot {
        self.inner.metrics_snapshot()
    }
}

impl Drop for BudgetedFileReader {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

fn largest_fitting_prefix(batch: &FileBatch, max_bytes: usize) -> FileResult<usize> {
    if slice_file_batch(batch, 0, 1)?.batch.get_array_memory_size() > max_bytes {
        return Err(FileError::new(
            FileErrorKind::ResourceExhausted,
            format!("one physical row exceeds file batch byte budget of {max_bytes} bytes"),
        ));
    }
    let mut low = 1usize;
    let mut high = batch.batch.num_rows();
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if slice_file_batch(batch, 0, middle)?
            .batch
            .get_array_memory_size()
            <= max_bytes
        {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    Ok(low)
}

fn slice_file_batch(batch: &FileBatch, offset: usize, length: usize) -> FileResult<FileBatch> {
    let indices = UInt64Array::from_iter_values(offset as u64..(offset + length) as u64);
    let columns = batch
        .batch
        .columns()
        .iter()
        .map(|column| {
            take(column.as_ref(), &indices, None).map_err(|error| {
                FileError::with_source(
                    FileErrorKind::Internal,
                    "failed to compact file batch to its byte budget",
                    error,
                )
            })
        })
        .collect::<FileResult<Vec<_>>>()?;
    let compacted =
        RecordBatch::try_new(Arc::clone(&batch.batch.schema()), columns).map_err(|error| {
            FileError::with_source(
                FileErrorKind::Internal,
                "failed to build compacted file batch",
                error,
            )
        })?;
    Ok(FileBatch {
        batch: compacted,
        physical_row_positions: batch.physical_row_positions.as_ref().map(|positions| {
            let array = positions.slice(offset, length);
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("UInt64Array slice")
                .clone()
        }),
    })
}
