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

mod budget;
mod chunk_reader;
mod orc;
mod parquet;
mod range_io;

use crate::{
    FileBatch, FileBatchReader, FileError, FileFormat, FileMetricsSnapshot, FileReadRequest,
    FileResult,
};

pub fn open_file_reader(request: FileReadRequest) -> FileResult<Box<dyn FileBatchReader>> {
    open_file_reader_with_parquet_inspection(request, None)
}

/// Open a physical reader using a previously inspected, provenance-bound
/// Parquet footer when one is available for this exact file.
pub fn open_file_reader_with_parquet_inspection(
    request: FileReadRequest,
    inspection: Option<&ParquetMetadataInspection>,
) -> FileResult<Box<dyn FileBatchReader>> {
    request.context.check_active()?;
    let budget = request.budget;
    let reader: Box<dyn FileBatchReader> = match request.format {
        FileFormat::Parquet => Box::new(parquet::ParquetPhysicalReader::try_new(
            request, inspection,
        )?),
        FileFormat::Orc => Box::new(orc::OrcPhysicalReader::try_new(request)?),
    };
    Ok(Box::new(budget::BudgetedFileReader::new(reader, budget)))
}

/// Opens a Parquet reader for a caller that must not block: its footer, page
/// indexes and data ranges are awaited through the source's range service,
/// and the decoder runs on the awaiting task. ORC has no awaited reader.
pub async fn open_file_reader_async(
    request: FileReadRequest,
    inspection: Option<&ParquetMetadataInspection>,
) -> FileResult<AsyncFileBatchReader> {
    request.context.check_active()?;
    if request.format != FileFormat::Parquet {
        return Err(FileError::unsupported(
            "only Parquet files have an awaited physical reader",
        ));
    }
    let budget = request.budget;
    Ok(AsyncFileBatchReader {
        inner: parquet::ParquetPhysicalReader::try_new_async(request, inspection).await?,
        splitter: budget::BudgetSplitter::new(budget),
        closed: false,
    })
}

/// A physical reader whose input is awaited rather than fetched on a
/// blocked thread; the awaited counterpart of a [`FileBatchReader`].
pub struct AsyncFileBatchReader {
    inner: parquet::ParquetPhysicalReader,
    splitter: budget::BudgetSplitter,
    closed: bool,
}

impl AsyncFileBatchReader {
    /// The next batch within the read budget, or `None` at the end.
    pub async fn next_batch(&mut self) -> FileResult<Option<FileBatch>> {
        if self.closed {
            return Ok(None);
        }
        loop {
            let batch = match self.splitter.take_pending() {
                Some(batch) => Some(batch),
                None => self.inner.next_batch_async().await?,
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

    pub fn close(&mut self) -> FileResult<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.splitter.clear();
        self.inner.close()
    }

    pub fn metrics_snapshot(&self) -> FileMetricsSnapshot {
        self.inner.metrics_snapshot()
    }
}

pub use parquet::{
    MAX_PARQUET_INSPECTION_PHYSICAL_COLUMNS, MAX_PARQUET_INSPECTION_ROW_GROUPS,
    MAX_PARQUET_INSPECTION_STATISTIC_CELLS, MAX_PARQUET_INSPECTION_STATISTIC_VALUE_BYTES,
    ParquetColumnStatistics, ParquetMetadataInspection, ParquetPhysicalColumn, ParquetPhysicalType,
    ParquetRowGroupLayout, ParquetStatisticsSortOrder, ParquetStatisticsValue,
    inspect_parquet_metadata, inspect_parquet_metadata_async,
    inspect_parquet_metadata_from_prepared, parquet_footer_range, plan_parquet_input_ranges,
};
