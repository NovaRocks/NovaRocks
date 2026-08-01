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

use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

use orc_rust::arrow_reader::{ArrowReader, ArrowReaderBuilder};
use orc_rust::projection::ProjectionMask;

use super::chunk_reader::{BoundChunkReader, ReaderMetrics};
use crate::{
    FileBatch, FileBatchReader, FileError, FileErrorKind, FileMetricsSnapshot, FileProjection,
    FileReadRange, FileReadRequest, FileResult,
};

pub(crate) struct OrcPhysicalReader {
    reader: Option<ArrowReader<BoundChunkReader>>,
    context: crate::FileReadContext,
    metrics: Arc<ReaderMetrics>,
    closed: bool,
}

impl OrcPhysicalReader {
    pub(crate) fn try_new(request: FileReadRequest) -> FileResult<Self> {
        request.context.check_active()?;
        if !request.predicates.is_empty()
            || request.pruning.row_groups.is_some()
            || !request.pruning.pages.is_empty()
        {
            return Err(FileError::unsupported(
                "ORC physical pruning is not supported by this reader",
            ));
        }
        let metrics = Arc::new(ReaderMetrics::default());
        let chunk_reader = BoundChunkReader::new(
            request.file,
            request.context.clone(),
            request.cache,
            true,
            Arc::clone(&metrics),
        );
        let mut builder = ArrowReaderBuilder::try_new(chunk_reader)
            .map_err(|error| orc_error("open ORC metadata", error))?;
        let projection = projection_mask(&builder, &request.projection)?;
        builder = builder
            .with_projection(projection)
            .with_batch_size(request.budget.max_rows.get());
        if let Some(range) = orc_range(request.range)? {
            builder = builder.with_file_byte_range(range);
        }
        let reader = builder.build();
        Ok(Self {
            reader: Some(reader),
            context: request.context,
            metrics,
            closed: false,
        })
    }
}

impl FileBatchReader for OrcPhysicalReader {
    fn next_batch(&mut self) -> FileResult<Option<FileBatch>> {
        if self.closed {
            return Ok(None);
        }
        self.context.check_active()?;
        let began = Instant::now();
        let next = self
            .reader
            .as_mut()
            .and_then(Iterator::next)
            .transpose()
            .map_err(|error| format_error("decode ORC batch", error))?;
        self.context.check_active()?;
        let Some(batch) = next else {
            self.close()?;
            return Ok(None);
        };
        self.metrics
            .record_decode(batch.num_rows(), began.elapsed().as_nanos());
        self.metrics.record_delivery();
        Ok(Some(FileBatch {
            batch,
            physical_row_positions: None,
        }))
    }

    fn close(&mut self) -> FileResult<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.reader = None;
        Ok(())
    }

    fn metrics_snapshot(&self) -> FileMetricsSnapshot {
        self.metrics.snapshot()
    }
}

impl Drop for OrcPhysicalReader {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

fn projection_mask(
    builder: &ArrowReaderBuilder<BoundChunkReader>,
    projection: &FileProjection,
) -> FileResult<ProjectionMask> {
    let root = builder.file_metadata().root_data_type();
    match projection {
        FileProjection::All => Ok(ProjectionMask::all()),
        FileProjection::RootNames(names) => {
            let available = root
                .children()
                .iter()
                .map(|column| column.name())
                .collect::<Vec<_>>();
            if let Some(missing) = names
                .iter()
                .find(|name| !available.contains(&name.as_str()))
            {
                return Err(FileError::invalid(format!(
                    "ORC projection column does not exist: {missing}"
                )));
            }
            Ok(ProjectionMask::named_roots(root, names))
        }
        FileProjection::RootIndices(indices) => {
            if let Some(index) = indices
                .iter()
                .find(|index| **index >= root.children().len())
            {
                return Err(FileError::invalid(format!(
                    "ORC root projection index out of bounds: {index}"
                )));
            }
            let column_indices = indices
                .iter()
                .map(|index| root.children()[*index].data_type().column_index());
            Ok(ProjectionMask::roots(root, column_indices))
        }
        FileProjection::FieldIds(_) => Err(FileError::unsupported(
            "ORC does not expose Iceberg-compatible field-ID projection",
        )),
    }
}

fn orc_range(range: FileReadRange) -> FileResult<Option<Range<usize>>> {
    let FileReadRange::Bounded { offset, length } = range else {
        return Ok(None);
    };
    let end = offset
        .checked_add(length)
        .ok_or_else(|| FileError::invalid("ORC range overflows"))?;
    Ok(Some(
        usize::try_from(offset)
            .map_err(|_| FileError::invalid("ORC range start overflows usize"))?
            ..usize::try_from(end)
                .map_err(|_| FileError::invalid("ORC range end overflows usize"))?,
    ))
}

fn orc_error(operation: &'static str, error: orc_rust::error::OrcError) -> FileError {
    let message = error.to_string();
    let kind = if message.contains("Cancelled:") {
        FileErrorKind::Cancelled
    } else if message.contains("DeadlineExceeded:") {
        FileErrorKind::DeadlineExceeded
    } else {
        FileErrorKind::Corrupt
    };
    FileError::with_source(kind, format!("{operation} failed"), error)
}

fn format_error(
    operation: &'static str,
    error: impl std::error::Error + Send + Sync + 'static,
) -> FileError {
    let message = error.to_string();
    let kind = if message.contains("Cancelled:") {
        FileErrorKind::Cancelled
    } else if message.contains("DeadlineExceeded:") {
        FileErrorKind::DeadlineExceeded
    } else {
        FileErrorKind::Corrupt
    };
    FileError::with_source(kind, format!("{operation} failed"), error)
}
