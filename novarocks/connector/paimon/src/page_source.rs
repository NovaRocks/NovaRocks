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

use std::time::Instant;

use arrow::array::ArrayRef;
use novarocks_spi::connector::read_stack::{ConnectorPageSource, PageSourceMetrics, SourcePage};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

use crate::reader::{PaimonBatchReader, PaimonReadBatch};
use crate::resources::PaimonRequestResources;

/// Page-source lifecycle for one already-merged Paimon SDK stream.
pub struct PaimonPageSource {
    reader: Option<Box<dyn PaimonBatchReader>>,
    resources: PaimonRequestResources,
    remaining_rows: Option<u64>,
    finished: bool,
    metrics: PageSourceMetrics,
}

impl PaimonPageSource {
    pub fn new(
        reader: Box<dyn PaimonBatchReader>,
        resources: PaimonRequestResources,
        row_limit: Option<u64>,
    ) -> Self {
        Self {
            reader: Some(reader),
            resources,
            remaining_rows: row_limit,
            finished: row_limit == Some(0),
            metrics: PageSourceMetrics::default(),
        }
    }

    fn close_once(&mut self) -> Result<(), ConnectorError> {
        self.finished = true;
        match self.reader.take() {
            Some(mut reader) => reader.close(),
            None => Ok(()),
        }
    }

    fn fail(&mut self, error: ConnectorError) -> ConnectorError {
        match self.close_once() {
            Ok(()) => error,
            Err(close_error) => error.with_cleanup_context(close_error.to_string()),
        }
    }

    fn page_from_batch(&mut self, batch: PaimonReadBatch) -> Result<SourcePage, ConnectorError> {
        let (batch, output_reservation) = batch.into_parts();
        let available = batch.num_rows();
        let rows = match self.remaining_rows {
            Some(remaining) => usize::try_from(remaining.min(available as u64)).map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "Paimon output row limit does not fit in memory",
                )
            })?,
            None => available,
        };
        let columns: Vec<ArrayRef> = if rows == available {
            batch.columns().to_vec()
        } else {
            batch
                .columns()
                .iter()
                .map(|column| column.slice(0, rows))
                .collect()
        };
        if let Some(remaining) = &mut self.remaining_rows {
            *remaining -= rows as u64;
        }

        let page = if columns.is_empty() {
            SourcePage::zero_channel(rows)
        } else {
            let retained_bytes = columns
                .iter()
                .map(|column| column.get_array_memory_size() as u64)
                .fold(0_u64, u64::saturating_add);
            let reservation = match output_reservation {
                Some(reservation) => reservation,
                None => self.resources.reserve_output(retained_bytes.max(1))?,
            };
            let output = self
                .resources
                .transfer_output(reservation, retained_bytes)?;
            SourcePage::try_new_accounted(rows, columns, output)?
        };
        self.resources.checkpoint()?;
        self.metrics.completed_positions =
            self.metrics.completed_positions.saturating_add(rows as u64);
        self.metrics.file.batches_delivered = self.metrics.file.batches_delivered.saturating_add(1);
        if self.remaining_rows == Some(0) {
            self.close_once()?;
        }
        Ok(page)
    }
}

impl ConnectorPageSource for PaimonPageSource {
    fn next_source_page(&mut self) -> Result<Option<SourcePage>, ConnectorError> {
        if self.finished {
            // A zero limit owns a reader but must still close it before return.
            self.close_once()?;
            return Ok(None);
        }
        if let Err(error) = self.resources.checkpoint() {
            return Err(self.fail(error));
        }
        loop {
            let started = Instant::now();
            let next = match self.reader.as_mut() {
                Some(reader) => reader.next_batch(),
                None => return Ok(None),
            };
            self.metrics.read_time_nanos = self
                .metrics
                .read_time_nanos
                .saturating_add(started.elapsed().as_nanos().try_into().unwrap_or(u64::MAX));
            match next {
                Ok(Some(batch)) => {
                    self.metrics.file.rows_decoded = self
                        .metrics
                        .file
                        .rows_decoded
                        .saturating_add(batch.num_rows() as u64);
                    if let Err(error) = self.resources.checkpoint() {
                        return Err(self.fail(error));
                    }
                    if batch.num_rows() == 0 {
                        if let Err(error) = self.resources.checkpoint() {
                            return Err(self.fail(error));
                        }
                        continue;
                    }
                    return self
                        .page_from_batch(batch)
                        .map(Some)
                        .map_err(|error| self.fail(error));
                }
                Ok(None) => {
                    self.close_once()?;
                    return Ok(None);
                }
                Err(error) => return Err(self.fail(error)),
            }
        }
    }

    fn is_finished(&self) -> bool {
        self.finished
    }

    fn metrics(&self) -> PageSourceMetrics {
        self.metrics
    }

    fn memory_usage_bytes(&self) -> u64 {
        0
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        self.close_once()
    }
}

impl Drop for PaimonPageSource {
    fn drop(&mut self) {
        let _ = self.close_once();
    }
}
