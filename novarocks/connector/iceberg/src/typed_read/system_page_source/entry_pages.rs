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

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use novarocks_functions::{KernelFailure, json_value::JsonWork};
use novarocks_spi::connector::read_stack::BudgetConsume;
use novarocks_spi::connector::{
    ConnectorErrorKind, ConnectorExecutionResources, ConnectorResourceClass,
    ConnectorResourceReservation, ENGINE_OUTPUT_POLICY,
};

use super::entry_columns::{EntryColumns, row_bytes};
use super::entry_view::{EntryPosition, EntryPositionFact};
use super::{
    ConnectorError, ConnectorPollBudget, FileReadContext, FilesSplitRelation,
    IcebergSystemPageSource, Manifest, MaterializedPages, NonZeroUsize, PageSourceMetrics,
    SourcePage, internal,
};
use crate::file_reader::map_file_error;

pub(super) struct EntryPages {
    relation: FilesSplitRelation,
    manifest: Manifest,
    position: EntryPosition,
    context: FileReadContext,
    budget: ConnectorPollBudget,
    spending: Option<BudgetConsume>,
    pending: Option<(EntryPositionFact, u128)>,
    rows: Vec<EntryPositionFact>,
    bytes: u128,
    max_page_rows: NonZeroUsize,
    source_bytes: u64,
    emitted: u64,
    resources: Option<ConnectorExecutionResources>,
    output: Option<IcebergSystemPageSource>,
}

impl EntryPages {
    pub(super) fn new(
        relation: FilesSplitRelation,
        manifest: Manifest,
        context: FileReadContext,
        budget: ConnectorPollBudget,
        max_page_rows: NonZeroUsize,
        source_bytes: u64,
        resources: Option<ConnectorExecutionResources>,
    ) -> Self {
        let position = EntryPosition::new(relation.scan.system_table_type(), &relation.manifest);
        Self {
            relation,
            manifest,
            position,
            context,
            budget,
            spending: None,
            pending: None,
            rows: Vec::new(),
            bytes: 0,
            max_page_rows,
            source_bytes,
            emitted: 0,
            resources,
            output: None,
        }
    }

    fn build_current_page(&mut self) -> Result<IcebergSystemPageSource, ConnectorError> {
        let schema = self.relation.scan.source_schema();
        let staging = self
            .resources
            .as_ref()
            .map(|resources| {
                let bytes = u64::try_from(self.bytes).map_err(|_| {
                    ConnectorError::new(
                        ConnectorErrorKind::ResourceExhausted,
                        "Iceberg entry page exceeds the resource domain",
                    )
                })?;
                (bytes != 0)
                    .then(|| resources.try_reserve(ConnectorResourceClass::ReaderState, bytes))
                    .transpose()
            })
            .transpose()?
            .flatten();
        let rows = self
            .rows
            .iter()
            .map(|position| position.view(&self.manifest))
            .collect::<Vec<_>>();
        let mut work = EntryJsonWork {
            context: &self.context,
            resources: self.resources.as_ref(),
            temporary: None,
            failure: None,
        };
        let spec = self
            .relation
            .scan
            .partition_spec(self.relation.manifest.partition_spec_id())
            .ok_or_else(|| internal("Iceberg manifest lost its frozen partition spec"))?;
        let writer = EntryColumns {
            scan: &self.relation.scan,
            manifest: &self.relation.manifest,
            spec,
        };
        let selected = self.relation.projection.as_slice();
        let columns = selected
            .iter()
            .map(|index| {
                writer
                    .build(&schema.fields()[*index], &rows, &mut work)
                    .map_err(|error| work.failure.take().unwrap_or(error))
            })
            .collect::<Result<Vec<_>, _>>()?;
        IcebergSystemPageSource::from_projection(
            schema,
            columns,
            selected,
            rows.len(),
            self.max_page_rows,
            self.source_bytes,
            &self.context,
            self.resources.clone(),
        )
        .map(|output| output.with_staging_reservation(staging))
    }

    fn poll_page(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<SourcePage>, ConnectorError>> {
        if let Some(output) = &mut self.output {
            let page = output.next_page()?;
            if output.is_exhausted() {
                self.output = None;
            }
            if let Some(page) = page {
                self.emitted += page.position_count() as u64;
                return Poll::Ready(Ok(Some(page)));
            }
        }
        if let Some((row, bytes)) = self.pending.take() {
            self.rows.push(row);
            self.bytes = bytes;
        }
        let soft_bytes = u128::from(ENGINE_OUTPUT_POLICY.soft_target_new_column_bytes);
        while self.rows.len() < self.max_page_rows.get()
            && self.bytes < soft_bytes
            && self.position.has_entry(&self.manifest)
        {
            let spending = self.spending.get_or_insert_with(|| self.budget.consume(1));
            if Pin::new(spending).poll(cx).is_pending() {
                return Poll::Pending;
            }
            self.spending = None;
            self.context.check_active().map_err(map_file_error)?;
            let Some(row) = self
                .position
                .take(&self.manifest, &self.relation.manifest)?
            else {
                continue;
            };
            let schema = self.relation.scan.source_schema();
            let width = self
                .relation
                .projection
                .iter()
                .map(|index| {
                    row_bytes(
                        &schema.fields()[*index],
                        &row,
                        &self.relation.scan,
                        &self.relation.manifest,
                    )
                })
                .sum::<u128>();
            let position = row.position();
            if !self.rows.is_empty() && self.bytes + width > soft_bytes {
                self.pending = Some((position, width));
                break;
            }
            self.rows.push(position);
            self.bytes += width;
        }
        if self.rows.is_empty() {
            return Poll::Ready(Ok(None));
        }
        self.output = Some(self.build_current_page()?);
        self.rows.clear();
        self.bytes = 0;
        self.poll_page(cx)
    }
}

impl MaterializedPages for EntryPages {
    fn poll_next_page(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<SourcePage>, ConnectorError>> {
        self.poll_page(cx)
    }

    fn is_exhausted(&self) -> bool {
        self.output.is_none()
            && self.pending.is_none()
            && self.rows.is_empty()
            && !self.position.has_entry(&self.manifest)
    }

    fn metrics(&self) -> PageSourceMetrics {
        PageSourceMetrics {
            completed_bytes: self.source_bytes,
            completed_positions: self.emitted,
            ..Default::default()
        }
    }

    fn memory_usage_bytes(&self) -> u64 {
        // The SDK owns allocations behind Manifest and provides no allocation
        // meter; this reports the observed object extent and current output.
        self.source_bytes
            + self
                .output
                .as_ref()
                .map_or(0, MaterializedPages::memory_usage_bytes)
    }
}

struct EntryJsonWork<'a> {
    context: &'a FileReadContext,
    resources: Option<&'a ConnectorExecutionResources>,
    temporary: Option<ConnectorResourceReservation>,
    failure: Option<ConnectorError>,
}

impl EntryJsonWork<'_> {
    fn preserve(&mut self, error: ConnectorError) -> KernelFailure {
        let projected = match error.kind() {
            ConnectorErrorKind::Cancelled => KernelFailure::Cancelled,
            ConnectorErrorKind::DeadlineExceeded => KernelFailure::DeadlineExceeded,
            _ => KernelFailure::ResourceExhausted,
        };
        self.failure = Some(error);
        projected
    }
}

impl JsonWork for EntryJsonWork<'_> {
    type Reservation = Option<ConnectorResourceReservation>;

    fn step(&mut self) -> Result<(), KernelFailure> {
        self.context
            .check_active()
            .map_err(map_file_error)
            .map_err(|error| self.preserve(error))?;
        if let Some(resources) = self.resources {
            resources
                .checkpoint()
                .map_err(|error| self.preserve(error))?;
        }
        Ok(())
    }

    fn reserve(&mut self, bytes: usize) -> Result<Self::Reservation, KernelFailure> {
        match (self.resources, bytes) {
            (Some(resources), bytes) if bytes != 0 => resources
                .try_reserve(ConnectorResourceClass::ReaderState, bytes as u64)
                .map(Some)
                .map_err(|error| self.preserve(error)),
            _ => Ok(None),
        }
    }

    fn reserve_output(&mut self, bytes: usize) -> Result<(), KernelFailure> {
        if let Some(temporary) = &mut self.temporary {
            temporary
                .try_grow(bytes as u64)
                .map_err(|error| self.preserve(error))?;
        } else {
            self.temporary = self.reserve(bytes)?;
        }
        Ok(())
    }
}
