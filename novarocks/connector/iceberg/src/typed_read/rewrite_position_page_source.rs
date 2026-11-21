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

//! The page stream of `REWRITE_POSITION_DELETE_FILES`.
//!
//! It is the one reader in this stack that never opens a data file. Its input
//! is the set of Puffin deletion vectors the frozen rewrite group selected for
//! one data file, and its output is the two columns an Iceberg position-delete
//! file holds: the data file's path, and each deleted row's absolute position
//! inside it.
//!
//! Positions are re-encoded, never re-derived. The rewrite exists to repack the
//! delete artifacts of a data file without changing which rows they remove, so
//! reading a vector the group did not select, or losing one it did, would
//! change the relation's visible rows rather than a query's answer.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use novarocks_fs::FileReadContext;
use novarocks_spi::connector::ConnectorError;
use novarocks_spi::connector::read_stack::{
    ConnectorPollBudget, OwnedConnectorPageStream, PageSourceMetrics, SourcePage,
};
use roaring::RoaringTreemap;

use crate::access_binding::IcebergReadBinding;
use crate::delete_file::{IcebergDeleteFileSpec, IcebergFileContent, IcebergFileFormat};
use crate::position_delete::load_position_deletes_async;

use super::column_handle::{IcebergColumnHandle, invalid};
use super::page_source::{MaterializedPageStream, MaterializedPages, SplitOperations};
use super::schema_binding::IcebergMetadataColumn;
use super::split::IcebergDeleteFile;
use super::table_execute::{
    IcebergRewritePositionDeleteFilesSplit, REWRITE_POSITION_DELETE_OUTPUT_COLUMNS,
};

/// The frozen facts one rewrite-position split reads.
pub struct IcebergRewritePositionDeleteFilesPageSourceRequest<'a> {
    pub split: &'a IcebergRewritePositionDeleteFilesSplit,
    /// The scan's ordered output columns. Channel `i` is produced for
    /// `columns[i]`, so their order -- not this module's -- is the contract.
    pub columns: &'a [IcebergColumnHandle],
    pub access_binding: IcebergReadBinding,
    pub context: FileReadContext,
}

/// Which of the two output columns one page channel carries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RewritePositionOutput {
    FilePath,
    Position,
}

/// Resolve the scan's ordered columns onto the two columns this reader has.
///
/// A column it cannot produce is refused here rather than filled with nulls: a
/// deletion-vector rewrite that emitted an unbound column would write a
/// position-delete file whose rows name nothing.
fn resolve_outputs(
    columns: &[IcebergColumnHandle],
) -> Result<Vec<RewritePositionOutput>, ConnectorError> {
    columns
        .iter()
        .map(|column| {
            if !column.is_base_column() {
                return Err(invalid(format!(
                    "the iceberg rewrite-position reader produces no nested field, but column '{}' dereferences one",
                    column.base_column_identity().name()
                )));
            }
            match IcebergMetadataColumn::from_field_id(column.base_field_id()) {
                Some(IcebergMetadataColumn::Path)
                    if column.base_column_identity().name()
                        == REWRITE_POSITION_DELETE_OUTPUT_COLUMNS[0].0 =>
                {
                    Ok(RewritePositionOutput::FilePath)
                }
                Some(IcebergMetadataColumn::RowPosition)
                    if column.base_column_identity().name()
                        == REWRITE_POSITION_DELETE_OUTPUT_COLUMNS[1].0 =>
                {
                    Ok(RewritePositionOutput::Position)
                }
                Some(IcebergMetadataColumn::Path | IcebergMetadataColumn::RowPosition) => {
                    Err(invalid(format!(
                        "the iceberg rewrite-position reader requires canonical output column names file_path and pos, not '{}'",
                        column.base_column_identity().name()
                    )))
                }
                Some(
                    IcebergMetadataColumn::RowId
                    | IcebergMetadataColumn::LastUpdatedSequenceNumber
                    | IcebergMetadataColumn::IsDeleted,
                )
                | None => Err(invalid(format!(
                    "the iceberg rewrite-position reader produces only the data file path and the deleted row position, not column '{}'",
                    column.base_column_identity().name()
                ))),
            }
        })
        .collect()
}

/// The Puffin content ranges one split selected, as the delete loader names
/// them.
///
/// [`IcebergRewritePositionDeleteFilesSplit`] already proved every selected
/// delete is a Puffin deletion vector with a content range, so this conversion
/// restates those facts rather than re-deciding them.
fn selected_delete_specs(
    deletes: &[IcebergDeleteFile],
) -> Result<Vec<IcebergDeleteFileSpec>, ConnectorError> {
    deletes
        .iter()
        .map(|delete| {
            let content_offset = delete.content_offset().ok_or_else(|| {
                invalid(format!(
                    "iceberg rewrite-position deletion vector {} has no puffin content offset",
                    delete.path()
                ))
            })?;
            let content_size_in_bytes = delete.content_size_in_bytes().ok_or_else(|| {
                invalid(format!(
                    "iceberg rewrite-position deletion vector {} has no puffin content size",
                    delete.path()
                ))
            })?;
            Ok(IcebergDeleteFileSpec {
                path: delete.path().to_string(),
                file_format: IcebergFileFormat::Puffin,
                file_content: IcebergFileContent::PositionDeletes,
                length: u64::try_from(delete.file_size_in_bytes()).ok(),
                content_offset: Some(content_offset),
                content_size_in_bytes: Some(content_size_in_bytes),
                referenced_data_file: delete.referenced_data_file().map(str::to_string),
            })
        })
        .collect()
}

/// Open the page stream for one rewrite-position split, polled with
/// `poll_budget`.
///
/// Creating it reads nothing. Its first poll resolves access and loads every
/// selected vector through the split's own operations; each page then spends
/// one unit of the host's turn budget.
///
/// Every selected vector is loaded up front: they belong to one data file, the
/// group froze how many there are, and the merged positions must be complete
/// and ordered before the first row can be emitted at all. `RoaringTreemap`
/// iteration is ascending and set-valued, so the union of the vectors is
/// sorted and deduplicated by construction -- which is what an Iceberg
/// position-delete file requires.
pub fn create_iceberg_rewrite_position_delete_files_page_stream(
    request: IcebergRewritePositionDeleteFilesPageSourceRequest<'_>,
    poll_budget: &ConnectorPollBudget,
) -> Result<OwnedConnectorPageStream, ConnectorError> {
    let IcebergRewritePositionDeleteFilesPageSourceRequest {
        split,
        columns,
        access_binding,
        mut context,
    } = request;

    let outputs = resolve_outputs(columns)?;
    let specs = selected_delete_specs(split.selected_position_deletes())?;
    let operations = SplitOperations::bind(&mut context)?;
    let data_file_path = split.data_file_path().to_string();
    let load = async move {
        let access = access_binding
            .resolve_access_for_locations_async(
                specs.iter().map(|spec| spec.path.as_str()),
                &context,
            )
            .await?;
        let positions = load_position_deletes_async(&specs, &data_file_path, &access, &context)
            .await
            .map_err(invalid)?;
        IcebergRewritePositionDeleteFilesPageSource::of(
            &data_file_path,
            outputs,
            &specs,
            &positions,
            context,
        )
    };
    Ok(Box::pin(MaterializedPageStream::new(
        Box::pin(load),
        operations,
        poll_budget,
    )))
}

/// One split's worth of re-encoded delete positions.
struct IcebergRewritePositionDeleteFilesPageSource {
    context: FileReadContext,
    output: super::output::OutputCursor,
    emitted: usize,
    retained_bytes: u64,
    bytes_read: u64,
}

impl IcebergRewritePositionDeleteFilesPageSource {
    /// The merged positions of every selected vector, re-encoded for pages.
    fn of(
        data_file_path: &str,
        outputs: Vec<RewritePositionOutput>,
        specs: &[IcebergDeleteFileSpec],
        positions: &RoaringTreemap,
        context: FileReadContext,
    ) -> Result<Self, ConnectorError> {
        let bytes_read = specs
            .iter()
            .filter_map(|spec| spec.content_size_in_bytes)
            .map(|size| u64::try_from(size).unwrap_or_default())
            .sum::<u64>();
        let mut rows = Vec::with_capacity(usize::try_from(positions.len()).unwrap_or_default());
        for position in positions {
            rows.push(i64::try_from(position).map_err(|_| {
                invalid(format!(
                    "iceberg rewrite-position deletion vector for {data_file_path} holds a position outside i64"
                ))
            })?);
        }
        use novarocks_value_arrays::measure::{OutputColumn, OutputRows};
        let row_count = rows.len();
        let retained_bytes = (rows.capacity() * size_of::<i64>() + data_file_path.len()) as u64;
        let rows: ArrayRef = Arc::new(Int64Array::from(rows));
        let path: ArrayRef = Arc::new(StringArray::from(vec![data_file_path]));
        let columns = outputs
            .iter()
            .map(|output| match output {
                RewritePositionOutput::FilePath => OutputColumn::constant(&path, path.data_type()),
                RewritePositionOutput::Position => OutputColumn::new(&rows, rows.data_type()),
            })
            .collect::<Vec<_>>();
        let output = super::output::OutputCursor::new(
            &columns,
            OutputRows::Contiguous(0..row_count),
            &context,
        )?;
        Ok(Self {
            context,
            output,
            emitted: 0,
            retained_bytes,
            bytes_read,
        })
    }
}

impl MaterializedPages for IcebergRewritePositionDeleteFilesPageSource {
    fn next_page(&mut self) -> Result<Option<SourcePage>, ConnectorError> {
        let page = self.output.next_page(&self.context)?;
        if let Some(page) = &page {
            self.emitted += page.position_count();
        }
        Ok(page)
    }

    fn is_exhausted(&self) -> bool {
        self.output.is_exhausted()
    }

    fn metrics(&self) -> PageSourceMetrics {
        PageSourceMetrics {
            completed_bytes: self.bytes_read,
            completed_positions: self.emitted as u64,
            read_time_nanos: 0,
            ..Default::default()
        }
    }

    fn memory_usage_bytes(&self) -> u64 {
        self.retained_bytes
    }
}

#[cfg(test)]
mod tests;
