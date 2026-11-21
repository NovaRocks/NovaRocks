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

//! The reader behind one change-window split.
//!
//! A change window is the **set difference of the visible rows at its two
//! endpoints**, never a replay of what happened between them. The enumeration
//! in `split_source` already guarantees that: a row written and deleted inside
//! the window is invisible at both endpoints, so no split names it. This module
//! must not reintroduce it, which is why nothing here ever reads a data file's
//! rows without also subtracting what was already invisible at the endpoint the
//! rows are being compared against.
//!
//! Two things separate this reader from the data reader it delegates to:
//!
//! * `__change_op` is derived from the split variant and produced here as an
//!   Arrow `Int32`, matching the provider's signed public schema. SQL authors
//!   an explicit cast when its internal delta algebra requires an Int8 sign;
//! * the reverse side *selects* the rows a delete removed instead of hiding
//!   them by comparing both complete endpoint closures. Added and removed
//!   files use ordinary exclusion at their independently pinned endpoint.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array};
use novarocks_fs::{FileReadContext, FileReaderOptions, ReaderSizing};
use novarocks_spi::connector::ConnectorError;
use novarocks_spi::connector::read_stack::{ConnectorPollBudget, OwnedConnectorPageStream};

use crate::access_binding::IcebergReadBinding;

use super::change_window::{
    ICEBERG_CHANGE_OP_FIELD_ID, IcebergChangeSplit, IcebergChangeWindowHandle,
    change_op_column_handle,
};
use super::column_handle::{IcebergColumnHandle, invalid, unsupported};
use super::delete_manager::{DeleteEvaluationMode, DeleteManager, EndpointDeleteClosure};
use super::output::{OutputProjection, OutputSlot as ScanOutputSlot};
use super::page_source::{
    IcebergDynamicFilter, IcebergPageSourceRequest, IcebergReadRelation, ParquetFooterCache,
    create_iceberg_page_stream,
};

/// Everything the change-window reader needs, all of it frozen or
/// process-local.
pub struct IcebergChangeWindowPageSourceRequest<'a> {
    pub handle: &'a IcebergChangeWindowHandle,
    pub split: &'a IcebergChangeSplit,
    /// The scan's ordered output columns; `__change_op` may appear among them.
    pub columns: &'a [IcebergColumnHandle],
    pub delete_manager: Arc<DeleteManager>,
    pub footers: Arc<ParquetFooterCache>,
    pub access_binding: IcebergReadBinding,
    pub context: FileReadContext,
    pub cache: Option<novarocks_fs::DataCachePolicy>,
    pub budget: ReaderSizing,
    pub reader_options: FileReaderOptions,
    pub(super) output_layout: super::runtime::IcebergReadLayout,
    pub scheduled_split_sequence_id: u64,
    pub dynamic_filter: Arc<IcebergDynamicFilter>,
}

/// Build the page stream for one change-window split, polled with `budget`:
/// the stream of its data split, with the derived sign put back in order.
pub fn create_iceberg_change_window_page_stream(
    request: IcebergChangeWindowPageSourceRequest<'_>,
    budget: &ConnectorPollBudget,
) -> Result<OwnedConnectorPageStream, ConnectorError> {
    let ChangeWindowRead {
        relation,
        base_columns,
        delete_mode,
        output,
    } = ChangeWindowRead::of(&request)?;
    create_iceberg_page_stream(
        data_request(request, &relation, &base_columns, delete_mode, output),
        budget,
    )
}

/// What a change-window split reads through the data reader, and how its
/// pages are put back into the scan's order.
struct ChangeWindowRead {
    relation: IcebergReadRelation,
    base_columns: Vec<IcebergColumnHandle>,
    delete_mode: DeleteEvaluationMode,
    output: OutputProjection,
}

impl ChangeWindowRead {
    fn of(request: &IcebergChangeWindowPageSourceRequest<'_>) -> Result<Self, ConnectorError> {
        let split = request.split;
        let ChangeOpProjection {
            slots,
            base_columns,
        } = ChangeOpProjection::of(request.columns)?;
        Ok(Self {
            relation: match split {
                IcebergChangeSplit::VisibilityDifference(_) => {
                    IcebergReadRelation::of_change_window(
                        request.handle,
                        split.data().partition_spec_id(),
                    )?
                }
                _ => IcebergReadRelation::of_change_window_endpoint(
                    request.handle,
                    split.data().partition_spec_id(),
                    matches!(split, IcebergChangeSplit::DeletedDataFileRows(_)),
                )?,
            }
            .with_output_layout(request.output_layout),
            delete_mode: delete_mode_of(split, request.handle)?,
            output: {
                let sign: ArrayRef = Arc::new(Int32Array::from(vec![i32::from(split.change_op())]));
                OutputProjection(
                    slots
                        .into_iter()
                        .map(|slot| match slot {
                            OutputSlot::Base => ScanOutputSlot::Base,
                            OutputSlot::ChangeOp => ScanOutputSlot::Constant(sign.clone()),
                        })
                        .collect(),
                )
            },
            base_columns,
        })
    }
}

/// The request of a change-window split's data split.
fn data_request<'a>(
    request: IcebergChangeWindowPageSourceRequest<'a>,
    relation: &'a IcebergReadRelation,
    base_columns: &'a [IcebergColumnHandle],
    delete_mode: DeleteEvaluationMode,
    output: OutputProjection,
) -> IcebergPageSourceRequest<'a> {
    IcebergPageSourceRequest {
        relation,
        split: request.split.data(),
        columns: base_columns,
        delete_manager: request.delete_manager,
        delete_mode,
        footers: request.footers,
        access_binding: request.access_binding,
        context: request.context,
        cache: request.cache,
        budget: request.budget,
        reader_options: request.reader_options,
        scheduled_split_sequence_id: request.scheduled_split_sequence_id,
        dynamic_filter: request.dynamic_filter,
        prepared_input: None,
        pending_preparation_control: None,
        output_projection: Some(output),
    }
}

/// How one split's delete state is spent, chosen by its variant.
///
/// Every reverse-side variant subtracts what the lower endpoint had already
/// removed. Leaving that out would emit rows that were invisible at both
/// endpoints, which is exactly the double counting the set-difference contract
/// exists to prevent.
fn delete_mode_of(
    split: &IcebergChangeSplit,
    handle: &IcebergChangeWindowHandle,
) -> Result<DeleteEvaluationMode, ConnectorError> {
    match split {
        IcebergChangeSplit::AddedRows(rows) => {
            if !rows.restricted_row_ids().is_empty() {
                // The wire field names row *ids*, not row positions, and
                // nothing in this stack produces one. Reading it as either
                // would be a guess about which rows a split owns.
                return Err(unsupported(format!(
                    "iceberg change-window added rows of {} are narrowed to {} row ids, which this page source does not implement",
                    rows.data().path(),
                    rows.restricted_row_ids().len()
                )));
            }
            // The split carries the upper endpoint's own closure, so excluding
            // what it deletes leaves exactly the rows that survive at `to`.
            Ok(DeleteEvaluationMode::ExcludeDeleted)
        }
        IcebergChangeSplit::VisibilityDifference(rows) => {
            Ok(DeleteEvaluationMode::VisibleDifference {
                from: EndpointDeleteClosure {
                    read_domain: handle.from_read_domain().clone(),
                    deletes: rows.from_deletes().to_vec(),
                },
                to: EndpointDeleteClosure {
                    read_domain: handle.to_read_domain().clone(),
                    deletes: rows.to_deletes().to_vec(),
                },
            })
        }
        IcebergChangeSplit::DeletedDataFileRows(_) => Ok(DeleteEvaluationMode::ExcludeDeleted),
    }
}

/// Which output channel each scan assignment comes from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OutputSlot {
    /// The next channel the data reader produces.
    Base,
    /// The split's sign, derived here.
    ChangeOp,
}

/// The scan's output columns, split into what a file supplies and what the
/// variant does.
struct ChangeOpProjection {
    slots: Vec<OutputSlot>,
    base_columns: Vec<IcebergColumnHandle>,
}

impl ChangeOpProjection {
    fn of(columns: &[IcebergColumnHandle]) -> Result<Self, ConnectorError> {
        let change_op = change_op_column_handle()?;
        let mut slots = Vec::with_capacity(columns.len());
        let mut base_columns = Vec::with_capacity(columns.len());
        for column in columns {
            if column.base_field_id() == ICEBERG_CHANGE_OP_FIELD_ID {
                // The reserved field ID belongs to the sign and to nothing
                // else, so a handle that claims it while describing another
                // column would silently be answered with a sign.
                if *column != change_op {
                    return Err(invalid(
                        "an iceberg scan assignment claims the change-window sign field id without being the sign column",
                    ));
                }
                slots.push(OutputSlot::ChangeOp);
            } else {
                slots.push(OutputSlot::Base);
                base_columns.push(column.clone());
            }
        }
        Ok(Self {
            slots,
            base_columns,
        })
    }
}

#[cfg(test)]
mod tests;
