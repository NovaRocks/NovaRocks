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

use novarocks_fs::FileReadContext;
use novarocks_spi::connector::read_stack::SourcePage;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ENGINE_OUTPUT_POLICY, ScanOutputPolicy,
};
use novarocks_value_arrays::{
    ByteLayout, ValueError, ValueFailure, ValueViolation,
    access::ByteSource,
    measure::{OutputColumn, OutputRows, PrefixPlanner},
};

use crate::file_reader::map_file_error;
use crate::temporal::TemporalReadProjection;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) enum OutputSlot {
    Base,
    Constant(arrow::array::ArrayRef),
}

#[derive(Clone)]
pub(crate) struct OutputProjection(pub(crate) Vec<OutputSlot>);

impl OutputProjection {
    pub(super) fn project<'a>(
        &'a self,
        base: &[OutputColumn<'a>],
    ) -> Result<Vec<OutputColumn<'a>>, ConnectorError> {
        let inconsistent = || {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "Iceberg output projection does not consume the exact base columns",
            )
        };
        let mut remaining = base.iter().copied();
        let columns = self
            .0
            .iter()
            .map(|slot| match slot {
                OutputSlot::Base => remaining.next().ok_or_else(inconsistent),
                OutputSlot::Constant(array) => Ok(OutputColumn::constant(array, array.data_type())),
            })
            .collect::<Result<Vec<_>, _>>()?;
        if remaining.next().is_some() {
            return Err(inconsistent());
        }
        Ok(columns)
    }
}

/// Keep physical layout adaptation separate from the final SQL carrier.
/// Every conversion is bound to the exact field ID and Iceberg declaration.
#[derive(Clone)]
pub(super) struct TemporalOutputColumn {
    path: String,
    projection: TemporalReadProjection,
}

impl TemporalOutputColumn {
    pub(super) fn from_declaration(field: &crate::iceberg::spec::NestedField) -> Option<Self> {
        Self::from_type(&field.field_type, &field.name, field.id)
    }

    fn from_type(declaration: &crate::iceberg::spec::Type, name: &str, id: i32) -> Option<Self> {
        let projection = TemporalReadProjection::from_type(declaration);
        projection.needs_read_conversion().then(|| Self {
            path: format!("{name} (field {id})"),
            projection,
        })
    }
}

pub(super) fn temporal_output(
    columns: &[super::schema_binding::IcebergBoundColumn],
    output: Option<&OutputProjection>,
) -> Result<Arc<[Option<TemporalOutputColumn>]>, ConnectorError> {
    let mut base = columns.iter().map(
        |column| -> Result<Option<TemporalOutputColumn>, ConnectorError> {
            let handle = column.handle();
            let declaration =
                super::column_handle::parse_type(handle.type_json(), "output temporal field type")?;
            Ok(TemporalOutputColumn::from_type(
                &declaration,
                column.target().name(),
                handle
                    .field_id_path()
                    .last()
                    .copied()
                    .unwrap_or(handle.base_field_id()),
            ))
        },
    );
    let projection = if let Some(output) = output {
        let columns = output
            .0
            .iter()
            .map(|slot| match slot {
                OutputSlot::Base => base.next().ok_or_else(output_shape_error)?,
                OutputSlot::Constant(_) => Ok(None),
            })
            .collect::<Result<Vec<_>, _>>()?;
        if base.next().is_some() {
            return Err(output_shape_error());
        }
        columns
    } else {
        base.collect::<Result<Vec<_>, _>>()?
    };
    Ok(projection.into())
}

fn output_shape_error() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Internal,
        "Iceberg temporal projection does not match the exact output columns",
    )
}

pub(crate) fn byte_source(
    input: &arrow::array::ArrayRef,
    utf8: bool,
) -> Result<ByteSource<'_>, ValueError> {
    use arrow::datatypes::DataType;
    let layout_type = match input.data_type() {
        DataType::Dictionary(_, values) => values.as_ref(),
        other => other,
    };
    let layout = ByteLayout::from_data_type(layout_type)
        .filter(|layout| layout.is_utf8() == utf8)
        .ok_or(ValueError::Invariant(ValueViolation::Layout))?;
    ByteSource::parse(input, layout, Some(layout))
}

pub(super) struct OutputCursor {
    planner: PrefixPlanner<'static>,
    policy: ScanOutputPolicy,
    source_array_bytes: u64,
    temporal: Option<TemporalOutput>,
    page_resources: Option<novarocks_spi::connector::ConnectorExecutionResources>,
}

struct TemporalOutput {
    projection: Arc<[Option<TemporalOutputColumn>]>,
    sources: Box<[(arrow::array::ArrayRef, bool)]>,
}

impl OutputCursor {
    pub(super) fn new(
        columns: &[OutputColumn<'_>],
        rows: OutputRows<'static>,
        context: &FileReadContext,
    ) -> Result<Self, ConnectorError> {
        Self::with_policy(columns, rows, context, ENGINE_OUTPUT_POLICY)
    }

    pub(super) fn with_policy(
        columns: &[OutputColumn<'_>],
        rows: OutputRows<'static>,
        context: &FileReadContext,
        policy: ScanOutputPolicy,
    ) -> Result<Self, ConnectorError> {
        let planner = PrefixPlanner::new(
            columns,
            rows,
            &mut |_| context.check_active().map_err(map_file_error),
            &mut |_| context.check_active().map_err(map_file_error),
        )
        .map_err(value_failure)?;
        Ok(Self {
            planner,
            policy,
            source_array_bytes: 0,
            temporal: None,
            page_resources: None,
        })
    }

    pub(super) fn with_temporal(
        mut self,
        projection: &Arc<[Option<TemporalOutputColumn>]>,
        columns: &[OutputColumn<'_>],
    ) -> Result<Self, ConnectorError> {
        if projection.iter().any(Option::is_some) {
            if columns.len() != projection.len() {
                return Err(output_shape_error());
            }
            self.temporal = Some(TemporalOutput {
                projection: projection.clone(),
                sources: columns
                    .iter()
                    .map(|column| (column.source().clone(), column.is_constant()))
                    .collect(),
            });
        }
        Ok(self)
    }

    pub(super) fn with_page_resources(
        mut self,
        resources: Option<novarocks_spi::connector::ConnectorExecutionResources>,
        source_array_bytes: u64,
    ) -> Self {
        self.page_resources = resources;
        self.source_array_bytes = source_array_bytes;
        self
    }

    pub(super) const fn is_exhausted(&self) -> bool {
        self.planner.remaining_rows() == 0
    }

    pub(super) fn next_page(
        &mut self,
        context: &FileReadContext,
    ) -> Result<Option<SourcePage>, ConnectorError> {
        if self.is_exhausted() {
            return Ok(None);
        }
        let mut work = |_| context.check_active().map_err(map_file_error);
        let prefix = if let Some(temporal) = &self.temporal {
            let mut measure = |row| {
                temporal
                    .projection
                    .iter()
                    .zip(temporal.sources.iter())
                    .filter_map(|(column, source)| column.as_ref().map(|column| (column, source)))
                    .try_fold(0, |total, (column, (source, constant))| {
                        let source_row = if *constant { 0 } else { row };
                        Ok(total
                            + column.projection.additional_read_bytes(
                                source,
                                source_row..source_row + 1,
                                &column.path,
                                &mut || context.check_active().map_err(map_file_error),
                            )?)
                    })
            };
            self.planner.plan_prefix_with_additional_storage(
                self.policy.max_rows,
                self.policy.soft_target_new_column_bytes,
                Some(&mut measure),
                &mut work,
            )
        } else {
            self.planner.plan_prefix(
                self.policy.max_rows,
                self.policy.soft_target_new_column_bytes,
                &mut work,
            )
        }
        .map_err(value_failure)?;
        let rows = prefix.rows();
        let mut output_reservation = self
            .page_resources
            .as_ref()
            .map(|resources| {
                use novarocks_spi::connector::ConnectorResourceClass;
                // Arrow can retain the input buffer in a slice. Include the
                // decoded prefix's actual backing and wrapper size along with
                // the output planner's estimate before copying or aliasing it.
                let bytes = u64::try_from(prefix.new_column_bytes())
                    .ok()
                    .and_then(|bytes| bytes.checked_add(self.source_array_bytes))
                    .ok_or_else(output_charge_out_of_range)?;
                (bytes != 0)
                    .then(|| resources.try_reserve(ConnectorResourceClass::ReaderOutput, bytes))
                    .transpose()
            })
            .transpose()?
            .flatten();
        let mut observed_bytes = self.source_array_bytes;
        let columns = prefix
            .build_observed(
                &mut |layout| {
                    context.check_active().map_err(map_file_error)?;
                    if let Some(reservation) = &mut output_reservation {
                        observed_bytes = observed_bytes
                            .checked_add(layout.pad_to_align().size() as u64)
                            .ok_or_else(output_charge_out_of_range)?;
                        reservation.try_grow(observed_bytes.saturating_sub(reservation.bytes()))?;
                    }
                    Ok(())
                },
                &mut work,
            )
            .map_err(value_failure)?;
        let columns = if let Some(TemporalOutput { projection, .. }) = &self.temporal {
            if columns.len() != projection.len() {
                return Err(output_shape_error());
            }
            columns
                .into_iter()
                .zip(projection.iter())
                .map(|(column, temporal)| match temporal {
                    Some(temporal) => {
                        temporal
                            .projection
                            .materialize(column, &temporal.path, &mut || {
                                context.check_active().map_err(map_file_error)
                            })
                    }
                    None => Ok(column),
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            columns
        };
        let output_bytes = output_reservation
            .as_mut()
            .map(|reservation| {
                // The prefix estimate excludes wrappers. Settle the exact
                // retained Arrow capacity, including the final temporal carrier,
                // before transferring custody to the page.
                let bytes = columns.iter().try_fold(0_u64, |bytes, column| {
                    bytes
                        .checked_add(column.get_array_memory_size() as u64)
                        .ok_or_else(output_charge_out_of_range)
                })?;
                reservation.try_grow(bytes.saturating_sub(reservation.bytes()))?;
                Ok::<_, ConnectorError>(bytes)
            })
            .transpose()?;
        let page = match output_reservation.zip(output_bytes) {
            Some((reservation, bytes)) => {
                SourcePage::try_new_accounted(rows, columns, reservation.into_output(bytes)?)?
            }
            None => SourcePage::try_new(rows, columns)?,
        };
        let physical_logical_bytes = prefix.logical_bytes();
        let constructed_column_bytes = prefix.new_column_bytes();
        self.planner.advance(rows).map_err(value_error)?;
        tracing::trace!(rows, %physical_logical_bytes, %constructed_column_bytes, "Iceberg output page constructed");
        Ok(Some(page))
    }
}

fn value_failure(failure: ValueFailure<ConnectorError>) -> ConnectorError {
    match failure {
        ValueFailure::Caller(error) => error,
        ValueFailure::Value(error) => value_error(error),
    }
}

fn output_charge_out_of_range() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        "Iceberg output charge is out of range",
    )
}

fn value_error(error: ValueError) -> ConnectorError {
    let kind = match error {
        ValueError::Allocation { .. } => ConnectorErrorKind::ResourceExhausted,
        _ => ConnectorErrorKind::CorruptData,
    };
    ConnectorError::from_domain(kind, error)
}
