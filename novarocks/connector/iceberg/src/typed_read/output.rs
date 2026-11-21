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
    temporal: Option<TemporalOutput>,
}

struct TemporalOutput {
    projection: Arc<[Option<TemporalOutputColumn>]>,
    sources: Box<[arrow::array::ArrayRef]>,
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
            temporal: None,
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
                    .map(|column| column.source().clone())
                    .collect(),
            });
        }
        Ok(self)
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
                    .try_fold(0, |total, (column, source)| {
                        Ok(total
                            + column.projection.additional_read_bytes(
                                source,
                                row..row + 1,
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
        let columns = prefix
            .build_observed(
                &mut |_| context.check_active().map_err(map_file_error),
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
        let page = SourcePage::try_new(rows, columns)?;
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

fn value_error(error: ValueError) -> ConnectorError {
    let kind = match error {
        ValueError::Allocation { .. } => ConnectorErrorKind::ResourceExhausted,
        _ => ConnectorErrorKind::CorruptData,
    };
    ConnectorError::from_domain(kind, error)
}
