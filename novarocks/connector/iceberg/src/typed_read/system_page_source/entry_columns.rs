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

use std::collections::HashMap;

use arrow::array::{LargeBinaryBuilder, StructArray};
use novarocks_functions::json_value::{self, Input, JsonWork, Scalar};

use super::bounds::{self, BoundText};
use super::entry_view::EntryView;
use super::{
    Arc, ArrayRef, BTreeMap, BinaryBuilder, ConnectorError, DataContentType, DataFile, DataType,
    Datum, Field, Int32Array, Int32Builder, Int64Array, Int64Builder, ListBuilder, Literal,
    ManifestStatus, MapBuilder, PartitionSpec, PrimitiveLiteral, Schema, StringArray,
    StringBuilder, TrinoManifestFile, content_code, corrupt, file_format_name, internal,
    map_field_names, partition_values, primitive_array,
};
use crate::partition_row::UnifiedPartitionRow;
use crate::typed_read::system_table::SystemTableScan;

pub(super) struct EntryColumns<'a> {
    pub(super) scan: &'a SystemTableScan,
    pub(super) manifest: &'a TrinoManifestFile,
    pub(super) spec: &'a PartitionSpec,
}

impl EntryColumns<'_> {
    pub(super) fn build(
        &self,
        field: &Field,
        rows: &[EntryView<'_>],
        work: &mut impl JsonWork,
    ) -> Result<ArrayRef, ConnectorError> {
        work.step().map_err(kernel_failure)?;
        let array: ArrayRef =
            match field.name().as_str() {
                "status" => Arc::new(Int32Array::from_iter_values(rows.iter().map(|row| {
                    match row.entry.status() {
                        ManifestStatus::Existing => 0,
                        ManifestStatus::Added => 1,
                        ManifestStatus::Deleted => 2,
                    }
                }))),
                "snapshot_id" | "added_snapshot_id" => Arc::new(Int64Array::from_iter(
                    rows.iter().map(|row| row.sequences.snapshot_id),
                )),
                "sequence_number" | "data_sequence_number" => Arc::new(Int64Array::from_iter(
                    rows.iter().map(|row| row.sequences.data_sequence),
                )),
                "file_sequence_number" => Arc::new(Int64Array::from_iter(
                    rows.iter().map(|row| row.sequences.file_sequence),
                )),
                "readable_metrics" => metrics_column(rows, self.scan.metric_fields(), work)?,
                "data_file" => {
                    let DataType::Struct(fields) = field.data_type() else {
                        return Err(internal("Iceberg data_file declaration is not a ROW"));
                    };
                    let cols = fields
                        .iter()
                        .map(|child| self.file_column(child, rows, work))
                        .collect::<Result<Vec<_>, _>>()?;
                    Arc::new(
                        StructArray::try_new(fields.clone(), cols, None)
                            .map_err(|error| internal(error.to_string()))?,
                    )
                }
                _ => self.file_column(field, rows, work)?,
            };
        Ok(array)
    }

    fn file_column(
        &self,
        field: &Field,
        rows: &[EntryView<'_>],
        work: &mut impl JsonWork,
    ) -> Result<ArrayRef, ConnectorError> {
        let files = || rows.iter().map(|row| row.entry.data_file());
        let name = field.name().as_str();
        let array: ArrayRef = match name {
            "content" => Arc::new(Int32Array::from_iter_values(
                files().map(|file| content_code(file.content_type())),
            )),
            "file_path" => Arc::new(StringArray::from_iter_values(
                files().map(DataFile::file_path),
            )),
            "file_format" => Arc::new(StringArray::from_iter_values(
                files().map(|file| file_format_name(file.file_format())),
            )),
            "spec_id" => Arc::new(Int32Array::from_value(
                self.manifest.partition_spec_id(),
                rows.len(),
            )),
            "partition" => partition_column(field, files(), self.spec, self.scan.partitions())?,
            "record_count" | "file_size_in_bytes" => {
                let values = files()
                    .map(|file| {
                        let value = if name == "record_count" {
                            file.record_count()
                        } else {
                            file.file_size_in_bytes()
                        };
                        i64::try_from(value)
                            .map_err(|_| corrupt(format!("Iceberg {name} exceeds BIGINT")))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Arc::new(Int64Array::from(values))
            }
            "column_sizes" => count_column(files().map(DataFile::column_sizes))?,
            "value_counts" => count_column(files().map(DataFile::value_counts))?,
            "null_value_counts" => count_column(files().map(DataFile::null_value_counts))?,
            "nan_value_counts" => count_column(files().map(DataFile::nan_value_counts))?,
            "lower_bounds" | "upper_bounds" => {
                let lower = name == "lower_bounds";
                match field.data_type() {
                    DataType::Struct(_) => {
                        typed_bounds(field, rows, self.scan.table_schema(), lower)?
                    }
                    DataType::Map(..) => text_bounds(rows, self.scan.table_schema(), lower, work)?,
                    _ => return Err(internal("Invalid Iceberg bounds declaration")),
                }
            }
            "key_metadata" => {
                let mut builder = BinaryBuilder::new();
                for file in files() {
                    builder.append_option(file.key_metadata());
                }
                Arc::new(builder.finish())
            }
            "split_offsets" => {
                let mut builder = ListBuilder::new(Int64Builder::new());
                for file in files() {
                    let offsets = file.split_offsets();
                    if let Some(offsets) = offsets {
                        builder.values().append_slice(offsets);
                    }
                    builder.append(offsets.is_some());
                }
                Arc::new(builder.finish())
            }
            "equality_ids" => {
                let mut builder = ListBuilder::new(Int32Builder::new());
                for file in files() {
                    let ids = if file.content_type() == DataContentType::EqualityDeletes {
                        file.equality_ids()
                    } else {
                        None
                    };
                    if let Some(ids) = &ids {
                        builder.values().append_slice(ids);
                    }
                    builder.append(ids.is_some());
                }
                Arc::new(builder.finish())
            }
            "sort_order_id" => Arc::new(Int32Array::from_iter(files().map(|file| {
                if file.content_type() == DataContentType::PositionDeletes {
                    None
                } else {
                    file.sort_order_id()
                }
            }))),
            "referenced_data_file" => Arc::new(StringArray::from_iter(
                files().map(DataFile::referenced_data_file),
            )),
            "pos" => {
                let values = rows
                    .iter()
                    .map(|row| {
                        i64::try_from(row.ordinal)
                            .map_err(|_| corrupt("Iceberg manifest position exceeds BIGINT"))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Arc::new(Int64Array::from(values))
            }
            "manifest_location" => Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                self.manifest.path(),
                rows.len(),
            ))),
            "first_row_id" => Arc::new(Int64Array::from_iter(
                rows.iter().map(|row| row.first_row_id),
            )),
            "content_offset" => {
                Arc::new(Int64Array::from_iter(files().map(DataFile::content_offset)))
            }
            "content_size_in_bytes" => Arc::new(Int64Array::from_iter(
                files().map(DataFile::content_size_in_bytes),
            )),
            _ => return Err(internal(format!("Unknown Iceberg file column {name}"))),
        };
        Ok(array)
    }
}

fn count_column<'a>(
    rows: impl Iterator<Item = &'a HashMap<i32, u64>>,
) -> Result<ArrayRef, ConnectorError> {
    let mut builder = MapBuilder::new(
        Some(map_field_names()),
        Int32Builder::new(),
        Int64Builder::new(),
    );
    // TODO(B6c-2): the SDK 0.11 source merges absent and empty maps.
    // Preserve that boundary until the entry reader supplies presence.
    for row in rows {
        for (id, value) in row.iter().collect::<BTreeMap<_, _>>() {
            let count = i64::try_from(*value)
                .map_err(|_| corrupt(format!("Iceberg field {id} count exceeds BIGINT")))?;
            builder.keys().append_value(*id);
            builder.values().append_value(count);
        }
        builder
            .append(true)
            .map_err(|error| internal(error.to_string()))?;
    }
    Ok(Arc::new(builder.finish()))
}

fn partition_column<'a>(
    field: &Field,
    files: impl Iterator<Item = &'a DataFile>,
    spec: &PartitionSpec,
    partition: &UnifiedPartitionRow,
) -> Result<ArrayRef, ConnectorError> {
    let DataType::Struct(fields) = field.data_type() else {
        return Err(internal("Iceberg partition declaration is not a ROW"));
    };
    let values = files
        .map(|file| partition_values(file, spec, partition))
        .collect::<Result<Vec<_>, _>>()?;
    let children = fields
        .iter()
        .enumerate()
        .map(|(slot, field)| {
            let child = values
                .iter()
                .map(|row| match row.get(slot).and_then(Option::as_ref) {
                    Some(Literal::Primitive(value)) => {
                        Ok(Some(crate::read_snapshot::resolved_partition_primitive(
                            value,
                            &partition.fields[slot].primitive,
                        )))
                    }
                    None => Ok(None),
                    _ => Err(corrupt("Iceberg partition contains a non-primitive value")),
                })
                .collect::<Result<Vec<_>, _>>()?;
            primitive_array(
                field,
                &child.iter().map(Option::as_deref).collect::<Vec<_>>(),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(
        StructArray::try_new(fields.clone(), children, None)
            .map_err(|error| internal(error.to_string()))?,
    ))
}

// TODO(B6c-2): consume raw bound bytes and presence from the entry reader.
fn bounds_map(file: &DataFile, lower: bool) -> &HashMap<i32, Datum> {
    if lower {
        file.lower_bounds()
    } else {
        file.upper_bounds()
    }
}

fn typed_bounds(
    field: &Field,
    rows: &[EntryView<'_>],
    table_schema: &Schema,
    lower: bool,
) -> Result<ArrayRef, ConnectorError> {
    let DataType::Struct(fields) = field.data_type() else {
        return Err(internal("Iceberg typed bounds declaration is not a ROW"));
    };
    let children = fields
        .iter()
        .map(|field| {
            let id = field
                .name()
                .parse::<i32>()
                .map_err(|_| internal("Invalid Iceberg bounds row field ID"))?;
            let source = table_schema
                .field_by_id(id)
                .and_then(|field| match field.field_type.as_ref() {
                    crate::iceberg::spec::Type::Primitive(primitive) => Some(primitive),
                    _ => None,
                })
                .ok_or_else(|| internal("Iceberg bounds row has no corresponding primitive"))?;
            let values = rows
                .iter()
                .map(|row| {
                    bounds_map(row.entry.data_file(), lower)
                        .get(&id)
                        .map(|value| bounds::promote(value, source))
                        .transpose()
                })
                .collect::<Result<Vec<_>, _>>()?;
            primitive_array(
                field,
                &values
                    .iter()
                    .map(|value| value.as_ref().map(|datum| datum.literal()))
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(
        StructArray::try_new(fields.clone(), children, None)
            .map_err(|error| internal(error.to_string()))?,
    ))
}

fn text_bounds(
    rows: &[EntryView<'_>],
    table_schema: &Schema,
    lower: bool,
    work: &mut impl JsonWork,
) -> Result<ArrayRef, ConnectorError> {
    use crate::iceberg::metadata_columns::{
        RESERVED_FIELD_ID_DELETE_FILE_PATH, RESERVED_FIELD_ID_DELETE_FILE_POS,
    };

    let mut builder = MapBuilder::new(
        Some(map_field_names()),
        Int32Builder::new(),
        StringBuilder::new(),
    );
    for row in rows {
        let file = row.entry.data_file();
        for (id, value) in bounds_map(file, lower).iter().collect::<BTreeMap<_, _>>() {
            if file.content_type() == DataContentType::PositionDeletes
                && *id != RESERVED_FIELD_ID_DELETE_FILE_PATH
                && *id != RESERVED_FIELD_ID_DELETE_FILE_POS
            {
                continue;
            }
            work.step().map_err(kernel_failure)?;
            let current =
                table_schema
                    .field_by_id(*id)
                    .and_then(|field| match field.field_type.as_ref() {
                        crate::iceberg::spec::Type::Primitive(primitive) => Some(primitive),
                        _ => None,
                    });
            let value = current.map_or_else(
                || Ok(std::borrow::Cow::Borrowed(value)),
                |current| bounds::promote(value, current),
            )?;
            let rendered = match float_scalar(value.as_ref()) {
                Some(scalar) => {
                    let encoded =
                        json_value::encode(Input::Scalar(scalar), work).map_err(json_failure)?;
                    json_value::format(&encoded, work)
                        .map_err(json_failure)?
                        .trim_matches('"')
                        .to_owned()
                }
                None => bounds::text_value(value.as_ref(), BoundText::Human)?.into_owned(),
            };
            builder.keys().append_value(*id);
            builder.values().append_value(&rendered);
        }
        builder
            .append(true)
            .map_err(|error| internal(error.to_string()))?;
    }
    Ok(Arc::new(builder.finish()))
}

fn float_scalar(value: &Datum) -> Option<Scalar<'_>> {
    match value.literal() {
        PrimitiveLiteral::Float(value) => Some(Scalar::Real(value.0)),
        PrimitiveLiteral::Double(value) => Some(Scalar::Double(value.0)),
        _ => None,
    }
}

fn kernel_failure(error: novarocks_functions::KernelFailure) -> ConnectorError {
    use novarocks_functions::KernelFailure;
    use novarocks_spi::connector::ConnectorErrorKind;
    let kind = match error {
        KernelFailure::Cancelled => ConnectorErrorKind::Cancelled,
        KernelFailure::DeadlineExceeded => ConnectorErrorKind::DeadlineExceeded,
        KernelFailure::ResourceExhausted | KernelFailure::Capacity(_) => {
            ConnectorErrorKind::ResourceExhausted
        }
        _ => ConnectorErrorKind::Internal,
    };
    ConnectorError::new(kind, error.to_string())
}

fn json_failure(error: json_value::JsonError) -> ConnectorError {
    match error {
        json_value::JsonError::Host(error) => kernel_failure(error),
        error => internal(error.to_string()),
    }
}

fn metrics_column(
    rows: &[EntryView<'_>],
    fields: &[super::StatisticsField],
    work: &mut impl JsonWork,
) -> Result<ArrayRef, ConnectorError> {
    let mut builder = LargeBinaryBuilder::new();
    for row in rows {
        work.step().map_err(kernel_failure)?;
        let file = row.entry.data_file();
        let promoted = fields
            .iter()
            .map(|field| {
                work.step().map_err(kernel_failure)?;
                let bound = |lower| {
                    bounds_map(file, lower)
                        .get(&field.id)
                        .map(|bound| bounds::promote(bound, &field.primitive))
                        .transpose()
                };
                Ok((bound(true)?, bound(false)?))
            })
            .collect::<Result<Vec<_>, ConnectorError>>()?;
        let mut bound_text = Vec::with_capacity(fields.len());
        for (lower, upper) in &promoted {
            bound_text.push((json_bound(lower.as_deref())?, json_bound(upper.as_deref())?));
        }
        let mut attributes = Vec::with_capacity(fields.len());
        for (field, (lower, upper)) in fields.iter().zip(&bound_text) {
            let count = |map: &HashMap<i32, u64>| {
                map.get(&field.id).map_or(Ok(Input::Null), |value| {
                    i64::try_from(*value)
                        .map(|value| Input::Scalar(Scalar::BigInt(value)))
                        .map_err(|_| corrupt("Iceberg readable metric count exceeds BIGINT"))
                })
            };
            attributes.push([
                ("column_size", count(file.column_sizes())?),
                ("value_count", count(file.value_counts())?),
                ("null_value_count", count(file.null_value_counts())?),
                ("nan_value_count", count(file.nan_value_counts())?),
                ("lower_bound", lower.input()),
                ("upper_bound", upper.input()),
            ]);
        }
        let object = fields
            .iter()
            .zip(&attributes)
            .map(|(field, attributes)| (field.name.as_str(), Input::Object(attributes)))
            .collect::<Vec<_>>();
        let encoded = json_value::encode(Input::Object(&object), work).map_err(json_failure)?;
        builder.append_value(&encoded);
    }
    Ok(Arc::new(builder.finish()))
}

enum JsonBound<'a> {
    Inline(Input<'a>),
    Text(std::borrow::Cow<'a, str>),
}

impl JsonBound<'_> {
    fn input(&self) -> Input<'_> {
        match self {
            Self::Inline(value) => *value,
            Self::Text(value) => Input::Scalar(Scalar::String(value)),
        }
    }
}

fn json_bound(value: Option<&Datum>) -> Result<JsonBound<'_>, ConnectorError> {
    use crate::iceberg::spec::PrimitiveType as P;

    let Some(value) = value else {
        return Ok(JsonBound::Inline(Input::Null));
    };
    let scalar = match (value.data_type(), value.literal()) {
        (P::Boolean, PrimitiveLiteral::Boolean(value)) => Scalar::Boolean(*value),
        (P::Int, PrimitiveLiteral::Int(value)) => Scalar::Integer(*value),
        (P::Long, PrimitiveLiteral::Long(value)) => Scalar::BigInt(*value),
        (P::Float, PrimitiveLiteral::Float(value)) => Scalar::Real(value.0),
        (P::Double, PrimitiveLiteral::Double(value)) => Scalar::Double(value.0),
        _ => return Ok(JsonBound::Text(bounds::text_value(value, BoundText::Json)?)),
    };
    Ok(JsonBound::Inline(Input::Scalar(scalar)))
}

/// Conservative size of a projected row before constructing the page. This
/// only selects a soft page boundary. The shared Arrow planner then measures
/// and builds the exact output buffers.
pub(super) fn row_bytes(
    field: &Field,
    row: &EntryView<'_>,
    scan: &SystemTableScan,
    manifest: &TrinoManifestFile,
) -> u128 {
    let file = row.entry.data_file();
    match field.name().as_str() {
        "file_path" => 16 + file.file_path().len() as u128,
        "manifest_location" => 16 + manifest.path().len() as u128,
        "referenced_data_file" => {
            16 + file.referenced_data_file().map_or(0, |path| path.len()) as u128
        }
        "key_metadata" => 16 + file.key_metadata().map_or(0, <[u8]>::len) as u128,
        "split_offsets" => 16 + file.split_offsets().map_or(0, <[i64]>::len) as u128 * 8,
        "equality_ids" => 16 + file.equality_ids().map_or(0, |ids| ids.len()) as u128 * 4,
        "column_sizes" => 16 + file.column_sizes().len() as u128 * 16,
        "value_counts" => 16 + file.value_counts().len() as u128 * 16,
        "null_value_counts" => 16 + file.null_value_counts().len() as u128 * 16,
        "nan_value_counts" => 16 + file.nan_value_counts().len() as u128 * 16,
        "partition" => {
            16 + scan.partitions().fields.len() as u128 * 16
                + file
                    .partition()
                    .fields()
                    .iter()
                    .filter_map(Option::as_ref)
                    .map(|literal| match literal {
                        Literal::Primitive(value) => variable_size(value),
                        _ => 0,
                    })
                    .sum::<u128>()
        }
        "lower_bounds" | "upper_bounds" => {
            let values = bounds_map(file, field.name() == "lower_bounds");
            let width = match field.data_type() {
                DataType::Struct(fields) => fields.len(),
                _ => values.len(),
            };
            16 + width as u128 * 48
                + values
                    .values()
                    .map(|value| variable_size(value.literal()) * 2)
                    .sum::<u128>()
        }
        "readable_metrics" => {
            32 + scan.metric_shape_bytes()
                + file
                    .lower_bounds()
                    .values()
                    .chain(file.upper_bounds().values())
                    .map(|value| variable_size(value.literal()) * 2)
                    .sum::<u128>()
        }
        "data_file" => match field.data_type() {
            DataType::Struct(fields) => {
                16 + fields
                    .iter()
                    .map(|child| row_bytes(child, row, scan, manifest))
                    .sum::<u128>()
            }
            _ => 16,
        },
        _ => 32,
    }
}

fn variable_size(value: &PrimitiveLiteral) -> u128 {
    match value {
        PrimitiveLiteral::String(text) => text.len() as u128,
        PrimitiveLiteral::Binary(bytes) => bytes.len() as u128,
        _ => 0,
    }
}
