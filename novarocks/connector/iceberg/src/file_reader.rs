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

//! Iceberg-owned use of connector-neutral physical file I/O.

use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow::array::{ArrayData, ArrayRef, make_array};
use arrow::datatypes::{DataType, Field, FieldRef};
use bytes::Bytes;
use novarocks_fs::{
    BoundFile, FileBatch, FileFormat, FileIdentity, FileProjection, FileReadBudget,
    FileReadContext, FileReadRange, FileReadRequest, FileResult, FsAccessHandle, MinMaxPredicateOp,
    MinMaxPredicateValue, PhysicalPruning, ScanPredicate, ScanPredicateDomain, ScanPredicateSource,
    open_file_reader, open_file_reader_async,
};
use novarocks_spi::connector::ConnectorError;

use crate::scan_model::{
    IcebergPhysicalPredicate, IcebergPhysicalPredicateDomain, IcebergPhysicalPredicateOp,
    IcebergPhysicalPredicateValue,
};

#[path = "equality_delete.rs"]
pub mod equality_delete;

#[path = "execution_installer.rs"]
pub mod execution_installer;
#[path = "execution_payload.rs"]
pub mod execution_payload;
#[path = "variant.rs"]
pub mod variant;

/// Re-label an Arrow array with an equivalent Iceberg schema type while
/// preserving every physical buffer. Iceberg schema evolution can change
/// nested field metadata without changing the underlying Parquet layout; this
/// belongs to the provider reader, not to the execution engine.
pub fn retag_iceberg_array(array: &ArrayRef, target: &DataType) -> Result<ArrayRef, String> {
    retag_iceberg_array_data(array.to_data(), target).map(make_array)
}

fn retag_iceberg_array_data(data: ArrayData, target: &DataType) -> Result<ArrayData, String> {
    use DataType::*;

    if data.data_type() == target
        && !matches!(
            target,
            DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _) | DataType::Struct(_)
        )
    {
        return Ok(data);
    }
    let source = data.data_type().clone();
    let children = match (&source, target) {
        (Decimal128(_, source_scale), Decimal128(_, target_scale))
        | (Decimal256(_, source_scale), Decimal256(_, target_scale))
            if source_scale == target_scale =>
        {
            Vec::new()
        }
        (Timestamp(source_unit, _), Timestamp(target_unit, _)) if source_unit == target_unit => {
            Vec::new()
        }
        (Utf8, Binary) | (Binary, Utf8) => Vec::new(),
        (List(source_field), List(target_field)) => {
            let child =
                retag_iceberg_array_data(data.child_data()[0].clone(), target_field.data_type())?;
            let target = List(runtime_iceberg_field(target_field, source_field, &child));
            return rebuild_iceberg_array_data(data, &source, target, vec![child]);
        }
        (LargeList(source_field), LargeList(target_field)) => {
            let child =
                retag_iceberg_array_data(data.child_data()[0].clone(), target_field.data_type())?;
            let target = LargeList(runtime_iceberg_field(target_field, source_field, &child));
            return rebuild_iceberg_array_data(data, &source, target, vec![child]);
        }
        (Map(source_field, source_ordered), Map(target_field, target_ordered))
            if source_ordered == target_ordered =>
        {
            let child =
                retag_iceberg_array_data(data.child_data()[0].clone(), target_field.data_type())?;
            let target = Map(
                runtime_iceberg_field(target_field, source_field, &child),
                *target_ordered,
            );
            return rebuild_iceberg_array_data(data, &source, target, vec![child]);
        }
        (Struct(source_fields), Struct(target_fields))
            if data.child_data().len() == target_fields.len()
                && source_fields.len() == target_fields.len() =>
        {
            let children = target_fields
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    retag_iceberg_array_data(data.child_data()[index].clone(), field.data_type())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let fields = target_fields
                .iter()
                .zip(source_fields.iter())
                .zip(children.iter())
                .map(|((target_field, source_field), child)| {
                    runtime_iceberg_field(target_field, source_field, child)
                })
                .collect::<Vec<_>>();
            return rebuild_iceberg_array_data(data, &source, Struct(fields.into()), children);
        }
        _ => {
            return Err(format!(
                "Iceberg physical Arrow type cannot be metadata-retagged from {source:?} to {target:?}"
            ));
        }
    };
    rebuild_iceberg_array_data(data, &source, target.clone(), children)
}

/// Re-tag one nested child against the Iceberg schema field, widening its
/// nullability only for nulls the array actually carries.
///
/// The Parquet carrier's own `nullable` flag is not evidence of a null; the
/// Iceberg schema is this table's authority on the shape, and a MAP key it
/// declares required must stay required. The null count is the fact.
fn runtime_iceberg_field(target: &FieldRef, _source: &FieldRef, child: &ArrayData) -> FieldRef {
    let nullable = target.is_nullable() || child.null_count() > 0;
    Arc::new(
        Field::new(target.name(), child.data_type().clone(), nullable)
            .with_metadata(target.metadata().clone()),
    )
}

fn rebuild_iceberg_array_data(
    data: ArrayData,
    source: &DataType,
    target: DataType,
    children: Vec<ArrayData>,
) -> Result<ArrayData, String> {
    let mut builder = data.into_builder().data_type(target.clone());
    if !children.is_empty() {
        builder = builder.child_data(children);
    }
    builder.build().map_err(|error| {
        format!("rebuild Iceberg Arrow metadata from {source:?} to {target:?}: {error}")
    })
}

/// Lower provider-owned Iceberg predicates into connector-neutral physical
/// file predicates.  Field IDs remain authoritative across Iceberg renames.
pub fn physical_predicates_to_file_predicates(
    predicates: &[IcebergPhysicalPredicate],
) -> Vec<ScanPredicate> {
    predicates
        .iter()
        .filter_map(|predicate| {
            let value = |value: &IcebergPhysicalPredicateValue| match value {
                IcebergPhysicalPredicateValue::Boolean(value) => {
                    MinMaxPredicateValue::Boolean(*value)
                }
                IcebergPhysicalPredicateValue::Int32(value) => MinMaxPredicateValue::Int32(*value),
                IcebergPhysicalPredicateValue::Int64(value) => MinMaxPredicateValue::Int64(*value),
                // Parquet exposes DATE statistics as INT32 day counts.
                IcebergPhysicalPredicateValue::Date32(value) => MinMaxPredicateValue::Int32(*value),
            };
            let domain = match &predicate.domain {
                IcebergPhysicalPredicateDomain::Range { op, value: literal } => {
                    ScanPredicateDomain::Range {
                        op: match op {
                            IcebergPhysicalPredicateOp::Eq => MinMaxPredicateOp::Eq,
                            IcebergPhysicalPredicateOp::Lt => MinMaxPredicateOp::Lt,
                            IcebergPhysicalPredicateOp::Le => MinMaxPredicateOp::Le,
                            IcebergPhysicalPredicateOp::Gt => MinMaxPredicateOp::Gt,
                            IcebergPhysicalPredicateOp::Ge => MinMaxPredicateOp::Ge,
                        },
                        value: value(literal),
                    }
                }
                IcebergPhysicalPredicateDomain::DiscreteSet { values } => {
                    let values = values.iter().map(value).collect::<Vec<_>>();
                    if values.is_empty() {
                        return None;
                    }
                    let min = values.first()?.clone();
                    let max = values.last()?.clone();
                    ScanPredicateDomain::DiscreteSet { values, min, max }
                }
            };
            Some(
                ScanPredicate::new(
                    predicate.column.clone(),
                    domain,
                    ScanPredicateSource::Static,
                )
                .with_physical_field_id(predicate.field_id),
            )
        })
        .collect()
}

/// Preserve the connector-neutral error taxonomy at the provider's physical
/// filesystem boundary.
pub fn map_file_error(error: novarocks_fs::FileError) -> ConnectorError {
    ConnectorError::from(error)
}

pub fn read_parquet_batches(
    access: &FsAccessHandle,
    path: &str,
    file_size: Option<u64>,
    projection: FileProjection,
    context: FileReadContext,
) -> Result<Vec<FileBatch>, String> {
    context.check_active().map_err(|error| error.to_string())?;
    let file = match known_size(file_size) {
        Some(size) => bind(access, path, size)?,
        None => {
            let size = context
                .runtime
                .block_on_u64(Box::pin(stat_size(bind(access, path, 0)?, context.clone())))
                .map_err(|error| error.to_string())?;
            context.check_active().map_err(|error| error.to_string())?;
            bind(access, path, size)?
        }
    };
    let mut reader = open_file_reader(whole_file_request(file, projection, context))
        .map_err(|error| error.to_string())?;
    let mut batches = Vec::new();
    while let Some(batch) = reader.next_batch().map_err(|error| error.to_string())? {
        batches.push(batch);
    }
    reader.close().map_err(|error| error.to_string())?;
    Ok(batches)
}

/// Awaited [`read_parquet_batches`]: an unknown size is a HEAD and the file
/// is read by the awaited reader, both through the source's range service
/// when the read has one.
pub async fn read_parquet_batches_async(
    access: &FsAccessHandle,
    path: &str,
    file_size: Option<u64>,
    projection: FileProjection,
    context: FileReadContext,
) -> Result<Vec<FileBatch>, String> {
    context.check_active().map_err(|error| error.to_string())?;
    let file = match known_size(file_size) {
        Some(size) => bind(access, path, size)?,
        None => {
            let size = stat_size(bind(access, path, 0)?, context.clone())
                .await
                .map_err(|error| error.to_string())?;
            context.check_active().map_err(|error| error.to_string())?;
            bind(access, path, size)?
        }
    };
    let mut reader = open_file_reader_async(whole_file_request(file, projection, context), None)
        .await
        .map_err(|error| error.to_string())?;
    let mut batches = Vec::new();
    while let Some(batch) = reader
        .next_batch()
        .await
        .map_err(|error| error.to_string())?
    {
        batches.push(batch);
    }
    reader.close().map_err(|error| error.to_string())?;
    Ok(batches)
}

pub fn read_bytes(
    access: &FsAccessHandle,
    path: &str,
    file_size: Option<u64>,
    range: FileReadRange,
    context: &FileReadContext,
) -> Result<Bytes, String> {
    context.check_active().map_err(|error| error.to_string())?;
    context
        .runtime
        .block_on_bytes(Box::pin(read_range(
            access, path, file_size, range, context,
        )?))
        .map_err(|error| error.to_string())
}

/// Awaited [`read_bytes`].
pub async fn read_bytes_async(
    access: &FsAccessHandle,
    path: &str,
    file_size: Option<u64>,
    range: FileReadRange,
    context: &FileReadContext,
) -> Result<Bytes, String> {
    context.check_active().map_err(|error| error.to_string())?;
    read_range(access, path, file_size, range, context)?
        .await
        .map_err(|error| error.to_string())
}

fn known_size(file_size: Option<u64>) -> Option<u64> {
    file_size.filter(|size| *size > 0)
}

fn bind(access: &FsAccessHandle, path: &str, size: u64) -> Result<BoundFile, String> {
    access
        .bind_location(path, FileIdentity::new(path, size, None))
        .map_err(|error| error.to_string())
}

fn whole_file_request(
    file: BoundFile,
    projection: FileProjection,
    context: FileReadContext,
) -> FileReadRequest {
    FileReadRequest {
        file,
        format: FileFormat::Parquet,
        range: FileReadRange::WholeFile,
        projection,
        budget: FileReadBudget {
            max_rows: NonZeroUsize::new(4096).expect("constant is nonzero"),
            max_bytes: NonZeroUsize::new(64 * 1024 * 1024).expect("constant is nonzero"),
        },
        predicates: Vec::new(),
        pruning: PhysicalPruning::default(),
        options: Default::default(),
        cache: None,
        prepared_input: None,
        context,
    }
}

/// The size of `file` in storage: a HEAD through the source's range service
/// when the read has one, under the read's cancellation and deadline.
async fn stat_size(file: BoundFile, context: FileReadContext) -> FileResult<u64> {
    let cancellation = context.bounded_cancellation();
    let Some(range) = context.range else {
        return file.stat(&cancellation).await;
    };
    let mut request = range.stat_wait(file, cancellation).await?;
    let size = request.size_ready().await;
    let exit = request.drained().await;
    let size = size?;
    exit?;
    Ok(size)
}

/// One read of `range` of `path`. With a range service, an unknown size is
/// a HEAD first and the read is a managed request, so neither bypasses the
/// source's windows and exit accounting; without one it is a direct read.
fn read_range(
    access: &FsAccessHandle,
    path: &str,
    file_size: Option<u64>,
    range: FileReadRange,
    context: &FileReadContext,
) -> Result<impl std::future::Future<Output = FileResult<Bytes>> + Send + 'static, String> {
    let provisional = bind(access, path, file_size.unwrap_or_default())?;
    let access = access.clone();
    let path = path.to_string();
    let context = context.clone();
    Ok(async move {
        let cancellation = context.bounded_cancellation();
        let Some(binding) = context.range.clone() else {
            return provisional.read(range, &cancellation).await;
        };
        let size = match known_size(file_size) {
            Some(size) => size,
            None => stat_size(provisional, context.clone()).await?,
        };
        let range = match range {
            FileReadRange::Bounded { .. } => range,
            FileReadRange::WholeFile if size == 0 => return Ok(Bytes::new()),
            FileReadRange::WholeFile => FileReadRange::bounded(0, size)?,
        };
        let file = access.bind_location(&path, FileIdentity::new(&path, size, None))?;
        let mut request = binding.start_wait(file, range, cancellation).await?;
        let bytes = request.result_ready().await;
        let exit = request.drained().await;
        let bytes = bytes?;
        exit?;
        Ok(bytes)
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Int32Array, Int64Array, MapArray, StringArray, StructArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field, Fields};

    use super::*;

    #[test]
    fn lowers_static_predicates_by_iceberg_field_id() {
        let predicates = physical_predicates_to_file_predicates(&[
            IcebergPhysicalPredicate {
                field_id: 7,
                column: "renamed".to_string(),
                domain: IcebergPhysicalPredicateDomain::Range {
                    op: IcebergPhysicalPredicateOp::Ge,
                    value: IcebergPhysicalPredicateValue::Date32(20_000),
                },
            },
            IcebergPhysicalPredicate {
                field_id: 8,
                column: "empty".to_string(),
                domain: IcebergPhysicalPredicateDomain::DiscreteSet { values: Vec::new() },
            },
        ]);

        assert_eq!(predicates.len(), 1);
        assert_eq!(predicates[0].column(), "renamed");
        assert_eq!(predicates[0].physical_field_id(), Some(7));
        assert_eq!(predicates[0].source(), ScanPredicateSource::Static);
        assert_eq!(
            predicates[0].domain(),
            &ScanPredicateDomain::Range {
                op: MinMaxPredicateOp::Ge,
                value: MinMaxPredicateValue::Int32(20_000),
            }
        );
    }

    #[test]
    fn retags_equivalent_physical_string_buffers_without_reencoding() {
        let source: ArrayRef = Arc::new(StringArray::from(vec!["iceberg", "provider"]));
        let retagged = retag_iceberg_array(&source, &DataType::Binary).expect("retag utf8");

        assert_eq!(retagged.data_type(), &DataType::Binary);
        assert_eq!(retagged.to_data().buffers(), source.to_data().buffers());
    }

    #[test]
    fn retags_iceberg_map_without_narrowing_runtime_key_nullability() {
        let keys = Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef;
        let values = Arc::new(Int64Array::from(vec![Some(10), Some(20)])) as ArrayRef;
        let entries = StructArray::new(
            Fields::from(vec![
                Field::new("key", DataType::Int32, true),
                Field::new("value", DataType::Int64, true),
            ]),
            vec![keys, values],
            None,
        );
        let source = Arc::new(MapArray::new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::from_lengths([2]),
            entries,
            None,
            false,
        )) as ArrayRef;
        let target = DataType::Map(
            Arc::new(Field::new(
                "iceberg_entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("iceberg_key", DataType::Int32, false)),
                        Arc::new(Field::new("iceberg_value", DataType::Int64, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );

        let out = retag_iceberg_array(&source, &target).expect("retag nullable map key");

        let DataType::Map(entries, false) = out.data_type() else {
            panic!("expected map");
        };
        assert_eq!(entries.name(), "iceberg_entries");
        let DataType::Struct(fields) = entries.data_type() else {
            panic!("expected entries struct");
        };
        assert_eq!(fields[0].name(), "iceberg_key");
        assert!(fields[0].is_nullable());
        let map = out.as_any().downcast_ref::<MapArray>().expect("map array");
        assert!(map.keys().is_null(1));
    }
}
