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
#[allow(
    dead_code,
    reason = "ADD FILES registration consumes the footer preview in P08"
)]
pub(crate) mod field_identity;
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

pub(crate) fn corrupt_delete_content(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(
        novarocks_spi::connector::ConnectorErrorKind::CorruptData,
        message,
    )
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

/// Visit projected batches without retaining the complete delete file.
pub(crate) fn visit_parquet_batches(
    access: &FsAccessHandle,
    path: &str,
    file_size: Option<u64>,
    projection: FileProjection,
    predicates: Vec<ScanPredicate>,
    context: FileReadContext,
    mut visit: impl FnMut(FileBatch) -> Result<(), String>,
) -> Result<(), String> {
    context.check_active().map_err(|e| e.to_string())?;
    let size = match known_size(file_size) {
        Some(size) => size,
        None => context
            .runtime
            .block_on_u64(Box::pin(stat_size(bind(access, path, 0)?, context.clone())))
            .map_err(|e| e.to_string())?,
    };
    let mut request = whole_file_request(bind(access, path, size)?, projection, context);
    request.predicates = predicates;
    let mut reader = open_file_reader(request).map_err(|e| e.to_string())?;
    let outcome = (|| {
        while let Some(batch) = reader.next_batch().map_err(|e| e.to_string())? {
            visit(batch)?;
        }
        Ok(())
    })();
    let closed = reader.close().map_err(|e| e.to_string());
    outcome.and(closed)
}

pub(crate) async fn visit_parquet_batches_async(
    access: &FsAccessHandle,
    path: &str,
    file_size: Option<u64>,
    projection: FileProjection,
    predicates: Vec<ScanPredicate>,
    context: FileReadContext,
    visit: impl FnMut(FileBatch) -> Result<(), ConnectorError>,
) -> Result<(), ConnectorError> {
    visit_parquet_batches_async_with_options(
        access,
        path,
        file_size,
        projection,
        predicates,
        context,
        Default::default(),
        visit,
    )
    .await
    .map(|_| ())
}

pub(crate) async fn visit_parquet_batches_async_with_options(
    access: &FsAccessHandle,
    path: &str,
    file_size: Option<u64>,
    projection: FileProjection,
    predicates: Vec<ScanPredicate>,
    context: FileReadContext,
    options: novarocks_fs::FileReaderOptions,
    mut visit: impl FnMut(FileBatch) -> Result<(), ConnectorError>,
) -> Result<novarocks_fs::FileMetricsSnapshot, ConnectorError> {
    context.check_active().map_err(map_file_error)?;
    let size = match known_size(file_size) {
        Some(size) => size,
        None => stat_size(
            access
                .bind_location(path, FileIdentity::new(path, 0, None))
                .map_err(map_file_error)?,
            context.clone(),
        )
        .await
        .map_err(map_file_error)?,
    };
    let mut request = whole_file_request(
        access
            .bind_location(path, FileIdentity::new(path, size, None))
            .map_err(map_file_error)?,
        projection,
        context.clone(),
    );
    request.predicates = predicates;
    request.options = iceberg_reader_options(options);
    let mut reader = open_file_reader_async(request, None)
        .await
        .map_err(map_file_error)?;
    let outcome = async {
        while let Some(batch) = reader.next_batch().await.map_err(map_file_error)? {
            context.check_active().map_err(map_file_error)?;
            visit(batch)?;
            // Cached input may make next_batch immediately ready. Release the
            // driver after each bounded batch, preserving full artifact validation.
            tokio::task::yield_now().await;
        }
        Ok(())
    }
    .await;
    let metrics = reader.metrics_snapshot();
    let closed = reader.close().map_err(map_file_error);
    outcome.and(closed).map(|_| metrics)
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
        .block_on_bytes(Box::pin(
            read_range(access, path, file_size, range, context)
                .map_err(|error| error.to_string())?,
        ))
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
    // Historical non-delete callers retain their string boundary explicitly.
    read_bytes_async_typed(access, path, file_size, range, context)
        .await
        .map_err(|error| error.to_string())
}

pub(crate) async fn read_bytes_async_typed(
    access: &FsAccessHandle,
    path: &str,
    file_size: Option<u64>,
    range: FileReadRange,
    context: &FileReadContext,
) -> Result<Bytes, ConnectorError> {
    context.check_active().map_err(map_file_error)?;
    read_range(access, path, file_size, range, context)
        .map_err(map_file_error)?
        .await
        .map_err(map_file_error)
}

fn known_size(file_size: Option<u64>) -> Option<u64> {
    file_size.filter(|size| *size > 0)
}

fn bind(access: &FsAccessHandle, path: &str, size: u64) -> Result<BoundFile, String> {
    access
        .bind_location(path, FileIdentity::new(path, size, None))
        .map_err(|error| error.to_string())
}

/// Iceberg table metadata and the real Parquet schema own read semantics.
/// Arrow's optional embedded schema must never assign or erase field IDs.
// Design: ADR-0213 (docs/adr/ADR-0213-iceberg-read-interpretation-and-current-write-authority.md)
pub(crate) fn iceberg_reader_options(
    mut options: novarocks_fs::FileReaderOptions,
) -> novarocks_fs::FileReaderOptions {
    options.parquet_interpretation_policy =
        novarocks_fs::ParquetInterpretationPolicy::SkipArrowHint;
    options
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
        options: iceberg_reader_options(Default::default()),
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
) -> FileResult<impl std::future::Future<Output = FileResult<Bytes>> + Send + 'static> {
    let provisional = access.bind_location(
        path,
        FileIdentity::new(path, file_size.unwrap_or_default(), None),
    )?;
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
pub(crate) mod tests;
