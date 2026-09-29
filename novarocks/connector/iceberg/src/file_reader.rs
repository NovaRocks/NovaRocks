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
    request.options = options;
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
pub(crate) mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Int32Array, Int64Array, MapArray, StringArray, StructArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field, Fields};
    use arrow::record_batch::RecordBatch;

    use super::*;

    #[derive(Clone, Debug, Default)]
    pub(crate) struct RangeReceipts(
        pub(crate) Arc<std::sync::Mutex<Vec<(String, u64, Option<u64>)>>>,
        Option<opendal::ErrorKind>,
    );
    #[derive(Debug)]
    pub(crate) struct RangeReceiptAccessor<A: opendal::raw::Access> {
        inner: A,
        receipts: RangeReceipts,
    }
    impl<A: opendal::raw::Access> opendal::raw::Layer<A> for RangeReceipts {
        type LayeredAccess = RangeReceiptAccessor<A>;
        fn layer(&self, inner: A) -> Self::LayeredAccess {
            RangeReceiptAccessor {
                inner,
                receipts: self.clone(),
            }
        }
    }
    impl<A: opendal::raw::Access> opendal::raw::LayeredAccess for RangeReceiptAccessor<A> {
        type Inner = A;
        type Reader = A::Reader;
        type Writer = A::Writer;
        type Lister = A::Lister;
        type Deleter = A::Deleter;
        fn inner(&self) -> &Self::Inner {
            &self.inner
        }
        async fn write(
            &self,
            path: &str,
            args: opendal::raw::OpWrite,
        ) -> opendal::Result<(opendal::raw::RpWrite, Self::Writer)> {
            self.inner.write(path, args).await
        }
        async fn delete(&self) -> opendal::Result<(opendal::raw::RpDelete, Self::Deleter)> {
            self.inner.delete().await
        }
        async fn list(
            &self,
            path: &str,
            args: opendal::raw::OpList,
        ) -> opendal::Result<(opendal::raw::RpList, Self::Lister)> {
            self.inner.list(path, args).await
        }
        async fn read(
            &self,
            path: &str,
            args: opendal::raw::OpRead,
        ) -> opendal::Result<(opendal::raw::RpRead, Self::Reader)> {
            self.receipts.0.lock().unwrap().push((
                path.to_string(),
                args.range().offset(),
                args.range().size(),
            ));
            if let Some(kind) = self.receipts.1 {
                return Err(opendal::Error::new(
                    kind,
                    "identified delete-object read failure",
                ));
            }
            self.inner.read(path, args).await
        }
    }
    pub(crate) fn recorded_access(
        access: &FsAccessHandle,
        receipts: &RangeReceipts,
    ) -> FsAccessHandle {
        FsAccessHandle::new(
            access.access_domain(),
            access.scheme(),
            access.operator().layer(receipts.clone()),
            access.authority().map(str::to_string),
            access.root().map(str::to_string),
            access.paths().to_vec(),
        )
    }

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
    #[test]
    fn position_path_predicate_reads_related_groups_and_reports_layout_degradation() {
        use arrow::array::BinaryArray;
        use novarocks_fs::{
            FileCancellation, FileReaderOptions, FsAccessResolver, TokioFileIoRuntime,
            TokioFileTaskSpawner,
        };
        use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
        use parquet::basic::Compression;
        use parquet::file::properties::{EnabledStatistics, WriterProperties};
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("file_path", DataType::Utf8, false).with_metadata(
                [(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    crate::delete_semantics::POSITION_FILE_PATH_FIELD_ID.to_string(),
                )]
                .into_iter()
                .collect(),
            ),
            Field::new("pos", DataType::Int64, false).with_metadata(
                [(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    (i32::MAX - 102).to_string(),
                )]
                .into_iter()
                .collect(),
            ),
            Field::new("unrelated_payload", DataType::Binary, false),
        ]));
        let paths = (0..16)
            .map(|group| format!("/warehouse/{}-{group:03}.parquet", "p".repeat(1000)))
            .collect::<Vec<_>>();
        let target = paths[7].clone();
        let mut metrics = Vec::new();
        for layout in ["full", "missing", "truncated"] {
            let path = directory.path().join(format!("{layout}.parquet"));
            let props = WriterProperties::builder()
                .set_max_row_group_row_count(Some(512))
                .set_dictionary_enabled(false)
                .set_compression(Compression::UNCOMPRESSED)
                .set_statistics_enabled(if layout == "missing" {
                    EnabledStatistics::None
                } else {
                    EnabledStatistics::Chunk
                })
                .set_statistics_truncate_length(if layout == "truncated" {
                    Some(16)
                } else {
                    None
                })
                .build();
            let mut writer = ArrowWriter::try_new(
                std::fs::File::create(&path).unwrap(),
                schema.clone(),
                Some(props),
            )
            .unwrap();
            let payload = vec![17u8; 2048];
            for data_path in &paths {
                let batch = arrow::record_batch::RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                            data_path.as_str(),
                            512,
                        ))),
                        Arc::new(Int64Array::from_iter_values(0..512)),
                        Arc::new(BinaryArray::from_iter_values(std::iter::repeat_n(
                            payload.as_slice(),
                            512,
                        ))),
                    ],
                )
                .unwrap();
                writer.write(&batch).unwrap();
                writer.flush().unwrap();
            }
            writer.close().unwrap();
            let size = std::fs::metadata(&path).unwrap().len();
            let access = FsAccessResolver::new()
                .resolve_location(
                    novarocks_spi::connector::StorageAccessDomainId::from_bytes([1; 32]),
                    path.to_string_lossy(),
                    None,
                )
                .unwrap();
            assert!(
                size > 64 * 1024,
                "fixture exceeds the whole-file probe threshold"
            );
            for coalesce in [false, true] {
                let receipts = RangeReceipts::default();
                let access = recorded_access(&access, &receipts);
                let context = FileReadContext {
                    cancellation: FileCancellation::new(),
                    deadline: None,
                    runtime: Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
                    task_spawner: Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
                    range: None,
                };
                let predicate = ScanPredicate::new(
                    "file_path",
                    ScanPredicateDomain::Range {
                        op: MinMaxPredicateOp::Eq,
                        value: MinMaxPredicateValue::ByteArray(target.as_bytes().to_vec()),
                    },
                    ScanPredicateSource::Static,
                )
                .with_physical_field_id(crate::delete_semantics::POSITION_FILE_PATH_FIELD_ID);
                let mut matched = Vec::new();
                let m = runtime
                    .block_on(visit_parquet_batches_async_with_options(
                        &access,
                        &path.to_string_lossy(),
                        Some(size),
                        FileProjection::RootNames(vec!["file_path".into(), "pos".into()]),
                        vec![predicate],
                        context,
                        FileReaderOptions {
                            coalesce_reads: coalesce,
                            ..Default::default()
                        },
                        |batch| {
                            let file_paths = batch
                                .batch
                                .column(0)
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .unwrap();
                            let positions = batch
                                .batch
                                .column(1)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap();
                            for row in 0..batch.batch.num_rows() {
                                if file_paths.value(row) == target {
                                    matched.push(positions.value(row));
                                }
                            }
                            Ok(())
                        },
                    ))
                    .unwrap();
                assert_eq!(matched, (0..512).collect::<Vec<i64>>());
                if layout == "full" {
                    assert_eq!(
                        (m.row_groups_read, m.row_groups_pruned, m.rows_decoded),
                        (1, 15, 512)
                    );
                    assert!(
                        m.bytes_read < size / 8,
                        "related ranges plus footer stay below an unrelated-file scan"
                    );
                } else {
                    assert_eq!(
                        (m.row_groups_read, m.row_groups_pruned, m.rows_decoded),
                        (16, 0, 8192)
                    );
                }
                let ranges = receipts.0.lock().unwrap();
                let range_bytes: u64 = ranges
                    .iter()
                    .map(|(_, start, length)| length.unwrap_or(size - start))
                    .sum();
                assert_eq!(
                    range_bytes, m.bytes_read,
                    "metrics count every footer and coalesced physical read"
                );
                assert_eq!(ranges.len() as u64, m.read_requests);
                assert!(
                    ranges
                        .iter()
                        .all(|(_, start, length)| start + length.unwrap_or(size - start) <= size)
                );
                assert!(
                    ranges
                        .iter()
                        .any(|(_, start, length)| start + length.unwrap_or(size - start) == size),
                    "footer tail is a physical range"
                );
                if layout == "full" {
                    assert!(
                        ranges
                            .iter()
                            .all(|(_, start, length)| !(*start == 0 && *length == Some(size))),
                        "large selective file is not probed whole"
                    );
                }
                eprintln!(
                    "POSITION_RANGE_RECEIPT layout={layout} coalesce={coalesce} file_bytes={size} physical_ranges={:?} metrics={m:?}",
                    *ranges
                );
                assert!(m.read_requests > 0);
                metrics.push((layout, coalesce, m));
            }
        }
        for coalesce in [false, true] {
            let full = metrics
                .iter()
                .find(|(layout, c, _)| *layout == "full" && *c == coalesce)
                .unwrap()
                .2;
            let missing = metrics
                .iter()
                .find(|(layout, c, _)| *layout == "missing" && *c == coalesce)
                .unwrap()
                .2;
            assert!(missing.bytes_read > full.bytes_read * 8);
        }
    }
    #[test]
    fn small_position_file_reports_whole_probe_even_when_one_group_matches() {
        use novarocks_fs::{
            FileCancellation, FileReaderOptions, FsAccessResolver, TokioFileIoRuntime,
            TokioFileTaskSpawner,
        };
        use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
        use parquet::file::properties::WriterProperties;
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("small-position.parquet");
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("file_path", DataType::Utf8, false).with_metadata(
                [(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    crate::delete_semantics::POSITION_FILE_PATH_FIELD_ID.to_string(),
                )]
                .into_iter()
                .collect(),
            ),
            Field::new("pos", DataType::Int64, false).with_metadata(
                [(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    (i32::MAX - 102).to_string(),
                )]
                .into_iter()
                .collect(),
            ),
        ]));
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(&path).unwrap(),
            schema.clone(),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_row_count(Some(1))
                    .build(),
            ),
        )
        .unwrap();
        for group in 0..16 {
            writer
                .write(
                    &RecordBatch::try_new(
                        schema.clone(),
                        vec![
                            Arc::new(StringArray::from(vec![format!("/data/{group:03}.parquet")])),
                            Arc::new(Int64Array::from(vec![group])),
                        ],
                    )
                    .unwrap(),
                )
                .unwrap();
            writer.flush().unwrap();
        }
        writer.close().unwrap();
        let size = std::fs::metadata(&path).unwrap().len();
        assert!(size <= 64 * 1024);
        let access = FsAccessResolver::new()
            .resolve_location(
                novarocks_spi::connector::StorageAccessDomainId::from_bytes([1; 32]),
                path.to_string_lossy(),
                None,
            )
            .unwrap();
        for coalesce in [false, true] {
            let receipts = RangeReceipts::default();
            let access = recorded_access(&access, &receipts);
            let context = FileReadContext {
                cancellation: FileCancellation::new(),
                deadline: None,
                runtime: Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
                task_spawner: Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
                range: None,
            };
            let predicate = ScanPredicate::new(
                "file_path",
                ScanPredicateDomain::Range {
                    op: MinMaxPredicateOp::Eq,
                    value: MinMaxPredicateValue::ByteArray(b"/data/007.parquet".to_vec()),
                },
                ScanPredicateSource::Static,
            )
            .with_physical_field_id(crate::delete_semantics::POSITION_FILE_PATH_FIELD_ID);
            let mut positions = Vec::new();
            let metrics = runtime
                .block_on(visit_parquet_batches_async_with_options(
                    &access,
                    &path.to_string_lossy(),
                    Some(size),
                    FileProjection::RootNames(vec!["file_path".into(), "pos".into()]),
                    vec![predicate],
                    context,
                    FileReaderOptions {
                        coalesce_reads: coalesce,
                        ..Default::default()
                    },
                    |batch| {
                        positions.extend(
                            batch
                                .batch
                                .column(1)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .values()
                                .iter()
                                .copied(),
                        );
                        Ok(())
                    },
                ))
                .unwrap();
            assert_eq!(positions, vec![7]);
            assert_eq!(
                (metrics.row_groups_read, metrics.row_groups_pruned),
                (1, 15)
            );
            assert!(
                receipts
                    .0
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|(_, offset, length)| *offset == 0 && *length == Some(size)),
                "small-file footer probe reads the full object"
            );
            assert!(metrics.bytes_read >= size);
            eprintln!(
                "POSITION_SMALL_PROBE coalesce={coalesce} file_bytes={size} physical_ranges={:?} metrics={metrics:?}",
                receipts.0.lock().unwrap()
            );
        }
    }
    #[test]
    fn delete_async_boundaries_preserve_physical_failure_kinds_and_content_corruption() {
        use novarocks_fs::{
            FileCancellation, FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner,
        };
        use novarocks_spi::connector::ConnectorErrorKind;
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("identified-delete-object.parquet");
        std::fs::write(&path, b"invalid-parquet-payload").unwrap();
        let size = std::fs::metadata(&path).unwrap().len();
        let access = FsAccessResolver::new()
            .resolve_location(
                novarocks_spi::connector::StorageAccessDomainId::from_bytes([1; 32]),
                path.to_string_lossy(),
                None,
            )
            .unwrap();
        let context = FileReadContext {
            cancellation: FileCancellation::new(),
            deadline: None,
            runtime: Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            task_spawner: Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
            range: None,
        };
        for (physical, expected) in [
            (opendal::ErrorKind::NotFound, ConnectorErrorKind::NotFound),
            (
                opendal::ErrorKind::PermissionDenied,
                ConnectorErrorKind::PermissionDenied,
            ),
            (
                opendal::ErrorKind::Unexpected,
                ConnectorErrorKind::Unavailable,
            ),
        ] {
            let receipts = RangeReceipts(Arc::default(), Some(physical));
            let injected = recorded_access(&access, &receipts);
            let raw = runtime
                .block_on(read_bytes_async_typed(
                    &injected,
                    &path.to_string_lossy(),
                    Some(size),
                    FileReadRange::bounded(0, size).unwrap(),
                    &context,
                ))
                .unwrap_err();
            assert_eq!(raw.kind(), expected);
            let parquet = runtime
                .block_on(visit_parquet_batches_async(
                    &injected,
                    &path.to_string_lossy(),
                    Some(size),
                    FileProjection::All,
                    Vec::new(),
                    context.clone(),
                    |_| Ok(()),
                ))
                .unwrap_err();
            assert_eq!(parquet.kind(), expected);
            assert!(
                receipts.0.lock().unwrap().len() >= 2,
                "both active physical boundaries reached the identified object"
            );
        }
        let malformed = runtime
            .block_on(visit_parquet_batches_async(
                &access,
                &path.to_string_lossy(),
                Some(size),
                FileProjection::All,
                Vec::new(),
                context.clone(),
                |_| Ok(()),
            ))
            .unwrap_err();
        assert_eq!(malformed.kind(), ConnectorErrorKind::CorruptData);
        let missing = directory.path().join("missing-delete.parquet");
        let unknown_size = runtime
            .block_on(visit_parquet_batches_async(
                &access,
                &missing.to_string_lossy(),
                None,
                FileProjection::All,
                Vec::new(),
                context,
                |_| Ok(()),
            ))
            .unwrap_err();
        assert_eq!(
            unknown_size.kind(),
            ConnectorErrorKind::NotFound,
            "HEAD errors keep their physical taxonomy"
        );
    }

    struct RejectRangeSpawner;
    impl novarocks_fs::FileTaskSpawner for RejectRangeSpawner {
        fn spawn(
            &self,
            _task: novarocks_fs::FileTaskFuture,
        ) -> novarocks_fs::FileResult<novarocks_fs::FileTask> {
            Err(novarocks_fs::FileError::new(
                novarocks_fs::FileErrorKind::ResourceExhausted,
                "identified range admission is exhausted",
            ))
        }
        fn spawn_detached_blocking(&self, job: Box<dyn FnOnce() + Send + 'static>) {
            job();
        }
    }
    #[test]
    fn delete_async_range_admission_retains_resource_exhausted_and_physical_exit() {
        use novarocks_fs::{FileCancellation, FsAccessResolver, TokioFileIoRuntime};
        use std::num::NonZeroUsize;
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("required-delete-exhausted.parquet");
        std::fs::write(&path, b"immutable-delete-content").unwrap();
        let size = std::fs::metadata(&path).unwrap().len();
        let access = FsAccessResolver::new()
            .resolve_location(
                novarocks_spi::connector::StorageAccessDomainId::from_bytes([1; 32]),
                path.to_string_lossy(),
                None,
            )
            .unwrap();
        let spawner = Arc::new(RejectRangeSpawner);
        let service = novarocks_fs::FileRangeService::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(4).unwrap(),
            spawner.clone(),
            runtime.handle().clone(),
        );
        let operations = novarocks_spi::connector::read_stack::ConnectorSourceOperations::new();
        let range = service.bind(
            novarocks_fs::FileRangeScope::try_new(1, 2, 1, 3, 4, 5).unwrap(),
            operations.clone(),
        );
        let context = FileReadContext {
            cancellation: FileCancellation::new(),
            deadline: None,
            runtime: Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            task_spawner: spawner,
            range: Some(range),
        };
        let raw = runtime
            .block_on(read_bytes_async_typed(
                &access,
                &path.to_string_lossy(),
                Some(size),
                FileReadRange::bounded(0, size).unwrap(),
                &context,
            ))
            .unwrap_err();
        assert_eq!(
            raw.kind(),
            novarocks_spi::connector::ConnectorErrorKind::ResourceExhausted
        );
        let parquet = runtime
            .block_on(visit_parquet_batches_async(
                &access,
                &path.to_string_lossy(),
                Some(size),
                FileProjection::All,
                Vec::new(),
                context,
                |_| Ok(()),
            ))
            .unwrap_err();
        assert_eq!(
            parquet.kind(),
            novarocks_spi::connector::ConnectorErrorKind::ResourceExhausted
        );
        operations.seal();
        runtime.block_on(operations.exited()).unwrap();
        assert_eq!(operations.live_operations(), 0);
    }
}
