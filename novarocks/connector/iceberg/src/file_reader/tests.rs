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

use std::sync::Arc;

use arrow::array::{Array, Int32Array, Int64Array, MapArray, StringArray, StructArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Fields};
use arrow::record_batch::RecordBatch;

use super::*;

// These are physical SDK fixtures: the row values and field IDs are the
// oracle, independently of the optional Arrow schema hint.
#[test]
fn iceberg_readers_ignore_bad_hints_and_keep_only_physical_ids() {
    use novarocks_fs::{
        FileCancellation, FsAccessResolver, TokioFileIoRuntime, TokioFileIoSpawner,
    };
    use parquet::arrow::arrow_writer::ArrowWriterOptions;
    use parquet::arrow::{ARROW_SCHEMA_META_KEY, ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use parquet::file::metadata::KeyValue;

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let directory = tempfile::tempdir().unwrap();
    for identified in [false, true] {
        let mut field = Field::new("value", DataType::Int64, true);
        if identified {
            field = field.with_metadata([(PARQUET_FIELD_ID_META_KEY.to_owned(), "17".to_owned())]);
        }
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![field]));
        for hint in ["absent", "bad", "conflicting-id"] {
            let path = directory
                .path()
                .join(format!("id-{identified}-hint-{hint}.parquet"));
            let mut writer = ArrowWriter::try_new_with_options(
                std::fs::File::create(&path).unwrap(),
                schema.clone(),
                ArrowWriterOptions::new().with_skip_arrow_metadata(true),
            )
            .unwrap();
            writer
                .write(
                    &RecordBatch::try_new(
                        schema.clone(),
                        vec![Arc::new(Int64Array::from(vec![Some(7), None, Some(-3)]))],
                    )
                    .unwrap(),
                )
                .unwrap();
            if hint != "absent" {
                let encoded = if hint == "bad" {
                    "not-an-arrow-schema".to_owned()
                } else {
                    // A valid hint claims a different ID for the same data.
                    let field = Field::new("value", DataType::Int64, true)
                        .with_metadata([(PARQUET_FIELD_ID_META_KEY.to_owned(), "99".to_owned())]);
                    parquet::arrow::encode_arrow_schema(&arrow::datatypes::Schema::new(vec![field]))
                };
                writer.append_key_value_metadata(KeyValue::new(
                    ARROW_SCHEMA_META_KEY.to_owned(),
                    encoded,
                ));
            }
            writer.close().unwrap();
            let location = path.to_string_lossy();
            let size = std::fs::metadata(&path).unwrap().len();
            let access = FsAccessResolver::new()
                .resolve_location(
                    novarocks_spi::connector::StorageAccessDomainId::from_bytes([17; 32]),
                    location.as_ref(),
                    None,
                )
                .unwrap();
            let context = FileReadContext {
                turn: None,
                cancellation: FileCancellation::new(),
                deadline: None,
                runtime: Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
                io_spawner: Arc::new(TokioFileIoSpawner::new(runtime.handle().clone())),
                range: None,
            };
            let projection = if identified {
                FileProjection::FieldIds(vec![17])
            } else {
                FileProjection::All
            };
            let synchronous = read_parquet_batches(
                &access,
                &location,
                Some(size),
                projection.clone(),
                context.clone(),
            )
            .unwrap();
            let asynchronous = runtime
                .block_on(read_parquet_batches_async(
                    &access,
                    &location,
                    Some(size),
                    projection,
                    context,
                ))
                .unwrap();
            for batches in [&synchronous, &asynchronous] {
                assert_eq!(batches.len(), 1);
                assert_eq!(
                    batches[0]
                        .batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap(),
                    &Int64Array::from(vec![Some(7), None, Some(-3)])
                );
                assert_eq!(
                    batches[0]
                        .batch
                        .schema()
                        .field(0)
                        .metadata()
                        .get(PARQUET_FIELD_ID_META_KEY),
                    identified.then(|| "17".to_owned()).as_ref()
                );
            }
        }
    }
}

pub(crate) type RangeReceipt = (String, u64, Option<u64>);
pub(crate) type RangeReceiptLog = Arc<std::sync::Mutex<Vec<RangeReceipt>>>;

#[derive(Clone, Debug, Default)]
pub(crate) struct RangeReceipts(pub(crate) RangeReceiptLog, Option<opendal::ErrorKind>);
#[derive(Debug)]
pub(crate) struct RangeReceiptAccessor {
    inner: opendal::raw::Servicer,
    receipts: RangeReceipts,
}
impl opendal::raw::Layer for RangeReceipts {
    fn apply_service(&self, inner: opendal::raw::Servicer) -> opendal::raw::Servicer {
        Arc::new(RangeReceiptAccessor {
            inner,
            receipts: self.clone(),
        })
    }
}
impl opendal::raw::Service for RangeReceiptAccessor {
    type Reader = RangeReceiptReader;
    type Writer = opendal::raw::oio::Writer;
    type Lister = opendal::raw::oio::Lister;
    type Deleter = opendal::raw::oio::Deleter;
    type Copier = opendal::raw::oio::Copier;
    fn info(&self) -> opendal::raw::ServiceInfo {
        opendal::raw::Service::info(&self.inner)
    }
    fn capability(&self) -> opendal::Capability {
        opendal::raw::Service::capability(&self.inner)
    }
    async fn create_dir(
        &self,
        ctx: &opendal::OperationContext,
        path: &str,
        args: opendal::raw::OpCreateDir,
    ) -> opendal::Result<opendal::raw::RpCreateDir> {
        opendal::raw::Service::create_dir(&self.inner, ctx, path, args).await
    }
    async fn stat(
        &self,
        ctx: &opendal::OperationContext,
        path: &str,
        args: opendal::raw::OpStat,
    ) -> opendal::Result<opendal::raw::RpStat> {
        opendal::raw::Service::stat(&self.inner, ctx, path, args).await
    }
    fn read(
        &self,
        ctx: &opendal::OperationContext,
        path: &str,
        args: opendal::raw::OpRead,
    ) -> opendal::Result<Self::Reader> {
        Ok(RangeReceiptReader {
            inner: opendal::raw::Service::read(&self.inner, ctx, path, args)?,
            path: path.to_owned(),
            receipts: self.receipts.clone(),
        })
    }
    fn write(
        &self,
        ctx: &opendal::OperationContext,
        path: &str,
        args: opendal::raw::OpWrite,
    ) -> opendal::Result<Self::Writer> {
        opendal::raw::Service::write(&self.inner, ctx, path, args)
    }
    fn delete(&self, ctx: &opendal::OperationContext) -> opendal::Result<Self::Deleter> {
        opendal::raw::Service::delete(&self.inner, ctx)
    }
    fn list(
        &self,
        ctx: &opendal::OperationContext,
        path: &str,
        args: opendal::raw::OpList,
    ) -> opendal::Result<Self::Lister> {
        opendal::raw::Service::list(&self.inner, ctx, path, args)
    }
    fn copy(
        &self,
        ctx: &opendal::OperationContext,
        from: &str,
        to: &str,
        args: opendal::raw::OpCopy,
        opts: opendal::raw::OpCopier,
    ) -> opendal::Result<Self::Copier> {
        opendal::raw::Service::copy(&self.inner, ctx, from, to, args, opts)
    }
    async fn rename(
        &self,
        ctx: &opendal::OperationContext,
        from: &str,
        to: &str,
        args: opendal::raw::OpRename,
    ) -> opendal::Result<opendal::raw::RpRename> {
        opendal::raw::Service::rename(&self.inner, ctx, from, to, args).await
    }
    async fn presign(
        &self,
        ctx: &opendal::OperationContext,
        path: &str,
        args: opendal::raw::OpPresign,
    ) -> opendal::Result<opendal::raw::RpPresign> {
        opendal::raw::Service::presign(&self.inner, ctx, path, args).await
    }
}
pub(crate) struct RangeReceiptReader {
    inner: opendal::raw::oio::Reader,
    path: String,
    receipts: RangeReceipts,
}
impl RangeReceiptReader {
    fn record(&self, range: opendal::BytesRange) -> opendal::Result<()> {
        self.receipts
            .0
            .lock()
            .unwrap()
            .push((self.path.clone(), range.offset(), range.size()));
        if let Some(kind) = self.receipts.1 {
            return Err(opendal::Error::new(
                kind,
                "identified delete-object read failure",
            ));
        }
        Ok(())
    }
}
impl opendal::raw::oio::Read for RangeReceiptReader {
    async fn open(
        &self,
        range: opendal::BytesRange,
    ) -> opendal::Result<(
        opendal::raw::RpRead,
        Box<dyn opendal::raw::oio::ReadStreamDyn>,
    )> {
        self.record(range)?;
        opendal::raw::oio::Read::open(&self.inner, range).await
    }
    async fn read(
        &self,
        range: opendal::BytesRange,
    ) -> opendal::Result<(opendal::raw::RpRead, opendal::Buffer)> {
        self.record(range)?;
        opendal::raw::oio::Read::read(&self.inner, range).await
    }
}
pub(crate) fn recorded_access(access: &FsAccessHandle, receipts: &RangeReceipts) -> FsAccessHandle {
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
fn retags_iceberg_map_with_required_key_and_nullable_value() {
    let keys = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;
    let values = Arc::new(Int64Array::from(vec![Some(10), None])) as ArrayRef;
    let entries = StructArray::new(
        Fields::from(vec![
            Field::new("key", DataType::Int32, false),
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

    let out = retag_iceberg_array(&source, &target).expect("retag valid map metadata");

    let DataType::Map(entries, false) = out.data_type() else {
        panic!("expected map");
    };
    assert_eq!(entries.name(), "iceberg_entries");
    let DataType::Struct(fields) = entries.data_type() else {
        panic!("expected entries struct");
    };
    assert_eq!(fields[0].name(), "iceberg_key");
    assert!(!fields[0].is_nullable());
    assert!(fields[1].is_nullable());
    let map = out.as_any().downcast_ref::<MapArray>().expect("map array");
    assert_eq!(map.keys().null_count(), 0);
    assert_eq!(
        map.keys()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[1, 2]
    );
    assert!(map.values().is_null(1));
}
#[test]
fn arrow_rejects_nullable_map_keys_before_iceberg_metadata_retagging() {
    let entries = StructArray::new(
        vec![
            Field::new("key", DataType::Int32, true),
            Field::new("value", DataType::Int64, true),
        ]
        .into(),
        vec![
            Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef,
            Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef,
        ],
        None,
    );
    let error = MapArray::try_new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::from_lengths([2]),
        entries,
        None,
        false,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        arrow::error::ArrowError::InvalidArgumentError(_)
    ));
    assert!(error.to_string().contains("keys field cannot be nullable"));
}
#[test]
fn position_path_predicate_reads_related_groups_and_reports_layout_degradation() {
    use arrow::array::BinaryArray;
    use novarocks_fs::{
        FileCancellation, FileReaderOptions, FsAccessResolver, TokioFileIoRuntime,
        TokioFileIoSpawner,
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
            .collect::<std::collections::HashMap<String, String>>(),
        ),
        Field::new("pos", DataType::Int64, false).with_metadata(
            [(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                (i32::MAX - 102).to_string(),
            )]
            .into_iter()
            .collect::<std::collections::HashMap<String, String>>(),
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
                turn: None,
                cancellation: FileCancellation::new(),
                deadline: None,
                runtime: Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
                io_spawner: Arc::new(TokioFileIoSpawner::new(runtime.handle().clone())),
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
                    (
                        default_parquet_reader_sizing(),
                        FileReaderOptions {
                            coalesce_reads: coalesce,
                            ..Default::default()
                        },
                    ),
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
        TokioFileIoSpawner,
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
            .collect::<std::collections::HashMap<String, String>>(),
        ),
        Field::new("pos", DataType::Int64, false).with_metadata(
            [(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                (i32::MAX - 102).to_string(),
            )]
            .into_iter()
            .collect::<std::collections::HashMap<String, String>>(),
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
            turn: None,
            cancellation: FileCancellation::new(),
            deadline: None,
            runtime: Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            io_spawner: Arc::new(TokioFileIoSpawner::new(runtime.handle().clone())),
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
                (
                    default_parquet_reader_sizing(),
                    FileReaderOptions {
                        coalesce_reads: coalesce,
                        ..Default::default()
                    },
                ),
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
        FileCancellation, FsAccessResolver, TokioFileIoRuntime, TokioFileIoSpawner,
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
        turn: None,
        cancellation: FileCancellation::new(),
        deadline: None,
        runtime: Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
        io_spawner: Arc::new(TokioFileIoSpawner::new(runtime.handle().clone())),
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
                default_parquet_reader_sizing(),
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
            default_parquet_reader_sizing(),
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
            default_parquet_reader_sizing(),
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
impl novarocks_fs::FileIoSpawner for RejectRangeSpawner {
    fn spawn(
        &self,
        _task: novarocks_fs::FileIoFuture,
    ) -> novarocks_fs::FileResult<novarocks_fs::FileIoJob> {
        Err(novarocks_fs::FileError::new(
            novarocks_fs::FileErrorKind::ResourceExhausted,
            "identified range admission is exhausted",
        ))
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
    let operations = novarocks_spi::connector::read_stack::ConnectorSourceOperations::for_test();
    let range = service.bind(
        novarocks_fs::FileRangeScope::for_test(1, 2, 1, 3, 4, 5).unwrap(),
        operations.clone(),
    );
    let context = FileReadContext {
        turn: None,
        cancellation: FileCancellation::new(),
        deadline: None,
        runtime: Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
        io_spawner: spawner,
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
            default_parquet_reader_sizing(),
            |_| Ok(()),
        ))
        .unwrap_err();
    assert_eq!(
        parquet.kind(),
        novarocks_spi::connector::ConnectorErrorKind::ResourceExhausted
    );
    operations.seal();
    assert_eq!(
        runtime.block_on(operations.exited()),
        novarocks_spi::connector::read_stack::ConnectorSourceExitProof::Exited
    );
    assert_eq!(operations.live_operations(), 0);
}
