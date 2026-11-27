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

use std::collections::BTreeMap;
use std::fs;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use novarocks_fs::{
    FileCancellation, FileIoRuntime, FileIoSpawner, FsAccessResolver, TokioFileIoRuntime,
    TokioFileIoSpawner,
};
use novarocks_spi::connector::ConnectorErrorKind;
use novarocks_spi::connector::read_stack::{
    CompleteAllDynamicFilter, SchemaTableName, SplitWeight, TupleDomain,
};
use parquet::arrow::ArrowWriter;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::iceberg::spec::{NestedField, PartitionSpec, PrimitiveType, Schema, Type};
use crate::position_delete::{FILE_PATH_COLUMN, POS_COLUMN};
use crate::typed_read::change_window::{
    IcebergAddedRows, IcebergChangeWindowHandleParams, IcebergDeletedDataFileRows,
    IcebergVisibilityDifferenceRows,
};
use crate::typed_read::split::{
    IcebergDeleteFile, IcebergDeleteFileContent, IcebergDeleteFileParams, IcebergFileFormat,
    IcebergSplit, IcebergSplitParams,
};
use crate::typed_read::test_streams::DrivenStream;

use super::*;

/// The data file's sequence number. Every delete below outranks it, which
/// is what makes it applicable at all.
const DATA_SEQUENCE_NUMBER: i64 = 3;

fn iceberg_schema() -> Schema {
    Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                2,
                "region",
                Type::Primitive(PrimitiveType::String),
            )),
        ])
        .build()
        .expect("frozen table schema")
}

fn identified(name: &str, data_type: DataType, field_id: i32, nullable: bool) -> Field {
    Field::new(name, data_type, nullable).with_metadata(
        [(PARQUET_FIELD_ID_META_KEY.to_owned(), field_id.to_string())]
            .into_iter()
            .collect::<std::collections::HashMap<String, String>>(),
    )
}

fn arrow_file_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        identified("id", DataType::Int64, 1, false),
        identified("region", DataType::Utf8, 2, true),
    ]))
}

/// One Parquet data file whose rows are `ids`, at positions `0..ids.len()`.
fn write_data_file(path: &Path, ids: &[i64], region: impl FnMut(i64) -> String) -> u64 {
    let schema = arrow_file_schema();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(
                ids.iter().copied().map(region).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("build data batch");
    write_parquet(path, schema, batch);
    fs::metadata(path).expect("stat data file").len()
}

fn write_position_delete(path: &Path, data_file: &str, positions: &[i64]) {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new(FILE_PATH_COLUMN, DataType::Utf8, false),
        Field::new(POS_COLUMN, DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![data_file; positions.len()])),
            Arc::new(Int64Array::from(positions.to_vec())),
        ],
    )
    .expect("build position-delete batch");
    write_parquet(path, schema, batch);
}

fn write_equality_delete(path: &Path, ids: &[i64]) {
    let schema = Arc::new(ArrowSchema::new(vec![identified(
        "id",
        DataType::Int64,
        1,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(ids.to_vec()))],
    )
    .expect("build equality-delete batch");
    write_parquet(path, schema, batch);
}

fn write_parquet(path: &Path, schema: Arc<ArrowSchema>, batch: RecordBatch) {
    let file = fs::File::create(path).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("parquet writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close parquet writer");
}

fn file_size_of(path: &Path) -> i64 {
    i64::try_from(fs::metadata(path).expect("stat file").len()).expect("size fits in i64")
}

fn position_delete_descriptor(path: &Path, data_sequence_number: i64) -> IcebergDeleteFile {
    delete_descriptor(
        IcebergDeleteFileContent::PositionDeletes,
        path,
        data_sequence_number,
        Vec::new(),
    )
}

fn equality_delete_descriptor(path: &Path, data_sequence_number: i64) -> IcebergDeleteFile {
    delete_descriptor(
        IcebergDeleteFileContent::EqualityDeletes,
        path,
        data_sequence_number,
        vec![1],
    )
}

fn delete_descriptor(
    content: IcebergDeleteFileContent,
    path: &Path,
    data_sequence_number: i64,
    equality_field_ids: Vec<i32>,
) -> IcebergDeleteFile {
    IcebergDeleteFile::try_new(IcebergDeleteFileParams {
        partition_spec_id: 0,
        partition_data_json: r#"{"version":1,"values":[]}"#.to_string(),
        content,
        path: path.to_string_lossy().to_string(),
        format: IcebergFileFormat::Parquet,
        record_count: parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            fs::File::open(path).unwrap(),
        )
        .unwrap()
        .metadata()
        .file_metadata()
        .num_rows(),
        file_size_in_bytes: file_size_of(path),
        equality_field_ids,
        row_position_lower_bound: None,
        row_position_upper_bound: None,
        data_sequence_number,
        content_offset: None,
        content_size_in_bytes: None,
        referenced_data_file: None,
        decryption_data: None,
    })
    .expect("valid delete descriptor")
}

fn change_window_handle(schema: &Schema) -> IcebergChangeWindowHandle {
    let spec = PartitionSpec::builder(schema.clone())
        .with_spec_id(0)
        .build()
        .expect("unpartitioned spec");
    IcebergChangeWindowHandle::try_new(IcebergChangeWindowHandleParams {
        schema_table_name: SchemaTableName::try_new("sales", "orders").expect("name"),
        table_schema_json: serde_json::to_string(schema).expect("schema json"),
        columns: vec![
            IcebergColumnHandle::base_column_of(schema, 1).expect("id"),
            IcebergColumnHandle::base_column_of(schema, 2).expect("region"),
        ],
        name_mapping_json: None,
        from_snapshot_id_exclusive: 11,
        to_snapshot_id_inclusive: 12,
        from_read_domain: crate::delete_semantics::test_read_domain(
            schema,
            std::slice::from_ref(&spec),
            11,
        ),
        to_read_domain: crate::delete_semantics::test_read_domain(
            schema,
            std::slice::from_ref(&spec),
            12,
        ),
        partition_spec_jsons: BTreeMap::from([(
            0,
            serde_json::to_string(&spec).expect("spec json"),
        )]),
    })
    .expect("change window handle")
}

struct Fixture {
    _runtime: tokio::runtime::Runtime,
    directory: tempfile::TempDir,
    binding: IcebergReadBinding,
    context: FileReadContext,
    footers: Arc<ParquetFooterCache>,
    delete_manager: Arc<DeleteManager>,
    data_path: PathBuf,
    data_file_size: u64,
    record_count: i64,
    first_row_id: Option<i64>,
}

impl Fixture {
    /// A fixture whose one data file holds `ids` at positions `0..n`.
    fn new(ids: &[i64]) -> Self {
        Self::with_regions(ids, |id| format!("r{id}"))
    }

    fn with_regions(ids: &[i64], region: impl FnMut(i64) -> String) -> Self {
        let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
        let file_runtime: Arc<dyn FileIoRuntime> =
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone()));
        let io_spawner: Arc<dyn FileIoSpawner> =
            Arc::new(TokioFileIoSpawner::new(runtime.handle().clone()));
        let binding = IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::clone(&file_runtime),
            novarocks_fs::TokioRefreshExecutor::shared(runtime.handle().clone()),
            Arc::clone(&io_spawner),
        );
        let context = FileReadContext {
            turn: None,
            cancellation: FileCancellation::new(),
            deadline: Some(Instant::now() + Duration::from_secs(60)),
            runtime: file_runtime,
            io_spawner,
            range: None,
        };
        let directory = tempfile::tempdir().expect("temporary directory");
        let data_path = directory.path().join("data.parquet");
        let data_file_size = write_data_file(&data_path, ids, region);
        Self {
            _runtime: runtime,
            directory,
            delete_manager: Arc::new(DeleteManager::new(binding.clone(), context.clone())),
            binding,
            context,
            footers: Arc::new(ParquetFooterCache::new()),
            data_path,
            data_file_size,
            record_count: ids.len() as i64,
            first_row_id: None,
        }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.directory.path().join(name)
    }

    fn data_file(&self) -> String {
        self.data_path.to_string_lossy().to_string()
    }

    fn data_split(&self, deletes: Vec<IcebergDeleteFile>) -> IcebergSplit {
        self.data_split_in_format(deletes, IcebergFileFormat::Parquet)
    }

    fn data_split_in_format(
        &self,
        deletes: Vec<IcebergDeleteFile>,
        file_format: IcebergFileFormat,
    ) -> IcebergSplit {
        self.data_split_at(deletes, file_format, 12)
    }

    fn data_split_at(
        &self,
        deletes: Vec<IcebergDeleteFile>,
        file_format: IcebergFileFormat,
        snapshot: i64,
    ) -> IcebergSplit {
        let schema = iceberg_schema();
        IcebergSplit::try_new(IcebergSplitParams {
            read_domain: crate::delete_semantics::test_read_domain(
                &schema,
                &[PartitionSpec::unpartition_spec()],
                snapshot,
            ),
            path: self.data_file(),
            start: 0,
            length: self.data_file_size as i64,
            file_size: self.data_file_size as i64,
            file_record_count: self.record_count,
            file_format,
            partition_spec_id: 0,
            partition_data_json: r#"{"version":1,"values":[]}"#.to_owned(),
            deletes: deletes.into(),
            file_statistics_domain: TupleDomain::all(),
            data_sequence_number: Some(DATA_SEQUENCE_NUMBER),
            file_first_row_id: self.first_row_id,
            decryption_data: None,
            split_weight: SplitWeight::STANDARD,
            affinity_key: None,
        })
        .expect("valid data split")
    }

    /// The split's stream, driven on the fixture runtime.
    fn page_source(
        &self,
        handle: &IcebergChangeWindowHandle,
        split: &IcebergChangeSplit,
        columns: &[IcebergColumnHandle],
    ) -> Result<DrivenStream<'_>, ConnectorError> {
        let budget = ConnectorPollBudget::new();
        let stream = self.page_stream(handle, split, columns, &budget)?;
        Ok(DrivenStream::new(&self._runtime, stream, budget))
    }

    fn page_stream(
        &self,
        handle: &IcebergChangeWindowHandle,
        split: &IcebergChangeSplit,
        columns: &[IcebergColumnHandle],
        budget: &ConnectorPollBudget,
    ) -> Result<OwnedConnectorPageStream, ConnectorError> {
        create_iceberg_change_window_page_stream(self.request(handle, split, columns), budget)
    }

    fn request<'a>(
        &self,
        handle: &'a IcebergChangeWindowHandle,
        split: &'a IcebergChangeSplit,
        columns: &'a [IcebergColumnHandle],
    ) -> IcebergChangeWindowPageSourceRequest<'a> {
        IcebergChangeWindowPageSourceRequest {
            output_layout: super::super::runtime::IcebergReadLayout::Offsets,
            handle,
            split,
            columns,
            delete_manager: Arc::clone(&self.delete_manager),
            footers: Arc::clone(&self.footers),
            access_binding: self.binding.clone(),
            context: self.context.clone(),
            cache: None,
            budget: ReaderSizing {
                max_read_rows: NonZeroUsize::new(1024).expect("nonzero"),
                max_read_bytes: 8 * 1024 * 1024,
            },
            parquet_policy: novarocks_fs::ParquetInterpretationPolicy::DerivedByteRepresentation {
                preserve_top_level_dictionary: false,
            },
            reader_options: FileReaderOptions::default(),
            scheduled_split_sequence_id: 0,
            dynamic_filter: Arc::new(CompleteAllDynamicFilter::new(
                std::collections::BTreeSet::new(),
            )) as Arc<IcebergDynamicFilter>,
        }
    }
}

/// The scan's output columns: the base relation's `id`, then the sign.
fn id_and_change_op(schema: &Schema) -> Vec<IcebergColumnHandle> {
    vec![
        IcebergColumnHandle::base_column_of(schema, 1).expect("id"),
        change_op_column_handle().expect("change op"),
    ]
}

/// Drain the source into `(id, __change_op)` pairs, proving the public sign
/// remains Int32 throughout the provider page stream.
fn drain(source: &mut DrivenStream<'_>) -> Vec<(i64, i32)> {
    let mut rows = Vec::new();
    for _ in 0..64 {
        let Some(page) = source.next_page().expect("page") else {
            break;
        };
        rows.extend(signed_rows(page));
    }
    assert!(source.is_finished(), "the split drains in bounded steps");
    rows
}

/// One page's `(id, __change_op)` pairs.
fn signed_rows(page: novarocks_spi::connector::read_stack::SourcePage) -> Vec<(i64, i32)> {
    let (count, columns) = page.into_columns().expect("materialize");
    assert_eq!(columns.len(), 2, "id and the derived sign");
    assert_eq!(
        columns[1].data_type(),
        &DataType::Int32,
        "the public change sign matches the signed Int32 schema"
    );
    let ids = columns[0]
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 ids");
    let signs = columns[1]
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("int32 signs");
    (0..count)
        .map(|row| (ids.value(row), signs.value(row)))
        .collect()
}

/// Reads the split as a stream and closes it.
fn read_streamed(
    fixture: &Fixture,
    handle: &IcebergChangeWindowHandle,
    split: &IcebergChangeSplit,
    schema: &Schema,
) -> Vec<(i64, i32)> {
    read_streamed_with(
        fixture,
        handle,
        split,
        &id_and_change_op(schema),
        signed_rows,
    )
}

fn read_streamed_with<T>(
    fixture: &Fixture,
    handle: &IcebergChangeWindowHandle,
    split: &IcebergChangeSplit,
    columns: &[IcebergColumnHandle],
    rows_from_page: impl Fn(novarocks_spi::connector::read_stack::SourcePage) -> Vec<T>,
) -> Vec<T> {
    use futures::StreamExt;

    let budget = ConnectorPollBudget::new();
    let mut stream = fixture
        .page_stream(handle, split, columns, &budget)
        .expect("page stream");
    fixture._runtime.block_on(async {
        let mut rows = Vec::new();
        loop {
            budget.refill(1024);
            let Some(page) = stream.next().await else {
                break;
            };
            rows.extend(rows_from_page(page.expect("page")));
        }
        stream.close().await.expect("close a drained stream");
        rows
    })
}

#[test]
fn added_rows_emit_every_row_that_survives_at_the_upper_endpoint_with_a_plus_one_sign() {
    let fixture = Fixture::new(&[10, 11, 12]);
    let schema = iceberg_schema();
    let handle = change_window_handle(&schema);
    let split = IcebergChangeSplit::AddedRows(
        IcebergAddedRows::try_new(fixture.data_split(Vec::new()), Vec::new()).expect("added rows"),
    );

    let mut source = fixture
        .page_source(&handle, &split, &id_and_change_op(&schema))
        .expect("page source");
    assert_eq!(drain(&mut source), vec![(10, 1), (11, 1), (12, 1)]);
}

#[test]
fn the_change_sign_can_shorten_the_complete_output_prefix() {
    // Two strings plus the three offsets use the full construction target.
    let region_bytes = (1024 * 1024 - 3 * size_of::<i32>()) / 2;
    let regions = ["x".repeat(region_bytes), "y".repeat(region_bytes)];
    let fixture = Fixture::with_regions(&[0, 1], |id| regions[id as usize].clone());
    let schema = iceberg_schema();
    let handle = change_window_handle(&schema);
    let split = IcebergChangeSplit::AddedRows(
        IcebergAddedRows::try_new(fixture.data_split(Vec::new()), Vec::new()).expect("added rows"),
    );
    let region = IcebergColumnHandle::base_column_of(&schema, 2).expect("region");

    let mut unsigned = fixture
        .page_source(&handle, &split, std::slice::from_ref(&region))
        .expect("unsigned page source");
    let page = unsigned.next_page().expect("page").expect("two rows");
    assert_eq!(page.position_count(), 2);
    assert!(unsigned.next_page().expect("end of stream").is_none());
    unsigned.close().expect("close unsigned source");

    let mut signed = fixture
        .page_source(
            &handle,
            &split,
            &[region, change_op_column_handle().expect("change op")],
        )
        .expect("signed page source");
    for expected in &regions {
        let page = signed.next_page().expect("page").expect("one row");
        let (rows, columns) = page.into_columns().expect("materialize page");
        assert_eq!(rows, 1);
        let regions = columns[0].as_any().downcast_ref::<StringArray>().unwrap();
        let signs = columns[1].as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(regions.value(0), expected);
        assert_eq!(signs.value(0), 1);
    }
    assert!(signed.next_page().expect("end of stream").is_none());
    signed.close().expect("close signed source");
}

#[test]
fn a_row_written_and_deleted_inside_the_window_produces_no_output_row() {
    // The file is new at the upper endpoint, so it travels with the upper
    // endpoint's own closure. Position 1 was written and deleted inside the
    // window: it is invisible at both endpoints and the difference does not
    // own it in either direction.
    let fixture = Fixture::new(&[10, 11, 12]);
    let schema = iceberg_schema();
    let handle = change_window_handle(&schema);
    let deletes = fixture.path("inside-window.parquet");
    write_position_delete(&deletes, &fixture.data_file(), &[1]);
    let split = IcebergChangeSplit::AddedRows(
        IcebergAddedRows::try_new(
            fixture.data_split(vec![position_delete_descriptor(
                &deletes,
                DATA_SEQUENCE_NUMBER + 1,
            )]),
            Vec::new(),
        )
        .expect("added rows"),
    );

    let mut source = fixture
        .page_source(&handle, &split, &id_and_change_op(&schema))
        .expect("page source");
    let rows = drain(&mut source);
    assert_eq!(rows, vec![(10, 1), (12, 1)]);
    assert!(
        rows.iter().all(|(id, _)| *id != 11),
        "a row written and deleted inside the window has no sign at all"
    );
}

fn difference(
    fixture: &Fixture,
    from: Vec<IcebergDeleteFile>,
    to: Vec<IcebergDeleteFile>,
) -> IcebergChangeSplit {
    IcebergChangeSplit::VisibilityDifference(
        IcebergVisibilityDifferenceRows::try_new(fixture.data_split(to.clone()), from, to).unwrap(),
    )
}
fn removed(fixture: &Fixture, from: Vec<IcebergDeleteFile>) -> IcebergChangeSplit {
    IcebergChangeSplit::DeletedDataFileRows(
        IcebergDeletedDataFileRows::try_new(fixture.data_split_at(
            from,
            IcebergFileFormat::Parquet,
            11,
        ))
        .unwrap(),
    )
}
#[test]
fn removed_files_exclude_the_complete_from_closure() {
    let fixture = Fixture::new(&[20, 21, 22]);
    let schema = iceberg_schema();
    let handle = change_window_handle(&schema);
    let old = fixture.path("old.parquet");
    write_position_delete(&old, &fixture.data_file(), &[0]);
    let split = removed(
        &fixture,
        vec![position_delete_descriptor(&old, DATA_SEQUENCE_NUMBER + 1)],
    );
    assert_eq!(
        read_streamed(&fixture, &handle, &split, &schema),
        vec![(21, -1), (22, -1)]
    );
    assert_eq!(
        read_streamed(&fixture, &handle, &removed(&fixture, vec![]), &schema),
        vec![(20, -1), (21, -1), (22, -1)]
    );
}

#[test]
fn removing_a_file_and_deleting_its_remaining_rows_retract_the_same_row_ids() {
    use crate::row_lineage_synth::{ICEBERG_RESERVED_FIELD_ID_ROW_ID, ICEBERG_ROW_ID_COL};

    let mut fixture = Fixture::new(&[10, 20, 20, 30]);
    fixture.first_row_id = Some(1_000);
    let schema = iceberg_schema();
    let handle = change_window_handle(&schema);
    let mut columns = id_and_change_op(&schema);
    columns.push(
        IcebergColumnHandle::base_column(&NestedField::optional(
            ICEBERG_RESERVED_FIELD_ID_ROW_ID,
            ICEBERG_ROW_ID_COL,
            Type::Primitive(PrimitiveType::Long),
        ))
        .expect("row id handle"),
    );
    let old_position = fixture.path("old-position.parquet");
    let old_equality = fixture.path("old-equality.parquet");
    let new_position = fixture.path("new-position.parquet");
    write_position_delete(&old_position, &fixture.data_file(), &[0]);
    write_equality_delete(&old_equality, &[10]);
    write_position_delete(&new_position, &fixture.data_file(), &[1, 2, 3]);

    let expected = [(20, -1, 1_001), (20, -1, 1_002), (30, -1, 1_003)];
    for from in [
        position_delete_descriptor(&old_position, 4),
        equality_delete_descriptor(&old_equality, 4),
    ] {
        let to = vec![from.clone(), position_delete_descriptor(&new_position, 5)];
        for split in [
            difference(&fixture, vec![from.clone()], to),
            removed(&fixture, vec![from]),
        ] {
            let rows = read_streamed_with(&fixture, &handle, &split, &columns, |page| {
                let (count, arrays) = page.into_columns().expect("materialize");
                let ids = arrays[0].as_any().downcast_ref::<Int64Array>().unwrap();
                let signs = arrays[1].as_any().downcast_ref::<Int32Array>().unwrap();
                let row_ids = arrays[2].as_any().downcast_ref::<Int64Array>().unwrap();
                assert_eq!(row_ids.null_count(), 0);
                (0..count)
                    .map(|row| (ids.value(row), signs.value(row), row_ids.value(row)))
                    .collect()
            });
            assert_eq!(rows, expected);
        }
    }
}
#[test]
fn replacement_closure_emits_only_the_endpoint_row_bag_difference() {
    let fixture = Fixture::new(&[30, 31, 32, 33]);
    let schema = iceberg_schema();
    let handle = change_window_handle(&schema);
    let old = fixture.path("old.parquet");
    let new = fixture.path("replacement.parquet");
    write_position_delete(&old, &fixture.data_file(), &[0]);
    write_position_delete(&new, &fixture.data_file(), &[0, 2]);
    let split = difference(
        &fixture,
        vec![position_delete_descriptor(&old, 4)],
        vec![position_delete_descriptor(&new, 5)],
    );
    assert_eq!(
        read_streamed(&fixture, &handle, &split, &schema),
        vec![(32, -1)]
    );
}
#[test]
fn overlapping_kinds_and_duplicate_row_values_preserve_signed_bag_multiplicity() {
    let fixture = Fixture::new(&[40, 41, 42, 42, 43]);
    let schema = iceberg_schema();
    let handle = change_window_handle(&schema);
    let pos = fixture.path("pos.parquet");
    let eq = fixture.path("eq.parquet");
    write_position_delete(&pos, &fixture.data_file(), &[1, 2]);
    write_equality_delete(&eq, &[41, 42]);
    let split = difference(
        &fixture,
        vec![],
        vec![
            position_delete_descriptor(&pos, 4),
            equality_delete_descriptor(&eq, 4),
        ],
    );
    assert_eq!(
        read_streamed(&fixture, &handle, &split, &schema),
        vec![(41, -1), (42, -1), (42, -1)]
    );
}
#[test]
fn from_equality_keys_remain_in_the_hidden_projection() {
    let fixture = Fixture::new(&[40, 41, 42]);
    let schema = iceberg_schema();
    let handle = change_window_handle(&schema);
    let old = fixture.path("old-eq.parquet");
    let new = fixture.path("new-pos.parquet");
    write_equality_delete(&old, &[41]);
    write_position_delete(&new, &fixture.data_file(), &[1, 2]);
    let split = difference(
        &fixture,
        vec![equality_delete_descriptor(&old, 4)],
        vec![position_delete_descriptor(&new, 5)],
    );
    assert_eq!(
        read_streamed(&fixture, &handle, &split, &schema),
        vec![(42, -1)]
    );
}
#[test]
fn repeated_output_field_ids_do_not_duplicate_the_equality_binding() {
    let fixture = Fixture::new(&[10, 11]);
    let schema = iceberg_schema();
    let handle = change_window_handle(&schema);
    let eq = fixture.path("eq.parquet");
    write_equality_delete(&eq, &[11]);
    let split = difference(&fixture, vec![], vec![equality_delete_descriptor(&eq, 4)]);
    let id = IcebergColumnHandle::base_column_of(&schema, 1).unwrap();
    let columns = vec![id.clone(), id, change_op_column_handle().unwrap()];
    let mut source = fixture.page_source(&handle, &split, &columns).unwrap();
    let mut rows = Vec::new();
    while let Some(page) = source.next_page().unwrap() {
        let (count, columns) = page.into_columns().unwrap();
        assert_eq!(columns.len(), 3);
        let a = columns[0].as_any().downcast_ref::<Int64Array>().unwrap();
        let b = columns[1].as_any().downcast_ref::<Int64Array>().unwrap();
        let sign = columns[2].as_any().downcast_ref::<Int32Array>().unwrap();
        rows.extend((0..count).map(|i| (a.value(i), b.value(i), sign.value(i))));
    }
    assert_eq!(rows, vec![(11, 11, -1)]);
}

#[test]
fn actual_row_reappearance_remains_explicitly_unsupported() {
    let fixture = Fixture::new(&[40, 41]);
    let schema = iceberg_schema();
    let handle = change_window_handle(&schema);
    let old = fixture.path("old.parquet");
    write_position_delete(&old, &fixture.data_file(), &[0]);
    let split = difference(&fixture, vec![position_delete_descriptor(&old, 4)], vec![]);
    let columns = id_and_change_op(&schema);
    let mut source = fixture.page_source(&handle, &split, &columns).unwrap();
    assert_eq!(
        source.next_page().unwrap_err().kind(),
        ConnectorErrorKind::Unsupported
    );
}

#[test]
fn the_change_sign_matches_the_public_int32_declaration() {
    // The page and the signed column handle agree at the provider boundary.
    // The SQL-owned internal narrowing is a separate expression.
    let fixture = Fixture::new(&[50]);
    let schema = iceberg_schema();
    let handle = change_window_handle(&schema);
    let column = change_op_column_handle().expect("change op");
    assert_eq!(
        column.type_json(),
        "\"int\"",
        "the declared iceberg type stays int"
    );

    let split = IcebergChangeSplit::AddedRows(
        IcebergAddedRows::try_new(fixture.data_split(Vec::new()), Vec::new()).expect("added rows"),
    );
    let mut source = fixture
        .page_source(&handle, &split, &[column])
        .expect("page source");
    let page = source
        .next_page()
        .expect("page")
        .expect("one page of one row");
    let (rows, columns) = page.into_columns().expect("materialize");
    assert_eq!(rows, 1);
    assert_eq!(columns.len(), 1, "the sign alone is a legal projection");
    assert_eq!(columns[0].data_type(), &DataType::Int32);
    let signs = columns[0]
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("int32 signs");
    assert_eq!(signs.value(0), 1);
}

#[test]
fn a_non_parquet_change_window_split_is_a_stable_unsupported_rather_than_a_wrong_read() {
    let fixture = Fixture::new(&[60]);
    let schema = iceberg_schema();
    let handle = change_window_handle(&schema);
    for format in [IcebergFileFormat::Orc, IcebergFileFormat::Avro] {
        let split = IcebergChangeSplit::AddedRows(
            IcebergAddedRows::try_new(fixture.data_split_in_format(Vec::new(), format), Vec::new())
                .expect("added rows"),
        );
        let error = fixture
            .page_source(&handle, &split, &id_and_change_op(&schema))
            .err()
            .expect("only parquet is implemented");
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
        assert!(
            error
                .to_string()
                .contains("not readable by this page source")
        );
    }
}

#[test]
fn an_added_rows_split_narrowed_to_row_ids_is_rejected_rather_than_guessed() {
    // The wire field names row *ids*, which are not row positions. Nothing
    // in this stack produces one, and reading it as either would be a guess
    // about which rows the split owns.
    let fixture = Fixture::new(&[70, 71]);
    let schema = iceberg_schema();
    let handle = change_window_handle(&schema);
    let split = IcebergChangeSplit::AddedRows(
        IcebergAddedRows::try_new(fixture.data_split(Vec::new()), vec![0, 1]).expect("added rows"),
    );

    let error = fixture
        .page_source(&handle, &split, &id_and_change_op(&schema))
        .err()
        .expect("a narrowed added-rows split is not implemented");
    assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
    assert!(error.to_string().contains("row ids"));
}

#[test]
fn the_sign_column_the_frontend_binds_is_the_one_this_reader_recognizes() {
    // The frontend appends `change_op_column_handle()` to a change
    // window's bindings, and the provider decodes each scan assignment from
    // its private wire value. A lossy round trip would make the sign look
    // like an impostor claiming its reserved field id.
    let column = change_op_column_handle().expect("change op");
    let decoded =
        IcebergColumnHandle::from_proto(&column.to_proto()).expect("decode the sign column");

    assert_eq!(decoded, column);
    let projection = ChangeOpProjection::of(&[decoded]).expect("the sign is recognized");
    assert_eq!(projection.slots, vec![OutputSlot::ChangeOp]);
    assert!(projection.base_columns.is_empty());
}

#[test]
fn a_scan_assignment_that_steals_the_sign_field_id_is_rejected() {
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(
            ICEBERG_CHANGE_OP_FIELD_ID,
            "borrowed",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()
        .expect("schema");
    let impostor =
        IcebergColumnHandle::base_column_of(&schema, ICEBERG_CHANGE_OP_FIELD_ID).expect("handle");

    let error = ChangeOpProjection::of(&[impostor])
        .err()
        .expect("the reserved field id belongs to the sign alone");
    assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
}

#[test]
fn an_equivalent_rewrite_of_delete_artifacts_emits_no_rows() {
    let fixture = Fixture::new(&[10, 11, 12]);
    let schema = iceberg_schema();
    let handle = change_window_handle(&schema);
    let before = fixture.path("before.parquet");
    let after = fixture.path("after.parquet");
    write_position_delete(&before, &fixture.data_file(), &[0, 1]);
    write_equality_delete(&after, &[10, 11]);
    let split = difference(
        &fixture,
        vec![position_delete_descriptor(&before, 4)],
        vec![equality_delete_descriptor(&after, 5)],
    );
    assert!(read_streamed(&fixture, &handle, &split, &schema).is_empty());
}
