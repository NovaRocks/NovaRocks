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

use std::fs;
use std::path::Path;
use std::sync::Arc as StdArc;
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use novarocks_fs::{
    FileCancellation, FileIoRuntime, FileIoSpawner, FsAccessResolver, TokioFileIoRuntime,
    TokioFileIoSpawner,
};
use novarocks_spi::connector::read_stack::{SplitWeight, TupleDomain};
use parquet::arrow::ArrowWriter;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::commit::DeletionVector;
use crate::iceberg::spec::{NestedField, PrimitiveType, Type};
use crate::position_delete::{FILE_PATH_COLUMN, POS_COLUMN};

use super::super::split::{IcebergDeleteFileParams, IcebergSplitParams};
use super::*;

const DATA_FILE: &str = "/warehouse/data/a.parquet";
const OTHER_DATA_FILE: &str = "/warehouse/data/b.parquet";

struct Fixture {
    // The Tokio runtime must outlive every handle the binding cloned out
    // of it, so the fixture owns it for the whole test.
    _runtime: tokio::runtime::Runtime,
    directory: tempfile::TempDir,
    manager: DeleteManager,
}

impl Fixture {
    fn new() -> Self {
        let runtime = tokio::runtime::Runtime::new().expect("build Tokio runtime");
        let file_runtime: StdArc<dyn FileIoRuntime> =
            StdArc::new(TokioFileIoRuntime::new(runtime.handle().clone()));
        let io_spawner: StdArc<dyn FileIoSpawner> =
            StdArc::new(TokioFileIoSpawner::new(runtime.handle().clone()));
        let binding = IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            file_runtime,
            novarocks_fs::TokioRefreshExecutor::shared(runtime.handle().clone()),
            io_spawner,
        );
        let context = binding
            .file_read_context(
                FileCancellation::new(),
                Instant::now() + Duration::from_secs(60),
            )
            .expect("build file read context");
        Self {
            _runtime: runtime,
            directory: tempfile::tempdir().expect("create temporary directory"),
            manager: DeleteManager::new(binding, context),
        }
    }

    fn path(&self, name: &str) -> std::path::PathBuf {
        self.directory.path().join(name)
    }

    fn loaded_artifacts(&self) -> usize {
        self.manager.loaded_artifacts().expect("loaded artifacts")
    }

    /// Opens a split's delete state the way its page stream does.
    fn open_split(
        &self,
        split: &IcebergSplit,
        table_schema: &Schema,
        mode: DeleteEvaluationMode,
    ) -> Result<SplitDeleteFilter, ConnectorError> {
        self._runtime.block_on(self.manager.open_split(
            split,
            table_schema,
            &DeleteDomainBindings::Single(split.read_domain().clone()),
            mode,
        ))
    }
}

fn table_schema() -> Schema {
    Schema::builder()
        .with_fields(vec![
            StdArc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            )),
            StdArc::new(NestedField::optional(
                2,
                "name",
                Type::Primitive(PrimitiveType::String),
            )),
            StdArc::new(NestedField::optional(
                3,
                "kind",
                Type::Primitive(PrimitiveType::String),
            )),
        ])
        .build()
        .expect("valid iceberg schema")
}

fn domain(snapshot: i64) -> Arc<ReadDomain> {
    let spec = PartitionSpec::builder(table_schema())
        .with_spec_id(0)
        .build()
        .unwrap();
    Arc::new(ReadDomain::new(
        ReadObservationId::try_new([7; 16]).unwrap(),
        PinnedEndpointFacts::try_new(
            uuid::Uuid::from_bytes([8; 16]),
            "metadata.json",
            snapshot,
            &table_schema(),
            &[spec],
        )
        .unwrap(),
    ))
}
fn tuple_json() -> String {
    let spec = PartitionSpec::builder(table_schema())
        .with_spec_id(0)
        .build()
        .unwrap();
    TypedPartition::bind(
        &spec,
        &table_schema(),
        &crate::iceberg::spec::Struct::empty(),
    )
    .unwrap()
    .to_json_string()
}
fn identified(name: &str, data_type: DataType, field_id: i32) -> Field {
    Field::new(name, data_type, true).with_metadata(
        [(PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string())]
            .into_iter()
            .collect::<std::collections::HashMap<String, String>>(),
    )
}

/// A data page of `id`/`name`/`kind`, tagged with Iceberg field IDs the way
/// the physical reader hands one to the filter.
fn data_batch(ids: &[i64], kinds: &[&str]) -> RecordBatch {
    let schema = StdArc::new(ArrowSchema::new(vec![
        identified("id", DataType::Int64, 1),
        identified("name", DataType::Utf8, 2),
        identified("kind", DataType::Utf8, 3),
    ]));
    let names = ids.iter().map(|id| format!("row-{id}")).collect::<Vec<_>>();
    RecordBatch::try_new(
        schema,
        vec![
            StdArc::new(Int64Array::from(ids.to_vec())),
            StdArc::new(StringArray::from_iter_values(&names)),
            StdArc::new(StringArray::from(kinds.to_vec())),
        ],
    )
    .expect("build data batch")
}

fn positions(values: &[u64]) -> UInt64Array {
    UInt64Array::from(values.to_vec())
}

fn keeps(mask: &BooleanArray) -> Vec<bool> {
    (0..mask.len()).map(|row| mask.value(row)).collect()
}

fn file_size(path: &Path) -> i64 {
    i64::try_from(fs::metadata(path).expect("delete file metadata").len())
        .expect("delete file size fits in i64")
}

fn write_position_delete_parquet(path: &Path, rows: &[(&str, i64)]) {
    let schema = StdArc::new(ArrowSchema::new(vec![
        Field::new(FILE_PATH_COLUMN, DataType::Utf8, false),
        Field::new(POS_COLUMN, DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        StdArc::clone(&schema),
        vec![
            StdArc::new(StringArray::from(
                rows.iter().map(|(path, _)| *path).collect::<Vec<_>>(),
            )),
            StdArc::new(Int64Array::from(
                rows.iter().map(|(_, pos)| *pos).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("build position-delete batch");
    write_parquet(path, schema, batch);
}

/// An equality-delete file over one long column, tagged with its field ID.
fn write_equality_delete_parquet(path: &Path, field_id: i32, name: &str, values: &[i64]) {
    let schema = StdArc::new(ArrowSchema::new(vec![identified(
        name,
        DataType::Int64,
        field_id,
    )]));
    let batch = RecordBatch::try_new(
        StdArc::clone(&schema),
        vec![StdArc::new(Int64Array::from(values.to_vec()))],
    )
    .expect("build equality-delete batch");
    write_parquet(path, schema, batch);
}

/// An equality-delete file over `(kind, id)`, deliberately in the order the
/// writer chose rather than the table's.
fn write_kind_id_equality_delete_parquet(path: &Path, rows: &[(&str, i64)]) {
    let schema = StdArc::new(ArrowSchema::new(vec![
        identified("kind", DataType::Utf8, 3),
        identified("id", DataType::Int64, 1),
    ]));
    let batch = RecordBatch::try_new(
        StdArc::clone(&schema),
        vec![
            StdArc::new(StringArray::from(
                rows.iter().map(|(kind, _)| *kind).collect::<Vec<_>>(),
            )),
            StdArc::new(Int64Array::from(
                rows.iter().map(|(_, id)| *id).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("build equality-delete batch");
    write_parquet(path, schema, batch);
}

fn write_parquet(path: &Path, schema: StdArc<ArrowSchema>, batch: RecordBatch) {
    let file = fs::File::create(path).expect("create delete file");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("create parquet writer");
    writer.write(&batch).expect("write delete batch");
    writer.close().expect("close parquet writer");
}

/// Write a deletion-vector blob behind a byte prefix, the way a Puffin
/// container places one, and return its content range.
fn write_deletion_vector(path: &Path, deleted: &[u64], prefix: usize) -> (i64, i64) {
    let mut vector = DeletionVector::new();
    for position in deleted {
        vector.insert(*position).expect("insert deleted position");
    }
    let payload = vector
        .to_iceberg_payload()
        .expect("encode deletion vector payload");
    let mut bytes = vec![0_u8; prefix];
    bytes.extend_from_slice(&payload);
    fs::write(path, &bytes).expect("write deletion vector file");
    (
        i64::try_from(prefix).expect("prefix fits in i64"),
        i64::try_from(payload.len()).expect("payload length fits in i64"),
    )
}

struct DeleteBuilder {
    content: IcebergDeleteFileContent,
    path: String,
    format: IcebergFileFormat,
    file_size_in_bytes: i64,
    equality_field_ids: Vec<i32>,
    row_position_lower_bound: Option<i64>,
    row_position_upper_bound: Option<i64>,
    data_sequence_number: i64,
    content_offset: Option<i64>,
    content_size_in_bytes: Option<i64>,
}

impl DeleteBuilder {
    fn position(path: &Path, data_sequence_number: i64) -> Self {
        Self {
            content: IcebergDeleteFileContent::PositionDeletes,
            path: path.to_string_lossy().to_string(),
            format: IcebergFileFormat::Parquet,
            file_size_in_bytes: file_size(path),
            equality_field_ids: Vec::new(),
            row_position_lower_bound: None,
            row_position_upper_bound: None,
            data_sequence_number,
            content_offset: None,
            content_size_in_bytes: None,
        }
    }

    fn deletion_vector(path: &Path, data_sequence_number: i64, range: (i64, i64)) -> Self {
        Self {
            content: IcebergDeleteFileContent::PositionDeletes,
            path: path.to_string_lossy().to_string(),
            format: IcebergFileFormat::Puffin,
            file_size_in_bytes: file_size(path),
            equality_field_ids: Vec::new(),
            row_position_lower_bound: None,
            row_position_upper_bound: None,
            data_sequence_number,
            content_offset: Some(range.0),
            content_size_in_bytes: Some(range.1),
        }
    }

    fn equality(path: &Path, data_sequence_number: i64, equality_field_ids: Vec<i32>) -> Self {
        Self {
            content: IcebergDeleteFileContent::EqualityDeletes,
            path: path.to_string_lossy().to_string(),
            format: IcebergFileFormat::Parquet,
            file_size_in_bytes: file_size(path),
            equality_field_ids,
            row_position_lower_bound: None,
            row_position_upper_bound: None,
            data_sequence_number,
            content_offset: None,
            content_size_in_bytes: None,
        }
    }

    fn with_row_position_bounds(mut self, lower: i64, upper: i64) -> Self {
        self.row_position_lower_bound = Some(lower);
        self.row_position_upper_bound = Some(upper);
        self
    }

    fn with_file_size(mut self, file_size_in_bytes: i64) -> Self {
        self.file_size_in_bytes = file_size_in_bytes;
        self
    }

    fn build(self) -> IcebergDeleteFile {
        IcebergDeleteFile::try_new(IcebergDeleteFileParams {
            partition_spec_id: 0,
            partition_data_json: tuple_json(),
            content: self.content,
            path: self.path,
            format: self.format,
            record_count: 1,
            file_size_in_bytes: self.file_size_in_bytes,
            equality_field_ids: self.equality_field_ids,
            row_position_lower_bound: self.row_position_lower_bound,
            row_position_upper_bound: self.row_position_upper_bound,
            data_sequence_number: self.data_sequence_number,
            content_offset: self.content_offset,
            content_size_in_bytes: self.content_size_in_bytes,
            referenced_data_file: (self.format == IcebergFileFormat::Puffin)
                .then(|| DATA_FILE.to_string()),
            decryption_data: None,
        })
        .expect("valid delete descriptor")
    }
}

fn split_of(
    path: &str,
    data_sequence_number: Option<i64>,
    deletes: Vec<IcebergDeleteFile>,
) -> IcebergSplit {
    split_range(path, 0, 1024, data_sequence_number, deletes)
}

fn split_range(
    path: &str,
    start: i64,
    length: i64,
    data_sequence_number: Option<i64>,
    deletes: Vec<IcebergDeleteFile>,
) -> IcebergSplit {
    IcebergSplit::try_new(IcebergSplitParams {
        read_domain: domain(1),
        path: path.to_string(),
        start,
        length,
        file_size: 1024,
        file_record_count: 3,
        file_format: IcebergFileFormat::Parquet,
        partition_spec_id: 0,
        partition_data_json: tuple_json(),
        deletes: deletes.into(),
        file_statistics_domain: TupleDomain::all(),
        data_sequence_number,
        file_first_row_id: None,
        decryption_data: None,
        split_weight: SplitWeight::STANDARD,
        affinity_key: None,
    })
    .expect("valid iceberg split")
}

#[test]
fn position_delete_at_equal_sequence_applies() {
    let fixture = Fixture::new();
    let stale = fixture.path("stale.parquet");
    let fresh = fixture.path("fresh.parquet");
    write_position_delete_parquet(&stale, &[(DATA_FILE, 0)]);
    write_position_delete_parquet(&fresh, &[(DATA_FILE, 2)]);

    let split = split_of(
        DATA_FILE,
        Some(5),
        vec![
            // Position deletes include equal data sequence.
            DeleteBuilder::position(&stale, 5).build(),
            DeleteBuilder::position(&fresh, 6).build(),
        ],
    );
    let filter = fixture
        .open_split(
            &split,
            &table_schema(),
            DeleteEvaluationMode::ExcludeDeleted,
        )
        .expect("open split");

    let mask = filter
        .evaluate(
            &data_batch(&[1, 2, 3], &["a", "a", "a"]),
            &positions(&[0, 1, 2]),
        )
        .expect("evaluate");
    assert_eq!(keeps(&mask), vec![false, true, false]);
    assert_eq!(fixture.loaded_artifacts(), 2);
}

#[test]
fn an_equality_delete_without_a_data_sequence_number_is_corrupt_data() {
    let fixture = Fixture::new();
    let deletes = fixture.path("equality.parquet");
    write_equality_delete_parquet(&deletes, 1, "id", &[2]);

    let split = split_of(
        DATA_FILE,
        None,
        vec![DeleteBuilder::equality(&deletes, 7, vec![1]).build()],
    );
    let error = fixture
        .open_split(
            &split,
            &table_schema(),
            DeleteEvaluationMode::ExcludeDeleted,
        )
        .expect_err("missing data sequence number must fail");

    assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    assert_eq!(fixture.loaded_artifacts(), 0);
}

#[test]
fn one_delete_path_loads_once_for_two_splits_of_the_same_file() {
    let fixture = Fixture::new();
    let deletes = fixture.path("deletes.parquet");
    write_position_delete_parquet(&deletes, &[(DATA_FILE, 1)]);

    let schema = table_schema();
    let first = split_range(
        DATA_FILE,
        0,
        512,
        Some(1),
        vec![DeleteBuilder::position(&deletes, 9).build()],
    );
    let second = split_range(
        DATA_FILE,
        512,
        512,
        Some(1),
        vec![DeleteBuilder::position(&deletes, 9).build()],
    );

    let first = fixture
        .open_split(&first, &schema, DeleteEvaluationMode::ExcludeDeleted)
        .expect("open first split");
    assert_eq!(fixture.loaded_artifacts(), 1);
    let second = fixture
        .open_split(&second, &schema, DeleteEvaluationMode::ExcludeDeleted)
        .expect("open second split");
    assert_eq!(fixture.loaded_artifacts(), 1);

    let batch = data_batch(&[1, 2, 3], &["a", "a", "a"]);
    for filter in [&first, &second] {
        let mask = filter
            .evaluate(&batch, &positions(&[0, 1, 2]))
            .expect("evaluate");
        assert_eq!(keeps(&mask), vec![true, false, true]);
    }
}

#[test]
fn position_deletes_match_absolute_positions_of_their_own_data_file() {
    let fixture = Fixture::new();
    let deletes = fixture.path("deletes.parquet");
    write_position_delete_parquet(
        &deletes,
        &[(DATA_FILE, 2), (OTHER_DATA_FILE, 1), (DATA_FILE, 5)],
    );

    let schema = table_schema();
    let batch = data_batch(&[1, 2, 3], &["a", "a", "a"]);

    let mine = split_of(
        DATA_FILE,
        Some(1),
        vec![DeleteBuilder::position(&deletes, 9).build()],
    );
    let mask = fixture
        .open_split(&mine, &schema, DeleteEvaluationMode::ExcludeDeleted)
        .expect("open split")
        .evaluate(&batch, &positions(&[0, 1, 2]))
        .expect("evaluate");
    assert_eq!(keeps(&mask), vec![true, true, false]);

    // Position 5 belongs to the same data file but to another page.
    let mask = fixture
        .open_split(&mine, &schema, DeleteEvaluationMode::ExcludeDeleted)
        .expect("reopen split")
        .evaluate(&batch, &positions(&[4, 5, 6]))
        .expect("evaluate");
    assert_eq!(keeps(&mask), vec![true, false, true]);

    // The other file's row 1 must not follow the delete file across.
    let theirs = split_of(
        OTHER_DATA_FILE,
        Some(1),
        vec![DeleteBuilder::position(&deletes, 9).build()],
    );
    let mask = fixture
        .open_split(&theirs, &schema, DeleteEvaluationMode::ExcludeDeleted)
        .expect("open other split")
        .evaluate(&batch, &positions(&[0, 1, 2]))
        .expect("evaluate");
    assert_eq!(keeps(&mask), vec![true, false, true]);
}

#[test]
fn backend_does_not_replan_using_position_manifest_bounds() {
    let fixture = Fixture::new();
    let deletes = fixture.path("deletes.parquet");
    // Consistent with its bounds: every entry names another data file, so
    // loading it would delete nothing here either.
    write_position_delete_parquet(&deletes, &[(OTHER_DATA_FILE, 50), (OTHER_DATA_FILE, 60)]);

    let split = split_of(
        DATA_FILE,
        Some(1),
        vec![
            DeleteBuilder::position(&deletes, 9)
                .with_row_position_bounds(50, 60)
                .build(),
        ],
    );
    let filter = fixture
        .open_split(
            &split,
            &table_schema(),
            DeleteEvaluationMode::ExcludeDeleted,
        )
        .expect("open split");

    // A three-row data file cannot hold position 50, so the artifact is
    // never opened -- and pruning removed work, not rows.
    assert_eq!(fixture.loaded_artifacts(), 1);
    assert!(filter.is_empty());
    let mask = filter
        .evaluate(
            &data_batch(&[1, 2, 3], &["a", "a", "a"]),
            &positions(&[0, 1, 2]),
        )
        .expect("evaluate");
    assert_eq!(keeps(&mask), vec![true, true, true]);
}

#[test]
fn one_deletion_vector_applies_and_a_second_one_fails_closed() {
    let fixture = Fixture::new();
    let first = fixture.path("first.puffin");
    let second = fixture.path("second.puffin");
    let first_range = write_deletion_vector(&first, &[1], 24);
    let second_range = write_deletion_vector(&second, &[2], 8);

    let schema = table_schema();
    let batch = data_batch(&[1, 2, 3], &["a", "a", "a"]);

    let single = split_of(
        DATA_FILE,
        Some(1),
        vec![DeleteBuilder::deletion_vector(&first, 9, first_range).build()],
    );
    let first_filter = fixture
        .open_split(&single, &schema, DeleteEvaluationMode::ExcludeDeleted)
        .expect("open split");
    let second_filter = fixture
        .open_split(&single, &schema, DeleteEvaluationMode::ExcludeDeleted)
        .expect("reopen split");
    let (FilterVerdict::Exclude(first_side), FilterVerdict::Exclude(second_side)) =
        (&first_filter.verdict, &second_filter.verdict)
    else {
        panic!("ordinary filters");
    };
    assert!(
        Arc::ptr_eq(&first_side.positions, &second_side.positions),
        "normalized single DV shares the cached bitmap without payload cloning"
    );
    let mask = first_filter
        .evaluate(&batch, &positions(&[0, 1, 2]))
        .expect("evaluate");
    assert_eq!(keeps(&mask), vec![true, false, true]);

    let two = split_of(
        DATA_FILE,
        Some(1),
        vec![
            DeleteBuilder::deletion_vector(&first, 9, first_range).build(),
            DeleteBuilder::deletion_vector(&second, 10, second_range).build(),
        ],
    );
    let error = fixture
        .open_split(&two, &schema, DeleteEvaluationMode::ExcludeDeleted)
        .expect_err("two deletion vectors must fail");
    assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
}

#[test]
fn a_deletion_vector_beside_a_position_delete_file_fails_closed() {
    let fixture = Fixture::new();
    let vector_path = fixture.path("dv.puffin");
    let range = write_deletion_vector(&vector_path, &[1], 16);
    let positions_path = fixture.path("positions.parquet");
    write_position_delete_parquet(&positions_path, &[(DATA_FILE, 2)]);

    let split = split_of(
        DATA_FILE,
        Some(1),
        vec![
            DeleteBuilder::deletion_vector(&vector_path, 9, range).build(),
            DeleteBuilder::position(&positions_path, 9).build(),
        ],
    );
    let error = fixture
        .open_split(
            &split,
            &table_schema(),
            DeleteEvaluationMode::ExcludeDeleted,
        )
        .expect_err("ambiguous position-delete state must fail");
    assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
}

#[test]
fn an_equality_field_id_outside_the_schema_is_corrupt_data() {
    let fixture = Fixture::new();
    let deletes = fixture.path("equality.parquet");
    write_equality_delete_parquet(&deletes, 99, "dropped", &[2]);

    let split = split_of(
        DATA_FILE,
        Some(1),
        vec![DeleteBuilder::equality(&deletes, 9, vec![99]).build()],
    );
    let error = fixture
        .open_split(
            &split,
            &table_schema(),
            DeleteEvaluationMode::ExcludeDeleted,
        )
        .expect_err("unknown equality field id must fail");

    assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    assert_eq!(fixture.loaded_artifacts(), 0);
}

#[test]
fn equality_keys_and_hidden_columns_follow_the_table_schema_order() {
    let fixture = Fixture::new();
    let deletes = fixture.path("equality.parquet");
    write_kind_id_equality_delete_parquet(&deletes, &[("beta", 2)]);

    // The manifest recorded the IDs as (kind, id); the table says (id, kind).
    let split = split_of(
        DATA_FILE,
        Some(1),
        vec![DeleteBuilder::equality(&deletes, 9, vec![3, 1]).build()],
    );
    let schema = table_schema();
    let preview = DeleteManager::preview_hidden_columns(
        &split,
        &schema,
        &DeleteDomainBindings::Single(split.read_domain().clone()),
        &DeleteEvaluationMode::ExcludeDeleted,
    )
    .expect("preview equality columns");
    assert_eq!(
        preview
            .iter()
            .map(IcebergColumnHandle::base_field_id)
            .collect::<Vec<_>>(),
        vec![1, 3]
    );
    assert_eq!(fixture.loaded_artifacts(), 0);
    let filter = fixture
        .open_split(&split, &schema, DeleteEvaluationMode::ExcludeDeleted)
        .expect("open split");

    assert_eq!(
        filter
            .required_hidden_columns()
            .iter()
            .map(IcebergColumnHandle::base_field_id)
            .collect::<Vec<_>>(),
        vec![1, 3]
    );
    let d = fixture.manager.diagnostics().unwrap();
    assert_eq!(d.buckets, 1);
    assert_eq!(d.application_merges, 1);

    let mask = filter
        .evaluate(
            &data_batch(&[1, 2, 3], &["beta", "beta", "beta"]),
            &positions(&[0, 1, 2]),
        )
        .expect("evaluate");
    assert_eq!(keeps(&mask), vec![true, false, true]);
}

#[test]
fn a_split_without_equality_deletes_needs_no_hidden_columns() {
    let fixture = Fixture::new();
    let deletes = fixture.path("deletes.parquet");
    write_position_delete_parquet(&deletes, &[(DATA_FILE, 1)]);

    let schema = table_schema();
    let with_positions = fixture
        .open_split(
            &split_of(
                DATA_FILE,
                Some(1),
                vec![DeleteBuilder::position(&deletes, 9).build()],
            ),
            &schema,
            DeleteEvaluationMode::ExcludeDeleted,
        )
        .expect("open split");
    assert!(with_positions.required_hidden_columns().is_empty());
    assert!(!with_positions.is_empty());

    let without_deletes = fixture
        .open_split(
            &split_of(DATA_FILE, Some(1), Vec::new()),
            &schema,
            DeleteEvaluationMode::ExcludeDeleted,
        )
        .expect("open split");
    assert!(without_deletes.required_hidden_columns().is_empty());
    assert!(without_deletes.is_empty());
}

#[test]
fn an_oversized_delete_set_is_rejected_by_the_cost_bound() {
    let fixture = Fixture::new();
    let deletes = fixture.path("deletes.parquet");
    write_position_delete_parquet(&deletes, &[(DATA_FILE, 1)]);

    const THREE_HUNDRED_MIB: i64 = 300 * 1024 * 1024;
    let split = split_of(
        DATA_FILE,
        Some(1),
        vec![
            DeleteBuilder::position(&deletes, 9)
                .with_file_size(THREE_HUNDRED_MIB)
                .build(),
            DeleteBuilder::position(&deletes, 10)
                .with_file_size(THREE_HUNDRED_MIB)
                .build(),
        ],
    );
    let error = fixture
        .open_split(
            &split,
            &table_schema(),
            DeleteEvaluationMode::ExcludeDeleted,
        )
        .expect_err("oversized delete set must fail");

    assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
    assert_eq!(fixture.loaded_artifacts(), 0);
}

#[test]
fn a_page_whose_positions_do_not_match_its_rows_is_corrupt_data() {
    let fixture = Fixture::new();
    let deletes = fixture.path("deletes.parquet");
    write_position_delete_parquet(&deletes, &[(DATA_FILE, 1)]);

    let filter = fixture
        .open_split(
            &split_of(
                DATA_FILE,
                Some(1),
                vec![DeleteBuilder::position(&deletes, 9).build()],
            ),
            &table_schema(),
            DeleteEvaluationMode::ExcludeDeleted,
        )
        .expect("open split");

    let error = filter
        .evaluate(
            &data_batch(&[1, 2, 3], &["a", "a", "a"]),
            &positions(&[0, 1]),
        )
        .expect_err("short position array must fail");
    assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
}

// --------------------------------------------------- change-window reverse
#[test]
fn forged_equal_sequence_equality_and_domain_mismatch_reject_without_io() {
    let fixture = Fixture::new();
    let path = fixture.path("equality.parquet");
    write_equality_delete_parquet(&path, 1, "id", &[2]);
    let split = split_of(
        DATA_FILE,
        Some(5),
        vec![DeleteBuilder::equality(&path, 5, vec![1]).build()],
    );
    assert!(
        fixture
            .open_split(
                &split,
                &table_schema(),
                DeleteEvaluationMode::ExcludeDeleted
            )
            .is_err()
    );
    assert_eq!(fixture.loaded_artifacts(), 0);
    let split = split_of(
        DATA_FILE,
        Some(1),
        vec![DeleteBuilder::equality(&path, 5, vec![1]).build()],
    );
    assert!(
        fixture
            ._runtime
            .block_on(fixture.manager.open_split(
                &split,
                &table_schema(),
                &DeleteDomainBindings::Single(domain(2)),
                DeleteEvaluationMode::ExcludeDeleted
            ))
            .is_err()
    );
    assert_eq!(fixture.loaded_artifacts(), 0);
}

#[test]
fn checkpoint_suffixes_share_one_union_and_probe_once_per_group() {
    let fixture = Fixture::new();
    let mut deletes = Vec::new();
    for sequence in 2..=21 {
        let path = fixture.path(&format!("checkpoint-{sequence}.parquet"));
        write_equality_delete_parquet(&path, 1, "id", &[2]);
        deletes.push(DeleteBuilder::equality(&path, sequence, vec![1]).build());
    }
    let batch = data_batch(&[1, 2, 3], &["a", "a", "a"]);
    let absolute = positions(&[0, 1, 2]);
    for sequence in (1..=20).rev() {
        let suffix = deletes
            .iter()
            .filter(|delete| delete.data_sequence_number() > sequence)
            .cloned()
            .collect();
        let split = split_of(DATA_FILE, Some(sequence), suffix);
        let filter = fixture
            .open_split(
                &split,
                &table_schema(),
                DeleteEvaluationMode::ExcludeDeleted,
            )
            .unwrap();
        assert_eq!(
            keeps(&filter.evaluate(&batch, &absolute).unwrap()),
            vec![true, false, true]
        );
    }
    let d = fixture.manager.diagnostics().unwrap();
    assert_eq!(
        (
            d.buckets,
            d.keys,
            d.physical_loads,
            d.application_merges,
            d.completed_applications
        ),
        (1, 1, 20, 20, 20)
    );
    assert_eq!(d.probes, 20 * 3);
}

#[test]
fn exact_applications_share_decode_but_never_ready_by_address_or_sequence() {
    let fixture = Fixture::new();
    let path = fixture.path("shared.parquet");
    write_equality_delete_parquet(&path, 1, "id", &[2]);
    let old = DeleteBuilder::equality(&path, 4, vec![1]).build();
    let newer = DeleteBuilder::equality(&path, 12, vec![1]).build();
    let first = split_of(DATA_FILE, Some(1), vec![old]);
    fixture
        .open_split(
            &first,
            &table_schema(),
            DeleteEvaluationMode::ExcludeDeleted,
        )
        .unwrap();
    let second = split_of(DATA_FILE, Some(7), vec![newer]);
    let filter = fixture
        .open_split(
            &second,
            &table_schema(),
            DeleteEvaluationMode::ExcludeDeleted,
        )
        .unwrap();
    assert_eq!(
        keeps(
            &filter
                .evaluate(
                    &data_batch(&[1, 2, 3], &["a", "a", "a"]),
                    &positions(&[0, 1, 2])
                )
                .unwrap()
        ),
        vec![true, false, true]
    );
    let d = fixture.manager.diagnostics().unwrap();
    assert_eq!(
        (
            d.physical_loads,
            d.application_merges,
            d.completed_applications
        ),
        (1, 2, 2)
    );
}

#[test]
fn endpoint_unions_are_isolated_and_actual_reappearance_is_rejected() {
    let fixture = Fixture::new();
    let path = fixture.path("delete.parquet");
    write_equality_delete_parquet(&path, 1, "id", &[2]);
    let delete = DeleteBuilder::equality(&path, 5, vec![1]).build();
    let split = split_of(DATA_FILE, Some(1), Vec::new());
    let bindings = DeleteDomainBindings::Window {
        from: domain(1),
        to: domain(2),
    };
    let closure = |snapshot, deletes| EndpointDeleteClosure {
        read_domain: domain(snapshot),
        deletes,
    };
    let removed = fixture
        ._runtime
        .block_on(fixture.manager.open_split(
            &split,
            &table_schema(),
            &bindings,
            DeleteEvaluationMode::VisibleDifference {
                from: closure(1, Vec::new()),
                to: closure(2, vec![delete.clone()]),
            },
        ))
        .unwrap();
    let batch = data_batch(&[1, 2, 3], &["a", "a", "a"]);
    let absolute = positions(&[0, 1, 2]);
    assert_eq!(
        keeps(&removed.evaluate(&batch, &absolute).unwrap()),
        vec![false, true, false]
    );
    let reappeared = fixture
        ._runtime
        .block_on(fixture.manager.open_split(
            &split,
            &table_schema(),
            &bindings,
            DeleteEvaluationMode::VisibleDifference {
                from: closure(1, vec![delete]),
                to: closure(2, Vec::new()),
            },
        ))
        .unwrap();
    assert_eq!(
        reappeared.evaluate(&batch, &absolute).unwrap_err().kind(),
        ConnectorErrorKind::Unsupported
    );
    let d = fixture.manager.diagnostics().unwrap();
    assert_eq!(
        (d.buckets, d.physical_loads, d.application_merges),
        (2, 1, 2)
    );
    assert_eq!(d.domains.len(), 2);
    assert_eq!(
        d.domains.iter().map(|d| d.buckets).sum::<usize>(),
        d.buckets
    );
    assert_eq!(d.domains.iter().map(|d| d.keys).sum::<usize>(), d.keys);
    assert_eq!(
        d.domains
            .iter()
            .map(|d| d.completed_applications)
            .sum::<usize>(),
        d.completed_applications
    );
    assert_eq!(
        d.domains
            .iter()
            .map(|d| d.retained_key_bytes)
            .sum::<usize>(),
        d.retained_key_bytes
    );
    assert!(d.retained_decode_bytes > 0 && d.retained_key_bytes > 0);
}

#[test]
fn split_prebinds_alias_projection_once_for_global_partition_and_both_endpoints() {
    use crate::iceberg::spec::{Literal, Struct, Transform};
    let fixture = Fixture::new();
    let global_path = fixture.path("prebound-global.parquet");
    let partition_path = fixture.path("prebound-partition.parquet");
    write_equality_delete_parquet(&global_path, 1, "writer_old_id", &[2]);
    write_equality_delete_parquet(&partition_path, 1, "writer_new_id", &[3]);
    let schema = table_schema();
    let global_spec = PartitionSpec::builder(schema.clone())
        .with_spec_id(0)
        .build()
        .unwrap();
    let partition_spec = PartitionSpec::builder(schema.clone())
        .with_spec_id(1)
        .add_partition_field("kind", "kind", Transform::Identity)
        .unwrap()
        .build()
        .unwrap();
    let partition = TypedPartition::bind(
        &partition_spec,
        &schema,
        &Struct::from_iter([Some(Literal::string("a"))]),
    )
    .unwrap()
    .to_json_string();
    let pinned = |snapshot| {
        Arc::new(ReadDomain::new(
            ReadObservationId::try_new([11; 16]).unwrap(),
            PinnedEndpointFacts::try_new(
                uuid::Uuid::from_bytes([12; 16]),
                "prebound-metadata.json",
                snapshot,
                &schema,
                &[global_spec.clone(), partition_spec.clone()],
            )
            .unwrap(),
        ))
    };
    let from = pinned(1);
    let to = pinned(2);
    let global = DeleteBuilder::equality(&global_path, 9, vec![1]).build();
    let mut local_dto = DeleteBuilder::equality(&partition_path, 9, vec![1])
        .build()
        .to_proto();
    local_dto.partition_spec_id = Some(1);
    local_dto.partition_data_json = partition.clone();
    let local = IcebergDeleteFile::from_proto(&local_dto).unwrap();
    let mut dto = split_of(DATA_FILE, Some(1), vec![global.clone(), local.clone()])
        .to_proto()
        .unwrap();
    dto.read_domain = Some(super::super::split::encode_read_domain(&to));
    dto.partition_spec_id = 1;
    dto.partition_data_json = partition;
    let split = IcebergSplit::from_proto(&dto, SplitWeight::STANDARD, None).unwrap();
    let original = data_batch(&[1, 2, 3], &["a", "a", "a"]);
    // Hidden projection changes names and order; binding uses IDs, never names.
    let projected_schema = Arc::new(ArrowSchema::new(vec![
        identified("alias_kind", DataType::Utf8, 3),
        identified("hidden_id", DataType::Int64, 1),
        identified("alias_name", DataType::Utf8, 2),
    ]));
    let projected = RecordBatch::try_new(
        projected_schema.clone(),
        vec![
            original.column(2).clone(),
            original.column(0).clone(),
            original.column(1).clone(),
        ],
    )
    .unwrap();
    let ordinary = fixture
        ._runtime
        .block_on(fixture.manager.open_split(
            &split,
            &schema,
            &DeleteDomainBindings::Single(to.clone()),
            DeleteEvaluationMode::ExcludeDeleted,
        ))
        .unwrap();
    assert_eq!(
        keeps(
            &ordinary
                .evaluate(&projected, &positions(&[0, 1, 2]))
                .unwrap()
        ),
        vec![true, false, false]
    );
    let d = fixture.manager.diagnostics().unwrap();
    assert_eq!(
        (d.buckets, d.probes, d.row_key_batches, d.row_key_rows),
        (2, 6, 1, 3)
    );
    let mut window_dto = dto.clone();
    window_dto.read_domain = Some(super::super::split::encode_read_domain(&from));
    window_dto.deletes = vec![global.to_proto()];
    let window_split =
        IcebergSplit::from_proto(&window_dto, SplitWeight::STANDARD, None).unwrap();
    let difference = fixture
        ._runtime
        .block_on(fixture.manager.open_split(
            &window_split,
            &schema,
            &DeleteDomainBindings::Window {
                from: from.clone(),
                to: to.clone(),
            },
            DeleteEvaluationMode::VisibleDifference {
                from: EndpointDeleteClosure {
                    read_domain: from,
                    deletes: vec![global.clone()],
                },
                to: EndpointDeleteClosure {
                    read_domain: to,
                    deletes: vec![global, local],
                },
            },
        ))
        .unwrap();
    assert_eq!(
        keeps(
            &difference
                .evaluate(&projected, &positions(&[0, 1, 2]))
                .unwrap()
        ),
        vec![false, false, true]
    );
    let d = fixture.manager.diagnostics().unwrap();
    assert_eq!((d.probes, d.row_key_batches, d.row_key_rows), (15, 2, 6));
    // An equal independently allocated schema is valid; a changed schema is not.
    let equal_schema = RecordBatch::try_new(
        Arc::new(projected_schema.as_ref().clone()),
        projected.columns().to_vec(),
    )
    .unwrap();
    ordinary
        .evaluate(&equal_schema, &positions(&[0, 1, 2]))
        .unwrap();
    let before = fixture.manager.diagnostics().unwrap();
    let changed = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            identified("alias_kind", DataType::Utf8, 3),
            identified("changed_hidden_id", DataType::Int64, 1),
            identified("alias_name", DataType::Utf8, 2),
        ])),
        projected.columns().to_vec(),
    )
    .unwrap();
    for filter in [&ordinary, &difference] {
        assert_eq!(
            filter
                .evaluate(&changed, &positions(&[0, 1, 2]))
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData
        );
    }
    let after = fixture.manager.diagnostics().unwrap();
    assert_eq!(
        (before.probes, before.row_key_batches, before.row_key_rows),
        (after.probes, after.row_key_batches, after.row_key_rows)
    );
}

#[test]
fn manager_and_endpoint_cost_matrix_retains_and_releases_exact_owners() {
    let fixture = Fixture::new();
    for scale in [1024, 8192] {
        let path = fixture.path(&format!("checkpoint-{scale}.parquet"));
        let values = (0..scale).collect::<Vec<i64>>();
        write_equality_delete_parquet(&path, 1, "historical_id", &values);
        let delete = DeleteBuilder::equality(&path, 9, vec![1]).build();
        for endpoints in [1, 2] {
            let mut unit_retained = None;
            for count in [1, 2, 4] {
                let mut managers = Vec::new();
                let mut filters = Vec::new();
                let mut manager_owners = Vec::new();
                let mut union_owners = Vec::new();
                let mut total = DeleteManagerDiagnostics::default();
                for manager_index in 0..count {
                    let manager = Arc::new(DeleteManager::new(
                        fixture.manager.binding.clone(),
                        fixture.manager.context.clone(),
                    ));
                    let pinned = |snapshot| {
                        Arc::new(ReadDomain::new(
                            ReadObservationId::try_new([manager_index as u8 + 9; 16]).unwrap(),
                            domain(snapshot).endpoint().clone(),
                        ))
                    };
                    let from = pinned(1);
                    let to = pinned(2);
                    let mut dto = split_of(DATA_FILE, Some(1), vec![delete.clone()])
                        .to_proto()
                        .unwrap();
                    dto.read_domain = Some(super::super::split::encode_read_domain(&from));
                    let split =
                        IcebergSplit::from_proto(&dto, SplitWeight::STANDARD, None).unwrap();
                    let (bindings, mode) = if endpoints == 1 {
                        (
                            DeleteDomainBindings::Single(from.clone()),
                            DeleteEvaluationMode::ExcludeDeleted,
                        )
                    } else {
                        (
                            DeleteDomainBindings::Window {
                                from: from.clone(),
                                to: to.clone(),
                            },
                            DeleteEvaluationMode::VisibleDifference {
                                from: EndpointDeleteClosure {
                                    read_domain: from,
                                    deletes: vec![delete.clone()],
                                },
                                to: EndpointDeleteClosure {
                                    read_domain: to,
                                    deletes: vec![delete.clone()],
                                },
                            },
                        )
                    };
                    let filter = fixture
                        ._runtime
                        .block_on(manager.open_split(&split, &table_schema(), &bindings, mode))
                        .unwrap();
                    let mask = filter
                        .evaluate(
                            &data_batch(&[2, 9000, 17000], &["a", "a", "a"]),
                            &positions(&[0, 1, 2]),
                        )
                        .unwrap();
                    assert_eq!(
                        keeps(&mask),
                        if endpoints == 1 {
                            vec![false, true, true]
                        } else {
                            vec![false, false, false]
                        }
                    );
                    let d = manager.diagnostics().unwrap();
                    assert_eq!(
                        (d.physical_loads, d.physical_load_attempts, d.decoded_rows),
                        (1, 1, scale as usize)
                    );
                    assert_eq!(
                        (d.buckets, d.keys, d.applications, d.completed_applications),
                        (endpoints, endpoints * scale as usize, endpoints, endpoints)
                    );
                    assert_eq!(d.probes, endpoints * 3);
                    assert_eq!((d.row_key_batches, d.row_key_rows), (1, 3));
                    assert_eq!(d.domains.len(), endpoints);
                    assert_eq!(
                        d.domains.iter().map(|domain| domain.keys).sum::<usize>(),
                        d.keys
                    );
                    total.physical_loads += d.physical_loads;
                    total.decoded_rows += d.decoded_rows;
                    total.buckets += d.buckets;
                    total.keys += d.keys;
                    total.completed_applications += d.completed_applications;
                    total.retained_key_bytes += d.retained_key_bytes;
                    total.retained_decode_bytes += d.retained_decode_bytes;
                    manager_owners.push(Arc::downgrade(&manager));
                    union_owners.extend(
                        manager
                            .state
                            .lock()
                            .unwrap()
                            .unions
                            .values()
                            .map(Arc::downgrade),
                    );
                    managers.push(manager);
                    filters.push(filter);
                }
                let retained = total.retained_key_bytes + total.retained_decode_bytes;
                let unit = *unit_retained.get_or_insert(retained);
                assert_eq!(
                    retained,
                    unit * count,
                    "each task/scan manager retains its own index"
                );
                assert_eq!(total.physical_loads, count);
                assert_eq!(total.keys, count * endpoints * scale as usize);
                drop(filters);
                drop(managers);
                assert!(manager_owners.iter().all(|owner| owner.upgrade().is_none()));
                assert!(union_owners.iter().all(|owner| owner.upgrade().is_none()));
                eprintln!(
                    "DELETE_COST_MATRIX scale={scale} managers={count} endpoints={endpoints} keys={} ready={} loads={} decoded={} retained_key_capacity_bytes={} retained_decode_capacity_bytes={} manager_owners_released=true union_owners_released=true",
                    total.keys,
                    total.completed_applications,
                    total.physical_loads,
                    total.decoded_rows,
                    total.retained_key_bytes,
                    total.retained_decode_bytes
                );
            }
        }
    }
}

#[test]
fn missing_physical_equality_artifact_keeps_not_found_through_application_barrier() {
    let fixture = Fixture::new();
    let path = fixture.path("required-but-missing-equality.parquet");
    write_equality_delete_parquet(&path, 1, "id", &[2]);
    let delete = DeleteBuilder::equality(&path, 9, vec![1]).build();
    fs::remove_file(&path).unwrap();
    let split = split_of(DATA_FILE, Some(1), vec![delete]);
    let error = fixture
        .open_split(
            &split,
            &table_schema(),
            DeleteEvaluationMode::ExcludeDeleted,
        )
        .unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::NotFound);
    let d = fixture.manager.diagnostics().unwrap();
    assert_eq!(
        (
            d.physical_load_attempts,
            d.physical_loads,
            d.completed_applications
        ),
        (1, 0, 0)
    );
}

#[test]
fn artifact_type_failure_publishes_no_partial_union_or_ready_application() {
    let fixture = Fixture::new();
    let path = fixture.path("invalid-typed-key.parquet");
    let schema = StdArc::new(ArrowSchema::new(vec![identified(
        "renamed_id",
        DataType::Utf8,
        1,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![StdArc::new(StringArray::from(vec![None::<&str>]))],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(fs::File::create(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let split = split_of(
        DATA_FILE,
        Some(1),
        vec![DeleteBuilder::equality(&path, 9, vec![1]).build()],
    );
    for _ in 0..2 {
        assert!(
            fixture
                .open_split(
                    &split,
                    &table_schema(),
                    DeleteEvaluationMode::ExcludeDeleted
                )
                .is_err()
        );
    }
    let d = fixture.manager.diagnostics().unwrap();
    assert_eq!(
        (d.physical_load_attempts, d.physical_loads, d.decoded_rows),
        (1, 0, 1)
    );
    assert_eq!(
        (d.keys, d.application_merges, d.completed_applications),
        (0, 0, 0)
    );
}

#[test]
fn large_union_merge_yields_between_bounded_chunks_and_observes_stop() {
    let fixture = Fixture::new();
    let group = EqualityFieldGroup::bind(&[1], &table_schema()).unwrap();
    let union = EqualityUnion::new(group);
    let keys = (0..8193)
        .map(|value| vec![Some(CanonicalScalar::Long(value))])
        .collect::<Vec<_>>();
    let wakes = Arc::new(WakeCount::default());
    let _entered = fixture._runtime.enter();
    let mut merge = Box::pin(union.merge_ready(&keys, 9, &fixture.manager.context));
    assert_pending(merge.as_mut(), &wakes);
    assert_eq!(
        union
            .shards
            .iter()
            .map(|shard| shard.read().unwrap().len())
            .sum::<usize>(),
        4096
    );
    fixture.manager.context.cancellation.cancel();
    assert!(fixture._runtime.block_on(merge).is_err());
    assert_eq!(
        union
            .shards
            .iter()
            .map(|shard| shard.read().unwrap().len())
            .sum::<usize>(),
        4096
    );
}

#[derive(Default)]
struct WakeCount {
    count: AtomicUsize,
    notified: tokio::sync::Notify,
}
impl std::task::Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.notified.notify_one();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.notified.notify_one();
    }
}
fn assert_pending<F: std::future::Future + ?Sized>(
    future: std::pin::Pin<&mut F>,
    wakes: &Arc<WakeCount>,
) {
    let waker = std::task::Waker::from(wakes.clone());
    let mut cx = std::task::Context::from_waker(&waker);
    assert!(future.poll(&mut cx).is_pending());
}
// This fixture owns one explicitly named artifact; its only physical read
// is held through exit. No ordinal task counter selects a barrier.
struct NamedArtifactGate {
    name: &'static str,
    handle: tokio::runtime::Handle,
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    finished: Arc<tokio::sync::Notify>,
    loads: AtomicUsize,
}
impl FileIoSpawner for NamedArtifactGate {
    fn spawn(
        &self,
        task: novarocks_fs::FileIoFuture,
    ) -> novarocks_fs::FileResult<novarocks_fs::FileIoJob> {
        self.loads.fetch_add(1, Ordering::Relaxed);
        let barrier = self.release.clone();
        let started = self.started.clone();
        let finished = self.finished.clone();
        Ok(novarocks_fs::FileIoJob::new(self.handle.spawn(
            async move {
                started.notify_one();
                barrier.notified().await;
                task.await;
                finished.notify_one();
            },
        )))
    }
}
struct FlightFixture {
    runtime: tokio::runtime::Runtime,
    _directory: tempfile::TempDir,
    file: novarocks_fs::BoundFile,
    context: FileReadContext,
    gate: Arc<NamedArtifactGate>,
    operations: novarocks_spi::connector::read_stack::ConnectorSourceOperations,
    reads: crate::file_reader::tests::RangeReceipts,
}
impl FlightFixture {
    fn new(name: &'static str) -> Self {
        use std::num::NonZeroUsize;
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let gate = Arc::new(NamedArtifactGate {
            name,
            handle: runtime.handle().clone(),
            started: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
            finished: Arc::new(tokio::sync::Notify::new()),
            loads: AtomicUsize::new(0),
        });
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join(name);
        fs::write(&path, b"immutable-delete-payload").unwrap();
        let access = FsAccessResolver::new()
            .resolve_location(
                novarocks_spi::connector::StorageAccessDomainId::from_bytes([1; 32]),
                path.to_string_lossy(),
                None,
            )
            .unwrap();
        let reads = crate::file_reader::tests::RangeReceipts::default();
        let access = crate::file_reader::tests::recorded_access(&access, &reads);
        let file = access
            .bind(
                0,
                novarocks_fs::FileIdentity::new(path.to_string_lossy(), 24, None),
            )
            .unwrap();
        let service = novarocks_fs::FileRangeService::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(4).unwrap(),
            gate.clone(),
            runtime.handle().clone(),
        );
        let operations =
            novarocks_spi::connector::read_stack::ConnectorSourceOperations::for_test();
        let range = service.bind(
            novarocks_fs::FileRangeScope::for_test(1, 2, 1, 3, 4, 5).unwrap(),
            operations.clone(),
        );
        let context = FileReadContext {
            cancellation: FileCancellation::new(),
            deadline: Some(Instant::now() + Duration::from_secs(60)),
            runtime: Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            io_spawner: gate.clone(),
            range: Some(range),
        };
        Self {
            runtime,
            _directory: directory,
            file,
            context,
            gate,
            operations,
            reads,
        }
    }
    fn flight(
        &self,
        cell: Arc<SharedLoad<usize>>,
    ) -> BoxFuture<'static, Result<Arc<usize>, ConnectorError>> {
        let context = self.context.clone();
        let file = self.file.clone();
        async move {
            let load_context = context.clone();
            cell.get(context, move || {
                async move {
                    let range = load_context.range.as_ref().unwrap();
                    let cancellation = load_context.cancellation.child();
                    let mut request = range
                        .start_wait(
                            file,
                            novarocks_fs::FileReadRange::bounded(0, 24).unwrap(),
                            cancellation,
                        )
                        .await
                        .map_err(map_file_error)?;
                    let bytes = request.result_ready().await.map_err(map_file_error)?;
                    request.drained().await.map_err(map_file_error)?;
                    Ok(bytes.len())
                }
                .boxed()
            })
            .await
        }
        .boxed()
    }
}
#[test]
fn stopped_owner_overrides_physical_error_without_publishing_ready() {
    let fixture = Fixture::new();
    let cell = Arc::new(SharedLoad::<usize>::default());
    let stop = fixture.manager.context.cancellation.clone();
    let error = fixture
        ._runtime
        .block_on(cell.get(fixture.manager.context.clone(), move || {
            async move {
                stop.cancel();
                Err(corrupt("physical helper returned its cancellation as text"))
            }
            .boxed()
        }))
        .unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Cancelled);
    assert!(!cell.ready.initialized());
}

#[test]
fn cancelling_initializer_waiter_preserves_other_demand_and_wakes_it() {
    let fixture = FlightFixture::new("required-delete-A.bin");
    let cell = Arc::new(SharedLoad::default());
    let wakes = Arc::new(WakeCount::default());
    let _entered = fixture.runtime.enter();
    let mut first = fixture.flight(cell.clone());
    assert_pending(first.as_mut(), &wakes);
    fixture.runtime.block_on(fixture.gate.started.notified());
    let mut second = fixture.flight(cell.clone());
    assert_pending(second.as_mut(), &wakes);
    drop(first);
    assert_eq!(
        fixture.gate.loads.load(Ordering::Relaxed),
        1,
        "{} is still owned by second demand",
        fixture.gate.name
    );
    assert_eq!(fixture.operations.live_operations(), 1);
    fixture.gate.release.notify_one();
    fixture.runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), wakes.notified.notified())
            .await
            .expect("registered pending waiter is awakened");
    });
    assert!(
        wakes.count.load(Ordering::Relaxed) > 0,
        "physical completion wakes the registered pending waiter before repoll"
    );
    assert_eq!(*fixture.runtime.block_on(second).unwrap(), 24);
    assert!(
        wakes.count.load(Ordering::Relaxed) > 0,
        "completion wakes the pending waiter"
    );
    assert!(cell.ready.initialized());
    assert_eq!(fixture.gate.loads.load(Ordering::Relaxed), 1);
    fixture.operations.seal();
    assert_eq!(
        fixture.runtime.block_on(fixture.operations.exited()),
        novarocks_spi::connector::read_stack::ConnectorSourceExitProof::Exited
    );
}
#[test]
fn last_demand_drop_requests_stop_but_physical_exit_remains_owned() {
    let fixture = FlightFixture::new("required-delete-B.bin");
    let cell = Arc::new(SharedLoad::default());
    let wakes = Arc::new(WakeCount::default());
    let _entered = fixture.runtime.enter();
    let mut first = fixture.flight(cell.clone());
    assert_pending(first.as_mut(), &wakes);
    fixture.runtime.block_on(fixture.gate.started.notified());
    let mut second = fixture.flight(cell.clone());
    assert_pending(second.as_mut(), &wakes);
    drop(first);
    drop(second);
    assert!(
        !cell.ready.initialized(),
        "abandoned load publishes no Ready"
    );
    assert!(
        cell.flight
            .lock()
            .unwrap()
            .as_ref()
            .and_then(WeakShared::upgrade)
            .is_none(),
        "last waiter drop releases the initializer"
    );
    assert_eq!(fixture.operations.live_operations(), 1);
    assert!(!fixture.context.cancellation.is_cancelled());
    fixture.gate.release.notify_one();
    fixture.runtime.block_on(fixture.gate.finished.notified());
    assert!(
        fixture.reads.0.lock().unwrap().is_empty(),
        "dropped request stops before object-store dispatch without source sealing"
    );
    assert!(!fixture.operations.is_sealed());
    // The gate marks the spawned read body's return, before the service's
    // supervisor joins it and settles its request/ticket. This isolated
    // fixture owns exactly this artifact's service; drain observes both
    // request settlement and supervisor exit without cancelling or sealing.
    fixture
        .runtime
        .block_on(fixture.context.range.as_ref().unwrap().service().drain())
        .unwrap();
    assert!(!fixture.operations.is_sealed());
    assert!(!fixture.context.cancellation.is_cancelled());
    assert_eq!(fixture.operations.live_operations(), 0);
    fixture.operations.seal();
    let exited = Box::pin(fixture.operations.exited());
    assert_eq!(
        fixture.runtime.block_on(exited),
        novarocks_spi::connector::read_stack::ConnectorSourceExitProof::Exited
    );
    assert_eq!(fixture.operations.live_operations(), 0);
    assert!(!cell.ready.initialized());
    // A dropped initializer leaves the application slot reusable.
    let restored = fixture
        .runtime
        .block_on(cell.get(fixture.context.clone(), || async { Ok(73) }.boxed()))
        .unwrap();
    assert_eq!(*restored, 73);
    fixture.context.cancellation.cancel();
    assert!(
        fixture
            .runtime
            .block_on(cell.get(fixture.context.clone(), || async { Ok(99) }.boxed()))
            .is_err()
    );
}
