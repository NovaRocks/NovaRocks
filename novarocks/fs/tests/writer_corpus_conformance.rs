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

mod common;

use std::fmt::Write as _;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    Array, Decimal128Array, Int32Array, Int64Array, ListArray, StringArray,
    TimestampMicrosecondArray, TimestampNanosecondArray,
};
use novarocks_fs::{
    FileCancellation, FileFormat, FileIdentity, FileIoRuntime, FileProjection, FileReadBudget,
    FileReadContext, FileReadRange, FileReadRequest, FileTaskSpawner, FsAccessResolver,
    PhysicalPruning, inspect_parquet_metadata, open_file_reader,
};
use novarocks_spi::connector::StorageAccessDomainId;
use sha2::{Digest, Sha256};

use common::TestIo;

const ROWS: usize = 4096;

#[derive(Clone, Copy)]
enum Writer {
    Spark,
    PyArrow,
    Trino,
    Flink,
}

impl Writer {
    fn name(self) -> &'static str {
        match self {
            Self::Spark => "Spark/parquet-mr",
            Self::PyArrow => "PyArrow 23.0.1",
            Self::Trino => "Trino 483",
            Self::Flink => "Flink 1.20.5",
        }
    }

    fn relative_file(self) -> &'static str {
        match self {
            Self::Spark => "../short_iceberg/data.parquet",
            Self::PyArrow => "pyarrow/pyarrow-23.0.1.parquet",
            Self::Trino => "trino/trino-483.parquet",
            Self::Flink => "flink/flink-local-1.20.5.parquet",
        }
    }

    fn file_sha256(self) -> &'static str {
        match self {
            Self::Spark => "83f8920d01744bb357f93abc46bfce7d6c5c40c9d3c15d44f2d36392926112e3",
            Self::PyArrow => "201538becb093d98fec2be4b3dca7126f2e7907f97d9fed63a571e84fd855f40",
            Self::Trino => "012f19f5e78f28bc006bcdae895df74fd9a69dda7428cbee05986dd6e0dc0ea8",
            Self::Flink => "55faf3ea8a35561c8a4750c244e25bf1d9ad4a2b301508a5e67f6956a9146851",
        }
    }

    fn position_oracle_sha256(self) -> Option<&'static str> {
        match self {
            Self::Spark => None,
            Self::PyArrow => {
                Some("2cf645aec1ff09ceac94895976db7d23ae80271c8af1e11cf353f416f09ad77e")
            }
            Self::Trino => Some("d924ee7a80d12c7437b7486d2f1bcc81cfa86b3a00468daf7e49f10cd05f8320"),
            Self::Flink => Some("4cea3896269603b7ae08165f3717406e02f922f443ad35a5a1fc4dfb41977e89"),
        }
    }

    fn expected_columns(self) -> &'static [&'static str] {
        match self {
            Self::Spark => &["id", "value"],
            Self::PyArrow => &["id", "label", "nested", "event_time", "amount"],
            Self::Trino => &["id", "category", "amount", "nested"],
            Self::Flink => &["id", "category", "label", "amount", "event_time", "nested"],
        }
    }
}

fn corpus_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/benchmarks/uea4a2/fixtures/corpus")
}

fn assert_sha256(path: &Path, expected: &str) {
    let data =
        std::fs::read(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    let mut actual = String::new();
    for byte in Sha256::digest(data) {
        write!(&mut actual, "{byte:02x}").expect("write digest");
    }
    assert_eq!(actual, expected, "frozen corpus hash: {}", path.display());
}

fn physical_ids(writer: Writer) -> Vec<i64> {
    if matches!(writer, Writer::Spark) {
        return (0..ROWS as i64).collect();
    }
    let path = corpus_root().join(match writer {
        Writer::PyArrow => "pyarrow/physical_ids.txt",
        Writer::Trino => "trino/physical_ids.txt",
        Writer::Flink => "flink/physical_ids.txt",
        Writer::Spark => unreachable!(),
    });
    assert_sha256(
        &path,
        writer.position_oracle_sha256().expect("external oracle"),
    );
    let positions = std::fs::read_to_string(&path)
        .expect("read independent physical-position oracle")
        .lines()
        .map(|line| line.parse::<i64>().expect("integer physical ID"))
        .collect::<Vec<_>>();
    assert_eq!(positions.len(), ROWS, "{} oracle row count", writer.name());
    let mut sorted = positions.clone();
    sorted.sort_unstable();
    assert_eq!(
        sorted,
        (0..ROWS as i64).collect::<Vec<_>>(),
        "{} oracle ID coverage",
        writer.name()
    );
    positions
}

fn downcast<'a, T: Array + 'static>(
    batch: &'a arrow::record_batch::RecordBatch,
    column: &str,
) -> &'a T {
    let index = batch
        .schema()
        .index_of(column)
        .expect("oracle column in batch");
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .unwrap_or_else(|| {
            panic!(
                "{column} has unexpected Arrow type {:?}",
                batch.column(index).data_type()
            )
        })
}

fn assert_label(batch: &arrow::record_batch::RecordBatch, row: usize, id: i64) {
    let labels = downcast::<StringArray>(batch, "label");
    if id % 17 == 0 {
        assert!(labels.is_null(row), "label null for id {id}");
    } else {
        assert_eq!(labels.value(row), format!("label-{}", id % 11));
    }
}

fn assert_nested(batch: &arrow::record_batch::RecordBatch, row: usize, id: i64, nullable: bool) {
    let lists = downcast::<ListArray>(batch, "nested");
    if nullable && id % 13 == 0 {
        assert!(lists.is_null(row), "nested null for id {id}");
        return;
    }
    assert!(!lists.is_null(row), "nested present for id {id}");
    let values = lists.value(row);
    let values = values
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("nested int32 elements");
    assert_eq!(values.len(), 2, "nested length for id {id}");
    assert_eq!(values.value(0), (id % 5) as i32);
    assert_eq!(values.value(1), (id % 7) as i32);
}

fn assert_row(writer: Writer, batch: &arrow::record_batch::RecordBatch, row: usize, id: i64) {
    assert_eq!(
        downcast::<Int64Array>(batch, "id").value(row),
        id,
        "{} row ID",
        writer.name()
    );
    match writer {
        Writer::Spark => {
            assert_eq!(downcast::<Int64Array>(batch, "value").value(row), id * 3);
        }
        Writer::PyArrow => {
            assert_label(batch, row, id);
            assert_nested(batch, row, id, true);
            assert_eq!(
                downcast::<TimestampMicrosecondArray>(batch, "event_time").value(row),
                1_700_000_000_000_000 + id * 1_000
            );
            assert_eq!(
                downcast::<Decimal128Array>(batch, "amount").value(row),
                i128::from(id)
            );
        }
        Writer::Trino => {
            assert_eq!(
                downcast::<Int32Array>(batch, "category").value(row),
                (id % 17) as i32
            );
            assert_eq!(
                downcast::<Decimal128Array>(batch, "amount").value(row),
                i128::from(id)
            );
            assert_nested(batch, row, id, false);
        }
        Writer::Flink => {
            assert_eq!(
                downcast::<Int32Array>(batch, "category").value(row),
                (id % 17) as i32
            );
            assert_label(batch, row, id);
            assert_eq!(
                downcast::<Decimal128Array>(batch, "amount").value(row),
                i128::from(id)
            );
            assert_eq!(
                downcast::<TimestampNanosecondArray>(batch, "event_time").value(row),
                1_704_067_200_000_000_000
            );
            assert_nested(batch, row, id, false);
        }
    }
}

fn assert_writer_via_production_fs(writer: Writer) {
    let path = corpus_root().join(writer.relative_file());
    assert_sha256(&path, writer.file_sha256());
    let expected_ids = physical_ids(writer);
    let file_size = std::fs::metadata(&path).expect("frozen Parquet file").len();
    let access = FsAccessResolver::new()
        .resolve_location(
            StorageAccessDomainId::from_bytes([42; 32]),
            path.to_string_lossy(),
            None,
        )
        .expect("resolve local corpus through FS authority");
    let file = access
        .bind(
            0,
            FileIdentity::new(path.to_string_lossy(), file_size, Some(7)),
        )
        .expect("bind immutable corpus file");
    let io = TestIo::new();
    let runtime: Arc<dyn FileIoRuntime> = io.clone();
    let task_spawner: Arc<dyn FileTaskSpawner> = io;
    let context = FileReadContext {
        cancellation: FileCancellation::new(),
        deadline: None,
        runtime,
        task_spawner,
        range: None,
    };
    let inspection = inspect_parquet_metadata(file.clone(), None, context.clone())
        .expect("production FS metadata inspection");
    assert_eq!(
        inspection
            .row_groups()
            .iter()
            .map(|group| group.row_count)
            .sum::<u64>(),
        ROWS as u64
    );
    assert_eq!(
        inspection
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        writer.expected_columns().to_vec()
    );
    let request = FileReadRequest {
        file,
        format: FileFormat::Parquet,
        range: FileReadRange::WholeFile,
        projection: FileProjection::All,
        budget: FileReadBudget {
            max_rows: NonZeroUsize::new(511).expect("positive rows"),
            max_bytes: NonZeroUsize::new(1024 * 1024).expect("positive bytes"),
        },
        predicates: Vec::new(),
        pruning: PhysicalPruning::default(),
        options: Default::default(),
        cache: None,
        prepared_input: None,
        context,
    };
    let mut reader = open_file_reader(request).expect("open production FS reader");
    let mut next_position = 0;
    let mut batches = 0;
    while let Some(file_batch) = reader
        .next_batch()
        .expect("decode corpus with production FS reader")
    {
        batches += 1;
        let batch = &file_batch.batch;
        let positions = file_batch
            .physical_row_positions
            .as_ref()
            .expect("absolute file positions");
        assert_eq!(positions.len(), batch.num_rows());
        for row in 0..batch.num_rows() {
            assert!(next_position < ROWS, "{} returned extra row", writer.name());
            assert_eq!(
                positions.value(row),
                next_position as u64,
                "{} absolute physical position",
                writer.name()
            );
            assert_row(writer, batch, row, expected_ids[next_position]);
            next_position += 1;
        }
    }
    assert!(
        batches > 1,
        "{} must cross FS batch boundaries",
        writer.name()
    );
    assert_eq!(
        next_position,
        ROWS,
        "{} complete physical row coverage",
        writer.name()
    );
}

#[test]
fn spark_parquet_mr_rows_and_absolute_positions() {
    assert_writer_via_production_fs(Writer::Spark);
}

#[test]
fn pyarrow_rows_and_absolute_positions() {
    assert_writer_via_production_fs(Writer::PyArrow);
}

#[test]
fn trino_rows_and_absolute_positions() {
    assert_writer_via_production_fs(Writer::Trino);
}

#[test]
fn flink_rows_and_absolute_positions() {
    assert_writer_via_production_fs(Writer::Flink);
}
