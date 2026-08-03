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

use std::time::{Duration, Instant};

use arrow::array::{Array, Int32Array, StringArray};
use novarocks_fs::{
    CacheOptions, DataCacheManager, DataCachePageCacheOptions, FileErrorKind, FileFormat,
    FileProjection, FileReadRange, MinMaxPredicateOp, MinMaxPredicateValue, PhysicalPageSelection,
    ScanPredicate, ScanPredicateDomain, ScanPredicateSource, inspect_parquet_metadata,
    open_file_reader,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use common::{Fixture, collect};

#[test]
fn parquet_metadata_inspection_reports_stable_footer_facts() {
    let fixture = Fixture::parquet();
    let request = fixture.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);
    let expected = {
        let file = std::fs::File::open(fixture.file.location().path()).expect("open fixture");
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("read fixture footer");
        builder
            .metadata()
            .row_groups()
            .iter()
            .enumerate()
            .map(|(ordinal, row_group)| {
                (
                    u32::try_from(ordinal).expect("fixture ordinal fits u32"),
                    u64::try_from(row_group.compressed_size())
                        .expect("fixture compressed size is non-negative"),
                    u64::try_from(row_group.num_rows()).expect("fixture row count is non-negative"),
                )
            })
            .collect::<Vec<_>>()
    };

    let first = inspect_parquet_metadata(fixture.file.clone(), None, request.context.clone())
        .expect("inspect fixture footer");
    let second = inspect_parquet_metadata(fixture.file.clone(), None, request.context)
        .expect("inspect fixture footer again");

    assert_eq!(
        first.schema(),
        second.schema(),
        "footer schema must be stable"
    );
    assert_eq!(
        first.physical_columns(),
        second.physical_columns(),
        "physical descriptors must be stable"
    );
    assert_eq!(
        first.row_groups(),
        second.row_groups(),
        "layout must be stable"
    );
    assert_eq!(
        first.row_groups().len(),
        2,
        "fixture has two real row groups"
    );
    assert_eq!(
        first
            .row_groups()
            .iter()
            .map(|layout| (layout.ordinal, layout.compressed_bytes, layout.row_count))
            .collect::<Vec<_>>(),
        expected
    );
    assert!(
        first
            .row_groups()
            .iter()
            .all(|layout| layout.compressed_bytes > 0)
    );
    assert_eq!(
        first
            .row_groups()
            .iter()
            .map(|layout| layout.row_count)
            .sum::<u64>(),
        8,
        "layout preserves total fixture row coverage"
    );
    assert_eq!(first.physical_columns().len(), 2);
    let id_stats = first
        .column_statistics(0, 0)
        .expect("fixture writes id statistics");
    assert_eq!(id_stats.null_count(), Some(0));
    assert!(id_stats.min_is_exact());
    assert!(id_stats.max_is_exact());
}

#[test]
fn parquet_metadata_inspection_rejects_corrupt_footer() {
    let fixture = Fixture::parquet();
    let path = fixture.file.location().path();
    let mut bytes = std::fs::read(path).expect("read fixture");
    *bytes.last_mut().expect("Parquet fixture is non-empty") ^= 0xff;
    std::fs::write(path, bytes).expect("corrupt footer marker");
    let request = fixture.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);

    let error = inspect_parquet_metadata(fixture.file.clone(), None, request.context)
        .expect_err("corrupt footer must not produce a layout");

    assert_eq!(error.kind(), FileErrorKind::Corrupt);
    assert!(
        error
            .to_string()
            .contains("inspect Parquet metadata failed"),
        "inspection keeps a typed footer failure boundary: {error}"
    );
}

#[test]
fn parquet_metadata_inspection_honors_cancel_and_deadline_before_footer_io() {
    let fixture = Fixture::parquet();
    let cancelled = fixture.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);
    cancelled.context.cancellation.cancel();
    assert_eq!(
        inspect_parquet_metadata(fixture.file.clone(), None, cancelled.context)
            .expect_err("cancelled inspection must not load a footer")
            .kind(),
        FileErrorKind::Cancelled
    );

    let mut expired = fixture.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);
    expired.context.deadline = Some(Instant::now() - Duration::from_millis(1));
    assert_eq!(
        inspect_parquet_metadata(fixture.file.clone(), None, expired.context)
            .expect_err("expired inspection must not load a footer")
            .kind(),
        FileErrorKind::DeadlineExceeded
    );
}

#[test]
fn parquet_projects_all_root_columns() {
    let fixture = Fixture::parquet();
    let mut reader = open_file_reader(fixture.request(
        FileFormat::Parquet,
        FileProjection::All,
        1024,
        1024 * 1024,
    ))
    .expect("open reader");
    let batches = collect(reader.as_mut()).expect("read Parquet");
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.batch.num_rows())
            .sum::<usize>(),
        8
    );
    assert_eq!(batches[0].batch.num_columns(), 2);
}

#[test]
fn parquet_projects_root_names() {
    let fixture = Fixture::parquet();
    let mut reader = open_file_reader(fixture.request(
        FileFormat::Parquet,
        FileProjection::RootNames(vec!["name".to_string()]),
        1024,
        1024 * 1024,
    ))
    .expect("open reader");
    let batches = collect(reader.as_mut()).expect("read Parquet");
    assert_eq!(batches[0].batch.num_columns(), 1);
    assert!(batches[0].batch.column(0).as_any().is::<StringArray>());
}

#[test]
fn parquet_projects_root_indices() {
    let fixture = Fixture::parquet();
    let mut reader = open_file_reader(fixture.request(
        FileFormat::Parquet,
        FileProjection::RootIndices(vec![0]),
        1024,
        1024 * 1024,
    ))
    .expect("open reader");
    let batches = collect(reader.as_mut()).expect("read Parquet");
    assert!(batches[0].batch.column(0).as_any().is::<Int32Array>());
}

#[test]
fn parquet_projects_field_ids() {
    let fixture = Fixture::parquet();
    let mut reader = open_file_reader(fixture.request(
        FileFormat::Parquet,
        FileProjection::FieldIds(vec![20]),
        1024,
        1024 * 1024,
    ))
    .expect("open reader");
    let batches = collect(reader.as_mut()).expect("read Parquet");
    assert_eq!(batches[0].batch.schema().field(0).name(), "name");
    assert_eq!(
        batches[0]
            .batch
            .schema()
            .field(0)
            .metadata()
            .get(parquet::arrow::PARQUET_FIELD_ID_META_KEY)
            .map(String::as_str),
        Some("20"),
        "physical Parquet decoding must retain field IDs for Iceberg schema evolution"
    );
}

#[test]
fn parquet_range_selects_row_group_by_physical_offset() {
    let fixture = Fixture::parquet();
    let file = std::fs::File::open(fixture.file.location().path()).expect("open fixture");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("metadata");
    let second = builder.metadata().row_group(1);
    let start = second.columns()[0]
        .dictionary_page_offset()
        .unwrap_or_else(|| second.columns()[0].data_page_offset())
        .min(second.columns()[0].data_page_offset()) as u64;
    let mut request = fixture.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);
    request.range = FileReadRange::bounded(start, fixture.file.identity().file_size() - start)
        .expect("bounded range");
    let mut reader = open_file_reader(request).expect("open reader");
    let batches = collect(reader.as_mut()).expect("read Parquet");
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.batch.num_rows())
            .sum::<usize>(),
        4
    );
    assert_eq!(
        batches[0].physical_row_positions.as_ref().unwrap().value(0),
        4
    );
}

#[test]
fn parquet_predicate_prunes_row_groups() {
    let fixture = Fixture::parquet();
    let mut request = fixture.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);
    request.predicates.push(ScanPredicate::new(
        "id",
        ScanPredicateDomain::Range {
            op: MinMaxPredicateOp::Ge,
            value: MinMaxPredicateValue::Int32(4),
        },
        ScanPredicateSource::Static,
    ));
    let mut reader = open_file_reader(request).expect("open reader");
    let batches = collect(reader.as_mut()).expect("read Parquet");
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.batch.num_rows())
            .sum::<usize>(),
        4
    );
    assert_eq!(
        batches[0].physical_row_positions.as_ref().unwrap().value(0),
        4
    );
}

#[test]
fn parquet_predicate_binds_field_id_before_column_name() {
    let fixture = Fixture::parquet();
    let mut request = fixture.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);
    request.predicates.push(
        ScanPredicate::new(
            "renamed_id",
            ScanPredicateDomain::Range {
                op: MinMaxPredicateOp::Ge,
                value: MinMaxPredicateValue::Int32(4),
            },
            ScanPredicateSource::Static,
        )
        .with_physical_field_id(10),
    );
    let mut reader = open_file_reader(request).expect("open reader");
    let batches = collect(reader.as_mut()).expect("read Parquet");
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.batch.num_rows())
            .sum::<usize>(),
        4
    );
    assert_eq!(
        batches[0].physical_row_positions.as_ref().unwrap().value(0),
        4
    );
}

#[test]
fn parquet_honors_explicit_page_selection_and_positions() {
    let fixture = Fixture::parquet();
    let mut request = fixture.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);
    request.predicates.push(ScanPredicate::new(
        "id",
        ScanPredicateDomain::Range {
            op: MinMaxPredicateOp::Ge,
            value: MinMaxPredicateValue::Int32(2),
        },
        ScanPredicateSource::Static,
    ));
    request.pruning.row_groups = Some(vec![0]);
    request.pruning.pages.push(PhysicalPageSelection {
        row_group: 0,
        page_indices: vec![1],
    });
    let mut reader = open_file_reader(request).expect("open reader");
    let batches = collect(reader.as_mut()).expect("read Parquet");
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.batch.num_rows())
            .sum::<usize>(),
        2
    );
    assert_eq!(
        batches[0].physical_row_positions.as_ref().unwrap().value(0),
        2
    );
    assert_eq!(batches[0].batch.num_columns(), 2);
    assert_eq!(reader.metrics_snapshot().delayed_materialization_ranges, 1);
}

#[test]
fn parquet_enforces_row_budget() {
    let fixture = Fixture::parquet();
    let mut reader =
        open_file_reader(fixture.request(FileFormat::Parquet, FileProjection::All, 3, 1024 * 1024))
            .expect("open reader");
    let batches = collect(reader.as_mut()).expect("read Parquet");
    assert!(batches.iter().all(|batch| batch.batch.num_rows() <= 3));
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.batch.num_rows())
            .sum::<usize>(),
        8
    );
}

#[test]
fn parquet_enforces_byte_budget_and_rejects_oversized_row() {
    let fixture = Fixture::parquet();
    let mut reader =
        open_file_reader(fixture.request(FileFormat::Parquet, FileProjection::All, 8, 260))
            .expect("open reader");
    let batches = collect(reader.as_mut()).expect("read within byte budget");
    assert!(
        batches
            .iter()
            .all(|batch| batch.batch.get_array_memory_size() <= 260)
    );

    let mut reader =
        open_file_reader(fixture.request(FileFormat::Parquet, FileProjection::All, 8, 1))
            .expect("open reader");
    assert_eq!(
        reader
            .next_batch()
            .expect_err("one row exceeds budget")
            .kind(),
        FileErrorKind::ResourceExhausted
    );
}

#[test]
fn parquet_positions_stay_aligned_across_budget_slices() {
    let fixture = Fixture::parquet();
    let mut reader =
        open_file_reader(fixture.request(FileFormat::Parquet, FileProjection::All, 3, 1024 * 1024))
            .expect("open reader");
    let batches = collect(reader.as_mut()).expect("read Parquet");
    let positions = batches
        .iter()
        .flat_map(|batch| {
            batch
                .physical_row_positions
                .as_ref()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect::<Vec<_>>();
    assert_eq!(positions, (0..8).collect::<Vec<_>>());
}

#[test]
fn parquet_exact_ranges_use_foundation_page_cache() {
    let _ = DataCacheManager::instance().init_page_cache(DataCachePageCacheOptions {
        capacity: 1024 * 1024,
        evict_probability: 100,
    });
    let fixture = Fixture::parquet();
    let cache = DataCacheManager::instance().external_context(CacheOptions {
        enable_scan_datacache: true,
        enable_populate_datacache: true,
        enable_datacache_async_populate_mode: false,
        enable_datacache_io_adaptor: false,
        enable_cache_select: false,
        datacache_evict_probability: 100,
        datacache_priority: 0,
        datacache_ttl_seconds: 0,
        datacache_sharing_work_period: None,
    });
    let mut first = fixture.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);
    first.cache = Some(cache.clone());
    let mut first = open_file_reader(first).expect("first reader");
    collect(first.as_mut()).expect("first read");

    let mut second = fixture.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);
    second.cache = Some(cache);
    let mut second = open_file_reader(second).expect("second reader");
    collect(second.as_mut()).expect("second read");
    assert!(second.metrics_snapshot().cache_hits > 0);
}

#[test]
fn orc_projects_physical_columns_and_honors_row_budget() {
    let fixture = Fixture::orc();
    let mut reader = open_file_reader(fixture.request(
        FileFormat::Orc,
        FileProjection::RootNames(vec!["name".to_string()]),
        3,
        1024 * 1024,
    ))
    .expect("open ORC reader");
    let batches = collect(reader.as_mut()).expect("read ORC");
    assert!(batches.iter().all(|batch| batch.batch.num_rows() <= 3));
    assert!(batches[0].batch.column(0).as_any().is::<StringArray>());
    assert!(
        batches
            .iter()
            .all(|batch| batch.physical_row_positions.is_none())
    );
}
