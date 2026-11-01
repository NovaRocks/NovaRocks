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

use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use arrow::array::{Array, Float32Array, Float64Array, Int32Array, StringArray};
use bytes::BytesMut;
use novarocks_fs::{
    CacheOptions, DataCacheManager, DataCachePageCacheOptions, FileErrorKind, FileFormat,
    FileProjection, FileRangeScope, FileRangeService, FileReadRange, MinMaxPredicateOp,
    MinMaxPredicateValue, PhysicalPageSelection, PreparedFileInput, ScanPredicate,
    ScanPredicateDomain, ScanPredicateSource, inspect_parquet_metadata, open_file_reader,
    open_file_reader_async,
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
fn parquet_decoder_consumes_prepared_whole_file_without_source_io() {
    let fixture = Fixture::parquet();
    let path = fixture.file.location().path();
    let bytes = std::fs::read(path).expect("read fixture backing");
    let prepared = PreparedFileInput::new(&fixture.file, 0, BytesMut::from(bytes.as_slice()))
        .expect("prepared whole file");
    let mut request = fixture.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);
    request.prepared_input = Some(prepared);
    std::fs::remove_file(path).expect("remove source to prove decoder needs no GET");
    let mut reader = open_file_reader(request).expect("open from prepared input");
    let batches = collect(reader.as_mut()).expect("decode prepared input");
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.batch.num_rows())
            .sum::<usize>(),
        8
    );
    assert_eq!(reader.metrics_snapshot().read_requests, 0);
    assert_eq!(reader.metrics_snapshot().bytes_read, 0);
}

#[test]
fn parquet_decoder_promotes_partial_backing_and_reads_only_its_gap() {
    let fixture = Fixture::parquet();
    let bytes = std::fs::read(fixture.file.location().path()).expect("read fixture backing");
    let covered = bytes.len() / 2;
    let prepared = PreparedFileInput::new(&fixture.file, 0, BytesMut::from(&bytes[..covered]))
        .expect("prepared first half");
    let service = FileRangeService::new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(2).unwrap(),
        fixture.io.clone(),
        fixture.io.handle(),
    );
    let mut request = fixture.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);
    request.prepared_input = Some(prepared);
    request.context.range = Some(service.bind(
        FileRangeScope::try_new(1, 0, 1, 1, 0, 1).unwrap(),
        novarocks_spi::connector::read_stack::ConnectorSourceOperations::new(),
    ));
    let mut reader = open_file_reader(request).expect("open from partial input");
    let batches = collect(reader.as_mut()).expect("decode promoted input");
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.batch.num_rows())
            .sum::<usize>(),
        8
    );
    let metrics = reader.metrics_snapshot();
    assert_eq!(metrics.bytes_read, (bytes.len() - covered) as u64);
    assert_eq!(metrics.partial_prefetch_copy_bytes, covered as u64);
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

/// Iceberg `int -> long` promotion reaching the reader: once the table schema
/// is promoted to `long`, predicate literals arrive as `Int64` while data files
/// written before the promotion still publish `Int32` statistics. The two are
/// not comparable, so pruning must keep every row group. Skipping them would
/// silently drop rows that satisfy the predicate -- the one failure mode that
/// pruning is never allowed to have.
#[test]
fn parquet_predicate_keeps_row_groups_when_literal_type_outgrew_statistics() {
    let fixture = Fixture::parquet();

    let baseline = fixture.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);
    let mut reader = open_file_reader(baseline).expect("open reader");
    let unfiltered: usize = collect(reader.as_mut())
        .expect("read Parquet")
        .iter()
        .map(|batch| batch.batch.num_rows())
        .sum();
    assert!(
        unfiltered > 0,
        "fixture must produce rows for this test to mean anything"
    );

    let mut request = fixture.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);
    request.predicates.push(ScanPredicate::new(
        "id",
        ScanPredicateDomain::Range {
            op: MinMaxPredicateOp::Ge,
            // The file publishes Int32 statistics for `id`; an Int64 literal
            // cannot be compared against them.
            value: MinMaxPredicateValue::Int64(4),
        },
        ScanPredicateSource::Static,
    ));
    let mut reader = open_file_reader(request).expect("open reader");
    let kept: usize = collect(reader.as_mut())
        .expect("read Parquet")
        .iter()
        .map(|batch| batch.batch.num_rows())
        .sum();

    assert_eq!(
        kept, unfiltered,
        "an incomparable literal must not prune any row group"
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
    assert_eq!(reader.metrics_snapshot().delayed_materialization_ranges, 0);
}

#[test]
fn parquet_predicate_page_index_prunes_rows_and_records_effect() {
    let fixture = Fixture::parquet();
    let mut request = fixture.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);
    request.options.enable_parquet_reader_page_index = true;
    request.pruning.row_groups = Some(vec![0]);
    request.predicates.push(ScanPredicate::new(
        "id",
        ScanPredicateDomain::Range {
            op: MinMaxPredicateOp::Ge,
            value: MinMaxPredicateValue::Int32(2),
        },
        ScanPredicateSource::Static,
    ));

    let mut reader = open_file_reader(request).expect("open page-index reader");
    let batches = collect(reader.as_mut()).expect("read pruned pages");
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.batch.num_rows())
            .sum::<usize>(),
        2
    );
    assert_eq!(
        batches[0].physical_row_positions.as_ref().unwrap().values(),
        &[2, 3]
    );
    let metrics = reader.metrics_snapshot();
    assert_eq!(metrics.page_index_attempts, 1);
    assert_eq!(metrics.page_index_fallbacks, 0);
    assert_eq!(metrics.page_index_rows_considered, 4);
    assert_eq!(metrics.page_index_rows_pruned, 2);
}

#[test]
fn parquet_predicate_page_index_falls_back_for_incomparable_bounds() {
    let fixture = Fixture::parquet();
    let mut request = fixture.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);
    request.options.enable_parquet_reader_page_index = true;
    request.pruning.row_groups = Some(vec![0]);
    request.predicates.push(ScanPredicate::new(
        "id",
        ScanPredicateDomain::Range {
            op: MinMaxPredicateOp::Ge,
            value: MinMaxPredicateValue::Int64(2),
        },
        ScanPredicateSource::Static,
    ));

    let mut reader = open_file_reader(request).expect("open page-index reader");
    let batches = collect(reader.as_mut()).expect("read fallback pages");
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.batch.num_rows())
            .sum::<usize>(),
        4,
        "incomparable page bounds must preserve the whole row group"
    );
    let metrics = reader.metrics_snapshot();
    assert_eq!(metrics.page_index_attempts, 1);
    assert_eq!(metrics.page_index_fallbacks, 1);
    assert_eq!(metrics.page_index_rows_considered, 4);
    assert_eq!(metrics.page_index_rows_pruned, 0);
}

#[test]
fn parquet_predicate_page_index_uses_name_when_the_requested_field_id_is_missing() {
    let fixture = Fixture::parquet();
    let mut request = fixture.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);
    request.options.enable_parquet_reader_page_index = true;
    request.pruning.row_groups = Some(vec![0]);
    request.predicates.push(
        ScanPredicate::new(
            "id",
            ScanPredicateDomain::Range {
                op: MinMaxPredicateOp::Ge,
                value: MinMaxPredicateValue::Int32(2),
            },
            ScanPredicateSource::Static,
        )
        .with_physical_field_id(999),
    );

    let mut reader = open_file_reader(request).expect("open page-index reader");
    let batches = collect(reader.as_mut()).expect("read pruned pages");
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.batch.num_rows())
            .sum::<usize>(),
        2
    );
    assert_eq!(reader.metrics_snapshot().page_index_fallbacks, 0);
}

#[test]
fn parquet_page_index_read_does_not_reuse_a_footer_only_metadata_cache_entry() {
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
    let inspection = fixture.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);
    inspect_parquet_metadata(
        fixture.file.clone(),
        Some(cache.clone()),
        inspection.context,
    )
    .expect("populate footer-only metadata cache entry");

    let mut request = fixture.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);
    request.cache = Some(cache);
    request.options.enable_parquet_reader_page_index = true;
    request.pruning.row_groups = Some(vec![0]);
    request.predicates.push(ScanPredicate::new(
        "id",
        ScanPredicateDomain::Range {
            op: MinMaxPredicateOp::Ge,
            value: MinMaxPredicateValue::Int32(2),
        },
        ScanPredicateSource::Static,
    ));
    let mut reader = open_file_reader(request).expect("reload page-index-capable metadata");
    let batches = collect(reader.as_mut()).expect("read page-index selection");
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.batch.num_rows())
            .sum::<usize>(),
        2
    );
    let metrics = reader.metrics_snapshot();
    assert_eq!(metrics.page_index_fallbacks, 0);
    assert_eq!(metrics.page_index_rows_pruned, 2);
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
    first.options.coalesce_reads = false;
    let mut first = open_file_reader(first).expect("first reader");
    collect(first.as_mut()).expect("first read");

    let mut second = fixture.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);
    second.cache = Some(cache);
    second.options.coalesce_reads = false;
    let mut second = open_file_reader(second).expect("second reader");
    collect(second.as_mut()).expect("second read");
    assert!(second.metrics_snapshot().cache_hits > 0);
}

#[test]
fn parquet_read_only_cache_hits_prewarmed_ranges_without_populating_misses() {
    let _ = DataCacheManager::instance().init_page_cache(DataCachePageCacheOptions {
        capacity: 1024 * 1024,
        evict_probability: 100,
    });
    let read_write_cache = DataCacheManager::instance().external_context(CacheOptions {
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
    let read_only_cache = DataCacheManager::instance().external_context(CacheOptions {
        enable_scan_datacache: true,
        enable_populate_datacache: false,
        enable_datacache_async_populate_mode: false,
        enable_datacache_io_adaptor: false,
        enable_cache_select: false,
        datacache_evict_probability: 100,
        datacache_priority: 0,
        datacache_ttl_seconds: 0,
        datacache_sharing_work_period: None,
    });

    let prewarmed = Fixture::parquet();
    let mut warm_request =
        prewarmed.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);
    warm_request.cache = Some(read_write_cache);
    warm_request.options.coalesce_reads = false;
    let mut warm_reader = open_file_reader(warm_request).expect("open cache-warming reader");
    collect(warm_reader.as_mut()).expect("warm cache");

    let mut cached_request =
        prewarmed.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);
    cached_request.cache = Some(read_only_cache.clone());
    cached_request.options.coalesce_reads = false;
    let mut cached_reader = open_file_reader(cached_request).expect("open read-only cached reader");
    collect(cached_reader.as_mut()).expect("read prewarmed cache");
    assert!(
        cached_reader.metrics_snapshot().cache_hits > 0,
        "read-only cache policy may consume bytes that an earlier request populated"
    );

    let uncached = Fixture::parquet();
    let mut first_miss =
        uncached.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);
    first_miss.cache = Some(read_only_cache.clone());
    first_miss.options.coalesce_reads = false;
    let mut first_reader = open_file_reader(first_miss).expect("open first read-only miss");
    collect(first_reader.as_mut()).expect("read uncached file");
    assert_eq!(first_reader.metrics_snapshot().cache_hits, 0);
    assert!(first_reader.metrics_snapshot().cache_misses > 0);

    let mut second_miss =
        uncached.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);
    second_miss.cache = Some(read_only_cache);
    second_miss.options.coalesce_reads = false;
    let mut second_reader = open_file_reader(second_miss).expect("open second read-only miss");
    collect(second_reader.as_mut()).expect("read uncached file again");
    let metrics = second_reader.metrics_snapshot();
    assert_eq!(
        metrics.cache_hits, 0,
        "a populate-disabled miss must not become a cached range"
    );
    assert!(metrics.cache_misses > 0);
}

// The bound fixture supplies authorized input only. Unsupported format dispatch
// must refuse before interpreting its bytes, in both entry points.
#[test]
fn orc_physical_reads_are_refused_with_a_recovery_condition() {
    let fixture = Fixture::parquet();
    let request = fixture.request(FileFormat::Orc, FileProjection::All, 3, 1024 * 1024);
    let error = match open_file_reader(request) {
        Ok(_) => panic!("suspended ORC reads must be refused"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), FileErrorKind::Unsupported);
    assert!(
        error
            .to_string()
            .contains("ORC physical reads are suspended")
    );
    assert!(
        error
            .to_string()
            .contains("orc-rust releases Arrow 60 and Zstd 0.14 support")
    );
    assert_eq!(
        fixture.io.block_on_bytes_calls(),
        0,
        "dispatch must not read the input"
    );
}

#[test]
fn awaited_orc_physical_reads_are_refused_with_a_recovery_condition() {
    let fixture = Fixture::parquet();
    let request = fixture.request(FileFormat::Orc, FileProjection::All, 3, 1024 * 1024);
    let error = match fixture
        .io
        .handle()
        .block_on(open_file_reader_async(request, None))
    {
        Ok(_) => panic!("suspended ORC reads must be refused"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), FileErrorKind::Unsupported);
    assert!(
        error
            .to_string()
            .contains("ORC physical reads are suspended")
    );
    assert!(
        error
            .to_string()
            .contains("orc-rust releases Arrow 60 and Zstd 0.14 support")
    );
    assert_eq!(
        fixture.io.block_on_bytes_calls(),
        0,
        "dispatch must not read the input"
    );
}

/// Statistics can prune candidates, but cannot remove a row satisfying the
/// independent IEEE comparison below. Row IDs avoid comparing NaNs for equality.
#[test]
fn parquet_float_statistics_pruning_preserves_nan_and_signed_zero_results() {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("float-statistics.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("f32", DataType::Float32, false),
        Field::new("f64", DataType::Float64, false),
    ]));
    // Each slice is flushed as a separate row group. The final finite group
    // proves that the pruning path actually eliminates candidates.
    let groups: &[&[f64]] = &[
        &[f64::NAN, f64::NAN],
        &[f64::NAN, -2.0, 3.0, f64::NAN],
        &[-0.0, 0.0],
        &[-9.0, -7.0],
    ];
    let original = groups
        .iter()
        .flat_map(|group| group.iter().copied())
        .collect::<Vec<_>>();
    let properties = WriterProperties::builder()
        .set_data_page_row_count_limit(2)
        .set_write_batch_size(2)
        .build();
    let mut writer = ArrowWriter::try_new(
        std::fs::File::create(&path).unwrap(),
        schema.clone(),
        Some(properties),
    )
    .unwrap();
    let mut next_id = 0;
    for group in groups {
        let end = next_id + i32::try_from(group.len()).unwrap();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(next_id..end)),
                Arc::new(Float32Array::from_iter_values(
                    group.iter().map(|value| *value as f32),
                )),
                Arc::new(Float64Array::from_iter_values(group.iter().copied())),
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.flush().unwrap();
        next_id = end;
    }
    writer.close().unwrap();
    let fixture = Fixture::from_path(directory, path);
    let mut baseline = open_file_reader(fixture.request(
        FileFormat::Parquet,
        FileProjection::All,
        1024,
        1024 * 1024,
    ))
    .unwrap();
    let baseline = collect(baseline.as_mut()).unwrap();
    let baseline_ids = baseline
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            ids.values().to_vec()
        })
        .collect::<Vec<_>>();
    assert_eq!(baseline_ids, (0..next_id).collect::<Vec<_>>());

    for (column, ordinal) in [("f32", 1), ("f64", 2)] {
        for op in [
            MinMaxPredicateOp::Eq,
            MinMaxPredicateOp::Ge,
            MinMaxPredicateOp::Lt,
        ] {
            // This oracle does not consult Parquet statistics or the production
            // pruning predicate. In particular, NaN compares false and -0 == +0.
            let matches = |value: f64| match op {
                MinMaxPredicateOp::Eq => value == 0.0,
                MinMaxPredicateOp::Ge => value >= 0.0,
                MinMaxPredicateOp::Lt => value < 0.0,
                _ => unreachable!(),
            };
            let expected = original
                .iter()
                .enumerate()
                .filter_map(|(id, value)| matches(*value).then_some(i32::try_from(id).unwrap()))
                .collect::<Vec<_>>();
            let selected_ids = |batches: &[novarocks_fs::FileBatch]| {
                let mut selected = Vec::new();
                for batch in batches {
                    let ids = batch
                        .batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    for row in 0..batch.batch.num_rows() {
                        let value = if ordinal == 1 {
                            f64::from(
                                batch
                                    .batch
                                    .column(ordinal)
                                    .as_any()
                                    .downcast_ref::<Float32Array>()
                                    .unwrap()
                                    .value(row),
                            )
                        } else {
                            batch
                                .batch
                                .column(ordinal)
                                .as_any()
                                .downcast_ref::<Float64Array>()
                                .unwrap()
                                .value(row)
                        };
                        if matches(value) {
                            selected.push(ids.value(row));
                        }
                    }
                }
                selected
            };
            assert_eq!(
                selected_ids(&baseline),
                expected,
                "unpruned oracle: {column} {op:?}"
            );
            for page_index in [false, true] {
                let mut request =
                    fixture.request(FileFormat::Parquet, FileProjection::All, 1024, 1024 * 1024);
                request.options.enable_parquet_reader_page_index = page_index;
                request.predicates.push(ScanPredicate::new(
                    column,
                    ScanPredicateDomain::Range {
                        op,
                        value: if ordinal == 1 {
                            MinMaxPredicateValue::Float(0.0)
                        } else {
                            MinMaxPredicateValue::Double(0.0)
                        },
                    },
                    ScanPredicateSource::Static,
                ));
                let mut reader = open_file_reader(request).unwrap();
                let pruned = collect(reader.as_mut()).unwrap();
                assert_eq!(
                    selected_ids(&pruned),
                    expected,
                    "pruning lost matching rows: {column} {op:?}, page_index={page_index}"
                );
                assert!(
                    reader.metrics_snapshot().row_groups_pruned > 0,
                    "fixture must exercise pruning: {column} {op:?}, page_index={page_index}"
                );
            }
        }
    }
}
