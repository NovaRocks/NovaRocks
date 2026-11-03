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

use arrow::array::Int32Array;
use bytes::BytesMut;
use novarocks_fs::{
    CacheOptions, DataCacheContext, FileErrorKind, FileFormat, FileIdentity, FileProjection,
    FileReadRange, MinMaxPredicateOp, MinMaxPredicateValue, PreparedFileInput, ScanPredicate,
    ScanPredicateDomain, ScanPredicateSource, inspect_parquet_metadata,
    inspect_parquet_metadata_from_prepared, open_file_reader_with_parquet_inspection,
    parquet_footer_range, plan_parquet_input_ranges,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use common::{Fixture, collect};

#[test]
fn prepared_footer_stages_exact_suffix_and_parses_without_second_get() {
    let fixture = Fixture::parquet();
    let bytes = std::fs::read(fixture.file.location().path()).expect("fixture bytes");
    let size = bytes.len();
    let tail = PreparedFileInput::new(
        &fixture.file,
        (size - 8) as u64,
        BytesMut::from(&bytes[size - 8..]),
    )
    .expect("prepared tail");
    let range = parquet_footer_range(&fixture.file, &tail).expect("footer range");
    let FileReadRange::Bounded { offset, length } = range else {
        panic!("footer is bounded")
    };
    assert_eq!(offset + length, size as u64);
    assert!(offset < (size - 8) as u64);
    let complete = PreparedFileInput::new(
        &fixture.file,
        offset,
        BytesMut::from(&bytes[offset as usize..]),
    )
    .expect("complete footer");
    let request = fixture.request(FileFormat::Parquet, FileProjection::All, 4, 1024 * 1024);
    let before = fixture.io.block_on_bytes_calls();
    std::fs::remove_file(fixture.file.location().path()).expect("remove source");
    let inspection = inspect_parquet_metadata_from_prepared(
        fixture.file.clone(),
        complete,
        request.context,
        novarocks_fs::ParquetInterpretationPolicy::UseArrowHint,
    )
    .expect("parse prepared footer");
    assert_eq!(inspection.row_groups().len(), 2);
    assert_eq!(fixture.io.block_on_bytes_calls(), before);
}

#[test]
fn prepared_footer_rejects_incomplete_or_wrong_identity() {
    let fixture = Fixture::parquet();
    let bytes = std::fs::read(fixture.file.location().path()).expect("fixture bytes");
    let size = bytes.len();
    let tail = PreparedFileInput::new(
        &fixture.file,
        (size - 8) as u64,
        BytesMut::from(&bytes[size - 8..]),
    )
    .expect("prepared tail");
    let request = fixture.request(FileFormat::Parquet, FileProjection::All, 4, 1024 * 1024);
    assert_eq!(
        inspect_parquet_metadata_from_prepared(
            fixture.file.clone(),
            tail.clone(),
            request.context,
            novarocks_fs::ParquetInterpretationPolicy::UseArrowHint
        )
        .expect_err("only trailer cannot parse complete metadata")
        .kind(),
        FileErrorKind::Invalid
    );
    let wrong = fixture
        .file
        .access()
        .bind(
            0,
            FileIdentity::new(fixture.file.identity().path(), size as u64, Some(8)),
        )
        .expect("alternate identity");
    assert_eq!(
        parquet_footer_range(&wrong, &tail)
            .expect_err("wrong identity")
            .kind(),
        FileErrorKind::Invalid
    );
}

#[test]
fn inspection_reuses_footer_across_row_group_runs_and_preserves_positions() {
    let fixture = Fixture::parquet();
    let request = fixture.request(FileFormat::Parquet, FileProjection::All, 2, 1024 * 1024);
    let inspection = inspect_parquet_metadata(
        fixture.file.clone(),
        None,
        request.context.clone(),
        novarocks_fs::ParquetInterpretationPolicy::UseArrowHint,
    )
    .expect("inspect file once");

    for (group, expected) in [(0, vec![0, 1, 2, 3]), (1, vec![4, 5, 6, 7])] {
        let mut request = fixture.request(FileFormat::Parquet, FileProjection::All, 2, 1024 * 1024);
        request.pruning.row_groups = Some(vec![group]);
        let mut reader = open_file_reader_with_parquet_inspection(request, Some(&inspection))
            .expect("open run with inspected footer");
        assert_eq!(
            reader.metrics_snapshot().read_requests,
            0,
            "opening a later run must not refetch its footer"
        );
        let positions = collect(reader.as_mut())
            .expect("decode run")
            .iter()
            .flat_map(|batch| {
                batch
                    .physical_row_positions
                    .as_ref()
                    .expect("physical positions")
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>();
        assert_eq!(positions, expected);
    }
}

#[test]
fn small_file_probe_serves_footer_and_later_runs_from_one_file_read() {
    let fixture = Fixture::parquet();
    assert!(fixture.file.identity().file_size() <= 64 * 1024);
    let request = fixture.request(FileFormat::Parquet, FileProjection::All, 4, 1024 * 1024);
    let before = fixture.io.block_on_bytes_calls();
    let inspection = inspect_parquet_metadata(
        fixture.file.clone(),
        None,
        request.context,
        novarocks_fs::ParquetInterpretationPolicy::UseArrowHint,
    )
    .expect("inspect small file");
    assert_eq!(fixture.io.block_on_bytes_calls() - before, 1);

    for group in [0, 1] {
        let mut request = fixture.request(FileFormat::Parquet, FileProjection::All, 4, 1024 * 1024);
        request.pruning.row_groups = Some(vec![group]);
        let mut reader = open_file_reader_with_parquet_inspection(request, Some(&inspection))
            .expect("open run from one small-file probe");
        collect(reader.as_mut()).expect("decode run from retained bytes");
    }
    assert_eq!(
        fixture.io.block_on_bytes_calls() - before,
        1,
        "later runs must share the original whole-file probe"
    );
}

#[test]
fn inspection_rejects_different_file_identity() {
    let fixture = Fixture::parquet();
    let request = fixture.request(FileFormat::Parquet, FileProjection::All, 4, 1024 * 1024);
    let inspection = inspect_parquet_metadata(
        fixture.file.clone(),
        None,
        request.context.clone(),
        novarocks_fs::ParquetInterpretationPolicy::UseArrowHint,
    )
    .expect("inspect file");
    let mut request = fixture.request(FileFormat::Parquet, FileProjection::All, 4, 1024 * 1024);
    let identity = fixture.file.identity();
    request.file = fixture
        .file
        .access()
        .bind(
            0,
            FileIdentity::new(identity.path(), identity.file_size(), Some(8)),
        )
        .expect("bind altered identity");
    assert_eq!(
        plan_parquet_input_ranges(&request, &inspection)
            .expect_err("mismatched planning identity must be rejected")
            .kind(),
        FileErrorKind::Invalid
    );
    let error = open_file_reader_with_parquet_inspection(request, Some(&inspection))
        .err()
        .expect("mismatched file identity must be rejected");
    assert_eq!(error.kind(), FileErrorKind::Invalid);
}

#[test]
fn inspection_upgrades_page_index_once_for_later_runs() {
    let fixture = Fixture::parquet();
    let request = fixture.request(FileFormat::Parquet, FileProjection::All, 4, 1024 * 1024);
    let cache = DataCacheContext::external(CacheOptions {
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
    let inspection = inspect_parquet_metadata(
        fixture.file.clone(),
        Some(cache.clone()),
        request.context.clone(),
        novarocks_fs::ParquetInterpretationPolicy::UseArrowHint,
    )
    .expect("inspect footer without page indexes");

    for (run, expected_index_requests) in [(0, true), (1, false)] {
        let mut request = fixture.request(FileFormat::Parquet, FileProjection::All, 4, 1024 * 1024);
        request.cache = Some(cache.clone());
        request.pruning.row_groups = Some(vec![run]);
        request.options.enable_parquet_reader_page_index = true;
        request.predicates.push(ScanPredicate::new(
            "id",
            ScanPredicateDomain::Range {
                op: MinMaxPredicateOp::Ge,
                value: MinMaxPredicateValue::Int32(2),
            },
            ScanPredicateSource::Static,
        ));
        let mut reader = open_file_reader_with_parquet_inspection(request, Some(&inspection))
            .expect("open indexed run");
        assert_eq!(
            reader.metrics_snapshot().read_requests > 0,
            expected_index_requests,
            "page indexes must be fetched only for the first indexed run"
        );
        collect(reader.as_mut()).expect("decode indexed run");
    }
}

#[test]
fn metadata_only_plan_uses_projected_column_range_without_decoder() {
    let fixture = Fixture::parquet();
    let mut request = fixture.request(
        FileFormat::Parquet,
        FileProjection::RootIndices(vec![0]),
        4,
        1024 * 1024,
    );
    request.pruning.row_groups = Some(vec![0]);
    request.options.coalesce_reads = false;
    let inspection = inspect_parquet_metadata(
        fixture.file.clone(),
        None,
        request.context.clone(),
        novarocks_fs::ParquetInterpretationPolicy::UseArrowHint,
    )
    .expect("inspect footer");
    let file = std::fs::File::open(fixture.file.location().path()).expect("fixture file");
    let footer = ParquetRecordBatchReaderBuilder::try_new(file).expect("fixture metadata");
    let (start, length) = footer.metadata().row_group(0).column(0).byte_range();

    assert_eq!(
        plan_parquet_input_ranges(&request, &inspection).expect("plan projected range"),
        vec![FileReadRange::Bounded {
            offset: start,
            length,
        }]
    );
}

#[test]
fn metadata_only_plan_uses_page_index_for_selected_pages() {
    let fixture = Fixture::parquet();
    let mut request = fixture.request(
        FileFormat::Parquet,
        FileProjection::RootIndices(vec![0]),
        4,
        1024 * 1024,
    );
    request.pruning.row_groups = Some(vec![0]);
    request
        .pruning
        .pages
        .push(novarocks_fs::PhysicalPageSelection {
            row_group: 0,
            page_indices: vec![1],
        });
    request.options.coalesce_reads = false;
    let inspection = inspect_parquet_metadata(
        fixture.file.clone(),
        None,
        request.context.clone(),
        novarocks_fs::ParquetInterpretationPolicy::UseArrowHint,
    )
    .expect("inspect footer");
    let planned = plan_parquet_input_ranges(&request, &inspection).expect("plan selected page");
    let file = std::fs::File::open(fixture.file.location().path()).expect("fixture file");
    let footer = ParquetRecordBatchReaderBuilder::try_new(file).expect("fixture metadata");
    let (_, full_column_bytes) = footer.metadata().row_group(0).column(0).byte_range();
    let planned_bytes = planned
        .iter()
        .map(|range| match range {
            FileReadRange::Bounded { length, .. } => *length,
            FileReadRange::WholeFile => panic!("planner must not return whole file"),
        })
        .sum::<u64>();
    assert!(planned_bytes > 0);
    assert!(planned_bytes < full_column_bytes);
}

#[test]
fn push_decoder_preserves_values_and_absolute_positions_across_disjoint_groups() {
    let fixture = Fixture::parquet_three_groups();
    let mut request = fixture.request(FileFormat::Parquet, FileProjection::All, 1, 1024 * 1024);
    request.pruning.row_groups = Some(vec![0, 2]);
    for row_group in [0, 2] {
        request
            .pruning
            .pages
            .push(novarocks_fs::PhysicalPageSelection {
                row_group,
                page_indices: vec![1],
            });
    }
    let inspection = inspect_parquet_metadata(
        fixture.file.clone(),
        None,
        request.context.clone(),
        novarocks_fs::ParquetInterpretationPolicy::UseArrowHint,
    )
    .expect("inspect three-group file");
    let mut reader = open_file_reader_with_parquet_inspection(request, Some(&inspection))
        .expect("open disjoint row groups");
    let batches = collect(reader.as_mut()).expect("decode selected pages");
    let positions = batches
        .iter()
        .flat_map(|batch| {
            batch
                .physical_row_positions
                .as_ref()
                .expect("physical positions")
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    let values = batches
        .iter()
        .flat_map(|batch| {
            batch
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("id column")
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    assert_eq!(positions, vec![2, 3, 10, 11]);
    assert_eq!(values, vec![2, 3, 10, 11]);
    assert!(batches.iter().all(|batch| batch.batch.num_rows() == 1));
}
