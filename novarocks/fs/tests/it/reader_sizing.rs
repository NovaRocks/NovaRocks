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

use novarocks_fs::{FileFormat, FileProjection, open_file_reader};

use crate::common::{Fixture, collect};

#[test]
fn parquet_decoder_enforces_row_limit() {
    let fixture = Fixture::parquet();
    let mut reader =
        open_file_reader(fixture.request(FileFormat::Parquet, FileProjection::All, 3, 1024 * 1024))
            .expect("open reader");
    assert_eq!(reader.reader_batch_rows(), Some(3));
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
fn parquet_fixed_width_projection_sizes_batches_without_copying_or_rejecting_rows() {
    let fixture = Fixture::parquet();
    // The id column is Int32, four bytes per row. Each group has four rows.
    for (bytes, batch_rows) in [(16, 4), (8, 2), (4, 1), (0, 1)] {
        let request = fixture.request(
            FileFormat::Parquet,
            FileProjection::RootIndices(vec![0]),
            4,
            bytes,
        );
        let mut reader = open_file_reader(request).expect("open reader");
        assert_eq!(reader.reader_batch_rows(), Some(batch_rows));
        let batches = collect(reader.as_mut()).expect("decode every row");
        assert_eq!(batches.len(), 8 / batch_rows);
        assert!(
            batches
                .iter()
                .all(|batch| batch.batch.num_rows() == batch_rows)
        );
    }
}

#[test]
fn parquet_zero_width_projection_uses_row_limit_even_with_zero_bytes() {
    let fixture = Fixture::parquet();
    let mut reader = open_file_reader(fixture.request(
        FileFormat::Parquet,
        FileProjection::RootIndices(Vec::new()),
        2,
        0,
    ))
    .expect("open zero-column reader");
    assert_eq!(reader.reader_batch_rows(), Some(2));
    let batches = collect(reader.as_mut()).expect("read positions");
    assert_eq!(batches.len(), 4);
    assert!(
        batches
            .iter()
            .all(|batch| { batch.batch.num_columns() == 0 && batch.batch.num_rows() == 2 })
    );
}

#[test]
fn parquet_no_selected_groups_needs_no_decoder() {
    let fixture = Fixture::parquet();
    let mut request = fixture.request(FileFormat::Parquet, FileProjection::All, 3, 0);
    request.pruning.row_groups = Some(Vec::new());
    let mut reader = open_file_reader(request).expect("open empty reader");
    assert_eq!(reader.reader_batch_rows(), None);
    assert!(reader.next_batch().expect("finished").is_none());
    assert_eq!(reader.metrics_snapshot().rows_decoded, 0);
    reader.close().expect("close empty reader");
}

#[test]
fn parquet_positions_stay_aligned_across_decoder_batches() {
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
