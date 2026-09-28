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

use std::ops::Range;

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};

use super::chunk_reader::BoundChunkReader;
use crate::{FileError, FileErrorKind, FileReaderOptions, FileResult};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CoalescedRange {
    pub(crate) range: Range<u64>,
    pub(crate) requests: Vec<usize>,
}

pub(crate) fn coalesce_ranges(
    ranges: &[Range<u64>],
    file_size: u64,
    options: FileReaderOptions,
) -> FileResult<Vec<CoalescedRange>> {
    let mut order = (0..ranges.len()).collect::<Vec<_>>();
    order.sort_by_key(|index| (ranges[*index].start, ranges[*index].end));
    let mut groups: Vec<CoalescedRange> = Vec::new();
    for index in order {
        let range = &ranges[index];
        if range.start >= range.end || range.end > file_size {
            return Err(FileError::new(
                FileErrorKind::Corrupt,
                format!(
                    "Parquet input range [{}, {}) is invalid for file length {file_size}",
                    range.start, range.end
                ),
            ));
        }
        if options.coalesce_reads
            && let Some(last) = groups.last_mut()
            && range.start <= last.range.end.saturating_add(options.coalesce_max_gap)
            && (range.end.max(last.range.end) - last.range.start <= options.coalesce_max_bytes
                || (range.start >= last.range.start && range.end <= last.range.end))
        {
            last.range.end = last.range.end.max(range.end);
            last.requests.push(index);
            continue;
        }
        groups.push(CoalescedRange {
            range: range.clone(),
            requests: vec![index],
        });
    }
    Ok(groups)
}

pub(crate) fn read_decoder_ranges(
    reader: &BoundChunkReader,
    ranges: &[Range<u64>],
    options: FileReaderOptions,
) -> FileResult<Vec<Bytes>> {
    let groups = coalesce_ranges(ranges, reader.file_size(), options)?;
    let mut output = vec![None; ranges.len()];
    for group in groups {
        let length = group_length(&group)?;
        let bytes = if is_exact(&group, ranges) {
            reader.read_bytes(group.range.start, length)?
        } else {
            reader.read_backing_bytes(group.range.start, length)?
        };
        fill_group(&mut output, ranges, &group, &bytes)?;
    }
    filled(output)
}

/// Awaited [`read_decoder_ranges`]. The independent merged groups of one
/// decoder request are fetched concurrently -- at most the source's window at
/// a time, each still admitted to the shared range service -- and handed back
/// in the decoder's original range order, whatever order they complete in.
/// The first error drops the groups still in flight, which stops their
/// requests; their exit stays observed by the operations they were admitted
/// to.
pub(crate) async fn read_decoder_ranges_async(
    reader: &BoundChunkReader,
    ranges: &[Range<u64>],
    options: FileReaderOptions,
) -> FileResult<Vec<Bytes>> {
    let groups = coalesce_ranges(ranges, reader.file_size(), options)?;
    let fetched: Vec<(CoalescedRange, Bytes)> =
        stream::iter(groups.into_iter().map(|group| async move {
            let length = group_length(&group)?;
            let bytes = if is_exact(&group, ranges) {
                reader.read_bytes_async(group.range.start, length).await?
            } else {
                reader
                    .read_backing_bytes_async(group.range.start, length)
                    .await?
            };
            Ok::<_, FileError>((group, bytes))
        }))
        .buffered(reader.range_concurrency())
        .try_collect()
        .await?;
    let mut output = vec![None; ranges.len()];
    for (group, bytes) in &fetched {
        fill_group(&mut output, ranges, group, bytes)?;
    }
    filled(output)
}

fn group_length(group: &CoalescedRange) -> FileResult<usize> {
    usize::try_from(group.range.end - group.range.start).map_err(|_| {
        FileError::new(
            FileErrorKind::ResourceExhausted,
            "Parquet input range is too large",
        )
    })
}

/// A group serving exactly one decoder range may fill the exact-range cache;
/// a merged backing must not.
fn is_exact(group: &CoalescedRange, ranges: &[Range<u64>]) -> bool {
    group.requests.len() == 1 && ranges[group.requests[0]] == group.range
}

fn fill_group(
    output: &mut [Option<Bytes>],
    ranges: &[Range<u64>],
    group: &CoalescedRange,
    bytes: &Bytes,
) -> FileResult<()> {
    for index in &group.requests {
        let range = &ranges[*index];
        let start = usize::try_from(range.start - group.range.start).map_err(|_| {
            FileError::new(
                FileErrorKind::ResourceExhausted,
                "Parquet input slice is too large",
            )
        })?;
        let end = usize::try_from(range.end - group.range.start).map_err(|_| {
            FileError::new(
                FileErrorKind::ResourceExhausted,
                "Parquet input slice is too large",
            )
        })?;
        output[*index] = Some(bytes.slice(start..end));
    }
    Ok(())
}

fn filled(output: Vec<Option<Bytes>>) -> FileResult<Vec<Bytes>> {
    output
        .into_iter()
        .map(|bytes| {
            bytes.ok_or_else(|| {
                FileError::new(
                    FileErrorKind::Internal,
                    "Parquet input range was not filled",
                )
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        FileCancellation, FileIdentity, FileIoRuntime, FileRangeScope, FileRangeService,
        FileReadContext, FileTaskSpawner, FsAccessResolver, PreparedFileInput, TokioFileIoRuntime,
        TokioFileTaskSpawner,
    };
    use bytes::BytesMut;
    use novarocks_spi::connector::StorageAccessDomainId;
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use super::super::chunk_reader::ReaderMetrics;

    #[test]
    fn merges_nearby_ranges_within_limit_and_preserves_membership() {
        let ranges = vec![40..50, 0..10, 12..20, 100..110];
        let groups = coalesce_ranges(
            &ranges,
            110,
            FileReaderOptions {
                coalesce_max_bytes: 32,
                coalesce_max_gap: 4,
                ..Default::default()
            },
        )
        .expect("coalesce valid ranges");
        assert_eq!(groups.len(), 3);
        assert_eq!(groups[0].range, 0..20);
        assert_eq!(groups[0].requests, vec![1, 2]);
        assert_eq!(groups[1].range, 40..50);
        assert_eq!(groups[2].range, 100..110);
    }

    #[test]
    fn disabled_coalescing_keeps_exact_ranges() {
        let ranges = vec![0..10, 10..20];
        let groups = coalesce_ranges(
            &ranges,
            20,
            FileReaderOptions {
                coalesce_reads: false,
                ..Default::default()
            },
        )
        .expect("valid ranges");
        assert_eq!(groups.len(), 2);
    }

    #[test]
    fn contained_request_reuses_a_large_existing_backing() {
        let ranges = vec![0..32, 8..16, 0..32];
        let groups = coalesce_ranges(
            &ranges,
            32,
            FileReaderOptions {
                coalesce_max_bytes: 16,
                coalesce_max_gap: 0,
                ..Default::default()
            },
        )
        .expect("contained input ranges");
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].range, 0..32);
        assert_eq!(groups[0].requests, vec![0, 2, 1]);
    }

    #[test]
    fn duplicate_overlap_and_out_of_order_requests_keep_exact_membership() {
        let ranges = vec![14..20, 0..10, 5..15, 0..10];
        let groups = coalesce_ranges(&ranges, 20, FileReaderOptions::default())
            .expect("valid overlapping ranges");
        assert_eq!(
            groups,
            vec![CoalescedRange {
                range: 0..20,
                requests: vec![1, 3, 2, 0],
            }]
        );
    }

    #[test]
    fn merged_backing_returns_exact_bytes_in_original_request_order() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("ranges.bin");
        std::fs::write(&path, b"abcdefghijklmnopqrst").expect("write fixture");
        let access = FsAccessResolver::new()
            .resolve_location(
                StorageAccessDomainId::from_bytes([17; 32]),
                path.to_string_lossy(),
                None,
            )
            .expect("local access");
        let file = access
            .bind(0, FileIdentity::new(path.to_string_lossy(), 20, None))
            .expect("bound fixture");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("test runtime");
        let handle = runtime.handle().clone();
        let context = FileReadContext {
            cancellation: FileCancellation::new(),
            deadline: None,
            runtime: Arc::new(TokioFileIoRuntime::new(handle.clone())) as Arc<dyn FileIoRuntime>,
            task_spawner: Arc::new(TokioFileTaskSpawner::new(handle)) as Arc<dyn FileTaskSpawner>,
            range: None,
        };
        let reader = BoundChunkReader::new(
            file,
            context,
            None,
            false,
            Arc::new(ReaderMetrics::default()),
        );
        let ranges = vec![14..20, 0..10, 5..15, 0..10];
        let bytes = read_decoder_ranges(&reader, &ranges, FileReaderOptions::default())
            .expect("read merged input");
        assert_eq!(
            bytes.iter().map(Bytes::as_ref).collect::<Vec<_>>(),
            vec![
                &b"opqrst"[..],
                &b"abcdefghij"[..],
                &b"fghijklmno"[..],
                &b"abcdefghij"[..]
            ]
        );
    }

    #[test]
    // The inverted range is the input under test, not an iteration.
    #[allow(clippy::reversed_empty_ranges)]
    fn rejects_empty_inverted_and_out_of_file_ranges() {
        for range in [0..0, 10..9, 9..11, u64::MAX - 1..u64::MAX] {
            assert_eq!(
                coalesce_ranges(&[range], 10, FileReaderOptions::default())
                    .expect_err("invalid range")
                    .kind(),
                FileErrorKind::Corrupt
            );
        }
    }

    #[test]
    fn prepared_full_hit_needs_no_file_read_and_preserves_backing_capacity() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("prepared.bin");
        std::fs::write(&path, b"abcdefghijkl").expect("fixture");
        let access = FsAccessResolver::new()
            .resolve_location(
                StorageAccessDomainId::from_bytes([17; 32]),
                path.to_string_lossy(),
                None,
            )
            .expect("access");
        let file = access
            .bind(0, FileIdentity::new(path.to_string_lossy(), 12, None))
            .expect("bound file");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let handle = runtime.handle().clone();
        let spawner = Arc::new(TokioFileTaskSpawner::new(handle.clone()));
        let service = FileRangeService::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            spawner.clone(),
            handle.clone(),
        );
        let mut backing = BytesMut::with_capacity(64);
        backing.extend_from_slice(b"abcdefghijkl");
        let prepared = PreparedFileInput::new(&file, 0, backing).expect("prepared input");
        assert_eq!(prepared.retained_backing_capacity(), 64);
        let metrics = Arc::new(ReaderMetrics::default());
        let reader = BoundChunkReader::new(
            file,
            FileReadContext {
                cancellation: FileCancellation::new(),
                deadline: None,
                runtime: Arc::new(TokioFileIoRuntime::new(handle.clone())),
                task_spawner: spawner,
                range: Some(service.bind(
                    FileRangeScope::try_new(1, 0, 1, 1, 0, 1).unwrap(),
                    novarocks_spi::connector::read_stack::ConnectorSourceOperations::new(),
                )),
            },
            None,
            false,
            Arc::clone(&metrics),
        )
        .with_prepared_input(Some(prepared))
        .expect("same file");
        std::fs::remove_file(path).expect("remove source to prove no GET");
        assert_eq!(
            reader.read_bytes(3, 5).expect("covered bytes"),
            b"defgh"[..]
        );
        assert_eq!(metrics.snapshot().read_requests, 0);
        assert_eq!(metrics.snapshot().bytes_read, 0);
    }

    #[test]
    fn prepared_partial_hit_fills_only_missing_spans() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("prepared.bin");
        std::fs::write(&path, b"abcdefghijkl").expect("fixture");
        let access = FsAccessResolver::new()
            .resolve_location(
                StorageAccessDomainId::from_bytes([17; 32]),
                path.to_string_lossy(),
                None,
            )
            .expect("access");
        let file = access
            .bind(0, FileIdentity::new(path.to_string_lossy(), 12, None))
            .expect("bound file");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let handle = runtime.handle().clone();
        let spawner = Arc::new(TokioFileTaskSpawner::new(handle.clone()));
        let service = FileRangeService::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            spawner.clone(),
            handle.clone(),
        );
        let metrics = Arc::new(ReaderMetrics::default());
        let reader = BoundChunkReader::new(
            file.clone(),
            FileReadContext {
                cancellation: FileCancellation::new(),
                deadline: None,
                runtime: Arc::new(TokioFileIoRuntime::new(handle.clone())),
                task_spawner: spawner,
                range: Some(service.bind(
                    FileRangeScope::try_new(1, 0, 1, 1, 0, 1).unwrap(),
                    novarocks_spi::connector::read_stack::ConnectorSourceOperations::new(),
                )),
            },
            None,
            false,
            Arc::clone(&metrics),
        )
        .with_prepared_input(Some(
            PreparedFileInput::new(&file, 4, BytesMut::from(&b"efgh"[..])).expect("prepared input"),
        ))
        .expect("same file");
        assert_eq!(
            reader.read_bytes(0, 12).expect("filled input"),
            b"abcdefghijkl"[..]
        );
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.bytes_read, 8);
        assert_eq!(snapshot.partial_prefetch_copy_bytes, 4);
    }
}
