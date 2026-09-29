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

//! Physical Iceberg position-delete application for provider batch readers.

use arrow::array::{Array, Int64Array, StringArray};
use novarocks_fs::{
    FileProjection, FileReadContext, FileReadRange, FsAccessHandle, MinMaxPredicateOp,
    MinMaxPredicateValue, ScanPredicate, ScanPredicateDomain, ScanPredicateSource,
};
use roaring::RoaringTreemap;

use crate::commit::DeletionVector;
use crate::delete_file::{IcebergDeleteFileSpec, IcebergFileContent, IcebergFileFormat};

pub const FILE_PATH_COLUMN: &str = "file_path";
pub const POS_COLUMN: &str = "pos";

/// Loads the delete positions attached to one provider-frozen data file using
/// the exact cancellation, deadline, and runtime resources of its reader.
pub fn load_position_deletes_with_context(
    specs: &[IcebergDeleteFileSpec],
    data_file_path: &str,
    access: &FsAccessHandle,
    context: &FileReadContext,
) -> Result<RoaringTreemap, String> {
    let mut deleted = RoaringTreemap::new();
    for spec in specs {
        if spec.file_content != IcebergFileContent::PositionDeletes {
            continue;
        }
        match plan_position_delete(spec, data_file_path)? {
            PositionDeleteRead::DeletionVector(range) => {
                let payload = crate::file_reader::read_bytes(
                    access,
                    &spec.path,
                    spec.length,
                    range,
                    context,
                )?;
                apply_deletion_vector(spec, &payload, &mut deleted)?;
            }
            PositionDeleteRead::Parquet => {
                crate::file_reader::visit_parquet_batches(
                    access,
                    &spec.path,
                    spec.length,
                    position_delete_projection(),
                    position_delete_predicates(data_file_path),
                    context.clone(),
                    |batch| apply_position_delete_batch(spec, data_file_path, batch, &mut deleted),
                )?;
            }
        }
    }
    Ok(deleted)
}

/// Awaited [`load_position_deletes_with_context`]: the same reads, awaited
/// through the source's range service.
pub async fn load_position_deletes_async(
    specs: &[IcebergDeleteFileSpec],
    data_file_path: &str,
    access: &FsAccessHandle,
    context: &FileReadContext,
) -> Result<RoaringTreemap, String> {
    load_position_deletes_async_with_metrics(specs, data_file_path, access, context, |_| {})
        .await
        .map(|(positions, _)| positions)
        .map_err(|error| error.to_string())
}

/// Physical decoded rows, including rows later rejected by the exact path check.
pub(crate) async fn load_position_deletes_async_with_metrics(
    specs: &[IcebergDeleteFileSpec],
    data_file_path: &str,
    access: &FsAccessHandle,
    context: &FileReadContext,
    mut on_decoded: impl FnMut(usize) + Send,
) -> Result<(RoaringTreemap, usize), novarocks_spi::connector::ConnectorError> {
    let mut deleted = RoaringTreemap::new();
    let mut decoded_rows = 0;
    for spec in specs {
        if spec.file_content != IcebergFileContent::PositionDeletes {
            continue;
        }
        match plan_position_delete(spec, data_file_path)
            .map_err(crate::file_reader::corrupt_delete_content)?
        {
            PositionDeleteRead::DeletionVector(range) => {
                let payload = crate::file_reader::read_bytes_async_typed(
                    access,
                    &spec.path,
                    spec.length,
                    range,
                    context,
                )
                .await?;
                let (positions, cardinality) = decode_deletion_vector_into_async(
                    &payload,
                    context,
                    std::mem::take(&mut deleted),
                    |_, _, _| {},
                )
                .await?;
                let rows = usize::try_from(cardinality).map_err(|_| {
                    novarocks_spi::connector::ConnectorError::new(
                        novarocks_spi::connector::ConnectorErrorKind::ResourceExhausted,
                        "DV cardinality exceeds addressable row count",
                    )
                })?;
                decoded_rows += rows;
                on_decoded(rows);
                deleted = positions;
            }
            PositionDeleteRead::Parquet => {
                crate::file_reader::visit_parquet_batches_async(
                    access,
                    &spec.path,
                    spec.length,
                    position_delete_projection(),
                    position_delete_predicates(data_file_path),
                    context.clone(),
                    |batch| {
                        decoded_rows += batch.batch.num_rows();
                        on_decoded(batch.batch.num_rows());
                        apply_position_delete_batch(spec, data_file_path, batch, &mut deleted)
                            .map_err(crate::file_reader::corrupt_delete_content)
                    },
                )
                .await?;
            }
        }
    }
    Ok((deleted, decoded_rows))
}

/// What one position-delete file needs read before it can be applied.
enum PositionDeleteRead {
    /// A deletion vector: one byte range of its Puffin container.
    DeletionVector(FileReadRange),
    /// Projected path/position ranges after conservative row-group pruning.
    Parquet,
}

fn plan_position_delete(
    spec: &IcebergDeleteFileSpec,
    data_file_path: &str,
) -> Result<PositionDeleteRead, String> {
    if let Some(referenced_data_file) = spec.referenced_data_file.as_deref()
        && referenced_data_file != data_file_path
    {
        return Err(format!(
            "iceberg position-delete file {} belongs to data file {referenced_data_file}, not {data_file_path}",
            spec.path
        ));
    }
    if spec.content_offset.is_some() || spec.content_size_in_bytes.is_some() {
        let offset = spec.content_offset.ok_or_else(|| {
            format!(
                "Puffin deletion vector {} missing content_offset",
                spec.path
            )
        })?;
        let size = spec.content_size_in_bytes.ok_or_else(|| {
            format!(
                "Puffin deletion vector {} missing content_size_in_bytes",
                spec.path
            )
        })?;
        let start = u64::try_from(offset)
            .map_err(|_| format!("Puffin deletion vector {} has negative offset", spec.path))?;
        let length = u64::try_from(size)
            .map_err(|_| format!("Puffin deletion vector {} size is too large", spec.path))?;
        return FileReadRange::bounded(start, length)
            .map(PositionDeleteRead::DeletionVector)
            .map_err(|error| error.to_string());
    }
    if spec.file_format != IcebergFileFormat::Parquet {
        return Err(format!(
            "iceberg position-delete file {} has unsupported format {:?}; only PARQUET is supported",
            spec.path, spec.file_format
        ));
    }
    Ok(PositionDeleteRead::Parquet)
}

fn position_delete_predicates(target: &str) -> Vec<ScanPredicate> {
    vec![
        ScanPredicate::new(
            FILE_PATH_COLUMN,
            ScanPredicateDomain::Range {
                op: MinMaxPredicateOp::Eq,
                value: MinMaxPredicateValue::ByteArray(target.as_bytes().to_vec()),
            },
            ScanPredicateSource::Static,
        )
        .with_physical_field_id(crate::delete_semantics::POSITION_FILE_PATH_FIELD_ID),
    ]
}

fn position_delete_projection() -> FileProjection {
    FileProjection::RootNames(vec![FILE_PATH_COLUMN.to_string(), POS_COLUMN.to_string()])
}

fn apply_deletion_vector(
    spec: &IcebergDeleteFileSpec,
    payload: &[u8],
    deleted: &mut RoaringTreemap,
) -> Result<(), String> {
    let dv = DeletionVector::from_iceberg_payload(payload).map_err(|error| {
        format!(
            "decode Puffin deletion vector {} failed: {error}",
            spec.path
        )
    })?;
    *deleted |= dv.to_roaring_treemap();
    Ok(())
}

/// CRC byte chunks and container chunks bound cooperative work independently
/// of logical DV cardinality. The same owner checks stop after each yield.
async fn decode_deletion_vector_into_async(
    payload: &[u8],
    context: &FileReadContext,
    seed: RoaringTreemap,
    mut progress: impl FnMut(usize, u64, usize) + Send,
) -> Result<(RoaringTreemap, u64), novarocks_spi::connector::ConnectorError> {
    use crate::commit::puffin_dv::DeletionVectorPayloadReader;
    use crate::file_reader::{corrupt_delete_content, map_file_error};
    context.check_active().map_err(map_file_error)?;
    let mut reader = DeletionVectorPayloadReader::new(payload)
        .map_err(|e| corrupt_delete_content(e.to_string()))?;
    let mut crc = crc32fast::Hasher::new();
    for chunk in reader.crc_bytes().chunks(64 * 1024) {
        context.check_active().map_err(map_file_error)?;
        crc.update(chunk);
        tokio::task::yield_now().await;
    }
    context.check_active().map_err(map_file_error)?;
    reader
        .validate_crc(crc.finalize())
        .map_err(|e| corrupt_delete_content(e.to_string()))?;
    let mut out = seed;
    let mut cardinality = 0u64;
    while let Some((high, mut physical)) = reader
        .begin_bitmap()
        .map_err(|e| corrupt_delete_content(e.to_string()))?
    {
        let mut bitmap = roaring::RoaringBitmap::new();
        let mut chunk_count = 0usize;
        let mut chunk_rows = 0u64;
        let mut chunk_bytes = 0usize;
        while let Some((container, bytes)) = physical
            .next_container()
            .map_err(|e| corrupt_delete_content(e.to_string()))?
        {
            chunk_count += 1;
            chunk_rows += container.len();
            cardinality = cardinality
                .checked_add(container.len())
                .ok_or_else(|| corrupt_delete_content("DV cardinality overflows"))?;
            chunk_bytes += bytes;
            // Borrowed |= avoids the owned operator's full-prefix len() scan.
            // Container keys are strictly ascending, so insertion appends.
            bitmap |= &container;
            if chunk_count == 32 || chunk_bytes >= 64 * 1024 {
                let part = RoaringTreemap::from_bitmaps([(high, std::mem::take(&mut bitmap))]);
                // Treemap borrowed |= also avoids scanning the entire prefix.
                out |= &part;
                progress(chunk_count, chunk_rows, chunk_bytes);
                chunk_count = 0;
                chunk_rows = 0;
                chunk_bytes = 0;
                tokio::task::yield_now().await;
                context.check_active().map_err(map_file_error)?;
            }
        }
        if chunk_count != 0 {
            let part = RoaringTreemap::from_bitmaps([(high, bitmap)]);
            out |= &part;
            progress(chunk_count, chunk_rows, chunk_bytes);
        }
        reader
            .finish_bitmap(physical)
            .map_err(|e| corrupt_delete_content(e.to_string()))?;
        tokio::task::yield_now().await;
        context.check_active().map_err(map_file_error)?;
    }
    Ok((out, cardinality))
}

#[cfg(test)]
async fn decode_deletion_vector_async(
    payload: &[u8],
    context: &FileReadContext,
    progress: impl FnMut(usize, u64, usize) + Send,
) -> Result<(RoaringTreemap, u64), novarocks_spi::connector::ConnectorError> {
    decode_deletion_vector_into_async(payload, context, RoaringTreemap::new(), progress).await
}

fn apply_position_delete_batch(
    spec: &IcebergDeleteFileSpec,
    data_file_path: &str,
    batch: novarocks_fs::FileBatch,
    deleted: &mut RoaringTreemap,
) -> Result<(), String> {
    {
        let batch = batch.batch;
        let schema = batch.schema();
        let file_path_index = schema.index_of(FILE_PATH_COLUMN).map_err(|error| {
            format!(
                "projected batch from {} missing `{FILE_PATH_COLUMN}`: {error}",
                spec.path
            )
        })?;
        let pos_index = schema.index_of(POS_COLUMN).map_err(|error| {
            format!(
                "projected batch from {} missing `{POS_COLUMN}`: {error}",
                spec.path
            )
        })?;
        let file_paths = batch
            .column(file_path_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                format!(
                    "iceberg position-delete file {} column `{FILE_PATH_COLUMN}` is not STRING",
                    spec.path
                )
            })?;
        let positions = batch
            .column(pos_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                format!(
                    "iceberg position-delete file {} column `{POS_COLUMN}` is not BIGINT",
                    spec.path
                )
            })?;
        for row in 0..batch.num_rows() {
            if file_paths.is_null(row)
                || positions.is_null(row)
                || file_paths.value(row) != data_file_path
            {
                continue;
            }
            let position = positions.value(row);
            if position < 0 {
                return Err(format!(
                    "iceberg position-delete file {} has negative pos {} for data file {data_file_path}",
                    spec.path, position
                ));
            }
            deleted.insert(position as u64);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_fs::{
        FileCancellation, FileIoRuntime, FileTaskSpawner, FsAccessResolver, TokioFileIoRuntime,
        TokioFileTaskSpawner,
    };
    use parquet::arrow::ArrowWriter;

    use super::*;
    use crate::access_binding::IcebergReadBinding;

    fn dv_context(runtime: &tokio::runtime::Runtime) -> FileReadContext {
        FileReadContext {
            cancellation: FileCancellation::new(),
            deadline: None,
            runtime: Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
            task_spawner: Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
            range: None,
        }
    }
    fn dv_payload(bitmaps: &[(u32, Vec<u8>)]) -> Vec<u8> {
        let mut body = vec![0xD1, 0xD3, 0x39, 0x64];
        body.extend_from_slice(&(bitmaps.len() as u64).to_le_bytes());
        for (key, bitmap) in bitmaps {
            body.extend_from_slice(&key.to_le_bytes());
            body.extend_from_slice(bitmap);
        }
        let mut payload = (body.len() as u32).to_be_bytes().to_vec();
        payload.extend_from_slice(&body);
        payload.extend_from_slice(&crc32fast::hash(&body).to_be_bytes());
        payload
    }
    fn full_run_bitmap(containers: usize) -> Vec<u8> {
        assert!((1..=65536).contains(&containers));
        let mut out = (12347u32 | (((containers - 1) as u32) << 16))
            .to_le_bytes()
            .to_vec();
        let mut flags = vec![255; containers.div_ceil(8)];
        if containers % 8 != 0 {
            *flags.last_mut().unwrap() = (1u8 << (containers % 8)) - 1;
        }
        out.extend_from_slice(&flags);
        for key in 0..containers {
            out.extend_from_slice(&(key as u16).to_le_bytes());
            out.extend_from_slice(&65535u16.to_le_bytes());
        }
        if containers >= 4 {
            let header = 4 + flags.len() + containers * 8;
            for i in 0..containers {
                out.extend_from_slice(&((header + i * 6) as u32).to_le_bytes());
            }
        }
        for _ in 0..containers {
            out.extend_from_slice(&1u16.to_le_bytes());
            out.extend_from_slice(&0u16.to_le_bytes());
            out.extend_from_slice(&65535u16.to_le_bytes());
        }
        out
    }
    struct DvWakeCount(std::sync::atomic::AtomicUsize);
    impl std::task::Wake for DvWakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
    #[test]
    fn large_continuous_dv_uses_container_chunks_instead_of_position_enumeration() {
        use std::future::Future;
        use std::task::{Context, Poll, Waker};
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let context = dv_context(&runtime);
        let _entered = runtime.enter();
        let containers = 2048;
        let cardinality = containers as u64 * 65536;
        let payload = dv_payload(&[(3, full_run_bitmap(containers))]);
        assert!(payload.len() < 64 * 1024);
        let work = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed = work.clone();
        let mut future = Box::pin(decode_deletion_vector_async(
            &payload,
            &context,
            move |count, rows, bytes| observed.lock().unwrap().push((count, rows, bytes)),
        ));
        let wakes = Arc::new(DvWakeCount(Default::default()));
        let waker = Waker::from(wakes.clone());
        let mut cx = Context::from_waker(&waker);
        let mut pending = 0;
        let mut before = 0;
        let (actual, actual_cardinality) = loop {
            let poll = future.as_mut().poll(&mut cx);
            let after = work
                .lock()
                .unwrap()
                .iter()
                .map(|(count, _, _)| count)
                .sum::<usize>();
            assert!(
                after - before <= 32,
                "one poll decodes at most 32 containers"
            );
            before = after;
            match poll {
                Poll::Pending => pending += 1,
                Poll::Ready(result) => break result.unwrap(),
            }
        };
        let work = work.lock().unwrap();
        assert_eq!(
            work.iter().map(|(count, _, _)| count).sum::<usize>(),
            containers
        );
        assert_eq!(
            work.iter().map(|(_, rows, _)| rows).sum::<u64>(),
            cardinality
        );
        assert_eq!(work.len(), containers / 32);
        assert!(pending >= containers / 32);
        assert!(wakes.0.load(std::sync::atomic::Ordering::Relaxed) >= pending);
        let mut expected = roaring::RoaringBitmap::new();
        expected.insert_range(0..cardinality as u32);
        assert_eq!(actual, RoaringTreemap::from_bitmaps([(3, expected)]));
        assert_eq!(actual.len(), cardinality);
        assert_eq!(actual_cardinality, cardinality);
        assert!(!actual.contains((3u64 << 32) + cardinality));
        eprintln!(
            "DV_CONTAINER_RECEIPT logical_positions={cardinality} physical_bytes={} containers={containers} container_batches={} max_containers_per_poll=32 pending_polls={pending} enumerated_positions=0",
            payload.len(),
            work.len()
        );
    }
    #[test]
    fn continuous_dv_cancel_after_container_yield_is_typed_and_publishes_no_bitmap() {
        use std::future::Future;
        use std::task::{Context, Poll, Waker};
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let context = dv_context(&runtime);
        let _entered = runtime.enter();
        let payload = dv_payload(&[(3, full_run_bitmap(2048))]);
        let chunks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = chunks.clone();
        let mut future = Box::pin(decode_deletion_vector_async(
            &payload,
            &context,
            move |count, _, _| {
                observed.fetch_add(count, std::sync::atomic::Ordering::Relaxed);
            },
        ));
        let waker = Waker::from(Arc::new(DvWakeCount(Default::default())));
        let mut cx = Context::from_waker(&waker);
        assert!(
            future.as_mut().poll(&mut cx).is_pending(),
            "CRC checkpoint yields before container decode"
        );
        assert_eq!(chunks.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert!(
            future.as_mut().poll(&mut cx).is_pending(),
            "identified first container chunk yields"
        );
        assert_eq!(chunks.load(std::sync::atomic::Ordering::Relaxed), 32);
        context.cancellation.cancel();
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(Err(error)) => assert_eq!(
                error.kind(),
                novarocks_spi::connector::ConnectorErrorKind::Cancelled
            ),
            _ => panic!("stop at container yield must finish without publishing a bitmap"),
        }
        assert_eq!(chunks.load(std::sync::atomic::Ordering::Relaxed), 32);
    }
    #[test]
    fn cooperative_dv_matches_sync_for_array_bitmap_runs_and_multiple_high_words() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let context = dv_context(&runtime);
        let mut bitmap = roaring::RoaringBitmap::new();
        bitmap.insert(2);
        bitmap.insert(7);
        bitmap.insert_range(65536..131072);
        let mut serialized = Vec::new();
        bitmap.serialize_into(&mut serialized).unwrap();
        let payload = dv_payload(&[(0, serialized), (5, full_run_bitmap(3))]);
        let (actual, cardinality) = runtime
            .block_on(decode_deletion_vector_async(
                &payload,
                &context,
                |_, _, _| {},
            ))
            .unwrap();
        // This modest reference deliberately exercises the unchanged sync API.
        let expected = DeletionVector::from_iceberg_payload(&payload)
            .unwrap()
            .to_roaring_treemap();
        assert_eq!(actual, expected);
        assert_eq!(cardinality, actual.len());
        let empty = dv_payload(&[]);
        assert!(
            runtime
                .block_on(decode_deletion_vector_async(&empty, &context, |_, _, _| {}))
                .unwrap()
                .0
                .is_empty()
        );
    }
    #[test]
    fn cooperative_dv_preserves_mixed_four_container_offset_layout() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let context = dv_context(&runtime);
        let mut bitmap = (12347u32 | (3u32 << 16)).to_le_bytes().to_vec();
        bitmap.push(0b1001); // Run, array, bitset, run.
        for (key, count) in [(0u16, 65535u16), (1, 1), (2, 4999), (3, 2)] {
            bitmap.extend_from_slice(&key.to_le_bytes());
            bitmap.extend_from_slice(&count.to_le_bytes());
        }
        for offset in [37u32, 43, 47, 8239] {
            bitmap.extend_from_slice(&offset.to_le_bytes());
        }
        bitmap.extend_from_slice(&[1, 0, 0, 0, 255, 255]);
        bitmap.extend_from_slice(&[2, 0, 7, 0]);
        for i in 0..1024 {
            let word: u64 = if i < 5000 / 64 {
                u64::MAX
            } else if i == 5000 / 64 {
                (1u64 << (5000 % 64)) - 1
            } else {
                0
            };
            bitmap.extend_from_slice(&word.to_le_bytes());
        }
        bitmap.extend_from_slice(&[1, 0, 9, 0, 2, 0]);
        let payload = dv_payload(&[(7, bitmap.clone())]);
        let (actual, cardinality) = runtime
            .block_on(decode_deletion_vector_async(
                &payload,
                &context,
                |_, _, _| {},
            ))
            .unwrap();
        let reference = DeletionVector::from_iceberg_payload(&payload)
            .unwrap()
            .to_roaring_treemap();
        assert_eq!(actual, reference);
        assert_eq!(cardinality, 65536 + 2 + 5000 + 3);
        bitmap[4] |= 0b11110000; // Unused run flag bits have no defined meaning.
        let equivalent = dv_payload(&[(7, bitmap)]);
        let (same, same_cardinality) = runtime
            .block_on(decode_deletion_vector_async(
                &equivalent,
                &context,
                |_, _, _| {},
            ))
            .unwrap();
        assert_eq!(same, actual);
        assert_eq!(same_cardinality, cardinality);
    }
    #[test]
    fn dv_container_byte_budget_yields_before_thirty_two_large_run_containers() {
        use std::future::Future;
        use std::task::{Context, Poll, Waker};
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let context = dv_context(&runtime);
        let _entered = runtime.enter();
        let mut run = 12347u32.to_le_bytes().to_vec();
        run.push(1);
        run.extend_from_slice(&0u16.to_le_bytes());
        run.extend_from_slice(&65534u16.to_le_bytes());
        run.extend_from_slice(&65535u16.to_le_bytes());
        for value in 0..65535u16 {
            run.extend_from_slice(&value.to_le_bytes());
            run.extend_from_slice(&0u16.to_le_bytes());
        }
        let payload = dv_payload(&[(1, run.clone()), (2, run)]);
        let work = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed = work.clone();
        let mut future = Box::pin(decode_deletion_vector_async(
            &payload,
            &context,
            move |count, rows, bytes| observed.lock().unwrap().push((count, rows, bytes)),
        ));
        let waker = Waker::from(Arc::new(DvWakeCount(Default::default())));
        let mut cx = Context::from_waker(&waker);
        let mut before = 0;
        let (actual, cardinality) = loop {
            let poll = future.as_mut().poll(&mut cx);
            let after = work.lock().unwrap().len();
            assert!(
                after - before <= 1,
                "physical byte budget yields after one large run container"
            );
            before = after;
            match poll {
                Poll::Pending => {}
                Poll::Ready(result) => break result.unwrap(),
            }
        };
        let work = work.lock().unwrap();
        assert_eq!(work.len(), 2);
        for &(count, rows, bytes) in work.iter() {
            assert_eq!((count, rows), (1, 65535));
            assert!(bytes > 64 * 1024 && bytes <= 64 * 1024 + 262144);
        }
        assert_eq!(cardinality, 131070);
        assert_eq!(actual.len(), cardinality);
        eprintln!(
            "DV_BYTE_BUDGET_RECEIPT physical_bytes={} logical_positions={cardinality} container_batches=2 max_containers_per_poll=1 largest_container_work_bytes={}",
            payload.len(),
            work.iter().map(|(_, _, b)| b).max().unwrap()
        );
    }

    #[test]
    fn async_selected_dv_refs_read_two_physical_blobs_and_merge_seed() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let context = dv_context(&runtime);
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("selected-immutable-dv-blobs.puffin");
        let first = dv_payload(&[(0, full_run_bitmap(4))]);
        let second = dv_payload(&[(0, full_run_bitmap(8))]);
        let mut bytes = first.clone();
        bytes.extend_from_slice(&second);
        fs::write(&path, &bytes).unwrap();
        let access = FsAccessResolver::new()
            .resolve_location(
                novarocks_spi::connector::StorageAccessDomainId::from_bytes([13; 32]),
                path.to_string_lossy(),
                None,
            )
            .unwrap();
        let receipts = crate::file_reader::tests::RangeReceipts::default();
        let access = crate::file_reader::tests::recorded_access(&access, &receipts);
        let spec = |offset, size| IcebergDeleteFileSpec {
            path: path.to_string_lossy().to_string(),
            file_format: IcebergFileFormat::Puffin,
            file_content: IcebergFileContent::PositionDeletes,
            length: Some(bytes.len() as u64),
            content_offset: Some(offset),
            content_size_in_bytes: Some(size),
            referenced_data_file: Some("/data/a.parquet".to_string()),
        };
        let specs = [
            spec(0, first.len() as i64),
            spec(first.len() as i64, second.len() as i64),
        ];
        let mut progress = Vec::new();
        let (actual, rows) = runtime
            .block_on(load_position_deletes_async_with_metrics(
                &specs,
                "/data/a.parquet",
                &access,
                &context,
                |decoded| progress.push(decoded),
            ))
            .unwrap();
        assert_eq!(actual.len(), 8 * 65536);
        assert_eq!(rows, 12 * 65536);
        assert_eq!(progress, vec![4 * 65536, 8 * 65536]);
        let ranges = receipts.0.lock().unwrap();
        assert_eq!(ranges.len(), 2);
        assert_eq!((ranges[0].1, ranges[0].2), (0, Some(first.len() as u64)));
        assert_eq!(
            (ranges[1].1, ranges[1].2),
            (first.len() as u64, Some(second.len() as u64))
        );
    }

    #[test]
    fn multiple_dv_artifacts_merge_seed_only_through_cooperative_container_path() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let context = dv_context(&runtime);
        let first = dv_payload(&[(0, full_run_bitmap(4))]);
        let (seed, first_rows) = runtime
            .block_on(decode_deletion_vector_async(&first, &context, |_, _, _| {}))
            .unwrap();
        let second = dv_payload(&[(0, full_run_bitmap(8)), (2, full_run_bitmap(4))]);
        let work = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed = work.clone();
        let (actual, second_rows) = runtime
            .block_on(decode_deletion_vector_into_async(
                &second,
                &context,
                seed,
                move |count, rows, bytes| observed.lock().unwrap().push((count, rows, bytes)),
            ))
            .unwrap();
        assert_eq!(first_rows, 4 * 65536);
        assert_eq!(second_rows, 12 * 65536);
        assert_eq!(actual.len(), second_rows);
        assert!(actual.contains(0));
        assert!(actual.contains((2u64 << 32) + 4 * 65536 - 1));
        assert!(!actual.contains(8 * 65536));
        assert_eq!(
            work.lock()
                .unwrap()
                .iter()
                .map(|(count, _, _)| count)
                .sum::<usize>(),
            12
        );
        let expected = DeletionVector::from_iceberg_payload(&second)
            .unwrap()
            .to_roaring_treemap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn cooperative_dv_rejects_crc_offsets_key_order_cardinality_and_trailing_corruption() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let context = dv_context(&runtime);
        let run = full_run_bitmap(4);
        let flags = 1;
        let descriptions = 4 + flags;
        let offsets = descriptions + 4 * 4;
        let values = offsets + 4 * 4;
        let mut bad_offset = run.clone();
        bad_offset[offsets..offsets + 4].copy_from_slice(&0u32.to_le_bytes());
        let mut bad_order = run.clone();
        bad_order[descriptions + 4..descriptions + 6].copy_from_slice(&0u16.to_le_bytes());
        let mut bad_count = run.clone();
        bad_count[descriptions + 2..descriptions + 4].copy_from_slice(&1u16.to_le_bytes());
        let mut overlapping_runs = run.clone();
        overlapping_runs[values..values + 2].copy_from_slice(&2u16.to_le_bytes());
        overlapping_runs.splice(values + 6..values + 6, [0u8, 0, 0, 0]);
        let mut crc = dv_payload(&[(0, run.clone())]);
        *crc.last_mut().unwrap() ^= 1;
        let mut trailing = dv_payload(&[(0, run.clone())]);
        trailing.insert(trailing.len() - 4, 0);
        let body_size = trailing.len() - 8;
        trailing[..4].copy_from_slice(&(body_size as u32).to_be_bytes());
        let checksum = crc32fast::hash(&trailing[4..4 + body_size]);
        let end = trailing.len();
        trailing[end - 4..].copy_from_slice(&checksum.to_be_bytes());
        let mut truncated = dv_payload(&[(0, run.clone())]);
        truncated.remove(truncated.len() - 5);
        for payload in [
            crc,
            trailing,
            truncated,
            dv_payload(&[(0, bad_offset)]),
            dv_payload(&[(0, bad_order)]),
            dv_payload(&[(0, bad_count)]),
            dv_payload(&[(0, overlapping_runs)]),
            dv_payload(&[(0, run.clone()), (0, run.clone())]),
            dv_payload(&[(1u32 << 31, run)]),
        ] {
            let error = runtime
                .block_on(decode_deletion_vector_async(
                    &payload,
                    &context,
                    |_, _, _| {},
                ))
                .unwrap_err();
            assert_eq!(
                error.kind(),
                novarocks_spi::connector::ConnectorErrorKind::CorruptData
            );
        }
    }

    #[test]
    fn context_loader_uses_provider_owned_file_resources() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let delete_path = directory.path().join("deletes.parquet");
        write_delete_parquet(
            &delete_path,
            &["/data/a.parquet", "/data/b.parquet", "/data/a.parquet"],
            &[2, 3, 5],
        );

        let runtime = tokio::runtime::Runtime::new().expect("build Tokio runtime");
        let file_runtime: Arc<dyn FileIoRuntime> =
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone()));
        let task_spawner: Arc<dyn FileTaskSpawner> =
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone()));
        let binding = IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::clone(&file_runtime),
            Arc::clone(&task_spawner),
        );
        let access = binding
            .resolve_access(&format!(
                "file://{}",
                directory.path().join("__binding__").display()
            ))
            .expect("resolve local access");
        let context = FileReadContext {
            cancellation: FileCancellation::new(),
            deadline: Some(Instant::now() + Duration::from_secs(1)),
            runtime: file_runtime,
            task_spawner,
            range: None,
        };
        let spec = IcebergDeleteFileSpec {
            path: delete_path
                .file_name()
                .expect("delete file name")
                .to_string_lossy()
                .to_string(),
            file_format: IcebergFileFormat::Parquet,
            file_content: IcebergFileContent::PositionDeletes,
            length: None,
            content_offset: None,
            content_size_in_bytes: None,
            referenced_data_file: Some("/data/a.parquet".to_string()),
        };

        let deleted = load_position_deletes_with_context(
            &[spec.clone()],
            "/data/a.parquet",
            &access,
            &context,
        )
        .expect("read position deletes");
        assert_eq!(deleted.iter().collect::<Vec<_>>(), vec![2, 5]);

        let foreign = IcebergDeleteFileSpec {
            referenced_data_file: Some("/data/b.parquet".to_string()),
            ..spec
        };
        let error =
            load_position_deletes_with_context(&[foreign], "/data/a.parquet", &access, &context)
                .expect_err("a delete file for another data file must fail before it is read");
        assert!(error.contains("belongs to data file /data/b.parquet"));
    }

    fn write_delete_parquet(path: &std::path::Path, file_paths: &[&str], positions: &[i64]) {
        let schema = Arc::new(Schema::new(vec![
            Field::new(FILE_PATH_COLUMN, DataType::Utf8, false),
            Field::new(POS_COLUMN, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(file_paths.to_vec())),
                Arc::new(Int64Array::from(positions.to_vec())),
            ],
        )
        .expect("build delete batch");
        let file = fs::File::create(path).expect("create delete file");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("create parquet writer");
        writer.write(&batch).expect("write delete batch");
        writer.close().expect("close parquet writer");
    }
}
