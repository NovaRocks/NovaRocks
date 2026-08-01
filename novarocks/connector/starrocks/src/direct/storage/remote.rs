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

//! Startup-local object-store adapter for frozen direct read splits.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use novarocks_fs::{
    BoundFile, FileCancellation, FileError, FileErrorKind, FileIdentity, FileReadRange,
    FsAccessHandle, FsAccessResolver, FsScheme,
};
use novarocks_spi::connector::{
    ConnectorBatchReader, ConnectorError, ConnectorErrorKind, ConnectorOpenReaderRequest,
    ConnectorReaderMetricsSnapshot, ConnectorRequestContext,
};

use crate::direct::reader::StarRocksDirectStorageResolver;
use crate::direct::staros::{StarOsV1ObjectStoreResolver, StarRocksDirectIoRuntime};
use crate::direct::{
    StarRocksDirectMetadataLayout, StarRocksDirectSplit, StarRocksSharedDataDirectReaderFactory,
};

use super::kernel::decode_plain_segment;
use super::segment::decode_segment_footer;
use super::wire::{
    StorageModel, StorageSchema, StorageTabletMetadata, decode_bundle_metadata,
    decode_standalone_metadata,
};
use super::{DirectStorageConnectorReader, DirectStorageReader, slice_batch};

/// Concrete startup-local resolver. Credentials are retrieved only from
/// StarOS for the split's frozen binding, then remain inside `novarocks-fs`.
pub struct StarRocksSharedDataStorageResolver {
    staros: StarOsV1ObjectStoreResolver,
    fs: FsAccessResolver,
    runtime: StarRocksDirectIoRuntime,
    cache: Mutex<BTreeMap<DirectStorageCacheKey, Bytes>>,
}

impl StarRocksSharedDataStorageResolver {
    pub fn new(
        staros: StarOsV1ObjectStoreResolver,
        fs: FsAccessResolver,
        runtime: StarRocksDirectIoRuntime,
    ) -> Self {
        Self {
            staros,
            fs,
            runtime,
            cache: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn into_reader_factory(self) -> StarRocksSharedDataDirectReaderFactory {
        StarRocksSharedDataDirectReaderFactory::new(Arc::new(self))
    }
}

impl StarRocksDirectStorageResolver for StarRocksSharedDataStorageResolver {
    fn open_direct_storage(
        &self,
        split: &StarRocksDirectSplit,
        request: ConnectorOpenReaderRequest,
    ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
        ensure_active(&request.context)?;
        let config = self.staros.resolve(
            split.storage_binding(),
            split.tablet_root(),
            &request.context,
        )?;
        let access = self
            .fs
            .resolve_location(split.tablet_root(), Some(&config))
            .map_err(map_file_error)?;
        if access.scheme() != FsScheme::ObjectStore {
            return Err(unsupported(
                "StarRocks direct production reader supports object storage only",
            ));
        }
        let metadata_path = join_frozen_path(split.tablet_root(), split.metadata_relative_path())?;
        let (metadata_bytes, metadata_cache_hit) = self.read_cached(
            split,
            &access,
            &metadata_path,
            FileReadRange::WholeFile,
            &request.context,
        )?;
        let metadata = match split.metadata_layout() {
            StarRocksDirectMetadataLayout::Standalone => decode_standalone_metadata(
                &metadata_bytes,
                split.tablet_id(),
                split.tablet_version(),
            ),
            StarRocksDirectMetadataLayout::Bundle => {
                decode_bundle_metadata(&metadata_bytes, split.tablet_id(), split.tablet_version())
            }
        }?;
        validate_metadata(split, &metadata)?;

        let mut chunks = VecDeque::new();
        let mut bytes_read = (!metadata_cache_hit)
            .then_some(metadata_bytes.len() as u64)
            .unwrap_or(0);
        let mut read_requests = u64::from(!metadata_cache_hit);
        let mut cache_hits = u64::from(metadata_cache_hit);
        let mut cache_misses = u64::from(!metadata_cache_hit);
        for rowset in &metadata.rowsets {
            ensure_active(&request.context)?;
            if rowset.delete_predicate.is_some() {
                return Err(unsupported(
                    "StarRocks direct storage delete predicates are not implemented",
                ));
            }
            for (index, relative_path) in rowset.segments.iter().enumerate() {
                let segment_path = join_frozen_path(split.tablet_root(), relative_path)?;
                let range = if let Some(offset) = rowset.bundle_offsets.get(index) {
                    let offset = u64::try_from(*offset)
                        .map_err(|_| corrupt("StarRocks bundle segment offset is invalid"))?;
                    let size = *rowset.segment_sizes.get(index).ok_or_else(|| {
                        corrupt("StarRocks bundle segment is missing its frozen size")
                    })?;
                    FileReadRange::bounded(offset, size).map_err(map_file_error)?
                } else {
                    FileReadRange::WholeFile
                };
                let (segment, cache_hit) =
                    self.read_cached(split, &access, &segment_path, range, &request.context)?;
                if let Some(expected_size) = rowset.segment_sizes.get(index)
                    && *expected_size != segment.len() as u64
                {
                    return Err(corrupt(
                        "StarRocks frozen segment size differs from storage metadata",
                    ));
                }
                if cache_hit {
                    cache_hits = cache_hits.saturating_add(1);
                } else {
                    cache_misses = cache_misses.saturating_add(1);
                    bytes_read = bytes_read.saturating_add(segment.len() as u64);
                    read_requests = read_requests.saturating_add(1);
                }
                let footer = decode_segment_footer(&segment_path, &segment)?;
                let batch = decode_plain_segment(
                    &segment_path,
                    &segment,
                    &footer,
                    request.expected_schema.clone(),
                    split.columns(),
                )?;
                chunks.extend(slice_batch(&batch, request.batch)?);
            }
        }
        Ok(Box::new(DirectStorageConnectorReader::new(
            Box::new(FrozenStorageReader {
                chunks,
                closed: false,
                metrics: ConnectorReaderMetricsSnapshot {
                    bytes_read,
                    read_requests,
                    cache_hits,
                    cache_misses,
                    ..ConnectorReaderMetricsSnapshot::default()
                },
            }),
            request.context,
        )))
    }
}

impl StarRocksSharedDataStorageResolver {
    fn read_cached(
        &self,
        split: &StarRocksDirectSplit,
        access: &FsAccessHandle,
        location: &str,
        range: FileReadRange,
        context: &ConnectorRequestContext,
    ) -> Result<(Bytes, bool), ConnectorError> {
        let key = DirectStorageCacheKey::new(split, location, range);
        if let Some(bytes) = self
            .cache
            .lock()
            .map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "StarRocks direct cache lock poisoned",
                )
            })?
            .get(&key)
            .cloned()
        {
            return Ok((bytes, true));
        }
        let bytes = self.read_range(access, location, range, context)?;
        let mut cache = self.cache.lock().map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "StarRocks direct cache lock poisoned",
            )
        })?;
        if cache.len() >= 128 {
            let _ = cache.pop_first();
        }
        cache.insert(key, bytes.clone());
        Ok((bytes, false))
    }

    fn read_range(
        &self,
        access: &FsAccessHandle,
        location: &str,
        range: FileReadRange,
        context: &ConnectorRequestContext,
    ) -> Result<Bytes, ConnectorError> {
        ensure_active(context)?;
        let file = access
            .bind_location(location, FileIdentity::new(location, 0, None))
            .map_err(map_file_error)?;
        let context = (*context).clone();
        self.runtime
            .block_on(async move { read_file(file, range, context).await })
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DirectStorageCacheKey {
    owner: Arc<str>,
    incarnation: [u8; 16],
    storage_identity: Arc<str>,
    tablet_id: i64,
    tablet_version: i64,
    schema_version: Bytes,
    data_version: Bytes,
    output_schema_digest: [u8; 32],
    location: Arc<str>,
    range: (u64, u64, bool),
}

impl DirectStorageCacheKey {
    fn new(split: &StarRocksDirectSplit, location: &str, range: FileReadRange) -> Self {
        Self {
            owner: Arc::clone(&split.owner),
            incarnation: split.incarnation,
            storage_identity: Arc::from(split.storage_identity()),
            tablet_id: split.tablet_id(),
            tablet_version: split.tablet_version(),
            schema_version: split.schema_version.clone(),
            data_version: split.data_version.clone(),
            output_schema_digest: split.output_schema_digest,
            location: Arc::from(location),
            range: match range {
                FileReadRange::WholeFile => (0, 0, true),
                FileReadRange::Bounded { offset, length } => (offset, length, false),
            },
        }
    }
}

async fn read_file(
    file: BoundFile,
    range: FileReadRange,
    context: ConnectorRequestContext,
) -> Result<Bytes, ConnectorError> {
    let cancellation = FileCancellation::new();
    let operation = file.read(range, &cancellation);
    tokio::pin!(operation);
    loop {
        ensure_active(&context)?;
        let wait = context
            .deadline()
            .checked_duration_since(std::time::Instant::now())
            .ok_or_else(|| deadline("StarRocks direct storage deadline elapsed"))?;
        tokio::select! {
            result = &mut operation => {
                let bytes = result.map_err(map_file_error)?;
                ensure_active(&context)?;
                return Ok(bytes);
            }
            _ = tokio::time::sleep(wait.min(std::time::Duration::from_millis(10))) => {}
        }
    }
}

fn validate_metadata(
    split: &StarRocksDirectSplit,
    metadata: &StorageTabletMetadata,
) -> Result<(), ConnectorError> {
    if metadata.id != split.tablet_id() || metadata.version != split.tablet_version() {
        return Err(corrupt(
            "StarRocks storage metadata identity differs from frozen split",
        ));
    }
    if metadata.schema.model != StorageModel::Duplicate {
        return Err(unsupported(
            "StarRocks direct key-model merge reader is not implemented",
        ));
    }
    if !metadata.delvecs.is_empty() {
        return Err(unsupported(
            "StarRocks direct delete-vector reader is not implemented",
        ));
    }
    validate_current_schema(split, &metadata.schema)?;
    for schema in metadata.historical_schemas.values() {
        validate_historical_schema(split.columns(), schema)?;
    }
    if metadata
        .rowset_to_schema
        .values()
        .any(|schema_id| !metadata.historical_schemas.contains_key(schema_id))
    {
        return Err(corrupt(
            "StarRocks rowset references a missing historical schema",
        ));
    }
    Ok(())
}

fn validate_current_schema(
    split: &StarRocksDirectSplit,
    schema: &StorageSchema,
) -> Result<(), ConnectorError> {
    validate_schema_columns(split.columns(), schema, false)
}

fn validate_historical_schema(
    bindings: &[crate::direct::StarRocksDirectColumnBinding],
    schema: &StorageSchema,
) -> Result<(), ConnectorError> {
    validate_schema_columns(bindings, schema, true)
}

fn validate_schema_columns(
    bindings: &[crate::direct::StarRocksDirectColumnBinding],
    schema: &StorageSchema,
    historical: bool,
) -> Result<(), ConnectorError> {
    let columns = schema
        .columns
        .iter()
        .map(|column| (column.unique_id, column))
        .collect::<BTreeMap<_, _>>();
    for binding in bindings {
        let Some(column) = columns.get(&binding.unique_id) else {
            if historical && (binding.nullable || binding.default_value.is_some()) {
                continue;
            }
            return Err(corrupt(
                "StarRocks storage schema omits a required frozen direct column",
            ));
        };
        if column.name != binding.name.as_ref()
            || !column
                .physical_type
                .eq_ignore_ascii_case(binding.physical_type.as_ref())
            || column.nullable != binding.nullable
            || (!historical && column.default_value.as_deref() != binding.default_value.as_deref())
        {
            return Err(corrupt(
                "StarRocks storage schema differs from frozen direct mapping",
            ));
        }
    }
    Ok(())
}

fn join_frozen_path(root: &str, relative: &str) -> Result<String, ConnectorError> {
    if !is_safe_relative_path(relative) {
        return Err(corrupt(
            "StarRocks storage metadata names an unsafe relative path",
        ));
    }
    Ok(format!("{}/{}", root.trim_end_matches('/'), relative))
}

fn is_safe_relative_path(value: &str) -> bool {
    !value.is_empty()
        && !value.starts_with('/')
        && !value.contains('\\')
        && value
            .split('/')
            .all(|part| !part.is_empty() && part != "." && part != "..")
}

struct FrozenStorageReader {
    chunks: VecDeque<RecordBatch>,
    closed: bool,
    metrics: ConnectorReaderMetricsSnapshot,
}

impl DirectStorageReader for FrozenStorageReader {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        if self.closed {
            return Ok(None);
        }
        let batch = self.chunks.pop_front();
        if let Some(batch) = &batch {
            self.metrics.rows_decoded = self
                .metrics
                .rows_decoded
                .saturating_add(batch.num_rows() as u64);
            self.metrics.batches_delivered = self.metrics.batches_delivered.saturating_add(1);
        }
        Ok(batch)
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        self.closed = true;
        self.chunks.clear();
        Ok(())
    }

    fn metrics_snapshot(&self) -> ConnectorReaderMetricsSnapshot {
        self.metrics
    }
}

fn map_file_error(error: FileError) -> ConnectorError {
    let kind = match error.kind() {
        FileErrorKind::Cancelled => ConnectorErrorKind::Cancelled,
        FileErrorKind::DeadlineExceeded => ConnectorErrorKind::DeadlineExceeded,
        FileErrorKind::NotFound => ConnectorErrorKind::NotFound,
        FileErrorKind::Permission => ConnectorErrorKind::PermissionDenied,
        FileErrorKind::Unsupported => ConnectorErrorKind::Unsupported,
        FileErrorKind::Invalid => ConnectorErrorKind::InvalidRequest,
        FileErrorKind::ResourceExhausted => ConnectorErrorKind::ResourceExhausted,
        FileErrorKind::Transient => ConnectorErrorKind::Unavailable,
        FileErrorKind::Corrupt => ConnectorErrorKind::CorruptData,
        FileErrorKind::Internal => ConnectorErrorKind::Internal,
    };
    ConnectorError::new(kind, "StarRocks direct storage I/O failed")
}

fn ensure_active(context: &ConnectorRequestContext) -> Result<(), ConnectorError> {
    if context.cancellation().is_cancelled() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Cancelled,
            "StarRocks direct storage read was cancelled",
        ));
    }
    if std::time::Instant::now() >= context.deadline() {
        return Err(deadline("StarRocks direct storage deadline elapsed"));
    }
    Ok(())
}

fn deadline(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::DeadlineExceeded, message)
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}

fn unsupported(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unsupported, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::direct::StarRocksDirectColumnBinding;
    use crate::direct::storage::wire::StorageColumn;

    fn schema(columns: Vec<StorageColumn>) -> StorageSchema {
        StorageSchema {
            id: None,
            model: StorageModel::Duplicate,
            columns,
        }
    }

    fn column(unique_id: i32) -> StorageColumn {
        StorageColumn {
            unique_id,
            name: format!("column_{unique_id}"),
            physical_type: "INT".to_string(),
            is_key: false,
            aggregation: None,
            nullable: false,
            default_value: None,
            precision: None,
            scale: None,
            length: None,
            children: Vec::new(),
        }
    }

    #[test]
    fn historical_schema_permits_frozen_default_or_nullable_missing_columns() {
        let defaulted = StarRocksDirectColumnBinding::try_new(
            0,
            2,
            "added",
            "INT",
            false,
            Some(Bytes::from_static(b"7")),
        )
        .unwrap();
        let nullable =
            StarRocksDirectColumnBinding::try_new(1, 3, "optional", "INT", true, None).unwrap();
        let old_schema = schema(vec![column(1)]);

        validate_historical_schema(&[defaulted, nullable], &old_schema).unwrap();
    }

    #[test]
    fn historical_schema_rejects_required_missing_column_without_default() {
        let required =
            StarRocksDirectColumnBinding::try_new(0, 2, "missing", "INT", false, None).unwrap();
        assert_eq!(
            validate_historical_schema(&[required], &schema(vec![column(1)]))
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn historical_schema_uses_physical_column_facts_but_not_current_default() {
        let binding = StarRocksDirectColumnBinding::try_new(
            0,
            1,
            "column_1",
            "INT",
            false,
            Some(Bytes::from_static(b"new_default")),
        )
        .unwrap();
        let mut previous = column(1);
        previous.default_value = Some(b"old_default".to_vec());

        validate_historical_schema(&[binding], &schema(vec![previous])).unwrap();
    }
}
