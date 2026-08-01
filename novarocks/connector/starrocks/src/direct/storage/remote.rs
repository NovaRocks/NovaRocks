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
use std::io::Cursor;
use std::sync::{Arc, Mutex};

use arrow::array::{
    Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, StringArray,
};
use arrow::compute::{concat_batches, filter_record_batch};
use arrow::datatypes::{DataType, Field, Schema};
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
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use roaring::RoaringBitmap;

use crate::direct::reader::StarRocksDirectStorageResolver;
use crate::direct::staros::{StarOsV1ObjectStoreResolver, StarRocksDirectIoRuntime};
use crate::direct::{
    StarRocksDirectColumnBinding, StarRocksDirectMetadataLayout, StarRocksDirectSplit,
    StarRocksSharedDataDirectReaderFactory,
};

use super::kernel::{decode_frozen_column, decode_plain_segment};
use super::model::merge_key_model_batches;
use super::segment::decode_segment_footer;
use super::wire::{
    StorageDeletePredicate, StorageModel, StorageRowset, StorageSchema, StorageTabletMetadata,
    decode_bundle_metadata, decode_standalone_metadata,
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

        let mut decoded_batches = Vec::new();
        let mut bytes_read = if metadata_cache_hit {
            0
        } else {
            metadata_bytes.len() as u64
        };
        let mut read_requests = u64::from(!metadata_cache_hit);
        let mut cache_hits = u64::from(metadata_cache_hit);
        let mut cache_misses = u64::from(!metadata_cache_hit);
        for (rowset_index, rowset) in metadata.rowsets.iter().enumerate() {
            ensure_active(&request.context)?;
            for (index, relative_path) in rowset.segments.iter().enumerate() {
                let segment_path = join_frozen_path(split.tablet_root(), relative_path)?;
                let is_parquet = relative_path.to_ascii_lowercase().ends_with(".parquet");
                let range = if let Some(offset) = rowset.bundle_offsets.get(index) {
                    if is_parquet {
                        return Err(unsupported(
                            "StarRocks direct embedded bundle Parquet segments are unsupported",
                        ));
                    }
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
                if is_parquet {
                    if !predicates_for_rowset(&metadata, rowset_index).is_empty()
                        || metadata
                            .delvecs
                            .contains_key(&rowset.id.saturating_add(index as u32))
                    {
                        return Err(unsupported(
                            "StarRocks direct Parquet rowsets with storage deletes are unsupported",
                        ));
                    }
                    decoded_batches.extend(decode_frozen_parquet_segment(
                        &segment_path,
                        &segment,
                        request.expected_schema.clone(),
                    )?);
                    continue;
                }
                let footer = decode_segment_footer(&segment_path, &segment)?;
                let mut batch = decode_plain_segment(
                    &segment_path,
                    &segment,
                    &footer,
                    request.expected_schema.clone(),
                    split.columns(),
                )?;
                let predicates = predicates_for_rowset(&metadata, rowset_index);
                if !predicates.is_empty() {
                    let mut predicate_batch = augment_predicate_batch(
                        &batch,
                        &segment_path,
                        &segment,
                        &footer,
                        &metadata,
                        rowset,
                        &predicates,
                    )?;
                    for predicate in predicates {
                        predicate_batch =
                            apply_storage_delete_predicate(&predicate_batch, predicate)?;
                    }
                    batch =
                        project_frozen_output(&predicate_batch, request.expected_schema.clone())?;
                }
                if let Some((deleted, bytes, cache_hit)) =
                    self.load_delvec(split, &access, &metadata, rowset, index, &request.context)?
                {
                    if cache_hit {
                        cache_hits = cache_hits.saturating_add(1);
                    } else {
                        cache_misses = cache_misses.saturating_add(1);
                        bytes_read = bytes_read.saturating_add(bytes as u64);
                        read_requests = read_requests.saturating_add(1);
                    }
                    batch = apply_delvec(&batch, &deleted)?;
                }
                if matches!(
                    metadata.schema.model,
                    StorageModel::Aggregate | StorageModel::Unique
                ) {
                    batch = augment_key_model_batch(
                        &batch,
                        &segment_path,
                        &segment,
                        &footer,
                        &metadata,
                        rowset,
                    )?;
                }
                decoded_batches.push(batch);
            }
        }
        let output_batch = if decoded_batches.is_empty() {
            RecordBatch::new_empty(request.expected_schema.clone())
        } else if matches!(
            metadata.schema.model,
            StorageModel::Aggregate | StorageModel::Unique
        ) {
            merge_key_model_batches(
                metadata.schema.model,
                &metadata.schema,
                request.expected_schema.clone(),
                &decoded_batches,
            )?
        } else {
            concat_batches(&request.expected_schema, &decoded_batches)
                .map_err(|_| corrupt("cannot concatenate StarRocks direct storage batches"))?
        };
        let chunks = VecDeque::from(slice_batch(&output_batch, request.batch)?);
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
    fn load_delvec(
        &self,
        split: &StarRocksDirectSplit,
        access: &FsAccessHandle,
        metadata: &StorageTabletMetadata,
        rowset: &StorageRowset,
        segment_index: usize,
        context: &ConnectorRequestContext,
    ) -> Result<Option<(RoaringBitmap, usize, bool)>, ConnectorError> {
        let segment_index = u32::try_from(segment_index)
            .map_err(|_| corrupt("StarRocks direct segment index overflows delete-vector ID"))?;
        let segment_id = rowset
            .id
            .checked_add(segment_index)
            .ok_or_else(|| corrupt("StarRocks direct delete-vector segment ID overflows"))?;
        let Some(page) = metadata.delvecs.get(&segment_id) else {
            return Ok(None);
        };
        let file = metadata.delvec_files.get(&page.version).ok_or_else(|| {
            corrupt("StarRocks direct delete-vector page has no frozen file mapping")
        })?;
        let end = page
            .offset
            .checked_add(page.size)
            .ok_or_else(|| corrupt("StarRocks direct delete-vector range overflows"))?;
        if file.size.is_some_and(|size| end > size) {
            return Err(corrupt(
                "StarRocks direct delete-vector range exceeds its frozen file size",
            ));
        }
        let path = join_frozen_path(
            split.tablet_root(),
            &format!("data/{}", file.name.trim_start_matches('/')),
        )?;
        let range = FileReadRange::bounded(page.offset, page.size).map_err(map_file_error)?;
        let (payload, cache_hit) = self.read_cached(split, access, &path, range, context)?;
        if payload.len() as u64 != page.size {
            return Err(corrupt(
                "StarRocks direct delete-vector payload differs from its frozen size",
            ));
        }
        if let Some(masked) = page.crc32c
            && page.crc32c_gen_version == Some(page.version)
            && crc32c::crc32c(&payload) != crc32c_unmask(masked)
        {
            return Err(corrupt(
                "StarRocks direct delete-vector checksum differs from frozen metadata",
            ));
        }
        let bitmap = decode_delvec_bitmap(&payload)?;
        Ok(Some((bitmap, payload.len(), cache_hit)))
    }

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
    if metadata
        .delvecs
        .values()
        .any(|page| !metadata.delvec_files.contains_key(&page.version))
    {
        return Err(corrupt(
            "StarRocks storage delete-vector page has no frozen file mapping",
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

fn predicates_for_rowset(
    metadata: &StorageTabletMetadata,
    rowset_index: usize,
) -> Vec<&StorageDeletePredicate> {
    metadata.rowsets[rowset_index..]
        .iter()
        .filter_map(|rowset| rowset.delete_predicate.as_ref())
        .collect()
}

fn decode_frozen_parquet_segment(
    _segment_path: &str,
    bytes: &Bytes,
    output_schema: Arc<Schema>,
) -> Result<Vec<RecordBatch>, ConnectorError> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
        .map_err(|_| corrupt("StarRocks direct Parquet segment is malformed"))?;
    let source_schema = builder.schema().clone();
    let reader = builder
        .build()
        .map_err(|_| corrupt("StarRocks direct Parquet segment reader cannot be built"))?;
    let mut projection = Vec::with_capacity(output_schema.fields().len());
    for field in output_schema.fields() {
        let index = source_schema.index_of(field.name()).map_err(|_| {
            corrupt("StarRocks direct Parquet segment omits a frozen output column")
        })?;
        let source = source_schema.field(index);
        if source.data_type() != field.data_type() || source.is_nullable() != field.is_nullable() {
            return Err(corrupt(
                "StarRocks direct Parquet segment schema differs from frozen output schema",
            ));
        }
        projection.push(index);
    }
    let mut batches = Vec::new();
    for batch in reader {
        let batch =
            batch.map_err(|_| corrupt("StarRocks direct Parquet segment cannot be decoded"))?;
        let columns = projection
            .iter()
            .map(|index| batch.column(*index).clone())
            .collect::<Vec<_>>();
        batches.push(
            RecordBatch::try_new(Arc::clone(&output_schema), columns).map_err(|_| {
                corrupt("StarRocks direct Parquet batch does not match frozen output schema")
            })?,
        );
    }
    Ok(batches)
}

fn decode_delvec_bitmap(payload: &[u8]) -> Result<RoaringBitmap, ConnectorError> {
    const DELVEC_FORMAT_V1: u8 = 1;
    let Some((&format, encoded)) = payload.split_first() else {
        return Err(corrupt("StarRocks direct delete-vector payload is empty"));
    };
    if format != DELVEC_FORMAT_V1 {
        return Err(unsupported(
            "StarRocks direct delete-vector payload version is unsupported",
        ));
    }
    if encoded.is_empty() {
        return Ok(RoaringBitmap::new());
    }
    let mut cursor = Cursor::new(encoded);
    let bitmap = RoaringBitmap::deserialize_from(&mut cursor)
        .map_err(|_| corrupt("StarRocks direct delete-vector payload is malformed"))?;
    if cursor.position() != encoded.len() as u64 {
        return Err(corrupt(
            "StarRocks direct delete-vector payload has trailing bytes",
        ));
    }
    Ok(bitmap)
}

fn apply_delvec(
    batch: &RecordBatch,
    deleted: &RoaringBitmap,
) -> Result<RecordBatch, ConnectorError> {
    if deleted.is_empty() {
        return Ok(batch.clone());
    }
    if batch.num_rows() > u32::MAX as usize {
        return Err(corrupt(
            "StarRocks direct segment has too many rows for its delete-vector format",
        ));
    }
    let keep = BooleanArray::from_iter(
        (0..batch.num_rows()).map(|row| Some(!deleted.contains(row as u32))),
    );
    filter_record_batch(batch, &keep)
        .map_err(|_| corrupt("failed to apply StarRocks direct delete-vector"))
}

fn crc32c_unmask(masked: u32) -> u32 {
    const CRC32C_MASK_DELTA: u32 = 0xa282_ead8;
    masked.wrapping_sub(CRC32C_MASK_DELTA).rotate_right(17)
}

fn apply_storage_delete_predicate(
    batch: &RecordBatch,
    predicate: &StorageDeletePredicate,
) -> Result<RecordBatch, ConnectorError> {
    if !predicate.sub_predicates.is_empty() {
        return Err(unsupported(
            "StarRocks direct storage delete predicate text clauses are unsupported",
        ));
    }
    let keep = BooleanArray::from_iter(
        (0..batch.num_rows())
            .map(|row| predicate_matches_row(batch, predicate, row).map(|deleted| Some(!deleted)))
            .collect::<Result<Vec<_>, _>>()?,
    );
    filter_record_batch(batch, &keep)
        .map_err(|_| corrupt("failed to apply StarRocks direct storage delete predicate"))
}

fn augment_predicate_batch(
    batch: &RecordBatch,
    segment_path: &str,
    segment: &[u8],
    footer: &super::segment::StarRocksSegmentFooter,
    metadata: &StorageTabletMetadata,
    rowset: &StorageRowset,
    predicates: &[&StorageDeletePredicate],
) -> Result<RecordBatch, ConnectorError> {
    let mut columns = batch.columns().to_vec();
    let mut fields = batch.schema().fields().to_vec();
    let source_schema = metadata
        .rowset_to_schema
        .get(&rowset.id)
        .map(|schema_id| {
            metadata.historical_schemas.get(schema_id).ok_or_else(|| {
                corrupt("StarRocks rowset predicate schema is missing from frozen metadata")
            })
        })
        .transpose()?
        .unwrap_or(&metadata.schema);
    let required = predicates
        .iter()
        .flat_map(|predicate| {
            predicate
                .in_predicates
                .iter()
                .map(|term| term.column_name.as_str())
                .chain(
                    predicate
                        .binary_predicates
                        .iter()
                        .map(|term| term.column_name.as_str()),
                )
                .chain(
                    predicate
                        .is_null_predicates
                        .iter()
                        .map(|term| term.column_name.as_str()),
                )
        })
        .collect::<std::collections::BTreeSet<_>>();
    for name in required {
        if batch.column_by_name(name).is_some() {
            continue;
        }
        let current = metadata
            .schema
            .columns
            .iter()
            .find(|column| column.name == name)
            .ok_or_else(|| {
                unsupported(
                    "StarRocks direct storage delete predicate references an unknown column",
                )
            })?;
        let source = source_schema
            .columns
            .iter()
            .find(|column| column.unique_id == current.unique_id);
        let physical_type = source
            .map(|column| column.physical_type.as_str())
            .unwrap_or(current.physical_type.as_str());
        let data_type = storage_arrow_type(physical_type)?;
        let binding = StarRocksDirectColumnBinding::try_new(
            0,
            current.unique_id,
            current.name.clone(),
            physical_type,
            source
                .map(|column| column.nullable)
                .unwrap_or(current.nullable),
            current.default_value.clone().map(Bytes::from),
        )?;
        let column = decode_frozen_column(segment_path, segment, footer, &binding, &data_type)?;
        if column.len() != batch.num_rows() {
            return Err(corrupt(
                "StarRocks direct storage predicate column row count differs from output",
            ));
        }
        fields.push(Arc::new(Field::new(name, data_type, binding.nullable)));
        columns.push(column);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|_| corrupt("StarRocks direct storage predicate batch is invalid"))
}

fn augment_key_model_batch(
    batch: &RecordBatch,
    segment_path: &str,
    segment: &[u8],
    footer: &super::segment::StarRocksSegmentFooter,
    metadata: &StorageTabletMetadata,
    rowset: &StorageRowset,
) -> Result<RecordBatch, ConnectorError> {
    let mut columns = batch.columns().to_vec();
    let mut fields = batch.schema().fields().to_vec();
    let source_schema = metadata
        .rowset_to_schema
        .get(&rowset.id)
        .map(|schema_id| {
            metadata.historical_schemas.get(schema_id).ok_or_else(|| {
                corrupt("StarRocks rowset key schema is missing from frozen metadata")
            })
        })
        .transpose()?
        .unwrap_or(&metadata.schema);
    for current in metadata
        .schema
        .columns
        .iter()
        .filter(|column| column.is_key)
    {
        if batch.column_by_name(&current.name).is_some() {
            continue;
        }
        let source = source_schema
            .columns
            .iter()
            .find(|column| column.unique_id == current.unique_id);
        let physical_type = source
            .map(|column| column.physical_type.as_str())
            .unwrap_or(current.physical_type.as_str());
        let data_type = storage_arrow_type(physical_type)?;
        let binding = StarRocksDirectColumnBinding::try_new(
            0,
            current.unique_id,
            current.name.clone(),
            physical_type,
            source
                .map(|column| column.nullable)
                .unwrap_or(current.nullable),
            current.default_value.clone().map(Bytes::from),
        )?;
        let column = decode_frozen_column(segment_path, segment, footer, &binding, &data_type)?;
        if column.len() != batch.num_rows() {
            return Err(corrupt(
                "StarRocks direct key-model column row count differs from output",
            ));
        }
        fields.push(Arc::new(Field::new(
            current.name.clone(),
            data_type,
            binding.nullable,
        )));
        columns.push(column);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|_| corrupt("StarRocks direct key-model batch is invalid"))
}

fn project_frozen_output(
    batch: &RecordBatch,
    schema: Arc<Schema>,
) -> Result<RecordBatch, ConnectorError> {
    if batch.num_columns() < schema.fields().len() {
        return Err(corrupt(
            "StarRocks direct storage predicate batch loses a frozen output column",
        ));
    }
    let output_columns = schema.fields().len();
    RecordBatch::try_new(schema, batch.columns()[..output_columns].to_vec())
        .map_err(|_| corrupt("StarRocks direct storage predicate projection is invalid"))
}

fn storage_arrow_type(physical_type: &str) -> Result<DataType, ConnectorError> {
    match physical_type.trim().to_ascii_uppercase().as_str() {
        "TINYINT" => Ok(DataType::Int8),
        "SMALLINT" => Ok(DataType::Int16),
        "INT" => Ok(DataType::Int32),
        "BIGINT" => Ok(DataType::Int64),
        "FLOAT" => Ok(DataType::Float32),
        "DOUBLE" => Ok(DataType::Float64),
        "BOOLEAN" => Ok(DataType::Boolean),
        "CHAR" | "VARCHAR" => Ok(DataType::Utf8),
        "BINARY" | "VARBINARY" => Ok(DataType::Binary),
        _ => Err(unsupported(
            "StarRocks direct storage delete predicate physical type is unsupported",
        )),
    }
}

fn predicate_matches_row(
    batch: &RecordBatch,
    predicate: &StorageDeletePredicate,
    row: usize,
) -> Result<bool, ConnectorError> {
    for term in &predicate.in_predicates {
        let values = term
            .values
            .iter()
            .map(|value| normalized_literal(value))
            .collect::<Vec<_>>();
        if !matches_column_value(
            batch,
            &term.column_name,
            row,
            if term.is_not_in {
                DeleteOp::NotIn
            } else {
                DeleteOp::In
            },
            &values,
        )? {
            return Ok(false);
        }
    }
    for term in &predicate.binary_predicates {
        let op = DeleteOp::parse(&term.op)?;
        if !matches_column_value(
            batch,
            &term.column_name,
            row,
            op,
            &[normalized_literal(&term.value)],
        )? {
            return Ok(false);
        }
    }
    for term in &predicate.is_null_predicates {
        let op = if term.is_not_null {
            DeleteOp::IsNotNull
        } else {
            DeleteOp::IsNull
        };
        if !matches_column_value(batch, &term.column_name, row, op, &[])? {
            return Ok(false);
        }
    }
    Ok(true)
}

#[derive(Clone, Copy)]
enum DeleteOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    In,
    NotIn,
    IsNull,
    IsNotNull,
}

impl DeleteOp {
    fn parse(value: &str) -> Result<Self, ConnectorError> {
        match value.trim() {
            "=" => Ok(Self::Eq),
            "!=" => Ok(Self::Ne),
            "<" | "<<" => Ok(Self::Lt),
            "<=" => Ok(Self::Le),
            ">" | ">>" => Ok(Self::Gt),
            ">=" => Ok(Self::Ge),
            _ => Err(unsupported(
                "StarRocks direct storage delete predicate operator is unsupported",
            )),
        }
    }
}

fn matches_column_value(
    batch: &RecordBatch,
    name: &str,
    row: usize,
    op: DeleteOp,
    values: &[String],
) -> Result<bool, ConnectorError> {
    let array = batch.column_by_name(name).ok_or_else(|| {
        unsupported("StarRocks direct storage delete predicate requires a non-projected column")
    })?;
    if array.is_null(row) {
        return Ok(matches!(op, DeleteOp::IsNull));
    }
    if matches!(op, DeleteOp::IsNull) {
        return Ok(false);
    }
    if matches!(op, DeleteOp::IsNotNull) {
        return Ok(true);
    }
    match array.data_type() {
        DataType::Int8 => typed_scalar(
            array,
            op,
            values,
            |array| {
                array
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .map(|array| array.value(row))
            },
            |value| value.parse::<i8>(),
        ),
        DataType::Int16 => typed_scalar(
            array,
            op,
            values,
            |array| {
                array
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .map(|array| array.value(row))
            },
            |value| value.parse::<i16>(),
        ),
        DataType::Int32 => typed_scalar(
            array,
            op,
            values,
            |array| {
                array
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .map(|array| array.value(row))
            },
            |value| value.parse::<i32>(),
        ),
        DataType::Int64 => typed_scalar(
            array,
            op,
            values,
            |array| {
                array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .map(|array| array.value(row))
            },
            |value| value.parse::<i64>(),
        ),
        DataType::Float32 => typed_scalar(
            array,
            op,
            values,
            |array| {
                array
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .map(|array| array.value(row))
            },
            |value| value.parse::<f32>(),
        ),
        DataType::Float64 => typed_scalar(
            array,
            op,
            values,
            |array| {
                array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .map(|array| array.value(row))
            },
            |value| value.parse::<f64>(),
        ),
        DataType::Boolean => typed_scalar(
            array,
            op,
            values,
            |array| {
                array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .map(|array| array.value(row))
            },
            |value| match value.to_ascii_lowercase().as_str() {
                "1" | "true" => Ok(true),
                "0" | "false" => Ok(false),
                _ => Err(()),
            },
        ),
        DataType::Utf8 => match_bytes(array, row, op, values),
        DataType::Binary => match_bytes(array, row, op, values),
        _ => Err(unsupported(
            "StarRocks direct storage delete predicate type is unsupported",
        )),
    }
}

fn typed_scalar<T, E>(
    array: &dyn Array,
    op: DeleteOp,
    raw: &[String],
    value: impl FnOnce(&dyn Array) -> Option<T>,
    parse: impl Fn(&str) -> Result<T, E>,
) -> Result<bool, ConnectorError>
where
    T: Copy + PartialEq + PartialOrd,
{
    let values = raw
        .iter()
        .map(|value| {
            parse(value).map_err(|_| {
                corrupt("StarRocks direct storage delete predicate literal is invalid")
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let value = value(array)
        .ok_or_else(|| corrupt("StarRocks direct storage delete predicate Arrow type mismatch"))?;
    Ok(compare(value, op, &values))
}

fn match_bytes(
    array: &dyn Array,
    row: usize,
    op: DeleteOp,
    raw: &[String],
) -> Result<bool, ConnectorError> {
    let value = if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        array.value(row).as_bytes()
    } else if let Some(array) = array.as_any().downcast_ref::<BinaryArray>() {
        array.value(row)
    } else {
        return Err(corrupt(
            "StarRocks direct storage delete predicate Arrow type mismatch",
        ));
    };
    Ok(compare_bytes(
        value,
        op,
        &raw.iter()
            .map(|value| value.as_bytes().to_vec())
            .collect::<Vec<_>>(),
    ))
}

fn compare<T: Copy + PartialEq + PartialOrd>(value: T, op: DeleteOp, values: &[T]) -> bool {
    match op {
        DeleteOp::Eq => values.first() == Some(&value),
        DeleteOp::Ne => values.first().is_some_and(|other| value != *other),
        DeleteOp::Lt => values.first().is_some_and(|other| value < *other),
        DeleteOp::Le => values.first().is_some_and(|other| value <= *other),
        DeleteOp::Gt => values.first().is_some_and(|other| value > *other),
        DeleteOp::Ge => values.first().is_some_and(|other| value >= *other),
        DeleteOp::In => values.contains(&value),
        DeleteOp::NotIn => !values.contains(&value),
        DeleteOp::IsNull | DeleteOp::IsNotNull => false,
    }
}

fn compare_bytes(value: &[u8], op: DeleteOp, values: &[Vec<u8>]) -> bool {
    let first = values.first().map(Vec::as_slice);
    match op {
        DeleteOp::Eq => first == Some(value),
        DeleteOp::Ne => first.is_some_and(|other| value != other),
        DeleteOp::Lt => first.is_some_and(|other| value < other),
        DeleteOp::Le => first.is_some_and(|other| value <= other),
        DeleteOp::Gt => first.is_some_and(|other| value > other),
        DeleteOp::Ge => first.is_some_and(|other| value >= other),
        DeleteOp::In => values.iter().any(|other| value == other),
        DeleteOp::NotIn => values.iter().all(|other| value != other),
        DeleteOp::IsNull | DeleteOp::IsNotNull => false,
    }
}

fn normalized_literal(value: &str) -> String {
    let value = value.trim();
    let bytes = value.as_bytes();
    if bytes.len() >= 2 && matches!(bytes[0], b'\'' | b'"') && bytes[0] == bytes[bytes.len() - 1] {
        value[1..value.len() - 1].to_string()
    } else {
        value.to_string()
    }
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
    use crate::direct::storage::wire::{
        StorageBinaryPredicate, StorageColumn, StorageInPredicate, StorageIsNullPredicate,
    };
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use std::io::Cursor;

    #[test]
    fn decodes_bundle_parquet_segment_with_exact_frozen_projection() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![7_i64, 8]))],
        )
        .unwrap();
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(Cursor::new(&mut bytes), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let batches = decode_frozen_parquet_segment(
            "data/bundle.parquet",
            &Bytes::from(bytes),
            batch.schema(),
        )
        .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
    }

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

    #[test]
    fn decodes_and_applies_frozen_primary_delete_vector() {
        let mut encoded = vec![1_u8];
        let bitmap = RoaringBitmap::from_iter([1_u32, 3]);
        bitmap.serialize_into(&mut encoded).unwrap();
        let bitmap = decode_delvec_bitmap(&encoded).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4, 5]))],
        )
        .unwrap();

        let filtered = apply_delvec(&batch, &bitmap).unwrap();
        assert_eq!(
            filtered
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[1, 3, 5]
        );
    }

    #[test]
    fn rejects_malformed_or_trailing_direct_delete_vector_bytes() {
        assert_eq!(
            decode_delvec_bitmap(&[]).unwrap_err().kind(),
            ConnectorErrorKind::CorruptData
        );
        assert_eq!(
            decode_delvec_bitmap(&[2]).unwrap_err().kind(),
            ConnectorErrorKind::Unsupported
        );
        let mut encoded = vec![1_u8];
        RoaringBitmap::from_iter([1_u32])
            .serialize_into(&mut encoded)
            .unwrap();
        encoded.push(0);
        assert_eq!(
            decode_delvec_bitmap(&encoded).unwrap_err().kind(),
            ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn applies_storage_delete_predicate_as_a_conjunction_to_frozen_output() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("state", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4])),
                Arc::new(StringArray::from(vec![
                    Some("old"),
                    Some("old"),
                    Some("new"),
                    None,
                ])),
            ],
        )
        .unwrap();
        let predicate = StorageDeletePredicate {
            sub_predicates: Vec::new(),
            in_predicates: vec![StorageInPredicate {
                column_name: "id".to_string(),
                is_not_in: false,
                values: vec!["2".to_string(), "3".to_string()],
            }],
            binary_predicates: vec![StorageBinaryPredicate {
                column_name: "state".to_string(),
                op: "=".to_string(),
                value: "'old'".to_string(),
            }],
            is_null_predicates: Vec::new(),
        };

        let filtered = apply_storage_delete_predicate(&batch, &predicate).unwrap();
        assert_eq!(
            filtered
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[1, 3, 4]
        );
        let null_predicate = StorageDeletePredicate {
            sub_predicates: Vec::new(),
            in_predicates: Vec::new(),
            binary_predicates: Vec::new(),
            is_null_predicates: vec![StorageIsNullPredicate {
                column_name: "state".to_string(),
                is_not_null: false,
            }],
        };
        assert_eq!(
            apply_storage_delete_predicate(&batch, &null_predicate)
                .unwrap()
                .num_rows(),
            3
        );
    }

    #[test]
    fn rejects_delete_predicates_that_cannot_be_evaluated_from_frozen_output() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1_i64]))],
        )
        .unwrap();
        let predicate = StorageDeletePredicate {
            sub_predicates: Vec::new(),
            in_predicates: vec![StorageInPredicate {
                column_name: "hidden".to_string(),
                is_not_in: false,
                values: vec!["1".to_string()],
            }],
            binary_predicates: Vec::new(),
            is_null_predicates: Vec::new(),
        };
        assert_eq!(
            apply_storage_delete_predicate(&batch, &predicate)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::Unsupported
        );
    }
}
