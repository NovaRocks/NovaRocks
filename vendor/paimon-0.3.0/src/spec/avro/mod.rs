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

pub mod cursor;
pub mod decode;
pub(crate) mod decode_helpers;
mod index_manifest_entry_decode;
pub(crate) mod manifest_entry_decode;
mod manifest_file_meta_decode;
pub mod ocf;
pub mod schema;

use crate::io::{ReadControl, ReadRetention};
use cursor::AvroCursor;
use decode::AvroRecordDecode;
use ocf::{parse_ocf_streaming, parse_ocf_streaming_with_control, MAX_AVRO_BLOCK_OBJECTS};
use schema::WriterSchema;
use std::mem::size_of;
use std::sync::{Arc, Mutex, RwLock};

const SCHEMA_PARSE_BASE_BYTES: u64 = 4 * 1024;
/// A schema byte may be represented simultaneously by the JSON parser tree,
/// recursive `WriterSchema` nodes, owned field names, and the cache key.
const SCHEMA_PARSE_BYTES_PER_INPUT_BYTE: u64 = 64;

struct CachedWriterSchema {
    schema_json: String,
    schema: Arc<WriterSchema>,
    control: Option<Arc<dyn ReadControl>>,
}

/// Cache for parsed WriterSchemas, keyed by schema JSON string.
/// Same manifest type always produces the same schema JSON, so parsing
/// once and reusing across files within a scan saves repeated work.
/// Uses Vec instead of HashMap since Paimon tables typically have 1-2 distinct schemas.
pub struct SchemaCache {
    cache: Vec<CachedWriterSchema>,
    // `ReadReservation` is Send but need not be Sync. The mutex lets a shared
    // schema cache own those leases without weakening the public host trait.
    retention: Mutex<ReadRetention>,
}

/// A decoded value and the host reservations covering its retained allocation.
///
/// Controlled table planning moves this wrapper through manifest pruning and
/// split construction so the reservation cannot disappear while the decoded
/// objects are still retained.
#[derive(Debug)]
pub(crate) struct RetainedDecode<T> {
    value: T,
    retention: ReadRetention,
}

impl<T> RetainedDecode<T> {
    pub(crate) fn new(value: T, retention: ReadRetention) -> Self {
        Self { value, retention }
    }

    pub(crate) fn as_ref(&self) -> &T {
        &self.value
    }

    pub(crate) fn into_parts(self) -> (T, ReadRetention) {
        (self.value, self.retention)
    }

    pub(crate) fn into_value(self) -> T {
        self.value
    }
}

impl SchemaCache {
    pub fn new() -> Self {
        Self {
            cache: Vec::new(),
            retention: Mutex::new(ReadRetention::default()),
        }
    }

    pub fn get_or_parse(&mut self, schema_json: &str) -> crate::Result<Arc<WriterSchema>> {
        self.get_or_parse_with_control(schema_json, None)
    }

    pub(crate) fn get_or_parse_with_control(
        &mut self,
        schema_json: &str,
        control: Option<&Arc<dyn ReadControl>>,
    ) -> crate::Result<Arc<WriterSchema>> {
        if let Some(index) = self
            .cache
            .iter()
            .position(|cached| cached.schema_json == schema_json)
        {
            if let Some(control) = control {
                match self.cache[index].control.as_ref() {
                    Some(existing) if !Arc::ptr_eq(existing, control) => {
                        return Err(crate::Error::DataInvalid {
                            message: "avro schema cache cannot cross a read-control boundary"
                                .to_string(),
                            source: None,
                        });
                    }
                    Some(_) => {}
                    None => {
                        let reservation = reserve_schema_cache_entry(schema_json, control)?;
                        self.cache[index].control = Some(control.clone());
                        self.retention
                            .get_mut()
                            .unwrap_or_else(|error| error.into_inner())
                            .push(reservation);
                    }
                }
            }
            return Ok(Arc::clone(&self.cache[index].schema));
        }

        // Reserve before serde_json or WriterSchema can allocate. The lease is
        // installed only after parse and insertion succeed and then remains
        // owned by this cache until the cache is dropped.
        let reservation = control
            .map(|control| reserve_schema_cache_entry(schema_json, control))
            .transpose()?;
        let ws = Arc::new(WriterSchema::parse(schema_json)?);
        if let Some(control) = control {
            control.checkpoint()?;
        }
        self.cache
            .try_reserve(1)
            .map_err(|error| crate::Error::DataInvalid {
                message: "avro schema cache cannot reserve an entry".to_string(),
                source: Some(Box::new(error)),
            })?;
        let mut owned_schema_json = String::new();
        owned_schema_json
            .try_reserve_exact(schema_json.len())
            .map_err(|error| crate::Error::DataInvalid {
                message: "avro schema cache cannot reserve its schema key".to_string(),
                source: Some(Box::new(error)),
            })?;
        owned_schema_json.push_str(schema_json);
        self.cache.push(CachedWriterSchema {
            schema_json: owned_schema_json,
            schema: Arc::clone(&ws),
            control: control.cloned(),
        });
        if let Some(reservation) = reservation {
            self.retention
                .get_mut()
                .unwrap_or_else(|error| error.into_inner())
                .push(reservation);
        }
        Ok(ws)
    }
}

impl Default for SchemaCache {
    fn default() -> Self {
        Self::new()
    }
}

fn schema_cache_reservation_bytes(schema_json: &str) -> crate::Result<u64> {
    u64::try_from(schema_json.len())
        .unwrap_or(u64::MAX)
        .checked_mul(SCHEMA_PARSE_BYTES_PER_INPUT_BYTE)
        .and_then(|bytes| bytes.checked_add(SCHEMA_PARSE_BASE_BYTES))
        .ok_or_else(|| crate::Error::DataInvalid {
            message: "avro schema cache reservation size overflow".to_string(),
            source: None,
        })
}

fn reserve_schema_cache_entry(
    schema_json: &str,
    control: &Arc<dyn ReadControl>,
) -> crate::Result<Box<dyn crate::io::ReadReservation>> {
    control.checkpoint()?;
    control.try_reserve(schema_cache_reservation_bytes(schema_json)?)
}

/// Thread-safe schema cache for sharing across concurrent async tasks.
/// Wraps `SchemaCache` in `Arc<RwLock<_>>` so multiple tasks can reuse
/// the same parsed `WriterSchema` without re-parsing.
#[derive(Clone)]
pub struct SharedSchemaCache {
    inner: Arc<RwLock<SchemaCache>>,
}

impl SharedSchemaCache {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(SchemaCache::new())),
        }
    }

    pub fn get_or_parse(&self, schema_json: &str) -> crate::Result<Arc<WriterSchema>> {
        // Fast path: read lock for cache hit
        {
            let cache = self.inner.read().unwrap_or_else(|e| e.into_inner());
            if let Some(cached) = cache
                .cache
                .iter()
                .find(|cached| cached.schema_json == schema_json)
            {
                return Ok(Arc::clone(&cached.schema));
            }
        }
        // Slow path: write lock for cache miss
        self.inner
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .get_or_parse(schema_json)
    }

    pub(crate) fn get_or_parse_with_control(
        &self,
        schema_json: &str,
        control: Option<&Arc<dyn ReadControl>>,
    ) -> crate::Result<Arc<WriterSchema>> {
        // A controlled lookup takes the write lock even on a hit so checking
        // or installing the single cache-owned lease is atomic with lookup.
        self.inner
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .get_or_parse_with_control(schema_json, control)
    }
}

impl Default for SharedSchemaCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Read an Avro OCF file and decode records directly into `T`, bypassing
/// the intermediate `apache_avro::Value` representation.
pub fn from_avro_bytes_fast<T: AvroRecordDecode>(bytes: &[u8]) -> crate::Result<Vec<T>> {
    let mut cache = SchemaCache::new();
    from_avro_bytes_with_cache(bytes, &mut cache)
}

pub(crate) fn from_avro_bytes_fast_with_control<T: AvroRecordDecode>(
    bytes: &[u8],
    control: Option<Arc<dyn ReadControl>>,
) -> crate::Result<RetainedDecode<Vec<T>>> {
    let mut cache = SchemaCache::new();
    from_avro_bytes_with_cache_and_control(bytes, &mut cache, control)
}

/// Same as `from_avro_bytes_fast` but reuses a `SchemaCache` across calls.
pub fn from_avro_bytes_with_cache<T: AvroRecordDecode>(
    bytes: &[u8],
    cache: &mut SchemaCache,
) -> crate::Result<Vec<T>> {
    from_avro_bytes_with_cache_and_control(bytes, cache, None).map(RetainedDecode::into_value)
}

fn from_avro_bytes_with_cache_and_control<T: AvroRecordDecode>(
    bytes: &[u8],
    cache: &mut SchemaCache,
    control: Option<Arc<dyn ReadControl>>,
) -> crate::Result<RetainedDecode<Vec<T>>> {
    let (header, mut block_iter) = parse_ocf_streaming_with_control(bytes, control.clone())?;
    let writer_schema = cache.get_or_parse_with_control(&header.schema_json, control.as_ref())?;

    let mut results = Vec::new();
    let mut retention = ReadRetention::default();
    while let Some(block) = block_iter.next_block()? {
        let (object_count, data, block_reservations) = block.into_parts();
        for reservation in block_reservations {
            retention.push(reservation);
        }
        reserve_decode_results::<T>(&mut results, object_count, control.as_ref(), &mut retention)?;
        let mut cursor = AvroCursor::new(data.as_ref());
        for index in 0..object_count {
            checkpoint_decode(control.as_ref(), index)?;
            let record = decode_top_level_record::<T>(&mut cursor, &writer_schema)?;
            results.push(record);
        }
    }

    Ok(RetainedDecode::new(results, retention))
}

/// Decode ManifestEntry records from Avro OCF bytes with a lightweight filter.
///
/// The filter receives `(kind, partition_bytes, bucket, total_buckets)` and
/// returns true to keep the entry. Entries that fail the filter skip the
/// expensive `DataFileMeta` decoding entirely.
#[allow(dead_code)]
pub fn from_manifest_bytes_filtered<F>(
    bytes: &[u8],
    cache: &mut SchemaCache,
    filter: &mut F,
) -> crate::Result<Vec<crate::spec::ManifestEntry>>
where
    F: FnMut(crate::spec::FileKind, &[u8], i32, i32) -> bool,
{
    let (header, mut block_iter) = parse_ocf_streaming(bytes)?;
    let writer_schema = cache.get_or_parse(&header.schema_json)?;
    decode_manifest_streaming(&mut block_iter, &writer_schema, None, filter)
        .map(RetainedDecode::into_value)
}

pub(crate) fn from_manifest_bytes_filtered_shared_with_control<F>(
    bytes: &[u8],
    shared_cache: &SharedSchemaCache,
    control: Option<Arc<dyn ReadControl>>,
    filter: &mut F,
) -> crate::Result<RetainedDecode<Vec<crate::spec::ManifestEntry>>>
where
    F: FnMut(crate::spec::FileKind, &[u8], i32, i32) -> bool,
{
    let (header, mut block_iter) = parse_ocf_streaming_with_control(bytes, control.clone())?;
    let writer_schema =
        shared_cache.get_or_parse_with_control(&header.schema_json, control.as_ref())?;
    decode_manifest_streaming(&mut block_iter, &writer_schema, control, filter)
}

/// Decode ManifestEntry records from Avro OCF bytes using a pre-resolved shared schema.
///
/// Use this when the `WriterSchema` is shared across concurrent tasks via
/// `SharedSchemaCache`. Falls back to parsing if the OCF schema differs.
#[allow(dead_code)]
pub fn from_manifest_bytes_filtered_shared<F>(
    bytes: &[u8],
    shared_cache: &SharedSchemaCache,
    filter: &mut F,
) -> crate::Result<Vec<crate::spec::ManifestEntry>>
where
    F: FnMut(crate::spec::FileKind, &[u8], i32, i32) -> bool,
{
    let (header, mut block_iter) = parse_ocf_streaming(bytes)?;
    let writer_schema = shared_cache.get_or_parse(&header.schema_json)?;
    decode_manifest_streaming(&mut block_iter, &writer_schema, None, filter)
        .map(RetainedDecode::into_value)
}

pub(crate) fn decode_manifest_streaming<F>(
    block_iter: &mut ocf::OcfBlockIter<'_>,
    writer_schema: &WriterSchema,
    control: Option<Arc<dyn ReadControl>>,
    filter: &mut F,
) -> crate::Result<RetainedDecode<Vec<crate::spec::ManifestEntry>>>
where
    F: FnMut(crate::spec::FileKind, &[u8], i32, i32) -> bool,
{
    let mut results = Vec::new();
    let mut retention = ReadRetention::default();
    while let Some(block) = block_iter.next_block()? {
        let (object_count, data, block_reservations) = block.into_parts();
        for reservation in block_reservations {
            retention.push(reservation);
        }
        reserve_decode_results::<crate::spec::ManifestEntry>(
            &mut results,
            object_count,
            control.as_ref(),
            &mut retention,
        )?;
        let mut cursor = AvroCursor::new(data.as_ref());
        for index in 0..object_count {
            checkpoint_decode(control.as_ref(), index)?;
            if let Some(entry) = manifest_entry_decode::decode_manifest_entries_filtered(
                &mut cursor,
                writer_schema,
                writer_schema.is_union_wrapped,
                filter,
            )? {
                results.push(entry);
            }
        }
    }
    Ok(RetainedDecode::new(results, retention))
}

fn reserve_decode_results<T>(
    results: &mut Vec<T>,
    object_count: usize,
    control: Option<&Arc<dyn ReadControl>>,
    retention: &mut ReadRetention,
) -> crate::Result<()> {
    if object_count > MAX_AVRO_BLOCK_OBJECTS {
        return Err(crate::Error::DataInvalid {
            message: format!(
                "avro ocf: block object count {object_count} exceeds limit {MAX_AVRO_BLOCK_OBJECTS}"
            ),
            source: None,
        });
    }
    let requested = object_count
        .checked_mul(size_of::<T>().max(1))
        .ok_or_else(|| crate::Error::DataInvalid {
            message: "avro ocf: decoded object allocation size overflow".to_string(),
            source: None,
        })?;
    if let Some(control) = control {
        retention.push(control.try_reserve(u64::try_from(requested).unwrap_or(u64::MAX).max(1))?);
    }
    results
        .try_reserve(object_count)
        .map_err(|error| crate::Error::DataInvalid {
            message: format!(
                "avro ocf: cannot reserve capacity for {object_count} decoded objects"
            ),
            source: Some(Box::new(error)),
        })
}

fn checkpoint_decode(
    control: Option<&Arc<dyn ReadControl>>,
    object_index: usize,
) -> crate::Result<()> {
    if object_index % 256 == 0 {
        if let Some(control) = control {
            control.checkpoint()?;
        }
    }
    Ok(())
}

/// Decode a single record from the cursor, handling the top-level union wrapper
/// that Paimon uses (`["null", record]`).
fn decode_top_level_record<T: AvroRecordDecode>(
    cursor: &mut AvroCursor,
    writer_schema: &WriterSchema,
) -> crate::Result<T> {
    if writer_schema.is_union_wrapped {
        let idx = cursor.read_union_index()?;
        if idx == 0 {
            return Err(crate::Error::UnexpectedError {
                message: "avro decode: unexpected null in top-level union".into(),
                source: None,
            });
        }
    }
    T::decode(cursor, writer_schema)
}

#[cfg(test)]
mod retention_tests {
    use super::*;
    use crate::io::{ReadControl, ReadReservation};
    use crate::spec::manifest_file_meta::MANIFEST_FILE_META_SCHEMA;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::ManifestFileMeta;
    use std::mem::size_of;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::thread;

    #[derive(Debug)]
    struct TestControl {
        retained: Arc<AtomicU64>,
        peak: AtomicU64,
        reserve_calls: AtomicUsize,
        limit: u64,
    }

    impl TestControl {
        fn new(limit: u64) -> Self {
            Self {
                retained: Arc::new(AtomicU64::new(0)),
                peak: AtomicU64::new(0),
                reserve_calls: AtomicUsize::new(0),
                limit,
            }
        }
    }

    impl ReadControl for TestControl {
        fn check_active(&self) -> crate::Result<()> {
            Ok(())
        }

        fn checkpoint(&self) -> crate::Result<()> {
            Ok(())
        }

        fn try_reserve(&self, bytes: u64) -> crate::Result<Box<dyn ReadReservation>> {
            let previous = self
                .retained
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                    current
                        .checked_add(bytes)
                        .filter(|next| *next <= self.limit)
                })
                .map_err(|current| crate::Error::DataInvalid {
                    message: format!(
                        "test reservation limit exceeded: current={current}, requested={bytes}, limit={}",
                        self.limit
                    ),
                    source: None,
                })?;
            let next = previous + bytes;
            self.peak.fetch_max(next, Ordering::SeqCst);
            self.reserve_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(TestReservation {
                bytes,
                retained: self.retained.clone(),
            }))
        }
    }

    #[derive(Debug)]
    struct TestReservation {
        bytes: u64,
        retained: Arc<AtomicU64>,
    }

    impl ReadReservation for TestReservation {
        fn bytes(&self) -> u64 {
            self.bytes
        }

        fn into_any(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
            self
        }
    }

    impl Drop for TestReservation {
        fn drop(&mut self) {
            self.retained.fetch_sub(self.bytes, Ordering::SeqCst);
        }
    }

    #[test]
    fn controlled_decode_retains_result_reservation_until_wrapper_drop() {
        assert_controlled_payload_retention("zstd");
    }

    #[test]
    fn controlled_null_payload_reservation_follows_decoded_result() {
        assert_controlled_payload_retention("null");
    }

    #[test]
    fn controlled_snappy_payload_reservation_follows_decoded_result() {
        assert_controlled_payload_retention("snappy");
    }

    #[test]
    fn controlled_zstd_payload_reservation_follows_decoded_result() {
        assert_controlled_payload_retention("zstd");
    }

    fn assert_controlled_payload_retention(compression: &str) {
        let original = vec![ManifestFileMeta::new(
            format!("manifest-{compression}-{}", "x".repeat(4096)),
            128,
            1,
            0,
            BinaryTableStats::empty(),
            0,
        )];
        let bytes = crate::spec::to_avro_bytes_with_compression(
            MANIFEST_FILE_META_SCHEMA,
            &original,
            compression,
        )
        .unwrap();
        let (_, blocks) = ocf::parse_ocf(&bytes).unwrap();
        let decoded_payload_bytes = blocks
            .iter()
            .map(|block| u64::try_from(block.data.len()).unwrap())
            .sum::<u64>();
        let object_bytes = blocks
            .iter()
            .map(|block| u64::try_from(block.object_count * size_of::<ManifestFileMeta>()).unwrap())
            .sum::<u64>();
        drop(blocks);

        let control = Arc::new(TestControl::new(u64::MAX));
        let read_control: Arc<dyn ReadControl> = control.clone();

        let decoded =
            from_avro_bytes_fast_with_control::<ManifestFileMeta>(&bytes, Some(read_control))
                .unwrap();
        assert_eq!(decoded.as_ref(), &original);
        assert_eq!(
            control.retained.load(Ordering::SeqCst),
            decoded_payload_bytes + object_bytes
        );

        drop(decoded);
        assert_eq!(control.retained.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn schema_cache_miss_hit_and_drop_have_one_cache_owned_lease() {
        let schema_json =
            r#"{"type":"record","name":"test","fields":[{"name":"value","type":"string"}]}"#;
        let expected = schema_cache_reservation_bytes(schema_json).unwrap();
        let control = Arc::new(TestControl::new(expected));
        let read_control: Arc<dyn ReadControl> = control.clone();
        let mut cache = SchemaCache::new();

        let first = cache
            .get_or_parse_with_control(schema_json, Some(&read_control))
            .unwrap();
        let second = cache
            .get_or_parse_with_control(schema_json, Some(&read_control))
            .unwrap();

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(control.reserve_calls.load(Ordering::SeqCst), 1);
        assert_eq!(control.retained.load(Ordering::SeqCst), expected);
        drop((first, second));
        assert_eq!(control.retained.load(Ordering::SeqCst), expected);

        drop(cache);
        assert_eq!(control.retained.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn shared_schema_cache_concurrent_miss_installs_one_lease() {
        let schema_json = Arc::<str>::from(
            r#"{"type":"record","name":"test","fields":[{"name":"value","type":"long"}]}"#,
        );
        let expected = schema_cache_reservation_bytes(&schema_json).unwrap();
        let control = Arc::new(TestControl::new(expected));
        let read_control: Arc<dyn ReadControl> = control.clone();
        let cache = SharedSchemaCache::new();

        let handles = (0..8)
            .map(|_| {
                let cache = cache.clone();
                let schema_json = schema_json.clone();
                let read_control = read_control.clone();
                thread::spawn(move || {
                    cache
                        .get_or_parse_with_control(&schema_json, Some(&read_control))
                        .unwrap()
                })
            })
            .collect::<Vec<_>>();
        let schemas = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();

        assert!(schemas
            .iter()
            .all(|schema| Arc::ptr_eq(schema, &schemas[0])));
        assert_eq!(control.reserve_calls.load(Ordering::SeqCst), 1);
        assert_eq!(control.retained.load(Ordering::SeqCst), expected);
        drop(schemas);
        drop(cache);
        assert_eq!(control.retained.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn oversized_schema_is_refused_before_parse_and_leaves_no_lease() {
        let oversized_invalid_json = "[".repeat(128 * 1024);
        let control = Arc::new(TestControl::new(1024));
        let read_control: Arc<dyn ReadControl> = control.clone();
        let mut cache = SchemaCache::new();

        let error = cache
            .get_or_parse_with_control(&oversized_invalid_json, Some(&read_control))
            .unwrap_err();

        assert!(error.to_string().contains("reservation limit exceeded"));
        assert_eq!(control.retained.load(Ordering::SeqCst), 0);
        assert_eq!(control.reserve_calls.load(Ordering::SeqCst), 0);
    }
}
