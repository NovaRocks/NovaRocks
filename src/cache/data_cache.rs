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
use std::any::Any;
use std::sync::{Arc, OnceLock};

use super::block_cache::get_block_cache;
use super::cache_input_stream::CacheBlockRead;
use super::page_cache::{PageCache, PageCacheStats, PageCacheValue};
use super::{
    BlockCache, CacheDomain, CacheOptions, DataCacheIoOptions, ExternalDataCacheRangeOptions,
};
use bytes::Bytes;

pub trait DataCacheMetricsRecorder {
    fn record_cache_hit(&self, length: usize);
    fn record_datacache_read(&self, length: usize, io_ns: u128);
    fn record_datacache_write(&self, length: usize, io_ns: u128);
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataCachePageCacheOptions {
    pub capacity: usize,
    pub evict_probability: u32,
}

impl Default for DataCachePageCacheOptions {
    fn default() -> Self {
        Self {
            capacity: 1,
            evict_probability: 100,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DataCachePageKey {
    namespace: String,
    key: Vec<u8>,
}

impl DataCachePageKey {
    pub fn new(namespace: impl Into<String>, key: impl Into<Vec<u8>>) -> Self {
        Self {
            namespace: namespace.into(),
            key: key.into(),
        }
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }
}

#[derive(Clone)]
struct DataCachePageValue {
    payload: Arc<dyn Any + Send + Sync>,
    charge: usize,
}

impl std::fmt::Debug for DataCachePageValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataCachePageValue")
            .field("charge", &self.charge)
            .finish_non_exhaustive()
    }
}

impl PageCacheValue for DataCachePageValue {
    fn charge(&self) -> usize {
        self.charge.max(1)
    }
}

#[derive(Debug)]
pub struct DataCachePageCache {
    inner: PageCache<DataCachePageKey, DataCachePageValue>,
}

impl DataCachePageCache {
    pub fn new(options: DataCachePageCacheOptions) -> Self {
        let capacity = options.capacity.max(1);
        let evict_probability = options.evict_probability.min(100);
        Self {
            inner: PageCache::new(capacity, evict_probability),
        }
    }

    pub fn lookup<T>(&self, key: &DataCachePageKey) -> Option<Arc<T>>
    where
        T: Any + Send + Sync + 'static,
    {
        let handle = self.inner.lookup(key)?;
        Arc::downcast::<T>(Arc::clone(&handle.value().payload)).ok()
    }

    pub fn lookup_bytes(&self, key: &DataCachePageKey) -> Option<Bytes> {
        self.lookup::<Bytes>(key).map(|v| (*v).clone())
    }

    pub fn insert<T>(
        &self,
        key: DataCachePageKey,
        value: Arc<T>,
        charge: usize,
        evict_probability: Option<u32>,
    ) -> bool
    where
        T: Any + Send + Sync + 'static,
    {
        let charge = charge.max(1);
        let entry = DataCachePageValue {
            payload: value,
            charge,
        };
        self.inner
            .insert_with_charge_and_probability(key, entry, charge, evict_probability)
    }

    pub fn insert_bytes(
        &self,
        key: DataCachePageKey,
        value: Bytes,
        charge: usize,
        evict_probability: Option<u32>,
    ) -> bool {
        self.insert(key, Arc::new(value), charge, evict_probability)
    }

    pub fn set_capacity(&self, capacity: usize) -> bool {
        self.inner.set_capacity(capacity.max(1))
    }

    pub fn set_evict_probability(&self, evict_probability: u32) {
        self.inner.set_evict_probability(evict_probability.min(100));
    }

    pub fn stats(&self) -> PageCacheStats {
        self.inner.stats()
    }
}

#[derive(Default)]
pub struct DataCacheManager {
    page_cache: OnceLock<Arc<DataCachePageCache>>,
}

static DATA_CACHE_MANAGER: OnceLock<DataCacheManager> = OnceLock::new();

impl DataCacheManager {
    pub fn instance() -> &'static Self {
        DATA_CACHE_MANAGER.get_or_init(DataCacheManager::default)
    }

    pub fn block_cache(&self) -> Option<Arc<BlockCache>> {
        get_block_cache()
    }

    pub fn init_page_cache(&self, options: DataCachePageCacheOptions) -> bool {
        if options.capacity == 0 {
            return false;
        }
        if let Some(cache) = self.page_cache.get() {
            let _ = cache.set_capacity(options.capacity.max(1));
            cache.set_evict_probability(options.evict_probability.min(100));
            return true;
        }
        self.page_cache
            .set(Arc::new(DataCachePageCache::new(options)))
            .is_ok()
    }

    pub fn page_cache(&self) -> Option<Arc<DataCachePageCache>> {
        self.page_cache.get().cloned()
    }

    pub fn external_context(&self, options: CacheOptions) -> DataCacheContext {
        DataCacheContext::external(options)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataCacheContext {
    domain: CacheDomain,
    options: CacheOptions,
    external_range_options: Option<ExternalDataCacheRangeOptions>,
}

impl DataCacheContext {
    pub fn external(options: CacheOptions) -> Self {
        Self {
            domain: CacheDomain::External,
            options,
            external_range_options: None,
        }
    }

    pub fn internal(options: CacheOptions) -> Self {
        Self {
            domain: CacheDomain::Internal,
            options,
            external_range_options: None,
        }
    }

    pub fn with_external_range_options(
        &self,
        range_options: Option<&ExternalDataCacheRangeOptions>,
    ) -> Result<Self, String> {
        if self.domain != CacheDomain::External {
            return Ok(self.clone());
        }
        let options = self.options.with_external_range_options(range_options)?;
        Ok(Self {
            domain: self.domain,
            options,
            external_range_options: range_options.cloned(),
        })
    }

    pub fn domain(&self) -> CacheDomain {
        self.domain
    }

    pub fn cache_options(&self) -> &CacheOptions {
        &self.options
    }

    pub fn io_options(&self) -> DataCacheIoOptions {
        match self.domain {
            CacheDomain::External => self.options.external_io_options(),
            // Internal-table cache is reserved for future extension.
            CacheDomain::Internal => DataCacheIoOptions {
                domain: CacheDomain::Internal,
                enable_datacache: false,
                enable_populate_datacache: false,
                enable_datacache_async_populate_mode: false,
                enable_datacache_io_adaptor: false,
                enable_cache_select: false,
                datacache_evict_probability: self.options.datacache_evict_probability,
                datacache_priority: self.options.datacache_priority,
                datacache_ttl_seconds: self.options.datacache_ttl_seconds,
            },
        }
    }

    pub fn datacache_requested(&self) -> bool {
        let io_options = self.io_options();
        io_options.enable_datacache || io_options.enable_populate_datacache
    }

    pub fn block_cache(&self) -> Option<Arc<BlockCache>> {
        if self.datacache_requested() {
            DataCacheManager::instance().block_cache()
        } else {
            None
        }
    }

    pub fn external_modification_time(&self) -> Option<i64> {
        self.external_range_options
            .as_ref()
            .and_then(|opts| opts.modification_time)
    }

    pub fn external_range_options(&self) -> Option<&ExternalDataCacheRangeOptions> {
        self.external_range_options.as_ref()
    }

    pub fn record_page_cache_hit<R: DataCacheMetricsRecorder>(
        &self,
        recorder: Option<&R>,
        length: usize,
    ) {
        if let Some(recorder) = recorder {
            recorder.record_cache_hit(length);
        }
    }

    pub fn record_block_cache_result<R: DataCacheMetricsRecorder>(
        &self,
        recorder: Option<&R>,
        block_len: usize,
        read: &CacheBlockRead,
    ) {
        if !self.datacache_requested() {
            return;
        }
        let Some(recorder) = recorder else {
            return;
        };
        if read.hit {
            recorder.record_cache_hit(block_len);
            recorder.record_datacache_read(block_len, read.cache_read_ns);
        }
        if read.cache_write_bytes > 0 {
            recorder.record_datacache_write(read.cache_write_bytes, read.cache_write_ns);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    fn test_cache_options() -> CacheOptions {
        CacheOptions {
            enable_scan_datacache: true,
            enable_populate_datacache: false,
            enable_datacache_async_populate_mode: true,
            enable_datacache_io_adaptor: true,
            enable_cache_select: false,
            datacache_evict_probability: 10,
            datacache_priority: 0,
            datacache_ttl_seconds: 0,
            datacache_sharing_work_period: None,
        }
    }

    #[test]
    fn external_context_applies_range_options() {
        let ctx = DataCacheContext::external(test_cache_options());
        let range_options = ExternalDataCacheRangeOptions {
            modification_time: Some(123),
            enable_populate_datacache: Some(true),
            datacache_priority: Some(-1),
            candidate_node: Some("b1".to_string()),
        };
        let derived = ctx
            .with_external_range_options(Some(&range_options))
            .expect("derive external datacache context");

        assert_eq!(derived.domain(), CacheDomain::External);
        assert_eq!(derived.external_modification_time(), Some(123));
        // Priority=-1 disables scan datacache for external table reads.
        assert!(!derived.io_options().enable_datacache);
        assert!(!derived.io_options().enable_populate_datacache);
    }

    #[test]
    fn internal_context_keeps_datacache_disabled() {
        let ctx = DataCacheContext::internal(test_cache_options());
        let io = ctx.io_options();
        assert_eq!(io.domain, CacheDomain::Internal);
        assert!(!io.enable_datacache);
        assert!(!io.enable_populate_datacache);
    }

    #[derive(Default)]
    struct MockRecorder {
        hit_count: AtomicUsize,
        read_count: AtomicUsize,
        write_count: AtomicUsize,
    }

    impl DataCacheMetricsRecorder for MockRecorder {
        fn record_cache_hit(&self, _length: usize) {
            self.hit_count.fetch_add(1, Ordering::Relaxed);
        }

        fn record_datacache_read(&self, _length: usize, _io_ns: u128) {
            self.read_count.fetch_add(1, Ordering::Relaxed);
        }

        fn record_datacache_write(&self, _length: usize, _io_ns: u128) {
            self.write_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn block_cache_result_records_metrics_via_context() {
        let ctx = DataCacheContext::external(test_cache_options());
        let recorder = MockRecorder::default();
        let read = CacheBlockRead {
            bytes: Bytes::from_static(b"abc"),
            hit: true,
            cache_read_ns: 100,
            cache_write_bytes: 3,
            cache_write_ns: 200,
        };
        ctx.record_block_cache_result(Some(&recorder), read.bytes.len(), &read);

        assert_eq!(recorder.hit_count.load(Ordering::Relaxed), 1);
        assert_eq!(recorder.read_count.load(Ordering::Relaxed), 1);
        assert_eq!(recorder.write_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn page_cache_supports_typed_values() {
        let cache = DataCachePageCache::new(DataCachePageCacheOptions {
            capacity: 16,
            evict_probability: 100,
        });
        let key = DataCachePageKey::new("test", b"k1".to_vec());
        let inserted = cache.insert_bytes(key.clone(), Bytes::from_static(b"hello"), 1, Some(100));
        assert!(inserted);
        let value = cache.lookup_bytes(&key).expect("lookup bytes");
        assert_eq!(value, Bytes::from_static(b"hello"));
    }
}
