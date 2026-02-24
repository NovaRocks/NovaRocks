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
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use parquet::file::metadata::ParquetMetaData;

use crate::cache::{DataCacheManager, DataCachePageKey};

const PARQUET_CACHE_NAMESPACE: &str = "formats.parquet";
const META_KEY_PREFIX: &[u8; 2] = b"ft";
const PAGE_KEY_PREFIX: &[u8; 2] = b"pg";
const FILE_CACHE_KEY_SIZE: usize = 14;
const PAGE_CACHE_KEY_SIZE: usize = 30;
const DEFAULT_META_CHARGE: usize = 1;
const DEFAULT_PAGE_CHARGE: usize = 1;

#[derive(Clone, Debug)]
pub struct ParquetCacheOptions {
    pub enable_metadata: bool,
    pub metadata_ttl: Duration,
    pub enable_page: bool,
}

impl Default for ParquetCacheOptions {
    fn default() -> Self {
        Self {
            enable_metadata: true,
            metadata_ttl: Duration::from_secs(3600),
            enable_page: true,
        }
    }
}

#[derive(Clone, Debug)]
struct TimedMetaData {
    metadata: Arc<ParquetMetaData>,
    expire_at: Instant,
}

static PARQUET_CACHE_OPTIONS: OnceLock<ParquetCacheOptions> = OnceLock::new();

pub fn init_datacache_parquet_cache(options: ParquetCacheOptions) -> bool {
    PARQUET_CACHE_OPTIONS.set(options).is_ok()
}

fn parquet_cache_options() -> &'static ParquetCacheOptions {
    PARQUET_CACHE_OPTIONS.get_or_init(ParquetCacheOptions::default)
}

fn datacache_page_cache() -> Option<Arc<crate::cache::DataCachePageCache>> {
    DataCacheManager::instance().page_cache()
}

fn meta_cache_enabled(enabled: bool) -> bool {
    enabled && parquet_cache_options().enable_metadata
}

fn page_cache_enabled(enabled: bool) -> bool {
    enabled && parquet_cache_options().enable_page
}

pub fn parquet_meta_cache_get(
    enabled: bool,
    path: &str,
    modification_time: Option<i64>,
    file_size: u64,
) -> Option<Arc<ParquetMetaData>> {
    if !meta_cache_enabled(enabled) {
        return None;
    }
    let cache = datacache_page_cache()?;
    let key = meta_cache_key(path, modification_time, file_size);
    let cached = cache.lookup::<TimedMetaData>(&key)?;
    if cached.expire_at <= Instant::now() {
        return None;
    }
    Some(Arc::clone(&cached.metadata))
}

pub fn parquet_meta_cache_put(
    enabled: bool,
    path: &str,
    modification_time: Option<i64>,
    file_size: u64,
    metadata: Arc<ParquetMetaData>,
    evict_probability: Option<u32>,
) {
    if !meta_cache_enabled(enabled) {
        return;
    }
    let Some(cache) = datacache_page_cache() else {
        return;
    };
    let ttl = parquet_cache_options().metadata_ttl;
    let value = TimedMetaData {
        metadata,
        expire_at: Instant::now().checked_add(ttl).unwrap_or_else(Instant::now),
    };
    let key = meta_cache_key(path, modification_time, file_size);
    let _ = cache.insert(
        key,
        Arc::new(value),
        DEFAULT_META_CHARGE,
        evict_probability.map(|v| v.min(100)),
    );
}

pub fn parquet_page_cache_get(
    enabled: bool,
    path: &str,
    modification_time: Option<i64>,
    file_size: u64,
    offset: u64,
    length: usize,
) -> Option<Bytes> {
    if !page_cache_enabled(enabled) {
        return None;
    }
    let cache = datacache_page_cache()?;
    let key = page_cache_key(path, modification_time, file_size, offset, length);
    cache.lookup_bytes(&key)
}

pub fn parquet_page_cache_put(
    enabled: bool,
    path: &str,
    modification_time: Option<i64>,
    file_size: u64,
    offset: u64,
    length: usize,
    data: Bytes,
    evict_probability: Option<u32>,
) {
    if !page_cache_enabled(enabled) {
        return;
    }
    let Some(cache) = datacache_page_cache() else {
        return;
    };
    let key = page_cache_key(path, modification_time, file_size, offset, length);
    let _ = cache.insert_bytes(
        key,
        data,
        DEFAULT_PAGE_CHARGE,
        evict_probability.map(|v| v.min(100)),
    );
}

fn meta_cache_key(path: &str, modification_time: Option<i64>, file_size: u64) -> DataCachePageKey {
    let key = file_cache_key(META_KEY_PREFIX, path, modification_time, file_size);
    DataCachePageKey::new(PARQUET_CACHE_NAMESPACE, key.to_vec())
}

fn page_cache_key(
    path: &str,
    modification_time: Option<i64>,
    file_size: u64,
    offset: u64,
    length: usize,
) -> DataCachePageKey {
    let mut key = [0u8; PAGE_CACHE_KEY_SIZE];
    key[..FILE_CACHE_KEY_SIZE].copy_from_slice(&file_cache_key(
        PAGE_KEY_PREFIX,
        path,
        modification_time,
        file_size,
    ));
    key[FILE_CACHE_KEY_SIZE..FILE_CACHE_KEY_SIZE + 8].copy_from_slice(&offset.to_le_bytes());
    let length = u64::try_from(length).unwrap_or(u64::MAX);
    key[FILE_CACHE_KEY_SIZE + 8..PAGE_CACHE_KEY_SIZE].copy_from_slice(&length.to_le_bytes());
    DataCachePageKey::new(PARQUET_CACHE_NAMESPACE, key.to_vec())
}

fn file_cache_key(
    prefix: &[u8; 2],
    path: &str,
    modification_time: Option<i64>,
    file_size: u64,
) -> [u8; FILE_CACHE_KEY_SIZE] {
    let mut key = [0u8; FILE_CACHE_KEY_SIZE];
    let hash_value = hash64(path.as_bytes(), 0);
    key[..8].copy_from_slice(&hash_value.to_le_bytes());
    key[8..10].copy_from_slice(prefix);

    // Follow StarRocks behavior: use mtime when available, fallback to file size.
    let tail = if modification_time.unwrap_or(0) > 0 {
        let mtime = modification_time.unwrap_or(0);
        ((mtime >> 9) & 0x0000_0000_FFFF_FFFF) as u32
    } else {
        file_size as u32
    };
    key[10..14].copy_from_slice(&tail.to_le_bytes());
    key
}

fn hash64(data: &[u8], seed: u64) -> u64 {
    murmur_hash3_x64_64(data, seed)
}

fn murmur_hash3_x64_64(data: &[u8], seed: u64) -> u64 {
    let nblocks = data.len() / 8;
    let mut h1 = seed;

    const C1: u64 = 0x87c3_7b91_1142_53d5;
    const C2: u64 = 0x4cf5_ad43_2745_937f;

    for i in 0..nblocks {
        let offset = i * 8;
        let mut k1 = u64::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
            data[offset + 4],
            data[offset + 5],
            data[offset + 6],
            data[offset + 7],
        ]);

        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(31);
        k1 = k1.wrapping_mul(C2);

        h1 ^= k1;
        h1 = h1.rotate_left(27);
        h1 = h1.wrapping_mul(5).wrapping_add(0x52dc_e729);
    }

    let tail = &data[nblocks * 8..];
    let mut k1 = 0u64;
    match tail.len() & 7 {
        7 => k1 ^= (tail[6] as u64) << 48,
        6 => k1 ^= (tail[5] as u64) << 40,
        5 => k1 ^= (tail[4] as u64) << 32,
        4 => k1 ^= (tail[3] as u64) << 24,
        3 => k1 ^= (tail[2] as u64) << 16,
        2 => k1 ^= (tail[1] as u64) << 8,
        1 => k1 ^= tail[0] as u64,
        _ => {}
    }
    if (tail.len() & 7) != 0 {
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(31);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;
    }

    h1 ^= data.len() as u64;
    fmix64(h1)
}

fn fmix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    k ^= k >> 33;
    k
}
