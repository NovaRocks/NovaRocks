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

use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::cache::{DataCacheManager, DataCachePageKey};

use super::segment::StarRocksSegmentFooter;

const STARROCKS_CACHE_NAMESPACE: &str = "formats.starrocks.native";
const SEGMENT_FOOTER_PREFIX: [u8; 2] = *b"sf";
const NATIVE_BATCH_PREFIX: [u8; 2] = *b"rb";
const SEGMENT_FOOTER_KEY_SIZE: usize = 26;

const DEFAULT_SEGMENT_FOOTER_CHARGE: usize = 1;
const DEFAULT_NATIVE_BATCH_CHARGE: usize = 1;

pub(crate) fn segment_footer_cache_get(
    tablet_root_path: &str,
    tablet_id: i64,
    version: i64,
) -> Option<Vec<StarRocksSegmentFooter>> {
    let cache = DataCacheManager::instance().page_cache()?;
    let key = segment_footer_key(tablet_root_path, tablet_id, version);
    cache
        .lookup::<Vec<StarRocksSegmentFooter>>(&key)
        .map(|v| (*v).clone())
}

pub(crate) fn segment_footer_cache_put(
    tablet_root_path: &str,
    tablet_id: i64,
    version: i64,
    footers: Vec<StarRocksSegmentFooter>,
) {
    let Some(cache) = DataCacheManager::instance().page_cache() else {
        return;
    };
    let key = segment_footer_key(tablet_root_path, tablet_id, version);
    let _ = cache.insert(key, Arc::new(footers), DEFAULT_SEGMENT_FOOTER_CHARGE, None);
}

pub(crate) fn native_batch_cache_get(
    tablet_root_path: &str,
    tablet_id: i64,
    version: i64,
    output_schema_signature: &str,
) -> Option<RecordBatch> {
    let cache = DataCacheManager::instance().page_cache()?;
    let key = native_batch_key(
        tablet_root_path,
        tablet_id,
        version,
        output_schema_signature,
    );
    cache.lookup::<RecordBatch>(&key).map(|v| (*v).clone())
}

pub(crate) fn native_batch_cache_put(
    tablet_root_path: &str,
    tablet_id: i64,
    version: i64,
    output_schema_signature: &str,
    batch: RecordBatch,
) {
    let Some(cache) = DataCacheManager::instance().page_cache() else {
        return;
    };
    let key = native_batch_key(
        tablet_root_path,
        tablet_id,
        version,
        output_schema_signature,
    );
    let _ = cache.insert(key, Arc::new(batch), DEFAULT_NATIVE_BATCH_CHARGE, None);
}

fn segment_footer_key(tablet_root_path: &str, tablet_id: i64, version: i64) -> DataCachePageKey {
    let mut key = [0u8; SEGMENT_FOOTER_KEY_SIZE];
    key[..2].copy_from_slice(&SEGMENT_FOOTER_PREFIX);
    key[2..10]
        .copy_from_slice(&hash64(normalize_path(tablet_root_path).as_bytes(), 0).to_le_bytes());
    key[10..18].copy_from_slice(&tablet_id.to_le_bytes());
    key[18..26].copy_from_slice(&version.to_le_bytes());
    DataCachePageKey::new(STARROCKS_CACHE_NAMESPACE, key.to_vec())
}

fn native_batch_key(
    tablet_root_path: &str,
    tablet_id: i64,
    version: i64,
    output_schema_signature: &str,
) -> DataCachePageKey {
    let signature_bytes = output_schema_signature.as_bytes();
    let mut key = Vec::with_capacity(26 + signature_bytes.len());
    key.extend_from_slice(&NATIVE_BATCH_PREFIX);
    key.extend_from_slice(&hash64(normalize_path(tablet_root_path).as_bytes(), 0).to_le_bytes());
    key.extend_from_slice(&tablet_id.to_le_bytes());
    key.extend_from_slice(&version.to_le_bytes());
    key.extend_from_slice(signature_bytes);
    DataCachePageKey::new(STARROCKS_CACHE_NAMESPACE, key)
}

fn normalize_path(path: &str) -> &str {
    path.trim().trim_end_matches('/')
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
