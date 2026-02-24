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
use bytes::Bytes;
use std::io;
use std::sync::Arc;
use std::time::Instant;

use crate::cache::block_cache::{BlockCache, CacheKey};

pub struct CacheInputStream {
    cache: Arc<BlockCache>,
    cache_key: CacheKey,
    enable_cache: bool,
    enable_populate: bool,
    async_populate: bool,
}

pub struct CacheBlockRead {
    pub bytes: Bytes,
    pub hit: bool,
    pub cache_read_ns: u128,
    pub cache_write_bytes: usize,
    pub cache_write_ns: u128,
}

impl CacheInputStream {
    pub fn new(
        cache: Arc<BlockCache>,
        path: String,
        modification_time: Option<i64>,
        file_size: u64,
        enable_cache: bool,
        enable_populate: bool,
        async_populate: bool,
    ) -> Self {
        let cache_key = CacheKey::from_path(&path, modification_time, file_size);
        Self {
            cache,
            cache_key,
            enable_cache,
            enable_populate,
            async_populate,
        }
    }

    pub fn block_size(&self) -> u64 {
        self.cache.block_size()
    }

    pub fn read_block<F>(
        &self,
        block_id: u64,
        block_len: usize,
        fetch: F,
    ) -> io::Result<CacheBlockRead>
    where
        F: FnOnce() -> io::Result<Bytes>,
    {
        if self.enable_cache {
            let cache_read_start = Instant::now();
            if let Some(bytes) = self.cache.read(self.cache_key, block_id, block_len) {
                return Ok(CacheBlockRead {
                    bytes,
                    hit: true,
                    cache_read_ns: cache_read_start.elapsed().as_nanos(),
                    cache_write_bytes: 0,
                    cache_write_ns: 0,
                });
            }
        }
        let bytes = fetch()?;
        let mut cache_write_bytes = 0usize;
        let mut cache_write_ns = 0u128;
        if self.enable_populate {
            let cache_write_start = Instant::now();
            if self
                .cache
                .write(self.cache_key, block_id, bytes.clone(), self.async_populate)
                .is_ok()
            {
                cache_write_bytes = bytes.len();
                cache_write_ns = cache_write_start.elapsed().as_nanos();
            }
        }
        Ok(CacheBlockRead {
            bytes,
            hit: false,
            cache_read_ns: 0,
            cache_write_bytes,
            cache_write_ns,
        })
    }
}
