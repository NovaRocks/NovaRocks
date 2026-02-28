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
use std::io;

use bytes::Bytes;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::reader::{ChunkReader, Length};

use crate::cache::CachedRangeReader;

use super::ParquetReadCachePolicy;
use super::cache::{parquet_page_cache_get, parquet_page_cache_put};

#[derive(Clone, Debug)]
pub struct ParquetCachedReader {
    inner: CachedRangeReader,
    cache_policy: ParquetReadCachePolicy,
}

impl ParquetCachedReader {
    pub fn new(inner: CachedRangeReader, cache_policy: ParquetReadCachePolicy) -> Self {
        Self {
            inner,
            cache_policy,
        }
    }

    fn get_bytes_impl(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        let end = start
            .checked_add(length as u64)
            .ok_or_else(|| ParquetError::General("range overflow".to_string()))?;
        if start > self.len() || end > self.len() {
            return Err(ParquetError::EOF(format!(
                "Expected to read {} bytes at offset {}, while file has length {}",
                length,
                start,
                self.len()
            )));
        }

        if let Some(cached_data) = parquet_page_cache_get(
            self.cache_policy.enable_pagecache,
            self.inner.file_identity(),
            start,
            length,
        ) {
            let counters = self.inner.counters();
            if let Some(datacache) = self.inner.datacache() {
                datacache.record_page_cache_hit(counters.as_ref(), length);
            }
            return Ok(cached_data);
        }

        let bytes = self
            .inner
            .read_bytes(start, length)
            .map_err(|e| match e.kind() {
                io::ErrorKind::UnexpectedEof => ParquetError::EOF(e.to_string()),
                _ => ParquetError::General(e.to_string()),
            })?;

        if self.cache_policy.should_cache_page_read(length) {
            parquet_page_cache_put(
                self.cache_policy.enable_pagecache,
                self.inner.file_identity(),
                start,
                length,
                bytes.clone(),
                self.cache_policy.page_cache_evict_probability,
            );
        }
        Ok(bytes)
    }
}

impl Length for ParquetCachedReader {
    fn len(&self) -> u64 {
        self.inner.file_len()
    }
}

impl ChunkReader for ParquetCachedReader {
    type T = crate::cache::CachedRead;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        self.inner.open_read(start).map_err(|e| match e.kind() {
            io::ErrorKind::UnexpectedEof => ParquetError::EOF(e.to_string()),
            _ => ParquetError::General(e.to_string()),
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        self.get_bytes_impl(start, length)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use parquet::file::reader::ChunkReader;

    use crate::cache::{DataCacheManager, DataCachePageCacheOptions};
    use crate::fs::opendal::{OpendalRangeReaderFactory, build_fs_operator};
    use crate::metrics;
    use crate::runtime::profile::RuntimeProfile;

    use super::{ParquetCachedReader, ParquetReadCachePolicy};

    #[test]
    fn get_bytes_uses_page_cache_after_first_fetch() {
        let _ = DataCacheManager::instance().init_page_cache(DataCachePageCacheOptions {
            capacity: 64,
            evict_probability: 100,
        });

        let temp_dir = tempfile::tempdir().expect("tempdir");
        fs::write(temp_dir.path().join("sample.bin"), vec![b'x'; 2048]).expect("write fixture");

        let profile = RuntimeProfile::new("parquet_cached_reader_test");
        let op = build_fs_operator(temp_dir.path().to_str().expect("temp dir path"))
            .expect("build fs operator");
        let factory = OpendalRangeReaderFactory::from_operator(op)
            .expect("reader factory")
            .with_profile(Some(profile.clone()));
        let reader = factory
            .open_with_len("sample.bin", Some(2048))
            .expect("open with len");
        let reader = ParquetCachedReader::new(
            crate::cache::CachedRangeReader::new(reader, None),
            ParquetReadCachePolicy::with_flags(true, true, Some(100)),
        );

        let first = reader.get_bytes(0, 1024).expect("first page-cache read");
        let read_requests = profile.add_counter("ReadRequests", metrics::TUnit::UNIT);
        assert_eq!(first.len(), 1024);
        assert_eq!(read_requests.value(), 1);

        let second = reader.get_bytes(0, 1024).expect("second page-cache read");
        assert_eq!(second.len(), 1024);
        assert_eq!(read_requests.value(), 1);
    }
}
