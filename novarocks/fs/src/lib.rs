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

//! Connector-neutral file access and columnar physical decoding.
//!
//! This crate deliberately has no connector identity and owns no table-format
//! correctness. Connector implementations bind authorized storage access and
//! ask this crate to read physical Parquet or ORC columns.

mod access;
mod cache;
mod error;
mod physical_reader;
mod predicate;
mod read;
mod runtime;

pub use access::{
    BoundFile, FileIdentity, FsAccessHandle, FsAccessResolver, FsLocation, FsScheme,
    ObjectStoreConfig, ResolvedFsPath, is_object_store_location_parse_only,
    parse_object_store_path_parse_only,
};
pub use cache::{
    BlockCache, BlockCacheOptions, CacheBlockRead, CacheDomain, CacheInputStream, CacheKey,
    CacheOptions, DataCacheContext, DataCacheIoOptions, DataCacheManager, DataCacheMetricsRecorder,
    DataCachePageCache, DataCachePageCacheOptions, DataCachePageKey, ExternalDataCacheRangeOptions,
    PageCache, PageCacheStats, PageCacheValue, PageHandle, ParquetCacheOptions, get_block_cache,
    init_block_cache, init_parquet_cache, validate_datacache_priority, validate_evict_probability,
    validate_non_negative_i64,
};
pub use error::{FileError, FileErrorKind, FileResult};
pub use physical_reader::{
    MAX_PARQUET_INSPECTION_PHYSICAL_COLUMNS, MAX_PARQUET_INSPECTION_ROW_GROUPS,
    MAX_PARQUET_INSPECTION_STATISTIC_CELLS, MAX_PARQUET_INSPECTION_STATISTIC_VALUE_BYTES,
    ParquetColumnStatistics, ParquetMetadataInspection, ParquetPhysicalColumn, ParquetPhysicalType,
    ParquetRowGroupLayout, ParquetStatisticsSortOrder, ParquetStatisticsValue,
    inspect_parquet_metadata, open_file_reader,
};
pub use predicate::{
    MinMaxPredicateOp, MinMaxPredicateValue, PhysicalPageSelection, PhysicalPruning, ScanPredicate,
    ScanPredicateDomain, ScanPredicateSource,
};
pub use read::{
    FileBatch, FileBatchReader, FileFormat, FileMetricsSnapshot, FileProjection, FileReadBudget,
    FileReadContext, FileReadRange, FileReadRequest,
};
pub use runtime::{
    FileBytesFuture, FileCancellation, FileIoRuntime, FileTask, FileTaskFuture, FileTaskSpawner,
    FileU64Future, TokioFileIoRuntime, TokioFileTaskSpawner,
};

// Design: ADR-0014 (docs/adr/ADR-0014-connector-neutral-file-foundation.md)
