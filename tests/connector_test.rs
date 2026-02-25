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
//! Integration tests for connectors (JDBC, Iceberg).

use crate::common::TestConfig;
use arrow::array::Array;
use novarocks::cache::{CacheOptions, DataCacheManager};
use novarocks::common::ids::SlotId;
use novarocks::connector::{self, FileFormatConfig, ParquetScanConfig};
use novarocks::exec::node::scan::ScanOp;
use novarocks::formats::parquet::ParquetReadCachePolicy;
use novarocks::novarocks_connector_jdbc::{JdbcScanConfig, JdbcScanOp};
use novarocks::types;

mod common;

fn test_cache_options() -> CacheOptions {
    CacheOptions {
        enable_scan_datacache: false,
        enable_populate_datacache: false,
        enable_datacache_async_populate_mode: false,
        enable_datacache_io_adaptor: false,
        enable_cache_select: false,
        datacache_evict_probability: 0,
        datacache_priority: 0,
        datacache_ttl_seconds: 0,
        datacache_sharing_work_period: None,
    }
}

#[test]
fn test_connector_registry_exists() {
    // Test that connector registry module exists and can be accessed
    // This is a basic smoke test to ensure the module is properly exported
    let _registry = connector::ConnectorRegistry::default();
}

#[test]
fn test_connector_registry_initialization() {
    // Test connector registry initialization
    let registry = connector::ConnectorRegistry::default();

    // Registry should be initialized with default connectors
    // Default registry includes jdbc, mysql, hdfs, and starrocks connectors
    let _ = registry;
}

#[test]
fn test_connector_registry_new() {
    // Test creating a new empty registry
    let registry = connector::ConnectorRegistry::new();
    let _ = registry;
}

#[test]
fn test_jdbc_connector_module() {
    let cfg = novarocks::novarocks_connector_jdbc::JdbcScanConfig {
        jdbc_url: "jdbc:sqlite::memory:".to_string(),
        jdbc_user: None,
        jdbc_passwd: None,
        table: "lineorder".to_string(),
        columns: vec!["lo_orderkey".to_string()],
        filters: vec![],
        limit: Some(1),
        slot_ids: vec![novarocks::common::ids::SlotId::new(1)],
    };
    let _op = novarocks::novarocks_connector_jdbc::JdbcScanOp::new(cfg.clone());
    assert_eq!(cfg.table, "lineorder");
}

#[test]
fn test_iceberg_connector_module() {
    let parquet_cfg = ParquetScanConfig {
        columns: vec!["col0".to_string()],
        slot_ids: vec![novarocks::common::ids::SlotId::new(1)],
        slot_types: vec![types::TPrimitiveType::INT],
        case_sensitive: false,
        enable_page_index: false,
        min_max_predicates: vec![],
        batch_size: None,
        datacache: DataCacheManager::instance().external_context(test_cache_options()),
        cache_policy: ParquetReadCachePolicy::with_flags(false, false, None),
        profile_label: Some("connector_smoke".to_string()),
    };
    let config = novarocks::novarocks_connector_iceberg::HdfsScanConfig {
        ranges: vec![novarocks::connector::FileScanRange {
            path: "/tmp/data.parquet".to_string(),
            file_len: 0,
            offset: 0,
            length: 0,
            scan_range_id: -1,
            first_row_id: None,
            external_datacache: None,
        }],
        original_range_count: 1,
        has_more: false,
        limit: Some(10),
        profile_label: Some("connector_smoke".to_string()),
        format: Some(FileFormatConfig::Parquet(parquet_cfg)),
        object_store_config: None,
    };
    let _scan = novarocks::novarocks_connector_iceberg::HdfsScanOp::new(config.clone());
    assert_eq!(config.ranges.len(), 1);
}

#[test]
fn test_connector_config_loading() {
    let test_config = TestConfig::new().expect("Failed to create test config");
    let config = test_config.load_config().expect("Failed to load config");
    assert_eq!(config.server.host, "127.0.0.1");
}

#[test]
fn test_jdbc_sqlite_scan_full_table() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("test.db");
    let conn = rusqlite::Connection::open(&path).expect("open");
    conn.execute("CREATE TABLE t(a INTEGER)", [])
        .expect("create");
    conn.execute("INSERT INTO t(a) VALUES (1)", [])
        .expect("insert 1");
    conn.execute("INSERT INTO t(a) VALUES (2)", [])
        .expect("insert 2");

    let cfg = JdbcScanConfig {
        jdbc_url: format!("jdbc:sqlite:{}", path.to_string_lossy()),
        jdbc_user: None,
        jdbc_passwd: None,
        table: "t".to_string(),
        columns: vec!["a".to_string()],
        filters: vec![],
        limit: None,
        slot_ids: vec![SlotId::new(1)],
    };
    let op = JdbcScanOp::new(cfg);
    let iter = op
        .execute_iter(
            novarocks::exec::node::scan::ScanMorsel::JdbcSingle,
            None,
            None,
        )
        .expect("scan");
    let mut values = Vec::new();
    for chunk in iter {
        let chunk = chunk.expect("chunk");
        assert_eq!(chunk.columns().len(), 1);
        let col_ref = chunk.column_by_slot_id(SlotId::new(1)).expect("a column");
        let col = col_ref
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("int64");
        for i in 0..col.len() {
            values.push(if col.is_null(i) {
                None
            } else {
                Some(col.value(i))
            });
        }
    }
    assert_eq!(values, vec![Some(1), Some(2)]);
}
