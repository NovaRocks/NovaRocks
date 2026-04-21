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
//! Common utilities and helpers for integration tests.
#![allow(dead_code)]
#![allow(unused_imports)]

use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;

use novarocks::common::types::UniqueId;
use novarocks::novarocks_config;
use novarocks::novarocks_logging;

/// Test configuration for integration tests.
pub struct TestConfig {
    /// Temporary directory for test artifacts
    pub temp_dir: TempDir,
    /// Test config path
    pub config_path: PathBuf,
}

impl TestConfig {
    /// Create a new test configuration with default settings.
    pub fn new() -> anyhow::Result<Self> {
        let temp_dir = tempfile::tempdir()?;
        let config_path = temp_dir.path().join("test_novarocks.toml");

        // Create a minimal test config
        let config_content = r#"
[server]
host = "127.0.0.1"
heartbeat_port = 9050
brpc_port = 9060
http_port = 8040
exchange_port = 9070

[runtime]
exchange_wait_ms = 5000

[runtime.cache]
parquet_meta_cache_enable = false
parquet_page_cache_enable = false
parquet_meta_cache_capacity = 1000
parquet_meta_cache_ttl_seconds = 3600
parquet_page_cache_capacity = 1000
parquet_page_cache_ttl_seconds = 3600

[debug]
exec_node_output = false
exec_batch_plan_json = false
"#;

        std::fs::write(&config_path, config_content)?;

        Ok(Self {
            temp_dir,
            config_path,
        })
    }

    /// Initialize logging for tests.
    pub fn init_logging(&self) {
        novarocks_logging::init_with_level("debug");
    }

    /// Load the test configuration.
    pub fn load_config(&self) -> anyhow::Result<&'static novarocks_config::NovaRocksConfig> {
        novarocks_config::init_from_path(&self.config_path)
    }
}

impl Default for TestConfig {
    fn default() -> Self {
        Self::new().expect("Failed to create test config")
    }
}

/// Generate a test query ID.
pub fn test_query_id() -> UniqueId {
    UniqueId {
        hi: 1234567890,
        lo: 9876543210,
    }
}

/// Generate a unique query ID based on test name.
pub fn unique_query_id(test_name: &str) -> UniqueId {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    test_name.hash(&mut hasher);
    let hash = hasher.finish();

    UniqueId {
        hi: hash as i64,
        lo: (hash >> 32) as i64,
    }
}

/// Wait for a condition to become true, with timeout.
pub fn wait_for<F>(mut condition: F, timeout: Duration) -> bool
where
    F: FnMut() -> bool,
{
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if condition() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    false
}

/// Run `f` and fail the test if it does not finish within `timeout`.
pub fn run_with_timeout<F, T>(timeout: Duration, f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    use std::sync::mpsc;

    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });

    match rx.recv_timeout(timeout) {
        Ok(v) => v,
        Err(_) => panic!("test timed out after {:?}", timeout),
    }
}

/// Assert that a result is Ok and return the value.
#[macro_export]
macro_rules! assert_ok {
    ($result:expr) => {
        match $result {
            Ok(value) => value,
            Err(e) => panic!("Expected Ok, got Err: {:?}", e),
        }
    };
    ($result:expr, $message:expr) => {
        match $result {
            Ok(value) => value,
            Err(e) => panic!("{}: {:?}", $message, e),
        }
    };
}

/// Assert that a result is Err.
#[macro_export]
macro_rules! assert_err {
    ($result:expr) => {
        match $result {
            Ok(value) => panic!("Expected Err, got Ok: {:?}", value),
            Err(e) => e,
        }
    };
}

// ---------------------------------------------------------------------------
// Function-test helpers (used by tests/function_*.rs integration tests)
// ---------------------------------------------------------------------------

/// Create a single-row Chunk for use in function unit tests.
pub fn chunk_len_1() -> novarocks::exec::chunk::Chunk {
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks::common::ids::SlotId;
    use novarocks::exec::chunk::ChunkSchema;
    use std::sync::Arc;

    let array = Arc::new(Int64Array::from(vec![1])) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "dummy",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
    let chunk_schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[SlotId::new(1)])
            .expect("chunk schema");
    novarocks::exec::chunk::Chunk::new_with_chunk_schema(batch, chunk_schema)
}

/// Push an Int64 literal into the arena and return its ExprId.
pub fn literal_i64(
    arena: &mut novarocks::exec::expr::ExprArena,
    v: i64,
) -> novarocks::exec::expr::ExprId {
    use novarocks::exec::expr::{ExprNode, LiteralValue};
    arena.push(ExprNode::Literal(LiteralValue::Int64(v)))
}

/// Push a Float64 literal into the arena and return its ExprId.
pub fn literal_f64(
    arena: &mut novarocks::exec::expr::ExprArena,
    v: f64,
) -> novarocks::exec::expr::ExprId {
    use novarocks::exec::expr::{ExprNode, LiteralValue};
    arena.push(ExprNode::Literal(LiteralValue::Float64(v)))
}

/// Push a UTF-8 string literal into the arena and return its ExprId.
pub fn literal_string(
    arena: &mut novarocks::exec::expr::ExprArena,
    v: &str,
) -> novarocks::exec::expr::ExprId {
    use novarocks::exec::expr::{ExprNode, LiteralValue};
    arena.push(ExprNode::Literal(LiteralValue::Utf8(v.to_string())))
}

/// Push a typed NULL literal into the arena and return its ExprId.
pub fn typed_null(
    arena: &mut novarocks::exec::expr::ExprArena,
    data_type: arrow::datatypes::DataType,
) -> novarocks::exec::expr::ExprId {
    use novarocks::exec::expr::{ExprNode, LiteralValue};
    arena.push_typed(ExprNode::Literal(LiteralValue::Null), data_type)
}
