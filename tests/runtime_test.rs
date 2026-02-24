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
//! Integration tests for runtime components (exchange, query context, etc.).

use std::time::Duration;

use crate::common::{TestConfig, test_query_id};
use novarocks::common::types::UniqueId;
use novarocks::runtime::exchange::{self, ExchangeKey};
use novarocks::runtime::exchange_scan::ExchangeScanOp;
use novarocks::runtime::profile::Profiler;

mod common;

fn sample_exchange_key(node_id: i32) -> ExchangeKey {
    ExchangeKey {
        finst_id_hi: 42,
        finst_id_lo: 24,
        node_id,
    }
}

#[test]
fn test_runtime_module_structure() {
    let key = sample_exchange_key(1);
    exchange::set_expected_senders(key, 2);
    exchange::cancel_fragment(key.finst_id_hi, key.finst_id_lo);
}

#[test]
fn test_exchange_module() {
    let key_a = sample_exchange_key(2);
    let key_b = sample_exchange_key(2);
    assert_eq!(key_a, key_b);
}

#[test]
fn test_profile_module() {
    let profiler = Profiler::new("runtime_profile_smoke");
    let tree = profiler.to_thrift_tree();
    assert_eq!(tree.nodes[0].name, "runtime_profile_smoke");
}

#[test]
fn test_exchange_scan_module() {
    let key = sample_exchange_key(3);
    let scan = ExchangeScanOp::new(key, 0, Duration::from_millis(1));
    // execute_iter would block without registered senders; we only ensure constructor works.
    let _scan = scan;
}

#[test]
fn test_unique_id_in_runtime() {
    // Test UniqueId usage in runtime context
    let query_id: UniqueId = test_query_id();

    // Verify UniqueId can be used
    assert_eq!(query_id.hi, 1234567890);
    assert_eq!(query_id.lo, 9876543210);
}

#[test]
fn test_runtime_config_loading() {
    // Test that runtime configuration can be loaded
    let test_config = TestConfig::new().expect("Failed to create test config");
    let config = test_config.load_config().expect("Failed to load config");

    // Verify runtime configuration
    assert!(config.runtime.exchange_wait_ms > 0);
    assert!(config.runtime.local_exchange_buffer_mem_limit_per_driver > 0);
    assert!(config.runtime.operator_buffer_chunks > 0);
}

#[test]
fn test_cache_config() {
    // Test cache configuration
    let test_config = TestConfig::new().expect("Failed to create test config");
    let config = test_config.load_config().expect("Failed to load config");

    // Verify cache configuration exists
    let _cache_config = &config.runtime.cache;
}
