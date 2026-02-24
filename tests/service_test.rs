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
//! Integration tests for service layer (internal_service).

use crate::common::{TestConfig, test_query_id, unique_query_id};
use novarocks::common::types::UniqueId;
use novarocks::service::internal_service;

mod common;

#[test]
fn test_service_module_initialization() {
    // Test that service modules can be imported and used
    let query_id = test_query_id();
    assert_eq!(query_id.hi, 1234567890);
    assert_eq!(query_id.lo, 9876543210);
}

#[test]
fn test_unique_id_creation() {
    // Test UniqueId creation and comparison
    let id1 = UniqueId { hi: 1, lo: 2 };
    let id2 = UniqueId { hi: 1, lo: 2 };
    let id3 = UniqueId { hi: 1, lo: 3 };

    assert_eq!(id1, id2);
    assert_ne!(id1, id3);
}

#[test]
fn test_config_loading() {
    // Test that test configuration can be loaded
    let test_config = TestConfig::new().expect("Failed to create test config");
    test_config.init_logging();

    let config = test_config.load_config().expect("Failed to load config");
    assert_eq!(config.server.heartbeat_port, 9050);
    assert_eq!(config.server.brpc_port, 9060);
}

#[test]
fn test_cancel_with_invalid_query_id() {
    // Test cancel with a non-existent query ID
    // This should not panic, even if the query doesn't exist
    let query_id = test_query_id();
    let result = internal_service::cancel(query_id);

    // Cancel should not fail even if query doesn't exist
    // The exact behavior depends on implementation
    // For now, we just verify it doesn't panic
    let _ = result;
}

#[test]
fn test_query_id_uniqueness() {
    // Test that unique query IDs are generated correctly
    let id1 = unique_query_id("test1");
    let id2 = unique_query_id("test2");
    let id3 = unique_query_id("test1");

    assert_ne!(id1, id2);
    assert_eq!(id1, id3); // Same test name should produce same ID
}
