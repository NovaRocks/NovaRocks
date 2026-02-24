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
//! Integration tests for execution engine.

use crate::common::TestConfig;

mod common;

fn assert_type_accessible<T: 'static>() {
    let type_name = std::any::type_name::<T>();
    assert!(
        !type_name.is_empty(),
        "type name for {} should not be empty",
        std::any::type_name::<T>()
    );
}

#[test]
fn test_exec_module_structure() {
    assert_type_accessible::<novarocks::exec::chunk::Chunk>();
}

#[test]
fn test_chunk_module() {
    let chunk = novarocks::exec::chunk::Chunk::default();
    assert_eq!(chunk.len(), 0);
}

#[test]
fn test_expr_module() {
    assert_type_accessible::<novarocks::exec::expr::ExprArena>();
}

#[test]
fn test_pipeline_module() {
    assert_type_accessible::<novarocks::exec::pipeline::builder::PipelineGraph>();
}

#[test]
fn test_node_module() {
    assert_type_accessible::<novarocks::exec::node::ExecNode>();
}

#[test]
fn test_operators_module() {
    assert_type_accessible::<novarocks::exec::operators::ScanSourceFactory>();
}

#[test]
fn test_exec_config_loading() {
    // Test that execution-related configuration can be loaded
    let test_config = TestConfig::new().expect("Failed to create test config");
    let config = test_config.load_config().expect("Failed to load config");

    // Verify runtime configuration exists
    assert!(config.runtime.exchange_wait_ms > 0);
    assert!(config.runtime.local_exchange_buffer_mem_limit_per_driver > 0);
    assert!(config.runtime.operator_buffer_chunks > 0);
}
