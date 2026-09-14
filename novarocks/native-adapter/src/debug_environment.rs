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

//! Process-owned debug and test environment switches for native adapters.
//!
//! These knobs are supplied by the process launcher, not role application
//! configuration. They are compiled out of release builds, and `app_config`
//! rejects their environment variables there so a release binary cannot
//! silently ignore one.

#[cfg(debug_assertions)]
fn debug_env_flag(name: &str) -> bool {
    std::env::var_os(name).is_some()
}

#[cfg(not(debug_assertions))]
fn debug_env_flag(_name: &str) -> bool {
    false
}

pub fn debug_exec_node_output() -> bool {
    debug_env_flag("NOVAROCKS_DEBUG_EXEC_NODE_OUTPUT")
}

pub fn debug_emit_grpc_fragment_marker() -> bool {
    debug_env_flag("NOVAROCKS_SQL_TEST_EMIT_GRPC_FRAGMENT_MARKER")
        || sql_test_fragment_failure_harness_enabled()
}

/// Returns whether execution should emit connector-reader evidence markers.
pub fn debug_emit_connector_reader_marker() -> bool {
    debug_env_flag("NOVAROCKS_SQL_TEST_EMIT_CONNECTOR_READER_MARKER")
}

/// Returns whether execution should emit connector-writer evidence markers.
pub fn debug_emit_connector_writer_marker() -> bool {
    debug_env_flag("NOVAROCKS_SQL_TEST_EMIT_CONNECTOR_WRITER_MARKER")
}

/// Returns whether catalog runtime materialization emits a test-only marker.
pub fn debug_emit_catalog_materialization_marker() -> bool {
    debug_env_flag("NOVAROCKS_SQL_TEST_EMIT_CATALOG_MATERIALIZATION_MARKER")
}

/// Returns the runner-owned hold file for catalog installation.
#[cfg(debug_assertions)]
pub fn debug_catalog_install_hold_file() -> Option<std::path::PathBuf> {
    std::env::var_os("NOVAROCKS_SQL_TEST_CATALOG_INSTALL_HOLD_FILE")
        .filter(|path| !path.is_empty())
        .map(std::path::PathBuf::from)
}

#[cfg(not(debug_assertions))]
pub fn debug_catalog_install_hold_file() -> Option<std::path::PathBuf> {
    None
}

/// Returns the runner-owned trigger file for one injected catalog-install failure.
#[cfg(debug_assertions)]
pub fn debug_catalog_install_failure_file() -> Option<std::path::PathBuf> {
    std::env::var_os("NOVAROCKS_SQL_TEST_CATALOG_INSTALL_FAILURE_FILE")
        .filter(|path| !path.is_empty())
        .map(std::path::PathBuf::from)
}

#[cfg(not(debug_assertions))]
pub fn debug_catalog_install_failure_file() -> Option<std::path::PathBuf> {
    None
}

/// Returns whether debug-only catalog lifecycle evidence is enabled.
pub fn debug_emit_catalog_lifecycle_marker() -> bool {
    debug_env_flag("NOVAROCKS_SQL_TEST_EMIT_CATALOG_LIFECYCLE_MARKER")
}

pub fn sql_test_fragment_failure_harness_enabled() -> bool {
    std::env::var_os("NOVAROCKS_SQL_TEST_FRAGMENT_FAILURE_TRIGGER_FILE").is_some()
}
