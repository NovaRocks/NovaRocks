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

/// Backend debug and test switches are supplied by the process environment, not the
/// application config.
///
/// These knobs are read from deep inside connector, decoder and operator code
/// that has no access to a composition-time config value, and they are owned by
/// whoever launched the process (a developer or the SQL test runner). Routing
/// them through the application config would force those paths to reach for a
/// process-global singleton. Every switch is compiled out of release builds,
/// and `app_config` refuses the variables there so a release binary cannot
/// silently ignore one.
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

pub fn debug_fault_inject_fetch_not_ready_count() -> Option<usize> {
    if !cfg!(debug_assertions) {
        return None;
    }
    std::env::var("NOVAROCKS_SQL_TEST_FAULT_INJECT_FETCH_NOT_READY_COUNT")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|count| *count > 0)
}

pub fn debug_emit_grpc_fragment_marker() -> bool {
    debug_env_flag("NOVAROCKS_SQL_TEST_EMIT_GRPC_FRAGMENT_MARKER")
        || sql_test_fragment_failure_harness_enabled()
}

/// Returns whether execution should emit connector-reader evidence markers.
/// Backend native plan decoding uses this without accessing runtime state.
pub fn debug_emit_connector_reader_marker() -> bool {
    debug_env_flag("NOVAROCKS_SQL_TEST_EMIT_CONNECTOR_READER_MARKER")
}

/// Returns whether execution should emit connector-writer evidence markers.
///
/// It is a separate switch from the reader marker rather than the same one:
/// the two prove different halves of a distributed write, and a case asserting
/// that a writer opened on a backend must not be satisfied by a page source
/// having opened there.
pub fn debug_emit_connector_writer_marker() -> bool {
    debug_env_flag("NOVAROCKS_SQL_TEST_EMIT_CONNECTOR_WRITER_MARKER")
}

/// Returns whether catalog runtime materialization emits a test-only marker.
///
/// The marker carries only the immutable catalog handle and is compiled out of
/// release builds, so it cannot expose configuration material in production.
pub fn debug_emit_catalog_materialization_marker() -> bool {
    debug_env_flag("NOVAROCKS_SQL_TEST_EMIT_CATALOG_MATERIALIZATION_MARKER")
}

/// Returns the runner-owned hold file for catalog installation.
///
/// A non-empty CatalogSet waits before any provider materialization while the
/// configured path exists. This is a debug-only cross-process test rendezvous:
/// the path is intentionally not logged, because its parent directories are
/// runner-private and carry no lifecycle evidence.
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

/// Returns the runner-owned trigger file for one injected catalog-install
/// failure. The runner places this only in one selected Backend's environment,
/// which lets the native control barrier exercise a real partial failure.
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

/// Returns whether the debug-only catalog lifecycle test evidence is enabled.
///
/// The markers contain only query identity, backend identity, and catalog
/// count. They never include catalog properties or the hold-file path.
pub fn debug_emit_catalog_lifecycle_marker() -> bool {
    debug_env_flag("NOVAROCKS_SQL_TEST_EMIT_CATALOG_LIFECYCLE_MARKER")
}

pub(crate) fn sql_test_fragment_failure_harness_enabled() -> bool {
    std::env::var_os("NOVAROCKS_SQL_TEST_FRAGMENT_FAILURE_TRIGGER_FILE").is_some()
}
