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

//! Native backend adapters for the Worker-owned task protocol.
//!
//! Worker owns Context/Task cells, pure protocol decisions, covered observation
//! and original admission/exit custody. This module supplies role-local
//! execution hosts; Native DTO decoding and wire projection remain in the
//! sibling Worker Node ingress.
//!
//! ```text
//! Native RPC -> WorkerTaskControl -> WorkerNodeHost
//!                                  |-- original Context/Task cells and Core
//!                                  `-- supervised Native execution host ports
//! ```
//!
//! Host callbacks report physical facts through their original Worker roles.
//! They do not create another task registry or status owner.
// Design: ADR-0161 (docs/adr/ADR-0161-pure-local-programs-and-supervised-task-lifecycle.md)

mod compiled_package;
mod context_host;
pub(crate) mod execution_host;
mod preparation_memory_control;
#[cfg(novarocks_dev_controls)]
mod prepared_exit_control;
pub(crate) mod prepared_owner;
mod project_materialization;
#[cfg(novarocks_dev_controls)]
mod task_update_replay_gate;
mod type_materialization;

#[cfg(test)]
pub(crate) mod tests;

pub use compiled_package::{
    CompiledPackageCompiler, CompiledPackageError, CompiledPackageInterpreter, CompiledTaskOptions,
    CompiledTaskProgram, CompiledTypeScope, CompiledTypeScopes, NativeTypeConstructionError,
};
pub use context_host::NativeQueryContextHost;
pub use execution_host::{NativeTaskExecutionHost, TaskQueryContextFacts};

#[cfg(any(test, novarocks_dev_controls))]
#[doc(hidden)]
#[path = "node_fixture.rs"]
pub mod node_test_support;

pub use type_materialization::{
    TypeMaterializationHost, TypeMaterializationJournal, TypeMaterializationRefusal,
};

#[cfg(any(test, novarocks_dev_controls))]
pub use type_materialization::materialize_package_types_for_test;

pub use project_materialization::{
    ProjectMetadataHost, ProjectMetadataJournal, ProjectMetadataPhase,
};

#[cfg(any(test, novarocks_dev_controls))]
mod project_metadata_test_support;
#[cfg(any(test, novarocks_dev_controls))]
pub use project_metadata_test_support::{
    PreparedProjectMetadataForTest, prepare_project_metadata_for_test,
};

#[cfg(any(test, novarocks_dev_controls))]
mod window_metadata_test_support;
#[cfg(any(test, novarocks_dev_controls))]
pub use window_metadata_test_support::{
    PreparedWindowMetadataForTest, PreparedWindowMetadataGraphForTest,
    WindowSchemaOccurrenceForTest, prepare_window_metadata_for_test,
    prepare_window_metadata_graph_for_test,
};

#[cfg(any(test, novarocks_dev_controls))]
pub use compiled_package::compile_native_package_with_funding_for_test;

#[cfg(any(test, novarocks_dev_controls))]
pub use compiled_package::with_native_type_construction_for_test;
