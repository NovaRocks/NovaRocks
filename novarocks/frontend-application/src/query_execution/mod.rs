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

pub mod artifact;
/// Carrier-neutral payload validation and patch helpers consumed by the
/// Frontend-owned native submission mapper.
pub mod assembly;
pub(crate) mod attempt_initialization;
pub mod completion;
// MIGRATION: the typed-scan lowering that consumes these lands in the same PR.
#[allow(
    dead_code,
    unused_imports,
    reason = "Consumed by the frontend typed-scan lowering in the same PR."
)]
pub(crate) mod connector_domain;
pub mod constant_eval;
pub mod contract;
mod core_bindings;
pub mod distributed_rewrite;
pub mod dml;
pub mod kernels;
pub(crate) mod lifecycle_diagnostics;
pub mod lifecycle_plan;
pub(crate) mod logical_read;
pub mod maintenance;
pub mod mv_assembly;
pub mod mv_native_write;
pub(crate) mod native_execution_adapter;
pub mod native_fragment;
pub(crate) mod outcome;
pub(crate) mod pinned_connector_read;
pub mod planning;
pub mod post_compile;
/// Sealed preparation carriers consumed by the native Frontend encoder.
pub mod preparation;
pub(crate) mod profile;
pub(crate) mod rewrite_group_read;
pub(crate) mod row_mutation;
pub(crate) mod runtime_filter_terminal_rollup;
pub(crate) mod schedule;
pub mod service;
// MIGRATION: the typed-scan lowering that consumes these lands in the same PR.
#[allow(
    dead_code,
    unused_imports,
    reason = "Consumed by the frontend typed-scan lowering in the same PR."
)]
pub(crate) mod split_assignment;
pub(crate) mod split_assignment_round;
pub mod statistics;
pub(crate) mod write_barrier;
pub(crate) mod write_result;
pub(crate) mod write_session;
pub(crate) mod write_transaction;

pub mod compiler;
#[cfg(test)]
mod tests;
