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
pub(crate) mod attempt_plan_facts;
pub(crate) mod attempt_runtime_filter_facts;
pub(crate) mod cohort_read;
pub mod completion;
pub(crate) mod completion_facts;
pub(crate) mod connector_domain;
pub mod constant_eval;
pub mod contract;
mod core_bindings;
#[cfg(test)]
mod dependency_artifact_records;
#[cfg(test)]
mod dependency_artifact_storage;
pub mod distributed_rewrite;
pub mod dml;
pub mod fragment_scheduling;
pub(crate) mod internal_consumer;
pub(crate) mod internal_result_cpu;
pub mod kernels;
pub(crate) mod lifecycle_diagnostics;
pub mod lifecycle_plan;
pub(crate) mod logical_execution;
#[cfg(test)]
pub(crate) mod logical_execution_test_profile;
pub mod maintenance;
pub mod mv_assembly;
pub mod mv_native_write;
pub(crate) mod native_execution_adapter;
pub mod native_fragment;
pub(crate) mod original_cow_result_scope;
pub(crate) mod outcome;
pub(crate) mod package_freeze;
pub(crate) mod package_reads;
pub(crate) mod package_writes;
pub(crate) mod physical_encoding;
pub(crate) mod pinned_connector_read;
pub mod planning;
pub mod post_compile;
/// Completed read access and runtime-filter deployment facts.
pub mod preparation;
pub(crate) mod profile;
pub(crate) mod provider_read_facts;
pub(crate) mod rewrite_group_read;
pub(crate) mod row_mutation;
pub(crate) mod runtime_filter_terminal_rollup;
pub(crate) mod schedule;
pub(crate) mod split_assignment;
pub(crate) mod split_assignment_round;
pub mod statistics;
pub(crate) mod write_result;
pub(crate) mod write_session;

pub mod compiler;
#[cfg(test)]
mod tests;
