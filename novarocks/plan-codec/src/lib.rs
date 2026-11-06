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

//! Deterministic final-physical-plan-to-protobuf encoding for the native FE/BE
//! boundary. The encoder consumes a completed physical plan and exact frozen
//! provider facts. It has no SQL compiler or Frontend runtime dependency.

mod allocation_exit_v2;
mod arrow_metadata_v2;
mod binding_index_v2;
mod borrowed_type_resources;
mod btree_resources_v2;
pub mod execution_type_table;
mod failure_projection;
mod hashmap_resources_v2;
pub mod host_projection_v2;
pub mod ipc_flat_batch_v2;
pub mod ipc_flat_pool_v2;
pub mod ipc_flat_stream_v2;
pub(crate) mod ipc_recursive_batch_v2;
pub mod ipc_recursive_pool_v2;
pub mod ipc_recursive_stream_v2;
pub mod ipc_schema_v2;
mod library_profile_v2;
pub mod physical_aggregate_binding_v2;
pub mod physical_aggregate_node_v2;
pub mod physical_assert_rows_v2;
pub mod physical_binding_v2;
pub mod physical_call_requests_v2;
pub mod physical_change_event_v2;
pub mod physical_connector_payload_v2;
pub mod physical_constant_v2;
pub mod physical_control_v2;
pub mod physical_cuts_v2;
pub mod physical_expression_v2;
pub(crate) mod physical_fragment_envelope_v2;
pub mod physical_node_v2;
pub mod physical_package_metadata_v2;
pub mod physical_package_v2;
pub(crate) mod physical_summary_v2;
pub use physical_package_v2::PhysicalEncodeError;
pub mod physical_properties_v2;
pub mod physical_provider_binding_v2;
pub mod physical_provider_read_v2;
pub mod physical_public_source_schema_v2;
pub mod physical_read_scan_v2;
pub mod physical_relation_v2;
pub mod physical_relational_nodes_v2;
pub mod physical_repeat_v2;
pub(crate) mod physical_result_v2;
pub mod physical_scan_v2;
pub mod physical_schema_v2;
pub mod physical_semantics_v2;
pub mod physical_simple_nodes_v2;
pub mod physical_table_function_node_v2;
pub mod physical_table_write_nodes_v2;
pub mod physical_topn_node_v2;
pub mod physical_type_v2;
pub mod physical_unpivot_v2;
pub mod physical_value_origin_v2;
pub mod physical_value_v2;
pub mod physical_writer_grouped_unpivot_v2;
pub mod physical_writer_recipe_v2;
pub mod physical_writer_schema_v2;
pub mod resource_preflight_v2;
mod resource_source_model;
mod runtime_filter_facts;
#[cfg(test)]
mod runtime_filter_test_support;
mod sealed_write_targets;
pub use runtime_filter_facts::runtime_filter_comparator_digest;
pub mod value_type_table;

pub use sealed_write_targets::SealedWriteTargets;

// Refuse flags that would make capability reporting disagree with compiled seams.
#[cfg(any(
    all(novarocks_dev_controls, not(debug_assertions)),
    all(not(novarocks_dev_controls), debug_assertions)
))]
compile_error!("novarocks_dev_controls must match debug_assertions");
