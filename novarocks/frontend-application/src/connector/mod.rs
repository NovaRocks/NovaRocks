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

pub mod backend;
pub mod cleanup_maintenance;
pub mod data_mutation;
pub mod metadata_maintenance;
pub mod mutation;
pub mod unified_statistics;
pub mod write_target;

mod application;
pub(crate) mod schema_creation;

#[cfg(test)]
pub mod fixture;
#[cfg(test)]
pub mod scan_model;

pub use application::{
    acquire_metadata_planning_lease, connector_request_context,
    connector_request_context_for_execution, connector_write_default_at,
    metadata_list_namespaces_with_planning_lease, metadata_list_tables_with_planning_lease,
    metadata_load_connector_table_with_planning_lease, metadata_load_table,
    metadata_read_reference_facts_with_planning_lease, metadata_table_exists_with_planning_lease,
    validate_request_context,
};
pub(crate) use application::{
    connector_planning_context_for_query_on_runtime,
    connector_request_context_for_execution_on_runtime,
    connector_request_context_for_query_on_runtime, connector_write_column,
    context_for_planning_lease, context_for_planning_lease_typed, metadata_binding_typed,
    metadata_list_namespaces_with_planning_lease_async,
    metadata_list_tables_with_planning_lease_async, metadata_namespace_exists_async,
    metadata_read_reference_facts_with_planning_lease_async,
    metadata_table_exists_with_planning_lease_async, query_connector_request_context_on_runtime,
    sql_column_from_connector_binding,
};
pub use unified_statistics::UnifiedStatisticsResolver;

#[cfg(test)]
pub use application::test_request_context;
#[cfg(test)]
pub use fixture::{FixtureConnectorRegistry, FixtureControlResolver};
