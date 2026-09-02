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

pub mod aggregate;
mod arithmetic;
pub mod arrow_cast;
pub mod arrow_primitive;
pub mod cluster_role;
pub mod coercion;
pub mod decimal;
pub mod engine_error_codes;
mod field_render;
pub mod field_render_schema;
pub mod identity;
pub mod largeint;
pub mod logical;
pub mod mv_aggregate_layout;
pub mod naming;
pub mod native_compatibility;
pub mod network;
mod predicate;
pub mod primitive;
pub mod schema;
pub mod slot_id;
pub mod value;

pub use arithmetic::{
    arithmetic_result_type, arithmetic_result_type_with_op, canonical_agg_decimal_type,
    decimal_arithmetic_result_type,
};
pub use cluster_role::ClusterRole;
pub use coercion::{comparison_common_type, wider_type};
pub use engine_error_codes::EngineErrorCode;
pub use field_render::{
    format_mysql_container_value_with_schema, http_json_row_from_arrays_with_primitives,
    mysql_text_row_from_arrays_with_primitives,
};
pub use field_render_schema::FieldRenderSchema;
pub use identity::{
    AttemptId, BackendProcessId, BackendProcessIdentityError, ExecutionIdentityError,
    FrontendProcessId, FrontendProcessIdentityError, LocalQuerySequence, QueryExecutionId, QueryId,
    QueryIdAttribution, QueryProcessNamespace, StageId, TaskId, UniqueId, format_uuid,
};
pub use native_compatibility::{NativeCompatibilityId, NativeCompatibilityIdError};
pub use network::{
    AdvertiseEndpoint, CanonicalDnsName, NativeEndpoint, NativeReferenceHost, format_host_for_url,
};
pub use primitive::PrimitiveType;
pub use slot_id::SlotId;

/// Worker thread stack size for Tokio runtimes that execute NovaRocks workloads.
///
/// Deep SQL planner and fragment-builder walks require more than the platform
/// default stack. All application and execution runtime builders use this
/// shared process-independent sizing contract.
pub const WORKER_STACK_SIZE_BYTES: usize = 16 * 1024 * 1024;
