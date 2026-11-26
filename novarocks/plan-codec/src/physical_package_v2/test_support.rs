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

//! Explicit profiles for tests that exercise bounded component APIs.

use crate::{
    physical_node_v2::NodeProjectionLimits,
    physical_properties_v2::PhysicalPropertyProjectionLimits,
    physical_type_v2::PackageTypeProjectionLimits, resource_preflight_v2::DecodeProjectionLimits,
};
use novarocks_functions::ConstantPolicy;
use novarocks_physical_plan as p;

use super::PackageDecodeLimits;

pub const CONSTANT_POLICY_FIXTURE: ConstantPolicy = constant_policy_fixture();
pub const PACKAGE_ADMISSION_FIXTURE: p::FragmentPackageAdmission = package_admission_fixture();

/// Legacy constant thresholds used by explicit bounded fixtures.
const fn constant_policy_fixture() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 4096,
        max_array_nodes: 4096,
        max_logical_elements: 65536,
        max_retained_buffer_bytes: 8 * MIB as u64,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 8,
        max_metadata_bytes: 65536,
        max_library_validation_work: 32 * MIB as u64,
        max_library_validation_bytes: 32 * MIB as u64,
    }
}

const MIB: usize = 1024 * 1024;
/// Bytes a decoded source may retain.
const SOURCE: usize = 128 * MIB;
/// Bytes one namespace may request.
const REQUEST: usize = 512 * MIB;
const COEXIST: usize = SOURCE + REQUEST;
const WORK: usize = usize::MAX / 4;

const fn package_admission_fixture() -> p::FragmentPackageAdmission {
    p::FragmentPackageAdmission {
        plan_limits: p::PlanLimits::FROZEN,
        source_retained_bytes: SOURCE,
        property_projection_limits: p::PropertyProofProjectionLimits {
            max_request_bytes: REQUEST,
            max_coexisting_bytes: COEXIST,
            max_projection_work: WORK,
        },
    }
}

pub fn decode_limits() -> PackageDecodeLimits {
    PackageDecodeLimits {
        wire: DecodeProjectionLimits {
            max_input_bytes: 64 << 20,
            max_requested_heap_bytes: 512 << 20,
            max_message_occurrences: 1_000_000,
            max_scalar_elements: 1_000_000,
            max_field_occurrences: 1_000_000,
            max_copied_bytes: 512 << 20,
            max_initialization_bytes: 512 << 20,
            max_wire_depth: 100,
        },
        plan_limits: p::PlanLimits::FROZEN,
    }
}

pub fn types_projection_limits() -> PackageTypeProjectionLimits {
    PackageTypeProjectionLimits {
        max_definitions: 1_000_000,
        max_expanded_nodes: 1_000_000,
        max_string_bytes: 64 * MIB,
        max_allocation_requests: 1_000_000,
        max_allocation_request_bytes: REQUEST,
        max_coexisting_source_and_request_bytes: COEXIST,
        max_work: WORK,
    }
}

pub fn node_projection_limits() -> NodeProjectionLimits {
    NodeProjectionLimits {
        max_input_nodes: 100_000,
        max_value_references: 100_000,
        max_list_items: 1_000_000,
        max_allocation_requests: 1_000_000,
        max_allocation_request_bytes: REQUEST,
        max_coexisting_source_and_request_bytes: COEXIST,
        max_work: WORK,
        properties: PhysicalPropertyProjectionLimits {
            max_value_references: 100_000,
            max_allocation_requests: 1_000_000,
            max_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_work: WORK,
        },
    }
}
