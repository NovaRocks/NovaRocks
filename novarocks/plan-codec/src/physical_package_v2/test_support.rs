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

//! Generous, explicit whole-package limits for tests of this crate and its
//! dependents. They prove composition, not production sizing.

use crate::physical_binding_v2::BindingProjectionLimits;

use crate::physical_connector_payload_v2::ConnectorPayloadProjectionLimits;

use crate::physical_cuts_v2::CutsProjectionLimits;

use crate::physical_node_v2::NodeProjectionLimits;
use crate::physical_properties_v2::PhysicalPropertyProjectionLimits;
use crate::physical_provider_binding_v2::ProviderBindingProjectionLimits;
use crate::physical_provider_read_v2::ProviderReadProjectionLimits;
use crate::physical_read_scan_v2::ReadScanProjectionLimits;
use crate::physical_relation_v2::RelationProjectionLimits;
use crate::physical_semantics_v2::ParameterProjectionLimits;
use crate::physical_type_v2::PackageTypeProjectionLimits;

use crate::physical_writer_recipe_v2::WriterRecipeProjectionLimits;
use crate::physical_writer_schema_v2::WriterSchemaProjectionLimits;
use crate::resource_preflight_v2::DecodeProjectionLimits;

use novarocks_physical_plan as p;

use super::{PackageDecodeLimits, PackageEncodeLimits};

const SOURCE: usize = 128 * 1024 * 1024;
const REQUEST: usize = 512 * 1024 * 1024;
const COEXIST: usize = SOURCE + REQUEST;
const WORK: usize = usize::MAX / 4;

fn properties() -> PhysicalPropertyProjectionLimits {
    PhysicalPropertyProjectionLimits {
        max_value_references: 100_000,
        max_allocation_requests: 1_000_000,
        max_allocation_request_bytes: REQUEST,
        max_coexisting_source_and_request_bytes: COEXIST,
        max_work: WORK,
    }
}
fn node() -> NodeProjectionLimits {
    NodeProjectionLimits {
        max_input_nodes: 100_000,
        max_value_references: 100_000,
        max_list_items: 1_000_000,
        max_allocation_requests: 1_000_000,
        max_allocation_request_bytes: REQUEST,
        max_coexisting_source_and_request_bytes: COEXIST,
        max_work: WORK,
        properties: properties(),
    }
}
fn bindings() -> BindingProjectionLimits {
    BindingProjectionLimits {
        max_definitions: 100_000,
        max_type_references: 1_000_000,
        max_request_bytes: REQUEST,
        max_allocation_requests: 1_000_000,
        max_coexisting_source_and_request_bytes: COEXIST,
        max_work: WORK,
    }
}
/// Generous test limits. They prove composition, not production sizing.
pub fn encode_limits() -> PackageEncodeLimits {
    PackageEncodeLimits {
        source_retained_bytes: SOURCE,
        node: node(),
        types: PackageTypeProjectionLimits {
            max_definitions: 1_000_000,
            max_expanded_nodes: 1_000_000,
            max_string_bytes: 64 * 1024 * 1024,
            max_allocation_requests: 1_000_000,
            max_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_work: WORK,
        },
        bindings: bindings(),
        provider_bindings: ProviderBindingProjectionLimits {
            max_definitions: 100_000,
            max_allocation_requests: 1_000_000,
            max_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_work: WORK,
        },
        payloads: ConnectorPayloadProjectionLimits {
            max_definitions: 100_000,
            max_payload_bytes: 64 * 1024 * 1024,
            max_allocation_requests: 1_000_000,
            max_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_work: WORK,
        },
        reads: ProviderReadProjectionLimits {
            max_definitions: 100_000,
            max_allocation_requests: 1_000_000,
            max_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_work: WORK,
        },
        relations: RelationProjectionLimits {
            max_definitions: 100_000,
            max_schema_fields: 100_000,
            max_predicate_guarantees: 100_000,
            max_metadata_kind_bytes: 1024,
            max_coverage_bytes: 1024,
            max_allocation_requests: 1_000_000,
            max_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_work: WORK,
            properties: properties(),
        },
        parameters: ParameterProjectionLimits {
            max_parameters: 100_000,
            max_timezone_request_bytes: 1024 * 1024,
            max_allocation_requests: 1_000_000,
            max_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_work: WORK,
        },
        writer_schema: WriterSchemaProjectionLimits {
            max_fields: 100_000,
            max_name_bytes: 1024 * 1024,
            max_type_references: 1_000_000,
            max_allocation_requests: 1_000_000,
            max_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_work: WORK,
        },
        cuts: CutsProjectionLimits {
            node: Some(node()),
            binding: Some(bindings()),
        },
        scans: ReadScanProjectionLimits {
            max_scans: 100_000,
            max_items: 1_000_000,
            max_scalar_bytes: 64 * 1024 * 1024,
            max_allocation_requests: 1_000_000,
            max_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_work: WORK,
        },
        writers: WriterRecipeProjectionLimits {
            max_recipes: 100_000,
            max_input_fields: 100_000,
            max_token_bytes: 32_000,
            max_allocation_requests: 1_000_000,
            max_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_work: WORK,
        },
    }
}

/// Generous receiver limits built from the sender's test limits where the
/// same component limit type serves both directions.
pub fn decode_limits() -> PackageDecodeLimits {
    let e = encode_limits();
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
        types: e.types,
        node: e.node,
        bindings: e.bindings,
        provider_bindings: e.provider_bindings,
        payloads: e.payloads,
        reads: e.reads,
        relations: e.relations,
        parameters: e.parameters,
        cuts: e.cuts,
        scans: e.scans,
        writers: e.writers,
        writer_schema: e.writer_schema,
        plan_limits: p::PlanLimits::FROZEN,
    }
}
