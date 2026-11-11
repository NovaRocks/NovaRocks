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

use crate::ipc_flat_batch_v2::FlatBatchProjectionLimits;
use crate::ipc_flat_pool_v2::FlatPoolWriteLimits;
use crate::ipc_flat_stream_v2::{FlatReaderProjectionLimits, FlatStreamProjectionLimits};
use crate::ipc_recursive_pool_v2::RecursivePoolWriteLimits;
use crate::ipc_recursive_stream_v2::{
    RecursiveBatchProjectionLimits, RecursiveReaderProjectionLimits,
    RecursiveStreamProjectionLimits,
};
use crate::ipc_schema_v2::IpcSchemaProjectionLimits;
use crate::physical_binding_v2::BindingProjectionLimits;
use crate::physical_call_requests_v2::CallRequestProjectionLimits;
use crate::physical_connector_payload_v2::ConnectorPayloadProjectionLimits;
use crate::physical_constant_v2::{
    ConstantDecodeProjectionLimits, ConstantNamespaceProjectionLimits,
    ConstantWriteProjectionLimits,
};
use crate::physical_control_v2::ControlProjectionLimits;
use crate::physical_cuts_v2::CutsProjectionLimits;
use crate::physical_expression_v2::ExpressionProjectionLimits;
use crate::physical_node_v2::NodeProjectionLimits;
use crate::physical_properties_v2::PhysicalPropertyProjectionLimits;
use crate::physical_provider_binding_v2::ProviderBindingProjectionLimits;
use crate::physical_provider_read_v2::ProviderReadProjectionLimits;
use crate::physical_read_scan_v2::ReadScanProjectionLimits;
use crate::physical_relation_v2::RelationProjectionLimits;
use crate::physical_semantics_v2::ParameterProjectionLimits;
use crate::physical_type_v2::PackageTypeProjectionLimits;
use crate::physical_value_origin_v2::ValueOriginProjectionLimits;
use crate::physical_value_v2::ValueProjectionLimits;
use crate::physical_writer_recipe_v2::WriterRecipeProjectionLimits;
use crate::physical_writer_schema_v2::WriterSchemaProjectionLimits;
use crate::resource_preflight_v2::DecodeProjectionLimits;
use novarocks_arrow_ipc_frame::VerifierOptions;
use novarocks_physical_plan as p;

use super::{
    BindingSourceLimits, DefinitionSourceLimits, PackageDecodeLimits, PackageEncodeLimits,
    ProviderSourceLimits, TypeViewLimits,
};

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
fn flat() -> FlatPoolWriteLimits {
    FlatPoolWriteLimits {
        max_rows: 4096,
        max_buffer_descriptors: 16384,
        max_body_bytes: 8 * 1024 * 1024,
        max_encoded_stream_bytes: 16 * 1024 * 1024,
        schema: IpcSchemaProjectionLimits {
            max_field_occurrences: 4096,
            max_type_occurrences: 4096,
            max_string_bytes: 65536,
            max_flatbuffer_bytes: 4 * 1024 * 1024,
        },
        max_new_allocation_request_bytes: REQUEST,
        max_coexisting_source_and_request_bytes: COEXIST,
        // A record writer's work bound scans whole-source metadata, so it
        // grows with the declared source size from the package's retained
        // floor upward; a fixed cap would refuse ordinary packages.
        max_cumulative_library_work: WORK,
    }
}

/// Generous test limits. They prove composition, not production sizing.
pub fn encode_limits() -> PackageEncodeLimits {
    PackageEncodeLimits {
        source_retained_bytes: SOURCE,
        views: TypeViewLimits {
            max_occurrences: 1_000_000,
            max_value_roots: 1_000_000,
            max_field_roots: 1_000_000,
            max_writer_recipes: 10_000,
            max_allocation_requests: 1_000_000,
            max_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_work: WORK,
        },
        binding_sources: BindingSourceLimits {
            max_functions: 100_000,
            max_aggregates: 100_000,
            max_arguments: 100_000,
            max_lambda_parameters: 100_000,
            max_relation_results: 100_000,
        },
        provider_sources: ProviderSourceLimits {
            max_provider_occurrences: 100_000,
            max_payload_occurrences: 100_000,
            max_read_occurrences: 100_000,
            max_relation_occurrences: 100_000,
            max_schema_occurrences: 100_000,
            max_scan_occurrences: 100_000,
            max_writer_occurrences: 100_000,
            max_relation_fields: 100_000,
            max_schema_fields: 100_000,
            max_connector_expression_occurrences: 100_000,
        },
        definition_sources: DefinitionSourceLimits {
            max_constants: 100_000,
            max_values: 100_000,
            max_expressions: 100_000,
            max_expression_lambda_parameters: 100_000,
            max_requests: 100_000,
            max_request_arguments: 100_000,
            max_request_lambda_parameters: 100_000,
            max_cut_type_occurrences: 100_000,
            max_result_fields: 100_000,
            max_writer_nodes: 100_000,
            max_writer_node_fields: 100_000,
        },
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
        constant_records: ConstantWriteProjectionLimits {
            flat: flat(),
            recursive: RecursivePoolWriteLimits {
                flat: flat(),
                max_field_nodes: 4096,
                max_total_rows: 65536,
            },
        },
        constants: ConstantNamespaceProjectionLimits {
            max_records: 100_000,
            max_preparation_request_bytes: REQUEST,
            max_new_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            // The namespace work bound grows with records times the declared
            // source size, so a fixed cap would refuse ordinary packages.
            max_cumulative_library_work: WORK,
        },
        parameters: ParameterProjectionLimits {
            max_parameters: 100_000,
            max_timezone_request_bytes: 1024 * 1024,
            max_allocation_requests: 1_000_000,
            max_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_work: WORK,
        },
        values: ValueProjectionLimits {
            max_definitions: 1_000_000,
            max_origin_references: 1_000_000,
            max_allocation_requests: 1_000_000,
            max_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_work: WORK,
            origins: ValueOriginProjectionLimits {
                max_allocation_requests: 1_000_000,
                max_allocation_request_bytes: REQUEST,
                max_coexisting_source_and_request_bytes: COEXIST,
                max_work: WORK,
            },
        },
        expressions: ExpressionProjectionLimits {
            max_definitions: 1_000_000,
            max_type_references: 1_000_000,
            max_expression_references: 1_000_000,
            max_new_allocation_requests: 1_000_000,
            max_new_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_cumulative_work: WORK,
        },
        requests: CallRequestProjectionLimits {
            max_definitions: 100_000,
            max_type_references: 1_000_000,
            max_request_bytes: REQUEST,
            max_allocation_requests: 1_000_000,
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
        control: ControlProjectionLimits {
            max_domains: 100_000,
            max_use_references: 1_000_000,
            max_root_bindings: 1_000_000,
            max_allocation_requests: 1_000_000,
            max_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_work: WORK,
        },
        cuts: CutsProjectionLimits {
            node: node(),
            binding: bindings(),
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

const GIB: usize = 1024 * 1024 * 1024;

fn schema() -> IpcSchemaProjectionLimits {
    IpcSchemaProjectionLimits {
        max_field_occurrences: 4096,
        max_type_occurrences: 4096,
        max_string_bytes: 65536,
        max_flatbuffer_bytes: 4 * 1024 * 1024,
    }
}
fn batch() -> FlatBatchProjectionLimits {
    FlatBatchProjectionLimits {
        max_metadata_bytes: 1024 * 1024,
        max_body_bytes: 8 * 1024 * 1024,
        max_rows: 4096,
        max_buffer_descriptors: 16384,
        max_view_validation_bytes: 8 * 1024 * 1024,
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
        constant_policy: p::ConstantPolicy {
            max_rows: 4096,
            max_array_nodes: 4096,
            max_logical_elements: 65536,
            max_retained_buffer_bytes: 8 * 1024 * 1024,
            max_type_depth: 64,
            max_type_nodes: 4096,
            max_dictionary_depth: 8,
            max_metadata_bytes: 65536,
            max_library_validation_work: 32 * 1024 * 1024,
            max_library_validation_bytes: 32 * 1024 * 1024,
        },
        constant_records: ConstantDecodeProjectionLimits {
            flat_stream: FlatStreamProjectionLimits {
                max_input_bytes: 16 * 1024 * 1024,
                schema: schema(),
                batch: batch(),
            },
            flat_reader: FlatReaderProjectionLimits {
                max_new_allocation_request_bytes: 128 * 1024 * 1024,
                max_coexisting_source_and_request_bytes: 4 * GIB,
                max_cumulative_library_work: 512 * 1024 * 1024,
            },
            recursive_stream: RecursiveStreamProjectionLimits {
                max_input_bytes: 16 * 1024 * 1024,
                schema: schema(),
                batch: RecursiveBatchProjectionLimits {
                    flat: batch(),
                    max_field_nodes: 4096,
                    max_total_rows: 65536,
                    max_geometry_request_bytes: 1024 * 1024,
                },
            },
            recursive_reader: RecursiveReaderProjectionLimits {
                max_new_allocation_request_bytes: 128 * 1024 * 1024,
                max_coexisting_source_and_request_bytes: 4 * GIB,
                max_cumulative_library_work: GIB,
            },
        },
        constants: e.constants,
        verifier: VerifierOptions {
            max_depth: 67,
            max_tables: 65536,
            max_apparent_size: 16 * 1024 * 1024,
            ignore_missing_null_terminator: false,
        },
        bindings: e.bindings,
        provider_bindings: e.provider_bindings,
        payloads: e.payloads,
        reads: e.reads,
        relations: e.relations,
        values: e.values,
        expressions: e.expressions,
        requests: e.requests,
        parameters: e.parameters,
        cuts: e.cuts,
        scans: e.scans,
        writers: e.writers,
        writer_schema: e.writer_schema,
        control: e.control,
        admission: p::FragmentPackageAdmission {
            plan_limits: p::PlanLimits::FROZEN,
            source_retained_bytes: 2 * GIB,
            property_projection_limits: p::PropertyProofProjectionLimits {
                max_request_bytes: 512 * 1024 * 1024,
                max_coexisting_bytes: 4 * GIB,
                max_projection_work: usize::MAX / 4,
            },
        },
    }
}
