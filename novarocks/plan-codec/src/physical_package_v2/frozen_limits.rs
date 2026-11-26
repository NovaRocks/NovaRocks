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

//! Frozen package structure and constant-admission limits.
//!
//! Sender and receiver share these ceilings. They bound one package's shape
//! and codec work; deployment-specific runtime capacity remains separately owned.

use crate::physical_binding_v2::BindingProjectionLimits;

use crate::physical_connector_payload_v2::ConnectorPayloadProjectionLimits;

use crate::physical_cuts_v2::CutsProjectionLimits;

use crate::physical_node_v2::NodeProjectionLimits;
use crate::physical_package_v2::{PackageDecodeLimits, PackageEncodeLimits};
use crate::physical_properties_v2::PhysicalPropertyProjectionLimits;
use crate::physical_provider_binding_v2::ProviderBindingProjectionLimits;
use crate::physical_provider_read_v2::ProviderReadProjectionLimits;
use crate::physical_read_scan_v2::ReadScanProjectionLimits;
use crate::physical_relation_v2::RelationProjectionLimits;
use crate::physical_semantics_v2::ParameterProjectionLimits;
use crate::physical_type_v2::PackageTypeProjectionLimits;
use crate::physical_value_origin_v2::ValueOriginProjectionLimits;

use crate::physical_writer_recipe_v2::WriterRecipeProjectionLimits;
use crate::physical_writer_schema_v2::WriterSchemaProjectionLimits;
use crate::resource_preflight_v2::{DecodeProjectionLimits, PROST_RECURSION_LIMIT};

use novarocks_functions::ConstantPolicy;
use novarocks_physical_plan as p;

/// Frozen constant policy shared by package decoding and compilation.
pub const FROZEN_CONSTANT_POLICY: ConstantPolicy = frozen_constant_policy();

/// The sender and receiver admit the same checked package structure.
pub const FROZEN_PACKAGE_ADMISSION: p::FragmentPackageAdmission = frozen_package_admission();

impl PackageEncodeLimits {
    /// Production structure ceilings. These are not deployment resource quotas.
    pub const FROZEN: Self = frozen_encode_limits();
}

impl PackageDecodeLimits {
    /// Production codec profile, including pre-Prost resource projection.
    pub const FROZEN: Self = frozen_decode_limits();
}

/// Frozen constant admission for package decoding and local compilation.
const fn frozen_constant_policy() -> ConstantPolicy {
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
/// Finite cumulative-model ceiling shared by namespaces and record readers.
/// Reader source work includes the complete retained invoice, not just the
/// payload: flat work starts above 6*SOURCE; recursive work above
/// (6*type_nodes+2)*SOURCE and adds type-path/metadata-name-group terms. Body,
/// schema, request and coexistence limits remain independently enforced.
/// Actual input-derived formulas use checked arithmetic and parent observation;
/// arithmetic overflow or this ceiling's exhaustion remains typed Control.
const WORK: usize = usize::MAX / 4;

const fn properties() -> PhysicalPropertyProjectionLimits {
    PhysicalPropertyProjectionLimits {
        max_value_references: 100_000,
        max_allocation_requests: 1_000_000,
        max_allocation_request_bytes: REQUEST,
        max_coexisting_source_and_request_bytes: COEXIST,
        max_work: WORK,
    }
}

const fn node() -> NodeProjectionLimits {
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

const fn bindings() -> BindingProjectionLimits {
    BindingProjectionLimits {
        max_definitions: 100_000,
        max_type_references: 1_000_000,
        max_request_bytes: REQUEST,
        max_allocation_requests: 1_000_000,
        max_coexisting_source_and_request_bytes: COEXIST,
        max_work: WORK,
    }
}

/// The one admission every production package is constructed under, by the
/// frontend's checked-package author and by the backend's receiver alike.
///
/// The package's retained source floor travels in the sender's cumulative
/// source invoice, so it is exactly the encode limits' `source_retained_bytes`,
/// and its property projection coexists within the same source-plus-request
/// envelope every component limit uses.
const fn frozen_package_admission() -> p::FragmentPackageAdmission {
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

/// Frozen sender structure limits for one whole package.
const fn frozen_encode_limits() -> PackageEncodeLimits {
    PackageEncodeLimits {
        source_retained_bytes: SOURCE,
        node: node(),
        types: PackageTypeProjectionLimits {
            max_definitions: 1_000_000,
            max_expanded_nodes: 1_000_000,
            max_string_bytes: 64 * MIB,
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
            max_payload_bytes: 64 * MIB,
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
            max_timezone_request_bytes: MIB,
            max_allocation_requests: 1_000_000,
            max_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_work: WORK,
        },
        writer_schema: WriterSchemaProjectionLimits {
            max_fields: 100_000,
            max_name_bytes: MIB,
            max_type_references: 1_000_000,
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
            max_scalar_bytes: 64 * MIB,
            max_allocation_requests: 1_000_000,
            max_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_work: WORK,
        },
        writers: WriterRecipeProjectionLimits {
            // These three fields only compare actual facts with a threshold.
            // Production adds no writer-specific count or token quota; source
            // invoices, checked layouts and shared resource controls still apply.
            max_recipes: usize::MAX,
            max_input_fields: usize::MAX,
            max_token_bytes: usize::MAX,
            max_allocation_requests: 1_000_000,
            max_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_work: WORK,
        },
    }
}

/// Shared component limits, constant-record/verifier admission, and resource
/// projection for one package. The wire projection adds no independent byte,
/// occurrence, heap, copy or initialization quota. Native carrier admission and
/// the winning query's existing resource owner retain those responsibilities.
const fn frozen_decode_limits() -> PackageDecodeLimits {
    let encode = frozen_encode_limits();
    PackageDecodeLimits {
        wire: DecodeProjectionLimits {
            // These former candidate sizing values had no production basis.
            // Keep checked resource accounting, but leave capacity decisions
            // to the caller instead of refusing an arbitrary structural count.
            max_input_bytes: usize::MAX,
            max_requested_heap_bytes: usize::MAX,
            max_message_occurrences: usize::MAX,
            max_scalar_elements: usize::MAX,
            max_field_occurrences: usize::MAX,
            max_copied_bytes: usize::MAX,
            max_initialization_bytes: usize::MAX,
            max_wire_depth: PROST_RECURSION_LIMIT,
        },
        types: encode.types,
        node: encode.node,
        bindings: encode.bindings,
        provider_bindings: encode.provider_bindings,
        payloads: encode.payloads,
        reads: encode.reads,
        relations: encode.relations,
        parameters: encode.parameters,
        cuts: encode.cuts,
        scans: encode.scans,
        writers: encode.writers,
        writer_schema: encode.writer_schema,
        admission: frozen_package_admission(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};

    struct Control;
    impl PureCompileControl for Control {
        fn checkpoint(&self, _phase: CompilePhase, _units: u32) -> Result<(), CompileControlError> {
            Ok(())
        }
    }

    #[test]
    fn production_wire_projection_counts_legal_occurrences_without_a_second_quota() {
        use crate::resource_preflight_v2::{
            FragmentDecodeResourceModel, ResourceCursorStatus, ResourceModelError,
        };
        use novarocks_proto_models::physical_package_v2::FragmentPackage;
        use prost::Message;
        use prost::encoding::{WireType, encode_key, encode_varint};

        // Lawful unknown varints fit the existing native carrier. Their
        // number is accounted for, but is not a separate allocation quota.
        // Required package semantics remain the regular receiver's concern.
        const OCCURRENCES: usize = 1_000_001;
        let mut field = Vec::new();
        encode_key(100_000, WireType::Varint, &mut field);
        encode_varint(0, &mut field);
        let raw = field.repeat(OCCURRENCES);
        let model = FragmentDecodeResourceModel::try_new(&Control).unwrap();
        let limits = PackageDecodeLimits::FROZEN.wire;
        let projection = model.preflight(&raw, limits, &Control).unwrap();
        assert_eq!(projection.status, ResourceCursorStatus::Complete);
        assert_eq!(projection.usage.field_occurrences, OCCURRENCES);
        assert_eq!(projection.usage.input_bytes, raw.len());
        FragmentPackage::decode(raw.as_slice()).expect("the pinned decoder accepts lawful fields");

        // Explicit caller policies are still enforced. Only the unexplained
        // production default quota was removed, not the caller's authority.
        assert!(matches!(
            model.preflight(
                &raw,
                DecodeProjectionLimits {
                    max_field_occurrences: OCCURRENCES - 1,
                    ..limits
                },
                &Control,
            ),
            Err(ResourceModelError::Limit(
                "protobuf field occurrences exceed caller limit"
            ))
        ));
    }

    #[test]
    fn frozen_writer_policy_has_no_additional_count_or_token_quota() {
        let encode = PackageEncodeLimits::FROZEN.writers;
        let decode = PackageDecodeLimits::FROZEN.writers;
        for limits in [encode, decode] {
            assert_eq!(limits.max_recipes, usize::MAX);
            assert_eq!(limits.max_input_fields, usize::MAX);
            assert_eq!(limits.max_token_bytes, usize::MAX);
            assert_eq!(limits.max_allocation_request_bytes, REQUEST);
            assert_eq!(limits.max_coexisting_source_and_request_bytes, COEXIST);
            assert_eq!(limits.max_work, WORK);
        }
    }

    #[test]
    fn frozen_type_ceiling_admits_wide_scalar_type_tables() {
        use crate::physical_type_v2::encode_type_table_writer_sources_observed;
        use arrow::datatypes::DataType;
        use novarocks_type_contract::{
            CompileCheckpoints, CompilePhase, FunctionValueType, MAX_VALUE_TYPE_NODES,
        };
        let encode = PackageEncodeLimits::FROZEN;
        let decode = PackageDecodeLimits::FROZEN;
        assert_eq!(
            decode.types.max_allocation_requests,
            encode.types.max_allocation_requests
        );
        assert_eq!(
            decode.types.max_allocation_request_bytes,
            encode.types.max_allocation_request_bytes
        );
        const ROOTS: u32 = 2000;
        assert!(ROOTS as usize * MAX_VALUE_TYPE_NODES > encode.types.max_allocation_requests);
        let roots = (0..ROOTS)
            .map(|id| (id, FunctionValueType::new(DataType::Int64, true)))
            .collect::<Vec<_>>();
        let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Encode).unwrap();
        let mut last = None;
        let table = encode_type_table_writer_sources_observed(
            &roots,
            &[],
            &[],
            encode.source_retained_bytes,
            encode.types,
            &mut |facts| {
                last = Some(*facts);
                Ok(())
            },
            &mut work,
        )
        .expect("a wide scalar type table fits the frozen type ceiling");
        work.finish().unwrap();
        assert_eq!(table.as_wire().value_types.len(), ROOTS as usize);
        let facts = last.expect("the sender admitted its type projection");
        assert!(facts.allocation_requests_upper_bound <= encode.types.max_allocation_requests);
        assert!(
            facts.allocation_requests_upper_bound < 4 * ROOTS as usize + 1024,
            "{facts:?}"
        );
    }
}
