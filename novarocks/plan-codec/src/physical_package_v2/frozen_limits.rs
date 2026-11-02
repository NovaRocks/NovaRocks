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
use crate::physical_package_v2::{
    BindingSourceLimits, DefinitionSourceLimits, PackageDecodeLimits, PackageEncodeLimits,
    ProviderSourceLimits, TypeViewLimits,
};
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
use crate::resource_preflight_v2::{DecodeProjectionLimits, PROST_RECURSION_LIMIT};
use novarocks_arrow_ipc_frame::VerifierOptions;
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
const GIB: usize = 1024 * MIB;
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

const fn ipc_schema() -> IpcSchemaProjectionLimits {
    IpcSchemaProjectionLimits {
        max_field_occurrences: 4096,
        max_type_occurrences: 4096,
        max_string_bytes: 65536,
        max_flatbuffer_bytes: 4 * MIB,
    }
}

const fn ipc_batch() -> FlatBatchProjectionLimits {
    FlatBatchProjectionLimits {
        max_metadata_bytes: MIB,
        max_body_bytes: 8 * MIB,
        max_rows: 4096,
        max_buffer_descriptors: 16384,
        max_view_validation_bytes: 8 * MIB,
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

const fn flat_pool() -> FlatPoolWriteLimits {
    FlatPoolWriteLimits {
        max_rows: 4096,
        max_buffer_descriptors: 16384,
        max_body_bytes: 8 * MIB,
        max_encoded_stream_bytes: 16 * MIB,
        schema: ipc_schema(),
        max_new_allocation_request_bytes: REQUEST,
        max_coexisting_source_and_request_bytes: COEXIST,
        // A record writer's work bound scans whole-source metadata, so it
        // grows with the declared source size from the package's retained
        // floor upward; a fixed cap would refuse ordinary packages.
        max_cumulative_library_work: WORK,
    }
}

/// Frozen sender structure limits for one whole package.
const fn frozen_encode_limits() -> PackageEncodeLimits {
    PackageEncodeLimits {
        source_retained_bytes: SOURCE,
        max_wire_bytes: 64 * MIB,
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
            max_input_version_bytes: 1024,
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
            flat: flat_pool(),
            recursive: RecursivePoolWriteLimits {
                flat: flat_pool(),
                max_field_nodes: 4096,
                max_total_rows: 65536,
            },
        },
        constants: ConstantNamespaceProjectionLimits {
            max_records: 100_000,
            max_preparation_request_bytes: REQUEST,
            max_new_allocation_request_bytes: REQUEST,
            max_coexisting_source_and_request_bytes: COEXIST,
            max_cumulative_library_work: WORK,
        },
        parameters: ParameterProjectionLimits {
            max_parameters: 100_000,
            max_timezone_request_bytes: MIB,
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
            max_name_bytes: MIB,
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
        constant_policy: frozen_constant_policy(),
        constant_records: ConstantDecodeProjectionLimits {
            flat_stream: FlatStreamProjectionLimits {
                max_input_bytes: 16 * MIB,
                schema: ipc_schema(),
                batch: ipc_batch(),
            },
            flat_reader: FlatReaderProjectionLimits {
                max_new_allocation_request_bytes: 128 * MIB,
                max_coexisting_source_and_request_bytes: 4 * GIB,
                max_cumulative_library_work: WORK,
            },
            recursive_stream: RecursiveStreamProjectionLimits {
                max_input_bytes: 16 * MIB,
                schema: ipc_schema(),
                batch: RecursiveBatchProjectionLimits {
                    flat: ipc_batch(),
                    max_field_nodes: 4096,
                    max_total_rows: 65536,
                    max_geometry_request_bytes: MIB,
                },
            },
            recursive_reader: RecursiveReaderProjectionLimits {
                max_new_allocation_request_bytes: 128 * MIB,
                max_coexisting_source_and_request_bytes: 4 * GIB,
                max_cumulative_library_work: WORK,
            },
        },
        constants: encode.constants,
        verifier: VerifierOptions {
            max_depth: 67,
            max_tables: 65536,
            max_apparent_size: 16 * MIB,
            ignore_missing_null_terminator: false,
        },
        bindings: encode.bindings,
        provider_bindings: encode.provider_bindings,
        payloads: encode.payloads,
        reads: encode.reads,
        relations: encode.relations,
        values: encode.values,
        expressions: encode.expressions,
        requests: encode.requests,
        parameters: encode.parameters,
        cuts: encode.cuts,
        scans: encode.scans,
        writers: encode.writers,
        writer_schema: encode.writer_schema,
        control: encode.control,
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

    /// The frontend author and the backend receiver share one admission,
    /// whose source floor is exactly the sender's invoiced source and whose
    /// property projection fits the shared coexistence envelope. Namespace
    /// library work is not capped below records times declared source size,
    /// and no record writer's work is capped below its whole-source scans.
    #[test]
    fn frozen_sender_and_receiver_limits_are_mutually_consistent() {
        let admission = FROZEN_PACKAGE_ADMISSION;
        let encode = PackageEncodeLimits::FROZEN;
        let decode = PackageDecodeLimits::FROZEN;

        assert_eq!(
            admission.source_retained_bytes,
            encode.source_retained_bytes
        );
        assert_eq!(
            decode.admission.source_retained_bytes,
            admission.source_retained_bytes
        );
        assert_eq!(decode.admission.plan_limits, admission.plan_limits);
        let property = admission.property_projection_limits;
        assert!(
            admission.source_retained_bytes + property.max_request_bytes
                <= property.max_coexisting_bytes
        );
        assert!(
            encode.source_retained_bytes + encode.types.max_allocation_request_bytes
                <= encode.types.max_coexisting_source_and_request_bytes
        );
        // The decoder must not reject the sender's legitimate wire size.
        // A receiver-only sizing quota is not part of that compatibility.
        assert!(encode.max_wire_bytes <= decode.wire.max_input_bytes);
        assert_eq!(decode.constant_policy, FROZEN_CONSTANT_POLICY);
        assert_eq!(
            decode
                .constant_records
                .flat_reader
                .max_cumulative_library_work,
            WORK
        );
        assert_eq!(
            decode
                .constant_records
                .recursive_reader
                .max_cumulative_library_work,
            WORK
        );
        assert_eq!(encode.constants.max_cumulative_library_work, usize::MAX / 4);
        assert_eq!(decode.constants.max_cumulative_library_work, usize::MAX / 4);
        for record in [
            encode.constant_records.flat,
            encode.constant_records.recursive.flat,
        ] {
            assert_eq!(record.max_cumulative_library_work, usize::MAX / 4);
        }
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

    fn constant_reader_fixture(
        recursive: bool,
    ) -> (Vec<u8>, std::sync::Arc<arrow::datatypes::Field>) {
        use arrow::{
            array::{Array, ArrayRef, Int64Array, StructArray},
            datatypes::{Field, Schema},
            ipc::writer::StreamWriter,
            record_batch::RecordBatch,
        };
        use std::sync::Arc;
        let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(7), None]));
        let values: ArrayRef = if recursive {
            Arc::new(StructArray::from(vec![(
                Arc::new(Field::new("child", arrow::datatypes::DataType::Int64, true)),
                values,
            )]))
        } else {
            values
        };
        let field = Arc::new(Field::new("constant", values.data_type().clone(), true));
        let schema = Arc::new(Schema::new(vec![field.clone()]));
        let batch = RecordBatch::try_new(schema.clone(), vec![values]).unwrap();
        let mut bytes = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut bytes, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        (bytes, field)
    }

    fn flat_reader_model(
        source: usize,
        limits: FlatReaderProjectionLimits,
        control: &dyn PureCompileControl,
    ) -> Result<
        crate::ipc_flat_stream_v2::FlatReaderResourceFacts,
        crate::ipc_flat_stream_v2::FlatReaderError,
    > {
        use crate::ipc_flat_stream_v2::{FlatReaderError, preflight_flat_constant_stream};
        use novarocks_type_contract::{CompileCheckpoints, FunctionValueType};
        let decode = PackageDecodeLimits::FROZEN;
        let (bytes, field) = constant_reader_fixture(false);
        let value_type = FunctionValueType::new(field.data_type().clone(), true);
        let stream = preflight_flat_constant_stream(
            &bytes,
            &field,
            decode.constant_records.flat_stream,
            &decode.verifier,
            &Control,
        )
        .map_err(crate::ipc_flat_stream_v2::FlatPoolResourceError::from)?;
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
        let prepared = stream.prepare_pool_borrowed_observed(
            field.clone(),
            &value_type,
            source,
            FROZEN_CONSTANT_POLICY,
            limits,
            &mut |_| Ok::<(), FlatReaderError>(()),
            &mut work,
        )?;
        let facts = *prepared.facts();
        drop(prepared);
        work.finish()?;
        Ok(facts)
    }

    fn recursive_reader_model(
        source: usize,
        limits: RecursiveReaderProjectionLimits,
        control: &dyn PureCompileControl,
    ) -> Result<
        crate::ipc_recursive_stream_v2::RecursiveReaderResourceFacts,
        crate::ipc_flat_stream_v2::FlatReaderError,
    > {
        use crate::ipc_flat_stream_v2::{FlatPoolResourceError, FlatReaderError};
        use crate::ipc_recursive_stream_v2::preflight_recursive_constant_stream;
        use novarocks_type_contract::{CompileCheckpoints, FunctionValueType};
        let decode = PackageDecodeLimits::FROZEN;
        let (bytes, field) = constant_reader_fixture(true);
        let value_type = FunctionValueType::new(field.data_type().clone(), true);
        let stream = preflight_recursive_constant_stream(
            &bytes,
            &field,
            decode.constant_records.recursive_stream,
            &decode.verifier,
            &Control,
        )
        .map_err(FlatPoolResourceError::from)?;
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
        let prepared = stream.prepare_pool_borrowed_observed(
            field.clone(),
            &value_type,
            source,
            FROZEN_CONSTANT_POLICY,
            limits,
            &mut |_| Ok::<(), FlatReaderError>(()),
            &mut work,
        )?;
        let facts = *prepared.facts();
        drop(prepared);
        work.finish()?;
        Ok(facts)
    }

    /// These two-row IPCs have tiny bodies. The real declared source invoice,
    /// including opaque metadata bucket backing, still enters the original
    /// complete numerical author. No 128-MiB source allocation is fabricated.
    #[test]
    fn frozen_reader_work_covers_real_flat_and_recursive_models_at_the_package_source_floor() {
        use crate::ipc_flat_stream_v2::{FlatPoolResourceError, FlatReaderError};
        let decode = PackageDecodeLimits::FROZEN;
        let flat_limits = decode.constant_records.flat_reader;
        let nested_limits = decode.constant_records.recursive_reader;
        let flat = flat_reader_model(SOURCE, flat_limits, &Control).unwrap();
        let nested = recursive_reader_model(SOURCE, nested_limits, &Control).unwrap();
        // Independent lower bounds from the original source-walk contracts:
        // flat >=6S; the recursive Struct+child has N=2, hence >=14S.
        assert!(flat.cumulative_library_work_upper_bound > 6 * SOURCE);
        assert!(nested.cumulative_library_work_upper_bound > 14 * SOURCE);
        assert!(flat.cumulative_library_work_upper_bound > 512 * MIB);
        assert!(nested.cumulative_library_work_upper_bound > GIB);
        assert!(
            flat.cumulative_library_work_upper_bound <= flat_limits.max_cumulative_library_work
        );
        assert!(
            nested.cumulative_library_work_upper_bound <= nested_limits.max_cumulative_library_work
        );
        assert!(
            flat.new_allocation_request_bytes_upper_bound
                <= flat_limits.max_new_allocation_request_bytes
        );
        assert!(
            nested.new_allocation_request_bytes_upper_bound
                <= nested_limits.max_new_allocation_request_bytes
        );
        for error in [
            flat_reader_model(
                SOURCE,
                FlatReaderProjectionLimits {
                    max_cumulative_library_work: 512 * MIB,
                    ..flat_limits
                },
                &Control,
            )
            .err()
            .unwrap(),
            recursive_reader_model(
                SOURCE,
                RecursiveReaderProjectionLimits {
                    max_cumulative_library_work: GIB,
                    ..nested_limits
                },
                &Control,
            )
            .err()
            .unwrap(),
        ] {
            assert!(matches!(
                error,
                FlatReaderError::Projection(FlatPoolResourceError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
        }
    }

    #[test]
    fn frozen_reader_model_cap_and_checked_overflow_remain_typed_control() {
        use crate::ipc_flat_stream_v2::{FlatPoolResourceError, FlatReaderError};
        let decode = PackageDecodeLimits::FROZEN;
        // Checked numerical overflow is distinct from a larger allocation
        // allowance: no original reader or constant pool is materialized.
        let flat = FlatReaderProjectionLimits {
            max_new_allocation_request_bytes: usize::MAX,
            max_coexisting_source_and_request_bytes: usize::MAX,
            max_cumulative_library_work: usize::MAX,
        };
        let nested = RecursiveReaderProjectionLimits {
            max_new_allocation_request_bytes: usize::MAX,
            max_coexisting_source_and_request_bytes: usize::MAX,
            max_cumulative_library_work: usize::MAX,
        };
        for error in [
            flat_reader_model(usize::MAX / 2, flat, &Control)
                .err()
                .unwrap(),
            recursive_reader_model(usize::MAX / 2, nested, &Control)
                .err()
                .unwrap(),
            flat_reader_model(
                SOURCE,
                FlatReaderProjectionLimits {
                    max_new_allocation_request_bytes: 0,
                    ..decode.constant_records.flat_reader
                },
                &Control,
            )
            .err()
            .unwrap(),
            recursive_reader_model(
                SOURCE,
                RecursiveReaderProjectionLimits {
                    max_new_allocation_request_bytes: 0,
                    ..decode.constant_records.recursive_reader
                },
                &Control,
            )
            .err()
            .unwrap(),
        ] {
            assert!(matches!(
                error,
                FlatReaderError::Projection(FlatPoolResourceError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
        }
    }

    #[test]
    fn frozen_reader_work_keeps_the_original_control_cause_without_footer() {
        use crate::ipc_flat_stream_v2::{FlatPoolResourceError, FlatReaderError};
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Refuse {
            calls: AtomicUsize,
            cause: CompileControlError,
        }
        impl PureCompileControl for Refuse {
            fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
                if self.calls.fetch_add(1, Ordering::SeqCst) == 1 {
                    Err(self.cause.clone())
                } else {
                    Ok(())
                }
            }
        }
        let decode = PackageDecodeLimits::FROZEN;
        for recursive in [false, true] {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Refuse {
                    calls: AtomicUsize::new(0),
                    cause: cause.clone(),
                };
                let result = if recursive {
                    recursive_reader_model(
                        SOURCE,
                        decode.constant_records.recursive_reader,
                        &control,
                    )
                    .map(|_| ())
                } else {
                    flat_reader_model(SOURCE, decode.constant_records.flat_reader, &control)
                        .map(|_| ())
                };
                assert!(matches!(result, Err(FlatReaderError::Projection(
                    FlatPoolResourceError::Control(actual))) if actual == cause));
                assert_eq!(control.calls.load(Ordering::SeqCst), 2);
            }
        }
    }

    /// Both directions share one type ceiling, and the sender charges each
    /// strict type root's validator by that root's actual node count, the
    /// receiver's own per-root summary bound. A wide fragment's scalar roots
    /// therefore fit the frozen ceiling under the frozen source invoice,
    /// where a fixed 4096-node charge per root refused from 245 roots.
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
