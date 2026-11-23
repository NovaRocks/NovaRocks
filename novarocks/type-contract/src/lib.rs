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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Pure semantic type contracts shared by compilers, physical plans and workers.
//!
//! This crate owns immutable type identities and deterministic type rules. It
//! deliberately excludes Arrow arrays, casts, rendering, serialization and
//! runtime kernels so contract consumers do not acquire those capabilities.

mod failure_projection;

mod between;
pub use between::*;
mod arithmetic;
mod array_cast;
mod array_common;
pub use array_cast::{
    ArrayCastFailureMode, array_cast_mode_types_valid, array_cast_mode_types_valid_observed,
};
pub use array_common::{
    ArrayCommonRuleFailure, ArrayCommonTypeFailure, array_common_coercion_types_valid_observed,
    array_common_numeric_coercion, array_constructor_result_with_construction,
    list_value_type_with_construction,
};
mod array_generate;
mod assignment_common;
pub use assignment_common::{AssignmentCommonTypeError, assignment_common_value_type};
pub use novarocks_result_contract::{CharacterKind, CharacterType};
mod trino_coercion;
pub use trino_coercion::{
    TrinoCastMode, trino_can_coerce, trino_can_coerce_scalar, trino_can_store_assign,
    trino_common_scalar_kind, trino_common_type_with_construction, trino_decimal_widening_is_total,
    trino_same_type,
};
mod call_context;
mod carrier_parameters;
mod coercion;
mod comparison;
mod comparison_coercion;
mod compile_control;
mod control_flow;
mod depth_profile;
mod effects;
pub use call_context::*;
mod function;
mod largeint;
mod logical;
pub use depth_profile::{ArrowTypeDepth, ArrowTypeProfile, TypeTraversalFrame};
pub mod owned_resources;
mod partition;
mod schema;
mod semantics;
mod temporal_source;
mod type_fingerprint;
mod value_type_identity;
pub use value_type_identity::{
    write_value_layout_identity_observed, write_value_type_identity_observed,
};
mod value_layout_match;
pub use value_layout_match::{
    CarrierNullabilityPolicy, byte_view_to_offsets_projection,
    data_type_layout_matches_carrier_observed,
    data_type_layout_matches_carrier_with_profile_observed, dictionary_value_layout,
    value_layout_matches_carrier_observed, value_layout_matches_carrier_with_nullability_observed,
    value_layout_matches_carrier_with_profile_observed,
};
mod value_identity;
pub use value_identity::{
    ValueSemanticVisit, value_types_equal_observed, value_types_equal_weighted_observed,
    visit_value_type_observed,
};
mod write_input_projection;
pub use write_input_projection::{
    CheckedWriteInputProjection, WriteInputDeclaration, WriteInputLeafProjection,
};
mod value_schema;
pub use value_schema::{TypedValueBinding, ValueSchema, validate_value_layout_observed};
mod value_construction;
pub use value_construction::{
    ValueConstructionFailure, ValueConstructionRequest, clone_data_type_with_construction,
};
mod value_layout;
pub use value_layout::ValueLayout;
mod value_type;
pub use value_type::{
    ExecutionValueTypeError, ValueField, ValueKind, ValueTypeRef, ValueTypeStorageHold,
    ValueTypeStorageLifetime, ValueUnionField,
};
mod operator_function;
pub use operator_function::OperatorFunction;
mod value_arithmetic;
mod value_projection;
mod window;

pub use type_fingerprint::{
    arrow_data_type_fingerprint_observed, arrow_data_type_fingerprint_with_profile_observed,
};

pub use arithmetic::{
    ArithmeticOperator, DecimalOverflowPolicy, arithmetic_result_type,
    arithmetic_result_type_with_op, canonical_agg_decimal_type, decimal_arithmetic_result_type,
    decimal_error_policy_cast_supported, decimal_error_policy_cast_supported_observed,
    is_checked_decimal_numeric_cast,
};
pub use array_generate::array_generate_item_type;
pub use carrier_parameters::{CarrierParameterError, validate_arrow_carrier_parameters_observed};
pub use coercion::{
    supports_boolean_negation_value_type, wider_type, wider_type_with_construction,
};
pub use comparison::{ComparisonOperator, JoinKeyMatch, OrderedComparisonAlgorithm};
pub use comparison_coercion::{
    ComparisonCoercionFailure, comparison_common_type, comparison_common_value_type,
    decimal_compare_type, value_type_for_common_layout,
    value_type_for_common_layout_with_construction, wider_comparison_value_type,
};
pub use compile_control::{
    CompileCheckpoints, CompileControlError, CompileControlProvenance, CompilePhase,
    MAX_UNOBSERVED_COMPILE_WORK, PureCompileControl,
};
pub use control_flow::*;
pub use effects::*;
pub use function::{
    AggregateStateArgumentContract, AggregateStateFormatId, FunctionArgumentEvaluation,
    FunctionArgumentType, FunctionFailureBehavior, FunctionId, FunctionIdentityError,
    FunctionIntrinsicRowError, FunctionKind, FunctionOverloadId, FunctionValueType,
    FunctionVolatility,
};
pub use largeint::{LARGEINT_BYTE_WIDTH, is_largeint_data_type};
pub use logical::{
    MAX_VALUE_TYPE_DEPTH, MAX_VALUE_TYPE_NODES, NR_LOGICAL_TYPE_KEY, ValueLogicalType,
    ValueTypeError, ValueTypeVisit, field_logical_type, preserves_nested_logical_identity,
    preserves_nested_logical_identity_observed, validate_arrow_layout_structure_observed,
    validate_arrow_layout_structure_with_profile_and_scratch_observed,
    validate_arrow_layout_structure_with_profile_observed,
    validate_arrow_layout_structure_with_scratch_observed, validate_nested_logical_types,
    validate_nested_logical_types_observed, validate_value_type_structure_observed,
    validate_value_type_structure_with_profile_and_scratch_observed,
    validate_value_type_structure_with_profile_observed,
    validate_value_type_structure_with_scratch_observed,
    visit_original_data_type_clone_with_scratch_observed,
};
pub use partition::{
    BucketLayoutAlgorithm, PartitionCountParameterId, PartitionCountParameterIdentityError,
    PartitionHashAlgorithm, PartitionHashDomain, PartitionSpaceId, PartitionSpaceIdentityError,
    avalanche_hash_u64,
};
pub use schema::{
    MAX_ARROW_FIELD_METADATA_BYTES, MAX_ARROW_FIELD_METADATA_ENTRIES,
    MAX_ARROW_FIELD_METADATA_KEY_BYTES, MAX_ARROW_FIELD_METADATA_VALUE_BYTES,
    MAX_ARROW_FIELD_NAME_BYTES, MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES, TypeComparisonStep,
    arrow_data_types_equal_borrowed_observed,
    arrow_data_types_equal_borrowed_with_profile_observed, arrow_data_types_equal_observed,
    arrow_data_types_equal_with_profile_observed, arrow_data_types_exact,
    arrow_data_types_exact_borrowed_observed,
    arrow_data_types_exact_borrowed_with_profile_observed, arrow_data_types_exact_observed,
    arrow_data_types_exact_with_profile_and_nullability_observed,
    arrow_data_types_exact_with_profile_observed, arrow_fields_exact,
    arrow_fields_exact_borrowed_observed, arrow_fields_exact_observed,
    arrow_fields_exact_with_nullability_observed, arrow_schemas_exact,
};
pub use semantics::{
    AggregateStateInterpretation, AggregateStateOrderKey, BooleanValue, EvaluationDemand,
    MAX_SEMANTIC_PARAMETERS, SemanticParameterError, SemanticParameterId, SemanticParameterKey,
    SemanticParameterProjectionError, SemanticParameterProjectionVisit, SemanticParameterRef,
    SemanticParameterValue, SemanticParameters, StatementStartUtc, StatementTimeEnvironment,
};
pub use temporal_source::*;
mod temporal_type;
pub use temporal_type::{
    IntervalField, IntervalQualifier, TemporalPrecision, TemporalType, TemporalTypeError,
};
mod temporal_value;
pub use temporal_value::*;
mod time_zone_key;
pub use time_zone_key::TimeZoneKey;

pub use value_arithmetic::{
    arithmetic_result_value_shape_with_op, arithmetic_result_value_type_with_op,
    is_integer_value_type, is_numeric_value_type,
};
pub use value_projection::{
    VariantTargetTypeError, arrow_type_equals_ignoring_metadata, variant_get_target_type,
};

pub use window::{WindowBound, WindowFrame, WindowFrameExclusion, WindowFrameUnits};

#[cfg(test)]
mod temporal_source_control_tests;

mod regexp_count_source;
pub use regexp_count_source::RegexpCountPatternSource;

mod to_base64_source;
pub use to_base64_source::{
    NATIVE_V1_ENCRYPTION_FUNCTIONS, NonCanonicalNativeV1FunctionName, ToBase64ByteSource,
    native_v1_function_name,
};

pub mod result_render_type;
pub mod result_scalar_type;

pub use owned_resources::metadata_request::{CompleteMetadataRequestFacts, MetadataRequestError};
mod value_domain_cast;
pub use value_domain_cast::{
    default_json_structure_carrier_cast_supported, is_default_json_structure_cast,
    is_null_list_to_json_cast, preserves_value_domains_observed,
};
mod client_input;
pub use client_input::{ClientInputTypeBindings, ValidatedClientInputTypes};
mod value_schema_creation;
pub use value_schema_creation::{
    ValueSchemaCreation, ValueSchemaCreationFactory, ValueSchemaCreationScope,
    ValueSchemaScopeBeginFailure,
};

mod typed_fact_slice;
pub use typed_fact_slice::{
    AggregateStateOrderKeys, FunctionArgumentTypes, FunctionParameterTypes, TypedFact,
    TypedFactIntoIter, TypedFactSlice,
};

pub use value_schema::BorrowedTypedValueBinding;

#[cfg(test)]
mod weighted_comparison_tests;

#[cfg(test)]
mod depth_profile_tests;
