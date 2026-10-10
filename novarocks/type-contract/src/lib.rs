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

mod between;
pub use between::*;
mod arithmetic;
mod array_generate;
mod carrier_parameters;
mod coercion;
mod comparison;
mod comparison_coercion;
mod compile_control;
mod control_flow;
mod effects;
mod function;
mod largeint;
mod logical;
pub mod owned_resources;
mod partition;
mod schema;
mod semantics;
mod temporal_source;
mod type_fingerprint;
mod value_arithmetic;
mod value_projection;
mod window;

pub use type_fingerprint::arrow_data_type_fingerprint_observed;

pub use arithmetic::{
    ArithmeticOperator, DecimalOverflowPolicy, arithmetic_result_type,
    arithmetic_result_type_with_op, canonical_agg_decimal_type, decimal_arithmetic_result_type,
    decimal_error_policy_cast_supported, decimal_error_policy_cast_supported_observed,
    decimal_multiplication_requires_float64, is_checked_decimal_numeric_cast,
};
pub use array_generate::array_generate_item_type;
pub use carrier_parameters::{CarrierParameterError, validate_arrow_carrier_parameters_observed};
pub use coercion::wider_type;
pub use comparison::{ComparisonOperator, OrderedComparisonAlgorithm};
pub use comparison_coercion::{
    comparison_common_type, comparison_common_value_type, decimal_compare_type,
    wider_comparison_value_type,
};
pub use compile_control::{
    CompileCheckpoints, CompileControlError, CompilePhase, MAX_UNOBSERVED_COMPILE_WORK,
    PureCompileControl,
};
pub use control_flow::*;
pub use effects::*;
pub use function::{
    AggregateStateArgumentContract, AggregateStateFormatId, FunctionArgumentEvaluation,
    FunctionArgumentType, FunctionFailureBehavior, FunctionId, FunctionIdentityError,
    FunctionIntrinsicRowError, FunctionKind, FunctionOverloadId, FunctionValueType,
    FunctionVolatility, fits_nested_nullability, fits_nested_nullability_observed,
};
pub use largeint::{LARGEINT_BYTE_WIDTH, is_largeint_data_type};
pub use logical::{
    MAX_VALUE_TYPE_DEPTH, MAX_VALUE_TYPE_NODES, NR_LOGICAL_TYPE_KEY, ValueLogicalType,
    ValueTypeError, ValueTypeVisit, field_logical_type, preserves_nested_logical_identity,
    preserves_nested_logical_identity_observed, validate_nested_logical_types,
    validate_nested_logical_types_observed, validate_value_type_structure_observed,
    validate_value_type_structure_with_scratch_observed,
    visit_original_data_type_clone_with_scratch_observed,
};
pub use partition::{
    BucketLayoutAlgorithm, PartitionCountParameterId, PartitionCountParameterIdentityError,
    PartitionHashAlgorithm, PartitionSpaceId, PartitionSpaceIdentityError,
};
pub use schema::{
    MAX_ARROW_FIELD_METADATA_BYTES, MAX_ARROW_FIELD_METADATA_ENTRIES,
    MAX_ARROW_FIELD_METADATA_KEY_BYTES, MAX_ARROW_FIELD_METADATA_VALUE_BYTES,
    MAX_ARROW_FIELD_NAME_BYTES, MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES,
    arrow_data_types_equal_observed, arrow_data_types_exact,
    arrow_data_types_exact_borrowed_observed, arrow_data_types_exact_observed, arrow_fields_exact,
    arrow_fields_exact_borrowed_observed, arrow_fields_exact_observed, arrow_schemas_exact,
};
pub use semantics::{
    AggregateStateInterpretation, AggregateStateOrderKey, BooleanValue, EvaluationDemand,
    MAX_SEMANTIC_PARAMETERS, SemanticParameterError, SemanticParameterId, SemanticParameterKey,
    SemanticParameterProjectionError, SemanticParameterProjectionVisit, SemanticParameterRef,
    SemanticParameterValue, SemanticParameters,
};
pub use temporal_source::*;
pub use value_arithmetic::{
    arithmetic_result_value_type_with_op, is_integer_value_type, is_numeric_value_type,
};
pub use value_projection::{arrow_type_equals_ignoring_metadata, variant_get_target_type};

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
