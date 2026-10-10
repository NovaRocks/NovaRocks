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

//! Process-wide immutable engine function catalog.
//!
//! The catalog owns function identity, visibility and signature resolution.
//! Scalar preparation and selected-batch invocation use neutral contracts.
//! Framework-owned typed erasure resolves immutable aggregate CPU handles;
//! Execution owns state storage, group mapping, memory scopes and teardown.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, Field, IntervalUnit, TimeUnit, UnionMode};
use sha2::{Digest, Sha256};

mod aggregate_call;
mod aggregate_erasure;
pub mod aggregate_format;
mod aggregate_host_allocator;
mod aggregate_invocation_backing;
mod aggregate_kernel;
pub mod aggregate_scalar;
pub mod aggregate_scalar_fingerprint;
mod aggregate_state_column;
pub mod aggregate_types;
pub mod approx_percentile_aggregate_core;
pub mod approx_percentile_core;
mod arithmetic;
mod binding;
pub mod bit_array;
pub mod bit_numeric;
pub mod bitmap_to_string_core;
pub mod bitmap_value;
pub mod builtin;
pub mod calendar_julian;
pub mod calendar_numeric;
pub mod calendar_period_numeric;
mod call_contract;
#[cfg(test)]
mod carrier_map_tests;
pub mod carrier_text;
mod cast;
mod comparison;
pub mod control_values;
pub mod datasketches_hll;
pub mod datasketches_hll_failure;
pub mod datetime_value;
pub mod decimal_text;
mod effect_metadata;
mod effect_refinement;
mod evaluation;
mod evaluation_failure;
#[cfg(test)]
mod exact_installed_owner_selection_tests;
#[cfg(test)]
mod exact_overload_selection_tests;
pub mod exact_percentile_core;
pub mod exact_percentile_failure;
pub mod generate_series_core;
mod higher_order_call;
mod higher_order_kernel;
pub mod hll;
pub mod invocation_arity;
mod kernel_control;
mod kernel_input;
mod lambda_rows;
pub mod largeint;
pub mod largeint_text;
pub mod math_numeric;
pub mod opaque_memory;
pub mod parse_json_core;
mod parse_json_resources;
pub mod pattern_memo;
pub mod percentile_hash_core;
pub mod percentile_input;
mod pure_catalogue;
mod scalar_kernel;
pub mod selected_copy;
pub mod sketch_hash;
mod specialization;
mod table_call;
mod table_kernel;
mod temporal_call;
pub mod temporal_carrier;
mod window_call;
mod window_kernel;

pub use aggregate_call::*;
pub use aggregate_erasure::*;
pub use aggregate_kernel::*;
pub use aggregate_state_column::*;
pub use arithmetic::*;
pub use binding::*;
pub use call_contract::*;
pub use cast::*;
pub use comparison::*;
pub use effect_refinement::*;
pub use evaluation::*;
pub use evaluation_failure::*;
pub use higher_order_call::*;
pub use higher_order_kernel::*;
pub use kernel_control::*;
pub use kernel_input::validate_type_observed as validate_function_value_type_observed;
pub use kernel_input::{
    EvaluationCheckpoints, validate_argument_observed as validate_evaluated_argument_observed,
    visit_selected_nulls,
};
pub use lambda_rows::*;
pub use novarocks_constant_contract::{
    ConstantError, ConstantPolicy, ConstantPool, ConstantResourceFacts, ConstantValue,
};
pub use novarocks_type_contract::{
    AggregateStateArgumentContract, AggregateStateFormatId as AggregateStateFormatIdentity,
    FunctionArgumentEvaluation, FunctionArgumentType, FunctionFailureBehavior, FunctionId,
    FunctionIntrinsicRowError, FunctionKind, FunctionOverloadId, FunctionValueType,
    FunctionVolatility, ValueLogicalType,
};
pub use pure_catalogue::*;
pub use scalar_kernel::*;
pub use specialization::FunctionSpecializationFailure;
pub use table_call::*;
pub use table_kernel::*;
pub use temporal_call::{PreparedTemporalSource, TemporalCallContract, TemporalSourceChannel};
pub use window_call::*;
pub use window_kernel::*;

const FUNCTION_CATALOG_DIGEST_DOMAIN: &[u8] = b"novarocks.engine-function-catalog/v7\0";
const RESOLVED_AGGREGATE_DIGEST_DOMAIN: &[u8] = b"novarocks.resolved-aggregate/v1\0";

const fn function_kind_tag(kind: FunctionKind) -> u8 {
    match kind {
        FunctionKind::Scalar => 1,
        FunctionKind::Aggregate => 2,
        FunctionKind::Window => 3,
        FunctionKind::Table => 4,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FunctionVisibility {
    Public,
    Hidden,
}

impl FunctionVisibility {
    const fn tag(self) -> u8 {
        match self {
            Self::Public => 1,
            Self::Hidden => 2,
        }
    }
}

const fn function_volatility_tag(volatility: FunctionVolatility) -> u8 {
    match volatility {
        FunctionVolatility::Immutable => 1,
        FunctionVolatility::Volatile => 2,
        FunctionVolatility::Stable => 3,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedFunctionSignature {
    pub return_type: DataType,
    pub argument_types: Vec<DataType>,
    pub enforce_argument_binding: bool,
}

/// Stable identity of one exact aggregate overload.
///
/// This is engine function metadata, not a Connector or provider identifier.
/// It must change whenever the logical overload itself is replaced rather than
/// evolved compatibly.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AggregateOverloadIdentity(Box<str>);

impl AggregateOverloadIdentity {
    pub fn try_new(value: impl AsRef<str>) -> Result<Self, FunctionCatalogError> {
        let value = value.as_ref();
        validate_stable_identity("aggregate overload", value)?;
        Ok(Self(value.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Stable compatibility identity of one executable aggregate implementation.
///
/// This is not a provider ABI, dynamic loading contract, or callable handle.
/// It is immutable process-composition metadata used to keep two native roles
/// out of the same compatibility island when their aggregate implementations
/// cannot safely participate in one distributed aggregation.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AggregateImplementationIdentity(Box<str>);

impl AggregateImplementationIdentity {
    pub fn try_new(value: impl AsRef<str>) -> Result<Self, FunctionCatalogError> {
        let value = value.as_ref();
        validate_stable_identity("aggregate implementation", value)?;
        Ok(Self(value.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One declared aggregate overload pattern in the immutable process catalog.
///
/// Patterns are canonical compatibility identities, not a second type system.
/// The family resolver owns parametric matching (for example `T -> list<T>`)
/// and returns concrete Arrow types. The catalog validates that the returned
/// overload and state format were declared here.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AggregateOverloadDeclaration {
    pub identity: AggregateOverloadIdentity,
    pub argument_pattern: Box<str>,
    pub intermediate_pattern: Box<str>,
    pub output_pattern: Box<str>,
    pub state_format: AggregateStateFormatIdentity,
}

impl AggregateOverloadDeclaration {
    pub fn try_new(
        identity: impl AsRef<str>,
        argument_pattern: impl AsRef<str>,
        intermediate_pattern: impl AsRef<str>,
        output_pattern: impl AsRef<str>,
        state_format: impl AsRef<str>,
    ) -> Result<Self, FunctionCatalogError> {
        let argument_pattern = argument_pattern.as_ref();
        let intermediate_pattern = intermediate_pattern.as_ref();
        let output_pattern = output_pattern.as_ref();
        validate_signature_pattern("aggregate argument", argument_pattern)?;
        validate_signature_pattern("aggregate intermediate", intermediate_pattern)?;
        validate_signature_pattern("aggregate output", output_pattern)?;
        Ok(Self {
            identity: AggregateOverloadIdentity::try_new(identity)?,
            argument_pattern: argument_pattern.into(),
            intermediate_pattern: intermediate_pattern.into(),
            output_pattern: output_pattern.into(),
            state_format: canonical_aggregate_state_format(state_format)?,
        })
    }
}

/// Complete native-compatibility metadata for one exact aggregate overload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AggregateOverloadMetadata {
    pub identity: AggregateOverloadIdentity,
    pub argument_types: Box<[DataType]>,
    pub intermediate_type: DataType,
    pub output_type: DataType,
    pub state_format: AggregateStateFormatIdentity,
}

impl AggregateOverloadMetadata {
    pub fn try_new(
        identity: impl AsRef<str>,
        argument_types: impl IntoIterator<Item = DataType>,
        intermediate_type: DataType,
        output_type: DataType,
        state_format: impl AsRef<str>,
    ) -> Result<Self, FunctionCatalogError> {
        Ok(Self {
            identity: AggregateOverloadIdentity::try_new(identity)?,
            argument_types: argument_types.into_iter().collect(),
            intermediate_type,
            output_type,
            state_format: canonical_aggregate_state_format(state_format)?,
        })
    }
}

/// Exact aggregate selection frozen into a plan or prepared kernel.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ResolvedAggregateSignature {
    pub overload: AggregateOverloadIdentity,
    /// Unpacked channels consumed by the executable update kernel. For an
    /// ordered aggregate this is the logical SQL arguments followed by the
    /// function ORDER BY expressions.
    pub argument_types: Vec<DataType>,
    pub intermediate_type: DataType,
    pub output_type: DataType,
    pub state_format: AggregateStateFormatIdentity,
}

impl From<&AggregateOverloadMetadata> for ResolvedAggregateSignature {
    fn from(metadata: &AggregateOverloadMetadata) -> Self {
        Self {
            overload: metadata.identity.clone(),
            argument_types: metadata.argument_types.to_vec(),
            intermediate_type: metadata.intermediate_type.clone(),
            output_type: metadata.output_type.clone(),
            state_format: metadata.state_format.clone(),
        }
    }
}

impl ResolvedAggregateSignature {
    /// Compatibility digest for the concrete overload frozen by planning.
    pub fn compatibility_digest(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(RESOLVED_AGGREGATE_DIGEST_DOMAIN);
        digest_text(&mut hasher, self.overload.as_str());
        hasher.update(
            u32::try_from(self.argument_types.len())
                .expect("aggregate argument count fits u32")
                .to_be_bytes(),
        );
        for argument_type in &self.argument_types {
            digest_data_type(&mut hasher, argument_type);
        }
        digest_data_type(&mut hasher, &self.intermediate_type);
        digest_data_type(&mut hasher, &self.output_type);
        digest_text(&mut hasher, self.state_format.as_str());
        hasher.finalize().into()
    }
}

/// Safe parametric aggregate signature resolver.
pub trait AggregateSignatureResolver: Send + Sync {
    /// Declare the complete value domains admitted by this family before any
    /// carrier-only kernel signature is materialized.
    fn validate_value_arguments(
        &self,
        argument_types: &[novarocks_type_contract::FunctionValueType],
    ) -> Result<(), FunctionResolutionError> {
        for argument in argument_types {
            argument
                .validate()
                .map_err(|error| FunctionResolutionError::BadSignature(error.to_string()))?;
            if argument.logical_type != novarocks_type_contract::ValueLogicalType::Physical {
                return Err(FunctionResolutionError::BadSignature(
                    "aggregate family does not declare this root logical identity".into(),
                ));
            }
        }
        Ok(())
    }

    fn resolve_aggregate(
        &self,
        argument_types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError>;

    /// Whether this aggregate family explicitly accepts function ORDER BY
    /// expressions as additional executable update channels.
    fn supports_ordered_update_channels(&self) -> bool {
        false
    }

    /// The original owner declares how this exact overload compares logical
    /// argument metadata across state-producing and state-consuming phases.
    /// This is not a runtime-success, value-equality or source-lineage proof.
    fn state_argument_contract(
        &self,
        _selected_overload: &AggregateOverloadIdentity,
    ) -> Result<novarocks_type_contract::AggregateStateArgumentContract, FunctionResolutionError>
    {
        Ok(novarocks_type_contract::AggregateStateArgumentContract::ExactSignature)
    }

    /// Whether this aggregate can produce NULL.
    ///
    /// Most can: an aggregate over no rows has nothing to return. One that
    /// always has an answer - a count, or a sketch whose empty form is still a
    /// sketch - says so, because a plan that declares a value nullable when it
    /// never is carries a null check no row will ever take.
    fn produces_null(&self) -> bool {
        true
    }

    /// Materialize the exact execution signature for an already-selected
    /// logical overload. The default refuses missing fixed-overload authors;
    /// it never runs candidate resolution as a substitute. Ordered aggregate
    /// families must declare their complete update channels explicitly.
    fn resolve_update_signature(
        &self,
        selected_overload: &AggregateOverloadIdentity,
        update_argument_types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        let _ = (selected_overload, update_argument_types);
        Err(FunctionResolutionError::BadSignature(
            "aggregate family does not declare fixed-overload update binding".into(),
        ))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FunctionResolutionError {
    Control(novarocks_type_contract::CompileControlError),
    UnknownFunction,
    HiddenFunction,
    NoMatchingSignature {
        candidates: usize,
        binding_enforced: bool,
    },
    BadSignature(String),
}

impl fmt::Display for FunctionResolutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(formatter),
            Self::UnknownFunction => formatter.write_str("function not registered"),
            Self::HiddenFunction => formatter.write_str("function is hidden from user SQL"),
            Self::NoMatchingSignature { candidates, .. } => write!(
                formatter,
                "no matching signature among {candidates} registered candidates"
            ),
            Self::BadSignature(message) => write!(formatter, "bad signature: {message}"),
        }
    }
}

impl std::error::Error for FunctionResolutionError {}

impl From<novarocks_type_contract::CompileControlError> for FunctionResolutionError {
    fn from(error: novarocks_type_contract::CompileControlError) -> Self {
        Self::Control(error)
    }
}

impl FunctionResolutionError {
    pub fn control_error(&self) -> Option<novarocks_type_contract::CompileControlError> {
        match self {
            Self::Control(error) => Some(*error),
            _ => None,
        }
    }
}

#[cfg(test)]
pub fn binding_test_control() -> &'static dyn novarocks_type_contract::PureCompileControl {
    struct Control;
    impl novarocks_type_contract::PureCompileControl for Control {
        fn checkpoint(
            &self,
            _: novarocks_type_contract::CompilePhase,
            _: u32,
        ) -> Result<(), novarocks_type_contract::CompileControlError> {
            Ok(())
        }
    }
    &Control
}

/// Safe type-level resolver supplied by a statically linked function bundle.
pub trait FunctionSignatureResolver: Send + Sync {
    fn resolve(
        &self,
        argument_types: &[DataType],
    ) -> Result<ResolvedFunctionSignature, FunctionResolutionError>;
}

#[derive(Clone, Copy, Debug)]
pub struct AggregateInputBatch<'batch> {
    values: Option<&'batch ArrayRef>,
    row_count: usize,
}

impl<'batch> AggregateInputBatch<'batch> {
    pub fn try_new(
        values: Option<&'batch ArrayRef>,
        row_count: usize,
    ) -> Result<Self, AggregateInputBatchError> {
        if let Some(actual) = values
            .map(|values| values.len())
            .filter(|actual| *actual != row_count)
        {
            return Err(AggregateInputBatchError::LengthMismatch {
                expected: row_count,
                actual,
            });
        }
        Ok(Self { values, row_count })
    }

    /// Returns the single argument array, or a packed `StructArray` for a
    /// multi-argument overload. Zero-argument aggregates receive `None`.
    pub fn values(&self) -> Option<&'batch dyn Array> {
        self.values.map(ArrayRef::as_ref)
    }

    /// Returns the Arrow owner used by Execution-private compatibility
    /// adapters. Typed contributors should prefer [`Self::values`].
    pub fn array_ref(&self) -> Option<&'batch ArrayRef> {
        self.values
    }

    pub const fn row_count(&self) -> usize {
        self.row_count
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AggregateInputBatchError {
    LengthMismatch { expected: usize, actual: usize },
}

impl fmt::Display for AggregateInputBatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LengthMismatch { expected, actual } => write!(
                formatter,
                "aggregate input has {actual} rows, expected {expected}"
            ),
        }
    }
}

impl std::error::Error for AggregateInputBatchError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AggregateOrderKey {
    pub ascending: bool,
    pub nulls_first: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AggregateBindOptions {
    distinct: bool,
    order_keys: Box<[AggregateOrderKey]>,
    max_output_bytes: Option<usize>,
}

impl AggregateBindOptions {
    pub fn try_new(
        distinct: bool,
        ascending: &[bool],
        nulls_first: &[bool],
        max_output_bytes: Option<i64>,
    ) -> Result<Self, AggregateBindOptionsError> {
        if ascending.len() != nulls_first.len() {
            return Err(AggregateBindOptionsError::OrderKeyLengthMismatch {
                ascending: ascending.len(),
                nulls_first: nulls_first.len(),
            });
        }
        let max_output_bytes = max_output_bytes
            .map(|value| {
                usize::try_from(value)
                    .map_err(|_| AggregateBindOptionsError::InvalidMaxOutputBytes(value))
            })
            .transpose()?;
        Ok(Self {
            distinct,
            order_keys: ascending
                .iter()
                .copied()
                .zip(nulls_first.iter().copied())
                .map(|(ascending, nulls_first)| AggregateOrderKey {
                    ascending,
                    nulls_first,
                })
                .collect(),
            max_output_bytes,
        })
    }

    pub const fn distinct(&self) -> bool {
        self.distinct
    }

    pub fn order_keys(&self) -> &[AggregateOrderKey] {
        &self.order_keys
    }

    pub const fn max_output_bytes(&self) -> Option<usize> {
        self.max_output_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AggregateBindOptionsError {
    OrderKeyLengthMismatch {
        ascending: usize,
        nulls_first: usize,
    },
    InvalidMaxOutputBytes(i64),
}

impl fmt::Display for AggregateBindOptionsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OrderKeyLengthMismatch {
                ascending,
                nulls_first,
            } => write!(
                formatter,
                "aggregate ORDER BY has {ascending} direction entries but {nulls_first} null-order entries"
            ),
            Self::InvalidMaxOutputBytes(value) => {
                write!(
                    formatter,
                    "aggregate max output bytes must be non-negative, got {value}"
                )
            }
        }
    }
}

impl std::error::Error for AggregateBindOptionsError {}

/// A safe aggregate kernel implemented by a statically linked function bundle.
///
/// The trait deliberately is not object safe. Execution monomorphizes one
/// private adapter per family and alone owns state type erasure, arena
/// placement, and destruction. Prepared batches can borrow vectorized input,
/// so neither update nor merge requires a per-row or per-group allocation.
pub trait TypedAggregateKernel: Send + Sync + 'static {
    type State: Send + 'static;
    type PreparedUpdateBatch<'batch>
    where
        Self: 'batch;
    type PreparedMergeBatch<'batch>
    where
        Self: 'batch;
    type Error: fmt::Display + Send + Sync + 'static;

    /// Declares the complete retained-memory contract for one state.
    ///
    /// Execution uses this contract to reserve mutation headroom before
    /// invoking provider code. Implementations must never retain more than a
    /// declared bound; unbounded post-allocation reconciliation is not a
    /// supported hard-limit policy.
    fn memory_policy(&self) -> AggregateStateMemoryPolicy;

    fn create_state(&self) -> Result<Self::State, Self::Error>;

    fn prepare_update<'batch>(
        &self,
        input: &'batch AggregateInputBatch<'batch>,
    ) -> Result<Self::PreparedUpdateBatch<'batch>, Self::Error>;

    fn update_row<'batch>(
        &self,
        state: &mut Self::State,
        prepared: &Self::PreparedUpdateBatch<'batch>,
        row: usize,
    ) -> Result<(), Self::Error>;

    fn prepare_merge<'batch>(
        &self,
        input: &'batch AggregateInputBatch<'batch>,
    ) -> Result<Self::PreparedMergeBatch<'batch>, Self::Error>;

    fn merge_row<'batch>(
        &self,
        state: &mut Self::State,
        prepared: &Self::PreparedMergeBatch<'batch>,
        row: usize,
    ) -> Result<(), Self::Error>;

    fn build_intermediate<'state, I>(&self, states: I) -> Result<ArrayRef, Self::Error>
    where
        Self::State: 'state,
        I: ExactSizeIterator<Item = &'state Self::State>;

    fn build_final<'state, I>(&self, states: I) -> Result<ArrayRef, Self::Error>
    where
        Self::State: 'state,
        I: ExactSizeIterator<Item = &'state Self::State>;

    /// Returns additional heap memory retained by `state` in O(1).
    ///
    /// The inline `size_of::<State>()` bytes are owned and accounted by the
    /// execution arena and must not be included. Implementations should cache
    /// recursive/container totals in the state when deriving them would
    /// otherwise require a scan.
    fn retained_bytes(&self, state: &Self::State) -> usize;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggregateStateMemoryPolicy {
    /// The state never owns memory outside its inline arena body.
    FixedZero,
    /// Every state-owned allocation is authorized and released by the explicit
    /// host allocator. Retained bytes are a fact, never a capacity grant.
    AllocationTracked,
    /// The state may retain heap memory up to this per-state bound.
    ///
    /// Execution reserves the unconsumed headroom before every state mutation,
    /// then reconciles the reservation down to the exact retained size.
    BoundedRetained { max_retained_bytes_per_state: usize },
}

/// One safe, statically linked aggregate family.
///
/// A family may expose several exact logical overloads. Its concrete kernel
/// type may itself be an enum when overloads use different state shapes; that
/// choice remains private to the contributor and never becomes a raw ABI.
pub trait TypedAggregateFamily: Send + Sync + 'static {
    type Kernel: TypedAggregateKernel;
    type PrepareError: fmt::Display + Send + Sync + 'static;

    /// Declare the complete value domains admitted by this family before any
    /// carrier-only kernel signature is materialized.
    fn validate_value_arguments(
        &self,
        argument_types: &[novarocks_type_contract::FunctionValueType],
    ) -> Result<(), FunctionResolutionError> {
        for argument in argument_types {
            argument
                .validate()
                .map_err(|error| FunctionResolutionError::BadSignature(error.to_string()))?;
            if argument.logical_type != novarocks_type_contract::ValueLogicalType::Physical {
                return Err(FunctionResolutionError::BadSignature(
                    "aggregate family does not declare this root logical identity".into(),
                ));
            }
        }
        Ok(())
    }

    fn overloads(&self) -> &[AggregateOverloadDeclaration];

    fn resolve_signature(
        &self,
        argument_types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError>;

    fn supports_ordered_update_channels(&self) -> bool {
        false
    }

    /// The original family declares how this exact overload compares logical
    /// argument metadata across state-producing and state-consuming phases.
    /// This is not a runtime-success, value-equality or source-lineage proof.
    fn state_argument_contract(
        &self,
        _selected_overload: &AggregateOverloadIdentity,
    ) -> Result<novarocks_type_contract::AggregateStateArgumentContract, FunctionResolutionError>
    {
        Ok(novarocks_type_contract::AggregateStateArgumentContract::ExactSignature)
    }

    /// Whether this family can produce NULL. See
    /// [`AggregateSignatureResolver::produces_null`].
    fn produces_null(&self) -> bool {
        true
    }

    fn resolve_update_signature(
        &self,
        selected_overload: &AggregateOverloadIdentity,
        update_argument_types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        let _ = (selected_overload, update_argument_types);
        Err(FunctionResolutionError::BadSignature(
            "typed aggregate family does not declare fixed-overload update binding".into(),
        ))
    }

    fn prepare(
        &self,
        selected: &ResolvedAggregateSignature,
        options: &AggregateBindOptions,
    ) -> Result<Self::Kernel, Self::PrepareError>;
}

/// Atomically validated metadata and implementation for one typed aggregate
/// family. Process composition can split this into the immutable catalog entry
/// and a generic Execution-private adapter without exposing erased state.
pub struct TypedAggregateRegistration<F> {
    definition: FunctionDefinition,
    implementation: AggregateImplementationIdentity,
    family: Arc<F>,
}

impl<F: TypedAggregateFamily> TypedAggregateRegistration<F> {
    pub fn try_new(
        canonical_name: impl AsRef<str>,
        visibility: FunctionVisibility,
        volatility: FunctionVolatility,
        implementation: AggregateImplementationIdentity,
        family: F,
    ) -> Result<Self, FunctionCatalogError> {
        let family = Arc::new(family);
        let definition = FunctionDefinition::try_new_parametric_aggregate(
            canonical_name,
            visibility,
            volatility,
            family.overloads().iter().cloned(),
            Arc::new(TypedFamilySignatureResolver {
                family: Arc::clone(&family),
            }),
        )?;
        Ok(Self {
            definition,
            implementation,
            family,
        })
    }

    pub fn definition(&self) -> &FunctionDefinition {
        &self.definition
    }

    pub fn family(&self) -> &Arc<F> {
        &self.family
    }

    pub fn implementation(&self) -> &AggregateImplementationIdentity {
        &self.implementation
    }

    /// Attach selected CPU preparation while retaining this registration's
    /// original typed family and implementation. Only explicit effect facts
    /// may be added: identity, overloads, patterns, state interpretation and
    /// legacy semantics must agree with the original binding declaration.
    pub fn try_attach_pure_aggregate<O>(mut self, owner: Arc<O>) -> Result<Self, PureCatalogError>
    where
        O: PureFunctionMetadataOwner
            + PureAggregateImplementation
            + AggregateSignatureResolver
            + 'static,
    {
        let original = self
            .definition
            .binding_declaration()
            .expect("typed registration authors its binding declaration");
        let pure = owner.binding_declaration();
        let matches = original.function_id() == pure.function_id()
            && original.kind() == pure.kind()
            && original.overloads().len() == pure.overloads().len()
            && original
                .overloads()
                .iter()
                .zip(pure.overloads())
                .all(|(old, new)| {
                    old.identity == new.identity
                        && old.semantics == new.semantics
                        && old.argument_pattern == new.argument_pattern
                        && old.result_pattern == new.result_pattern
                        && old.aggregate == new.aggregate
                        && old
                            .effects
                            .as_ref()
                            .is_none_or(|effects| new.effects.as_ref() == Some(effects))
                });
        if !matches {
            return Err(FunctionBindingError::InvalidBinding(
                "pure aggregate attachment differs from its original typed declaration".into(),
            )
            .into());
        }
        let mut definition = FunctionDefinition::try_new_pure_aggregate(
            self.definition.canonical_name(),
            self.definition.visibility,
            owner,
        )?;
        definition
            .binding
            .as_mut()
            .expect("pure attachment authors its binding")
            .retain_resolver_from(
                self.definition
                    .binding
                    .as_ref()
                    .expect("typed registration authors its binding"),
            );
        definition.aggregate_resolver = self.definition.aggregate_resolver.clone();
        self.definition = definition;
        Ok(self)
    }

    pub fn into_parts(self) -> (FunctionDefinition, AggregateImplementationIdentity, Arc<F>) {
        (self.definition, self.implementation, self.family)
    }
}

struct TypedFamilySignatureResolver<F> {
    family: Arc<F>,
}

impl<F: TypedAggregateFamily> AggregateSignatureResolver for TypedFamilySignatureResolver<F> {
    fn validate_value_arguments(
        &self,
        argument_types: &[novarocks_type_contract::FunctionValueType],
    ) -> Result<(), FunctionResolutionError> {
        self.family.validate_value_arguments(argument_types)
    }

    fn resolve_aggregate(
        &self,
        argument_types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        self.family.resolve_signature(argument_types)
    }

    fn supports_ordered_update_channels(&self) -> bool {
        self.family.supports_ordered_update_channels()
    }

    fn state_argument_contract(
        &self,
        selected_overload: &AggregateOverloadIdentity,
    ) -> Result<novarocks_type_contract::AggregateStateArgumentContract, FunctionResolutionError>
    {
        self.family.state_argument_contract(selected_overload)
    }

    fn produces_null(&self) -> bool {
        self.family.produces_null()
    }

    fn resolve_update_signature(
        &self,
        selected_overload: &AggregateOverloadIdentity,
        update_argument_types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        self.family
            .resolve_update_signature(selected_overload, update_argument_types)
    }
}

#[derive(Clone)]
pub struct FunctionDefinition {
    canonical_name: Box<str>,
    kind: FunctionKind,
    visibility: FunctionVisibility,
    volatility: FunctionVolatility,
    canonical_signatures: Box<[Box<str>]>,
    aggregate_overloads: Box<[AggregateOverloadDeclaration]>,
    exact_aggregate_overloads: Box<[AggregateOverloadMetadata]>,
    aggregate_resolver: Option<Arc<dyn AggregateSignatureResolver>>,
    resolver: Option<Arc<dyn FunctionSignatureResolver>>,
    binding: Option<binding::FunctionBindingDefinition>,
}

impl fmt::Debug for FunctionDefinition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FunctionDefinition")
            .field("canonical_name", &self.canonical_name)
            .field("kind", &self.kind)
            .field("visibility", &self.visibility)
            .field("volatility", &self.volatility)
            .field("canonical_signatures", &self.canonical_signatures)
            .field("aggregate_overloads", &self.aggregate_overloads)
            .field("binding", &self.binding)
            .finish_non_exhaustive()
    }
}

impl FunctionDefinition {
    pub fn try_new(
        canonical_name: impl AsRef<str>,
        kind: FunctionKind,
        visibility: FunctionVisibility,
        volatility: FunctionVolatility,
        canonical_signatures: impl IntoIterator<Item = impl AsRef<str>>,
        resolver: Arc<dyn FunctionSignatureResolver>,
    ) -> Result<Self, FunctionCatalogError> {
        let canonical_name = canonical_name.as_ref();
        validate_canonical_name(canonical_name)?;
        let mut canonical_signatures = canonical_signatures
            .into_iter()
            .map(|signature| signature.as_ref().to_string().into_boxed_str())
            .collect::<Vec<_>>();
        if canonical_signatures.is_empty() {
            return Err(FunctionCatalogError::EmptySignatureSet {
                name: canonical_name.into(),
            });
        }
        canonical_signatures.sort_unstable();
        for pair in canonical_signatures.windows(2) {
            if pair[0] == pair[1] {
                return Err(FunctionCatalogError::DuplicateSignature {
                    name: canonical_name.into(),
                    signature: pair[0].clone(),
                });
            }
        }
        if let Some(signature) = canonical_signatures.iter().find(|value| value.is_empty()) {
            return Err(FunctionCatalogError::EmptySignature {
                name: canonical_name.into(),
                signature: signature.clone(),
            });
        }
        Ok(Self {
            canonical_name: canonical_name.into(),
            kind,
            visibility,
            volatility,
            canonical_signatures: canonical_signatures.into_boxed_slice(),
            aggregate_overloads: Box::default(),
            exact_aggregate_overloads: Box::default(),
            aggregate_resolver: None,
            resolver: Some(resolver),
            binding: None,
        })
    }

    pub fn try_new_exact_aggregate(
        canonical_name: impl AsRef<str>,
        visibility: FunctionVisibility,
        volatility: FunctionVolatility,
        overloads: impl IntoIterator<Item = AggregateOverloadMetadata>,
    ) -> Result<Self, FunctionCatalogError> {
        let canonical_name = canonical_name.as_ref();
        validate_canonical_name(canonical_name)?;
        let overloads = validate_aggregate_overloads(canonical_name, overloads)?;
        let declarations = overloads
            .iter()
            .map(exact_overload_declaration)
            .collect::<Result<Vec<_>, _>>()?;
        let exact_resolver = Arc::new(ExactAggregateResolver {
            overloads: overloads.clone(),
        });
        let aggregate_resolver: Arc<dyn AggregateSignatureResolver> = exact_resolver.clone();
        let resolver: Arc<dyn FunctionSignatureResolver> = exact_resolver;
        Ok(Self {
            canonical_name: canonical_name.into(),
            kind: FunctionKind::Aggregate,
            visibility,
            volatility,
            canonical_signatures: declarations
                .iter()
                .map(|declaration| declaration.argument_pattern.clone())
                .collect(),
            aggregate_overloads: declarations.into_boxed_slice(),
            exact_aggregate_overloads: overloads,
            aggregate_resolver: Some(aggregate_resolver),
            resolver: Some(resolver),
            binding: None,
        })
    }

    /// Register an aggregate whose overloads are resolved by a typed family.
    ///
    /// The binding declaration is derived here rather than asked for. An
    /// aggregate overload already states everything a binding needs - its
    /// identity, what it takes, what it returns, and what its state looks like
    /// - so asking a caller to restate it invites the two halves to disagree,
    /// and leaving it out produces a function that is named everywhere and
    /// resolvable nowhere.
    pub fn try_new_parametric_aggregate(
        canonical_name: impl AsRef<str>,
        visibility: FunctionVisibility,
        volatility: FunctionVolatility,
        overloads: impl IntoIterator<Item = AggregateOverloadDeclaration>,
        aggregate_resolver: Arc<dyn AggregateSignatureResolver>,
    ) -> Result<Self, FunctionCatalogError> {
        let canonical_name = canonical_name.as_ref();
        validate_canonical_name(canonical_name)?;
        let overloads = validate_aggregate_declarations(canonical_name, overloads)?;
        let resolver: Arc<dyn FunctionSignatureResolver> =
            Arc::new(LegacyAggregateSignatureResolver {
                aggregate_resolver: Arc::clone(&aggregate_resolver),
            });
        let binding = parametric_aggregate_binding(
            canonical_name,
            volatility,
            &overloads,
            Arc::clone(&aggregate_resolver),
        )?;
        Ok(Self {
            canonical_name: canonical_name.into(),
            kind: FunctionKind::Aggregate,
            visibility,
            volatility,
            canonical_signatures: overloads
                .iter()
                .map(|overload| overload.argument_pattern.clone())
                .collect(),
            aggregate_overloads: overloads,
            exact_aggregate_overloads: Box::default(),
            aggregate_resolver: Some(aggregate_resolver),
            resolver: Some(resolver),
            binding: Some(binding),
        })
    }

    pub fn canonical_name(&self) -> &str {
        &self.canonical_name
    }

    pub const fn kind(&self) -> FunctionKind {
        self.kind
    }

    pub const fn visibility(&self) -> FunctionVisibility {
        self.visibility
    }

    pub const fn volatility(&self) -> FunctionVolatility {
        self.volatility
    }

    pub fn canonical_signatures(&self) -> &[Box<str>] {
        &self.canonical_signatures
    }

    pub fn aggregate_overloads(&self) -> &[AggregateOverloadDeclaration] {
        &self.aggregate_overloads
    }
}

struct ExactAggregateResolver {
    overloads: Box<[AggregateOverloadMetadata]>,
}

/// The typed signature contract of an aggregate whose overloads are exact.
///
/// Resolution is a match against the declared overloads, so an aggregate that
/// declares them has no reason to write its own contract - and writing one is
/// where the two can disagree.
pub fn exact_aggregate_signature_contract(
    overloads: impl IntoIterator<Item = AggregateOverloadMetadata>,
) -> Arc<dyn AggregateSignatureResolver> {
    Arc::new(ExactAggregateResolver {
        overloads: overloads.into_iter().collect(),
    })
}

impl FunctionSignatureResolver for ExactAggregateResolver {
    fn resolve(
        &self,
        argument_types: &[DataType],
    ) -> Result<ResolvedFunctionSignature, FunctionResolutionError> {
        let selected = select_exact_aggregate_overload(&self.overloads, argument_types)?;
        Ok(ResolvedFunctionSignature {
            return_type: selected.output_type.clone(),
            argument_types: selected.argument_types.to_vec(),
            enforce_argument_binding: true,
        })
    }
}

impl AggregateSignatureResolver for ExactAggregateResolver {
    fn resolve_aggregate(
        &self,
        argument_types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        select_exact_aggregate_overload(&self.overloads, argument_types).map(Into::into)
    }
    fn resolve_update_signature(
        &self,
        selected_overload: &AggregateOverloadIdentity,
        argument_types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        let selected = self
            .overloads
            .iter()
            .find(|entry| &entry.identity == selected_overload)
            .ok_or_else(|| {
                FunctionResolutionError::BadSignature(
                    "exact aggregate overload is not declared by this family".into(),
                )
            })?;
        if selected.argument_types.len() != argument_types.len()
            || !selected
                .argument_types
                .iter()
                .zip(argument_types)
                .all(|(expected, actual)| {
                    novarocks_type_contract::arrow_data_types_exact(expected, actual)
                })
        {
            return Err(FunctionResolutionError::NoMatchingSignature {
                candidates: 1,
                binding_enforced: true,
            });
        }
        Ok(selected.into())
    }
}

struct LegacyAggregateSignatureResolver {
    aggregate_resolver: Arc<dyn AggregateSignatureResolver>,
}

impl FunctionSignatureResolver for LegacyAggregateSignatureResolver {
    fn resolve(
        &self,
        argument_types: &[DataType],
    ) -> Result<ResolvedFunctionSignature, FunctionResolutionError> {
        let resolved = self.aggregate_resolver.resolve_aggregate(argument_types)?;
        Ok(ResolvedFunctionSignature {
            return_type: resolved.output_type,
            argument_types: resolved.argument_types,
            enforce_argument_binding: true,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FunctionCatalogError {
    EmptyCatalog,
    DuplicateFunctionIdentity {
        identity: FunctionId,
    },
    MissingBindingDeclaration {
        name: Box<str>,
        kind: FunctionKind,
    },
    InvalidCanonicalName {
        name: Box<str>,
    },
    EmptySignatureSet {
        name: Box<str>,
    },
    EmptySignature {
        name: Box<str>,
        signature: Box<str>,
    },
    DuplicateSignature {
        name: Box<str>,
        signature: Box<str>,
    },
    DuplicateDefinition {
        name: Box<str>,
        kind: FunctionKind,
    },
    InvalidStableIdentity {
        subject: &'static str,
        value: Box<str>,
    },
    DuplicateOverloadIdentity {
        name: Box<str>,
        identity: AggregateOverloadIdentity,
    },
    AmbiguousAggregateOverload {
        name: Box<str>,
        argument_types: Box<[DataType]>,
    },
    AmbiguousAggregatePattern {
        name: Box<str>,
        argument_pattern: Box<str>,
    },
}

impl fmt::Display for FunctionCatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyCatalog => formatter.write_str("engine function catalog is empty"),
            Self::DuplicateFunctionIdentity { identity } => {
                write!(
                    formatter,
                    "duplicate function identity `{}`",
                    identity.as_str()
                )
            }
            Self::MissingBindingDeclaration { name, kind } => {
                write!(
                    formatter,
                    "{kind:?} function `{name}` has no exact binding declaration"
                )
            }
            Self::InvalidCanonicalName { name } => {
                write!(formatter, "invalid canonical function name `{name}`")
            }
            Self::EmptySignatureSet { name } => {
                write!(formatter, "function `{name}` has no declared signatures")
            }
            Self::EmptySignature { name, .. } => {
                write!(formatter, "function `{name}` has an empty signature")
            }
            Self::DuplicateSignature { name, signature } => write!(
                formatter,
                "function `{name}` declares duplicate signature `{signature}`"
            ),
            Self::DuplicateDefinition { name, kind } => {
                write!(formatter, "duplicate {kind:?} function definition `{name}`")
            }
            Self::InvalidStableIdentity { subject, value } => {
                write!(formatter, "invalid {subject} identity `{value}`")
            }
            Self::DuplicateOverloadIdentity { name, identity } => write!(
                formatter,
                "aggregate function `{name}` declares duplicate overload identity `{}`",
                identity.as_str()
            ),
            Self::AmbiguousAggregateOverload {
                name,
                argument_types,
            } => write!(
                formatter,
                "aggregate function `{name}` declares ambiguous exact argument types {argument_types:?}"
            ),
            Self::AmbiguousAggregatePattern {
                name,
                argument_pattern,
            } => write!(
                formatter,
                "aggregate function `{name}` declares ambiguous argument pattern `{argument_pattern}`"
            ),
        }
    }
}

impl std::error::Error for FunctionCatalogError {}

fn validate_canonical_name(name: &str) -> Result<(), FunctionCatalogError> {
    let valid = !name.is_empty()
        && name.len() <= u16::MAX as usize
        && name.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'$')
        });
    if !valid {
        return Err(FunctionCatalogError::InvalidCanonicalName { name: name.into() });
    }
    Ok(())
}

fn validate_stable_identity(
    subject: &'static str,
    value: &str,
) -> Result<(), FunctionCatalogError> {
    let valid = !value.is_empty()
        && value.len() <= u16::MAX as usize
        && value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && !matches!(byte, b'|' | b','));
    if !valid {
        return Err(FunctionCatalogError::InvalidStableIdentity {
            subject,
            value: value.into(),
        });
    }
    Ok(())
}

fn canonical_aggregate_state_format(
    value: impl AsRef<str>,
) -> Result<AggregateStateFormatIdentity, FunctionCatalogError> {
    let value = value.as_ref();
    AggregateStateFormatIdentity::try_new(value).map_err(|_| {
        FunctionCatalogError::InvalidStableIdentity {
            subject: "aggregate state format",
            value: value.into(),
        }
    })
}

fn validate_signature_pattern(
    subject: &'static str,
    value: &str,
) -> Result<(), FunctionCatalogError> {
    let valid = !value.is_empty()
        && value.len() <= u16::MAX as usize
        && value
            .bytes()
            .all(|byte| byte.is_ascii() && !byte.is_ascii_control());
    if !valid {
        return Err(FunctionCatalogError::InvalidStableIdentity {
            subject,
            value: value.into(),
        });
    }
    Ok(())
}

fn exact_overload_declaration(
    overload: &AggregateOverloadMetadata,
) -> Result<AggregateOverloadDeclaration, FunctionCatalogError> {
    let identity = overload.identity.as_str();
    AggregateOverloadDeclaration::try_new(
        identity,
        format!("exact:{identity}:arguments"),
        format!("exact:{identity}:intermediate"),
        format!("exact:{identity}:output"),
        overload.state_format.as_str(),
    )
}

fn validate_aggregate_declarations(
    name: &str,
    overloads: impl IntoIterator<Item = AggregateOverloadDeclaration>,
) -> Result<Box<[AggregateOverloadDeclaration]>, FunctionCatalogError> {
    let mut overloads = overloads.into_iter().collect::<Vec<_>>();
    if overloads.is_empty() {
        return Err(FunctionCatalogError::EmptySignatureSet { name: name.into() });
    }
    overloads.sort_unstable_by(|left, right| left.identity.cmp(&right.identity));
    for pair in overloads.windows(2) {
        if pair[0].identity == pair[1].identity {
            return Err(FunctionCatalogError::DuplicateOverloadIdentity {
                name: name.into(),
                identity: pair[0].identity.clone(),
            });
        }
    }
    for (index, overload) in overloads.iter().enumerate() {
        if overloads[..index]
            .iter()
            .any(|candidate| candidate.argument_pattern == overload.argument_pattern)
        {
            return Err(FunctionCatalogError::AmbiguousAggregatePattern {
                name: name.into(),
                argument_pattern: overload.argument_pattern.clone(),
            });
        }
    }
    Ok(overloads.into_boxed_slice())
}

fn validate_aggregate_overloads(
    name: &str,
    overloads: impl IntoIterator<Item = AggregateOverloadMetadata>,
) -> Result<Box<[AggregateOverloadMetadata]>, FunctionCatalogError> {
    let mut overloads = overloads.into_iter().collect::<Vec<_>>();
    if overloads.is_empty() {
        return Err(FunctionCatalogError::EmptySignatureSet { name: name.into() });
    }
    overloads.sort_unstable_by(|left, right| left.identity.cmp(&right.identity));
    for pair in overloads.windows(2) {
        if pair[0].identity == pair[1].identity {
            return Err(FunctionCatalogError::DuplicateOverloadIdentity {
                name: name.into(),
                identity: pair[0].identity.clone(),
            });
        }
    }
    for (index, overload) in overloads.iter().enumerate() {
        if overloads[..index]
            .iter()
            .any(|candidate| candidate.argument_types == overload.argument_types)
        {
            return Err(FunctionCatalogError::AmbiguousAggregateOverload {
                name: name.into(),
                argument_types: overload.argument_types.clone(),
            });
        }
    }
    Ok(overloads.into_boxed_slice())
}

fn select_exact_aggregate_overload<'a>(
    overloads: &'a [AggregateOverloadMetadata],
    argument_types: &[DataType],
) -> Result<&'a AggregateOverloadMetadata, FunctionResolutionError> {
    overloads
        .iter()
        .find(|overload| overload.argument_types.as_ref() == argument_types)
        .ok_or(FunctionResolutionError::NoMatchingSignature {
            candidates: overloads.len(),
            binding_enforced: true,
        })
}

#[derive(Default)]
pub struct EngineFunctionCatalogBuilder {
    definitions: BTreeMap<(Box<str>, FunctionKind), FunctionDefinition>,
    identities: BTreeSet<FunctionId>,
}

impl EngineFunctionCatalogBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, definition: FunctionDefinition) -> Result<(), FunctionCatalogError> {
        let key = (definition.canonical_name.clone(), definition.kind);
        if self.definitions.contains_key(&key) {
            return Err(FunctionCatalogError::DuplicateDefinition {
                name: definition.canonical_name.clone(),
                kind: definition.kind,
            });
        }
        if let Some(binding) = &definition.binding
            && self.identities.contains(binding.declaration.function_id())
        {
            return Err(FunctionCatalogError::DuplicateFunctionIdentity {
                identity: binding.declaration.function_id().clone(),
            });
        }
        if let Some(binding) = &definition.binding {
            self.identities
                .insert(binding.declaration.function_id().clone());
        }
        self.definitions.insert(key, definition);
        Ok(())
    }

    pub fn definition(&self, name: &str, kind: FunctionKind) -> Option<&FunctionDefinition> {
        let canonical_name = name.to_ascii_lowercase();
        self.definitions
            .get(&(canonical_name.into_boxed_str(), kind))
    }

    pub fn definitions(&self) -> impl Iterator<Item = &FunctionDefinition> {
        self.definitions.values()
    }

    pub fn seal(self) -> Result<EngineFunctionCatalog, FunctionCatalogError> {
        if self.definitions.is_empty() {
            return Err(FunctionCatalogError::EmptyCatalog);
        }
        let definitions = self.definitions.into_values().collect::<Vec<_>>();
        let digest = digest_definitions(&definitions);
        let identities = definitions
            .iter()
            .enumerate()
            .filter_map(|(index, definition)| {
                definition
                    .binding
                    .as_ref()
                    .map(|binding| (binding.declaration.function_id().clone(), index))
            })
            .collect();
        Ok(EngineFunctionCatalog {
            definitions: definitions.into_boxed_slice(),
            digest,
            identities,
        })
    }
}

pub trait FunctionBundleContributor {
    fn contribute(
        &self,
        builder: &mut EngineFunctionCatalogBuilder,
    ) -> Result<(), FunctionCatalogError>;
}

#[derive(Clone)]
pub struct EngineFunctionCatalog {
    definitions: Box<[FunctionDefinition]>,
    digest: [u8; 32],
    identities: BTreeMap<FunctionId, usize>,
}

impl fmt::Debug for EngineFunctionCatalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EngineFunctionCatalog")
            .field("definitions", &self.definitions)
            .field("digest", &self.digest)
            .finish()
    }
}

impl EngineFunctionCatalog {
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub fn definitions(&self) -> &[FunctionDefinition] {
        &self.definitions
    }

    pub fn definition(&self, name: &str, kind: FunctionKind) -> Option<&FunctionDefinition> {
        let canonical_name = name.to_ascii_lowercase();
        self.definitions
            .binary_search_by(|candidate| {
                (candidate.canonical_name(), candidate.kind()).cmp(&(canonical_name.as_str(), kind))
            })
            .ok()
            .map(|index| &self.definitions[index])
    }

    pub fn resolve_user(
        &self,
        name: &str,
        kind: FunctionKind,
        argument_types: &[DataType],
    ) -> Result<ResolvedFunctionSignature, FunctionResolutionError> {
        let definition = self
            .definition(name, kind)
            .ok_or(FunctionResolutionError::UnknownFunction)?;
        if definition.visibility == FunctionVisibility::Hidden {
            return Err(FunctionResolutionError::HiddenFunction);
        }
        legacy_function_resolver(definition)?.resolve(argument_types)
    }

    pub fn resolve_trusted(
        &self,
        name: &str,
        kind: FunctionKind,
        argument_types: &[DataType],
    ) -> Result<ResolvedFunctionSignature, FunctionResolutionError> {
        let definition = self
            .definition(name, kind)
            .ok_or(FunctionResolutionError::UnknownFunction)?;
        legacy_function_resolver(definition)?.resolve(argument_types)
    }

    pub fn resolve_aggregate_user(
        &self,
        name: &str,
        argument_types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        let definition = self
            .definition(name, FunctionKind::Aggregate)
            .ok_or(FunctionResolutionError::UnknownFunction)?;
        if definition.visibility == FunctionVisibility::Hidden {
            return Err(FunctionResolutionError::HiddenFunction);
        }
        resolve_exact_aggregate(definition, argument_types)
    }

    /// Resolve a logical SQL overload first, then materialize its exact
    /// executable update signature. Function ORDER BY channels belong only to
    /// `update_argument_types` and can never make an invalid logical call
    /// match a declared overload.
    pub fn resolve_aggregate_update_user(
        &self,
        name: &str,
        logical_argument_types: &[DataType],
        update_argument_types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        let definition = self
            .definition(name, FunctionKind::Aggregate)
            .ok_or(FunctionResolutionError::UnknownFunction)?;
        if definition.visibility == FunctionVisibility::Hidden {
            return Err(FunctionResolutionError::HiddenFunction);
        }
        resolve_aggregate_update(definition, logical_argument_types, update_argument_types)
    }

    pub fn resolve_aggregate_trusted(
        &self,
        name: &str,
        argument_types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        let definition = self
            .definition(name, FunctionKind::Aggregate)
            .ok_or(FunctionResolutionError::UnknownFunction)?;
        resolve_exact_aggregate(definition, argument_types)
    }

    pub fn resolve_aggregate_update_trusted(
        &self,
        name: &str,
        logical_argument_types: &[DataType],
        update_argument_types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        let definition = self
            .definition(name, FunctionKind::Aggregate)
            .ok_or(FunctionResolutionError::UnknownFunction)?;
        resolve_aggregate_update(definition, logical_argument_types, update_argument_types)
    }

    /// Re-materialize a previously selected execution signature without
    /// reinterpreting update channels as a logical SQL call. Used by Execution
    /// after FE/BE plan decoding already performed the two-stage validation.
    pub fn resolve_selected_aggregate_update_trusted(
        &self,
        name: &str,
        selected_overload: &AggregateOverloadIdentity,
        update_argument_types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        let definition = self
            .definition(name, FunctionKind::Aggregate)
            .ok_or(FunctionResolutionError::UnknownFunction)?;
        resolve_selected_aggregate_update(definition, selected_overload, update_argument_types)
    }
}

fn legacy_function_resolver(
    definition: &FunctionDefinition,
) -> Result<&dyn FunctionSignatureResolver, FunctionResolutionError> {
    definition.resolver.as_deref().ok_or_else(|| {
        FunctionResolutionError::BadSignature("function requires the exact binding API".into())
    })
}

fn resolve_exact_aggregate(
    definition: &FunctionDefinition,
    argument_types: &[DataType],
) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
    let resolver = definition.aggregate_resolver.as_ref().ok_or_else(|| {
        FunctionResolutionError::BadSignature(
            "aggregate has no safe typed signature contract".into(),
        )
    })?;
    let resolved = resolver.resolve_aggregate(argument_types)?;
    validate_resolved_aggregate(definition, resolved, argument_types)
}

fn resolve_aggregate_update(
    definition: &FunctionDefinition,
    logical_argument_types: &[DataType],
    update_argument_types: &[DataType],
) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
    if !update_argument_types.starts_with(logical_argument_types) {
        return Err(FunctionResolutionError::BadSignature(
            "aggregate update arguments must begin with the logical arguments".into(),
        ));
    }
    let resolver = definition.aggregate_resolver.as_ref().ok_or_else(|| {
        FunctionResolutionError::BadSignature(
            "aggregate has no safe typed signature contract".into(),
        )
    })?;
    if update_argument_types != logical_argument_types
        && !resolver.supports_ordered_update_channels()
    {
        return Err(FunctionResolutionError::BadSignature(format!(
            "aggregate `{}` does not support function ORDER BY update channels",
            definition.canonical_name()
        )));
    }
    let logical = resolve_exact_aggregate(definition, logical_argument_types)?;
    let update =
        resolve_selected_aggregate_update(definition, &logical.overload, update_argument_types)?;
    if update.output_type != logical.output_type || update.state_format != logical.state_format {
        return Err(FunctionResolutionError::BadSignature(format!(
            "aggregate overload `{}` update signature changed its logical output or state format",
            logical.overload.as_str()
        )));
    }
    Ok(update)
}

fn resolve_selected_aggregate_update(
    definition: &FunctionDefinition,
    selected_overload: &AggregateOverloadIdentity,
    update_argument_types: &[DataType],
) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
    let resolver = definition.aggregate_resolver.as_ref().ok_or_else(|| {
        FunctionResolutionError::BadSignature(
            "aggregate has no safe typed signature contract".into(),
        )
    })?;
    let resolved = resolver.resolve_update_signature(selected_overload, update_argument_types)?;
    if &resolved.overload != selected_overload {
        return Err(FunctionResolutionError::BadSignature(format!(
            "aggregate update resolver selected overload `{}` instead of `{}`",
            resolved.overload.as_str(),
            selected_overload.as_str()
        )));
    }
    validate_resolved_aggregate(definition, resolved, update_argument_types)
}

fn validate_resolved_aggregate(
    definition: &FunctionDefinition,
    resolved: ResolvedAggregateSignature,
    argument_types: &[DataType],
) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
    if resolved.argument_types != argument_types {
        return Err(FunctionResolutionError::BadSignature(
            "aggregate resolver returned argument types different from the bound input".into(),
        ));
    }
    let declaration = definition
        .aggregate_overloads
        .iter()
        .find(|declaration| declaration.identity == resolved.overload)
        .ok_or_else(|| {
            FunctionResolutionError::BadSignature(format!(
                "aggregate resolver returned undeclared overload `{}`",
                resolved.overload.as_str()
            ))
        })?;
    if declaration.state_format != resolved.state_format {
        return Err(FunctionResolutionError::BadSignature(format!(
            "aggregate overload `{}` returned undeclared state format `{}`",
            resolved.overload.as_str(),
            resolved.state_format.as_str()
        )));
    }
    Ok(resolved)
}

fn digest_definitions(definitions: &[FunctionDefinition]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(FUNCTION_CATALOG_DIGEST_DOMAIN);
    hasher.update(
        u32::try_from(definitions.len())
            .expect("function definition count fits u32")
            .to_be_bytes(),
    );
    for definition in definitions {
        let name = definition.canonical_name().as_bytes();
        hasher.update(
            u16::try_from(name.len())
                .expect("validated function name fits u16")
                .to_be_bytes(),
        );
        hasher.update(name);
        hasher.update([function_kind_tag(definition.kind)]);
        hasher.update([definition.visibility.tag()]);
        hasher.update([function_volatility_tag(definition.volatility)]);
        binding::digest_binding_definition(&mut hasher, definition.binding.as_ref());
        hasher.update(
            u32::try_from(definition.canonical_signatures.len())
                .expect("function signature count fits u32")
                .to_be_bytes(),
        );
        for signature in definition.canonical_signatures() {
            let signature = signature.as_bytes();
            hasher.update(
                u32::try_from(signature.len())
                    .expect("function signature length fits u32")
                    .to_be_bytes(),
            );
            hasher.update(signature);
        }
        hasher.update(
            u32::try_from(definition.aggregate_overloads.len())
                .expect("aggregate overload count fits u32")
                .to_be_bytes(),
        );
        for overload in definition.aggregate_overloads() {
            digest_text(&mut hasher, overload.identity.as_str());
            digest_text(&mut hasher, &overload.argument_pattern);
            digest_text(&mut hasher, &overload.intermediate_pattern);
            digest_text(&mut hasher, &overload.output_pattern);
            digest_text(&mut hasher, overload.state_format.as_str());
        }
        hasher.update(
            u32::try_from(definition.exact_aggregate_overloads.len())
                .expect("exact aggregate overload count fits u32")
                .to_be_bytes(),
        );
        for overload in &definition.exact_aggregate_overloads {
            digest_text(&mut hasher, overload.identity.as_str());
            hasher.update(
                u32::try_from(overload.argument_types.len())
                    .expect("aggregate argument count fits u32")
                    .to_be_bytes(),
            );
            for data_type in &overload.argument_types {
                digest_data_type(&mut hasher, data_type);
            }
            digest_data_type(&mut hasher, &overload.intermediate_type);
            digest_data_type(&mut hasher, &overload.output_type);
            digest_text(&mut hasher, overload.state_format.as_str());
        }
    }
    hasher.finalize().into()
}

fn digest_text(hasher: &mut Sha256, value: &str) {
    hasher.update(
        u32::try_from(value.len())
            .expect("validated metadata text length fits u32")
            .to_be_bytes(),
    );
    hasher.update(value.as_bytes());
}

fn digest_data_type(hasher: &mut Sha256, data_type: &DataType) {
    match data_type {
        DataType::Null => hasher.update([1]),
        DataType::Boolean => hasher.update([2]),
        DataType::Int8 => hasher.update([3]),
        DataType::Int16 => hasher.update([4]),
        DataType::Int32 => hasher.update([5]),
        DataType::Int64 => hasher.update([6]),
        DataType::UInt8 => hasher.update([7]),
        DataType::UInt16 => hasher.update([8]),
        DataType::UInt32 => hasher.update([9]),
        DataType::UInt64 => hasher.update([10]),
        DataType::Float16 => hasher.update([11]),
        DataType::Float32 => hasher.update([12]),
        DataType::Float64 => hasher.update([13]),
        DataType::Timestamp(unit, timezone) => {
            hasher.update([14, time_unit_tag(*unit)]);
            match timezone {
                Some(timezone) => {
                    hasher.update([1]);
                    digest_text(hasher, timezone);
                }
                None => hasher.update([0]),
            }
        }
        DataType::Date32 => hasher.update([15]),
        DataType::Date64 => hasher.update([16]),
        DataType::Time32(unit) => hasher.update([17, time_unit_tag(*unit)]),
        DataType::Time64(unit) => hasher.update([18, time_unit_tag(*unit)]),
        DataType::Duration(unit) => hasher.update([19, time_unit_tag(*unit)]),
        DataType::Interval(unit) => hasher.update([20, interval_unit_tag(*unit)]),
        DataType::Binary => hasher.update([21]),
        DataType::FixedSizeBinary(size) => {
            hasher.update([22]);
            hasher.update(size.to_be_bytes());
        }
        DataType::LargeBinary => hasher.update([23]),
        DataType::BinaryView => hasher.update([24]),
        DataType::Utf8 => hasher.update([25]),
        DataType::LargeUtf8 => hasher.update([26]),
        DataType::Utf8View => hasher.update([27]),
        DataType::List(field) => {
            hasher.update([28]);
            digest_field(hasher, field);
        }
        DataType::ListView(field) => {
            hasher.update([29]);
            digest_field(hasher, field);
        }
        DataType::FixedSizeList(field, size) => {
            hasher.update([30]);
            digest_field(hasher, field);
            hasher.update(size.to_be_bytes());
        }
        DataType::LargeList(field) => {
            hasher.update([31]);
            digest_field(hasher, field);
        }
        DataType::LargeListView(field) => {
            hasher.update([32]);
            digest_field(hasher, field);
        }
        DataType::Struct(fields) => {
            hasher.update([33]);
            hasher.update(
                u32::try_from(fields.len())
                    .expect("Arrow field count fits u32")
                    .to_be_bytes(),
            );
            for field in fields {
                digest_field(hasher, field);
            }
        }
        DataType::Union(fields, mode) => {
            hasher.update([34, union_mode_tag(*mode)]);
            hasher.update(
                u32::try_from(fields.len())
                    .expect("Arrow union field count fits u32")
                    .to_be_bytes(),
            );
            for (type_id, field) in fields.iter() {
                hasher.update(type_id.to_be_bytes());
                digest_field(hasher, field);
            }
        }
        DataType::Dictionary(key, value) => {
            hasher.update([35]);
            digest_data_type(hasher, key);
            digest_data_type(hasher, value);
        }
        DataType::Decimal32(precision, scale) => {
            hasher.update([36, *precision]);
            hasher.update(scale.to_be_bytes());
        }
        DataType::Decimal64(precision, scale) => {
            hasher.update([37, *precision]);
            hasher.update(scale.to_be_bytes());
        }
        DataType::Decimal128(precision, scale) => {
            hasher.update([38, *precision]);
            hasher.update(scale.to_be_bytes());
        }
        DataType::Decimal256(precision, scale) => {
            hasher.update([39, *precision]);
            hasher.update(scale.to_be_bytes());
        }
        DataType::Map(field, sorted) => {
            hasher.update([40, u8::from(*sorted)]);
            digest_field(hasher, field);
        }
        DataType::RunEndEncoded(run_ends, values) => {
            hasher.update([41]);
            digest_field(hasher, run_ends);
            digest_field(hasher, values);
        }
    }
}

fn digest_field(hasher: &mut Sha256, field: &Field) {
    digest_text(hasher, field.name());
    digest_data_type(hasher, field.data_type());
    hasher.update([u8::from(field.is_nullable())]);
    #[expect(deprecated)]
    match field.dict_id() {
        Some(dict_id) => {
            hasher.update([1]);
            hasher.update(dict_id.to_be_bytes());
        }
        None => hasher.update([0]),
    }
    match field.dict_is_ordered() {
        Some(ordered) => hasher.update([1, u8::from(ordered)]),
        None => hasher.update([0]),
    }
    let mut metadata = field.metadata().iter().collect::<Vec<_>>();
    metadata.sort_unstable_by(|left, right| left.0.cmp(right.0));
    hasher.update(
        u32::try_from(metadata.len())
            .expect("Arrow field metadata count fits u32")
            .to_be_bytes(),
    );
    for (key, value) in metadata {
        digest_text(hasher, key);
        digest_text(hasher, value);
    }
}

const fn time_unit_tag(unit: TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 2,
        TimeUnit::Microsecond => 3,
        TimeUnit::Nanosecond => 4,
    }
}

const fn interval_unit_tag(unit: IntervalUnit) -> u8 {
    match unit {
        IntervalUnit::YearMonth => 1,
        IntervalUnit::DayTime => 2,
        IntervalUnit::MonthDayNano => 3,
    }
}

const fn union_mode_tag(mode: UnionMode) -> u8 {
    match mode {
        UnionMode::Sparse => 1,
        UnionMode::Dense => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Int64Resolver;

    impl FunctionSignatureResolver for Int64Resolver {
        fn resolve(
            &self,
            argument_types: &[DataType],
        ) -> Result<ResolvedFunctionSignature, FunctionResolutionError> {
            if argument_types != [DataType::Int64] {
                return Err(FunctionResolutionError::NoMatchingSignature {
                    candidates: 1,
                    binding_enforced: true,
                });
            }
            Ok(ResolvedFunctionSignature {
                return_type: DataType::Int64,
                argument_types: argument_types.to_vec(),
                enforce_argument_binding: true,
            })
        }
    }

    fn definition(name: &str, visibility: FunctionVisibility) -> FunctionDefinition {
        FunctionDefinition::try_new(
            name,
            FunctionKind::Aggregate,
            visibility,
            FunctionVolatility::Immutable,
            ["(int64)->int64"],
            Arc::new(Int64Resolver),
        )
        .expect("definition")
    }

    fn aggregate_overload(
        identity: &str,
        argument_type: DataType,
        intermediate_type: DataType,
        output_type: DataType,
        state_format: &str,
    ) -> AggregateOverloadMetadata {
        AggregateOverloadMetadata::try_new(
            identity,
            [argument_type],
            intermediate_type,
            output_type,
            state_format,
        )
        .expect("overload")
    }

    fn exact_aggregate_definition(
        name: &str,
        visibility: FunctionVisibility,
        overloads: impl IntoIterator<Item = AggregateOverloadMetadata>,
    ) -> FunctionDefinition {
        FunctionDefinition::try_new_exact_aggregate(
            name,
            visibility,
            FunctionVolatility::Immutable,
            overloads,
        )
        .expect("exact aggregate definition")
    }

    #[test]
    fn hidden_functions_require_trusted_resolution() {
        let mut builder = EngineFunctionCatalogBuilder::new();
        builder
            .register(definition("$hidden", FunctionVisibility::Hidden))
            .unwrap();
        let catalog = builder.seal().unwrap();
        assert_eq!(
            catalog.resolve_user("$hidden", FunctionKind::Aggregate, &[DataType::Int64]),
            Err(FunctionResolutionError::HiddenFunction)
        );
        assert_eq!(
            catalog
                .resolve_trusted("$hidden", FunctionKind::Aggregate, &[DataType::Int64])
                .unwrap()
                .return_type,
            DataType::Int64
        );
    }

    #[test]
    fn duplicate_name_and_kind_fail_closed() {
        let mut builder = EngineFunctionCatalogBuilder::new();
        builder
            .register(definition("count", FunctionVisibility::Public))
            .unwrap();
        let error = builder
            .register(definition("count", FunctionVisibility::Public))
            .unwrap_err();
        assert!(matches!(
            error,
            FunctionCatalogError::DuplicateDefinition { .. }
        ));
    }

    #[test]
    fn digest_is_independent_of_contribution_order_and_covers_visibility() {
        let build = |reverse: bool, hidden: bool| {
            let mut builder = EngineFunctionCatalogBuilder::new();
            let mut definitions = vec![
                definition("alpha", FunctionVisibility::Public),
                definition(
                    "beta",
                    if hidden {
                        FunctionVisibility::Hidden
                    } else {
                        FunctionVisibility::Public
                    },
                ),
            ];
            if reverse {
                definitions.reverse();
            }
            for definition in definitions {
                builder.register(definition).unwrap();
            }
            builder.seal().unwrap().digest()
        };
        assert_eq!(build(false, false), build(true, false));
        assert_ne!(build(false, false), build(false, true));
    }

    #[test]
    fn definitions_require_canonical_names_and_unique_signatures() {
        assert!(matches!(
            FunctionDefinition::try_new(
                "NotCanonical",
                FunctionKind::Scalar,
                FunctionVisibility::Public,
                FunctionVolatility::Immutable,
                ["()->int64"],
                Arc::new(Int64Resolver),
            ),
            Err(FunctionCatalogError::InvalidCanonicalName { .. })
        ));
        assert!(matches!(
            FunctionDefinition::try_new(
                "duplicate",
                FunctionKind::Scalar,
                FunctionVisibility::Public,
                FunctionVolatility::Immutable,
                ["()->int64", "()->int64"],
                Arc::new(Int64Resolver),
            ),
            Err(FunctionCatalogError::DuplicateSignature { .. })
        ));
    }

    #[test]
    fn exact_aggregate_resolution_freezes_the_selected_overload() {
        let int64 = aggregate_overload(
            "sum/int64/v1",
            DataType::Int64,
            DataType::Int64,
            DataType::Int64,
            "sum-int64-state/v1",
        );
        let float64 = aggregate_overload(
            "sum/float64/v1",
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
            "sum-float64-state/v1",
        );
        let mut builder = EngineFunctionCatalogBuilder::new();
        builder
            .register(exact_aggregate_definition(
                "typed_sum",
                FunctionVisibility::Public,
                [float64, int64],
            ))
            .unwrap();
        let catalog = builder.seal().unwrap();

        let resolved = catalog
            .resolve_aggregate_user("TYPED_SUM", &[DataType::Int64])
            .unwrap();
        assert_eq!(resolved.overload.as_str(), "sum/int64/v1");
        assert_eq!(resolved.argument_types, [DataType::Int64]);
        assert_eq!(resolved.intermediate_type, DataType::Int64);
        assert_eq!(resolved.output_type, DataType::Int64);
        assert_eq!(resolved.state_format.as_str(), "sum-int64-state/v1");
        assert_eq!(
            catalog
                .resolve_selected_aggregate_update_trusted(
                    "typed_sum",
                    &resolved.overload,
                    &[DataType::Int64],
                )
                .unwrap(),
            resolved,
        );
        // FLOAT64 is a declared candidate, but cannot replace the chosen I64
        // overload at the update boundary.
        assert!(
            catalog
                .resolve_selected_aggregate_update_trusted(
                    "typed_sum",
                    &resolved.overload,
                    &[DataType::Float64],
                )
                .is_err()
        );
        let foreign = AggregateOverloadIdentity::try_new("foreign/int64/v1").unwrap();
        assert!(
            catalog
                .resolve_selected_aggregate_update_trusted(
                    "typed_sum",
                    &foreign,
                    &[DataType::Int64],
                )
                .is_err()
        );
        assert!(matches!(
            catalog.resolve_aggregate_user("typed_sum", &[DataType::UInt64]),
            Err(FunctionResolutionError::NoMatchingSignature {
                candidates: 2,
                binding_enforced: true
            })
        ));
    }

    struct ArrayAggResolver;

    impl AggregateSignatureResolver for ArrayAggResolver {
        fn resolve_aggregate(
            &self,
            argument_types: &[DataType],
        ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
            let [element_type] = argument_types else {
                return Err(FunctionResolutionError::NoMatchingSignature {
                    candidates: 1,
                    binding_enforced: true,
                });
            };
            let list_type =
                DataType::List(Arc::new(Field::new("item", element_type.clone(), true)));
            Ok(ResolvedAggregateSignature {
                overload: AggregateOverloadIdentity::try_new("array_agg/T/v1").unwrap(),
                argument_types: argument_types.to_vec(),
                intermediate_type: list_type.clone(),
                output_type: list_type,
                state_format: AggregateStateFormatIdentity::try_new("array-agg-state/v1").unwrap(),
            })
        }
    }

    #[test]
    fn parametric_overload_resolves_multiple_concrete_signatures() {
        let declaration = AggregateOverloadDeclaration::try_new(
            "array_agg/T/v1",
            "(T:any)",
            "list<T>",
            "list<T>",
            "array-agg-state/v1",
        )
        .unwrap();
        let definition = FunctionDefinition::try_new_parametric_aggregate(
            "typed_array_agg",
            FunctionVisibility::Public,
            FunctionVolatility::Immutable,
            [declaration],
            Arc::new(ArrayAggResolver),
        )
        .unwrap();
        let mut builder = EngineFunctionCatalogBuilder::new();
        builder.register(definition).unwrap();
        let catalog = builder.seal().unwrap();

        let int64 = catalog
            .resolve_aggregate_user("typed_array_agg", &[DataType::Int64])
            .unwrap();
        let utf8 = catalog
            .resolve_aggregate_user("typed_array_agg", &[DataType::Utf8])
            .unwrap();
        assert_eq!(int64.overload, utf8.overload);
        assert_ne!(int64.output_type, utf8.output_type);
        assert_eq!(int64.argument_types, [DataType::Int64]);
        assert_eq!(utf8.argument_types, [DataType::Utf8]);
    }

    #[test]
    fn exact_overload_family_rejects_duplicate_or_ambiguous_contracts_atomically() {
        let int64 = || {
            aggregate_overload(
                "sum/int64/v1",
                DataType::Int64,
                DataType::Int64,
                DataType::Int64,
                "sum-int64-state/v1",
            )
        };
        let duplicate_identity = FunctionDefinition::try_new_exact_aggregate(
            "typed_sum",
            FunctionVisibility::Public,
            FunctionVolatility::Immutable,
            [int64(), int64()],
        );
        assert!(matches!(
            duplicate_identity,
            Err(FunctionCatalogError::DuplicateOverloadIdentity { .. })
        ));

        let ambiguous = FunctionDefinition::try_new_exact_aggregate(
            "typed_sum",
            FunctionVisibility::Public,
            FunctionVolatility::Immutable,
            [
                int64(),
                aggregate_overload(
                    "sum/int64/new-state/v2",
                    DataType::Int64,
                    DataType::Binary,
                    DataType::Int64,
                    "sum-int64-state/v2",
                ),
            ],
        );
        assert!(matches!(
            ambiguous,
            Err(FunctionCatalogError::AmbiguousAggregateOverload { .. })
        ));
    }

    #[test]
    fn aggregate_digest_covers_intermediate_and_state_format_and_is_order_stable() {
        let digest = |reverse: bool, intermediate: DataType, state_format: &str| {
            let mut overloads = vec![
                aggregate_overload(
                    "sketch/binary/v1",
                    DataType::Binary,
                    intermediate,
                    DataType::Binary,
                    state_format,
                ),
                aggregate_overload(
                    "sketch/int64/v1",
                    DataType::Int64,
                    DataType::Binary,
                    DataType::Binary,
                    "theta-compact/v1",
                ),
            ];
            if reverse {
                overloads.reverse();
            }
            let mut builder = EngineFunctionCatalogBuilder::new();
            builder
                .register(exact_aggregate_definition(
                    "$typed_sketch",
                    FunctionVisibility::Hidden,
                    overloads,
                ))
                .unwrap();
            builder.seal().unwrap().digest()
        };

        let baseline = digest(false, DataType::Binary, "theta-compact/v1");
        assert_eq!(baseline, digest(true, DataType::Binary, "theta-compact/v1"));
        assert_ne!(
            baseline,
            digest(false, DataType::LargeBinary, "theta-compact/v1")
        );
        assert_ne!(
            baseline,
            digest(false, DataType::Binary, "theta-compact/v2")
        );
    }

    #[test]
    fn hidden_exact_aggregate_requires_trusted_resolution() {
        let mut builder = EngineFunctionCatalogBuilder::new();
        builder
            .register(exact_aggregate_definition(
                "$typed_sketch",
                FunctionVisibility::Hidden,
                [aggregate_overload(
                    "sketch/int64/v1",
                    DataType::Int64,
                    DataType::Binary,
                    DataType::Binary,
                    "theta-compact/v1",
                )],
            ))
            .unwrap();
        let catalog = builder.seal().unwrap();

        assert_eq!(
            catalog.resolve_aggregate_user("$typed_sketch", &[DataType::Int64]),
            Err(FunctionResolutionError::HiddenFunction)
        );
        assert_eq!(
            catalog
                .resolve_aggregate_trusted("$typed_sketch", &[DataType::Int64])
                .unwrap()
                .overload
                .as_str(),
            "sketch/int64/v1"
        );
    }

    #[test]
    fn legacy_aggregate_never_silently_becomes_a_typed_contract() {
        let mut builder = EngineFunctionCatalogBuilder::new();
        builder
            .register(definition("legacy", FunctionVisibility::Public))
            .unwrap();
        let catalog = builder.seal().unwrap();
        assert_eq!(
            catalog.resolve_aggregate_user("legacy", &[DataType::Int64]),
            Err(FunctionResolutionError::BadSignature(
                "aggregate has no safe typed signature contract".into()
            ))
        );
    }

    struct SumKernel;

    impl TypedAggregateKernel for SumKernel {
        type State = Vec<i64>;
        type PreparedUpdateBatch<'batch> = &'batch arrow_array::Int64Array;
        type PreparedMergeBatch<'batch> = &'batch arrow_array::Int64Array;
        type Error = String;

        fn memory_policy(&self) -> AggregateStateMemoryPolicy {
            AggregateStateMemoryPolicy::BoundedRetained {
                max_retained_bytes_per_state: 4096,
            }
        }

        fn create_state(&self) -> Result<Self::State, Self::Error> {
            Ok(Vec::new())
        }

        fn prepare_update<'batch>(
            &self,
            input: &'batch AggregateInputBatch<'batch>,
        ) -> Result<Self::PreparedUpdateBatch<'batch>, Self::Error> {
            input
                .values()
                .and_then(|values| values.as_any().downcast_ref())
                .ok_or_else(|| "expected Int64 aggregate input".into())
        }

        fn update_row<'batch>(
            &self,
            state: &mut Self::State,
            prepared: &Self::PreparedUpdateBatch<'batch>,
            row: usize,
        ) -> Result<(), Self::Error> {
            state.push(prepared.value(row));
            Ok(())
        }

        fn prepare_merge<'batch>(
            &self,
            input: &'batch AggregateInputBatch<'batch>,
        ) -> Result<Self::PreparedMergeBatch<'batch>, Self::Error> {
            self.prepare_update(input)
        }

        fn merge_row<'batch>(
            &self,
            state: &mut Self::State,
            prepared: &Self::PreparedMergeBatch<'batch>,
            row: usize,
        ) -> Result<(), Self::Error> {
            self.update_row(state, prepared, row)
        }

        fn build_intermediate<'state, I>(&self, states: I) -> Result<ArrayRef, Self::Error>
        where
            Self::State: 'state,
            I: ExactSizeIterator<Item = &'state Self::State>,
        {
            Ok(Arc::new(arrow_array::Int64Array::from_iter_values(
                states.map(|state| state.iter().sum::<i64>()),
            )))
        }

        fn build_final<'state, I>(&self, states: I) -> Result<ArrayRef, Self::Error>
        where
            Self::State: 'state,
            I: ExactSizeIterator<Item = &'state Self::State>,
        {
            self.build_intermediate(states)
        }

        fn retained_bytes(&self, state: &Self::State) -> usize {
            state.capacity() * std::mem::size_of::<i64>()
        }
    }

    struct SumFamily {
        overloads: Box<[AggregateOverloadDeclaration]>,
    }

    impl TypedAggregateFamily for SumFamily {
        type Kernel = SumKernel;
        type PrepareError = String;

        fn overloads(&self) -> &[AggregateOverloadDeclaration] {
            &self.overloads
        }

        fn resolve_signature(
            &self,
            argument_types: &[DataType],
        ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
            if argument_types != [DataType::Int64] {
                return Err(FunctionResolutionError::NoMatchingSignature {
                    candidates: 1,
                    binding_enforced: true,
                });
            }
            Ok(ResolvedAggregateSignature {
                overload: AggregateOverloadIdentity::try_new("sum/T/v1").unwrap(),
                argument_types: argument_types.to_vec(),
                intermediate_type: DataType::Int64,
                output_type: DataType::Int64,
                state_format: AggregateStateFormatIdentity::try_new("sum-state/v1").unwrap(),
            })
        }

        fn prepare(
            &self,
            _selected: &ResolvedAggregateSignature,
            _options: &AggregateBindOptions,
        ) -> Result<Self::Kernel, Self::PrepareError> {
            Ok(SumKernel)
        }
    }

    #[test]
    fn typed_registration_keeps_state_and_borrowed_batches_safe() {
        let family = SumFamily {
            overloads: vec![
                AggregateOverloadDeclaration::try_new(
                    "sum/T/v1",
                    "(T:numeric)",
                    "T",
                    "T",
                    "sum-state/v1",
                )
                .unwrap(),
            ]
            .into_boxed_slice(),
        };
        let registration = TypedAggregateRegistration::try_new(
            "typed_sum",
            FunctionVisibility::Public,
            FunctionVolatility::Immutable,
            AggregateImplementationIdentity::try_new("typed-sum/exec-v1").unwrap(),
            family,
        )
        .unwrap();
        let (definition, implementation, family) = registration.into_parts();
        assert_eq!(implementation.as_str(), "typed-sum/exec-v1");
        let mut builder = EngineFunctionCatalogBuilder::new();
        builder.register(definition).unwrap();
        let catalog = builder.seal().unwrap();
        let selected = catalog
            .resolve_aggregate_user("typed_sum", &[DataType::Int64])
            .unwrap();
        assert!(
            matches!(
                family.resolve_update_signature(&selected.overload, &[DataType::Int64]),
                Err(FunctionResolutionError::BadSignature(_)),
            ),
            "a working typed election author grants no default fixed capability"
        );
        let kernel = family
            .prepare(&selected, &AggregateBindOptions::default())
            .unwrap();
        let mut state = kernel.create_state().unwrap();
        let input: ArrayRef = Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3, 4]));
        let input = AggregateInputBatch::try_new(Some(&input), 4).unwrap();
        let prepared = kernel.prepare_update(&input).unwrap();
        for row in 0..input.row_count() {
            kernel.update_row(&mut state, &prepared, row).unwrap();
        }
        let output = kernel.build_final(std::iter::once(&state)).unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap()
                .value(0),
            10
        );
        assert!(kernel.retained_bytes(&state) >= 4 * std::mem::size_of::<i64>());
    }

    #[test]
    fn aggregate_bind_options_validate_order_shape_and_byte_limit() {
        assert!(matches!(
            AggregateBindOptions::try_new(false, &[true], &[], None),
            Err(AggregateBindOptionsError::OrderKeyLengthMismatch { .. })
        ));
        assert!(matches!(
            AggregateBindOptions::try_new(false, &[], &[], Some(-1)),
            Err(AggregateBindOptionsError::InvalidMaxOutputBytes(-1))
        ));
        let options =
            AggregateBindOptions::try_new(true, &[true, false], &[false, true], Some(4096))
                .unwrap();
        assert!(options.distinct());
        assert_eq!(
            options.order_keys(),
            [
                AggregateOrderKey {
                    ascending: true,
                    nulls_first: false,
                },
                AggregateOrderKey {
                    ascending: false,
                    nulls_first: true,
                },
            ]
        );
        assert_eq!(options.max_output_bytes(), Some(4096));
    }
}

pub mod date_float_cast;

pub mod append_trailing_core;
pub mod float_date_cast;
pub mod string_reverse_shared;

pub mod legacy_arithmetic;
pub mod legacy_decimal;
pub mod legacy_literal;
mod native_negate;
pub use native_negate::*;

pub mod binary_text;

#[cfg(test)]
mod native_negate_original_decimal_carrier_baseline_tests;

pub mod decimal_float_cast;
pub mod float_decimal128;

pub mod time_text_cast;
#[cfg(test)]
mod time_text_cast_tests;

pub mod time_calendar_cast;

pub mod field_shared;

mod native_bitnot;
pub use native_bitnot::*;

/// Original Decimal128 CAST computation shared by legacy and selected shells.
pub mod decimal128_rescale;

pub mod list_cast_core;
mod list_cast_selected;

mod native_between;
pub use native_between::*;

/// Original signed integral Decimal128 conversion and policy projection.
pub mod integral_decimal128;

pub mod native_inlist;
pub use native_inlist::PreparedNativeInListRecipe;

pub mod hll_hash_core;

pub mod native_like;
pub use native_like::PreparedNativeLikeRecipe;

pub mod bitmap_aggregate_core;

mod bitmap_decode_resources;

pub mod percentile_approx_raw_core;

pub mod approx_percentile_failure;

mod arrow_result_custody;
mod scalar_output_operation;
mod scalar_output_resources;
pub mod window_format;
mod window_invocation_data;
mod window_output_scalars;
pub use window_invocation_data::{
    WindowEvaluationFailure, WindowInvocationContext, WindowInvocationData, WindowInvocationPhase,
};

mod window_result_carrier;
pub use window_result_carrier::WindowResultCarrier;

mod window_invocation_input;
pub use window_invocation_input::{
    FullWindowInvocationInput, WindowFrameOrigin, WindowInvocationInput,
};
mod window_evaluation_invocation;
pub use window_evaluation_invocation::WindowEvaluationInvocation;
mod window_invocation_scratch;
pub use window_invocation_scratch::WindowInvocationScratch;
pub mod window_input_order;
pub use window_invocation_data::WindowInvocationScope;

mod array_backing_geometry;
pub use arrow_result_custody::{SourceBackingOwner, retain_source_backing};

pub mod string_repeat_pad_core;

mod scalar_invocation;
pub use scalar_invocation::*;

pub mod string_left_right_core;

pub mod string_split_part_core;

pub mod approx_top_k_core;

#[cfg(test)]
mod common_type_m07_contract_tests;
