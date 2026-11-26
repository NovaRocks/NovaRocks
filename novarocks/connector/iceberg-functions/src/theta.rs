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

use std::fmt;
use std::mem::size_of;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BinaryArray};
use arrow_schema::DataType;
use datasketches::hash::value::raw_bytes;
use datasketches::theta::{
    CompactThetaSketch, ThetaSketch, ThetaSketchBuilder, ThetaUnion, ThetaUnionBuilder,
};
use novarocks_functions::{
    AggregateBindOptions, AggregateImplementationIdentity, AggregateInputBatch,
    AggregateOverloadDeclaration, AggregateOverloadIdentity, AggregateStateFormatIdentity,
    AggregateStateMemoryPolicy, AggregateUpdateSignatureRecipe, EngineFunctionCatalogBuilder,
    FunctionBundleContributor, FunctionCatalogError, FunctionResolutionError, FunctionValueType,
    FunctionVisibility, FunctionVolatility, ResolvedAggregateSignature, TypedAggregateFamily,
    TypedAggregateKernel, TypedAggregateRegistration, ValueLogicalType,
};

use crate::canonical::{CanonicalKind, PreparedCanonicalBatch, canonical_width};

#[path = "theta_pure.rs"]
mod pure;
pub use pure::{
    PreparedThetaKernel, THETA_EMISSION_LIBRARY_REQUEST_BYTES, ThetaOutputResourceFacts,
    iceberg_theta_pure_definition, theta_output_resource_facts,
};

pub const ICEBERG_THETA_AGGREGATE_NAME: &str = "$iceberg_theta_stat";
pub const ICEBERG_THETA_STATE_FORMAT_IDENTITY: &str =
    "iceberg/apache-datasketches-theta-v1/default-seed/ordered-compact";
pub const ICEBERG_THETA_IMPLEMENTATION_IDENTITY: &str = "iceberg/theta-quickselect/default-seed/v1";

const DEFAULT_LG_K: u8 = 12;
const _: () = assert!(
    size_of::<usize>() == 8,
    "Iceberg Theta bounds require a 64-bit target"
);
const MAX_TABLE_CAPACITY: usize = 1 << (DEFAULT_LG_K + 1);
// The pinned release exposes footprint, not its private table slot. The 64-bit Theta slot is
// one `u64`; the compile-time architecture gate above keeps this bound explicit.
const TABLE_ENTRY_BYTES: usize = size_of::<u64>();
const MAX_TABLE_RETAINED_BYTES: usize = MAX_TABLE_CAPACITY * TABLE_ENTRY_BYTES;
// X8 growth starts lg_k=12 at lg_size=7.
const INITIAL_TABLE_RETAINED_BYTES: usize = (1 << 7) * TABLE_ENTRY_BYTES;

// A rebuild temporarily owns both the retained-entry vector and the replacement
// hash table. A first merge may additionally coexist with the empty update
// table while the state changes mode. Execution reserves this operation bound
// before each mutation, then reconciles to `retained_bytes`.
pub const ICEBERG_THETA_OPERATION_MAX_RETAINED_BYTES: usize =
    (2 * MAX_TABLE_RETAINED_BYTES) + INITIAL_TABLE_RETAINED_BYTES;
pub const ICEBERG_THETA_MAX_COMPACT_BYTES: usize = 24 + (MAX_TABLE_CAPACITY * size_of::<u64>());
const _: () = assert!(ICEBERG_THETA_OPERATION_MAX_RETAINED_BYTES < 512 * 1024);

const EMPTY_ORDERED_COMPACT_V3: [u8; 8] = [1, 3, 3, 0, 0, 0x1e, 0, 0];

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IcebergThetaError {
    UnsupportedInputType(novarocks_failure_contract::BoundedDetail),
    ArrayTypeMismatch(novarocks_failure_contract::BoundedDetail),
    MissingInput,
    InvalidBindOptions(&'static str),
    InvalidResolvedSignature,
    InvalidCompact(String),
    CompactTooLarge { actual: usize, maximum: usize },
    MixedUpdateAndMerge,
    Sketch(String),
    OutputResourceExhausted,
    InvalidOutputExtent,
}

impl fmt::Display for IcebergThetaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedInputType(data_type) => {
                write!(
                    formatter,
                    "Iceberg Theta does not support input type {data_type}"
                )
            }
            Self::ArrayTypeMismatch(data_type) => {
                write!(
                    formatter,
                    "Iceberg Theta input array does not match {data_type}"
                )
            }
            Self::MissingInput => formatter.write_str("Iceberg Theta requires one input array"),
            Self::InvalidBindOptions(message) => formatter.write_str(message),
            Self::InvalidResolvedSignature => {
                formatter.write_str("Iceberg Theta received an invalid resolved signature")
            }
            Self::InvalidCompact(message) => {
                write!(formatter, "invalid Iceberg Theta compact body: {message}")
            }
            Self::CompactTooLarge { actual, maximum } => write!(
                formatter,
                "Iceberg Theta compact body has {actual} bytes, maximum is {maximum}"
            ),
            Self::MixedUpdateAndMerge => formatter.write_str(
                "Iceberg Theta state cannot mix raw updates and compact merges in one phase",
            ),
            Self::Sketch(message) => formatter.write_str(message),
            Self::OutputResourceExhausted => {
                formatter.write_str("Iceberg Theta output resources were exhausted")
            }
            Self::InvalidOutputExtent => {
                formatter.write_str("Iceberg Theta output iterator or Binary extent is invalid")
            }
        }
    }
}

impl std::error::Error for IcebergThetaError {}

pub(crate) fn theta_type_description(
    value: &DataType,
) -> novarocks_failure_contract::BoundedDetail {
    novarocks_failure_contract::BoundedDetail::truncating_display(format_args!("{value:?}"))
}
impl novarocks_failure_contract::FailureProjection for IcebergThetaError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        // These are the provider's existing outer operational/refusal channels.
        // No per-row data is promoted through this internal statistics aggregate.
        let (class, code) = match self {
            Self::UnsupportedInputType(_) => (FailureClass::Invariant, 200),
            Self::ArrayTypeMismatch(_) => (FailureClass::Invariant, 201),
            Self::MissingInput => (FailureClass::Invariant, 202),
            Self::InvalidBindOptions(_) => (FailureClass::Invariant, 203),
            Self::InvalidResolvedSignature => (FailureClass::Invariant, 204),
            Self::InvalidCompact(_) => (FailureClass::Unexpected, 205),
            Self::CompactTooLarge { .. } => (FailureClass::Unexpected, 206),
            Self::MixedUpdateAndMerge => (FailureClass::Unexpected, 207),
            Self::Sketch(_) => (FailureClass::Unexpected, 208),
            Self::OutputResourceExhausted => (FailureClass::Resource, 209),
            Self::InvalidOutputExtent => (FailureClass::Invariant, 210),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::CONNECTOR, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for IcebergThetaError {}

pub struct IcebergThetaAggregateFamily {
    overloads: Box<[AggregateOverloadDeclaration]>,
}

impl IcebergThetaAggregateFamily {
    pub fn try_new() -> Result<Self, FunctionCatalogError> {
        let kinds = [
            CanonicalKind::Boolean,
            CanonicalKind::TinyInt,
            CanonicalKind::SmallInt,
            CanonicalKind::Int,
            CanonicalKind::Long,
            CanonicalKind::Float,
            CanonicalKind::Double,
            CanonicalKind::Decimal,
            CanonicalKind::Date,
            CanonicalKind::TimeMicros,
            CanonicalKind::TimestampMicros,
            CanonicalKind::TimestampNanos,
            CanonicalKind::String,
            CanonicalKind::LargeString,
            CanonicalKind::Binary,
            CanonicalKind::LargeBinary,
            CanonicalKind::Fixed,
        ];
        let overloads = kinds
            .into_iter()
            .map(|kind| {
                AggregateOverloadDeclaration::try_new(
                    overload_identity(kind),
                    kind.pattern(),
                    "binary",
                    "binary",
                    ICEBERG_THETA_STATE_FORMAT_IDENTITY,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            overloads: overloads.into_boxed_slice(),
        })
    }

    fn canonical_argument<'a>(
        &self,
        argument_types: &'a [DataType],
    ) -> Result<(&'a DataType, CanonicalKind), FunctionResolutionError> {
        let [argument_type] = argument_types else {
            return Err(FunctionResolutionError::NoMatchingSignature {
                candidates: self.overloads.len(),
                binding_enforced: true,
            });
        };
        let kind = CanonicalKind::from_data_type(argument_type).map_err(|_| {
            FunctionResolutionError::NoMatchingSignature {
                candidates: self.overloads.len(),
                binding_enforced: true,
            }
        })?;
        Ok((argument_type, kind))
    }

    fn signature_recipe<'a>(
        &'a self,
        argument_types: &'a [DataType],
        selected_overload: Option<&str>,
    ) -> Result<AggregateUpdateSignatureRecipe<'a>, FunctionResolutionError> {
        let (_, kind) = self.canonical_argument(argument_types)?;
        let original = self
            .overloads
            .iter()
            .find(|declaration| overload_identity_matches(kind, declaration.identity.as_str()))
            .ok_or_else(|| {
                FunctionResolutionError::BadSignature(
                    "Iceberg Theta canonical kind lacks its original overload declaration".into(),
                )
            })?;
        if selected_overload.is_some_and(|selected| selected != original.identity.as_str()) {
            return Err(FunctionResolutionError::BadSignature(
                "Iceberg Theta update carrier differs from the selected overload".into(),
            ));
        }
        Ok(AggregateUpdateSignatureRecipe {
            original,
            argument_types,
            intermediate_type: &DataType::Binary,
            output_type: &DataType::Binary,
        })
    }

    fn materialize_signature(
        &self,
        recipe: AggregateUpdateSignatureRecipe<'_>,
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        recipe.materialize_pure()
    }
}

impl IcebergThetaAggregateFamily {
    pub(crate) fn prepare_borrowed(
        &self,
        selected_overload: &str,
        argument_types: &[DataType],
        intermediate_type: &DataType,
        output_type: &DataType,
        state_format: &AggregateStateFormatIdentity,
        options: &AggregateBindOptions,
    ) -> Result<IcebergThetaKernel, IcebergThetaError> {
        let expected = self
            .signature_recipe(argument_types, Some(selected_overload))
            .map_err(|_| IcebergThetaError::InvalidResolvedSignature)?;
        if expected.original.identity.as_str() != selected_overload
            || expected.argument_types != argument_types
            || expected.intermediate_type != intermediate_type
            || expected.output_type != output_type
            || &expected.original.state_format != state_format
        {
            return Err(IcebergThetaError::InvalidResolvedSignature);
        }
        if options.distinct() {
            return Err(IcebergThetaError::InvalidBindOptions(
                "Iceberg Theta does not accept DISTINCT",
            ));
        }
        if !options.order_keys().is_empty() {
            return Err(IcebergThetaError::InvalidBindOptions(
                "Iceberg Theta does not accept function-internal ORDER BY",
            ));
        }
        if options
            .max_output_bytes()
            .is_some_and(|maximum| maximum < ICEBERG_THETA_MAX_COMPACT_BYTES)
        {
            return Err(IcebergThetaError::InvalidBindOptions(
                "Iceberg Theta output limit is below its declared compact bound",
            ));
        }
        Ok(IcebergThetaKernel)
    }
}

impl TypedAggregateFamily for IcebergThetaAggregateFamily {
    fn borrow_update_signature<'a>(
        &'a self,
        selected_overload: &str,
        arguments: &'a [FunctionValueType],
    ) -> Result<Option<AggregateUpdateSignatureRecipe<'a>>, FunctionResolutionError> {
        let [argument] = arguments else {
            return Err(FunctionResolutionError::NoMatchingSignature {
                candidates: self.overloads.len(),
                binding_enforced: true,
            });
        };
        // Borrow the original one-channel carrier without copying any Arrow
        // tree before the sole family matcher has accepted it.
        self.signature_recipe(
            std::slice::from_ref(argument.data_type()),
            Some(selected_overload),
        )
        .map(Some)
    }

    type Kernel = IcebergThetaKernel;
    type PrepareError = IcebergThetaError;

    fn overloads(&self) -> &[AggregateOverloadDeclaration] {
        &self.overloads
    }

    fn state_argument_contract(
        &self,
        selected_overload: &AggregateOverloadIdentity,
    ) -> Result<novarocks_functions::AggregateStateArgumentContract, FunctionResolutionError> {
        if !self
            .overloads
            .iter()
            .any(|overload| overload.identity == *selected_overload)
        {
            return Err(FunctionResolutionError::BadSignature(
                "Iceberg Theta state contract references a foreign overload".into(),
            ));
        }
        // Canonical update and compact merge skip physical NULL roots. Only
        // logical Value root nullability is independent; all other metadata
        // and the original state format remain exact.
        Ok(novarocks_functions::AggregateStateArgumentContract::ValueRootNullabilityIndependent)
    }

    /// A Theta sketch always has an answer: a group with no rows produces the
    /// canonical empty sketch, not the absence of one.
    fn produces_null(&self) -> bool {
        false
    }

    fn validate_value_arguments(
        &self,
        arguments: &[FunctionValueType],
    ) -> Result<(), FunctionResolutionError> {
        let [argument] = arguments else {
            return Err(FunctionResolutionError::NoMatchingSignature {
                candidates: self.overloads.len(),
                binding_enforced: true,
            });
        };
        argument
            .validate()
            .map_err(|error| FunctionResolutionError::BadSignature(error.to_string()))?;
        match argument.logical_type() {
            ValueLogicalType::Physical => {}
            // Iceberg UUID uses its exact raw 16 bytes. This grants no
            // numeric meaning to the fixed-binary carrier. LARGEINT statistics
            // retain the existing storage-byte policy as well.
            ValueLogicalType::Uuid | ValueLogicalType::LargeInt
                if argument.data_type() == &DataType::FixedSizeBinary(16) => {}
            // These source-declared opaque domains retain the original raw
            // serialized-byte statistics policy, without interpreting them.
            ValueLogicalType::Hll | ValueLogicalType::Bitmap
                if argument.data_type() == &DataType::Binary => {}
            // Existing Iceberg VARIANT statistics hash the serialized raw
            // bytes. This preserves that source-authored domain without
            // parsing it or deriving identity from a LargeBinary carrier.
            ValueLogicalType::Variant if argument.data_type() == &DataType::LargeBinary => {}
            _ => {
                return Err(FunctionResolutionError::BadSignature(format!(
                    "Iceberg Theta does not support input value domain {argument:?}"
                )));
            }
        }
        // The original canonical family remains the sole carrier admission
        // rule, including fixed width, decimal shape, and temporal units.
        self.canonical_argument(std::slice::from_ref(argument.data_type()))
            .map(|_| ())
    }

    fn resolve_signature(
        &self,
        argument_types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        let recipe = self.signature_recipe(argument_types, None)?;
        self.materialize_signature(recipe)
    }

    fn resolve_update_signature(
        &self,
        selected_overload: &AggregateOverloadIdentity,
        argument_types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        let recipe = self.signature_recipe(argument_types, Some(selected_overload.as_str()))?;
        self.materialize_signature(recipe)
    }

    fn prepare(
        &self,
        selected: &ResolvedAggregateSignature,
        options: &AggregateBindOptions,
    ) -> Result<Self::Kernel, Self::PrepareError> {
        self.prepare_borrowed(
            selected.overload.as_str(),
            &selected.argument_types,
            &selected.intermediate_type,
            &selected.output_type,
            &selected.state_format,
            options,
        )
    }
}

pub struct IcebergThetaKernel;

pub struct IcebergThetaState {
    mode: ThetaStateMode,
    has_updates: bool,
}

enum ThetaStateMode {
    Update(ThetaSketch),
    Merge(ThetaUnion),
}

impl TypedAggregateKernel for IcebergThetaKernel {
    type State = IcebergThetaState;
    type PreparedUpdateBatch<'batch> = PreparedCanonicalBatch<'batch>;
    type PreparedMergeBatch<'batch> = &'batch BinaryArray;
    type Error = IcebergThetaError;

    fn memory_policy(&self) -> AggregateStateMemoryPolicy {
        AggregateStateMemoryPolicy::BoundedRetained {
            max_retained_bytes_per_state: ICEBERG_THETA_OPERATION_MAX_RETAINED_BYTES,
        }
    }

    fn create_state(&self) -> Result<Self::State, Self::Error> {
        Ok(IcebergThetaState {
            mode: ThetaStateMode::Update(new_update_sketch()?),
            has_updates: false,
        })
    }

    fn prepare_update<'batch>(
        &self,
        input: &'batch AggregateInputBatch<'batch>,
    ) -> Result<Self::PreparedUpdateBatch<'batch>, Self::Error> {
        PreparedCanonicalBatch::try_new(input.values().ok_or(IcebergThetaError::MissingInput)?)
    }

    fn update_row<'batch>(
        &self,
        state: &mut Self::State,
        prepared: &Self::PreparedUpdateBatch<'batch>,
        row: usize,
    ) -> Result<(), Self::Error> {
        let Some(canonical) = prepared.canonical_bytes(row) else {
            return Ok(());
        };
        let width = canonical_width(prepared);
        let bytes = canonical.as_slice(width);
        let sketch = match &mut state.mode {
            ThetaStateMode::Update(sketch) => sketch,
            ThetaStateMode::Merge(_) => return Err(IcebergThetaError::MixedUpdateAndMerge),
        };
        // Java DataSketches ignores zero-length byte inputs. Preserve that
        // behavior for empty Iceberg strings/binary/fixed values.
        if !bytes.is_empty() {
            sketch.update(raw_bytes::from_slice(bytes));
        }
        state.has_updates = true;
        Ok(())
    }

    fn prepare_merge<'batch>(
        &self,
        input: &'batch AggregateInputBatch<'batch>,
    ) -> Result<Self::PreparedMergeBatch<'batch>, Self::Error> {
        input
            .values()
            .ok_or(IcebergThetaError::MissingInput)?
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                IcebergThetaError::ArrayTypeMismatch(theta_type_description(
                    input.values().expect("checked above").data_type(),
                ))
            })
    }

    fn merge_row<'batch>(
        &self,
        state: &mut Self::State,
        prepared: &Self::PreparedMergeBatch<'batch>,
        row: usize,
    ) -> Result<(), Self::Error> {
        if prepared.is_null(row) {
            return Ok(());
        }
        let compact = decode_canonical_compact(prepared.value(row))?;
        match &mut state.mode {
            ThetaStateMode::Update(_) if state.has_updates => {
                Err(IcebergThetaError::MixedUpdateAndMerge)
            }
            ThetaStateMode::Merge(union) => union.update(&compact).map_err(|error| {
                IcebergThetaError::Sketch(format!("union Theta partial: {error}"))
            }),
            ThetaStateMode::Update(_) => {
                let mut union = new_union()?;
                union.update(&compact).map_err(|error| {
                    IcebergThetaError::Sketch(format!("union first Theta partial: {error}"))
                })?;
                state.mode = ThetaStateMode::Merge(union);
                Ok(())
            }
        }
    }

    fn build_intermediate<'state, I>(&self, states: I) -> Result<ArrayRef, Self::Error>
    where
        Self::State: 'state,
        I: ExactSizeIterator<Item = &'state Self::State>,
    {
        build_output(states)
    }

    fn build_final<'state, I>(&self, states: I) -> Result<ArrayRef, Self::Error>
    where
        Self::State: 'state,
        I: ExactSizeIterator<Item = &'state Self::State>,
    {
        build_output(states)
    }

    fn retained_bytes(&self, state: &Self::State) -> usize {
        match &state.mode {
            ThetaStateMode::Update(sketch) => sketch.estimated_size() - size_of::<ThetaSketch>(),
            ThetaStateMode::Merge(union) => union.estimated_size() - size_of::<ThetaUnion>(),
        }
    }
}

pub struct IcebergFunctionBundle;

impl FunctionBundleContributor for IcebergFunctionBundle {
    fn contribute(
        &self,
        builder: &mut EngineFunctionCatalogBuilder,
    ) -> Result<(), FunctionCatalogError> {
        let registration = iceberg_theta_registration()?;
        builder.register(registration.definition().clone())
    }
}

pub fn iceberg_theta_registration()
-> Result<TypedAggregateRegistration<IcebergThetaAggregateFamily>, FunctionCatalogError> {
    pure::attach_registration(metadata_registration()?)
}

fn metadata_registration()
-> Result<TypedAggregateRegistration<IcebergThetaAggregateFamily>, FunctionCatalogError> {
    TypedAggregateRegistration::try_new(
        ICEBERG_THETA_AGGREGATE_NAME,
        FunctionVisibility::Hidden,
        FunctionVolatility::Immutable,
        AggregateImplementationIdentity::try_new(ICEBERG_THETA_IMPLEMENTATION_IDENTITY)?,
        IcebergThetaAggregateFamily::try_new()?,
    )
}

pub fn validate_compact_theta(bytes: &[u8]) -> Result<(), IcebergThetaError> {
    decode_canonical_compact(bytes).map(|_| ())
}

pub fn estimate_compact_theta(bytes: &[u8]) -> Result<f64, IcebergThetaError> {
    decode_canonical_compact(bytes).map(|sketch| sketch.estimate())
}

pub fn union_compact_theta<'a>(
    bodies: impl IntoIterator<Item = &'a [u8]>,
) -> Result<Vec<u8>, IcebergThetaError> {
    let mut union = new_union()?;
    for body in bodies {
        let compact = decode_canonical_compact(body)?;
        union.update(&compact).map_err(|error| {
            IcebergThetaError::Sketch(format!("union Iceberg Theta body: {error}"))
        })?;
    }
    Ok(serialize_canonical_compact(&union.to_sketch(true)))
}

const THETA_OVERLOAD_PREFIX: &str = "iceberg/theta-stat/";
const THETA_OVERLOAD_SUFFIX: &str = "/v1";

fn overload_identity_matches(kind: CanonicalKind, identity: &str) -> bool {
    identity
        .strip_prefix(THETA_OVERLOAD_PREFIX)
        .and_then(|tail| tail.strip_suffix(THETA_OVERLOAD_SUFFIX))
        == Some(kind.identity_suffix())
}

fn overload_identity(kind: CanonicalKind) -> String {
    format!(
        "{THETA_OVERLOAD_PREFIX}{}{THETA_OVERLOAD_SUFFIX}",
        kind.identity_suffix()
    )
}

fn new_update_sketch() -> Result<ThetaSketch, IcebergThetaError> {
    ThetaSketchBuilder::default()
        .lg_k(DEFAULT_LG_K)
        .build()
        .map_err(|error| IcebergThetaError::Sketch(format!("create Theta update sketch: {error}")))
}

fn new_union() -> Result<ThetaUnion, IcebergThetaError> {
    ThetaUnionBuilder::default()
        .lg_k(DEFAULT_LG_K)
        .build()
        .map_err(|error| IcebergThetaError::Sketch(format!("create Theta union: {error}")))
}

fn decode_canonical_compact(bytes: &[u8]) -> Result<CompactThetaSketch, IcebergThetaError> {
    if bytes.len() > ICEBERG_THETA_MAX_COMPACT_BYTES {
        return Err(IcebergThetaError::CompactTooLarge {
            actual: bytes.len(),
            maximum: ICEBERG_THETA_MAX_COMPACT_BYTES,
        });
    }
    let sketch = CompactThetaSketch::deserialize(bytes)
        .map_err(|error| IcebergThetaError::InvalidCompact(error.to_string()))?;
    if !sketch.is_ordered() {
        return Err(IcebergThetaError::InvalidCompact(
            "compact body is not ordered".to_string(),
        ));
    }
    Ok(sketch)
}

fn serialize_canonical_compact(sketch: &CompactThetaSketch) -> Vec<u8> {
    if sketch.is_empty() {
        EMPTY_ORDERED_COMPACT_V3.to_vec()
    } else {
        sketch.serialize()
    }
}

fn build_output<'state, I>(states: I) -> Result<ArrayRef, IcebergThetaError>
where
    I: ExactSizeIterator<Item = &'state IcebergThetaState>,
{
    pure::build_output_observed(states, None).map_err(|error| match error {
        novarocks_functions::KernelFailure::ResourceExhausted => {
            IcebergThetaError::OutputResourceExhausted
        }
        _ => IcebergThetaError::InvalidOutputExtent,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
        FixedSizeBinaryArray, Float32Array, Float64Array, Int32Array, Int64Array, LargeBinaryArray,
        LargeStringArray, StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
        TimestampNanosecondArray,
    };
    use novarocks_functions::{EngineFunctionCatalogBuilder, FunctionResolutionError};

    fn compile_control() -> &'static dyn novarocks_type_contract::PureCompileControl {
        struct Control;
        impl novarocks_type_contract::PureCompileControl for Control {
            fn checkpoint(
                &self,
                _phase: novarocks_type_contract::CompilePhase,
                _work: u32,
            ) -> Result<(), novarocks_type_contract::CompileControlError> {
                Ok(())
            }
        }
        static CONTROL: Control = Control;
        &CONTROL
    }

    fn run_update(input: ArrayRef) -> Vec<u8> {
        let kernel = IcebergThetaKernel;
        let mut state = kernel.create_state().expect("state");
        let batch = AggregateInputBatch::try_new(Some(&input), input.len()).expect("batch");
        let prepared = kernel.prepare_update(&batch).expect("prepare");
        for row in 0..input.len() {
            kernel
                .update_row(&mut state, &prepared, row)
                .expect("update");
        }
        let output = kernel.build_final(std::iter::once(&state)).expect("output");
        output
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("binary")
            .value(0)
            .to_vec()
    }

    #[test]
    fn scalar_integer_domains_bind_exactly_and_share_iceberg_int_encoding() {
        use arrow_array::{Int8Array, Int16Array, Int32Array};
        let family = IcebergThetaAggregateFamily::try_new().expect("family");
        for (input, data_type, suffix) in [
            (
                Arc::new(Int8Array::from(vec![
                    Some(-128),
                    None,
                    Some(127),
                    Some(-128),
                ])) as ArrayRef,
                DataType::Int8,
                "tinyint",
            ),
            (
                Arc::new(Int16Array::from(vec![
                    Some(-128),
                    None,
                    Some(127),
                    Some(-128),
                ])) as ArrayRef,
                DataType::Int16,
                "smallint",
            ),
        ] {
            let signature = family
                .resolve_signature(std::slice::from_ref(&data_type))
                .expect("logical signature");
            assert_eq!(signature.argument_types, vec![data_type]);
            assert!(format!("{:?}", signature.overload).contains(suffix));
            let wide = Arc::new(Int32Array::from(vec![
                Some(-128),
                None,
                Some(127),
                Some(-128),
            ])) as ArrayRef;
            assert_eq!(
                run_update(input),
                run_update(wide),
                "standard Iceberg INT bytes, including negative sign extension"
            );
        }
    }

    fn java_oracle_compact(label: &str) -> Vec<u8> {
        let row = include_str!(
            "../../../../tests/datasketches-tck/fixtures/theta/iceberg_java62_single_value_vectors.tsv"
        )
        .lines()
        .find(|row| row.starts_with(label) && row.as_bytes().get(label.len()) == Some(&b'\t'))
        .unwrap_or_else(|| panic!("missing Java oracle row {label}"));
        let compact = row.split('\t').nth(2).expect("Java oracle compact column");
        assert_eq!(compact.len() % 2, 0);
        compact
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| {
                u8::from_str_radix(std::str::from_utf8(pair).expect("hex utf8"), 16)
                    .expect("hex byte")
            })
            .collect()
    }

    fn assert_java_oracle(label: &str, input: ArrayRef) {
        let actual = run_update(input);
        let expected = java_oracle_compact(label);
        if expected.len() == EMPTY_ORDERED_COMPACT_V3.len() {
            assert_eq!(actual, expected, "{label}");
        } else {
            // Java marks its read-only compact image with an additional
            // advisory flag that the pinned release does not emit. Both readers tolerate the
            // difference; the retained hash payload is the cross-language
            // proof of exact Iceberg value canonicalization.
            assert_eq!(&actual[8..], &expected[8..], "{label}");
        }
    }

    #[test]
    fn bundle_is_hidden_but_trusted_resolution_is_exact() {
        let mut builder = EngineFunctionCatalogBuilder::new();
        IcebergFunctionBundle.contribute(&mut builder).unwrap();
        let catalog = builder.seal().unwrap();
        assert_eq!(
            catalog.resolve_aggregate_user(ICEBERG_THETA_AGGREGATE_NAME, &[DataType::Int32]),
            Err(FunctionResolutionError::HiddenFunction)
        );
        let resolved = catalog
            .resolve_aggregate_trusted(ICEBERG_THETA_AGGREGATE_NAME, &[DataType::Int32])
            .unwrap();
        assert_eq!(resolved.output_type, DataType::Binary);
        assert_eq!(resolved.intermediate_type, DataType::Binary);
        assert_eq!(resolved.overload.as_str(), "iceberg/theta-stat/int/v1");
        assert_eq!(
            resolved.state_format.as_str(),
            ICEBERG_THETA_STATE_FORMAT_IDENTITY
        );
    }

    fn theta_value_request(
        arguments: &[novarocks_functions::FunctionArgument],
    ) -> novarocks_functions::FunctionBindingRequest<'_> {
        novarocks_functions::FunctionBindingRequest {
            arguments,
            logical_argument_count: arguments.len(),
            expected_result_type: None,
        }
    }

    fn theta_value_argument(
        value_type: FunctionValueType,
    ) -> novarocks_functions::FunctionArgument {
        novarocks_functions::FunctionArgument::Value {
            value_type,
            constant: None,
        }
    }

    #[test]
    fn exact_uuid_variant_and_physical_bindings_keep_overload_and_state_identity() {
        use novarocks_functions::{FunctionArgumentType, FunctionKind, FunctionResultType};
        let mut builder = EngineFunctionCatalogBuilder::new();
        IcebergFunctionBundle.contribute(&mut builder).unwrap();
        let catalog = builder.seal().unwrap();
        for nullable in [false, true] {
            for (value_type, overload) in [
                (
                    FunctionValueType::new(DataType::Int32, nullable),
                    "iceberg/theta-stat/int/v1",
                ),
                (
                    FunctionValueType::new(DataType::FixedSizeBinary(4), nullable),
                    "iceberg/theta-stat/fixed/v1",
                ),
                (
                    FunctionValueType::new(DataType::FixedSizeBinary(16), nullable),
                    "iceberg/theta-stat/fixed/v1",
                ),
                (
                    FunctionValueType::try_with_logical_type(
                        DataType::FixedSizeBinary(16),
                        nullable,
                        ValueLogicalType::Uuid,
                    )
                    .unwrap(),
                    "iceberg/theta-stat/fixed/v1",
                ),
                (
                    FunctionValueType::new(DataType::LargeBinary, nullable),
                    "iceberg/theta-stat/large-binary/v1",
                ),
                (
                    FunctionValueType::try_with_logical_type(
                        DataType::LargeBinary,
                        nullable,
                        ValueLogicalType::Variant,
                    )
                    .unwrap(),
                    "iceberg/theta-stat/large-binary/v1",
                ),
                (
                    FunctionValueType::try_with_logical_type(
                        DataType::FixedSizeBinary(16),
                        nullable,
                        ValueLogicalType::LargeInt,
                    )
                    .unwrap(),
                    "iceberg/theta-stat/fixed/v1",
                ),
                (
                    FunctionValueType::try_with_logical_type(
                        DataType::Binary,
                        nullable,
                        ValueLogicalType::Hll,
                    )
                    .unwrap(),
                    "iceberg/theta-stat/binary/v1",
                ),
                (
                    FunctionValueType::try_with_logical_type(
                        DataType::Binary,
                        nullable,
                        ValueLogicalType::Bitmap,
                    )
                    .unwrap(),
                    "iceberg/theta-stat/binary/v1",
                ),
            ] {
                let arguments = [theta_value_argument(value_type.clone())];
                let request = theta_value_request(&arguments);
                let bound = catalog
                    .resolve_bound_trusted(
                        ICEBERG_THETA_AGGREGATE_NAME,
                        FunctionKind::Aggregate,
                        request,
                        compile_control(),
                    )
                    .unwrap();
                catalog
                    .validate_bound(&bound, request, compile_control())
                    .unwrap();
                let exact = catalog
                    .select_exact_overload_observed(
                        &bound.function_id,
                        FunctionKind::Aggregate,
                        &bound.selected.overload,
                        request,
                        compile_control(),
                    )
                    .unwrap();
                assert_eq!(exact.as_ref(), &bound.selected);
                assert_eq!(
                    bound.selected.argument_types.as_ref(),
                    &[FunctionArgumentType::Value(value_type)]
                );
                assert_eq!(bound.selected.overload.as_str(), overload);
                assert_eq!(
                    bound.selected.result_type,
                    FunctionResultType::Scalar(FunctionValueType::new(DataType::Binary, false))
                );
                let state = bound.selected.aggregate.as_ref().unwrap();
                assert_eq!(
                    state.intermediate_type,
                    FunctionValueType::new(DataType::Binary, false)
                );
                assert_eq!(
                    state.state_format.as_str(),
                    ICEBERG_THETA_STATE_FORMAT_IDENTITY
                );
            }
        }
        let registration = iceberg_theta_registration().unwrap();
        assert_eq!(
            registration.implementation().as_str(),
            ICEBERG_THETA_IMPLEMENTATION_IDENTITY
        );
        let raw = [
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff,
        ];
        assert_java_oracle(
            "uuid",
            Arc::new(FixedSizeBinaryArray::try_from_iter([raw].into_iter()).unwrap()),
        );
    }

    #[test]
    fn theta_fixed_update_refuses_carrier_reselection_and_forged_preparation() {
        let family = IcebergThetaAggregateFamily::try_new().unwrap();
        let int = AggregateOverloadIdentity::try_new("iceberg/theta-stat/int/v1").unwrap();
        let selected = family
            .resolve_update_signature(&int, &[DataType::Int32])
            .unwrap();
        assert_eq!(
            selected,
            family.resolve_signature(&[DataType::Int32]).unwrap()
        );
        for actual in [
            vec![],
            vec![DataType::Int64],
            vec![DataType::Int32, DataType::Int32],
        ] {
            assert!(family.resolve_update_signature(&int, &actual).is_err());
        }
        let foreign = AggregateOverloadIdentity::try_new("foreign/theta-stat/int/v1").unwrap();
        assert!(
            family
                .resolve_update_signature(&foreign, &[DataType::Int32])
                .is_err()
        );
        let mut forged = selected.clone();
        forged.argument_types = vec![DataType::Int64];
        assert!(matches!(
            family.prepare(&forged, &AggregateBindOptions::default()),
            Err(IcebergThetaError::InvalidResolvedSignature)
        ));
        // The borrowed caller must keep the same original signature refusal,
        // before ordinary options errors, without creating an owned signature.
        let bad_options = AggregateBindOptions::try_new(true, &[], &[], None).unwrap();
        for axis in 0..5 {
            let mut wrong = selected.clone();
            match axis {
                0 => wrong.overload = foreign.clone(),
                1 => wrong.argument_types = vec![DataType::Int64],
                2 => wrong.intermediate_type = DataType::Int64,
                3 => wrong.output_type = DataType::Int64,
                4 => {
                    wrong.state_format =
                        AggregateStateFormatIdentity::try_new("foreign/theta/state-v1").unwrap()
                }
                _ => unreachable!(),
            }
            assert!(matches!(
                family.prepare(&wrong, &bad_options),
                Err(IcebergThetaError::InvalidResolvedSignature)
            ));
            assert!(matches!(
                family.prepare_borrowed(
                    wrong.overload.as_str(),
                    &wrong.argument_types,
                    &wrong.intermediate_type,
                    &wrong.output_type,
                    &wrong.state_format,
                    &bad_options
                ),
                Err(IcebergThetaError::InvalidResolvedSignature)
            ));
        }
        assert!(matches!(
            family.prepare_borrowed(
                selected.overload.as_str(),
                &selected.argument_types,
                &selected.intermediate_type,
                &selected.output_type,
                &selected.state_format,
                &bad_options
            ),
            Err(IcebergThetaError::InvalidBindOptions(
                "Iceberg Theta does not accept DISTINCT"
            ))
        ));
        let borrowed = family
            .prepare_borrowed(
                selected.overload.as_str(),
                &selected.argument_types,
                &selected.intermediate_type,
                &selected.output_type,
                &selected.state_format,
                &AggregateBindOptions::default(),
            )
            .unwrap();
        let borrowed_state = borrowed.create_state().unwrap();
        let empty = borrowed
            .build_final(std::iter::once(&borrowed_state))
            .unwrap();
        assert_eq!(
            empty
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0),
            EMPTY_ORDERED_COMPACT_V3
        );
        let kernel = family
            .prepare(&selected, &AggregateBindOptions::default())
            .unwrap();
        let state = kernel.create_state().unwrap();
        assert_eq!(
            kernel.build_final(std::iter::once(&state)).unwrap().len(),
            1
        );
    }

    #[test]
    fn exact_theta_validation_rejects_root_forgery_and_new_logical_domains() {
        assert!(
            FunctionValueType::try_with_logical_type(
                DataType::Binary,
                false,
                ValueLogicalType::Variant
            )
            .is_err()
        );
        assert!(
            FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(4),
                false,
                ValueLogicalType::Uuid
            )
            .is_err()
        );
        use novarocks_functions::{FunctionArgumentType, FunctionKind};
        let mut builder = EngineFunctionCatalogBuilder::new();
        IcebergFunctionBundle.contribute(&mut builder).unwrap();
        let catalog = builder.seal().unwrap();
        let uuid = FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            false,
            ValueLogicalType::Uuid,
        )
        .unwrap();
        let arguments = [theta_value_argument(uuid.clone())];
        let bound = catalog
            .resolve_bound_trusted(
                ICEBERG_THETA_AGGREGATE_NAME,
                FunctionKind::Aggregate,
                theta_value_request(&arguments),
                compile_control(),
            )
            .unwrap();
        // Even another admitted domain cannot replace a frozen source while
        // the real expression still supplies UUID.
        let mut changed = bound.clone();
        let mut changed_arguments = changed.selected.argument_types.to_vec();
        changed_arguments[0] = FunctionArgumentType::Value(FunctionValueType::new(
            DataType::FixedSizeBinary(16),
            false,
        ));
        changed.selected.argument_types = novarocks_type_contract::TypedFactSlice::from_pure_boxed(
            changed_arguments.into_boxed_slice(),
        );
        assert!(
            catalog
                .validate_bound(&changed, theta_value_request(&arguments), compile_control())
                .is_err()
        );
        for value_type in [
            FunctionValueType::try_with_logical_type(
                DataType::Binary,
                false,
                ValueLogicalType::Object,
            )
            .unwrap(),
            FunctionValueType::try_with_logical_type(
                DataType::Binary,
                false,
                ValueLogicalType::Percentile,
            )
            .unwrap(),
            FunctionValueType::try_with_logical_type(
                DataType::LargeBinary,
                false,
                ValueLogicalType::Json,
            )
            .unwrap(),
        ] {
            let arguments = [theta_value_argument(value_type.clone())];
            let request = theta_value_request(&arguments);
            assert!(
                catalog
                    .resolve_bound_trusted(
                        ICEBERG_THETA_AGGREGATE_NAME,
                        FunctionKind::Aggregate,
                        request,
                        compile_control(),
                    )
                    .is_err(),
                "{value_type:?}"
            );
            let mut changed = bound.clone();
            let mut changed_arguments = changed.selected.argument_types.to_vec();
            changed_arguments[0] = FunctionArgumentType::Value(value_type.clone());
            changed.selected.argument_types =
                novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                    changed_arguments.into_boxed_slice(),
                );
            // Change both the request and selected root to exercise the
            // family gate after exact expression/selection correspondence.
            assert!(
                catalog
                    .validate_bound(&changed, request, compile_control())
                    .is_err(),
                "{value_type:?}"
            );
        }
        let family = IcebergThetaAggregateFamily::try_new().unwrap();
        assert!(family.validate_value_arguments(&[uuid]).is_ok());
        assert!(
            family
                .validate_value_arguments(&[FunctionValueType::new(DataType::UInt8, false)])
                .is_err()
        );
    }

    #[test]
    fn variant_statistics_preserve_the_existing_serialized_raw_bytes() {
        use novarocks_functions::FunctionKind;
        let mut builder = EngineFunctionCatalogBuilder::new();
        IcebergFunctionBundle.contribute(&mut builder).unwrap();
        let catalog = builder.seal().unwrap();
        let variant = FunctionValueType::try_with_logical_type(
            DataType::LargeBinary,
            true,
            ValueLogicalType::Variant,
        )
        .unwrap();
        let arguments = [theta_value_argument(variant)];
        let request = theta_value_request(&arguments);
        let bound = catalog
            .resolve_bound_trusted(
                ICEBERG_THETA_AGGREGATE_NAME,
                FunctionKind::Aggregate,
                request,
                compile_control(),
            )
            .unwrap();
        catalog
            .validate_bound(&bound, request, compile_control())
            .unwrap();
        assert_eq!(
            bound.selected.overload.as_str(),
            "iceberg/theta-stat/large-binary/v1"
        );
        let raw_a: &[u8] = &[0, 1, 255, 127];
        let raw_b: &[u8] = &[127, 255, 1, 0];
        // The legacy producer supplied these exact serialized bytes. The
        // family must not decode VARIANT values or normalize their contents.
        let values = vec![Some(raw_a), None, Some(raw_b), Some(raw_a)];
        let actual = run_update(Arc::new(LargeBinaryArray::from(values.clone())));
        let old_binary = run_update(Arc::new(BinaryArray::from(values)));
        assert_eq!(actual, old_binary);
        assert_eq!(estimate_compact_theta(&actual).unwrap(), 2.0);
    }

    #[test]
    fn closed_signature_set_rejects_ambiguous_and_nested_types() {
        let family = IcebergThetaAggregateFamily::try_new().unwrap();
        for unsupported in [
            DataType::UInt8,
            DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
            DataType::Decimal128(38, -1),
            DataType::List(Arc::new(arrow_schema::Field::new(
                "item",
                DataType::Int32,
                true,
            ))),
        ] {
            assert!(family.resolve_signature(&[unsupported]).is_err());
        }
    }

    #[test]
    fn empty_and_duplicate_values_are_standard_compact() {
        let empty = run_update(Arc::new(Int32Array::from(Vec::<Option<i32>>::new())));
        assert_eq!(empty, EMPTY_ORDERED_COMPACT_V3);
        let body = run_update(Arc::new(Int32Array::from(vec![Some(7), None, Some(7)])));
        validate_compact_theta(&body).unwrap();
        assert_eq!(estimate_compact_theta(&body).unwrap(), 1.0);
    }

    #[test]
    fn distinct_canonical_bytes_remain_distinct_without_private_float_normalization() {
        let zeros = run_update(Arc::new(Float32Array::from(vec![0.0_f32, -0.0_f32])));
        assert_eq!(estimate_compact_theta(&zeros).unwrap(), 2.0);

        let decimals = Decimal128Array::from(vec![127_i128, 128, -128, -129])
            .with_precision_and_scale(10, 2)
            .unwrap();
        assert_eq!(
            estimate_compact_theta(&run_update(Arc::new(decimals))).unwrap(),
            4.0
        );
    }

    #[test]
    fn every_accepted_primitive_matches_iceberg_java_single_value_oracle() {
        assert_java_oracle("boolean_false", Arc::new(BooleanArray::from(vec![false])));
        assert_java_oracle("boolean_true", Arc::new(BooleanArray::from(vec![true])));
        assert_java_oracle(
            "int32_negative",
            Arc::new(Int32Array::from(vec![-123_456_789])),
        );
        assert_java_oracle(
            "int64_negative",
            Arc::new(Int64Array::from(vec![-1_234_567_890_123_456_789])),
        );
        assert_java_oracle(
            "float_negative_zero",
            Arc::new(Float32Array::from(vec![f32::from_bits(0x8000_0000)])),
        );
        assert_java_oracle(
            "float_nan_payload",
            Arc::new(Float32Array::from(vec![f32::from_bits(0x7fc0_0001)])),
        );
        assert_java_oracle(
            "double_negative_zero",
            Arc::new(Float64Array::from(vec![f64::from_bits(
                0x8000_0000_0000_0000,
            )])),
        );
        assert_java_oracle(
            "double_nan_payload",
            Arc::new(Float64Array::from(vec![f64::from_bits(
                0x7ff8_0000_0000_0001,
            )])),
        );
        assert_java_oracle(
            "decimal_negative",
            Arc::new(
                Decimal128Array::from(vec![-129_i128])
                    .with_precision_and_scale(38, 4)
                    .unwrap(),
            ),
        );
        assert_java_oracle("date_negative", Arc::new(Date32Array::from(vec![-12_345])));
        assert_java_oracle(
            "time_micros",
            Arc::new(Time64MicrosecondArray::from(vec![1_234_567_890])),
        );
        assert_java_oracle(
            "timestamp_micros",
            Arc::new(TimestampMicrosecondArray::from(vec![-1_234_567_890_123])),
        );
        assert_java_oracle(
            "timestamp_nanos",
            Arc::new(TimestampNanosecondArray::from(vec![
                1_234_567_890_123_456_789,
            ])),
        );
        assert_java_oracle("utf8", Arc::new(StringArray::from(vec!["NovaRocks-雪"])));
        assert_java_oracle(
            "utf8",
            Arc::new(LargeStringArray::from(vec!["NovaRocks-雪"])),
        );
        assert_java_oracle("utf8_empty", Arc::new(StringArray::from(vec![""])));
        assert_java_oracle(
            "binary",
            Arc::new(BinaryArray::from(vec![&[0, 1, 255, 127][..]])),
        );
        assert_java_oracle(
            "binary",
            Arc::new(LargeBinaryArray::from(vec![&[0, 1, 255, 127][..]])),
        );
        assert_java_oracle("binary_empty", Arc::new(BinaryArray::from(vec![&[][..]])));
        assert_java_oracle(
            "fixed",
            Arc::new(FixedSizeBinaryArray::try_from_iter([[0, 1, 255, 127]].into_iter()).unwrap()),
        );
        // T07 binds Iceberg UUID as FixedSizeBinary(16); T06 intentionally
        // does not guess UUID semantics from Utf8 contents.
        assert_java_oracle(
            "uuid",
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(
                    [[
                        0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb,
                        0xcc, 0xdd, 0xee, 0xff,
                    ]]
                    .into_iter(),
                )
                .unwrap(),
            ),
        );
    }

    #[test]
    fn utf8_is_raw_and_empty_matches_java_zero_length_update() {
        let body = run_update(Arc::new(StringArray::from(vec!["", "abc", "abc"])));
        assert_eq!(estimate_compact_theta(&body).unwrap(), 1.0);
    }

    #[test]
    fn pure_validate_union_and_estimate_accept_java_and_rust_vectors() {
        let left = include_bytes!(
            "../../../../tests/datasketches-tck/fixtures/theta/java62_quickselect_overlap_left_ordered_v3.sk"
        );
        let right = include_bytes!(
            "../../../../tests/datasketches-tck/fixtures/theta/java62_quickselect_overlap_right_ordered_v3.sk"
        );
        validate_compact_theta(left).unwrap();
        validate_compact_theta(right).unwrap();
        let union = union_compact_theta([left.as_slice(), right.as_slice()]).unwrap();
        validate_compact_theta(&union).unwrap();
        assert_eq!(estimate_compact_theta(&union).unwrap(), 1100.0);

        // Iceberg names Alpha as its reference producer. NovaRocks uses the
        // accepted pinned QuickSelect substrate, while preserving standard
        // compact/set-operation interoperability with Alpha bodies.
        let alpha = include_bytes!(
            "../../../../tests/datasketches-tck/fixtures/theta/java62_alpha_n100000_ordered_v3.sk"
        );
        validate_compact_theta(alpha).unwrap();
        let mixed = union_compact_theta([alpha.as_slice(), right.as_slice()]).unwrap();
        validate_compact_theta(&mixed).unwrap();
        assert!((90_000.0..110_000.0).contains(&estimate_compact_theta(&mixed).unwrap()));
    }

    #[test]
    fn validation_rejects_seed_mismatch_unordered_and_oversized() {
        let custom_seed = include_bytes!(
            "../../../../tests/datasketches-tck/fixtures/theta/java62_quickselect_n1000_custom_seed_ordered_v3.sk"
        );
        let unordered = include_bytes!(
            "../../../../tests/datasketches-tck/fixtures/theta/java62_quickselect_n1000_unordered_v3.sk"
        );
        assert!(validate_compact_theta(custom_seed).is_err());
        assert!(validate_compact_theta(unordered).is_err());

        assert!(validate_compact_theta(&vec![0; ICEBERG_THETA_MAX_COMPACT_BYTES + 1]).is_err());
    }

    #[test]
    fn validation_preserves_tolerant_decode_domain() {
        let mut body = run_update(Arc::new(Int32Array::from(vec![1, 2, 3])));
        body.extend_from_slice(&[0xa5; 16]);
        body[5] |= 0x80;
        validate_compact_theta(&body).unwrap();
        assert_eq!(estimate_compact_theta(&body).unwrap(), 3.0);
    }

    #[test]
    fn retained_memory_is_o1_exact_and_bounded_well_below_legacy_cap() {
        assert_eq!(MAX_TABLE_CAPACITY, 8_192);
        assert_eq!(MAX_TABLE_RETAINED_BYTES, 65_536);
        assert_eq!(ICEBERG_THETA_OPERATION_MAX_RETAINED_BYTES, 132_096);

        let kernel = IcebergThetaKernel;
        let mut state = kernel.create_state().unwrap();
        let initial = kernel.retained_bytes(&state);
        let input: ArrayRef = Arc::new(Int32Array::from_iter_values(0..100_000));
        let batch = AggregateInputBatch::try_new(Some(&input), input.len()).unwrap();
        let prepared = kernel.prepare_update(&batch).unwrap();
        for row in 0..input.len() {
            kernel.update_row(&mut state, &prepared, row).unwrap();
        }
        let retained = kernel.retained_bytes(&state);
        assert!(
            initial <= INITIAL_TABLE_RETAINED_BYTES,
            "initial={initial}, bound={INITIAL_TABLE_RETAINED_BYTES}"
        );
        assert!(retained <= MAX_TABLE_RETAINED_BYTES);
        assert!(retained > initial);
        assert_eq!(
            kernel.memory_policy(),
            AggregateStateMemoryPolicy::BoundedRetained {
                max_retained_bytes_per_state: ICEBERG_THETA_OPERATION_MAX_RETAINED_BYTES,
            }
        );
        let java_partial = include_bytes!(
            "../../../../tests/datasketches-tck/fixtures/theta/java62_quickselect_n100000_ordered_v3.sk"
        );
        let partials: ArrayRef = Arc::new(BinaryArray::from(vec![java_partial.as_slice()]));
        let batch = AggregateInputBatch::try_new(Some(&partials), 1).unwrap();
        let prepared = kernel.prepare_merge(&batch).unwrap();
        let mut merged = kernel.create_state().unwrap();
        kernel.merge_row(&mut merged, &prepared, 0).unwrap();
        assert!(kernel.retained_bytes(&merged) <= MAX_TABLE_RETAINED_BYTES);
        let output = kernel.build_final(std::iter::once(&merged)).unwrap();
        let output = output.as_any().downcast_ref::<BinaryArray>().unwrap();
        let estimate = estimate_compact_theta(output.value(0)).unwrap();
        assert!((90_000.0..110_000.0).contains(&estimate));
    }

    #[test]
    fn theta_state_law_is_owned_by_every_registered_overload_and_exact_selection() {
        use novarocks_functions::AggregateStateArgumentContract;
        use novarocks_functions::{FunctionArgumentType, FunctionKind, FunctionResultType};
        let registration = iceberg_theta_registration().unwrap();
        let family = registration.family().clone();
        let declaration = registration.definition().binding_declaration().unwrap();
        assert_eq!(declaration.overloads().len(), 17);
        for overload in declaration.overloads() {
            let identity = AggregateOverloadIdentity::try_new(overload.identity.as_str()).unwrap();
            assert_eq!(
                family.state_argument_contract(&identity).unwrap(),
                AggregateStateArgumentContract::ValueRootNullabilityIndependent
            );
            assert_eq!(
                overload.aggregate.as_ref().unwrap().state_argument_contract,
                AggregateStateArgumentContract::ValueRootNullabilityIndependent
            );
        }
        let foreign = AggregateOverloadIdentity::try_new("foreign/theta-state/v1").unwrap();
        assert!(
            matches!(family.state_argument_contract(&foreign), Err(FunctionResolutionError::BadSignature(message)) if message == "Iceberg Theta state contract references a foreign overload")
        );
        let mut builder = EngineFunctionCatalogBuilder::new();
        builder.register(registration.definition().clone()).unwrap();
        let catalog = builder.seal_bound().unwrap();
        let carriers = [
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal128(18, 2),
            DataType::Date32,
            DataType::Time64(arrow_schema::TimeUnit::Microsecond),
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Binary,
            DataType::LargeBinary,
            DataType::FixedSizeBinary(16),
        ];
        for carrier in carriers {
            for nullable in [false, true] {
                let value_type = FunctionValueType::new(carrier.clone(), nullable);
                let arguments = [theta_value_argument(value_type.clone())];
                let request = theta_value_request(&arguments);
                let original = catalog
                    .resolve_bound_trusted(
                        ICEBERG_THETA_AGGREGATE_NAME,
                        FunctionKind::Aggregate,
                        request,
                        compile_control(),
                    )
                    .unwrap();
                let selected = catalog
                    .select_exact_overload_observed(
                        &original.function_id,
                        FunctionKind::Aggregate,
                        &original.selected.overload,
                        request,
                        compile_control(),
                    )
                    .unwrap();
                assert_eq!(selected.as_ref(), &original.selected);
                assert_eq!(
                    selected.argument_types.as_ref(),
                    &[FunctionArgumentType::Value(value_type)]
                );
                assert_eq!(
                    selected.result_type,
                    FunctionResultType::Scalar(FunctionValueType::new(DataType::Binary, false))
                );
                let state = selected.aggregate.as_ref().unwrap();
                assert_eq!(
                    state.state_argument_contract,
                    AggregateStateArgumentContract::ValueRootNullabilityIndependent
                );
                assert_eq!(
                    state.intermediate_type,
                    FunctionValueType::new(DataType::Binary, false)
                );
                assert_eq!(
                    state.state_format.as_str(),
                    ICEBERG_THETA_STATE_FORMAT_IDENTITY
                );
                let mut forged = selected.as_ref().clone();
                forged.aggregate.as_mut().unwrap().state_argument_contract =
                    AggregateStateArgumentContract::ExactSignature;
                assert!(
                    catalog
                        .validate_frozen_selection(
                            &original.function_id,
                            FunctionKind::Aggregate,
                            &forged,
                            request,
                            compile_control()
                        )
                        .is_err()
                );
            }
        }
        assert!(
            Arc::ptr_eq(&family, registration.family()),
            "declaration and selections keep the original family owner"
        );
    }

    #[test]
    fn theta_null_update_and_merge_preserve_original_nonnull_compact_bytes() {
        let nonnull = run_update(Arc::new(Int64Array::from(vec![7, 11])));
        let nullable = run_update(Arc::new(Int64Array::from(vec![
            None,
            Some(7),
            None,
            Some(11),
            Some(7),
            None,
        ])));
        assert_eq!(nullable, nonnull);
        assert_eq!(estimate_compact_theta(&nullable).unwrap(), 2.0);
        assert_eq!(
            run_update(Arc::new(Int64Array::from(vec![None, None]))),
            EMPTY_ORDERED_COMPACT_V3
        );
        let kernel = IcebergThetaKernel;
        for rows in [
            vec![Some(nonnull.as_slice())],
            vec![None, Some(nonnull.as_slice()), None],
        ] {
            let array: ArrayRef = Arc::new(BinaryArray::from(rows));
            let batch = AggregateInputBatch::try_new(Some(&array), array.len()).unwrap();
            let prepared = kernel.prepare_merge(&batch).unwrap();
            let mut state = kernel.create_state().unwrap();
            for row in 0..array.len() {
                kernel.merge_row(&mut state, &prepared, row).unwrap();
            }
            let output = kernel.build_final(std::iter::once(&state)).unwrap();
            let output = output.as_any().downcast_ref::<BinaryArray>().unwrap();
            assert_eq!(output.len(), 1);
            assert!(!output.is_null(0));
            assert_eq!(output.value(0), nonnull);
        }
        let nulls: ArrayRef = Arc::new(BinaryArray::from(vec![None::<&[u8]>, None]));
        let batch = AggregateInputBatch::try_new(Some(&nulls), nulls.len()).unwrap();
        let prepared = kernel.prepare_merge(&batch).unwrap();
        let mut state = kernel.create_state().unwrap();
        for row in 0..nulls.len() {
            kernel.merge_row(&mut state, &prepared, row).unwrap();
        }
        let output = kernel.build_final(std::iter::once(&state)).unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0),
            EMPTY_ORDERED_COMPACT_V3
        );
    }
}

#[cfg(test)]
#[path = "theta_created_tests.rs"]
mod created_tests;

#[cfg(test)]
mod failure_projection_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    #[test]
    fn actual_type_author_refusal_drops_source_metadata_before_cause() {
        let field = Arc::new(arrow_schema::Field::new("value", DataType::Utf8, true));
        let weak = Arc::downgrade(&field);
        let data_type = DataType::Struct(vec![field.clone()].into());
        let error = CanonicalKind::from_data_type(&data_type).unwrap_err();
        drop(data_type);
        drop(field);
        assert!(weak.upgrade().is_none());
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Invariant);
        assert_eq!(
            fact.reason().domain(),
            novarocks_failure_contract::domain::CONNECTOR.get()
        );
        assert_eq!(fact.reason().code(), 200);
        assert!(error.to_string().contains("input type Struct"));
    }
}
