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

use arrow_array::builder::BinaryBuilder;
use arrow_array::{Array, ArrayRef, BinaryArray};
use arrow_schema::DataType;
use datasketches::hash::value::raw_bytes;
use datasketches::theta::{
    CompactThetaSketch, ThetaSketch, ThetaSketchBuilder, ThetaUnion, ThetaUnionBuilder,
};
use novarocks_functions::{
    AggregateBindOptions, AggregateImplementationIdentity, AggregateInputBatch,
    AggregateOverloadDeclaration, AggregateOverloadIdentity, AggregateStateFormatIdentity,
    AggregateStateMemoryPolicy, EngineFunctionCatalogBuilder, FunctionBundleContributor,
    FunctionCatalogError, FunctionResolutionError, FunctionVisibility, FunctionVolatility,
    ResolvedAggregateSignature, TypedAggregateFamily, TypedAggregateKernel,
    TypedAggregateRegistration,
};

use crate::canonical::{CanonicalKind, PreparedCanonicalBatch, canonical_width};

pub const ICEBERG_THETA_AGGREGATE_NAME: &str = "$iceberg_theta_stat";
pub const ICEBERG_THETA_STATE_FORMAT_IDENTITY: &str =
    "iceberg/apache-datasketches-theta-v1/default-seed/ordered-compact";
pub const ICEBERG_THETA_IMPLEMENTATION_IDENTITY: &str =
    "iceberg/theta-quickselect-rc1/default-seed/v1";

const DEFAULT_LG_K: u8 = 12;
const _: () = assert!(
    size_of::<usize>() == 8,
    "Iceberg Theta bounds require a 64-bit target"
);
const MAX_TABLE_CAPACITY: usize = 1 << (DEFAULT_LG_K + 1);
// RC1 exposes footprint, not its private table slot. The 64-bit Theta slot is
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
    UnsupportedInputType(DataType),
    ArrayTypeMismatch(DataType),
    MissingInput,
    InvalidBindOptions(&'static str),
    InvalidResolvedSignature,
    InvalidCompact(String),
    CompactTooLarge { actual: usize, maximum: usize },
    MixedUpdateAndMerge,
    Sketch(String),
}

impl fmt::Display for IcebergThetaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedInputType(data_type) => {
                write!(
                    formatter,
                    "Iceberg Theta does not support input type {data_type:?}"
                )
            }
            Self::ArrayTypeMismatch(data_type) => {
                write!(
                    formatter,
                    "Iceberg Theta input array does not match {data_type:?}"
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
        }
    }
}

impl std::error::Error for IcebergThetaError {}

pub struct IcebergThetaAggregateFamily {
    overloads: Box<[AggregateOverloadDeclaration]>,
}

impl IcebergThetaAggregateFamily {
    pub fn try_new() -> Result<Self, FunctionCatalogError> {
        let kinds = [
            CanonicalKind::Boolean,
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
}

impl TypedAggregateFamily for IcebergThetaAggregateFamily {
    type Kernel = IcebergThetaKernel;
    type PrepareError = IcebergThetaError;

    fn overloads(&self) -> &[AggregateOverloadDeclaration] {
        &self.overloads
    }

    fn resolve_signature(
        &self,
        argument_types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
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
        Ok(ResolvedAggregateSignature {
            overload: AggregateOverloadIdentity::try_new(overload_identity(kind))
                .map_err(|error| FunctionResolutionError::BadSignature(error.to_string()))?,
            argument_types: vec![argument_type.clone()],
            intermediate_type: DataType::Binary,
            output_type: DataType::Binary,
            state_format: AggregateStateFormatIdentity::try_new(
                ICEBERG_THETA_STATE_FORMAT_IDENTITY,
            )
            .map_err(|error| FunctionResolutionError::BadSignature(error.to_string()))?,
        })
    }

    fn prepare(
        &self,
        selected: &ResolvedAggregateSignature,
        options: &AggregateBindOptions,
    ) -> Result<Self::Kernel, Self::PrepareError> {
        let expected = self
            .resolve_signature(&selected.argument_types)
            .map_err(|_| IcebergThetaError::InvalidResolvedSignature)?;
        if expected != *selected {
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
                IcebergThetaError::ArrayTypeMismatch(
                    input.values().expect("checked above").data_type().clone(),
                )
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

fn overload_identity(kind: CanonicalKind) -> String {
    format!("iceberg/theta-stat/{}/v1", kind.identity_suffix())
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
    let state_count = states.len();
    // Do not reserve the per-state worst case. Empty and sparse sketches are
    // common, and Arrow grows this output from the actual serialized bodies.
    let mut builder = BinaryBuilder::with_capacity(state_count, 0);
    for state in states {
        let compact = match &state.mode {
            ThetaStateMode::Update(_) if !state.has_updates => {
                builder.append_value(EMPTY_ORDERED_COMPACT_V3);
                continue;
            }
            ThetaStateMode::Update(sketch) => sketch.compact(true),
            ThetaStateMode::Merge(union) => union.to_sketch(true),
        };
        builder.append_value(serialize_canonical_compact(&compact));
    }
    Ok(Arc::new(builder.finish()))
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
            .chunks_exact(2)
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
            // advisory flag that RC1 does not emit. Both readers tolerate the
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

    #[test]
    fn closed_signature_set_rejects_ambiguous_and_nested_types() {
        let family = IcebergThetaAggregateFamily::try_new().unwrap();
        for unsupported in [
            DataType::Int8,
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
    fn pure_validate_union_and_estimate_accept_java_and_rust_rc1_vectors() {
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
        // accepted RC1 QuickSelect substrate, while preserving standard
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
    fn validation_preserves_rc1_tolerant_decode_domain() {
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
}
