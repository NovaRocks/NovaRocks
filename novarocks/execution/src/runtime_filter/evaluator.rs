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

//! Execution-owned runtime-filter row evaluation.
//!
//! The artifact query is deliberately narrower than an evaluator: a Backend
//! adapter can expose immutable, validated artifact facts, but it never sees
//! Arrow arrays and cannot make a fragment-local row or scan decision.
// Design: ADR-0043 (docs/adr/ADR-0043-runtime-filter-artifact-query-and-evaluator-boundary.md)

mod controlled;
pub(crate) use controlled::evaluate_rows_controlled;

use arrow::array::{Array, ArrayData, ArrayRef, BooleanArray};
use arrow::datatypes::{DataType, TimeUnit};
use novarocks_spi::connector::ConnectorScalarValue;

use super::{
    LogicalVersion, RuntimeFilterBindingId, RuntimeFilterContractViolation,
    RuntimeFilterContractViolationKind,
};

/// A borrowed, execution-owned scalar extracted from an Arrow input column.
///
/// The containing [`RuntimeFilterArtifactQuery`] has already frozen the Arrow
/// data type.  This value therefore intentionally carries no lossy type tag:
/// callers must never infer a timestamp unit, decimal scale, or LargeInt width
/// from its payload.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RuntimeFilterScalarRef<'a> {
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    LargeInt(i128),
    Float32(f32),
    Float64(f64),
    Utf8(&'a str),
    Date32(i32),
    TimestampSecond(i64),
    TimestampMillisecond(i64),
    TimestampMicrosecond(i64),
    TimestampNanosecond(i64),
    Temporal([u8; 12]),
    Decimal128(i128),
}

/// Closed failure categories for an immutable artifact query.
///
/// `ContractViolation` means the retained artifact no longer matches the
/// fragment contract and is fail-fast.  The other variants are ordinary
/// fail-open evaluation outcomes owned by Execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterArtifactQueryError {
    Unsupported,
    ResourceUnavailable,
    ContractViolation,
}

/// The controlled scalar port preserves interruption separately from the
/// artifact owner's original immutable query verdicts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeFilterControlledQueryError {
    Artifact(RuntimeFilterArtifactQueryError),
    Kernel(novarocks_functions::KernelFailure),
}
impl From<RuntimeFilterArtifactQueryError> for RuntimeFilterControlledQueryError {
    fn from(error: RuntimeFilterArtifactQueryError) -> Self {
        Self::Artifact(error)
    }
}
impl From<novarocks_functions::KernelFailure> for RuntimeFilterControlledQueryError {
    fn from(error: novarocks_functions::KernelFailure) -> Self {
        Self::Kernel(error)
    }
}

/// Explicitly installed computation capability. It owns no row mask/effect
/// and accepts neither Arrow nor Task/scan facts. Absence is not a fallback
/// to the original uncontrolled query methods.
pub trait RuntimeFilterControlledScalarQuery: Send + Sync {
    fn matches_null_controlled(
        &self,
        control: &dyn novarocks_functions::KernelEvaluationControl,
    ) -> Result<bool, RuntimeFilterControlledQueryError>;

    fn non_null_value_may_match_controlled(
        &self,
        value: RuntimeFilterScalarRef<'_>,
        control: &dyn novarocks_functions::KernelEvaluationControl,
    ) -> Result<bool, RuntimeFilterControlledQueryError>;
}

/// Neutral, immutable queries over one retained runtime-filter artifact.
///
/// Implementations belong to the participant owner.  This trait deliberately
/// accepts neither Arrow batches nor connector scan facts and returns neither
/// a Boolean mask nor an evaluation outcome/effect.  Execution alone combines
/// those artifact facts with fragment data and decides fail-open behavior.
pub trait RuntimeFilterArtifactQuery: Send + Sync {
    /// The exact, frozen Arrow data type accepted by this artifact.
    fn data_type(&self) -> &DataType;

    fn temporal_type(&self) -> Option<novarocks_type_contract::TemporalType> {
        None
    }

    /// The declared temporal family when the artifact consumes normalized
    /// relational keys. Compact membership can keep its raw equality owner.
    fn temporal_relation_key_type(&self) -> Option<novarocks_type_contract::TemporalType> {
        self.temporal_type()
    }

    /// None explicitly identifies an uninstalled controlled capability. New
    /// compiled consumers must refuse it, including on an all-NULL batch.
    fn controlled_scalar_query(&self) -> Option<&dyn RuntimeFilterControlledScalarQuery> {
        None
    }

    /// Whether a null input value can match the artifact under its frozen null
    /// semantics.
    fn matches_null(&self) -> Result<bool, RuntimeFilterArtifactQueryError>;

    /// Whether the artifact contains any non-null matches.
    fn has_non_null_matches(&self) -> Result<bool, RuntimeFilterArtifactQueryError>;

    /// Whether one non-null scalar can match the artifact.
    fn non_null_value_may_match(
        &self,
        value: RuntimeFilterScalarRef<'_>,
    ) -> Result<bool, RuntimeFilterArtifactQueryError>;

    /// Whether an inclusive closed connector range can match the artifact.
    /// This is a primitive only; Execution owns all scan-facts validation and
    /// final scan-unit outcomes.
    fn non_null_range_may_match(
        &self,
        inclusive_min: &ConnectorScalarValue,
        inclusive_max: &ConnectorScalarValue,
    ) -> Result<bool, RuntimeFilterArtifactQueryError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterRowNotEvaluatedReason {
    DataTypeUnsupported,
    ArtifactQueryUnsupported,
    ResourceUnavailable,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RuntimeFilterRowEvaluation {
    Evaluated {
        mask: BooleanArray,
        logical_version: LogicalVersion,
    },
    NotEvaluated {
        reason: RuntimeFilterRowNotEvaluatedReason,
        observed_version: LogicalVersion,
    },
}

/// The binding-local result of evaluating one Arrow input column.
#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeFilterRowOutcome {
    binding_id: RuntimeFilterBindingId,
    evaluation: RuntimeFilterRowEvaluation,
}

impl RuntimeFilterRowOutcome {
    pub const fn binding_id(&self) -> RuntimeFilterBindingId {
        self.binding_id
    }

    pub const fn evaluation(&self) -> &RuntimeFilterRowEvaluation {
        &self.evaluation
    }

    /// Effects are derived from an evaluated mask only.  In particular, a
    /// fail-open outcome cannot fabricate profile deltas for a version it did
    /// not evaluate.
    pub fn effect(&self) -> Option<RuntimeFilterRowEffect> {
        match self.effect_observed(|| Ok::<(), std::convert::Infallible>(())) {
            Ok(effect) => effect,
            Err(impossible) => match impossible {},
        }
    }
    fn effect_observed<E>(
        &self,
        mut observe: impl FnMut() -> Result<(), E>,
    ) -> Result<Option<RuntimeFilterRowEffect>, E> {
        let RuntimeFilterRowEvaluation::Evaluated {
            mask,
            logical_version,
        } = &self.evaluation
        else {
            return Ok(None);
        };
        let mut output_rows = 0;
        for matched in mask.values().iter() {
            observe()?;
            output_rows += u64::from(matched);
        }
        Ok(Some(RuntimeFilterRowEffect {
            binding_id: self.binding_id,
            logical_version: *logical_version,
            input_rows: mask.len() as u64,
            output_rows,
        }))
    }
}

/// A profile/event fact that can only be obtained from an evaluated row mask.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeFilterRowEffect {
    binding_id: RuntimeFilterBindingId,
    logical_version: LogicalVersion,
    input_rows: u64,
    output_rows: u64,
}

impl RuntimeFilterRowEffect {
    pub const fn binding_id(self) -> RuntimeFilterBindingId {
        self.binding_id
    }

    pub const fn logical_version(self) -> LogicalVersion {
        self.logical_version
    }

    pub const fn input_rows(self) -> u64 {
        self.input_rows
    }

    pub const fn output_rows(self) -> u64 {
        self.output_rows
    }
}

/// Evaluates one Arrow input column through an immutable artifact query.
///
/// The input type must exactly equal the artifact's frozen type.  A mismatch
/// is an invalid fragment contract, not a best-effort conversion.  Supported
/// Arrow values are converted into [`RuntimeFilterScalarRef`] one row at a
/// time, so participant adapters never receive Arrow memory.
pub fn evaluate_rows(
    binding_id: RuntimeFilterBindingId,
    logical_version: LogicalVersion,
    artifact: &dyn RuntimeFilterArtifactQuery,
    input: &ArrayRef,
) -> Result<RuntimeFilterRowOutcome, RuntimeFilterContractViolation> {
    evaluate_rows_with(
        &mut LegacyRowQueryPolicy,
        binding_id,
        logical_version,
        artifact,
        input,
    )
}

// The inner verdict is the original advisory artifact result; the outer
// error is the policy's fatal cause. Neither shell parses error messages.
trait RowQueryPolicy {
    type Error: From<RuntimeFilterContractViolation>;
    fn before_row(&mut self) -> Result<(), Self::Error>;
    fn matches_null(
        &mut self,
        artifact: &dyn RuntimeFilterArtifactQuery,
    ) -> Result<Result<bool, RuntimeFilterArtifactQueryError>, Self::Error>;
    fn non_null_value_may_match(
        &mut self,
        artifact: &dyn RuntimeFilterArtifactQuery,
        value: RuntimeFilterScalarRef<'_>,
    ) -> Result<Result<bool, RuntimeFilterArtifactQueryError>, Self::Error>;
}
struct LegacyRowQueryPolicy;
impl RowQueryPolicy for LegacyRowQueryPolicy {
    type Error = RuntimeFilterContractViolation;
    fn before_row(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
    fn matches_null(
        &mut self,
        artifact: &dyn RuntimeFilterArtifactQuery,
    ) -> Result<Result<bool, RuntimeFilterArtifactQueryError>, Self::Error> {
        Ok(artifact.matches_null())
    }
    fn non_null_value_may_match(
        &mut self,
        artifact: &dyn RuntimeFilterArtifactQuery,
        value: RuntimeFilterScalarRef<'_>,
    ) -> Result<Result<bool, RuntimeFilterArtifactQueryError>, Self::Error> {
        Ok(artifact.non_null_value_may_match(value))
    }
}

fn evaluate_rows_with<P: RowQueryPolicy>(
    policy: &mut P,
    binding_id: RuntimeFilterBindingId,
    logical_version: LogicalVersion,
    artifact: &dyn RuntimeFilterArtifactQuery,
    input: &ArrayRef,
) -> Result<RuntimeFilterRowOutcome, P::Error> {
    if input.data_type() != artifact.data_type() {
        return Err(violation(
            "runtime-filter row input type differs from the immutable artifact type",
        )
        .into());
    }

    let data = input.to_data();
    let evaluation = if let Some(declaration) = artifact.temporal_relation_key_type() {
        let view = novarocks_value_arrays::decode::PreparedTemporalArray::bind(
            declaration,
            input.as_ref(),
            true,
        )
        .map_err(|_| violation("runtime filter temporal carrier does not match the key type"))?;
        evaluate_data(policy, artifact, input, &data, logical_version, |_, row| {
            let key = view
                .relation_key(row)
                .map_err(|_| violation("invalid temporal runtime filter key"))?
                .ok_or_else(|| violation("null reached temporal runtime filter probe"))?;
            Ok(RuntimeFilterScalarRef::Temporal(key.order_bytes()))
        })
    } else {
        match input.data_type() {
            DataType::Boolean => evaluate_data(
                policy,
                artifact,
                input,
                &data,
                logical_version,
                |data, row| Ok(RuntimeFilterScalarRef::Boolean(boolean_value(data, row)?)),
            ),
            DataType::Int8 => evaluate_data(
                policy,
                artifact,
                input,
                &data,
                logical_version,
                |data, row| {
                    Ok(RuntimeFilterScalarRef::Int8(i8::from_ne_bytes(
                        fixed_value::<1>(data, row)?,
                    )))
                },
            ),
            DataType::Int16 => evaluate_data(
                policy,
                artifact,
                input,
                &data,
                logical_version,
                |data, row| {
                    Ok(RuntimeFilterScalarRef::Int16(i16::from_ne_bytes(
                        fixed_value::<2>(data, row)?,
                    )))
                },
            ),
            DataType::Int32 => evaluate_data(
                policy,
                artifact,
                input,
                &data,
                logical_version,
                |data, row| {
                    Ok(RuntimeFilterScalarRef::Int32(i32::from_ne_bytes(
                        fixed_value::<4>(data, row)?,
                    )))
                },
            ),
            DataType::Int64 => evaluate_data(
                policy,
                artifact,
                input,
                &data,
                logical_version,
                |data, row| {
                    Ok(RuntimeFilterScalarRef::Int64(i64::from_ne_bytes(
                        fixed_value::<8>(data, row)?,
                    )))
                },
            ),
            DataType::FixedSizeBinary(width)
                if *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH =>
            {
                evaluate_data(
                    policy,
                    artifact,
                    input,
                    &data,
                    logical_version,
                    |data, row| {
                        let value =
                            novarocks_types::largeint::i128_from_be_bytes(&fixed_value::<16>(
                                data, row,
                            )?)
                            .map_err(|_| violation("invalid FixedSizeBinary(16) LargeInt value"))?;
                        Ok(RuntimeFilterScalarRef::LargeInt(value))
                    },
                )
            }
            DataType::Float32 => evaluate_data(
                policy,
                artifact,
                input,
                &data,
                logical_version,
                |data, row| {
                    Ok(RuntimeFilterScalarRef::Float32(f32::from_ne_bytes(
                        fixed_value::<4>(data, row)?,
                    )))
                },
            ),
            DataType::Float64 => evaluate_data(
                policy,
                artifact,
                input,
                &data,
                logical_version,
                |data, row| {
                    Ok(RuntimeFilterScalarRef::Float64(f64::from_ne_bytes(
                        fixed_value::<8>(data, row)?,
                    )))
                },
            ),
            DataType::Utf8 => evaluate_data(
                policy,
                artifact,
                input,
                &data,
                logical_version,
                |data, row| Ok(RuntimeFilterScalarRef::Utf8(utf8_value(data, row)?)),
            ),
            DataType::Date32 => evaluate_data(
                policy,
                artifact,
                input,
                &data,
                logical_version,
                |data, row| {
                    Ok(RuntimeFilterScalarRef::Date32(i32::from_ne_bytes(
                        fixed_value::<4>(data, row)?,
                    )))
                },
            ),
            DataType::Timestamp(TimeUnit::Second, _) => evaluate_data(
                policy,
                artifact,
                input,
                &data,
                logical_version,
                |data, row| {
                    Ok(RuntimeFilterScalarRef::TimestampSecond(i64::from_ne_bytes(
                        fixed_value::<8>(data, row)?,
                    )))
                },
            ),
            DataType::Timestamp(TimeUnit::Millisecond, _) => evaluate_data(
                policy,
                artifact,
                input,
                &data,
                logical_version,
                |data, row| {
                    Ok(RuntimeFilterScalarRef::TimestampMillisecond(
                        i64::from_ne_bytes(fixed_value::<8>(data, row)?),
                    ))
                },
            ),
            DataType::Timestamp(TimeUnit::Microsecond, _) => evaluate_data(
                policy,
                artifact,
                input,
                &data,
                logical_version,
                |data, row| {
                    Ok(RuntimeFilterScalarRef::TimestampMicrosecond(
                        i64::from_ne_bytes(fixed_value::<8>(data, row)?),
                    ))
                },
            ),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => evaluate_data(
                policy,
                artifact,
                input,
                &data,
                logical_version,
                |data, row| {
                    Ok(RuntimeFilterScalarRef::TimestampNanosecond(
                        i64::from_ne_bytes(fixed_value::<8>(data, row)?),
                    ))
                },
            ),
            DataType::Decimal128(_, _) => evaluate_data(
                policy,
                artifact,
                input,
                &data,
                logical_version,
                |data, row| {
                    Ok(RuntimeFilterScalarRef::Decimal128(i128::from_ne_bytes(
                        fixed_value::<16>(data, row)?,
                    )))
                },
            ),
            _ => Ok(RuntimeFilterRowEvaluation::NotEvaluated {
                reason: RuntimeFilterRowNotEvaluatedReason::DataTypeUnsupported,
                observed_version: logical_version,
            }),
        }
    }?;

    Ok(RuntimeFilterRowOutcome {
        binding_id,
        evaluation,
    })
}

fn evaluate_data<P: RowQueryPolicy>(
    policy: &mut P,
    artifact: &dyn RuntimeFilterArtifactQuery,
    input: &ArrayRef,
    data: &ArrayData,
    logical_version: LogicalVersion,
    scalar: impl Fn(
        &ArrayData,
        usize,
    ) -> Result<RuntimeFilterScalarRef<'_>, RuntimeFilterContractViolation>,
) -> Result<RuntimeFilterRowEvaluation, P::Error> {
    if data.data_type() != input.data_type() || data.len() != input.len() {
        return Err(violation(
            "runtime-filter Arrow array physical data differs from its declared input type or length",
        ).into());
    }
    let mut mask = Vec::new();
    if mask.try_reserve_exact(data.len()).is_err() {
        return Ok(RuntimeFilterRowEvaluation::NotEvaluated {
            reason: RuntimeFilterRowNotEvaluatedReason::ResourceUnavailable,
            observed_version: logical_version,
        });
    }
    for index in 0..data.len() {
        policy.before_row()?;
        let result = if row_is_null(data, index)? {
            policy.matches_null(artifact)?
        } else {
            policy.non_null_value_may_match(artifact, scalar(data, index)?)?
        };
        match result {
            Ok(matched) => mask.push(matched),
            Err(RuntimeFilterArtifactQueryError::Unsupported) => {
                return Ok(RuntimeFilterRowEvaluation::NotEvaluated {
                    reason: RuntimeFilterRowNotEvaluatedReason::ArtifactQueryUnsupported,
                    observed_version: logical_version,
                });
            }
            Err(RuntimeFilterArtifactQueryError::ResourceUnavailable) => {
                return Ok(RuntimeFilterRowEvaluation::NotEvaluated {
                    reason: RuntimeFilterRowNotEvaluatedReason::ResourceUnavailable,
                    observed_version: logical_version,
                });
            }
            Err(RuntimeFilterArtifactQueryError::ContractViolation) => {
                return Err(violation(
                    "runtime-filter artifact query rejected its immutable artifact",
                )
                .into());
            }
        }
    }
    Ok(RuntimeFilterRowEvaluation::Evaluated {
        mask: BooleanArray::from(mask),
        logical_version,
    })
}

pub(crate) fn row_is_null(
    data: &ArrayData,
    row: usize,
) -> Result<bool, RuntimeFilterContractViolation> {
    let physical_row = physical_row(data, row)?;
    let Some(nulls) = data.nulls() else {
        return Ok(false);
    };
    if physical_row >= nulls.len() {
        return Err(violation(
            "runtime-filter Arrow null bitmap is shorter than its declared array length",
        ));
    }
    Ok(nulls.is_null(physical_row))
}

pub(crate) fn boolean_value(
    data: &ArrayData,
    row: usize,
) -> Result<bool, RuntimeFilterContractViolation> {
    let physical_row = physical_row(data, row)?;
    let byte = *value_buffer(data)?.get(physical_row / 8).ok_or_else(|| {
        violation("runtime-filter Arrow boolean values buffer is shorter than its declared length")
    })?;
    Ok((byte & (1 << (physical_row % 8))) != 0)
}

pub(crate) fn fixed_value<const WIDTH: usize>(
    data: &ArrayData,
    row: usize,
) -> Result<[u8; WIDTH], RuntimeFilterContractViolation> {
    let start = physical_row(data, row)?
        .checked_mul(WIDTH)
        .ok_or_else(|| violation("runtime-filter Arrow values offset overflow"))?;
    let end = start
        .checked_add(WIDTH)
        .ok_or_else(|| violation("runtime-filter Arrow values range overflow"))?;
    value_buffer(data)?
        .get(start..end)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| {
            violation(
                "runtime-filter Arrow fixed-width values buffer is shorter than its declared length",
            )
        })
}

pub(crate) fn utf8_value(
    data: &ArrayData,
    row: usize,
) -> Result<&str, RuntimeFilterContractViolation> {
    if data.buffers().len() != 2 {
        return Err(violation(
            "runtime-filter Arrow UTF-8 array does not have offset and values buffers",
        ));
    }
    let physical_row = physical_row(data, row)?;
    let start = utf8_offset(data, physical_row)?;
    let end = utf8_offset(
        data,
        physical_row
            .checked_add(1)
            .ok_or_else(|| violation("runtime-filter Arrow UTF-8 offset overflow"))?,
    )?;
    if start > end {
        return Err(violation(
            "runtime-filter Arrow UTF-8 offsets are not monotonically increasing",
        ));
    }
    let bytes = data.buffers()[1]
        .as_slice()
        .get(start..end)
        .ok_or_else(|| {
            violation(
                "runtime-filter Arrow UTF-8 values buffer is shorter than its declared offsets",
            )
        })?;
    std::str::from_utf8(bytes)
        .map_err(|_| violation("runtime-filter Arrow UTF-8 values buffer contains invalid UTF-8"))
}

fn utf8_offset(data: &ArrayData, row: usize) -> Result<usize, RuntimeFilterContractViolation> {
    let start = row
        .checked_mul(std::mem::size_of::<i32>())
        .ok_or_else(|| violation("runtime-filter Arrow UTF-8 offset position overflow"))?;
    let end = start
        .checked_add(std::mem::size_of::<i32>())
        .ok_or_else(|| violation("runtime-filter Arrow UTF-8 offset range overflow"))?;
    let bytes: [u8; 4] = data
        .buffers()
        .first()
        .and_then(|buffer| buffer.as_slice().get(start..end))
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| {
            violation(
                "runtime-filter Arrow UTF-8 offsets buffer is shorter than its declared length",
            )
        })?;
    usize::try_from(i32::from_ne_bytes(bytes))
        .map_err(|_| violation("runtime-filter Arrow UTF-8 offset is negative"))
}

fn physical_row(data: &ArrayData, row: usize) -> Result<usize, RuntimeFilterContractViolation> {
    if row >= data.len() {
        return Err(violation(
            "runtime-filter Arrow row is outside the declared array length",
        ));
    }
    data.offset()
        .checked_add(row)
        .ok_or_else(|| violation("runtime-filter Arrow row offset overflow"))
}

fn value_buffer(data: &ArrayData) -> Result<&[u8], RuntimeFilterContractViolation> {
    if data.buffers().len() != 1 {
        return Err(violation(
            "runtime-filter Arrow fixed-width array does not have exactly one values buffer",
        ));
    }
    Ok(data.buffers()[0].as_slice())
}

fn violation(detail: &'static str) -> RuntimeFilterContractViolation {
    RuntimeFilterContractViolation::new(
        RuntimeFilterContractViolationKind::ContractMismatch,
        detail,
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Float64Array, Int32Array, StringArray};

    use super::*;

    struct Query {
        data_type: DataType,
        null_matches: Result<bool, RuntimeFilterArtifactQueryError>,
        value_matches: Result<bool, RuntimeFilterArtifactQueryError>,
    }

    impl RuntimeFilterArtifactQuery for Query {
        fn data_type(&self) -> &DataType {
            &self.data_type
        }

        fn matches_null(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
            self.null_matches
        }

        fn has_non_null_matches(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
            Ok(true)
        }

        fn non_null_value_may_match(
            &self,
            _: RuntimeFilterScalarRef<'_>,
        ) -> Result<bool, RuntimeFilterArtifactQueryError> {
            self.value_matches
        }

        fn non_null_range_may_match(
            &self,
            _: &ConnectorScalarValue,
            _: &ConnectorScalarValue,
        ) -> Result<bool, RuntimeFilterArtifactQueryError> {
            Ok(true)
        }
    }

    fn query(data_type: DataType) -> Query {
        Query {
            data_type,
            null_matches: Ok(true),
            value_matches: Ok(false),
        }
    }

    #[test]
    fn evaluates_nulls_and_derives_effect_only_from_evaluated_mask() {
        let input: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(2)]));
        let outcome = evaluate_rows(
            RuntimeFilterBindingId::new(7),
            LogicalVersion::new(3),
            &query(DataType::Int32),
            &input,
        )
        .unwrap();

        let RuntimeFilterRowEvaluation::Evaluated {
            mask,
            logical_version,
        } = outcome.evaluation()
        else {
            panic!("expected evaluated outcome");
        };
        assert_eq!(
            mask.values().iter().collect::<Vec<_>>(),
            vec![false, true, false]
        );
        assert_eq!(*logical_version, LogicalVersion::new(3));
        assert_eq!(
            outcome.effect(),
            Some(RuntimeFilterRowEffect {
                binding_id: RuntimeFilterBindingId::new(7),
                logical_version: LogicalVersion::new(3),
                input_rows: 3,
                output_rows: 1,
            })
        );
    }

    #[test]
    fn unsupported_or_resource_query_is_fail_open_without_an_effect() {
        let input: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        for (error, reason) in [
            (
                RuntimeFilterArtifactQueryError::Unsupported,
                RuntimeFilterRowNotEvaluatedReason::ArtifactQueryUnsupported,
            ),
            (
                RuntimeFilterArtifactQueryError::ResourceUnavailable,
                RuntimeFilterRowNotEvaluatedReason::ResourceUnavailable,
            ),
        ] {
            let mut artifact = query(DataType::Int32);
            artifact.value_matches = Err(error);
            let outcome = evaluate_rows(
                RuntimeFilterBindingId::new(1),
                LogicalVersion::new(9),
                &artifact,
                &input,
            )
            .unwrap();
            assert_eq!(
                outcome.evaluation(),
                &RuntimeFilterRowEvaluation::NotEvaluated {
                    reason,
                    observed_version: LogicalVersion::new(9),
                }
            );
            assert_eq!(outcome.effect(), None);
        }
    }

    #[test]
    fn frozen_type_mismatch_and_contract_query_failure_are_fail_fast() {
        let input: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        let mismatch = evaluate_rows(
            RuntimeFilterBindingId::new(1),
            LogicalVersion::FIRST,
            &query(DataType::Int64),
            &input,
        )
        .unwrap_err();
        assert_eq!(
            mismatch.kind(),
            RuntimeFilterContractViolationKind::ContractMismatch
        );

        let mut artifact = query(DataType::Int32);
        artifact.value_matches = Err(RuntimeFilterArtifactQueryError::ContractViolation);
        let failure = evaluate_rows(
            RuntimeFilterBindingId::new(1),
            LogicalVersion::FIRST,
            &artifact,
            &input,
        )
        .unwrap_err();
        assert_eq!(
            failure.kind(),
            RuntimeFilterContractViolationKind::ContractMismatch
        );
    }

    #[test]
    fn unsupported_arrow_type_is_typed_not_evaluated() {
        let input: ArrayRef = Arc::new(arrow::array::BinaryArray::from(vec![b"one".as_slice()]));
        let outcome = evaluate_rows(
            RuntimeFilterBindingId::new(1),
            LogicalVersion::new(4),
            &query(DataType::Binary),
            &input,
        )
        .unwrap();
        assert_eq!(
            outcome.evaluation(),
            &RuntimeFilterRowEvaluation::NotEvaluated {
                reason: RuntimeFilterRowNotEvaluatedReason::DataTypeUnsupported,
                observed_version: LogicalVersion::new(4),
            }
        );
    }

    #[test]
    fn floating_values_are_forwarded_without_lossy_conversion() {
        struct FloatQuery(Query);
        impl RuntimeFilterArtifactQuery for FloatQuery {
            fn data_type(&self) -> &DataType {
                self.0.data_type()
            }
            fn matches_null(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
                self.0.matches_null()
            }
            fn has_non_null_matches(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
                self.0.has_non_null_matches()
            }
            fn non_null_value_may_match(
                &self,
                value: RuntimeFilterScalarRef<'_>,
            ) -> Result<bool, RuntimeFilterArtifactQueryError> {
                assert!(matches!(value, RuntimeFilterScalarRef::Float64(value) if value.is_nan()));
                Ok(true)
            }
            fn non_null_range_may_match(
                &self,
                min: &ConnectorScalarValue,
                max: &ConnectorScalarValue,
            ) -> Result<bool, RuntimeFilterArtifactQueryError> {
                self.0.non_null_range_may_match(min, max)
            }
        }
        let input: ArrayRef = Arc::new(Float64Array::from(vec![f64::NAN]));
        let outcome = evaluate_rows(
            RuntimeFilterBindingId::new(1),
            LogicalVersion::FIRST,
            &FloatQuery(query(DataType::Float64)),
            &input,
        )
        .unwrap();
        assert_eq!(outcome.effect().unwrap().output_rows(), 1);
    }

    #[test]
    fn utf8_slice_uses_validated_offsets_from_array_data() {
        struct Utf8Query(Query);
        impl RuntimeFilterArtifactQuery for Utf8Query {
            fn data_type(&self) -> &DataType {
                self.0.data_type()
            }
            fn matches_null(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
                self.0.matches_null()
            }
            fn has_non_null_matches(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
                self.0.has_non_null_matches()
            }
            fn non_null_value_may_match(
                &self,
                value: RuntimeFilterScalarRef<'_>,
            ) -> Result<bool, RuntimeFilterArtifactQueryError> {
                let RuntimeFilterScalarRef::Utf8(value) = value else {
                    panic!("expected UTF-8 scalar");
                };
                Ok(value == "second")
            }
            fn non_null_range_may_match(
                &self,
                min: &ConnectorScalarValue,
                max: &ConnectorScalarValue,
            ) -> Result<bool, RuntimeFilterArtifactQueryError> {
                self.0.non_null_range_may_match(min, max)
            }
        }

        let array: ArrayRef = Arc::new(StringArray::from(vec!["first", "second", "third"]));
        let input = array.slice(1, 2);
        let outcome = evaluate_rows(
            RuntimeFilterBindingId::new(5),
            LogicalVersion::new(2),
            &Utf8Query(query(DataType::Utf8)),
            &input,
        )
        .unwrap();

        let RuntimeFilterRowEvaluation::Evaluated { mask, .. } = outcome.evaluation() else {
            panic!("expected evaluated outcome");
        };
        assert_eq!(mask.values().iter().collect::<Vec<_>>(), vec![true, false]);
    }
}
