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

//! Execution-neutral queries over Worker-retained artifacts.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::datatypes::DataType;
use novarocks_execution::runtime_filter::{
    KernelEvaluationControl, RuntimeFilterArtifactQuery, RuntimeFilterArtifactQueryError,
    RuntimeFilterControlledQueryError, RuntimeFilterControlledScalarQuery,
    RuntimeFilterMembershipSchema, RuntimeFilterNullSemantics, RuntimeFilterScalarRef,
    contribution::{
        ControlledOrderedTupleError, OrderedScalar, OrderedScalarRef, OrderedTuple,
        OrderedTupleRef, RuntimeOrderContract,
    },
    is_aggregate_topn_key_carrier,
};
use novarocks_spi::connector::ConnectorScalarValue;

use crate::runtime_filter::artifact::{
    ArtifactBundle, ArtifactKind, ConsumerArtifactProfile, PhysicalArtifact, RangeResidentData,
    ResidentMembershipIndex,
};
use crate::runtime_filter::codec::leaf::{
    ArtifactCodecError, ControlledMembershipProbeError, MembershipProbe,
    indexed_membership_contains, indexed_membership_contains_controlled,
    indexed_membership_range_may_match,
};

/// Immutable adapter over one Backend artifact bundle. It has no Arrow batch,
/// scan-unit fact, reader, provider, outcome, or Effect API.
pub enum BackendRuntimeFilterArtifactQuery {
    Membership {
        artifact: Arc<PhysicalArtifact>,
        data_type: DataType,
        null_semantics: RuntimeFilterNullSemantics,
        temporal: Option<novarocks_type_contract::TemporalType>,
    },
    Ordered {
        artifact: Arc<PhysicalArtifact>,
        data_type: DataType,
        temporal: Option<novarocks_type_contract::TemporalType>,
    },
}

enum ScalarQueryMode<'a> {
    Legacy,
    Controlled(&'a dyn KernelEvaluationControl),
}

fn legacy_query_error(error: RuntimeFilterControlledQueryError) -> RuntimeFilterArtifactQueryError {
    match error {
        RuntimeFilterControlledQueryError::Artifact(error) => error,
        RuntimeFilterControlledQueryError::Kernel(_) => {
            unreachable!("the explicit legacy scalar query cannot refuse control")
        }
    }
}

impl BackendRuntimeFilterArtifactQuery {
    pub fn membership(
        bundle: &ArtifactBundle,
        data_type: DataType,
        null_semantics: RuntimeFilterNullSemantics,
    ) -> Result<Self, RuntimeFilterArtifactQueryError> {
        let schema = RuntimeFilterMembershipSchema::new(&data_type, null_semantics)
            .map_err(|_| RuntimeFilterArtifactQueryError::ContractViolation)?;
        Self::membership_typed(bundle, &schema)
    }

    pub fn membership_typed(
        bundle: &ArtifactBundle,
        schema: &RuntimeFilterMembershipSchema,
    ) -> Result<Self, RuntimeFilterArtifactQueryError> {
        let data_type = schema.data_type();
        let null_semantics = schema.null_semantics();
        let temporal = schema
            .value_type()
            .and_then(|value_type| value_type.value_type().temporal());
        if matches!(data_type, DataType::FixedSizeBinary(10 | 12 | 14)) && temporal.is_none() {
            return Err(RuntimeFilterArtifactQueryError::ContractViolation);
        }
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .map_err(|_| RuntimeFilterArtifactQueryError::ContractViolation)?;
        if bundle.profile_id() != profile.id() {
            return Err(RuntimeFilterArtifactQueryError::ContractViolation);
        }
        let [(kind, artifact)] = bundle.artifacts() else {
            return Err(RuntimeFilterArtifactQueryError::ContractViolation);
        };
        let Some(index) = artifact.membership_index() else {
            return Err(RuntimeFilterArtifactQueryError::ContractViolation);
        };
        let empty = matches!(index, ResidentMembershipIndex::EmptyDomain);
        if !matches!(kind, ArtifactKind::ValueSet | ArtifactKind::EmptyDomain)
            || artifact.kind() != *kind
            || artifact.version() != bundle.version()
            || artifact.schema_digest().bytes() != schema.digest()
            || (artifact.contains_null()
                && null_semantics != RuntimeFilterNullSemantics::NullSafeEqual)
            || (matches!(kind, ArtifactKind::EmptyDomain) != empty)
        {
            return Err(RuntimeFilterArtifactQueryError::ContractViolation);
        }
        Ok(Self::Membership {
            artifact: Arc::clone(artifact),
            data_type: data_type.clone(),
            null_semantics,
            temporal,
        })
    }

    pub fn ordered(
        bundle: &ArtifactBundle,
        contract: &RuntimeOrderContract,
    ) -> Result<Self, RuntimeFilterArtifactQueryError> {
        if contract.keys().len() != 1 {
            return Err(RuntimeFilterArtifactQueryError::Unsupported);
        }
        let profile = ConsumerArtifactProfile::new_ordered_range(contract.digest())
            .map_err(|_| RuntimeFilterArtifactQueryError::ContractViolation)?;
        if bundle.profile_id() != profile.id() {
            return Err(RuntimeFilterArtifactQueryError::ContractViolation);
        }
        let [(kind, artifact)] = bundle.artifacts() else {
            return Err(RuntimeFilterArtifactQueryError::ContractViolation);
        };
        let range = artifact
            .range_data()
            .ok_or(RuntimeFilterArtifactQueryError::ContractViolation)?;
        if *kind != ArtifactKind::Range
            || artifact.kind() != ArtifactKind::Range
            || artifact.version() != bundle.version()
            || artifact.schema_digest().bytes() != contract.digest()
            || range.contract.as_ref() != contract
            || range.contract.compare(&range.bound, &range.bound).is_err()
        {
            return Err(RuntimeFilterArtifactQueryError::ContractViolation);
        }
        Ok(Self::Ordered {
            artifact: Arc::clone(artifact),
            data_type: contract.keys()[0].data_type().clone(),
            temporal: contract.keys()[0].temporal_type(),
        })
    }

    fn membership_matches_scalar(
        artifact: &PhysicalArtifact,
        data_type: &DataType,
        value: RuntimeFilterScalarRef<'_>,
        mode: ScalarQueryMode<'_>,
    ) -> Result<bool, RuntimeFilterControlledQueryError> {
        let index = artifact
            .membership_index()
            .ok_or(RuntimeFilterArtifactQueryError::ContractViolation)?;
        let probe = membership_probe(value, data_type)?;
        match mode {
            ScalarQueryMode::Legacy => {
                indexed_membership_contains(artifact.canonical_bytes(), index, probe)
                    .map_err(|cause| map_query_error(cause).into())
            }
            ScalarQueryMode::Controlled(control) => indexed_membership_contains_controlled(
                artifact.canonical_bytes(),
                index,
                probe,
                control,
            )
            .map_err(|cause| match cause {
                ControlledMembershipProbeError::Codec(cause) => map_query_error(cause).into(),
                ControlledMembershipProbeError::Kernel(cause) => {
                    RuntimeFilterControlledQueryError::Kernel(cause)
                }
            }),
        }
    }

    fn ordered_range(&self) -> Result<&RangeResidentData, RuntimeFilterArtifactQueryError> {
        let Self::Ordered { artifact, .. } = self else {
            return Err(RuntimeFilterArtifactQueryError::ContractViolation);
        };
        artifact
            .range_data()
            .map(AsRef::as_ref)
            .ok_or(RuntimeFilterArtifactQueryError::ContractViolation)
    }

    fn ordered_matches_tuple(
        &self,
        value: OrderedTuple,
    ) -> Result<bool, RuntimeFilterArtifactQueryError> {
        let range = self.ordered_range()?;
        Ok(range
            .contract
            .compare(&value, &range.bound)
            .map_err(|_| RuntimeFilterArtifactQueryError::ContractViolation)?
            != std::cmp::Ordering::Greater)
    }

    fn ordered_matches_scalar(
        &self,
        value: Option<OrderedScalarRef<'_>>,
        mode: ScalarQueryMode<'_>,
    ) -> Result<bool, RuntimeFilterControlledQueryError> {
        let range = self.ordered_range()?;
        let value = OrderedTupleRef::Single(value);
        let bound = OrderedTupleRef::Owned(&range.bound);
        let ordering = match mode {
            ScalarQueryMode::Legacy => {
                range.contract.compare_borrowed(value, bound).map_err(|_| {
                    RuntimeFilterControlledQueryError::Artifact(
                        RuntimeFilterArtifactQueryError::ContractViolation,
                    )
                })?
            }
            ScalarQueryMode::Controlled(control) => range
                .contract
                .compare_borrowed_controlled(value, bound, control)
                .map_err(|error| match error {
                    ControlledOrderedTupleError::Tuple(_) => {
                        RuntimeFilterControlledQueryError::Artifact(
                            RuntimeFilterArtifactQueryError::ContractViolation,
                        )
                    }
                    ControlledOrderedTupleError::Kernel(cause) => {
                        RuntimeFilterControlledQueryError::Kernel(cause)
                    }
                })?,
        };
        Ok(ordering != std::cmp::Ordering::Greater)
    }
}

impl RuntimeFilterArtifactQuery for BackendRuntimeFilterArtifactQuery {
    fn temporal_type(&self) -> Option<novarocks_type_contract::TemporalType> {
        match self {
            Self::Membership { temporal, .. } | Self::Ordered { temporal, .. } => *temporal,
        }
    }

    fn temporal_relation_key_type(&self) -> Option<novarocks_type_contract::TemporalType> {
        match self {
            Self::Ordered { temporal, .. }
            | Self::Membership {
                data_type: DataType::FixedSizeBinary(10 | 12 | 14),
                temporal,
                ..
            } => *temporal,
            Self::Membership { .. } => None,
        }
    }

    fn data_type(&self) -> &DataType {
        match self {
            Self::Membership { data_type, .. } | Self::Ordered { data_type, .. } => data_type,
        }
    }

    fn controlled_scalar_query(&self) -> Option<&dyn RuntimeFilterControlledScalarQuery> {
        match self {
            Self::Membership { .. } => Some(self),
            Self::Ordered {
                data_type,
                temporal,
                ..
            } if temporal.is_some() || is_aggregate_topn_key_carrier(data_type) => Some(self),
            _ => None,
        }
    }

    fn matches_null(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
        match self {
            Self::Membership {
                artifact,
                null_semantics,
                ..
            } => Ok(membership_matches_null(artifact, *null_semantics)),
            Self::Ordered { .. } => self
                .ordered_matches_scalar(None, ScalarQueryMode::Legacy)
                .map_err(legacy_query_error),
        }
    }

    fn has_non_null_matches(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
        match self {
            Self::Membership { artifact, .. } => Ok(!matches!(
                artifact.membership_index(),
                Some(ResidentMembershipIndex::EmptyDomain)
            )),
            Self::Ordered { .. } => Ok(true),
        }
    }

    fn non_null_value_may_match(
        &self,
        value: RuntimeFilterScalarRef<'_>,
    ) -> Result<bool, RuntimeFilterArtifactQueryError> {
        match self {
            Self::Membership {
                artifact,
                data_type,
                ..
            } => {
                Self::membership_matches_scalar(artifact, data_type, value, ScalarQueryMode::Legacy)
                    .map_err(legacy_query_error)
            }
            Self::Ordered {
                data_type,
                temporal,
                ..
            } => {
                self.ordered_range()?;
                let scalar = ordered_scalar_ref(value, data_type, *temporal)?;
                self.ordered_matches_scalar(Some(scalar), ScalarQueryMode::Legacy)
                    .map_err(legacy_query_error)
            }
        }
    }

    fn non_null_range_may_match(
        &self,
        inclusive_min: &ConnectorScalarValue,
        inclusive_max: &ConnectorScalarValue,
    ) -> Result<bool, RuntimeFilterArtifactQueryError> {
        match self {
            Self::Membership {
                artifact,
                data_type,
                ..
            } => indexed_membership_range_may_match(
                artifact.canonical_bytes(),
                artifact
                    .membership_index()
                    .ok_or(RuntimeFilterArtifactQueryError::ContractViolation)?,
                connector_membership_probe(inclusive_min, data_type)?,
                connector_membership_probe(inclusive_max, data_type)?,
            )
            .map_err(map_query_error),
            Self::Ordered {
                temporal: Some(_), ..
            } => Err(RuntimeFilterArtifactQueryError::Unsupported),
            Self::Ordered { data_type, .. } => {
                let range = self.ordered_range()?;
                let min = OrderedTuple::try_new(
                    &range.contract,
                    [Some(connector_ordered_scalar(inclusive_min, data_type)?)],
                )
                .map_err(|_| RuntimeFilterArtifactQueryError::ContractViolation)?;
                let max = OrderedTuple::try_new(
                    &range.contract,
                    [Some(connector_ordered_scalar(inclusive_max, data_type)?)],
                )
                .map_err(|_| RuntimeFilterArtifactQueryError::ContractViolation)?;
                Ok(self.ordered_matches_tuple(min)? || self.ordered_matches_tuple(max)?)
            }
        }
    }
}

impl RuntimeFilterControlledScalarQuery for BackendRuntimeFilterArtifactQuery {
    fn matches_null_controlled(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<bool, RuntimeFilterControlledQueryError> {
        control.checkpoint(0)?;
        match self {
            Self::Membership {
                artifact,
                null_semantics,
                ..
            } => Ok(membership_matches_null(artifact, *null_semantics)),
            Self::Ordered {
                data_type,
                temporal,
                ..
            } if temporal.is_some() || is_aggregate_topn_key_carrier(data_type) => {
                self.ordered_matches_scalar(None, ScalarQueryMode::Controlled(control))
            }
            _ => Err(RuntimeFilterArtifactQueryError::Unsupported.into()),
        }
    }

    fn non_null_value_may_match_controlled(
        &self,
        value: RuntimeFilterScalarRef<'_>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<bool, RuntimeFilterControlledQueryError> {
        control.checkpoint(0)?;
        match self {
            Self::Membership {
                artifact,
                data_type,
                ..
            } => Self::membership_matches_scalar(
                artifact,
                data_type,
                value,
                ScalarQueryMode::Controlled(control),
            ),
            Self::Ordered {
                data_type,
                temporal,
                ..
            } if temporal.is_some() || is_aggregate_topn_key_carrier(data_type) => {
                self.ordered_range()?;
                let scalar = ordered_scalar_ref(value, data_type, *temporal)?;
                self.ordered_matches_scalar(Some(scalar), ScalarQueryMode::Controlled(control))
            }
            _ => Err(RuntimeFilterArtifactQueryError::Unsupported.into()),
        }
    }
}

fn membership_matches_null(
    artifact: &PhysicalArtifact,
    null_semantics: RuntimeFilterNullSemantics,
) -> bool {
    null_semantics == RuntimeFilterNullSemantics::NullSafeEqual && artifact.contains_null()
}

fn map_query_error(error: ArtifactCodecError) -> RuntimeFilterArtifactQueryError {
    match error {
        ArtifactCodecError::ResourceLimit => RuntimeFilterArtifactQueryError::ResourceUnavailable,
        ArtifactCodecError::ContractViolation
        | ArtifactCodecError::Malformed
        | ArtifactCodecError::Truncated
        | ArtifactCodecError::InvalidLogicalVersion
        | ArtifactCodecError::UnknownVersion
        | ArtifactCodecError::UnknownKind
        | ArtifactCodecError::InvalidFlags
        | ArtifactCodecError::InvalidHashContract
        | ArtifactCodecError::KindMismatch
        | ArtifactCodecError::SchemaMismatch
        | ArtifactCodecError::VersionMismatch
        | ArtifactCodecError::HashContractMismatch
        | ArtifactCodecError::LengthOverflow
        | ArtifactCodecError::TrailingBytes
        | ArtifactCodecError::NonCanonicalPayload
        | ArtifactCodecError::EncodedSizeExceeded => {
            RuntimeFilterArtifactQueryError::ContractViolation
        }
    }
}

fn membership_probe<'a>(
    value: RuntimeFilterScalarRef<'a>,
    expected: &DataType,
) -> Result<MembershipProbe<'a>, RuntimeFilterArtifactQueryError> {
    match (value, expected) {
        (RuntimeFilterScalarRef::Temporal(value), DataType::FixedSizeBinary(10 | 12 | 14)) => {
            Ok(MembershipProbe::Temporal(value))
        }
        (RuntimeFilterScalarRef::Boolean(value), DataType::Boolean) => {
            Ok(MembershipProbe::Boolean(value))
        }
        (RuntimeFilterScalarRef::Int8(value), DataType::Int8) => Ok(MembershipProbe::Int8(value)),
        (RuntimeFilterScalarRef::Int16(value), DataType::Int16) => {
            Ok(MembershipProbe::Int16(value))
        }
        (RuntimeFilterScalarRef::Int32(value), DataType::Int32) => {
            Ok(MembershipProbe::Int32(value))
        }
        (RuntimeFilterScalarRef::Int64(value), DataType::Int64) => {
            Ok(MembershipProbe::Int64(value))
        }
        (RuntimeFilterScalarRef::LargeInt(value), DataType::FixedSizeBinary(16)) => {
            Ok(MembershipProbe::LargeInt(value))
        }
        (RuntimeFilterScalarRef::Float32(value), DataType::Float32) => {
            Ok(MembershipProbe::Float32(value))
        }
        (RuntimeFilterScalarRef::Float64(value), DataType::Float64) => {
            Ok(MembershipProbe::Float64(value))
        }
        (RuntimeFilterScalarRef::Utf8(value), DataType::Utf8) => Ok(MembershipProbe::Utf8(value)),
        (RuntimeFilterScalarRef::Date32(value), DataType::Date32) => {
            Ok(MembershipProbe::Date32(value))
        }
        (
            RuntimeFilterScalarRef::TimestampSecond(value),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None),
        )
        | (
            RuntimeFilterScalarRef::TimestampMillisecond(value),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
        )
        | (
            RuntimeFilterScalarRef::TimestampMicrosecond(value),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        )
        | (
            RuntimeFilterScalarRef::TimestampNanosecond(value),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
        ) => Ok(MembershipProbe::Timestamp(value)),
        (RuntimeFilterScalarRef::Decimal128(value), DataType::Decimal128(_, _)) => {
            Ok(MembershipProbe::Decimal128(value))
        }
        _ => Err(RuntimeFilterArtifactQueryError::Unsupported),
    }
}

fn connector_membership_probe<'a>(
    value: &'a ConnectorScalarValue,
    expected: &DataType,
) -> Result<MembershipProbe<'a>, RuntimeFilterArtifactQueryError> {
    match (value, expected) {
        (ConnectorScalarValue::Boolean(value), DataType::Boolean) => {
            Ok(MembershipProbe::Boolean(*value))
        }
        (ConnectorScalarValue::Int8(value), DataType::Int8) => Ok(MembershipProbe::Int8(*value)),
        (ConnectorScalarValue::Int16(value), DataType::Int16) => Ok(MembershipProbe::Int16(*value)),
        (ConnectorScalarValue::Int32(value), DataType::Int32) => Ok(MembershipProbe::Int32(*value)),
        (ConnectorScalarValue::Int64(value), DataType::Int64) => Ok(MembershipProbe::Int64(*value)),
        (ConnectorScalarValue::Date32(value), DataType::Date32) => {
            Ok(MembershipProbe::Date32(*value))
        }
        (
            ConnectorScalarValue::TimestampMillis(value),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
        )
        | (
            ConnectorScalarValue::TimestampMicros(value),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        )
        | (
            ConnectorScalarValue::TimestampNanos(value),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
        ) => Ok(MembershipProbe::Timestamp(*value)),
        (ConnectorScalarValue::Utf8(value), DataType::Utf8) => Ok(MembershipProbe::Utf8(value)),
        _ => Err(RuntimeFilterArtifactQueryError::Unsupported),
    }
}

fn ordered_scalar_ref<'a>(
    value: RuntimeFilterScalarRef<'a>,
    expected: &DataType,
    temporal: Option<novarocks_type_contract::TemporalType>,
) -> Result<OrderedScalarRef<'a>, RuntimeFilterArtifactQueryError> {
    if let Some(temporal) = temporal {
        let RuntimeFilterScalarRef::Temporal(bytes) = value else {
            return Err(RuntimeFilterArtifactQueryError::ContractViolation);
        };
        novarocks_type_contract::TemporalRelationKey::from_order_bytes(temporal, bytes)
            .map_err(|_| RuntimeFilterArtifactQueryError::ContractViolation)?;
        return Ok(OrderedScalarRef::Temporal(bytes));
    }
    Ok(match (value, expected) {
        (RuntimeFilterScalarRef::Boolean(value), DataType::Boolean) => {
            OrderedScalarRef::Boolean(value)
        }
        (RuntimeFilterScalarRef::Int8(value), DataType::Int8) => OrderedScalarRef::Int8(value),
        (RuntimeFilterScalarRef::Int16(value), DataType::Int16) => OrderedScalarRef::Int16(value),
        (RuntimeFilterScalarRef::Int32(value), DataType::Int32) => OrderedScalarRef::Int32(value),
        (RuntimeFilterScalarRef::Int64(value), DataType::Int64) => OrderedScalarRef::Int64(value),
        (RuntimeFilterScalarRef::LargeInt(value), DataType::FixedSizeBinary(16)) => {
            OrderedScalarRef::LargeInt(value)
        }
        (RuntimeFilterScalarRef::Utf8(value), DataType::Utf8) => OrderedScalarRef::Utf8(value),
        (RuntimeFilterScalarRef::Date32(value), DataType::Date32) => {
            OrderedScalarRef::Date32(value)
        }
        (
            RuntimeFilterScalarRef::TimestampSecond(value),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None),
        )
        | (
            RuntimeFilterScalarRef::TimestampMillisecond(value),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
        )
        | (
            RuntimeFilterScalarRef::TimestampMicrosecond(value),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        )
        | (
            RuntimeFilterScalarRef::TimestampNanosecond(value),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
        ) => OrderedScalarRef::Timestamp(value),
        (RuntimeFilterScalarRef::Decimal128(value), DataType::Decimal128(_, _)) => {
            OrderedScalarRef::Decimal128(value)
        }
        _ => return Err(RuntimeFilterArtifactQueryError::Unsupported),
    })
}

fn connector_ordered_scalar(
    value: &ConnectorScalarValue,
    expected: &DataType,
) -> Result<OrderedScalar, RuntimeFilterArtifactQueryError> {
    match (value, expected) {
        (ConnectorScalarValue::Boolean(value), DataType::Boolean) => {
            Ok(OrderedScalar::Boolean(*value))
        }
        (ConnectorScalarValue::Int8(value), DataType::Int8) => Ok(OrderedScalar::Int8(*value)),
        (ConnectorScalarValue::Int16(value), DataType::Int16) => Ok(OrderedScalar::Int16(*value)),
        (ConnectorScalarValue::Int32(value), DataType::Int32) => Ok(OrderedScalar::Int32(*value)),
        (ConnectorScalarValue::Int64(value), DataType::Int64) => Ok(OrderedScalar::Int64(*value)),
        (ConnectorScalarValue::Date32(value), DataType::Date32) => {
            Ok(OrderedScalar::Date32(*value))
        }
        (
            ConnectorScalarValue::TimestampMillis(value),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
        )
        | (
            ConnectorScalarValue::TimestampMicros(value),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        )
        | (
            ConnectorScalarValue::TimestampNanos(value),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
        ) => Ok(OrderedScalar::Timestamp(*value)),
        (ConnectorScalarValue::Utf8(value), DataType::Utf8) => {
            Ok(OrderedScalar::Utf8(value.clone()))
        }
        _ => Err(RuntimeFilterArtifactQueryError::Unsupported),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_filter::materializer::range::{decode_range_leaf, encode_range_leaf};

    struct NoControl;
    impl KernelEvaluationControl for NoControl {
        fn checkpoint(
            &self,
            _: u32,
        ) -> Result<(), novarocks_execution::runtime_filter::KernelFailure> {
            Ok(())
        }
        fn wait(
            &self,
            _: std::time::Duration,
        ) -> Result<(), novarocks_execution::runtime_filter::KernelFailure> {
            panic!("ordered artifact query never waits")
        }
    }
    use novarocks_execution::runtime_filter::LogicalVersion;
    use novarocks_execution::runtime_filter::contribution::{
        RuntimeOrderKey, RuntimeOrderNullOrder, RuntimeOrderSortDirection,
    };

    fn ordered_query(
        data_type: DataType,
        direction: RuntimeOrderSortDirection,
        null_order: RuntimeOrderNullOrder,
        bound: Option<OrderedScalar>,
    ) -> BackendRuntimeFilterArtifactQuery {
        let contract = RuntimeOrderContract::from_frozen(
            [RuntimeOrderKey::with_order(
                data_type.clone(),
                direction,
                null_order,
            )],
            [3; 32],
            [7; 32],
        );
        let bytes = encode_range_leaf(
            &contract,
            &OrderedTuple::new([bound]),
            LogicalVersion::FIRST,
        )
        .unwrap();
        let (artifact, _) =
            decode_range_leaf(&bytes, &contract, LogicalVersion::FIRST, bytes.len()).unwrap();
        BackendRuntimeFilterArtifactQuery::Ordered {
            artifact,
            data_type,
            temporal: None,
        }
    }

    #[test]
    fn temporal_ordered_artifact_probes_exact_instants_under_control() {
        use arrow::array::{ArrayRef, FixedSizeBinaryBuilder};
        use novarocks_execution::runtime_filter::{
            KernelFailure, RuntimeFilterBindingId, RuntimeFilterRowEvaluation,
            evaluator::evaluate_rows,
        };
        use novarocks_local_program::{
            FilterMembershipSchemaError, FilterNullOrder, FilterSortDirection,
            single_key_ordered_filter_digests_observed, visit_ordered_filter_key_type,
        };
        use novarocks_type_contract::{
            FunctionValueType, TemporalCarrier, TemporalMicros, TemporalPrecision, TemporalType,
            TemporalValue, TimeZoneKey, ValueKind, ValueLayout, ValueTypeRef,
            ValueTypeStorageLifetime,
        };

        let ty = TemporalType::TimestampWithTimeZone(TemporalPrecision::PICOSECONDS);
        let binding = |ty: TemporalType| {
            let storage = ValueTypeStorageLifetime::pure();
            FunctionValueType::try_from_value(
                ValueTypeRef::try_new(ValueKind::Temporal(ty), true, storage.clone()).unwrap(),
                ValueLayout::try_new(&ty.carrier(), storage).unwrap(),
            )
            .unwrap()
        };
        let digests = |ty: TemporalType| {
            single_key_ordered_filter_digests_observed::<FilterMembershipSchemaError>(
                FilterSortDirection::Ascending,
                FilterNullOrder::Last,
                |emit| visit_ordered_filter_key_type(&ty.carrier(), Some(ty), emit),
                || Ok(()),
            )
            .unwrap()
        };
        let (comparator, digest) = digests(ty);
        let wrong = TemporalType::TimestampWithTimeZone(TemporalPrecision::NANOSECONDS);
        assert_eq!(wrong.carrier(), ty.carrier());
        assert_ne!(digests(wrong), (comparator, digest));
        let key = |ty| {
            RuntimeOrderKey::with_value_type(
                binding(ty),
                RuntimeOrderSortDirection::Ascending,
                RuntimeOrderNullOrder::Last,
            )
        };
        let contract =
            RuntimeOrderContract::from_fragment_contract([key(ty)], comparator, digest).unwrap();
        assert!(
            RuntimeOrderContract::from_fragment_contract([key(wrong)], comparator, digest).is_err()
        );

        let value = |fraction, zone| TemporalValue::TimestampWithTimeZone {
            instant: TemporalMicros::new(-1, fraction).unwrap(),
            zone,
        };
        let utc = TimeZoneKey::UTC;
        let same = TimeZoneKey::parse("Asia/Shanghai").unwrap();
        let boundary = value(1, utc).relation_key(ty).unwrap().order_bytes();
        let bound = OrderedTuple::new([Some(OrderedScalar::Temporal(boundary))]);
        let encoded = encode_range_leaf(&contract, &bound, LogicalVersion::FIRST).unwrap();
        let (artifact, _) =
            decode_range_leaf(&encoded, &contract, LogicalVersion::FIRST, encoded.len()).unwrap();
        let foreign = RuntimeOrderContract::from_frozen([key(wrong)], comparator, digest);
        assert!(matches!(
            decode_range_leaf(&encoded, &foreign, LogicalVersion::FIRST, encoded.len()),
            Err(ArtifactCodecError::ContractViolation)
        ));
        let query = BackendRuntimeFilterArtifactQuery::Ordered {
            artifact,
            data_type: ty.carrier(),
            temporal: Some(ty),
        };
        assert_eq!(query.temporal_type(), Some(ty));
        let controlled = query.controlled_scalar_query().unwrap();
        assert_eq!(
            controlled.non_null_value_may_match_controlled(
                RuntimeFilterScalarRef::Temporal(
                    value(1, same).relation_key(ty).unwrap().order_bytes()
                ),
                &NoControl,
            ),
            Ok(true)
        );
        assert_eq!(
            controlled.non_null_value_may_match_controlled(
                RuntimeFilterScalarRef::Temporal(
                    value(2, same).relation_key(ty).unwrap().order_bytes()
                ),
                &NoControl,
            ),
            Ok(false)
        );
        let mut corrupt = boundary;
        corrupt[8..].fill(255);
        assert_eq!(
            controlled.non_null_value_may_match_controlled(
                RuntimeFilterScalarRef::Temporal(corrupt),
                &NoControl
            ),
            Err(RuntimeFilterControlledQueryError::Artifact(
                RuntimeFilterArtifactQueryError::ContractViolation
            ))
        );

        struct Cancelled;
        impl KernelEvaluationControl for Cancelled {
            fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
                Err(KernelFailure::Cancelled)
            }
            fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
                panic!("ordered artifact query never waits")
            }
        }
        assert_eq!(
            controlled.non_null_value_may_match_controlled(
                RuntimeFilterScalarRef::Temporal(boundary),
                &Cancelled,
            ),
            Err(RuntimeFilterControlledQueryError::Kernel(
                KernelFailure::Cancelled
            ))
        );

        let DataType::FixedSizeBinary(width) = ty.carrier() else {
            panic!("zoned timestamp carrier")
        };
        let mut rows = FixedSizeBinaryBuilder::with_capacity(4, width);
        for (fraction, zone) in [(1, same), (2, utc), (0, same)] {
            let TemporalCarrier::ZonedTimestamp(bytes) =
                value(fraction, zone).to_carrier(ty).unwrap()
            else {
                panic!("zoned timestamp value")
            };
            rows.append_value(bytes).unwrap();
        }
        rows.append_null();
        let rows: ArrayRef = Arc::new(rows.finish());
        let outcome = evaluate_rows(
            RuntimeFilterBindingId::new(1),
            LogicalVersion::FIRST,
            &query,
            &rows,
        )
        .unwrap();
        let RuntimeFilterRowEvaluation::Evaluated { mask, .. } = outcome.evaluation() else {
            panic!("typed temporal filter must evaluate");
        };
        assert_eq!(
            mask.iter().collect::<Vec<_>>(),
            [Some(true), Some(false), Some(true), Some(false)]
        );
    }

    /// Frozen row-query oracle before extraction: actual range codec input,
    /// independent std byte order, no service/thread or resource qualification.
    #[test]
    fn ordered_query_legacy_scalar_contract_oracle() {
        let values = [
            None,
            Some(""),
            Some("a"),
            Some("m"),
            Some("z"),
            Some("é"),
            Some("海港"),
        ];
        for direction in [
            RuntimeOrderSortDirection::Ascending,
            RuntimeOrderSortDirection::Descending,
        ] {
            for null_order in [RuntimeOrderNullOrder::First, RuntimeOrderNullOrder::Last] {
                for bound in [None, Some("m"), Some("é")] {
                    let query = ordered_query(
                        DataType::Utf8,
                        direction,
                        null_order,
                        bound.map(|s| OrderedScalar::Utf8(s.into())),
                    );
                    for value in values {
                        let ordering = match (value, bound) {
                            (None, None) => std::cmp::Ordering::Equal,
                            (None, Some(_)) => {
                                if null_order == RuntimeOrderNullOrder::First {
                                    std::cmp::Ordering::Less
                                } else {
                                    std::cmp::Ordering::Greater
                                }
                            }
                            (Some(_), None) => {
                                if null_order == RuntimeOrderNullOrder::First {
                                    std::cmp::Ordering::Greater
                                } else {
                                    std::cmp::Ordering::Less
                                }
                            }
                            (Some(value), Some(bound)) => {
                                let ordering = value.as_bytes().cmp(bound.as_bytes());
                                if direction == RuntimeOrderSortDirection::Descending {
                                    ordering.reverse()
                                } else {
                                    ordering
                                }
                            }
                        };
                        let result = match value {
                            None => query.matches_null(),
                            Some(value) => {
                                query.non_null_value_may_match(RuntimeFilterScalarRef::Utf8(value))
                            }
                        };
                        assert_eq!(result, Ok(ordering != std::cmp::Ordering::Greater));
                    }
                    assert_eq!(
                        query.non_null_value_may_match(RuntimeFilterScalarRef::Int64(0)),
                        Err(RuntimeFilterArtifactQueryError::Unsupported)
                    );
                }
            }
        }
        // Preserve the old query's narrower timezone domain independently
        // of the comparator contract, which accepts frozen Timestamp metadata.
        let query = ordered_query(
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
            RuntimeOrderSortDirection::Ascending,
            RuntimeOrderNullOrder::Last,
            Some(OrderedScalar::Timestamp(7)),
        );
        assert_eq!(
            query.non_null_value_may_match(RuntimeFilterScalarRef::TimestampMicrosecond(7)),
            Err(RuntimeFilterArtifactQueryError::Unsupported)
        );
    }

    #[test]
    fn ordered_query_controlled_scalar_preserves_inclusive_null_and_direction_contract() {
        for (direction, null_order, expected_null, expected_a, expected_z) in [
            (
                RuntimeOrderSortDirection::Ascending,
                RuntimeOrderNullOrder::First,
                true,
                true,
                false,
            ),
            (
                RuntimeOrderSortDirection::Ascending,
                RuntimeOrderNullOrder::Last,
                false,
                true,
                false,
            ),
            (
                RuntimeOrderSortDirection::Descending,
                RuntimeOrderNullOrder::First,
                true,
                false,
                true,
            ),
            (
                RuntimeOrderSortDirection::Descending,
                RuntimeOrderNullOrder::Last,
                false,
                false,
                true,
            ),
        ] {
            let query = ordered_query(
                DataType::Utf8,
                direction,
                null_order,
                Some(OrderedScalar::Utf8("m".into())),
            );
            let controlled = query
                .controlled_scalar_query()
                .expect("the Utf8 port is installed");
            assert_eq!(
                controlled.matches_null_controlled(&NoControl),
                Ok(expected_null)
            );
            for (value, expected) in [("a", expected_a), ("m", true), ("z", expected_z)] {
                assert_eq!(
                    controlled.non_null_value_may_match_controlled(
                        RuntimeFilterScalarRef::Utf8(value),
                        &NoControl
                    ),
                    Ok(expected)
                );
                assert_eq!(
                    query.non_null_value_may_match(RuntimeFilterScalarRef::Utf8(value)),
                    Ok(expected)
                );
            }
            assert_eq!(
                controlled.non_null_value_may_match_controlled(
                    RuntimeFilterScalarRef::Int64(0),
                    &NoControl
                ),
                Err(RuntimeFilterControlledQueryError::Artifact(
                    RuntimeFilterArtifactQueryError::Unsupported
                ))
            );
        }
        let query = ordered_query(
            DataType::Int64,
            RuntimeOrderSortDirection::Ascending,
            RuntimeOrderNullOrder::Last,
            Some(OrderedScalar::Int64(7)),
        );
        assert_eq!(
            query
                .controlled_scalar_query()
                .unwrap()
                .non_null_value_may_match_controlled(RuntimeFilterScalarRef::Int64(7), &NoControl),
            Ok(true)
        );
        assert_eq!(
            query.non_null_value_may_match(RuntimeFilterScalarRef::Int64(7)),
            Ok(true)
        );
    }

    // Existing numeric legacy queries are an additional comparison, while
    // expected inclusive/NULL order comes from std integers and declared order.
    // No service/transport is created; this Worker owner tests its real adapter.
    #[test]
    fn ordered_query_controlled_numeric_matches_literal_order_and_typed_refusals() {
        type Make = fn(i64) -> (OrderedScalar, RuntimeFilterScalarRef<'static>);
        let cases: Vec<(DataType, Make)> = vec![
            (DataType::Int8, |v| {
                (
                    OrderedScalar::Int8(v as i8),
                    RuntimeFilterScalarRef::Int8(v as i8),
                )
            }),
            (DataType::Int16, |v| {
                (
                    OrderedScalar::Int16(v as i16),
                    RuntimeFilterScalarRef::Int16(v as i16),
                )
            }),
            (DataType::Int32, |v| {
                (
                    OrderedScalar::Int32(v as i32),
                    RuntimeFilterScalarRef::Int32(v as i32),
                )
            }),
            (DataType::Int64, |v| {
                (OrderedScalar::Int64(v), RuntimeFilterScalarRef::Int64(v))
            }),
            (DataType::FixedSizeBinary(16), |v| {
                (
                    OrderedScalar::LargeInt(v.into()),
                    RuntimeFilterScalarRef::LargeInt(v.into()),
                )
            }),
            (DataType::Date32, |v| {
                (
                    OrderedScalar::Date32(v as i32),
                    RuntimeFilterScalarRef::Date32(v as i32),
                )
            }),
            (DataType::Decimal128(18, 3), |v| {
                (
                    OrderedScalar::Decimal128(v.into()),
                    RuntimeFilterScalarRef::Decimal128(v.into()),
                )
            }),
            (
                DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None),
                |v| {
                    (
                        OrderedScalar::Timestamp(v),
                        RuntimeFilterScalarRef::TimestampSecond(v),
                    )
                },
            ),
            (
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                |v| {
                    (
                        OrderedScalar::Timestamp(v),
                        RuntimeFilterScalarRef::TimestampMillisecond(v),
                    )
                },
            ),
            (
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                |v| {
                    (
                        OrderedScalar::Timestamp(v),
                        RuntimeFilterScalarRef::TimestampMicrosecond(v),
                    )
                },
            ),
            (
                DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                |v| {
                    (
                        OrderedScalar::Timestamp(v),
                        RuntimeFilterScalarRef::TimestampNanosecond(v),
                    )
                },
            ),
        ];
        struct Refuse(novarocks_execution::runtime_filter::KernelFailure);
        impl KernelEvaluationControl for Refuse {
            fn checkpoint(
                &self,
                _: u32,
            ) -> Result<(), novarocks_execution::runtime_filter::KernelFailure> {
                Err(self.0.clone())
            }
            fn wait(
                &self,
                _: std::time::Duration,
            ) -> Result<(), novarocks_execution::runtime_filter::KernelFailure> {
                panic!("scalar query never waits")
            }
        }
        for (ty, make) in cases {
            for direction in [
                RuntimeOrderSortDirection::Ascending,
                RuntimeOrderSortDirection::Descending,
            ] {
                for nulls in [RuntimeOrderNullOrder::First, RuntimeOrderNullOrder::Last] {
                    for bound in [None, Some(7i64)] {
                        let query =
                            ordered_query(ty.clone(), direction, nulls, bound.map(|v| make(v).0));
                        let port = query
                            .controlled_scalar_query()
                            .expect("numeric controlled port installed");
                        for value in [None, Some(-7i64), Some(7), Some(99)] {
                            let order = match (value, bound) {
                                (None, None) => std::cmp::Ordering::Equal,
                                (None, Some(_)) => {
                                    if nulls == RuntimeOrderNullOrder::First {
                                        std::cmp::Ordering::Less
                                    } else {
                                        std::cmp::Ordering::Greater
                                    }
                                }
                                (Some(_), None) => {
                                    if nulls == RuntimeOrderNullOrder::First {
                                        std::cmp::Ordering::Greater
                                    } else {
                                        std::cmp::Ordering::Less
                                    }
                                }
                                (Some(a), Some(b)) => {
                                    if direction == RuntimeOrderSortDirection::Ascending {
                                        a.cmp(&b)
                                    } else {
                                        b.cmp(&a)
                                    }
                                }
                            };
                            let expected = order != std::cmp::Ordering::Greater;
                            match value {
                                None => {
                                    assert_eq!(
                                        port.matches_null_controlled(&NoControl),
                                        Ok(expected)
                                    );
                                    assert_eq!(query.matches_null(), Ok(expected));
                                }
                                Some(v) => {
                                    assert_eq!(
                                        port.non_null_value_may_match_controlled(
                                            make(v).1,
                                            &NoControl
                                        ),
                                        Ok(expected)
                                    );
                                    assert_eq!(
                                        query.non_null_value_may_match(make(v).1),
                                        Ok(order != std::cmp::Ordering::Greater)
                                    );
                                }
                            }
                        }
                        for cause in [
                            novarocks_execution::runtime_filter::KernelFailure::Cancelled,
                            novarocks_execution::runtime_filter::KernelFailure::DeadlineExceeded,
                            novarocks_execution::runtime_filter::KernelFailure::ResourceExhausted,
                        ] {
                            assert_eq!(
                                port.matches_null_controlled(&Refuse(cause.clone())),
                                Err(RuntimeFilterControlledQueryError::Kernel(cause.clone()))
                            );
                            assert_eq!(
                                port.non_null_value_may_match_controlled(
                                    make(7).1,
                                    &Refuse(cause.clone())
                                ),
                                Err(RuntimeFilterControlledQueryError::Kernel(cause))
                            );
                        }
                    }
                }
            }
        }
    }

    /// Existing control port, deterministic units, no wall-clock or private
    /// controller. The Worker must forward the original cause from mid-value.
    #[test]
    fn ordered_query_controlled_scalar_forwards_long_prefix_and_null_refusals() {
        use novarocks_execution::runtime_filter::KernelFailure;
        use std::sync::atomic::{AtomicU32, Ordering};
        struct Control {
            units: AtomicU32,
            cause: KernelFailure,
            refuse_null: bool,
        }
        impl KernelEvaluationControl for Control {
            fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
                assert!(units <= 256);
                let total = self.units.fetch_add(units, Ordering::Relaxed) + units;
                if self.refuse_null || total >= 512 {
                    Err(self.cause.clone())
                } else {
                    Ok(())
                }
            }
            fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
                panic!("ordered artifact query never waits")
            }
        }
        let value = "x".repeat(1 << 20);
        let query = ordered_query(
            DataType::Utf8,
            RuntimeOrderSortDirection::Ascending,
            RuntimeOrderNullOrder::Last,
            Some(OrderedScalar::Utf8(value.clone())),
        );
        let controlled = query.controlled_scalar_query().unwrap();
        for cause in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
        ] {
            let control = Control {
                units: AtomicU32::new(0),
                cause: cause.clone(),
                refuse_null: false,
            };
            assert_eq!(
                controlled.non_null_value_may_match_controlled(
                    RuntimeFilterScalarRef::Utf8(&value),
                    &control
                ),
                Err(RuntimeFilterControlledQueryError::Kernel(cause.clone()))
            );
            let units = control.units.load(Ordering::Relaxed);
            assert!((512..1024).contains(&units));
            let control = Control {
                units: AtomicU32::new(0),
                cause: cause.clone(),
                refuse_null: true,
            };
            assert_eq!(
                controlled.matches_null_controlled(&control),
                Err(RuntimeFilterControlledQueryError::Kernel(cause))
            );
        }
    }
}

#[cfg(test)]
#[path = "controlled_membership_tests.rs"]
mod controlled_membership_tests;
