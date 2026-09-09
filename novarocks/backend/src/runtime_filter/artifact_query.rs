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

//! Execution-neutral queries over Backend-retained artifacts.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::datatypes::DataType;
use novarocks_execution::runtime_filter::{
    RuntimeFilterArtifactQuery, RuntimeFilterArtifactQueryError, RuntimeFilterMembershipSchema,
    RuntimeFilterNullSemantics, RuntimeFilterScalarRef,
    contribution::{OrderedScalar, OrderedTuple, RuntimeOrderContract},
};
use novarocks_spi::connector::ConnectorScalarValue;

use crate::runtime_filter::artifact::{
    ArtifactBundle, ArtifactKind, ConsumerArtifactProfile, PhysicalArtifact,
    ResidentMembershipIndex,
};
use crate::runtime_filter::codec::leaf::{
    ArtifactCodecError, MembershipProbe, indexed_membership_contains,
    indexed_membership_range_may_match,
};

/// Immutable adapter over one Backend artifact bundle. It has no Arrow batch,
/// scan-unit fact, reader, provider, outcome, or Effect API.
pub(crate) enum BackendRuntimeFilterArtifactQuery {
    Membership {
        artifact: Arc<PhysicalArtifact>,
        data_type: DataType,
        null_semantics: RuntimeFilterNullSemantics,
    },
    Ordered {
        artifact: Arc<PhysicalArtifact>,
        data_type: DataType,
    },
}

impl BackendRuntimeFilterArtifactQuery {
    pub(crate) fn membership(
        bundle: &ArtifactBundle,
        data_type: DataType,
        null_semantics: RuntimeFilterNullSemantics,
    ) -> Result<Self, RuntimeFilterArtifactQueryError> {
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
        let schema = RuntimeFilterMembershipSchema::new(&data_type, null_semantics)
            .map_err(|_| RuntimeFilterArtifactQueryError::ContractViolation)?;
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
            data_type,
            null_semantics,
        })
    }

    pub(crate) fn ordered(
        bundle: &ArtifactBundle,
        contract: Arc<RuntimeOrderContract>,
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
            || range.contract.as_ref() != contract.as_ref()
            || range.contract.compare(&range.bound, &range.bound).is_err()
        {
            return Err(RuntimeFilterArtifactQueryError::ContractViolation);
        }
        Ok(Self::Ordered {
            artifact: Arc::clone(artifact),
            data_type: contract.keys()[0].data_type().clone(),
        })
    }

    fn ordered_range(
        &self,
    ) -> Result<&crate::runtime_filter::artifact::RangeResidentData, RuntimeFilterArtifactQueryError>
    {
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
}

impl RuntimeFilterArtifactQuery for BackendRuntimeFilterArtifactQuery {
    fn data_type(&self) -> &DataType {
        match self {
            Self::Membership { data_type, .. } | Self::Ordered { data_type, .. } => data_type,
        }
    }

    fn matches_null(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
        match self {
            Self::Membership {
                artifact,
                null_semantics,
                ..
            } => Ok(*null_semantics == RuntimeFilterNullSemantics::NullSafeEqual
                && artifact.contains_null()),
            Self::Ordered { .. } => {
                let range = self.ordered_range()?;
                self.ordered_matches_tuple(
                    OrderedTuple::try_new(&range.contract, [None])
                        .map_err(|_| RuntimeFilterArtifactQueryError::ContractViolation)?,
                )
            }
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
            } => indexed_membership_contains(
                artifact.canonical_bytes(),
                artifact
                    .membership_index()
                    .ok_or(RuntimeFilterArtifactQueryError::ContractViolation)?,
                membership_probe(value, data_type)?,
            )
            .map_err(map_query_error),
            Self::Ordered { data_type, .. } => {
                let range = self.ordered_range()?;
                let scalar = ordered_scalar(value, data_type)?;
                self.ordered_matches_tuple(
                    OrderedTuple::try_new(&range.contract, [Some(scalar)])
                        .map_err(|_| RuntimeFilterArtifactQueryError::ContractViolation)?,
                )
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

fn ordered_scalar(
    value: RuntimeFilterScalarRef<'_>,
    expected: &DataType,
) -> Result<OrderedScalar, RuntimeFilterArtifactQueryError> {
    Ok(match (value, expected) {
        (RuntimeFilterScalarRef::Boolean(value), DataType::Boolean) => {
            OrderedScalar::Boolean(value)
        }
        (RuntimeFilterScalarRef::Int8(value), DataType::Int8) => OrderedScalar::Int8(value),
        (RuntimeFilterScalarRef::Int16(value), DataType::Int16) => OrderedScalar::Int16(value),
        (RuntimeFilterScalarRef::Int32(value), DataType::Int32) => OrderedScalar::Int32(value),
        (RuntimeFilterScalarRef::Int64(value), DataType::Int64) => OrderedScalar::Int64(value),
        (RuntimeFilterScalarRef::LargeInt(value), DataType::FixedSizeBinary(16)) => {
            OrderedScalar::LargeInt(value)
        }
        (RuntimeFilterScalarRef::Utf8(value), DataType::Utf8) => OrderedScalar::Utf8(value.into()),
        (RuntimeFilterScalarRef::Date32(value), DataType::Date32) => OrderedScalar::Date32(value),
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
        ) => OrderedScalar::Timestamp(value),
        (RuntimeFilterScalarRef::Decimal128(value), DataType::Decimal128(_, _)) => {
            OrderedScalar::Decimal128(value)
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
