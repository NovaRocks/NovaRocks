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

use std::cmp::Ordering;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray, Int8Array, Int16Array,
    Int32Array, Int64Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};

use crate::runtime_filter::materializer::codec::{ArtifactCodecError, encode_range_leaf};
use crate::runtime_filter::model::contract::ChannelId;
use crate::runtime_filter::port::artifact::{
    ArtifactBundle, ArtifactKind, ArtifactSchemaDigest, ConsumerArtifactProfile, ConsumerProfileId,
    LEAF_CODEC_VERSION, PhysicalArtifact,
};
use crate::runtime_filter::port::identity::LogicalVersion;
use crate::runtime_filter::port::ordered_bound::{
    OrderedScalar, OrderedTuple, OrderedTupleError, RuntimeOrderContract,
};

#[derive(Clone, Debug)]
pub(crate) struct OrderedRangePredicateContract {
    channel_id: ChannelId,
    order_contract: Arc<RuntimeOrderContract>,
    logical_version: LogicalVersion,
    profile: ConsumerArtifactProfile,
}

impl OrderedRangePredicateContract {
    pub(crate) fn new(
        channel_id: ChannelId,
        order_contract: Arc<RuntimeOrderContract>,
        logical_version: LogicalVersion,
    ) -> Result<Self, OrderedPredicateCompileError> {
        if order_contract.keys().len() != 1 {
            return Err(OrderedPredicateCompileError::UnsupportedArity {
                actual: order_contract.keys().len(),
            });
        }
        let profile = ConsumerArtifactProfile::new_ordered_range(order_contract.digest())
            .map_err(|_| OrderedPredicateCompileError::OrderContractMismatch)?;
        Ok(Self {
            channel_id,
            order_contract,
            logical_version,
            profile,
        })
    }

    pub(crate) const fn order_contract(&self) -> &Arc<RuntimeOrderContract> {
        &self.order_contract
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum OrderedPredicateCompileError {
    ChannelMismatch {
        expected: ChannelId,
        actual: ChannelId,
    },
    ProfileMismatch {
        expected: ConsumerProfileId,
        actual: ConsumerProfileId,
    },
    VersionMismatch {
        expected: LogicalVersion,
        actual: LogicalVersion,
    },
    ArtifactCount {
        actual: usize,
    },
    KindOutsideOrderedProfile(ArtifactKind),
    CodecVersionMismatch {
        expected: u16,
        actual: u16,
    },
    UnsupportedArity {
        actual: usize,
    },
    SchemaMismatch,
    OrderContractMismatch,
    ArtifactMetadataMismatch,
    MalformedArtifact(ArtifactCodecError),
    ResourceUnavailable,
}

impl fmt::Display for OrderedPredicateCompileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid ordered runtime filter predicate contract: {self:?}"
        )
    }
}

impl Error for OrderedPredicateCompileError {}

#[derive(Clone)]
pub(crate) struct NativeOrderedRangePredicate {
    artifact: Arc<PhysicalArtifact>,
    order_contract: Arc<RuntimeOrderContract>,
    bound: OrderedTuple,
    logical_version: LogicalVersion,
}

impl NativeOrderedRangePredicate {
    pub(crate) fn compile(
        bundle: &ArtifactBundle,
        expected: &OrderedRangePredicateContract,
    ) -> Result<Self, OrderedPredicateCompileError> {
        if bundle.channel_id() != expected.channel_id {
            return Err(OrderedPredicateCompileError::ChannelMismatch {
                expected: expected.channel_id,
                actual: bundle.channel_id(),
            });
        }
        if bundle.profile_id() != expected.profile.id() {
            return Err(OrderedPredicateCompileError::ProfileMismatch {
                expected: expected.profile.id(),
                actual: bundle.profile_id(),
            });
        }
        if bundle.version() != expected.logical_version {
            return Err(OrderedPredicateCompileError::VersionMismatch {
                expected: expected.logical_version,
                actual: bundle.version(),
            });
        }
        let [(kind, artifact)] = bundle.artifacts() else {
            return Err(OrderedPredicateCompileError::ArtifactCount {
                actual: bundle.artifacts().len(),
            });
        };
        if *kind != ArtifactKind::Range {
            return Err(OrderedPredicateCompileError::KindOutsideOrderedProfile(
                *kind,
            ));
        }
        if artifact.codec_version() != LEAF_CODEC_VERSION {
            return Err(OrderedPredicateCompileError::CodecVersionMismatch {
                expected: LEAF_CODEC_VERSION,
                actual: artifact.codec_version(),
            });
        }
        if artifact.schema_digest()
            != ArtifactSchemaDigest::from_canonical_bytes(expected.order_contract.digest().bytes())
        {
            return Err(OrderedPredicateCompileError::SchemaMismatch);
        }
        if artifact.kind() != *kind || artifact.version() != expected.logical_version {
            return Err(OrderedPredicateCompileError::ArtifactMetadataMismatch);
        }
        let range = artifact
            .range()
            .ok_or(OrderedPredicateCompileError::MalformedArtifact(
                ArtifactCodecError::ContractViolation,
            ))?;
        if range.contract().as_ref() != expected.order_contract.as_ref()
            || range.contract().digest() != expected.order_contract.digest()
        {
            return Err(OrderedPredicateCompileError::OrderContractMismatch);
        }
        range
            .contract()
            .compare(range.bound(), range.bound())
            .map_err(|_| OrderedPredicateCompileError::OrderContractMismatch)?;
        let canonical =
            encode_range_leaf(range.contract(), range.bound(), expected.logical_version)
                .map_err(classify_codec_error)?;
        if canonical.as_slice() != artifact.canonical_bytes() {
            return Err(OrderedPredicateCompileError::MalformedArtifact(
                ArtifactCodecError::NonCanonicalPayload,
            ));
        }
        Ok(Self {
            artifact: artifact.clone(),
            order_contract: range.contract().clone(),
            bound: range.bound().clone(),
            logical_version: expected.logical_version,
        })
    }

    pub(crate) const fn logical_version(&self) -> LogicalVersion {
        self.logical_version
    }

    pub(crate) fn data_type(&self) -> &DataType {
        self.order_contract.keys()[0].data_type()
    }

    pub(crate) fn evaluate(
        &self,
        array: &dyn Array,
    ) -> Result<BooleanArray, OrderedPredicateEvaluationError> {
        let data_type = self.order_contract.keys()[0].data_type();
        if array.data_type() != data_type {
            return Err(OrderedPredicateEvaluationError::TypeMismatch {
                expected: data_type.clone(),
                actual: array.data_type().clone(),
            });
        }
        macro_rules! primitive {
            ($array_ty:ty, $variant:ident) => {{
                let typed = downcast::<$array_ty>(array, data_type)?;
                self.evaluate_rows(typed, |array, index| {
                    OrderedScalar::$variant(array.value(index))
                })
            }};
        }
        match data_type {
            DataType::Boolean => primitive!(BooleanArray, Boolean),
            DataType::Int8 => primitive!(Int8Array, Int8),
            DataType::Int16 => primitive!(Int16Array, Int16),
            DataType::Int32 => primitive!(Int32Array, Int32),
            DataType::Int64 => primitive!(Int64Array, Int64),
            DataType::FixedSizeBinary(width)
                if *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH =>
            {
                let typed = downcast::<FixedSizeBinaryArray>(array, data_type)?;
                self.evaluate_rows(typed, |array, index| {
                    OrderedScalar::LargeInt(
                        novarocks_types::largeint::i128_from_be_bytes(array.value(index))
                            .expect("FixedSizeBinary(16) is a complete i128"),
                    )
                })
            }
            DataType::Utf8 => {
                let typed = downcast::<StringArray>(array, data_type)?;
                self.evaluate_rows(typed, |array, index| {
                    OrderedScalar::Utf8(Arc::from(array.value(index)))
                })
            }
            DataType::Date32 => primitive!(Date32Array, Date32),
            DataType::Timestamp(unit, _) => match unit {
                TimeUnit::Second => primitive!(TimestampSecondArray, Timestamp),
                TimeUnit::Millisecond => primitive!(TimestampMillisecondArray, Timestamp),
                TimeUnit::Microsecond => primitive!(TimestampMicrosecondArray, Timestamp),
                TimeUnit::Nanosecond => primitive!(TimestampNanosecondArray, Timestamp),
            },
            DataType::Decimal128(_, _) => primitive!(Decimal128Array, Decimal128),
            _ => Err(OrderedPredicateEvaluationError::TypeMismatch {
                expected: data_type.clone(),
                actual: array.data_type().clone(),
            }),
        }
    }

    fn evaluate_rows<'a, A: Array>(
        &self,
        array: &'a A,
        scalar: impl Fn(&'a A, usize) -> OrderedScalar,
    ) -> Result<BooleanArray, OrderedPredicateEvaluationError> {
        let mut mask = Vec::new();
        mask.try_reserve_exact(array.len())
            .map_err(|_| OrderedPredicateEvaluationError::ResourceUnavailable)?;
        for index in 0..array.len() {
            let value = if array.is_null(index) {
                None
            } else {
                Some(scalar(array, index))
            };
            let row = OrderedTuple::try_new(&self.order_contract, [value])
                .map_err(OrderedPredicateEvaluationError::MalformedPredicate)?;
            mask.push(
                self.order_contract
                    .compare(&row, &self.bound)
                    .map_err(OrderedPredicateEvaluationError::MalformedPredicate)?
                    != Ordering::Greater,
            );
        }
        Ok(BooleanArray::from(mask))
    }
}

impl fmt::Debug for NativeOrderedRangePredicate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeOrderedRangePredicate")
            .field("artifact", &self.artifact)
            .field("logical_version", &self.logical_version)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum OrderedPredicateEvaluationError {
    TypeMismatch {
        expected: DataType,
        actual: DataType,
    },
    ResourceUnavailable,
    MalformedPredicate(OrderedTupleError),
}

impl fmt::Display for OrderedPredicateEvaluationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "ordered runtime filter predicate evaluation failed: {self:?}"
        )
    }
}

impl Error for OrderedPredicateEvaluationError {}

fn classify_codec_error(error: ArtifactCodecError) -> OrderedPredicateCompileError {
    match error {
        ArtifactCodecError::ResourceUnavailable | ArtifactCodecError::ResourceLimit => {
            OrderedPredicateCompileError::ResourceUnavailable
        }
        error => OrderedPredicateCompileError::MalformedArtifact(error),
    }
}

fn downcast<'a, T: Array + 'static>(
    array: &'a dyn Array,
    data_type: &DataType,
) -> Result<&'a T, OrderedPredicateEvaluationError> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        OrderedPredicateEvaluationError::TypeMismatch {
            expected: data_type.clone(),
            actual: array.data_type().clone(),
        }
    })
}

#[cfg(test)]
pub(crate) mod tests_support {
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use crate::runtime_filter::core::ordered_reducer::OrderedBoundDomain;
    use crate::runtime_filter::materializer::range::{
        RangeMaterializationOutcome, RangeMaterializer,
    };
    use crate::runtime_filter::model::contract::{
        ChannelId, NullOrder, OrderContract, OrderKeyContract, SortDirection,
    };
    use crate::runtime_filter::port::artifact::{ArtifactBundle, ConsumerArtifactProfile};
    use crate::runtime_filter::port::identity::LogicalVersion;
    use crate::runtime_filter::port::ordered_bound::{
        COMPARATOR_ALGORITHM_VERSION, ComparatorDigestV1, OrderedScalar, OrderedTuple,
        RuntimeOrderContract,
    };
    use crate::runtime_filter::port::support::{
        ArtifactRetainedBudget, ArtifactScratchBudget, MemoryAccountError,
        RetainedMemoryReservation, RuntimeFilterMemoryAccount,
    };
    use crate::runtime_filter::port::value_domain::LogicalSnapshot;

    struct UnlimitedMemory;

    impl RuntimeFilterMemoryAccount for UnlimitedMemory {
        fn try_consume(&self, _bytes: usize) -> Result<(), MemoryAccountError> {
            Ok(())
        }

        fn release(&self, _bytes: usize) {}
    }

    pub(crate) fn contract(
        data_type: DataType,
        direction: SortDirection,
        null_order: NullOrder,
    ) -> Arc<RuntimeOrderContract> {
        let keys = vec![OrderKeyContract {
            data_type,
            direction,
            null_order,
        }];
        Arc::new(
            RuntimeOrderContract::try_from_plan(&OrderContract {
                comparator_digest: ComparatorDigestV1::for_contract(
                    &keys,
                    COMPARATOR_ALGORITHM_VERSION,
                )
                .unwrap(),
                keys,
                inclusive: true,
            })
            .unwrap(),
        )
    }

    pub(crate) fn bundle(
        contract: Arc<RuntimeOrderContract>,
        bound: Option<OrderedScalar>,
        version: LogicalVersion,
    ) -> Arc<ArtifactBundle> {
        let bound = OrderedTuple::try_new(&contract, [bound]).unwrap();
        let snapshot = Arc::new(LogicalSnapshot::ordered(
            ChannelId::new(7),
            version,
            Arc::new(OrderedBoundDomain::new(contract.clone(), bound)),
            RetainedMemoryReservation::empty(),
        ));
        let profile = ConsumerArtifactProfile::new_ordered_range(contract.digest()).unwrap();
        match RangeMaterializer::materialize(
            snapshot,
            &profile,
            usize::MAX,
            Arc::new(ArtifactRetainedBudget::new(1 << 20)),
            Arc::new(ArtifactScratchBudget::new(1 << 20, 1 << 20).unwrap()),
            Arc::new(UnlimitedMemory),
        ) {
            RangeMaterializationOutcome::Published(bundle) => bundle,
            other => panic!("range fixture must publish, got {other:?}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::mem::size_of;
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, BooleanArray, Date32Array, Decimal128Array, Int8Array, Int16Array, Int32Array,
        Int64Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    };
    use arrow::datatypes::{DataType, TimeUnit};

    use super::{
        NativeOrderedRangePredicate, OrderedPredicateCompileError, OrderedPredicateEvaluationError,
        OrderedRangePredicateContract,
    };
    use crate::runtime_filter::materializer::codec::encode_range_leaf;
    use crate::runtime_filter::model::contract::{ChannelId, NullOrder, SortDirection};
    use crate::runtime_filter::port::artifact::{
        ArtifactBundle, ArtifactKind, ConsumerArtifactProfile, PhysicalArtifact, RangeArtifactData,
    };
    use crate::runtime_filter::port::identity::LogicalVersion;
    use crate::runtime_filter::port::ordered_bound::{
        OrderedScalar, OrderedTuple, RuntimeOrderContract, RuntimeOrderKey,
    };
    use crate::runtime_filter::port::support::{
        ArtifactRetainedBudget, ArtifactRetention, MemoryAccountError, RuntimeFilterMemoryAccount,
    };

    use super::tests_support::{bundle, contract};

    struct UnlimitedMemory;

    impl RuntimeFilterMemoryAccount for UnlimitedMemory {
        fn try_consume(&self, _bytes: usize) -> Result<(), MemoryAccountError> {
            Ok(())
        }

        fn release(&self, _bytes: usize) {}
    }

    fn predicate(
        data_type: DataType,
        direction: SortDirection,
        null_order: NullOrder,
        bound: Option<OrderedScalar>,
    ) -> NativeOrderedRangePredicate {
        let contract = contract(data_type, direction, null_order);
        let bundle = bundle(contract.clone(), bound, LogicalVersion::FIRST);
        NativeOrderedRangePredicate::compile(
            &bundle,
            &OrderedRangePredicateContract::new(ChannelId::new(7), contract, LogicalVersion::FIRST)
                .unwrap(),
        )
        .unwrap()
    }

    fn mask_values(mask: &BooleanArray) -> Vec<bool> {
        mask.iter().map(|value| value.unwrap()).collect()
    }

    #[derive(Clone, Copy, Debug)]
    enum NaturalRelation {
        Null,
        Less,
        Equal,
        Greater,
    }

    struct OrderedScalarFixture {
        name: &'static str,
        data_type: DataType,
        bound: OrderedScalar,
        values: ArrayRef,
        relations: Vec<NaturalRelation>,
    }

    fn supported_ordered_scalar_fixtures() -> Vec<OrderedScalarFixture> {
        let timezone: Arc<str> = Arc::from("UTC");
        vec![
            OrderedScalarFixture {
                name: "Boolean",
                data_type: DataType::Boolean,
                bound: OrderedScalar::Boolean(false),
                values: Arc::new(BooleanArray::from(vec![None, Some(false), Some(true)])),
                relations: vec![
                    NaturalRelation::Null,
                    NaturalRelation::Equal,
                    NaturalRelation::Greater,
                ],
            },
            OrderedScalarFixture {
                name: "Int8",
                data_type: DataType::Int8,
                bound: OrderedScalar::Int8(1),
                values: Arc::new(Int8Array::from(vec![None, Some(0), Some(1), Some(2)])),
                relations: ordered_numeric_relations(),
            },
            OrderedScalarFixture {
                name: "Int16",
                data_type: DataType::Int16,
                bound: OrderedScalar::Int16(2),
                values: Arc::new(Int16Array::from(vec![None, Some(1), Some(2), Some(3)])),
                relations: ordered_numeric_relations(),
            },
            OrderedScalarFixture {
                name: "Int32",
                data_type: DataType::Int32,
                bound: OrderedScalar::Int32(3),
                values: Arc::new(Int32Array::from(vec![None, Some(2), Some(3), Some(4)])),
                relations: ordered_numeric_relations(),
            },
            OrderedScalarFixture {
                name: "Int64",
                data_type: DataType::Int64,
                bound: OrderedScalar::Int64(4),
                values: Arc::new(Int64Array::from(vec![None, Some(3), Some(4), Some(5)])),
                relations: ordered_numeric_relations(),
            },
            OrderedScalarFixture {
                name: "LargeInt",
                data_type: DataType::FixedSizeBinary(
                    novarocks_types::largeint::LARGEINT_BYTE_WIDTH,
                ),
                bound: OrderedScalar::LargeInt(5),
                values: novarocks_types::largeint::array_from_i128(&[
                    None,
                    Some(4),
                    Some(5),
                    Some(6),
                ])
                .unwrap(),
                relations: ordered_numeric_relations(),
            },
            OrderedScalarFixture {
                name: "Utf8",
                data_type: DataType::Utf8,
                bound: OrderedScalar::Utf8("m".into()),
                values: Arc::new(StringArray::from(vec![
                    None,
                    Some("a"),
                    Some("m"),
                    Some("z"),
                ])),
                relations: ordered_numeric_relations(),
            },
            OrderedScalarFixture {
                name: "Date32",
                data_type: DataType::Date32,
                bound: OrderedScalar::Date32(10),
                values: Arc::new(Date32Array::from(vec![None, Some(9), Some(10), Some(11)])),
                relations: ordered_numeric_relations(),
            },
            OrderedScalarFixture {
                name: "TimestampSecond",
                data_type: DataType::Timestamp(TimeUnit::Second, Some(timezone.clone())),
                bound: OrderedScalar::Timestamp(20),
                values: Arc::new(
                    TimestampSecondArray::from(vec![None, Some(19), Some(20), Some(21)])
                        .with_timezone(timezone.clone()),
                ),
                relations: ordered_numeric_relations(),
            },
            OrderedScalarFixture {
                name: "TimestampMillisecond",
                data_type: DataType::Timestamp(TimeUnit::Millisecond, Some(timezone.clone())),
                bound: OrderedScalar::Timestamp(30),
                values: Arc::new(
                    TimestampMillisecondArray::from(vec![None, Some(29), Some(30), Some(31)])
                        .with_timezone(timezone.clone()),
                ),
                relations: ordered_numeric_relations(),
            },
            OrderedScalarFixture {
                name: "TimestampMicrosecond",
                data_type: DataType::Timestamp(TimeUnit::Microsecond, Some(timezone.clone())),
                bound: OrderedScalar::Timestamp(40),
                values: Arc::new(
                    TimestampMicrosecondArray::from(vec![None, Some(39), Some(40), Some(41)])
                        .with_timezone(timezone.clone()),
                ),
                relations: ordered_numeric_relations(),
            },
            OrderedScalarFixture {
                name: "TimestampNanosecond",
                data_type: DataType::Timestamp(TimeUnit::Nanosecond, Some(timezone.clone())),
                bound: OrderedScalar::Timestamp(50),
                values: Arc::new(
                    TimestampNanosecondArray::from(vec![None, Some(49), Some(50), Some(51)])
                        .with_timezone(timezone),
                ),
                relations: ordered_numeric_relations(),
            },
            OrderedScalarFixture {
                name: "Decimal128",
                data_type: DataType::Decimal128(18, 2),
                bound: OrderedScalar::Decimal128(300),
                values: Arc::new(
                    Decimal128Array::from(vec![None, Some(299), Some(300), Some(301)])
                        .with_precision_and_scale(18, 2)
                        .unwrap(),
                ),
                relations: ordered_numeric_relations(),
            },
        ]
    }

    fn ordered_numeric_relations() -> Vec<NaturalRelation> {
        vec![
            NaturalRelation::Null,
            NaturalRelation::Less,
            NaturalRelation::Equal,
            NaturalRelation::Greater,
        ]
    }

    fn expected_matrix_mask(
        relations: &[NaturalRelation],
        direction: SortDirection,
        null_order: NullOrder,
    ) -> Vec<bool> {
        relations
            .iter()
            .map(|relation| match relation {
                NaturalRelation::Null => null_order == NullOrder::First,
                NaturalRelation::Less => direction == SortDirection::Ascending,
                NaturalRelation::Equal => true,
                NaturalRelation::Greater => direction == SortDirection::Descending,
            })
            .collect()
    }

    #[test]
    fn ordered_range_predicate_compiles_exact_range_contract_and_version() {
        let order = contract(DataType::Int64, SortDirection::Ascending, NullOrder::Last);
        let artifact = bundle(
            order.clone(),
            Some(OrderedScalar::Int64(3)),
            LogicalVersion::new(4),
        );

        let predicate = NativeOrderedRangePredicate::compile(
            &artifact,
            &OrderedRangePredicateContract::new(ChannelId::new(7), order, LogicalVersion::new(4))
                .unwrap(),
        )
        .unwrap();

        assert_eq!(predicate.logical_version(), LogicalVersion::new(4));
    }

    #[test]
    fn ordered_range_predicate_keeps_only_rows_comparing_at_or_before_inclusive_bound() {
        let ascending = predicate(
            DataType::Int64,
            SortDirection::Ascending,
            NullOrder::Last,
            Some(OrderedScalar::Int64(3)),
        );
        assert_eq!(
            mask_values(
                &ascending
                    .evaluate(&Int64Array::from(vec![2, 3, 4]))
                    .unwrap()
            ),
            vec![true, true, false]
        );

        let descending = predicate(
            DataType::Int64,
            SortDirection::Descending,
            NullOrder::Last,
            Some(OrderedScalar::Int64(3)),
        );
        assert_eq!(
            mask_values(
                &descending
                    .evaluate(&Int64Array::from(vec![4, 3, 2]))
                    .unwrap()
            ),
            vec![true, true, false]
        );
    }

    #[test]
    fn ordered_range_predicate_applies_direction_and_null_order_matrix_to_every_supported_type() {
        let directions = [SortDirection::Ascending, SortDirection::Descending];
        let null_orders = [NullOrder::First, NullOrder::Last];
        let fixtures = supported_ordered_scalar_fixtures();
        assert_eq!(fixtures.len(), 13, "the supported scalar matrix drifted");

        for fixture in fixtures {
            for direction in directions {
                for null_order in null_orders {
                    let predicate = predicate(
                        fixture.data_type.clone(),
                        direction,
                        null_order,
                        Some(fixture.bound.clone()),
                    );
                    assert_eq!(
                        mask_values(&predicate.evaluate(fixture.values.as_ref()).unwrap()),
                        expected_matrix_mask(&fixture.relations, direction, null_order),
                        "{} {direction:?} {null_order:?}",
                        fixture.name
                    );
                }
            }
        }
    }

    #[test]
    fn ordered_range_predicate_applies_null_bound_through_frozen_comparator() {
        let null_bound = predicate(
            DataType::Int64,
            SortDirection::Descending,
            NullOrder::Last,
            None,
        );
        assert_eq!(
            mask_values(
                &null_bound
                    .evaluate(&Int64Array::from(vec![Some(7), None]))
                    .unwrap()
            ),
            vec![true, true]
        );
    }

    #[test]
    fn ordered_range_predicate_rejects_wrong_codec_version_before_payload_validation() {
        let order = contract(DataType::Int64, SortDirection::Ascending, NullOrder::Last);
        let valid = bundle(
            order.clone(),
            Some(OrderedScalar::Int64(3)),
            LogicalVersion::FIRST,
        );
        let (kind, artifact) = valid.artifacts()[0].clone();
        let wrong_codec = Arc::new(
            artifact.clone_with_test_codec_version(super::LEAF_CODEC_VERSION.wrapping_add(1)),
        );
        let profile = ConsumerArtifactProfile::new_ordered_range(order.digest()).unwrap();
        let bundle = ArtifactBundle::new(
            ChannelId::new(7),
            LogicalVersion::FIRST,
            &profile,
            vec![(kind, wrong_codec)],
            usize::MAX,
        )
        .unwrap();
        let expected =
            OrderedRangePredicateContract::new(ChannelId::new(7), order, LogicalVersion::FIRST)
                .unwrap();

        assert!(matches!(
            NativeOrderedRangePredicate::compile(&bundle, &expected),
            Err(OrderedPredicateCompileError::CodecVersionMismatch {
                expected: super::LEAF_CODEC_VERSION,
                actual
            }) if actual == super::LEAF_CODEC_VERSION.wrapping_add(1)
        ));
    }

    #[test]
    fn ordered_range_predicate_rejects_wrong_kind_schema_order_version_and_probe_type() {
        let expected_order = contract(DataType::Int64, SortDirection::Ascending, NullOrder::Last);
        let expected = OrderedRangePredicateContract::new(
            ChannelId::new(7),
            expected_order.clone(),
            LogicalVersion::FIRST,
        )
        .unwrap();

        let membership =
            crate::exec::operators::runtime_filter::tests_support::membership_bundle(&[1]);
        let (membership_kind, membership_artifact) = membership.artifacts()[0].clone();
        let wrong_kind_profile =
            ConsumerArtifactProfile::new(BTreeSet::from([membership_kind]), None)
                .unwrap()
                .with_test_identity(
                    ConsumerArtifactProfile::new_ordered_range(expected_order.digest())
                        .unwrap()
                        .id(),
                );
        let wrong_kind = ArtifactBundle::new(
            ChannelId::new(7),
            LogicalVersion::FIRST,
            &wrong_kind_profile,
            vec![(membership_kind, membership_artifact)],
            usize::MAX,
        )
        .unwrap();
        assert!(matches!(
            NativeOrderedRangePredicate::compile(&wrong_kind, &expected),
            Err(OrderedPredicateCompileError::KindOutsideOrderedProfile(_))
        ));

        let wrong_schema_order =
            contract(DataType::Int32, SortDirection::Ascending, NullOrder::Last);
        let wrong_schema_source = bundle(
            wrong_schema_order.clone(),
            Some(OrderedScalar::Int32(3)),
            LogicalVersion::FIRST,
        );
        let (range_kind, wrong_schema_artifact) = wrong_schema_source.artifacts()[0].clone();
        let wrong_schema_profile =
            ConsumerArtifactProfile::new_ordered_range(wrong_schema_order.digest())
                .unwrap()
                .with_test_identity(
                    ConsumerArtifactProfile::new_ordered_range(expected_order.digest())
                        .unwrap()
                        .id(),
                );
        let wrong_schema_bundle = ArtifactBundle::new(
            ChannelId::new(7),
            LogicalVersion::FIRST,
            &wrong_schema_profile,
            vec![(range_kind, wrong_schema_artifact)],
            usize::MAX,
        )
        .unwrap();
        assert!(matches!(
            NativeOrderedRangePredicate::compile(&wrong_schema_bundle, &expected),
            Err(OrderedPredicateCompileError::SchemaMismatch)
        ));

        let forged_order = Arc::new(
            RuntimeOrderContract::from_codec(
                vec![RuntimeOrderKey::new(
                    DataType::Int64,
                    SortDirection::Descending,
                    NullOrder::Last,
                )],
                expected_order.plan_comparator_digest(),
                expected_order.digest(),
            )
            .unwrap(),
        );
        let wrong_order_bundle = bundle(
            forged_order,
            Some(OrderedScalar::Int64(3)),
            LogicalVersion::FIRST,
        );
        assert!(matches!(
            NativeOrderedRangePredicate::compile(&wrong_order_bundle, &expected),
            Err(OrderedPredicateCompileError::OrderContractMismatch)
        ));

        let wrong_version = OrderedRangePredicateContract::new(
            ChannelId::new(7),
            expected_order,
            LogicalVersion::new(2),
        )
        .unwrap();
        let valid = bundle(
            wrong_version.order_contract().clone(),
            Some(OrderedScalar::Int64(3)),
            LogicalVersion::FIRST,
        );
        assert!(matches!(
            NativeOrderedRangePredicate::compile(&valid, &wrong_version),
            Err(OrderedPredicateCompileError::VersionMismatch { .. })
        ));

        let predicate = NativeOrderedRangePredicate::compile(
            &valid,
            &OrderedRangePredicateContract::new(
                ChannelId::new(7),
                wrong_version.order_contract().clone(),
                LogicalVersion::FIRST,
            )
            .unwrap(),
        )
        .unwrap();
        assert!(matches!(
            predicate.evaluate(&Int32Array::from(vec![3])),
            Err(OrderedPredicateEvaluationError::TypeMismatch { .. })
        ));
    }

    #[test]
    fn ordered_range_predicate_rejects_malformed_trusted_range_payload() {
        let order = contract(DataType::Int64, SortDirection::Ascending, NullOrder::Last);
        let bound = OrderedTuple::try_new(&order, [Some(OrderedScalar::Int64(3))]).unwrap();
        let data =
            RangeArtifactData::new(order.clone(), bound.clone(), LogicalVersion::FIRST).unwrap();
        let mut encoded = encode_range_leaf(&order, &bound, LogicalVersion::FIRST).unwrap();
        *encoded.last_mut().unwrap() ^= 0xff;
        let component =
            PhysicalArtifact::accounted_range_resident_component_bytes(encoded.len(), &data)
                .unwrap();
        let retained_bytes = component + size_of::<ArtifactRetention>() + 2 * size_of::<usize>();
        let retained = ArtifactRetention::try_new(
            retained_bytes,
            Arc::new(ArtifactRetainedBudget::new(retained_bytes)),
            Arc::new(UnlimitedMemory),
        )
        .unwrap();
        let artifact = Arc::new(
            PhysicalArtifact::from_range_retained(
                LogicalVersion::FIRST,
                data,
                encoded.into(),
                retained,
            )
            .unwrap(),
        );
        let profile = ConsumerArtifactProfile::new_ordered_range(order.digest()).unwrap();
        let malformed = ArtifactBundle::new(
            ChannelId::new(7),
            LogicalVersion::FIRST,
            &profile,
            vec![(ArtifactKind::Range, artifact)],
            usize::MAX,
        )
        .unwrap();
        let expected =
            OrderedRangePredicateContract::new(ChannelId::new(7), order, LogicalVersion::FIRST)
                .unwrap();

        assert!(matches!(
            NativeOrderedRangePredicate::compile(&malformed, &expected),
            Err(OrderedPredicateCompileError::MalformedArtifact(_))
        ));
    }

    #[test]
    fn ordered_range_resource_unavailable_is_explicitly_fail_open_classifiable() {
        assert_eq!(
            super::classify_codec_error(
                crate::runtime_filter::materializer::codec::ArtifactCodecError::ResourceUnavailable
            ),
            OrderedPredicateCompileError::ResourceUnavailable
        );
        assert!(matches!(
            OrderedPredicateEvaluationError::ResourceUnavailable,
            OrderedPredicateEvaluationError::ResourceUnavailable
        ));
    }
}
