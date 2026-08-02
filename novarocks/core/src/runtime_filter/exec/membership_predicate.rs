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

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};

use crate::runtime_filter::materializer::codec::{
    ArtifactCodecError, MembershipProbe, indexed_membership_contains,
};
use crate::runtime_filter::model::contract::{ChannelId, NullSemantics};
use crate::runtime_filter::port::artifact::{
    ArtifactBundle, ArtifactContractError, ArtifactKind, ArtifactMembershipSchema,
    ConsumerArtifactProfile, ConsumerProfileId, LEAF_CODEC_VERSION, PhysicalArtifact,
};
use crate::runtime_filter::port::identity::LogicalVersion;

#[derive(Clone, Debug)]
pub struct MembershipPredicateContract {
    pub channel_id: ChannelId,
    pub data_type: DataType,
    pub null_semantics: NullSemantics,
    pub logical_version: LogicalVersion,
    profile: ConsumerArtifactProfile,
}

impl MembershipPredicateContract {
    pub fn join(
        channel_id: ChannelId,
        data_type: DataType,
        null_semantics: NullSemantics,
        logical_version: LogicalVersion,
    ) -> Result<Self, ArtifactContractError> {
        Ok(Self {
            channel_id,
            data_type,
            null_semantics,
            logical_version,
            profile: ConsumerArtifactProfile::new(
                BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
                None,
            )?,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PredicateCompileError {
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
    KindOutsideJoinProfile(ArtifactKind),
    CodecVersionMismatch {
        expected: u16,
        actual: u16,
    },
    SchemaMismatch,
    NullContractMismatch,
    ArtifactMetadataMismatch,
    MalformedArtifact(ArtifactCodecError),
}

impl fmt::Display for PredicateCompileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid native runtime filter predicate contract: {self:?}"
        )
    }
}

impl Error for PredicateCompileError {}

#[derive(Clone)]
pub struct NativeRuntimeFilterPredicate {
    // The retained canonical artifact is the only resident membership payload.
    // `resident` stores validated offsets/metadata only, so compilation adds no
    // unaccounted value-set or string allocation beyond the existing retention.
    artifact: Arc<PhysicalArtifact>,
    data_type: DataType,
    null_semantics: NullSemantics,
}

impl NativeRuntimeFilterPredicate {
    pub fn compile(
        bundle: &ArtifactBundle,
        expected: &MembershipPredicateContract,
    ) -> Result<Self, PredicateCompileError> {
        if bundle.channel_id() != expected.channel_id {
            return Err(PredicateCompileError::ChannelMismatch {
                expected: expected.channel_id,
                actual: bundle.channel_id(),
            });
        }
        if bundle.profile_id() != expected.profile.id() {
            return Err(PredicateCompileError::ProfileMismatch {
                expected: expected.profile.id(),
                actual: bundle.profile_id(),
            });
        }
        if bundle.version() != expected.logical_version {
            return Err(PredicateCompileError::VersionMismatch {
                expected: expected.logical_version,
                actual: bundle.version(),
            });
        }
        let [(kind, artifact)] = bundle.artifacts() else {
            return Err(PredicateCompileError::ArtifactCount {
                actual: bundle.artifacts().len(),
            });
        };
        if !matches!(kind, ArtifactKind::ValueSet | ArtifactKind::EmptyDomain) {
            return Err(PredicateCompileError::KindOutsideJoinProfile(*kind));
        }
        if artifact.codec_version() != LEAF_CODEC_VERSION {
            return Err(PredicateCompileError::CodecVersionMismatch {
                expected: LEAF_CODEC_VERSION,
                actual: artifact.codec_version(),
            });
        }
        let resident =
            artifact
                .membership_index()
                .ok_or(PredicateCompileError::MalformedArtifact(
                    ArtifactCodecError::ContractViolation,
                ))?;
        let expected_schema =
            ArtifactMembershipSchema::new(&expected.data_type, expected.null_semantics)
                .map_err(|_| PredicateCompileError::SchemaMismatch)?;
        if artifact.schema_digest() != expected_schema.digest() {
            return Err(PredicateCompileError::SchemaMismatch);
        }
        if artifact.contains_null() && expected.null_semantics != NullSemantics::NullSafeEqual {
            return Err(PredicateCompileError::NullContractMismatch);
        }
        if artifact.kind() != *kind || artifact.version() != expected.logical_version {
            return Err(PredicateCompileError::ArtifactMetadataMismatch);
        }
        if matches!(kind, ArtifactKind::EmptyDomain)
            != matches!(
                resident.view(),
                crate::runtime_filter::port::artifact::ResidentMembershipIndexView::EmptyDomain
            )
        {
            return Err(PredicateCompileError::ArtifactMetadataMismatch);
        }
        Ok(Self {
            artifact: artifact.clone(),
            data_type: expected.data_type.clone(),
            null_semantics: expected.null_semantics,
        })
    }

    pub fn evaluate(&self, array: &dyn Array) -> Result<BooleanArray, PredicateEvaluationError> {
        if array.data_type() != &self.data_type {
            return Err(PredicateEvaluationError::TypeMismatch {
                expected: self.data_type.clone(),
                actual: array.data_type().clone(),
            });
        }
        macro_rules! primitive {
            ($array_ty:ty, $variant:ident) => {{
                let typed = downcast::<$array_ty>(array, &self.data_type)?;
                self.evaluate_rows(typed, |array, index| {
                    MembershipProbe::$variant(array.value(index))
                })
            }};
        }
        match &self.data_type {
            DataType::Boolean => primitive!(BooleanArray, Boolean),
            DataType::Int8 => primitive!(Int8Array, Int8),
            DataType::Int16 => primitive!(Int16Array, Int16),
            DataType::Int32 => primitive!(Int32Array, Int32),
            DataType::Int64 => primitive!(Int64Array, Int64),
            DataType::FixedSizeBinary(width)
                if *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH =>
            {
                let typed = downcast::<FixedSizeBinaryArray>(array, &self.data_type)?;
                self.evaluate_rows(typed, |array, index| {
                    MembershipProbe::LargeInt(
                        novarocks_types::largeint::i128_from_be_bytes(array.value(index))
                            .expect("FixedSizeBinary(16) is a complete i128"),
                    )
                })
            }
            DataType::Float32 => primitive!(Float32Array, Float32),
            DataType::Float64 => primitive!(Float64Array, Float64),
            DataType::Utf8 => primitive!(StringArray, Utf8),
            DataType::Date32 => primitive!(Date32Array, Date32),
            DataType::Timestamp(unit, _) => match unit {
                TimeUnit::Second => primitive!(TimestampSecondArray, Timestamp),
                TimeUnit::Millisecond => primitive!(TimestampMillisecondArray, Timestamp),
                TimeUnit::Microsecond => primitive!(TimestampMicrosecondArray, Timestamp),
                TimeUnit::Nanosecond => primitive!(TimestampNanosecondArray, Timestamp),
            },
            DataType::Decimal128(_, _) => primitive!(Decimal128Array, Decimal128),
            _ => Err(PredicateEvaluationError::TypeMismatch {
                expected: self.data_type.clone(),
                actual: array.data_type().clone(),
            }),
        }
    }

    fn evaluate_rows<'a, A: Array>(
        &self,
        array: &'a A,
        probe: impl Fn(&'a A, usize) -> MembershipProbe<'a>,
    ) -> Result<BooleanArray, PredicateEvaluationError> {
        let mut mask = Vec::new();
        mask.try_reserve_exact(array.len())
            .map_err(|_| PredicateEvaluationError::ResourceUnavailable)?;
        for index in 0..array.len() {
            if array.is_null(index) {
                mask.push(
                    self.null_semantics == NullSemantics::NullSafeEqual
                        && self.artifact.contains_null(),
                );
            } else {
                mask.push(
                    indexed_membership_contains(
                        self.artifact.canonical_bytes(),
                        self.artifact.membership_index().ok_or(
                            PredicateEvaluationError::MalformedArtifact(
                                ArtifactCodecError::ContractViolation,
                            ),
                        )?,
                        probe(array, index),
                    )
                    .map_err(PredicateEvaluationError::MalformedArtifact)?,
                );
            }
        }
        Ok(BooleanArray::from(mask))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PredicateEvaluationError {
    TypeMismatch {
        expected: DataType,
        actual: DataType,
    },
    ResourceUnavailable,
    MalformedArtifact(ArtifactCodecError),
}

impl fmt::Display for PredicateEvaluationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "native runtime filter predicate evaluation failed: {self:?}"
        )
    }
}

impl Error for PredicateEvaluationError {}

fn downcast<'a, T: Array + 'static>(
    array: &'a dyn Array,
    data_type: &DataType,
) -> Result<&'a T, PredicateEvaluationError> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| PredicateEvaluationError::TypeMismatch {
            expected: data_type.clone(),
            actual: array.data_type().clone(),
        })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::array::{
        Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
        Int8Array, Int16Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
    };
    use arrow::datatypes::{DataType, TimeUnit};

    use super::{MembershipPredicateContract, NativeRuntimeFilterPredicate, PredicateCompileError};
    use crate::runtime_filter::exec::membership_delta::{
        MembershipDeltaEncoder, MembershipEncodingOutcome,
    };
    use crate::runtime_filter::materializer::codec::{
        ArtifactCodecError, ArtifactDecodeExpectations, build_membership_index, decode_leaf,
        encode_membership_leaf, encode_physical_leaf, inspect_membership_index,
    };
    use crate::runtime_filter::materializer::{MaterializationOutcome, Materializer};
    use crate::runtime_filter::model::contract::{ChannelId, NullSemantics};
    use crate::runtime_filter::port::artifact::{
        ArtifactBundle, ArtifactKind, ArtifactMembershipSchema, ConsumerArtifactProfile,
        HashContractDigest, PhysicalArtifact,
    };
    use crate::runtime_filter::port::identity::LogicalVersion;
    use crate::runtime_filter::port::install::MaterializationPolicy;
    use crate::runtime_filter::port::support::{
        ArtifactRetainedBudget, ArtifactScratchBudget, MemoryAccountError,
        RuntimeFilterMemoryAccount,
    };
    use crate::runtime_filter::port::value_domain::{
        LogicalSnapshot, MembershipValues, ReducedMembershipDomain,
    };

    struct UnlimitedMemory;

    impl RuntimeFilterMemoryAccount for UnlimitedMemory {
        fn try_consume(&self, _bytes: usize) -> Result<(), MemoryAccountError> {
            Ok(())
        }

        fn release(&self, _bytes: usize) {}
    }

    #[derive(Default)]
    struct TrackingMemory {
        bytes: AtomicUsize,
    }

    impl TrackingMemory {
        fn bytes(&self) -> usize {
            self.bytes.load(Ordering::Acquire)
        }
    }

    impl RuntimeFilterMemoryAccount for TrackingMemory {
        fn try_consume(&self, bytes: usize) -> Result<(), MemoryAccountError> {
            self.bytes.fetch_add(bytes, Ordering::AcqRel);
            Ok(())
        }

        fn release(&self, bytes: usize) {
            let previous = self.bytes.fetch_sub(bytes, Ordering::AcqRel);
            assert!(previous >= bytes);
        }
    }

    fn join_profile() -> ConsumerArtifactProfile {
        ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap()
    }

    fn bundle_for(
        values: MembershipValues,
        contains_null: bool,
        null_semantics: NullSemantics,
    ) -> ArtifactBundle {
        let version = LogicalVersion::FIRST;
        let data_type = values.data_type();
        let schema = ArtifactMembershipSchema::new(&data_type, null_semantics).unwrap();
        let encoded = encode_membership_leaf(
            &ReducedMembershipDomain::new(values, contains_null),
            null_semantics,
            version,
        )
        .unwrap();
        let kind = if contains_null || encoded.last().is_some_and(|_| encoded.len() > 0) {
            // The encoded leaf is authoritative; read the physical kind from its tag.
            ArtifactKind::from_tag(encoded[6]).unwrap()
        } else {
            unreachable!()
        };
        let index_plan = inspect_membership_index(&encoded).unwrap();
        let index = build_membership_index(&encoded, &index_plan).unwrap();
        let artifact = Arc::new(
            PhysicalArtifact::new_indexed_test(
                kind,
                schema.digest(),
                version,
                contains_null,
                encoded.into(),
                index,
            )
            .unwrap(),
        );
        ArtifactBundle::new(
            ChannelId::new(7),
            version,
            &join_profile(),
            vec![(kind, artifact)],
            usize::MAX,
        )
        .unwrap()
    }

    fn contract_for(
        channel_id: ChannelId,
        data_type: DataType,
        null_semantics: NullSemantics,
    ) -> MembershipPredicateContract {
        MembershipPredicateContract::join(
            channel_id,
            data_type,
            null_semantics,
            LogicalVersion::FIRST,
        )
        .unwrap()
    }

    fn contract(data_type: DataType, null_semantics: NullSemantics) -> MembershipPredicateContract {
        contract_for(ChannelId::new(7), data_type, null_semantics)
    }

    fn strict_decode_membership(
        encoded: &[u8],
        data_type: &DataType,
        null_semantics: NullSemantics,
        kind: ArtifactKind,
    ) -> Result<Arc<PhysicalArtifact>, ArtifactCodecError> {
        let schema = ArtifactMembershipSchema::new(data_type, null_semantics).unwrap();
        decode_leaf(
            encoded,
            ArtifactDecodeExpectations {
                expected_kind: kind,
                expected_schema_digest: schema.digest(),
                expected_logical_version: LogicalVersion::FIRST,
                expected_hash_contract: None,
            },
            encoded.len(),
            Arc::new(ArtifactRetainedBudget::new(1 << 20)),
            Arc::new(TrackingMemory::default()),
        )
    }

    fn mask_values(mask: &arrow::array::BooleanArray) -> Vec<bool> {
        (0..mask.len()).map(|index| mask.value(index)).collect()
    }

    fn assert_remote_arrow_membership(
        build: &dyn Array,
        data_type: DataType,
        probe: &dyn Array,
        expected_mask: Vec<bool>,
    ) {
        let null_semantics = NullSemantics::NullSafeEqual;
        let MembershipEncodingOutcome::Deltas(deltas) =
            MembershipDeltaEncoder::encode(build, &data_type, usize::MAX).unwrap()
        else {
            panic!("small exact domain must remain available");
        };
        let mut deltas = deltas.into_iter();
        let first = deltas.next().unwrap();
        let mut domain = ReducedMembershipDomain::new(
            first.values().clone(),
            first.retains_null(null_semantics),
        );
        for delta in deltas {
            domain
                .union_prevalidated(delta.values(), delta.retains_null(null_semantics))
                .unwrap();
        }

        let version = LogicalVersion::FIRST;
        let schema = ArtifactMembershipSchema::new(&data_type, null_semantics).unwrap();
        let encoded = encode_membership_leaf(&domain, null_semantics, version).unwrap();
        let kind = ArtifactKind::from_tag(encoded[6]).unwrap();
        let retained_budget = Arc::new(ArtifactRetainedBudget::new(1 << 20));
        let memory_account = Arc::new(TrackingMemory::default());
        let artifact = decode_leaf(
            &encoded,
            ArtifactDecodeExpectations {
                expected_kind: kind,
                expected_schema_digest: schema.digest(),
                expected_logical_version: version,
                expected_hash_contract: None,
            },
            encoded.len(),
            retained_budget.clone(),
            memory_account.clone(),
        )
        .unwrap();
        assert!(artifact.membership_index().is_some());
        assert!(artifact.retained_memory_bytes() > 0);
        assert_eq!(
            retained_budget.retained_bytes(),
            artifact.retained_memory_bytes()
        );
        assert_eq!(memory_account.bytes(), artifact.retained_memory_bytes());

        let bundle = ArtifactBundle::new(
            ChannelId::new(7),
            version,
            &join_profile(),
            vec![(kind, artifact)],
            usize::MAX,
        )
        .unwrap();
        let predicate =
            NativeRuntimeFilterPredicate::compile(&bundle, &contract(data_type, null_semantics))
                .unwrap();
        assert_eq!(
            mask_values(&predicate.evaluate(probe).unwrap()),
            expected_mask
        );
        drop(predicate);
        drop(bundle);
        assert_eq!(retained_budget.retained_bytes(), 0);
        assert_eq!(memory_account.bytes(), 0);
    }

    #[test]
    fn arrow_to_remote_decode_to_predicate_covers_typed_index_layouts() {
        assert_remote_arrow_membership(
            &Int64Array::from(vec![Some(2), None, Some(1), Some(2)]),
            DataType::Int64,
            &Int64Array::from(vec![Some(2), Some(3), None]),
            vec![true, false, true],
        );
        assert_remote_arrow_membership(
            &StringArray::from(vec![Some("alpha"), None, Some("omega")]),
            DataType::Utf8,
            &StringArray::from(vec![Some("omega"), Some("middle"), None]),
            vec![true, false, true],
        );

        let timezone: Arc<str> = Arc::from("Asia/Shanghai");
        let timestamp_type = DataType::Timestamp(TimeUnit::Microsecond, Some(timezone.clone()));
        let timestamp_build = TimestampMicrosecondArray::from(vec![Some(10), None, Some(20)])
            .with_timezone(timezone.clone());
        let timestamp_probe =
            TimestampMicrosecondArray::from(vec![Some(20), Some(30), None]).with_timezone(timezone);
        assert_remote_arrow_membership(
            &timestamp_build,
            timestamp_type,
            &timestamp_probe,
            vec![true, false, true],
        );

        let decimal_type = DataType::Decimal128(18, 2);
        let decimal_build = Decimal128Array::from(vec![Some(100), None, Some(250)])
            .with_precision_and_scale(18, 2)
            .unwrap();
        let decimal_probe = Decimal128Array::from(vec![Some(250), Some(300), None])
            .with_precision_and_scale(18, 2)
            .unwrap();
        assert_remote_arrow_membership(
            &decimal_build,
            decimal_type,
            &decimal_probe,
            vec![true, false, true],
        );
    }

    fn materialize_arrow_membership(
        array: &dyn Array,
        data_type: &DataType,
        null_semantics: NullSemantics,
    ) -> Arc<ArtifactBundle> {
        let MembershipEncodingOutcome::Deltas(deltas) =
            MembershipDeltaEncoder::encode(array, data_type, usize::MAX).unwrap()
        else {
            panic!("small Arrow domain must remain available");
        };
        let mut deltas = deltas.into_iter();
        let first = deltas.next().unwrap();
        let mut domain = ReducedMembershipDomain::new(
            first.values().clone(),
            first.retains_null(null_semantics),
        );
        for delta in deltas {
            domain
                .union_prevalidated(delta.values(), delta.retains_null(null_semantics))
                .unwrap();
        }
        let schema = ArtifactMembershipSchema::new(data_type, null_semantics).unwrap();
        let profile = join_profile();
        let plan = Materializer::plan(
            Arc::new(LogicalSnapshot::first(
                ChannelId::new(7),
                domain,
                Default::default(),
            )),
            &schema,
            &profile,
            MaterializationPolicy::for_test(),
            1 << 20,
        )
        .unwrap();
        let MaterializationOutcome::Published(bundle) = Materializer::materialize(
            plan,
            Arc::new(ArtifactRetainedBudget::new(1 << 20)),
            Arc::new(ArtifactScratchBudget::new(1 << 20, 1 << 20).unwrap()),
            Arc::new(UnlimitedMemory),
        ) else {
            panic!("small exact Arrow domain must materialize");
        };
        bundle
    }

    #[test]
    fn arrow_to_local_materializer_to_predicate_covers_typed_index_layouts() {
        let fixed_build = Int64Array::from(vec![1, 3, 5]);
        let fixed_bundle = materialize_arrow_membership(
            &fixed_build,
            &DataType::Int64,
            NullSemantics::NeverMatches,
        );
        let fixed = NativeRuntimeFilterPredicate::compile(
            &fixed_bundle,
            &contract(DataType::Int64, NullSemantics::NeverMatches),
        )
        .unwrap();
        assert_eq!(
            mask_values(&fixed.evaluate(&Int64Array::from(vec![1, 2, 5])).unwrap()),
            vec![true, false, true]
        );

        let utf8_build = StringArray::from(vec!["alpha", "omega"]);
        let utf8_bundle =
            materialize_arrow_membership(&utf8_build, &DataType::Utf8, NullSemantics::NeverMatches);
        let utf8 = NativeRuntimeFilterPredicate::compile(
            &utf8_bundle,
            &contract(DataType::Utf8, NullSemantics::NeverMatches),
        )
        .unwrap();
        assert_eq!(
            mask_values(
                &utf8
                    .evaluate(&StringArray::from(vec!["omega", "middle"]))
                    .unwrap()
            ),
            vec![true, false]
        );

        let timezone: Arc<str> = Arc::from("Asia/Shanghai");
        let timestamp_type = DataType::Timestamp(TimeUnit::Microsecond, Some(timezone.clone()));
        let timestamp_build =
            TimestampMicrosecondArray::from(vec![10, 20]).with_timezone(timezone.clone());
        let timestamp_bundle = materialize_arrow_membership(
            &timestamp_build,
            &timestamp_type,
            NullSemantics::NeverMatches,
        );
        let timestamp = NativeRuntimeFilterPredicate::compile(
            &timestamp_bundle,
            &contract(timestamp_type.clone(), NullSemantics::NeverMatches),
        )
        .unwrap();
        assert_eq!(
            mask_values(
                &timestamp
                    .evaluate(
                        &TimestampMicrosecondArray::from(vec![20, 30]).with_timezone(timezone),
                    )
                    .unwrap()
            ),
            vec![true, false]
        );

        let decimal_type = DataType::Decimal128(18, 2);
        let decimal_build = Decimal128Array::from(vec![100, 250])
            .with_precision_and_scale(18, 2)
            .unwrap();
        let decimal_bundle = materialize_arrow_membership(
            &decimal_build,
            &decimal_type,
            NullSemantics::NeverMatches,
        );
        let decimal = NativeRuntimeFilterPredicate::compile(
            &decimal_bundle,
            &contract(decimal_type, NullSemantics::NeverMatches),
        )
        .unwrap();
        let decimal_probe = Decimal128Array::from(vec![250, 300])
            .with_precision_and_scale(18, 2)
            .unwrap();
        assert_eq!(
            mask_values(&decimal.evaluate(&decimal_probe).unwrap()),
            vec![true, false]
        );
    }

    fn assert_membership_mask(values: MembershipValues, probe: ArrayRef, expected_mask: Vec<bool>) {
        let data_type = values.data_type();
        let bundle = bundle_for(values, false, NullSemantics::NeverMatches);
        let predicate = NativeRuntimeFilterPredicate::compile(
            &bundle,
            &contract(data_type, NullSemantics::NeverMatches),
        )
        .unwrap();
        assert_eq!(
            mask_values(&predicate.evaluate(probe.as_ref()).unwrap()),
            expected_mask
        );
    }

    #[test]
    fn installed_join_value_set_executes_every_supported_membership_type() {
        assert_membership_mask(
            MembershipValues::boolean([true]),
            Arc::new(BooleanArray::from(vec![true, false])),
            vec![true, false],
        );
        assert_membership_mask(
            MembershipValues::int8([-1]),
            Arc::new(Int8Array::from(vec![-1, 1])),
            vec![true, false],
        );
        assert_membership_mask(
            MembershipValues::int16([-2]),
            Arc::new(Int16Array::from(vec![-2, 2])),
            vec![true, false],
        );
        assert_membership_mask(
            MembershipValues::int32([-3]),
            Arc::new(Int32Array::from(vec![-3, 3])),
            vec![true, false],
        );
        assert_membership_mask(
            MembershipValues::int64([-4]),
            Arc::new(Int64Array::from(vec![-4, 4])),
            vec![true, false],
        );
        assert_membership_mask(
            MembershipValues::large_int([i128::MAX]),
            novarocks_types::largeint::array_from_i128(&[Some(i128::MAX), Some(i128::MIN)])
                .unwrap(),
            vec![true, false],
        );
        assert_membership_mask(
            MembershipValues::float32([f32::NAN, -0.0]),
            Arc::new(Float32Array::from(vec![
                f32::from_bits(0x7fa0_1234),
                0.0,
                1.0,
            ])),
            vec![true, true, false],
        );
        assert_membership_mask(
            MembershipValues::float64([f64::NAN, -0.0]),
            Arc::new(Float64Array::from(vec![
                f64::from_bits(0x7ff0_0000_0000_0001),
                0.0,
                1.0,
            ])),
            vec![true, true, false],
        );
        assert_membership_mask(
            MembershipValues::utf8(["match"]),
            Arc::new(StringArray::from(vec!["match", "miss"])),
            vec![true, false],
        );
        assert_membership_mask(
            MembershipValues::date32([123]),
            Arc::new(Date32Array::from(vec![123, 124])),
            vec![true, false],
        );

        let timezone: Arc<str> = Arc::from("Asia/Shanghai");
        let timestamp_cases: Vec<(MembershipValues, ArrayRef)> = vec![
            (
                MembershipValues::timestamp(TimeUnit::Second, None, [7]),
                Arc::new(TimestampSecondArray::from(vec![7, 8])),
            ),
            (
                MembershipValues::timestamp(TimeUnit::Millisecond, None, [7]),
                Arc::new(TimestampMillisecondArray::from(vec![7, 8])),
            ),
            (
                MembershipValues::timestamp(TimeUnit::Microsecond, Some(timezone.clone()), [7]),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![7, 8]).with_timezone(timezone.clone()),
                ),
            ),
            (
                MembershipValues::timestamp(TimeUnit::Nanosecond, None, [7]),
                Arc::new(TimestampNanosecondArray::from(vec![7, 8])),
            ),
        ];
        for (values, probe) in timestamp_cases {
            assert_membership_mask(values, probe, vec![true, false]);
        }

        assert_membership_mask(
            MembershipValues::decimal128(18, 2, [-1234]).unwrap(),
            Arc::new(
                Decimal128Array::from(vec![-1234, 1234])
                    .with_precision_and_scale(18, 2)
                    .unwrap(),
            ),
            vec![true, false],
        );
    }

    #[test]
    fn value_set_compiles_once_and_reuses_an_immutable_resident_domain() {
        let bundle = bundle_for(
            MembershipValues::int64([1, 3]),
            false,
            NullSemantics::NeverMatches,
        );
        let predicate = NativeRuntimeFilterPredicate::compile(
            &bundle,
            &contract(DataType::Int64, NullSemantics::NeverMatches),
        )
        .unwrap();
        let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3), None]));
        assert_eq!(
            mask_values(&predicate.evaluate(array.as_ref()).unwrap()),
            vec![true, false, true, false]
        );
        assert_eq!(
            mask_values(&predicate.evaluate(array.as_ref()).unwrap()),
            vec![true, false, true, false]
        );
    }

    #[test]
    fn empty_domain_rejects_every_probe_row() {
        let bundle = bundle_for(
            MembershipValues::int64([]),
            false,
            NullSemantics::NeverMatches,
        );
        let predicate = NativeRuntimeFilterPredicate::compile(
            &bundle,
            &contract(DataType::Int64, NullSemantics::NeverMatches),
        )
        .unwrap();
        let mask = predicate
            .evaluate(&Int64Array::from(vec![Some(1), None]))
            .unwrap();
        assert_eq!(mask_values(&mask), vec![false, false]);
    }

    #[test]
    fn canonical_null_semantics_are_applied_exactly() {
        let bundle = bundle_for(
            MembershipValues::int64([]),
            true,
            NullSemantics::NullSafeEqual,
        );
        let predicate = NativeRuntimeFilterPredicate::compile(
            &bundle,
            &contract(DataType::Int64, NullSemantics::NullSafeEqual),
        )
        .unwrap();
        let mask = predicate
            .evaluate(&Int64Array::from(vec![Some(1), Some(2), None]))
            .unwrap();
        assert_eq!(mask_values(&mask), vec![false, false, true]);
    }

    #[test]
    fn contract_profile_version_and_type_drift_fail_at_compile_or_evaluate() {
        let bundle = bundle_for(
            MembershipValues::int64([1]),
            false,
            NullSemantics::NeverMatches,
        );
        let mut wrong_version = contract(DataType::Int64, NullSemantics::NeverMatches);
        wrong_version.logical_version = LogicalVersion::new(2);
        assert!(matches!(
            NativeRuntimeFilterPredicate::compile(&bundle, &wrong_version),
            Err(PredicateCompileError::VersionMismatch { .. })
        ));

        let predicate = NativeRuntimeFilterPredicate::compile(
            &bundle,
            &contract(DataType::Int64, NullSemantics::NeverMatches),
        )
        .unwrap();
        assert!(
            predicate
                .evaluate(&arrow::array::Int32Array::from(vec![1]))
                .is_err()
        );

        assert!(matches!(
            NativeRuntimeFilterPredicate::compile(
                &bundle,
                &contract(DataType::Int32, NullSemantics::NeverMatches)
            ),
            Err(PredicateCompileError::SchemaMismatch)
        ));
        assert!(matches!(
            NativeRuntimeFilterPredicate::compile(
                &bundle,
                &contract_for(
                    ChannelId::new(8),
                    DataType::Int64,
                    NullSemantics::NeverMatches
                )
            ),
            Err(PredicateCompileError::ChannelMismatch { .. })
        ));
    }

    #[test]
    fn timestamp_timezone_decimal_and_codec_version_drift_are_rejected() {
        let timezone: Arc<str> = Arc::from("UTC");
        let timestamp_bundle = bundle_for(
            MembershipValues::timestamp(TimeUnit::Microsecond, Some(timezone), [1]),
            false,
            NullSemantics::NeverMatches,
        );
        assert!(matches!(
            NativeRuntimeFilterPredicate::compile(
                &timestamp_bundle,
                &contract(
                    DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("Asia/Shanghai"))),
                    NullSemantics::NeverMatches,
                ),
            ),
            Err(PredicateCompileError::SchemaMismatch)
        ));

        let decimal_bundle = bundle_for(
            MembershipValues::decimal128(18, 2, [100]).unwrap(),
            false,
            NullSemantics::NeverMatches,
        );
        assert!(matches!(
            NativeRuntimeFilterPredicate::compile(
                &decimal_bundle,
                &contract(DataType::Decimal128(18, 3), NullSemantics::NeverMatches),
            ),
            Err(PredicateCompileError::SchemaMismatch)
        ));

        let version = LogicalVersion::FIRST;
        let mut encoded = encode_membership_leaf(
            &ReducedMembershipDomain::new(MembershipValues::int64([1]), false),
            NullSemantics::NeverMatches,
            version,
        )
        .unwrap();
        encoded[4..6].copy_from_slice(&2u16.to_be_bytes());
        assert_eq!(
            strict_decode_membership(
                &encoded,
                &DataType::Int64,
                NullSemantics::NeverMatches,
                ArtifactKind::ValueSet,
            )
            .unwrap_err(),
            ArtifactCodecError::UnknownVersion
        );

        let mut encoded = encode_membership_leaf(
            &ReducedMembershipDomain::new(MembershipValues::int64([1]), false),
            NullSemantics::NeverMatches,
            version,
        )
        .unwrap();
        encoded[7] ^= 1;
        assert_eq!(
            strict_decode_membership(
                &encoded,
                &DataType::Int64,
                NullSemantics::NeverMatches,
                ArtifactKind::ValueSet,
            )
            .unwrap_err(),
            ArtifactCodecError::SchemaMismatch
        );
    }

    #[test]
    fn malformed_canonical_value_set_payload_is_rejected() {
        let version = LogicalVersion::FIRST;
        let mut encoded = encode_membership_leaf(
            &ReducedMembershipDomain::new(MembershipValues::int64([1, 2]), false),
            NullSemantics::NeverMatches,
            version,
        )
        .unwrap();
        encoded.pop();
        assert_eq!(
            strict_decode_membership(
                &encoded,
                &DataType::Int64,
                NullSemantics::NeverMatches,
                ArtifactKind::ValueSet,
            )
            .unwrap_err(),
            ArtifactCodecError::Truncated
        );
    }

    #[test]
    fn missing_resident_index_is_rejected_as_an_artifact_invariant() {
        let version = LogicalVersion::FIRST;
        let schema =
            ArtifactMembershipSchema::new(&DataType::Int64, NullSemantics::NeverMatches).unwrap();
        let encoded = encode_membership_leaf(
            &ReducedMembershipDomain::new(MembershipValues::int64([1, 2]), false),
            NullSemantics::NeverMatches,
            version,
        )
        .unwrap();
        let artifact = Arc::new(PhysicalArtifact::new_test(
            ArtifactKind::ValueSet,
            schema.digest(),
            version,
            false,
            encoded.into(),
        ));
        let bundle = ArtifactBundle::new(
            ChannelId::new(9),
            version,
            &join_profile(),
            vec![(ArtifactKind::ValueSet, artifact)],
            usize::MAX,
        )
        .unwrap();
        assert!(matches!(
            NativeRuntimeFilterPredicate::compile(
                &bundle,
                &contract_for(
                    ChannelId::new(9),
                    DataType::Int64,
                    NullSemantics::NeverMatches
                )
            ),
            Err(PredicateCompileError::MalformedArtifact(
                ArtifactCodecError::ContractViolation
            ))
        ));
    }

    #[test]
    fn bitset_and_bloom_are_rejected_outside_installed_join_profile() {
        let version = LogicalVersion::FIRST;
        let schema =
            ArtifactMembershipSchema::new(&DataType::Int64, NullSemantics::NeverMatches).unwrap();
        for (kind, hash) in [
            (ArtifactKind::Bitset, None),
            (ArtifactKind::Bloom, Some(HashContractDigest::new([3; 32]))),
        ] {
            let encoded = encode_physical_leaf(kind, &schema, version, false, hash, &[1]).unwrap();
            let artifact = Arc::new(PhysicalArtifact::new_test(
                kind,
                schema.digest(),
                version,
                false,
                encoded.into(),
            ));
            let profile = ConsumerArtifactProfile::new(BTreeSet::from([kind]), hash).unwrap();
            let bundle = ArtifactBundle::new(
                ChannelId::new(11),
                version,
                &profile,
                vec![(kind, artifact)],
                usize::MAX,
            )
            .unwrap();
            assert!(matches!(
                NativeRuntimeFilterPredicate::compile(
                    &bundle,
                    &contract_for(
                        ChannelId::new(11),
                        DataType::Int64,
                        NullSemantics::NeverMatches
                    )
                ),
                Err(PredicateCompileError::ProfileMismatch { .. })
            ));
        }
    }
}
