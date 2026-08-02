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

use std::sync::Arc;

use crate::runtime_filter::materializer::codec::{encode_range_leaf, encoded_range_leaf_len};
use crate::runtime_filter::port::artifact::{
    ArtifactBundle, ArtifactKind, ConsumerArtifactProfile, PhysicalArtifact, RangeArtifactData,
    RangeArtifactResidentLayout,
};
use crate::runtime_filter::port::producer::{
    RuntimeContractViolation, RuntimeContractViolationKind,
};
use crate::runtime_filter::port::support::{
    ArtifactRetainedBudget, ArtifactRetention, ArtifactScratchBudget, ArtifactScratchReservation,
    RuntimeFilterMemoryAccount,
};
use crate::runtime_filter::port::value_domain::LogicalSnapshot;

#[derive(Debug)]
pub enum RangeMaterializationOutcome {
    Published(Arc<ArtifactBundle>),
    ContractViolation(RuntimeContractViolation),
    ResourceUnavailable,
    MaterializationFailed,
}

pub struct RangeMaterializationPlan<'a> {
    snapshot: Arc<LogicalSnapshot>,
    profile: &'a ConsumerArtifactProfile,
    max_artifact_bytes: usize,
    leaf_encoded_bytes: usize,
    resident_layout: RangeArtifactResidentLayout,
}

pub struct AdmittedRangeMaterialization<'a> {
    plan: RangeMaterializationPlan<'a>,
    _scratch: ArtifactScratchReservation,
    artifact_footprint: usize,
    total_footprint: usize,
    retained: Arc<ArtifactRetention>,
}

pub struct RangeMaterializer;

impl RangeMaterializer {
    pub fn plan<'a>(
        snapshot: Arc<LogicalSnapshot>,
        profile: &'a ConsumerArtifactProfile,
        max_artifact_bytes: usize,
    ) -> Result<RangeMaterializationPlan<'a>, RangeMaterializationOutcome> {
        let domain = snapshot
            .ordered_bound()
            .ok_or_else(|| range_contract_violation("Range plan requires an ordered snapshot"))?;
        if profile.accepted_kinds() != &std::collections::BTreeSet::from([ArtifactKind::Range])
            || profile.order_contract_digest() != Some(domain.contract().digest())
        {
            return Err(range_contract_violation(
                "Range plan profile does not match the ordered snapshot contract",
            ));
        }
        domain
            .contract()
            .compare(domain.bound(), domain.bound())
            .map_err(|_| {
                range_contract_violation("Range plan bound violates its order contract")
            })?;
        let leaf_encoded_bytes = encoded_range_leaf_len(domain.contract(), domain.bound())
            .map_err(classify_range_plan_codec_error)?;
        let bundle_bytes =
            ArtifactBundle::canonical_encoded_len_for_single_artifact(leaf_encoded_bytes)
                .map_err(|_| RangeMaterializationOutcome::ResourceUnavailable)?;
        if bundle_bytes > max_artifact_bytes {
            return Err(RangeMaterializationOutcome::ResourceUnavailable);
        }
        let resident_layout =
            RangeArtifactResidentLayout::from_data(domain.contract(), domain.bound())
                .map_err(|_| RangeMaterializationOutcome::ResourceUnavailable)?;
        Ok(RangeMaterializationPlan {
            snapshot,
            profile,
            max_artifact_bytes,
            leaf_encoded_bytes,
            resident_layout,
        })
    }

    pub fn admit<'a>(
        plan: RangeMaterializationPlan<'a>,
        retained_budget: Arc<ArtifactRetainedBudget>,
        scratch_budget: Arc<ArtifactScratchBudget>,
        memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
    ) -> Result<AdmittedRangeMaterialization<'a>, RangeMaterializationOutcome> {
        let domain = plan.snapshot.ordered_bound().ok_or_else(|| {
            range_contract_violation("Range admission requires an ordered snapshot")
        })?;
        let actual_layout =
            RangeArtifactResidentLayout::from_data(domain.contract(), domain.bound())
                .map_err(|_| RangeMaterializationOutcome::ResourceUnavailable)?;
        if plan.profile.order_contract_digest() != Some(domain.contract().digest())
            || actual_layout != plan.resident_layout
        {
            return Err(range_contract_violation(
                "Range admission plan no longer matches trusted ordered metadata",
            ));
        }
        let scratch_bytes = plan.leaf_encoded_bytes;
        let scratch = ArtifactScratchReservation::try_new(
            scratch_bytes,
            scratch_budget,
            memory_account.clone(),
        )
        .map_err(|_| RangeMaterializationOutcome::ResourceUnavailable)?;
        let artifact_footprint =
            PhysicalArtifact::accounted_range_resident_component_bytes_for_layout(
                plan.leaf_encoded_bytes,
                plan.resident_layout,
            )
            .map_err(|_| RangeMaterializationOutcome::ResourceUnavailable)?;
        let bundle_footprint = ArtifactBundle::accounted_range_resident_overhead(1)
            .map_err(|_| RangeMaterializationOutcome::ResourceUnavailable)?;
        let total_footprint = artifact_footprint
            .checked_add(bundle_footprint)
            .ok_or(RangeMaterializationOutcome::ResourceUnavailable)?;
        let retained = Arc::new(
            ArtifactRetention::try_new(total_footprint, retained_budget, memory_account)
                .map_err(|_| RangeMaterializationOutcome::ResourceUnavailable)?,
        );
        Ok(AdmittedRangeMaterialization {
            plan,
            _scratch: scratch,
            artifact_footprint,
            total_footprint,
            retained,
        })
    }

    pub fn encode(
        admitted: AdmittedRangeMaterialization<'_>,
    ) -> Result<Arc<ArtifactBundle>, RangeMaterializationOutcome> {
        let domain =
            admitted.plan.snapshot.ordered_bound().ok_or_else(|| {
                range_contract_violation("Range encode requires an ordered snapshot")
            })?;
        let encoded = encode_range_leaf(
            domain.contract(),
            domain.bound(),
            admitted.plan.snapshot.version(),
        )
        .map_err(classify_range_encode_codec_error)?;
        if encoded.len() != admitted.plan.leaf_encoded_bytes {
            return Err(RangeMaterializationOutcome::MaterializationFailed);
        }
        let data = RangeArtifactData::new(
            domain.contract().clone(),
            domain.bound().clone(),
            admitted.plan.snapshot.version(),
        )
        .map_err(|_| range_contract_violation("Range encode payload violates trusted metadata"))?;
        let artifact = Arc::new(
            PhysicalArtifact::from_range_shared_retained(
                admitted.plan.snapshot.version(),
                data,
                encoded.into(),
                admitted.artifact_footprint,
                admitted.total_footprint,
                admitted.retained.clone(),
            )
            .map_err(|_| RangeMaterializationOutcome::MaterializationFailed)?,
        );
        let bundle = ArtifactBundle::new_retained(
            admitted.plan.snapshot.channel_id(),
            admitted.plan.snapshot.version(),
            admitted.plan.profile,
            vec![(ArtifactKind::Range, artifact)],
            admitted.plan.max_artifact_bytes,
            admitted.retained,
        )
        .map_err(|_| RangeMaterializationOutcome::MaterializationFailed)?;
        Ok(Arc::new(bundle))
    }

    pub fn materialize(
        snapshot: Arc<LogicalSnapshot>,
        profile: &ConsumerArtifactProfile,
        max_artifact_bytes: usize,
        retained_budget: Arc<ArtifactRetainedBudget>,
        scratch_budget: Arc<ArtifactScratchBudget>,
        memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
    ) -> RangeMaterializationOutcome {
        let plan = match Self::plan(snapshot, profile, max_artifact_bytes) {
            Ok(plan) => plan,
            Err(outcome) => return outcome,
        };
        let admitted = match Self::admit(plan, retained_budget, scratch_budget, memory_account) {
            Ok(admitted) => admitted,
            Err(outcome) => return outcome,
        };
        match Self::encode(admitted) {
            Ok(bundle) => RangeMaterializationOutcome::Published(bundle),
            Err(outcome) => outcome,
        }
    }
}

fn range_contract_violation(detail: &'static str) -> RangeMaterializationOutcome {
    RangeMaterializationOutcome::ContractViolation(RuntimeContractViolation::new(
        RuntimeContractViolationKind::OrderedContractMismatch,
        detail,
    ))
}

fn classify_range_plan_codec_error(
    error: crate::runtime_filter::materializer::codec::ArtifactCodecError,
) -> RangeMaterializationOutcome {
    match error {
        crate::runtime_filter::materializer::codec::ArtifactCodecError::ContractViolation => {
            range_contract_violation("Range plan codec rejected trusted ordered metadata")
        }
        _ => RangeMaterializationOutcome::ResourceUnavailable,
    }
}

fn classify_range_encode_codec_error(
    error: crate::runtime_filter::materializer::codec::ArtifactCodecError,
) -> RangeMaterializationOutcome {
    match error {
        crate::runtime_filter::materializer::codec::ArtifactCodecError::ContractViolation => {
            range_contract_violation("Range encode codec rejected trusted ordered metadata")
        }
        crate::runtime_filter::materializer::codec::ArtifactCodecError::ResourceUnavailable
        | crate::runtime_filter::materializer::codec::ArtifactCodecError::ResourceLimit => {
            RangeMaterializationOutcome::ResourceUnavailable
        }
        _ => RangeMaterializationOutcome::MaterializationFailed,
    }
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::datatypes::{DataType, TimeUnit};

    use crate::runtime_filter::model::contract::{
        ChannelId, NullOrder, OrderContract, OrderKeyContract, SortDirection,
    };
    use crate::runtime_filter::port::artifact::{
        ArtifactBundle, ArtifactKind, ConsumerArtifactProfile, PhysicalArtifact, RangeArtifactData,
    };
    use crate::runtime_filter::port::identity::LogicalVersion;
    use crate::runtime_filter::port::ordered_bound::{
        COMPARATOR_ALGORITHM_VERSION, ComparatorDigestV1, OrderedScalar, OrderedTuple,
        RuntimeOrderContract, RuntimeOrderKey,
    };
    use crate::runtime_filter::port::producer::RuntimeContractViolationKind;
    use crate::runtime_filter::port::support::{
        ArtifactRetainedBudget, ArtifactRetention, ArtifactScratchBudget, MemoryAccountError,
        RetainedMemoryReservation, RuntimeFilterMemoryAccount,
    };
    use crate::runtime_filter::port::value_domain::OrderedBoundDomain;
    use crate::runtime_filter::port::value_domain::{
        LogicalSnapshot, MembershipValues, ReducedMembershipDomain,
    };

    use super::{RangeMaterializationOutcome, RangeMaterializer};

    #[derive(Default)]
    struct CountingMemory(AtomicUsize);

    impl RuntimeFilterMemoryAccount for CountingMemory {
        fn try_consume(&self, bytes: usize) -> Result<(), MemoryAccountError> {
            self.0.fetch_add(bytes, Ordering::SeqCst);
            Ok(())
        }

        fn release(&self, bytes: usize) {
            self.0.fetch_sub(bytes, Ordering::SeqCst);
        }
    }

    struct Fixture {
        snapshot: Arc<LogicalSnapshot>,
        profile: ConsumerArtifactProfile,
        retained: Arc<ArtifactRetainedBudget>,
        scratch: Arc<ArtifactScratchBudget>,
        memory: Arc<CountingMemory>,
    }

    fn fixture() -> Fixture {
        let keys = vec![
            OrderKeyContract {
                data_type: DataType::Int64,
                direction: SortDirection::Ascending,
                null_order: NullOrder::Last,
            },
            OrderKeyContract {
                data_type: DataType::Utf8,
                direction: SortDirection::Descending,
                null_order: NullOrder::First,
            },
        ];
        let comparator =
            ComparatorDigestV1::for_contract(&keys, COMPARATOR_ALGORITHM_VERSION).unwrap();
        let contract = Arc::new(
            RuntimeOrderContract::try_from_plan(&OrderContract {
                keys,
                inclusive: true,
                comparator_digest: comparator,
            })
            .unwrap(),
        );
        let bound = OrderedTuple::try_new(
            &contract,
            [
                Some(OrderedScalar::Int64(42)),
                Some(OrderedScalar::Utf8("deterministic".into())),
            ],
        )
        .unwrap();
        let snapshot = Arc::new(LogicalSnapshot::ordered(
            ChannelId::new(7),
            LogicalVersion::new(3),
            Arc::new(OrderedBoundDomain::new(contract.clone(), bound)),
            RetainedMemoryReservation::empty(),
        ));
        Fixture {
            profile: ConsumerArtifactProfile::new_ordered_range(contract.digest()).unwrap(),
            snapshot,
            retained: Arc::new(ArtifactRetainedBudget::new(1 << 20)),
            scratch: Arc::new(ArtifactScratchBudget::new(1 << 20, 1 << 20).unwrap()),
            memory: Arc::new(CountingMemory::default()),
        }
    }

    #[test]
    fn range_materialization_is_deterministic_and_preserves_the_typed_payload() {
        let first = fixture();
        let second = fixture();
        let first_bundle = RangeMaterializer::materialize(
            first.snapshot,
            &first.profile,
            usize::MAX,
            first.retained,
            first.scratch,
            first.memory,
        );
        let second_bundle = RangeMaterializer::materialize(
            second.snapshot,
            &second.profile,
            usize::MAX,
            second.retained,
            second.scratch,
            second.memory,
        );
        let (
            RangeMaterializationOutcome::Published(first_bundle),
            RangeMaterializationOutcome::Published(second_bundle),
        ) = (first_bundle, second_bundle)
        else {
            panic!("valid Range fixtures must materialize")
        };
        assert_eq!(
            first_bundle.canonical_digest(),
            second_bundle.canonical_digest()
        );
        assert_eq!(first_bundle.artifacts()[0].0, ArtifactKind::Range);
        assert_eq!(
            first_bundle.artifacts()[0].1.range().unwrap().bound(),
            second_bundle.artifacts()[0].1.range().unwrap().bound()
        );
        assert_eq!(
            first_bundle.artifacts()[0]
                .1
                .range()
                .unwrap()
                .semantic_digest(),
            second_bundle.artifacts()[0]
                .1
                .range()
                .unwrap()
                .semantic_digest()
        );
    }

    #[test]
    fn range_materialization_budget_failures_leave_zero_retained_bytes() {
        let fixture = fixture();
        let retained = Arc::new(ArtifactRetainedBudget::new(1));
        let outcome = RangeMaterializer::materialize(
            fixture.snapshot,
            &fixture.profile,
            usize::MAX,
            retained.clone(),
            fixture.scratch.clone(),
            fixture.memory.clone(),
        );
        assert!(matches!(
            outcome,
            RangeMaterializationOutcome::ResourceUnavailable
        ));
        assert_eq!(retained.retained_bytes(), 0);
        assert_eq!(fixture.scratch.retained_bytes(), 0);
        assert_eq!(fixture.memory.0.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn order_mismatch_allocates_zero_and_failed_scratch_admission_rolls_back() {
        let fixture = fixture();
        let wrong_profile = ConsumerArtifactProfile::new_ordered_range(
            crate::runtime_filter::port::ordered_bound::OrderContractDigest::from_bytes_for_codec(
                [0x5a; 32],
            ),
        )
        .unwrap();
        let RangeMaterializationOutcome::ContractViolation(error) = RangeMaterializer::materialize(
            fixture.snapshot.clone(),
            &wrong_profile,
            usize::MAX,
            fixture.retained.clone(),
            fixture.scratch.clone(),
            fixture.memory.clone(),
        ) else {
            panic!("order digest mismatch must remain a trusted contract violation");
        };
        assert_eq!(
            error.kind(),
            RuntimeContractViolationKind::OrderedContractMismatch
        );
        assert_eq!(fixture.retained.retained_bytes(), 0);
        assert_eq!(fixture.scratch.retained_bytes(), 0);
        assert_eq!(fixture.memory.0.load(Ordering::SeqCst), 0);

        let plan = RangeMaterializer::plan(fixture.snapshot, &fixture.profile, usize::MAX).unwrap();
        let scratch = Arc::new(ArtifactScratchBudget::new(1, 1).unwrap());
        assert!(matches!(
            RangeMaterializer::admit(
                plan,
                fixture.retained.clone(),
                scratch.clone(),
                fixture.memory.clone(),
            ),
            Err(RangeMaterializationOutcome::ResourceUnavailable)
        ));
        assert_eq!(fixture.retained.retained_bytes(), 0);
        assert_eq!(scratch.retained_bytes(), 0);
        assert_eq!(fixture.memory.0.load(Ordering::SeqCst), 0);
    }

    fn fixture_for_types(
        data: Vec<(DataType, Option<OrderedScalar>)>,
        version: LogicalVersion,
    ) -> Fixture {
        let keys = data
            .iter()
            .map(|(data_type, _)| OrderKeyContract {
                data_type: data_type.clone(),
                direction: SortDirection::Ascending,
                null_order: NullOrder::Last,
            })
            .collect::<Vec<_>>();
        let comparator =
            ComparatorDigestV1::for_contract(&keys, COMPARATOR_ALGORITHM_VERSION).unwrap();
        let contract = Arc::new(
            RuntimeOrderContract::try_from_plan(&OrderContract {
                keys,
                inclusive: true,
                comparator_digest: comparator,
            })
            .unwrap(),
        );
        let bound =
            OrderedTuple::try_new(&contract, data.into_iter().map(|(_, value)| value)).unwrap();
        let snapshot = Arc::new(LogicalSnapshot::ordered(
            ChannelId::new(7),
            version,
            Arc::new(OrderedBoundDomain::new(contract.clone(), bound)),
            RetainedMemoryReservation::empty(),
        ));
        Fixture {
            profile: ConsumerArtifactProfile::new_ordered_range(contract.digest()).unwrap(),
            snapshot,
            retained: Arc::new(ArtifactRetainedBudget::new(1 << 20)),
            scratch: Arc::new(ArtifactScratchBudget::new(1 << 20, 1 << 20).unwrap()),
            memory: Arc::new(CountingMemory::default()),
        }
    }

    fn exact_range_graph_bytes(fixture: &Fixture, encoded_bytes: usize) -> usize {
        let domain = fixture.snapshot.ordered_bound().unwrap();
        let arc_header = 2 * size_of::<usize>();
        let timezone_allocations = domain
            .contract()
            .keys()
            .iter()
            .filter_map(|key| match key.data_type() {
                DataType::Timestamp(_, Some(timezone)) => Some(arc_header + timezone.len()),
                _ => None,
            })
            .sum::<usize>();
        let utf8_allocations = domain
            .bound()
            .values()
            .iter()
            .filter_map(|value| match value {
                Some(OrderedScalar::Utf8(value)) => Some(arc_header + value.len()),
                _ => None,
            })
            .sum::<usize>();
        let data = arc_header
            + size_of::<RangeArtifactData>()
            + arc_header
            + size_of::<RuntimeOrderContract>()
            + arc_header
            + domain.contract().keys().len() * size_of::<RuntimeOrderKey>()
            + timezone_allocations
            + arc_header
            + domain.bound().values().len() * size_of::<Option<OrderedScalar>>()
            + utf8_allocations;
        let artifact =
            arc_header + size_of::<PhysicalArtifact>() + arc_header + encoded_bytes + data;
        let bundle = arc_header
            + size_of::<ArtifactBundle>()
            + size_of::<(ArtifactKind, Arc<PhysicalArtifact>)>()
            + arc_header
            + size_of::<ArtifactRetention>();
        artifact + bundle
    }

    fn assert_exact_retained_boundary(fixture: Fixture) {
        let plan = RangeMaterializer::plan(fixture.snapshot.clone(), &fixture.profile, usize::MAX)
            .unwrap();
        let expected = exact_range_graph_bytes(&fixture, plan.leaf_encoded_bytes);
        let admitted = RangeMaterializer::admit(
            plan,
            fixture.retained.clone(),
            fixture.scratch.clone(),
            fixture.memory.clone(),
        )
        .unwrap();
        assert_eq!(admitted.total_footprint, expected);
        drop(admitted);
        assert_eq!(fixture.retained.retained_bytes(), 0);
        assert_eq!(fixture.memory.0.load(Ordering::SeqCst), 0);

        let exact = Arc::new(ArtifactRetainedBudget::new(expected));
        let outcome = RangeMaterializer::materialize(
            fixture.snapshot.clone(),
            &fixture.profile,
            usize::MAX,
            exact.clone(),
            fixture.scratch.clone(),
            fixture.memory.clone(),
        );
        let RangeMaterializationOutcome::Published(bundle) = outcome else {
            panic!("the exact retained ownership budget must succeed")
        };
        assert_eq!(exact.retained_bytes(), expected);
        drop(bundle);
        assert_eq!(exact.retained_bytes(), 0);
        assert_eq!(fixture.memory.0.load(Ordering::SeqCst), 0);

        let one_under = Arc::new(ArtifactRetainedBudget::new(expected - 1));
        assert!(matches!(
            RangeMaterializer::materialize(
                fixture.snapshot,
                &fixture.profile,
                usize::MAX,
                one_under.clone(),
                fixture.scratch,
                fixture.memory.clone(),
            ),
            RangeMaterializationOutcome::ResourceUnavailable
        ));
        assert_eq!(one_under.retained_bytes(), 0);
        assert_eq!(fixture.memory.0.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn fixed_width_multi_key_range_uses_exact_retained_ownership_boundary() {
        assert_exact_retained_boundary(fixture_for_types(
            vec![
                (DataType::Int64, Some(OrderedScalar::Int64(7))),
                (DataType::Int32, Some(OrderedScalar::Int32(3))),
                (
                    DataType::Decimal128(18, 2),
                    Some(OrderedScalar::Decimal128(1234)),
                ),
            ],
            LogicalVersion::FIRST,
        ));
    }

    #[test]
    fn utf8_and_timestamp_timezone_range_uses_exact_retained_ownership_boundary() {
        assert_exact_retained_boundary(fixture_for_types(
            vec![
                (
                    DataType::Utf8,
                    Some(OrderedScalar::Utf8("owned-bound".into())),
                ),
                (
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("Asia/Shanghai".into())),
                    Some(OrderedScalar::Timestamp(42)),
                ),
            ],
            LogicalVersion::FIRST,
        ));
    }

    #[test]
    fn range_semantic_digest_includes_logical_version() {
        let first = fixture_for_types(
            vec![(DataType::Int64, Some(OrderedScalar::Int64(7)))],
            LogicalVersion::FIRST,
        );
        let second = fixture_for_types(
            vec![(DataType::Int64, Some(OrderedScalar::Int64(7)))],
            LogicalVersion::new(2),
        );
        let materialize = |fixture: Fixture| {
            let RangeMaterializationOutcome::Published(bundle) = RangeMaterializer::materialize(
                fixture.snapshot,
                &fixture.profile,
                usize::MAX,
                fixture.retained,
                fixture.scratch,
                fixture.memory,
            ) else {
                panic!("valid range fixture must materialize")
            };
            bundle.artifacts()[0].1.range().unwrap().semantic_digest()
        };
        assert_ne!(materialize(first), materialize(second));
    }

    #[test]
    fn trusted_range_mismatches_remain_query_fatal_across_plan_admit_and_encode() {
        let fixture = fixture();
        let wrong_profile = ConsumerArtifactProfile::new_ordered_range(
            crate::runtime_filter::port::ordered_bound::OrderContractDigest::from_bytes_for_codec(
                [9; 32],
            ),
        )
        .unwrap();
        let Err(RangeMaterializationOutcome::ContractViolation(plan_error)) =
            RangeMaterializer::plan(fixture.snapshot.clone(), &wrong_profile, usize::MAX)
        else {
            panic!("trusted plan mismatch must remain typed")
        };
        assert_eq!(
            plan_error.kind(),
            RuntimeContractViolationKind::OrderedContractMismatch
        );

        let mut plan =
            RangeMaterializer::plan(fixture.snapshot.clone(), &fixture.profile, usize::MAX)
                .unwrap();
        plan.resident_layout.key_count += 1;
        let Err(RangeMaterializationOutcome::ContractViolation(admit_error)) =
            RangeMaterializer::admit(
                plan,
                fixture.retained.clone(),
                fixture.scratch.clone(),
                fixture.memory.clone(),
            )
        else {
            panic!("trusted admission mismatch must remain typed")
        };
        assert_eq!(
            admit_error.kind(),
            RuntimeContractViolationKind::OrderedContractMismatch
        );
        assert_eq!(fixture.retained.retained_bytes(), 0);
        assert_eq!(fixture.scratch.retained_bytes(), 0);
        assert_eq!(fixture.memory.0.load(Ordering::SeqCst), 0);

        let plan = RangeMaterializer::plan(fixture.snapshot.clone(), &fixture.profile, usize::MAX)
            .unwrap();
        let mut admitted = RangeMaterializer::admit(
            plan,
            fixture.retained.clone(),
            fixture.scratch.clone(),
            fixture.memory.clone(),
        )
        .unwrap();
        admitted.plan.snapshot = Arc::new(LogicalSnapshot::first(
            ChannelId::new(7),
            ReducedMembershipDomain::new(MembershipValues::int64([7]), false),
            RetainedMemoryReservation::empty(),
        ));
        let RangeMaterializationOutcome::ContractViolation(encode_error) =
            RangeMaterializer::encode(admitted).unwrap_err()
        else {
            panic!("trusted encode mismatch must remain typed")
        };
        assert_eq!(
            encode_error.kind(),
            RuntimeContractViolationKind::OrderedContractMismatch
        );
        assert_eq!(fixture.retained.retained_bytes(), 0);
        assert_eq!(fixture.scratch.retained_bytes(), 0);
        assert_eq!(fixture.memory.0.load(Ordering::SeqCst), 0);
    }
}
