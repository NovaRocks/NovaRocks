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
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use arrow::datatypes::DataType;

use novarocks::runtime_filter_transition::materializer::codec::{
    ArtifactDecodeExpectations, decode_leaf, encode_physical_leaf,
};
use novarocks::runtime_filter_transition::model::contract::{
    BindingId, ChannelId, NullOrder, NullSemantics, OrderContract, OrderKeyContract, SortDirection,
    TopKSummaryRequirement,
};
use novarocks::runtime_filter_transition::port::artifact::{
    ArtifactBundle, ArtifactKind, ArtifactMembershipSchema, ConsumerArtifactProfile,
    PhysicalArtifact,
};
use novarocks::runtime_filter_transition::port::events::{
    RuntimeFilterEvent, RuntimeFilterEventSink,
};
use novarocks::runtime_filter_transition::port::final_domain::{
    CollectingFinalDomainTestIssuer, FinalDomainTestIssuerTransition, FrozenFinalDomainTestIssuer,
};
use novarocks::runtime_filter_transition::port::identity::{
    DeploymentEpoch, LogicalVersion, PartitionId, ProducerSequence, ProducerStreamId,
};
use novarocks::runtime_filter_transition::port::install::RuntimeFilterParticipantInstall;
use novarocks::runtime_filter_transition::port::ordered_bound::{
    COMPARATOR_ALGORITHM_VERSION, OrderedBoundUpdate, OrderedScalar, OrderedTuple,
    RuntimeOrderContract, comparator_digest_for_test,
};
use novarocks::runtime_filter_transition::port::producer::{
    FinalDomainProducerAdapter, InstallOutcome, OrderedBoundProducerAdapter, ProducerAdapter,
    ProducerHandle, ProducerPortKind, RuntimeContractViolation, RuntimeContractViolationKind,
    SubmitOutcome, TopKSummaryProducerAdapter,
};
use novarocks::runtime_filter_transition::port::subscription::{
    BlockingSnapshotSubscription, LivePollOutcome, LiveTerminal, NonBlockingLiveSubscription,
    SubscriptionHandle, SubscriptionKind, UnavailableReason,
};
use novarocks::runtime_filter_transition::port::support::{
    ArtifactRetainedBudget, MemoryAccountError, RuntimeFilterClock, RuntimeFilterMemoryAccount,
};
use novarocks::runtime_filter_transition::port::topk_summary::{
    RuntimeTopKSummaryContract, TopKSummary,
};
use novarocks::runtime_filter_transition::port::value_domain::{
    MembershipValues, ValueDomainDelta,
};
use novarocks::runtime_filter_transition::test_support::{
    CompiledRuntimeFilterServiceFixture, RuntimeFilterFixtureCoverage,
    compiled_fenced_final_fixture, compiled_live_final_domain_fixture,
    compiled_membership_service_fixture, compiled_ordered_bound_fixture, compiled_topk_fixture,
};
use novarocks_types::UniqueId;

use super::RuntimeFilterService;
use super::memory::MemTrackerMemoryAccount;

const CHANNEL: ChannelId = ChannelId::new(1);
const PRODUCER_A: BindingId = BindingId::new(10);
const PRODUCER_B: BindingId = BindingId::new(20);
const CONSUMER: BindingId = BindingId::new(30);
const INSTANCE_A: UniqueId = UniqueId::new(94, 10);
const INSTANCE_B: UniqueId = UniqueId::new(94, 20);

struct MembershipHarness {
    service: Arc<RuntimeFilterService>,
    blocking: Arc<dyn BlockingSnapshotSubscription>,
}

struct MembershipProducer {
    port: Arc<dyn ProducerAdapter>,
}

impl MembershipProducer {
    fn submit_values(
        &self,
        partition: u32,
        sequence: u64,
        values: impl IntoIterator<Item = i64>,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.port.submit(
            PartitionId::new(partition),
            ProducerSequence::new(sequence),
            ValueDomainDelta::new(MembershipValues::int64(values), false),
        )
    }

    fn close(
        &self,
        partition: u32,
        terminal_sequence: u64,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.port.close_partition(
            PartitionId::new(partition),
            ProducerSequence::new(terminal_sequence),
        )
    }
}

impl MembershipHarness {
    fn producer(&self, binding: BindingId, instance: UniqueId) -> MembershipProducer {
        let ProducerHandle::Membership(port) = self
            .service
            .open_producer(binding, instance, 1, ProducerPortKind::Membership)
            .expect("compiler-installed producer is authorized")
        else {
            panic!("membership graph must install only the Membership producer port")
        };
        MembershipProducer { port }
    }
}

struct DeterministicClock(Instant);

impl RuntimeFilterClock for DeterministicClock {
    fn now(&self) -> Instant {
        self.0
    }
}

#[derive(Default)]
struct RecordingEvents(Mutex<Vec<RuntimeFilterEvent>>);

impl RuntimeFilterEventSink for RecordingEvents {
    fn record(&self, event: RuntimeFilterEvent) {
        self.0.lock().unwrap().push(event);
    }
}

fn install_service(install: RuntimeFilterParticipantInstall) -> Arc<RuntimeFilterService> {
    install_service_with_memory(
        install,
        MemTrackerMemoryAccount::new_root_for_test("m4-conformance"),
    )
}

fn install_service_with_memory(
    install: RuntimeFilterParticipantInstall,
    memory: Arc<dyn RuntimeFilterMemoryAccount>,
) -> Arc<RuntimeFilterService> {
    let service = Arc::new(RuntimeFilterService::new_with_dependencies(
        UniqueId::new(94, 0),
        Arc::new(DeterministicClock(Instant::now())),
        Arc::new(RecordingEvents::default()),
        memory,
    ));
    assert_eq!(service.install(install).unwrap(), InstallOutcome::Installed);
    service
}

fn join_harness(coverage: RuntimeFilterFixtureCoverage) -> MembershipHarness {
    let fixture = compiled_membership_service_fixture(coverage);
    assert_eq!(fixture.channel_id(), CHANNEL);
    assert_eq!(fixture.producers().len(), 2);
    let producers = fixture.producers().to_vec();
    let consumer = fixture.consumer();
    let service = install_service(fixture.into_install());
    for producer in producers {
        let ProducerHandle::Membership(_) = service
            .open_producer(
                producer.binding_id(),
                producer.instance_id(),
                1,
                ProducerPortKind::Membership,
            )
            .expect("all scheduled producer instances open before execution")
        else {
            panic!("membership graph must install only Membership producer ports")
        };
    }
    let SubscriptionHandle::Blocking(blocking) = service
        .subscribe(
            consumer.binding_id(),
            consumer.instance_id(),
            SubscriptionKind::BlockingSnapshot,
        )
        .expect("compiler-installed blocking consumer is authorized")
    else {
        panic!("blocking graph consumer must install only BlockingSnapshot")
    };
    MembershipHarness { service, blocking }
}

fn join_allof_harness() -> MembershipHarness {
    join_harness(RuntimeFilterFixtureCoverage::AllOf)
}

fn join_anyof_harness() -> MembershipHarness {
    join_harness(RuntimeFilterFixtureCoverage::AnyOf)
}

fn publish_membership(
    harness: &MembershipHarness,
    binding: BindingId,
    instance: UniqueId,
    values: &[i64],
) {
    let producer = harness.producer(binding, instance);
    producer
        .submit_values(0, 0, values.iter().copied())
        .unwrap();
    producer.close(0, 1).unwrap();
}

fn membership_payload(artifact: &PhysicalArtifact) -> &[u8] {
    let bytes = artifact.canonical_bytes();
    assert_eq!(&bytes[..4], b"NRFL");
    let schema_len = u16::from_be_bytes(bytes[39..41].try_into().unwrap()) as usize;
    let mut cursor = 41 + schema_len;
    assert_eq!(
        LogicalVersion::new(u64::from_be_bytes(
            bytes[cursor..cursor + 8].try_into().unwrap()
        )),
        artifact.version()
    );
    cursor += 8;
    let flags = bytes[cursor];
    assert_eq!(flags & 1 != 0, artifact.contains_null());
    cursor += 1;
    assert_eq!(bytes[cursor], 0, "membership ValueSet has no hash contract");
    cursor += 1;
    let payload_len = u64::from_be_bytes(bytes[cursor..cursor + 8].try_into().unwrap()) as usize;
    cursor += 8;
    assert_eq!(cursor + payload_len, bytes.len());
    &bytes[cursor..]
}

fn assert_membership_values(bundle: &ArtifactBundle, expected: &[i64]) {
    let [(ArtifactKind::ValueSet, artifact)] = bundle.artifacts() else {
        panic!("non-empty Int64 membership must publish one ValueSet leaf")
    };
    let payload = membership_payload(artifact);
    assert_eq!(payload[0], 5, "canonical membership payload must be Int64");
    let count = u64::from_be_bytes(payload[1..9].try_into().unwrap()) as usize;
    assert_eq!(payload.len(), 9 + count * 8);
    let values = payload[9..]
        .chunks_exact(8)
        .map(|bytes| i64::from_be_bytes(bytes.try_into().unwrap()))
        .collect::<Vec<_>>();
    assert_eq!(values, expected);
}

fn membership_profile() -> ConsumerArtifactProfile {
    ConsumerArtifactProfile::new(
        BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
        None,
    )
    .unwrap()
}

fn assert_fixture_remote_equivalent(local: &ArtifactBundle) {
    let [(kind, local_leaf)] = local.artifacts() else {
        panic!("Join fixture publishes one physical membership leaf")
    };
    let profile = membership_profile();
    assert_eq!(local.profile_id(), profile.id());

    let schema =
        ArtifactMembershipSchema::new(&DataType::Int64, NullSemantics::NeverMatches).unwrap();
    let encoded = encode_physical_leaf(
        *kind,
        &schema,
        local_leaf.version(),
        local_leaf.contains_null(),
        None,
        membership_payload(local_leaf),
    )
    .unwrap();
    assert_eq!(encoded, local_leaf.canonical_bytes());

    let retained_bytes = PhysicalArtifact::accounted_resident_bytes(encoded.len()).unwrap();
    let decoded_memory: Arc<dyn RuntimeFilterMemoryAccount> =
        MemTrackerMemoryAccount::new_root_for_test("m4-fixture-remote-decode");
    let remote_leaf = decode_leaf(
        &encoded,
        ArtifactDecodeExpectations {
            expected_kind: *kind,
            expected_schema_digest: local_leaf.schema_digest(),
            expected_logical_version: local.version(),
            expected_hash_contract: None,
        },
        encoded.len(),
        Arc::new(ArtifactRetainedBudget::new(retained_bytes)),
        decoded_memory,
    )
    .unwrap();
    let remote = ArtifactBundle::new(
        local.channel_id(),
        local.version(),
        &profile,
        vec![(*kind, remote_leaf)],
        local.encoded_bytes(),
    )
    .unwrap();

    assert_eq!(local.artifacts()[0].0, remote.artifacts()[0].0);
    assert_eq!(local.profile_id(), remote.profile_id());
    assert_eq!(local.version(), remote.version());
    assert_eq!(local.canonical_digest(), remote.canonical_digest());
}

fn order_plan(
    direction: SortDirection,
    null_order: NullOrder,
) -> (OrderContract, Arc<RuntimeOrderContract>) {
    let keys = vec![OrderKeyContract {
        data_type: DataType::Int64,
        direction,
        null_order,
    }];
    let plan = OrderContract {
        comparator_digest: comparator_digest_for_test(&keys, COMPARATOR_ALGORITHM_VERSION),
        keys,
        inclusive: true,
    };
    let contract = Arc::new(RuntimeOrderContract::try_from_plan(&plan).unwrap());
    (plan, contract)
}

struct DirectTopNHarness {
    _service: Arc<RuntimeFilterService>,
    producer: Arc<dyn OrderedBoundProducerAdapter>,
    live: Arc<dyn NonBlockingLiveSubscription>,
}

fn direct_topn_harness(contract: Arc<RuntimeOrderContract>) -> DirectTopNHarness {
    let fixture = compiled_ordered_bound_fixture(&contract);
    let direct = fixture.producers()[0];
    let consumer = fixture.consumer();
    let service = install_service(fixture.into_install());
    let ProducerHandle::OrderedBound(producer) = service
        .open_producer(
            direct.binding_id(),
            direct.instance_id(),
            1,
            ProducerPortKind::OrderedBound,
        )
        .expect("compiler-installed direct TopN producer is authorized")
    else {
        panic!("direct TopN graph must install only the OrderedBound producer port")
    };
    let SubscriptionHandle::Live(live) = service
        .subscribe(
            consumer.binding_id(),
            consumer.instance_id(),
            SubscriptionKind::NonBlockingLive,
        )
        .expect("compiler-installed ordered consumer is authorized")
    else {
        panic!("ordered graph consumer must install only NonBlockingLive")
    };
    DirectTopNHarness {
        _service: service,
        producer,
        live,
    }
}

struct TopNHeapAdapter {
    k: usize,
    contract: Arc<RuntimeOrderContract>,
    producer: Arc<dyn OrderedBoundProducerAdapter>,
    candidates: Vec<OrderedTuple>,
    next_sequence: u64,
    published: Vec<LogicalVersion>,
}

impl TopNHeapAdapter {
    fn new(
        k: usize,
        contract: Arc<RuntimeOrderContract>,
        producer: Arc<dyn OrderedBoundProducerAdapter>,
    ) -> Self {
        assert!(k > 0, "TopN adapter requires a positive limit");
        Self {
            k,
            contract,
            producer,
            candidates: Vec::new(),
            next_sequence: 0,
            published: Vec::new(),
        }
    }

    fn push(
        &mut self,
        row: OrderedTuple,
    ) -> Result<Option<LogicalVersion>, RuntimeContractViolation> {
        let previous_kth = self.candidates.get(self.k - 1).cloned();
        self.candidates.push(row);
        self.candidates.sort_by(|left, right| {
            self.contract
                .compare(left, right)
                .expect("TopN candidates match their runtime order contract")
        });
        self.candidates.truncate(self.k);
        let Some(current_kth) = self.candidates.get(self.k - 1).cloned() else {
            return Ok(None);
        };
        if previous_kth.as_ref().is_some_and(|previous| {
            self.contract
                .compare(&current_kth, previous)
                .expect("TopN candidates match their runtime order contract")
                != Ordering::Less
        }) {
            return Ok(None);
        }

        let outcome = self.producer.submit_bound(
            PartitionId::new(0),
            ProducerSequence::new(self.next_sequence),
            OrderedBoundUpdate::new(&self.contract, current_kth)
                .expect("TopN kth tuple matches its runtime order contract"),
        )?;
        assert_eq!(
            outcome,
            SubmitOutcome::Published,
            "a genuinely tighter direct TopN bound must publish"
        );
        self.next_sequence += 1;
        let version = self
            .published
            .last()
            .copied()
            .and_then(LogicalVersion::checked_next)
            .unwrap_or(LogicalVersion::FIRST);
        self.published.push(version);
        Ok(Some(version))
    }

    fn published_versions(&self) -> &[LogicalVersion] {
        &self.published
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IndependentOrder {
    AscNullsLast,
    DescNullsFirst,
}

impl IndependentOrder {
    const fn direction(self) -> SortDirection {
        match self {
            Self::AscNullsLast => SortDirection::Ascending,
            Self::DescNullsFirst => SortDirection::Descending,
        }
    }

    const fn null_order(self) -> NullOrder {
        match self {
            Self::AscNullsLast => NullOrder::Last,
            Self::DescNullsFirst => NullOrder::First,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ExpectedBound(Option<i64>);

struct TopNCase {
    order: IndependentOrder,
    contract: Arc<RuntimeOrderContract>,
    raw_values: Vec<Option<i64>>,
    rows: Vec<OrderedTuple>,
    final_topn_values: Vec<Option<i64>>,
    expected_bounds: Vec<Option<ExpectedBound>>,
}

fn tuple(contract: &RuntimeOrderContract, value: Option<i64>) -> OrderedTuple {
    OrderedTuple::try_new(contract, [value.map(OrderedScalar::Int64)])
        .expect("finite TopN sample matches the Int64 order contract")
}

fn independent_ordering(
    order: IndependentOrder,
    left: &Option<i64>,
    right: &Option<i64>,
) -> Ordering {
    match order {
        IndependentOrder::AscNullsLast => match (left, right) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Greater,
            (Some(_), None) => Ordering::Less,
            (Some(left), Some(right)) => left.cmp(right),
        },
        IndependentOrder::DescNullsFirst => match (left, right) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (Some(left), Some(right)) => right.cmp(left),
        },
    }
}

fn independent_topn(order: IndependentOrder, rows: &[Option<i64>], k: usize) -> Vec<Option<i64>> {
    let mut ranked = rows.to_vec();
    ranked.sort_by(|left, right| independent_ordering(order, left, right));
    ranked.truncate(k);
    ranked
}

fn independent_publication_bounds(
    order: IndependentOrder,
    rows: &[Option<i64>],
    k: usize,
) -> Vec<Option<ExpectedBound>> {
    let mut prefix = Vec::with_capacity(rows.len());
    let mut previous_bound: Option<Option<i64>> = None;
    rows.iter()
        .map(|row| {
            prefix.push(*row);
            let ranked = independent_topn(order, &prefix, k);
            let Some(bound) = ranked.get(k - 1).copied() else {
                return None;
            };
            let publish = previous_bound.as_ref().is_none_or(|previous| {
                independent_ordering(order, &bound, previous) == Ordering::Less
            });
            previous_bound = Some(bound);
            publish.then_some(ExpectedBound(bound))
        })
        .collect()
}

fn topn_case(order: IndependentOrder, values: impl IntoIterator<Item = Option<i64>>) -> TopNCase {
    let (_, contract) = order_plan(order.direction(), order.null_order());
    let raw_values = values.into_iter().collect::<Vec<_>>();
    let rows = raw_values
        .iter()
        .copied()
        .map(|value| tuple(&contract, value))
        .collect::<Vec<_>>();
    let final_topn_values = independent_topn(order, &raw_values, 3);
    let expected_bounds = independent_publication_bounds(order, &raw_values, 3);
    TopNCase {
        order,
        contract,
        raw_values,
        rows,
        final_topn_values,
        expected_bounds,
    }
}

fn lcg_next(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *state
}

fn lcg_shuffle<T>(state: &mut u64, values: &mut [T]) {
    for upper in (1..values.len()).rev() {
        let index = (lcg_next(state) % (upper as u64 + 1)) as usize;
        values.swap(upper, index);
    }
}

fn topn_cases_with_fixed_seed() -> Vec<TopNCase> {
    let mut cases = vec![
        topn_case(
            IndependentOrder::AscNullsLast,
            [
                Some(30),
                Some(20),
                Some(10),
                Some(100),
                Some(30),
                Some(5),
                Some(20),
                Some(1),
                None,
            ],
        ),
        topn_case(
            IndependentOrder::DescNullsFirst,
            [
                Some(10),
                Some(20),
                Some(30),
                Some(-100),
                Some(10),
                Some(40),
                Some(20),
                Some(50),
                None,
            ],
        ),
    ];
    let mut state = 0x4d59_5df4_d0f3_3173_u64;
    for index in 0..64 {
        let base = ((lcg_next(&mut state) >> 32) % 20_000) as i64 - 10_000;
        let mode = index % 4;
        if index % 2 == 0 {
            let mut initial = vec![Some(base + 30), Some(base + 20), Some(base + 10)];
            lcg_shuffle(&mut state, &mut initial);
            let mut tail = vec![Some(base + 5), Some(base + 1)];
            match mode {
                0 => tail.extend([Some(base + 100), Some(base + 30)]),
                1 => tail.extend([None, Some(base + 100), Some(base + 20)]),
                2 => tail.extend([Some(base + 80), Some(base + 70)]),
                3 => tail.extend([None, None, Some(base + 10), Some(base + 90)]),
                _ => unreachable!(),
            }
            lcg_shuffle(&mut state, &mut tail);
            initial.extend(tail);
            cases.push(topn_case(IndependentOrder::AscNullsLast, initial));
        } else {
            let mut initial = vec![Some(base + 10), Some(base + 20), Some(base + 30)];
            lcg_shuffle(&mut state, &mut initial);
            let mut tail = vec![Some(base + 40), Some(base + 50)];
            match mode {
                0 => tail.extend([Some(base - 100), Some(base + 10)]),
                1 => tail.extend([None, Some(base - 100), Some(base + 20)]),
                2 => tail.extend([Some(base - 80), Some(base - 70)]),
                3 => tail.extend([None, None, Some(base + 30), Some(base - 90)]),
                _ => unreachable!(),
            }
            lcg_shuffle(&mut state, &mut tail);
            initial.extend(tail);
            cases.push(topn_case(IndependentOrder::DescNullsFirst, initial));
        }
    }
    assert_eq!(cases.len(), 66);
    cases
}

fn relative_value_pattern(values: &[Option<i64>]) -> Vec<Option<usize>> {
    let unique = values
        .iter()
        .flatten()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    values
        .iter()
        .map(|value| value.map(|value| unique.binary_search(&value).unwrap()))
        .collect()
}

fn published_expected_bounds(case: &TopNCase) -> Vec<Option<i64>> {
    case.expected_bounds
        .iter()
        .flatten()
        .map(|bound| bound.0)
        .collect()
}

fn assert_fixed_seed_case_diversity(cases: &[TopNCase]) {
    assert_eq!(cases.len(), 66, "two fixed plus 64 LCG-generated cases");
    assert_eq!(cases[0].final_topn_values, vec![Some(1), Some(5), Some(10)]);
    assert_eq!(
        published_expected_bounds(&cases[0]),
        vec![Some(30), Some(20), Some(10)]
    );
    assert_eq!(cases[1].final_topn_values, vec![None, Some(50), Some(40)]);
    assert_eq!(
        published_expected_bounds(&cases[1]),
        vec![Some(10), Some(20), Some(30), Some(40)]
    );

    let generated = &cases[2..];
    assert_eq!(generated.len(), 64);
    assert!(generated.iter().any(|case| case.raw_values.contains(&None)));
    assert!(
        generated
            .iter()
            .any(|case| !case.raw_values.contains(&None))
    );
    assert!(generated.iter().any(|case| {
        let mut seen = BTreeSet::new();
        case.raw_values.iter().any(|value| !seen.insert(*value))
    }));
    assert!(generated.iter().any(|case| {
        let mut seen = BTreeSet::new();
        case.raw_values.iter().all(|value| seen.insert(*value))
    }));
    assert!(generated.iter().all(|case| {
        case.expected_bounds
            .iter()
            .filter(|bound| bound.is_some())
            .count()
            >= 3
    }));

    let relative_patterns = generated
        .iter()
        .map(|case| relative_value_pattern(&case.raw_values))
        .collect::<BTreeSet<_>>();
    let tightening_cadences = generated
        .iter()
        .map(|case| {
            case.expected_bounds
                .iter()
                .enumerate()
                .filter_map(|(index, bound)| bound.map(|_| index))
                .collect::<Vec<_>>()
        })
        .collect::<BTreeSet<_>>();
    assert!(relative_patterns.len() >= 16);
    assert!(tightening_cadences.len() >= 8);
}

fn assert_bound_is_sound_for_final_topn(
    live: &Arc<dyn NonBlockingLiveSubscription>,
    version: LogicalVersion,
    order: IndependentOrder,
    expected_bound: ExpectedBound,
    final_topn: &[Option<i64>],
) -> Arc<ArtifactBundle> {
    let LivePollOutcome::Updated {
        bundle,
        terminal: None,
    } = live.poll_after(None)
    else {
        panic!("direct TopN tightening must be visible as a non-terminal live update")
    };
    assert_eq!(bundle.version(), version);
    let [(ArtifactKind::Range, artifact)] = bundle.artifacts() else {
        panic!("ordered TopN must materialize exactly one Range artifact")
    };
    let range = artifact.range().expect("Range leaf owns ordered data");
    let [actual_bound] = range.bound().values() else {
        panic!("TopN fixture expects a single-key Range bound")
    };
    let actual_bound = match actual_bound {
        None => None,
        Some(OrderedScalar::Int64(value)) => Some(*value),
        Some(_) => panic!("TopN fixture expects an Int64 Range bound"),
    };
    assert_eq!(actual_bound, expected_bound.0);
    let [key] = range.contract().keys() else {
        panic!("TopN fixture expects a single-key order contract")
    };
    assert_eq!(key.direction(), order.direction());
    assert_eq!(key.null_order(), order.null_order());
    for row in final_topn {
        assert!(
            independent_bound_survives(order, *row, actual_bound),
            "visible TopN bound must not eliminate a row in the final TopN"
        );
    }
    bundle
}

fn independent_bound_survives(
    order: IndependentOrder,
    row: Option<i64>,
    bound: Option<i64>,
) -> bool {
    match order {
        IndependentOrder::AscNullsLast => match (row, bound) {
            (_, None) => true,
            (None, Some(_)) => false,
            (Some(row), Some(bound)) => row <= bound,
        },
        IndependentOrder::DescNullsFirst => match (row, bound) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(row), Some(bound)) => row >= bound,
        },
    }
}

fn assert_immutable_version_history(
    observed: &[(Arc<ArtifactBundle>, LogicalVersion, [u8; 32])],
    expected_versions: &[LogicalVersion],
) {
    assert_eq!(observed.len(), expected_versions.len());
    assert!(observed.len() >= 2);
    for (record, expected) in observed.iter().zip(expected_versions) {
        assert_eq!(record.0.version(), record.1);
        assert_eq!(record.0.version(), *expected);
        assert_eq!(record.0.canonical_digest(), record.2);
    }
    for pair in observed.windows(2) {
        assert!(pair[0].1 < pair[1].1);
        assert!(!Arc::ptr_eq(&pair[0].0, &pair[1].0));
    }
}

struct TopKSummaryHarness {
    service: Arc<RuntimeFilterService>,
    contract: Arc<RuntimeTopKSummaryContract>,
    live: Arc<dyn NonBlockingLiveSubscription>,
}

impl TopKSummaryHarness {
    fn producer(
        &self,
        binding: BindingId,
        instance: UniqueId,
    ) -> Arc<dyn TopKSummaryProducerAdapter> {
        let ProducerHandle::TopKSummary(producer) = self
            .service
            .open_producer(binding, instance, 1, ProducerPortKind::TopKSummary)
            .expect("compiler-installed TopKSummary producer is authorized")
        else {
            panic!("TopKSummary graph must install only the TopKSummary producer port")
        };
        producer
    }

    fn submit_summary(
        &self,
        binding: BindingId,
        instance: UniqueId,
        values: &[i64],
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        let mut values = values.to_vec();
        values.sort_unstable();
        let candidates = values
            .iter()
            .map(|value| tuple(self.contract.order(), Some(*value)))
            .collect::<Vec<_>>();
        self.producer(binding, instance).submit_summary(
            PartitionId::new(0),
            ProducerSequence::new(0),
            TopKSummary::try_new(&self.contract, candidates)
                .expect("TopKSummary fixture constructs canonical candidates"),
        )
    }

    fn close_all(&self) -> Result<(), RuntimeContractViolation> {
        let visible_version = self
            .live
            .snapshot()
            .expect("TopKSummary bound is visible before close")
            .version();
        let first = self
            .producer(PRODUCER_A, INSTANCE_A)
            .close_partition(PartitionId::new(0), ProducerSequence::new(1))?;
        assert_ne!(first, SubmitOutcome::Completed);
        assert!(matches!(
            self.live.poll_after(Some(visible_version)),
            LivePollOutcome::Idle {
                latest_version: Some(latest),
                terminal: None,
            } if latest == visible_version
        ));
        let second = self
            .producer(PRODUCER_B, INSTANCE_B)
            .close_partition(PartitionId::new(0), ProducerSequence::new(1))?;
        assert_eq!(second, SubmitOutcome::Completed);
        Ok(())
    }
}

fn topk_allof_harness(k: u32) -> TopKSummaryHarness {
    let (plan, _) = order_plan(SortDirection::Ascending, NullOrder::Last);
    let requirement = TopKSummaryRequirement::try_new(k).expect("TopK requires positive K");
    let contract = Arc::new(
        RuntimeTopKSummaryContract::try_from_plan(&plan, requirement)
            .expect("TopKSummary plan contract is valid"),
    );
    let fixture = compiled_topk_fixture(contract.order(), requirement.k());
    let consumer = fixture.consumer();
    let service = install_service(fixture.into_install());
    let SubscriptionHandle::Live(live) = service
        .subscribe(
            consumer.binding_id(),
            consumer.instance_id(),
            SubscriptionKind::NonBlockingLive,
        )
        .expect("compiler-installed TopKSummary consumer is authorized")
    else {
        panic!("TopKSummary consumer must install only NonBlockingLive")
    };
    TopKSummaryHarness {
        service,
        contract,
        live,
    }
}

fn expect_live_update(outcome: LivePollOutcome) -> Arc<ArtifactBundle> {
    let LivePollOutcome::Updated {
        bundle,
        terminal: None,
    } = outcome
    else {
        panic!("expected a non-terminal live range update")
    };
    bundle
}

fn assert_sound_topk_bound(bundle: &ArtifactBundle, final_topk: &[i64]) {
    let [(ArtifactKind::Range, artifact)] = bundle.artifacts() else {
        panic!("TopKSummary must materialize exactly one Range artifact")
    };
    let range = artifact.range().expect("Range leaf owns ordered data");
    let [Some(OrderedScalar::Int64(actual_bound))] = range.bound().values() else {
        panic!("TopKSummary fixture expects one non-null Int64 bound")
    };
    let [key] = range.contract().keys() else {
        panic!("TopKSummary fixture expects a single-key order contract")
    };
    assert_eq!(key.direction(), SortDirection::Ascending);
    assert_eq!(key.null_order(), NullOrder::Last);
    assert_eq!(Some(actual_bound), final_topk.last());
    for value in final_topk {
        assert!(
            independent_bound_survives(
                IndependentOrder::AscNullsLast,
                Some(*value),
                Some(*actual_bound),
            ),
            "merged TopK bound must not eliminate a final TopK row"
        );
    }
}

fn assert_completed_without_new_unsound_version(
    live: &Arc<dyn NonBlockingLiveSubscription>,
    version: LogicalVersion,
) {
    assert!(matches!(
        live.poll_after(Some(version)),
        LivePollOutcome::Idle {
            latest_version: Some(latest),
            terminal: Some(LiveTerminal::Completed),
        } if latest == version
    ));
    assert_eq!(live.snapshot().unwrap().version(), version);
}

struct AggregateHarness {
    service: Arc<RuntimeFilterService>,
    producers: Vec<(BindingId, UniqueId, Arc<dyn FinalDomainProducerAdapter>)>,
    live: Arc<dyn NonBlockingLiveSubscription>,
}

impl AggregateHarness {
    fn only_producer_identity(&self) -> (BindingId, UniqueId) {
        let [(binding, instance, _)] = self.producers.as_slice() else {
            panic!("single-producer Aggregate fixture must install exactly one producer")
        };
        (*binding, *instance)
    }

    fn producer(
        &self,
        binding: BindingId,
        instance: UniqueId,
    ) -> &Arc<dyn FinalDomainProducerAdapter> {
        &self
            .producers
            .iter()
            .find(|(installed_binding, installed_instance, _)| {
                *installed_binding == binding && *installed_instance == instance
            })
            .expect("Aggregate fixture keeps every compiler-installed producer open")
            .2
    }

    fn collecting_issuer(
        &self,
        binding: BindingId,
        instance: UniqueId,
        open_drivers: u32,
    ) -> CollectingFinalDomainTestIssuer {
        self.service
            .final_domain_test_issuer(binding, instance, open_drivers)
            .expect("opened final-domain producer owns the test-only fence authority")
    }

    fn freeze(
        &self,
        binding: BindingId,
        instance: UniqueId,
        open_drivers: u32,
    ) -> FrozenFinalDomainTestIssuer {
        let mut transition = FinalDomainTestIssuerTransition::Collecting(self.collecting_issuer(
            binding,
            instance,
            open_drivers,
        ));
        loop {
            transition = match transition {
                FinalDomainTestIssuerTransition::Collecting(collecting) => {
                    collecting.close_driver()
                }
                FinalDomainTestIssuerTransition::Frozen(frozen) => return frozen,
            };
        }
    }

    fn complete(
        &self,
        binding: BindingId,
        instance: UniqueId,
        sequence: u64,
        issuer: &FrozenFinalDomainTestIssuer,
        values: &[i64],
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        let partition = PartitionId::new(0);
        let sequence = ProducerSequence::new(sequence);
        let shard = issuer
            .issue_shard(
                ProducerStreamId::new(binding, instance, partition),
                sequence,
                ValueDomainDelta::new(MembershipValues::int64(values.iter().copied()), false),
            )
            .expect("frozen Aggregate issuer signs only its installed producer stream");
        self.producer(binding, instance)
            .complete(partition, sequence, shard)
    }

    fn close(
        &self,
        binding: BindingId,
        instance: UniqueId,
        partition: u32,
        terminal_sequence: u64,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.producer(binding, instance).close_partition(
            PartitionId::new(partition),
            ProducerSequence::new(terminal_sequence),
        )
    }
}

fn aggregate_harness_with_memory(
    fixture: CompiledRuntimeFilterServiceFixture,
    memory: Arc<dyn RuntimeFilterMemoryAccount>,
) -> AggregateHarness {
    let producers = fixture.producers().to_vec();
    let consumer = fixture.consumer();
    let service = install_service_with_memory(fixture.into_install(), memory);
    let mut opened = Vec::with_capacity(producers.len());
    for producer in producers {
        let error = service
            .open_producer(
                producer.binding_id(),
                producer.instance_id(),
                1,
                ProducerPortKind::Membership,
            )
            .expect_err("fenced-final Aggregate graph must reject the Membership producer port");
        assert_eq!(
            error.kind(),
            RuntimeContractViolationKind::ProducerPortMismatch
        );
        let ProducerHandle::FinalDomain(handle) = service
            .open_producer(
                producer.binding_id(),
                producer.instance_id(),
                1,
                ProducerPortKind::FinalDomain,
            )
            .expect("compiler-installed Aggregate producer exposes FinalDomain")
        else {
            panic!("Aggregate graph must install only the FinalDomain producer port")
        };
        opened.push((producer.binding_id(), producer.instance_id(), handle));
    }
    let SubscriptionHandle::Live(live) = service
        .subscribe(
            consumer.binding_id(),
            consumer.instance_id(),
            SubscriptionKind::NonBlockingLive,
        )
        .expect("compiler-installed Aggregate consumer is authorized")
    else {
        panic!("Aggregate graph consumer must install only NonBlockingLive")
    };
    AggregateHarness {
        service,
        producers: opened,
        live,
    }
}

fn aggregate_allof_harness() -> AggregateHarness {
    aggregate_harness_with_memory(
        compiled_live_final_domain_fixture(),
        MemTrackerMemoryAccount::new_root_for_test("m4-aggregate-allof"),
    )
}

fn expect_collecting(
    transition: FinalDomainTestIssuerTransition,
) -> CollectingFinalDomainTestIssuer {
    let FinalDomainTestIssuerTransition::Collecting(collecting) = transition else {
        panic!("one local Aggregate driver must remain open")
    };
    collecting
}

fn expect_frozen(transition: FinalDomainTestIssuerTransition) -> FrozenFinalDomainTestIssuer {
    let FinalDomainTestIssuerTransition::Frozen(frozen) = transition else {
        panic!("Aggregate issuer freezes only after the last local driver closes")
    };
    frozen
}

fn expect_live_completed(outcome: LivePollOutcome) -> Arc<ArtifactBundle> {
    let LivePollOutcome::Updated {
        bundle,
        terminal: Some(LiveTerminal::Completed),
    } = outcome
    else {
        panic!("fenced-final AllOf must publish one terminal live artifact")
    };
    bundle
}

fn assert_explicit_empty_is_empty_domain() {
    let empty = aggregate_harness_with_memory(
        compiled_fenced_final_fixture(),
        MemTrackerMemoryAccount::new_root_for_test("m4-aggregate-explicit-empty"),
    );
    let (producer, instance) = empty.only_producer_identity();
    let frozen = empty.freeze(producer, instance, 1);
    empty.complete(producer, instance, 0, &frozen, &[]).unwrap();
    assert_eq!(
        empty.close(producer, instance, 0, 1).unwrap(),
        SubmitOutcome::Completed
    );
    let bundle = expect_live_completed(empty.live.poll_after(None));
    assert_eq!(bundle.version(), LogicalVersion::FIRST);
    let [(ArtifactKind::EmptyDomain, _)] = bundle.artifacts() else {
        panic!("explicit empty Aggregate shard must publish exactly one EmptyDomain artifact")
    };
}

struct RejectingMemoryAccount;

impl RuntimeFilterMemoryAccount for RejectingMemoryAccount {
    fn try_consume(&self, _bytes: usize) -> Result<(), MemoryAccountError> {
        Err(MemoryAccountError::CapacityExceeded)
    }

    fn release(&self, _bytes: usize) {}
}

fn assert_resource_failure_is_unavailable() {
    let unavailable = aggregate_harness_with_memory(
        compiled_fenced_final_fixture(),
        Arc::new(RejectingMemoryAccount),
    );
    let (producer, instance) = unavailable.only_producer_identity();
    let frozen = unavailable.freeze(producer, instance, 1);
    assert_eq!(
        unavailable
            .complete(producer, instance, 0, &frozen, &[])
            .unwrap(),
        SubmitOutcome::TerminalNoop
    );
    assert!(unavailable.live.snapshot().is_none());
    assert!(matches!(
        unavailable.live.poll_after(None),
        LivePollOutcome::Idle {
            latest_version: None,
            terminal: Some(LiveTerminal::Unavailable(UnavailableReason::ResourceLimit)),
        }
    ));
}

#[test]
fn m4_join_conformance_uses_compiler_produced_install_and_route_equivalent_artifacts() {
    let all_of = join_allof_harness();
    let first = all_of.producer(PRODUCER_A, INSTANCE_A);
    first.submit_values(0, 0, [1]).unwrap();
    first.close(0, 1).unwrap();
    assert!(all_of.blocking.snapshot().is_none());
    let second = all_of.producer(PRODUCER_B, INSTANCE_B);
    second.submit_values(0, 0, [2]).unwrap();
    second.close(0, 1).unwrap();
    let local = all_of
        .blocking
        .snapshot()
        .expect("AllOf publishes after both witnesses");
    assert_eq!(local.version(), LogicalVersion::FIRST);
    assert_membership_values(&local, &[1, 2]);
    assert_fixture_remote_equivalent(&local);

    let any_of = join_anyof_harness();
    publish_membership(&any_of, PRODUCER_A, INSTANCE_A, &[7]);
    let first = any_of
        .blocking
        .snapshot()
        .expect("first valid replica publishes");
    publish_membership(&any_of, PRODUCER_B, INSTANCE_B, &[9]);
    let after_late = any_of.blocking.snapshot().expect("winner remains visible");
    assert_eq!(first.version(), after_late.version());
    assert_eq!(first.canonical_digest(), after_late.canonical_digest());
}

#[test]
fn m4_direct_topn_conformance_delays_until_n_and_preserves_sound_monotonic_bounds() {
    let cases = topn_cases_with_fixed_seed();
    assert_fixed_seed_case_diversity(&cases);
    for case in cases {
        let harness = direct_topn_harness(case.contract.clone());
        let mut adapter = TopNHeapAdapter::new(3, case.contract.clone(), harness.producer.clone());
        assert!(case.expected_bounds[0].is_none());
        assert!(case.expected_bounds[1].is_none());
        assert!(adapter.push(case.rows[0].clone()).unwrap().is_none());
        assert!(adapter.push(case.rows[1].clone()).unwrap().is_none());
        assert!(matches!(
            harness.live.poll_after(None),
            LivePollOutcome::Idle { .. }
        ));
        let mut observed = Vec::new();
        for (index, row) in case.rows.into_iter().enumerate().skip(2) {
            let published = adapter.push(row).unwrap();
            assert_eq!(published.is_some(), case.expected_bounds[index].is_some());
            if let Some(version) = published {
                let bundle = assert_bound_is_sound_for_final_topn(
                    &harness.live,
                    version,
                    case.order,
                    case.expected_bounds[index]
                        .expect("an expected publication carries its independent oracle bound"),
                    &case.final_topn_values,
                );
                observed.push((bundle.clone(), bundle.version(), bundle.canonical_digest()));
            }
        }
        assert!(adapter.published_versions().len() >= 2);
        assert_immutable_version_history(&observed, adapter.published_versions());
    }
}

#[test]
fn m4_topk_summary_conformance_merges_incomplete_shards_only_after_allof() {
    let harness = topk_allof_harness(3);
    harness
        .submit_summary(PRODUCER_A, INSTANCE_A, &[1, 4])
        .unwrap();
    assert!(matches!(
        harness.live.poll_after(None),
        LivePollOutcome::Idle { .. }
    ));
    harness
        .submit_summary(PRODUCER_B, INSTANCE_B, &[2, 3])
        .unwrap();
    let first = expect_live_update(harness.live.poll_after(None));
    assert_sound_topk_bound(&first, &[1, 2, 3]);
    harness.close_all().unwrap();
    assert_completed_without_new_unsound_version(&harness.live, first.version());
}

#[test]
fn m4_aggregate_conformance_requires_frozen_allof_and_separates_empty_unavailable() {
    let aggregate = aggregate_allof_harness();
    let collecting = aggregate.collecting_issuer(PRODUCER_A, INSTANCE_A, 2);
    let collecting = expect_collecting(collecting.close_driver());
    assert!(aggregate.live.snapshot().is_none());
    let frozen_a = expect_frozen(collecting.close_driver());
    let frozen_b = aggregate.freeze(PRODUCER_B, INSTANCE_B, 1);
    aggregate
        .complete(PRODUCER_A, INSTANCE_A, 0, &frozen_a, &[1])
        .unwrap();
    aggregate.close(PRODUCER_A, INSTANCE_A, 0, 1).unwrap();
    assert!(aggregate.live.snapshot().is_none());
    aggregate
        .complete(PRODUCER_B, INSTANCE_B, 0, &frozen_b, &[2])
        .unwrap();
    aggregate.close(PRODUCER_B, INSTANCE_B, 0, 1).unwrap();
    let bundle = expect_live_completed(aggregate.live.poll_after(None));
    assert_eq!(bundle.version(), LogicalVersion::FIRST);
    assert_membership_values(&bundle, &[1, 2]);
    assert!(matches!(
        aggregate.live.poll_after(Some(bundle.version())),
        LivePollOutcome::Idle {
            latest_version: Some(LogicalVersion::FIRST),
            terminal: Some(LiveTerminal::Completed),
        }
    ));

    assert_explicit_empty_is_empty_domain();
    assert_resource_failure_is_unavailable();
}
