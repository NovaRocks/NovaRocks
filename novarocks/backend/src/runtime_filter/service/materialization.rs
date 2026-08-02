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

use std::collections::BTreeMap;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};

use novarocks::runtime_filter_transition::model::contract::ChannelId;
use novarocks::runtime_filter_transition::port::artifact::{ArtifactBundle, ConsumerProfileId};
use novarocks::runtime_filter_transition::port::identity::{DeploymentEpoch, LogicalVersion};
use novarocks::runtime_filter_transition::port::producer::{
    RuntimeContractViolation, RuntimeContractViolationKind,
};
use novarocks::runtime_filter_transition::port::subscription::ArtifactDeliveryOutcome;

use novarocks::runtime_filter_transition::materializer::range::{
    AdmittedRangeMaterialization, RangeMaterializationOutcome, RangeMaterializer,
};
use novarocks::runtime_filter_transition::materializer::{
    AdmittedMaterialization, MaterializationAdmission, MaterializationOutcome, Materializer,
    UnavailableReason as MaterializerUnavailableReason,
    UnsupportedReason as MaterializerUnsupportedReason,
};
use novarocks::runtime_filter_transition::port::events::{
    ArtifactMaterializationIdentity, RuntimeFilterEvent,
};
use novarocks::runtime_filter_transition::port::subscription::{
    ArtifactUnsupportedReason, UnavailableReason,
};
use novarocks::runtime_filter_transition::port::support::RuntimeFilterMemoryAccount;
use novarocks::runtime_filter_transition::port::value_domain::LogicalSnapshot;

use super::registry::{CapabilityGroup, ChannelArtifactPlan};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct ArtifactPublishKey {
    channel_id: ChannelId,
    epoch: DeploymentEpoch,
    profile_id: ConsumerProfileId,
}

impl ArtifactPublishKey {
    pub(super) const fn new(
        channel_id: ChannelId,
        epoch: DeploymentEpoch,
        profile_id: ConsumerProfileId,
    ) -> Self {
        Self {
            channel_id,
            epoch,
            profile_id,
        }
    }

    pub(super) const fn channel_id(self) -> ChannelId {
        self.channel_id
    }

    pub(super) const fn profile_id(self) -> ConsumerProfileId {
        self.profile_id
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PublishCommitOutcome {
    Published,
    Stale,
    Idempotent,
    Cancelled,
}

#[derive(Default)]
struct KeyState {
    generation: u64,
    cancelled: bool,
    latest: Option<(LogicalVersion, ArtifactDeliveryOutcome)>,
    in_flight: BTreeMap<(LogicalVersion, u64), Arc<JobFlight>>,
}

#[derive(Default)]
struct GateState {
    keys: BTreeMap<ArtifactPublishKey, KeyState>,
}

#[derive(Default)]
struct JobFlight {
    outcome: Mutex<Option<ArtifactDeliveryOutcome>>,
    changed: Condvar,
    #[cfg(test)]
    followers: AtomicUsize,
}

impl JobFlight {
    fn completed(outcome: ArtifactDeliveryOutcome) -> Arc<Self> {
        Arc::new(Self {
            outcome: Mutex::new(Some(outcome)),
            changed: Condvar::new(),
            #[cfg(test)]
            followers: AtomicUsize::new(0),
        })
    }

    fn finish(&self, outcome: ArtifactDeliveryOutcome) {
        let mut state = self
            .outcome
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.is_none() {
            *state = Some(outcome);
            self.changed.notify_all();
        }
    }

    fn wait(&self) -> ArtifactDeliveryOutcome {
        let mut state = self
            .outcome
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        while state.is_none() {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(|error| error.into_inner());
        }
        state.as_ref().expect("completed artifact flight").clone()
    }
}

#[derive(Clone, Default)]
pub(super) struct ArtifactPublishGate {
    state: Arc<Mutex<GateState>>,
}

pub(super) enum ArtifactJobClaim {
    Owner(ArtifactJobOwner),
    Follower(ArtifactJobFollower),
    Stale,
}

pub(super) struct ArtifactJobFollower {
    flight: Arc<JobFlight>,
}

impl ArtifactJobFollower {
    pub(super) fn wait(self) -> ArtifactDeliveryOutcome {
        self.flight.wait()
    }
}

pub(super) struct ArtifactJobOwner {
    gate: Weak<Mutex<GateState>>,
    key: ArtifactPublishKey,
    version: LogicalVersion,
    generation: u64,
    flight: Arc<JobFlight>,
    finished: bool,
}

impl ArtifactJobOwner {
    pub(super) fn finish(
        mut self,
        outcome: ArtifactDeliveryOutcome,
    ) -> Result<PublishCommitOutcome, RuntimeContractViolation> {
        self.finished = true;
        let Some(state) = self.gate.upgrade() else {
            self.flight.finish(ArtifactDeliveryOutcome::Cancelled);
            return Ok(PublishCommitOutcome::Cancelled);
        };
        finish_job(
            &state,
            self.key,
            self.version,
            self.generation,
            &self.flight,
            outcome,
            |_, _| {},
        )
    }

    pub(super) fn finish_after_delivery(
        mut self,
        outcome: ArtifactDeliveryOutcome,
        deliver: impl FnOnce(PublishCommitOutcome, &ArtifactDeliveryOutcome),
    ) -> Result<PublishCommitOutcome, RuntimeContractViolation> {
        self.finished = true;
        let Some(state) = self.gate.upgrade() else {
            self.flight.finish(ArtifactDeliveryOutcome::Cancelled);
            return Ok(PublishCommitOutcome::Cancelled);
        };
        finish_job(
            &state,
            self.key,
            self.version,
            self.generation,
            &self.flight,
            outcome,
            deliver,
        )
    }
}

impl Drop for ArtifactJobOwner {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let Some(state) = self.gate.upgrade() else {
            self.flight.finish(ArtifactDeliveryOutcome::Cancelled);
            return;
        };
        let _ = finish_job(
            &state,
            self.key,
            self.version,
            self.generation,
            &self.flight,
            ArtifactDeliveryOutcome::Cancelled,
            |_, _| {},
        );
    }
}

impl ArtifactPublishGate {
    pub(super) fn generation(&self, key: ArtifactPublishKey) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .keys
            .entry(key)
            .or_default()
            .generation
    }

    pub(super) fn claim(
        &self,
        key: ArtifactPublishKey,
        version: LogicalVersion,
    ) -> ArtifactJobClaim {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let key_state = state.keys.entry(key).or_default();
        if key_state.cancelled {
            return ArtifactJobClaim::Follower(ArtifactJobFollower {
                flight: JobFlight::completed(ArtifactDeliveryOutcome::Cancelled),
            });
        }
        let generation = key_state.generation;
        if let Some(flight) = key_state.in_flight.get(&(version, generation)) {
            #[cfg(test)]
            flight.followers.fetch_add(1, Ordering::SeqCst);
            return ArtifactJobClaim::Follower(ArtifactJobFollower {
                flight: flight.clone(),
            });
        }
        if let Some((latest_version, outcome)) = &key_state.latest {
            if version < *latest_version {
                return ArtifactJobClaim::Stale;
            }
            if version == *latest_version {
                return ArtifactJobClaim::Follower(ArtifactJobFollower {
                    flight: JobFlight::completed(outcome.clone()),
                });
            }
        }
        let flight = Arc::new(JobFlight::default());
        key_state
            .in_flight
            .insert((version, generation), flight.clone());
        ArtifactJobClaim::Owner(ArtifactJobOwner {
            gate: Arc::downgrade(&self.state),
            key,
            version,
            generation,
            flight,
            finished: false,
        })
    }

    #[cfg(test)]
    pub(super) fn in_flight_follower_count(
        &self,
        key: ArtifactPublishKey,
        version: LogicalVersion,
    ) -> usize {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let Some(key_state) = state.keys.get(&key) else {
            return 0;
        };
        let Some(flight) = key_state.in_flight.get(&(version, key_state.generation)) else {
            return 0;
        };
        flight.followers.load(Ordering::SeqCst)
    }

    pub(super) fn commit_published(
        &self,
        key: ArtifactPublishKey,
        generation: u64,
        bundle: Arc<ArtifactBundle>,
    ) -> Result<PublishCommitOutcome, RuntimeContractViolation> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        commit_locked(
            state.keys.entry(key).or_default(),
            generation,
            bundle.version(),
            ArtifactDeliveryOutcome::Published(bundle),
        )
    }

    pub(super) fn cancel(&self, key: ArtifactPublishKey) -> bool {
        self.cancel_all([key]).contains(&key)
    }

    pub(super) fn cancel_all(
        &self,
        keys: impl IntoIterator<Item = ArtifactPublishKey>,
    ) -> Vec<ArtifactPublishKey> {
        let (newly_terminalized, flights) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            let mut newly_terminalized = Vec::new();
            let mut flights = Vec::new();
            for key in keys {
                let key_state = state.keys.entry(key).or_default();
                if key_state.cancelled {
                    continue;
                }
                if key_state.latest.is_none() {
                    newly_terminalized.push(key);
                }
                key_state.generation = key_state.generation.checked_add(1).unwrap_or(u64::MAX);
                key_state.cancelled = true;
                flights.extend(std::mem::take(&mut key_state.in_flight).into_values());
            }
            (newly_terminalized, flights)
        };
        for flight in flights {
            flight.finish(ArtifactDeliveryOutcome::Cancelled);
        }
        newly_terminalized
    }

    pub(super) fn cancel_channel(&self, channel_id: ChannelId, epoch: DeploymentEpoch) {
        let keys = {
            let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state
                .keys
                .keys()
                .copied()
                .filter(|key| key.channel_id == channel_id && key.epoch == epoch)
                .collect::<Vec<_>>()
        };
        self.cancel_all(keys);
    }
}

fn finish_job(
    state: &Arc<Mutex<GateState>>,
    key: ArtifactPublishKey,
    version: LogicalVersion,
    generation: u64,
    flight: &Arc<JobFlight>,
    outcome: ArtifactDeliveryOutcome,
    before_notify: impl FnOnce(PublishCommitOutcome, &ArtifactDeliveryOutcome),
) -> Result<PublishCommitOutcome, RuntimeContractViolation> {
    let result = {
        let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
        let key_state = state.keys.entry(key).or_default();
        let active = key_state
            .in_flight
            .get(&(version, generation))
            .is_some_and(|active| Arc::ptr_eq(active, flight));
        if !active || key_state.cancelled || key_state.generation != generation {
            Ok((
                PublishCommitOutcome::Cancelled,
                ArtifactDeliveryOutcome::Cancelled,
            ))
        } else {
            commit_locked(key_state, generation, version, outcome.clone()).map(|decision| {
                let follower = if decision == PublishCommitOutcome::Cancelled {
                    ArtifactDeliveryOutcome::Cancelled
                } else {
                    outcome
                };
                (decision, follower)
            })
        }
    };
    match result {
        Ok((decision, follower_outcome)) => {
            let delivery = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                before_notify(decision, &follower_outcome);
            }));
            let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
            let key_state = state.keys.entry(key).or_default();
            if key_state
                .in_flight
                .get(&(version, generation))
                .is_some_and(|active| Arc::ptr_eq(active, flight))
            {
                key_state.in_flight.remove(&(version, generation));
            }
            drop(state);
            flight.finish(follower_outcome);
            if let Err(payload) = delivery {
                std::panic::resume_unwind(payload);
            }
            Ok(decision)
        }
        Err(error) => {
            let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
            let key_state = state.keys.entry(key).or_default();
            if key_state
                .in_flight
                .get(&(version, generation))
                .is_some_and(|active| Arc::ptr_eq(active, flight))
            {
                key_state.in_flight.remove(&(version, generation));
            }
            drop(state);
            flight.finish(ArtifactDeliveryOutcome::Cancelled);
            Err(error)
        }
    }
}

fn commit_locked(
    state: &mut KeyState,
    generation: u64,
    version: LogicalVersion,
    outcome: ArtifactDeliveryOutcome,
) -> Result<PublishCommitOutcome, RuntimeContractViolation> {
    if state.cancelled || state.generation != generation {
        return Ok(PublishCommitOutcome::Cancelled);
    }
    if let Some((latest_version, latest_outcome)) = &state.latest {
        if version < *latest_version {
            return Ok(PublishCommitOutcome::Stale);
        }
        if version == *latest_version {
            if outcomes_identical(latest_outcome, &outcome) {
                return Ok(PublishCommitOutcome::Idempotent);
            }
            return Err(RuntimeContractViolation::new(
                RuntimeContractViolationKind::ConflictingArtifactPublish,
                "same artifact profile version carried a different terminal outcome",
            ));
        }
    }
    state.latest = Some((version, outcome));
    Ok(PublishCommitOutcome::Published)
}

fn outcomes_identical(left: &ArtifactDeliveryOutcome, right: &ArtifactDeliveryOutcome) -> bool {
    match (left, right) {
        (ArtifactDeliveryOutcome::Published(left), ArtifactDeliveryOutcome::Published(right)) => {
            left.canonical_digest() == right.canonical_digest()
        }
        (
            ArtifactDeliveryOutcome::Unsupported(left),
            ArtifactDeliveryOutcome::Unsupported(right),
        ) => left == right,
        (
            ArtifactDeliveryOutcome::Unavailable(left),
            ArtifactDeliveryOutcome::Unavailable(right),
        ) => left == right,
        (ArtifactDeliveryOutcome::Cancelled, ArtifactDeliveryOutcome::Cancelled) => true,
        _ => false,
    }
}

pub(super) enum MaterializationWorkClaim {
    Owner(ArtifactJobOwner),
    Follower,
    Stale,
}

pub(super) struct MaterializationWorkResult {
    pub(super) group: CapabilityGroup,
    pub(super) claim: MaterializationWorkClaim,
    pub(super) outcome: Option<ArtifactDeliveryOutcome>,
    pub(super) events: Vec<RuntimeFilterEvent>,
    pub(super) contract_violation: Option<RuntimeContractViolation>,
}

pub(super) enum ClaimedMaterializationJob {
    Owner {
        group: CapabilityGroup,
        owner: ArtifactJobOwner,
        launch_event: Option<RuntimeFilterEvent>,
    },
    Follower {
        group: CapabilityGroup,
        follower: ArtifactJobFollower,
    },
    Stale {
        group: CapabilityGroup,
    },
}

pub(super) fn claim_materialization_jobs(
    plan: &ChannelArtifactPlan,
    gate: &ArtifactPublishGate,
    version: LogicalVersion,
) -> Vec<ClaimedMaterializationJob> {
    plan.groups()
        .iter()
        .map(|group| match gate.claim(group.key(), version) {
            ArtifactJobClaim::Owner(owner) => ClaimedMaterializationJob::Owner {
                group: group.clone(),
                owner,
                launch_event: Some(RuntimeFilterEvent::MaterializationStarted {
                    identity: ArtifactMaterializationIdentity::new(
                        group.common(),
                        group.profile().id(),
                        version,
                    ),
                }),
            },
            ArtifactJobClaim::Follower(follower) => ClaimedMaterializationJob::Follower {
                group: group.clone(),
                follower,
            },
            ArtifactJobClaim::Stale => ClaimedMaterializationJob::Stale {
                group: group.clone(),
            },
        })
        .collect()
}

pub(super) fn take_materialization_launch_events(
    claimed: &mut [ClaimedMaterializationJob],
) -> Vec<RuntimeFilterEvent> {
    claimed
        .iter_mut()
        .filter_map(|claim| match claim {
            ClaimedMaterializationJob::Owner { launch_event, .. } => launch_event.take(),
            ClaimedMaterializationJob::Follower { .. }
            | ClaimedMaterializationJob::Stale { .. } => None,
        })
        .collect()
}

pub(super) fn run_materialization_jobs(
    plan: &ChannelArtifactPlan,
    gate: &ArtifactPublishGate,
    snapshot: &Arc<LogicalSnapshot>,
    memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
    before_encode: Option<Arc<dyn Fn(ConsumerProfileId) + Send + Sync>>,
    after_encode: Option<Arc<dyn Fn(ConsumerProfileId) + Send + Sync>>,
) -> Vec<MaterializationWorkResult> {
    let claimed = claim_materialization_jobs(plan, gate, snapshot.version());
    execute_materialization_jobs(
        plan,
        snapshot,
        memory_account,
        before_encode,
        after_encode,
        claimed,
    )
}

pub(super) fn execute_materialization_jobs(
    plan: &ChannelArtifactPlan,
    snapshot: &Arc<LogicalSnapshot>,
    memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
    before_encode: Option<Arc<dyn Fn(ConsumerProfileId) + Send + Sync>>,
    after_encode: Option<Arc<dyn Fn(ConsumerProfileId) + Send + Sync>>,
    claimed: Vec<ClaimedMaterializationJob>,
) -> Vec<MaterializationWorkResult> {
    if claimed.is_empty() {
        return Vec::new();
    }
    let batch_size = plan.max_concurrent_jobs().min(claimed.len()).max(1);
    let mut results = Vec::with_capacity(claimed.len());
    let mut claimed = claimed.into_iter();
    loop {
        let batch = claimed.by_ref().take(batch_size).collect::<Vec<_>>();
        if batch.is_empty() {
            break;
        }
        // Plan and reserve in canonical profile order. This makes scarce-budget winners
        // deterministic while keeping the expensive codec phase concurrent.
        let admitted = batch
            .into_iter()
            .map(|claim| admit_claimed_group(plan, snapshot, claim, memory_account.clone()))
            .collect::<Vec<_>>();
        let mut batch_results = std::thread::scope(|scope| {
            let mut jobs = Vec::with_capacity(admitted.len());
            for work in admitted {
                match work {
                    AdmittedGroup::Complete(result) => jobs.push(ScopedJob::Complete(result)),
                    AdmittedGroup::Follower { group, follower } => {
                        jobs.push(ScopedJob::Running(scope.spawn(move || {
                            MaterializationWorkResult {
                                group,
                                claim: MaterializationWorkClaim::Follower,
                                outcome: Some(follower.wait()),
                                events: Vec::new(),
                                contract_violation: None,
                            }
                        })));
                    }
                    AdmittedGroup::Ready {
                        group,
                        owner,
                        identity,
                        admitted,
                        events,
                    } => {
                        let before_encode = before_encode.clone();
                        let after_encode = after_encode.clone();
                        jobs.push(ScopedJob::Running(scope.spawn(move || {
                            let outcome = std::panic::catch_unwind(
                                std::panic::AssertUnwindSafe(|| {
                                    if let Some(hook) = before_encode {
                                        hook(group.profile().id());
                                    }
                                    match admitted {
                                        AdmittedArtifactMaterialization::Membership(admitted) => {
                                            Materializer::encode(admitted).map_or_else(
                                                |_| EncodeDispatch::Complete(
                                                    MaterializationOutcome::Unavailable(
                                                        MaterializerUnavailableReason::MaterializationFailed,
                                                    ),
                                                ),
                                                EncodeDispatch::Published,
                                            )
                                        }
                                        AdmittedArtifactMaterialization::Range(admitted) => {
                                            match RangeMaterializer::encode(admitted) {
                                                Ok(bundle) => EncodeDispatch::Published(bundle),
                                                Err(RangeMaterializationOutcome::ContractViolation(
                                                    violation,
                                                )) => EncodeDispatch::ContractViolation(violation),
                                                Err(RangeMaterializationOutcome::ResourceUnavailable) => {
                                                    EncodeDispatch::Complete(
                                                        MaterializationOutcome::Unavailable(
                                                            MaterializerUnavailableReason::ResourceLimit,
                                                        ),
                                                    )
                                                }
                                                Err(_) => EncodeDispatch::Complete(
                                                    MaterializationOutcome::Unavailable(
                                                        MaterializerUnavailableReason::MaterializationFailed,
                                                    ),
                                                ),
                                            }
                                        }
                                    }
                                }),
                            )
                            .unwrap_or(EncodeDispatch::Complete(
                                MaterializationOutcome::Unavailable(
                                    MaterializerUnavailableReason::MaterializationFailed,
                                ),
                            ));
                            if let Some(hook) = after_encode {
                                hook(group.profile().id());
                            }
                            match outcome {
                                EncodeDispatch::Published(bundle) => complete_group(
                                    group,
                                    owner,
                                    identity,
                                    events,
                                    MaterializationOutcome::Published(bundle),
                                ),
                                EncodeDispatch::Complete(outcome) => {
                                    complete_group(group, owner, identity, events, outcome)
                                }
                                EncodeDispatch::ContractViolation(violation) => {
                                    drop(owner);
                                    MaterializationWorkResult {
                                        group,
                                        claim: MaterializationWorkClaim::Stale,
                                        outcome: None,
                                        events,
                                        contract_violation: Some(violation),
                                    }
                                }
                            }
                        })));
                    }
                }
            }
            jobs.into_iter()
                .map(|job| match job {
                    ScopedJob::Complete(result) => result,
                    ScopedJob::Running(handle) => handle
                        .join()
                        .expect("materialization encode worker catches every panic"),
                })
                .collect::<Vec<_>>()
        });
        results.append(&mut batch_results);
    }
    results
}

enum AdmittedGroup<'a> {
    Ready {
        group: CapabilityGroup,
        owner: ArtifactJobOwner,
        identity: ArtifactMaterializationIdentity,
        admitted: AdmittedArtifactMaterialization<'a>,
        events: Vec<RuntimeFilterEvent>,
    },
    Follower {
        group: CapabilityGroup,
        follower: ArtifactJobFollower,
    },
    Complete(MaterializationWorkResult),
}

enum AdmittedArtifactMaterialization<'a> {
    Membership(AdmittedMaterialization<'a>),
    Range(AdmittedRangeMaterialization<'a>),
}

enum AdmissionDispatch<'a> {
    Ready(AdmittedArtifactMaterialization<'a>),
    Complete(MaterializationOutcome),
}

enum EncodeDispatch {
    Published(Arc<ArtifactBundle>),
    Complete(MaterializationOutcome),
    ContractViolation(RuntimeContractViolation),
}

enum ScopedJob<'scope> {
    Complete(MaterializationWorkResult),
    Running(std::thread::ScopedJoinHandle<'scope, MaterializationWorkResult>),
}

fn admit_claimed_group<'a>(
    plan: &'a ChannelArtifactPlan,
    snapshot: &Arc<LogicalSnapshot>,
    claim: ClaimedMaterializationJob,
    memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
) -> AdmittedGroup<'a> {
    match claim {
        ClaimedMaterializationJob::Stale { group } => {
            AdmittedGroup::Complete(MaterializationWorkResult {
                group,
                claim: MaterializationWorkClaim::Stale,
                outcome: None,
                events: Vec::new(),
                contract_violation: None,
            })
        }
        ClaimedMaterializationJob::Follower { group, follower } => {
            AdmittedGroup::Follower { group, follower }
        }
        ClaimedMaterializationJob::Owner {
            group,
            owner,
            launch_event,
        } => {
            let planned_group = plan
                .groups()
                .iter()
                .find(|planned| planned.key() == group.key())
                .expect("claimed materialization group remains in its channel plan");
            let identity = ArtifactMaterializationIdentity::new(
                group.common(),
                group.profile().id(),
                snapshot.version(),
            );
            let events = launch_event.into_iter().collect();
            let admitted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if snapshot.ordered_bound().is_some() {
                    let range_plan = match RangeMaterializer::plan(
                        snapshot.clone(),
                        planned_group.profile(),
                        plan.max_artifact_bytes(),
                    ) {
                        Ok(plan) => plan,
                        Err(RangeMaterializationOutcome::ResourceUnavailable) => {
                            return Ok(AdmissionDispatch::Complete(
                                MaterializationOutcome::Unavailable(
                                    MaterializerUnavailableReason::ResourceLimit,
                                ),
                            ));
                        }
                        Err(RangeMaterializationOutcome::ContractViolation(violation)) => {
                            return Err(Some(violation));
                        }
                        Err(_) => return Err(None),
                    };
                    return match RangeMaterializer::admit(
                        range_plan,
                        plan.retained_budget(),
                        plan.scratch_budget(),
                        memory_account,
                    ) {
                        Ok(admitted) => Ok(AdmissionDispatch::Ready(
                            AdmittedArtifactMaterialization::Range(admitted),
                        )),
                        Err(RangeMaterializationOutcome::ResourceUnavailable) => Ok(
                            AdmissionDispatch::Complete(MaterializationOutcome::Unavailable(
                                MaterializerUnavailableReason::ResourceLimit,
                            )),
                        ),
                        Err(RangeMaterializationOutcome::ContractViolation(violation)) => {
                            Err(Some(violation))
                        }
                        Err(_) => Err(None),
                    };
                }
                let schema = plan.schema().ok_or(None)?;
                let materialization_plan = Materializer::plan(
                    snapshot.clone(),
                    schema,
                    planned_group.profile(),
                    plan.policy(),
                    plan.max_artifact_bytes(),
                )
                .map_err(|_| None)?;
                match Materializer::admit(
                    materialization_plan,
                    plan.retained_budget(),
                    plan.scratch_budget(),
                    memory_account,
                ) {
                    MaterializationAdmission::Ready(admitted) => Ok(AdmissionDispatch::Ready(
                        AdmittedArtifactMaterialization::Membership(admitted),
                    )),
                    MaterializationAdmission::Complete(outcome) => {
                        Ok(AdmissionDispatch::Complete(outcome))
                    }
                }
            }));
            match admitted {
                Ok(Ok(AdmissionDispatch::Ready(admitted))) => AdmittedGroup::Ready {
                    group,
                    owner,
                    identity,
                    admitted,
                    events,
                },
                Ok(Ok(AdmissionDispatch::Complete(outcome))) => AdmittedGroup::Complete(
                    complete_group(group.clone(), owner, identity, events, outcome),
                ),
                Ok(Err(Some(violation))) => {
                    drop(owner);
                    AdmittedGroup::Complete(MaterializationWorkResult {
                        group,
                        claim: MaterializationWorkClaim::Stale,
                        outcome: None,
                        events,
                        contract_violation: Some(violation),
                    })
                }
                Ok(Err(None)) | Err(_) => AdmittedGroup::Complete(complete_group(
                    group.clone(),
                    owner,
                    identity,
                    events,
                    MaterializationOutcome::Unavailable(
                        MaterializerUnavailableReason::MaterializationFailed,
                    ),
                )),
            }
        }
    }
}

fn complete_group(
    group: CapabilityGroup,
    owner: ArtifactJobOwner,
    identity: ArtifactMaterializationIdentity,
    mut events: Vec<RuntimeFilterEvent>,
    outcome: MaterializationOutcome,
) -> MaterializationWorkResult {
    let outcome = match outcome {
        MaterializationOutcome::Published(bundle) => {
            let (kind, _) = bundle
                .artifacts()
                .first()
                .expect("materialized bundle is non-empty");
            events.push(RuntimeFilterEvent::ArtifactMaterialized {
                identity,
                kind: *kind,
                bytes: bundle.encoded_bytes(),
                digest: bundle.canonical_digest(),
            });
            ArtifactDeliveryOutcome::Published(bundle)
        }
        MaterializationOutcome::Unsupported(reason) => {
            let reason = match reason {
                MaterializerUnsupportedReason::NoAcceptedRepresentation => {
                    ArtifactUnsupportedReason::NoAcceptedRepresentation
                }
            };
            events.push(RuntimeFilterEvent::ArtifactUnsupported { identity, reason });
            ArtifactDeliveryOutcome::Unsupported(reason)
        }
        MaterializationOutcome::Unavailable(MaterializerUnavailableReason::ResourceLimit) => {
            events.push(RuntimeFilterEvent::ArtifactUnavailable {
                identity,
                reason: UnavailableReason::ResourceLimit,
            });
            ArtifactDeliveryOutcome::Unavailable(UnavailableReason::ResourceLimit)
        }
        MaterializationOutcome::Unavailable(
            MaterializerUnavailableReason::MaterializationFailed,
        ) => {
            events.push(RuntimeFilterEvent::ArtifactUnavailable {
                identity,
                reason: UnavailableReason::MaterializationFailed,
            });
            ArtifactDeliveryOutcome::Unavailable(UnavailableReason::MaterializationFailed)
        }
    };
    MaterializationWorkResult {
        group,
        claim: MaterializationWorkClaim::Owner(owner),
        outcome: Some(outcome),
        events,
        contract_violation: None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex, mpsc};
    use std::time::Duration;

    use arrow::datatypes::DataType;

    use novarocks::runtime_filter_transition::model::contract::{ChannelId, NullSemantics};
    use novarocks::runtime_filter_transition::port::artifact::{
        ArtifactBundle, ArtifactKind, ArtifactSchemaDigest, ConsumerArtifactProfile,
        ConsumerProfileId, PhysicalArtifact,
    };
    use novarocks::runtime_filter_transition::port::identity::{DeploymentEpoch, LogicalVersion};
    use novarocks::runtime_filter_transition::port::producer::RuntimeContractViolationKind;
    use novarocks::runtime_filter_transition::port::subscription::{
        ArtifactDeliveryOutcome, ArtifactUnsupportedReason, UnavailableReason,
    };
    use novarocks::runtime_filter_transition::port::support::{
        ArtifactRetainedBudget, ArtifactRetention, MemoryAccountError, RuntimeFilterMemoryAccount,
    };

    use super::{ArtifactJobClaim, ArtifactPublishGate, ArtifactPublishKey, PublishCommitOutcome};

    fn bundle(version: LogicalVersion, byte: u8) -> Arc<ArtifactBundle> {
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        let schema =
            ArtifactSchemaDigest::for_membership(&DataType::Int64, NullSemantics::NeverMatches)
                .unwrap();
        let artifact = Arc::new(PhysicalArtifact::new_test(
            ArtifactKind::ValueSet,
            schema,
            version,
            false,
            Arc::from([byte]),
        ));
        Arc::new(
            ArtifactBundle::new(
                ChannelId::new(1),
                version,
                &profile,
                vec![(ArtifactKind::ValueSet, artifact)],
                usize::MAX,
            )
            .unwrap(),
        )
    }

    fn key(profile: u8) -> ArtifactPublishKey {
        ArtifactPublishKey::new(
            ChannelId::new(1),
            DeploymentEpoch::new(2),
            ConsumerProfileId::for_test([profile; 32]),
        )
    }

    #[derive(Default)]
    struct CountingMemory {
        current: AtomicUsize,
    }

    impl RuntimeFilterMemoryAccount for CountingMemory {
        fn try_consume(&self, bytes: usize) -> Result<(), MemoryAccountError> {
            self.current.fetch_add(bytes, Ordering::SeqCst);
            Ok(())
        }

        fn release(&self, bytes: usize) {
            let previous = self.current.fetch_sub(bytes, Ordering::SeqCst);
            assert!(previous >= bytes);
        }
    }

    fn retained_bundle(
        version: LogicalVersion,
        byte: u8,
        retained_budget: Arc<ArtifactRetainedBudget>,
        account: Arc<CountingMemory>,
    ) -> Arc<ArtifactBundle> {
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        let schema =
            ArtifactSchemaDigest::for_membership(&DataType::Int64, NullSemantics::NeverMatches)
                .unwrap();
        let encoded: Arc<[u8]> = Arc::from([byte]);
        let component_bytes =
            PhysicalArtifact::accounted_resident_component_bytes(encoded.len()).unwrap();
        let retained_bytes = ArtifactBundle::accounted_resident_overhead(&profile, 1)
            .unwrap()
            .checked_add(component_bytes)
            .unwrap();
        let retention =
            Arc::new(ArtifactRetention::try_new(retained_bytes, retained_budget, account).unwrap());
        let artifact = Arc::new(
            PhysicalArtifact::from_shared_retained_bytes(
                ArtifactKind::ValueSet,
                schema,
                version,
                false,
                encoded,
                component_bytes,
                retained_bytes,
                retention.clone(),
            )
            .unwrap(),
        );
        Arc::new(
            ArtifactBundle::new_retained(
                ChannelId::new(1),
                version,
                &profile,
                vec![(ArtifactKind::ValueSet, artifact)],
                usize::MAX,
                retention,
            )
            .unwrap(),
        )
    }

    #[test]
    fn publish_gate_handles_first_stale_idempotent_conflict_and_higher() {
        let gate = ArtifactPublishGate::default();
        let key = key(3);
        let generation = gate.generation(key);
        let first = bundle(LogicalVersion::FIRST, 1);
        assert_eq!(
            gate.commit_published(key, generation, first.clone())
                .unwrap(),
            PublishCommitOutcome::Published
        );
        assert_eq!(
            gate.commit_published(key, generation, bundle(LogicalVersion::new(0), 2))
                .unwrap(),
            PublishCommitOutcome::Stale
        );
        assert_eq!(
            gate.commit_published(key, generation, first).unwrap(),
            PublishCommitOutcome::Idempotent
        );
        assert_eq!(
            gate.commit_published(key, generation, bundle(LogicalVersion::FIRST, 9))
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::ConflictingArtifactPublish
        );
        assert_eq!(
            gate.commit_published(key, generation, bundle(LogicalVersion::new(2), 7))
                .unwrap(),
            PublishCommitOutcome::Published
        );
    }

    #[test]
    fn same_version_single_flight_reuses_success_unsupported_and_unavailable() {
        let gate = ArtifactPublishGate::default();
        let success_key = key(4);
        let ArtifactJobClaim::Owner(success_owner) = gate.claim(success_key, LogicalVersion::FIRST)
        else {
            panic!("first claimant must own the job");
        };
        let ArtifactJobClaim::Follower(success_follower) =
            gate.claim(success_key, LogicalVersion::FIRST)
        else {
            panic!("duplicate claimant must follow");
        };
        let published = bundle(LogicalVersion::FIRST, 3);
        success_owner
            .finish(ArtifactDeliveryOutcome::Published(published.clone()))
            .unwrap();
        let ArtifactDeliveryOutcome::Published(reused) = success_follower.wait() else {
            panic!("follower must reuse published outcome");
        };
        assert!(Arc::ptr_eq(&published, &reused));

        let unavailable_key = key(5);
        let ArtifactJobClaim::Owner(owner) = gate.claim(unavailable_key, LogicalVersion::FIRST)
        else {
            panic!("first claimant must own the job");
        };
        let ArtifactJobClaim::Follower(follower) =
            gate.claim(unavailable_key, LogicalVersion::FIRST)
        else {
            panic!("duplicate claimant must follow");
        };
        owner
            .finish(ArtifactDeliveryOutcome::Unavailable(
                UnavailableReason::ResourceLimit,
            ))
            .unwrap();
        assert!(matches!(
            follower.wait(),
            ArtifactDeliveryOutcome::Unavailable(UnavailableReason::ResourceLimit)
        ));

        let unsupported_key = key(8);
        let ArtifactJobClaim::Owner(owner) = gate.claim(unsupported_key, LogicalVersion::FIRST)
        else {
            panic!("first claimant must own the job");
        };
        let ArtifactJobClaim::Follower(follower) =
            gate.claim(unsupported_key, LogicalVersion::FIRST)
        else {
            panic!("duplicate claimant must follow");
        };
        owner
            .finish(ArtifactDeliveryOutcome::Unsupported(
                ArtifactUnsupportedReason::NoAcceptedRepresentation,
            ))
            .unwrap();
        assert!(matches!(
            follower.wait(),
            ArtifactDeliveryOutcome::Unsupported(
                ArtifactUnsupportedReason::NoAcceptedRepresentation
            )
        ));
    }

    #[test]
    fn concurrent_same_profile_followers_reuse_one_owner_terminal_outcome() {
        struct ReleaseOnDrop(Arc<(Mutex<bool>, Condvar)>);

        impl ReleaseOnDrop {
            fn release(&self) {
                let (lock, changed) = &*self.0;
                *lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
                changed.notify_all();
            }
        }

        impl Drop for ReleaseOnDrop {
            fn drop(&mut self) {
                self.release();
            }
        }

        const CLAIMANTS: usize = 5;
        let gate = ArtifactPublishGate::default();

        let success_key = key(13);
        let start = Arc::new((Mutex::new(false), Condvar::new()));
        let finish_claim = Arc::new((Mutex::new(false), Condvar::new()));
        let start_release = ReleaseOnDrop(start.clone());
        let finish_claim_release = ReleaseOnDrop(finish_claim.clone());
        let published = bundle(LogicalVersion::FIRST, 4);
        let (sent, received) = mpsc::channel();
        let (ready_sent, ready_received) = mpsc::channel();
        let (claimed_sent, claimed_received) = mpsc::channel();
        let threads = (0..CLAIMANTS)
            .map(|_| {
                let gate = gate.clone();
                let start = start.clone();
                let finish_claim = finish_claim.clone();
                let published = published.clone();
                let sent = sent.clone();
                let ready_sent = ready_sent.clone();
                let claimed_sent = claimed_sent.clone();
                std::thread::spawn(move || {
                    ready_sent.send(()).unwrap();
                    let (lock, changed) = &*start;
                    let mut started = lock.lock().unwrap();
                    while !*started {
                        started = changed.wait(started).unwrap();
                    }
                    drop(started);
                    let job = gate.claim(success_key, LogicalVersion::FIRST);
                    claimed_sent.send(()).unwrap();
                    let (lock, changed) = &*finish_claim;
                    let mut may_finish = lock.lock().unwrap();
                    while !*may_finish {
                        may_finish = changed.wait(may_finish).unwrap();
                    }
                    drop(may_finish);
                    match job {
                        ArtifactJobClaim::Owner(owner) => {
                            owner
                                .finish(ArtifactDeliveryOutcome::Published(published.clone()))
                                .unwrap();
                            sent.send((true, published)).unwrap();
                        }
                        ArtifactJobClaim::Follower(follower) => {
                            let ArtifactDeliveryOutcome::Published(reused) = follower.wait() else {
                                panic!("success follower must reuse the published bundle");
                            };
                            sent.send((false, reused)).unwrap();
                        }
                        ArtifactJobClaim::Stale => panic!("first-version claim cannot be stale"),
                    }
                })
            })
            .collect::<Vec<_>>();
        for _ in 0..CLAIMANTS {
            ready_received
                .recv_timeout(Duration::from_secs(5))
                .expect("every claimant must reach the start gate");
        }
        start_release.release();
        for _ in 0..CLAIMANTS {
            claimed_received
                .recv_timeout(Duration::from_secs(5))
                .expect("every claimant must claim before the owner finishes");
        }
        finish_claim_release.release();
        let mut owners = 0;
        for _ in 0..CLAIMANTS {
            let (owner, reused) = received
                .recv_timeout(Duration::from_secs(5))
                .expect("success claimant must finish");
            owners += usize::from(owner);
            assert!(Arc::ptr_eq(&published, &reused));
        }
        assert_eq!(owners, 1);
        for thread in threads {
            thread.join().unwrap();
        }

        let unavailable_key = key(14);
        let ArtifactJobClaim::Owner(owner) = gate.claim(unavailable_key, LogicalVersion::FIRST)
        else {
            panic!("first claimant must own the unavailable job");
        };
        let followers = (0..CLAIMANTS - 1)
            .map(|_| {
                let ArtifactJobClaim::Follower(follower) =
                    gate.claim(unavailable_key, LogicalVersion::FIRST)
                else {
                    panic!("same-version claimant must follow");
                };
                follower
            })
            .collect::<Vec<_>>();
        owner
            .finish(ArtifactDeliveryOutcome::Unavailable(
                UnavailableReason::ResourceLimit,
            ))
            .unwrap();
        assert!(followers.into_iter().all(|follower| matches!(
            follower.wait(),
            ArtifactDeliveryOutcome::Unavailable(UnavailableReason::ResourceLimit)
        )));
    }

    #[test]
    fn late_lower_version_is_stale_and_releases_its_retained_candidate() {
        let gate = ArtifactPublishGate::default();
        let key = key(15);
        let account = Arc::new(CountingMemory::default());
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        let retained_per_bundle = ArtifactBundle::accounted_resident_overhead(&profile, 1)
            .unwrap()
            .checked_add(PhysicalArtifact::accounted_resident_component_bytes(1).unwrap())
            .unwrap();
        let retained_budget = Arc::new(ArtifactRetainedBudget::new(retained_per_bundle * 2));
        let ArtifactJobClaim::Owner(lower_owner) = gate.claim(key, LogicalVersion::FIRST) else {
            panic!("lower version must own its admitted job");
        };
        let ArtifactJobClaim::Owner(higher_owner) = gate.claim(key, LogicalVersion::new(2)) else {
            panic!("higher version must be independently admitted");
        };
        let lower = retained_bundle(
            LogicalVersion::FIRST,
            1,
            retained_budget.clone(),
            account.clone(),
        );
        let higher = retained_bundle(
            LogicalVersion::new(2),
            2,
            retained_budget.clone(),
            account.clone(),
        );
        assert_eq!(lower.retained_memory_bytes(), retained_per_bundle);
        assert_eq!(higher.retained_memory_bytes(), retained_per_bundle);
        assert_eq!(
            account.current.load(Ordering::SeqCst),
            retained_per_bundle * 2
        );
        assert_eq!(retained_budget.retained_bytes(), retained_per_bundle * 2);

        assert_eq!(
            higher_owner
                .finish(ArtifactDeliveryOutcome::Published(higher.clone()))
                .unwrap(),
            PublishCommitOutcome::Published
        );
        assert_eq!(
            lower_owner
                .finish(ArtifactDeliveryOutcome::Published(lower.clone()))
                .unwrap(),
            PublishCommitOutcome::Stale
        );
        drop(lower);
        assert_eq!(account.current.load(Ordering::SeqCst), retained_per_bundle);
        assert_eq!(retained_budget.retained_bytes(), retained_per_bundle);
        let ArtifactJobClaim::Follower(latest) = gate.claim(key, LogicalVersion::new(2)) else {
            panic!("published higher version must be reused");
        };
        let ArtifactDeliveryOutcome::Published(latest) = latest.wait() else {
            panic!("higher version remains published");
        };
        assert!(Arc::ptr_eq(&higher, &latest));
        drop(latest);
        drop(gate);
        drop(higher);
        assert_eq!(account.current.load(Ordering::SeqCst), 0);
        assert_eq!(retained_budget.retained_bytes(), 0);
    }

    #[test]
    fn cancel_invalidates_generation_wakes_follower_and_rejects_late_finish() {
        let gate = ArtifactPublishGate::default();
        let key = key(6);
        let ArtifactJobClaim::Owner(owner) = gate.claim(key, LogicalVersion::FIRST) else {
            panic!("first claimant must own the job");
        };
        let ArtifactJobClaim::Follower(follower) = gate.claim(key, LogicalVersion::FIRST) else {
            panic!("duplicate claimant must follow");
        };
        gate.cancel(key);
        assert!(matches!(
            follower.wait(),
            ArtifactDeliveryOutcome::Cancelled
        ));
        assert_eq!(
            owner
                .finish(ArtifactDeliveryOutcome::Published(bundle(
                    LogicalVersion::FIRST,
                    1
                )))
                .unwrap(),
            PublishCommitOutcome::Cancelled,
        );
    }

    #[test]
    fn shutdown_generation_invalidates_v1_and_v2_without_orphaned_flights() {
        let gate = ArtifactPublishGate::default();
        let key = key(16);
        let ArtifactJobClaim::Owner(v1_owner) = gate.claim(key, LogicalVersion::FIRST) else {
            panic!("v1 must own its generation-scoped flight");
        };
        let ArtifactJobClaim::Follower(v1_follower) = gate.claim(key, LogicalVersion::FIRST) else {
            panic!("duplicate v1 must follow");
        };
        let ArtifactJobClaim::Owner(v2_owner) = gate.claim(key, LogicalVersion::new(2)) else {
            panic!("v2 must own an independent generation-scoped flight");
        };
        let ArtifactJobClaim::Follower(v2_follower) = gate.claim(key, LogicalVersion::new(2))
        else {
            panic!("duplicate v2 must follow");
        };

        assert_eq!(gate.generation(key), 0);
        gate.cancel_channel(ChannelId::new(1), DeploymentEpoch::new(2));
        assert_eq!(gate.generation(key), 1);
        assert!(matches!(
            v1_follower.wait(),
            ArtifactDeliveryOutcome::Cancelled
        ));
        assert!(matches!(
            v2_follower.wait(),
            ArtifactDeliveryOutcome::Cancelled
        ));
        assert_eq!(
            v2_owner
                .finish(ArtifactDeliveryOutcome::Published(bundle(
                    LogicalVersion::new(2),
                    2,
                )))
                .unwrap(),
            PublishCommitOutcome::Cancelled
        );
        assert_eq!(
            v1_owner
                .finish(ArtifactDeliveryOutcome::Published(bundle(
                    LogicalVersion::FIRST,
                    1,
                )))
                .unwrap(),
            PublishCommitOutcome::Cancelled
        );
        assert_eq!(gate.in_flight_follower_count(key, LogicalVersion::FIRST), 0);
        assert_eq!(
            gate.in_flight_follower_count(key, LogicalVersion::new(2)),
            0
        );
        let ArtifactJobClaim::Follower(after_shutdown) = gate.claim(key, LogicalVersion::new(3))
        else {
            panic!("a cancelled generation cannot recreate an owner");
        };
        assert!(matches!(
            after_shutdown.wait(),
            ArtifactDeliveryOutcome::Cancelled
        ));
    }

    #[test]
    fn conflicting_owner_finish_always_wakes_same_version_follower() {
        let gate = ArtifactPublishGate::default();
        let key = key(7);
        let ArtifactJobClaim::Owner(owner) = gate.claim(key, LogicalVersion::new(2)) else {
            panic!("first claimant must own the job");
        };
        let ArtifactJobClaim::Follower(follower) = gate.claim(key, LogicalVersion::new(2)) else {
            panic!("duplicate claimant must follow");
        };
        let generation = gate.generation(key);
        gate.commit_published(key, generation, bundle(LogicalVersion::new(2), 1))
            .unwrap();
        let (sent, received) = std::sync::mpsc::channel();
        std::thread::spawn(move || sent.send(follower.wait()).unwrap());

        let _ = owner.finish(ArtifactDeliveryOutcome::Published(bundle(
            LogicalVersion::new(2),
            9,
        )));
        assert!(matches!(
            received.recv_timeout(std::time::Duration::from_secs(1)),
            Ok(ArtifactDeliveryOutcome::Cancelled)
        ));
    }

    #[test]
    fn dropping_owner_and_generation_exhaustion_never_leave_followers_pending() {
        let gate = ArtifactPublishGate::default();
        let dropped_key = key(9);
        let ArtifactJobClaim::Owner(owner) = gate.claim(dropped_key, LogicalVersion::FIRST) else {
            panic!("first claimant must own the job");
        };
        let ArtifactJobClaim::Follower(follower) = gate.claim(dropped_key, LogicalVersion::FIRST)
        else {
            panic!("duplicate claimant must follow");
        };
        drop(owner);
        assert!(matches!(
            follower.wait(),
            ArtifactDeliveryOutcome::Cancelled
        ));

        let exhausted_key = key(10);
        gate.state
            .lock()
            .unwrap()
            .keys
            .entry(exhausted_key)
            .or_default()
            .generation = u64::MAX;
        gate.cancel(exhausted_key);
        gate.cancel(exhausted_key);
        assert_eq!(gate.generation(exhausted_key), u64::MAX);
        let ArtifactJobClaim::Follower(follower) = gate.claim(exhausted_key, LogicalVersion::FIRST)
        else {
            panic!("exhausted cancelled generation cannot resurrect an owner");
        };
        assert!(matches!(
            follower.wait(),
            ArtifactDeliveryOutcome::Cancelled
        ));
    }

    #[test]
    fn cancel_only_newly_terminalizes_a_pending_publish_key() {
        let gate = ArtifactPublishGate::default();
        let pending = key(11);
        assert!(gate.cancel(pending));
        assert!(!gate.cancel(pending));

        let published = key(12);
        let generation = gate.generation(published);
        assert_eq!(
            gate.commit_published(published, generation, bundle(LogicalVersion::FIRST, 1),)
                .unwrap(),
            PublishCommitOutcome::Published
        );
        assert!(!gate.cancel(published));
    }
}
