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
use std::fmt;
use std::sync::{Arc, Mutex, Weak};

use arrow::datatypes::DataType;
use sha2::{Digest, Sha256};

use crate::common::types::UniqueId;
use crate::runtime_filter::model::contract::BindingId;
use crate::runtime_filter::port::final_domain::{FinalDomainShard, RuntimeCompletionFenceContract};
use crate::runtime_filter::port::identity::{PartitionId, ProducerSequence, ProducerStreamId};
use crate::runtime_filter::port::producer::{
    FinalDomainProducerAdapter, ProducerFailureReason, RuntimeContractViolation,
    RuntimeContractViolationKind, SubmitOutcome,
};
use crate::runtime_filter::port::value_domain::ValueDomainDelta;

const COMPLETION_SET_DOMAIN: &[u8] = b"novarocks.runtime-filter.final-domain-completion-set";
const COMPLETION_SET_VERSION: u16 = 1;
const FROZEN_SET_DOMAIN: &[u8] = b"novarocks.runtime-filter.final-domain-frozen-set";
const FROZEN_SET_VERSION: u16 = 1;
const ISSUANCE_PERMIT_DOMAIN: &[u8] = b"novarocks.runtime-filter.final-domain-issuance-permit";
const ISSUANCE_PERMIT_VERSION: u16 = 1;

pub(crate) struct FinalDomainServiceIssuancePermit {
    frozen_set_digest: [u8; 32],
    stream: ProducerStreamId,
    domain_fingerprint: [u8; 32],
    binding_digest: [u8; 32],
}

pub(crate) struct FinalDomainCompletionSession {
    inner: Arc<FinalDomainCompletionSessionInner>,
    membership_key_type: DataType,
    _owner_lease: FinalDomainCompletionOwnerLease,
}

impl fmt::Debug for FinalDomainCompletionSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("FinalDomainCompletionSession")
    }
}

pub(crate) struct FinalDomainPartitionCommitter {
    inner: Arc<FinalDomainCompletionSessionInner>,
    partition_id: PartitionId,
    capability: Option<FinalDomainFreezeCapability>,
    closed: bool,
}

impl fmt::Debug for FinalDomainPartitionCommitter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FinalDomainPartitionCommitter")
            .field("partition_id", &self.partition_id)
            .field("sealed", &self.capability.is_none())
            .field("closed", &self.closed)
            .finish()
    }
}

pub(super) struct FinalDomainCompletionSessionWeak(Weak<FinalDomainCompletionSessionInner>);

#[derive(Default)]
pub(super) struct FinalDomainCompletionSessionRegistry {
    sessions: Mutex<BTreeMap<(BindingId, UniqueId), FinalDomainCompletionSessionWeak>>,
}

struct FinalDomainCompletionOwnerLease {
    inner: Arc<FinalDomainCompletionSessionInner>,
}

struct FinalDomainCompletionSessionInner {
    producer: Arc<dyn FinalDomainProducerAdapter>,
    operation: Mutex<()>,
    state: Mutex<FinalDomainCompletionState>,
}

struct FinalDomainCompletionState {
    lifecycle: FinalDomainCompletionLifecycle,
    authority: Option<FinalDomainCompletionAuthority>,
    partitions: Vec<FinalDomainPartitionState>,
    fail_sent: bool,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum FinalDomainCompletionLifecycle {
    Collecting,
    Issuing,
    Completed,
    Failed,
}

struct FinalDomainPartitionState {
    claimed: bool,
    capability: Option<FinalDomainFreezeCapability>,
    payload: Option<FrozenFinalDomainPayload>,
    closed: bool,
}

struct FinalDomainCompletionAuthority {
    contract: Arc<RuntimeCompletionFenceContract>,
    binding_id: BindingId,
    fragment_instance_id: UniqueId,
    partition_count: u32,
    completion_set_digest: [u8; 32],
}

#[derive(Debug)]
struct FinalDomainFreezeCapability {
    stream: ProducerStreamId,
    completion_set_digest: [u8; 32],
}

#[derive(Debug)]
struct FrozenFinalDomainPayload {
    stream: ProducerStreamId,
    completion_set_digest: [u8; 32],
    domain: ValueDomainDelta,
}

struct FrozenFinalDomainSet {
    _proof_digest: [u8; 32],
    shards: Vec<(PartitionId, FinalDomainShard)>,
}

impl FinalDomainCompletionAuthority {
    fn new(
        contract: Arc<RuntimeCompletionFenceContract>,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        partition_count: u32,
    ) -> Self {
        let completion_set_digest = completion_set_digest(
            contract.digest().bytes(),
            binding_id,
            fragment_instance_id,
            partition_count,
        );
        Self {
            contract,
            binding_id,
            fragment_instance_id,
            partition_count,
            completion_set_digest,
        }
    }

    fn partition_capability(&self, partition_id: PartitionId) -> FinalDomainFreezeCapability {
        FinalDomainFreezeCapability {
            stream: ProducerStreamId::new(self.binding_id, self.fragment_instance_id, partition_id),
            completion_set_digest: self.completion_set_digest,
        }
    }

    fn freeze(
        self,
        payloads: Vec<FrozenFinalDomainPayload>,
    ) -> Result<FrozenFinalDomainSet, RuntimeContractViolation> {
        if payloads.len() != self.partition_count as usize {
            return Err(violation(
                RuntimeContractViolationKind::FinalDomainMissing,
                "frozen final-domain payload set does not cover every declared partition",
            ));
        }
        for (expected, payload) in payloads.iter().enumerate() {
            if payload.completion_set_digest != self.completion_set_digest {
                return Err(violation(
                    RuntimeContractViolationKind::UnauthorizedFragmentInstance,
                    "frozen final-domain payload belongs to a different completion set",
                ));
            }
            if payload.stream.binding_id() != self.binding_id
                || payload.stream.fragment_instance_id() != self.fragment_instance_id
            {
                return Err(violation(
                    RuntimeContractViolationKind::UnauthorizedFragmentInstance,
                    "frozen final-domain payload scope does not match the completion session",
                ));
            }
            if payload.stream.partition_id().get() as usize != expected {
                return Err(violation(
                    RuntimeContractViolationKind::InvalidPartition,
                    "frozen final-domain payload set is reordered, duplicated, or incomplete",
                ));
            }
        }

        let proof_digest = frozen_set_digest(self.completion_set_digest, &payloads);
        let mut shards = Vec::with_capacity(payloads.len());
        for payload in payloads {
            let partition_id = payload.stream.partition_id();
            let permit = FinalDomainServiceIssuancePermit::new(
                proof_digest,
                payload.stream,
                &payload.domain,
            );
            let shard = FinalDomainShard::issue_for_service(
                permit,
                &self.contract,
                payload.stream,
                ProducerSequence::new(0),
                payload.domain,
            )
            .map_err(|error| {
                violation(
                    RuntimeContractViolationKind::TypeMismatch,
                    error.to_string(),
                )
            })?;
            shards.push((partition_id, shard));
        }
        Ok(FrozenFinalDomainSet {
            _proof_digest: proof_digest,
            shards,
        })
    }
}

impl FinalDomainServiceIssuancePermit {
    fn new(
        frozen_set_digest: [u8; 32],
        stream: ProducerStreamId,
        domain: &ValueDomainDelta,
    ) -> Self {
        let domain_fingerprint = domain.fingerprint().bytes();
        let binding_digest = issuance_permit_digest(frozen_set_digest, stream, domain_fingerprint);
        Self {
            frozen_set_digest,
            stream,
            domain_fingerprint,
            binding_digest,
        }
    }

    pub(crate) fn authorizes(&self, stream: ProducerStreamId, domain: &ValueDomainDelta) -> bool {
        let domain_fingerprint = domain.fingerprint().bytes();
        self.stream == stream
            && self.domain_fingerprint == domain_fingerprint
            && self.binding_digest
                == issuance_permit_digest(self.frozen_set_digest, stream, domain_fingerprint)
    }
}

impl FinalDomainFreezeCapability {
    fn seal(self, domain: ValueDomainDelta) -> FrozenFinalDomainPayload {
        FrozenFinalDomainPayload {
            stream: self.stream,
            completion_set_digest: self.completion_set_digest,
            domain,
        }
    }
}

impl FrozenFinalDomainSet {
    fn into_shards(self) -> Vec<(PartitionId, FinalDomainShard)> {
        self.shards
    }
}

impl FinalDomainCompletionSession {
    pub(super) fn new(
        contract: Arc<RuntimeCompletionFenceContract>,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        producer: Arc<dyn FinalDomainProducerAdapter>,
        partition_count: u32,
    ) -> Result<Self, RuntimeContractViolation> {
        if partition_count == 0 {
            return Err(violation(
                RuntimeContractViolationKind::InvalidPartitionCount,
                "a final-domain completion session requires at least one partition",
            ));
        }
        let membership_key_type = contract.membership_schema().data_type().clone();
        let authority = FinalDomainCompletionAuthority::new(
            contract,
            binding_id,
            fragment_instance_id,
            partition_count,
        );
        let partitions = (0..partition_count)
            .map(|partition| FinalDomainPartitionState {
                claimed: false,
                capability: Some(authority.partition_capability(PartitionId::new(partition))),
                payload: None,
                closed: false,
            })
            .collect();
        let inner = Arc::new(FinalDomainCompletionSessionInner {
            producer,
            operation: Mutex::new(()),
            state: Mutex::new(FinalDomainCompletionState {
                lifecycle: FinalDomainCompletionLifecycle::Collecting,
                authority: Some(authority),
                partitions,
                fail_sent: false,
            }),
        });
        Ok(Self {
            inner: Arc::clone(&inner),
            membership_key_type,
            _owner_lease: FinalDomainCompletionOwnerLease { inner },
        })
    }

    pub(crate) const fn membership_key_type(&self) -> &DataType {
        &self.membership_key_type
    }

    pub(crate) fn partition(
        &self,
        partition_id: PartitionId,
    ) -> Result<FinalDomainPartitionCommitter, RuntimeContractViolation> {
        let result = {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if state.lifecycle != FinalDomainCompletionLifecycle::Collecting {
                return Err(session_unavailable());
            }
            let Some(partition) = state.partitions.get_mut(partition_id.get() as usize) else {
                drop(state);
                self.inner.fail_contract();
                return Err(violation(
                    RuntimeContractViolationKind::InvalidPartition,
                    "final-domain partition is outside the declared local partition set",
                ));
            };
            if partition.claimed {
                Err(violation(
                    RuntimeContractViolationKind::ConflictingReplay,
                    "final-domain partition committer was already created",
                ))
            } else {
                partition.claimed = true;
                Ok(partition
                    .capability
                    .take()
                    .expect("an unclaimed partition owns its freeze capability"))
            }
        };
        match result {
            Ok(capability) => Ok(FinalDomainPartitionCommitter {
                inner: Arc::clone(&self.inner),
                partition_id,
                capability: Some(capability),
                closed: false,
            }),
            Err(error) => {
                self.inner.fail_contract();
                Err(error)
            }
        }
    }

    pub(crate) fn fail(
        &self,
        reason: ProducerFailureReason,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        self.inner.fail_once(reason)
    }

    pub(super) fn weak(&self) -> FinalDomainCompletionSessionWeak {
        FinalDomainCompletionSessionWeak(Arc::downgrade(&self.inner))
    }
}

impl FinalDomainPartitionCommitter {
    pub(crate) fn seal(
        &mut self,
        domain: ValueDomainDelta,
    ) -> Result<(), RuntimeContractViolation> {
        if self.closed || self.capability.is_none() {
            self.inner.fail_contract();
            return Err(violation(
                RuntimeContractViolationKind::ConflictingReplay,
                "final-domain partition may be sealed exactly once before close",
            ));
        }
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.lifecycle != FinalDomainCompletionLifecycle::Collecting {
            return Err(session_unavailable());
        }
        let partition = &mut state.partitions[self.partition_id.get() as usize];
        if partition.payload.is_some() || partition.closed {
            drop(state);
            self.inner.fail_contract();
            return Err(violation(
                RuntimeContractViolationKind::ConflictingReplay,
                "final-domain partition may be sealed exactly once before close",
            ));
        }
        let capability = self
            .capability
            .take()
            .expect("an unsealed partition committer owns its capability");
        partition.payload = Some(capability.seal(domain));
        Ok(())
    }

    pub(crate) fn close(&mut self) -> Result<(), RuntimeContractViolation> {
        if self.closed {
            self.inner.fail_contract();
            return Err(violation(
                RuntimeContractViolationKind::ConflictingReplay,
                "final-domain partition committer may close exactly once",
            ));
        }
        let terminal = {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if state.lifecycle != FinalDomainCompletionLifecycle::Collecting {
                return Err(session_unavailable());
            }
            let partition = &mut state.partitions[self.partition_id.get() as usize];
            if partition.payload.is_none() {
                drop(state);
                self.inner.fail_contract();
                return Err(violation(
                    RuntimeContractViolationKind::FinalDomainMissing,
                    "final-domain partition must be sealed before close",
                ));
            }
            partition.closed = true;
            self.closed = true;
            let terminal = state
                .partitions
                .iter()
                .all(|partition| partition.payload.is_some() && partition.closed);
            if terminal {
                state.lifecycle = FinalDomainCompletionLifecycle::Issuing;
            }
            terminal
        };
        if terminal {
            self.inner.issue_all()
        } else {
            Ok(())
        }
    }
}

impl FinalDomainCompletionSessionRegistry {
    pub(super) fn ensure_vacant(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
    ) -> Result<(), RuntimeContractViolation> {
        let sessions = self
            .sessions
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if sessions
            .get(&(binding_id, fragment_instance_id))
            .is_some_and(|existing| existing.0.upgrade().is_some())
        {
            return Err(session_already_open());
        }
        Ok(())
    }

    pub(super) fn register(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        session: FinalDomainCompletionSessionWeak,
    ) -> Result<(), RuntimeContractViolation> {
        let key = (binding_id, fragment_instance_id);
        let mut sessions = self
            .sessions
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if sessions
            .get(&key)
            .is_some_and(|existing| existing.0.upgrade().is_some())
        {
            return Err(session_already_open());
        }
        sessions.insert(key, session);
        Ok(())
    }
}

impl FinalDomainCompletionSessionInner {
    fn fail_contract(&self) {
        let _ = self.fail_once(ProducerFailureReason::ExecutionFailed);
    }

    fn fail_once(
        &self,
        reason: ProducerFailureReason,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        let _operation = self
            .operation
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.fail_while_holding_operation(reason)
    }

    fn fail_while_holding_operation(
        &self,
        reason: ProducerFailureReason,
    ) -> Result<SubmitOutcome, RuntimeContractViolation> {
        let should_send = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if matches!(
                state.lifecycle,
                FinalDomainCompletionLifecycle::Completed | FinalDomainCompletionLifecycle::Failed
            ) {
                false
            } else {
                state.lifecycle = FinalDomainCompletionLifecycle::Failed;
                if state.fail_sent {
                    false
                } else {
                    state.fail_sent = true;
                    true
                }
            }
        };
        if should_send {
            self.producer.fail(reason)
        } else {
            Ok(SubmitOutcome::TerminalNoop)
        }
    }

    fn issue_all(&self) -> Result<(), RuntimeContractViolation> {
        let _operation = self
            .operation
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let (authority, payloads) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.lifecycle != FinalDomainCompletionLifecycle::Issuing {
                return Err(session_unavailable());
            }
            let authority = state
                .authority
                .take()
                .expect("the terminal transition owns the completion authority");
            let payloads = state
                .partitions
                .iter_mut()
                .map(|partition| {
                    partition
                        .payload
                        .take()
                        .expect("the terminal transition owns every frozen payload")
                })
                .collect::<Vec<_>>();
            (authority, payloads)
        };
        let frozen = match authority.freeze(payloads) {
            Ok(frozen) => frozen,
            Err(error) => {
                let _ = self.fail_while_holding_operation(ProducerFailureReason::ExecutionFailed);
                return Err(error);
            }
        };
        for (partition_id, shard) in frozen.into_shards() {
            let complete = self
                .producer
                .complete(partition_id, ProducerSequence::new(0), shard);
            if let Err(error) = require_non_terminal_submit(complete) {
                let _ = self.fail_while_holding_operation(ProducerFailureReason::ExecutionFailed);
                return Err(error);
            }
            let close = self
                .producer
                .close_partition(partition_id, ProducerSequence::new(1));
            if let Err(error) = require_non_terminal_submit(close) {
                let _ = self.fail_while_holding_operation(ProducerFailureReason::ExecutionFailed);
                return Err(error);
            }
        }
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.lifecycle == FinalDomainCompletionLifecycle::Issuing {
            state.lifecycle = FinalDomainCompletionLifecycle::Completed;
            Ok(())
        } else {
            Err(session_unavailable())
        }
    }
}

impl Drop for FinalDomainCompletionOwnerLease {
    fn drop(&mut self) {
        let all_claimed = {
            let state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.partitions.iter().all(|partition| partition.claimed)
        };
        if !all_claimed {
            self.inner.fail_contract();
        }
    }
}

impl Drop for FinalDomainPartitionCommitter {
    fn drop(&mut self) {
        if !self.closed {
            self.inner.fail_contract();
        }
    }
}

fn completion_set_digest(
    contract_digest: [u8; 32],
    binding_id: BindingId,
    fragment_instance_id: UniqueId,
    partition_count: u32,
) -> [u8; 32] {
    let mut canonical = Sha256::new();
    canonical.update(COMPLETION_SET_DOMAIN);
    canonical.update(COMPLETION_SET_VERSION.to_be_bytes());
    canonical.update(contract_digest);
    canonical.update(binding_id.get().to_be_bytes());
    canonical.update(fragment_instance_id.high().to_be_bytes());
    canonical.update(fragment_instance_id.low().to_be_bytes());
    canonical.update(partition_count.to_be_bytes());
    canonical.finalize().into()
}

fn frozen_set_digest(
    completion_set_digest: [u8; 32],
    payloads: &[FrozenFinalDomainPayload],
) -> [u8; 32] {
    let mut canonical = Sha256::new();
    canonical.update(FROZEN_SET_DOMAIN);
    canonical.update(FROZEN_SET_VERSION.to_be_bytes());
    canonical.update(completion_set_digest);
    canonical.update((payloads.len() as u64).to_be_bytes());
    for payload in payloads {
        canonical.update(payload.stream.partition_id().get().to_be_bytes());
        canonical.update(payload.domain.fingerprint().bytes());
    }
    canonical.finalize().into()
}

fn issuance_permit_digest(
    frozen_set_digest: [u8; 32],
    stream: ProducerStreamId,
    domain_fingerprint: [u8; 32],
) -> [u8; 32] {
    let mut canonical = Sha256::new();
    canonical.update(ISSUANCE_PERMIT_DOMAIN);
    canonical.update(ISSUANCE_PERMIT_VERSION.to_be_bytes());
    canonical.update(frozen_set_digest);
    canonical.update(stream.binding_id().get().to_be_bytes());
    canonical.update(stream.fragment_instance_id().high().to_be_bytes());
    canonical.update(stream.fragment_instance_id().low().to_be_bytes());
    canonical.update(stream.partition_id().get().to_be_bytes());
    canonical.update(domain_fingerprint);
    canonical.finalize().into()
}

fn require_non_terminal_submit(
    result: Result<SubmitOutcome, RuntimeContractViolation>,
) -> Result<SubmitOutcome, RuntimeContractViolation> {
    match result? {
        SubmitOutcome::TerminalNoop => Err(violation(
            RuntimeContractViolationKind::ServiceUnavailable,
            "final-domain producer became terminal during completion issuance",
        )),
        outcome => Ok(outcome),
    }
}

fn violation(
    kind: RuntimeContractViolationKind,
    detail: impl Into<String>,
) -> RuntimeContractViolation {
    RuntimeContractViolation::new(kind, detail)
}

fn session_unavailable() -> RuntimeContractViolation {
    violation(
        RuntimeContractViolationKind::ServiceUnavailable,
        "final-domain completion session is not collecting",
    )
}

fn session_already_open() -> RuntimeContractViolation {
    violation(
        RuntimeContractViolationKind::ConflictingReplay,
        "a final-domain completion session is already open for this producer instance",
    )
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    use arrow::datatypes::DataType;

    use crate::common::types::UniqueId;
    use crate::runtime_filter::model::contract::{
        BindingId, ChannelId, CompletionFenceKind, NullSemantics,
    };
    use crate::runtime_filter::port::artifact::ArtifactMembershipSchema;
    use crate::runtime_filter::port::events::{RuntimeFilterEvent, RuntimeFilterEventSink};
    use crate::runtime_filter::port::final_domain::{
        FinalDomainError, FinalDomainShard, RuntimeCompletionFenceContract,
    };
    use crate::runtime_filter::port::identity::{DeploymentEpoch, PartitionId, ProducerSequence};
    use crate::runtime_filter::port::producer::{
        FinalDomainProducerAdapter, ProducerFailureReason, RuntimeContractViolation,
        RuntimeContractViolationKind, SubmitOutcome,
    };
    use crate::runtime_filter::port::subscription::{
        LivePollOutcome, LiveTerminal, SubscriptionKind, UnavailableReason,
    };
    use crate::runtime_filter::port::support::{
        MemoryAccountError, RuntimeFilterClock, RuntimeFilterMemoryAccount,
    };
    use crate::runtime_filter::port::value_domain::{MembershipValues, ValueDomainDelta};

    use super::*;

    macro_rules! assert_not_clone {
        ($type:ty) => {{
            trait AmbiguousIfClone<Marker> {
                fn marker() {}
            }
            impl<Type: ?Sized> AmbiguousIfClone<()> for Type {}
            impl<Type: ?Sized + Clone> AmbiguousIfClone<u8> for Type {}
            let _ = <$type as AmbiguousIfClone<_>>::marker;
        }};
    }

    const BINDING: BindingId = BindingId::new(7);
    const INSTANCE: UniqueId = UniqueId::new(8, 9);

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum AdapterCall {
        Complete(PartitionId, ProducerSequence),
        Close(PartitionId, ProducerSequence),
        Fail(ProducerFailureReason),
    }

    struct RecordingAdapter {
        calls: Mutex<Vec<AdapterCall>>,
        complete_calls: AtomicUsize,
        fail_complete_call: Option<usize>,
    }

    #[derive(Default)]
    struct RecordingEvents(Mutex<Vec<RuntimeFilterEvent>>);

    impl RuntimeFilterEventSink for RecordingEvents {
        fn record(&self, event: RuntimeFilterEvent) {
            self.0.lock().unwrap().push(event);
        }
    }

    struct FixedClock(Instant);

    impl RuntimeFilterClock for FixedClock {
        fn now(&self) -> Instant {
            self.0
        }
    }

    struct AcceptingMemory;

    impl RuntimeFilterMemoryAccount for AcceptingMemory {
        fn try_consume(&self, _bytes: usize) -> Result<(), MemoryAccountError> {
            Ok(())
        }

        fn release(&self, _bytes: usize) {}
    }

    impl RecordingAdapter {
        fn new(fail_complete_call: Option<usize>) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                complete_calls: AtomicUsize::new(0),
                fail_complete_call,
            }
        }

        fn calls(&self) -> Vec<AdapterCall> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl FinalDomainProducerAdapter for RecordingAdapter {
        fn complete(
            &self,
            partition_id: PartitionId,
            sequence: ProducerSequence,
            _shard: FinalDomainShard,
        ) -> Result<SubmitOutcome, RuntimeContractViolation> {
            self.calls
                .lock()
                .unwrap()
                .push(AdapterCall::Complete(partition_id, sequence));
            let call = self.complete_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if self.fail_complete_call == Some(call) {
                return Err(RuntimeContractViolation::new(
                    RuntimeContractViolationKind::ServiceUnavailable,
                    "injected final-domain submit failure",
                ));
            }
            Ok(SubmitOutcome::Applied)
        }

        fn close_partition(
            &self,
            partition_id: PartitionId,
            terminal_sequence: ProducerSequence,
        ) -> Result<SubmitOutcome, RuntimeContractViolation> {
            self.calls
                .lock()
                .unwrap()
                .push(AdapterCall::Close(partition_id, terminal_sequence));
            Ok(SubmitOutcome::Applied)
        }

        fn fail(
            &self,
            reason: ProducerFailureReason,
        ) -> Result<SubmitOutcome, RuntimeContractViolation> {
            self.calls.lock().unwrap().push(AdapterCall::Fail(reason));
            Ok(SubmitOutcome::Applied)
        }
    }

    fn contract() -> Arc<RuntimeCompletionFenceContract> {
        let schema =
            ArtifactMembershipSchema::new(&DataType::Int64, NullSemantics::NullSafeEqual).unwrap();
        Arc::new(
            RuntimeCompletionFenceContract::try_from_install(
                UniqueId::new(1, 2),
                DeploymentEpoch::new(3),
                ChannelId::new(4),
                CompletionFenceKind::CommittedDomainFrozen,
                &schema,
            )
            .unwrap(),
        )
    }

    fn domain(value: i64) -> ValueDomainDelta {
        ValueDomainDelta::new(MembershipValues::int64([value]), false)
    }

    fn session(
        partition_count: u32,
        fail_complete_call: Option<usize>,
    ) -> (FinalDomainCompletionSession, Arc<RecordingAdapter>) {
        let adapter = Arc::new(RecordingAdapter::new(fail_complete_call));
        let typed: Arc<dyn FinalDomainProducerAdapter> = adapter.clone();
        (
            FinalDomainCompletionSession::new(
                contract(),
                BINDING,
                INSTANCE,
                typed,
                partition_count,
            )
            .unwrap(),
            adapter,
        )
    }

    #[test]
    fn final_domain_freeze_capability_cannot_be_cloned() {
        assert_not_clone!(FinalDomainFreezeCapability);
        assert_not_clone!(FinalDomainServiceIssuancePermit);
    }

    #[test]
    fn collecting_completion_session_cannot_issue() {
        let (session, adapter) = session(2, None);
        let mut partition_0 = session.partition(PartitionId::new(0)).unwrap();
        let _partition_1 = session.partition(PartitionId::new(1)).unwrap();

        partition_0.seal(domain(10)).unwrap();
        partition_0.close().unwrap();

        assert!(adapter.calls().is_empty());
    }

    #[test]
    fn partition_must_freeze_before_close() {
        let (session, adapter) = session(1, None);
        let mut partition = session.partition(PartitionId::new(0)).unwrap();

        let error = partition.close().unwrap_err();

        assert_eq!(
            error.kind(),
            RuntimeContractViolationKind::FinalDomainMissing
        );
        assert_eq!(
            adapter.calls(),
            vec![AdapterCall::Fail(ProducerFailureReason::ExecutionFailed)]
        );
    }

    #[test]
    fn last_local_partition_enables_issuance() {
        let (session, adapter) = session(2, None);
        let mut partition_0 = session.partition(PartitionId::new(0)).unwrap();
        let mut partition_1 = session.partition(PartitionId::new(1)).unwrap();

        partition_1.seal(domain(11)).unwrap();
        partition_1.close().unwrap();
        assert!(adapter.calls().is_empty());

        partition_0.seal(domain(10)).unwrap();
        partition_0.close().unwrap();
        assert_eq!(
            adapter.calls(),
            vec![
                AdapterCall::Complete(PartitionId::new(0), ProducerSequence::new(0)),
                AdapterCall::Close(PartitionId::new(0), ProducerSequence::new(1)),
                AdapterCall::Complete(PartitionId::new(1), ProducerSequence::new(0)),
                AdapterCall::Close(PartitionId::new(1), ProducerSequence::new(1)),
            ]
        );
    }

    #[test]
    fn completion_proof_rejects_empty_and_duplicate_partition_sets() {
        assert!(
            FinalDomainCompletionAuthority::new(contract(), BINDING, INSTANCE, 2)
                .freeze(Vec::new())
                .is_err()
        );

        let authority = FinalDomainCompletionAuthority::new(contract(), BINDING, INSTANCE, 2);
        let subset = authority
            .partition_capability(PartitionId::new(0))
            .seal(domain(10));
        assert!(
            FinalDomainCompletionAuthority::new(contract(), BINDING, INSTANCE, 2)
                .freeze(vec![subset])
                .is_err()
        );

        let authority = FinalDomainCompletionAuthority::new(contract(), BINDING, INSTANCE, 2);
        let duplicate_0 = authority
            .partition_capability(PartitionId::new(0))
            .seal(domain(10));
        let replayed_0 = authority
            .partition_capability(PartitionId::new(0))
            .seal(domain(11));
        assert!(authority.freeze(vec![duplicate_0, replayed_0]).is_err());

        let first_authority = FinalDomainCompletionAuthority::new(contract(), BINDING, INSTANCE, 2);
        let first_payloads = vec![
            first_authority
                .partition_capability(PartitionId::new(0))
                .seal(domain(10)),
            first_authority
                .partition_capability(PartitionId::new(1))
                .seal(domain(11)),
        ];
        let first = first_authority.freeze(first_payloads).unwrap();
        let second_authority =
            FinalDomainCompletionAuthority::new(contract(), BINDING, INSTANCE, 2);
        let second_payloads = vec![
            second_authority
                .partition_capability(PartitionId::new(0))
                .seal(domain(10)),
            second_authority
                .partition_capability(PartitionId::new(1))
                .seal(domain(12)),
        ];
        let second = second_authority.freeze(second_payloads).unwrap();
        assert_ne!(first._proof_digest, second._proof_digest);

        let stream = ProducerStreamId::new(BINDING, INSTANCE, PartitionId::new(0));
        let permitted_domain = domain(10);
        let permit =
            FinalDomainServiceIssuancePermit::new(first._proof_digest, stream, &permitted_domain);
        assert!(permit.authorizes(stream, &permitted_domain));
        assert!(!permit.authorizes(stream, &domain(99)));
        assert!(!permit.authorizes(
            ProducerStreamId::new(BINDING, INSTANCE, PartitionId::new(1)),
            &permitted_domain,
        ));

        let mut forged =
            FinalDomainServiceIssuancePermit::new(first._proof_digest, stream, &permitted_domain);
        forged.binding_digest[0] ^= 1;
        assert_eq!(
            FinalDomainShard::issue_for_service(
                forged,
                &contract(),
                stream,
                ProducerSequence::new(0),
                permitted_domain,
            ),
            Err(FinalDomainError::FrozenProofMismatch)
        );
    }

    #[test]
    fn all_shards_are_prevalidated_before_first_adapter_mutation() {
        let (session, adapter) = session(2, None);
        let mut partition_0 = session.partition(PartitionId::new(0)).unwrap();
        let mut partition_1 = session.partition(PartitionId::new(1)).unwrap();

        partition_0.seal(domain(10)).unwrap();
        partition_1
            .seal(ValueDomainDelta::new(
                MembershipValues::utf8(["wrong-schema"]),
                false,
            ))
            .unwrap();
        partition_0.close().unwrap();
        let error = partition_1.close().unwrap_err();

        assert_eq!(error.kind(), RuntimeContractViolationKind::TypeMismatch);
        assert_eq!(
            adapter.calls(),
            vec![AdapterCall::Fail(ProducerFailureReason::ExecutionFailed)]
        );
    }

    #[test]
    fn unknown_or_duplicate_partition_is_contract_violation() {
        let (unknown_session, unknown_adapter) = session(2, None);
        let unknown = unknown_session.partition(PartitionId::new(2)).unwrap_err();
        assert_eq!(
            unknown.kind(),
            RuntimeContractViolationKind::InvalidPartition
        );
        assert_eq!(
            unknown_adapter.calls(),
            vec![AdapterCall::Fail(ProducerFailureReason::ExecutionFailed)]
        );

        let (duplicate_session, duplicate_adapter) = session(2, None);
        let _partition = duplicate_session.partition(PartitionId::new(0)).unwrap();
        let duplicate = duplicate_session
            .partition(PartitionId::new(0))
            .unwrap_err();
        assert_eq!(
            duplicate.kind(),
            RuntimeContractViolationKind::ConflictingReplay
        );
        assert_eq!(
            duplicate_adapter.calls(),
            vec![AdapterCall::Fail(ProducerFailureReason::ExecutionFailed)]
        );
    }

    #[test]
    fn same_binding_instance_cannot_open_two_sessions() {
        let fixture = super::super::tests::fixture();
        fixture
            .service
            .install(super::super::tests::compiled_fenced_final_install())
            .unwrap();
        let _first = fixture
            .service
            .open_final_aggregate_producer(BindingId::new(10), UniqueId::new(70, 10), 1)
            .unwrap();

        let error = fixture
            .service
            .open_final_aggregate_producer(BindingId::new(10), UniqueId::new(70, 10), 1)
            .unwrap_err();

        assert_eq!(
            error.kind(),
            RuntimeContractViolationKind::ConflictingReplay
        );
    }

    #[test]
    fn failed_session_cannot_publish_late_partitions() {
        let (session, adapter) = session(1, None);
        let mut partition = session.partition(PartitionId::new(0)).unwrap();

        session
            .fail(ProducerFailureReason::ExecutionFailed)
            .unwrap();
        let error = partition.seal(domain(10)).unwrap_err();

        assert_eq!(
            error.kind(),
            RuntimeContractViolationKind::ServiceUnavailable
        );
        assert_eq!(
            adapter.calls(),
            vec![AdapterCall::Fail(ProducerFailureReason::ExecutionFailed)]
        );
    }

    #[test]
    fn owner_drop_before_all_partition_committers_are_created_fails_session() {
        let (session, adapter) = session(2, None);
        let mut partition_0 = session.partition(PartitionId::new(0)).unwrap();

        drop(session);

        assert_eq!(
            adapter.calls(),
            vec![AdapterCall::Fail(ProducerFailureReason::ExecutionFailed)]
        );
        let error = partition_0.seal(domain(10)).unwrap_err();
        assert_eq!(
            error.kind(),
            RuntimeContractViolationKind::ServiceUnavailable
        );
    }

    #[test]
    fn selected_partition_submit_failure_stops_and_fails_without_materializing_subset() {
        let events = Arc::new(RecordingEvents::default());
        let memory = Arc::new(AcceptingMemory);
        let service = Arc::new(super::super::RuntimeFilterService::new_with_dependencies(
            UniqueId::new(0, 0),
            Arc::new(FixedClock(Instant::now())),
            events.clone(),
            memory,
        ));
        service
            .install(super::super::tests::compiled_fenced_final_install())
            .unwrap();
        let live = service
            .subscribe(
                BindingId::new(30),
                UniqueId::new(70, 10),
                SubscriptionKind::NonBlockingLive,
            )
            .unwrap()
            .into_live()
            .unwrap();
        let session = service
            .open_final_aggregate_producer(BindingId::new(10), UniqueId::new(70, 10), 3)
            .unwrap();
        service.inject_final_domain_submit_failure_for_test(
            BindingId::new(10),
            UniqueId::new(70, 10),
            PartitionId::new(1),
            ProducerSequence::new(0),
        );
        let mut partitions = [
            session.partition(PartitionId::new(0)).unwrap(),
            session.partition(PartitionId::new(1)).unwrap(),
            session.partition(PartitionId::new(2)).unwrap(),
        ];
        for (partition, value) in partitions.iter_mut().zip([10, 11, 12]) {
            partition.seal(domain(value)).unwrap();
        }
        partitions[0].close().unwrap();
        partitions[1].close().unwrap();

        let error = partitions[2].close().unwrap_err();

        assert_eq!(
            error.kind(),
            RuntimeContractViolationKind::ServiceUnavailable
        );
        assert!(live.snapshot().is_none());
        assert!(matches!(
            live.poll_after(None),
            LivePollOutcome::Idle {
                latest_version: None,
                terminal: Some(LiveTerminal::Unavailable(UnavailableReason::ProducerFailed)),
            }
        ));
        let events = events.0.lock().unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, RuntimeFilterEvent::ArtifactPublished { .. }))
                .count(),
            0
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event,
                    RuntimeFilterEvent::FinalDomainShardAccepted { .. }
                ))
                .count(),
            1
        );
    }
}
