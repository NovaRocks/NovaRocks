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

//! Worker-owned admission authority for task exchange destinations.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};

use novarocks_execution_contract::{
    ExchangeCloseDisposition, ExchangeDeliveryRequirement, ExchangeSource, ExchangeStreamClose,
    ExchangeStreamIdentity, FragmentNodeId, QueryContextRef, TaskDescriptor, TaskIdentity,
};
use novarocks_types::{QueryExecutionId, UniqueId};

use crate::{HostRejection, IngressRejection, authorize_inbound_frame};

const CAPABILITY_LOCK: &str = "task inbound capability lock";

/// Refusals protect exact protocol identities and receiver-authored facts.
#[derive(Debug)]
enum InboundCapabilityFailure {
    InvalidCloseFacts {
        task: TaskIdentity,
    },
    ConflictingCloseFacts {
        task: TaskIdentity,
    },
    ContextClosed {
        task: TaskIdentity,
    },
    KeyClosed {
        task: TaskIdentity,
        key: UniqueId,
        holder: TaskIdentity,
    },
    KeyClaimed {
        task: TaskIdentity,
        key: UniqueId,
        holder: TaskIdentity,
    },
}
impl std::fmt::Display for InboundCapabilityFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidCloseFacts { task } => write!(
                f,
                "task {task} normal-close facts do not cover its exact frozen streams and delivery obligations"
            ),
            Self::ConflictingCloseFacts { task } => write!(
                f,
                "task {task} normal-close facts conflict with its retained original record"
            ),
            Self::ContextClosed { task } => write!(
                f,
                "task {task} cannot install inbound capability after its query context closed"
            ),
            Self::KeyClosed { task, key, .. } => write!(
                f,
                "task {task} cannot reuse a normally closed kernel key {key}"
            ),
            Self::KeyClaimed { task, key, holder } => write!(
                f,
                "task {task} cannot claim kernel key {key}, which task {holder} already holds"
            ),
        }
    }
}
impl std::error::Error for InboundCapabilityFailure {}
impl novarocks_execution_contract::failure::FailureProjection for InboundCapabilityFailure {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use crate::failure_reason::WorkerFailureReason as R;
        use novarocks_execution_contract::failure::*;
        match self {
            Self::InvalidCloseFacts { .. } | Self::ConflictingCloseFacts { .. } => {
                FailureFact::new(FailureClass::Invariant, R::InboundCloseContract.reason())
            }
            Self::ContextClosed { .. } => {
                FailureFact::stop_observation(R::InboundContextClosed.reason())
            }
            Self::KeyClosed { task, holder, .. } => {
                if task == holder {
                    FailureFact::stop_observation(R::InboundKeyClosed.reason())
                } else {
                    FailureFact::new(FailureClass::Invariant, R::InboundForeignClosedKey.reason())
                }
            }
            Self::KeyClaimed { .. } => {
                FailureFact::new(FailureClass::Invariant, R::InboundKeyClaimed.reason())
            }
        }
    }
}
fn capability_rejection(source: InboundCapabilityFailure) -> HostRejection {
    let detail = novarocks_execution_contract::SafeDetail::truncating_display(&source);
    HostRejection::from_domain_with_detail(
        novarocks_execution_contract::TaskFailureCategory::Protocol,
        source,
        detail,
    )
}

/// Which task, if any, may receive one inbound exchange frame.
///
/// A descriptor already freezes its complete inbound topology, so the answer
/// is a single lookup on the kernel key the frame carries, followed by the
/// descriptor's own authorization. Installation is exclusive on that key:
/// two live tasks sharing one key would make a frame ambiguous.
#[derive(Debug, Default)]
struct TaskInboundCapabilityState {
    installed: HashMap<UniqueId, Arc<TaskDescriptor>>,
    /// A receiver-local gate keeps normal retirement ordered with in-flight
    /// delivery without serializing decode on unrelated destinations.
    delivery_gates: HashMap<UniqueId, Arc<RwLock<()>>>,
    normally_closed: HashMap<UniqueId, NormalClosedInboundRecord>,
    // Observation of these same owned records, updated at their actual map
    // mutations. This grants no capacity and never controls retention.
    normal_close_bytes_estimate: Option<usize>,
    normal_closed_executions: HashSet<QueryExecutionId>,
    /// A descriptor carries no frontend process identity. The worker context
    /// owner admits at most one context per execution, so the execution is
    /// the complete key this data-plane owner must fence.
    closed_executions: HashSet<QueryExecutionId>,
    aborted_executions: HashSet<QueryExecutionId>,
}

#[derive(Debug)]
pub struct TaskInboundCapabilities {
    state: Mutex<TaskInboundCapabilityState>,
}

use novarocks_worker_protocol::inbound_cost::NormalClosedInboundRecord;

/// Exact record count and documented structural estimate, never actual heap.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InboundRetentionSnapshot {
    pub records: usize,
    pub metadata_bytes_estimate: Option<usize>,
}
fn normal_close_record_bytes_estimate(record: &NormalClosedInboundRecord) -> Option<usize> {
    let inbound = record.inbound.capacity().checked_mul(std::mem::size_of::<
        novarocks_execution_contract::ExchangeInbound,
    >())?;
    let sources = record.inbound.iter().try_fold(0usize, |total, node| {
        total.checked_add(std::mem::size_of_val(node.sources()))
    })?;
    std::mem::size_of::<NormalClosedInboundRecord>()
        .checked_add(inbound)?
        .checked_add(sources)?
        .checked_add(std::mem::size_of_val(record.closes.as_ref()))
}
impl TaskInboundCapabilityState {
    fn insert_normal_close(&mut self, key: UniqueId, record: NormalClosedInboundRecord) {
        let bytes = normal_close_record_bytes_estimate(&record);
        let previous = if self.normally_closed.is_empty() {
            Some(0)
        } else {
            self.normal_close_bytes_estimate
        };
        let old = self.normally_closed.insert(key, record);
        self.normal_close_bytes_estimate = previous.zip(bytes).and_then(|(n, added)| {
            n.checked_sub(match old.as_ref() {
                Some(record) => normal_close_record_bytes_estimate(record)?,
                None => 0,
            })?
            .checked_add(added)
        });
    }
    fn forget_normal_close(&mut self, execution: QueryExecutionId) {
        let mut removed = Some(0usize);
        self.normally_closed.retain(|_, record| {
            if record.destination.query_execution_id() != execution {
                return true;
            }
            removed = removed
                .zip(normal_close_record_bytes_estimate(record))
                .and_then(|(n, size)| n.checked_add(size));
            false
        });
        self.normal_close_bytes_estimate = if self.normally_closed.is_empty() {
            Some(0)
        } else {
            self.normal_close_bytes_estimate
                .zip(removed)
                .and_then(|(n, gone)| n.checked_sub(gone))
        };
    }
}

/// An exact normal-close answer to a frozen sender of one input occurrence.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct NormalClosedInbound {
    // The close fact is the sole author of the exact stream endpoints.
    sender_count: u32,
    close: ExchangeStreamClose,
}

impl NormalClosedInbound {
    pub const fn close(self) -> ExchangeStreamClose {
        self.close
    }

    pub const fn destination(self) -> TaskIdentity {
        self.close.stream.destination()
    }
    pub const fn destination_kernel_key(self) -> UniqueId {
        self.close.stream.destination_fragment_instance_id()
    }
    pub const fn destination_node_id(self) -> FragmentNodeId {
        self.close.stream.destination_node()
    }
    pub const fn source(self) -> ExchangeSource {
        self.close.stream.source()
    }
    pub const fn sender_count(self) -> u32 {
        self.sender_count
    }
}

/// The task and frozen source an admitted frame belongs to.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct InboundFrameAdmission {
    destination: TaskIdentity,
    source: ExchangeSource,
    sender_count: u32,
}

impl InboundFrameAdmission {
    pub const fn sender_count(self) -> u32 {
        self.sender_count
    }
    pub const fn destination(self) -> TaskIdentity {
        self.destination
    }

    pub const fn source(self) -> ExchangeSource {
        self.source
    }
}

/// The worker's ownership verdict for an inbound exchange destination.
///
/// `NotHeld` is intentionally distinct from `Refused`: another independent
/// destination owner may hold the same kernel key in a composed data plane.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InboundFrameClaim {
    NotHeld,
    Authorized,
    NormallyClosed(NormalClosedInbound),
    Refused(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InboundFrameDelivery<E> {
    Claim(InboundFrameClaim),
    Receiver(E),
}

impl TaskInboundCapabilities {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(TaskInboundCapabilityState::default()),
        })
    }

    /// Original record inventory, sampled without waiting for the owner lock.
    /// Estimate coverage matches normal_close_retention; allocator, map nodes,
    /// shared source allocations and Context fences are unmeasured.
    pub fn try_normal_close_retention_snapshot(&self) -> Option<InboundRetentionSnapshot> {
        let state = self.state.try_lock().ok()?;
        Some(InboundRetentionSnapshot {
            records: state.normally_closed.len(),
            metadata_bytes_estimate: if state.normally_closed.is_empty() {
                Some(0)
            } else {
                state.normal_close_bytes_estimate
            },
        })
    }

    /// Existing estimate API. This is neither allocation/RSS nor a heap bound.
    /// Metrics use the nonblocking typed snapshot and preserve unknown values.
    pub fn normal_close_retention(&self) -> (usize, usize) {
        let state = self.state.lock().expect(CAPABILITY_LOCK);
        let bytes = state
            .normally_closed
            .values()
            .fold(0usize, |total, record| {
                total.saturating_add(
                    normal_close_record_bytes_estimate(record).unwrap_or(usize::MAX),
                )
            });
        (state.normally_closed.len(), bytes)
    }

    /// Authorizes one frame from only frozen task facts.
    pub fn authorize_frame(
        &self,
        destination_kernel_key: UniqueId,
        destination_node_id: FragmentNodeId,
        source_kernel_key: UniqueId,
        sender_ordinal: u32,
        sender_count: u32,
    ) -> Result<InboundFrameAdmission, IngressRejection> {
        let descriptor = {
            let state = self.state.lock().expect(CAPABILITY_LOCK);
            let descriptor = state
                .installed
                .get(&destination_kernel_key)
                .map(Arc::clone)
                .ok_or(IngressRejection::UnknownDestinationTask)?;
            if state
                .aborted_executions
                .contains(&descriptor.identity().query_execution_id())
            {
                return Err(IngressRejection::UnknownDestinationTask);
            }
            descriptor
        };
        let source = authorize_inbound_frame(
            &descriptor,
            destination_kernel_key,
            destination_node_id,
            source_kernel_key,
            sender_ordinal,
            sender_count,
        )?;
        Ok(InboundFrameAdmission {
            destination: descriptor.identity(),
            source,
            sender_count,
        })
    }

    /// Resolve the complete source/destination identity from frozen facts.
    /// The sender count is authored by the descriptor, never by a request.
    pub fn claim_stream(
        &self,
        stream: ExchangeStreamIdentity,
    ) -> Result<InboundFrameAdmission, InboundFrameClaim> {
        Self::claim_stream_locked(&self.state.lock().expect(CAPABILITY_LOCK), stream)
    }

    /// Synchronously deliver only while the exact stream remains installed.
    /// The existing receiver-local read fence orders this callback with normal
    /// retirement; the process capability lock is released before the callback.
    /// The callback must not perform network I/O or wait for external capacity.
    pub fn with_claimed_stream<T, E>(
        &self,
        stream: ExchangeStreamIdentity,
        claim: impl FnOnce(InboundFrameAdmission) -> Result<T, E>,
    ) -> Result<Result<T, E>, InboundFrameClaim> {
        let gate = self
            .state
            .lock()
            .expect(CAPABILITY_LOCK)
            .delivery_gates
            .get(&stream.destination_fragment_instance_id())
            .cloned();
        let _delivery = gate
            .as_ref()
            .map(|gate| gate.read().expect("task inbound delivery gate"));
        let admission =
            Self::claim_stream_locked(&self.state.lock().expect(CAPABILITY_LOCK), stream)?;
        Ok(claim(admission))
    }

    fn claim_stream_locked(
        state: &TaskInboundCapabilityState,
        stream: ExchangeStreamIdentity,
    ) -> Result<InboundFrameAdmission, InboundFrameClaim> {
        let key = stream.destination_fragment_instance_id();
        let (destination, inbound) = if let Some(closed) = state.normally_closed.get(&key) {
            (closed.destination, closed.inbound.as_slice())
        } else if let Some(descriptor) = state.installed.get(&key) {
            (descriptor.identity(), descriptor.topology().inbound())
        } else {
            return Err(InboundFrameClaim::NotHeld);
        };
        if destination != stream.destination() {
            return Err(InboundFrameClaim::Refused(
                "exchange stream names a different destination Task".into(),
            ));
        }
        let node = inbound
            .iter()
            .find(|node| node.node_id() == stream.destination_node())
            .ok_or_else(|| {
                InboundFrameClaim::Refused("exchange stream destination node was not frozen".into())
            })?;
        if !node.sources().contains(&stream.source()) {
            return Err(InboundFrameClaim::Refused(
                "exchange stream source Task, kernel, or ordinal was not frozen".into(),
            ));
        }
        let count = node.expected_sender_count().get();
        match Self::claim_locked(
            state,
            key,
            stream.destination_node(),
            stream.source().fragment_instance_id(),
            stream.source().sender_ordinal(),
            count,
        ) {
            InboundFrameClaim::Authorized => Ok(InboundFrameAdmission {
                destination,
                source: stream.source(),
                sender_count: count,
            }),
            claim => Err(claim),
        }
    }

    /// States whether this worker owns a destination and, if it does, whether
    /// the frozen route is legal. The wire adapter composes this verdict with
    /// any other destination owners.
    pub fn claim_frame(
        &self,
        destination_kernel_key: UniqueId,
        destination_node_id: FragmentNodeId,
        source_kernel_key: UniqueId,
        sender_ordinal: u32,
        sender_count: u32,
    ) -> InboundFrameClaim {
        let state = self.state.lock().expect(CAPABILITY_LOCK);
        Self::claim_locked(
            &state,
            destination_kernel_key,
            destination_node_id,
            source_kernel_key,
            sender_ordinal,
            sender_count,
        )
    }

    /// Concurrent senders may decode under a shared receiver-local gate.
    /// Normal retirement takes its exclusive side before removing the route.
    pub fn deliver_frame(
        &self,
        destination_kernel_key: UniqueId,
        destination_node_id: FragmentNodeId,
        source_kernel_key: UniqueId,
        sender_ordinal: u32,
        sender_count: u32,
        deliver: impl FnOnce() -> Result<(), String>,
    ) -> InboundFrameClaim {
        match self.deliver_frame_with_receiver_error(
            destination_kernel_key,
            destination_node_id,
            source_kernel_key,
            sender_ordinal,
            sender_count,
            deliver,
        ) {
            InboundFrameDelivery::Claim(claim) => claim,
            InboundFrameDelivery::Receiver(error) => InboundFrameClaim::Refused(error),
        }
    }

    pub fn deliver_frame_with_receiver_error<E>(
        &self,
        destination_kernel_key: UniqueId,
        destination_node_id: FragmentNodeId,
        source_kernel_key: UniqueId,
        sender_ordinal: u32,
        sender_count: u32,
        deliver: impl FnOnce() -> Result<(), E>,
    ) -> InboundFrameDelivery<E> {
        let gate = self
            .state
            .lock()
            .expect(CAPABILITY_LOCK)
            .delivery_gates
            .get(&destination_kernel_key)
            .cloned();
        let Some(gate) = gate else {
            return InboundFrameDelivery::Claim(self.claim_frame(
                destination_kernel_key,
                destination_node_id,
                source_kernel_key,
                sender_ordinal,
                sender_count,
            ));
        };
        let _delivery = gate.read().expect("task inbound delivery gate");
        let claim = Self::claim_locked(
            &self.state.lock().expect(CAPABILITY_LOCK),
            destination_kernel_key,
            destination_node_id,
            source_kernel_key,
            sender_ordinal,
            sender_count,
        );
        if claim != InboundFrameClaim::Authorized {
            return InboundFrameDelivery::Claim(claim);
        }
        match deliver() {
            Ok(()) => InboundFrameDelivery::Claim(InboundFrameClaim::Authorized),
            Err(error) => InboundFrameDelivery::Receiver(error),
        }
    }

    fn claim_locked(
        state: &TaskInboundCapabilityState,
        destination_kernel_key: UniqueId,
        destination_node_id: FragmentNodeId,
        source_kernel_key: UniqueId,
        sender_ordinal: u32,
        sender_count: u32,
    ) -> InboundFrameClaim {
        if let Some(closed) = state.normally_closed.get(&destination_kernel_key) {
            let Some(node) = closed
                .inbound
                .iter()
                .find(|node| node.node_id() == destination_node_id)
            else {
                return InboundFrameClaim::Refused(format!(
                    "normally closed task {} has no input node {:?}",
                    closed.destination, destination_node_id
                ));
            };
            let Some(source) = node.source_by_kernel_key(source_kernel_key) else {
                return InboundFrameClaim::Refused(format!(
                    "normally closed task {} did not freeze this sender",
                    closed.destination
                ));
            };
            let expected = node.expected_sender_count().get();
            if sender_count != expected || sender_ordinal != source.sender_ordinal() {
                return InboundFrameClaim::Refused(format!(
                    "normally closed task {} received a sender ordinal/count mismatch",
                    closed.destination
                ));
            }
            let stream = ExchangeStreamIdentity::try_new(
                source,
                closed.destination,
                destination_kernel_key,
                destination_node_id,
            )
            .expect("frozen inbound stream identity");
            let Ok(index) = closed
                .closes
                .binary_search_by_key(&stream, |close| close.stream)
            else {
                return InboundFrameClaim::Refused(
                    "normally closed input omits its exact source close fact".into(),
                );
            };
            let close = closed.closes[index];
            return InboundFrameClaim::NormallyClosed(NormalClosedInbound {
                close,
                sender_count: expected,
            });
        }
        let Some(descriptor) = state.installed.get(&destination_kernel_key) else {
            return InboundFrameClaim::NotHeld;
        };
        if state
            .aborted_executions
            .contains(&descriptor.identity().query_execution_id())
        {
            return InboundFrameClaim::Refused(format!(
                "task {} belongs to a query context whose data-plane admission is closed",
                descriptor.identity()
            ));
        }
        match authorize_inbound_frame(
            descriptor,
            destination_kernel_key,
            destination_node_id,
            source_kernel_key,
            sender_ordinal,
            sender_count,
        ) {
            Ok(_) => InboundFrameClaim::Authorized,
            Err(rejection) => InboundFrameClaim::Refused(format!(
                "task {} froze this destination but {rejection}",
                descriptor.identity()
            )),
        }
    }

    pub fn len(&self) -> usize {
        self.state.lock().expect(CAPABILITY_LOCK).installed.len()
    }

    pub fn retains_normal_close(&self, context: QueryContextRef) -> bool {
        self.state
            .lock()
            .expect(CAPABILITY_LOCK)
            .normal_closed_executions
            .contains(&context.query_execution_id())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn install(&self, descriptor: Arc<TaskDescriptor>) -> Result<(), HostRejection> {
        let key = descriptor.fragment_instance_id();
        let mut state = self.state.lock().expect(CAPABILITY_LOCK);
        if state
            .closed_executions
            .contains(&descriptor.identity().query_execution_id())
        {
            return Err(capability_rejection(
                InboundCapabilityFailure::ContextClosed {
                    task: descriptor.identity(),
                },
            ));
        }
        if let Some(closed) = state.normally_closed.get(&key) {
            return Err(capability_rejection(InboundCapabilityFailure::KeyClosed {
                task: descriptor.identity(),
                key,
                holder: closed.destination,
            }));
        }
        if let Some(existing) = state.installed.get(&key) {
            return Err(capability_rejection(InboundCapabilityFailure::KeyClaimed {
                task: descriptor.identity(),
                key,
                holder: existing.identity(),
            }));
        }
        state.installed.insert(key, descriptor);
        if !state.installed[&key].topology().inbound().is_empty() {
            state.delivery_gates.insert(key, Arc::new(RwLock::new(())));
        }
        Ok(())
    }

    /// Normal closure fences new receiver installation, while an already
    /// installed exact stream keeps its DATA permission until actual retirement.
    /// Only abort revokes that existing data-plane permission.
    pub fn close_context(&self, context: QueryContextRef) {
        self.state
            .lock()
            .expect(CAPABILITY_LOCK)
            .closed_executions
            .insert(context.query_execution_id());
    }

    pub fn abort_context(&self, context: QueryContextRef) {
        let mut state = self.state.lock().expect(CAPABILITY_LOCK);
        let execution = context.query_execution_id();
        state.closed_executions.insert(execution);
        state.aborted_executions.insert(execution);
        state.normal_closed_executions.remove(&execution);
        state.forget_normal_close(execution);
    }

    /// The original Context owner calls this only at its request horizon, or
    /// after process ingress has closed and every installed receiver exited.
    /// Hot execution retirement alone never forgets retained close facts.
    pub fn forget_context(&self, context: QueryContextRef) {
        let mut state = self.state.lock().expect(CAPABILITY_LOCK);
        let execution = context.query_execution_id();
        assert!(
            state
                .installed
                .values()
                .all(|descriptor| { descriptor.identity().query_execution_id() != execution }),
            "Context forgetting requires every original receiver to exit"
        );
        state.closed_executions.remove(&execution);
        state.aborted_executions.remove(&execution);
        state.normal_closed_executions.remove(&execution);
        state.forget_normal_close(execution);
        let live_keys: HashSet<_> = state
            .installed
            .keys()
            .chain(state.normally_closed.keys())
            .copied()
            .collect();
        state
            .delivery_gates
            .retain(|key, _| live_keys.contains(key));
    }

    /// Withdraws exactly this task's capability. Identity is re-checked so a
    /// stale rollback cannot evict a newer owner of the same kernel key.
    pub fn remove(&self, descriptor: &TaskDescriptor) {
        self.remove_with_capture(descriptor, || ());
    }

    /// Move the original receiver capture while the same delivery writer
    /// fence excludes every already-claimed synchronous push. `detach` may
    /// only move ownership/metadata; callbacks, source destruction and actual
    /// receiver cleanup belong to the supervised host after this returns.
    /// No capability-state lock is held while moving the original capture.
    pub fn remove_with_capture<T>(
        &self,
        descriptor: &TaskDescriptor,
        detach: impl FnOnce() -> T,
    ) -> T {
        let key = descriptor.fragment_instance_id();
        let gate = self
            .state
            .lock()
            .expect(CAPABILITY_LOCK)
            .delivery_gates
            .get(&key)
            .cloned();
        let _retirement = gate
            .as_ref()
            .map(|gate| gate.write().expect("task inbound delivery gate"));
        let mut state = self.state.lock().expect(CAPABILITY_LOCK);
        if state
            .installed
            .get(&key)
            .is_some_and(|held| held.identity() == descriptor.identity())
        {
            state.installed.remove(&key);
            state.delivery_gates.remove(&key);
        }
        drop(state);
        let captured = detach();
        drop(_retirement);
        captured
    }

    /// Existing DemandDriven retirement adapter. It records explicit withdrawal,
    /// never consumption. This void adapter is transitional: the strict host
    /// composition must replace it with receiver-authored facts and the checked
    /// API below, with its pre-success validation, before MustDrain is enabled.
    pub fn close_normally(&self, descriptor: &TaskDescriptor, retire_receiver: impl FnOnce()) {
        let mut closes = descriptor
            .topology()
            .inbound()
            .iter()
            .flat_map(|node| {
                node.sources()
                    .iter()
                    .map(move |source| ExchangeStreamClose {
                        stream: ExchangeStreamIdentity::try_new(
                            *source,
                            descriptor.identity(),
                            descriptor.fragment_instance_id(),
                            node.node_id(),
                        )
                        .expect("frozen inbound stream identity"),
                        disposition: ExchangeCloseDisposition::Withdrawn,
                    })
            })
            .collect::<Vec<_>>();
        closes.sort_unstable_by_key(|close| close.stream);
        self.close_normally_record(descriptor, closes.into_boxed_slice(), retire_receiver)
            .expect("the same legacy normal-close author repeats its original withdrawal facts");
    }

    /// Retain actual receiver-authored facts, covering every frozen stream once.
    /// Validation refuses strict withdrawal before invoking retirement. This is
    /// not the pre-success Task hook: the host must install that separately and
    /// propagate this Result rather than discarding a strict close violation.
    pub fn close_normally_with_facts(
        &self,
        descriptor: &TaskDescriptor,
        mut closes: Vec<ExchangeStreamClose>,
        retire_receiver: impl FnOnce(),
    ) -> Result<(), HostRejection> {
        closes.sort_unstable_by_key(|close| close.stream);
        Self::validate_normal_close_facts(descriptor, &closes)?;
        self.close_normally_record(descriptor, closes.into_boxed_slice(), retire_receiver)
    }

    /// Check the receiver author's exact, canonical facts before publishing
    /// successful Task completion. This grants no route or close authority;
    /// the later writer-fenced close records the same exact facts.
    pub fn validate_normal_close_facts(
        descriptor: &TaskDescriptor,
        closes: &[ExchangeStreamClose],
    ) -> Result<(), HostRejection> {
        let invalid = || {
            capability_rejection(InboundCapabilityFailure::InvalidCloseFacts {
                task: descriptor.identity(),
            })
        };
        let expected = descriptor
            .topology()
            .inbound()
            .iter()
            .try_fold(0usize, |count, node| {
                count.checked_add(node.sources().len())
            })
            .ok_or_else(invalid)?;
        if closes.len() != expected {
            return Err(invalid());
        }
        if closes
            .windows(2)
            .any(|pair| pair[0].stream >= pair[1].stream)
        {
            return Err(invalid());
        }
        for node in descriptor.topology().inbound() {
            for source in node.sources() {
                let stream = ExchangeStreamIdentity::try_new(
                    *source,
                    descriptor.identity(),
                    descriptor.fragment_instance_id(),
                    node.node_id(),
                )
                .map_err(|_| invalid())?;
                let index = closes
                    .binary_search_by_key(&stream, |close| close.stream)
                    .map_err(|_| invalid())?;
                if node.delivery() == ExchangeDeliveryRequirement::MustDrain
                    && !matches!(
                        closes[index].disposition,
                        ExchangeCloseDisposition::Consumed { .. }
                    )
                {
                    return Err(invalid());
                }
            }
        }
        Ok(())
    }

    /// Atomically replaces the installed capability while holding the existing
    /// delivery writer fence. A pending synchronous delivery rechecks its claim.
    fn close_normally_record(
        &self,
        descriptor: &TaskDescriptor,
        closes: Box<[ExchangeStreamClose]>,
        retire_receiver: impl FnOnce(),
    ) -> Result<(), HostRejection> {
        let key = descriptor.fragment_instance_id();
        let gate = self
            .state
            .lock()
            .expect(CAPABILITY_LOCK)
            .delivery_gates
            .get(&key)
            .cloned();
        let _retirement = gate
            .as_ref()
            .map(|gate| gate.write().expect("task inbound delivery gate"));
        {
            let state = self.state.lock().expect(CAPABILITY_LOCK);
            if let Some(original) = state.normally_closed.get(&key)
                && (original.destination != descriptor.identity() || original.closes != closes)
            {
                return Err(capability_rejection(
                    InboundCapabilityFailure::ConflictingCloseFacts {
                        task: descriptor.identity(),
                    },
                ));
            }
        }
        // No further push can begin for this destination while the receiver
        // route is removed; a pending delivery rechecks the final verdict.
        retire_receiver();
        let mut state = self.state.lock().expect(CAPABILITY_LOCK);
        let held = state
            .installed
            .get(&key)
            .is_some_and(|held| held.identity() == descriptor.identity());
        if held
            && !state
                .aborted_executions
                .contains(&descriptor.identity().query_execution_id())
        {
            state.installed.remove(&key);
            if !descriptor.topology().inbound().is_empty() {
                state.insert_normal_close(
                    key,
                    NormalClosedInboundRecord {
                        destination: descriptor.identity(),
                        inbound: descriptor.topology().inbound().to_vec(),
                        closes,
                    },
                );
                state
                    .normal_closed_executions
                    .insert(descriptor.identity().query_execution_id());
            }
        } else if held {
            state.installed.remove(&key);
        }
        drop(state);
        drop(_retirement);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        num::NonZeroUsize,
        sync::{Arc, mpsc},
        time::Duration,
    };

    use novarocks_execution_contract::{
        ExchangeInbound, ExchangeSource, ExchangeTopology, FragmentNodeId, QueryContextRef,
        TaskDescriptor, TaskIdentity,
    };
    use novarocks_types::{
        UniqueId,
        identity::{
            AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId,
            TaskId,
        },
    };

    use super::{InboundFrameClaim, InboundRetentionSnapshot, TaskInboundCapabilities};

    fn descriptor(query: i64, kernel: UniqueId) -> (TaskDescriptor, UniqueId) {
        let execution =
            QueryExecutionId::new(QueryId::new(query, query + 1), AttemptId::new(1).unwrap())
                .unwrap();
        let destination = TaskIdentity::new(
            execution,
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        );
        let source = TaskIdentity::new(
            execution,
            StageId::new(2).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        );
        let source_kernel = UniqueId::new(query + 10, query + 11);
        let topology = ExchangeTopology::try_new(
            Vec::new(),
            vec![
                ExchangeInbound::try_new(
                    FragmentNodeId::new(7),
                    vec![ExchangeSource::new(source, source_kernel, 0)],
                    novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
                )
                .unwrap(),
            ],
        )
        .unwrap();
        (
            TaskDescriptor::try_new(
                destination,
                kernel,
                NonZeroUsize::new(1).unwrap(),
                Vec::new(),
                topology,
            )
            .unwrap(),
            source_kernel,
        )
    }

    fn claim(
        capabilities: &TaskInboundCapabilities,
        kernel: UniqueId,
        source: UniqueId,
        ordinal: u32,
        count: u32,
    ) -> InboundFrameClaim {
        capabilities.claim_frame(kernel, FragmentNodeId::new(7), source, ordinal, count)
    }

    #[test]
    fn retained_normal_close_does_not_block_new_destination_installation() {
        use novarocks_execution_contract::failure::FailureProjection;
        let capabilities = TaskInboundCapabilities::new();
        let (first, source) = descriptor(101, UniqueId::new(1, 2));
        let (second, second_source) = descriptor(201, UniqueId::new(3, 4));
        capabilities.install(Arc::new(first.clone())).unwrap();
        assert_eq!(capabilities.normal_close_retention(), (0, 0));
        capabilities.close_normally(&first, || {});
        let retained = capabilities.normal_close_retention();
        assert_eq!(retained.0, 1);
        assert!(retained.1 > 0);
        let closed = capabilities.install(Arc::new(first.clone())).unwrap_err();
        assert_eq!(
            closed.failure_fact().evidence(),
            novarocks_execution_contract::failure::FailureEvidence::StopObservation
        );
        capabilities.install(Arc::new(second.clone())).unwrap();
        assert_eq!(capabilities.normal_close_retention(), retained);
        assert!(matches!(
            claim(&capabilities, first.fragment_instance_id(), source, 0, 1),
            InboundFrameClaim::NormallyClosed(_)
        ));
        assert_eq!(
            claim(
                &capabilities,
                second.fragment_instance_id(),
                second_source,
                0,
                1
            ),
            InboundFrameClaim::Authorized
        );
        let context = QueryContextRef::new(
            first.identity().query_execution_id(),
            FrontendProcessId::new_v7(),
            first.identity().backend_process_id(),
        );
        capabilities.forget_context(context);
        assert_eq!(capabilities.normal_close_retention(), (0, 0));
        assert_eq!(
            claim(&capabilities, first.fragment_instance_id(), source, 0, 1),
            InboundFrameClaim::NotHeld
        );
        assert_eq!(
            claim(
                &capabilities,
                second.fragment_instance_id(),
                second_source,
                0,
                1
            ),
            InboundFrameClaim::Authorized
        );
        capabilities.remove(&second);
    }

    #[test]
    fn retained_normal_close_keeps_original_facts_across_replay_and_late_removal() {
        use novarocks_execution_contract::{ExchangeCloseDisposition, ExchangeFrameSequence};
        use std::cell::Cell;
        let owner = TaskInboundCapabilities::new();
        let (first, source) = descriptor(111, UniqueId::new(11, 12));
        let (second, _) = descriptor(211, UniqueId::new(13, 14));
        owner.install(Arc::new(first.clone())).unwrap();
        let callbacks = Cell::new(0);
        owner.close_normally(&first, || callbacks.set(callbacks.get() + 1));
        let InboundFrameClaim::NormallyClosed(original) =
            claim(&owner, first.fragment_instance_id(), source, 0, 1)
        else {
            panic!("the original exact normal-close record remains visible");
        };
        let original = original.close();
        let retention = owner.normal_close_retention();
        assert_eq!(retention.0, 1);
        assert!(retention.1 > 0);
        owner.install(Arc::new(second.clone())).unwrap();
        owner
            .close_normally_with_facts(&first, vec![original], || {
                callbacks.set(callbacks.get() + 1);
            })
            .unwrap();
        assert_eq!(callbacks.get(), 2);
        assert_eq!(owner.normal_close_retention(), retention);

        owner.remove(&first);
        assert_eq!(owner.normal_close_retention(), retention);
        let mut conflicting = original;
        conflicting.disposition = ExchangeCloseDisposition::Consumed {
            final_sequence: ExchangeFrameSequence::FIRST,
        };
        assert!(
            owner
                .close_normally_with_facts(&first, vec![conflicting], || {
                    callbacks.set(callbacks.get() + 1);
                })
                .is_err(),
            "a replay cannot replace the original receiver-authored close fact"
        );
        assert_eq!(callbacks.get(), 2, "refusal precedes metadata detachment");
        let InboundFrameClaim::NormallyClosed(retained) =
            claim(&owner, first.fragment_instance_id(), source, 0, 1)
        else {
            panic!("a conflicting replay cannot erase the original record");
        };
        assert_eq!(retained.close(), original);
        assert_eq!(owner.normal_close_retention(), retention);

        owner.forget_context(QueryContextRef::new(
            first.identity().query_execution_id(),
            FrontendProcessId::new_v7(),
            first.identity().backend_process_id(),
        ));
        assert_eq!(owner.normal_close_retention(), (0, 0));
        owner.remove(&second);
    }

    #[test]
    fn normal_close_metadata_grows_with_actual_records_until_each_context_forgets() {
        let (first, first_source) = descriptor(102, UniqueId::new(101, 102));
        let (second, second_source) = descriptor(202, UniqueId::new(201, 202));
        let (third, _) = descriptor(302, UniqueId::new(301, 302));
        let capabilities = TaskInboundCapabilities::new();
        capabilities.install(Arc::new(first.clone())).unwrap();
        capabilities.close_normally(&first, || {});
        let first_retained = capabilities.normal_close_retention();
        assert_eq!(first_retained.0, 1);
        assert!(first_retained.1 > 0);
        capabilities.install(Arc::new(second.clone())).unwrap();
        capabilities.install(Arc::new(third.clone())).unwrap();
        assert_eq!(capabilities.normal_close_retention(), first_retained);
        capabilities.close_normally(&second, || {});
        let both_retained = capabilities.normal_close_retention();
        assert_eq!(both_retained.0, 2);
        assert_eq!(
            capabilities.try_normal_close_retention_snapshot(),
            Some(InboundRetentionSnapshot {
                records: both_retained.0,
                metadata_bytes_estimate: Some(both_retained.1),
            })
        );
        {
            let _original_lock = capabilities.state.lock().unwrap();
            assert!(capabilities.try_normal_close_retention_snapshot().is_none());
        }
        assert!(both_retained.1 > first_retained.1);
        assert!(matches!(
            claim(
                &capabilities,
                first.fragment_instance_id(),
                first_source,
                0,
                1
            ),
            InboundFrameClaim::NormallyClosed(_)
        ));
        capabilities.remove(&third);
        assert_eq!(capabilities.normal_close_retention(), both_retained);
        capabilities.forget_context(QueryContextRef::new(
            first.identity().query_execution_id(),
            FrontendProcessId::new_v7(),
            first.identity().backend_process_id(),
        ));
        assert_eq!(capabilities.normal_close_retention().0, 1);
        let remaining = capabilities.normal_close_retention();
        assert_eq!(
            capabilities.try_normal_close_retention_snapshot(),
            Some(InboundRetentionSnapshot {
                records: remaining.0,
                metadata_bytes_estimate: Some(remaining.1),
            })
        );
        assert!(matches!(
            claim(
                &capabilities,
                second.fragment_instance_id(),
                second_source,
                0,
                1
            ),
            InboundFrameClaim::NormallyClosed(_)
        ));
        capabilities.forget_context(QueryContextRef::new(
            second.identity().query_execution_id(),
            FrontendProcessId::new_v7(),
            second.identity().backend_process_id(),
        ));
        assert_eq!(capabilities.normal_close_retention(), (0, 0));
        assert_eq!(
            capabilities.try_normal_close_retention_snapshot(),
            Some(InboundRetentionSnapshot {
                records: 0,
                metadata_bytes_estimate: Some(0),
            })
        );
        assert!(
            capabilities
                .state
                .lock()
                .unwrap()
                .normally_closed
                .is_empty()
        );
    }

    #[test]
    fn only_exact_frozen_input_receives_normal_close() {
        let capabilities = TaskInboundCapabilities::new();
        let kernel = UniqueId::new(5, 6);
        let (descriptor, source) = descriptor(301, kernel);
        capabilities.install(Arc::new(descriptor.clone())).unwrap();
        let mut delivered = false;
        assert_eq!(
            capabilities.deliver_frame(kernel, FragmentNodeId::new(7), source, 0, 1, || {
                delivered = true;
                Ok(())
            }),
            InboundFrameClaim::Authorized
        );
        assert!(delivered);
        capabilities.close_normally(&descriptor, || {});
        let mut delivered_after_close = false;
        let exact =
            capabilities.deliver_frame(kernel, FragmentNodeId::new(7), source, 0, 1, || {
                delivered_after_close = true;
                Ok(())
            });
        let InboundFrameClaim::NormallyClosed(proof) = exact else {
            panic!("exact frozen route must receive normal-close proof")
        };
        assert_eq!(proof.destination(), descriptor.identity());
        assert!(!delivered_after_close);
        for mismatch in [
            claim(&capabilities, kernel, UniqueId::new(99, 99), 0, 1),
            claim(&capabilities, kernel, source, 1, 1),
            claim(&capabilities, kernel, source, 0, 2),
        ] {
            assert!(matches!(mismatch, InboundFrameClaim::Refused(_)));
        }
    }

    #[test]
    fn abort_revokes_prior_normal_close_evidence() {
        let capabilities = TaskInboundCapabilities::new();
        let kernel = UniqueId::new(8, 9);
        let (descriptor, source) = descriptor(401, kernel);
        capabilities.install(Arc::new(descriptor.clone())).unwrap();
        let context = QueryContextRef::new(
            descriptor.identity().query_execution_id(),
            FrontendProcessId::new_v7(),
            descriptor.identity().backend_process_id(),
        );
        capabilities.close_context(context);
        assert_eq!(
            claim(&capabilities, kernel, source, 0, 1),
            InboundFrameClaim::Authorized,
            "normal closure preserves this already installed exact stream"
        );
        capabilities
            .authorize_frame(kernel, FragmentNodeId::new(7), source, 0, 1)
            .expect("the same installed DATA admission remains valid");
        let later = TaskDescriptor::try_new(
            TaskIdentity::new(
                context.query_execution_id(),
                descriptor.identity().stage_id(),
                TaskId::new(2).unwrap(),
                context.backend_process_id(),
            ),
            UniqueId::new(10, 11),
            descriptor.pipeline_dop(),
            Vec::new(),
            descriptor.topology().clone(),
        )
        .unwrap();
        assert!(
            capabilities.install(Arc::new(later)).is_err(),
            "the same normal closure still fences a new destination"
        );
        capabilities.close_normally(&descriptor, || {});
        capabilities.abort_context(context);
        assert_eq!(
            claim(&capabilities, kernel, source, 0, 1),
            InboundFrameClaim::NotHeld
        );
    }

    #[test]
    fn normal_close_waits_for_its_receiver_without_blocking_other_destinations() {
        let capabilities = TaskInboundCapabilities::new();
        let first_key = UniqueId::new(31, 32);
        let second_key = UniqueId::new(41, 42);
        let (first, first_source) = descriptor(501, first_key);
        let (second, second_source) = descriptor(601, second_key);
        capabilities.install(Arc::new(first.clone())).unwrap();
        capabilities.install(Arc::new(second)).unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let delivering = Arc::clone(&capabilities);
        let first_push = std::thread::spawn(move || {
            delivering.deliver_frame(
                first_key,
                FragmentNodeId::new(7),
                first_source,
                0,
                1,
                || {
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    Ok(())
                },
            )
        });
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let (closed_tx, closed_rx) = mpsc::channel();
        let closing = Arc::clone(&capabilities);
        let close = std::thread::spawn(move || {
            closing.close_normally(&first, || {
                closed_tx.send(()).unwrap();
            })
        });
        assert_eq!(
            capabilities.deliver_frame(
                second_key,
                FragmentNodeId::new(7),
                second_source,
                0,
                1,
                || Ok(())
            ),
            InboundFrameClaim::Authorized
        );
        assert!(
            closed_rx.recv_timeout(Duration::from_millis(20)).is_err(),
            "retirement must wait for accepted delivery"
        );
        release_tx.send(()).unwrap();
        assert_eq!(first_push.join().unwrap(), InboundFrameClaim::Authorized);
        close.join().unwrap();
        assert!(matches!(
            claim(&capabilities, first_key, first_source, 0, 1),
            InboundFrameClaim::NormallyClosed(_)
        ));
        assert!(matches!(
            claim(&capabilities, second_key, second_source, 0, 1),
            InboundFrameClaim::Authorized
        ));
    }
    #[test]
    fn exact_stream_claim_uses_frozen_tasks_and_count() {
        use novarocks_execution_contract::{ExchangeSource, ExchangeStreamIdentity};
        let capabilities = TaskInboundCapabilities::new();
        let (descriptor, _) = descriptor(501, UniqueId::new(51, 52));
        capabilities.install(Arc::new(descriptor.clone())).unwrap();
        let node = &descriptor.topology().inbound()[0];
        let source = node.sources()[0];
        let stream = ExchangeStreamIdentity::try_new(
            source,
            descriptor.identity(),
            descriptor.fragment_instance_id(),
            node.node_id(),
        )
        .unwrap();
        assert_eq!(capabilities.claim_stream(stream).unwrap().sender_count(), 1);
        let forged = TaskIdentity::new(
            source.task().query_execution_id(),
            source.task().stage_id(),
            source.task().task_id(),
            BackendProcessId::new_v7(),
        );
        let forged_stream = ExchangeStreamIdentity::try_new(
            ExchangeSource::new(
                forged,
                source.fragment_instance_id(),
                source.sender_ordinal(),
            ),
            descriptor.identity(),
            descriptor.fragment_instance_id(),
            node.node_id(),
        )
        .unwrap();
        assert!(matches!(
            capabilities.claim_stream(forged_stream),
            Err(InboundFrameClaim::Refused(_))
        ));
        let claimed = capabilities
            .with_claimed_stream(stream, |admission| Ok::<_, ()>(admission.sender_count()));
        assert_eq!(claimed, Ok(Ok(1)));
    }

    #[test]
    fn strict_normal_retirement_requires_exact_receiver_consumption_facts() {
        use novarocks_execution_contract::{
            ExchangeCloseDisposition, ExchangeDeliveryRequirement, ExchangeFrameSequence,
            ExchangeStreamClose, ExchangeStreamIdentity,
        };
        let (base, _) = descriptor(601, UniqueId::new(61, 62));
        let node = &base.topology().inbound()[0];
        let strict = TaskDescriptor::try_new(
            base.identity(),
            base.fragment_instance_id(),
            NonZeroUsize::new(1).unwrap(),
            Vec::new(),
            ExchangeTopology::try_new(
                Vec::new(),
                vec![
                    ExchangeInbound::try_new(
                        node.node_id(),
                        node.sources().to_vec(),
                        ExchangeDeliveryRequirement::MustDrain,
                    )
                    .unwrap(),
                ],
            )
            .unwrap(),
        )
        .unwrap();
        let capabilities = TaskInboundCapabilities::new();
        capabilities.install(Arc::new(strict.clone())).unwrap();
        let retired = std::sync::atomic::AtomicBool::new(false);
        let stream = ExchangeStreamIdentity::try_new(
            node.sources()[0],
            strict.identity(),
            strict.fragment_instance_id(),
            node.node_id(),
        )
        .unwrap();
        assert!(
            capabilities
                .close_normally_with_facts(
                    &strict,
                    vec![ExchangeStreamClose {
                        stream,
                        disposition: ExchangeCloseDisposition::Withdrawn
                    }],
                    || retired.store(true, std::sync::atomic::Ordering::SeqCst)
                )
                .is_err()
        );
        assert!(!retired.load(std::sync::atomic::Ordering::SeqCst));
        let close = ExchangeStreamClose {
            stream,
            disposition: ExchangeCloseDisposition::Consumed {
                final_sequence: ExchangeFrameSequence::try_new(7).unwrap(),
            },
        };
        capabilities
            .close_normally_with_facts(&strict, vec![close], || {
                retired.store(true, std::sync::atomic::Ordering::SeqCst)
            })
            .unwrap();
        assert!(retired.load(std::sync::atomic::Ordering::SeqCst));
        let Err(InboundFrameClaim::NormallyClosed(proof)) = capabilities.claim_stream(stream)
        else {
            panic!("expected frozen per-source close")
        };
        assert_eq!(proof.close(), close);
        assert_eq!(proof.sender_count(), 1);
    }
    #[test]
    fn checked_close_rejects_missing_duplicate_foreign_and_strict_withdrawn_facts() {
        use novarocks_execution_contract::{
            ExchangeCloseDisposition, ExchangeDeliveryRequirement, ExchangeFrameSequence,
            ExchangeStreamClose, ExchangeStreamIdentity,
        };
        let (base, _) = descriptor(701, UniqueId::new(71, 72));
        let source = base.topology().inbound()[0].sources()[0];
        let topology = ExchangeTopology::try_new(
            Vec::new(),
            vec![
                ExchangeInbound::try_new(
                    FragmentNodeId::new(7),
                    vec![source],
                    ExchangeDeliveryRequirement::MustDrain,
                )
                .unwrap(),
                ExchangeInbound::try_new(
                    FragmentNodeId::new(8),
                    vec![source],
                    ExchangeDeliveryRequirement::DemandDriven,
                )
                .unwrap(),
            ],
        )
        .unwrap();
        let descriptor = TaskDescriptor::try_new(
            base.identity(),
            base.fragment_instance_id(),
            NonZeroUsize::new(1).unwrap(),
            Vec::new(),
            topology,
        )
        .unwrap();
        let capabilities = TaskInboundCapabilities::new();
        capabilities.install(Arc::new(descriptor.clone())).unwrap();
        let stream = |node| {
            ExchangeStreamIdentity::try_new(
                source,
                descriptor.identity(),
                descriptor.fragment_instance_id(),
                FragmentNodeId::new(node),
            )
            .unwrap()
        };
        let first = ExchangeStreamClose {
            stream: stream(7),
            disposition: ExchangeCloseDisposition::Consumed {
                final_sequence: ExchangeFrameSequence::try_new(17).unwrap(),
            },
        };
        let second = ExchangeStreamClose {
            stream: stream(8),
            disposition: ExchangeCloseDisposition::Withdrawn,
        };
        let invalids = [
            vec![],
            vec![first],
            vec![first, first],
            vec![
                first,
                ExchangeStreamClose {
                    stream: stream(99),
                    ..second
                },
            ],
            vec![
                ExchangeStreamClose {
                    disposition: ExchangeCloseDisposition::Withdrawn,
                    ..first
                },
                second,
            ],
        ];
        for closes in invalids {
            let retired = std::sync::atomic::AtomicBool::new(false);
            assert!(
                capabilities
                    .close_normally_with_facts(&descriptor, closes, || {
                        retired.store(true, std::sync::atomic::Ordering::SeqCst);
                    })
                    .is_err()
            );
            assert!(!retired.load(std::sync::atomic::Ordering::SeqCst));
            assert!(capabilities.claim_stream(first.stream).is_ok());
        }
        capabilities
            .close_normally_with_facts(&descriptor, vec![second, first], || {})
            .unwrap();
        for expected in [first, second] {
            let Err(InboundFrameClaim::NormallyClosed(proof)) =
                capabilities.claim_stream(expected.stream)
            else {
                panic!("expected exact per-input close fact");
            };
            assert_eq!(proof.close(), expected);
        }
    }

    #[test]
    fn legacy_void_retirement_records_withdrawal_and_never_forges_consumption() {
        use novarocks_execution_contract::{ExchangeCloseDisposition, ExchangeStreamIdentity};
        let (descriptor, _) = descriptor(801, UniqueId::new(81, 82));
        let input = &descriptor.topology().inbound()[0];
        let stream = ExchangeStreamIdentity::try_new(
            input.sources()[0],
            descriptor.identity(),
            descriptor.fragment_instance_id(),
            input.node_id(),
        )
        .unwrap();
        let capabilities = TaskInboundCapabilities::new();
        capabilities.install(Arc::new(descriptor.clone())).unwrap();
        capabilities.close_normally(&descriptor, || {});
        let Err(InboundFrameClaim::NormallyClosed(proof)) = capabilities.claim_stream(stream)
        else {
            panic!("expected original DemandDriven normal close");
        };
        assert_eq!(
            proof.close().disposition,
            ExchangeCloseDisposition::Withdrawn
        );
        let forged = TaskIdentity::new(
            stream.source().task().query_execution_id(),
            stream.source().task().stage_id(),
            stream.source().task().task_id(),
            BackendProcessId::new_v7(),
        );
        let forged = ExchangeStreamIdentity::try_new(
            ExchangeSource::new(
                forged,
                stream.source().fragment_instance_id(),
                stream.source().sender_ordinal(),
            ),
            descriptor.identity(),
            descriptor.fragment_instance_id(),
            input.node_id(),
        )
        .unwrap();
        assert!(matches!(
            capabilities.claim_stream(forged),
            Err(InboundFrameClaim::Refused(_))
        ));
        let mut delivered = false;
        assert!(
            capabilities
                .with_claimed_stream(stream, |_| {
                    delivered = true;
                    Ok::<_, ()>(())
                })
                .is_err()
        );
        assert!(!delivered);
    }

    #[test]
    fn abnormal_retirement_moves_the_original_capture_and_destroys_it_after_both_fences() {
        use std::sync::{
            RwLock,
            atomic::{AtomicUsize, Ordering},
        };
        struct Capture {
            owner: Arc<TaskInboundCapabilities>,
            gate: Arc<RwLock<()>>,
            drops: Arc<AtomicUsize>,
        }
        impl Drop for Capture {
            fn drop(&mut self) {
                assert!(self.gate.try_write().is_ok(), "delivery writer exited");
                assert!(
                    self.owner.state.try_lock().is_ok(),
                    "capability lock exited"
                );
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }
        let (descriptor, _) = descriptor(911, UniqueId::new(93, 94));
        let owner = TaskInboundCapabilities::new();
        owner.install(Arc::new(descriptor.clone())).unwrap();
        let gate =
            owner.state.lock().unwrap().delivery_gates[&descriptor.fragment_instance_id()].clone();
        let drops = Arc::new(AtomicUsize::new(0));
        let original = Box::new(Capture {
            owner: owner.clone(),
            gate: gate.clone(),
            drops: drops.clone(),
        });
        let identity = std::ptr::from_ref(original.as_ref());
        let detached = owner.remove_with_capture(&descriptor, || {
            assert!(
                gate.try_write().is_err(),
                "same writer fence orders the move"
            );
            assert_eq!(owner.len(), 0, "no process lock encloses the move");
            original
        });
        assert_eq!(std::ptr::from_ref(detached.as_ref()), identity);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(owner.normal_close_retention(), (0, 0));
        drop(detached);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn exact_stream_delivery_releases_process_lock_and_retirement_waits_for_callback() {
        use novarocks_execution_contract::ExchangeStreamIdentity;
        let (descriptor, _) = descriptor(901, UniqueId::new(91, 92));
        let node = &descriptor.topology().inbound()[0];
        let stream = ExchangeStreamIdentity::try_new(
            node.sources()[0],
            descriptor.identity(),
            descriptor.fragment_instance_id(),
            node.node_id(),
        )
        .unwrap();
        let capabilities = TaskInboundCapabilities::new();
        capabilities.install(Arc::new(descriptor.clone())).unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let delivery_owner = Arc::clone(&capabilities);
        let delivery = std::thread::spawn(move || {
            delivery_owner.with_claimed_stream(stream, |_| {
                assert_eq!(
                    delivery_owner.len(),
                    1,
                    "callback does not hold process lock"
                );
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok::<_, ()>(())
            })
        });
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let (closed_tx, closed_rx) = mpsc::channel();
        let close_owner = Arc::clone(&capabilities);
        let close = std::thread::spawn(move || {
            close_owner.close_normally(&descriptor, || closed_tx.send(()).unwrap())
        });
        assert!(closed_rx.recv_timeout(Duration::from_millis(20)).is_err());
        release_tx.send(()).unwrap();
        assert_eq!(delivery.join().unwrap(), Ok(Ok(())));
        close.join().unwrap();
        closed_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(matches!(
            capabilities.claim_stream(stream),
            Err(InboundFrameClaim::NormallyClosed(_))
        ));
    }
}
