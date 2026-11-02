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

//! Structural slots for receiver-authorized exchange units.
//!
//! The directory lock only resolves instance handles. Transitions then lock
//! one instance, followed by the process pool; no transition holds two instance
//! locks. Floating returns are drained after the donor unlocks. Listener calls
//! run after every state lock has been released and carry full cumulative facts,
//! including the last close observation when an instance leaves the directory.
//!
//! This module owns no decoded queue. The synchronous `*_with` callbacks let
//! its composing queue owner commit, transfer or clear queue contents in the
//! same instance critical section. Every queue path takes instance then queue;
//! queue callbacks return before the process pool is locked. No queue path may
//! take the queue lock and then enter this ledger. Callbacks must not reenter it,
//! perform I/O or wait for external capacity. Decode work runs outside that
//! section and keeps its non-cloneable lease until actual exit.

// Design: ADR-0209 (docs/adr/ADR-0209-receiver-authorized-exchange-units-and-shared-wait-graph.md)

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use novarocks_execution_contract::ordered_retained_stream::exchange::{
    ExchangeAdmissionStart, ExchangeAdmissionVerdict, ExchangeDecodeCommit, ExchangeDecodeToken,
    ExchangeReceiptError, ExchangeStreamFacts, ExchangeStreamReceipt, ExchangeUnitCloseDisposition,
    ExchangeUnitDemand, ExchangeUnitItem, ExchangeUnitSequence,
};
use novarocks_execution_contract::{ExchangeStreamIdentity, FragmentNodeId, TaskIdentity};
use novarocks_types::{BackendProcessId, QueryExecutionId, UniqueId};

use super::execution_runtime::ExchangeFlowLimits;
use super::fragment::io::exchange_receiver::ExchangeReceiverKey;
use super::fragment::io::exchange_unit::{
    ExchangeStreamObservation, ExchangeUnitReceipt, ExchangeUnitStateListener,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExchangeSlotError {
    InvalidRegistration(&'static str),
    DuplicateInstance(ExchangeReceiverKey),
    UnknownStream(ExchangeStreamIdentity),
    InvalidCloseSet,
    ListenerAlreadyInstalled,
    Receipt(ExchangeReceiptError),
}

impl From<ExchangeReceiptError> for ExchangeSlotError {
    fn from(error: ExchangeReceiptError) -> Self {
        Self::Receipt(error)
    }
}

impl fmt::Display for ExchangeSlotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRegistration(reason) => {
                write!(f, "invalid exchange slot registration: {reason}")
            }
            Self::DuplicateInstance(key) => {
                write!(f, "exchange slot instance is already registered: {key:?}")
            }
            Self::UnknownStream(stream) => write!(
                f,
                "exchange stream was not frozen for this instance: {stream:?}"
            ),
            Self::InvalidCloseSet => {
                f.write_str("exchange close facts must cover every frozen stream exactly once")
            }
            Self::ListenerAlreadyInstalled => {
                f.write_str("exchange slot state listener is already installed")
            }
            Self::Receipt(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for ExchangeSlotError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Receipt(source) => Some(source),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for ExchangeSlotError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact};
        match self {
            Self::Receipt(source) => source.failure_fact(),
            _ => FailureFact::new(
                FailureClass::Invariant,
                super::fragment::ExecutionReason::ExchangeStreamContract.reason(),
            ),
        }
    }
}

/// All identities come from the frozen Task and route; counts cannot mint them.
#[derive(Clone, Debug)]
pub struct ExchangeSlotRegistration {
    pub task: TaskIdentity,
    pub fragment_instance_id: UniqueId,
    pub node: FragmentNodeId,
    pub streams: Vec<ExchangeStreamIdentity>,
    pub grantable_unit_bytes: u64,
}

impl ExchangeSlotRegistration {
    pub fn key(&self) -> ExchangeReceiverKey {
        ExchangeReceiverKey {
            fragment_instance_id: self.fragment_instance_id,
            node_id: self.node.get(),
        }
    }

    fn validate(&self) -> Result<(), ExchangeSlotError> {
        if self.fragment_instance_id == UniqueId::new(0, 0) || self.node.get() < 0 {
            return Err(ExchangeSlotError::InvalidRegistration(
                "the destination kernel and node must be exact",
            ));
        }
        if self.streams.is_empty() || self.grantable_unit_bytes == 0 {
            return Err(ExchangeSlotError::InvalidRegistration(
                "a frozen stream roster and positive unit limit are required",
            ));
        }
        let mut seen = HashSet::new();
        for stream in &self.streams {
            if stream.destination() != self.task
                || stream.destination_fragment_instance_id() != self.fragment_instance_id
                || stream.destination_node() != self.node
                || !seen.insert(*stream)
            {
                return Err(ExchangeSlotError::InvalidRegistration(
                    "the stream roster must uniquely name this exact destination",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct InstanceIdentity {
    task: TaskIdentity,
    key: ExchangeReceiverKey,
}

struct SlotInstance {
    identity: InstanceIdentity,
    grantable_unit_bytes: u64,
    state: Mutex<InstanceLedger>,
    observation_owner: Weak<RegistryCore>,
}

impl Drop for SlotInstance {
    fn drop(&mut self) {
        if let Some(core) = self.observation_owner.upgrade() {
            if let Ok(mut observed) = core.observed_instances.try_lock() {
                observed.remove(&(self as *const Self as usize));
            } else {
                // A sampler may own the last temporary pin. Its destructor
                // must not wait on another sampler or a registration.
                core.observation_cleanup_needed
                    .store(true, Ordering::Release);
            }
        }
    }
}

struct InstanceLedger {
    streams: BTreeMap<ExchangeStreamIdentity, ExchangeStreamReceipt>,
    demand: VecDeque<ExchangeStreamIdentity>,
    queued: HashSet<ExchangeStreamIdentity>,
    in_use: usize,
    borrowed: usize,
    closed: bool,
}

impl InstanceLedger {
    fn receipt_mut(
        &mut self,
        stream: ExchangeStreamIdentity,
    ) -> Result<&mut ExchangeStreamReceipt, ExchangeSlotError> {
        self.streams
            .get_mut(&stream)
            .ok_or(ExchangeSlotError::UnknownStream(stream))
    }

    fn observation(
        &self,
        stream: ExchangeStreamIdentity,
    ) -> Result<ExchangeStreamObservation, ExchangeSlotError> {
        Ok(ExchangeStreamObservation {
            stream,
            facts: self
                .streams
                .get(&stream)
                .ok_or(ExchangeSlotError::UnknownStream(stream))?
                .facts(),
        })
    }

    fn enqueue_demand(&mut self, stream: ExchangeStreamIdentity) {
        if self.streams[&stream].pending_demand().is_some() && self.queued.insert(stream) {
            self.demand.push_back(stream);
        }
    }

    fn grant_one(&mut self) -> Option<ExchangeStreamObservation> {
        while let Some(stream) = self.demand.pop_front() {
            self.queued.remove(&stream);
            let granted = self
                .streams
                .get_mut(&stream)
                .expect("the demand roster is frozen")
                .grant_next()
                .expect("instance capacity bounds every stream receipt");
            if granted.is_some() {
                self.in_use += 1;
                return Some(
                    self.observation(stream)
                        .expect("the granted stream is frozen"),
                );
            }
        }
        None
    }
}

#[derive(Default)]
struct ProcessPool {
    floating_in_use: usize,
    query_order: VecDeque<QueryExecutionId>,
    waiting: HashMap<QueryExecutionId, VecDeque<Weak<SlotInstance>>>,
    queued: HashSet<InstanceIdentity>,
    draining: bool,
}

impl ProcessPool {
    fn enqueue(&mut self, instance: &Arc<SlotInstance>) {
        if !self.queued.insert(instance.identity) {
            return;
        }
        let query = instance.identity.task.query_execution_id();
        if !self.waiting.contains_key(&query) {
            self.query_order.push_back(query);
        }
        self.waiting
            .entry(query)
            .or_default()
            .push_back(Arc::downgrade(instance));
    }

    fn remove(&mut self, identity: InstanceIdentity) {
        if !self.queued.remove(&identity) {
            return;
        }
        let query = identity.task.query_execution_id();
        if let Some(instances) = self.waiting.get_mut(&query) {
            instances.retain(|instance| {
                instance
                    .upgrade()
                    .is_some_and(|instance| instance.identity != identity)
            });
            if instances.is_empty() {
                self.waiting.remove(&query);
                self.query_order.retain(|candidate| *candidate != query);
            }
        }
    }

    fn pop(&mut self) -> Option<Arc<SlotInstance>> {
        while let Some(query) = self.query_order.pop_front() {
            let instances = self
                .waiting
                .get_mut(&query)
                .expect("the query index follows its queue");
            let instance = instances
                .pop_front()
                .expect("an indexed query has a waiting instance");
            if instances.is_empty() {
                self.waiting.remove(&query);
            } else {
                self.query_order.push_back(query);
            }
            if let Some(instance) = instance.upgrade() {
                self.queued.remove(&instance.identity);
                return Some(instance);
            }
        }
        None
    }
}

struct RegistryCore {
    limits: ExchangeFlowLimits,
    directory: Mutex<HashMap<ExchangeReceiverKey, Arc<SlotInstance>>>,
    pool: Mutex<ProcessPool>,
    listener: Mutex<Option<Arc<dyn ExchangeUnitStateListener>>>,
    // Observation only: retired instances remain visible while real leases or
    // owners retain them. These weak references grant no routing or capacity.
    observed_instances: Mutex<HashMap<usize, Weak<SlotInstance>>>,
    observation_cleanup_needed: AtomicBool,
    replayed_items: AtomicU64,
}

impl RegistryCore {
    fn clean_observed_instances(&self, observed: &mut HashMap<usize, Weak<SlotInstance>>) {
        if self
            .observation_cleanup_needed
            .swap(false, Ordering::AcqRel)
        {
            observed.retain(|_, instance| instance.strong_count() != 0);
        }
    }

    fn publish(&self, changes: Vec<ExchangeStreamObservation>) {
        let listener = self
            .listener
            .lock()
            .expect("exchange slot listener lock")
            .clone();
        if let Some(listener) = listener {
            for change in changes {
                listener.changed(change);
            }
        }
    }

    // Called with the instance lock. Pool is always the inner lock.
    fn settle_instance(
        &self,
        instance: &Arc<SlotInstance>,
        state: &mut InstanceLedger,
        changes: &mut Vec<ExchangeStreamObservation>,
    ) {
        let mut pool = self.pool.lock().expect("exchange slot process pool lock");
        let borrowed = state
            .in_use
            .saturating_sub(self.limits.instance_floor_slots());
        assert!(
            borrowed <= state.borrowed,
            "only the fair drainer may borrow floating slots"
        );
        pool.floating_in_use -= state.borrowed - borrowed;
        state.borrowed = borrowed;
        if !state.closed {
            while state.in_use < self.limits.instance_floor_slots() && !state.demand.is_empty() {
                if let Some(change) = state.grant_one() {
                    changes.push(change);
                }
            }
        }
        if !state.closed
            && !state.demand.is_empty()
            && state.in_use < self.limits.instance_max_slots()
        {
            pool.enqueue(instance);
        } else {
            pool.remove(instance.identity);
        }
    }

    // No instance or directory lock is held when entering the fair drainer.
    fn drain_floating(&self, changes: &mut Vec<ExchangeStreamObservation>) {
        {
            let mut pool = self.pool.lock().expect("exchange slot process pool lock");
            if pool.draining {
                return;
            }
            pool.draining = true;
        }
        loop {
            let instance = {
                let mut pool = self.pool.lock().expect("exchange slot process pool lock");
                if pool.floating_in_use == self.limits.floating_slots() {
                    pool.draining = false;
                    break;
                }
                match pool.pop() {
                    Some(instance) => instance,
                    None => {
                        pool.draining = false;
                        break;
                    }
                }
            };
            let mut state = instance.state.lock().expect("exchange slot instance lock");
            let mut pool = self.pool.lock().expect("exchange slot process pool lock");
            if state.closed || state.in_use >= self.limits.instance_max_slots() {
                continue;
            }
            if let Some(change) = state.grant_one() {
                // A concurrent handoff may have made this an unused floor slot.
                if state.in_use > self.limits.instance_floor_slots() {
                    state.borrowed += 1;
                    pool.floating_in_use += 1;
                }
                changes.push(change);
            }
            if !state.demand.is_empty() && state.in_use < self.limits.instance_max_slots() {
                pool.enqueue(&instance);
            }
        }
    }

    fn finish_transition(&self, mut changes: Vec<ExchangeStreamObservation>) {
        self.drain_floating(&mut changes);
        self.publish(changes);
    }
}

#[derive(Clone)]
pub struct ExchangeSlotRegistry {
    core: Arc<RegistryCore>,
}

impl fmt::Debug for ExchangeSlotRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExchangeSlotRegistry")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExchangeSlotPoolSnapshot {
    pub registered_instances: usize,
    pub floating_in_use: usize,
    pub waiting_instances: usize,
}

/// A nonblocking census of the original ledgers, including retired decode work.
/// `in_use` includes grants awaiting a Unit, not just retained payloads.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ExchangeFlowSnapshot {
    pub live_instances: usize,
    pub closed_instances: usize,
    pub in_use: usize,
    pub max_instance_in_use: usize,
    pub decoding: usize,
    pub pending_demands: usize,
    pub floating_in_use: usize,
    pub replayed_items: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExchangeSlotStreamSnapshot {
    pub facts: ExchangeStreamFacts,
    pub observed_end: Option<ExchangeUnitSequence>,
    pub handed_off_through: u64,
    pub occupied_positions: usize,
    pub decoding_positions: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExchangeSlotInstanceSnapshot {
    pub in_use: usize,
    pub borrowed: usize,
    pub decoding: usize,
    pub pending_demands: usize,
    pub closed: bool,
}

impl ExchangeSlotRegistry {
    pub fn new(limits: ExchangeFlowLimits) -> Self {
        Self {
            core: Arc::new(RegistryCore {
                limits,
                directory: Mutex::new(HashMap::new()),
                pool: Mutex::new(ProcessPool::default()),
                listener: Mutex::new(None),
                observed_instances: Mutex::new(HashMap::new()),
                observation_cleanup_needed: AtomicBool::new(false),
                replayed_items: AtomicU64::new(0),
            }),
        }
    }

    pub fn set_listener(
        &self,
        listener: Arc<dyn ExchangeUnitStateListener>,
    ) -> Result<(), ExchangeSlotError> {
        let mut installed = self
            .core
            .listener
            .lock()
            .expect("exchange slot listener lock");
        if installed.is_some() {
            return Err(ExchangeSlotError::ListenerAlreadyInstalled);
        }
        *installed = Some(listener);
        Ok(())
    }

    /// Validate the complete batch before publishing any instance. Every
    /// installed instance has its own progress floor, independent of other
    /// instances and the shared floating pool.
    pub fn register_batch(
        &self,
        registrations: Vec<ExchangeSlotRegistration>,
    ) -> Result<Vec<ExchangeSlotInstance>, ExchangeSlotError> {
        let mut directory = self
            .core
            .directory
            .lock()
            .expect("exchange slot directory lock");
        let mut seen = HashSet::new();
        for registration in &registrations {
            registration.validate()?;
            let key = registration.key();
            if directory.contains_key(&key) || !seen.insert(key) {
                return Err(ExchangeSlotError::DuplicateInstance(key));
            }
        }
        let mut handles = Vec::with_capacity(registrations.len());
        for registration in registrations {
            let identity = InstanceIdentity {
                task: registration.task,
                key: registration.key(),
            };
            let streams = registration
                .streams
                .into_iter()
                .map(|stream| {
                    (
                        stream,
                        ExchangeStreamReceipt::try_new(self.core.limits.instance_max_slots())
                            .expect("validated instance limit"),
                    )
                })
                .collect();
            let instance = Arc::new(SlotInstance {
                identity,
                grantable_unit_bytes: registration.grantable_unit_bytes,
                observation_owner: Arc::downgrade(&self.core),
                state: Mutex::new(InstanceLedger {
                    streams,
                    demand: VecDeque::new(),
                    queued: HashSet::new(),
                    in_use: 0,
                    borrowed: 0,
                    closed: false,
                }),
            });
            directory.insert(identity.key, Arc::clone(&instance));
            let mut observed = self
                .core
                .observed_instances
                .lock()
                .expect("exchange observation lock");
            self.core.clean_observed_instances(&mut observed);
            observed.insert(Arc::as_ptr(&instance) as usize, Arc::downgrade(&instance));
            drop(observed);
            handles.push(ExchangeSlotInstance {
                core: Arc::clone(&self.core),
                instance,
            });
        }
        Ok(handles)
    }

    pub fn register(
        &self,
        registration: ExchangeSlotRegistration,
    ) -> Result<ExchangeSlotInstance, ExchangeSlotError> {
        Ok(self
            .register_batch(vec![registration])?
            .pop()
            .expect("one registration returns one handle"))
    }

    pub fn get(&self, key: ExchangeReceiverKey) -> Option<ExchangeSlotInstance> {
        self.core
            .directory
            .lock()
            .expect("exchange slot directory lock")
            .get(&key)
            .map(|instance| ExchangeSlotInstance {
                core: Arc::clone(&self.core),
                instance: Arc::clone(instance),
            })
    }

    /// Enumerates only directory-resident instances. A retired stream may be
    /// absent; its sender must use the subscription Resynchronize recovery.
    pub fn sender_states(
        &self,
        sender: BackendProcessId,
        after: Option<ExchangeStreamIdentity>,
        limit: usize,
    ) -> Vec<ExchangeStreamObservation> {
        if limit == 0 {
            return Vec::new();
        }
        let instances: Vec<_> = self
            .core
            .directory
            .lock()
            .expect("exchange slot directory lock")
            .values()
            .cloned()
            .collect();
        let mut page = BTreeMap::new();
        for instance in instances {
            let state = instance.state.lock().expect("exchange slot instance lock");
            if state.closed {
                continue;
            }
            for (stream, receipt) in &state.streams {
                if stream.source().task().backend_process_id() != sender
                    || after.is_some_and(|after| *stream <= after)
                {
                    continue;
                }
                page.insert(
                    *stream,
                    ExchangeStreamObservation {
                        stream: *stream,
                        facts: receipt.facts(),
                    },
                );
                if page.len() > limit {
                    page.pop_last();
                }
            }
        }
        page.into_values().collect()
    }

    pub fn snapshot(&self) -> ExchangeSlotPoolSnapshot {
        let registered_instances = self
            .core
            .directory
            .lock()
            .expect("exchange slot directory lock")
            .len();
        let pool = self
            .core
            .pool
            .lock()
            .expect("exchange slot process pool lock");
        ExchangeSlotPoolSnapshot {
            registered_instances,
            floating_in_use: pool.floating_in_use,
            waiting_instances: pool.queued.len(),
        }
    }

    /// Busy ledgers are unavailable, never zero. No payloads are cloned, and
    /// sampling never waits on an executing transition. Instance -> pool is
    /// the same lock order as admission and actual lease release.
    pub fn try_flow_snapshot(&self) -> Option<ExchangeFlowSnapshot> {
        // Declare the pins before the index guard: every return, including a
        // busy ledger, unlocks the index before dropping its last instance pin.
        let instances: Vec<Arc<SlotInstance>>;
        let mut observed = self.core.observed_instances.try_lock().ok()?;
        self.core.clean_observed_instances(&mut observed);
        instances = observed.values().filter_map(Weak::upgrade).collect();
        let ledgers = instances
            .iter()
            .map(|instance| instance.state.try_lock())
            .collect::<Result<Vec<_>, _>>()
            .ok()?;
        let pool = self.core.pool.try_lock().ok()?;
        let mut snapshot = ExchangeFlowSnapshot {
            live_instances: instances.len(),
            floating_in_use: pool.floating_in_use,
            replayed_items: self.core.replayed_items.load(Ordering::Relaxed),
            ..Default::default()
        };
        for state in &ledgers {
            snapshot.closed_instances += usize::from(state.closed);
            snapshot.in_use = snapshot.in_use.checked_add(state.in_use)?;
            snapshot.max_instance_in_use = snapshot.max_instance_in_use.max(state.in_use);
            snapshot.pending_demands = snapshot.pending_demands.checked_add(state.queued.len())?;
            for stream in state.streams.values() {
                snapshot.decoding = snapshot.decoding.checked_add(stream.decoding_positions())?;
            }
        }
        Some(snapshot)
    }
}

#[derive(Clone)]
pub struct ExchangeSlotInstance {
    core: Arc<RegistryCore>,
    instance: Arc<SlotInstance>,
}

impl fmt::Debug for ExchangeSlotInstance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExchangeSlotInstance")
            .field("identity", &self.instance.identity)
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

impl ExchangeSlotInstance {
    pub fn key(&self) -> ExchangeReceiverKey {
        self.instance.identity.key
    }
    pub fn task(&self) -> TaskIdentity {
        self.instance.identity.task
    }

    pub fn snapshot(&self) -> ExchangeSlotInstanceSnapshot {
        let state = self
            .instance
            .state
            .lock()
            .expect("exchange slot instance lock");
        ExchangeSlotInstanceSnapshot {
            in_use: state.in_use,
            borrowed: state.borrowed,
            decoding: state
                .streams
                .values()
                .map(ExchangeStreamReceipt::decoding_positions)
                .sum(),
            pending_demands: state.queued.len(),
            closed: state.closed,
        }
    }

    pub fn facts(
        &self,
        stream: ExchangeStreamIdentity,
    ) -> Result<ExchangeStreamFacts, ExchangeSlotError> {
        Ok(self
            .instance
            .state
            .lock()
            .expect("exchange slot instance lock")
            .observation(stream)?
            .facts)
    }

    pub fn stream_snapshot(
        &self,
        stream: ExchangeStreamIdentity,
    ) -> Result<ExchangeSlotStreamSnapshot, ExchangeSlotError> {
        let state = self
            .instance
            .state
            .lock()
            .expect("exchange slot instance lock");
        let receipt = state
            .streams
            .get(&stream)
            .ok_or(ExchangeSlotError::UnknownStream(stream))?;
        Ok(ExchangeSlotStreamSnapshot {
            facts: receipt.facts(),
            observed_end: receipt.observed_end(),
            handed_off_through: receipt.handed_off_through(),
            occupied_positions: receipt.occupied_positions(),
            decoding_positions: receipt.decoding_positions(),
        })
    }

    pub fn register_demand(
        &self,
        stream: ExchangeStreamIdentity,
        demand: ExchangeUnitDemand,
    ) -> Result<ExchangeStreamFacts, ExchangeSlotError> {
        let mut changes = Vec::new();
        {
            let mut state = self
                .instance
                .state
                .lock()
                .expect("exchange slot instance lock");
            state
                .receipt_mut(stream)?
                .register_demand(demand, self.instance.grantable_unit_bytes)?;
            state.enqueue_demand(stream);
            self.core
                .settle_instance(&self.instance, &mut state, &mut changes);
        }
        self.core.finish_transition(changes);
        self.facts(stream)
    }

    pub fn begin_admission(
        &self,
        stream: ExchangeStreamIdentity,
        item: ExchangeUnitItem,
    ) -> Result<ExchangeSlotAdmission, ExchangeSlotError> {
        let (start, observation, changed) = {
            let mut state = self
                .instance
                .state
                .lock()
                .expect("exchange slot instance lock");
            let before = state.observation(stream)?.facts;
            let start = state.receipt_mut(stream)?.begin_admission(item)?;
            let observation = state.observation(stream)?;
            (start, observation, observation.facts != before)
        };
        if changed {
            self.core.publish(vec![observation]);
        }
        match start {
            ExchangeAdmissionStart::Decode(token) => {
                Ok(ExchangeSlotAdmission::Decode(ExchangeSlotDecodeLease {
                    core: Arc::clone(&self.core),
                    instance: Arc::clone(&self.instance),
                    stream,
                    token: Some(token),
                }))
            }
            ExchangeAdmissionStart::Observed(verdict) => {
                if verdict == ExchangeAdmissionVerdict::Idempotent {
                    self.core.replayed_items.fetch_add(1, Ordering::Relaxed);
                }
                Ok(ExchangeSlotAdmission::Observed(ExchangeUnitReceipt {
                    verdict,
                    facts: observation.facts,
                }))
            }
            ExchangeAdmissionStart::Closed => {
                self.core.replayed_items.fetch_add(1, Ordering::Relaxed);
                Ok(ExchangeSlotAdmission::Observed(ExchangeUnitReceipt {
                    verdict: ExchangeAdmissionVerdict::Idempotent,
                    facts: observation.facts,
                }))
            }
        }
    }

    /// A consumer transaction keeps the instance locked across queue inspection,
    /// receipt validation and the actual transfer. `inspect` reports a marker
    /// only when this transfer moves the last batch of a unit. Other batches or
    /// an empty queue return None. The queue must use this transaction for every
    /// consumer, including concurrent drivers; neither callback may reenter it.
    /// Invalid handoff metadata is refused before `transfer` can pop anything.
    pub fn consume_with<R>(
        &self,
        inspect: impl FnOnce() -> Option<(ExchangeStreamIdentity, ExchangeUnitSequence)>,
        transfer: impl FnOnce() -> R,
    ) -> Result<R, ExchangeSlotError> {
        let mut changes = Vec::new();
        let result = {
            let mut state = self
                .instance
                .state
                .lock()
                .expect("exchange slot instance lock");
            if state.closed {
                return Err(ExchangeReceiptError::HandoffGap.into());
            }
            let marker = inspect();
            if let Some((stream, sequence)) = marker {
                state.receipt_mut(stream)?.hand_off(sequence)?;
            }
            let result = transfer();
            if marker.is_some() {
                state.in_use -= 1;
            }
            self.core
                .settle_instance(&self.instance, &mut state, &mut changes);
            result
        };
        self.core.finish_transition(changes);
        Ok(result)
    }

    /// For queue owners whose transfer already knows its exact final-unit
    /// marker. Multi-driver queue polling should use `consume_with` instead.
    pub fn hand_off_with<R>(
        &self,
        stream: ExchangeStreamIdentity,
        sequence: ExchangeUnitSequence,
        transfer: impl FnOnce() -> R,
    ) -> Result<R, ExchangeSlotError> {
        let mut changes = Vec::new();
        let result = {
            let mut state = self
                .instance
                .state
                .lock()
                .expect("exchange slot instance lock");
            state.receipt_mut(stream)?.hand_off(sequence)?;
            let result = transfer();
            state.in_use -= 1;
            self.core
                .settle_instance(&self.instance, &mut state, &mut changes);
            result
        };
        self.core.finish_transition(changes);
        Ok(result)
    }

    pub fn complete_input_consumption(
        &self,
        stream: ExchangeStreamIdentity,
    ) -> Result<(), ExchangeSlotError> {
        self.instance
            .state
            .lock()
            .expect("exchange slot instance lock")
            .receipt_mut(stream)?
            .complete_input_consumption()?;
        Ok(())
    }

    /// The queue owner checks actual emptiness inside this instance transaction.
    /// All streams must already have handed off every data unit and observed a
    /// complete End prefix. A temporarily empty queue is therefore only false,
    /// and cannot advance some streams while another still awaits input.
    pub fn complete_all_input_consumption_with(
        &self,
        queue_is_empty: impl FnOnce() -> bool,
    ) -> Result<bool, ExchangeSlotError> {
        let mut state = self
            .instance
            .state
            .lock()
            .expect("exchange slot instance lock");
        if state.closed || !queue_is_empty() {
            return Ok(false);
        }
        let ready = state.streams.values().all(|receipt| {
            receipt.observed_end().is_some_and(|end| {
                receipt.facts().custody_through == end.get()
                    && (receipt.handed_off_through() == end.get()
                        || receipt.handed_off_through().checked_add(1) == Some(end.get()))
                    && receipt.occupied_positions() == 0
            })
        });
        if !ready {
            return Ok(false);
        }
        for receipt in state.streams.values_mut() {
            receipt.complete_input_consumption()?;
        }
        Ok(true)
    }

    pub fn stop_sender(
        &self,
        stream: ExchangeStreamIdentity,
    ) -> Result<ExchangeStreamFacts, ExchangeSlotError> {
        let mut changes = Vec::new();
        {
            let mut state = self
                .instance
                .state
                .lock()
                .expect("exchange slot instance lock");
            let before = state.observation(stream)?.facts;
            let released = state.receipt_mut(stream)?.stop_sender().len();
            state.in_use -= released;
            state.demand.retain(|candidate| *candidate != stream);
            state.queued.remove(&stream);
            let observation = state.observation(stream)?;
            if observation.facts != before {
                changes.push(observation);
            }
            self.core
                .settle_instance(&self.instance, &mut state, &mut changes);
        }
        self.core.finish_transition(changes);
        self.facts(stream)
    }

    /// `close` is authored by the consuming input. This operation does not infer
    /// Consumed from queue deletion or Task status. It removes the directory
    /// entry only after closing every stream, while leases pin real decode work.
    pub fn retire_with(
        &self,
        closes: &[(ExchangeStreamIdentity, ExchangeUnitCloseDisposition)],
        clear_queue: impl FnOnce(),
    ) -> Result<(), ExchangeSlotError> {
        let mut changes = Vec::new();
        {
            let mut state = self
                .instance
                .state
                .lock()
                .expect("exchange slot instance lock");
            let close_map: BTreeMap<_, _> = closes.iter().copied().collect();
            if closes.len() != state.streams.len()
                || close_map.len() != closes.len()
                || close_map.keys().ne(state.streams.keys())
            {
                return Err(ExchangeSlotError::InvalidCloseSet);
            }
            // Prevalidate every close so one invalid stream cannot commit a prefix.
            for (stream, disposition) in &close_map {
                let receipt = &state.streams[stream];
                if let Some(previous) = receipt.facts().receiver_closed {
                    if previous != *disposition {
                        return Err(ExchangeReceiptError::CloseConflict.into());
                    }
                } else if let ExchangeUnitCloseDisposition::Consumed { final_sequence } =
                    disposition
                    && (receipt.observed_end() != Some(*final_sequence)
                        || receipt.handed_off_through() != final_sequence.get())
                {
                    return Err(ExchangeReceiptError::InputNotConsumed.into());
                }
            }
            // The first retire owns final-fact handoff and directory removal.
            // A concurrent replay must not remove the entry while that handoff
            // is still running outside this lock.
            if state.closed {
                return Ok(());
            }
            for (stream, disposition) in close_map {
                let before = state.observation(stream)?.facts;
                let released = state
                    .receipt_mut(stream)?
                    .close_receiver(disposition)?
                    .len();
                state.in_use -= released;
                let observation = state.observation(stream)?;
                if observation.facts != before {
                    changes.push(observation);
                }
            }
            state.closed = true;
            state.demand.clear();
            state.queued.clear();
            clear_queue();
            self.core
                .settle_instance(&self.instance, &mut state, &mut changes);
        }
        // Hand off the final cumulative close facts before removing this
        // authoritative entry. The listener keeps values, never retired keys.
        self.core.finish_transition(changes);
        {
            let mut directory = self
                .core
                .directory
                .lock()
                .expect("exchange slot directory lock");
            if directory
                .get(&self.key())
                .is_some_and(|instance| Arc::ptr_eq(instance, &self.instance))
            {
                directory.remove(&self.key());
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum ExchangeSlotAdmission {
    Decode(ExchangeSlotDecodeLease),
    Observed(ExchangeUnitReceipt),
}

/// The exact instance and stream are private, so a pure token from stream A
/// cannot accidentally complete stream B. The lease also survives directory
/// removal and returns a decoding slot when its actual work owner exits.
pub struct ExchangeSlotDecodeLease {
    core: Arc<RegistryCore>,
    instance: Arc<SlotInstance>,
    stream: ExchangeStreamIdentity,
    token: Option<ExchangeDecodeToken>,
}

impl fmt::Debug for ExchangeSlotDecodeLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExchangeSlotDecodeLease")
            .field("stream", &self.stream)
            .field("sequence", &self.sequence())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExchangeSlotDecodeResult {
    pub disposition: ExchangeDecodeCommit,
    pub receipt: ExchangeUnitReceipt,
}

impl ExchangeSlotDecodeLease {
    pub const fn stream(&self) -> ExchangeStreamIdentity {
        self.stream
    }
    pub fn sequence(&self) -> ExchangeUnitSequence {
        self.token
            .as_ref()
            .expect("a live decode lease owns its token")
            .sequence()
    }

    /// Called after decoding actually returns; `commit_queue` only mutates the
    /// composing queue in this same instance critical section.
    pub fn finish_with<R>(
        mut self,
        commit_queue: impl FnOnce(ExchangeSlotDecodeResult) -> R,
    ) -> Result<R, ExchangeSlotError> {
        let token = self.token.take().expect("a decode lease completes once");
        let mut changes = Vec::new();
        let result = {
            let mut state = self
                .instance
                .state
                .lock()
                .expect("exchange slot instance lock");
            let before = state.observation(self.stream)?.facts;
            let disposition = state.receipt_mut(self.stream)?.finish_decode(token)?;
            let observation = state.observation(self.stream)?;
            let result = commit_queue(ExchangeSlotDecodeResult {
                disposition,
                receipt: ExchangeUnitReceipt {
                    verdict: ExchangeAdmissionVerdict::Applied,
                    facts: observation.facts,
                },
            });
            if disposition == ExchangeDecodeCommit::Discarded {
                state.in_use -= 1;
            }
            if observation.facts != before {
                changes.push(observation);
            }
            self.core
                .settle_instance(&self.instance, &mut state, &mut changes);
            result
        };
        self.core.finish_transition(changes);
        Ok(result)
    }

    /// Failed decoding reports its originating failure through its composing
    /// work owner; abandoning this lease only returns actual resource ownership.
    pub fn abandon(self) {
        drop(self);
    }
}

impl Drop for ExchangeSlotDecodeLease {
    fn drop(&mut self) {
        let Some(token) = self.token.take() else {
            return;
        };
        let mut changes = Vec::new();
        {
            let mut state = self
                .instance
                .state
                .lock()
                .expect("exchange slot instance lock");
            let before = state
                .observation(self.stream)
                .expect("the stream is pinned")
                .facts;
            state
                .receipt_mut(self.stream)
                .expect("a decode lease pins its frozen stream")
                .abandon_decode(token)
                .expect("the lease owns its unique decode claim");
            state.in_use -= 1;
            let observation = state
                .observation(self.stream)
                .expect("the stream is pinned");
            if observation.facts != before {
                changes.push(observation);
            }
            self.core
                .settle_instance(&self.instance, &mut state, &mut changes);
        }
        self.core.finish_transition(changes);
    }
}

#[cfg(test)]
mod tests;
