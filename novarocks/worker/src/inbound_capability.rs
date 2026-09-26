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
use std::mem::size_of;
use std::sync::{Arc, Mutex, RwLock};

use novarocks_execution_contract::{
    ExchangeInbound, ExchangeSource, FragmentNodeId, QueryContextRef, TaskDescriptor, TaskIdentity,
};
use novarocks_types::{QueryExecutionId, UniqueId};

use crate::{HostRejection, IngressRejection, authorize_inbound_frame};

const CAPABILITY_LOCK: &str = "task inbound capability lock";

/// Validated process-local bounds shared by active and retained close records.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TaskInboundCapabilityLimits {
    max_records: usize,
    max_bytes: usize,
}

impl Default for TaskInboundCapabilityLimits {
    fn default() -> Self {
        Self {
            max_records: 32_768,
            max_bytes: 64 * 1024 * 1024,
        }
    }
}

impl TaskInboundCapabilityLimits {
    pub fn try_new(max_records: usize, max_bytes: usize) -> Result<Self, String> {
        if max_records == 0 {
            return Err("normal-close record count limit must be nonzero".to_owned());
        }
        if max_bytes == 0 {
            return Err("normal-close record byte limit must be nonzero".to_owned());
        }
        Ok(Self {
            max_records,
            max_bytes,
        })
    }

    pub const fn max_records(self) -> usize {
        self.max_records
    }

    pub const fn max_bytes(self) -> usize {
        self.max_bytes
    }
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
    normal_closed_executions: HashSet<QueryExecutionId>,
    reserved: HashMap<TaskIdentity, usize>,
    reserved_bytes: usize,
    /// A descriptor carries no frontend process identity. The worker context
    /// owner admits at most one context per execution, so the execution is
    /// the complete key this data-plane owner must fence.
    closed_executions: HashSet<QueryExecutionId>,
    aborted_executions: HashSet<QueryExecutionId>,
}

#[derive(Debug)]
pub struct TaskInboundCapabilities {
    state: Mutex<TaskInboundCapabilityState>,
    max_records: usize,
    max_bytes: usize,
}

/// The bounded, payload-free remainder of a receiver that normally stopped.
#[derive(Debug)]
struct NormalClosedInboundRecord {
    destination: TaskIdentity,
    inbound: Vec<ExchangeInbound>,
}

/// An exact normal-close answer to a frozen sender of one input occurrence.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct NormalClosedInbound {
    destination: TaskIdentity,
    destination_kernel_key: UniqueId,
    destination_node_id: FragmentNodeId,
    source: ExchangeSource,
    sender_count: u32,
}

impl NormalClosedInbound {
    pub const fn destination(self) -> TaskIdentity {
        self.destination
    }
    pub const fn destination_kernel_key(self) -> UniqueId {
        self.destination_kernel_key
    }
    pub const fn destination_node_id(self) -> FragmentNodeId {
        self.destination_node_id
    }
    pub const fn source(self) -> ExchangeSource {
        self.source
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
}

impl InboundFrameAdmission {
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

impl TaskInboundCapabilities {
    pub fn new() -> Arc<Self> {
        Self::with_capacity_limits(TaskInboundCapabilityLimits::default())
    }

    /// The accepted-task owner must reserve against these hard bounds before
    /// acknowledging creation. Normal retirement only converts that charge.
    pub fn with_limits(max_records: usize, max_bytes: usize) -> Arc<Self> {
        Self::with_capacity_limits(
            TaskInboundCapabilityLimits::try_new(max_records, max_bytes)
                .expect("normal-close capacity limits must be nonzero"),
        )
    }

    pub fn with_capacity_limits(limits: TaskInboundCapabilityLimits) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(TaskInboundCapabilityState::default()),
            max_records: limits.max_records(),
            max_bytes: limits.max_bytes(),
        })
    }

    fn record_bytes(descriptor: &TaskDescriptor) -> Option<usize> {
        if descriptor.topology().inbound().is_empty() {
            return Some(0);
        }
        descriptor.topology().inbound().iter().try_fold(
            size_of::<NormalClosedInboundRecord>() + 128,
            |bytes, node| {
                bytes
                    .checked_add(size_of::<ExchangeInbound>() + 64)?
                    .checked_add(
                        node.sources()
                            .len()
                            .checked_mul(size_of::<ExchangeSource>() + 16)?,
                    )
            },
        )
    }

    pub fn reserve_normal_close(&self, descriptor: &TaskDescriptor) -> Result<(), HostRejection> {
        let bytes = Self::record_bytes(descriptor).ok_or_else(|| {
            HostRejection::new(
                novarocks_execution_contract::TaskFailureCategory::ResourceExhausted,
                "normal-close record size overflow",
            )
        })?;
        if bytes == 0 {
            return Ok(());
        }
        let mut state = self.state.lock().expect(CAPABILITY_LOCK);
        if let Some(existing) = state.reserved.get(&descriptor.identity()) {
            return if *existing == bytes {
                Ok(())
            } else {
                Err(HostRejection::new(
                    novarocks_execution_contract::TaskFailureCategory::Protocol,
                    "normal-close reservation conflicts with the accepted task",
                ))
            };
        }
        if state.reserved.len() >= self.max_records
            || bytes > self.max_bytes.saturating_sub(state.reserved_bytes)
        {
            return Err(HostRejection::new(
                novarocks_execution_contract::TaskFailureCategory::ResourceExhausted,
                "backend normal-close record capacity exhausted",
            ));
        }
        state.reserved.insert(descriptor.identity(), bytes);
        state.reserved_bytes += bytes;
        Ok(())
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
                .closed_executions
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
        })
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
        let gate = self
            .state
            .lock()
            .expect(CAPABILITY_LOCK)
            .delivery_gates
            .get(&destination_kernel_key)
            .cloned();
        let Some(gate) = gate else {
            return self.claim_frame(
                destination_kernel_key,
                destination_node_id,
                source_kernel_key,
                sender_ordinal,
                sender_count,
            );
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
            return claim;
        }
        match deliver() {
            Ok(()) => InboundFrameClaim::Authorized,
            Err(detail) => InboundFrameClaim::Refused(detail),
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
            return InboundFrameClaim::NormallyClosed(NormalClosedInbound {
                destination: closed.destination,
                destination_kernel_key,
                destination_node_id,
                source,
                sender_count: expected,
            });
        }
        let Some(descriptor) = state.installed.get(&destination_kernel_key) else {
            return InboundFrameClaim::NotHeld;
        };
        if state
            .closed_executions
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
        self.reserve_normal_close(&descriptor)?;
        let mut state = self.state.lock().expect(CAPABILITY_LOCK);
        if state
            .closed_executions
            .contains(&descriptor.identity().query_execution_id())
        {
            return Err(HostRejection::new(
                novarocks_execution_contract::TaskFailureCategory::Protocol,
                format!(
                    "task {} cannot install inbound capability after its query context closed",
                    descriptor.identity()
                ),
            ));
        }
        if state.normally_closed.contains_key(&key) {
            return Err(HostRejection::new(
                novarocks_execution_contract::TaskFailureCategory::Protocol,
                format!(
                    "task {} cannot reuse a normally closed kernel key {key}",
                    descriptor.identity()
                ),
            ));
        }
        if let Some(existing) = state.installed.get(&key) {
            return Err(HostRejection::new(
                novarocks_execution_contract::TaskFailureCategory::Protocol,
                format!(
                    "task {} cannot claim kernel key {key}, which task {} already holds",
                    descriptor.identity(),
                    existing.identity()
                ),
            ));
        }
        state.installed.insert(key, descriptor);
        if !state.installed[&key].topology().inbound().is_empty() {
            state.delivery_gates.insert(key, Arc::new(RwLock::new(())));
        }
        Ok(())
    }

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
        let keys: Vec<_> = state
            .normally_closed
            .iter()
            .filter(|(_, record)| record.destination.query_execution_id() == execution)
            .map(|(key, _)| *key)
            .collect();
        for key in keys {
            if let Some(record) = state.normally_closed.remove(&key) {
                if let Some(bytes) = state.reserved.remove(&record.destination) {
                    state.reserved_bytes -= bytes;
                }
            }
        }
    }

    pub fn forget_context(&self, context: QueryContextRef) {
        let mut state = self.state.lock().expect(CAPABILITY_LOCK);
        debug_assert!(state.installed.values().all(|descriptor| {
            descriptor.identity().query_execution_id() != context.query_execution_id()
        }));
        state
            .closed_executions
            .remove(&context.query_execution_id());
        state
            .aborted_executions
            .remove(&context.query_execution_id());
        state
            .normal_closed_executions
            .remove(&context.query_execution_id());
        state.normally_closed.retain(|_, record| {
            record.destination.query_execution_id() != context.query_execution_id()
        });
        let live_keys: HashSet<_> = state
            .installed
            .keys()
            .chain(state.normally_closed.keys())
            .copied()
            .collect();
        state
            .delivery_gates
            .retain(|key, _| live_keys.contains(key));
        let reservations: Vec<_> = state
            .reserved
            .keys()
            .copied()
            .filter(|identity| identity.query_execution_id() == context.query_execution_id())
            .collect();
        for identity in reservations {
            if let Some(bytes) = state.reserved.remove(&identity) {
                state.reserved_bytes -= bytes;
            }
        }
    }

    /// Withdraws exactly this task's capability. Identity is re-checked so a
    /// stale rollback cannot evict a newer owner of the same kernel key.
    pub fn remove(&self, descriptor: &TaskDescriptor) {
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
        if let Some(bytes) = state.reserved.remove(&descriptor.identity()) {
            state.reserved_bytes -= bytes;
        }
    }

    /// Atomically replaces one installed receiver capability with its frozen,
    /// bounded normal-close record while the host retires the receiver route.
    pub fn close_normally(&self, descriptor: &TaskDescriptor, retire_receiver: impl FnOnce()) {
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
                assert!(
                    state.reserved.contains_key(&descriptor.identity()),
                    "normal close requires accepted capacity reservation"
                );
                state.normally_closed.insert(
                    key,
                    NormalClosedInboundRecord {
                        destination: descriptor.identity(),
                        inbound: descriptor.topology().inbound().to_vec(),
                    },
                );
                state
                    .normal_closed_executions
                    .insert(descriptor.identity().query_execution_id());
            }
        } else {
            if held {
                state.installed.remove(&key);
            }
            if let Some(bytes) = state.reserved.remove(&descriptor.identity()) {
                state.reserved_bytes -= bytes;
            }
        }
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

    use super::{InboundFrameClaim, TaskInboundCapabilities, TaskInboundCapabilityLimits};

    #[test]
    fn normal_close_capacity_limits_reject_zero_before_composition() {
        assert!(TaskInboundCapabilityLimits::try_new(0, 1).is_err());
        assert!(TaskInboundCapabilityLimits::try_new(1, 0).is_err());
        let limits = TaskInboundCapabilityLimits::try_new(2, 512).unwrap();
        assert_eq!(limits.max_records(), 2);
        assert_eq!(limits.max_bytes(), 512);
        let defaults = TaskInboundCapabilityLimits::default();
        assert_eq!(defaults.max_records(), 32_768);
        assert_eq!(defaults.max_bytes(), 64 * 1024 * 1024);
    }

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
    fn accepted_capacity_is_reserved_before_normal_retirement() {
        let capabilities = TaskInboundCapabilities::with_limits(1, 4096);
        let (first, source) = descriptor(101, UniqueId::new(1, 2));
        let (second, _) = descriptor(201, UniqueId::new(3, 4));
        capabilities.reserve_normal_close(&first).unwrap();
        assert!(capabilities.reserve_normal_close(&second).is_err());
        capabilities.install(Arc::new(first.clone())).unwrap();
        capabilities.close_normally(&first, || {});
        assert!(matches!(
            claim(&capabilities, first.fragment_instance_id(), source, 0, 1),
            InboundFrameClaim::NormallyClosed(_)
        ));
        assert!(
            capabilities.reserve_normal_close(&second).is_err(),
            "normal-close record retains its accepted reservation"
        );
        let context = QueryContextRef::new(
            first.identity().query_execution_id(),
            FrontendProcessId::new_v7(),
            first.identity().backend_process_id(),
        );
        capabilities.forget_context(context);
        capabilities.reserve_normal_close(&second).unwrap();
    }

    #[test]
    fn active_and_retained_normal_close_records_share_the_byte_bound() {
        let (first, first_source) = descriptor(102, UniqueId::new(101, 102));
        let (second, _) = descriptor(202, UniqueId::new(201, 202));
        let (third, _) = descriptor(302, UniqueId::new(301, 302));
        let one_record_bytes = TaskInboundCapabilities::record_bytes(&first).unwrap();
        let capabilities = TaskInboundCapabilities::with_limits(3, one_record_bytes * 2);

        capabilities.install(Arc::new(first.clone())).unwrap();
        capabilities.close_normally(&first, || {});
        capabilities.install(Arc::new(second.clone())).unwrap();
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
        assert_eq!(
            capabilities
                .reserve_normal_close(&third)
                .expect_err("retained and active records must consume the same byte limit")
                .category(),
            novarocks_execution_contract::TaskFailureCategory::ResourceExhausted
        );

        capabilities.remove(&second);
        capabilities.reserve_normal_close(&third).unwrap();
        capabilities.remove(&third);
        let context = QueryContextRef::new(
            first.identity().query_execution_id(),
            FrontendProcessId::new_v7(),
            first.identity().backend_process_id(),
        );
        capabilities.forget_context(context);
        let state = capabilities.state.lock().unwrap();
        assert_eq!(state.reserved_bytes, 0);
        assert!(state.reserved.is_empty());
        assert!(state.normally_closed.is_empty());
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
        capabilities.close_normally(&descriptor, || {});
        let context = QueryContextRef::new(
            descriptor.identity().query_execution_id(),
            FrontendProcessId::new_v7(),
            descriptor.identity().backend_process_id(),
        );
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
}
