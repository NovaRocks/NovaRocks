// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::{
    WorkError, WorkId, WorkScope,
    scope::{Inner, ResultWaiter, State},
};
use std::sync::{Arc, Mutex};

/// Explicit local allocation budget. Server must resolve actual configuration;
/// no default claims to know the process's usable memory.
#[derive(Clone, Debug)]
pub struct ResourceConfig {
    pub total_bytes: u64,
    /// A hard partition within total_bytes, unavailable to data allocations.
    pub control_bytes: u64,
    /// Data admission ceiling for this scope's held bytes. Cleanup can use its
    /// control partition beyond this ceiling, within the same process total.
    pub per_scope_bytes: u64,
}

impl ResourceConfig {
    pub fn validate(&self) -> Result<(), WorkError> {
        if self.total_bytes == 0 || self.total_bytes > isize::MAX as u64 {
            return Err(WorkError::InvalidConfig("total_bytes"));
        }
        if self.control_bytes == 0 || self.control_bytes >= self.total_bytes {
            return Err(WorkError::InvalidConfig("control_bytes"));
        }
        if self.per_scope_bytes == 0 || self.per_scope_bytes > self.total_bytes {
            return Err(WorkError::InvalidConfig("per_scope_bytes"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ResourceClass {
    Data,
    Control,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResourceSnapshot {
    pub total_limit_bytes: u64,
    pub data_reserved_bytes: u64,
    pub data_used_bytes: u64,
    pub control_reserved_bytes: u64,
    pub control_used_bytes: u64,
    pub peak_held_bytes: u64,
    pub result_credit: ResultCreditSnapshot,
}

/// Current result-delivery holdings. These are a breakdown of the ordinary
/// data reserved/used ledger, not an additional capacity authority.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ResultCreditSnapshot {
    pub reserved_before_fetch_bytes: u64,
    pub in_flight_raw_bytes: u64,
    pub raw_retained_bytes: u64,
    /// Raw input plus its concurrently reserved decoded output.
    pub decode_reserved_bytes: u64,
    pub decoded_queued_bytes: u64,
    /// Decoded input plus concurrently reserved protocol output.
    pub protocol_reserved_bytes: u64,
    pub protocol_writing_bytes: u64,
}

impl ResultCreditSnapshot {
    pub fn held_bytes(&self) -> u64 {
        self.reserved_before_fetch_bytes
            + self.in_flight_raw_bytes
            + self.raw_retained_bytes
            + self.decode_reserved_bytes
            + self.decoded_queued_bytes
            + self.protocol_reserved_bytes
            + self.protocol_writing_bytes
    }

    fn bytes(&self, stage: ResultCreditStage) -> u64 {
        match stage {
            ResultCreditStage::ReservedBeforeFetch => self.reserved_before_fetch_bytes,
            ResultCreditStage::InFlightRaw => self.in_flight_raw_bytes,
            ResultCreditStage::RawRetained => self.raw_retained_bytes,
            ResultCreditStage::DecodeReserved => self.decode_reserved_bytes,
            ResultCreditStage::DecodedQueued => self.decoded_queued_bytes,
            ResultCreditStage::ProtocolReserved => self.protocol_reserved_bytes,
            ResultCreditStage::ProtocolWriting => self.protocol_writing_bytes,
            ResultCreditStage::Consumed => 0,
        }
    }

    fn set_bytes(&mut self, stage: ResultCreditStage, bytes: u64) {
        match stage {
            ResultCreditStage::ReservedBeforeFetch => self.reserved_before_fetch_bytes = bytes,
            ResultCreditStage::InFlightRaw => self.in_flight_raw_bytes = bytes,
            ResultCreditStage::RawRetained => self.raw_retained_bytes = bytes,
            ResultCreditStage::DecodeReserved => self.decode_reserved_bytes = bytes,
            ResultCreditStage::DecodedQueued => self.decoded_queued_bytes = bytes,
            ResultCreditStage::ProtocolReserved => self.protocol_reserved_bytes = bytes,
            ResultCreditStage::ProtocolWriting => self.protocol_writing_bytes = bytes,
            ResultCreditStage::Consumed => debug_assert_eq!(bytes, 0),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResultCreditStage {
    ReservedBeforeFetch,
    InFlightRaw,
    RawRetained,
    DecodeReserved,
    DecodedQueued,
    ProtocolReserved,
    ProtocolWriting,
    Consumed,
}

impl ResourceSnapshot {
    pub fn held_bytes(&self) -> u64 {
        self.data_reserved_bytes
            + self.data_used_bytes
            + self.control_reserved_bytes
            + self.control_used_bytes
    }
}

/// One process-local authority, shared with the responsibility owner. It cannot
/// consume a scope minted by another controller or reserve remote BE memory.
#[derive(Clone)]
pub struct LocalResourceAuthority {
    pub(crate) inner: Arc<Inner>,
}

/// Metadata admission only. The request's desired bytes are not a reservation.
/// The guard is owned by the waiting future and retains its scope until every
/// exit path (including dropping a pending future) removes the registration.
struct ResourceWaitRegistration {
    scope: WorkScope,
    class: ResourceClass,
}

impl ResourceWaitRegistration {
    fn register(scope: &WorkScope, class: ResourceClass) -> Result<Self, WorkError> {
        scope.inner.update_facts(|state| {
            let node = state.nodes.get(&scope.id).ok_or(WorkError::Released)?;
            if class == ResourceClass::Data {
                node.check()?;
            }
            let key = (scope.id, class);
            if state.resource_waiters.generic.contains(&key) {
                return Err(WorkError::AlreadyWaitingForResource(class));
            }
            if state.waiting_records() >= scope.inner.config.waiting_limit {
                return Err(WorkError::Capacity("waiting entries"));
            }
            state.resource_waiters.generic.insert(key);
            state.nodes.get_mut(&scope.id).unwrap().resource_waiters += 1;
            state.record_waiting_peak();
            Ok(Self {
                scope: scope.clone(),
                class,
            })
        })
    }
}

impl Drop for ResourceWaitRegistration {
    fn drop(&mut self) {
        self.scope.inner.update_facts(|state| {
            let removed = state
                .resource_waiters
                .generic
                .remove(&(self.scope.id, self.class));
            debug_assert!(removed, "Resource waiter registration was already removed");
            state
                .nodes
                .get_mut(&self.scope.id)
                .unwrap()
                .resource_waiters -= 1;
            state.collect(self.scope.id);
        });
    }
}

/// One ordered reservation before a Worker result fetch. A logical execution
/// has at most one poll in flight, while different scopes queue independently
/// under the process authority.
struct ResultFetchWaitRegistration {
    scope: WorkScope,
    ticket: Option<u64>,
}

impl ResultFetchWaitRegistration {
    fn remove_locked(&mut self, state: &mut State) -> bool {
        let Some(ticket) = self.ticket.take() else {
            return false;
        };
        let removed = state.resource_waiters.result_fetch.remove(&ticket);
        let scope_ticket = state
            .resource_waiters
            .result_fetch_by_scope
            .remove(&self.scope.id);
        debug_assert!(
            removed.is_some(),
            "Result fetch waiter registration was already removed"
        );
        debug_assert_eq!(scope_ticket, Some(ticket));
        if removed.is_some() {
            state
                .nodes
                .get_mut(&self.scope.id)
                .unwrap()
                .resource_waiters -= 1;
            state.collect(self.scope.id);
            true
        } else {
            false
        }
    }
}

impl Drop for ResultFetchWaitRegistration {
    fn drop(&mut self) {
        if self.ticket.is_none() {
            return;
        }
        let inner = Arc::clone(&self.scope.inner);
        inner.update_facts(|state| {
            self.remove_locked(state);
        });
    }
}

/// One ordered decoded-output reservation. A scope admits at most one decode
/// wait because decoding is the single owner transition for its retained raw
/// result.
struct DecodeWaitRegistration {
    scope: WorkScope,
    ticket: Option<u64>,
}

impl DecodeWaitRegistration {
    fn remove_locked(&mut self, state: &mut State) -> bool {
        let Some(ticket) = self.ticket.take() else {
            return false;
        };
        let removed = state.resource_waiters.decode.remove(&ticket);
        let scope_ticket = state
            .resource_waiters
            .decode_by_scope
            .remove(&self.scope.id);
        debug_assert!(
            removed.is_some(),
            "Decode waiter registration was already removed"
        );
        debug_assert_eq!(scope_ticket, Some(ticket));
        if removed.is_some() {
            state
                .nodes
                .get_mut(&self.scope.id)
                .unwrap()
                .resource_waiters -= 1;
            state.collect(self.scope.id);
            true
        } else {
            false
        }
    }
}

impl Drop for DecodeWaitRegistration {
    fn drop(&mut self) {
        if self.ticket.is_none() {
            return;
        }
        let inner = Arc::clone(&self.scope.inner);
        inner.update_facts(|state| {
            self.remove_locked(state);
        });
    }
}

/// One independently ordered protocol-output wait. Unlike generic resource
/// waits, a scope may own several of these because every retained result batch
/// carries its own credit and must be able to make progress independently.
struct ProtocolWaitRegistration {
    scope: WorkScope,
    ticket: Option<u64>,
}

impl ProtocolWaitRegistration {
    fn remove_locked(&mut self, state: &mut State) -> bool {
        let Some(ticket) = self.ticket.take() else {
            return false;
        };
        let removed = state.resource_waiters.protocol.remove(&ticket);
        debug_assert!(
            removed.is_some(),
            "Protocol waiter registration was already removed"
        );
        if removed.is_some() {
            state
                .nodes
                .get_mut(&self.scope.id)
                .unwrap()
                .resource_waiters -= 1;
            state.collect(self.scope.id);
            true
        } else {
            false
        }
    }
}

impl Drop for ProtocolWaitRegistration {
    fn drop(&mut self) {
        if self.ticket.is_none() {
            return;
        }
        let inner = Arc::clone(&self.scope.inner);
        inner.update_facts(|state| {
            self.remove_locked(state);
        });
    }
}

fn check_capacity(
    state: &State,
    config: &ResourceConfig,
    id: WorkId,
    bytes: u64,
    class: ResourceClass,
) -> Result<(), WorkError> {
    let node = state.nodes.get(&id).ok_or(WorkError::Released)?;
    if class == ResourceClass::Data {
        node.check()?;
    }
    let scope_held = node
        .reserved_bytes
        .checked_add(node.used_bytes)
        .and_then(|held| held.checked_add(bytes))
        .ok_or(WorkError::ArithmeticOverflow)?;
    if class == ResourceClass::Data && scope_held > config.per_scope_bytes {
        return Err(WorkError::Capacity("scope allocation bytes"));
    }
    let (reserved, used, limit) = match class {
        ResourceClass::Data => (
            state.data_reserved,
            state.data_used,
            config.total_bytes - config.control_bytes,
        ),
        ResourceClass::Control => (
            state.control_reserved,
            state.control_used,
            config.control_bytes,
        ),
    };
    let held = reserved
        .checked_add(used)
        .and_then(|held| held.checked_add(bytes))
        .ok_or(WorkError::ArithmeticOverflow)?;
    if held > limit {
        return Err(WorkError::Capacity("local allocation bytes"));
    }
    Ok(())
}

fn reserve_bytes(state: &mut State, id: WorkId, bytes: u64, class: ResourceClass) {
    state.nodes.get_mut(&id).unwrap().reserved_bytes += bytes;
    match class {
        ResourceClass::Data => state.data_reserved += bytes,
        ResourceClass::Control => state.control_reserved += bytes,
    }
    state.peak_held_bytes = state
        .peak_held_bytes
        .max(state.data_reserved + state.data_used + state.control_reserved + state.control_used);
}

fn release_reserved(state: &mut State, id: WorkId, bytes: u64, class: ResourceClass) {
    state.nodes.get_mut(&id).unwrap().reserved_bytes -= bytes;
    match class {
        ResourceClass::Data => state.data_reserved -= bytes,
        ResourceClass::Control => state.control_reserved -= bytes,
    }
}

fn reserve_protocol_bytes(
    state: &mut State,
    config: &ResourceConfig,
    scope: WorkId,
    decoded_bytes: u64,
    protocol_bytes: u64,
) -> Result<(), WorkError> {
    let combined = decoded_bytes
        .checked_add(protocol_bytes)
        .ok_or(WorkError::ArithmeticOverflow)?;
    check_capacity(state, config, scope, protocol_bytes, ResourceClass::Data)?;
    checked_result_stage_add(state, scope, ResultCreditStage::ProtocolReserved, combined)?;
    remove_result_stage(
        state,
        scope,
        ResultCreditStage::DecodedQueued,
        decoded_bytes,
    );
    reserve_bytes(state, scope, protocol_bytes, ResourceClass::Data);
    add_result_stage(state, scope, ResultCreditStage::ProtocolReserved, combined);
    Ok(())
}

fn reserve_decode_bytes(
    state: &mut State,
    config: &ResourceConfig,
    scope: WorkId,
    raw_bytes: u64,
    decoded_bytes: u64,
) -> Result<(), WorkError> {
    let combined = raw_bytes
        .checked_add(decoded_bytes)
        .ok_or(WorkError::ArithmeticOverflow)?;
    check_capacity(state, config, scope, decoded_bytes, ResourceClass::Data)?;
    checked_result_stage_add(state, scope, ResultCreditStage::DecodeReserved, combined)?;
    remove_result_stage(state, scope, ResultCreditStage::RawRetained, raw_bytes);
    reserve_bytes(state, scope, decoded_bytes, ResourceClass::Data);
    add_result_stage(state, scope, ResultCreditStage::DecodeReserved, combined);
    Ok(())
}

fn reserve_result_fetch_bytes(
    state: &mut State,
    config: &ResourceConfig,
    scope: &WorkScope,
    bytes: u64,
) -> Result<ResultCredit, WorkError> {
    check_capacity(state, config, scope.id, bytes, ResourceClass::Data)?;
    let node = state.nodes.get(&scope.id).unwrap();
    let holders = node
        .resource_holders
        .checked_add(1)
        .ok_or(WorkError::ArithmeticOverflow)?;
    checked_result_stage_add(
        state,
        scope.id,
        ResultCreditStage::ReservedBeforeFetch,
        bytes,
    )?;

    state.nodes.get_mut(&scope.id).unwrap().resource_holders = holders;
    reserve_bytes(state, scope.id, bytes, ResourceClass::Data);
    add_result_stage(
        state,
        scope.id,
        ResultCreditStage::ReservedBeforeFetch,
        bytes,
    );
    Ok(ResultCredit {
        scope: scope.clone(),
        stage: ResultCreditStage::ReservedBeforeFetch,
        primary_bytes: bytes,
        secondary_bytes: 0,
    })
}

fn register_result_fetch_waiter(
    scope: &WorkScope,
    state: &mut State,
    bytes: u64,
) -> Result<ResultFetchWaitRegistration, WorkError> {
    state
        .nodes
        .get(&scope.id)
        .ok_or(WorkError::Released)?
        .check()?;
    if state
        .resource_waiters
        .result_fetch_by_scope
        .contains_key(&scope.id)
    {
        return Err(WorkError::AlreadyWaitingForResultFetch);
    }
    if state.waiting_records() >= scope.inner.config.waiting_limit {
        return Err(WorkError::Capacity("waiting entries"));
    }
    let ticket = state.next_result_fetch_waiter_id()?;
    state.resource_waiters.result_fetch.insert(
        ticket,
        ResultWaiter {
            scope: scope.id,
            bytes,
        },
    );
    state
        .resource_waiters
        .result_fetch_by_scope
        .insert(scope.id, ticket);
    state.nodes.get_mut(&scope.id).unwrap().resource_waiters += 1;
    state.record_waiting_peak();
    Ok(ResultFetchWaitRegistration {
        scope: scope.clone(),
        ticket: Some(ticket),
    })
}

fn register_protocol_waiter(
    scope: &WorkScope,
    state: &mut State,
    bytes: u64,
) -> Result<ProtocolWaitRegistration, WorkError> {
    state
        .nodes
        .get(&scope.id)
        .ok_or(WorkError::Released)?
        .check()?;
    if state.waiting_records() >= scope.inner.config.waiting_limit {
        return Err(WorkError::Capacity("waiting entries"));
    }
    let ticket = state.next_protocol_waiter_id()?;
    state.resource_waiters.protocol.insert(
        ticket,
        ResultWaiter {
            scope: scope.id,
            bytes,
        },
    );
    state.nodes.get_mut(&scope.id).unwrap().resource_waiters += 1;
    state.record_waiting_peak();
    Ok(ProtocolWaitRegistration {
        scope: scope.clone(),
        ticket: Some(ticket),
    })
}

fn register_decode_waiter(
    scope: &WorkScope,
    state: &mut State,
    bytes: u64,
) -> Result<DecodeWaitRegistration, WorkError> {
    state
        .nodes
        .get(&scope.id)
        .ok_or(WorkError::Released)?
        .check()?;
    if state
        .resource_waiters
        .decode_by_scope
        .contains_key(&scope.id)
    {
        return Err(WorkError::AlreadyWaitingForResultDecode);
    }
    if state.waiting_records() >= scope.inner.config.waiting_limit {
        return Err(WorkError::Capacity("waiting entries"));
    }
    let ticket = state.next_decode_waiter_id()?;
    state.resource_waiters.decode.insert(
        ticket,
        ResultWaiter {
            scope: scope.id,
            bytes,
        },
    );
    state
        .resource_waiters
        .decode_by_scope
        .insert(scope.id, ticket);
    state.nodes.get_mut(&scope.id).unwrap().resource_waiters += 1;
    state.record_waiting_peak();
    Ok(DecodeWaitRegistration {
        scope: scope.clone(),
        ticket: Some(ticket),
    })
}

impl LocalResourceAuthority {
    pub fn reserve(
        &self,
        scope: &WorkScope,
        bytes: u64,
        class: ResourceClass,
    ) -> Result<Reservation, WorkError> {
        if !Arc::ptr_eq(&self.inner, &scope.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        if bytes == 0 {
            return Err(WorkError::Capacity("zero-byte reservation"));
        }
        self.inner.update_facts_silent(|state| {
            check_capacity(state, &self.inner.resource_config, scope.id, bytes, class)?;
            let node = state.nodes.get_mut(&scope.id).unwrap();
            node.resource_holders = node
                .resource_holders
                .checked_add(1)
                .ok_or(WorkError::ArithmeticOverflow)?;
            reserve_bytes(state, scope.id, bytes, class);
            Ok(Reservation {
                scope: scope.clone(),
                remaining: bytes,
                class,
            })
        })
    }

    pub fn snapshot(&self) -> ResourceSnapshot {
        let state = self.inner.state.lock().unwrap();
        ResourceSnapshot {
            total_limit_bytes: self.inner.resource_config.total_bytes,
            data_reserved_bytes: state.data_reserved,
            data_used_bytes: state.data_used,
            control_reserved_bytes: state.control_reserved,
            control_used_bytes: state.control_used,
            peak_held_bytes: state.peak_held_bytes,
            result_credit: state.result_credit,
        }
    }

    /// Reserve data capacity before asking a Worker for a result batch.
    pub fn reserve_result_credit(
        &self,
        scope: &WorkScope,
        bytes: u64,
    ) -> Result<ResultCredit, WorkError> {
        if !Arc::ptr_eq(&self.inner, &scope.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        if bytes == 0 {
            return Err(WorkError::Capacity("zero-byte result credit"));
        }
        self.inner.update_facts_silent(|state| {
            if !state.resource_waiters.result_fetch.is_empty() {
                return Err(WorkError::Capacity("result fetch reservation queue"));
            }
            reserve_result_fetch_bytes(state, &self.inner.resource_config, scope, bytes)
        })
    }

    /// Reserve result capacity in FIFO order before issuing a Worker fetch.
    ///
    /// The queue head checks and charges capacity under the process authority
    /// lock, so a later fetch cannot steal a release between notification and
    /// retry. One absolute deadline bounds the whole wait. Cancellation,
    /// timeout, Drop, and every error remove the registration exactly once.
    pub async fn reserve_result_credit_when_available(
        &self,
        scope: &WorkScope,
        bytes: u64,
    ) -> Result<ResultCredit, WorkError> {
        if !Arc::ptr_eq(&self.inner, &scope.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        if bytes == 0 {
            return Err(WorkError::Capacity("zero-byte result credit"));
        }
        let data_limit =
            self.inner.resource_config.total_bytes - self.inner.resource_config.control_bytes;
        if bytes > self.inner.resource_config.per_scope_bytes || bytes > data_limit {
            return Err(WorkError::Capacity(
                "unrepresentable result fetch allocation",
            ));
        }
        let cancellation = scope.cancellation()?;
        let capacity_deadline = tokio::time::Instant::now()
            .checked_add(self.inner.config.capacity_wait_timeout)
            .ok_or(WorkError::ArithmeticOverflow)?;
        let wait_deadline = cancellation
            .deadline()
            .map_or(capacity_deadline, |deadline| {
                deadline.min(capacity_deadline)
            });

        let start = self.inner.update_facts(|state| {
            state
                .nodes
                .get(&scope.id)
                .ok_or(WorkError::Released)?
                .check()?;
            if tokio::time::Instant::now() >= wait_deadline {
                return Err(WorkError::CapacityWaitTimeout);
            }
            if state.resource_waiters.result_fetch.is_empty() {
                match reserve_result_fetch_bytes(state, &self.inner.resource_config, scope, bytes) {
                    Ok(credit) => return Ok(Ok(credit)),
                    Err(WorkError::Capacity(_)) => {}
                    Err(error) => return Err(error),
                }
            }
            register_result_fetch_waiter(scope, state, bytes).map(Err)
        });
        let mut registration = match start? {
            Ok(credit) => return Ok(credit),
            Err(registration) => registration,
        };

        let timeout = tokio::time::sleep_until(wait_deadline);
        let cancelled = cancellation.cancelled();
        tokio::pin!(timeout, cancelled);
        loop {
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            scope.check()?;
            if tokio::time::Instant::now() >= wait_deadline {
                return Err(WorkError::CapacityWaitTimeout);
            }

            let grant = self.inner.update_facts_silent(|state| {
                let ticket = registration
                    .ticket
                    .expect("live result fetch wait owns its queue ticket");
                let waiter = state
                    .resource_waiters
                    .result_fetch
                    .get(&ticket)
                    .ok_or(WorkError::Released)?;
                debug_assert_eq!((waiter.scope, waiter.bytes), (scope.id, bytes));
                if state.resource_waiters.result_fetch.keys().next().copied() != Some(ticket) {
                    return Ok(None);
                }
                state
                    .nodes
                    .get(&scope.id)
                    .ok_or(WorkError::Released)?
                    .check()?;
                if tokio::time::Instant::now() >= wait_deadline {
                    return Err(WorkError::CapacityWaitTimeout);
                }
                match reserve_result_fetch_bytes(state, &self.inner.resource_config, scope, bytes) {
                    Ok(credit) => {
                        registration.remove_locked(state);
                        Ok(Some(credit))
                    }
                    Err(WorkError::Capacity(_)) => Ok(None),
                    Err(error) => Err(error),
                }
            });
            match grant {
                Ok(Some(credit)) => {
                    self.inner.notify_capacity_available();
                    return Ok(credit);
                }
                Ok(None) => {}
                Err(error) => return Err(error),
            }

            tokio::select! {
                _ = changed => {},
                _ = &mut timeout => {},
                reason = &mut cancelled => return Err(WorkError::Cancelled(reason)),
            }
        }
    }

    /// Wait for a capacity hint; callers retry reserve to acquire the capacity.
    /// On first poll, register one waiter per exact scope/class within the same
    /// global entry limit used by stage admission. A duplicate is rejected.
    /// Data waits inherit cancellation and the earlier parent deadline. Control
    /// waits keep their independent capacity timeout so expired work can clean up.
    pub async fn wait_for_capacity(
        &self,
        scope: &WorkScope,
        bytes: u64,
        class: ResourceClass,
    ) -> Result<(), WorkError> {
        if !Arc::ptr_eq(&self.inner, &scope.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        let limit = match class {
            ResourceClass::Data => {
                self.inner.resource_config.total_bytes - self.inner.resource_config.control_bytes
            }
            ResourceClass::Control => self.inner.resource_config.control_bytes,
        };
        if bytes == 0
            || bytes > limit
            || (class == ResourceClass::Data && bytes > self.inner.resource_config.per_scope_bytes)
        {
            return Err(WorkError::Capacity("unrepresentable allocation"));
        }
        let _registration = ResourceWaitRegistration::register(scope, class)?;
        let cancellation = scope.cancellation()?;
        let wait_deadline = tokio::time::Instant::now()
            .checked_add(self.inner.config.capacity_wait_timeout)
            .ok_or(WorkError::ArithmeticOverflow)?;
        let wait_deadline = match class {
            ResourceClass::Data => cancellation
                .deadline()
                .map_or(wait_deadline, |parent| parent.min(wait_deadline)),
            ResourceClass::Control => wait_deadline,
        };
        let timeout = tokio::time::sleep_until(wait_deadline);
        tokio::pin!(timeout);
        loop {
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if class == ResourceClass::Data {
                scope.check()?;
            }
            if tokio::time::Instant::now() >= wait_deadline {
                return Err(WorkError::CapacityWaitTimeout);
            }
            match check_capacity(
                &self.inner.state.lock().unwrap(),
                &self.inner.resource_config,
                scope.id,
                bytes,
                class,
            ) {
                Ok(()) => return Ok(()),
                Err(WorkError::Capacity(_)) => {}
                Err(error) => return Err(error),
            }
            if class == ResourceClass::Control {
                tokio::select! { _ = changed => {}, _ = &mut timeout => {} }
            } else {
                tokio::select! {
                    _ = changed => {},
                    _ = &mut timeout => {},
                    reason = cancellation.cancelled() => return Err(WorkError::Cancelled(reason)),
                }
            }
        }
    }
}

fn checked_result_stage_add(
    state: &State,
    id: WorkId,
    stage: ResultCreditStage,
    bytes: u64,
) -> Result<(), WorkError> {
    state
        .result_credit
        .bytes(stage)
        .checked_add(bytes)
        .ok_or(WorkError::ArithmeticOverflow)?;
    state
        .nodes
        .get(&id)
        .ok_or(WorkError::Released)?
        .result_credit
        .bytes(stage)
        .checked_add(bytes)
        .ok_or(WorkError::ArithmeticOverflow)?;
    Ok(())
}

fn add_result_stage(state: &mut State, id: WorkId, stage: ResultCreditStage, bytes: u64) {
    let process = state.result_credit.bytes(stage) + bytes;
    state.result_credit.set_bytes(stage, process);
    let scope = &mut state.nodes.get_mut(&id).unwrap().result_credit;
    scope.set_bytes(stage, scope.bytes(stage) + bytes);
}

fn remove_result_stage(state: &mut State, id: WorkId, stage: ResultCreditStage, bytes: u64) {
    state
        .result_credit
        .set_bytes(stage, state.result_credit.bytes(stage) - bytes);
    let scope = &mut state.nodes.get_mut(&id).unwrap().result_credit;
    scope.set_bytes(stage, scope.bytes(stage) - bytes);
}

/// Move-only ownership of one result batch's local memory budget.
pub struct ResultCredit {
    scope: WorkScope,
    stage: ResultCreditStage,
    /// Reserved or retained bytes, depending on the current stage.
    primary_bytes: u64,
    /// Decoded-output reservation held concurrently with raw bytes.
    secondary_bytes: u64,
}

/// A result-credit transition refusal that preserves the existing credit. The
/// caller destroys or transfers the still-accounted payload before releasing
/// the token, or may retry a capacity-dependent transition.
pub struct ResultCreditReservationError {
    error: WorkError,
    credit: ResultCredit,
}

impl ResultCreditReservationError {
    pub const fn error(&self) -> &WorkError {
        &self.error
    }

    pub fn into_parts(self) -> (WorkError, ResultCredit) {
        (self.error, self.credit)
    }
}

impl std::fmt::Debug for ResultCreditReservationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResultCreditReservationError")
            .field("error", &self.error)
            .field("stage", &self.credit.stage)
            .finish()
    }
}

impl ResultCredit {
    pub fn stage(&self) -> ResultCreditStage {
        self.stage
    }

    pub fn held_bytes(&self) -> u64 {
        self.primary_bytes + self.secondary_bytes
    }

    fn require(
        &self,
        expected: ResultCreditStage,
        requested: ResultCreditStage,
    ) -> Result<(), WorkError> {
        if self.stage != expected {
            return Err(WorkError::InvalidResultCreditTransition {
                from: self.stage,
                requested,
            });
        }
        Ok(())
    }

    pub fn begin_fetch(mut self) -> Result<Self, WorkError> {
        self.require(
            ResultCreditStage::ReservedBeforeFetch,
            ResultCreditStage::InFlightRaw,
        )?;
        self.scope.check()?;
        self.move_stage(ResultCreditStage::InFlightRaw)?;
        Ok(self)
    }

    /// Record the exact bytes returned by the Worker. Unused fetch capacity is
    /// released only after the fetch completes.
    pub fn retain_raw(mut self, actual_bytes: u64) -> Result<Self, ResultCreditReservationError> {
        if let Err(error) = self.require(
            ResultCreditStage::InFlightRaw,
            ResultCreditStage::RawRetained,
        ) {
            return Err(Self::reservation_error(error, self));
        }
        if actual_bytes == 0 || actual_bytes > self.primary_bytes {
            return Err(Self::reservation_error(
                WorkError::Capacity("invalid raw result bytes"),
                self,
            ));
        }
        let reserved = self.primary_bytes;
        let result = self.scope.inner.update_facts_silent(|state| {
            checked_result_stage_add(
                state,
                self.scope.id,
                ResultCreditStage::RawRetained,
                actual_bytes,
            )?;
            state
                .data_used
                .checked_add(actual_bytes)
                .ok_or(WorkError::ArithmeticOverflow)?;
            state
                .nodes
                .get(&self.scope.id)
                .unwrap()
                .used_bytes
                .checked_add(actual_bytes)
                .ok_or(WorkError::ArithmeticOverflow)?;

            remove_result_stage(
                state,
                self.scope.id,
                ResultCreditStage::InFlightRaw,
                reserved,
            );
            release_reserved(state, self.scope.id, reserved, ResourceClass::Data);
            state.nodes.get_mut(&self.scope.id).unwrap().used_bytes += actual_bytes;
            state.data_used += actual_bytes;
            add_result_stage(
                state,
                self.scope.id,
                ResultCreditStage::RawRetained,
                actual_bytes,
            );
            Ok(())
        });
        if let Err(error) = result {
            return Err(Self::reservation_error(error, self));
        }
        if actual_bytes < reserved {
            self.scope.inner.notify_capacity_available();
        }
        self.stage = ResultCreditStage::RawRetained;
        self.primary_bytes = actual_bytes;
        Ok(self)
    }

    /// Reserve decoded output while raw input remains retained. Requiring the
    /// authority here prevents a foreign authority from growing the token.
    pub fn reserve_decode(
        mut self,
        authority: &LocalResourceAuthority,
        bytes: u64,
    ) -> Result<Self, ResultCreditReservationError> {
        if let Err(error) = self.require(
            ResultCreditStage::RawRetained,
            ResultCreditStage::DecodeReserved,
        ) {
            return Err(Self::reservation_error(error, self));
        }
        if let Err(error) = self.scope.check() {
            return Err(Self::reservation_error(error, self));
        }
        if !Arc::ptr_eq(&authority.inner, &self.scope.inner) {
            return Err(Self::reservation_error(WorkError::ForeignAuthority, self));
        }
        if bytes == 0 {
            return Err(Self::reservation_error(
                WorkError::Capacity("zero-byte decode reservation"),
                self,
            ));
        }
        let result = self.scope.inner.update_facts_silent(|state| {
            if !state.resource_waiters.decode.is_empty() {
                return Err(WorkError::Capacity("decode reservation queue"));
            }
            reserve_decode_bytes(
                state,
                &self.scope.inner.resource_config,
                self.scope.id,
                self.primary_bytes,
                bytes,
            )
        });
        if let Err(error) = result {
            return Err(Self::reservation_error(error, self));
        }
        self.stage = ResultCreditStage::DecodeReserved;
        self.secondary_bytes = bytes;
        Ok(self)
    }

    /// Reserve decoded-output bytes in FIFO order while retaining the raw
    /// credit. At most one decode wait may be registered for a scope. The queue
    /// head checks and charges capacity under the authority lock, so a direct
    /// reservation or a later waiter cannot bypass it. Cancellation, timeout,
    /// Drop, and every error remove the registration without consuming the
    /// original token.
    pub async fn reserve_decode_when_available(
        mut self,
        authority: &LocalResourceAuthority,
        bytes: u64,
    ) -> Result<Self, ResultCreditReservationError> {
        if let Err(error) = self.require(
            ResultCreditStage::RawRetained,
            ResultCreditStage::DecodeReserved,
        ) {
            return Err(Self::reservation_error(error, self));
        }
        if let Err(error) = self.scope.check() {
            return Err(Self::reservation_error(error, self));
        }
        if !Arc::ptr_eq(&authority.inner, &self.scope.inner) {
            return Err(Self::reservation_error(WorkError::ForeignAuthority, self));
        }
        if bytes == 0 {
            return Err(Self::reservation_error(
                WorkError::Capacity("zero-byte decode reservation"),
                self,
            ));
        }
        let Some(combined) = self.primary_bytes.checked_add(bytes) else {
            return Err(Self::reservation_error(WorkError::ArithmeticOverflow, self));
        };
        let config = &self.scope.inner.resource_config;
        let data_limit = config.total_bytes - config.control_bytes;
        if combined > config.per_scope_bytes || combined > data_limit {
            return Err(Self::reservation_error(
                WorkError::Capacity("unrepresentable decode allocation"),
                self,
            ));
        }
        let cancellation = match self.scope.cancellation() {
            Ok(cancellation) => cancellation,
            Err(error) => return Err(Self::reservation_error(error, self)),
        };
        let capacity_deadline = match tokio::time::Instant::now()
            .checked_add(self.scope.inner.config.capacity_wait_timeout)
        {
            Some(deadline) => deadline,
            None => {
                return Err(Self::reservation_error(WorkError::ArithmeticOverflow, self));
            }
        };
        let wait_deadline = cancellation
            .deadline()
            .map_or(capacity_deadline, |deadline| {
                deadline.min(capacity_deadline)
            });

        let start = self.scope.inner.update_facts(|state| {
            state
                .nodes
                .get(&self.scope.id)
                .ok_or(WorkError::Released)?
                .check()?;
            if tokio::time::Instant::now() >= wait_deadline {
                return Err(WorkError::CapacityWaitTimeout);
            }
            if state.resource_waiters.decode.is_empty() {
                match reserve_decode_bytes(
                    state,
                    &self.scope.inner.resource_config,
                    self.scope.id,
                    self.primary_bytes,
                    bytes,
                ) {
                    Ok(()) => return Ok(None),
                    Err(WorkError::Capacity(_)) => {}
                    Err(error) => return Err(error),
                }
            }
            register_decode_waiter(&self.scope, state, bytes).map(Some)
        });
        let mut registration = match start {
            Ok(None) => {
                self.stage = ResultCreditStage::DecodeReserved;
                self.secondary_bytes = bytes;
                return Ok(self);
            }
            Ok(Some(registration)) => registration,
            Err(error) => return Err(Self::reservation_error(error, self)),
        };

        let inner = Arc::clone(&self.scope.inner);
        let timeout = tokio::time::sleep_until(wait_deadline);
        let cancelled = cancellation.cancelled();
        tokio::pin!(timeout, cancelled);
        loop {
            let changed = inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Err(error) = self.scope.check() {
                return Err(Self::reservation_error(error, self));
            }
            if tokio::time::Instant::now() >= wait_deadline {
                return Err(Self::reservation_error(
                    WorkError::CapacityWaitTimeout,
                    self,
                ));
            }

            let grant = inner.update_facts_silent(|state| {
                let ticket = registration
                    .ticket
                    .expect("live decode wait owns its queue ticket");
                let waiter = state
                    .resource_waiters
                    .decode
                    .get(&ticket)
                    .ok_or(WorkError::Released)?;
                debug_assert_eq!((waiter.scope, waiter.bytes), (self.scope.id, bytes));
                if state.resource_waiters.decode.keys().next().copied() != Some(ticket) {
                    return Ok(false);
                }
                state
                    .nodes
                    .get(&self.scope.id)
                    .ok_or(WorkError::Released)?
                    .check()?;
                if tokio::time::Instant::now() >= wait_deadline {
                    return Err(WorkError::CapacityWaitTimeout);
                }
                match reserve_decode_bytes(
                    state,
                    &self.scope.inner.resource_config,
                    self.scope.id,
                    self.primary_bytes,
                    bytes,
                ) {
                    Ok(()) => {
                        registration.remove_locked(state);
                        Ok(true)
                    }
                    Err(WorkError::Capacity(_)) => Ok(false),
                    Err(error) => Err(error),
                }
            });
            match grant {
                Ok(true) => {
                    inner.notify_capacity_available();
                    self.stage = ResultCreditStage::DecodeReserved;
                    self.secondary_bytes = bytes;
                    return Ok(self);
                }
                Ok(false) => {}
                Err(error) => return Err(Self::reservation_error(error, self)),
            }

            tokio::select! {
                _ = changed => {},
                _ = &mut timeout => {},
                reason = &mut cancelled => {
                    return Err(Self::reservation_error(WorkError::Cancelled(reason), self));
                }
            }
        }
    }

    /// Publish decoded output and release its raw predecessor in one ledger
    /// update.
    pub fn queue_decoded(
        mut self,
        actual_bytes: u64,
    ) -> Result<Self, ResultCreditReservationError> {
        if let Err(error) = self.require(
            ResultCreditStage::DecodeReserved,
            ResultCreditStage::DecodedQueued,
        ) {
            return Err(Self::reservation_error(error, self));
        }
        if actual_bytes == 0 || actual_bytes > self.secondary_bytes {
            return Err(Self::reservation_error(
                WorkError::Capacity("invalid decoded result bytes"),
                self,
            ));
        }
        let raw = self.primary_bytes;
        let reserved = self.secondary_bytes;
        let Some(combined) = raw.checked_add(reserved) else {
            return Err(Self::reservation_error(WorkError::ArithmeticOverflow, self));
        };
        let result = self.scope.inner.update_facts_silent(|state| {
            checked_result_stage_add(
                state,
                self.scope.id,
                ResultCreditStage::DecodedQueued,
                actual_bytes,
            )?;
            let next_process_used = state
                .data_used
                .checked_sub(raw)
                .and_then(|used| used.checked_add(actual_bytes))
                .ok_or(WorkError::ArithmeticOverflow)?;
            let node = state.nodes.get(&self.scope.id).unwrap();
            let next_scope_used = node
                .used_bytes
                .checked_sub(raw)
                .and_then(|used| used.checked_add(actual_bytes))
                .ok_or(WorkError::ArithmeticOverflow)?;

            remove_result_stage(
                state,
                self.scope.id,
                ResultCreditStage::DecodeReserved,
                combined,
            );
            release_reserved(state, self.scope.id, reserved, ResourceClass::Data);
            state.nodes.get_mut(&self.scope.id).unwrap().used_bytes = next_scope_used;
            state.data_used = next_process_used;
            add_result_stage(
                state,
                self.scope.id,
                ResultCreditStage::DecodedQueued,
                actual_bytes,
            );
            Ok(())
        });
        if let Err(error) = result {
            return Err(Self::reservation_error(error, self));
        }
        self.scope.inner.notify_capacity_available();
        self.stage = ResultCreditStage::DecodedQueued;
        self.primary_bytes = actual_bytes;
        self.secondary_bytes = 0;
        Ok(self)
    }

    /// Reserve protocol output while the decoded batch remains live. The
    /// adapter performs this before allocating an encoded packet or row buffer.
    pub fn reserve_protocol(
        mut self,
        authority: &LocalResourceAuthority,
        bytes: u64,
    ) -> Result<Self, ResultCreditReservationError> {
        if let Err(error) = self.require(
            ResultCreditStage::DecodedQueued,
            ResultCreditStage::ProtocolReserved,
        ) {
            return Err(Self::reservation_error(error, self));
        }
        if let Err(error) = self.scope.check() {
            return Err(Self::reservation_error(error, self));
        }
        if !Arc::ptr_eq(&authority.inner, &self.scope.inner) {
            return Err(Self::reservation_error(WorkError::ForeignAuthority, self));
        }
        if bytes == 0 {
            return Err(Self::reservation_error(
                WorkError::Capacity("zero-byte protocol reservation"),
                self,
            ));
        }
        let result = self.scope.inner.update_facts_silent(|state| {
            if !state.resource_waiters.protocol.is_empty() {
                return Err(WorkError::Capacity("protocol reservation queue"));
            }
            reserve_protocol_bytes(
                state,
                &self.scope.inner.resource_config,
                self.scope.id,
                self.primary_bytes,
                bytes,
            )
        });
        if let Err(error) = result {
            return Err(Self::reservation_error(error, self));
        }
        self.stage = ResultCreditStage::ProtocolReserved;
        self.secondary_bytes = bytes;
        Ok(self)
    }

    /// Reserve protocol-output bytes in FIFO order, waiting through temporary
    /// local pressure while retaining this decoded credit.
    ///
    /// Each invocation owns a distinct bounded registration, so several live
    /// batches from one scope can wait concurrently. The queue head checks and
    /// charges capacity under the authority lock, preventing a later protocol
    /// waiter from stealing a release between notification and retry. One
    /// absolute deadline bounds the entire operation.
    pub async fn reserve_protocol_when_available(
        mut self,
        authority: &LocalResourceAuthority,
        bytes: u64,
    ) -> Result<Self, ResultCreditReservationError> {
        if let Err(error) = self.require(
            ResultCreditStage::DecodedQueued,
            ResultCreditStage::ProtocolReserved,
        ) {
            return Err(Self::reservation_error(error, self));
        }
        if let Err(error) = self.scope.check() {
            return Err(Self::reservation_error(error, self));
        }
        if !Arc::ptr_eq(&authority.inner, &self.scope.inner) {
            return Err(Self::reservation_error(WorkError::ForeignAuthority, self));
        }
        if bytes == 0 {
            return Err(Self::reservation_error(
                WorkError::Capacity("zero-byte protocol reservation"),
                self,
            ));
        }
        let Some(combined) = self.primary_bytes.checked_add(bytes) else {
            return Err(Self::reservation_error(WorkError::ArithmeticOverflow, self));
        };
        let config = &self.scope.inner.resource_config;
        let data_limit = config.total_bytes - config.control_bytes;
        if combined > config.per_scope_bytes || combined > data_limit {
            return Err(Self::reservation_error(
                WorkError::Capacity("unrepresentable protocol allocation"),
                self,
            ));
        }
        let cancellation = match self.scope.cancellation() {
            Ok(cancellation) => cancellation,
            Err(error) => return Err(Self::reservation_error(error, self)),
        };
        let capacity_deadline = match tokio::time::Instant::now()
            .checked_add(self.scope.inner.config.capacity_wait_timeout)
        {
            Some(deadline) => deadline,
            None => {
                return Err(Self::reservation_error(WorkError::ArithmeticOverflow, self));
            }
        };
        let wait_deadline = cancellation
            .deadline()
            .map_or(capacity_deadline, |deadline| {
                deadline.min(capacity_deadline)
            });

        let start = self.scope.inner.update_facts(|state| {
            state
                .nodes
                .get(&self.scope.id)
                .ok_or(WorkError::Released)?
                .check()?;
            if tokio::time::Instant::now() >= wait_deadline {
                return Err(WorkError::CapacityWaitTimeout);
            }
            if state.resource_waiters.protocol.is_empty() {
                match reserve_protocol_bytes(
                    state,
                    &self.scope.inner.resource_config,
                    self.scope.id,
                    self.primary_bytes,
                    bytes,
                ) {
                    Ok(()) => return Ok(None),
                    Err(WorkError::Capacity(_)) => {}
                    Err(error) => return Err(error),
                }
            }
            register_protocol_waiter(&self.scope, state, bytes).map(Some)
        });
        let mut registration = match start {
            Ok(None) => {
                self.stage = ResultCreditStage::ProtocolReserved;
                self.secondary_bytes = bytes;
                return Ok(self);
            }
            Ok(Some(registration)) => registration,
            Err(error) => return Err(Self::reservation_error(error, self)),
        };

        let inner = Arc::clone(&self.scope.inner);
        let timeout = tokio::time::sleep_until(wait_deadline);
        let cancelled = cancellation.cancelled();
        tokio::pin!(timeout, cancelled);
        loop {
            let changed = inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Err(error) = self.scope.check() {
                return Err(Self::reservation_error(error, self));
            }
            if tokio::time::Instant::now() >= wait_deadline {
                return Err(Self::reservation_error(
                    WorkError::CapacityWaitTimeout,
                    self,
                ));
            }

            let grant = inner.update_facts_silent(|state| {
                let ticket = registration
                    .ticket
                    .expect("live protocol wait owns its queue ticket");
                let waiter = state
                    .resource_waiters
                    .protocol
                    .get(&ticket)
                    .ok_or(WorkError::Released)?;
                debug_assert_eq!((waiter.scope, waiter.bytes), (self.scope.id, bytes));
                if state.resource_waiters.protocol.keys().next().copied() != Some(ticket) {
                    return Ok(false);
                }
                state
                    .nodes
                    .get(&self.scope.id)
                    .ok_or(WorkError::Released)?
                    .check()?;
                if tokio::time::Instant::now() >= wait_deadline {
                    return Err(WorkError::CapacityWaitTimeout);
                }
                match reserve_protocol_bytes(
                    state,
                    &self.scope.inner.resource_config,
                    self.scope.id,
                    self.primary_bytes,
                    bytes,
                ) {
                    Ok(()) => {
                        registration.remove_locked(state);
                        Ok(true)
                    }
                    Err(WorkError::Capacity(_)) => Ok(false),
                    Err(error) => Err(error),
                }
            });
            match grant {
                Ok(true) => {
                    inner.notify_capacity_available();
                    self.stage = ResultCreditStage::ProtocolReserved;
                    self.secondary_bytes = bytes;
                    return Ok(self);
                }
                Ok(false) => {}
                Err(error) => return Err(Self::reservation_error(error, self)),
            }

            tokio::select! {
                _ = changed => {},
                _ = &mut timeout => {},
                reason = &mut cancelled => {
                    return Err(Self::reservation_error(WorkError::Cancelled(reason), self));
                }
            }
        }
    }

    /// Convert the protocol reservation to its retained bytes without
    /// releasing the decoded batch that the writer may still reference.
    pub fn begin_protocol_write(
        mut self,
        actual_bytes: u64,
    ) -> Result<Self, ResultCreditReservationError> {
        if let Err(error) = self.require(
            ResultCreditStage::ProtocolReserved,
            ResultCreditStage::ProtocolWriting,
        ) {
            return Err(Self::reservation_error(error, self));
        }
        if let Err(error) = self.scope.check() {
            return Err(Self::reservation_error(error, self));
        }
        if actual_bytes == 0 || actual_bytes > self.secondary_bytes {
            return Err(Self::reservation_error(
                WorkError::Capacity("invalid protocol result bytes"),
                self,
            ));
        }
        let decoded = self.primary_bytes;
        let reserved = self.secondary_bytes;
        let Some(retained) = decoded.checked_add(actual_bytes) else {
            return Err(Self::reservation_error(WorkError::ArithmeticOverflow, self));
        };
        let result = self.scope.inner.update_facts_silent(|state| {
            checked_result_stage_add(
                state,
                self.scope.id,
                ResultCreditStage::ProtocolWriting,
                retained,
            )?;
            let next_process_used = state
                .data_used
                .checked_add(actual_bytes)
                .ok_or(WorkError::ArithmeticOverflow)?;
            let next_scope_used = state
                .nodes
                .get(&self.scope.id)
                .ok_or(WorkError::Released)?
                .used_bytes
                .checked_add(actual_bytes)
                .ok_or(WorkError::ArithmeticOverflow)?;
            remove_result_stage(
                state,
                self.scope.id,
                ResultCreditStage::ProtocolReserved,
                decoded + reserved,
            );
            release_reserved(state, self.scope.id, reserved, ResourceClass::Data);
            state.nodes.get_mut(&self.scope.id).unwrap().used_bytes = next_scope_used;
            state.data_used = next_process_used;
            add_result_stage(
                state,
                self.scope.id,
                ResultCreditStage::ProtocolWriting,
                retained,
            );
            Ok(())
        });
        if let Err(error) = result {
            return Err(Self::reservation_error(error, self));
        }
        if actual_bytes < reserved {
            self.scope.inner.notify_capacity_available();
        }
        self.stage = ResultCreditStage::ProtocolWriting;
        self.secondary_bytes = actual_bytes;
        Ok(self)
    }

    /// The protocol owner calls this only after actual consumer acceptance.
    pub fn consume(mut self) -> Result<(), WorkError> {
        self.require(
            ResultCreditStage::ProtocolWriting,
            ResultCreditStage::Consumed,
        )?;
        self.release_current();
        self.stage = ResultCreditStage::Consumed;
        Ok(())
    }

    fn move_stage(&mut self, next: ResultCreditStage) -> Result<(), WorkError> {
        let bytes = self.held_bytes();
        self.scope.inner.update_facts_silent(|state| {
            checked_result_stage_add(state, self.scope.id, next, bytes)?;
            remove_result_stage(state, self.scope.id, self.stage, bytes);
            add_result_stage(state, self.scope.id, next, bytes);
            Ok(())
        })?;
        self.stage = next;
        Ok(())
    }

    fn reservation_error(error: WorkError, credit: Self) -> ResultCreditReservationError {
        ResultCreditReservationError { error, credit }
    }

    fn release_current(&mut self) {
        if self.stage == ResultCreditStage::Consumed {
            return;
        }
        let stage = self.stage;
        let primary = self.primary_bytes;
        let secondary = self.secondary_bytes;
        self.scope.inner.update_facts_silent(|state| {
            remove_result_stage(state, self.scope.id, stage, primary + secondary);
            match stage {
                ResultCreditStage::ReservedBeforeFetch | ResultCreditStage::InFlightRaw => {
                    release_reserved(state, self.scope.id, primary, ResourceClass::Data);
                }
                ResultCreditStage::RawRetained | ResultCreditStage::DecodedQueued => {
                    state.nodes.get_mut(&self.scope.id).unwrap().used_bytes -= primary;
                    state.data_used -= primary;
                }
                ResultCreditStage::DecodeReserved | ResultCreditStage::ProtocolReserved => {
                    release_reserved(state, self.scope.id, secondary, ResourceClass::Data);
                    state.nodes.get_mut(&self.scope.id).unwrap().used_bytes -= primary;
                    state.data_used -= primary;
                }
                ResultCreditStage::ProtocolWriting => {
                    state.nodes.get_mut(&self.scope.id).unwrap().used_bytes -= primary + secondary;
                    state.data_used -= primary + secondary;
                }
                ResultCreditStage::Consumed => unreachable!(),
            }
            state
                .nodes
                .get_mut(&self.scope.id)
                .unwrap()
                .resource_holders -= 1;
            state.collect(self.scope.id);
        });
        self.scope.inner.notify_capacity_available();
        self.primary_bytes = 0;
        self.secondary_bytes = 0;
    }
}

impl Drop for ResultCredit {
    fn drop(&mut self) {
        self.release_current();
    }
}

/// Owns unused capacity. Dropping it cannot release bytes already converted
/// into allocation charges. Growth is checked before the allocation occurs.
pub struct Reservation {
    scope: WorkScope,
    remaining: u64,
    class: ResourceClass,
}

impl Reservation {
    pub fn remaining_bytes(&self) -> u64 {
        self.remaining
    }

    pub fn grow(&mut self, additional: u64) -> Result<(), WorkError> {
        self.scope.inner.update_facts(|state| {
            check_capacity(
                state,
                &self.scope.inner.resource_config,
                self.scope.id,
                additional,
                self.class,
            )?;
            if self.remaining == 0 && additional != 0 {
                let node = state.nodes.get_mut(&self.scope.id).unwrap();
                node.resource_holders = node
                    .resource_holders
                    .checked_add(1)
                    .ok_or(WorkError::ArithmeticOverflow)?;
            }
            self.remaining = self
                .remaining
                .checked_add(additional)
                .ok_or(WorkError::ArithmeticOverflow)?;
            reserve_bytes(state, self.scope.id, additional, self.class);
            Ok(())
        })
    }

    /// Attach the returned charge to the backing allocation, before retaining
    /// its bytes. Slices must clone this same charge, not create smaller charges.
    pub fn charge(&mut self, bytes: u64) -> Result<AllocationCharge, WorkError> {
        if bytes == 0 || bytes > self.remaining {
            return Err(WorkError::Capacity("unused reservation"));
        }
        self.scope.inner.update_facts(|state| {
            let node = state.nodes.get_mut(&self.scope.id).unwrap();
            if self.class == ResourceClass::Data {
                node.check()?;
            }
            if bytes < self.remaining {
                node.resource_holders = node
                    .resource_holders
                    .checked_add(1)
                    .ok_or(WorkError::ArithmeticOverflow)?;
            }
            node.reserved_bytes -= bytes;
            node.used_bytes += bytes;
            match self.class {
                ResourceClass::Data => {
                    state.data_reserved -= bytes;
                    state.data_used += bytes;
                }
                ResourceClass::Control => {
                    state.control_reserved -= bytes;
                    state.control_used += bytes;
                }
            }
            self.remaining -= bytes;
            Ok(AllocationCharge {
                allocation: Arc::new(Allocation {
                    inner: Arc::clone(&self.scope.inner),
                    owner: Mutex::new(self.scope.id),
                    bytes,
                    class: self.class,
                }),
            })
        })
    }

    pub fn release_unused(&mut self, bytes: u64) -> Result<(), WorkError> {
        if bytes > self.remaining {
            return Err(WorkError::Capacity("unused reservation"));
        }
        if bytes == 0 {
            return Ok(());
        }
        self.scope.inner.update_facts(|state| {
            release_reserved(state, self.scope.id, bytes, self.class);
            if bytes == self.remaining {
                state
                    .nodes
                    .get_mut(&self.scope.id)
                    .unwrap()
                    .resource_holders -= 1;
                state.collect(self.scope.id);
            }
        });
        self.remaining -= bytes;
        Ok(())
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if self.remaining == 0 {
            return;
        }
        self.scope.inner.update_facts(|state| {
            release_reserved(state, self.scope.id, self.remaining, self.class);
            state
                .nodes
                .get_mut(&self.scope.id)
                .unwrap()
                .resource_holders -= 1;
            state.collect(self.scope.id);
        });
    }
}

struct Allocation {
    inner: Arc<Inner>,
    owner: Mutex<WorkId>,
    bytes: u64,
    class: ResourceClass,
}

impl Drop for Allocation {
    fn drop(&mut self) {
        let id = *self.owner.get_mut().unwrap();
        self.inner.update_facts(|state| {
            let node = state.nodes.get_mut(&id).unwrap();
            node.used_bytes -= self.bytes;
            node.resource_holders -= 1;
            match self.class {
                ResourceClass::Data => state.data_used -= self.bytes,
                ResourceClass::Control => state.control_used -= self.bytes,
            }
            state.collect(id);
        });
    }
}

/// Shared backing-allocation ownership. Only the last clone's destruction
/// supplies a release fact. Cloning cannot double-charge bytes.
#[derive(Clone)]
pub struct AllocationCharge {
    allocation: Arc<Allocation>,
}

impl AllocationCharge {
    pub fn bytes(&self) -> u64 {
        self.allocation.bytes
    }

    /// Transfer all shared aliases together within the same local authority.
    /// No temporary global uncharge occurs; the recipient's scope limit applies.
    pub fn transfer_to(&self, scope: &WorkScope) -> Result<(), WorkError> {
        if !Arc::ptr_eq(&self.allocation.inner, &scope.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        let mut owner = self.allocation.owner.lock().unwrap();
        if *owner == scope.id {
            return if self.allocation.class == ResourceClass::Data {
                scope.check()
            } else {
                Ok(())
            };
        }
        self.allocation.inner.update_facts(|state| {
            let target = state.nodes.get_mut(&scope.id).ok_or(WorkError::Released)?;
            if self.allocation.class == ResourceClass::Data {
                target.check()?;
            }
            let held = target
                .used_bytes
                .checked_add(target.reserved_bytes)
                .and_then(|held| held.checked_add(self.allocation.bytes))
                .ok_or(WorkError::ArithmeticOverflow)?;
            if self.allocation.class == ResourceClass::Data
                && held > self.allocation.inner.resource_config.per_scope_bytes
            {
                return Err(WorkError::Capacity("recipient allocation bytes"));
            }
            target.resource_holders = target
                .resource_holders
                .checked_add(1)
                .ok_or(WorkError::ArithmeticOverflow)?;
            target.used_bytes += self.allocation.bytes;
            let source = state.nodes.get_mut(&owner).unwrap();
            source.resource_holders -= 1;
            source.used_bytes -= self.allocation.bytes;
            state.collect(*owner);
            *owner = scope.id;
            Ok(())
        })
    }
}
