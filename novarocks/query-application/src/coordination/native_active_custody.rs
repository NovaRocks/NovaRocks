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

//! Original activation-return custody under the same runtime registration.
//! Live Native resources belong to the installed Source. This table records
//! only the original return and its checked local disposition, never protocol
//! dispatch, remote closure, Work or an active execution behavior.

use super::runtime_registry::health::LogicalExecutionHealth;
use crate::api::{
    ActivatedNativeAttempt, NativeRowsAttemptRuntime, QueryExecutionError, QueryExecutionErrorKind,
    StatementNativeHealthFailure,
};
use novarocks_coordination_contract::AttemptActivationIdentity;
use novarocks_execution_contract::{ParticipationRef, TaskIdentity};
use novarocks_types::{AttemptId, QueryExecutionId, QueryId};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::Notify;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct NativeActiveCustodyKey {
    pub(crate) initial: QueryExecutionId,
    pub(crate) generation: u64,
    pub(crate) execution: QueryExecutionId,
}
impl NativeActiveCustodyKey {
    fn registration_lower_bound(initial: QueryExecutionId, generation: u64) -> Self {
        Self {
            initial,
            generation,
            execution: QueryExecutionId::new(
                QueryId::new(i64::MIN, i64::MIN),
                AttemptId::new(1).expect("lowest nonzero attempt"),
            )
            .expect("signed-min query halves are nonzero"),
        }
    }
}
// A completion-only return still occupies Returned. Absence of rows never
// means Reserved, actual Native resource exit, or permission to remove a slot.
enum ReturnState {
    Reserved,
    Received(AttemptActivationIdentity),
    Returned(AttemptActivationIdentity),
    Borrowed(AttemptActivationIdentity),
    // This is retained custody, not another failure decision. Keep the exact
    // activation and raw failure alive until explicit process-exit abandonment,
    // even when production only needs the slot's registration-blocking state.
    Unproven {
        _activation: AttemptActivationIdentity,
        _failure: StatementNativeHealthFailure,
    },
    Closed,
}
struct ActiveSlot {
    original_exit: Weak<super::original_exit::OriginalLogicalExit>,
    participations: BTreeSet<ParticipationRef>,
    state: Mutex<ReturnState>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PendingNativeCleanupRefusal {
    Registry(super::LogicalExecutionRuntimeRegistryError),
    ForeignSource,
    NotStaged,
    SlotUnavailable,
    ForeignSlot,
    OwnerUnavailable,
}
impl PendingNativeCleanupRefusal {
    pub(crate) fn query_error(self) -> QueryExecutionError {
        use crate::execution_error::{
            ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason,
        };
        use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};
        let (fact, detail) = match self {
            Self::Registry(error) => (error.failure_fact(), error.safe_detail()),
            other => (
                QueryFailureReason::ResultProtocol.failure_fact(),
                SafeDetail::truncating(match other {
                    Self::ForeignSource => "pending Native cleanup has another original Source",
                    Self::NotStaged => "pending Native cleanup has no original staged output",
                    Self::SlotUnavailable => "pending Native cleanup lost its original slot",
                    Self::ForeignSlot => "pending Native cleanup has another original slot",
                    Self::OwnerUnavailable => {
                        "pending Native cleanup has no staged original return"
                    }
                    Self::Registry(_) => unreachable!("registry refusal projected separately"),
                }),
            ),
        };
        QueryExecutionError::from_execution(ExecutionFailureInput::new(
            fact,
            detail,
            ExecutionFailureSite::Coordinator,
        ))
    }
}
#[derive(Clone)]
pub(crate) struct NativeActiveCustodyRegistry {
    slots: Arc<Mutex<BTreeMap<NativeActiveCustodyKey, Arc<ActiveSlot>>>>,
    health: LogicalExecutionHealth,
    progress: Arc<Notify>,
}
impl NativeActiveCustodyRegistry {
    pub(super) fn new(health: LogicalExecutionHealth, progress: Arc<Notify>) -> Self {
        Self {
            slots: Arc::new(Mutex::new(BTreeMap::new())),
            health,
            progress,
        }
    }
    pub(super) fn reserve_with_original_exit(
        &self,
        key: NativeActiveCustodyKey,
        participations: &[ParticipationRef],
        original_exit: Weak<super::original_exit::OriginalLogicalExit>,
    ) -> Option<NativeActivationReservationObligation> {
        let exact = participations.iter().copied().collect::<BTreeSet<_>>();
        if exact.is_empty()
            || exact.len() != participations.len()
            || exact
                .iter()
                .any(|p| p.query_execution_id() != key.execution)
        {
            return None;
        }
        let mut slots = self.slots.lock().unwrap_or_else(|p| p.into_inner());
        if slots.contains_key(&key) {
            return None;
        }
        let slot = Arc::new(ActiveSlot {
            original_exit,
            participations: exact,
            state: Mutex::new(ReturnState::Reserved),
        });
        slots.insert(key, slot.clone());
        Some(NativeActivationReservationObligation {
            custody: Some(NativeActivationReturnCustody {
                storage: self.clone(),
                key,
                slot,
            }),
        })
    }
    pub(super) fn take_staged_for_cleanup(
        &self,
        initial: QueryExecutionId,
        generation: u64,
        original_exit: &Arc<super::original_exit::OriginalLogicalExit>,
        pending: &mut Option<PendingNativeActivationObligation>,
    ) -> Result<
        (
            NativeActivationReturnObligation,
            Option<NativeRowsAttemptRuntime>,
        ),
        PendingNativeCleanupRefusal,
    > {
        let Some(PendingNativeActivationObligation {
            state:
                PendingActivationState::Staged {
                    key,
                    slot,
                    activation,
                    ..
                },
        }) = pending.as_ref()
        else {
            return Err(PendingNativeCleanupRefusal::NotStaged);
        };
        let key = *key;
        let activation = *activation;
        if key.initial != initial
            || key.generation != generation
            || key.execution.query_id() != initial.query_id()
            || activation.execution() != key.execution
        {
            return Err(PendingNativeCleanupRefusal::ForeignSlot);
        }
        let slot = slot
            .upgrade()
            .ok_or(PendingNativeCleanupRefusal::SlotUnavailable)?;
        let slots = self.slots.lock().unwrap_or_else(|p| p.into_inner());
        if !slots
            .get(&key)
            .is_some_and(|original| Arc::ptr_eq(original, &slot))
            || !slot.original_exit.ptr_eq(&Arc::downgrade(original_exit))
        {
            return Err(PendingNativeCleanupRefusal::ForeignSlot);
        }
        let mut state = slot.state.lock().unwrap_or_else(|p| p.into_inner());
        if !matches!(*state, ReturnState::Returned(original) if original == activation) {
            return Err(PendingNativeCleanupRefusal::OwnerUnavailable);
        }
        let Some(PendingNativeActivationObligation {
            state: PendingActivationState::Staged { rows, .. },
        }) = pending.take()
        else {
            unreachable!()
        };
        *state = ReturnState::Borrowed(activation);
        drop(state);
        drop(slots);
        Ok((
            NativeActivationReturnObligation {
                custody: NativeActivationReturnCustody {
                    storage: self.clone(),
                    key,
                    slot,
                },
                activation,
                closed: false,
            },
            rows,
        ))
    }
    fn remove_reserved(&self, custody: &NativeActivationReturnCustody) {
        let removed = {
            let mut slots = self.slots.lock().unwrap_or_else(|p| p.into_inner());
            if !slots
                .get(&custody.key)
                .is_some_and(|s| Arc::ptr_eq(s, &custody.slot))
            {
                return;
            }
            let state = custody.slot.state.lock().unwrap_or_else(|p| p.into_inner());
            if !matches!(*state, ReturnState::Reserved) {
                return;
            }
            drop(state);
            slots.remove(&custody.key)
        };
        if let Some(removed) = removed {
            if let Some(source) = removed.original_exit.upgrade() {
                source.native_return_silent(
                    custody.key,
                    super::LocalExecutionDisposition::ActualSourcesExited,
                );
            }
            drop(removed);
            self.progress.notify_waiters();
        }
    }
    fn complete_silent(
        &self,
        custody: &NativeActivationReturnCustody,
        activation: AttemptActivationIdentity,
    ) {
        let removed = {
            let mut slots = self.slots.lock().unwrap_or_else(|p| p.into_inner());
            assert!(
                slots
                    .get(&custody.key)
                    .is_some_and(|s| Arc::ptr_eq(s, &custody.slot)),
                "the original Native return slot remains registered until actual local closure"
            );
            let mut state = custody.slot.state.lock().unwrap_or_else(|p| p.into_inner());
            assert!(
                matches!(*state, ReturnState::Borrowed(original) if original == activation),
                "only the original borrowed activation return can complete"
            );
            *state = ReturnState::Closed;
            drop(state);
            slots.remove(&custody.key)
        };
        if let Some(source) = custody.slot.original_exit.upgrade() {
            source.native_return_silent(
                custody.key,
                super::LocalExecutionDisposition::OriginalPaidOwnerReturn,
            );
        }
        drop(removed);
    }
    pub(crate) fn blocks_registration(&self, initial: QueryExecutionId, generation: u64) -> bool {
        self.slots
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .range(NativeActiveCustodyKey::registration_lower_bound(initial, generation)..)
            .next()
            .is_some_and(|(key, _)| key.initial == initial && key.generation == generation)
    }
    pub(crate) fn len(&self) -> usize {
        self.slots.lock().unwrap_or_else(|p| p.into_inner()).len()
    }
    #[cfg(test)]
    pub(crate) fn registration_health(
        &self,
        initial: QueryExecutionId,
        generation: u64,
    ) -> Option<QueryExecutionError> {
        self.slots
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .range(NativeActiveCustodyKey::registration_lower_bound(initial, generation)..)
            .take_while(|(key, _)| key.initial == initial && key.generation == generation)
            .find_map(|(_, slot)| {
                let state = slot.state.lock().unwrap_or_else(|p| p.into_inner());
                match &*state {
                    ReturnState::Received(_) | ReturnState::Returned(_) => {
                        Some(QueryExecutionError::new(
                            QueryExecutionErrorKind::Failed,
                            "successful Native activation still awaits its original handoff",
                        ))
                    }
                    ReturnState::Unproven {
                        _failure: failure, ..
                    } => Some(QueryExecutionError::new(
                        QueryExecutionErrorKind::Failed,
                        failure.detail(),
                    )),
                    _ => None,
                }
            })
    }
    pub(crate) fn abandon_for_process_exit(&self) {
        let slots = std::mem::take(&mut *self.slots.lock().unwrap_or_else(|p| p.into_inner()));
        drop(slots);
    }
    pub(super) fn slot_layout() -> std::alloc::Layout {
        novarocks_type_contract::owned_resources::layout::arc_layout(std::alloc::Layout::new::<
            ActiveSlot,
        >())
        .expect("fixed original Native return slot layout")
    }
}
struct NativeActivationReturnCustody {
    storage: NativeActiveCustodyRegistry,
    key: NativeActiveCustodyKey,
    slot: Arc<ActiveSlot>,
}
/// Checked by the original Native body close: a pending output prevents close.
/// This obligation has no Drop exit check and cannot author actual resource exit.
#[must_use = "Adopt or finish the same successful original activation return"]
pub(crate) struct PendingNativeActivationObligation {
    state: PendingActivationState,
}
enum PendingActivationState {
    Unbound {
        original: ActivatedNativeAttempt,
        key: NativeActiveCustodyKey,
        slot: Weak<ActiveSlot>,
    },
    Staged {
        key: NativeActiveCustodyKey,
        slot: Weak<ActiveSlot>,
        activation: AttemptActivationIdentity,
        rows: Option<NativeRowsAttemptRuntime>,
    },
}
impl PendingNativeActivationObligation {
    pub(crate) fn staged_execution(&self) -> Option<QueryExecutionId> {
        match &self.state {
            PendingActivationState::Staged { key, .. } => Some(key.execution),
            _ => None,
        }
    }
    #[cfg(test)]
    pub(crate) fn staged_slot_address(&self) -> Option<usize> {
        match &self.state {
            PendingActivationState::Staged { slot, .. } => {
                slot.upgrade().map(|s| Arc::as_ptr(&s) as usize)
            }
            _ => None,
        }
    }
}
/// Drop removes only an unused Reserved slot. Returned outputs remain in their
/// same canonical slot until the original Native cleanup consumes them.
#[must_use = "Stage the original activation return or release an unused reservation"]
pub(crate) struct NativeActivationReservationObligation {
    custody: Option<NativeActivationReturnCustody>,
}
impl NativeActivationReservationObligation {
    /// Receive into the original stable body and canonical slot BEFORE the
    /// borrowed Future can be destroyed. No identity refusal can turn a
    /// received output back into an unused reservation.
    pub(crate) fn receive_activated_in(
        &self,
        source: &mut Option<PendingNativeActivationObligation>,
        original: ActivatedNativeAttempt,
    ) {
        assert!(source.is_none(), "one original activation output");
        let custody = self
            .custody
            .as_ref()
            .expect("same original activation reservation");
        let activation = original.activation();
        *source = Some(PendingNativeActivationObligation {
            state: PendingActivationState::Unbound {
                original,
                key: custody.key,
                slot: Arc::downgrade(&custody.slot),
            },
        });
        let mut state = custody.slot.state.lock().unwrap_or_else(|p| p.into_inner());
        assert!(
            matches!(*state, ReturnState::Reserved),
            "receive into the original unused slot"
        );
        *state = ReturnState::Received(activation);
    }
    pub(crate) fn stage_activated_in(
        &self,
        expected: AttemptActivationIdentity,
        expected_root: TaskIdentity,
        source: &mut Option<PendingNativeActivationObligation>,
    ) -> Result<(), QueryExecutionError> {
        let custody = self
            .custody
            .as_ref()
            .ok_or_else(|| protocol("original activation reservation is absent"))?;
        let Some(PendingNativeActivationObligation {
            state:
                PendingActivationState::Unbound {
                    original,
                    key,
                    slot,
                },
        }) = source.as_ref()
        else {
            return Err(protocol(
                "original successful activation source is absent or already staged",
            ));
        };
        if *key != custody.key || !slot.ptr_eq(&Arc::downgrade(&custody.slot)) {
            return Err(protocol(
                "original activation output belongs to another exact slot",
            ));
        }
        if original.activation() != expected
            || expected.execution() != custody.key.execution
            || expected_root.query_execution_id() != expected.execution()
        {
            return Err(protocol(
                "returned activation differs from the original action",
            ));
        }
        if let Some(rows) = original.rows()
            && (rows.binding.frontier.root() != expected_root
                || !custody.slot.participations.iter().any(|p| {
                    p.backend_process_id() == rows.binding.frontier.root().backend_process_id()
                }))
        {
            return Err(protocol(
                "returned root binding differs from its original roster",
            ));
        }
        let mut state = custody.slot.state.lock().unwrap_or_else(|p| p.into_inner());
        if !matches!(*state, ReturnState::Received(activation) if activation == original.activation())
        {
            return Err(protocol(
                "original activation slot has no same received output",
            ));
        }
        let Some(PendingNativeActivationObligation {
            state: PendingActivationState::Unbound { original, .. },
        }) = source.take()
        else {
            unreachable!()
        };
        let (activation, rows) = original.into_parts();
        *state = ReturnState::Returned(activation);
        *source = Some(PendingNativeActivationObligation {
            state: PendingActivationState::Staged {
                key: custody.key,
                slot: Arc::downgrade(&custody.slot),
                activation,
                rows,
            },
        });
        drop(state);
        Ok(())
    }
    pub(crate) fn finish_activated_in(
        &mut self,
        source: &mut Option<PendingNativeActivationObligation>,
    ) -> Result<
        (
            NativeActivationReturnObligation,
            Option<NativeRowsAttemptRuntime>,
        ),
        QueryExecutionError,
    > {
        let custody = self
            .custody
            .as_ref()
            .ok_or_else(|| protocol("original activation reservation is absent"))?;
        let Some(PendingNativeActivationObligation {
            state:
                PendingActivationState::Staged {
                    key,
                    slot,
                    activation,
                    ..
                },
        }) = source.as_ref()
        else {
            return Err(protocol("original activation output is not staged"));
        };
        let activation = *activation;
        if *key != custody.key || !slot.ptr_eq(&Arc::downgrade(&custody.slot)) {
            return Err(protocol(
                "original activation output belongs to another exact slot",
            ));
        }
        let mut state = custody.slot.state.lock().unwrap_or_else(|p| p.into_inner());
        if !matches!(*state, ReturnState::Returned(original) if original == activation) {
            return Err(protocol("original activation slot has no staged return"));
        }
        let Some(PendingNativeActivationObligation {
            state: PendingActivationState::Staged { rows, .. },
        }) = source.take()
        else {
            unreachable!()
        };
        *state = ReturnState::Borrowed(activation);
        drop(state);
        Ok((
            NativeActivationReturnObligation {
                custody: self.custody.take().expect("validated original reservation"),
                activation,
                closed: false,
            },
            rows,
        ))
    }
}
impl Drop for NativeActivationReservationObligation {
    fn drop(&mut self) {
        if let Some(custody) = self.custody.take() {
            custody.storage.remove_reserved(&custody);
        }
    }
}
/// Drop checks that actual installed resource closure completed the original
/// return. An unfinished return remains in the same slot and process health;
/// it never reports Native, AP or remote closure from passive data alone.
#[must_use = "Complete the original return only after installed resources actually exit"]
pub(crate) struct NativeActivationReturnObligation {
    custody: NativeActivationReturnCustody,
    activation: AttemptActivationIdentity,
    closed: bool,
}
impl fmt::Debug for NativeActivationReturnObligation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeActivationReturnObligation")
            .field("activation", &self.activation)
            .field("closed", &self.closed)
            .finish_non_exhaustive()
    }
}
impl NativeActivationReturnObligation {
    #[cfg(test)]
    pub(crate) fn slot_address(&self) -> usize {
        Arc::as_ptr(&self.custody.slot) as usize
    }
    #[must_use = "Notify original registry progress after caller metadata is stable"]
    pub(crate) fn retire_local(&mut self) -> Arc<Notify> {
        assert!(!self.closed, "the original Native return retires once");
        self.custody
            .storage
            .complete_silent(&self.custody, self.activation);
        self.closed = true;
        // The caller records its actual-exit observation before any wake.
        self.custody.storage.progress.clone()
    }
}
#[derive(Debug)]
struct UnclosedNativeActivationReturn;
impl fmt::Display for UnclosedNativeActivationReturn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("original Native activation return has no proven local closure")
    }
}
impl std::error::Error for UnclosedNativeActivationReturn {}
impl Drop for NativeActivationReturnObligation {
    fn drop(&mut self) {
        if self.closed {
            return;
        }
        let failure = StatementNativeHealthFailure::actual_exit_unproven(
            "Native activation return left supervision before actual local closure",
            Arc::new(UnclosedNativeActivationReturn),
        );
        {
            let mut state = self
                .custody
                .slot
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            assert!(
                matches!(*state, ReturnState::Borrowed(original) if original == self.activation),
                "the same original Native return remains borrowed"
            );
            *state = ReturnState::Unproven {
                _activation: self.activation,
                _failure: failure.clone(),
            };
        }
        self.custody.storage.health.record_unproven_exit(failure);
    }
}
fn protocol(detail: &'static str) -> QueryExecutionError {
    QueryExecutionError::new(QueryExecutionErrorKind::Failed, detail)
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_types::{BackendProcessId, FrontendProcessId, StageId, TaskId};

    fn execution(query: QueryId) -> QueryExecutionId {
        QueryExecutionId::new(query, AttemptId::new(1).unwrap()).unwrap()
    }
    fn fixture() -> (
        NativeActiveCustodyRegistry,
        Arc<super::super::original_exit::OriginalLogicalExit>,
        NativeActiveCustodyKey,
        ParticipationRef,
        AttemptActivationIdentity,
        TaskIdentity,
    ) {
        let initial = execution(QueryId::new(71, 72));
        let key = NativeActiveCustodyKey {
            initial,
            generation: 7,
            execution: initial,
        };
        let participation = ParticipationRef::new(
            initial,
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        );
        let activation = AttemptActivationIdentity::from_parts(19, initial, 23);
        let root = TaskIdentity::new(
            initial,
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            participation.backend_process_id(),
        );
        let table = NativeActiveCustodyRegistry::new(
            LogicalExecutionHealth::new(Arc::new(Notify::new())),
            Arc::new(Notify::new()),
        );
        (
            table,
            super::super::original_exit::OriginalLogicalExit::new(),
            key,
            participation,
            activation,
            root,
        )
    }

    #[test]
    fn received_completion_with_wrong_full_activation_never_becomes_an_unused_reservation() {
        let (table, source, key, participation, expected, root) = fixture();
        let reservation = table
            .reserve_with_original_exit(key, &[participation], Arc::downgrade(&source))
            .unwrap();
        let returned =
            AttemptActivationIdentity::from_parts(expected.actor().get(), expected.execution(), 24);
        let mut pending = None;
        reservation
            .receive_activated_in(&mut pending, ActivatedNativeAttempt::completion(returned));
        assert!(
            reservation
                .stage_activated_in(expected, root, &mut pending)
                .is_err()
        );
        assert!(
            matches!(pending.as_ref().unwrap().state, PendingActivationState::Unbound { ref original, .. } if original.activation() == returned)
        );
        drop(reservation);
        assert_eq!(table.len(), 1);
        assert!(table.blocks_registration(key.initial, key.generation));
        let slots = table.slots.lock().unwrap();
        assert!(
            matches!(*slots[&key].state.lock().unwrap(), ReturnState::Received(actual) if actual == returned)
        );
        drop(slots);
        // A task-free table fixture has no installed Source to close. Only
        // explicit process-exit disposal releases this unresolved test state.
        table.abandon_for_process_exit();
        drop(pending);
    }

    #[test]
    fn staged_completion_adopts_same_slot_and_keeps_unproven_return_without_positive_exit() {
        let (table, source, key, participation, activation, root) = fixture();
        let reservation = table
            .reserve_with_original_exit(key, &[participation], Arc::downgrade(&source))
            .unwrap();
        let mut pending = None;
        reservation
            .receive_activated_in(&mut pending, ActivatedNativeAttempt::completion(activation));
        reservation
            .stage_activated_in(activation, root, &mut pending)
            .unwrap();
        let address = pending.as_ref().unwrap().staged_slot_address().unwrap();
        assert_ne!(activation.generation(), key.generation);
        drop(reservation);
        assert!(matches!(
            table.take_staged_for_cleanup(
                key.initial,
                activation.generation(),
                &source,
                &mut pending
            ),
            Err(PendingNativeCleanupRefusal::ForeignSlot)
        ));
        let foreign = super::super::original_exit::OriginalLogicalExit::new();
        assert!(matches!(
            table.take_staged_for_cleanup(key.initial, key.generation, &foreign, &mut pending),
            Err(PendingNativeCleanupRefusal::ForeignSlot)
        ));
        assert_eq!(
            pending.as_ref().unwrap().staged_slot_address(),
            Some(address)
        );
        let (original, rows) = table
            .take_staged_for_cleanup(key.initial, key.generation, &source, &mut pending)
            .unwrap();
        assert!(rows.is_none());
        assert_eq!(original.activation, activation);
        assert_eq!(Arc::as_ptr(&original.custody.slot) as usize, address);
        assert!(pending.is_none());
        assert!(matches!(
            table.take_staged_for_cleanup(key.initial, key.generation, &source, &mut pending),
            Err(PendingNativeCleanupRefusal::NotStaged)
        ));
        // This fixture proves no installed Native/AP resource exit. Dropping
        // the checked return must keep its exact slot and original failure.
        drop(original);
        assert_eq!(table.len(), 1);
        assert!(table.blocks_registration(key.initial, key.generation));
        assert!(table.health.health().is_some());
        let slots = table.slots.lock().unwrap();
        let state = slots[&key].state.lock().unwrap();
        assert!(
            matches!(&*state, ReturnState::Unproven { _activation: actual, _failure: failure } if *actual == activation && failure.detail() == "Native activation return left supervision before actual local closure")
        );
        drop(state);
        drop(slots);
        table.abandon_for_process_exit();
        drop(table.health.take_for_process_exit());
    }

    #[test]
    fn health_search_stays_in_exact_prefix_and_keeps_first_held_source_order() {
        let (registry, _, _, original_participation, _, _) = fixture();
        let original_execution = |attempt| {
            QueryExecutionId::new(QueryId::new(91, 71), AttemptId::new(attempt).unwrap()).unwrap()
        };
        let initial = original_execution(3);
        let key = |generation, attempt| NativeActiveCustodyKey {
            initial,
            generation,
            execution: original_execution(attempt),
        };
        let before = key(6, 3);
        let unheld = key(7, 3);
        let first_held = key(7, 4);
        let later_held = key(7, 5);
        let after = key(8, 3);
        let cause: Arc<dyn std::error::Error + Send + Sync> =
            Arc::new(UnclosedNativeActivationReturn);
        let mut reservations = Vec::new();
        for (key, detail) in [
            (before, Some("foreign earlier prefix health")),
            (unheld, None),
            (first_held, Some("first exact prefix health")),
            (later_held, Some("later exact prefix health")),
            (after, Some("foreign later prefix health")),
        ] {
            let participation = ParticipationRef::new(
                key.execution,
                original_participation.frontend_process_id(),
                original_participation.backend_process_id(),
            );
            reservations.push(
                registry
                    .reserve_with_original_exit(key, &[participation], Weak::new())
                    .unwrap(),
            );
            if let Some(detail) = detail {
                // Passive private-table input, at the original metadata tier.
                // No Native future, physical exit or health publication is forged.
                let slots = registry.slots.lock().unwrap();
                *slots[&key].state.lock().unwrap() = ReturnState::Unproven {
                    _activation: AttemptActivationIdentity::from_parts(19, key.execution, 23),
                    _failure: StatementNativeHealthFailure::actual_exit_unproven(
                        detail,
                        Arc::clone(&cause),
                    ),
                };
            }
        }
        assert!(
            registry.registration_health(initial, 5).is_none(),
            "the next prefix cannot supply missing-prefix health"
        );
        assert!(
            registry
                .registration_health(original_execution(2), 7)
                .is_none(),
            "the same QueryId with another initial attempt is a different registration"
        );
        assert_eq!(
            registry.registration_health(initial, 7).unwrap().message(),
            "first exact prefix health"
        );
        let retained = registry
            .slots
            .lock()
            .unwrap()
            .get(&first_held)
            .cloned()
            .unwrap();
        let retained_cause = {
            let state = retained.state.lock().unwrap();
            let ReturnState::Unproven {
                _failure: original, ..
            } = &*state
            else {
                panic!("the exact passive slot keeps its original failure");
            };
            original.clone().into_parts().1
        };
        assert!(
            Arc::ptr_eq(&retained_cause, &cause),
            "read-only health lookup retains the original typed source Arc"
        );
        // Remove only this private test's passive metadata entries. This is not
        // Native resource closure, a Source exit, or checked-return completion.
        assert!(registry.slots.lock().unwrap().remove(&first_held).is_some());
        assert_eq!(
            registry.registration_health(initial, 7).unwrap().message(),
            "later exact prefix health"
        );
        assert!(registry.slots.lock().unwrap().remove(&later_held).is_some());
        assert!(
            registry.blocks_registration(initial, 7),
            "the same unheld reservation remains"
        );
        assert!(
            registry.registration_health(initial, 7).is_none(),
            "an unheld own slot cannot leak the next prefix's health"
        );
        registry.slots.lock().unwrap().clear();
        drop(reservations);
    }

    #[test]
    fn registration_prefix_keeps_signed_extremes_and_generation_boundaries() {
        let (table, _, key, _, _, _) = fixture();
        let mut reservations = Vec::new();
        for query in [
            QueryId::new(i64::MIN, i64::MIN),
            QueryId::new(i64::MAX, i64::MAX),
        ] {
            let exact = execution(query);
            let participation = ParticipationRef::new(
                exact,
                FrontendProcessId::new_v7(),
                BackendProcessId::new_v7(),
            );
            reservations.push(
                table
                    .reserve_with_original_exit(
                        NativeActiveCustodyKey {
                            execution: exact,
                            ..key
                        },
                        &[participation],
                        Weak::new(),
                    )
                    .unwrap(),
            );
        }
        assert!(table.blocks_registration(key.initial, key.generation));
        assert!(!table.blocks_registration(key.initial, key.generation - 1));
        assert!(!table.blocks_registration(key.initial, key.generation + 1));
        assert!(!table.blocks_registration(execution(QueryId::new(70, 72)), key.generation));
        drop(reservations.pop());
        assert!(table.blocks_registration(key.initial, key.generation));
        drop(reservations);
        assert!(!table.blocks_registration(key.initial, key.generation));
        assert_eq!(table.len(), 0);
    }
}
