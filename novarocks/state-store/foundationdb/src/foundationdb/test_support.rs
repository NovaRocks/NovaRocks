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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use tokio::sync::Notify;

use novarocks_state_store_api::{CommitOutcome, StateStoreError, StateStoreErrorKind};

static NEXT_COMMIT_GATES: OnceLock<Mutex<Option<Arc<GateState>>>> = OnceLock::new();

struct GateState {
    block_pre_native: bool,
    block_response: bool,
    lose_response: bool,
    pre_native_reached: AtomicBool,
    pre_native_released: AtomicBool,
    response_reached: AtomicBool,
    response_released: AtomicBool,
    waiter_dropped: AtomicBool,
    pre_native_notify: Notify,
    response_notify: Notify,
    waiter_notify: Notify,
}

/// Test-only control for the two native commit boundaries that the binding exposes.
#[doc(hidden)]
#[derive(Clone)]
pub struct FoundationDbCommitGateControl {
    state: Arc<GateState>,
}

pub(super) struct CommitGates {
    state: Arc<GateState>,
}

pub(super) struct CommitWaiterDropGuard {
    state: Option<Arc<GateState>>,
}

/// Arms the next FoundationDB commit supervisor in this process.
#[doc(hidden)]
pub fn arm_next_foundationdb_commit(
    block_pre_native: bool,
    block_response: bool,
    lose_response: bool,
) -> Result<FoundationDbCommitGateControl, StateStoreError> {
    let state = Arc::new(GateState {
        block_pre_native,
        block_response,
        lose_response,
        pre_native_reached: AtomicBool::new(false),
        pre_native_released: AtomicBool::new(!block_pre_native),
        response_reached: AtomicBool::new(false),
        response_released: AtomicBool::new(!block_response),
        waiter_dropped: AtomicBool::new(false),
        pre_native_notify: Notify::new(),
        response_notify: Notify::new(),
        waiter_notify: Notify::new(),
    });
    let mut slot = gate_slot().lock().map_err(|_| hook_error())?;
    if slot.is_some() {
        return Err(StateStoreError::new(
            StateStoreErrorKind::InvalidRequest,
            "a FoundationDB commit test gate is already armed",
        ));
    }
    *slot = Some(Arc::clone(&state));
    Ok(FoundationDbCommitGateControl { state })
}

pub(super) fn arm_commit_waiter_drop_guard() -> Option<CommitWaiterDropGuard> {
    let state = match gate_slot().lock() {
        Ok(slot) => slot.as_ref().map(Arc::clone),
        Err(poisoned) => poisoned.into_inner().as_ref().map(Arc::clone),
    }?;
    Some(CommitWaiterDropGuard { state: Some(state) })
}

pub(super) fn take_commit_gates() -> Option<CommitGates> {
    let state = match gate_slot().lock() {
        Ok(mut slot) => slot.take(),
        Err(poisoned) => poisoned.into_inner().take(),
    }?;
    Some(CommitGates { state })
}

impl CommitGates {
    pub async fn before_native_commit(&self) {
        self.state.pre_native_reached.store(true, Ordering::Release);
        self.state.pre_native_notify.notify_waiters();
        if self.state.block_pre_native {
            wait_flag(
                &self.state.pre_native_released,
                &self.state.pre_native_notify,
            )
            .await;
        }
    }

    pub async fn before_response(&self, outcome: CommitOutcome) -> CommitOutcome {
        self.state.response_reached.store(true, Ordering::Release);
        self.state.response_notify.notify_waiters();
        if self.state.block_response {
            wait_flag(&self.state.response_released, &self.state.response_notify).await;
        }
        if self.state.lose_response && matches!(outcome, CommitOutcome::Committed(_)) {
            CommitOutcome::CommitUnknown(StateStoreError::new(
                StateStoreErrorKind::Transient,
                "FoundationDB committed response was lost by a test gate",
            ))
        } else {
            outcome
        }
    }
}

impl FoundationDbCommitGateControl {
    pub async fn wait_pre_native(&self) {
        wait_flag(
            &self.state.pre_native_reached,
            &self.state.pre_native_notify,
        )
        .await;
    }

    pub fn release_pre_native(&self) {
        self.state
            .pre_native_released
            .store(true, Ordering::Release);
        self.state.pre_native_notify.notify_waiters();
    }

    pub async fn wait_response(&self) {
        wait_flag(&self.state.response_reached, &self.state.response_notify).await;
    }

    pub fn release_response(&self) {
        self.state.response_released.store(true, Ordering::Release);
        self.state.response_notify.notify_waiters();
    }

    pub async fn wait_waiter_dropped(&self) {
        wait_flag(&self.state.waiter_dropped, &self.state.waiter_notify).await;
    }
}

impl CommitWaiterDropGuard {
    pub fn complete(mut self) {
        self.state.take();
    }
}

impl Drop for CommitWaiterDropGuard {
    fn drop(&mut self) {
        let Some(state) = self.state.take() else {
            return;
        };
        state.waiter_dropped.store(true, Ordering::Release);
        state.waiter_notify.notify_waiters();
    }
}

async fn wait_flag(flag: &AtomicBool, notify: &Notify) {
    loop {
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if flag.load(Ordering::Acquire) {
            return;
        }
        notified.await;
    }
}

fn gate_slot() -> &'static Mutex<Option<Arc<GateState>>> {
    NEXT_COMMIT_GATES.get_or_init(|| Mutex::new(None))
}

fn hook_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::Internal,
        "FoundationDB commit test gate registry is poisoned",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn test_gate_lock() -> tokio::sync::OwnedMutexGuard<()> {
        static LOCK: OnceLock<Arc<tokio::sync::Mutex<()>>> = OnceLock::new();
        Arc::clone(LOCK.get_or_init(|| Arc::new(tokio::sync::Mutex::new(()))))
            .lock_owned()
            .await
    }

    #[tokio::test]
    async fn gates_expose_pre_native_and_response_as_distinct_boundaries() {
        let _guard = test_gate_lock().await;
        let control = arm_next_foundationdb_commit(true, true, true).expect("arm gates");
        let gates = take_commit_gates().expect("take gates");
        let owner = tokio::spawn(async move {
            gates.before_native_commit().await;
            gates
                .before_response(CommitOutcome::Committed(
                    novarocks_state_store_api::CommitReceipt {
                        attempt: crate::codec::tests_support::attempt_id(1),
                        revision: novarocks_state_store_api::StoreRevision::try_from(
                            bytes::Bytes::from_static(&[0x22; 10]),
                        )
                        .expect("revision"),
                    },
                ))
                .await
        });
        control.wait_pre_native().await;
        assert!(!control.state.response_reached.load(Ordering::Acquire));
        control.release_pre_native();
        control.wait_response().await;
        control.release_response();
        assert!(matches!(
            owner.await.expect("owner"),
            CommitOutcome::CommitUnknown(_)
        ));
    }

    #[tokio::test]
    async fn gate_waits_observe_signals_emitted_before_registration() {
        let _guard = test_gate_lock().await;
        let control = arm_next_foundationdb_commit(false, false, false).expect("arm gates");
        let gates = take_commit_gates().expect("take gates");
        gates.before_native_commit().await;
        let outcome = gates
            .before_response(CommitOutcome::DefiniteFailure(StateStoreError::new(
                StateStoreErrorKind::InvalidRequest,
                "test",
            )))
            .await;
        assert!(matches!(outcome, CommitOutcome::DefiniteFailure(_)));
        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            control.wait_pre_native(),
        )
        .await
        .expect("pre-native signal must not be lost");
        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            control.wait_response(),
        )
        .await
        .expect("response signal must not be lost");
    }

    #[tokio::test]
    async fn caller_waiter_drop_is_observable_without_dropping_the_provider_owner() {
        let _guard = test_gate_lock().await;
        let control = arm_next_foundationdb_commit(true, false, false).expect("arm gates");
        let waiter_guard = arm_commit_waiter_drop_guard().expect("register waiter drop guard");
        let gates = take_commit_gates().expect("take provider gates");
        let owner = tokio::spawn(async move {
            gates.before_native_commit().await;
            gates
                .before_response(CommitOutcome::DefiniteFailure(StateStoreError::new(
                    StateStoreErrorKind::InvalidRequest,
                    "owner continued after caller cancellation",
                )))
                .await
        });
        control.wait_pre_native().await;

        drop(waiter_guard);
        control.wait_waiter_dropped().await;
        assert!(
            !owner.is_finished(),
            "provider owner must remain blocked and owned after caller waiter drop"
        );

        control.release_pre_native();
        assert!(matches!(
            owner.await.expect("provider owner remains alive"),
            CommitOutcome::DefiniteFailure(_)
        ));
    }
}
