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

//! Provider-private, purpose-closed originals. Physical fields remain in the
//! selected role/Action's receiving record. This module never opens an executor.

mod evidence;
mod open;
#[cfg(test)]
mod tests;
mod transaction;

use novarocks_failure_contract::{FailureClass, FailureProjection};
use novarocks_state_store_api::{
    StateStoreBlockingExit as Exit, StateStoreBlockingOriginal as Original,
    StateStoreBlockingPhase as Phase, StateStoreBlockingRequest as Request,
    StateStoreBlockingSlot as Slot, StateStoreBlockingSourceVocabulary as Vocabulary,
    StateStoreError, StateStoreErrorKind, StateStoreExecutionContext as Execution,
    StateStoreExecutionFailure as Failure,
};
use std::{
    sync::{Arc, Mutex, MutexGuard},
    task::{Context, Poll},
};

pub(super) use evidence::{adjudicate_evidence_in, release_evidence_in};
pub(super) use open::{
    OpenOriginal, borrow_instance_close_in, prepare_instance_close_in, prepare_open_in,
};
pub(super) use transaction::{begin_read_in, begin_write_in};

pub(super) fn vocabulary() -> Vocabulary {
    // Transaction settlement stays with its original; standalone evidence
    // has a distinct private source and cannot start a transaction.
    Vocabulary::for_provider::<
        open::OpenOriginal,
        transaction::TxnOriginal,
        evidence::EvidenceOriginal,
    >(super::SQLITE_STATE_STORE_PROVIDER_ID)
}
fn error(kind: StateStoreErrorKind, detail: &'static str) -> Failure {
    StateStoreError::new(kind, detail).into_original_failure()
}
fn invalid(detail: &'static str) -> Failure {
    error(StateStoreErrorKind::Internal, detail)
}
fn lock<T>(value: &Mutex<T>) -> MutexGuard<'_, T> {
    value.lock().unwrap_or_else(|poison| poison.into_inner())
}

#[derive(Default)]
struct Header {
    revision: u64,
    request: Option<Request>,
    entered: bool,
    advancing: bool,
    before_publish: bool,
    result_received: bool,
    physical_failure: Option<Failure>,
    first_physical_failure: Option<Failure>,
    // Business physical health must survive successful settlement separately
    // from the first ordinary cause, without refusing that same settlement.
    deferred_health: Option<Failure>,
    health: Option<Failure>,
    complete: bool,
}
struct Core<B> {
    header: Mutex<Header>,
    body: Mutex<Option<B>>,
    slot: Mutex<Option<Box<dyn Slot>>>,
    execution: Execution,
}
impl<B> Core<B> {
    fn new(body: B, execution: Execution) -> Self {
        Self {
            header: Mutex::new(Header::default()),
            body: Mutex::new(Some(body)),
            slot: Mutex::new(None),
            execution,
        }
    }
    fn bind(&self, original: &Arc<dyn Original>) -> Result<(), Failure> {
        let mut receiving = None;
        self.execution.bind_original_in(original, &mut receiving)?;
        // No physical polling precedes stable borrower installation. The port
        // has already installed its separate strong original receiving record.
        *lock(&self.slot) = receiving;
        Ok(())
    }
    fn request(&self) -> Option<Request> {
        lock(&self.header).request
    }
    fn queue_in(h: &mut Header, phase: Phase) -> Result<(Request, Option<Failure>), Failure> {
        let revision = h
            .revision
            .checked_add(1)
            .and_then(std::num::NonZeroU64::new)
            .ok_or_else(|| invalid("SQLite original request revision is exhausted"))?;
        let request = Request { phase, revision };
        h.revision = revision.get();
        h.request = Some(request);
        h.entered = false;
        h.advancing = false;
        h.before_publish = false;
        h.result_received = false;
        let retired = h.physical_failure.take();
        Ok((request, retired))
    }
    fn queue(&self, phase: Phase) -> Result<Request, Failure> {
        let mut h = lock(&self.header);
        if h.complete || h.request.is_some() || h.health.is_some() {
            return Err(invalid("SQLite original is not idle for another phase"));
        }
        let (request, retired) = Self::queue_in(&mut h, phase)?;
        drop(h);
        drop(retired);
        Ok(request)
    }
    fn queue_if_idle(&self, phase: Phase) -> Result<(), Failure> {
        let mut h = lock(&self.header);
        // Concurrent close observations are idempotent. Only the first one
        // owns the next phase; none can poison an already accepted phase.
        if h.complete || h.request.is_some() || h.advancing || h.health.is_some() {
            return Ok(());
        }
        let (_, retired) = Self::queue_in(&mut h, phase)?;
        drop(h);
        drop(retired);
        Ok(())
    }
    fn enter(&self, request: Request) -> bool {
        let mut h = lock(&self.header);
        if h.request != Some(request) || !h.before_publish || h.entered || h.health.is_some() {
            h.health.get_or_insert_with(|| {
                invalid("SQLite worker does not match its original request")
            });
            return false;
        }
        h.entered = true;
        true
    }
    fn prepare(&self, request: Request) -> Result<(), Failure> {
        let mut h = lock(&self.header);
        if h.request != Some(request) || h.before_publish || h.entered {
            return Err(invalid(
                "SQLite publication does not match its original request",
            ));
        }
        h.before_publish = true;
        Ok(())
    }
    fn result(&self, request: Request, result: Result<(), Failure>) {
        let mut h = lock(&self.header);
        if h.request != Some(request) || h.result_received {
            h.health.get_or_insert_with(|| {
                invalid("SQLite result does not match its original request")
            });
            return;
        }
        h.result_received = true;
        if let Err(failure) = result {
            h.first_physical_failure
                .get_or_insert_with(|| failure.clone());
            if matches!(
                failure.failure_fact().class(),
                FailureClass::Unexpected | FailureClass::Invariant
            ) {
                h.deferred_health.get_or_insert_with(|| failure.clone());
                // A physical failure in settlement cannot grant a connection
                // close or an evidence receipt, even if business failed first.
                if matches!(
                    request.phase,
                    Phase::Close | Phase::Rollback | Phase::EvidenceRead | Phase::EvidenceRelease
                ) {
                    h.health.get_or_insert_with(|| failure.clone());
                }
            }
            let retired = h.physical_failure.replace(failure);
            drop(h);
            drop(retired);
        }
    }
    fn exit(&self, request: Request, exit: Result<Exit, Failure>) -> Option<Exit> {
        let mut h = lock(&self.header);
        if h.request != Some(request) || !h.result_received {
            h.health
                .get_or_insert_with(|| invalid("SQLite exit does not match its original result"));
            return None;
        }
        match exit {
            Err(failure) => {
                let rejected = if h.health.is_none() {
                    h.health = Some(failure);
                    None
                } else {
                    Some(failure)
                };
                drop(h);
                drop(rejected);
                None
            }
            Ok(Exit::NeverPublishedReturned) if h.entered => {
                h.health
                    .get_or_insert_with(|| invalid("SQLite started call cannot be unpublished"));
                None
            }
            Ok(exit) => {
                h.request = None;
                h.advancing = true;
                Some(exit)
            }
        }
    }
    fn health(&self) -> Option<Failure> {
        lock(&self.header).health.clone()
    }
    fn local_failure(&self) -> Option<Failure> {
        let h = lock(&self.header);
        h.health.clone().or_else(|| {
            if h.complete {
                h.deferred_health.clone()
            } else {
                None
            }
        })
    }
    fn physical_failure(&self) -> Option<Failure> {
        lock(&self.header).physical_failure.clone()
    }
    fn fail(&self, failure: Failure) {
        let rejected = {
            let mut h = lock(&self.header);
            if h.health.is_none() {
                h.health = Some(failure);
                None
            } else {
                Some(failure)
            }
        };
        drop(rejected);
    }
    fn complete(&self) {
        let mut h = lock(&self.header);
        h.complete = true;
        h.advancing = false;
    }
    fn idle_silent(&self) {
        lock(&self.header).advancing = false;
    }
    fn idle(&self) {
        self.idle_silent();
        self.notify();
    }
    fn is_idle(&self) -> bool {
        let h = lock(&self.header);
        h.request.is_none() && !h.advancing
    }

    fn is_complete(&self) -> bool {
        let h = lock(&self.header);
        h.complete && h.request.is_none() && h.health.is_none()
    }
    fn notify(&self) {
        if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.execution.request_progress();
        })) {
            self.fail(failure(NotificationPanic {
                _payload: Mutex::new(payload),
            }));
        }
    }
    fn poll_step(&self, cx: &mut Context<'_>) -> Poll<Result<(), Failure>> {
        if let Some(failure) = self.health() {
            return Poll::Ready(Err(failure));
        }
        let Some(request) = self.request() else {
            return Poll::Ready(Ok(()));
        };
        let mut slot = SlotLoan::take(&self.slot)?;
        // Foreign poll is outside the original/receiving mutex. Port callbacks
        // update the exact source before returning its phase/exit observation.
        let phase = std::task::ready!(slot.value.as_mut().unwrap().poll_phase(cx, request));
        let exit = slot.value.as_mut().unwrap().poll_exit(cx, request);
        match (phase, exit) {
            (Err(failure), _) => Poll::Ready(Err(failure)),
            (_, Poll::Pending) => Poll::Pending,
            (_, Poll::Ready(Err(failure))) => Poll::Ready(Err(failure)),
            (_, Poll::Ready(Ok(_))) => Poll::Ready(Ok(())),
        }
    }
}
struct BodyLoan<'a, B> {
    owner: &'a Mutex<Option<B>>,
    value: Option<B>,
}
impl<'a, B> BodyLoan<'a, B> {
    fn take(owner: &'a Mutex<Option<B>>) -> Result<Self, Failure> {
        let value = lock(owner)
            .take()
            .ok_or_else(|| invalid("SQLite original body is already borrowed"))?;
        Ok(Self {
            owner,
            value: Some(value),
        })
    }
}
impl<B> Drop for BodyLoan<'_, B> {
    fn drop(&mut self) {
        // Restore before any worker unwind reaches its physical receiver.
        *lock(self.owner) = self.value.take();
    }
}
struct SlotLoan<'a> {
    owner: &'a Mutex<Option<Box<dyn Slot>>>,
    value: Option<Box<dyn Slot>>,
}
impl<'a> SlotLoan<'a> {
    fn take(owner: &'a Mutex<Option<Box<dyn Slot>>>) -> Result<Self, Failure> {
        let value = lock(owner)
            .take()
            .ok_or_else(|| invalid("SQLite original receiving borrower is unavailable"))?;
        Ok(Self {
            owner,
            value: Some(value),
        })
    }
}
impl Drop for SlotLoan<'_> {
    fn drop(&mut self) {
        *lock(self.owner) = self.value.take();
    }
}
struct NotificationPanic {
    _payload: Mutex<Box<dyn std::any::Any + Send>>,
}
impl std::fmt::Debug for NotificationPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SQLite notification panic")
    }
}
impl std::fmt::Display for NotificationPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SQLite original progress notification failed")
    }
}
impl std::error::Error for NotificationPanic {}

fn failure<
    E: std::error::Error + novarocks_failure_contract::FailureProjection + Send + Sync + 'static,
>(
    error: E,
) -> Failure {
    Failure::from_static_detail(error, "SQLite original operation failed")
}
impl novarocks_failure_contract::FailureProjection for NotificationPanic {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        FailureFact::new(
            FailureClass::Unexpected,
            FailureReason::new(domain::STATE_STORE, std::num::NonZeroU16::new(21).unwrap()),
        )
    }
}
