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

//! One original Task read's payload pin and authority return position.
//! The shared cell grants no Work, reply, read admission or physical exit.

use super::{dynamic_filters::TaskDynamicFilterLoan, request_endpoint::RequestNotice};
use novarocks_control_runtime::exit_custody::ExitObligation;
use novarocks_execution_contract::{DomainVersion, task_execution::domain::CodecOwnedContent};
use novarocks_worker_protocol::creation::PayloadId;
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TaskReadPayloadError {
    WrongPhase,
    Unresolved,
}

enum State {
    Empty,
    Ready(TaskDynamicFilterLoan),
    Lent,
    // Keep this phase after the unique token is taken; the same R cannot
    // install another payload by returning to an apparently unused cell.
    Returned(Option<ExitObligation>),
    Unresolved,
}

pub(crate) struct TaskReadPayloadShared {
    state: Mutex<State>,
    notice: RequestNotice,
}
impl TaskReadPayloadShared {
    pub(super) fn new(notice: RequestNotice) -> Self {
        Self {
            state: Mutex::new(State::Empty),
            notice,
        }
    }

    /// The original custodian moves the actual pin here before releasing the
    /// PrepareReadPayload effect. Refusal returns that same complete loan.
    pub(super) fn install_ready(
        &self,
        loan: TaskDynamicFilterLoan,
    ) -> Result<(), (TaskReadPayloadError, TaskDynamicFilterLoan)> {
        let mut state = self.state.lock().expect("original Task read payload");
        match &*state {
            State::Empty => {
                *state = State::Ready(loan);
                Ok(())
            }
            State::Unresolved => Err((TaskReadPayloadError::Unresolved, loan)),
            _ => Err((TaskReadPayloadError::WrongPhase, loan)),
        }
    }

    /// Only the real reply taker moves the pin to its unique delivery. The
    /// getter itself runs later, without this cell or a Context lock.
    pub(super) fn take_ready(
        self: &Arc<Self>,
    ) -> Result<Option<DynamicFilterReadDelivery>, TaskReadPayloadError> {
        let mut state = self.state.lock().expect("original Task read payload");
        match &*state {
            State::Empty => return Ok(None),
            State::Unresolved => return Err(TaskReadPayloadError::Unresolved),
            State::Ready(_) => {}
            _ => return Err(TaskReadPayloadError::WrongPhase),
        }
        let State::Ready(loan) = std::mem::replace(&mut *state, State::Lent) else {
            unreachable!("qualified original read pin")
        };
        Ok(Some(DynamicFilterReadDelivery {
            loan: Some(loan),
            return_to: self.clone(),
        }))
    }

    /// The original R custodian must first prove its external leg Ended.
    /// Only an unborrowed Ready pin can be reclaimed here. Release the pin
    /// before returning the same authority; a Lent delivery still owns it.
    pub(super) fn take_unlent_after_external_end(&self) -> Option<ExitObligation> {
        let loan = {
            let mut state = self.state.lock().expect("original Task read payload");
            if !matches!(*state, State::Ready(_)) {
                return None;
            }
            let State::Ready(loan) = std::mem::replace(&mut *state, State::Lent) else {
                unreachable!("qualified unborrowed read pin")
            };
            loan
        };
        if std::thread::panicking() {
            self.mark_unresolved();
            drop(loan);
            return None;
        }
        self.return_original(loan.reclaim());
        self.take_returned()
    }

    pub(super) fn take_returned(&self) -> Option<ExitObligation> {
        let mut state = self.state.lock().expect("original Task read payload");
        match &mut *state {
            State::Returned(authority) => authority.take(),
            _ => None,
        }
    }

    pub(super) fn is_unresolved(&self) -> bool {
        matches!(
            *self.state.lock().expect("original Task read payload"),
            State::Unresolved
        )
    }

    fn return_original(&self, authority: ExitObligation) {
        let refused = {
            let mut state = self.state.lock().expect("original Task read payload");
            if matches!(*state, State::Lent) {
                *state = State::Returned(Some(authority));
                None
            } else {
                Some(authority)
            }
        };
        if let Some(authority) = refused {
            // This cannot be a second successful return. Preserve Unknown
            // rather than signing exit or dropping an original under a lock.
            self.mark_unresolved();
            drop(authority);
        }
        self.notice.notify();
    }

    fn mark_unresolved(&self) {
        let original = {
            let mut state = self.state.lock().expect("original Task read payload");
            std::mem::replace(&mut *state, State::Unresolved)
        };
        // A surviving token/loan marks its original role Unknown on Drop;
        // neither path destroys an opaque RF value or invents physical exit.
        drop(original);
        self.notice.notify();
    }
}

/// This non-cloneable delivery keeps one actual RF row pin. The Ready loan
/// never retains the shared cell, so only this external loan has a return link.
#[must_use = "Retain the original read pin through actual projection and encoding"]
pub(crate) struct DynamicFilterReadDelivery {
    loan: Option<TaskDynamicFilterLoan>,
    return_to: Arc<TaskReadPayloadShared>,
}
impl DynamicFilterReadDelivery {
    pub(super) fn version(&self) -> DomainVersion {
        self.loan.as_ref().expect("live original RF read").version()
    }
    pub(super) fn payload_id(&self) -> PayloadId {
        self.loan
            .as_ref()
            .expect("live original RF read")
            .payload_id()
    }
    pub(super) fn payload(&self) -> &dyn CodecOwnedContent {
        self.loan.as_ref().expect("live original RF read").payload()
    }
}
impl Drop for DynamicFilterReadDelivery {
    fn drop(&mut self) {
        let Some(loan) = self.loan.take() else {
            return;
        };
        if std::thread::panicking() {
            self.return_to.mark_unresolved();
            drop(loan);
        } else {
            // Reclaim first releases the actual row pin. The custodian still
            // owes every input, external, report and final Work-exit fact.
            self.return_to.return_original(loan.reclaim());
        }
    }
}
