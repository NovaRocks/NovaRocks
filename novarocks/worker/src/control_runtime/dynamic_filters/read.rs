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

//! Exact committed RF row pins borrow the original request's authority.
//! They never issue another Work or lend an Arc of the opaque payload.

use super::{
    CodecOwnedContent, DomainVersion, DynamicFilterError, ExitAuthority, OpaquePayload, PayloadId,
    TaskDynamicFilterInbox, TaskIdentity,
};
use crate::{control_runtime::actors::WorkerAction, task_port::TaskPendingNotice};
use novarocks_control_runtime::{driver::EffectTicket, read_resource::ResourceReadLoan};
use novarocks_worker_protocol::context::ContextEffect;
use std::sync::Arc;

#[must_use = "Return the original read authority after the actual payload borrow exits"]
pub(crate) struct TaskDynamicFilterLoan {
    original: Option<ResourceReadLoan<OpaquePayload>>,
    version: DomainVersion,
    payload: PayloadId,
    pending: Arc<dyn TaskPendingNotice>,
}
impl TaskDynamicFilterLoan {
    pub(crate) fn version(&self) -> DomainVersion {
        self.version
    }
    pub(crate) fn payload_id(&self) -> PayloadId {
        self.payload
    }
    /// Only the actual external codec body calls the getter. Metadata may
    /// prepare this loan without invoking opaque code or copying its owner.
    pub(crate) fn payload(&self) -> &dyn CodecOwnedContent {
        self.original
            .as_ref()
            .expect("original RF read loan")
            .value()
            .value
            .as_ref()
    }
    /// The common loan releases its real pin before returning the SAME token.
    /// Notify the original RF owner only afterwards, outside every store lock.
    pub(crate) fn reclaim(mut self) -> ExitAuthority {
        let original = self
            .original
            .take()
            .expect("one original RF read authority")
            .reclaim();
        self.pending.pending();
        original
    }
}
impl Drop for TaskDynamicFilterLoan {
    fn drop(&mut self) {
        if let Some(original) = self.original.take() {
            // A lost/unwinding loan cannot manufacture positive authority
            // exit. The common loan records Unknown and never destroys T.
            drop(original);
            self.pending.pending();
        }
    }
}
impl TaskDynamicFilterInbox {
    /// The request issuer independently validates the exact original R input,
    /// command and Task. Common validates the actual ledger token, not numeric
    /// identity. Failure returns that original token without starting it.
    pub(crate) fn loan_selected(
        &self,
        task: TaskIdentity,
        version: DomainVersion,
        payload: PayloadId,
        authority: ExitAuthority,
    ) -> Result<TaskDynamicFilterLoan, (DynamicFilterError, ExitAuthority)> {
        if task != self.custody.task {
            return Err((DynamicFilterError::WrongReadSelection, authority));
        }
        match self.custody.scope.gates() {
            Ok(gates) if gates.failed => {
                return Err((DynamicFilterError::FailurePublicationRequired, authority));
            }
            Err(error) => return Err((DynamicFilterError::Runtime(error), authority)),
            _ => {}
        }
        let state = self
            .custody
            .state
            .lock()
            .expect("original RF read selector");
        if state.read_admission_closed || self.closed {
            return Err((DynamicFilterError::ReadAdmissionClosed, authority));
        }
        if state.unresolved || state.rejected.is_some() {
            return Err((DynamicFilterError::Unresolved, authority));
        }
        // A committed flight is already selectable even while a slow reader
        // pins the old current row. Never substitute a later version or an
        // unexposed candidate for the Core's exact frozen selection.
        let snapshot = state
            .flight
            .as_ref()
            .filter(|flight| flight.published)
            .map(|flight| &flight.snapshot)
            .filter(|snapshot| snapshot.version == version && snapshot.row.id().get() == payload.0)
            .or_else(|| {
                state.latest.as_ref().filter(|snapshot| {
                    snapshot.version == version && snapshot.row.id().get() == payload.0
                })
            });
        let Some(snapshot) = snapshot else {
            return Err((DynamicFilterError::WrongReadSelection, authority));
        };
        // Keep the same state -> store order as publication/disposal. A real
        // pin is installed before the Task lane may publish a replacement.
        let original = match snapshot.row.view().loan(authority) {
            Ok(loan) => loan,
            Err((error, authority)) => {
                return Err((DynamicFilterError::Resource(error), authority));
            }
        };
        drop(state);
        Ok(TaskDynamicFilterLoan {
            original: Some(original),
            version,
            payload,
            pending: self.custody.pending.clone(),
        })
    }

    /// Called in the original RetireTask lane, after earlier PrepareReadPayload
    /// effects completed their pins. Returning closes admission only: existing
    /// loans and physical jobs finish outside the lane before actual disposal.
    pub(crate) fn close_read_admission(
        &mut self,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<bool, DynamicFilterError> {
        self.runtime.validate_effect_ticket(ticket)?;
        if ticket.actor != self.actor
            || !matches!(&ticket.effect,
                WorkerAction::Context(ContextEffect::RetireTask { task, .. })
                if *task == self.custody.task)
            || !self.runtime.owns_scope(self.actor, &self.custody.scope)?
        {
            return Err(DynamicFilterError::WrongReadSelection);
        }
        let mut state = self
            .custody
            .state
            .lock()
            .expect("original RF read admission close");
        let changed = !state.read_admission_closed;
        state.read_admission_closed = true;
        state.sealed = true;
        drop(state);
        if changed {
            self.custody.pending.pending();
        }
        Ok(changed)
    }
}
