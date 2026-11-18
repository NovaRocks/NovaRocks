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

//! Stable original governance custody and actual Source/failure-job rendezvous.
//! These levels report their authors' completed work; they grant no capacity.

use novarocks_query_application::coordination::executor::{
    GovernanceExecutor, GovernanceOwnerState,
};
use std::task::Waker;
use std::{
    mem::ManuallyDrop,
    sync::{Arc, Mutex},
};
use tokio::sync::Notify;

struct State {
    other_work_exited: bool,
    disposal_requested: bool,
    originals_disposed: bool,
    governance: Option<ManuallyDrop<Arc<GovernanceExecutor>>>,
    governance_completed: bool,
}
/// Explicitly consumed by the original Source and failure job; no Drop check.
/// Unconsumed governance is retained and cannot request OwnerDropped or refund
/// its stage merely because a parent future/observer disappeared.
#[must_use = "Observe actual application disposal and consume the original completed governor"]
pub(in crate::control_runtime::logical) struct LogicalFailureProgressObligation {
    state: Mutex<State>,
    pub(in crate::control_runtime::logical) changed: Arc<Notify>,
    host: Waker,
}
impl LogicalFailureProgressObligation {
    pub(in crate::control_runtime::logical) fn new(
        governance: Option<Arc<GovernanceExecutor>>,
        host: Waker,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                other_work_exited: false,
                disposal_requested: false,
                originals_disposed: false,
                governance: governance.map(ManuallyDrop::new),
                governance_completed: false,
            }),
            changed: Arc::new(Notify::new()),
            host,
        })
    }
    /// Only the Source calls this after its OTHER original jobs, tails,
    /// inputs and AP child Scopes have actually retired. Excludes this same
    /// failure job's own input/header; waiting for those would self-deadlock.
    pub(in crate::control_runtime::logical) fn observe_other_work_exited(&self) {
        let changed = {
            let mut state = self.state.lock().unwrap();
            !std::mem::replace(&mut state.other_work_exited, true)
        };
        if changed {
            self.changed.notify_waiters();
        }
    }
    pub(in crate::control_runtime::logical) fn observe_other_work_progress(&self) {
        self.changed.notify_waiters();
    }
    pub(in crate::control_runtime::logical) fn other_work_exited(&self) -> bool {
        self.state.lock().unwrap().other_work_exited
    }
    /// The failure job has relinquished every non-governance application
    /// alias. The Source independently starts its SAME prepaid disposal Work.
    pub(in crate::control_runtime::logical) fn request_disposal(&self) {
        let changed = {
            let mut state = self.state.lock().unwrap();
            !std::mem::replace(&mut state.disposal_requested, true)
        };
        if changed {
            self.host.wake_by_ref();
        }
    }
    pub(in crate::control_runtime::logical) fn disposal_requested(&self) -> bool {
        self.state.lock().unwrap().disposal_requested
    }
    /// Only an actual disposal Join/report/header retirement supplies this fact.
    pub(in crate::control_runtime::logical) fn observe_originals_disposed(&self) {
        let changed = {
            let mut state = self.state.lock().unwrap();
            !std::mem::replace(&mut state.originals_disposed, true)
        };
        if changed {
            self.changed.notify_waiters();
        }
    }
    pub(in crate::control_runtime::logical) async fn wait_disposed(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.state.lock().unwrap().originals_disposed {
                return;
            }
            changed.await;
        }
    }
    /// Borrow only the SAME stable original. The caller drops this temporary
    /// alias before taking the final original for controlled destruction.
    pub(in crate::control_runtime::logical) fn governance(
        &self,
    ) -> Option<Arc<GovernanceExecutor>> {
        self.state
            .lock()
            .unwrap()
            .governance
            .as_ref()
            .cloned()
            .map(ManuallyDrop::into_inner)
    }
    /// Transfer the unique completed original for actual Drop inside the
    /// credentialed failure job. Refusal preserves its stable owner slot.
    pub(in crate::control_runtime::logical) fn take_completed_governance(
        &self,
    ) -> Result<Option<GovernanceExecutor>, ()> {
        let original = {
            let mut state = self.state.lock().unwrap();
            if !state.originals_disposed {
                return Err(());
            }
            state.governance.take()
        };
        let Some(original) = original else {
            return Ok(None);
        };
        let original = ManuallyDrop::into_inner(original);
        if original.owner_state() != GovernanceOwnerState::Completed {
            self.state.lock().unwrap().governance = Some(ManuallyDrop::new(original));
            return Err(());
        }
        match Arc::try_unwrap(original) {
            Ok(original) => Ok(Some(original)),
            Err(original) => {
                self.state.lock().unwrap().governance = Some(ManuallyDrop::new(original));
                Err(())
            }
        }
    }
    /// The actual final governor Drop has returned normally. An ambiguous
    /// operation, retained alias or Drop panic must never call this method.
    pub(in crate::control_runtime::logical) fn observe_governance_completed(&self) {
        let mut state = self.state.lock().unwrap();
        assert!(
            state.originals_disposed && state.governance.is_none(),
            "actual original governor destruction"
        );
        state.governance_completed = true;
        drop(state);
        self.host.wake_by_ref();
    }
    pub(in crate::control_runtime::logical) fn governance_completed(&self) -> bool {
        self.state.lock().unwrap().governance_completed
    }
}
