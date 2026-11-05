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

//! The Participation's unique retained Root. Task execution holds only a Weak view;
//! read aliases and payload credit stay here through real Participation disposal.
use super::TaskRootReadError;
use crate::control_runtime::participation_host::task_io::TaskRootWake;
use crate::root_result_channel::{RootChannelRead, RootResultChannel};
use novarocks_control_runtime::transaction::ScopeHandle;
use novarocks_execution_contract::{
    ParticipationRef, TaskIdentity, root_lifetime::RootRetentionClose, root_result::RootResultRead,
};
use std::{
    any::Any,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
    task::Wake,
};

struct State {
    root: Option<ManuallyDrop<Arc<RootResultChannel>>>,
    adopted: bool,
    seal_queued: bool,
    seal_body_done: bool,
    panic: Option<ManuallyDrop<Box<dyn Any + Send>>>,
}
pub(crate) struct RootRetentionCustody {
    identity: TaskIdentity,
    participation: ParticipationRef,
    scope: ScopeHandle,
    wake: Arc<TaskRootWake>,
    state: Mutex<State>,
}
/// Unique actual owner token, moved into the original Participation's finite slot.
pub(crate) struct RootParticipationRetention {
    custody: Arc<RootRetentionCustody>,
}
impl RootParticipationRetention {
    pub(super) fn from_installed(
        task: TaskIdentity,
        participation: ParticipationRef,
        scope: ScopeHandle,
        wake: Arc<TaskRootWake>,
        root: Arc<RootResultChannel>,
    ) -> Self {
        assert_eq!(
            root.spec().task,
            task,
            "the original Root belongs to this Task"
        );
        Self {
            custody: Arc::new(RootRetentionCustody {
                identity: task,
                participation,
                scope,
                wake,
                state: Mutex::new(State {
                    root: Some(ManuallyDrop::new(root)),
                    adopted: false,
                    seal_queued: false,
                    seal_body_done: false,
                    panic: None,
                }),
            }),
        }
    }
    pub(super) fn downgrade(&self) -> std::sync::Weak<RootRetentionCustody> {
        Arc::downgrade(&self.custody)
    }
    pub(crate) fn identity(&self) -> TaskIdentity {
        self.custody.identity
    }
    pub(crate) fn participation(&self) -> ParticipationRef {
        self.custody.participation
    }
    pub(crate) fn matches_scope(&self, scope: &ScopeHandle) -> bool {
        self.custody.scope.same_scope(scope)
    }
    pub(crate) fn adopt(&self) -> Result<(), TaskRootReadError> {
        let mut state = self
            .custody
            .state
            .lock()
            .expect("original Participation Root handoff");
        let root = state.root.as_ref().ok_or(TaskRootReadError::NoRoot)?;
        // A real Participation seal may have won installation; preserve that closed
        // channel without reopening reads or manufacturing an End.
        if !root.is_closed() {
            root.mark_participation_owned()
                .map_err(TaskRootReadError::Channel)?;
        }
        state.adopted = true;
        Ok(())
    }
    pub(crate) fn task_retired(&self) {
        self.custody.wake.retain_by_participation();
    }
    pub(crate) fn begin_original_root_read(
        &self,
        read: &RootResultRead,
    ) -> Result<RootChannelRead, TaskRootReadError> {
        self.custody.begin_original_root_read(read)
    }
    pub(crate) fn closed_read_frontier(
        &self,
        read: &RootResultRead,
    ) -> Result<Option<u64>, TaskRootReadError> {
        let state = self
            .custody
            .state
            .lock()
            .expect("original Participation Root route");
        if !state.adopted {
            return Err(TaskRootReadError::OwnerNotReady);
        }
        state
            .root
            .as_ref()
            .ok_or(TaskRootReadError::NoRoot)?
            .closed_read_frontier(read)
            .map_err(TaskRootReadError::Channel)
    }
    /// Borrow the original channel without creating a rich alias or a credit pin.
    pub(crate) fn try_ownership_snapshot(
        &self,
    ) -> Option<crate::root_result_channel::RootOwnershipSnapshot> {
        let state = self.custody.state.try_lock().ok()?;
        state.root.as_ref()?.try_ownership_snapshot().ok()?
    }
    pub(crate) fn physical_idle(&self) -> bool {
        self.custody.physical_idle()
    }
    pub(crate) fn dispose_on_host(self) {
        let root = self
            .custody
            .state
            .lock()
            .expect("original Participation Root disposal")
            .root
            .take();
        drop(root.map(ManuallyDrop::into_inner));
    }
    pub(crate) fn prepare_seal_for_participation(
        &self,
        reason: RootRetentionClose,
    ) -> Result<RetainedRootSealPreparation, RetainedRootSealError> {
        let mut state = self
            .custody
            .state
            .lock()
            .expect("original Participation Root seal");
        if state.panic.is_some() {
            return Err(RetainedRootSealError::Panicked);
        }
        if state.seal_body_done {
            return Ok(RetainedRootSealPreparation::Ready);
        }
        if state.seal_queued {
            return Ok(RetainedRootSealPreparation::Pending);
        }
        state
            .root
            .as_ref()
            .ok_or(RetainedRootSealError::NoRoot)?
            .seal_reads(reason);
        state.seal_queued = true;
        Ok(RetainedRootSealPreparation::Job(RetainedRootSeal {
            owner: ManuallyDrop::new(self.custody.clone()),
        }))
    }
}
impl RootRetentionCustody {
    pub(super) fn producer_relinquished(&self) -> bool {
        let state = self.state.lock().expect("original Root producer receipt");
        state.adopted
            && state
                .root
                .as_ref()
                .is_some_and(|root| root.producer_retirement_ready())
    }
    pub(super) fn begin_original_root_read(
        &self,
        read: &RootResultRead,
    ) -> Result<RootChannelRead, TaskRootReadError> {
        let state = self.state.lock().expect("original Participation Root read");
        if !state.adopted {
            return Err(TaskRootReadError::OwnerNotReady);
        }
        state
            .root
            .as_ref()
            .ok_or(TaskRootReadError::NoRoot)?
            .begin_read(read)
            .map_err(TaskRootReadError::Channel)
    }
    fn physical_idle(&self) -> bool {
        let state = self.state.lock().expect("original Participation Root idle");
        state.seal_body_done
            && state.panic.is_none()
            && state
                .root
                .as_ref()
                .is_some_and(|root| root.try_join_physical())
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RetainedRootSealError {
    NoRoot,
    Panicked,
}
pub(crate) enum RetainedRootSealPreparation {
    Ready,
    Pending,
    Job(RetainedRootSeal),
}
#[must_use = "The original Participation Seal ticket owns this physical capture"]
pub(crate) struct RetainedRootSeal {
    owner: ManuallyDrop<Arc<RootRetentionCustody>>,
}
impl RetainedRootSeal {
    pub(crate) fn finish_on_host(self) -> Result<(), RetainedRootSealError> {
        let owner = ManuallyDrop::into_inner(self.owner);
        let result = catch_unwind(AssertUnwindSafe(|| {
            let root = owner
                .state
                .lock()
                .expect("original Root seal capture")
                .root
                .clone()
                .map(ManuallyDrop::into_inner)
                .ok_or(RetainedRootSealError::NoRoot)?;
            root.finish_seal();
            drop(root);
            Ok(())
        }));
        let outcome = match result {
            Ok(Ok(())) => {
                owner
                    .state
                    .lock()
                    .expect("actual Root seal exit")
                    .seal_body_done = true;
                Ok(())
            }
            Ok(Err(error)) => Err(error),
            Err(payload) => {
                owner.state.lock().expect("original Root seal panic").panic =
                    Some(ManuallyDrop::new(payload));
                Err(RetainedRootSealError::Panicked)
            }
        };
        owner.wake.wake_by_ref();
        outcome
    }
}
