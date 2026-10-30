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

//! One exact attempt's jointly paid scan and covered-subscription children.

use novarocks_workload_control::joint_admission::{
    JointAdjustmentReceipt, JointAdmission, JointProposalSlotWait, JointTryBegin, SourceCohort,
};
use novarocks_workload_control::{WorkError, WorkOwner, WorkScope, WorkloadProgressNotification};
use std::{
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

/// The same checked recipe author is used by production and the static minimum
/// invoice. RootStage is the existing QA execution owner, never another Source.
pub(crate) fn source_count(scans: usize, contexts: usize) -> Result<NonZeroUsize, WorkError> {
    let count = scans
        .checked_add(contexts)
        .ok_or(WorkError::ArithmeticOverflow)?;
    NonZeroUsize::new(count).ok_or(WorkError::InvalidConfig("empty Native Source cohort"))
}

/// Borrow the same positive lease; cloning this alias grants no new position.
/// The final alias retains every unused position until its actual owner exits.
#[derive(Clone)]
pub(crate) struct NativeSourceCohort(Arc<Mutex<Option<SourceCohort>>>);
impl NativeSourceCohort {
    /// Caller/Native Core locks must already be released. The destination is
    /// the actual stable Source receiver; notification is left to its author.
    pub(crate) fn take_child_in(
        &self,
        parent: &WorkScope,
        destination: &mut Option<WorkOwner>,
    ) -> Result<WorkloadProgressNotification, WorkError> {
        let mut original = self.0.lock().unwrap_or_else(|error| error.into_inner());
        original
            .as_mut()
            .ok_or(WorkError::NotReady)?
            .take_child_in(parent, destination)?
            .ok_or(WorkError::Capacity("Native Source cohort exhausted"))
    }

    #[cfg(test)]
    pub(crate) fn remaining(&self) -> usize {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .map_or(0, SourceCohort::remaining)
    }
}

/// Original projection fields, never captured by the borrowed polled F.
/// Declaration order retires original requests before the receiving Arc.
#[derive(Default)]
pub(super) struct NativeSourceCohortAdmission {
    admission: Option<JointAdmission>,
    receipt: Option<JointAdjustmentReceipt>,
    wait: Option<JointProposalSlotWait>,
    cohort: Option<NativeSourceCohort>,
    count: Option<NonZeroUsize>,
    failure: Option<WorkError>,
    polling: bool,
    installed: bool,
}
impl NativeSourceCohortAdmission {
    pub(super) fn poll(
        &mut self,
        parent: &WorkScope,
        count: NonZeroUsize,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), WorkError>> {
        if let Some(failure) = &self.failure {
            return Poll::Ready(Err(failure.clone()));
        }
        if self.polling || self.count.is_some_and(|old| old != count) {
            return Poll::Ready(Err(WorkError::Conflict));
        }
        // An unwound original poll is poisoned rather than recreated.
        self.polling = true;
        let result = self.poll_original(parent, count, cx);
        self.polling = false;
        if let Poll::Ready(Err(failure)) = &result {
            self.failure = Some(failure.clone());
        }
        result
    }

    fn poll_original(
        &mut self,
        parent: &WorkScope,
        count: NonZeroUsize,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), WorkError>> {
        if self.installed {
            return Poll::Ready(Ok(()));
        }
        self.count = Some(count);
        if self.cohort.is_none() {
            self.cohort = Some(NativeSourceCohort(Arc::new(Mutex::new(None))));
        }
        if let Some(wait) = self.wait.as_mut() {
            match Pin::new(wait).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) => drop(self.wait.take()),
            }
        }
        if self.admission.is_none() && self.receipt.is_none() {
            match parent.try_request_source_cohort(count)? {
                JointTryBegin::Started(admission) => self.admission = Some(admission),
                JointTryBegin::Full(signal) => {
                    self.wait = Some(signal.into_wait());
                    match Pin::new(self.wait.as_mut().unwrap()).poll(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                        Poll::Ready(Ok(())) => {
                            drop(self.wait.take());
                            cx.waker().wake_by_ref();
                            return Poll::Pending;
                        }
                    }
                }
            }
        }
        if self.receipt.is_none() {
            let mut batch = match Pin::new(self.admission.as_mut().unwrap()).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error.error)),
                Poll::Ready(Ok(batch)) => batch,
            };
            // The original receipt is stable before either original F or batch
            // may exit. A Source cohort never acquires physical claims.
            self.receipt = batch.adjustment.take();
            let empty = batch.new_claims.is_empty();
            drop(batch);
            drop(self.admission.take());
            if !empty {
                return Poll::Ready(Err(WorkError::Conflict));
            }
        }
        {
            let mut destination = self
                .cohort
                .as_ref()
                .unwrap()
                .0
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            self.receipt
                .as_mut()
                .ok_or(WorkError::Conflict)?
                .install_source_cohort_in(parent, &mut destination)?;
        }
        self.installed = true;
        // The receiving field is installed, its lock released, then the
        // original disarmed reply exits. No Source/SDK effect happened here.
        drop(self.receipt.take());
        Poll::Ready(Ok(()))
    }

    pub(super) fn borrowed(&self) -> Result<NativeSourceCohort, WorkError> {
        if !self.installed {
            return Err(WorkError::NotReady);
        }
        self.cohort.clone().ok_or(WorkError::Released)
    }

    /// Only called after all original scan-close and round convergence jobs
    /// joined. Pending subscription records retain their own same alias.
    pub(super) fn close_after_sources(&mut self) {
        drop(self.wait.take());
        drop(self.receipt.take());
        drop(self.admission.take());
        drop(self.cohort.take());
        self.failure = Some(WorkError::Released);
    }
}

#[cfg(test)]
pub(crate) mod tests;
