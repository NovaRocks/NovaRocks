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

//! Same original child receipt and stable responsibility cell.

use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::task::{Context, Poll};

use novarocks_workload_control::joint_admission::{
    JointAdjustmentReceipt, JointAdmission, JointProposalSlotWait, JointTryBegin,
};
use novarocks_workload_control::{ResultWindowAlias, WorkError, WorkScope};

use super::{ConnectorBlockingIoResponsibility, ResponsibilityCell};
use crate::query_execution::native_execution_adapter::source_cohort::NativeSourceCohort;

enum ChildSource {
    LegacyIndividual,
    Cohort(NativeSourceCohort),
}

/// Move-only, unpublished custody of the original request, reply and receiving
/// cell. A poll only borrows it; cancellation never moves the unique cell into
/// an independent waiting task. Declaration order releases request/receipt
/// before the original cell/window, including on unwind.
pub(crate) struct ConnectorBlockingIoAdmission {
    request: Option<JointAdmission>,
    wait: Option<JointProposalSlotWait>,
    receipt: Option<JointAdjustmentReceipt>,
    responsibility: Option<ConnectorBlockingIoResponsibility>,
    source: ChildSource,
    parent: WorkScope,
    failure: Option<WorkError>,
}

impl ConnectorBlockingIoResponsibility {
    pub(crate) fn request(
        scope: &WorkScope,
        window: &ResultWindowAlias,
    ) -> Result<ConnectorBlockingIoAdmission, WorkError> {
        if !window.is_for_scope(scope) {
            return Err(WorkError::Conflict);
        }
        Ok(ConnectorBlockingIoAdmission {
            request: None,
            wait: None,
            receipt: None,
            responsibility: Some(Self(Arc::new(ResponsibilityCell {
                owner: None,
                pending_joins: AtomicUsize::new(0),
                _window: Some(window.clone()),
            }))),
            source: ChildSource::LegacyIndividual,
            parent: scope.clone(),
            failure: None,
        })
    }
}

impl ConnectorBlockingIoResponsibility {
    /// Production factories borrow the same already-paid attempt cohort.
    /// Construction is metadata-only; polling installs in the original cell.
    pub(crate) fn request_from_cohort(
        scope: &WorkScope,
        window: &ResultWindowAlias,
        cohort: NativeSourceCohort,
    ) -> Result<ConnectorBlockingIoAdmission, WorkError> {
        if !window.is_for_scope(scope) {
            return Err(WorkError::Conflict);
        }
        Ok(ConnectorBlockingIoAdmission {
            request: None,
            wait: None,
            receipt: None,
            responsibility: Some(Self(Arc::new(ResponsibilityCell {
                owner: None,
                pending_joins: AtomicUsize::new(0),
                _window: Some(window.clone()),
            }))),
            source: ChildSource::Cohort(cohort),
            parent: scope.clone(),
            failure: None,
        })
    }
}

impl ConnectorBlockingIoAdmission {
    fn poll_individual_request(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), WorkError>> {
        if self.request.is_some() || self.receipt.is_some() {
            return Poll::Ready(Ok(()));
        }
        // Full never suppresses the original parent's stop fact. Neither a
        // completed signal nor a free Q authorizes a new child by itself.
        self.parent.check()?;
        if let Some(wait) = self.wait.as_mut() {
            match Pin::new(wait).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => result?,
            }
            drop(self.wait.take());
        }
        if self.request.is_none() && self.receipt.is_none() {
            match self.parent.try_request_joint_child()? {
                JointTryBegin::Started(request) => self.request = Some(request),
                JointTryBegin::Full(signal) => {
                    self.wait = Some(signal.into_wait());
                    // Full may have captured its progress marker after a
                    // racing cancellation. Observe the original fact again
                    // before waiting on that marker; no new authority enters.
                    self.parent.check()?;
                    match Pin::new(self.wait.as_mut().unwrap()).poll(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(result) => result?,
                    }
                    // One failed constructor attempt per poll. Preserve the
                    // original cell across a real event racing registration.
                    drop(self.wait.take());
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
            }
        }
        Poll::Ready(Ok(()))
    }

    pub(crate) async fn ready(&mut self) -> Result<ConnectorBlockingIoResponsibility, WorkError> {
        poll_fn(|cx| self.poll_ready(cx)).await
    }

    pub(crate) fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<ConnectorBlockingIoResponsibility, WorkError>> {
        if let Some(error) = &self.failure {
            return Poll::Ready(Err(error.clone()));
        }
        let result = self.poll_install(cx);
        if let Poll::Ready(Err(error)) = &result {
            self.failure = Some(error.clone());
        }
        result
    }

    fn poll_install(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<ConnectorBlockingIoResponsibility, WorkError>> {
        if matches!(self.source, ChildSource::LegacyIndividual)
            && self
                .responsibility
                .as_ref()
                .is_some_and(|cell| cell.0.owner.is_none())
        {
            match self.poll_individual_request(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => result?,
            }
        }
        let receiving = self.responsibility.as_mut().ok_or(WorkError::Released)?;
        // No alias is published before the final Ready. The stable Arc allocation
        // is the original receiving cell, not a replacement owner or a registry.
        let cell = Arc::get_mut(&mut receiving.0).ok_or(WorkError::Conflict)?;
        if cell.owner.is_none() {
            let progress = match &self.source {
                ChildSource::Cohort(cohort) => {
                    cohort.take_child_in(&self.parent, &mut cell.owner)?
                }
                ChildSource::LegacyIndividual => {
                    if self.receipt.is_none() {
                        let request = self.request.as_mut().ok_or(WorkError::Released)?;
                        let batch = match Pin::new(request).poll(cx) {
                            Poll::Pending => return Poll::Pending,
                            Poll::Ready(Err(error)) => {
                                self.request.take();
                                return Poll::Ready(Err(error.error));
                            }
                            Poll::Ready(Ok(batch)) => batch,
                        };
                        // Retain the exact receipt before retiring the completed future
                        // or batch. A same-class child cannot mint physical claims.
                        self.receipt = batch.adjustment;
                        let no_physical_claims = batch.new_claims.is_empty();
                        drop(batch.new_claims);
                        self.request.take();
                        if !no_physical_claims {
                            return Poll::Ready(Err(WorkError::Conflict));
                        }
                    }
                    self.receipt
                        .as_mut()
                        .ok_or(WorkError::Conflict)?
                        .install_child_in(&self.parent, &mut cell.owner)?
                }
            };
            // owner is already in its stable cell and the original receipt is
            // disarmed. No receiver or WC lock is held during arbitrary wake.
            // If wake unwinds, this admission still owns that same child.
            progress.notify();
        }
        // Delegate the exact original window only after stable installation and
        // notification. Its predecessor is replaced before any actual alias Drop.
        let child = cell.owner.as_ref().ok_or(WorkError::Released)?.scope();
        let delegated = cell
            ._window
            .as_ref()
            .ok_or(WorkError::Released)?
            .for_child(&child)?;
        let previous = cell._window.replace(delegated);
        drop(previous);
        self.receipt.take();
        Poll::Ready(Ok(self.responsibility.take().ok_or(WorkError::Released)?))
    }
}
