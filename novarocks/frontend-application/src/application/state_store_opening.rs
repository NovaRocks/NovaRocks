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

//! Original opening and instance-close receiving in the existing Host.
//! Only the borrowed bootstrap/teardown polls this custody; there is no task.
use super::*;
use crate::state_store::execution::StateStoreOriginalOwner;
use novarocks_state_store_api::{StateStoreExecutionFailure as Failure, StateStoreOriginalClose};
use std::task::{Context, Poll};

struct InstanceClose {
    owner: StateStoreOriginalOwner,
    original: Option<Box<dyn StateStoreOriginalClose>>,
    receiver_closed: bool,
    receiver_failure: Option<Failure>,
}
pub(super) struct FrontendStateStoreOpening {
    prepared: PreparedStateStoreHost,
    owner: Option<StateStoreOriginalOwner>,
    instance_close: Option<InstanceClose>,
    evidence_owner: Option<StateStoreOriginalOwner>,
    bootstrap_read: Option<StateStoreOriginalOwner>,
    started: bool,
    closing: bool,
    first_failure: Option<Failure>,
}
impl FrontendStateStoreOpening {
    pub(super) fn new(prepared: PreparedStateStoreHost) -> Self {
        Self {
            prepared,
            owner: None,
            instance_close: None,
            evidence_owner: None,
            bootstrap_read: None,
            started: false,
            closing: false,
            first_failure: None,
        }
    }
    pub(super) fn evidence_progress(
        &mut self,
        ready: &StateStoreHost,
        executor: &QueryBlockingExecutor,
    ) -> Result<crate::state_store::execution::StateStoreOriginalProgress, Failure> {
        if self.evidence_owner.is_none() {
            let (vocabulary, instance) = ready.original_blocking_sources().ok_or_else(|| {
                novarocks_state_store_api::StateStoreOriginalUnsupported.into_original_failure()
            })?;
            self.evidence_owner = Some(StateStoreOriginalOwner::instance_evidence(
                executor,
                vocabulary,
                instance,
                ready.run_policy(),
            )?);
        }
        Ok(self.evidence_owner.as_ref().unwrap().progress_handle())
    }
    /// Install the finite startup reader before any repository future exists.
    pub(super) fn bootstrap_read_execution(
        &mut self,
        ready: &StateStoreHost,
        executor: &QueryBlockingExecutor,
    ) -> Result<crate::state_store::execution::StateStoreOriginalReadExecution, Failure> {
        if self.closing {
            return Err(
                novarocks_state_store_api::StateStoreOriginalUnsupported.into_original_failure()
            );
        }
        if self.bootstrap_read.is_none() {
            let (vocabulary, instance) = ready.original_blocking_sources().ok_or_else(|| {
                novarocks_state_store_api::StateStoreOriginalUnsupported.into_original_failure()
            })?;
            self.bootstrap_read = Some(StateStoreOriginalOwner::bootstrap_read(
                executor,
                vocabulary,
                instance,
                ready.run_policy(),
            )?);
        }
        let store = ready.state_store().ok_or_else(|| {
            novarocks_state_store_api::StateStoreOriginalUnsupported.into_original_failure()
        })?;
        self.bootstrap_read
            .as_ref()
            .unwrap()
            .read_execution(store.limits().transaction_deadline)
    }
    pub(super) fn poll_bootstrap_read(&mut self, cx: &mut Context<'_>) -> Result<bool, Failure> {
        self.bootstrap_read
            .as_ref()
            .ok_or_else(|| {
                novarocks_state_store_api::StateStoreOriginalUnsupported.into_original_failure()
            })?
            .poll_progress(cx)
    }
    pub(super) fn poll_bootstrap_read_closed(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Failure>> {
        if let Some(reader) = self.bootstrap_read.as_mut() {
            std::task::ready!(reader.poll_closed(cx))?;
        }
        self.bootstrap_read.take();
        Poll::Ready(Ok(()))
    }

    pub(super) fn retain_evidence_failure(&mut self, cause: Failure) {
        self.first_failure.get_or_insert(cause);
    }
    pub(super) fn deadline(&self) -> Instant {
        self.prepared.deadline()
    }
    pub(super) fn start(&mut self, executor: &QueryBlockingExecutor) -> Result<(), Failure> {
        if self.started || self.closing {
            return Err(novarocks_state_store_api::StateStoreError::new(
                novarocks_state_store_api::StateStoreErrorKind::InvalidRequest,
                "frontend original opening already started or stopped",
            )
            .into_original_failure());
        }
        self.owner = Some(StateStoreOriginalOwner::opening(
            executor,
            self.prepared.vocabulary(),
            self.prepared.run_policy(),
        )?);
        self.started = true;
        let context = self.owner.as_ref().unwrap().context(self.deadline());
        let result = self.prepared.start_in(&context);
        if let Err(error) = &result {
            self.first_failure.get_or_insert_with(|| error.clone());
        }
        result
    }
    pub(super) fn original_failure(&self) -> Option<&Failure> {
        self.first_failure
            .as_ref()
            .or_else(|| self.prepared.original_failure())
    }
    pub(super) fn poll_open(
        &mut self,
        cx: &mut Context<'_>,
        target: &mut Option<StateStoreHost>,
    ) -> Poll<Result<(), FrontendApplicationError>> {
        let result = self.poll_open_inner(cx, target);
        if let Poll::Ready(Err(error)) = &result
            && let Some(cause) = error.original_state_store_failure()
        {
            self.first_failure.get_or_insert_with(|| cause.clone());
        }
        result
    }
    fn poll_open_inner(
        &mut self,
        cx: &mut Context<'_>,
        target: &mut Option<StateStoreHost>,
    ) -> Poll<Result<(), FrontendApplicationError>> {
        let Some(owner) = self.owner.as_ref() else {
            return Poll::Ready(Err(FrontendApplicationError::server(
                "original opening owner was not installed",
            )));
        };
        if let Err(error) = owner.poll_progress(cx) {
            return Poll::Ready(Err(FrontendApplicationError::original_state_store(error)));
        }
        match self.prepared.poll_ready(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => {
                Poll::Ready(Err(FrontendApplicationError::original_state_store(error)))
            }
            Poll::Ready(Ok(())) => Poll::Ready(
                self.prepared
                    .install_ready_in(target)
                    .map_err(FrontendApplicationError::state_store),
            ),
        }
    }
    /// Both receivers remain fields of this Host across bounded shutdown passes.
    /// A deadline only ends the caller's wait; it never clears this inventory.
    pub(super) fn poll_close(
        &mut self,
        ready: &mut Option<StateStoreHost>,
        executor: &QueryBlockingExecutor,
        deadline: Instant,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Failure>> {
        let result = self.poll_close_inner(ready, executor, deadline, cx);
        if let Some(cause) = self
            .instance_close
            .as_ref()
            .and_then(|close| close.receiver_failure.as_ref())
        {
            self.first_failure.get_or_insert_with(|| cause.clone());
        }
        if let Poll::Ready(Err(error)) = &result {
            self.first_failure.get_or_insert_with(|| error.clone());
        }
        result
    }
    fn poll_close_inner(
        &mut self,
        ready: &mut Option<StateStoreHost>,
        executor: &QueryBlockingExecutor,
        deadline: Instant,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Failure>> {
        if !self.closing {
            self.closing = true;
            self.prepared.request_close();
        }
        if let Some(owner) = self.owner.as_mut() {
            match owner.poll_closed(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) => {}
            }
        }
        // A cancelled startup borrower may leave an exact read/close receipt.
        // It remains in this Host field until its original physical exit.
        std::task::ready!(self.poll_bootstrap_read_closed(cx))?;
        // The cadence was stopped/joined and its final borrowed pass ended
        // before this method. Keep the same evidence receiver until real exit;
        // otherwise its connection bundle would pin the following File close.
        if let Some(owner) = self.evidence_owner.as_mut() {
            match owner.poll_closed(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(cause)) => return Poll::Ready(Err(cause)),
                Poll::Ready(Ok(())) => {}
            }
        }
        self.evidence_owner.take();
        // This selected actual instance may have failed validation; it is not
        // dropped or handed to a legacy shutdown future on that error path.
        let scope = if let Some(instance) = self.prepared.retained_instance_mut() {
            instance.blocking_instance_scope()
        } else {
            ready
                .as_ref()
                .and_then(|host| host.original_blocking_sources().map(|(_, scope)| scope))
        };
        let Some(scope) = scope else {
            if self.prepared.has_retained_instance() || ready.is_some() {
                return Poll::Ready(Err(novarocks_state_store_api::StateStoreError::new(
                    novarocks_state_store_api::StateStoreErrorKind::Internal,
                    "retained state store instance has no original close scope",
                )
                .into_original_failure()));
            }
            return Poll::Ready(Ok(()));
        };
        if self.instance_close.is_none() {
            let owner = match StateStoreOriginalOwner::closing_instance(
                executor,
                self.prepared.vocabulary(),
                scope,
                self.prepared.run_policy(),
            ) {
                Ok(owner) => owner,
                Err(error) => return Poll::Ready(Err(error)),
            };
            self.instance_close = Some(InstanceClose {
                owner,
                original: None,
                receiver_closed: false,
                receiver_failure: None,
            });
        }
        let close = self.instance_close.as_mut().unwrap();
        if close.original.is_none() {
            let context = close.owner.context(deadline);
            let result = if let Some(instance) = self.prepared.retained_instance_mut() {
                instance.prepare_close_in(&context, &mut close.original)
            } else {
                ready
                    .as_mut()
                    .expect("actual selected instance")
                    .prepare_original_close_in(&context, &mut close.original)
            };
            if let Err(error) = result {
                return Poll::Ready(Err(error));
            }
            if close.original.is_none() {
                return Poll::Ready(Err(novarocks_state_store_api::StateStoreError::new(
                    novarocks_state_store_api::StateStoreErrorKind::Internal,
                    "provider returned no original instance close receiver",
                )
                .into_original_failure()));
            }
        }
        close.poll_closed(ready, cx)
    }
}
impl InstanceClose {
    fn poll_closed(
        &mut self,
        ready: &mut Option<StateStoreHost>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Failure>> {
        // Binding must precede closing intake. Always drive the same owner's
        // original backoff, including while the provider has no next request
        // because an external alias still pins its actual Store/File.
        let physical = self.owner.poll_closed(cx);
        if !self.receiver_closed && self.receiver_failure.is_none() {
            let original = self
                .original
                .as_mut()
                .expect("close receiver bound before intake closes");
            let result = if let Some(host) = ready.as_mut() {
                host.poll_original_closed(original.as_mut(), cx)
            } else {
                original.poll_closed(cx)
            };
            match result {
                Poll::Pending => {}
                Poll::Ready(Err(cause)) => {
                    self.receiver_failure = Some(cause);
                }
                Poll::Ready(Ok(())) => self.receiver_closed = true,
            }
        }
        // A receiver can expose Original health before QA's actual footer.
        // Keep that evidence, but only the physical owner's reportable-health
        // gate/actual-empty observation can end this borrowed Host wait.
        match physical {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(cause)) => {
                Poll::Ready(Err(self.receiver_failure.clone().unwrap_or(cause)))
            }
            Poll::Ready(Ok(())) => {
                if let Some(cause) = &self.receiver_failure {
                    return Poll::Ready(Err(cause.clone()));
                }
                if !self.receiver_closed {
                    return Poll::Pending;
                }
                drop(ready.take());
                Poll::Ready(Ok(()))
            }
        }
    }
}

#[cfg(test)]
#[path = "state_store_opening_tests.rs"]
mod tests;
