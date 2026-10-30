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

//! Background borrowing adapter for the SAME original action custody kernel.
use super::statement_action::{StatementActionFailure, StatementActionSource};
use super::*;
use crate::admitted_query_context::QueryExecutionContext;
use crate::api::StatementExternalEffectDisposition;
use crate::api::{QueryExecutionError, QueryExecutionErrorKind};
use crate::session_error::QueryServiceError;
use novarocks_workload_control::joint_admission::RetainedPayloadGrant;
use std::{
    alloc::Layout,
    task::{Context, Poll},
};

/// FA retains its domain result/proof in its original typed action slots.
/// Ready here is one neutral business completion; it grants no domain proof.
pub trait GovernedBackgroundAction: Send + 'static {
    fn external_effect_disposition(&self) -> Option<StatementExternalEffectDisposition> {
        None
    }
    fn local_health_failure(&self) -> Option<crate::api::StatementNativeHealthFailure> {
        None
    }
    fn handoff_residual_in(
        &mut self,
        _target: &mut Option<Box<dyn GovernedStatementResidual>>,
    ) -> Result<(), crate::api::StatementNativeHealthFailure> {
        Ok(())
    }
    fn poll_action(
        &mut self,
        context: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), QueryServiceError>>;
}

/// Original admitted job values. Domain objects stay in FA's typed Action or
/// its actual residual; QA has no Any/downcast or reconstruction capability.
pub struct BackgroundActionCustody {
    pub(super) owner: Option<WorkOwner>,
    scope: WorkScope,
    context: Option<Box<QueryExecutionContext>>,
    // Same original window, no compute/activity or query-concurrency pin.
    _result_retention: Option<ResultWindowAlias>,
    query_permit: Option<QueryConcurrencyPermit>,
    _payload: Option<RetainedPayloadGrant>,
    cancellation: CancellationView,
    timeout_ms: Option<u64>,
    finished: bool,
}
impl BackgroundActionCustody {
    pub(crate) fn validate(
        owner: &WorkOwner,
        context: &QueryExecutionContext,
        permit: &QueryConcurrencyPermit,
        payload: Option<&RetainedPayloadGrant>,
    ) -> Result<Option<u64>, WorkError> {
        let scope = owner.scope();
        scope.check()?;
        let binding = context.result_capacity().ok_or(WorkError::Conflict)?;
        if !owner.is_for_scope(binding.scope())
            || !permit.is_for_scope(&scope)
            || !binding.window_alias().is_for_scope(&scope)
            || binding.class() != ResultWindowClass::Internal
            || payload.is_some_and(|p| !p.is_for_scope(&scope))
        {
            return Err(WorkError::Conflict);
        }
        context.cancellation().governed_timeout_for_scope(&scope)
    }
    pub(crate) fn take_in(
        owner: &WorkOwner,
        context: &mut Option<QueryExecutionContext>,
        permit: &mut Option<QueryConcurrencyPermit>,
        payload: &mut Option<RetainedPayloadGrant>,
        timeout_ms: Option<u64>,
    ) -> Result<Self, WorkError> {
        // Derive from the exact original owner, never trust a caller's token.
        let scope = owner.scope();
        let cancellation = scope.cancellation()?;
        let result_retention = context
            .as_ref()
            .and_then(|c| c.result_capacity())
            .map(|binding| binding.window_alias());
        // Receive the same original context once, before any business poll.
        // The unique backing moves with custody and creates no new authority.
        let context = context.take().map(Box::new);
        Ok(Self {
            owner: None,
            scope,
            context,
            _result_retention: result_retention,
            query_permit: permit.take(),
            _payload: payload.take(),
            cancellation,
            timeout_ms,
            finished: false,
        })
    }
    pub(super) fn cancellation(&self) -> &CancellationView {
        &self.cancellation
    }
    pub(super) fn poll(
        &mut self,
        action: &mut dyn GovernedBackgroundAction,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), QueryServiceError>> {
        let window = self
            .context
            .as_ref()
            .and_then(|c| c.result_capacity())
            .map(|b| b.window_alias());
        let mut context = GovernedActionContext::new(
            &self.scope,
            &mut self.owner,
            &self.cancellation,
            self.timeout_ms,
            window.as_ref(),
        );
        action.poll_action(&mut context, cx)
    }
    pub(super) fn install_owner(&mut self, owner: &mut Option<WorkOwner>) {
        assert!(
            self.owner.is_none()
                && owner
                    .as_ref()
                    .is_some_and(|o| self.cancellation.is_for_scope(&o.scope()))
        );
        self.owner = owner.take();
    }
    /// Frontend business terminal releases only the original query quota.
    /// Actual Work/context/payload remain in this same source through residual exit.
    pub(super) fn take_business_query_permit(&mut self) -> Option<QueryConcurrencyPermit> {
        self.query_permit.take()
    }
    pub(super) fn close(&mut self) {
        // Every actual typed destructor runs in the common restoration loan.
        drop(self.context.take());
        drop(self.query_permit.take());
        // The passive original window and optional payload remain in this
        // SAME Source to fund its canonical result through the last observer.
        // They neither keep an ActiveOwner nor authorize another computation.
        self.owner
            .take()
            .expect("background retains its same original Root")
            .complete_after_terminal_cancel_settled();
        self.finished = true;
    }
    pub(super) fn finished(&self) -> bool {
        self.finished
    }
}

/// The observer carries no Root owner and can neither poll nor dispose it.
pub struct BackgroundActionReady {
    pub(crate) source: Arc<StatementActionSource>,
    pub(crate) completed: bool,
}
impl std::future::Future for BackgroundActionReady {
    type Output = Result<BackgroundActionReceipt, Arc<StatementActionFailure>>;
    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(!self.completed, "one original background business receiver");
        match self.source.poll_background_ready(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(failure)) => {
                self.completed = true;
                Poll::Ready(Err(failure))
            }
            Poll::Ready(Ok(())) => {
                self.completed = true;
                Poll::Ready(Ok(BackgroundActionReceipt {
                    source: Arc::clone(&self.source),
                    acknowledged: false,
                }))
            }
        }
    }
}
impl Drop for BackgroundActionReady {
    fn drop(&mut self) {
        if !self.completed {
            self.source.abandon_background();
        }
    }
}
/// The original result remains paid in Source until the product records it.
/// No variable domain error/proof is cloned or erased by this borrow.
pub struct BackgroundActionReceipt {
    source: Arc<StatementActionSource>,
    acknowledged: bool,
}
pub enum BackgroundActionResultView<'a> {
    Succeeded,
    Failed(&'a QueryServiceError),
}
impl BackgroundActionReceipt {
    /// Drop this borrow before taking a product lock or awaiting.
    pub fn view(&self) -> super::statement_action::BackgroundActionResultGuard<'_> {
        self.source.background_result()
    }
    /// Only acknowledges the business receipt. Actual exit is separate.
    pub fn finish(mut self) {
        self.acknowledged = true;
        self.source.acknowledge_background();
    }
}
impl Drop for BackgroundActionReceipt {
    fn drop(&mut self) {
        if !self.acknowledged {
            self.source.abandon_background();
        }
    }
}

/// Actual layouts from the common owner. `custody` is nested inside `source`;
/// it is informational, not a second allocation. The unique context backing
/// is separate; these layouts are not a complete retained-heap quote or an
/// admission policy.
#[derive(Clone, Copy)]
pub struct BackgroundActionSourceGeometry {
    pub source: Layout,
    pub wake: Layout,
    pub failure: Layout,
    pub summary: Layout,
    pub ready: Layout,
    pub receipt: Layout,
    pub custody: Layout,
    pub mutex_backing: Layout,
}

pub(super) const BACKGROUND_FAILURE_DETAIL: &str =
    "background action failed; the original source retains its cause";

pub(crate) fn background_failed() -> QueryExecutionError {
    QueryExecutionError::new(QueryExecutionErrorKind::Failed, BACKGROUND_FAILURE_DETAIL)
}
