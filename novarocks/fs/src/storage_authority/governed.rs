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

//! The existing authority's one governed in-flight refresh. No task is spawned.
//! The request cell retains only Weak progress; the same authority retains F,
//! its output and both original panic payloads until actual, proven exit.

use super::*;
use futures::future::BoxFuture;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorIoProgressSource, ConnectorRequestContext,
};
use std::{
    any::Any,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Weak,
    task::{Context, Poll},
};

type RawCause = Box<dyn Any + Send + 'static>;
pub(super) struct GovernedRefresh {
    shared: Weak<AuthorityShared>,
    body: Mutex<Option<Body>>,
}
struct Body {
    shared: Weak<AuthorityShared>,
    generation: u64,
    sender: Option<watch::Sender<Option<AcquisitionOutcome>>>,
    future: Option<BoxFuture<'static, Result<AuthorityMaterial, AcquisitionFailure>>>,
    outcome: Option<AcquisitionOutcome>,
    poll_failure: Option<RawCause>,
    drop_failure: Option<RawCause>,
    published: bool,
    // This same paid alias remains even after a poll/drop panic. No terminal,
    // timeout or authority closure is allowed to pretend the source exited.
    _request: Option<ConnectorRequestContext>,
}
struct Loan<'a> {
    original: &'a Mutex<Option<Body>>,
    body: Option<Body>,
}
impl Drop for Loan<'_> {
    fn drop(&mut self) {
        let mut slot = self.original.lock().unwrap_or_else(|p| p.into_inner());
        assert!(slot.is_none(), "authority refresh loan restored once");
        *slot = self.body.take();
    }
}
impl GovernedRefresh {
    pub(super) fn new(
        shared: Weak<AuthorityShared>,
        generation: u64,
        sender: watch::Sender<Option<AcquisitionOutcome>>,
        deadline: Instant,
        source: Arc<dyn AuthorityMaterialSource>,
        request: ConnectorRequestContext,
    ) -> Self {
        let request = match request.io_phase() {
            Some(phase) => request.clone().with_io_weak_phase(phase.weak_phase()),
            None => request,
        };
        let input = request.clone();
        // Construction is pure. The provider is invoked only once the whole
        // future has reached this original authority's fixed header.
        let future = Box::pin(async move { source.acquire_async(deadline, input).await });
        Self {
            shared: shared.clone(),
            body: Mutex::new(Some(Body {
                shared,
                generation,
                sender: Some(sender),
                future: Some(future),
                outcome: None,
                poll_failure: None,
                drop_failure: None,
                published: false,
                _request: Some(request),
            })),
        }
    }
    fn poll(&self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
        let body = self.body.lock().unwrap_or_else(|p| p.into_inner()).take();
        let Some(body) = body else {
            return Poll::Pending;
        };
        let mut loan = Loan {
            original: &self.body,
            body: Some(body),
        };
        let body = loan.body.as_mut().unwrap();
        if body.poll_failure.is_some() || body.drop_failure.is_some() {
            return Poll::Ready(Err(poisoned()));
        }
        if body.published {
            return Poll::Ready(Ok(()));
        }
        if body.outcome.is_none() {
            let result = catch_unwind(AssertUnwindSafe(|| {
                body.future.as_mut().unwrap().as_mut().poll(cx)
            }));
            match result {
                Ok(Poll::Pending) => return Poll::Pending,
                Ok(Poll::Ready(outcome)) => body.outcome = Some(Arc::new(outcome)),
                Err(cause) => body.poll_failure = Some(cause),
            }
            // Primary/outcome is stable before arbitrary future destruction.
            let future = body.future.take();
            if let Err(cause) = catch_unwind(AssertUnwindSafe(|| drop(future))) {
                body.drop_failure = Some(cause);
            }
            if body.poll_failure.is_some() || body.drop_failure.is_some() {
                return Poll::Ready(Err(poisoned()));
            }
        }
        if let Some(request) = body._request.take()
            && let Err(cause) = catch_unwind(AssertUnwindSafe(|| drop(request)))
        {
            body.drop_failure = Some(cause);
            return Poll::Ready(Err(poisoned()));
        }
        let Some(shared) = body.shared.upgrade() else {
            return Poll::Ready(Err(poisoned()));
        };
        let outcome = Arc::clone(body.outcome.as_ref().unwrap());
        // Keep the original header registered until every callback returned.
        // A wake panic leaves output/cause/context reachable from the authority.
        shared.apply_outcome_in(body.generation, &outcome, true);
        if let Some(sender) = body.sender.as_ref()
            && let Err(cause) = catch_unwind(AssertUnwindSafe(|| {
                let _ = sender.send(Some(Arc::clone(&outcome)));
            }))
        {
            body.drop_failure = Some(cause);
            return Poll::Ready(Err(poisoned()));
        }
        body.published = true;
        Poll::Ready(Ok(()))
    }
    fn completed(&self) -> bool {
        self.body
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .is_some_and(|body| body.published)
    }
}
fn poisoned() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Internal,
        "storage authority original governed refresh failed; original custody retained",
    )
}
impl std::fmt::Debug for InflightRefresh {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("InflightRefresh { original governed source }")
    }
}
impl std::fmt::Display for InflightRefresh {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original storage refresh cannot prove actual exit")
    }
}
impl std::error::Error for InflightRefresh {}
impl ConnectorIoProgressSource for InflightRefresh {
    fn actual_exit_observed(&self) -> bool {
        self.asynchronous
            .as_ref()
            .is_none_or(GovernedRefresh::completed)
    }
    fn health_cause(self: Arc<Self>) -> Option<Arc<dyn std::error::Error + Send + Sync>> {
        let failed = self.asynchronous.as_ref().is_some_and(|source| {
            source
                .body
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .as_ref()
                .is_some_and(|body| body.poll_failure.is_some() || body.drop_failure.is_some())
        });
        failed.then_some(self as Arc<dyn std::error::Error + Send + Sync>)
    }
    fn poll_original_progress(&self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
        let Some(source) = self.asynchronous.as_ref() else {
            return Poll::Ready(Ok(()));
        };
        let result = source.poll(cx);
        if matches!(result, Poll::Ready(Ok(())))
            && let Some(shared) = source.shared.upgrade()
        {
            shared.reap_governed_refresh();
        }
        result
    }
}
impl StorageAuthority {
    /// Same authority identity, material versions, coalescing and deadlines;
    /// only the original source of progress is different. This path never uses
    /// RefreshExecutor or a synchronous material-source fallback.
    pub async fn material_for_request_in(
        &self,
        now: Instant,
        deadline: Instant,
        request: ConnectorRequestContext,
    ) -> FileResult<AuthorityMaterial> {
        if request.io_admission().is_none() {
            return Err(FileError::invalid(
                "governed authority refresh requires the original request I/O source",
            ));
        }
        let (step, legacy) = self.decide_in(now, deadline, Some(&request));
        debug_assert!(legacy.is_none());
        let inflight = self.shared.lock_state().inflight.clone();
        if let Some(original) = inflight.as_ref().filter(|i| i.asynchronous.is_some()) {
            let phase = request.io_phase().ok_or_else(|| {
                FileError::invalid(
                    "governed authority acquisition requires its original admitted phase",
                )
            })?;
            let progress: Arc<dyn ConnectorIoProgressSource> =
                Arc::clone(original) as Arc<dyn ConnectorIoProgressSource>;
            phase
                .attach_original_progress(progress)
                .map_err(governed_file_error)?;
            // The first inline poll is optional progress, not ownership. Even
            // if this borrow is cancelled, the same original cell drives F.
            std::future::poll_fn(|cx| {
                let result = original.poll_original_progress(cx);
                match result {
                    Poll::Ready(Err(error)) => Poll::Ready(Err(governed_file_error(error))),
                    _ => Poll::Ready(Ok(())),
                }
            })
            .await?;
        }
        let result = match step {
            RequestStep::Done(result) => result,
            RequestStep::Wait { inflight, deadline } => {
                self.shared.await_refresh(inflight, now, deadline).await
            }
        };
        self.reap_governed_refresh();
        result
    }
    fn reap_governed_refresh(&self) {
        self.shared.reap_governed_refresh();
    }
}
impl AuthorityShared {
    fn reap_governed_refresh(&self) {
        // Move the original header out under its lock; all actual field Drop
        // (including the phase alias) is outside that lock.
        let retired = {
            let mut state = self.lock_state();
            if state
                .inflight
                .as_ref()
                .and_then(|i| i.asynchronous.as_ref())
                .is_some_and(GovernedRefresh::completed)
            {
                state.inflight.take()
            } else {
                None
            }
        };
        drop(retired);
    }
}

fn governed_file_error(error: ConnectorError) -> FileError {
    let kind = match error.kind() {
        ConnectorErrorKind::Cancelled => FileErrorKind::Cancelled,
        ConnectorErrorKind::ResourceExhausted => FileErrorKind::ResourceExhausted,
        _ => FileErrorKind::Internal,
    };
    FileError::with_source(kind, "governed authority source", error)
}
