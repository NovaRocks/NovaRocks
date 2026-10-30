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

//! Private original relay pairing; no listener, fixture wire, grant or Native authority.
use super::*;
use opensrv_mysql::{ClosingResponseLease, QueryResultWriter};
use std::future::poll_fn;

pub(crate) struct PressureRelayHook {
    scope: PressureScope,
    statement: StatementToken,
}

/// Keep the original protocol IO and original hook/admission failure as distinct owned sources.
pub(crate) fn preserve_refusal_hook_failure(original: io::Error, hook: io::Error) -> io::Error {
    io::Error::new(original.kind(), RefusalHookFailure { original, hook })
}
struct RefusalHookFailure {
    original: io::Error,
    hook: io::Error,
}
impl std::fmt::Debug for RefusalHookFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("pressure refusal failed; original_protocol_and_hook_sources_retained=true")
    }
}
impl std::fmt::Display for RefusalHookFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}
impl std::error::Error for RefusalHookFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.original)
    }
}
impl PressureOwner {
    /// Original successful bind inputs only; never Gate snapshots or a caller's expected DTO.
    pub(crate) fn bind_relay(
        &self,
        connection: ClientConnectionToken,
        expected_session: novarocks_query_application::session_control::SessionToken,
        statement: StatementToken,
        actual_sql_sha256: [u8; 32],
    ) -> io::Result<Option<PressureRelayHook>> {
        Ok(self
            .bind_statement(connection, expected_session, statement, actual_sql_sha256)?
            .map(|scope| PressureRelayHook { scope, statement }))
    }
    fn scope_for_original_writer(
        &self,
        connection: ClientConnectionToken,
    ) -> io::Result<Option<PressureScope>> {
        let _selection = self
            .core
            .selection
            .lock()
            .map_err(|_| self.core.fail(Failure::Poison))?;
        for index in 0..TARGETS {
            let state = self.core.state(index)?;
            if state.facts.handshake_connection_id != Some(connection.connection_id()) {
                continue;
            }
            self.core.check()?;
            if state.facts.phase == Phase::Armed {
                return Ok(None);
            } // Original handshake/auth still raw.
            if state.facts.phase != Phase::Bound
                || state.facts.connection != Some(connection)
                || state.facts.writer_attached
                || state.facts.statement.is_none()
            {
                return Err(self.core.fail(Failure::Identity));
            }
            return Ok(Some(PressureScope {
                core: Arc::clone(&self.core),
                slot: index,
            }));
        }
        Ok(None)
    }
    /// Reject a selected frozen statement that never reached its original streaming owner.
    pub(crate) fn reject_non_streaming(
        &self,
        connection: ClientConnectionToken,
        actual_sql_sha256: [u8; 32],
    ) -> io::Result<()> {
        for index in 0..TARGETS {
            let state = self.core.state(index)?;
            if state.facts.handshake_connection_id == Some(connection.connection_id()) {
                self.core.check()?;
                if state.facts.phase == Phase::Released
                    && state.facts.connection == Some(connection)
                    && actual_sql_sha256 != ORIGINAL_SQL_SHA256
                    && state.facts.cancel_receipt.is_some()
                {
                    return Ok(());
                }
                return Err(self.core.fail(Failure::Transition));
            }
        }
        Ok(())
    }
}
impl PressureRelayHook {
    pub(crate) fn begin_rows(&self, actual: FramingCursor) -> io::Result<()> {
        self.scope.begin_rows(self.statement, actual)
    }
    /// Only after select! dropped the actual original Data write future.
    pub(crate) fn record_cancel(&self, actual: FramingCursor) -> io::Result<()> {
        self.scope.observe_cancel(self.statement, actual)
    }
    pub(crate) fn fail(&self, reason: Failure) {
        let _ = self.scope.core.fail(reason);
    }
    pub(crate) fn record_capacity_refusal(
        &self,
        actual: FramingCursor,
        original: WorkError,
    ) -> io::Result<()> {
        self.scope
            .observe_capacity_refused(self.statement, actual, original)
    }
    /// Mint only while this exact original writer is live, after its one real capacity refusal.
    pub(crate) fn capacity_eof(&self, receipt: FramingCursor, original: io::Error) -> io::Error {
        let checked = (|| {
            self.scope.core.check()?;
            let mut state = self.scope.core.state(self.scope.slot)?;
            if self.scope.slot != CLOSING_TARGETS
                || state.facts.phase != Phase::CapacityRefused
                || state.facts.statement != Some(self.statement)
                || state.facts.cancel_receipt != Some(receipt)
                || !state.facts.writer_attached
                || state.facts.writer_destructor_returned
                || !matches!(state.refusal, Some(WorkError::Capacity(_)))
                || state.capacity_eof_minted
            {
                return Err(self.scope.core.fail(Failure::Identity));
            }
            let connection = state
                .facts
                .connection
                .ok_or_else(|| self.scope.core.fail(Failure::Identity))?;
            state.capacity_eof_minted = true;
            Ok(connection)
        })();
        match checked {
            Ok(connection) => io::Error::new(
                original.kind(),
                PressureCapacityEof {
                    core: Arc::clone(&self.scope.core),
                    connection,
                    statement: self.statement,
                    receipt,
                    original,
                },
            ),
            Err(hook) => preserve_refusal_hook_failure(original, hook),
        }
    }
    /// Invoke ONLY from inside the original timeout(CLOSING_DEADLINE, ...).
    /// The real ClosingDelivery stays owned in outer close_relay on every branch.
    pub(crate) async fn finish_original_closing<'slot, W: AsyncWrite + Unpin>(
        &self,
        actual: FramingCursor,
        closing: &mut ClosingDelivery<ClosingResponseLease<'slot, W>>,
    ) -> io::Result<QueryResultWriter<'slot, W>> {
        self.scope
            .observe_installed_closing(self.statement, actual, closing)?;
        let original = closing.writer_mut().finish();
        tokio::pin!(original);
        paired_finish(&self.scope, original).await
    }
}

/// Private same-returned-error classification. Identity conveys no resource rights.
pub(crate) struct PressureCapacityEof {
    core: Arc<Core>,
    connection: ClientConnectionToken,
    statement: StatementToken,
    receipt: FramingCursor,
    original: io::Error,
}
impl PressureCapacityEof {
    pub(crate) fn from_error(error: &io::Error) -> Option<&Self> {
        error.get_ref()?.downcast_ref()
    }
    pub(crate) fn matches(&self, connection: ClientConnectionToken) -> bool {
        if self.connection != connection || self.core.first_failure.load(Ordering::Acquire) != 0 {
            return false;
        }
        self.core.state(CLOSING_TARGETS).is_ok_and(|state| {
            state.capacity_eof_minted
                && state.facts.phase == Phase::CapacityRefused
                && state.facts.connection == Some(connection)
                && state.facts.statement == Some(self.statement)
                && state.facts.cancel_receipt == Some(self.receipt)
                && matches!(state.refusal, Some(WorkError::Capacity(_)))
        })
    }
    pub(crate) fn matches_controller(&self, controller: &PressureController) -> bool {
        Arc::ptr_eq(&self.core, &controller.core) && self.matches(self.connection)
    }
}
impl std::fmt::Debug for PressureCapacityEof {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("pressure original Closing capacity refusal retained")
    }
}
impl std::fmt::Display for PressureCapacityEof {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}
impl std::error::Error for PressureCapacityEof {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.original)
    }
}

/// A per-original-future-poll scalar witness, not an IO/grant/body alias.
struct ClosingPollGuard {
    core: Arc<Core>,
    slot: usize,
    before: u64,
}
impl ClosingPollGuard {
    fn enter(scope: &PressureScope) -> io::Result<Self> {
        scope.core.check()?;
        let mut state = scope.core.state(scope.slot)?;
        if !state.facts.real_closing_observed
            || !state.facts.writer_attached
            || state.facts.writer_destructor_returned
            || state.closing_poll_active
            || !matches!(state.facts.phase, Phase::ClosingHeld | Phase::Released)
        {
            return Err(scope.core.fail(Failure::Transition));
        }
        state.closing_poll_active = true;
        Ok(Self {
            core: Arc::clone(&scope.core),
            slot: scope.slot,
            before: state.facts.paired_closing_polls,
        })
    }
    fn check_pending(&self) -> io::Result<()> {
        let state = self.core.state(self.slot)?;
        if state.facts.phase == Phase::ClosingHeld
            && state.facts.paired_closing_polls <= self.before
        {
            return Err(self.core.fail(Failure::Identity));
        }
        if state.facts.phase == Phase::Released && state.facts.paired_closing_polls == 0 {
            return Err(self.core.fail(Failure::Identity));
        }
        self.core.check()
    }
    fn check_success(&self) -> io::Result<()> {
        let state = self.core.state(self.slot)?;
        if state.facts.phase != Phase::Released || state.facts.paired_closing_polls == 0 {
            return Err(self.core.fail(Failure::Identity));
        }
        self.core.check()
    }
}
impl Drop for ClosingPollGuard {
    fn drop(&mut self) {
        let mut state = self.core.slots[self.slot]
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.closing_poll_active = false;
        drop(state);
        if std::thread::panicking() {
            // Record only the finite cause. Unwinding retains the original
            // panic payload and never substitutes for the outer owner exit.
            let _ = self.core.fail(Failure::Panic);
        }
    }
}
/// Cancelling the borrowed original finish invalidates its held witness before
/// the outer ClosingDelivery can drop its grant. W destruction can occur later.
struct ClosingFutureGuard {
    core: Arc<Core>,
    completed: bool,
}
impl Drop for ClosingFutureGuard {
    fn drop(&mut self) {
        if !self.completed {
            let _ = self.core.fail(if std::thread::panicking() {
                Failure::Panic
            } else {
                Failure::Transition
            });
        }
    }
}
/// Production calls this only with the one original ClosingResponseLease::finish future.
/// No spawn/owned handle/clock is created. Cancel drops a borrowed future, not the outer ClosingDelivery.
async fn paired_finish<F, T>(scope: &PressureScope, mut original: Pin<&mut F>) -> io::Result<T>
where
    F: Future<Output = io::Result<T>>,
{
    let mut lifetime = ClosingFutureGuard {
        core: Arc::clone(&scope.core),
        completed: false,
    };
    let result = poll_fn(|cx| {
        let guard = match ClosingPollGuard::enter(scope) {
            Ok(guard) => guard,
            Err(error) => return Poll::Ready(Err(error)),
        };
        let polled = original.as_mut().poll(cx);
        match polled {
            Poll::Pending => match guard.check_pending() {
                Ok(()) => Poll::Pending,
                Err(error) => Poll::Ready(Err(error)),
            },
            Poll::Ready(Ok(value)) => match guard.check_success() {
                Ok(()) => Poll::Ready(Ok(value)),
                Err(error) => Poll::Ready(Err(error)),
            },
            Poll::Ready(Err(original)) => {
                // Preserve this original owned source; the scalar journal records only first cause.
                let _ = scope.core.fail(Failure::InnerIo);
                Poll::Ready(Err(original))
            }
        }
    })
    .await;
    lifetime.completed = result.is_ok();
    result
}

/// Construct at the original listener-owned W and exact original connection token.
/// Existing control/default writers stay raw; original attachment is move-only and occurs at metadata IO.
pub(crate) struct InitiallyRawPressureWriter<W> {
    original: Option<W>,
    bound: Option<ClosingPressureGate<W>>,
    connection: ClientConnectionToken,
    owner: Arc<PressureOwner>,
}
impl<W> InitiallyRawPressureWriter<W> {
    pub(crate) fn new(
        original: W,
        connection: ClientConnectionToken,
        owner: Arc<PressureOwner>,
    ) -> Self {
        Self {
            original: Some(original),
            bound: None,
            connection,
            owner,
        }
    }
    fn attach(&mut self) -> io::Result<()> {
        if self.bound.is_some() {
            return Ok(());
        }
        let Some(scope) = self.owner.scope_for_original_writer(self.connection)? else {
            return Ok(());
        };
        let original = self
            .original
            .take()
            .ok_or_else(|| self.owner.core.fail(Failure::Transition))?;
        self.bound = Some(ClosingPressureGate::new(original, &scope)?);
        Ok(())
    }
}
impl<W: AsyncWrite + Unpin> AsyncWrite for InitiallyRawPressureWriter<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if let Err(error) = this.attach() {
            return Poll::Ready(Err(error));
        }
        match (&mut this.bound, &mut this.original) {
            (Some(bound), _) => Pin::new(bound).poll_write(cx, bytes),
            (_, Some(original)) => Pin::new(original).poll_write(cx, bytes),
            _ => Poll::Ready(Err(this.owner.core.fail(Failure::Transition))),
        }
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        slices: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if let Err(error) = this.attach() {
            return Poll::Ready(Err(error));
        }
        match (&mut this.bound, &mut this.original) {
            (Some(bound), _) => Pin::new(bound).poll_write_vectored(cx, slices),
            (_, Some(original)) => Pin::new(original).poll_write_vectored(cx, slices),
            _ => Poll::Ready(Err(this.owner.core.fail(Failure::Transition))),
        }
    }
    fn is_write_vectored(&self) -> bool {
        match (&self.bound, &self.original) {
            (Some(bound), _) => bound.is_write_vectored(),
            (_, Some(original)) => original.is_write_vectored(),
            _ => false,
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if let Err(error) = this.attach() {
            return Poll::Ready(Err(error));
        }
        match (&mut this.bound, &mut this.original) {
            (Some(bound), _) => Pin::new(bound).poll_flush(cx),
            (_, Some(original)) => Pin::new(original).poll_flush(cx),
            _ => Poll::Ready(Err(this.owner.core.fail(Failure::Transition))),
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut(); // Never attach/reject shutdown on a failed/late diagnostic clock.
        match (&mut this.bound, &mut this.original) {
            (Some(bound), _) => Pin::new(bound).poll_shutdown(cx),
            (_, Some(original)) => Pin::new(original).poll_shutdown(cx),
            _ => Poll::Ready(Ok(())),
        }
    }
}
impl<W> Drop for InitiallyRawPressureWriter<W> {
    fn drop(&mut self) {
        drop(self.bound.take());
        drop(self.original.take());
    }
}

#[cfg(test)]
#[path = "relay_tests.rs"]
mod tests;
