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

//! Test-only late binding on the original socket. No listener or native caller yet.
//! A listener-owned slot attaches only when the real protocol caller binds its identity.

use super::*;
use novarocks_types::FrontendProcessId;

#[derive(Clone, Copy)]
struct Arm {
    connection_id: u32,
    sql_sha256: [u8; 32],
    cut: u64,
}
struct HubState {
    used_arm: bool,
    stopped: bool,
    failure: Option<GateFailure>,
    arm: Option<Arm>,
    scope: Option<Arc<MysqlWriteTestScope>>,
    original_writer_exited: bool,
    final_gate: Option<MysqlWriteGateSnapshot>,
    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    original_freeze: Option<super::original_freeze::OriginalFreezeScalars>,
}

/// One listener-owned slot. Per-connection aliases never allocate another scope.
pub(crate) struct MysqlWriteGateHub {
    #[cfg(all(novarocks_dev_controls, feature = "mem-1-m07-exact-mysql-write"))]
    action_observation:
        Mutex<Option<Arc<crate::test_fixture_authorization::FixtureActionObservation>>>,
    frontend: FrontendProcessId,
    nonce: [u8; 16],
    deadline: Instant,
    state: Mutex<HubState>,
}
/// The service owner retains this value. It deliberately does not implement Clone.
pub(crate) struct MysqlWriteGateController {
    hub: Arc<MysqlWriteGateHub>,
}
/// One hook moved through the original streaming delivery path; no payload aliases.
pub(crate) struct MysqlWriteRelayHook {
    hub: Arc<MysqlWriteGateHub>,
    scope: Arc<MysqlWriteTestScope>,
    statement: StatementToken,
}
#[derive(Clone, Copy)]
pub(crate) struct MysqlWriteHubSnapshot {
    pub frontend: FrontendProcessId,
    pub used_arm: bool,
    pub stopped: bool,
    pub failure: Option<GateFailure>,
    pub original_writer_exited: bool,
    pub gate: Option<MysqlWriteGateSnapshot>,
    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    pub original_freeze: Option<super::original_freeze::OriginalFreezeScalars>,
}

fn hub_fail(state: &mut HubState, reason: GateFailure) -> io::Error {
    let earlier = state
        .scope
        .as_ref()
        .and_then(|scope| scope.snapshot().failure)
        .unwrap_or(reason);
    error(*state.failure.get_or_insert(earlier))
}
impl MysqlWriteGateHub {
    #[cfg(all(novarocks_dev_controls, feature = "mem-1-m07-exact-mysql-write"))]
    pub(crate) fn install_action_observation(
        &self,
        observation: Arc<crate::test_fixture_authorization::FixtureActionObservation>,
    ) {
        *self
            .action_observation
            .lock()
            .expect("original fixture observation lock") = Some(observation);
    }
    #[cfg(novarocks_dev_controls)]
    fn observe_reached_cut(&self) {
        #[cfg(feature = "mem-1-m07-exact-mysql-write")]
        {
            let reached = self
                .state
                .lock()
                .expect("original hub lock")
                .scope
                .as_ref()
                .is_some_and(|scope| scope.snapshot().blocked_after_acceptance);
            if reached
                && let Some(observation) = self
                    .action_observation
                    .lock()
                    .expect("original observation lock")
                    .as_ref()
                && observation.record_hit().is_err()
            {
                self.fail(GateFailure::Transition);
            }
        }
    }

    pub(crate) fn new(
        actual_frontend: FrontendProcessId,
        nonce: [u8; 16],
        original_absolute_deadline: Instant,
    ) -> io::Result<(Arc<Self>, MysqlWriteGateController)> {
        if nonce == [0; 16] {
            return Err(error(GateFailure::Identity));
        }
        if Instant::now() >= original_absolute_deadline {
            return Err(error(GateFailure::Deadline));
        }
        let hub = Arc::new(Self {
            #[cfg(all(novarocks_dev_controls, feature = "mem-1-m07-exact-mysql-write"))]
            action_observation: Mutex::new(None),
            frontend: actual_frontend,
            nonce,
            deadline: original_absolute_deadline,
            state: Mutex::new(HubState {
                used_arm: false,
                stopped: false,
                failure: None,
                arm: None,
                scope: None,
                original_writer_exited: false,
                final_gate: None,
                #[cfg(feature = "mem-1-m07-exact-mysql-write")]
                original_freeze: None,
            }),
        });
        Ok((Arc::clone(&hub), MysqlWriteGateController { hub }))
    }
    // No hub lock is held during IO, scope Stop wakes or an inner poll.
    fn checked<T>(&self, operation: impl FnOnce(&mut HubState) -> io::Result<T>) -> io::Result<T> {
        let (result, stop) = {
            let mut state = self.state.lock().expect("MySQL fixture hub lock");
            if state.failure.is_none() {
                state.failure = state
                    .scope
                    .as_ref()
                    .and_then(|scope| scope.snapshot().failure);
            }
            let result = if let Some(reason) = state.failure {
                Err(error(reason))
            } else if state.stopped {
                Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "MySQL fixture hub stopped",
                ))
            } else if Instant::now() >= self.deadline {
                Err(hub_fail(&mut state, GateFailure::Deadline))
            } else {
                operation(&mut state)
            };
            let stop = if state.failure.is_some() {
                state.scope.as_ref().map(Arc::clone)
            } else {
                None
            };
            (result, stop)
        };
        if let Some(scope) = stop {
            scope.stop();
        }
        result
    }
    fn fail_scope(&self, scope: &MysqlWriteTestScope) {
        let Some(reason) = scope.snapshot().failure else {
            // Original inner IO errors remain the caller's errors, never a fabricated gate cause.
            return;
        };
        let stop = {
            let mut state = self.state.lock().expect("MySQL fixture hub lock");
            let _ = hub_fail(&mut state, reason);
            state.scope.as_ref().map(Arc::clone)
        };
        if let Some(scope) = stop {
            scope.stop();
        }
    }
    fn propagate_scope_failure(&self) {
        let stop = {
            let mut state = self.state.lock().expect("MySQL fixture hub lock");
            let reason = state
                .scope
                .as_ref()
                .and_then(|scope| scope.snapshot().failure);
            if let Some(reason) = reason {
                let _ = hub_fail(&mut state, reason);
                state.scope.as_ref().map(Arc::clone)
            } else {
                None
            }
        };
        if let Some(scope) = stop {
            scope.stop();
        }
    }
    /// Only the real ordinary, single-statement streaming caller invokes this.
    /// It passes its original connection token and the original protocol owner's token.
    pub(crate) fn bind_statement(
        self: &Arc<Self>,
        actual_connection: ClientConnectionToken,
        expected_session: novarocks_query_application::session_control::SessionToken,
        actual_statement: StatementToken,
        actual_sql_sha256: [u8; 32],
    ) -> io::Result<Option<MysqlWriteRelayHook>> {
        if !self.is_selected_connection(actual_connection) {
            return Ok(None);
        }
        let scope = self.checked(|state| {
            let Some(arm) = state.arm else {
                return Ok(None);
            };
            if arm.connection_id != actual_connection.connection_id() {
                return Ok(None);
            }
            if let Some(scope) = &state.scope {
                let facts = scope.snapshot();
                // Only the validated, locally released cancel cut permits follow-up SQL.
                if facts.connection != actual_connection {
                    return Err(hub_fail(state, GateFailure::Identity));
                }
                let original = facts
                    .statement
                    .expect("bound scope retains original statement");
                if actual_statement.session() != original.session()
                    || actual_statement.generation() <= original.generation()
                {
                    return Err(hub_fail(state, GateFailure::Identity));
                }
                if arm.sql_sha256 == actual_sql_sha256
                    || facts.phase != GatePhase::Resumed
                    || facts.cancel_receipt.is_none()
                    || facts.failure.is_some()
                {
                    return Err(hub_fail(state, GateFailure::Transition));
                }
                return Ok(None);
            }
            if arm.sql_sha256 != actual_sql_sha256
                || actual_statement.session() != expected_session
                || actual_statement.generation() == 0
            {
                return Err(hub_fail(state, GateFailure::Identity));
            }
            // The statement epoch is never equated with connection generation.
            let scope = Arc::new(MysqlWriteTestScope::new(actual_connection, self.deadline));
            state.scope = Some(Arc::clone(&scope));
            if let Err(original_error) = scope.arm(
                actual_statement,
                expected_session,
                actual_sql_sha256,
                arm.cut,
            ) {
                state
                    .failure
                    .get_or_insert(scope.snapshot().failure.unwrap_or(GateFailure::Transition));
                return Err(original_error);
            }
            Ok(Some(scope))
        })?;
        Ok(scope.map(|scope| MysqlWriteRelayHook {
            hub: Arc::clone(self),
            scope,
            statement: actual_statement,
        }))
    }
    /// Before a target returns Local/Error/Completion or an unsupported batch, fail it.
    pub(crate) fn reject_non_streaming_target(
        &self,
        actual_connection: ClientConnectionToken,
        actual_sql_sha256: [u8; 32],
    ) -> io::Result<()> {
        if !self.is_selected_connection(actual_connection) {
            return Ok(());
        }
        self.checked(|state| {
            let arm = state.arm.expect("selected connection retains arm");
            if let Some(scope) = &state.scope {
                let facts = scope.snapshot();
                if facts.connection != actual_connection {
                    return Err(hub_fail(state, GateFailure::Identity));
                }
                if arm.sql_sha256 != actual_sql_sha256
                    && facts.phase == GatePhase::Resumed
                    && facts.cancel_receipt.is_some()
                    && facts.failure.is_none()
                {
                    return Ok(());
                }
            }
            Err(hub_fail(state, GateFailure::Transition))
        })
    }
    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    pub(crate) fn reject_unsupported_batch(
        &self,
        actual_connection: ClientConnectionToken,
    ) -> io::Result<()> {
        if !self.is_selected_connection(actual_connection) {
            return Ok(());
        }
        self.checked(|state| Err(hub_fail(state, GateFailure::Transition)))
    }

    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    pub(crate) fn fail_selected(
        &self,
        actual_connection: ClientConnectionToken,
        reason: GateFailure,
    ) {
        if self.is_selected_connection(actual_connection) {
            self.fail(reason);
        }
    }

    pub(crate) fn is_selected_connection(&self, connection: ClientConnectionToken) -> bool {
        let state = self.state.lock().expect("MySQL fixture hub lock");
        state
            .arm
            .is_some_and(|arm| arm.connection_id == connection.connection_id())
    }
    fn scope_for_writer(
        &self,
        actual_connection: ClientConnectionToken,
    ) -> io::Result<Option<Arc<MysqlWriteTestScope>>> {
        // Unrelated connections retain original IO behavior even when the fixture fails.
        let selected = {
            let state = self.state.lock().expect("MySQL fixture hub lock");
            state
                .scope
                .as_ref()
                .is_some_and(|scope| scope.snapshot().connection == actual_connection)
        };
        if !selected {
            return Ok(None);
        }
        self.checked(|state| Ok(state.scope.as_ref().map(Arc::clone)))
    }
    fn writer_dropped(&self, connection: ClientConnectionToken) {
        let mut state = self.state.lock().expect("MySQL fixture hub lock");
        if let Some(scope) = &state.scope {
            if scope.snapshot().connection == connection {
                state.original_writer_exited = true;
            }
        } else if state
            .arm
            .is_some_and(|arm| arm.connection_id == connection.connection_id())
        {
            state.original_writer_exited = true;
            state.failure.get_or_insert(GateFailure::Transition);
        }
    }
    fn fail(&self, reason: GateFailure) {
        let scope = {
            let mut state = self.state.lock().expect("MySQL fixture hub lock");
            let _ = hub_fail(&mut state, reason);
            state.stopped = true;
            state.scope.as_ref().map(Arc::clone)
        };
        if let Some(scope) = scope {
            scope.stop()
        }
    }
    fn stop(&self) {
        let scope = {
            let mut state = self.state.lock().expect("MySQL fixture hub lock");
            state.stopped = true;
            state.scope.as_ref().map(Arc::clone)
        };
        if let Some(scope) = scope {
            scope.stop()
        }
    }
    fn snapshot(&self) -> MysqlWriteHubSnapshot {
        let state = self.state.lock().expect("MySQL fixture hub lock");
        MysqlWriteHubSnapshot {
            frontend: self.frontend,
            used_arm: state.used_arm,
            stopped: state.stopped,
            failure: state.failure.or_else(|| {
                state
                    .scope
                    .as_ref()
                    .and_then(|scope| scope.snapshot().failure)
            }),
            original_writer_exited: state.original_writer_exited,
            gate: state
                .scope
                .as_ref()
                .map(|scope| scope.snapshot())
                .or(state.final_gate),
            #[cfg(feature = "mem-1-m07-exact-mysql-write")]
            original_freeze: state.original_freeze,
        }
    }
}
impl MysqlWriteGateController {
    /// The original service owner validates and decodes bounded one-peer control input.
    pub(crate) fn arm(
        &mut self,
        frontend: FrontendProcessId,
        nonce: [u8; 16],
        handshake_connection_id: u32,
        sql_sha256: [u8; 32],
        cut: u64,
    ) -> io::Result<()> {
        self.hub.checked(|state| {
            if state.used_arm {
                return Err(hub_fail(state, GateFailure::Transition));
            }
            // Even a rejected arm consumes this listener's one attempt.
            state.used_arm = true;
            if frontend != self.hub.frontend
                || nonce != self.hub.nonce
                || handshake_connection_id == 0
            {
                return Err(hub_fail(state, GateFailure::Identity));
            }
            if cut == 0 || cut > MAX_CUT_BYTES {
                return Err(hub_fail(state, GateFailure::Length));
            }
            state.arm = Some(Arm {
                connection_id: handshake_connection_id,
                sql_sha256,
                cut,
            });
            Ok(())
        })
    }
    pub(crate) fn snapshot(&self) -> MysqlWriteHubSnapshot {
        self.hub.snapshot()
    }
    /// The service owner calls this on its original absolute timeout/error path.
    /// Snapshot is factual and does not itself poll or refresh a deadline.
    pub(crate) fn fail(&mut self, first_reason: GateFailure) {
        self.hub.fail(first_reason)
    }
    pub(crate) fn stop(&mut self) {
        self.hub.stop()
    }
    /// Call only after actually awaiting the original listener/session/watcher exit.
    /// Arc checks are additional facts; they cannot substitute for that actual await.
    /// This is cleanup, never an assertion that an unarmed/unbound experiment passed.
    pub(crate) fn finish_after_protocol_join(&mut self) -> io::Result<MysqlWriteHubSnapshot> {
        self.stop();
        let last = {
            let mut state = self.hub.state.lock().expect("MySQL fixture hub lock");
            if let Some(scope) = &state.scope {
                let facts = scope.snapshot();
                if !state.original_writer_exited
                    || (facts.writer_attached && !facts.writer_exited)
                    || Arc::strong_count(scope) != 1
                    || Arc::strong_count(&scope.core) != 1
                {
                    // Keep the original failure. Cleanup errors must also be retained by the service owner.
                    state.failure.get_or_insert(GateFailure::Transition);
                    return Err(error(GateFailure::Transition));
                }
                state.final_gate = Some(facts);
            }
            state.scope.take()
        };
        // Release the last scope/Core owner after copying only fixed facts.
        drop(last);
        Ok(self.hub.snapshot())
    }
}
impl Drop for MysqlWriteGateController {
    fn drop(&mut self) {
        self.hub.stop()
    }
}
#[cfg(feature = "mem-1-m07-exact-mysql-write")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PrescribedRelayEofKind {
    MissingResidentTail,
    ClosingAdmissionCapacityRefused,
}

/// An opaque provenance on this one returned error, never a connection-wide waiver.
/// Construction requires the original hook's exact accepted cancellation receipt.
#[cfg(feature = "mem-1-m07-exact-mysql-write")]
pub(crate) struct PrescribedRelayEof {
    kind: PrescribedRelayEofKind,
    connection: ClientConnectionToken,
    statement: StatementToken,
    receipt: FramingCursor,
    original: io::Error,
}
#[cfg(feature = "mem-1-m07-exact-mysql-write")]
impl PrescribedRelayEof {
    pub(crate) fn matches(
        &self,
        connection: ClientConnectionToken,
        statement: Option<StatementToken>,
    ) -> bool {
        self.connection == connection && statement.is_none_or(|token| self.statement == token)
    }
    pub(crate) fn matches_gate(&self, gate: &MysqlWriteGateSnapshot) -> bool {
        self.connection == gate.connection
            && Some(self.statement) == gate.statement
            && Some(self.receipt) == gate.cancel_receipt
            && gate.failure.is_none()
    }
    pub(crate) fn from_error(error: &io::Error) -> Option<&Self> {
        error.get_ref()?.downcast_ref()
    }
}
#[cfg(feature = "mem-1-m07-exact-mysql-write")]
impl std::fmt::Display for PrescribedRelayEof {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "prescribed relay EOF: kind={:?} connection_id={} generation={} receipt={:?}",
            self.kind,
            self.connection.connection_id(),
            self.connection.generation(),
            self.receipt
        )
    }
}
#[cfg(feature = "mem-1-m07-exact-mysql-write")]
impl std::fmt::Debug for PrescribedRelayEof {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
#[cfg(feature = "mem-1-m07-exact-mysql-write")]
impl std::error::Error for PrescribedRelayEof {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.original)
    }
}

impl MysqlWriteRelayHook {
    /// Copy-only observation after the ONE production freeze and original tail selection.
    /// checked keeps the established Hub -> Core order and Stop/wake on every refusal.
    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    pub(crate) fn record_original_freeze(
        &self,
        observed: io::Result<super::original_freeze::OriginalFreezeScalars>,
    ) {
        let Ok(observed) = observed else {
            self.hub.fail(GateFailure::Receipt);
            return;
        };
        let _ = self.hub.checked(|state| {
            let facts = self.scope.snapshot();
            if state.original_freeze.is_some()
                || !state
                    .scope
                    .as_ref()
                    .is_some_and(|scope| Arc::ptr_eq(scope, &self.scope))
                || facts.statement != Some(self.statement)
                || facts.phase != GatePhase::Resumed
                || facts.failure.is_some()
                || facts.cancel_receipt != Some(observed.framing)
                || facts.accepted_prefix_bytes != facts.cut_bytes
                || !facts.blocked_after_acceptance
            {
                return Err(hub_fail(state, GateFailure::Receipt));
            }
            state.original_freeze = Some(observed);
            Ok(())
        });
    }

    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    pub(crate) fn fail_fixture(&self, reason: GateFailure) {
        self.hub.fail(reason);
    }

    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    pub(crate) fn prescribed_eof(
        &self,
        kind: PrescribedRelayEofKind,
        actual_receipt: FramingCursor,
        original: io::Error,
    ) -> io::Error {
        let facts = self.scope.snapshot();
        let hub = self.hub.snapshot();
        if facts.statement != Some(self.statement)
            || facts.cancel_receipt != Some(actual_receipt)
            || facts.phase != GatePhase::Resumed
            || facts.failure.is_some()
            || hub.failure.is_some()
        {
            self.hub.fail(GateFailure::Receipt);
            return original;
        }
        io::Error::new(
            original.kind(),
            PrescribedRelayEof {
                kind,
                connection: facts.connection,
                statement: self.statement,
                receipt: actual_receipt,
                original,
            },
        )
    }

    /// Synchronous: after complete metadata, before any target row writes.
    pub(crate) fn begin_rows(&self, actual_receipt: FramingCursor) -> io::Result<()> {
        let result = self.scope.begin_rows(self.statement, actual_receipt);
        if result.is_err() {
            self.hub.fail_scope(&self.scope)
        }
        result
    }
    /// Synchronous: only after the real target write future dropped on KILL QUERY.
    /// Local release avoids a KILL-return/Closing-writer dependency cycle.
    pub(crate) fn record_cancel_and_resume(&self, actual_receipt: FramingCursor) -> io::Result<()> {
        let result = self
            .scope
            .record_cancel_receipt(self.statement, actual_receipt)
            .and_then(|()| self.scope.resume());
        if result.is_err() {
            self.hub.fail_scope(&self.scope)
        }
        result
    }
}

/// Feature-only wrapper installed before the handshake on the original OwnedWriteHalf.
/// The real listener must keep Control connections and the default build raw.
pub(crate) struct InitiallyRawMysqlWriter<W> {
    original: Option<W>,
    bound: Option<MysqlWriteGate<W>>,
    actual_connection: ClientConnectionToken,
    hub: Arc<MysqlWriteGateHub>,
}
impl<W> InitiallyRawMysqlWriter<W> {
    pub(crate) fn new(
        original: W,
        actual_connection: ClientConnectionToken,
        hub: Arc<MysqlWriteGateHub>,
    ) -> Self {
        Self {
            original: Some(original),
            bound: None,
            actual_connection,
            hub,
        }
    }
    fn attach(&mut self) -> io::Result<()> {
        if self.bound.is_some() {
            return Ok(());
        }
        let Some(scope) = self.hub.scope_for_writer(self.actual_connection)? else {
            return Ok(());
        };
        let original = self
            .original
            .take()
            .ok_or_else(|| error(GateFailure::Transition))?;
        match MysqlWriteGate::new(original, &scope) {
            Ok(bound) => {
                self.bound = Some(bound);
                Ok(())
            }
            Err(original_error) => {
                self.hub.fail_scope(&scope);
                Err(original_error)
            }
        }
    }
}
impl<W: AsyncWrite + Unpin> AsyncWrite for InitiallyRawMysqlWriter<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if let Err(error) = this.attach() {
            return Poll::Ready(Err(error));
        }
        let result = match (&mut this.bound, &mut this.original) {
            (Some(bound), _) => Pin::new(bound).poll_write(cx, bytes),
            (_, Some(original)) => Pin::new(original).poll_write(cx, bytes),
            _ => Poll::Ready(Err(error(GateFailure::Transition))),
        };
        #[cfg(novarocks_dev_controls)]
        this.hub.observe_reached_cut();
        this.hub.propagate_scope_failure();
        result
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if let Err(error) = this.attach() {
            return Poll::Ready(Err(error));
        }
        let result = match (&mut this.bound, &mut this.original) {
            (Some(bound), _) => Pin::new(bound).poll_write_vectored(cx, bytes),
            (_, Some(original)) => Pin::new(original).poll_write_vectored(cx, bytes),
            _ => Poll::Ready(Err(error(GateFailure::Transition))),
        };
        #[cfg(novarocks_dev_controls)]
        this.hub.observe_reached_cut();
        this.hub.propagate_scope_failure();
        result
    }
    fn is_write_vectored(&self) -> bool {
        if let Some(bound) = &self.bound {
            bound.is_write_vectored()
        } else {
            self.original
                .as_ref()
                .is_some_and(|original| original.is_write_vectored())
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if let Err(error) = this.attach() {
            return Poll::Ready(Err(error));
        }
        let result = match (&mut this.bound, &mut this.original) {
            (Some(bound), _) => Pin::new(bound).poll_flush(cx),
            (_, Some(original)) => Pin::new(original).poll_flush(cx),
            _ => Poll::Ready(Err(error(GateFailure::Transition))),
        };
        #[cfg(novarocks_dev_controls)]
        this.hub.observe_reached_cut();
        this.hub.propagate_scope_failure();
        result
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        // Teardown does not attach a new gate or reject physical shutdown on a failed clock.
        match (&mut this.bound, &mut this.original) {
            (Some(bound), _) => Pin::new(bound).poll_shutdown(cx),
            (_, Some(original)) => Pin::new(original).poll_shutdown(cx),
            _ => Poll::Ready(Ok(())),
        }
    }
}
impl<W> Drop for InitiallyRawMysqlWriter<W> {
    fn drop(&mut self) {
        drop(self.bound.take());
        drop(self.original.take());
        // Facts are published only after the actual original IO destructor returned.
        self.hub.writer_dropped(self.actual_connection);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_query_application::session_control::SessionToken;
    use opensrv_mysql::{FrozenMetadata, OwnedStreamingMysqlWriter, ProtocolLimits};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::oneshot;
    use tokio::task::JoinSet;
    #[cfg(all(unix, novarocks_dev_controls, feature = "mem-1-m07-exact-mysql-write"))]
    #[tokio::test]
    async fn explicit_grant_hit_is_the_actual_write_cut_and_not_fixture_construction() {
        use crate::test_fixture_authorization::{
            FixtureActionObservation, MysqlFixtureAuthorization,
        };
        use novarocks_failpoint::{
            liveness::{PreparedRoleSession, RunnerSession},
            scope::ProcessBinding,
            session::{DECLARATION_VERSION, DomainGrant, SessionDeclaration, SessionRole},
        };
        use std::os::unix::fs::DirBuilderExt;
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let root = std::path::PathBuf::from(format!(
            "/tmp/g2hit-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&root)
            .unwrap();
        let declaration = SessionDeclaration {
            schema_version: DECLARATION_VERSION,
            session_id: "cut-session".into(),
            launch_id: "cut-launch".into(),
            role: SessionRole::Fe,
            lifetime_millis: 10_000,
            liveness_socket: root.join("live.sock"),
            handshake_secret: "abcdefghijklmnopqrstuvwxyz0123456789".into(),
            domain_grants: vec![DomainGrant {
                domain: MysqlFixtureAuthorization::DOMAIN.into(),
                grant_id: "one-cut".into(),
                parameters: serde_json::json!({"action":"exact-write","socket":root.join("fixture.sock"),"nonce_hex":"09090909090909090909090909090909"}),
            }],
        };
        let path = root.join("declaration.json");
        declaration.write_private(&path).unwrap();
        let prepared = PreparedRoleSession::read_and_claim(&path, SessionRole::Fe).unwrap();
        let mut runner = RunnerSession::start(declaration).unwrap();
        let frontend = FrontendProcessId::new_v7();
        let mut role = prepared.bind(ProcessBinding::Frontend(frontend)).unwrap();
        let authorization = MysqlFixtureAuthorization::from_grants(
            role.scope().grants_for(MysqlFixtureAuthorization::DOMAIN),
        )
        .unwrap()
        .unwrap();
        let (_, nonce, cancellation, claim) = authorization.claim("exact-write", frontend).unwrap();
        let observation = FixtureActionObservation::new(claim);
        let (hub, mut controller) =
            MysqlWriteGateHub::new(frontend, nonce, cancellation.deadline()).unwrap();
        hub.install_action_observation(Arc::clone(&observation));
        assert!(!role.scope().observations().iter().any(|o| o.event == "hit"));
        let connection = ClientConnectionToken::new(71, 19).unwrap();
        let statement = StatementToken::new(
            SessionToken::new(
                novarocks_query_application::session_control::ClientSessionKey::new(
                    (71_u64 << 32) | 23_u64,
                )
                .expect("nonzero fixture session"),
            ),
            30,
        );
        controller.arm(frontend, nonce, 71, SQL, 2).unwrap();
        let hook = hub
            .bind_statement(connection, statement.session(), statement, SQL)
            .unwrap()
            .unwrap();
        let raw = InitiallyRawMysqlWriter::new(Vec::new(), connection, Arc::clone(&hub));
        let mut writer = OwnedStreamingMysqlWriter::new(raw, ProtocolLimits::default(), 1).unwrap();
        writer.start_metadata(metadata().unwrap()).unwrap();
        writer.finish_metadata().await.unwrap();
        hook.begin_rows(writer.receipt()).unwrap();
        assert!(!role.scope().observations().iter().any(|o| o.event == "hit"));
        writer.start_row(4).unwrap();
        writer.push_slice(b"\x03abc").unwrap();
        {
            let flushing = writer.flush_pending();
            tokio::pin!(flushing);
            tokio::select! {result=&mut flushing=>panic!("cut must hold: {result:?}"), blocked=hook.scope.wait_blocked()=>{blocked.unwrap();}}
        }
        assert_eq!(
            role.scope()
                .observations()
                .iter()
                .filter(|o| o.event == "hit")
                .count(),
            1
        );
        hook.record_cancel_and_resume(writer.receipt()).unwrap();
        writer.flush_pending().await.unwrap();
        drop(writer);
        drop(hook);
        controller.stop();
        assert!(controller.snapshot().original_writer_exited);
        drop(controller);
        drop(hub);
        drop(observation);
        assert_eq!(
            role.scope()
                .observations()
                .iter()
                .filter(|o| o.event == "claim-released")
                .count(),
            1
        );
        role.close_and_join().unwrap();
        runner.end_and_join().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
    #[derive(Debug)]
    struct FixtureExitError {
        primary: Option<io::Error>,
        first_cleanup: io::Error,
        cleanup_failures: usize,
    }
    impl std::fmt::Display for FixtureExitError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            if let Some(primary) = &self.primary {
                write!(f, "fixture primary: {primary}; ")?;
            }
            write!(
                f,
                "fixture cleanup (failures={}): {}",
                self.cleanup_failures, self.first_cleanup
            )
        }
    }
    impl std::error::Error for FixtureExitError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(self.primary.as_ref().unwrap_or(&self.first_cleanup))
        }
    }
    fn fixture_panic(payload: &(dyn std::any::Any + Send)) -> io::Error {
        let text = payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| payload.downcast_ref::<&str>().copied());
        let (class, text) = match text {
            Some(text) => ("string", text),
            None => ("non-string", "non-string panic payload"),
        };
        io::Error::other(format!(
            "fixture panic (class={class}, bytes={}, sha256={:x})",
            text.len(),
            Sha256::digest(text.as_bytes())
        ))
    }
    async fn fixture_work<F: Future<Output = io::Result<()>>>(work: F) -> io::Result<()> {
        tokio::pin!(work);
        std::future::poll_fn(|cx| {
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| work.as_mut().poll(cx)))
            {
                Ok(polled) => polled,
                Err(payload) => Poll::Ready(Err(fixture_panic(payload.as_ref()))),
            }
        })
        .await
    }

    async fn finish_fixture(
        controller: &mut MysqlWriteGateController,
        children: &mut JoinSet<io::Result<()>>,
        primary: io::Result<()>,
    ) -> io::Result<()> {
        controller.stop();
        children.abort_all();
        let mut first_cleanup = None;
        let mut cleanup_failures = 0usize;
        while let Some(joined) = children.join_next().await {
            let failure = match joined {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error),
                Err(error) if error.is_cancelled() => None,
                Err(error) => Some(io::Error::other(error)),
            };
            if let Some(error) = failure {
                cleanup_failures += 1;
                first_cleanup.get_or_insert(error);
            }
        }
        // Only after the unique parent reaped every original TCP child handle.
        if let Err(error) = controller.finish_after_protocol_join() {
            cleanup_failures += 1;
            first_cleanup.get_or_insert(error);
        }
        if let Some(first_cleanup) = first_cleanup {
            let kind = primary
                .as_ref()
                .err()
                .map_or(first_cleanup.kind(), io::Error::kind);
            Err(io::Error::new(
                kind,
                FixtureExitError {
                    primary: primary.err(),
                    first_cleanup,
                    cleanup_failures,
                },
            ))
        } else {
            primary
        }
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Case {
        Healthy,
        WrongGeneration,
        EarlyStreaming,
        EarlyLocal,
        ParentTimeout,
        ParentPanic,
        ChildPanic,
        StaleStatement,
        OlderStatement,
        WrongSession,
        WrongSessionEpoch,
        ZeroStatement,
        ZeroEpoch,
    }
    impl Case {
        fn has_rows(self) -> bool {
            !matches!(
                self,
                Self::WrongGeneration | Self::EarlyStreaming | Self::EarlyLocal
            )
        }
        fn resumes(self) -> bool {
            matches!(
                self,
                Self::Healthy
                    | Self::StaleStatement
                    | Self::OlderStatement
                    | Self::WrongSession
                    | Self::WrongSessionEpoch
                    | Self::ZeroStatement
                    | Self::ZeroEpoch
            )
        }
        fn invalid_followup(self) -> Option<StatementToken> {
            match self {
                Self::StaleStatement => Some(identities().1),
                Self::OlderStatement => Some(StatementToken::new(
                    SessionToken::new(
                        novarocks_query_application::session_control::ClientSessionKey::new(
                            (71_u64 << 32) | 23_u64,
                        )
                        .expect("nonzero fixture session"),
                    ),
                    28,
                )),
                Self::WrongSession => Some(StatementToken::new(
                    SessionToken::new(
                        novarocks_query_application::session_control::ClientSessionKey::new(
                            (72_u64 << 32) | 23_u64,
                        )
                        .expect("nonzero fixture session"),
                    ),
                    30,
                )),
                Self::WrongSessionEpoch => Some(StatementToken::new(
                    SessionToken::new(
                        novarocks_query_application::session_control::ClientSessionKey::new(
                            (71_u64 << 32) | 24_u64,
                        )
                        .expect("nonzero fixture session"),
                    ),
                    30,
                )),
                Self::ZeroStatement => Some(StatementToken::new(
                    SessionToken::new(
                        novarocks_query_application::session_control::ClientSessionKey::new(
                            (71_u64 << 32) | 23_u64,
                        )
                        .expect("nonzero fixture session"),
                    ),
                    0,
                )),
                Self::ZeroEpoch => Some(StatementToken::new(
                    SessionToken::new(
                        novarocks_query_application::session_control::ClientSessionKey::new(
                            71_u64 << 32,
                        )
                        .expect("nonzero fixture session"),
                    ),
                    30,
                )),
                _ => None,
            }
        }
    }
    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    #[tokio::test]
    async fn prescribed_eof_mint_refuses_wrong_receipt_statement_phase_and_prior_failure() {
        for case in 0..4 {
            let connection = ClientConnectionToken::new(71, 19).unwrap();
            let original_statement = StatementToken::new(
                SessionToken::new(
                    novarocks_query_application::session_control::ClientSessionKey::new(
                        (71_u64 << 32) | 23_u64,
                    )
                    .expect("nonzero fixture session"),
                ),
                30,
            );
            let (hub, mut controller) = MysqlWriteGateHub::new(
                FrontendProcessId::new_v7(),
                NONCE,
                Instant::now() + Duration::from_secs(3),
            )
            .unwrap();
            controller
                .arm(controller.snapshot().frontend, NONCE, 71, SQL, 2)
                .unwrap();
            let mut hook = hub
                .bind_statement(
                    connection,
                    original_statement.session(),
                    original_statement,
                    SQL,
                )
                .unwrap()
                .unwrap();
            let original = InitiallyRawMysqlWriter::new(Vec::new(), connection, hub.clone());
            let mut writer =
                OwnedStreamingMysqlWriter::new(original, ProtocolLimits::default(), 1).unwrap();
            writer.start_metadata(metadata().unwrap()).unwrap();
            writer.finish_metadata().await.unwrap();
            hook.begin_rows(writer.receipt()).unwrap();
            writer.start_row(4).unwrap();
            writer.push_slice(b"\x03abc").unwrap();
            {
                let flushing = writer.flush_pending();
                tokio::pin!(flushing);
                tokio::select! {
                    result=&mut flushing => panic!("cut must block: {result:?}"),
                    blocked=hook.scope.wait_blocked() => { blocked.unwrap(); },
                }
            }
            let mut receipt = writer.receipt();
            if case != 0 {
                hook.record_cancel_and_resume(receipt).unwrap();
            }
            match case {
                1 => receipt.rows_completed += 1,
                2 => hook.statement = StatementToken::new(original_statement.session(), 31),
                3 => hook.fail_fixture(GateFailure::Identity),
                _ => {}
            }
            let returned = hook.prescribed_eof(
                PrescribedRelayEofKind::MissingResidentTail,
                receipt,
                io::Error::from_raw_os_error(5),
            );
            assert!(PrescribedRelayEof::from_error(&returned).is_none());
            assert_eq!(
                returned.raw_os_error(),
                Some(5),
                "original IO source stays intact"
            );
            assert_eq!(
                controller.snapshot().failure,
                Some(if case == 3 {
                    GateFailure::Identity
                } else {
                    GateFailure::Receipt
                })
            );
            drop(writer);
            drop(hook);
            controller.stop();
            controller.finish_after_protocol_join().unwrap();
        }
    }

    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    #[tokio::test]
    async fn original_freeze_observation_refuses_duplicate_wrong_receipt_phase_and_prior_failure() {
        for case in 0..4 {
            let connection = ClientConnectionToken::new(71, 19).unwrap();
            let statement = StatementToken::new(
                SessionToken::new(
                    novarocks_query_application::session_control::ClientSessionKey::new(
                        (71_u64 << 32) | 23_u64,
                    )
                    .expect("nonzero fixture session"),
                ),
                30,
            );
            let (hub, mut controller) = MysqlWriteGateHub::new(
                FrontendProcessId::new_v7(),
                NONCE,
                Instant::now() + Duration::from_secs(3),
            )
            .unwrap();
            controller
                .arm(controller.snapshot().frontend, NONCE, 71, SQL, 2)
                .unwrap();
            let hook = hub
                .bind_statement(connection, statement.session(), statement, SQL)
                .unwrap()
                .unwrap();
            let original = InitiallyRawMysqlWriter::new(Vec::new(), connection, hub.clone());
            let mut writer =
                OwnedStreamingMysqlWriter::new(original, ProtocolLimits::default(), 1).unwrap();
            writer.start_metadata(metadata().unwrap()).unwrap();
            writer.finish_metadata().await.unwrap();
            hook.begin_rows(writer.receipt()).unwrap();
            writer.start_row(4).unwrap();
            writer.push_slice(b"\x03abc").unwrap();
            {
                let flushing = writer.flush_pending();
                tokio::pin!(flushing);
                tokio::select! {
                    result=&mut flushing => panic!("cut must block: {result:?}"),
                    blocked=hook.scope.wait_blocked() => {blocked.unwrap();},
                }
            }
            let mut receipt = writer.receipt();
            if case != 0 {
                hook.record_cancel_and_resume(receipt).unwrap();
            }
            let observed = super::super::original_freeze::OriginalFreezeScalars::capture(
                false,
                &[None, None],
                None,
                receipt,
                0,
                None,
                None,
                None,
            )
            .unwrap();
            if case == 1 {
                receipt.rows_completed += 1;
            }
            if case == 2 {
                hook.record_original_freeze(Ok(observed));
                assert!(controller.snapshot().original_freeze.is_some());
            }
            if case == 3 {
                hook.fail_fixture(GateFailure::Identity);
            }
            let mut supplied = observed;
            supplied.framing = receipt;
            hook.record_original_freeze(Ok(supplied));
            assert_eq!(
                controller.snapshot().failure,
                Some(if case == 3 {
                    GateFailure::Identity
                } else {
                    GateFailure::Receipt
                })
            );
            assert_eq!(controller.snapshot().original_freeze.is_some(), case == 2);
            assert_eq!(
                hook.scope.snapshot().phase,
                GatePhase::Stopped,
                "refusal uses original Stop/wake path"
            );
            drop(writer);
            drop(hook);
            controller.stop();
            controller.finish_after_protocol_join().unwrap();
        }
    }

    const SQL: [u8; 32] = [7; 32];
    const HEALTHY_SQL: [u8; 32] = [8; 32];
    const NONCE: [u8; 16] = [9; 16];
    const COLUMN: &[u8] = &[
        3, b'd', b'e', b'f', 0, 0, 0, 7, b'p', b'a', b'y', b'l', b'o', b'a', b'd', 0, 12, 45, 0,
        64, 0, 0, 0, 253, 0, 0, 0, 0, 0,
    ];
    const EOF: &[u8] = &[0xfe, 0, 0, 2, 0];
    fn append_packet(out: &mut Vec<u8>, sequence: u8, payload: &[u8]) {
        // All fixture payloads are frozen and the total below is less than 512 B.
        assert!(payload.len() <= 128 && out.len() + payload.len() + 4 <= 512);
        out.extend_from_slice(&[payload.len() as u8, 0, 0, sequence]);
        out.extend_from_slice(payload);
    }
    fn greeting() -> Vec<u8> {
        // Frozen HandshakeV10 server greeting. This test does not authenticate a FE session.
        let mut payload = Vec::with_capacity(96);
        payload.extend_from_slice(b"\x0agate-fixture-0\0");
        payload.extend_from_slice(&71u32.to_le_bytes());
        payload.extend_from_slice(b"12345678\0");
        payload.extend_from_slice(&0x8201u16.to_le_bytes());
        payload.push(45);
        payload.extend_from_slice(&2u16.to_le_bytes());
        payload.extend_from_slice(&8u16.to_le_bytes());
        payload.push(21);
        payload.extend_from_slice(&[0; 10]);
        payload.extend_from_slice(b"abcdefghijkl\0mysql_native_password\0");
        let mut wire = Vec::with_capacity(128);
        append_packet(&mut wire, 0, &payload);
        wire
    }
    fn metadata() -> io::Result<FrozenMetadata> {
        FrozenMetadata::new(
            vec![
                Arc::from([1u8].as_slice()),
                Arc::from(COLUMN),
                Arc::from(EOF),
            ],
            ProtocolLimits::default(),
        )
    }
    fn metadata_wire() -> Vec<u8> {
        let mut out = Vec::with_capacity(64);
        append_packet(&mut out, 1, &[1]);
        append_packet(&mut out, 2, COLUMN);
        append_packet(&mut out, 3, EOF);
        out
    }
    fn identities() -> (ClientConnectionToken, StatementToken) {
        (
            ClientConnectionToken::new(71, 19).unwrap(),
            StatementToken::new(
                SessionToken::new(
                    novarocks_query_application::session_control::ClientSessionKey::new(
                        (71_u64 << 32) | 23_u64,
                    )
                    .expect("nonzero fixture session"),
                ),
                29,
            ),
        )
    }
    fn followup_statement() -> StatementToken {
        StatementToken::new(
            SessionToken::new(
                novarocks_query_application::session_control::ClientSessionKey::new(
                    (71_u64 << 32) | 23_u64,
                )
                .expect("nonzero fixture session"),
            ),
            30,
        )
    }
    async fn pair() -> io::Result<(TcpStream, tokio::net::tcp::OwnedWriteHalf)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let (client, accepted) = tokio::try_join!(
            TcpStream::connect(listener.local_addr()?),
            listener.accept()
        )?;
        drop(listener);
        let (server, _) = accepted;
        let (read, write) = server.into_split();
        drop(read);
        Ok((client, write))
    }
    struct Wire {
        bytes: [u8; 512],
        length: usize,
        sha256: [u8; 32],
    }
    #[derive(Clone, Copy)]
    struct ServerFacts {
        baseline: FramingCursor,
        cancel: Option<FramingCursor>,
        hub: MysqlWriteHubSnapshot,
    }
    async fn tcp_case(case: Case) -> io::Result<()> {
        let (connection, statement) = identities();
        let frontend = FrontendProcessId::new_v7();
        // Component-only deadline; no native/production clock is modified.
        let deadline = Instant::now() + Duration::from_secs(5);
        let (hub, mut controller) = MysqlWriteGateHub::new(frontend, NONCE, deadline)?;
        let mut children: JoinSet<io::Result<()>> = JoinSet::new();
        let work = fixture_work(async {
            let (mut client, original) = pair().await?;
            let (handshake_tx, handshake_rx) = oneshot::channel();
            let (arm_tx, arm_rx) = oneshot::channel();
            let (facts_tx, facts_rx) = oneshot::channel();
            let (wire_tx, wire_rx) = oneshot::channel();
            let (prefix_tx, prefix_rx) = oneshot::channel();
            let writer_hub = Arc::clone(&hub);
            children.spawn(async move {
                let mut raw = InitiallyRawMysqlWriter::new(original, connection, Arc::clone(&writer_hub));
                raw.write_all(&greeting()).await?;
                assert!(raw.bound.is_none());
                arm_rx.await.map_err(io::Error::other)?;
                let hook = writer_hub.bind_statement(connection, statement.session(), statement, SQL)?.expect("one real fixture owner");
                assert!(!hook.scope.snapshot().writer_attached);
                let mut writer = OwnedStreamingMysqlWriter::new(raw, ProtocolLimits::default(), 1)?;
                writer.start_metadata(metadata()?)?;
                assert!(!hook.scope.snapshot().writer_attached);
                writer.finish_metadata().await?;
                let baseline = writer.receipt();
                assert!(hook.scope.snapshot().writer_attached);
                assert_eq!(baseline.phase, WritePhase::Boundary);
                assert_eq!(baseline.rows_completed, 0);
                assert_eq!(baseline.committed_wire_bytes, metadata_wire().len() as u64);
                assert_eq!(hook.scope.snapshot().accepted_prefix_bytes, 0);
                if !case.has_rows() {
                    let failure = match case {
                        Case::WrongGeneration => writer_hub.bind_statement(ClientConnectionToken::new(71, 20).unwrap(), followup_statement().session(), followup_statement(), HEALTHY_SQL).err().expect("same id, different original generation is refused"),
                        Case::EarlyStreaming => writer_hub.bind_statement(connection, followup_statement().session(), followup_statement(), HEALTHY_SQL)
                            .err().expect("early streaming query is refused"),
                        Case::EarlyLocal => writer_hub.reject_non_streaming_target(connection, HEALTHY_SQL)
                            .expect_err("early local query is refused"),
                        _ => unreachable!(),
                    };
                    assert_eq!(failure.kind(), io::ErrorKind::InvalidData);
                    facts_tx.send(ServerFacts { baseline, cancel: None, hub: writer_hub.snapshot() })
                        .map_err(|_| io::Error::other("fixture facts receiver closed"))?;
                    drop(writer);
                    drop(hook);
                    return Ok(());
                }
                hook.begin_rows(baseline)?;
                writer.start_row(4)?;
                assert_eq!(writer.push_slice(b"\x03abc")?, 4);
                {
                    let flushing = writer.flush_pending();
                    tokio::pin!(flushing);
                    tokio::select! {
                        biased;
                        result = &mut flushing => panic!("target completed before exact cut: {result:?}"),
                        blocked = hook.scope.wait_blocked() => { blocked?; }
                    }
                } // Real flushing future has dropped; receipt updates cannot race this cut.
                let cancel = writer.receipt();
                assert_eq!(cancel.committed_wire_bytes - baseline.committed_wire_bytes, 2);
                assert_eq!(cancel.header_written, 2);
                assert_eq!(cancel.phase, WritePhase::Row);
                if !case.resumes() {
                    facts_tx.send(ServerFacts { baseline, cancel: Some(cancel), hub: writer_hub.snapshot() })
                        .map_err(|_| io::Error::other("fixture facts receiver closed"))?;
                    if case == Case::ChildPanic { panic!("intentional actual TCP late-binding child panic") }
                    std::future::pending::<()>().await;
                    unreachable!();
                }
                // A component models this call boundary; it does not claim actual FE KILL proof.
                hook.record_cancel_and_resume(cancel)?;
                let released = writer_hub.snapshot();
                let facts = released.gate.expect("bound gate");
                assert_eq!(facts.phase, GatePhase::Resumed);
                assert_eq!(facts.cancel_receipt, Some(cancel));
                assert_eq!(facts.accepted_prefix_bytes, 2);
                assert_eq!(facts.accepted_prefix_sha256, <[u8; 32]>::from(Sha256::digest([4, 0])));
                assert!(facts.vectored_inner_polls > 0);
                writer.flush_pending().await?;
                writer.flush_socket().await?;
                assert_eq!(writer.receipt().rows_completed, 1);
                if let Some(invalid_statement) = case.invalid_followup() {
                    assert_eq!(writer_hub.bind_statement(connection, invalid_statement.session(), invalid_statement, HEALTHY_SQL)
                        .err().expect("a different SQL digest cannot validate a stale or foreign statement").kind(), io::ErrorKind::InvalidData);
                    assert_eq!(writer_hub.snapshot().failure, Some(GateFailure::Identity));
                    facts_tx.send(ServerFacts { baseline, cancel: Some(cancel), hub: writer_hub.snapshot() })
                        .map_err(|_| io::Error::other("fixture facts receiver closed"))?;
                    drop(writer);
                    drop(hook);
                    return Ok(());
                }
                // A legitimate newer generation need not be the immediately adjacent one.
                assert!(writer_hub.bind_statement(connection, StatementToken::new(statement.session(), 31).session(), StatementToken::new(statement.session(), 31), HEALTHY_SQL)?.is_none());
                writer_hub.reject_non_streaming_target(connection, HEALTHY_SQL)?;
                // Same original TCP half and same scope; reset only the new statement's framing cursor.
                let same_io = writer.into_inner();
                let mut followup = OwnedStreamingMysqlWriter::new(same_io, ProtocolLimits::default(), 1)?;
                followup.start_metadata(metadata()?)?;
                followup.finish_metadata().await?;
                followup.start_row(4)?;
                assert_eq!(followup.push_slice(b"\x03abc")?, 4);
                followup.flush_pending().await?;
                followup.flush_socket().await?;
                assert_eq!(hook.scope.snapshot().accepted_prefix_bytes, 2);
                facts_tx.send(ServerFacts { baseline, cancel: Some(cancel), hub: released })
                    .map_err(|_| io::Error::other("fixture facts receiver closed"))?;
                drop(followup);
                drop(hook);
                Ok(())
            });
            children.spawn(async move {
                let greeting = greeting();
                let mut observed = [0; 512];
                client.read_exact(&mut observed[..greeting.len()]).await?;
                assert_eq!(&observed[..greeting.len()], greeting.as_slice());
                let connection_offset = 4 + b"\x0agate-fixture-0\0".len();
                let id = u32::from_le_bytes(
                    observed[connection_offset..connection_offset + 4]
                        .try_into()
                        .unwrap(),
                );
                handshake_tx
                    .send(id)
                    .map_err(|_| io::Error::other("fixture handshake receiver closed"))?;
                let mut length = greeting.len();
                let prefix_at = greeting.len() + metadata_wire().len() + 2;
                let mut prefix_tx = Some(prefix_tx);
                loop {
                    if length == observed.len() {
                        return Err(io::Error::other("fixture actual wire exceeded fixed bound"));
                    }
                    let n = client.read(&mut observed[length..]).await?;
                    if n == 0 {
                        break;
                    }
                    length += n;
                    if case.has_rows()
                        && length >= prefix_at
                        && let Some(sender) = prefix_tx.take()
                    {
                        let prefix = [observed[prefix_at - 2], observed[prefix_at - 1]];
                        sender
                            .send(prefix)
                            .map_err(|_| io::Error::other("fixture prefix receiver closed"))?;
                    }
                }
                wire_tx
                    .send(Wire {
                        bytes: observed,
                        length,
                        sha256: Sha256::digest(&observed[..length]).into(),
                    })
                    .map_err(|_| io::Error::other("fixture wire receiver closed"))?;
                Ok(())
            });
            let actual_handshake_id = handshake_rx.await.map_err(io::Error::other)?;
            assert_eq!(actual_handshake_id, connection.connection_id());
            assert!(hub.snapshot().gate.is_none());
            controller.arm(frontend, NONCE, actual_handshake_id, SQL, 2)?;
            arm_tx
                .send(())
                .map_err(|_| io::Error::other("fixture arm receiver closed"))?;
            let facts = facts_rx.await.map_err(io::Error::other)?;
            if case.has_rows() {
                assert_eq!(prefix_rx.await.map_err(io::Error::other)?, [4, 0]);
                let cancel = facts.cancel.expect("actual cut receipt");
                assert_eq!(
                    cancel.committed_wire_bytes - facts.baseline.committed_wire_bytes,
                    2
                );
            } else {
                assert_eq!(
                    facts.hub.failure,
                    Some(if case == Case::WrongGeneration {
                        GateFailure::Identity
                    } else {
                        GateFailure::Transition
                    })
                );
                assert_eq!(facts.hub.gate.unwrap().accepted_prefix_bytes, 0);
                assert!(facts.cancel.is_none());
            }
            if case == Case::ParentPanic {
                panic!("intentional late-binding parent panic after actual cut")
            }
            if case == Case::ParentTimeout {
                // Inject failure only after observing the actual TCP cut.
                // This bound is clipped by the original absolute deadline.
                let failure_at = deadline.min(Instant::now() + Duration::from_millis(100));
                let _ = tokio::time::timeout_at(
                    tokio::time::Instant::from_std(failure_at),
                    std::future::pending::<()>(),
                )
                .await;
                controller.fail(GateFailure::Deadline);
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "actual TCP late-binding fixture timed out after its cut",
                ));
            }
            let wire = wire_rx.await.map_err(io::Error::other)?;
            let mut expected = greeting();
            expected.extend_from_slice(&metadata_wire());
            if case == Case::Healthy {
                append_packet(&mut expected, 4, b"\x03abc");
                expected.extend_from_slice(&metadata_wire());
                append_packet(&mut expected, 4, b"\x03abc");
            } else if case.invalid_followup().is_some() {
                append_packet(&mut expected, 4, b"\x03abc");
            } else if case == Case::ChildPanic {
                expected.extend_from_slice(&[4, 0]);
            }
            assert_eq!(&wire.bytes[..wire.length], expected.as_slice());
            assert_eq!(wire.sha256, <[u8; 32]>::from(Sha256::digest(&expected)));
            Ok(())
        });
        let outer = deadline;
        let primary =
            match tokio::time::timeout_at(tokio::time::Instant::from_std(outer), work).await {
                Ok(result) => result,
                Err(_) => {
                    controller.fail(GateFailure::Deadline);
                    Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "actual TCP late-binding fixture timed out",
                    ))
                }
            };
        let result = finish_fixture(&mut controller, &mut children, primary).await;
        assert!(children.is_empty());
        let final_facts = controller.snapshot();
        assert!(final_facts.original_writer_exited);
        assert!(final_facts.gate.unwrap().writer_exited);
        assert!(hub.state.lock().unwrap().scope.is_none());
        match case {
            Case::ParentTimeout => {
                assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
                assert_eq!(final_facts.failure, Some(GateFailure::Deadline));
            }
            Case::ParentPanic => {
                let error = result.unwrap_err();
                assert_eq!(error.kind(), io::ErrorKind::Other);
                assert!(error.to_string().contains("sha256="));
            }
            Case::ChildPanic => {
                let error = result.unwrap_err();
                let summary = error
                    .get_ref()
                    .and_then(|error| error.downcast_ref::<FixtureExitError>())
                    .expect("original join summary");
                assert!(
                    summary
                        .first_cleanup
                        .get_ref()
                        .and_then(|error| error.downcast_ref::<tokio::task::JoinError>())
                        .is_some_and(tokio::task::JoinError::is_panic)
                );
            }
            _ => {
                result?;
                if case.invalid_followup().is_some() {
                    assert_eq!(final_facts.failure, Some(GateFailure::Identity));
                }
            }
        }
        Ok(())
    }
    #[tokio::test]
    async fn actual_tcp_late_bind_then_exact_cut_and_healthy_same_io() -> io::Result<()> {
        tcp_case(Case::Healthy).await
    }
    #[tokio::test]
    async fn actual_tcp_stale_statement_followup_is_refused() -> io::Result<()> {
        tcp_case(Case::StaleStatement).await
    }
    #[tokio::test]
    async fn actual_tcp_older_statement_followup_is_refused() -> io::Result<()> {
        tcp_case(Case::OlderStatement).await
    }
    #[tokio::test]
    async fn actual_tcp_foreign_session_followup_is_refused() -> io::Result<()> {
        tcp_case(Case::WrongSession).await
    }
    #[tokio::test]
    async fn actual_tcp_new_session_key_followup_is_refused() -> io::Result<()> {
        tcp_case(Case::WrongSessionEpoch).await
    }
    #[tokio::test]
    async fn actual_tcp_zero_statement_followup_is_refused() -> io::Result<()> {
        tcp_case(Case::ZeroStatement).await
    }
    #[tokio::test]
    async fn actual_tcp_zero_epoch_followup_is_refused() -> io::Result<()> {
        tcp_case(Case::ZeroEpoch).await
    }
    #[tokio::test]
    async fn actual_tcp_full_connection_generation_refusal() -> io::Result<()> {
        tcp_case(Case::WrongGeneration).await
    }
    #[tokio::test]
    async fn actual_tcp_early_streaming_followup_refusal() -> io::Result<()> {
        tcp_case(Case::EarlyStreaming).await
    }
    #[tokio::test]
    async fn actual_tcp_early_local_followup_refusal() -> io::Result<()> {
        tcp_case(Case::EarlyLocal).await
    }
    #[tokio::test]
    async fn actual_tcp_outer_timeout_reaps_original_handles() -> io::Result<()> {
        tcp_case(Case::ParentTimeout).await
    }
    #[tokio::test]
    async fn actual_tcp_parent_panic_reaps_original_handles() -> io::Result<()> {
        tcp_case(Case::ParentPanic).await
    }
    #[tokio::test]
    async fn actual_tcp_child_panic_keeps_actual_join_error() -> io::Result<()> {
        tcp_case(Case::ChildPanic).await
    }
    #[tokio::test]
    async fn unbound_service_timeout_is_sticky_and_preserves_first_cause() -> io::Result<()> {
        let frontend = FrontendProcessId::new_v7();
        let absolute = Instant::now() + Duration::from_secs(5);
        let (hub, mut controller) = MysqlWriteGateHub::new(frontend, NONCE, absolute)?;
        controller.fail(GateFailure::Deadline);
        assert_eq!(controller.snapshot().failure, Some(GateFailure::Deadline));
        assert!(controller.snapshot().stopped && controller.snapshot().gate.is_none());
        controller.fail(GateFailure::Identity);
        assert_eq!(controller.snapshot().failure, Some(GateFailure::Deadline));
        assert_eq!(hub.deadline, absolute);
        assert_eq!(
            controller
                .arm(frontend, NONCE, 71, SQL, 2)
                .unwrap_err()
                .kind(),
            io::ErrorKind::TimedOut
        );
        let (_, mut prior) = MysqlWriteGateHub::new(frontend, NONCE, absolute)?;
        assert!(prior.arm(frontend, [0; 16], 71, SQL, 2).is_err());
        prior.fail(GateFailure::Deadline);
        assert_eq!(prior.snapshot().failure, Some(GateFailure::Identity));
        Ok(())
    }

    // NO-I/O negative fixture: nonconforming inner acceptance and original OS errors.
    struct ScriptedInner {
        over_length: Option<bool>,
    }
    impl AsyncWrite for ScriptedInner {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            match self.over_length {
                None => Poll::Ready(Ok(bytes.len())),
                Some(true) => Poll::Ready(Ok(bytes.len() + 1)),
                Some(false) => Poll::Ready(Err(io::Error::from_raw_os_error(libc::EPIPE))),
            }
        }
        fn poll_write_vectored(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            slices: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            match self.over_length {
                None => Poll::Ready(Ok(slices.iter().map(|slice| slice.len()).sum())),
                Some(true) => {
                    Poll::Ready(Ok(slices.iter().map(|slice| slice.len()).sum::<usize>() + 1))
                }
                Some(false) => Poll::Ready(Err(io::Error::from_raw_os_error(libc::EPIPE))),
            }
        }
        fn is_write_vectored(&self) -> bool {
            true
        }
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }
    async fn scripted_attached_failure(vectored: bool, over_length: bool) -> io::Result<()> {
        let (connection, statement) = identities();
        let frontend = FrontendProcessId::new_v7();
        let (hub, mut controller) =
            MysqlWriteGateHub::new(frontend, NONCE, Instant::now() + Duration::from_secs(5))?;
        controller.arm(frontend, NONCE, connection.connection_id(), SQL, 2)?;
        let hook = hub
            .bind_statement(connection, statement.session(), statement, SQL)?
            .unwrap();
        let raw = InitiallyRawMysqlWriter::new(
            ScriptedInner { over_length: None },
            connection,
            Arc::clone(&hub),
        );
        let mut writer = OwnedStreamingMysqlWriter::new(raw, ProtocolLimits::default(), 1)?;
        writer.start_metadata(metadata()?)?;
        writer.finish_metadata().await?;
        hook.begin_rows(writer.receipt())?;
        let mut raw = writer.into_inner();
        raw.bound
            .as_mut()
            .unwrap()
            .inner
            .as_mut()
            .unwrap()
            .over_length = Some(over_length);
        let actual_error = std::future::poll_fn(|cx| {
            if vectored {
                Pin::new(&mut raw).poll_write_vectored(cx, &[IoSlice::new(b"abc")])
            } else {
                Pin::new(&mut raw).poll_write(cx, b"abc")
            }
        })
        .await
        .unwrap_err();
        assert!(raw.bound.is_some());
        assert_eq!(hook.scope.snapshot().accepted_prefix_bytes, 0);
        if over_length {
            assert_eq!(actual_error.kind(), io::ErrorKind::InvalidData);
            assert_eq!(hook.scope.snapshot().failure, Some(GateFailure::Length));
            // The original inner-poll cause has already reached the actual Hub state.
            assert_eq!(hub.state.lock().unwrap().failure, Some(GateFailure::Length));
            assert!(
                hub.bind_statement(
                    connection,
                    followup_statement().session(),
                    followup_statement(),
                    HEALTHY_SQL
                )
                .is_err()
            );
            controller.fail(GateFailure::Identity);
            assert_eq!(controller.snapshot().failure, Some(GateFailure::Length));
        } else {
            assert_eq!(actual_error.raw_os_error(), Some(libc::EPIPE));
            assert_eq!(hook.scope.snapshot().failure, None);
            assert_eq!(hub.state.lock().unwrap().failure, None);
        }
        drop(raw);
        drop(hook);
        controller.finish_after_protocol_join()?;
        assert!(hub.state.lock().unwrap().scope.is_none());
        Ok(())
    }
    #[tokio::test]
    async fn scripted_attached_scalar_and_vectored_scope_failure_preserve_first_cause()
    -> io::Result<()> {
        for vectored in [false, true] {
            scripted_attached_failure(vectored, true).await?;
        }
        Ok(())
    }
    #[tokio::test]
    async fn scripted_attached_original_io_error_does_not_fabricate_scope_failure() -> io::Result<()>
    {
        for vectored in [false, true] {
            scripted_attached_failure(vectored, false).await?;
        }
        Ok(())
    }
}
