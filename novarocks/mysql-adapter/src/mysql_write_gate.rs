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

//! Test-only exact socket acceptance gate. No listener, registry or external control API.
//! Counts actual inner AsyncWrite acceptance; retains no offered bytes or body aliases.

pub(crate) mod late_binding;
pub(crate) mod unix_control;

use std::future::Future;
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use novarocks_query_application::client_connection::ClientConnectionToken;
use novarocks_query_application::session_control::StatementToken;
use opensrv_mysql::{FramingCursor, WritePhase};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWrite;
use tokio::sync::Notify;

const MAX_CUT_BYTES: u64 = 64 * 1024 * 1024;
const MAX_IO_SLICES: usize = 32;
// Bound one test poll to the existing 64 KiB payload quantum plus one header.
// Coalesced rows may offer more headers; returning a shorter actual write is valid.
const MAX_POLL_BYTES: usize = 64 * 1024 + 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GatePhase {
    Fresh,
    Armed,
    Rows,
    CancelRecorded,
    Resumed,
    Stopped,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GateFailure {
    Transition,
    Identity,
    Deadline,
    Length,
    Receipt,
    Counter,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct MysqlWriteGateSnapshot {
    pub connection: ClientConnectionToken,
    pub statement: Option<StatementToken>,
    pub sql_sha256: Option<[u8; 32]>,
    pub phase: GatePhase,
    pub failure: Option<GateFailure>,
    pub cut_bytes: u64,
    pub accepted_prefix_bytes: u64,
    pub accepted_prefix_sha256: [u8; 32],
    pub scalar_inner_polls: u64,
    pub vectored_inner_polls: u64,
    pub successful_inner_writes: u64,
    /// A subsequent write poll returned Pending at zero budget.
    pub blocked_after_acceptance: bool,
    pub baseline: Option<FramingCursor>,
    pub cancel_receipt: Option<FramingCursor>,
    pub writer_attached: bool,
    pub writer_exited: bool,
}

struct State {
    facts: MysqlWriteGateSnapshot,
    digest: Sha256,
    waiter: Option<Waker>,
}
struct Core {
    state: Mutex<State>,
    changed: Notify,
    deadline: Instant,
}

/// Control aliases own only fixed scalar observations, never the IO or result lease.
pub(crate) struct MysqlWriteTestScope {
    core: Arc<Core>,
}

fn error(failure: GateFailure) -> io::Error {
    let kind = if failure == GateFailure::Deadline {
        io::ErrorKind::TimedOut
    } else {
        io::ErrorKind::InvalidData
    };
    io::Error::new(
        kind,
        match failure {
            GateFailure::Transition => "invalid MySQL test gate transition",
            GateFailure::Identity => "MySQL test gate identity mismatch",
            GateFailure::Deadline => "MySQL test gate absolute deadline expired",
            GateFailure::Length => "invalid MySQL test gate write length",
            GateFailure::Receipt => "MySQL test gate receipt differs from actual accepted prefix",
            GateFailure::Counter => "MySQL test gate counter overflow",
        },
    )
}
fn fail(state: &mut State, reason: GateFailure) -> io::Error {
    error(*state.facts.failure.get_or_insert(reason))
}
fn check(state: &mut State, deadline: Instant) -> io::Result<()> {
    if let Some(reason) = state.facts.failure {
        return Err(error(reason));
    }
    if Instant::now() >= deadline {
        return Err(fail(state, GateFailure::Deadline));
    }
    Ok(())
}

impl MysqlWriteTestScope {
    pub(crate) fn new(connection: ClientConnectionToken, deadline: Instant) -> Self {
        Self {
            core: Arc::new(Core {
                state: Mutex::new(State {
                    facts: MysqlWriteGateSnapshot {
                        connection,
                        statement: None,
                        sql_sha256: None,
                        phase: GatePhase::Fresh,
                        failure: None,
                        cut_bytes: 0,
                        accepted_prefix_bytes: 0,
                        accepted_prefix_sha256: Sha256::digest([]).into(),
                        scalar_inner_polls: 0,
                        vectored_inner_polls: 0,
                        successful_inner_writes: 0,
                        blocked_after_acceptance: false,
                        baseline: None,
                        cancel_receipt: None,
                        writer_attached: false,
                        writer_exited: false,
                    },
                    digest: Sha256::new(),
                    waiter: None,
                }),
                changed: Notify::new(),
                deadline,
            }),
        }
    }
    /// The caller must supply the real owner token, never a reconstructed generation.
    pub(crate) fn arm(
        &self,
        statement: StatementToken,
        expected_session: novarocks_query_application::session_control::SessionToken,
        sql_sha256: [u8; 32],
        cut: u64,
    ) -> io::Result<()> {
        let mut state = self.core.state.lock().expect("MySQL test gate lock");
        check(&mut state, self.core.deadline)?;
        if state.facts.phase != GatePhase::Fresh {
            return Err(fail(&mut state, GateFailure::Transition));
        }
        if statement.session() != expected_session {
            return Err(fail(&mut state, GateFailure::Identity));
        }
        // The original session association was observed from the application owner.
        if cut == 0 || cut > MAX_CUT_BYTES {
            return Err(fail(&mut state, GateFailure::Length));
        }
        state.facts.statement = Some(statement);
        state.facts.sql_sha256 = Some(sql_sha256);
        state.facts.cut_bytes = cut;
        state.facts.phase = GatePhase::Armed;
        Ok(())
    }
    /// Called after metadata completes and before the first target row write.
    pub(crate) fn begin_rows(
        &self,
        statement: StatementToken,
        receipt: FramingCursor,
    ) -> io::Result<()> {
        let mut state = self.core.state.lock().expect("MySQL test gate lock");
        check(&mut state, self.core.deadline)?;
        if state.facts.statement != Some(statement) {
            return Err(fail(&mut state, GateFailure::Identity));
        }
        if state.facts.phase != GatePhase::Armed
            || receipt.phase != WritePhase::Boundary
            || receipt.rows_completed != 0
            || !state.facts.writer_attached
            || state.facts.writer_exited
        {
            return Err(fail(&mut state, GateFailure::Transition));
        }
        state.facts.baseline = Some(receipt);
        state.facts.phase = GatePhase::Rows;
        Ok(())
    }
    /// Called only after the target write future is dropped at the real cancel cut.
    pub(crate) fn record_cancel_receipt(
        &self,
        statement: StatementToken,
        receipt: FramingCursor,
    ) -> io::Result<()> {
        let mut state = self.core.state.lock().expect("MySQL test gate lock");
        check(&mut state, self.core.deadline)?;
        if state.facts.statement != Some(statement) {
            return Err(fail(&mut state, GateFailure::Identity));
        }
        if state.facts.phase != GatePhase::Rows || !state.facts.blocked_after_acceptance {
            return Err(fail(&mut state, GateFailure::Transition));
        }
        let baseline = state.facts.baseline.expect("Rows retains baseline");
        let delta = receipt
            .committed_wire_bytes
            .checked_sub(baseline.committed_wire_bytes);
        // Receipt position details remain factual; the runner checks its frozen row/packet oracle.
        if delta != Some(state.facts.cut_bytes)
            || delta != Some(state.facts.accepted_prefix_bytes)
            || receipt.phase != WritePhase::Row
            || !receipt.row_has_started()
        {
            return Err(fail(&mut state, GateFailure::Receipt));
        }
        state.facts.cancel_receipt = Some(receipt);
        state.facts.phase = GatePhase::CancelRecorded;
        Ok(())
    }
    pub(crate) fn resume(&self) -> io::Result<()> {
        let waiter = {
            let mut state = self.core.state.lock().expect("MySQL test gate lock");
            check(&mut state, self.core.deadline)?;
            if state.facts.phase != GatePhase::CancelRecorded {
                return Err(fail(&mut state, GateFailure::Transition));
            }
            state.facts.phase = GatePhase::Resumed;
            state.waiter.take()
        };
        if let Some(waker) = waiter {
            waker.wake();
        }
        self.core.changed.notify_waiters();
        Ok(())
    }
    /// Teardown never clears the original failure or makes a deadline fresh.
    pub(crate) fn stop(&self) {
        let waiter = {
            let mut state = self.core.state.lock().expect("MySQL test gate lock");
            state.facts.phase = GatePhase::Stopped;
            state.waiter.take()
        };
        if let Some(waker) = waiter {
            waker.wake();
        }
        self.core.changed.notify_waiters();
    }
    pub(crate) fn snapshot(&self) -> MysqlWriteGateSnapshot {
        let state = self.core.state.lock().expect("MySQL test gate lock");
        let mut facts = state.facts;
        facts.accepted_prefix_sha256 = state.digest.clone().finalize().into();
        facts
    }
    pub(crate) async fn wait_blocked(&self) -> io::Result<MysqlWriteGateSnapshot> {
        loop {
            let notified = self.core.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self.core.state.lock().expect("MySQL test gate lock");
                check(&mut state, self.core.deadline)?;
            }
            let facts = self.snapshot();
            if let Some(reason) = facts.failure {
                return Err(error(reason));
            }
            if facts.blocked_after_acceptance {
                return Ok(facts);
            }
            if facts.phase == GatePhase::Stopped || facts.writer_exited {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "MySQL test gate exited before blocking",
                ));
            }
            if tokio::time::timeout_at(tokio::time::Instant::from_std(self.core.deadline), notified)
                .await
                .is_err()
            {
                let (error, waiter) = {
                    let mut state = self.core.state.lock().expect("MySQL test gate lock");
                    let error = fail(&mut state, GateFailure::Deadline);
                    (error, state.waiter.take())
                };
                if let Some(waiter) = waiter {
                    waiter.wake();
                }
                self.core.changed.notify_waiters();
                return Err(error);
            }
        }
    }
}

/// Exactly one attached IO owner per scope; its Drop is a separate factual exit event.
pub(crate) struct MysqlWriteGate<W> {
    inner: Option<W>,
    scope: MysqlWriteTestScope,
    deadline_sleep: Pin<Box<tokio::time::Sleep>>,
}
impl<W> MysqlWriteGate<W> {
    pub(crate) fn new(inner: W, scope: &MysqlWriteTestScope) -> io::Result<Self> {
        {
            let mut state = scope.core.state.lock().expect("MySQL test gate lock");
            check(&mut state, scope.core.deadline)?;
            if state.facts.writer_attached {
                return Err(fail(&mut state, GateFailure::Transition));
            }
            state.facts.writer_attached = true;
        }
        let deadline_sleep = Box::pin(tokio::time::sleep_until(tokio::time::Instant::from_std(
            scope.core.deadline,
        )));
        Ok(Self {
            inner: Some(inner),
            scope: MysqlWriteTestScope {
                core: Arc::clone(&scope.core),
            },
            deadline_sleep,
        })
    }
    fn check_clock(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        if self.deadline_sleep.as_mut().poll(cx).is_ready() {
            let mut state = self.scope.core.state.lock().expect("MySQL test gate lock");
            return Err(fail(&mut state, GateFailure::Deadline));
        }
        Ok(())
    }
}
impl<W> Drop for MysqlWriteGate<W> {
    fn drop(&mut self) {
        // Publish only after the concrete IO owner destructor has returned.
        drop(self.inner.take());
        let mut state = self.scope.core.state.lock().expect("MySQL test gate lock");
        state.facts.writer_exited = true;
        state.waiter.take();
        drop(state);
        self.scope.core.changed.notify_waiters();
    }
}

fn budget(state: &mut State) -> io::Result<Option<usize>> {
    if let Some(reason) = state.facts.failure {
        return Err(error(reason));
    }
    if state.facts.phase == GatePhase::Stopped {
        return Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "MySQL test gate stopped",
        ));
    }
    if matches!(
        state.facts.phase,
        GatePhase::Rows | GatePhase::CancelRecorded
    ) {
        let remaining = state.facts.cut_bytes - state.facts.accepted_prefix_bytes;
        Ok(Some(remaining as usize))
    } else {
        Ok(None)
    }
}
fn count_poll(state: &mut State, vectored: bool) -> io::Result<()> {
    let counter = if vectored {
        &mut state.facts.vectored_inner_polls
    } else {
        &mut state.facts.scalar_inner_polls
    };
    match counter.checked_add(1) {
        Some(n) => {
            *counter = n;
            Ok(())
        }
        None => Err(fail(state, GateFailure::Counter)),
    }
}
fn accepted(state: &mut State, slices: &[IoSlice<'_>], n: usize, offered: usize) -> io::Result<()> {
    if n > offered {
        return Err(fail(state, GateFailure::Length));
    }
    // Count only actual acceptance during the frozen prefix. Passthrough is not hashed.
    if state.facts.phase != GatePhase::Rows {
        return Ok(());
    }
    if n == 0 {
        return Ok(());
    } // Original framing owner handles WriteZero.
    let Some(total) = state.facts.accepted_prefix_bytes.checked_add(n as u64) else {
        return Err(fail(state, GateFailure::Counter));
    };
    if total > state.facts.cut_bytes {
        return Err(fail(state, GateFailure::Length));
    }
    let Some(writes) = state.facts.successful_inner_writes.checked_add(1) else {
        return Err(fail(state, GateFailure::Counter));
    };
    let mut remaining = n;
    for slice in slices {
        let take = remaining.min(slice.len());
        state.digest.update(&slice[..take]);
        remaining -= take;
        if remaining == 0 {
            break;
        }
    }
    if remaining != 0 {
        return Err(fail(state, GateFailure::Length));
    }
    state.facts.accepted_prefix_bytes = total;
    state.facts.successful_inner_writes = writes;
    Ok(())
}

impl<W: AsyncWrite + Unpin> AsyncWrite for MysqlWriteGate<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.as_mut().get_mut();
        if let Err(error) = this.check_clock(cx) {
            return Poll::Ready(Err(error));
        }
        let mut state = this.scope.core.state.lock().expect("MySQL test gate lock");
        let budget = match budget(&mut state) {
            Ok(n) => n,
            Err(e) => return Poll::Ready(Err(e)),
        };
        if budget == Some(0) && !bytes.is_empty() {
            state.facts.blocked_after_acceptance = true;
            state.waiter = Some(cx.waker().clone());
            this.scope.core.changed.notify_waiters();
            return Poll::Pending;
        }
        let count = bytes
            .len()
            .min(budget.map(|n| n.min(MAX_POLL_BYTES)).unwrap_or(bytes.len()));
        if let Err(error) = count_poll(&mut state, false) {
            return Poll::Ready(Err(error));
        }
        let result = Pin::new(this.inner.as_mut().expect("attached MySQL test IO"))
            .poll_write(cx, &bytes[..count]);
        if let Poll::Ready(Ok(n)) = &result
            && let Err(e) = accepted(&mut state, &[IoSlice::new(&bytes[..count])], *n, count)
        {
            return Poll::Ready(Err(e));
        }
        result
    }
    fn is_write_vectored(&self) -> bool {
        self.inner
            .as_ref()
            .expect("attached MySQL test IO")
            .is_write_vectored()
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.as_mut().get_mut();
        if let Err(error) = this.check_clock(cx) {
            return Poll::Ready(Err(error));
        }
        let mut state = this.scope.core.state.lock().expect("MySQL test gate lock");
        let budget = match budget(&mut state) {
            Ok(n) => n,
            Err(e) => return Poll::Ready(Err(e)),
        };
        let mut slices = [IoSlice::new(&[]); MAX_IO_SLICES];
        let mut count = 0;
        let mut offered = 0usize;
        let result = if let Some(remaining) = budget {
            let mut remaining = remaining.min(MAX_POLL_BYTES);
            if bytes.len() > MAX_IO_SLICES {
                return Poll::Ready(Err(fail(&mut state, GateFailure::Length)));
            }
            if remaining == 0 && bytes.iter().any(|slice| !slice.is_empty()) {
                state.facts.blocked_after_acceptance = true;
                state.waiter = Some(cx.waker().clone());
                this.scope.core.changed.notify_waiters();
                return Poll::Pending;
            }
            for slice in bytes {
                if slice.is_empty() {
                    continue;
                }
                let take = slice.len().min(remaining);
                if take == 0 {
                    break;
                }
                slices[count] = IoSlice::new(&slice[..take]);
                count += 1;
                offered += take;
                remaining -= take;
            }
            if let Err(error) = count_poll(&mut state, true) {
                return Poll::Ready(Err(error));
            }
            Pin::new(this.inner.as_mut().expect("attached MySQL test IO"))
                .poll_write_vectored(cx, &slices[..count])
        } else {
            // No test gate: retain the exact inner call and all original slices.
            if let Err(error) = count_poll(&mut state, true) {
                return Poll::Ready(Err(error));
            }
            return Pin::new(this.inner.as_mut().expect("attached MySQL test IO"))
                .poll_write_vectored(cx, bytes);
        };
        if let Poll::Ready(Ok(n)) = &result
            && let Err(e) = accepted(&mut state, &slices[..count], *n, offered)
        {
            return Poll::Ready(Err(e));
        }
        result
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.as_mut().get_mut();
        if let Err(e) = this.check_clock(cx) {
            return Poll::Ready(Err(e));
        }
        Pin::new(this.inner.as_mut().expect("attached MySQL test IO")).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Physical teardown must remain possible even after a sticky fault/deadline.
        Pin::new(
            self.as_mut()
                .get_mut()
                .inner
                .as_mut()
                .expect("attached MySQL test IO"),
        )
        .poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_query_application::session_control::SessionToken;
    use opensrv_mysql::{OwnedStreamingMysqlWriter, ProtocolLimits};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::task::JoinSet;

    #[cfg(all(unix, novarocks_dev_controls, feature = "mem-1-m07-exact-mysql-write"))]
    mod authorization_tests {
        use super::*;
        use crate::{
            exact_mysql_write_fixture::MysqlWriteFixture,
            test_fixture_authorization::MysqlFixtureAuthorization,
        };
        use novarocks_failpoint::{
            liveness::{PreparedRoleSession, RunnerSession},
            scope::ProcessBinding,
            session::{DECLARATION_VERSION, DomainGrant, SessionDeclaration, SessionRole},
        };
        use novarocks_types::FrontendProcessId;
        use std::os::unix::fs::DirBuilderExt;
        use tokio::net::UnixStream;

        #[derive(Debug)]
        struct OwnedErrors(Vec<io::Error>);
        impl std::fmt::Display for OwnedErrors {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "authorized exact-write owner failures: {}", self.0.len())
            }
        }
        impl std::error::Error for OwnedErrors {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                self.0.first().map(|e| e as _)
            }
        }
        struct OriginalPanic(Mutex<Box<dyn std::any::Any + Send>>);
        impl std::fmt::Debug for OriginalPanic {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let _retained = &self.0;
                f.write_str("OriginalAuthorizedWriterPanic { payload_retained: true }")
            }
        }
        impl std::fmt::Display for OriginalPanic {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                std::fmt::Debug::fmt(self, f)
            }
        }
        impl std::error::Error for OriginalPanic {}
        async fn owned_work<F: Future<Output = io::Result<()>>>(work: F) -> io::Result<()> {
            tokio::pin!(work);
            std::future::poll_fn(|cx| {
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    work.as_mut().poll(cx)
                })) {
                    Ok(poll) => poll,
                    Err(original) => {
                        Poll::Ready(Err(io::Error::other(OriginalPanic(Mutex::new(original)))))
                    }
                }
            })
            .await
        }
        async fn command(
            peer: &mut UnixStream,
            frontend: FrontendProcessId,
            arm: bool,
            connection: u32,
        ) -> io::Result<()> {
            let mut body = vec![2, if arm { 1 } else { 3 }];
            for (tag, value) in [(1u8, frontend.to_bytes().to_vec()), (2, vec![1; 16])] {
                body.push(tag);
                body.extend_from_slice(&(value.len() as u16).to_le_bytes());
                body.extend_from_slice(&value);
            }
            if arm {
                for (tag, value) in [
                    (3u8, connection.to_le_bytes().to_vec()),
                    (4, vec![0; 32]),
                    (5, 2u64.to_le_bytes().to_vec()),
                ] {
                    body.push(tag);
                    body.extend_from_slice(&(value.len() as u16).to_le_bytes());
                    body.extend_from_slice(&value);
                }
            }
            peer.write_all(&(body.len() as u32).to_le_bytes()).await?;
            peer.write_all(&body).await?;
            let mut length = [0; 4];
            peer.read_exact(&mut length).await?;
            let n = u32::from_le_bytes(length) as usize;
            if !(3..=4092).contains(&n) {
                return Err(io::Error::other(
                    "original exact-write reply exceeded bound",
                ));
            }
            let mut reply = vec![0; n];
            peer.read_exact(&mut reply).await?;
            if reply[..3] != [2, if arm { 1 } else { 3 }, 0] {
                return Err(io::Error::other("original exact-write command refused"));
            }
            Ok(())
        }
        #[tokio::test]
        async fn finite_fe_grant_binds_real_uds_and_records_actual_tcp_cut_before_end_and_join()
        -> io::Result<()> {
            let deadline = Instant::now() + Duration::from_secs(10);
            let frontend = FrontendProcessId::new_v7();
            let root = std::path::PathBuf::from(format!("/tmp/nr-exact-auth-{}", frontend));
            std::fs::DirBuilder::new().mode(0o700).create(&root)?;
            let socket = root.join("gate.sock");
            let path = root.join("declaration.json");
            let declaration = SessionDeclaration {
                schema_version: DECLARATION_VERSION,
                session_id: "exact-session".into(),
                launch_id: "exact-launch".into(),
                role: SessionRole::Fe,
                lifetime_millis: 10_000,
                liveness_socket: root.join("live.sock"),
                handshake_secret: "abcdefghijklmnopqrstuvwxyz0123456789".into(),
                domain_grants: vec![DomainGrant {
                    domain: MysqlFixtureAuthorization::DOMAIN.into(),
                    grant_id: "exact-once".into(),
                    parameters: serde_json::json!({"action":"exact-write", "socket":socket,"nonce_hex":"01010101010101010101010101010101"}),
                }],
            };
            declaration.write_private(&path)?;
            let prepared = PreparedRoleSession::read_and_claim(&path, SessionRole::Fe)?;
            let mut runner = RunnerSession::start_until(declaration, deadline)?;
            let mut role = match prepared.bind(ProcessBinding::Frontend(frontend)) {
                Ok(role) => role,
                Err(primary) => {
                    let mut failures = vec![primary];
                    if let Err(error) = runner.end_and_join() {
                        failures.push(error);
                    }
                    return Err(io::Error::other(OwnedErrors(failures)));
                }
            };
            let scope = role.scope().clone();
            let grants = scope.grants_for(MysqlFixtureAuthorization::DOMAIN);
            let mut fixture = None;
            let primary = owned_work(async {
                assert!(MysqlFixtureAuthorization::from_grants(Vec::new())?.is_none());
                assert!(!socket.exists());
                let authorization =
                    MysqlFixtureAuthorization::from_grants(grants.clone())?.unwrap();
                fixture = Some(
                    MysqlWriteFixture::bind_authorized(authorization, frontend)
                        .map_err(io::Error::other)?,
                );
                assert!(socket.exists());
                assert!(
                    MysqlWriteFixture::bind_authorized(
                        MysqlFixtureAuthorization::from_grants(grants.clone())?.unwrap(),
                        frontend
                    )
                    .is_err()
                );
                let owner = fixture.as_mut().unwrap();
                let cancellation = owner.control_cancellation().unwrap();
                assert!(cancellation.deadline() <= deadline);
                let binding = owner.listener_binding().map_err(io::Error::other)?;
                let work = async {
                    let mut control = UnixStream::connect(&socket).await?;
                    let (connection, statement) = identities();
                    command(&mut control, frontend, true, connection.connection_id()).await?;
                    let (mut client, write) = pair().await?;
                    let hook = binding
                        .hub
                        .bind_statement(connection, statement.session(), statement, [0; 32])?
                        .unwrap();
                    let gate = super::super::late_binding::InitiallyRawMysqlWriter::new(
                        write,
                        connection,
                        Arc::clone(&binding.hub),
                    );
                    let mut writer =
                        OwnedStreamingMysqlWriter::new(gate, ProtocolLimits::default(), 1)?;
                    // The original socket flush attaches the selected gate before row admission.
                    writer.flush_socket().await?;
                    hook.begin_rows(writer.receipt())?;
                    writer.start_row(8)?;
                    writer.push_slice(b"abcdefgh")?;
                    let mut wire = [0; 2];
                    {
                        let flush = writer.flush_pending();
                        tokio::pin!(flush);
                        tokio::select! {
                            biased;
                            result = &mut flush => { result?; return Err(io::Error::other("authorized writer escaped exact cut")); },
                            result = client.read_exact(&mut wire) => { result?; },
                        }
                        // Keep polling the original pending writer until its real cut publishes the hit.
                        tokio::select! {
                            biased;
                            result = &mut flush => { result?; return Err(io::Error::other("authorized writer escaped exact cut")); },
                            _ = async { loop { if scope.observations().iter().any(|o| o.event == "hit") { break; } tokio::task::yield_now().await; } } => {},
                        }
                    }
                    assert_eq!(wire, [8, 0]);
                    hook.record_cancel_and_resume(writer.receipt())?;
                    runner.end_and_join()?;
                    cancellation.cancelled().await;
                    assert!(!cancellation.is_active());
                    drop(writer);
                    drop(hook);
                    assert_eq!(client.read(&mut [0; 1]).await?, 0);
                    command(&mut control, frontend, false, connection.connection_id()).await?;
                    assert_eq!(control.read(&mut [0; 1]).await?, 0);
                    Ok::<_, io::Error>(())
                };
                let (control, work) = tokio::join!(owner.run_control(), work);
                let mut failures = Vec::new();
                if let Err(error) = control {
                    failures.push(io::Error::other(error));
                }
                if let Err(error) = work {
                    failures.push(error);
                }
                if failures.is_empty() {
                    Ok(())
                } else {
                    Err(io::Error::other(OwnedErrors(failures)))
                }
            });
            let mut failures = Vec::new();
            if let Err(error) =
                timeout_result(tokio::time::timeout_at(deadline.into(), primary).await)
            {
                failures.push(error);
            }
            if let Some(owner) = fixture.as_mut() {
                if !failures.is_empty() {
                    owner.fail_and_stop();
                }
                if let Err(error) = owner.close_control() {
                    failures.push(io::Error::other(error));
                }
            }
            if let Err(error) = runner.end_and_join() {
                failures.push(error);
            }
            scope.wait_closed();
            if let Err(error) = role.close_and_join() {
                failures.push(error);
            }
            drop(fixture);
            for event in ["claimed", "hit", "claim-released"] {
                if scope
                    .observations()
                    .iter()
                    .filter(|o| o.event == event)
                    .count()
                    != 1
                {
                    failures.push(io::Error::other(format!(
                        "exact action did not record one {event}"
                    )));
                }
            }
            if socket.exists() || root.join("live.sock").exists() {
                failures.push(io::Error::other(
                    "original authorized owner socket remained",
                ));
            }
            if let Err(error) = std::fs::remove_file(runner.process_receipt_path()) {
                failures.push(error);
            }
            if let Err(error) = std::fs::remove_file(path.with_extension("claimed")) {
                failures.push(error);
            }
            if let Err(error) = std::fs::remove_file(path) {
                failures.push(error);
            }
            if let Err(error) = std::fs::remove_dir(root) {
                failures.push(error);
            }
            if failures.is_empty() {
                Ok(())
            } else {
                Err(io::Error::other(OwnedErrors(failures)))
            }
        }
    }
    // Only this parent owns task handles. Work borrows the set, so timeout or
    // unwinding cannot detach a child by consuming a JoinHandle.
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
        scope: &MysqlWriteTestScope,
        children: &mut JoinSet<io::Result<()>>,
        primary: io::Result<()>,
    ) -> io::Result<()> {
        scope.stop();
        children.abort_all();
        let mut first_cleanup = None;
        let mut cleanup_failures = 0usize;
        while let Some(joined) = children.join_next().await {
            let error = match joined {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error),
                Err(error) if error.is_cancelled() => None,
                Err(error) => Some(io::Error::other(error)),
            };
            if let Some(error) = error {
                cleanup_failures += 1;
                first_cleanup.get_or_insert(error);
            }
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
    fn timeout_result(
        outcome: Result<io::Result<()>, tokio::time::error::Elapsed>,
    ) -> io::Result<()> {
        outcome.map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "MySQL loopback fixture absolute deadline expired",
            )
        })?
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
    async fn pair() -> io::Result<(TcpStream, tokio::net::tcp::OwnedWriteHalf)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let connecting = TcpStream::connect(listener.local_addr()?);
        let (client, accepted) = tokio::try_join!(connecting, listener.accept())?;
        drop(listener);
        let (server, _) = accepted;
        let (read, write) = server.into_split();
        drop(read);
        Ok((client, write))
    }
    struct Scalar<W>(W);
    impl<W: AsyncWrite + Unpin> AsyncWrite for Scalar<W> {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            Pin::new(&mut self.0).poll_write(cx, bytes)
        }
        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.0).poll_flush(cx)
        }
        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.0).poll_shutdown(cx)
        }
    }
    async fn small_case<W: AsyncWrite + Unpin>(
        client: &mut TcpStream,
        io: W,
        vectored: bool,
        cut: u64,
    ) -> io::Result<()> {
        let (connection, statement) = identities();
        let scope = MysqlWriteTestScope::new(connection, Instant::now() + Duration::from_secs(5));
        let gate = MysqlWriteGate::new(io, &scope)?;
        assert_eq!(gate.is_write_vectored(), vectored);
        scope.arm(
            statement,
            statement.session(),
            Sha256::digest(b"fixture SQL identity").into(),
            cut,
        )?;
        let mut writer = OwnedStreamingMysqlWriter::new(gate, ProtocolLimits::default(), 1)?;
        scope.begin_rows(statement, writer.receipt())?;
        writer.start_row(8)?;
        assert_eq!(writer.push_slice(b"abcdefgh")?, 8);
        {
            let flush = writer.flush_pending();
            tokio::pin!(flush);
            tokio::select! {
                biased;
                result = &mut flush => panic!("flush completed before exact cut: {result:?}"),
                result = scope.wait_blocked() => { result?; }
            }
        }
        let receipt = writer.receipt();
        scope.record_cancel_receipt(statement, receipt)?;
        let facts = scope.snapshot();
        assert_eq!(facts.accepted_prefix_bytes, cut);
        assert_eq!(receipt.committed_wire_bytes, cut);
        assert_eq!(receipt.header_written, cut.min(4) as u8);
        assert_eq!(receipt.packet_payload_written, cut.saturating_sub(4) as u32);
        assert_eq!(facts.vectored_inner_polls > 0, vectored);
        let mut prefix = vec![0; cut as usize];
        client.read_exact(&mut prefix).await?;
        let expected = [8, 0, 0, 1, b'a', b'b', b'c', b'd', b'e', b'f', b'g', b'h'];
        assert_eq!(prefix, expected[..cut as usize]);
        assert_eq!(
            facts.accepted_prefix_sha256,
            <[u8; 32]>::from(Sha256::digest(&prefix))
        );
        scope.resume()?;
        writer.flush_pending().await?;
        writer.flush_socket().await?;
        assert_eq!(writer.receipt().rows_completed, 1);
        drop(writer);
        let mut suffix = vec![0; expected.len() - cut as usize];
        client.read_exact(&mut suffix).await?;
        assert_eq!(suffix, expected[cut as usize..]);
        assert_eq!(client.read(&mut [0; 1]).await?, 0);
        assert!(scope.snapshot().writer_exited);
        scope.stop();
        Ok(())
    }
    #[tokio::test]
    async fn actual_loopback_scalar_six_cuts() -> io::Result<()> {
        tokio::time::timeout(Duration::from_secs(10), async {
            for cut in 1..=6 {
                let (mut client, write) = pair().await?;
                small_case(&mut client, Scalar(write), false, cut).await?;
            }
            Ok(())
        })
        .await
        .map_err(|_| error(GateFailure::Deadline))?
    }
    #[tokio::test]
    async fn actual_loopback_vectored_six_cuts() -> io::Result<()> {
        tokio::time::timeout(Duration::from_secs(10), async {
            for cut in 1..=6 {
                let (mut client, write) = pair().await?;
                assert!(write.is_write_vectored());
                small_case(&mut client, write, true, cut).await?;
            }
            Ok(())
        })
        .await
        .map_err(|_| error(GateFailure::Deadline))?
    }
    #[tokio::test]
    async fn duplicate_binding_is_sticky_and_stop_reaps_actual_writer() -> io::Result<()> {
        let (connection, statement) = identities();
        let scope = MysqlWriteTestScope::new(connection, Instant::now() + Duration::from_secs(5));
        let mut children = JoinSet::new();
        let primary = timeout_result(
            tokio::time::timeout(
                Duration::from_secs(5),
                fixture_work(async {
                    let (mut client, write) = pair().await?;
                    let gate = MysqlWriteGate::new(write, &scope)?;
                    scope.arm(statement, statement.session(), [0; 32], 2)?;
                    assert!(
                        scope
                            .arm(statement, statement.session(), [0; 32], 2)
                            .is_err()
                    );
                    scope.stop();
                    children.spawn(async move {
                        let mut gate = gate;
                        assert!(gate.write_all(b"must not write").await.is_err());
                        gate.shutdown().await?;
                        drop(gate);
                        Ok::<_, io::Error>(())
                    });
                    let mut bytes = [0; 1];
                    assert_eq!(client.read(&mut bytes).await?, 0);
                    assert!(scope.snapshot().writer_exited);
                    assert_eq!(scope.snapshot().failure, Some(GateFailure::Transition));
                    Ok(())
                }),
            )
            .await,
        );
        finish_fixture(&scope, &mut children, primary).await
    }
    #[tokio::test]
    async fn wrong_statement_and_reused_writer_are_refused() -> io::Result<()> {
        let (client, write) = pair().await?;
        let (connection, statement) = identities();
        let scope = MysqlWriteTestScope::new(connection, Instant::now() + Duration::from_secs(5));
        let gate = MysqlWriteGate::new(write, &scope)?;
        let writer = OwnedStreamingMysqlWriter::new(gate, ProtocolLimits::default(), 1)?;
        scope.arm(statement, statement.session(), [0; 32], 2)?;
        let other = StatementToken::new(statement.session(), statement.generation() + 1);
        assert!(scope.begin_rows(other, writer.receipt()).is_err());
        drop(writer);
        let (_, another_write) = pair().await?;
        assert!(MysqlWriteGate::new(another_write, &scope).is_err());
        assert_eq!(scope.snapshot().failure, Some(GateFailure::Identity));
        drop(client);
        scope.stop();
        Ok(())
    }
    #[tokio::test]
    async fn stop_wakes_blocked_actual_tcp_writer_and_join_proves_exit() -> io::Result<()> {
        let (connection, statement) = identities();
        let scope = MysqlWriteTestScope::new(connection, Instant::now() + Duration::from_secs(5));
        let mut children = JoinSet::new();
        let primary = timeout_result(
            tokio::time::timeout(
                Duration::from_secs(5),
                fixture_work(async {
                    let (mut client, write) = pair().await?;
                    let gate = MysqlWriteGate::new(write, &scope)?;
                    scope.arm(statement, statement.session(), [0; 32], 2)?;
                    let mut writer =
                        OwnedStreamingMysqlWriter::new(gate, ProtocolLimits::default(), 1)?;
                    scope.begin_rows(statement, writer.receipt())?;
                    writer.start_row(8)?;
                    writer.push_slice(b"abcdefgh")?;
                    let (outcome_tx, outcome_rx) = tokio::sync::oneshot::channel();
                    children.spawn(async move {
                        let outcome = writer.flush_pending().await;
                        drop(writer);
                        outcome_tx
                            .send(outcome)
                            .map_err(|_| io::Error::other("writer outcome receiver exited"))?;
                        Ok::<_, io::Error>(())
                    });
                    scope.wait_blocked().await?;
                    scope.stop();
                    assert_eq!(
                        outcome_rx
                            .await
                            .map_err(io::Error::other)?
                            .unwrap_err()
                            .kind(),
                        io::ErrorKind::Interrupted
                    );
                    let mut wire = [0; 2];
                    client.read_exact(&mut wire).await?;
                    assert_eq!(wire, [8, 0]);
                    assert_eq!(client.read(&mut [0; 1]).await?, 0);
                    assert!(scope.snapshot().writer_exited);
                    Ok(())
                }),
            )
            .await,
        );
        finish_fixture(&scope, &mut children, primary).await
    }
    #[tokio::test]
    async fn actual_loopback_prefix_crosses_s_and_real_receipt_matches() -> io::Result<()> {
        const S: usize = 1_048_576;
        let (connection, statement) = identities();
        let scope = MysqlWriteTestScope::new(connection, Instant::now() + Duration::from_secs(5));
        let mut children = JoinSet::new();
        let primary = timeout_result(tokio::time::timeout(Duration::from_secs(5), fixture_work(async {
            let (mut client, write) = pair().await?;
            let gate = MysqlWriteGate::new(write, &scope)?;
            scope.arm(statement, statement.session(), [0; 32], (S + 1) as u64)?;
            let mut writer = OwnedStreamingMysqlWriter::new(gate, ProtocolLimits::default(), 1)?;
            scope.begin_rows(statement, writer.receipt())?;
            writer.start_row((S + 4) as u32)?;
            let payload = vec![b'x'; S + 4];
            let (reader_tx, reader_rx) = tokio::sync::oneshot::channel();
            children.spawn(async move {
                let mut prefix = vec![0; S + 1];
                client.read_exact(&mut prefix).await?;
                let digest: [u8; 32] = Sha256::digest(&prefix).into();
                assert_eq!(&prefix[..4], &[4, 0, 16, 1]);
                assert!(prefix[4..].iter().all(|byte| *byte == b'x'));
                drop(prefix);
                let mut suffix = [0; 7];
                client.read_exact(&mut suffix).await?;
                assert_eq!(client.read(&mut [0; 1]).await?, 0);
                reader_tx.send((digest, suffix)).map_err(|_| io::Error::other("reader outcome receiver exited"))?;
                Ok::<_, io::Error>(())
            });
            {
                let writing = writer.write_slice(&payload);
                tokio::pin!(writing);
                tokio::select! {
                    biased;
                    result = &mut writing => panic!("large write completed before cut: {result:?}"),
                    result = scope.wait_blocked() => { result?; }
                }
            }
            let receipt = writer.receipt();
            assert_eq!(receipt.committed_wire_bytes, (S + 1) as u64);
            assert_eq!(receipt.logical_written, (S - 3) as u32);
            let staged_end = receipt.logical_written as usize + writer.buffered_row_bytes();
            scope.record_cancel_receipt(statement, receipt)?;
            let digest = scope.snapshot().accepted_prefix_sha256;
            scope.resume()?;
            writer.flush_pending().await?;
            writer.write_slice(&payload[staged_end..]).await?;
            assert_eq!(writer.receipt().rows_completed, 1);
            drop(writer);
            let (actual_digest, suffix) = reader_rx.await.map_err(io::Error::other)?;
            assert_eq!(digest, actual_digest);
            assert_eq!(suffix, [b'x'; 7]);
            assert!(scope.snapshot().writer_exited);
            Ok(())
        })).await);
        finish_fixture(&scope, &mut children, primary).await
    }
    #[tokio::test]
    async fn original_deadline_wakes_gate_without_a_control_refresh() -> io::Result<()> {
        let (connection, statement) = identities();
        let scope = MysqlWriteTestScope::new(connection, Instant::now() + Duration::from_secs(1));
        let mut children = JoinSet::new();
        let primary = timeout_result(
            tokio::time::timeout(
                Duration::from_secs(5),
                fixture_work(async {
                    let (mut client, write) = pair().await?;
                    let gate = MysqlWriteGate::new(write, &scope)?;
                    scope.arm(statement, statement.session(), [0; 32], 2)?;
                    let mut writer =
                        OwnedStreamingMysqlWriter::new(gate, ProtocolLimits::default(), 1)?;
                    scope.begin_rows(statement, writer.receipt())?;
                    writer.start_row(8)?;
                    writer.push_slice(b"abcdefgh")?;
                    let (outcome_tx, outcome_rx) = tokio::sync::oneshot::channel();
                    children.spawn(async move {
                        let outcome = writer.flush_pending().await;
                        drop(writer);
                        outcome_tx
                            .send(outcome)
                            .map_err(|_| io::Error::other("deadline outcome receiver exited"))?;
                        Ok::<_, io::Error>(())
                    });
                    scope.wait_blocked().await?;
                    assert_eq!(
                        outcome_rx
                            .await
                            .map_err(io::Error::other)?
                            .unwrap_err()
                            .kind(),
                        io::ErrorKind::TimedOut
                    );
                    assert_eq!(scope.snapshot().failure, Some(GateFailure::Deadline));
                    assert!(scope.snapshot().writer_exited);
                    let mut prefix = [0; 2];
                    client.read_exact(&mut prefix).await?;
                    assert_eq!(prefix, [8, 0]);
                    assert_eq!(client.read(&mut [0; 1]).await?, 0);
                    scope.stop();
                    assert_eq!(scope.snapshot().failure, Some(GateFailure::Deadline));
                    Ok(())
                }),
            )
            .await,
        );
        finish_fixture(&scope, &mut children, primary).await
    }
    #[tokio::test]
    async fn control_timeout_is_sticky_and_preserves_prior_cause() -> io::Result<()> {
        let (connection, statement) = identities();
        let deadline = Instant::now() + Duration::from_millis(50);
        let scope = MysqlWriteTestScope::new(connection, deadline);
        assert_eq!(
            scope.wait_blocked().await.unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        assert_eq!(scope.snapshot().failure, Some(GateFailure::Deadline));
        assert_eq!(scope.core.deadline, deadline);
        assert_eq!(
            scope
                .arm(statement, statement.session(), [0; 32], 2)
                .unwrap_err()
                .kind(),
            io::ErrorKind::TimedOut
        );
        scope.stop();
        assert_eq!(scope.snapshot().failure, Some(GateFailure::Deadline));
        let prior = MysqlWriteTestScope::new(connection, Instant::now() + Duration::from_secs(1));
        prior.arm(statement, statement.session(), [0; 32], 2)?;
        assert!(
            prior
                .arm(statement, statement.session(), [0; 32], 2)
                .is_err()
        );
        assert_eq!(
            prior.wait_blocked().await.unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(prior.snapshot().failure, Some(GateFailure::Transition));
        prior.stop();
        Ok(())
    }

    #[tokio::test]
    async fn parent_panic_and_timeout_reap_the_actual_blocked_tcp_child() -> io::Result<()> {
        for panic_parent in [false, true] {
            let (connection, statement) = identities();
            let scope =
                MysqlWriteTestScope::new(connection, Instant::now() + Duration::from_secs(5));
            let mut children = JoinSet::new();
            let (mut client, write) = tokio::time::timeout_at(
                tokio::time::Instant::from_std(scope.core.deadline),
                pair(),
            )
            .await
            .map_err(io::Error::other)??;
            let primary = timeout_result(
                tokio::time::timeout(
                    Duration::from_millis(100),
                    fixture_work(async {
                        let gate = MysqlWriteGate::new(write, &scope)?;
                        scope.arm(statement, statement.session(), [0; 32], 2)?;
                        let mut writer =
                            OwnedStreamingMysqlWriter::new(gate, ProtocolLimits::default(), 1)?;
                        scope.begin_rows(statement, writer.receipt())?;
                        writer.start_row(8)?;
                        writer.push_slice(b"abcdefgh")?;
                        children.spawn(async move {
                            let outcome = writer.flush_pending().await;
                            drop(writer);
                            outcome
                        });
                        scope.wait_blocked().await?;
                        if panic_parent {
                            panic!("intentional parent fixture panic after actual TCP cut");
                        }
                        std::future::pending::<io::Result<()>>().await
                    }),
                )
                .await,
            );
            let error = finish_fixture(&scope, &mut children, primary)
                .await
                .unwrap_err();
            assert_eq!(
                error.kind(),
                if panic_parent {
                    io::ErrorKind::Other
                } else {
                    io::ErrorKind::TimedOut
                }
            );
            assert!(children.is_empty());
            assert!(scope.snapshot().writer_exited);
            // Cleanup is not inferred from the snapshot: the original JoinSet
            // has drained, and the real socket now supplies its exact prefix/EOF.
            tokio::time::timeout(Duration::from_secs(1), async {
                let mut prefix = [0; 2];
                client.read_exact(&mut prefix).await?;
                assert_eq!(prefix, [8, 0]);
                assert_eq!(client.read(&mut [0; 1]).await?, 0);
                Ok::<_, io::Error>(())
            })
            .await
            .map_err(io::Error::other)??;
        }
        Ok(())
    }

    #[tokio::test]
    async fn child_panic_is_a_join_error_after_actual_tcp_owner_exit() -> io::Result<()> {
        let (connection, statement) = identities();
        let scope = MysqlWriteTestScope::new(connection, Instant::now() + Duration::from_secs(5));
        let mut children = JoinSet::new();
        let primary = timeout_result(
            tokio::time::timeout(
                Duration::from_secs(5),
                fixture_work(async {
                    let (mut client, write) = pair().await?;
                    let gate = MysqlWriteGate::new(write, &scope)?;
                    scope.arm(statement, statement.session(), [0; 32], 2)?;
                    let mut writer =
                        OwnedStreamingMysqlWriter::new(gate, ProtocolLimits::default(), 1)?;
                    scope.begin_rows(statement, writer.receipt())?;
                    writer.start_row(8)?;
                    writer.push_slice(b"abcdefgh")?;
                    children.spawn(async move {
                        let error = writer.flush_pending().await.unwrap_err();
                        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
                        panic!("intentional TCP writer child panic after Stop");
                        #[allow(unreachable_code)]
                        Ok::<_, io::Error>(())
                    });
                    scope.wait_blocked().await?;
                    scope.stop();
                    let mut prefix = [0; 2];
                    client.read_exact(&mut prefix).await?;
                    assert_eq!(prefix, [8, 0]);
                    assert_eq!(client.read(&mut [0; 1]).await?, 0);
                    Ok(())
                }),
            )
            .await,
        );
        let error = finish_fixture(&scope, &mut children, primary)
            .await
            .unwrap_err();
        let exit = error
            .get_ref()
            .and_then(|error| error.downcast_ref::<FixtureExitError>())
            .expect("actual cleanup summary");
        assert!(exit.primary.is_none());
        assert_eq!(exit.cleanup_failures, 1);
        assert!(
            exit.first_cleanup
                .get_ref()
                .and_then(|error| error.downcast_ref::<tokio::task::JoinError>())
                .is_some_and(tokio::task::JoinError::is_panic)
        );
        assert!(children.is_empty());
        assert!(scope.snapshot().writer_exited);
        Ok(())
    }
    #[tokio::test]
    async fn duplicate_begin_and_resume_before_cancel_do_not_rearm() -> io::Result<()> {
        let (client, write) = pair().await?;
        let (connection, statement) = identities();
        let scope = MysqlWriteTestScope::new(connection, Instant::now() + Duration::from_secs(5));
        let gate = MysqlWriteGate::new(write, &scope)?;
        let writer = OwnedStreamingMysqlWriter::new(gate, ProtocolLimits::default(), 1)?;
        scope.arm(statement, statement.session(), [0; 32], 2)?;
        scope.begin_rows(statement, writer.receipt())?;
        assert!(scope.begin_rows(statement, writer.receipt()).is_err());
        assert!(scope.resume().is_err());
        assert_eq!(scope.snapshot().phase, GatePhase::Rows);
        assert_eq!(scope.snapshot().failure, Some(GateFailure::Transition));
        drop(writer);
        drop(client);
        scope.stop();
        Ok(())
    }
    #[tokio::test]
    async fn over_bounded_vectored_input_is_refused_before_inner_io() -> io::Result<()> {
        let (mut client, write) = pair().await?;
        let (connection, statement) = identities();
        let scope = MysqlWriteTestScope::new(connection, Instant::now() + Duration::from_secs(5));
        let gate = MysqlWriteGate::new(write, &scope)?;
        let writer = OwnedStreamingMysqlWriter::new(gate, ProtocolLimits::default(), 1)?;
        scope.arm(statement, statement.session(), [0; 32], 2)?;
        scope.begin_rows(statement, writer.receipt())?;
        let mut gate = writer.into_inner();
        assert!(
            gate.write_vectored(&[IoSlice::new(b"x"); 33])
                .await
                .is_err()
        );
        let facts = scope.snapshot();
        assert_eq!(facts.failure, Some(GateFailure::Length));
        assert_eq!(facts.accepted_prefix_bytes, 0);
        assert_eq!(facts.vectored_inner_polls, 0);
        drop(gate);
        assert_eq!(client.read(&mut [0; 1]).await?, 0);
        assert!(scope.snapshot().writer_exited);
        scope.stop();
        Ok(())
    }

    mod scripted_gate_extra_tests {
        use super::*;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::Wake;

        #[derive(Clone, Copy)]
        enum Step {
            Pending,
            Zero,
            Error,
            Ready(usize),
            OverOffered,
        }
        struct ScriptState {
            steps: [Step; 8],
            steps_len: usize,
            next: usize,
            calls: usize,
            vectored_calls: usize,
            offered: [usize; 8],
            // Fixed test fixture oracle; not a buffer in the Gate implementation.
            accepted: [u8; 64],
            accepted_len: usize,
            pending: Option<Waker>,
        }
        struct Scripted(Arc<Mutex<ScriptState>>);
        impl Scripted {
            fn new(steps: &[Step]) -> (Self, Arc<Mutex<ScriptState>>) {
                assert!(steps.len() <= 8);
                let mut values = [Step::Zero; 8];
                values[..steps.len()].copy_from_slice(steps);
                let state = Arc::new(Mutex::new(ScriptState {
                    steps: values,
                    steps_len: steps.len(),
                    next: 0,
                    calls: 0,
                    vectored_calls: 0,
                    offered: [0; 8],
                    accepted: [0; 64],
                    accepted_len: 0,
                    pending: None,
                }));
                (Self(Arc::clone(&state)), state)
            }
            fn scripted_poll(
                &self,
                cx: &Context<'_>,
                slices: &[IoSlice<'_>],
                vectored: bool,
            ) -> Poll<io::Result<usize>> {
                let mut state = self.0.lock().unwrap();
                assert!(state.next < state.steps_len, "script exhausted");
                assert!(state.calls < 8, "script call bound exceeded");
                let offered = slices.iter().map(|slice| slice.len()).sum::<usize>();
                let ordinal = state.calls;
                state.offered[ordinal] = offered;
                state.calls += 1;
                state.vectored_calls += usize::from(vectored);
                let step = state.steps[state.next];
                state.next += 1;
                match step {
                    Step::Pending => {
                        state.pending = Some(cx.waker().clone());
                        Poll::Pending
                    }
                    Step::Zero => Poll::Ready(Ok(0)),
                    // Exact original io::Error identity, with no OS/network claim.
                    Step::Error => Poll::Ready(Err(io::Error::from_raw_os_error(1234))),
                    Step::OverOffered => Poll::Ready(Ok(offered + 1)),
                    Step::Ready(n) => {
                        assert!(n <= offered && state.accepted_len + n <= 64);
                        let mut remaining = n;
                        for slice in slices {
                            let take = remaining.min(slice.len());
                            let offset = state.accepted_len;
                            state.accepted[offset..offset + take].copy_from_slice(&slice[..take]);
                            state.accepted_len += take;
                            remaining -= take;
                            if remaining == 0 {
                                break;
                            }
                        }
                        assert_eq!(remaining, 0);
                        Poll::Ready(Ok(n))
                    }
                }
            }
        }
        impl AsyncWrite for Scripted {
            fn poll_write(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                bytes: &[u8],
            ) -> Poll<io::Result<usize>> {
                self.get_mut()
                    .scripted_poll(cx, &[IoSlice::new(bytes)], false)
            }
            fn poll_write_vectored(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                bytes: &[IoSlice<'_>],
            ) -> Poll<io::Result<usize>> {
                self.get_mut().scripted_poll(cx, bytes, true)
            }
            fn is_write_vectored(&self) -> bool {
                true
            }
            fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
            fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
        }
        struct WakeCount(AtomicUsize);
        impl Wake for WakeCount {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        type ScriptFixture = (
            MysqlWriteTestScope,
            MysqlWriteGate<Scripted>,
            Arc<Mutex<ScriptState>>,
        );
        fn fixture(steps: &[Step], cut: u64) -> io::Result<ScriptFixture> {
            let (connection, statement) = identities();
            let scope =
                MysqlWriteTestScope::new(connection, Instant::now() + Duration::from_secs(5));
            let (inner, script) = Scripted::new(steps);
            let gate = MysqlWriteGate::new(inner, &scope)?;
            let writer = OwnedStreamingMysqlWriter::new(gate, ProtocolLimits::default(), 1)?;
            scope.arm(statement, statement.session(), [0; 32], cut)?;
            scope.begin_rows(statement, writer.receipt())?;
            Ok((scope, writer.into_inner(), script))
        }
        fn invoke(
            gate: &mut MysqlWriteGate<Scripted>,
            cx: &mut Context<'_>,
            bytes: &[u8],
            vectored: bool,
        ) -> Poll<io::Result<usize>> {
            if vectored {
                Pin::new(gate).poll_write_vectored(
                    cx,
                    &[IoSlice::new(&[]), IoSlice::new(bytes), IoSlice::new(&[])],
                )
            } else {
                Pin::new(gate).poll_write(cx, bytes)
            }
        }
        fn assert_no_charge(scope: &MysqlWriteTestScope) {
            let facts = scope.snapshot();
            assert_eq!(facts.accepted_prefix_bytes, 0);
            assert_eq!(facts.successful_inner_writes, 0);
            assert_eq!(
                facts.accepted_prefix_sha256,
                <[u8; 32]>::from(Sha256::digest([]))
            );
            assert!(!facts.blocked_after_acceptance);
        }

        #[tokio::test]
        async fn scripted_rows_pending_preserves_inner_waker_and_charges_only_ready()
        -> io::Result<()> {
            for vectored in [false, true] {
                let (scope, mut gate, script) = fixture(&[Step::Pending, Step::Ready(1)], 3)?;
                let wake = Arc::new(WakeCount(AtomicUsize::new(0)));
                let waker = Waker::from(Arc::clone(&wake));
                let mut cx = Context::from_waker(&waker);
                assert!(invoke(&mut gate, &mut cx, b"abc", vectored).is_pending());
                assert_no_charge(&scope);
                assert!(scope.snapshot().failure.is_none());
                // This is the INNER pending waker, not a synthetic Gate budget wake.
                script.lock().unwrap().pending.take().unwrap().wake();
                assert!(wake.0.load(Ordering::SeqCst) > 0);
                assert!(matches!(
                    invoke(&mut gate, &mut cx, b"abc", vectored),
                    Poll::Ready(Ok(1))
                ));
                let facts = scope.snapshot();
                assert_eq!(facts.accepted_prefix_bytes, 1);
                assert_eq!(facts.successful_inner_writes, 1);
                assert_eq!(
                    facts.accepted_prefix_sha256,
                    <[u8; 32]>::from(Sha256::digest(b"a"))
                );
                assert_eq!(script.lock().unwrap().calls, 2);
                scope.stop();
                drop(gate);
                assert!(scope.snapshot().writer_exited); // Script destructor only, not TCP exit.
            }
            Ok(())
        }
        #[tokio::test]
        async fn scripted_rows_zero_and_original_error_are_transparent_without_charge()
        -> io::Result<()> {
            for vectored in [false, true] {
                let (scope, mut gate, script) = fixture(&[Step::Zero, Step::Error], 3)?;
                let waker = Waker::from(Arc::new(WakeCount(AtomicUsize::new(0))));
                let mut cx = Context::from_waker(&waker);
                assert!(matches!(
                    invoke(&mut gate, &mut cx, b"abc", vectored),
                    Poll::Ready(Ok(0))
                ));
                assert_no_charge(&scope);
                let err = match invoke(&mut gate, &mut cx, b"abc", vectored) {
                    Poll::Ready(Err(error)) => error,
                    _ => panic!("original inner error was not forwarded"),
                };
                assert_eq!(err.raw_os_error(), Some(1234));
                assert_no_charge(&scope);
                assert!(
                    scope.snapshot().failure.is_none(),
                    "IO cause remains with original framing caller"
                );
                assert_eq!(script.lock().unwrap().calls, 2);
                scope.stop();
                drop(gate);
            }
            Ok(())
        }
        #[tokio::test]
        async fn scripted_rows_zero_still_becomes_original_framer_write_zero() -> io::Result<()> {
            let (scope, gate, _) = fixture(&[Step::Zero], 3)?;
            let mut writer = OwnedStreamingMysqlWriter::new(gate, ProtocolLimits::default(), 1)?;
            writer.start_row(3)?;
            writer.push_slice(b"abc")?;
            assert_eq!(
                writer.flush_pending().await.unwrap_err().kind(),
                io::ErrorKind::WriteZero
            );
            assert_eq!(writer.receipt().phase, WritePhase::Poisoned);
            assert_no_charge(&scope);
            scope.stop();
            drop(writer);
            assert!(scope.snapshot().writer_exited);
            Ok(())
        }
        #[tokio::test]
        async fn scripted_rows_overoffered_is_sticky_before_hash_or_acceptance_changes()
        -> io::Result<()> {
            for vectored in [false, true] {
                let (scope, mut gate, script) = fixture(&[Step::OverOffered], 3)?;
                let waker = Waker::from(Arc::new(WakeCount(AtomicUsize::new(0))));
                let mut cx = Context::from_waker(&waker);
                let err = match invoke(&mut gate, &mut cx, b"abc", vectored) {
                    Poll::Ready(Err(error)) => error,
                    _ => panic!("invalid inner length was accepted"),
                };
                assert_eq!(err.kind(), io::ErrorKind::InvalidData);
                assert_eq!(scope.snapshot().failure, Some(GateFailure::Length));
                assert_no_charge(&scope);
                // Existing Length wins over a later invalid controller transition.
                assert!(scope.resume().is_err());
                assert!(matches!(
                    invoke(&mut gate, &mut cx, b"abc", vectored),
                    Poll::Ready(Err(_))
                ));
                assert_eq!(script.lock().unwrap().calls, 1);
                scope.stop();
                assert_eq!(scope.snapshot().failure, Some(GateFailure::Length));
                drop(gate);
            }
            Ok(())
        }
        #[tokio::test]
        async fn scripted_rows_multiple_ready_and_pending_hash_only_actual_byte_prefixes()
        -> io::Result<()> {
            for vectored in [false, true] {
                let (scope, mut gate, script) = fixture(
                    &[
                        Step::Ready(1),
                        Step::Pending,
                        Step::Ready(1),
                        Step::Ready(1),
                    ],
                    3,
                )?;
                let waker = Waker::from(Arc::new(WakeCount(AtomicUsize::new(0))));
                let mut cx = Context::from_waker(&waker);
                assert!(matches!(
                    invoke(&mut gate, &mut cx, b"abc", vectored),
                    Poll::Ready(Ok(1))
                ));
                assert!(invoke(&mut gate, &mut cx, b"bc", vectored).is_pending());
                assert_eq!(scope.snapshot().accepted_prefix_bytes, 1);
                script.lock().unwrap().pending.take().unwrap().wake();
                assert!(matches!(
                    invoke(&mut gate, &mut cx, b"bc", vectored),
                    Poll::Ready(Ok(1))
                ));
                assert!(matches!(
                    invoke(&mut gate, &mut cx, b"c", vectored),
                    Poll::Ready(Ok(1))
                ));
                let facts = scope.snapshot();
                assert_eq!(facts.accepted_prefix_bytes, 3);
                assert_eq!(facts.successful_inner_writes, 3);
                assert_eq!(
                    facts.accepted_prefix_sha256,
                    <[u8; 32]>::from(Sha256::digest(b"abc"))
                );
                assert_eq!(&script.lock().unwrap().accepted[..3], b"abc");
                assert_eq!(script.lock().unwrap().calls, 4);
                // Fifth poll is held by Gate; no fifth INNER poll or false charge.
                assert!(invoke(&mut gate, &mut cx, b"d", vectored).is_pending());
                assert!(scope.snapshot().blocked_after_acceptance);
                assert_eq!(script.lock().unwrap().calls, 4);
                scope.stop();
                drop(gate);
            }
            Ok(())
        }
        #[tokio::test]
        async fn scripted_rows_empty_scalar_and_slices_do_not_fabricate_gate_blocked()
        -> io::Result<()> {
            let (scope, mut gate, script) = fixture(&[Step::Ready(3), Step::Zero, Step::Zero], 3)?;
            let waker = Waker::from(Arc::new(WakeCount(AtomicUsize::new(0))));
            let mut cx = Context::from_waker(&waker);
            assert!(matches!(
                Pin::new(&mut gate).poll_write(&mut cx, b"abc"),
                Poll::Ready(Ok(3))
            ));
            assert!(matches!(
                Pin::new(&mut gate).poll_write(&mut cx, &[]),
                Poll::Ready(Ok(0))
            ));
            let empties = [IoSlice::new(&[]); 3];
            assert!(matches!(
                Pin::new(&mut gate).poll_write_vectored(&mut cx, &empties),
                Poll::Ready(Ok(0))
            ));
            let facts = scope.snapshot();
            assert_eq!(facts.accepted_prefix_bytes, 3);
            assert_eq!(facts.successful_inner_writes, 1);
            assert_eq!(
                facts.accepted_prefix_sha256,
                <[u8; 32]>::from(Sha256::digest(b"abc"))
            );
            assert!(!facts.blocked_after_acceptance);
            assert_eq!(script.lock().unwrap().offered[..3], [3, 0, 0]);
            assert!(Pin::new(&mut gate).poll_write(&mut cx, b"d").is_pending());
            assert!(scope.snapshot().blocked_after_acceptance);
            assert_eq!(script.lock().unwrap().calls, 3);
            scope.stop();
            drop(gate);
            Ok(())
        }

        // This case is REAL TCP + public FramingCursor. Wrong-receipt controller
        // failure does not invent an immediate wake contract: explicit Stop remains
        // required, with the original bounded fixture parent retaining every child.
        #[tokio::test]
        async fn actual_blocked_tcp_wrong_receipt_then_stop_preserves_first_cause_and_joins()
        -> io::Result<()> {
            let (connection, statement) = identities();
            let scope =
                MysqlWriteTestScope::new(connection, Instant::now() + Duration::from_secs(5));
            let mut children = JoinSet::new();
            let primary = timeout_result(
                tokio::time::timeout(
                    Duration::from_secs(5),
                    fixture_work(async {
                        let (mut client, write) = pair().await?;
                        let gate = MysqlWriteGate::new(write, &scope)?;
                        scope.arm(statement, statement.session(), [0; 32], 2)?;
                        let mut writer =
                            OwnedStreamingMysqlWriter::new(gate, ProtocolLimits::default(), 1)?;
                        scope.begin_rows(statement, writer.receipt())?;
                        writer.start_row(8)?;
                        writer.push_slice(b"abcdefgh")?;
                        let (outcome_tx, outcome_rx) = tokio::sync::oneshot::channel();
                        children.spawn(async move {
                            let outcome = writer.flush_pending().await;
                            drop(writer);
                            outcome_tx
                                .send(outcome)
                                .map_err(|_| io::Error::other("writer outcome receiver exited"))?;
                            Ok::<_, io::Error>(())
                        });
                        scope.wait_blocked().await?;
                        // Begin-rows receipt is a genuine old public receipt, intentionally
                        // stale for this negative case (no fake cursor field construction).
                        let stale = scope.snapshot().baseline.unwrap();
                        assert!(scope.record_cancel_receipt(statement, stale).is_err());
                        assert_eq!(scope.snapshot().failure, Some(GateFailure::Receipt));
                        assert!(scope.resume().is_err());
                        scope.stop();
                        assert_eq!(
                            outcome_rx
                                .await
                                .map_err(io::Error::other)?
                                .unwrap_err()
                                .kind(),
                            io::ErrorKind::InvalidData
                        );
                        let mut prefix = [0; 2];
                        client.read_exact(&mut prefix).await?;
                        assert_eq!(prefix, [8, 0]);
                        assert_eq!(client.read(&mut [0; 1]).await?, 0);
                        assert!(scope.snapshot().writer_exited);
                        assert_eq!(scope.snapshot().failure, Some(GateFailure::Receipt));
                        Ok(())
                    }),
                )
                .await,
            );
            finish_fixture(&scope, &mut children, primary).await
        }
    }
}

#[cfg(feature = "mem-1-m07-exact-mysql-write")]
#[path = "mysql_write_gate/original_freeze.rs"]
pub(crate) mod original_freeze;
