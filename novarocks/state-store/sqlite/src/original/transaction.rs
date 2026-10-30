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

use super::*;
use crate::{
    evidence::{DispatchGuard, SqliteCommitEvidence},
    txn::{self, SqliteTxnState},
};
use novarocks_state_store_api::{
    AttemptId, AttemptOutcome, CommitOutcome, InstanceScope, Key, OriginalReadTransaction,
    OriginalWriteTransaction, Precondition, RangePage, RangeRequest, StateRecord,
    StateStoreBlockingOrigin as Origin, Value, WriteAttempt,
};
use std::sync::atomic::{AtomicBool, Ordering};

// Closed concrete inputs only; no closure, executor, or boxed future is stored.
enum Command {
    Begin,
    Get(Key),
    Range(RangeRequest),
    Put(Key, Value, Precondition),
    Delete(Key, Precondition),
    Commit,
}
impl Command {
    fn phase(&self, write: bool) -> Phase {
        match self {
            Self::Begin if write => Phase::BeginWrite,
            Self::Begin => Phase::BeginRead,
            Self::Get(_) => Phase::Get,
            Self::Range(_) => Phase::Range,
            Self::Put(..) => Phase::Put,
            Self::Delete(..) => Phase::Delete,
            Self::Commit => Phase::Commit,
        }
    }
}
enum Output {
    Unit,
    Record(Option<StateRecord>),
    Page(RangePage),
}
struct Input {
    path: std::path::PathBuf,
    limits: novarocks_state_store_api::StateStoreLimits,
    deadline: std::time::Instant,
    lifetime: Option<Arc<crate::SqliteInstanceLifetime>>,
    metrics: Arc<crate::metrics::StateStoreMetrics>,
    #[cfg(test)]
    hooks: txn::TestHooks,
}
struct TxnBody {
    input: Input,
    state: Option<SqliteTxnState>,
    attempt: Option<WriteAttempt>,
    evidence: Option<Arc<SqliteCommitEvidence>>,
    command: Option<Command>,
    output: Option<(Request, Result<Output, Failure>)>,
    commit: Option<CommitOutcome>,
    dispatch: Option<DispatchGuard>,
    marked: bool,
    withdrawn_commit: bool,
    settled: bool,
    commit_ready: bool,
    closed_connection: bool,
    released_evidence: bool,
    close_result: Option<Result<(), StateStoreError>>,
    operation_started: std::time::Instant,
    commit_started: Option<std::time::Instant>,
    commit_metric_recorded: bool,
}
pub(super) struct TxnOriginal {
    core: Core<TxnBody>,
    scope: InstanceScope,
    cancelled: Arc<AtomicBool>,
    closing: AtomicBool,
    interrupt: Arc<Mutex<Option<Arc<rusqlite::InterruptHandle>>>>,
}
impl TxnOriginal {
    fn queue_command(&self, command: Command) -> Result<Request, Failure> {
        let mut h = lock(&self.core.header);
        if h.complete
            || h.request.is_some()
            || h.advancing
            || h.health.is_some()
            || self.closing.load(Ordering::Acquire)
            || self.cancelled.load(Ordering::Acquire)
        {
            return Err(error(
                StateStoreErrorKind::Cancelled,
                "SQLite original transaction is not accepting business work",
            ));
        }
        let revision = h
            .revision
            .checked_add(1)
            .and_then(std::num::NonZeroU64::new)
            .ok_or_else(|| invalid("SQLite original request revision is exhausted"))?;
        let mut loan = BodyLoan::take(&self.core.body)?;
        let body = loan.value.as_mut().unwrap();
        if body.output.is_some() || body.command.is_some() {
            return Err(invalid(
                "SQLite original input or result remains unconsumed",
            ));
        }
        let request = Request {
            phase: command.phase(body.attempt.is_some()),
            revision,
        };
        body.operation_started = std::time::Instant::now();
        if matches!(command, Command::Commit) {
            body.commit_started = Some(body.operation_started);
        }
        body.command = Some(command);
        // All actual input is restored before the request becomes observable.
        drop(loan);
        h.revision = revision.get();
        h.request = Some(request);
        h.entered = false;
        h.before_publish = false;
        h.result_received = false;
        let retired = h.physical_failure.take();
        drop(h);
        drop(retired);
        self.core.notify();
        Ok(request)
    }
    fn close_if_idle(&self) {
        if !self.core.is_idle() || self.core.is_complete() || self.core.health().is_some() {
            return;
        }
        let should_close = lock(&self.core.body)
            .as_ref()
            .is_some_and(|b| !b.closed_connection);
        if should_close
            && (self.closing.load(Ordering::Acquire) || self.cancelled.load(Ordering::Acquire))
            && let Err(failure) = self.core.queue_if_idle(Phase::Close)
        {
            self.core.fail(failure);
        }
    }
    fn record_commit(body: &mut TxnBody) {
        if !body.commit_metric_recorded
            && let (Some(started), Some(outcome)) = (body.commit_started, body.commit.as_ref())
        {
            body.input.metrics.record_operation(
                crate::metrics::StateStoreOperation::Commit,
                txn::commit_metric_outcome(outcome),
                started.elapsed(),
            );
            body.commit_metric_recorded = true;
        }
    }
    fn settle(body: &mut TxnBody) -> Result<(), StateStoreError> {
        let Some(attempt) = body.attempt.as_ref() else {
            return Ok(());
        };
        if body.settled {
            return Ok(());
        }
        let verdict = match body.commit.as_ref() {
            Some(CommitOutcome::Committed(receipt)) => AttemptOutcome::Committed(receipt.clone()),
            Some(CommitOutcome::CommitUnknown(_)) | None => return Ok(()),
            _ => AttemptOutcome::NotCommitted,
        };
        attempt.settle(verdict)?;
        body.settled = true;
        body.commit_ready = true;
        Self::record_commit(body);
        Ok(())
    }
    // Called only after the same physical exit. No connection/call is borrowed
    // while selecting its next phase; a result alone cannot advance this state.
    fn after_exit(&self, request: Request, exit: Exit) -> Result<Option<Phase>, Failure> {
        let mut loan = BodyLoan::take(&self.core.body)?;
        let b = loan.value.as_mut().unwrap();
        // An unpublished business call never ran its output constructor. Keep
        // its exact received refusal before queueing Close clears the current
        // phase failure; the borrower may not poll until after Close exits.
        if matches!(
            request.phase,
            Phase::BeginRead
                | Phase::BeginWrite
                | Phase::Get
                | Phase::Range
                | Phase::Put
                | Phase::Delete
        ) && b.output.is_none()
            && let Some(failure) = self.core.physical_failure()
        {
            b.output = Some((request, Err(failure)));
        }
        if request.phase == Phase::Commit && exit == Exit::NeverPublishedReturned {
            b.withdrawn_commit = true;
            // This is not a worker liveness proof. The exact withdrawn call
            // cannot run; rollback/close must still return before settlement.
            // Keep the registered liveness guard until the original close's
            // actual exit. A legacy observer must not adjudicate absence while
            // this same unpublished transaction still owns its connection.
        }
        match request.phase {
            Phase::Commit => return Ok(Some(Phase::Close)),
            Phase::Close => {
                if exit != Exit::ActualExited || !b.closed_connection {
                    return Err(invalid("SQLite connection close has not actually returned"));
                }
                drop(b.dispatch.take());
                if let Some(attempt) = b.attempt.as_ref() {
                    if !b.marked {
                        attempt.cancel_before_dispatch().map_err(failure)?;
                        b.settled = true;
                        b.commit_ready = true;
                    } else if b.withdrawn_commit
                        && b.close_result.as_ref().is_some_and(Result::is_ok)
                    {
                        attempt
                            .settle(AttemptOutcome::NotCommitted)
                            .map_err(failure)?;
                        b.settled = true;
                        b.commit_ready = true;
                        b.commit = Some(CommitOutcome::DefiniteFailure(txn::worker_error()));
                        Self::record_commit(b);
                    } else {
                        Self::settle(b).map_err(failure)?;
                    }
                    return Ok(Some(if b.settled {
                        Phase::EvidenceRelease
                    } else {
                        Phase::EvidenceRead
                    }));
                }
                b.input.lifetime.take();
                return Ok(None);
            }
            Phase::EvidenceRead => {
                Self::settle(b).map_err(failure)?;
                if b.settled {
                    return Ok(Some(Phase::EvidenceRelease));
                }
                // Unresolved is retained, never turned into absence proof or
                // a new commit. Existing owner adjudication policy must retry.
                if b.commit.is_none() {
                    b.commit = Some(CommitOutcome::CommitUnknown(txn::worker_error()));
                }
                b.commit_ready = true;
                Self::record_commit(b);
                return Ok(None);
            }
            Phase::EvidenceRelease => {
                if b.released_evidence {
                    return Ok(None);
                }
                // The first-party release error stays in this same original.
                return Ok(None);
            }
            _ => {}
        }
        if exit == Exit::NeverPublishedReturned
            || self.core.physical_failure().is_some()
            || self.closing.load(Ordering::Acquire)
            || self.cancelled.load(Ordering::Acquire)
            || b.state.as_ref().is_none_or(|state| !state.active)
        {
            return Ok(Some(Phase::Close));
        }
        Ok(None)
    }
    fn execute(&self, request: Request, b: &mut TxnBody) {
        match request.phase {
            Phase::Close => {
                drop(b.command.take());
                b.close_result = Some(match b.state.as_mut() {
                    Some(s) => txn::rollback(s),
                    None => Ok(()),
                });
                // Actual connection destruction remains on the original worker.
                drop(b.state.take());
                b.closed_connection = true;
                if b.attempt.is_none() {
                    drop(b.input.lifetime.take());
                }
            }
            Phase::EvidenceRead => {
                let attempt = b.attempt.as_ref().unwrap().id();
                let outcome = b
                    .evidence
                    .as_ref()
                    .unwrap()
                    .adjudicate_original_blocking(attempt);
                match outcome {
                    AttemptOutcome::Committed(receipt) => {
                        b.commit = Some(CommitOutcome::Committed(receipt))
                    }
                    AttemptOutcome::NotCommitted => {
                        let failure = match b.commit.take() {
                            Some(CommitOutcome::CommitUnknown(error)) => error,
                            _ => txn::worker_error(),
                        };
                        b.commit = Some(CommitOutcome::DefiniteFailure(failure));
                    }
                    AttemptOutcome::Unresolved => {
                        // Preserve the original typed unknown reason; a failed
                        // evidence read adds no new commit knowledge.
                        if b.commit.is_none() {
                            b.commit = Some(CommitOutcome::CommitUnknown(txn::worker_error()));
                        }
                    }
                }
            }
            Phase::EvidenceRelease => {
                let result = b
                    .evidence
                    .as_ref()
                    .unwrap()
                    .release_original_blocking(b.attempt.as_ref().unwrap().id());
                if result.is_ok() {
                    b.released_evidence = true;
                    drop(b.attempt.take());
                    drop(b.evidence.take());
                    drop(b.input.lifetime.take());
                }
                b.close_result = Some(match (b.close_result.take(), result) {
                    (Some(Err(first)), Err(later)) => Err(first.with_cleanup_context(later)),
                    (Some(Err(first)), Ok(())) => Err(first),
                    (_, result) => result,
                });
            }
            Phase::Commit => {
                drop(b.command.take());
                let _dispatch = b.dispatch.take().expect("same registered commit call");
                #[cfg(test)]
                {
                    b.input.hooks.pause_commit_before_apply();
                    b.input.hooks.panic_commit_before_apply();
                }
                b.commit = Some(txn::commit_blocking(
                    b.state.as_mut().unwrap(),
                    b.attempt.as_ref().unwrap().id(),
                    b.evidence.as_ref().unwrap(),
                ));
                #[cfg(test)]
                b.input.hooks.panic_commit_after_apply();
            }
            _ => {
                let command = b.command.take().expect("same original closed input");
                let started = b.operation_started;
                let metrics = &b.input.metrics;
                let limits = &b.input.limits;
                let attempt = b.attempt.as_ref().map(WriteAttempt::id);
                use crate::metrics::StateStoreOperation as Operation;
                let result = match command {
                    Command::Begin => {
                        let result = txn::begin_transaction_blocking(
                            b.input.path.clone(),
                            b.input.limits.clone(),
                            b.input.deadline,
                            self.cancelled.clone(),
                            self.interrupt.clone(),
                            b.input.lifetime.as_ref().unwrap().clone(),
                            #[cfg(test)]
                            b.input.hooks.clone(),
                        );
                        txn::record_result(metrics, Operation::Begin, started, &result);
                        result.map(|state| {
                            b.state = Some(state);
                            Output::Unit
                        })
                    }
                    Command::Get(key) => {
                        let result = txn::run_operation_blocking(b.state.as_mut().unwrap(), |s| {
                            txn::validate_key_value(&key, None, limits)?;
                            txn::get_blocking(s, key)
                        });
                        txn::record_read_result(metrics, Operation::Get, started, &result);
                        result.map(Output::Record)
                    }
                    Command::Range(request) => {
                        let result = txn::run_operation_blocking(b.state.as_mut().unwrap(), |s| {
                            txn::validate_range_request(&request, limits)?;
                            let page = crate::range::range_page(s, &request)?;
                            if attempt.is_some() && page.continuation.is_some() {
                                s.range_frozen = true;
                            }
                            Ok(page)
                        });
                        txn::record_range_result(metrics, started, &result);
                        result.map(Output::Page)
                    }
                    Command::Put(key, value, precondition) => {
                        let result = txn::run_operation_blocking(b.state.as_mut().unwrap(), |s| {
                            txn::validate_key_value(&key, Some(&value), limits)?;
                            let measured = txn::accounted_mutation_bytes(
                                &key,
                                &txn::Mutation::Put {
                                    value: value.clone(),
                                    precondition: precondition.clone(),
                                    provisional_version: txn::provisional_version(
                                        attempt.unwrap(),
                                        1,
                                    ),
                                },
                            )?;
                            let next = txn::next_operation_count(s)?;
                            txn::stage_mutation(
                                s,
                                key,
                                txn::Mutation::Put {
                                    value,
                                    precondition,
                                    provisional_version: txn::provisional_version(
                                        attempt.unwrap(),
                                        next,
                                    ),
                                },
                                next,
                            )?;
                            Ok(measured)
                        });
                        txn::record_write_result(metrics, Operation::Put, started, &result);
                        result.map(|_| Output::Unit)
                    }
                    Command::Delete(key, precondition) => {
                        let result = txn::run_operation_blocking(b.state.as_mut().unwrap(), |s| {
                            txn::validate_key_value(&key, None, limits)?;
                            let measured = txn::accounted_mutation_bytes(
                                &key,
                                &txn::Mutation::Delete {
                                    precondition: precondition.clone(),
                                },
                            )?;
                            let next = txn::next_operation_count(s)?;
                            txn::stage_mutation(
                                s,
                                key,
                                txn::Mutation::Delete { precondition },
                                next,
                            )?;
                            Ok(measured)
                        });
                        txn::record_write_result(metrics, Operation::Delete, started, &result);
                        result.map(|_| Output::Unit)
                    }
                    Command::Commit => unreachable!("commit has its registered phase"),
                };
                b.output = Some((request, result.map_err(failure)));
            }
        }
    }
    async fn idle(&self) -> Result<(), Failure> {
        std::future::poll_fn(|cx| {
            if let Some(failure) = self.core.health() {
                return Poll::Ready(Err(failure));
            }
            if self.core.is_idle() {
                return Poll::Ready(Ok(()));
            }
            match self.core.poll_step(cx) {
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                _ => Poll::Pending,
            }
        })
        .await
    }
    async fn call(&self, command: Command) -> Result<Output, Failure> {
        self.idle().await?;
        let request = self.queue_command(command)?;
        let mut guard = InterruptOnDrop {
            original: self,
            armed: true,
        };
        let result = std::future::poll_fn(|cx| {
            let progress = self.core.poll_step(cx);
            let mut body = lock(&self.core.body);
            if let Some(b) = body.as_mut()
                && b.output.as_ref().is_some_and(|(r, _)| *r == request)
            {
                let (_, result) = b.output.take().unwrap();
                return Poll::Ready(result);
            }
            match progress {
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                _ => Poll::Pending,
            }
        })
        .await;
        guard.armed = false;
        result
    }
}
impl Original for TxnOriginal {
    fn origin(&self) -> Origin {
        Origin::Instance(self.scope)
    }
    fn requested(&self) -> Option<Request> {
        self.close_if_idle();
        self.core.request()
    }
    fn publication_refusal(&self, request: Request) -> Option<Failure> {
        if self.core.request() != Some(request) {
            return Some(invalid("SQLite transaction stop observation is stale"));
        }
        if matches!(
            request.phase,
            Phase::Close | Phase::Rollback | Phase::EvidenceRead | Phase::EvidenceRelease
        ) {
            return None;
        }
        if self.cancelled.load(Ordering::Acquire) || self.closing.load(Ordering::Acquire) {
            return Some(error(
                StateStoreErrorKind::Cancelled,
                "SQLite original transaction was stopped",
            ));
        }
        (std::time::Instant::now() >= self.core.execution.deadline()).then(|| {
            error(
                StateStoreErrorKind::DeadlineExceeded,
                "SQLite original transaction deadline exceeded",
            )
        })
    }
    fn before_publish(&self, request: Request) -> Result<(), Failure> {
        self.core.prepare(request)?;
        if request.phase == Phase::Commit {
            let mut loan = BodyLoan::take(&self.core.body)?;
            let b = loan.value.as_mut().unwrap();
            b.dispatch = Some(
                b.evidence
                    .as_ref()
                    .unwrap()
                    .register_original_dispatch(b.attempt.as_ref().unwrap().id())
                    .map_err(failure)?,
            );
            b.attempt
                .as_ref()
                .unwrap()
                .mark_dispatched()
                .map_err(failure)?;
            b.marked = true;
        }
        Ok(())
    }
    fn run_original_phase(&self, request: Request) {
        if !self.core.enter(request) {
            return;
        }
        match BodyLoan::take(&self.core.body) {
            Ok(mut loan) => self.execute(request, loan.value.as_mut().unwrap()),
            Err(failure) => self.core.fail(failure),
        }
    }
    fn receive_result(&self, request: Request, result: Result<(), Failure>) {
        if result.is_err()
            && let Some(body) = lock(&self.core.body).as_ref()
        {
            body.input.metrics.record_blocking_failure();
        }
        self.core.result(request, result);
    }
    fn receive_exit(&self, request: Request, exit: Result<Exit, Failure>) {
        let Some(exit) = self.core.exit(request, exit) else {
            return;
        };
        // The physical footer is recorded, but a failed settlement call cannot
        // authorize another phase or a positive complete fact.
        if self.core.health().is_some() {
            self.core.notify();
            return;
        }
        match self.after_exit(request, exit) {
            Ok(Some(next)) => {
                if let Err(failure) = self.core.queue(next) {
                    self.core.fail(failure);
                }
            }
            Ok(None) => {
                let b = lock(&self.core.body);
                let complete = b.as_ref().is_some_and(|b| {
                    b.closed_connection && (b.attempt.is_none() || b.released_evidence)
                });
                drop(b);
                if complete {
                    self.core.complete();
                } else {
                    self.core.idle();
                }
            }
            Err(failure) => self.core.fail(failure),
        }
        self.core.notify();
    }
    fn is_complete(&self) -> bool {
        self.core.is_complete()
    }
    fn local_failure(&self) -> Option<Failure> {
        self.core.local_failure()
    }
    fn request_interrupt(&self) {
        self.cancelled.store(true, Ordering::Release);
        let interrupt = lock(&self.interrupt).clone();
        if let Some(interrupt) = interrupt {
            interrupt.interrupt();
        }
        self.core.notify();
    }
    fn request_close(&self) {
        self.closing.store(true, Ordering::Release);
        self.request_interrupt();
        // A new explicit close observation may retry the same original debt.
        // requested() never manufactures a self-waking evidence retry loop.
        if self.core.is_idle() && !self.core.is_complete() && self.core.health().is_none() {
            let retry = lock(&self.core.body).as_ref().and_then(|b| {
                (b.closed_connection && b.attempt.is_some()).then_some(if b.settled {
                    Phase::EvidenceRelease
                } else {
                    Phase::EvidenceRead
                })
            });
            if let Some(phase) = retry
                && let Err(failure) = self.core.queue_if_idle(phase)
            {
                self.core.fail(failure);
            }
            self.core.notify();
        }
    }
}
struct InterruptOnDrop<'a> {
    original: &'a TxnOriginal,
    armed: bool,
}
impl Drop for InterruptOnDrop<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.original.request_interrupt();
        }
    }
}
struct Transaction {
    original: Arc<TxnOriginal>,
    attempt: Option<AttemptId>,
}
impl Drop for Transaction {
    fn drop(&mut self) {
        self.original.request_close();
    }
}
#[async_trait::async_trait]
impl OriginalReadTransaction for Transaction {
    async fn get_in(&mut self, key: &Key) -> Result<Option<StateRecord>, Failure> {
        match self.original.call(Command::Get(key.clone())).await? {
            Output::Record(value) => Ok(value),
            _ => Err(invalid("SQLite original get returned another output")),
        }
    }
    async fn range_in(&mut self, request: &RangeRequest) -> Result<RangePage, Failure> {
        match self.original.call(Command::Range(request.clone())).await? {
            Output::Page(value) => Ok(value),
            _ => Err(invalid("SQLite original range returned another output")),
        }
    }
    async fn close_in(&mut self) -> Result<(), Failure> {
        self.original.request_close();
        std::future::poll_fn(|cx| {
            let progress = self.original.core.poll_step(cx);
            if self.original.core.is_complete() {
                let body = lock(&self.original.core.body);
                return Poll::Ready(
                    body.as_ref()
                        .unwrap()
                        .close_result
                        .clone()
                        .unwrap_or(Ok(()))
                        .map_err(failure),
                );
            }
            match progress {
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                _ => Poll::Pending,
            }
        })
        .await
    }
}
#[async_trait::async_trait]
impl OriginalWriteTransaction for Transaction {
    fn attempt(&self) -> AttemptId {
        self.attempt.unwrap()
    }
    fn original_failure(&self) -> Option<Failure> {
        lock(&self.original.core.header)
            .first_physical_failure
            .clone()
    }
    async fn put_in(
        &mut self,
        key: Key,
        value: Value,
        precondition: Precondition,
    ) -> Result<(), Failure> {
        self.original
            .call(Command::Put(key, value, precondition))
            .await
            .map(|_| ())
    }
    async fn delete_in(&mut self, key: Key, precondition: Precondition) -> Result<(), Failure> {
        self.original
            .call(Command::Delete(key, precondition))
            .await
            .map(|_| ())
    }
    async fn commit_in(&mut self) -> Result<CommitOutcome, Failure> {
        if let Some(outcome) = lock(&self.original.core.body)
            .as_ref()
            .filter(|body| body.commit_ready)
            .and_then(|body| body.commit.clone())
        {
            return Ok(outcome);
        }
        self.original.idle().await?;
        let ready = lock(&self.original.core.body)
            .as_ref()
            .unwrap()
            .commit_ready;
        if !ready {
            self.original.queue_command(Command::Commit)?;
        }
        let mut guard = InterruptOnDrop {
            original: &self.original,
            armed: true,
        };
        let result = std::future::poll_fn(|cx| {
            let progress = self.original.core.poll_step(cx);
            let body = lock(&self.original.core.body);
            if let Some(body) = body.as_ref()
                && body.commit_ready
            {
                return Poll::Ready(Ok(body
                    .commit
                    .clone()
                    .unwrap_or_else(|| CommitOutcome::DefiniteFailure(txn::worker_error()))));
            }
            drop(body);
            if let Some(failure) = self.original.core.health() {
                return Poll::Ready(Err(failure));
            }
            let _ = progress;
            Poll::Pending
        })
        .await;
        guard.armed = false;
        result
    }
}
fn form(
    store: &crate::SqliteStateStore,
    attempt: Option<WriteAttempt>,
    execution: &Execution,
) -> Result<Arc<TxnOriginal>, Failure> {
    let id = attempt.as_ref().map(WriteAttempt::id);
    if let Some(attempt) = &attempt {
        attempt
            .require_scope(store.attempts.scope())
            .map_err(failure)?;
        txn::validate_transaction_envelope(&store.limits).map_err(failure)?;
    }
    let deadline = std::time::Instant::now()
        .checked_add(store.limits.transaction_deadline)
        .ok_or_else(|| invalid("SQLite transaction deadline is not representable"))?
        .min(execution.deadline());
    let original = Arc::new(TxnOriginal {
        core: Core::new(
            TxnBody {
                input: Input {
                    path: store.path.clone(),
                    limits: store.limits.clone(),
                    deadline,
                    lifetime: Some(store.lifetime.clone()),
                    metrics: store.metrics.clone(),
                    #[cfg(test)]
                    hooks: store.test_hooks.clone(),
                },
                state: None,
                evidence: id.map(|_| store.evidence.clone()),
                attempt,
                command: None,
                output: None,
                commit: None,
                dispatch: None,
                marked: false,
                withdrawn_commit: false,
                settled: false,
                commit_ready: false,
                closed_connection: false,
                released_evidence: false,
                close_result: None,
                operation_started: std::time::Instant::now(),
                commit_started: None,
                commit_metric_recorded: false,
            },
            execution.with_deadline(deadline),
        ),
        scope: store.attempts.scope(),
        cancelled: Arc::new(AtomicBool::new(false)),
        closing: AtomicBool::new(false),
        interrupt: Arc::new(Mutex::new(None)),
    });
    original.queue_command(Command::Begin)?;
    original
        .core
        .bind(&(original.clone() as Arc<dyn Original>))?;
    Ok(original)
}
pub(crate) async fn begin_read_in(
    store: &crate::SqliteStateStore,
    execution: &Execution,
) -> Result<Box<dyn OriginalReadTransaction>, Failure> {
    let original = form(store, None, execution)?;
    let transaction = Transaction {
        original: original.clone(),
        attempt: None,
    };
    // Begin was already installed before first polling. Borrow it without
    // forming another request, pool, Root, or disposable unique connection.
    wait_begin(&original).await?;
    Ok(Box::new(transaction))
}
pub(crate) async fn begin_write_in(
    store: &crate::SqliteStateStore,
    attempt: WriteAttempt,
    execution: &Execution,
) -> Result<Box<dyn OriginalWriteTransaction>, Failure> {
    let id = attempt.id();
    let original = form(store, Some(attempt), execution)?;
    let transaction = Transaction {
        original: original.clone(),
        attempt: Some(id),
    };
    wait_begin(&original).await?;
    Ok(Box::new(transaction))
}
async fn wait_begin(original: &Arc<TxnOriginal>) -> Result<(), Failure> {
    let mut guard = InterruptOnDrop {
        original,
        armed: true,
    };
    let result = std::future::poll_fn(|cx| {
        let progress = original.core.poll_step(cx);
        let mut b = lock(&original.core.body);
        if let Some(body) = b.as_mut()
            && body
                .output
                .as_ref()
                .is_some_and(|(r, _)| matches!(r.phase, Phase::BeginRead | Phase::BeginWrite))
        {
            let (_, value) = body.output.take().unwrap();
            return Poll::Ready(value.map(|_| ()));
        }
        match progress {
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            _ => Poll::Pending,
        }
    })
    .await;
    guard.armed = false;
    result
}
