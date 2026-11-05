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

//! Existing scan SQL actors with an explicit statement submission gate.
use super::super::native_sql_controls::SelectedStatement;
use crate::{
    actors::sql::{SqlSession, StatementFailure, StatementResult},
    scenario::ScenarioContext,
};
use anyhow::{Context, Result};
use novarocks_cluster_harness::TestClusterOwner;
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Instant;

#[derive(Clone, Copy)]
enum SqlOutcome {
    Terminal,
    NonTerminal,
}
fn terminal<T>(result: &std::result::Result<T, StatementFailure>) -> SqlOutcome {
    match result {
        Ok(_) | Err(StatementFailure::Trino(_)) => SqlOutcome::Terminal,
        Err(_) => SqlOutcome::NonTerminal,
    }
}
pub(in crate::scenarios) struct ScanActor<T> {
    pub client_query_id: String,
    pub done: mpsc::Receiver<std::result::Result<T, StatementFailure>>,
    actor: Option<JoinHandle<Result<()>>>,
    cancel: novarocks_test_trino_client::CancelHandle,
    deadline: Instant,
    outcome: Arc<Mutex<Option<SqlOutcome>>>,
    observation: Option<novarocks_cluster_harness::QueryLifecycleStatementObservation>,
}
impl<T: Send + 'static> ScanActor<T> {
    pub fn start(
        context: &mut ScenarioContext,
        sql: String,
        selected: Option<SelectedStatement>,
        decode: impl FnOnce(StatementResult) -> std::result::Result<T, StatementFailure>
        + Send
        + 'static,
    ) -> Result<Self> {
        Self::start_with_setup(context, sql, selected, |_| Ok(()), decode)
    }
    pub fn start_with_setup(
        context: &mut ScenarioContext,
        sql: String,
        selected: Option<SelectedStatement>,
        setup: impl FnOnce(&mut SqlSession) -> Result<()> + Send + 'static,
        decode: impl FnOnce(StatementResult) -> std::result::Result<T, StatementFailure>
        + Send
        + 'static,
    ) -> Result<Self> {
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let (submit_tx, submit_rx) =
            mpsc::sync_channel::<novarocks_test_trino_client::PendingStatement>(1);
        let (done_tx, done) = mpsc::sync_channel(1);
        let user = context.sql_user().to_owned();
        let port = context.trino_port();
        context.remaining("connect original scan actor")?;
        let deadline = context.deadline();
        let actor_sql = sql.clone();
        let outcome = Arc::new(Mutex::new(None));
        let observed = outcome.clone();
        let actor = thread::Builder::new()
            .name("selected-scan-sql".into())
            .spawn(move || -> Result<()> {
                let mut session = SqlSession::new(&user, port, deadline)?;
                setup(&mut session)?;
                let pending = session.submit(&actor_sql)?;
                if let Err(mpsc::SendError(pending)) = ready_tx.send(pending) {
                    return combine(
                        Err(anyhow::anyhow!(
                            "original scan admission owner closed before Pending handoff"
                        )),
                        pending
                            .cancel_handle()
                            .delete(deadline)
                            .map_err(anyhow::Error::from),
                    );
                }
                let pending = match submit_rx
                    .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                {
                    Ok(pending) => pending,
                    Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        anyhow::bail!("original SQL submission gate deadline expired")
                    }
                };
                let result = session.run_pending(pending);
                *observed
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(terminal(&result));
                match done_tx.send(result.and_then(decode)) {
                    Ok(()) => Ok(()),
                    Err(mpsc::SendError(Err(actual))) => Err(anyhow::Error::new(actual)
                        .context("original scan query failed after its result owner closed")),
                    Err(mpsc::SendError(Ok(_))) => {
                        anyhow::bail!("original scan result owner closed before delivery")
                    }
                }
            })?;
        let mut actual_cancel = None;
        let ready = (|| -> Result<_> {
            let pending = ready_rx
                .recv_timeout(context.remaining("receive original scan Pending")?)
                .context("original scan actor did not publish its actual Pending")?;
            let client_query_id = pending.query_id.clone();
            let cancel = pending.cancel_handle();
            actual_cancel = Some(cancel.clone());
            let (pending, observation) = if let Some(selected) = selected {
                let (pending, observation) = selected.arm_pending(context, pending, &sql)?;
                (pending, Some(observation))
            } else {
                (pending, None)
            };
            Ok((pending, client_query_id, cancel, observation))
        })();
        let (pending, client_query_id, cancel, observation) = match ready {
            Ok(original) => original,
            Err(primary) => {
                drop(submit_tx);
                let joined = join(actor);
                // A successful worker->parent send may race the parent's
                // bounded receive. Drain that same original only after join.
                if let Ok(pending) = ready_rx.try_recv() {
                    actual_cancel = Some(pending.cancel_handle());
                }
                let cancelled = actual_cancel.map_or(Ok(()), |cancel| {
                    cancel.delete(deadline).map_err(anyhow::Error::from)
                });
                return combine(Err(primary), combine(joined, cancelled));
            }
        };
        if let Err(mpsc::SendError(pending)) = submit_tx.send(pending) {
            let cancelled = pending
                .cancel_handle()
                .delete(deadline)
                .map_err(anyhow::Error::from);
            let drained = observation.as_ref().map_or(Ok(()), |observation| {
                context
                    .handle()
                    .drain_query_lifecycle_statement_binding(observation, deadline)
            });
            return combine(
                Err(anyhow::anyhow!(
                    "original scan consumer closed before first GET"
                )),
                combine(combine(cancelled, join(actor)), drained),
            );
        }
        Ok(Self {
            client_query_id,
            done,
            actor: Some(actor),
            cancel,
            deadline,
            outcome,
            observation,
        })
    }
    pub fn is_finished(&self) -> bool {
        self.actor.as_ref().is_none_or(|actor| actor.is_finished())
    }
    pub fn cancel(&self) -> Result<()> {
        self.cancel
            .delete(self.deadline)
            .map_err(anyhow::Error::from)
    }
    /// A joined client reader is not a server terminal receipt.
    pub fn settle<U>(mut self, context: &mut ScenarioContext, primary: Result<U>) -> Result<U> {
        let before = *self
            .outcome
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let attempted = primary.is_err() && !matches!(before, Some(SqlOutcome::Terminal));
        let mut cleanup = if attempted { self.cancel() } else { Ok(()) };
        if cleanup.is_err() {
            cleanup = combine(cleanup, context.shutdown());
        }
        if let Some(actor) = self.actor.take() {
            cleanup = combine(cleanup, join(actor));
        }
        let after = *self
            .outcome
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !attempted && !matches!(after, Some(SqlOutcome::Terminal)) {
            let cancelled = self.cancel();
            cleanup = combine(
                cleanup,
                if cancelled.is_err() {
                    combine(cancelled, context.shutdown())
                } else {
                    cancelled
                },
            );
        }
        if let Ok(Err(query_error)) = self.done.try_recv() {
            cleanup = combine(cleanup, Err(query_error.into()));
        }
        let drained = self.observation.as_ref().map_or(Ok(()), |observation| {
            context
                .handle()
                .drain_query_lifecycle_statement_binding(observation, self.deadline)
        });
        combine(primary, combine(cleanup, drained))
    }
}
impl<T> Drop for ScanActor<T> {
    fn drop(&mut self) {
        if let Some(actor) = self.actor.take() {
            let before = *self
                .outcome
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let attempted = !matches!(before, Some(SqlOutcome::Terminal));
            if attempted {
                let _ = self.cancel.delete(self.deadline);
            }
            let _ = join(actor);
            let after = *self
                .outcome
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if !attempted && !matches!(after, Some(SqlOutcome::Terminal)) {
                let _ = self.cancel.delete(self.deadline);
            }
        }
    }
}
pub(in crate::scenarios) fn join(actor: JoinHandle<Result<()>>) -> Result<()> {
    actor
        .join()
        .map_err(|payload| ActorPanic(Mutex::new(payload)))?
}
pub(in crate::scenarios) fn combine<T>(primary: Result<T>, cleanup: Result<()>) -> Result<T> {
    match (primary, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(primary), Err(cleanup)) => Err(ActorCleanupFailure { primary, cleanup }.into()),
    }
}
pub(in crate::scenarios) struct ActorCleanupFailure {
    primary: anyhow::Error,
    cleanup: anyhow::Error,
}
impl ActorCleanupFailure {
    /// Borrow the actual errors without reclassifying their causal order.
    pub(in crate::scenarios) fn original_primary(&self) -> &anyhow::Error {
        &self.primary
    }
    pub(in crate::scenarios) fn actual_cleanup(&self) -> &anyhow::Error {
        &self.cleanup
    }
}
impl std::fmt::Debug for ActorCleanupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::fmt::Display for ActorCleanupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "original scan operation failed: {:#}; actual actor cleanup failed: {:#}",
            self.primary, self.cleanup
        )
    }
}
impl std::error::Error for ActorCleanupFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.primary.as_ref())
    }
}
struct ActorPanic(Mutex<Box<dyn std::any::Any + Send>>);
impl std::fmt::Debug for ActorPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::fmt::Display for ActorPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _ = &self.0;
        f.write_str("original scan actor panicked; source retained")
    }
}
impl std::error::Error for ActorPanic {}
