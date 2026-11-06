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

//! SQL actor with an exact statement nomination gate and joined failure cleanup.
use super::native_sql_controls::SelectedStatement;
use crate::{
    actors::sql::{self, PendingStatement, SqlSession, StatementFailure, StatementResult},
    scenario::ScenarioContext,
};
use anyhow::{Context, Result};
use novarocks_cluster_harness::{QueryLifecycleStatementObservation, TestClusterOwner};
use novarocks_test_trino_client::CancelHandle;
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Instant;

pub(super) struct WriterActor {
    pub client_query_id: String,
    pub done: mpsc::Receiver<std::result::Result<StatementResult, StatementFailure>>,
    actor: Option<JoinHandle<Result<()>>>,
    cancel: CancelHandle,
    deadline: Instant,
    observation: QueryLifecycleStatementObservation,
    sql_outcome: Arc<Mutex<Option<SqlOutcome>>>,
    reentry_release: Option<mpsc::SyncSender<()>>,
}
#[derive(Clone, Copy)]
enum SqlOutcome {
    Success,
    PublicError,
    TransportError,
}

#[derive(Clone, Copy)]
pub(super) struct ReadSessionSettings {
    pub pipeline_dop: u32,
}
impl ReadSessionSettings {
    pub fn apply(self, connection: &mut SqlSession) -> Result<()> {
        anyhow::ensure!(self.pipeline_dop > 0, "read control DOP must be positive");
        // Only this original session property is supported. Memory is frozen
        // by each statement's admitted hint; batch size remains its default.
        // The scenario checks the actual build participant count separately.
        connection.execute(format!("SET pipeline_dop={}", self.pipeline_dop))?;
        Ok(())
    }
}

pub(super) fn require_public_error_reentry(connection: &mut SqlSession) -> Result<()> {
    let result = connection
        .execute("SELECT 1")
        .context("reenter the original writer session after its public SQL error")?;
    let rows = sql::integer_column(&result)?;
    anyhow::ensure!(rows == vec![1], "writer session reentry returned {rows:?}");
    Ok(())
}
impl WriterActor {
    pub fn observation(&self) -> &QueryLifecycleStatementObservation {
        &self.observation
    }

    pub fn start(
        context: &mut ScenarioContext,
        sql: &str,
        selected: SelectedStatement,
        query_timeout_secs: Option<u64>,
    ) -> Result<Self> {
        Self::start_internal(context, sql, selected, query_timeout_secs, None, None)
    }
    pub fn start_read(
        context: &mut ScenarioContext,
        sql: &str,
        selected: SelectedStatement,
        result: Arc<Mutex<Option<StatementResult>>>,
        settings: Option<ReadSessionSettings>,
    ) -> Result<Self> {
        Self::start_internal(context, sql, selected, None, Some(result), settings)
    }
    fn start_internal(
        context: &mut ScenarioContext,
        sql: &str,
        selected: SelectedStatement,
        query_timeout_secs: Option<u64>,
        read_result: Option<Arc<Mutex<Option<StatementResult>>>>,
        read_settings: Option<ReadSessionSettings>,
    ) -> Result<Self> {
        let (ready_tx, ready_rx) = mpsc::sync_channel::<PendingStatement>(1);
        let (submit_tx, submit_rx) = mpsc::sync_channel::<PendingStatement>(1);
        let (done_tx, done) = mpsc::sync_channel(1);
        let (reentry_release, reentry_wait) = mpsc::sync_channel(1);
        let user = context.sql_user().to_owned();
        let port = context.trino_port();
        context.remaining("connect original writer actor")?;
        let deadline = context.deadline();
        let actor_sql = sql.to_owned();
        let sql_outcome = Arc::new(Mutex::new(None));
        let actor_outcome = Arc::clone(&sql_outcome);
        let actor = thread::Builder::new()
            .name("selected-distributed-writer-sql".into())
            .spawn(move || -> Result<()> {
                let mut connection = SqlSession::new(&user, port, deadline)?;
                if let Some(settings) = read_settings {
                    settings.apply(&mut connection)?;
                }
                if let Some(seconds) = query_timeout_secs {
                    connection
                        .execute(format!("SET query_timeout = {seconds}"))
                        .context("set the deadline write session query_timeout")?;
                }
                let pending = connection.submit(&actor_sql)?;
                let cancel = pending.cancel_handle();
                if ready_tx.send(pending).is_err() {
                    return combine(
                        Err(anyhow::anyhow!("original writer ready owner closed")),
                        cancel.delete(deadline).map_err(Into::into),
                    );
                }
                let pending = match submit_rx
                    .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                {
                    Ok(pending) => pending,
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        return cancel.delete(deadline).map_err(Into::into);
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        return combine(
                            Err(anyhow::anyhow!(
                                "original SQL submission gate deadline expired"
                            )),
                            cancel.delete(deadline).map_err(Into::into),
                        );
                    }
                };
                let outcome = connection.run_pending(pending);
                let classification = match &outcome {
                    Ok(_) => SqlOutcome::Success,
                    Err(StatementFailure::Trino(_)) => SqlOutcome::PublicError,
                    Err(_) => SqlOutcome::TransportError,
                };
                // Capture the same completed response, including its original
                // types, query id and page count, without a second SQL reader.
                let captured = match (&outcome, read_result) {
                    (Ok(actual), Some(result)) => result
                        .lock()
                        .map(|mut saved| *saved = Some(actual.clone()))
                        .map_err(|_| anyhow::anyhow!("original read result owner poisoned")),
                    _ => Ok(()),
                };
                let recorded = actor_outcome
                    .lock()
                    .map(|mut saved| *saved = Some(classification))
                    .map_err(|_| anyhow::anyhow!("original writer SQL outcome owner poisoned"));
                let delivery = match done_tx.send(outcome) {
                    Ok(()) => Ok(()),
                    Err(mpsc::SendError(Err(actual))) => Err(anyhow::Error::from(actual)
                        .context("original writer query failed after its result owner closed")),
                    Err(mpsc::SendError(Ok(_))) => Err(anyhow::anyhow!(
                        "original writer result owner closed before delivery"
                    )),
                };
                // Keep successful effects and public-error reentry on this same
                // actor session, after the original writer-only counter window.
                let reentry = match reentry_wait
                    .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                {
                    Ok(()) if matches!(classification, SqlOutcome::PublicError) => {
                        require_public_error_reentry(&mut connection)
                    }
                    Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => Ok(()),
                    Err(mpsc::RecvTimeoutError::Timeout) => Err(anyhow::anyhow!(
                        "original writer reentry gate deadline expired"
                    )),
                };
                combine(combine(combine(delivery, reentry), recorded), captured)
            })?;
        let ready = (|| -> Result<_> {
            let pending = ready_rx
                .recv_timeout(context.remaining("receive idle writer session")?)
                .context("original writer actor did not publish its actual POST")?;
            let cancel = pending.cancel_handle();
            let client_query_id = pending.query_id.clone();
            let (pending, observation) = selected.arm_pending(context, pending, sql)?;
            Ok((client_query_id, cancel, pending, observation))
        })();
        let (client_query_id, cancel, pending, observation) = match ready {
            Ok(ready) => ready,
            Err(primary) => {
                drop(submit_tx);
                drop(reentry_release);
                return combine::<Self>(Err(primary), join(actor));
            }
        };
        if let Err(primary) = submit_tx.send(pending) {
            drop(reentry_release);
            let cleanup = combine(
                cancel.delete(deadline).map_err(Into::into),
                combine(
                    join(actor),
                    context
                        .handle()
                        .drain_query_lifecycle_statement_binding(&observation, deadline),
                ),
            );
            return combine::<Self>(
                Err(anyhow::anyhow!(
                    "original writer submission owner closed: {primary}"
                )),
                cleanup,
            );
        }
        Ok(Self {
            client_query_id,
            done,
            actor: Some(actor),
            cancel,
            deadline,
            observation,
            sql_outcome,
            reentry_release: Some(reentry_release),
        })
    }
    pub fn cancel(&self, deadline: Instant) -> Result<()> {
        self.cancel
            .delete(deadline)
            .with_context(|| format!("cancel original writer query {}", self.client_query_id))
    }

    /// Every known success/failure path settles the same original actor. Public
    /// cancellation and cluster shutdown are failure-only cleanup, never new admission.
    pub fn settle(
        mut self,
        context: &mut ScenarioContext,
        primary: Result<std::result::Result<StatementResult, StatementFailure>>,
    ) -> Result<std::result::Result<StatementResult, StatementFailure>> {
        let mut cleanup = Ok(());
        if primary.is_err()
            && self
                .sql_outcome
                .lock()
                .map(|saved| saved.is_none())
                .unwrap_or(true)
            && self
                .actor
                .as_ref()
                .is_some_and(|actor| !actor.is_finished())
        {
            cleanup = self
                .cancel
                .delete(self.deadline)
                .map_err(anyhow::Error::from);
            if cleanup.is_err() {
                cleanup = combine(cleanup, context.shutdown());
            }
        }
        // Cancellation above cannot strike SELECT 1: reentry is still gated until the
        // original SQL is known complete or its cancellation has returned.
        if let Some(release) = self.reentry_release.take() {
            let released = release.send(()).map_err(|_| {
                anyhow::anyhow!("original writer reentry owner exited before release")
            });
            cleanup = combine(cleanup, released);
        }
        if let Some(actor) = self.actor.take() {
            cleanup = combine(cleanup, join(actor));
        }
        if let Ok(Err(query_error)) = self.done.try_recv() {
            cleanup = combine(cleanup, Err(query_error.into()));
        }
        let classification = self
            .sql_outcome
            .lock()
            .map(|saved| *saved)
            .map_err(|_| anyhow::anyhow!("original writer SQL outcome owner poisoned"));
        // A joined HTTP/client reader is not a server terminal receipt. The
        // same registered query still needs DELETE on every non-Trino verdict.
        let actual_terminal = matches!(
            &classification,
            Ok(Some(SqlOutcome::Success | SqlOutcome::PublicError))
        );
        let nonterminal_client_failure = !actual_terminal
            && (matches!(&classification, Ok(Some(SqlOutcome::TransportError)))
                || matches!(&primary, Ok(Err(error)) if !matches!(error, StatementFailure::Trino(_))));
        if nonterminal_client_failure {
            let cancelled = self
                .cancel
                .delete(self.deadline)
                .map_err(anyhow::Error::from);
            let shutdown = if cancelled.is_err() {
                context.shutdown()
            } else {
                Ok(())
            };
            cleanup = combine(cleanup, combine(cancelled, shutdown));
        }
        match classification {
            Ok(Some(SqlOutcome::Success)) => {
                cleanup = combine(
                    cleanup,
                    context
                        .handle()
                        .await_query_lifecycle_statement_observation(
                            &self.observation,
                            true,
                            self.deadline,
                        )
                        .map(|_| ()),
                );
                cleanup = combine(
                    cleanup,
                    context
                        .handle()
                        .drain_query_lifecycle_statement_binding(&self.observation, self.deadline),
                );
            }
            classification => {
                if let Err(error) = classification {
                    cleanup = combine(cleanup, Err(error));
                }
                // The actor was joined; its public-error path also checked
                // reentry. Binding attribution is not a completion proof.
                cleanup = combine(
                    cleanup,
                    context
                        .handle()
                        .drain_query_lifecycle_statement_binding(&self.observation, self.deadline),
                );
            }
        }
        let primary = match primary {
            Ok(Err(error)) if !matches!(&error, StatementFailure::Trino(_)) => Err(error.into()),
            primary => primary,
        };
        retain_write_result(primary, cleanup)
    }
}
impl Drop for WriterActor {
    fn drop(&mut self) {
        // The normal driver calls settle so failures remain reportable. This
        // unwind fallback still joins, instead of detaching live SQL work.
        // Disconnect before cancellation/join: an idle actor must not retain
        // this thread, and an in-flight SQL must never start reentry on Drop.
        drop(self.reentry_release.take());
        if let Some(actor) = self.actor.take() {
            let disposition = self.sql_outcome.lock().map(|saved| *saved);
            let sql_running = disposition
                .as_ref()
                .map(|saved| saved.is_none())
                .unwrap_or(true);
            let nonterminal_client_failure =
                matches!(disposition, Ok(Some(SqlOutcome::TransportError)));
            if (sql_running && !actor.is_finished()) || nonterminal_client_failure {
                let _ = self
                    .cancel
                    .delete(self.deadline)
                    .map_err(anyhow::Error::from);
            }
            let _ = join(actor);
        }
    }
}
fn join(actor: JoinHandle<Result<()>>) -> Result<()> {
    actor
        .join()
        .map_err(|payload| ActorPanic(Mutex::new(payload)))?
}
pub(super) fn combine<T>(primary: Result<T>, cleanup: Result<()>) -> Result<T> {
    match (primary, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(primary), Err(cleanup)) => Err(ActorCleanupFailure { primary, cleanup }.into()),
    }
}
pub(super) fn retain_write_result(
    primary: Result<std::result::Result<StatementResult, StatementFailure>>,
    cleanup: Result<()>,
) -> Result<std::result::Result<StatementResult, StatementFailure>> {
    match (primary, cleanup) {
        (Ok(Err(actual)), Err(cleanup)) => combine(Err(actual.into()), Err(cleanup)),
        (primary, cleanup) => combine(primary, cleanup),
    }
}
struct ActorCleanupFailure {
    primary: anyhow::Error,
    cleanup: anyhow::Error,
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
            "original writer operation failed: {:#}; actual actor cleanup failed: {:#}",
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
        f.write_str("original writer actor panicked; source retained")
    }
}
impl std::error::Error for ActorPanic {}
