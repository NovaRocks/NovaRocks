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
use crate::{actors::mysql as mysql_actor, scenario::ScenarioContext};
use anyhow::{Context, Result};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::{QueryLifecycleStatementObservation, TestClusterOwner};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Instant;

pub(super) struct WriterActor {
    pub connection_id: u32,
    pub done: mpsc::Receiver<mysql::Result<()>>,
    actor: Option<JoinHandle<Result<()>>>,
    user: String,
    port: u16,
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
    pub fn apply(self, connection: &mut mysql::Conn) -> Result<()> {
        anyhow::ensure!(self.pipeline_dop > 0, "read control DOP must be positive");
        // Only this original session property is supported. Memory is frozen
        // by each statement's admitted hint; batch size remains its default.
        // The scenario checks the actual build participant count separately.
        connection.query_drop(format!("SET pipeline_dop={}", self.pipeline_dop))?;
        Ok(())
    }
}

pub(super) fn require_public_error_reentry(connection: &mut mysql::Conn) -> Result<()> {
    let rows: Vec<i64> = connection
        .query("SELECT 1")
        .context("reenter the original writer connection after its public SQL error")?;
    anyhow::ensure!(
        rows == vec![1],
        "writer connection reentry returned {rows:?}"
    );
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
        rows: Arc<Mutex<Option<Vec<mysql::Row>>>>,
        settings: Option<ReadSessionSettings>,
    ) -> Result<Self> {
        Self::start_internal(context, sql, selected, None, Some(rows), settings)
    }
    fn start_internal(
        context: &mut ScenarioContext,
        sql: &str,
        selected: SelectedStatement,
        query_timeout_secs: Option<u64>,
        rows: Option<Arc<Mutex<Option<Vec<mysql::Row>>>>>,
        read_settings: Option<ReadSessionSettings>,
    ) -> Result<Self> {
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let (submit_tx, submit_rx) = mpsc::sync_channel(1);
        let (done_tx, done) = mpsc::sync_channel(1);
        let (reentry_release, reentry_wait) = mpsc::sync_channel(1);
        let user = context.mysql_user().to_owned();
        let port = context.mysql_port();
        context.remaining("connect original writer actor")?;
        let deadline = context.deadline();
        let actor_user = user.clone();
        let actor_sql = sql.to_owned();
        let sql_outcome = Arc::new(Mutex::new(None));
        let actor_outcome = Arc::clone(&sql_outcome);
        let actor = thread::Builder::new()
            .name("selected-distributed-writer-sql".into())
            .spawn(move || -> Result<()> {
                let connection =
                    mysql_actor::connect_for_cancellation_until(&actor_user, port, deadline)?;
                connection.run(|connection| {
                    if let Some(settings) = read_settings {
                        settings.apply(connection)?;
                    }
                    if let Some(seconds) = query_timeout_secs {
                        connection
                            .query_drop(format!("SET query_timeout = {seconds}"))
                            .context("set the deadline write session query_timeout")?;
                    }
                    // The real connection is idle after its original SET prelude.
                    ready_tx.send(connection.connection_id())?;
                    match submit_rx.recv_timeout(deadline.saturating_duration_since(Instant::now()))
                    {
                        Ok(()) => {}
                        Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
                        Err(mpsc::RecvTimeoutError::Timeout) => {
                            anyhow::bail!("original SQL submission gate deadline expired")
                        }
                    }
                    let outcome = match rows {
                        None => connection.query_drop(actor_sql),
                        Some(rows) => connection.query::<mysql::Row, _>(actor_sql).map(|actual| {
                            *rows.lock().expect("original read result owner") = Some(actual);
                        }),
                    };
                    let classification = match &outcome {
                        Ok(()) => SqlOutcome::Success,
                        Err(mysql::Error::MySqlError(_)) => SqlOutcome::PublicError,
                        Err(_) => SqlOutcome::TransportError,
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
                    // Preserve this exact idle connection while the scenario
                    // observes only the original writer's counter window.
                    // A disconnected gate is failure cleanup, never reentry.
                    let reentry = match reentry_wait
                        .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                    {
                        Ok(()) if matches!(classification, SqlOutcome::PublicError) => {
                            require_public_error_reentry(connection)
                        }
                        Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => Ok(()),
                        Err(mpsc::RecvTimeoutError::Timeout) => Err(anyhow::anyhow!(
                            "original writer reentry gate deadline expired"
                        )),
                    };
                    combine(combine(delivery, reentry), recorded)
                })
            })?;
        let ready = (|| -> Result<(u32, QueryLifecycleStatementObservation)> {
            let cid = ready_rx
                .recv_timeout(context.remaining("receive idle writer connection")?)
                .context("original writer actor did not publish its actual connection")?;
            let observation = selected.arm(context, cid, sql)?;
            Ok((cid, observation))
        })();
        let (cid, observation) = match ready {
            Ok(ready) => ready,
            Err(primary) => {
                drop(submit_tx);
                drop(reentry_release);
                return combine::<Self>(Err(primary), join(actor));
            }
        };
        if let Err(primary) = submit_tx.send(()) {
            drop(reentry_release);
            let cleanup = combine(
                join(actor),
                context
                    .handle()
                    .drain_query_lifecycle_statement_observation(&observation),
            );
            return combine::<Self>(Err(primary.into()), cleanup);
        }
        Ok(Self {
            connection_id: cid,
            done,
            actor: Some(actor),
            user,
            port,
            deadline,
            observation,
            sql_outcome,
            reentry_release: Some(reentry_release),
        })
    }

    /// Every known success/failure path settles the same original actor. Public
    /// KILL and cluster shutdown are failure-only cleanup, never new admission.
    pub fn settle(
        mut self,
        context: &mut ScenarioContext,
        primary: Result<mysql::Result<()>>,
    ) -> Result<mysql::Result<()>> {
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
            cleanup = cancel(&self.user, self.port, self.connection_id, self.deadline);
            if cleanup.is_err() {
                cleanup = combine(cleanup, context.shutdown());
            }
        }
        // KILL above cannot strike SELECT 1: reentry is still gated until the
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
                        .drain_query_lifecycle_statement_observation(&self.observation),
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
            Ok(Err(error)) if !matches!(&error, mysql::Error::MySqlError(_)) => Err(error.into()),
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
            let sql_running = self
                .sql_outcome
                .lock()
                .map(|saved| saved.is_none())
                .unwrap_or(true);
            if sql_running && !actor.is_finished() {
                let _ = cancel(&self.user, self.port, self.connection_id, self.deadline);
            }
            let _ = join(actor);
        }
    }
}
fn cancel(user: &str, port: u16, cid: u32, deadline: Instant) -> Result<()> {
    mysql_actor::connect_until(user, port, deadline)?.run(|control| {
        control.query_drop(format!("KILL QUERY {cid}"))?;
        Ok(())
    })
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
    primary: Result<mysql::Result<()>>,
    cleanup: Result<()>,
) -> Result<mysql::Result<()>> {
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
