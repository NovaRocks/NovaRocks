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

//! Finite shared typed-SQL actors with explicit cancellation and actual joins.

use super::connector::{await_resource_convergence, resource_baseline};
use super::paimon::scan_statement_actor::{combine, join};
use crate::actors::sql::{SqlSession, StatementFailure, StatementResult};
use crate::scenario::ScenarioContext;
use anyhow::{Context, Result, ensure};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

// These are physical test cleanup guards, never a fresh product SQL deadline.
const IO_GUARD: Duration = Duration::from_secs(60);
const CONTROL_GUARD: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub(super) struct OriginalProviderQuery {
    cancel: novarocks_test_trino_client::CancelHandle,
    deadline: Instant,
}
impl OriginalProviderQuery {
    pub(super) fn cancel(&self) -> Result<()> {
        self.cancel
            .delete(self.deadline)
            .map_err(anyhow::Error::from)
    }
}
#[derive(Default)]
struct StatementState {
    requested: bool,
    active: Option<OriginalProviderQuery>,
}
/// One actor's actual active statement only. It is not a SQL session facade,
/// query registry or admission authority; each call borrows the original session.
#[derive(Clone)]
pub(super) struct ActorStatements {
    state: Arc<Mutex<StatementState>>,
    deadline: Instant,
}
impl ActorStatements {
    pub(super) fn run(
        &self,
        session: &mut SqlSession,
        sql: impl AsRef<str>,
    ) -> Result<std::result::Result<StatementResult, StatementFailure>> {
        let pending = session.submit(sql)?;
        let original = OriginalProviderQuery {
            cancel: pending.cancel_handle(),
            deadline: self.deadline,
        };
        let registered = (|| -> Result<bool> {
            let mut state = self
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("original provider statement owner poisoned"))?;
            ensure!(
                state.active.is_none(),
                "one actual current statement per provider actor"
            );
            state.active = Some(original.clone());
            Ok(state.requested)
        })();
        let requested = match registered {
            Ok(requested) => requested,
            Err(primary) => return combine(Err(primary), original.cancel()),
        };
        if requested {
            original.cancel()?;
        }
        let result = session.run_pending(pending);
        let terminal = matches!(&result, Ok(_) | Err(StatementFailure::Trino(_)));
        let cancelled = if terminal { Ok(()) } else { original.cancel() };
        let retired = if terminal || cancelled.is_ok() {
            self.state
                .lock()
                .map_err(|_| anyhow::anyhow!("original provider statement owner poisoned"))
                .map(|mut state| {
                    state.active.take();
                })
        } else {
            Ok(())
        };
        let cleanup = combine(cancelled, retired);
        match (result, cleanup) {
            (result, Ok(())) => Ok(result),
            (Ok(_), Err(cleanup)) => Err(cleanup),
            (Err(primary), Err(cleanup)) => combine(Err(anyhow::Error::new(primary)), Err(cleanup)),
        }
    }
    fn cancel_active(&self) -> Result<()> {
        let original = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("original provider cancellation owner poisoned"))?;
            state.requested = true;
            state.active.clone()
        };
        if let Some(original) = original {
            original.cancel()?;
        }
        Ok(())
    }
    fn active(&self) -> Result<OriginalProviderQuery> {
        self.state
            .lock()
            .map_err(|_| anyhow::anyhow!("original provider current statement owner poisoned"))?
            .active
            .clone()
            .context("the provider actor has no actual active original statement")
    }
}

pub(super) struct ProviderActor<T> {
    ready: mpsc::Receiver<()>,
    done: mpsc::Receiver<Result<T>>,
    started: bool,
    thread: Option<JoinHandle<Result<()>>>,
    label: String,
    statements: ActorStatements,
}
impl<T: Send + 'static> ProviderActor<T> {
    pub(super) fn spawn(
        context: &ScenarioContext,
        label: &str,
        operation: impl FnOnce(&mut SqlSession, &ActorStatements) -> Result<T> + Send + 'static,
    ) -> Result<Self> {
        let user = context.sql_user().to_owned();
        let port = context.trino_port();
        context.remaining("connect finite provider actor")?;
        let deadline = context.deadline();
        let statements = ActorStatements {
            state: Arc::new(Mutex::new(StatementState::default())),
            deadline,
        };
        let actor_statements = statements.clone();
        let (ready_tx, ready) = mpsc::channel();
        let (done_tx, done) = mpsc::channel();
        let thread =
            thread::Builder::new()
                .name(label.to_owned())
                .spawn(move || -> Result<()> {
                    let result = (|| {
                        let mut session = SqlSession::new(&user, port, deadline)?;
                        // This is actual actor setup, never a fabricated query identity
                        // or a claim that a statement has entered the FE.
                        ready_tx.send(()).context("publish provider actor setup")?;
                        operation(&mut session, &actor_statements)
                    })();
                    match done_tx.send(result) {
                        Ok(()) => Ok(()),
                        Err(mpsc::SendError(Err(primary))) => {
                            Err(primary.context("original provider result owner closed"))
                        }
                        Err(mpsc::SendError(Ok(_))) => {
                            anyhow::bail!("original provider result owner closed")
                        }
                    }
                })?;
        Ok(Self {
            ready,
            done,
            started: false,
            thread: Some(thread),
            label: label.to_owned(),
            statements,
        })
    }
    pub(super) fn wait_started(&mut self, timeout: Duration) -> Result<()> {
        if self.started {
            return Ok(());
        }
        match self.ready.recv_timeout(timeout) {
            Ok(()) => {
                self.started = true;
                Ok(())
            }
            // Setup waiting does not own T's shape. Leave the original done
            // value intact for its business consumer or finish_with validator.
            Err(error) => {
                Err(error).with_context(|| format!("{} did not report actor setup", self.label))
            }
        }
    }
    pub(super) fn current_query(&self) -> Result<OriginalProviderQuery> {
        self.statements.active()
    }
    pub(super) fn receive(&mut self, timeout: Duration) -> Result<T> {
        self.done
            .recv_timeout(timeout)
            .with_context(|| format!("{} did not complete", self.label))?
    }
    pub(super) fn try_result(&mut self) -> Result<Option<T>> {
        match self.done.try_recv() {
            Ok(result) => result.map(Some),
            Err(mpsc::TryRecvError::Empty) => Ok(None),
            Err(mpsc::TryRecvError::Disconnected) => {
                anyhow::bail!("{} exited without a result", self.label)
            }
        }
    }
    pub(super) fn cancel(&mut self, _context: &ScenarioContext) -> Result<()> {
        self.statements.cancel_active()
    }
    pub(super) fn finish_with(
        &mut self,
        context: &mut ScenarioContext,
        consume_unobserved: impl FnOnce(T) -> Result<()>,
    ) -> Result<()> {
        let Some(actor) = self.thread.as_ref() else {
            return Ok(());
        };
        let expires = Instant::now() + IO_GUARD + CONTROL_GUARD;
        while !actor.is_finished() && Instant::now() < expires {
            thread::sleep(Duration::from_millis(10));
        }
        let forced_shutdown = !actor.is_finished();
        let mut cleanup = if forced_shutdown {
            self.statements.cancel_active()
        } else {
            Ok(())
        };
        if forced_shutdown {
            let saved = serde_json::to_vec_pretty(
                &serde_json::json!({"actor":self.label,"diagnostics":context.diagnostics(),"failed":true}),
            ).map_err(anyhow::Error::from).and_then(|bytes| {
                std::fs::write(context.scenario_root().join(format!("{}-forced-shutdown.json", self.label)), bytes)
                    .map_err(anyhow::Error::from)
            });
            cleanup = combine(cleanup, saved);
            cleanup = combine(cleanup, context.shutdown());
            cleanup = combine(
                Err(anyhow::anyhow!(
                    "{} did not physically exit within the test I/O guard",
                    self.label
                )),
                cleanup,
            );
        }
        let actual_join = join(
            self.thread
                .take()
                .expect("actor retained until actual join"),
        );
        // Joining the original worker does not consume its delivered callback.
        // Recover only an unconsumed original once; the business owner decides
        // the closed shape of T, including a nested StatementFailure.
        let unobserved = consume_unobserved_result(&self.done, consume_unobserved);
        // A joined callback with a failed client reader still owns the original
        // active capability. Actual terminal SQL cleared it in run().
        let cancelled = self.statements.cancel_active();
        combine(
            combine(cleanup, actual_join),
            combine(unobserved, cancelled),
        )
    }
}
impl<T> Drop for ProviderActor<T> {
    fn drop(&mut self) {
        if let Some(actor) = self.thread.take() {
            let _ = self.statements.cancel_active();
            let _ = join(actor);
            let _ = self.statements.cancel_active();
        }
    }
}

pub(super) type ProviderCheck =
    Box<dyn FnOnce(&mut SqlSession, &ActorStatements) -> Result<()> + Send>;
struct StartGate(Arc<(Mutex<Option<bool>>, Condvar)>);
impl StartGate {
    fn release(&self, start: bool) {
        let (state, ready) = &*self.0;
        *state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(start);
        ready.notify_all();
    }
}
impl Drop for StartGate {
    fn drop(&mut self) {
        self.release(false);
    }
}
/// Preserve the original simultaneous start, finite inputs, peer cancellation,
/// actual joins and independent Native resource-exit check.
pub(super) fn run_checks(
    context: &mut ScenarioContext,
    label: &str,
    checks: Vec<ProviderCheck>,
) -> Result<()> {
    let baseline = resource_baseline(context)?;
    let mut actors = Vec::new();
    // On unwind release the original start gate before actor Drop joins peers.
    let gate = StartGate(Arc::new((Mutex::new(None::<bool>), Condvar::new())));
    let mut errors: Vec<anyhow::Error> = Vec::new();
    for (index, check) in checks.into_iter().enumerate() {
        let actor_gate = gate.0.clone();
        let deadline = context.deadline();
        match ProviderActor::spawn(
            context,
            &format!("{label}-{index}"),
            move |session, statements| {
                let (state, ready) = &*actor_gate;
                let mut state = state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                while state.is_none() {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    ensure!(
                        !remaining.is_zero(),
                        "original provider start gate deadline expired"
                    );
                    state = ready
                        .wait_timeout(state, remaining)
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .0;
                }
                ensure!(
                    *state == Some(true),
                    "provider actor start aborted after peer setup failed"
                );
                drop(state);
                check(session, statements)
            },
        ) {
            Ok(actor) => actors.push(actor),
            Err(primary) => {
                errors.push(primary.context(format!("spawn actor {index}")));
                break;
            }
        }
    }
    for actor in &mut actors {
        if let Err(primary) = actor.wait_started(CONTROL_GUARD) {
            errors.push(primary.context(format!("prepare {}", actor.label)));
        }
    }
    gate.release(errors.is_empty());
    let mut completed = vec![false; actors.len()];
    let execution = (|| -> Result<()> {
        ensure!(errors.is_empty(), "provider actor setup failed");
        while completed.iter().any(|done| !done) {
            for (index, actor) in actors.iter_mut().enumerate() {
                if !completed[index] {
                    match actor.try_result() {
                        Ok(Some(())) => completed[index] = true,
                        Ok(None) => {}
                        Err(primary) => {
                            completed[index] = true;
                            return Err(primary);
                        }
                    }
                }
            }
            context.remaining("await finite provider actors")?;
            thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    })();
    let failed = execution.is_err();
    if let Err(primary) = execution {
        errors.push(primary.context("execution"));
    }
    if failed {
        for actor in &mut actors {
            if let Err(primary) = actor.cancel(context) {
                errors.push(primary.context("cancellation"));
            }
        }
    }
    for (index, actor) in actors.iter_mut().enumerate() {
        if let Err(primary) = actor.finish_with(context, |()| {
            // This actual successful callback already checked its independent
            // finite business oracle, even if the parent stopped waiting.
            completed[index] = true;
            Ok(())
        }) {
            errors.push(primary.context("actual join and original result"));
        }
    }
    if let Err(primary) = await_resource_convergence(context, &baseline, label) {
        errors.push(primary.context("native resource exit"));
    }
    let rendered = errors
        .iter()
        .map(|error| format!("{error:#}"))
        .collect::<Vec<_>>();
    let saved = (|| -> Result<()> {
        std::fs::write(
            context.scenario_root().join(format!("{label}-actors.json")),
            serde_json::to_vec_pretty(&serde_json::json!({
                "actors":actors.len(), "errors":rendered,
                "owned_error_facts":errors.iter().map(provider_failure_receipt).collect::<Vec<_>>(),
                "actual_joins_completed":true,
            }))?,
        )?;
        Ok(())
    })();
    if let Err(primary) = saved {
        errors.push(primary.context("persist original provider actor evidence"));
    }
    let mut result = Ok(());
    for error in errors {
        result = combine(result, Err(error));
    }
    result
}

pub(super) fn record_cancelled_proxy_read(
    context: &ScenarioContext,
    label: &str,
    proxy: &novarocks_cluster_harness::delayed_s3::DelayedS3Proxy,
    hold: &novarocks_cluster_harness::delayed_s3::DelayedS3ReadHold,
) -> Result<()> {
    let observation = hold.read_exit_observation();
    let identity = observation.as_ref().ok();
    let overflow = proxy.snapshot().event_overflow;
    let events = (overflow == 0).then(|| {
        proxy
            .event_log()
            .into_iter()
            .filter(|event| {
                identity.is_some_and(|identity| event.request_id == identity.request_id)
            })
            .map(|event| {
                serde_json::json!({
                    "kind":format!("{:?}",event.kind),"request_id":event.request_id,
                    "connection_id":event.connection_id,"elapsed_millis":event.elapsed_millis,
                    "method":event.method.as_str(),"protocol":event.protocol,
                    "object_id":event.object_id,"range":event.range,"bytes":event.bytes,
                })
            })
            .collect::<Vec<_>>()
    });
    let connections = (overflow == 0).then(|| {
        proxy
            .connection_log()
            .into_iter()
            .filter(|event| {
                identity.is_some_and(|identity| event.connection_id == identity.connection_id)
            })
            .map(|event| {
                serde_json::json!({
                    "kind":format!("{:?}",event.kind),"connection_id":event.connection_id,
                    "elapsed_millis":event.elapsed_millis,
                })
            })
            .collect::<Vec<_>>()
    });
    std::fs::write(
        context
            .scenario_root()
            .join(format!("{label}-proxy-exit.json")),
        serde_json::to_vec_pretty(&serde_json::json!({
            "observation":identity,"observation_error":observation.as_ref().err().map(ToString::to_string),
            "events":events,"connections":connections,"event_overflow":overflow,
        }))?,
    )?;
    ensure!(
        overflow == 0,
        "cancelled proxy raw evidence overflowed: {overflow}"
    );
    Ok(())
}

/// These finite fixture inputs are nonnegative signed SQL integers. No carrier
/// default, lossy cast or tuple conversion participates in the independent oracle.
pub(super) fn unsigned_rows(
    result: StatementResult,
) -> std::result::Result<Vec<u64>, StatementFailure> {
    crate::actors::sql::integer_column(&result)
        .and_then(|rows| {
            rows.into_iter()
                .map(|value| u64::try_from(value).map_err(anyhow::Error::from))
                .collect()
        })
        .map_err(|error| StatementFailure::Protocol(error.to_string()))
}
pub(super) fn unsigned_first(
    result: StatementResult,
) -> std::result::Result<Option<u64>, StatementFailure> {
    if !result.is_result_set()
        || result.columns.len() != 1
        || result.rows.len() != 1
        || result.columns[0].type_signature.raw_type != "bigint"
        || !result.columns[0].type_signature.arguments.is_empty()
    {
        return Err(StatementFailure::Protocol(format!(
            "expected one non-null bigint count cell, got {result:?}"
        )));
    }
    unsigned_rows(result).and_then(|rows| {
        rows.into_iter()
            .next()
            .map(Some)
            .ok_or_else(|| StatementFailure::Protocol("count row missing".into()))
    })
}
pub(super) fn unsigned_pairs(
    result: StatementResult,
) -> std::result::Result<Vec<(u64, u64)>, StatementFailure> {
    use crate::actors::sql::TrinoValue;
    if !result.is_result_set()
        || result.columns.len() != 2
        || result.columns.iter().any(|column| {
            !matches!(
                column.type_signature.raw_type.as_str(),
                "tinyint" | "smallint" | "integer" | "bigint"
            ) || !column.type_signature.arguments.is_empty()
        })
    {
        return Err(StatementFailure::Protocol(format!(
            "expected two integer fixture columns, got {result:?}"
        )));
    }
    result
        .rows
        .iter()
        .map(|row| match row.as_slice() {
            [TrinoValue::Integer(id), TrinoValue::Integer(value)] => Ok((
                u64::try_from(*id)
                    .map_err(|error| StatementFailure::Protocol(error.to_string()))?,
                u64::try_from(*value)
                    .map_err(|error| StatementFailure::Protocol(error.to_string()))?,
            )),
            row => Err(StatementFailure::Protocol(format!(
                "expected two non-null integer fixture cells, got {row:?}"
            ))),
        })
        .collect()
}

/// Preserve the original operation's causal priority. A statement recovered
/// only after cancellation/Join can be derived; keep its actual object as a
/// secondary failure rather than guessing the first cause from its type.
pub(super) fn finish_provider_operation<T>(
    operation: Result<T>,
    cleanup: Vec<anyhow::Error>,
) -> Result<T> {
    cleanup
        .into_iter()
        .fold(operation, |primary, error| combine(primary, Err(error)))
}

fn consume_unobserved_result<T>(
    done: &mpsc::Receiver<Result<T>>,
    consume: impl FnOnce(T) -> Result<()>,
) -> Result<()> {
    match done.try_recv() {
        Ok(Ok(value)) => consume(value),
        Ok(Err(primary)) => Err(primary),
        Err(mpsc::TryRecvError::Empty | mpsc::TryRecvError::Disconnected) => Ok(()),
    }
}
/// Structured projection borrows both actual error objects. The standard
/// Error::source chain follows primary only; that does not erase secondary.
pub(super) fn provider_failure_receipt(error: &anyhow::Error) -> serde_json::Value {
    use super::paimon::scan_statement_actor::ActorCleanupFailure;
    if let Some(pair) = error.downcast_ref::<ActorCleanupFailure>() {
        return serde_json::json!({
            "message":format!("{error:#}"),
            "primary":provider_failure_receipt(pair.original_primary()),
            "cleanup":provider_failure_receipt(pair.actual_cleanup()),
        });
    }
    let statement_kind = error.chain().find_map(|source| {
        source
            .downcast_ref::<StatementFailure>()
            .map(|failure| match failure {
                StatementFailure::Trino(_) => "Trino",
                StatementFailure::Http { .. } => "Http",
                StatementFailure::Transport(_) => "Transport",
                StatementFailure::Deadline => "Deadline",
                StatementFailure::UnsupportedSessionHeader(_) => "UnsupportedSessionHeader",
                StatementFailure::Protocol(_) => "Protocol",
            })
    });
    serde_json::json!({"message":format!("{error:#}"),"statement_failure_kind":statement_kind})
}
#[cfg(test)]
mod retained_result_tests {
    use super::super::paimon::scan_statement_actor::ActorCleanupFailure;
    use super::*;

    #[test]
    fn joined_unobserved_outer_and_nested_reader_errors_keep_original_objects() {
        for nested in [false, true] {
            let (sender, done) =
                mpsc::channel::<Result<std::result::Result<(), StatementFailure>>>();
            let actor = std::thread::spawn(move || {
                let original = StatementFailure::Transport(
                    "original SQL reader failed before SDK entry".into(),
                );
                let result = if nested {
                    Ok(Err(original))
                } else {
                    Err(original.into())
                };
                assert!(sender.send(result).is_ok());
            });
            actor.join().unwrap();
            let actual =
                consume_unobserved_result(&done, |result| result.map_err(anyhow::Error::from));
            let error = finish_provider_operation::<()>(
                Err(anyhow::anyhow!(
                    "SDK wait remained the first observed failure"
                )),
                vec![actual.unwrap_err()],
            )
            .unwrap_err();
            let pair = error.downcast_ref::<ActorCleanupFailure>().unwrap();
            assert_eq!(
                pair.original_primary().to_string(),
                "SDK wait remained the first observed failure"
            );
            assert!(pair.actual_cleanup().chain().any(|source| matches!(source.downcast_ref::<StatementFailure>(), Some(StatementFailure::Transport(message)) if message == "original SQL reader failed before SDK entry")));
            let receipt = provider_failure_receipt(&error);
            assert_eq!(receipt["cleanup"]["statement_failure_kind"], "Transport");
        }
    }
    #[test]
    fn business_consumed_result_is_not_consumed_or_reclassified_again() {
        let (sender, done) = mpsc::channel::<Result<()>>();
        let actor = std::thread::spawn(move || {
            assert!(sender.send(Ok(())).is_ok());
        });
        actor.join().unwrap();
        done.recv().unwrap().unwrap();
        consume_unobserved_result(&done, |()| anyhow::bail!("a consumed callback ran twice"))
            .unwrap();
    }
}
