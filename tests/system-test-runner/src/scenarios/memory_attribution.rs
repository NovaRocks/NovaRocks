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

//! Functional native observation and cancellation on independent processes.
use crate::actors::sql::{self, SqlSession, StatementFailure, StatementResult};
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use crate::scenarios::native_sql_controls::{SqlIntentOwner, StatementSpec};
use crate::scenarios::{query_lifecycle, task_evidence};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::{QueryExecutionResourceSnapshot, TestClusterOwner};
use novarocks_test_trino_client::CancelHandle;
use std::{
    collections::BTreeSet,
    fs,
    path::Path,
    sync::Mutex,
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
const OBSERVATION_QUERIES: [&str; 2] = [
    "SELECT v FROM (SELECT 1 AS v UNION ALL SELECT 2) t ORDER BY v",
    "SELECT SUM(i) FROM TABLE(generate_series(1, 1000)) AS t(i)",
];
const CANCELLATION_QUERY: &str =
    "SELECT v FROM (SELECT sleep(10) AS v UNION ALL SELECT sleep(10)) t ORDER BY v";

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![Box::new(ObservationFamilies::default())]
}
#[derive(Default)]
struct ObservationFamilies {
    intent: SqlIntentOwner,
}
impl Scenario for ObservationFamilies {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "memory_attribution",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }
    fn name(&self) -> &'static str {
        "memory-attribution/observation-families"
    }
    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        let statements = OBSERVATION_QUERIES
            .iter()
            .map(|sql| StatementSpec {
                sql: (*sql).into(),
                faults: vec![],
                observations: (0..cluster_size)
                    .map(|index| (index, novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol.as_str(), None))
                    .collect(),
            })
            .collect();
        Ok(Some(self.intent.freeze(
            statements,
            &[],
            cluster_size,
            binary,
            deadline,
        )?))
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "native observation requires 1FE+3BE"
        );
        let ids = context.process_ids();
        ensure!(
            ids.backends.iter().copied().collect::<BTreeSet<_>>().len() == 3,
            "backend observation must come from three independent processes"
        );
        let baseline = query_lifecycle::resource_snapshot(context)?;
        let mut session =
            SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
        for (ordinal, (sql, expected)) in [
            (OBSERVATION_QUERIES[0], vec![1, 2]),
            (OBSERVATION_QUERIES[1], vec![500500]),
        ]
        .into_iter()
        .enumerate()
        {
            let selected = self.intent.take(ordinal)?;
            let pending = session.submit(sql)?;
            let cancel = pending.cancel_handle();
            let (pending, observation) = selected.arm_pending(context, pending, sql)?;
            let result = session.run_pending(pending);
            let nonterminal_client_failure =
                matches!(&result, Err(error) if !matches!(error, StatementFailure::Trino(_)));
            let primary = (|| -> Result<()> {
                let rows =
                    sql::integer_column(&result.context("execute distributed observation query")?)?;
                ensure!(
                    rows == expected,
                    "observation workload returned unexpected rows: {rows:?}"
                );
                let deadline = context.deadline();
                let terminal = context
                    .handle()
                    .await_query_lifecycle_statement_observation(&observation, true, deadline)?;
                task_evidence::assert_query_completed_across_boundary(
                    context,
                    &terminal,
                    "native observation workload",
                )?;
                Ok(())
            })();
            let mut cleanup = if nonterminal_client_failure {
                cancel
                    .delete(context.deadline())
                    .map_err(anyhow::Error::from)
            } else {
                Ok(())
            };
            let deadline = context.deadline();
            let binding = context
                .handle()
                .drain_query_lifecycle_statement_binding(&observation, deadline);
            cleanup = combine(cleanup, binding);
            combine(primary, cleanup)?;
        }
        query_lifecycle::await_resource_convergence(context, &baseline)?;
        let mut actor = CancellationQuery::start(session, context.deadline())?;
        let activity = (|| -> Result<QueryExecutionResourceSnapshot> {
            loop {
                let snapshot = query_lifecycle::resource_snapshot(context)?;
                if snapshot != baseline && !actor.is_finished() {
                    return Ok(snapshot);
                }
                ensure!(
                    !actor.is_finished(),
                    "cancellation workload finished before active Native resources were observed"
                );
                thread::sleep(
                    context
                        .remaining("observe active cancellation workload")?
                        .min(Duration::from_millis(50)),
                );
            }
        })();
        // DELETE belongs to this original continuation, including when the
        // reader has already exited with a nonterminal transport failure.
        let early = actor.is_finished().then(|| actor.join());
        let actual_terminal = matches!(&early, Some(Ok(Ok(_) | Err(StatementFailure::Trino(_)))));
        let cancel_requested = !actual_terminal;
        let cancelled = if cancel_requested {
            actor
                .cancel
                .delete(context.deadline())
                .map_err(anyhow::Error::from)
        } else {
            Ok(())
        };
        let shutdown = if cancelled.is_err() {
            context.shutdown()
        } else {
            Ok(())
        };
        let joined = match early {
            Some(actual) => actual,
            None => actor.join(),
        };
        let query = match joined {
            Ok(Err(actual)) if activity.is_ok() && cancel_requested => {
                let expected = sql::expect_error(&actual, 3, "USER_CANCELED").and_then(|wire| {
                    ensure!(
                        wire.error_type == "USER_ERROR",
                        "cancellation returned an unexpected error type: {}",
                        wire.error_type
                    );
                    Ok(())
                });
                match expected {
                    Ok(()) => Ok(()),
                    Err(mismatch) => Err(anyhow::Error::from(actual).context(mismatch)),
                }
            }
            Ok(Err(actual)) => Err(actual.into()),
            Ok(Ok(actual)) => Err(anyhow::anyhow!(
                "active cancellation workload unexpectedly succeeded: {actual:?}"
            )),
            Err(actual) => Err(actual),
        };
        let cleanup = combine(combine(cancelled, shutdown), query);
        let result = combine(activity, cleanup);
        // Resource convergence remains an independent original Native oracle.
        let convergence = query_lifecycle::await_resource_convergence(context, &baseline);
        let during = combine(result, convergence)?;
        let after = query_lifecycle::resource_snapshot(context)?;
        fs::write(
            context.scenario_root().join("memory-attribution.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "schema_version": 3,
                "backend_process_ids": ids.backends,
                "functional_native_observation": true,
                "active_cancellation_observed": true,
                "reader_joined": true,
                "during_query": resource_record(&during),
                "after_cancel": resource_record(&after),
            }))?,
        )?;
        context.action("verified distributed rows, original Native boundary evidence and active Trino cancellation with actual reader exit and resource convergence on independent backends");
        Ok(())
    }
}

fn resource_record(snapshot: &QueryExecutionResourceSnapshot) -> serde_json::Value {
    serde_json::json!({
        "fe_running": snapshot.fe_running,
        "backends": snapshot.backends.iter().map(|backend| serde_json::json!({
            "index": backend.index,
            "process_running": backend.process_running,
            "resources": backend.resources,
        })).collect::<Vec<_>>()
    })
}

struct CancellationQuery {
    cancel: CancelHandle,
    actor: Option<JoinHandle<std::result::Result<StatementResult, StatementFailure>>>,
    deadline: Instant,
}
impl CancellationQuery {
    fn start(mut session: SqlSession, deadline: Instant) -> Result<Self> {
        let pending = session.submit(CANCELLATION_QUERY)?;
        let cancel = pending.cancel_handle();
        let actor = match thread::Builder::new()
            .name("native-observation-cancel".into())
            .spawn(move || session.run_pending(pending))
        {
            Ok(actor) => actor,
            Err(primary) => {
                return combine(
                    Err(primary.into()),
                    cancel.delete(deadline).map_err(anyhow::Error::from),
                );
            }
        };
        Ok(Self {
            cancel,
            actor: Some(actor),
            deadline,
        })
    }
    fn is_finished(&self) -> bool {
        self.actor.as_ref().is_none_or(JoinHandle::is_finished)
    }
    fn join(&mut self) -> Result<std::result::Result<StatementResult, StatementFailure>> {
        let actor = self
            .actor
            .take()
            .context("original cancellation reader was already joined")?;
        actor
            .join()
            .map_err(|payload| ActorPanic(Mutex::new(payload)).into())
    }
}
impl Drop for CancellationQuery {
    fn drop(&mut self) {
        if let Some(actor) = self.actor.take() {
            let _ = self.cancel.delete(self.deadline);
            let _ = actor.join();
        }
    }
}
fn combine<T>(primary: Result<T>, cleanup: Result<()>) -> Result<T> {
    match (primary, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(primary), Err(cleanup)) => Err(CleanupFailure { primary, cleanup }.into()),
    }
}
struct CleanupFailure {
    primary: anyhow::Error,
    cleanup: anyhow::Error,
}
impl std::fmt::Display for CleanupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "native observation failed: {:#}; actual cleanup also failed: {:#}",
            self.primary, self.cleanup
        )
    }
}
impl std::fmt::Debug for CleanupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::error::Error for CleanupFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.primary.as_ref())
    }
}
struct ActorPanic(Mutex<Box<dyn std::any::Any + Send>>);
impl std::fmt::Display for ActorPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _ = &self.0;
        f.write_str("original cancellation reader panicked; its actual payload is retained")
    }
}
impl std::fmt::Debug for ActorPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::error::Error for ActorPanic {}
