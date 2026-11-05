// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this
// file to you under the Apache License, Version 2.0 (the
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

//! Exact SQL observations consumed only by the Native trust scenarios.

use crate::scenario::ScenarioContext;
use crate::scenarios::native_sql_controls::{SelectedStatement, StatementSpec};
use anyhow::{Context, Result};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::{QueryLifecycleStructuredSnapshot, TestClusterOwner};

pub(super) const BASELINE_SQL: &str =
    "SELECT v FROM (SELECT 1 AS v UNION ALL SELECT 2) t ORDER BY v";
pub(super) const PARITY_SQL: &str =
    "SELECT count(*) FROM vended_parity.vended_parity_db.vended_parity_data";

pub(super) fn grpc_statement(sql: &str) -> StatementSpec {
    StatementSpec {
        sql: sql.into(),
        faults: vec![],
        // These are possible placements, not an oracle claiming all three
        // participate. The original Task evidence checks the actual set.
        observations: (0..super::REQUIRED_BACKENDS)
            .map(|index| {
                (
                    index,
                    novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol
                        .as_str(),
                    None,
                )
            })
            .collect(),
    }
}

pub(super) fn baseline_statements() -> Vec<StatementSpec> {
    (0..3).map(|_| grpc_statement(BASELINE_SQL)).collect()
}

pub(super) fn observed_query(
    context: &mut ScenarioContext,
    selected: &SelectedStatement,
    connection: &mut mysql::Conn,
    sql: &str,
    subject: &str,
) -> Result<(Vec<i64>, QueryLifecycleStructuredSnapshot)> {
    // All ordinary connection setup has completed. No generation is guessed;
    // the FE journal binds the next actual statement on this existing client.
    let observation = selected.arm(context, connection.connection_id(), sql)?;
    let result = (|| {
        let rows: Vec<i64> = connection
            .query(sql)
            .with_context(|| format!("run {subject}"))?;
        let snapshot = context
            .handle()
            .await_query_lifecycle_statement_observation(&observation, true, selected.deadline)
            .with_context(|| format!("read exact lifecycle journal for {subject}"))?;
        Ok((rows, snapshot))
    })();
    // The SQL call is synchronous. On success the journal additionally proves
    // the query owner's actual exit before observation teardown.
    let drained = context
        .handle()
        .drain_query_lifecycle_statement_observation(&observation);
    with_cleanup(result, drained)
}

pub(super) fn with_cleanup<T>(primary: Result<T>, cleanup: Result<()>) -> Result<T> {
    match (primary, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(primary), Err(cleanup)) => {
            Err(anyhow::anyhow!("{primary:#}; cleanup failed: {cleanup:#}"))
        }
    }
}

pub(super) async fn join_probe_connection(
    connection_job: Option<tokio::task::JoinHandle<std::result::Result<(), h2::Error>>>,
) -> Result<()> {
    let Some(connection_job) = connection_job else {
        return Ok(());
    };
    connection_job.abort();
    match connection_job.await {
        Ok(Ok(())) => Ok(()),
        Err(error) if error.is_cancelled() => Ok(()),
        Ok(Err(error)) => {
            Err(anyhow::Error::from(error).context("Native probe HTTP/2 connection job failed"))
        }
        Err(error) => {
            Err(anyhow::Error::from(error).context("join Native probe HTTP/2 connection job"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scenarios::native_sql_controls::SqlIntentOwner;
    use std::time::{Duration, Instant};

    #[test]
    fn baseline_has_three_exact_finite_queries_without_faults() {
        let specs = baseline_statements();
        assert_eq!(specs.len(), 3);
        for spec in specs {
            assert_eq!(spec.sql, BASELINE_SQL);
            assert!(spec.faults.is_empty());
            assert_eq!(
                spec.observations,
                vec![
                    (
                        0,
                        novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol
                            .as_str(),
                        None
                    ),
                    (
                        1,
                        novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol
                            .as_str(),
                        None
                    ),
                    (
                        2,
                        novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol
                            .as_str(),
                        None
                    )
                ]
            );
        }
    }

    #[test]
    fn parity_grants_only_the_marker_owner_needed_by_its_original_oracle() {
        let spec = grpc_statement(PARITY_SQL);
        assert_eq!(spec.sql, PARITY_SQL);
        assert!(spec.faults.is_empty());
        assert!(spec.observations.iter().all(|(_, action, _)| *action
            == novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol.as_str()));
        assert_eq!(spec.observations.len(), super::super::REQUIRED_BACKENDS);
    }

    #[test]
    fn repeated_sql_has_distinct_prelaunch_tokens_and_preserves_original_deadline() {
        let directory = tempfile::tempdir().unwrap();
        let binary = directory.path().join("binary");
        std::fs::write(&binary, []).unwrap();
        let owner = SqlIntentOwner::default();
        let deadline = Instant::now() + Duration::from_secs(5);
        let plan = owner
            .freeze(baseline_statements(), &[], 3, &binary, deadline)
            .unwrap();
        assert_eq!(plan.deadline(), deadline);
        let statements = (0..3)
            .map(|index| owner.take(index).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            statements
                .iter()
                .map(|selected| &selected.token)
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            3
        );
        assert!(
            statements
                .iter()
                .all(|selected| selected.sql == BASELINE_SQL && selected.deadline == deadline)
        );
        assert!(owner.take(0).is_err());
        assert!(owner.take(3).is_err());
    }

    #[test]
    fn cleanup_failure_cannot_be_hidden_by_successful_sql_or_original_failure() {
        assert!(with_cleanup(Ok(vec![1, 2]), Err(anyhow::anyhow!("drain failed"))).is_err());
        let result: Result<()> = with_cleanup(
            Err(anyhow::anyhow!("SQL failed")),
            Err(anyhow::anyhow!("drain failed")),
        );
        let message = result.unwrap_err().to_string();
        assert!(message.contains("SQL failed") && message.contains("drain failed"));
    }

    #[test]
    fn cancelled_h2_connection_is_joined_before_cleanup_returns() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        struct Exited(Arc<AtomicBool>);
        impl Drop for Exited {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let exited = Arc::new(AtomicBool::new(false));
            let retained = exited.clone();
            let (started, observed) = tokio::sync::oneshot::channel();
            let connection_job = tokio::spawn(async move {
                let _owner = Exited(retained);
                started.send(()).unwrap();
                std::future::pending::<std::result::Result<(), h2::Error>>().await
            });
            observed.await.unwrap();
            join_probe_connection(Some(connection_job)).await.unwrap();
            assert!(exited.load(Ordering::Acquire));
        });
    }

    #[test]
    fn h2_connection_panic_is_a_cleanup_failure() {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let connection_job: tokio::task::JoinHandle<std::result::Result<(), h2::Error>> =
                tokio::spawn(async {
                    panic!("connection job panic");
                });
            tokio::time::timeout(Duration::from_secs(1), async {
                while !connection_job.is_finished() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert!(join_probe_connection(Some(connection_job)).await.is_err());
        });
    }
}
