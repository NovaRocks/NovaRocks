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

//! Exact compatibility-artifact role declarations for finite SQL observations.
use super::super::native_sql_controls::SelectedStatement;
use crate::{
    actors::sql::{self, SqlSession, StatementFailure, StatementResult},
    scenario::ScenarioContext,
};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::test_sessions::{
    ClusterTestSessionPlan, RoleLaunchReference, RoleSlot,
};
use novarocks_cluster_harness::{
    CrossProcessCluster, QueryLifecycleStatementObservation, TestClusterOwner,
};
use novarocks_failpoint::session::DomainGrant;
use novarocks_test_trino_client::CancelHandle;
use serde_json::json;
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::Mutex,
    time::Instant,
};

pub(super) struct CompatibilitySql {
    pub sql: String,
    /// The initial BE or the explicitly reserved replacement, never next BE.
    pub backends: Vec<usize>,
}
struct Launch {
    reference: RoleLaunchReference,
    binary: PathBuf,
    grants: Vec<DomainGrant>,
}
fn add(launch: &mut Launch, domain: &str, parameters: serde_json::Value) {
    launch.grants.push(DomainGrant {
        domain: domain.into(),
        grant_id: format!("{domain}-{}", launch.grants.len()),
        parameters,
    });
}
#[derive(Default)]
pub(super) struct CompatibilityIntent(Mutex<Option<(Vec<SelectedStatement>, BTreeSet<usize>)>>);
impl CompatibilityIntent {
    /// All paths are the actual scenario-selected artifacts. This helper never
    /// substitutes the runner's primary binary for a compatibility build.
    pub fn freeze(
        &self,
        frontend: &Path,
        backends: &[PathBuf],
        replacement: Option<(usize, &Path)>,
        statements: Vec<CompatibilitySql>,
        direct_backends: &[usize],
        deadline: Instant,
    ) -> Result<ClusterTestSessionPlan> {
        ensure!(
            backends.len() == 3 && (!statements.is_empty() || !direct_backends.is_empty()),
            "compatibility SQL requires finite native 1FE+3BE intent"
        );
        let mut state = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("compatibility intent owner poisoned"))?;
        ensure!(state.is_none(), "compatibility SQL intent already frozen");
        let mut plan = ClusterTestSessionPlan::new(deadline)?;
        let root = plan.reserve_transport("compatibility-statements")?;
        let mut fe = Launch {
            reference: plan.reserve_launch(RoleSlot::Frontend)?,
            binary: frontend.into(),
            grants: vec![],
        };
        let mut bes = backends
            .iter()
            .enumerate()
            .map(|(index, binary)| {
                Ok(Launch {
                    reference: plan.reserve_launch(RoleSlot::Backend(index))?,
                    binary: binary.clone(),
                    grants: vec![],
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut successor = replacement
            .map(|(index, binary)| {
                ensure!(index < 3, "compatibility replacement index is invalid");
                Ok::<_, anyhow::Error>((
                    index,
                    Launch {
                        reference: plan.reserve_launch(RoleSlot::Backend(index))?,
                        binary: binary.into(),
                        grants: vec![],
                    },
                ))
            })
            .transpose()?;
        if !statements.is_empty() {
            let secret = plan.fresh_domain_secret()?;
            add(
                &mut fe,
                "statement-observation",
                json!({"action":"observe", "bearer_secret":secret}),
            );
        }
        let mut direct_seen = BTreeSet::new();
        for &index in direct_backends {
            ensure!(
                index < 3 && direct_seen.insert(index),
                "invalid or duplicate direct Native compatibility target"
            );
            let be = &mut bes[index];
            let target_session = be.reference.session_id.clone();
            // This is the original raw fixture query identity, not a minted
            // SQL nomination or a request to observe an arbitrary next query.
            add(
                be,
                "native-observation",
                json!({
                    "action":novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol.as_str(), "root":root, "backend_index":index,
                    "target_session_id":target_session, "max_events":16384,
                    "query_execution":{"query_id_hi":91,"query_id_lo":92,"attempt_id":1},
                }),
            );
        }
        let mut selected = vec![];
        for statement in statements {
            ensure!(
                !statement.sql.trim().is_empty(),
                "compatibility submission must contain complete SQL"
            );
            let token = plan.fresh_domain_secret()?;
            let mut seen = BTreeSet::new();
            for index in statement.backends {
                ensure!(
                    index < 3 && seen.insert(index),
                    "duplicate or invalid compatibility SQL backend"
                );
                let be = if let Some((replacement_index, launch)) = successor.as_mut() {
                    ensure!(
                        index == *replacement_index,
                        "post-replacement SQL must name its exact replacement launch"
                    );
                    launch
                } else {
                    &mut bes[index]
                };
                add(
                    &mut fe,
                    "native-observation",
                    json!({
                        "action":"bind-task-protocol", "root":root, "backend_index":index,
                        "statement_run_token":token, "target_session_id":be.reference.session_id,
                        "target_process_receipt":be.reference.process_receipt,
                    }),
                );
                let target_session = be.reference.session_id.clone();
                add(
                    be,
                    "native-observation",
                    json!({
                        "action":novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol.as_str(), "root":root, "backend_index":index,
                        "statement_run_token":token, "target_session_id":target_session, "max_events":16384,
                    }),
                );
            }
            selected.push(SelectedStatement {
                sql: statement.sql,
                token,
                faults: vec![],
                deadline,
            });
        }
        authorize(&mut plan, &fe, "novarocks-frontend-application")?;
        for be in &bes {
            authorize(&mut plan, be, "novarocks-native-adapter")?;
        }
        if let Some((_, be)) = &successor {
            authorize(&mut plan, be, "novarocks-native-adapter")?;
        }
        plan.validate_selected_intents(3)?;
        *state = Some((selected, BTreeSet::new()));
        Ok(plan)
    }
    pub fn take(&self, ordinal: usize) -> Result<SelectedStatement> {
        let mut state = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("compatibility intent owner poisoned"))?;
        let (statements, used) = state
            .as_mut()
            .context("compatibility intent was not frozen before launch")?;
        let selected = statements
            .get(ordinal)
            .context("SQL outside frozen compatibility intent")?
            .clone();
        ensure!(
            used.insert(ordinal),
            "compatibility SQL was already submitted"
        );
        Ok(selected)
    }
}
fn authorize(plan: &mut ClusterTestSessionPlan, launch: &Launch, owner: &str) -> Result<()> {
    if launch.grants.is_empty() {
        plan.authorize_uncontrolled_launch(
            &launch.reference,
            launch.binary.clone(),
            vec![
                ("novarocks-server".into(), "test-controls".into()),
                (owner.into(), "test-controls".into()),
            ],
        )
    } else {
        plan.authorize_launch(
            &launch.reference,
            launch.grants.clone(),
            vec![
                ("novarocks-server".into(), "test-controls".into()),
                (owner.into(), "test-controls".into()),
            ],
        )?;
        plan.set_launch_binary(&launch.reference, launch.binary.clone())
    }
}

/// These are three real SQL submissions, even when their complete SQL text is
/// equal. Each receives one independently frozen statement run token.
pub(super) fn run_baselines(
    context: &mut ScenarioContext,
    intent: &CompatibilityIntent,
    full_sql: &str,
    expected_backends: &[usize],
) -> Result<()> {
    let mut connection =
        SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
    for ordinal in 0..3 {
        let selected = intent.take(ordinal)?;
        let pending = connection.submit(full_sql)?;
        let cancel = pending.cancel_handle();
        let (pending, observation) = selected.arm_pending(context, pending, full_sql)?;
        let outcome = connection.run_pending(pending);
        let outcome_class = classify_sql_outcome(&outcome);
        let sql_succeeded = outcome.is_ok();
        let reentry = if matches!(&outcome, Err(StatementFailure::Trino(_))) {
            require_public_error_reentry(&mut connection)
        } else {
            Ok(())
        };
        let result = outcome
            .with_context(|| format!("run compatibility-island query {}", ordinal + 1))
            .and_then(|result| {
                let rows = sql::integer_column(&result)?;
                ensure!(
                    rows == vec![1, 2],
                    "compatibility-island query {} returned {rows:?}",
                    ordinal + 1
                );
                Ok(())
            });
        let deadline = context.deadline();
        let cancelled = if matches!(&outcome_class, SqlOutcomeClass::Unresolved) {
            let cancelled = cancel.delete(deadline).map_err(Into::into);
            if cancelled.is_err() {
                combine(cancelled, context.shutdown())
            } else {
                cancelled
            }
        } else {
            Ok(())
        };
        let observed =
            settle_sql_observation(context.handle(), &observation, sql_succeeded, deadline);
        combine(combine(combine(result, reentry), cancelled), observed)?;
    }
    for &backend in expected_backends {
        context.handle().assert_be_log(
            backend,
            super::super::task_evidence::PARTICIPATION_ESTABLISH_APPLIED,
        )?;
    }
    Ok(())
}

fn require_public_error_reentry(connection: &mut SqlSession) -> Result<()> {
    let result = connection
        .execute("SELECT 1")
        .context("reenter the original compatibility session after its public SQL error")?;
    ensure!(
        sql::integer_column(&result)? == vec![1],
        "compatibility reentry returned another row set"
    );
    Ok(())
}

fn settle_sql_observation(
    handle: &mut dyn TestClusterOwner,
    observation: &QueryLifecycleStatementObservation,
    sql_succeeded: bool,
    deadline: Instant,
) -> Result<()> {
    if sql_succeeded {
        let terminal = handle
            .await_query_lifecycle_statement_observation(observation, true, deadline)
            .map(|_| ());
        let drain = handle.drain_query_lifecycle_statement_binding(observation, deadline);
        combine(terminal, drain)
    } else {
        // This validates the original attribution, not worker completion.
        handle.drain_query_lifecycle_statement_binding(observation, deadline)
    }
}
pub(super) fn combine<T>(primary: Result<T>, cleanup: Result<()>) -> Result<T> {
    match (primary, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(primary), Err(cleanup)) => Err(DualFailure { primary, cleanup }.into()),
    }
}
struct DualFailure {
    primary: anyhow::Error,
    cleanup: anyhow::Error,
}
impl std::fmt::Display for DualFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "compatibility operation failed: {}; actual cleanup also failed: {}",
            self.primary, self.cleanup
        )
    }
}
impl std::fmt::Debug for DualFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::error::Error for DualFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.primary.as_ref())
    }
}

/// The source actor owns its exact submitted query through real exit.
/// Its original FE drain closes the source observation as before.
pub(super) struct SourceQuery {
    actor: Option<std::thread::JoinHandle<std::result::Result<StatementResult, StatementFailure>>>,
    cancel: CancelHandle,
    deadline: Instant,
}
impl SourceQuery {
    pub fn start(context: &mut ScenarioContext, selected: SelectedStatement) -> Result<Self> {
        let deadline = context.deadline();
        let mut connection = SqlSession::new(context.sql_user(), context.trino_port(), deadline)?;
        let pending = connection.submit(&selected.sql)?;
        let (pending, observation) = selected.arm_pending(context, pending, &selected.sql)?;
        let cancel = pending.cancel_handle();
        let actor = std::thread::Builder::new()
            .name("compatibility-source-cutover".into())
            .spawn(move || connection.run_pending(pending));
        let actor = match actor {
            Ok(actor) => actor,
            Err(primary) => {
                let cancelled = cancel.delete(deadline).map_err(Into::into);
                let drained = context
                    .handle()
                    .drain_query_lifecycle_statement_binding(&observation, deadline);
                return combine(Err(primary.into()), combine(cancelled, drained));
            }
        };
        let mut source = Self {
            actor: Some(actor),
            cancel,
            deadline,
        };
        if let Err(primary) = context
            .handle()
            .await_query_lifecycle_statement_owned(&observation, deadline)
        {
            let exited = source.close();
            let drained = context
                .handle()
                .drain_query_lifecycle_statement_binding(&observation, deadline);
            return combine(Err(primary), combine(exited, drained));
        }
        Ok(source)
    }
    pub fn join(mut self) -> Result<()> {
        let joined = join_source(self.actor.take().expect("live source query actor"));
        let unresolved = source_outcome_unresolved(&joined);
        let cancelled = if unresolved {
            self.cancel.delete(self.deadline).map_err(Into::into)
        } else {
            Ok(())
        };
        let result = joined
            .and_then(|actual| actual.map_err(anyhow::Error::from))
            .and_then(|result| {
                ensure!(
                    result.is_result_set()
                        && result.columns.len() == 1
                        && result.columns[0].type_name == "boolean"
                        && result.columns[0].type_signature.raw_type == "boolean"
                        && result.columns[0].type_signature.arguments.is_empty()
                        && result.rows == vec![vec![novarocks_test_trino_client::TrinoValue::Boolean(true)]],
                    "source pre-cutover SLEEP query did not return one completed BOOLEAN row: {result:?}"
                );
                Ok(())
            });
        combine(result, cancelled)
    }
    fn close(&mut self) -> Result<()> {
        let Some(actor) = self.actor.take() else {
            return Ok(());
        };
        let was_running = !actor.is_finished();
        let mut cancelled = if was_running {
            self.cancel.delete(self.deadline).map_err(Into::into)
        } else {
            Ok(())
        };
        let actual = join_source(actor);
        // Actual local exit does not settle an HTTP/transport/panic outcome.
        if !was_running && source_outcome_unresolved(&actual) {
            cancelled = self.cancel.delete(self.deadline).map_err(Into::into);
        }
        let joined = actual.and_then(|result| match result {
            Ok(_) => Ok(()),
            Err(StatementFailure::Trino(error))
                if error.error_code == 3
                    && error.error_name == "USER_CANCELED"
                    && error.error_type == "USER_ERROR" =>
            {
                Ok(())
            }
            Err(error) => {
                Err(anyhow::Error::new(error).context("original source SQL result during cleanup"))
            }
        });
        combine(cancelled, joined)
    }
}

#[derive(Clone, Copy)]
enum SqlOutcomeClass {
    Terminal,
    Unresolved,
}
fn classify_sql_outcome(
    actual: &std::result::Result<StatementResult, StatementFailure>,
) -> SqlOutcomeClass {
    match actual {
        Ok(_) | Err(StatementFailure::Trino(_)) => SqlOutcomeClass::Terminal,
        Err(_) => SqlOutcomeClass::Unresolved,
    }
}
fn source_outcome_unresolved(
    actual: &Result<std::result::Result<StatementResult, StatementFailure>>,
) -> bool {
    !matches!(actual, Ok(Ok(_) | Err(StatementFailure::Trino(_))))
}

impl Drop for SourceQuery {
    fn drop(&mut self) {
        if let Err(error) = self.close() {
            eprintln!("Source compatibility actor cleanup failed: {error:#}");
        }
    }
}
fn join_source(
    actor: std::thread::JoinHandle<std::result::Result<StatementResult, StatementFailure>>,
) -> Result<std::result::Result<StatementResult, StatementFailure>> {
    actor
        .join()
        .map_err(|payload| anyhow::Error::new(SourcePanic(std::sync::Mutex::new(payload))))
}

struct SourcePanic(std::sync::Mutex<Box<dyn std::any::Any + Send>>);
impl std::fmt::Display for SourcePanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _ = &self.0;
        f.write_str("source compatibility actor panicked; payload retained")
    }
}
impl std::fmt::Debug for SourcePanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::error::Error for SourcePanic {}

pub(super) fn run_peer_baseline(
    target: &mut CrossProcessCluster,
    selected: SelectedStatement,
    sql: &str,
    deadline: Instant,
) -> Result<()> {
    ensure!(
        selected.sql == sql && selected.deadline == deadline && Instant::now() < deadline,
        "peer SQL differs from its original finite intent"
    );
    let mut connection =
        SqlSession::new(target.sql_user(), target.runtime().fe_trino_port, deadline)?;
    let pending = connection.submit(sql)?;
    let cancel = pending.cancel_handle();
    let (pending, observation) = selected.arm_pending_on(target, pending, sql, deadline)?;
    let outcome = connection.run_pending(pending);
    let outcome_class = classify_sql_outcome(&outcome);
    let sql_succeeded = outcome.is_ok();
    let reentry = if matches!(&outcome, Err(StatementFailure::Trino(_))) {
        require_public_error_reentry(&mut connection)
    } else {
        Ok(())
    };
    let result = outcome
        .context("run new query on target island")
        .and_then(|result| {
            let rows = sql::integer_column(&result)?;
            ensure!(rows == vec![1, 2], "target island query returned {rows:?}");
            Ok(())
        });
    let cancelled = if matches!(outcome_class, SqlOutcomeClass::Unresolved) {
        let cancelled = cancel.delete(deadline).map_err(Into::into);
        if cancelled.is_err() {
            combine(cancelled, target.shutdown())
        } else {
            cancelled
        }
    } else {
        Ok(())
    };
    let observed = settle_sql_observation(target, &observation, sql_succeeded, deadline);
    combine(combine(combine(result, reentry), cancelled), observed)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn actual_finished_source_failure_remains_owned_and_unresolved() {
        let actor = std::thread::spawn(|| {
            Err(StatementFailure::Transport(
                "original source failure".into(),
            ))
        });
        let actual = join_source(actor).unwrap();
        assert!(matches!(
            classify_sql_outcome(&actual),
            SqlOutcomeClass::Unresolved
        ));
        let actual = actual.unwrap_err();
        assert!(
            matches!(actual, StatementFailure::Transport(message) if message == "original source failure")
        );
        assert!(source_outcome_unresolved(&Err(anyhow::anyhow!(
            "original panic owner"
        ))));
    }
    fn deadline() -> Instant {
        Instant::now() + std::time::Duration::from_secs(20)
    }
    #[test]
    fn equal_sql_still_has_distinct_once_only_nomination() {
        let artifact = tempfile::NamedTempFile::new().unwrap();
        let owner = CompatibilityIntent::default();
        let sql = "SELECT 1";
        let _plan = owner
            .freeze(
                artifact.path(),
                &vec![artifact.path().to_owned(); 3],
                None,
                (0..3)
                    .map(|_| CompatibilitySql {
                        sql: sql.into(),
                        backends: vec![0],
                    })
                    .collect(),
                &[],
                deadline(),
            )
            .unwrap();
        let first = owner.take(0).unwrap();
        let second = owner.take(1).unwrap();
        assert_ne!(first.token, second.token);
        assert_eq!(first.sql, second.sql);
        assert!(owner.take(0).is_err());
        assert!(owner.take(3).is_err());
    }
    #[test]
    fn direct_native_fixture_does_not_create_mysql_authority() {
        let artifact = tempfile::NamedTempFile::new().unwrap();
        let owner = CompatibilityIntent::default();
        let plan = owner
            .freeze(
                artifact.path(),
                &vec![artifact.path().to_owned(); 3],
                None,
                vec![],
                &[0],
                deadline(),
            )
            .unwrap();
        assert!(owner.take(0).is_err());
        let summaries = plan.planned_launch_summaries();
        let roles = summaries.as_array().unwrap();
        let fe = roles.iter().find(|role| role["role"] == "fe").unwrap();
        assert_eq!(fe["ordinary"], true);
        assert!(fe["grants"].as_array().unwrap().is_empty());
        assert!(
            roles
                .iter()
                .all(|role| role["binary"] == json!(artifact.path()))
        );
        let be = roles.iter().find(|role| role["role"] == "be-0").unwrap();
        assert_eq!(be["grants"].as_array().unwrap().len(), 1);
    }
    #[test]
    fn replacement_permission_does_not_adopt_initial_process() {
        let artifact = tempfile::NamedTempFile::new().unwrap();
        let replacement = tempfile::NamedTempFile::new().unwrap();
        let owner = CompatibilityIntent::default();
        let plan = owner
            .freeze(
                artifact.path(),
                &vec![artifact.path().to_owned(); 3],
                Some((2, replacement.path())),
                vec![CompatibilitySql {
                    sql: "SELECT 1".into(),
                    backends: vec![2],
                }],
                &[],
                deadline(),
            )
            .unwrap();
        let summaries = plan.planned_launch_summaries();
        let be2 = summaries
            .as_array()
            .unwrap()
            .iter()
            .filter(|role| role["role"] == "be-2")
            .collect::<Vec<_>>();
        assert_eq!(be2.len(), 2);
        assert_ne!(be2[0]["session_id"], be2[1]["session_id"]);
        assert!(be2[0]["grants"].as_array().unwrap().is_empty());
        assert_eq!(be2[0]["ordinary"], true);
        assert_eq!(be2[1]["grants"].as_array().unwrap().len(), 1);
        assert_eq!(be2[1]["ordinary"], false);
        assert_eq!(be2[0]["binary"], json!(artifact.path()));
        assert_eq!(
            be2[1]["binary"],
            replacement.path().to_string_lossy().as_ref()
        );
    }
}
