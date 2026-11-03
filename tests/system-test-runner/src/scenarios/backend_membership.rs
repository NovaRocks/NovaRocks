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

//! Process-boundary backend self-registration acceptance.

use super::distributed_writer::{assert_commit_never_invoked, publication_terminals};
use super::native_sql_controls::{SqlIntentOwner, StatementSpec};
use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, bail, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::TestClusterOwner;
use novarocks_failpoint::target_binding::StatementExecutionBinding;
use std::fs;
use std::io::{Read, Write};
use std::net::Shutdown;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;
#[cfg(test)]
use std::time::Duration;
use std::time::Instant;

const REQUIRED_BACKENDS: usize = 3;

/// The task protocol's restart rendezvous: it holds one applied
/// `EstablishParticipation` open so the harness can replace that exact process.
/// It replaces the retired protocol's `restart-after-init-ack` because
/// establish is the task protocol's first per-backend admission point.
const RESTART_AFTER_ESTABLISH_PARTICIPATION: &str = "restart-after-establish-participation";

/// Runner-side half of the establish-before-restart handoff.
///
/// The listener exists before the query starts. The backend sends the exact
/// fault token after it applies EstablishParticipation and keeps that RPC
/// parked until the runner completes process replacement. A log marker proves
/// the same event but cannot synchronize it: a polling runner can see the
/// marker only after the backend has consumed the request boundary.
struct RestartAfterEstablishRendezvous {
    path: PathBuf,
    token: String,
    listener: UnixListener,
}

impl RestartAfterEstablishRendezvous {
    const CANCEL: &'static [u8] = b"scenario-cancelled";

    fn bind(token: &str) -> Result<Self> {
        let path = novarocks_failpoint::restart_after_establish_rendezvous_socket_path(token)
            .map_err(anyhow::Error::msg)?;
        if path.exists() {
            fs::remove_file(&path)
                .with_context(|| format!("remove stale establish rendezvous {}", path.display()))?;
        }
        let listener = UnixListener::bind(&path)
            .with_context(|| format!("bind establish rendezvous {}", path.display()))?;
        Ok(Self {
            path,
            token: token.to_owned(),
            listener,
        })
    }

    /// Receives the backend's exact token before the runner kills the old
    /// process. On timeout, connect once with a local cancellation payload so
    /// the accept worker can finish before cleanup removes the listener.
    fn wait_for_trigger(&self, deadline: Instant) -> Result<UnixStream> {
        let listener = self
            .listener
            .try_clone()
            .context("clone establish rendezvous listener")?;
        let token = self.token.clone();
        std::thread::scope(|scope| -> Result<UnixStream> {
            let (sender, receiver) = mpsc::sync_channel(1);
            scope.spawn(move || {
                let result = (|| -> Result<UnixStream> {
                    let (mut stream, _) =
                        listener.accept().context("accept establish rendezvous")?;
                    let mut payload = Vec::new();
                    stream
                        .read_to_end(&mut payload)
                        .context("read establish rendezvous token")?;
                    if payload == token.as_bytes() {
                        return Ok(stream);
                    }
                    if payload == Self::CANCEL {
                        bail!("scenario ended before the establish rendezvous fired");
                    }
                    bail!("establish rendezvous received an unexpected token");
                })();
                let _ = sender.send(result);
            });
            match receiver.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                Ok(result) => result,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    Self::cancel(&self.path);
                    let _ = receiver.recv();
                    bail!("timed out waiting for establish-before-restart rendezvous");
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    bail!("establish rendezvous worker disconnected before an arrival");
                }
            }
        })
    }

    fn cancel(path: &Path) {
        let Ok(mut stream) = UnixStream::connect(path) else {
            return;
        };
        let _ = stream.write_all(Self::CANCEL);
        let _ = stream.shutdown(Shutdown::Write);
    }
}

impl Drop for RestartAfterEstablishRendezvous {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(BackendSelfRegistration),
        Box::new(PreReadyReplan::default()),
        Box::new(PreReadyDmlNoRecovery::default()),
    ]
}

struct BackendSelfRegistration;

impl Scenario for BackendSelfRegistration {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "backend_membership",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "membership/self-registration"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == REQUIRED_BACKENDS,
            "membership acceptance requires exactly {REQUIRED_BACKENDS} BEs"
        );
        let initial_process_ids = (0..REQUIRED_BACKENDS)
            .map(|index| context.handle().backend_process_id(index))
            .collect::<Result<Vec<_>>>()?;
        // A parsed process identity is valid by construction, so the fact
        // worth asserting is that the three backends are three *different*
        // processes — a shared identity would mean the topology is not what
        // this acceptance claims to exercise.
        let distinct = initial_process_ids
            .iter()
            .collect::<std::collections::BTreeSet<_>>();
        ensure!(
            distinct.len() == initial_process_ids.len(),
            "self-registered backends share a process identity: {initial_process_ids:?}"
        );
        context.action(format!(
            "observed {} eligible self-registered backend process identities",
            initial_process_ids.len()
        ));

        query_one(context, "before FE restart")?;
        let deadline = context.deadline();
        context
            .handle()
            .drain_be_until(REQUIRED_BACKENDS - 1, deadline)
            .context("gracefully drain one BE through SIGTERM")?;
        query_one(context, "while one BE is draining")?;
        context.action(
            "proved SIGTERM removes a BE from future eligibility while the remaining BEs serve queries",
        );
        let deadline = context.deadline();
        context
            .handle()
            .restart_be_until(REQUIRED_BACKENDS - 1, deadline)
            .context("replace drained BE and wait for a new eligible process identity")?;
        let live_process_ids = (0..REQUIRED_BACKENDS)
            .map(|index| context.handle().backend_process_id(index))
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            live_process_ids[REQUIRED_BACKENDS - 1] != initial_process_ids[REQUIRED_BACKENDS - 1],
            "replacement after drain must receive a new BackendProcessId"
        );

        let deadline = context.deadline();
        context
            .handle()
            .restart_fe_until(deadline)
            .context("restart FE and wait for BE renew announce plus exact heartbeat")?;
        let after_fe_restart = (0..REQUIRED_BACKENDS)
            .map(|index| context.handle().backend_process_id(index))
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            after_fe_restart == live_process_ids,
            "FE restart must rebuild membership from the same live BE process identities"
        );
        context.action("proved FE restart rebuilds only from BE renew announce and heartbeat");

        let deadline = context.deadline();
        context
            .handle()
            .restart_be_until(0, deadline)
            .context("restart BE[0] and wait for replacement identity eligibility")?;
        let replacement_process_id = context.handle().backend_process_id(0)?;
        ensure!(
            replacement_process_id != initial_process_ids[0],
            "same endpoint replacement must receive a new BackendProcessId"
        );
        query_one(context, "after endpoint replacement")?;
        context.action(
            "proved endpoint replacement cannot inherit the prior process identity and remains queryable only after re-verification",
        );
        Ok(())
    }
}

fn query_one(context: &mut ScenarioContext, phase: &str) -> Result<()> {
    let mut connection = mysql_actor::connect(
        context.mysql_user(),
        context.mysql_port(),
        context.remaining(&format!("connect {phase}"))?,
    )?;
    let rows: Vec<i64> = connection
        .query("SELECT 1")
        .with_context(|| format!("run distributed query {phase}"))?;
    ensure!(
        rows == vec![1],
        "distributed query {phase} returned {rows:?}"
    );
    Ok(())
}

const REPLAN_SQL: &str = "SELECT v FROM pre_ready_replan.ns.probe ORDER BY v";
const DML_SQL: &str = "INSERT INTO pre_ready_dml.ns.orders VALUES (1, 10), (2, 20)";

// Every worker is joined, including an early rendezvous/replacement error. The
// original socket deadline still owns termination; this guard never detaches it.
struct MembershipActor(Option<thread::JoinHandle<()>>);
impl Drop for MembershipActor {
    fn drop(&mut self) {
        if let Some(actor) = self.0.take()
            && actor.join().is_err()
        {
            eprintln!("Original membership SQL actor panicked during join");
        }
    }
}

fn membership_statement(sql: &str) -> StatementSpec {
    StatementSpec {
        sql: sql.into(),
        faults: vec![(0, RESTART_AFTER_ESTABLISH_PARTICIPATION)],
        observations: vec![(
            0,
            novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol.as_str(),
            Some(1),
        )],
    }
}

#[derive(Default)]
struct PreReadyReplan {
    intent: SqlIntentOwner,
}

impl Scenario for PreReadyReplan {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "backend_membership",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "membership/pre-ready-replan"
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        Ok(Some(self.intent.freeze(
            vec![membership_statement(REPLAN_SQL)],
            &[0],
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == REQUIRED_BACKENDS,
            "pre-ready replan acceptance requires exactly {REQUIRED_BACKENDS} BEs"
        );
        // The subject is a backend replaced after it admitted the attempt and
        // before the attempt became ready, so the query has to place a task on
        // that exact backend. A constant-relation query does not: a fragment
        // with no scan gets one instance, and the scheduler puts it on a
        // backend chosen from the query id. A connector scan is what fans a
        // fragment out to every live backend, which is why this fixture is a
        // real table rather than a UNION ALL of literals.
        let catalog = "pre_ready_replan";
        let warehouse = context.runtime_dir().join("pre-ready-replan-warehouse");
        fs::create_dir_all(&warehouse)
            .with_context(|| format!("create replan warehouse {}", warehouse.display()))?;
        let mut setup = mysql_actor::connect(
            context.mysql_user(),
            context.mysql_port(),
            context.remaining("connect replan setup client")?,
        )?;
        setup
            .query_drop(format!(
                "CREATE EXTERNAL CATALOG {catalog} PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"hadoop\",\"iceberg.catalog.warehouse\"=\"{}\")",
                warehouse.display()
            ))
            .context("create replan Hadoop Iceberg catalog")?;
        setup
            .query_drop(format!("CREATE DATABASE {catalog}.ns"))
            .context("create replan Iceberg namespace")?;
        setup
            .query_drop(format!("SET CATALOG {catalog}"))
            .context("select replan Iceberg catalog")?;
        setup
            .query_drop("USE ns")
            .context("select replan Iceberg namespace")?;
        setup
            .query_drop("CREATE TABLE probe (v BIGINT)")
            .context("create replan scan table")?;
        setup
            .query_drop("INSERT INTO probe VALUES (1), (2)")
            .context("seed replan scan table")?;
        drop(setup);

        let target = 0;
        let old_process_id = context.handle().backend_process_id(target)?;
        let selected = self.intent.take(0)?;
        let connection = mysql_actor::connect_until(
            context.mysql_user(),
            context.mysql_port(),
            context.deadline(),
        )?;
        let observation = selected.arm(context, connection.connection_id(), REPLAN_SQL)?;
        let token = selected.token.clone();
        let rendezvous = RestartAfterEstablishRendezvous::bind(&token)?;
        context.action(format!(
            "armed token-scoped restart after BE[{target}] EstablishParticipation; old_process_id={old_process_id}"
        ));
        let (sender, receiver) = mpsc::sync_channel(1);
        let _actor = MembershipActor(Some(thread::spawn(move || {
            let result: Result<Vec<i64>> = connection.run(|connection| {
                connection
                    .query(REPLAN_SQL)
                    .context("run query during pre-ready replacement")
            });
            let _ = sender.send(result);
        })));

        let restart_ack = rendezvous.wait_for_trigger(context.deadline())?;
        // The canonical owner validates the original session/process receipt.
        // FE publishes this exact first-attempt binding before Establish runs.
        let binding = context
            .handle()
            .selected_native_observation_binding(
                target,
                novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol.as_str(),
                &token,
                Some(1),
            )?
            .context("fired restart lacks its declared first-attempt binding")?;
        let expected_attempt = bound_replan_attempt(&binding, &token, target)?;

        let deadline = context.deadline();
        context
            .handle()
            .restart_be_until(target, deadline)
            .context("replace BE immediately after token-scoped EstablishParticipation")?;
        let replacement_process_id = context.handle().backend_process_id(target)?;
        ensure!(
            replacement_process_id != old_process_id,
            "pre-ready replacement must create a new BackendProcessId"
        );
        context
            .handle()
            .clear_query_lifecycle_faults()
            .context("clear pre-ready restart trigger")?;
        drop(restart_ack);

        let remaining = context.remaining("await re-planned query")?;
        let rows = receiver.recv_timeout(remaining).map_err(|error| {
            anyhow::anyhow!("pre-ready query did not return before deadline: {error}")
        })??;
        ensure!(
            rows == vec![1, 2],
            "pre-ready re-planned query returned unexpected rows: {rows:?}"
        );
        let deadline = context.deadline();
        let terminal = context
            .handle()
            .await_query_lifecycle_statement_observation(&observation, true, deadline)
            .context("read terminal snapshot for re-planned statement")?;
        ensure!(
            terminal.attempt_id == 2
                && terminal.process_namespace == expected_attempt.process_namespace
                && terminal.local_sequence == expected_attempt.local_sequence,
            "pre-ready replacement snapshot mismatch: candidate={terminal:?}, target={expected_attempt:?}"
        );
        context
            .handle()
            .drain_query_lifecycle_statement_observation(&observation)?;
        context.action(format!(
            "replaced BE[{target}] after EstablishParticipation and observed successful statement attempt=2 completion with new_process_id={replacement_process_id}"
        ));
        Ok(())
    }
}

fn bound_replan_attempt(
    binding: &StatementExecutionBinding,
    token: &str,
    backend_index: usize,
) -> Result<novarocks_cluster_harness::QueryLifecycleAttemptTarget> {
    binding.validate()?;
    ensure!(
        binding.statement_run_token == token
            && binding.backend_index == backend_index
            && binding.attempt_id == 1,
        "restart binding must name the exact fired token/backend and first attempt"
    );
    let local_sequence =
        u64::try_from(binding.query_id_lo).context("restart binding local sequence is negative")?;
    ensure!(local_sequence > 0, "restart binding local sequence is zero");
    Ok(novarocks_cluster_harness::QueryLifecycleAttemptTarget {
        process_namespace: u64::from_ne_bytes(binding.query_id_hi.to_ne_bytes()),
        local_sequence,
        attempt_id: 2,
    })
}

#[derive(Default)]
struct PreReadyDmlNoRecovery {
    intent: SqlIntentOwner,
}

impl Scenario for PreReadyDmlNoRecovery {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "backend_membership",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "membership/pre-ready-dml-no-recovery"
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        Ok(Some(self.intent.freeze(
            vec![membership_statement(DML_SQL)],
            &[0],
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == REQUIRED_BACKENDS,
            "pre-ready DML no-recovery acceptance requires exactly {REQUIRED_BACKENDS} BEs"
        );
        let catalog = "pre_ready_dml";
        let warehouse = context.runtime_dir().join("pre-ready-dml-warehouse");
        fs::create_dir_all(&warehouse)
            .with_context(|| format!("create DML warehouse {}", warehouse.display()))?;
        let mut setup = mysql_actor::connect(
            context.mysql_user(),
            context.mysql_port(),
            context.remaining("connect DML setup client")?,
        )?;
        setup
            .query_drop(format!(
                "CREATE EXTERNAL CATALOG {catalog} PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"hadoop\",\"iceberg.catalog.warehouse\"=\"{}\")",
                warehouse.display()
            ))
            .context("create DML Hadoop Iceberg catalog")?;
        setup
            .query_drop(format!("CREATE DATABASE {catalog}.ns"))
            .context("create DML Iceberg namespace")?;
        setup
            .query_drop(format!("SET CATALOG {catalog}"))
            .context("select DML Iceberg catalog")?;
        setup
            .query_drop("USE ns")
            .context("select DML Iceberg namespace")?;
        setup
            .query_drop("CREATE TABLE orders (k1 INT, v2 BIGINT)")
            .context("create DML target table")?;
        drop(setup);

        // This scenario owns a fresh FE and runs no concurrent DML between the
        // snapshots. The exact token-scoped observation below binds this one
        // statement; the publication labels independently prove no commit.
        let publication_before = publication_terminals(context, "write")?;
        let target = 0;
        let old_process_id = context.handle().backend_process_id(target)?;
        let selected = self.intent.take(0)?;
        let connection = mysql_actor::connect_until(
            context.mysql_user(),
            context.mysql_port(),
            context.deadline(),
        )?;
        let observation = selected.arm(context, connection.connection_id(), DML_SQL)?;
        let token = selected.token.clone();
        let rendezvous = RestartAfterEstablishRendezvous::bind(&token)?;
        context.action(format!(
            "armed token-scoped restart after BE[{target}] DML EstablishParticipation; old_process_id={old_process_id}"
        ));
        let (sender, receiver) = mpsc::sync_channel(1);
        let _actor = MembershipActor(Some(thread::spawn(move || {
            let result = connection.run(|connection| {
                connection
                    .query_drop(DML_SQL)
                    .context("run DML during pre-ready replacement")
            });
            let _ = sender.send(result);
        })));

        let restart_ack = rendezvous.wait_for_trigger(context.deadline())?;
        let deadline = context.deadline();
        context
            .handle()
            .restart_be_until(target, deadline)
            .context("replace BE immediately after token-scoped DML EstablishParticipation")?;
        let replacement_process_id = context.handle().backend_process_id(target)?;
        ensure!(
            replacement_process_id != old_process_id,
            "pre-ready DML replacement must create a new BackendProcessId"
        );
        context
            .handle()
            .clear_query_lifecycle_faults()
            .context("clear pre-ready DML restart trigger")?;
        drop(restart_ack);
        let error = receiver
            .recv_timeout(context.remaining("await no-recovery DML")?)
            .map_err(|error| {
                anyhow::anyhow!("pre-ready DML did not return before deadline: {error}")
            })?
            .expect_err("a pre-ready external-effect execution must not auto-replan");
        let error_message = format!("{error:#}");
        // KnownUncommitted is a typed publication sidecar, not a suffix of
        // the canonical public execution error (SEP-1).
        let publication_after = publication_terminals(context, "write")?;
        assert_commit_never_invoked(
            publication_before,
            publication_after,
            "pre-ready DML without recovery",
        )
        .with_context(|| format!("verify failed DML publication: {error_message}"))?;

        let deadline = context.deadline();
        let terminal = context
            .handle()
            .await_query_lifecycle_statement_observation(&observation, false, deadline)?;
        ensure!(
            terminal.attempt_id == 1,
            "pre-ready DML must retain its original attempt without auto-recovery, got: {terminal:?}"
        );
        context
            .handle()
            .drain_query_lifecycle_statement_observation(&observation)?;

        let mut verify = mysql_actor::connect(
            context.mysql_user(),
            context.mysql_port(),
            context.remaining("connect DML verification client")?,
        )?;
        let rows: Vec<(i32, i64)> = verify
            .query("SELECT k1, v2 FROM pre_ready_dml.ns.orders ORDER BY k1")
            .context("read DML target after no-recovery statement")?;
        ensure!(
            rows.is_empty(),
            "the rejected pre-ready DML must not publish data, got {rows:?}"
        );
        verify
            .query_drop("INSERT INTO pre_ready_dml.ns.orders VALUES (1, 10), (2, 20)")
            .context("retry the entire DML statement after KnownUncommitted")?;
        let rows: Vec<(i32, i64)> = verify
            .query("SELECT k1, v2 FROM pre_ready_dml.ns.orders ORDER BY k1")
            .context("read DML target after client retry")?;
        ensure!(
            rows == vec![(1, 10), (2, 20)],
            "the client statement retry must publish exactly once, got {rows:?}"
        );
        context.action(format!(
            "replaced BE[{target}] after DML EstablishParticipation; the first attempt returned KnownUncommitted without published data, then an explicit client statement retry published once; new_process_id={replacement_process_id}"
        ));
        Ok(())
    }
}

#[cfg(test)]
mod binding_tests {
    use super::*;

    #[test]
    fn replan_binding_uses_the_exact_fired_token_backend_and_signed_namespace() {
        let binding = StatementExecutionBinding {
            schema_version: 1,
            target_session_id: "original-session".into(),
            statement_run_token: "fired".into(),
            backend_index: 1,
            backend_process_id: novarocks_types::BackendProcessId::new_v7().to_string(),
            query_id_hi: -11,
            query_id_lo: 12,
            attempt_id: 1,
        };
        let target = bound_replan_attempt(&binding, "fired", 1).expect("exact binding");
        assert_eq!(
            target.process_namespace,
            u64::from_ne_bytes((-11_i64).to_ne_bytes())
        );
        assert_eq!(target.local_sequence, 12);
        assert_eq!(target.attempt_id, 2);
        assert!(bound_replan_attempt(&binding, "fired", 0).is_err());
        assert!(bound_replan_attempt(&binding, "unrelated", 1).is_err());
        let mut later = binding.clone();
        later.attempt_id = 2;
        assert!(bound_replan_attempt(&later, "fired", 1).is_err());
    }

    #[test]
    fn membership_plans_reserve_the_original_fault_and_only_an_ordinary_successor() {
        let binary = std::env::current_exe().unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        for sql in [REPLAN_SQL, DML_SQL] {
            let owner = SqlIntentOwner::default();
            let plan = owner
                .freeze(vec![membership_statement(sql)], &[0], 3, &binary, deadline)
                .unwrap();
            plan.validate_selected_intents(3).unwrap();
            let selected = owner.take(0).unwrap();
            assert_eq!(selected.sql, sql);
            assert_eq!(
                selected.faults,
                vec![(0, RESTART_AFTER_ESTABLISH_PARTICIPATION)]
            );
            assert_eq!(selected.deadline, deadline);
            assert!(owner.take(0).is_err());
        }
    }
}
