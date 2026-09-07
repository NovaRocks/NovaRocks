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

use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext};
use anyhow::{Context, Result, bail, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::ServerHandle;
use std::fs;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

const REQUIRED_BACKENDS: usize = 3;

/// The task protocol's restart rendezvous: it holds one applied
/// `EstablishQueryContext` open so the harness can replace that exact process.
/// It replaces the retired protocol's `restart-after-init-ack` because
/// establish is the task protocol's first per-backend admission point.
const RESTART_AFTER_ESTABLISH_CONTEXT: &str = "restart-after-establish-context";

/// The token-scoped marker that rendezvous emits once the establish applied.
const ESTABLISH_CONTEXT_OBSERVED_MARKER: &str = "NOVAROCKS_TASK_ESTABLISH_CONTEXT_OBSERVED";

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(BackendSelfRegistration),
        Box::new(PreReadyReplan),
        Box::new(PreReadyDmlReplan),
    ]
}

struct BackendSelfRegistration;

impl Scenario for BackendSelfRegistration {
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

struct PreReadyReplan;

impl Scenario for PreReadyReplan {
    fn name(&self) -> &'static str {
        "membership/pre-ready-replan"
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
        context
            .handle()
            .arm_query_lifecycle_fault(target, RESTART_AFTER_ESTABLISH_CONTEXT)
            .context("arm token-scoped BE restart after EstablishQueryContext")?;
        let token = context
            .handle()
            .armed_query_lifecycle_fault_token(target, RESTART_AFTER_ESTABLISH_CONTEXT)?
            .context("armed pre-ready restart has no token")?;
        let before_execution = context
            .handle()
            .query_lifecycle_structured_snapshot()?
            .and_then(|snapshot| snapshot.execution_id);
        context.action(format!(
            "armed token-scoped restart after BE[{target}] EstablishQueryContext; old_process_id={old_process_id}"
        ));

        let mysql_user = context.mysql_user().to_string();
        let mysql_port = context.mysql_port();
        let (sender, receiver) = mpsc::sync_channel(1);
        thread::spawn(move || {
            let result = (|| -> Result<Vec<i64>> {
                let mut connection =
                    mysql_actor::connect(&mysql_user, mysql_port, Duration::from_secs(30))?;
                connection
                    .query(format!("SELECT v FROM {catalog}.ns.probe ORDER BY v"))
                    .context("run query during pre-ready replacement")
            })();
            let _ = sender.send(result);
        });

        wait_for_token_scoped_marker(context, target, ESTABLISH_CONTEXT_OBSERVED_MARKER, &token)?;
        let deadline = context.deadline();
        context
            .handle()
            .restart_be_until(target, deadline)
            .context("replace BE immediately after token-scoped EstablishQueryContext")?;
        let replacement_process_id = context.handle().backend_process_id(target)?;
        ensure!(
            replacement_process_id != old_process_id,
            "pre-ready replacement must create a new BackendProcessId"
        );
        context
            .handle()
            .clear_query_lifecycle_faults()
            .context("clear pre-ready restart trigger")?;

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
            .await_query_lifecycle_structured_snapshot_after(before_execution.as_deref(), deadline)
            .context("read terminal snapshot for re-planned statement")?;
        ensure!(
            terminal.attempt_id == 2,
            "pre-ready replacement must complete as statement attempt 2, got attempt {}",
            terminal.attempt_id
        );
        context.action(format!(
            "replaced BE[{target}] after EstablishQueryContext and observed successful statement attempt=2 completion with new_process_id={replacement_process_id}"
        ));
        Ok(())
    }
}

struct PreReadyDmlReplan;

impl Scenario for PreReadyDmlReplan {
    fn name(&self) -> &'static str {
        "membership/pre-ready-dml-replan"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == REQUIRED_BACKENDS,
            "pre-ready DML replan acceptance requires exactly {REQUIRED_BACKENDS} BEs"
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

        let target = 0;
        let old_process_id = context.handle().backend_process_id(target)?;
        // The same rendezvous the read case uses. `restart-after-init-ack`
        // parked inside the retired `InitQuery` handler, which a distributed
        // write stopped reaching once the task protocol took over admission,
        // so arming it here left the fault with no emitter and this case
        // waiting out its budget for `NOVAROCKS_QUERY_INIT_ACK_OBSERVED`.
        // That handler and that fault kind have since been deleted.
        context
            .handle()
            .arm_query_lifecycle_fault(target, RESTART_AFTER_ESTABLISH_CONTEXT)
            .context("arm token-scoped BE restart after DML EstablishQueryContext")?;
        let token = context
            .handle()
            .armed_query_lifecycle_fault_token(target, RESTART_AFTER_ESTABLISH_CONTEXT)?
            .context("armed pre-ready DML restart has no token")?;
        let before_execution = context
            .handle()
            .query_lifecycle_structured_snapshot()?
            .and_then(|snapshot| snapshot.execution_id);
        context.action(format!(
            "armed token-scoped restart after BE[{target}] DML EstablishQueryContext; old_process_id={old_process_id}"
        ));

        let mysql_user = context.mysql_user().to_string();
        let mysql_port = context.mysql_port();
        let (sender, receiver) = mpsc::sync_channel(1);
        thread::spawn(move || {
            let result = (|| -> Result<()> {
                let mut connection =
                    mysql_actor::connect(&mysql_user, mysql_port, Duration::from_secs(30))?;
                connection
                    .query_drop("INSERT INTO pre_ready_dml.ns.orders VALUES (1, 10), (2, 20)")
                    .context("run DML during pre-ready replacement")
            })();
            let _ = sender.send(result);
        });

        wait_for_token_scoped_marker(context, target, ESTABLISH_CONTEXT_OBSERVED_MARKER, &token)?;
        let deadline = context.deadline();
        context
            .handle()
            .restart_be_until(target, deadline)
            .context("replace BE immediately after token-scoped DML EstablishQueryContext")?;
        let replacement_process_id = context.handle().backend_process_id(target)?;
        ensure!(
            replacement_process_id != old_process_id,
            "pre-ready DML replacement must create a new BackendProcessId"
        );
        context
            .handle()
            .clear_query_lifecycle_faults()
            .context("clear pre-ready DML restart trigger")?;
        receiver
            .recv_timeout(context.remaining("await re-planned DML")?)
            .map_err(|error| {
                anyhow::anyhow!("pre-ready DML did not return before deadline: {error}")
            })??;

        // The lifecycle debug surface reports the latest distributed statement.
        // Read the retried DML terminal before the verification SELECT creates
        // its own distributed execution record.
        let deadline = context.deadline();
        let terminal = context
            .handle()
            .await_query_lifecycle_structured_snapshot_after(before_execution.as_deref(), deadline)
            .context("read terminal snapshot for re-planned DML statement")?;
        ensure!(
            terminal.attempt_id == 2,
            "pre-ready DML replacement must complete as statement attempt 2, got attempt {}",
            terminal.attempt_id
        );

        let mut verify = mysql_actor::connect(
            context.mysql_user(),
            context.mysql_port(),
            context.remaining("connect DML verification client")?,
        )?;
        let rows: Vec<(i32, i64)> = verify
            .query("SELECT k1, v2 FROM pre_ready_dml.ns.orders ORDER BY k1")
            .context("read DML target after re-planned statement")?;
        ensure!(
            rows == vec![(1, 10), (2, 20)],
            "pre-ready DML retry must publish one result set, got {rows:?}"
        );
        context.action(format!(
            "replaced BE[{target}] after DML EstablishQueryContext and observed one Iceberg write result with successful statement attempt=2 completion; new_process_id={replacement_process_id}"
        ));
        Ok(())
    }
}

/// Waits until the armed rendezvous reports that this exact backend applied the
/// admission the fault parks on.
///
/// The token is part of the match rather than the marker name alone: the log is
/// preserved across restarts, so an earlier scenario's rendezvous line would
/// otherwise satisfy this wait and the harness would replace a process that had
/// admitted nothing.
fn wait_for_token_scoped_marker(
    context: &mut ScenarioContext,
    backend_index: usize,
    marker: &str,
    token: &str,
) -> Result<()> {
    let deadline = context.deadline();
    loop {
        let log = context.handle().be_log_contents(backend_index)?;
        if log
            .lines()
            .any(|line| line.contains(marker) && line.contains(&format!("token={token}")))
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!(
                "timed out waiting for token-scoped {marker} on BE[{backend_index}] token={token}; log_tail={:?}",
                log.lines().rev().take(20).collect::<Vec<_>>()
            );
        }
        thread::sleep(
            deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(25)),
        );
    }
}
