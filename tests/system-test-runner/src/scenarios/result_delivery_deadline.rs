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

//! Original query cancellation and actual owner exit remain distinct facts.

use super::result_delivery::root_census;
use super::result_delivery_concurrency::retain_failure;
use super::result_delivery_support::await_idle;
use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext};
use anyhow::{Context, Result, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::TestClusterOwner;
use reqwest::blocking::Client;
use serde_json::{Value, json};
use std::time::{Duration, Instant};

const PRODUCER_SQL: &str = "SELECT SLEEP(35) AS value";

#[derive(Clone, Copy)]
enum Mode {
    ProducerWait,
    QueryDeadline,
}

struct ResponseDeadline(Mode);

pub(super) fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(ResponseDeadline(Mode::ProducerWait)),
        Box::new(ResponseDeadline(Mode::QueryDeadline)),
    ]
}

impl Scenario for ResponseDeadline {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "result_delivery_deadline",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        match self.0 {
            Mode::ProducerWait => "result-delivery/producer-wait-query-deadline-separation",
            Mode::QueryDeadline => "result-delivery/original-query-deadline-cancels-producer-wait",
        }
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "response deadlines require native 1FE+3BE"
        );
        let epoch = Instant::now();
        await_idle(context, "response-deadline", "before", epoch)?;
        await_actual_exit(context, "before", epoch)?;
        let mut connection = mysql_actor::connect(
            context.mysql_user(),
            context.mysql_port(),
            context.remaining("connect response deadline client")?,
        )?;
        let connection_id = connection.connection_id();
        let timeout_seconds = match self.0 {
            Mode::ProducerWait => 120,
            Mode::QueryDeadline => 2,
        };
        let mut evidence = json!({
            "schema_version": 1,
            "scenario": self.name(),
            "connection_id": connection_id,
            "sql": PRODUCER_SQL,
            "query_timeout_seconds": timeout_seconds,
            "expected_success_rows": [1],
            "expected_query_deadline_error": {"code": 1317, "sqlstate": "70100", "public_error_code": "query.execution.deadline_exceeded"},
            "same_socket_reentry": false,
        });
        let mut cleanup = Vec::new();
        let mut causal = (|| -> Result<()> {
            connection.query_drop(format!("SET query_timeout = {timeout_seconds}"))?;
            context.action(format!("connection {connection_id} set query_timeout={timeout_seconds} through normal MySQL"));
            context.remaining("send producer wait query")?;
            evidence["query_sent_epoch_micros"] = json!(epoch.elapsed().as_micros());
            let response: mysql::Result<Vec<mysql::Row>> = connection.query(PRODUCER_SQL);
            evidence["response_epoch_micros"] = json!(epoch.elapsed().as_micros());
            if let Err(mysql::Error::MySqlError(error)) = &response {
                evidence["actual_server_error"] = json!({"code":error.code,"sqlstate":error.state,"message":error.message});
            }
            match (self.0, response) {
                (Mode::ProducerWait, Ok(rows)) => {
                    let rows = scalar_values(rows)?;
                    evidence["actual_rows"] = json!(rows);
                    ensure!(rows == [1], "SLEEP(35) differs from the independent Boolean true oracle");
                    context.action("SLEEP(35) completed with Boolean true under the original 120-second query deadline");
                }
                (Mode::QueryDeadline, Err(mysql::Error::MySqlError(error))) => {
                    evidence["actual_server_error"] = json!({"code":error.code,"sqlstate":error.state,"message":error.message});
                    ensure!(error.code == 1317, "query deadline returned a different MySQL error code");
                    // KILL QUERY shares errno 1317. Require the public typed
                    // deadline identity instead of an obsolete display string.
                    ensure!(error.state == "70100"
                        && error.message.starts_with("[query.execution.deadline_exceeded] "),
                        "ERR 1317 was not an original query-deadline projection: {}", error.message);
                    context.action("received native ERR 1317 with an original typed query-deadline projection; owner exit is not inferred from this response");
                }
                (_, Err(error)) => {
                    evidence["actual_failure"] = json!(error.to_string());
                    return Err(error.into());
                }
                (Mode::QueryDeadline, Ok(rows)) => {
                    evidence["unexpected_rows"] = json!(format!("{rows:?}"));
                    anyhow::bail!("the original two-second query deadline did not cancel SLEEP(35)");
                }
            }
            context.remaining("same socket reentry")?;
            let rows: Vec<mysql::Row> = connection.query("SELECT 1")?;
            let rows = scalar_values(rows)?;
            evidence["reentry_rows"] = json!(rows);
            evidence["reentry_epoch_micros"] = json!(epoch.elapsed().as_micros());
            ensure!(rows == [1], "same socket SELECT 1 differs from its independent oracle");
            ensure!(connection.connection_id() == connection_id, "reentry changed the MySQL connection identity");
            evidence["same_socket_reentry"] = json!(true);
            context.action("the original MySQL socket accepted SELECT 1 after the response");
            Ok(())
        })().err();
        if causal.is_some() {
            let deadline = context.deadline();
            if let Err(error) = context.handle().kill_query_until(connection_id, deadline) {
                cleanup.push(format!("cancel failed deadline actor: {error:#}"));
            }
        }
        // This actor has no detached thread or pending borrowed read future.
        // Closing the client is followed by physical observations, not a claim
        // that ERR, socket drop, or cancellation itself released the workers.
        drop(connection);
        evidence["client_dropped_epoch_micros"] = json!(epoch.elapsed().as_micros());
        if let Err(error) = await_actual_exit(context, "after-client", epoch) {
            retain_failure(&mut causal, &mut cleanup, error);
        }
        if let Err(error) = await_idle(context, "response-deadline", "after-client", epoch) {
            retain_failure(&mut causal, &mut cleanup, error);
        }
        evidence["causal_error"] = json!(causal.as_ref().map(|error| format!("{error:#}")));
        evidence["cleanup_errors"] = json!(cleanup);
        std::fs::write(
            context.scenario_root().join("response-deadline.json"),
            serde_json::to_vec_pretty(&evidence)?,
        )?;
        if let Some(error) = causal {
            return Err(error.context(format!("deadline cleanup errors: {cleanup:?}")));
        }
        ensure!(cleanup.is_empty(), "deadline cleanup failed: {cleanup:?}");
        context.action("separate public root/worker observations and consecutive owner barriers confirmed actual drain; normal runner still owns process shutdown");
        Ok(())
    }
}

fn scalar_values(rows: Vec<mysql::Row>) -> Result<Vec<u8>> {
    rows.into_iter()
        .map(|row| mysql::from_row_opt::<(u8,)>(row).map(|(value,)| value))
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("decode scalar text result without panicking on a mismatched row")
}

fn await_actual_exit(context: &mut ScenarioContext, phase: &str, epoch: Instant) -> Result<()> {
    let client = Client::builder().timeout(Duration::from_secs(1)).build()?;
    let mut samples = Vec::new();
    let outcome = (|| -> Result<()> {
        let mut consecutive = 0;
        loop {
            context.remaining("actual response owner exit")?;
            let resource = context
                .handle()
                .query_execution_resource_snapshot()?
                .context("native query execution resource observation is unavailable")?;
            ensure!(
                resource.fe_running && resource.backends.len() == 3,
                "native role processes changed during owner exit observation"
            );
            let mut idle = true;
            let mut backends = Vec::new();
            let ports = context
                .handle()
                .runtime()
                .be
                .iter()
                .map(|be| be.http)
                .collect::<Vec<_>>();
            for (backend, port) in resource.backends.iter().zip(ports) {
                ensure!(
                    backend.process_running && !backend.resources.is_empty(),
                    "BE query owner observation is absent or its process exited"
                );
                for count in backend.resources.values() {
                    ensure!(
                        count.is_finite() && *count >= 0.0 && count.fract() == 0.0,
                        "invalid actual BE owner count"
                    );
                    idle &= *count == 0.0;
                }
                let rows: Value = client
                    .get(format!("http://127.0.0.1:{port}/metrics?type=json"))
                    .send()?
                    .error_for_status()?
                    .json()?;
                let roots = root_census(
                    rows.as_array()
                        .context("invalid root census metric array")?,
                )?;
                // A busy or incomplete census is explicitly unavailable.
                idle &= roots
                    .as_ref()
                    .is_some_and(|counts| counts.values().all(|count| *count == 0));
                backends.push(json!({"index":backend.index,"query_resources":backend.resources,"root_ownership":roots}));
            }
            samples.push(json!({"query_epoch_micros":epoch.elapsed().as_micros(),"idle":idle,"backends":backends}));
            consecutive = if idle { consecutive + 1 } else { 0 };
            if consecutive == 2 {
                return Ok(());
            }
            ensure!(
                samples.len() < 3001,
                "owner exit observation exceeded the original finite sample guard"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    })();
    std::fs::write(
        context
            .scenario_root()
            .join(format!("response-deadline-owner-exit-{phase}.json")),
        serde_json::to_vec_pretty(&json!({"schema_version":1,"phase":phase,"samples":samples,
            "error":outcome.as_ref().err().map(|error| format!("{error:#}"))}))?,
    )?;
    outcome
}
