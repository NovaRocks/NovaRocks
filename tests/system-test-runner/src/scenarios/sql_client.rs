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

//! Shared-client session effects and exact pre-GET statement observation.
//! Literal DML counts come from this finite input, independently of the wire.

use super::native_sql_controls::{SqlIntentOwner, StatementSpec};
use crate::{
    actors::sql,
    scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig},
};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::{
    TestClusterOwner,
    resource_registration::{self, Registration},
    test_sessions::ClusterTestSessionPlan,
};
use novarocks_test_trino_client::{CancelHandle, StatementFailure, StatementResult};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Mutex,
    thread::JoinHandle,
    time::Instant,
};

const HELD_SQL: &str = "SELECT SLEEP(10)";

#[derive(Default)]
pub(super) struct SharedSqlClient {
    intent: SqlIntentOwner,
    warehouse: Mutex<Option<(PathBuf, Registration)>>,
}

impl Scenario for SharedSqlClient {
    fn name(&self) -> &'static str {
        "trino-protocol/shared-client-session-and-control"
    }

    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        super::declarations::for_module("sql_client", self.name(), false, false)
    }

    fn test_session_plan(
        &self,
        _: &ScenarioLaunchConfig,
        _: &Path,
        size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<ClusterTestSessionPlan>> {
        self.intent
            .freeze(
                (0..2)
                    .map(|_| StatementSpec {
                        sql: HELD_SQL.into(),
                        faults: vec![],
                        observations: vec![],
                    })
                    .collect(),
                &[],
                size,
                binary,
                deadline,
            )
            .map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "shared SQL client requires native 1FE+3BE"
        );
        let path = context.scenario_root().join("shared-client-warehouse");
        let registration = resource_registration::register(
            context.scenario_root(),
            "shared-sql-client",
            "warehouse",
            &format!("shared SQL client owned warehouse {}", path.display()),
        )?;
        *self
            .warehouse
            .lock()
            .map_err(|_| anyhow::anyhow!("warehouse owner poisoned"))? =
            Some((path.clone(), registration));
        fs::create_dir_all(&path)?;
        let mut client =
            sql::SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
        let phase = (|| -> Result<()> {
            ensure!(
                sql::single_integer(&client.execute("SELECT 1")?)? == 1,
                "initial query changed"
            );
            let warehouse = path.to_string_lossy().replace('"', "\\\"");
            let created = client.execute(format!("CREATE EXTERNAL CATALOG trc3_client PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"hadoop\",\"iceberg.catalog.warehouse\"=\"{warehouse}\")"))?;
            ensure!(
                created.columns.is_empty() && created.update_count.is_none(),
                "CREATE CATALOG has no count or columns"
            );
            client.execute("CREATE DATABASE trc3_client.ns")?;
            client.execute("SET query_timeout = 30")?;
            client.execute("USE trc3_client.ns")?;
            ensure!(
                client.session().catalog.as_deref() == Some("trc3_client")
                    && client.session().schema.as_deref() == Some("ns")
                    && client
                        .session()
                        .properties
                        .get("query_timeout")
                        .map(String::as_str)
                        == Some("30"),
                "successful session effects were not applied: {:?}",
                client.session()
            );
            let created = client.execute(
                "CREATE TABLE items (id INT, v INT) TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")",
            )?;
            ensure!(
                created.columns.is_empty() && created.update_count.is_none(),
                "CREATE TABLE has no count or columns"
            );
            for (statement, expected) in [
                ("INSERT INTO items VALUES (1, 10), (2, 20)", 2),
                ("UPDATE items SET v = v + 1 WHERE id = 1", 1),
                ("DELETE FROM items WHERE id = 2", 1),
                (
                    "MERGE INTO items t USING (SELECT 1 AS id, 100 AS v UNION ALL SELECT 3, 300) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)",
                    2,
                ),
                ("UPDATE items SET v = 0 WHERE id = 99", 0),
            ] {
                sql::expect_update_count(&client.execute(statement)?, expected)?;
            }
            ensure!(
                sql::integer_column(&client.execute("SELECT id FROM items ORDER BY id")?)?
                    == vec![1, 3],
                "DML changed the independently expected row set"
            );
            refused_arm_cancels_unstarted(context, &self.intent, &client)?;
            exact_cancel(context, &self.intent, &mut client)?;
            context.action("shared typed client preserved session effects, logical command counts, exact FE ownership and query-scoped cancellation");
            Ok(())
        })();
        let cleanup = client
            .execute("DROP CATALOG IF EXISTS trc3_client")
            .map(|_| ())
            .map_err(Into::into);
        combine(phase, cleanup)
    }

    fn teardown(&self) -> Result<()> {
        if let Some((path, mut registration)) = self
            .warehouse
            .lock()
            .map_err(|_| anyhow::anyhow!("warehouse owner poisoned"))?
            .take()
        {
            let removed = match fs::remove_dir_all(path) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error.into()),
            };
            let recorded = registration.settle(&removed);
            combine(removed, recorded)?;
        }
        Ok(())
    }
}

struct QueryActor {
    cancel: CancelHandle,
    deadline: Instant,
    join: Option<JoinHandle<std::result::Result<StatementResult, StatementFailure>>>,
}
impl QueryActor {
    fn join(&mut self) -> Result<std::result::Result<StatementResult, StatementFailure>> {
        self.join
            .take()
            .context("query actor was already joined")?
            .join()
            .map_err(|_| anyhow::anyhow!("shared SQL actor panicked"))
    }
    fn close(&mut self) -> Result<()> {
        if self.join.is_none() {
            return Ok(());
        }
        let cancel = self.cancel.delete(self.deadline).map_err(Into::into);
        let joined = self.join().and_then(|outcome| match outcome {
            Ok(_) => Ok(()),
            Err(error) if sql::expect_error(&error, 3, "USER_CANCELED").is_ok() => Ok(()),
            Err(error) => {
                Err(anyhow::Error::new(error).context("original SQL result during actor cleanup"))
            }
        });
        combine(cancel, joined)
    }
}
impl Drop for QueryActor {
    fn drop(&mut self) {
        if let Err(error) = self.close() {
            eprintln!("shared SQL actor cleanup failed: {error:#}");
        }
    }
}

fn exact_cancel(
    context: &mut ScenarioContext,
    intent: &SqlIntentOwner,
    client: &mut sql::SqlSession,
) -> Result<()> {
    let selected = intent.take(1)?;
    let pending = client.submit(HELD_SQL)?;
    let (pending, observation) = selected.arm_pending(context, pending, HELD_SQL)?;
    let mut owned_session = client.clone();
    let mut actor = QueryActor {
        cancel: pending.cancel_handle(),
        deadline: context.deadline(),
        join: Some(std::thread::spawn(move || {
            owned_session.run_pending(pending)
        })),
    };
    let deadline = context.deadline();
    let mut independent_cancel = None;
    let phase = (|| -> Result<()> {
        context
            .handle()
            .await_query_lifecycle_statement_owned(&observation, deadline)?;
        // Register a distinct peer before DELETE and retain its continuation
        // until the selected actor exits. It must remain executable.
        let independent = client.submit("SELECT 7")?;
        independent_cancel = Some(independent.cancel_handle());
        actor.cancel.delete(deadline)?;
        let error = match actor.join()? {
            Err(error) => error,
            Ok(result) => anyhow::bail!("selected SLEEP completed before cancellation: {result:?}"),
        };
        sql::expect_error(&error, 3, "USER_CANCELED")?;
        ensure!(
            sql::single_integer(&client.run_pending(independent)?)? == 7,
            "cancellation escaped to a distinct registered query"
        );
        ensure!(
            sql::single_integer(&client.execute("SELECT 8")?)? == 8,
            "cancellation escaped its original query"
        );
        Ok(())
    })();
    let closed = actor.close();
    let peer_closed = independent_cancel
        .map(|cancel| cancel.delete(deadline))
        .transpose()
        .map(|_| ())
        .map_err(Into::into);
    let drained = context
        .handle()
        .drain_query_lifecycle_statement_binding(&observation, deadline);
    combine(combine(combine(phase, closed), peer_closed), drained)
}

fn refused_arm_cancels_unstarted(
    context: &mut ScenarioContext,
    intent: &SqlIntentOwner,
    client: &sql::SqlSession,
) -> Result<()> {
    let selected = intent.take(0)?;
    let pending = client.submit(HELD_SQL)?;
    let next = pending
        .initial_next_uri
        .clone()
        .context("registered query omitted continuation")?;
    let refusal = match selected.arm_pending(context, pending, "SELECT 0") {
        Err(error) => error,
        Ok((pending, observation)) => {
            let cancelled = pending
                .cancel_handle()
                .delete(context.deadline())
                .map_err(Into::into);
            let drained = context
                .handle()
                .drain_query_lifecycle_statement_observation(&observation);
            return combine(
                Err(anyhow::anyhow!(
                    "arm accepted SQL outside its original finite intent"
                )),
                combine(cancelled, drained),
            );
        }
    };
    ensure!(
        format!("{refusal:#}").contains("SQL differs from its frozen Native submission"),
        "unexpected arm refusal: {refusal:#}"
    );
    let oracle = crate::actors::trino_protocol::TrinoClient::new(
        context.trino_port(),
        context.sql_user(),
        context.deadline(),
        &context.scenario_root().join("arm-refusal-response"),
    )?;
    let response = oracle.get(&next)?;
    ensure!(
        response.status == 200,
        "arm refusal query returned HTTP {}",
        response.status
    );
    let page = response.json()?;
    ensure!(
        page["error"]["errorCode"] == 3
            && page["error"]["errorName"] == "USER_CANCELED"
            && page.get("nextUri").is_none()
            && page.get("data").is_none(),
        "rejected arm left its original query executable: {page}"
    );
    Ok(())
}

fn combine(primary: Result<()>, cleanup: Result<()>) -> Result<()> {
    match (primary, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(primary), Err(cleanup)) => {
            anyhow::bail!("{primary:#}; shared SQL cleanup: {cleanup:#}")
        }
        (Err(error), _) | (_, Err(error)) => Err(error),
    }
}
