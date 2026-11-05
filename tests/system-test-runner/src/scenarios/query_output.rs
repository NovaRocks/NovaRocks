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

//! Public typed layout, strict single-statement admission and explicit effects.
//! MySQL packet sequencing and negotiated batches have no Trino carrier.

use crate::actors::sql::{self, SqlSession};
use crate::actors::trino_protocol::TrinoClient;
use crate::scenario::{Scenario, ScenarioContext};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::TestClusterOwner;

const TWO_ROW_QUERY: &str = "SELECT v FROM (SELECT 1 AS v UNION ALL SELECT 2) t ORDER BY v";

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![Box::new(TypedLayout), Box::new(SingleStatementEffects)]
}

struct TypedLayout;
struct SingleStatementEffects;

impl Scenario for TypedLayout {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module("query_output", self.name(), false, false)
    }
    fn name(&self) -> &'static str {
        "query-output/typed-layout"
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = context
            .handle()
            .query_execution_resource_snapshot()?
            .context("native query-output resource baseline missing")?;
        let mut session =
            SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
        let result = session.execute(TWO_ROW_QUERY)?;
        ensure!(
            result.is_result_set() && result.update_type.is_none() && result.update_count.is_none(),
            "ordinary query changed its result shape: {result:?}"
        );
        ensure!(
            result.columns.len() == 1
                && result.columns[0].name == "v"
                && result.columns[0].type_signature.raw_type == "bigint"
                && result.columns[0].type_signature.arguments.is_empty(),
            "original two-row query changed its column layout: {result:?}"
        );
        ensure!(
            sql::integer_column(&result)? == [1, 2],
            "original ordered two-row query changed its rows: {result:?}"
        );
        context.action("verified original one-column ordered two-row typed result");
        let deadline = context.deadline();
        context
            .handle()
            .await_query_execution_resource_convergence(&baseline, deadline)?;
        Ok(())
    }
}

impl Scenario for SingleStatementEffects {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module("query_output", self.name(), false, false)
    }
    fn name(&self) -> &'static str {
        "query-output/single-statement-and-effects"
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = context
            .handle()
            .query_execution_resource_snapshot()?
            .context("native query-output resource baseline missing")?;
        let mut session =
            SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
        let completion = session.execute("SET query_timeout = 61")?;
        ensure!(
            completion.is_completion()
                && completion.columns.is_empty()
                && completion.rows.is_empty()
                && completion.update_count.is_none(),
            "session effect changed its empty command shape: {completion:?}"
        );
        ensure!(
            session
                .session()
                .properties
                .get("query_timeout")
                .map(String::as_str)
                == Some("61"),
            "successful session effect was not applied explicitly"
        );
        let before = session.session().clone();
        let raw = TrinoClient::new(
            context.trino_port(),
            context.sql_user(),
            context.deadline(),
            &context.scenario_root().join("batch-http"),
        )?
        .with_session(&[("x-trino-session", "query_timeout=61")]);
        let pages = raw.execute("SET query_timeout = 60; SELECT 1")?;
        let first = pages
            .first()
            .context("batch admission omitted its original response")?
            .json()?;
        let id = first["id"]
            .as_str()
            .context("batch admission omitted query id")?;
        for page in &pages {
            let value = page.json()?;
            ensure!(
                page.status == 200 && value["id"] == id,
                "batch admission changed its original query identity/status"
            );
            for field in ["columns", "data", "updateType", "updateCount"] {
                ensure!(
                    value.get(field).is_none(),
                    "rejected batch published a prefix success field {field}"
                );
            }
            ensure!(
                !page.headers.contains_key("x-trino-set-session")
                    && !page.headers.contains_key("x-trino-clear-session"),
                "rejected batch emitted prefix SessionEffects"
            );
        }
        let terminal = pages.last().unwrap();
        let value = terminal.json()?;
        ensure!(
            terminal.next_uri()?.is_none()
                && value["stats"]["state"] == "FAILED"
                && value["error"]["errorCode"] == 1
                && value["error"]["errorName"] == "SYNTAX_ERROR"
                && value["error"]["errorType"] == "USER_ERROR",
            "single POST did not reach its typed multi-statement refusal"
        );
        ensure!(
            session.session().properties == before.properties
                && session.session().catalog == before.catalog
                && session.session().schema == before.schema
                && session.session().time_zone == before.time_zone,
            "rejected batch applied a prefix SessionEffect"
        );
        ensure!(
            sql::single_integer(&session.execute("SELECT 1")?)? == 1,
            "query after rejected batch did not retain ordinary progress"
        );
        ensure!(
            session
                .session()
                .properties
                .get("query_timeout")
                .map(String::as_str)
                == Some("61"),
            "original explicit SessionEffect changed after ordinary follow-up query"
        );
        context.action("verified single-POST batch refusal without prefix effects and successful explicit SessionEffects");
        let deadline = context.deadline();
        context
            .handle()
            .await_query_execution_resource_convergence(&baseline, deadline)?;
        Ok(())
    }
}

fn require_three_backends(context: &mut ScenarioContext) -> Result<()> {
    let actual = context.handle().be_count();
    ensure!(
        actual == 3,
        "{} requires native 1FE+3BE, got 1FE+{actual}BE",
        context.name()
    );
    Ok(())
}
