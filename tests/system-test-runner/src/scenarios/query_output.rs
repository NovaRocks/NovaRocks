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

use super::connector::{create_catalog, create_warehouse};
use super::connector_control_plan::with_cleanup;
use crate::actors::sql::{self, SqlSession, TrinoValue};
use crate::actors::trino_protocol::{RawResponse, TrinoClient};
use crate::scenario::{Scenario, ScenarioContext};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::TestClusterOwner;
use novarocks_result_contract::RootProfileV1;
use serde_json::{Value, json};

const TWO_ROW_QUERY: &str =
    "SELECT v FROM (SELECT CAST(1 AS BIGINT) AS v UNION ALL SELECT CAST(2 AS BIGINT)) t ORDER BY v";

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
        let raw = TrinoClient::new(
            context.trino_port(),
            context.sql_user(),
            context.deadline(),
            &context.scenario_root().join("row-map-http"),
        )?;
        let pages = raw.execute(
            r#"SELECT ROW(7 AS "id", CAST(NULL AS INTEGER), 9) AS payload,
                MAP(ARRAY[MAP(ARRAY[2, 1], ARRAY['a"b', 'x=y,{z}'])], ARRAY[7]) AS keyed"#,
        )?;
        let (columns, rows) = read_typed_pages(&pages, "ROW/MAP")?;
        ensure!(
            columns[0]["type"] == "row(id integer, integer, integer)"
                && columns[0]["typeSignature"]["rawType"] == "row"
                && columns[0]["typeSignature"]["arguments"][0]["value"]["fieldName"]["name"]
                    == "id"
                && columns[0]["typeSignature"]["arguments"][1]["kind"] == "NAMED_TYPE"
                && columns[0]["typeSignature"]["arguments"][1]["value"].get("fieldName")
                    == Some(&serde_json::Value::Null)
                && columns[1]["typeSignature"]["rawType"] == "map",
            "ROW field declarations changed: {columns}"
        );
        ensure!(
            rows == [json!([[7, null, 9], {"{2=a\"b, 1=x=y,{z}}": 7}])],
            "ROW/MAP result changed its field positions or key text: {rows:?}"
        );
        context.action("verified anonymous ROW declarations and MAP key text on raw Trino pages");
        native_json_fixture(context, &mut session)?;
        let deadline = context.deadline();
        context
            .handle()
            .await_query_execution_resource_convergence(&baseline, deadline)?;
        Ok(())
    }
}

fn read_typed_pages(pages: &[RawResponse], label: &str) -> Result<(Value, Vec<Value>)> {
    let mut columns = None;
    let mut rows = Vec::new();
    for page in pages {
        let value = page.json()?;
        ensure!(
            page.status == 200 && value.get("error").is_none(),
            "{label} result page failed (HTTP {}): {:?}",
            page.status,
            value.get("error")
        );
        if let Some(page_columns) = value.get("columns") {
            if let Some(previous) = &columns {
                ensure!(
                    previous == page_columns,
                    "{label} metadata changed between pages"
                );
            } else {
                columns = Some(page_columns.clone());
            }
        }
        if let Some(data) = value.get("data").and_then(Value::as_array) {
            rows.extend(data.iter().cloned());
        }
    }
    let terminal = pages
        .last()
        .with_context(|| format!("{label} omitted its terminal page"))?;
    ensure!(
        terminal.next_uri()?.is_none() && terminal.json()?["stats"]["state"] == "FINISHED",
        "{label} did not reach its original terminal page"
    );
    Ok((
        columns.with_context(|| format!("{label} omitted column metadata"))?,
        rows,
    ))
}

fn native_json_fixture(context: &mut ScenarioContext, session: &mut SqlSession) -> Result<()> {
    const CATALOG: &str = "native_json_output";
    let schema = format!("{CATALOG}.fixture");
    let table = format!("{schema}.numbers");
    let warehouse = create_warehouse(context, "native-json-output")?;
    let result = (|| -> Result<()> {
        create_catalog(session, CATALOG, &warehouse)?;
        session.execute(format!("CREATE SCHEMA {schema}"))?;
        session.execute(format!("CREATE TABLE {table} (n BIGINT NOT NULL)"))?;
        // Each commit supplies an independent scan split to the live BE roster.
        for n in 1..=9 {
            sql::expect_update_count(
                &session.execute(format!("INSERT INTO {table} VALUES (BIGINT '{n}')"))?,
                1,
            )?;
        }
        let counts = sql::integer_column(&session.execute(format!(
            "SELECT record_count FROM {schema}.\"numbers$files\""
        ))?)?;
        ensure!(
            counts == [1; 9],
            "expected nine single-row JSON source files"
        );
        native_json_pages(context, session, &table)
    })();
    let cleanup = session
        .execute(format!("DROP CATALOG IF EXISTS {CATALOG}"))
        .map(|_| ())
        .context("drop the native JSON fixture catalog");
    with_cleanup(result, cleanup)
}

fn native_json_pages(
    context: &mut ScenarioContext,
    session: &mut SqlSession,
    table: &str,
) -> Result<()> {
    // The document itself makes the row exceed a Native root segment.
    let wide = RootProfileV1::SEGMENT_BYTES;
    let sql = format!(
        r#"WITH source AS (
               SELECT n, n % 3 AS bucket,
                      CASE n
                          WHEN 1 THEN CAST(NULL AS JSON)
                          WHEN 2 THEN JSON 'null'
                          WHEN 3 THEN json_parse(concat(rpad('', 1024, '['), CAST(n AS VARCHAR), rpad('', 1024, ']')))
                          WHEN 8 THEN CAST(rpad(CAST(n AS VARCHAR), {wide}, 'q') AS JSON)
                          ELSE json_parse(concat('{{"b":', CAST(n AS VARCHAR), ',"a":1,"a":2}}'))
                      END AS doc
               FROM {table}
           ), grouped AS (
               SELECT bucket, array_agg(doc ORDER BY n) AS documents
               FROM source GROUP BY bucket
           )
           SELECT bucket, documents[1] AS direct, documents AS nested
           FROM grouped ORDER BY bucket"#
    );
    let root = context.scenario_root().join("native-json-http");
    let client = TrinoClient::new(
        context.trino_port(),
        context.sql_user(),
        context.deadline(),
        &root,
    )?;
    let plan = session.execute(format!(
        "EXPLAIN WITH SESSION explain_detail = 'CONTRACT' {sql}"
    ))?;
    let mut plan_text = String::new();
    for row in &plan.rows {
        let [TrinoValue::Text(line)] = row.as_slice() else {
            anyhow::bail!("unexpected native JSON EXPLAIN row: {row:?}");
        };
        plan_text.push_str(line);
        plan_text.push('\n');
    }
    std::fs::write(root.join("source-plan.txt"), &plan_text)?;
    ensure!(
        plan_text.contains("EDGE GRAPH"),
        "native JSON source plan has no Exchange"
    );
    let before = native_json_exchange_metrics(context, &root, "before")?;
    let pages = client.execute(&sql)?;
    let after = native_json_exchange_metrics(context, &root, "after")?;
    let (columns, rows) = read_typed_pages(&pages, "native JSON")?;
    ensure!(
        columns[1]["type"] == "json"
            && columns[1]["typeSignature"] == json!({"rawType":"json", "arguments":[]})
            && columns[2]["type"] == "array(json)"
            && columns[2]["typeSignature"]["arguments"][0]
                == json!({"kind":"TYPE", "value":{"rawType":"json", "arguments":[]}}),
        "unexpected native JSON column declarations: {columns}"
    );
    let object = |n| format!(r#"{{"b":{n},"a":1,"a":2}}"#);
    let depth = format!("{}3{}", "[".repeat(1024), "]".repeat(1024));
    let large_document = format!("\"8{}\"", "q".repeat(wide - 1));
    let expected = [
        json!([0, depth, [depth, object(6), object(9)]]),
        json!([1, null, [null, object(4), object(7)]]),
        json!([2, "null", ["null", object(5), large_document]]),
    ];
    ensure!(
        rows == expected,
        "native JSON values differ across direct and aggregate array fields (expected three ordered buckets)"
    );
    let senders = before
        .iter()
        .zip(&after)
        .filter(|(before, after)| after.0 > before.0 && after.1 > before.1)
        .count();
    ensure!(
        senders >= 2,
        "native JSON query lacks observed distributed Task and exchange receipts: {before:?} -> {after:?}"
    );
    context.action("verified native JSON text, SQL NULL and JSON null through actual distributed Tasks and exchange");
    Ok(())
}

fn native_json_exchange_metrics(
    context: &mut ScenarioContext,
    root: &std::path::Path,
    phase: &str,
) -> Result<Vec<(f64, u64)>> {
    let handle = context.handle();
    (0..handle.be_count())
        .map(|index| {
            let text = handle.backend_prometheus_text(index)?;
            std::fs::write(root.join(format!("be-{index}-{phase}.prom")), &text)?;
            let mut samples = text
                .lines()
                .filter_map(|line| line.strip_prefix("novarocks_exchange_shuffle_bytes_total "));
            let bytes = samples
                .next()
                .context("BE exchange bytes metric missing")?
                .parse()?;
            ensure!(
                samples.next().is_none(),
                "duplicate BE exchange bytes metric"
            );
            let tasks = handle.backend_task_execution_tasks_created(index)?;
            Ok((tasks, bytes))
        })
        .collect()
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
        let completion = session.execute("SET SESSION query_timeout = 61")?;
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
        let pages = raw.execute("SET SESSION query_timeout = 60; SELECT 1")?;
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
