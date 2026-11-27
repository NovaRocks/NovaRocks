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

use super::connector::{create_catalog, create_warehouse, with_cleanup};
use super::native_sql_controls::{SelectedStatement, SqlIntentOwner, StatementSpec};
use crate::actors::sql::{self, SqlSession, TrinoValue};
use crate::actors::trino_protocol::{RawResponse, TrinoClient};
use crate::scenario::{Scenario, ScenarioContext};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::{LaunchProfile, TestClusterOwner};
use novarocks_failpoint::observation_marker::{
    EXCHANGE_GRANT_ONLY_PROBE, EXCHANGE_SNAPSHOT, ObservationAction, TASK_CONSTANT_IPC_RECORD,
    TASK_TERMINAL_RETAINED, TASK_VALUES_CAST_SOURCE, TASK_VALUES_SOURCE,
};
use novarocks_result_contract::RootProfileV1;
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
    time::Instant,
};

const TWO_ROW_QUERY: &str =
    "SELECT v FROM (SELECT CAST(1 AS BIGINT) AS v UNION ALL SELECT CAST(2 AS BIGINT)) t ORDER BY v";

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(TypedLayout),
        Box::new(SingleStatementEffects),
        Box::new(DeepWideTypes::default()),
    ]
}

struct TypedLayout;
struct SingleStatementEffects;
#[derive(Default)]
struct DeepWideTypes {
    observe: AtomicBool,
    intent: SqlIntentOwner,
}

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
    with_number_files(
        context,
        session,
        "native_json_output",
        "native-json-output",
        native_json_pages,
    )
}

fn with_number_files(
    context: &mut ScenarioContext,
    session: &mut SqlSession,
    catalog: &str,
    fixture: &str,
    run: impl FnOnce(&mut ScenarioContext, &mut SqlSession, &str) -> Result<()>,
) -> Result<()> {
    let schema = format!("{catalog}.fixture");
    let table = format!("{schema}.numbers");
    let warehouse = create_warehouse(context, fixture)?;
    let result = (|| -> Result<()> {
        create_catalog(session, catalog, &warehouse)?;
        session.execute(format!("CREATE SCHEMA {schema}"))?;
        session.execute(format!("CREATE TABLE {table} (n BIGINT NOT NULL)"))?;
        // Independent files give the original scheduler distinct scan splits.
        for n in 1..=9 {
            sql::expect_update_count(
                &session.execute(format!("INSERT INTO {table} VALUES (BIGINT '{n}')"))?,
                1,
            )?;
        }
        let counts = sql::integer_column(&session.execute(format!(
            "SELECT record_count FROM {schema}.\"numbers$files\""
        ))?)?;
        ensure!(counts == [1; 9], "expected nine independent source files");
        run(context, session, &table)
    })();
    let cleanup = session
        .execute(format!("DROP CATALOG IF EXISTS {catalog}"))
        .map(|_| ())
        .context("drop the original fixture catalog");
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

impl Scenario for DeepWideTypes {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module("query_output", self.name(), true, false)
    }

    fn name(&self) -> &'static str {
        "query-output/deep-wide-types"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn validate_runner_inputs(&self, profile: LaunchProfile) -> Result<()> {
        ensure!(
            matches!(
                profile,
                LaunchProfile::FaultScenario | LaunchProfile::Performance
            ),
            "deep types require the fault-scenario or performance launch profile"
        );
        self.observe
            .store(profile == LaunchProfile::FaultScenario, Ordering::Relaxed);
        Ok(())
    }

    fn test_session_plan(
        &self,
        _: &crate::scenario::ScenarioLaunchConfig,
        _: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        if !self.observe.load(Ordering::Relaxed) {
            return Ok(None);
        }
        let observations = (0..cluster_size)
            .flat_map(|index| {
                [
                    ObservationAction::TaskProtocol,
                    ObservationAction::ExchangeSnapshot,
                ]
                .map(|action| (index, action.as_str(), None))
            })
            .collect();
        Ok(Some(self.intent.freeze(
            vec![StatementSpec {
                sql: deep_wide_query("deep_wide_output.fixture.numbers", true),
                faults: vec![],
                observations,
            }],
            &[],
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = context
            .handle()
            .query_execution_resource_snapshot()?
            .context("native deep-type resource baseline missing")?;
        let mut session =
            SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
        let selected = if context.launch_profile() == LaunchProfile::FaultScenario {
            Some(self.intent.take(0)?)
        } else {
            None
        };
        let result = with_number_files(
            context,
            &mut session,
            "deep_wide_output",
            "deep-wide-output",
            |context, session, table| deep_wide_pages(context, session, table, selected.as_ref()),
        );
        let result = result.and_then(|()| verify_deep_type_refusal(context));
        let deadline = context.deadline();
        let convergence = context
            .handle()
            .await_query_execution_resource_convergence(&baseline, deadline);
        with_cleanup(result, convergence)
    }
}

const TYPE_DEPTH: usize = 64;
const ROW_WIDTH: usize = 64;

fn deep_wide_query(table: &str, observe: bool) -> String {
    let deep = |leaf: &str| {
        format!(
            "{}{leaf}{}",
            "[".repeat(TYPE_DEPTH - 1),
            "]".repeat(TYPE_DEPTH - 1)
        )
    };
    let wide = |leaf: i64| {
        let mut inner = vec!["0".to_owned(); ROW_WIDTH];
        inner[ROW_WIDTH - 1] = leaf.to_string();
        let inner = format!("ROW({})", inner.join(","));
        let mut outer = vec![format!("ROW({})", vec!["0"; ROW_WIDTH].join(",")); ROW_WIDTH];
        outer[ROW_WIDTH - 1] = inner;
        format!("ROW({})", outer.join(","))
    };
    let even = deep("BIGINT '7'");
    let odd = deep("BIGINT '19'");
    let null = deep("CAST(NULL AS BIGINT)");
    let leaf = "[1]".repeat(TYPE_DEPTH - 1);
    let even_wide = wide(41);
    let odd_wide = wide(73);
    if observe {
        // The VALUES side supplies the complete constant inputs. The scan side
        // allows the joined values to cross the same distributed TopN cut.
        return format!(
            "SELECT n, deep{leaf} AS leaf, wide[{ROW_WIDTH}][{ROW_WIDTH}] AS corner \
             FROM (SELECT n, deep, wide FROM {table} JOIN \
                   (VALUES (BIGINT '0',{even},{even_wide}), (BIGINT '1',{odd},{odd_wide}), \
                           (BIGINT '9',{null},{odd_wide})) original(k,deep,wide) \
                   ON (CASE WHEN n=9 THEN 9 ELSE n%2 END)=k ORDER BY n LIMIT 9) t ORDER BY n"
        );
    }
    format!(
        "SELECT n, deep{leaf} AS leaf, wide[{ROW_WIDTH}][{ROW_WIDTH}] AS corner \
         FROM (SELECT n, CASE WHEN n=9 THEN {null} WHEN n%2=0 THEN {even} ELSE {odd} END AS deep, \
               IF(n%2=0,{even_wide},{odd_wide}) AS wide FROM {table} ORDER BY n LIMIT 9) t \
         ORDER BY n"
    )
}

fn deep_wide_pages(
    context: &mut ScenarioContext,
    session: &mut SqlSession,
    table: &str,
    selected: Option<&SelectedStatement>,
) -> Result<()> {
    let query = deep_wide_query(table, selected.is_some());
    let root = context.scenario_root().join("deep-wide-http");
    let client = TrinoClient::new(
        context.trino_port(),
        context.sql_user(),
        context.deadline(),
        &root,
    )?;
    let explain = session.execute(format!(
        "EXPLAIN WITH SESSION explain_detail = 'CONTRACT' {query}"
    ))?;
    let mut plan_text = String::new();
    for row in &explain.rows {
        let [TrinoValue::Text(line)] = row.as_slice() else {
            anyhow::bail!("unexpected deep-type EXPLAIN row: {row:?}");
        };
        plan_text.push_str(line);
        plan_text.push('\n');
    }
    std::fs::write(root.join("source.sql"), &query)?;
    std::fs::write(root.join("source-plan.txt"), &plan_text)?;
    let edges: Vec<_> = plan_text
        .lines()
        .filter(|line| line.starts_with("  edge") && line.contains("kind=stream"))
        .collect();
    if selected.is_none() {
        ensure!(edges.len() == 1, "expected one target stream: {edges:?}");
    }
    let root_fragment = plan_text
        .lines()
        .find_map(|line| line.strip_prefix("RESULT PORT fragment="))
        .and_then(|tail| tail.split_once(','))
        .map(|(fragment, _)| fragment)
        .context("the compiled plan has no root fragment")?;
    let fragments: BTreeSet<u32> = plan_text
        .lines()
        .filter_map(|line| line.strip_prefix("PLAN FRAGMENT "))
        .map(str::parse)
        .collect::<std::result::Result<_, _>>()?;
    let stage = |fragment: &str| -> Result<String> {
        let fragment = fragment.parse::<u32>()?;
        // Scheduling assigns stage IDs in increasing fragment ID order.
        ensure!(fragments.contains(&fragment), "unknown fragment {fragment}");
        Ok((fragments.range(..fragment).count() + 1).to_string())
    };
    let root_header = format!("PLAN FRAGMENT {root_fragment}\n");
    let root_plan = plan_text
        .split_once(&root_header)
        .map(|(_, plan)| plan.split("\nPLAN FRAGMENT ").next().unwrap())
        .context("the compiled plan has no root fragment body")?;
    let imports: Vec<_> = root_plan
        .lines()
        .filter(|line| {
            line.trim_start().starts_with("inbound edge")
                && line.contains("destination-distribution=singleton")
                && line.matches("List(").count() == TYPE_DEPTH - 1
                && line.matches("Struct(").count() == ROW_WIDTH + 1
        })
        .collect();
    ensure!(
        imports.len() == 1,
        "the root must import the full deep and wide values"
    );
    let import_field = |prefix| {
        imports[0]
            .split_ascii_whitespace()
            .find_map(|word| word.strip_prefix(prefix))
    };
    let destination_node = import_field("destination=n").context("target cut has no node")?;
    let sender_stage = stage(import_field("source=f").context("target cut has no source")?)?;
    let root_stage = stage(root_fragment)?;
    let mut pool_references = BTreeSet::new();
    if selected.is_some() {
        let mut deep_references = BTreeSet::new();
        let mut wide_references = BTreeSet::new();
        for line in plan_text.lines() {
            let Some((_, reference)) = line.split_once("constant-reference(pool=") else {
                continue;
            };
            let Some((reference, _)) = reference.split_once(')') else {
                continue;
            };
            let (pool, ordinal) = reference
                .split_once(", ordinal=")
                .context("constant reference has no original ordinal")?;
            let reference = (pool.parse::<u32>()?, ordinal.parse::<usize>()?);
            if line.matches("List(").count() == TYPE_DEPTH - 1 {
                deep_references.insert(reference);
            }
            if line.matches("Struct(").count() == ROW_WIDTH + 1 {
                wide_references.insert(reference);
            }
        }
        ensure!(
            deep_references.len() >= 3 && wide_references.len() >= 2,
            "expected complete constant references for all target values: lists={deep_references:?}, rows={wide_references:?}"
        );
        pool_references.extend(deep_references);
        pool_references.extend(wide_references);
    }

    let before_roles = context.handle().launched_backend_topology()?;
    let before = deep_exchange_metrics(context, &root, "before")?;
    let pages = if let Some(selected) = selected {
        let initial = client.submit(&query)?;
        let next = initial
            .next_uri()?
            .context("missing original native continuation")?;
        let first = initial.json()?;
        let query_id = first["id"]
            .as_str()
            .context("missing original statement ID")?;
        if let Err(error) = selected.arm_client_query(context, query_id, &query) {
            return with_cleanup(Err(error), client.delete(&next).map(|_| ()));
        }
        let mut pages = vec![initial, client.get(&next)?];
        while let Some(uri) = pages.last().unwrap().next_uri()? {
            pages.push(client.get(&uri)?);
        }
        pages
    } else {
        client.execute(&query)?
    };
    let after = deep_exchange_metrics(context, &root, "after")?;
    let after_roles = context.handle().launched_backend_topology()?;
    ensure!(
        before_roles
            .iter()
            .map(|role| &role.process_id)
            .eq(after_roles.iter().map(|role| &role.process_id)),
        "the BE identity changed while observing the target exchange"
    );
    let (columns, rows) = read_typed_pages(&pages, "deep and wide values")?;
    ensure!(
        columns[1]["type"] == "bigint" && columns[2]["type"] == "integer",
        "deep and wide values were not consumed before the Trino projection: {columns}"
    );
    let expected: Vec<_> = (1..=9)
        .map(|n| {
            let leaf = if n == 9 {
                None
            } else if n % 2 == 0 {
                Some(7)
            } else {
                Some(19)
            };
            let corner = if n % 2 == 0 { 41 } else { 73 };
            json!([n, leaf, corner])
        })
        .collect();
    ensure!(rows == expected, "incorrect deep and wide leaves: {rows:?}");

    if let Some(selected) = selected {
        let source_fragment = plan_text
            .lines()
            .find(|line| line.trim_start().starts_with("VALUES stats="))
            .and_then(|line| line.split_once("[fragment="))
            .and_then(|(_, tail)| tail.split_once(','))
            .map(|(fragment, _)| fragment)
            .context("no VALUES fragment in the compiled plan")?;
        verify_deep_type_observations(
            context,
            selected,
            (sender_stage.as_str(), root_stage.as_str(), destination_node),
            source_fragment,
            &pool_references,
            &root,
        )?;
        return Ok(());
    }

    let receivers: Vec<_> = before
        .iter()
        .zip(&after)
        .enumerate()
        .filter(|(_, (old, new))| new.0.checked_sub(old.0) == Some(1))
        .map(|(index, _)| index)
        .collect();
    ensure!(
        receivers.len() == 1
            && before
                .iter()
                .zip(&after)
                .enumerate()
                .all(|(index, (old, new))| index == receivers[0] || new.0 == old.0),
        "the single receiving Root EOF must have an exact BE owner: {before:?} -> {after:?}"
    );
    ensure!(
        before
            .iter()
            .zip(&after)
            .enumerate()
            .any(|(index, (old, new))| index != receivers[0] && new.1 > old.1),
        "no different BE acknowledged bytes on the single target stream"
    );
    Ok(())
}

fn marker_field<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    line.split_ascii_whitespace()
        .filter_map(|word| word.split_once('='))
        .find_map(|(name, value)| (name == key).then_some(value))
}

fn verify_deep_type_observations(
    context: &mut ScenarioContext,
    selected: &SelectedStatement,
    target_route: (&str, &str, &str),
    source_fragment: &str,
    pool_references: &BTreeSet<(u32, usize)>,
    artifact_root: &Path,
) -> Result<()> {
    let (sender_stage, root_stage, destination_node) = target_route;
    let deadline = context.deadline();
    loop {
        let mut accepted_values = BTreeSet::new();
        let mut data_routes = BTreeSet::new();
        let mut consumed_routes = BTreeSet::new();
        for index in 0..context.handle().be_count() {
            let handle = context.handle();
            let binding = handle.selected_native_observation_binding(
                index,
                ObservationAction::TaskProtocol.as_str(),
                &selected.token,
                None,
            )?;
            let Some(binding) = binding else { continue };
            let execution = format!(
                "{}:{}:{}",
                binding.query_id_hi, binding.query_id_lo, binding.attempt_id,
            );
            let log = handle.be_current_log_contents(index)?;
            let scoped: Vec<_> = log
                .lines()
                .filter(|line| marker_field(line, "execution_id") == Some(execution.as_str()))
                .collect();

            for source in scoped.iter().copied().filter(|line| {
                (line.contains(TASK_VALUES_SOURCE) || line.contains(TASK_VALUES_CAST_SOURCE))
                    && marker_field(line, "fragment") == Some(source_fragment)
            }) {
                let same_task = |other: &str| {
                    ["backend", "stage", "task"]
                        .into_iter()
                        .all(|key| marker_field(source, key) == marker_field(other, key))
                };
                let pool =
                    marker_field(source, "pool_id").and_then(|value| value.parse::<u32>().ok());
                let ordinals = marker_field(source, "first_ordinal")
                    .and_then(|value| value.parse::<usize>().ok())
                    .zip(
                        marker_field(source, "length")
                            .and_then(|value| value.parse::<usize>().ok()),
                    )
                    .and_then(|(first, length)| Some(first..first.checked_add(length)?));
                if let Some((pool, ordinals)) = pool.zip(ordinals) {
                    let record_present = scoped.iter().copied().any(|record| {
                        record.contains(TASK_CONSTANT_IPC_RECORD)
                            && same_task(record)
                            && marker_field(record, "fragment") == Some(source_fragment)
                            && marker_field(record, "pool_id") == marker_field(source, "pool_id")
                            && marker_field(record, "rows")
                                .and_then(|value| value.parse::<usize>().ok())
                                .is_some_and(|rows| ordinals.end <= rows)
                    });
                    let task_finished = scoped.iter().copied().any(|record| {
                        record.contains(TASK_TERMINAL_RETAINED)
                            && same_task(record)
                            && marker_field(record, "state") == Some("FINISHED")
                    });
                    if record_present && task_finished {
                        accepted_values.extend(pool_references.iter().copied().filter(
                            |(expected_pool, ordinal)| {
                                pool == *expected_pool && ordinals.contains(ordinal)
                            },
                        ));
                    }
                }
            }

            for line in &scoped {
                if marker_field(line, "source_stage") != Some(sender_stage)
                    || marker_field(line, "destination_stage") != Some(root_stage)
                    || marker_field(line, "node_id") != Some(destination_node)
                    || marker_field(line, "source") == marker_field(line, "destination")
                {
                    continue;
                }
                let route = [
                    "execution_id",
                    "source",
                    "source_task",
                    "destination",
                    "destination_task",
                    "sender_ordinal",
                ]
                .map(|name| marker_field(line, name).map(str::to_owned));
                if route.iter().any(Option::is_none) {
                    continue;
                }
                if line.contains(EXCHANGE_GRANT_ONLY_PROBE)
                    && marker_field(line, "event") == Some("data_custody_received")
                    && marker_field(line, "source") == Some(binding.backend_process_id.as_str())
                {
                    data_routes.insert(route);
                } else if line.contains(EXCHANGE_SNAPSHOT)
                    && marker_field(line, "event") == Some("receiver_consumed")
                    && marker_field(line, "destination")
                        == Some(binding.backend_process_id.as_str())
                {
                    consumed_routes.insert(route);
                }
            }
            std::fs::write(
                artifact_root.join(format!("be-{index}-native-observation.log")),
                scoped.join("\n"),
            )?;
        }
        let accepted_route = !data_routes.is_disjoint(&consumed_routes);
        if &accepted_values == pool_references && accepted_route {
            return Ok(());
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        ensure!(
            !remaining.is_zero(),
            "missing native deep-type observations: values={accepted_values:?}/{pool_references:?}, exchange={accepted_route}"
        );
        std::thread::sleep(remaining.min(std::time::Duration::from_millis(25)));
    }
}

fn verify_deep_type_refusal(context: &mut ScenarioContext) -> Result<()> {
    let raw = TrinoClient::new(
        context.trino_port(),
        context.sql_user(),
        context.deadline(),
        &context.scenario_root().join("depth-limit-http"),
    )?;
    let measure = |context: &mut ScenarioContext| {
        let owner = context.handle();
        (0..owner.be_count())
            .map(|i| owner.backend_task_execution_tasks_created(i))
            .collect::<Result<Vec<_>>>()
    };
    let before = measure(context)?;
    let type_name = format!(
        "{}BIGINT{}",
        "ARRAY(".repeat(TYPE_DEPTH),
        ")".repeat(TYPE_DEPTH)
    );
    let pages = raw.execute(&format!("SELECT CAST(NULL AS {type_name})"))?;
    let terminal = pages.last().context("missing D+1 rejection page")?;
    let verdict = terminal.json()?;
    let message = verdict["error"]["message"].as_str().unwrap_or_default();
    ensure!(
        terminal.next_uri()?.is_none()
            && verdict["error"]["errorName"] == "NOT_SUPPORTED"
            && message.contains(&format!("depth limit of {TYPE_DEPTH}")),
        "unexpected D+1 SQL verdict: {verdict}"
    );
    ensure!(measure(context)? == before, "D+1 SQL created a BE Task");
    Ok(())
}

fn deep_exchange_metrics(
    context: &mut ScenarioContext,
    root: &std::path::Path,
    phase: &str,
) -> Result<Vec<(u64, u64)>> {
    let handle = context.handle();
    (0..handle.be_count())
        .map(|index| {
            let text = handle.backend_prometheus_text(index)?;
            std::fs::write(root.join(format!("be-{index}-{phase}.prom")), &text)?;
            let sample = |prefix: &str| -> Result<u64> {
                let mut values = text.lines().filter_map(|line| line.strip_prefix(prefix));
                let value = values.next().map_or(Ok(0), |value| value.parse())?;
                ensure!(values.next().is_none(), "duplicate metric {prefix}");
                Ok(value)
            };
            Ok((
                sample("novarocks_backend_task_result_terminals_total{terminal=\"finished\"} ")?,
                sample("novarocks_exchange_shuffle_bytes_total ")?,
            ))
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
