// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Explicit native acceptance scenarios with independent REST/wire side effects.
pub(super) mod wire;
use super::mv_recovery::control_plan::{self, FeLaunchIntent};
use super::mv_recovery::protobuf_bytes_field;
use super::mv_uea7::{
    ManagedMvRestFixture, assert_rows, execute, require_three_backends,
    select_catalog_and_namespace,
};
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, bail, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::ServerHandle;
use novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan;
use reqwest::blocking::Client;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use wire::{Graph, Marker, RECORD, ROOT, Record, Reference, Version};

const NS: &str = "ns";
#[derive(Clone, Copy)]
enum Kind {
    Expiry,
    Consistent,
    Keyed,
}
struct Documents {
    kind: Kind,
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}
pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    [Kind::Expiry, Kind::Consistent, Kind::Keyed]
        .into_iter()
        .map(|kind| {
            Box::new(Documents {
                kind,
                fixture: Mutex::new(None),
            }) as Box<dyn Scenario>
        })
        .collect()
}
impl Documents {
    fn catalog(&self) -> &'static str {
        match self.kind {
            Kind::Expiry => "uea7_expiry",
            Kind::Consistent => "uea7_consistent",
            Kind::Keyed => "uea7_keyed",
        }
    }
}
impl Scenario for Documents {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "mv_uea7_documents",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        match self.kind {
            Kind::Expiry => "mv/document-expiry-channel",
            Kind::Consistent => "mv/document-consistent-read",
            Kind::Keyed => "mv/document-keyed-writes",
        }
    }
    fn is_explicit_stage(&self) -> bool {
        true
    }
    fn launch_config(&self, root: &Path) -> Result<ScenarioLaunchConfig> {
        let (fixture, mut launch) = ManagedMvRestFixture::start_recorded(root, self.catalog())?;
        if matches!(self.kind, Kind::Consistent) {
            // Use the existing FE file-log owner, isolated under this scenario's
            // collected artifacts. Public SQL errors never carry document keys.
            let diagnostics = root.join("provider-diagnostics");
            fs::create_dir_all(&diagnostics)?;
            launch.child_environment.fe.insert(
                "NOVAROCKS_LOG_DIR".into(),
                diagnostics.to_string_lossy().into_owned(),
            );
            launch.child_environment.fe.insert(
                "NOVAROCKS_LOG_BASENAME".into(),
                "mv-document-consistent".into(),
            );
        }
        launch
            .config_overlay
            .fe
            .get_or_insert_with(String::new)
            .push_str("\n[standalone_server]\niceberg_maintenance_enabled = false\n");
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("MV document fixture lock poisoned"))?;
        ensure!(slot.is_none(), "document fixture initialized twice");
        *slot = Some(fixture);
        Ok(launch)
    }
    fn test_session_plan(
        &self,
        _: &ScenarioLaunchConfig,
        root: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<ClusterTestSessionPlan>> {
        let mut grants = Vec::new();
        let selections: &[(&str, &str, bool)] = match self.kind {
            Kind::Expiry => &[
                ("a", "a-initial", false),
                ("a", "a-orphans", false),
                ("b_main", "b-main", true),
                ("b_move", "b-move", true),
                ("b_delete", "b-delete", true),
                ("d", "d-layout", true),
                ("e", "e-rollback", true),
            ],
            Kind::Consistent => &[("history", "history-diagnostic", false)],
            Kind::Keyed => &[],
        };
        for (table, token, barrier) in selections {
            grants.push(control_plan::document_expiry(
                token,
                self.catalog(),
                NS,
                table,
                token,
                barrier.then(|| root.join(token)).as_deref(),
            ));
        }
        let launch = if grants.is_empty() {
            FeLaunchIntent::Ordinary
        } else {
            FeLaunchIntent::MvActions(grants)
        };
        control_plan::plan(cluster_size, binary, deadline, vec![launch]).map(Some)
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let (create, rest, workspace, env) = {
            let fixture = self
                .fixture
                .lock()
                .map_err(|_| anyhow::anyhow!("MV document fixture lock poisoned"))?;
            let f = fixture.as_ref().context("document fixture missing")?;
            (
                f.create_catalog_sql().to_owned(),
                f.rest_uri().to_owned(),
                f.rest_fixture().workspace_root().to_owned(),
                f.rest_fixture().runtime_env_file()?,
            )
        };
        let catalog = self.catalog();
        let mut conn = connect(context)?;
        execute(
            context,
            &mut conn,
            "create private document catalog",
            &create,
        )?;
        execute(
            context,
            &mut conn,
            "create document namespace",
            &format!("CREATE DATABASE {catalog}.{NS}"),
        )?;
        select_catalog_and_namespace(context, &mut conn, catalog)?;
        execute(
            context,
            &mut conn,
            "create row-lineage document source",
            "CREATE TABLE orders (k1 INT, v2 BIGINT) TBLPROPERTIES ('format-version'='3','write.row-lineage'='true')",
        )?;
        execute(
            context,
            &mut conn,
            "seed document source",
            "INSERT INTO orders VALUES (1, 10), (2, 20)",
        )?;
        match self.kind {
            Kind::Expiry => run_expiry(context, &mut conn, catalog, &rest, &workspace, &env)?,
            Kind::Consistent => self.run_consistent(context, &mut conn, catalog, &rest)?,
            Kind::Keyed => run_keyed(context, &mut conn, &rest)?,
        }
        conn.finish()?;
        Ok(())
    }
    fn teardown(&self) -> Result<()> {
        if let Some(mut fixture) = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("document fixture lock poisoned"))?
            .take()
        {
            fixture.shutdown()?;
        }
        Ok(())
    }
}

pub(super) fn load_metadata(
    context: &ScenarioContext,
    rest: &str,
    namespace: &str,
    table: &str,
) -> Result<Value> {
    let response = client(context)?
        .get(table_url(rest, namespace, table))
        .send()?
        .error_for_status()?;
    let loaded: Value = response.json()?;
    Ok(loaded
        .get("metadata")
        .context("REST load lacks metadata")?
        .clone())
}
fn connect(context: &mut ScenarioContext) -> Result<crate::actors::mysql::DeadlineConnection> {
    crate::actors::mysql::connect_until(
        context.mysql_user(),
        context.mysql_port(),
        context.deadline(),
    )
}
fn client(context: &ScenarioContext) -> Result<Client> {
    Ok(Client::builder()
        .no_proxy()
        .timeout(
            context
                .remaining("document REST operation")?
                .min(Duration::from_secs(30)),
        )
        .build()?)
}
fn table_url(rest: &str, namespace: &str, table: &str) -> String {
    format!(
        "{}/v1/namespaces/{namespace}/tables/{table}",
        rest.trim_end_matches('/')
    )
}
pub(super) fn commit_updates(
    context: &ScenarioContext,
    rest: &str,
    namespace: &str,
    table: &str,
    base: &Value,
    updates: Vec<Value>,
    label: &str,
) -> Result<Value> {
    let mut requirements = vec![
        json!({"type":"assert-table-uuid","uuid":base["table-uuid"]}),
        json!({"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":base["current-snapshot-id"].as_i64().filter(|id|*id!=-1)}),
    ];
    if updates
        .iter()
        .any(|update| update["action"].as_str() == Some("add-schema"))
    {
        requirements.push(json!({"type":"assert-current-schema-id","current-schema-id":base["current-schema-id"]}));
        requirements.push(json!({"type":"assert-last-assigned-field-id","last-assigned-field-id":base["last-column-id"]}));
    }
    let request = json!({"requirements":requirements,"updates":updates});
    let evidence = artifact(context, label)?;
    fs::write(
        evidence.join("before.json"),
        serde_json::to_vec_pretty(base)?,
    )?;
    fs::write(
        evidence.join("request.json"),
        serde_json::to_vec_pretty(&request)?,
    )?;
    let response = client(context)?
        .post(table_url(rest, namespace, table))
        .json(&request)
        .send()?;
    let status = response.status();
    let body = response.text()?;
    fs::write(
        evidence.join("response.status"),
        format!("{}\n", status.as_u16()),
    )?;
    fs::write(evidence.join("response.body"), &body)?;
    ensure!(
        status.is_success(),
        "external document mutation {label} returned {status}: {body}"
    );
    let after = load_metadata(context, rest, namespace, table)?;
    fs::write(
        evidence.join("after.json"),
        serde_json::to_vec_pretty(&after)?,
    )?;
    Ok(after)
}
static ARTIFACT_ORDINAL: AtomicU64 = AtomicU64::new(0);
fn artifact(context: &ScenarioContext, label: &str) -> Result<PathBuf> {
    let parent = context.scenario_root().join("document-evidence");
    fs::create_dir_all(&parent)?;
    let dir = parent.join(format!(
        "{:04}-{label}",
        ARTIFACT_ORDINAL.fetch_add(1, Ordering::SeqCst)
    ));
    fs::create_dir(&dir)?;
    Ok(dir)
}
fn save_graph(context: &ScenarioContext, label: &str, metadata: &Value) -> Result<Graph> {
    let graph = Graph::from_metadata(metadata)?;
    let dir = artifact(context, label)?;
    fs::write(
        dir.join("metadata.json"),
        serde_json::to_vec_pretty(metadata)?,
    )?;
    fs::write(
        dir.join("oracle.json"),
        serde_json::to_vec_pretty(
            &json!({"definition":graph.definition,"interpretation":graph.interpretation,"binding":graph.binding,"current":graph.current,"private_bytes":graph.private_bytes,"record_bytes":graph.record_bytes,"diagnostics":graph.diagnostics}),
        )?,
    )?;
    Ok(graph)
}
fn create_mv(
    context: &mut ScenarioContext,
    conn: &mut crate::actors::mysql::DeadlineConnection,
    name: &str,
) -> Result<()> {
    execute(
        context,
        conn,
        "create isolated document MV",
        &format!(
            "CREATE MATERIALIZED VIEW {name} DISTRIBUTED BY HASH(k1) BUCKETS 1 REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine'='iceberg') AS SELECT k1, v2 FROM orders"
        ),
    )
}
fn refresh(
    context: &mut ScenarioContext,
    conn: &mut crate::actors::mysql::DeadlineConnection,
    name: &str,
) -> Result<()> {
    execute(
        context,
        conn,
        "ordinary document MV refresh",
        &format!("REFRESH MATERIALIZED VIEW {name} WITH SYNC MODE"),
    )
}
fn repartition(
    context: &mut ScenarioContext,
    conn: &mut crate::actors::mysql::DeadlineConnection,
    name: &str,
    buckets: u32,
) -> Result<()> {
    execute(
        context,
        conn,
        "publish a distinct document interpretation",
        &format!("ALTER MATERIALIZED VIEW {name} REPARTITION BY (bucket(k1, {buckets}))"),
    )
}
fn expiry_sql(catalog: &str, table: &str, token: &str, keep: usize) -> String {
    format!(
        "CALL {catalog}.system.novarocks_mv_document_expiry(table => '{NS}.{table}', keep_last => {keep}, token => '{token}')"
    )
}
fn expire(
    context: &mut ScenarioContext,
    conn: &mut crate::actors::mysql::DeadlineConnection,
    catalog: &str,
    table: &str,
    token: &str,
    keep: usize,
) -> Result<()> {
    execute(
        context,
        conn,
        "finite document expiry channel",
        &expiry_sql(catalog, table, token, keep),
    )
}
fn current_record(metadata: &Value) -> Result<Record> {
    let graph = Graph::from_metadata(metadata)?;
    graph
        .snapshots
        .iter()
        .find(|(id, _)| Some(*id) == graph.current)
        .and_then(|(_, p)| p.clone())
        .context("current MV lacks P")
}
fn snapshot_ids(metadata: &Value) -> Result<BTreeSet<i64>> {
    metadata["snapshots"]
        .as_array()
        .context("metadata has no snapshots")?
        .iter()
        .map(|s| s["snapshot-id"].as_i64().context("snapshot lacks ID"))
        .collect()
}
fn exact_private_after_prune(before: &Graph, after: &Graph) -> Result<()> {
    for (key, value) in &before.properties {
        match after.properties.get(key) {
            Some(actual) => ensure!(actual == value, "expiry rewrote retained private key {key}"),
            None => ensure!(
                key.starts_with("novarocks.doc.v2.rev."),
                "expiry removed non-version private key {key}"
            ),
        }
    }
    ensure!(
        after
            .properties
            .keys()
            .all(|key| before.properties.contains_key(key)),
        "expiry invented private properties"
    );
    Ok(())
}
fn reintroduce_closure(graph: &Graph) -> BTreeMap<String, String> {
    graph
        .versions
        .values()
        .map(|v| (v.key(), v.encoded().expect("validated version")))
        .chain([(ROOT.into(), graph.binding.clone())])
        .collect()
}
fn append_zero(
    context: &ScenarioContext,
    rest: &str,
    table: &str,
    base: &Value,
    record: Option<&str>,
    properties: BTreeMap<String, String>,
    extra: Vec<Value>,
) -> Result<Value> {
    let snapshots = base["snapshots"].as_array().context("no snapshots")?;
    let parent = base["current-snapshot-id"]
        .as_i64()
        .context("zero-file commit needs a parent")?;
    let old = snapshots
        .iter()
        .find(|s| s["snapshot-id"].as_i64() == Some(parent))
        .context("parent snapshot missing")?;
    let id = snapshot_ids(base)?
        .last()
        .copied()
        .context("no snapshot ID")?
        .checked_add(1)
        .context("snapshot ID overflow")?;
    let timestamp = snapshots
        .iter()
        .map(|s| {
            s["timestamp-ms"]
                .as_u64()
                .context("snapshot timestamp missing")
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .max()
        .context("no snapshot timestamp")?
        .max(
            base["last-updated-ms"]
                .as_u64()
                .context("table timestamp missing")?,
        )
        .checked_add(1)
        .context("timestamp overflow")?;
    let mut summary = serde_json::Map::from_iter([
        ("operation".into(), json!("append")),
        ("added-data-files".into(), json!("0")),
        ("added-delete-files".into(), json!("0")),
        ("added-records".into(), json!("0")),
    ]);
    for (key, value) in old["summary"]
        .as_object()
        .context("parent summary missing")?
    {
        if key.starts_with("total-") {
            summary.insert(key.clone(), value.clone());
        }
    }
    if let Some(frame) = record {
        wire::record(frame)?;
        summary.insert(RECORD.into(), json!(frame));
    }
    let sequence = base["last-sequence-number"]
        .as_i64()
        .context("table sequence number missing")?
        .checked_add(1)
        .context("sequence number overflow")?;
    let schema_updates: Vec<_> = extra
        .iter()
        .filter(|update| update["action"].as_str() == Some("set-current-schema"))
        .collect();
    ensure!(
        schema_updates.len() <= 1,
        "ambiguous snapshot schema update"
    );
    let schema_id = schema_updates
        .first()
        .map_or(&base["current-schema-id"], |update| &update["schema-id"])
        .as_i64()
        .filter(|id| *id >= 0)
        .context("zero-file snapshot needs an exact schema ID")?;
    let mut snapshot = json!({"snapshot-id":id,"parent-snapshot-id":parent,"sequence-number":sequence,"timestamp-ms":timestamp,"manifest-list":old["manifest-list"],"summary":summary,"schema-id":schema_id});
    match base["format-version"]
        .as_u64()
        .context("table format version missing")?
    {
        2 => {}
        3 => {
            let first_row_id = base["next-row-id"]
                .as_u64()
                .context("v3 table next row ID missing")?;
            snapshot["first-row-id"] = json!(first_row_id);
            snapshot["added-rows"] = json!(0);
        }
        version => bail!("unsupported external zero-file snapshot format: {version}"),
    }
    let mut updates = extra;
    if !properties.is_empty() {
        updates.push(json!({"action":"set-properties","updates":properties}));
    }
    updates.push(json!({"action":"add-snapshot","snapshot":snapshot}));
    updates.push(
        json!({"action":"set-snapshot-ref","ref-name":"main","type":"branch","snapshot-id":id}),
    );
    commit_updates(
        context,
        rest,
        NS,
        table,
        base,
        updates,
        "external-zero-file-publication",
    )
}

fn publish_reordered_schema(
    context: &ScenarioContext,
    rest: &str,
    table: &str,
    base: &Value,
) -> Result<Value> {
    let current_schema_id = base["current-schema-id"]
        .as_i64()
        .context("current schema ID missing")?;
    let schemas = base["schemas"]
        .as_array()
        .context("table schemas missing")?;
    let next_schema_id = schemas
        .iter()
        .map(|schema| schema["schema-id"].as_i64().context("schema ID missing"))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .max()
        .context("no table schema")?
        .checked_add(1)
        .context("schema ID overflow")?;
    let next_version = i32::try_from(next_schema_id).context("schema version exceeds i32")?;
    let mut schema = schemas
        .iter()
        .find(|schema| schema["schema-id"].as_i64() == Some(current_schema_id))
        .cloned()
        .context("current schema missing")?;
    schema["schema-id"] = json!(next_schema_id);
    let fields = schema["fields"]
        .as_array_mut()
        .context("schema fields missing")?;
    ensure!(fields.len() >= 2, "schema reorder needs two fields");
    fields.reverse();

    let graph = Graph::from_metadata(base)?;
    let interpretation = &graph.versions[&graph.interpretation];
    let mut content = interpretation.bytes()?;
    let target = protobuf_bytes_field(&content, 9)?;
    let schema_version = protobuf_bytes_field(&content[target.clone()], 2)?;
    ensure!(
        schema_version.len() == 4,
        "schema version is not four bytes"
    );
    let range = target.start + schema_version.start..target.start + schema_version.end;
    let observed_version = i32::from_le_bytes(content[range.clone()].try_into()?);
    ensure!(
        i64::from(observed_version) == current_schema_id,
        "L schema version differs from observed physical schema"
    );
    content[range].copy_from_slice(&next_version.to_le_bytes());
    let new_l = Version::new(
        &interpretation.owner,
        &interpretation.name,
        interpretation.format.clone(),
        interpretation.refs.clone(),
        &content,
    )?;
    let binding = &graph.versions[&graph.binding];
    let new_b = Version::new(
        &binding.owner,
        &binding.name,
        binding.format.clone(),
        vec![
            Reference {
                rel: "definition".into(),
                id: graph.definition.clone(),
            },
            Reference {
                rel: "interpretation".into(),
                id: new_l.id.clone(),
            },
        ],
        &binding.bytes()?,
    )?;
    let p = current_record(base)?;
    let frame = wire::record_frame(&new_b.id, &p.payload);
    let properties = BTreeMap::from([
        (new_l.key(), new_l.encoded()?),
        (new_b.key(), new_b.encoded()?),
        (ROOT.into(), new_b.id.clone()),
    ]);
    let last_column_id = base["last-column-id"]
        .as_i64()
        .context("last column ID missing")?;
    let published = append_zero(
        context,
        rest,
        table,
        base,
        Some(&frame),
        properties,
        vec![
            json!({"action":"add-schema","schema":schema.clone(),"last-column-id":last_column_id}),
            json!({"action":"set-current-schema","schema-id":next_schema_id}),
        ],
    )?;
    let actual = Graph::from_metadata(&published)?;
    let published_schema = published["schemas"]
        .as_array()
        .context("published schemas missing")?
        .iter()
        .find(|schema| schema["schema-id"].as_i64() == Some(next_schema_id))
        .context("published schema missing")?;
    ensure!(
        published["current-schema-id"].as_i64() == Some(next_schema_id)
            && published_schema["fields"] == schema["fields"]
            && published["last-column-id"] == base["last-column-id"]
            && published["partition-specs"] == base["partition-specs"]
            && published["default-spec-id"] == base["default-spec-id"]
            && published["next-row-id"] == base["next-row-id"]
            && actual.interpretation == new_l.id
            && actual.binding == new_b.id
            && actual.definition == graph.definition
            && new_l.id != graph.interpretation
            && new_b.id != graph.binding,
        "external schema publication did not install the exact distinct L/B closure"
    );
    Ok(published)
}

fn simulate_old_layout(
    context: &ScenarioContext,
    rest: &str,
    table: &str,
    current: &Value,
    old: &Value,
    restore_default_spec: bool,
) -> Result<Value> {
    let graph = Graph::from_metadata(old)?;
    let p = current_record(old)?;
    let frame = wire::record_frame(&graph.binding, &p.payload);
    let mut extra = Vec::new();
    if restore_default_spec {
        let id = old["default-spec-id"]
            .as_i64()
            .context("old partition spec missing")?;
        extra.push(json!({"action":"set-default-spec","spec-id":id}));
        let schema = old["current-schema-id"]
            .as_i64()
            .context("old schema missing")?;
        extra.push(json!({"action":"set-current-schema","schema-id":schema}));
    }
    append_zero(
        context,
        rest,
        table,
        current,
        Some(&frame),
        reintroduce_closure(&graph),
        extra,
    )
}

struct HeldExpiry {
    actor: Option<JoinHandle<std::result::Result<(), String>>>,
    trigger: PathBuf,
}
impl HeldExpiry {
    fn start(
        context: &mut ScenarioContext,
        catalog: &str,
        table: &str,
        token: &str,
    ) -> Result<Self> {
        let dir = context.scenario_root().join(token);
        fs::create_dir_all(&dir)?;
        let trigger = dir.join("mv-document-expiry-planned.trigger");
        fs::write(&trigger, format!("token={token}\n"))?;
        let user = context.mysql_user().to_owned();
        let port = context.mysql_port();
        let deadline = context.deadline();
        let sql = expiry_sql(catalog, table, token, 1);
        let actor = thread::spawn(move || {
            crate::actors::mysql::connect_until(&user, port, deadline)
                .and_then(|conn| {
                    conn.run(|c| {
                        c.query_drop(sql)?;
                        Ok(())
                    })
                })
                .map_err(|e| format!("{e:#}"))
        });
        let mut held = Self {
            actor: Some(actor),
            trigger,
        };
        let marker = format!("NOVAROCKS_MV_DOCUMENT_EXPIRY_PLANNED token={token}");
        loop {
            if context.handle().fe_log_contents()?.contains(&marker) {
                break;
            }
            if held.actor.as_ref().is_some_and(|a| a.is_finished()) {
                let result = held.release()?;
                bail!("expiry failed before its planning barrier: {result:?}");
            }
            context.remaining("wait for expiry planning barrier")?;
            thread::sleep(Duration::from_millis(20));
        }
        Ok(held)
    }
    fn release(&mut self) -> Result<std::result::Result<(), String>> {
        if self.trigger.exists() {
            fs::remove_file(&self.trigger)?;
        }
        let actor = self.actor.take().context("expiry actor already joined")?;
        actor
            .join()
            .map_err(|_| anyhow::anyhow!("expiry SQL actor panicked"))
    }
}
impl Drop for HeldExpiry {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.trigger);
        if let Some(actor) = self.actor.take() {
            let _ = actor.join();
        }
    }
}

fn require_uncommitted(result: std::result::Result<(), String>, label: &str) -> Result<()> {
    let error = result
        .err()
        .with_context(|| format!("expiry accepted {label}"))?;
    ensure!(
        error.contains("MV document expiry did not commit"),
        "expiry failed before a definite rejected commit for {label}: {error}"
    );
    Ok(())
}

fn run_keyed(
    context: &mut ScenarioContext,
    conn: &mut crate::actors::mysql::DeadlineConnection,
    rest: &str,
) -> Result<()> {
    create_mv(context, conn, "keyed")?;
    refresh(context, conn, "keyed")?;
    repartition(context, conn, "keyed", 2)?;
    let before = load_metadata(context, rest, NS, "keyed")?;
    let graph = save_graph(context, "before-late-marker", &before)?;
    let marker = Marker::late()?;
    let after = commit_updates(
        context,
        rest,
        NS,
        "keyed",
        &before,
        vec![json!({"action":"set-properties","updates":{(marker.key()):marker.encoded()?}})],
        "late-third-party-marker",
    )?;
    let actual = save_graph(context, "after-late-marker", &after)?;
    let mut expected = graph.properties.clone();
    expected.insert(marker.key(), marker.encoded()?);
    ensure!(
        actual.properties == expected
            && before["snapshots"] == after["snapshots"]
            && actual.binding == graph.binding,
        "late marker changed unrelated keys, P, L2 or snapshots"
    );
    assert_rows(
        context,
        conn,
        "SELECT k1,v2 FROM keyed ORDER BY k1",
        &[(1, 10), (2, 20)],
        "read MV with late foreign marker",
    )?;
    execute(
        context,
        conn,
        "advance keyed source",
        "INSERT INTO orders VALUES (3,30)",
    )?;
    refresh(context, conn, "keyed")?;
    let refreshed = save_graph(
        context,
        "late-marker-after-refresh",
        &load_metadata(context, rest, NS, "keyed")?,
    )?;
    ensure!(
        refreshed.properties.get(&marker.key()) == Some(&marker.encoded()?),
        "ordinary publication removed foreign marker"
    );
    assert_rows(
        context,
        conn,
        "SELECT k1,v2 FROM keyed ORDER BY k1",
        &[(1, 10), (2, 20), (3, 30)],
        "refresh with foreign marker publishes new rows",
    )?;
    Ok(())
}

fn run_expiry(
    context: &mut ScenarioContext,
    conn: &mut crate::actors::mysql::DeadlineConnection,
    catalog: &str,
    rest: &str,
    workspace: &Path,
    env: &Path,
) -> Result<()> {
    // a: channel pruning, outside snapshot removal, and exact reintroduction.
    create_mv(context, conn, "a")?;
    refresh(context, conn, "a")?;
    let a1 = load_metadata(context, rest, NS, "a")?;
    repartition(context, conn, "a", 2)?;
    let a2 = load_metadata(context, rest, NS, "a")?;
    // A second native repartition requires unsupported multi-spec overwrite.
    // An external schema reorder preserves field identities and publishes a
    // third distinct L/B closure without changing the writer's support contract.
    let a3 = publish_reordered_schema(context, rest, "a", &a2)?;
    assert_rows(
        context,
        conn,
        "SELECT k1,v2 FROM a ORDER BY k1",
        &[(1, 10), (2, 20)],
        "read exact externally published third interpretation",
    )?;
    let before = save_graph(context, "a-before-expiry", &a3)?;
    let first_graph = Graph::from_metadata(&a1)?;
    expire(context, conn, catalog, "a", "a-initial", 2)?;
    let pruned = load_metadata(context, rest, NS, "a")?;
    let after = save_graph(context, "a-after-expiry", &pruned)?;
    ensure!(
        snapshot_ids(&pruned)?.len() == 2,
        "expiry channel did not remove exactly the oldest snapshot"
    );
    ensure!(
        !after.versions.contains_key(&first_graph.binding),
        "expiry retained unreferenced old binding"
    );
    let second_graph = Graph::from_metadata(&a2)?;
    let expected: BTreeSet<_> = [
        before.definition.clone(),
        before.interpretation.clone(),
        before.binding.clone(),
        second_graph.interpretation,
        second_graph.binding,
    ]
    .into_iter()
    .collect();
    ensure!(
        after.versions.keys().cloned().collect::<BTreeSet<_>>() == expected,
        "expiry did not collect exactly the old unreferenced D/L/B closure"
    );
    exact_private_after_prune(&before, &after)?;
    let old = a2["current-snapshot-id"]
        .as_i64()
        .context("a2 head missing")?;
    let external = commit_updates(
        context,
        rest,
        NS,
        "a",
        &pruned,
        vec![json!({"action":"remove-snapshots","snapshot-ids":[old]})],
        "external-snapshot-only-removal",
    )?;
    ensure!(
        external["properties"] == pruned["properties"],
        "external snapshot removal touched properties"
    );
    expire(context, conn, catalog, "a", "a-orphans", 1)?;
    let orphan_pruned = load_metadata(context, rest, NS, "a")?;
    let orphan_graph = save_graph(context, "a-orphan-versions-collected", &orphan_pruned)?;
    let expected: BTreeSet<_> = [
        before.definition.clone(),
        before.interpretation.clone(),
        before.binding.clone(),
    ]
    .into_iter()
    .collect();
    ensure!(
        orphan_graph
            .versions
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
            == expected,
        "orphan pass did not retain exactly the current D/L/B closure"
    );
    let a2_graph = Graph::from_metadata(&a2)?;
    ensure!(
        !orphan_graph.versions.contains_key(&a2_graph.binding),
        "next expiry retained externally orphaned binding"
    );
    let restored = simulate_old_layout(context, rest, "a", &orphan_pruned, &a1, true)?;
    let restored_graph = save_graph(context, "a-old-layout-reintroduced", &restored)?;
    ensure!(
        restored["current-schema-id"] == a1["current-schema-id"]
            && restored["default-spec-id"] == a1["default-spec-id"],
        "old layout publication did not restore its exact schema and partition spec"
    );
    for (key, value) in reintroduce_closure(&first_graph) {
        ensure!(
            restored_graph.properties.get(&key) == Some(&value),
            "reintroduced version differs from exact prior bytes: {key}"
        );
    }
    assert_rows(
        context,
        conn,
        "SELECT k1,v2 FROM a ORDER BY k1",
        &[(1, 10), (2, 20)],
        "read exact reintroduced old layout",
    )?;
    // b: advancing main, moving a tag and deleting a tag all race the frozen plan.
    for (table, token, action) in [
        ("b_main", "b-main", "main"),
        ("b_move", "b-move", "move"),
        ("b_delete", "b-delete", "delete"),
    ] {
        create_mv(context, conn, table)?;
        refresh(context, conn, table)?;
        let first = load_metadata(context, rest, NS, table)?;
        repartition(context, conn, table, 2)?;
        let middle = load_metadata(context, rest, NS, table)?;
        let middle_head = middle["current-snapshot-id"]
            .as_i64()
            .context("ref-race middle snapshot missing")?;
        execute(
            context,
            conn,
            "create a real unreferenced expiry candidate",
            &format!("ALTER MATERIALIZED VIEW {table} RESUME REFRESH"),
        )?;
        let mut before = load_metadata(context, rest, NS, table)?;
        let head = before["current-snapshot-id"]
            .as_i64()
            .context("ref-race current snapshot missing")?;
        let carrier = before["snapshots"]
            .as_array()
            .context("ref-race snapshots missing")?
            .iter()
            .find(|snapshot| snapshot["snapshot-id"].as_i64() == Some(head))
            .context("ref-race carrier missing")?;
        let middle_graph = Graph::from_metadata(&middle)?;
        let before_graph = Graph::from_metadata(&before)?;
        ensure!(
            snapshot_ids(&before)?.len() == 3
                && head != middle_head
                && carrier["parent-snapshot-id"].as_i64() == Some(middle_head)
                && before_graph.versions == middle_graph.versions
                && before_graph.roots == middle_graph.roots
                && current_record(&before)?.frame == current_record(&middle)?.frame
                && before_graph.slots[wire::CONFIG].content
                    == middle_graph.slots[wire::CONFIG].content
                && before_graph.slots[wire::CONFIG].format
                    == middle_graph.slots[wire::CONFIG].format
                && before_graph.slots[wire::CONFIG].writer == Some(head),
            "ref-race setup did not create a same-value carried-record slot snapshot"
        );
        if action != "main" {
            before = commit_updates(
                context,
                rest,
                NS,
                table,
                &before,
                vec![
                    json!({"action":"set-snapshot-ref","ref-name":"observed","type":"tag","snapshot-id":first["current-snapshot-id"]}),
                ],
                "create-observed-tag",
            )?;
        }
        ensure!(
            before["refs"]
                .as_object()
                .context("ref-race refs missing")?
                .values()
                .all(|reference| reference["snapshot-id"].as_i64() != Some(middle_head)),
            "ref-race middle snapshot is protected, so expiry would have no effect"
        );
        let mut held = HeldExpiry::start(context, catalog, table, token)?;
        let raced = match action {
            "main" => append_zero(
                context,
                rest,
                table,
                &before,
                Some(&current_record(&before)?.frame),
                BTreeMap::new(),
                vec![],
            )?,
            "move" => commit_updates(
                context,
                rest,
                NS,
                table,
                &before,
                vec![
                    json!({"action":"set-snapshot-ref","ref-name":"observed","type":"tag","snapshot-id":before["current-snapshot-id"]}),
                ],
                "move-observed-tag",
            )?,
            _ => commit_updates(
                context,
                rest,
                NS,
                table,
                &before,
                vec![json!({"action":"remove-snapshot-ref","ref-name":"observed"})],
                "delete-observed-tag",
            )?,
        };
        let result = held.release()?;
        require_uncommitted(result, &format!("changed observed {action}"))?;
        let final_table = load_metadata(context, rest, NS, table)?;
        ensure!(
            snapshot_ids(&final_table)? == snapshot_ids(&raced)?
                && final_table["properties"] == raced["properties"],
            "ref race expiry removed snapshots or properties"
        );
        save_graph(context, "ref-race-rejected", &final_table)?;
    }
    // c: Spark removes every record-bearing snapshot after one unrecorded append.
    create_mv(context, conn, "c")?;
    refresh(context, conn, "c")?;
    repartition(context, conn, "c", 2)?;
    let c = load_metadata(context, rest, NS, "c")?;
    let graph = Graph::from_metadata(&c)?;
    let bare = append_zero(context, rest, "c", &c, None, BTreeMap::new(), vec![])?;
    spark_expire(context, workspace, env, "c")?;
    let c_after = load_metadata(context, rest, NS, "c")?;
    ensure!(
        snapshot_ids(&c_after)?.len() == 1
            && c_after["current-snapshot-id"] == bare["current-snapshot-id"],
        "Spark expiry did not retain only unrecorded current snapshot"
    );
    for key in [
        ROOT.to_owned(),
        format!("novarocks.doc.v2.rev.{}", graph.binding),
        format!("novarocks.doc.v2.rev.{}", graph.definition),
        format!("novarocks.doc.v2.rev.{}", graph.interpretation),
    ] {
        ensure!(
            c_after["properties"][&key] == c["properties"][&key],
            "Spark removed root/current semantic closure key {key}"
        );
    }
    fs::write(
        artifact(context, "spark-expiry-current-root-retained")?.join("metadata.json"),
        serde_json::to_vec_pretty(&c_after)?,
    )?;
    // d: a new publication restores the old binding while expiry is held.
    create_mv(context, conn, "d")?;
    refresh(context, conn, "d")?;
    let d1 = load_metadata(context, rest, NS, "d")?;
    repartition(context, conn, "d", 2)?;
    let d2 = load_metadata(context, rest, NS, "d")?;
    let mut held = HeldExpiry::start(context, catalog, "d", "d-layout")?;
    let restored = simulate_old_layout(context, rest, "d", &d2, &d1, false)?;
    require_uncommitted(held.release()?, "changed-main old-binding publication")?;
    let d_after = load_metadata(context, rest, NS, "d")?;
    ensure!(
        d_after["properties"] == restored["properties"]
            && snapshot_ids(&d_after)? == snapshot_ids(&restored)?,
        "rejected expiry changed the reintroduced closure"
    );
    save_graph(context, "old-layout-race-rejected", &d_after)?;
    // e / D15: re-reference, then external rollback to the frozen main.
    create_mv(context, conn, "e")?;
    refresh(context, conn, "e")?;
    let e1 = load_metadata(context, rest, NS, "e")?;
    repartition(context, conn, "e", 2)?;
    let e2 = load_metadata(context, rest, NS, "e")?;
    let mut held = HeldExpiry::start(context, catalog, "e", "e-rollback")?;
    let reintroduced = simulate_old_layout(context, rest, "e", &e2, &e1, false)?;
    commit_updates(
        context,
        rest,
        NS,
        "e",
        &reintroduced,
        vec![
            json!({"action":"set-snapshot-ref","ref-name":"main","type":"branch","snapshot-id":e2["current-snapshot-id"]}),
        ],
        "rollback-after-reintroduction",
    )?;
    held.release()?.map_err(|e| {
        anyhow::anyhow!("D15 expiry unexpectedly rejected restored observed main: {e}")
    })?;
    let e_after = load_metadata(context, rest, NS, "e")?;
    let e_graph = save_graph(context, "rollback-history-diagnostic", &e_after)?;
    ensure!(
        !snapshot_ids(&e_after)?.contains(
            &e1["current-snapshot-id"]
                .as_i64()
                .context("e1 lacks main")?
        ) && snapshot_ids(&e_after)?.contains(
            &reintroduced["current-snapshot-id"]
                .as_i64()
                .context("reintroduced history lacks ID")?
        ),
        "D15 expiry did not remove its frozen selection while retaining the rolled-back snapshot"
    );
    ensure!(
        !e_graph.diagnostics.is_empty(),
        "rolled-back referencing snapshot did not expose history diagnostic"
    );
    assert_rows(
        context,
        conn,
        "SELECT k1,v2 FROM e ORDER BY k1",
        &[(1, 10), (2, 20)],
        "D15 current data remains readable",
    )?;
    execute(
        context,
        conn,
        "advance source before D15 repair",
        "INSERT INTO orders VALUES (3,30)",
    )?;
    refresh(context, conn, "e")?;
    let repaired = save_graph(
        context,
        "rollback-root-repaired",
        &load_metadata(context, rest, NS, "e")?,
    )?;
    ensure!(
        repaired.properties.get(ROOT) == Some(&repaired.binding),
        "next refresh did not repair root to current B"
    );
    Ok(())
}

#[cfg(unix)]
fn spark_expire(
    context: &ScenarioContext,
    workspace: &Path,
    env: &Path,
    table: &str,
) -> Result<()> {
    use novarocks_test_support::ManagedCommand;
    use std::os::unix::fs::OpenOptionsExt;
    let dir = artifact(context, "spark-expire-snapshots")?;
    let sql = dir.join("expire.sql");
    fs::write(
        &sql,
        format!(
            "CALL ice_rest.system.expire_snapshots(table => '{NS}.{table}', older_than => TIMESTAMP '2999-01-01 00:00:00', retain_last => 1);\n"
        ),
    )?;
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .context("system runner repository root")?;
    let mut command = ManagedCommand::new(repository.join("docker/iceberg-rest/spark-sql.sh"));
    command
        .arg(&sql)
        .env("NOVA_ENV_REST_ENV_FILE", env)
        .env("NOVAROCKS_WORKSPACE_ROOT", workspace);
    fs::write(
        dir.join("command.json"),
        serde_json::to_vec_pretty(
            &json!({"program":repository.join("docker/iceberg-rest/spark-sql.sh"),"arguments":[sql],"environment_locators":{"NOVA_ENV_REST_ENV_FILE":env,"NOVAROCKS_WORKSPACE_ROOT":workspace}}),
        )?,
    )?;
    let out = novarocks_test_support::capture_command_until(
        command,
        fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(dir.join("stdout.log"))?,
        fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(dir.join("stderr.log"))?,
        context.deadline(),
    )?;
    fs::write(
        dir.join("exit.json"),
        serde_json::to_vec_pretty(
            &json!({"exit":out.output.status.code(),"timed_out":out.timed_out,"failures":out.failures}),
        )?,
    )?;
    ensure!(
        out.output.status.success() && !out.timed_out && out.failures.is_empty(),
        "Spark snapshot expiry failed; retained outputs at {}",
        dir.display()
    );
    out.release_diagnostics()?;
    Ok(())
}
#[cfg(not(unix))]
fn spark_expire(_: &ScenarioContext, _: &Path, _: &Path, _: &str) -> Result<()> {
    bail!("Spark expiry process ownership requires Unix")
}

impl Documents {
    fn audit(
        &self,
    ) -> Result<Vec<novarocks_cluster_harness::recording_rest_catalog::RecordedRestRequest>> {
        self.fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("document fixture lock poisoned"))?
            .as_ref()
            .context("missing document fixture")?
            .recorded_requests()
    }
    fn assert_refs_since(&self, start: usize, table: &str) -> Result<()> {
        let audit = self.audit()?;
        let path = format!("/v1/namespaces/{NS}/tables/{table}");
        let loads: Vec<_> = audit[start..]
            .iter()
            .filter(|r| {
                r.method == "GET" && r.path_and_query.split('?').next() == Some(path.as_str())
            })
            .collect();
        ensure!(
            !loads.is_empty(),
            "product operation emitted no recorded table load"
        );
        ensure!(
            loads.iter().all(|r| r
                .path_and_query
                .split('?')
                .nth(1)
                .is_some_and(|query| query.split('&').any(|p| p == "snapshots=refs"))),
            "ordinary MV path loaded full history: {loads:?}"
        );
        Ok(())
    }
    fn run_consistent(
        &self,
        context: &mut ScenarioContext,
        conn: &mut crate::actors::mysql::DeadlineConnection,
        catalog: &str,
        rest: &str,
    ) -> Result<()> {
        for case in ["tamper", "missing_d", "missing_b", "unknown", "bad_record"] {
            create_mv(context, conn, case)?;
            refresh(context, conn, case)?;
            let before = load_metadata(context, rest, NS, case)?;
            let graph = Graph::from_metadata(&before)?;
            let (key, updates) = match case {
                "tamper" => {
                    let v = &graph.versions[&graph.definition];
                    let mut value = v.encoded()?;
                    value.push(' ');
                    (
                        v.key(),
                        vec![json!({"action":"set-properties","updates":{(v.key()):value}})],
                    )
                }
                "missing_d" | "missing_b" => {
                    let id = if case == "missing_d" {
                        &graph.definition
                    } else {
                        &graph.binding
                    };
                    let key = format!("novarocks.doc.v2.rev.{id}");
                    (
                        key.clone(),
                        vec![json!({"action":"remove-properties","removals":[key]})],
                    )
                }
                "unknown" => {
                    let key = "novarocks.doc.v2.unrecognized.invalid".to_string();
                    (
                        key.clone(),
                        vec![json!({"action":"set-properties","updates":{(key):"unknown"}})],
                    )
                }
                _ => {
                    let bad =
                        append_zero(context, rest, case, &before, None, BTreeMap::new(), vec![])?;
                    let id = bad["current-snapshot-id"]
                        .as_i64()
                        .context("bare snapshot missing")?;
                    let mut snapshot = bad["snapshots"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .find(|s| s["snapshot-id"].as_i64() == Some(id))
                        .unwrap()
                        .clone();
                    snapshot["snapshot-id"] = json!(id + 1);
                    snapshot["parent-snapshot-id"] = json!(id);
                    snapshot["sequence-number"] =
                        json!(bad["last-sequence-number"].as_i64().unwrap_or(0) + 1);
                    snapshot["summary"][RECORD] = json!("2.-.!");
                    let changed = commit_updates(
                        context,
                        rest,
                        NS,
                        case,
                        &bad,
                        vec![
                            json!({"action":"add-snapshot","snapshot":snapshot}),
                            json!({"action":"set-snapshot-ref","ref-name":"main","type":"branch","snapshot-id":id+1}),
                        ],
                        "malformed-current-record",
                    )?;
                    let _ = changed;
                    (RECORD.into(), vec![])
                }
            };
            if !updates.is_empty() {
                commit_updates(
                    context,
                    rest,
                    NS,
                    case,
                    &before,
                    updates,
                    "corrupt-current-closure",
                )?;
            }
            let frozen = load_metadata(context, rest, NS, case)?;
            let diagnostic_path = context
                .scenario_root()
                .join("provider-diagnostics/mv-document-consistent.WARNING");
            let prior_diagnostics = fs::read_to_string(&diagnostic_path).with_context(|| {
                format!("read original FE warning log {}", diagnostic_path.display())
            })?;
            let error = conn
                .query_drop(format!("REFRESH MATERIALIZED VIEW {case} WITH SYNC MODE"))
                .expect_err("corrupt current documents must refuse refresh")
                .to_string();
            context.action(format!("corruption {case} rejected: {error}"));
            ensure!(
                error.contains("[query.execution.invalid_data]")
                    && !error.contains("novarocks.doc."),
                "current corruption lost its public invalid-data category or exposed a private document key: {error}"
            );
            let diagnostics = fs::read_to_string(&diagnostic_path).with_context(|| {
                format!("read original FE warning log {}", diagnostic_path.display())
            })?;
            let current = diagnostics
                .strip_prefix(&prior_diagnostics)
                .context("original FE warning log rotated during one corruption request")?;
            let exact_key = format!("document_key={key:?}");
            ensure!(
                current.lines().any(|line| {
                    line.contains("Iceberg keyed document corruption")
                        && line.split_whitespace().any(|field| field == exact_key)
                        && [
                            "connector_kind=CorruptData",
                            "failure_class=Data",
                            "failure_domain=4",
                            "failure_code=9",
                        ]
                        .iter()
                        .all(|expected| line.split_whitespace().any(|field| field == *expected))
                }),
                "current corruption lost its exact original document key or typed Connector corruption fact in {}",
                diagnostic_path.display()
            );
            let after = load_metadata(context, rest, NS, case)?;
            ensure!(
                snapshot_ids(&after)? == snapshot_ids(&frozen)?
                    && after["current-snapshot-id"] == frozen["current-snapshot-id"],
                "corruption rejection committed a snapshot"
            );
        }
        create_mv(context, conn, "history")?;
        refresh(context, conn, "history")?;
        let old = load_metadata(context, rest, NS, "history")?;
        let old_graph = Graph::from_metadata(&old)?;
        repartition(context, conn, "history", 2)?;
        let current = load_metadata(context, rest, NS, "history")?;
        let missing = format!("novarocks.doc.v2.rev.{}", old_graph.interpretation);
        let broken = commit_updates(
            context,
            rest,
            NS,
            "history",
            &current,
            vec![json!({"action":"remove-properties","removals":[missing]})],
            "historical-only-version-missing",
        )?;
        let diagnostics = save_graph(context, "history-only-diagnostic", &broken)?;
        ensure!(
            !diagnostics.diagnostics.is_empty(),
            "missing old L was not classified as historical diagnostic"
        );
        assert_rows(
            context,
            conn,
            "SELECT k1,v2 FROM history ORDER BY k1",
            &[(1, 10), (2, 20)],
            "historical-only gap preserves current service",
        )?;
        let start = self.audit()?.len();
        expire(context, conn, catalog, "history", "history-diagnostic", 1)?;
        let audit = self.audit()?;
        let table_path = "/v1/namespaces/ns/tables/history";
        ensure!(
            audit[start..]
                .iter()
                .any(|r| r.method == "GET" && r.path_and_query == table_path),
            "expiry never used a Full table load"
        );
        create_mv(context, conn, "stable")?;
        for n in 0..300 {
            execute(
                context,
                conn,
                "advance source for ordinary continuous refresh",
                &format!("INSERT INTO orders VALUES ({}, {})", 1000 + n, n),
            )?;
            let start = self.audit()?.len();
            refresh(context, conn, "stable")?;
            if n == 0 || n == 299 {
                self.assert_refs_since(start, "stable")?;
            }
            if n % 25 == 0 {
                save_graph(
                    context,
                    "continuous-refresh-progress",
                    &load_metadata(context, rest, NS, "stable")?,
                )?;
            }
        }
        let stable = load_metadata(context, rest, NS, "stable")?;
        let graph = save_graph(context, "continuous-refresh-300", &stable)?;
        ensure!(
            graph.snapshots.len() == 300,
            "300 ordinary refreshes did not produce 300 publication snapshots"
        );
        let count: Option<u64> = conn.query_first("SELECT COUNT(*) FROM stable")?;
        ensure!(
            count == Some(302),
            "300-refresh output lost source rows: {count:?}"
        );
        let logical_rows = graph.record_logical_rows(&current_record(&stable)?)?;
        ensure!(
            logical_rows == count,
            "publication logical rows differ from the complete MV: P={logical_rows:?} actual={count:?}"
        );
        ensure!(
            graph.private_bytes <= 262144 && graph.record_bytes.iter().all(|b| *b <= 4096),
            "inline documents exceeded frozen limits"
        );
        let start = self.audit()?.len();
        execute(
            context,
            conn,
            "ordinary ALTER uses Refs loads",
            "ALTER MATERIALIZED VIEW stable PAUSE REFRESH",
        )?;
        self.assert_refs_since(start, "stable")?;
        let after = save_graph(
            context,
            "alter-carried-record",
            &load_metadata(context, rest, NS, "stable")?,
        )?;
        ensure!(
            after.snapshots.len() == graph.snapshots.len() + 1,
            "ALTER did not append its slot-change snapshot"
        );
        let prior = current_record(&stable)?;
        let final_meta = load_metadata(context, rest, NS, "stable")?;
        ensure!(
            current_record(&final_meta)?.frame == prior.frame,
            "ALTER did not carry the current P bytes"
        );
        let configuration = after.slots[wire::CONFIG].content_revision()?;
        execute(
            context,
            conn,
            "same-value ALTER still commits a slot snapshot",
            "ALTER MATERIALIZED VIEW stable PAUSE REFRESH",
        )?;
        let same = save_graph(
            context,
            "same-value-slot-snapshot",
            &load_metadata(context, rest, NS, "stable")?,
        )?;
        ensure!(
            same.slots[wire::CONFIG].content_revision()? == configuration
                && same.snapshots.len() == after.snapshots.len() + 1
                && same.snapshots.last().map(|(_, record)| record)
                    == after.snapshots.last().map(|(_, record)| record),
            "same-value slot change changed C revision or skipped its snapshot"
        );
        fs::write(
            artifact(context, "rest-load-mode-audit")?.join("requests.json"),
            serde_json::to_vec_pretty(&self.audit()?)?,
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn document_scenarios_are_registered_explicit_and_unique() {
        let all = super::super::all();
        for name in [
            "mv/document-expiry-channel",
            "mv/document-consistent-read",
            "mv/document-keyed-writes",
        ] {
            let matching: Vec<_> = all.iter().filter(|s| s.name() == name).collect();
            assert_eq!(matching.len(), 1);
            assert!(matching[0].is_explicit_stage());
        }
    }
    #[test]
    fn finite_expiry_grants_distinguish_tokens_on_same_mv() {
        let a = control_plan::document_expiry("one", "ice", "ns", "a", "first", None);
        let b = control_plan::document_expiry("two", "ice", "ns", "a", "second", None);
        assert_eq!(a.parameters["action"], "document-expiry");
        assert_ne!(a.parameters["token"], b.parameters["token"]);
        assert!(a.parameters.get("directory").is_none());
    }
}
