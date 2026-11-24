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

use super::mv_recovery::control_plan::{self, FeLaunchIntent};
use super::mv_uea7::{
    ManagedMvRestFixture, assert_rows, execute, full_refresh_orders, property, require_property,
    require_three_backends, select_catalog_and_namespace, status, wait_for_status_phase,
};
use super::mv_uea7::{mv_properties, mv_public_error, mv_text_lines};
use crate::actors::sql::SqlSession;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, bail, ensure};
use novarocks_cluster_harness::TestClusterOwner;
use novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan;
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

const CATALOG: &str = "uea7_mv_storage";
const NAMESPACE: &str = "ns";
const VIEW: &str = "orders_mv";

#[derive(Default)]
pub(super) struct MvStorageContract {
    fixture: Mutex<Option<ManagedMvRestFixture>>,
}

impl Scenario for MvStorageContract {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "mv_uea7_storage",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "mv/storage-contract"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let (rest, launch) = ManagedMvRestFixture::start_recorded(scenario_root, CATALOG)?;
        let barrier_dir = scenario_root.join("mv-rewrite-final-barrier");
        fs::create_dir_all(&barrier_dir).context("create final MV rewrite barrier directory")?;
        let mut fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        ensure!(
            fixture.is_none(),
            "managed MV fixture was initialized more than once"
        );
        *fixture = Some(rest);
        Ok(launch)
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        scenario_root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: Instant,
    ) -> Result<Option<ClusterTestSessionPlan>> {
        control_plan::plan(
            cluster_size,
            binary,
            original_deadline,
            vec![
                FeLaunchIntent::ObservedMvActions(vec![control_plan::rewrite(
                    "storage-final-rewrite",
                    CATALOG,
                    NAMESPACE,
                    control_plan::STORAGE_REWRITE_TOKEN,
                    &scenario_root.join("mv-rewrite-final-barrier"),
                )]),
                FeLaunchIntent::Ordinary,
            ],
        )
        .map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let (create_catalog_sql, rest_uri) = {
            let fixture = self
                .fixture
                .lock()
                .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
            let fixture = fixture
                .as_ref()
                .context("managed MV fixture is missing after cluster launch")?;
            (
                fixture.create_catalog_sql().to_owned(),
                fixture.rest_uri().to_owned(),
            )
        };

        let mut conn = connect(context)?;
        execute(
            context,
            &mut conn,
            "create private REST catalog",
            &create_catalog_sql,
        )?;
        execute(
            context,
            &mut conn,
            "create managed MV namespace",
            &format!("CREATE SCHEMA {CATALOG}.{NAMESPACE}"),
        )?;
        execute(
            context,
            &mut conn,
            "create row-lineage source table",
            &format!(
                "CREATE TABLE {CATALOG}.{NAMESPACE}.orders (k1 INT, v2 BIGINT) \
                 WITH (format_version = 3)"
            ),
        )?;
        execute(
            context,
            &mut conn,
            "insert source rows for a decisive aggregate rewrite",
            &format!(
                "INSERT INTO {CATALOG}.{NAMESPACE}.orders \
                 SELECT CASE WHEN n % 2 = 0 THEN 1 ELSE 2 END, \
                 CAST(n % 100 AS BIGINT) \
                 FROM TABLE(sequence(1, 4000)) t(n)"
            ),
        )?;
        select_catalog_and_namespace(context, &mut conn, CATALOG)?;
        execute(
            context,
            &mut conn,
            "create document-managed Iceberg MV without publishing",
            "CREATE MATERIALIZED VIEW orders_mv WITH (refresh_policy = 'MANUAL') AS SELECT k1, SUM(v2) AS total FROM orders GROUP BY k1",
        )?;
        context.action("verify CREATE wrote table-level D/L/C and no snapshot or P to REST");
        let created = load_graph(context, &rest_uri, 0)?;

        let first_commits_before = self.recorded_table_commits()?;
        execute(
            context,
            &mut conn,
            "publish the first exact MV output",
            "REFRESH MATERIALIZED VIEW orders_mv",
        )?;
        context.action("verify first P binds its exact snapshot and table-level D/L revisions");
        let first = load_graph(context, &rest_uri, 2)?;
        assert_full_refresh_pair(&created, &first, 2)?;
        ensure!(
            self.recorded_table_commits()? == first_commits_before + 1,
            "first FULL must publish both snapshots in one REST table commit"
        );
        ensure!(
            first.definition == created.definition
                && first.interpretation == created.interpretation,
            "the first publication changed the CREATE D/L revisions"
        );
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, total FROM orders_mv ORDER BY k1",
            &[(1, 98_000), (2, 100_000)],
            "read the first publication directly",
        )?;
        require_rewrite(context, &mut conn)?;
        let mut independent_reader = connect(context)?;
        select_catalog_and_namespace(context, &mut independent_reader, CATALOG)?;
        require_rewrite(context, &mut independent_reader)?;
        context.action("execute EXPLAIN ANALYZE through the final query plan");
        let analyzed: Vec<(String,)> = mv_text_lines(
            &independent_reader
                .execute("EXPLAIN ANALYZE SELECT k1, SUM(v2) FROM orders GROUP BY k1 ORDER BY k1")
                .context("execute final plan with MV rewrite candidate")?,
        )?;
        ensure!(
            analyzed
                .iter()
                .any(|(line,)| line.contains("rewritten with mv: orders_mv")),
            "EXPLAIN ANALYZE final plan did not select the MV: {analyzed:?}"
        );
        drop(independent_reader);
        require_actual_rewrite_barrier(context)?;
        drop(conn);

        context.action("restart FE while preserving the private REST catalog and its MV documents");
        let deadline = context.deadline();
        context
            .handle()
            .restart_fe_until(deadline)
            .context("restart FE after the first MV publication")?;
        let mut conn = connect(context)?;
        let closed = wait_for_status_phase(
            context,
            &mut conn,
            CATALOG,
            VIEW,
            "AWAITING_EFFECT_SETTLEMENT",
            "wait for read-only management after FE restart",
        )?;
        require_property(&closed, "UnsettledEffects", "1")?;
        assert_rows(
            context,
            &mut conn,
            &format!("SELECT k1, total FROM {CATALOG}.{NAMESPACE}.{VIEW} ORDER BY k1"),
            &[(1, 98_000), (2, 100_000)],
            "read the lake publication on the restarted FE",
        )?;
        let after_restart = load_graph(context, &rest_uri, 2)?;
        ensure!(
            after_restart == first,
            "FE restart changed the exact D/L/P attachment graph"
        );
        select_catalog_and_namespace(context, &mut conn, CATALOG)?;
        context.action("verify refresh remains closed until an operator declaration");
        let error = match conn.execute("REFRESH MATERIALIZED VIEW orders_mv") {
            Err(error) => error,
            Ok(_) => bail!(
                "restarted FE refreshed the MV before management readmission; {}",
                context.diagnostics()
            ),
        };
        let public_error = mv_public_error(&error)?;
        ensure!(
            public_error
                .message
                .contains("MV target requires a successful fresh Current observation"),
            "closed MV refresh returned an unrelated error {error}; {}",
            context.diagnostics()
        );

        let closed = status(context, &mut conn, CATALOG, VIEW)?;
        require_property(&closed, "Phase", "AWAITING_EFFECT_SETTLEMENT")?;
        let challenge = property(&closed, "Challenge")?;
        let previous_incarnation = property(&closed, "UnsettledEffect1Incarnation")?;
        context.action("readmit management through the public operator procedure");
        let resumed: Vec<(String, Option<String>)> = mv_properties(&conn
            .execute(format!(
                "CALL novarocks_mv_resume_management('{CATALOG}', '{NAMESPACE}', '{VIEW}', \
                 '{challenge}', '{previous_incarnation}', 'uea7-system-runner', \
                 'the system scenario replaced the declared frontend process before this statement')"
            ))
            .context("resume managed MV after FE restart")?)?;
        require_property(&resumed, "SettledEffects", "1")?;
        execute(
            context,
            &mut conn,
            "insert source changes after management readmission",
            "INSERT INTO orders VALUES (1, 5), (2, 7)",
        )?;
        let before_full = load_graph(context, &rest_uri, 3)?;
        let commits_before = self.recorded_table_commits()?;
        full_refresh_orders(
            context,
            &mut conn,
            "publish a second exact MV output through FULL refresh",
        )?;
        assert_rows(
            context,
            &mut conn,
            "SELECT k1, total FROM orders_mv ORDER BY k1",
            &[(1, 98_005), (2, 100_007)],
            "read the second publication directly",
        )?;
        context.action("verify FULL added an exact delete and append in one REST commit");
        let second = load_graph(context, &rest_uri, 5)?;
        assert_full_refresh_pair(&before_full, &second, 2)?;
        ensure!(
            self.recorded_table_commits()? == commits_before + 1,
            "FULL dispatched more or less than one target REST commit"
        );
        ensure!(
            second.definition == first.definition && second.interpretation == first.interpretation,
            "FULL refresh changed the stable D/L revisions"
        );
        ensure!(
            second.publications[0] == first.publications[0]
                && second.publications.last().map(|(_, revision)| revision)
                    != first.publications.last().map(|(_, revision)| revision),
            "FULL refresh did not retain the original exact P and add a distinct P"
        );
        context.action(
            "proved CREATE, exact publications, restart reads, readmission and FULL refresh",
        );
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?
            .take();
        let Some(mut fixture) = fixture else {
            return Ok(());
        };
        fixture.shutdown()
    }
}

impl MvStorageContract {
    fn recorded_table_commits(&self) -> Result<usize> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("managed MV fixture lock poisoned"))?;
        let requests = fixture
            .as_ref()
            .context("managed MV fixture is missing")?
            .recorded_requests()?;
        let target = format!("/v1/namespaces/{NAMESPACE}/tables/{VIEW}");
        Ok(requests
            .iter()
            .filter(|r| {
                r.method == "POST" && r.path_and_query.split('?').next() == Some(target.as_str())
            })
            .count())
    }
}

struct RewriteHold(PathBuf);

impl RewriteHold {
    fn create(path: PathBuf) -> Result<Self> {
        fs::write(&path, "hold\n").with_context(|| format!("create {}", path.display()))?;
        Ok(Self(path))
    }

    fn release(self) -> Result<()> {
        match fs::remove_file(&self.0) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error).with_context(|| format!("release {}", self.0.display())),
        }
    }
}

impl Drop for RewriteHold {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn require_actual_rewrite_barrier(context: &mut ScenarioContext) -> Result<()> {
    let barrier_dir = context.scenario_root().join("mv-rewrite-final-barrier");
    let marker = barrier_dir.join("mvx4-completed-mv-target-frozen.marker");
    if marker.exists() {
        fs::remove_file(&marker).context("remove stale final MV rewrite marker")?;
    }
    let hold = RewriteHold::create(barrier_dir.join("mvx4-rewrite-hold.trigger"))?;
    let query = match control_plan::ObservedQuery::start(
        context,
        CATALOG,
        NAMESPACE,
        control_plan::STORAGE_REWRITE_TOKEN,
    ) {
        Ok(query) => query,
        Err(error) => return control_plan::with_cleanup(Err(error), hold.release()),
    };
    let phase = (|| -> Result<()> {
        context.action("wait for actual query to freeze its completed MV plan and read access");
        while !marker.exists() {
            if query.is_finished() {
                bail!("actual query completed before completed MV plan freeze");
            }
            context.remaining("wait for actual completed MV plan")?;
            thread::sleep(Duration::from_millis(100));
        }
        Ok(())
    })();
    let released = hold.release();
    let rows =
        query.finish_after_release(context, phase, released, "finish actual MV rewrite query")?;
    ensure!(
        rows == [(1, 98_000), (2, 100_000)],
        "actual MV query returned {rows:?}"
    );
    context.action("actual query froze and consumed its completed MV plan");
    Ok(())
}

fn require_rewrite(context: &mut ScenarioContext, conn: &mut SqlSession) -> Result<()> {
    context.remaining("explain base aggregate rewrite")?;
    context.action("verify the base aggregate rewrites onto the published MV");
    let plan: Vec<(String,)> = mv_text_lines(
        &conn
            .execute("EXPLAIN SELECT k1, SUM(v2) FROM orders GROUP BY k1 ORDER BY k1")
            .context("explain aggregate over the MV source")?,
    )?;
    ensure!(
        plan.iter()
            .any(|(line,)| line.contains("rewritten with mv: orders_mv")),
        "published MV did not rewrite the base aggregate: {plan:?}; {}",
        context.diagnostics()
    );
    Ok(())
}

#[derive(Debug, PartialEq)]
struct DocumentGraph {
    definition: Value,
    interpretation: Value,
    publications: Vec<(i64, Value)>,
    snapshots: Vec<Value>,
    current: Option<i64>,
    next_row_id: u64,
    logical_rows: Option<u64>,
}

fn load_graph(context: &ScenarioContext, rest_uri: &str, expected: usize) -> Result<DocumentGraph> {
    use super::mv_uea7_documents::{load_metadata, wire::Graph};
    use sha2::{Digest, Sha256};
    let metadata = load_metadata(context, rest_uri, NAMESPACE, VIEW)?;
    let graph = Graph::from_metadata(&metadata)?;
    ensure!(
        graph.snapshots.len() == expected,
        "expected {expected} MV snapshots, observed {}",
        graph.snapshots.len()
    );
    ensure!(
        graph.diagnostics.is_empty(),
        "MV storage contract history is not interpretable: {:?}",
        graph.diagnostics
    );
    let publications = graph
        .snapshots
        .iter()
        .filter_map(|(id, record)| record.as_ref().map(|record| (*id, record)))
        .map(|(id, record)| {
            let revision: String = Sha256::digest(record.frame.as_bytes())
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect();
            (id, Value::String(revision))
        })
        .collect::<Vec<_>>();
    ensure!(
        graph.current == publications.last().map(|(id, _)| *id),
        "current snapshot does not carry the latest P record"
    );
    let logical_rows = graph
        .snapshots
        .iter()
        .find(|(id, _)| Some(*id) == graph.current)
        .and_then(|(_, record)| record.as_ref())
        .map(|record| graph.record_logical_rows(record))
        .transpose()?
        .flatten();
    let mut snapshots = metadata["snapshots"]
        .as_array()
        .context("managed MV metadata has no snapshots")?
        .clone();
    snapshots.sort_by_key(|snapshot| snapshot["sequence-number"].as_i64());
    Ok(DocumentGraph {
        definition: Value::String(graph.definition),
        interpretation: Value::String(graph.interpretation),
        publications,
        current: graph.current,
        next_row_id: metadata["next-row-id"]
            .as_u64()
            .context("row-lineage MV metadata has no next row ID")?,
        logical_rows,
        snapshots,
    })
}

fn assert_full_refresh_pair(
    before: &DocumentGraph,
    after: &DocumentGraph,
    rows: u64,
) -> Result<()> {
    use super::mv_uea7_documents::wire::RECORD;
    const SESSION_MARKER: &str = "novarocks.write.session.v1";

    let retained = before.snapshots.len();
    ensure!(
        after.snapshots.len() == retained + 2 && after.snapshots.starts_with(&before.snapshots),
        "FULL did not retain the original snapshots and add exactly two more"
    );
    let delete = &after.snapshots[retained];
    let append = &after.snapshots[retained + 1];
    ensure!(
        delete["summary"]["operation"] == "delete"
            && append["summary"]["operation"] == "append"
            && delete["parent-snapshot-id"].as_i64() == before.current
            && append["parent-snapshot-id"] == delete["snapshot-id"]
            && append["snapshot-id"].as_i64() == after.current
            && delete["sequence-number"]
                .as_i64()
                .and_then(|seq| seq.checked_add(1))
                == append["sequence-number"].as_i64(),
        "FULL did not publish consecutive delete and append snapshots rooted at its original main"
    );
    let delete_summary = delete["summary"]
        .as_object()
        .context("FULL delete summary missing")?;
    ensure!(
        ["total-records", "total-data-files", "total-delete-files"]
            .iter()
            .all(|key| delete["summary"][*key].as_str() == Some("0"))
            && delete_summary
                .keys()
                .all(|key| !key.starts_with("novarocks.")),
        "FULL delete must be empty and have only standard summary fields"
    );
    let append_summary = &append["summary"];
    let session = append_summary[SESSION_MARKER]
        .as_str()
        .context("FULL append session missing")?;
    ensure!(
        !session.is_empty()
            && after
                .snapshots
                .iter()
                .filter(|s| s["summary"][SESSION_MARKER].as_str() == Some(session))
                .count()
                == 1
            && append_summary[RECORD].as_str().is_some()
            && before.publications.len() + 1 == after.publications.len()
            && after.publications.starts_with(&before.publications),
        "FULL did not add one exact document publication and one uniquely marked append"
    );
    ensure!(
        append_summary["total-records"]
            .as_str()
            .and_then(|s| s.parse::<u64>().ok())
            == Some(rows)
            && after.logical_rows == Some(rows)
            && append["first-row-id"].as_u64() == Some(before.next_row_id)
            && append["added-rows"].as_u64() == Some(rows)
            && before.next_row_id.checked_add(rows) == Some(after.next_row_id),
        "FULL append did not project the exact logical and physical row totals and lineage"
    );
    Ok(())
}

fn connect(context: &mut ScenarioContext) -> Result<SqlSession> {
    SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())
        .map_err(Into::into)
}
