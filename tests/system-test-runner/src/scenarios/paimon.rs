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

//! Native Paimon product acceptance against the external Spark/Paimon oracle.
//!
//! Every scenario is explicit because the checked-in fixture definition uses
//! Docker, Spark and MinIO. Read-only scenarios consume the manifest prepared
//! by CI through `NOVAROCKS_PAIMON_FIXTURE_MANIFEST`. The snapshot scenario
//! owns a separate staged fixture because advancing S1 to S2 is its subject.

use super::connector::{
    await_resource_convergence, connector_launch_config, create_catalog, create_warehouse,
    mysql_endpoint, require_three_backends, resource_baseline,
};
use super::task_evidence::assert_query_completed_across_boundary;
use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, bail, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::ServerHandle;
use serde_json::Value;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, mpsc};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const FIXTURE_MANIFEST_ENV: &str = "NOVAROCKS_PAIMON_FIXTURE_MANIFEST";
const FIXTURE_ENV_FILE_ENV: &str = "NOVA_ENV_REST_ENV_FILE";
const FIXTURE_KIND: &str = "novarocks-paimon-read-v1";
const SPLIT_ACCEPTED: &str = "NOVAROCKS_TASK_SPLIT_ASSIGNMENT_ACCEPTED";
const SPLIT_NO_MORE: &str = "NOVAROCKS_TASK_SPLIT_NO_MORE";
const PAGE_SOURCE_OPEN: &str = "NOVAROCKS_CONNECTOR_PAGE_SOURCE_OPEN";
const PAGE_SOURCE_CLOSE: &str = "NOVAROCKS_CONNECTOR_PAGE_SOURCE_CLOSE";

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(PaimonReadData),
        Box::new(PaimonSnapshotAndSchema::default()),
        Box::new(PaimonMergeFilterCorrectness),
        Box::new(PaimonCancelAndBudget),
        Box::new(PaimonUnsupportedAndBinding),
        Box::new(IcebergPaimonRead),
    ]
}

struct PaimonReadData;

impl Scenario for PaimonReadData {
    fn name(&self) -> &'static str {
        "connector/paimon-read-data"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn launch_config(&self, _scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        Ok(connector_launch_config())
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let fixture = load_shared_fixture("schema")?;
        verify_fixture(&fixture.root)?;
        let baseline = resource_baseline(context)?;
        let before = backend_logs(context)?;
        let mut control = connect(context, "connect Paimon read-data control session")?;
        const CATALOG: &str = "paimon_read_data";
        create_paimon_catalog(&mut control, CATALOG, &fixture)?;

        let codec_rows: Vec<(i64, String)> = control
            .query(format!(
                "SELECT id, value FROM {CATALOG}.fixture.append_none \
                 UNION ALL SELECT id, value FROM {CATALOG}.fixture.append_snappy \
                 UNION ALL SELECT id, value FROM {CATALOG}.fixture.append_zstd \
                 UNION ALL SELECT id, value FROM {CATALOG}.fixture.append_lz4 \
                 ORDER BY id"
            ))
            .context("read Paimon append tables across supported codecs")?;
        ensure!(
            codec_rows
                == [
                    (1, "none-a".to_string()),
                    (2, "none-b".to_string()),
                    (3, "none-c".to_string()),
                    (10, "snappy".to_string()),
                    (20, "zstd".to_string()),
                    (30, "lz4-raw".to_string()),
                ],
            "Paimon append/codec rows differ from the external oracle: {codec_rows:?}"
        );

        let partitioned: Vec<(String, i64, String)> = control
            .query(format!(
                "SELECT part, id, value FROM {CATALOG}.fixture.append_partitioned \
                 ORDER BY part, id"
            ))
            .context("read partitioned Paimon append table")?;
        ensure!(
            partitioned
                == [
                    ("east".to_string(), 1, "e1".to_string()),
                    ("east".to_string(), 2, "e2".to_string()),
                    ("east".to_string(), 4, "e4".to_string()),
                    ("north".to_string(), 5, "n5".to_string()),
                    ("west".to_string(), 3, "w3".to_string()),
                ],
            "partitioned Paimon rows differ from the external oracle: {partitioned:?}"
        );

        let empty: Vec<i64> = control
            .query(format!(
                "SELECT count(*) FROM {CATALOG}.fixture.empty_append"
            ))
            .context("read empty Paimon append table")?;
        ensure!(empty == [0], "empty Paimon table returned {empty:?}");

        let typed: Vec<(i64, Option<String>, Option<String>, Option<String>)> = control
            .query(format!(
                "SELECT id, CAST(c_decimal AS STRING), HEX(c_binary), \
                 CAST(c_timestamp AS STRING) FROM {CATALOG}.fixture.type_matrix ORDER BY id"
            ))
            .context("read Paimon scalar type matrix")?;
        ensure!(
            typed
                == [
                    (
                        1,
                        Some("12345678901234567890123456789.123456789".to_string()),
                        Some("00FF10".to_string()),
                        Some("2024-02-29 12:34:56.123456".to_string()),
                    ),
                    (2, None, None, None),
                ],
            "Paimon scalar type rows differ from the external oracle: {typed:?}"
        );

        wait_for_distributed_reader_lifecycle(context, &before)?;
        await_resource_convergence(context, &baseline, "Paimon read-data queries")?;
        drop_catalog(&mut control, CATALOG)?;
        Ok(())
    }
}

#[derive(Default)]
struct PaimonSnapshotAndSchema {
    fixture: Mutex<Option<OwnedFixture>>,
}

impl Scenario for PaimonSnapshotAndSchema {
    fn name(&self) -> &'static str {
        "connector/paimon-snapshot-and-schema"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        let fixture = prepare_owned_fixture(scenario_root, "s1")?;
        *self.fixture.lock().expect("Paimon fixture mutex poisoned") = Some(fixture);
        Ok(connector_launch_config())
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let owned = self
            .fixture
            .lock()
            .expect("Paimon fixture mutex poisoned")
            .clone()
            .context("snapshot scenario has no prepared S1 fixture")?;
        let fixture = load_fixture(&owned.root.join("manifest.json"), "s1")?;
        let baseline = resource_baseline(context)?;
        let before = backend_logs(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = connect(context, "connect Paimon snapshot control session")?;
        const CATALOG: &str = "paimon_snapshot";
        create_paimon_catalog(&mut control, CATALOG, &fixture)?;

        let held = start_query(
            &user,
            port,
            context.remaining("connect S1 Paimon query actor")?,
            format!("SELECT count(*) FROM {CATALOG}.fixture.append_none WHERE sleep(90)"),
        )?;
        wait_for_new_marker(context, &before, PAGE_SOURCE_OPEN, &held.done)?;
        context.action("observed the S1 Paimon page source before external S2 publication");

        let (advance_tx, advance_done) = mpsc::sync_channel(1);
        let advance_fixture = owned.clone();
        let advance_thread = thread::spawn(move || -> Result<()> {
            advance_tx
                .send(advance_owned_fixture(&advance_fixture, "s2"))
                .context("publish S2 fixture advance result")?;
            Ok(())
        });
        wait_for_owned_fixture_stage(context, &owned.root, "s2", &advance_done)?;
        ensure!(
            matches!(held.done.try_recv(), Err(mpsc::TryRecvError::Empty)),
            "S1 query reached terminal before the S2 snapshot was published"
        );
        advance_done
            .recv_timeout(context.remaining("finish S2 fixture verification")?)
            .context("S2 fixture verification did not finish before the scenario deadline")??;
        advance_thread
            .join()
            .map_err(|_| anyhow::anyhow!("S2 fixture advance actor panicked"))??;
        let s1 = held
            .done
            .recv_timeout(context.remaining("await frozen S1 query")?)
            .context("frozen S1 query did not complete before the scenario deadline")?
            .context("frozen S1 query failed")?;
        held.thread
            .join()
            .map_err(|_| anyhow::anyhow!("S1 Paimon query actor panicked"))??;
        ensure!(
            s1 == [2],
            "in-flight S1 query observed another snapshot: {s1:?}"
        );

        let s2: Vec<i64> = control
            .query(format!(
                "SELECT id FROM {CATALOG}.fixture.append_none ORDER BY id"
            ))
            .context("read newly published Paimon S2")?;
        ensure!(s2 == [1, 2, 3], "fresh query did not observe S2: {s2:?}");

        advance_owned_fixture(&owned, "schema")?;
        let evolved: Vec<(i64, String, Option<String>)> = control
            .query(format!(
                "SELECT id, name, added FROM {CATALOG}.fixture.schema_evolution ORDER BY id"
            ))
            .context("read current Paimon schema over historical files")?;
        ensure!(
            evolved
                == [
                    (1, "after-a".to_string(), Some("added-a".to_string())),
                    (2, "before-b".to_string(), None),
                    (3, "after-c".to_string(), Some("added-c".to_string())),
                ],
            "Paimon field-ID schema evolution differs from the oracle: {evolved:?}"
        );

        await_resource_convergence(context, &baseline, "Paimon snapshot/schema reads")?;
        drop_catalog(&mut control, CATALOG)?;
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .expect("Paimon fixture mutex poisoned")
            .take();
        if let Some(fixture) = fixture {
            cleanup_owned_fixture(&fixture)?;
        }
        Ok(())
    }
}

struct PaimonMergeFilterCorrectness;

impl Scenario for PaimonMergeFilterCorrectness {
    fn name(&self) -> &'static str {
        "connector/paimon-merge-filter-correctness"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn launch_config(&self, _scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        Ok(connector_launch_config())
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let fixture = load_shared_fixture("schema")?;
        let baseline = resource_baseline(context)?;
        let mut control = connect(context, "connect Paimon merge control session")?;
        const CATALOG: &str = "paimon_merge";
        create_paimon_catalog(&mut control, CATALOG, &fixture)?;

        let compacted: Vec<(i64, i64, String)> = control
            .query(format!(
                "SELECT id, metric, note FROM {CATALOG}.fixture.pk_default ORDER BY id"
            ))
            .context("read compacted deduplicate Paimon table")?;
        ensure!(
            compacted
                == [
                    (1, 21, "new-a".to_string()),
                    (2, 22, "new-b".to_string()),
                    (4, 40, "new-d".to_string()),
                ],
            "compacted deduplicate result differs from the oracle: {compacted:?}"
        );

        let uncompact: Vec<(i64, i64, String)> = control
            .query(format!(
                "SELECT id, metric, note FROM {CATALOG}.fixture.pk_dynamic ORDER BY id"
            ))
            .context("read uncompact dynamic-bucket deduplicate Paimon table")?;
        ensure!(
            uncompact
                == [
                    (101, 1111, "d101-new".to_string()),
                    (102, 1020, "d102".to_string()),
                    (103, 1030, "d103".to_string()),
                    (104, 1040, "d104".to_string()),
                    (105, 1050, "d105".to_string()),
                ],
            "uncompact deduplicate result differs from the oracle: {uncompact:?}"
        );

        let sequence: Vec<(i64, i64, String)> = control
            .query(format!(
                "SELECT id, seq, value FROM {CATALOG}.fixture.pk_sequence ORDER BY id"
            ))
            .context("read sequence-field Paimon table")?;
        ensure!(
            sequence
                == [
                    (1, 11, "seq-11".to_string()),
                    (3, 1, "seq-1".to_string()),
                    (4, 7, "equal-second".to_string()),
                ],
            "sequence-field winners differ from the oracle: {sequence:?}"
        );

        let filtered: Vec<(i64, i64)> = control
            .query(format!(
                "SELECT id, metric FROM {CATALOG}.fixture.pk_dynamic \
                 WHERE (id = 101 OR id = 105) AND metric >= 1050 ORDER BY id LIMIT 2"
            ))
            .context("apply engine predicate and limit after Paimon merge")?;
        ensure!(
            filtered == [(101, 1111), (105, 1050)],
            "filter/limit changed merged Paimon rows: {filtered:?}"
        );
        let deleted: Vec<i64> = control
            .query(format!(
                "SELECT count(*) FROM {CATALOG}.fixture.pk_all_deleted"
            ))
            .context("read all-deleted Paimon key group")?;
        ensure!(
            deleted == [0],
            "deleted Paimon keys were resurrected: {deleted:?}"
        );

        await_resource_convergence(context, &baseline, "Paimon merge/filter reads")?;
        drop_catalog(&mut control, CATALOG)?;
        Ok(())
    }
}

struct PaimonCancelAndBudget;

impl Scenario for PaimonCancelAndBudget {
    fn name(&self) -> &'static str {
        "connector/paimon-cancel-and-budget"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn launch_config(&self, _scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        Ok(connector_launch_config())
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let fixture = load_shared_fixture("schema")?;
        let baseline = resource_baseline(context)?;
        let before = backend_logs(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = connect(context, "connect Paimon cancellation control session")?;
        const CATALOG: &str = "paimon_cancel";
        create_paimon_catalog(&mut control, CATALOG, &fixture)?;

        let target = start_query(
            &user,
            port,
            context.remaining("connect cancellable Paimon query actor")?,
            format!("SELECT count(*) FROM {CATALOG}.fixture.pk_dynamic WHERE sleep(60)"),
        )?;
        let connection_id = target
            .ready
            .recv_timeout(context.remaining("receive cancellable Paimon connection id")?)
            .context("cancellable Paimon query did not publish its connection ID")?;
        wait_for_new_marker(context, &before, PAGE_SOURCE_OPEN, &target.done)?;
        control
            .query_drop(format!("KILL QUERY {connection_id}"))
            .context("cancel active Paimon query through public MySQL")?;
        let error = target
            .done
            .recv_timeout(context.remaining("await Paimon query cancellation")?)
            .context("cancelled Paimon query did not terminate")?
            .expect_err("cancelled Paimon query unexpectedly succeeded");
        ensure!(
            matches!(error, mysql::Error::MySqlError(ref error) if error.code == 1317),
            "expected MySQL cancellation error 1317, received {error}"
        );
        target
            .thread
            .join()
            .map_err(|_| anyhow::anyhow!("Paimon cancellation query actor panicked"))??;

        let budget_error = control
            .query::<mysql::Row, _>(format!(
                "SELECT /*+ SET_VAR(query_mem_limit=1) */ * \
                 FROM {CATALOG}.fixture.type_matrix"
            ))
            .expect_err("one-byte query memory budget must reject the Paimon read");
        ensure!(
            budget_error
                .to_string()
                .to_ascii_lowercase()
                .contains("memory"),
            "low-budget Paimon read failed for another reason: {budget_error}"
        );

        wait_for_balanced_page_sources(context, &before)?;
        await_resource_convergence(context, &baseline, "Paimon cancellation and budget refusal")?;
        drop_catalog(&mut control, CATALOG)?;
        Ok(())
    }
}

struct PaimonUnsupportedAndBinding;

impl Scenario for PaimonUnsupportedAndBinding {
    fn name(&self) -> &'static str {
        "connector/paimon-unsupported-and-binding"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn launch_config(&self, _scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        Ok(connector_launch_config())
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let fixture = load_shared_fixture("schema")?;
        verify_fixture(&fixture.root)?;
        let mut control = connect(context, "connect Paimon rejection control session")?;
        const CATALOG: &str = "paimon_reject";
        create_paimon_catalog(&mut control, CATALOG, &fixture)?;

        expect_error_any(
            control.query_drop(format!(
                "INSERT INTO {CATALOG}.fixture.pk_default VALUES (9, 9, 'forbidden')"
            )),
            &[
                "no distributed write capability",
                "no active mutation binding",
            ],
            "Paimon INSERT",
        )?;
        expect_error_any(
            control.query_drop(format!("CREATE TABLE {CATALOG}.fixture.forbidden (id INT)")),
            &[
                "no catalog mutation capability",
                "no active mutation binding",
                "does not support",
            ],
            "Paimon CREATE TABLE",
        )?;
        expect_error_any(
            control.query_drop(format!("ALTER TABLE {CATALOG}.fixture.pk_default OPTIMIZE")),
            &[
                "no metadata maintenance capability",
                "no active metadata maintenance binding",
            ],
            "Paimon OPTIMIZE",
        )?;
        expect_error_any(
            control.query_drop(format!("ANALYZE TABLE {CATALOG}.fixture.pk_default")),
            &["no statistics capability", "no active statistics binding"],
            "Paimon ANALYZE",
        )?;

        for (table, expected) in [
            (
                "unsupported_orc",
                "only paimon parquet data files are supported",
            ),
            (
                "unsupported_avro_data",
                "only paimon parquet data files are supported",
            ),
            ("unsupported_dv", "paimon deletion vectors are unsupported"),
            (
                "unsupported_aggregation",
                "only paimon merge-engine=deduplicate is supported",
            ),
            ("unsupported_nested", "paimon column type is unsupported"),
            (
                "unsupported_timestamp_ltz",
                "paimon column type is unsupported",
            ),
            (
                "unsupported_multi_sequence",
                "multiple paimon sequence fields are unsupported",
            ),
            (
                "unsupported_postpone",
                "paimon postpone bucket mode is unsupported",
            ),
        ] {
            expect_error_any(
                control.query::<mysql::Row, _>(format!("SELECT * FROM {CATALOG}.fixture.{table}")),
                &[expected],
                table,
            )?;
        }

        expect_error_any(
            control.query_drop(format!(
                "CREATE EXTERNAL CATALOG paimon_bad_binding PROPERTIES(\
                 \"type\"=\"paimon\",\
                 \"paimon.catalog.type\"=\"rest\",\
                 \"warehouse\"=\"{}\")",
                sql_string(&fixture.warehouse)
            )),
            &["paimon.catalog.type must be filesystem"],
            "invalid Paimon catalog binding",
        )?;

        verify_fixture(&fixture.root)?;
        drop_catalog(&mut control, CATALOG)?;
        Ok(())
    }
}

struct IcebergPaimonRead;

impl Scenario for IcebergPaimonRead {
    fn name(&self) -> &'static str {
        "connector/iceberg-paimon-read"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn launch_config(&self, _scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        Ok(connector_launch_config())
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let fixture = load_shared_fixture("schema")?;
        let baseline = resource_baseline(context)?;
        let mut control = connect(context, "connect Iceberg/Paimon control session")?;
        const PAIMON: &str = "paimon_cross";
        const ICEBERG: &str = "iceberg_cross";
        create_paimon_catalog(&mut control, PAIMON, &fixture)?;
        let warehouse = create_warehouse(context, "iceberg-paimon-read")?;
        create_catalog(&mut control, ICEBERG, &warehouse)?;
        control
            .query_drop(format!("CREATE DATABASE {ICEBERG}.fixture"))
            .context("create Iceberg cross-provider database")?;
        control
            .query_drop(format!(
                "CREATE TABLE {ICEBERG}.fixture.dim (id BIGINT, label STRING)"
            ))
            .context("create Iceberg cross-provider table")?;
        control
            .query_drop(format!(
                "INSERT INTO {ICEBERG}.fixture.dim VALUES (1, 'ice-a'), (4, 'ice-d'), (9, 'ice-x')"
            ))
            .context("write Iceberg cross-provider rows")?;

        let joined: Vec<(i64, String, String)> = control
            .query(format!(
                "SELECT p.id, p.note, i.label \
                 FROM {PAIMON}.fixture.pk_default p \
                 JOIN {ICEBERG}.fixture.dim i ON p.id = i.id ORDER BY p.id"
            ))
            .context("join Paimon and Iceberg through one native query")?;
        ensure!(
            joined
                == [
                    (1, "new-a".to_string(), "ice-a".to_string()),
                    (4, "new-d".to_string(), "ice-d".to_string()),
                ],
            "cross-provider join returned {joined:?}"
        );

        let snapshot = context
            .handle()
            .query_lifecycle_structured_snapshot()?
            .context("cross-provider query published no lifecycle snapshot")?;
        assert_query_completed_across_boundary(context, &snapshot, "Iceberg/Paimon join")?;
        await_resource_convergence(context, &baseline, "Iceberg/Paimon join")?;
        control
            .query_drop(format!("DROP TABLE {ICEBERG}.fixture.dim FORCE"))
            .context("drop Iceberg cross-provider table")?;
        drop_catalog(&mut control, ICEBERG)?;
        drop_catalog(&mut control, PAIMON)?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct Fixture {
    root: PathBuf,
    warehouse: String,
    endpoint: String,
    region: String,
    credential_name: String,
    credential_generation: String,
}

#[derive(Clone, Debug)]
struct OwnedFixture {
    root: PathBuf,
    run_id: String,
    env_file: PathBuf,
}

struct QueryActor {
    ready: mpsc::Receiver<u32>,
    done: mpsc::Receiver<std::result::Result<Vec<i64>, mysql::Error>>,
    thread: thread::JoinHandle<Result<()>>,
}

fn connect(context: &ScenarioContext, operation: &str) -> Result<mysql::Conn> {
    mysql_actor::connect(
        context.mysql_user(),
        context.mysql_port(),
        context.remaining(operation)?,
    )
}

fn load_shared_fixture(expected_stage: &str) -> Result<Fixture> {
    let path = env::var(FIXTURE_MANIFEST_ENV).with_context(|| {
        format!(
            "{FIXTURE_MANIFEST_ENV} must name the manifest produced by docker/paimon-read/prepare.sh"
        )
    })?;
    load_fixture(Path::new(&path), expected_stage)
}

fn load_fixture(path: &Path, expected_stage: &str) -> Result<Fixture> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("read Paimon fixture manifest {}", path.display()))?;
    let value: Value = serde_json::from_str(&raw)
        .with_context(|| format!("decode Paimon fixture manifest {}", path.display()))?;
    ensure!(
        value.get("fixture_kind").and_then(Value::as_str) == Some(FIXTURE_KIND),
        "{} is not a PAI-1 fixture manifest",
        path.display()
    );
    ensure!(
        value.get("last_stage").and_then(Value::as_str) == Some(expected_stage),
        "{} has stage {:?}, expected {expected_stage}",
        path.display(),
        value.get("last_stage")
    );
    ensure!(
        value.get("secret_scan").and_then(Value::as_str) == Some("passed"),
        "Paimon fixture did not pass its secret scan"
    );
    let catalog = value
        .get("catalog")
        .and_then(Value::as_object)
        .context("Paimon fixture manifest has no catalog object")?;
    ensure!(
        catalog.get("type").and_then(Value::as_str) == Some("filesystem"),
        "Paimon fixture is not a Filesystem Catalog"
    );
    let bindings = catalog
        .get("credential_bindings")
        .and_then(Value::as_object)
        .context("Paimon fixture has no credential bindings")?;
    let metadata_binding = bindings
        .get("object_store_metadata")
        .and_then(Value::as_object)
        .context("Paimon fixture has no object-store metadata binding")?;
    let data_binding = bindings
        .get("object_store_data")
        .and_then(Value::as_object)
        .context("Paimon fixture has no object-store data binding")?;
    ensure!(
        metadata_binding
            .get("consumer_role")
            .and_then(Value::as_str)
            == Some("frontend"),
        "Paimon metadata credential binding is not frontend-owned"
    );
    ensure!(
        data_binding.get("consumer_role").and_then(Value::as_str) == Some("backend"),
        "Paimon data credential binding is not backend-owned"
    );
    let credential_name = required_map_string(metadata_binding, "name")?;
    let credential_generation = required_map_string(metadata_binding, "generation")?;
    ensure!(
        required_map_string(data_binding, "name")? == credential_name
            && required_map_string(data_binding, "generation")? == credential_generation,
        "Paimon metadata and data bindings must use the same fixture credential generation"
    );
    let root = path
        .parent()
        .context("Paimon fixture manifest has no parent directory")?
        .to_path_buf();
    ensure!(
        root.join("READY").is_file(),
        "Paimon fixture READY is missing"
    );
    Ok(Fixture {
        root,
        warehouse: required_string(&value, "warehouse_uri")?,
        endpoint: required_map_string(catalog, "endpoint")?,
        region: required_map_string(catalog, "region")?,
        credential_name,
        credential_generation,
    })
}

fn required_string(value: &Value, key: &str) -> Result<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .with_context(|| format!("Paimon fixture manifest is missing {key}"))
}

fn required_map_string(value: &serde_json::Map<String, Value>, key: &str) -> Result<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .with_context(|| format!("Paimon fixture manifest is missing catalog.{key}"))
}

fn create_paimon_catalog(control: &mut mysql::Conn, name: &str, fixture: &Fixture) -> Result<()> {
    control
        .query_drop(format!("DROP CATALOG IF EXISTS {name}"))
        .with_context(|| format!("remove stale Paimon catalog {name}"))?;
    control
        .query_drop(format!(
            "CREATE EXTERNAL CATALOG {name} PROPERTIES(\
             \"type\"=\"paimon\",\
             \"paimon.catalog.type\"=\"filesystem\",\
             \"warehouse\"=\"{}\",\
             \"aws.s3.endpoint\"=\"{}\",\
             \"aws.s3.region\"=\"{}\",\
             \"aws.s3.enable_path_style_access\"=\"true\",\
             \"credential.object-store-metadata.consumer-role\"=\"frontend\",\
             \"credential.object-store-metadata.mode\"=\"static\",\
             \"credential.object-store-metadata.name\"=\"{}\",\
             \"credential.object-store-metadata.generation\"=\"{}\",\
             \"credential.object-store-data.consumer-role\"=\"backend\",\
             \"credential.object-store-data.mode\"=\"static\",\
             \"credential.object-store-data.name\"=\"{}\",\
             \"credential.object-store-data.generation\"=\"{}\")",
            sql_string(&fixture.warehouse),
            sql_string(&fixture.endpoint),
            sql_string(&fixture.region),
            sql_string(&fixture.credential_name),
            sql_string(&fixture.credential_generation),
            sql_string(&fixture.credential_name),
            sql_string(&fixture.credential_generation),
        ))
        .with_context(|| format!("create Filesystem Paimon catalog {name}"))
}

fn drop_catalog(control: &mut mysql::Conn, name: &str) -> Result<()> {
    control
        .query_drop(format!("DROP CATALOG IF EXISTS {name}"))
        .with_context(|| format!("drop catalog {name}"))
}

fn sql_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn verify_fixture(root: &Path) -> Result<()> {
    let output = Command::new("python3")
        .arg(fixture_script())
        .arg("verify")
        .arg("--output-dir")
        .arg(root)
        .output()
        .with_context(|| format!("run Paimon fixture verification for {}", root.display()))?;
    ensure!(
        output.status.success(),
        "Paimon fixture verification failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

fn prepare_owned_fixture(scenario_root: &Path, stage: &str) -> Result<OwnedFixture> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let fixture = OwnedFixture {
        root: scenario_root.join("paimon-fixture"),
        run_id: format!("pai1-snapshot-{}-{now:x}", std::process::id()),
        env_file: fixture_env_file(),
    };
    advance_owned_fixture(&fixture, stage)?;
    Ok(fixture)
}

fn advance_owned_fixture(fixture: &OwnedFixture, stage: &str) -> Result<()> {
    let output = Command::new(fixture_prepare_script())
        .arg("--env-file")
        .arg(&fixture.env_file)
        .arg("--run-id")
        .arg(&fixture.run_id)
        .arg("--output-dir")
        .arg(&fixture.root)
        .arg("--stop-after")
        .arg(stage)
        .output()
        .with_context(|| format!("prepare Paimon {stage} fixture"))?;
    ensure!(
        output.status.success(),
        "prepare Paimon {stage} fixture failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

fn wait_for_owned_fixture_stage(
    context: &ScenarioContext,
    root: &Path,
    stage: &str,
    advance_done: &mpsc::Receiver<Result<()>>,
) -> Result<()> {
    let state_path = root.join("state.json");
    loop {
        if let Ok(contents) = fs::read(&state_path)
            && serde_json::from_slice::<Value>(&contents)
                .ok()
                .and_then(|state| {
                    state
                        .get("last_stage")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
                .as_deref()
                == Some(stage)
        {
            return Ok(());
        }
        match advance_done.try_recv() {
            Ok(result) => {
                result?;
                bail!("Paimon fixture advance completed without publishing stage {stage}");
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                bail!("Paimon fixture advance actor disconnected before stage {stage}")
            }
            Err(mpsc::TryRecvError::Empty) => {}
        }
        let remaining = context.remaining("wait for Paimon snapshot publication")?;
        thread::sleep(remaining.min(Duration::from_millis(50)));
    }
}

fn cleanup_owned_fixture(fixture: &OwnedFixture) -> Result<()> {
    let output = Command::new(fixture_cleanup_script())
        .arg("--env-file")
        .arg(&fixture.env_file)
        .arg("--run-id")
        .arg(&fixture.run_id)
        .arg("--output-dir")
        .arg(&fixture.root)
        .output()
        .context("clean Paimon snapshot fixture")?;
    ensure!(
        output.status.success(),
        "clean Paimon snapshot fixture failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docker/paimon-read")
}

fn fixture_script() -> PathBuf {
    fixture_root().join("fixture.py")
}

fn fixture_prepare_script() -> PathBuf {
    fixture_root().join("prepare.sh")
}

fn fixture_cleanup_script() -> PathBuf {
    fixture_root().join("cleanup.sh")
}

fn fixture_env_file() -> PathBuf {
    env::var_os(FIXTURE_ENV_FILE_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../docker/iceberg-rest/runtime/current/env.sh")
        })
}

fn backend_logs(context: &mut ScenarioContext) -> Result<Vec<String>> {
    (0..context.handle().be_count())
        .map(|index| context.handle().be_current_log_contents(index))
        .collect::<Result<Vec<_>>>()
        .context("read Paimon scenario Backend logs")
}

fn appended_logs(context: &mut ScenarioContext, before: &[String]) -> Result<Vec<String>> {
    backend_logs(context)?
        .into_iter()
        .zip(before)
        .enumerate()
        .map(|(index, (log, old))| {
            log.get(old.len()..)
                .map(str::to_string)
                .with_context(|| format!("BE[{index}] log was truncated during Paimon scenario"))
        })
        .collect()
}

fn wait_for_new_marker(
    context: &mut ScenarioContext,
    before: &[String],
    marker: &str,
    done: &mpsc::Receiver<std::result::Result<Vec<i64>, mysql::Error>>,
) -> Result<()> {
    loop {
        match done.try_recv() {
            Ok(result) => bail!("Paimon query reached terminal before {marker}: {result:?}"),
            Err(mpsc::TryRecvError::Disconnected) => {
                bail!("Paimon query actor disconnected before {marker}")
            }
            Err(mpsc::TryRecvError::Empty) => {}
        }
        if appended_logs(context, before)?
            .iter()
            .any(|log| log.contains(marker))
        {
            return Ok(());
        }
        let remaining = context.remaining(&format!("observe {marker} for Paimon query"))?;
        thread::sleep(remaining.min(Duration::from_millis(50)));
    }
}

fn wait_for_distributed_reader_lifecycle(
    context: &mut ScenarioContext,
    before: &[String],
) -> Result<()> {
    loop {
        let logs = appended_logs(context, before)?;
        let accepting = logs
            .iter()
            .filter(|log| log.contains(SPLIT_ACCEPTED))
            .count();
        let balanced = logs.iter().all(|log| {
            let opens = log.matches(PAGE_SOURCE_OPEN).count();
            let closes = log.matches(PAGE_SOURCE_CLOSE).count();
            opens == closes
        });
        let terminal = logs
            .iter()
            .filter(|log| log.contains(SPLIT_ACCEPTED))
            .all(|log| log.contains(SPLIT_NO_MORE));
        if accepting >= 2 && balanced && terminal {
            return Ok(());
        }
        let remaining = context.remaining("observe distributed Paimon split/page lifecycle")?;
        thread::sleep(remaining.min(Duration::from_millis(50)));
    }
}

fn wait_for_balanced_page_sources(context: &mut ScenarioContext, before: &[String]) -> Result<()> {
    loop {
        let logs = appended_logs(context, before)?;
        let opens = logs
            .iter()
            .map(|log| log.matches(PAGE_SOURCE_OPEN).count())
            .sum::<usize>();
        let closes = logs
            .iter()
            .map(|log| log.matches(PAGE_SOURCE_CLOSE).count())
            .sum::<usize>();
        if opens > 0 && opens == closes {
            return Ok(());
        }
        let remaining = context.remaining("observe balanced Paimon page-source lifecycle")?;
        thread::sleep(remaining.min(Duration::from_millis(50)));
    }
}

fn start_query(
    user: &str,
    port: u16,
    connect_timeout: Duration,
    query: String,
) -> Result<QueryActor> {
    let (ready_tx, ready) = mpsc::sync_channel(1);
    let (done_tx, done) = mpsc::sync_channel(1);
    let user = user.to_string();
    let thread = thread::spawn(move || -> Result<()> {
        let mut connection = mysql_actor::connect_for_cancellation(&user, port, connect_timeout)
            .context("connect Paimon query actor")?;
        ready_tx
            .send(connection.connection_id())
            .context("publish Paimon query connection ID")?;
        done_tx
            .send(connection.query::<i64, _>(query))
            .context("publish Paimon query result")?;
        Ok(())
    });
    Ok(QueryActor {
        ready,
        done,
        thread,
    })
}

fn expect_error_any<T>(
    result: std::result::Result<T, mysql::Error>,
    expected: &[&str],
    operation: &str,
) -> Result<()> {
    let error = match result {
        Ok(_) => bail!("{operation} unexpectedly succeeded"),
        Err(error) => error,
    };
    let message = error.to_string().to_ascii_lowercase();
    ensure!(
        expected
            .iter()
            .any(|needle| message.contains(&needle.to_ascii_lowercase())),
        "{operation} failed with an unrelated error: {error}; expected one of {expected:?}"
    );
    Ok(())
}
