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

//! Warm, concurrent reads of independently generated delete families and
//! snapshot-specific visibility across a private DELETE publication.

use super::connector::{await_resource_convergence, require_three_backends, resource_baseline};
use super::iceberg_delete_applicability::{
    CATALOG, create_catalog_sql_from_parts, delete_launch_config, produce_delete_receipt,
    table_name, with_cleanup,
};
use super::provider_mysql_actors::{ActorStatements, ProviderCheck, run_checks};
use crate::actors::sql::{SqlSession, StatementResult, TrinoValue};
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::fixture_run::RunBudget;
use novarocks_cluster_harness::isolated_iceberg_rest::IsolatedIcebergRestFixture;
use serde::Deserialize;
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const REPRESENTATIVE_CASES: [&str; 6] = [
    "same_commit_dv_equality",
    "same_commit_position_equality",
    "multi_target_position",
    "multi_blob_dv",
    "equality_null",
    "equality_promoted_int",
];
type RowBag = Vec<(Option<i64>, Option<i64>, Option<String>)>;

#[derive(Deserialize)]
struct CorpusReceipt {
    cases: Vec<CorpusCase>,
}
#[derive(Clone, Deserialize)]
struct CorpusCase {
    case: String,
    table: String,
    snapshot: i64,
    independent_expected_rows: RowBag,
    independent_oracle_matched: bool,
    java_row_read: JavaRowRead,
}
#[derive(Clone, Deserialize)]
struct JavaRowRead {
    status: String,
}
struct WarmFixture {
    rest: IsolatedIcebergRestFixture,
    input: Option<PreparedWarmInput>,
}
struct PreparedWarmInput {
    receipt_path: PathBuf,
    receipt_sha256: String,
    cases: Vec<CorpusCase>,
}

#[derive(Default)]
struct IcebergDeleteWarmConcurrent {
    fixture: Mutex<Option<WarmFixture>>,
}
impl Scenario for IcebergDeleteWarmConcurrent {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "iceberg_delete_warm",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "connector/iceberg-delete-warm-concurrent"
    }
    fn is_explicit_stage(&self) -> bool {
        true
    }
    fn launch_config_with_fixture_run(
        &self,
        root: &Path,
        budget: &RunBudget,
    ) -> Result<ScenarioLaunchConfig> {
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("warm delete fixture lock poisoned"))?;
        ensure!(
            slot.is_none(),
            "warm delete fixture launched more than once"
        );
        let (rest, _) = IsolatedIcebergRestFixture::start_private_with_budget(root, budget, false)?;
        let fixture = slot.insert(WarmFixture { rest, input: None });
        let rest = &fixture.rest;
        fs::write(
            root.join("delete-fixture-runtime.json"),
            serde_json::to_vec_pretty(&rest.runtime_identity()?)?,
        )?;
        let generated = produce_delete_receipt(rest, root, budget, "corpus")?;
        let receipt: CorpusReceipt = serde_json::from_value(generated.value)?;
        let mut cases = Vec::new();
        for name in REPRESENTATIVE_CASES {
            let selected = receipt
                .cases
                .iter()
                .filter(|case| case.case == name)
                .collect::<Vec<_>>();
            ensure!(
                selected.len() == 1,
                "corpus must contain one independently verified {name} case"
            );
            let case = selected[0];
            ensure!(
                case.independent_oracle_matched && case.java_row_read.status == "success",
                "corpus case {name} has no independently verified Java row bag"
            );
            table_name(&case.table)?;
            cases.push(case.clone());
        }
        let launch = delete_launch_config(rest, true);
        fixture.input = Some(PreparedWarmInput {
            receipt_path: generated.path,
            receipt_sha256: generated.sha256,
            cases,
        });
        Ok(launch)
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("warm delete fixture lock poisoned"))?;
        let fixture = slot.as_ref().context("missing warm delete fixture")?;
        let input = fixture
            .input
            .as_ref()
            .context("missing prepared warm delete input")?;
        let endpoints = fixture.rest.endpoints();
        let baseline = resource_baseline(context)?;
        let mut control =
            SqlSession::new(context.sql_user(), context.trino_port(), context.deadline())?;
        control
            .execute("SET enable_scan_datacache = true")
            .map(|_| ())?;
        control
            .execute(create_catalog_sql_from_parts(
                &endpoints.rest_uri,
                &endpoints.rest_warehouse,
                &endpoints.minio_endpoint,
            ))
            .map(|_| ())?;
        let mut observations = Vec::new();
        for case in &input.cases {
            let table = table_name(&case.table)?;
            let sql = format!(
                "SELECT id, p, value FROM {table} FOR VERSION AS OF {}",
                case.snapshot
            );
            verify_row_bag(&mut control, &sql, &case.independent_expected_rows)?;
            let actors = (0..4)
                .map(|_| (sql.clone(), case.independent_expected_rows.clone()))
                .collect::<Vec<_>>();
            concurrent_row_bags(context, actors, &format!("delete-warm-{}", case.case))?;
            observations.push(
                json!({"case":case.case,"snapshot":case.snapshot,"warm_verified":true,"actors":4}),
            );
        }
        let namespace = "uea4g_warm";
        let table = format!("{CATALOG}.{namespace}.visible_rows");
        let private_operation = (|| -> Result<()> {
            control
                .execute(format!("CREATE DATABASE {CATALOG}.{namespace}"))
                .map(|_| ())?;
            control.execute(format!("CREATE TABLE {table} (id INT, p INT, value STRING) TBLPROPERTIES ('format-version'='3', 'write.row-lineage'='true')")).map(|_| ())?;
            control
                .execute(format!(
                    "INSERT INTO {table} VALUES (1,1,'one'),(2,1,'two'),(3,1,'three')"
                ))
                .map(|_| ())?;
            control
                .execute(format!("ALTER TABLE {table} CREATE TAG before_delete"))
                .map(|_| ())?;
            let old = format!("SELECT id,p,value FROM {table} FOR VERSION AS OF 'before_delete'");
            let latest = format!("SELECT id,p,value FROM {table}");
            let seed: RowBag = [(1, "one"), (2, "two"), (3, "three")]
                .into_iter()
                .map(|(id, value)| (Some(id), Some(1), Some(value.to_owned())))
                .collect();
            let surviving: RowBag = [(1, "one"), (3, "three")]
                .into_iter()
                .map(|(id, value)| (Some(id), Some(1), Some(value.to_owned())))
                .collect();
            verify_row_bag(&mut control, &old, &seed)?;
            verify_row_bag(&mut control, &latest, &seed)?;
            control
                .execute(format!("DELETE FROM {table} WHERE id = 2"))
                .map(|_| ())?;
            concurrent_row_bags(
                context,
                vec![
                    (old.clone(), seed.clone()),
                    (latest.clone(), surviving.clone()),
                    (old, seed),
                    (latest, surviving),
                ],
                "delete-snapshot-visibility",
            )?;
            context.action("pinned and warmed old snapshot before DELETE; concurrent old/latest actors independently checked their snapshot-specific row bags");
            Ok(())
        })();
        drop(control);
        let exit = await_resource_convergence(
            context,
            &baseline,
            "warm delete and snapshot visibility readers",
        );
        fs::write(
            context.scenario_root().join("delete-warm-concurrent.json"),
            serde_json::to_vec_pretty(&json!({
                "corpus_receipt": input.receipt_path, "corpus_sha256": input.receipt_sha256,
                "observations": observations,
                "private_operation": private_operation.as_ref().err().map(ToString::to_string),
                "resource_exit": exit.as_ref().err().map(ToString::to_string),
            }))?,
        )?;
        with_cleanup(private_operation, exit)?;
        context.action("warm concurrent equality, position and DV reads matched independent producer rows and native owners exited");
        Ok(())
    }
    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("warm delete fixture lock poisoned"))?
            .take();
        if let Some(WarmFixture { mut rest, .. }) = fixture {
            rest.shutdown()?;
        }
        Ok(())
    }
}

fn verify_row_bag(connection: &mut SqlSession, sql: &str, expected: &RowBag) -> Result<()> {
    verify_row_bag_result(connection.execute(sql)?, expected)
}
fn verify_row_bag_result(result: StatementResult, expected: &RowBag) -> Result<()> {
    ensure!(
        result.is_result_set() && result.columns.len() == 3,
        "expected row-bag result set: {result:?}"
    );
    ensure!(
        result.columns[..2].iter().all(|column| matches!(
            column.type_signature.raw_type.as_str(),
            "tinyint" | "smallint" | "integer" | "bigint"
        ) && column.type_signature.arguments.is_empty())
            && result.columns[2].type_signature.raw_type == "varchar",
        "row-bag declared types differ: {result:?}"
    );
    let mut actual: RowBag = result
        .rows
        .iter()
        .map(|row| {
            let integer = |value: &TrinoValue| match value {
                TrinoValue::Null => Ok(None),
                TrinoValue::Integer(value) => Ok(Some(*value)),
                other => anyhow::bail!("expected nullable row-bag integer, got {other:?}"),
            };
            match row.as_slice() {
                [id, partition, value] => Ok((
                    integer(id)?,
                    integer(partition)?,
                    match value {
                        TrinoValue::Null => None,
                        TrinoValue::Text(value) => Some(value.clone()),
                        other => anyhow::bail!("expected nullable row-bag text, got {other:?}"),
                    },
                )),
                other => anyhow::bail!("expected exactly three row-bag cells, got {other:?}"),
            }
        })
        .collect::<Result<_>>()?;
    let mut expected = expected.clone();
    actual.sort();
    expected.sort();
    ensure!(
        actual == expected,
        "delete row bag differs from its independent input: actual={actual:?}, expected={expected:?}"
    );
    Ok(())
}
fn concurrent_row_bags(
    context: &mut ScenarioContext,
    queries: Vec<(String, RowBag)>,
    label: &str,
) -> Result<()> {
    let checks = queries
        .into_iter()
        .map(|(sql, expected)| {
            Box::new(
                move |connection: &mut SqlSession, statements: &ActorStatements| -> Result<()> {
                    statements.run(connection, "SET enable_scan_datacache = true")??;
                    verify_row_bag_result(statements.run(connection, &sql)??, &expected)
                },
            ) as ProviderCheck
        })
        .collect();
    run_checks(context, label, checks)
}

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![Box::<IcebergDeleteWarmConcurrent>::default()]
}
