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

//! Ordinary Trino results preserve exact rows across Native segments/pages.
//! Logical value hashes use independent fixed-width input framing, never an
//! adapter encoder or historical MySQL packet bytes.
use super::result_delivery_support::await_idle;
use crate::actors::trino_protocol::TrinoClient;
use crate::scenario::{Scenario, ScenarioContext};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::TestClusterOwner;
use novarocks_test_trino_client::{Column, StatementFailure, TrinoError, TypeArgument};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

const SEGMENT_BYTES: usize = 1_048_576;
const MANY_SQL: &str =
    "SELECT generate_series FROM generate_series(1, 200000) ORDER BY generate_series";
const LARGE_SQL: &str = "SELECT REPEAT('q', 1048575 + generate_series) AS c0, REPEAT('q', 1048575 + generate_series) AS c1, REPEAT('q', 1048575 + generate_series) AS c2 FROM generate_series(1, 1)";
const MANY_VALUE_SHA256: &str = "bc29c9d2007ce126ecf9011853cd852c713f7ccb39905b7556e062a94e3d921f";
const LARGE_VALUE_SHA256: &str = "9c6f181b4bb3e2acdfed1047fae807b8e9373fa570932fe269d04754420d84ac";

#[derive(Clone, Copy)]
enum Shape {
    Many,
    Large,
}
impl Shape {
    fn sql(self) -> &'static str {
        match self {
            Self::Many => MANY_SQL,
            Self::Large => LARGE_SQL,
        }
    }
    fn rows(self) -> u64 {
        match self {
            Self::Many => 200_000,
            Self::Large => 1,
        }
    }
    fn hash(self) -> &'static str {
        match self {
            Self::Many => MANY_VALUE_SHA256,
            Self::Large => LARGE_VALUE_SHA256,
        }
    }
    fn assert_columns(self, columns: &[Column]) -> Result<()> {
        match self {
            Self::Many => ensure!(
                columns.len() == 1
                    && columns[0].name == "generate_series"
                    && columns[0].type_name == "bigint"
                    && columns[0].type_signature.raw_type == "bigint"
                    && columns[0].type_signature.arguments.is_empty(),
                "ordered series lost its exact BIGINT declaration"
            ),
            Self::Large => {
                ensure!(
                    columns.len() == 3,
                    "legal cross-segment row lost its three columns"
                );
                for (index, column) in columns.iter().enumerate() {
                    ensure!(
                        column.name == format!("c{index}")
                            && column.type_name == "varchar"
                            && column.type_signature.raw_type == "varchar"
                            && column.type_signature.arguments
                                == [TypeArgument::Long(2_147_483_647)],
                        "legal cross-segment row lost its exact VARCHAR declaration"
                    );
                }
            }
        }
        Ok(())
    }
    fn assert_row(self, row: &Value, ordinal: u64, hash: &mut Sha256) -> Result<()> {
        let cells = row
            .as_array()
            .context("Trino row is not a complete JSON array")?;
        match self {
            Self::Many => {
                ensure!(
                    cells.len() == 1 && ordinal <= self.rows(),
                    "ordered series has extra cells/rows"
                );
                let value = cells[0]
                    .as_i64()
                    .context("ordered series requires non-null signed BIGINT")?;
                ensure!(
                    value == i64::try_from(ordinal)?,
                    "ordered series lost or duplicated an original row"
                );
                hash.update(value.to_le_bytes());
            }
            Self::Large => {
                ensure!(
                    ordinal == 1 && cells.len() == 3,
                    "legal large row has extra cells/rows"
                );
                for cell in cells {
                    let value = cell
                        .as_str()
                        .context("legal large row requires non-null VARCHAR")?;
                    ensure!(
                        value.len() == SEGMENT_BYTES && value.bytes().all(|b| b == b'q'),
                        "legal cross-segment row changed an original byte"
                    );
                    hash.update(u64::try_from(value.len())?.to_le_bytes());
                    hash.update(value.as_bytes());
                }
            }
        }
        Ok(())
    }
}
pub(super) fn scenarios() -> Vec<Box<dyn Scenario>> {
    [Shape::Many, Shape::Large]
        .into_iter()
        .map(|shape| Box::new(Rows(shape)) as Box<dyn Scenario>)
        .collect()
}
struct Rows(Shape);
impl Scenario for Rows {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "result_delivery",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }
    fn name(&self) -> &'static str {
        match self.0 {
            Shape::Many => "result-delivery/many-small-rows-cross-segment",
            Shape::Large => "result-delivery/large-row-cross-native-segment",
        }
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "Trino row correctness requires native 1FE+3BE"
        );
        let epoch = Instant::now();
        await_idle(context, "trino-rows", "before", epoch)?;
        let before = (0..3)
            .map(|i| context.handle().backend_task_execution_tasks_created(i))
            .collect::<Result<Vec<_>>>()?;
        // Preserve the former ordinary wire query's 30 second bound. Polling
        // another continuation never creates a fresh deadline.
        let deadline = context
            .deadline()
            .min(Instant::now() + Duration::from_secs(30));
        let actor = TrinoClient::new(
            context.trino_port(),
            context.sql_user(),
            deadline,
            &context.scenario_root().join("trino-row-http"),
        )?
        .with_session(&[("x-trino-time-zone", "Asia/Shanghai")]);
        let mut live_uri = None;
        let mut rows = 0u64;
        let mut pages = 0u64;
        let mut data_pages = 0u64;
        let mut columns = None::<Vec<Column>>;
        let mut id = None::<String>;
        let mut hash = Sha256::new();
        let mut observed_hash = None::<String>;
        let mut terminal_failure = false;
        let result = (|| -> Result<()> {
            let mut page = actor.submit(self.0.sql())?;
            loop {
                ensure!(
                    page.status == 200,
                    "ordinary Trino response has HTTP {}",
                    page.status
                );
                #[derive(serde::Deserialize)]
                struct OriginalFailure {
                    error: Option<TrinoError>,
                }
                let failure: OriginalFailure = serde_json::from_slice(&page.body)?;
                if let Some(error) = failure.error {
                    terminal_failure = true;
                    return Err(StatementFailure::Trino(error).into());
                }
                let value = page.json()?;
                let actual_id = value["id"]
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .context("Trino page omitted its original query id")?;
                if let Some(original) = &id {
                    ensure!(
                        actual_id == original,
                        "continuation changed its original query id"
                    );
                } else {
                    id = Some(actual_id.to_owned());
                }
                let next = page.next_uri()?;
                live_uri = next.clone();
                ensure!(
                    value.get("updateType").is_none_or(Value::is_null)
                        && value.get("updateCount").is_none_or(Value::is_null),
                    "ordinary rows became command completion"
                );
                if let Some(metadata) = value.get("columns").filter(|v| !v.is_null()) {
                    let actual: Vec<Column> = serde_json::from_value(metadata.clone())?;
                    self.0.assert_columns(&actual)?;
                    if let Some(original) = &columns {
                        ensure!(
                            original == &actual,
                            "result column declarations changed between pages"
                        );
                    } else {
                        columns = Some(actual);
                    }
                }
                if let Some(data) = value.get("data").filter(|v| !v.is_null()) {
                    ensure!(columns.is_some(), "data preceded its exact declaration");
                    let data = data.as_array().context("Trino data is not a row array")?;
                    ensure!(!data.is_empty(), "Trino data page contains no complete row");
                    data_pages += 1;
                    for row in data {
                        rows += 1;
                        self.0.assert_row(row, rows, &mut hash)?;
                    }
                    ensure!(
                        next.is_some(),
                        "data page omitted its terminal acknowledgement continuation"
                    );
                }
                pages += 1;
                match next {
                    Some(next) => page = actor.get(&next)?,
                    None => {
                        ensure!(
                            value["stats"]["state"] == "FINISHED"
                                && value.get("data").is_none_or(Value::is_null),
                            "ordinary query did not end in an acknowledged success page"
                        );
                        break;
                    }
                }
            }
            ensure!(
                columns.is_some() && rows == self.0.rows(),
                "ordinary query lost schema or original rows"
            );
            ensure!(
                data_pages
                    >= match self.0 {
                        Shape::Many => 2,
                        Shape::Large => 1,
                    },
                "required data-page boundary was not covered"
            );
            let actual_hash = format!("{:x}", hash.finalize());
            ensure!(
                actual_hash == self.0.hash(),
                "independent logical value hash changed"
            );
            observed_hash = Some(actual_hash);
            Ok(())
        })();
        let cleanup = if result.is_err() && !terminal_failure {
            live_uri.as_deref().map_or(Ok(()), |uri| {
                let response = actor.delete(uri)?;
                ensure!(
                    matches!(response.status, 200 | 204),
                    "same-query DELETE cleanup failed"
                );
                Ok(())
            })
        } else {
            Ok(())
        };
        combine(result, cleanup)?;
        let after = (0..3)
            .map(|i| context.handle().backend_task_execution_tasks_created(i))
            .collect::<Result<Vec<_>>>()?;
        let mut created = 0u64;
        for (before, after) in before.iter().zip(&after) {
            ensure!(
                before.is_finite() && after.is_finite() && after >= before,
                "native task counter is invalid"
            );
            created += (after - before) as u64;
        }
        ensure!(
            created > 0,
            "ordinary Trino rows never executed a native backend Task"
        );
        std::fs::write(
            context.scenario_root().join("trino-row-observation.json"),
            serde_json::to_vec_pretty(&json!({
                "query_id":id,"rows":rows,"pages":pages,"data_pages":data_pages,"columns":columns,
                "observed_logical_value_sha256":observed_hash,"expected_logical_value_sha256":self.0.hash(),"native_tasks_created":created,
                "native_segment_bytes":SEGMENT_BYTES,"query_bound_seconds":30
            }))?,
        )?;
        await_idle(context, "trino-rows", "after", epoch)?;
        context.record_phase_observation(
            "row-correctness",
            1,
            1,
            1,
            "public-trino-native-root",
            1,
            "passed",
            BTreeMap::from([
                ("rows", rows),
                ("data_pages", data_pages),
                ("native_tasks_created", created),
            ]),
        )?;
        context.action("verified exact Trino declarations, original independent values/order, Native segment and page spans, terminal acknowledgement and actual owner convergence");
        Ok(())
    }
}
fn combine<T>(primary: Result<T>, cleanup: Result<()>) -> Result<T> {
    match (primary, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(e), Ok(())) | (Ok(_), Err(e)) => Err(e),
        (Err(primary), Err(cleanup)) => Err(RowsCleanup { primary, cleanup }.into()),
    }
}
struct RowsCleanup {
    primary: anyhow::Error,
    cleanup: anyhow::Error,
}
impl std::fmt::Display for RowsCleanup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "original Trino rows failed: {:#}; actual cleanup also failed: {:#}",
            self.primary, self.cleanup
        )
    }
}
impl std::fmt::Debug for RowsCleanup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::error::Error for RowsCleanup {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.primary.as_ref())
    }
}
