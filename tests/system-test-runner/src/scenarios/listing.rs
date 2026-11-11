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

use super::connector::{await_resource_convergence, require_three_backends, resource_baseline};
use crate::actors::sql::{self, SqlSession, StatementFailure, StatementResult, TrinoValue};
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, ensure};
use novarocks_cluster_harness::CrossProcessConfigOverlay;
use novarocks_cluster_harness::listing_rest::{
    ListingMode, ListingRestFixture, MEMBERS, NAMESPACES, OBJECT_STORE_LIST_BODY_BYTES,
};
use serde::Serialize;
use serde_json::{Value, json};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

pub struct CatalogListing;

fn require_listing_snapshot_capacity(context: &ScenarioContext) -> Result<()> {
    let evidence: serde_json::Value =
        serde_json::from_slice(context.effective_launch_config_evidence().artifact_bytes())?;
    let roles = evidence["semantics"]["roles"]
        .as_array()
        .context("missing effective listing role configurations")?;
    let value_at = |role: &serde_json::Value, path: &[&str]| -> Result<i64> {
        let mut value = &role["effective_config"];
        for key in path {
            ensure!(
                value["type"] == "table",
                "missing listing config table before {}",
                path.join(".")
            );
            value = &value["value"][*key];
        }
        ensure!(
            value["type"] == "integer",
            "listing config {} is not an integer",
            path.join(".")
        );
        value["value"]
            .as_i64()
            .context("listing config integer is not represented")
    };
    let frontend = roles
        .iter()
        .filter(|role| role["role"] == "fe")
        .collect::<Vec<_>>();
    ensure!(
        frontend.len() == 1,
        "listing requires one effective FE config"
    );
    ensure!(
        value_at(
            frontend[0],
            &[
                "runtime",
                "frontend_workload",
                "client_result_all_objects_bytes"
            ]
        )? == 201326592,
        "listing FE must retain the declared 192 MiB Client budget"
    );
    let backends = roles
        .iter()
        .filter(|role| role["role"] == "be")
        .collect::<Vec<_>>();
    ensure!(
        backends.len() == 3,
        "listing requires three effective BE configs"
    );
    for backend in backends {
        ensure!(
            value_at(backend, &["runtime", "result_retained_bytes_per_root"])? == 268435456
                && value_at(backend, &["runtime", "result_retained_bytes_per_process"])?
                    == 4294967296,
            "listing BE must retain the declared 256 MiB Root and 4 GiB process budgets"
        );
    }
    Ok(())
}

fn listing_fixture_launch_config() -> Result<ScenarioLaunchConfig> {
    let credential = |purpose: &str| {
        format!(
            "[[connector.credentials]]\npurpose = '{purpose}'\nname = 'cl-fixture'\ngeneration = 'v1'\nkind = 's3'\naccess_key_id = 'cl-fixture'\naccess_key_secret = 'cl-fixture-test-only'\n"
        )
    };
    Ok(ScenarioLaunchConfig {
        config_overlay: CrossProcessConfigOverlay {
            fe: Some(credential("object-store-metadata")),
            be: Some(credential("object-store-data")),
            ..Default::default()
        },
        ..Default::default()
    })
}

impl Scenario for CatalogListing {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "listing",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "catalog/mem-1-m07-sdk-listing"
    }
    fn is_explicit_stage(&self) -> bool {
        true
    }
    fn launch_config(&self, _root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        let mut config = listing_fixture_launch_config()?;
        config.config_overlay.fe.as_mut().unwrap().push_str(
            "\n[runtime.frontend_workload]\nclient_result_all_objects_bytes = 201326592\n",
        );
        config.config_overlay.be.as_mut().unwrap().push_str(
            "\n[runtime]\nresult_retained_bytes_per_root = 268435456\nresult_retained_bytes_per_process = 4294967296\n",
        );
        Ok(config)
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        require_listing_snapshot_capacity(context)?;
        let baseline = resource_baseline(context)?;
        let fixture = ListingRestFixture::start()?;
        let timeout = context
            .remaining("listing SQL")?
            .min(Duration::from_secs(120));
        let user = context.sql_user().to_string();
        let port = context.trino_port();
        let deadline = context.deadline();
        let mut control = SqlSession::new(&user, port, deadline)?;
        listing_sql(
            &mut control,
            listing_catalog_sql(&fixture, "cl_normal"),
            deadline,
        )?;
        await_fixture(&fixture, timeout, |audit| {
            audit.namespace_pages == 1 && audit.active_listing_requests == 0
        })?;
        let mut phases = Vec::new();
        fixture.set_mode(ListingMode::Normal)?;
        phases.push(record_listing_phase(context, "lake-discovery", || {
            listing_sql(
                &mut control,
                listing_catalog_sql(&fixture, "cl_discovery"),
                deadline,
            )?;
            let audit = await_fixture(&fixture, timeout, |audit| {
                audit.table_loads == (NAMESPACES * MEMBERS) as u64
                    && audit.active_listing_requests == 0
            })?;
            Ok(json!({"provider":audit}))
        })?);
        for clients in [1, 8, 16] {
            fixture.set_mode(ListingMode::Normal)?;
            phases.push(record_listing_phase(context, "information-schema", || {
                let barrier = Arc::new(Barrier::new(clients));
                std::thread::scope(|scope| -> Result<()> {
                    let handles = (0..clients).map(|_| {
                        let barrier = barrier.clone(); let user = &user;
                        scope.spawn(move || -> Result<()> {
                            barrier.wait();
                            let mut connection = SqlSession::new(user, port, deadline)?;
                            let count = listing_sql(&mut connection, "SELECT COUNT(*) FROM cl_normal.information_schema.tables WHERE TABLE_CATALOG='cl_normal' AND TABLE_SCHEMA LIKE 'cl_ns_%' AND TABLE_TYPE='BASE TABLE'", deadline).and_then(|result| listing_count(&result).map(Some))?;
                            ensure!(count == Some((NAMESPACES * MEMBERS) as u64), "incomplete information_schema table list");
                            Ok(())
                        })
                    }).collect::<Vec<_>>();
                    let mut joined = Ok(());
                    for handle in handles { joined = listing_with_cleanup(joined, join_listing_client(handle.join())); }
                    joined
                })?;
                let audit = fixture.snapshot()?;
                ensure!(audit.peak_listing_requests <= 8, "catalog generation exceeded eight active listing requests");
                ensure!(audit.table_pages == (clients * NAMESPACES * 2) as u64, "information_schema skipped or duplicated table pages");
                Ok(json!({"clients":clients,"provider":audit}))
            })?);
        }
        fixture.set_mode(ListingMode::Normal)?;
        listing_sql(&mut control, "USE cl_normal.cl_ns_0000", deadline)?;
        phases.push(record_listing_phase(context, "show-views", || {
            let views =
                listing_sql(&mut control, "SELECT table_name FROM information_schema.tables WHERE table_schema='cl_ns_0000' AND table_type='VIEW' ORDER BY table_name", deadline).and_then(listing_views)?;
            ensure!(
                views.len() == MEMBERS
                    && views[0] == "cl_view_000000"
                    && views[MEMBERS - 1] == "cl_view_000511",
                "incomplete view-row output"
            );
            Ok(json!({"rows":views.len(),"provider":fixture.snapshot()?}))
        })?);
        {
            let mode = ListingMode::TokenCycle;
            fixture.set_mode(mode)?;
            phases.push(record_listing_phase(context, "drop-refusal", || {
                let error = listing_sql(
                    &mut control,
                    "DROP DATABASE cl_normal.cl_ns_0000 FORCE",
                    deadline,
                )
                .err()
                .context("cyclic discovery must refuse the whole DROP")?;
                let diagnostic = listing_public_error(&error)?.message.clone();
                let expected_kind = "CorruptData";
                if !diagnostic.contains(expected_kind) {
                    return Err(error.context("DROP failed outside its listing boundary"));
                }
                let audit = fixture.snapshot()?;
                ensure!(
                    audit.destructive_mutations == 0,
                    "DROP mutated provider state before complete discovery"
                );
                Ok(json!({"mode":format!("{mode:?}"),"provider":audit}))
            })?);
        }
        fixture.set_mode(ListingMode::PagedOverflow)?;
        phases.push(record_listing_phase(context,"views-beyond-old-entries",|| {
            let views=listing_sql(&mut control,"SELECT table_name FROM information_schema.tables WHERE table_schema='cl_ns_0000' AND table_type='VIEW' ORDER BY table_name",deadline).and_then(listing_views)?;
            ensure!(views.len()==65537 && views.iter().enumerate().all(|(index,name)|*name==format!("cl_view_{index:06}")),"view rows beyond the former entry quota were truncated or duplicated");
            let audit=fixture.snapshot()?;
            ensure!(audit.view_pages==257 && audit.table_pages==0 && audit.active_listing_requests==0 && audit.destructive_mutations==0 && audit.table_commits==0,"large view listing violated pagination or performed unrelated work: {audit:?}");
            Ok(json!({"rows":views.len(),"provider":audit}))
        })?);
        fixture.set_mode(ListingMode::NameOverflow)?;
        phases.push(record_listing_phase(context,"views-beyond-old-name-bytes",|| {
            let views=listing_sql(&mut control,"SELECT table_name FROM information_schema.tables WHERE table_schema='cl_ns_0000' AND table_type='VIEW' ORDER BY table_name",deadline).and_then(listing_views)?;
            ensure!(views.len()==257 && views[..256].iter().all(|name|name.len()==65536) && views.last().map(String::as_str)==Some("x"),"view names beyond the former byte quota were truncated or incomplete");
            let name_bytes=views.iter().map(String::len).sum::<usize>();
            ensure!(name_bytes==16777217,"large view names lost their original bytes");
            let audit=fixture.snapshot()?;
            ensure!(audit.view_pages==2 && audit.table_pages==0 && audit.active_listing_requests==0 && audit.destructive_mutations==0 && audit.table_commits==0,"large-name listing violated pagination or performed unrelated work: {audit:?}");
            Ok(json!({"rows":views.len(),"name_bytes":name_bytes,"provider":audit}))
        })?);
        fixture.set_mode(ListingMode::ExactEntries)?;
        phases.push(record_listing_phase(context, "exact-entries", || {
            let views =
                listing_sql(&mut control, "SELECT table_name FROM information_schema.tables WHERE table_schema='cl_ns_0000' AND table_type='VIEW' ORDER BY table_name", deadline).and_then(listing_views)?;
            ensure!(
                views.len() == 65536
                    && views.first().map(String::as_str) == Some("cl_view_000000")
                    && views.last().map(String::as_str) == Some("cl_view_065535"),
                "fixed-scale view list was truncated or incomplete"
            );
            Ok(json!({"rows":views.len(),"provider":fixture.snapshot()?}))
        })?);
        fixture.set_mode(ListingMode::Normal)?;
        phases.push(record_listing_phase(context, "drop-success", || {
            listing_sql(
                &mut control,
                "DROP DATABASE cl_normal.cl_ns_0000 FORCE",
                deadline,
            )?;
            let audit = fixture.snapshot()?;
            ensure!(
                audit.destructive_mutations == (MEMBERS * 2 + 1) as u64,
                "DROP omitted table, view or namespace mutations"
            );
            Ok(json!({"provider":audit}))
        })?);
        await_resource_convergence(context, &baseline, "listing SQL")?;
        std::fs::write(
            context
                .scenario_root()
                .join("listing-functional-evidence.json"),
            serde_json::to_vec_pretty(&json!({
                "input_freeze":"docs/testing/mem-1-m07/inputs/cl-listing-freeze-v2.json",
                "fixture":"controlled standard REST protocol; no row data",
                "scope":"functional listing, pagination, complete refusal and mutation evidence; no allocator measurement or capacity proof",
                "phases":phases
            }))?,
        )?;
        Ok(())
    }
}

fn await_fixture(
    fixture: &ListingRestFixture,
    timeout: Duration,
    predicate: impl Fn(&novarocks_cluster_harness::listing_rest::ListingSnapshot) -> bool,
) -> Result<novarocks_cluster_harness::listing_rest::ListingSnapshot> {
    let deadline = std::time::Instant::now() + timeout;
    let mut previous_match = false;
    loop {
        let audit = fixture.snapshot()?;
        let matches = predicate(&audit);
        if matches && previous_match {
            return Ok(audit);
        }
        ensure!(
            std::time::Instant::now() < deadline,
            "controlled listing phase did not converge: {audit:?}"
        );
        previous_match = matches;
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Preserve functional phase evidence and original process identities only.
pub(super) fn record_listing_phase(
    context: &mut ScenarioContext,
    phase: &'static str,
    operation: impl FnOnce() -> Result<Value>,
) -> Result<Value> {
    let sequence = context.actions().len();
    context.action(format!("verify CL phase {phase} sequence={sequence}"));
    let identities = context.recheck_live_process_launch_identities()?;
    let outcome = operation();
    let checked = (|| -> Result<()> {
        let after = context.recheck_live_process_launch_identities()?;
        ensure!(
            serde_json::to_value(&identities)? == serde_json::to_value(&after)?,
            "listing target process identity changed"
        );
        Ok(())
    })();
    let evidence = json!({"phase":phase,"process_launch_identities":identities,
        "outcome":if outcome.is_ok() && checked.is_ok() {"passed"} else {"failed"},
        "error":outcome.as_ref().err().map(|error| format!("{error:#}")),
        "process_identity_error":checked.as_ref().err().map(|error| format!("{error:#}")),
        "result":outcome.as_ref().ok()});
    let saved = (|| -> Result<()> {
        std::fs::write(
            context
                .scenario_root()
                .join(format!("listing-phase-{sequence:03}.json")),
            serde_json::to_vec_pretty(&evidence)?,
        )?;
        Ok(())
    })();
    listing_with_cleanup(outcome, listing_with_cleanup(checked, saved))
        .with_context(|| format!("listing phase {phase}"))?;
    Ok(evidence)
}
pub(super) fn listing_with_cleanup<T>(primary: Result<T>, cleanup: Result<()>) -> Result<T> {
    match (primary, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(primary), Err(cleanup)) => Err(ListingCleanup { primary, cleanup }.into()),
    }
}
pub(super) fn listing_sql(
    session: &mut SqlSession,
    sql: impl AsRef<str>,
    deadline: Instant,
) -> Result<StatementResult> {
    let pending = session.submit(sql.as_ref())?;
    run_listing_pending(session, pending, deadline)
}
fn run_listing_pending(
    session: &mut SqlSession,
    pending: novarocks_test_trino_client::PendingStatement,
    deadline: Instant,
) -> Result<StatementResult> {
    let cancel = pending.cancel_handle();
    let response = session.run_pending(pending);
    let cleanup = if matches!(&response, Err(error) if !matches!(error, StatementFailure::Trino(_)))
    {
        cancel.delete(deadline).map_err(anyhow::Error::from)
    } else {
        Ok(())
    };
    listing_with_cleanup(response.map_err(anyhow::Error::from), cleanup)
}
pub(super) fn listing_count(result: &StatementResult) -> Result<u64> {
    ensure!(
        result.is_result_set()
            && result.columns.len() == 1
            && result.columns[0].type_signature.raw_type == "bigint"
            && result.columns[0].type_signature.arguments.is_empty(),
        "listing COUNT lost its declared BIGINT column"
    );
    listing_integer(result)
}
fn listing_integer(result: &StatementResult) -> Result<u64> {
    Ok(u64::try_from(sql::single_integer(result)?)?)
}
pub(super) fn listing_views(result: StatementResult) -> Result<Vec<String>> {
    ensure!(
        result.is_result_set()
            && result.columns.len() == 1
            && result.columns[0].type_signature.raw_type == "varchar",
        "view-row query lost its declared text column"
    );
    result
        .rows
        .into_iter()
        .map(|row| {
            let mut values = row.into_iter();
            match (values.next(), values.next()) {
                (Some(TrinoValue::Text(value)), None) => Ok(value),
                actual => {
                    anyhow::bail!("view-row query requires one non-null text cell: {actual:?}")
                }
            }
        })
        .collect()
}

fn listing_public_error(error: &anyhow::Error) -> Result<&novarocks_test_trino_client::TrinoError> {
    let original = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<StatementFailure>())
        .context("listing refusal lost its original typed SQL failure")?;
    match original {
        StatementFailure::Trino(server) => Ok(server),
        other => Err(anyhow::Error::new(other.clone())
            .context("listing refusal was not an actual Trino SQL error")),
    }
}
pub(super) fn join_listing_client<T>(original: std::thread::Result<Result<T>>) -> Result<T> {
    original.map_err(|payload| anyhow::Error::new(ListingClientPanic(Mutex::new(payload))))?
}
fn finish_listing_response(
    response: Result<std::result::Result<StatementResult, StatementFailure>>,
    cleanup: Result<()>,
) -> Result<std::result::Result<StatementResult, StatementFailure>> {
    match (response, cleanup) {
        (Ok(Err(primary)), Err(cleanup)) => listing_with_cleanup(Err(primary.into()), Err(cleanup)),
        (response, cleanup) => listing_with_cleanup(response, cleanup),
    }
}
struct ListingCleanup {
    primary: anyhow::Error,
    cleanup: anyhow::Error,
}
impl std::fmt::Debug for ListingCleanup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::fmt::Display for ListingCleanup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The standard source chain renders the original primary. Rendering
        // it here as well repeats every nested primary subtree exponentially.
        write!(
            f,
            "original listing operation failed; actual cleanup failed: {:#}",
            self.cleanup
        )
    }
}
impl std::error::Error for ListingCleanup {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.primary.as_ref())
    }
}
struct ListingClientPanic(Mutex<Box<dyn std::any::Any + Send>>);
impl std::fmt::Debug for ListingClientPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::fmt::Display for ListingClientPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _ = &self.0;
        f.write_str("original listing SQL actor panicked; payload retained")
    }
}
impl std::error::Error for ListingClientPanic {}

fn listing_catalog_sql(fixture: &ListingRestFixture, name: &str) -> String {
    format!(
        "CREATE EXTERNAL CATALOG {name} PROPERTIES (\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"rest\",\"uri\"=\"{}\",\"warehouse\"=\"s3://cl-fixture/warehouse\",\"aws.s3.endpoint\"=\"{}\",\"aws.s3.region\"=\"us-east-1\",\"aws.s3.enable_path_style_access\"=\"true\",\"credential.object-store-metadata.consumer-role\"=\"frontend\",\"credential.object-store-metadata.mode\"=\"static\",\"credential.object-store-metadata.name\"=\"cl-fixture\",\"credential.object-store-metadata.generation\"=\"v1\",\"credential.object-store-data.consumer-role\"=\"backend\",\"credential.object-store-data.mode\"=\"static\",\"credential.object-store-data.name\"=\"cl-fixture\",\"credential.object-store-data.generation\"=\"v1\")",
        fixture.endpoint(),
        fixture.endpoint()
    )
}

pub struct CatalogListingCancellation;

pub struct ObjectStoreListingBoundary;

impl Scenario for ObjectStoreListingBoundary {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "listing",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "catalog/mem-1-m07-opendal-listing-boundary"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn launch_config(&self, _root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        listing_fixture_launch_config()
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let fixture = ListingRestFixture::start()?;
        let timeout = context
            .remaining("OpenDAL listing boundary")?
            .min(Duration::from_secs(120));
        let deadline = context.deadline();
        let mut control = SqlSession::new(context.sql_user(), context.trino_port(), deadline)?;
        listing_sql(
            &mut control,
            listing_catalog_sql(&fixture, "cl_opendal"),
            deadline,
        )?;
        await_fixture(&fixture, timeout, |audit| {
            audit.namespace_pages == 1 && audit.active_listing_requests == 0
        })?;
        for bytes in [
            OBJECT_STORE_LIST_BODY_BYTES,
            OBJECT_STORE_LIST_BODY_BYTES + 1,
        ] {
            fixture.set_object_store_list_body_bytes(bytes)?;
            record_listing_phase(context, "opendal-list-body-boundary", || {
                let error = listing_sql(&mut control, "ALTER TABLE cl_opendal.cl_ns_0000.cl_table_000000 ADD FILES FROM 's3://cl-fixture/import-source/'", deadline)
                    .err()
                    .context("empty source listing must not commit ADD FILES")?;
                let diagnostic = listing_public_error(&error)?.message.clone();
                if !(diagnostic.contains("no visible Parquet files")
                    && !diagnostic.contains("ResourceExhausted"))
                {
                    return Err(error.context(
                        "legal list response did not reach the empty-source semantic check",
                    ));
                }
                let audit = fixture.snapshot()?;
                ensure!(
                    audit.object_store_list_requests == 1
                        && audit.object_store_list_body_bytes == bytes as u64,
                    "OpenDAL boundary was retried or did not consume the frozen response"
                );
                ensure!(
                    audit.table_commits == 0 && audit.destructive_mutations == 0,
                    "ADD FILES attempted an external effect before complete source discovery"
                );
                ensure!(
                    listing_sql(&mut control, "SELECT 1", deadline)
                        .and_then(|result| listing_integer(&result).map(Some))?
                        == Some(1),
                    "listing refusal left the connection unusable"
                );
                Ok(json!({"response_bytes":bytes,"error":diagnostic,"provider":audit}))
            })?;
        }
        await_resource_convergence(context, &baseline, "OpenDAL listing boundary")?;
        Ok(())
    }
}

impl Scenario for CatalogListingCancellation {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "listing",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "catalog/mem-1-m07-sdk-listing-cancellation"
    }
    fn is_explicit_stage(&self) -> bool {
        true
    }
    fn launch_config(&self, root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        let mut config = CatalogListing.launch_config(root)?;
        // One synchronous SQL worker serializes view-row queries before the eight
        // actors can reach the catalog generation's listing admission.
        config
            .config_overlay
            .fe
            .as_mut()
            .context("listing cancellation requires the FE fixture overlay")?
            .push_str("\n[runtime]\nquery_blocking_worker_threads = 8\n");
        Ok(config)
    }
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        require_listing_snapshot_capacity(context)?;
        let baseline = resource_baseline(context)?;
        let fixture = ListingRestFixture::start()?;
        let timeout = context
            .remaining("listing cancellation")?
            .min(Duration::from_secs(20));
        let user = context.sql_user().to_string();
        let port = context.trino_port();
        let deadline = context.deadline();
        let mut control = SqlSession::new(&user, port, deadline)?;
        listing_sql(
            &mut control,
            listing_catalog_sql(&fixture, "cl_cancel"),
            deadline,
        )?;
        await_fixture(&fixture, timeout, |audit| {
            audit.namespace_pages == 1 && audit.active_listing_requests == 0
        })?;
        listing_sql(&mut control, "USE cl_cancel.cl_ns_0000", deadline)?;
        fixture.set_mode(ListingMode::Delayed)?;
        record_listing_phase(context, "listing-deadline", || {
            listing_sql(&mut control, "SET query_timeout=1", deadline)?;
            let error = listing_sql(&mut control, "SELECT table_name FROM information_schema.tables WHERE table_schema='cl_ns_0000' AND table_type='VIEW' ORDER BY table_name", deadline)
                .and_then(listing_views)
                .err()
                .context("delayed SDK listing must exceed its absolute deadline")?;
            let server = listing_public_error(&error)?;
            if !(server.error_code == 131075
                && server.error_name == "EXCEEDED_TIME_LIMIT"
                && server.error_type == "INSUFFICIENT_RESOURCES"
                && server.message
                    == "[query.execution.deadline_exceeded] query timed out after 1000 ms")
            {
                return Err(error.context("listing failed outside its absolute deadline"));
            }
            listing_sql(&mut control, "SET query_timeout=0", deadline)?;
            let one = listing_sql(&mut control, "SELECT 1", deadline)
                .and_then(|result| listing_integer(&result).map(Some))?;
            ensure!(
                one == Some(1),
                "deadline left the statement generation unusable"
            );
            Ok(json!({"error":error.to_string(),"provider":fixture.snapshot()?}))
        })?;
        // The remote HTTP handler can outlive its cancelled client future.
        // Drain that fixture handler before resetting provider observations.
        await_fixture(&fixture, timeout, |audit| {
            audit.active_listing_requests == 0
        })?;
        fixture.set_mode(ListingMode::Delayed)?;
        let mut victim = SqlSession::new(&user, port, deadline)?;
        listing_sql(&mut victim, "USE cl_cancel.cl_ns_0000", deadline)?;
        record_listing_phase(context, "listing-kill-query", || {
            let pending = victim.submit("SELECT table_name FROM information_schema.tables WHERE table_schema='cl_ns_0000' AND table_type='VIEW' ORDER BY table_name")?;
            let cancel = pending.cancel_handle();
            let terminal = AtomicBool::new(false);
            let response = std::thread::scope(|scope| {
                let target = std::thread::Builder::new()
                    .name("listing-original-cancel-target".into())
                    .spawn_scoped(scope, || {
                        let response = victim.run_pending(pending);
                        terminal.store(
                            matches!(&response, Ok(_) | Err(StatementFailure::Trino(_))),
                            Ordering::Release,
                        );
                        Ok(response)
                    });
                let target = match target {
                    Ok(target) => target,
                    Err(primary) => {
                        return listing_with_cleanup(
                            Err(primary.into()),
                            cancel.delete(deadline).map_err(anyhow::Error::from),
                        );
                    }
                };
                let phase = (|| -> Result<()> {
                    // Actual SDK traffic proves this original query reached listing.
                    await_fixture(&fixture, timeout, |audit| {
                        audit.active_listing_requests == 1
                    })?;
                    cancel.delete(deadline)?;
                    Ok(())
                })();
                let attempted = phase.is_err() && !terminal.load(Ordering::Acquire);
                let before_join = if attempted {
                    cancel.delete(deadline).map_err(anyhow::Error::from)
                } else {
                    Ok(())
                };
                let response = join_listing_client(target.join());
                let after_join = if !attempted && !terminal.load(Ordering::Acquire) {
                    cancel.delete(deadline).map_err(anyhow::Error::from)
                } else {
                    Ok(())
                };
                finish_listing_response(
                    response,
                    listing_with_cleanup(phase, listing_with_cleanup(before_join, after_join)),
                )
            })?;
            let error = response
                .err()
                .context("cancelled listing unexpectedly succeeded")?;
            let error = anyhow::Error::new(error);
            let server = listing_public_error(&error)?;
            if !(server.error_code == 3
                && server.error_name == "USER_CANCELED"
                && server.error_type == "USER_ERROR")
            {
                return Err(error
                    .context("listing cancellation did not return its exact USER_CANCELED error"));
            }
            ensure!(
                listing_integer(&listing_sql(&mut victim, "SELECT 1", deadline)?)? == 1,
                "cancelled listing left the statement generation unusable"
            );
            Ok(json!({"error":error.to_string(),"provider":fixture.snapshot()?}))
        })?;
        await_fixture(&fixture, timeout, |audit| {
            audit.active_listing_requests == 0
        })?;
        fixture.set_mode(ListingMode::Delayed)?;
        let reuse_diagnostics_path = context
            .scenario_root()
            .join("listing-eight-position-reuse-diagnostics.json");
        record_listing_phase(context, "listing-eight-position-reuse", || {
            #[derive(Serialize)]
            struct ActorObservation {
                actor_id: usize,
                stage: &'static str,
                client_query_id: Option<String>,
                elapsed_millis: u128,
                stages: Vec<(&'static str, u128)>,
                outcome: Option<&'static str>,
                join_outcome: Option<&'static str>,
                joined_elapsed_millis: Option<u128>,
                error: Option<String>,
                observation_lock_poisoned: bool,
            }
            fn observe(
                slot: &std::sync::Mutex<ActorObservation>,
                started: std::time::Instant,
                stage: &'static str,
                client_query_id: Option<String>,
            ) {
                let mut actor = slot.lock().unwrap_or_else(|poisoned| {
                    let mut actor = poisoned.into_inner();
                    actor.observation_lock_poisoned = true;
                    actor
                });
                actor.stage = stage;
                actor.elapsed_millis = started.elapsed().as_millis();
                let elapsed_millis = actor.elapsed_millis;
                actor.stages.push((stage, elapsed_millis));
                if client_query_id.is_some() {
                    actor.client_query_id = client_query_id;
                }
            }
            let started = std::time::Instant::now();
            let barrier = Arc::new(Barrier::new(8));
            let actors = (0..8)
                .map(|actor_id| {
                    std::sync::Mutex::new(ActorObservation {
                        actor_id,
                        stage: "created",
                        client_query_id: None,
                        elapsed_millis: 0,
                        stages: Vec::new(),
                        outcome: None,
                        join_outcome: None,
                        joined_elapsed_millis: None,
                        error: None,
                        observation_lock_poisoned: false,
                    })
                })
                .collect::<Vec<_>>();
            let first_failure = std::sync::OnceLock::new();
            let record_first_failure = |actor_id, trigger| {
                first_failure.get_or_init(|| {
                    let provider = fixture.snapshot();
                    if let Err(error) = &provider {
                        eprintln!("listing reuse first-error fixture snapshot failed: {error:#}");
                    }
                    json!({
                        "actor_id": actor_id,
                        "trigger": trigger,
                        "elapsed_millis": started.elapsed().as_millis(),
                        "observation": "instantaneous; other actors may still be active",
                        "provider": provider.as_ref().ok(),
                        "snapshot_error": provider.as_ref().err().map(|error| format!("{error:#}")),
                    })
                });
            };
            let outcomes = std::thread::scope(|scope| {
                let targets = (0..8)
                    .map(|actor_id| {
                        let barrier = barrier.clone();
                        let user = &user;
                        let actor = &actors[actor_id];
                        let record_first_failure = &record_first_failure;
                        scope.spawn(move || -> Result<()> {
                            let outcome = (|| -> Result<()> {
                                observe(actor, started, "barrier_wait", None);
                                barrier.wait();
                                observe(actor, started, "connect", None);
                                let mut client = SqlSession::new(user, port, deadline)
                                    .with_context(|| {
                                        format!("listing reuse actor {actor_id} connect")
                                    })
                                    .inspect_err(|_| {
                                        record_first_failure(actor_id, "actor_error")
                                    })?;
                                observe(actor, started, "connected", None);
                                observe(actor, started, "use", None);
                                listing_sql(&mut client, "USE cl_cancel.cl_ns_0000", deadline)
                                    .with_context(|| format!("listing reuse actor {actor_id} use"))
                                    .inspect_err(|_| {
                                        record_first_failure(actor_id, "actor_error")
                                    })?;
                                observe(actor, started, "use_done", None);
                                observe(actor, started, "show_views", None);
                                let pending = client.submit("SELECT table_name FROM information_schema.tables WHERE table_schema='cl_ns_0000' AND table_type='VIEW' ORDER BY table_name")?;
                                observe(
                                    actor,
                                    started,
                                    "show_views_submitted",
                                    Some(pending.query_id.clone()),
                                );
                                let views = run_listing_pending(&mut client, pending, deadline)
                                    .and_then(listing_views)
                                    .with_context(|| {
                                        format!("listing reuse actor {actor_id} show_views")
                                    })
                                    .inspect_err(|_| {
                                        record_first_failure(actor_id, "actor_error")
                                    })?;
                                observe(actor, started, "show_views_done", None);
                                if views.len() != MEMBERS {
                                    record_first_failure(actor_id, "actor_error");
                                }
                                ensure!(
                                    views.len() == MEMBERS,
                                    "reused listing position produced incomplete output"
                                );
                                observe(actor, started, "rows_verified", None);
                                Ok(())
                            })();
                            if outcome.is_err() {
                                record_first_failure(actor_id, "actor_error");
                            }
                            outcome
                        })
                    })
                    .collect::<Vec<_>>();
                targets
                    .into_iter()
                    .enumerate()
                    .map(|(actor_id, target)| {
                        let (join_outcome, outcome) = match target.join() {
                            Ok(result) => ("returned", result),
                            Err(payload) => {
                                record_first_failure(actor_id, "join_panic");
                                (
                                    "panicked",
                                    Err(anyhow::Error::new(ListingClientPanic(Mutex::new(
                                        payload,
                                    )))),
                                )
                            }
                        };
                        let mut actor = actors[actor_id].lock().unwrap_or_else(|poisoned| {
                            let mut actor = poisoned.into_inner();
                            actor.observation_lock_poisoned = true;
                            actor
                        });
                        actor.join_outcome = Some(join_outcome);
                        actor.joined_elapsed_millis = Some(started.elapsed().as_millis());
                        actor.outcome = Some(if outcome.is_ok() { "passed" } else { "failed" });
                        actor.error = outcome.as_ref().err().map(|error| format!("{error:#}"));
                        (actor_id, outcome)
                    })
                    .collect::<Vec<_>>()
            });
            let post_join = fixture.snapshot();
            if let Err(error) = &post_join {
                eprintln!("listing reuse post-join fixture snapshot failed: {error:#}");
            }
            let observations = actors
                .iter()
                .map(|actor| {
                    let actor = actor.lock().unwrap_or_else(|poisoned| {
                        let mut actor = poisoned.into_inner();
                        actor.observation_lock_poisoned = true;
                        actor
                    });
                    json!(&*actor)
                })
                .collect::<Vec<_>>();
            let diagnostics = json!({
                "phase": "listing-eight-position-reuse",
                "elapsed_millis": started.elapsed().as_millis(),
                "actors": observations,
                "first_failure_observation": first_failure.get(),
                "post_join_observation": {
                    "observation": "all eight actor threads actually joined; provider work may still be active",
                    "provider": post_join.as_ref().ok(),
                    "snapshot_error": post_join.as_ref().err().map(|error| format!("{error:#}")),
                },
                "primary_error_order": "first error in original actor join order; all errors retained above",
                "primary_actor_id": outcomes.iter().find(|(_, result)| result.is_err()).map(|(actor_id, _)| actor_id),
            });
            let primary = outcomes.into_iter().find_map(|(_, result)| result.err());
            let written = (|| -> Result<()> {
                let bytes = serde_json::to_vec_pretty(&diagnostics)?;
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&reuse_diagnostics_path)?;
                std::io::Write::write_all(&mut file, &bytes)?;
                Ok(())
            })();
            if let Err(error) = &written {
                eprintln!("listing reuse diagnostic artifact write failed: {error:#}");
            }
            if let Some(error) = primary {
                return Err(error);
            }
            let audit = post_join?;
            ensure!(
                audit.peak_listing_requests == 8
                    && audit.view_pages == 16
                    && audit.active_listing_requests == 0,
                "all eight listing positions were not reused: {audit:?}"
            );
            written.context("persist listing reuse diagnostic artifact")?;
            Ok(json!({"clients":8,"provider":audit}))
        })?;
        await_resource_convergence(context, &baseline, "listing cancellation")?;
        Ok(())
    }
}

#[cfg(test)]
mod error_aggregation_tests {
    use super::{StatementFailure, join_listing_client, listing_with_cleanup};
    use anyhow::{Context, Result};
    use std::fmt;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct OwnedStatementFailure {
        label: String,
        original: StatementFailure,
        dropped: Arc<AtomicUsize>,
    }

    impl fmt::Display for OwnedStatementFailure {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(&self.label)
        }
    }

    impl std::error::Error for OwnedStatementFailure {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.original)
        }
    }

    impl Drop for OwnedStatementFailure {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn joined_listing_failures_render_each_original_cause_once_and_keep_ownership() {
        const ACTORS: usize = 16;
        let dropped = Arc::new(AtomicUsize::new(0));
        let handles = (0..ACTORS)
            .map(|actor| {
                let dropped = Arc::clone(&dropped);
                std::thread::spawn(move || -> Result<()> {
                    // Each original actor independently returns its typed SQL
                    // failure and the typed failure of its DELETE cleanup.
                    let failure = |operation| {
                        anyhow::Error::new(OwnedStatementFailure {
                            label: format!("{operation} actor [{actor:02}]"),
                            original: StatementFailure::Deadline,
                            dropped: Arc::clone(&dropped),
                        })
                    };
                    listing_with_cleanup(Err(failure("SQL")), Err(failure("DELETE")))
                })
            })
            .collect::<Vec<_>>();

        // Use the real listing owner's join and merge helpers, retaining the
        // same left fold even when every original actor has failed.
        let mut joined = Ok(());
        for handle in handles {
            joined = listing_with_cleanup(joined, join_listing_client(handle.join()));
        }
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        let phase = listing_with_cleanup(
            joined.context("native SQL phase failed"),
            Err(
                anyhow::anyhow!("observer quiescence original deadline expired")
                    .context("observer quiescence failed"),
            ),
        );
        let report = listing_with_cleanup(
            phase,
            Err(anyhow::anyhow!(
                "backend process 0 is not live during launch identity recheck"
            )),
        )
        .unwrap_err()
        .context("listing phase real-rest-information-schema");

        let primary = report
            .chain()
            .find_map(|cause| cause.downcast_ref::<OwnedStatementFailure>())
            .expect("the original typed primary remains in the source chain");
        assert_eq!(primary.label, "SQL actor [00]");
        assert!(report.chain().any(|cause| matches!(
            cause.downcast_ref::<StatementFailure>(),
            Some(StatementFailure::Deadline)
        )));

        let rendered = format!("{report:#}");
        for actor in 0..ACTORS {
            for operation in ["SQL", "DELETE"] {
                let label = format!("{operation} actor [{actor:02}]");
                assert_eq!(rendered.matches(&label).count(), 1, "{label}");
            }
        }
        assert_eq!(
            rendered.matches("Trino statement deadline expired").count(),
            ACTORS * 2
        );
        for cause in [
            "observer quiescence original deadline expired",
            "backend process 0 is not live during launch identity recheck",
        ] {
            assert_eq!(rendered.matches(cause).count(), 1, "{cause}");
        }
        // Formatting and joining do not dispose either original SQL or DELETE
        // cause. They remain owned until this same final report actually exits.
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        drop(report);
        assert_eq!(dropped.load(Ordering::SeqCst), ACTORS * 2);
    }
}

#[cfg(test)]
mod snapshot_configuration_tests {
    use super::*;

    #[test]
    fn cancellation_overlay_keeps_the_snapshot_budget_and_original_worker_override() {
        let launch = CatalogListingCancellation
            .launch_config(std::path::Path::new("."))
            .unwrap();
        let fe: toml::Value = toml::from_str(launch.config_overlay.fe.as_ref().unwrap()).unwrap();
        let be: toml::Value = toml::from_str(launch.config_overlay.be.as_ref().unwrap()).unwrap();
        assert_eq!(
            fe["runtime"]["frontend_workload"]["client_result_all_objects_bytes"].as_integer(),
            Some(201326592)
        );
        assert_eq!(
            fe["runtime"]["query_blocking_worker_threads"].as_integer(),
            Some(8)
        );
        assert_eq!(
            be["runtime"]["result_retained_bytes_per_root"].as_integer(),
            Some(268435456)
        );
        assert_eq!(
            be["runtime"]["result_retained_bytes_per_process"].as_integer(),
            Some(4294967296)
        );
    }
}
