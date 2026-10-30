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
use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::CrossProcessConfigOverlay;
use novarocks_cluster_harness::listing_rest::{
    ListingMode, ListingRestFixture, MEMBERS, NAMESPACES, OBJECT_STORE_LIST_BODY_BYTES,
};
use serde::Serialize;
use serde_json::{Value, json};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

pub struct CatalogListing;

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
    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let fixture = ListingRestFixture::start()?;
        let timeout = context
            .remaining("listing SQL")?
            .min(Duration::from_secs(120));
        let user = context.mysql_user().to_string();
        let port = context.mysql_port();
        let mut control = mysql_actor::connect(&user, port, timeout)?;
        control.query_drop(listing_catalog_sql(&fixture, "cl_normal"))?;
        await_fixture(&fixture, timeout, |audit| {
            audit.namespace_pages == 1 && audit.active_listing_requests == 0
        })?;
        let mut phases = Vec::new();
        fixture.set_mode(ListingMode::Normal)?;
        phases.push(measure(context, "lake-discovery", || {
            control.query_drop(listing_catalog_sql(&fixture, "cl_discovery"))?;
            let audit = await_fixture(&fixture, timeout, |audit| {
                audit.table_loads == (NAMESPACES * MEMBERS) as u64
                    && audit.active_listing_requests == 0
            })?;
            Ok(json!({"provider":audit}))
        })?);
        for clients in [1, 8, 16] {
            fixture.set_mode(ListingMode::Normal)?;
            phases.push(measure(context, "information-schema", || {
                let barrier = Arc::new(Barrier::new(clients));
                std::thread::scope(|scope| -> Result<()> {
                    let handles = (0..clients).map(|_| {
                        let barrier = barrier.clone(); let user = &user;
                        scope.spawn(move || -> Result<()> {
                            barrier.wait();
                            let mut connection = mysql_actor::connect(user, port, timeout)?;
                            let count = connection.query_first::<u64, _>("SELECT COUNT(*) FROM cl_normal.information_schema.tables WHERE TABLE_CATALOG='cl_normal' AND TABLE_SCHEMA LIKE 'cl_ns_%'")?;
                            ensure!(count == Some((NAMESPACES * MEMBERS) as u64), "incomplete information_schema table list");
                            Ok(())
                        })
                    }).collect::<Vec<_>>();
                    for handle in handles { handle.join().map_err(|_| anyhow::anyhow!("listing client panicked"))??; }
                    Ok(())
                })?;
                let audit = fixture.snapshot()?;
                ensure!(audit.peak_listing_requests <= 8, "catalog generation exceeded eight active listing requests");
                ensure!(audit.table_pages == (clients * NAMESPACES * 2) as u64, "information_schema skipped or duplicated table pages");
                Ok(json!({"clients":clients,"provider":audit}))
            })?);
        }
        fixture.set_mode(ListingMode::Normal)?;
        control.query_drop("USE cl_normal.cl_ns_0000")?;
        phases.push(measure(context, "show-views", || {
            let views = control.query::<String, _>("SHOW VIEWS")?;
            ensure!(
                views.len() == MEMBERS
                    && views[0] == "cl_view_000000"
                    && views[MEMBERS - 1] == "cl_view_000511",
                "incomplete SHOW VIEWS output"
            );
            Ok(json!({"rows":views.len(),"provider":fixture.snapshot()?}))
        })?);
        for mode in [
            ListingMode::PagedOverflow,
            ListingMode::TerminalOverflow,
            ListingMode::NameOverflow,
            ListingMode::TokenOverflow,
            ListingMode::TokenCycle,
            ListingMode::PageOverflow,
        ] {
            fixture.set_mode(mode)?;
            phases.push(measure(context, "drop-refusal", || {
                let error = control
                    .query_drop("DROP DATABASE cl_normal.cl_ns_0000 FORCE")
                    .err()
                    .context("over-bound discovery must refuse the whole DROP")?;
                let diagnostic = error.to_string();
                let expected_kind = if mode == ListingMode::TokenCycle {
                    "CorruptData"
                } else {
                    "ResourceExhausted"
                };
                ensure!(
                    diagnostic.contains(expected_kind),
                    "DROP failed outside its listing boundary"
                );
                let audit = fixture.snapshot()?;
                ensure!(
                    audit.destructive_mutations == 0,
                    "DROP mutated provider state before complete discovery"
                );
                Ok(json!({"mode":format!("{mode:?}"),"provider":audit}))
            })?);
        }
        fixture.set_mode(ListingMode::ExactEntries)?;
        phases.push(measure(context, "exact-entries", || {
            let views = control.query::<String, _>("SHOW VIEWS")?;
            ensure!(
                views.len() == 65536
                    && views.first().map(String::as_str) == Some("cl_view_000000")
                    && views.last().map(String::as_str) == Some("cl_view_065535"),
                "exact-bound view list was truncated or incomplete"
            );
            Ok(json!({"rows":views.len(),"provider":fixture.snapshot()?}))
        })?);
        fixture.set_mode(ListingMode::Normal)?;
        phases.push(measure(context, "drop-success", || {
            control.query_drop("DROP DATABASE cl_normal.cl_ns_0000 FORCE")?;
            let audit = fixture.snapshot()?;
            ensure!(
                audit.destructive_mutations == (MEMBERS * 2 + 1) as u64,
                "DROP omitted table, view or namespace mutations"
            );
            Ok(json!({"provider":audit}))
        })?);
        await_resource_convergence(context, &baseline, "listing SQL")?;
        std::fs::write(
            context.scenario_root().join("listing-measurements.json"),
            serde_json::to_vec_pretty(&json!({
                "input_freeze":"docs/testing/mem-1-m07/inputs/cl-listing-freeze-v2.json",
                "fixture":"controlled standard REST protocol; no row data",
                "scope":"sampled FE allocator high-water; not a hard SDK byte bound or cross-provider CL completion",
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

#[derive(Clone, Copy, Debug, Serialize)]
struct AllocatorReading {
    allocated: u64,
    active: u64,
    resident: u64,
}

struct StopSampler<'a>(&'a AtomicBool);
impl Drop for StopSampler<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

fn allocator(client: &reqwest::blocking::Client, port: u16) -> Result<AllocatorReading> {
    let body = client
        .get(format!("http://127.0.0.1:{port}/metrics"))
        .send()?
        .error_for_status()?
        .text()?;
    let required = |statistic: &str| -> Result<u64> {
        let prefix = format!(
            "novarocks_frontend_process_allocator_memory_bytes{{statistic=\"{statistic}\"}} "
        );
        let values = body
            .lines()
            .filter_map(|line| line.strip_prefix(&prefix))
            .collect::<Vec<_>>();
        ensure!(
            values.len() == 1,
            "missing or duplicate allocator statistic"
        );
        Ok(values[0].parse()?)
    };
    Ok(AllocatorReading {
        allocated: required("allocated")?,
        active: required("active")?,
        resident: required("resident")?,
    })
}

pub(super) fn measure(
    context: &mut ScenarioContext,
    phase: &'static str,
    operation: impl FnOnce() -> Result<Value>,
) -> Result<Value> {
    let sequence = context.actions().len();
    context.action(format!("measure CL phase {phase} sequence={sequence}"));
    let identities = context.recheck_live_process_launch_identities()?;
    let port = context.fe_http_port();
    let client = reqwest::blocking::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(1))
        .build()?;
    let before = allocator(&client, port)?;
    let stopped = AtomicBool::new(false);
    let (outcome, observed) = std::thread::scope(|scope| {
        let sampler = scope.spawn(|| -> Result<(AllocatorReading, u64)> {
            let mut peak = before;
            let mut samples = 0;
            while !stopped.load(Ordering::Acquire) {
                let reading = allocator(&client, port)?;
                peak.allocated = peak.allocated.max(reading.allocated);
                peak.active = peak.active.max(reading.active);
                peak.resident = peak.resident.max(reading.resident);
                samples += 1;
                std::thread::sleep(Duration::from_millis(100));
            }
            Ok((peak, samples))
        });
        let stop = StopSampler(&stopped);
        let outcome = operation();
        drop(stop);
        let observed = sampler
            .join()
            .map_err(|_| anyhow::anyhow!("allocator sampler panicked"))?;
        Ok::<_, anyhow::Error>((outcome, observed))
    })?;
    let (peak, samples) = observed?;
    let after = allocator(&client, port)?;
    let after_identities = context.recheck_live_process_launch_identities()?;
    ensure!(
        serde_json::to_value(&identities)? == serde_json::to_value(&after_identities)?,
        "listing target process identity changed"
    );
    let measurement = json!({"phase":phase,"before":before,"sampled_peak":peak,"after":after,"samples":samples,"process_launch_identities":identities,"outcome":if outcome.is_ok() {"passed"} else {"failed"},"error":outcome.as_ref().err().map(|error| format!("{error:#}")),"result":outcome.as_ref().ok()});
    std::fs::write(
        context
            .scenario_root()
            .join(format!("listing-phase-{sequence:03}.json")),
        serde_json::to_vec_pretty(&measurement)?,
    )?;
    outcome.with_context(|| format!("listing phase {phase}"))?;
    Ok(measurement)
}

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

    fn launch_config(&self, root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        CatalogListing.launch_config(root)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let fixture = ListingRestFixture::start()?;
        let timeout = context
            .remaining("OpenDAL listing boundary")?
            .min(Duration::from_secs(120));
        let mut control =
            mysql_actor::connect(context.mysql_user(), context.mysql_port(), timeout)?;
        control.query_drop(listing_catalog_sql(&fixture, "cl_opendal"))?;
        await_fixture(&fixture, timeout, |audit| {
            audit.namespace_pages == 1 && audit.active_listing_requests == 0
        })?;
        for bytes in [
            OBJECT_STORE_LIST_BODY_BYTES,
            OBJECT_STORE_LIST_BODY_BYTES + 1,
        ] {
            fixture.set_object_store_list_body_bytes(bytes)?;
            measure(context, "opendal-list-body-boundary", || {
                let error = control
                    .query_drop("ALTER TABLE cl_opendal.cl_ns_0000.cl_table_000000 ADD FILES FROM 's3://cl-fixture/import-source/'")
                    .err()
                    .context("empty or oversized source listing must not commit ADD FILES")?;
                let diagnostic = error.to_string();
                if bytes == OBJECT_STORE_LIST_BODY_BYTES {
                    ensure!(
                        diagnostic.contains("no visible Parquet files")
                            && !diagnostic.contains("ResourceExhausted"),
                        "exact-bound list did not reach the empty-source semantic check: {diagnostic}"
                    );
                } else {
                    ensure!(
                        diagnostic.contains("ResourceExhausted"),
                        "oversized OpenDAL list lost its error classification: {diagnostic}"
                    );
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
                    control.query_first::<u64, _>("SELECT 1")? == Some(1),
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
        // One synchronous SQL worker serializes SHOW VIEWS before the eight
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
        let baseline = resource_baseline(context)?;
        let fixture = ListingRestFixture::start()?;
        let timeout = context
            .remaining("listing cancellation")?
            .min(Duration::from_secs(20));
        let user = context.mysql_user().to_string();
        let port = context.mysql_port();
        let mut control = mysql_actor::connect(&user, port, timeout)?;
        control.query_drop(listing_catalog_sql(&fixture, "cl_cancel"))?;
        await_fixture(&fixture, timeout, |audit| {
            audit.namespace_pages == 1 && audit.active_listing_requests == 0
        })?;
        control.query_drop("USE cl_cancel.cl_ns_0000")?;
        fixture.set_mode(ListingMode::Delayed)?;
        measure(context, "listing-deadline", || {
            control.query_drop("SET query_timeout=1")?;
            let error = control
                .query::<String, _>("SHOW VIEWS")
                .err()
                .context("delayed SDK listing must exceed its absolute deadline")?;
            ensure!(
                matches!(&error, mysql::Error::MySqlError(error)
                    if error.code == 1317
                        && error.state == "70100"
                        && error.message
                            == "[query.execution.deadline_exceeded] query timed out after 1000 ms"),
                "listing failed outside its absolute deadline: {error}"
            );
            control.query_drop("SET query_timeout=0")?;
            let one = control.query_first::<u64, _>("SELECT 1")?;
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
        let mut victim = mysql_actor::connect(&user, port, timeout)?;
        victim.query_drop("USE cl_cancel.cl_ns_0000")?;
        let connection_id = victim.connection_id();
        measure(context, "listing-kill-query", || {
            let error = std::thread::scope(|scope| -> Result<mysql::Error> {
                let target = scope.spawn(|| victim.query::<String, _>("SHOW VIEWS"));
                await_fixture(&fixture, timeout, |audit| {
                    audit.active_listing_requests == 1
                })?;
                control.query_drop(format!("KILL QUERY {connection_id}"))?;
                target
                    .join()
                    .map_err(|_| anyhow::anyhow!("cancelled listing actor panicked"))?
                    .err()
                    .context("cancelled listing unexpectedly succeeded")
            })?;
            ensure!(
                matches!(&error, mysql::Error::MySqlError(error) if error.code == 1317),
                "listing cancellation did not return ER_QUERY_INTERRUPTED: {error}"
            );
            ensure!(
                victim.query_first::<u64, _>("SELECT 1")? == Some(1),
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
        measure(context, "listing-eight-position-reuse", || {
            #[derive(Serialize)]
            struct ActorObservation {
                actor_id: usize,
                stage: &'static str,
                connection_id: Option<u32>,
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
                connection_id: Option<u32>,
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
                if connection_id.is_some() {
                    actor.connection_id = connection_id;
                }
            }
            let started = std::time::Instant::now();
            let barrier = Arc::new(Barrier::new(8));
            let actors = (0..8)
                .map(|actor_id| {
                    std::sync::Mutex::new(ActorObservation {
                        actor_id,
                        stage: "created",
                        connection_id: None,
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
                                let mut client = mysql_actor::connect(user, port, timeout)
                                    .with_context(|| {
                                        format!("listing reuse actor {actor_id} connect")
                                    })
                                    .inspect_err(|_| {
                                        record_first_failure(actor_id, "actor_error")
                                    })?;
                                observe(actor, started, "connected", Some(client.connection_id()));
                                observe(actor, started, "use", None);
                                client
                                    .query_drop("USE cl_cancel.cl_ns_0000")
                                    .with_context(|| format!("listing reuse actor {actor_id} use"))
                                    .inspect_err(|_| {
                                        record_first_failure(actor_id, "actor_error")
                                    })?;
                                observe(actor, started, "use_done", None);
                                observe(actor, started, "show_views", None);
                                let views = client
                                    .query::<String, _>("SHOW VIEWS")
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
                            Err(_) => {
                                record_first_failure(actor_id, "join_panic");
                                (
                                    "panicked",
                                    Err(anyhow::anyhow!("listing reuse actor panicked")),
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
