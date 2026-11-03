#[path = "storage_control_plan.rs"]
mod storage_control_plan;

#[path = "connector_control_plan.rs"]
mod connector_control_plan;

use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, bail, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::isolated_iceberg_rest::{
    IsolatedIcebergRestFixture, IsolatedS3Identity,
};
use novarocks_cluster_harness::loopback_s3::{
    LoopbackS3Config, LoopbackS3Fixture, LoopbackS3Object, LoopbackS3Request,
};
use novarocks_cluster_harness::vended_rest_catalog::{
    VendedMutationRequest, VendedRefreshBehavior, VendedRefreshTarget, VendedReplacementAnswer,
    VendedResponseReplacement, VendedRestCatalogConfig, VendedRestCatalogFixture,
    VendedS3Credential, VendedTableCommitResponseBehavior,
};
use novarocks_cluster_harness::{
    CrossProcessChildEnvironment, CrossProcessConfigOverlay, NativeTrustFixture,
    QueryExecutionResourceSnapshot, TestClusterOwner,
};
use novarocks_secret::SecretValue;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;
use std::path::Path;
use std::sync::Mutex;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

const CONNECTOR_READER_OPEN: &str =
    novarocks_failpoint::observation_marker::CONNECTOR_UNIT_READER_OPEN;
/// The task protocol's abort marker, emitted where the abort actually
/// applied. It replaces `NOVAROCKS_QUERY_LIFECYCLE_ABORT`, which only the
/// retired chain emits and which a production query no longer reaches.
const PARTICIPATION_ABORT_APPLIED: &str =
    novarocks_failpoint::observation_marker::PARTICIPATION_ABORT_APPLIED;
const VENDED_METADATA_CREDENTIAL_NAME: &str = "vended-rest-metadata";
const VENDED_METADATA_CREDENTIAL_GENERATION: &str = "v1";
const VENDED_METADATA_ACCESS_KEY_ENV: &str = "NOVAROCKS_VENDED_METADATA_ACCESS_KEY_ID";
/// The execution node's own catalog identity (CAD-1 D1).
///
/// It is a different reference from the coordinator's, which is the point: the
/// two roles must never share a principal, and the configuration layer refuses
/// a deployment where they do.
const VENDED_EXECUTION_CREDENTIAL_NAME: &str = "vended-rest-execution";
const VENDED_EXECUTION_CREDENTIAL_GENERATION: &str = "v1";
const VENDED_EXECUTION_BEARER_ENV_PREFIX: &str = "NOVAROCKS_VENDED_EXECUTION_BEARER_BE_";
const VENDED_METADATA_SECRET_KEY_ENV: &str = "NOVAROCKS_VENDED_METADATA_SECRET_ACCESS_KEY";
/// A Backend is about to build at least one catalog runtime for one attempt.
///
/// The task protocol's successor of `NOVAROCKS_CATALOG_LOADING`, and the only
/// point at which a cold install is observably in flight: the install runs
/// inside the establish that carries the catalog bindings, not in a background
/// pass with its own Loading/Ready progression.
const CATALOG_INSTALL_STARTED: &str =
    novarocks_failpoint::observation_marker::CATALOG_INSTALL_STARTED;
/// One selected Backend's cold install was failed by the runner's trigger.
const CATALOG_INSTALL_FAILED: &str =
    novarocks_failpoint::observation_marker::CATALOG_INSTALL_FAILED;
/// A provider bind produced one catalog runtime. Protocol-neutral and live:
/// it is emitted by the catalog manager's factory set, which both the retired
/// chain and the task protocol drive, and it is the fact "this catalog is
/// usable here" actually consists of.
const CATALOG_RUNTIME_MATERIALIZED: &str =
    novarocks_failpoint::observation_marker::CATALOG_RUNTIME_MATERIALIZED;
/// One task was admitted on a Backend. The successor of
/// `NOVAROCKS_CATALOG_STAGE_ADMITTED` for ordering claims: a task is the
/// smallest thing a Backend admits, and it cannot exist before the establish
/// that installed its context's catalogs returned.
const TASK_CREATE_APPLIED: &str = novarocks_failpoint::observation_marker::TASK_CREATE_APPLIED;
/// One Backend renewed a query context's task lease.
///
/// Emitted on a timer by a Backend that still holds the context, which makes it
/// the evidence that an attempt is alive when the data path is deliberately
/// slow.
const PARTICIPATION_LEASE_RENEWED: &str =
    novarocks_failpoint::observation_marker::PARTICIPATION_LEASE_RENEWED;
/// The task protocol's establish acknowledgement drop, and the marker its
/// claim prints. It replaces the retired `InitAck` drop: `EstablishParticipation`
/// is where a query's catalog bindings cross the boundary, so it is the
/// operation whose lost answer could make one Backend install twice.
const ESTABLISH_PARTICIPATION_ACK_DROP: &str = "establish-participation-ack-drop";
const ESTABLISH_PARTICIPATION_ACK_DROPPED_MARKER: &str =
    novarocks_failpoint::observation_marker::PARTICIPATION_ESTABLISH_ACK_DROPPED;
/// The one Backend the injected catalog-install failure and the dropped
/// establish acknowledgement are armed on.
///
/// A Backend index, never a count: it is fixed so the scenario can name the
/// process its evidence must come from, and nothing here assumes how many
/// Backends a plan reaches.
const CATALOG_FAILURE_BACKEND: usize = 1;
const TYPED_SPLIT_ACCEPTED: &str =
    novarocks_failpoint::observation_marker::TASK_SPLIT_ASSIGNMENT_ACCEPTED;
const TYPED_SPLIT_NO_MORE: &str = novarocks_failpoint::observation_marker::TASK_SPLIT_NO_MORE;
const TYPED_PAGE_SOURCE_OPEN: &str =
    novarocks_failpoint::observation_marker::CONNECTOR_PAGE_SOURCE_OPEN;
const TYPED_PAGE_SOURCE_CLOSE: &str =
    novarocks_failpoint::observation_marker::CONNECTOR_PAGE_SOURCE_CLOSE;
const CONNECTOR_READER_CLOSE: &str =
    novarocks_failpoint::observation_marker::CONNECTOR_UNIT_READER_CLOSE;
const READER_CACHE_OVERLAY: &str = r#"
[runtime.cache]
page_cache_enable = true
# This fixture writes several real Parquet files. Capacity is bytes at the
# filesystem boundary, so retain enough ranges to make a repeated typed read
# a meaningful same-process cache hit check.
page_cache_capacity = 67108864
page_cache_evict_probability = 100
parquet_meta_cache_enable = true
parquet_meta_cache_ttl_seconds = 3600
parquet_page_cache_enable = true
"#;

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(DistributedReaderCancel::default()),
        Box::new(DistributedReaderKillConnection::default()),
        Box::new(CatalogFeRestartCache::default()),
        Box::new(CatalogReadyLifecycle::default()),
        Box::new(CatalogReadWriteRuntime::default()),
        Box::new(VendedRestReadWritePem::default()),
        Box::new(VendedRestRefreshPem::default()),
        Box::new(VendedCredentialFaultPem::new(
            VendedCredentialFault::TargetedDeadline,
        )),
        Box::new(VendedCredentialFaultPem::new(
            VendedCredentialFault::LateClose,
        )),
        Box::new(VendedCredentialFaultPem::new(
            VendedCredentialFault::Unreachable,
        )),
        Box::new(VendedRestWriteOutcomePem::default()),
        Box::new(VendedRestPostPublishAnswerPem::new(
            PostPublishAnswerCase::InsertNotFound,
        )),
        Box::new(VendedRestPostPublishAnswerPem::new(
            PostPublishAnswerCase::InsertBadRequest,
        )),
        Box::new(VendedRestPostPublishAnswerPem::new(
            PostPublishAnswerCase::CreateDatabaseNotFound,
        )),
        Box::new(CatalogVersionDrain::default()),
        Box::new(StaticCredentialGeneration::default()),
        Box::new(AccessDomainCacheIsolation::default()),
        Box::new(PredicatePageIndexPruning),
        Box::new(TypedReadData::default()),
        Box::new(Uea5dMultiSplitDop::default()),
    ]
}

/// A D00 correctness receipt for real Iceberg splits at two requested driver
/// widths. It does not time queries or stand in for performance evidence.
#[derive(Default)]
struct Uea5dMultiSplitDop {
    controls: connector_control_plan::Owner,
}

#[derive(Serialize)]
struct Uea5dDopObservation {
    requested_dop: i32,
    rows: (i64, i64),
    backend_markers: Vec<Vec<String>>,
}

impl Scenario for Uea5dMultiSplitDop {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "connector",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "connector/uea5d-multi-split-dop"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        CrossProcessChildEnvironment::default()
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(connector_control_plan::launch_config())
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: std::time::Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        let (specs, fe_restart, be_restarts) = connector_control_plan::specs(self.name())?;
        self.controls
            .freeze(
                specs,
                fe_restart,
                &be_restarts,
                cluster_size,
                binary,
                original_deadline,
            )
            .map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect_until(&user, port, context.deadline())?;
        let control_phase = (|| -> Result<()> {
            const CATALOG: &str = "uea5d_dop_catalog";
            const DATABASE: &str = "uea5d_dop_db";
            const TABLE: &str = "uea5d_dop_data";
            let warehouse = create_warehouse(context, "uea5d-multi-split-dop")?;
            context.action("create three independent Iceberg data files for UEA-5D DOP reads");
            create_catalog_table_and_data(&mut control, CATALOG, DATABASE, TABLE, &warehouse)?;

            let mut observations = Vec::new();
            for (index, dop) in [1, 4].into_iter().enumerate() {
                control
                    .query_drop(format!("SET pipeline_dop = {dop}"))
                    .with_context(|| format!("select requested read DOP {dop}"))?;
                let before = backend_log_snapshots(context)?;
                context.action(format!(
                    "read three Iceberg splits with requested DOP {dop}"
                ));
                let rows: Vec<(i64, i64)> = connector_control_plan::query(
                    context,
                    &self.controls.take(index)?,
                    &mut control,
                    &format!("SELECT count(*), sum(v) FROM {CATALOG}.{DATABASE}.{TABLE}"),
                )
                .with_context(|| format!("read UEA-5D multi-split fixture at DOP {dop}"))?;
                ensure!(
                    rows == [(300_000, 45_000_150_000)],
                    "DOP {dop} typed read returned {rows:?}, expected count and sum"
                );

                let logs = wait_for_backend_logs_while(
                    context,
                    &format!("observe DOP {dop} split completion on multiple backends"),
                    None,
                    |logs| {
                        let added = appended_since(logs, &before, "UEA-5D DOP read")?;
                        let added = added
                            .iter()
                            .map(|log| (*log).to_owned())
                            .collect::<Vec<_>>();
                        Ok(assert_typed_split_evidence(&added).is_ok())
                    },
                )?;
                let added = appended_since(&logs, &before, "UEA-5D DOP read")?
                    .into_iter()
                    .map(str::to_owned)
                    .collect::<Vec<_>>();
                assert_typed_split_evidence(&added)?;
                let actual_dop_marker = format!("dop={dop}");
                ensure!(
                    added.iter().any(|log| {
                        let scan_instances = log
                            .lines()
                            .filter(|line| line.contains(TYPED_SPLIT_ACCEPTED))
                            .filter_map(|line| line.split_once("task_key=").map(|(_, rest)| rest))
                            .filter_map(|rest| rest.split_whitespace().next())
                            .collect::<Vec<_>>();
                        log.lines().any(|line| {
                            line.contains(
                                novarocks_failpoint::observation_marker::TASK_PREPARED_DOP,
                            ) && line.contains(&actual_dop_marker)
                                && scan_instances
                                    .iter()
                                    .any(|instance| line.contains(&format!("task_key={instance} ")))
                        })
                    }),
                    "DOP {dop} read produced no split-owning prepared task with {actual_dop_marker}"
                );
                observations.push(Uea5dDopObservation {
                    requested_dop: dop,
                    rows: rows[0],
                    backend_markers: added
                        .iter()
                        .map(|log| {
                            log.lines()
                                .filter(|line| {
                                    line.contains(TYPED_SPLIT_ACCEPTED)
                                        || line.contains(TYPED_SPLIT_NO_MORE)
                                        || line.contains(TYPED_PAGE_SOURCE_OPEN)
                                        || line.contains(TYPED_PAGE_SOURCE_CLOSE)
                                        || line.contains(novarocks_failpoint::observation_marker::TASK_PREPARED_DOP)
                                })
                                .map(str::to_owned)
                                .collect()
                        })
                        .collect(),
                });
            }
            std::fs::write(
                context.scenario_root().join("uea5d-multi-split-dop.json"),
                serde_json::to_vec_pretty(&observations)?,
            )
            .context("persist exact UEA-5D multi-split DOP observations")?;
            await_resource_convergence(context, &baseline, "UEA-5D multi-split DOP reads")?;
            Ok(())
        })();
        connector_control_plan::with_cleanup(control_phase, control.finish())
    }
}

/// Proves a typed connector read works on the real 1FE+3BE topology, and that
/// its splits are delivered at runtime rather than frozen into the plan.
///
/// A correct result alone would not show that: a single backend reading every
/// file would produce exactly the same rows. The evidence that distinguishes
/// the two is which processes accepted split assignments.
#[derive(Default)]
struct TypedReadData {
    controls: connector_control_plan::Owner,
}

impl Scenario for TypedReadData {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "connector",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "connector/iceberg-typed-read-data"
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        CrossProcessChildEnvironment::default()
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(connector_control_plan::launch_config())
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: std::time::Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        let (specs, fe_restart, be_restarts) = connector_control_plan::specs(self.name())?;
        self.controls
            .freeze(
                specs,
                fe_restart,
                &be_restarts,
                cluster_size,
                binary,
                original_deadline,
            )
            .map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect_until(&user, port, context.deadline())?;
        let control_phase = (|| -> Result<()> {
            // Cache service availability alone is intentionally inert: this
            // scenario explicitly opts its control session into both cache reads
            // and population before asserting the warm/cached profiles below.
            control
                .query_drop("SET enable_scan_datacache = true")
                .context("enable cache reads for typed connector data")?;
            control
                .query_drop("SET enable_populate_datacache = true")
                .context("enable cache population for typed connector data")?;

            const CATALOG: &str = "typed_read_catalog";
            const DATABASE: &str = "typed_read_db";
            const TABLE: &str = "typed_read_data";
            let warehouse = create_warehouse(context, "iceberg-typed-read-data")?;

            // Three files, three backends: fewer splits than backends could not
            // show distribution even if it worked.
            context.action("create three independent Iceberg data files");
            create_catalog_table_and_data(&mut control, CATALOG, DATABASE, TABLE, &warehouse)?;

            let counted_query = format!("SELECT count(*) FROM {CATALOG}.{DATABASE}.{TABLE}");
            context.action("warm the enabled role-local cache through a typed connector read");
            let warm_profile: Vec<String> = control
                .query(format!("EXPLAIN ANALYZE {counted_query}"))
                .context("collect cache-warming typed connector EXPLAIN ANALYZE profile")?;
            let warm_profile = warm_profile.join("\n");
            assert_positive_profile_counter(&warm_profile, "ConnectorFileCacheMisses")?;

            context.action("repeat the same typed connector read from the role-local cache");
            let cached_profile: Vec<String> = control
                .query(format!("EXPLAIN ANALYZE {counted_query}"))
                .context("collect cached typed connector EXPLAIN ANALYZE profile")?;
            let cached_profile = cached_profile.join("\n");
            assert_positive_profile_counter(&cached_profile, "ConnectorFileCacheHits")?;

            context.action("read every row through the typed connector stack");
            let counted: Vec<i64> = connector_control_plan::query(
                context,
                &self.controls.take(0)?,
                &mut control,
                &counted_query,
            )
            .context("count rows through the typed connector read")?;
            if counted != [300_000] {
                bail!("typed connector read returned {counted:?} rows, expected [300000]");
            }
            // A count alone can be right while the values are not; the sum pins
            // which rows were read, not just how many.
            let summed: Vec<i64> = control
                .query(format!("SELECT sum(v) FROM {CATALOG}.{DATABASE}.{TABLE}"))
                .context("sum values through the typed connector read")?;
            if summed != [45_000_150_000] {
                bail!("typed connector read summed {summed:?}, expected [45000150000]");
            }

            context.action("assert splits reached more than one backend process");
            let logs = wait_for_backend_logs(context, "observe typed split assignments", |logs| {
                logs.iter()
                    .filter(|log| log.contains(TYPED_SPLIT_ACCEPTED))
                    .count()
                    >= 2
            })?;
            assert_typed_split_evidence(&logs)?;

            await_resource_convergence(context, &baseline, "typed connector read")?;
            Ok(())
        })();
        connector_control_plan::with_cleanup(control_phase, control.finish())
    }
}

/// Every backend that accepted a split must have opened a page source for it
/// and closed it, and must have been told the assignment is terminal.
///
/// Counting opens against closes is what separates "read and released" from
/// "read and leaked"; counting accepted backends is what separates a
/// distributed read from one backend doing all of it.
fn assert_typed_split_evidence(logs: &[String]) -> Result<()> {
    let mut accepting_backends = 0_usize;
    for (index, log) in logs.iter().enumerate() {
        let accepted = log.matches(TYPED_SPLIT_ACCEPTED).count();
        if accepted == 0 {
            continue;
        }
        accepting_backends += 1;
        if !log.contains(TYPED_SPLIT_NO_MORE) {
            bail!(
                "BE[{index}] accepted {accepted} split assignments but was never told the \
                 assignment is terminal, so its scan could still be waiting"
            );
        }
        let opens = log.matches(TYPED_PAGE_SOURCE_OPEN).count();
        let closes = log.matches(TYPED_PAGE_SOURCE_CLOSE).count();
        if opens == 0 {
            bail!(
                "BE[{index}] accepted {accepted} split assignments and opened no page source: \
                 the splits arrived and were never read"
            );
        }
        if opens != closes {
            bail!("BE[{index}] opened {opens} page sources and closed {closes}");
        }
    }
    if accepting_backends < 2 {
        bail!(
            "only {accepting_backends} backend accepted a split assignment; a read served by one \
             backend cannot show that assignment is distributed at runtime"
        );
    }
    Ok(())
}

#[derive(Default)]
struct DistributedReaderCancel {
    controls: connector_control_plan::Owner,
}

impl Scenario for DistributedReaderCancel {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "connector",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "connector/distributed-reader-cancel"
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        CrossProcessChildEnvironment::default()
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(connector_control_plan::launch_config())
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: std::time::Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        let (specs, fe_restart, be_restarts) = connector_control_plan::specs(self.name())?;
        self.controls
            .freeze(
                specs,
                fe_restart,
                &be_restarts,
                cluster_size,
                binary,
                original_deadline,
            )
            .map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect_until(&user, port, context.deadline())?;
        let control_phase = (|| -> Result<()> {
            let warehouse = create_warehouse(context, "distributed-reader-cancel")?;
            context.action("create Hadoop Iceberg catalog and three independent data files");
            create_catalog_table_and_data(
                &mut control,
                "connector_cancel_catalog",
                "connector_cancel_db",
                "connector_cancel_data",
                &warehouse,
            )?;

            context.action("start a public-MySQL distributed read that retains connector readers");
            let baseline_logs = backend_log_snapshots(context)?;
            let selected = self.controls.take(0)?;
            let owned =
                connector_control_plan::Reader::start(context, &selected, &user, port, None)?;
            let target = &owned.read;
            let phase = (|| -> Result<()> {
                let connection_id = target
                    .ready
                    .recv_timeout(context.remaining("receive connector read connection id")?)
                    .context("connector read terminated before publishing its connection id")?;

                wait_for_new_reader_on_every_backend(
                    context,
                    "connector_cancel_catalog",
                    "wait for every BE to open a distributed connector reader",
                    &baseline_logs,
                    AwaitedRead::new(target, "the connector read"),
                )?;
                // Kept alongside the barrier's own liveness check rather than folded
                // into it: the barrier covers the wait, this covers the instant
                // between its last poll and the cancellation below.
                if let Ok(result) = target.done.try_recv() {
                    bail!("connector read completed before cancellation was issued: {result:?}");
                }

                context.action(format!(
                    "cancel connector read through KILL QUERY {connection_id}"
                ));
                control
                    .query_drop(format!("KILL QUERY {connection_id}"))
                    .context("issue public MySQL KILL QUERY for connector read")?;
                assert_cancelled_query(
                    &target.done,
                    context.remaining("await connector read cancellation")?,
                )?;
                assert_target_connection_remains_usable(
                    target,
                    context.remaining("verify KILL QUERY target connection remains usable")?,
                )?;
                assert_idle_query(&mut control, connection_id)?;
                release_connector_read(target)?;
                Ok(())
            })();
            owned.finish_after(context, &mut control, phase)?;

            let reader_logs = wait_for_balanced_reader_lifecycle(
                context,
                "wait for connector reader close after cancellation",
            )?;
            assert_no_reader_open_after_abort(&reader_logs)?;
            await_resource_convergence(context, &baseline, "cancelled connector read")?;

            context
                .action("verify a subsequent distributed query succeeds after connector cleanup");
            let rows: Vec<i64> = control
                .query("SELECT v FROM (SELECT 1 AS v UNION ALL SELECT 2) t ORDER BY v")
                .context("run post-cancellation distributed query")?;
            if rows != [1, 2] {
                bail!("post-cancellation distributed query returned {rows:?}, expected [1, 2]");
            }
            Ok(())
        })();
        connector_control_plan::with_cleanup(control_phase, control.finish())
    }
}

#[derive(Default)]
struct DistributedReaderKillConnection {
    controls: connector_control_plan::Owner,
}

impl Scenario for DistributedReaderKillConnection {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "connector",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "connector/distributed-reader-kill-connection"
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        CrossProcessChildEnvironment::default()
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(connector_control_plan::launch_config())
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: std::time::Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        let (specs, fe_restart, be_restarts) = connector_control_plan::specs(self.name())?;
        self.controls
            .freeze(
                specs,
                fe_restart,
                &be_restarts,
                cluster_size,
                binary,
                original_deadline,
            )
            .map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect_until(&user, port, context.deadline())?;
        let control_phase = (|| -> Result<()> {
            let warehouse = create_warehouse(context, "distributed-reader-kill-connection")?;
            context.action("create Hadoop Iceberg catalog and three independent data files");
            create_catalog_table_and_data(
                &mut control,
                "connector_kill_connection_catalog",
                "connector_kill_connection_db",
                "connector_kill_connection_data",
                &warehouse,
            )?;

            context.action("start a public-MySQL distributed read that retains connector readers");
            let baseline_logs = backend_log_snapshots(context)?;
            let selected = self.controls.take(0)?;
            let owned =
                connector_control_plan::Reader::start(context, &selected, &user, port, None)?;
            let target = &owned.read;
            let phase = (|| -> Result<()> {
                let connection_id = target
                    .ready
                    .recv_timeout(context.remaining("receive KILL CONNECTION target id")?)
                    .context(
                        "KILL CONNECTION target terminated before publishing its connection id",
                    )?;
                wait_for_new_reader_on_every_backend(
                    context,
                    "connector_kill_connection_catalog",
                    "wait for every BE to open a KILL CONNECTION target reader",
                    &baseline_logs,
                    AwaitedRead::new(target, "the KILL CONNECTION target read"),
                )?;

                context.action(format!(
            "terminate the active public-MySQL reader through KILL CONNECTION {connection_id}"
        ));
                control
                    .query_drop(format!("KILL CONNECTION {connection_id}"))
                    .context("issue public MySQL KILL CONNECTION for connector read")?;
                assert_connection_killed_query(
                    &target.done,
                    context.remaining("await KILL CONNECTION target query termination")?,
                )?;
                assert_target_connection_is_closed(
                    target,
                    context.remaining("verify KILL CONNECTION closes the target socket")?,
                )?;
                release_connector_read(target)?;
                Ok(())
            })();
            owned.finish_after(context, &mut control, phase)?;

            let reader_logs = wait_for_balanced_reader_lifecycle(
                context,
                "wait for connector reader close after KILL CONNECTION",
            )?;
            assert_no_reader_open_after_abort(&reader_logs)?;
            await_resource_convergence(context, &baseline, "KILL CONNECTION connector read")?;

            context.action("verify bare KILL closes an idle public-MySQL target socket");
            let idle_owned = connector_control_plan::Idle::start(context, &user, port)?;
            let idle_target = &idle_owned.client;
            let idle_phase = (|| -> Result<()> {
                let idle_connection_id = idle_target
                    .ready
                    .recv_timeout(context.remaining("receive bare KILL target id")?)
                    .context("bare KILL target terminated before publishing its connection id")?;
                control
                    .query_drop(format!("KILL {idle_connection_id}"))
                    .context("issue bare public MySQL KILL for idle target")?;
                assert_idle_target_connection_is_closed(
                    idle_target,
                    context.remaining("verify bare KILL closes the idle target socket")?,
                )?;
                Ok(())
            })();
            idle_owned.finish_after(&mut control, idle_phase)?;

            context
                .action("verify the KILL requester remains usable after both target terminations");
            let rows: Vec<i64> = control
                .query("SELECT v FROM (SELECT 1 AS v UNION ALL SELECT 2) t ORDER BY v")
                .context("run requester query after KILL CONNECTION and bare KILL")?;
            if rows != [1, 2] {
                bail!("post-KILL requester query returned {rows:?}, expected [1, 2]");
            }
            Ok(())
        })();
        connector_control_plan::with_cleanup(control_phase, control.finish())
    }
}

#[derive(Default)]
struct CatalogVersionDrain {
    controls: connector_control_plan::Owner,
}

/// Drives one CatalogSet through its observable cold and warm lifecycle on
/// the real control stream, including cancellation while installation is held.
#[derive(Default)]
struct CatalogReadyLifecycle {
    controls: connector_control_plan::Owner,
}

impl Scenario for CatalogReadyLifecycle {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "connector",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "connector/catalog-ready-lifecycle"
    }

    fn launch_config(&self, _scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        Ok(connector_control_plan::launch_config())
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: std::time::Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        let (specs, fe_restart, be_restarts) = connector_control_plan::specs(self.name())?;
        self.controls
            .freeze(
                specs,
                fe_restart,
                &be_restarts,
                cluster_size,
                binary,
                original_deadline,
            )
            .map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect_until(&user, port, context.deadline())?;
        let control_phase = (|| -> Result<()> {
            const CATALOG: &str = "catalog_ready_lifecycle";
            const DATABASE: &str = "catalog_ready_db";
            const TABLE: &str = "catalog_ready_data";
            let warehouse = create_warehouse(context, "catalog-ready-lifecycle")?;
            create_catalog_table_and_data(&mut control, CATALOG, DATABASE, TABLE, &warehouse)?;

            // A cold install that is cancelled while it is still held.
            //
            // The task protocol installs a query context's catalogs inside the
            // establish that carries them, so "the install is in flight" lasts a
            // few milliseconds and cancellation would otherwise always land
            // before or after it. The hold is what makes that instant long enough
            // to act on, and the marker is what proves this backend is inside it.
            control
                .query_drop(format!("DROP CATALOG {CATALOG}"))
                .context("replace warm catalog before held cancellation")?;
            create_catalog(&mut control, CATALOG, &warehouse)?;
            let before_cancel = backend_log_snapshots(context)?;
            let held_selection = self.controls.take(0)?;
            held_selection.trigger(connector_control_plan::HOLD, "hold\n")?;
            context
                .action("hold a cold catalog install, then cancel it before any runtime is built");
            // The scenario-bounded connection form, because this statement is
            // parked before it produces a single row: its first response byte is
            // the cancellation itself, so a socket read timeout would turn any
            // server latency into EAGAIN and destroy the measurement below.
            let cancelled_owner = connector_control_plan::Reader::start_with_hold(
                context,
                &held_selection,
                &user,
                port,
            )?;
            let cancelled = &cancelled_owner.read;
            let phase = (|| -> Result<()> {
                let connection_id = cancelled
                    .ready
                    .recv_timeout(context.remaining("receive held catalog query connection id")?)
                    .context("held catalog query terminated before publishing connection id")?;
                let installing = wait_for_cold_catalog_install(
                    context,
                    &before_cancel,
                    cancelled,
                    "observe a held cold catalog install",
                )?;
                // The hold is before the provider bind, so nothing may be usable yet.
                // This is the assertion that gives the whole phase its meaning: it is
                // checked against a marker this same scenario later observes
                // appearing, so it cannot pass because the marker has no emitter.
                assert_no_appended_catalog_runtime(
                    context,
                    &before_cancel,
                    CATALOG,
                    "while the cold catalog install is held",
                )?;
                // And nothing may execute against a catalog that is not usable. On
                // the retired chain this was `no Stage was admitted`; a task cannot be
                // created before its context's establish returns, so the observable
                // form of the same claim is that no reader for this catalog opened.
                assert_no_appended_reader_open(
                    context,
                    &before_cancel,
                    CATALOG,
                    "while the cold catalog install is held",
                )?;
                control
                    .query_drop(format!("KILL QUERY {connection_id}"))
                    .context("cancel query while catalog install is held")?;
                let killed_at = std::time::Instant::now();
                // Delivery of the abort is asserted before the client's error code,
                // and the order is load-bearing twice over.
                //
                // It is the more primitive fact: the backend standing its held
                // install down is what has to happen for the client to be owed
                // anything at all. Asserting it first splits a single opaque failure
                // into two different diagnoses -- an abort that never reached a
                // backend parked in its establish, versus one that did while the
                // frontend still failed to report the interrupt.
                //
                // And it must precede lifting the hold: the held install ends on
                // either the abort or the hold file, so lifting the file first would
                // let a cancellation that had not arrived yet race a provider bind
                // that then really would make the catalog usable.
                wait_for_participation_abort_on(
                    context,
                    &before_cancel,
                    &installing,
                    "observe the abort reach every Backend holding a cold catalog install",
                )?;
                // `KILL QUERY` owes this client 1317, and owes it only once the
                // coordinator's worker has unwound: the frontend deliberately
                // withholds the interrupt until the statement generation is released,
                // so that the probe below can reuse this connection
                // (`cancellation_requires_statement_fence` in
                // `novarocks/frontend-application/src/query.rs`). By this line the abort has
                // already reached every installing Backend, so anything other than a
                // prompt 1317 is the frontend failing to report an interrupt it owes,
                // never the expectation being wrong.
                assert_held_query_interrupted(
                    &cancelled.done,
                    killed_at,
                    HELD_QUERY_INTERRUPT_BUDGET
                        .min(context.remaining("await held catalog query cancellation")?),
                )?;
                assert_target_connection_remains_usable(
                    cancelled,
                    context.remaining("verify KILL QUERY preserves held client connection")?,
                )?;
                held_selection.release(connector_control_plan::HOLD)?;
                release_connector_read(cancelled)?;
                Ok(())
            })();
            cancelled_owner.finish_after(context, &mut control, phase)?;
            await_resource_convergence(context, &baseline, "cancelled catalog install")?;
            // Re-checked over the whole cancelled window, after convergence: the
            // attempt is fully unwound by now, so a runtime that appeared here
            // would be one the cancelled install left behind.
            assert_no_appended_catalog_runtime(
                context,
                &before_cancel,
                CATALOG,
                "as a result of the cancelled catalog install",
            )?;
            assert_no_appended_reader_open(
                context,
                &before_cancel,
                CATALOG,
                "as a result of the cancelled catalog install",
            )?;

            // One backend's cold install fails, and the retry after the trigger is
            // cleared has to succeed on that same backend.
            control
                .query_drop(format!("DROP CATALOG {CATALOG}"))
                .context("replace cancelled catalog before injected install failure")?;
            create_catalog(&mut control, CATALOG, &warehouse)?;
            let failure_selection = self.controls.take(1)?;
            let before_failure = backend_log_snapshots(context)?;
            failure_selection.trigger(connector_control_plan::FAILURE, "fail\n")?;
            context.action("fail the cold catalog install on one Backend and reject the query");
            let failed = connector_control_plan::outcome::<i64>(
                context,
                &failure_selection,
                &mut control,
                &format!("SELECT count(*) FROM {CATALOG}.{DATABASE}.{TABLE}"),
            );
            let failed_query = connector_control_plan::with_cleanup(
                failed,
                failure_selection.release(connector_control_plan::FAILURE),
            )?;
            match failed_query {
                Err(mysql::Error::MySqlError(_)) => {}
                Ok(rows) => bail!("catalog install failure query unexpectedly succeeded: {rows:?}"),
                Err(error) => {
                    bail!("catalog install failure did not return a public SQL error: {error}")
                }
            }
            wait_for_catalog_lifecycle_marker_on_backend(
                context,
                &before_failure,
                CATALOG_FAILURE_BACKEND,
                CATALOG_INSTALL_FAILED,
                "observe the injected catalog install failure from the selected Backend",
            )?;
            // Only on the Backend the failure was injected into, and deliberately
            // not cluster-wide: the task protocol has no cross-Backend establish
            // barrier, so a Backend whose own install succeeded may create its
            // task and open its reader while this one is still failing. What the
            // refusal has to mean is that *this* Backend never held usable
            // catalog facts, and therefore never read through them.
            assert_no_appended_reader_open_on_backend(
                context,
                &before_failure,
                CATALOG_FAILURE_BACKEND,
                CATALOG,
                "after the injected catalog install failure",
            )?;
            let before_retry = backend_log_snapshots(context)?;
            context
                .action("retry the failed catalog version after clearing the one-Backend failure");
            let rows: Vec<i64> = connector_control_plan::query(
                context,
                &self.controls.take(2)?,
                &mut control,
                &format!("SELECT count(*) FROM {CATALOG}.{DATABASE}.{TABLE}"),
            )
            .context("retry catalog query after clearing injected install failure")?;
            if rows != [300_000] {
                bail!("catalog install retry returned {rows:?}, expected [300000]");
            }
            // Asserted on the one backend the failure was injected into, because
            // it is the only one whose cell was never built: the others took the
            // whole-set ready fast path on the retry and correctly rebuild
            // nothing. A cluster-wide count here would be asserting that.
            wait_for_catalog_runtime_on_backend(
                context,
                &before_retry,
                CATALOG_FAILURE_BACKEND,
                CATALOG,
                "observe the formerly failing Backend build this catalog's runtime on retry",
            )?;

            // A cold install that is released rather than cancelled: the runtime
            // has to exist on a backend before that backend admits any task of the
            // query that asked for it.
            control
                .query_drop(format!("DROP CATALOG {CATALOG}"))
                .context("replace retried catalog before cold ready path")?;
            create_catalog(&mut control, CATALOG, &warehouse)?;
            let before_cold = backend_log_snapshots(context)?;
            let cold_selection = self.controls.take(3)?;
            cold_selection.trigger(connector_control_plan::HOLD, "hold\n")?;
            context.action("hold a cold catalog install until every installing Backend reports it");
            let cold_owner = connector_control_plan::Reader::start_with_hold(
                context,
                &cold_selection,
                &user,
                port,
            )?;
            let cold = &cold_owner.read;
            let phase = (|| -> Result<()> {
                let cold_connection_id = cold
                    .ready
                    .recv_timeout(context.remaining("receive cold catalog query connection id")?)
                    .context("cold catalog query terminated before publishing connection id")?;
                let installing = wait_for_cold_catalog_install(
                    context,
                    &before_cold,
                    cold,
                    "observe the cold catalog install before releasing it",
                )?;
                assert_no_appended_catalog_runtime(
                    context,
                    &before_cold,
                    CATALOG,
                    "before the cold catalog install is released",
                )?;
                context.action(
                    "release the cold catalog install and require the runtime before any task",
                );
                cold_selection.release(connector_control_plan::HOLD)?;
                let after_cold = wait_for_open_reader_on(
                    context,
                    &before_cold,
                    CATALOG,
                    &installing,
                    "observe a reader on every Backend that installed the cold catalog",
                )?;
                assert_catalog_runtime_precedes_task_create(
                    &before_cold,
                    &after_cold,
                    &installing,
                    CATALOG,
                )?;
                control
                    .query_drop(format!("KILL QUERY {cold_connection_id}"))
                    .context("cancel cold catalog reader after its runtime exists")?;
                assert_cancelled_query(
                    &cold.done,
                    context.remaining("await cold catalog reader cancellation")?,
                )?;
                assert_target_connection_remains_usable(
                    cold,
                    context.remaining("verify cold catalog KILL QUERY connection")?,
                )?;
                release_connector_read(cold)?;
                Ok(())
            })();
            cold_owner.finish_after(context, &mut control, phase)?;

            // A warm query reuses the runtime the cold one built.
            let before_warm = backend_log_snapshots(context)?;
            context.action("execute a warm query without another catalog install");
            let rows: Vec<i64> = connector_control_plan::query(
                context,
                &self.controls.take(4)?,
                &mut control,
                &format!("SELECT count(*) FROM {CATALOG}.{DATABASE}.{TABLE}"),
            )
            .context("run warm catalog query")?;
            if rows != [300_000] {
                bail!("warm catalog query returned {rows:?}, expected [300000]");
            }
            let after_warm = backend_log_snapshots(context)?;
            assert_no_appended_marker(
                &before_warm,
                &after_warm,
                CATALOG_INSTALL_STARTED,
                "during a warm catalog query",
            )?;
            assert_no_appended_catalog_runtime(
                context,
                &before_warm,
                CATALOG,
                "during a warm catalog query",
            )?;

            // A dropped establish acknowledgement is recovered by replaying the
            // exact establish, and the replay must not build a second runtime.
            //
            // This replaced the retired InitAck drop: `EstablishParticipation` is
            // where the task protocol carries a query's catalog bindings, so it is
            // the operation whose lost answer can make a backend install twice.
            control
                .query_drop(format!("DROP CATALOG {CATALOG}"))
                .context("replace warm catalog before establish-replay coverage")?;
            create_catalog(&mut control, CATALOG, &warehouse)?;
            let before_replay = backend_log_snapshots(context)?;
            let before_ack_drop = context.handle().be_log_count(
                CATALOG_FAILURE_BACKEND,
                ESTABLISH_PARTICIPATION_ACK_DROPPED_MARKER,
            )?;
            context.action("retry the exact cold establish after its acknowledgement is dropped");
            let rows: Vec<i64> = connector_control_plan::query(
                context,
                &self.controls.take(5)?,
                &mut control,
                &format!("SELECT count(*) FROM {CATALOG}.{DATABASE}.{TABLE}"),
            )
            .context("execute cold catalog query with a dropped establish acknowledgement")?;
            context
                .handle()
                .clear_query_lifecycle_faults()
                .context("clear the establish acknowledgement drop")?;
            if rows != [300_000] {
                bail!("establish replay catalog query returned {rows:?}, expected [300000]");
            }
            let after_replay = backend_log_snapshots(context)?;
            // The fault has to have fired. Without this the phase passes on a
            // query whose acknowledgement was never dropped, which is the shape a
            // retired fault leaves behind.
            // Fault claims print on stderr, unlike the authorized catalog
            // observers on stdout. The owner counts each raw stream separately;
            // no byte cursor or ordering across the two streams is inferred.
            let after_ack_drop = context.handle().be_log_count(
                CATALOG_FAILURE_BACKEND,
                ESTABLISH_PARTICIPATION_ACK_DROPPED_MARKER,
            )?;
            ensure!(
                after_ack_drop.checked_sub(before_ack_drop) == Some(1),
                "BE[{CATALOG_FAILURE_BACKEND}] must emit exactly one {ESTABLISH_PARTICIPATION_ACK_DROPPED_MARKER} in the selected replay window; before={before_ack_drop}, after={after_ack_drop}",
            );
            assert_catalog_runtime_built_at_most_once(&before_replay, &after_replay, CATALOG)?;
            // A prepared task with no registered fragment must release its query
            // context at terminal settlement, not wait for the 300-second query
            // timeout. Allow ample scheduling slack while enforcing that owner.
            let release_deadline = context
                .deadline()
                .min(std::time::Instant::now() + Duration::from_secs(30));
            context.action("await catalog ready lifecycle resource release within 30 seconds");
            context
                .handle()
                .await_query_execution_resource_convergence(&baseline, release_deadline)
                .context("catalog ready lifecycle resource release")?;
            // This scenario never kills a Backend. Resource convergence alone
            // treats process exit as release, so also require every process to
            // survive the complete catalog lifecycle.
            let final_resources = resource_baseline(context)?;
            if !final_resources.fe_running
                || final_resources
                    .backends
                    .iter()
                    .any(|backend| !backend.process_running)
            {
                bail!("catalog ready lifecycle lost a native process: {final_resources:?}");
            }
            Ok(())
        })();
        connector_control_plan::with_cleanup(control_phase, control.finish())
    }
}

/// Exercises distributed writes and reads through one exact catalog runtime,
/// then proves a replacement Backend can rebuild that runtime from the frozen
/// catalog properties.
#[derive(Default)]
struct CatalogReadWriteRuntime {
    controls: connector_control_plan::Owner,
}

impl Scenario for CatalogReadWriteRuntime {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "connector",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "connector/catalog-read-write-runtime"
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        CrossProcessChildEnvironment::default()
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(connector_control_plan::launch_config())
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: std::time::Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        let (specs, fe_restart, be_restarts) = connector_control_plan::specs(self.name())?;
        self.controls
            .freeze(
                specs,
                fe_restart,
                &be_restarts,
                cluster_size,
                binary,
                original_deadline,
            )
            .map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect_until(&user, port, context.deadline())?;
        let control_phase = (|| -> Result<()> {
            const CATALOG: &str = "catalog_read_write_runtime";
            const DATABASE: &str = "catalog_read_write_db";
            const TABLE: &str = "catalog_read_write_data";
            let warehouse = create_warehouse(context, "catalog-read-write-runtime")?;
            create_catalog(&mut control, CATALOG, &warehouse)?;
            control
                .query_drop(format!("CREATE DATABASE {CATALOG}.{DATABASE}"))
                .context("create catalog read-write database")?;
            control
                .query_drop(format!(
                    "CREATE TABLE {CATALOG}.{DATABASE}.{TABLE} (v BIGINT)"
                ))
                .context("create catalog read-write table")?;

            context.action("write and read through the catalog runtime on every Backend");
            for (index, range) in ["1, 1000", "1001, 2000", "2001, 3000"]
                .into_iter()
                .enumerate()
            {
                connector_control_plan::execute(context, &self.controls.take(index)?, &mut control,
                &format!("INSERT INTO {CATALOG}.{DATABASE}.{TABLE} SELECT generate_series FROM TABLE(generate_series({range}))"))
                .with_context(|| format!("distributed insert range {range} through catalog writer runtime"))?;
            }
            let rows: Vec<(i64, i64)> = connector_control_plan::query(
                context,
                &self.controls.take(3)?,
                &mut control,
                &format!("SELECT count(*), sum(v) FROM {CATALOG}.{DATABASE}.{TABLE}"),
            )?;
            ensure!(
                rows == [(3_000, 4_501_500)],
                "catalog read-write summary returned {rows:?}, expected [(3000,4501500)]"
            );
            let before_restart_logs = wait_for_open_reader_on_every_backend(
                context,
                CATALOG,
                "observe catalog readers after distributed write",
            )?;
            let before_restart_versions = reader_catalog_versions(&before_restart_logs, CATALOG)?;
            let before_restart_materializations =
                catalog_materialization_counts(&before_restart_logs);

            context.action(
                "replace one Backend and rebuild its catalog runtime from CatalogProperties",
            );
            let original_process = context.handle().backend_process_id(0)?;
            let deadline = context.deadline();
            context
                .handle()
                .restart_be_until(0, deadline)
                .context("restart BE[0] for catalog runtime reconstruction")?;
            let replacement_process = context.handle().backend_process_id(0)?;
            if replacement_process == original_process {
                bail!("replacement BE[0] retained its previous process identity");
            }

            connector_control_plan::execute(
                context,
                &self.controls.take(4)?,
                &mut control,
                &format!("INSERT INTO {CATALOG}.{DATABASE}.{TABLE} VALUES (1001)"),
            )
            .context("distributed insert after BE replacement")?;
            let rows: Vec<(i64, i64)> = connector_control_plan::query(
                context,
                &self.controls.take(5)?,
                &mut control,
                &format!("SELECT count(*), sum(v) FROM {CATALOG}.{DATABASE}.{TABLE}"),
            )?;
            ensure!(
                rows == [(3_001, 4_502_501)],
                "catalog read-write summary returned {rows:?}, expected [(3001,4502501)]"
            );
            let after_restart_logs = wait_for_backend_logs(
                context,
                "observe rebuilt catalog runtime after BE replacement",
                |logs| {
                    let counts = catalog_materialization_counts(logs);
                    counts[0] > 0
                        && counts[1] == before_restart_materializations[1]
                        && counts[2] == before_restart_materializations[2]
                },
            )?;
            let after_restart_versions = reader_catalog_versions(&after_restart_logs, CATALOG)?;
            if after_restart_versions != before_restart_versions {
                bail!(
                    "BE replacement changed catalog versions: before={before_restart_versions:?}, after={after_restart_versions:?}"
                );
            }

            await_resource_convergence(context, &baseline, "catalog read-write runtime")?;
            Ok(())
        })();
        connector_control_plan::with_cleanup(control_phase, control.finish())
    }
}

/// Reserved M2 acceptance entrypoint for the real REST-vended read/write
/// path. It is explicit-only because the fixture must own its isolated REST
/// catalog and S3 credential authority rather than consuming shared Docker
/// state.
#[derive(Default)]
struct VendedRestReadWritePem {
    fixture: Mutex<Option<VendedRestSystemFixture>>,
    controls: connector_control_plan::Owner,
}

struct VendedRestSystemFixture {
    rest: IsolatedIcebergRestFixture,
    proxy: VendedRestCatalogFixture,
}

impl Scenario for VendedRestReadWritePem {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "connector",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "connector/vended-credential-read-write"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        CrossProcessChildEnvironment::default()
    }

    fn launch_config_with_fixture_run(
        &self,
        scenario_root: &std::path::Path,
        budget: &novarocks_cluster_harness::fixture_run::RunBudget,
    ) -> Result<ScenarioLaunchConfig> {
        let mut rest = IsolatedIcebergRestFixture::start_with_shared_minio(scenario_root, budget)
            .context("start isolated REST and MinIO fixture for vended credentials")?;
        // Bootstrap the namespace and target table with the fixture's private
        // catalog authority before the vended proxy exists. NovaRocks must
        // observe and use that table solely through a per-attempt lease.
        rest.provision_empty_table("vended_rest_db", "vended_rest_data")
            .context("provision isolated vended REST source table")?;
        let endpoints = rest.endpoints().clone();
        let metadata_identity = rest.static_s3_identity();
        let identities = rest
            .provision_vended_s3_identities()
            .context("provision isolated initial and rotated vended S3 identities")?;
        let proxy = VendedRestCatalogFixture::start(VendedRestCatalogConfig {
            downstream: endpoints.rest_uri.clone(),
            scope_prefix: format!("{}/", endpoints.rest_warehouse.trim_end_matches('/')),
            initial: VendedS3Credential::new(
                identities.initial.access_key_id,
                SecretValue::new(identities.initial.secret_access_key),
                SecretValue::new(identities.initial.session_token),
            )
            .and_then(|credential| {
                credential.with_not_after_unix_ms(identities.initial.not_after_unix_ms)
            })
            .context("build initial vended S3 credential")?,
            rotated: VendedS3Credential::new(
                identities.rotated.access_key_id,
                SecretValue::new(identities.rotated.secret_access_key),
                SecretValue::new(identities.rotated.session_token),
            )
            .and_then(|credential| {
                credential.with_not_after_unix_ms(identities.rotated.not_after_unix_ms)
            })
            .context("build rotated vended S3 credential")?,
            // This read/write scenario proves normal use of the initial
            // leased credential. Refresh uses a dedicated short-TTL scenario.
            initial_ttl: Duration::from_secs(60),
            refresh_ttl: Duration::from_secs(60),
            refresh_behavior: Default::default(),
            table_commit_response_behavior: Default::default(),
            hold_first_table_commit_response: false,
        })
        .context("start bounded REST vended-credential proxy")?;
        let mut fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("vended REST fixture lock poisoned"))?;
        if fixture.is_some() {
            bail!("vended REST fixture was initialized more than once");
        }
        *fixture = Some(VendedRestSystemFixture { rest, proxy });

        let mut config = connector_control_plan::launch_config();
        configure_vended_metadata_access(&mut config, metadata_identity);
        // Vended lease envelopes are confidential lifecycle payloads. The
        // scenario must therefore exercise the native TLS branch, never the
        // default authenticated h2c fixture.
        config.native_trust_fixture = NativeTrustFixture::pem_ip();
        Ok(config)
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: std::time::Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        let (specs, fe_restart, be_restarts) = connector_control_plan::specs(self.name())?;
        self.controls
            .freeze(
                specs,
                fe_restart,
                &be_restarts,
                cluster_size,
                binary,
                original_deadline,
            )
            .map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (proxy_uri, warehouse) = {
            let fixture = self
                .fixture
                .lock()
                .map_err(|_| anyhow::anyhow!("vended REST fixture lock poisoned"))?;
            let fixture = fixture.as_ref().ok_or_else(|| {
                anyhow::anyhow!("vended REST fixture is missing after cluster launch")
            })?;
            (
                fixture.proxy.uri().to_string(),
                fixture.rest.endpoints().rest_warehouse.clone(),
            )
        };
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect_until(&user, port, context.deadline())?;
        let control_phase = (|| -> Result<()> {
            const CATALOG: &str = "vended_rest_catalog";
            const DATABASE: &str = "vended_rest_db";
            const TABLE: &str = "vended_rest_data";
            const CTAS: &str = "vended_rest_ctas";
            context.action("create REST catalog with an explicit vended data binding");
            control
            .query_drop(format!(
                "CREATE EXTERNAL CATALOG {CATALOG} PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"rest\",\"uri\"=\"{proxy_uri}\",\"iceberg.catalog.warehouse\"=\"{warehouse}\",\"aws.s3.endpoint\"=\"{}\",\"aws.s3.region\"=\"us-east-1\",\"aws.s3.enable_path_style_access\"=\"true\",\"credential.object-store-metadata.consumer-role\"=\"frontend\",\"credential.object-store-metadata.mode\"=\"static\",\"credential.object-store-metadata.name\"=\"{VENDED_METADATA_CREDENTIAL_NAME}\",\"credential.object-store-metadata.generation\"=\"{VENDED_METADATA_CREDENTIAL_GENERATION}\",\"credential.object-store-data.consumer-role\"=\"backend\",\"credential.object-store-data.mode\"=\"vended\",\"credential.data-credential-vending.consumer-role\"=\"backend\",\"credential.data-credential-vending.mode\"=\"static\",\"credential.data-credential-vending.name\"=\"{VENDED_EXECUTION_CREDENTIAL_NAME}\",\"credential.data-credential-vending.generation\"=\"{VENDED_EXECUTION_CREDENTIAL_GENERATION}\")",
                self.vended_minio_endpoint()?
            ))
            .context("create vended REST catalog")?;
            context.action("write three vended REST data files through the native 1FE+3BE path");
            // Three committed files give the three-backend read enough independent
            // work to prove that every Backend uses the vended data-plane lease.
            for range in ["1, 1000", "1001, 2000", "2001, 3000"] {
                control
                .query_drop(format!(
                    "INSERT INTO {CATALOG}.{DATABASE}.{TABLE} SELECT generate_series FROM TABLE(generate_series({range}))"
                ))
                .with_context(|| format!("insert vended REST data range {range}"))?;
            }
            let rows: Vec<(i64, i64)> = connector_control_plan::query(
                context,
                &self.controls.take(0)?,
                &mut control,
                &format!("SELECT count(*), sum(v) FROM {CATALOG}.{DATABASE}.{TABLE}"),
            )
            .context("read vended REST data")?;
            if rows != [(3_000, 4_501_500)] {
                bail!("vended REST read returned {rows:?}, expected [(3000, 4501500)]");
            }
            wait_for_open_reader_on_every_backend(
                context,
                CATALOG,
                "observe a vended REST reader on every Backend",
            )?;
            context.action("create a vended REST CTAS target through staged publication");
            control
            .query_drop(format!(
                "CREATE TABLE {CATALOG}.{DATABASE}.{CTAS} AS SELECT v FROM {CATALOG}.{DATABASE}.{TABLE} WHERE v <= 1000"
            ))
            .with_context(|| {
                self.vended_proxy_audit()
                    .map(|audit| format!("create vended REST CTAS target; proxy audit: {audit:?}"))
                    .unwrap_or_else(|error| {
                        format!("create vended REST CTAS target; read proxy audit: {error}")
                    })
            })?;
            let ctas_rows: Vec<(i64, i64)> = control
                .query(format!(
                    "SELECT count(*), sum(v) FROM {CATALOG}.{DATABASE}.{CTAS}"
                ))
                .context("read vended REST CTAS target")?;
            if ctas_rows != [(1_000, 500_500)] {
                bail!("vended REST CTAS returned {ctas_rows:?}, expected [(1000, 500500)]");
            }
            let audit = self.vended_proxy_audit()?;
            if audit.table_loads == 0 || audit.staged_creates < 1 {
                bail!(
                    "vended REST fixture did not observe expected table-load/staged-create calls: {audit:?}"
                );
            }
            // A normal read/write acquires rather than being handed material: each
            // consumer asks the catalog for itself now (CAD-1 D1), so "the quiet
            // path never calls the credentials endpoint" is no longer the
            // invariant. What still holds is that nothing asks more than the
            // consumers that ran: one authority per (catalog, prefix) per process,
            // so at most the coordinator plus each backend.
            let ceiling =
                u64::try_from(context.handle().be_count().saturating_add(1)).unwrap_or(u64::MAX);
            if audit.refreshes == 0 || audit.refreshes > ceiling {
                bail!(
                    "a normal vended read/write must acquire once per consumer and no more; ceiling={ceiling}, audit={audit:?}"
                );
            }
            if audit.refresh_failures != 0 {
                bail!("a normal vended read/write must not be refused: {audit:?}");
            }
            context.action(
                "verify every consumer acquired its own vended credential in normal read/write",
            );
            await_resource_convergence(context, &baseline, "vended REST read/write")?;
            Ok(())
        })();
        connector_control_plan::with_cleanup(control_phase, control.finish())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("vended REST fixture lock poisoned"))?
            .take();
        let Some(VendedRestSystemFixture { mut rest, proxy }) = fixture else {
            return Ok(());
        };
        drop(proxy);
        rest.shutdown()
            .context("shutdown isolated vended REST fixture")
    }
}

impl VendedRestReadWritePem {
    fn vended_minio_endpoint(&self) -> Result<String> {
        self.fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("vended REST fixture lock poisoned"))?
            .as_ref()
            .map(|fixture| fixture.rest.endpoints().minio_endpoint.clone())
            .ok_or_else(|| anyhow::anyhow!("vended REST fixture is missing"))
    }

    fn vended_proxy_audit(
        &self,
    ) -> Result<novarocks_cluster_harness::vended_rest_catalog::VendedRestCatalogAudit> {
        self.fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("vended REST fixture lock poisoned"))?
            .as_ref()
            .map(|fixture| fixture.proxy.audit())
            .ok_or_else(|| anyhow::anyhow!("vended REST fixture is missing"))
    }
}

/// Exercises the write outcome boundary after a catalog-side effect exists but
/// before its response reaches NovaRocks. The fixture reports response loss
/// after the one durable commit; the statement must resolve the committed
/// outcome without replaying it.
#[derive(Default)]
struct VendedRestWriteOutcomePem {
    fixture: Mutex<Option<VendedRestSystemFixture>>,
}

impl Scenario for VendedRestWriteOutcomePem {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "connector",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "connector/vended-credential-write-outcome"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        CrossProcessChildEnvironment::default()
    }

    fn launch_config_with_fixture_run(
        &self,
        scenario_root: &std::path::Path,
        budget: &novarocks_cluster_harness::fixture_run::RunBudget,
    ) -> Result<ScenarioLaunchConfig> {
        let mut rest = IsolatedIcebergRestFixture::start_with_shared_minio(scenario_root, budget)
            .context("start isolated REST fixture for vended write outcome")?;
        rest.provision_empty_table("vended_write_outcome_db", "vended_write_outcome_data")
            .context("provision isolated vended write-outcome table")?;
        let endpoints = rest.endpoints().clone();
        let metadata_identity = rest.static_s3_identity();
        let identities = rest
            .provision_vended_s3_identities()
            .context("provision isolated vended write-outcome identities")?;
        let proxy = VendedRestCatalogFixture::start(VendedRestCatalogConfig {
            downstream: endpoints.rest_uri.clone(),
            scope_prefix: format!("{}/", endpoints.rest_warehouse.trim_end_matches('/')),
            initial: VendedS3Credential::new(
                identities.initial.access_key_id,
                SecretValue::new(identities.initial.secret_access_key),
                SecretValue::new(identities.initial.session_token),
            )
            .context("build short-lived initial vended write-outcome credential")?,
            rotated: VendedS3Credential::new(
                identities.rotated.access_key_id,
                SecretValue::new(identities.rotated.secret_access_key),
                SecretValue::new(identities.rotated.session_token),
            )
            .context("build rotated vended write-outcome credential")?,
            initial_ttl: Duration::from_secs(60),
            refresh_ttl: Duration::from_secs(60),
            refresh_behavior: Default::default(),
            table_commit_response_behavior:
                VendedTableCommitResponseBehavior::FailUnavailableAfterSideEffect,
            hold_first_table_commit_response: false,
        })
        .context("start response-loss vended REST catalog proxy")?;
        let mut fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("vended write-outcome fixture lock poisoned"))?;
        if fixture.is_some() {
            bail!("vended write-outcome fixture was initialized more than once");
        }
        *fixture = Some(VendedRestSystemFixture { rest, proxy });

        let mut config = connector_control_plan::launch_config();
        configure_vended_metadata_access(&mut config, metadata_identity);
        config.native_trust_fixture = NativeTrustFixture::pem_ip();
        Ok(config)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (proxy_uri, warehouse, minio_endpoint) = self.fixture_endpoints()?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect_until(&user, port, context.deadline())?;
        let control_phase = (|| -> Result<()> {
            const CATALOG: &str = "vended_write_outcome_catalog";
            const DATABASE: &str = "vended_write_outcome_db";
            const TABLE: &str = "vended_write_outcome_data";
            control
            .query_drop(format!(
                "CREATE EXTERNAL CATALOG {CATALOG} PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"rest\",\"uri\"=\"{proxy_uri}\",\"iceberg.catalog.warehouse\"=\"{warehouse}\",\"aws.s3.endpoint\"=\"{minio_endpoint}\",\"aws.s3.region\"=\"us-east-1\",\"aws.s3.enable_path_style_access\"=\"true\",\"credential.object-store-metadata.consumer-role\"=\"frontend\",\"credential.object-store-metadata.mode\"=\"static\",\"credential.object-store-metadata.name\"=\"{VENDED_METADATA_CREDENTIAL_NAME}\",\"credential.object-store-metadata.generation\"=\"{VENDED_METADATA_CREDENTIAL_GENERATION}\",\"credential.object-store-data.consumer-role\"=\"backend\",\"credential.object-store-data.mode\"=\"vended\",\"credential.data-credential-vending.consumer-role\"=\"backend\",\"credential.data-credential-vending.mode\"=\"static\",\"credential.data-credential-vending.name\"=\"{VENDED_EXECUTION_CREDENTIAL_NAME}\",\"credential.data-credential-vending.generation\"=\"{VENDED_EXECUTION_CREDENTIAL_GENERATION}\")"
            ))
            .context("create response-loss REST vended catalog")?;

            let (done_tx, done) = mpsc::sync_channel(1);
            let query_user = user.clone();
            let insert = format!("INSERT INTO {CATALOG}.{DATABASE}.{TABLE} SELECT 1 AS v");
            let deadline = context.deadline();
            let writer = thread::spawn(move || {
                let mut connection = mysql_actor::connect_until(&query_user, port, deadline)
                    .context("connect vended write-outcome writer")?;
                let result = connection.query_drop(insert);
                let published = done_tx
                    .send(result.map_err(anyhow::Error::from))
                    .context("publish vended write-outcome result");
                connector_control_plan::with_cleanup(published, connection.finish())
            });

            context.action("inject catalog response loss after one durable vended table commit");
            let received = (|| {
                done.recv_timeout(
                    context.remaining("await failed write after held response release")?,
                )
                .context("vended write-outcome writer did not finish")
            })();
            let joined = writer
                .join()
                .map_err(|_| anyhow::anyhow!("vended write-outcome writer panicked"))
                .and_then(|result| result);
            let outcome = connector_control_plan::with_cleanup(received, joined)?;
            ensure!(
                outcome.is_ok(),
                "response-lost vended write must reconcile the already-committed outcome: {outcome:?}"
            );
            let audit = self.vended_proxy_audit()?;
            ensure!(
                audit.table_commits == 1,
                "vended write must not replay the catalog commit after response loss; audit={audit:?}"
            );
            ensure!(
                audit.refresh_failures == 0,
                "response-loss write must not manufacture a refused refresh; audit={audit:?}"
            );
            let rows: Vec<i64> = control
                .query(format!("SELECT count(*) FROM {CATALOG}.{DATABASE}.{TABLE}"))
                .context("read the already-applied vended write outcome")?;
            ensure!(
                rows == vec![1],
                "response-loss write side effect must be visible exactly once, got {rows:?}"
            );
            await_resource_convergence(context, &baseline, "vended write-outcome failure")?;
            Ok(())
        })();
        connector_control_plan::with_cleanup(control_phase, control.finish())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("vended write-outcome fixture lock poisoned"))?
            .take();
        let Some(VendedRestSystemFixture { mut rest, proxy }) = fixture else {
            return Ok(());
        };
        drop(proxy);
        rest.shutdown()
            .context("shutdown isolated vended write-outcome REST fixture")
    }
}

impl VendedRestWriteOutcomePem {
    fn fixture_endpoints(&self) -> Result<(String, String, String)> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("vended write-outcome fixture lock poisoned"))?;
        let fixture = fixture
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("vended write-outcome fixture is missing"))?;
        Ok((
            fixture.proxy.uri().to_owned(),
            fixture.rest.endpoints().rest_warehouse.clone(),
            fixture.rest.endpoints().minio_endpoint.clone(),
        ))
    }

    fn vended_proxy_audit(
        &self,
    ) -> Result<novarocks_cluster_harness::vended_rest_catalog::VendedRestCatalogAudit> {
        self.fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("vended write-outcome fixture lock poisoned"))?
            .as_ref()
            .map(|fixture| fixture.proxy.audit())
            .ok_or_else(|| anyhow::anyhow!("vended write-outcome fixture is missing"))
    }
}

/// The statement and the replaced answer of one post-publish scenario.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PostPublishAnswerCase {
    InsertNotFound,
    InsertBadRequest,
    CreateDatabaseNotFound,
}

impl PostPublishAnswerCase {
    #[cfg(test)]
    const ALL: [Self; 3] = [
        Self::InsertNotFound,
        Self::InsertBadRequest,
        Self::CreateDatabaseNotFound,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::InsertNotFound => "connector/vended-credential-write-outcome-post-publish-404",
            Self::InsertBadRequest => "connector/vended-credential-write-outcome-post-publish-400",
            Self::CreateDatabaseNotFound => {
                "connector/vended-credential-create-database-post-publish-404"
            }
        }
    }

    const fn replacement(self) -> VendedResponseReplacement {
        match self {
            Self::InsertNotFound => VendedResponseReplacement {
                request: VendedMutationRequest::TableCommit,
                answer: VendedReplacementAnswer::NotFound,
            },
            Self::InsertBadRequest => VendedResponseReplacement {
                request: VendedMutationRequest::TableCommit,
                answer: VendedReplacementAnswer::BadRequest,
            },
            Self::CreateDatabaseNotFound => VendedResponseReplacement {
                request: VendedMutationRequest::NamespaceCreate,
                answer: VendedReplacementAnswer::NotFound,
            },
        }
    }

    const fn status(self) -> u16 {
        match self {
            Self::InsertNotFound | Self::CreateDatabaseNotFound => 404,
            Self::InsertBadRequest => 400,
        }
    }
}

const POST_PUBLISH_CATALOG: &str = "vended_post_publish_catalog";
const POST_PUBLISH_DATABASE: &str = "vended_post_publish_db";
const POST_PUBLISH_TABLE: &str = "vended_post_publish_data";
const POST_PUBLISH_CREATED_DATABASE: &str = "vended_post_publish_created_db";

/// Exercises a 404 or 400 that arrives after the catalog already applied the
/// mutation. A reference REST server commits first and then re-reads the
/// object to build its answer, so a concurrent rename or drop turns an applied
/// mutation into 404, and a failed re-read validation into 400. The fixture
/// lets the real downstream catalog apply the request and only then replaces
/// its answer. Neither status proves the mutation absent: an INSERT must
/// verify its commit as applied and succeed without replaying it or deleting
/// its data, and CREATE DATABASE must report an unknown outcome while the
/// namespace exists downstream.
struct VendedRestPostPublishAnswerPem {
    case: PostPublishAnswerCase,
    fixture: Mutex<Option<VendedRestSystemFixture>>,
}

impl VendedRestPostPublishAnswerPem {
    fn new(case: PostPublishAnswerCase) -> Self {
        Self {
            case,
            fixture: Mutex::new(None),
        }
    }
}

impl Scenario for VendedRestPostPublishAnswerPem {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "connector",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        self.case.name()
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        connector_reader_environment()
    }

    fn launch_config_with_fixture_run(
        &self,
        scenario_root: &std::path::Path,
        budget: &novarocks_cluster_harness::fixture_run::RunBudget,
    ) -> Result<ScenarioLaunchConfig> {
        let mut rest = IsolatedIcebergRestFixture::start_with_shared_minio(scenario_root, budget)
            .context("start isolated REST fixture for a post-publish answer")?;
        rest.provision_empty_table(POST_PUBLISH_DATABASE, POST_PUBLISH_TABLE)
            .context("provision isolated post-publish answer table")?;
        let endpoints = rest.endpoints().clone();
        let metadata_identity = rest.static_s3_identity();
        let identities = rest
            .provision_vended_s3_identities()
            .context("provision isolated post-publish answer identities")?;
        let proxy = VendedRestCatalogFixture::start(VendedRestCatalogConfig {
            downstream: endpoints.rest_uri.clone(),
            scope_prefix: format!("{}/", endpoints.rest_warehouse.trim_end_matches('/')),
            initial: VendedS3Credential::new(
                identities.initial.access_key_id,
                SecretValue::new(identities.initial.secret_access_key),
                SecretValue::new(identities.initial.session_token),
            )
            .context("build initial post-publish answer credential")?,
            rotated: VendedS3Credential::new(
                identities.rotated.access_key_id,
                SecretValue::new(identities.rotated.secret_access_key),
                SecretValue::new(identities.rotated.session_token),
            )
            .context("build rotated post-publish answer credential")?,
            initial_ttl: Duration::from_secs(60),
            refresh_ttl: Duration::from_secs(60),
            refresh_behavior: Default::default(),
            table_commit_response_behavior:
                VendedTableCommitResponseBehavior::ReplaceAfterSideEffect(self.case.replacement()),
            hold_first_table_commit_response: false,
        })
        .context("start post-publish answer vended REST catalog proxy")?;
        let mut fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("post-publish answer fixture lock poisoned"))?;
        if fixture.is_some() {
            bail!("post-publish answer fixture was initialized more than once");
        }
        *fixture = Some(VendedRestSystemFixture { rest, proxy });

        let mut config = connector_launch_config();
        configure_vended_metadata_access(&mut config, metadata_identity);
        config.native_trust_fixture = NativeTrustFixture::pem_ip();
        Ok(config)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let endpoints = self.fixture_endpoints()?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect(
            &user,
            port,
            context.remaining("connect post-publish answer control session")?,
        )?;
        context.action("create a vended REST catalog behind the post-publish answer proxy");
        control
            .query_drop(format!(
                "CREATE EXTERNAL CATALOG {POST_PUBLISH_CATALOG} PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"rest\",\"uri\"=\"{}\",\"iceberg.catalog.warehouse\"=\"{}\",\"aws.s3.endpoint\"=\"{}\",\"aws.s3.region\"=\"us-east-1\",\"aws.s3.enable_path_style_access\"=\"true\",\"credential.object-store-metadata.consumer-role\"=\"frontend\",\"credential.object-store-metadata.mode\"=\"static\",\"credential.object-store-metadata.name\"=\"{VENDED_METADATA_CREDENTIAL_NAME}\",\"credential.object-store-metadata.generation\"=\"{VENDED_METADATA_CREDENTIAL_GENERATION}\",\"credential.object-store-data.consumer-role\"=\"backend\",\"credential.object-store-data.mode\"=\"vended\",\"credential.data-credential-vending.consumer-role\"=\"backend\",\"credential.data-credential-vending.mode\"=\"static\",\"credential.data-credential-vending.name\"=\"{VENDED_EXECUTION_CREDENTIAL_NAME}\",\"credential.data-credential-vending.generation\"=\"{VENDED_EXECUTION_CREDENTIAL_GENERATION}\")",
                endpoints.proxy_uri, endpoints.warehouse, endpoints.minio_endpoint
            ))
            .context("create post-publish answer REST vended catalog")?;
        match self.case {
            PostPublishAnswerCase::InsertNotFound | PostPublishAnswerCase::InsertBadRequest => {
                self.run_insert(context, &mut control, &user, port, &endpoints.rest_uri)?
            }
            PostPublishAnswerCase::CreateDatabaseNotFound => {
                self.run_create_database(context, &user, port, &endpoints.rest_uri)?
            }
        }
        await_resource_convergence(context, &baseline, self.case.name())?;
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("post-publish answer fixture lock poisoned"))?
            .take();
        let Some(VendedRestSystemFixture { mut rest, proxy }) = fixture else {
            return Ok(());
        };
        drop(proxy);
        rest.shutdown()
            .context("shutdown isolated post-publish answer REST fixture")
    }
}

/// Non-secret endpoints of one post-publish answer fixture.
struct PostPublishEndpoints {
    proxy_uri: String,
    warehouse: String,
    minio_endpoint: String,
    /// The fixture's own REST Catalog, never the proxy: the independent oracle
    /// for what the catalog actually holds.
    rest_uri: String,
}

impl VendedRestPostPublishAnswerPem {
    fn run_insert(
        &self,
        context: &mut ScenarioContext,
        control: &mut mysql::Conn,
        user: &str,
        port: u16,
        rest_uri: &str,
    ) -> Result<()> {
        let status = self.case.status();
        let qualified =
            format!("{POST_PUBLISH_CATALOG}.{POST_PUBLISH_DATABASE}.{POST_PUBLISH_TABLE}");
        context.action(format!(
            "insert one row; the proxy answers the applied table commit with HTTP {status}"
        ));
        let mut writer = mysql_actor::connect_with_io_timeout(
            user,
            port,
            Duration::from_secs(10),
            context.remaining("bound the post-publish answer INSERT")?,
        )
        .context("connect post-publish answer writer")?;
        let outcome = writer.query_drop(format!("INSERT INTO {qualified} SELECT 1 AS v"));
        let audit = self.vended_proxy_audit()?;
        ensure!(
            outcome.is_ok(),
            "an applied INSERT whose commit was answered with HTTP {status} must be verified as \
             committed and succeed: {outcome:?}; audit={audit:?}"
        );
        ensure!(
            audit.replacement_class_requests == 1
                && audit.replaced_responses == 1
                && audit.table_commits == 1,
            "the INSERT must dispatch its table commit exactly once and never replay it; \
             audit={audit:?}"
        );
        ensure!(
            audit.refresh_failures == 0,
            "a post-publish answer must not manufacture a refused refresh; audit={audit:?}"
        );

        context
            .action("verify the applied snapshot downstream and read its data through NovaRocks");
        let (catalog_status, table) = downstream_catalog_get(
            rest_uri,
            &format!("/v1/namespaces/{POST_PUBLISH_DATABASE}/tables/{POST_PUBLISH_TABLE}"),
        )?;
        ensure!(
            catalog_status == reqwest::StatusCode::OK,
            "downstream catalog could not load the INSERT target: HTTP {catalog_status}"
        );
        let metadata = table
            .as_ref()
            .and_then(|table| table.get("metadata"))
            .context("downstream load-table answer has no metadata")?;
        let snapshots = metadata
            .get("snapshots")
            .and_then(serde_json::Value::as_array)
            .context("downstream table metadata has no snapshots")?;
        let [snapshot] = snapshots.as_slice() else {
            bail!(
                "downstream must hold exactly the one applied INSERT snapshot, found {}",
                snapshots.len()
            );
        };
        let snapshot_id = snapshot
            .get("snapshot-id")
            .and_then(serde_json::Value::as_i64)
            .context("downstream snapshot has no snapshot-id")?;
        ensure!(
            metadata
                .get("current-snapshot-id")
                .and_then(serde_json::Value::as_i64)
                == Some(snapshot_id),
            "downstream current snapshot is not the applied INSERT snapshot {snapshot_id}"
        );
        ensure!(
            snapshot
                .pointer("/summary/operation")
                .and_then(serde_json::Value::as_str)
                == Some("append"),
            "downstream snapshot {snapshot_id} is not the INSERT append"
        );
        // Reading the row opens the data file the applied snapshot references,
        // so a successful read also proves no cleanup deleted that file.
        let rows: Vec<(i64, i64)> = control
            .query(format!("SELECT count(*), sum(v) FROM {qualified}"))
            .context("read the applied post-publish answer INSERT")?;
        ensure!(
            rows == [(1, 1)],
            "the applied INSERT must be readable exactly once under its snapshot, got {rows:?}"
        );
        Ok(())
    }

    fn run_create_database(
        &self,
        context: &mut ScenarioContext,
        user: &str,
        port: u16,
        rest_uri: &str,
    ) -> Result<()> {
        let namespace_path = format!("/v1/namespaces/{POST_PUBLISH_CREATED_DATABASE}");
        let (before, _) = downstream_catalog_get(rest_uri, &namespace_path)?;
        ensure!(
            before == reqwest::StatusCode::NOT_FOUND,
            "post-publish answer namespace must not exist before CREATE DATABASE, got HTTP {before}"
        );
        context.action(format!(
            "create a database; the proxy answers the applied namespace creation with HTTP {}",
            self.case.status()
        ));
        let mut writer = mysql_actor::connect_with_io_timeout(
            user,
            port,
            Duration::from_secs(10),
            context.remaining("bound the post-publish answer CREATE DATABASE")?,
        )
        .context("connect post-publish answer DDL session")?;
        let outcome = writer.query_drop(format!(
            "CREATE DATABASE {POST_PUBLISH_CATALOG}.{POST_PUBLISH_CREATED_DATABASE}"
        ));
        let audit = self.vended_proxy_audit()?;
        let error = match outcome {
            Ok(()) => bail!(
                "CREATE DATABASE answered with HTTP {} after it was applied must report an \
                 unknown outcome, but it succeeded; audit={audit:?}",
                self.case.status()
            ),
            Err(error) => error.to_string(),
        };
        let lowered = error.to_ascii_lowercase();
        ensure!(
            lowered.contains("unknown") || lowered.contains("cannot be attributed"),
            "CREATE DATABASE answered with HTTP {} after it was applied must report an unknown \
             outcome, got: {error}",
            self.case.status()
        );
        ensure!(
            audit.replacement_class_requests == 1 && audit.replaced_responses == 1,
            "CREATE DATABASE must send exactly one namespace creation and never replay it; \
             audit={audit:?}"
        );
        let (after, namespace) = downstream_catalog_get(rest_uri, &namespace_path)?;
        ensure!(
            after == reqwest::StatusCode::OK,
            "the applied namespace must exist downstream, got HTTP {after}"
        );
        ensure!(
            namespace.as_ref().and_then(|value| value.get("namespace"))
                == Some(&serde_json::json!([POST_PUBLISH_CREATED_DATABASE])),
            "downstream namespace answer does not name {POST_PUBLISH_CREATED_DATABASE}"
        );
        Ok(())
    }

    fn fixture_endpoints(&self) -> Result<PostPublishEndpoints> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("post-publish answer fixture lock poisoned"))?;
        let fixture = fixture
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("post-publish answer fixture is missing"))?;
        let endpoints = fixture.rest.endpoints();
        Ok(PostPublishEndpoints {
            proxy_uri: fixture.proxy.uri().to_owned(),
            warehouse: endpoints.rest_warehouse.clone(),
            minio_endpoint: endpoints.minio_endpoint.clone(),
            rest_uri: endpoints.rest_uri.clone(),
        })
    }

    fn vended_proxy_audit(
        &self,
    ) -> Result<novarocks_cluster_harness::vended_rest_catalog::VendedRestCatalogAudit> {
        self.fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("post-publish answer fixture lock poisoned"))?
            .as_ref()
            .map(|fixture| fixture.proxy.audit())
            .ok_or_else(|| anyhow::anyhow!("post-publish answer fixture is missing"))
    }
}

/// Reads one resource directly from the fixture's own REST Catalog. The body
/// is decoded only when it is JSON; catalog answers carry no credentials.
fn downstream_catalog_get(
    rest_uri: &str,
    path: &str,
) -> Result<(reqwest::StatusCode, Option<serde_json::Value>)> {
    let response = reqwest::blocking::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(30))
        .build()
        .context("build downstream catalog oracle client")?
        .get(format!("{}{path}", rest_uri.trim_end_matches('/')))
        .send()
        .with_context(|| format!("read downstream catalog resource {path}"))?;
    let status = response.status();
    let bytes = response
        .bytes()
        .with_context(|| format!("read downstream catalog answer for {path}"))?;
    Ok((status, serde_json::from_slice(&bytes).ok()))
}

/// Exercises consumer-driven renewal on one identified BE, then verifies
/// three-BE reader liveness and cancellation on the production TLS path. The
/// source lease has a synthetic short expiry while its real STS material
/// remains valid, so the test can prove renewal without invalid S3 secrets.
#[derive(Default)]
struct VendedRestRefreshPem {
    fixture: Mutex<Option<VendedRestSystemFixture>>,
    controls: Mutex<Option<storage_control_plan::Inputs>>,
}

impl Scenario for VendedRestRefreshPem {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "connector",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "connector/vended-credential-refresh"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        CrossProcessChildEnvironment::default()
    }

    fn launch_config_with_fixture_run(
        &self,
        scenario_root: &std::path::Path,
        budget: &novarocks_cluster_harness::fixture_run::RunBudget,
    ) -> Result<ScenarioLaunchConfig> {
        let mut rest =
            IsolatedIcebergRestFixture::start_with_shared_minio(scenario_root, budget)
                .context("start isolated REST and MinIO fixture for vended credential refresh")?;
        rest.provision_empty_table("vended_refresh_db", "vended_refresh_data")
            .context("provision isolated vended refresh source table")?;
        rest.provision_empty_table("vended_refresh_db", "vended_refresh_target")
            .context("provision one-file target consumer table")?;
        let endpoints = rest.endpoints().clone();
        let metadata_identity = rest.static_s3_identity();
        let identities = rest
            .provision_vended_s3_identities()
            .context("provision isolated vended refresh S3 identities")?;
        let proxy = VendedRestCatalogFixture::start(VendedRestCatalogConfig {
            downstream: endpoints.rest_uri.clone(),
            scope_prefix: format!("{}/", endpoints.rest_warehouse.trim_end_matches('/')),
            // Deliberately omit the STS-issued expiration from the fixture
            // response. The proxy instead advertises this bounded synthetic
            // TTL, while MinIO continues to validate the real STS material.
            initial: VendedS3Credential::new(
                identities.initial.access_key_id,
                SecretValue::new(identities.initial.secret_access_key),
                SecretValue::new(identities.initial.session_token),
            )
            .context("build short-lived initial vended S3 credential")?,
            rotated: VendedS3Credential::new(
                identities.rotated.access_key_id,
                SecretValue::new(identities.rotated.secret_access_key),
                SecretValue::new(identities.rotated.session_token),
            )
            .context("build rotated vended S3 credential")?,
            // Both short on purpose. Renewal is demand-driven now: a consumer
            // asks only when a request needs material, so the scenario makes
            // material age fast rather than waiting out a coordinator's timer
            // (CAD-1 D1, D3).
            initial_ttl: Duration::from_secs(6),
            refresh_ttl: Duration::from_secs(6),
            refresh_behavior: Default::default(),
            table_commit_response_behavior: Default::default(),
            hold_first_table_commit_response: false,
        })
        .context("start short-TTL REST vended-credential proxy")?;
        let mut fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("vended refresh REST fixture lock poisoned"))?;
        if fixture.is_some() {
            bail!("vended refresh REST fixture was initialized more than once");
        }
        *fixture = Some(VendedRestSystemFixture { rest, proxy });

        let mut config = connector_launch_config();
        // Preserve normal credentials/config while removing inherited marker activation.
        config.child_environment = CrossProcessChildEnvironment::default();
        configure_vended_metadata_access(&mut config, metadata_identity);
        config.native_trust_fixture = NativeTrustFixture::pem_ip();
        Ok(config)
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _scenario_root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: std::time::Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        let (scope, endpoint) = {
            let fixture = self
                .fixture
                .lock()
                .map_err(|_| anyhow::anyhow!("vended refresh fixture lock poisoned"))?;
            let fixture = fixture
                .as_ref()
                .context("vended refresh fixture not prepared")?;
            (
                format!(
                    "{}/",
                    fixture
                        .rest
                        .endpoints()
                        .rest_warehouse
                        .trim_end_matches('/')
                ),
                format!(
                    "{}/_fixture/vended-credentials/refresh",
                    fixture.proxy.uri()
                ),
            )
        };
        let authority = storage_control_plan::AuthorityIntent {
            catalog: "vended_refresh_catalog".into(),
            scope,
            endpoint,
            close: false,
        };
        let queries = vec![storage_control_plan::QueryIntent {
            run_token: storage_control_plan::REFRESH_HELD_TOKEN,
            sql: storage_control_plan::held_sql(
                "vended_refresh_catalog",
                "vended_refresh_db",
                "vended_refresh_data",
            ),
            task_protocol_markers: true,
        }];
        let (plan, inputs) = storage_control_plan::plan(
            cluster_size,
            binary,
            original_deadline,
            authority,
            &queries,
        )?;
        let mut controls = self
            .controls
            .lock()
            .map_err(|_| anyhow::anyhow!("Storage consumer input lock poisoned"))?;
        ensure!(controls.is_none(), "Storage consumer input prepared twice");
        *controls = Some(inputs);
        Ok(Some(plan))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let storage_inputs = self.control_inputs()?;
        let baseline = resource_baseline(context)?;
        let (proxy_uri, warehouse, minio_endpoint) = {
            let fixture = self
                .fixture
                .lock()
                .map_err(|_| anyhow::anyhow!("vended refresh REST fixture lock poisoned"))?;
            let fixture = fixture.as_ref().ok_or_else(|| {
                anyhow::anyhow!("vended refresh REST fixture is missing after cluster launch")
            })?;
            (
                fixture.proxy.uri().to_string(),
                fixture.rest.endpoints().rest_warehouse.clone(),
                fixture.rest.endpoints().minio_endpoint.clone(),
            )
        };
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect(
            &user,
            port,
            context.remaining("connect vended credential refresh control session")?,
        )?;

        const CATALOG: &str = "vended_refresh_catalog";
        const DATABASE: &str = "vended_refresh_db";
        const TABLE: &str = "vended_refresh_data";
        context.action("create short-TTL REST catalog with a vended data binding");
        control
            .query_drop(format!(
                "CREATE EXTERNAL CATALOG {CATALOG} PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"rest\",\"uri\"=\"{proxy_uri}\",\"iceberg.catalog.warehouse\"=\"{warehouse}\",\"aws.s3.endpoint\"=\"{minio_endpoint}\",\"aws.s3.region\"=\"us-east-1\",\"aws.s3.enable_path_style_access\"=\"true\",\"credential.object-store-metadata.consumer-role\"=\"frontend\",\"credential.object-store-metadata.mode\"=\"static\",\"credential.object-store-metadata.name\"=\"{VENDED_METADATA_CREDENTIAL_NAME}\",\"credential.object-store-metadata.generation\"=\"{VENDED_METADATA_CREDENTIAL_GENERATION}\",\"credential.object-store-data.consumer-role\"=\"backend\",\"credential.object-store-data.mode\"=\"vended\",\"credential.data-credential-vending.consumer-role\"=\"backend\",\"credential.data-credential-vending.mode\"=\"static\",\"credential.data-credential-vending.name\"=\"{VENDED_EXECUTION_CREDENTIAL_NAME}\",\"credential.data-credential-vending.generation\"=\"{VENDED_EXECUTION_CREDENTIAL_GENERATION}\")"
            ))
            .context("create short-TTL vended REST catalog")?;
        context.action("write three independent vended data files for the 1FE+3BE long read");
        for range in ["1, 100000", "100001, 200000", "200001, 300000"] {
            control
                .query_drop(format!(
                    "INSERT INTO {CATALOG}.{DATABASE}.{TABLE} SELECT generate_series FROM TABLE(generate_series({range}))"
                ))
                .with_context(|| format!("insert vended refresh data range {range}"))?;
        }
        // Each setup write is itself a legitimate short-TTL attempt and can
        // independently refresh. Drain them before taking the audit baseline
        // so the assertion below is attributable to the one long read.
        await_resource_convergence(context, &baseline, "short-TTL vended setup writes")?;

        // A single file gives the split-assignment driver one destination: the
        // first admitted scan task (BE[0]). Observe its real authority before
        // arming the fixture, then make only that consumer request new material.
        const TARGET_TABLE: &str = "vended_refresh_target";
        control
            .query_drop(format!(
                "INSERT INTO {CATALOG}.{DATABASE}.{TARGET_TABLE} SELECT generate_series FROM TABLE(generate_series(1, 1000))"
            ))
            .context("write one targeted vended data file")?;
        let target_sql =
            format!("SELECT count(*) FROM {CATALOG}.{DATABASE}.{TARGET_TABLE} WHERE v % 100 = 0");
        let primed: Vec<i64> = control
            .query(&target_sql)
            .context("prime one identified BE authority through a real data read")?;
        ensure!(
            primed == [10],
            "unexpected target priming result: {primed:?}"
        );
        let scope = format!("{}/", warehouse.trim_end_matches('/'));
        let identity = storage_control_plan::identity(context, &storage_inputs)?;
        context.action(format!(
            "BE[0] authority {} at catalog generation {} primed by one-file read",
            identity.key, identity.version
        ));
        await_resource_convergence(context, &baseline, "vended target priming read")?;

        context.action("let the target consumer's material expire between two binary reads");
        thread::sleep(Duration::from_secs(7).min(context.remaining("age vended material")?));
        let renewal_baseline = self.vended_proxy_audit()?;
        {
            let slot = self
                .fixture
                .lock()
                .map_err(|_| anyhow::anyhow!("vended refresh REST fixture lock poisoned"))?;
            let fixture = slot.as_ref().context("vended refresh fixture missing")?;
            fixture.proxy.arm_targeted_refresh_hold(
                VendedRefreshTarget::new(
                    0,
                    identity.key.clone(),
                    identity.version.clone(),
                    scope,
                    format!("{proxy_uri}/_fixture/vended-credentials/refresh"),
                    SecretValue::new(vended_execution_bearer(0)),
                )?,
                VendedRefreshBehavior::IssueRotatedCredential,
            )?;
        }
        let renewal =
            storage_control_plan::SimpleRead::start_ordinary(context, &user, port, &target_sql)?;
        let phase =
            (|| -> Result<()> {
                let observed = {
                    let slot = self.fixture.lock().map_err(|_| {
                        anyhow::anyhow!("vended refresh REST fixture lock poisoned")
                    })?;
                    let fixture = slot.as_ref().context("vended refresh fixture missing")?;
                    fixture.proxy.wait_for_targeted_refresh(
                        context.remaining("observe identified BE consumer acquisition")?,
                    )?
                };
                ensure!(
                    observed.be_index == 0
                        && observed.authority_id == identity.key
                        && observed.catalog_generation == identity.version
                        && observed.request_sequence.is_some()
                        && observed.mismatched_requests == 0
                        && observed.ambiguous_requests == 0,
                    "vended renewal was not attributable to the selected consumer: {observed:?}"
                );
                {
                    let slot = self.fixture.lock().map_err(|_| {
                        anyhow::anyhow!("vended refresh REST fixture lock poisoned")
                    })?;
                    slot.as_ref()
                        .context("vended refresh fixture missing")?
                        .proxy
                        .release_targeted_refresh()?;
                }
                let renewed = renewal
                    .result
                    .recv_timeout(context.remaining("await target BE renewed data read")?)
                    .context("target BE renewed read did not finish")?
                    .context("target BE renewed read failed")?;
                ensure!(renewed == [10], "renewed target read returned {renewed:?}");
                Ok(())
            })();
        let released = {
            let slot = self
                .fixture
                .lock()
                .map_err(|_| anyhow::anyhow!("vended refresh REST fixture lock poisoned"));
            slot.and_then(|slot| {
                slot.as_ref()
                    .context("vended refresh fixture missing")?
                    .proxy
                    .release_targeted_refresh()
            })
        };
        renewal.finish_after(
            context,
            &mut control,
            storage_control_plan::with_cleanup(phase, released),
        )?;
        let targeted = {
            let slot = self
                .fixture
                .lock()
                .map_err(|_| anyhow::anyhow!("vended refresh REST fixture lock poisoned"))?;
            slot.as_ref()
                .context("vended refresh fixture missing")?
                .proxy
                .finish_targeted_refresh_hold()?
        };
        let renewal_audit = self.vended_proxy_audit()?;
        ensure!(
            targeted.response_issued
                && renewal_audit.refreshes > renewal_baseline.refreshes
                && renewal_audit.issued_key_ids.len() == 2,
            "identified consumer did not receive and use rotated material: target={targeted:?}, before={renewal_baseline:?}, after={renewal_audit:?}"
        );
        context.record_phase_observation(
            "identified_consumer_renewal",
            1,
            1,
            1,
            "credentials-endpoint",
            targeted.request_sequence.expect("observed above"),
            "read-succeeded",
            BTreeMap::from([
                ("target_be_index", 0),
                (
                    "target_request_sequence",
                    targeted.request_sequence.expect("observed above"),
                ),
                (
                    "issued_credential_generations",
                    renewal_audit.issued_key_ids.len() as u64,
                ),
            ]),
        )?;
        await_resource_convergence(context, &baseline, "consumer credential renewal read")?;

        // The three-file read retains the original cross-process liveness and
        // reader-cleanup witness. It makes no fixed REST request-count claim:
        // each BE may reuse or refresh its own process-lived authority.
        context.action("start one long-running vended read on all three Backends");
        let read_baseline_logs = backend_log_snapshots(context)?;
        let held_sql = storage_control_plan::held_sql(CATALOG, DATABASE, TABLE);
        let observed_read = storage_control_plan::HeldRead::start(
            context,
            &storage_inputs,
            &user,
            port,
            storage_control_plan::REFRESH_HELD_TOKEN,
            &held_sql,
        )?;
        let target = &observed_read.read;
        let phase = (|| -> Result<()> {
            let connection_id = target
                .ready
                .recv_timeout(context.remaining("receive vended read connection id")?)
                .context("vended read ended before publishing its connection id")?;
            wait_for_new_reader_on_every_backend(
                context,
                CATALOG,
                "observe a vended unit reader on every Backend",
                &read_baseline_logs,
                AwaitedRead::new(target, "the three-BE vended read"),
            )?;
            let lease_baseline_logs = backend_log_snapshots(context)?;
            wait_for_lease_renewal_on_every_backend(
                context,
                "verify every Backend continues the same vended read",
                &lease_baseline_logs,
                AwaitedRead::new(target, "the three-BE vended read"),
            )?;
            if let Ok(result) = target.done.try_recv() {
                bail!("three-BE vended read terminated before cancellation: {result:?}");
            }
            context.action(format!(
                "cancel the long vended read through KILL QUERY {connection_id}"
            ));
            control
                .query_drop(format!("KILL QUERY {connection_id}"))
                .context("cancel three-BE vended reader")?;
            assert_cancelled_query(
                &target.done,
                context.remaining("await three-BE vended read cancellation")?,
            )?;
            assert_target_connection_remains_usable(
                target,
                context.remaining("verify post-cancellation connection behavior")?,
            )?;
            assert_idle_query(&mut control, connection_id)?;
            release_connector_read(target)?;
            Ok(())
        })();
        observed_read.finish_after(context, &mut control, phase)?;
        let reader_logs = wait_for_balanced_reader_lifecycle(
            context,
            "wait for three-BE vended reader close after cancellation",
        )?;
        assert_no_reader_open_after_abort(&reader_logs)?;
        await_resource_convergence(context, &baseline, "short-TTL vended credential refresh")?;
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("vended refresh REST fixture lock poisoned"))?
            .take();
        let Some(VendedRestSystemFixture { mut rest, proxy }) = fixture else {
            return Ok(());
        };
        drop(proxy);
        rest.shutdown()
            .context("shutdown isolated vended credential refresh fixture")
    }
}

impl VendedRestRefreshPem {
    fn control_inputs(&self) -> Result<storage_control_plan::Inputs> {
        self.controls
            .lock()
            .map_err(|_| anyhow::anyhow!("Storage consumer input lock poisoned"))?
            .clone()
            .context("Storage consumer plan was not prepared before launch")
    }

    fn vended_proxy_audit(
        &self,
    ) -> Result<novarocks_cluster_harness::vended_rest_catalog::VendedRestCatalogAudit> {
        self.fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("vended refresh REST fixture lock poisoned"))?
            .as_ref()
            .map(|fixture| fixture.proxy.audit())
            .ok_or_else(|| anyhow::anyhow!("vended refresh REST fixture is missing"))
    }
}

#[derive(Clone, Copy)]
enum VendedCredentialFault {
    TargetedDeadline,
    LateClose,
    Unreachable,
}

struct VendedCredentialFaultPem {
    kind: VendedCredentialFault,
    fixture: Mutex<Option<VendedRestSystemFixture>>,
    controls: Mutex<Option<storage_control_plan::Inputs>>,
    unreachable: Mutex<Option<storage_control_plan::UnreachableEndpoint>>,
}

impl VendedCredentialFaultPem {
    fn new(kind: VendedCredentialFault) -> Self {
        Self {
            kind,
            fixture: Mutex::new(None),
            controls: Mutex::new(None),
            unreachable: Mutex::new(None),
        }
    }

    fn control_inputs(&self) -> Result<storage_control_plan::Inputs> {
        self.controls
            .lock()
            .map_err(|_| anyhow::anyhow!("Storage consumer input lock poisoned"))?
            .clone()
            .context("Storage consumer plan was not prepared before launch")
    }

    fn with_fixture<T>(
        &self,
        action: impl FnOnce(&VendedRestSystemFixture) -> Result<T>,
    ) -> Result<T> {
        let slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("vended credential fault fixture lock poisoned"))?;
        action(slot.as_ref().ok_or_else(|| {
            anyhow::anyhow!("vended credential fault fixture is missing after cluster launch")
        })?)
    }
}

impl Scenario for VendedCredentialFaultPem {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "connector",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        match self.kind {
            VendedCredentialFault::TargetedDeadline => {
                "connector/vended-credential-targeted-deadline"
            }
            VendedCredentialFault::LateClose => "connector/vended-credential-late-close",
            VendedCredentialFault::Unreachable => "connector/vended-credential-unreachable",
        }
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        CrossProcessChildEnvironment::default()
    }

    fn launch_config_with_fixture_run(
        &self,
        scenario_root: &Path,
        budget: &novarocks_cluster_harness::fixture_run::RunBudget,
    ) -> Result<ScenarioLaunchConfig> {
        const DATABASE: &str = "cad2_vended_db";
        const TABLE: &str = "cad2_vended_data";
        let mut rest = IsolatedIcebergRestFixture::start_with_shared_minio(scenario_root, budget)
            .context("start isolated REST and MinIO fixture for credential fault")?;
        rest.provision_empty_table(DATABASE, TABLE)
            .context("provision isolated credential fault table")?;
        let endpoints = rest.endpoints().clone();
        let metadata_identity = rest.static_s3_identity();
        let identities = rest
            .provision_vended_s3_identities()
            .context("provision credential fault S3 identities")?;
        let proxy = VendedRestCatalogFixture::start(VendedRestCatalogConfig {
            downstream: endpoints.rest_uri,
            scope_prefix: format!("{}/", endpoints.rest_warehouse.trim_end_matches('/')),
            initial: VendedS3Credential::new(
                identities.initial.access_key_id,
                SecretValue::new(identities.initial.secret_access_key),
                SecretValue::new(identities.initial.session_token),
            )?,
            rotated: VendedS3Credential::new(
                identities.rotated.access_key_id,
                SecretValue::new(identities.rotated.secret_access_key),
                SecretValue::new(identities.rotated.session_token),
            )?,
            initial_ttl: Duration::from_secs(6),
            refresh_ttl: Duration::from_secs(6),
            refresh_behavior: Default::default(),
            table_commit_response_behavior: Default::default(),
            hold_first_table_commit_response: false,
        })?;
        let mut slot = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("vended credential fault fixture lock poisoned"))?;
        ensure!(
            slot.is_none(),
            "vended credential fault fixture initialized twice"
        );
        *slot = Some(VendedRestSystemFixture { rest, proxy });

        if matches!(self.kind, VendedCredentialFault::Unreachable) {
            let mut endpoint = self
                .unreachable
                .lock()
                .map_err(|_| anyhow::anyhow!("unreachable endpoint input lock poisoned"))?;
            ensure!(endpoint.is_none(), "unreachable endpoint prepared twice");
            *endpoint = Some(storage_control_plan::UnreachableEndpoint::prepare()?);
        }

        let mut config = connector_launch_config();
        // Preserve normal credentials/config while removing inherited marker activation.
        config.child_environment = CrossProcessChildEnvironment::default();
        configure_vended_metadata_access(&mut config, metadata_identity);
        config.native_trust_fixture = NativeTrustFixture::pem_ip();
        Ok(config)
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _scenario_root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: std::time::Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        let (scope, mut endpoint) = self.with_fixture(|fixture| {
            Ok((
                format!(
                    "{}/",
                    fixture
                        .rest
                        .endpoints()
                        .rest_warehouse
                        .trim_end_matches('/')
                ),
                format!(
                    "{}/_fixture/vended-credentials/refresh",
                    fixture.proxy.uri()
                ),
            ))
        })?;
        if matches!(self.kind, VendedCredentialFault::Unreachable) {
            endpoint = self
                .unreachable
                .lock()
                .map_err(|_| anyhow::anyhow!("unreachable endpoint input lock poisoned"))?
                .as_ref()
                .context("unreachable endpoint was not prepared before launch")?
                .endpoint
                .clone();
        }
        let authority = storage_control_plan::AuthorityIntent {
            catalog: "cad2_vended_catalog".into(),
            scope,
            endpoint,
            close: matches!(self.kind, VendedCredentialFault::LateClose),
        };
        let sql = "SELECT count(*) FROM cad2_vended_catalog.cad2_vended_db.cad2_vended_data WHERE v % 100 = 0".to_string();
        let mut queries = Vec::new();
        if !matches!(self.kind, VendedCredentialFault::Unreachable) {
            queries.push(storage_control_plan::QueryIntent {
                run_token: storage_control_plan::FAULT_PRIME_TOKEN,
                sql: sql.clone(),
                task_protocol_markers: false,
            });
        }
        queries.push(storage_control_plan::QueryIntent {
            run_token: storage_control_plan::FAULT_READ_TOKEN,
            sql: sql.clone(),
            task_protocol_markers: false,
        });
        if matches!(self.kind, VendedCredentialFault::LateClose) {
            queries.push(storage_control_plan::QueryIntent {
                run_token: storage_control_plan::FAULT_FOLLOWUP_TOKEN,
                sql,
                task_protocol_markers: false,
            });
        }
        let (plan, inputs) = storage_control_plan::plan(
            cluster_size,
            binary,
            original_deadline,
            authority,
            &queries,
        )?;
        let mut controls = self
            .controls
            .lock()
            .map_err(|_| anyhow::anyhow!("Storage consumer input lock poisoned"))?;
        ensure!(controls.is_none(), "Storage consumer input prepared twice");
        *controls = Some(inputs);
        Ok(Some(plan))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let storage_inputs = self.control_inputs()?;
        let baseline = resource_baseline(context)?;
        let (proxy_uri, warehouse, minio_endpoint, scope) = self.with_fixture(|fixture| {
            Ok((
                fixture.proxy.uri().to_string(),
                fixture.rest.endpoints().rest_warehouse.clone(),
                fixture.rest.endpoints().minio_endpoint.clone(),
                format!(
                    "{}/",
                    fixture
                        .rest
                        .endpoints()
                        .rest_warehouse
                        .trim_end_matches('/')
                ),
            ))
        })?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect(
            &user,
            port,
            context.remaining("connect credential fault control session")?,
        )?;
        const CATALOG: &str = "cad2_vended_catalog";
        const DATABASE: &str = "cad2_vended_db";
        const TABLE: &str = "cad2_vended_data";
        let catalog_sql = |catalog: &str| {
            format!(
                "CREATE EXTERNAL CATALOG {catalog} PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"rest\",\"uri\"=\"{proxy_uri}\",\"iceberg.catalog.warehouse\"=\"{warehouse}\",\"aws.s3.endpoint\"=\"{minio_endpoint}\",\"aws.s3.region\"=\"us-east-1\",\"aws.s3.enable_path_style_access\"=\"true\",\"credential.object-store-metadata.consumer-role\"=\"frontend\",\"credential.object-store-metadata.mode\"=\"static\",\"credential.object-store-metadata.name\"=\"{VENDED_METADATA_CREDENTIAL_NAME}\",\"credential.object-store-metadata.generation\"=\"{VENDED_METADATA_CREDENTIAL_GENERATION}\",\"credential.object-store-data.consumer-role\"=\"backend\",\"credential.object-store-data.mode\"=\"vended\",\"credential.data-credential-vending.consumer-role\"=\"backend\",\"credential.data-credential-vending.mode\"=\"static\",\"credential.data-credential-vending.name\"=\"{VENDED_EXECUTION_CREDENTIAL_NAME}\",\"credential.data-credential-vending.generation\"=\"{VENDED_EXECUTION_CREDENTIAL_GENERATION}\")"
            )
        };
        let setup_catalog = if matches!(self.kind, VendedCredentialFault::Unreachable) {
            "cad2_setup_catalog"
        } else {
            CATALOG
        };
        control
            .query_drop(catalog_sql(setup_catalog))
            .context("create CAD-2 vended catalog")?;
        control
            .query_drop(format!(
                "INSERT INTO {setup_catalog}.{DATABASE}.{TABLE} SELECT generate_series FROM TABLE(generate_series(1, 1000))"
            ))
            .context("write one CAD-2 vended data file")?;
        await_resource_convergence(context, &baseline, "credential fault setup write")?;

        let read_sql =
            format!("SELECT count(*) FROM {CATALOG}.{DATABASE}.{TABLE} WHERE v % 100 = 0");
        if matches!(self.kind, VendedCredentialFault::Unreachable) {
            let endpoint = self
                .unreachable
                .lock()
                .map_err(|_| anyhow::anyhow!("unreachable endpoint input lock poisoned"))?
                .take()
                .context("unreachable endpoint reservation was already consumed")?
                .release_and_verify(context)?;
            ensure!(
                endpoint == storage_inputs.intent.endpoint,
                "runtime endpoint differs from frozen Storage intent"
            );
            self.with_fixture(|fixture| {
                fixture
                    .proxy
                    .advertise_unreachable_refresh_endpoint(endpoint.clone())
            })?;
            control
                .query_drop(catalog_sql(CATALOG))
                .context("create target catalog with unreachable acquisition endpoint")?;
            context.action(format!(
                "target catalog announces an unavailable loopback endpoint {endpoint}"
            ));
            let before = self.with_fixture(|fixture| Ok(fixture.proxy.audit()))?;
            let observed = storage_control_plan::observed_outcome::<i64>(
                context,
                &storage_inputs,
                &mut control,
                storage_control_plan::FAULT_READ_TOKEN,
                &read_sql,
            )?;
            let error = observed
                .result
                .err()
                .context("target BE cannot connect to its advertised endpoint")?;
            storage_control_plan::assert_acquisition_failure(
                context,
                &observed.snapshot,
                3,
                &error,
            )?;
            let identity = storage_control_plan::identity(context, &storage_inputs)?;
            context.action(format!(
                "BE[0] authority {} uses the target catalog generation {}",
                identity.key, identity.version
            ));
            let after = self.with_fixture(|fixture| Ok(fixture.proxy.audit()))?;
            ensure!(
                after.refreshes == before.refreshes,
                "connect-refusal case unexpectedly reached the working fixture endpoint: before={before:?}, after={after:?}"
            );
            context.record_phase_observation(
                "target_catalog_unreachable",
                1,
                1,
                1,
                "credentials-endpoint",
                0,
                "connect-refused",
                BTreeMap::from([
                    ("target_be_index", 0),
                    (
                        "working_endpoint_refreshes",
                        after.refreshes - before.refreshes,
                    ),
                ]),
            )?;
            await_resource_convergence(context, &baseline, "unreachable target BE catalog")?;
            wait_for_backend_logs(
                context,
                "settle unreachable-case connector readers",
                |logs| {
                    logs.iter().all(|log| {
                        let (opens, closes) = reader_counts(log);
                        opens == closes
                    })
                },
            )?;
            return Ok(());
        }
        let first: Vec<i64> = storage_control_plan::query(
            context,
            &storage_inputs,
            &mut control,
            storage_control_plan::FAULT_PRIME_TOKEN,
            &read_sql,
        )
        .context("prime BE authority with one real file read")?;
        ensure!(first == [10], "unexpected priming read result: {first:?}");
        let identity = storage_control_plan::identity(context, &storage_inputs)?;
        context.action(format!(
            "target BE[0] authority {} catalog generation {}",
            identity.key, identity.version
        ));
        await_resource_convergence(context, &baseline, "credential fault priming read")?;

        context.action("let target BE material expire before its next data request");
        thread::sleep(Duration::from_secs(7).min(context.remaining("age target material")?));
        let endpoint = format!("{proxy_uri}/_fixture/vended-credentials/refresh");
        self.with_fixture(|fixture| {
            let target = VendedRefreshTarget::new(
                0,
                identity.key.clone(),
                identity.version.clone(),
                scope.clone(),
                endpoint,
                SecretValue::new(vended_execution_bearer(0)),
            )?;
            if matches!(self.kind, VendedCredentialFault::TargetedDeadline) {
                fixture.proxy.arm_targeted_refresh_retry_hold(target)
            } else {
                fixture.proxy.arm_targeted_refresh_hold(
                    target,
                    VendedRefreshBehavior::IssueRotatedCredential,
                )
            }
        })?;

        let started = std::time::Instant::now();
        let read = storage_control_plan::SimpleRead::start(
            context,
            &storage_inputs,
            &user,
            port,
            storage_control_plan::FAULT_READ_TOKEN,
            &read_sql,
        )?;
        let read_result = &read.result;
        let phase = (|| {
            let observed = self.with_fixture(|fixture| {
                fixture
                    .proxy
                    .wait_for_targeted_refresh(context.remaining("observe BE[0] acquisition")?)
            })?;
            ensure!(
                observed.be_index == 0
                    && observed.authority_id == identity.key
                    && observed.catalog_generation == identity.version
                    && observed.request_sequence.is_some()
                    && observed.mismatched_requests == 0
                    && observed.ambiguous_requests == 0,
                "targeted fixture request was not attributable to BE[0]: {observed:?}"
            );
            context.action(format!(
                "BE[0] targeted refresh request sequence {} entered",
                observed.request_sequence.expect("checked")
            ));

            match self.kind {
                VendedCredentialFault::TargetedDeadline => {
                    let error = read_result
                        .recv_timeout(context.remaining("await bounded BE acquisition failure")?)
                        .context("held acquisition did not return within scenario budget")?
                        .err()
                        .context("held acquisition must fail its own deadline")?;
                    let original_read_elapsed = started.elapsed();
                    ensure!(
                        original_read_elapsed < Duration::from_secs(50),
                        "targeted acquisition exceeded its D5 combined bound"
                    );
                    let terminal = read.terminal_snapshot(context, &error)?;
                    storage_control_plan::assert_acquisition_failure(
                        context, &terminal, 1, &error,
                    )?;
                    self.with_fixture(|fixture| fixture.proxy.release_targeted_refresh())?;
                    context.record_phase_observation(
                        "targeted_provider_deadline",
                        1,
                        1,
                        1,
                        "credentials-endpoint",
                        observed.request_sequence.expect("checked"),
                        "bounded-failure",
                        BTreeMap::from([
                            ("target_be_index", 0),
                            (
                                "target_request_sequence",
                                observed.request_sequence.expect("checked"),
                            ),
                        ]),
                    )?;
                }
                VendedCredentialFault::LateClose => {
                    let closed =
                        storage_control_plan::close(context, &storage_inputs, &identity.key)?;
                    self.with_fixture(|fixture| fixture.proxy.release_targeted_refresh())?;
                    let late = storage_control_plan::late(context, &storage_inputs, &identity.key)?;
                    let applied_before = debug_close_counter(&closed, "refreshes_applied")?;
                    let applied_after = debug_close_counter(&late, "refreshes_applied")?;
                    let discarded_before = debug_close_counter(&closed, "late_results_discarded")?;
                    let discarded_after = debug_close_counter(&late, "late_results_discarded")?;
                    ensure!(
                        applied_after == applied_before && discarded_after > discarded_before,
                        "closed authority did not fence its late provider result: closed={closed:?}, late={late:?}"
                    );
                    let error = read_result
                        .recv_timeout(context.remaining("await closed authority waiter")?)
                        .context("closed authority waiter did not return")?
                        .err()
                        .context("closed authority waiter must not use late success")?;
                    let terminal = read.terminal_snapshot(context, &error)?;
                    storage_control_plan::assert_acquisition_failure(
                        context, &terminal, 2, &error,
                    )?;
                    let subsequent = storage_control_plan::observed_outcome::<i64>(
                        context,
                        &storage_inputs,
                        &mut control,
                        storage_control_plan::FAULT_FOLLOWUP_TOKEN,
                        &read_sql,
                    )?;
                    let subsequent_error = subsequent
                        .result
                        .err()
                        .context("closed authority must reject a later read")?;
                    storage_control_plan::assert_acquisition_failure(
                        context,
                        &subsequent.snapshot,
                        2,
                        &subsequent_error,
                    )?;
                    context.record_phase_observation(
                        "late_success_after_close",
                        1,
                        1,
                        1,
                        "credentials-endpoint",
                        observed.request_sequence.expect("checked"),
                        "discarded",
                        BTreeMap::from([
                            ("target_be_index", 0),
                            (
                                "target_request_sequence",
                                observed.request_sequence.expect("checked"),
                            ),
                            (
                                "refreshes_applied_after_close",
                                applied_after - applied_before,
                            ),
                            (
                                "late_results_discarded_delta",
                                discarded_after - discarded_before,
                            ),
                        ]),
                    )?;
                }
                VendedCredentialFault::Unreachable => unreachable!("handled before targeted hold"),
            }
            Ok(observed)
        })();
        let released = self.with_fixture(|fixture| fixture.proxy.release_targeted_refresh());
        let phase = storage_control_plan::with_cleanup(phase, released);
        let observed = read.finish_after(context, &mut control, phase)?;
        let final_audit = self.with_fixture(|fixture| {
            fixture
                .proxy
                .targeted_refresh_audit()
                .ok_or_else(|| anyhow::anyhow!("target audit missing"))
        })?;
        ensure!(
            final_audit.request_sequence == observed.request_sequence
                && final_audit.matching_requests >= 1
                && final_audit.mismatched_requests == 0
                && final_audit.ambiguous_requests == 0
                && final_audit.released
                && (matches!(self.kind, VendedCredentialFault::TargetedDeadline)
                    || final_audit.response_issued)
                && (!matches!(self.kind, VendedCredentialFault::TargetedDeadline)
                    || !final_audit.response_issued),
            "targeted fixture lost its exact consumer witness: {final_audit:?}"
        );
        context.action(format!(
            "BE[0] acquisition stayed attributable across {} matching HTTP request(s); fixture issued credential={}",
            final_audit.matching_requests, final_audit.response_issued
        ));
        await_resource_convergence(context, &baseline, "credential fault read")?;
        wait_for_backend_logs(
            context,
            "settle credential-fault connector readers",
            |logs| {
                logs.iter().all(|log| {
                    let (opens, closes) = reader_counts(log);
                    opens == closes
                })
            },
        )?;
        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        // Release a prelaunch-only reservation even if startup/run failed.
        self.unreachable
            .lock()
            .map_err(|_| anyhow::anyhow!("unreachable endpoint input lock poisoned"))?
            .take();
        let fixture = self
            .fixture
            .lock()
            .map_err(|_| anyhow::anyhow!("vended credential fault fixture lock poisoned"))?
            .take();
        let Some(VendedRestSystemFixture { mut rest, proxy }) = fixture else {
            return Ok(());
        };
        drop(proxy);
        rest.shutdown()
            .context("shutdown CAD-2 vended credential fault fixture")
    }
}

fn debug_close_counter(record: &BTreeMap<String, String>, field: &str) -> Result<u64> {
    record
        .get(field)
        .ok_or_else(|| anyhow::anyhow!("debug close record omitted {field}"))?
        .parse::<u64>()
        .with_context(|| format!("parse debug close {field}"))
}

/// Proves a Frontend restart reconstructs its durable catalog projection
/// without invalidating catalog runtimes retained by the live Backends.
#[derive(Default)]
struct CatalogFeRestartCache {
    controls: connector_control_plan::Owner,
}

impl Scenario for CatalogFeRestartCache {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "connector",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "connector/catalog-fe-restart-cache"
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        CrossProcessChildEnvironment::default()
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(connector_control_plan::launch_config())
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: std::time::Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        let (specs, fe_restart, be_restarts) = connector_control_plan::specs(self.name())?;
        self.controls
            .freeze(
                specs,
                fe_restart,
                &be_restarts,
                cluster_size,
                binary,
                original_deadline,
            )
            .map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect_until(&user, port, context.deadline())?;
        let control_phase = (|| -> Result<()> {
            const CATALOG: &str = "catalog_fe_restart_cache";
            const DATABASE: &str = "catalog_fe_restart_db";
            const TABLE: &str = "catalog_fe_restart_data";
            let warehouse = create_warehouse(context, "catalog-fe-restart-cache")?;
            connector_control_plan::observed_fixture(
                context,
                &self.controls,
                &mut control,
                CATALOG,
                DATABASE,
                TABLE,
                &warehouse,
            )?;

            context.action("warm the exact catalog runtime on every Backend");
            let warm_rows: Vec<i64> = connector_control_plan::query(
                context,
                &self.controls.take(3)?,
                &mut control,
                &format!("SELECT count(*) FROM {CATALOG}.{DATABASE}.{TABLE}"),
            )
            .context("warm catalog runtime before FE restart")?;
            if warm_rows != [300_000] {
                bail!("warm catalog read returned {warm_rows:?}, expected [300000]");
            }
            let warm_logs = wait_for_open_reader_on_every_backend(
                context,
                CATALOG,
                "observe warm catalog readers on every Backend",
            )?;
            let warm_versions = reader_catalog_versions(&warm_logs, CATALOG)?;
            let materializations_before = catalog_materialization_counts(&warm_logs);

            context.action("start an in-flight query before the Frontend restart");
            let selected = self.controls.take(4)?;
            let owned =
                connector_control_plan::Reader::start(context, &selected, &user, port, None)?;
            let target = &owned.read;
            let mut fe_exited = false;
            let phase = (|| -> Result<()> {
                target
                    .ready
                    .recv_timeout(context.remaining("receive in-flight reader connection id")?)
                    .context("in-flight reader terminated before FE restart")?;
                wait_for_new_reader_on_every_backend(
                    context,
                    CATALOG,
                    "observe in-flight readers before FE restart",
                    &warm_logs,
                    AwaitedRead::new(target, "the in-flight FE restart reader"),
                )?;

                context
                    .action("restart the Frontend and require its in-flight client query to fail");
                let deadline = context.deadline();
                context
                    .handle()
                    .restart_fe_until(deadline)
                    .context("restart FE while catalog reader is in flight")?;
                fe_exited = true;
                assert_connection_killed_query(
                    &target.done,
                    context.remaining("await in-flight query failure after FE restart")?,
                )?;
                assert_target_connection_is_closed(
                    target,
                    context.remaining("verify FE restart closed in-flight client connection")?,
                )?;
                release_connector_read(target)?;
                Ok(())
            })();
            if fe_exited {
                owned.finish_after_fe_exit(context, &mut control, phase)?;
            } else {
                owned.finish_after(context, &mut control, phase)?;
            }

            context.action("read through the restored Frontend catalog projection");
            let mut restored = mysql_actor::connect_until(&user, port, context.deadline())?;
            let restored_phase = (|| -> Result<()> {
                let restored_rows: Vec<i64> = connector_control_plan::query(
                    context,
                    &self.controls.take(5)?,
                    &mut restored,
                    &format!("SELECT count(*) FROM {CATALOG}.{DATABASE}.{TABLE}"),
                )
                .context("read catalog after FE restart")?;
                if restored_rows != [300_000] {
                    bail!("restored catalog read returned {restored_rows:?}, expected [300000]");
                }
                let restored_logs = wait_for_backend_logs(
                    context,
                    "observe post-restart readers on every Backend",
                    |logs| {
                        logs.iter().zip(&warm_logs).all(|(current, previous)| {
                            reader_open_lines(current, CATALOG).count()
                                > reader_open_lines(previous, CATALOG).count()
                        })
                    },
                )?;
                let restored_versions =
                    reader_catalog_versions_after(&restored_logs, &warm_logs, CATALOG)?;
                if restored_versions != warm_versions {
                    bail!(
                        "FE restart changed retained catalog versions: before={warm_versions:?}, after={restored_versions:?}"
                    );
                }
                let materializations_after = catalog_materialization_counts(&restored_logs);
                if materializations_after != materializations_before {
                    bail!(
                        "FE restart rematerialized retained catalog runtimes: before={materializations_before:?}, after={materializations_after:?}"
                    );
                }

                await_resource_convergence(context, &baseline, "FE restart catalog cache")?;
                Ok(())
            })();
            connector_control_plan::with_cleanup(restored_phase, restored.finish())
        })();
        connector_control_plan::with_cleanup(control_phase, control.finish())
    }
}

/// A static-file catalog source with two simultaneously active, role-local
/// object-store credential generations. The two fixtures deliberately accept
/// different key IDs, so a successful read plus their request logs proves that
/// each catalog stayed on its exact declared credential generation.
#[derive(Default)]
struct StaticCredentialGeneration {
    fixtures: Mutex<Option<StaticCredentialFixtures>>,
    controls: connector_control_plan::Owner,
}

/// Exercises the cache key boundary on the same native topology as static
/// credentials, but is independently selectable by the M1 acceptance gate.
#[derive(Default)]
struct AccessDomainCacheIsolation {
    fixtures: Mutex<Option<StaticCredentialFixtures>>,
    controls: connector_control_plan::Owner,
}

struct StaticCredentialFixtures {
    blue: LoopbackS3Fixture,
    green: LoopbackS3Fixture,
}

const STATIC_BLUE_CATALOG: &str = "cca_static_blue";
const STATIC_GREEN_CATALOG: &str = "cca_static_green";
const STATIC_BLUE_CREDENTIAL_NAME: &str = "cca-static-blue";
const STATIC_GREEN_CREDENTIAL_NAME: &str = "cca-static-green";
const STATIC_BLUE_CREDENTIAL_GENERATION: &str = "v1";
const STATIC_GREEN_CREDENTIAL_GENERATION: &str = "v2";
const STATIC_BLUE_KEY_ID: &str = "cca-static-blue-key";
const STATIC_GREEN_KEY_ID: &str = "cca-static-green-key";
const STATIC_BLUE_KEY_SECRET: &str = "cca-static-blue-secret";
const STATIC_GREEN_KEY_SECRET: &str = "cca-static-green-secret";
const COLLISION_BLUE_CATALOG: &str = "cca_cache_domain_blue";
const COLLISION_GREEN_CATALOG: &str = "cca_cache_domain_green";
const COLLISION_DATABASE: &str = "cache_domain_db";
const COLLISION_TABLE: &str = "cache_domain_data";
const COLLISION_WAREHOUSE: &str = "s3://cca-cache-domain-collision/warehouse";

impl Scenario for StaticCredentialGeneration {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "connector",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "connector/static-credential-generation"
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        CrossProcessChildEnvironment::default()
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        static_credential_launch_config(&self.fixtures, scenario_root)
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: std::time::Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        let (specs, fe_restart, be_restarts) = connector_control_plan::specs(self.name())?;
        self.controls
            .freeze(
                specs,
                fe_restart,
                &be_restarts,
                cluster_size,
                binary,
                original_deadline,
            )
            .map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect_until(&user, port, context.deadline())?;
        let control_phase = (|| -> Result<()> {
            run_static_credential_generation(
                context,
                &mut control,
                &self.controls,
                &self.fixtures,
            )?;
            await_resource_convergence(context, &baseline, "static credential generation reads")?;
            Ok(())
        })();
        connector_control_plan::with_cleanup(control_phase, control.finish())
    }
}

impl Scenario for AccessDomainCacheIsolation {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "connector",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "connector/access-domain-cache-isolation"
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        CrossProcessChildEnvironment::default()
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        access_domain_collision_launch_config(&self.fixtures, scenario_root)
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: std::time::Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        let (specs, fe_restart, be_restarts) = connector_control_plan::specs(self.name())?;
        self.controls
            .freeze(
                specs,
                fe_restart,
                &be_restarts,
                cluster_size,
                binary,
                original_deadline,
            )
            .map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect_until(&user, port, context.deadline())?;
        let control_phase = (|| -> Result<()> {
            // Cache policy is a query-scoped FE-to-BE contract. The process-level
            // cache service configured by this scenario is intentionally inert
            // until the client opts this session into both reads and population.
            control
                .query_drop("SET enable_scan_datacache = true")
                .context("enable cache reads for access-domain isolation")?;
            control
                .query_drop("SET enable_populate_datacache = true")
                .context("enable cache population for access-domain isolation")?;

            run_access_domain_cache_isolation(
                context,
                &mut control,
                &self.controls,
                &self.fixtures,
            )?;
            await_resource_convergence(context, &baseline, "access-domain cache isolation")?;
            Ok(())
        })();
        connector_control_plan::with_cleanup(control_phase, control.finish())
    }
}

fn static_credential_launch_config(
    fixtures: &Mutex<Option<StaticCredentialFixtures>>,
    scenario_root: &Path,
) -> Result<ScenarioLaunchConfig> {
    let blue = LoopbackS3Fixture::start(LoopbackS3Config::for_access_key(STATIC_BLUE_KEY_ID))
        .context("start blue loopback S3 fixture")?;
    let green = LoopbackS3Fixture::start(LoopbackS3Config::for_access_key(STATIC_GREEN_KEY_ID))
        .context("start green loopback S3 fixture")?;
    let snapshot = scenario_root.join("static-credential-catalogs.toml");
    write_static_credential_snapshot(&snapshot, blue.endpoint(), green.endpoint())?;
    let mut fixtures = fixtures
        .lock()
        .map_err(|_| anyhow::anyhow!("static credential fixture lock poisoned"))?;
    if fixtures.is_some() {
        bail!("static credential fixtures were initialized more than once");
    }
    *fixtures = Some(StaticCredentialFixtures { blue, green });

    Ok(ScenarioLaunchConfig {
        child_environment: CrossProcessChildEnvironment::default(),
        config_overlay: static_credential_launch_overlay(&snapshot),
        ..Default::default()
    })
}

fn access_domain_collision_launch_config(
    fixtures: &Mutex<Option<StaticCredentialFixtures>>,
    scenario_root: &Path,
) -> Result<ScenarioLaunchConfig> {
    let blue = LoopbackS3Fixture::start(collision_loopback_s3_config(STATIC_BLUE_KEY_ID))
        .context("start blue collision loopback S3 fixture")?;
    let green = LoopbackS3Fixture::start(collision_loopback_s3_config(STATIC_GREEN_KEY_ID))
        .context("start green collision loopback S3 fixture")?;
    let snapshot = scenario_root.join("access-domain-collision-catalogs.toml");
    write_access_domain_collision_snapshot(&snapshot, blue.endpoint(), green.endpoint())?;
    let mut fixtures = fixtures
        .lock()
        .map_err(|_| anyhow::anyhow!("access-domain collision fixture lock poisoned"))?;
    if fixtures.is_some() {
        bail!("access-domain collision fixtures were initialized more than once");
    }
    *fixtures = Some(StaticCredentialFixtures { blue, green });

    Ok(ScenarioLaunchConfig {
        child_environment: CrossProcessChildEnvironment::default(),
        config_overlay: static_credential_launch_overlay(&snapshot),
        ..Default::default()
    })
}

fn collision_loopback_s3_config(access_key_id: &str) -> LoopbackS3Config {
    let mut config = LoopbackS3Config::for_access_key(access_key_id);
    // The source corpus and its same-length canonical copies are both kept
    // only for this bounded collision setup. The default 128-object ceiling
    // cannot retain both graphs after three Iceberg commits.
    config.max_objects = 256;
    config
}

fn run_static_credential_generation(
    context: &mut ScenarioContext,
    control: &mut mysql::Conn,
    controls: &connector_control_plan::Owner,
    fixtures: &Mutex<Option<StaticCredentialFixtures>>,
) -> Result<()> {
    context.action("write three blue and three green S3 Iceberg files through StaticFile catalogs");
    create_static_catalog_table_and_data(
        control,
        STATIC_BLUE_CATALOG,
        "static_blue_db",
        "static_blue_data",
        ["1, 100000", "100001, 200000", "200001, 300000"],
    )?;
    create_static_catalog_table_and_data(
        control,
        STATIC_GREEN_CATALOG,
        "static_green_db",
        "static_green_data",
        ["300001, 400000", "400001, 500000", "500001, 600000"],
    )?;

    context.action("read the blue static credential catalog through every backend");
    let _blue_profile = static_catalog_profile(
        control,
        STATIC_BLUE_CATALOG,
        "static_blue_db",
        "static_blue_data",
    )?;
    let rows: Vec<i64> = connector_control_plan::query(
        context,
        &controls.take(0)?,
        control,
        &connector_control_plan::sum_sql(STATIC_BLUE_CATALOG, "static_blue_db", "static_blue_data"),
    )?;
    ensure!(
        rows == [45_000_150_000],
        "static credential catalog sum returned {rows:?}, expected [45_000_150_000]"
    );
    let blue_logs = wait_for_open_reader_on_every_backend(
        context,
        STATIC_BLUE_CATALOG,
        "observe every BE read the blue static credential generation",
    )?;
    assert_static_reader_opened_on_every_backend(&blue_logs, STATIC_BLUE_CATALOG)?;

    context.action("read the green static credential catalog through every backend");
    let _green_profile = static_catalog_profile(
        control,
        STATIC_GREEN_CATALOG,
        "static_green_db",
        "static_green_data",
    )?;
    let rows: Vec<i64> = connector_control_plan::query(
        context,
        &controls.take(1)?,
        control,
        &connector_control_plan::sum_sql(
            STATIC_GREEN_CATALOG,
            "static_green_db",
            "static_green_data",
        ),
    )?;
    ensure!(
        rows == [135_000_150_000],
        "static credential catalog sum returned {rows:?}, expected [135_000_150_000]"
    );
    let green_logs = wait_for_open_reader_on_every_backend(
        context,
        STATIC_GREEN_CATALOG,
        "observe every BE read the green static credential generation",
    )?;
    assert_static_reader_opened_on_every_backend(&green_logs, STATIC_GREEN_CATALOG)?;

    let fixtures = fixtures
        .lock()
        .map_err(|_| anyhow::anyhow!("static credential fixture lock poisoned"))?;
    let fixtures = fixtures
        .as_ref()
        .context("static credential fixtures were not retained through scenario execution")?;
    assert_fixture_used_only_expected_key(
        "blue",
        &fixtures.blue.request_log(),
        STATIC_BLUE_KEY_ID,
    )?;
    assert_fixture_used_only_expected_key(
        "green",
        &fixtures.green.request_log(),
        STATIC_GREEN_KEY_ID,
    )?;
    context.action("proved blue/v1 and green/v2 reads used distinct exact role-local S3 keys");
    Ok(())
}

fn run_access_domain_cache_isolation(
    context: &mut ScenarioContext,
    control: &mut mysql::Conn,
    controls: &connector_control_plan::Owner,
    fixtures: &Mutex<Option<StaticCredentialFixtures>>,
) -> Result<()> {
    context.action("write colliding blue and green Iceberg corpora at one S3 URI");
    create_static_catalog_table_and_data(
        control,
        COLLISION_BLUE_CATALOG,
        COLLISION_DATABASE,
        COLLISION_TABLE,
        ["100001, 200000", "200001, 300000", "300001, 400000"],
    )?;
    create_static_catalog_table_and_data(
        control,
        COLLISION_GREEN_CATALOG,
        COLLISION_DATABASE,
        COLLISION_TABLE,
        ["100002, 200001", "200002, 300001", "300002, 400001"],
    )?;

    let fixtures = fixtures
        .lock()
        .map_err(|_| anyhow::anyhow!("static credential fixture lock poisoned"))?;
    let fixtures = fixtures
        .as_ref()
        .context("static credential fixtures were not retained through scenario execution")?;
    let canonical_data_paths = canonicalize_collision_corpus(&fixtures.blue, &fixtures.green)?;

    context.action("warm the blue access domain for the colliding S3 files");
    let blue_profile = static_catalog_profile(
        control,
        COLLISION_BLUE_CATALOG,
        COLLISION_DATABASE,
        COLLISION_TABLE,
    )?;
    assert_positive_profile_counter(&blue_profile, "ConnectorFileCacheMisses")?;
    let rows: Vec<i64> = connector_control_plan::query(
        context,
        &controls.take(0)?,
        control,
        &connector_control_plan::sum_sql(
            COLLISION_BLUE_CATALOG,
            COLLISION_DATABASE,
            COLLISION_TABLE,
        ),
    )?;
    ensure!(
        rows == [75_000_150_000],
        "static credential catalog sum returned {rows:?}, expected [75_000_150_000]"
    );
    let blue_logs = wait_for_open_reader_on_every_backend(
        context,
        COLLISION_BLUE_CATALOG,
        "observe every BE read the blue collision access domain",
    )?;
    assert_static_reader_opened_on_every_backend(&blue_logs, COLLISION_BLUE_CATALOG)?;

    context.action("repeat the blue collision read from its role-local cache");
    let blue_cached_profile = static_catalog_profile(
        control,
        COLLISION_BLUE_CATALOG,
        COLLISION_DATABASE,
        COLLISION_TABLE,
    )?;
    assert_positive_profile_counter(&blue_cached_profile, "ConnectorFileCacheHits")?;

    let green_data_reads_before =
        successful_gets_for_paths(&fixtures.green.request_log(), &canonical_data_paths);
    context.action("read the same S3 URI through the green endpoint and access domain");
    let green_profile = static_catalog_profile(
        control,
        COLLISION_GREEN_CATALOG,
        COLLISION_DATABASE,
        COLLISION_TABLE,
    )?;
    assert_positive_profile_counter(&green_profile, "ConnectorFileCacheMisses")?;
    let rows: Vec<i64> = connector_control_plan::query(
        context,
        &controls.take(1)?,
        control,
        &connector_control_plan::sum_sql(
            COLLISION_GREEN_CATALOG,
            COLLISION_DATABASE,
            COLLISION_TABLE,
        ),
    )?;
    ensure!(
        rows == [75_000_450_000],
        "static credential catalog sum returned {rows:?}, expected [75_000_450_000]"
    );
    let green_data_reads_after =
        successful_gets_for_paths(&fixtures.green.request_log(), &canonical_data_paths);
    if green_data_reads_after <= green_data_reads_before {
        bail!(
            "green collision endpoint received no canonical Parquet GET after blue cache warm: before={green_data_reads_before}, after={green_data_reads_after}"
        );
    }
    let green_logs = wait_for_open_reader_on_every_backend(
        context,
        COLLISION_GREEN_CATALOG,
        "observe every BE read the green collision access domain",
    )?;
    assert_static_reader_opened_on_every_backend(&green_logs, COLLISION_GREEN_CATALOG)?;
    assert_fixture_used_only_expected_key(
        "blue collision",
        &fixtures.blue.request_log(),
        STATIC_BLUE_KEY_ID,
    )?;
    assert_fixture_used_only_expected_key(
        "green collision",
        &fixtures.green.request_log(),
        STATIC_GREEN_KEY_ID,
    )?;
    context.action(
        "proved a same-URI, same-size, same-mtime cross-endpoint corpus did not reuse blue cache data",
    );
    Ok(())
}

fn canonicalize_collision_corpus(
    blue: &LoopbackS3Fixture,
    green: &LoopbackS3Fixture,
) -> Result<BTreeSet<String>> {
    let blue_objects = blue.object_snapshot_for_test();
    let green_objects = green.object_snapshot_for_test();
    let mappings = collision_data_path_mapping(&blue_objects, &green_objects)?;
    for blue_object in blue_objects
        .iter()
        .filter(|object| object.key.ends_with(".avro"))
    {
        let bytes = rewrite_fixed_width_paths(&blue_object.bytes, &mappings.replacements)?;
        blue.replace_object_for_test(LoopbackS3Object {
            bucket: blue_object.bucket.clone(),
            key: blue_object.key.clone(),
            bytes,
        })?;
    }
    for blue_object in blue_objects
        .iter()
        .filter(|object| object.key.ends_with(".parquet"))
    {
        let target_key = mappings
            .replacements
            .get(&blue_object.key)
            .context("blue collision data object has no green canonical path")?;
        blue.replace_object_for_test(LoopbackS3Object {
            bucket: blue_object.bucket.clone(),
            key: target_key.clone(),
            bytes: blue_object.bytes.clone(),
        })?;
    }
    Ok(mappings.canonical_paths)
}

struct CollisionDataPathMapping {
    replacements: BTreeMap<String, String>,
    canonical_paths: BTreeSet<String>,
}

fn collision_data_path_mapping(
    blue_objects: &[LoopbackS3Object],
    green_objects: &[LoopbackS3Object],
) -> Result<CollisionDataPathMapping> {
    let blue_data = parquet_objects_by_bucket_and_length(blue_objects);
    let green_data = parquet_objects_by_bucket_and_length(green_objects);
    let blue_count = blue_data.values().map(Vec::len).sum::<usize>();
    let green_count = green_data.values().map(Vec::len).sum::<usize>();
    if blue_data.is_empty() || blue_count != green_count {
        bail!(
            "collision corpus must contain equal non-empty blue and green Parquet data files, got {} and {}",
            blue_count,
            green_count
        );
    }
    if blue_data.keys().collect::<Vec<_>>() != green_data.keys().collect::<Vec<_>>() {
        bail!(
            "collision Parquet `(bucket, byte_length)` multisets differ: blue={:?}, green={:?}",
            parquet_length_multiset(&blue_data),
            parquet_length_multiset(&green_data)
        );
    }

    let mut replacements = BTreeMap::new();
    let mut canonical_paths = BTreeSet::new();
    let mut distinct_payloads = 0;
    for ((bucket, byte_length), mut blue_group) in blue_data {
        let mut green_group = green_data
            .get(&(bucket.clone(), byte_length))
            .cloned()
            .expect("validated collision Parquet multiset has green group");
        // UUID suffixes are fixed-width. Sorting within equal-length groups makes
        // the fixture-only rewriting deterministic without changing any bytes.
        blue_group.sort_by_key(|object| &object.key);
        green_group.sort_by_key(|object| &object.key);
        if blue_group.len() != green_group.len() {
            bail!(
                "collision Parquet `(bucket, byte_length)` multiplicity differs for {bucket}/{byte_length}: blue={}, green={}",
                blue_group.len(),
                green_group.len()
            );
        }
        for (blue_object, green_object) in blue_group.into_iter().zip(green_group) {
            if blue_object.key.len() != green_object.key.len() {
                bail!(
                    "collision data file paths must have equal width: {}/{} vs {}/{}",
                    blue_object.bucket,
                    blue_object.key,
                    green_object.bucket,
                    green_object.key
                );
            }
            if blue_object.bytes != green_object.bytes {
                distinct_payloads += 1;
            }
            replacements.insert(blue_object.key.clone(), green_object.key.clone());
            canonical_paths.insert(format!("/{}/{}", green_object.bucket, green_object.key));
        }
    }
    if distinct_payloads == 0 {
        bail!("collision Parquet corpora have no distinct blue and green payloads");
    }
    Ok(CollisionDataPathMapping {
        replacements,
        canonical_paths,
    })
}

fn parquet_objects_by_bucket_and_length(
    objects: &[LoopbackS3Object],
) -> BTreeMap<(String, usize), Vec<&LoopbackS3Object>> {
    let mut groups = BTreeMap::new();
    for object in objects
        .iter()
        .filter(|object| object.key.ends_with(".parquet"))
    {
        groups
            .entry((object.bucket.clone(), object.bytes.len()))
            .or_insert_with(Vec::new)
            .push(object);
    }
    groups
}

fn parquet_length_multiset(
    groups: &BTreeMap<(String, usize), Vec<&LoopbackS3Object>>,
) -> Vec<((String, usize), usize)> {
    groups
        .iter()
        .map(|(group, objects)| (group.clone(), objects.len()))
        .collect()
}

fn successful_gets_for_paths(requests: &[LoopbackS3Request], paths: &BTreeSet<String>) -> usize {
    requests
        .iter()
        .filter(|request| {
            request.method == "GET" && request.status < 400 && paths.contains(&request.path)
        })
        .count()
}

fn rewrite_fixed_width_paths(
    input: &[u8],
    mappings: &std::collections::BTreeMap<String, String>,
) -> Result<Vec<u8>> {
    let mut output = input.to_vec();
    for (source, target) in mappings {
        if source.len() != target.len() {
            bail!("collision path replacement changes byte width: {source} -> {target}");
        }
        let source = source.as_bytes();
        let target = target.as_bytes();
        let mut offset = 0;
        while let Some(relative) = output[offset..]
            .windows(source.len())
            .position(|window| window == source)
        {
            let start = offset + relative;
            output[start..start + source.len()].copy_from_slice(target);
            offset = start + source.len();
        }
    }
    Ok(output)
}

struct PredicatePageIndexPruning;

impl Scenario for PredicatePageIndexPruning {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "connector",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "connector/predicate-page-index-pruning"
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(connector_control_plan::launch_config())
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect_until(&user, port, context.deadline())?;
        let control_phase = (|| -> Result<()> {
            let warehouse = create_warehouse(context, "predicate-page-index-pruning")?;
            const CATALOG: &str = "page_index_catalog";
            const DATABASE: &str = "page_index_db";
            const TABLE: &str = "page_index_data";
            const PREDICATE: &str = "v >= 199000";

            context.action("create three dense Iceberg files that each require page-level pruning");
            create_catalog_table_and_dense_data(
                &mut control,
                CATALOG,
                DATABASE,
                TABLE,
                &warehouse,
            )?;

            let select =
                format!("SELECT count(*) FROM {CATALOG}.{DATABASE}.{TABLE} WHERE {PREDICATE}");
            context.action("run the static predicate with page-index reader disabled");
            control
                .query_drop("SET enable_parquet_reader_page_index = false")
                .context("disable predicate-driven page-index pruning")?;
            let disabled: Vec<i64> = control
                .query(&select)
                .context("query dense Iceberg files with page-index disabled")?;

            context.action("run the same static predicate with page-index reader enabled");
            control
                .query_drop("SET enable_parquet_reader_page_index = true")
                .context("enable predicate-driven page-index pruning")?;
            let enabled: Vec<i64> = control
                .query(&select)
                .context("query dense Iceberg files with page-index enabled")?;
            if enabled != disabled || enabled != [3_003] {
                bail!(
                    "page-index toggle changed query correctness: disabled={disabled:?}, enabled={enabled:?}, expected=[3003]"
                );
            }

            context.action("assert EXPLAIN ANALYZE surfaces typed connector scan activity");
            let explain: Vec<String> = control
                .query(format!("EXPLAIN ANALYZE {select}"))
                .context("collect typed connector EXPLAIN ANALYZE profile")?;
            let explain = explain.join("\n");
            if !explain.contains("TypedConnectorMetrics:") {
                bail!(
                    "page-index EXPLAIN ANALYZE has no typed connector metrics; profile={explain}"
                );
            }
            assert_positive_profile_counter(&explain, "TypedConnectorPageSourcesOpened")?;
            Ok(())
        })();
        connector_control_plan::with_cleanup(control_phase, control.finish())
    }
}

impl Scenario for CatalogVersionDrain {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "connector",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "connector/catalog-version-drain"
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        CrossProcessChildEnvironment::default()
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        let mut launch = connector_control_plan::launch_config();
        // The old reader's scan-side filter holds its pipeline driver in SLEEP.
        // Reserve a second driver worker so the replacement query can run before
        // cancellation; this scenario verifies concurrent catalog versions.
        let overlay = launch
            .config_overlay
            .be
            .as_mut()
            .expect("connector BE overlay");
        *overlay = overlay.replacen(
            "[runtime]",
            "[runtime]\npipeline_exec_thread_pool_thread_num = 2",
            1,
        );
        Ok(launch)
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        original_deadline: std::time::Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        let (specs, fe_restart, be_restarts) = connector_control_plan::specs(self.name())?;
        self.controls
            .freeze(
                specs,
                fe_restart,
                &be_restarts,
                cluster_size,
                binary,
                original_deadline,
            )
            .map(Some)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect_until(&user, port, context.deadline())?;
        let control_phase = (|| -> Result<()> {
            control
                .query_drop("SET pipeline_dop = 1")
                .context("bound catalog version drain control driver parallelism")?;

            let warehouse = create_warehouse(context, "catalog-version-drain")?;
            context.action("create the first Iceberg catalog version and three data files");
            create_catalog_table_and_data(
                &mut control,
                "connector_generation_catalog",
                "connector_generation_db",
                "connector_generation_data",
                &warehouse,
            )?;

            context.action("start a read pinned to the first catalog version");
            let baseline_logs = backend_log_snapshots(context)?;
            let selected = self.controls.take(0)?;
            let owned = connector_control_plan::Reader::start(
                context,
                &selected,
                &user,
                port,
                Some(NonZeroU32::new(1).expect("nonzero reader DOP")),
            )?;
            let target = &owned.read;
            let phase = (|| -> Result<Vec<String>> {
                let connection_id = target
                    .ready
                    .recv_timeout(context.remaining("receive old-version connection id")?)
                    .context(
                        "old-version connector read terminated before publishing its connection id",
                    )?;
                let old_logs = wait_for_new_reader_on_every_backend(
                    context,
                    "connector_generation_catalog",
                    "wait for every BE to open an old-version connector reader",
                    &baseline_logs,
                    AwaitedRead::new(target, "the old-version connector read"),
                )?;
                let old_versions =
                    reader_catalog_versions(&old_logs, "connector_generation_catalog")?;
                if let Ok(result) = target.done.try_recv() {
                    bail!(
                        "old-version connector read completed before replacement was published: {result:?}"
                    );
                }

                context.action("drop and recreate the catalog while old readers remain in flight");
                control
                    .query_drop("DROP CATALOG connector_generation_catalog")
                    .context("retire first catalog version")?;
                create_catalog(&mut control, "connector_generation_catalog", &warehouse)?;

                context
                    .action("read the replacement catalog version while the old version is leased");
                let replacement_rows: Vec<i64> = connector_control_plan::query(context, &self.controls.take(1)?, &mut control, "SELECT count(*) FROM connector_generation_catalog.connector_generation_db.connector_generation_data")
            .context("read table through replacement catalog version while old version is leased")?;
                if replacement_rows != [300_000] {
                    bail!(
                        "replacement catalog version returned {replacement_rows:?}, expected [300000]"
                    );
                }
                wait_for_replacement_reader_on_every_backend(
                    context,
                    "connector_generation_catalog",
                    &old_versions,
                )?;
                if let Ok(result) = target.done.try_recv() {
                    bail!(
                        "old-version connector read completed while replacement was being verified: {result:?}"
                    );
                }

                context.action(format!(
                    "cancel old-version reader through KILL QUERY {connection_id}"
                ));
                control
                    .query_drop(format!("KILL QUERY {connection_id}"))
                    .context("issue public MySQL KILL QUERY for old-generation reader")?;
                assert_cancelled_query(
                    &target.done,
                    context.remaining("await old-version reader cancellation")?,
                )?;
                assert_target_connection_remains_usable(
                    target,
                    context.remaining("verify old-generation KILL QUERY target remains usable")?,
                )?;
                assert_idle_query(&mut control, connection_id)?;
                release_connector_read(target)?;
                Ok(old_versions)
            })();
            let old_versions = owned.finish_after(context, &mut control, phase)?;

            wait_for_retired_catalog_version_close(
                context,
                "connector_generation_catalog",
                &old_versions,
            )?;

            await_resource_convergence(context, &baseline, "catalog version drain")?;
            Ok(())
        })();
        connector_control_plan::with_cleanup(control_phase, control.finish())
    }
}

struct ConnectorRead {
    ready: mpsc::Receiver<u32>,
    done: mpsc::Receiver<std::result::Result<Vec<i64>, mysql::Error>>,
    probe: mpsc::SyncSender<()>,
    probe_result: mpsc::Receiver<std::result::Result<Option<i64>, mysql::Error>>,
    release: mpsc::Sender<()>,
    thread: thread::JoinHandle<Result<()>>,
}

struct IdleMysqlConnection {
    ready: mpsc::Receiver<u32>,
    probe: mpsc::SyncSender<()>,
    probe_result: mpsc::Receiver<std::result::Result<Option<i64>, mysql::Error>>,
    thread: thread::JoinHandle<Result<()>>,
}

pub(super) fn connector_reader_environment() -> CrossProcessChildEnvironment {
    CrossProcessChildEnvironment::default()
}

pub(super) fn connector_launch_config() -> ScenarioLaunchConfig {
    ScenarioLaunchConfig {
        child_environment: CrossProcessChildEnvironment::default(),
        config_overlay: CrossProcessConfigOverlay {
            fe: Some(READER_CACHE_OVERLAY.to_string()),
            be: Some(format!(
                r#"
[runtime]
operator_buffer_chunks = 1
{READER_CACHE_OVERLAY}
"#
            )),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn configure_vended_metadata_access(
    config: &mut ScenarioLaunchConfig,
    identity: IsolatedS3Identity,
) {
    config.child_environment.fe.insert(
        VENDED_METADATA_ACCESS_KEY_ENV.to_string(),
        identity.access_key_id,
    );
    config.child_environment.fe.insert(
        VENDED_METADATA_SECRET_KEY_ENV.to_string(),
        identity.secret_access_key,
    );
    let existing = config.config_overlay.fe.take().unwrap_or_default();
    config.config_overlay.fe = Some(format!(
        r#"{existing}
[[connector.credentials]]
purpose = "object-store-metadata"
name = "{VENDED_METADATA_CREDENTIAL_NAME}"
generation = "{VENDED_METADATA_CREDENTIAL_GENERATION}"
kind = "s3"
access_key_id = "${{ENV:{VENDED_METADATA_ACCESS_KEY_ENV}}}"
access_key_secret = "${{ENV:{VENDED_METADATA_SECRET_KEY_ENV}}}"
"#
    ));
    // Each BE gets a distinct catalog principal so the fixture can attribute
    // an acquisition to the process that signs with its vended material.
    // The bearer is supplied through that BE's environment and is absent from
    // the retained launch-config evidence.
    for backend_index in 0..3 {
        let environment_name = format!("{VENDED_EXECUTION_BEARER_ENV_PREFIX}{backend_index}");
        config
            .child_environment
            .be_by_index
            .entry(backend_index)
            .or_default()
            .insert(
                environment_name.clone(),
                vended_execution_bearer(backend_index),
            );
        let existing = config
            .config_overlay
            .be_by_index
            .remove(&backend_index)
            .unwrap_or_default();
        config.config_overlay.be_by_index.insert(
            backend_index,
            format!(
                r#"{existing}
[[connector.credentials]]
purpose = "data-credential-vending"
name = "{VENDED_EXECUTION_CREDENTIAL_NAME}"
generation = "{VENDED_EXECUTION_CREDENTIAL_GENERATION}"
kind = "iceberg-rest-bearer"
token = "${{ENV:{environment_name}}}"
"#
            ),
        );
    }
}

fn vended_execution_bearer(backend_index: usize) -> String {
    format!("novarocks-execution-node-be-{backend_index}")
}

fn static_credential_launch_overlay(snapshot: &Path) -> CrossProcessConfigOverlay {
    CrossProcessConfigOverlay {
        fe: Some(format!(
            "[catalog_source]\nmode = \"static-file\"\nstatic_file_path = \"{}\"\n{}\n{READER_CACHE_OVERLAY}",
            snapshot.display(),
            static_credential_registry_overlay("object-store-metadata"),
        )),
        be: Some(format!(
            "{}\n{READER_CACHE_OVERLAY}",
            static_credential_registry_overlay("object-store-data"),
        )),
        ..Default::default()
    }
}

fn static_credential_registry_overlay(purpose: &str) -> String {
    format!(
        r#"
[[connector.credentials]]
purpose = "{purpose}"
name = "{STATIC_BLUE_CREDENTIAL_NAME}"
generation = "{STATIC_BLUE_CREDENTIAL_GENERATION}"
kind = "s3"
access_key_id = "{STATIC_BLUE_KEY_ID}"
access_key_secret = "{STATIC_BLUE_KEY_SECRET}"

[[connector.credentials]]
purpose = "{purpose}"
name = "{STATIC_GREEN_CREDENTIAL_NAME}"
generation = "{STATIC_GREEN_CREDENTIAL_GENERATION}"
kind = "s3"
access_key_id = "{STATIC_GREEN_KEY_ID}"
access_key_secret = "{STATIC_GREEN_KEY_SECRET}"
"#
    )
}

fn write_static_credential_snapshot(
    snapshot: &Path,
    blue_endpoint: &str,
    green_endpoint: &str,
) -> Result<()> {
    std::fs::write(
        snapshot,
        format!(
            "format_version = 3\n\
             [[catalogs]]\n\
             instance_id = \"{STATIC_BLUE_CATALOG}\"\n\
             provider_id = \"iceberg\"\n\
             display_name = \"{STATIC_BLUE_CATALOG}\"\n\
             config_format_version = 3\n\
             [[catalogs.credential_bindings]]\n\
             purpose = \"object-store-metadata\"\n\
             consumer_role = \"frontend\"\n\
             mode = \"static\"\n\
             name = \"{STATIC_BLUE_CREDENTIAL_NAME}\"\n\
             generation = \"{STATIC_BLUE_CREDENTIAL_GENERATION}\"\n\
             [[catalogs.credential_bindings]]\n\
             purpose = \"object-store-data\"\n\
             consumer_role = \"backend\"\n\
             mode = \"static\"\n\
             name = \"{STATIC_BLUE_CREDENTIAL_NAME}\"\n\
             generation = \"{STATIC_BLUE_CREDENTIAL_GENERATION}\"\n\
             [catalogs.properties]\n\
             type = \"iceberg\"\n\
             \"iceberg.catalog.type\" = \"hadoop\"\n\
             \"iceberg.catalog.warehouse\" = \"s3://cca-static-blue/warehouse\"\n\
             \"aws.s3.endpoint\" = \"{blue_endpoint}\"\n\
             \"aws.s3.enable_path_style_access\" = \"true\"\n\
             [[catalogs]]\n\
             instance_id = \"{STATIC_GREEN_CATALOG}\"\n\
             provider_id = \"iceberg\"\n\
             display_name = \"{STATIC_GREEN_CATALOG}\"\n\
             config_format_version = 3\n\
             [[catalogs.credential_bindings]]\n\
             purpose = \"object-store-metadata\"\n\
             consumer_role = \"frontend\"\n\
             mode = \"static\"\n\
             name = \"{STATIC_GREEN_CREDENTIAL_NAME}\"\n\
             generation = \"{STATIC_GREEN_CREDENTIAL_GENERATION}\"\n\
             [[catalogs.credential_bindings]]\n\
             purpose = \"object-store-data\"\n\
             consumer_role = \"backend\"\n\
             mode = \"static\"\n\
             name = \"{STATIC_GREEN_CREDENTIAL_NAME}\"\n\
             generation = \"{STATIC_GREEN_CREDENTIAL_GENERATION}\"\n\
             [catalogs.properties]\n\
             type = \"iceberg\"\n\
             \"iceberg.catalog.type\" = \"hadoop\"\n\
             \"iceberg.catalog.warehouse\" = \"s3://cca-static-green/warehouse\"\n\
             \"aws.s3.endpoint\" = \"{green_endpoint}\"\n\
             \"aws.s3.enable_path_style_access\" = \"true\"\n"
        ),
    )
    .with_context(|| format!("write static credential snapshot {}", snapshot.display()))
}

fn write_access_domain_collision_snapshot(
    snapshot: &Path,
    blue_endpoint: &str,
    green_endpoint: &str,
) -> Result<()> {
    std::fs::write(
        snapshot,
        format!(
            "format_version = 3\n\
             [[catalogs]]\n\
             instance_id = \"{COLLISION_BLUE_CATALOG}\"\n\
             provider_id = \"iceberg\"\n\
             display_name = \"{COLLISION_BLUE_CATALOG}\"\n\
             config_format_version = 3\n\
             [[catalogs.credential_bindings]]\n\
             purpose = \"object-store-metadata\"\n\
             consumer_role = \"frontend\"\n\
             mode = \"static\"\n\
             name = \"{STATIC_BLUE_CREDENTIAL_NAME}\"\n\
             generation = \"{STATIC_BLUE_CREDENTIAL_GENERATION}\"\n\
             [[catalogs.credential_bindings]]\n\
             purpose = \"object-store-data\"\n\
             consumer_role = \"backend\"\n\
             mode = \"static\"\n\
             name = \"{STATIC_BLUE_CREDENTIAL_NAME}\"\n\
             generation = \"{STATIC_BLUE_CREDENTIAL_GENERATION}\"\n\
             [catalogs.properties]\n\
             type = \"iceberg\"\n\
             \"iceberg.catalog.type\" = \"hadoop\"\n\
             \"iceberg.catalog.warehouse\" = \"{COLLISION_WAREHOUSE}\"\n\
             \"aws.s3.endpoint\" = \"{blue_endpoint}\"\n\
             \"aws.s3.enable_path_style_access\" = \"true\"\n\
             [[catalogs]]\n\
             instance_id = \"{COLLISION_GREEN_CATALOG}\"\n\
             provider_id = \"iceberg\"\n\
             display_name = \"{COLLISION_GREEN_CATALOG}\"\n\
             config_format_version = 3\n\
             [[catalogs.credential_bindings]]\n\
             purpose = \"object-store-metadata\"\n\
             consumer_role = \"frontend\"\n\
             mode = \"static\"\n\
             name = \"{STATIC_GREEN_CREDENTIAL_NAME}\"\n\
             generation = \"{STATIC_GREEN_CREDENTIAL_GENERATION}\"\n\
             [[catalogs.credential_bindings]]\n\
             purpose = \"object-store-data\"\n\
             consumer_role = \"backend\"\n\
             mode = \"static\"\n\
             name = \"{STATIC_GREEN_CREDENTIAL_NAME}\"\n\
             generation = \"{STATIC_GREEN_CREDENTIAL_GENERATION}\"\n\
             [catalogs.properties]\n\
             type = \"iceberg\"\n\
             \"iceberg.catalog.type\" = \"hadoop\"\n\
             \"iceberg.catalog.warehouse\" = \"{COLLISION_WAREHOUSE}\"\n\
             \"aws.s3.endpoint\" = \"{green_endpoint}\"\n\
             \"aws.s3.enable_path_style_access\" = \"true\"\n"
        ),
    )
    .with_context(|| {
        format!(
            "write access-domain collision snapshot {}",
            snapshot.display()
        )
    })
}

pub(super) fn require_three_backends(context: &mut ScenarioContext) -> Result<()> {
    let count = context.handle().be_count();
    if count != 3 {
        bail!(
            "{} requires the native acceptance topology 1FE+3BE, received 1FE+{count}BE",
            context.name()
        );
    }
    Ok(())
}

pub(super) fn mysql_endpoint(context: &ScenarioContext) -> (String, u16) {
    (context.mysql_user().to_string(), context.mysql_port())
}

pub(super) fn resource_baseline(
    context: &mut ScenarioContext,
) -> Result<QueryExecutionResourceSnapshot> {
    context
        .handle()
        .query_execution_resource_snapshot()?
        .context("cross-process system scenario requires the query resource oracle")
}

pub(super) fn await_resource_convergence(
    context: &mut ScenarioContext,
    baseline: &QueryExecutionResourceSnapshot,
    operation: &str,
) -> Result<()> {
    let deadline = context.deadline();
    context.action(format!(
        "await query-execution resource convergence after {operation}"
    ));
    context
        .handle()
        .await_query_execution_resource_convergence(baseline, deadline)
        .with_context(|| format!("resource convergence after {operation}"))
}

pub(super) fn create_warehouse(
    context: &ScenarioContext,
    name: &str,
) -> Result<std::path::PathBuf> {
    let warehouse = context.runtime_dir().join("warehouses").join(name);
    std::fs::create_dir_all(&warehouse)
        .with_context(|| format!("create Iceberg warehouse {}", warehouse.display()))?;
    Ok(warehouse)
}

fn create_catalog_table_and_data(
    control: &mut mysql::Conn,
    catalog: &str,
    database: &str,
    table: &str,
    warehouse: &std::path::Path,
) -> Result<()> {
    create_catalog(control, catalog, warehouse)?;
    control
        .query_drop(format!("CREATE DATABASE {catalog}.{database}"))
        .with_context(|| format!("create {catalog}.{database}"))?;
    control
        .query_drop(format!(
            "CREATE TABLE {catalog}.{database}.{table} (v BIGINT)"
        ))
        .with_context(|| format!("create {catalog}.{database}.{table}"))?;
    for range in ["1, 100000", "100001, 200000", "200001, 300000"] {
        control
            .query_drop(format!(
                "INSERT INTO {catalog}.{database}.{table} SELECT generate_series FROM TABLE(generate_series({range}))"
            ))
            .with_context(|| format!("write data range {range} to {catalog}.{database}.{table}"))?;
    }
    Ok(())
}

fn create_static_catalog_table_and_data(
    control: &mut mysql::Conn,
    catalog: &str,
    database: &str,
    table: &str,
    ranges: [&str; 3],
) -> Result<()> {
    control
        .query_drop(format!("CREATE DATABASE {catalog}.{database}"))
        .with_context(|| format!("create {catalog}.{database} from StaticFile source"))?;
    control
        .query_drop(format!(
            "CREATE TABLE {catalog}.{database}.{table} (v BIGINT)"
        ))
        .with_context(|| format!("create {catalog}.{database}.{table} from StaticFile source"))?;
    // One committed data file per range gives the distributed 1FE+3BE query
    // enough independent S3 work to prove every BE read the declared catalog.
    for range in ranges {
        control
            .query_drop(format!(
                "INSERT INTO {catalog}.{database}.{table} SELECT generate_series FROM TABLE(generate_series({range}))"
            ))
            .with_context(|| {
                format!("write static credential range {range} to {catalog}.{database}.{table}")
            })?;
    }
    Ok(())
}

fn static_catalog_profile(
    control: &mut mysql::Conn,
    catalog: &str,
    database: &str,
    table: &str,
) -> Result<String> {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let rows: Vec<String> = control
            .query(format!(
                "EXPLAIN ANALYZE SELECT count(*) FROM {catalog}.{database}.{table}"
            ))
            .with_context(|| {
                format!("profile {catalog}.{database}.{table} through typed connector")
            })?;
        let profile = rows.join("\n");
        if profile.contains("TypedConnectorMetrics:") {
            return Ok(profile);
        }
        if std::time::Instant::now() >= deadline {
            bail!(
                "static credential catalog {catalog}.{database}.{table} EXPLAIN ANALYZE has no typed connector metrics after bounded metadata visibility wait; profile={profile}"
            );
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn assert_static_reader_opened_on_every_backend(logs: &[String], catalog: &str) -> Result<()> {
    for (index, log) in logs.iter().enumerate() {
        if reader_open_lines(log, catalog).next().is_none() {
            bail!("BE[{index}] did not open a typed reader for static catalog {catalog}");
        }
    }
    Ok(())
}

fn assert_fixture_used_only_expected_key(
    fixture: &str,
    requests: &[LoopbackS3Request],
    expected_key_id: &str,
) -> Result<()> {
    let reads = requests
        .iter()
        .filter(|request| request.method == "GET" && request.status < 400)
        .collect::<Vec<_>>();
    if reads.is_empty() {
        bail!("{fixture} loopback S3 fixture recorded no successful GET request");
    }
    for request in reads {
        if request.credential_key_id.as_deref() != Some(expected_key_id) {
            bail!(
                "{fixture} loopback S3 GET {} used credential key {:?}, expected {expected_key_id}",
                request.path,
                request.credential_key_id
            );
        }
    }
    Ok(())
}

fn create_catalog_table_and_dense_data(
    control: &mut mysql::Conn,
    catalog: &str,
    database: &str,
    table: &str,
    warehouse: &std::path::Path,
) -> Result<()> {
    create_catalog(control, catalog, warehouse)?;
    control
        .query_drop(format!("CREATE DATABASE {catalog}.{database}"))
        .with_context(|| format!("create {catalog}.{database}"))?;
    control
        .query_drop(format!(
            "CREATE TABLE {catalog}.{database}.{table} (v BIGINT)"
        ))
        .with_context(|| format!("create {catalog}.{database}.{table}"))?;
    // Each transaction writes one file. The duplicated ordered range prevents
    // Iceberg file-metric pruning from eliminating an entire file, while its
    // size forces multiple Parquet data pages per file for the FS page-index
    // path under test.
    for _ in 0..3 {
        control
            .query_drop(format!(
                "INSERT INTO {catalog}.{database}.{table} SELECT generate_series FROM TABLE(generate_series(1, 200000))"
            ))
            .with_context(|| format!("write dense page-index data to {catalog}.{database}.{table}"))?;
    }
    Ok(())
}

pub(super) fn create_catalog(
    control: &mut mysql::Conn,
    catalog: &str,
    warehouse: &std::path::Path,
) -> Result<()> {
    let warehouse = warehouse.to_string_lossy().replace('"', "\\\"");
    control
        .query_drop(format!(
            "CREATE EXTERNAL CATALOG {catalog} PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"hadoop\",\"iceberg.catalog.warehouse\"=\"{warehouse}\")"
        ))
        .with_context(|| format!("create Hadoop Iceberg catalog {catalog}"))
}

fn assert_positive_profile_counter(profile: &str, name: &str) -> Result<()> {
    let marker = format!("{name}=");
    let value = profile
        .split(&marker)
        .nth(1)
        .and_then(|tail| {
            tail.chars()
                .take_while(char::is_ascii_digit)
                .collect::<String>()
                .parse::<u64>()
                .ok()
        })
        .context(format!(
            "page-index EXPLAIN ANALYZE profile is missing {marker}; profile={profile}"
        ))?;
    if value == 0 {
        bail!("page-index EXPLAIN ANALYZE counter {name} must be positive; profile={profile}");
    }
    Ok(())
}

fn assert_idle_query(control: &mut mysql::Conn, connection_id: u32) -> Result<()> {
    control
        .query_drop(format!("KILL QUERY {connection_id}"))
        .context("idle KILL QUERY must succeed for a live target connection")
}

fn assert_target_connection_remains_usable(
    target: &ConnectorRead,
    timeout: Duration,
) -> Result<()> {
    target
        .probe
        .send(())
        .context("request KILL QUERY target connection probe")?;
    match target
        .probe_result
        .recv_timeout(timeout)
        .context("KILL QUERY target did not answer the connection probe")?
    {
        Ok(Some(1)) => Ok(()),
        Ok(result) => bail!("KILL QUERY target probe returned {result:?}, expected Some(1)"),
        Err(error) => bail!("KILL QUERY unexpectedly closed target connection: {error}"),
    }
}

fn assert_target_connection_is_closed(target: &ConnectorRead, timeout: Duration) -> Result<()> {
    target
        .probe
        .send(())
        .context("request KILL CONNECTION target connection probe")?;
    match target
        .probe_result
        .recv_timeout(timeout)
        .context("KILL CONNECTION target did not answer the connection probe")?
    {
        Err(_) => Ok(()),
        Ok(result) => bail!("KILL CONNECTION left the target connection usable: {result:?}"),
    }
}

fn assert_idle_target_connection_is_closed(
    target: &IdleMysqlConnection,
    timeout: Duration,
) -> Result<()> {
    target
        .probe
        .send(())
        .context("request bare KILL target connection probe")?;
    match target
        .probe_result
        .recv_timeout(timeout)
        .context("bare KILL target did not answer the connection probe")?
    {
        Err(_) => Ok(()),
        Ok(result) => bail!("bare KILL left the idle target connection usable: {result:?}"),
    }
}

fn release_connector_read(target: &ConnectorRead) -> Result<()> {
    target
        .release
        .send(())
        .context("release connector reader session after cancellation")
}

/// How long a `KILL QUERY` may take to reach the client that issued the
/// statement, once every Backend has already applied the abort.
///
/// The frontend withholds the interrupt until the coordinator's worker has
/// unwound, which is a bounded amount of local work: the drain loop notices
/// cancellation within its five-millisecond idle wait, releases the aborts to
/// the transport without waiting for their acknowledgements, and returns. Five
/// seconds is therefore three orders of magnitude of slack on the expected
/// path, while still landing below the frontend's own fifteen-second transport
/// queue-residence bound and the thirty-second initial query execution lease.
/// That gap is the point: an interrupt that only arrives after one of those
/// elapses is a cancellation that waited out a timeout rather than one that
/// was delivered, and this budget is what tells the two apart.
const HELD_QUERY_INTERRUPT_BUDGET: Duration = Duration::from_secs(5);

/// The interrupt owed to a client whose parked statement was killed.
///
/// Separate from [`assert_cancelled_query`] because it measures rather than
/// only classifies. A statement that produced nothing before the kill has its
/// whole wait inside one socket read, so "which error arrived" and "how long
/// it took" are the same question here, and a run that cannot report the
/// second cannot diagnose the first.
///
/// `killed_at` is the instant the `KILL QUERY` statement returned, so the
/// elapsed time this reports is the latency the client actually experienced.
fn assert_held_query_interrupted(
    done: &mpsc::Receiver<std::result::Result<Vec<i64>, mysql::Error>>,
    killed_at: std::time::Instant,
    budget: Duration,
) -> Result<()> {
    let result = match done.recv_timeout(budget) {
        Ok(result) => result,
        Err(_) => bail!(
            "the killed statement produced no answer within {} ms of its KILL QUERY, although \
             every installing Backend had already applied the abort; the frontend withholds a \
             KILL QUERY interrupt until its coordinator worker unwinds, so this is that worker \
             failing to unwind rather than a client-side wait",
            budget.as_millis()
        ),
    };
    let elapsed = killed_at.elapsed();
    let error = match result {
        Ok(rows) => bail!(
            "the killed statement succeeded after {} ms with {rows:?}",
            elapsed.as_millis()
        ),
        Err(error) => error,
    };
    match error {
        mysql::Error::MySqlError(error) if error.code == 1317 => {
            println!(
                "held-install KILL QUERY interrupt delivered after {} ms",
                elapsed.as_millis()
            );
            Ok(())
        }
        other => bail!(
            "expected MySQL cancellation error 1317 after {} ms, received {other}",
            elapsed.as_millis()
        ),
    }
}

fn assert_cancelled_query(
    done: &mpsc::Receiver<std::result::Result<Vec<i64>, mysql::Error>>,
    timeout: Duration,
) -> Result<()> {
    let result = done
        .recv_timeout(timeout)
        .context("connector reader did not terminate before the scenario deadline")?;
    let error = match result {
        Ok(rows) => bail!("connector reader unexpectedly succeeded after KILL QUERY: {rows:?}"),
        Err(error) => error,
    };
    match error {
        mysql::Error::MySqlError(error) if error.code == 1317 => Ok(()),
        other => bail!("expected MySQL cancellation error 1317, received {other}"),
    }
}

fn assert_connection_killed_query(
    done: &mpsc::Receiver<std::result::Result<Vec<i64>, mysql::Error>>,
    timeout: Duration,
) -> Result<()> {
    match done
        .recv_timeout(timeout)
        .context("KILL CONNECTION target query did not terminate before the scenario deadline")?
    {
        Ok(rows) => bail!("KILL CONNECTION target query unexpectedly succeeded: {rows:?}"),
        Err(_) => Ok(()),
    }
}

fn wait_for_open_reader_on_every_backend(
    context: &mut ScenarioContext,
    catalog: &str,
    operation: &str,
) -> Result<Vec<String>> {
    let marker = format!("{CONNECTOR_READER_OPEN} provider=iceberg instance={catalog}");
    wait_for_backend_logs(context, operation, |logs| {
        logs.iter().all(|log| log.contains(&marker))
    })
}

/// Waits until every Backend has opened a reader for `catalog` since
/// `baselines` were captured, with `awaited` bounding the wait.
///
/// `baselines` is what makes this repeatable within one scenario: each phase
/// judges only what its own read appended, so a phase cannot be satisfied by
/// a reader that a previous phase opened.
fn wait_for_new_reader_on_every_backend(
    context: &mut ScenarioContext,
    catalog: &str,
    operation: &str,
    baselines: &[String],
    awaited: AwaitedRead<'_>,
) -> Result<Vec<String>> {
    let baselines = baselines.to_vec();
    let catalog = catalog.to_owned();
    wait_for_backend_logs_while(context, operation, Some(awaited), move |logs| {
        every_backend_opened_reader_since(logs, &baselines, &catalog)
    })
}

/// Waits until every Backend has renewed this attempt's task lease again.
///
/// The barrier this replaced waited for a *new reader* after the rotation.
/// That is not a property a live attempt is obliged to produce: one unit reader
/// belongs to one scheduled split, a long read can sit inside a single split
/// for longer than the exchange's idle budget, and the barrier then outlives
/// the very query it is asking about. It has been observed to fail that way on
/// this scenario with and without CAD-1.
///
/// Lease renewal is the signal the question actually wants. A Backend renews
/// only for a context it still holds, it renews on a timer rather than on data,
/// and the renewals are monotonic, so "renewed since the rotation" is true
/// forever once it happens and cannot be missed by a poll.
fn wait_for_lease_renewal_on_every_backend(
    context: &mut ScenarioContext,
    operation: &str,
    baselines: &[String],
    awaited: AwaitedRead<'_>,
) -> Result<Vec<String>> {
    let baselines = baselines.to_vec();
    wait_for_backend_logs_while(context, operation, Some(awaited), move |logs| {
        let appended = appended_since(
            logs,
            &baselines,
            "for a task lease renewed since this phase began",
        )?;
        Ok(appended
            .iter()
            .all(|log| log.contains(PARTICIPATION_LEASE_RENEWED)))
    })
}

fn wait_for_replacement_reader_on_every_backend(
    context: &mut ScenarioContext,
    catalog: &str,
    old_versions: &[String],
) -> Result<Vec<String>> {
    wait_for_backend_logs(
        context,
        "wait for every BE to resolve the replacement catalog version",
        |logs| {
            logs.iter().zip(old_versions).all(|(log, old)| {
                reader_open_lines(log, catalog)
                    .any(|line| reader_catalog_version(line).is_some_and(|current| current != old))
            })
        },
    )
}

/// Waits until no reader is left open on any Backend.
///
/// Unlike an in-flight judgement this one is stable rather than a per-split
/// window: once the read is over its readers stay closed, so a poll cannot
/// miss it. Two properties it does lean on, both of which hold today and
/// neither of which is enforced here:
///
/// - the counts are not scoped to a catalog, so a second catalog reading on
///   the same Backend would be counted; every scenario that calls this uses
///   exactly one;
/// - a cumulative `opens == closes` could be satisfied by a previous phase
///   whose readers all closed, before this phase opens any. Every call site
///   is preceded, in the same phase, by `wait_for_new_reader_on_every_backend`,
///   which has already proved this phase's readers opened.
///
/// A scenario that breaks either assumption needs a baseline-relative
/// judgement here too, the way the in-flight barrier got one.
fn wait_for_balanced_reader_lifecycle(
    context: &mut ScenarioContext,
    operation: &str,
) -> Result<Vec<String>> {
    wait_for_backend_logs(context, operation, |logs| {
        logs.iter().all(|log| {
            let (opens, closes) = reader_counts(log);
            opens > 0 && opens == closes
        })
    })
}

fn wait_for_retired_catalog_version_close(
    context: &mut ScenarioContext,
    catalog: &str,
    old_versions: &[String],
) -> Result<Vec<String>> {
    wait_for_backend_logs(
        context,
        "wait for every retired catalog-version reader to close",
        |logs| {
            logs.iter().zip(old_versions).all(|(log, version)| {
                let (opens, closes) = reader_counts_for_catalog_version(log, catalog, version);
                opens > 0 && opens == closes
            })
        },
    )
}

fn wait_for_backend_logs(
    context: &mut ScenarioContext,
    operation: &str,
    predicate: impl Fn(&[String]) -> bool,
) -> Result<Vec<String>> {
    wait_for_backend_logs_while(context, operation, None, |logs| Ok(predicate(logs)))
}

/// The read a barrier is waiting on, so the wait cannot outlive it.
///
/// A barrier reads Backend logs; it cannot see that the query producing those
/// entries has already died. Without this, a wait whose read failed keeps
/// polling until the scenario budget is gone and then reports its own
/// timeout, naming the observation that never arrived instead of the failure
/// that prevented it -- and the real error, which only the client connection
/// saw, is lost.
struct AwaitedRead<'a> {
    read: &'a ConnectorRead,
    subject: &'a str,
}

impl AwaitedRead<'_> {
    fn new<'a>(read: &'a ConnectorRead, subject: &'a str) -> AwaitedRead<'a> {
        AwaitedRead { read, subject }
    }

    /// `Ok(())` while the read is still running.
    ///
    /// Taking the result is safe precisely because it ends the wait: nothing
    /// downstream of a failed barrier gets to consume it again.
    fn still_running(&self) -> Result<()> {
        match self.read.done.try_recv() {
            Err(mpsc::TryRecvError::Empty) => Ok(()),
            Err(mpsc::TryRecvError::Disconnected) => {
                bail!("{} ended without reporting a result", self.subject)
            }
            Ok(result) => bail!("{} ended first: {result:?}", self.subject),
        }
    }
}

/// Waits for an authorized Backend stdout observation, bounded by the scenario
/// deadline and by the liveness of the read that is supposed to produce it.
/// Catalog, reader and normal task markers use GrantedObservation on stdout;
/// the ACK-drop fault is counted independently across raw streams. Combined
/// diagnostic rendering is not an append-only observation stream.
fn wait_for_backend_logs_while(
    context: &mut ScenarioContext,
    operation: &str,
    awaited: Option<AwaitedRead<'_>>,
    predicate: impl Fn(&[String]) -> Result<bool>,
) -> Result<Vec<String>> {
    loop {
        // Before the logs: a read that is already over will never add the
        // entry this is waiting for, so its own failure is the answer.
        if let Some(awaited) = awaited.as_ref() {
            awaited
                .still_running()
                .with_context(|| format!("waiting to {operation}"))?;
        }
        let logs = (0..context.handle().be_count())
            .map(|index| context.handle().be_stdout_contents(index, usize::MAX))
            .collect::<Result<Vec<_>>>()
            .with_context(|| format!("read BE stdout observations while waiting to {operation}"))?;
        if predicate(&logs)? {
            return Ok(logs);
        }
        let remaining = context.remaining(operation)?;
        thread::sleep(remaining.min(Duration::from_millis(50)));
    }
}

/// Snapshot the same raw stdout stream the observation barriers read.
/// Its byte cursor remains valid when stderr independently receives diagnostics.
fn backend_log_snapshots(context: &mut ScenarioContext) -> Result<Vec<String>> {
    (0..context.handle().be_count())
        .map(|index| context.handle().be_stdout_contents(index, usize::MAX))
        .collect::<Result<Vec<_>>>()
        .context("read Backend stdout observation snapshots")
}

/// The per-backend stdout observations appended since `before`.
///
/// A truncated or replaced stream is an error rather than an empty appendix:
/// silently resetting a cursor would make absence assertions pass vacuously.
fn appended_since<'a>(logs: &'a [String], before: &[String], moment: &str) -> Result<Vec<&'a str>> {
    ensure!(
        logs.len() == before.len(),
        "stdout snapshot covers {} Backends but {} are running while checking {moment}",
        before.len(),
        logs.len(),
    );
    logs.iter()
        .zip(before)
        .enumerate()
        .map(|(index, (log, previous))| {
            ensure!(
                log.len() >= previous.len(),
                "BE[{index}] log was truncated while checking {moment}",
            );
            ensure!(
                log.starts_with(previous),
                "BE[{index}] stdout log prefix was replaced while checking {moment}",
            );
            Ok(&log[previous.len()..])
        })
        .collect()
}

/// The byte offset of the first appended line satisfying `matches`.
fn first_line_offset(text: &str, matches: impl Fn(&str) -> bool) -> Option<usize> {
    let mut offset = 0;
    for line in text.split_inclusive('\n') {
        if matches(line) {
            return Some(offset);
        }
        offset += line.len();
    }
    None
}

/// The exact shape one catalog's name takes inside a materialization marker.
///
/// `CatalogHandle` is printed with its derived `Debug`, so the name arrives
/// quoted inside its newtype. Matching that shape rather than the bare name
/// keeps a marker for `catalog_ready_lifecycle_v2` from satisfying an
/// assertion about `catalog_ready_lifecycle`.
fn catalog_instance_needle(catalog: &str) -> String {
    format!("ConnectorInstanceId(\"{catalog}\")")
}

/// Waits until at least one Backend has begun a cold catalog install, and
/// returns every Backend that has.
///
/// Not every Backend in the cluster, and not a count: a query context exists
/// only where the scheduler placed a task, and a catalog is installed on a
/// Backend because it has one, so the number of installing Backends is a
/// property of the plan rather than of the cluster. Requiring the marker
/// cluster-wide waits forever on a plan that touched two of three Backends.
///
/// The producing read must still be alive: an earlier SQL failure must end
/// this pre-cancellation barrier with its actual error, not a marker timeout.
/// This check does not belong on the post-KILL abort barrier.
///
/// The returned set is what every later assertion of the phase is keyed on,
/// and it is required to be non-empty: a phase whose subject never began an
/// install must fail here rather than pass every assertion vacuously.
fn wait_for_cold_catalog_install(
    context: &mut ScenarioContext,
    before: &[String],
    read: &ConnectorRead,
    operation: &str,
) -> Result<BTreeSet<usize>> {
    let logs = wait_for_backend_logs_while(
        context,
        operation,
        Some(AwaitedRead::new(read, "cold catalog install query")),
        |logs| {
            let added = appended_since(logs, before, operation)?;
            Ok(added
                .iter()
                .any(|text| text.contains(CATALOG_INSTALL_STARTED)))
        },
    )?;
    let installing = appended_since(&logs, before, operation)?
        .into_iter()
        .enumerate()
        .filter(|(_, added)| added.contains(CATALOG_INSTALL_STARTED))
        .map(|(index, _)| index)
        .collect::<BTreeSet<_>>();
    ensure!(
        !installing.is_empty(),
        "no Backend began a cold catalog install while waiting to {operation}"
    );
    Ok(installing)
}

/// Waits until every Backend in `backends` has applied this query's abort.
fn wait_for_participation_abort_on(
    context: &mut ScenarioContext,
    before: &[String],
    backends: &BTreeSet<usize>,
    operation: &str,
) -> Result<()> {
    ensure!(
        !backends.is_empty(),
        "no Backend was named while waiting to {operation}"
    );
    wait_for_backend_logs(context, operation, |logs| {
        backends.iter().all(|&index| {
            logs.get(index)
                .zip(before.get(index))
                .and_then(|(log, previous)| log.get(previous.len()..))
                .is_some_and(|added| added.contains(PARTICIPATION_ABORT_APPLIED))
        })
    })
    .map(|_| ())
}

/// Waits until every Backend in `backends` has opened a reader for `catalog`.
///
/// Keyed on the named Backends rather than on the whole cluster, for the same
/// reason `wait_for_cold_catalog_install` is: a Backend the scheduler gave no
/// task will never open a reader, so requiring one cluster-wide waits forever
/// on a plan that touched fewer Backends than the cluster has.
fn wait_for_open_reader_on(
    context: &mut ScenarioContext,
    before: &[String],
    catalog: &str,
    backends: &BTreeSet<usize>,
    operation: &str,
) -> Result<Vec<String>> {
    ensure!(
        !backends.is_empty(),
        "no Backend was named while waiting to {operation}"
    );
    wait_for_backend_logs(context, operation, |logs| {
        backends.iter().all(|&index| {
            logs.get(index)
                .zip(before.get(index))
                .and_then(|(log, previous)| log.get(previous.len()..))
                .is_some_and(|added| reader_open_lines(added, catalog).next().is_some())
        })
    })
}

fn wait_for_catalog_lifecycle_marker_on_backend(
    context: &mut ScenarioContext,
    before: &[String],
    backend: usize,
    marker: &str,
    operation: &str,
) -> Result<()> {
    let previous = before
        .get(backend)
        .with_context(|| format!("missing BE[{backend}] log snapshot"))?;
    wait_for_backend_logs(context, operation, |logs| {
        logs.get(backend)
            .and_then(|log| log.get(previous.len()..))
            .is_some_and(|appended| appended.contains(marker))
    })
    .map(|_| ())
}

/// Waits until one named Backend has built a runtime for exactly `catalog`.
fn wait_for_catalog_runtime_on_backend(
    context: &mut ScenarioContext,
    before: &[String],
    backend: usize,
    catalog: &str,
    operation: &str,
) -> Result<()> {
    let previous = before
        .get(backend)
        .with_context(|| format!("missing BE[{backend}] log snapshot"))?;
    let needle = catalog_instance_needle(catalog);
    wait_for_backend_logs(context, operation, |logs| {
        logs.get(backend)
            .and_then(|log| log.get(previous.len()..))
            .and_then(|added| {
                first_line_offset(added, |line| {
                    line.contains(CATALOG_RUNTIME_MATERIALIZED) && line.contains(&needle)
                })
            })
            .is_some()
    })
    .map(|_| ())
}

fn assert_no_appended_marker(
    before: &[String],
    after: &[String],
    marker: &str,
    moment: &str,
) -> Result<()> {
    for (index, added) in appended_since(after, before, moment)?
        .into_iter()
        .enumerate()
    {
        if added.contains(marker) {
            bail!("BE[{index}] emitted {marker} {moment}");
        }
    }
    Ok(())
}

/// No Backend built a runtime for `catalog` in this window.
///
/// Scoped to the one catalog rather than to the marker alone, because the
/// marker is process-wide: another catalog's runtime being built says nothing
/// about this one. The marker itself is the live, protocol-neutral emitter in
/// `ConnectorExecutionRoleBindingFactorySet::bind`, and this scenario observes
/// it appearing in its own later phases -- so an absence here is a fact about
/// this window and not about a marker nothing emits.
fn assert_no_appended_catalog_runtime(
    context: &mut ScenarioContext,
    before: &[String],
    catalog: &str,
    moment: &str,
) -> Result<()> {
    let logs = backend_log_snapshots(context)?;
    let needle = catalog_instance_needle(catalog);
    for (index, added) in appended_since(&logs, before, moment)?
        .into_iter()
        .enumerate()
    {
        if first_line_offset(added, |line| {
            line.contains(CATALOG_RUNTIME_MATERIALIZED) && line.contains(&needle)
        })
        .is_some()
        {
            bail!("BE[{index}] built a runtime for catalog {catalog} {moment}");
        }
    }
    Ok(())
}

/// No Backend read `catalog` in this window.
///
/// The observable successor of the retired "no Stage was admitted while the
/// catalog was not ready": a task cannot exist before its context's establish
/// returns, and the establish is where the install runs, so what a case can
/// see is that no execution reached this catalog's data.
///
/// Only usable where every Backend is prevented from finishing its install,
/// because the task protocol has no cross-Backend establish barrier: one
/// Backend whose own install completed will create its task and open its
/// reader while another is still installing. Use the per-Backend form for
/// anything narrower.
fn assert_no_appended_reader_open(
    context: &mut ScenarioContext,
    before: &[String],
    catalog: &str,
    moment: &str,
) -> Result<()> {
    let logs = backend_log_snapshots(context)?;
    for (index, added) in appended_since(&logs, before, moment)?
        .into_iter()
        .enumerate()
    {
        if reader_open_lines(added, catalog).next().is_some() {
            bail!("BE[{index}] opened a connector reader for catalog {catalog} {moment}");
        }
    }
    Ok(())
}

/// One named Backend read nothing from `catalog` in this window.
fn assert_no_appended_reader_open_on_backend(
    context: &mut ScenarioContext,
    before: &[String],
    backend: usize,
    catalog: &str,
    moment: &str,
) -> Result<()> {
    let logs = backend_log_snapshots(context)?;
    let appended = appended_since(&logs, before, moment)?;
    let added = appended
        .get(backend)
        .with_context(|| format!("missing BE[{backend}] log snapshot"))?;
    if reader_open_lines(added, catalog).next().is_some() {
        bail!("BE[{backend}] opened a connector reader for catalog {catalog} {moment}");
    }
    Ok(())
}

/// Every installing Backend built the runtime before it admitted any task.
///
/// This is what the retired "Stage only after CatalogReady" proved, expressed
/// in the two live markers that bracket it. Both offsets are required: a
/// Backend that began a cold install and never built the runtime, and one that
/// built it and then admitted nothing, are each a broken phase rather than a
/// satisfied ordering.
fn assert_catalog_runtime_precedes_task_create(
    before: &[String],
    after: &[String],
    installing: &BTreeSet<usize>,
    catalog: &str,
) -> Result<()> {
    let moment = "the cold catalog install ordering";
    ensure!(
        !installing.is_empty(),
        "no Backend began a cold install, so {moment} has no subject"
    );
    let appended = appended_since(after, before, moment)?;
    let needle = catalog_instance_needle(catalog);
    for &index in installing {
        let added = appended
            .get(index)
            .with_context(|| format!("missing BE[{index}] log snapshot"))?;
        let runtime = first_line_offset(added, |line| {
            line.contains(CATALOG_RUNTIME_MATERIALIZED) && line.contains(&needle)
        })
        .with_context(|| {
            format!(
                "BE[{index}] began a cold install of catalog {catalog} but never built its runtime"
            )
        })?;
        let created = first_line_offset(added, |line| line.contains(TASK_CREATE_APPLIED))
            .with_context(|| {
                format!(
                    "BE[{index}] built a runtime for catalog {catalog} but admitted no task, so \
                     nothing ever used it"
                )
            })?;
        if created < runtime {
            bail!(
                "BE[{index}] admitted a task before catalog {catalog} had a runtime on it \
                 (task at {created}, runtime at {runtime})"
            );
        }
    }
    Ok(())
}

/// A replayed establish reused the cell the first one installed.
///
/// Per Backend, because building a runtime twice is the defect; and at least
/// once across the cluster, because a window in which nothing was built at all
/// would satisfy the per-Backend bound without the replay having had a cold
/// install to be idempotent about.
fn assert_catalog_runtime_built_at_most_once(
    before: &[String],
    after: &[String],
    catalog: &str,
) -> Result<()> {
    let moment = "the establish replay";
    let needle = catalog_instance_needle(catalog);
    let mut built = 0;
    for (index, added) in appended_since(after, before, moment)?
        .into_iter()
        .enumerate()
    {
        let count = added
            .lines()
            .filter(|line| line.contains(CATALOG_RUNTIME_MATERIALIZED) && line.contains(&needle))
            .count();
        if count > 1 {
            bail!(
                "BE[{index}] built catalog {catalog}'s runtime {count} times across {moment}; a \
                 replayed establish must reuse the cell the first one installed"
            );
        }
        built += count;
    }
    ensure!(
        built > 0,
        "no Backend built catalog {catalog}'s runtime across {moment}, so the replay had no cold \
         install to be idempotent about"
    );
    Ok(())
}

/// No connector reader may open after this query's abort reached a backend.
///
/// Asserted per backend that had a context to abort, not per backend in the
/// cluster: a query context exists only where a task was placed, so one the
/// scheduler gave no task has nothing to abort and nothing to prove. At least
/// one must have recorded the abort, or this would pass on a query that was
/// never aborted anywhere -- which is the vacuous form the marker rename
/// could easily have left behind.
fn assert_no_reader_open_after_abort(logs: &[String]) -> Result<()> {
    let mut aborted_backends = 0_usize;
    for (index, log) in logs.iter().enumerate() {
        let Some(abort_offset) = log.find(PARTICIPATION_ABORT_APPLIED) else {
            continue;
        };
        aborted_backends += 1;
        if log[abort_offset..].contains(CONNECTOR_READER_OPEN) {
            bail!("BE[{index}] opened a connector reader after its query context was aborted");
        }
    }
    if aborted_backends == 0 {
        bail!(
            "no backend recorded {PARTICIPATION_ABORT_APPLIED} after KILL QUERY, so the abort \
             never reached one"
        );
    }
    Ok(())
}

fn reader_catalog_versions(logs: &[String], catalog: &str) -> Result<Vec<String>> {
    logs.iter()
        .enumerate()
        .map(|(index, log)| {
            reader_open_lines(log, catalog)
                .find_map(reader_catalog_version)
                .map(ToOwned::to_owned)
                .with_context(|| {
                    format!("BE[{index}] reader marker did not include catalog version")
                })
        })
        .collect()
}

fn reader_catalog_versions_after(
    logs: &[String],
    previous_logs: &[String],
    catalog: &str,
) -> Result<Vec<String>> {
    logs.iter()
        .zip(previous_logs)
        .enumerate()
        .map(|(index, (log, previous))| {
            let appended = log.get(previous.len()..).with_context(|| {
                format!("BE[{index}] log was truncated while checking FE restart catalog version")
            })?;
            reader_open_lines(appended, catalog)
                .find_map(reader_catalog_version)
                .map(ToOwned::to_owned)
                .with_context(|| {
                    format!(
                        "BE[{index}] post-restart reader marker did not include catalog version"
                    )
                })
        })
        .collect()
}

fn reader_open_lines<'a>(log: &'a str, catalog: &str) -> impl Iterator<Item = &'a str> {
    log.lines().filter(move |line| {
        line.contains(CONNECTOR_READER_OPEN)
            && line.contains("provider=iceberg")
            && line.contains(&format!("instance={catalog}"))
    })
}

fn reader_catalog_version(line: &str) -> Option<&str> {
    line.split_whitespace()
        .find_map(|field| field.strip_prefix("catalog_version="))
}

/// Whether every Backend opened a reader for `catalog` after its baseline.
///
/// This is deliberately a monotonic fact rather than the instantaneous
/// `opens > closes`. One unit reader belongs to one scheduled split and is
/// closed before the next split's is opened (`novarocks/worker/src/
/// typed_connector_runtime.rs`), so "a reader is open right now" is a
/// per-split window that a poll can miss entirely, on any one of the
/// Backends. "Opened one since this phase began" is true forever once it
/// happens, so no poll can miss it -- and unlike a bare "has ever opened"
/// latch it still tells two phases of the same scenario apart, which is what
/// the repeated barriers in the vended scenarios depend on.
fn every_backend_opened_reader_since(
    logs: &[String],
    baselines: &[String],
    catalog: &str,
) -> Result<bool> {
    ensure!(
        logs.len() == baselines.len(),
        "phase baseline covers {} Backends but {} are running",
        baselines.len(),
        logs.len()
    );
    let appended = appended_since(
        logs,
        baselines,
        "for a reader opened since this phase began",
    )?;
    Ok(appended
        .iter()
        .all(|log| reader_open_lines(log, catalog).next().is_some()))
}

fn reader_counts(log: &str) -> (usize, usize) {
    (
        log.match_indices(CONNECTOR_READER_OPEN).count(),
        log.match_indices(CONNECTOR_READER_CLOSE).count(),
    )
}

fn catalog_materialization_counts(logs: &[String]) -> Vec<usize> {
    logs.iter()
        .map(|log| {
            log.matches(novarocks_failpoint::observation_marker::CATALOG_RUNTIME_MATERIALIZED)
                .count()
        })
        .collect()
}

fn reader_counts_for_catalog_version(log: &str, catalog: &str, version: &str) -> (usize, usize) {
    let count = |event| {
        log.lines()
            .filter(|line| {
                line.contains(event)
                    && line.contains("provider=iceberg")
                    && line.contains(&format!("instance={catalog}"))
                    && reader_catalog_version(line) == Some(version)
            })
            .count()
    };
    (count(CONNECTOR_READER_OPEN), count(CONNECTOR_READER_CLOSE))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_line(catalog: &str, version: &str, sequence: u32) -> String {
        format!(
            "{CONNECTOR_READER_OPEN} provider=iceberg instance={catalog} \
             catalog_version={version} scheduled_split_sequence_id={sequence}\n"
        )
    }

    fn close_line(catalog: &str, version: &str, sequence: u32) -> String {
        format!(
            "{CONNECTOR_READER_CLOSE} provider=iceberg instance={catalog} \
             catalog_version={version} scheduled_split_sequence_id={sequence}\n"
        )
    }

    /// Every channel a `ConnectorRead` owns, so a test can hold the sending
    /// ends and decide when the read is over.
    struct ReadHarness {
        read: ConnectorRead,
        done_tx: mpsc::SyncSender<std::result::Result<Vec<i64>, mysql::Error>>,
        _ready_tx: mpsc::SyncSender<u32>,
        _probe_rx: mpsc::Receiver<()>,
        _probe_result_tx: mpsc::SyncSender<std::result::Result<Option<i64>, mysql::Error>>,
        _release_rx: mpsc::Receiver<()>,
    }

    fn read_harness() -> ReadHarness {
        let (ready_tx, ready) = mpsc::sync_channel(1);
        let (done_tx, done) = mpsc::sync_channel(1);
        let (probe, probe_rx) = mpsc::sync_channel(1);
        let (probe_result_tx, probe_result) = mpsc::sync_channel(1);
        let (release, release_rx) = mpsc::channel();
        ReadHarness {
            read: ConnectorRead {
                ready,
                done,
                probe,
                probe_result,
                release,
                thread: thread::spawn(|| Ok(())),
            },
            done_tx,
            _ready_tx: ready_tx,
            _probe_rx: probe_rx,
            _probe_result_tx: probe_result_tx,
            _release_rx: release_rx,
        }
    }

    #[test]
    fn post_publish_answer_scenarios_are_distinct_explicit_cases() {
        let registered = scenarios();
        let mut replacements = Vec::new();
        for case in PostPublishAnswerCase::ALL {
            let matching = registered
                .iter()
                .filter(|scenario| scenario.name() == case.name())
                .collect::<Vec<_>>();
            assert_eq!(matching.len(), 1, "{}", case.name());
            assert!(matching[0].is_explicit_stage(), "{}", case.name());
            assert!(!replacements.contains(&case.replacement()), "{case:?}");
            replacements.push(case.replacement());
        }
        assert_eq!(
            PostPublishAnswerCase::InsertNotFound.replacement(),
            VendedResponseReplacement {
                request: VendedMutationRequest::TableCommit,
                answer: VendedReplacementAnswer::NotFound,
            }
        );
        assert_eq!(
            PostPublishAnswerCase::InsertBadRequest.replacement(),
            VendedResponseReplacement {
                request: VendedMutationRequest::TableCommit,
                answer: VendedReplacementAnswer::BadRequest,
            }
        );
        assert_eq!(
            PostPublishAnswerCase::CreateDatabaseNotFound.replacement(),
            VendedResponseReplacement {
                request: VendedMutationRequest::NamespaceCreate,
                answer: VendedReplacementAnswer::NotFound,
            }
        );
    }

    #[test]
    fn a_running_read_lets_the_barrier_keep_waiting() {
        let harness = read_harness();
        AwaitedRead::new(&harness.read, "the probe read")
            .still_running()
            .expect("a read that has not finished does not end the wait");
    }

    #[test]
    fn a_read_that_already_ended_stops_the_wait_and_names_its_result() {
        let harness = read_harness();
        harness.done_tx.send(Ok(vec![7])).expect("publish result");
        let error = AwaitedRead::new(&harness.read, "the probe read")
            .still_running()
            .expect_err("a finished read must end the wait");
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains("the probe read ended first"),
            "{rendered}"
        );
        assert!(rendered.contains('7'), "{rendered}");
    }

    #[test]
    fn an_early_failed_cold_read_preserves_the_actual_sql_error() {
        let harness = read_harness();
        harness
            .done_tx
            .send(Err(mysql::Error::MySqlError(mysql::MySqlError {
                code: 1105,
                state: "HY000".into(),
                message: "original cold preparation failure".into(),
            })))
            .expect("publish original SQL failure");
        let error = AwaitedRead::new(&harness.read, "cold catalog install query")
            .still_running()
            .expect_err("the producer failure must end a cold-install wait");
        let rendered = format!("{error:#}");
        assert!(rendered.contains("cold catalog install query ended first"));
        assert!(rendered.contains("1105"));
        assert!(rendered.contains("HY000"));
        assert!(rendered.contains("original cold preparation failure"));
    }

    #[test]
    fn a_read_whose_thread_vanished_stops_the_wait_too() {
        let mut harness = read_harness();
        // Dropping the last sender is how a reader thread that died without
        // publishing anything becomes observable.
        let (dead_tx, dead_rx) = mpsc::sync_channel(1);
        drop(dead_tx);
        harness.read.done = dead_rx;
        let error = AwaitedRead::new(&harness.read, "the probe read")
            .still_running()
            .expect_err("a vanished read must end the wait");
        assert!(
            format!("{error:#}").contains("ended without reporting a result"),
            "{error:#}"
        );
    }

    #[test]
    fn a_reader_opened_after_the_baseline_counts_on_every_backend() {
        let baselines = vec!["prologue\n".to_string(); 3];
        let logs = baselines
            .iter()
            .map(|baseline| format!("{baseline}{}", open_line("cat", "v1", 1)))
            .collect::<Vec<_>>();
        assert!(every_backend_opened_reader_since(&logs, &baselines, "cat").expect("judged"));
    }

    #[test]
    fn a_backend_that_has_not_opened_since_the_baseline_holds_the_barrier() {
        let baselines = vec!["prologue\n".to_string(); 3];
        let mut logs = baselines
            .iter()
            .map(|baseline| format!("{baseline}{}", open_line("cat", "v1", 1)))
            .collect::<Vec<_>>();
        logs[1].clone_from(&baselines[1]);
        assert!(!every_backend_opened_reader_since(&logs, &baselines, "cat").expect("judged"));
    }

    #[test]
    fn a_reader_opened_before_the_baseline_does_not_satisfy_a_later_phase() {
        // The defect a bare "has ever opened" latch would reintroduce: the
        // vended scenarios run this barrier once per phase, and every phase
        // after the first would pass without observing anything.
        let baselines = vec![open_line("cat", "v1", 1); 3];
        let logs = baselines.clone();
        assert!(!every_backend_opened_reader_since(&logs, &baselines, "cat").expect("judged"));
    }

    #[test]
    fn the_judgement_is_scoped_to_one_catalog() {
        let baselines = vec![String::new(); 3];
        let logs = vec![open_line("other_catalog", "v1", 1); 3];
        assert!(!every_backend_opened_reader_since(&logs, &baselines, "cat").expect("judged"));
    }

    #[test]
    fn closing_a_reader_does_not_retract_the_fact_that_it_opened() {
        // The whole point of the monotonic judgement: one unit reader belongs
        // to one split and is closed before the next opens, so an
        // `opens > closes` judgement would flip back to false here.
        let baselines = vec![String::new(); 3];
        let logs = vec![
            format!(
                "{}{}",
                open_line("cat", "v1", 1),
                close_line("cat", "v1", 1)
            );
            3
        ];
        assert!(every_backend_opened_reader_since(&logs, &baselines, "cat").expect("judged"));
        // Guards the contrast rather than the old helper: the instantaneous
        // form really does go false at exactly this point.
        let (opens, closes) = reader_counts(&logs[0]);
        assert_eq!(opens, closes);
    }

    #[test]
    fn a_first_stdout_catalog_marker_remains_complete_and_in_stream_order() {
        let ready = &*format!(
            "{} role=be\n",
            novarocks_failpoint::observation_marker::READY
        );
        let started = format!("{CATALOG_INSTALL_STARTED} execution_id=1:2:1 catalog_count=1\n");
        let runtime = format!("{CATALOG_RUNTIME_MATERIALIZED} catalog=fixture\n");
        let created = format!("{TASK_CREATE_APPLIED} execution_id=1:2:1\n");
        let expected = format!("{started}{runtime}{created}");
        let before = vec![ready.to_string()];
        let after = vec![format!("{ready}{expected}")];
        let added = appended_since(&after, &before, "the first cold install").unwrap();
        assert_eq!(added, vec![expected.as_str()]);
        assert_eq!(
            first_line_offset(added[0], |line| line.contains(CATALOG_INSTALL_STARTED)),
            Some(0)
        );
        assert_eq!(
            first_line_offset(added[0], |line| line.contains(CATALOG_RUNTIME_MATERIALIZED)),
            Some(started.len())
        );
        assert_eq!(
            first_line_offset(added[0], |line| line.contains(TASK_CREATE_APPLIED)),
            Some(started.len() + runtime.len())
        );
    }

    #[test]
    fn a_replaced_stdout_prefix_is_rejected_even_when_its_length_did_not_shrink() {
        let before = vec!["original\n".to_string()];
        for replacement in [
            "replaced\n".to_string(),
            format!("replaced\n{CATALOG_INSTALL_STARTED}\n"),
        ] {
            let after = vec![replacement];
            assert!(after[0].len() >= before[0].len());
            let error = appended_since(&after, &before, "a replaced stream")
                .expect_err("replacement must not invent a fresh observation window");
            assert!(format!("{error:#}").contains("stdout log prefix was replaced"));
        }
    }

    #[test]
    fn a_log_that_shrank_below_its_baseline_is_an_error_not_a_panic() {
        let baselines = vec!["a much longer prologue\n".to_string(); 3];
        let logs = vec!["short\n".to_string(); 3];
        let error = every_backend_opened_reader_since(&logs, &baselines, "cat")
            .expect_err("a truncated log cannot be judged");
        assert!(
            format!("{error:#}").contains("log was truncated while checking"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn a_baseline_that_does_not_cover_every_backend_is_an_error() {
        let baselines = vec![String::new(); 2];
        let logs = vec![open_line("cat", "v1", 1); 3];
        let error = every_backend_opened_reader_since(&logs, &baselines, "cat")
            .expect_err("a baseline must cover every running Backend");
        assert!(
            format!("{error:#}").contains("covers 2 Backends but 3 are running"),
            "unexpected error: {error:#}"
        );
    }
}
