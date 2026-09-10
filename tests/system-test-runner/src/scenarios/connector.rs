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
    VendedRestCatalogConfig, VendedRestCatalogFixture, VendedS3Credential,
    VendedTableCommitResponseBehavior,
};
use novarocks_cluster_harness::{
    CrossProcessChildEnvironment, CrossProcessConfigOverlay, NativeTrustFixture,
    QueryExecutionResourceSnapshot, ServerHandle,
};
use novarocks_secret::SecretValue;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Mutex;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

const CONNECTOR_READER_OPEN: &str = "NOVAROCKS_CONNECTOR_UNIT_READER_OPEN";
/// The task protocol's abort marker, emitted where the abort actually
/// applied. It replaces `NOVAROCKS_QUERY_LIFECYCLE_ABORT`, which only the
/// retired chain emits and which a production query no longer reaches.
const TASK_CONTEXT_ABORT_APPLIED: &str = "NOVAROCKS_TASK_CONTEXT_ABORT_APPLIED";
const VENDED_METADATA_CREDENTIAL_NAME: &str = "vended-rest-metadata";
const VENDED_METADATA_CREDENTIAL_GENERATION: &str = "v1";
const VENDED_METADATA_ACCESS_KEY_ENV: &str = "NOVAROCKS_VENDED_METADATA_ACCESS_KEY_ID";
const VENDED_METADATA_SECRET_KEY_ENV: &str = "NOVAROCKS_VENDED_METADATA_SECRET_ACCESS_KEY";
/// A Backend is about to build at least one catalog runtime for one attempt.
///
/// The task protocol's successor of `NOVAROCKS_CATALOG_LOADING`, and the only
/// point at which a cold install is observably in flight: the install runs
/// inside the establish that carries the catalog bindings, not in a background
/// pass with its own Loading/Ready progression.
const CATALOG_INSTALL_STARTED: &str = "NOVAROCKS_CATALOG_INSTALL_STARTED";
/// One selected Backend's cold install was failed by the runner's trigger.
const CATALOG_INSTALL_FAILED: &str = "NOVAROCKS_CATALOG_INSTALL_FAILED";
/// A provider bind produced one catalog runtime. Protocol-neutral and live:
/// it is emitted by the catalog manager's factory set, which both the retired
/// chain and the task protocol drive, and it is the fact "this catalog is
/// usable here" actually consists of.
const CATALOG_RUNTIME_MATERIALIZED: &str = "NOVAROCKS_CATALOG_RUNTIME_MATERIALIZED";
/// One task was admitted on a Backend. The successor of
/// `NOVAROCKS_CATALOG_STAGE_ADMITTED` for ordering claims: a task is the
/// smallest thing a Backend admits, and it cannot exist before the establish
/// that installed its context's catalogs returned.
const TASK_CREATE_APPLIED: &str = "NOVAROCKS_TASK_CREATE_APPLIED";
/// The task protocol's establish acknowledgement drop, and the marker its
/// claim prints. It replaces the retired `InitAck` drop: `EstablishQueryContext`
/// is where a query's catalog bindings cross the boundary, so it is the
/// operation whose lost answer could make one Backend install twice.
const ESTABLISH_CONTEXT_ACK_DROP: &str = "establish-context-ack-drop";
const ESTABLISH_CONTEXT_ACK_DROPPED_MARKER: &str = "NOVAROCKS_TASK_ESTABLISH_CONTEXT_ACK_DROPPED";
/// The one Backend the injected catalog-install failure and the dropped
/// establish acknowledgement are armed on.
///
/// A Backend index, never a count: it is fixed so the scenario can name the
/// process its evidence must come from, and nothing here assumes how many
/// Backends a plan reaches.
const CATALOG_FAILURE_BACKEND: usize = 1;
const TYPED_SPLIT_ACCEPTED: &str = "NOVAROCKS_TASK_SPLIT_ASSIGNMENT_ACCEPTED";
const TYPED_SPLIT_NO_MORE: &str = "NOVAROCKS_TASK_SPLIT_NO_MORE";
const TYPED_PAGE_SOURCE_OPEN: &str = "NOVAROCKS_CONNECTOR_PAGE_SOURCE_OPEN";
const TYPED_PAGE_SOURCE_CLOSE: &str = "NOVAROCKS_CONNECTOR_PAGE_SOURCE_CLOSE";
const CONNECTOR_READER_CLOSE: &str = "NOVAROCKS_CONNECTOR_UNIT_READER_CLOSE";
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
        Box::new(DistributedReaderCancel),
        Box::new(DistributedReaderKillConnection),
        Box::new(CatalogFeRestartCache),
        Box::new(CatalogReadyLifecycle),
        Box::new(CatalogReadWriteRuntime),
        Box::new(VendedRestReadWritePem::default()),
        Box::new(VendedRestRefreshPem::default()),
        Box::new(VendedRestWriteOutcomePem::default()),
        Box::new(CatalogVersionDrain),
        Box::new(StaticCredentialGeneration::default()),
        Box::new(AccessDomainCacheIsolation::default()),
        Box::new(PredicatePageIndexPruning),
        Box::new(TypedReadData),
    ]
}

/// Proves a typed connector read works on the real 1FE+3BE topology, and that
/// its splits are delivered at runtime rather than frozen into the plan.
///
/// A correct result alone would not show that: a single backend reading every
/// file would produce exactly the same rows. The evidence that distinguishes
/// the two is which processes accepted split assignments.
struct TypedReadData;

impl Scenario for TypedReadData {
    fn name(&self) -> &'static str {
        "connector/iceberg-typed-read-data"
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        connector_reader_environment()
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(connector_launch_config())
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect(
            &user,
            port,
            context.remaining("connect typed read control session")?,
        )?;

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
        let warm_profile: Vec<String> =
            control
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
        let counted: Vec<i64> = control
            .query(&counted_query)
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

struct DistributedReaderCancel;

impl Scenario for DistributedReaderCancel {
    fn name(&self) -> &'static str {
        "connector/distributed-reader-cancel"
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        connector_reader_environment()
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(connector_launch_config())
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect(
            &user,
            port,
            context.remaining("connect connector cancellation control session")?,
        )?;

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
        let target = start_connector_read(
            &user,
            port,
            "connector_cancel_catalog",
            "connector_cancel_db",
            "connector_cancel_data",
        )?;
        let connection_id = target
            .ready
            .recv_timeout(context.remaining("receive connector read connection id")?)
            .context("connector read terminated before publishing its connection id")?;

        wait_for_in_flight_reader_on_every_backend(
            context,
            "connector_cancel_catalog",
            "wait for every BE to open a distributed connector reader",
        )?;
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
            &target,
            context.remaining("verify KILL QUERY target connection remains usable")?,
        )?;
        assert_idle_query(&mut control, connection_id)?;
        release_connector_read(&target)?;
        target
            .thread
            .join()
            .map_err(|_| anyhow::anyhow!("connector read thread panicked"))??;

        let reader_logs = wait_for_balanced_reader_lifecycle(
            context,
            "wait for connector reader close after cancellation",
        )?;
        assert_no_reader_open_after_abort(&reader_logs)?;
        await_resource_convergence(context, &baseline, "cancelled connector read")?;

        context.action("verify a subsequent distributed query succeeds after connector cleanup");
        let rows: Vec<i64> = control
            .query("SELECT v FROM (SELECT 1 AS v UNION ALL SELECT 2) t ORDER BY v")
            .context("run post-cancellation distributed query")?;
        if rows != [1, 2] {
            bail!("post-cancellation distributed query returned {rows:?}, expected [1, 2]");
        }
        Ok(())
    }
}

struct DistributedReaderKillConnection;

impl Scenario for DistributedReaderKillConnection {
    fn name(&self) -> &'static str {
        "connector/distributed-reader-kill-connection"
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        connector_reader_environment()
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(connector_launch_config())
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect(
            &user,
            port,
            context.remaining("connect connector KILL CONNECTION control session")?,
        )?;

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
        let target = start_connector_read(
            &user,
            port,
            "connector_kill_connection_catalog",
            "connector_kill_connection_db",
            "connector_kill_connection_data",
        )?;
        let connection_id = target
            .ready
            .recv_timeout(context.remaining("receive KILL CONNECTION target id")?)
            .context("KILL CONNECTION target terminated before publishing its connection id")?;
        wait_for_in_flight_reader_on_every_backend(
            context,
            "connector_kill_connection_catalog",
            "wait for every BE to open a KILL CONNECTION target reader",
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
            &target,
            context.remaining("verify KILL CONNECTION closes the target socket")?,
        )?;
        release_connector_read(&target)?;
        target
            .thread
            .join()
            .map_err(|_| anyhow::anyhow!("KILL CONNECTION target thread panicked"))??;

        let reader_logs = wait_for_balanced_reader_lifecycle(
            context,
            "wait for connector reader close after KILL CONNECTION",
        )?;
        assert_no_reader_open_after_abort(&reader_logs)?;
        await_resource_convergence(context, &baseline, "KILL CONNECTION connector read")?;

        context.action("verify bare KILL closes an idle public-MySQL target socket");
        let idle_target = start_idle_mysql_connection(&user, port)?;
        let idle_connection_id = idle_target
            .ready
            .recv_timeout(context.remaining("receive bare KILL target id")?)
            .context("bare KILL target terminated before publishing its connection id")?;
        control
            .query_drop(format!("KILL {idle_connection_id}"))
            .context("issue bare public MySQL KILL for idle target")?;
        assert_idle_target_connection_is_closed(
            &idle_target,
            context.remaining("verify bare KILL closes the idle target socket")?,
        )?;
        idle_target
            .thread
            .join()
            .map_err(|_| anyhow::anyhow!("bare KILL target thread panicked"))??;

        context.action("verify the KILL requester remains usable after both target terminations");
        let rows: Vec<i64> = control
            .query("SELECT v FROM (SELECT 1 AS v UNION ALL SELECT 2) t ORDER BY v")
            .context("run requester query after KILL CONNECTION and bare KILL")?;
        if rows != [1, 2] {
            bail!("post-KILL requester query returned {rows:?}, expected [1, 2]");
        }
        Ok(())
    }
}

struct CatalogVersionDrain;

/// Drives one CatalogSet through its observable cold and warm lifecycle on
/// the real control stream, including cancellation while installation is held.
struct CatalogReadyLifecycle;

impl Scenario for CatalogReadyLifecycle {
    fn name(&self) -> &'static str {
        "connector/catalog-ready-lifecycle"
    }

    fn launch_config(&self, scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        let mut config = connector_launch_config();
        let hold_file = scenario_root.join("catalog-install-hold");
        let failure_file = scenario_root.join("catalog-install-failure");
        // Both triggers are cleared here, before the cluster is launched, and
        // not only where each is written.
        //
        // Neither is armed by the scenario until well after the fixture has
        // written 300,000 rows through this catalog, and those writes are
        // themselves cold installs. So a trigger left behind by a run that
        // died while armed takes effect on the *fixture*, long before the
        // clear at either write site can run: the fixture's first cold
        // install is held from startup, its lease renewal outruns the
        // establish, and the write fails closed with "a renewal cannot create
        // a query context" -- observed exactly that way. The scenario root
        // outlives a single run and nothing else removes these files, so the
        // only clear that can prevent it is one that happens before the
        // backends exist.
        for trigger in [&hold_file, &failure_file] {
            if trigger.exists() {
                std::fs::remove_file(trigger).with_context(|| {
                    format!("clear stale catalog trigger {}", trigger.display())
                })?;
            }
        }
        config.child_environment.be.insert(
            "NOVAROCKS_SQL_TEST_CATALOG_INSTALL_HOLD_FILE".to_string(),
            hold_file.to_string_lossy().into_owned(),
        );
        config.child_environment.be.insert(
            "NOVAROCKS_SQL_TEST_EMIT_CATALOG_LIFECYCLE_MARKER".to_string(),
            "1".to_string(),
        );
        config
            .child_environment
            .be_by_index
            .entry(CATALOG_FAILURE_BACKEND)
            .or_default()
            .insert(
                "NOVAROCKS_SQL_TEST_CATALOG_INSTALL_FAILURE_FILE".to_string(),
                failure_file.to_string_lossy().into_owned(),
            );
        Ok(config)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect(
            &user,
            port,
            context.remaining("connect catalog ready lifecycle control session")?,
        )?;

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
        let hold_file = context.scenario_root().join("catalog-install-hold");
        // The scenario root outlives a single run, and a run that dies while
        // the install is held leaves this file behind -- after which every
        // later run's first cold install is held from the start, its lease
        // renewal outruns the establish, and the fixture write fails closed
        // with "a renewal cannot create a query context". Observed exactly
        // that way. Clearing it here makes the setup idempotent rather than
        // dependent on the previous run having exited cleanly.
        if hold_file.exists() {
            std::fs::remove_file(&hold_file).with_context(|| {
                format!(
                    "clear stale catalog-install hold file {}",
                    hold_file.display()
                )
            })?;
        }
        let before_cancel = backend_log_snapshots(context)?;
        std::fs::write(&hold_file, "hold\n")
            .with_context(|| format!("create catalog-install hold file {}", hold_file.display()))?;
        context.action("hold a cold catalog install, then cancel it before any runtime is built");
        // The scenario-bounded connection form, because this statement is
        // parked before it produces a single row: its first response byte is
        // the cancellation itself, so a socket read timeout would turn any
        // server latency into EAGAIN and destroy the measurement below.
        let cancelled = start_held_connector_read(&user, port, CATALOG, DATABASE, TABLE)?;
        let connection_id = cancelled
            .ready
            .recv_timeout(context.remaining("receive held catalog query connection id")?)
            .context("held catalog query terminated before publishing connection id")?;
        let installing = wait_for_cold_catalog_install(
            context,
            &before_cancel,
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
        wait_for_context_abort_on(
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
        // `novarocks/frontend/src/query.rs`). By this line the abort has
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
            &cancelled,
            context.remaining("verify KILL QUERY preserves held client connection")?,
        )?;
        release_catalog_install_hold(&hold_file)?;
        release_connector_read(&cancelled)?;
        cancelled
            .thread
            .join()
            .map_err(|_| anyhow::anyhow!("held catalog reader thread panicked"))??;
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
        let failure_file = context.scenario_root().join("catalog-install-failure");
        let before_failure = backend_log_snapshots(context)?;
        std::fs::write(&failure_file, "fail\n").with_context(|| {
            format!(
                "create catalog-install failure trigger {}",
                failure_file.display()
            )
        })?;
        context.action("fail the cold catalog install on one Backend and reject the query");
        let failed_query: Result<Vec<i64>, mysql::Error> =
            control.query(format!("SELECT count(*) FROM {CATALOG}.{DATABASE}.{TABLE}"));
        if let Ok(rows) = failed_query {
            bail!("catalog install failure query unexpectedly succeeded: {rows:?}");
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
        std::fs::remove_file(&failure_file).with_context(|| {
            format!(
                "remove catalog-install failure trigger {}",
                failure_file.display()
            )
        })?;
        let before_retry = backend_log_snapshots(context)?;
        context.action("retry the failed catalog version after clearing the one-Backend failure");
        let rows: Vec<i64> = control
            .query(format!("SELECT count(*) FROM {CATALOG}.{DATABASE}.{TABLE}"))
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
        std::fs::write(&hold_file, "hold\n").with_context(|| {
            format!("recreate catalog-install hold file {}", hold_file.display())
        })?;
        context.action("hold a cold catalog install until every installing Backend reports it");
        let cold = start_connector_read(&user, port, CATALOG, DATABASE, TABLE)?;
        let cold_connection_id = cold
            .ready
            .recv_timeout(context.remaining("receive cold catalog query connection id")?)
            .context("cold catalog query terminated before publishing connection id")?;
        let installing = wait_for_cold_catalog_install(
            context,
            &before_cold,
            "observe the cold catalog install before releasing it",
        )?;
        assert_no_appended_catalog_runtime(
            context,
            &before_cold,
            CATALOG,
            "before the cold catalog install is released",
        )?;
        context.action("release the cold catalog install and require the runtime before any task");
        release_catalog_install_hold(&hold_file)?;
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
            &cold,
            context.remaining("verify cold catalog KILL QUERY connection")?,
        )?;
        release_connector_read(&cold)?;
        cold.thread
            .join()
            .map_err(|_| anyhow::anyhow!("cold catalog reader thread panicked"))??;

        // A warm query reuses the runtime the cold one built.
        let before_warm = backend_log_snapshots(context)?;
        context.action("execute a warm query without another catalog install");
        let rows: Vec<i64> = control
            .query(format!("SELECT count(*) FROM {CATALOG}.{DATABASE}.{TABLE}"))
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
        // This replaced the retired InitAck drop: `EstablishQueryContext` is
        // where the task protocol carries a query's catalog bindings, so it is
        // the operation whose lost answer can make a backend install twice.
        control
            .query_drop(format!("DROP CATALOG {CATALOG}"))
            .context("replace warm catalog before establish-replay coverage")?;
        create_catalog(&mut control, CATALOG, &warehouse)?;
        let before_replay = backend_log_snapshots(context)?;
        context
            .handle()
            .arm_query_lifecycle_fault(CATALOG_FAILURE_BACKEND, ESTABLISH_CONTEXT_ACK_DROP)
            .context("arm the establish acknowledgement drop for the cold catalog replay")?;
        context.action("retry the exact cold establish after its acknowledgement is dropped");
        let rows: Vec<i64> = control
            .query(format!("SELECT count(*) FROM {CATALOG}.{DATABASE}.{TABLE}"))
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
        assert_appended_marker_on_backend(
            &before_replay,
            &after_replay,
            CATALOG_FAILURE_BACKEND,
            ESTABLISH_CONTEXT_ACK_DROPPED_MARKER,
        )?;
        assert_catalog_runtime_built_at_most_once(&before_replay, &after_replay, CATALOG)?;
        await_resource_convergence(context, &baseline, "catalog ready lifecycle")?;
        Ok(())
    }
}

/// Exercises distributed writes and reads through one exact catalog runtime,
/// then proves a replacement Backend can rebuild that runtime from the frozen
/// catalog properties.
struct CatalogReadWriteRuntime;

impl Scenario for CatalogReadWriteRuntime {
    fn name(&self) -> &'static str {
        "connector/catalog-read-write-runtime"
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        connector_reader_environment()
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(connector_launch_config())
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect(
            &user,
            port,
            context.remaining("connect catalog read-write control session")?,
        )?;

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
        for range in ["1, 1000", "1001, 2000", "2001, 3000"] {
            control
                .query_drop(format!(
                    "INSERT INTO {CATALOG}.{DATABASE}.{TABLE} SELECT generate_series FROM TABLE(generate_series({range}))"
                ))
                .with_context(|| format!("distributed insert range {range} through catalog writer runtime"))?;
        }
        assert_catalog_read_summary(&mut control, CATALOG, DATABASE, TABLE, 3_000, 4_501_500)?;
        let before_restart_logs = wait_for_open_reader_on_every_backend(
            context,
            CATALOG,
            "observe catalog readers after distributed write",
        )?;
        let before_restart_versions = reader_catalog_versions(&before_restart_logs, CATALOG)?;
        let before_restart_materializations = catalog_materialization_counts(&before_restart_logs);

        context
            .action("replace one Backend and rebuild its catalog runtime from CatalogProperties");
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

        control
            .query_drop(format!(
                "INSERT INTO {CATALOG}.{DATABASE}.{TABLE} VALUES (1001)"
            ))
            .context("distributed insert after BE replacement")?;
        assert_catalog_read_summary(&mut control, CATALOG, DATABASE, TABLE, 3_001, 4_502_501)?;
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
    }
}

/// Reserved M2 acceptance entrypoint for the real REST-vended read/write
/// path. It is explicit-only because the fixture must own its isolated REST
/// catalog and S3 credential authority rather than consuming shared Docker
/// state.
#[derive(Default)]
struct VendedRestReadWritePem {
    fixture: Mutex<Option<VendedRestSystemFixture>>,
}

struct VendedRestSystemFixture {
    rest: IsolatedIcebergRestFixture,
    proxy: VendedRestCatalogFixture,
}

impl Scenario for VendedRestReadWritePem {
    fn name(&self) -> &'static str {
        "connector/vended-credential-read-write"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        connector_reader_environment()
    }

    fn launch_config(&self, scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        let mut rest = IsolatedIcebergRestFixture::start(scenario_root)
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

        let mut config = connector_launch_config();
        configure_vended_metadata_access(&mut config, metadata_identity);
        // Vended lease envelopes are confidential lifecycle payloads. The
        // scenario must therefore exercise the native TLS branch, never the
        // default authenticated h2c fixture.
        config.native_trust_fixture = NativeTrustFixture::pem_ip();
        Ok(config)
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
        let mut control = mysql_actor::connect(
            &user,
            port,
            context.remaining("connect vended REST read/write control session")?,
        )?;

        const CATALOG: &str = "vended_rest_catalog";
        const DATABASE: &str = "vended_rest_db";
        const TABLE: &str = "vended_rest_data";
        const CTAS: &str = "vended_rest_ctas";
        context.action("create REST catalog with an explicit vended data binding");
        control
            .query_drop(format!(
                "CREATE EXTERNAL CATALOG {CATALOG} PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"rest\",\"uri\"=\"{proxy_uri}\",\"iceberg.catalog.warehouse\"=\"{warehouse}\",\"aws.s3.endpoint\"=\"{}\",\"aws.s3.region\"=\"us-east-1\",\"aws.s3.enable_path_style_access\"=\"true\",\"credential.object-store-metadata.consumer-role\"=\"frontend\",\"credential.object-store-metadata.mode\"=\"static\",\"credential.object-store-metadata.name\"=\"{VENDED_METADATA_CREDENTIAL_NAME}\",\"credential.object-store-metadata.generation\"=\"{VENDED_METADATA_CREDENTIAL_GENERATION}\",\"credential.object-store-data.consumer-role\"=\"backend\",\"credential.object-store-data.mode\"=\"vended\")",
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
        let rows: Vec<(i64, i64)> = control
            .query(format!(
                "SELECT count(*), sum(v) FROM {CATALOG}.{DATABASE}.{TABLE}"
            ))
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
        if audit.refreshes != 0 || audit.issued_key_ids.len() != 1 {
            bail!(
                "normal vended read/write unexpectedly refreshed or issued a second key: {audit:?}"
            );
        }
        context.action(
            "verify REST fixture observed only the initial vended credential in normal read/write",
        );
        await_resource_convergence(context, &baseline, "vended REST read/write")?;
        Ok(())
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
    fn name(&self) -> &'static str {
        "connector/vended-credential-write-outcome"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        connector_reader_environment()
    }

    fn launch_config(&self, scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        let mut rest = IsolatedIcebergRestFixture::start(scenario_root)
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

        let mut config = connector_launch_config();
        configure_vended_metadata_access(&mut config, metadata_identity);
        config.native_trust_fixture = NativeTrustFixture::pem_ip();
        Ok(config)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (proxy_uri, warehouse, minio_endpoint) = self.fixture_endpoints()?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect(
            &user,
            port,
            context.remaining("connect vended write-outcome control session")?,
        )?;
        const CATALOG: &str = "vended_write_outcome_catalog";
        const DATABASE: &str = "vended_write_outcome_db";
        const TABLE: &str = "vended_write_outcome_data";
        control
            .query_drop(format!(
                "CREATE EXTERNAL CATALOG {CATALOG} PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"rest\",\"uri\"=\"{proxy_uri}\",\"iceberg.catalog.warehouse\"=\"{warehouse}\",\"aws.s3.endpoint\"=\"{minio_endpoint}\",\"aws.s3.region\"=\"us-east-1\",\"aws.s3.enable_path_style_access\"=\"true\",\"credential.object-store-metadata.consumer-role\"=\"frontend\",\"credential.object-store-metadata.mode\"=\"static\",\"credential.object-store-metadata.name\"=\"{VENDED_METADATA_CREDENTIAL_NAME}\",\"credential.object-store-metadata.generation\"=\"{VENDED_METADATA_CREDENTIAL_GENERATION}\",\"credential.object-store-data.consumer-role\"=\"backend\",\"credential.object-store-data.mode\"=\"vended\")"
            ))
            .context("create response-loss REST vended catalog")?;

        let (done_tx, done) = mpsc::sync_channel(1);
        let query_user = user.clone();
        let insert = format!("INSERT INTO {CATALOG}.{DATABASE}.{TABLE} SELECT 1 AS v");
        let writer = thread::spawn(move || {
            let mut connection = mysql_actor::connect(&query_user, port, Duration::from_secs(10))
                .context("connect vended write-outcome writer")?;
            let result = connection.query_drop(insert);
            done_tx
                .send(result.map_err(anyhow::Error::from))
                .context("publish vended write-outcome result")
        });

        context.action("inject catalog response loss after one durable vended table commit");
        let outcome = done
            .recv_timeout(context.remaining("await failed write after held response release")?)
            .context("vended write-outcome writer did not finish")?;
        writer
            .join()
            .map_err(|_| anyhow::anyhow!("vended write-outcome writer panicked"))??;
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
            audit.refreshes == 0 && audit.refresh_failures == 0,
            "response-loss write must not manufacture a refresh side path; audit={audit:?}"
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

/// Exercises the one attempt-local FE refresh owner against the production
/// 1FE+3BE TLS path. The source lease has a synthetic short expiry while its
/// real STS material remains valid, so the test can prove refresh semantics
/// without manufacturing invalid S3 credentials.
#[derive(Default)]
struct VendedRestRefreshPem {
    fixture: Mutex<Option<VendedRestSystemFixture>>,
}

impl Scenario for VendedRestRefreshPem {
    fn name(&self) -> &'static str {
        "connector/vended-credential-refresh"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        connector_reader_environment()
    }

    fn launch_config(&self, scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        let mut rest = IsolatedIcebergRestFixture::start(scenario_root)
            .context("start isolated REST and MinIO fixture for vended credential refresh")?;
        rest.provision_empty_table("vended_refresh_db", "vended_refresh_data")
            .context("provision isolated vended refresh source table")?;
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
            // The vended refresh policy clamps the soft margin at five
            // seconds. Eight seconds leaves a deterministic local window for
            // the 3-BE prepare/commit barrier without slowing the scenario.
            initial_ttl: Duration::from_secs(8),
            refresh_ttl: Duration::from_secs(60),
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
        configure_vended_metadata_access(&mut config, metadata_identity);
        config.native_trust_fixture = NativeTrustFixture::pem_ip();
        Ok(config)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
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
                "CREATE EXTERNAL CATALOG {CATALOG} PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"rest\",\"uri\"=\"{proxy_uri}\",\"iceberg.catalog.warehouse\"=\"{warehouse}\",\"aws.s3.endpoint\"=\"{minio_endpoint}\",\"aws.s3.region\"=\"us-east-1\",\"aws.s3.enable_path_style_access\"=\"true\",\"credential.object-store-metadata.consumer-role\"=\"frontend\",\"credential.object-store-metadata.mode\"=\"static\",\"credential.object-store-metadata.name\"=\"{VENDED_METADATA_CREDENTIAL_NAME}\",\"credential.object-store-metadata.generation\"=\"{VENDED_METADATA_CREDENTIAL_GENERATION}\",\"credential.object-store-data.consumer-role\"=\"backend\",\"credential.object-store-data.mode\"=\"vended\")"
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
        let refresh_baseline = self.vended_proxy_audit()?;

        context.action("start one long-running vended read on all three Backends");
        let target = start_connector_read(&user, port, CATALOG, DATABASE, TABLE)?;
        let connection_id = target
            .ready
            .recv_timeout(context.remaining("receive vended refresh read connection id")?)
            .context("vended refresh read terminated before publishing its connection id")?;
        wait_for_in_flight_reader_on_every_backend(
            context,
            CATALOG,
            "observe the short-TTL vended read on every Backend",
        )?;

        context.action("wait for the FE-owned vended credential refresh response");
        let _first_refresh = self.wait_for_refresh(context, refresh_baseline.refreshes)?;
        // A refresh response alone precedes the distributed prepare/commit
        // acknowledgement barrier. Keep the same statement alive after that
        // barrier's local control round, then require every BE still owns its
        // original reader before deliberately terminating the test query.
        thread::sleep(
            context
                .remaining("allow vended refresh prepare/commit to settle")?
                .min(Duration::from_secs(2)),
        );
        let settled_audit = self.vended_proxy_audit()?;
        let expected_table_loads = refresh_baseline.table_loads.saturating_add(1);
        let expected_refreshes = refresh_baseline.refreshes.saturating_add(1);
        let strict_observation_failure = (settled_audit.table_loads != expected_table_loads
            || settled_audit.refreshes != expected_refreshes
            || settled_audit.issued_key_ids.len() != 2)
        .then(|| {
            format!(
                "one vended attempt must observe one metadata response and execute one refresh; baseline={refresh_baseline:?}, expected_table_loads={expected_table_loads}, expected_refreshes={expected_refreshes}, observed={settled_audit:?}"
            )
        });
        wait_for_in_flight_reader_on_every_backend(
            context,
            CATALOG,
            "verify every Backend continues the same vended read after refresh",
        )?;
        if let Ok(result) = target.done.try_recv() {
            bail!(
                "vended read terminated after refresh instead of continuing across the 3-BE epoch commit: {result:?}"
            );
        }

        context.action(format!(
            "cancel the post-refresh vended read through KILL QUERY {connection_id}"
        ));
        control
            .query_drop(format!("KILL QUERY {connection_id}"))
            .context("cancel post-refresh vended reader")?;
        assert_cancelled_query(
            &target.done,
            context.remaining("await post-refresh vended read cancellation")?,
        )?;
        assert_target_connection_remains_usable(
            &target,
            context.remaining("verify post-refresh KILL QUERY connection behavior")?,
        )?;
        assert_idle_query(&mut control, connection_id)?;
        release_connector_read(&target)?;
        target
            .thread
            .join()
            .map_err(|_| anyhow::anyhow!("vended refresh reader thread panicked"))??;

        let reader_logs = wait_for_balanced_reader_lifecycle(
            context,
            "wait for post-refresh vended reader close after cancellation",
        )?;
        assert_no_reader_open_after_abort(&reader_logs)?;
        let audit = self.vended_proxy_audit()?;
        if audit != settled_audit {
            bail!(
                "vended refresh audit changed unexpectedly after cancellation; settled={settled_audit:?}, observed={audit:?}"
            );
        }
        await_resource_convergence(context, &baseline, "short-TTL vended credential refresh")?;
        if let Some(failure) = strict_observation_failure {
            bail!("{failure}");
        }
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

    fn wait_for_refresh(
        &self,
        context: &mut ScenarioContext,
        refresh_baseline: u64,
    ) -> Result<novarocks_cluster_harness::vended_rest_catalog::VendedRestCatalogAudit> {
        loop {
            let audit = self.vended_proxy_audit()?;
            if audit.refreshes > refresh_baseline {
                return Ok(audit);
            }
            let remaining = context.remaining("observe vended credential refresh")?;
            thread::sleep(remaining.min(Duration::from_millis(50)));
        }
    }
}

/// Proves a Frontend restart reconstructs its durable catalog projection
/// without invalidating catalog runtimes retained by the live Backends.
struct CatalogFeRestartCache;

impl Scenario for CatalogFeRestartCache {
    fn name(&self) -> &'static str {
        "connector/catalog-fe-restart-cache"
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        connector_reader_environment()
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(connector_launch_config())
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect(
            &user,
            port,
            context.remaining("connect FE restart cache control session")?,
        )?;

        const CATALOG: &str = "catalog_fe_restart_cache";
        const DATABASE: &str = "catalog_fe_restart_db";
        const TABLE: &str = "catalog_fe_restart_data";
        let warehouse = create_warehouse(context, "catalog-fe-restart-cache")?;
        create_catalog_table_and_data(&mut control, CATALOG, DATABASE, TABLE, &warehouse)?;

        context.action("warm the exact catalog runtime on every Backend");
        let warm_rows: Vec<i64> = control
            .query(format!("SELECT count(*) FROM {CATALOG}.{DATABASE}.{TABLE}"))
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
        let target = start_connector_read(&user, port, CATALOG, DATABASE, TABLE)?;
        target
            .ready
            .recv_timeout(context.remaining("receive in-flight reader connection id")?)
            .context("in-flight reader terminated before FE restart")?;
        wait_for_open_reader_on_every_backend(
            context,
            CATALOG,
            "observe in-flight readers before FE restart",
        )?;

        context.action("restart the Frontend and require its in-flight client query to fail");
        let deadline = context.deadline();
        context
            .handle()
            .restart_fe_until(deadline)
            .context("restart FE while catalog reader is in flight")?;
        assert_connection_killed_query(
            &target.done,
            context.remaining("await in-flight query failure after FE restart")?,
        )?;
        assert_target_connection_is_closed(
            &target,
            context.remaining("verify FE restart closed in-flight client connection")?,
        )?;
        release_connector_read(&target)?;
        target
            .thread
            .join()
            .map_err(|_| anyhow::anyhow!("FE restart reader thread panicked"))??;

        context.action("read through the restored Frontend catalog projection");
        let mut restored = mysql_actor::connect(
            &user,
            port,
            context.remaining("connect restored FE catalog control session")?,
        )?;
        let restored_rows: Vec<i64> = restored
            .query(format!("SELECT count(*) FROM {CATALOG}.{DATABASE}.{TABLE}"))
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
        let restored_versions = reader_catalog_versions_after(&restored_logs, &warm_logs, CATALOG)?;
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
    }
}

/// A static-file catalog source with two simultaneously active, role-local
/// object-store credential generations. The two fixtures deliberately accept
/// different key IDs, so a successful read plus their request logs proves that
/// each catalog stayed on its exact declared credential generation.
#[derive(Default)]
struct StaticCredentialGeneration {
    fixtures: Mutex<Option<StaticCredentialFixtures>>,
}

/// Exercises the cache key boundary on the same native topology as static
/// credentials, but is independently selectable by the M1 acceptance gate.
#[derive(Default)]
struct AccessDomainCacheIsolation {
    fixtures: Mutex<Option<StaticCredentialFixtures>>,
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
    fn name(&self) -> &'static str {
        "connector/static-credential-generation"
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        connector_reader_environment()
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        static_credential_launch_config(&self.fixtures, scenario_root)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect(
            &user,
            port,
            context.remaining("connect static credential control session")?,
        )?;

        run_static_credential_generation(context, &mut control, &self.fixtures)?;
        await_resource_convergence(context, &baseline, "static credential generation reads")?;
        Ok(())
    }
}

impl Scenario for AccessDomainCacheIsolation {
    fn name(&self) -> &'static str {
        "connector/access-domain-cache-isolation"
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        connector_reader_environment()
    }

    fn launch_config(&self, scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        access_domain_collision_launch_config(&self.fixtures, scenario_root)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect(
            &user,
            port,
            context.remaining("connect access-domain cache control session")?,
        )?;
        // Cache policy is a query-scoped FE-to-BE contract. The process-level
        // cache service configured by this scenario is intentionally inert
        // until the client opts this session into both reads and population.
        control
            .query_drop("SET enable_scan_datacache = true")
            .context("enable cache reads for access-domain isolation")?;
        control
            .query_drop("SET enable_populate_datacache = true")
            .context("enable cache population for access-domain isolation")?;

        run_access_domain_cache_isolation(context, &mut control, &self.fixtures)?;
        await_resource_convergence(context, &baseline, "access-domain cache isolation")?;
        Ok(())
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
        child_environment: connector_reader_environment(),
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
        child_environment: connector_reader_environment(),
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
    assert_static_catalog_sum(
        control,
        STATIC_BLUE_CATALOG,
        "static_blue_db",
        "static_blue_data",
        45_000_150_000,
    )?;
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
    assert_static_catalog_sum(
        control,
        STATIC_GREEN_CATALOG,
        "static_green_db",
        "static_green_data",
        135_000_150_000,
    )?;
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
    assert_static_catalog_sum(
        control,
        COLLISION_BLUE_CATALOG,
        COLLISION_DATABASE,
        COLLISION_TABLE,
        75_000_150_000,
    )?;
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
    assert_static_catalog_sum(
        control,
        COLLISION_GREEN_CATALOG,
        COLLISION_DATABASE,
        COLLISION_TABLE,
        75_000_450_000,
    )?;
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
    fn name(&self) -> &'static str {
        "connector/predicate-page-index-pruning"
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(connector_launch_config())
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect(
            &user,
            port,
            context.remaining("connect page-index control session")?,
        )?;
        let warehouse = create_warehouse(context, "predicate-page-index-pruning")?;
        const CATALOG: &str = "page_index_catalog";
        const DATABASE: &str = "page_index_db";
        const TABLE: &str = "page_index_data";
        const PREDICATE: &str = "v >= 199000";

        context.action("create three dense Iceberg files that each require page-level pruning");
        create_catalog_table_and_dense_data(&mut control, CATALOG, DATABASE, TABLE, &warehouse)?;

        let select = format!("SELECT count(*) FROM {CATALOG}.{DATABASE}.{TABLE} WHERE {PREDICATE}");
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
            bail!("page-index EXPLAIN ANALYZE has no typed connector metrics; profile={explain}");
        }
        assert_positive_profile_counter(&explain, "TypedConnectorPageSourcesOpened")?;
        Ok(())
    }
}

impl Scenario for CatalogVersionDrain {
    fn name(&self) -> &'static str {
        "connector/catalog-version-drain"
    }

    fn child_environment(&self) -> CrossProcessChildEnvironment {
        connector_reader_environment()
    }

    fn launch_config(&self, _scenario_root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        Ok(connector_launch_config())
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_baseline(context)?;
        let (user, port) = mysql_endpoint(context);
        let mut control = mysql_actor::connect(
            &user,
            port,
            context.remaining("connect catalog version drain control session")?,
        )?;

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
        let target = start_connector_read(
            &user,
            port,
            "connector_generation_catalog",
            "connector_generation_db",
            "connector_generation_data",
        )?;
        let connection_id = target
            .ready
            .recv_timeout(context.remaining("receive old-version connection id")?)
            .context("old-version connector read terminated before publishing its connection id")?;
        let old_logs = wait_for_in_flight_reader_on_every_backend(
            context,
            "connector_generation_catalog",
            "wait for every BE to open an old-version connector reader",
        )?;
        let old_versions = reader_catalog_versions(&old_logs, "connector_generation_catalog")?;
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

        context.action("read the replacement catalog version while the old version is leased");
        let replacement_rows: Vec<i64> = control
            .query(
                "SELECT count(*) FROM connector_generation_catalog.connector_generation_db.connector_generation_data",
            )
            .context("read table through replacement catalog version while old version is leased")?;
        if replacement_rows != [300_000] {
            bail!("replacement catalog version returned {replacement_rows:?}, expected [300000]");
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
            &target,
            context.remaining("verify old-generation KILL QUERY target remains usable")?,
        )?;
        assert_idle_query(&mut control, connection_id)?;
        release_connector_read(&target)?;
        target
            .thread
            .join()
            .map_err(|_| anyhow::anyhow!("old-generation connector read thread panicked"))??;

        wait_for_retired_catalog_version_close(
            context,
            "connector_generation_catalog",
            &old_versions,
        )?;

        await_resource_convergence(context, &baseline, "catalog version drain")?;
        Ok(())
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
    let mut environment = CrossProcessChildEnvironment::default();
    // This is a generic child launch input, not a connector-specific harness
    // API. The runner uses the marker only to establish the observable
    // in-flight reader/retirement boundary for these scenarios.
    environment.be.insert(
        "NOVAROCKS_SQL_TEST_EMIT_GRPC_FRAGMENT_MARKER".to_string(),
        "1".to_string(),
    );
    environment.be.insert(
        "NOVAROCKS_SQL_TEST_EMIT_CONNECTOR_READER_MARKER".to_string(),
        "1".to_string(),
    );
    environment.be.insert(
        "NOVAROCKS_SQL_TEST_EMIT_CATALOG_MATERIALIZATION_MARKER".to_string(),
        "1".to_string(),
    );
    environment
}

pub(super) fn connector_launch_config() -> ScenarioLaunchConfig {
    ScenarioLaunchConfig {
        child_environment: connector_reader_environment(),
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

fn assert_static_catalog_sum(
    control: &mut mysql::Conn,
    catalog: &str,
    database: &str,
    table: &str,
    expected: i64,
) -> Result<()> {
    let rows: Vec<i64> = control
        .query(format!("SELECT sum(v) FROM {catalog}.{database}.{table}"))
        .with_context(|| format!("sum {catalog}.{database}.{table} through typed connector"))?;
    if rows != [expected] {
        bail!(
            "static credential catalog {catalog}.{database}.{table} returned sum {rows:?}, expected [{expected}]"
        );
    }
    Ok(())
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

fn assert_catalog_read_summary(
    control: &mut mysql::Conn,
    catalog: &str,
    database: &str,
    table: &str,
    expected_count: i64,
    expected_sum: i64,
) -> Result<()> {
    let rows: Vec<(i64, i64)> = control
        .query(format!(
            "SELECT count(*), sum(v) FROM {catalog}.{database}.{table}"
        ))
        .context("read catalog table after distributed write")?;
    if rows != [(expected_count, expected_sum)] {
        bail!(
            "catalog read-write summary returned {rows:?}, expected [({expected_count}, {expected_sum})]"
        );
    }
    Ok(())
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

/// Which connection form a connector reader runs on.
///
/// The distinction is not cosmetic, and it is about what the *first* byte of
/// the response is. `SocketBounded` carries a ten-second socket read timeout,
/// which is harmless — and useful — for a reader that is already streaming
/// rows when it is cancelled: every packet resets the window, so the timeout
/// can only fire on a genuine stall.
///
/// A statement that is parked before it produces anything has no such
/// packets. Its first byte *is* the cancellation response, so the whole wait
/// sits inside one read and the socket timeout converts any server latency
/// past ten seconds into `EAGAIN` — on macOS, `Resource temporarily
/// unavailable (os error 35)`, which says nothing about whether the server
/// answered wrongly, late, or not at all. `ScenarioBounded` drops the read
/// timeout so the wait is bounded by an explicit assertion instead, and the
/// answer that arrives can be judged on its merits.
#[derive(Copy, Clone, Eq, PartialEq)]
enum ReaderConnection {
    SocketBounded,
    ScenarioBounded,
}

fn start_connector_read(
    user: &str,
    port: u16,
    catalog: &str,
    database: &str,
    table: &str,
) -> Result<ConnectorRead> {
    start_connector_read_on(
        user,
        port,
        catalog,
        database,
        table,
        ReaderConnection::SocketBounded,
    )
}

/// A connector reader whose statement is expected to be parked, producing
/// nothing, until another session cancels it.
fn start_held_connector_read(
    user: &str,
    port: u16,
    catalog: &str,
    database: &str,
    table: &str,
) -> Result<ConnectorRead> {
    start_connector_read_on(
        user,
        port,
        catalog,
        database,
        table,
        ReaderConnection::ScenarioBounded,
    )
}

fn start_connector_read_on(
    user: &str,
    port: u16,
    catalog: &str,
    database: &str,
    table: &str,
    connection_form: ReaderConnection,
) -> Result<ConnectorRead> {
    let (ready_tx, ready) = mpsc::sync_channel(1);
    let (done_tx, done) = mpsc::sync_channel(1);
    let (probe, probe_rx) = mpsc::sync_channel(1);
    let (probe_result_tx, probe_result) = mpsc::sync_channel(1);
    let (release, release_rx) = mpsc::channel();
    let user = user.to_string();
    // Keep every file reader in flight long enough to observe and cancel it,
    // while bounding each synchronous SLEEP evaluation to one second per
    // 4,096-row connector batch. Sleeping once for every input row would keep
    // the driver inside a single expression evaluation for hours after abort.
    let query = format!(
        "SELECT t.s FROM (SELECT sleep(1) AS s FROM {catalog}.{database}.{table} WHERE v % 4096 = 0) AS t CROSS JOIN TABLE(generate_series(1, 1000000000)) AS gs(x)"
    );
    let thread = thread::spawn(move || -> Result<()> {
        let mut connection = match connection_form {
            ReaderConnection::SocketBounded => {
                mysql_actor::connect(&user, port, Duration::from_secs(10))
            }
            ReaderConnection::ScenarioBounded => {
                mysql_actor::connect_for_cancellation(&user, port, Duration::from_secs(10))
            }
        }
        .context("connect connector reader MySQL client")?;
        ready_tx
            .send(connection.connection_id())
            .context("publish connector reader MySQL connection id")?;
        let result = connection.query::<i64, _>(query);
        done_tx
            .send(result)
            .context("publish connector reader MySQL result")?;
        probe_rx
            .recv()
            .context("receive connector reader connection probe")?;
        probe_result_tx
            .send(connection.query_first::<i64, _>("SELECT 1"))
            .context("publish connector reader connection probe result")?;
        release_rx
            .recv()
            .context("release connector reader MySQL session")?;
        Ok(())
    });
    Ok(ConnectorRead {
        ready,
        done,
        probe,
        probe_result,
        release,
        thread,
    })
}

fn start_idle_mysql_connection(user: &str, port: u16) -> Result<IdleMysqlConnection> {
    let (ready_tx, ready) = mpsc::sync_channel(1);
    let (probe, probe_rx) = mpsc::sync_channel(1);
    let (probe_result_tx, probe_result) = mpsc::sync_channel(1);
    let user = user.to_string();
    let thread = thread::spawn(move || -> Result<()> {
        let mut connection = mysql_actor::connect(&user, port, Duration::from_secs(10))
            .context("connect idle MySQL target")?;
        ready_tx
            .send(connection.connection_id())
            .context("publish idle MySQL target connection id")?;
        probe_rx
            .recv()
            .context("receive idle MySQL target connection probe")?;
        probe_result_tx
            .send(connection.query_first::<i64, _>("SELECT 1"))
            .context("publish idle MySQL target connection probe result")?;
        Ok(())
    });
    Ok(IdleMysqlConnection {
        ready,
        probe,
        probe_result,
        thread,
    })
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

fn wait_for_in_flight_reader_on_every_backend(
    context: &mut ScenarioContext,
    catalog: &str,
    operation: &str,
) -> Result<Vec<String>> {
    let marker = format!("{CONNECTOR_READER_OPEN} provider=iceberg instance={catalog}");
    wait_for_backend_logs(context, operation, |logs| {
        logs.iter().all(|log| {
            let (opens, closes) = reader_counts(log);
            log.contains(&marker) && opens > closes
        })
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
    loop {
        let logs = (0..context.handle().be_count())
            .map(|index| context.handle().be_current_log_contents(index))
            .collect::<Result<Vec<_>>>()
            .with_context(|| format!("read BE logs while waiting to {operation}"))?;
        if predicate(&logs) {
            return Ok(logs);
        }
        let remaining = context.remaining(operation)?;
        thread::sleep(remaining.min(Duration::from_millis(50)));
    }
}

fn backend_log_snapshots(context: &mut ScenarioContext) -> Result<Vec<String>> {
    (0..context.handle().be_count())
        .map(|index| context.handle().be_current_log_contents(index))
        .collect::<Result<Vec<_>>>()
        .context("read Backend log snapshots")
}

/// The per-backend text appended to every Backend log since `before`.
///
/// A truncated log is an error rather than an empty appendix: a rotation that
/// silently reset the window would make every absence assertion below pass.
fn appended_since<'a>(logs: &'a [String], before: &[String], moment: &str) -> Result<Vec<&'a str>> {
    logs.iter()
        .zip(before)
        .enumerate()
        .map(|(index, (log, previous))| {
            log.get(previous.len()..)
                .with_context(|| format!("BE[{index}] log was truncated while checking {moment}"))
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
/// The returned set is what every later assertion of the phase is keyed on,
/// and it is required to be non-empty: a phase whose subject never began an
/// install must fail here rather than pass every assertion vacuously.
fn wait_for_cold_catalog_install(
    context: &mut ScenarioContext,
    before: &[String],
    operation: &str,
) -> Result<BTreeSet<usize>> {
    let logs = wait_for_backend_logs(context, operation, |logs| {
        logs.iter().zip(before).any(|(log, previous)| {
            log.get(previous.len()..)
                .is_some_and(|added| added.contains(CATALOG_INSTALL_STARTED))
        })
    })?;
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
fn wait_for_context_abort_on(
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
                .is_some_and(|added| added.contains(TASK_CONTEXT_ABORT_APPLIED))
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

fn assert_appended_marker_on_backend(
    before: &[String],
    after: &[String],
    backend: usize,
    marker: &str,
) -> Result<()> {
    let moment = format!("{marker} on BE[{backend}]");
    let appended = appended_since(after, before, &moment)?;
    let added = appended
        .get(backend)
        .with_context(|| format!("missing BE[{backend}] log snapshot"))?;
    ensure!(
        added.contains(marker),
        "BE[{backend}] did not emit {marker}, so the fault it belongs to never fired"
    );
    Ok(())
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

fn release_catalog_install_hold(hold_file: &std::path::Path) -> Result<()> {
    std::fs::remove_file(hold_file)
        .with_context(|| format!("release catalog-install hold file {}", hold_file.display()))
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
        let Some(abort_offset) = log.find(TASK_CONTEXT_ABORT_APPLIED) else {
            continue;
        };
        aborted_backends += 1;
        if log[abort_offset..].contains(CONNECTOR_READER_OPEN) {
            bail!("BE[{index}] opened a connector reader after its query context was aborted");
        }
    }
    if aborted_backends == 0 {
        bail!(
            "no backend recorded {TASK_CONTEXT_ABORT_APPLIED} after KILL QUERY, so the abort \
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

fn reader_counts(log: &str) -> (usize, usize) {
    (
        log.match_indices(CONNECTOR_READER_OPEN).count(),
        log.match_indices(CONNECTOR_READER_CLOSE).count(),
    )
}

fn catalog_materialization_counts(logs: &[String]) -> Vec<usize> {
    logs.iter()
        .map(|log| {
            log.matches("NOVAROCKS_CATALOG_RUNTIME_MATERIALIZED")
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
