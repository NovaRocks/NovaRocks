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

use crate::actors::mysql as mysql_actor;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, bail, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::{
    NativeTrustFixture, QueryExecutionResourceSnapshot, QueryLifecycleStructuredSnapshot,
    RuntimeFilterParticipantTerminalDetails, RuntimeFilterParticipantTerminalTelemetry,
    RuntimeFilterParticipantTerminalTelemetryValue, RuntimeFilterTerminalRollup,
    RuntimeFilterTerminalTotals, RuntimeFilterTerminalTotalsTelemetry, ServerHandle,
};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

const REQUIRED_BACKENDS: usize = 3;
const ACK_DROP_TARGET_BACKEND: usize = 1;
const RESOURCE_POLL_INTERVAL: Duration = Duration::from_millis(50);
const RUNTIME_FILTER_SERVICE_RESOURCE: &str = "native_runtime_filter_services";
const NATIVE_QUERY_ACTIVE_FRAGMENTS_RESOURCE: &str = "native_query_active_fragments";

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(AcceptedAfterAckDrop),
        Box::new(CancelWithTerminalAckReplay),
    ]
}

/// Trust acceptance needs directional evidence from the same native 1FE+3BE
/// launcher: FE-to-BE lifecycle admission, BE-to-BE Runtime Filter transport,
/// and the BE-to-FE unary terminal fallback.  Keep this as one scenario per
/// transport profile so no alternate orchestration owns those assertions.
pub fn native_trust_directional_scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(NativeTrustDirectional {
            name: "native-trust/directional-plaintext-ip",
            fixture: NativeTrustFixture::plaintext_ip(),
        }),
        Box::new(NativeTrustDirectional {
            name: "native-trust/directional-automatic-dns",
            fixture: NativeTrustFixture::automatic_dns(),
        }),
        Box::new(NativeTrustDirectional {
            name: "native-trust/directional-pem-ip",
            fixture: NativeTrustFixture::pem_ip(),
        }),
    ]
}

struct AcceptedAfterAckDrop;

impl Scenario for AcceptedAfterAckDrop {
    fn name(&self) -> &'static str {
        "runtime-filter/accepted-after-ack-drop"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        run_accepted_after_ack_drop(context)
    }
}

struct CancelWithTerminalAckReplay;

impl Scenario for CancelWithTerminalAckReplay {
    fn name(&self) -> &'static str {
        "runtime-filter/cancel-terminal-ack-replay"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        run_cancel_with_terminal_ack_replay(context)
    }
}

struct NativeTrustDirectional {
    name: &'static str,
    fixture: NativeTrustFixture,
}

impl Scenario for NativeTrustDirectional {
    fn name(&self) -> &'static str {
        self.name
    }

    fn launch_config(&self, _scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        Ok(ScenarioLaunchConfig {
            native_trust_fixture: self.fixture.clone(),
            ..Default::default()
        })
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        run_accepted_after_ack_drop(context)?;
        run_cancel_with_terminal_ack_replay(context)?;
        context.action("proved FE-to-BE lifecycle admission, BE-to-BE Runtime Filter transport, and BE-to-FE terminal fallback using the same 1FE+3BE Native trust profile");
        Ok(())
    }
}

fn run_accepted_after_ack_drop(context: &mut ScenarioContext) -> Result<()> {
    require_three_backends(context)?;
    let mut control = connect_control(context, "connect retry scenario control session")?;
    let tables = create_runtime_filter_tables(context, &mut control, "retry")?;
    configure_partitioned_runtime_filter(&mut control)?;
    assert_partitioned_runtime_filter_plan(&mut control, &tables)?;
    let before_candidate_execution_id = latest_execution_id(context)?;

    let candidate_rows: Vec<i64> = control
        .query(runtime_filter_count_query(&tables))
        .context("execute native partitioned Runtime Filter candidate query")?;
    ensure!(
        candidate_rows == [30],
        "Runtime Filter candidate query returned unexpected rows: {candidate_rows:?}"
    );
    let candidate_snapshot =
        await_terminal_snapshot(context, before_candidate_execution_id.as_deref())?;
    assert_remote_contribution_candidate(&candidate_snapshot)?;
    context.action(
        "typed terminal oracle verified a multi-backend Runtime Filter contribution candidate",
    );
    let before_execution_id = candidate_snapshot.execution_id.clone();

    arm_target_backend(context, "runtime-filter-contribution-ack-drop")?;
    context.action(
        "armed an Accepted-after-ACK-drop Runtime Filter fault for the targeted native backend",
    );

    let rows: Vec<i64> = control
        .query(runtime_filter_count_query(&tables))
        .context("execute native partitioned Runtime Filter query with ACK-drop fault")?;
    ensure!(
        rows == [30],
        "Runtime Filter retry query returned unexpected rows: {rows:?}"
    );
    context.action("completed the partitioned Runtime Filter query with expected row count");

    let snapshot = await_terminal_snapshot(context, before_execution_id.as_deref())?;
    assert_retry_duplicate_conformance(&snapshot)?;
    context
        .action("typed terminal oracle proved a retried sender route and receiver-side duplicate");
    context
        .handle()
        .clear_query_lifecycle_faults()
        .context("clear Runtime Filter ACK-drop fault tokens")?;
    Ok(())
}

fn run_cancel_with_terminal_ack_replay(context: &mut ScenarioContext) -> Result<()> {
    require_three_backends(context)?;
    let mut control = connect_control(context, "connect cancellation scenario control session")?;
    let tables = create_runtime_filter_tables(context, &mut control, "cancel")?;
    configure_broadcast_runtime_filter(&mut control)?;
    let baseline = resource_snapshot(context)?;
    let before_execution_id = latest_execution_id(context)?;
    let fallback_before = context
        .handle()
        .backend_terminal_fallback_accepted(0)
        .context("read terminal fallback baseline for BE[0]")?;

    context
        .handle()
        .arm_terminal_ack_drop(0)
        .context("arm terminal ACK drop for the targeted backend")?;
    context.action("armed a terminal ACK-drop replay fault for the targeted native backend");

    let target = start_blocking_runtime_filter_query(
        context.mysql_user(),
        context.mysql_port(),
        context.remaining("connect blocking Runtime Filter query actor")?,
        runtime_filter_blocking_query(&tables),
    )?;
    let connection_id = target
        .ready
        .recv_timeout(context.remaining("receive Runtime Filter query connection id")?)
        .context("Runtime Filter query terminated before publishing its connection id")?;
    context.action("started an in-flight native Runtime Filter query through public MySQL");
    await_runtime_filter_activity(context, &baseline)?;
    context.action(
        "observed active Runtime Filter services, all participant ControlReady events, and an admitted native fragment through the typed resource oracle",
    );

    control
        .query_drop(format!("KILL QUERY {connection_id}"))
        .context("cancel Runtime Filter query through public MySQL KILL QUERY")?;
    assert_cancelled_query(
        &target.done,
        context.remaining("await Runtime Filter query cancellation")?,
    )?;
    target
        .thread
        .join()
        .map_err(|_| anyhow::anyhow!("Runtime Filter query actor panicked"))??;
    context.action("cancelled the active Runtime Filter query through public MySQL");

    let snapshot = await_terminal_snapshot(context, before_execution_id.as_deref())?;
    assert_cancelled_replay_conformance(&snapshot)?;
    context.action(
        "typed terminal oracle proved replay retained one complete, non-duplicated Runtime Filter view",
    );
    await_terminal_fallback_accepted(context, 0, fallback_before)?;
    context.action("observed BE[0] terminal fallback acceptance counter advance after the dropped terminal ACK");
    context
        .handle()
        .clear_query_lifecycle_faults()
        .context("clear terminal ACK-drop replay fault tokens")?;
    let deadline = context.deadline();
    context
        .handle()
        .await_query_execution_resource_convergence(&baseline, true, deadline)
        .context("await resource convergence after Runtime Filter cancellation")?;
    Ok(())
}

#[derive(Clone)]
struct RuntimeFilterTables {
    catalog: String,
    database: String,
    probe: String,
    build: String,
}

fn require_three_backends(context: &mut ScenarioContext) -> Result<()> {
    let actual = context.handle().be_count();
    ensure!(
        actual == REQUIRED_BACKENDS,
        "{} requires native 1FE+3BE, but the runner launched 1FE+{actual}BE",
        context.name()
    );
    context.action("verified native 1FE+3BE topology");
    Ok(())
}

fn connect_control(context: &ScenarioContext, operation: &str) -> Result<mysql::Conn> {
    mysql_actor::connect(
        context.mysql_user(),
        context.mysql_port(),
        context.remaining(operation)?,
    )
}

fn create_runtime_filter_tables(
    context: &mut ScenarioContext,
    control: &mut mysql::Conn,
    suffix: &str,
) -> Result<RuntimeFilterTables> {
    let warehouse = context
        .runtime_dir()
        .join("warehouses")
        .join(format!("runtime-filter-{suffix}"));
    std::fs::create_dir_all(&warehouse)
        .with_context(|| format!("create Runtime Filter warehouse {}", warehouse.display()))?;
    let tables = RuntimeFilterTables {
        catalog: format!("rf_system_{suffix}_catalog"),
        database: format!("rf_system_{suffix}_db"),
        probe: "probe".to_string(),
        build: "build".to_string(),
    };
    create_hadoop_catalog(control, &tables.catalog, &warehouse)?;
    control
        .query_drop(format!(
            "CREATE DATABASE {}.{}",
            tables.catalog, tables.database
        ))
        .context("create Runtime Filter scenario database")?;
    control
        .query_drop(format!(
            "CREATE TABLE {}.{}.{} (id INT NOT NULL, k INT)",
            tables.catalog, tables.database, tables.probe
        ))
        .context("create Runtime Filter probe table")?;
    control
        .query_drop(format!(
            "CREATE TABLE {}.{}.{} (k INT, flag VARCHAR(8))",
            tables.catalog, tables.database, tables.build
        ))
        .context("create Runtime Filter build table")?;
    control
        .query_drop(format!(
            "INSERT INTO {}.{}.{} SELECT generate_series, generate_series % 600 FROM TABLE(generate_series(1, 6000))",
            tables.catalog, tables.database, tables.probe
        ))
        .context("write Runtime Filter probe rows")?;
    for partition in 0..REQUIRED_BACKENDS {
        control
            .query_drop(format!(
                "INSERT INTO {}.{}.{} SELECT (generate_series * 3 + {partition}) % 600, CASE WHEN generate_series = 1 THEN 'Y' ELSE 'N' END FROM TABLE(generate_series(1, 200))",
                tables.catalog, tables.database, tables.build
            ))
            .with_context(|| format!("write Runtime Filter build partition {partition}"))?;
    }
    control
        .query_drop(format!(
            "ANALYZE TABLE {}.{}.{}",
            tables.catalog, tables.database, tables.probe
        ))
        .context("analyze Runtime Filter probe table")?;
    control
        .query_drop(format!(
            "ANALYZE TABLE {}.{}.{}",
            tables.catalog, tables.database, tables.build
        ))
        .context("analyze Runtime Filter build table")?;
    context.action("created and analyzed a local Iceberg Runtime Filter join fixture");
    Ok(tables)
}

fn create_hadoop_catalog(control: &mut mysql::Conn, catalog: &str, warehouse: &Path) -> Result<()> {
    let warehouse = warehouse.to_string_lossy().replace('"', "\\\"");
    control
        .query_drop(format!(
            "CREATE EXTERNAL CATALOG {catalog} PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"hadoop\",\"iceberg.catalog.warehouse\"=\"{warehouse}\")"
        ))
        .with_context(|| format!("create local Hadoop Iceberg catalog {catalog}"))
}

fn configure_broadcast_runtime_filter(control: &mut mysql::Conn) -> Result<()> {
    configure_runtime_filter(
        control,
        "SET cbo_broadcast_node_mem_budget_bytes = 10737418240",
    )
}

fn configure_partitioned_runtime_filter(control: &mut mysql::Conn) -> Result<()> {
    configure_runtime_filter(control, "SET cbo_broadcast_node_mem_budget_bytes = 0")
}

fn configure_runtime_filter(control: &mut mysql::Conn, join_distribution: &str) -> Result<()> {
    for setting in [
        "SET global_runtime_filter_build_max_size = 10737418240",
        "SET global_runtime_filter_probe_min_selectivity = 0.0",
        join_distribution,
        "SET disable_optimizer_rules = ''",
    ] {
        control
            .query_drop(setting)
            .with_context(|| format!("configure Runtime Filter setting {setting}"))?;
    }
    Ok(())
}

fn runtime_filter_count_query(tables: &RuntimeFilterTables) -> String {
    format!(
        "SELECT COUNT(*) FROM {catalog}.{database}.{probe} p JOIN {catalog}.{database}.{build} b ON p.k = b.k WHERE b.flag = 'Y'",
        catalog = tables.catalog,
        database = tables.database,
        probe = tables.probe,
        build = tables.build,
    )
}

fn assert_partitioned_runtime_filter_plan(
    control: &mut mysql::Conn,
    tables: &RuntimeFilterTables,
) -> Result<()> {
    let explain: Vec<String> = control
        .query(format!(
            "EXPLAIN VERBOSE {}",
            runtime_filter_count_query(tables)
        ))
        .context("explain native partitioned Runtime Filter candidate query")?;
    let explain = explain.join("\n");
    ensure!(
        explain.contains("HASH JOIN (PARTITIONED"),
        "Runtime Filter candidate did not select a partitioned hash join: {explain}"
    );
    ensure!(
        explain.contains("HASH_PARTITIONED (k)"),
        "Runtime Filter candidate did not expose a key-aligned hash partition: {explain}"
    );
    ensure!(
        explain.contains("producer binding") && explain.contains("consumer binding"),
        "Runtime Filter candidate did not expose producer and consumer bindings: {explain}"
    );
    Ok(())
}

fn runtime_filter_blocking_query(tables: &RuntimeFilterTables) -> String {
    format!(
        "SELECT sleep(completed_join.matched_rows - completed_join.matched_rows + 10) FROM (SELECT COUNT(*) AS matched_rows FROM ({join}) filtered) completed_join WHERE completed_join.matched_rows > 0",
        join = runtime_filter_counting_join(tables),
    )
}

fn runtime_filter_counting_join(tables: &RuntimeFilterTables) -> String {
    format!(
        "SELECT p.id FROM {catalog}.{database}.{probe} p JOIN {catalog}.{database}.{build} b ON p.k = b.k WHERE b.flag = 'Y'",
        catalog = tables.catalog,
        database = tables.database,
        probe = tables.probe,
        build = tables.build,
    )
}

fn latest_execution_id(context: &mut ScenarioContext) -> Result<Option<String>> {
    Ok(context
        .handle()
        .query_lifecycle_structured_snapshot()?
        .and_then(|snapshot| snapshot.execution_id))
}

fn arm_target_backend(context: &mut ScenarioContext, kind: &'static str) -> Result<()> {
    context
        .handle()
        .arm_query_lifecycle_fault(ACK_DROP_TARGET_BACKEND, kind)
        .with_context(|| format!("arm {kind} fault for BE[{ACK_DROP_TARGET_BACKEND}]"))
}

fn await_terminal_snapshot(
    context: &mut ScenarioContext,
    before_execution_id: Option<&str>,
) -> Result<QueryLifecycleStructuredSnapshot> {
    let deadline = context.deadline();
    context
        .handle()
        .await_query_lifecycle_structured_snapshot_after(before_execution_id, deadline)
        .context("await a new typed Runtime Filter terminal snapshot")
}

fn resource_snapshot(context: &mut ScenarioContext) -> Result<QueryExecutionResourceSnapshot> {
    context
        .handle()
        .query_execution_resource_snapshot()?
        .context("cross-process Runtime Filter scenario requires resource oracle")
}

fn await_terminal_fallback_accepted(
    context: &mut ScenarioContext,
    backend: usize,
    before: f64,
) -> Result<()> {
    loop {
        let current = context
            .handle()
            .backend_terminal_fallback_accepted(backend)
            .with_context(|| format!("read terminal fallback count from BE[{backend}]"))?;
        if current >= before + 1.0 {
            return Ok(());
        }
        let remaining = context.remaining("await BE-to-FE terminal fallback acceptance")?;
        thread::sleep(remaining.min(RESOURCE_POLL_INTERVAL));
    }
}

fn await_runtime_filter_activity(
    context: &mut ScenarioContext,
    baseline: &QueryExecutionResourceSnapshot,
) -> Result<()> {
    loop {
        let current = resource_snapshot(context)?;
        let runtime_filter_active =
            current
                .backends
                .iter()
                .zip(&baseline.backends)
                .any(|(current, baseline)| {
                    current
                        .resources
                        .get(RUNTIME_FILTER_SERVICE_RESOURCE)
                        .copied()
                        .unwrap_or_default()
                        > baseline
                            .resources
                            .get(RUNTIME_FILTER_SERVICE_RESOURCE)
                            .copied()
                            .unwrap_or_default()
                });
        let required_control_ready = u64::try_from(context.handle().be_count())?;
        let control_ready = current.frontend_control_ready
            >= baseline.frontend_control_ready + required_control_ready as f64;
        let native_fragment_active =
            current
                .backends
                .iter()
                .zip(&baseline.backends)
                .any(|(current, baseline)| {
                    current
                        .resources
                        .get(NATIVE_QUERY_ACTIVE_FRAGMENTS_RESOURCE)
                        .copied()
                        .unwrap_or_default()
                        > baseline
                            .resources
                            .get(NATIVE_QUERY_ACTIVE_FRAGMENTS_RESOURCE)
                            .copied()
                            .unwrap_or_default()
                });
        if runtime_filter_active && control_ready && native_fragment_active {
            return Ok(());
        }
        let remaining = context.remaining(
            "observe active Runtime Filter services, ControlReady, and native fragment admission",
        )?;
        thread::sleep(remaining.min(RESOURCE_POLL_INTERVAL));
    }
}

struct BlockingQuery {
    ready: mpsc::Receiver<u32>,
    done: mpsc::Receiver<std::result::Result<Vec<i64>, mysql::Error>>,
    thread: thread::JoinHandle<Result<()>>,
}

fn start_blocking_runtime_filter_query(
    user: &str,
    port: u16,
    connect_timeout: Duration,
    query: String,
) -> Result<BlockingQuery> {
    let (ready_tx, ready) = mpsc::sync_channel(1);
    let (done_tx, done) = mpsc::sync_channel(1);
    let user = user.to_string();
    let thread = thread::spawn(move || -> Result<()> {
        let mut connection = mysql_actor::connect_for_cancellation(&user, port, connect_timeout)
            .context("connect blocking Runtime Filter query actor")?;
        ready_tx
            .send(connection.connection_id())
            .context("publish blocking Runtime Filter query connection id")?;
        done_tx
            .send(connection.query::<i64, _>(query))
            .context("publish blocking Runtime Filter query result")?;
        Ok(())
    });
    Ok(BlockingQuery {
        ready,
        done,
        thread,
    })
}

fn assert_cancelled_query(
    done: &mpsc::Receiver<std::result::Result<Vec<i64>, mysql::Error>>,
    timeout: Duration,
) -> Result<()> {
    let result = done
        .recv_timeout(timeout)
        .context("Runtime Filter query did not terminate before scenario deadline")?;
    let error = match result {
        Ok(rows) => bail!("Runtime Filter query unexpectedly succeeded after KILL QUERY: {rows:?}"),
        Err(error) => error,
    };
    match error {
        mysql::Error::MySqlError(error) if error.code == 1317 => Ok(()),
        other => bail!("expected MySQL cancellation error 1317, received {other}"),
    }
}

fn assert_retry_duplicate_conformance(snapshot: &QueryLifecycleStructuredSnapshot) -> Result<()> {
    let (participants, totals) = available_rollup(snapshot)?;
    assert_complete_nonduplicated_rollup(participants, totals)?;
    let producer_duplicates = participants
        .iter()
        .flat_map(available_details)
        .flat_map(|details| details.producer_streams.iter())
        .map(|stream| stream.duplicate_count)
        .try_fold(0_u64, |total, value| {
            checked_add(total, value, "producer duplicate")
        })?;
    ensure!(
        producer_duplicates >= 1,
        "ACK-drop retry terminal facts contained no receiver producer duplicate: {snapshot:?}"
    );
    let retried_and_acked = participants
        .iter()
        .flat_map(available_details)
        .flat_map(|details| details.transport_routes.iter())
        .any(|route| route.sent_count >= 1 && route.retried_count >= 1 && route.acked_count >= 1);
    ensure!(
        retried_and_acked,
        "ACK-drop retry terminal facts contained no sender route with sent, retried, and ACKed counts: {snapshot:?}"
    );
    Ok(())
}

fn assert_remote_contribution_candidate(snapshot: &QueryLifecycleStructuredSnapshot) -> Result<()> {
    let (participants, totals) = available_rollup(snapshot)?;
    assert_complete_nonduplicated_rollup(participants, totals)?;
    let producer_backends = participants
        .iter()
        .filter(|participant| {
            available_details(participant)
                .is_some_and(|details| !details.producer_streams.is_empty())
        })
        .count();
    ensure!(
        producer_backends >= 2,
        "partitioned Runtime Filter candidate did not place producers on multiple backends: {snapshot:?}"
    );
    ensure!(
        totals.transport_routes.sent_count >= 1,
        "partitioned Runtime Filter candidate retained no Runtime Filter transport route: {snapshot:?}"
    );
    Ok(())
}

fn assert_cancelled_replay_conformance(snapshot: &QueryLifecycleStructuredSnapshot) -> Result<()> {
    let (participants, totals) = available_rollup(snapshot)?;
    assert_complete_nonduplicated_rollup(participants, totals)?;
    ensure!(
        totals.channels.count >= 1
            && totals.producer_streams.accepted_count >= 1
            && totals.transport_routes.sent_count >= 1,
        "cancelled query retained no Runtime Filter terminal contribution after terminal ACK replay: {snapshot:?}"
    );
    Ok(())
}

fn available_rollup(
    snapshot: &QueryLifecycleStructuredSnapshot,
) -> Result<(
    &[RuntimeFilterParticipantTerminalTelemetry],
    &RuntimeFilterTerminalTotals,
)> {
    let RuntimeFilterTerminalRollup::Available {
        participants,
        totals,
    } = &snapshot.runtime_filter
    else {
        bail!("Runtime Filter terminal rollup was unavailable: {snapshot:?}");
    };
    let RuntimeFilterTerminalTotalsTelemetry::Available(totals) = totals else {
        bail!("Runtime Filter terminal totals were unavailable: {snapshot:?}");
    };
    Ok((participants, totals))
}

fn assert_complete_nonduplicated_rollup(
    participants: &[RuntimeFilterParticipantTerminalTelemetry],
    totals: &RuntimeFilterTerminalTotals,
) -> Result<()> {
    ensure!(
        participants.len() == REQUIRED_BACKENDS,
        "expected one Runtime Filter terminal contribution per backend, got {participants:?}"
    );
    let identities = participants
        .iter()
        .map(|participant| participant.participant.clone())
        .collect::<BTreeSet<_>>();
    ensure!(
        identities.len() == participants.len(),
        "Runtime Filter rollup retained duplicate participant identities: {participants:?}"
    );
    for participant in participants {
        let RuntimeFilterParticipantTerminalTelemetryValue::Available(_) = participant.telemetry
        else {
            bail!(
                "terminal ACK replay must retain complete Runtime Filter telemetry, found unavailable participant {participant:?}"
            );
        };
    }
    assert_totals_match_details(participants, totals)
}

fn available_details(
    participant: &RuntimeFilterParticipantTerminalTelemetry,
) -> Option<&RuntimeFilterParticipantTerminalDetails> {
    match &participant.telemetry {
        RuntimeFilterParticipantTerminalTelemetryValue::Available(details) => Some(details),
        RuntimeFilterParticipantTerminalTelemetryValue::Unavailable(_) => None,
    }
}

fn assert_totals_match_details(
    participants: &[RuntimeFilterParticipantTerminalTelemetry],
    totals: &RuntimeFilterTerminalTotals,
) -> Result<()> {
    let details = participants
        .iter()
        .map(|participant| {
            available_details(participant).context("participant telemetry unexpectedly unavailable")
        })
        .collect::<Result<Vec<_>>>()?;
    let channel_count = checked_len(details.iter().flat_map(|detail| detail.channels.iter()))?;
    let producer_count = checked_len(
        details
            .iter()
            .flat_map(|detail| detail.producer_streams.iter()),
    )?;
    let route_count = checked_len(
        details
            .iter()
            .flat_map(|detail| detail.transport_routes.iter()),
    )?;
    let consumer_count = checked_len(details.iter().flat_map(|detail| detail.consumers.iter()))?;
    ensure!(
        totals.channels.count == channel_count,
        "channel total count drifted"
    );
    ensure!(
        totals.producer_streams.count == producer_count,
        "producer total count drifted"
    );
    ensure!(
        totals.transport_routes.count == route_count,
        "transport total count drifted"
    );
    ensure!(
        totals.consumers.count == consumer_count,
        "consumer total count drifted"
    );

    macro_rules! total_matches {
        ($expected:expr, $iter:expr, $name:literal) => {
            ensure!(
                $expected == checked_sum($iter, $name)?,
                "Runtime Filter total drifted for {}",
                $name
            )
        };
    }
    total_matches!(
        totals.channels.published_count,
        details
            .iter()
            .flat_map(|detail| detail.channels.iter())
            .map(|v| v.published_count),
        "channel published"
    );
    total_matches!(
        totals.channels.completed_count,
        details
            .iter()
            .flat_map(|detail| detail.channels.iter())
            .map(|v| v.completed_count),
        "channel completed"
    );
    total_matches!(
        totals.channels.unavailable_count,
        details
            .iter()
            .flat_map(|detail| detail.channels.iter())
            .map(|v| v.unavailable_count),
        "channel unavailable"
    );
    total_matches!(
        totals.channels.cancelled_count,
        details
            .iter()
            .flat_map(|detail| detail.channels.iter())
            .map(|v| v.cancelled_count),
        "channel cancelled"
    );
    total_matches!(
        totals.producer_streams.accepted_count,
        details
            .iter()
            .flat_map(|detail| detail.producer_streams.iter())
            .map(|v| v.accepted_count),
        "producer accepted"
    );
    total_matches!(
        totals.producer_streams.duplicate_count,
        details
            .iter()
            .flat_map(|detail| detail.producer_streams.iter())
            .map(|v| v.duplicate_count),
        "producer duplicate"
    );
    total_matches!(
        totals.producer_streams.stale_count,
        details
            .iter()
            .flat_map(|detail| detail.producer_streams.iter())
            .map(|v| v.stale_count),
        "producer stale"
    );
    total_matches!(
        totals.producer_streams.conflict_count,
        details
            .iter()
            .flat_map(|detail| detail.producer_streams.iter())
            .map(|v| v.conflict_count),
        "producer conflict"
    );
    total_matches!(
        totals.producer_streams.resource_limit_count,
        details
            .iter()
            .flat_map(|detail| detail.producer_streams.iter())
            .map(|v| v.resource_limit_count),
        "producer resource limit"
    );
    total_matches!(
        totals.transport_routes.sent_count,
        details
            .iter()
            .flat_map(|detail| detail.transport_routes.iter())
            .map(|v| v.sent_count),
        "transport sent"
    );
    total_matches!(
        totals.transport_routes.retried_count,
        details
            .iter()
            .flat_map(|detail| detail.transport_routes.iter())
            .map(|v| v.retried_count),
        "transport retried"
    );
    total_matches!(
        totals.transport_routes.acked_count,
        details
            .iter()
            .flat_map(|detail| detail.transport_routes.iter())
            .map(|v| v.acked_count),
        "transport ACKed"
    );
    total_matches!(
        totals.transport_routes.fail_open_count,
        details
            .iter()
            .flat_map(|detail| detail.transport_routes.iter())
            .map(|v| v.fail_open_count),
        "transport fail-open"
    );
    total_matches!(
        totals.transport_routes.sent_bytes,
        details
            .iter()
            .flat_map(|detail| detail.transport_routes.iter())
            .map(|v| v.sent_bytes),
        "transport sent bytes"
    );
    total_matches!(
        totals.transport_routes.retried_bytes,
        details
            .iter()
            .flat_map(|detail| detail.transport_routes.iter())
            .map(|v| v.retried_bytes),
        "transport retried bytes"
    );
    total_matches!(
        totals.transport_routes.acked_bytes,
        details
            .iter()
            .flat_map(|detail| detail.transport_routes.iter())
            .map(|v| v.acked_bytes),
        "transport ACKed bytes"
    );
    total_matches!(
        totals.transport_routes.fail_open_bytes,
        details
            .iter()
            .flat_map(|detail| detail.transport_routes.iter())
            .map(|v| v.fail_open_bytes),
        "transport fail-open bytes"
    );
    total_matches!(
        totals.consumers.row_evaluations,
        details
            .iter()
            .flat_map(|detail| detail.consumers.iter())
            .map(|v| v.row_evaluations),
        "consumer row evaluations"
    );
    total_matches!(
        totals.consumers.input_rows,
        details
            .iter()
            .flat_map(|detail| detail.consumers.iter())
            .map(|v| v.input_rows),
        "consumer input rows"
    );
    total_matches!(
        totals.consumers.output_rows,
        details
            .iter()
            .flat_map(|detail| detail.consumers.iter())
            .map(|v| v.output_rows),
        "consumer output rows"
    );
    total_matches!(
        totals.consumers.scan_evaluated,
        details
            .iter()
            .flat_map(|detail| detail.consumers.iter())
            .map(|v| v.scan_evaluated),
        "consumer scan evaluated"
    );
    total_matches!(
        totals.consumers.scan_kept,
        details
            .iter()
            .flat_map(|detail| detail.consumers.iter())
            .map(|v| v.scan_kept),
        "consumer scan kept"
    );
    total_matches!(
        totals.consumers.scan_pruned,
        details
            .iter()
            .flat_map(|detail| detail.consumers.iter())
            .map(|v| v.scan_pruned),
        "consumer scan pruned"
    );
    total_matches!(
        totals.consumers.scan_not_evaluated,
        details
            .iter()
            .flat_map(|detail| detail.consumers.iter())
            .map(|v| v.scan_not_evaluated),
        "consumer scan not evaluated"
    );
    total_matches!(
        totals
            .consumers
            .scan_not_evaluated_reasons
            .unit_facts_missing,
        details
            .iter()
            .flat_map(|detail| detail.consumers.iter())
            .map(|v| v.scan_not_evaluated_reasons.unit_facts_missing),
        "consumer scan not evaluated unit facts missing"
    );
    total_matches!(
        totals
            .consumers
            .scan_not_evaluated_reasons
            .column_facts_missing,
        details
            .iter()
            .flat_map(|detail| detail.consumers.iter())
            .map(|v| v.scan_not_evaluated_reasons.column_facts_missing),
        "consumer scan not evaluated column facts missing"
    );
    total_matches!(
        totals
            .consumers
            .scan_not_evaluated_reasons
            .data_type_unsupported,
        details
            .iter()
            .flat_map(|detail| detail.consumers.iter())
            .map(|v| v.scan_not_evaluated_reasons.data_type_unsupported),
        "consumer scan not evaluated data type unsupported"
    );
    total_matches!(
        totals
            .consumers
            .scan_not_evaluated_reasons
            .predicate_capability_unsupported,
        details
            .iter()
            .flat_map(|detail| detail.consumers.iter())
            .map(|v| v
                .scan_not_evaluated_reasons
                .predicate_capability_unsupported),
        "consumer scan not evaluated predicate capability unsupported"
    );
    total_matches!(
        totals
            .consumers
            .scan_not_evaluated_reasons
            .resource_unavailable,
        details
            .iter()
            .flat_map(|detail| detail.consumers.iter())
            .map(|v| v.scan_not_evaluated_reasons.resource_unavailable),
        "consumer scan not evaluated resource unavailable"
    );
    total_matches!(
        totals
            .consumers
            .scan_not_evaluated_reasons
            .snapshot_unavailable,
        details
            .iter()
            .flat_map(|detail| detail.consumers.iter())
            .map(|v| v.scan_not_evaluated_reasons.snapshot_unavailable),
        "consumer scan not evaluated snapshot unavailable"
    );
    total_matches!(
        totals
            .consumers
            .scan_not_evaluated_reasons
            .snapshot_timed_out,
        details
            .iter()
            .flat_map(|detail| detail.consumers.iter())
            .map(|v| v.scan_not_evaluated_reasons.snapshot_timed_out),
        "consumer scan not evaluated snapshot timed out"
    );
    total_matches!(
        totals
            .consumers
            .scan_not_evaluated_reasons
            .snapshot_not_published,
        details
            .iter()
            .flat_map(|detail| detail.consumers.iter())
            .map(|v| v.scan_not_evaluated_reasons.snapshot_not_published),
        "consumer scan not evaluated snapshot not published"
    );
    Ok(())
}

fn checked_len<T>(mut values: impl Iterator<Item = T>) -> Result<u64> {
    values.try_fold(0_u64, |count, _| checked_add(count, 1, "detail count"))
}

fn checked_sum(mut values: impl Iterator<Item = u64>, name: &str) -> Result<u64> {
    values.try_fold(0_u64, |total, value| checked_add(total, value, name))
}

fn checked_add(total: u64, value: u64, name: &str) -> Result<u64> {
    total
        .checked_add(value)
        .with_context(|| format!("Runtime Filter terminal assertion overflow for {name}"))
}
