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
const ACK_DROP_FAULT_KIND: &str = "runtime-filter-contribution-ack-drop";
const RESOURCE_POLL_INTERVAL: Duration = Duration::from_millis(50);
const NATIVE_QUERY_ACTIVE_FRAGMENTS_RESOURCE: &str = "native_query_active_fragments";

/// The task protocol's terminal acknowledgement loss: the split delivery that
/// closed a plan node was applied and only its answer is dropped.
const TASK_UPDATE_TERMINAL_ACK_DROP: &str = "task-update-terminal-ack-drop";
const TASK_UPDATE_TERMINAL_ACK_DROPPED_MARKER: &str = "NOVAROCKS_TASK_UPDATE_TERMINAL_ACK_DROPPED";
/// The replay verdict the resend must reach: the same splits recognised again,
/// not assigned twice.
const TASK_SPLIT_ASSIGNMENT_DUPLICATE_MARKER: &str = "NOVAROCKS_TASK_SPLIT_ASSIGNMENT_DUPLICATE";
/// Delivery of a client cancellation to a backend.
const TASK_CONTEXT_ABORT_APPLIED_MARKER: &str = "NOVAROCKS_TASK_CONTEXT_ABORT_APPLIED";
/// A task was admitted and is running on this backend.
const TASK_CREATE_APPLIED_MARKER: &str = "NOVAROCKS_TASK_CREATE_APPLIED";
/// The three dynamic-filter feedback perturbations, each emitted by the task
/// carrier at the publication it perturbed. They are asserted alongside every
/// client-visible outcome below: a fault whose claim did not move emits
/// nothing, and a case that only reads the client's side cannot tell that
/// apart from the fault firing.
const TASK_FEEDBACK_CONTRACT_DIGEST_CORRUPT_MARKER: &str =
    "NOVAROCKS_TASK_RUNTIME_FILTER_FEEDBACK_CONTRACT_DIGEST_CORRUPT";
const TASK_FEEDBACK_UNAVAILABLE_MARKER: &str = "NOVAROCKS_TASK_RUNTIME_FILTER_FEEDBACK_UNAVAILABLE";
const TASK_FEEDBACK_FOREIGN_ATTEMPT_MARKER: &str =
    "NOVAROCKS_TASK_RUNTIME_FILTER_FEEDBACK_FOREIGN_ATTEMPT";

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(AcceptedAfterAckDrop),
        Box::new(CancelWithTerminalAckReplay),
        Box::new(Ncp5FeedbackContractDigestCorrupt),
        Box::new(Ncp5FeedbackUnavailable),
        Box::new(Nid2ForeignAttemptRejection),
        Box::new(Ncp5PartitionedFeedbackPruning),
    ]
}

/// Trust acceptance needs directional evidence from the same native 1FE+3BE
/// launcher: FE-to-BE task admission and control, and BE-to-BE Runtime Filter
/// transport.  Keep this as one scenario per transport profile so no alternate
/// orchestration owns those assertions.
///
/// The BE-to-FE unary terminal fallback is no longer among them. That direction
/// belonged to the retired lifecycle's terminal report; on the task protocol
/// every operation is frontend-initiated, so a distributed SELECT opens no
/// BE-to-FE call for this profile to exercise. Asserting it here would have
/// meant asserting a direction the protocol does not use.
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

struct Ncp5PartitionedFeedbackPruning;

impl Scenario for Ncp5PartitionedFeedbackPruning {
    fn name(&self) -> &'static str {
        "runtime-filter/ncp-5-partitioned-feedback-pruning"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        run_ncp5_partitioned_feedback_pruning(context)
    }
}

struct Ncp5FeedbackContractDigestCorrupt;

impl Scenario for Ncp5FeedbackContractDigestCorrupt {
    fn name(&self) -> &'static str {
        "runtime-filter/ncp-5-feedback-contract-digest-corrupt"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        run_ncp5_feedback_contract_digest_corrupt(context)
    }
}

struct Ncp5FeedbackUnavailable;

impl Scenario for Ncp5FeedbackUnavailable {
    fn name(&self) -> &'static str {
        "runtime-filter/ncp-5-feedback-unavailable"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        run_ncp5_feedback_unavailable(context)
    }
}

struct Nid2ForeignAttemptRejection;

impl Scenario for Nid2ForeignAttemptRejection {
    fn name(&self) -> &'static str {
        "runtime-filter/nid-2-foreign-attempt-rejection"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        run_nid2_foreign_attempt_rejection(context)
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
        context.action("proved FE-to-BE task admission and control, and BE-to-BE Runtime Filter transport, using the same 1FE+3BE Native trust profile");
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
    let baseline = resource_snapshot(context)?;
    let created_baseline = be_marker_counts(context, TASK_CREATE_APPLIED_MARKER)?;
    arm_all_backends(context, ACK_DROP_FAULT_KIND)?;
    context.action(
        "armed one Accepted-after-ACK-drop Runtime Filter fault for every native participant",
    );

    let target = start_held_runtime_filter_count_query(
        context.mysql_user(),
        context.mysql_port(),
        context.remaining("connect held Runtime Filter retry query actor")?,
        runtime_filter_held_count_query(&tables),
    )?;
    target
        .ready
        .recv_timeout(context.remaining("receive held Runtime Filter retry connection id")?)
        .context(
            "held Runtime Filter retry query terminated before publishing its connection id",
        )?;
    context.action("started a held Runtime Filter retry query through public MySQL");
    await_ack_drop_while_query_active(context, &baseline, &created_baseline, &target.done)?;
    context.action(
        "observed the Accepted ACK-drop token consumed while the Runtime Filter query remained active",
    );

    let rows = target
        .done
        .recv_timeout(context.remaining("await held Runtime Filter retry query completion")?)
        .context("held Runtime Filter retry query did not complete before scenario deadline")?
        .context("execute native partitioned Runtime Filter query with ACK-drop fault")?;
    target
        .thread
        .join()
        .map_err(|_| anyhow::anyhow!("held Runtime Filter retry query actor panicked"))??;
    ensure!(
        rows == [(30, true)],
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

/// Cancellation of an in-flight Runtime Filter query whose terminal
/// acknowledgement was lost.
///
/// Two properties, and they need two different kinds of evidence.
///
/// The lost-acknowledgement half is the task protocol's own terminal ack:
/// `task-update-terminal-ack-drop` loses the answer to a terminal, non-empty
/// split delivery the backend already applied. The frontend resends the exact
/// request and the backend must recognise it as a replay rather than assign the
/// same splits twice, which is what the accepted/duplicate marker pair says.
/// Both are waited for *before* the cancellation, so the loss and its replay are
/// facts of this attempt rather than a race against the kill.
///
/// The cancellation half is delivery, not the client's error: a client error is
/// also what a timeout looks like. `NOVAROCKS_TASK_CONTEXT_ABORT_APPLIED` is the
/// only evidence that KILL QUERY reached the backends.
///
/// What is deliberately not asserted here: the retired protocol's complete
/// per-participant Runtime Filter terminal rollup. On the task path a backend's
/// sealed Runtime Filter observation rides `ReleaseQueryContextAck`, and release
/// requires an `Active` context — an aborted one never releases, so the
/// frontend refuses the rollup as incomplete rather than summing the backends
/// that did answer. `runtime-filter/accepted-after-ack-drop` keeps that rollup
/// assertion, on a query that completes. The BE-to-FE `terminal_fallback_accepted`
/// counter is likewise retired-protocol-only and is not replaced.
fn run_cancel_with_terminal_ack_replay(context: &mut ScenarioContext) -> Result<()> {
    require_three_backends(context)?;
    let mut control = connect_control(context, "connect cancellation scenario control session")?;
    let tables = create_runtime_filter_tables(context, &mut control, "cancel")?;
    configure_broadcast_runtime_filter(&mut control)?;
    let baseline = resource_snapshot(context)?;
    // Captured before the query under test starts, so every count below is this
    // query's own. The fixture setup above already ran distributed statements.
    let created_baseline = be_marker_counts(context, TASK_CREATE_APPLIED_MARKER)?;
    let ack_dropped_baseline = be_marker_counts(context, TASK_UPDATE_TERMINAL_ACK_DROPPED_MARKER)?;
    let duplicate_baseline = be_marker_counts(context, TASK_SPLIT_ASSIGNMENT_DUPLICATE_MARKER)?;
    let abort_baseline = be_marker_counts(context, TASK_CONTEXT_ABORT_APPLIED_MARKER)?;

    context
        .handle()
        .arm_query_lifecycle_fault(0, TASK_UPDATE_TERMINAL_ACK_DROP)
        .context("arm terminal task-update ACK drop for the targeted backend")?;
    context.action("armed a terminal task-update ACK-drop replay fault for BE[0]");

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
    await_runtime_filter_activity(context, &baseline, &created_baseline)?;
    context.action(
        "observed an admitted task on more than one backend and an active native fragment through the typed resource oracle",
    );
    await_total_advanced(
        context,
        TASK_UPDATE_TERMINAL_ACK_DROPPED_MARKER,
        &ack_dropped_baseline,
        1,
    )?;
    await_total_advanced(
        context,
        TASK_SPLIT_ASSIGNMENT_DUPLICATE_MARKER,
        &duplicate_baseline,
        1,
    )?;
    context.action(
        "observed the terminal task-update acknowledgement dropped and its exact resend recognised as a duplicate assignment",
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

    // Participants, not the cluster size: a query context exists only where a
    // task was placed, and which backends receive a table's splits is the
    // scheduler's business. What this case is about survives that -- the abort
    // was delivered to more than one backend rather than only observed by the
    // client.
    await_backends_advanced(
        context,
        TASK_CONTEXT_ABORT_APPLIED_MARKER,
        &abort_baseline,
        2,
    )?;
    context.action("observed the cancellation applied as an abort on more than one backend");
    context
        .handle()
        .clear_query_lifecycle_faults()
        .context("clear terminal task-update ACK-drop replay fault tokens")?;
    let deadline = context.deadline();
    context
        .handle()
        .await_query_execution_resource_convergence(&baseline, true, deadline)
        .context("await resource convergence after Runtime Filter cancellation")?;
    Ok(())
}

fn run_ncp5_partitioned_feedback_pruning(context: &mut ScenarioContext) -> Result<()> {
    require_three_backends(context)?;
    let mut control = connect_control(context, "connect NCP-5 profile control session")?;
    let tables = create_ncp5_pruning_tables(context, &mut control)?;
    configure_partitioned_runtime_filter(&mut control)?;
    assert_partitioned_runtime_filter_plan(&mut control, &tables)?;
    let query = runtime_filter_count_query(&tables);

    control
        .query_drop("SET enable_global_runtime_filter = false")
        .context("disable Runtime Filter for NCP-5 baseline")?;
    let disabled: Vec<i64> = control
        .query(&query)
        .context("execute NCP-5 Runtime Filter disabled baseline")?;

    control
        .query_drop("SET enable_global_runtime_filter = true")
        .context("enable Runtime Filter for NCP-5 candidate")?;
    let enabled: Vec<i64> = control
        .query(&query)
        .context("execute NCP-5 Runtime Filter candidate")?;
    ensure!(
        enabled == disabled && enabled == [1000],
        "NCP-5 Runtime Filter changed query correctness: disabled={disabled:?}, enabled={enabled:?}"
    );
    context.action("proved Runtime Filter on/off row fingerprint equivalence");

    // PARTITIONED coverage above is the aggregation/correctness gate. Its
    // blocking wait graph may deliberately reject an FE source wait, so use
    // the direct-source BROADCAST phase to prove the bounded-wait pruning
    // optimization without weakening cycle safety.
    configure_broadcast_runtime_filter(&mut control)?;
    let profile: Vec<String> = control
        .query(format!("EXPLAIN ANALYZE {query}"))
        .context("collect NCP-5 broadcast feedback profile")?;
    let profile = profile.join("\n");
    assert_positive_profile_counter(&profile, "ConnectorFilesConsidered")?;
    assert_positive_profile_counter(&profile, "ConnectorWholeFilesPruned")?;
    assert_positive_profile_counter(&profile, "ConnectorFileRowGroupsPruned")?;
    context.action(
        "EXPLAIN ANALYZE proved broadcast FE feedback reached Iceberg whole-file dynamic pruning",
    );
    Ok(())
}

/// A backend that publishes a domain under the wrong contract digest must fail
/// the query, not prune on it.
///
/// The fault now fires on the task carrier
/// (`TaskRuntimeFilterFeedbackEgress::try_publish`) rather than on the retired
/// control stream, so the assertion pairs the client's error with the
/// backend's own marker. Without the marker the `expect_err` would also be
/// satisfied by a query that failed for any unrelated reason -- including the
/// fault never firing at all and something else going wrong -- which is the
/// exact failure mode a moved claim has to rule out.
fn run_ncp5_feedback_contract_digest_corrupt(context: &mut ScenarioContext) -> Result<()> {
    require_three_backends(context)?;
    let mut control = connect_control(context, "connect NCP-5 corrupt-feedback control session")?;
    let tables = create_ncp5_pruning_tables(context, &mut control)?;
    configure_broadcast_runtime_filter(&mut control)?;
    let baseline = resource_snapshot(context)?;
    let corrupt_baseline = be_marker_counts(context, TASK_FEEDBACK_CONTRACT_DIGEST_CORRUPT_MARKER)?;
    for backend_index in 0..REQUIRED_BACKENDS {
        context
            .handle()
            .arm_query_lifecycle_fault(
                backend_index,
                "runtime-filter-feedback-contract-digest-corrupt",
            )
            .with_context(|| {
                format!("arm feedback contract-digest corruption fault for BE[{backend_index}]")
            })?;
    }
    context
        .action("armed an active-attempt Runtime Filter feedback contract-digest corruption fault");

    let error = control
        .query::<String, _>(format!(
            "EXPLAIN ANALYZE {}",
            runtime_filter_count_query(&tables)
        ))
        .expect_err("active-attempt feedback contract corruption must fail the query closed");
    await_total_advanced(
        context,
        TASK_FEEDBACK_CONTRACT_DIGEST_CORRUPT_MARKER,
        &corrupt_baseline,
        1,
    )?;
    context.action(format!(
        "public MySQL query failed closed after invalid Runtime Filter feedback: {error}"
    ));
    context
        .handle()
        .clear_query_lifecycle_faults()
        .context("clear feedback contract-digest corruption fault token")?;
    let deadline = context.deadline();
    context
        .handle()
        .await_query_execution_resource_convergence(&baseline, true, deadline)
        .context("await resource convergence after fail-closed feedback rejection")?;
    context.action("typed resource oracle confirmed fail-closed feedback cleanup converged");
    Ok(())
}

/// A channel with no usable domain must fail open: correct rows, no pruning.
///
/// `runtime-filter/ncp-5-partitioned-feedback-pruning` proves the same fixture
/// prunes a positive number of whole files with the loop intact, so the
/// zero here is this fault's effect rather than a fixture that never prunes.
/// The marker is still asserted, because a claim that failed to move would
/// leave that zero unreachable and this case would then pass only if the
/// pruning optimization had regressed on its own.
fn run_ncp5_feedback_unavailable(context: &mut ScenarioContext) -> Result<()> {
    require_three_backends(context)?;
    let mut control = connect_control(
        context,
        "connect NCP-5 unavailable-feedback control session",
    )?;
    let tables = create_ncp5_pruning_tables(context, &mut control)?;
    configure_broadcast_runtime_filter(&mut control)?;
    let unavailable_baseline = be_marker_counts(context, TASK_FEEDBACK_UNAVAILABLE_MARKER)?;
    for backend_index in 0..REQUIRED_BACKENDS {
        context
            .handle()
            .arm_query_lifecycle_fault(backend_index, "runtime-filter-feedback-unavailable")
            .with_context(|| format!("arm feedback unavailable fault for BE[{backend_index}]"))?;
    }
    let profile: Vec<String> = control
        .query(format!(
            "EXPLAIN ANALYZE {}",
            runtime_filter_count_query(&tables)
        ))
        .context("typed unavailable feedback must remain query-correct")?;
    let profile = profile.join("\n");
    assert_positive_profile_counter(&profile, "ConnectorFilesConsidered")?;
    ensure!(
        profile.contains("ConnectorWholeFilesPruned=0"),
        "typed unavailable must fail open for FE whole-file pruning; profile={profile}"
    );
    await_total_advanced(
        context,
        TASK_FEEDBACK_UNAVAILABLE_MARKER,
        &unavailable_baseline,
        1,
    )?;
    context
        .handle()
        .clear_query_lifecycle_faults()
        .context("clear feedback unavailable fault tokens")?;
    context.action(
        "typed unavailable feedback preserved correctness and disabled FE whole-file pruning",
    );
    Ok(())
}

/// Feedback that names an attempt other than the running one must be fenced
/// before it can become the pruning winner.
///
/// # Why the subject moved, and why it had to
///
/// This case was `nid-2-participant-ref-rejection`: the backend replaced the
/// publisher's `ParticipantAttemptRef` on the control stream and the frontend
/// refused it, leaving pruning conservatively failed open. The forgery it
/// performed does not exist on the task carrier.
/// `filter::RuntimeFilterEnvelope` carries no participant field the frontend
/// reads, and `DynamicFilterFeedbackPump` derives the publisher from the
/// `TaskIdentity` it fetched from -- so a backend cannot claim another
/// process's publisher slot, because the claim never travels. Keeping the old
/// injection here would have meant arming a fault with no producer and
/// asserting a zero that any query with no feedback also satisfies.
///
/// The surviving fence is the same one, reached by the identity a backend can
/// still misstate: the attempt. `admit_terminal` checks the deployment epoch
/// against the active attempt first, ahead of the winner, so a forged epoch
/// must fail the query closed with the winner untouched. That inverts the old
/// expression -- an `expect_err` where there used to be a zero-pruning profile
/// -- because on the task path an inadmissible publication is a
/// `TaskExecutionError::Schedule`, not a widened channel.
///
/// The publisher-authorization half of the old assertion is not lost, only
/// moved to where it can still be provoked:
/// `the_task_carrier_is_authorized_by_the_process_that_ran_the_producing_task`
/// in `novarocks/frontend/src/runtime_filter/feedback.rs` drives an undeclared
/// process straight into `admit_task_feedback` and asserts both the
/// "publisher is not authorized" refusal and an untouched winner.
fn run_nid2_foreign_attempt_rejection(context: &mut ScenarioContext) -> Result<()> {
    require_three_backends(context)?;
    let mut control = connect_control(context, "connect NID-2 foreign-attempt control session")?;
    let tables = create_ncp5_pruning_tables(context, &mut control)?;
    configure_broadcast_runtime_filter(&mut control)?;
    let baseline = resource_snapshot(context)?;
    let foreign_baseline = be_marker_counts(context, TASK_FEEDBACK_FOREIGN_ATTEMPT_MARKER)?;
    for backend_index in 0..REQUIRED_BACKENDS {
        context
            .handle()
            .arm_query_lifecycle_fault(backend_index, "runtime-filter-feedback-foreign-attempt")
            .with_context(|| {
                format!("arm foreign-attempt Runtime Filter feedback for BE[{backend_index}]")
            })?;
    }
    context.action("armed a foreign-attempt Runtime Filter feedback fault on every backend");

    let error = control
        .query::<String, _>(format!(
            "EXPLAIN ANALYZE {}",
            runtime_filter_count_query(&tables)
        ))
        .expect_err("feedback naming another attempt must fail the query closed");
    await_total_advanced(
        context,
        TASK_FEEDBACK_FOREIGN_ATTEMPT_MARKER,
        &foreign_baseline,
        1,
    )?;
    context
        .handle()
        .clear_query_lifecycle_faults()
        .context("clear foreign-attempt Runtime Filter feedback fault tokens")?;
    let deadline = context.deadline();
    context
        .handle()
        .await_query_execution_resource_convergence(&baseline, true, deadline)
        .context("await resource convergence after fail-closed foreign-attempt rejection")?;
    context.action(format!(
        "Runtime Filter feedback naming a foreign attempt was fenced ahead of the pruning winner and failed the query closed: {error}"
    ));
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

/// Write three disjoint `k` ranges as separate Iceberg commits. The build
/// side exposes only `k = 3`, so a completed feedback domain can retain the
/// first range and prove the other two whole files impossible from manifest
/// statistics before they are expanded into splits.
fn create_ncp5_pruning_tables(
    context: &mut ScenarioContext,
    control: &mut mysql::Conn,
) -> Result<RuntimeFilterTables> {
    let warehouse = context.runtime_dir().join("warehouses").join("ncp-5");
    std::fs::create_dir_all(&warehouse)
        .with_context(|| format!("create NCP-5 warehouse {}", warehouse.display()))?;
    let tables = RuntimeFilterTables {
        catalog: "rf_ncp5_catalog".to_string(),
        database: "rf_ncp5_db".to_string(),
        probe: "probe".to_string(),
        build: "build".to_string(),
    };
    create_hadoop_catalog(control, &tables.catalog, &warehouse)?;
    control
        .query_drop(format!(
            "CREATE DATABASE {}.{}",
            tables.catalog, tables.database
        ))
        .context("create NCP-5 database")?;
    control
        .query_drop(format!(
            "CREATE TABLE {}.{}.{} (id INT NOT NULL, k INT)",
            tables.catalog, tables.database, tables.probe
        ))
        .context("create NCP-5 probe table")?;
    control
        .query_drop(format!(
            "ALTER TABLE {}.{}.{} SET TBLPROPERTIES ('write.parquet.row-group-size-bytes'='1024')",
            tables.catalog, tables.database, tables.probe
        ))
        .context("set NCP-5 probe row-group size")?;
    control
        .query_drop(format!(
            "CREATE TABLE {}.{}.{} (k INT, flag VARCHAR(8))",
            tables.catalog, tables.database, tables.build
        ))
        .context("create NCP-5 build table")?;
    for range in 0..REQUIRED_BACKENDS {
        let lower = range * 200;
        control
            .query_drop(format!(
                "INSERT INTO {}.{}.{} SELECT {} + generate_series, {} + FLOOR((generate_series - 1) / 1000) FROM TABLE(generate_series(1, 200000))",
                tables.catalog,
                tables.database,
                tables.probe,
                range * 10_000,
                lower,
            ))
            .with_context(|| format!("write NCP-5 probe range {range}"))?;
    }
    control
        .query_drop(format!(
            "INSERT INTO {}.{}.{} VALUES (3, 'Y'), (3, 'N'), (4, 'N')",
            tables.catalog, tables.database, tables.build
        ))
        .context("write NCP-5 selective build rows")?;
    control
        .query_drop(format!(
            "ANALYZE TABLE {}.{}.{}",
            tables.catalog, tables.database, tables.probe
        ))
        .context("analyze NCP-5 probe table")?;
    control
        .query_drop(format!(
            "ANALYZE TABLE {}.{}.{}",
            tables.catalog, tables.database, tables.build
        ))
        .context("analyze NCP-5 build table")?;
    context.action("created three disjoint Iceberg probe ranges for NCP-5 whole-file pruning");
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
            "NCP-5 profile is missing {marker}; profile={profile}"
        ))?;
    ensure!(
        value > 0,
        "NCP-5 profile counter {name} must be positive; profile={profile}"
    );
    Ok(())
}

fn runtime_filter_blocking_query(tables: &RuntimeFilterTables) -> String {
    format!(
        "SELECT sleep(completed_join.matched_rows - completed_join.matched_rows + 10) FROM (SELECT COUNT(*) AS matched_rows FROM ({join}) filtered) completed_join WHERE completed_join.matched_rows > 0",
        join = runtime_filter_counting_join(tables),
    )
}

fn runtime_filter_held_count_query(tables: &RuntimeFilterTables) -> String {
    format!(
        "SELECT completed_join.matched_rows, sleep(completed_join.matched_rows - completed_join.matched_rows + 3) AS hold_complete FROM (SELECT COUNT(*) AS matched_rows FROM ({join}) filtered) completed_join WHERE completed_join.matched_rows > 0",
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

fn arm_all_backends(context: &mut ScenarioContext, kind: &'static str) -> Result<()> {
    for backend in 0..context.handle().be_count() {
        context
            .handle()
            .arm_query_lifecycle_fault(backend, kind)
            .with_context(|| format!("arm {kind} fault for BE[{backend}]"))?;
    }
    Ok(())
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

/// One marker's count in each backend's log.
///
/// Every assertion below compares against a captured vector of these rather
/// than against an absolute number. A scenario's own setup already runs
/// distributed statements, so an absolute count would credit this step with
/// evidence an earlier one produced.
fn be_marker_counts(context: &mut ScenarioContext, marker: &str) -> Result<Vec<usize>> {
    let be_count = context.handle().be_count();
    let mut counts = Vec::with_capacity(be_count);
    for index in 0..be_count {
        counts.push(
            context
                .handle()
                .be_log_count(index, marker)
                .with_context(|| format!("count {marker} in BE[{index}] log"))?,
        );
    }
    Ok(counts)
}

/// How many backends emitted this marker at least once more than the baseline.
///
/// The comparison is per backend on purpose. "How many distinct backends have
/// this marker" saturates at the backend count, so it cannot be compared as
/// `baseline + n`: a setup step that already touched every backend would leave
/// no headroom and the wait could never be satisfied.
fn backends_advanced(baseline: &[usize], current: &[usize]) -> usize {
    baseline
        .iter()
        .zip(current)
        .filter(|(baseline, current)| current > baseline)
        .count()
}

fn total_advanced(baseline: &[usize], current: &[usize]) -> usize {
    baseline
        .iter()
        .zip(current)
        .map(|(baseline, current)| current.saturating_sub(*baseline))
        .sum()
}

/// Waits until this step produced `required` more of one marker in total.
fn await_total_advanced(
    context: &mut ScenarioContext,
    marker: &str,
    baseline: &[usize],
    required: usize,
) -> Result<()> {
    loop {
        let current = be_marker_counts(context, marker)?;
        if total_advanced(baseline, &current) >= required {
            return Ok(());
        }
        let remaining = context.remaining(&format!("observe {required} more of {marker}"))?;
        thread::sleep(remaining.min(RESOURCE_POLL_INTERVAL));
    }
}

/// Waits until this step produced one marker on `required` more backends.
fn await_backends_advanced(
    context: &mut ScenarioContext,
    marker: &str,
    baseline: &[usize],
    required: usize,
) -> Result<()> {
    loop {
        let current = be_marker_counts(context, marker)?;
        if backends_advanced(baseline, &current) >= required {
            return Ok(());
        }
        let remaining = context.remaining(&format!(
            "observe {marker} newly emitted on {required} backends"
        ))?;
        thread::sleep(remaining.min(RESOURCE_POLL_INTERVAL));
    }
}

/// Waits until the query under test is really running distributed.
///
/// Both halves are needed. `NOVAROCKS_TASK_CREATE_APPLIED` newly emitted on
/// more than one backend is the task protocol's own statement that this attempt
/// was admitted past a single process; the active-fragment gauge is the
/// statement that work is in flight right now, which a log line -- never
/// retracted -- cannot make. The retired protocol's `control_ready` count and
/// its `native_runtime_filter_services` gauge are both published only by the
/// lifecycle chain, so neither can gate anything on the task path.
fn await_runtime_filter_activity(
    context: &mut ScenarioContext,
    baseline: &QueryExecutionResourceSnapshot,
    created_baseline: &[usize],
) -> Result<()> {
    loop {
        let current = resource_snapshot(context)?;
        if task_query_activity_observed(context, baseline, &current, created_baseline)? {
            return Ok(());
        }
        let remaining = context.remaining(
            "observe an admitted task on more than one backend and an active fragment",
        )?;
        thread::sleep(remaining.min(RESOURCE_POLL_INTERVAL));
    }
}

fn await_ack_drop_while_query_active<T: std::fmt::Debug>(
    context: &mut ScenarioContext,
    baseline: &QueryExecutionResourceSnapshot,
    created_baseline: &[usize],
    done: &mpsc::Receiver<std::result::Result<Vec<T>, mysql::Error>>,
) -> Result<()> {
    let fault_root = context.runtime_dir().join("query-lifecycle-faults");
    let backend_count = context.handle().be_count();
    let arms = (0..backend_count)
        .map(|backend| fault_root.join(format!("be-{backend}.{ACK_DROP_FAULT_KIND}.arm")))
        .collect::<Vec<_>>();
    let triggers = (0..backend_count)
        .map(|backend| fault_root.join(format!("be-{backend}.{ACK_DROP_FAULT_KIND}.trigger")))
        .collect::<Vec<_>>();
    let mut latest = None;
    loop {
        match done.try_recv() {
            Ok(result) => bail!(
                "Runtime Filter retry query reached terminal before the Accepted ACK-drop was observed: {result:?}; baseline={baseline:?}; latest={latest:?}"
            ),
            Err(mpsc::TryRecvError::Disconnected) => {
                bail!("Runtime Filter retry query actor disconnected before ACK-drop observation")
            }
            Err(mpsc::TryRecvError::Empty) => {}
        }
        let current = resource_snapshot(context)?;
        let armed_count = arms.iter().filter(|path| path.exists()).count();
        let trigger_count = triggers.iter().filter(|path| path.exists()).count();
        let query_active =
            task_query_activity_observed(context, baseline, &current, created_baseline)?;
        latest = Some((armed_count, trigger_count, query_active, current));
        if armed_count == 0 && trigger_count < backend_count && query_active {
            match done.try_recv() {
                Ok(result) => bail!(
                    "Runtime Filter retry query reached terminal during the Accepted ACK-drop observation: {result:?}; baseline={baseline:?}; latest={latest:?}"
                ),
                Err(mpsc::TryRecvError::Disconnected) => {
                    bail!(
                        "Runtime Filter retry query actor disconnected during ACK-drop observation"
                    )
                }
                Err(mpsc::TryRecvError::Empty) => return Ok(()),
            }
        }
        let remaining = context.remaining(
            "observe Accepted ACK-drop consumption while the Runtime Filter query remains active",
        )?;
        thread::sleep(remaining.min(RESOURCE_POLL_INTERVAL));
    }
}

fn task_query_activity_observed(
    context: &mut ScenarioContext,
    baseline: &QueryExecutionResourceSnapshot,
    current: &QueryExecutionResourceSnapshot,
    created_baseline: &[usize],
) -> Result<bool> {
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
    let created = be_marker_counts(context, TASK_CREATE_APPLIED_MARKER)?;
    Ok(native_fragment_active && backends_advanced(created_baseline, &created) >= 2)
}

struct BlockingQuery {
    ready: mpsc::Receiver<u32>,
    done: mpsc::Receiver<std::result::Result<Vec<i64>, mysql::Error>>,
    thread: thread::JoinHandle<Result<()>>,
}

struct HeldCountQuery {
    ready: mpsc::Receiver<u32>,
    done: mpsc::Receiver<std::result::Result<Vec<(i64, bool)>, mysql::Error>>,
    thread: thread::JoinHandle<Result<()>>,
}

fn start_held_runtime_filter_count_query(
    user: &str,
    port: u16,
    connect_timeout: Duration,
    query: String,
) -> Result<HeldCountQuery> {
    let (ready_tx, ready) = mpsc::sync_channel(1);
    let (done_tx, done) = mpsc::sync_channel(1);
    let user = user.to_string();
    let thread = thread::spawn(move || -> Result<()> {
        let mut connection = mysql_actor::connect_for_cancellation(&user, port, connect_timeout)
            .context("connect held Runtime Filter count query actor")?;
        configure_partitioned_runtime_filter(&mut connection)
            .context("configure held Runtime Filter count query actor")?;
        ready_tx
            .send(connection.connection_id())
            .context("publish held Runtime Filter count query connection id")?;
        done_tx
            .send(connection.query::<(i64, bool), _>(query))
            .context("publish held Runtime Filter count query result")?;
        Ok(())
    });
    Ok(HeldCountQuery {
        ready,
        done,
        thread,
    })
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
