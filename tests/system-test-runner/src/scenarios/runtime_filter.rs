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

use crate::actors::sql::{SqlSession, StatementFailure, StatementResult, TrinoValue};
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use anyhow::{Context, Result, bail, ensure};
use novarocks_cluster_harness::{
    NativeTrustFixture, QueryExecutionResourceSnapshot, QueryLifecycleStatementObservation,
    QueryLifecycleStructuredSnapshot, RuntimeFilterParticipantTerminalDetails,
    RuntimeFilterParticipantTerminalTelemetry, RuntimeFilterParticipantTerminalTelemetryValue,
    RuntimeFilterTerminalRollup, RuntimeFilterTerminalTotals, RuntimeFilterTerminalTotalsTelemetry,
    TestClusterOwner,
};
use novarocks_test_trino_client::{CancelHandle, Column, PendingStatement, TrinoError};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::thread;
use std::time::{Duration, Instant};

use crate::scenarios::native_sql_controls::{SelectedStatement, SqlIntentOwner, StatementSpec};

const REQUIRED_BACKENDS: usize = 3;
const ACK_DROP_FAULT_KIND: &str = "runtime-filter-contribution-ack-drop";
const ACK_DROP_RENDEZVOUS_FAULT_KIND: &str = "runtime-filter-contribution-ack-drop-rendezvous";
const RESOURCE_POLL_INTERVAL: Duration = Duration::from_millis(50);
const NATIVE_QUERY_ACTIVE_TASKS_RESOURCE: &str = "native_query_active_tasks";

/// The task protocol's terminal acknowledgement loss: the split delivery that
/// closed a plan node was applied and only its answer is dropped.
const TASK_UPDATE_TERMINAL_ACK_DROP: &str = "task-update-terminal-ack-drop";
const TASK_UPDATE_TERMINAL_ACK_DROPPED_MARKER: &str =
    novarocks_failpoint::observation_marker::TASK_UPDATE_TERMINAL_ACK_DROPPED;
/// The replay verdict the resend must reach: the same splits recognised again,
/// not assigned twice.
const TASK_SPLIT_ASSIGNMENT_DUPLICATE_MARKER: &str =
    novarocks_failpoint::observation_marker::TASK_SPLIT_ASSIGNMENT_DUPLICATE;
/// Delivery of a client cancellation to a backend.
const PARTICIPATION_ABORT_APPLIED_MARKER: &str =
    novarocks_failpoint::observation_marker::PARTICIPATION_ABORT_APPLIED;
/// A task was admitted and is running on this backend.
const TASK_CREATE_APPLIED_MARKER: &str =
    novarocks_failpoint::observation_marker::TASK_CREATE_APPLIED;
/// The three dynamic-filter feedback perturbations, each emitted by the task
/// carrier at the publication it perturbed. They are asserted alongside every
/// client-visible outcome below: a fault whose claim did not move emits
/// nothing, and a case that only reads the client's side cannot tell that
/// apart from the fault firing.
const TASK_FEEDBACK_CONTRACT_DIGEST_CORRUPT_MARKER: &str =
    novarocks_failpoint::observation_marker::TASK_RUNTIME_FILTER_FEEDBACK_CONTRACT_DIGEST_CORRUPT;
const TASK_FEEDBACK_UNAVAILABLE_MARKER: &str =
    novarocks_failpoint::observation_marker::TASK_RUNTIME_FILTER_FEEDBACK_UNAVAILABLE;
const TASK_FEEDBACK_FOREIGN_ATTEMPT_MARKER: &str =
    novarocks_failpoint::observation_marker::TASK_RUNTIME_FILTER_FEEDBACK_FOREIGN_ATTEMPT;

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(AcceptedAfterAckDrop::default()),
        Box::new(CancelWithTerminalAckReplay::default()),
        Box::new(Ncp5FeedbackContractDigestCorrupt::default()),
        Box::new(Ncp5FeedbackUnavailable::default()),
        Box::new(Nid2ForeignAttemptRejection::default()),
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
            intent: SqlIntentOwner::default(),
        }),
        Box::new(NativeTrustDirectional {
            name: "native-trust/directional-automatic-dns",
            fixture: NativeTrustFixture::automatic_dns(),
            intent: SqlIntentOwner::default(),
        }),
        Box::new(NativeTrustDirectional {
            name: "native-trust/directional-pem-ip",
            fixture: NativeTrustFixture::pem_ip(),
            intent: SqlIntentOwner::default(),
        }),
    ]
}

#[derive(Default)]
struct AcceptedAfterAckDrop {
    intent: SqlIntentOwner,
}

impl Scenario for AcceptedAfterAckDrop {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "runtime_filter",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "runtime-filter/accepted-after-ack-drop"
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        Ok(Some(self.intent.freeze(
            vec![retry_spec(), candidate_spec()],
            &[],
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        run_accepted_after_ack_drop(context, &self.intent.take(0)?, &self.intent.take(1)?)
    }
}

#[derive(Default)]
struct CancelWithTerminalAckReplay {
    intent: SqlIntentOwner,
}

impl Scenario for CancelWithTerminalAckReplay {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "runtime_filter",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "runtime-filter/cancel-terminal-ack-replay"
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        Ok(Some(self.intent.freeze(
            vec![cancel_spec()],
            &[],
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        run_cancel_with_terminal_ack_replay(context, &self.intent.take(0)?)
    }
}

struct Ncp5PartitionedFeedbackPruning;

impl Scenario for Ncp5PartitionedFeedbackPruning {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "runtime_filter",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "runtime-filter/ncp-5-partitioned-feedback-pruning"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        run_ncp5_partitioned_feedback_pruning(context)
    }
}

#[derive(Default)]
struct Ncp5FeedbackContractDigestCorrupt {
    intent: SqlIntentOwner,
}

impl Scenario for Ncp5FeedbackContractDigestCorrupt {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "runtime_filter",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "runtime-filter/ncp-5-feedback-contract-digest-corrupt"
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        Ok(Some(self.intent.freeze(
            vec![feedback_spec(
                "runtime-filter-feedback-contract-digest-corrupt",
            )],
            &[],
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        run_ncp5_feedback_contract_digest_corrupt(context, &self.intent.take(0)?)
    }
}

#[derive(Default)]
struct Ncp5FeedbackUnavailable {
    intent: SqlIntentOwner,
}

impl Scenario for Ncp5FeedbackUnavailable {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "runtime_filter",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "runtime-filter/ncp-5-feedback-unavailable"
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        Ok(Some(self.intent.freeze(
            vec![feedback_spec("runtime-filter-feedback-unavailable")],
            &[],
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        run_ncp5_feedback_unavailable(context, &self.intent.take(0)?)
    }
}

#[derive(Default)]
struct Nid2ForeignAttemptRejection {
    intent: SqlIntentOwner,
}

impl Scenario for Nid2ForeignAttemptRejection {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "runtime_filter",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "runtime-filter/nid-2-foreign-attempt-rejection"
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        Ok(Some(self.intent.freeze(
            vec![feedback_spec("runtime-filter-feedback-foreign-attempt")],
            &[],
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        run_nid2_foreign_attempt_rejection(context, &self.intent.take(0)?)
    }
}

struct NativeTrustDirectional {
    name: &'static str,
    fixture: NativeTrustFixture,
    intent: SqlIntentOwner,
}

impl Scenario for NativeTrustDirectional {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "runtime_filter",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        self.name
    }

    fn launch_config(&self, _scenario_root: &Path) -> Result<ScenarioLaunchConfig> {
        Ok(ScenarioLaunchConfig {
            native_trust_fixture: self.fixture.clone(),
            ..Default::default()
        })
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        Ok(Some(self.intent.freeze(
            vec![retry_spec(), cancel_spec(), candidate_spec()],
            &[],
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        run_accepted_after_ack_drop(context, &self.intent.take(0)?, &self.intent.take(2)?)?;
        run_cancel_with_terminal_ack_replay(context, &self.intent.take(1)?)?;
        context.action("proved FE-to-BE task admission and control, and BE-to-BE Runtime Filter transport, using the same 1FE+3BE Native trust profile");
        Ok(())
    }
}

fn run_accepted_after_ack_drop(
    context: &mut ScenarioContext,
    selected: &SelectedStatement,
    candidate: &SelectedStatement,
) -> Result<()> {
    require_three_backends(context)?;
    let mut control = connect_control(context, "connect retry scenario control session")?;
    let deadline = context.deadline();
    (|| {
        let control = &mut control;
        let tables = create_runtime_filter_tables(context, control, "retry")?;
        configure_partitioned_runtime_filter(control, deadline)?;
        assert_partitioned_runtime_filter_plan(control, &tables, deadline)?;
        let candidate_query = runtime_filter_count_query(&tables);
        let (pending, candidate_observation) =
            arm_selected(context, control, candidate, &candidate_query)?;
        let candidate_phase = (|| {
            let candidate_rows = integer_rows(rf_run_pending(control, pending, deadline)?)?;
            ensure!(
                candidate_rows == [30],
                "Runtime Filter candidate query returned unexpected rows: {candidate_rows:?}"
            );
            // The public result can precede the original Worker contributions.
            // Await this exact statement's terminal facts before reading them.
            let candidate_snapshot = context
                .handle()
                .await_query_lifecycle_statement_observation(
                    &candidate_observation,
                    true,
                    candidate.deadline,
                )?;
            assert_remote_contribution_candidate(&candidate_snapshot)?;
            context.action(
                "typed terminal oracle verified a multi-backend Runtime Filter contribution candidate",
            );
            Ok(())
        })();
        let candidate_drain = context
            .handle()
            .drain_query_lifecycle_statement_binding(&candidate_observation, candidate.deadline);
        merge_cleanup(candidate_phase, candidate_drain)?;
        let baseline = resource_snapshot(context)?;
        let created_baseline = be_marker_counts(context, TASK_CREATE_APPLIED_MARKER)?;

        context.action(
            "armed one Accepted-after-ACK-drop Runtime Filter fault for every native participant",
        );

        let query = runtime_filter_held_count_query(&tables);
        let mut target_connection =
            connect_control(context, "connect selected Runtime Filter session")?;
        configure_partitioned_runtime_filter(&mut target_connection, deadline)?;
        let (pending, observation) = arm_selected(context, &target_connection, selected, &query)?;
        let selected_phase = (|| {
            let target =
                start_held_runtime_filter_count_query(target_connection, pending, deadline)?;
            let phase = (|| -> Result<Vec<(i64, bool)>> {
                target
        .ready
        .recv_timeout(context.remaining("receive held Runtime Filter retry query id")?)
        .context(
            "held Runtime Filter retry query terminated before publishing its actual query id",
        )?;
                context.action("started a held Runtime Filter retry query through public Trino");
                await_ack_drop_while_query_active(
                    context,
                    ACK_DROP_RENDEZVOUS_FAULT_KIND,
                    selected,
                    &baseline,
                    &created_baseline,
                    &target.done,
                )?;
                context.action(
        "observed the Accepted ACK-drop token consumed while the Runtime Filter query remained active",
    );

                let rows = target
                    .done
                    .recv_timeout(
                        context.remaining("await held Runtime Filter retry query completion")?,
                    )
                    .context(
                        "held Runtime Filter retry query did not complete before scenario deadline",
                    )?
                    .context(
                        "execute native partitioned Runtime Filter query with ACK-drop fault",
                    )?;
                Ok(rows)
            })();
            let rows = settle_query(target.thread, &target.done, phase)?;
            ensure!(
                rows == [(30, true)],
                "Runtime Filter retry query returned unexpected rows: {rows:?}"
            );
            context
                .action("completed the partitioned Runtime Filter query with expected row count");

            let snapshot = context
                .handle()
                .await_query_lifecycle_statement_observation(
                    &observation,
                    true,
                    selected.deadline,
                )?;
            assert_retry_duplicate_conformance(&snapshot)?;
            context.action(
                "typed terminal oracle proved a retried sender route and receiver-side duplicate",
            );
            context
                .handle()
                .clear_query_lifecycle_faults()
                .context("clear Runtime Filter ACK-drop fault tokens")?;
            Ok(())
        })();
        let binding_drain = context
            .handle()
            .drain_query_lifecycle_statement_binding(&observation, selected.deadline);
        let fault_cleanup = if selected_phase.is_err() {
            context
                .handle()
                .clear_query_lifecycle_faults()
                .context("end original Runtime Filter fault bindings after failed phase")
        } else {
            Ok(())
        };
        merge_cleanup(selected_phase, merge_cleanup(binding_drain, fault_cleanup))
    })()
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
/// also what a timeout looks like. `NOVAROCKS_PARTICIPATION_ABORT_APPLIED` is the
/// only evidence that public cancellation reached the backends.
///
/// What is deliberately not asserted here: the retired protocol's complete
/// per-participant Runtime Filter terminal rollup. On the task path a backend's
/// sealed Runtime Filter observation rides `ReleaseParticipationAck`, and release
/// requires an `Active` context — an aborted one never releases, so the
/// frontend refuses the rollup as incomplete rather than summing the backends
/// that did answer. `runtime-filter/accepted-after-ack-drop` keeps that rollup
/// assertion, on a query that completes. The BE-to-FE `terminal_fallback_accepted`
/// counter is likewise retired-protocol-only and is not replaced.
fn run_cancel_with_terminal_ack_replay(
    context: &mut ScenarioContext,
    selected: &SelectedStatement,
) -> Result<()> {
    require_three_backends(context)?;
    let mut control = connect_control(context, "connect cancellation scenario control session")?;
    let deadline = context.deadline();
    (|| {
        let control = &mut control;
        let tables = create_runtime_filter_tables(context, control, "cancel")?;
        let baseline = resource_snapshot(context)?;
        // Captured before the query under test starts, so every count below is this
        // query's own. The fixture setup above already ran distributed statements.
        let created_baseline = be_marker_counts(context, TASK_CREATE_APPLIED_MARKER)?;
        let ack_dropped_baseline =
            be_marker_counts(context, TASK_UPDATE_TERMINAL_ACK_DROPPED_MARKER)?;
        let duplicate_baseline = be_marker_counts(context, TASK_SPLIT_ASSIGNMENT_DUPLICATE_MARKER)?;
        let abort_baseline = be_marker_counts(context, PARTICIPATION_ABORT_APPLIED_MARKER)?;

        let query = runtime_filter_blocking_query(&tables);
        let mut target_connection =
            connect_control(context, "connect selected Runtime Filter session")?;
        configure_partitioned_runtime_filter(&mut target_connection, deadline)?;
        let (pending, observation) = arm_selected(context, &target_connection, selected, &query)?;
        let selected_phase = (|| {
            let target = start_blocking_runtime_filter_query(target_connection, pending, deadline)?;
            let phase = (|| -> Result<()> {
                let _client_query_id = target
                    .ready
                    .recv_timeout(context.remaining("receive Runtime Filter query id")?)
                    .context(
                        "Runtime Filter query terminated before publishing its actual query id",
                    )?;
                context.action(
                    "started an in-flight native Runtime Filter query through public Trino",
                );
                await_ack_drop_while_query_active(
                    context,
                    ACK_DROP_FAULT_KIND,
                    selected,
                    &baseline,
                    &created_baseline,
                    &target.done,
                )?;
                context.action(
        "observed a receiver-Accepted Runtime Filter contribution while the EES task query remained active",
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

                target
                    .thread
                    .delete()
                    .context("cancel Runtime Filter query through same Trino continuation")?;
                assert_cancelled_query(
                    &target.done,
                    context.remaining("await Runtime Filter query cancellation")?,
                )?;
                Ok(())
            })();
            settle_query(target.thread, &target.done, phase)?;
            context.action("cancelled the active Runtime Filter query through public Trino");

            // A query context exists only where the scheduler placed work. This query
            // has completed its distributed join before its root sleep, so its active
            // participant set is not the three-node cluster. One abort marker is the
            // exact proof that public cancellation reached the remaining live backend; asking
            // for two would assert a placement the task graph no longer has.
            await_backends_advanced(
                context,
                PARTICIPATION_ABORT_APPLIED_MARKER,
                &abort_baseline,
                1,
            )?;
            context.action("observed the cancellation applied as an abort on a live backend");
            context
                .handle()
                .clear_query_lifecycle_faults()
                .context("clear terminal task-update ACK-drop replay fault tokens")?;
            let deadline = context.deadline();
            context
                .handle()
                .await_query_execution_resource_convergence(&baseline, deadline)
                .context("await resource convergence after Runtime Filter cancellation")?;
            Ok(())
        })();
        let binding_drain = context
            .handle()
            .drain_query_lifecycle_statement_binding(&observation, selected.deadline);
        let fault_cleanup = if selected_phase.is_err() {
            context
                .handle()
                .clear_query_lifecycle_faults()
                .context("end original Runtime Filter fault bindings after failed phase")
        } else {
            Ok(())
        };
        merge_cleanup(selected_phase, merge_cleanup(binding_drain, fault_cleanup))
    })()
}

fn run_ncp5_partitioned_feedback_pruning(context: &mut ScenarioContext) -> Result<()> {
    require_three_backends(context)?;
    let mut control = connect_control(context, "connect NCP-5 profile control session")?;
    let deadline = context.deadline();
    (|| {
        let control = &mut control;
        let tables = create_ncp5_pruning_tables(context, control)?;
        configure_partitioned_runtime_filter(control, deadline)?;
        assert_partitioned_runtime_filter_plan(control, &tables, deadline)?;
        let query = runtime_filter_count_query(&tables);

        rf_execute(
            control,
            "SET enable_global_runtime_filter = false",
            deadline,
        )
        .context("disable Runtime Filter for NCP-5 baseline")?;
        let disabled: Vec<i64> = rf_execute(control, &query, deadline)
            .and_then(integer_rows)
            .context("execute NCP-5 Runtime Filter disabled baseline")?;

        rf_execute(control, "SET enable_global_runtime_filter = true", deadline)
            .context("enable Runtime Filter for NCP-5 candidate")?;
        let enabled: Vec<i64> = rf_execute(control, &query, deadline)
            .and_then(integer_rows)
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
        configure_broadcast_runtime_filter(control, deadline)?;
        let profile: Vec<String> =
            rf_execute(control, format!("EXPLAIN ANALYZE {query}"), deadline)
                .and_then(text_rows)
                .context("collect NCP-5 broadcast feedback profile")?;
        let profile = profile.join("\n");
        assert_positive_profile_counter(&profile, "ConnectorFilesConsidered")?;
        assert_positive_profile_counter(&profile, "ConnectorWholeFilesPruned")?;
        assert_positive_profile_counter(&profile, "ConnectorFileRowGroupsPruned")?;
        context.action(
        "EXPLAIN ANALYZE proved broadcast FE feedback reached Iceberg whole-file dynamic pruning",
    );
        Ok(())
    })()
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
fn run_ncp5_feedback_contract_digest_corrupt(
    context: &mut ScenarioContext,
    selected: &SelectedStatement,
) -> Result<()> {
    require_three_backends(context)?;
    let mut control = connect_control(context, "connect NCP-5 corrupt-feedback control session")?;
    let deadline = context.deadline();
    (|| {
        let control = &mut control;
        let tables = create_ncp5_pruning_tables(context, control)?;
        configure_broadcast_runtime_filter(control, deadline)?;
        let baseline = resource_snapshot(context)?;
        let corrupt_baseline =
            be_marker_counts(context, TASK_FEEDBACK_CONTRACT_DIGEST_CORRUPT_MARKER)?;
        let query = format!("EXPLAIN ANALYZE {}", runtime_filter_count_query(&tables));
        let (pending, observation) = arm_selected(context, control, selected, &query)?;
        let selected_phase = (|| {
            context.action(
                "armed an active-attempt Runtime Filter feedback contract-digest corruption fault",
            );

            let error = rf_run_pending(control, pending, deadline)
                .and_then(text_rows)
                .err()
                .context(
                    "active-attempt feedback contract corruption must fail the query closed",
                )?;
            public_sql_error(&error)?;
            await_total_advanced(
                context,
                TASK_FEEDBACK_CONTRACT_DIGEST_CORRUPT_MARKER,
                &corrupt_baseline,
                1,
            )?;
            context.action(format!(
                "public Trino query failed closed after invalid Runtime Filter feedback: {error}"
            ));
            context
                .handle()
                .clear_query_lifecycle_faults()
                .context("clear feedback contract-digest corruption fault token")?;
            let deadline = context.deadline();
            context
                .handle()
                .await_query_execution_resource_convergence(&baseline, deadline)
                .context("await resource convergence after fail-closed feedback rejection")?;
            context
                .action("typed resource oracle confirmed fail-closed feedback cleanup converged");
            Ok(())
        })();
        let binding_drain = context
            .handle()
            .drain_query_lifecycle_statement_binding(&observation, selected.deadline);
        let fault_cleanup = if selected_phase.is_err() {
            context
                .handle()
                .clear_query_lifecycle_faults()
                .context("end original Runtime Filter fault bindings after failed phase")
        } else {
            Ok(())
        };
        merge_cleanup(selected_phase, merge_cleanup(binding_drain, fault_cleanup))
    })()
}

/// A channel with no usable domain must fail open: correct rows, no pruning.
///
/// `runtime-filter/ncp-5-partitioned-feedback-pruning` proves the same fixture
/// prunes a positive number of whole files with the loop intact, so the
/// zero here is this fault's effect rather than a fixture that never prunes.
/// The marker is still asserted, because a claim that failed to move would
/// leave that zero unreachable and this case would then pass only if the
/// pruning optimization had regressed on its own.
fn run_ncp5_feedback_unavailable(
    context: &mut ScenarioContext,
    selected: &SelectedStatement,
) -> Result<()> {
    require_three_backends(context)?;
    let mut control = connect_control(
        context,
        "connect NCP-5 unavailable-feedback control session",
    )?;
    let deadline = context.deadline();
    (|| {
        let control = &mut control;
        let tables = create_ncp5_pruning_tables(context, control)?;
        configure_broadcast_runtime_filter(control, deadline)?;
        let unavailable_baseline = be_marker_counts(context, TASK_FEEDBACK_UNAVAILABLE_MARKER)?;
        let query = format!("EXPLAIN ANALYZE {}", runtime_filter_count_query(&tables));
        let (pending, observation) = arm_selected(context, control, selected, &query)?;
        let selected_phase = (|| {
            let profile: Vec<String> = rf_run_pending(control, pending, deadline)
                .and_then(text_rows)
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
        })();
        let binding_drain = context
            .handle()
            .drain_query_lifecycle_statement_binding(&observation, selected.deadline);
        let fault_cleanup = if selected_phase.is_err() {
            context
                .handle()
                .clear_query_lifecycle_faults()
                .context("end original Runtime Filter fault bindings after failed phase")
        } else {
            Ok(())
        };
        merge_cleanup(selected_phase, merge_cleanup(binding_drain, fault_cleanup))
    })()
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
/// in `novarocks/frontend-application/src/runtime_filter/feedback.rs` drives an undeclared
/// process straight into `admit_task_feedback` and asserts both the
/// "publisher is not authorized" refusal and an untouched winner.
fn run_nid2_foreign_attempt_rejection(
    context: &mut ScenarioContext,
    selected: &SelectedStatement,
) -> Result<()> {
    require_three_backends(context)?;
    let mut control = connect_control(context, "connect NID-2 foreign-attempt control session")?;
    let deadline = context.deadline();
    (|| {
        let control = &mut control;
        let tables = create_ncp5_pruning_tables(context, control)?;
        configure_broadcast_runtime_filter(control, deadline)?;
        let baseline = resource_snapshot(context)?;
        let foreign_baseline = be_marker_counts(context, TASK_FEEDBACK_FOREIGN_ATTEMPT_MARKER)?;
        let query = format!("EXPLAIN ANALYZE {}", runtime_filter_count_query(&tables));
        let (pending, observation) = arm_selected(context, control, selected, &query)?;
        let selected_phase = (|| {
            context
                .action("armed a foreign-attempt Runtime Filter feedback fault on every backend");

            let error = rf_run_pending(control, pending, deadline)
                .and_then(text_rows)
                .err()
                .context("feedback naming another attempt must fail the query closed")?;
            public_sql_error(&error)?;
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
                .await_query_execution_resource_convergence(&baseline, deadline)
                .context(
                    "await resource convergence after fail-closed foreign-attempt rejection",
                )?;
            context.action(format!(
        "Runtime Filter feedback naming a foreign attempt was fenced ahead of the pruning winner and failed the query closed: {error}"
    ));
            Ok(())
        })();
        let binding_drain = context
            .handle()
            .drain_query_lifecycle_statement_binding(&observation, selected.deadline);
        let fault_cleanup = if selected_phase.is_err() {
            context
                .handle()
                .clear_query_lifecycle_faults()
                .context("end original Runtime Filter fault bindings after failed phase")
        } else {
            Ok(())
        };
        merge_cleanup(selected_phase, merge_cleanup(binding_drain, fault_cleanup))
    })()
}

#[derive(Clone)]
struct RuntimeFilterTables {
    catalog: String,
    database: String,
    probe: String,
    build: String,
}

fn runtime_filter_tables(suffix: &str) -> RuntimeFilterTables {
    RuntimeFilterTables {
        catalog: format!("rf_system_{suffix}_catalog"),
        database: format!("rf_system_{suffix}_db"),
        probe: "probe".into(),
        build: "build".into(),
    }
}
fn ncp5_tables() -> RuntimeFilterTables {
    RuntimeFilterTables {
        catalog: "rf_ncp5_catalog".into(),
        database: "rf_ncp5_db".into(),
        probe: "probe".into(),
        build: "build".into(),
    }
}
fn candidate_spec() -> StatementSpec {
    StatementSpec {
        sql: runtime_filter_count_query(&runtime_filter_tables("retry")),
        faults: vec![],
        observations: vec![],
    }
}
fn retry_spec() -> StatementSpec {
    StatementSpec {
        sql: runtime_filter_held_count_query(&runtime_filter_tables("retry")),
        faults: (0..3)
            .map(|i| (i, ACK_DROP_RENDEZVOUS_FAULT_KIND))
            .collect(),
        observations: (0..3)
            .map(|i| {
                (
                    i,
                    novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol
                        .as_str(),
                    None,
                )
            })
            .collect(),
    }
}
fn cancel_spec() -> StatementSpec {
    let mut faults = (0..3).map(|i| (i, ACK_DROP_FAULT_KIND)).collect::<Vec<_>>();
    faults.push((0, TASK_UPDATE_TERMINAL_ACK_DROP));
    StatementSpec {
        sql: runtime_filter_blocking_query(&runtime_filter_tables("cancel")),
        faults,
        observations: (0..3)
            .flat_map(|i| {
                [
                    (
                        i,
                        novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol
                            .as_str(),
                        None,
                    ),
                    (
                        i,
                        novarocks_failpoint::observation_marker::ObservationAction::ReaderMarker
                            .as_str(),
                        None,
                    ),
                ]
            })
            .collect(),
    }
}
fn feedback_spec(kind: &'static str) -> StatementSpec {
    StatementSpec {
        sql: format!(
            "EXPLAIN ANALYZE {}",
            runtime_filter_count_query(&ncp5_tables())
        ),
        faults: (0..3).map(|i| (i, kind)).collect(),
        observations: vec![],
    }
}

fn rf_execute(
    control: &mut SqlSession,
    query: impl AsRef<str>,
    deadline: Instant,
) -> Result<StatementResult> {
    let pending = control.submit(query)?;
    rf_run_pending(control, pending, deadline)
}
fn rf_run_pending(
    control: &mut SqlSession,
    pending: PendingStatement,
    deadline: Instant,
) -> Result<StatementResult> {
    let cancel = pending.cancel_handle();
    let response = control.run_pending(pending);
    let cleanup = if matches!(&response, Err(error) if !matches!(error, StatementFailure::Trino(_)))
    {
        cancel.delete(deadline).map_err(anyhow::Error::from)
    } else {
        Ok(())
    };
    merge_cleanup(response.map_err(anyhow::Error::from), cleanup)
}
fn arm_selected(
    context: &mut ScenarioContext,
    control: &SqlSession,
    selected: &SelectedStatement,
    query: &str,
) -> Result<(PendingStatement, QueryLifecycleStatementObservation)> {
    let pending = control.submit(query)?;
    let cancel = pending.cancel_handle();
    match selected.arm_client_query(context, &pending.query_id, query) {
        Ok(observation) => Ok((pending, observation)),
        Err(primary) => merge_cleanup(
            Err(primary),
            cancel.delete(selected.deadline).map_err(Into::into),
        ),
    }
}
fn public_sql_error(error: &anyhow::Error) -> Result<&TrinoError> {
    match error
        .chain()
        .find_map(|source| source.downcast_ref::<StatementFailure>())
    {
        Some(StatementFailure::Trino(error)) => Ok(error),
        Some(actual) => Err(anyhow::Error::new(actual.clone())
            .context("Runtime Filter expected original Trino SQL failure")),
        None => bail!("Runtime Filter failure has no original Trino SQL source: {error:#}"),
    }
}
fn require_column(columns: &[Column], index: usize, expected: &str) -> Result<()> {
    let column = columns
        .get(index)
        .context("Runtime Filter result column missing")?;
    // String's Trino signature carries the protocol's unbounded VARCHAR
    // length. Integer/boolean scalars carry no type arguments. Keep the
    // original nominal type and its full declared signature separate.
    let arguments_match = if expected == "varchar" {
        column.type_signature.arguments.as_slice()
            == [novarocks_test_trino_client::TypeArgument::Long(
                2_147_483_647,
            )]
    } else {
        column.type_signature.arguments.is_empty()
    };
    ensure!(
        column.type_signature.raw_type == expected && arguments_match,
        "Runtime Filter expected {expected} result column, got {column:?}"
    );
    Ok(())
}
fn integer_rows(result: StatementResult) -> Result<Vec<i64>> {
    ensure!(
        result.is_result_set() && result.columns.len() == 1,
        "Runtime Filter COUNT requires one result column"
    );
    require_column(&result.columns, 0, "bigint")?;
    result
        .rows
        .into_iter()
        .map(|row| match row.as_slice() {
            [TrinoValue::Integer(value)] => Ok(*value),
            _ => bail!("Runtime Filter COUNT requires one non-null BIGINT: {row:?}"),
        })
        .collect()
}
fn text_rows(result: StatementResult) -> Result<Vec<String>> {
    ensure!(
        result.is_result_set() && result.columns.len() == 1,
        "Runtime Filter profile requires one result column"
    );
    require_column(&result.columns, 0, "varchar")?;
    result
        .rows
        .into_iter()
        .map(|row| {
            let mut cells = row.into_iter();
            match (cells.next(), cells.next()) {
                (Some(TrinoValue::Text(value)), None) => Ok(value),
                actual => {
                    bail!("Runtime Filter profile requires one non-null text value: {actual:?}")
                }
            }
        })
        .collect()
}
fn held_count_rows(result: StatementResult) -> Result<Vec<(i64, bool)>> {
    ensure!(
        result.is_result_set() && result.columns.len() == 2,
        "held Runtime Filter query requires two result columns"
    );
    require_column(&result.columns, 0, "bigint")?;
    require_column(&result.columns, 1, "boolean")?;
    result
        .rows
        .into_iter()
        .map(|row| match row.as_slice() {
            [TrinoValue::Integer(value), TrinoValue::Boolean(complete)] => Ok((*value, *complete)),
            _ => bail!("held Runtime Filter query requires BIGINT and BOOLEAN values: {row:?}"),
        })
        .collect()
}
fn blocking_rows(result: StatementResult) -> Result<Vec<bool>> {
    ensure!(
        result.is_result_set() && result.columns.len() == 1,
        "blocking Runtime Filter query requires one result column"
    );
    require_column(&result.columns, 0, "boolean")?;
    result
        .rows
        .into_iter()
        .map(|row| match row.as_slice() {
            [TrinoValue::Boolean(value)] => Ok(*value),
            _ => bail!("blocking Runtime Filter SLEEP requires a non-null BOOLEAN: {row:?}"),
        })
        .collect()
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

fn connect_control(context: &ScenarioContext, operation: &str) -> Result<SqlSession> {
    context.remaining(operation)?;
    Ok(SqlSession::new(
        context.sql_user(),
        context.trino_port(),
        context.deadline(),
    )?)
}

fn create_runtime_filter_tables(
    context: &mut ScenarioContext,
    control: &mut SqlSession,
    suffix: &str,
) -> Result<RuntimeFilterTables> {
    let deadline = context.deadline();
    let warehouse = context
        .runtime_dir()
        .join("warehouses")
        .join(format!("runtime-filter-{suffix}"));
    std::fs::create_dir_all(&warehouse)
        .with_context(|| format!("create Runtime Filter warehouse {}", warehouse.display()))?;
    let tables = runtime_filter_tables(suffix);
    create_hadoop_catalog(control, &tables.catalog, &warehouse, context.deadline())?;
    rf_execute(
        control,
        format!("CREATE DATABASE {}.{}", tables.catalog, tables.database),
        deadline,
    )
    .context("create Runtime Filter scenario database")?;
    rf_execute(
        control,
        format!(
            "CREATE TABLE {}.{}.{} (id INT NOT NULL, k INT)",
            tables.catalog, tables.database, tables.probe
        ),
        deadline,
    )
    .context("create Runtime Filter probe table")?;
    rf_execute(
        control,
        format!(
            "CREATE TABLE {}.{}.{} (k INT, flag VARCHAR(8))",
            tables.catalog, tables.database, tables.build
        ),
        deadline,
    )
    .context("create Runtime Filter build table")?;
    rf_execute(control, format!(
            "INSERT INTO {}.{}.{} SELECT generate_series, generate_series % 600 FROM TABLE(generate_series(1, 6000))",
            tables.catalog, tables.database, tables.probe
        ), deadline)
        .context("write Runtime Filter probe rows")?;
    for partition in 0..REQUIRED_BACKENDS {
        rf_execute(control, format!(
                "INSERT INTO {}.{}.{} SELECT (generate_series * 3 + {partition}) % 600, CASE WHEN generate_series = 1 THEN 'Y' ELSE 'N' END FROM TABLE(generate_series(1, 200))",
                tables.catalog, tables.database, tables.build
            ), deadline)
            .with_context(|| format!("write Runtime Filter build partition {partition}"))?;
    }
    rf_execute(
        control,
        format!(
            "ANALYZE TABLE {}.{}.{}",
            tables.catalog, tables.database, tables.probe
        ),
        deadline,
    )
    .context("analyze Runtime Filter probe table")?;
    rf_execute(
        control,
        format!(
            "ANALYZE TABLE {}.{}.{}",
            tables.catalog, tables.database, tables.build
        ),
        deadline,
    )
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
    control: &mut SqlSession,
) -> Result<RuntimeFilterTables> {
    let deadline = context.deadline();
    let warehouse = context.runtime_dir().join("warehouses").join("ncp-5");
    std::fs::create_dir_all(&warehouse)
        .with_context(|| format!("create NCP-5 warehouse {}", warehouse.display()))?;
    let tables = ncp5_tables();
    create_hadoop_catalog(control, &tables.catalog, &warehouse, context.deadline())?;
    rf_execute(
        control,
        format!("CREATE DATABASE {}.{}", tables.catalog, tables.database),
        deadline,
    )
    .context("create NCP-5 database")?;
    rf_execute(
        control,
        format!(
            "CREATE TABLE {}.{}.{} (id INT NOT NULL, k INT)",
            tables.catalog, tables.database, tables.probe
        ),
        deadline,
    )
    .context("create NCP-5 probe table")?;
    rf_execute(
        control,
        format!(
            "ALTER TABLE {}.{}.{} SET TBLPROPERTIES ('write.parquet.row-group-size-bytes'='1024')",
            tables.catalog, tables.database, tables.probe
        ),
        deadline,
    )
    .context("set NCP-5 probe row-group size")?;
    rf_execute(
        control,
        format!(
            "CREATE TABLE {}.{}.{} (k INT, flag VARCHAR(8))",
            tables.catalog, tables.database, tables.build
        ),
        deadline,
    )
    .context("create NCP-5 build table")?;
    for range in 0..REQUIRED_BACKENDS {
        let lower = range * 200;
        rf_execute(control, format!(
                "INSERT INTO {}.{}.{} SELECT {} + generate_series, {} + FLOOR((generate_series - 1) / 1000) FROM TABLE(generate_series(1, 200000))",
                tables.catalog,
                tables.database,
                tables.probe,
                range * 10_000,
                lower,
            ), deadline)
            .with_context(|| format!("write NCP-5 probe range {range}"))?;
    }
    rf_execute(
        control,
        format!(
            "INSERT INTO {}.{}.{} VALUES (3, 'Y'), (3, 'N'), (4, 'N')",
            tables.catalog, tables.database, tables.build
        ),
        deadline,
    )
    .context("write NCP-5 selective build rows")?;
    rf_execute(
        control,
        format!(
            "ANALYZE TABLE {}.{}.{}",
            tables.catalog, tables.database, tables.probe
        ),
        deadline,
    )
    .context("analyze NCP-5 probe table")?;
    rf_execute(
        control,
        format!(
            "ANALYZE TABLE {}.{}.{}",
            tables.catalog, tables.database, tables.build
        ),
        deadline,
    )
    .context("analyze NCP-5 build table")?;
    context.action("created three disjoint Iceberg probe ranges for NCP-5 whole-file pruning");
    Ok(tables)
}

fn create_hadoop_catalog(
    control: &mut SqlSession,
    catalog: &str,
    warehouse: &Path,
    deadline: Instant,
) -> Result<()> {
    let warehouse = warehouse.to_string_lossy().replace('"', "\\\"");
    rf_execute(control, format!(
            "CREATE EXTERNAL CATALOG {catalog} PROPERTIES(\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"hadoop\",\"iceberg.catalog.warehouse\"=\"{warehouse}\")"
        ), deadline)
        .with_context(|| format!("create local Hadoop Iceberg catalog {catalog}"))
        .map(|_| ())
}

fn configure_broadcast_runtime_filter(control: &mut SqlSession, deadline: Instant) -> Result<()> {
    configure_runtime_filter(
        control,
        "SET cbo_broadcast_node_mem_budget_bytes = 10737418240",
        deadline,
    )
}

fn configure_partitioned_runtime_filter(control: &mut SqlSession, deadline: Instant) -> Result<()> {
    configure_runtime_filter(
        control,
        "SET cbo_broadcast_node_mem_budget_bytes = 0",
        deadline,
    )
}

fn configure_runtime_filter(
    control: &mut SqlSession,
    join_distribution: &str,
    deadline: Instant,
) -> Result<()> {
    for setting in [
        "SET global_runtime_filter_build_max_size = 10737418240",
        "SET global_runtime_filter_probe_min_selectivity = 0.0",
        join_distribution,
        "SET disable_optimizer_rules = ''",
    ] {
        rf_execute(control, setting, deadline)
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
    control: &mut SqlSession,
    tables: &RuntimeFilterTables,
    deadline: Instant,
) -> Result<()> {
    let explain: Vec<String> = rf_execute(
        control,
        format!("EXPLAIN VERBOSE {}", runtime_filter_count_query(tables)),
        deadline,
    )
    .and_then(text_rows)
    .context("explain native partitioned Runtime Filter candidate query")?;
    let explain = explain.join("\n");
    ensure!(
        explain.contains("HASH JOIN (PARTITIONED"),
        "Runtime Filter candidate did not select a partitioned hash join: {explain}"
    );
    for key in ["p.k", "b.k"] {
        let partition = format!("PARTITION: HASH_PARTITIONED ({key})");
        ensure!(
            explain.lines().any(|line| line.trim() == partition),
            "Runtime Filter candidate did not expose a hash partition on {key}: {explain}"
        );
    }
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

fn await_ack_drop_while_query_active<T: std::fmt::Debug>(
    context: &mut ScenarioContext,
    fault_kind: &str,
    selected: &SelectedStatement,
    baseline: &QueryExecutionResourceSnapshot,
    created_baseline: &[usize],
    done: &mpsc::Receiver<Result<Vec<T>>>,
) -> Result<()> {
    let mut latest = None;
    loop {
        match done.try_recv() {
            Ok(Err(error)) => return Err(error.context(format!("Runtime Filter retry query reached terminal before the Accepted ACK-drop was observed; baseline={baseline:?}; latest={latest:?}"))),
            Ok(Ok(rows)) => bail!("Runtime Filter retry query reached terminal before the Accepted ACK-drop was observed: {rows:?}; baseline={baseline:?}; latest={latest:?}"),
            Err(mpsc::TryRecvError::Disconnected) => {
                bail!("Runtime Filter retry query actor disconnected before ACK-drop observation")
            }
            Err(mpsc::TryRecvError::Empty) => {}
        }
        let current = resource_snapshot(context)?;
        let mut hit_count = 0;
        for &(index, kind) in &selected.faults {
            if kind == fault_kind
                && context.handle().selected_query_lifecycle_fault_hit(
                    index,
                    kind,
                    &selected.token,
                )?
            {
                hit_count += 1;
            }
        }
        let query_active =
            task_query_activity_observed(context, baseline, &current, created_baseline)?;
        latest = Some((hit_count, query_active, current));
        if hit_count > 0 && query_active {
            match done.try_recv() {
                Ok(Err(error)) => return Err(error.context(format!("Runtime Filter retry query reached terminal during the Accepted ACK-drop observation; baseline={baseline:?}; latest={latest:?}"))),
                Ok(Ok(rows)) => bail!("Runtime Filter retry query reached terminal during the Accepted ACK-drop observation: {rows:?}; baseline={baseline:?}; latest={latest:?}"),
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
                    .get(NATIVE_QUERY_ACTIVE_TASKS_RESOURCE)
                    .copied()
                    .unwrap_or_default()
                    > baseline
                        .resources
                        .get(NATIVE_QUERY_ACTIVE_TASKS_RESOURCE)
                        .copied()
                        .unwrap_or_default()
            });
    let created = be_marker_counts(context, TASK_CREATE_APPLIED_MARKER)?;
    Ok(native_fragment_active && backends_advanced(created_baseline, &created) >= 2)
}

fn merge_cleanup<T>(primary: Result<T>, cleanup: Result<()>) -> Result<T> {
    match (primary, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(primary), Err(cleanup)) => {
            Err(RuntimeFilterCleanupFailure { primary, cleanup }.into())
        }
    }
}
struct RuntimeFilterCleanupFailure {
    primary: anyhow::Error,
    cleanup: anyhow::Error,
}
impl std::fmt::Display for RuntimeFilterCleanupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Runtime Filter operation failed: {:#}; actual cleanup also failed: {:#}",
            self.primary, self.cleanup
        )
    }
}
impl std::fmt::Debug for RuntimeFilterCleanupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::error::Error for RuntimeFilterCleanupFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.primary.as_ref())
    }
}

/// All returned actor phases settle the exact query and actually join the same
/// original worker. Raw non-Trino refusal remains nonterminal after Join.
fn settle_query<T, R>(
    mut actor: JoinedClientThread,
    done: &mpsc::Receiver<Result<Vec<T>>>,
    primary: Result<R>,
) -> Result<R> {
    let cancellation = if primary.is_err() {
        actor.cancel_active()
    } else {
        Ok(())
    };
    let joined = merge_cleanup(actor.join(), cancellation);
    let pending = match done.try_recv() {
        Ok(Err(error)) => {
            Err(error.context("original Runtime Filter SQL result failed during cleanup"))
        }
        _ => Ok(()),
    };
    merge_cleanup(merge_cleanup(primary, joined), pending)
}
struct RuntimeFilterActorPanic(std::sync::Mutex<Box<dyn std::any::Any + Send>>);
impl std::fmt::Display for RuntimeFilterActorPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _ = &self.0;
        f.write_str("original Runtime Filter SQL actor panicked; payload retained")
    }
}
impl std::fmt::Debug for RuntimeFilterActorPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::error::Error for RuntimeFilterActorPanic {}

enum RuntimeFilterCancellation {
    Submitted {
        cancel: Box<CancelHandle>,
        deadline: Instant,
    },
    #[cfg(test)]
    NoSubmission,
}
struct JoinedClientThread {
    thread: Option<thread::JoinHandle<Result<()>>>,
    cancellation: RuntimeFilterCancellation,
    nonterminal: Arc<AtomicBool>,
}
impl JoinedClientThread {
    fn delete(&self) -> Result<()> {
        match &self.cancellation {
            RuntimeFilterCancellation::Submitted { cancel, deadline } => {
                Ok(cancel.delete(*deadline)?)
            }
            #[cfg(test)]
            RuntimeFilterCancellation::NoSubmission => Ok(()),
        }
    }
    fn cancel_active(&self) -> Result<()> {
        if self.nonterminal.load(Ordering::Acquire)
            && self
                .thread
                .as_ref()
                .is_some_and(|worker| !worker.is_finished())
        {
            self.delete()?;
        }
        Ok(())
    }
    fn join(&mut self) -> Result<()> {
        let joined = self
            .thread
            .take()
            .context("Runtime Filter SQL client already joined")?
            .join()
            .map_err(|payload| {
                anyhow::Error::from(RuntimeFilterActorPanic(std::sync::Mutex::new(payload)))
            })
            .and_then(|outcome| outcome);
        let cancellation = if self.nonterminal.load(Ordering::Acquire) {
            self.delete()
        } else {
            Ok(())
        };
        merge_cleanup(joined, cancellation)
    }
    #[cfg(test)]
    fn unsubmitted_worker(thread: thread::JoinHandle<Result<()>>) -> Self {
        Self {
            thread: Some(thread),
            cancellation: RuntimeFilterCancellation::NoSubmission,
            nonterminal: Arc::new(AtomicBool::new(false)),
        }
    }
}
impl Drop for JoinedClientThread {
    fn drop(&mut self) {
        if self.thread.is_some() {
            let cancellation = self.cancel_active();
            let joined = self.join();
            if let Err(error) = merge_cleanup(joined, cancellation) {
                eprintln!("Original Runtime Filter SQL actor cleanup failed: {error:#}");
            }
        }
    }
}
struct BlockingQuery {
    ready: mpsc::Receiver<String>,
    done: mpsc::Receiver<Result<Vec<bool>>>,
    thread: JoinedClientThread,
}
struct HeldCountQuery {
    ready: mpsc::Receiver<String>,
    done: mpsc::Receiver<Result<Vec<(i64, bool)>>>,
    thread: JoinedClientThread,
}
fn publish_result<T>(sender: &mpsc::SyncSender<Result<T>>, outcome: Result<T>) -> Result<()> {
    match sender.send(outcome) {
        Ok(()) => Ok(()),
        Err(mpsc::SendError(Err(actual))) => {
            Err(actual.context("original Runtime Filter query failed after result owner closed"))
        }
        Err(mpsc::SendError(Ok(_))) => {
            bail!("original Runtime Filter result owner closed before delivery")
        }
    }
}
type StartedRuntimeFilterQuery<T> = (
    mpsc::Receiver<String>,
    mpsc::Receiver<Result<Vec<T>>>,
    JoinedClientThread,
);

fn start_runtime_filter_query<T: Send + 'static>(
    mut session: SqlSession,
    pending: PendingStatement,
    deadline: Instant,
    decode: fn(StatementResult) -> Result<Vec<T>>,
) -> Result<StartedRuntimeFilterQuery<T>> {
    let (ready_tx, ready) = mpsc::sync_channel(1);
    let (done_tx, done) = mpsc::sync_channel(1);
    let cancel = pending.cancel_handle();
    // Before the first GET, no server terminal has been observed.
    let nonterminal = Arc::new(AtomicBool::new(true));
    let worker_nonterminal = nonterminal.clone();
    let thread = thread::Builder::new()
        .name("runtime-filter-sql".into())
        .spawn(move || -> Result<()> {
            ready_tx
                .send(pending.query_id.clone())
                .context("publish actual Runtime Filter Pending query id")?;
            let response = session.run_pending(pending);
            worker_nonterminal.store(
                matches!(&response, Err(error) if !matches!(error, StatementFailure::Trino(_))),
                Ordering::Release,
            );
            publish_result(
                &done_tx,
                response.map_err(anyhow::Error::from).and_then(decode),
            )
        });
    match thread {
        Ok(thread) => Ok((
            ready,
            done,
            JoinedClientThread {
                thread: Some(thread),
                cancellation: RuntimeFilterCancellation::Submitted {
                    cancel: Box::new(cancel),
                    deadline,
                },
                nonterminal,
            },
        )),
        Err(primary) => merge_cleanup(
            Err(primary.into()),
            cancel.delete(deadline).map_err(Into::into),
        ),
    }
}
fn start_held_runtime_filter_count_query(
    session: SqlSession,
    pending: PendingStatement,
    deadline: Instant,
) -> Result<HeldCountQuery> {
    let (ready, done, thread) =
        start_runtime_filter_query(session, pending, deadline, held_count_rows)?;
    Ok(HeldCountQuery {
        ready,
        done,
        thread,
    })
}
fn start_blocking_runtime_filter_query(
    session: SqlSession,
    pending: PendingStatement,
    deadline: Instant,
) -> Result<BlockingQuery> {
    let (ready, done, thread) =
        start_runtime_filter_query(session, pending, deadline, blocking_rows)?;
    Ok(BlockingQuery {
        ready,
        done,
        thread,
    })
}
fn assert_cancelled_query(
    done: &mpsc::Receiver<Result<Vec<bool>>>,
    timeout: Duration,
) -> Result<()> {
    let result = done
        .recv_timeout(timeout)
        .context("Runtime Filter query did not terminate before scenario deadline")?;
    let error = match result {
        Ok(rows) => bail!("Runtime Filter query unexpectedly succeeded after DELETE: {rows:?}"),
        Err(error) => error,
    };
    let wire = public_sql_error(&error)?;
    if wire.error_code == 3 && wire.error_name == "USER_CANCELED" && wire.error_type == "USER_ERROR"
    {
        Ok(())
    } else {
        Err(error.context("expected original Trino USER_CANCELED (3, USER_ERROR)"))
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

#[cfg(test)]
mod returned_error_cleanup_tests {
    use super::*;

    #[test]
    fn string_signature_keeps_unbounded_varchar_length_and_scalar_nominal_types() {
        let column = |raw: &str, arguments: serde_json::Value| -> Column {
            serde_json::from_value(serde_json::json!({
                "name":"catalog", "type":raw,
                "typeSignature":{"rawType":raw,"arguments":arguments}
            }))
            .unwrap()
        };
        // Independent protocol literal already protected by columns.rs;
        // neither a SHOW result nor a client parser supplies this expectation.
        let string = column(
            "varchar",
            serde_json::json!([{"kind":"LONG","value":2147483647}]),
        );
        require_column(&[string], 0, "varchar").unwrap();
        for arguments in [
            serde_json::json!([]),
            serde_json::json!([{"kind":"LONG","value":64}]),
        ] {
            assert!(require_column(&[column("varchar", arguments)], 0, "varchar").is_err());
        }
        assert!(require_column(&[column("bigint", serde_json::json!([]))], 0, "varchar").is_err());
        for raw in ["bigint", "boolean"] {
            require_column(&[column(raw, serde_json::json!([]))], 0, raw).unwrap();
            assert!(
                require_column(
                    &[column(raw, serde_json::json!([{"kind":"LONG","value":1}]))],
                    0,
                    raw
                )
                .is_err()
            );
        }
    }

    #[test]
    fn failed_phase_retains_actual_join_and_unconsumed_sql_failures() {
        let (sender, done) = mpsc::sync_channel::<Result<Vec<i64>>>(1);
        let actor = JoinedClientThread::unsubmitted_worker(thread::spawn(move || -> Result<()> {
            publish_result(
                &sender,
                Err(anyhow::Error::from(StatementFailure::Transport(
                    "pending SQL failure".into(),
                ))),
            )?;
            Err(anyhow::anyhow!("actual worker cleanup failure"))
        }));
        let phase: Result<()> = Err(anyhow::anyhow!("original observation failure"));
        let error = settle_query(actor, &done, phase).unwrap_err();
        let text = format!("{error:#}");
        assert!(text.contains("original observation failure"));
        assert!(text.contains("actual worker cleanup failure"));
        assert!(text.contains("pending SQL failure"));
        // join completed before returning: no producer remains to hide an error.
        assert!(matches!(
            done.try_recv(),
            Err(mpsc::TryRecvError::Disconnected)
        ));
    }

    #[test]
    fn consumed_expected_query_error_is_not_reintroduced_by_settle() {
        let (sender, done) = mpsc::sync_channel::<Result<Vec<i64>>>(1);
        let actor = JoinedClientThread::unsubmitted_worker(thread::spawn(move || -> Result<()> {
            publish_result(
                &sender,
                Err(anyhow::Error::from(StatementFailure::Transport(
                    "already checked cancellation".into(),
                ))),
            )
        }));
        assert!(done.recv().unwrap().is_err());
        settle_query(actor, &done, Ok(())).unwrap();
    }
    #[test]
    fn cancellation_requires_actual_trino_user_canceled() {
        let (sender, done) = mpsc::sync_channel(1);
        let wire: TrinoError = serde_json::from_value(serde_json::json!({
            "message":"original cancellation", "errorCode":3,
            "errorName":"USER_CANCELED", "errorType":"USER_ERROR"
        }))
        .unwrap();
        sender
            .send(Err(anyhow::Error::from(StatementFailure::Trino(wire))))
            .unwrap();
        assert_cancelled_query(&done, Duration::ZERO).unwrap();
        sender
            .send(Err(anyhow::Error::from(StatementFailure::Http {
                status: 503,
                body: "USER_CANCELED code=3".into(),
            })))
            .unwrap();
        assert!(assert_cancelled_query(&done, Duration::ZERO).is_err());
    }
}
