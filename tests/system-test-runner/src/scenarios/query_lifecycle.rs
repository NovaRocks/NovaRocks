use super::result_delivery_deadline::set_query_timeout;
use super::trino_protocol::{observation, require_error};
use crate::actors::sql::{self, SqlSession, StatementFailure, StatementResult, TrinoValue};
use crate::actors::trino_protocol::TrinoClient;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use crate::scenarios::task_evidence;
use anyhow::{Context, Result, bail, ensure};
use novarocks_cluster_harness::{
    CrossProcessNativeFaultProxyConfig, QueryExecutionResourceSnapshot,
    QueryLifecycleStructuredSnapshot, TestClusterOwner,
    native_fault_proxy::{ProxyDirection, ProxyMode},
};
use novarocks_execution_contract::failure::{FailureClass, FailureFact, FailureReason, domain};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use crate::scenarios::native_sql_controls::{SelectedStatement, SqlIntentOwner, StatementSpec};

mod limit_zero;

const REQUIRED_BACKENDS: usize = 3;
const IO_TIMEOUT_CAP: Duration = Duration::from_secs(10);
const RESOURCE_POLL_INTERVAL: Duration = Duration::from_millis(50);
const BASELINE_QUERY: &str = "SELECT v FROM (SELECT 1 AS v UNION ALL SELECT 2) t ORDER BY v";
/// The query the three identity-fence cases below issue.
///
/// Unlike the coordinator-local baseline it has to place real tasks on a
/// backend, because every one of these faults is claimed on an operation about
/// a task: two on `CreateTask` and one on a status frame. The sleep keeps a
/// task running rather than being needed by any fault -- a status frame exists
/// only while a task does, and a constant relation can finish before its
/// subscription has carried one.
pub(super) const NID2_FENCE_QUERY: &str =
    "SELECT v FROM (SELECT sleep(10) AS v UNION ALL SELECT sleep(10)) t ORDER BY v";

fn observed_statement(sql: &str) -> StatementSpec {
    StatementSpec {
        sql: sql.into(),
        faults: vec![],
        observations: (0..REQUIRED_BACKENDS)
            .map(|index| {
                (
                    index,
                    novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol
                        .as_str(),
                    None,
                )
            })
            .collect(),
    }
}
fn fault_statement(sql: &str, fault: &'static str) -> StatementSpec {
    StatementSpec {
        sql: sql.into(),
        faults: (0..REQUIRED_BACKENDS).map(|index| (index, fault)).collect(),
        observations: vec![],
    }
}

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(DistributedBaseline::default()),
        Box::new(ClientAbandonment),
        Box::new(QueryTimeout),
        Box::new(NoEffectReadAfterBackendExit::default()),
        Box::new(LiveBackendPartition::default()),
        Box::new(limit_zero::LimitZeroUnknownSender::default()),
        Box::new(Nid2CreateRejected::default()),
        Box::new(Nid2CreateReceiptForeignTask::default()),
        Box::new(Nid2ForeignStatusProcess::default()),
    ]
}

/// A live process that becomes unreachable is not a process exit.
///
/// The target is selected from the task protocol's own CreateTask marker, so
/// the partition always applies to a backend the old attempt really uses.
/// Pausing its advertised Native endpoint must revoke it for *future*
/// admission without replacing its process identity. The old read is kept
/// unread and must remain nonterminal until its path is restored; a new read
/// then proves the revoked backend cannot contaminate a fresh attempt.
#[derive(Default)]
struct LiveBackendPartition {
    intent: SqlIntentOwner,
}

impl Scenario for LiveBackendPartition {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "query_lifecycle",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "query-lifecycle/live-backend-partition"
    }

    fn launch_config(
        &self,
        _scenario_root: &std::path::Path,
    ) -> Result<crate::scenario::ScenarioLaunchConfig> {
        Ok(crate::scenario::ScenarioLaunchConfig {
            native_fault_proxies: CrossProcessNativeFaultProxyConfig {
                backend_retained_byte_limits: (0..REQUIRED_BACKENDS)
                    .map(|index| (index, 64 * 1024))
                    .collect::<BTreeMap<_, _>>(),
            },
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
            vec![
                observed_statement(NID2_FENCE_QUERY),
                observed_statement(BASELINE_QUERY),
            ],
            &[],
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_snapshot(context)?;
        let create_counts = (0..context.handle().be_count())
            .map(|index| {
                context.handle().be_log_count(
                    index,
                    novarocks_failpoint::observation_marker::TASK_CREATE_APPLIED,
                )
            })
            .collect::<Result<Vec<_>>>()?;

        use super::paimon::scan_statement_actor::combine;

        let deadline = context.deadline();
        let selected = self.intent.take(0)?;
        let mut session = SqlSession::new(context.sql_user(), context.trino_port(), deadline)?;
        let pending = session.submit(NID2_FENCE_QUERY)?;
        let cancel = pending.cancel_handle();
        let (pending, old_observation) =
            selected.arm_pending(context, pending, NID2_FENCE_QUERY)?;
        let partition = thread::scope(|scope| -> Result<_> {
            let (done_tx, done) = mpsc::sync_channel(1);
            let worker = scope.spawn(move || -> Result<()> {
                match done_tx.send(session.run_pending(pending)) {
                    Ok(()) => Ok(()),
                    Err(mpsc::SendError(Err(error))) => Err(error.into()),
                    Err(mpsc::SendError(Ok(_))) => bail!("old read result receiver closed"),
                }
            });
            let mut paused = None;
            let phase = (|| -> Result<_> {
                await_resource_activity(context, &baseline)?;
                let target = await_fresh_task_create(context, &create_counts)?;
                let process_before = context.process_ids().backends[target];
                let proxy = context.handle().native_fault_proxy(target)?;
                await_proxy_connection(context, &proxy, target)?;
                let proxy = paused.insert(proxy);
                proxy.set_mode(ProxyDirection::ClientToUpstream, ProxyMode::Paused);
                proxy.set_mode(ProxyDirection::UpstreamToClient, ProxyMode::Paused);
                context.action(format!(
                    "paused both directions for admitted live BE[{target}]"
                ));
                await_backend_revoked_for_future_admission(context, target, process_before)?;
                assert_partition_read_pending(&done, target)?;

                let mut independent =
                    SqlSession::new(context.sql_user(), context.trino_port(), deadline)?;
                let terminal = run_observed_baseline(
                    context,
                    &mut independent,
                    &self.intent.take(1)?,
                    "post-partition independent",
                )?;
                let participants = task_evidence::assert_query_completed_across_boundary(
                    context,
                    &terminal,
                    "post-partition independent read",
                )?;
                ensure!(
                    !participants.contains(&target),
                    "fresh read used revoked BE[{target}]: {participants:?}"
                );
                assert_partition_read_pending(&done, target)?;
                context.action(format!(
                    "independent query used {participants:?} while original query stayed pending and BE[{target}] was revoked"
                ));
                Ok((target, process_before))
            })();
            if let Some(proxy) = paused {
                proxy.set_mode(ProxyDirection::ClientToUpstream, ProxyMode::Forward);
                proxy.set_mode(ProxyDirection::UpstreamToClient, ProxyMode::Forward);
                context.action("restored both Native transport directions");
            }
            let primary = phase.and_then(|(target, process_before)| {
                let result = done
                    .recv_timeout(context.remaining("await restored old read")?)
                    .context("old read did not finish after Native transport restoration")??;
                ensure!(
                    result.columns.len() == 1
                        && result.columns[0].name == "v"
                        && result.columns[0].type_signature.raw_type == "boolean"
                        && result.columns[0].type_signature.arguments.is_empty()
                        && result.rows
                            == [
                                vec![TrinoValue::Boolean(true)],
                                vec![TrinoValue::Boolean(true)]
                            ],
                    "restored old read returned unexpected result: {result:?}"
                );
                Ok((target, process_before))
            });
            let cancelled = if primary.is_err() {
                cancel.delete(deadline).map_err(anyhow::Error::from)
            } else {
                Ok(())
            };
            let joined = worker
                .join()
                .map_err(|_| anyhow::anyhow!("old live-partition read actor panicked"))
                .and_then(|result| result);
            combine(primary, combine(cancelled, joined))
        });
        let terminal = partition.and_then(|(target, process_before)| {
            let terminal = context
                .handle()
                .await_query_lifecycle_statement_observation(&old_observation, true, deadline)?;
            ensure!(
                terminal.attempt_id == 1,
                "restored old read unexpectedly replaced its attempt: {}",
                terminal.attempt_id
            );
            ensure!(
                context.process_ids().backends[target] == process_before,
                "BE[{target}] process identity changed during a live transport partition"
            );
            context.action(format!(
                "restored old attempt completed without replacing BE[{target}] process identity"
            ));
            Ok(())
        });
        let drained = context
            .handle()
            .drain_query_lifecycle_statement_binding(&old_observation, deadline);
        let convergence = await_resource_convergence(context, &baseline);
        combine(terminal, combine(drained, convergence))
    }
}

fn assert_partition_read_pending(
    done: &mpsc::Receiver<std::result::Result<StatementResult, StatementFailure>>,
    target: usize,
) -> Result<()> {
    match done.try_recv() {
        Err(mpsc::TryRecvError::Empty) => Ok(()),
        Ok(outcome) => {
            bail!("old read became terminal while BE[{target}] remained partitioned: {outcome:?}")
        }
        Err(mpsc::TryRecvError::Disconnected) => {
            bail!("old read actor disconnected while BE[{target}] remained partitioned")
        }
    }
}

fn ordinary_frontend_has_test_controls(records: &serde_json::Value) -> Result<bool> {
    let mut captures = records
        .as_array()
        .context("ordinary peer build captures are not an array")?
        .iter()
        .filter(|record| record["launch_capture"] == "fe.build-info.json");
    let capture = captures
        .next()
        .context("ordinary peer omitted its original FE build capture")?;
    ensure!(
        captures.next().is_none(),
        "ordinary peer repeats its FE build capture"
    );
    let info = &capture["build_info"];
    ensure!(
        info["schema_version"] == 1 && info["package"] == "novarocks-server",
        "ordinary peer FE build identity is invalid"
    );
    let mut owners = info["capabilities"]
        .as_array()
        .context("ordinary FE omitted owner capabilities")?
        .iter()
        .filter(|owner| owner["owner"] == "novarocks-frontend-application");
    let owner = owners
        .next()
        .context("ordinary FE omitted application capabilities")?;
    ensure!(
        owners.next().is_none(),
        "ordinary FE repeats application capabilities"
    );
    let capabilities = owner["compiled_capabilities"]
        .as_array()
        .context("ordinary FE capabilities are invalid")?;
    ensure!(
        capabilities.iter().all(serde_json::Value::is_string),
        "ordinary FE capability is not a string"
    );
    Ok(capabilities
        .iter()
        .any(|capability| capability == "test-controls"))
}

fn ordinary_call_refusal_matches(message: &str, controls_compiled: bool) -> bool {
    if controls_compiled {
        message.contains("MV test CALL requires explicit authorization")
    } else {
        message.contains("unsupported") && message.contains("novarocks_imv_stateless_rebuild")
    }
}

#[derive(Default)]
struct DistributedBaseline {
    intent: SqlIntentOwner,
    binary: Mutex<Option<PathBuf>>,
}

impl DistributedBaseline {
    fn run_ordinary_baseline(&self, context: &mut ScenarioContext) -> Result<()> {
        use novarocks_cluster_harness::test_sessions::{ClusterTestSessionPlan, RoleSlot};

        let binary = self
            .binary
            .lock()
            .map_err(|_| anyhow::anyhow!("binary baseline artifact owner poisoned"))?
            .take()
            .context("binary baseline artifact was not frozen before launch")?;
        let deadline = context.deadline();
        let mut plan = ClusterTestSessionPlan::new(deadline)?;
        let frontend = plan.reserve_launch(RoleSlot::Frontend)?;
        let backends = (0..REQUIRED_BACKENDS)
            .map(|index| plan.reserve_launch(RoleSlot::Backend(index)))
            .collect::<Result<Vec<_>>>()?;
        plan.authorize_ordinary_launch(&frontend, binary.clone())?;
        for backend in &backends {
            plan.authorize_ordinary_launch(backend, binary.clone())?;
        }
        let mut peer = context.launch_peer_cluster_with_test_session(
            "ordinary-baseline",
            ScenarioLaunchConfig::default(),
            plan,
        )?;
        let primary = (|| -> Result<()> {
            ensure!(
                peer.be_count() == REQUIRED_BACKENDS,
                "ordinary baseline requires native 1FE+3BE, got 1FE+{}BE",
                peer.be_count()
            );
            let controls_compiled =
                ordinary_frontend_has_test_controls(&peer.build_information_evidence()?)?;
            let baseline = peer
                .query_execution_resource_snapshot()?
                .context("binary baseline peer did not expose the public query-resource oracle")?;
            context.action("captured binary artifact native 1FE+3BE query-resource baseline");
            let query = (|| -> Result<()> {
                let mut session =
                    SqlSession::new(peer.sql_user(), peer.runtime().fe_trino_port, deadline)?;
                for level in ["package", "wipe"] {
                    let sql = format!(
                        "CALL test_control_probe.system.novarocks_imv_stateless_rebuild(table => 'ns.mv', level => '{level}')"
                    );
                    match session.execute(&sql) {
                        Err(StatementFailure::Trino(error)) => {
                            ensure!(
                                error.error_code == 65536
                                    && error.error_name == "GENERIC_INTERNAL_ERROR"
                                    && error.error_type == "INTERNAL_ERROR"
                                    && ordinary_call_refusal_matches(
                                        &error.message,
                                        controls_compiled
                                    ),
                                "ordinary test-only CALL returned another SQL failure: {error:?}"
                            );
                        }
                        Err(error) => {
                            return Err(anyhow::Error::new(error).context(
                                "binary test-only CALL did not return a public SQL error",
                            ));
                        }
                        Ok(_) => bail!("binary artifact accepted test-only {level} CALL"),
                    }
                }
                for ordinal in ["binary first", "binary second", "binary third"] {
                    assert_baseline_rows(&session.execute(BASELINE_QUERY)?, ordinal)?;
                }
                Ok(())
            })();
            let convergence = peer
                .await_query_execution_resource_convergence(&baseline, deadline)
                .context("await binary baseline public query-resource convergence");
            match (query, convergence) {
                (Ok(()), Ok(())) => {
                    ensure!(
                        Instant::now() < deadline,
                        "ordinary baseline original deadline expired before resource convergence completed"
                    );
                    context.action(
                        "ordinary artifact rejected package/wipe CALL through public SQL errors, then returned [1, 2] for all three baseline queries; all original statements reached terminal pages and query resources converged",
                    );
                    Ok(())
                }
                (Err(primary), Err(cleanup)) => {
                    bail!("{primary:#}; binary baseline resource convergence: {cleanup:#}")
                }
                (Err(error), _) | (_, Err(error)) => Err(error),
            }
        })();
        if primary.is_err() {
            peer.retain_runtime_artifacts();
        }
        let cleanup = TestClusterOwner::shutdown(&mut peer);
        match (primary, cleanup) {
            (Ok(()), Ok(())) => {
                context.action("binary baseline peer roles actually stopped and reaped");
                Ok(())
            }
            (Err(primary), Err(cleanup)) => {
                bail!("{primary:#}; binary baseline peer shutdown: {cleanup:#}")
            }
            (Err(error), _) | (_, Err(error)) => Err(error),
        }
    }
}

impl Scenario for DistributedBaseline {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "query_lifecycle",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "query-lifecycle/distributed-baseline"
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        let mut frozen_binary = self
            .binary
            .lock()
            .map_err(|_| anyhow::anyhow!("binary baseline artifact owner poisoned"))?;
        ensure!(
            frozen_binary.is_none(),
            "ordinary baseline artifact was already frozen"
        );
        let plan = self.intent.freeze(
            (0..3).map(|_| observed_statement(BASELINE_QUERY)).collect(),
            &[],
            cluster_size,
            binary,
            deadline,
        )?;
        *frozen_binary = Some(binary.to_path_buf());
        Ok(Some(plan))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        self.run_ordinary_baseline(context)?;
        let baseline = resource_snapshot(context)?;
        context.action("captured query-resource baseline");

        let deadline = context.deadline();
        let mut session = SqlSession::new(context.sql_user(), context.trino_port(), deadline)?;
        let mut snapshots = Vec::new();
        for (index, ordinal) in ["first", "second", "third"].into_iter().enumerate() {
            snapshots.push(run_observed_baseline(
                context,
                &mut session,
                &self.intent.take(index)?,
                ordinal,
            )?);
        }
        let [first, second, third] = snapshots.as_slice() else {
            unreachable!("exactly three selected baseline statements")
        };
        let snapshots = [first, second, third];
        assert_process_attribution(&snapshots)?;
        assert_process_attribution_diagnostics(context, &snapshots)?;
        context.action(format!(
            "verified three native terminal snapshots share namespace=0x{:016x}, use consecutive sequence {}, {}, {}, and attempt=1",
            first.process_namespace,
            first.local_sequence,
            second.local_sequence,
            third.local_sequence,
        ));

        await_resource_convergence(context, &baseline)
    }
}

struct ClientAbandonment;

impl Scenario for ClientAbandonment {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "query_lifecycle",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "query-lifecycle/client-abandonment"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        const CLIENT_IDLE_TIMEOUT: Duration = Duration::from_millis(300_000);
        require_three_backends(context)?;
        ensure!(
            context.remaining("observe default Trino client inactivity deadline")?
                > CLIENT_IDLE_TIMEOUT + IO_TIMEOUT_CAP,
            "client abandonment needs a case budget beyond the production 300-second timeout"
        );
        let baseline = resource_snapshot(context)?;
        let before = observation(context, "lifecycle-abandon-before")?;
        ensure!(before.active == 0, "expected an idle Trino registry");
        let actor = TrinoClient::new(
            context.trino_port(),
            context.sql_user(),
            context.deadline(),
            &context.scenario_root().join("abandon-http"),
        )?;
        let mut live_uri = None;
        let outcome = (|| -> Result<()> {
            let post = actor.submit(NID2_FENCE_QUERY)?;
            live_uri = post.next_uri()?;
            let admitted = post.assert_success()?;
            let id = admitted["id"]
                .as_str()
                .context("POST omitted query identity")?;
            let first = live_uri.as_deref().context("POST omitted nextUri")?;
            let last_request_started = Instant::now();
            let running = actor.get(first)?;
            live_uri = running.next_uri()?;
            let value = running.assert_success()?;
            ensure!(
                value["id"] == id && value.get("data").is_none() && live_uri.is_some(),
                "first GET did not leave the original query running without rows"
            );
            await_resource_activity(context, &baseline)?;
            context.action(
                "observed the original distributed query before stopping continuation requests",
            );
            let mut samples = Vec::new();
            // Only the management endpoint is polled while the query is idle.
            // A statement GET, HEAD, or DELETE here would renew its timer.
            let after = loop {
                let elapsed = last_request_started.elapsed();
                let snapshot = observation(context, "lifecycle-abandon-idle")?;
                samples.push(serde_json::json!({
                    "elapsed_ms": elapsed.as_millis(),
                    "active": snapshot.active,
                    "abandoned": snapshot.abandoned,
                }));
                if snapshot.active == 0 {
                    ensure!(
                        last_request_started.elapsed() >= CLIENT_IDLE_TIMEOUT,
                        "query settled before the production inactivity timeout"
                    );
                    break snapshot;
                }
                ensure!(
                    snapshot.active == 1 && snapshot.abandoned == before.abandoned,
                    "unexpected registry transition while the original client was idle"
                );
                ensure!(
                    elapsed < CLIENT_IDLE_TIMEOUT + IO_TIMEOUT_CAP,
                    "original query did not settle after the production inactivity timeout"
                );
                thread::sleep(
                    context
                        .remaining("observe client inactivity")?
                        .min(Duration::from_secs(1)),
                );
            };
            std::fs::write(
                context.scenario_root().join("abandonment-clock.json"),
                serde_json::to_vec_pretty(&samples)?,
            )?;
            ensure!(
                after.abandoned == before.abandoned + 1
                    && after.complete == before.complete
                    && after.failed == before.failed
                    && after.client_cancel == before.client_cancel,
                "client inactivity did not produce exactly one abandonment settlement"
            );
            let terminal = actor.get(live_uri.as_deref().context("missing idle continuation")?)?;
            let value = require_error(&terminal, (2, "ABANDONED_QUERY", "USER_ERROR"))?;
            ensure!(
                value["id"] == id,
                "abandonment changed the original query id"
            );
            live_uri = None;
            Ok(())
        })();
        let cleanup = cancel_live_trino_query(&actor, live_uri.as_deref());
        let convergence = await_resource_convergence(context, &baseline);
        if outcome.is_err() || cleanup.is_err() || convergence.is_err() {
            bail!(
                "Trino lifecycle abandonment failed: outcome={outcome:?}, cleanup={cleanup:?}, convergence={convergence:?}"
            );
        }
        Ok(())
    }
}

struct QueryTimeout;

impl Scenario for QueryTimeout {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "query_lifecycle",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "query-lifecycle/query-timeout"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_snapshot(context)?;
        context.action("captured query-resource baseline");

        let deadline = Instant::now() + bounded_io_timeout(context, "Trino lifecycle timeout")?;
        let mut actor = TrinoClient::new(
            context.trino_port(),
            context.sql_user(),
            deadline,
            &context.scenario_root().join("timeout-http"),
        )?;
        let mut live_uri = None;
        let outcome = (|| -> Result<()> {
            set_query_timeout(&mut actor, 1, &mut live_uri)?;
            let post = actor.submit(NID2_FENCE_QUERY)?;
            live_uri = post.next_uri()?;
            let submitted = post.assert_success()?;
            let id = submitted["id"]
                .as_str()
                .context("POST omitted query identity")?;
            let first = live_uri.as_deref().context("POST omitted nextUri")?;
            // The first GET starts execution and can wait as long as this query's
            // deadline. Observe its Native resources while the original GET runs.
            let (first_page, activity) = thread::scope(|scope| {
                let get = scope.spawn(|| actor.get(first));
                let activity = (|| -> Result<_> {
                    while !get.is_finished() {
                        let snapshot = resource_snapshot(context)?;
                        if snapshot != baseline && !get.is_finished() {
                            return Ok(snapshot);
                        }
                        thread::sleep(RESOURCE_POLL_INTERVAL);
                    }
                    bail!("initial GET completed before observing in-flight Native resources");
                })();
                (get.join(), activity)
            });
            let mut page = first_page.map_err(|_| anyhow::anyhow!("initial GET panicked"))??;
            live_uri = page.next_uri()?;
            let activity = activity?;
            std::fs::write(
                context.scenario_root().join("timeout-native-activity.txt"),
                format!("baseline={baseline:#?}\nin_flight={activity:#?}\n"),
            )?;
            context.action("observed in-flight distributed query resources before timeout");
            loop {
                let value = page.json()?;
                ensure!(
                    value["id"] == id,
                    "Trino continuation changed the original query id"
                );
                if let Some(uri) = live_uri.as_deref() {
                    page.assert_success()?;
                    ensure!(
                        value.get("data").is_none(),
                        "query produced data before its original deadline"
                    );
                    page = actor.get(uri)?;
                    live_uri = page.next_uri()?;
                } else {
                    require_error(
                        &page,
                        (131075, "EXCEEDED_TIME_LIMIT", "INSUFFICIENT_RESOURCES"),
                    )?;
                    break;
                }
            }
            Ok(())
        })();
        let cleanup = cancel_live_trino_query(&actor, live_uri.as_deref());
        let convergence = await_resource_convergence(context, &baseline);
        if outcome.is_err() || cleanup.is_err() || convergence.is_err() {
            bail!(
                "Trino lifecycle timeout failed: outcome={outcome:?}, cleanup={cleanup:?}, convergence={convergence:?}"
            );
        }
        Ok(())
    }
}

fn cancel_live_trino_query(actor: &TrinoClient, live_uri: Option<&str>) -> Result<()> {
    if let Some(uri) = live_uri {
        let response = actor.delete(uri)?;
        ensure!(
            response.status == 204,
            "cleanup DELETE returned {}",
            response.status
        );
    }
    Ok(())
}

/// A read may replace its attempt only before it has made any result visible.
///
/// This case stops continuation requests after the first empty Trino page
/// until the selected BE has exited. A fresh task-create marker is the admission witness:
/// it identifies the process that accepted this exact in-flight attempt without
/// guessing a scheduler placement. The slow expression is workload, not the
/// oracle; the marker is what orders the kill after task admission.
#[derive(Default)]
struct NoEffectReadAfterBackendExit {
    intent: SqlIntentOwner,
}

impl Scenario for NoEffectReadAfterBackendExit {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "query_lifecycle",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "query-lifecycle/no-effect-read-after-backend-exit"
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        let statement = StatementSpec {
            sql: NID2_FENCE_QUERY.into(),
            faults: vec![],
            observations: (0..REQUIRED_BACKENDS)
                .flat_map(|index| {
                    [1, 2].map(move |attempt| (index, novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol.as_str(), Some(attempt)))
                })
                .collect(),
        };
        Ok(Some(self.intent.freeze(
            vec![statement],
            &[0, 1, 2],
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_snapshot(context)?;
        let create_counts = (0..context.handle().be_count())
            .map(|index| {
                context.handle().be_log_count(
                    index,
                    novarocks_failpoint::observation_marker::TASK_CREATE_APPLIED,
                )
            })
            .collect::<Result<Vec<_>>>()?;

        use super::paimon::scan_statement_actor::combine;

        let deadline = context.deadline();
        let actor = TrinoClient::new(
            context.trino_port(),
            context.sql_user(),
            deadline,
            &context.scenario_root().join("no-effect-recovery-http"),
        )?;
        let selected = self.intent.take(0)?;
        let post = actor.submit(NID2_FENCE_QUERY)?;
        let mut live = post.next_uri()?;
        let armed = (|| -> Result<_> {
            let value = post.assert_success()?;
            let id = value["id"]
                .as_str()
                .context("POST omitted query identity")?;
            ensure!(live.is_some(), "POST omitted first continuation");
            let observation = selected.arm_client_query(context, id, NID2_FENCE_QUERY)?;
            Ok((id.to_owned(), observation))
        })();
        let (id, observation) = match armed {
            Ok(armed) => armed,
            Err(error) => {
                return combine(Err(error), cancel_live_trino_query(&actor, live.as_deref()));
            }
        };

        let recovery = (|| -> Result<_> {
            let initial = actor.get(live.as_deref().context("missing first continuation")?)?;
            live = initial.next_uri()?;
            let value = initial.assert_success()?;
            ensure!(
                value["id"] == id && value.get("data").is_none() && live.is_some(),
                "first GET did not preserve the original running query without visible rows"
            );
            let target = await_fresh_task_create(context, &create_counts)?;
            context.action(format!(
                "observed a fresh TaskCreate for the original unread query on BE[{target}]"
            ));
            context
                .handle()
                .kill_be(target)
                .with_context(|| format!("kill admitted BE[{target}] without restarting it"))?;
            await_backend_exit(context, target)?;
            context.action(format!(
                "confirmed BE[{target}] exited before fetching any result rows"
            ));

            let mut rows = 0;
            while let Some(uri) = live.as_deref() {
                let page = actor.get(uri)?;
                let value = page.assert_success()?;
                ensure!(
                    value["id"] == id,
                    "recovery changed the original Trino query identity"
                );
                live = page.next_uri()?;
                if let Some(data) = value.get("data") {
                    let columns = &value["columns"];
                    ensure!(
                        columns.as_array().is_some_and(|columns| columns.len() == 1)
                            && columns[0]["name"] == "v"
                            && columns[0]["typeSignature"]["rawType"] == "boolean"
                            && columns[0]["typeSignature"]["arguments"] == serde_json::json!([]),
                        "recovered read changed its boolean result declaration"
                    );
                    for row in data.as_array().context("invalid recovered data array")? {
                        ensure!(
                            *row == serde_json::json!([true]),
                            "invalid recovered row: {row}"
                        );
                        rows += 1;
                    }
                }
                if live.is_none() {
                    ensure!(
                        value["stats"]["state"] == "FINISHED"
                            && value.get("data").is_none()
                            && value.get("updateType").is_none()
                            && value.get("updateCount").is_none(),
                        "recovered read did not reach a successful query terminal"
                    );
                }
            }
            ensure!(
                rows == 2,
                "recovered read returned {rows} rows instead of two"
            );
            let terminal = context
                .handle()
                .await_query_lifecycle_statement_observation(&observation, true, deadline)?;
            std::fs::write(
                context
                    .scenario_root()
                    .join("no-effect-recovery-terminal.txt"),
                format!("{terminal:#?}\n"),
            )?;
            Ok((target, terminal))
        })();
        let cancelled = cancel_live_trino_query(&actor, live.as_deref());
        let drained = context
            .handle()
            .drain_query_lifecycle_statement_binding(&observation, deadline);
        let (target, terminal) = combine(recovery, combine(cancelled, drained))?;
        ensure!(
            terminal.attempt_id == 2,
            "no-effect read after BE exit must complete on replacement attempt 2, got attempt {}",
            terminal.attempt_id
        );
        let participants = task_evidence::assert_query_completed_across_boundary(
            context,
            &terminal,
            "no-effect read after BE exit",
        )?;
        ensure!(
            !participants.contains(&target),
            "replacement attempt unexpectedly completed through exited BE[{target}]: {participants:?}"
        );
        context.action(format!(
            "verified unread no-effect read completed as attempt=2 on remaining task participants {participants:?}"
        ));

        await_resource_convergence(context, &baseline)?;
        let deadline = context.deadline();
        context
            .handle()
            .restart_be_until(target, deadline)
            .with_context(|| format!("restore BE[{target}] after no-restart recovery assertion"))?;
        context.action(format!(
            "restored BE[{target}] only after the no-restart recovery assertion completed"
        ));
        Ok(())
    }
}

/// One NID-2 identity fence, expressed as the wire value a backend misstates
/// and the frontend refusal that must follow.
struct Nid2Fence {
    /// The runner-owned fault kind, armed by name through the cluster harness.
    fault: &'static str,
    /// The marker that fault prints, token-scoped to one arming.
    marker: &'static str,
    /// What the misstated value was, for the accepted-action line.
    subject: &'static str,
    /// An exact typed preparation failure expected for this statement, if set.
    expected_failure_fact: Option<FailureFact>,
    /// Substrings the client-visible error must carry.
    ///
    /// Both halves matter. The canonical public code and safe detail must
    /// agree with the expected projection. A token-scoped backend marker and,
    /// where set, the exact statement failure additionally bind the fence.
    /// A bare `expect_err` would have been satisfied by any
    /// failure at all, including one the fault did not cause.
    error_fragments: &'static [&'static str],
}

/// A refused `CreateTask` answer for a task that really was admitted.
///
/// # Why the subject moved, and why it had to
///
/// This case was `nid-2-stage-conflict`. Its fault claimed the retired
/// protocol's `StageFragments` handler and rewrote a staged participant's
/// outcome to a stage conflict; no production query reached that handler on
/// the task path, so the fault was armed, nothing consumed it, the query
/// succeeded, and the case waited out its whole budget for a marker with no
/// emitter. The handler, and the `stage-conflict-after-apply` fault kind with
/// it, have since been deleted.
///
/// It was then `nid-2-create-conflict`, which forged the task protocol's
/// content-conflict verdict. A create replay is now decided by the exact task
/// identity and its lifecycle, never by comparing bodies, so that verdict no
/// longer exists and a forged one would assert a dead outcome.
///
/// The fence itself did not move far. `CreateTask` is the task protocol's
/// single per-task admission point, and `InvalidStateOrRequest` is still the
/// refusal a first creation receives when the backend rejects its body, so the
/// fault answers an admitted create with that verdict. The frontend half is
/// `RemoteTask::on_create_ack` reporting `CreateSettlement::FailedClosed` and
/// `QueryTaskExecution::acknowledge_task` turning it into
/// `TaskExecutionError::OperationFailed`
/// (`novarocks/frontend-application/src/task_execution/execution.rs`).
///
/// What is preserved verbatim is the "after apply" half, which is the whole
/// reason this case can fail: the task is admitted and running on that backend,
/// so a frontend that retried the refusal or ignored it would find working
/// state behind the lie and the statement would return rows.
#[derive(Default)]
struct Nid2CreateRejected {
    intent: SqlIntentOwner,
}

impl Scenario for Nid2CreateRejected {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "query_lifecycle",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "query-lifecycle/nid-2-create-rejected"
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
            vec![fault_statement(
                NID2_FENCE_QUERY,
                "create-task-rejected-after-apply",
            )],
            &[],
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        run_nid2_fence(
            context,
            &self.intent.take(0)?,
            &Nid2Fence {
                fault: "create-task-rejected-after-apply",
                marker: novarocks_failpoint::observation_marker::TASK_CREATE_REJECTED_AFTER_APPLY,
                subject: "a CreateTask rejection answered after the task was admitted",
                expected_failure_fact: None,
                error_fragments: &[
                    "[query.execution.invariant]",
                    "runner-owned CreateTask rejection",
                ],
            },
        )
    }
}

/// A `CreateTask` acknowledgement that names a task the frontend never asked
/// about.
///
/// # Why the digest this replaces has no counterpart
///
/// This case was `nid-2-start-digest-conflict`. Its fault flipped a bit of the
/// stage digest carried by the retired protocol's `StartPreparedQuery`, and
/// the fence it met was that protocol's backend registry refusing a start
/// whose digest was not the one it had staged. Both the operation and the
/// `start-digest-corrupt` fault kind have since been deleted.
///
/// That fence does not exist on the task protocol, and no substitute was
/// invented for it. There is no second operation that commits an already
/// staged plan --- a descriptor travels once, on `CreateTask` --- and a
/// create's static plan is interpreted only by the backend that wins that
/// identity's creation, so no field of a first delivery can be corrupted into
/// disagreeing with a plan the receiver already holds. A replay of an existing
/// identity is answered from that task's lifecycle and its body is never read,
/// so there is no content comparison left to corrupt either.
///
/// What survives as a distinct fence is the identity half. `TaskIdentity` is
/// indivisible, and an answer that names another task is refused rather than
/// adopted --- by `decode_create_task_ack` against the request's own identity
/// (`novarocks/proto-codec/src/task_execution/operation.rs`) and again by
/// `TaskIdentity::verify_matches` in `RemoteTask::on_create_ack`. So the
/// forgery is the acknowledgement's task id, and the direction of the
/// substitution flips with it: the corrupted value is now in the answer rather
/// than in the request, because a `CreateTask` request is a validated neutral
/// value whose identity and context are checked against each other at
/// construction and therefore cannot be built inconsistent.
///
/// A frontend that adopted the foreign receipt would mark this attempt's task
/// created and the statement would return rows, so the refusal is what the case
/// rests on.
#[derive(Default)]
struct Nid2CreateReceiptForeignTask {
    intent: SqlIntentOwner,
}

impl Scenario for Nid2CreateReceiptForeignTask {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "query_lifecycle",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "query-lifecycle/nid-2-create-receipt-foreign-task"
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
            vec![fault_statement(
                NID2_FENCE_QUERY,
                "create-task-receipt-foreign-task",
            )],
            &[],
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        run_nid2_fence(
            context,
            &self.intent.take(0)?,
            &Nid2Fence {
                fault: "create-task-receipt-foreign-task",
                marker: novarocks_failpoint::observation_marker::TASK_CREATE_RECEIPT_FOREIGN_TASK,
                subject: "a CreateTask acknowledgement naming a task the request never addressed",
                // A foreign Accepted receipt is refused by the frontend
                // decoder, not a Worker InvalidStateOrRequest verdict. Keep
                // the exact original protocol invariant on attempt 1.
                expected_failure_fact: Some(FailureFact::new(
                    FailureClass::Invariant,
                    FailureReason::new(domain::QUERY, std::num::NonZeroU16::new(9).unwrap()),
                )),
                error_fragments: &[
                    "[query.execution.invariant]",
                    "frontend task protocol could not continue",
                ],
            },
        )
    }
}

/// A delivered task status event that names a foreign backend process.
///
/// # Why the subject moved, and why the verdict changed with it
///
/// This case was `nid-2-post-init-foreign-ref`. Its fault replaced the
/// participant reference of a fragment observation published on the retired
/// control stream, and the frontend dropped that observation before it could
/// reach mutable telemetry state --- so the old case asserted that the
/// statement *succeeded* while the forged reference was refused. That stream,
/// and the `observation-foreign-participant` fault kind with it, have since
/// been deleted.
///
/// The surviving observation channel is `SubscribeTaskStatus`. It names no
/// participant of its own, so the forgeable fact is the backend process inside
/// the event's `TaskIdentity`, and the fence is `observe_event`
/// (`novarocks/frontend-application/src/native/task_transport.rs`) comparing it against the
/// subscription's own query context.
///
/// The verdict is genuinely different, and this case now asserts the new one
/// rather than the old one: `SubscriptionState::ProcessMismatch` is fatal by
/// design, and `TaskRound::turn` reads it through `settled_fatally` and fails
/// the attempt with `TaskExecutionError::ParticipantUnobservable` naming the
/// backend. Asserting the old success would now be asserting the opposite of
/// what the protocol does.
///
/// The "never reached mutable state" half is still proved, just not from a
/// cluster: `observe_event` returns before `intake.publish`, and
/// `a_status_event_from_another_process_is_fatal` in that module asserts the
/// intake stayed empty. What only a cluster can show is that the fence fires at
/// all on a real subscription --- which is this case.
#[derive(Default)]
struct Nid2ForeignStatusProcess {
    intent: SqlIntentOwner,
}

impl Scenario for Nid2ForeignStatusProcess {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "query_lifecycle",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "query-lifecycle/nid-2-foreign-status-process"
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
            vec![fault_statement(
                NID2_FENCE_QUERY,
                "task-status-foreign-process",
            )],
            &[],
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        run_nid2_fence(
            context,
            &self.intent.take(0)?,
            &Nid2Fence {
                fault: "task-status-foreign-process",
                marker: novarocks_failpoint::observation_marker::TASK_STATUS_FOREIGN_PROCESS,
                subject: "a task status event naming a backend process other than its own",
                // QueryFailureReason::PreparationProtocol is the SEP-1
                // projection of a fatal non-transport participant violation.
                expected_failure_fact: Some(FailureFact::new(
                    FailureClass::Invariant,
                    FailureReason::new(domain::QUERY, std::num::NonZeroU16::new(9).unwrap()),
                )),
                error_fragments: &[
                    "[query.execution.invariant]",
                    "frontend task protocol could not continue",
                ],
            },
        )
    }
}

/// Drives one identity fence: arm, run, prove the refusal, disarm, converge.
///
/// The arming is cleared on every path, including a failed assertion, and that
/// is not tidiness. An arm the frontend has already bound to this attempt is
/// harmless -- its trigger names a dead execution identity and can never match
/// again -- but a case that fails before its statement is scheduled leaves
/// three *unbound* arms behind, and the scheduler binds whatever it finds to
/// the next attempt it plans. That would perturb an unrelated scenario, and the
/// failure would be attributed to it.
fn run_nid2_fence(
    context: &mut ScenarioContext,
    selected: &SelectedStatement,
    fence: &Nid2Fence,
) -> Result<()> {
    require_three_backends(context)?;
    let baseline = resource_snapshot(context)?;
    use super::paimon::scan_statement_actor::combine;

    let deadline = context.deadline();
    let actor = TrinoClient::new(
        context.trino_port(),
        context.sql_user(),
        deadline,
        &context.scenario_root().join("nid2-http"),
    )?;
    let post = actor.submit(NID2_FENCE_QUERY)?;
    let mut live = post.next_uri()?;
    let mut observation = None;
    let observed = (|| -> Result<_> {
        let submitted = post.assert_success()?;
        let id = submitted["id"]
            .as_str()
            .context("POST omitted query identity")?;
        ensure!(live.is_some(), "POST omitted first continuation");
        let armed = observation.insert(selected.arm_client_query(context, id, NID2_FENCE_QUERY)?);
        let tokens = vec![selected.token.clone(); REQUIRED_BACKENDS];
        context.action(format!(
            "armed NID-2 {} across every Backend participant with tokens {tokens:?}",
            fence.fault
        ));

        let error = loop {
            let page = actor.get(live.as_deref().context("missing NID-2 continuation")?)?;
            let value = page.json()?;
            ensure!(
                value["id"] == id,
                "NID-2 continuation changed query identity"
            );
            live = page.next_uri()?;
            if live.is_none() {
                let error =
                    require_error(&page, (65536, "GENERIC_INTERNAL_ERROR", "INTERNAL_ERROR"))?;
                ensure!(
                    error["stats"]["state"] == "FAILED",
                    "NID-2 terminal state changed"
                );
                break error["error"]["message"]
                    .as_str()
                    .context("NID-2 refusal omitted its message")?
                    .to_owned();
            }
            page.assert_success()?;
            ensure!(
                value.get("data").is_none(),
                "NID-2 exposed rows before its refusal"
            );
        };
        let backend = observe_nid2_fence(context, fence, &tokens, armed, &error)?;
        Ok((backend, error))
    })();
    let cancelled = cancel_live_trino_query(&actor, live.as_deref());
    let cleared = context
        .handle()
        .clear_query_lifecycle_faults()
        .with_context(|| format!("clear {} tokens", fence.fault));
    let drained = if let Some(observation) = observation {
        context
            .handle()
            .drain_query_lifecycle_statement_binding(&observation, deadline)
            .context("drain NID-2 statement observation")
    } else {
        Ok(())
    };
    let (backend, error) = combine(observed, combine(cancelled, combine(cleared, drained)))?;
    await_resource_convergence(context, &baseline)?;
    context.action(format!(
        "BE[{backend}] published {} and the frontend fenced {}: {error}",
        fence.marker, fence.subject
    ));
    Ok(())
}

/// Correlate the public refusal with the original Native fence observation.
fn observe_nid2_fence(
    context: &mut ScenarioContext,
    fence: &Nid2Fence,
    tokens: &[String],
    observation: &novarocks_cluster_harness::QueryLifecycleStatementObservation,
    error: &str,
) -> Result<usize> {
    for fragment in fence.error_fragments {
        ensure!(
            error.contains(fragment),
            "{} rejected the public query without naming its fence: expected {fragment:?} in {error}",
            fence.fault
        );
    }
    // Read after the refusal, and polled rather than sampled once. The marker
    // is printed before the answer that carries the forgery is sent, so it is
    // always already produced by now -- but backend output reaches the harness
    // through an asynchronous pump, so a single read can be a few milliseconds
    // early. The previous form read once and accepted any nonzero count, which
    // could be satisfied by an earlier scenario's marker in the same log.
    let backend = await_token_scoped_marker(context, fence.marker, tokens)?;
    if let Some(expected) = fence.expected_failure_fact {
        let deadline = context.deadline();
        let terminal = context
            .handle()
            .await_query_lifecycle_statement_observation(observation, false, deadline)
            .context("read exact NID-2 statement failure")?;
        ensure!(
            terminal.attempt_id == 1
                && terminal.failure_fact == Some(expected)
                && terminal.failure_site
                    == Some(novarocks_cluster_harness::QueryLifecycleFailureSite::Coordinator),
            "{} must retain the original attempt's exact preparation failure {expected:?}, got {terminal:?}",
            fence.fault,
        );
    }
    Ok(backend)
}

/// Waits until some backend logged `marker` carrying one of `tokens`.
///
/// The marker name and the token have to come from the same line, so the
/// evidence is one emission of this scenario's own arming rather than a marker
/// from an earlier case standing next to a token from this one.
pub(super) fn await_token_scoped_marker(
    context: &mut ScenarioContext,
    marker: &str,
    tokens: &[String],
) -> Result<usize> {
    let needles = tokens
        .iter()
        .map(|token| format!("token={token}"))
        .collect::<Vec<_>>();
    loop {
        for backend_index in 0..context.handle().be_count() {
            let log = context
                .handle()
                .be_log_contents(backend_index)
                .with_context(|| format!("read BE[{backend_index}] log for {marker}"))?;
            if log.lines().any(|line| {
                line.contains(marker) && needles.iter().any(|needle| line.contains(needle))
            }) {
                return Ok(backend_index);
            }
        }
        let remaining = context.remaining(&format!(
            "observe {marker} carrying one of this arming's tokens {tokens:?}"
        ))?;
        thread::sleep(remaining.min(RESOURCE_POLL_INTERVAL));
    }
}

/// Finds the BE that admitted a task after this scenario captured its own log
/// counts. The marker is emitted only after the backend has accepted the
/// CreateTask operation, so a returned index is an actual participant rather
/// than a scheduler prediction.
pub(super) fn await_fresh_task_create(
    context: &mut ScenarioContext,
    baseline_counts: &[usize],
) -> Result<usize> {
    loop {
        for (index, baseline) in baseline_counts.iter().copied().enumerate() {
            let current = context
                .handle()
                .be_log_count(
                    index,
                    novarocks_failpoint::observation_marker::TASK_CREATE_APPLIED,
                )
                .with_context(|| format!("read TaskCreate evidence from BE[{index}]"))?;
            if current > baseline {
                return Ok(index);
            }
        }
        let remaining =
            context.remaining("observe a fresh TaskCreate admission for the unread read")?;
        thread::sleep(remaining.min(RESOURCE_POLL_INTERVAL));
    }
}

pub(super) fn await_backend_exit(context: &mut ScenarioContext, target: usize) -> Result<()> {
    loop {
        let snapshot = resource_snapshot(context)?;
        let backend = snapshot
            .backends
            .get(target)
            .with_context(|| format!("resource snapshot omitted BE[{target}]"))?;
        if !backend.process_running {
            return Ok(());
        }
        let remaining = context.remaining(&format!("observe BE[{target}] process exit"))?;
        thread::sleep(remaining.min(RESOURCE_POLL_INTERVAL));
    }
}

fn await_proxy_connection(
    context: &ScenarioContext,
    proxy: &novarocks_cluster_harness::native_fault_proxy::NativeFaultProxyControl,
    target: usize,
) -> Result<()> {
    loop {
        if proxy.active_connections() > 0 {
            return Ok(());
        }
        let remaining = context.remaining(&format!(
            "observe a live Native connection through BE[{target}] proxy"
        ))?;
        thread::sleep(remaining.min(RESOURCE_POLL_INTERVAL));
    }
}

fn await_backend_revoked_for_future_admission(
    context: &mut ScenarioContext,
    target: usize,
    expected_pid: u32,
) -> Result<()> {
    let advertised_port = context.handle().native_be_endpoint(target)?.port();
    loop {
        let snapshot = resource_snapshot(context)?;
        let backend = snapshot
            .backends
            .get(target)
            .with_context(|| format!("resource snapshot omitted BE[{target}]"))?;
        ensure!(
            backend.process_running,
            "BE[{target}] exited while its Native endpoint was partitioned"
        );
        ensure!(
            context.process_ids().backends[target] == expected_pid,
            "BE[{target}] process identity changed while awaiting transport revocation"
        );
        let still_eligible = context
            .handle()
            .frontend_backend_topology()?
            .iter()
            .any(|row| row.grpc_port == advertised_port && row.is_eligible_live());
        if !still_eligible {
            return Ok(());
        }
        let remaining = context.remaining(&format!(
            "observe FE revoke partitioned BE[{target}] from future admission"
        ))?;
        thread::sleep(remaining.min(RESOURCE_POLL_INTERVAL));
    }
}

/// The delayed read returns two `sleep(10)` values. We intentionally begin
/// reading only after the BE exit above, so this validates both result delivery
/// and that no result became visible before recovery was required.
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

pub(super) fn resource_snapshot(
    context: &mut ScenarioContext,
) -> Result<QueryExecutionResourceSnapshot> {
    context
        .handle()
        .query_execution_resource_snapshot()?
        .context("cross-process harness did not expose the query-resource oracle")
}

fn assert_baseline_rows(result: &StatementResult, ordinal: &str) -> Result<()> {
    ensure!(
        sql::integer_column(result)? == [1, 2],
        "{ordinal} distributed baseline returned unexpected rows: {:?}",
        result.rows
    );
    Ok(())
}

fn run_observed_baseline(
    context: &mut ScenarioContext,
    session: &mut SqlSession,
    selected: &SelectedStatement,
    ordinal: &str,
) -> Result<QueryLifecycleStructuredSnapshot> {
    use super::paimon::scan_statement_actor::combine;

    let deadline = context.deadline();
    let pending = session.submit(BASELINE_QUERY)?;
    let cancel = pending.cancel_handle();
    let (pending, observation) = selected.arm_pending(context, pending, BASELINE_QUERY)?;
    let result = session.run_pending(pending);
    let cancelled = if matches!(&result, Err(error) if !matches!(error, StatementFailure::Trino(_)))
    {
        cancel.delete(deadline).map_err(anyhow::Error::from)
    } else {
        Ok(())
    };
    let primary = (|| -> Result<_> {
        assert_baseline_rows(&result?, ordinal)?;
        context
            .handle()
            .await_query_lifecycle_statement_observation(&observation, true, deadline)
    })();
    let drained = context
        .handle()
        .drain_query_lifecycle_statement_binding(&observation, deadline);
    combine(primary, combine(cancelled, drained))
}

fn assert_process_attribution(snapshots: &[&QueryLifecycleStructuredSnapshot]) -> Result<()> {
    ensure!(
        !snapshots.is_empty(),
        "attribution requires at least one snapshot"
    );
    for pair in snapshots.windows(2) {
        let previous = pair[0];
        let next = pair[1];
        ensure!(
            previous.process_namespace == next.process_namespace,
            "consecutive baseline queries changed process namespace: previous=0x{:016x}, next=0x{:016x}",
            previous.process_namespace,
            next.process_namespace,
        );
        ensure!(
            previous.local_sequence.checked_add(1) == Some(next.local_sequence),
            "consecutive baseline query sequences are not adjacent: previous={}, next={}",
            previous.local_sequence,
            next.local_sequence,
        );
    }
    for (ordinal, snapshot) in snapshots.iter().enumerate() {
        let ordinal = ordinal + 1;
        ensure!(
            snapshot.attempt_id == 1,
            "baseline query {ordinal} used unexpected attempt id {}",
            snapshot.attempt_id
        );
        // Three participant-outcome assertions stood here: the list was not
        // empty, it held no more entries than the topology, and every entry
        // was a positive proof. All three are retired rather than restated,
        // because their subject no longer exists: the task protocol mints no
        // `ParticipantTerminalOutcome` at all, and the frontend deliberately
        // publishes an empty list instead of inventing proofs the protocol
        // never made (ADR-0135). What they were proving -- that a backend
        // really executed the attempt and its work completed -- is now proved
        // per query by `assert_query_completed_across_boundary`, from the
        // establish and release receipts and the frontend's own report that
        // every placed context answered.
    }
    ensure!(
        snapshots.len() >= 2,
        "attribution acceptance requires at least two consecutive queries"
    );
    Ok(())
}

fn assert_process_attribution_diagnostics(
    context: &mut ScenarioContext,
    snapshots: &[&QueryLifecycleStructuredSnapshot],
) -> Result<()> {
    let snapshot = snapshots
        .last()
        .context("attribution diagnostics require at least one snapshot")?;
    let namespace = format!("0x{:016x}", snapshot.process_namespace);
    let namespace_field = format!("query_process_namespace={namespace}");
    let startup_message = novarocks_failpoint::observation_marker::QUERY_PROCESS_NAMESPACE;
    let startup_count = context.handle().fe_log_count(startup_message)?;
    ensure!(
        startup_count == 1,
        "expected exactly one FE process namespace startup publication, found {startup_count}"
    );
    ensure!(
        context
            .handle()
            .fe_log_contents()?
            .contains(&namespace_field),
        "FE startup diagnostics did not publish {namespace_field}"
    );
    // A per-backend loop over `NOVAROCKS_QUERY_INIT_APPLIED` and
    // `query_process_namespace=<namespace>` stood here. Both were emitted only
    // by the retired lifecycle registry, so neither has a producer on the task
    // path, and neither can be restated as an all-three-backends fact: a query
    // context is established only where the scheduler placed a task, and this
    // baseline query -- constant rows, no scan, no splits -- places tasks on
    // one backend. The loop also matched the whole accumulated log, so it
    // never actually attributed a marker to this query.
    //
    // The replacement is scoped to the attempt instead of to the cluster.
    // Backend markers carry the execution identity, whose high and low halves
    // are the namespace and local sequence the frontend published, so an
    // establish admitted under this identity is the same cross-process
    // attribution fact the retired `namespace_field` assertion was making --
    // now provably about this query.
    //
    // Checked for every baseline query rather than only the last, because the
    // retired participant-outcome assertions it replaces also ran per query.
    let mut backends = BTreeSet::new();
    for (ordinal, snapshot) in snapshots.iter().enumerate() {
        backends.extend(task_evidence::assert_query_completed_across_boundary(
            context,
            snapshot,
            &format!("baseline query {}", ordinal + 1),
        )?);
    }
    context.action(format!(
        "verified one FE startup namespace publication for {namespace} and matching task-protocol \
         attribution on the backends that ran the attempts: {backends:?}"
    ));
    Ok(())
}

fn await_resource_activity(
    context: &mut ScenarioContext,
    baseline: &QueryExecutionResourceSnapshot,
) -> Result<()> {
    loop {
        if resource_snapshot(context)? != *baseline {
            return Ok(());
        }
        let remaining = context.remaining("observe in-flight distributed query resources")?;
        thread::sleep(remaining.min(RESOURCE_POLL_INTERVAL));
    }
}

pub(super) fn await_resource_convergence(
    context: &mut ScenarioContext,
    baseline: &QueryExecutionResourceSnapshot,
) -> Result<()> {
    let deadline = context.deadline();
    context
        .handle()
        .await_query_execution_resource_convergence(baseline, deadline)
        .context("await query-resource convergence after terminal lifecycle outcome")?;
    context.action("verified query resources converged after the terminal lifecycle outcome");
    Ok(())
}

fn bounded_io_timeout(context: &ScenarioContext, operation: &str) -> Result<Duration> {
    Ok(context.remaining(operation)?.min(IO_TIMEOUT_CAP))
}

#[cfg(test)]
mod ordinary_call_authorization_tests {
    use super::*;

    fn captures(capabilities: serde_json::Value) -> serde_json::Value {
        serde_json::json!([{"launch_capture":"fe.build-info.json", "build_info":{
            "schema_version":1, "package":"novarocks-server", "capabilities":[{
                "owner":"novarocks-frontend-application", "compiled_capabilities":capabilities
            }]
        }}])
    }

    #[test]
    fn ordinary_negative_call_uses_the_actual_frontend_capability_without_granting_mv() {
        assert!(
            ordinary_frontend_has_test_controls(&captures(serde_json::json!(["test-controls"])))
                .unwrap()
        );
        assert!(!ordinary_frontend_has_test_controls(&captures(serde_json::json!([]))).unwrap());
        assert!(ordinary_frontend_has_test_controls(&serde_json::json!([])).is_err());
        assert!(ordinary_frontend_has_test_controls(&captures(serde_json::json!([7]))).is_err());
        let duplicate = captures(serde_json::json!([]))[0].clone();
        assert!(
            ordinary_frontend_has_test_controls(&serde_json::json!([duplicate, duplicate]))
                .is_err()
        );
        let authorized_only = "MV test CALL requires explicit authorization";
        let uncompiled = "unsupported procedure novarocks_imv_stateless_rebuild";
        assert!(ordinary_call_refusal_matches(authorized_only, true));
        assert!(!ordinary_call_refusal_matches(uncompiled, true));
        assert!(ordinary_call_refusal_matches(uncompiled, false));
        assert!(!ordinary_call_refusal_matches(authorized_only, false));
        for controls in [true, false] {
            assert!(!ordinary_call_refusal_matches(
                "permission denied",
                controls
            ));
            assert!(!ordinary_call_refusal_matches(
                "unsupported unrelated_procedure",
                controls
            ));
        }
    }
}
