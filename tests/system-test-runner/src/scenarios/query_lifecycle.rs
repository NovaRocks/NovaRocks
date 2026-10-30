use crate::actors::mysql as mysql_actor;
use crate::actors::mysql_stream::MysqlStream;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};
use crate::scenarios::task_evidence;
use anyhow::{Context, Result, bail, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::{
    CrossProcessNativeFaultProxyConfig, QueryExecutionResourceSnapshot,
    QueryLifecycleStructuredSnapshot, ServerHandle,
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
            .map(|index| (index, "grpc-fragment", None))
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
        Box::new(MysqlDisconnect),
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
                context
                    .handle()
                    .be_log_count(index, "NOVAROCKS_TASK_CREATE_APPLIED")
            })
            .collect::<Result<Vec<_>>>()?;

        let old_connection = mysql_actor::connect_until(
            context.mysql_user(),
            context.mysql_port(),
            context.deadline(),
        )?;
        let old_observation =
            self.intent
                .take(0)?
                .arm(context, old_connection.connection_id(), NID2_FENCE_QUERY)?;
        let old_read = start_partitioned_read(old_connection)?;
        context.action(
            "started an unread distributed read whose admitted backend will be partitioned",
        );
        await_resource_activity(context, &baseline)?;
        let target = await_fresh_task_create(context, &create_counts)?;
        let process_before = context.process_ids().backends[target];
        let proxy = context
            .handle()
            .native_fault_proxy(target)
            .with_context(|| format!("obtain Native fault proxy for admitted BE[{target}]"))?;
        await_proxy_connection(context, &proxy, target)?;
        context.action(format!(
            "observed old attempt TaskCreate and a live Native connection on BE[{target}]"
        ));

        proxy.set_mode(ProxyDirection::ClientToUpstream, ProxyMode::Paused);
        proxy.set_mode(ProxyDirection::UpstreamToClient, ProxyMode::Paused);
        context.action(format!(
            "paused both Native transport directions for admitted, still-running BE[{target}]"
        ));
        await_backend_revoked_for_future_admission(context, target, process_before)?;
        context.action(format!(
            "confirmed BE[{target}] retained process identity {process_before} while FE revoked it for future admission"
        ));
        match old_read.done.try_recv() {
            Err(mpsc::TryRecvError::Empty) => {}
            Ok(outcome) => bail!(
                "old read became terminal while BE[{target}] remained partitioned: {outcome:?}"
            ),
            Err(mpsc::TryRecvError::Disconnected) => {
                bail!("old read actor disconnected while BE[{target}] remained partitioned")
            }
        }

        let mut independent = mysql_actor::connect_until(
            context.mysql_user(),
            context.mysql_port(),
            context.deadline(),
        )?;
        let independent_observation =
            self.intent
                .take(1)?
                .arm(context, independent.connection_id(), BASELINE_QUERY)?;
        execute_baseline_query(&mut independent, "post-partition independent")?;
        independent.finish()?;
        let deadline = context.deadline();
        let independent_terminal = context
            .handle()
            .await_query_lifecycle_statement_observation(
                &independent_observation,
                true,
                deadline,
            )?;
        context
            .handle()
            .drain_query_lifecycle_statement_observation(&independent_observation)?;
        let independent_participants = task_evidence::assert_query_completed_across_boundary(
            context,
            &independent_terminal,
            "post-partition independent read",
        )?;
        ensure!(
            !independent_participants.contains(&target),
            "fresh read used revoked BE[{target}]: {independent_participants:?}"
        );
        context.action(format!(
            "completed a new attempt on live participants {independent_participants:?}, excluding partitioned BE[{target}]"
        ));

        proxy.set_mode(ProxyDirection::ClientToUpstream, ProxyMode::Forward);
        proxy.set_mode(ProxyDirection::UpstreamToClient, ProxyMode::Forward);
        context.action(format!(
            "restored both Native transport directions for BE[{target}]"
        ));
        let old_rows = old_read
            .done
            .recv_timeout(context.remaining("await restored old read")?)
            .context("old read did not finish after Native transport restoration")??;
        old_read
            .thread
            .join()
            .map_err(|_| anyhow::anyhow!("old live-partition read actor panicked"))??;
        ensure!(
            old_rows == vec![1, 1],
            "restored old read returned unexpected rows: {old_rows:?}"
        );
        let deadline = context.deadline();
        let old_terminal = context
            .handle()
            .await_query_lifecycle_statement_observation(&old_observation, true, deadline)?;
        context
            .handle()
            .drain_query_lifecycle_statement_observation(&old_observation)?;
        ensure!(
            old_terminal.attempt_id == 1,
            "restored old read unexpectedly replaced its attempt: {}",
            old_terminal.attempt_id
        );
        ensure!(
            context.process_ids().backends[target] == process_before,
            "BE[{target}] process identity changed during a live transport partition"
        );
        context.action(format!(
            "restored old attempt completed without replacing BE[{target}] process identity"
        ));
        await_resource_convergence(context, &baseline)
    }
}

struct JoinedClientThread(Option<thread::JoinHandle<Result<()>>>);
impl JoinedClientThread {
    fn join(mut self) -> std::thread::Result<Result<()>> {
        self.0.take().expect("Native SQL client exists").join()
    }
}
impl Drop for JoinedClientThread {
    fn drop(&mut self) {
        if let Some(client) = self.0.take() {
            match client.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => eprintln!("Original SQL actor cleanup failed: {error:#}"),
                Err(_) => eprintln!("Original SQL actor panicked during actual join"),
            }
        }
    }
}

struct PendingPartitionedRead {
    thread: JoinedClientThread,
    done: mpsc::Receiver<Result<Vec<i64>, mysql::Error>>,
}

fn start_partitioned_read(
    connection: mysql_actor::DeadlineConnection,
) -> Result<PendingPartitionedRead> {
    let (done_tx, done) = mpsc::sync_channel(1);
    let thread = thread::Builder::new()
        .name("live-backend-partition-read".to_string())
        .spawn(move || -> Result<()> {
            connection.run(|connection| {
                let outcome = connection.query(NID2_FENCE_QUERY);
                done_tx
                    .send(outcome)
                    .context("publish live-partition read result")
            })
        })
        .context("start live-partition read actor")?;
    Ok(PendingPartitionedRead {
        thread: JoinedClientThread(Some(thread)),
        done,
    })
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
            let query = mysql_actor::connect_until(
                peer.mysql_user(),
                peer.runtime().fe_mysql_port,
                deadline,
            )
            .and_then(|connection| {
                connection.run(|connection| {
                    for level in ["package", "wipe"] {
                        let sql = format!(
                            "CALL test_control_probe.system.novarocks_imv_stateless_rebuild(table => 'ns.mv', level => '{level}')"
                        );
                        match connection.query_drop(&sql) {
                            Err(mysql::Error::MySqlError(error)) => {
                                ensure!(
                                    error.code == 1105
                                        && error.state == "HY000"
                                        && ordinary_call_refusal_matches(&error.message, controls_compiled),
                                    "ordinary test-only CALL returned another SQL failure: {error}"
                                );
                            }
                            Err(error) => {
                                return Err(anyhow::Error::new(error)
                                    .context("binary test-only CALL did not return a public SQL error"));
                            }
                            Ok(()) => bail!("binary artifact accepted test-only {level} CALL"),
                        }
                    }
                    for ordinal in ["binary first", "binary second", "binary third"] {
                        execute_baseline_query(connection, ordinal)?;
                    }
                    Ok(())
                })
            });
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
                        "ordinary artifact rejected package/wipe CALL through public SQL errors, then returned [1, 2] for all three baseline queries; its socket joined and query resources converged",
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
        let cleanup = ServerHandle::shutdown(&mut peer);
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

        let mut connection = mysql_actor::connect_until(
            context.mysql_user(),
            context.mysql_port(),
            context.deadline(),
        )?;
        context.action("connected baseline client through public MySQL protocol");

        let first_observation =
            self.intent
                .take(0)?
                .arm(context, connection.connection_id(), BASELINE_QUERY)?;
        execute_baseline_query(&mut connection, "first")?;
        let deadline = context.deadline();
        let first = context
            .handle()
            .await_query_lifecycle_statement_observation(&first_observation, true, deadline)?;
        context
            .handle()
            .drain_query_lifecycle_statement_observation(&first_observation)?;

        let second_observation =
            self.intent
                .take(1)?
                .arm(context, connection.connection_id(), BASELINE_QUERY)?;
        execute_baseline_query(&mut connection, "second")?;
        let deadline = context.deadline();
        let second = context
            .handle()
            .await_query_lifecycle_statement_observation(&second_observation, true, deadline)?;
        context
            .handle()
            .drain_query_lifecycle_statement_observation(&second_observation)?;
        let third_observation =
            self.intent
                .take(2)?
                .arm(context, connection.connection_id(), BASELINE_QUERY)?;
        execute_baseline_query(&mut connection, "third")?;
        let deadline = context.deadline();
        let third = context
            .handle()
            .await_query_lifecycle_statement_observation(&third_observation, true, deadline)?;
        context
            .handle()
            .drain_query_lifecycle_statement_observation(&third_observation)?;
        let snapshots = [&first, &second, &third];
        assert_process_attribution(&snapshots)?;
        assert_process_attribution_diagnostics(context, &snapshots)?;
        context.action(format!(
            "verified three native terminal snapshots share namespace=0x{:016x}, use consecutive sequence {}, {}, {}, and attempt=1",
            first.process_namespace,
            first.local_sequence,
            second.local_sequence,
            third.local_sequence,
        ));

        connection.finish()?;
        await_resource_convergence(context, &baseline)
    }
}

struct MysqlDisconnect;

impl Scenario for MysqlDisconnect {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "query_lifecycle",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "query-lifecycle/mysql-disconnect"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_snapshot(context)?;
        context.action("captured query-resource baseline");

        let stream = MysqlStream::query(
            context.mysql_user(),
            context.mysql_port(),
            "SELECT v FROM (SELECT sleep(10) AS v UNION ALL SELECT sleep(10)) t ORDER BY v",
            bounded_io_timeout(context, "open disconnecting MySQL client")?,
        )?;
        context.action("sent a blocking query through a raw public MySQL connection");
        await_resource_activity(context, &baseline)?;
        context.action("observed in-flight distributed query resources");

        stream.shutdown()?;
        context.action("closed the raw public MySQL client connection");

        await_resource_convergence(context, &baseline)
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

        let mut stream = MysqlStream::connect(
            context.mysql_user(),
            context.mysql_port(),
            bounded_io_timeout(context, "open timeout MySQL client")?,
        )?;
        context.action("connected timeout client through public MySQL protocol");
        stream.send_query("SET query_timeout = 1")?;
        stream.expect_ok_packet("SET query_timeout")?;
        context.action("set query_timeout = 1 through the public MySQL protocol");

        stream.send_query(
            "SELECT v FROM (SELECT sleep(10) AS v UNION ALL SELECT sleep(10)) t ORDER BY v",
        )?;
        context.action("sent a blocking query expected to time out");
        await_resource_activity(context, &baseline)?;
        context.action("observed in-flight distributed query resources before timeout");
        let error = stream.read_timeout_query_error()?;
        ensure!(
            error.contains("timed out") || error.contains("timeout"),
            "expected MySQL timeout error, got: {error}"
        );
        context.action(format!("received expected MySQL timeout error: {error}"));

        await_resource_convergence(context, &baseline)
    }
}

/// A read may replace its attempt only before it has made any result visible.
///
/// This case deliberately keeps the public MySQL stream unread until after the
/// selected BE has exited. A fresh task-create marker is the admission witness:
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
                    [1, 2].map(move |attempt| (index, "grpc-fragment", Some(attempt)))
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
                context
                    .handle()
                    .be_log_count(index, "NOVAROCKS_TASK_CREATE_APPLIED")
            })
            .collect::<Result<Vec<_>>>()?;

        let mut stream = MysqlStream::connect(
            context.mysql_user(),
            context.mysql_port(),
            context.remaining("open no-effect recovery MySQL client")?,
        )?;
        let observation =
            self.intent
                .take(0)?
                .arm(context, stream.connection_id()?, NID2_FENCE_QUERY)?;
        stream.send_query(NID2_FENCE_QUERY)?;
        context.action(
            "sent an unread no-effect distributed read through a raw public MySQL connection",
        );

        let target = await_fresh_task_create(context, &create_counts)?;
        context.action(format!(
            "observed a fresh TaskCreate admission for the unread read on BE[{target}]"
        ));
        context
            .handle()
            .kill_be(target)
            .with_context(|| format!("kill admitted BE[{target}] without restarting it"))?;
        await_backend_exit(context, target)?;
        context.action(format!(
            "confirmed BE[{target}] exited while the public client had not read a result"
        ));

        assert_two_sleep_rows(&mut stream)?;
        let deadline = context.deadline();
        let terminal = context
            .handle()
            .await_query_lifecycle_statement_observation(&observation, true, deadline)?;
        std::fs::write(
            context
                .scenario_root()
                .join("no-effect-recovery-terminal.txt"),
            format!("{terminal:#?}\n"),
        )?;
        context
            .handle()
            .drain_query_lifecycle_statement_observation(&observation)?;
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
                marker: "NOVAROCKS_TASK_CREATE_REJECTED_AFTER_APPLY",
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
                marker: "NOVAROCKS_TASK_CREATE_RECEIPT_FOREIGN_TASK",
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
                marker: "NOVAROCKS_TASK_STATUS_FOREIGN_PROCESS",
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
    let mut connection = mysql_actor::connect_until(
        context.mysql_user(),
        context.mysql_port(),
        context.deadline(),
    )?;
    let observation = selected.arm(context, connection.connection_id(), NID2_FENCE_QUERY)?;
    let tokens = vec![selected.token.clone(); REQUIRED_BACKENDS];
    context.action(format!(
        "armed NID-2 {} across every Backend participant with tokens {tokens:?}",
        fence.fault
    ));

    let observed = observe_nid2_fence(context, fence, &tokens, &observation, &mut connection);
    let cleared = context
        .handle()
        .clear_query_lifecycle_faults()
        .with_context(|| format!("clear {} tokens", fence.fault));
    let closed = connection.finish();
    let drained = context
        .handle()
        .drain_query_lifecycle_statement_observation(&observation)
        .context("drain NID-2 statement observation");
    let cleanup = crate::scenarios::paimon::scan_statement_actor::combine(
        crate::scenarios::paimon::scan_statement_actor::combine(cleared, closed),
        drained,
    );
    let (backend, error) =
        crate::scenarios::paimon::scan_statement_actor::combine(observed, cleanup)?;
    await_resource_convergence(context, &baseline)?;
    context.action(format!(
        "BE[{backend}] published {} and the frontend fenced {}: {error}",
        fence.marker, fence.subject
    ));
    Ok(())
}

/// Runs the query under one armed fence and returns the backend that fired
/// together with the client-visible refusal.
fn observe_nid2_fence(
    context: &mut ScenarioContext,
    fence: &Nid2Fence,
    tokens: &[String],
    observation: &novarocks_cluster_harness::QueryLifecycleStatementObservation,
    connection: &mut mysql::Conn,
) -> Result<(usize, String)> {
    let error = match connection.query::<i64, _>(NID2_FENCE_QUERY) {
        Ok(rows) => bail!(
            "{} was armed on every backend but the public query returned {rows:?} instead of \
             failing closed",
            fence.fault
        ),
        Err(error) => error.to_string(),
    };
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
    Ok((backend, error))
}

/// Select the finite all-backend fault set for this exact connected submission.
/// No trigger file or fresh token may nominate an unrelated next statement.
pub(super) fn arm_on_every_backend(
    context: &mut ScenarioContext,
    selected: &SelectedStatement,
    fault: &'static str,
    connection_id: u32,
    sql: &str,
) -> Result<novarocks_cluster_harness::QueryLifecycleStatementObservation> {
    ensure!(
        selected.faults.len() == REQUIRED_BACKENDS
            && (0..REQUIRED_BACKENDS).all(|index| selected.faults.contains(&(index, fault))),
        "all-backend arming differs from the exact finite fault set"
    );
    selected.arm(context, connection_id, sql)
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
                .be_log_count(index, "NOVAROCKS_TASK_CREATE_APPLIED")
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
pub(super) fn assert_two_sleep_rows(stream: &mut MysqlStream) -> Result<()> {
    let mut packets = Vec::with_capacity(6);
    for (offset, operation) in [
        "recovered read column count",
        "recovered read column definition",
        "recovered read metadata terminator",
        "recovered read first row",
        "recovered read second row",
        "recovered read terminal",
    ]
    .into_iter()
    .enumerate()
    {
        let packet = stream.read_packet(operation)?;
        ensure!(
            packet.sequence() == offset as u8 + 1,
            "recovered read packet {} had sequence {}, expected {}",
            offset + 1,
            packet.sequence(),
            offset + 1
        );
        // An ERR terminates the response; no later result packet will arrive.
        ensure!(
            !packet.is_error(),
            "recovered read packet {} was a MySQL error: {:?}",
            offset + 1,
            packet.payload()
        );
        packets.push(packet);
    }
    ensure!(
        packets[0].payload() == [1],
        "recovered read expected one result column, got {:?}",
        packets[0].payload()
    );
    ensure!(
        !packets[1].is_result_terminator(),
        "recovered read column definition was a terminator: {:?}",
        packets[1].payload()
    );
    ensure!(
        packets[2].is_result_terminator(),
        "recovered read metadata did not terminate: {:?}",
        packets[2].payload()
    );
    for (ordinal, packet) in packets[3..5].iter().enumerate() {
        ensure!(
            packet.payload() == [1, b'1'],
            "recovered read row {} expected sleep result 1, got {:?}",
            ordinal + 1,
            packet.payload()
        );
    }
    ensure!(
        packets[5].is_result_terminator() && !packets[5].has_more_results(),
        "recovered read did not end with one terminal success packet: {:?}",
        packets[5].payload()
    );
    Ok(())
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

pub(super) fn resource_snapshot(
    context: &mut ScenarioContext,
) -> Result<QueryExecutionResourceSnapshot> {
    context
        .handle()
        .query_execution_resource_snapshot()?
        .context("cross-process harness did not expose the query-resource oracle")
}

fn execute_baseline_query(connection: &mut mysql::Conn, ordinal: &str) -> Result<()> {
    let rows: Vec<i64> = connection
        .query(BASELINE_QUERY)
        .with_context(|| format!("execute {ordinal} distributed baseline query"))?;
    ensure!(
        rows == vec![1, 2],
        "{ordinal} distributed baseline query returned unexpected rows: {rows:?}"
    );
    Ok(())
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
    let startup_message = "NOVAROCKS_QUERY_PROCESS_NAMESPACE";
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
