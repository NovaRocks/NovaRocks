// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::scenarios::native_create_hold::{HeldCreate, HoldListener};
use std::time::Instant;

const FAULT: &str = "create-task-before-worker-hold";
const CREATE_ACK_DROP: &str = "create-task-ack-drop";
const QUERY: &str = "SELECT v FROM cbw_catalog.cbw_db.cbw_data LIMIT 0";

#[derive(Default)]
pub(super) struct LimitZeroUnknownSender {
    intent: SqlIntentOwner,
}

impl Scenario for LimitZeroUnknownSender {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "query_lifecycle",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "query-lifecycle/limit-zero-unknown-sender"
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn launch_config(&self, _: &std::path::Path) -> Result<crate::scenario::ScenarioLaunchConfig> {
        Ok(crate::scenario::ScenarioLaunchConfig {
            native_fault_proxies: CrossProcessNativeFaultProxyConfig {
                backend_retained_byte_limits: (0..REQUIRED_BACKENDS)
                    .map(|index| (index, 64 * 1024))
                    .collect(),
            },
            ..crate::scenarios::connector::connector_launch_config()
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
        let mut statement = fault_statement(QUERY, FAULT);
        statement
            .faults
            .extend((0..REQUIRED_BACKENDS).map(|index| (index, CREATE_ACK_DROP)));
        statement.observations = (0..REQUIRED_BACKENDS)
            .map(|index| {
                (
                    index,
                    novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol
                        .as_str(),
                    None,
                )
            })
            .collect();
        Ok(Some(self.intent.freeze(
            vec![statement],
            &[],
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        let result = run_fixture(context, &self.intent.take(0)?);
        let cleared = context.handle().clear_query_lifecycle_faults();
        crate::scenarios::paimon::scan_statement_actor::combine(result, cleared)
    }
}

fn run_fixture(context: &mut ScenarioContext, selected: &SelectedStatement) -> Result<()> {
    require_three_backends(context)?;
    let warehouse =
        crate::scenarios::connector::create_warehouse(context, "limit-zero-unknown-sender")?;
    let mut control = crate::actors::sql::SqlSession::new(
        context.sql_user(),
        context.trino_port(),
        context.deadline(),
    )?;
    crate::scenarios::connector::create_catalog(&mut control, "cbw_catalog", &warehouse)?;
    control.execute("CREATE DATABASE cbw_catalog.cbw_db")?;
    control.execute("CREATE TABLE cbw_catalog.cbw_db.cbw_data (v BIGINT) TBLPROPERTIES ('novarocks.statistics.collect-on-write'='false')")?;
    for value in [1, 2, 3] {
        control.execute(format!(
            "INSERT INTO cbw_catalog.cbw_db.cbw_data VALUES ({value})"
        ))?;
    }
    drop(control);
    context.action("created three independent Iceberg scan files before arming exact Create holds");
    let baseline = resource_snapshot(context)?;
    let listener = HoldListener::bind(&selected.token, context.process_ids().backends.len())?;
    let deadline = context.deadline();
    let mut session = SqlSession::new(context.sql_user(), context.trino_port(), deadline)?;
    let pending = session.submit(QUERY)?;
    let cancel = pending.cancel_handle();
    let (pending, observation) = selected.arm_pending(context, pending, QUERY)?;
    let (done_tx, done) = mpsc::sync_channel(1);
    let query = QueryRead(Some(thread::spawn(move || -> Result<()> {
        match done_tx.send(session.run_pending(pending)) {
            Ok(()) => Ok(()),
            Err(mpsc::SendError(Err(error))) => Err(error.into()),
            Err(mpsc::SendError(Ok(_))) => bail!("LIMIT 0 result receiver closed"),
        }
    })));
    let mut held = Vec::<HeldCreate>::new();
    let result = (|| -> Result<String> {
        let (root_backend, target_index) = loop {
            while let Some(create) = listener.try_receive(&selected.token, context.deadline())? {
                context.action(format!("held exact pre-Worker Create: {}", create.payload));
                held.push(create);
            }
            if let Some(root) = held.iter().find(|create| create.outbound_edges == 0)
                && let Some(target) = held.iter().position(|create| {
                    create.outbound_edges > 0
                        && create.backend != root.backend
                        && create.execution == root.execution
                })
            {
                break (root.backend, target);
            }
            match done.try_recv() {
                Err(mpsc::TryRecvError::Empty) => {}
                Ok(outcome) => bail!(
                    "LIMIT 0 returned before its frozen remote Create fixture existed: {outcome:?}"
                ),
                Err(mpsc::TryRecvError::Disconnected) => {
                    bail!("LIMIT 0 client disconnected before the held fixture existed")
                }
            }
            thread::sleep(
                context
                    .remaining("observe exact root and remote sender Creates")?
                    .min(RESOURCE_POLL_INTERVAL),
            );
        };
        let root_index = held
            .iter()
            .position(|create| create.backend == root_backend && create.outbound_edges == 0)
            .expect("the root Create was required above");
        let root = &held[root_index];
        let root_identity = root.identity_marker();
        let first_read_fields = [
            format!("stage={}", root.stage),
            format!("task={}", root.task),
            format!("backend={}", root.process),
        ];
        ensure!(
            first_frontend_root_read(&context.handle().fe_log_contents()?, &root.execution)?
                .is_none(),
            "FE attempted a Root read while its exact Create was held before Worker"
        );
        let root_execution_parts = root.execution.split(':').collect::<Vec<_>>();
        ensure!(
            root_execution_parts.len() == 3,
            "invalid exact root execution identity: {}",
            root.execution
        );
        let root_fetch_identity = format!(
            "query_hi={} query_lo={} attempt={} stage={} task={} backend={}",
            root_execution_parts[0],
            root_execution_parts[1],
            root_execution_parts[2],
            root.stage,
            root.task,
            root.process,
        );
        let target = held[target_index].backend;
        let identity = held[target_index].identity_marker();
        let execution = held[target_index].execution.clone();
        ensure!(
            execution.ends_with(":1"),
            "fixture must remain attempt 1: {execution}"
        );
        let process = context.handle().backend_process_id(target)?;
        let process_pid = context.process_ids().backends[target];
        ensure!(
            held[target_index].process == process.to_string(),
            "held Create process differs from frozen backend: {}",
            held[target_index].payload
        );
        let proxy = context.handle().native_fault_proxy(target)?;
        proxy.set_mode(ProxyDirection::ClientToUpstream, ProxyMode::Drop);
        proxy.set_mode(ProxyDirection::UpstreamToClient, ProxyMode::Drop);
        proxy.disconnect_all();
        let restore = RestoreProxy(proxy);
        context.action(format!(
            "partitioned live BE[{target}] process={process} with {identity} still held before Worker; root BE[{root_backend}] remains reachable"
        ));
        // Let only the first Root Create reach its Worker on that process.
        // The ACK drop then belongs to that operation, and a replay cannot
        // answer through the Worker before the first Root read.
        held[root_index].release()?;
        for create in &mut held {
            if create.backend != target && create.backend != root_backend {
                create.release()?;
            }
        }
        context.remaining("receive complete LIMIT 0 result with unknown sender")?;
        let deadline = context.deadline();
        let response = loop {
            match done.try_recv() {
                Ok(response) => {
                    break response
                        .context("LIMIT 0 must seal while its unneeded Create is unknown")?;
                }
                Err(mpsc::TryRecvError::Disconnected) => bail!("LIMIT 0 client actor disconnected"),
                Err(mpsc::TryRecvError::Empty) => {}
            }
            while let Some(mut create) =
                listener.try_receive(&selected.token, context.deadline())?
            {
                if create.backend != target && create.backend != root_backend {
                    create.release()?;
                }
                held.push(create);
            }
            ensure!(
                Instant::now() < deadline,
                "LIMIT 0 did not complete while exact unneeded Create remained unknown: {identity}"
            );
            thread::sleep(RESOURCE_POLL_INTERVAL);
        };
        ensure!(
            response.is_result_set()
                && response.columns.len() == 1
                && response.columns[0].name == "v"
                && response.columns[0].type_signature.raw_type == "bigint"
                && response.columns[0].type_signature.arguments.is_empty()
                && response.rows.is_empty(),
            "LIMIT 0 returned an unexpected typed result: {response:?}"
        );
        ensure!(
            context.process_ids().backends[target] == process_pid
                && context.handle().backend_process_id(target)? == process,
            "partition replaced the target process"
        );
        ensure!(
            resource_snapshot(context)?
                .backends
                .iter()
                .any(|backend| backend.index == target && backend.process_running),
            "target backend exited before root success evidence"
        );
        let target_log = context.handle().be_log_contents(target)?;
        ensure!(
            !target_log.lines().any(|line| line
                .contains(novarocks_failpoint::observation_marker::TASK_CREATE_APPLIED)
                && line.contains(&format!("{identity} "))),
            "held Create unexpectedly reached Worker before root success: {identity}"
        );
        let frontend_log = context.handle().fe_log_contents()?;
        let first_read = first_frontend_root_read(&frontend_log, &execution)?
            .context("the original FE root relay omitted its first-read observation")?;
        let fields = first_read.split_whitespace().collect::<Vec<_>>();
        ensure!(
            first_read_fields
                .iter()
                .all(|field| fields.contains(&field.as_str()))
                && fields.contains(&"wanted=1")
                && fields.contains(&"consumed=0"),
            "the first FE read differs from the exact frozen Root or initial frontier: {first_read}"
        );
        let installed_version = fields
            .iter()
            .find_map(|field| field.strip_prefix("installed_status_version="))
            .context("first FE Root read omitted the original Installed observation")?;
        ensure!(
            installed_version
                .parse::<u64>()
                .is_ok_and(|version| version > 0),
            "first FE Root read overtook its exact Host Installed observation: {first_read}"
        );
        context.action(format!(
            "first FE Root read used the installed original task: {first_read}"
        ));
        let root_marker_fields = format!("{root_identity} ");
        let root_log = loop {
            let log = context.handle().be_log_contents(root_backend)?;
            let ack_dropped = log.lines().any(|line| {
                line.contains(novarocks_failpoint::observation_marker::TASK_CREATE_ACK_DROPPED)
                    && line.contains(&root_marker_fields)
                    && line
                        .split_whitespace()
                        .any(|field| field.strip_prefix("token=") == Some(selected.token.as_str()))
            });
            if ack_dropped {
                break log;
            }
            thread::sleep(
                context
                    .remaining("observe the exact dropped Root Create ACK")?
                    .min(RESOURCE_POLL_INTERVAL),
            );
        };
        ensure!(
            !root_log.lines().any(|line| {
                line.contains(novarocks_failpoint::observation_marker::TASK_CREATE_IDEMPOTENT)
                    && line.contains(&root_marker_fields)
            }),
            "the Worker answered a replay of the exact Root Create before root success: {root_identity}"
        );
        context.action(format!(
            "the original Worker dropped the exact Root Create ACK while any replay remained held: {root_identity}"
        ));
        ensure!(
            root_log.lines().any(|line| line
                .contains(novarocks_failpoint::observation_marker::GRPC_FETCH_TYPED)
                && line.contains(&root_fetch_identity)
                && line.contains("eos=true")),
            "completed empty result omitted the exact root EOS evidence: {root_fetch_identity}"
        );
        context.action(format!("empty root result completed on exact attempt={execution} while {identity} remained unadmitted on live partitioned process={process}"));
        drop(restore);
        Ok(execution)
    })();
    // Keep observed peers alive until their release bytes are written. Ending
    // the action lets those peers close their sockets without reading a release.
    let released_and_cleared = release_observed_then_clear(&mut held, || {
        context.handle().clear_query_lifecycle_faults()
    });
    // The grant is now retired. Listener drop releases pending connections and
    // unlinks the socket before joining the bounded client.
    drop(listener);
    use super::super::paimon::scan_statement_actor::combine;
    let cancelled = if result.is_err() {
        cancel.delete(deadline).map_err(anyhow::Error::from)
    } else {
        Ok(())
    };
    let joined = query.join();
    let terminal = (|| -> Result<_> {
        let execution = combine(
            result,
            combine(released_and_cleared, combine(cancelled, joined)),
        )?;
        // The public answer above is the seal witness. Full background cleanup
        // evidence is read after restoring transport and releasing all holds.
        let terminal = context
            .handle()
            .await_query_lifecycle_statement_observation(&observation, true, deadline)?;
        Ok((execution, terminal))
    })();
    let drained = context
        .handle()
        .drain_query_lifecycle_statement_binding(&observation, deadline);
    let (execution, terminal) = combine(terminal, drained)?;
    ensure!(
        terminal.execution_id.as_deref() == Some(execution.as_str()) && terminal.attempt_id == 1,
        "LIMIT 0 cleanup must belong to the same frozen attempt: {terminal:?}"
    );
    await_resource_convergence(context, &baseline)?;
    Ok(())
}

fn first_frontend_root_read<'a>(log: &'a str, execution: &str) -> Result<Option<&'a str>> {
    let execution = format!("execution_id={execution}");
    let mut matching = log.lines().filter_map(|line| {
        let (_, fields) =
            line.split_once(novarocks_failpoint::observation_marker::FRONTEND_ROOT_FIRST_READ)?;
        let fields = fields.strip_prefix(' ')?;
        fields
            .split_whitespace()
            .any(|field| field == execution)
            .then_some(fields)
    });
    let first = matching.next();
    ensure!(
        matching.next().is_none(),
        "more than one first FE Root read was observed for {execution}"
    );
    Ok(first)
}

fn release_observed_then_clear(
    held: &mut [HeldCreate],
    clear: impl FnOnce() -> Result<()>,
) -> Result<()> {
    let mut released = Ok(());
    for create in held {
        released =
            crate::scenarios::paimon::scan_statement_actor::combine(released, create.release());
    }
    crate::scenarios::paimon::scan_statement_actor::combine(released, clear())
}

struct QueryRead(Option<thread::JoinHandle<Result<()>>>);
impl QueryRead {
    fn join(mut self) -> Result<()> {
        self.0
            .take()
            .expect("query thread exists")
            .join()
            .map_err(|_| anyhow::anyhow!("LIMIT 0 client panicked"))?
    }
}
impl Drop for QueryRead {
    fn drop(&mut self) {
        // Held Creates are declared later and release first during unwinding.
        // Socket I/O is bounded, and joining keeps no scenario client alive.
        if let Some(query) = self.0.take() {
            let _ = query.join();
        }
    }
}

struct RestoreProxy(novarocks_cluster_harness::native_fault_proxy::NativeFaultProxyControl);
impl Drop for RestoreProxy {
    fn drop(&mut self) {
        self.0
            .set_mode(ProxyDirection::ClientToUpstream, ProxyMode::Forward);
        self.0
            .set_mode(ProxyDirection::UpstreamToClient, ProxyMode::Forward);
    }
}
