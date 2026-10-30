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
        statement.observations = (0..REQUIRED_BACKENDS)
            .map(|index| (index, "grpc-fragment", None))
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
    let mut control = mysql_actor::connect_until(
        context.mysql_user(),
        context.mysql_port(),
        context.deadline(),
    )?;
    crate::scenarios::connector::create_catalog(&mut control, "cbw_catalog", &warehouse)?;
    control.query_drop("CREATE DATABASE cbw_catalog.cbw_db")?;
    control.query_drop("CREATE TABLE cbw_catalog.cbw_db.cbw_data (v BIGINT) TBLPROPERTIES ('novarocks.statistics.collect-on-write'='false')")?;
    for value in [1, 2, 3] {
        control.query_drop(format!(
            "INSERT INTO cbw_catalog.cbw_db.cbw_data VALUES ({value})"
        ))?;
    }
    control.finish()?;
    context.action("created three independent Iceberg scan files before arming exact Create holds");
    let baseline = resource_snapshot(context)?;
    let listener = HoldListener::bind(&selected.token, context.process_ids().backends.len())?;
    let connection = mysql_actor::connect_until(
        context.mysql_user(),
        context.mysql_port(),
        context.deadline(),
    )?;
    let observation = selected.arm(context, connection.connection_id(), QUERY)?;
    let (done_tx, done) = mpsc::sync_channel(1);
    let query = QueryRead(Some(thread::spawn(move || -> Result<()> {
        connection.run(|connection| {
            let rows = connection.query::<i64, _>(QUERY);
            done_tx.send(rows).context("publish LIMIT 0 result")
        })
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
        let root = held
            .iter()
            .find(|create| create.backend == root_backend && create.outbound_edges == 0)
            .expect("the root Create was required above");
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
        // Root creation is released only after the target partition is active,
        // so neither exact root Finished nor EOS can precede the disconnect.
        for create in &mut held {
            if create.backend != target {
                create.release()?;
            }
        }
        context.remaining("receive complete LIMIT 0 result with unknown sender")?;
        let deadline = context.deadline();
        let rows = loop {
            match done.try_recv() {
                Ok(rows) => {
                    break rows
                        .context("LIMIT 0 must seal while its unneeded Create is unknown")?;
                }
                Err(mpsc::TryRecvError::Disconnected) => bail!("LIMIT 0 client actor disconnected"),
                Err(mpsc::TryRecvError::Empty) => {}
            }
            while let Some(mut create) =
                listener.try_receive(&selected.token, context.deadline())?
            {
                if create.backend != target {
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
            rows.is_empty(),
            "LIMIT 0 returned unexpected rows: {rows:?}"
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
            !target_log
                .lines()
                .any(|line| line.contains("NOVAROCKS_TASK_CREATE_APPLIED")
                    && line.contains(&format!("{identity} "))),
            "held Create unexpectedly reached Worker before root success: {identity}"
        );
        let root_log = context.handle().be_log_contents(root_backend)?;
        ensure!(
            root_log
                .lines()
                .any(|line| line.contains("NOVAROCKS_GRPC_FETCH_TYPED")
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
    let joined = query.join();
    let cleanup =
        crate::scenarios::paimon::scan_statement_actor::combine(released_and_cleared, joined);
    let execution = crate::scenarios::paimon::scan_statement_actor::combine(result, cleanup)?;
    // The public answer above is the seal witness. Full background cleanup
    // evidence is read only after restoring transport and releasing every
    // pre-Worker hold, including requests that arrived after the root EOS.
    let terminal = context
        .handle()
        .await_query_lifecycle_statement_observation(&observation, true, selected.deadline)?;
    context
        .handle()
        .drain_query_lifecycle_statement_observation(&observation)?;
    ensure!(
        terminal.execution_id.as_deref() == Some(execution.as_str()) && terminal.attempt_id == 1,
        "LIMIT 0 cleanup must belong to the same frozen attempt: {terminal:?}"
    );
    await_resource_convergence(context, &baseline)?;
    Ok(())
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
