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
use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::time::Instant;

const FAULT: &str = "create-task-before-worker-hold";
const QUERY: &str = "SELECT v FROM cbw_catalog.cbw_db.cbw_data LIMIT 0";

pub(super) struct LimitZeroUnknownSender;

impl Scenario for LimitZeroUnknownSender {
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

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        let result = run_fixture(context);
        let cleared = context.handle().clear_query_lifecycle_faults();
        result.and(cleared)
    }
}

fn run_fixture(context: &mut ScenarioContext) -> Result<()> {
    require_three_backends(context)?;
    let warehouse =
        crate::scenarios::connector::create_warehouse(context, "limit-zero-unknown-sender")?;
    let mut control = mysql_actor::connect(
        context.mysql_user(),
        context.mysql_port(),
        bounded_io_timeout(context, "connect LIMIT 0 fixture setup")?,
    )?;
    crate::scenarios::connector::create_catalog(&mut control, "cbw_catalog", &warehouse)?;
    control.query_drop("CREATE DATABASE cbw_catalog.cbw_db")?;
    control.query_drop("CREATE TABLE cbw_catalog.cbw_db.cbw_data (v BIGINT) TBLPROPERTIES ('novarocks.statistics.collect-on-write'='false')")?;
    for value in [1, 2, 3] {
        control.query_drop(format!(
            "INSERT INTO cbw_catalog.cbw_db.cbw_data VALUES ({value})"
        ))?;
    }
    context.action("created three independent Iceberg scan files before arming exact Create holds");
    let baseline = resource_snapshot(context)?;
    let before = latest_execution_id(context)?;
    let tokens = arm_on_every_backend(context, FAULT)?;
    let listeners = tokens
        .iter()
        .map(|token| HoldListener::bind(token))
        .collect::<Result<Vec<_>>>()?;
    let user = context.mysql_user().to_owned();
    let port = context.mysql_port();
    let connect_timeout = bounded_io_timeout(context, "connect LIMIT 0 read")?;
    let (done_tx, done) = mpsc::sync_channel(1);
    let query = QueryRead(Some(thread::spawn(move || -> Result<()> {
        let mut connection = mysql_actor::connect(&user, port, connect_timeout)?;
        let rows = connection.query::<i64, _>(QUERY);
        done_tx.send(rows).context("publish LIMIT 0 result")
    })));
    let mut held = Vec::<HeldCreate>::new();
    let result = (|| -> Result<String> {
        let (root_backend, target_index) = loop {
            for (backend, listener) in listeners.iter().enumerate() {
                while let Some(create) = listener.try_receive(backend, &tokens[backend])? {
                    context.action(format!("held exact pre-Worker Create: {}", create.payload));
                    held.push(create);
                }
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
        let deadline = Instant::now()
            + context.remaining("receive complete LIMIT 0 result with unknown sender")?;
        let rows = loop {
            match done.try_recv() {
                Ok(rows) => {
                    break rows
                        .context("LIMIT 0 must seal while its unneeded Create is unknown")?;
                }
                Err(mpsc::TryRecvError::Disconnected) => bail!("LIMIT 0 client actor disconnected"),
                Err(mpsc::TryRecvError::Empty) => {}
            }
            for (backend, listener) in listeners.iter().enumerate() {
                while let Some(mut create) = listener.try_receive(backend, &tokens[backend])? {
                    if backend != target {
                        create.release()?;
                    }
                    held.push(create);
                }
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
    // Disable future holds before releasing the observed requests. Listener
    // drop also releases pending connections and unlinks the socket, so a
    // late hook cannot remain parked while the bounded client is joined.
    let cleared = context.handle().clear_query_lifecycle_faults();
    let mut release_error = None;
    for create in &mut held {
        if let Err(error) = create.release() {
            release_error.get_or_insert(error);
        }
    }
    drop(listeners);
    let joined = query.join();
    let execution = result?;
    cleared?;
    if let Some(error) = release_error {
        return Err(error);
    }
    joined?;
    // The public answer above is the seal witness. Full background cleanup
    // evidence is read only after restoring transport and releasing every
    // pre-Worker hold, including requests that arrived after the root EOS.
    let terminal = await_terminal_snapshot(context, before.as_deref())?;
    ensure!(
        terminal.execution_id.as_deref() == Some(execution.as_str()) && terminal.attempt_id == 1,
        "LIMIT 0 cleanup must belong to the same frozen attempt: {terminal:?}"
    );
    await_resource_convergence(context, &baseline)?;
    Ok(())
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

struct HoldListener {
    path: PathBuf,
    listener: UnixListener,
}
impl HoldListener {
    fn bind(token: &str) -> Result<Self> {
        let path = novarocks_failpoint::create_before_worker_rendezvous_socket_path(token)
            .map_err(anyhow::Error::msg)?;
        let listener = UnixListener::bind(&path)?;
        listener.set_nonblocking(true)?;
        Ok(Self { path, listener })
    }
    fn try_receive(&self, backend: usize, token: &str) -> Result<Option<HeldCreate>> {
        let (mut stream, _) = match self.listener.accept() {
            Ok(value) => value,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        stream.set_read_timeout(Some(Duration::from_secs(1)))?;
        let mut payload = String::new();
        stream.read_to_string(&mut payload)?;
        let field = |name: &str| -> Result<String> {
            payload
                .split_whitespace()
                .find_map(|value| value.strip_prefix(&format!("{name}=")))
                .map(str::to_owned)
                .with_context(|| format!("missing {name} in held Create {payload:?}"))
        };
        ensure!(
            field("token")? == token && field("backend_index")?.parse::<usize>()? == backend,
            "held Create rendezvous identity differs from arming: {payload}"
        );
        let create = HeldCreate {
            backend,
            execution: field("execution_id")?,
            stage: field("stage")?,
            process: field("process_id")?,
            task: field("task")?,
            outbound_edges: field("outbound_edges")?.parse()?,
            payload,
            stream: Some(stream),
        };
        Ok(Some(create))
    }
}
impl Drop for HoldListener {
    fn drop(&mut self) {
        // Any connection that the main loop has not yet observed still owns
        // an exact parked Native request. Release it before unlinking.
        while let Ok((mut stream, _)) = self.listener.accept() {
            let _ = stream.set_read_timeout(Some(Duration::from_secs(1)));
            let mut payload = Vec::new();
            let _ = stream.read_to_end(&mut payload);
            let _ = stream.write_all(b"R");
        }
        let _ = std::fs::remove_file(&self.path);
    }
}
struct HeldCreate {
    backend: usize,
    execution: String,
    stage: String,
    task: String,
    process: String,
    outbound_edges: usize,
    payload: String,
    stream: Option<UnixStream>,
}
impl HeldCreate {
    fn identity_marker(&self) -> String {
        format!(
            "execution_id={} stage={} task={}",
            self.execution, self.stage, self.task
        )
    }
    fn release(&mut self) -> Result<()> {
        if let Some(mut stream) = self.stream.take() {
            stream.write_all(b"R")?;
        }
        Ok(())
    }
}
impl Drop for HeldCreate {
    fn drop(&mut self) {
        let _ = self.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Shutdown;

    #[test]
    fn listener_drop_releases_unobserved_parked_create_and_unlinks_socket() {
        let token = format!(
            "cbw-pending-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let listener = HoldListener::bind(&token).unwrap();
        let path = listener.path.clone();
        let mut backend = UnixStream::connect(&path).unwrap();
        backend.write_all(format!("token={token} execution_id=10:20:1 stage=2 task=3 backend_index=1 process_id=exact-process outbound_edges=1\n").as_bytes()).unwrap();
        backend.shutdown(Shutdown::Write).unwrap();
        drop(listener);
        let mut release = [0_u8; 1];
        backend.read_exact(&mut release).unwrap();
        assert_eq!(release, [b'R']);
        assert!(!path.exists());
    }

    #[test]
    fn held_create_rendezvous_preserves_identity_until_explicit_release() {
        let token = format!(
            "cbw-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let listener = HoldListener::bind(&token).unwrap();
        let mut backend = UnixStream::connect(&listener.path).unwrap();
        backend.write_all(format!(
            "token={token} execution_id=10:20:1 stage=2 task=3 backend_index=1 process_id=exact-process outbound_edges=1\n"
        ).as_bytes()).unwrap();
        backend.shutdown(Shutdown::Write).unwrap();
        let mut create = listener.try_receive(1, &token).unwrap().unwrap();
        assert_eq!(
            create.identity_marker(),
            "execution_id=10:20:1 stage=2 task=3"
        );
        assert_eq!(create.process, "exact-process");
        assert_eq!(create.outbound_edges, 1);
        create.release().unwrap();
        let mut release = [0_u8; 1];
        backend.read_exact(&mut release).unwrap();
        assert_eq!(release, [b'R']);
        let path = listener.path.clone();
        drop(listener);
        assert!(!path.exists());
    }
}
