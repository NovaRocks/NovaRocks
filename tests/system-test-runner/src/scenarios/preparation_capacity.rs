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

//! Temporary preparation ownership through the authenticated native listener.

use std::fs;
use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail, ensure};
use novarocks_cluster_harness::ServerHandle;
use novarocks_proto_models::novarocks as proto;

use super::native_compatibility::raw_apply_task_operations;
use super::native_creation::{
    RawBackendSession, RawCreate, await_installed, ensure_accepted,
    observe_all_canceled_and_stopped, outcome, quiesce_session, raw_create_identity,
};
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(PreparationCapacity { bytes: false }),
        Box::new(PreparationCapacity { bytes: true }),
        Box::new(PreparationInterleaving { same_batch: false }),
        Box::new(PreparationInterleaving { same_batch: true }),
    ]
}

struct PreparationCapacity {
    bytes: bool,
}

impl Scenario for PreparationCapacity {
    fn name(&self) -> &'static str {
        if self.bytes {
            "native-creation/preparation-byte-capacity"
        } else {
            "native-creation/preparation-fifo-capacity"
        }
    }

    fn launch_config(&self, root: &Path) -> Result<ScenarioLaunchConfig> {
        fs::create_dir_all(root)?;
        let token = format!(
            "prep-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        );
        fs::write(root.join("preparation-hold-token"), &token)?;
        let mut launch = ScenarioLaunchConfig::default();
        launch
            .child_environment
            .be_by_index
            .entry(0)
            .or_default()
            .insert(
                "NOVAROCKS_SQL_TEST_TASK_PREPARATION_HOLD_TOKEN".into(),
                token,
            );
        let (per_context, count, bytes) = if self.bytes {
            (32, 32, 2048)
        } else {
            (2, 3, 64 * 1024 * 1024)
        };
        launch.config_overlay.fe = Some("[runtime]\ntask_dispatch_create_permits = 2\n".to_owned());
        launch.config_overlay.be = Some(format!(
            "[runtime]\ntask_preparation_max_tasks_per_context = {per_context}\ntask_preparation_max_tasks = {count}\ntask_preparation_max_bytes = {bytes}\ntask_preparation_max_workers = 1\n"
        ));
        Ok(launch)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "preparation evidence requires native 1FE+3BE"
        );
        let token = fs::read_to_string(context.scenario_root().join("preparation-hold-token"))?;
        let mut gate = PreparationRendezvous::bind(&token)?;
        if self.bytes {
            byte_capacity(context, &mut gate)
        } else {
            fifo_capacity(context, &mut gate)
        }
    }
}

struct PreparationInterleaving {
    same_batch: bool,
}

impl Scenario for PreparationInterleaving {
    fn name(&self) -> &'static str {
        if self.same_batch {
            "native-creation/preparation-same-batch-fast-slow"
        } else {
            "native-creation/preparation-cross-backend-nonparking"
        }
    }

    fn launch_config(&self, root: &Path) -> Result<ScenarioLaunchConfig> {
        fs::create_dir_all(root)?;
        let mut launch = ScenarioLaunchConfig::default();
        for index in 0..if self.same_batch { 1 } else { 2 } {
            let token = format!(
                "prep-mix-{}-{}-{index}",
                std::process::id(),
                SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
            );
            fs::write(root.join(format!("preparation-hold-token-{index}")), &token)?;
            launch
                .child_environment
                .be_by_index
                .entry(index)
                .or_default()
                .insert(
                    "NOVAROCKS_SQL_TEST_TASK_PREPARATION_HOLD_TOKEN".into(),
                    token,
                );
        }
        launch.config_overlay.fe = Some("[runtime]\ntask_dispatch_create_permits = 2\n".into());
        launch.config_overlay.be = Some("[runtime]\ntask_preparation_max_tasks_per_context = 2\ntask_preparation_max_tasks = 4\ntask_preparation_max_bytes = 67108864\ntask_preparation_max_workers = 2\n".into());
        Ok(launch)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "preparation evidence requires native 1FE+3BE"
        );
        let bind = |index| -> Result<PreparationRendezvous> {
            PreparationRendezvous::bind(&fs::read_to_string(
                context
                    .scenario_root()
                    .join(format!("preparation-hold-token-{index}")),
            )?)
        };
        let mut first = bind(0)?;
        if self.same_batch {
            same_batch(context, &mut first)
        } else {
            let mut second = bind(1)?;
            cross_backend(context, &mut first, &mut second)
        }
    }
}

fn assert_still_preparing(session: &RawBackendSession, create: &RawCreate) -> Result<()> {
    let receipt = session.apply(create.operation(), "replay held preparation identity")?;
    ensure!(
        outcome(&receipt) == proto::TaskOperationOutcome::Idempotent,
        "held entity was not replayed by identity: {receipt:?}"
    );
    let Some(proto::task_operation_receipt::Ack::CreateTask(ack)) = receipt.ack else {
        bail!("held replay omitted Create acknowledgement");
    };
    let status = ack.current_status.context("held replay omitted status")?;
    ensure!(
        status.installed == Some(false) && status.state == proto::TaskState::Planned as i32,
        "held slow task was installed or stopped: {status:?}"
    );
    Ok(())
}

fn cross_backend(
    context: &mut ScenarioContext,
    gate0: &mut PreparationRendezvous,
    gate1: &mut PreparationRendezvous,
) -> Result<()> {
    // Exercise both target orders. The later target finishes before the first
    // target releases its preparation worker, without bypassing either FIFO.
    for order in [[0, 1], [1, 0]] {
        let slow = RawBackendSession::establish(context, order[0])?;
        let fast = RawBackendSession::establish_same_attempt(context, order[1], &slow)?;
        let slow_create = receiver(&slow, 1);
        let fast_create = receiver(&fast, 2);
        let (slow_gate, fast_gate) = if order[0] == 0 {
            (&mut *gate0, &mut *gate1)
        } else {
            (&mut *gate1, &mut *gate0)
        };
        ensure_accepted(&slow, &slow_create)?;
        slow_gate.await_entered(context, &slow, &slow_create)?;
        ensure_accepted(&fast, &fast_create)?;
        fast_gate.await_entered(context, &fast, &fast_create)?;
        fast_gate.release()?;
        await_installed(context, &fast, &fast_create)?;
        await_snapshot_at(context, order[1], 0, 0, 0)?;
        assert_still_preparing(&slow, &slow_create)?;
        let held = await_snapshot_at(context, order[0], 1, 0, 1)?;
        ensure!(held.bytes > 0, "held first target lost its input charge");
        context.action(format!("S2 submit order BE{} -> BE{}: later exact Task {} Installed while first Task {} remained Preparing with one charged worker/position and {} bytes",
            order[0], order[1], fast_create.task_id, slow_create.task_id, held.bytes));
        slow_gate.release()?;
        await_installed(context, &slow, &slow_create)?;
        await_snapshot_at(context, order[0], 0, 0, 0)?;
        finish(context, &slow, &[slow_create])?;
        finish(context, &fast, &[fast_create])?;
    }
    gate0.ensure_no_pending()?;
    gate1.ensure_no_pending()?;
    Ok(())
}

fn same_batch(context: &mut ScenarioContext, gate: &mut PreparationRendezvous) -> Result<()> {
    // Worker intentionally serializes preparation within one Context. Two
    // Contexts on the same BE allow its two workers to progress independently.
    let slow = RawBackendSession::establish(context, 0)?;
    let fast = RawBackendSession::establish(context, 0)?;
    let slow_create = receiver(&slow, 1);
    let fast_create = receiver(&fast, 2);
    let operations = vec![slow_create.operation(), fast_create.operation()];
    let operation_ids = operations
        .iter()
        .map(|operation| {
            operation
                .envelope
                .as_ref()
                .and_then(|envelope| envelope.operation_id.clone())
        })
        .collect::<Vec<_>>();
    let response = raw_apply_task_operations(&slow.connector, &slow.authorization, operations)?;
    ensure!(
        response.grpc_status == 0,
        "same ApplyBatch failed at the Native boundary: {:?} {:?}",
        response.grpc_status,
        response.grpc_message
    );
    let response = response
        .message
        .context("same ApplyBatch omitted response body")?;
    ensure!(
        response.receipts.len() == 2,
        "same ApplyBatch omitted a receipt: {response:?}"
    );
    for ((receipt, operation_id), create) in response
        .receipts
        .iter()
        .zip(operation_ids)
        .zip([&slow_create, &fast_create])
    {
        ensure!(
            receipt.operation_id == operation_id
                && outcome(receipt) == proto::TaskOperationOutcome::Accepted,
            "same ApplyBatch did not promptly accept the exact ordered item: {receipt:?}"
        );
        let Some(proto::task_operation_receipt::Ack::CreateTask(ack)) = &receipt.ack else {
            bail!("same ApplyBatch omitted the Create acknowledgement");
        };
        ensure!(
            ack.identity == Some(raw_create_identity(create))
                && ack
                    .current_status
                    .as_ref()
                    .is_some_and(|status| status.installed == Some(false)),
            "batch acknowledgement changed identity or installed before held preparation: {ack:?}"
        );
    }
    let mut held_slow = None;
    let mut held_fast = None;
    for _ in 0..2 {
        let (marker, stream) = gate.capture(context)?;
        if marker == gate.marker(&slow, &slow_create) {
            ensure!(held_slow.is_none(), "slow preparation entered twice");
            held_slow = Some(PreparationHold(Some(stream)));
        } else if marker == gate.marker(&fast, &fast_create) {
            ensure!(held_fast.is_none(), "fast preparation entered twice");
            held_fast = Some(PreparationHold(Some(stream)));
        } else {
            bail!("same batch preparation entered an unrelated identity: {marker:?}");
        }
    }
    let mut held_slow = held_slow.context("slow batch item did not enter")?;
    let mut held_fast = held_fast.context("fast batch item did not enter")?;
    let full = await_snapshot(context, 2, 0, 2)?;
    ensure!(
        full.worker_limit == 2 && full.bytes > 0,
        "same batch did not use two charged workers: {full:?}"
    );
    held_fast.release()?;
    await_installed(context, &fast, &fast_create)?;
    assert_still_preparing(&slow, &slow_create)?;
    let remaining = await_snapshot(context, 1, 0, 1)?;
    ensure!(
        remaining.bytes > 0 && remaining.bytes < full.bytes,
        "fast exit returned the wrong input charges: full={full:?}, remaining={remaining:?}"
    );
    context.action(format!("S3 one authenticated ApplyBatch returned two ordered Accepted receipts before either held preparation exited; fast sibling Installed while slow remained Preparing, charged positions/workers 2->1 and bytes {}->{}", full.bytes, remaining.bytes));
    held_slow.release()?;
    await_installed(context, &slow, &slow_create)?;
    await_snapshot(context, 0, 0, 0)?;
    finish(context, &slow, &[slow_create])?;
    finish(context, &fast, &[fast_create])?;
    gate.ensure_no_pending()?;
    Ok(())
}

struct PreparationHold(Option<UnixStream>);
impl PreparationHold {
    fn release(&mut self) -> Result<()> {
        self.0
            .as_mut()
            .context("preparation hold already released")?
            .write_all(b"R")?;
        self.0 = None;
        Ok(())
    }
}
impl Drop for PreparationHold {
    fn drop(&mut self) {
        if let Some(stream) = &mut self.0 {
            let _ = stream.write_all(b"R");
        }
    }
}

fn receiver(session: &RawBackendSession, id: u32) -> RawCreate {
    let mut create = RawCreate::values(session, id);
    create.pipeline_dop = 1;
    create.exchange_input = true;
    create
}

fn fifo_capacity(context: &mut ScenarioContext, gate: &mut PreparationRendezvous) -> Result<()> {
    let a = RawBackendSession::establish(context, 0)?;
    let b = RawBackendSession::establish(context, 0)?;
    let a1 = receiver(&a, 1);
    let a2 = receiver(&a, 2);
    let b1 = receiver(&b, 1);
    ensure_accepted(&a, &a1)?;
    gate.await_entered(context, &a, &a1)?;
    ensure_accepted(&a, &a2)?;
    ensure!(
        outcome(&a.apply(receiver(&a, 3).operation(), "full Context P")?)
            == proto::TaskOperationOutcome::PreparationBusy,
        "full Context preparation positions did not reject"
    );
    ensure_accepted(&b, &b1)?;
    ensure!(
        outcome(&b.apply(receiver(&b, 2).operation(), "full global preparation count")?)
            == proto::TaskOperationOutcome::PreparationBusy,
        "full global preparation count did not reject"
    );
    let full = await_snapshot(context, 3, 2, 1)?;
    ensure!(
        full.context_positions == 2
            && full.position_limit == 3
            && full.context_limit == 2
            && full.worker_limit == 1
            && full.bytes > 0,
        "wrong configured owner view: {full:?}"
    );
    gate.release()?;
    gate.await_entered(context, &a, &a2)?;
    await_installed(context, &a, &a1)?;
    let after_first = await_snapshot(context, 2, 1, 1)?;
    ensure!(
        after_first.bytes < full.bytes,
        "Installed live receiver kept its temporary bytes"
    );
    gate.release()?;
    gate.await_entered(context, &b, &b1)?;
    await_installed(context, &a, &a2)?;
    await_snapshot(context, 1, 0, 1)?;
    gate.release()?;
    await_installed(context, &b, &b1)?;
    await_snapshot(context, 0, 0, 0)?;
    // Each Installed receiver is still blocked on its real exchange source.
    // Retry the earlier Busy identity while both predecessors remain live.
    let a3 = receiver(&a, 3);
    ensure_accepted(&a, &a3)?;
    gate.await_entered(context, &a, &a3)?;
    await_snapshot(context, 1, 0, 1)?;
    await_installed(context, &a, &a1)?;
    await_installed(context, &a, &a2)?;
    gate.release()?;
    await_installed(context, &a, &a3)?;
    await_snapshot(context, 0, 0, 0)?;
    finish(context, &a, &[a1, a2, a3])?;
    finish(context, &b, &[b1])?;
    context.action("P=2/global=3/workers=1: full Context and global positions rejected with PreparationBusy; exact rendezvous order A1,A2,B1 and owner gauges 3->2->1->0 proved FIFO and live Installed charge handoff; the previously Busy A3 was then Accepted while A1/A2 stayed live");

    let canceled = RawBackendSession::establish(context, 0)?;
    let first = receiver(&canceled, 1);
    let queued = receiver(&canceled, 2);
    ensure_accepted(&canceled, &first)?;
    gate.await_entered(context, &canceled, &first)?;
    ensure_accepted(&canceled, &queued)?;
    let held = await_snapshot(context, 2, 1, 1)?;
    let fence = quiesce_session(&canceled)?;
    let identities = vec![raw_create_identity(&first), raw_create_identity(&queued)];
    ensure!(
        fence.accepted_tasks == identities,
        "Quiesce omitted exact Accepted/queued membership"
    );
    let after_quiesce = await_snapshot(context, 2, 1, 1)?;
    ensure!(
        after_quiesce.bytes == held.bytes,
        "Quiesce returned held preparation bytes before real exit"
    );
    ensure!(
        outcome(&canceled.apply(
            receiver(&canceled, 3).operation(),
            "Create after held Quiesce"
        )?) == proto::TaskOperationOutcome::ContextTerminalReceipt,
        "Quiesce reopened preparation admission"
    );
    gate.release()?;
    await_snapshot(context, 0, 0, 0)?;
    gate.ensure_no_pending()?;
    observe_all_canceled_and_stopped(context, &canceled, &fence, &identities)?;
    canceled.release_when_ready(context)?;
    context.action("Quiesce during post-Accepted preparation retained exact P/count/bytes until job exit; queued canceled task never entered host; covered stream proved Canceled and actual_stopped for both identities before Release");
    Ok(())
}

fn byte_capacity(context: &mut ScenarioContext, gate: &mut PreparationRendezvous) -> Result<()> {
    let session = RawBackendSession::establish(context, 0)?;
    let first = receiver(&session, 1);
    ensure_accepted(&session, &first)?;
    gate.await_entered(context, &session, &first)?;
    let mut accepted = vec![first];
    loop {
        let next = receiver(&session, accepted.len() as u32 + 1);
        match outcome(&session.apply(next.operation(), "fill temporary preparation bytes")?) {
            proto::TaskOperationOutcome::Accepted => accepted.push(next),
            proto::TaskOperationOutcome::PreparationBusy => break,
            verdict => bail!("byte capacity returned {verdict:?}"),
        }
        ensure!(
            accepted.len() < 32,
            "independent count could explain byte rejection"
        );
    }
    let full = await_snapshot(context, accepted.len(), accepted.len() - 1, 1)?;
    ensure!(
        full.position_limit == 32
            && full.context_limit == 32
            && full.byte_limit == 2048
            && full.bytes > 0
            && full.bytes <= 2048
            && accepted.len() < 32,
        "byte probe did not isolate byte cap: {full:?}"
    );
    let mut oversized = receiver(&session, 100);
    oversized.initial_domains = vec![proto::TaskDomainUpdate {
        domain: Some(proto::task_domain_update::Domain::OpenExchangeEdges(
            proto::OpenExchangeEdgesDomain {
                version: 1,
                edge_ids: vec![u32::MAX; 256],
            },
        )),
    }];
    ensure!(
        outcome(&session.apply(oversized.operation(), "single oversized preparation")?)
            == proto::TaskOperationOutcome::ResourceExhausted,
        "single oversized preparation was reported as temporary Busy"
    );
    let fence = quiesce_session(&session)?;
    let identities = accepted.iter().map(raw_create_identity).collect::<Vec<_>>();
    ensure!(
        fence.accepted_tasks == identities,
        "byte gate admitted the rejected/oversized identity"
    );
    let canceled = await_snapshot(context, accepted.len(), accepted.len() - 1, 1)?;
    ensure!(
        canceled.bytes == full.bytes,
        "normal stop prematurely returned byte charges"
    );
    gate.release()?;
    await_snapshot(context, 0, 0, 0)?;
    gate.ensure_no_pending()?;
    observe_all_canceled_and_stopped(context, &session, &fence, &identities)?;
    session.release_when_ready(context)?;
    context.action(format!("temporary byte cap=2048 independently rejected with PreparationBusy at {} positions (<32), used={}<=2048; single oversized item ResourceExhausted; held cancellation preserved bytes until real exit, then exact covered stops and Release", accepted.len(), full.bytes));
    Ok(())
}

fn finish(
    context: &mut ScenarioContext,
    session: &RawBackendSession,
    creates: &[RawCreate],
) -> Result<()> {
    let fence = quiesce_session(session)?;
    let identities = creates.iter().map(raw_create_identity).collect::<Vec<_>>();
    ensure!(
        fence.accepted_tasks == identities,
        "normal receiver fence changed membership"
    );
    observe_all_canceled_and_stopped(context, session, &fence, &identities)?;
    session.release_when_ready(context)?;
    Ok(())
}

#[derive(Debug)]
struct Snapshot {
    positions: usize,
    position_limit: usize,
    context_positions: usize,
    context_limit: usize,
    queued: usize,
    workers: usize,
    worker_limit: usize,
    bytes: usize,
    byte_limit: usize,
}

fn scrape(context: &mut ScenarioContext, index: usize) -> Result<Snapshot> {
    let port = context.handle().runtime().be[index].http;
    let body = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()?
        .get(format!("http://127.0.0.1:{port}/metrics"))
        .send()?
        .error_for_status()?
        .text()?;
    let sample = |resource: &str, dimension: &str| -> Result<usize> {
        let name = format!(
            "novarocks_backend_task_preparation{{dimension=\"{dimension}\",resource=\"{resource}\"}}"
        );
        let value = body
            .lines()
            .find_map(|line| {
                line.strip_prefix(&name)
                    .and_then(|tail| tail.strip_prefix(' '))
            })
            .with_context(|| format!("missing owner sample {name}"))?;
        Ok(value.trim().parse()?)
    };
    Ok(Snapshot {
        positions: sample("positions", "used")?,
        position_limit: sample("positions", "limit")?,
        context_positions: sample("context_positions", "used")?,
        context_limit: sample("context_positions", "limit")?,
        queued: sample("queued_positions", "used")?,
        workers: sample("workers", "used")?,
        worker_limit: sample("workers", "limit")?,
        bytes: sample("bytes", "used")?,
        byte_limit: sample("bytes", "limit")?,
    })
}

fn await_snapshot(
    context: &mut ScenarioContext,
    positions: usize,
    queued: usize,
    workers: usize,
) -> Result<Snapshot> {
    await_snapshot_at(context, 0, positions, queued, workers)
}

fn await_snapshot_at(
    context: &mut ScenarioContext,
    index: usize,
    positions: usize,
    queued: usize,
    workers: usize,
) -> Result<Snapshot> {
    loop {
        let snapshot = scrape(context, index)?;
        if (snapshot.positions, snapshot.queued, snapshot.workers) == (positions, queued, workers) {
            ensure!(
                snapshot.bytes <= snapshot.byte_limit
                    && snapshot.positions <= snapshot.position_limit
                    && snapshot.context_positions <= snapshot.context_limit
                    && snapshot.workers <= snapshot.worker_limit,
                "owner exceeded configured preparation charge limits: {snapshot:?}"
            );
            if positions == 0 {
                ensure!(
                    snapshot.bytes == 0 && snapshot.context_positions == 0,
                    "exited jobs leaked temporary byte/Context charges: {snapshot:?}"
                );
            }
            return Ok(snapshot);
        }
        thread::sleep(
            context
                .remaining("await exact preparation owner conservation")?
                .min(Duration::from_millis(10)),
        );
    }
}

struct PreparationRendezvous {
    path: PathBuf,
    listener: UnixListener,
    stream: Option<UnixStream>,
    token: String,
}
impl PreparationRendezvous {
    fn bind(token: &str) -> Result<Self> {
        let path = novarocks_failpoint::native_registry_hold_socket_path(token)
            .map_err(anyhow::Error::msg)?;
        let listener = UnixListener::bind(&path)?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            path,
            listener,
            stream: None,
            token: token.into(),
        })
    }
    fn await_entered(
        &mut self,
        context: &mut ScenarioContext,
        session: &RawBackendSession,
        create: &RawCreate,
    ) -> Result<()> {
        let (marker, stream) = self.capture(context)?;
        ensure!(
            marker == self.marker(session, create),
            "preparation FIFO/identity changed: {marker:?}"
        );
        self.stream = Some(stream);
        Ok(())
    }
    fn marker(&self, session: &RawBackendSession, create: &RawCreate) -> String {
        format!(
            "NTP1 {} {} {} 1 1 {} {}\n",
            session.backend, session.query_id.hi, session.query_id.lo, create.task_id, self.token
        )
    }
    fn capture(&self, context: &mut ScenarioContext) -> Result<(String, UnixStream)> {
        let deadline = context
            .deadline()
            .min(Instant::now() + Duration::from_secs(10));
        let (mut stream, _) = loop {
            match self.listener.accept() {
                Ok(stream) => break stream,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    ensure!(
                        Instant::now() < deadline,
                        "post-Accepted preparation rendezvous did not enter"
                    );
                    thread::sleep(Duration::from_millis(1));
                }
                Err(error) => return Err(error.into()),
            }
        };
        stream.set_read_timeout(Some(Duration::from_secs(3)))?;
        let mut marker = String::new();
        stream.read_to_string(&mut marker)?;
        Ok((marker, stream))
    }

    fn ensure_no_pending(&self) -> Result<()> {
        match self.listener.accept() {
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(()),
            Ok(_) => bail!("a canceled queued preparation entered the Native host"),
            Err(error) => Err(error.into()),
        }
    }

    fn release(&mut self) -> Result<()> {
        self.stream
            .as_mut()
            .context("preparation hold never entered")?
            .write_all(b"R")?;
        self.stream = None;
        Ok(())
    }
}
impl Drop for PreparationRendezvous {
    fn drop(&mut self) {
        if let Some(stream) = &mut self.stream {
            let _ = stream.write_all(b"R");
        }
        let _ = fs::remove_file(&self.path);
    }
}
