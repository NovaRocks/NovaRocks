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

use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use novarocks_cluster_harness::TestClusterOwner;
use novarocks_proto_models::novarocks as proto;

use super::native_compatibility::raw_apply_task_operations_until;
use super::native_creation::{
    RawBackendSession, RawCreate, await_installed, ensure_accepted,
    observe_all_canceled_and_stopped, outcome, quiesce_session, raw_create_identity,
    receipt_verdict,
};
use super::native_direct_controls::{
    DirectIntent, DirectIntentOwner, close_hold, read_marker, release_hold,
};
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(PreparationCapacity {
            bytes: false,
            intent: DirectIntentOwner::default(),
        }),
        Box::new(PreparationCapacity {
            bytes: true,
            intent: DirectIntentOwner::default(),
        }),
        Box::new(PreparationInterleaving {
            same_batch: false,
            intent: DirectIntentOwner::default(),
        }),
        Box::new(PreparationInterleaving {
            same_batch: true,
            intent: DirectIntentOwner::default(),
        }),
    ]
}

struct PreparationCapacity {
    bytes: bool,
    intent: DirectIntentOwner,
}

impl Scenario for PreparationCapacity {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "preparation_capacity",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        if self.bytes {
            "native-creation/preparation-byte-capacity"
        } else {
            "native-creation/preparation-fifo-capacity"
        }
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        let targets = if self.bytes {
            (1..=32)
                .chain(std::iter::once(100))
                .map(|task| (0, 0, Some(task)))
                .collect::<Vec<_>>()
        } else {
            vec![
                (0, 0, Some(1)),
                (0, 0, Some(2)),
                (0, 1, Some(1)),
                (0, 0, Some(3)),
                (0, 2, Some(1)),
                (0, 1, Some(2)),
                (0, 2, Some(2)),
                (0, 2, Some(3)),
            ]
        };
        Ok(Some(self.intent.freeze(
            "preparation-hold",
            if self.bytes { 1 } else { 3 },
            &targets,
            true,
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn launch_config(&self, _root: &Path) -> Result<ScenarioLaunchConfig> {
        let mut launch = ScenarioLaunchConfig::default();
        let (per_participation, count, bytes) = if self.bytes {
            (32, 32, 2048)
        } else {
            (2, 3, 64 * 1024 * 1024)
        };
        launch.config_overlay.fe = Some("[runtime]\ntask_dispatch_create_permits = 2\n".to_owned());
        launch.config_overlay.be = Some(format!(
            "[runtime]\ntask_preparation_max_tasks_per_participation = {per_participation}\ntask_preparation_max_tasks = {count}\ntask_preparation_max_bytes = {bytes}\ntask_preparation_max_workers = 1\n"
        ));
        Ok(launch)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "preparation evidence requires native 1FE+3BE"
        );
        let intent = self.intent.get()?;
        ensure!(
            context.deadline() == intent.deadline,
            "Native preparation deadline changed after freeze"
        );
        let mut gate = PreparationRendezvous::bind(intent.token(0)?)?;
        if self.bytes {
            byte_capacity(context, &mut gate, &intent)
        } else {
            fifo_capacity(context, &mut gate, &intent)
        }
    }
}

struct PreparationInterleaving {
    same_batch: bool,
    intent: DirectIntentOwner,
}

impl Scenario for PreparationInterleaving {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "preparation_capacity",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        if self.same_batch {
            "native-creation/preparation-same-batch-fast-slow"
        } else {
            "native-creation/preparation-cross-backend-nonparking"
        }
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &Path,
        cluster_size: usize,
        binary: &Path,
        deadline: Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        let targets = if self.same_batch {
            vec![(0, 0, Some(1)), (0, 1, Some(2))]
        } else {
            vec![
                (0, 0, Some(1)),
                (1, 0, Some(2)),
                (1, 1, Some(1)),
                (0, 1, Some(2)),
            ]
        };
        Ok(Some(self.intent.freeze(
            "preparation-hold",
            2,
            &targets,
            true,
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn launch_config(&self, _root: &Path) -> Result<ScenarioLaunchConfig> {
        let mut launch = ScenarioLaunchConfig::default();
        launch.config_overlay.fe = Some("[runtime]\ntask_dispatch_create_permits = 2\n".into());
        launch.config_overlay.be = Some("[runtime]\ntask_preparation_max_tasks_per_participation = 2\ntask_preparation_max_tasks = 4\ntask_preparation_max_bytes = 67108864\ntask_preparation_max_workers = 2\n".into());
        Ok(launch)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        ensure!(
            context.handle().be_count() == 3,
            "preparation evidence requires native 1FE+3BE"
        );
        let intent = self.intent.get()?;
        ensure!(
            context.deadline() == intent.deadline,
            "Native preparation deadline changed after freeze"
        );
        let mut first = PreparationRendezvous::bind(intent.token(0)?)?;
        if self.same_batch {
            same_batch(context, &mut first, &intent)
        } else {
            let mut second = PreparationRendezvous::bind(intent.token(1)?)?;
            cross_backend(context, &mut first, &mut second, &intent)
        }
    }
}

fn establish_declared(
    context: &mut ScenarioContext,
    intent: &DirectIntent,
    query_index: usize,
    backend_index: usize,
) -> Result<RawBackendSession> {
    let query = intent
        .queries
        .get(query_index)
        .context("query outside frozen Native preparation set")?;
    let session = RawBackendSession::establish_declared(
        context,
        backend_index,
        query.query_id,
        query.frontend,
    )?;
    intent.verify_backend(backend_index, session.backend)?;
    Ok(session)
}

fn assert_still_preparing(session: &RawBackendSession, create: &RawCreate) -> Result<()> {
    let receipt = session.apply(create.operation(), "replay held preparation identity")?;
    ensure!(
        outcome(&receipt)? == proto::TaskOperationOutcome::Idempotent,
        "held entity was not replayed by identity: {receipt:?}"
    );
    let Some(proto::task_operation_verdict::Ack::CreateTask(ack)) =
        receipt_verdict(&receipt)?.ack.clone()
    else {
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
    intent: &DirectIntent,
) -> Result<()> {
    // Exercise both target orders. The later target finishes before the first
    // target releases its preparation worker, without bypassing either FIFO.
    for (query_index, order) in [[0, 1], [1, 0]].into_iter().enumerate() {
        let slow = establish_declared(context, intent, query_index, order[0])?;
        let fast = establish_declared(context, intent, query_index, order[1])?;
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

fn same_batch(
    context: &mut ScenarioContext,
    gate: &mut PreparationRendezvous,
    intent: &DirectIntent,
) -> Result<()> {
    // Worker intentionally serializes preparation within one Participation. Two
    // Contexts on the same BE allow its two workers to progress independently.
    let slow = establish_declared(context, intent, 0, 0)?;
    let fast = establish_declared(context, intent, 1, 0)?;
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
    let response = raw_apply_task_operations_until(
        &slow.connector,
        &slow.authorization,
        operations,
        intent.deadline,
    )?;
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
                && outcome(receipt)? == proto::TaskOperationOutcome::Accepted,
            "same ApplyBatch did not promptly accept the exact ordered item: {receipt:?}"
        );
        let Some(proto::task_operation_verdict::Ack::CreateTask(ack)) =
            &receipt_verdict(receipt)?.ack
        else {
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
            held_slow = Some(PreparationHold(Some(stream), context.deadline()));
        } else if marker == gate.marker(&fast, &fast_create) {
            ensure!(held_fast.is_none(), "fast preparation entered twice");
            held_fast = Some(PreparationHold(Some(stream), context.deadline()));
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

struct PreparationHold(Option<UnixStream>, Instant);
impl PreparationHold {
    fn release(&mut self) -> Result<()> {
        release_hold(
            self.0
                .as_mut()
                .context("preparation hold already released")?,
            self.1,
        )?;
        self.0 = None;
        Ok(())
    }
}
impl Drop for PreparationHold {
    fn drop(&mut self) {
        if let Some(stream) = &mut self.0 {
            close_hold(stream);
        }
    }
}

fn receiver(session: &RawBackendSession, id: u32) -> RawCreate {
    let mut create = RawCreate::values(session, id);
    create.pipeline_dop = 1;
    create.exchange_input = true;
    create
}

fn fifo_capacity(
    context: &mut ScenarioContext,
    gate: &mut PreparationRendezvous,
    intent: &DirectIntent,
) -> Result<()> {
    let a = establish_declared(context, intent, 0, 0)?;
    let b = establish_declared(context, intent, 1, 0)?;
    let a1 = receiver(&a, 1);
    let a2 = receiver(&a, 2);
    let b1 = receiver(&b, 1);
    ensure_accepted(&a, &a1)?;
    gate.await_entered(context, &a, &a1)?;
    ensure_accepted(&a, &a2)?;
    ensure!(
        outcome(&a.apply(receiver(&a, 3).operation(), "full Participation P")?)?
            == proto::TaskOperationOutcome::PreparationBusy,
        "full Participation preparation positions did not reject"
    );
    ensure_accepted(&b, &b1)?;
    ensure!(
        outcome(&b.apply(receiver(&b, 2).operation(), "full global preparation count")?)?
            == proto::TaskOperationOutcome::PreparationBusy,
        "full global preparation count did not reject"
    );
    let full = await_snapshot(context, 3, 2, 1)?;
    ensure!(
        full.participation_positions == 2
            && full.position_limit == 3
            && full.participation_limit == 2
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
    context.action("P=2/global=3/workers=1: full Participation and global positions rejected with PreparationBusy; exact rendezvous order A1,A2,B1 and owner gauges 3->2->1->0 proved FIFO and live Installed charge handoff; the previously Busy A3 was then Accepted while A1/A2 stayed live");

    let canceled = establish_declared(context, intent, 2, 0)?;
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
        )?)? == proto::TaskOperationOutcome::ParticipationTerminalReceipt,
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

fn byte_capacity(
    context: &mut ScenarioContext,
    gate: &mut PreparationRendezvous,
    intent: &DirectIntent,
) -> Result<()> {
    let session = establish_declared(context, intent, 0, 0)?;
    let first = receiver(&session, 1);
    ensure_accepted(&session, &first)?;
    gate.await_entered(context, &session, &first)?;
    let mut accepted = vec![first];
    loop {
        let next = receiver(&session, accepted.len() as u32 + 1);
        match outcome(&session.apply(next.operation(), "fill temporary preparation bytes")?)? {
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
            && full.participation_limit == 32
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
        outcome(&session.apply(oversized.operation(), "single oversized preparation")?)?
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
pub(super) struct Snapshot {
    pub(super) positions: usize,
    pub(super) position_limit: usize,
    pub(super) participation_positions: usize,
    pub(super) participation_limit: usize,
    pub(super) queued: usize,
    pub(super) workers: usize,
    pub(super) worker_limit: usize,
    pub(super) bytes: usize,
    pub(super) byte_limit: usize,
}

fn scrape(context: &mut ScenarioContext, index: usize) -> Result<Option<Snapshot>> {
    let port = context.handle().runtime().be[index].http;
    let body = reqwest::blocking::Client::builder()
        .timeout(
            context
                .remaining("read preparation owner metrics")?
                .min(Duration::from_secs(3)),
        )
        .build()?
        .get(format!("http://127.0.0.1:{port}/metrics"))
        .send()?
        .error_for_status()?
        .text()?;
    parse_snapshot(&body)
}

fn parse_snapshot(body: &str) -> Result<Option<Snapshot>> {
    let metric_sample = |name: &str| -> Result<usize> {
        let value = body
            .lines()
            .find_map(|line| {
                line.strip_prefix(name)
                    .and_then(|tail| tail.strip_prefix(' '))
            })
            .with_context(|| format!("missing owner sample {name}"))?;
        Ok(value.trim().parse()?)
    };
    match metric_sample("novarocks_backend_task_preparation_snapshot_available")? {
        0 => {
            ensure!(
                !body
                    .lines()
                    .any(|line| line.starts_with("novarocks_backend_task_preparation{")),
                "unavailable preparation snapshot published owner samples"
            );
            return Ok(None);
        }
        1 => {}
        value => bail!("invalid preparation snapshot availability: {value}"),
    }
    let sample = |resource: &str, dimension: &str| -> Result<usize> {
        metric_sample(&format!(
            "novarocks_backend_task_preparation{{dimension=\"{dimension}\",resource=\"{resource}\"}}"
        ))
    };
    Ok(Some(Snapshot {
        positions: sample("positions", "used")?,
        position_limit: sample("positions", "limit")?,
        participation_positions: sample("participation_positions", "used")?,
        participation_limit: sample("participation_positions", "limit")?,
        queued: sample("queued_positions", "used")?,
        workers: sample("workers", "used")?,
        worker_limit: sample("workers", "limit")?,
        bytes: sample("bytes", "used")?,
        byte_limit: sample("bytes", "limit")?,
    }))
}

fn await_snapshot(
    context: &mut ScenarioContext,
    positions: usize,
    queued: usize,
    workers: usize,
) -> Result<Snapshot> {
    await_snapshot_at(context, 0, positions, queued, workers)
}

pub(super) fn await_snapshot_at(
    context: &mut ScenarioContext,
    index: usize,
    positions: usize,
    queued: usize,
    workers: usize,
) -> Result<Snapshot> {
    loop {
        if let Some(snapshot) = scrape(context, index)?
            && (snapshot.positions, snapshot.queued, snapshot.workers)
                == (positions, queued, workers)
        {
            ensure!(
                snapshot.bytes <= snapshot.byte_limit
                    && snapshot.positions <= snapshot.position_limit
                    && snapshot.participation_positions <= snapshot.participation_limit
                    && snapshot.workers <= snapshot.worker_limit,
                "owner exceeded configured preparation charge limits: {snapshot:?}"
            );
            if positions == 0 {
                ensure!(
                    snapshot.bytes == 0 && snapshot.participation_positions == 0,
                    "exited jobs leaked temporary byte/Participation charges: {snapshot:?}"
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
    deadline: Option<Instant>,
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
            deadline: None,
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
        self.deadline = Some(context.deadline());
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
        let marker = read_marker(&mut stream, deadline)?;
        stream.set_write_timeout(Some(context.remaining("release preparation hold")?))?;
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
        release_hold(
            self.stream
                .as_mut()
                .context("preparation hold never entered")?,
            self.deadline
                .context("preparation hold lacks original deadline")?,
        )?;
        self.stream = None;
        Ok(())
    }
}
impl Drop for PreparationRendezvous {
    fn drop(&mut self) {
        if let Some(stream) = &mut self.stream {
            close_hold(stream);
        }
        let _ = fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod snapshot_tests {
    use super::parse_snapshot;

    #[test]
    fn unavailable_owner_snapshot_is_explicitly_pending() {
        assert!(
            parse_snapshot("novarocks_backend_task_preparation_snapshot_available 0\n")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn available_owner_snapshot_requires_owner_samples() {
        let error = parse_snapshot("novarocks_backend_task_preparation_snapshot_available 1\n")
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("missing owner sample novarocks_backend_task_preparation{")
        );
    }

    #[test]
    fn available_owner_snapshot_preserves_nonzero_samples_and_limits() {
        let snapshot = parse_snapshot(concat!(
            "novarocks_backend_task_preparation_snapshot_available 1\n",
            "novarocks_backend_task_preparation{dimension=\"used\",resource=\"positions\"} 3\n",
            "novarocks_backend_task_preparation{dimension=\"limit\",resource=\"positions\"} 7\n",
            "novarocks_backend_task_preparation{dimension=\"used\",resource=\"participation_positions\"} 2\n",
            "novarocks_backend_task_preparation{dimension=\"limit\",resource=\"participation_positions\"} 5\n",
            "novarocks_backend_task_preparation{dimension=\"used\",resource=\"queued_positions\"} 2\n",
            "novarocks_backend_task_preparation{dimension=\"used\",resource=\"workers\"} 1\n",
            "novarocks_backend_task_preparation{dimension=\"limit\",resource=\"workers\"} 4\n",
            "novarocks_backend_task_preparation{dimension=\"used\",resource=\"bytes\"} 1234\n",
            "novarocks_backend_task_preparation{dimension=\"limit\",resource=\"bytes\"} 8192\n"
        )).unwrap().expect("available exact owner snapshot");
        assert_eq!(snapshot.positions, 3);
        assert_eq!(snapshot.position_limit, 7);
        assert_eq!(snapshot.participation_positions, 2);
        assert_eq!(snapshot.participation_limit, 5);
        assert_eq!(snapshot.queued, 2);
        assert_eq!(snapshot.workers, 1);
        assert_eq!(snapshot.worker_limit, 4);
        assert_eq!(snapshot.bytes, 1234);
        assert_eq!(snapshot.byte_limit, 8192);
    }

    #[test]
    fn missing_or_invalid_availability_is_an_error() {
        assert!(parse_snapshot("").is_err());
        assert!(
            parse_snapshot("novarocks_backend_task_preparation_snapshot_available 2\n").is_err()
        );
    }

    #[test]
    fn unavailable_owner_snapshot_rejects_stale_samples() {
        assert!(
            parse_snapshot(concat!(
                "novarocks_backend_task_preparation_snapshot_available 0\n",
                "novarocks_backend_task_preparation{dimension=\"used\",resource=\"positions\"} 0\n"
            ))
            .is_err()
        );
    }
}
