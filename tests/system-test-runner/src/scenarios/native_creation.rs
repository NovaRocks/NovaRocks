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

//! Frozen task creation across the real 1FE+3BE process boundary.
//!
//! A create is decided by its exact task identity: the first legal round wins
//! and prepares the task, and every later request for that identity is
//! answered from the task that round created, whatever body it carries. The
//! frontend freezes each static plan once per statement, freezes a create's
//! metadata once when the create is first admitted, resends exactly those
//! bytes, and lets go of them once the create is answered.
//!
//! The backend half is observed through its own receipts and the create
//! markers it prints; the frontend half through the task-creation gauges it
//! exports, which fall only when a payload's last holder drops it.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use mysql::prelude::Queryable;
use novarocks_cluster_harness::ServerHandle;
use novarocks_native_trust::NativeEndpointConnector;
use novarocks_proto_models::{common, novarocks as proto, plan};
use novarocks_types::identity::{BackendProcessId, FrontendProcessId};

use crate::actors::mysql as mysql_actor;
use crate::actors::mysql_stream::MysqlStream;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};

use super::native_compatibility::{
    HEARTBEAT_PATH, RawUnaryResponse, authorization_header, decode_hex_32, only_successful_receipt,
    raw_acquire_admission_ticket, raw_apply_task_operations, raw_establish, raw_operation_envelope,
    raw_unary, raw_unary_response,
};
use super::query_lifecycle::{
    NID2_FENCE_QUERY, arm_on_every_backend, assert_two_sleep_rows, await_backend_exit,
    await_fresh_task_create, await_resource_convergence, await_terminal_snapshot,
    await_token_scoped_marker, latest_execution_id, resource_snapshot,
};

const REQUIRED_BACKENDS: usize = 3;
const BASELINE_QUERY: &str = "SELECT v FROM (SELECT 1 AS v UNION ALL SELECT 2) t ORDER BY v";
const POLL_INTERVAL: Duration = Duration::from_millis(50);
/// Normal context fencing and release are small control operations.
const CONTROL_PATH: &str = "/novarocks.NovaRocksGrpc/ApplyTaskControlOperations";

const CREATE_APPLIED: &str = "NOVAROCKS_TASK_CREATE_APPLIED";
const CREATE_IDEMPOTENT: &str = "NOVAROCKS_TASK_CREATE_IDEMPOTENT";
const CREATE_ACK_DROPPED: &str = "NOVAROCKS_TASK_CREATE_ACK_DROPPED";
const LEASE_RENEWED: &str = "NOVAROCKS_TASK_LEASE_RENEWED";

const STATIC_FRAGMENTS_FROZEN: &str = "novarocks_task_static_fragments_frozen_total";
const STATIC_FRAGMENTS_RETAINED: &str = "novarocks_task_static_fragments_retained";
const CREATES_PRICED: &str = "novarocks_task_creates_priced_total";
const CREATES_FROZEN: &str = "novarocks_task_creates_frozen_total";
const CREATE_PAYLOADS_RETAINED: &str = "novarocks_task_create_payloads_retained";

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(FrozenReplayAndMembership),
        Box::new(CreationPayloadLifetime),
        Box::new(FixedPlanRecovery),
        Box::new(NormalCloseCapacity {
            byte_limited: false,
        }),
        Box::new(NormalCloseCapacity { byte_limited: true }),
        Box::new(AcceptedPreparationControlRaces),
    ]
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

// ---------------------------------------------------------------------------
// native-creation/frozen-replay-and-membership
// ---------------------------------------------------------------------------

/// Same-identity creates across the real authenticated boundary.
///
/// One backend receives, over its own Native listener, a legal create, the
/// identical create again, and a create for the same identity whose every
/// body fact differs. The first wins; both replays are answered with the
/// winner's identity and a current status, without interpreting the replay a
/// second time. A request under another frontend process, another attempt or
/// another backend's identity never reads that receipt. A create whose initial
/// domain names a member its descriptor never froze is accepted, then fails
/// preparation while retaining its identity. After
/// the context is released, a replay still cannot bring a task back.
struct FrozenReplayAndMembership;

impl Scenario for FrozenReplayAndMembership {
    fn name(&self) -> &'static str {
        "native-creation/frozen-replay-and-membership"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let session = RawBackendSession::establish(context, 0)?;
        let applied = |context: &mut ScenarioContext| session.marker_count(context, CREATE_APPLIED);
        let idempotent =
            |context: &mut ScenarioContext| session.marker_count(context, CREATE_IDEMPOTENT);
        let renewed_before = context.handle().be_log_count(0, LEASE_RENEWED)?;
        context.action(format!(
            "established raw query context {} on BE[0] through its authenticated Native listener",
            session.execution_label()
        ));

        // The winner.
        let first = RawCreate::values(&session, 1);
        let accepted = session.apply(first.operation(), "first CreateTask")?;
        ensure!(
            outcome(&accepted) == proto::TaskOperationOutcome::Accepted,
            "the first legal create must win, got {accepted:?}"
        );
        let original = accepted
            .ack
            .clone()
            .context("the winning create returned no acknowledgement")?;
        ensure!(
            matches!(original, proto::task_operation_receipt::Ack::CreateTask(_)),
            "the winning create acknowledged something other than a task: {original:?}"
        );
        await_count(context, "the winner's create marker", 1, applied)?;
        context.action("the first legal create won and applied exactly one task on BE[0]");

        // The identical request, and the same identity with every body fact
        // changed: parallelism and its frozen domain, kernel key, instance
        // ordinal and sink.
        let exact = session.apply(first.operation(), "exact CreateTask replay")?;
        let mut variant = first.clone();
        variant.kernel_key_low = 2;
        variant.pipeline_dop = 3;
        variant.sink = plan::data_sink::Kind::Result(true);
        variant.instance_ordinal = 9;
        let changed = session.apply(variant.operation(), "changed-body CreateTask replay")?;
        for (label, replay) in [("exact", &exact), ("changed-body", &changed)] {
            ensure!(
                outcome(replay) == proto::TaskOperationOutcome::Idempotent,
                "an {label} replay of a created identity must be idempotent, got {replay:?}"
            );
            ensure!(
                replay
                    .ack
                    .as_ref()
                    .is_some_and(|ack| same_create_entity(&original, ack)),
                "an {label} replay must retain the winner's entity with a current status, got {replay:?}"
            );
        }
        await_count(context, "both replays' idempotent markers", 2, idempotent)?;
        ensure!(
            applied(context)? == 1,
            "a replay applied a second creation for one identity"
        );
        ensure!(
            context.handle().be_log_count(0, LEASE_RENEWED)? == renewed_before,
            "a create replay renewed the context lease"
        );
        context.action(
            "exact and changed-body replays retained the original entity; nothing was applied, interpreted or renewed again",
        );

        // Another scope never reads this identity's receipt.
        let mut foreign_frontend = first.clone();
        foreign_frontend.context = session.context_under(FrontendProcessId::new_v7(), 1);
        let mut next_attempt = first.clone();
        next_attempt.context = session.context_under(session.frontend, 2);
        let mut foreign_backend = first.clone();
        let stranger = BackendProcessId::new_v7();
        foreign_backend.backend_override = Some(stranger);
        let mut scope_outcomes = Vec::new();
        for (label, mut request) in [
            ("another frontend process", foreign_frontend),
            ("another attempt", next_attempt),
            ("another backend's identity", foreign_backend),
        ] {
            request.max_wait_millis = 50;
            let receipt = session.apply(request.operation(), label)?;
            let verdict = outcome(&receipt);
            ensure!(
                !matches!(
                    verdict,
                    proto::TaskOperationOutcome::Accepted | proto::TaskOperationOutcome::Idempotent
                ) && receipt.ack.is_none(),
                "a create under {label} read or created a task: {receipt:?}"
            );
            scope_outcomes.push(format!("{label}={verdict:?}"));
        }
        ensure!(
            applied(context)? == 1,
            "a create under another scope applied a task"
        );
        context.action(format!(
            "creates under other scopes read no receipt and applied nothing: {}",
            scope_outcomes.join(", ")
        ));

        // Membership is checked by the accepted preparation. Failure retains
        // the spent identity, so a later legal body cannot replace it.
        let mut refused = RawCreate::values(&session, 2);
        refused.initial_domains = vec![proto::TaskDomainUpdate {
            domain: Some(proto::task_domain_update::Domain::OpenExchangeEdges(
                proto::OpenExchangeEdgesDomain {
                    version: 1,
                    edge_ids: vec![7],
                },
            )),
        }];
        let refusal = session.apply(refused.operation(), "CreateTask naming an unfrozen edge")?;
        ensure!(
            outcome(&refusal) == proto::TaskOperationOutcome::Accepted,
            "the task is accepted before its initial domain is interpreted, got {refusal:?}"
        );
        await_count(
            context,
            "the failed preparation's terminal marker",
            2,
            |context| session.marker_count(context, "NOVAROCKS_TASK_TERMINAL_RETAINED"),
        )?;
        let mut legal = refused.clone();
        legal.initial_domains.clear();
        let won = session.apply(
            legal.operation(),
            "legal body replay after failed preparation",
        )?;
        ensure!(
            outcome(&won) == proto::TaskOperationOutcome::Idempotent
                && won.ack.as_ref().is_some_and(|ack| {
                    refusal
                        .ack
                        .as_ref()
                        .is_some_and(|first| same_create_entity(first, ack))
                }),
            "a failed accepted identity cannot be replaced by another body, got {won:?}"
        );
        ensure!(
            applied(context)? == 2,
            "the second identity must be accepted only once"
        );
        context.action(
            "an initial domain naming an unfrozen edge failed after acceptance, and the same identity remained spent",
        );

        // After release nothing is created again.
        let released = session.release_when_ready(context)?;
        let after = session.apply(first.operation(), "CreateTask replay after release")?;
        let verdict = outcome(&after);
        ensure!(
            verdict != proto::TaskOperationOutcome::Accepted,
            "a replay after release created a task again: {after:?}"
        );
        if verdict == proto::TaskOperationOutcome::Idempotent {
            ensure!(
                after
                    .ack
                    .as_ref()
                    .is_some_and(|ack| same_create_entity(&original, ack)),
                "a retained answer after release must name the original entity, got {after:?}"
            );
        }
        ensure!(
            applied(context)? == 2,
            "a replay after release applied a creation"
        );
        context.action(format!(
            "released the context ({released:?}); a later replay answered {verdict:?} and applied nothing"
        ));
        Ok(())
    }
}

/// One raw, authenticated Native session against one backend, holding an
/// established query context of its own.
pub(super) struct RawBackendSession {
    pub(super) index: usize,
    pub(super) connector: NativeEndpointConnector,
    pub(super) control_connector: NativeEndpointConnector,
    pub(super) authorization: String,
    pub(super) backend: BackendProcessId,
    pub(super) frontend: FrontendProcessId,
    pub(super) query_id: common::UniqueId,
    pub(super) establish_request: Option<proto::TaskOperation>,
}

impl RawBackendSession {
    pub(super) fn establish(context: &mut ScenarioContext, index: usize) -> Result<Self> {
        Self::establish_with_identity(context, index, None)
    }

    /// Establish another backend Context for the same frozen first attempt.
    pub(super) fn establish_same_attempt(
        context: &mut ScenarioContext,
        index: usize,
        owner: &Self,
    ) -> Result<Self> {
        Self::establish_with_identity(context, index, Some((owner.query_id, owner.frontend)))
    }

    fn establish_with_identity(
        context: &mut ScenarioContext,
        index: usize,
        shared: Option<(common::UniqueId, FrontendProcessId)>,
    ) -> Result<Self> {
        let port = context.handle().runtime().be[index].grpc;
        let rows = context.handle().frontend_backend_topology()?;
        let row = rows
            .iter()
            .find(|row| row.grpc_port == port)
            .with_context(|| format!("SHOW BACKENDS omitted BE[{index}]"))?;
        ensure!(
            row.is_eligible_live(),
            "BE[{index}] must be eligible and live, row={row:?}"
        );
        let backend = row
            .process_id
            .parse::<BackendProcessId>()
            .context("parse the target backend process identity")?;
        let compatibility = decode_hex_32(&row.native_compatibility_id)?;
        let endpoint = context.handle().native_be_endpoint(index)?;
        let mode = context.handle().native_trust_mode();
        let connector = context.handle().native_probe_connector(endpoint, mode)?;
        let control_endpoint = context.handle().native_be_control_endpoint(index)?;
        let control_connector = context
            .handle()
            .native_probe_connector(control_endpoint, mode)?;
        let trust = context.handle().native_probe_trust()?;
        let authorization = authorization_header(&trust)?;
        let heartbeat: proto::HeartbeatResponse = raw_unary(
            control_connector.clone(),
            HEARTBEAT_PATH,
            &authorization,
            proto::HeartbeatRequest {
                expected_process_id: Some(proto::BackendProcessId {
                    value: backend.to_bytes().to_vec(),
                }),
            },
        )?;
        let admission_epoch = heartbeat
            .admission_epoch_capability
            .context("the target heartbeat omitted its admission epoch capability")?;
        let mut session = Self {
            index,
            connector,
            control_connector,
            authorization,
            backend,
            frontend: FrontendProcessId::new_v7(),
            // A query id of this scenario's own, so every marker it counts is
            // this session's and no other case's.
            establish_request: None,
            query_id: common::UniqueId {
                hi: 0x5b5b,
                lo: {
                    static NEXT_SESSION: AtomicU64 = AtomicU64::new(1);
                    ((u64::from(std::process::id()) << 32)
                        | NEXT_SESSION.fetch_add(1, Ordering::Relaxed)) as i64
                },
            },
        };
        if let Some((query_id, frontend)) = shared {
            session.query_id = query_id;
            session.frontend = frontend;
        }
        let query_context = session.context_under(session.frontend, 1);
        let acquisition = session.apply(
            raw_acquire_admission_ticket(query_context.clone(), compatibility, admission_epoch),
            "admission ticket acquisition",
        )?;
        let Some(proto::task_operation_receipt::Ack::QueryContextAdmissionTicket(ticket)) =
            acquisition.ack
        else {
            bail!(
                "admission acquisition was not granted: {:?}",
                acquisition.outcome
            );
        };
        let ticket = ticket
            .ticket_id
            .context("the admission acknowledgement omitted its ticket id")?;
        let establish_request = raw_establish(query_context, ticket, Some(compatibility.to_vec()));
        let establish = session.apply(establish_request.clone(), "Establish")?;
        session.establish_request = Some(establish_request);
        ensure!(
            outcome(&establish) == proto::TaskOperationOutcome::Accepted,
            "the raw Establish must be accepted, got {establish:?}"
        );
        Ok(session)
    }

    /// This session's query context, under `frontend` and `attempt`.
    pub(super) fn context_under(
        &self,
        frontend: FrontendProcessId,
        attempt: u64,
    ) -> proto::QueryContextRef {
        proto::QueryContextRef {
            query_execution_id: Some(proto::QueryExecutionId {
                query_id: Some(self.query_id),
                attempt_id: attempt,
            }),
            frontend_process_id: Some(proto::FrontendProcessId {
                value: frontend.to_bytes().to_vec(),
            }),
            backend_process_id: Some(proto::BackendProcessId {
                value: self.backend.to_bytes().to_vec(),
            }),
        }
    }

    fn execution_label(&self) -> String {
        format!("{}:{}", self.query_id.hi, self.query_id.lo)
    }

    /// How many `marker` lines this backend printed for this session's first
    /// attempt.
    fn marker_count(&self, context: &mut ScenarioContext, marker: &str) -> Result<usize> {
        let needle = format!("{marker} execution_id={}:1 ", self.execution_label());
        context.handle().be_log_count(self.index, &needle)
    }

    pub(super) fn apply(
        &self,
        operation: proto::TaskOperation,
        subject: &str,
    ) -> Result<proto::TaskOperationReceipt> {
        only_successful_receipt(
            raw_apply_task_operations(&self.connector, &self.authorization, vec![operation])?,
            subject,
        )
    }

    /// Fences new identities, then releases once every local task is terminal.
    pub(super) fn release_when_ready(
        &self,
        context: &mut ScenarioContext,
    ) -> Result<proto::ReleaseQueryContextOutcome> {
        let quiesce: RawUnaryResponse<proto::ApplyTaskOperationsResponse> = raw_unary_response(
            &self.control_connector,
            CONTROL_PATH,
            &self.authorization,
            proto::ApplyTaskControlOperationsRequest {
                operations: vec![proto::TaskControlOperation {
                    envelope: Some(raw_operation_envelope(999)),
                    control: Some(proto::task_control_operation::Control::QuiesceQueryContext(
                        proto::QuiesceQueryContextRequest {
                            query_context: Some(self.context_under(self.frontend, 1)),
                        },
                    )),
                }],
            },
        )?;
        let receipt = only_successful_receipt(quiesce, "QuiesceQueryContext")?;
        let Some(proto::task_operation_receipt::Ack::QuiesceQueryContext(fence)) = receipt.ack
        else {
            bail!("normal context fence returned no Quiesce acknowledgement: {receipt:?}");
        };
        ensure!(
            fence.fence_version != 0,
            "normal context fence returned a zero version"
        );
        context.action(format!(
            "normal context fence froze {} accepted task identities",
            fence.accepted_tasks.len()
        ));
        loop {
            let response: RawUnaryResponse<proto::ApplyTaskOperationsResponse> =
                raw_unary_response(
                    &self.control_connector,
                    CONTROL_PATH,
                    &self.authorization,
                    proto::ApplyTaskControlOperationsRequest {
                        operations: vec![proto::TaskControlOperation {
                            envelope: Some(raw_operation_envelope(1_000)),
                            control: Some(
                                proto::task_control_operation::Control::ReleaseQueryContext(
                                    proto::ReleaseQueryContextRequest {
                                        query_context: Some(self.context_under(self.frontend, 1)),
                                    },
                                ),
                            ),
                        }],
                    },
                )?;
            let receipt = only_successful_receipt(response, "ReleaseQueryContext")?;
            let Some(proto::task_operation_receipt::Ack::ReleaseQueryContext(ack)) = receipt.ack
            else {
                bail!("the release returned no release acknowledgement: {receipt:?}");
            };
            let released = proto::ReleaseQueryContextOutcome::try_from(ack.outcome)
                .context("decode the release outcome")?;
            if released != proto::ReleaseQueryContextOutcome::NotReady {
                return Ok(released);
            }
            let remaining = context.remaining("release the raw query context")?;
            thread::sleep(remaining.min(POLL_INTERVAL));
        }
    }
}

/// Every fact one raw create carries, so a test can vary exactly one body
/// fact and keep the identity.
#[derive(Clone)]
pub(super) struct RawCreate {
    pub(super) context: proto::QueryContextRef,
    pub(super) task_id: u32,
    pub(super) backend_override: Option<BackendProcessId>,
    pub(super) kernel_key_low: i64,
    pub(super) pipeline_dop: u32,
    pub(super) sink: plan::data_sink::Kind,
    pub(super) instance_ordinal: u32,
    pub(super) initial_domains: Vec<proto::TaskDomainUpdate>,
    pub(super) max_wait_millis: u64,
    pub(super) exchange_input: bool,
}

impl RawCreate {
    /// A decodable, self-contained task: one VALUES node into a NOOP sink,
    /// frozen for exactly its own parallelism.
    pub(super) fn values(session: &RawBackendSession, task_id: u32) -> Self {
        Self {
            context: session.context_under(session.frontend, 1),
            task_id,
            backend_override: None,
            kernel_key_low: session
                .query_id
                .lo
                .wrapping_mul(1024)
                .wrapping_add(i64::from(task_id) * 100 + 1),
            pipeline_dop: 2,
            sink: plan::data_sink::Kind::Noop(true),
            instance_ordinal: 0,
            initial_domains: Vec::new(),
            max_wait_millis: 15_000,
            exchange_input: false,
        }
    }

    pub(super) fn operation(&self) -> proto::TaskOperation {
        use prost::Message;

        let execution = self
            .context
            .query_execution_id
            .expect("a raw context carries its execution");
        let mut context = self.context.clone();
        let backend = match self.backend_override {
            Some(backend) => {
                let backend = proto::BackendProcessId {
                    value: backend.to_bytes().to_vec(),
                };
                context.backend_process_id = Some(backend.clone());
                backend
            }
            None => context
                .backend_process_id
                .clone()
                .expect("a raw context carries its backend"),
        };
        let frozen = proto::FrozenFragment {
            package: Default::default(),
            plan_version: vec![0x5b; 16],
            plan_contract_revision: 1,
            fragment_contract_version: 1,
            pipeline_dop_domain: Some(proto::PipelineDopDomain {
                min: self.pipeline_dop,
                max: self.pipeline_dop,
                requires_power_of_two: false,
            }),
            plan: Some(plan::PlanFragment {
                fragment_id: 1,
                root: Some(plan::DistributedNode {
                    node_id: 10,
                    fragment_id: 1,
                    limit: -1,
                    payload: Some(if self.exchange_input {
                        plan::distributed_node::Payload::Exchange(plan::ExchangeReceiver {
                            partition_type: plan::PartitionType::Unpartitioned as i32,
                            source_fragment_id: 2,
                            flavor: Some(plan::ExchangeFlavor {
                                kind: Some(plan::exchange_flavor::Kind::Distribution(true)),
                            }),
                            ..Default::default()
                        })
                    } else {
                        plan::distributed_node::Payload::Physical(plan::PlanNode {
                            output_columns: Vec::new(),
                            kind: Some(plan::plan_node::Kind::Values(plan::ValuesNode {
                                rows: Vec::new(),
                                columns: Vec::new(),
                            })),
                        })
                    }),
                    ..Default::default()
                }),
                sink: Some(plan::DataSink {
                    kind: Some(self.sink.clone()),
                }),
                runtime_filter_bindings: Some(plan::RuntimeFilterBindingTable {
                    fragment_id: 1,
                    bindings: Vec::new(),
                }),
                ..Default::default()
            }),
        };
        let metadata = proto::CreationMetadata {
            query_context: Some(context),
            descriptor: Some(proto::TaskDescriptor {
                identity: Some(proto::TaskIdentity {
                    query_execution_id: Some(execution),
                    stage_id: 1,
                    task_id: self.task_id,
                    backend_process_id: Some(backend),
                }),
                fragment_instance_id: Some(common::UniqueId {
                    hi: 0x5b,
                    lo: self.kernel_key_low,
                }),
                pipeline_dop: self.pipeline_dop,
                split_plan_nodes: Vec::new(),
                topology: Some(proto::TaskExchangeTopology {
                    outbound: Vec::new(),
                    inbound: if self.exchange_input {
                        vec![proto::TaskExchangeInbound {
                            destination_node_id: 10,
                            sources: vec![proto::TaskExchangeSource {
                                task: Some(proto::TaskIdentity {
                                    query_execution_id: Some(execution),
                                    stage_id: 2,
                                    task_id: self.task_id,
                                    backend_process_id: self.context.backend_process_id.clone(),
                                }),
                                fragment_instance_id: Some(common::UniqueId {
                                    hi: 0x5c,
                                    lo: self.kernel_key_low,
                                }),
                                sender_ordinal: 0,
                            }],
                        }]
                    } else {
                        Vec::new()
                    },
                }),
            }),
            initial_domains: self.initial_domains.clone(),
            assignment: Some(proto::TaskAssignment {
                instance_ordinal: self.instance_ordinal,
                initial_scan_ranges: Vec::new(),
                sink_edge_ids: Vec::new(),
            }),
        };
        proto::TaskOperation {
            envelope: Some(raw_operation_envelope(self.max_wait_millis)),
            operation: Some(proto::task_operation::Operation::CreateTask(
                proto::CreateTaskRequest {
                    frozen_fragment: frozen.encode_to_vec().into(),
                    creation_metadata: metadata.encode_to_vec().into(),
                },
            )),
        }
    }
}

pub(super) fn outcome(receipt: &proto::TaskOperationReceipt) -> proto::TaskOperationOutcome {
    proto::TaskOperationOutcome::try_from(receipt.outcome)
        .unwrap_or(proto::TaskOperationOutcome::Unspecified)
}

fn same_create_entity(
    original: &proto::task_operation_receipt::Ack,
    replay: &proto::task_operation_receipt::Ack,
) -> bool {
    let (
        proto::task_operation_receipt::Ack::CreateTask(original),
        proto::task_operation_receipt::Ack::CreateTask(replay),
    ) = (original, replay)
    else {
        return false;
    };
    let (Some(first_status), Some(current_status)) = (
        original.current_status.as_ref(),
        replay.current_status.as_ref(),
    ) else {
        return false;
    };
    original.identity == replay.identity
        && original.accepted_domains == replay.accepted_domains
        && first_status.identity == current_status.identity
        && current_status.status_version >= first_status.status_version
}

/// Polls `count` until it reaches `expected`: backend output reaches the
/// harness through an asynchronous pump, so one read can be a little early.
fn await_count(
    context: &mut ScenarioContext,
    subject: &str,
    expected: usize,
    count: impl Fn(&mut ScenarioContext) -> Result<usize>,
) -> Result<()> {
    loop {
        let current = count(context)?;
        ensure!(
            current <= expected,
            "{subject}: observed {current}, more than the expected {expected}"
        );
        if current == expected {
            return Ok(());
        }
        let remaining = context.remaining(&format!("observe {subject}"))?;
        thread::sleep(remaining.min(POLL_INTERVAL));
    }
}

// ---------------------------------------------------------------------------
// native-creation/creation-payload-lifetime
// ---------------------------------------------------------------------------

/// Where the frontend's creation payloads are, through the real statement
/// path.
///
/// An exactly answered create releases its FE replay payload while the statement runs;
/// the statement's static plans stay until the statement ends. A create
/// whose acknowledgement is lost retains its payload until ownership is
/// proved by covered Installed/terminal facts or an exact replay; it is never
/// frozen or applied twice. A cancelled statement releases everything it froze.
struct CreationPayloadLifetime;

impl Scenario for CreationPayloadLifetime {
    fn name(&self) -> &'static str {
        "native-creation/creation-payload-lifetime"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_snapshot(context)?;
        let idle = FrontendCreationGauges::scrape(context)?;
        context.action(format!("idle frontend creation gauges: {idle:?}"));

        // Exact acknowledgements release the payload mid-statement.
        let pending = PendingRead::start(context, NID2_FENCE_QUERY)?;
        let mut peak_payloads = 0;
        loop {
            let now = FrontendCreationGauges::scrape(context)?;
            peak_payloads = peak_payloads.max(now.create_payloads_retained);
            if now.creates_frozen > idle.creates_frozen
                && now.create_payloads_retained == idle.create_payloads_retained
                && now.static_fragments_retained > idle.static_fragments_retained
            {
                ensure!(
                    pending.still_running(),
                    "the statement ended before its create payloads were observed released"
                );
                context.action(format!(
                    "while the statement still ran, every answered create released its payload \
                     (peak {peak_payloads} retained) and its static plans stayed retained ({} alive)",
                    now.static_fragments_retained
                ));
                break;
            }
            ensure!(
                pending.still_running(),
                "the statement ended before its create payloads were observed released: {now:?}"
            );
            let remaining =
                context.remaining("observe answered creates releasing their payloads")?;
            thread::sleep(remaining.min(POLL_INTERVAL));
        }
        let rows = pending.finish(context)?;
        ensure!(
            rows.len() == 2,
            "the delayed read returned {rows:?} instead of two rows"
        );
        await_idle_gauges(context, &idle, "a completed statement")?;
        context.action("the completed statement released its static plans");

        // A lost acknowledgement can settle through covered Installed/terminal
        // facts or an exact replay; nothing is frozen or applied twice.
        let before = FrontendCreationGauges::scrape(context)?;
        let applied_before = backend_marker_total(context, CREATE_APPLIED)?;
        let idempotent_before = backend_marker_total(context, CREATE_IDEMPOTENT)?;
        let tokens = arm_on_every_backend(context, "create-task-ack-drop")?;
        let result = run_baseline_query(context);
        let cleared = context
            .handle()
            .clear_query_lifecycle_faults()
            .context("clear create-task-ack-drop tokens");
        result?;
        cleared?;
        let dropped_on = await_token_scoped_marker(context, CREATE_ACK_DROPPED, &tokens)?;
        let after = FrontendCreationGauges::scrape(context)?;
        let applied = backend_marker_total(context, CREATE_APPLIED)? - applied_before;
        let replayed = backend_marker_total(context, CREATE_IDEMPOTENT)? - idempotent_before;
        let frozen = after.creates_frozen - before.creates_frozen;
        let priced = after.creates_priced - before.creates_priced;
        ensure!(
            frozen == i64::try_from(applied)? && priced == frozen,
            "every task must be priced, frozen and applied exactly once despite a lost \
             acknowledgement: priced={priced} frozen={frozen} applied={applied}"
        );
        await_idle_gauges(
            context,
            &idle,
            "a statement whose create acknowledgement was lost",
        )?;
        context.action(format!(
            "BE[{dropped_on}] dropped a create acknowledgement; ownership settled through covered \
             facts or exact replay, {replayed} replay(s) were answered idempotently, and \
             {applied} task(s) were priced, frozen and applied once each"
        ));

        // Cancellation releases everything the statement froze.
        let created_before = (0..context.handle().be_count())
            .map(|index| context.handle().be_log_count(index, CREATE_APPLIED))
            .collect::<Result<Vec<_>>>()?;
        let pending = PendingRead::start(context, NID2_FENCE_QUERY)?;
        await_fresh_task_create(context, &created_before)?;
        let deadline = context.deadline();
        context
            .handle()
            .kill_query_until(pending.connection_id, deadline)
            .context("cancel the delayed read")?;
        match pending.finish(context) {
            Ok(rows) => bail!("a cancelled statement returned {rows:?}"),
            Err(error) => {
                context.action(format!("the cancelled statement failed as asked: {error}"))
            }
        }
        await_idle_gauges(context, &idle, "a cancelled statement")?;
        await_resource_convergence(context, &baseline)?;
        context.action("the cancelled statement released every payload it froze");
        Ok(())
    }
}

/// The frontend's task-creation gauges and counters at one instant.
#[derive(Clone, Copy, Debug)]
struct FrontendCreationGauges {
    static_fragments_frozen: i64,
    static_fragments_retained: i64,
    creates_priced: i64,
    creates_frozen: i64,
    create_payloads_retained: i64,
}

impl FrontendCreationGauges {
    fn scrape(context: &mut ScenarioContext) -> Result<Self> {
        let port = context.handle().runtime().fe_http_port;
        let body = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()?
            .get(format!("http://127.0.0.1:{port}/metrics"))
            .send()
            .context("scrape FE /metrics")?
            .error_for_status()
            .context("FE /metrics status")?
            .text()
            .context("read FE /metrics")?;
        Ok(Self {
            static_fragments_frozen: metric(&body, STATIC_FRAGMENTS_FROZEN)?,
            static_fragments_retained: metric(&body, STATIC_FRAGMENTS_RETAINED)?,
            creates_priced: metric(&body, CREATES_PRICED)?,
            creates_frozen: metric(&body, CREATES_FROZEN)?,
            create_payloads_retained: metric(&body, CREATE_PAYLOADS_RETAINED)?,
        })
    }
}

/// The one unlabelled sample of `name` in a Prometheus text body.
fn metric(body: &str, name: &str) -> Result<i64> {
    let samples = body
        .lines()
        .filter(|line| !line.starts_with('#'))
        .filter_map(|line| {
            let (metric, value) = line.split_once(' ')?;
            (metric == name).then_some(value.trim())
        })
        .collect::<Vec<_>>();
    let [value] = samples.as_slice() else {
        bail!("FE /metrics must carry exactly one {name} sample, found {samples:?}");
    };
    let value = value
        .parse::<f64>()
        .with_context(|| format!("parse {name} sample {value:?}"))?;
    Ok(value as i64)
}

/// Waits until the frontend holds no more creation payload or static plan
/// than it did while idle.
fn await_idle_gauges(
    context: &mut ScenarioContext,
    idle: &FrontendCreationGauges,
    subject: &str,
) -> Result<()> {
    loop {
        let now = FrontendCreationGauges::scrape(context)?;
        if now.create_payloads_retained == idle.create_payloads_retained
            && now.static_fragments_retained == idle.static_fragments_retained
        {
            return Ok(());
        }
        let remaining = context.remaining(&format!(
            "observe {subject} releasing its payloads: {now:?}"
        ))?;
        thread::sleep(remaining.min(POLL_INTERVAL));
    }
}

fn backend_marker_total(context: &mut ScenarioContext, marker: &str) -> Result<usize> {
    let mut total = 0;
    for index in 0..context.handle().be_count() {
        total += context.handle().be_log_count(index, marker)?;
    }
    Ok(total)
}

fn run_baseline_query(context: &mut ScenarioContext) -> Result<()> {
    let mut connection = mysql_actor::connect(
        context.mysql_user(),
        context.mysql_port(),
        context.remaining("connect the baseline read")?,
    )?;
    let rows: Vec<i64> = connection
        .query(BASELINE_QUERY)
        .context("run the baseline read")?;
    ensure!(
        rows == vec![1, 2],
        "the baseline read returned {rows:?} instead of [1, 2]"
    );
    Ok(())
}

/// One statement running on its own connection, cancellable by id.
struct PendingRead {
    thread: thread::JoinHandle<Result<()>>,
    connection_id: u32,
    done: mpsc::Receiver<Result<Vec<i64>, mysql::Error>>,
}

impl PendingRead {
    fn start(context: &mut ScenarioContext, sql: &'static str) -> Result<Self> {
        let (id_tx, id_rx) = mpsc::sync_channel(1);
        let (done_tx, done) = mpsc::sync_channel(1);
        let user = context.mysql_user().to_owned();
        let port = context.mysql_port();
        let connect = context.remaining("connect the delayed read")?;
        let thread = thread::Builder::new()
            .name("native-creation-read".to_owned())
            .spawn(move || -> Result<()> {
                let mut connection = mysql_actor::connect_for_cancellation(&user, port, connect)?;
                id_tx
                    .send(connection.connection_id())
                    .context("publish the delayed read connection id")?;
                let outcome = connection.query(sql);
                done_tx
                    .send(outcome)
                    .context("publish the delayed read result")
            })
            .context("start the delayed read")?;
        let connection_id = id_rx
            .recv_timeout(context.remaining("receive the delayed read connection id")?)
            .context("the delayed read ended before publishing its connection id")?;
        Ok(Self {
            thread,
            connection_id,
            done,
        })
    }

    fn still_running(&self) -> bool {
        matches!(self.done.try_recv(), Err(mpsc::TryRecvError::Empty))
    }

    fn finish(self, context: &mut ScenarioContext) -> Result<Vec<i64>> {
        let outcome = self
            .done
            .recv_timeout(context.remaining("await the delayed read")?)
            .context("the delayed read did not finish")?;
        self.thread
            .join()
            .map_err(|_| anyhow::anyhow!("the delayed read thread panicked"))??;
        outcome.context("the delayed read failed")
    }
}

// ---------------------------------------------------------------------------
// native-creation/fixed-plan-recovery
// ---------------------------------------------------------------------------

/// A recovered statement runs the plan it was activated with.
///
/// The same delayed read runs once cleanly and once with the backend that
/// admitted its first task killed before any result was read. The second
/// completes on replacement attempt 2 through the real actor authorization
/// and recovery path, and the frontend freezes exactly as many static plans
/// for it as for the clean run: the recovery attempt encoded none of its own.
struct FixedPlanRecovery;

impl Scenario for FixedPlanRecovery {
    fn name(&self) -> &'static str {
        "native-creation/fixed-plan-recovery"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_snapshot(context)?;
        let idle = FrontendCreationGauges::scrape(context)?;

        // The reference: how many static plans one clean run freezes.
        let before_clean = latest_execution_id(context)?;
        let pending = PendingRead::start(context, NID2_FENCE_QUERY)?;
        let rows = pending.finish(context)?;
        ensure!(rows.len() == 2, "the clean read returned {rows:?}");
        let clean_terminal = await_terminal_snapshot(context, before_clean.as_deref())?;
        ensure!(
            clean_terminal.attempt_id == 1,
            "the clean read unexpectedly ran attempt {}",
            clean_terminal.attempt_id
        );
        let after_clean = FrontendCreationGauges::scrape(context)?;
        let clean_static = after_clean.static_fragments_frozen - idle.static_fragments_frozen;
        ensure!(
            clean_static > 0,
            "the clean read froze no static plan: {idle:?} -> {after_clean:?}"
        );
        await_idle_gauges(context, &idle, "the clean read")?;
        context.action(format!(
            "a clean run froze {clean_static} static plan(s) on attempt 1"
        ));

        // The recovery: kill the backend that admitted a task before any row
        // is read, so the statement must complete on a replacement attempt.
        let before_recovery = latest_execution_id(context)?;
        let frozen_before = FrontendCreationGauges::scrape(context)?;
        let created_before = (0..context.handle().be_count())
            .map(|index| context.handle().be_log_count(index, CREATE_APPLIED))
            .collect::<Result<Vec<_>>>()?;
        let mut stream = MysqlStream::query(
            context.mysql_user(),
            context.mysql_port(),
            NID2_FENCE_QUERY,
            context.remaining("open the recovered read")?,
        )?;
        let target = await_fresh_task_create(context, &created_before)?;
        context
            .handle()
            .kill_be(target)
            .with_context(|| format!("kill admitted BE[{target}]"))?;
        await_backend_exit(context, target)?;
        assert_two_sleep_rows(&mut stream)?;
        let terminal = await_terminal_snapshot(context, before_recovery.as_deref())?;
        ensure!(
            terminal.attempt_id == 2,
            "the read must complete on replacement attempt 2, got attempt {}",
            terminal.attempt_id
        );
        let recovered = FrontendCreationGauges::scrape(context)?;
        let recovered_static =
            recovered.static_fragments_frozen - frozen_before.static_fragments_frozen;
        ensure!(
            recovered_static == clean_static,
            "a recovered statement froze {recovered_static} static plan(s) where a clean run \
             freezes {clean_static}: the recovery attempt encoded its own"
        );
        await_idle_gauges(context, &idle, "the recovered read")?;
        await_resource_convergence(context, &baseline)?;
        context.action(format!(
            "the read recovered on attempt 2 after BE[{target}] exited and froze {recovered_static} \
             static plan(s), exactly as the clean run did"
        ));

        let deadline = context.deadline();
        context
            .handle()
            .restart_be_until(target, deadline)
            .with_context(|| format!("restore BE[{target}]"))?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Native normal-close capacity: Accepted reservations remain charged through
// active execution and the legal late-frame retention horizon.
// ---------------------------------------------------------------------------

struct NormalCloseCapacity {
    byte_limited: bool,
}

impl Scenario for NormalCloseCapacity {
    fn name(&self) -> &'static str {
        if self.byte_limited {
            "native-creation/normal-close-byte-capacity"
        } else {
            "native-creation/normal-close-count-capacity"
        }
    }

    fn launch_config(&self, _root: &std::path::Path) -> Result<ScenarioLaunchConfig> {
        let mut launch = ScenarioLaunchConfig::default();
        let (records, bytes) = if self.byte_limited {
            (32, 2048)
        } else {
            (2, 64 * 1024 * 1024)
        };
        launch.config_overlay.be = Some(format!(
            "[runtime]\ntask_normal_close_max_records = {records}\ntask_normal_close_max_bytes = {bytes}\n"
        ));
        Ok(launch)
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let exchange_authorization =
            authorization_header(&context.handle().native_backend_probe_trust()?)?;
        let retained = RawBackendSession::establish(context, 0)?;
        let live = RawBackendSession::establish(context, 0)?;
        let receiver = |session: &RawBackendSession, id| {
            let mut create = RawCreate::values(session, id);
            create.pipeline_dop = 1;
            create.exchange_input = true;
            create
        };
        let first = receiver(&retained, 1);
        ensure_accepted(&retained, &first)?;
        await_installed(context, &retained, &first)?;
        let mut active = Vec::new();
        let refused = loop {
            let create = receiver(&live, active.len() as u32 + 1);
            let receipt = live.apply(create.operation(), "fill normal-close capacity")?;
            match outcome(&receipt) {
                proto::TaskOperationOutcome::Accepted => {
                    await_installed(context, &live, &create)?;
                    active.push(create);
                    ensure!(
                        active.len() < 32,
                        "byte capacity did not reject before the independent record limit"
                    );
                }
                proto::TaskOperationOutcome::ResourceExhausted => break create,
                verdict => bail!("capacity probe returned {verdict:?}: {receipt:?}"),
            }
        };
        let occupied = active.len() + 1;
        ensure!(
            !active.is_empty(),
            "capacity fixture must hold a second live receiver"
        );
        if self.byte_limited {
            ensure!(
                occupied < 32,
                "record limit could explain the supposed byte rejection"
            );
        } else {
            ensure!(occupied == 2, "record cap 2 accepted {occupied} receivers");
        }
        context.action(format!("real Native Create accepted {occupied} installed inbound receivers, then returned ResourceExhausted at the configured {} bound", if self.byte_limited { "byte" } else { "record" }));

        retained.release_when_ready(context)?;
        assert_normal_close(&retained, &first, &exchange_authorization)?;
        ensure_capacity_refused(&live, &refused)?;
        // A still-live exact receiver accepts EOS after another Context has
        // moved its reservation from active into retained; capacity pressure
        // neither evicts that route nor lets a new Create overtake it.
        let response = exchange_probe(&live, &active[0], true, &exchange_authorization)?;
        ensure!(
            response
                .status
                .as_ref()
                .is_some_and(|status| status.code == 0)
                && response.normal_closed.is_none(),
            "active receiver was evicted or closed under capacity pressure: {response:?}"
        );
        context.action("one Context retained exact typed normal-close evidence while another active receiver still accepted its exact sender EOS; new admission stayed refused");
        live.release_when_ready(context)?;
        // The retained Context itself cannot admit tasks after Quiesce. Use a
        // third Context to prove retained reservations still occupy the cap.
        let blocked = RawBackendSession::establish(context, 0)?;
        let blocked_create = receiver(&blocked, 1);
        ensure_capacity_refused(&blocked, &blocked_create)?;
        blocked.release_when_ready(context)?;
        assert_normal_close(&retained, &first, &exchange_authorization)?;
        context.action("both receiver Contexts released, but their retained normal-close reservations still refused a fresh Context");

        let horizon = Duration::from_secs(120);
        let deadline = std::time::Instant::now() + horizon;
        while std::time::Instant::now() < deadline {
            let remaining =
                context.remaining("wait the legal 120 second normal-close retention horizon")?;
            thread::sleep(
                remaining
                    .min(Duration::from_secs(1))
                    .min(deadline.saturating_duration_since(std::time::Instant::now())),
            );
        }
        let recovered = RawBackendSession::establish(context, 0)?;
        let recovered_create = receiver(&recovered, 1);
        loop {
            let receipt = recovered.apply(
                recovered_create.operation(),
                "admission after normal-close horizon",
            )?;
            match outcome(&receipt) {
                proto::TaskOperationOutcome::Accepted => break,
                proto::TaskOperationOutcome::ResourceExhausted => {
                    thread::sleep(
                        context
                            .remaining("await normal-close horizon sweep")?
                            .min(POLL_INTERVAL),
                    );
                }
                verdict => bail!("post-horizon admission returned {verdict:?}: {receipt:?}"),
            }
        }
        await_installed(context, &recovered, &recovered_create)?;
        let forgotten = exchange_probe(&retained, &first, false, &exchange_authorization)?;
        ensure!(
            forgotten.normal_closed.is_none()
                && forgotten
                    .status
                    .as_ref()
                    .is_some_and(|status| status.code != 0),
            "expired Context still retained normal-close evidence: {forgotten:?}"
        );
        recovered.release_when_ready(context)?;
        context.action("after the legal horizon, the old exact late frame no longer received closure evidence and a fresh receiver installed successfully; retained capacity returned at Context Gone");
        Ok(())
    }
}

pub(super) fn ensure_accepted(session: &RawBackendSession, create: &RawCreate) -> Result<()> {
    let receipt = session.apply(create.operation(), "accept the capacity receiver")?;
    ensure!(
        outcome(&receipt) == proto::TaskOperationOutcome::Accepted,
        "receiver Create was not accepted: {receipt:?}"
    );
    Ok(())
}

fn ensure_capacity_refused(session: &RawBackendSession, create: &RawCreate) -> Result<()> {
    let receipt = session.apply(
        create.operation(),
        "refuse admission at occupied normal-close capacity",
    )?;
    ensure!(
        outcome(&receipt) == proto::TaskOperationOutcome::ResourceExhausted
            && receipt.ack.is_none(),
        "occupied normal-close capacity must reject before Accepted: {receipt:?}"
    );
    Ok(())
}

pub(super) fn await_installed(
    context: &mut ScenarioContext,
    session: &RawBackendSession,
    create: &RawCreate,
) -> Result<()> {
    loop {
        let receipt = session.apply(create.operation(), "observe exact receiver installation")?;
        let Some(proto::task_operation_receipt::Ack::CreateTask(ack)) = receipt.ack else {
            bail!("accepted receiver replay omitted its Create acknowledgement: {receipt:?}");
        };
        let status = ack
            .current_status
            .context("receiver acknowledgement omitted status")?;
        ensure!(
            status.state != proto::TaskState::Failed as i32,
            "receiver preparation failed: {status:?}"
        );
        if status.installed == Some(true) {
            return Ok(());
        }
        thread::sleep(
            context
                .remaining("await capacity receiver installation")?
                .min(POLL_INTERVAL),
        );
    }
}

fn exchange_probe(
    session: &RawBackendSession,
    create: &RawCreate,
    eos: bool,
    exchange_authorization: &str,
) -> Result<proto::ExchangeResponse> {
    raw_unary(
        session.connector.clone(),
        "/novarocks.NovaRocksGrpc/ExchangeUnary",
        exchange_authorization,
        proto::ExchangeRequest {
            finst_id_hi: 0x5b,
            finst_id_lo: create.kernel_key_low,
            node_id: 10,
            sender_id: 0,
            be_number: 0,
            eos,
            sequence: 0,
            payload: Vec::new().into(),
            source_finst_id_hi: 0x5c,
            source_finst_id_lo: create.kernel_key_low,
            sender_ordinal: 0,
            sender_count: 1,
        },
    )
}

fn assert_normal_close(
    session: &RawBackendSession,
    create: &RawCreate,
    exchange_authorization: &str,
) -> Result<()> {
    let response = exchange_probe(session, create, false, exchange_authorization)?;
    ensure!(
        response
            .status
            .as_ref()
            .is_some_and(|status| status.code == 0),
        "late frame normal close failed: {response:?}"
    );
    let proof = response
        .normal_closed
        .context("late frame omitted typed normal close")?;
    ensure!(
        proof
            .destination_task
            .as_ref()
            .is_some_and(
                |task| task.query_execution_id == create.context.query_execution_id
                    && task.stage_id == 1
                    && task.task_id == create.task_id
                    && task.backend_process_id == create.context.backend_process_id
            )
            && proof.destination_finst_id_hi == 0x5b
            && proof.destination_finst_id_lo == create.kernel_key_low
            && proof.destination_node_id == 10
            && proof.source_finst_id_hi == 0x5c
            && proof.source_finst_id_lo == create.kernel_key_low
            && proof.sender_ordinal == 0
            && proof.sender_count == 1,
        "normal close proof disagrees with the frozen exact route: {proof:?}"
    );
    Ok(())
}

/// A real Accepted-to-Quiesce schedule. Preparation can legitimately finish
/// first: no test delay is installed, and this scenario does not claim that
/// any particular instruction ran while the backend was Preparing.
struct AcceptedPreparationControlRaces;

impl Scenario for AcceptedPreparationControlRaces {
    fn name(&self) -> &'static str {
        "native-creation/accepted-preparation-control-races"
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let session = RawBackendSession::establish(context, 0)?;
        let mut failed = RawCreate::values(&session, 1);
        failed.initial_domains = vec![proto::TaskDomainUpdate {
            domain: Some(proto::task_domain_update::Domain::OpenExchangeEdges(
                proto::OpenExchangeEdgesDomain {
                    version: 1,
                    edge_ids: vec![7],
                },
            )),
        }];
        ensure_accepted(&session, &failed)?;
        loop {
            let receipt =
                session.apply(failed.operation(), "observe retained preparation failure")?;
            let Some(proto::task_operation_receipt::Ack::CreateTask(ack)) = receipt.ack else {
                bail!("failed accepted identity lost its Create acknowledgement: {receipt:?}");
            };
            let status = ack
                .current_status
                .context("preparation failure replay omitted status")?;
            if status.state == proto::TaskState::Failed as i32 {
                ensure!(
                    status.installed == Some(false)
                        && matches!(status.termination.as_ref().and_then(|termination| termination.cause.as_ref()), Some(proto::task_termination::Cause::Failed(failure)) if failure.phase == proto::TaskFailurePhase::Preparation as i32),
                    "invalid initial domain did not retain a preparation failure: {status:?}"
                );
                break;
            }
            thread::sleep(
                context
                    .remaining("await accepted preparation failure")?
                    .min(POLL_INTERVAL),
            );
        }
        let mut legal_replay = failed.clone();
        legal_replay.initial_domains.clear();
        let replay = session.apply(
            legal_replay.operation(),
            "legal replay of failed accepted identity",
        )?;
        ensure!(
            outcome(&replay) == proto::TaskOperationOutcome::Idempotent,
            "a failed accepted identity was replaced: {replay:?}"
        );
        context.action("an invalid initial domain was Accepted, then retained an explicit preparation failure; a later legal body replay was Idempotent for the spent identity");

        let receiver = |id| {
            let mut create = RawCreate::values(&session, id);
            create.pipeline_dop = 1;
            create.exchange_input = true;
            create
        };
        let canceled = receiver(2);
        ensure_accepted(&session, &canceled)?;
        await_installed(context, &session, &canceled)?;
        let cancel = session_control(
            &session,
            proto::task_control_operation::Control::CancelTask(proto::CancelTaskRequest {
                identity: Some(raw_create_identity(&canceled)),
                reason: proto::TaskCancelReason::UpstreamNoLongerNeeded as i32,
            }),
        )?;
        ensure!(
            matches!(
                outcome(&cancel),
                proto::TaskOperationOutcome::Accepted | proto::TaskOperationOutcome::Idempotent
            ),
            "normal Cancel of the installed receiver was not accepted: {cancel:?}"
        );
        let racing = receiver(3);
        ensure_accepted(&session, &racing)?;
        // Intentionally perform no poll or wait between the Accepted receipt
        // and this exact admission fence.
        let fence = quiesce_session(&session)?;
        let expected = vec![
            raw_create_identity(&failed),
            raw_create_identity(&canceled),
            raw_create_identity(&racing),
        ];
        ensure!(
            fence.fence_version != 0 && fence.accepted_tasks == expected,
            "Quiesce did not freeze the exact accumulated Accepted membership: {fence:?}"
        );
        let replay_fence = quiesce_session(&session)?;
        ensure!(
            replay_fence.query_context == fence.query_context
                && replay_fence.fence_version == fence.fence_version
                && replay_fence.accepted_tasks == expected,
            "Quiesce replay changed its cut or membership: {fence:?} -> {replay_fence:?}"
        );
        context.action("exact normal Cancel progressed; a third receiver's Accepted receipt was immediately followed by Quiesce, which froze and replayed all three exact identities without a Preparing hold");

        let late = receiver(4);
        let rejected = session.apply(late.operation(), "late Create after Quiesce")?;
        ensure!(
            outcome(&rejected) == proto::TaskOperationOutcome::ContextTerminalReceipt
                && rejected.ack.is_none(),
            "Quiesce admitted a late identity: {rejected:?}"
        );
        for _ in 0..2 {
            let duplicate = session.apply(
                session
                    .establish_request
                    .clone()
                    .context("raw session lost its original Establish")?,
                "duplicate original Establish after Quiesce",
            )?;
            ensure!(
                !matches!(
                    outcome(&duplicate),
                    proto::TaskOperationOutcome::Accepted | proto::TaskOperationOutcome::NotReady
                ),
                "duplicate Establish reopened admission: {duplicate:?}"
            );
            if let Some(proto::task_operation_receipt::Ack::QueryContext(ack)) = duplicate.ack {
                ensure!(
                    ack.state != proto::QueryContextState::Active as i32
                        && ack.state != proto::QueryContextState::Establishing as i32,
                    "duplicate Establish returned an open Context: {ack:?}"
                );
            }
            let rejected =
                session.apply(late.operation(), "late Create after duplicate Establish")?;
            ensure!(
                outcome(&rejected) == proto::TaskOperationOutcome::ContextTerminalReceipt
                    && rejected.ack.is_none(),
                "duplicate Establish revived late Create: {rejected:?}"
            );
        }
        context.action("late Create returned ContextTerminalReceipt; two original Establish replays left the Context fenced and could not reopen admission");
        observe_terminal_stop_and_fence(context, &session, &fence, &expected)?;
        reject_invalid_covered_subscriptions(context, &session, &expected)?;
        let released = session.release_when_ready(context)?;
        ensure!(
            released == proto::ReleaseQueryContextOutcome::Released,
            "normally fenced Context did not release: {released:?}"
        );
        let late_establish = session.apply(
            session
                .establish_request
                .clone()
                .context("raw session lost its original Establish")?,
            "original Establish after normal Release",
        )?;
        ensure!(
            outcome(&late_establish) == proto::TaskOperationOutcome::ContextTerminalReceipt,
            "late Establish revived a released Context: {late_establish:?}"
        );
        let late_create = session.apply(late.operation(), "late Create after normal Release")?;
        ensure!(
            outcome(&late_create) == proto::TaskOperationOutcome::ContextTerminalReceipt
                && late_create.ack.is_none(),
            "late Create revived a released Context: {late_create:?}"
        );
        let after_release = quiesce_session(&session)?;
        ensure!(
            after_release.fence_version == fence.fence_version
                && after_release.accepted_tasks == expected,
            "Release discarded the retained exact admission cut: {after_release:?}"
        );
        context.action("real covered stream provided terminal status and actual_stopped for each exact Task plus the exact fence, CatchUpComplete and covered Bookmark; normal Release then completed and retained the fence");
        Ok(())
    }
}

pub(super) fn raw_create_identity(create: &RawCreate) -> proto::TaskIdentity {
    proto::TaskIdentity {
        query_execution_id: create.context.query_execution_id,
        stage_id: 1,
        task_id: create.task_id,
        backend_process_id: create.context.backend_process_id.clone(),
    }
}

pub(super) fn session_control(
    session: &RawBackendSession,
    control: proto::task_control_operation::Control,
) -> Result<proto::TaskOperationReceipt> {
    only_successful_receipt(
        raw_unary_response(
            &session.control_connector,
            CONTROL_PATH,
            &session.authorization,
            proto::ApplyTaskControlOperationsRequest {
                operations: vec![proto::TaskControlOperation {
                    envelope: Some(raw_operation_envelope(1_000)),
                    control: Some(control),
                }],
            },
        )?,
        "native control probe",
    )
}

pub(super) fn quiesce_session(
    session: &RawBackendSession,
) -> Result<proto::QuiesceQueryContextAck> {
    let receipt = session_control(
        session,
        proto::task_control_operation::Control::QuiesceQueryContext(
            proto::QuiesceQueryContextRequest {
                query_context: Some(session.context_under(session.frontend, 1)),
            },
        ),
    )?;
    let Some(proto::task_operation_receipt::Ack::QuiesceQueryContext(ack)) = receipt.ack else {
        bail!("Quiesce omitted the exact fence acknowledgement: {receipt:?}");
    };
    Ok(ack)
}

/// Reject invalid requests at the authenticated server-streaming RPC boundary.
/// A server that accidentally admits a legacy stream fails this bounded probe
/// rather than hanging the scenario while waiting for its infinite stream.
fn reject_invalid_covered_subscriptions(
    context: &mut ScenarioContext,
    session: &RawBackendSession,
    identities: &[proto::TaskIdentity],
) -> Result<()> {
    use prost::Message;
    let query_context = session.context_under(session.frontend, 1);
    let base = proto::SubscribeTaskStatusRequest {
        query_context: Some(query_context.clone()),
        generation: 1,
        required_identities: identities.to_vec(),
        ..Default::default()
    };
    let mut missing_generation = base.clone();
    missing_generation.generation = 0;
    let cursor_only = proto::SubscribeTaskStatusRequest {
        query_context: Some(query_context.clone()),
        ..Default::default()
    };
    let mut future_task = base.clone();
    future_task.task_convergence_cursors = vec![proto::TaskConvergenceCursor {
        identity: Some(identities[0].clone()),
        current_version: u64::MAX,
    }];
    let mut future_context = base.clone();
    future_context.context_convergence_cursor = Some(proto::QueryContextConvergenceCursor {
        query_context: Some(query_context),
        current_version: u64::MAX,
    });
    let mut foreign_context = base;
    foreign_context.context_convergence_cursor = Some(proto::QueryContextConvergenceCursor {
        query_context: Some(session.context_under(session.frontend, 2)),
        current_version: 0,
    });
    let requests = [
        ("missing generation", missing_generation),
        ("cursor-only legacy mode", cursor_only),
        ("future Task convergence cursor", future_task),
        ("future Context convergence cursor", future_context),
        ("cross-Context convergence cursor", foreign_context),
    ];
    let connector = session.connector.clone();
    let authorization = session.authorization.clone();
    let budget = context.remaining("reject malformed covered subscriptions")?;
    tokio::runtime::Runtime::new()?.block_on(async move {
        tokio::time::timeout(budget, async move {
            for (name, request) in requests {
                let stream = connector.connect().await.map_err(anyhow::Error::msg)?;
                let (mut sender, connection) = h2::client::handshake(stream).await?;
                let driver = tokio::spawn(connection);
                let result = async {
                    let headers = http::Request::builder()
                        .method("POST")
                        .uri("/novarocks.NovaRocksGrpc/SubscribeTaskStatus")
                        .header(http::header::CONTENT_TYPE, "application/grpc")
                        .header("te", "trailers")
                        .header(http::header::AUTHORIZATION, &authorization)
                        .body(())?;
                    let (response, mut send) = sender.send_request(headers, false)?;
                    let payload = request.encode_to_vec();
                    let mut frame = vec![0];
                    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
                    frame.extend_from_slice(&payload);
                    send.send_data(bytes::Bytes::from(frame), true)?;
                    let response = response.await?;
                    ensure!(
                        response.status().as_u16() == 200,
                        "{name}: HTTP {}",
                        response.status()
                    );
                    let header_status = response
                        .headers()
                        .get("grpc-status")
                        .map(|value| value.to_str().map(str::to_owned))
                        .transpose()?;
                    let mut body = response.into_body();
                    let mut received_bytes = 0usize;
                    while let Some(chunk) = body.data().await {
                        let chunk = chunk?;
                        received_bytes += chunk.len();
                        body.flow_control().release_capacity(chunk.len())?;
                        ensure!(
                            received_bytes == 0,
                            "{name}: invalid request received stream data"
                        );
                    }
                    let trailers = body.trailers().await?;
                    let status = header_status.or_else(|| {
                        trailers
                            .as_ref()
                            .and_then(|values| values.get("grpc-status"))
                            .and_then(|value| value.to_str().ok())
                            .map(str::to_owned)
                    });
                    ensure!(
                        status.as_deref() == Some("3"),
                        "{name}: expected InvalidArgument, got {status:?}"
                    );
                    anyhow::Ok(())
                }
                .await;
                driver.abort();
                let _ = driver.await;
                result?;
            }
            anyhow::Ok(())
        })
        .await
        .context(
            "invalid covered subscription was admitted or did not settle within its deadline",
        )?
    })?;
    context.action("authenticated Native SubscribeTaskStatus rejected missing generation, legacy cursor-only mode, future Task/Context convergence cursors and cross-Context cursor, without producing stream facts");
    Ok(())
}

/// Reads the production covered stream through the same authenticated Native
/// listener. Full snapshots and real actual-stop facts are required; a Gone,
/// Unknown, bookmark, or RPC Release receipt cannot substitute for either.
pub(super) fn observe_terminal_stop_and_fence(
    context: &mut ScenarioContext,
    session: &RawBackendSession,
    fence: &proto::QuiesceQueryContextAck,
    identities: &[proto::TaskIdentity],
) -> Result<()> {
    observe_terminal_stop_and_fence_with_failure(
        context,
        session,
        Some(fence),
        identities,
        true,
        None,
    )
}

pub(super) fn observe_all_canceled_and_stopped(
    context: &mut ScenarioContext,
    session: &RawBackendSession,
    fence: &proto::QuiesceQueryContextAck,
    identities: &[proto::TaskIdentity],
) -> Result<()> {
    observe_terminal_stop_and_fence_with_failure(
        context,
        session,
        Some(fence),
        identities,
        false,
        None,
    )
}

/// Require real covered terminal and stop facts without fencing a sibling Task.
pub(super) fn observe_task_terminal_and_stopped(
    context: &mut ScenarioContext,
    session: &RawBackendSession,
    identity: &proto::TaskIdentity,
    state: proto::TaskState,
) -> Result<()> {
    observe_terminal_stop_and_fence_with_failure(
        context,
        session,
        None,
        std::slice::from_ref(identity),
        false,
        Some(vec![state as i32]),
    )
}

fn observe_terminal_stop_and_fence_with_failure(
    context: &mut ScenarioContext,
    session: &RawBackendSession,
    fence: Option<&proto::QuiesceQueryContextAck>,
    identities: &[proto::TaskIdentity],
    first_is_failed: bool,
    expected_states: Option<Vec<i32>>,
) -> Result<()> {
    use prost::Message;
    let connector = session.connector.clone();
    let authorization = session.authorization.clone();
    let expected = identities.to_vec();
    let fence = fence.cloned();
    let request = proto::SubscribeTaskStatusRequest {
        query_context: Some(session.context_under(session.frontend, 1)),
        generation: 1,
        required_identities: expected.clone(),
        ..Default::default()
    };
    let budget = context.remaining("observe real covered terminal and actual-stop facts")?;
    tokio::runtime::Runtime::new()?.block_on(async move {
        tokio::time::timeout(budget, async move {
            let stream = connector.connect().await.map_err(anyhow::Error::msg)?;
            let (mut sender, connection) = h2::client::handshake(stream).await?;
            let driver = tokio::spawn(async move { connection.await });
            let result = async {
                let request_headers = http::Request::builder().method("POST")
                    .uri("/novarocks.NovaRocksGrpc/SubscribeTaskStatus")
                    .header(http::header::CONTENT_TYPE, "application/grpc")
                    .header("te", "trailers")
                    .header(http::header::AUTHORIZATION, authorization).body(())?;
                let (response, mut send) = sender.send_request(request_headers, false)?;
                let payload = request.encode_to_vec();
                let mut frame = vec![0];
                frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
                frame.extend_from_slice(&payload);
                send.send_data(bytes::Bytes::from(frame), true)?;
                let response = response.await?;
                ensure!(response.status().as_u16() == 200, "covered stream returned HTTP {}", response.status());
                let mut body = response.into_body();
                let mut pending = Vec::new();
                let mut terminal = vec![false; expected.len()];
                let mut stopped = vec![false; expected.len()];
                let mut fence_seen = fence.is_none();
                let mut complete = false;
                while let Some(chunk) = body.data().await {
                    let chunk = chunk?;
                    body.flow_control().release_capacity(chunk.len())?;
                    pending.extend_from_slice(&chunk);
                    while pending.len() >= 5 {
                        ensure!(pending[0] == 0, "covered stream unexpectedly compressed a frame");
                        let length = u32::from_be_bytes(pending[1..5].try_into().expect("gRPC header width")) as usize;
                        ensure!(length <= 8 * 1024 * 1024, "covered probe frame exceeded its bounded buffer");
                        if pending.len() < length + 5 { break; }
                        let event = proto::TaskStatusStreamEvent::decode(&pending[5..length + 5])?;
                        pending.drain(..length + 5);
                        match event.event.context("covered stream omitted event")? {
                            proto::task_status_stream_event::Event::TaskStatus(status) => {
                                let index = expected.iter().position(|identity| status.identity.as_ref() == Some(identity)).context("covered status named another identity")?;
                                if matches!(proto::TaskState::try_from(status.state)?, proto::TaskState::Finished | proto::TaskState::Canceled | proto::TaskState::Aborted | proto::TaskState::Failed) {
                                    if let Some(states) = &expected_states {
                                        ensure!(status.state == states[index], "exact Task terminal changed: {status:?}");
                                    } else if first_is_failed && index == 0 {
                                        ensure!(status.state == proto::TaskState::Failed as i32 && status.installed == Some(false), "preparation failure history changed: {status:?}");
                                    } else {
                                        ensure!(status.state == proto::TaskState::Canceled as i32, "normal Cancel/Quiesce became failure or abort: {status:?}");
                                    }
                                    terminal[index] = true;
                                }
                            }
                            proto::task_status_stream_event::Event::TaskConvergence(receipt) => {
                                let index = expected.iter().position(|identity| receipt.identity.as_ref() == Some(identity)).context("covered actual-stop named another identity")?;
                                ensure!(receipt.version != 0, "covered actual-stop version was zero");
                                stopped[index] = true;
                            }
                            proto::task_status_stream_event::Event::Quiesce(receipt) => {
                                if let Some(fence) = &fence {
                                    ensure!(receipt.query_context == fence.query_context && receipt.fence_version == fence.fence_version && receipt.accepted_tasks == expected, "covered Quiesce changed the exact fence: {receipt:?}");
                                    fence_seen = true;
                                }
                            }
                            proto::task_status_stream_event::Event::CatchUpComplete(receipt) => {
                                ensure!(receipt.generation == 1, "covered catch-up names another generation");
                                complete = true;
                            }
                            proto::task_status_stream_event::Event::Bookmark(receipt) => {
                                ensure!(receipt.generation == 1 && receipt.sequence != 0 && receipt.covered_prefix <= receipt.source_cut, "covered bookmark is invalid: {receipt:?}");
                                if complete && fence_seen && terminal.iter().all(|fact| *fact) && stopped.iter().all(|fact| *fact) && receipt.covered_prefix == receipt.source_cut {
                                    return Ok(());
                                }
                            }
                            proto::task_status_stream_event::Event::TaskGone(receipt) => bail!("required terminal/stop evidence was reclaimed: {receipt:?}"),
                            proto::task_status_stream_event::Event::TaskUnknown(receipt) => bail!("required accepted identity was unknown: {receipt:?}"),
                            proto::task_status_stream_event::Event::TaskStatusUnchanged(_) | proto::task_status_stream_event::Event::TaskConvergenceUnchanged(_) => bail!("zero-cursor probe unexpectedly received Unchanged"),
                            proto::task_status_stream_event::Event::ContextConvergence(_) => {}
                        }
                    }
                }
                bail!("covered stream ended before exact terminal/stop/fence evidence; trailers={:?}", body.trailers().await?)
            }.await;
            driver.abort();
            let _ = driver.await;
            result
        }).await.context("covered terminal/actual-stop probe exhausted its scenario deadline")?
    })
}
