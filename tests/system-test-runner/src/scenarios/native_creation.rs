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
use novarocks_cluster_harness::TestClusterOwner;
use novarocks_native_trust::NativeEndpointConnector;
use novarocks_proto_models::{common, novarocks as proto};
use novarocks_types::identity::{BackendProcessId, FrontendProcessId};

use crate::actors::mysql as mysql_actor;
use crate::actors::mysql_stream::MysqlStream;
use crate::scenario::{Scenario, ScenarioContext, ScenarioLaunchConfig};

use super::native_compatibility::{
    HEARTBEAT_PATH, RawUnaryResponse, authorization_header, decode_hex_32, only_successful_receipt,
    raw_acquire_admission_ticket, raw_apply_task_operations_until, raw_establish,
    raw_operation_envelope, raw_unary, raw_unary_response, raw_unary_until,
};
use super::native_sql_controls::{SelectedStatement, SqlIntentOwner, StatementSpec};
use super::query_lifecycle::{
    NID2_FENCE_QUERY, arm_on_every_backend, assert_two_sleep_rows, await_backend_exit,
    await_fresh_task_create, await_resource_convergence, await_token_scoped_marker,
    resource_snapshot,
};

#[path = "raw_creation_controls.rs"]
mod raw_creation_controls;

const REQUIRED_BACKENDS: usize = 3;
const BASELINE_QUERY: &str = "SELECT v FROM (SELECT 1 AS v UNION ALL SELECT 2) t ORDER BY v";
const POLL_INTERVAL: Duration = Duration::from_millis(50);
/// Normal context fencing and release are small control operations.
const CONTROL_PATH: &str = "/novarocks.NovaRocksGrpc/ApplyTaskControlOperations";

const CREATE_APPLIED: &str = novarocks_failpoint::observation_marker::TASK_CREATE_APPLIED;
const CREATE_IDEMPOTENT: &str = novarocks_failpoint::observation_marker::TASK_CREATE_IDEMPOTENT;
const CREATE_ACK_DROPPED: &str = novarocks_failpoint::observation_marker::TASK_CREATE_ACK_DROPPED;
const LEASE_RENEWED: &str = novarocks_failpoint::observation_marker::PARTICIPATION_LEASE_RENEWED;

const STATIC_FRAGMENTS_FROZEN: &str = "novarocks_task_static_fragments_frozen_total";
const STATIC_FRAGMENTS_RETAINED: &str = "novarocks_task_static_fragments_retained";
const CREATES_PRICED: &str = "novarocks_task_creates_priced_total";
const CREATES_FROZEN: &str = "novarocks_task_creates_frozen_total";
const CREATE_PAYLOADS_RETAINED: &str = "novarocks_task_create_payloads_retained";

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(FrozenReplayAndMembership::default()),
        Box::new(CreationPayloadLifetime::default()),
        Box::new(FixedPlanRecovery::default()),
        Box::new(NormalCloseRetainedAdmission { byte_corpus: false }),
        Box::new(NormalCloseRetainedAdmission { byte_corpus: true }),
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
#[derive(Default)]
struct FrozenReplayAndMembership {
    observation: raw_creation_controls::RawCreationObservationOwner,
}

impl Scenario for FrozenReplayAndMembership {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "native_creation",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "native-creation/frozen-replay-and-membership"
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &std::path::Path,
        cluster_size: usize,
        binary: &std::path::Path,
        deadline: std::time::Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        Ok(Some(self.observation.freeze(
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let (query_id, frontend) = self.observation.take_identity(context.deadline())?;
        let session = RawBackendSession::establish_declared(context, 0, query_id, frontend)?;
        self.observation.verify_backend(session.backend)?;
        let applied = |context: &mut ScenarioContext| session.marker_count(context, CREATE_APPLIED);
        let idempotent =
            |context: &mut ScenarioContext| session.marker_count(context, CREATE_IDEMPOTENT);
        let renewed_before = context.handle().be_log_count(0, LEASE_RENEWED)?;
        context.action(format!(
            "established raw query context {} on BE[0] through its authenticated Native listener",
            session.execution_label()
        ));

        let replay_epoch = std::time::Instant::now();
        // Each new RPC gets a real new Worker carrier. Preserve the complete
        // original operation/ticket/credential snapshot for every replay.
        let original_establish = session
            .establish_request
            .clone()
            .context("the raw session lost its original Establish")?;
        let active = session.apply(
            original_establish.clone(),
            "active original Establish replay",
        )?;
        ensure!(
            outcome(&active)? == proto::TaskOperationOutcome::Idempotent,
            "the unchanged active Establish must be idempotent: {active:?}"
        );
        let Some(proto::task_operation_verdict::Ack::Participation(active_ack)) =
            receipt_verdict(&active)?.ack.as_ref()
        else {
            bail!("active Establish replay lost its typed Context acknowledgement: {active:?}");
        };
        ensure!(
            active_ack.participation.as_ref()
                == Some(&session.participation_under(session.frontend, 1))
                && active_ack.state == proto::ParticipationState::Active as i32,
            "active Establish replay changed the actual Context: {active_ack:?}"
        );
        let current_credential = session.advance_original_credential_epoch()?;
        ensure!(
            session.establish_request.as_ref() == Some(&original_establish),
            "credential advance rewrote the immutable original Establish"
        );
        let stale = session.apply(
            original_establish.clone(),
            "unchanged original Establish after actual credential advance",
        )?;
        ensure!(
            outcome(&stale)? == proto::TaskOperationOutcome::ParticipationConflict,
            "the old original Establish must conflict with the advanced current credential: {stale:?}"
        );
        let Some(proto::task_operation_verdict::Ack::Participation(stale_ack)) =
            receipt_verdict(&stale)?.ack.as_ref()
        else {
            bail!("conflicting original Establish lost its current Context receipt: {stale:?}");
        };
        ensure!(
            stale_ack.participation.as_ref()
                == Some(&session.participation_under(session.frontend, 1))
                && stale_ack.state == proto::ParticipationState::Active as i32,
            "conflicting original Establish changed the active Context: {stale_ack:?}"
        );
        ensure!(
            session.establish_request.as_ref() == Some(&original_establish),
            "the exact replay source changed after credential comparison"
        );
        std::fs::write(
            context.scenario_root().join("active-establish-replay.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "query_id_high": session.query_id.hi,
                "query_id_low": session.query_id.lo,
                "backend_process": session.backend.to_string(),
                "original_operation_preserved": true,
                "active_replay": "Idempotent",
                "credential_advance": "Apply",
                "current_credential_lease": current_credential.credential_domain_id,
                "current_credential_epoch": current_credential.epoch,
                "unchanged_original_replay_after_advance": "ParticipationConflict",
                "carrier_evidence": "distinct RPC submissions; Worker issues carrier identities",
                "comparison_evidence": "active Pure Idempotent requires the actual current-credential comparison; no independent M or getter counter",
                "material_omitted": true,
            }))?,
        )?;
        context.action("the unchanged active Establish replay returned Idempotent; an actual same-lease credential advance returned Apply, and replaying the untouched original operation then returned ParticipationConflict; the original snapshot and all later Create bodies remain unchanged");

        // The winner.
        let first = RawCreate::values(&session, 1);
        let accepted = session.apply(first.operation(), "first CreateTask")?;
        ensure!(
            outcome(&accepted)? == proto::TaskOperationOutcome::Accepted,
            "the first legal create must win, got {accepted:?}"
        );
        let original = receipt_verdict(&accepted)?
            .ack
            .clone()
            .context("the winning create returned no acknowledgement")?;
        ensure!(
            matches!(original, proto::task_operation_verdict::Ack::CreateTask(_)),
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
        variant.sink = RawSink::Result;
        variant.instance_ordinal = 9;
        let changed = session.apply(variant.operation(), "changed-body CreateTask replay")?;
        for (label, replay) in [("exact", &exact), ("changed-body", &changed)] {
            ensure!(
                outcome(replay)? == proto::TaskOperationOutcome::Idempotent,
                "an {label} replay of a created identity must be idempotent, got {replay:?}"
            );
            ensure!(
                receipt_verdict(replay)?
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
        foreign_frontend.participation =
            session.participation_under(FrontendProcessId::new_v7(), 1);
        let mut next_attempt = first.clone();
        next_attempt.participation = session.participation_under(session.frontend, 2);
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
            let verdict = outcome(&receipt)?;
            ensure!(
                !matches!(
                    verdict,
                    proto::TaskOperationOutcome::Accepted | proto::TaskOperationOutcome::Idempotent
                ) && receipt_verdict(&receipt)?.ack.is_none(),
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
            outcome(&refusal)? == proto::TaskOperationOutcome::Accepted,
            "the task is accepted before its initial domain is interpreted, got {refusal:?}"
        );
        await_count(
            context,
            "the failed preparation's terminal marker",
            2,
            |context| {
                session.marker_count(
                    context,
                    novarocks_failpoint::observation_marker::TASK_TERMINAL_RETAINED,
                )
            },
        )?;
        let mut legal = refused.clone();
        legal.initial_domains.clear();
        let won = session.apply(
            legal.operation(),
            "legal body replay after failed preparation",
        )?;
        let refusal_ack = receipt_verdict(&refusal)?.ack.as_ref();
        ensure!(
            outcome(&won)? == proto::TaskOperationOutcome::Idempotent
                && receipt_verdict(&won)?.ack.as_ref().is_some_and(|ack| {
                    refusal_ack.is_some_and(|first| same_create_entity(first, ack))
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
        let verdict = outcome(&after)?;
        ensure!(
            verdict != proto::TaskOperationOutcome::Accepted,
            "a replay after release created a task again: {after:?}"
        );
        if verdict == proto::TaskOperationOutcome::Idempotent {
            ensure!(
                receipt_verdict(&after)?
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
        super::result_delivery_support::await_idle(
            context,
            "active-establish-replay",
            "after-original-release",
            replay_epoch,
        )?;
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
    pub(super) original_deadline: std::time::Instant,
}

impl RawBackendSession {
    pub(super) fn establish(context: &mut ScenarioContext, index: usize) -> Result<Self> {
        Self::establish_with_identity(context, index, None)
    }

    /// Allocate the existing fixture identity once, before role launch when declared.
    pub(super) fn declare_identity() -> (common::UniqueId, FrontendProcessId) {
        static NEXT_SESSION: AtomicU64 = AtomicU64::new(1);
        let query_id = common::UniqueId {
            hi: 0x5b5b,
            lo: ((u64::from(std::process::id()) << 32)
                | NEXT_SESSION.fetch_add(1, Ordering::Relaxed)) as i64,
        };
        (query_id, FrontendProcessId::new_v7())
    }

    /// Use the request identities fixed by this direct consumer before role launch.
    pub(super) fn establish_declared(
        context: &mut ScenarioContext,
        index: usize,
        query_id: common::UniqueId,
        frontend: FrontendProcessId,
    ) -> Result<Self> {
        Self::establish_with_identity(context, index, Some((query_id, frontend)))
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
        let heartbeat: proto::HeartbeatResponse = raw_unary_until(
            control_connector.clone(),
            HEARTBEAT_PATH,
            &authorization,
            proto::HeartbeatRequest {
                expected_process_id: Some(proto::BackendProcessId {
                    value: backend.to_bytes().to_vec(),
                }),
            },
            context.deadline(),
        )?;
        let admission_epoch = heartbeat
            .admission_epoch_capability
            .context("the target heartbeat omitted its admission epoch capability")?;
        let (query_id, frontend) = shared.unwrap_or_else(Self::declare_identity);
        let mut session = Self {
            index,
            original_deadline: context.deadline(),
            connector,
            control_connector,
            authorization,
            backend,
            frontend,
            establish_request: None,
            query_id,
        };
        let target_participation = session.participation_under(session.frontend, 1);
        let acquisition = session.apply(
            raw_acquire_admission_ticket(
                target_participation.clone(),
                compatibility,
                admission_epoch,
            ),
            "admission ticket acquisition",
        )?;
        let Some(proto::task_operation_verdict::Ack::ParticipationAdmissionTicket(ticket)) =
            receipt_verdict(&acquisition)?.ack.clone()
        else {
            bail!("admission acquisition was not granted: {:?}", acquisition);
        };
        let ticket = ticket
            .ticket_id
            .context("the admission acknowledgement omitted its ticket id")?;
        let establish_request =
            raw_establish(target_participation, ticket, Some(compatibility.to_vec()));
        let establish = session.apply(establish_request.clone(), "Establish")?;
        session.establish_request = Some(establish_request);
        ensure!(
            outcome(&establish)? == proto::TaskOperationOutcome::Accepted,
            "the raw Establish must be accepted, got {establish:?}"
        );
        Ok(session)
    }

    /// Advance exactly the original empty-storage credential domain through
    /// the real Native operation. Keep the frozen Establish replay source
    /// immutable; existing RawCreate carries no context credential epoch.
    fn advance_original_credential_epoch(&self) -> Result<proto::ParticipationCredentialDomain> {
        let operation = self
            .establish_request
            .as_ref()
            .context("the raw session lost its original Establish")?;
        let Some(proto::task_operation::Operation::UpdateParticipation(update)) =
            &operation.operation
        else {
            bail!("the original operation is not a query-context update")
        };
        let Some(proto::update_participation_request::Command::Establish(establish)) =
            &update.command
        else {
            bail!("the original query-context update is not Establish")
        };
        let target_participation = establish
            .participation
            .clone()
            .context("the original Establish omitted its exact Context")?;
        ensure!(
            target_participation == self.participation_under(self.frontend, 1),
            "the original Establish belongs to a different Context"
        );
        let mut credential = establish
            .initial_credential
            .clone()
            .context("the original Establish omitted its credential domain")?;
        ensure!(
            credential.descriptors.is_empty(),
            "the direct replay fixture must not depend on external storage credentials"
        );
        credential.epoch = credential
            .epoch
            .checked_add(1)
            .context("the original credential epoch cannot advance")?;
        let receipt = self.apply(
            proto::TaskOperation {
                envelope: Some(raw_operation_envelope(15_000)),
                operation: Some(proto::task_operation::Operation::UpdateParticipation(
                    proto::UpdateParticipationRequest {
                        command: Some(proto::update_participation_request::Command::AdvanceDomain(
                            proto::AdvanceParticipationDomainRequest {
                                participation: Some(target_participation.clone()),
                                domain: Some(proto::ParticipationDomainUpdate {
                                    domain: Some(
                                        proto::participation_domain_update::Domain::Credential(
                                            credential.clone(),
                                        ),
                                    ),
                                }),
                            },
                        )),
                    },
                )),
            },
            "advance the exact original Context credential epoch",
        )?;
        ensure!(
            outcome(&receipt)? == proto::TaskOperationOutcome::Accepted,
            "the exact next credential epoch was not accepted: {receipt:?}"
        );
        let Some(proto::task_operation_verdict::Ack::Participation(ack)) =
            receipt_verdict(&receipt)?.ack.as_ref()
        else {
            bail!("credential advance omitted its typed Context acknowledgement: {receipt:?}");
        };
        ensure!(
            ack.participation.as_ref() == Some(&target_participation)
                && ack.state == proto::ParticipationState::Active as i32
                && ack.accepted_domains.len() == 1,
            "credential advance changed the exact active Context or receipt cardinality: {ack:?}"
        );
        let domain = &ack.accepted_domains[0];
        ensure!(
            domain.progression == proto::AcceptedDomainProgression::Apply as i32
                && matches!(domain.receipt.as_ref(),
                Some(proto::participation_domain_receipt::Receipt::Credential(current))
                if current.credential_domain_id == credential.credential_domain_id && current.accepted_epoch == credential.epoch),
            "credential advance did not attest the same lease and actual next epoch Apply: {domain:?}"
        );
        Ok(credential)
    }

    /// This session's query context, under `frontend` and `attempt`.
    pub(super) fn participation_under(
        &self,
        frontend: FrontendProcessId,
        attempt: u64,
    ) -> proto::ParticipationRef {
        proto::ParticipationRef {
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
            raw_apply_task_operations_until(
                &self.connector,
                &self.authorization,
                vec![operation],
                self.original_deadline,
            )?,
            subject,
        )
    }

    /// Fences new identities, then releases once every local task is terminal.
    pub(super) fn release_when_ready(
        &self,
        context: &mut ScenarioContext,
    ) -> Result<proto::ReleaseParticipationOutcome> {
        let quiesce: RawUnaryResponse<proto::ApplyTaskOperationsResponse> = raw_unary_response(
            &self.control_connector,
            CONTROL_PATH,
            &self.authorization,
            proto::ApplyTaskControlOperationsRequest {
                operations: vec![proto::TaskControlOperation {
                    envelope: Some(raw_operation_envelope(999)),
                    control: Some(
                        proto::task_control_operation::Control::QuiesceParticipation(
                            proto::QuiesceParticipationRequest {
                                participation: Some(self.participation_under(self.frontend, 1)),
                            },
                        ),
                    ),
                }],
            },
            self.original_deadline,
        )?;
        let receipt = only_successful_receipt(quiesce, "QuiesceParticipation")?;
        let Some(proto::task_operation_verdict::Ack::QuiesceParticipation(fence)) =
            receipt_verdict(&receipt)?.ack.clone()
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
                                proto::task_control_operation::Control::ReleaseParticipation(
                                    proto::ReleaseParticipationRequest {
                                        participation: Some(
                                            self.participation_under(self.frontend, 1),
                                        ),
                                    },
                                ),
                            ),
                        }],
                    },
                    self.original_deadline,
                )?;
            let receipt = only_successful_receipt(response, "ReleaseParticipation")?;
            let Some(proto::task_operation_verdict::Ack::ReleaseParticipation(ack)) =
                receipt_verdict(&receipt)?.ack.clone()
            else {
                bail!("the release returned no release acknowledgement: {receipt:?}");
            };
            let released = proto::ReleaseParticipationOutcome::try_from(ack.outcome)
                .context("decode the release outcome")?;
            if released != proto::ReleaseParticipationOutcome::NotReady {
                return Ok(released);
            }
            let remaining = context.remaining("release the raw query context")?;
            thread::sleep(remaining.min(POLL_INTERVAL));
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum RawSink {
    CountOnly,
    ClientRows,
    Result,
    Stream { multicast: bool },
}

/// Author immutable package facts through the checked production constructors.
/// No runtime capability, legacy tree, or test-only library helper is involved.
pub(super) fn compiled_fragment(
    fragment_id: u32,
    dop: u32,
    input: Option<(u32, bool)>,
    sink: RawSink,
    rows: &[i64],
) -> proto::FrozenFragment {
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field};
    use novarocks_functions::{ConstantPool, FunctionValueType};
    use novarocks_physical_plan::*;
    use novarocks_type_contract::{
        CompileControlError, CompilePhase, ControlShape, EvaluationDomainId, ExpressionControlFlow,
        ExpressionEffectContext, ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId,
        PureCompileControl, SemanticParameters,
    };
    use prost::Message;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    struct Control;
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Ok(())
        }
    }
    let root = NodeId::new(10);
    let value = ValueId::new(1);
    let ty = FunctionValueType::new(DataType::Int64, false);
    let mut builder = FragmentBuilder::new(FragmentId::new(fragment_id));
    let mut cuts = FragmentCuts::default();
    let partitioning = || EdgePartitioning {
        source: Distribution::Singleton,
        source_multiplicity: RowMultiplicity::SingleCopy,
        destination: Distribution::Singleton,
        destination_multiplicity: RowMultiplicity::SingleCopy,
    };
    let cut_value = || CutValue {
        value,
        ty: ty.clone(),
    };
    let mut constants = ConstantPools::empty();
    if let Some((edge, multicast)) = input {
        let edge = EdgeId::new(edge);
        builder
            .insert_value(ValueDef {
                id: value,
                ty: ty.clone(),
                origin: if multicast {
                    ValueOrigin::CteImport {
                        edge,
                        producer_fragment: FragmentId::new(2),
                        producer_value: value,
                    }
                } else {
                    ValueOrigin::ExchangeImport {
                        edge,
                        source_value: value,
                    }
                },
            })
            .expect("receiver value");
        builder
            .add_exchange_source(
                root,
                edge,
                Box::from([(value, value)]),
                Box::from([value]),
                Distribution::Singleton,
                RowMultiplicity::SingleCopy,
            )
            .expect("receiver node");
        cuts.inbound = Box::from([InboundFragmentCut {
            edge,
            kind: if multicast {
                EdgeKind::CteMulticast
            } else {
                EdgeKind::Stream
            },
            source_fragment: FragmentId::new(2),
            destination_node: root,
            imports: Box::from([CutImport {
                source: cut_value(),
                destination: value,
            }]),
            partitioning: partitioning(),
            source_port: novarocks_physical_plan::FragmentOutputPort::Root,
            delivery: novarocks_physical_plan::DeliveryRequirement::DemandDriven,
            payload: novarocks_physical_plan::ExchangePayloadContract::Relational,
        }]);
    } else {
        builder
            .insert_value(ValueDef {
                id: value,
                ty: ty.clone(),
                origin: ValueOrigin::NodeOutput {
                    node: root,
                    output_ordinal: 0,
                },
            })
            .expect("values output");
        let mut cells: Vec<Box<[ExprId]>> = Vec::new();
        if !rows.is_empty() {
            let array = Int64Array::from(rows.to_vec());
            let source = ConstantPool::try_new(
                Arc::new(Field::new("v", DataType::Int64, false)),
                ty.clone(),
                arrow_array::Array::to_data(&array),
                novarocks_plan_codec::physical_package_v2::FROZEN_CONSTANT_POLICY,
                CompilePhase::Validate,
                &Control,
            )
            .expect("BIGINT constant owner");
            constants
                .insert(ConstantPoolId::new(0), source)
                .expect("constant pool");
            for ordinal in 0..rows.len() {
                let expression = builder
                    .add_expression(
                        root,
                        ty.clone(),
                        ExprKind::Constant(ConstantReference {
                            pool: ConstantPoolId::new(0),
                            ordinal: ordinal as u32,
                        }),
                    )
                    .expect("constant expression");
                cells.push(Box::from([expression]));
            }
        }
        builder
            .add_values(root, cells.into_boxed_slice(), Box::from([value]))
            .expect("values node");
    }
    let sink = match sink {
        RawSink::CountOnly => FragmentSink::RootResult(Box::new(
            novarocks_result_contract::RootOutputContract::new(
                novarocks_result_contract::RootProfileId::V1,
                novarocks_result_contract::FrozenRootOutput::CountOnly,
            ),
        )),
        RawSink::ClientRows => {
            use novarocks_result_contract::{
                ClientRenderSchema, FrozenRootOutput, NativeRenderType, RenderColumn, RenderField,
                RenderPresentation, RootOutputContract, RootProfileId,
            };
            // This fixture authors one actual non-null I64 root occurrence.
            // LocalCompiler binds its slot from the original native layout.
            let schema = ClientRenderSchema::try_new(
                novarocks_result_contract::ClientPresentationFamily::MysqlText,
                vec![RenderColumn {
                    source_ordinal: 0,
                    source_slot: None,
                    name: "v".into(),
                    field: RenderField {
                        presentation: RenderPresentation::ScalarText,
                        nullable: false,
                        native_type: NativeRenderType::SignedInteger(64),
                    },
                }],
                1,
            )
            .expect("checked original one-column ClientRows root declaration");
            FragmentSink::RootResult(Box::new(RootOutputContract::new(
                RootProfileId::V1,
                FrozenRootOutput::ClientRows(schema),
            )))
        }
        RawSink::Result => FragmentSink::Result,
        RawSink::Stream { multicast } => {
            cuts.outbound = (1..=if multicast { 2 } else { 1 })
                .map(|edge| OutboundFragmentCut {
                    edge: EdgeId::new(edge),
                    kind: if multicast {
                        EdgeKind::CteMulticast
                    } else {
                        EdgeKind::Stream
                    },
                    destination_fragment: FragmentId::new(if edge == 2 { 3 } else { 1 }),
                    destination_node: root,
                    projection: Box::from([cut_value()]),
                    destination_imports: Box::from([CutImport {
                        source: cut_value(),
                        destination: value,
                    }]),
                    partitioning: partitioning(),
                    source_port: novarocks_physical_plan::FragmentOutputPort::Root,
                    delivery: novarocks_physical_plan::DeliveryRequirement::DemandDriven,
                    payload: novarocks_physical_plan::ExchangePayloadContract::Relational,
                })
                .collect::<Vec<_>>()
                .into_boxed_slice();
            if multicast {
                FragmentSink::Multicast {
                    edges: Box::from([EdgeId::new(1), EdgeId::new(2)]),
                }
            } else {
                FragmentSink::Stream {
                    edge: EdgeId::new(1),
                }
            }
        }
    };
    let fragment = builder
        .finish_definition(
            root,
            sink,
            PipelineDopDomain {
                min: dop,
                max: dop,
                requires_power_of_two: false,
            },
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .expect("checked fragment");
    let roots = PhysicalExpressionRoots::try_new(&fragment, &Control).expect("expression roots");
    let domain = EvaluationDomainId::new(0);
    let mut invocations = Vec::new();
    let mut bindings = Vec::new();
    for (ordinal, (site, expression)) in roots.sites().iter().enumerate() {
        let use_id = ExpressionUseId::new(ordinal as u32);
        invocations.push(ExpressionInvocation {
            context: ExpressionEffectContext {
                use_id,
                domain,
                demand: expression.demand,
            },
            definition: expression.expr,
            control: ControlShape::Eager,
            arguments: Box::default(),
        });
        bindings.push((*site, use_id));
    }
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
        invocations,
        fragment.expressions(),
        CompilePhase::Validate,
        &Control,
    )
    .expect("expression control");
    let uses = PhysicalRootUses::try_new(&fragment, flow, bindings, &Control).expect("root uses");
    let calls =
        FrozenFragmentCalls::try_new(&fragment, &uses, vec![], &Control).expect("call-free table");
    let result = matches!(
        fragment.sink(),
        FragmentSink::Result | FragmentSink::RootResult(_)
    )
    .then(|| ResultPort {
        scalar_schema: None,
        fragment: fragment.id(),
        output: fragment.nodes()[&root].output.clone(),
        fields: vec![ResultField {
            domain: ResultValueDomain::Plain,
            name: "v".into(),
            alias: None,
            value,
            ty: ty.clone(),
        }]
        .into_boxed_slice(),
    });
    let package = FragmentPackage::try_new(
        FragmentPackageInput {
            constants,
            version: PlanVersionId::try_new([0x5b; 16]).expect("version"),
            required: RequiredContracts::default(),
            pruning: FrozenFragmentPruning::try_new(fragment.id(), vec![], &Control)
                .expect("empty pruning"),
            fragment,
            expression_uses: uses,
            calls,
            cuts,
            result,
            parameters: SemanticParameters::try_new([]).expect("parameters"),
            scans: BTreeMap::new(),
            writes: BTreeMap::new(),
            annotations: Box::default(),
        },
        novarocks_plan_codec::physical_package_v2::FROZEN_PACKAGE_ADMISSION,
        &Control,
    )
    .expect("checked package");
    let wire = novarocks_plan_codec::physical_package_v2::encode_fragment_package(
        &package,
        &novarocks_plan_codec::physical_package_v2::PackageEncodeLimits::FROZEN,
        &Control,
    )
    .expect("package codec");
    proto::FrozenFragment {
        package: wire.encode_to_vec().into(),
    }
}

/// Every fact one raw create carries, so a test can vary exactly one body
/// fact and keep the identity.
#[derive(Clone)]
pub(super) struct RawCreate {
    pub(super) participation: proto::ParticipationRef,
    pub(super) task_id: u32,
    pub(super) backend_override: Option<BackendProcessId>,
    pub(super) kernel_key_low: i64,
    pub(super) pipeline_dop: u32,
    pub(super) sink: RawSink,
    pub(super) instance_ordinal: u32,
    pub(super) initial_domains: Vec<proto::TaskDomainUpdate>,
    pub(super) max_wait_millis: u64,
    pub(super) exchange_input: bool,
    pub(super) exchange_source_backend: Option<BackendProcessId>,
}

impl RawCreate {
    /// A checked, self-contained task: one VALUES node into a count-only root,
    /// frozen for exactly its own parallelism.
    pub(super) fn values(session: &RawBackendSession, task_id: u32) -> Self {
        Self {
            participation: session.participation_under(session.frontend, 1),
            task_id,
            backend_override: None,
            kernel_key_low: session
                .query_id
                .lo
                .wrapping_mul(1024)
                .wrapping_add(i64::from(task_id) * 100 + 1),
            pipeline_dop: 2,
            sink: RawSink::CountOnly,
            instance_ordinal: 0,
            initial_domains: Vec::new(),
            max_wait_millis: 15_000,
            exchange_input: false,
            exchange_source_backend: None,
        }
    }

    pub(super) fn operation(&self) -> proto::TaskOperation {
        use prost::Message;

        let execution = self
            .participation
            .query_execution_id
            .expect("a raw context carries its execution");
        let mut participation = self.participation.clone();
        let backend = match self.backend_override {
            Some(backend) => {
                let backend = proto::BackendProcessId {
                    value: backend.to_bytes().to_vec(),
                };
                participation.backend_process_id = Some(backend.clone());
                backend
            }
            None => participation
                .backend_process_id
                .clone()
                .expect("a raw context carries its backend"),
        };
        let frozen = compiled_fragment(
            1,
            self.pipeline_dop,
            self.exchange_input.then_some((1, false)),
            self.sink,
            &[],
        );
        let metadata = proto::CreationMetadata {
            participation: Some(participation),
            descriptor: Some(proto::TaskDescriptor {
                identity: Some(proto::TaskIdentity {
                    query_execution_id: Some(execution),
                    stage_id: 1,
                    task_id: self.task_id,
                    backend_process_id: Some(backend),
                }),
                task_key: Some(common::UniqueId {
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
                            delivery: proto::ExchangeDeliveryRequirement::DemandDriven as i32,
                            sources: vec![proto::TaskExchangeSource {
                                task: Some(proto::TaskIdentity {
                                    query_execution_id: Some(execution),
                                    stage_id: 2,
                                    task_id: self.task_id,
                                    backend_process_id: self
                                        .exchange_source_backend
                                        .map(|process| proto::BackendProcessId {
                                            value: process.to_bytes().to_vec(),
                                        })
                                        .or_else(|| self.participation.backend_process_id.clone()),
                                }),
                                task_key: Some(common::UniqueId {
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

/// ACKs and protocol outcomes belong only to the explicit Verdict branch.
pub(super) fn receipt_verdict(
    receipt: &proto::TaskOperationReceipt,
) -> Result<&proto::TaskOperationVerdict> {
    novarocks_task_codec::operation::decode_receipt_header(
        receipt,
        novarocks_proto_codec::FieldPath::root("scenario_receipt"),
    )?;
    match receipt
        .result
        .as_ref()
        .context("receipt omitted its mandatory result")?
    {
        proto::task_operation_receipt::Result::Verdict(verdict) => Ok(verdict),
        proto::task_operation_receipt::Result::DomainFailure(failure) => {
            bail!(
                "expected a protocol verdict, got the producer's domain failure without an ACK: {failure:?}"
            )
        }
    }
}

pub(super) fn outcome(
    receipt: &proto::TaskOperationReceipt,
) -> Result<proto::TaskOperationOutcome> {
    Ok(proto::TaskOperationOutcome::try_from(
        receipt_verdict(receipt)?.outcome,
    )?)
}

fn same_create_entity(
    original: &proto::task_operation_verdict::Ack,
    replay: &proto::task_operation_verdict::Ack,
) -> bool {
    let (
        proto::task_operation_verdict::Ack::CreateTask(original),
        proto::task_operation_verdict::Ack::CreateTask(replay),
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
        // These creation scenarios require the same live roles. A marker
        // cannot arrive from an already exited producer; report its original
        // process failure while the remaining roles can still shut down.
        context
            .recheck_live_process_launch_identities()
            .with_context(|| format!("verify original processes while observing {subject}"))?;
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
#[derive(Default)]
struct CreationPayloadLifetime {
    intent: SqlIntentOwner,
}

fn creation_statement(sql: &str, attempts: &[u64], fault: Option<&'static str>) -> StatementSpec {
    StatementSpec {
        sql: sql.into(),
        faults: fault.map_or_else(Vec::new, |kind| {
            (0..REQUIRED_BACKENDS).map(|index| (index, kind)).collect()
        }),
        observations: (0..REQUIRED_BACKENDS)
            .flat_map(|index| {
                attempts.iter().map(move |&attempt| {
                    (
                        index,
                        novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol
                            .as_str(),
                        Some(attempt),
                    )
                })
            })
            .collect(),
    }
}

impl Scenario for CreationPayloadLifetime {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "native_creation",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "native-creation/creation-payload-lifetime"
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &std::path::Path,
        cluster_size: usize,
        binary: &std::path::Path,
        deadline: std::time::Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        Ok(Some(self.intent.freeze(
            vec![
                creation_statement(NID2_FENCE_QUERY, &[1], None),
                creation_statement(BASELINE_QUERY, &[1], Some("create-task-ack-drop")),
                creation_statement(NID2_FENCE_QUERY, &[1], None),
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
        let idle = FrontendCreationGauges::scrape(context)?;
        context.action(format!("idle frontend creation gauges: {idle:?}"));

        // Exact acknowledgements release the payload mid-statement.
        let pending = PendingRead::start(context, self.intent.take(0)?)?;
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
        let selected = self.intent.take(1)?;
        let tokens = vec![selected.token.clone(); REQUIRED_BACKENDS];
        let result = run_baseline_query(context, &selected);
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
        let pending = PendingRead::start(context, self.intent.take(2)?)?;
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

fn run_baseline_query(context: &mut ScenarioContext, selected: &SelectedStatement) -> Result<()> {
    let mut connection = mysql_actor::connect_until(
        context.mysql_user(),
        context.mysql_port(),
        context.deadline(),
    )?;
    let observation = arm_on_every_backend(
        context,
        selected,
        "create-task-ack-drop",
        connection.connection_id(),
        BASELINE_QUERY,
    )?;
    let rows: Vec<i64> = connection
        .query(BASELINE_QUERY)
        .context("run the baseline read")?;
    ensure!(
        rows == vec![1, 2],
        "the baseline read returned {rows:?} instead of [1, 2]"
    );
    connection.finish()?;
    let deadline = context.deadline();
    context
        .handle()
        .await_query_lifecycle_statement_observation(&observation, true, deadline)?;
    context
        .handle()
        .drain_query_lifecycle_statement_observation(&observation)?;
    Ok(())
}

/// One statement running on its own connection, cancellable by id.
struct PendingRead {
    observation: novarocks_cluster_harness::QueryLifecycleStatementObservation,
    thread: Option<thread::JoinHandle<Result<()>>>,
    connection_id: u32,
    done: mpsc::Receiver<Result<Vec<i64>, mysql::Error>>,
}

impl Drop for PendingRead {
    fn drop(&mut self) {
        if let Some(actor) = self.thread.take() {
            match actor.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    eprintln!("Original delayed read actor cleanup failed: {error:#}")
                }
                Err(_) => eprintln!("Original delayed read actor panicked during join"),
            }
        }
    }
}

impl PendingRead {
    fn start(context: &mut ScenarioContext, selected: SelectedStatement) -> Result<Self> {
        let (done_tx, done) = mpsc::sync_channel(1);
        let connection = mysql_actor::connect_for_cancellation_until(
            context.mysql_user(),
            context.mysql_port(),
            context.deadline(),
        )?;
        let connection_id = connection.connection_id();
        let observation = selected.arm(context, connection_id, &selected.sql)?;
        let sql = selected.sql;
        let thread = thread::Builder::new()
            .name("native-creation-read".to_owned())
            .spawn(move || -> Result<()> {
                connection.run(|connection| {
                    let outcome = connection.query(sql);
                    done_tx
                        .send(outcome)
                        .context("publish the delayed read result")
                })
            })
            .context("start the delayed read")?;
        Ok(Self {
            observation,
            thread: Some(thread),
            connection_id,
            done,
        })
    }

    fn still_running(&self) -> bool {
        matches!(self.done.try_recv(), Err(mpsc::TryRecvError::Empty))
    }

    fn finish(mut self, context: &mut ScenarioContext) -> Result<Vec<i64>> {
        let outcome = self
            .done
            .recv_timeout(context.remaining("await the delayed read")?)
            .context("the delayed read did not finish")?;
        self.thread
            .take()
            .expect("original delayed read actor")
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
#[derive(Default)]
struct FixedPlanRecovery {
    intent: SqlIntentOwner,
}

impl Scenario for FixedPlanRecovery {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "native_creation",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        "native-creation/fixed-plan-recovery"
    }

    fn test_session_plan(
        &self,
        _launch: &ScenarioLaunchConfig,
        _root: &std::path::Path,
        cluster_size: usize,
        binary: &std::path::Path,
        deadline: std::time::Instant,
    ) -> Result<Option<novarocks_cluster_harness::test_sessions::ClusterTestSessionPlan>> {
        Ok(Some(self.intent.freeze(
            vec![
                creation_statement(NID2_FENCE_QUERY, &[1], None),
                creation_statement(NID2_FENCE_QUERY, &[1, 2], None),
            ],
            &[0, 1, 2],
            cluster_size,
            binary,
            deadline,
        )?))
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let baseline = resource_snapshot(context)?;
        let idle = FrontendCreationGauges::scrape(context)?;

        // The reference: how many static plans one clean run freezes.
        let pending = PendingRead::start(context, self.intent.take(0)?)?;
        let clean_observation = pending.observation.clone();
        let rows = pending.finish(context)?;
        ensure!(rows.len() == 2, "the clean read returned {rows:?}");
        let deadline = context.deadline();
        let clean_terminal = context
            .handle()
            .await_query_lifecycle_statement_observation(&clean_observation, true, deadline)?;
        context
            .handle()
            .drain_query_lifecycle_statement_observation(&clean_observation)?;
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
        let frozen_before = FrontendCreationGauges::scrape(context)?;
        let created_before = (0..context.handle().be_count())
            .map(|index| context.handle().be_log_count(index, CREATE_APPLIED))
            .collect::<Result<Vec<_>>>()?;
        let mut stream = MysqlStream::connect(
            context.mysql_user(),
            context.mysql_port(),
            context.remaining("open the recovered read")?,
        )?;
        let recovery_observation =
            self.intent
                .take(1)?
                .arm(context, stream.connection_id()?, NID2_FENCE_QUERY)?;
        stream.send_query(NID2_FENCE_QUERY)?;
        let target = await_fresh_task_create(context, &created_before)?;
        context
            .handle()
            .kill_be(target)
            .with_context(|| format!("kill admitted BE[{target}]"))?;
        await_backend_exit(context, target)?;
        assert_two_sleep_rows(&mut stream)?;
        let deadline = context.deadline();
        let terminal = context
            .handle()
            .await_query_lifecycle_statement_observation(&recovery_observation, true, deadline)?;
        context
            .handle()
            .drain_query_lifecycle_statement_observation(&recovery_observation)?;
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
// Native normal-close retention keeps exact late-frame facts while execution
// resources admit new Tasks and Contexts throughout the original request horizon.
// ---------------------------------------------------------------------------

struct NormalCloseRetainedAdmission {
    byte_corpus: bool,
}

impl Scenario for NormalCloseRetainedAdmission {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "native_creation",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

    fn name(&self) -> &'static str {
        // Preserve the original runner identities for these two regressions.
        if self.byte_corpus {
            "native-creation/normal-close-byte-capacity"
        } else {
            "native-creation/normal-close-count-capacity"
        }
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        require_three_backends(context)?;
        let exchange_trust = context.handle().native_backend_probe_trust()?;
        let exchange_authorization = authorization_header(&exchange_trust)?;
        let source_process = match exchange_trust.local_process_identity() {
            Some(novarocks_native_trust::NativeProcessIdentity::Backend(process)) => process,
            _ => bail!("Exchange probe requires its authenticated backend process"),
        };
        let retained = RawBackendSession::establish(context, 0)?;
        let live = RawBackendSession::establish(context, 0)?;
        let receiver = |session: &RawBackendSession, id| {
            let mut create = RawCreate::values(session, id);
            create.pipeline_dop = 1;
            create.exchange_input = true;
            create.exchange_source_backend = Some(source_process);
            create
        };
        // These are fixed input corpus sizes, with no storage-capacity claim.
        let retained_receivers = if self.byte_corpus { 32 } else { 2 };
        let mut originals = Vec::with_capacity(retained_receivers);
        for index in 0..retained_receivers {
            let create = receiver(&retained, index as u32 + 1);
            ensure_accepted(&retained, &create)?;
            await_installed(context, &retained, &create)?;
            originals.push(create);
        }
        let active = receiver(&live, 1);
        ensure_accepted(&live, &active)?;
        await_installed(context, &live, &active)?;
        retained.release_when_ready(context)?;
        let mut wire_bytes = 0usize;
        for original in &originals {
            wire_bytes += assert_normal_close(&retained, original, &exchange_authorization)?;
        }
        context.action(format!(
            "released one Context and read {} exact typed normal-close facts ({} protobuf bytes observed on the wire)",
            originals.len(), wire_bytes,
        ));

        let next_task = receiver(&live, 2);
        ensure_accepted(&live, &next_task)?;
        await_installed(context, &live, &next_task)?;
        // An already-live exact receiver remains usable while a different
        // Context retains its normal-close facts and another Task is admitted.
        let response = exchange_probe(&live, &active, true, &exchange_authorization)?;
        ensure!(
            matches!(response.outcome,novarocks_task_codec::exchange_unit_codec::DecodedExchangeDeliveryOutcome::Observed{facts,..} if facts.receiver_closed.is_none()),
            "the live exact receiver was evicted or prematurely closed: {response:?}"
        );
        for original in &originals {
            assert_normal_close(&retained, original, &exchange_authorization)?;
        }
        context.action("the existing live Context admitted and installed another Task, its first exact receiver accepted EOS, and every original retained normal-close fact remained readable");
        live.release_when_ready(context)?;
        assert_normal_close(&live, &active, &exchange_authorization)?;
        assert_normal_close(&live, &next_task, &exchange_authorization)?;

        let fresh = RawBackendSession::establish(context, 0)?;
        let fresh_create = receiver(&fresh, 1);
        ensure_accepted(&fresh, &fresh_create)?;
        await_installed(context, &fresh, &fresh_create)?;
        for original in &originals {
            assert_normal_close(&retained, original, &exchange_authorization)?;
        }
        assert_normal_close(&live, &active, &exchange_authorization)?;
        assert_normal_close(&live, &next_task, &exchange_authorization)?;
        fresh.release_when_ready(context)?;
        context.action("with both earlier Contexts retaining exact normal-close facts, a fresh Context established and its new receiver was Accepted and Installed; retained facts did not occupy an execution-admission position");

        // Preserve the original real-clock horizon check. Start after positive
        // retained-fact observations; no shorter retention or simulated clock
        // is introduced to make the final forget happen sooner.
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
        // Expiry authorizes asynchronous physical forget. Observe the real
        // old stream's result, rather than using unrelated fresh admission as
        // a proxy for that callback's actual return.
        loop {
            let forgotten =
                exchange_probe(&retained, &originals[0], false, &exchange_authorization)?;
            if matches!(
                forgotten.outcome,
                novarocks_task_codec::exchange_unit_codec::DecodedExchangeDeliveryOutcome::Refused(
                    _
                )
            ) {
                break;
            }
            normal_close_response_bytes(&originals[0], &forgotten)?;
            thread::sleep(
                context
                    .remaining("observe actual normal-close forget after the legal horizon")?
                    .min(POLL_INTERVAL),
            );
        }
        for original in &originals {
            let forgotten = exchange_probe(&retained, original, false, &exchange_authorization)?;
            ensure!(
                matches!(forgotten.outcome, novarocks_task_codec::exchange_unit_codec::DecodedExchangeDeliveryOutcome::Refused(_)),
                "expired Context still retained an original normal-close fact: {forgotten:?}"
            );
        }
        let recovered = RawBackendSession::establish(context, 0)?;
        let recovered_create = receiver(&recovered, 1);
        ensure_accepted(&recovered, &recovered_create)?;
        await_installed(context, &recovered, &recovered_create)?;
        recovered.release_when_ready(context)?;
        context.action("after the legal horizon and actual forget, all old exact streams were rejected without closure evidence; a fresh Context and receiver also admitted successfully");
        Ok(())
    }
}

pub(super) fn ensure_accepted(session: &RawBackendSession, create: &RawCreate) -> Result<()> {
    let receipt = session.apply(create.operation(), "accept the capacity receiver")?;
    ensure!(
        outcome(&receipt)? == proto::TaskOperationOutcome::Accepted,
        "receiver Create was not accepted: {receipt:?}"
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
        let Some(proto::task_operation_verdict::Ack::CreateTask(ack)) =
            receipt_verdict(&receipt)?.ack.clone()
        else {
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

#[derive(Debug)]
struct ExchangeProbeResponse {
    outcome: novarocks_task_codec::exchange_unit_codec::DecodedExchangeDeliveryOutcome,
    wire_bytes: usize,
}

fn exchange_probe(
    session: &RawBackendSession,
    create: &RawCreate,
    eos: bool,
    exchange_authorization: &str,
) -> Result<ExchangeProbeResponse> {
    use prost::Message;
    let Some(proto::task_operation::Operation::CreateTask(request)) = create.operation().operation
    else {
        bail!("exchange probe requires its actual frozen Create");
    };
    let metadata = proto::CreationMetadata::decode(request.creation_metadata.as_ref())?;
    let descriptor = metadata
        .descriptor
        .context("probe Create omits descriptor")?;
    let inbound = descriptor
        .topology
        .as_ref()
        .and_then(|topology| topology.inbound.first())
        .context("probe Create omits frozen input")?;
    let source = inbound
        .sources
        .first()
        .context("probe input omits source")?;
    let stream = proto::ExchangeStreamIdentity {
        source_task: source.task.clone(),
        destination_task: descriptor.identity.clone(),
        source_task_key_hi: source.task_key.context("probe source omits kernel")?.hi,
        source_task_key_lo: source.task_key.context("probe source omits kernel")?.lo,
        sender_ordinal: source.sender_ordinal,
        destination_task_key_hi: descriptor
            .task_key
            .context("probe destination omits kernel")?
            .hi,
        destination_task_key_lo: descriptor
            .task_key
            .context("probe destination omits kernel")?
            .lo,
        destination_node_id: inbound.destination_node_id,
    };
    use novarocks_execution_contract::ordered_retained_stream::exchange::{
        ExchangeUnitDemand, ExchangeUnitItem, ExchangeUnitSequence,
    };
    use novarocks_task_codec::exchange_unit_codec as codec;
    let stream = novarocks_task_codec::exchange_flow_codec::decode_stream(
        &stream,
        novarocks_proto_codec::FieldPath::root("probe_stream"),
    )?;
    let sequence = ExchangeUnitSequence::FIRST;
    let item = if eos {
        ExchangeUnitItem::End(sequence)
    } else {
        ExchangeUnitItem::Unit {
            demand: ExchangeUnitDemand {
                sequence,
                wire_bytes: 1,
            },
            end_after_unit: false,
        }
    };
    let response: proto::DeliverExchangeUnitResponse = raw_unary(
        session.connector.clone(),
        "/novarocks.NovaRocksGrpc/DeliverExchangeUnit",
        exchange_authorization,
        codec::encode_delivery_request(codec::DecodedExchangeUnitFrame {
            stream,
            sender_count: u32::try_from(inbound.sources.len())?,
            sender_id: 0,
            backend_number: 0,
            item,
            payload: if eos {
                bytes::Bytes::new()
            } else {
                bytes::Bytes::from_static(&[0xff])
            },
            next_demand: None,
        }),
        session.original_deadline,
    )?;
    // Decode binds the response to this original Create's complete Task,
    // kernel, source ordinal and item identity; a close for another route fails.
    Ok(ExchangeProbeResponse {
        wire_bytes: response.encoded_len(),
        outcome: codec::decode_delivery_response(
            &response,
            stream,
            sequence,
            novarocks_proto_codec::FieldPath::root("exchange_probe"),
        )?,
    })
}

fn assert_normal_close(
    session: &RawBackendSession,
    create: &RawCreate,
    exchange_authorization: &str,
) -> Result<usize> {
    let response = exchange_probe(session, create, false, exchange_authorization)?;
    normal_close_response_bytes(create, &response)
}

fn normal_close_response_bytes(
    _create: &RawCreate,
    response: &ExchangeProbeResponse,
) -> Result<usize> {
    // The actual decoder above already bound the complete frozen route and
    // sequence. Count the original wire response, not a reconstructed proof.
    ensure!(
        matches!(response.outcome,
            novarocks_task_codec::exchange_unit_codec::DecodedExchangeDeliveryOutcome::Observed { facts, .. }
            if facts.receiver_closed.is_some()),
        "late Unit must retain this exact frozen route's typed normal close: {response:?}"
    );
    Ok(response.wire_bytes)
}

/// A real Accepted-to-Quiesce schedule. Preparation can legitimately finish
/// first: no test delay is installed, and this scenario does not claim that
/// any particular instruction ran while the backend was Preparing.
struct AcceptedPreparationControlRaces;

impl Scenario for AcceptedPreparationControlRaces {
    fn declaration(&self) -> crate::scenario::ScenarioDeclaration {
        crate::scenarios::declarations::for_module(
            "native_creation",
            self.name(),
            self.is_explicit_stage(),
            self.is_compatibility_stage(),
        )
    }

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
            let Some(proto::task_operation_verdict::Ack::CreateTask(ack)) =
                receipt_verdict(&receipt)?.ack.clone()
            else {
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
            outcome(&replay)? == proto::TaskOperationOutcome::Idempotent,
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
                outcome(&cancel)?,
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
            replay_fence.participation == fence.participation
                && replay_fence.fence_version == fence.fence_version
                && replay_fence.accepted_tasks == expected,
            "Quiesce replay changed its cut or membership: {fence:?} -> {replay_fence:?}"
        );
        context.action("exact normal Cancel progressed; a third receiver's Accepted receipt was immediately followed by Quiesce, which froze and replayed all three exact identities without a Preparing hold");

        let late = receiver(4);
        let rejected = session.apply(late.operation(), "late Create after Quiesce")?;
        ensure!(
            outcome(&rejected)? == proto::TaskOperationOutcome::ParticipationTerminalReceipt
                && receipt_verdict(&rejected)?.ack.is_none(),
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
                    outcome(&duplicate)?,
                    proto::TaskOperationOutcome::Accepted | proto::TaskOperationOutcome::NotReady
                ),
                "duplicate Establish reopened admission: {duplicate:?}"
            );
            if let Some(proto::task_operation_verdict::Ack::Participation(ack)) =
                receipt_verdict(&duplicate)?.ack.clone()
            {
                ensure!(
                    ack.state != proto::ParticipationState::Active as i32
                        && ack.state != proto::ParticipationState::Establishing as i32,
                    "duplicate Establish returned an open Context: {ack:?}"
                );
            }
            let rejected =
                session.apply(late.operation(), "late Create after duplicate Establish")?;
            ensure!(
                outcome(&rejected)? == proto::TaskOperationOutcome::ParticipationTerminalReceipt
                    && receipt_verdict(&rejected)?.ack.is_none(),
                "duplicate Establish revived late Create: {rejected:?}"
            );
        }
        context.action("late Create returned ParticipationTerminalReceipt; two original Establish replays left the Participation fenced and could not reopen admission");
        observe_terminal_stop_and_fence(context, &session, &fence, &expected)?;
        reject_invalid_covered_subscriptions(context, &session, &expected)?;
        let released = session.release_when_ready(context)?;
        ensure!(
            released == proto::ReleaseParticipationOutcome::Released,
            "normally fenced Participation did not release: {released:?}"
        );
        let late_establish = session.apply(
            session
                .establish_request
                .clone()
                .context("raw session lost its original Establish")?,
            "original Establish after normal Release",
        )?;
        ensure!(
            outcome(&late_establish)? == proto::TaskOperationOutcome::ParticipationTerminalReceipt,
            "late Establish revived a released Context: {late_establish:?}"
        );
        let late_create = session.apply(late.operation(), "late Create after normal Release")?;
        ensure!(
            outcome(&late_create)? == proto::TaskOperationOutcome::ParticipationTerminalReceipt
                && receipt_verdict(&late_create)?.ack.is_none(),
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
        query_execution_id: create.participation.query_execution_id,
        stage_id: 1,
        task_id: create.task_id,
        backend_process_id: create.participation.backend_process_id.clone(),
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
            session.original_deadline,
        )?,
        "native control probe",
    )
}

pub(super) fn quiesce_session(
    session: &RawBackendSession,
) -> Result<proto::QuiesceParticipationAck> {
    let receipt = session_control(
        session,
        proto::task_control_operation::Control::QuiesceParticipation(
            proto::QuiesceParticipationRequest {
                participation: Some(session.participation_under(session.frontend, 1)),
            },
        ),
    )?;
    let Some(proto::task_operation_verdict::Ack::QuiesceParticipation(ack)) =
        receipt_verdict(&receipt)?.ack.clone()
    else {
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
    let target_participation = session.participation_under(session.frontend, 1);
    let base = proto::SubscribeTaskStatusRequest {
        participation: Some(target_participation.clone()),
        generation: 1,
        required_identities: identities.to_vec(),
        ..Default::default()
    };
    let mut missing_generation = base.clone();
    missing_generation.generation = 0;
    let cursor_only = proto::SubscribeTaskStatusRequest {
        participation: Some(target_participation.clone()),
        ..Default::default()
    };
    let mut future_task = base.clone();
    future_task.task_convergence_cursors = vec![proto::TaskConvergenceCursor {
        identity: Some(identities[0].clone()),
        current_version: u64::MAX,
    }];
    let mut future_participation = base.clone();
    future_participation.participation_convergence_cursor =
        Some(proto::ParticipationConvergenceCursor {
            participation: Some(target_participation),
            current_version: u64::MAX,
        });
    let mut foreign_participation = base;
    foreign_participation.participation_convergence_cursor =
        Some(proto::ParticipationConvergenceCursor {
            participation: Some(session.participation_under(session.frontend, 2)),
            current_version: 0,
        });
    let requests = [
        ("missing generation", missing_generation),
        ("cursor-only legacy mode", cursor_only),
        ("future Task convergence cursor", future_task),
        ("future Context convergence cursor", future_participation),
        ("cross-Context convergence cursor", foreign_participation),
    ];
    let connector = session.connector.clone();
    let authorization = session.authorization.clone();
    let budget = context.remaining("reject malformed covered subscriptions")?;
    tokio::runtime::Runtime::new()?.block_on(async move {
        tokio::time::timeout(budget, async move {
            for (name, request) in requests {
                let stream = connector.connect().await.map_err(anyhow::Error::msg)?;
                let (mut sender, connection) = h2::client::handshake(stream).await?;
                let connection_job = tokio::spawn(connection);
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
                connection_job.abort();
                let _ = connection_job.await;
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
    fence: &proto::QuiesceParticipationAck,
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
    fence: &proto::QuiesceParticipationAck,
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
    fence: Option<&proto::QuiesceParticipationAck>,
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
        participation: Some(session.participation_under(session.frontend, 1)),
        generation: 1,
        required_identities: expected.clone(),
        ..Default::default()
    };
    let budget = context.remaining("observe real covered terminal and actual-stop facts")?;
    tokio::runtime::Runtime::new()?.block_on(async move {
        tokio::time::timeout(budget, async move {
            let stream = connector.connect().await.map_err(anyhow::Error::msg)?;
            let (mut sender, connection) = h2::client::handshake(stream).await?;
            let connection_job = tokio::spawn(connection);
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
                                    ensure!(receipt.participation == fence.participation && receipt.fence_version == fence.fence_version && receipt.accepted_tasks == expected, "covered Quiesce changed the exact fence: {receipt:?}");
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
                            proto::task_status_stream_event::Event::ParticipationConvergence(_) => {}
                        }
                    }
                }
                bail!("covered stream ended before exact terminal/stop/fence evidence; trailers={:?}", body.trailers().await?)
            }.await;
            connection_job.abort();
            let _ = connection_job.await;
            result
        }).await.context("covered terminal/actual-stop probe exhausted its scenario deadline")?
    })
}

#[cfg(test)]
mod finite_sql_authorization_tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn creation_payload_submissions_are_distinct_and_only_the_second_has_the_fault() {
        let scenario = CreationPayloadLifetime::default();
        let deadline = Instant::now() + Duration::from_secs(10);
        let plan = scenario
            .test_session_plan(
                &ScenarioLaunchConfig::default(),
                std::path::Path::new("/unused"),
                3,
                &std::env::current_exe().unwrap(),
                deadline,
            )
            .unwrap()
            .unwrap();
        plan.validate_selected_intents(3).unwrap();
        let first = scenario.intent.take(0).unwrap();
        let lost_ack = scenario.intent.take(1).unwrap();
        let canceled = scenario.intent.take(2).unwrap();
        assert_eq!(first.sql, NID2_FENCE_QUERY);
        assert_eq!(canceled.sql, first.sql);
        assert_ne!(first.token, canceled.token);
        assert_eq!(lost_ack.sql, BASELINE_QUERY);
        assert_eq!(
            lost_ack.faults,
            (0..3)
                .map(|index| (index, "create-task-ack-drop"))
                .collect::<Vec<_>>()
        );
        assert!(first.faults.is_empty() && canceled.faults.is_empty());
        assert_eq!(first.deadline, deadline);
        assert!(scenario.intent.take(1).is_err());
        assert!(scenario.intent.take(3).is_err());
    }

    #[test]
    fn fixed_plan_recovery_keeps_explicit_attempts_without_reusing_a_submission() {
        let scenario = FixedPlanRecovery::default();
        let deadline = Instant::now() + Duration::from_secs(10);
        let plan = scenario
            .test_session_plan(
                &ScenarioLaunchConfig::default(),
                std::path::Path::new("/unused"),
                3,
                &std::env::current_exe().unwrap(),
                deadline,
            )
            .unwrap()
            .unwrap();
        plan.validate_selected_intents(3).unwrap();
        let first = scenario.intent.take(0).unwrap();
        let recovery = scenario.intent.take(1).unwrap();
        assert_eq!(first.sql, NID2_FENCE_QUERY);
        assert_eq!(first.sql, recovery.sql);
        assert_ne!(first.token, recovery.token);
        assert!(first.faults.is_empty() && recovery.faults.is_empty());
        assert!(scenario.intent.take(1).is_err());
        let spec = creation_statement(NID2_FENCE_QUERY, &[1, 2], None);
        assert_eq!(
            spec.observations,
            (0..3)
                .flat_map(|index| [1, 2].map(move |attempt| (
                    index,
                    novarocks_failpoint::observation_marker::ObservationAction::TaskProtocol
                        .as_str(),
                    Some(attempt)
                )))
                .collect::<Vec<_>>()
        );
    }
}
