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

//! Owned, transport-independent Native root reads. No RPC is installed here.
//!
//! The consumer must retain the returned owner beside every independently
//! allocated wire backing, through Body and the last physical DATA alias.
//! Pure protobuf projection or handler return is not a send-exit receipt.

use std::alloc::Layout;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_execution_contract::root_result::{
    RootReadDeferral, RootReadDeferred, RootReadRefusal, RootResultRead, RootResultReply,
};
use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};
use novarocks_result_contract::RootProfileV1;
use novarocks_worker::TaskExecutionRegistry;
use novarocks_worker::root_result_channel::{
    ContextRootRoute, RootChannelError, RootDeliveryOwner, RootMetadataReservation,
};

/// Closed-route responses carry domain facts without admitting a new context read.
pub enum NativeRootReadResponse {
    Owned(NativeRootResultReply),
    AwaitTerminalControl { accepted_consumed: u64 },
    Deferred(RootReadDeferred),
    Refused(Box<RootReadRefusal>),
}

/// Full copy credit follows the send owner; logical encoded length cannot
/// shrink it. The same owner keeps the existing root delivery position live.
pub struct NativeRootSendOwnership {
    resources: Option<Arc<NativeRootSendResources>>,
}

struct NativeRootSendResources {
    // Physical wire backings live in the caller's wrapper, before this owner.
    // Credit and delivery exit precede the fixed wrapper metadata reservation.
    _delivery: RootDeliveryOwner,
    backing: NativeRootSendBacking,
    _metadata: Option<RootMetadataReservation>,
}

enum NativeRootSendBacking {
    Payload(ResultWriteCredit),
    // ACK-only has no data-position dependency. Its complete two-copy
    // envelope and wrapper share one original fixed metadata reservation.
    ControlMetadata(RootMetadataReservation),
}

impl NativeRootSendOwnership {
    pub fn backing_capacity_bytes(&self) -> usize {
        match &self
            .resources
            .as_ref()
            .expect("live root send owner")
            .backing
        {
            NativeRootSendBacking::Payload(credit) => credit.bytes(),
            NativeRootSendBacking::ControlMetadata(_reservation) => {
                2 * RootProfileV1::ENVELOPE_BYTES
            }
        }
    }
}

impl Clone for NativeRootSendOwnership {
    fn clone(&self) -> Self {
        Self {
            resources: Some(Arc::clone(
                self.resources.as_ref().expect("live root send owner"),
            )),
        }
    }
}

impl Drop for NativeRootSendOwnership {
    fn drop(&mut self) {
        // No Weak, raw Arc, or Deref escapes this strong-only handle. Pinned
        // std Arc::into_inner drops its implicit Weak (freeing the allocation)
        // before returning the last Resources value. Only then can its copy
        // credit, delivery position and metadata reservation physically exit.
        let resources = Arc::into_inner(self.resources.take().expect("live root send owner"));
        drop(resources);
    }
}

/// Declaration order destroys the actual reply/Bytes before its send grant.
/// Exporting ownership is for the future physical transport wrapper; this
/// module neither constructs a Tonic response nor allocates encoded bytes.
pub struct NativeRootResultReply {
    reply: RootResultReply,
    ownership: NativeRootSendOwnership,
}

impl NativeRootResultReply {
    pub fn reply(&self) -> &RootResultReply {
        &self.reply
    }

    /// Clones no payload. Keep this owner until every backing using its
    /// pregrant has actually dropped; a response future is not that proof.
    pub fn ownership(&self) -> NativeRootSendOwnership {
        self.ownership.clone()
    }
}

/// Exact local wrapper layout under the workspace's pinned Rust 1.98.1 Arc
/// representation. No callback, schema tree, or opaque allocator is guessed.
/// Worker owns and covers its existing delivery/credit issuer scaffolds.
pub fn native_root_send_metadata_bytes() -> usize {
    #[repr(C, align(2))]
    struct ArcHeader {
        strong: AtomicUsize,
        weak: AtomicUsize,
    }
    let ownership = Layout::new::<ArcHeader>()
        .extend(Layout::new::<NativeRootSendResources>())
        .expect("fixed root send Arc layout fits usize")
        .0
        .pad_to_align();
    ownership
        .size()
        .checked_add(Layout::new::<NativeRootReadResponse>().size())
        .expect("fixed root reply layout fits usize")
}

/// A lightweight composition handle. Registry lookup mints the read under
/// the original context fence, independent of the retired task's horizon.
pub struct NativeRootResultReader {
    registry: Arc<TaskExecutionRegistry>,
    #[cfg(novarocks_dev_controls)]
    test_controls: Option<Arc<crate::test_controls::NativeTestControls>>,
    #[cfg(novarocks_dev_controls)]
    observation_controls: Option<Arc<crate::test_observation_controls::NativeObservationControls>>,
}

impl NativeRootResultReader {
    pub fn new(registry: Arc<TaskExecutionRegistry>) -> Self {
        Self {
            registry,
            #[cfg(novarocks_dev_controls)]
            test_controls: None,
            #[cfg(novarocks_dev_controls)]
            observation_controls: None,
        }
    }

    #[cfg(novarocks_dev_controls)]
    pub fn with_test_controls(
        mut self,
        controls: Option<Arc<crate::test_controls::NativeTestControls>>,
    ) -> Self {
        self.test_controls = controls;
        self
    }

    #[cfg(novarocks_dev_controls)]
    pub fn with_observation_controls(
        mut self,
        controls: Option<Arc<crate::test_observation_controls::NativeObservationControls>>,
    ) -> Self {
        self.observation_controls = controls;
        self
    }

    /// Observe only first Data and actual EOS settled by the original root owner.
    /// Control-only replies cannot spend this finite stream grant or mint EOS.
    #[cfg(novarocks_dev_controls)]
    fn observe_read(&self, request: &RootResultRead, response: &NativeRootReadResponse) {
        let Some(controls) = &self.observation_controls else {
            return;
        };
        let identity = request.root_task();
        if !controls.owns_backend(identity.backend_process_id()) {
            return;
        }
        let observer =
            match controls.observation_for("grpc-fragment", identity.query_execution_id()) {
                Ok(Some(observer)) => observer,
                Ok(None) => return,
                Err(error) => {
                    tracing::warn!(%error, task = %identity, "root read observation refused");
                    return;
                }
            };
        if !observer.is_active() {
            return;
        }
        let marker = match response {
            NativeRootReadResponse::Owned(reply) => root_reply_marker(reply.reply()),
            NativeRootReadResponse::Refused(_)
            | NativeRootReadResponse::AwaitTerminalControl { .. }
            | NativeRootReadResponse::Deferred(_) => None,
        };
        let Some(marker) = marker else {
            return;
        };
        observer.observe(&marker);
    }

    /// A finite original-role action holds only the second read, before any
    /// additional Data offer. The runner enables it after parsing the first
    /// complete client row; that observation does not invent a Backend ACK.
    #[cfg(novarocks_dev_controls)]
    async fn fail_after_visible_prefix(
        &self,
        request: &RootResultRead,
        admitted: &novarocks_worker::root_result_channel::RootChannelRead,
    ) -> bool {
        if request.wanted().map(|sequence| sequence.get()) != Some(2) {
            return false;
        }
        let Some(controls) = self.test_controls.as_ref() else {
            return false;
        };
        let identity = request.root_task();
        let Some(control) = self.registry.live_task_execution_control_for_test(identity) else {
            return false;
        };
        let claim = match controls.claim_dormant_after_visible_prefix(
            identity.query_execution_id(),
            identity.backend_process_id(),
        ) {
            Ok(Some(claim)) => claim,
            Ok(None) => return false,
            Err(error) => {
                tracing::error!(%error, "visible-prefix failure action could not be claimed");
                return false;
            }
        };
        let cancellation = claim.cancellation();
        loop {
            if !cancellation.is_active() || control.is_stopped() {
                return false;
            }
            match claim.action_ended() {
                Ok(false) => {}
                Ok(true) => return false,
                Err(error) => {
                    tracing::error!(%error, "visible-prefix failure action end could not be observed");
                    return false;
                }
            }
            match controls.after_visible_prefix_enabled(&claim) {
                Ok(true) => break,
                Ok(false) => {}
                Err(error) => {
                    tracing::error!(%error, "visible-prefix failure activation could not be observed");
                    return false;
                }
            }
            // Only this finite role-granted read is suspended. Its original
            // scope lifetime and the caller's future remain the owners of
            // cancellation; no Task failure or ACK is synthesized on exit.
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return false,
                _ = tokio::time::sleep(std::time::Duration::from_millis(20)) => {},
            }
        }
        if !cancellation.is_active() || control.is_stopped() {
            return false;
        }
        let original = crate::task_protocol_fault::injected_task_execution_failure();
        control.report_failure(original.clone());
        let consumed = admitted.accepted_consumed();
        let control = Arc::clone(&control);
        control.clone().on_settled(Box::new(move |snapshot| {
            if !snapshot.as_ref().and_then(|snapshot| snapshot.originating_failure())
                .is_some_and(|accepted| std::ptr::eq(accepted.cause(), original.cause())) {
                return;
            }
            if let Err(error) =
                crate::task_protocol_fault::record_task_execution_failure(identity, &claim)
            {
                control.report_cleanup(&error);
                return;
            }
            let execution = identity.query_execution_id();
            eprintln!(
                "NOVAROCKS_TASK_EXECUTION_FAILURE_AFTER_VISIBLE_PREFIX_ACCEPTED execution_id={}:{}:{} stage={} task={} backend={} token={} accepted_consumed={}",
                execution.query_id().high(), execution.query_id().low(),
                execution.attempt_id().get(), identity.stage_id().get(),
                identity.task_id().get(), identity.backend_process_id(),
                claim.token, consumed
            );
        }));
        true
    }

    pub async fn read(&self, request: &RootResultRead) -> NativeRootReadResponse {
        self.read_with_transport_metadata(request, 0).await
    }

    /// Pregrant the transport's exact post-admission allocation inventory
    /// from the same fixed root metadata envelope, before ACK or projection.
    /// Pre-decode lane/stream allocations remain the caller's responsibility.
    pub(crate) async fn read_with_transport_metadata(
        &self,
        request: &RootResultRead,
        transport_metadata_bytes: usize,
    ) -> NativeRootReadResponse {
        let response = self
            .read_owned_with_transport_metadata(request, transport_metadata_bytes)
            .await;
        #[cfg(novarocks_dev_controls)]
        self.observe_read(request, &response);
        response
    }

    async fn read_owned_with_transport_metadata(
        &self,
        request: &RootResultRead,
        transport_metadata_bytes: usize,
    ) -> NativeRootReadResponse {
        let admitted = match self.registry.context_root_result_route(request) {
            ContextRootRoute::Read(read) => read,
            ContextRootRoute::AwaitTerminalControl { accepted_consumed } => {
                return NativeRootReadResponse::AwaitTerminalControl { accepted_consumed };
            }
            ContextRootRoute::UnknownRoot => {
                return refusal(request, RootChannelError::UnknownRoot);
            }
            ContextRootRoute::Mismatch => return refusal(request, RootChannelError::Identity),
            ContextRootRoute::Preparing => return deferred(request, RootReadDeferral::Preparing),
            ContextRootRoute::Busy(facts) => {
                return deferred(request, RootReadDeferral::ReadHoldersBusy(facts));
            }
            ContextRootRoute::Refused(cause) => return refusal(request, cause),
        };
        // ACK retires queue ownership before data-copy admission. It cannot
        // wait for the data budget that its own retirement makes available.
        // Metadata precedes ACK and every reply/Arc projection; ACK-only's
        // complete two-copy envelope is included in this same reservation.
        let ack_only = request.wanted().is_none();
        let metadata_bytes = native_root_send_metadata_bytes()
            .checked_add(if ack_only {
                2 * RootProfileV1::ENVELOPE_BYTES
            } else {
                0
            })
            .and_then(|bytes| bytes.checked_add(transport_metadata_bytes));
        let Some(metadata_bytes) = metadata_bytes else {
            return refusal(request, RootChannelError::SendGeometry);
        };
        let metadata = match admitted.try_reserve_native_send_metadata(metadata_bytes) {
            Ok(metadata) => metadata,
            Err(RootChannelError::Closed) => {
                return NativeRootReadResponse::AwaitTerminalControl {
                    accepted_consumed: admitted.accepted_consumed(),
                };
            }
            Err(RootChannelError::Capacity(cause)) => {
                return deferred(
                    request,
                    RootReadDeferral::SendMetadataBusy(cause.resource_facts()),
                );
            }
            Err(error) => return refusal(request, error),
        };
        if let Err(error) = admitted.apply_consumed() {
            return if error == RootChannelError::Closed {
                NativeRootReadResponse::AwaitTerminalControl {
                    accepted_consumed: admitted.accepted_consumed(),
                }
            } else {
                refusal(request, error)
            };
        }
        #[cfg(novarocks_dev_controls)]
        if self.fail_after_visible_prefix(request, &admitted).await {
            return NativeRootReadResponse::AwaitTerminalControl {
                accepted_consumed: admitted.accepted_consumed(),
            };
        }
        let (backing, metadata) = if ack_only {
            (NativeRootSendBacking::ControlMetadata(metadata), None)
        } else {
            let credit = match admitted.try_reserve_native_send_backing() {
                Ok(ResultWriteAdmission::Granted(credit)) => credit,
                Ok(ResultWriteAdmission::Blocked) => {
                    return match admitted.native_send_backing_refusal() {
                        Ok(cause) => deferred(
                            request,
                            RootReadDeferral::SendBackingBusy(cause.resource_facts()),
                        ),
                        Err(error) => refusal(request, error),
                    };
                }
                Err(RootChannelError::Closed) => {
                    return NativeRootReadResponse::AwaitTerminalControl {
                        accepted_consumed: admitted.accepted_consumed(),
                    };
                }
                Err(error) => return refusal(request, error),
            };
            (NativeRootSendBacking::Payload(credit), Some(metadata))
        };
        let delivery = match admitted.read().await {
            Ok(delivery) => delivery,
            Err(error) => return refusal(request, error),
        };
        let (reply, guard) = delivery.into_parts();
        #[cfg(novarocks_dev_controls)]
        let reply = {
            let mut reply = reply;
            match write_root_read_fault(self.test_controls.as_deref(), request, &mut reply) {
                Ok(Some(failure)) => return NativeRootReadResponse::Refused(Box::new(failure)),
                Ok(None) => {}
                Err(_) => return refusal(request, RootChannelError::Identity),
            }
            reply
        };
        NativeRootReadResponse::Owned(NativeRootResultReply {
            reply,
            ownership: NativeRootSendOwnership {
                resources: Some(Arc::new(NativeRootSendResources {
                    backing,
                    _delivery: guard,
                    _metadata: metadata,
                })),
            },
        })
    }
}

#[cfg(any(test, novarocks_dev_controls))]
fn root_marker_identity(
    identity: novarocks_execution_contract::task_execution::identity::TaskIdentity,
) -> String {
    let execution = identity.query_execution_id();
    format!(
        "NOVAROCKS_GRPC_FETCH_TYPED query_hi={} query_lo={} attempt={} stage={} task={} backend={} protocol=root-v1",
        execution.query_id().high(),
        execution.query_id().low(),
        execution.attempt_id().get(),
        identity.stage_id().get(),
        identity.task_id().get(),
        identity.backend_process_id(),
    )
}

#[cfg(any(test, novarocks_dev_controls))]
fn root_reply_marker(reply: &RootResultReply) -> Option<String> {
    use novarocks_execution_contract::root_result::RootReadOutcome;
    let prefix = || root_marker_identity(reply.root_task);
    match &reply.outcome {
        RootReadOutcome::Data(data)
            if data.sequence().get() == 1 || data.end_after_data().is_some() =>
        {
            let mut marker = format!(
                "{} outcome=data sequence={} eos={} payload_bytes={}",
                prefix(),
                data.sequence(),
                data.end_after_data().is_some(),
                data.body().len(),
            );
            if let Some(end) = data.end_after_data() {
                use std::fmt::Write;
                write!(
                    marker,
                    " end_sequence={} output_rows={}",
                    end.sequence, end.output_rows
                )
                .expect("writing to String cannot fail");
            }
            Some(marker)
        }
        RootReadOutcome::End(end) => Some(format!(
            "{} outcome=end sequence={} eos=true output_rows={} payload_bytes=0",
            prefix(),
            end.sequence,
            end.output_rows,
        )),
        RootReadOutcome::AwaitTerminalControl
        | RootReadOutcome::AckOnly
        | RootReadOutcome::NotReady
        | RootReadOutcome::Retired
        | RootReadOutcome::Data(_) => None,
    }
}

fn refusal(request: &RootResultRead, cause: RootChannelError) -> NativeRootReadResponse {
    let fact = cause.failure_fact();
    let detail = SafeDetail::truncating("root read was refused by its owning channel");
    NativeRootReadResponse::Refused(Box::new(RootReadRefusal::for_request(
        request, fact, detail,
    )))
}
fn deferred(request: &RootResultRead, reason: RootReadDeferral) -> NativeRootReadResponse {
    NativeRootReadResponse::Deferred(RootReadDeferred::for_request(request, reason))
}

/// This finite action withholds the second write reply until the original FE
/// acknowledges its first consumed Data. It neither depends on a live Task nor
/// manufactures an ACK, Task failure, successful End or resource deferral.
#[cfg(novarocks_dev_controls)]
fn write_root_read_fault(
    controls: Option<&crate::test_controls::NativeTestControls>,
    request: &RootResultRead,
    reply: &mut RootResultReply,
) -> Result<Option<RootReadRefusal>, String> {
    use novarocks_execution_contract::root_result::RootReadOutcome;
    use novarocks_result_contract::{InternalResultDomain, RootOutputKind};
    if request.kind() != RootOutputKind::InternalFacts(InternalResultDomain::PreparedWriteCommitV1)
        || !matches!(request.wanted().map(|sequence| sequence.get()), Some(1 | 2))
    {
        return Ok(None);
    }
    let Some(controls) = controls else {
        return Ok(None);
    };
    let root = request.root_task();
    if request.wanted().unwrap().get() == 1
        && matches!(&reply.outcome, RootReadOutcome::Data(_))
        && let Some(claim) = controls.claim(
            novarocks_failpoint::QueryLifecycleFaultKind::ConnectorWriteRootReadBeforeDataFailure,
            root.query_execution_id(),
            root.backend_process_id(),
            true,
        )?
    {
        if let RootReadOutcome::Data(data) = &reply.outcome
            && (data.sequence().get() != 1 || data.end_after_data().is_some())
        {
            return Err("write before-data action requires a nonterminal first Data".into());
        }
        claim.record_hit()?;
        return Ok(Some(RootReadRefusal::for_request(
            request,
            crate::task_protocol_fault::injected_task_execution_failure().failure_fact(),
            SafeDetail::truncating("injected write Root read failure before first Data"),
        )));
    }
    let Some(claim) = controls.claim(
        novarocks_failpoint::QueryLifecycleFaultKind::ConnectorWriteRootReadAfterPrefixFailure,
        root.query_execution_id(),
        root.backend_process_id(),
        true,
    )?
    else {
        return Ok(None);
    };
    if request.wanted().unwrap().get() == 1 {
        if let RootReadOutcome::Data(data) = &reply.outcome
            && data.end_after_data().is_some()
        {
            return Err("write prefix action requires a nonterminal first Data".into());
        }
        return Ok(None);
    }
    if !matches!(
        &reply.outcome,
        RootReadOutcome::Data(_) | RootReadOutcome::End(_) | RootReadOutcome::NotReady
    ) {
        return Ok(None);
    }
    if reply.accepted_consumed == 0 {
        // The ordinary relay can prefetch before the domain CPU's receipt.
        // Keep the channel's retained item and original identity/ACK frontier;
        // only this granted response withholds its body. The next original
        // request carries any real consumption completed in the meantime.
        reply.outcome = RootReadOutcome::NotReady;
        return Ok(None);
    }
    claim.record_hit()?;
    Ok(Some(RootReadRefusal::for_request(
        request,
        crate::task_protocol_fault::injected_task_execution_failure().failure_fact(),
        SafeDetail::truncating("injected write Root read failure after accepted prefix"),
    )))
}

#[cfg(all(test, novarocks_dev_controls))]
mod write_prefix_tests {
    use super::*;
    use crate::test_controls::tests::{bind_fixture, controlled, end_fixture, task};
    use novarocks_execution_contract::root_result::{
        RootReadOutcome, RootResultData, RootResultEnd,
    };
    use novarocks_result_contract::{InternalResultDomain, RootOutputKind, RootProfileId};
    use std::{num::NonZeroU64, time::Duration};

    fn event_count(fixture: &crate::test_controls::tests::Controlled, event: &str) -> usize {
        fixture
            .role
            .scope()
            .observations()
            .iter()
            .filter(|observation| observation.event == event)
            .count()
    }

    fn reply(
        fixture: &crate::test_controls::tests::Controlled,
        wanted: u64,
        consumed: u64,
    ) -> (RootResultRead, RootResultReply) {
        let root = task(fixture, 1);
        let kind = RootOutputKind::InternalFacts(InternalResultDomain::PreparedWriteCommitV1);
        let request = RootResultRead::try_new(
            root,
            RootProfileId::V1,
            kind,
            NonZeroU64::new(wanted),
            consumed,
            Duration::from_millis(1),
        )
        .unwrap();
        let reply = RootResultReply {
            root_task: root,
            profile: RootProfileId::V1,
            kind,
            accepted_consumed: consumed,
            outcome: RootReadOutcome::Data(
                RootResultData::try_new(
                    kind,
                    NonZeroU64::new(wanted).unwrap(),
                    bytes::Bytes::from_static(b"original body"),
                    None,
                )
                .unwrap(),
            ),
        };
        (request, reply)
    }

    #[test]
    fn real_first_data_is_refused_before_offer_without_any_task_control() {
        let kind =
            novarocks_failpoint::QueryLifecycleFaultKind::ConnectorWriteRootReadBeforeDataFailure;
        let fixture = controlled(kind.as_str());
        bind_fixture(&fixture, kind);
        let (request, mut original) = reply(&fixture, 1, 0);
        let before = original.clone();
        let failure = write_root_read_fault(Some(&fixture.controls), &request, &mut original)
            .unwrap()
            .unwrap();
        assert_eq!(
            failure.fact(),
            crate::task_protocol_fault::injected_task_execution_failure().failure_fact()
        );
        assert_eq!(
            failure.detail().as_str(),
            "injected write Root read failure before first Data"
        );
        assert_eq!(original, before);
        assert_eq!(event_count(&fixture, "claimed"), 1);
        assert_eq!(event_count(&fixture, "hit"), 1);
    }

    #[test]
    fn exact_role_claim_withholds_prefetch_then_refuses_only_after_original_ack() {
        let kind =
            novarocks_failpoint::QueryLifecycleFaultKind::ConnectorWriteRootReadAfterPrefixFailure;
        let fixture = controlled(kind.as_str());
        let binding = bind_fixture(&fixture, kind);
        // No Task registry/control is created: the original Root channel may
        // outlive a normally terminal Task.
        let (first, mut original) = reply(&fixture, 1, 0);
        let before = original.clone();
        assert!(
            write_root_read_fault(Some(&fixture.controls), &first, &mut original)
                .unwrap()
                .is_none()
        );
        assert_eq!(original, before);
        for _ in 0..2 {
            let (prefetch, mut original) = reply(&fixture, 2, 0);
            assert!(
                write_root_read_fault(Some(&fixture.controls), &prefetch, &mut original)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(original.outcome, RootReadOutcome::NotReady);
            assert_eq!(original.accepted_consumed, 0);
            assert_eq!(event_count(&fixture, "claimed"), 1);
            assert_eq!(event_count(&fixture, "hit"), 0);
        }
        let (acked, mut original) = reply(&fixture, 2, 1);
        let failure = write_root_read_fault(Some(&fixture.controls), &acked, &mut original)
            .unwrap()
            .unwrap();
        assert_eq!(
            failure.fact(),
            crate::task_protocol_fault::injected_task_execution_failure().failure_fact()
        );
        assert_eq!(
            failure.detail().as_str(),
            "injected write Root read failure after accepted prefix"
        );
        assert_eq!(event_count(&fixture, "claimed"), 1);
        assert_eq!(event_count(&fixture, "hit"), 1);
        end_fixture(&fixture, kind, &binding);
        let (ended, mut original) = reply(&fixture, 2, 1);
        let before = original.clone();
        assert!(
            write_root_read_fault(Some(&fixture.controls), &ended, &mut original)
                .unwrap()
                .is_none()
        );
        assert_eq!(original, before);
    }

    #[test]
    fn unbound_foreign_and_client_reads_cannot_spend_the_write_action() {
        let kind =
            novarocks_failpoint::QueryLifecycleFaultKind::ConnectorWriteRootReadAfterPrefixFailure;
        let fixture = controlled(kind.as_str());
        let (request, mut original) = reply(&fixture, 2, 1);
        let before = original.clone();
        assert!(
            write_root_read_fault(Some(&fixture.controls), &request, &mut original)
                .unwrap()
                .is_none()
        );
        assert_eq!(original, before);
        bind_fixture(&fixture, kind);
        let foreign = controlled(kind.as_str());
        let (request, mut original) = reply(&foreign, 2, 1);
        let before = original.clone();
        assert!(
            write_root_read_fault(Some(&fixture.controls), &request, &mut original)
                .unwrap()
                .is_none()
        );
        assert_eq!(original, before);
        let root = task(&fixture, 1);
        let client = RootResultRead::try_new(
            root,
            RootProfileId::V1,
            RootOutputKind::ClientRows,
            NonZeroU64::new(2),
            1,
            Duration::from_millis(1),
        )
        .unwrap();
        assert!(
            write_root_read_fault(Some(&fixture.controls), &client, &mut original)
                .unwrap()
                .is_none()
        );
        assert!(fixture.role.scope().observations().is_empty());
    }

    #[test]
    fn fused_first_end_cannot_masquerade_as_an_incomplete_prefix() {
        let kind =
            novarocks_failpoint::QueryLifecycleFaultKind::ConnectorWriteRootReadAfterPrefixFailure;
        let fixture = controlled(kind.as_str());
        bind_fixture(&fixture, kind);
        let (request, mut original) = reply(&fixture, 1, 0);
        let RootReadOutcome::Data(data) = &mut original.outcome else {
            unreachable!()
        };
        *data = RootResultData::try_new(
            data.kind(),
            data.sequence(),
            data.body().clone(),
            Some(RootResultEnd {
                sequence: NonZeroU64::new(2).unwrap(),
                output_rows: 1,
            }),
        )
        .unwrap();
        assert!(write_root_read_fault(Some(&fixture.controls), &request, &mut original).is_err());
        assert_eq!(event_count(&fixture, "claimed"), 1);
        assert_eq!(event_count(&fixture, "hit"), 0);
    }
}

#[cfg(test)]
mod observation_marker_tests {
    use super::*;
    use bytes::Bytes;
    use novarocks_execution_contract::root_result::{
        RootReadOutcome, RootResultData, RootResultEnd,
    };
    use novarocks_execution_contract::task_execution::identity::TaskIdentity;
    use novarocks_result_contract::{RootOutputKind, RootProfileId};
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use std::num::NonZeroU64;

    fn sequence(value: u64) -> NonZeroU64 {
        NonZeroU64::new(value).unwrap()
    }

    fn reply(outcome: RootReadOutcome) -> RootResultReply {
        RootResultReply {
            root_task: TaskIdentity::new(
                QueryExecutionId::new(QueryId::new(7, 9), AttemptId::new(2).unwrap()).unwrap(),
                StageId::new(3).unwrap(),
                TaskId::new(4).unwrap(),
                BackendProcessId::new_v7(),
            ),
            profile: RootProfileId::V1,
            kind: RootOutputKind::ClientRows,
            accepted_consumed: 0,
            outcome,
        }
    }

    #[test]
    fn empty_end_observation_retains_exact_root_identity_and_original_row_count() {
        let reply = reply(RootReadOutcome::End(RootResultEnd {
            sequence: sequence(1),
            output_rows: 0,
        }));
        let marker = root_reply_marker(&reply).unwrap();
        assert!(marker.contains("query_hi=7 query_lo=9 attempt=2 stage=3 task=4"));
        assert!(marker.contains(&format!("backend={}", reply.root_task.backend_process_id())));
        assert!(marker.contains(
            "protocol=root-v1 outcome=end sequence=1 eos=true output_rows=0 payload_bytes=0"
        ));
        assert!(!marker.contains("packet_seq="));
        assert!(!marker.contains("status="));
    }

    #[test]
    fn data_observation_reports_eos_only_for_an_original_attached_end() {
        for end in [
            None,
            Some(RootResultEnd {
                sequence: sequence(2),
                output_rows: 17,
            }),
        ] {
            let reply = reply(RootReadOutcome::Data(
                RootResultData::try_new(
                    RootOutputKind::ClientRows,
                    sequence(1),
                    Bytes::from_static(b"original rows"),
                    end,
                )
                .unwrap(),
            ));
            let marker = root_reply_marker(&reply).unwrap();
            assert!(marker.contains("outcome=data sequence=1"));
            assert!(marker.contains("payload_bytes=13"));
            assert_eq!(marker.contains("eos=true"), end.is_some());
            assert_eq!(marker.contains("eos=false"), end.is_none());
            assert_eq!(
                marker.contains("end_sequence=2 output_rows=17"),
                end.is_some()
            );
        }
        let later = reply(RootReadOutcome::Data(
            RootResultData::try_new(
                RootOutputKind::ClientRows,
                sequence(2),
                Bytes::from_static(b"later rows"),
                None,
            )
            .unwrap(),
        ));
        assert!(root_reply_marker(&later).is_none());
    }

    #[test]
    fn control_outcomes_do_not_consume_finite_root_observations() {
        for outcome in [
            RootReadOutcome::AwaitTerminalControl,
            RootReadOutcome::AckOnly,
            RootReadOutcome::NotReady,
            RootReadOutcome::Retired,
        ] {
            assert!(root_reply_marker(&reply(outcome)).is_none());
        }
    }
}
