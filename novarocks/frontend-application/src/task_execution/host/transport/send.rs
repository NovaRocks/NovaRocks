// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! One Source-bound original job. Only the Source's credentialed carrier
//! polls this factory and owns its one Join. This module never spawns F.
use super::{
    capacity::{EncodingCustody, QueueCustody},
    prepare::PrepareSend,
    send_binding::ResolvedBatch,
    send_reports::SendReports,
    settlement::{AcceptedReceipts, PendingReceipt},
    source_ports::{BodyOutcome, TransportLeg},
};
use crate::{
    native::{
        apply_send_owner::ApplySendPort,
        task_transport::wire::{self, AckAddress, AttemptWireFacts, SentOperation},
        transport::Client,
        transport_capacity::{NativeTransportCapacity, TransportCapacityLane},
    },
    task_execution::{
        host::custody::TransportJoinRegistration, host::failure_lifecycle::LifecycleStarts,
        intent::OperationAcknowledgement,
    },
};
use novarocks_control_runtime::{
    exit_work::{GatedOutcome, GatedWork, run_future},
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::identity::Generation;
use novarocks_coordination_contract::MonotonicClock;
use novarocks_execution_contract::OperationOutcome;
use novarocks_workload_control::joint_admission::{
    NativeAsyncActionAuthority, NativeAsyncActionQuote,
};
use std::{any::Any, sync::Arc};

/// Constructed only by the same admitted Source. Resolved operations borrow
/// its canonical original allocations, and queue holds the same split quota.
/// original_quote describes this actual factory/layout plus complete original
/// request/reply backing; it is not inferred from encoded byte estimates.
pub(crate) struct BoundSend {
    pub(crate) backend: novarocks_types::BackendProcessId,
    pub(crate) control_progress: bool,
    pub(crate) scope_generation: Generation,
    pub(crate) ledger: RuntimeLedger,
    pub(crate) scope: ScopeHandle,
    pub(crate) layout: NativeTransportCapacity,
    pub(crate) transport: novarocks_task_codec::TransportBudget,
    pub(crate) queue: QueueCustody,
    pub(crate) resolved: ResolvedBatch,
    pub(crate) authority: NativeAsyncActionAuthority,
    pub(crate) original_quote: NativeAsyncActionQuote,
    pub(crate) encoding: novarocks_native_adapter::native_codec::NativeUnaryEncodingPlan,
    pub(crate) wire: AttemptWireFacts,
    pub(crate) client: Client,
    pub(crate) apply: ApplySendPort,
    pub(super) reports: SendReports,
    pub(crate) clock: Arc<dyn MonotonicClock>,
    pub(crate) lifecycle: Option<LifecycleStarts>,
    pub(super) deadline: Option<tokio::time::Instant>,
    pub(crate) original_captures: Box<dyn Any + Send>,
}
pub(crate) struct SendJob {
    pub(crate) leg: TransportLeg,
    pub(crate) registration: TransportJoinRegistration,
    pub(crate) bound: Box<BoundSend>,
}
struct Captures {
    operations: Vec<crate::task_execution::intent::OperationIntent>,
    keys: Arc<[super::queue_input::SendKey]>,
    queue: QueueCustody,
    wire: AttemptWireFacts,
    transport: novarocks_task_codec::TransportBudget,
    client: Client,
    reports: SendReports,
    clock: Arc<dyn MonotonicClock>,
    lifecycle: Option<LifecycleStarts>,
    deadline: Option<tokio::time::Instant>,
    _original: Box<dyn Any + Send>,
    panic: novarocks_control_runtime::exit_work::PanicSink,
}
fn sent(intent: &crate::task_execution::intent::OperationIntent) -> SentOperation {
    SentOperation {
        operation_id: intent.operation_id(),
        kind: intent.kind(),
        address: AckAddress::of(intent),
        lease_renewal: crate::native::task_transport::wire::is_lease_renewal(intent),
        establish_participation: match intent {
            crate::task_execution::intent::OperationIntent::EstablishParticipation(value) => {
                Some(value.participation())
            }
            _ => None,
        },
    }
}
fn local_refusal(reports: &SendReports, item: PendingReceipt, outcome: OperationOutcome) {
    reports.record_ack(
        item.key,
        OperationAcknowledgement::local_refusal(
            item.sent.operation_id,
            item.sent.kind,
            crate::task_execution::error::local_protocol_refusal(item.sent.kind, outcome),
        ),
    );
}
/// The factory itself is used for layout quotation, without constructing a
/// dummy F/request/capture. P10a adds its exact carrier input/return layouts.
pub(crate) fn future_layout() -> std::alloc::Layout {
    fn of<F: std::future::Future<Output = ()>>(_factory: fn(SendJob) -> F) -> std::alloc::Layout {
        std::alloc::Layout::new::<F>()
    }
    of(run)
}
pub(crate) fn run(job: SendJob) -> impl std::future::Future<Output = ()> {
    let SendJob {
        leg,
        registration,
        bound,
    } = job;
    let TransportLeg {
        exit,
        handoff,
        panic,
        result,
    } = leg;
    let BoundSend {
        backend,
        control_progress,
        scope_generation,
        ledger,
        scope,
        layout,
        transport,
        queue,
        resolved,
        authority,
        original_quote,
        encoding,
        wire,
        client,
        apply,
        reports,
        clock,
        lifecycle,
        deadline,
        original_captures,
    } = *bound;
    let lane = if control_progress {
        TransportCapacityLane::Control
    } else {
        TransportCapacityLane::Ordinary
    };
    let first = resolved
        .operations
        .first()
        .expect("Source binds a nonempty exact original batch");
    let method = first.native_method();
    assert_eq!(resolved.operations.len(), resolved.queued_inputs.len());
    assert_eq!(resolved.operations.len(), resolved.keys.len());
    let parent = queue.parent_reservation();
    assert!(queue.matches_send(&registration, &resolved.keys));
    let client_for_pre = client.clone();
    let pre = prepare_input(PreparationInput {
        authority,
        original_quote,
        ledger,
        scope,
        parent,
        scope_generation,
        layout,
        backend,
        lane,
        encoding_bytes: encoding.protobuf_bytes(),
        method,
        client: client_for_pre,
        apply,
        inputs: resolved.queued_inputs,
        registration,
        keys: resolved.keys.clone(),
        deadline,
    });
    let captures = Captures {
        operations: resolved.operations,
        keys: resolved.keys,
        queue,
        wire,
        transport,
        client,
        reports: reports.clone(),
        clock: clock.clone(),
        lifecycle,
        deadline,
        _original: original_captures,
        panic: panic.clone(),
    };
    let body: fn(
        Captures,
        Result<super::prepare::ReadyAction, super::prepare::PreparationFailure>,
    ) -> _ = execute_prepared;
    let running = GatedWork::new(exit, captures, handoff, panic).run(scope_generation, pre, body);
    async move {
        let outcome = match running.await {
            Ok(GatedOutcome::Completed(outcome)) => outcome,
            Ok(GatedOutcome::NotStarted(reason)) => BodyOutcome::Unused(reason),
            Err(error) => BodyOutcome::RawFailure(Box::new(error)),
        };
        // This moves the original job leg; it does NOT publish ActualExit or refund.
        // Source waits its actual unique Join and Native physical-return boundary.
        result.body_finished(outcome);
    }
}

async fn encode_and_send(
    mut captures: Captures,
    mut ready: super::prepare::ReadyAction,
    mut encoding: EncodingCustody,
) -> BodyOutcome {
    let mut operations = Vec::with_capacity(captures.operations.len());
    let mut pending = Vec::with_capacity(captures.operations.len());
    for (intent, key) in captures.operations.iter().zip(captures.keys.iter()) {
        let item = PendingReceipt {
            key: *key,
            sent: sent(intent),
        };
        match wire::encode_operation(intent, &captures.wire) {
            Ok(operation) => {
                operations.push((intent.native_method() == novarocks_proto_codec::native_rpc::NativeRpcMethod::ApplyTaskControlOperations, operation));
                pending.push(item);
            }
            Err(detail) => {
                tracing::warn!(detail=%detail, kind=intent.kind().as_str(), "task operation cannot be encoded");
                local_refusal(
                    &captures.reports,
                    item,
                    OperationOutcome::InvalidStateOrRequest,
                );
            }
        }
    }
    if operations.is_empty() {
        return BodyOutcome::ReceiptsRecorded;
    }
    let mut requests = match wire::encode_method_batches(operations, captures.transport) {
        Ok(requests) => requests,
        Err(error) => {
            for item in pending {
                local_refusal(&captures.reports, item, OperationOutcome::ResourceExhausted);
            }
            return BodyOutcome::RawFailure(Box::new(error));
        }
    };
    if requests.len() != 1 {
        for item in pending {
            local_refusal(
                &captures.reports,
                item,
                OperationOutcome::InvalidStateOrRequest,
            );
        }
        return BodyOutcome::LocalFailure(novarocks_workload_control::WorkError::Conflict);
    }
    let request = requests.pop().expect("one same-method original run");
    let actual = match &request.request {
        wire::EncodedMethodRequest::Ordinary(value) => prost::Message::encoded_len(value),
        wire::EncodedMethodRequest::Control(value) => prost::Message::encoded_len(value),
    };
    if let Err(error) = encoding.shrink_to(actual) {
        for item in pending {
            local_refusal(&captures.reports, item, OperationOutcome::ResourceExhausted);
        }
        return BodyOutcome::RawFailure(Box::new(error));
    }
    if let Err(error) = captures.queue.mark_in_flight() {
        for item in pending {
            local_refusal(&captures.reports, item, OperationOutcome::ResourceExhausted);
        }
        return BodyOutcome::RawFailure(Box::new(error));
    }
    let expires = tokio::time::Instant::now() + request.deadline;
    let expires = captures
        .deadline
        .map_or(expires, |limit| expires.min(limit));
    let publisher = ready
        .publisher
        .take()
        .expect("same accepted original reply publisher");
    let mut action_tail = Some(
        publisher
            .protocol_tail()
            .expect("same accepted original action source"),
    );
    let mut receipts = AcceptedReceipts::new(pending, publisher, captures.reports.clone());
    if !receipts.freeze_dispatch(expires).await {
        return BodyOutcome::ReceiptsRecorded;
    }
    let response = super::execute::send_operations_in(
        &captures.client,
        request.request,
        expires,
        &receipts,
        captures.clock.as_ref(),
        captures.lifecycle.as_ref(),
        &mut ready.call,
        &mut action_tail,
        novarocks_task_codec::resource_preflight::OperationReplyDecodePlan::for_transport(
            captures.transport,
        ),
    )
    .await;
    match response {
        Ok(response) => receipts.response(response),
        Err(dispatch) => receipts.uniform(dispatch),
    }
    BodyOutcome::ReceiptsRecorded
}

struct PreparationInput {
    authority: NativeAsyncActionAuthority,
    original_quote: NativeAsyncActionQuote,
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    parent: novarocks_control_runtime_core::identity::ReservationId,
    scope_generation: Generation,
    layout: NativeTransportCapacity,
    backend: novarocks_types::BackendProcessId,
    lane: TransportCapacityLane,
    encoding_bytes: usize,
    method: novarocks_proto_codec::native_rpc::NativeRpcMethod,
    client: Client,
    apply: ApplySendPort,
    inputs: Vec<super::queue_input::SendInputLoan>,
    registration: TransportJoinRegistration,
    keys: Arc<[super::queue_input::SendKey]>,
    deadline: Option<tokio::time::Instant>,
}
async fn prepare_input(
    input: PreparationInput,
) -> Result<super::prepare::ReadyAction, super::prepare::PreparationFailure> {
    assert!(
        input
            .inputs
            .iter()
            .zip(input.keys.iter())
            .enumerate()
            .all(|(index, (loan, key))| loan.matches_send(&input.registration, index, *key)),
        "original send owns every exact loan"
    );
    match PrepareSend::new(
        &input.authority,
        input.original_quote,
        input.ledger,
        input.scope,
        input.parent,
        input.scope_generation,
        input.layout,
        input.backend,
        input.lane,
        input.encoding_bytes,
        input.method,
        input.client,
        input.apply,
        input.inputs,
        input.registration,
    ) {
        Ok(preparation) => match input.deadline {
            Some(deadline) => tokio::time::timeout_at(deadline, preparation)
                .await
                .map_err(|_| {
                    super::prepare::PreparationFailure::Governance(
                        novarocks_workload_control::WorkError::CapacityWaitTimeout,
                    )
                })?,
            None => preparation.await,
        },
        Err(error) => Err(error),
    }
}
async fn execute_prepared(
    captures: Captures,
    prepared: Result<super::prepare::ReadyAction, super::prepare::PreparationFailure>,
) -> BodyOutcome {
    let mut ready = match prepared {
        Ok(ready) => ready,
        Err(error) => {
            return captures.reports.preparation_failed(
                &captures.operations,
                &captures.keys,
                error,
            );
        }
    };
    captures.reports.send_started(captures.clock.now());
    let guard = match ready.row.take_encoding() {
        Ok(guard) => guard,
        Err(error) => return BodyOutcome::LocalFailure(error),
    };
    let encoding_job = match ready.bind_encoding(guard) {
        Ok(job) => job,
        Err(error) => return BodyOutcome::RawFailure(Box::new(error)),
    };
    let encoding = EncodingCustody::new(encoding_job.window, encoding_job.returned);
    let panic = captures.panic.clone();
    let child = encode_and_send(captures, ready, encoding);
    match run_future(encoding_job.exit, child, panic).await {
        Ok(outcome) => outcome,
        Err(error) => BodyOutcome::RawFailure(Box::new(error)),
    }
}
/// The same function-pointer B is forced in run(), so the generic RAII tuple
/// and body layouts describe the production objects rather than another closure.
pub(crate) fn backing_layouts() -> [std::alloc::Layout; 5] {
    fn gated<P, F>(
        _: fn(PreparationInput) -> P,
        body: fn(Captures, P::Output) -> F,
    ) -> [std::alloc::Layout; 4]
    where
        P: std::future::Future,
        F: std::future::Future,
    {
        GatedWork::<Captures>::backing_layouts::<P, F>(body)
    }
    fn encoded<F: std::future::Future>(
        _: fn(Captures, super::prepare::ReadyAction, EncodingCustody) -> F,
    ) -> std::alloc::Layout {
        std::alloc::Layout::new::<F>()
    }
    let layouts = gated(prepare_input, execute_prepared);
    [
        layouts[0],
        layouts[1],
        layouts[2],
        layouts[3],
        encoded(encode_and_send),
    ]
}
