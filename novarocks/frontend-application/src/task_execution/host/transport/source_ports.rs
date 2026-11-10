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

//! Seams implemented only by P10a's original per-attempt Source. They are not
//! alternate protocol owners, result stores, Work factories or quota wallets.

use super::{
    capacity::{EncodingWindow, ReturnedPhysical},
    queue_input::{OriginalQueuedInput, SendInputLoan, SendKey},
};
use crate::task_execution::intent::{OperationAcknowledgement, OperationIntent};
use novarocks_control_runtime::{exit_custody::ExitObligation, obligation::StepRegistration};
use novarocks_control_runtime_core::scope::WorkKind;
use novarocks_coordination_contract::MonotonicInstant;
use novarocks_execution_contract::{ParticipationRef, TaskOperationId};
use novarocks_workload_control::WorkError;
use std::{any::Any, sync::Arc};

pub(crate) struct EncodingJob {
    /// Same reserve_child_next_or_wait guard, bound to one actual child Work.
    pub(crate) exit: ExitObligation,
    pub(crate) window: EncodingWindow,
}

pub(crate) enum BodyOutcome {
    /// The same real WC grant is installed in the canonical Source record.
    InputInstalled,
    /// Individual original ACK cells contain every exact operation result.
    ReceiptsRecorded,
    /// GatedWork destroyed pre-handoff, captures and unused original authority.
    Unused(novarocks_control_runtime::exit_work::GatedNotStarted),
    /// Original first-party cause; the Source pins this same cause through its
    /// final logical/error consumer, independently of Core input consumption.
    LocalFailure(WorkError),
    RawFailure(Box<dyn Any + Send>),
}

/// Per-job port bound by the original Source record. Implementations keep a
/// Weak reference to the Join-owning record; routes need no invented numeric
/// JobKey and holding the port must not make an owner/Join cycle.
pub(crate) trait TransportReports: Send + Sync + 'static {
    /// Cells already exist before the original operation/QueuedInputGranted
    /// becomes visible. This never allocates/reissues authors from a WorkId.
    /// The original canonical acknowledgement retains its real raw cause and
    /// rich body. P10a projects the passive AP fact from this same record.
    fn record_ack(&self, key: SendKey, ack: OperationAcknowledgement);
    /// Independent original paid progress input; never deferred until final
    /// RPC outcome and never proof that Establish unary was polled.
    fn send_started(&self, at: MonotonicInstant);
    /// Invoked exactly after this same original unary future's first poll.
    /// Independent prepaid progress input, not the job's single result role.
    fn establish_rpc_started(
        &self,
        operation: TaskOperationId,
        participation: ParticipationRef,
        at: MonotonicInstant,
    );
    /// Consume the original WC input and bind its actual host record before
    /// exposing the passive reference. Ordinary refusal returns the full input.
    fn install_input(&self, input: OriginalQueuedInput) -> Result<(), OriginalQueuedInput>;
    /// No borrowed grant or numeric reference is accepted as a producer.
    /// This hook binds the same paid child guard, report/captures/consumer and
    /// real ExitCustody; it does not create another Work or capacity invoice.
    fn bind_encoding(
        &self,
        guard: StepRegistration,
        kind: WorkKind,
        quoted_bytes: usize,
    ) -> EncodingJob;
    /// Gated capture teardown parks ONLY the same financial partition here.
    /// This is not physical-return/actual-exit evidence. Source retains it and
    /// its original report/Join obligations. Once its unique Join truly exits,
    /// Source transfers the staged bundle to that same Native row's protected
    /// physical retirement; only return_physical marks that later boundary.
    fn stage_quota(
        &self,
        leg: super::capacity::QuotaLeg,
        original: novarocks_control_runtime::obligation::ReservationObligation,
    ) -> Result<(), novarocks_control_runtime::obligation::ReservationObligation>;
    /// Only the original Apply reaper may call this AFTER consuming the same
    /// credentialed physical-cleanup report AND its actual unique Join. The
    /// opaque destructor/body returning is insufficient and never invokes this.
    /// Source retains these obligations independently of the destroyed opaque
    /// value, until original report/custody/child facts permit release/finish.
    /// Return failure
    /// preserves every original object and remains UNKNOWN, never fake exit.
    fn return_physical(&self, physical: ReturnedPhysical) -> Result<(), ReturnedPhysical>;
}

/// Rich requests are resolved by the same canonical host record. In particular
/// an Establish returns the exact Arc frozen BEFORE QA authorization. Rebuilding
/// an equivalent Establish request here is forbidden.
pub(crate) struct ResolvedBatch {
    pub(crate) operations: Vec<OperationIntent>,
    /// Each loan is resolved from the exact operation and generation-bearing
    /// QueuedInputGrantRef in the core's original batch, before this binding is
    /// exposed. record()/operation ids alone cannot supply a generation.
    pub(crate) queued_inputs: Vec<SendInputLoan>,
    /// Same original binding's preallocated generation association. The
    /// factory only moves this storage; it never clones N entries on a poller.
    pub(crate) keys: Arc<[SendKey]>,
}

/// Original per-job result obligation, supplied by the same admitted Source.
/// Actor adapters move their same CausalInput here; replacement adapters move
/// the QA original return leg. No constructor accepts a WorkId or mints Work.
/// Completing it reports body outcome only; Source independently retains its
/// actual Join, report/custody, physical-return and cleanup responsibilities.
pub(crate) trait TransportCompletionObligation: Send {
    fn body_finished(self: Box<Self>, outcome: BodyOutcome);
}
pub(crate) struct TransportLeg {
    pub(crate) exit: ExitObligation,
    pub(crate) handoff: Arc<novarocks_control_runtime::exit_work::HandoffCell>,
    pub(crate) panic: novarocks_control_runtime::exit_work::PanicSink,
    pub(crate) result: Box<dyn TransportCompletionObligation>,
}
