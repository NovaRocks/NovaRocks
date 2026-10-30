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

//! Attempt-local internal consumption and actor-issued readiness receipts.
//!
//! Design: ADR-0198 (docs/adr/ADR-0198-one-logical-execution-owner-for-every-distributed-intent.md).

use std::{future::Future, num::NonZeroUsize, pin::Pin, sync::Arc, time::Duration};

use novarocks_coordination_contract::{AttemptActivationIdentity, MonotonicInstant};
use novarocks_execution_contract::{
    ResultPacketSequence, TaskIdentity, root_result::RootResultEnd,
};
use tokio::sync::{mpsc, oneshot, watch};

use super::result::{DeliverySignal, ResultOwnerObservation};
use super::{
    PlanSeal, QueryExecutionError, QueryResultTransport, ResultDeliveryDisposition,
    ResultDeliveryReceipt, ResultRowCarrier, RetainedRootReply, RootRelayResidentWindow,
    RootSegmentDelivery,
};
use crate::coordination::InternalReadyActorPort;

/// This allocation is minted for one exact registered consumer. Correlation
/// values alone cannot recreate its source capability or a decoded receipt.
#[derive(Debug)]
pub(crate) struct InternalConsumerSeal {
    pub(crate) activation: AttemptActivationIdentity,
    pub(crate) root: TaskIdentity,
    pub(crate) plan: PlanSeal,
}

/// Independent nonrenewable observation budgets established at the computation cut.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ComputationObservationBudgets {
    diagnostics: Duration,
    convergence: Duration,
}

impl ComputationObservationBudgets {
    pub const fn new(diagnostics: Duration, convergence: Duration) -> Self {
        assert!(
            !diagnostics.is_zero(),
            "diagnostic observation budget must be nonzero"
        );
        assert!(
            !convergence.is_zero(),
            "convergence observation budget must be nonzero"
        );
        Self {
            diagnostics,
            convergence,
        }
    }
}

impl Default for ComputationObservationBudgets {
    fn default() -> Self {
        Self::new(Duration::from_secs(5), Duration::from_secs(5))
    }
}

/// An accepted computation cut. It grants no publication or object-deletion right.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ComputationReadyToken {
    activation: AttemptActivationIdentity,
    root: TaskIdentity,
    cut: MonotonicInstant,
    diagnostic_deadline: MonotonicInstant,
    convergence_deadline: MonotonicInstant,
    end: RootResultEnd,
}

impl ComputationReadyToken {
    pub(crate) fn accepted(
        source: &InternalConsumerSeal,
        cut: MonotonicInstant,
        end: RootResultEnd,
        budgets: ComputationObservationBudgets,
    ) -> Self {
        Self {
            activation: source.activation,
            root: source.root,
            cut,
            diagnostic_deadline: cut.saturating_add(budgets.diagnostics),
            convergence_deadline: cut.saturating_add(budgets.convergence),
            end,
        }
    }
    pub const fn activation(&self) -> AttemptActivationIdentity {
        self.activation
    }
    pub const fn root(&self) -> TaskIdentity {
        self.root
    }
    pub const fn cut(&self) -> MonotonicInstant {
        self.cut
    }
    pub const fn diagnostic_deadline(&self) -> MonotonicInstant {
        self.diagnostic_deadline
    }
    pub const fn convergence_deadline(&self) -> MonotonicInstant {
        self.convergence_deadline
    }
    pub const fn end(&self) -> RootResultEnd {
        self.end
    }
}

/// The actor accepted the original internal End's actual consumption. Domain
/// completeness and stable receiving custody remain additional prerequisites.
#[derive(Debug)]
pub struct DecodedInternalEndReceipt {
    source: Arc<InternalConsumerSeal>,
    end: RootResultEnd,
}

impl DecodedInternalEndReceipt {
    pub(crate) fn issued(source: Arc<InternalConsumerSeal>, end: RootResultEnd) -> Self {
        Self { source, end }
    }
    pub(crate) fn matches(&self, source: &Arc<InternalConsumerSeal>, end: RootResultEnd) -> bool {
        Arc::ptr_eq(&self.source, source) && self.end == end
    }
    pub fn activation(&self) -> AttemptActivationIdentity {
        self.source.activation
    }
    pub fn root(&self) -> TaskIdentity {
        self.source.root
    }
    pub const fn end(&self) -> RootResultEnd {
        self.end
    }
}

/// Move-only proposal capability supplied only to the registered domain owner.
/// That owner retains this inside its typed proof and calls accept only after
/// its protocol rules and actual receiving-custody transfer have succeeded.
pub struct ComputationReadySource {
    port: InternalReadyActorPort,
    liveness: Option<oneshot::Sender<()>>,
}

impl ComputationReadySource {
    pub(crate) fn issued(port: InternalReadyActorPort) -> (Self, oneshot::Receiver<()>) {
        let (liveness, receiver) = oneshot::channel();
        (
            Self {
                port,
                liveness: Some(liveness),
            },
            receiver,
        )
    }
    pub fn activation(&self) -> AttemptActivationIdentity {
        self.port.origin().activation
    }
    pub fn root(&self) -> TaskIdentity {
        self.port.origin().root
    }
    pub fn plan(&self) -> PlanSeal {
        self.port.origin().plan
    }

    pub async fn accept(
        mut self,
        decoded_end: DecodedInternalEndReceipt,
    ) -> Result<ComputationReadyToken, QueryExecutionError> {
        let accepted = self.port.accept(decoded_end).await?;
        if let Some(liveness) = self.liveness.take() {
            let _ = liveness.send(());
        }
        Ok(accepted)
    }

    pub async fn fail(mut self, error: QueryExecutionError) -> Result<(), QueryExecutionError> {
        self.port.fail(error).await?;
        if let Some(liveness) = self.liveness.take() {
            let _ = liveness.send(());
        }
        Ok(())
    }
}

/// The original retained reply moves into the finite CPU job without a byte copy.
pub struct InternalSegmentDelivery {
    delivery: RootSegmentDelivery,
}

impl InternalSegmentDelivery {
    pub(crate) fn from_root(delivery: RootSegmentDelivery) -> Self {
        Self { delivery }
    }
    pub fn sequence(&self) -> ResultPacketSequence {
        self.delivery.sequence()
    }
    pub fn rows(&self) -> u64 {
        self.delivery.rows()
    }
    pub fn into_parts(self) -> (Arc<RetainedRootReply>, InternalSegmentConsumption) {
        let (reply, signal, resident_window) = self.delivery.into_internal_parts();
        (
            reply,
            InternalSegmentConsumption {
                signal,
                resident_window,
            },
        )
    }
}

/// Complete only after the actual CPU result has relinquished the original input.
pub struct InternalSegmentConsumption {
    signal: DeliverySignal,
    resident_window: Option<RootRelayResidentWindow>,
}

impl InternalSegmentConsumption {
    pub fn complete(mut self) {
        self.resident_window.take();
        self.signal.finish(ResultDeliveryDisposition::Completed);
    }
    pub fn fail(mut self, error: QueryExecutionError) {
        self.resident_window.take();
        self.signal
            .finish(ResultDeliveryDisposition::Failed(Box::new(error)));
    }
}

pub(crate) type InternalEndAcknowledgement =
    oneshot::Sender<Result<DecodedInternalEndReceipt, QueryExecutionError>>;

/// Provisional input, distinct from the client's irrevocable successful EOF.
pub struct InternalEndDelivery {
    source: Arc<InternalConsumerSeal>,
    end: RootResultEnd,
    signal: DeliverySignal,
    checked: Option<oneshot::Receiver<Result<DecodedInternalEndReceipt, QueryExecutionError>>>,
}

impl InternalEndDelivery {
    pub(crate) fn new(
        source: Arc<InternalConsumerSeal>,
        end: RootResultEnd,
    ) -> (Self, ResultDeliveryReceipt, InternalEndAcknowledgement) {
        let (signal, receipt) = DeliverySignal::channel();
        let (acknowledge, checked) = oneshot::channel();
        (
            Self {
                source,
                end,
                signal,
                checked: Some(checked),
            },
            receipt,
            acknowledge,
        )
    }
    pub fn activation(&self) -> AttemptActivationIdentity {
        self.source.activation
    }
    pub const fn end(&self) -> RootResultEnd {
        self.end
    }
    pub async fn complete(mut self) -> Result<DecodedInternalEndReceipt, QueryExecutionError> {
        self.signal.finish(ResultDeliveryDisposition::Completed);
        self.checked
            .take()
            .expect("internal End owns its acknowledgement")
            .await
            .map_err(|_| ResultOwnerObservation::FailureOwnerClosed.into_error())?
    }
    pub fn fail(mut self, error: QueryExecutionError) {
        self.signal
            .finish(ResultDeliveryDisposition::Failed(Box::new(error)));
    }
}

pub enum InternalResultDelivery {
    Segment(InternalSegmentDelivery),
    End(InternalEndDelivery),
}

/// Each activation gets a distinct channel; retiring a consumer never redirects
/// an old CPU receipt or queued payload into its successor.
pub struct InternalResultStream {
    source: Arc<InternalConsumerSeal>,
    carrier: ResultRowCarrier,
    receiver: mpsc::Receiver<InternalResultDelivery>,
    failure: watch::Receiver<Option<QueryExecutionError>>,
    terminal_seen: bool,
}

impl InternalResultStream {
    pub(crate) fn channel(
        source: Arc<InternalConsumerSeal>,
        carrier: ResultRowCarrier,
        capacity: NonZeroUsize,
    ) -> (
        QueryResultTransport<InternalResultDelivery>,
        watch::Sender<Option<QueryExecutionError>>,
        Self,
    ) {
        let (transport, receiver) = QueryResultTransport::channel(capacity);
        let (failure_sender, failure) = watch::channel(None);
        (
            transport,
            failure_sender,
            Self {
                source,
                carrier,
                receiver,
                failure,
                terminal_seen: false,
            },
        )
    }
    pub fn activation(&self) -> AttemptActivationIdentity {
        self.source.activation
    }
    pub const fn carrier(&self) -> ResultRowCarrier {
        self.carrier
    }
    /// The same original failure owner remains observable after provisional
    /// End while a domain waits for its remaining completion facts.
    pub fn current_failure(&self) -> Option<QueryExecutionError> {
        self.failure.borrow().clone()
    }

    pub async fn failure(&mut self) -> QueryExecutionError {
        loop {
            if let Some(error) = self.current_failure() {
                return error;
            }
            if self.failure.changed().await.is_err() {
                return self
                    .current_failure()
                    .unwrap_or_else(|| ResultOwnerObservation::FailureOwnerClosed.into_error());
            }
        }
    }
    pub async fn next(&mut self) -> Result<Option<InternalResultDelivery>, QueryExecutionError> {
        if self.terminal_seen {
            return Ok(None);
        }
        loop {
            if let Some(error) = self.failure.borrow().clone() {
                return Err(error);
            }
            tokio::select! {
                biased;
                changed = self.failure.changed() => {
                    if changed.is_err() { return Err(ResultOwnerObservation::FailureOwnerClosed.into_error()); }
                }
                delivery = self.receiver.recv() => {
                    let delivery = delivery.ok_or_else(|| ResultOwnerObservation::QueueClosedBeforeEnd.into_error())?;
                    self.terminal_seen = matches!(delivery, InternalResultDelivery::End(_));
                    return Ok(Some(delivery));
                }
            }
        }
    }
}

pub struct InternalConsumerActivation {
    pub(crate) stream: InternalResultStream,
    pub(crate) source: ComputationReadySource,
    pub(crate) ready: watch::Receiver<Option<Result<ComputationReadyToken, QueryExecutionError>>>,
}

impl InternalConsumerActivation {
    pub(crate) async fn supervise_exit(
        &self,
    ) -> Result<InternalConsumerExitReporter, crate::coordination::LogicalExecutionActorError> {
        let (sender, receiver) = oneshot::channel();
        self.source.port.supervise_exit(receiver).await?;
        Ok(InternalConsumerExitReporter(sender))
    }

    pub fn into_parts(self) -> (InternalResultStream, ComputationReadySource) {
        (self.stream, self.source)
    }
}

/// Reserved consumer-future exit receipt retained by the process supervisor,
/// outside the destructible domain future and its proof source. Independent
/// CPU jobs and retained input keep their own actual-exit/capacity obligations.
pub(crate) struct InternalConsumerExitReporter(oneshot::Sender<Result<(), QueryExecutionError>>);

impl InternalConsumerExitReporter {
    pub(crate) fn complete(self, outcome: Result<(), QueryExecutionError>) {
        let _ = self.0.send(outcome);
    }
}

pub type InternalConsumerFuture =
    Pin<Box<dyn Future<Output = Result<(), QueryExecutionError>> + Send + 'static>>;

/// The registered application owner implements actual decoding, domain proof
/// and custody. It receives no retry, cancellation or finish-policy callback.
pub trait InternalComputationConsumer: Send + Sync + std::fmt::Debug + 'static {
    /// Close admission to this original consumer's local computation jobs.
    /// The execution owner first revokes the activation's production authority.
    fn begin_local_close(&self);

    /// Observe the same original local jobs and their actual exit. This is
    /// independent of consumer-task, Native, remote and payload convergence.
    /// An owner that never issues local jobs supplies its explicit no-issue
    /// implementation; an empty field or cancellation is not exit evidence.
    fn poll_local_closed(
        &self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), crate::api::StatementNativeHealthFailure>>;

    /// Borrow the original abnormal local custody after its actual consumer
    /// join. Ordinary domain errors have no process-health projection.
    fn local_health_failure(&self) -> Option<crate::api::StatementNativeHealthFailure> {
        None
    }
    fn consume(&self, activation: InternalConsumerActivation) -> InternalConsumerFuture;
}
