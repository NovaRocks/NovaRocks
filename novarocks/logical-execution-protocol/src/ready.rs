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

//! The domain's proof stays opaque. This owner validates its original source,
//! activation, decoded End and common protocol gates, then freezes one cut.

use crate::{
    LogicalRequestToken, LogicalSourceId,
    failure::{LogicalRequestFailure, query_failure},
    input::InternalEndReceiptId,
    start::{LogicalPlanIdentity, ObservationBudgets},
};
use novarocks_coordination_contract::{
    AttemptActivationIdentity, AttemptComputationCut, MonotonicInstant, QueryExecutionError,
    QueryFailureReason,
};
use novarocks_execution_contract::{TaskIdentity, root_result::RootResultEnd};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReadyRefusal {
    ForeignSource,
    ForeignActivation,
    ForeignRoot,
    ForeignPlan,
    DuplicateSource,
    MissingEndReceipt,
    ConflictingEndReceipt,
    DuplicateProposal,
    Concluded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReadyProposal {
    pub source: LogicalSourceId,
    pub token: LogicalRequestToken,
    pub activation: AttemptActivationIdentity,
    pub root: TaskIdentity,
    pub plan: LogicalPlanIdentity,
    pub receipt: InternalEndReceiptId,
    pub end: RootResultEnd,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReadyAcceptance {
    pub source: LogicalSourceId,
    pub token: LogicalRequestToken,
    pub cut: AttemptComputationCut,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PublicationState {
    Waiting,
    InFlight,
    Published,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ConvergenceSnapshot {
    pub all_participations_closed: bool,
    pub remote_tracking_ended: bool,
    pub local_resources_exited: bool,
}

#[derive(Debug)]
struct ConsumerSource {
    source: LogicalSourceId,
    activation: AttemptActivationIdentity,
    root: TaskIdentity,
    supervised_exit: bool,
    source_closed: bool,
    exited: bool,
    decoded: Option<(InternalEndReceiptId, RootResultEnd)>,
    proposal: Option<ReadyProposal>,
}

#[derive(Debug)]
pub(crate) struct InternalReady {
    plan: LogicalPlanIdentity,
    budgets: ObservationBudgets,
    source: Option<ConsumerSource>,
    accepted: Option<ReadyAcceptance>,
    diagnostics: PublicationState,
    convergence: PublicationState,
}

impl InternalReady {
    pub(crate) fn is_source(
        &self,
        source: LogicalSourceId,
        activation: AttemptActivationIdentity,
    ) -> bool {
        self.source
            .as_ref()
            .is_some_and(|s| s.source == source && s.activation == activation)
    }
    pub(crate) fn has_activation(&self, activation: AttemptActivationIdentity) -> bool {
        self.source
            .as_ref()
            .is_some_and(|s| s.activation == activation)
    }
    pub(crate) fn has_pending_proposal(&self) -> bool {
        self.source.as_ref().is_some_and(|s| s.proposal.is_some())
    }
    pub(crate) fn new(plan: LogicalPlanIdentity, budgets: ObservationBudgets) -> Self {
        Self {
            plan,
            budgets,
            source: None,
            accepted: None,
            diagnostics: PublicationState::Waiting,
            convergence: PublicationState::Waiting,
        }
    }
    pub(crate) const fn accepted(&self) -> Option<ReadyAcceptance> {
        self.accepted
    }
    pub(crate) const fn diagnostics(&self) -> PublicationState {
        self.diagnostics
    }
    pub(crate) const fn convergence(&self) -> PublicationState {
        self.convergence
    }

    pub(crate) fn register(
        &mut self,
        source: LogicalSourceId,
        activation: AttemptActivationIdentity,
        root: TaskIdentity,
        plan: LogicalPlanIdentity,
        supervised_exit: bool,
        current: bool,
        concluded: bool,
    ) -> Result<Option<ReadyProposal>, ReadyRefusal> {
        if concluded || self.accepted.is_some() {
            return Err(ReadyRefusal::Concluded);
        }
        if !current {
            return Err(ReadyRefusal::ForeignActivation);
        }
        if root.query_execution_id() != activation.execution() {
            return Err(ReadyRefusal::ForeignRoot);
        }
        if plan != self.plan {
            return Err(ReadyRefusal::ForeignPlan);
        }
        if self
            .source
            .as_ref()
            .is_some_and(|s| s.activation == activation)
        {
            return Err(ReadyRefusal::DuplicateSource);
        }
        let retired = self.source.as_mut().and_then(|old| old.proposal.take());
        self.source = Some(ConsumerSource {
            source,
            activation,
            root,
            supervised_exit,
            source_closed: false,
            exited: false,
            decoded: None,
            proposal: None,
        });
        Ok(retired)
    }

    /// The core replies to every retired proposal before releasing its source.
    pub(crate) fn retire_source(
        &mut self,
        activation: AttemptActivationIdentity,
    ) -> Result<Option<ReadyProposal>, ReadyRefusal> {
        if !self
            .source
            .as_ref()
            .is_some_and(|s| s.activation == activation)
        {
            return Err(ReadyRefusal::ForeignActivation);
        }
        Ok(self
            .source
            .take()
            .and_then(|mut source| source.proposal.take()))
    }

    pub(crate) fn take_pending_proposal(&mut self) -> Option<ReadyProposal> {
        self.source
            .as_mut()
            .and_then(|source| source.proposal.take())
    }

    #[cfg(test)]
    pub(crate) fn source_matches(
        &self,
        source: LogicalSourceId,
        activation: AttemptActivationIdentity,
        root: TaskIdentity,
    ) -> bool {
        self.source
            .as_ref()
            .is_some_and(|s| s.source == source && s.activation == activation && s.root == root)
    }

    pub(crate) fn decoded_end(
        &mut self,
        source: LogicalSourceId,
        activation: AttemptActivationIdentity,
        receipt: InternalEndReceiptId,
        end: RootResultEnd,
    ) -> Result<(), ReadyRefusal> {
        let record = self.record_mut(source, activation)?;
        match record.decoded {
            Some(held) if held == (receipt, end) => Ok(()),
            Some(_) => Err(ReadyRefusal::ConflictingEndReceipt),
            None => {
                record.decoded = Some((receipt, end));
                Ok(())
            }
        }
    }

    /// A valid proposal may wait for the exact root/seal/Establish facts.
    pub(crate) fn propose(
        &mut self,
        proposal: ReadyProposal,
        concluded: bool,
    ) -> Result<Option<ReadyAcceptance>, ReadyRefusal> {
        if proposal.plan != self.plan {
            return Err(ReadyRefusal::ForeignPlan);
        }
        let record = self.source.as_ref().ok_or(ReadyRefusal::ForeignSource)?;
        if record.source != proposal.source {
            return Err(ReadyRefusal::ForeignSource);
        }
        if record.activation != proposal.activation {
            return Err(ReadyRefusal::ForeignActivation);
        }
        if record.root != proposal.root {
            return Err(ReadyRefusal::ForeignRoot);
        }
        if record.decoded != Some((proposal.receipt, proposal.end)) {
            return Err(ReadyRefusal::MissingEndReceipt);
        }
        if let Some(accepted) = self.accepted {
            return Ok(Some(ReadyAcceptance {
                token: proposal.token,
                ..accepted
            }));
        }
        if concluded {
            return Err(ReadyRefusal::Concluded);
        }
        let record = self
            .source
            .as_mut()
            .expect("validated source remains owned");
        if record.proposal.is_some() {
            return Err(ReadyRefusal::DuplicateProposal);
        }
        record.proposal = Some(proposal);
        Ok(None)
    }

    /// The root End and consumer decoded End come from independent owners.
    pub(crate) fn try_accept(
        &mut self,
        now: MonotonicInstant,
        current: AttemptActivationIdentity,
        root_finished: bool,
        success_sealed: bool,
        establish_ready: bool,
        consumed_end: Option<RootResultEnd>,
        delivery_failed_or_pending: bool,
        concluded: bool,
    ) -> Result<Option<ReadyAcceptance>, QueryExecutionError> {
        if concluded
            || self.accepted.is_some()
            || delivery_failed_or_pending
            || !root_finished
            || !success_sealed
            || !establish_ready
        {
            return Ok(None);
        }
        let Some(record) = self.source.as_mut() else {
            return Ok(None);
        };
        let Some(proposal) = record.proposal else {
            return Ok(None);
        };
        if record.activation != current {
            return Ok(None);
        }
        if record.source_closed || record.exited {
            if record.supervised_exit && !record.exited {
                return Ok(None);
            }
            return Err(query_failure(
                QueryFailureReason::LogicalOwnerStopped,
                "internal proof source exited before its cut",
            ));
        }
        if consumed_end != Some(proposal.end) {
            return if consumed_end.is_none() {
                Ok(None)
            } else {
                Err(LogicalRequestFailure::ResultProtocolViolation.error())
            };
        }
        let cut = AttemptComputationCut {
            activation: current,
            root: record.root,
            cut: now,
            diagnostic_deadline: now.saturating_add(self.budgets.diagnostics),
            convergence_deadline: now.saturating_add(self.budgets.convergence),
            end: proposal.end,
        };
        let accepted = ReadyAcceptance {
            source: record.source,
            token: proposal.token,
            cut,
        };
        record.proposal = None;
        self.accepted = Some(accepted);
        Ok(Some(accepted))
    }

    pub(crate) fn source_closed(
        &mut self,
        source: LogicalSourceId,
        activation: AttemptActivationIdentity,
    ) -> Result<Option<QueryExecutionError>, ReadyRefusal> {
        let accepted = self.accepted.is_some();
        let record = self.record_mut(source, activation)?;
        record.source_closed = true;
        if accepted || record.supervised_exit {
            return Ok(None);
        }
        record.exited = true;
        Ok(Some(query_failure(
            QueryFailureReason::LogicalOwnerStopped,
            "the internal consumer exited before readiness",
        )))
    }

    pub(crate) fn exit_observed(
        &mut self,
        source: LogicalSourceId,
        activation: AttemptActivationIdentity,
        outcome: Option<Result<(), QueryExecutionError>>,
    ) -> Result<Option<QueryExecutionError>, ReadyRefusal> {
        let accepted = self.accepted.is_some();
        let record = self.record_mut(source, activation)?;
        if record.exited {
            return Ok(None);
        }
        record.exited = true;
        if accepted {
            return Ok(None);
        }
        Ok(Some(match outcome {
            Some(Err(error)) => error,
            Some(Ok(())) | None => query_failure(
                QueryFailureReason::LogicalOwnerStopped,
                "the internal consumer exited before readiness",
            ),
        }))
    }

    /// Independent publication machines. Deadline yields an honest partial
    /// snapshot; it never marks missing diagnostics or work exit complete.
    pub(crate) fn begin_diagnostics(&mut self, now: MonotonicInstant, complete: bool) -> bool {
        let Some(accepted) = self.accepted else {
            return false;
        };
        if self.diagnostics != PublicationState::Waiting
            || (!complete && now < accepted.cut.diagnostic_deadline)
        {
            return false;
        }
        self.diagnostics = PublicationState::InFlight;
        true
    }
    pub(crate) fn diagnostics_published(&mut self) -> bool {
        if self.diagnostics != PublicationState::InFlight {
            return false;
        }
        self.diagnostics = PublicationState::Published;
        true
    }
    pub(crate) fn begin_convergence(
        &mut self,
        now: MonotonicInstant,
        snapshot: ConvergenceSnapshot,
    ) -> bool {
        let Some(accepted) = self.accepted else {
            return false;
        };
        let complete = snapshot.local_resources_exited
            && (snapshot.all_participations_closed || snapshot.remote_tracking_ended);
        if self.convergence != PublicationState::Waiting
            || (!complete && now < accepted.cut.convergence_deadline)
        {
            return false;
        }
        self.convergence = PublicationState::InFlight;
        true
    }
    pub(crate) fn convergence_published(&mut self) -> bool {
        if self.convergence != PublicationState::InFlight {
            return false;
        }
        self.convergence = PublicationState::Published;
        true
    }
    pub(crate) fn pending_observation_deadline(&self) -> Option<MonotonicInstant> {
        let cut = self.accepted?.cut;
        match (self.diagnostics, self.convergence) {
            (PublicationState::Waiting, PublicationState::Waiting) => {
                Some(cut.diagnostic_deadline.min(cut.convergence_deadline))
            }
            (PublicationState::Waiting, _) => Some(cut.diagnostic_deadline),
            (_, PublicationState::Waiting) => Some(cut.convergence_deadline),
            _ => None,
        }
    }
    pub(crate) fn original_consumer_exited(&self) -> bool {
        self.source.as_ref().is_some_and(|s| s.exited)
    }
    fn record_mut(
        &mut self,
        source: LogicalSourceId,
        activation: AttemptActivationIdentity,
    ) -> Result<&mut ConsumerSource, ReadyRefusal> {
        let record = self.source.as_mut().ok_or(ReadyRefusal::ForeignSource)?;
        if record.source != source {
            return Err(ReadyRefusal::ForeignSource);
        }
        if record.activation != activation {
            return Err(ReadyRefusal::ForeignActivation);
        }
        Ok(record)
    }
}

#[cfg(test)]
mod tests;
