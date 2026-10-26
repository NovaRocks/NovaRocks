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

//! Exact bounded summaries of proposals between causal cuts.
//!
//! Only neutral facts and custody IDs enter this module. The caller disposes
//! every returned losing source after releasing its ingress lock.

use novarocks_execution_contract::{
    ConclusionDecision, SafeDetail, TaskConclusion, TaskFailurePhase, TerminationDetail,
    decide_conclusion, decide_reported_failure,
    failure::{FailureEvidence, FailureFact},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct SourceId(pub u64);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Diagnostic {
    pub phase: TaskFailurePhase,
    pub fact: FailureFact,
    /// Already safe, bounded projection; never an arbitrary error source.
    pub detail: SafeDetail,
    pub cleanup: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Proposal {
    pub conclusion: TaskConclusion,
    pub evidence: Option<FailureEvidence>,
    /// Each handoff receives its own custody ID even when the originating
    /// failure occurrence is the same shared object. IDs do not deduplicate.
    pub source: Option<SourceId>,
    pub diagnostic: Option<Diagnostic>,
}
impl Proposal {
    fn originating(&self) -> bool {
        self.evidence == Some(FailureEvidence::Originating)
            && matches!(
                self.conclusion,
                TaskConclusion::Terminated(TerminationDetail::Failed(_))
            )
    }
    fn decide(&self, state: &ConclusionState) -> ConclusionDecision {
        if state.sealed {
            assert!(
                state.accepted.is_some(),
                "terminal Task retains its accepted conclusion"
            );
            ConclusionDecision::Keep
        } else if let Some(evidence) = self.evidence {
            decide_reported_failure(state.accepted.as_ref(), &self.conclusion, evidence)
        } else {
            decide_conclusion(state.accepted.as_ref(), &self.conclusion)
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ConclusionState {
    pub accepted: Option<TaskConclusion>,
    pub source: Option<SourceId>,
    pub generation: u64,
    pub sealed: bool,
}
impl ConclusionState {
    pub fn apply(&mut self, proposal: &Proposal) -> ConclusionDecision {
        let decision = proposal.decide(self);
        if decision != ConclusionDecision::Keep {
            self.generation = self
                .generation
                .checked_add(1)
                .expect("conclusion generation exhausted");
            self.accepted = Some(proposal.conclusion.clone());
            self.source = proposal.source;
        }
        decision
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Diagnostics {
    pub entries: Vec<Diagnostic>,
    pub dropped: u64,
}
impl Diagnostics {
    pub fn record(&mut self, value: Diagnostic) {
        if self.entries.len() < 4 {
            self.entries.push(value);
        } else {
            self.dropped = self.dropped.saturating_add(1);
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ProposalSummary {
    next_order: u64,
    first: Option<(u64, Proposal)>,
    originating: Option<(u64, Proposal)>,
    originating_diagnostic: Option<(u64, Diagnostic)>,
    other_diagnostics: Vec<(u64, Diagnostic)>,
    omitted_other_diagnostics: u64,
}
#[derive(Debug)]
pub struct SummaryResult {
    pub state: ConclusionState,
    pub diagnostics: Diagnostics,
    /// Candidate sources no longer held by the accepted snapshot.
    pub released: Vec<SourceId>,
}
impl ProposalSummary {
    fn order(&mut self) -> u64 {
        let order = self.next_order;
        self.next_order = order
            .checked_add(1)
            .expect("proposal segment ordinal exhausted");
        order
    }
    fn record_other(&mut self, order: u64, diagnostic: Diagnostic) {
        if self.other_diagnostics.len() < 4 {
            self.other_diagnostics.push((order, diagnostic));
        } else {
            self.omitted_other_diagnostics = self.omitted_other_diagnostics.saturating_add(1);
        }
    }
    /// Returns only sources guaranteed to lose for every legal entry state.
    pub fn push(&mut self, mut proposal: Proposal) -> Option<SourceId> {
        let order = self.order();
        let first_origin = proposal.originating()
            && self.originating.is_none()
            && !self.first.as_ref().is_some_and(|(_, p)| p.originating());
        let diagnostic = proposal.diagnostic.take();
        if first_origin {
            if let Some(diagnostic) = diagnostic {
                self.originating_diagnostic = Some((order, diagnostic));
            }
        } else if let Some(diagnostic) = diagnostic {
            self.record_other(order, diagnostic);
        }
        if self.first.is_none() {
            self.first = Some((order, proposal));
            None
        } else if first_origin {
            self.originating = Some((order, proposal));
            None
        } else {
            proposal.source
        }
    }
    pub fn cleanup(&mut self, diagnostic: Diagnostic) {
        let order = self.order();
        self.record_other(order, diagnostic);
    }
    pub fn candidate_count(&self) -> usize {
        usize::from(self.first.is_some()) + usize::from(self.originating.is_some())
    }
    pub fn diagnostic_count(&self) -> usize {
        self.other_diagnostics.len() + usize::from(self.originating_diagnostic.is_some())
    }
    pub fn is_empty(&self) -> bool {
        self.next_order == 0
    }
    pub fn apply(self, mut state: ConclusionState, mut diagnostics: Diagnostics) -> SummaryResult {
        let mut candidates: Vec<_> = self.first.into_iter().chain(self.originating).collect();
        candidates.sort_by_key(|(order, _)| *order);
        let mut candidate_lost = false;
        let mut released = Vec::with_capacity(2);
        for (_, proposal) in candidates {
            let previous_source = state.source;
            let decision = state.apply(&proposal);
            if proposal.originating() {
                candidate_lost = decision == ConclusionDecision::Keep;
            }
            if decision == ConclusionDecision::Keep {
                if let Some(source) = proposal.source {
                    released.push(source);
                }
            } else if let Some(source) = previous_source {
                released.push(source);
            }
        }
        let mut ordered = self.other_diagnostics;
        if candidate_lost && let Some(candidate) = self.originating_diagnostic {
            ordered.push(candidate);
        }
        ordered.sort_by_key(|(order, _)| *order);
        for (_, diagnostic) in ordered {
            diagnostics.record(diagnostic);
        }
        // Count direct overflow. Never subtract an accepted candidate from a
        // saturated total; that would undercount at u64::MAX.
        diagnostics.dropped = diagnostics
            .dropped
            .saturating_add(self.omitted_other_diagnostics);
        SummaryResult {
            state,
            diagnostics,
            released,
        }
    }
}

/// Causal cuts are prepaid by admitted roles. A consumed cell remains occupied
/// through effect/capture exit and every declared completion consumer.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct CutId(pub u64);
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CutError {
    Full,
    IdentityOrder,
    IdentityExhausted,
    Unknown,
    NotReserved,
    NotReady,
    Consumer,
    ExitUnobserved,
    Consuming,
}
struct CutCell {
    segment: Option<ProposalSummary>,
    filled: bool,
    core_consumption: novarocks_control_runtime_core::completion::Consumption,
    captures_exited: bool,
    effects_exited: bool,
    consumers: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ConsumingCut {
    id: CutId,
    abandoning: bool,
}
pub struct CausalSegments {
    limit: usize,
    last_cut: u64,
    open: ProposalSummary,
    cells: std::collections::BTreeMap<CutId, CutCell>,
    filled: std::collections::VecDeque<CutId>,
    consuming_cut: Option<ConsumingCut>,
    // This slot owns the open summary until its real publication/disposal
    // captures exit. Core consumption alone permits later cuts, not slot reuse.
    consuming_open: Option<novarocks_control_runtime_core::completion::Consumption>,
    failed: bool,
}
impl CausalSegments {
    pub fn new(max_cuts: usize) -> Self {
        Self {
            limit: max_cuts,
            last_cut: 0,
            open: ProposalSummary::default(),
            cells: Default::default(),
            filled: Default::default(),
            consuming_cut: None,
            consuming_open: None,
            failed: false,
        }
    }
    pub fn push(&mut self, proposal: Proposal) -> Option<SourceId> {
        if self.failed {
            return proposal.source;
        }
        self.open.push(proposal)
    }
    pub fn cleanup(&mut self, diagnostic: Diagnostic) {
        if !self.failed {
            self.open.cleanup(diagnostic);
        }
    }
    pub fn reserve_cut(&mut self, id: CutId, consumers: u64) -> Result<(), CutError> {
        if self.failed {
            return Err(CutError::NotReady);
        }
        if id.0 <= self.last_cut {
            return Err(CutError::IdentityOrder);
        }
        if self.cells.len() == self.limit {
            return Err(CutError::Full);
        }
        self.last_cut = id.0;
        self.cells.insert(
            id,
            CutCell {
                segment: None,
                filled: false,
                core_consumption: novarocks_control_runtime_core::completion::Consumption::Pending,
                captures_exited: false,
                effects_exited: false,
                consumers,
            },
        );
        Ok(())
    }
    /// This is the input linearization point, not the earlier role reservation.
    pub fn seal_cut(&mut self, id: CutId) -> Result<(), CutError> {
        let cell = self.cells.get_mut(&id).ok_or(CutError::Unknown)?;
        if cell.filled {
            return Err(CutError::NotReserved);
        }
        cell.segment = Some(std::mem::take(&mut self.open));
        cell.filled = true;
        self.filled.push_back(id);
        Ok(())
    }
    /// Revoke a role that never registered or published a cut. The role's
    /// actual teardown is still observed before its prepaid cell is reusable.
    pub fn revoke_unused(&mut self, id: CutId) -> Result<(), CutError> {
        let cell = self.cells.get(&id).ok_or(CutError::Unknown)?;
        if cell.filled {
            return Err(CutError::NotReserved);
        }
        if !cell.captures_exited || !cell.effects_exited {
            return Err(CutError::ExitUnobserved);
        }
        self.cells.remove(&id);
        Ok(())
    }
    /// An open segment can progress without a lifecycle role registering a
    /// cut. Its sole custody slot remains occupied after core acknowledgement,
    /// until the original publication/disposal captures really exit.
    pub fn begin_open(&mut self) -> Result<ProposalSummary, CutError> {
        if self.failed {
            return Err(CutError::NotReady);
        }
        if self.consuming_open.is_some() || self.consuming_cut.is_some() {
            return Err(CutError::Consuming);
        }
        if !self.filled.is_empty() || self.open.is_empty() {
            return Err(CutError::NotReady);
        }
        self.consuming_open =
            Some(novarocks_control_runtime_core::completion::Consumption::Pending);
        Ok(std::mem::take(&mut self.open))
    }
    /// Records only the successful Core leg. It neither releases original
    /// source custody nor acknowledges publication/disposal capture exit.
    pub fn observe_open_consumed(&mut self) -> Result<(), CutError> {
        use novarocks_control_runtime_core::completion::Consumption;
        if self.failed || self.consuming_open != Some(Consumption::Pending) {
            return Err(CutError::NotReady);
        }
        self.consuming_open = Some(Consumption::Consumed);
        Ok(())
    }
    /// The original open publication/disposal owner reports actual completion.
    /// Legacy callers may acknowledge Core success and actual completion at
    /// once. Failure teardown never turns a Pending Core leg into Consumed.
    /// An overlapping cut has its own cell and is completely unaffected.
    pub fn finish_open(&mut self) -> Result<(), CutError> {
        use novarocks_control_runtime_core::completion::Consumption;
        let consumption = self.consuming_open.ok_or(CutError::NotReady)?;
        if consumption == Consumption::Pending && !self.failed {
            self.observe_open_consumed()?;
        }
        self.consuming_open = None;
        Ok(())
    }
    /// Move a queued cut to failed-owner custody. FIFO still applies, but no
    /// failed Core acknowledgement is required from an earlier open summary.
    /// The failed target is fenced against new Source/cut admission.
    pub fn begin_abandon(&mut self, id: CutId) -> Result<ProposalSummary, CutError> {
        let summary = self.take_cut(id, true)?;
        self.failed = true;
        Ok(summary)
    }
    /// The Core may fail after taking a cut. Only that cut's consumption leg
    /// changes; an overlapping open summary retains its original custody slot.
    pub fn abandon_consuming(&mut self, id: CutId) -> Result<(), CutError> {
        if self.consuming_cut
            != Some(ConsumingCut {
                id,
                abandoning: false,
            })
        {
            return Err(CutError::NotReady);
        }
        self.failed = true;
        self.consuming_cut = Some(ConsumingCut {
            id,
            abandoning: true,
        });
        self.finish_abandon(id)
    }
    /// Original failure teardown takes over the open custody outside this
    /// metadata owner. No positive Core acknowledgement is manufactured and
    /// no fresh Source/open summary may reuse the failed target afterwards.
    pub fn abandon_open(&mut self) -> Result<(), CutError> {
        if self.consuming_open.is_none() {
            return Err(CutError::NotReady);
        }
        self.failed = true;
        self.consuming_open = None;
        Ok(())
    }
    pub fn finish_abandon(&mut self, id: CutId) -> Result<(), CutError> {
        if self.consuming_cut
            != Some(ConsumingCut {
                id,
                abandoning: true,
            })
        {
            return Err(CutError::NotReady);
        }
        self.cells
            .get_mut(&id)
            .ok_or(CutError::Unknown)?
            .core_consumption =
            novarocks_control_runtime_core::completion::Consumption::AbandonedByOwnerFailure;
        self.consuming_cut = None;
        Ok(())
    }
    pub fn core_consumption(
        &self,
        id: CutId,
    ) -> Option<novarocks_control_runtime_core::completion::Consumption> {
        self.cells.get(&id).map(|c| c.core_consumption)
    }
    pub fn next_cut(&self) -> Option<CutId> {
        self.filled.front().copied()
    }
    pub fn begin_consume(&mut self, id: CutId) -> Result<ProposalSummary, CutError> {
        self.take_cut(id, false)
    }
    fn take_cut(&mut self, id: CutId, abandoning: bool) -> Result<ProposalSummary, CutError> {
        use novarocks_control_runtime_core::completion::Consumption;
        if self.consuming_cut.is_some() {
            return Err(CutError::Consuming);
        }
        if !abandoning {
            if self.failed {
                return Err(CutError::NotReady);
            }
            if self.consuming_open == Some(Consumption::Pending) {
                return Err(CutError::Consuming);
            }
        }
        if self.next_cut() != Some(id) {
            return Err(CutError::NotReady);
        }
        let cell = self.cells.get_mut(&id).ok_or(CutError::Unknown)?;
        let segment = cell.segment.take().ok_or(CutError::NotReady)?;
        self.filled.pop_front();
        self.consuming_cut = Some(ConsumingCut { id, abandoning });
        Ok(segment)
    }
    /// Records this cut's successful Core consumption. Its prepaid cell still
    /// retains independent effect, capture and consumer retirement obligations.
    /// The open summary's actual source custody is not released by this call.
    pub fn finish_consume(&mut self, id: CutId) -> Result<(), CutError> {
        if self.failed
            || self.consuming_cut
                != Some(ConsumingCut {
                    id,
                    abandoning: false,
                })
        {
            return Err(CutError::NotReady);
        }
        self.cells
            .get_mut(&id)
            .ok_or(CutError::Unknown)?
            .core_consumption = novarocks_control_runtime_core::completion::Consumption::Consumed;
        self.consuming_cut = None;
        Ok(())
    }
    pub fn observe_captures_exit(&mut self, id: CutId) -> Result<(), CutError> {
        self.cells
            .get_mut(&id)
            .ok_or(CutError::Unknown)?
            .captures_exited = true;
        Ok(())
    }
    pub fn observe_effects_exit(&mut self, id: CutId) -> Result<(), CutError> {
        self.cells
            .get_mut(&id)
            .ok_or(CutError::Unknown)?
            .effects_exited = true;
        Ok(())
    }
    pub fn consume(&mut self, id: CutId, consumer: u8) -> Result<(), CutError> {
        let bit = 1_u64
            .checked_shl(u32::from(consumer))
            .ok_or(CutError::Consumer)?;
        let cell = self.cells.get_mut(&id).ok_or(CutError::Unknown)?;
        if cell.consumers & bit == 0 {
            return Err(CutError::Consumer);
        }
        cell.consumers &= !bit;
        Ok(())
    }
    pub fn retire(&mut self, id: CutId) -> Result<(), CutError> {
        let cell = self.cells.get(&id).ok_or(CutError::Unknown)?;
        if cell.core_consumption == novarocks_control_runtime_core::completion::Consumption::Pending
            || !cell.effects_exited
            || !cell.captures_exited
            || cell.consumers != 0
        {
            return Err(CutError::ExitUnobserved);
        }
        self.cells.remove(&id);
        Ok(())
    }
    pub fn resident_cuts(&self) -> usize {
        self.cells.len()
    }
    pub fn resident_segments(&self) -> usize {
        // The open buffer and its one original custody slot contribute two.
        // Every filled or consuming cut belongs to a distinct resident cell.
        1 + usize::from(self.consuming_open.is_some())
            + usize::from(self.consuming_cut.is_some())
            + self.filled.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_execution_contract::failure::{FailureClass, FailureReason, domain};
    use novarocks_execution_contract::{AbortCause, CancelReason, TaskFailure};
    use std::num::NonZeroU16;

    fn diagnostic(n: u16, cleanup: bool) -> Diagnostic {
        Diagnostic {
            phase: TaskFailurePhase::Execution,
            fact: FailureFact::new(
                FailureClass::Data,
                FailureReason::new(domain::EXECUTION, NonZeroU16::new(n).unwrap()),
            ),
            detail: SafeDetail::truncating(&format!("failure {n}")),
            cleanup,
        }
    }
    fn proposal(kind: u8, ordinal: u16) -> Proposal {
        let diagnostic = diagnostic(ordinal, false);
        let (conclusion, evidence) = match kind {
            0 => (TaskConclusion::Succeeded, None),
            1 => (
                TaskConclusion::Terminated(TerminationDetail::Aborted(AbortCause::PeerTaskFailed)),
                None,
            ),
            2 => (
                TaskConclusion::Terminated(TerminationDetail::Canceled(
                    CancelReason::UpstreamNoLongerNeeded,
                )),
                None,
            ),
            3 => (
                TaskConclusion::Terminated(TerminationDetail::Failed(TaskFailure::new(
                    diagnostic.fact,
                    diagnostic.detail.clone(),
                ))),
                Some(FailureEvidence::Originating),
            ),
            _ => (
                TaskConclusion::Terminated(TerminationDetail::Failed(TaskFailure::new(
                    diagnostic.fact,
                    diagnostic.detail.clone(),
                ))),
                Some(FailureEvidence::StopObservation),
            ),
        };
        Proposal {
            conclusion,
            evidence,
            source: (kind >= 3).then_some(SourceId(u64::from(ordinal))),
            diagnostic: (kind == 3).then_some(diagnostic),
        }
    }
    fn reference(
        mut state: ConclusionState,
        diagnostics: Diagnostics,
        input: &[Proposal],
    ) -> (ConclusionState, Diagnostics) {
        let mut stream = diagnostics.entries;
        for (i, item) in input.iter().enumerate() {
            // Interpret every occurrence in its original order, independently
            // of the summary's candidate selection and overflow algorithm.
            let decision = if state.sealed {
                ConclusionDecision::Keep
            } else if let Some(evidence) = item.evidence {
                decide_reported_failure(state.accepted.as_ref(), &item.conclusion, evidence)
            } else {
                decide_conclusion(state.accepted.as_ref(), &item.conclusion)
            };
            if decision != ConclusionDecision::Keep {
                state.generation += 1;
                state.accepted = Some(item.conclusion.clone());
                state.source = item.source;
            } else if let Some(diagnostic) = &item.diagnostic {
                stream.push(diagnostic.clone());
            }
            if i % 2 == 0 {
                stream.push(diagnostic(100 + i as u16, true));
            }
        }
        let omitted = stream.len().saturating_sub(4) as u64;
        let kept = stream.into_iter().take(4).collect();
        (
            state,
            Diagnostics {
                entries: kept,
                dropped: diagnostics.dropped.saturating_add(omitted),
            },
        )
    }
    #[test]
    fn every_legal_entry_state_matches_uncompressed_occurrences_and_cleanup() {
        for length in 0..=5_u32 {
            for sequence in 0..5_u32.pow(length) {
                let mut code = sequence;
                let input: Vec<_> = (0..length)
                    .map(|i| {
                        let p = proposal((code % 5) as u8, (i + 1) as u16);
                        code /= 5;
                        p
                    })
                    .collect();
                for entry in 0..6 {
                    for sealed in [false, true] {
                        if entry == 0 && sealed {
                            continue;
                        }
                        let state = if entry == 0 {
                            ConclusionState::default()
                        } else {
                            let p = proposal(entry - 1, 90);
                            ConclusionState {
                                accepted: Some(p.conclusion),
                                source: p.source,
                                generation: 1,
                                sealed,
                            }
                        };
                        let mut summary = ProposalSummary::default();
                        for (i, p) in input.iter().enumerate() {
                            summary.push(p.clone());
                            if i % 2 == 0 {
                                summary.cleanup(diagnostic(100 + i as u16, true));
                            }
                        }
                        assert!(summary.candidate_count() <= 2);
                        assert!(summary.diagnostic_count() <= 5);
                        for stored in 0..=4 {
                            for dropped in [0, u64::MAX - 1, u64::MAX] {
                                let diagnostics = Diagnostics {
                                    entries: (0..stored)
                                        .map(|n| diagnostic(200 + n, true))
                                        .collect(),
                                    dropped,
                                };
                                let expected =
                                    reference(state.clone(), diagnostics.clone(), &input);
                                let actual = summary.clone().apply(state.clone(), diagnostics);
                                assert_eq!(
                                    (actual.state, actual.diagnostics),
                                    expected,
                                    "sequence {sequence}, length {length}, entry {entry}, sealed {sealed}, stored {stored}, dropped {dropped}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }
    #[test]
    fn accepting_candidate_never_subtracts_from_saturated_direct_overflow() {
        let mut summary = ProposalSummary::default();
        summary.push(proposal(3, 1));
        for i in 2..=5 {
            summary.push(proposal(3, i));
        }
        summary.omitted_other_diagnostics = u64::MAX;
        let result = summary.apply(ConclusionState::default(), Diagnostics::default());
        assert_eq!(result.diagnostics.entries.len(), 4);
        assert_eq!(result.diagnostics.dropped, u64::MAX);
        assert_eq!(result.state.source, Some(SourceId(1)));
    }
    #[test]
    fn only_retained_candidates_keep_source_custody_until_adjudication() {
        let mut summary = ProposalSummary::default();
        assert_eq!(summary.push(proposal(1, 1)), None);
        assert_eq!(summary.push(proposal(3, 2)), None);
        assert_eq!(summary.push(proposal(3, 3)), Some(SourceId(3)));
        assert_eq!(summary.push(proposal(4, 4)), Some(SourceId(4)));
        let result = summary.apply(ConclusionState::default(), Diagnostics::default());
        assert_eq!(result.state.source, Some(SourceId(2)));
        assert_eq!(result.diagnostics.entries.len(), 1);
    }
    #[test]
    fn cut_reuse_waits_for_consumption_effects_and_capture_exit() {
        let mut cuts = CausalSegments::new(1);
        cuts.reserve_cut(CutId(1), 3).unwrap();
        cuts.push(proposal(1, 1));
        cuts.seal_cut(CutId(1)).unwrap();
        assert_eq!(cuts.reserve_cut(CutId(2), 1), Err(CutError::Full));
        assert_eq!(cuts.resident_segments(), 2);
        let summary = cuts.begin_consume(CutId(1)).unwrap();
        cuts.push(proposal(3, 2));
        assert_eq!(cuts.resident_segments(), 2);
        assert!(
            summary
                .apply(ConclusionState::default(), Diagnostics::default())
                .state
                .accepted
                .is_some()
        );
        cuts.finish_consume(CutId(1)).unwrap();
        cuts.observe_effects_exit(CutId(1)).unwrap();
        cuts.consume(CutId(1), 0).unwrap();
        cuts.observe_captures_exit(CutId(1)).unwrap();
        assert_eq!(cuts.retire(CutId(1)), Err(CutError::ExitUnobserved));
        cuts.consume(CutId(1), 1).unwrap();
        cuts.retire(CutId(1)).unwrap();
        cuts.reserve_cut(CutId(2), 1).unwrap();
        assert_eq!(cuts.reserve_cut(CutId(1), 1), Err(CutError::IdentityOrder));
    }
    #[test]
    fn cut_order_follows_handoff_rather_than_role_reservation_order() {
        let mut cuts = CausalSegments::new(2);
        cuts.reserve_cut(CutId(1), 1).unwrap();
        cuts.reserve_cut(CutId(2), 1).unwrap();
        cuts.seal_cut(CutId(2)).unwrap();
        cuts.seal_cut(CutId(1)).unwrap();
        assert_eq!(cuts.next_cut(), Some(CutId(2)));
        assert!(matches!(
            cuts.begin_consume(CutId(1)),
            Err(CutError::NotReady)
        ));
        cuts.begin_consume(CutId(2)).unwrap();
        assert!(matches!(
            cuts.begin_consume(CutId(1)),
            Err(CutError::Consuming)
        ));
        assert!(cuts.resident_segments() <= cuts.resident_cuts() + 2);
    }
    #[test]
    fn open_proposals_progress_without_cut_credits_or_other_inputs() {
        let mut cuts = CausalSegments::new(0);
        cuts.push(proposal(3, 1));
        let summary = cuts.begin_open().unwrap();
        cuts.push(proposal(3, 2));
        assert_eq!(cuts.resident_segments(), 2);
        assert!(matches!(cuts.begin_open(), Err(CutError::Consuming)));
        assert!(
            summary
                .apply(ConclusionState::default(), Diagnostics::default())
                .state
                .accepted
                .is_some()
        );
        cuts.finish_open().unwrap();
        cuts.begin_open().unwrap();
        cuts.finish_open().unwrap();
        assert!(matches!(cuts.begin_open(), Err(CutError::NotReady)));
    }
    #[test]
    fn unused_cut_revocation_waits_for_original_role_teardown() {
        let mut cuts = CausalSegments::new(1);
        cuts.reserve_cut(CutId(1), 1).unwrap();
        assert_eq!(cuts.revoke_unused(CutId(1)), Err(CutError::ExitUnobserved));
        cuts.observe_captures_exit(CutId(1)).unwrap();
        cuts.observe_effects_exit(CutId(1)).unwrap();
        cuts.revoke_unused(CutId(1)).unwrap();
        cuts.reserve_cut(CutId(2), 1).unwrap();
    }
    #[test]
    fn failed_core_abandons_only_its_consumption_leg() {
        use novarocks_control_runtime_core::completion::Consumption;
        let mut cuts = CausalSegments::new(1);
        cuts.reserve_cut(CutId(1), 1).unwrap();
        cuts.push(proposal(3, 1));
        cuts.seal_cut(CutId(1)).unwrap();
        let abandoned = cuts.begin_abandon(CutId(1)).unwrap();
        drop(abandoned); // Neutral IDs only; driver still holds actual sources.
        assert_eq!(cuts.finish_consume(CutId(1)), Err(CutError::NotReady));
        cuts.finish_abandon(CutId(1)).unwrap();
        assert_eq!(
            cuts.core_consumption(CutId(1)),
            Some(Consumption::AbandonedByOwnerFailure)
        );
        assert_eq!(cuts.retire(CutId(1)), Err(CutError::ExitUnobserved));
        cuts.consume(CutId(1), 0).unwrap();
        cuts.observe_captures_exit(CutId(1)).unwrap();
        cuts.observe_effects_exit(CutId(1)).unwrap();
        cuts.retire(CutId(1)).unwrap();
    }
    #[test]
    fn repeated_failure_occurrence_is_a_diagnostic_for_every_handoff() {
        let mut summary = ProposalSummary::default();
        for custody in 1..=8 {
            let mut same = proposal(3, 1);
            same.source = Some(SourceId(custody));
            summary.push(same);
        }
        let result = summary.apply(ConclusionState::default(), Diagnostics::default());
        assert_eq!(result.state.source, Some(SourceId(1)));
        assert_eq!(result.diagnostics.entries.len(), 4);
        assert_eq!(result.diagnostics.dropped, 3);
        assert!(
            result
                .diagnostics
                .entries
                .iter()
                .all(|d| d == &diagnostic(1, false))
        );
    }
    fn assert_segment_bound(cuts: &CausalSegments) {
        assert!(cuts.resident_segments() <= cuts.resident_cuts() + 2);
    }
    #[test]
    fn open_core_ack_unblocks_a_prepaid_cut_without_reusing_original_source_custody() {
        use novarocks_control_runtime_core::completion::Consumption;
        let input = [
            proposal(3, 1),
            proposal(3, 2),
            proposal(2, 3),
            proposal(3, 4),
        ];
        let mut cuts = CausalSegments::new(1);
        cuts.reserve_cut(CutId(1), 1).unwrap();
        assert_eq!(cuts.push(input[0].clone()), None);
        cuts.cleanup(diagnostic(100, true));
        assert_eq!(cuts.push(input[1].clone()), Some(SourceId(2)));
        let open = cuts.begin_open().unwrap();
        assert_segment_bound(&cuts);
        cuts.push(input[2].clone());
        cuts.cleanup(diagnostic(102, true));
        cuts.push(input[3].clone());
        cuts.seal_cut(CutId(1)).unwrap();
        assert_eq!(cuts.resident_segments(), 3);
        assert_segment_bound(&cuts);
        assert!(matches!(
            cuts.begin_consume(CutId(1)),
            Err(CutError::Consuming)
        ));
        assert_eq!(cuts.core_consumption(CutId(1)), Some(Consumption::Pending));
        let open_result = open.apply(ConclusionState::default(), Diagnostics::default());
        cuts.observe_open_consumed().unwrap();
        assert_eq!(cuts.observe_open_consumed(), Err(CutError::NotReady));
        let closing = cuts.begin_consume(CutId(1)).unwrap();
        assert_eq!(
            cuts.resident_segments(),
            3,
            "open custody and current cut occupy different slots"
        );
        assert_segment_bound(&cuts);
        let result = closing.apply(open_result.state, open_result.diagnostics);
        assert_eq!(
            (result.state, result.diagnostics),
            reference(ConclusionState::default(), Diagnostics::default(), &input)
        );
        assert_eq!(result.released, vec![SourceId(4)]);
        cuts.finish_consume(CutId(1)).unwrap();
        assert_eq!(cuts.core_consumption(CutId(1)), Some(Consumption::Consumed));
        assert!(matches!(cuts.begin_open(), Err(CutError::Consuming)));
        assert_eq!(cuts.reserve_cut(CutId(2), 1), Err(CutError::Full));
        assert_eq!(cuts.retire(CutId(1)), Err(CutError::ExitUnobserved));
        cuts.observe_captures_exit(CutId(1)).unwrap();
        cuts.observe_effects_exit(CutId(1)).unwrap();
        assert_eq!(cuts.retire(CutId(1)), Err(CutError::ExitUnobserved));
        cuts.consume(CutId(1), 0).unwrap();
        cuts.retire(CutId(1)).unwrap();
        assert_eq!(
            cuts.resident_segments(),
            2,
            "the slow original source still occupies the open custody slot"
        );
        assert_segment_bound(&cuts);
        cuts.reserve_cut(CutId(2), 1).unwrap();
        cuts.push(proposal(3, 5));
        assert!(matches!(cuts.begin_open(), Err(CutError::Consuming)));
        cuts.finish_open().unwrap();
        cuts.begin_open().unwrap();
        cuts.finish_open().unwrap();
        assert_segment_bound(&cuts);
    }
    #[test]
    fn finishing_open_custody_never_clears_an_overlapping_cut_or_its_fifo_successor() {
        use novarocks_control_runtime_core::completion::Consumption;
        let mut cuts = CausalSegments::new(2);
        cuts.push(proposal(3, 1));
        cuts.begin_open().unwrap();
        cuts.reserve_cut(CutId(1), 1).unwrap();
        cuts.seal_cut(CutId(1)).unwrap();
        cuts.observe_open_consumed().unwrap();
        cuts.begin_consume(CutId(1)).unwrap();
        cuts.push(proposal(3, 2));
        cuts.reserve_cut(CutId(2), 1).unwrap();
        cuts.seal_cut(CutId(2)).unwrap();
        assert_eq!(cuts.resident_segments(), 4);
        assert_segment_bound(&cuts);
        cuts.finish_open().unwrap();
        assert_eq!(cuts.core_consumption(CutId(1)), Some(Consumption::Pending));
        assert_eq!(cuts.next_cut(), Some(CutId(2)));
        assert!(matches!(
            cuts.begin_consume(CutId(2)),
            Err(CutError::Consuming)
        ));
        cuts.finish_consume(CutId(1)).unwrap();
        cuts.begin_consume(CutId(2)).unwrap();
        cuts.finish_consume(CutId(2)).unwrap();
        assert_eq!(cuts.core_consumption(CutId(1)), Some(Consumption::Consumed));
        assert_eq!(cuts.core_consumption(CutId(2)), Some(Consumption::Consumed));
        assert_segment_bound(&cuts);
    }
    #[test]
    fn failed_cut_teardown_preserves_open_custody_and_never_acknowledges_a_failed_core() {
        use novarocks_control_runtime_core::completion::Consumption;
        let mut cuts = CausalSegments::new(2);
        cuts.push(proposal(3, 1));
        cuts.begin_open().unwrap();
        for id in [CutId(1), CutId(2)] {
            cuts.reserve_cut(id, 1).unwrap();
            cuts.seal_cut(id).unwrap();
        }
        assert!(matches!(
            cuts.begin_abandon(CutId(2)),
            Err(CutError::NotReady)
        ));
        cuts.begin_abandon(CutId(1)).unwrap();
        assert_eq!(cuts.consuming_open, Some(Consumption::Pending));
        assert_eq!(cuts.observe_open_consumed(), Err(CutError::NotReady));
        assert_eq!(cuts.finish_consume(CutId(1)), Err(CutError::NotReady));
        cuts.finish_abandon(CutId(1)).unwrap();
        assert_eq!(cuts.consuming_open, Some(Consumption::Pending));
        assert_eq!(
            cuts.core_consumption(CutId(1)),
            Some(Consumption::AbandonedByOwnerFailure)
        );
        assert!(matches!(
            cuts.begin_consume(CutId(2)),
            Err(CutError::NotReady)
        ));
        cuts.begin_abandon(CutId(2)).unwrap();
        cuts.finish_abandon(CutId(2)).unwrap();
        assert_eq!(
            cuts.core_consumption(CutId(2)),
            Some(Consumption::AbandonedByOwnerFailure)
        );
        assert_eq!(cuts.retire(CutId(1)), Err(CutError::ExitUnobserved));
        // Actual open teardown remains independent and cannot rewrite the
        // failed cut legs as positive consumption for the Core that failed.
        cuts.finish_open().unwrap();
        assert!(matches!(cuts.begin_open(), Err(CutError::NotReady)));
        assert_eq!(cuts.reserve_cut(CutId(3), 1), Err(CutError::NotReady));
        assert_eq!(cuts.push(proposal(3, 99)), Some(SourceId(99)));
        assert_segment_bound(&cuts);
    }
    #[test]
    fn abandoning_open_metadata_fences_reuse_without_finishing_the_current_cut() {
        use novarocks_control_runtime_core::completion::Consumption;
        let mut cuts = CausalSegments::new(1);
        cuts.push(proposal(3, 1));
        cuts.begin_open().unwrap();
        cuts.observe_open_consumed().unwrap();
        cuts.reserve_cut(CutId(1), 1).unwrap();
        cuts.seal_cut(CutId(1)).unwrap();
        cuts.begin_consume(CutId(1)).unwrap();
        cuts.abandon_open().unwrap();
        assert_eq!(cuts.core_consumption(CutId(1)), Some(Consumption::Pending));
        assert_eq!(cuts.finish_consume(CutId(1)), Err(CutError::NotReady));
        cuts.abandon_consuming(CutId(1)).unwrap();
        assert_eq!(
            cuts.core_consumption(CutId(1)),
            Some(Consumption::AbandonedByOwnerFailure)
        );
        assert_eq!(cuts.observe_open_consumed(), Err(CutError::NotReady));
        assert!(matches!(cuts.begin_open(), Err(CutError::NotReady)));
        assert_eq!(cuts.push(proposal(3, 7)), Some(SourceId(7)));
        assert_segment_bound(&cuts);
    }
}
