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
enum ConsumingSegment {
    Open,
    Cut(CutId, bool),
}
pub struct CausalSegments {
    limit: usize,
    last_cut: u64,
    open: ProposalSummary,
    cells: std::collections::BTreeMap<CutId, CutCell>,
    filled: std::collections::VecDeque<CutId>,
    consuming: Option<ConsumingSegment>,
}
impl CausalSegments {
    pub fn new(max_cuts: usize) -> Self {
        Self {
            limit: max_cuts,
            last_cut: 0,
            open: ProposalSummary::default(),
            cells: Default::default(),
            filled: Default::default(),
            consuming: None,
        }
    }
    pub fn push(&mut self, proposal: Proposal) -> Option<SourceId> {
        self.open.push(proposal)
    }
    pub fn cleanup(&mut self, diagnostic: Diagnostic) {
        self.open.cleanup(diagnostic);
    }
    pub fn reserve_cut(&mut self, id: CutId, consumers: u64) -> Result<(), CutError> {
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
    /// An open segment can progress without another lifecycle role registering
    /// a cut. Earlier filled cuts and the sole consuming segment keep order.
    pub fn begin_open(&mut self) -> Result<ProposalSummary, CutError> {
        if self.consuming.is_some() {
            return Err(CutError::Consuming);
        }
        if !self.filled.is_empty() || self.open.is_empty() {
            return Err(CutError::NotReady);
        }
        self.consuming = Some(ConsumingSegment::Open);
        Ok(std::mem::take(&mut self.open))
    }
    pub fn finish_open(&mut self) -> Result<(), CutError> {
        if self.consuming != Some(ConsumingSegment::Open) {
            return Err(CutError::NotReady);
        }
        self.consuming = None;
        Ok(())
    }
    /// Move source custody to failure teardown. This never adjudicates the
    /// abandoned summary or acknowledges effects, captures or other consumers.
    pub fn begin_abandon(&mut self, id: CutId) -> Result<ProposalSummary, CutError> {
        let summary = self.begin_consume(id)?;
        self.consuming = Some(ConsumingSegment::Cut(id, true));
        Ok(summary)
    }
    /// The core may fail after taking a segment. Its original custodian still
    /// owns every source until teardown; only this consumption leg changes.
    pub fn abandon_consuming(&mut self, id: CutId) -> Result<(), CutError> {
        if self.consuming != Some(ConsumingSegment::Cut(id, false)) {
            return Err(CutError::NotReady);
        }
        self.consuming = Some(ConsumingSegment::Cut(id, true));
        self.finish_abandon(id)
    }
    pub fn abandon_open(&mut self) -> Result<(), CutError> {
        self.finish_open()
    }
    pub fn finish_abandon(&mut self, id: CutId) -> Result<(), CutError> {
        if self.consuming != Some(ConsumingSegment::Cut(id, true)) {
            return Err(CutError::NotReady);
        }
        self.cells
            .get_mut(&id)
            .ok_or(CutError::Unknown)?
            .core_consumption =
            novarocks_control_runtime_core::completion::Consumption::AbandonedByOwnerFailure;
        self.consuming = None;
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
        if self.consuming.is_some() {
            return Err(CutError::Consuming);
        }
        if self.next_cut() != Some(id) {
            return Err(CutError::NotReady);
        }
        let cell = self.cells.get_mut(&id).ok_or(CutError::Unknown)?;
        let segment = cell.segment.take().ok_or(CutError::NotReady)?;
        self.filled.pop_front();
        self.consuming = Some(ConsumingSegment::Cut(id, false));
        Ok(segment)
    }
    /// The source custody owner calls this only when the consuming summary is
    /// fully handed off; at most one external consuming summary exists.
    pub fn finish_consume(&mut self, id: CutId) -> Result<(), CutError> {
        if self.consuming != Some(ConsumingSegment::Cut(id, false)) {
            return Err(CutError::NotReady);
        }
        self.cells
            .get_mut(&id)
            .ok_or(CutError::Unknown)?
            .core_consumption = novarocks_control_runtime_core::completion::Consumption::Consumed;
        self.consuming = None;
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
        1 + usize::from(self.consuming.is_some()) + self.filled.len()
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
}
