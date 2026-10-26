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

//! A Task's short handoff lock: compressed proposals and prepaid causal cuts.
//! Rich source disposal is always returned to a submitter or a host executor.

use novarocks_control_runtime_core::{
    completion::Consumption,
    identity::{Generation, WorkId},
};
use novarocks_execution::runtime::fragment::ExecutionFailure;
use novarocks_execution_contract::TaskConclusionProposal;
use novarocks_execution_contract::TaskIdentity;
use novarocks_worker_protocol::context::ContextInput;
use novarocks_worker_protocol::proposal_summary::{
    CausalSegments, CutError, CutId, Diagnostic, ProposalSummary, SourceId,
};
use std::{collections::BTreeMap, mem::ManuallyDrop, sync::Mutex};

struct State {
    segments: CausalSegments,
    sources: BTreeMap<SourceId, ManuallyDrop<ExecutionFailure>>,
    last_source: u64,
    last_cut: u64,
    events: BTreeMap<CutId, EventCell>,
    failed: bool,
}
struct EventCell {
    task: TaskIdentity,
    work: Option<WorkId>,
    phase: EventPhase,
    kind: CausalEventKind,
}
#[derive(Clone, Copy)]
pub(crate) enum CausalEventKind {
    Settlement,
    ApplyClose(Generation),
}
#[derive(Clone, Copy, Eq, PartialEq)]
enum EventPhase {
    Reserved,
    Registered,
    Taken,
    Unused,
}
pub(crate) struct CausalEventInput {
    pub prefix: SummaryInput,
    pub work: WorkId,
    pub event: ContextInput,
}

pub(crate) struct TaskProposalIngress {
    max_sources: usize,
    state: Mutex<State>,
}

/// An owned plain summary. Its candidate sources remain in the ingress's
/// original custody until publication or disposal takes each precise source.
pub(crate) struct SummaryInput {
    pub cut: Option<CutId>,
    pub summary: ProposalSummary,
}

pub(crate) struct ProposalHandoff {
    /// A transferred occurrence may retain only a diagnostic and return its
    /// rich source as a definite loser. It still needs a coalesced wake.
    pub transferred: bool,
    pub discarded: Option<ExecutionFailure>,
}

impl TaskProposalIngress {
    pub(crate) fn new(max_cuts: usize) -> Option<Self> {
        let max_sources = max_cuts.checked_add(2)?.checked_mul(2)?;
        Some(Self {
            max_sources,
            state: Mutex::new(State {
                segments: CausalSegments::new(max_cuts),
                sources: BTreeMap::new(),
                last_source: 0,
                last_cut: 0,
                events: BTreeMap::new(),
                failed: false,
            }),
        })
    }

    /// A returned loser must be dropped by the submitter after returning from
    /// this API, with no cut/ledger lock. A failed target transfers no input,
    /// even for a control/success proposal which has no source to return.
    pub(crate) fn submit(
        &self,
        proposal: TaskConclusionProposal<ExecutionFailure>,
    ) -> ProposalHandoff {
        let (mut proposal, source) =
            crate::task_conclusion::PreparedTaskConclusion::new(proposal).into_protocol();
        // Unwind cannot destroy an opaque graph while this short lock is held.
        let mut source = source.map(ManuallyDrop::new);
        let mut state = self.state.lock().expect("task proposal handoff lock");
        if state.failed {
            drop(state);
            return ProposalHandoff {
                transferred: false,
                discarded: source.map(ManuallyDrop::into_inner),
            };
        }
        let id = if source.is_some() {
            state.last_source = state
                .last_source
                .checked_add(1)
                .expect("task proposal source identity exhausted");
            let id = SourceId(state.last_source);
            proposal.source = Some(id);
            Some(id)
        } else {
            None
        };
        let discarded = state.segments.push(proposal);
        if let Some(discarded) = discarded {
            assert_eq!(
                Some(discarded),
                id,
                "summary discards only this new occurrence"
            );
        } else if let Some(id) = id {
            assert!(
                state.sources.len() < self.max_sources,
                "prepaid proposal capture geometry"
            );
            assert!(
                state
                    .sources
                    .insert(id, source.take().expect("candidate source"))
                    .is_none()
            );
        }
        drop(state);
        ProposalHandoff {
            transferred: true,
            discarded: source.map(ManuallyDrop::into_inner),
        }
    }

    pub(crate) fn cleanup(&self, diagnostic: Diagnostic) -> bool {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        if state.failed {
            return false;
        }
        state.segments.cleanup(diagnostic);
        true
    }

    /// Called before the role acquires captures, not at registration time.
    pub(crate) fn reserve_cut(&self, cut: CutId, consumers: u64) -> Result<(), CutError> {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        if state.failed {
            return Err(CutError::NotReady);
        }
        state.segments.reserve_cut(cut, consumers)?;
        state.last_cut = cut.0;
        Ok(())
    }
    pub(crate) fn reserve_event_cut(
        &self,
        task: TaskIdentity,
        kind: CausalEventKind,
        consumers: u64,
    ) -> Result<CutId, CutError> {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        if state.failed {
            return Err(CutError::NotReady);
        }
        let cut = CutId(
            state
                .last_cut
                .checked_add(1)
                .ok_or(CutError::IdentityExhausted)?,
        );
        state.segments.reserve_cut(cut, consumers)?;
        state.last_cut = cut.0;
        assert!(
            state
                .events
                .insert(
                    cut,
                    EventCell {
                        task,
                        work: None,
                        phase: EventPhase::Reserved,
                        kind
                    }
                )
                .is_none()
        );
        Ok(cut)
    }
    pub(crate) fn bind_event_work(&self, cut: CutId, work: WorkId) -> Result<(), CutError> {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        let event = state.events.get_mut(&cut).ok_or(CutError::Unknown)?;
        if event.phase != EventPhase::Reserved || event.work.is_some() {
            return Err(CutError::NotReserved);
        }
        event.work = Some(work);
        Ok(())
    }
    /// Existing admitted cleanup authority can register after failure. It
    /// still occupies its original cut for supervisor takeover; no fresh
    /// source, normal input position or work is admitted here.
    pub(crate) fn register_causal_event(&self, cut: CutId) -> Result<(), CutError> {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        let event = state.events.get(&cut).ok_or(CutError::Unknown)?;
        if event.phase != EventPhase::Reserved || event.work.is_none() {
            return Err(CutError::NotReserved);
        }
        state.segments.seal_cut(cut)?;
        state
            .events
            .get_mut(&cut)
            .expect("original event cell")
            .phase = EventPhase::Registered;
        Ok(())
    }
    pub(crate) fn revoke_event(&self, cut: CutId) -> Result<(), CutError> {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        let event = state.events.get_mut(&cut).ok_or(CutError::Unknown)?;
        if event.phase != EventPhase::Reserved {
            return Err(CutError::NotReserved);
        }
        event.phase = EventPhase::Unused;
        Ok(())
    }
    pub(crate) fn event_unused(&self, cut: CutId) -> Result<bool, CutError> {
        let state = self.state.lock().expect("task proposal handoff lock");
        Ok(state.events.get(&cut).ok_or(CutError::Unknown)?.phase == EventPhase::Unused)
    }
    pub(crate) fn take_next_event(&self, abandon: bool) -> Result<CausalEventInput, CutError> {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        if state.failed != abandon {
            return Err(CutError::NotReady);
        }
        let cut = state.segments.next_cut().ok_or(CutError::NotReady)?;
        let event = state.events.get(&cut).ok_or(CutError::NotReady)?;
        if event.phase != EventPhase::Registered {
            return Err(CutError::NotReady);
        }
        let (task, work, kind) = (
            event.task,
            event.work.expect("bound admitted event"),
            event.kind,
        );
        let summary = if abandon {
            state.segments.begin_abandon(cut)?
        } else {
            state.segments.begin_consume(cut)?
        };
        state
            .events
            .get_mut(&cut)
            .expect("original event cell")
            .phase = EventPhase::Taken;
        if abandon {
            state.segments.finish_abandon(cut)?;
        }
        let event = match kind {
            CausalEventKind::Settlement => ContextInput::Settle { task, work },
            CausalEventKind::ApplyClose(generation) => {
                ContextInput::ApplyTaskClose { task, generation }
            }
        };
        Ok(CausalEventInput {
            prefix: SummaryInput {
                cut: Some(cut),
                summary,
            },
            work,
            event,
        })
    }
    pub(crate) fn cut_consumption(&self, cut: CutId) -> Option<Consumption> {
        self.state
            .lock()
            .expect("task proposal handoff lock")
            .segments
            .core_consumption(cut)
    }
    pub(crate) fn abandon_consuming_event(&self, cut: CutId) -> Result<(), CutError> {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        if !state.failed {
            return Err(CutError::NotReady);
        }
        state.segments.abandon_consuming(cut)
    }

    /// The actual input handoff linearizes here with proposals, independently
    /// of when this role reserved its finite position.
    pub(crate) fn seal_cut(&self, cut: CutId) -> Result<(), CutError> {
        self.state
            .lock()
            .expect("task proposal handoff lock")
            .segments
            .seal_cut(cut)
    }

    pub(crate) fn take_next_summary(&self) -> Result<SummaryInput, CutError> {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        if state.failed {
            return Err(CutError::NotReady);
        }
        if let Some(cut) = state.segments.next_cut() {
            if state.events.contains_key(&cut) {
                return Err(CutError::NotReady);
            }
            let summary = state.segments.begin_consume(cut)?;
            Ok(SummaryInput {
                cut: Some(cut),
                summary,
            })
        } else {
            let summary = state.segments.begin_open()?;
            Ok(SummaryInput { cut: None, summary })
        }
    }

    /// The open summary's core leg completed. Its actual publication/source
    /// disposal still owns the open consuming slot; a paid later cut may now
    /// advance without waiting for that unrelated foreign destructor.
    pub(crate) fn open_summary_consumed(&self) -> Result<(), CutError> {
        self.state
            .lock()
            .expect("task proposal handoff lock")
            .segments
            .observe_open_consumed()
    }

    /// A cut has its own prepaid storage after core consumption. The open
    /// consuming segment has no such cell, so its caller delays this release
    /// until its publication/disposal captures have really exited.
    pub(crate) fn summary_consumed(&self, cut: Option<CutId>) -> Result<(), CutError> {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        match cut {
            Some(cut) => state.segments.finish_consume(cut),
            None => state.segments.finish_open(),
        }
    }

    /// A host publication/disposal executor takes exactly one real source.
    /// Pure core decisions refer only to its ID and never run its destructor.
    pub(crate) fn take_source(&self, id: SourceId) -> Option<ExecutionFailure> {
        self.state
            .lock()
            .expect("task proposal handoff lock")
            .sources
            .remove(&id)
            .map(ManuallyDrop::into_inner)
    }

    pub(crate) fn observe_cut_exit(
        &self,
        cut: CutId,
        captures: bool,
        effects: bool,
    ) -> Result<(), CutError> {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        if captures {
            state.segments.observe_captures_exit(cut)?;
        }
        if effects {
            state.segments.observe_effects_exit(cut)?;
        }
        Ok(())
    }

    pub(crate) fn consume_cut(&self, cut: CutId, consumer: u8) -> Result<(), CutError> {
        self.state
            .lock()
            .expect("task proposal handoff lock")
            .segments
            .consume(cut, consumer)
    }

    pub(crate) fn retire_cut(&self, cut: CutId) -> Result<(), CutError> {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        state.segments.retire(cut)?;
        state.events.remove(&cut);
        Ok(())
    }

    pub(crate) fn revoke_unused_cut(&self, cut: CutId) -> Result<(), CutError> {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        state.segments.revoke_unused(cut)?;
        state.events.remove(&cut);
        Ok(())
    }

    /// Failure seals handoff, but does not adjudicate or dispose any candidate.
    /// The scope's real resource inventory drives bounded teardown afterwards.
    pub(crate) fn fail(&self) {
        self.state
            .lock()
            .expect("task proposal handoff lock")
            .failed = true;
    }

    pub(crate) fn take_failed_source(&self) -> Option<ExecutionFailure> {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        assert!(
            state.failed,
            "only failed-owner teardown abandons pending sources"
        );
        state
            .sources
            .pop_first()
            .map(|(_, source)| ManuallyDrop::into_inner(source))
    }

    pub(crate) fn retained_sources(&self) -> usize {
        self.state
            .lock()
            .expect("task proposal handoff lock")
            .sources
            .len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_execution_contract::{
        AbortCause, TaskConclusion, TaskFailurePhase, TerminationDetail,
        failure::{FailureClass, FailureFact, FailureProjection, FailureReason, domain},
    };
    use novarocks_worker_protocol::proposal_summary::{ConclusionState, Diagnostics};
    use std::{
        fmt,
        num::NonZeroU16,
        sync::{
            Arc, Weak,
            atomic::{AtomicUsize, Ordering},
        },
    };

    #[derive(Debug)]
    struct Source {
        ingress: Weak<TaskProposalIngress>,
        drops: Arc<AtomicUsize>,
    }
    impl fmt::Debug for TaskProposalIngress {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("TaskProposalIngress")
                .finish_non_exhaustive()
        }
    }
    impl fmt::Display for Source {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            // Diagnostic rendering, like destruction, is a foreign call.
            if let Some(ingress) = self.ingress.upgrade() {
                let _ = ingress.retained_sources();
            }
            f.write_str("proposal custody probe")
        }
    }
    impl std::error::Error for Source {}
    impl FailureProjection for Source {
        fn failure_fact(&self) -> FailureFact {
            FailureFact::new(
                FailureClass::Data,
                FailureReason::new(domain::WORKER, NonZeroU16::new(65000).unwrap()),
            )
        }
    }
    impl Drop for Source {
        fn drop(&mut self) {
            if let Some(ingress) = self.ingress.upgrade() {
                let _ = ingress.retained_sources();
            }
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn failure(
        ingress: &Arc<TaskProposalIngress>,
        drops: &Arc<AtomicUsize>,
    ) -> TaskConclusionProposal<ExecutionFailure> {
        TaskConclusionProposal::Failed {
            cause: ExecutionFailure::from_domain(Source {
                ingress: Arc::downgrade(ingress),
                drops: drops.clone(),
            }),
            phase: TaskFailurePhase::Execution,
        }
    }

    #[test]
    fn repeated_proposals_keep_two_candidates_and_dispose_losers_outside_the_cut_lock() {
        let ingress = Arc::new(TaskProposalIngress::new(1).unwrap());
        let drops = Arc::new(AtomicUsize::new(0));
        assert!(
            ingress
                .submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed))
                .discarded
                .is_none()
        );
        assert!(
            ingress
                .submit(failure(&ingress, &drops))
                .discarded
                .is_none()
        );
        for _ in 0..20 {
            drop(ingress.submit(failure(&ingress, &drops)));
        }
        assert_eq!(drops.load(Ordering::SeqCst), 20);
        assert_eq!(ingress.retained_sources(), 1);
        let input = ingress.take_next_summary().unwrap();
        assert!(input.cut.is_none());
        let result = input
            .summary
            .apply(ConclusionState::default(), Diagnostics::default());
        assert!(matches!(
            result.state.accepted,
            Some(TaskConclusion::Terminated(TerminationDetail::Failed(_)))
        ));
        assert_eq!(result.state.generation, 2);
        assert_eq!(result.diagnostics.entries.len(), 4);
        assert_eq!(result.diagnostics.dropped, 16);
        drop(ingress.take_source(result.state.source.unwrap()));
        ingress.summary_consumed(None).unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 21);
        assert_eq!(ingress.retained_sources(), 0);
    }

    #[test]
    fn registration_order_seals_the_prefix_and_cut_storage_waits_for_real_exit() {
        let ingress = Arc::new(TaskProposalIngress::new(2).unwrap());
        let drops = Arc::new(AtomicUsize::new(0));
        ingress.reserve_cut(CutId(1), 1).unwrap();
        ingress.reserve_cut(CutId(2), 1).unwrap();
        assert!(
            ingress
                .submit(failure(&ingress, &drops))
                .discarded
                .is_none()
        );
        ingress.seal_cut(CutId(2)).unwrap();
        let first = ingress.take_next_summary().unwrap();
        assert_eq!(first.cut, Some(CutId(2)));
        // The role reserved first has not registered; its prefix is still open.
        assert!(
            ingress
                .submit(TaskConclusionProposal::Succeeded)
                .discarded
                .is_none()
        );
        ingress.seal_cut(CutId(1)).unwrap();
        assert!(matches!(
            ingress.take_next_summary(),
            Err(CutError::Consuming)
        ));
        let first_result = first
            .summary
            .apply(ConclusionState::default(), Diagnostics::default());
        ingress.summary_consumed(first.cut).unwrap();
        assert_eq!(ingress.retire_cut(CutId(2)), Err(CutError::ExitUnobserved));
        ingress.consume_cut(CutId(2), 0).unwrap();
        ingress.observe_cut_exit(CutId(2), false, true).unwrap();
        assert_eq!(ingress.retire_cut(CutId(2)), Err(CutError::ExitUnobserved));
        drop(ingress.take_source(first_result.state.source.unwrap()));
        ingress.observe_cut_exit(CutId(2), true, false).unwrap();
        ingress.retire_cut(CutId(2)).unwrap();
        let second = ingress.take_next_summary().unwrap();
        assert_eq!(second.cut, Some(CutId(1)));
        let second_result = second
            .summary
            .apply(first_result.state, first_result.diagnostics);
        assert_eq!(
            second_result.state.generation, 1,
            "later success cannot replace failure"
        );
        ingress.summary_consumed(second.cut).unwrap();
        ingress.consume_cut(CutId(1), 0).unwrap();
        ingress.observe_cut_exit(CutId(1), true, true).unwrap();
        ingress.retire_cut(CutId(1)).unwrap();
    }

    #[test]
    fn failed_owner_teardown_takes_actual_sources_without_adjudicating_them() {
        let ingress = Arc::new(TaskProposalIngress::new(1).unwrap());
        let drops = Arc::new(AtomicUsize::new(0));
        assert!(
            ingress
                .submit(failure(&ingress, &drops))
                .discarded
                .is_none()
        );
        ingress.fail();
        assert!(
            matches!(ingress.take_next_summary(), Err(CutError::NotReady)),
            "a failed target cannot resume normal summary adjudication"
        );
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(ingress.submit(failure(&ingress, &drops)));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        drop(ingress.take_failed_source());
        assert_eq!(drops.load(Ordering::SeqCst), 2);
        assert!(ingress.take_failed_source().is_none());
    }

    #[test]
    fn unused_cut_stays_owned_through_role_teardown() {
        let ingress = TaskProposalIngress::new(1).unwrap();
        ingress.reserve_cut(CutId(1), 0).unwrap();
        assert_eq!(
            ingress.revoke_unused_cut(CutId(1)),
            Err(CutError::ExitUnobserved)
        );
        assert_eq!(ingress.reserve_cut(CutId(2), 0), Err(CutError::Full));
        ingress.observe_cut_exit(CutId(1), true, true).unwrap();
        ingress.revoke_unused_cut(CutId(1)).unwrap();
        ingress.reserve_cut(CutId(2), 0).unwrap();
    }

    #[test]
    fn cleanup_handoffs_are_bounded_diagnostics_and_never_accept_a_conclusion() {
        let ingress = TaskProposalIngress::new(1).unwrap();
        let fact = FailureFact::new(
            FailureClass::Data,
            FailureReason::new(domain::WORKER, NonZeroU16::new(65001).unwrap()),
        );
        for _ in 0..7 {
            ingress.cleanup(Diagnostic {
                phase: TaskFailurePhase::Execution,
                fact,
                detail: novarocks_execution_contract::SafeDetail::truncating("cleanup probe"),
                cleanup: true,
            });
        }
        let input = ingress.take_next_summary().unwrap();
        let result = input
            .summary
            .apply(ConclusionState::default(), Diagnostics::default());
        assert!(result.state.accepted.is_none());
        assert_eq!(result.state.generation, 0);
        assert_eq!(result.diagnostics.entries.len(), 4);
        assert_eq!(result.diagnostics.dropped, 3);
        assert!(result.diagnostics.entries.iter().all(|d| d.cleanup));
        assert_eq!(ingress.retained_sources(), 0);
        ingress.summary_consumed(None).unwrap();
    }

    #[test]
    fn an_open_publication_hold_does_not_block_a_later_paid_cut_after_core_ack() {
        let ingress = Arc::new(TaskProposalIngress::new(1).unwrap());
        let drops = Arc::new(AtomicUsize::new(0));
        ingress.reserve_cut(CutId(1), 1).unwrap();
        drop(ingress.submit(failure(&ingress, &drops)));
        let open = ingress.take_next_summary().unwrap();
        assert!(open.cut.is_none());
        let result = open
            .summary
            .apply(ConclusionState::default(), Diagnostics::default());
        ingress.seal_cut(CutId(1)).unwrap();
        assert!(matches!(
            ingress.take_next_summary(),
            Err(CutError::Consuming)
        ));
        ingress.open_summary_consumed().unwrap();
        let later = ingress.take_next_summary().unwrap();
        assert_eq!(later.cut, Some(CutId(1)));
        ingress.summary_consumed(later.cut).unwrap();
        assert_eq!(
            drops.load(Ordering::SeqCst),
            0,
            "the old publication still owns its actual source"
        );
        drop(ingress.submit(TaskConclusionProposal::Succeeded));
        assert!(
            matches!(ingress.take_next_summary(), Err(CutError::Consuming)),
            "the old open slot cannot be reused yet"
        );
        assert_eq!(ingress.retire_cut(CutId(1)), Err(CutError::ExitUnobserved));
        drop(ingress.take_source(result.state.source.unwrap()));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        ingress.summary_consumed(None).unwrap();
        let open = ingress.take_next_summary().unwrap();
        assert!(open.cut.is_none());
        ingress.summary_consumed(None).unwrap();
        ingress.observe_cut_exit(CutId(1), true, true).unwrap();
        ingress.consume_cut(CutId(1), 0).unwrap();
        ingress.retire_cut(CutId(1)).unwrap();
    }
}
