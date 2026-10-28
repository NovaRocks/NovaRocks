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

//! A Task's original paid source inventory and causal handoff fence.
//! Foreign projection, binding, destruction and ledger operations run outside it.

use crate::{
    control_runtime::{
        source_custody::SourceCustody,
        source_positions::{
            BoundSource, ReapedPosition, RetirementProbe, SourcePositionError, SourcePositionKey,
            SourcePositionObservation, SourcePositionPair, SourcePositionPhase, SourcePositions,
            UnusedPosition,
        },
    },
    task_completion_roles::AdmittedCompletionRole,
    task_port::TaskPendingNotice,
};
use novarocks_control_runtime_core::{
    completion::Consumption,
    identity::{Generation, WorkId},
};
use novarocks_execution::runtime::{execution_health::ExecutionHealth, fragment::ExecutionFailure};
use novarocks_execution_contract::{
    RetainedTaskSource, TaskConclusionProposal, TaskFailurePhase, TaskIdentity, TaskOperationId,
};
use novarocks_worker_protocol::{
    context::ContextInput,
    lease::MonotonicInstant,
    proposal_summary::{CausalSegments, CutError, CutId, Diagnostic, ProposalSummary, SourceId},
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

type RetainedSource = RetainedTaskSource<ExecutionFailure>;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IngressError {
    Geometry,
    WrongTask,
    Cut(CutError),
    Source(SourcePositionError),
}
impl From<CutError> for IngressError {
    fn from(error: CutError) -> Self {
        Self::Cut(error)
    }
}
fn storage_gate(error: SourcePositionError) -> IngressError {
    match error {
        SourcePositionError::NotAvailable | SourcePositionError::SourceStillHeld => {
            IngressError::Cut(CutError::ExitUnobserved)
        }
        error => IngressError::Source(error),
    }
}
impl From<SourcePositionError> for IngressError {
    fn from(error: SourcePositionError) -> Self {
        Self::Source(error)
    }
}

/// Original paid roles or a unique pair owner, never a source-credit count.
pub(crate) enum CutSourceAdmission {
    Fresh([AdmittedCompletionRole; 2]),
    Rearm {
        pair: SourcePositionPair,
        reaped: [ReapedPosition; 2],
        roles: [AdmittedCompletionRole; 2],
    },
    Unclaimed(SourcePositionPair),
}
/// Original funding handles survive rotation of a cut's prefix pair. Their
/// report/ledger operations belong to the host, outside this fence.
#[derive(Clone)]
pub(crate) struct SourceFunding {
    pub keys: [SourcePositionKey; 2],
    pub custody: [SourceCustody; 2],
}
impl SourceFunding {
    pub(crate) fn matches_pair(&self, pair: &SourcePositionPair) -> bool {
        self.keys
            .iter()
            .zip(pair.keys())
            .all(|(original, current)| original.same_original(current))
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PairRoute {
    Open,
    ConsumingOpen,
    Cut(CutId),
}
struct CutCell {
    pair: Option<SourcePositionPair>,
    funding: SourceFunding,
    event: Option<EventCell>,
    sealed: bool,
    unused: bool,
}
struct State {
    task: TaskIdentity,
    segments: CausalSegments,
    sources: SourcePositions,
    open: Option<SourcePositionPair>,
    spare: Option<SourcePositionPair>,
    open_consuming: bool,
    open_core_consumed: bool,
    last_source: u64,
    last_cut: u64,
    cuts: BTreeMap<CutId, CutCell>,
    failed: bool,
    source_closed: bool,
}
struct EventCell {
    task: TaskIdentity,
    work: Option<WorkId>,
    phase: EventPhase,
    kind: CausalEventKind,
    input: Option<ContextInput>,
}
/// The request token comes from the actual consumed runtime envelope. A
/// causal author retains only the original business operation and deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RequestMetadata {
    pub operation: TaskOperationId,
    pub deadline: MonotonicInstant,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CausalEventKind {
    Settlement,
    ApplyClose(Generation),
    Input,
    Request(RequestMetadata),
}
impl CausalEventKind {
    fn has_input(self) -> bool {
        matches!(self, Self::Input | Self::Request(_))
    }
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
    pub request: Option<RequestMetadata>,
    pub event: ContextInput,
}
pub(crate) struct TaskProposalIngress {
    state: Mutex<State>,
}
pub(crate) struct SummaryInput {
    pub cut: Option<CutId>,
    pub summary: ProposalSummary,
}
pub(crate) enum DiscardedProposal {
    Unadmitted(ExecutionFailure),
    Admitted(RetainedSource),
}
pub(crate) struct ProposalHandoff {
    pub transferred: bool,
    pub discarded: Option<DiscardedProposal>,
    /// The live custodian reports an inventory fault outside the cut fence.
    pub error: Option<IngressError>,
}
pub(crate) struct RetiredCut {
    pub pair: SourcePositionPair,
    pub funding: SourceFunding,
}

impl State {
    fn pair(&self, route: PairRoute) -> Result<&SourcePositionPair, IngressError> {
        match route {
            PairRoute::Open => self.open.as_ref(),
            PairRoute::ConsumingOpen => self.spare.as_ref(),
            PairRoute::Cut(cut) => self.cuts.get(&cut).and_then(|cell| cell.pair.as_ref()),
        }
        .ok_or(IngressError::Cut(CutError::Unknown))
    }
    fn observe_pair(
        &self,
        route: PairRoute,
    ) -> Result<[SourcePositionObservation; 2], IngressError> {
        self.observe_owned_pair(self.pair(route)?)
    }
    fn observe_owned_pair(
        &self,
        pair: &SourcePositionPair,
    ) -> Result<[SourcePositionObservation; 2], IngressError> {
        let keys = pair.keys();
        Ok([
            self.sources.inspect(keys[0])?,
            self.sources.inspect(keys[1])?,
        ])
    }
    fn funding(&self, pair: &SourcePositionPair) -> Result<SourceFunding, IngressError> {
        let [first, second] = pair.keys();
        Ok(SourceFunding {
            keys: [first.clone(), second.clone()],
            custody: [
                self.sources.inspect(first)?.custody,
                self.sources.inspect(second)?.custody,
            ],
        })
    }
    fn admit_pair(
        &mut self,
        admission: CutSourceAdmission,
    ) -> Result<SourcePositionPair, (IngressError, CutSourceAdmission)> {
        match admission {
            CutSourceAdmission::Fresh(roles) => self
                .sources
                .append_pair(roles)
                .map_err(|(error, roles)| (error.into(), CutSourceAdmission::Fresh(roles))),
            CutSourceAdmission::Unclaimed(pair) => {
                match self.sources.validate_unclaimed_pair(&pair) {
                    Ok(()) => Ok(pair),
                    Err(error) => Err((error.into(), CutSourceAdmission::Unclaimed(pair))),
                }
            }
            CutSourceAdmission::Rearm {
                pair,
                reaped,
                roles,
            } => self.sources.rearm_pair(pair, reaped, roles).map_err(
                |(error, pair, reaped, roles)| {
                    (
                        error.into(),
                        CutSourceAdmission::Rearm {
                            pair,
                            reaped,
                            roles,
                        },
                    )
                },
            ),
        }
    }
    fn reserve(
        &mut self,
        cut: CutId,
        consumers: u64,
        admission: CutSourceAdmission,
        event: Option<EventCell>,
    ) -> Result<(), (IngressError, CutSourceAdmission)> {
        let checked = if self.failed || self.source_closed {
            Err(CutError::NotReady)
        } else {
            self.segments.check_reserve_cut(cut)
        };
        if let Err(error) = checked {
            return Err((error.into(), admission));
        }
        let pair = self.admit_pair(admission)?;
        let funding = self
            .funding(&pair)
            .expect("the same newly admitted original pair");
        self.segments
            .reserve_cut(cut, consumers)
            .expect("same-lock pure reservation preflight");
        self.last_cut = cut.0;
        assert!(
            self.cuts
                .insert(
                    cut,
                    CutCell {
                        pair: Some(pair),
                        funding,
                        event,
                        sealed: false,
                        unused: false,
                    }
                )
                .is_none()
        );
        Ok(())
    }
    fn seal(&mut self, cut: CutId) -> Result<(), IngressError> {
        let cell = self.cuts.get(&cut).ok_or(CutError::Unknown)?;
        if cell.unused {
            return Err(CutError::NotReserved.into());
        }
        self.sources
            .validate_unclaimed_pair(cell.pair.as_ref().expect("original cut pair"))?;
        self.segments.seal_cut(cut)?;
        self.cuts.get_mut(&cut).expect("original cut").sealed = true;
        let cut_pair = self
            .cuts
            .get_mut(&cut)
            .expect("original cut")
            .pair
            .as_mut()
            .expect("original cut pair");
        self.open
            .as_mut()
            .expect("original open pair")
            .swap_with(cut_pair)
            .expect("same original bank");
        Ok(())
    }
    fn ready(&self, summary: &ProposalSummary) -> bool {
        summary
            .candidate_sources()
            .all(|id| self.sources.is_ready(id))
    }
    fn take_open_summary(&mut self) -> Result<SummaryInput, IngressError> {
        let summary = self.segments.peek_open()?;
        if !self.ready(summary) {
            return Err(CutError::NotReady.into());
        }
        self.sources
            .validate_unclaimed_pair(self.spare.as_ref().expect("original base spare"))
            .map_err(storage_gate)?;
        let summary = self.segments.begin_open()?;
        self.open
            .as_mut()
            .expect("original open pair")
            .swap_with(self.spare.as_mut().expect("original base spare"))
            .expect("same bank");
        self.open_consuming = true;
        self.open_core_consumed = false;
        Ok(SummaryInput { cut: None, summary })
    }
}
impl TaskProposalIngress {
    pub(crate) fn from_admitted(
        task: TaskIdentity,
        max_cuts: usize,
        roles: [AdmittedCompletionRole; 4],
        pending: Arc<dyn TaskPendingNotice>,
        health: Arc<ExecutionHealth>,
    ) -> Result<Self, (IngressError, [AdmittedCompletionRole; 4])> {
        let Some(limit) = max_cuts.checked_add(2).and_then(|n| n.checked_mul(2)) else {
            return Err((IngressError::Geometry, roles));
        };
        let mut sources = SourcePositions::new(task, limit, pending, health);
        let [a, b, c, d] = roles;
        let open = sources
            .append_pair([a, b])
            .unwrap_or_else(|_| unreachable!("four base roles fit the checked geometry"));
        let spare = sources
            .append_pair([c, d])
            .unwrap_or_else(|_| unreachable!("four base roles fit the checked geometry"));
        Ok(Self {
            state: Mutex::new(State {
                task,
                segments: CausalSegments::new(max_cuts),
                sources,
                open: Some(open),
                spare: Some(spare),
                open_consuming: false,
                open_core_consumed: false,
                last_source: 0,
                last_cut: 0,
                cuts: BTreeMap::new(),
                failed: false,
                source_closed: false,
            }),
        })
    }
    pub(crate) fn submit(
        &self,
        proposal: TaskConclusionProposal<ExecutionFailure>,
    ) -> ProposalHandoff {
        let phase = match &proposal {
            TaskConclusionProposal::Failed { phase, .. } => Some(*phase),
            _ => None,
        };
        let (mut proposal, mut original) =
            crate::task_conclusion::PreparedTaskConclusion::new(proposal).into_protocol();
        // Declared before the lock: an unwind destroys an unbound source only
        // after the cut guard has unlocked. A SourceClaim Drop is metadata-only.
        let claim = {
            let mut state = self.state.lock().expect("task proposal handoff lock");
            if state.failed || state.source_closed {
                drop(state);
                return ProposalHandoff {
                    transferred: false,
                    discarded: original.map(DiscardedProposal::Unadmitted),
                    error: None,
                };
            }
            if original.is_none() {
                state.segments.push(proposal);
                return ProposalHandoff {
                    transferred: true,
                    discarded: None,
                    error: None,
                };
            }
            let Some(next) = state.last_source.checked_add(1) else {
                drop(state);
                return ProposalHandoff {
                    transferred: false,
                    discarded: original.map(DiscardedProposal::Unadmitted),
                    error: Some(IngressError::Source(SourcePositionError::IdentityExhausted)),
                };
            };
            state.last_source = next;
            let id = SourceId(next);
            proposal.source = Some(id);
            if !state.segments.will_retain(&proposal) {
                assert_eq!(
                    state.segments.push(proposal),
                    Some(id),
                    "only this new definite loser is released"
                );
                drop(state);
                return ProposalHandoff {
                    transferred: true,
                    discarded: original.map(DiscardedProposal::Unadmitted),
                    error: None,
                };
            }
            let key = state
                .open
                .as_ref()
                .expect("original open pair")
                .keys()
                .into_iter()
                .find_map(|key| {
                    state
                        .sources
                        .inspect(key)
                        .ok()
                        .filter(|o| {
                            o.phase == SourcePositionPhase::Available
                                && o.custody.unused_source_ready()
                        })
                        .map(|o| o.key)
                });
            let Some(key) = key else {
                drop(state);
                return ProposalHandoff {
                    transferred: false,
                    discarded: original.map(DiscardedProposal::Unadmitted),
                    error: Some(IngressError::Source(SourcePositionError::NotAvailable)),
                };
            };
            let claim =
                match state
                    .sources
                    .claim(&key, id, phase.expect("the original failure phase"))
                {
                    Ok(claim) => claim,
                    Err(error) => {
                        drop(state);
                        return ProposalHandoff {
                            transferred: false,
                            discarded: original.map(DiscardedProposal::Unadmitted),
                            error: Some(error.into()),
                        };
                    }
                };
            assert_eq!(
                state.segments.push(proposal),
                None,
                "same-lock candidate prediction"
            );
            claim
        };
        let bound = claim.bind(original.take().expect("the same original source"));
        self.finish_binding(bound)
    }
    fn finish_binding(&self, bound: BoundSource) -> ProposalHandoff {
        let refusal = {
            let mut state = self.state.lock().expect("task proposal handoff lock");
            if state.failed {
                Some((None, bound))
            } else {
                state
                    .sources
                    .install(bound)
                    .err()
                    .map(|(error, bound)| (Some(error.into()), bound))
            }
        };
        match refusal {
            None => ProposalHandoff {
                transferred: true,
                discarded: None,
                error: None,
            },
            Some((error, bound)) => ProposalHandoff {
                transferred: true,
                discarded: Some(DiscardedProposal::Admitted(bound.into_retained())),
                error,
            },
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
    pub(crate) fn reserve_cut(
        &self,
        cut: CutId,
        consumers: u64,
        admission: CutSourceAdmission,
    ) -> Result<(), (IngressError, CutSourceAdmission)> {
        self.state
            .lock()
            .expect("task proposal handoff lock")
            .reserve(cut, consumers, admission, None)
    }
    pub(crate) fn reserve_event_cut(
        &self,
        task: TaskIdentity,
        kind: CausalEventKind,
        consumers: u64,
        admission: CutSourceAdmission,
    ) -> Result<CutId, (IngressError, CutSourceAdmission)> {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        if task != state.task {
            return Err((IngressError::WrongTask, admission));
        }
        let Some(next) = state.last_cut.checked_add(1) else {
            return Err((IngressError::Cut(CutError::IdentityExhausted), admission));
        };
        let cut = CutId(next);
        state.reserve(
            cut,
            consumers,
            admission,
            Some(EventCell {
                task,
                work: None,
                phase: EventPhase::Reserved,
                kind,
                input: None,
            }),
        )?;
        Ok(cut)
    }
    pub(crate) fn bind_event_work(&self, cut: CutId, work: WorkId) -> Result<(), IngressError> {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        let event = state
            .cuts
            .get_mut(&cut)
            .and_then(|cell| cell.event.as_mut())
            .ok_or(CutError::Unknown)?;
        if event.phase != EventPhase::Reserved || event.work.is_some() {
            return Err(CutError::NotReserved.into());
        }
        event.work = Some(work);
        Ok(())
    }
    pub(crate) fn register_causal_event(&self, cut: CutId) -> Result<(), IngressError> {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        let event = state
            .cuts
            .get(&cut)
            .and_then(|cell| cell.event.as_ref())
            .ok_or(CutError::Unknown)?;
        if event.phase != EventPhase::Reserved || event.work.is_none() || event.kind.has_input() {
            return Err(CutError::NotReserved.into());
        }
        state.seal(cut)?;
        state
            .cuts
            .get_mut(&cut)
            .unwrap()
            .event
            .as_mut()
            .unwrap()
            .phase = EventPhase::Registered;
        Ok(())
    }
    pub(crate) fn register_input_event(
        &self,
        cut: CutId,
        input: ContextInput,
    ) -> Result<(), (IngressError, ContextInput)> {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        let Some(event) = state.cuts.get(&cut).and_then(|cell| cell.event.as_ref()) else {
            return Err((CutError::Unknown.into(), input));
        };
        let input_task = match (event.kind, &input) {
            (
                CausalEventKind::Request(metadata),
                ContextInput::Cancel { request, task, .. }
                | ContextInput::UpdateTask { request, task, .. }
                | ContextInput::RootDrained { request, task }
                | ContextInput::GetFinalInfo { request, task }
                | ContextInput::GetDynamicFilters { request, task, .. },
            ) if *request == metadata.operation => Some(*task),
            (
                CausalEventKind::Input,
                ContextInput::Loaded { task, .. }
                | ContextInput::Running { task }
                | ContextInput::Flushing { task }
                | ContextInput::CompletionIntent { task, .. }
                | ContextInput::CompletionExited { task }
                | ContextInput::TerminalObservation { task, .. }
                | ContextInput::FinishWithOutput { task, .. }
                | ContextInput::Physical { task, .. }
                | ContextInput::Metrics { task, .. }
                | ContextInput::OperatorStatistics { task, .. }
                | ContextInput::DynamicFilterPublished { task, .. },
            ) => Some(*task),
            _ => None,
        };
        if event.phase != EventPhase::Reserved
            || event.work.is_none()
            || !event.kind.has_input()
            || event.input.is_some()
            || input_task != Some(event.task)
        {
            return Err((CutError::NotReserved.into(), input));
        }
        if let Err(error) = state.seal(cut) {
            return Err((error, input));
        }
        let event = state.cuts.get_mut(&cut).unwrap().event.as_mut().unwrap();
        event.input = Some(input);
        event.phase = EventPhase::Registered;
        Ok(())
    }
    pub(crate) fn revoke_event(&self, cut: CutId) -> Result<(), IngressError> {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        let event = state
            .cuts
            .get_mut(&cut)
            .and_then(|cell| cell.event.as_mut())
            .ok_or(CutError::Unknown)?;
        if event.phase != EventPhase::Reserved {
            return Err(CutError::NotReserved.into());
        }
        event.phase = EventPhase::Unused;
        state
            .cuts
            .get_mut(&cut)
            .expect("original revoked cut")
            .unused = true;
        Ok(())
    }
    /// The original non-event author relinquishes a reservation which never
    /// registered. Actual producer/effect exits are still independent facts.
    pub(crate) fn revoke_unregistered_cut(&self, cut: CutId) -> Result<(), IngressError> {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        let cell = state.cuts.get_mut(&cut).ok_or(CutError::Unknown)?;
        if cell.sealed || cell.unused || cell.event.is_some() {
            return Err(CutError::NotReserved.into());
        }
        cell.unused = true;
        Ok(())
    }
    pub(crate) fn event_unused(&self, cut: CutId) -> Result<bool, IngressError> {
        let state = self.state.lock().expect("task proposal handoff lock");
        Ok(state
            .cuts
            .get(&cut)
            .and_then(|cell| cell.event.as_ref())
            .ok_or(CutError::Unknown)?
            .phase
            == EventPhase::Unused)
    }
    /// Read only the original finite author's metadata before attaching its
    /// completion authority. No role, event, report or exit is issued here.
    pub(crate) fn validate_input_author(
        &self,
        cut: CutId,
        task: TaskIdentity,
        work: WorkId,
    ) -> Result<(), IngressError> {
        let state = self.state.lock().expect("task proposal handoff lock");
        let event = state
            .cuts
            .get(&cut)
            .and_then(|cell| cell.event.as_ref())
            .ok_or(CutError::Unknown)?;
        if event.task != task {
            return Err(IngressError::WrongTask);
        }
        if !event.kind.has_input()
            || event.phase != EventPhase::Reserved
            || event.work != Some(work)
            || event.input.is_some()
        {
            return Err(CutError::NotReserved.into());
        }
        Ok(())
    }
    /// The original author may return after its event was registered, consumed
    /// or revoked. Those phases do not replace its exact Task/Work binding.
    pub(crate) fn validate_bound_input_author(
        &self,
        cut: CutId,
        task: TaskIdentity,
        work: WorkId,
    ) -> Result<(), IngressError> {
        let state = self.state.lock().expect("task proposal handoff lock");
        let event = state
            .cuts
            .get(&cut)
            .and_then(|cell| cell.event.as_ref())
            .ok_or(CutError::Unknown)?;
        if event.task != task {
            return Err(IngressError::WrongTask);
        }
        if !event.kind.has_input() || event.work != Some(work) {
            return Err(CutError::NotReserved.into());
        }
        Ok(())
    }
    pub(crate) fn seal_cut(&self, cut: CutId) -> Result<(), IngressError> {
        self.state
            .lock()
            .expect("task proposal handoff lock")
            .seal(cut)
    }
    pub(crate) fn take_next_event(&self, abandon: bool) -> Result<CausalEventInput, IngressError> {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        if state.failed != abandon {
            return Err(CutError::NotReady.into());
        }
        let cut = state.segments.next_cut().ok_or(CutError::NotReady)?;
        let event = state
            .cuts
            .get(&cut)
            .and_then(|cell| cell.event.as_ref())
            .ok_or(CutError::NotReady)?;
        if event.phase != EventPhase::Registered
            || (event.kind.has_input() && event.input.is_none())
        {
            return Err(CutError::NotReady.into());
        }
        let (task, work, kind) = (
            event.task,
            event.work.expect("original admitted event"),
            event.kind,
        );
        let summary = if abandon {
            state.segments.begin_abandon(cut)?
        } else {
            if !state.ready(state.segments.peek_cut(cut)?) {
                return Err(CutError::NotReady.into());
            }
            state.segments.begin_consume(cut)?
        };
        if abandon {
            state.segments.finish_abandon(cut)?;
        }
        let event = state.cuts.get_mut(&cut).unwrap().event.as_mut().unwrap();
        event.phase = EventPhase::Taken;
        let input = match kind {
            CausalEventKind::Settlement => ContextInput::Settle { task, work },
            CausalEventKind::ApplyClose(generation) => {
                ContextInput::ApplyTaskClose { task, generation }
            }
            CausalEventKind::Input | CausalEventKind::Request(_) => {
                event.input.take().expect("registered input")
            }
        };
        Ok(CausalEventInput {
            prefix: SummaryInput {
                cut: Some(cut),
                summary,
            },
            work,
            request: match kind {
                CausalEventKind::Request(metadata) => Some(metadata),
                _ => None,
            },
            event: input,
        })
    }
    pub(crate) fn take_next_summary(&self) -> Result<SummaryInput, IngressError> {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        if state.failed {
            return Err(CutError::NotReady.into());
        }
        if let Some(cut) = state.segments.next_cut() {
            if state
                .cuts
                .get(&cut)
                .is_some_and(|cell| cell.event.is_some())
            {
                return Err(CutError::NotReady.into());
            }
            if !state.ready(state.segments.peek_cut(cut)?) {
                return Err(CutError::NotReady.into());
            }
            Ok(SummaryInput {
                cut: Some(cut),
                summary: state.segments.begin_consume(cut)?,
            })
        } else {
            state.take_open_summary()
        }
    }
    /// Only the open-summary owner uses this path. A preceding cut retains
    /// its own author, even when it has no event, and an empty open segment
    /// never occupies the reusable runtime input or swaps its source pair.
    pub(crate) fn try_take_open_summary(&self) -> Result<Option<SummaryInput>, IngressError> {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        if state.failed {
            return Err(CutError::NotReady.into());
        }
        if state.segments.next_cut().is_some() {
            return Ok(None);
        }
        match state.take_open_summary() {
            Ok(input) => Ok(Some(input)),
            Err(IngressError::Cut(CutError::NotReady | CutError::Consuming)) => Ok(None),
            Err(error) => Err(error),
        }
    }
    /// Core consumption alone releases the predecessor gate, never its pair.
    pub(crate) fn summary_consumed(&self, cut: Option<CutId>) -> Result<(), IngressError> {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        match cut {
            Some(cut) => state.segments.finish_consume(cut)?,
            None => {
                state.segments.observe_open_consumed()?;
                state.open_core_consumed = true;
            }
        }
        Ok(())
    }
    pub(crate) fn open_summary_consumed(&self) -> Result<(), IngressError> {
        self.summary_consumed(None)
    }
    pub(crate) fn cut_consumption(&self, cut: CutId) -> Option<Consumption> {
        self.state
            .lock()
            .expect("task proposal handoff lock")
            .segments
            .core_consumption(cut)
    }
    pub(crate) fn abandon_consuming_event(&self, cut: CutId) -> Result<(), IngressError> {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        if !state.failed {
            return Err(CutError::NotReady.into());
        }
        state.segments.abandon_consuming(cut)?;
        Ok(())
    }
    pub(crate) fn observe_cut_exit(
        &self,
        cut: CutId,
        captures: bool,
        effects: bool,
    ) -> Result<(), IngressError> {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        if captures {
            state.segments.observe_captures_exit(cut)?;
        }
        if effects {
            state.segments.observe_effects_exit(cut)?;
        }
        Ok(())
    }
    pub(crate) fn consume_cut(&self, cut: CutId, consumer: u8) -> Result<(), IngressError> {
        self.state
            .lock()
            .expect("task proposal handoff lock")
            .segments
            .consume(cut, consumer)
            .map_err(Into::into)
    }
    pub(crate) fn take_source(&self, id: SourceId) -> Result<RetainedSource, SourcePositionError> {
        self.state
            .lock()
            .expect("task proposal handoff lock")
            .sources
            .take_source(id)
    }
    pub(crate) fn source_inventory(
        &self,
        id: SourceId,
    ) -> Result<SourcePositionObservation, SourcePositionError> {
        self.state
            .lock()
            .expect("task proposal handoff lock")
            .sources
            .source_inventory(id)
    }
    pub(crate) fn pair_inventory(
        &self,
        route: PairRoute,
    ) -> Result<[SourcePositionObservation; 2], IngressError> {
        self.state
            .lock()
            .expect("task proposal handoff lock")
            .observe_pair(route)
    }
    /// A returned pair may be held by its original finite issuer rather than
    /// a routing cell. Its same original owners still require explicit exit.
    pub(crate) fn owned_pair_inventory(
        &self,
        pair: &SourcePositionPair,
    ) -> Result<[SourcePositionObservation; 2], IngressError> {
        self.state
            .lock()
            .expect("task proposal handoff lock")
            .observe_owned_pair(pair)
    }
    pub(crate) fn prepare_owned_pair_reap(
        &self,
        pair: &SourcePositionPair,
    ) -> Result<[RetirementProbe; 2], IngressError> {
        let state = self.state.lock().expect("task proposal handoff lock");
        let observations = state.observe_owned_pair(pair)?;
        Ok([
            state.sources.retirement_probe_for(&observations[0].key)?,
            state.sources.retirement_probe_for(&observations[1].key)?,
        ])
    }
    pub(crate) fn take_unused_owned_pair(
        &self,
        pair: &SourcePositionPair,
    ) -> Result<[Option<UnusedPosition>; 2], IngressError> {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        let observations = state.observe_owned_pair(pair)?;
        for observation in &observations {
            if observation.phase == SourcePositionPhase::Available
                && !observation.custody.unused_source_ready()
            {
                return Err(SourcePositionError::SourceUnresolved.into());
            }
        }
        let mut holders = [None, None];
        for (holder, observation) in holders.iter_mut().zip(observations) {
            if observation.phase == SourcePositionPhase::Available {
                *holder = Some(
                    state
                        .sources
                        .take_unused(&observation.key)
                        .expect("same-lock original unused holder"),
                );
            }
        }
        Ok(holders)
    }
    pub(crate) fn release_owned_pair(
        &self,
        pair: &SourcePositionPair,
        reaped: [ReapedPosition; 2],
    ) -> Result<(), (IngressError, [ReapedPosition; 2])> {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        if let Err(error) = state.sources.validate_reaped_pair(pair, &reaped) {
            return Err((error.into(), reaped));
        }
        for witness in reaped {
            state
                .sources
                .release_reaped(witness)
                .expect("same-lock validated original pair");
        }
        Ok(())
    }
    pub(crate) fn cut_funding(&self, cut: CutId) -> Result<SourceFunding, IngressError> {
        Ok(self
            .state
            .lock()
            .expect("task proposal handoff lock")
            .cuts
            .get(&cut)
            .ok_or(CutError::Unknown)?
            .funding
            .clone())
    }
    /// A consumed cut returns the previous open pair, while its own paid pair
    /// may now be the empty open or spare. Exchange the two original owners
    /// before disposing either, so a lone RPC does not wait for another RPC
    /// just to recover its never-claimed funding roles. Only these two fixed
    /// locations are inspected; another live cut keeps its own unique pair.
    ///
    /// Both sides must still own their actual unused permits. Claimed,
    /// abandoned or reaped positions cannot be substituted. This moves only
    /// neutral ownership: it grants no source exit, Core consumption, report
    /// acknowledgement, new role or refund. Refusal returns the same pair.
    pub(crate) fn restore_retired_funding_pair(
        &self,
        mut retired: RetiredCut,
    ) -> Result<RetiredCut, (IngressError, RetiredCut)> {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        let checked = (|| {
            let funding = &retired.funding;
            state.sources.validate_unclaimed_pair(&retired.pair)?;
            for (key, original) in funding.keys.iter().zip(&funding.custody) {
                let observed = state.sources.inspect(key)?;
                if !observed.custody.report().same_slot(original.report()) {
                    return Err(IngressError::Source(SourcePositionError::ForeignInventory));
                }
            }
            if funding.matches_pair(&retired.pair) {
                return Ok(None);
            }
            let route = if state.open.as_ref().is_some_and(|p| funding.matches_pair(p)) {
                PairRoute::Open
            } else if !state.open_consuming
                && state
                    .spare
                    .as_ref()
                    .is_some_and(|p| funding.matches_pair(p))
            {
                PairRoute::ConsumingOpen
            } else {
                return Err(IngressError::Cut(CutError::NotReady));
            };
            state.sources.validate_unclaimed_pair(state.pair(route)?)?;
            Ok(Some(route))
        })();
        let route = match checked {
            Ok(route) => route,
            Err(error) => return Err((error, retired)),
        };
        if let Some(route) = route {
            let current = match route {
                PairRoute::Open => state.open.as_mut(),
                PairRoute::ConsumingOpen => state.spare.as_mut(),
                PairRoute::Cut(_) => unreachable!("only two original fixed pair locations"),
            }
            .expect("the validated original pair");
            current
                .swap_with(&mut retired.pair)
                .expect("same-lock validated original bank");
        }
        Ok(retired)
    }
    pub(crate) fn prepare_pair_reap(
        &self,
        route: PairRoute,
    ) -> Result<[RetirementProbe; 2], IngressError> {
        let state = self.state.lock().expect("task proposal handoff lock");
        let observations = state.observe_pair(route)?;
        Ok([
            state.sources.retirement_probe_for(&observations[0].key)?,
            state.sources.retirement_probe_for(&observations[1].key)?,
        ])
    }
    /// Only a closed prefix/spare or failed owner can retire unused holders.
    /// Returning them neither consumes reports nor observes their true exit.
    pub(crate) fn take_unused_pair(
        &self,
        route: PairRoute,
    ) -> Result<[Option<UnusedPosition>; 2], IngressError> {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        let safe = match route {
            PairRoute::Open => state.failed || state.source_closed,
            PairRoute::ConsumingOpen => state.open_consuming || state.failed || state.source_closed,
            PairRoute::Cut(cut) => state
                .cuts
                .get(&cut)
                .is_some_and(|cell| cell.sealed || cell.unused),
        };
        if !safe {
            return Err(CutError::NotReady.into());
        }
        let observations = state.observe_pair(route)?;
        let mut holders = [None, None];
        for (holder, observation) in holders.iter_mut().zip(observations) {
            if observation.phase == SourcePositionPhase::Available {
                *holder = Some(
                    state
                        .sources
                        .take_unused(&observation.key)
                        .expect("same-lock original available permit"),
                );
            }
        }
        Ok(holders)
    }
    /// The original host closes producer admission after closing its actual
    /// execution borrowing gate. Existing lifecycle authors still own cuts.
    pub(crate) fn seal_source_admission(&self) {
        self.state
            .lock()
            .expect("task proposal handoff lock")
            .source_closed = true;
    }
    /// Closing the reusable input must not strand a still-buffered neutral
    /// summary. Failure does not make that original buffer disappear.
    pub(crate) fn open_summary_drained(&self) -> bool {
        let state = self.state.lock().expect("task proposal handoff lock");
        state.source_closed && !state.open_consuming && state.segments.open_is_empty()
    }
    /// An empty source pair transfers the same untouched paid permits back
    /// to the spare. This releases neither credit nor any original source role.
    pub(crate) fn finish_open_unclaimed(&self) -> Result<(), IngressError> {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        if !state.open_consuming {
            return Err(CutError::NotReady.into());
        }
        if !state.failed && !state.open_core_consumed {
            return Err(CutError::Consuming.into());
        }
        state
            .sources
            .validate_unclaimed_pair(state.spare.as_ref().expect("original consuming pair"))
            .map_err(storage_gate)?;
        if state.failed {
            state.segments.abandon_open()?;
        } else {
            state.segments.finish_open()?;
        }
        state.open_consuming = false;
        state.open_core_consumed = false;
        Ok(())
    }
    /// Return a never-claimed pair for an explicit same-permit transfer. This
    /// is not permission to release the original funding or report consumers.
    pub(crate) fn retire_cut_unclaimed(&self, cut: CutId) -> Result<RetiredCut, IngressError> {
        self.retire_unclaimed_inner(cut, false)
    }
    pub(crate) fn revoke_unused_cut_unclaimed(
        &self,
        cut: CutId,
    ) -> Result<RetiredCut, IngressError> {
        self.retire_unclaimed_inner(cut, true)
    }
    fn retire_unclaimed_inner(&self, cut: CutId, unused: bool) -> Result<RetiredCut, IngressError> {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        let cell = state.cuts.get(&cut).ok_or(CutError::Unknown)?;
        if unused && !cell.unused {
            return Err(CutError::NotReserved.into());
        }
        state
            .sources
            .validate_unclaimed_pair(cell.pair.as_ref().expect("original cut pair"))
            .map_err(storage_gate)?;
        if unused {
            state.segments.revoke_unused(cut)?;
        } else {
            state.segments.retire(cut)?;
        }
        let cell = state.cuts.remove(&cut).expect("original retired cut");
        Ok(RetiredCut {
            pair: cell.pair.expect("original paid pair"),
            funding: cell.funding,
        })
    }
    /// A reap witness is produced outside this fence. Core acknowledgement
    /// remains an independent earlier fact; storage exit cannot manufacture it.
    pub(crate) fn finish_open_after_reap(
        &self,
        reaped: [ReapedPosition; 2],
    ) -> Result<(), (IngressError, [ReapedPosition; 2])> {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        let checked = (|| {
            if !state.open_consuming {
                return Err(CutError::NotReady.into());
            }
            if !state.failed && !state.open_core_consumed {
                return Err(CutError::Consuming.into());
            }
            state.sources.validate_reaped_pair(
                state.spare.as_ref().expect("original consuming pair"),
                &reaped,
            )?;
            Ok(())
        })();
        if let Err(error) = checked {
            return Err((error, reaped));
        }
        if state.failed {
            state
                .segments
                .abandon_open()
                .expect("original failed open custody");
        } else {
            state
                .segments
                .finish_open()
                .expect("the actual Core leg was already observed");
        }
        for witness in reaped {
            state
                .sources
                .release_reaped(witness)
                .expect("same-lock validated original pair");
        }
        state.open_consuming = false;
        state.open_core_consumed = false;
        Ok(())
    }
    /// Reissue only the original base spare addresses, after actual retirement.
    /// Admission of these new paid roles occurred on the original host outside
    /// this fence, never by waiting here for another queue or new capacity.
    pub(crate) fn rearm_open_spare(
        &self,
        reaped: [ReapedPosition; 2],
        roles: [AdmittedCompletionRole; 2],
    ) -> Result<
        (),
        (
            IngressError,
            [ReapedPosition; 2],
            [AdmittedCompletionRole; 2],
        ),
    > {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        if state.failed || state.source_closed || state.open_consuming {
            return Err((CutError::NotReady.into(), reaped, roles));
        }
        let pair = state.spare.take().expect("original base spare");
        match state.sources.rearm_pair(pair, reaped, roles) {
            Ok(pair) => {
                state.spare = Some(pair);
                Ok(())
            }
            Err((error, pair, reaped, roles)) => {
                state.spare = Some(pair);
                Err((error.into(), reaped, roles))
            }
        }
    }
    pub(crate) fn retire_cut(
        &self,
        cut: CutId,
        reaped: [ReapedPosition; 2],
    ) -> Result<RetiredCut, (IngressError, [ReapedPosition; 2])> {
        self.retire_cut_inner(cut, reaped, false)
    }
    pub(crate) fn revoke_unused_cut(
        &self,
        cut: CutId,
        reaped: [ReapedPosition; 2],
    ) -> Result<RetiredCut, (IngressError, [ReapedPosition; 2])> {
        self.retire_cut_inner(cut, reaped, true)
    }
    fn retire_cut_inner(
        &self,
        cut: CutId,
        reaped: [ReapedPosition; 2],
        unused: bool,
    ) -> Result<RetiredCut, (IngressError, [ReapedPosition; 2])> {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        let checked = (|| {
            if unused && !state.cuts.get(&cut).ok_or(CutError::Unknown)?.unused {
                return Err(CutError::NotReserved.into());
            }
            let pair = state.pair(PairRoute::Cut(cut))?;
            state.sources.validate_reaped_pair(pair, &reaped)?;
            if unused {
                state.segments.revoke_unused(cut)?;
            } else {
                state.segments.retire(cut)?;
            }
            Ok(())
        })();
        if let Err(error) = checked {
            return Err((error, reaped));
        }
        for witness in reaped {
            state
                .sources
                .release_reaped(witness)
                .expect("same-lock validated original pair");
        }
        let cell = state.cuts.remove(&cut).expect("original retired cut");
        Ok(RetiredCut {
            pair: cell.pair.expect("original source pair"),
            funding: cell.funding,
        })
    }
    pub(crate) fn fail(&self) {
        self.state
            .lock()
            .expect("task proposal handoff lock")
            .failed = true;
    }
    /// Inspect exactly one original pair member; the host walks its finite
    /// original inventory and retains Pending duties for the late binder wake.
    pub(crate) fn take_failed_source(
        &self,
        route: PairRoute,
        ordinal: usize,
    ) -> Result<Option<RetainedSource>, IngressError> {
        let mut state = self.state.lock().expect("task proposal handoff lock");
        if !state.failed {
            return Err(CutError::NotReady.into());
        }
        let observation = state
            .observe_pair(route)?
            .into_iter()
            .nth(ordinal)
            .ok_or(SourcePositionError::UnknownPosition)?;
        match observation.phase {
            SourcePositionPhase::Ready | SourcePositionPhase::Abandoned => {
                match state.sources.take_for_disposal(&observation.key) {
                    Ok(source) => Ok(Some(source)),
                    Err(SourcePositionError::NotReady) => Ok(None),
                    Err(error) => Err(error.into()),
                }
            }
            _ => Ok(None),
        }
    }
    pub(crate) fn retained_sources(&self) -> usize {
        let state = self.state.lock().expect("task proposal handoff lock");
        state
            .open
            .iter()
            .chain(state.spare.iter())
            .chain(state.cuts.values().filter_map(|cell| cell.pair.as_ref()))
            .flat_map(|pair| pair.keys())
            .filter(|key| {
                state
                    .sources
                    .inspect(key)
                    .expect("current original pair")
                    .source
                    .is_some()
            })
            .count()
    }
}

#[cfg(test)]
mod tests {
    mod request_funding;
    use super::*;
    use novarocks_control_runtime::{
        completion::ReportSlot,
        lease::WorkLease,
        transaction::{RuntimeLedger, ScopeHandle},
    };
    use novarocks_control_runtime_core::{
        capacity::ResourceVector,
        completion::ConsumerKind,
        identity::{ConsumerId, DimensionId},
        scope::{WorkAdmission, WorkKind},
    };
    use novarocks_execution_contract::{
        AcceptedConclusion, AcceptedTaskConclusion,
        failure::{FailureClass, FailureFact, FailureProjection, FailureReason, domain},
    };
    use novarocks_worker_protocol::proposal_summary::{ConclusionState, Diagnostics};
    use std::{
        fmt,
        num::{NonZeroU16, NonZeroU64},
        sync::{
            Weak,
            atomic::{AtomicUsize, Ordering},
        },
    };

    #[derive(Default)]
    struct Pending(AtomicUsize);
    impl TaskPendingNotice for Pending {
        fn pending(&self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn task(n: u32) -> TaskIdentity {
        use novarocks_types::{
            AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
        };
        TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(7311, 7312), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(n).unwrap(),
            BackendProcessId::new_v7(),
        )
    }
    fn observer() -> ConsumerId {
        ConsumerId::new(2).unwrap()
    }
    /// These component roles declare only the real host observer. The joint
    /// Causal/Service tests must supply actual InputId proof for a Core owner;
    /// this fixture never manufactures a Core consumer receipt.
    struct Fixture {
        task: TaskIdentity,
        ledger: RuntimeLedger,
        scope: ScopeHandle,
        parent: WorkLease,
        capacity: novarocks_control_runtime::lease::CapacityBundle,
        pending: Arc<Pending>,
        health: Arc<ExecutionHealth>,
        ingress: Arc<TaskProposalIngress>,
    }
    impl Fixture {
        fn new(max_cuts: usize) -> Self {
            let task = task(1);
            let resources = ResourceVector::new([(DimensionId::new(1).unwrap(), 1)]).unwrap();
            let ledger = RuntimeLedger::new(resources.clone(), 128, 1);
            let generation = Generation::new(1).unwrap();
            let scope = ledger.open_scope_next(None, generation, 128, 128).unwrap();
            let (mut parent, capacity) = scope
                .reserve_next(
                    generation,
                    resources,
                    WorkKind::Business,
                    WorkAdmission::External,
                )
                .unwrap()
                .bind(WorkKind::Business)
                .unwrap();
            parent.start().unwrap();
            let pending = Arc::new(Pending::default());
            let health = Arc::new(ExecutionHealth::default());
            let roles = std::array::from_fn(|_| Self::make_role(&scope, capacity.id()));
            let ingress = Arc::new(
                TaskProposalIngress::from_admitted(
                    task,
                    max_cuts,
                    roles,
                    pending.clone(),
                    health.clone(),
                )
                .unwrap_or_else(|_| panic!("checked original roles")),
            );
            Self {
                task,
                ledger,
                scope,
                parent,
                capacity,
                pending,
                health,
                ingress,
            }
        }
        fn make_role(
            scope: &ScopeHandle,
            parent: novarocks_control_runtime_core::identity::ReservationId,
        ) -> AdmittedCompletionRole {
            let generation = scope.gates().unwrap().generation;
            let guard = scope
                .reserve_child_next(
                    generation,
                    parent,
                    ResourceVector::default(),
                    WorkKind::Cleanup,
                )
                .unwrap();
            let report = ReportSlot::reserve_next(
                scope,
                generation,
                guard.work_id(),
                1,
                [(observer(), ConsumerKind::Observer)],
            )
            .unwrap();
            let (work, capacity) = guard.bind(WorkKind::Cleanup).unwrap();
            AdmittedCompletionRole {
                work,
                capacity,
                report,
            }
        }
        fn roles(&self) -> [AdmittedCompletionRole; 2] {
            std::array::from_fn(|_| Self::make_role(&self.scope, self.capacity.id()))
        }
        fn reserve(&self, cut: CutId) {
            self.ingress
                .reserve_cut(cut, 0, CutSourceAdmission::Fresh(self.roles()))
                .unwrap_or_else(|_| panic!("actual paid cut"));
        }
        fn dispose_roles<const N: usize>(&self, roles: [AdmittedCompletionRole; N]) {
            for role in roles {
                let (custody, permit) = SourceCustody::from_admitted(
                    self.task,
                    role,
                    self.pending.clone(),
                    self.health.clone(),
                );
                drop(permit);
                assert_eq!(
                    custody.report().consume(observer()),
                    Err(
                        novarocks_control_runtime::transaction::RuntimeError::Completion(
                            novarocks_control_runtime_core::completion::CompletionError::NotFilled
                        )
                    )
                );
                assert!(custody.try_retire().unwrap());
                assert_eq!(self.ledger.reap_scope_ready(self.scope.id(), 1).unwrap(), 1);
                assert!(custody.reusable());
            }
        }
        fn reap(&self, route: PairRoute) -> [ReapedPosition; 2] {
            for holder in self
                .ingress
                .take_unused_pair(route)
                .unwrap()
                .into_iter()
                .flatten()
            {
                holder.exit();
            }
            let observations = self.ingress.pair_inventory(route).unwrap();
            for observation in observations {
                assert!(observation.custody.actual_exit_ready().unwrap());
                if observation.source.is_some() {
                    observation.custody.report().consume(observer()).unwrap();
                }
                assert!(observation.custody.try_retire().unwrap());
            }
            self.ledger.reap_scope_ready(self.scope.id(), 2).unwrap();
            self.ingress
                .prepare_pair_reap(route)
                .unwrap()
                .map(|probe| probe.observe_reaped().unwrap())
        }
        fn finish(mut self) {
            self.ingress.seal_source_admission();
            assert!(self.ingress.state.lock().unwrap().cuts.is_empty());
            for route in [PairRoute::Open, PairRoute::ConsumingOpen] {
                let witnesses = self.reap(route);
                let mut state = self.ingress.state.lock().unwrap();
                for witness in witnesses {
                    state.sources.release_reaped(witness).unwrap();
                }
            }
            assert_eq!(self.ingress.retained_sources(), 0);
            self.parent.body_exited().unwrap();
            self.parent.captures_exited().unwrap();
            self.capacity
                .release(&self.capacity.resources().unwrap())
                .unwrap();
            self.ledger.reap_scope_ready(self.scope.id(), 1).unwrap();
            self.scope.close_external().unwrap();
            self.ledger.remove_scope(self.scope.id()).unwrap();
            assert!(self.ledger.is_empty());
            assert!(self.health.failure().is_none());
        }
    }
    struct Source {
        ingress: Weak<TaskProposalIngress>,
        drops: Arc<AtomicUsize>,
    }
    impl fmt::Debug for Source {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("Source")
        }
    }
    impl fmt::Display for Source {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("original ingress source")
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
                assert!(
                    ingress.state.try_lock().is_ok(),
                    "opaque destruction must not hold the cut"
                );
            }
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn original(f: &Fixture, drops: &Arc<AtomicUsize>) -> ExecutionFailure {
        ExecutionFailure::from_domain(Source {
            ingress: Arc::downgrade(&f.ingress),
            drops: drops.clone(),
        })
    }
    fn failure(f: &Fixture, drops: &Arc<AtomicUsize>) -> TaskConclusionProposal<ExecutionFailure> {
        TaskConclusionProposal::Failed {
            cause: original(f, drops),
            phase: TaskFailurePhase::Execution,
        }
    }
    fn drop_handoff(handoff: ProposalHandoff) {
        assert!(handoff.error.is_none());
        drop(handoff.discarded);
    }

    #[test]
    fn open_only_take_preserves_empty_pairs_and_does_not_steal_an_earlier_cut() {
        let f = Fixture::new(1);
        let addresses = [PairRoute::Open, PairRoute::ConsumingOpen].map(|route| {
            f.ingress
                .pair_inventory(route)
                .unwrap()
                .map(|position| position.key.address())
        });
        for _ in 0..32 {
            assert!(f.ingress.try_take_open_summary().unwrap().is_none());
        }
        assert_eq!(
            [PairRoute::Open, PairRoute::ConsumingOpen].map(|route| {
                f.ingress
                    .pair_inventory(route)
                    .unwrap()
                    .map(|position| position.key.address())
            }),
            addresses
        );
        drop_handoff(f.ingress.submit(TaskConclusionProposal::Succeeded));
        f.reserve(CutId(1));
        f.ingress.seal_cut(CutId(1)).unwrap();
        drop_handoff(f.ingress.submit(TaskConclusionProposal::Succeeded));
        assert!(f.ingress.try_take_open_summary().unwrap().is_none());
        let predecessor = f.ingress.take_next_summary().unwrap();
        assert_eq!(predecessor.cut, Some(CutId(1)));
        assert!(!predecessor.summary.is_empty());
        f.ingress.summary_consumed(Some(CutId(1))).unwrap();
        let open = f.ingress.try_take_open_summary().unwrap().unwrap();
        assert!(open.cut.is_none());
        assert!(!open.summary.is_empty());
        assert!(f.ingress.try_take_open_summary().unwrap().is_none());
        f.ingress.open_summary_consumed().unwrap();
        f.ingress.finish_open_unclaimed().unwrap();
        f.ingress.observe_cut_exit(CutId(1), true, true).unwrap();
        let retired = f.ingress.retire_cut_unclaimed(CutId(1)).unwrap();
        for holder in f
            .ingress
            .take_unused_owned_pair(&retired.pair)
            .unwrap()
            .into_iter()
            .flatten()
        {
            holder.exit();
        }
        for position in f.ingress.owned_pair_inventory(&retired.pair).unwrap() {
            assert!(position.custody.try_retire().unwrap());
        }
        f.ledger.reap_scope_ready(f.scope.id(), 2).unwrap();
        let reaped = f
            .ingress
            .prepare_owned_pair_reap(&retired.pair)
            .unwrap()
            .map(|probe| probe.observe_reaped().unwrap());
        f.ingress
            .release_owned_pair(&retired.pair, reaped)
            .unwrap_or_else(|_| panic!("the exact retired cut pair"));
        drop(retired.funding);
        f.ingress.seal_source_admission();
        assert!(f.ingress.open_summary_drained());
        f.finish();
    }

    #[test]
    fn invalid_geometry_and_full_cut_return_the_same_original_paid_roles() {
        let f = Fixture::new(1);
        let roles = std::array::from_fn(|_| Fixture::make_role(&f.scope, f.capacity.id()));
        let ids = roles.each_ref().map(|role| role.work.id());
        let (error, returned) = match TaskProposalIngress::from_admitted(
            f.task,
            usize::MAX,
            roles,
            f.pending.clone(),
            f.health.clone(),
        ) {
            Err(refusal) => refusal,
            Ok(_) => panic!("overflow must refuse"),
        };
        assert_eq!(error, IngressError::Geometry);
        assert_eq!(returned.each_ref().map(|role| role.work.id()), ids);
        f.dispose_roles(returned);
        f.reserve(CutId(1));
        let roles = f.roles();
        let ids = roles.each_ref().map(|role| role.work.id());
        let version = f.ledger.change_version();
        let (error, admission) = f
            .ingress
            .reserve_cut(CutId(2), 0, CutSourceAdmission::Fresh(roles))
            .unwrap_err();
        assert_eq!(error, IngressError::Cut(CutError::Full));
        assert_eq!(f.ledger.change_version(), version);
        let CutSourceAdmission::Fresh(returned) = admission else {
            panic!("same paid group")
        };
        assert_eq!(returned.each_ref().map(|role| role.work.id()), ids);
        f.dispose_roles(returned);
        f.ingress.observe_cut_exit(CutId(1), true, true).unwrap();
        f.ingress.revoke_unregistered_cut(CutId(1)).unwrap();
        let reaped = f.reap(PairRoute::Cut(CutId(1)));
        let retired = f
            .ingress
            .revoke_unused_cut(CutId(1), reaped)
            .unwrap_or_else(|_| panic!("unused source pair reaped"));
        drop(retired);
        f.finish();
    }
    #[test]
    fn repeated_definite_losers_never_claim_another_source_role_or_drop_under_cut() {
        let f = Fixture::new(0);
        let drops = Arc::new(AtomicUsize::new(0));
        drop_handoff(f.ingress.submit(TaskConclusionProposal::Aborted(
            novarocks_execution_contract::AbortCause::PeerTaskFailed,
        )));
        drop_handoff(f.ingress.submit(failure(&f, &drops)));
        for _ in 0..20 {
            let handoff = f.ingress.submit(failure(&f, &drops));
            assert!(matches!(
                handoff.discarded,
                Some(DiscardedProposal::Unadmitted(_))
            ));
            drop_handoff(handoff);
        }
        assert_eq!(drops.load(Ordering::SeqCst), 20);
        assert_eq!(f.ingress.retained_sources(), 1);
        assert_eq!(f.scope.snapshot().unwrap().work, 5);
        let input = f.ingress.take_next_summary().unwrap();
        let result = input
            .summary
            .apply(ConclusionState::default(), Diagnostics::default());
        assert_eq!(result.state.generation, 2);
        assert_eq!(result.diagnostics.entries.len(), 4);
        assert_eq!(result.diagnostics.dropped, 16);
        let retained = f.ingress.take_source(result.state.source.unwrap()).unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 20);
        f.ingress.open_summary_consumed().unwrap();
        drop(retained);
        assert_eq!(drops.load(Ordering::SeqCst), 21);
        let reaped = f.reap(PairRoute::ConsumingOpen);
        f.ingress
            .finish_open_after_reap(reaped)
            .unwrap_or_else(|_| panic!("original source exit"));
        f.finish();
    }
    #[test]
    fn pending_source_sealed_before_binding_blocks_only_its_task_causal_successor() {
        let f = Fixture::new(1);
        let other = Fixture::new(0);
        let drops = Arc::new(AtomicUsize::new(0));
        let source = original(&f, &drops);
        let phase = TaskFailurePhase::Execution;
        let (mut proposal, source) =
            crate::task_conclusion::PreparedTaskConclusion::new(TaskConclusionProposal::Failed {
                cause: source,
                phase,
            })
            .into_protocol();
        let claim = {
            let mut state = f.ingress.state.lock().unwrap();
            let key = state.open.as_ref().unwrap().keys()[0].clone();
            proposal.source = Some(SourceId(1));
            state.last_source = 1;
            let claim = state.sources.claim(&key, SourceId(1), phase).unwrap();
            assert_eq!(state.segments.push(proposal), None);
            claim
        };
        assert!(f.ingress.try_take_open_summary().unwrap().is_none());
        let cut = f
            .ingress
            .reserve_event_cut(
                f.task,
                CausalEventKind::Input,
                0,
                CutSourceAdmission::Fresh(f.roles()),
            )
            .unwrap_or_else(|_| panic!("actual input pair"));
        f.ingress.bind_event_work(cut, f.parent.id()).unwrap();
        f.ingress
            .register_input_event(cut, ContextInput::CompletionExited { task: f.task })
            .unwrap();
        assert!(matches!(
            f.ingress.take_next_event(false),
            Err(IngressError::Cut(CutError::NotReady))
        ));
        drop_handoff(other.ingress.submit(TaskConclusionProposal::Succeeded));
        let input = other.ingress.take_next_summary().unwrap();
        assert!(input.cut.is_none());
        other.ingress.open_summary_consumed().unwrap();
        let reaped = other.reap(PairRoute::ConsumingOpen);
        other
            .ingress
            .finish_open_after_reap(reaped)
            .unwrap_or_else(|_| panic!("other Task continued"));
        let bound = claim.bind(source.unwrap());
        f.ingress
            .state
            .lock()
            .unwrap()
            .sources
            .install(bound)
            .unwrap();
        let event = f.ingress.take_next_event(false).unwrap();
        assert_eq!(
            event.prefix.summary.candidate_sources().collect::<Vec<_>>(),
            vec![SourceId(1)]
        );
        assert!(matches!(event.event,ContextInput::CompletionExited{task} if task==f.task));
        drop(f.ingress.take_source(SourceId(1)).unwrap());
        f.ingress.summary_consumed(Some(cut)).unwrap();
        f.ingress.observe_cut_exit(cut, true, true).unwrap();
        let reaped = f.reap(PairRoute::Cut(cut));
        drop(
            f.ingress
                .retire_cut(cut, reaped)
                .unwrap_or_else(|_| panic!("source pair actually reaped")),
        );
        f.finish();
        other.finish();
    }
    #[test]
    fn accepted_alias_pins_its_open_cell_while_a_paid_later_cut_and_funding_leg_remain_independent()
    {
        let f = Fixture::new(1);
        let drops = Arc::new(AtomicUsize::new(0));
        f.reserve(CutId(1));
        let funded = f.ingress.cut_funding(CutId(1)).unwrap();
        let old_open = f
            .ingress
            .pair_inventory(PairRoute::Open)
            .unwrap()
            .map(|o| o.key.address());
        let source = original(&f, &drops);
        let early = source.clone();
        drop_handoff(f.ingress.submit(TaskConclusionProposal::Failed {
            cause: source,
            phase: TaskFailurePhase::Execution,
        }));
        let input = f.ingress.take_next_summary().unwrap();
        let result = input
            .summary
            .apply(ConclusionState::default(), Diagnostics::default());
        let retained = f.ingress.take_source(result.state.source.unwrap()).unwrap();
        let accepted = Arc::new(
            AcceptedTaskConclusion::from_retained_source(
                AcceptedConclusion::new(
                    f.task,
                    NonZeroU64::new(result.state.generation).unwrap(),
                    result.state.accepted.unwrap(),
                ),
                retained,
            )
            .unwrap(),
        );
        let late = accepted.originating_failure().unwrap().clone();
        f.ingress.seal_cut(CutId(1)).unwrap();
        assert!(matches!(
            f.ingress.take_next_summary(),
            Err(IngressError::Cut(CutError::Consuming))
        ));
        f.ingress.open_summary_consumed().unwrap();
        assert_eq!(
            f.ingress.finish_open_unclaimed(),
            Err(IngressError::Cut(CutError::ExitUnobserved)),
            "an Accepted source is not an unused permit"
        );
        let later = f.ingress.take_next_summary().unwrap();
        assert_eq!(later.cut, Some(CutId(1)));
        f.ingress.summary_consumed(later.cut).unwrap();
        assert_eq!(
            f.ingress
                .pair_inventory(PairRoute::ConsumingOpen)
                .unwrap()
                .map(|o| o.key.address()),
            old_open
        );
        assert_eq!(
            f.ingress
                .pair_inventory(PairRoute::Open)
                .unwrap()
                .map(|o| o.key.address()),
            funded.keys.each_ref().map(|key| key.address())
        );
        assert!(
            f.ingress
                .prepare_pair_reap(PairRoute::ConsumingOpen)
                .unwrap()
                .into_iter()
                .any(|probe| probe.observe_reaped().is_err())
        );
        assert!(funded.custody.iter().all(|custody| !custody.reusable()));
        drop(accepted);
        drop(early);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(late);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        let reaped = f.reap(PairRoute::ConsumingOpen);
        f.ingress
            .finish_open_after_reap(reaped)
            .unwrap_or_else(|_| panic!("true alias exit"));
        f.ingress.observe_cut_exit(CutId(1), true, true).unwrap();
        let reaped = f.reap(PairRoute::Cut(CutId(1)));
        let retired = f
            .ingress
            .retire_cut(CutId(1), reaped)
            .unwrap_or_else(|_| panic!("prefix exit"));
        assert!(
            retired
                .funding
                .custody
                .iter()
                .all(|custody| !custody.reusable()),
            "prefix exit cannot return the original funding roles now in open"
        );
        drop(retired);
        f.finish();
    }
    #[test]
    fn failed_owner_retains_pending_duties_and_returns_whole_late_and_ready_carriers() {
        let f = Fixture::new(0);
        let drops = Arc::new(AtomicUsize::new(0));
        drop_handoff(f.ingress.submit(failure(&f, &drops)));
        f.ingress.fail();
        assert!(matches!(
            f.ingress.take_next_summary(),
            Err(IngressError::Cut(CutError::NotReady))
        ));
        let late = f.ingress.submit(failure(&f, &drops));
        assert!(!late.transferred);
        assert!(matches!(
            late.discarded,
            Some(DiscardedProposal::Unadmitted(_))
        ));
        drop_handoff(late);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        let retained = f
            .ingress
            .take_failed_source(PairRoute::Open, 0)
            .unwrap()
            .unwrap();
        assert_eq!(retained.cause().failure_fact().class(), FailureClass::Data);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        drop(retained);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
        assert!(
            f.ingress
                .take_failed_source(PairRoute::Open, 0)
                .unwrap()
                .is_none()
        );
        f.finish();
    }
    #[test]
    fn mixed_unused_and_retired_base_pair_requires_actual_unused_exit_before_refill() {
        let f = Fixture::new(0);
        drop_handoff(f.ingress.submit(TaskConclusionProposal::Succeeded));
        f.ingress.take_next_summary().unwrap();
        f.ingress.open_summary_consumed().unwrap();
        let holders = f
            .ingress
            .take_unused_pair(PairRoute::ConsumingOpen)
            .unwrap();
        let [first, second] = holders;
        first.unwrap().exit();
        let observations = f.ingress.pair_inventory(PairRoute::ConsumingOpen).unwrap();
        assert!(observations[0].custody.try_retire().unwrap());
        f.ledger.reap_scope_ready(f.scope.id(), 1).unwrap();
        let [first_probe, second_probe] = f
            .ingress
            .prepare_pair_reap(PairRoute::ConsumingOpen)
            .unwrap();
        assert!(first_probe.observe_reaped().is_ok());
        assert!(second_probe.observe_reaped().is_err());
        second.unwrap().exit();
        assert!(observations[1].custody.try_retire().unwrap());
        f.ledger.reap_scope_ready(f.scope.id(), 1).unwrap();
        let reaped = f
            .ingress
            .prepare_pair_reap(PairRoute::ConsumingOpen)
            .unwrap()
            .map(|probe| probe.observe_reaped().unwrap());
        f.ingress
            .finish_open_after_reap(reaped)
            .unwrap_or_else(|_| panic!("both original unused exits"));
        let reaped = f
            .ingress
            .prepare_pair_reap(PairRoute::ConsumingOpen)
            .unwrap()
            .map(|probe| probe.observe_reaped().unwrap());
        let roles = f.roles();
        let ids = roles.each_ref().map(|role| role.report.id());
        f.ingress
            .rearm_open_spare(reaped, roles)
            .unwrap_or_else(|_| panic!("same base addresses"));
        assert_eq!(
            f.ingress
                .pair_inventory(PairRoute::ConsumingOpen)
                .unwrap()
                .map(|o| o.custody.report().id()),
            ids
        );
        assert_eq!(f.ingress.state.lock().unwrap().sources.len(), 4);
        f.finish();
    }
    #[test]
    fn a_late_binder_after_owner_failure_returns_the_same_admitted_carrier() {
        let f = Fixture::new(0);
        let drops = Arc::new(AtomicUsize::new(0));
        let original = original(&f, &drops);
        let early = original.clone();
        let pointer = original.cause() as *const _;
        let (mut proposal, original) =
            crate::task_conclusion::PreparedTaskConclusion::new(TaskConclusionProposal::Failed {
                cause: original,
                phase: TaskFailurePhase::Preparation,
            })
            .into_protocol();
        let claim = {
            let mut state = f.ingress.state.lock().unwrap();
            let key = state.open.as_ref().unwrap().keys()[0].clone();
            proposal.source = Some(SourceId(1));
            state.last_source = 1;
            let claim = state
                .sources
                .claim(&key, SourceId(1), TaskFailurePhase::Preparation)
                .unwrap();
            assert_eq!(state.segments.push(proposal), None);
            claim
        };
        f.ingress.fail();
        assert!(
            f.ingress
                .take_failed_source(PairRoute::Open, 0)
                .unwrap()
                .is_none(),
            "Pending is a retained duty, not a missing source"
        );
        let handoff = f.ingress.finish_binding(claim.bind(original.unwrap()));
        assert!(handoff.transferred);
        assert!(handoff.error.is_none());
        let Some(DiscardedProposal::Admitted(carrier)) = handoff.discarded else {
            panic!("same admitted carrier")
        };
        assert_eq!(carrier.cause().cause() as *const _, pointer);
        assert!(
            !f.ingress
                .state
                .lock()
                .unwrap()
                .sources
                .is_ready(SourceId(1))
        );
        let observed = f.ingress.source_inventory(SourceId(1)).unwrap();
        assert_eq!(observed.phase, SourcePositionPhase::Taken);
        drop(carrier);
        assert!(!observed.custody.actual_exit_ready().unwrap());
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(early);
        assert!(observed.custody.actual_exit_ready().unwrap());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        f.finish();
    }
    #[test]
    fn unclaimed_open_summaries_rotate_original_permits_without_churning_work() {
        let f = Fixture::new(0);
        let before = f.scope.snapshot().unwrap();
        let version = f.ledger.change_version();
        let initial = [PairRoute::Open, PairRoute::ConsumingOpen].map(|route| {
            f.ingress
                .pair_inventory(route)
                .unwrap()
                .map(|o| o.custody.report().id())
        });
        for _ in 0..128 {
            drop_handoff(f.ingress.submit(TaskConclusionProposal::Succeeded));
            let input = f.ingress.take_next_summary().unwrap();
            assert!(input.cut.is_none());
            assert_eq!(input.summary.candidate_sources().count(), 0);
            assert_eq!(
                f.ingress.finish_open_unclaimed(),
                Err(IngressError::Cut(CutError::Consuming)),
                "storage observation cannot sign a Core acknowledgement"
            );
            f.ingress.open_summary_consumed().unwrap();
            f.ingress.finish_open_unclaimed().unwrap();
            assert_eq!(f.scope.snapshot().unwrap(), before);
            assert_eq!(f.ledger.change_version(), version);
        }
        let final_reports = [PairRoute::Open, PairRoute::ConsumingOpen].map(|route| {
            f.ingress
                .pair_inventory(route)
                .unwrap()
                .map(|o| o.custody.report().id())
        });
        assert_eq!(final_reports, initial);
        assert_eq!(f.ingress.state.lock().unwrap().sources.len(), 4);
        f.finish();
    }
    #[test]
    fn unclaimed_cut_handoff_reuses_six_addresses_and_retains_original_funding_observation() {
        let f = Fixture::new(1);
        let mut admission = CutSourceAdmission::Fresh(f.roles());
        let before = f.scope.snapshot().unwrap();
        let version = f.ledger.change_version();
        for n in 1..=64 {
            let cut = CutId(n);
            f.ingress
                .reserve_cut(cut, 0, admission)
                .unwrap_or_else(|_| panic!("same real paid permits"));
            drop_handoff(f.ingress.submit(TaskConclusionProposal::Succeeded));
            f.ingress.seal_cut(cut).unwrap();
            let input = f.ingress.take_next_summary().unwrap();
            assert_eq!(input.cut, Some(cut));
            assert_eq!(input.summary.candidate_sources().count(), 0);
            f.ingress.summary_consumed(Some(cut)).unwrap();
            f.ingress.observe_cut_exit(cut, true, true).unwrap();
            let retired = f.ingress.retire_cut_unclaimed(cut).unwrap();
            assert!(
                retired.funding.custody.iter().all(|role| role.phase()
                    == novarocks_control_runtime::exit_role::ExitRolePhase::Admitted),
                "cell handoff neither exits source roles nor refunds funding"
            );
            admission = CutSourceAdmission::Unclaimed(retired.pair);
            assert_eq!(f.scope.snapshot().unwrap(), before);
            assert_eq!(f.ledger.change_version(), version);
            assert_eq!(f.ingress.state.lock().unwrap().sources.len(), 6);
        }
        f.ingress
            .reserve_cut(CutId(65), 0, admission)
            .unwrap_or_else(|_| panic!("original unused pair"));
        f.ingress.revoke_unregistered_cut(CutId(65)).unwrap();
        f.ingress.observe_cut_exit(CutId(65), true, true).unwrap();
        let reaped = f.reap(PairRoute::Cut(CutId(65)));
        drop(
            f.ingress
                .revoke_unused_cut(CutId(65), reaped)
                .unwrap_or_else(|_| panic!("original unused source roles actually reaped")),
        );
        f.finish();
    }
    #[test]
    fn an_owned_unclaimed_pair_can_really_exit_after_routing_admission_has_closed() {
        let f = Fixture::new(1);
        f.reserve(CutId(1));
        f.ingress.revoke_unregistered_cut(CutId(1)).unwrap();
        f.ingress.observe_cut_exit(CutId(1), true, true).unwrap();
        let retired = f.ingress.revoke_unused_cut_unclaimed(CutId(1)).unwrap();
        f.ingress.seal_source_admission();
        assert!(retired.funding.custody.iter().all(|role| !role.reusable()));
        for holder in f
            .ingress
            .take_unused_owned_pair(&retired.pair)
            .unwrap()
            .into_iter()
            .flatten()
        {
            holder.exit();
        }
        for observation in f.ingress.owned_pair_inventory(&retired.pair).unwrap() {
            assert!(observation.custody.actual_exit_ready().unwrap());
            assert!(observation.source.is_none());
            assert!(observation.custody.try_retire().unwrap());
        }
        f.ledger.reap_scope_ready(f.scope.id(), 2).unwrap();
        let witnesses = f
            .ingress
            .prepare_owned_pair_reap(&retired.pair)
            .unwrap()
            .map(|probe| probe.observe_reaped().unwrap());
        f.ingress
            .release_owned_pair(&retired.pair, witnesses)
            .unwrap_or_else(|_| panic!("same owned pair actually reaped"));
        assert!(retired.funding.custody.iter().all(|role| role.reusable()));
        drop(retired);
        f.finish();
    }
}
