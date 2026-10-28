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

//! The Task's one original paid open-summary input. Pending notifications
//! only ask this custodian to inspect the existing proposal buffer; they never
//! admit another Work, source position, cut, or runtime input role.

use super::{
    actors::{ContextMessage, WorkerActor, WorkerMessage},
    failure::ScopePublicationsDrained,
    lifecycle_read::TaskLifetime,
    source_positions::ReapedPosition,
};
use crate::task_proposal_ingress::{IngressError, TaskProposalIngress};
use novarocks_control_runtime::{
    driver::{ActorRuntime, DriverError, InputPhase, InputRole},
    transaction::RuntimeError,
};
use novarocks_control_runtime_core::{
    completion::Consumption,
    identity::{ActorId, Generation, InputId, WorkId},
};
use novarocks_execution_contract::TaskIdentity;
use novarocks_worker_protocol::{context::ContextInput, proposal_summary::SourceId};
use std::{mem::ManuallyDrop, sync::Arc};

/// The original Task lifetime funds this one reusable neutral input. It owns
/// no additional Work or report, and source positions are paid separately.
pub(crate) const OPEN_SUMMARY_STORAGE: crate::control_runtime_policy::StorageNeed =
    crate::control_runtime_policy::StorageNeed {
        requests: 0,
        inputs: 1,
        batches: 1,
        effects: novarocks_worker_protocol::context::ContextCore::MAX_STEP_WORK,
        cuts: 0,
        replies: 0,
        completions: 0,
        captures: 0,
        work: 0,
    };

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OpenSummaryError {
    Driver(DriverError),
    Ingress(IngressError),
    Runtime(RuntimeError),
    Busy,
    NotDelivered,
    InputObservationMissing,
    UnknownSource,
    WrongLifetime,
    WorkStillLive,
}
impl From<DriverError> for OpenSummaryError {
    fn from(error: DriverError) -> Self {
        Self::Driver(error)
    }
}
impl From<IngressError> for OpenSummaryError {
    fn from(error: IngressError) -> Self {
        Self::Ingress(error)
    }
}

struct Delivered {
    input: Option<InputId>,
    sources: [Option<SourceId>; 2],
    consumption: Consumption,
    // Unexpected owner loss must not drop even the precise refused neutral
    // message. Rich sources remain in the original ingress positions.
    rejected: Option<ManuallyDrop<WorkerMessage>>,
}

#[must_use = "The original open-summary input closes only after actual Task and source retirement"]
pub(crate) struct TaskOpenSummaryInbox {
    runtime: ActorRuntime<WorkerActor>,
    actor: ActorId,
    task: TaskIdentity,
    work: WorkId,
    ingress: Arc<TaskProposalIngress>,
    input: InputRole<WorkerActor>,
    delivered: Option<Delivered>,
    closed: bool,
}
impl TaskOpenSummaryInbox {
    /// The trusted Task issuer supplies its existing InputRole, original Task
    /// Work, and the same source FIFO as the Task's causal authors. This method
    /// reserves nothing and creates no alternate protocol generation state.
    pub(crate) fn from_admitted(
        runtime: ActorRuntime<WorkerActor>,
        actor: ActorId,
        task: TaskIdentity,
        work: WorkId,
        ingress: Arc<TaskProposalIngress>,
        input: InputRole<WorkerActor>,
    ) -> Result<Self, (OpenSummaryError, InputRole<WorkerActor>)> {
        match input.in_flight() {
            Ok(None) => Ok(Self {
                runtime,
                actor,
                task,
                work,
                ingress,
                input,
                delivered: None,
                closed: false,
            }),
            Ok(Some(_)) => Err((OpenSummaryError::Busy, input)),
            Err(error) => Err((error.into(), input)),
        }
    }
    /// One input may be in flight or awaiting source retirement. The caller's
    /// current protocol generation is used verbatim for this original summary.
    pub(crate) fn deliver_next(
        &mut self,
        generation: Generation,
    ) -> Result<Option<InputId>, OpenSummaryError> {
        if self.closed {
            return Err(DriverError::Closed.into());
        }
        if self.delivered.is_some() || self.input.in_flight()?.is_some() {
            return Ok(None);
        }
        let Some(summary) = self.ingress.try_take_open_summary()? else {
            return Ok(None);
        };
        assert!(summary.cut.is_none(), "only the original open segment");
        let mut candidates = summary.summary.candidate_sources();
        let sources = [candidates.next(), candidates.next()];
        assert!(candidates.next().is_none(), "two fixed source candidates");
        // End the borrowed candidate iterator before moving the same summary.
        drop(candidates);
        let message = WorkerMessage::Context(ContextMessage::Command {
            work: self.work,
            request: None,
            command: ContextInput::Summary {
                task: self.task,
                generation,
                cut: None,
                summary: summary.summary,
            },
        });
        let (input, rejected, error) = match self.input.fill(message) {
            Ok(input) => (Some(input), None, None),
            Err((error, message)) => (None, Some(ManuallyDrop::new(message)), Some(error)),
        };
        self.delivered = Some(Delivered {
            input,
            sources,
            consumption: Consumption::Pending,
            rejected,
        });
        match error {
            Some(error) => Err(error.into()),
            None => Ok(input),
        }
    }
    /// Only the exact real runtime input's commit consumes this summary. The
    /// cached receipt outlives that input record until the same pair retires.
    pub(crate) fn observe_core(&mut self) -> Result<Consumption, OpenSummaryError> {
        let delivered = self
            .delivered
            .as_mut()
            .ok_or(OpenSummaryError::NotDelivered)?;
        if delivered.consumption != Consumption::Pending {
            return Ok(delivered.consumption);
        }
        let Some(input) = delivered.input else {
            return Ok(Consumption::Pending);
        };
        let phase = self
            .runtime
            .input_custody(self.actor, input)?
            .ok_or(OpenSummaryError::InputObservationMissing)?
            .phase;
        if phase == InputPhase::Committed {
            self.ingress.open_summary_consumed()?;
            delivered.consumption = Consumption::Consumed;
        }
        Ok(delivered.consumption)
    }
    pub(crate) fn source_input(
        &self,
        source: SourceId,
    ) -> Result<Option<InputId>, OpenSummaryError> {
        Ok(self.source_delivery(source)?.input)
    }
    /// Idle notification storage alone proves neither Task nor Source exit.
    pub(crate) fn is_idle(&self) -> Result<bool, OpenSummaryError> {
        Ok(self.closed || (self.delivered.is_none() && self.input.in_flight()?.is_none()))
    }
    pub(crate) const fn is_closed(&self) -> bool {
        self.closed
    }
    pub(crate) fn source_core_consumption(
        &self,
        source: SourceId,
    ) -> Result<Consumption, OpenSummaryError> {
        Ok(self.source_delivery(source)?.consumption)
    }
    fn source_delivery(&self, source: SourceId) -> Result<&Delivered, OpenSummaryError> {
        let delivered = self
            .delivered
            .as_ref()
            .ok_or(OpenSummaryError::NotDelivered)?;
        if !delivered.sources.contains(&Some(source)) {
            return Err(OpenSummaryError::UnknownSource);
        }
        Ok(delivered)
    }
    /// Take an original queued summary which never filled this input. Only
    /// the actual drained failure session can name its Core leg abandoned.
    /// No new input, report, producer or storage credit is admitted here.
    pub(crate) fn take_failed_queued(
        &mut self,
        drained: &ScopePublicationsDrained<'_>,
    ) -> Result<Option<crate::task_proposal_ingress::SummaryInput>, OpenSummaryError> {
        drained.validate(&self.runtime, self.actor)?;
        if self.closed || self.delivered.is_some() || self.input.in_flight()?.is_some() {
            return Err(OpenSummaryError::Busy);
        }
        self.ingress.fail();
        let original = self.ingress.take_failed_open_summary()?;
        if let Some(original) = &original {
            let mut sources = [None, None];
            for (target, source) in sources.iter_mut().zip(original.summary.candidate_sources()) {
                *target = Some(source);
            }
            self.delivered = Some(Delivered {
                input: None,
                sources,
                consumption: Consumption::AbandonedByOwnerFailure,
                rejected: None,
            });
        }
        Ok(original)
    }
    /// Only the real failed-owner publication session can abandon the Core
    /// leg. A refused fill proves that no input transferred; a transferred one
    /// must have the actual runtime abandonment phase before takeover.
    pub(crate) fn abandon_core(
        &mut self,
        drained: &ScopePublicationsDrained<'_>,
    ) -> Result<Consumption, OpenSummaryError> {
        drained.validate(&self.runtime, self.actor)?;
        let delivered = self
            .delivered
            .as_mut()
            .ok_or(OpenSummaryError::NotDelivered)?;
        if delivered.consumption != Consumption::Pending {
            return Ok(delivered.consumption);
        }
        if let Some(input) = delivered.input {
            let phase = self
                .runtime
                .input_custody(self.actor, input)?
                .ok_or(OpenSummaryError::InputObservationMissing)?
                .phase;
            if phase == InputPhase::Committed {
                return self.observe_core();
            }
            if phase != InputPhase::AbandonedByOwnerFailure {
                return Ok(Consumption::Pending);
            }
        }
        self.ingress.fail();
        delivered.consumption = Consumption::AbandonedByOwnerFailure;
        Ok(delivered.consumption)
    }
    pub(crate) fn take_rejected(
        &mut self,
        drained: &ScopePublicationsDrained<'_>,
    ) -> Result<Option<WorkerMessage>, OpenSummaryError> {
        drained.validate(&self.runtime, self.actor)?;
        Ok(self
            .delivered
            .as_mut()
            .and_then(|delivered| delivered.rejected.take())
            .map(ManuallyDrop::into_inner))
    }
    fn check_retirement(&self) -> Result<(), OpenSummaryError> {
        let delivered = self
            .delivered
            .as_ref()
            .ok_or(OpenSummaryError::NotDelivered)?;
        if delivered.consumption == Consumption::Pending
            || delivered.rejected.is_some()
            || self.input.in_flight()?.is_some()
        {
            return Err(OpenSummaryError::Busy);
        }
        Ok(())
    }
    /// A source-free summary can hand the same unused permits back to spare.
    /// Neither this operation nor input retirement releases those source roles.
    pub(crate) fn retire_unclaimed(&mut self) -> Result<(), OpenSummaryError> {
        self.check_retirement()?;
        self.ingress.finish_open_unclaimed()?;
        self.delivered = None;
        Ok(())
    }
    pub(crate) fn retire_after_reap(
        &mut self,
        reaped: [ReapedPosition; 2],
    ) -> Result<(), (OpenSummaryError, [ReapedPosition; 2])> {
        if let Err(error) = self.check_retirement() {
            return Err((error, reaped));
        }
        if let Err((error, reaped)) = self.ingress.finish_open_after_reap(reaped) {
            return Err((error.into(), reaped));
        }
        self.delivered = None;
        Ok(())
    }
    /// Close only after the original Task Work (including its source children)
    /// has really exited, and after its last summary/input has retired.
    pub(crate) fn close_after_exit(
        &mut self,
        lifetime: &TaskLifetime,
    ) -> Result<(), OpenSummaryError> {
        if lifetime.id() != self.work || lifetime.observation().identity() != self.task {
            return Err(OpenSummaryError::WrongLifetime);
        }
        if self.closed {
            return Ok(());
        }
        if !lifetime
            .actual_exit_ready()
            .map_err(OpenSummaryError::Runtime)?
        {
            return Err(OpenSummaryError::WorkStillLive);
        }
        if !self.is_idle()? || !self.ingress.open_summary_drained() {
            return Err(OpenSummaryError::Busy);
        }
        self.input.close_after_exit()?;
        self.closed = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_runtime::{
        actors::tests::{Fixture, actor, finish_source_routes, generation, task},
        task::TaskHostResources,
    };
    use crate::task_proposal_ingress::PairRoute;
    use novarocks_control_runtime_core::{
        identity::{ConsumerId, Tick},
        mailbox::InputClass,
    };
    use novarocks_execution::runtime::fragment::ExecutionFailure;
    use novarocks_execution_contract::{
        CancelReason, TaskConclusionProposal, TaskFailurePhase,
        failure::{FailureClass, FailureFact, FailureProjection, FailureReason, domain},
    };
    use novarocks_worker_protocol::creation::CreationCost;
    use std::{
        num::NonZeroU16,
        sync::atomic::{AtomicUsize, Ordering},
    };

    fn admitted(f: &mut Fixture) -> (TaskHostResources, TaskOpenSummaryInbox) {
        let task = task(f.identity, 1);
        let mut host = f.adopt_task_host(task, 1);
        host.admit_open_summary(&f.runtime).unwrap();
        assert!(host.open_summary_mut().is_some());
        let count = f.runtime.role_inventory(actor(), None, 32).unwrap().len();
        assert!(matches!(
            host.admit_open_summary(&f.runtime),
            Err(crate::control_runtime::task::TaskHostError::OpenSummaryAlreadyIssued)
        ));
        assert_eq!(
            f.runtime.role_inventory(actor(), None, 32).unwrap().len(),
            count
        );
        let mut resources = host.into_resources();
        // Summary is an Accepted Task protocol fact. These are model-world
        // events; the actual preparation reservation still belongs to the
        // fixture's admitted job until its real child exit at teardown.
        for command in [
            ContextInput::Loaded {
                task,
                result: Ok(CreationCost {
                    input_retained_bytes: 1,
                    ingress_retained_bytes: 1,
                    structural_bytes: 1,
                    inbound_records: 0,
                    inbound_bytes: 0,
                }),
            },
            ContextInput::Installed { task },
        ] {
            let input = f.send(command, None, InputClass::Lifecycle, 2);
            f.runtime.poll_one(Tick(1)).unwrap();
            assert_eq!(
                f.runtime
                    .input_custody(actor(), input)
                    .unwrap()
                    .unwrap()
                    .phase,
                InputPhase::Committed,
                "the real Accepted/Installed Core step precedes a summary"
            );
            f.drain(input);
        }
        let open = resources
            .open_summary
            .take()
            .expect("the original host-owned role");
        (resources, open)
    }
    fn finish(mut f: Fixture, mut resources: TaskHostResources, mut open: TaskOpenSummaryInbox) {
        let ingress = resources.port.ingress().clone();
        finish_source_routes(&f, &ingress);
        f.finish_jobs();
        crate::control_runtime::task::retire_unused_host_calls_for_test(
            &resources.host_calls,
            &f.ledger,
        );
        resources.lifetime.body_exited().unwrap();
        resources.lifetime.captures_exited().unwrap();
        assert!(resources.lifetime.actual_exit_ready().unwrap());
        open.close_after_exit(&resources.lifetime).unwrap();
        resources
            .active
            .release(&resources.active.resources().unwrap())
            .unwrap();
        f.ledger.reap_scope_ready(f.scope.id(), 64).unwrap();
        drop(open);
        drop(resources);
        drop(ingress);
        f.finish();
    }
    #[test]
    fn empty_notifications_do_not_fill_or_rotate_and_committed_input_retirement_is_not_summary_retirement()
     {
        let mut f = Fixture::new();
        let (resources, mut open) = admitted(&mut f);
        let ingress = resources.port.ingress().clone();
        let pairs = [PairRoute::Open, PairRoute::ConsumingOpen].map(|route| {
            ingress
                .pair_inventory(route)
                .unwrap()
                .map(|position| position.key.address())
        });
        let before = f.scope.snapshot().unwrap();
        for _ in 0..32 {
            assert_eq!(open.deliver_next(generation()).unwrap(), None);
        }
        assert_eq!(f.scope.snapshot().unwrap(), before);
        assert_eq!(open.input.in_flight().unwrap(), None);
        assert!(open.is_idle().unwrap());
        assert_eq!(
            [PairRoute::Open, PairRoute::ConsumingOpen].map(|route| {
                ingress
                    .pair_inventory(route)
                    .unwrap()
                    .map(|position| position.key.address())
            }),
            pairs
        );
        assert_eq!(
            open.close_after_exit(&resources.lifetime),
            Err(OpenSummaryError::WorkStillLive)
        );
        let handoff = ingress.submit(TaskConclusionProposal::Canceled(
            CancelReason::UpstreamNoLongerNeeded,
        ));
        assert!(handoff.error.is_none() && handoff.discarded.is_none());
        let input = open.deliver_next(generation()).unwrap().unwrap();
        assert!(!open.is_idle().unwrap());
        assert_eq!(open.observe_core().unwrap(), Consumption::Pending);
        assert_eq!(open.deliver_next(generation()).unwrap(), None);
        assert_eq!(open.retire_unclaimed(), Err(OpenSummaryError::Busy));
        f.runtime.poll_one(Tick(2)).unwrap();
        assert_eq!(open.observe_core().unwrap(), Consumption::Consumed);
        assert_eq!(
            open.retire_unclaimed(),
            Err(OpenSummaryError::Busy),
            "the original input is still in flight"
        );
        f.drain(input);
        assert!(f.runtime.input_custody(actor(), input).unwrap().is_none());
        assert_eq!(open.observe_core().unwrap(), Consumption::Consumed);
        assert_eq!(
            open.deliver_next(generation()).unwrap(),
            None,
            "retired InputId alone cannot overwrite the summary cache"
        );
        open.retire_unclaimed().unwrap();
        assert!(open.is_idle().unwrap());
        assert_eq!(open.deliver_next(generation()).unwrap(), None);
        finish(f, resources, open);
    }

    #[test]
    fn host_read_seal_refuses_its_original_unclosed_open_summary_role() {
        let mut f = Fixture::new();
        let (mut resources, open) = admitted(&mut f);
        assert!(!open.is_closed());
        resources.open_summary = Some(open);
        // Isolate the original open-summary gate: empty host-call retirement
        // must not mask whether this still-live summary prevents read sealing.
        crate::control_runtime::task::retire_unused_host_calls_for_test(
            &resources.host_calls,
            &f.ledger,
        );
        assert!(matches!(
            resources.seal_read_observation(),
            Err(crate::control_runtime::lifecycle_read::TaskReadError::HostInventoryStillLive)
        ));
        let open = resources.open_summary.take().unwrap();
        finish(f, resources, open);
    }

    #[derive(Debug)]
    struct Source(Arc<AtomicUsize>);
    impl std::fmt::Display for Source {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("actual open-summary source")
        }
    }
    impl std::error::Error for Source {}
    impl FailureProjection for Source {
        fn failure_fact(&self) -> FailureFact {
            FailureFact::new(
                FailureClass::Data,
                FailureReason::new(domain::WORKER, NonZeroU16::new(65002).unwrap()),
            )
        }
    }
    impl Drop for Source {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[test]
    fn original_source_receipt_survives_input_retirement_and_alias_pins_the_same_pair_until_reap() {
        let mut f = Fixture::new();
        let (resources, mut open) = admitted(&mut f);
        let ingress = resources.port.ingress().clone();
        let drops = Arc::new(AtomicUsize::new(0));
        let cause = ExecutionFailure::from_domain(Source(drops.clone()));
        let early_alias = cause.clone();
        let handoff = ingress.submit(TaskConclusionProposal::Failed {
            cause,
            phase: TaskFailurePhase::Execution,
        });
        assert!(handoff.transferred && handoff.discarded.is_none() && handoff.error.is_none());
        let source = ingress
            .pair_inventory(PairRoute::Open)
            .unwrap()
            .into_iter()
            .find_map(|position| position.source)
            .unwrap();
        let input = open.deliver_next(generation()).unwrap().unwrap();
        assert_eq!(open.source_input(source).unwrap(), Some(input));
        assert_eq!(
            open.source_core_consumption(source).unwrap(),
            Consumption::Pending
        );
        assert_eq!(
            open.source_input(SourceId(source.0 + 1)),
            Err(OpenSummaryError::UnknownSource)
        );
        f.runtime.poll_one(Tick(2)).unwrap();
        assert_eq!(open.observe_core().unwrap(), Consumption::Consumed);
        f.drain(input);
        assert_eq!(open.source_input(source).unwrap(), Some(input));
        assert_eq!(
            open.source_core_consumption(source).unwrap(),
            Consumption::Consumed
        );
        let retained = ingress.take_source(source).unwrap();
        let late_alias = retained.cause().clone();
        drop(retained);
        drop(early_alias);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(
            open.retire_unclaimed(),
            Err(OpenSummaryError::Ingress(IngressError::Cut(
                novarocks_worker_protocol::proposal_summary::CutError::ExitUnobserved
            )))
        );
        assert!(
            ingress
                .prepare_pair_reap(PairRoute::ConsumingOpen)
                .unwrap()
                .into_iter()
                .any(|probe| probe.observe_reaped().is_err())
        );
        assert_eq!(open.deliver_next(generation()).unwrap(), None);
        for holder in ingress
            .take_unused_pair(PairRoute::ConsumingOpen)
            .unwrap()
            .into_iter()
            .flatten()
        {
            holder.exit();
        }
        let observations = ingress.pair_inventory(PairRoute::ConsumingOpen).unwrap();
        let original = observations
            .iter()
            .find(|position| position.source == Some(source))
            .unwrap();
        assert_eq!(
            original
                .custody
                .report()
                .consume(ConsumerId::new(1).unwrap()),
            Err(RuntimeError::Completion(
                novarocks_control_runtime_core::completion::CompletionError::NotFilled
            )),
            "the cached Core receipt cannot fill a still-live Source report"
        );
        assert!(!original.custody.actual_exit_ready().unwrap());
        assert!(!resources.lifetime.actual_exit_ready().unwrap());
        drop(late_alias);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        for position in observations {
            assert!(position.custody.actual_exit_ready().unwrap());
            if let Some(source) = position.source {
                assert_eq!(
                    open.source_core_consumption(source).unwrap(),
                    Consumption::Consumed
                );
                position
                    .custody
                    .report()
                    .consume(ConsumerId::new(1).unwrap())
                    .unwrap();
                position
                    .custody
                    .report()
                    .consume(ConsumerId::new(2).unwrap())
                    .unwrap();
            }
            assert!(position.custody.try_retire().unwrap());
        }
        assert!(
            ingress
                .prepare_pair_reap(PairRoute::ConsumingOpen)
                .unwrap()
                .into_iter()
                .all(|probe| probe.observe_reaped().is_err()),
            "report retirement does not reap common Work headers"
        );
        f.ledger.reap_scope_ready(f.scope.id(), 64).unwrap();
        let reaped = ingress
            .prepare_pair_reap(PairRoute::ConsumingOpen)
            .unwrap()
            .map(|probe| probe.observe_reaped().unwrap());
        open.retire_after_reap(reaped)
            .unwrap_or_else(|_| panic!("the exact source pair really reaped"));
        assert_eq!(
            open.source_input(source),
            Err(OpenSummaryError::NotDelivered)
        );
        finish(f, resources, open);
    }

    #[test]
    fn missing_actual_input_does_not_become_a_cached_commit() {
        let mut f = Fixture::new();
        let (resources, mut open) = admitted(&mut f);
        let ingress = resources.port.ingress().clone();
        drop(ingress.submit(TaskConclusionProposal::Canceled(
            CancelReason::UpstreamNoLongerNeeded,
        )));
        let input = open.deliver_next(generation()).unwrap().unwrap();
        f.runtime.poll_one(Tick(2)).unwrap();
        // This deliberately violates the host observation order. There is no
        // fallback from the absent record to a guessed positive Core receipt.
        f.drain(input);
        assert_eq!(
            open.observe_core(),
            Err(OpenSummaryError::InputObservationMissing)
        );
        assert_eq!(open.retire_unclaimed(), Err(OpenSummaryError::Busy));
        // Keep the real owner inventory: without its consumed-input proof this
        // is unresolved, not a cleanly closed Task fabricated by the fixture.
        let _retained = ManuallyDrop::new((f, resources, open));
    }
    #[test]
    fn failed_real_scope_fill_retains_the_exact_neutral_message_and_original_source_inventory() {
        let mut f = Fixture::new();
        let (resources, mut open) = admitted(&mut f);
        let ingress = resources.port.ingress().clone();
        let drops = Arc::new(AtomicUsize::new(0));
        let handoff = ingress.submit(TaskConclusionProposal::Failed {
            cause: ExecutionFailure::from_domain(Source(drops.clone())),
            phase: TaskFailurePhase::Execution,
        });
        assert!(handoff.transferred && handoff.discarded.is_none() && handoff.error.is_none());
        let source = ingress
            .pair_inventory(PairRoute::Open)
            .unwrap()
            .into_iter()
            .find_map(|position| position.source)
            .unwrap();
        f.scope.fail().unwrap();
        assert_eq!(
            open.deliver_next(generation()),
            Err(OpenSummaryError::Driver(DriverError::ActorFailed))
        );
        assert_eq!(open.source_input(source).unwrap(), None);
        assert_eq!(
            open.source_core_consumption(source).unwrap(),
            Consumption::Pending
        );
        assert_eq!(open.observe_core().unwrap(), Consumption::Pending);
        assert_eq!(open.deliver_next(generation()).unwrap(), None);
        assert_eq!(open.retire_unclaimed(), Err(OpenSummaryError::Busy));
        assert!(open.input.in_flight().unwrap().is_none());
        let rejected = open.delivered.as_ref().unwrap().rejected.as_ref().unwrap();
        let WorkerMessage::Context(ContextMessage::Command {
            work,
            command:
                ContextInput::Summary {
                    task,
                    generation: epoch,
                    cut,
                    summary,
                },
            ..
        }) = &**rejected
        else {
            panic!("the same refused neutral summary is retained");
        };
        assert_eq!(*work, resources.lifetime.id());
        assert_eq!(*task, resources.port.identity());
        assert_eq!(*epoch, generation());
        assert_eq!(*cut, None);
        assert_eq!(
            summary.candidate_sources().collect::<Vec<_>>(),
            vec![source]
        );
        let position = ingress.source_inventory(source).unwrap();
        assert_eq!(position.source, Some(source));
        assert!(!position.custody.actual_exit_ready().unwrap());
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        // Scope failure is not a publication-drained token. Preserve this
        // original refused owner inventory for its actual failure dispatcher.
        let _retained = ManuallyDrop::new((f, resources, open));
    }
}
