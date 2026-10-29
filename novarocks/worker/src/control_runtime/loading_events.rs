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

//! Two original prepaid loading authors. The measured result can reach Core
//! before the loader's input child exits; only the separate exit author waits
//! for that complete physical tree. No notification admits another role.

use super::{
    causal::{CausalAuthorBinding, CausalError, TaskCausalInbox, TaskCausalSender},
    loading::{LoaderCustody, LoaderJob, LoaderOutcome},
    preparation::QueuedPreparation,
};
use novarocks_control_runtime::{
    completion::ReportSlot,
    exit_role::{ExitRoleError, ExitRolePhase},
    transaction::RuntimeError,
};
use novarocks_control_runtime_core::{
    completion::Consumption,
    identity::{ConsumerId, ScopeId, WorkId},
};
use novarocks_execution_contract::{OperationOutcome, TaskIdentity};
use novarocks_worker_protocol::{
    context::{ContextInput, PhysicalFact},
    creation::AdmissionRefusal,
    proposal_summary::CutId,
};
use std::{
    mem::ManuallyDrop,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

const LOADED: usize = 0;
const EXITED: usize = 1;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AuthorPhase {
    Waiting,
    Sending,
    Sent,
    Rejected,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReturnPhase {
    Waiting,
    Issued,
    Observed,
    Dropped,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LoadingEventError {
    Causal(CausalError),
    Role(ExitRoleError),
    Report(RuntimeError),
    LoadedRejected,
    ExitRejected,
    StartRefused(ExitRoleError),
    Unresolved,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LoadingEventOutcome {
    Loaded,
    Canceled,
    Rejected,
    StartRefused(ExitRoleError),
    Unresolved,
}
#[derive(Clone, Copy)]
enum Fault {
    StartRefused(ExitRoleError),
    Unresolved,
}
struct State {
    senders: [Option<TaskCausalSender>; 2],
    phases: [AuthorPhase; 2],
    returns: [ReturnPhase; 2],
    // Exactly the two original authors can refuse a neutral handoff. Losing
    // their owner cannot drop or reinterpret those exact returned payloads.
    rejected: [Option<ManuallyDrop<ContextInput>>; 2],
    fault: Option<Fault>,
}
struct Custody {
    task: TaskIdentity,
    loader: LoaderCustody,
    bindings: [CausalAuthorBinding; 2],
    issued: AtomicBool,
    state: Mutex<State>,
    retained: Mutex<Option<Arc<Self>>>,
}
#[derive(Clone)]
pub(crate) struct LoadingEvents(Arc<Custody>);
#[must_use = "The original loader job must run once or retain its unknown exit"]
pub(crate) struct LoadingEventJob {
    job: Option<LoaderJob>,
    events: LoadingEvents,
    active: bool,
}
/// The actual original author has returned to its caller. Cloning a binding
/// grants no such fact; only the unique job/publisher can create this token.
#[must_use = "The original author return must reach its owning Causal inbox"]
pub(crate) struct LoadingAuthorReturn {
    events: LoadingEvents,
    author: usize,
    armed: bool,
}
impl std::fmt::Debug for LoadingAuthorReturn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadingAuthorReturn")
            .field("author", &self.author)
            .finish_non_exhaustive()
    }
}
impl LoadingAuthorReturn {
    pub(crate) fn binding(&self) -> &CausalAuthorBinding {
        &self.events.0.bindings[self.author]
    }
    pub(crate) fn belongs_to(&self, events: &LoadingEvents) -> bool {
        Arc::ptr_eq(&self.events.0, &events.0)
    }
    /// Only the actual matching Causal owner calls this after recording the
    /// capture leg. Taking or dropping an unconsumed token never supplies it.
    pub(crate) fn disarm(mut self) {
        {
            let mut state = self
                .events
                .0
                .state
                .lock()
                .expect("original loading author return");
            assert_eq!(
                state.returns[self.author],
                ReturnPhase::Issued,
                "one observation of the original return"
            );
            state.returns[self.author] = ReturnPhase::Observed;
        }
        self.armed = false;
        self.events.release_retention();
    }
}
impl Drop for LoadingAuthorReturn {
    fn drop(&mut self) {
        if self.armed {
            self.events
                .0
                .state
                .lock()
                .expect("original loading author return")
                .returns[self.author] = ReturnPhase::Dropped;
            self.events.note_fault(Fault::Unresolved);
        }
    }
}
#[must_use = "The outcome and its unique original author return have different consumers"]
pub(crate) struct LoadingJobReturned {
    origin: LoadingEvents,
    outcome: LoadingEventOutcome,
    author_return: Option<LoadingAuthorReturn>,
}
struct AuthorSending {
    events: LoadingEvents,
    active: bool,
}
impl Drop for AuthorSending {
    fn drop(&mut self) {
        if self.active {
            self.events.note_fault(Fault::Unresolved);
        }
    }
}
impl std::fmt::Debug for LoadingJobReturned {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadingJobReturned")
            .field("outcome", &self.outcome)
            .field("has_author_return", &self.author_return.is_some())
            .finish_non_exhaustive()
    }
}
impl LoadingJobReturned {
    pub(crate) fn belongs_to(&self, events: &LoadingEvents) -> bool {
        Arc::ptr_eq(&self.origin.0, &events.0)
    }
    pub(crate) const fn outcome(&self) -> LoadingEventOutcome {
        self.outcome
    }
    /// The owning Task checks origin before this call, even when no positive
    /// token exists. A refusal preserves this same envelope and original token.
    pub(crate) fn try_observe_with<E>(
        mut self,
        observer: impl FnOnce(LoadingAuthorReturn) -> Result<(), (E, LoadingAuthorReturn)>,
    ) -> Result<LoadingEventOutcome, (E, Self)> {
        if let Some(receipt) = self.author_return.take()
            && let Err((error, receipt)) = observer(receipt)
        {
            self.author_return = Some(receipt);
            return Err((error, self));
        }
        Ok(self.outcome)
    }
    /// Primitive access for an already matched owner or component tests.
    /// Service routing retains the complete envelope on every refusal.
    pub(crate) fn into_parts(self) -> (LoadingEventOutcome, Option<LoadingAuthorReturn>) {
        (self.outcome, self.author_return)
    }
}
impl LoadingEvents {
    /// Wake the original custodian after its paid host-return cell changes.
    /// This notification supplies no author, Work, report or source exit.
    pub(crate) fn notify_pending(&self) {
        self.0.loader.notify_pending();
    }
    /// The trusted Task issuer has paid and admitted both original authors,
    /// bound them to this exact loader Work and kept the same source FIFO.
    /// Validate before retaining a job; constructor failure is an invariant
    /// failure, never unused revocation of an already captured Task.
    pub(crate) fn from_admitted(
        task: TaskIdentity,
        loader: LoaderCustody,
        loaded: (CutId, TaskCausalSender),
        exited: (CutId, TaskCausalSender),
        inbox: &TaskCausalInbox,
    ) -> Self {
        assert_eq!(
            loader.identity(),
            task,
            "the original loading Task identity"
        );
        assert_ne!(loaded.0, exited.0, "two distinct original loading authors");
        let bindings = [&loaded, &exited].map(|(cut, sender)| {
            inbox
                .author_binding(task, *cut, loader.work_id(), sender)
                .expect("the original loading sender and Work belong to this Inbox")
        });
        let custody = Arc::new(Custody {
            task,
            loader,
            bindings,
            issued: AtomicBool::new(false),
            state: Mutex::new(State {
                senders: [Some(loaded.1), Some(exited.1)],
                phases: [AuthorPhase::Waiting; 2],
                returns: [ReturnPhase::Waiting; 2],
                rejected: [None, None],
                fault: None,
            }),
            retained: Mutex::new(None),
        });
        *custody
            .retained
            .lock()
            .expect("original loading event retention") = Some(custody.clone());
        Self(custody)
    }
    pub(crate) fn wrap_job(&self, job: LoaderJob) -> Result<LoadingEventJob, LoaderJob> {
        if !self.0.loader.owns_job(&job)
            || self
                .0
                .issued
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            return Err(job);
        }
        Ok(LoadingEventJob {
            job: Some(job),
            events: self.clone(),
            active: true,
        })
    }
    fn take_sender(&self, index: usize) -> Option<TaskCausalSender> {
        let mut state = self.0.state.lock().expect("original loading event custody");
        if state.phases[index] != AuthorPhase::Waiting {
            return None;
        }
        state.phases[index] = AuthorPhase::Sending;
        Some(
            state.senders[index]
                .take()
                .expect("the original loading sender"),
        )
    }
    fn send(&self, index: usize, input: ContextInput) -> bool {
        let sender = self
            .take_sender(index)
            .expect("one handoff per original author");
        self.send_taken(index, sender, input)
    }
    fn send_taken(&self, index: usize, sender: TaskCausalSender, input: ContextInput) -> bool {
        let mut sending = AuthorSending {
            events: self.clone(),
            active: true,
        };
        // This can wake the host and must never run under our metadata lock.
        let rejected = sender.send_input(input).err();
        let sent = rejected.is_none();
        {
            let mut state = self.0.state.lock().expect("original loading event custody");
            state.phases[index] = if sent {
                AuthorPhase::Sent
            } else {
                AuthorPhase::Rejected
            };
            state.rejected[index] = rejected.map(ManuallyDrop::new);
        }
        // A fast consumer could have observed Sending after the sender's
        // wake but before this install. Publish the final phase, then wake.
        self.0.loader.notify_pending();
        sending.active = false;
        sent
    }
    fn outcome(&self, outcome: LoaderOutcome) -> LoadingEventOutcome {
        match outcome {
            LoaderOutcome::Measured(result) => {
                if self.send(
                    LOADED,
                    ContextInput::Loaded {
                        task: self.0.task,
                        result,
                    },
                ) {
                    LoadingEventOutcome::Loaded
                } else {
                    LoadingEventOutcome::Rejected
                }
            }
            LoaderOutcome::Canceled => {
                // The actual canceled measurement did not produce a cost.
                // Core's current close state selects its own exact outcome,
                // including LeaseExpired; no successful measurement is guessed.
                if self.send(
                    LOADED,
                    ContextInput::Loaded {
                        task: self.0.task,
                        result: Err(AdmissionRefusal::Verdict(
                            OperationOutcome::ContextTerminalReceipt,
                        )),
                    },
                ) {
                    LoadingEventOutcome::Canceled
                } else {
                    LoadingEventOutcome::Rejected
                }
            }
            LoaderOutcome::StartRefused(error) => {
                self.note_fault(Fault::StartRefused(error));
                LoadingEventOutcome::StartRefused(error)
            }
            LoaderOutcome::Unresolved => {
                self.note_fault(Fault::Unresolved);
                LoadingEventOutcome::Unresolved
            }
        }
    }
    fn note_fault(&self, fault: Fault) {
        self.0
            .state
            .lock()
            .expect("original loading event custody")
            .fault = Some(fault);
        self.0.loader.notify_pending();
    }
    fn known_fault(&self) -> Result<(), LoadingEventError> {
        let state = self.0.state.lock().expect("original loading event custody");
        if let Some(fault) = state.fault {
            return Err(match fault {
                Fault::StartRefused(error) => LoadingEventError::StartRefused(error),
                Fault::Unresolved => LoadingEventError::Unresolved,
            });
        }
        if state.phases[LOADED] == AuthorPhase::Rejected {
            return Err(LoadingEventError::LoadedRejected);
        }
        if state.phases[EXITED] == AuthorPhase::Rejected {
            return Err(LoadingEventError::ExitRejected);
        }
        Ok(())
    }
    fn author_return(&self, author: usize) -> LoadingAuthorReturn {
        {
            let mut state = self.0.state.lock().expect("original loading author return");
            assert_eq!(
                state.returns[author],
                ReturnPhase::Waiting,
                "one token from one actual author return"
            );
            state.returns[author] = ReturnPhase::Issued;
        }
        LoadingAuthorReturn {
            events: self.clone(),
            author,
            armed: true,
        }
    }
    /// Actual whole exit proves Host2. Sending the one original exit event is
    /// a separate operation from its Core consumption: return its unique author
    /// token even when that actual late send was refused. A later report error
    /// cannot swallow a completed author's capture proof.
    pub(crate) fn publish_ready_exit(
        &self,
    ) -> Result<Option<LoadingAuthorReturn>, LoadingEventError> {
        let phase = self.0.loader.phase();
        let ready = self
            .0
            .loader
            .ready_for_exit()
            .map_err(LoadingEventError::Role)?;
        if ready && phase != ExitRolePhase::Retired {
            let observer = ConsumerId::new(2).unwrap();
            if self
                .0
                .loader
                .report()
                .consumption(observer)
                .map_err(LoadingEventError::Report)?
                == Consumption::Pending
            {
                self.0
                    .loader
                    .report()
                    .consume(observer)
                    .map_err(LoadingEventError::Report)?;
            }
        }
        self.known_fault()?;
        if !ready || phase == ExitRolePhase::Retired {
            return Ok(None);
        }
        let (loaded, exit) = {
            let state = self.0.state.lock().expect("original loading event custody");
            (state.phases[LOADED], state.phases[EXITED])
        };
        // The loader's exit wake may race its wrapper installing the measured
        // result. Installing that result wakes again, without an invented fact.
        if loaded != AuthorPhase::Sent || exit != AuthorPhase::Waiting {
            return Ok(None);
        }
        let Some(sender) = self.take_sender(EXITED) else {
            return Ok(None);
        };
        self.send_taken(
            EXITED,
            sender,
            ContextInput::Physical {
                task: self.0.task,
                fact: PhysicalFact::LoaderExited,
            },
        );
        Ok(Some(self.author_return(EXITED)))
    }
    /// Only the same original exit event's committed input can consume Core1.
    /// Publication/capture return has already been handed to its own caller;
    /// neither a Core lookup error nor report retirement can consume that token.
    pub(crate) fn observe_exit_report(
        &self,
        inbox: &mut TaskCausalInbox,
    ) -> Result<bool, LoadingEventError> {
        self.known_fault()?;
        if self.0.loader.phase() == ExitRolePhase::Retired {
            self.release_retention();
            return Ok(self.is_retired());
        }
        let sent = self
            .0
            .state
            .lock()
            .expect("original loading event custody")
            .phases[EXITED]
            == AuthorPhase::Sent;
        if !sent {
            return Ok(false);
        }
        let core_consumed = inbox
            .bound_author_report_consumed(&self.0.bindings[EXITED])
            .map_err(LoadingEventError::Causal)?;
        let core = ConsumerId::new(1).unwrap();
        if core_consumed
            && self
                .0
                .loader
                .report()
                .consumption(core)
                .map_err(LoadingEventError::Report)?
                == Consumption::Pending
        {
            self.0
                .loader
                .report()
                .consume(core)
                .map_err(LoadingEventError::Report)?;
        }
        self.0
            .loader
            .try_retire()
            .map_err(LoadingEventError::Role)?;
        self.release_retention();
        Ok(self.is_retired())
    }
    /// Transfers the precise refused neutral values to the real failure
    /// custodian. This grants neither Core consumption nor physical exit and
    /// deliberately leaves the original author phases Rejected.
    pub(crate) fn take_rejected(&self) -> [Option<ContextInput>; 2] {
        let mut state = self.0.state.lock().expect("original loading event custody");
        std::array::from_fn(|index| state.rejected[index].take().map(ManuallyDrop::into_inner))
    }
    pub(crate) fn is_retired(&self) -> bool {
        if self.0.loader.phase() != ExitRolePhase::Retired {
            return false;
        }
        let state = self.0.state.lock().expect("original loading event custody");
        state.fault.is_none()
            && state
                .returns
                .iter()
                .all(|phase| *phase == ReturnPhase::Observed)
            && state.phases.iter().all(|phase| *phase == AuthorPhase::Sent)
            && state.senders.iter().all(Option::is_none)
            && state.rejected.iter().all(Option::is_none)
    }
    fn release_retention(&self) {
        if self.is_retired() {
            let original = self
                .0
                .retained
                .lock()
                .expect("original loading event retention")
                .take();
            drop(original);
        }
    }
}
impl LoadingEventJob {
    pub(crate) fn report_slot(&self) -> &ReportSlot {
        self.events.0.loader.report()
    }
    pub(crate) fn owner_notice(&self) -> LoadingEvents {
        self.events.clone()
    }
    pub(crate) fn run(mut self) -> LoadingJobReturned {
        let outcome = self.job.take().expect("one original loading job").run();
        let outcome = self.events.outcome(outcome);
        let author_return = match outcome {
            LoadingEventOutcome::Loaded
            | LoadingEventOutcome::Canceled
            | LoadingEventOutcome::Rejected => Some(self.events.author_return(LOADED)),
            LoadingEventOutcome::StartRefused(_) | LoadingEventOutcome::Unresolved => None,
        };
        self.active = false;
        LoadingJobReturned {
            origin: self.events.clone(),
            outcome,
            author_return,
        }
    }
}

impl QueuedPreparation for LoadingEventJob {
    fn queue_key(&self) -> (ScopeId, WorkId) {
        (
            self.events.0.loader.scope_id(),
            self.events.0.loader.work_id(),
        )
    }
}
impl Drop for LoadingEventJob {
    fn drop(&mut self) {
        if self.active {
            self.events.note_fault(Fault::Unresolved);
            // The actual LoaderJob Drop retains its original unresolved Work.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_runtime::{
        actors::{
            WorkerAction,
            tests::{Fixture, actor, finish_source_routes, generation, operation, task},
        },
        causal::{CausalSourceRetirement, TaskCausalInbox},
        effect_dispatch::EffectDispatch,
        loading::TaskCreatePayload,
        preparation::{
            PREPARATION_CORE_CONSUMER, PREPARATION_OBSERVER_CONSUMER, PreparationOutcome,
        },
        task::TaskHostRecord,
        task_publication::commit_task_projection,
    };
    use crate::task_proposal_ingress::RetiredCut;
    use novarocks_control_runtime::driver::{DriverError, InputPhase};
    use novarocks_control_runtime_core::{
        identity::{ActorId, InputId, Tick},
        mailbox::InputClass,
    };
    use novarocks_execution_contract::{
        AbortCause, CreateTask, CreationContent, ExchangeTopology, FrozenBytes, TaskCreationInput,
        TaskDescriptor,
    };
    use novarocks_types::UniqueId;
    use novarocks_worker_protocol::context::ContextCommand;
    use novarocks_worker_protocol::{context::ContextEffect, creation::PayloadId};
    use std::{any::Any, num::NonZeroUsize, sync::atomic::AtomicUsize};

    #[derive(Debug)]
    struct Input {
        getters: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
        panic: bool,
    }
    impl CreationContent for Input {
        fn encoded_len(&self) -> usize {
            1
        }
        fn retained_bytes(&self) -> usize {
            self.getters.fetch_add(1, Ordering::SeqCst);
            assert!(!self.panic, "original loader getter panic");
            1
        }
        fn into_stored(self: Box<Self>) -> Box<dyn Any + Send> {
            self
        }
    }
    impl Drop for Input {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn setup(
        panic: bool,
    ) -> (
        Fixture,
        TaskHostRecord,
        LoadingEventJob,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
    ) {
        let mut f = Fixture::with_policy_byte_limits(4096);
        let identity = task(f.identity, 1);
        let mut host = f.adopt_task_host(identity, 4);
        let descriptor = TaskDescriptor::try_new(
            identity,
            UniqueId::new(1, 1),
            NonZeroUsize::new(1).unwrap(),
            vec![],
            ExchangeTopology::default(),
        )
        .unwrap();
        let request =
            Arc::new(CreateTask::try_new(operation(1), f.identity, descriptor, vec![]).unwrap());
        let getters = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let payload = TaskCreatePayload {
            id: PayloadId(1),
            request,
            input: TaskCreationInput::new(
                FrozenBytes::freeze(bytes::Bytes::from_static(b"plan")),
                Box::new(Input {
                    getters: getters.clone(),
                    drops: drops.clone(),
                    panic,
                }),
            ),
            ingress_retained_bytes: 1,
        };
        let job = host
            .begin_causal_loader(payload)
            .unwrap_or_else(|_| panic!("the original paid loading authors"));
        assert_eq!(
            f.runtime.role_inventory(actor(), None, 32).unwrap().len(),
            4,
            "two real causal authors, each with its original prefix and event input"
        );
        let loader = host.loader().unwrap();
        assert_eq!(job.queue_key(), (loader.scope_id(), loader.work_id()));
        (f, host, job, getters, drops)
    }
    fn world(f: &mut Fixture, command: ContextInput) -> Vec<WorkerAction> {
        let input = f.send(command, None, InputClass::Lifecycle, 2);
        committed_world_input(f, input)
    }
    fn committed_world_input(f: &mut Fixture, input: InputId) -> Vec<WorkerAction> {
        f.runtime.poll_one(Tick(2)).unwrap();
        assert_eq!(
            f.runtime
                .input_custody(actor(), input)
                .unwrap()
                .unwrap()
                .phase,
            InputPhase::Committed
        );
        f.drain(input)
    }
    fn dispose_pair(f: &Fixture, host: &TaskHostRecord, retired: RetiredCut) {
        let ingress = host.port().ingress().clone();
        for holder in ingress
            .take_unused_owned_pair(&retired.pair)
            .unwrap()
            .into_iter()
            .flatten()
        {
            holder.exit();
        }
        for position in ingress.owned_pair_inventory(&retired.pair).unwrap() {
            assert!(
                position.source.is_none(),
                "no fabricated source Core receipt"
            );
            assert!(position.custody.actual_exit_ready().unwrap());
            assert!(position.custody.try_retire().unwrap());
        }
        f.ledger.reap_scope_ready(f.scope.id(), 64).unwrap();
        let reaped = ingress
            .prepare_owned_pair_reap(&retired.pair)
            .unwrap()
            .map(|probe| probe.observe_reaped().unwrap());
        let detached = ingress.release_owned_pair(&retired.pair, reaped).unwrap();
        for mut storage in detached {
            storage.release().unwrap();
        }
        drop(retired);
    }
    fn retire(f: &Fixture, host: &mut TaskHostRecord, cut: CutId) {
        let ingress = host.port().ingress().clone();
        assert!(
            host.causal().observe_effects_exited(cut).unwrap(),
            "only actual retirement of this original input proves its effects exit"
        );
        ingress.consume_cut(cut, 0).unwrap();
        let retired = host
            .causal()
            .retire_after_exit(cut, CausalSourceRetirement::Unclaimed)
            .unwrap_or_else(|_| panic!("the actual neutral batch and its author exited"));
        dispose_pair(f, host, retired);
        assert_eq!(
            host.causal().observe_effects_exited(cut),
            Err(CausalError::UnknownRole),
            "a removed original role is an error, never inferred exit"
        );
    }
    fn finish(f: Fixture, host: TaskHostRecord) {
        let mut resources = host.into_resources();
        assert!(resources.causal.is_empty());
        assert!(resources.loading_events.as_ref().unwrap().is_retired());
        assert!(resources.loader.as_ref().unwrap().is_retired());
        let ingress = resources.port.ingress().clone();
        finish_source_routes(&f, &ingress);
        // The closed original routes cannot issue another lifecycle Source.
        // Return only the Accepted pool's still-unbound original remainder.
        if let Some(mut storage) = resources.accepted_source_storage.take() {
            storage.release(&storage.resources().unwrap()).unwrap();
        }
        assert!(resources.source_partials.is_empty());
        assert!(resources.pending_preparation_adoption.is_none());
        crate::control_runtime::task::retire_unused_host_calls_for_test(
            &resources.host_calls,
            &f.ledger,
        );
        resources.lifetime.body_exited().unwrap();
        resources.lifetime.captures_exited().unwrap();
        assert!(resources.lifetime.actual_exit_ready().unwrap());
        if let Some(mut remainder) = resources.preparation_remainder.take() {
            remainder.release(&remainder.resources().unwrap()).unwrap();
        }
        resources
            .active
            .release(&resources.active.resources().unwrap())
            .unwrap();
        f.ledger.reap_scope_ready(f.scope.id(), 64).unwrap();
        drop(resources);
        drop(ingress);
        f.finish();
    }
    fn finish_pristine(f: Fixture, host: TaskHostRecord) {
        let mut resources = host.into_resources();
        assert!(resources.loading_events.is_none());
        assert!(resources.loader.is_none());
        assert!(resources.causal.is_empty());
        finish_source_routes(&f, resources.port.ingress());
        crate::control_runtime::task::retire_unused_host_calls_for_test(
            &resources.host_calls,
            &f.ledger,
        );
        resources.lifetime.body_exited().unwrap();
        resources.lifetime.captures_exited().unwrap();
        assert!(resources.lifetime.actual_exit_ready().unwrap());
        resources
            .active
            .release(&resources.active.resources().unwrap())
            .unwrap();
        f.ledger.reap_scope_ready(f.scope.id(), 64).unwrap();
        drop(resources);
        f.finish();
    }
    fn cancel_from_core(f: &mut Fixture, host: &TaskHostRecord) {
        let input = f.send(
            ContextInput::Request(ContextCommand::Abort {
                cause: AbortCause::LeaseExpired,
            }),
            Some(operation(7)),
            InputClass::Control,
            2,
        );
        let outputs = committed_world_input(f, input);
        assert!(
            outputs
                .iter()
                .any(|effect| matches!(effect, WorkerAction::Context(ContextEffect::WakeInternal)))
        );
        let outputs = world(f, ContextInput::NextInternal);
        let epoch = outputs
            .iter()
            .find_map(|effect| match effect {
                WorkerAction::Context(ContextEffect::ApplyTaskClose { generation, .. }) => {
                    Some(*generation)
                }
                _ => None,
            })
            .unwrap();
        let outputs = world(
            f,
            ContextInput::ApplyTaskClose {
                task: host.port().identity(),
                generation: epoch,
            },
        );
        assert!(outputs.iter().any(|effect| matches!(
            effect,
            WorkerAction::Context(ContextEffect::CancelLoader { .. })
        )));
        host.loader().unwrap().cancel();
    }
    fn commit_exit(f: &mut Fixture, host: &mut TaskHostRecord) {
        let returned = host
            .publish_loading_exit()
            .unwrap()
            .expect("the original Exited author returned");
        assert!(
            host.publish_loading_exit().unwrap().is_none(),
            "a cloned owner cannot mint a second return token"
        );
        assert!(
            !host.observe_loading_exit_report().unwrap(),
            "the original exit event needs its own Core step"
        );
        let loader = host.loader().unwrap().clone();
        assert_eq!(
            loader
                .report()
                .consumption(ConsumerId::new(2).unwrap())
                .unwrap(),
            Consumption::Consumed
        );
        assert_eq!(
            loader
                .report()
                .consumption(ConsumerId::new(1).unwrap())
                .unwrap(),
            Consumption::Pending
        );
        // A later report lookup failure cannot consume or swallow this
        // already-returned author's token. The same token remains usable.
        let mut other_fixture = Fixture::with_policy_byte_limits(4096);
        let other_task = task(other_fixture.identity, 1);
        let mut other_host = other_fixture.adopt_task_host(other_task, 4);
        assert_eq!(
            returned.events.observe_exit_report(other_host.causal()),
            Err(LoadingEventError::Causal(CausalError::WrongTask))
        );
        assert_eq!(
            returned.events.0.state.lock().unwrap().returns[EXITED],
            ReturnPhase::Issued
        );
        assert_eq!(
            loader
                .report()
                .consumption(ConsumerId::new(1).unwrap())
                .unwrap(),
            Consumption::Pending
        );
        let mut wrong_inbox = TaskCausalInbox::new(
            other_fixture.runtime.clone(),
            actor(),
            ActorId::new(2).unwrap(),
            returned.events.0.task,
            generation(),
            other_host.port().ingress().clone(),
            other_host.port().health(),
        );
        assert_eq!(
            returned.events.observe_exit_report(&mut wrong_inbox),
            Err(LoadingEventError::Causal(CausalError::WrongTask)),
            "equal Task metadata cannot substitute a foreign bank's Core receipt"
        );
        assert_eq!(
            loader
                .report()
                .consumption(ConsumerId::new(1).unwrap())
                .unwrap(),
            Consumption::Pending
        );
        drop(wrong_inbox);
        finish_pristine(other_fixture, other_host);
        let exit = host.causal().deliver_next().unwrap().unwrap();
        assert!(exit.prefix.is_none());
        // Keep this real return unconsumed while Core and effects progress.
        // Merely holding the token grants no capture observation to the cut.
        let ingress = host.port().ingress().clone();
        assert!(
            !host.causal().observe_effects_exited(exit.cut).unwrap(),
            "Filled is not effect exit"
        );
        f.runtime.poll_one(Tick(2)).unwrap();
        assert_eq!(
            host.causal().observe_core(exit.cut).unwrap(),
            Consumption::Consumed
        );
        assert!(
            !host.causal().observe_effects_exited(exit.cut).unwrap(),
            "Committed input is still in flight until its batch really retires"
        );
        f.drain(exit.event);
        assert!(
            f.runtime
                .input_custody(actor(), exit.event)
                .unwrap()
                .is_none()
        );
        assert!(host.causal().observe_effects_exited(exit.cut).unwrap());
        ingress.consume_cut(exit.cut, 0).unwrap();
        let refusal = host
            .causal()
            .retire_after_exit(exit.cut, CausalSourceRetirement::Unclaimed)
            .err()
            .expect("the original exit receipt stays until the loader report really retires");
        assert_eq!(refusal.error, CausalError::Driver(DriverError::RoleBusy));
        assert!(
            !host.observe_loading_exit_report().unwrap(),
            "report retirement alone cannot consume the held author token"
        );
        assert_eq!(loader.phase(), ExitRolePhase::Retired);
        let missing_return = host
            .causal()
            .retire_after_exit(exit.cut, refusal.retirement)
            .err()
            .expect("the exact Core receipt and effects exit cannot substitute for author return");
        assert_eq!(
            missing_return.error,
            CausalError::Cut(novarocks_worker_protocol::proposal_summary::CutError::ExitUnobserved)
        );
        // The original event/input is already Committed and retired. Its
        // phase-independent binding must still accept this same late token.
        host.observe_loading_author_return(returned).unwrap();
        assert!(host.observe_loading_exit_report().unwrap());
        assert!(
            host.observe_loading_exit_report().unwrap(),
            "retry cannot reread a removed ReportSlot"
        );
        let retired = host
            .causal()
            .retire_after_exit(exit.cut, missing_return.retirement)
            .unwrap_or_else(|_| panic!("same original pair after report ACK and author return"));
        dispose_pair(f, host, retired);
        assert_eq!(
            host.causal().observe_effects_exited(exit.cut),
            Err(CausalError::UnknownRole)
        );
    }
    #[test]
    fn measured_loaded_commits_before_input_transfer_then_exact_exit_receipt_survives_input_retirement()
     {
        let (mut f, mut host, job, getters, drops) = setup(false);
        let loaded_cut = job.events.0.bindings[LOADED].cut();
        let exit_cut = job.events.0.bindings[EXITED].cut();
        assert!(
            !host.causal().observe_effects_exited(loaded_cut).unwrap(),
            "an initially idle but unfilled author has no exit proof"
        );
        assert!(!host.causal().observe_effects_exited(exit_cut).unwrap());
        let (outcome, returned) = job.run().into_parts();
        assert_eq!(outcome, LoadingEventOutcome::Loaded);
        let loaded_return = returned.expect("the original Loaded author returned");
        assert!(
            !host.causal().observe_effects_exited(loaded_cut).unwrap(),
            "the returned author's registered event has not transferred yet"
        );
        assert_eq!(getters.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert!(!host.loader().unwrap().ready_for_exit().unwrap());
        assert!(
            !host.observe_loading_exit().unwrap(),
            "Loaded must not wait on the child that Accepted will take"
        );
        let loaded = host.causal().deliver_next().unwrap().unwrap();
        assert!(loaded.prefix.is_none());
        assert_eq!(loaded.cut, loaded_cut);
        assert!(
            !host.causal().observe_effects_exited(loaded.cut).unwrap(),
            "Filled has neither executed nor retired the actual batch"
        );
        f.runtime.poll_one(Tick(2)).unwrap();
        assert_eq!(
            host.causal().observe_core(loaded.cut).unwrap(),
            Consumption::Consumed
        );
        assert!(!host.loader().unwrap().ready_for_exit().unwrap());
        assert_eq!(
            f.runtime
                .input_custody(actor(), loaded.event)
                .unwrap()
                .unwrap()
                .phase,
            InputPhase::Committed
        );
        assert!(
            !host.causal().observe_effects_exited(loaded.cut).unwrap(),
            "Core commit does not complete publication, enqueue, or reply effects"
        );
        let premature = host
            .causal()
            .retire_after_exit(loaded.cut, CausalSourceRetirement::Unclaimed)
            .err()
            .expect("the original live batch cannot retire its causal role");
        assert_eq!(premature.error, CausalError::Driver(DriverError::RoleBusy));
        let mut dispatch = EffectDispatch::new(actor(), 15);
        dispatch.index_committed(&f.runtime, 32).unwrap();
        let publication = dispatch.take_ready(&f.runtime, 15).unwrap().unwrap();
        assert!(matches!(
            publication.effect,
            WorkerAction::Context(ContextEffect::PublishTask { .. })
        ));
        assert!(
            !host.causal().observe_effects_exited(loaded.cut).unwrap(),
            "the started actual publication effect still owns this batch"
        );
        drop(commit_task_projection(&f.runtime, &publication, &host.port()).unwrap());
        dispatch
            .publication_committed(&f.runtime, &publication)
            .unwrap();
        let preparation = dispatch.take_ready(&f.runtime, 15).unwrap().unwrap();
        let job = host.adopt_preparation(&f.runtime, &preparation).unwrap();
        assert!(
            !host.causal().observe_effects_exited(loaded.cut).unwrap(),
            "original reservation transfer alone does not exit enqueue/publication/reply effects"
        );
        f.runtime.effect_completed(preparation).unwrap();
        f.runtime.effect_completed(publication).unwrap();
        f.drain(loaded.event);
        assert!(
            f.runtime
                .input_custody(actor(), loaded.event)
                .unwrap()
                .is_none()
        );
        assert!(host.causal().observe_effects_exited(loaded.cut).unwrap());
        host.port().ingress().consume_cut(loaded.cut, 0).unwrap();
        let missing_return = host
            .causal()
            .retire_after_exit(loaded.cut, CausalSourceRetirement::Unclaimed)
            .err()
            .expect("a held return token does not mark original capture exit");
        assert_eq!(
            missing_return.error,
            CausalError::Cut(novarocks_worker_protocol::proposal_summary::CutError::ExitUnobserved)
        );
        host.observe_loading_author_return(loaded_return).unwrap();
        // The original host consumer was already consumed above. Preserve the
        // same refusal inventory; do not acknowledge that consumer twice.
        let retired = host
            .causal()
            .retire_after_exit(loaded.cut, missing_return.retirement)
            .unwrap_or_else(|_| panic!("the same Loaded pair after its actual author return"));
        dispose_pair(&f, &host, retired); // no loader/report hold on InputLoaded
        assert_eq!(
            host.causal().observe_effects_exited(loaded.cut),
            Err(CausalError::UnknownRole)
        );
        assert!(host.loader().unwrap().ready_for_exit().unwrap());
        commit_exit(&mut f, &mut host);
        assert!(matches!(
            job.run(|_, input, _| drop(input)),
            PreparationOutcome::Returned(())
        ));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        let prep = host.preparation().unwrap().clone();
        assert!(prep.publish_whole_exit().unwrap());
        // These are observed facts from the original actual preparation above,
        // not a fake Native install. Their exact Core inputs commit separately.
        world(
            &mut f,
            ContextInput::Physical {
                task: host.port().identity(),
                fact: PhysicalFact::PreparationBodyExited,
            },
        );
        prep.body_report()
            .consume(PREPARATION_CORE_CONSUMER)
            .unwrap();
        prep.body_report()
            .consume(PREPARATION_OBSERVER_CONSUMER)
            .unwrap();
        world(
            &mut f,
            ContextInput::Physical {
                task: host.port().identity(),
                fact: PhysicalFact::PreparationExited,
            },
        );
        prep.exit_report()
            .consume(PREPARATION_CORE_CONSUMER)
            .unwrap();
        prep.exit_report()
            .consume(PREPARATION_OBSERVER_CONSUMER)
            .unwrap();
        assert!(host.collect_preparation_remainder().unwrap());
        f.ledger.reap_scope_ready(f.scope.id(), 64).unwrap();
        finish(f, host);
    }
    #[test]
    fn core_cancel_still_gets_a_rejected_loaded_and_actual_disposal_precedes_loader_exited() {
        let (mut f, mut host, job, getters, drops) = setup(false);
        cancel_from_core(&mut f, &host);
        let returned = job.run();
        assert_eq!(returned.outcome(), LoadingEventOutcome::Canceled);
        let origin = returned.origin.clone();
        let original_cut = returned
            .author_return
            .as_ref()
            .expect("the original canceled author returned")
            .binding()
            .cut();
        let mut other_fixture = Fixture::with_policy_byte_limits(4096);
        let other_task = task(other_fixture.identity, 1);
        let mut other_host = other_fixture.adopt_task_host(other_task, 4);
        let (error, returned) = returned
            .try_observe_with(|receipt| other_host.observe_loading_author_return(receipt))
            .expect_err(
                "a different actual Task owner returns the original token to its same envelope",
            );
        assert!(matches!(
            error,
            crate::control_runtime::task::TaskHostError::WrongTaskOrPayload
        ));
        assert!(returned.belongs_to(&origin));
        assert_eq!(
            returned.author_return.as_ref().unwrap().binding().cut(),
            original_cut
        );
        // Deliberately wrong glue supplies the original Task metadata to an
        // Inbox over the second real paid owner's runtime/Ingress. No Work,
        // report or Core fact is created. The Weak origin must reject before
        // the (empty) role table; deleting that check would yield UnknownRole.
        let wrong_inbox = TaskCausalInbox::new(
            other_fixture.runtime.clone(),
            actor(),
            ActorId::new(2).unwrap(),
            origin.0.task,
            generation(),
            other_host.port().ingress().clone(),
            other_host.port().health(),
        );
        let (error, returned) = returned
            .try_observe_with(|receipt| wrong_inbox.observe_author_return(receipt))
            .expect_err("equal Task metadata cannot impersonate the original Ingress");
        assert_eq!(error, CausalError::WrongTask);
        assert!(returned.belongs_to(&origin));
        assert_eq!(
            returned.author_return.as_ref().unwrap().binding().cut(),
            original_cut
        );
        drop(wrong_inbox);
        assert_eq!(
            origin.0.state.lock().unwrap().returns[LOADED],
            ReturnPhase::Issued
        );
        assert!(origin.0.state.lock().unwrap().fault.is_none());
        assert_eq!(
            host.observe_loading_job_return(returned).unwrap(),
            LoadingEventOutcome::Canceled
        );
        finish_pristine(other_fixture, other_host);
        assert_eq!(getters.load(Ordering::SeqCst), 0);
        let loaded = host.causal().deliver_next().unwrap().unwrap();
        f.runtime.poll_one(Tick(2)).unwrap();
        assert_eq!(
            host.causal().observe_core(loaded.cut).unwrap(),
            Consumption::Consumed
        );
        let effects = f.drain(loaded.event);
        assert!(effects.iter().any(|effect| matches!(
            effect,
            WorkerAction::Reply(reply)
                if matches!(reply.body.as_ref(),
                    novarocks_worker_protocol::context::ContextReplyBody::Verdict(
                        novarocks_worker_protocol::context::RequestResult::Outcome(
                            OperationOutcome::LeaseExpired)))
        )));
        assert!(!host.observe_loading_exit().unwrap());
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert!(host.loader().unwrap().take_input_disposal().unwrap().run());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        f.ledger.reap_scope_ready(f.scope.id(), 64).unwrap();
        retire(&f, &mut host, loaded.cut);
        commit_exit(&mut f, &mut host);
        finish(f, host);
    }
    #[test]
    fn failed_real_start_keeps_both_original_authors_and_never_invents_loaded_or_exit() {
        let (f, mut host, job, getters, drops) = setup(false);
        f.scope.fail().unwrap();
        let returned = job.run();
        assert!(matches!(
            returned.outcome(),
            LoadingEventOutcome::StartRefused(_)
        ));
        assert!(
            returned.author_return.is_none(),
            "a refused start grants no normal author return"
        );
        let origin = returned.origin.clone();
        let mut other_fixture = Fixture::with_policy_byte_limits(4096);
        let other_task = task(other_fixture.identity, 1);
        let mut other_host = other_fixture.adopt_task_host(other_task, 4);
        let (error, returned) = other_host
            .observe_loading_job_return(returned)
            .expect_err("even a fault without a token belongs to one actual Task owner");
        assert!(matches!(
            error,
            crate::control_runtime::task::TaskHostError::WrongTaskOrPayload
        ));
        assert!(returned.belongs_to(&origin));
        assert!(returned.author_return.is_none());
        let outcome = host.observe_loading_job_return(returned).unwrap();
        assert!(matches!(outcome, LoadingEventOutcome::StartRefused(_)));
        finish_pristine(other_fixture, other_host);
        assert_eq!(getters.load(Ordering::SeqCst), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert!(host.causal().deliver_next().unwrap().is_none());
        assert!(matches!(
            host.observe_loading_exit(),
            Err(LoadingEventError::StartRefused(_))
        ));
        let resources = host.into_resources();
        let events = resources.loading_events.as_ref().unwrap();
        let state = events.0.state.lock().unwrap();
        assert!(state.senders.iter().all(Option::is_some));
        assert_eq!(state.phases, [AuthorPhase::Waiting; 2]);
        drop(state);
        assert!(!events.is_retired());
        // A failed scope is not a drained-publication token or a Core ACK.
        let _original = ManuallyDrop::new((f, resources));
    }
    #[test]
    fn actual_getter_panic_retains_original_input_and_authors_without_loader_exited() {
        let (f, mut host, job, getters, drops) = setup(true);
        let (outcome, returned) = job.run().into_parts();
        assert_eq!(outcome, LoadingEventOutcome::Unresolved);
        assert!(
            returned.is_none(),
            "unresolved author exit is not positive return"
        );
        assert_eq!(getters.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(
            host.observe_loading_exit(),
            Err(LoadingEventError::Unresolved)
        );
        assert_eq!(host.loader().unwrap().phase(), ExitRolePhase::Unresolved);
        assert!(host.loader().unwrap().take_input_disposal().is_none());
        assert!(host.causal().deliver_next().unwrap().is_none());
        let resources = host.into_resources();
        let events = resources.loading_events.as_ref().unwrap();
        let state = events.0.state.lock().unwrap();
        assert!(state.senders.iter().all(Option::is_some));
        assert_eq!(state.phases, [AuthorPhase::Waiting; 2]);
        drop(state);
        let _original = ManuallyDrop::new((f, resources));
    }
    #[test]
    fn late_loaded_after_original_author_revocation_preserves_the_same_refused_payload() {
        let (f, mut host, job, getters, drops) = setup(false);
        // Explicit invalid late-author ordering: revoke this exact original
        // cut before its own loader result attempts to register. A failed flag
        // alone is not treated as permission to discard a late admitted event.
        let loaded_cut = job.events.0.bindings[LOADED].cut();
        host.port().ingress().revoke_event(loaded_cut).unwrap();
        let (outcome, returned) = job.run().into_parts();
        assert_eq!(outcome, LoadingEventOutcome::Rejected);
        host.observe_loading_author_return(
            returned.expect("the actual refused send still returned"),
        )
        .unwrap();
        assert_eq!(getters.load(Ordering::SeqCst), 1);
        assert!(!host.loader().unwrap().ready_for_exit().unwrap());
        assert_eq!(
            host.observe_loading_exit(),
            Err(LoadingEventError::LoadedRejected)
        );
        let resources = host.into_resources();
        let events = resources.loading_events.as_ref().unwrap();
        let [loaded, exit] = events.take_rejected();
        assert!(exit.is_none());
        assert!(
            matches!(loaded, Some(ContextInput::Loaded { task, result: Ok(_) }) if task == resources.port.identity())
        );
        assert!(events.take_rejected().iter().all(Option::is_none));
        assert_eq!(
            events.0.state.lock().unwrap().phases[LOADED],
            AuthorPhase::Rejected
        );
        assert!(
            !events.is_retired(),
            "taking a neutral refusal is not a successful publication"
        );
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        // Original input, reports, authors and Source positions remain bounded
        // in this owner. This is failure custody, not successful cleanup.
        let _original = ManuallyDrop::new((f, resources));
    }
    #[test]
    fn dropping_an_unconsumed_author_return_keeps_the_original_capture_duty_unknown() {
        let (f, host, job, _, drops) = setup(false);
        let origin = job.events.clone();
        let (outcome, returned) = job.run().into_parts();
        assert_eq!(outcome, LoadingEventOutcome::Loaded);
        let returned = returned.expect("the original author really returned");
        assert_eq!(
            origin.0.state.lock().unwrap().returns[LOADED],
            ReturnPhase::Issued
        );
        drop(returned);
        assert_eq!(
            origin.0.state.lock().unwrap().returns[LOADED],
            ReturnPhase::Dropped
        );
        assert_eq!(
            origin.publish_ready_exit().err(),
            Some(LoadingEventError::Unresolved)
        );
        assert!(!origin.is_retired());
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        // No Causal consumer received this token. Dropping it supplied neither
        // capture exit nor normal cleanup; the same bounded owner awaits S8.
        let _original = ManuallyDrop::new((f, host, origin));
    }
    #[test]
    fn late_exit_rejection_returns_its_original_author_token_before_the_report_error() {
        let (mut f, mut host, job, getters, drops) = setup(false);
        let origin = job.events.clone();
        let exit_cut = origin.0.bindings[EXITED].cut();
        cancel_from_core(&mut f, &host);
        let returned = job.run();
        assert_eq!(
            host.observe_loading_job_return(returned).unwrap(),
            LoadingEventOutcome::Canceled
        );
        assert_eq!(getters.load(Ordering::SeqCst), 0);
        let loaded = host.causal().deliver_next().unwrap().unwrap();
        f.runtime.poll_one(Tick(2)).unwrap();
        assert_eq!(
            host.causal().observe_core(loaded.cut).unwrap(),
            Consumption::Consumed
        );
        f.drain(loaded.event);
        assert!(host.loader().unwrap().take_input_disposal().unwrap().run());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        f.ledger.reap_scope_ready(f.scope.id(), 64).unwrap();
        retire(&f, &mut host, loaded.cut);
        assert!(host.loader().unwrap().ready_for_exit().unwrap());
        // Explicit invalid late-author ordering, on this exact original cut.
        // A failed flag alone does not revoke already admitted late authors.
        host.port().ingress().revoke_event(exit_cut).unwrap();
        let returned = host
            .publish_loading_exit()
            .unwrap()
            .expect("the actual refused Exit call still returned");
        assert_eq!(returned.binding().cut(), exit_cut);
        assert_eq!(
            host.observe_loading_exit_report(),
            Err(LoadingEventError::ExitRejected)
        );
        assert_eq!(
            origin.0.state.lock().unwrap().returns[EXITED],
            ReturnPhase::Issued
        );
        assert_eq!(
            host.loader()
                .unwrap()
                .report()
                .consumption(ConsumerId::new(1).unwrap())
                .unwrap(),
            Consumption::Pending
        );
        host.observe_loading_author_return(returned).unwrap();
        assert_eq!(
            origin.0.state.lock().unwrap().returns[EXITED],
            ReturnPhase::Observed
        );
        assert_eq!(
            origin.0.state.lock().unwrap().phases[EXITED],
            AuthorPhase::Rejected
        );
        let [loaded, rejected_exit] = origin.take_rejected();
        assert!(loaded.is_none());
        assert!(
            matches!(rejected_exit, Some(ContextInput::Physical { task, fact: PhysicalFact::LoaderExited }) if task == host.port().identity())
        );
        assert_eq!(
            host.publish_loading_exit().err(),
            Some(LoadingEventError::ExitRejected)
        );
        assert!(!origin.is_retired());
        // Return observation does not turn rejected publication into Core
        // consumption. Retain the original bounded report/cut duties for S8.
        let _original = ManuallyDrop::new((f, host, origin));
    }
}
