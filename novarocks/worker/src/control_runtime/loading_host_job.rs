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

//! One real host runner publishes the loader's completed result into the same
//! original paid report. The return cell, author capture proof, physical host
//! slot, and Task/source exits remain separate facts.

use super::{
    loading_events::{LoadingEventJob, LoadingEventOutcome, LoadingJobReturned},
    preparation::QueuedPreparation,
};
use novarocks_control_runtime::owned_return::{OwnedReturnPublishRefusal, OwnedReturnSender};
use novarocks_control_runtime_core::identity::{ScopeId, WorkId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LoadingHostJobError {
    WrongReport,
}
#[must_use = "Both the original job and return sender remain owned on refusal"]
pub(crate) struct LoadingHostJobRefusal {
    pub error: LoadingHostJobError,
    pub job: LoadingEventJob,
    pub sender: OwnedReturnSender<LoadingJobReturned>,
}
impl std::fmt::Debug for LoadingHostJobRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadingHostJobRefusal")
            .field("error", &self.error)
            .field("sender", &self.sender)
            .finish_non_exhaustive()
    }
}
#[must_use = "The original host job must publish its result or retain unknown custody"]
pub(crate) struct LoadingHostJob {
    job: LoadingEventJob,
    sender: OwnedReturnSender<LoadingJobReturned>,
}
impl LoadingHostJob {
    /// The Task issuer claimed this same report's owned return while the
    /// original producer was bound, queued, and had captured no payload yet.
    /// Matching numeric IDs from another runtime cannot pass this check.
    pub(crate) fn from_admitted(
        job: LoadingEventJob,
        sender: OwnedReturnSender<LoadingJobReturned>,
    ) -> Result<Self, LoadingHostJobRefusal> {
        if !job.report_slot().same_slot(sender.report_slot()) {
            return Err(LoadingHostJobRefusal {
                error: LoadingHostJobError::WrongReport,
                job,
                sender,
            });
        }
        Ok(Self { job, sender })
    }
    /// No Service lock or mutable Core enters this host call. The complete
    /// author call returns before its same original envelope fills the paid
    /// report cell. The following wake is metadata, not another author exit.
    pub(crate) fn run(
        self,
    ) -> Result<LoadingEventOutcome, OwnedReturnPublishRefusal<LoadingJobReturned>> {
        let Self { job, sender } = self;
        let notice = job.owner_notice();
        let returned = job.run();
        let outcome = returned.outcome();
        sender.publish(returned)?;
        notice.notify_pending();
        Ok(outcome)
    }
    /// The physical executor publishes only into this original paid slot.
    /// Scope close/failure cannot reject that publication: the live sender's
    /// Reserved state prevents retirement. Losing the slot or its unique
    /// Reserved state is a shared custody invariant failure. Retain the whole
    /// refusal before fail-stop rather than unwinding opaque author captures.
    pub(crate) fn run_on_host(self) {
        if let Err(refusal) = self.run() {
            std::mem::forget(refusal);
            std::process::abort();
        }
    }
}
impl QueuedPreparation for LoadingHostJob {
    fn queue_key(&self) -> (ScopeId, WorkId) {
        self.job.queue_key()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_runtime::{
        actors::{
            WorkerAction,
            tests::{Fixture, actor, finish_source_routes, operation, task},
        },
        causal::CausalSourceRetirement,
        host_executor::{HostExecutor, HostExecutorLimits, HostSubmitError},
        loading::TaskCreatePayload,
        task::{LoadingSubmission, TaskHostRecord},
    };
    use novarocks_control_runtime::driver::InputPhase;
    use novarocks_control_runtime_core::{completion::ReturnState, identity::Tick};
    use novarocks_execution_contract::{
        CreateTask, CreationContent, ExchangeTopology, FrozenBytes, OperationOutcome,
        TaskCreationInput, TaskDescriptor,
    };
    use novarocks_types::UniqueId;
    use novarocks_worker_protocol::creation::PayloadId;
    use std::{
        any::Any,
        num::NonZeroUsize,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    #[derive(Debug)]
    struct Input {
        // This real backing exceeds the fixture's task-byte admission bound;
        // Core rejects the actual measured geometry without a fake cost.
        bytes: Vec<u8>,
        getters: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    }
    impl CreationContent for Input {
        fn encoded_len(&self) -> usize {
            self.bytes.len()
        }
        fn retained_bytes(&self) -> usize {
            self.getters.fetch_add(1, Ordering::SeqCst);
            self.bytes.len()
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
    fn setup_payload() -> (
        Fixture,
        TaskHostRecord,
        TaskCreatePayload,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
    ) {
        let mut f = Fixture::with_policy_byte_limits(4096);
        let task = task(f.identity, 1);
        let host = f.adopt_task_host(task, 4);
        let request = Arc::new(
            CreateTask::try_new(
                operation(1),
                f.identity,
                TaskDescriptor::try_new(
                    task,
                    UniqueId::new(1, 1),
                    NonZeroUsize::new(1).unwrap(),
                    vec![],
                    ExchangeTopology::default(),
                )
                .unwrap(),
                vec![],
            )
            .unwrap(),
        );
        let getters = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let payload = TaskCreatePayload {
            id: PayloadId(1),
            request,
            input: TaskCreationInput::new(
                FrozenBytes::freeze(bytes::Bytes::from_static(b"plan")),
                Box::new(Input {
                    bytes: vec![0; 8192],
                    getters: getters.clone(),
                    drops: drops.clone(),
                }),
            ),
            ingress_retained_bytes: 1,
        };
        (f, host, payload, getters, drops)
    }
    fn setup() -> (
        Fixture,
        TaskHostRecord,
        LoadingHostJob,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
    ) {
        let (f, mut host, payload, getters, drops) = setup_payload();
        let job = host
            .begin_host_loader(payload)
            .unwrap_or_else(|_| panic!("claim the original paid return before capture"));
        (f, host, job, getters, drops)
    }
    fn retire_cut(
        f: &Fixture,
        host: &mut TaskHostRecord,
        cut: novarocks_worker_protocol::proposal_summary::CutId,
    ) {
        assert!(host.causal().observe_effects_exited(cut).unwrap());
        let ingress = host.port().ingress().clone();
        ingress.consume_cut(cut, 0).unwrap();
        let retired = host
            .causal()
            .retire_after_exit(cut, CausalSourceRetirement::Unclaimed)
            .unwrap_or_else(|_| panic!("the exact original author and actual batch retired"));
        for holder in ingress
            .take_unused_owned_pair(&retired.pair)
            .unwrap()
            .into_iter()
            .flatten()
        {
            holder.exit();
        }
        for position in ingress.owned_pair_inventory(&retired.pair).unwrap() {
            assert!(position.source.is_none());
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
    fn complete_rejected(f: &mut Fixture, host: &mut TaskHostRecord) {
        let loaded = host.causal().deliver_next().unwrap().unwrap();
        assert!(loaded.prefix.is_none());
        f.runtime.poll_one(Tick(2)).unwrap();
        assert_eq!(
            f.runtime
                .input_custody(actor(), loaded.event)
                .unwrap()
                .unwrap()
                .phase,
            InputPhase::Committed
        );
        host.causal().observe_core(loaded.cut).unwrap();
        let outputs = f.drain(loaded.event);
        assert!(outputs.iter().any(|effect| matches!(
            effect,
            WorkerAction::Reply(reply)
                if matches!(reply.body.as_ref(),
                    novarocks_worker_protocol::context::ContextReplyBody::Verdict(
                        novarocks_worker_protocol::context::RequestResult::Outcome(
                            OperationOutcome::ResourceExhausted)))
        )));
        assert!(host.loader().unwrap().take_input_disposal().unwrap().run());
        f.ledger.reap_scope_ready(f.scope.id(), 64).unwrap();
        retire_cut(f, host, loaded.cut);
        let returned = host
            .publish_loading_exit()
            .unwrap()
            .expect("the original loader actually exited");
        host.observe_loading_author_return(returned).unwrap();
        let exit = host.causal().deliver_next().unwrap().unwrap();
        f.runtime.poll_one(Tick(2)).unwrap();
        host.causal().observe_core(exit.cut).unwrap();
        f.drain(exit.event);
        assert!(host.observe_loading_exit_report().unwrap());
        retire_cut(f, host, exit.cut);
    }
    fn finish(f: Fixture, host: TaskHostRecord) {
        let mut resources = host.into_resources();
        assert!(resources.causal.is_empty());
        assert!(resources.loader.as_ref().unwrap().is_retired());
        assert!(resources.loading_events.as_ref().unwrap().is_retired());
        let ingress = resources.port.ingress().clone();
        finish_source_routes(&f, &ingress);
        resources.lifetime.body_exited().unwrap();
        resources.lifetime.captures_exited().unwrap();
        assert!(resources.lifetime.actual_exit_ready().unwrap());
        resources
            .active
            .release(&resources.active.resources().unwrap())
            .unwrap();
        f.ledger.reap_scope_ready(f.scope.id(), 64).unwrap();
        drop(resources);
        drop(ingress);
        f.finish();
    }
    #[test]
    fn same_original_report_cell_is_empty_before_getter_and_filled_after_real_author_return() {
        let (mut f, mut host, job, getters, drops) = setup();
        let report = job.job.report_slot().clone();
        let key = job.job.queue_key();
        assert_eq!(job.queue_key(), key);
        assert_eq!(report.owned_return_state().unwrap(), ReturnState::Reserved);
        assert!(host.take_loading_return().unwrap().is_none());
        assert_eq!(getters.load(Ordering::SeqCst), 0);
        assert_eq!(job.run().unwrap(), LoadingEventOutcome::Loaded);
        assert_eq!(getters.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(report.owned_return_state().unwrap(), ReturnState::Filled);
        let returned = host
            .take_loading_return()
            .unwrap()
            .expect("the same paid cell owns the complete result");
        assert_eq!(report.owned_return_state().unwrap(), ReturnState::Taken);
        assert!(host.take_loading_return().unwrap().is_none());
        assert_eq!(
            host.observe_loading_job_return(returned).unwrap(),
            LoadingEventOutcome::Loaded
        );
        complete_rejected(&mut f, &mut host);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        finish(f, host);
    }
    #[test]
    fn equal_numeric_report_ids_from_another_real_runtime_refuse_without_losing_job_or_sender() {
        let (mut first_fixture, mut first_host, first, first_getters, first_drops) = setup();
        let (mut second_fixture, mut second_host, second, second_getters, second_drops) = setup();
        let LoadingHostJob {
            job: first_job,
            sender: first_sender,
        } = first;
        let LoadingHostJob {
            job: second_job,
            sender: second_sender,
        } = second;
        let first_report = first_job.report_slot().clone();
        let second_report = second_job.report_slot().clone();
        assert_eq!(
            first_report.id(),
            second_report.id(),
            "the numeric identity alone grants no authority"
        );
        assert!(!first_report.same_slot(&second_report));
        let refusal = LoadingHostJob::from_admitted(first_job, second_sender)
            .err()
            .expect("foreign report must refuse");
        assert_eq!(refusal.error, LoadingHostJobError::WrongReport);
        assert!(refusal.job.report_slot().same_slot(&first_report));
        assert!(refusal.sender.report_slot().same_slot(&second_report));
        assert_eq!(first_getters.load(Ordering::SeqCst), 0);
        assert_eq!(second_getters.load(Ordering::SeqCst), 0);
        assert_eq!(
            first_report.owned_return_state().unwrap(),
            ReturnState::Reserved
        );
        assert_eq!(
            second_report.owned_return_state().unwrap(),
            ReturnState::Reserved
        );
        let first = LoadingHostJob::from_admitted(refusal.job, first_sender)
            .unwrap_or_else(|_| panic!("same original first pair"));
        let second = LoadingHostJob::from_admitted(second_job, refusal.sender)
            .unwrap_or_else(|_| panic!("same original second pair"));
        assert_eq!(first.run().unwrap(), LoadingEventOutcome::Loaded);
        assert_eq!(second.run().unwrap(), LoadingEventOutcome::Loaded);
        let returned = first_host.take_loading_return().unwrap().unwrap();
        first_host.observe_loading_job_return(returned).unwrap();
        let returned = second_host.take_loading_return().unwrap().unwrap();
        second_host.observe_loading_job_return(returned).unwrap();
        complete_rejected(&mut first_fixture, &mut first_host);
        complete_rejected(&mut second_fixture, &mut second_host);
        assert_eq!(first_getters.load(Ordering::SeqCst), 1);
        assert_eq!(second_getters.load(Ordering::SeqCst), 1);
        assert_eq!(first_drops.load(Ordering::SeqCst), 1);
        assert_eq!(second_drops.load(Ordering::SeqCst), 1);
        finish(first_fixture, first_host);
        finish(second_fixture, second_host);
    }

    fn executor(
        backend: novarocks_types::BackendProcessId,
    ) -> HostExecutor<LoadingHostJob, fn(LoadingHostJob)> {
        let mut config = crate::TaskExecutionRegistryConfig::for_process(backend, 1, 1);
        config.max_prepare_workers = 1;
        let limits = HostExecutorLimits::for_loading(&config, NonZeroUsize::new(1).unwrap())
            .expect("the same actual fixed-Task inventory");
        match HostExecutor::start(limits, LoadingHostJob::run_on_host as fn(LoadingHostJob)) {
            Ok(executor) => executor,
            Err((error, _original)) => panic!("actual host executor start failed: {error}"),
        }
    }
    fn drain_executor(executor: HostExecutor<LoadingHostJob, fn(LoadingHostJob)>) {
        match executor.shutdown() {
            Ok(runner) => drop(runner),
            Err((reason, _original)) => panic!("actual physical drain failed: {reason:?}"),
        }
    }
    #[test]
    fn refused_submission_retains_the_original_task_job_until_indexed_physical_retry() {
        let (mut f, mut host, payload, getters, drops) = setup_payload();
        let closed = executor(f.identity.backend_process_id());
        let closed_handle = closed.handle();
        closed_handle.close_external();
        let submitted = host
            .submit_loading(payload, &closed_handle)
            .unwrap_or_else(|_| panic!("original paid loading roles admitted"));
        assert_eq!(
            submitted,
            LoadingSubmission::Pending(HostSubmitError::Closed)
        );
        assert_eq!(getters.load(Ordering::SeqCst), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert!(host.take_loading_return().unwrap().is_none());
        let original = host.loader().unwrap().work_id();
        let waiting = f.scope.snapshot().unwrap();
        assert_eq!(
            host.retry_loading_submission(&closed_handle),
            LoadingSubmission::Pending(HostSubmitError::Closed)
        );
        assert_eq!(f.scope.snapshot().unwrap().work, waiting.work);
        assert_eq!(f.scope.snapshot().unwrap().storage, waiting.storage);
        assert_eq!(host.loader().unwrap().work_id(), original);
        assert_eq!(getters.load(Ordering::SeqCst), 0);
        drain_executor(closed);

        // Replacing only the physical executor neither reissues admission nor
        // replaces the Task's original input, Work, author or report cell.
        let live = executor(f.identity.backend_process_id());
        let handle = live.handle();
        assert_eq!(
            host.retry_loading_submission(&handle),
            LoadingSubmission::Submitted
        );
        assert_eq!(
            host.retry_loading_submission(&handle),
            LoadingSubmission::NoPendingJob
        );
        drain_executor(live);
        assert_eq!(getters.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(host.loader().unwrap().work_id(), original);
        assert!(!host.loader().unwrap().ready_for_exit().unwrap());
        let returned = host
            .take_loading_return()
            .unwrap()
            .expect("the actual physical job filled its original paid slot");
        assert!(host.take_loading_return().unwrap().is_none());
        assert_eq!(
            host.observe_loading_job_return(returned).unwrap(),
            LoadingEventOutcome::Loaded
        );
        complete_rejected(&mut f, &mut host);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        finish(f, host);
    }
}
