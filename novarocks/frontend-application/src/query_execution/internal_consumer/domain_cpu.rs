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

//! The fixed domain consumer's stable custody of its one serial CPU job.

use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use novarocks_execution_contract::{
    SafeDetail, failure::FailureProjection, root_result::RootResultEnd,
};
use novarocks_query_application::{
    admitted_query_context::QueryResultCapacityBinding,
    api::{QueryExecutionError, RetainedRootReply},
    cancellation::QueryCancellationView,
    execution_error::{ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason},
};

use super::domain_state::{InternalDomainFailure, InternalDomainSlot, InternalDomainState};
use crate::native::data_runtime::FrontendDataRuntime;
use crate::query_execution::internal_result_cpu::{
    InternalResultCpu, InternalResultCpuJob, InternalResultOperation as Operation,
    InternalResultValue,
};
use crate::task_execution::status_intake::NotifyWake;

type Job = InternalResultCpuJob<Result<InternalDomainSlot, InternalDomainFailure>>;

#[derive(Clone, Copy, PartialEq, Eq)]
enum LocalClosePhase {
    Open,
    // Original objects are being destroyed outside State. Empty fields alone
    // must not let the other fixed observer report actual exit.
    Retiring,
    Closed,
    Failed,
}

#[derive(Clone, Copy)]
enum CloseObserver {
    Consumer,
    Supervisor,
}

// One fixed fanout for the two named close observers. The job's registered
// waker owns only this wake header, never Domain/State/job/compute custody.
// Cancelling the last poller cannot replace the other observer's join wake.
#[derive(Default)]
struct LocalCloseWake {
    consumer: futures::task::AtomicWaker,
    supervisor: futures::task::AtomicWaker,
}
impl futures::task::ArcWake for LocalCloseWake {
    fn wake_by_ref(original: &Arc<Self>) {
        original.consumer.wake();
        original.supervisor.wake();
    }
}

struct State {
    value: Option<InternalResultValue<InternalDomainSlot>>,
    computation: Option<novarocks_workload_control::joint_admission::InternalComputationGrant>,
    job: Option<Job>,
    busy: bool,
    closing: bool,
    local_close: LocalClosePhase,
    receipt: Option<Result<(), QueryExecutionError>>,
    domain_failure: Option<InternalDomainFailure>,
    cpu_admission_failure: Option<novarocks_workload_control::WorkError>,
}

/// This object belongs to the registered consumer/statement holder, outside
/// every destructible receipt waiter. Dropping a waiter cannot lose its join,
/// decoded payload, original input allowance, or an early-cancelled job.
pub(crate) struct DomainCpuCustody {
    state: Mutex<State>,
    binding: QueryResultCapacityBinding,
    cpu: InternalResultCpu,
    runtime: FrontendDataRuntime,
    cancellation: QueryCancellationView,
    close_wake: Arc<LocalCloseWake>,
    receipt_wake: futures::task::AtomicWaker,
}

impl DomainCpuCustody {
    pub(crate) fn try_new(
        domain: InternalDomainState,
        binding: QueryResultCapacityBinding,
        cpu: InternalResultCpu,
        runtime: FrontendDataRuntime,
        cancellation: QueryCancellationView,
    ) -> Result<Arc<Self>, QueryExecutionError> {
        Self::try_new_stage(domain, binding, cpu, runtime, cancellation, None)
    }

    pub(crate) fn try_new_with_computation(
        domain: InternalDomainState,
        binding: QueryResultCapacityBinding,
        cpu: InternalResultCpu,
        runtime: FrontendDataRuntime,
        cancellation: QueryCancellationView,
        computation: novarocks_workload_control::joint_admission::InternalComputationGrant,
    ) -> Result<Arc<Self>, QueryExecutionError> {
        Self::try_new_stage(
            domain,
            binding,
            cpu,
            runtime,
            cancellation,
            Some(computation),
        )
    }

    fn try_new_stage(
        domain: InternalDomainState,
        binding: QueryResultCapacityBinding,
        cpu: InternalResultCpu,
        runtime: FrontendDataRuntime,
        cancellation: QueryCancellationView,
        computation: Option<novarocks_workload_control::joint_admission::InternalComputationGrant>,
    ) -> Result<Arc<Self>, QueryExecutionError> {
        if binding.is_root_backed_internal() != computation.is_some() {
            return Err(domain_error(
                "Root payload requires its exact independent stage computation",
            ));
        }
        if let Some(grant) = &computation {
            let alias = grant
                .retain_alias()
                .map_err(|_| domain_error("stage computation pin is unavailable"))?;
            if !alias.is_for_scope(binding.scope()) {
                return Err(domain_error(
                    "stage computation belongs to a foreign execution",
                ));
            }
        }
        let window = binding
            .try_window_alias()
            .map_err(|_| domain_error("original payload pin is unavailable"))?;
        let value = InternalResultValue::try_produce(binding.scope(), window, |_| {
            Ok(InternalDomainSlot::Collecting {
                state: Box::new(domain),
                #[cfg(test)]
                receipt: None,
            })
        })
        .map_err(|_| domain_error("internal decoder has no complete admitted capacity"))?;
        Ok(Arc::new(Self {
            state: Mutex::new(State {
                value: Some(value),
                computation,
                job: None,
                busy: false,
                closing: false,
                local_close: LocalClosePhase::Open,
                receipt: None,
                domain_failure: None,
                cpu_admission_failure: None,
            }),
            binding,
            cpu,
            runtime,
            cancellation,
            close_wake: Arc::new(LocalCloseWake::default()),
            receipt_wake: futures::task::AtomicWaker::new(),
        }))
    }

    pub(crate) async fn apply_body(
        &self,
        reply: Arc<RetainedRootReply>,
    ) -> Result<(), QueryExecutionError> {
        self.start(Operation::Body(reply))?;
        self.receipt().await
    }

    pub(crate) async fn finish_eof(&self, end: RootResultEnd) -> Result<(), QueryExecutionError> {
        self.start(Operation::End(end))?;
        self.receipt().await
    }

    pub(crate) async fn finish_statistics(&self) -> Result<(), QueryExecutionError> {
        self.start(Operation::FinishStatistics)?;
        self.receipt().await
    }

    pub(crate) async fn accept_write(
        &self,
        receiving: crate::query_execution::write_session::WriteReceivingPort,
    ) -> Result<(), QueryExecutionError> {
        self.start(Operation::AcceptWrite(receiving))?;
        self.receipt().await
    }

    fn start(&self, operation: Operation) -> Result<(), QueryExecutionError> {
        #[cfg(test)]
        {
            self.start_original(operation, None)
        }
        #[cfg(not(test))]
        {
            self.start_original(operation)
        }
    }

    // Production has no callback parameter. The test-only gate pauses this
    // SAME synchronous start after busy/source capture and before publication.
    fn start_original(
        &self,
        operation: Operation,
        #[cfg(test)] after_capture: Option<&dyn Fn()>,
    ) -> Result<(), QueryExecutionError> {
        let (input, computation, source) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.closing
                || state.busy
                || state.local_close == LocalClosePhase::Retiring
                || state.job.is_some()
            {
                return Err(domain_error(
                    "internal decoder already has an active or closed CPU job",
                ));
            }
            // Checked retain is a callback-free original State transaction.
            // The stable grant remains here until the complete receiving cut.
            let source = match state
                .computation
                .as_ref()
                .map(|grant| self.cpu.capture_action_source(grant, self.binding.scope()))
                .transpose()
            {
                Ok(source) => source,
                Err(error) => {
                    let projected =
                        QueryExecutionError::from_execution(ExecutionFailureInput::new(
                            error.failure_fact(),
                            SafeDetail::truncating("stage CPU action source is unavailable"),
                            ExecutionFailureSite::Coordinator,
                        ));
                    state.cpu_admission_failure = Some(error);
                    return Err(projected);
                }
            };
            let computation = state
                .computation
                .as_ref()
                .map(|grant| grant.retain_alias())
                .transpose()
                .map_err(|_| domain_error("stage computation pin count overflow"))?;
            let input = state
                .value
                .take()
                .ok_or_else(|| domain_error("internal decoder has no serial input"))?;
            state.busy = true;
            state.receipt = None;
            (input, computation, source)
        };
        #[cfg(test)]
        if let Some(after_capture) = after_capture {
            after_capture();
        }
        let tracked = match computation {
            Some(computation) => input.track_computation_exit(&self.binding, computation),
            None => input.track_actual_exit(&self.binding),
        };
        let input = match tracked {
            Ok(input) => input,
            Err(_) => {
                let error =
                    domain_error("internal decoder could not register its actual CPU input");
                let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                state.busy = false;
                state.receipt = Some(Err(error.clone()));
                drop(state);
                self.wake_close_observers();
                return Err(error);
            }
        };
        let wake = Arc::new(NotifyWake::new(Arc::new(tokio::sync::Notify::new())));
        let job = match source {
            Some(source) => self.cpu.submit_root_operation(
                input,
                source,
                self.cancellation.clone(),
                &self.runtime,
                wake,
                operation,
            ),
            // Explicit old non-Root path. It grants no joint/finite-source
            // qualification; all Root-backed domains above require the source.
            None => self.cpu.submit_operation(
                input,
                self.cancellation.clone(),
                &self.runtime,
                wake,
                operation,
            ),
        };
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.job = Some(job);
        // A concurrent close sees busy and retains this exact job. Receipt
        // polling itself uses the original oneshot waker, never a timed poll.
        state.busy = false;
        drop(state);
        self.wake_close_observers();
        Ok(())
    }

    pub(crate) async fn receipt(&self) -> Result<(), QueryExecutionError> {
        std::future::poll_fn(|cx| self.poll_receipt(cx)).await
    }

    fn poll_receipt(&self, cx: &mut Context<'_>) -> Poll<Result<(), QueryExecutionError>> {
        // Receiver::close need not wake a previous oneshot receiver poll.
        // Closing this custody explicitly wakes its original receipt waiter.
        self.receipt_wake.register(cx.waker());
        // Result receipt and the consume tail's close are sequential phases
        // of the same fixed caller, not separate close subscriptions.
        self.close_wake.consumer.register(cx.waker());
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.closing {
            return Poll::Ready(Err(domain_error(
                "internal decoder receipt owner is closing",
            )));
        }
        if state.local_close == LocalClosePhase::Retiring {
            return Poll::Pending;
        }
        if state.receipt.is_none() {
            let Some(job) = state.job.as_mut() else {
                return Poll::Ready(Err(domain_error(
                    "internal decoder has no dispatched CPU job",
                )));
            };
            let result = std::task::ready!(job.poll_take(cx));
            match result {
                Ok(output) => match output.try_transform(|slot, _| slot) {
                    Ok(output) => {
                        state.value = Some(output);
                        state.receipt = Some(Ok(()));
                    }
                    Err(error) => {
                        state.receipt = Some(Err(project_domain_failure(&error)));
                        state.domain_failure = Some(error);
                    }
                },
                Err(error) => state.receipt = Some(Err(QueryExecutionError::from_execution(error))),
            }
        }
        if let Some(Err(error)) = &state.receipt {
            // Early cancellation is an outcome, not actual exit. The original
            // job stays here until the independent close observer joins it.
            return Poll::Ready(Err(error.clone()));
        }
        let Some(job) = state.job.as_mut() else {
            return Poll::Ready(Ok(()));
        };
        let original_wake = futures::task::waker_ref(&self.close_wake);
        if std::task::ready!(job.poll_forwarder_closed(&mut Context::from_waker(&original_wake)))
            .is_err()
        {
            let error = domain_error("internal decoder CPU forwarder did not exit normally");
            state.receipt = Some(Err(error.clone()));
            return Poll::Ready(Err(error));
        }
        let retired = state
            .job
            .as_mut()
            .and_then(|job| job.take_retired_outcome());
        let original_job = state.job.take();
        state.local_close = LocalClosePhase::Retiring;
        drop(state);
        drop(retired);
        drop(original_job);
        self.finish_retirement(LocalClosePhase::Open);
        Poll::Ready(Ok(()))
    }

    /// Only a checked CPU result can leave this holder. The closed factory
    /// then embeds it in its concrete receiving holder before proposing Ready.
    pub(crate) fn take(
        &self,
    ) -> Result<InternalResultValue<InternalDomainSlot>, QueryExecutionError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.closing
            || state.busy
            || state.local_close == LocalClosePhase::Retiring
            || state.job.is_some()
            || !matches!(state.receipt, Some(Ok(())))
        {
            return Err(domain_error(
                "internal decoder output is not completely received",
            ));
        }
        state
            .value
            .take()
            .ok_or_else(|| domain_error("internal decoder output was already transferred"))
    }

    pub(crate) fn finish_computation_after_handoff(&self) -> Result<(), QueryExecutionError> {
        let computation = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.closing
                || state.busy
                || state.local_close == LocalClosePhase::Retiring
                || state.job.is_some()
                || state.value.is_some()
                || !matches!(state.receipt, Some(Ok(())))
            {
                return Err(domain_error(
                    "stage computation has not reached its actual receiving cut",
                ));
            }
            state.local_close = LocalClosePhase::Retiring;
            state.computation.take()
        };
        drop(computation);
        self.finish_retirement(LocalClosePhase::Open);
        Ok(())
    }

    pub(crate) fn begin_close(&self) {
        let (retired, began, retiring) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            let began = !state.closing;
            state.closing = true;
            // The other original poller may already own an outside-lock Drop.
            // Leave its deferred value in State until that retirement ends.
            let retired = if state.local_close == LocalClosePhase::Open {
                state.value.take()
            } else {
                None
            };
            let retiring = retired.is_some();
            if retiring {
                state.local_close = LocalClosePhase::Retiring;
            }
            (retired, began, retiring)
        };
        // A panicking original Drop leaves Retiring, never a false Closed.
        drop(retired);
        if retiring {
            self.finish_retirement(LocalClosePhase::Open);
        }
        if !retiring {
            self.receipt_wake.wake();
        }
        if began && !retiring {
            self.wake_close_observers();
        }
    }

    fn wake_close_observers(&self) {
        futures::task::ArcWake::wake_by_ref(&self.close_wake);
    }

    fn finish_retirement(&self, next: LocalClosePhase) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        debug_assert!(state.local_close == LocalClosePhase::Retiring);
        state.local_close = next;
        drop(state);
        // These are the two fixed close callers and the original result waiter.
        // Actual original Drop and State publication precede foreign callbacks.
        self.receipt_wake.wake();
        self.wake_close_observers();
    }

    pub(crate) fn health_failure(&self) -> Option<Arc<dyn std::error::Error + Send + Sync>> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.job.as_ref().and_then(|job| job.health_failure())
    }

    /// The supervisor's fixed observation position. This does not revoke
    /// execution permission and proves only this original local CPU exit.
    pub(crate) fn poll_join_closed(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Arc<dyn std::error::Error + Send + Sync>>> {
        self.poll_original_closed(cx, CloseObserver::Supervisor)
    }

    fn poll_original_closed(
        &self,
        cx: &mut Context<'_>,
        observer: CloseObserver,
    ) -> Poll<Result<(), Arc<dyn std::error::Error + Send + Sync>>> {
        match observer {
            CloseObserver::Consumer => self.close_wake.consumer.register(cx.waker()),
            CloseObserver::Supervisor => self.close_wake.supervisor.register(cx.waker()),
        }
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if !state.closing || state.busy || state.local_close == LocalClosePhase::Retiring {
            return Poll::Pending;
        }
        match state.local_close {
            LocalClosePhase::Closed => return Poll::Ready(Ok(())),
            LocalClosePhase::Failed => {
                // The exact failed job owns the original cause. Do not repoll
                // its source or acknowledge its disposition by removing it.
                return match state.job.as_ref().and_then(|job| job.health_failure()) {
                    Some(original) => Poll::Ready(Err(original)),
                    None => Poll::Pending,
                };
            }
            LocalClosePhase::Open => {}
            LocalClosePhase::Retiring => unreachable!(),
        }
        let Some(job) = state.job.as_mut() else {
            let value = state.value.take();
            let computation = state.computation.take();
            state.local_close = LocalClosePhase::Retiring;
            drop(state);
            drop(value);
            drop(computation);
            self.finish_retirement(LocalClosePhase::Closed);
            return Poll::Ready(Ok(()));
        };
        let retired = job.close_delivery();
        // Both fixed callers poll the one original JoinHandle with the same
        // pure fanout waker, so a cancelled waiter cannot steal its wake.
        let original_wake = futures::task::waker_ref(&self.close_wake);
        let result = job.poll_forwarder_closed(&mut Context::from_waker(&original_wake));
        let succeeded = matches!(result, Poll::Ready(Ok(())));
        let failed = matches!(result, Poll::Ready(Err(_)));
        let (original_job, late, value, computation) = if succeeded {
            let late = state
                .job
                .as_mut()
                .and_then(|job| job.take_retired_outcome());
            (
                state.job.take(),
                late,
                state.value.take(),
                state.computation.take(),
            )
        } else {
            (None, None, None, None)
        };
        let retiring = succeeded || failed || retired.is_some();
        if retiring {
            state.local_close = LocalClosePhase::Retiring;
        }
        drop(state);
        // No observer can infer exit while these original objects still drop.
        // Raw failures remain in the stable failed job, not in this result copy.
        drop(retired);
        drop(late);
        drop(value);
        drop(original_job);
        drop(computation);
        if retiring {
            self.finish_retirement(if succeeded {
                LocalClosePhase::Closed
            } else if failed {
                LocalClosePhase::Failed
            } else {
                LocalClosePhase::Open
            });
        }
        result
    }

    pub(crate) async fn join_closed(&self) -> Result<(), Arc<dyn std::error::Error + Send + Sync>> {
        self.begin_close();
        std::future::poll_fn(|cx| self.poll_original_closed(cx, CloseObserver::Consumer)).await
    }
}

pub(crate) fn domain_error(detail: &'static str) -> QueryExecutionError {
    QueryExecutionError::from_execution(ExecutionFailureInput::new(
        QueryFailureReason::PreparationProtocol.failure_fact(),
        SafeDetail::truncating(detail),
        ExecutionFailureSite::Coordinator,
    ))
}

fn project_domain_failure(error: &InternalDomainFailure) -> QueryExecutionError {
    match error.execution_input() {
        Some(input) => QueryExecutionError::from_execution(input),
        None => domain_error("internal domain decoder rejected its input"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_execution::{
        internal_result_cpu::{InternalResultCpuOwner, admitted_internal_fixture},
        statistics::StatisticsRootResultDecoder,
    };
    use novarocks_query_application::cancellation::QueryCancellationSource;
    use std::{
        future::Future,
        num::NonZeroU64,
        time::{Duration, Instant},
    };

    fn end() -> RootResultEnd {
        RootResultEnd {
            sequence: NonZeroU64::new(1).unwrap(),
            output_rows: 0,
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn statistics_retains_distinct_eof_and_success_receipts_in_stable_custody() {
        let (_control, root, binding, capacity) = admitted_internal_fixture();
        let mut owner = InternalResultCpuOwner::try_new_for_test().unwrap();
        let cancellation = QueryCancellationSource::new();
        let custody = DomainCpuCustody::try_new(
            InternalDomainState::Statistics(StatisticsRootResultDecoder::for_test_with_capacity(
                [],
                &binding,
            )),
            binding.clone(),
            owner.runtime(),
            FrontendDataRuntime::new(tokio::runtime::Handle::current()),
            cancellation.view(),
        )
        .unwrap();
        custody.finish_eof(end()).await.unwrap();
        {
            let state = custody.state.lock().unwrap();
            let value = state.value.as_ref().unwrap().value();
            assert_eq!(value.end(), Some(end()));
            assert!(!value.statistics_finished());
        }
        custody.finish_statistics().await.unwrap();
        let output = custody.take().unwrap();
        assert!(custody.take().is_err());
        assert!(
            output
                .hand_off(|slot, _| slot.into_statistics())
                .unwrap()
                .is_empty()
        );
        custody.join_closed().await.unwrap();
        drop(custody);
        drop(binding);
        root.owner.complete();
        root.business.release();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropping_receipt_waiter_keeps_original_job_until_real_cpu_exit() {
        let (_control, root, binding, capacity) = admitted_internal_fixture();
        let mut owner = InternalResultCpuOwner::try_new_for_test().unwrap();
        let runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let cancellation = QueryCancellationSource::new();
        let mut blockers = Vec::new();
        let mut releases = Vec::new();
        let mut entered = Vec::new();
        // Occupy the actual finite worker set so EOF is genuinely queued.
        let workers = owner.runtime().configured_capacity().worker_threads().get();
        for _ in 0..workers {
            let (release, gate) = std::sync::mpsc::channel();
            let (started, start) = tokio::sync::oneshot::channel();
            let input =
                InternalResultValue::try_produce(binding.scope(), binding.window_alias(), |_| {
                    Ok(())
                })
                .unwrap()
                .track_actual_exit(&binding)
                .unwrap();
            blockers.push(owner.runtime().submit(
                input,
                cancellation.view(),
                &runtime,
                Arc::new(NotifyWake::new(Arc::new(tokio::sync::Notify::new()))),
                move |(), _| {
                    let _ = started.send(());
                    gate.recv_timeout(Duration::from_secs(10))
                        .expect("release CPU gate watchdog");
                },
            ));
            releases.push(release);
            entered.push(start);
        }
        for entered in entered {
            tokio::time::timeout(Duration::from_secs(5), entered)
                .await
                .unwrap()
                .unwrap();
        }
        let custody = DomainCpuCustody::try_new(
            InternalDomainState::Statistics(StatisticsRootResultDecoder::for_test_with_capacity(
                [],
                &binding,
            )),
            binding.clone(),
            owner.runtime(),
            runtime,
            cancellation.view(),
        )
        .unwrap();
        custody.start(Operation::End(end())).unwrap();
        {
            let receipt = custody.receipt();
            tokio::pin!(receipt);
            std::future::poll_fn(|cx| {
                assert!(receipt.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
        }
        {
            struct ReceiptWake(std::sync::atomic::AtomicBool);
            impl std::task::Wake for ReceiptWake {
                fn wake(self: Arc<Self>) {
                    self.wake_by_ref();
                }
                fn wake_by_ref(self: &Arc<Self>) {
                    self.0.store(true, std::sync::atomic::Ordering::Release);
                }
            }
            let receipt_wake = Arc::new(ReceiptWake(std::sync::atomic::AtomicBool::new(false)));
            let waker = std::task::Waker::from(Arc::clone(&receipt_wake));
            let mut receipt_context = Context::from_waker(&waker);
            let receipt = custody.receipt();
            tokio::pin!(receipt);
            assert!(receipt.as_mut().poll(&mut receipt_context).is_pending());
            let drain = custody.join_closed();
            tokio::pin!(drain);
            std::future::poll_fn(|cx| {
                assert!(drain.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            assert!(receipt_wake.0.load(std::sync::atomic::Ordering::Acquire));
            assert!(matches!(
                receipt.as_mut().poll(&mut receipt_context),
                Poll::Ready(Err(_))
            ));
            assert!(custody.state.lock().unwrap().job.is_some());
            assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
            for release in releases {
                release.send(()).unwrap();
            }
            tokio::time::timeout(Duration::from_secs(5), drain)
                .await
                .unwrap()
                .unwrap();
        }
        for mut job in blockers {
            job.join_closed().await.unwrap();
        }
        binding.result_activities_exited().await;
        drop(custody);
        drop(binding);
        root.owner.complete();
        root.business.release();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn close_destroys_unclaimed_original_payload_outside_the_custody_lock() {
        close_original_payload(false).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn the_other_close_observer_waits_for_the_original_taken_payload_drop() {
        close_original_payload(true).await;
    }

    async fn close_original_payload(pause_drop: bool) {
        use novarocks_spi::connector::{ConnectorPayloadRetentionGuard, StatisticsArtifactDraft};
        use std::sync::atomic::{AtomicBool, Ordering};
        struct DropProbe {
            custody: std::sync::Weak<DomainCpuCustody>,
            dropped: Arc<AtomicBool>,
            gate: Option<(
                std::sync::mpsc::SyncSender<()>,
                Mutex<std::sync::mpsc::Receiver<()>>,
            )>,
        }
        impl Drop for DropProbe {
            fn drop(&mut self) {
                let custody = self.custody.upgrade().expect("stable receiving owner");
                assert!(
                    custody.state.try_lock().is_ok(),
                    "payload destroyed under custody lock"
                );
                if let Some((entered, gate)) = &self.gate {
                    entered.send(()).unwrap();
                    gate.lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(10))
                        .unwrap();
                }
                self.dropped.store(true, Ordering::Release);
            }
        }
        let (_control, root, binding, capacity) = admitted_internal_fixture();
        let mut owner = InternalResultCpuOwner::try_new_for_test().unwrap();
        let runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        let cancellation = QueryCancellationSource::new();
        let custody = DomainCpuCustody::try_new(
            InternalDomainState::Statistics(StatisticsRootResultDecoder::for_test_with_capacity(
                [],
                &binding,
            )),
            binding.clone(),
            owner.runtime(),
            runtime.clone(),
            cancellation.view(),
        )
        .unwrap();
        let input = custody
            .state
            .lock()
            .unwrap()
            .value
            .take()
            .unwrap()
            .track_actual_exit(&binding)
            .unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        let (entered, entry) = std::sync::mpsc::sync_channel(1);
        let (release, gate) = std::sync::mpsc::sync_channel(0);
        let probe = DropProbe {
            custody: Arc::downgrade(&custody),
            dropped: Arc::clone(&dropped),
            gate: pause_drop.then_some((entered, Mutex::new(gate))),
        };
        let completed = Arc::new(tokio::sync::Notify::new());
        let job = owner.runtime().submit(
            input,
            cancellation.view(),
            &runtime,
            Arc::new(NotifyWake::new(Arc::clone(&completed))),
            move |_original, window| {
                let artifact = StatisticsArtifactDraft::try_new_with_guard(
                    vec![1],
                    "apache-datasketches-theta-v1",
                    bytes::Bytes::from_static(b"original artifact"),
                    Default::default(),
                    ConnectorPayloadRetentionGuard::new((window.clone(), probe)),
                )
                .unwrap();
                Ok(InternalDomainSlot::StatisticsFinished(vec![artifact]))
            },
        );
        custody.state.lock().unwrap().job = Some(job);
        tokio::time::timeout(Duration::from_secs(5), completed.notified())
            .await
            .unwrap();
        assert!(!dropped.load(Ordering::Acquire));
        if pause_drop {
            struct ObserveWake(std::sync::atomic::AtomicUsize);
            impl std::task::Wake for ObserveWake {
                fn wake(self: Arc<Self>) {
                    self.wake_by_ref();
                }
                fn wake_by_ref(self: &Arc<Self>) {
                    self.0.fetch_add(1, Ordering::AcqRel);
                }
            }
            custody.begin_close();
            let wake = Arc::new(ObserveWake(std::sync::atomic::AtomicUsize::new(0)));
            let waker = std::task::Waker::from(Arc::clone(&wake));
            std::thread::scope(|threads| {
                let original = Arc::clone(&custody);
                let poller = threads.spawn(move || {
                    original.poll_original_closed(
                        &mut Context::from_waker(std::task::Waker::noop()),
                        CloseObserver::Consumer,
                    )
                });
                // This is the actual output destructor taken by the original
                // close poll, not a synthetic empty field/phase observation.
                entry.recv_timeout(Duration::from_secs(5)).unwrap();
                assert!(!dropped.load(Ordering::Acquire));
                assert!(
                    custody
                        .poll_join_closed(&mut Context::from_waker(&waker))
                        .is_pending()
                );
                {
                    let state = custody.state.lock().unwrap();
                    assert!(state.local_close == LocalClosePhase::Retiring);
                    assert!(
                        state.job.is_none(),
                        "the original joined job must already be taken"
                    );
                }
                let before_exit = wake.0.load(Ordering::Acquire);
                release.send(()).unwrap();
                assert!(matches!(poller.join().unwrap(), Poll::Ready(Ok(()))));
                assert!(wake.0.load(Ordering::Acquire) > before_exit);
                assert!(matches!(
                    custody.poll_join_closed(&mut Context::from_waker(&waker)),
                    Poll::Ready(Ok(()))
                ));
            });
        } else {
            custody.join_closed().await.unwrap();
        }
        assert!(dropped.load(Ordering::Acquire));
        drop(custody);
        drop(binding);
        root.owner.complete();
        root.business.release();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }
}

#[cfg(test)]
pub(crate) mod root_action_tests {
    use super::*;
    use crate::query_execution::internal_result_cpu::{
        INTERNAL_PEAK_BYTES, InternalResultCpuOwner,
    };
    use crate::query_execution::statistics::StatisticsRootResultDecoder;
    use novarocks_query_application::cancellation::{
        QueryCancellationReason, QueryCancellationSource,
    };
    use novarocks_resource_contract::SharedDependency;
    use novarocks_workload_control::joint_admission::*;
    use novarocks_workload_control::{
        ControlStorageConfig, WorkClass, WorkOwner, WorkRequest, WorkScope, WorkloadConfig,
        WorkloadControl,
    };
    use std::{
        future::Future,
        num::NonZeroU64,
        pin::Pin,
        task::Waker,
        time::{Duration, Instant},
    };
    pub(crate) fn root_control(
        cpu_owner: &mut InternalResultCpuOwner,
    ) -> (WorkloadControl, JointAdmissionHandle, JointProgressDriver) {
        let count = 8;
        let storage = ControlStorageConfig::new(count, &[]);
        let mut control = WorkloadControl::try_new_counted(WorkloadConfig {
            root_limit: count,
            business_limit: count,
            query_concurrency_limit: count,
            scope_records_limit: count,
            ..WorkloadConfig::default()
        })
        .unwrap()
        .owner;
        let (handle, driver) = control
            .configure_responsibility(ResponsibilityConfig {
                work_per_turn: std::num::NonZeroUsize::new(7).unwrap(),
            })
            .unwrap();
        control.configure_control_storage(storage).unwrap();
        control
            .configure_result_capacity(novarocks_workload_control::ResultCapacityConfig::V1)
            .unwrap();
        cpu_owner.bind_for_test_before_ready(&mut control).unwrap();
        control.mark_ready().unwrap();
        (control, handle, driver)
    }
    pub(crate) fn drive(driver: &mut JointProgressDriver) {
        for _ in 0..10000 {
            if matches!(driver.step(), JointProgress::Idle(_)) {
                return;
            }
        }
        panic!("original proof driver failed to reach idle");
    }
    fn receive(request: &mut JointAdmission) -> JointGrantedBatch {
        match Pin::new(request).poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(Ok(reply)) => reply,
            _ => panic!("expected the original paid reply"),
        }
    }
    pub(crate) fn bind(scope: &WorkScope, driver: &mut JointProgressDriver) {
        let mut proposal = scope.begin_control_binding().unwrap();
        proposal
            .push_dependency(&SharedDependency::LocalHost)
            .unwrap();
        proposal.end_change().unwrap();
        let mut request = proposal.submit().unwrap();
        drive(driver);
        let mut reply = receive(&mut request);
        reply
            .adjustment
            .as_mut()
            .unwrap()
            .install_control_scope_in(scope)
            .unwrap()
            .notify();
        drop(reply);
        drive(driver);
    }
    pub(crate) fn paid_claim(
        handle: &JointAdmissionHandle,
        owner: &WorkOwner,
        lane: JointLane,
        resource: JointResource,
        amount: u64,
        driver: &mut JointProgressDriver,
    ) -> JointClaim {
        let mut proposal = handle.begin_paid_proposal(owner).unwrap();
        proposal
            .begin_new(
                lane,
                JointResources::ZERO
                    .with(JointResource::ClaimRecords, 1)
                    .with(resource, amount),
            )
            .unwrap();
        proposal
            .push_dependency(&SharedDependency::LocalHost)
            .unwrap();
        proposal.end_change().unwrap();
        let mut request = proposal.submit().unwrap();
        drive(driver);
        let mut reply = receive(&mut request);
        reply.new_claims.pop().unwrap()
    }
    fn child(scope: &WorkScope, driver: &mut JointProgressDriver) -> WorkOwner {
        let mut request = scope.request_joint_child().unwrap();
        drive(driver);
        let mut reply = receive(&mut request);
        let mut target = None;
        reply
            .adjustment
            .as_mut()
            .unwrap()
            .install_child_in(scope, &mut target)
            .unwrap()
            .notify();
        drop(reply);
        drive(driver);
        target.unwrap()
    }
    async fn with_driver<F: Future>(
        control: &WorkloadControl,
        driver: &mut JointProgressDriver,
        future: F,
    ) -> F::Output {
        tokio::pin!(future);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match driver.step() {
                    JointProgress::More => tokio::task::yield_now().await,
                    JointProgress::Idle(revision) => tokio::select! {
                        biased;
                        result = &mut future => return result,
                        _ = control.wait_progress(revision) => {},
                    },
                }
            }
        })
        .await
        .unwrap()
    }
    #[tokio::test(flavor = "current_thread")]
    async fn root_eof_and_statistics_reuse_one_compute_through_the_real_joint_cpu_source() {
        root_case(false).await;
    }
    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_root_wait_keeps_its_original_input_until_the_same_forwarder_joins() {
        root_case(true).await;
    }
    #[derive(Clone, Copy)]
    enum LocalCloseCase {
        ObserveBeforeCut,
        StartPublication,
        EarlyResultWhileWorkerRuns,
        TwoCloseObservers,
    }

    #[tokio::test(flavor = "current_thread")]
    async fn polling_local_close_is_not_an_execution_cut_and_real_close_wakes_outside_state() {
        root_case_with_local_close(false, Some(LocalCloseCase::ObserveBeforeCut)).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn local_close_waits_for_the_actual_start_to_publish_its_original_job() {
        root_case_with_local_close(false, Some(LocalCloseCase::StartPublication)).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn early_cancel_result_does_not_retire_computation_before_the_real_worker_and_join() {
        root_case_with_local_close(false, Some(LocalCloseCase::EarlyResultWhileWorkerRuns)).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn two_fixed_close_observers_keep_the_original_join_wake_after_waiter_cancellation() {
        root_case_with_local_close(false, Some(LocalCloseCase::TwoCloseObservers)).await;
    }

    async fn root_case(cancel_before_proof: bool) {
        root_case_with_local_close(cancel_before_proof, None).await;
    }

    async fn root_case_with_local_close(
        cancel_before_proof: bool,
        local_close: Option<LocalCloseCase>,
    ) {
        let mut cpu_owner = InternalResultCpuOwner::try_new_for_test().unwrap();
        let (control, handle, mut driver) = root_control(&mut cpu_owner);
        let cpu = cpu_owner.runtime();
        let quote = cpu.operation_action_quote().unwrap();
        assert!(quote.metadata_bytes.get() > 0 && quote.physical_queue_bytes.get() > 0);
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let root_scope = work.owner.scope();
        bind(&root_scope, &mut driver);
        let mut raw = Some(paid_claim(
            &handle,
            &work.owner,
            JointLane::Custody,
            JointResource::PayloadBytes,
            INTERNAL_PEAK_BYTES,
            &mut driver,
        ));
        let mut payload = None;
        RetainedPayloadGrant::adopt_in(&root_scope, &mut raw, &mut payload)
            .unwrap()
            .notify();
        let stage = child(&root_scope, &mut driver);
        let stage_scope = stage.scope();
        let mut raw = Some(paid_claim(
            &handle,
            &stage,
            JointLane::Foreground,
            JointResource::Computations,
            1,
            &mut driver,
        ));
        let mut computation = None;
        InternalComputationGrant::adopt_in(&stage_scope, &mut raw, &mut computation)
            .unwrap()
            .notify();
        let binding = QueryResultCapacityBinding::from_internal_payload(
            &root_scope,
            &stage_scope,
            payload.as_ref().unwrap(),
        )
        .unwrap();
        let cancellation = QueryCancellationSource::new();
        let custody = DomainCpuCustody::try_new_with_computation(
            InternalDomainState::Statistics(StatisticsRootResultDecoder::for_test_with_capacity(
                [],
                &binding,
            )),
            binding.clone(),
            cpu,
            FrontendDataRuntime::new(tokio::runtime::Handle::current()),
            cancellation.view(),
            computation.unwrap(),
        )
        .unwrap();
        let end = RootResultEnd {
            sequence: NonZeroU64::new(1).unwrap(),
            output_rows: 0,
        };
        if let Some(case) = local_close {
            match case {
                LocalCloseCase::ObserveBeforeCut => {
                    use std::sync::atomic::{AtomicBool, Ordering};
                    struct CloseWake {
                        custody: std::sync::Weak<DomainCpuCustody>,
                        woken: AtomicBool,
                    }
                    impl std::task::Wake for CloseWake {
                        fn wake(self: Arc<Self>) {
                            self.wake_by_ref();
                        }
                        fn wake_by_ref(self: &Arc<Self>) {
                            let original = self.custody.upgrade().unwrap();
                            assert!(original.state.try_lock().is_ok(), "close wake under State");
                            self.woken.store(true, Ordering::Release);
                        }
                    }
                    let wake = Arc::new(CloseWake {
                        custody: Arc::downgrade(&custody),
                        woken: AtomicBool::new(false),
                    });
                    let waker = Waker::from(Arc::clone(&wake));
                    let mut context = Context::from_waker(&waker);
                    assert!(custody.poll_join_closed(&mut context).is_pending());
                    assert!(custody.state.lock().unwrap().computation.is_some());
                    assert!(!wake.woken.load(Ordering::Acquire));
                    custody.begin_close();
                    assert!(wake.woken.load(Ordering::Acquire));
                    assert!(matches!(
                        custody.poll_join_closed(&mut context),
                        Poll::Ready(Ok(()))
                    ));
                }
                LocalCloseCase::StartPublication => {
                    // Pause the SAME real start after source/alias capture.
                    // No test writes the busy flag or invents a queued job.
                    std::thread::scope(|threads| {
                        let (entered, entry) = std::sync::mpsc::sync_channel(1);
                        let (release, gate) = std::sync::mpsc::sync_channel(0);
                        let original = Arc::clone(&custody);
                        let starter = threads.spawn(move || {
                            original.start_original(
                                Operation::End(end),
                                Some(&|| {
                                    entered.send(()).unwrap();
                                    gate.recv_timeout(Duration::from_secs(10)).unwrap();
                                }),
                            )
                        });
                        entry.recv_timeout(Duration::from_secs(5)).unwrap();
                        custody.begin_close();
                        assert!(
                            custody
                                .poll_join_closed(&mut Context::from_waker(Waker::noop()))
                                .is_pending()
                        );
                        {
                            let state = custody.state.lock().unwrap();
                            assert!(state.busy);
                            assert!(state.job.is_none());
                            assert!(state.computation.is_some());
                        }
                        release.send(()).unwrap();
                        starter.join().unwrap().unwrap();
                    });
                    assert!(custody.state.lock().unwrap().job.is_some());
                    with_driver(
                        &control,
                        &mut driver,
                        std::future::poll_fn(|cx| custody.poll_join_closed(cx)),
                    )
                    .await
                    .unwrap();
                }
                LocalCloseCase::EarlyResultWhileWorkerRuns | LocalCloseCase::TwoCloseObservers => {
                    // This existing test-only injection uses the original
                    // real worker/forwarder and original computation activity.
                    // It does not certify arbitrary FnOnce as finite or joint.
                    let (input, computation) = {
                        let mut state = custody.state.lock().unwrap();
                        (
                            state.value.take().unwrap(),
                            state.computation.as_ref().unwrap().retain_alias().unwrap(),
                        )
                    };
                    let input = input.track_computation_exit(&binding, computation).unwrap();
                    let (release, gate) = std::sync::mpsc::channel();
                    let (entered, entry) = tokio::sync::oneshot::channel();
                    let runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
                    let completed = Arc::new(tokio::sync::Notify::new());
                    // Deliberately blocked arbitrary callbacks are not the
                    // registered closed finite factory. Keep this lifecycle
                    // injection on its original unbound physical test owner;
                    // the same Root's real computation alias still stays in
                    // the original job and is observed by this same custody.
                    let mut injection_owner = InternalResultCpuOwner::try_new_for_test().unwrap();
                    let job: Job = injection_owner.runtime().submit(
                        input,
                        cancellation.view(),
                        &runtime,
                        Arc::new(NotifyWake::new(Arc::clone(&completed))),
                        move |original, _| {
                            let _ = entered.send(());
                            gate.recv_timeout(Duration::from_secs(10)).unwrap();
                            Ok(original)
                        },
                    );
                    custody.state.lock().unwrap().job = Some(job);
                    tokio::time::timeout(Duration::from_secs(5), entry)
                        .await
                        .unwrap()
                        .unwrap();
                    cancellation.request(QueryCancellationReason::ExecutionCancellationRequested);
                    assert!(
                        tokio::time::timeout(Duration::from_secs(5), custody.receipt())
                            .await
                            .unwrap()
                            .is_err()
                    );
                    custody.begin_close();
                    assert!(
                        custody
                            .poll_join_closed(&mut Context::from_waker(Waker::noop()))
                            .is_pending()
                    );
                    {
                        let state = custody.state.lock().unwrap();
                        assert!(state.job.is_some());
                        assert!(state.computation.is_some());
                    }
                    let input_exit = binding.result_activities_exited();
                    tokio::pin!(input_exit);
                    assert!(
                        input_exit
                            .as_mut()
                            .poll(&mut Context::from_waker(Waker::noop()))
                            .is_pending()
                    );
                    if matches!(case, LocalCloseCase::TwoCloseObservers) {
                        use std::sync::atomic::{AtomicUsize, Ordering};
                        struct CountWake(AtomicUsize);
                        impl std::task::Wake for CountWake {
                            fn wake(self: Arc<Self>) {
                                self.wake_by_ref();
                            }
                            fn wake_by_ref(self: &Arc<Self>) {
                                self.0.fetch_add(1, Ordering::AcqRel);
                            }
                        }
                        // Consume the early cancellation's real Notify. The
                        // next notification is the original worker's late exit.
                        tokio::time::timeout(Duration::from_secs(5), completed.notified())
                            .await
                            .unwrap();
                        let supervisor = Arc::new(CountWake(AtomicUsize::new(0)));
                        let supervisor_waker = Waker::from(Arc::clone(&supervisor));
                        assert!(
                            custody
                                .poll_join_closed(&mut Context::from_waker(&supervisor_waker))
                                .is_pending()
                        );
                        let consumer = Arc::new(CountWake(AtomicUsize::new(0)));
                        let consumer_waker = Waker::from(Arc::clone(&consumer));
                        let mut abandoned = Box::pin(custody.join_closed());
                        assert!(
                            abandoned
                                .as_mut()
                                .poll(&mut Context::from_waker(&consumer_waker))
                                .is_pending()
                        );
                        drop(abandoned);
                        let before_join = supervisor.0.load(Ordering::Acquire);
                        let late = completed.notified();
                        tokio::pin!(late);
                        late.as_mut().enable();
                        release.send(()).unwrap();
                        tokio::time::timeout(Duration::from_secs(5), late)
                            .await
                            .unwrap();
                        assert!(
                            supervisor.0.load(Ordering::Acquire) > before_join,
                            "dropping the last poller stole the other original join wake"
                        );
                        // A new waiter uses the same named consumer position,
                        // not a third subscription or a second JoinHandle.
                        let mut original_consumer = Box::pin(custody.join_closed());
                        // It may itself win the real join. Either winner must
                        // publish the same Closed and notify the other caller.
                        let first = original_consumer
                            .as_mut()
                            .poll(&mut Context::from_waker(&consumer_waker));
                        let before_take = consumer.0.load(Ordering::Acquire);
                        with_driver(
                            &control,
                            &mut driver,
                            std::future::poll_fn(|cx| custody.poll_join_closed(cx)),
                        )
                        .await
                        .unwrap();
                        if first.is_pending() {
                            assert!(consumer.0.load(Ordering::Acquire) > before_take);
                            assert!(matches!(
                                original_consumer
                                    .as_mut()
                                    .poll(&mut Context::from_waker(&consumer_waker)),
                                Poll::Ready(Ok(()))
                            ));
                        } else {
                            assert!(matches!(first, Poll::Ready(Ok(()))));
                        }
                    } else {
                        release.send(()).unwrap();
                        with_driver(
                            &control,
                            &mut driver,
                            std::future::poll_fn(|cx| custody.poll_join_closed(cx)),
                        )
                        .await
                        .unwrap();
                    }
                    tokio::time::timeout(Duration::from_secs(5), input_exit)
                        .await
                        .unwrap();
                    injection_owner
                        .shutdown_until(Instant::now() + Duration::from_secs(5))
                        .await
                        .unwrap();
                }
            }
            assert!(custody.state.lock().unwrap().job.is_none());
            assert!(custody.state.lock().unwrap().computation.is_none());
            assert!(custody.start(Operation::End(end)).is_err());
            // This is only local compute closure: both real Work owners and
            // the old Root payload remain live. No Native receipt is forged.
            stage_scope.check().unwrap();
            root_scope.check().unwrap();
            let original_payload = payload.as_ref().unwrap().retain_alias().unwrap();
            assert!(original_payload.is_for_scope(&root_scope));
            // The same configured result owner supplies bytes; the retained
            // responsibility quantity is not a new resource authorization.
            binding
                .window_alias()
                .check_backing_total(INTERNAL_PEAK_BYTES)
                .unwrap();
            drop(original_payload);
            let next = paid_claim(
                &handle,
                &stage,
                JointLane::Foreground,
                JointResource::Computations,
                1,
                &mut driver,
            );
            drop(next);
        } else if cancel_before_proof {
            custody.start(Operation::End(end)).unwrap();
            let mut input_exit = Box::pin(binding.result_activities_exited());
            assert!(
                input_exit
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            cancellation.request(QueryCancellationReason::ExecutionCancellationRequested);
            with_driver(&control, &mut driver, custody.join_closed())
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(5), input_exit)
                .await
                .unwrap();
            assert!(custody.take().is_err());
        } else {
            with_driver(&control, &mut driver, custody.finish_eof(end))
                .await
                .unwrap();
            assert!(
                custody.state.lock().unwrap().job.is_none(),
                "the original forwarder joins before serial reuse"
            );
            with_driver(&control, &mut driver, custody.finish_statistics())
                .await
                .unwrap();
            assert!(custody.state.lock().unwrap().job.is_none());
            let output = custody.take().unwrap();
            assert!(
                output
                    .hand_off(|slot, _| slot.into_statistics())
                    .unwrap()
                    .is_empty()
            );
            custody.finish_computation_after_handoff().unwrap();
            with_driver(&control, &mut driver, custody.join_closed())
                .await
                .unwrap();
        }
        drop(custody);
        drop(binding);
        drop(payload);
        stage.complete();
        work.owner.complete();
        work.business.release();
        drive(&mut driver);
        cpu_owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
    }
}
