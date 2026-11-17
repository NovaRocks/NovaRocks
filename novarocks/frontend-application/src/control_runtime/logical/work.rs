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

//! One original logical job, its controlled launch, result and actual Join.
//! The source keeps the same custody until both independent inputs retire.

use super::{adapter::LogicalHostEffect, funding::GENERATION};
use crate::control_runtime::{
    actor::{FrontendActor, FrontendInput},
    host::PreparedEffectJob,
};
use novarocks_control_runtime::{
    actor_turn::{ActorRegistration, ActorRuntime, ActorTurnError},
    causal_input::CausalInputObligation,
    completion::ReportSlot,
    exit_custody::{ExitCustody, ExitCustodyPhase, ExitObligation},
    exit_work::{ExitWorkError, PanicSink, run_future_in_generation},
    header_readiness::HeaderReadyRegistration,
    obligation::WorkRetirementObserver,
    reported_work::ReportedWork,
    transaction::ScopeHandle,
};
use novarocks_control_runtime_core::{completion::SlotState, identity::ConsumerId};
use novarocks_logical_execution_protocol::{
    LogicalActionId,
    input::{ActionResult, LogicalInput},
};
use novarocks_query_application::api::logical_runtime::LogicalFuture;
use std::{
    any::Any,
    future::Future,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};
use tokio::{runtime::Handle, task::JoinHandle};

pub(super) const OBSERVER: ConsumerId = ConsumerId::new(1).unwrap();
pub(super) type RawFailure = Box<dyn Any + Send>;
pub(super) type FailureReporter = Arc<dyn Fn(RawFailure) + Send + Sync>;

/// Only the original carrier supplies the execution body. This seam cannot
/// manufacture work, a result position or a positive exit observation.
pub(super) trait LogicalJobExecutor: Send + Sync {
    fn deliver_port_now(
        &self,
        _: &ActorRuntime<FrontendActor>,
        _: &novarocks_control_runtime::actor_turn::EffectTicket<
            crate::control_runtime::actor::FrontendEffect,
        >,
        _: novarocks_coordination_contract::logical_port::LogicalToAttempt,
    ) -> Option<Result<(), novarocks_coordination_contract::QueryExecutionError>> {
        None
    }
    /// Clone only the SAME installed application originals for the prepaid
    /// failure job. Carrier-only test executors own no application resources.
    fn failure_executors(&self) -> Option<Arc<dyn super::failure::LogicalFailureOriginals>> {
        None
    }
    fn native_executor(
        &self,
    ) -> Option<Arc<novarocks_query_application::coordination::executor::NativeExecutor>> {
        None
    }
    fn take_joined_relay_cut(
        &self,
        _: novarocks_coordination_contract::AttemptActivationIdentity,
    ) -> Option<novarocks_query_application::coordination::StreamedRowsComputedCut> {
        None
    }
    fn execute(&self, body: LogicalHostEffect) -> LogicalFuture<'_, Option<ActionResult>>;
}

struct State {
    launch: Option<PreparedEffectJob>,
    join: Option<JoinHandle<Result<(), ExitWorkError>>>,
    launch_finished: bool,
    joined: bool,
    result: Option<CausalInputObligation<FrontendActor>>,
    result_filled: bool,
    result_action: Option<LogicalActionId>,
    failed: bool,
    report_consumed: bool,
}

#[must_use = "The original logical job retains its actual Join and exit custody"]
pub(super) struct LogicalWorkObligation {
    custody: ExitCustody,
    scope: ScopeHandle,
    report: ReportSlot,
    readiness: Mutex<Option<HeaderReadyRegistration>>,
    observer: WorkRetirementObserver,
    state: Mutex<State>,
    panic: PanicSink,
    wake: Waker,
    failure: FailureReporter,
    retained: Mutex<Option<Arc<Self>>>,
    captures: OriginalCaptures,
}

impl LogicalWorkObligation {
    pub(super) fn prepare(
        mut original: ReportedWork,
        wake: Waker,
        failure: FailureReporter,
    ) -> (Arc<Self>, ExitObligation) {
        let readiness = original
            .header_readiness_registration()
            .expect("original logical job header readiness");
        let scope = original.work().scope().clone();
        let observer = original.work().retirement_observer();
        let report = original.report().clone();
        let (custody, exit) = ExitCustody::from_admitted(original);
        let owner = Arc::new(Self {
            custody,
            scope,
            report,
            readiness: Mutex::new(Some(readiness)),
            observer,
            state: Mutex::new(State {
                launch: None,
                join: None,
                launch_finished: false,
                joined: false,
                result: None,
                result_filled: false,
                result_action: None,
                failed: false,
                report_consumed: false,
            }),
            panic: PanicSink::new(),
            wake,
            failure,
            retained: Mutex::new(None),
            captures: Arc::new(Mutex::new(None)),
        });
        *owner.retained.lock().unwrap() = Some(owner.clone());
        (owner, exit)
    }
    pub(super) fn owns_exit(&self, exit: &ExitObligation) -> bool {
        self.custody.owns_exit(exit)
    }
    /// The launch future captures only this stable slot. A spawn refusal never
    /// destroys an unpolled future containing the unique exit credential.
    pub(super) fn launch(
        self: &Arc<Self>,
        runtime: &Handle,
        mut job: PreparedEffectJob,
        executor: Arc<dyn LogicalJobExecutor>,
    ) {
        assert!(self.owns_exit(&job.exit), "same original logical exit");
        let capture = std::mem::replace(&mut job.captures, Box::new(()));
        assert!(
            self.captures
                .lock()
                .unwrap()
                .replace(ManuallyDrop::new(capture))
                .is_none(),
            "same original logical capture slot"
        );
        {
            let mut state = self.state.lock().unwrap();
            assert!(!state.launch_finished && state.launch.is_none());
            state.launch = Some(job);
        }
        let slot = self.clone();
        let spawned = catch_unwind(AssertUnwindSafe(|| {
            runtime.spawn(async move {
            let job = slot.state.lock().unwrap().launch.take()
                .expect("same original logical launch slot");
            let PreparedEffectJob { effect, exit, result, captures, transport } = job;
            let result = ResultPositionLoan { owner: slot.clone(), position: Some(result) };
            let panic = slot.panic.clone();
            let original_captures = slot.captures.clone();
            let body = async move {
                assert!(transport.is_none(), "logical jobs have their own original custody");
                let crate::control_runtime::actor::FrontendEffect::Logical(effect) = effect else {
                    panic!("foreign effect in logical launch");
                };
                // The Source retains a strong executor owner through controlled
                // disposal. This alias cannot be its last rich destructor.
                let action = match &effect.body {
                    LogicalHostEffect::Protocol(effect) => Some(effect.action),
                    LogicalHostEffect::JoinReadiness(_) | LogicalHostEffect::Bootstrap | LogicalHostEffect::ClientCloseObservation | LogicalHostEffect::PortDelivered(_) => None,
                };
                let required = matches!(&effect.body, LogicalHostEffect::Protocol(effect) if matches!(
                    effect.body,
                    novarocks_logical_execution_protocol::effect::LogicalEffectBody::Native(_)
                    | novarocks_logical_execution_protocol::effect::LogicalEffectBody::Relay(_)
                    | novarocks_logical_execution_protocol::effect::LogicalEffectBody::Consumer(_)
                    | novarocks_logical_execution_protocol::effect::LogicalEffectBody::Governance(_)));
                let fact = executor.execute(effect.body.clone()).await;
                assert!(!required || fact.is_some(), "logical executor returned without its required result");
                let input = match (action, fact) {
                    (Some(action), Some(result)) => FrontendInput::Logical(Box::new(
                        LogicalInput::ActionResult { action, result })),
                    (_, None) => FrontendInput::LogicalHostSettled,
                    (None, Some(_)) => panic!("host notice returned a protocol action result"),
                };
                result.fill(input);
                drop(captures); // the metadata-only empty launch placeholder
            };
            // Rich originals stay out of async local-variable unwind. Their
            // explicit destruction happens in the wrapper's F-Drop, after the
            // outer credentialed wrapper has caught any body panic.
            run_future_in_generation(exit, LogicalBodyFuture {
                inner: Some(Box::pin(body)), captures: original_captures,
            }, panic, GENERATION).await
        })
        }));
        match spawned {
            Ok(join) => {
                let mut state = self.state.lock().unwrap();
                assert!(state.join.replace(join).is_none());
                state.launch_finished = true;
            }
            Err(original) => {
                self.state.lock().unwrap().failed = true;
                // The same PreparedEffectJob remains in launch custody. Its
                // unique exit authority has not been dropped or declared gone.
                (self.failure)(original);
            }
        }
        self.wake.wake_by_ref();
    }
    pub(super) fn poll_join(self: &Arc<Self>, cx: &mut Context<'_>) -> bool {
        let join = {
            let mut state = self.state.lock().unwrap();
            if state.joined {
                return true;
            }
            if !state.launch_finished {
                return false;
            }
            state.join.take()
        };
        let Some(join) = join else {
            return false;
        };
        let mut loan = JoinLoan {
            owner: self.clone(),
            join: Some(join),
        };
        let result = match Pin::new(loan.join.as_mut().unwrap()).poll(cx) {
            Poll::Pending => return false,
            Poll::Ready(result) => result,
        };
        drop(loan.join.take());
        self.state.lock().unwrap().joined = true;
        match result {
            Ok(Ok(())) => {}
            Ok(Err(original)) => self.fail(Box::new(original)),
            Err(original) => self.fail(Box::new(original)),
        }
        while let Some(original) = self.panic.take() {
            self.fail(original);
        }
        if !self.state.lock().unwrap().result_filled {
            self.fail(Box::new(
                "logical executor exited without its original result",
            ));
        }
        true
    }
    fn fail(&self, original: RawFailure) {
        self.state.lock().unwrap().failed = true;
        (self.failure)(original);
    }
    pub(super) fn actual_exit(&self) -> bool {
        matches!(
            self.custody.phase(),
            ExitCustodyPhase::Exited | ExitCustodyPhase::Retired
        ) && self.custody.body_exited()
            && self.custody.captures_exited()
    }
    pub(super) fn take_result(&self) -> Option<CausalInputObligation<FrontendActor>> {
        self.state.lock().unwrap().result.take()
    }
    pub(super) fn custody_retired(&self) -> bool {
        self.custody.phase() == ExitCustodyPhase::Retired
    }
    pub(super) fn result_action(&self) -> Option<LogicalActionId> {
        self.state.lock().unwrap().result_action
    }
    #[cfg(test)]
    pub(super) fn result_filled(&self) -> bool {
        self.state.lock().unwrap().result_filled
    }
    pub(super) fn retire(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        actor: &ActorRegistration,
    ) -> Result<bool, ActorTurnError> {
        if !self.state.lock().unwrap().joined || !self.actual_exit() {
            return Ok(false);
        }
        if !self.state.lock().unwrap().report_consumed {
            match self.report.publication_state()? {
                SlotState::Filled => self.report.consume(OBSERVER)?,
                SlotState::RevokedUnused => {}
                SlotState::Reserved => return Ok(false),
            }
            self.state.lock().unwrap().report_consumed = true;
        }
        if (self.custody.phase() == ExitCustodyPhase::Retired
            || self
                .custody
                .try_retire()
                .expect("original logical job custody"))
            && self.scope.reap_observed_header(&self.observer)?
        {
            let readiness = self.readiness.lock().unwrap().take();
            if let Some(mut readiness) = readiness {
                readiness.cancel();
            }
            drop(self.retained.lock().unwrap().take());
            return Ok(true);
        }
        // The exact original header, including its children, owns this wake.
        // A result or a Join alone cannot make it ready.
        let mut readiness = self
            .readiness
            .lock()
            .unwrap()
            .take()
            .expect("original header registration");
        readiness
            .arm(runtime, actor, &self.observer)
            .expect("arm original logical job header");
        *self.readiness.lock().unwrap() = Some(readiness);
        Ok(false)
    }
}

type OriginalCaptures = Arc<Mutex<Option<ManuallyDrop<Box<dyn Any + Send>>>>>;
struct LogicalBodyFuture<F> {
    inner: Option<Pin<Box<F>>>,
    captures: OriginalCaptures,
}
impl<F: Future> Future for LogicalBodyFuture<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut()
            .inner
            .as_mut()
            .expect("original logical body")
            .as_mut()
            .poll(cx)
    }
}
impl<F> Drop for LogicalBodyFuture<F> {
    fn drop(&mut self) {
        // If the inner future's own teardown panics, the retained slot keeps
        // the other originals intact. Never start a second foreign destructor
        // while an unwind is already active.
        drop(self.inner.take());
        let capture = self.captures.lock().unwrap().take();
        if let Some(capture) = capture {
            drop(ManuallyDrop::into_inner(capture));
        }
    }
}

struct ResultPositionLoan {
    owner: Arc<LogicalWorkObligation>,
    position: Option<CausalInputObligation<FrontendActor>>,
}
impl ResultPositionLoan {
    fn fill(mut self, input: FrontendInput) {
        let action = match &input {
            FrontendInput::Logical(input) => match &**input {
                LogicalInput::ActionResult { action, .. } => Some(*action),
                _ => None,
            },
            _ => None,
        };
        let result = self
            .position
            .as_mut()
            .unwrap()
            .fill_with_receipt(None, input);
        match result {
            Ok(_) => {
                let mut state = self.owner.state.lock().unwrap();
                state.result_action = action;
                state.result_filled = true;
            }
            Err(original) => self.owner.fail(Box::new(original)),
        }
    }
}
impl Drop for ResultPositionLoan {
    fn drop(&mut self) {
        let position = self.position.take().expect("same original result position");
        assert!(
            self.owner
                .state
                .lock()
                .unwrap()
                .result
                .replace(position)
                .is_none()
        );
        self.owner.wake.wake_by_ref();
    }
}
struct JoinLoan {
    owner: Arc<LogicalWorkObligation>,
    join: Option<JoinHandle<Result<(), ExitWorkError>>>,
}
impl Drop for JoinLoan {
    fn drop(&mut self) {
        if let Some(join) = self.join.take() {
            assert!(
                self.owner
                    .state
                    .lock()
                    .unwrap()
                    .join
                    .replace(join)
                    .is_none()
            );
        }
    }
}

#[cfg(test)]
mod tests;
