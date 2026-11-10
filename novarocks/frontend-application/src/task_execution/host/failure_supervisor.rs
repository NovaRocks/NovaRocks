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

//! The original cleanup carrier, separate from normal attempt dispatch.

use super::failure_publication::FailureContext;
use crate::control_runtime::actor::{FrontendActor, FrontendEffect};
use novarocks_control_runtime::{
    actor_turn::{ActorRegistration, ActorRuntime, ActorTurnError, EffectTicket},
    completion::ReportSlot,
    exit_custody::ExitCustody,
    exit_work::{ExitWorkError, PanicPayload, PanicSink, run_future},
    obligation::WorkRetirementObserver,
    reported_work::ReportedWork,
    transaction::ScopeHandle,
};
use novarocks_control_runtime_core::{
    completion::ConsumerKind,
    identity::{ConsumerId, ScopedWorkRef},
};
use novarocks_coordination_contract::AttemptActionId;
use novarocks_execution_contract::{AbortParticipation, ParticipationRef};
use std::{
    collections::BTreeSet,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
};
use tokio::{
    runtime::Handle,
    task::{JoinError, JoinHandle},
};

pub(crate) struct FailureCleanupRequest {
    pub(crate) core_panic: PanicPayload,
    pub(crate) publication: Result<(), ActorTurnError>,
    pub(crate) unknown_participations: Box<[ParticipationRef]>,
}

/// The bound Native/bridge cleanup owner consumes the original requests and
/// transfers unresolved remote responsibility before returning. Its borrowed
/// or child work must really exit before the enclosing job can retire.
pub(crate) trait FailureCleanupPort: Send + 'static {
    /// One best-effort send of the original frozen request. The port retains
    /// its actual verdict, including refusal or unknown, for the final handoff.
    fn abort_once(
        &mut self,
        request: AbortParticipation,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;

    fn finish(
        self: Box<Self>,
        original: FailureCleanupRequest,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>;
}

struct SupervisorWake {
    runtime: ActorRuntime<FrontendActor>,
    registration: ActorRegistration,
}
impl Wake for SupervisorWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.runtime.notify_host_actor(&self.registration);
    }
}

/// One unstarted Work, pre-paid in the attempt Scope and named by its immutable
/// runtime registration. The assembler retains it until failure or normal drain.
#[must_use]
pub(crate) struct FailureSupervisor {
    runtime: ActorRuntime<FrontendActor>,
    registration: ActorRegistration,
    original: ReportedWork,
    observer: ConsumerId,
    executor: Handle,
}

impl FailureSupervisor {
    pub(crate) fn new(
        runtime: ActorRuntime<FrontendActor>,
        registration: ActorRegistration,
        original: ReportedWork,
        observer: ConsumerId,
        executor: Handle,
    ) -> Self {
        assert!(registration.matches_scope(original.work().scope()));
        assert_eq!(
            original.report().consumer_kind(observer).unwrap(),
            ConsumerKind::Observer
        );
        Self {
            runtime,
            registration,
            original,
            observer,
            executor,
        }
    }

    pub(crate) fn matches(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        registration: &ActorRegistration,
        work: ScopedWorkRef,
    ) -> bool {
        self.runtime.same_runtime(runtime)
            && self.registration.same_registration(registration)
            && self.original.work().reference() == work
    }

    pub(crate) fn start(
        self,
        ticket: EffectTicket<FrontendEffect>,
        context: FailureContext,
        unresolved_actions: BTreeSet<AttemptActionId>,
        mut port: Box<dyn FailureCleanupPort>,
    ) -> RunningFailureSupervisor {
        assert_eq!(ticket.actor, self.registration.actor());
        assert_eq!(ticket.descriptor.work, self.original.work().reference());
        self.runtime
            .validate_failure_effect_ticket(&ticket)
            .expect("original supervisor ticket");
        let ticket = Arc::new(ticket);
        let body_ticket = ticket.clone();
        let runtime = self.runtime.clone();
        let body = async move {
            let publication = context.publish(
                &runtime,
                &body_ticket,
                unresolved_actions.into_iter().collect(),
            );
            let aborts = context.lifecycle.started_aborts().await;
            for &request in &aborts {
                port.abort_once(request).await;
            }
            // Starting or accepting an Abort does not prove remote exit.
            // The logical owner can apply any original positive facts it has.
            let unknown_participations = aborts
                .iter()
                .map(|request| request.participation())
                .collect();
            let core_panic = runtime
                .take_failure_payload(body_ticket.actor)
                .expect("original failed actor")
                .expect("original Core failure payload");
            port.finish(FailureCleanupRequest {
                core_panic,
                publication,
                unknown_participations,
            })
            .await;
        };
        self.launch(Some(ticket), body)
    }

    pub(crate) fn close_unused(
        self,
        port: Box<dyn FailureCleanupPort>,
    ) -> RunningFailureSupervisor {
        let state = self
            .runtime
            .snapshot(self.registration.actor())
            .expect("original attempt registration");
        assert!(state.sealed && !state.failed);
        self.launch(None, async move {
            drop(port);
        })
    }

    fn launch(
        self,
        ticket: Option<Arc<EffectTicket<FrontendEffect>>>,
        body: impl Future<Output = ()> + Send + 'static,
    ) -> RunningFailureSupervisor {
        let scope = self.original.work().scope().clone();
        let header = self.original.work().retirement_observer();
        let report = self.original.report().clone();
        let (custody, exit) = ExitCustody::from_admitted(self.original);
        let panic = PanicSink::new();
        let join = self.executor.spawn(run_future(exit, body, panic.clone()));
        let waker = Waker::from(Arc::new(SupervisorWake {
            runtime: self.runtime.clone(),
            registration: self.registration.clone(),
        }));
        RunningFailureSupervisor {
            runtime: self.runtime,
            registration: self.registration,
            scope,
            header,
            custody,
            report,
            observer: self.observer,
            ticket,
            join: Some(join),
            panic,
            waker,
            finished: false,
        }
    }
}

#[derive(Debug)]
pub(crate) enum FailureSupervisorError {
    Join(JoinError),
    Exit(ExitWorkError),
}

#[must_use = "the host retains the supervisor's Join, panic, and original ticket"]
pub(crate) struct RunningFailureSupervisor {
    runtime: ActorRuntime<FrontendActor>,
    registration: ActorRegistration,
    scope: ScopeHandle,
    header: WorkRetirementObserver,
    custody: ExitCustody,
    report: ReportSlot,
    observer: ConsumerId,
    ticket: Option<Arc<EffectTicket<FrontendEffect>>>,
    join: Option<JoinHandle<Result<(), ExitWorkError>>>,
    panic: PanicSink,
    waker: Waker,
    finished: bool,
}

impl RunningFailureSupervisor {
    pub(crate) fn panic(&self) -> &PanicSink {
        &self.panic
    }

    /// Poll once in the original host turn. A pending Join is only woken by
    /// the actual carrier. Publication and a failed Join leave the ticket live.
    pub(crate) fn progress(&mut self) -> Poll<Result<(), FailureSupervisorError>> {
        let Some(join) = self.join.as_mut() else {
            return if self.finished {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            };
        };
        let result = std::task::ready!(Pin::new(join).poll(&mut Context::from_waker(&self.waker)));
        self.join.take();
        match result {
            Err(error) => return Poll::Ready(Err(FailureSupervisorError::Join(error))),
            Ok(Err(error)) => return Poll::Ready(Err(FailureSupervisorError::Exit(error))),
            Ok(Ok(())) => {}
        }
        assert!(
            self.custody
                .actual_exit_ready()
                .expect("supervisor actual exit")
        );
        self.report
            .consume(self.observer)
            .expect("original exit report leg");
        assert!(
            self.custody
                .try_retire()
                .expect("supervisor actual retirement")
        );
        if let Some(ticket) = self.ticket.take() {
            let ticket = Arc::into_inner(ticket).expect("supervisor capture exited");
            let input = ticket.input;
            self.runtime
                .effect_completed_for(&self.registration, ticket)
                .expect("original supervisor completion");
            self.runtime
                .retire_input(self.registration.actor(), input)
                .expect("original supervisor input");
        }
        assert!(
            self.scope
                .reap_observed_header(&self.header)
                .expect("original supervisor header")
        );
        self.finished = true;
        Poll::Ready(Ok(()))
    }
}
