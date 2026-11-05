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

//! One exclusive physical comparison on the original admitted route Work.

use super::*;
use crate::control_runtime::{
    establish_ingress::EstablishMaterializationLoan,
    participation_host::failure::ScopePublicationsDrained,
    participation_host_jobs::ParticipationDomainInstance,
    preparation::{HostFairnessGroup, QueuedPreparation},
};
use std::{
    any::Any,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
};

pub(super) struct Capture {
    authority: Option<ExitObligation>,
    source: Option<EstablishMaterializationLoan>,
    instance: Option<ParticipationDomainInstance>,
}
pub(super) enum ComparisonState {
    Idle,
    InFlight,
    Returned(Option<EstablishCredentialComparison>),
    Unresolved {
        // A lost or partly destroyed body cannot be moved or retried.
        _capture: Box<ManuallyDrop<Capture>>,
        _panic: Option<ManuallyDrop<Box<dyn Any + Send>>>,
    },
}
#[must_use = "Retain the same route authority and both original loans on queue refusal"]
pub(crate) struct EstablishComparisonJob {
    route: Arc<EstablishRouteCustody>,
    key: ScopedWorkRef,
    capture: Option<Box<ManuallyDrop<Capture>>>,
}
impl QueuedPreparation for EstablishComparisonJob {
    fn queue_key(
        &self,
    ) -> (
        novarocks_control_runtime_core::identity::ScopeId,
        novarocks_control_runtime_core::identity::WorkId,
    ) {
        (self.key.scope, self.key.work)
    }
    fn fairness_group(
        &self,
        _: novarocks_control_runtime_core::identity::ScopeId,
    ) -> HostFairnessGroup {
        HostFairnessGroup::Participation(self.route.participation)
    }
}
impl EstablishComparisonJob {
    pub(crate) fn belongs_to(
        &self,
        ready: &super::super::control_request::ControlRequestReady,
    ) -> bool {
        self.route.ingress.belongs_to(ready)
    }
    pub(crate) fn run(mut self) {
        let mut capture = self
            .capture
            .take()
            .expect("one original comparison capture");
        let result = catch_unwind(AssertUnwindSafe(|| {
            let cancelled = {
                let leg = self
                    .route
                    .participation_leg
                    .lock()
                    .expect("original comparison start");
                leg.cancelled || self.route.runtime.snapshot(self.route.actor)?.failed
            };
            let comparison = if cancelled {
                None
            } else {
                let original: &mut Capture = &mut capture;
                Some(
                    original
                        .instance
                        .as_mut()
                        .expect("original Participation instance")
                        .compare_establish_on_host(
                            original
                                .source
                                .as_ref()
                                .expect("original raw replay loan")
                                .shared_facts()
                                .initial_credential(),
                        )
                        .map_err(|_| EstablishRouteError::ComparisonUnresolved)?,
                )
            };
            // Foreign captures leave on this physical body before it returns
            // the same exclusive route authority. Source return is metadata.
            capture
                .instance
                .as_mut()
                .expect("original Participation instance")
                .finish_on_host();
            drop(capture.instance.take());
            drop(capture.source.take());
            Ok::<_, EstablishRouteError>(comparison)
        }));
        match result {
            Ok(Ok(comparison)) => {
                let authority = capture
                    .authority
                    .take()
                    .expect("same original route authority");
                let mut leg = self
                    .route
                    .participation_leg
                    .lock()
                    .expect("original comparison return");
                assert!(matches!(leg.comparison, ComparisonState::InFlight));
                assert!(leg.authority.is_none());
                leg.authority = Some(authority);
                leg.comparison = ComparisonState::Returned(comparison);
                drop(leg);
                drop(capture);
            }
            result => {
                let panic = match result {
                    Err(panic) => Some(ManuallyDrop::new(panic)),
                    Ok(Err(_)) => None,
                    Ok(Ok(_)) => unreachable!(),
                };
                self.retain_unknown(capture, panic);
            }
        }
        self.route.notify_progress();
    }
    fn retain_unknown(
        &self,
        mut capture: Box<ManuallyDrop<Capture>>,
        panic: Option<ManuallyDrop<Box<dyn Any + Send>>>,
    ) {
        // Dropping only the real authority marks its original Work Unresolved.
        // Rich captures remain at their stable addresses, without another Drop.
        drop(capture.authority.take());
        let mut leg = self
            .route
            .participation_leg
            .lock()
            .expect("original unresolved comparison");
        assert!(matches!(leg.comparison, ComparisonState::InFlight));
        leg.comparison = ComparisonState::Unresolved {
            _capture: capture,
            _panic: panic,
        };
    }
}
impl Drop for EstablishComparisonJob {
    fn drop(&mut self) {
        if let Some(capture) = self.capture.take() {
            self.retain_unknown(capture, None);
            self.route.notify_progress();
        }
    }
}
impl EstablishRouteCustody {
    pub(crate) fn bind_host_jobs(
        &self,
        jobs: Option<ParticipationHostJobs>,
        execution_scope: &ScopeHandle,
        participation_notice: ParticipationReadyNotice,
    ) -> Result<(), EstablishRouteError> {
        let mut leg = self
            .participation_leg
            .lock()
            .expect("original route Participation binding");
        if !matches!(leg.phase, ProbePhase::Constructing) || leg.scope.is_none() || leg.bound {
            return Err(EstablishRouteError::WrongPhase);
        }
        if let Some(jobs) = &jobs
            && !jobs.belongs_to_participation(
                &self.runtime,
                self.actor,
                self.participation,
                execution_scope,
            )
        {
            return Err(EstablishRouteError::WrongOwner);
        }
        if !participation_notice.matches_registration(&self.runtime, &self.registration)
            || participation_notice.participation() != self.participation
        {
            return Err(EstablishRouteError::WrongOwner);
        }
        leg.jobs = jobs;
        leg.participation_notice = Some(participation_notice);
        leg.bound = true;
        Ok(())
    }
    pub(crate) fn notify_progress(&self) {
        self.notice.notify();
        let notice = self
            .participation_leg
            .lock()
            .expect("original route progress notice")
            .participation_notice
            .clone();
        if let Some(notice) = notice {
            // A host claim alone cannot reopen a completed finite route
            // sweep. Publish this real return/cancel revision before waking.
            notice.notify();
            return;
        }
        // A never-bound route has no Participation inventory row or revision.
        match self.runtime.notify_host_actor(&self.registration) {
            Ok(())
            | Err(ActorTurnError::UnknownActor | ActorTurnError::EffectTicketOriginMismatch) => {}
            Err(_) => std::process::abort(),
        }
    }
    /// Only a real Core comparison request can move this route's one authority.
    /// No Work, report, reply, raw input or Participation instance is recreated.
    pub(crate) fn take_comparison_job(
        self: &Arc<Self>,
    ) -> Result<Option<EstablishComparisonJob>, EstablishRouteError> {
        let mut leg = self
            .participation_leg
            .lock()
            .expect("original routed comparison");
        if leg.cancelled
            || !matches!(
                leg.phase,
                ProbePhase::Waiting(EstablishReplayResult::CompareCredential { .. })
            )
            || !matches!(leg.comparison, ComparisonState::Idle)
        {
            return Ok(None);
        }
        let key = leg.work_id.ok_or(EstablishRouteError::WrongPhase)?;
        if leg.authority.is_none() {
            return Err(EstablishRouteError::WrongPhase);
        }
        let jobs = leg.jobs.as_ref().ok_or(EstablishRouteError::WrongPhase)?;
        let source = self
            .ingress
            .take_replay_comparison()
            .ok_or(EstablishRouteError::WrongPhase)?;
        let instance = match jobs.borrow_domain_instance() {
            Ok(instance) => instance,
            Err(_) if jobs.replay_comparison_retiring() => {
                // This exact monotonic Release/H instruction overtook the
                // read-only probe. Return the metadata-only raw loan and ask
                // the Core again; never turn ordinary stand-down into failure.
                drop(source);
                leg.comparison = ComparisonState::Returned(None);
                drop(leg);
                self.notify_progress();
                return Ok(None);
            }
            Err(_) => {
                drop(source);
                return Err(EstablishRouteError::WrongPhase);
            }
        };
        let authority = leg
            .authority
            .take()
            .expect("validated original route authority");
        leg.comparison = ComparisonState::InFlight;
        Ok(Some(EstablishComparisonJob {
            route: self.clone(),
            key,
            capture: Some(Box::new(ManuallyDrop::new(Capture {
                authority: Some(authority),
                source: Some(source),
                instance: Some(instance),
            }))),
        }))
    }
    /// The Participation lane retries only an actual M decision or physical return.
    pub(crate) fn advance_replay(&self) -> Result<(), EstablishRouteError> {
        let mut leg = self
            .participation_leg
            .lock()
            .expect("original replay continuation");
        if leg.cancelled || self.runtime.snapshot(self.actor)?.failed {
            if matches!(leg.comparison, ComparisonState::Returned(_)) {
                leg.comparison = ComparisonState::Idle;
            }
            return Ok(());
        }
        let comparison = match leg.comparison {
            ComparisonState::Returned(comparison) => comparison,
            ComparisonState::Idle
                if matches!(
                    leg.phase,
                    ProbePhase::Waiting(EstablishReplayResult::AwaitMaterialization)
                ) && leg
                    .jobs
                    .as_ref()
                    .is_some_and(ParticipationHostJobs::actual_establishment_decided) =>
            {
                None
            }
            _ => return Ok(()),
        };
        if !self.retire_participation_input(&mut leg)? {
            return Ok(());
        }
        leg.comparison = ComparisonState::Idle;
        self.issue_probe(&mut leg, comparison)
    }
    pub(crate) fn return_comparison_failure(&self) -> Result<(), EstablishRouteError> {
        let mut leg = self
            .participation_leg
            .lock()
            .expect("original comparison failure verdict");
        if !matches!(leg.comparison, ComparisonState::Unresolved { .. })
            || leg.owner_failed_returned
            || leg.cancelled
            || matches!(leg.phase, ProbePhase::Finished)
        {
            return Ok(());
        }
        self.fill_owner_failed(&mut leg)
    }
    fn fill_owner_failed(&self, leg: &mut ParticipationLeg) -> Result<(), EstablishRouteError> {
        let root = self
            .root_leg
            .lock()
            .expect("original failed route Root return");
        root.input
            .as_ref()
            .ok_or(EstablishRouteError::WrongPhase)?
            .fill_from_author(
                &self.root_author,
                WorkerMessage::Control(ControlMessage::Command {
                    work: self.root_author.work(),
                    request: None,
                    command: ControlInput::RoutedEstablishOwnerFailed {
                        participation: self.participation,
                        request: self.request.request,
                    },
                }),
            )
            .map_err(|(error, _)| EstablishRouteError::ActorTurn(error))?;
        leg.owner_failed_returned = true;
        leg.cancelled = true;
        Ok(())
    }
    pub(crate) fn fail_after_publications_drained(
        &self,
        stage: &ScopePublicationsDrained<'_>,
    ) -> Result<(), EstablishRouteError> {
        let mut leg = self
            .participation_leg
            .lock()
            .expect("original failed Participation route");
        stage
            .validate(&self.runtime, self.actor)
            .map_err(EstablishRouteError::ActorTurn)?;
        if !leg.owner_failed_returned
            && !leg.cancelled
            && !matches!(leg.phase, ProbePhase::Finished)
        {
            self.fill_owner_failed(&mut leg)?;
        }
        leg.cancelled = true;
        drop(leg);
        self.notice.notify();
        Ok(())
    }
}
