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

//! Retain the original per-attempt Source works through their physical exit.

use crate::control_runtime::actor::FrontendActor;
use novarocks_control_runtime::{
    actor_turn::{ActorRegistration, ActorRuntime, ActorTurnError},
    header_readiness::HeaderReadyRegistration,
    obligation::{
        ReservationObligation, ReservationRetirementObserver, StepRegistration, WorkObligation,
    },
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::ReservationResources,
    identity::Generation,
    scope::{WorkAdmission, WorkKind},
};

struct SourceWork {
    work: WorkObligation,
    payer: ReservationObligation,
    readiness: HeaderReadyRegistration,
    exited: bool,
}

impl SourceWork {
    fn start(reservation: StepRegistration) -> Self {
        let (mut work, payer) = reservation
            .bind(WorkKind::Settlement)
            .expect("the original source reservation binds its admitted kind");
        work.start()
            .expect("the admitted settlement source can start");
        let readiness = work
            .header_readiness_registration()
            .expect("one readiness registration for the original source");
        Self {
            work,
            payer,
            readiness,
            exited: false,
        }
    }

    fn record_exit(&mut self) -> Result<(), RuntimeError> {
        if !self.exited {
            self.work.body_exited()?;
            self.work.captures_exited()?;
            self.exited = true;
        }
        Ok(())
    }

    fn retire_unpublished(mut self) {
        let unused = self.payer.resources().expect("original unused resources");
        if !unused.quota.is_empty() || unused.keyed.is_some() {
            self.payer
                .release_quota_with_keyed(&unused.quota, unused.keyed.as_ref())
                .expect("unpublished source has no consumer of unused quota");
        }
        self.record_exit()
            .expect("unpublished source has no captures");
        self.payer
            .finish()
            .expect("unpublished source has no children");
        assert!(
            self.work
                .scope()
                .reap_observed_header(&self.work.retirement_observer())
                .expect("unpublished source belongs to the original scope")
        );
    }

    fn progress(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        actor: &ActorRegistration,
    ) -> Result<bool, ActorTurnError> {
        self.record_exit()?;
        let observer = self.work.retirement_observer();
        if self.work.actual_exit_ready()? {
            self.payer.finish()?;
            if self.work.scope().reap_observed_header(&observer)? {
                self.readiness.cancel();
                return Ok(true);
            }
        }
        self.readiness
            .arm(runtime, actor, &observer)
            .expect("arm the original source for its installed host");
        Ok(false)
    }
}

#[derive(Clone, Copy)]
pub(crate) enum FundingProgress {
    Waiting,
    More,
    Retired,
}

#[must_use]
pub(crate) struct AttemptHostFunding {
    scope: ScopeHandle,
    control_scope: ScopeHandle,
    parent: Option<SourceWork>,
    root_control: Option<SourceWork>,
    observation: Option<SourceWork>,
    observation_exit: Option<ReservationRetirementObserver>,
}

impl AttemptHostFunding {
    pub(crate) fn reserve(
        ledger: &RuntimeLedger,
        outer: Option<&WorkObligation>,
        generation: Generation,
        observation: ReservationResources,
    ) -> Result<Self, RuntimeError> {
        let root_resources = ReservationResources::work_only();
        let (scope, root) = if let Some(outer) = outer {
            ledger.open_child_scope_with_root(
                outer,
                generation,
                WorkKind::Settlement,
                root_resources.clone(),
            )?
        } else {
            let scope = ledger.open_scope_next(None, generation)?;
            let root = match scope.reserve_next(
                generation,
                root_resources.clone(),
                WorkKind::Settlement,
                WorkAdmission::External,
            ) {
                Ok(root) => root,
                Err(error) => {
                    remove_unpublished_scope(ledger, &scope);
                    return Err(error);
                }
            };
            (scope, root)
        };
        let parent = SourceWork::start(root);
        let generation = scope
            .gates()
            .expect("the new attempt scope is live")
            .generation;
        let (control_scope, control) = match ledger.open_child_scope_with_root(
            &parent.work,
            generation,
            WorkKind::Settlement,
            root_resources,
        ) {
            Ok(original) => original,
            Err(error) => {
                parent.retire_unpublished();
                remove_unpublished_scope(ledger, &scope);
                return Err(error);
            }
        };
        let root_control = SourceWork::start(control);
        let observation = match scope.reserve_child_next(
            generation,
            parent.payer.id(),
            observation,
            WorkKind::Settlement,
        ) {
            Ok(original) => SourceWork::start(original),
            Err(error) => {
                root_control.retire_unpublished();
                remove_unpublished_scope(ledger, &control_scope);
                parent.retire_unpublished();
                remove_unpublished_scope(ledger, &scope);
                return Err(error);
            }
        };
        Ok(Self {
            scope,
            control_scope,
            parent: Some(parent),
            root_control: Some(root_control),
            observation: Some(observation),
            observation_exit: None,
        })
    }

    pub(crate) fn scope(&self) -> &ScopeHandle {
        &self.scope
    }

    pub(crate) fn parent(&self) -> (&WorkObligation, &ReservationObligation) {
        let parent = self
            .parent
            .as_ref()
            .expect("original attempt source parent");
        (&parent.work, &parent.payer)
    }

    pub(crate) fn observation_and_root_control(
        &mut self,
    ) -> (&WorkObligation, &mut ReservationObligation, &WorkObligation) {
        let observation = self
            .observation
            .as_mut()
            .expect("original observation source");
        let root_control = self
            .root_control
            .as_ref()
            .expect("original root control source");
        (
            &observation.work,
            &mut observation.payer,
            &root_control.work,
        )
    }

    pub(crate) fn owns_observation(&self, backing: &ReservationRetirementObserver) -> bool {
        self.observation
            .as_ref()
            .is_some_and(|source| backing.belongs_to_bound_work(&source.work))
    }

    pub(crate) fn bind_observation_exit(&mut self, backing: ReservationRetirementObserver) {
        assert!(self.observation_exit.replace(backing).is_none());
    }

    pub(crate) fn retire_unpublished(mut self, ledger: &RuntimeLedger) {
        assert!(self.observation_exit.is_none());
        self.observation.take().unwrap().retire_unpublished();
        self.root_control.take().unwrap().retire_unpublished();
        remove_unpublished_scope(ledger, &self.control_scope);
        self.parent.take().unwrap().retire_unpublished();
        remove_unpublished_scope(ledger, &self.scope);
    }

    /// Called only once every installed source and transport obligation has
    /// returned. A retained backing alias signals the original host on release.
    pub(crate) fn progress(
        &mut self,
        ledger: &RuntimeLedger,
        runtime: &ActorRuntime<FrontendActor>,
        actor: &ActorRegistration,
    ) -> Result<FundingProgress, ActorTurnError> {
        if let Some(control) = &mut self.root_control {
            if !control.progress(runtime, actor)? {
                return Ok(FundingProgress::Waiting);
            }
            self.root_control = None;
            self.control_scope.close_external()?;
            ledger.remove_scope(self.control_scope.id())?;
            return Ok(FundingProgress::More);
        }
        if let Some(observation) = &mut self.observation {
            if !self
                .observation_exit
                .as_ref()
                .expect("installed observation has exact backing")
                .is_retired()
                || !observation.progress(runtime, actor)?
            {
                return Ok(FundingProgress::Waiting);
            }
            self.observation = None;
            self.observation_exit = None;
            return Ok(FundingProgress::More);
        }
        if let Some(parent) = &mut self.parent {
            if !parent.progress(runtime, actor)? {
                return Ok(FundingProgress::Waiting);
            }
            self.parent = None;
            self.scope.close_external()?;
        }
        Ok(FundingProgress::Retired)
    }
}

fn remove_unpublished_scope(ledger: &RuntimeLedger, scope: &ScopeHandle) {
    scope
        .close_external()
        .expect("unpublished scope is still live");
    ledger
        .remove_scope(scope.id())
        .expect("unpublished scope has no remaining roles");
}
