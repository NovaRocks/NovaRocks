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

//! Original logical Scope funding. These checked obligations retain the same
//! root, source reports and headers; dropping them never supplies exit proof.

use crate::control_runtime::actor::FrontendActor;
use novarocks_control_runtime::{
    actor_turn::{ActorRegistration, ActorRuntime, ActorTurnError},
    completion::ReportSlot,
    header_readiness::HeaderReadyRegistration,
    obligation::{ReservationObligation, StepRegistration, WorkObligation},
    reported_work::ReportedWork,
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    completion::ConsumerKind,
    identity::{ConsumerId, Generation, ReservationId},
    owned_storage::OwnedSlotNeed,
    scope::{WorkAdmission, WorkKind},
};

pub(super) const GENERATION: Generation = Generation::new(1).unwrap();
const OBSERVER: ConsumerId = ConsumerId::new(1).unwrap();

#[must_use = "Original logical funding must retire after its sources and captures actually exit"]
pub(super) struct LogicalFundingObligation {
    scope: ScopeHandle,
    root: Option<SourceFundingObligation>,
}

/// Private routing to the SAME live root reservation. The kernel validates its
/// original bound Work, generation, scope gates and real resource request on
/// every reservation. This projection carries no prepaid capacity or exit proof.
#[derive(Clone)]
pub(super) struct LogicalStepReservations {
    scope: ScopeHandle,
    parent: ReservationId,
}
impl LogicalStepReservations {
    pub(super) fn reserve(&self, kind: WorkKind) -> Result<StepRegistration, RuntimeError> {
        self.scope.reserve_child_next(
            GENERATION,
            self.parent,
            ReservationResources::new(
                ResourceVector::default(),
                OwnedSlotNeed {
                    work_headers: 1,
                    completions: 1,
                    captures: 1,
                    ..Default::default()
                },
            ),
            kind,
        )
    }
}
pub(super) struct SourceFundingObligation {
    work: WorkObligation,
    payer: ReservationObligation,
    readiness: HeaderReadyRegistration,
    exited: bool,
}
impl SourceFundingObligation {
    pub(super) fn work(&self) -> &WorkObligation {
        &self.work
    }
    pub(super) fn start(guard: StepRegistration) -> Self {
        let (mut work, payer) = guard
            .bind(WorkKind::Settlement)
            .expect("original logical root kind");
        work.start().expect("original logical root starts once");
        let readiness = work
            .header_readiness_registration()
            .expect("original root readiness registration");
        Self {
            work,
            payer,
            readiness,
            exited: false,
        }
    }
    fn exit(&mut self) -> Result<(), RuntimeError> {
        if !self.exited {
            self.work.body_exited()?;
            self.work.captures_exited()?;
            self.exited = true;
        }
        Ok(())
    }
    pub(super) fn progress(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        actor: &ActorRegistration,
    ) -> Result<bool, ActorTurnError> {
        self.exit()?;
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
            .expect("arm original logical root for its installed host");
        Ok(false)
    }
    #[cfg_attr(
        test,
        expect(
            dead_code,
            reason = "P12 installer composition owns this pre-publication rollback path"
        )
    )]
    pub(super) fn retire_unpublished(mut self) {
        self.exit()
            .expect("unpublished logical root has no body or captures");
        self.payer
            .finish()
            .expect("unpublished logical root has no child custody");
        assert!(
            self.work
                .scope()
                .reap_observed_header(&self.work.retirement_observer())
                .expect("original unpublished logical header")
        );
        self.readiness.cancel();
    }
}
impl LogicalFundingObligation {
    pub(super) fn reserve(ledger: &RuntimeLedger) -> Result<Self, RuntimeError> {
        let scope = ledger.open_scope_next(None, GENERATION)?;
        let guard = match scope.reserve_next(
            GENERATION,
            ReservationResources::work_only(),
            WorkKind::Settlement,
            WorkAdmission::External,
        ) {
            Ok(guard) => guard,
            Err(error) => {
                ledger
                    .remove_scope(scope.id())
                    .expect("unused original logical scope");
                return Err(error);
            }
        };
        Ok(Self {
            scope,
            root: Some(SourceFundingObligation::start(guard)),
        })
    }
    pub(super) fn scope(&self) -> &ScopeHandle {
        &self.scope
    }
    pub(super) fn step_reservations(&self) -> LogicalStepReservations {
        LogicalStepReservations {
            scope: self.scope.clone(),
            parent: self
                .root
                .as_ref()
                .expect("live original logical funding")
                .payer
                .id(),
        }
    }
    pub(super) fn reserve_step(
        &self,
        kind: WorkKind,
        owned_slots: OwnedSlotNeed,
    ) -> Result<StepRegistration, RuntimeError> {
        let root = self.root.as_ref().expect("live original logical funding");
        self.scope.reserve_child_next(
            GENERATION,
            root.payer.id(),
            ReservationResources::new(ResourceVector::default(), owned_slots),
            kind,
        )
    }
    /// A source/report author is derived before any standing input is bound.
    /// Refusal returns no fabricated role and leaves the root live.
    pub(super) fn reserve_reported(
        &self,
        kind: WorkKind,
        inputs: usize,
    ) -> Result<ReportedWork, RuntimeError> {
        let guard = self.reserve_step(
            kind,
            OwnedSlotNeed {
                inputs,
                work_headers: 1,
                completions: 1,
                captures: 1,
                ..Default::default()
            },
        )?;
        Ok(reported_step(guard, kind))
    }

    /// The caller has already observed real source and rich-capture disposal.
    /// Runtime child/role/report/header gates remain independently enforced.
    pub(super) fn progress(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        actor: &ActorRegistration,
    ) -> Result<bool, ActorTurnError> {
        let Some(root) = self.root.as_mut() else {
            return Ok(true);
        };
        if root.progress(runtime, actor)? {
            self.root = None;
            return Ok(true);
        }
        Ok(false)
    }
    #[cfg_attr(
        test,
        expect(
            dead_code,
            reason = "P12 installer composition owns this pre-publication rollback path"
        )
    )]
    pub(super) fn retire_unpublished(mut self, ledger: &RuntimeLedger) {
        if let Some(root) = self.root.take() {
            root.retire_unpublished();
        }
        ledger
            .remove_scope(self.scope.id())
            .expect("unpublished logical scope has no retained originals");
    }
}

pub(super) fn reported_step(guard: StepRegistration, kind: WorkKind) -> ReportedWork {
    let (work, mut payer) = guard
        .bind(kind)
        .expect("original logical purpose binds its reserved kind");
    let report = ReportSlot::reserve_next(
        &mut payer,
        GENERATION,
        1,
        [(OBSERVER, ConsumerKind::Observer)],
    )
    .expect("original logical purpose owns its report position");
    ReportedWork::try_new(work, payer, report).expect("same original logical work/report/captures")
}
