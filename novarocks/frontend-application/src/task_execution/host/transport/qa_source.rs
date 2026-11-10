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

//! The original paid reply lane for Establish authorization and settlement.

use super::{ActorTransportWork, BodyOutcome, prepare_actor_transport};
use crate::{
    control_runtime::{
        actor::{FrontendActor, FrontendEffect, FrontendInput},
        host::PreparedEffectJob,
        transport_withdrawal::AttemptEffectEnvelope,
    },
    task_execution::{
        bridge::qa::{AttemptQaBridge, AttemptQaPortError, AttemptQaReply},
        error::TaskExecutionError,
        host::custody::ReturnedActorTransportResult,
    },
};
use novarocks_attempt_protocol::{
    effect::EffectBody, failure::HostFailureRef, input::AttemptInput,
};
use novarocks_control_runtime::{
    actor_turn::{ActorTurnError, EffectTicket},
    causal_input::{CausalFillFailure, CausalInputObligation},
    completion::ReportSlot,
    exit_custody::ExitCustody,
    obligation::{ReservationObligation, WorkObligation, WorkRetirementObserver},
    reported_work::ReportedWork,
    transaction::{RuntimeError, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{CapacityError, ReservationResources, ResourceVector},
    completion::ConsumerKind,
    identity::{ConsumerId, Generation, ReservationId},
    owned_storage::OwnedSlotNeed,
    scope::{ScopeError, WorkKind},
};
use novarocks_coordination_contract::{
    AttemptActionId, AttemptAuthorization, AttemptToLogical, AttemptToLogicalBody,
};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};
use tokio::sync::Mutex;

type Position = CausalInputObligation<FrontendActor>;

pub(crate) struct BoundAttemptQa(Arc<Mutex<AttemptQaBridge>>);
impl BoundAttemptQa {
    pub(super) async fn run(
        self,
        effect: Arc<AttemptEffectEnvelope>,
    ) -> Result<AttemptQaReply, AttemptQaPortError> {
        let mut bridge = self.0.lock().await;
        match &effect.protocol.body {
            EffectBody::EstablishAuthorization { operation, request } => {
                bridge.authorize(operation, request).await
            }
            EffectBody::Port(port) => bridge.handle_port(port).await,
            _ => unreachable!("same original QA effect"),
        }
    }
}

struct Live {
    effect: Arc<AttemptEffectEnvelope>,
    header: WorkRetirementObserver,
    result: Option<Position>,
    retired: bool,
}

#[must_use = "retain the original QA result, input credit, and refused publication"]
pub(crate) struct AttemptQaResultRefusal {
    pub(crate) original: ReturnedActorTransportResult,
    pub(crate) publication: Option<CausalFillFailure<FrontendInput>>,
}

pub(crate) struct AttemptQaSource {
    scope: ScopeHandle,
    generation: Generation,
    parent: ReservationId,
    observer: ConsumerId,
    bridge: Option<Arc<Mutex<AttemptQaBridge>>>,
    idle: Option<Position>,
    pending: Option<(Arc<AttemptEffectEnvelope>, ReportedWork)>,
    live: Option<Live>,
    failures: BTreeMap<HostFailureRef, TaskExecutionError>,
}

impl AttemptQaSource {
    pub(crate) fn new(
        parent: &WorkObligation,
        payer: &ReservationObligation,
        generation: Generation,
        observer: ConsumerId,
        bridge: AttemptQaBridge,
        position: Position,
    ) -> Self {
        let scope = parent.scope().clone();
        assert!(payer.belongs_to_bound_work(&scope, parent.id()));
        Self {
            scope,
            generation,
            parent: payer.id(),
            observer,
            bridge: Some(Arc::new(Mutex::new(bridge))),
            idle: Some(position),
            pending: None,
            live: None,
            failures: BTreeMap::new(),
        }
    }

    pub(super) fn matches_scope(&self, scope: &ScopeHandle) -> bool {
        self.scope.same_scope(scope)
    }

    pub(crate) fn prepay(
        &mut self,
        effect: &Arc<AttemptEffectEnvelope>,
    ) -> Result<Option<Position>, RuntimeError> {
        assert!(effect.is_qa_call());
        assert!(self.pending.is_none());
        if self.idle.is_none() {
            return Ok(None);
        }
        let kind = match effect.protocol.body {
            EffectBody::EstablishAuthorization { .. }
            | EffectBody::Port(AttemptToLogical {
                body:
                    AttemptToLogicalBody::AuthorizationRequest {
                        request:
                            AttemptAuthorization::BeginAdmission(_)
                            | AttemptAuthorization::ReauthorizeEstablish { .. },
                        ..
                    },
                ..
            }) => WorkKind::Business,
            _ => WorkKind::Settlement,
        };
        let resources = ReservationResources::new(
            ResourceVector::default(),
            OwnedSlotNeed {
                work_headers: 1,
                completions: 1,
                captures: 1,
                ..Default::default()
            },
        );
        let guard =
            match self
                .scope
                .reserve_child_next(self.generation, self.parent, resources, kind)
            {
                Ok(guard) => guard,
                Err(
                    RuntimeError::Scope(error)
                    | RuntimeError::Capacity(CapacityError::Scope(error)),
                ) if matches!(
                    error,
                    ScopeError::Failed | ScopeError::GenerationMismatch | ScopeError::Stopped
                ) =>
                {
                    return Ok(None);
                }
                Err(error) => return Err(error),
            };
        let (work, mut payer) = guard.bind(kind).expect("original QA child guard");
        let report = ReportSlot::reserve_next(
            &mut payer,
            self.generation,
            1,
            [(self.observer, ConsumerKind::Observer)],
        )
        .expect("original QA exit report");
        let work = ReportedWork::try_new(work, payer, report).expect("same original QA work");
        self.pending = Some((effect.clone(), work));
        Ok(self.idle.take())
    }

    pub(crate) fn bind(
        &mut self,
        ticket: &EffectTicket<FrontendEffect>,
        result: Position,
    ) -> PreparedEffectJob {
        let (effect, work) = self.pending.take().expect("same original QA prepayment");
        assert!(
            matches!(&ticket.effect, FrontendEffect::Attempt(original) if Arc::ptr_eq(original, &effect))
        );
        let header = work.work().retirement_observer();
        assert!(
            self.live
                .replace(Live {
                    effect: effect.clone(),
                    header,
                    result: None,
                    retired: false,
                })
                .is_none()
        );
        let bridge = self
            .bridge
            .as_ref()
            .expect("original QA executor owner")
            .clone();
        prepare_actor_transport(effect, result, work, VecDeque::new(), self.observer, |_| {
            ActorTransportWork::Qa(BoundAttemptQa(bridge))
        })
        .unwrap_or_else(|refused| std::panic::resume_unwind(Box::new(refused)))
    }

    pub(crate) fn return_unclaimed(&mut self, result: Position) {
        let (_, work) = self
            .pending
            .take()
            .expect("same original unclaimed QA role");
        let header = work.work().retirement_observer();
        let (custody, exit) = ExitCustody::from_admitted(work);
        exit.revoke_unused_and_exit()
            .expect("original unused QA work");
        assert!(custody.try_retire().expect("unused QA report"));
        assert!(
            self.scope
                .reap_observed_header(&header)
                .expect("unused QA header")
        );
        assert!(self.idle.replace(result).is_none());
    }

    pub(super) fn owns(&self, effect: AttemptActionId) -> bool {
        self.live
            .as_ref()
            .is_some_and(|live| live.effect.protocol.id == effect)
    }

    pub(super) fn needs_progress(&self, closing: bool) -> bool {
        self.live
            .as_ref()
            .is_some_and(|live| live.retired && live.result.is_some())
            || closing
                && self.pending.is_none()
                && self.live.is_none()
                && self.bridge.is_some()
                && (self.idle.is_some() || self.failures.is_empty())
    }

    pub(crate) fn accept(
        &mut self,
        mut original: ReturnedActorTransportResult,
    ) -> Result<(), AttemptQaResultRefusal> {
        let live = self
            .live
            .as_mut()
            .expect("original QA result has a live role");
        assert!(Arc::ptr_eq(&original.effect, &live.effect));
        if !matches!(&original.outcome, Some(BodyOutcome::Qa(_))) {
            return Err(AttemptQaResultRefusal {
                original,
                publication: None,
            });
        }
        let Some(BodyOutcome::Qa(AttemptQaReply { reply, failure })) = original.outcome.take()
        else {
            unreachable!("validated original QA outcome");
        };
        if let Some((reference, failure)) = failure {
            assert!(self.failures.insert(reference, failure).is_none());
        }
        let input = FrontendInput::Attempt(Box::new(AttemptInput::Port(reply)));
        if let Err(publication) = original.original.fill_with_receipt(None, input) {
            return Err(AttemptQaResultRefusal {
                original,
                publication: Some(publication),
            });
        }
        assert!(live.result.replace(original.original).is_none());
        Ok(())
    }

    pub(crate) fn transport_retired(&mut self, effect: AttemptActionId) {
        let live = self.live.as_mut().expect("original QA retirement");
        assert_eq!(live.effect.protocol.id, effect);
        assert!(!std::mem::replace(&mut live.retired, true));
    }

    pub(crate) fn retire_result(&mut self) -> Result<bool, ActorTurnError> {
        let Some(live) = self.live.as_ref() else {
            return Ok(false);
        };
        let Some(result) = live.result.as_ref() else {
            return Ok(false);
        };
        if !live.retired
            || !result.retire_ready_after_exit()?
            || !self.scope.reap_observed_header(&live.header)?
        {
            return Ok(false);
        }
        let result = self.live.take().unwrap().result.unwrap();
        assert!(self.idle.replace(result).is_none());
        Ok(true)
    }

    pub(crate) fn take_failure(&mut self, reference: HostFailureRef) -> Option<TaskExecutionError> {
        self.failures.remove(&reference)
    }

    pub(crate) fn close_idle(&mut self) -> Result<bool, ActorTurnError> {
        let Some(position) = self.idle.as_mut() else {
            return Ok(false);
        };
        position.close_after_exit()?;
        self.idle.take();
        Ok(true)
    }

    pub(crate) fn take_drained_bridge(&mut self) -> Option<Arc<Mutex<AttemptQaBridge>>> {
        if self.idle.is_none()
            && self.pending.is_none()
            && self.live.is_none()
            && self.failures.is_empty()
        {
            self.bridge.take()
        } else {
            None
        }
    }

    pub(super) fn is_drained(&self) -> bool {
        self.bridge.is_none()
    }
}
