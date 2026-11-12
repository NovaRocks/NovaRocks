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

//! Passive attempt notices and the original host's infrastructure-failure input.

use super::{adapter::AttemptNotice, custody::TransportSourceEvent};
use crate::control_runtime::{
    actor::{FrontendActor, FrontendEffect, FrontendInput},
    host::{
        EffectBindRefusal, EffectExecutor, EffectSource, PendingHandoff, PreparedEffectJob,
        SourceProgress,
    },
};
use novarocks_control_runtime::{
    actor_turn::{
        ActorHostTurn, ActorRegistration, ActorRuntime, ActorTurnError, EffectTicket,
        StandingInputObligation,
    },
    causal_input::CausalInputObligation,
};
use novarocks_control_runtime_core::identity::{InputId, Tick};
use novarocks_coordination_contract::AttemptActionId;
use std::{
    any::Any,
    fmt,
    future::pending,
    mem::ManuallyDrop,
    sync::{Arc, Mutex},
};
use tokio::sync::watch;

type Original = Box<dyn Any + Send>;

/// The exact failure stays in the original host and is shared with QA health.
/// Loss of a cleanup proof leaves the original captures in custody.
pub(crate) struct AttemptInfrastructureFailure {
    originals: Mutex<Vec<ManuallyDrop<Original>>>,
    notification_refusal: Mutex<Option<ActorTurnError>>,
}

impl fmt::Debug for AttemptInfrastructureFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AttemptInfrastructureFailure")
            .field("notification_refusal", &self.notification_refusal)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for AttemptInfrastructureFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Native attempt host retained unresolved infrastructure custody")
    }
}

impl std::error::Error for AttemptInfrastructureFailure {}

pub(crate) struct AttemptInfrastructureHealth(
    watch::Receiver<Option<Arc<AttemptInfrastructureFailure>>>,
);

impl AttemptInfrastructureHealth {
    pub(crate) async fn failed(&mut self) -> Arc<AttemptInfrastructureFailure> {
        loop {
            if let Some(original) = self.0.borrow_and_update().clone() {
                return original;
            }
            if self.0.changed().await.is_err() {
                pending().await
            }
        }
    }
}

/// Terminal and diagnostics notices observe facts already applied by Core.
/// The typed AbortSettlement is delivered by its QA Source. Retirement of
/// protocol authorization has no corresponding queue in this host; the
/// original QA call has already applied its settlement.
pub(crate) fn report_notice(notice: &AttemptNotice) {
    if let Some(failure) = &notice.diagnostic_failure {
        tracing::warn!(?failure, "Native attempt diagnostics could not be observed");
    }
    if let Some(failure) = &notice.round_failure {
        tracing::warn!(?failure, "Native attempt protocol round failed");
    }
}

pub(crate) struct AttemptAuxiliarySource {
    registration: ActorRegistration,
    input: Option<StandingInputObligation<FrontendActor>>,
    health: watch::Sender<Option<Arc<AttemptInfrastructureFailure>>>,
    closing: bool,
    transport_drained: bool,
    input_retirement_failed: bool,
}

impl AttemptAuxiliarySource {
    pub(crate) fn new(
        registration: ActorRegistration,
        input: StandingInputObligation<FrontendActor>,
    ) -> (Self, AttemptInfrastructureHealth) {
        assert!(input.matches_registration(&registration));
        let (health, observer) = watch::channel(None);
        (
            Self {
                registration,
                input: Some(input),
                health,
                closing: false,
                transport_drained: false,
                input_retirement_failed: false,
            },
            AttemptInfrastructureHealth(observer),
        )
    }

    fn retain(&mut self, original: Original) {
        if let Some(failure) = self.health.borrow().as_ref() {
            failure
                .originals
                .lock()
                .expect("original infrastructure custody")
                .push(ManuallyDrop::new(original));
            return;
        }
        let failure = Arc::new(AttemptInfrastructureFailure {
            originals: Mutex::new(vec![ManuallyDrop::new(original)]),
            notification_refusal: Mutex::new(None),
        });
        // The actor's own input source stays selectable while Core waits on
        // another Source. The original failure never enters the actor mailbox.
        if let Some(input) = &self.input
            && let Err((error, marker)) = input.fill(FrontendInput::AttemptInfrastructureFailed)
        {
            assert!(matches!(marker, FrontendInput::AttemptInfrastructureFailed));
            *failure
                .notification_refusal
                .lock()
                .expect("original infrastructure notification") = Some(error);
        }
        self.health.send_replace(Some(failure));
    }

    fn retire_input(&mut self, failed: bool) -> Result<bool, ActorTurnError> {
        let Some(input) = self.input.as_mut() else {
            return Ok(false);
        };
        if !input.retire_ready_after_exit()? {
            if failed {
                match input.take_failed_role_payload() {
                    Ok(Some(marker)) => {
                        marker.dispose()?;
                        return Ok(true);
                    }
                    Ok(None) | Err(ActorTurnError::InputAuthorMismatch) => {}
                    Err(error) => return Err(error),
                }
            }
            return Ok(false);
        }
        input.close_after_exit()?;
        self.input = None;
        Ok(true)
    }
}

impl EffectSource for AttemptAuxiliarySource {
    fn prepay_result(
        &mut self,
        effect: &FrontendEffect,
    ) -> Option<CausalInputObligation<FrontendActor>> {
        if self.health.borrow().is_none() {
            self.retain(Box::new(effect.clone()));
        }
        None
    }

    fn bind_job(
        &mut self,
        _: &ActorRuntime<FrontendActor>,
        _: &ActorHostTurn<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        result: CausalInputObligation<FrontendActor>,
    ) -> PreparedEffectJob {
        std::panic::resume_unwind(Box::new((ticket.effect.clone(), result)));
    }

    fn return_unclaimed_result(&mut self, original: CausalInputObligation<FrontendActor>) {
        self.retain(Box::new(original));
    }

    fn core_consumed(&mut self, _: &[InputId]) {}

    fn retain_transport_failure(&mut self, original: Original) {
        self.retain(original);
    }

    fn retain_transport_release(&mut self, original: Original) {
        self.retain(original);
    }

    fn transport_event(&mut self, effect: AttemptActionId, original: TransportSourceEvent) {
        self.retain(Box::new((effect, original)));
    }

    fn owner_failed_binding(
        &mut self,
        ticket: EffectTicket<FrontendEffect>,
        refused: EffectBindRefusal,
    ) {
        self.retain(Box::new((ticket, refused)));
    }

    fn owner_failed_pending(&mut self, original: PendingHandoff) {
        self.retain(Box::new(original));
    }

    fn transport_roles_drained(&mut self) {
        self.transport_drained = true;
    }

    fn close_external(&mut self) {
        self.closing = true;
    }

    fn result_admission_closed(&self) -> bool {
        self.closing || self.health.borrow().is_some()
    }

    fn progress_retirement(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        turn: &ActorHostTurn<FrontendActor>,
        _: Tick,
        budget: usize,
    ) -> SourceProgress {
        assert!(turn.matches_registration(&self.registration));
        let mut progressed = false;
        let mut units = 0;
        if self.input.is_some() && self.closing && !self.input_retirement_failed {
            if budget == 0 {
                return SourceProgress {
                    units,
                    progressed,
                    more: true,
                    retired: false,
                };
            }
            units += 1;
            let failed = runtime
                .snapshot(self.registration.actor())
                .expect("original infrastructure registration")
                .failed;
            if self.transport_drained || failed {
                match self.retire_input(failed) {
                    Ok(changed) => progressed = changed,
                    Err(error) => {
                        self.input_retirement_failed = true;
                        self.retain(Box::new(error));
                    }
                }
            }
        }
        SourceProgress {
            units,
            progressed,
            more: progressed,
            retired: self.input.is_none() && self.health.borrow().is_none(),
        }
    }
}

/// All executable purposes bind to their own native executor. Keep an
/// unsupported handoff intact for the host's failure custodian.
pub(crate) struct AttemptAuxiliaryExecutor;

impl EffectExecutor for AttemptAuxiliaryExecutor {
    fn handoff(&self, original: PreparedEffectJob) -> Result<(), PreparedEffectJob> {
        Err(original)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_runtime::actor::TestActor;
    use novarocks_control_runtime::{
        actor::{ActorConfig, ActorStep},
        transaction::RuntimeLedger,
    };
    use novarocks_control_runtime_core::{
        capacity::{ReservationResources, ResourceVector},
        identity::{ActorId, Generation},
        mailbox::{InputClass, InputEnvelope},
        owned_storage::OwnedSlotNeed,
        scope::{WorkAdmission, WorkKind},
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Probe(Arc<AtomicUsize>);
    impl TestActor for Probe {
        fn handle(
            &mut self,
            _: Tick,
            _: InputEnvelope,
            input: FrontendInput,
            step: &mut ActorStep<'_, FrontendEffect>,
        ) {
            match input {
                FrontendInput::Test(0) => {
                    step.select_input_source(Some(ActorId::new(2).unwrap()));
                }
                FrontendInput::AttemptInfrastructureFailed => {
                    self.0.fetch_add(1, Ordering::Relaxed);
                }
                _ => panic!("unexpected infrastructure test input"),
            }
        }
    }

    #[tokio::test]
    async fn original_infrastructure_fault_wakes_actor_during_foreign_source_selection() {
        let ledger = RuntimeLedger::new(ResourceVector::default());
        let generation = Generation::new(1).unwrap();
        let scope = ledger.open_scope_next(None, generation).unwrap();
        let (mut work, mut payer) = scope
            .reserve_next(
                generation,
                ReservationResources::new(
                    ResourceVector::default(),
                    OwnedSlotNeed {
                        inputs: 1,
                        work_headers: 1,
                        ..Default::default()
                    },
                ),
                WorkKind::Settlement,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Settlement)
            .unwrap();
        work.start().unwrap();

        let runtime = ActorRuntime::new();
        let actor = ActorId::new(1).unwrap();
        let received = Arc::new(AtomicUsize::new(0));
        let registration = runtime
            .register_bound(
                actor,
                scope.clone(),
                FrontendActor::Test(Box::new(Probe(received.clone()))),
                ActorConfig {
                    mailbox: [1; 4],
                    max_batches: 4,
                    max_effects_per_step: 1,
                    max_dependencies_per_effect: 0,
                    turn_budget: 8,
                    max_step_work: 1,
                },
            )
            .unwrap_or_else(|(error, _)| {
                panic!("register original infrastructure test actor: {error:?}")
            });
        let input = runtime
            .reserve_admitted_input_for(&registration, actor, InputClass::Lifecycle, &work)
            .unwrap()
            .into_standing_input()
            .unwrap_or_else(|(error, _)| panic!("reserve original infrastructure role: {error:?}"));
        let (mut source, mut health) = AttemptAuxiliarySource::new(registration, input);
        let selection = runtime
            .reserve_input(actor, actor, InputClass::Control)
            .unwrap()
            .fill(FrontendInput::Test(0))
            .unwrap();
        assert!(runtime.poll_one(Tick(1)).unwrap());
        runtime.retire_input(actor, selection).unwrap();

        let original = Arc::new(42);
        source.retain_transport_failure(Box::new(original.clone()));
        source.retain_transport_failure(Box::new("secondary original"));
        assert!(source.result_admission_closed());
        let failure = health.failed().await;
        assert!(runtime.poll_one(Tick(2)).unwrap());
        assert_eq!(received.load(Ordering::Relaxed), 1);
        assert!(!runtime.poll_one(Tick(2)).unwrap());

        source.close_external();
        source.transport_roles_drained();
        let turn = runtime.claim_host_actor().unwrap();
        let progress = source.progress_retirement(&runtime, &turn, Tick(2), 1);
        assert!(progress.progressed && !progress.retired);
        turn.release(false).unwrap();

        // The exact raw originals remain in health even after the neutral
        // actor input and its original standing position actually retired.
        let mut originals = failure.originals.lock().unwrap();
        assert_eq!(originals.len(), 2);
        let captured = originals[0].downcast_ref::<Arc<i32>>().unwrap();
        assert!(Arc::ptr_eq(captured, &original));
        assert_eq!(
            originals[1].downcast_ref::<&str>(),
            Some(&"secondary original")
        );
        for value in originals.drain(..) {
            drop(ManuallyDrop::into_inner(value));
        }
        drop(originals);

        work.body_exited().unwrap();
        work.captures_exited().unwrap();
        payer.finish().unwrap();
        scope.close_external().unwrap();
        scope.reap_ready(1).unwrap();
        runtime.close_actor_admission(actor).unwrap();
        runtime.seal_actor(actor).unwrap();
        runtime.remove_actor(actor).unwrap();
        ledger.remove_scope(scope.id()).unwrap();
    }
}
