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

//! One real Core/Host interleaving at the end of a finite Source pass.
use super::super::*;
use crate::fixtures::{
    AttemptScript, FrontendFixture, LogicalScript, ProgressBudget, ProgressSelection,
};
use novarocks_control_runtime::actor_turn::InputPhase;
use novarocks_control_runtime_core::identity::ActorId;
use novarocks_coordination_contract::{
    AttemptActivationIdentity, AttemptConvergenceClosure,
    logical_port::{AttemptTerminal, AttemptToLogicalBody},
};
use novarocks_logical_execution_protocol::effect::{LogicalEffectBody, NativeEffect};
use std::{cell::RefCell, sync::Mutex};

#[derive(Default)]
struct Observation {
    activation: Option<AttemptActivationIdentity>,
    attempt: Option<ActorId>,
    armed: bool,
    input: Option<InputId>,
    startup_quiescent: bool,
    startup_detail: String,
    partial_busy_cut: bool,
    link_visited_busy: bool,
    completed_busy_pass: bool,
    committed_empty_batch: bool,
    indexed: bool,
    retired_without_unrelated_wake: bool,
    handoff: Option<Arc<handoff_credit::LaunchGate>>,
}
thread_local! {
    // Only the synchronous assembly below takes this one-shot test observer.
    // Unrelated tests and subsequent installations keep their original Source.
    static NEXT: RefCell<Option<Arc<Mutex<Observation>>>> = const { RefCell::new(None) };
}

pub(super) fn wrap_source(original: LogicalEffectSource) -> Box<dyn EffectSource> {
    let gate = NEXT.with(|next| {
        next.borrow()
            .as_ref()
            .and_then(|observation| observation.lock().unwrap().handoff.clone())
    });
    if let Some(gate) = gate {
        gate.bind_wake(original.wake.clone());
    }
    match NEXT.with(|next| next.borrow_mut().take()) {
        Some(observed) => Box::new(ObservedSource { original, observed }),
        None => Box::new(original),
    }
}
struct ObservedSource {
    original: LogicalEffectSource,
    observed: Arc<Mutex<Observation>>,
}
impl EffectSource for ObservedSource {
    fn prepay_result(&mut self, effect: &FrontendEffect) -> Option<Position> {
        self.original.prepay_result(effect)
    }
    fn bind_job(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        turn: &ActorHostTurn<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        result: Position,
    ) -> PreparedEffectJob {
        self.original.bind_job(runtime, turn, ticket, result)
    }
    fn try_bind_job(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        turn: &ActorHostTurn<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        result: Position,
    ) -> Result<PreparedEffectJob, EffectBindRefusal> {
        self.original.try_bind_job(runtime, turn, ticket, result)
    }
    fn retain_transport_failure(&mut self, original: Box<dyn std::any::Any + Send>) {
        self.original.retain_transport_failure(original);
    }
    fn retain_transport_release(&mut self, original: Box<dyn std::any::Any + Send>) {
        self.original.retain_transport_release(original);
    }
    fn return_unclaimed_result(&mut self, original: Position) {
        self.original.return_unclaimed_result(original);
    }
    fn core_consumed(&mut self, inputs: &[InputId]) {
        self.original.core_consumed(inputs);
        let gate = self.observed.lock().unwrap().handoff.clone();
        if let Some(gate) = gate {
            gate.indexed(inputs);
        }
        let mut observed = self.observed.lock().unwrap();
        if observed.completed_busy_pass
            && observed.input.is_some_and(|input| inputs.contains(&input))
        {
            observed.indexed = true;
        }
    }
    fn owner_failed_binding(
        &mut self,
        ticket: EffectTicket<FrontendEffect>,
        refused: EffectBindRefusal,
    ) -> Option<crate::control_runtime::host::OriginalUntrackedTransport> {
        self.original.owner_failed_binding(ticket, refused)
    }
    fn owner_failed_pending(&mut self, original: PendingHandoff) {
        self.original.owner_failed_pending(original);
    }
    fn owner_failed_cleanup(
        &mut self,
        ticket: EffectTicket<FrontendEffect>,
        context: Option<crate::task_execution::host::failure_publication::FailureContext>,
        observations: Option<
            crate::task_execution::host::observations::host_turn::OrderedObservationFailure,
        >,
    ) {
        self.original
            .owner_failed_cleanup(ticket, context, observations);
    }
    fn close_external(&mut self) {
        self.original.close_external();
    }
    fn result_admission_closed(&self) -> bool {
        self.original.result_admission_closed()
    }
    fn progress_retirement(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        turn: &ActorHostTurn<FrontendActor>,
        now: Tick,
        budget: usize,
    ) -> SourceProgress {
        let target = {
            let observed = self.observed.lock().unwrap();
            observed
                .armed
                .then_some((observed.activation, observed.attempt))
        };
        let original_input = target.and_then(|(activation, attempt)| {
            let endpoint = self.original.links.endpoint(activation?)?;
            let attempt = attempt?;
            let actual = runtime
                .input_inventory(self.original.registration.actor(), None, usize::MAX)
                .unwrap()
                .into_iter()
                .find(|input| input.source == attempt)?;
            Some((actual.input, actual.phase, endpoint))
        });
        let cursor = self.original.cursor;
        let before_remaining = if self.original.pass_active {
            self.original.remaining
        } else {
            self.original.purposes.len()
        };
        let link_keys = self
            .original
            .purposes
            .iter()
            .enumerate()
            .filter_map(|(key, purpose)| purpose.activation_link.map(|_| key))
            .collect::<Vec<_>>();
        let gate = self.observed.lock().unwrap().handoff.clone();
        if let Some(gate) = &gate {
            gate.visit(&self.original, runtime, false, false);
        }
        let progress = self
            .original
            .progress_retirement(runtime, turn, now, budget);
        let visits = before_remaining.saturating_sub(self.original.remaining);
        let visited_link =
            (0..visits).any(|n| link_keys.contains(&((cursor + n) % self.original.purposes.len())));
        if let Some(gate) = &gate {
            gate.visit(
                &self.original,
                runtime,
                visited_link,
                !self.original.pass_active
                    && self.original.remaining == 0
                    && self.original.input_pass_done
                    && self.original.tail_pass_done
                    && self.original.header_remaining == 0
                    && !progress.more,
            );
        }
        let mut observed = self.observed.lock().unwrap();
        // Purpose routing owns these original slots: first-attempt Run is 1
        // and the Source itself owns bootstrap_purpose. Both are deliberately
        // held. The original Activate job at 0 has really exited, but its
        // child AP still pins custody (LogicalWorkObligation::retire). Its
        // result and ActionExited must already be consumed, independently of
        // that honest child-held header. Every other job must be gone.
        observed.startup_detail = format!(
            "live_jobs={}, live_authors={}, pending={}, pending_headers={}, pass_active={}, remaining={}, cursor={}, input_done={}, tail_done={}, header_remaining={}, rescan={}, more={}, jobs={:?}",
            self.original.live_jobs,
            self.original.live_authors,
            self.original.pending.is_some(),
            self.original.pending_headers.len(),
            self.original.pass_active,
            self.original.remaining,
            self.original.cursor,
            self.original.input_pass_done,
            self.original.tail_pass_done,
            self.original.header_remaining,
            self.original.rescan_requested,
            progress.more,
            self.original
                .purposes
                .iter()
                .enumerate()
                .filter_map(|(key, purpose)| {
                    purpose.job.as_ref().map(|job| {
                        (
                            key,
                            job.actual_exit(),
                            job.custody_retired(),
                            job.result_action(),
                            purpose.activation_link,
                            purpose.exit_filled,
                            purpose.result.as_ref().map(Position::in_flight),
                            purpose.exit.as_ref().map(Position::in_flight),
                        )
                    })
                })
                .collect::<Vec<_>>()
        );
        observed.startup_quiescent = !self.original.pass_active
            && self.original.remaining == 0
            && self.original.input_pass_done
            && self.original.tail_pass_done
            && self.original.header_remaining == 0
            && !progress.more
            && self.original.live_jobs == 3
            && self.original.pending.is_none()
            && self.original.pending_headers.is_empty()
            && self
                .original
                .purposes
                .iter()
                .enumerate()
                .all(|(key, purpose)| {
                    if key == 0 {
                        purpose.activation_link.is_some()
                            && purpose.exit_filled
                            && purpose
                                .job
                                .as_ref()
                                .is_some_and(|job| job.actual_exit() && !job.custody_retired())
                            && [purpose.result.as_ref(), purpose.exit.as_ref()]
                                .into_iter()
                                .all(|position| {
                                    position.is_some_and(|position| {
                                        position.in_flight().is_ok_and(|inputs| {
                                            inputs.into_iter().all(|input| input.is_none())
                                        })
                                    })
                                })
                    } else if key == 1 || key == self.original.bootstrap_purpose {
                        purpose.job.as_ref().is_some_and(|job| !job.actual_exit())
                    } else {
                        purpose.job.is_none()
                    }
                });
        if observed.armed
            && !observed.partial_busy_cut
            && let Some((input, InputPhase::Filled, endpoint)) = &original_input
            && visited_link
            && self.original.pass_active
            && self.original.remaining > 0
            && !self.original.rescan_requested
            && self.original.input_pass_done
            && self.original.tail_pass_done
            && self.original.header_remaining == 0
        {
            // This is an actual Full request: it registers the AP's waiter.
            // The real bank's third role visit has finished port.more, while
            // the unchanged production geometry still owes other purposes.
            let busy = endpoint
                .facts
                .try_loan(crate::task_execution::host::port::facts::FactRole::Lifecycle)
                .is_none();
            observed.input = Some(*input);
            observed.partial_busy_cut = busy;
            observed.link_visited_busy = visited_link;
        }
        if observed.armed
            && observed.partial_busy_cut
            && observed.committed_empty_batch
            && !self.original.pass_active
            && !visited_link
        {
            // The real Core committed BETWEEN Host claims. This final Source
            // remainder did not revisit the already-scanned bank, and index
            // still happens after this return. A genuine notification may
            // already owe another pass; that does not extend this old pass.
            // No state or cursor is changed.
            observed.completed_busy_pass = true;
        }
        if observed.indexed
            && observed.armed
            && observed.input.is_some_and(|input| {
                runtime
                    .input_custody(self.original.registration.actor(), input)
                    .is_ok_and(|input| input.is_none())
            })
        {
            observed.retired_without_unrelated_wake = true;
        }
        progress
    }
}

fn budget(actors: usize) -> ProgressBudget {
    ProgressBudget {
        actors,
        hosts: 64,
        timers: 0,
    }
}
fn empty_progress() -> AttemptToLogicalBody {
    AttemptToLogicalBody::ConvergenceProgress {
        local_exited: false,
        closure: None,
        lifecycle_runtime_filters: None,
    }
}

#[tokio::test]
async fn actual_empty_fact_index_reopens_same_bank_after_its_source_pass_finished() {
    let observed = Arc::new(Mutex::new(Observation::default()));
    let mut fixture = FrontendFixture::new();
    let script = LogicalScript::default();
    let start = LogicalScript::start(9351, 0, 3);
    observed.lock().unwrap().activation = Some(start.first_attempt.activation);
    NEXT.with(|next| *next.borrow_mut() = Some(observed.clone()));
    let logical = fixture.install_logical(start, script.clone());
    let attempt = fixture
        .activate_attempt(
            &logical,
            logical.start.first_attempt.activation,
            AttemptScript::default(),
        )
        .await;
    // The original max_attempts=3 geometry has 98 purposes. Its unmodified
    // Host allowance visits about 73 purposes per Source family, so a pass
    // genuinely spans claims; no cursor or budget is manufactured here.
    for _ in 0..256 {
        let drained = fixture.progress(ProgressSelection::All, budget(64)).await;
        if drained.actors == 0
            && drained.hosts == 0
            && observed.lock().unwrap().startup_quiescent
            && fixture.observe(&logical).effects.iter().any(|effect| {
                matches!(
                    effect,
                    LogicalEffectBody::Native(NativeEffect::RunAttempt { .. })
                )
            })
            && fixture
                .runtime()
                .snapshot(logical.registration.actor())
                .unwrap()
                .inputs
                == 0
        {
            break;
        }
    }
    let (startup_quiescent, startup_detail) = {
        let observation = observed.lock().unwrap();
        (
            observation.startup_quiescent,
            observation.startup_detail.clone(),
        )
    };
    let mut quiet = true;
    let mut quiet_visits = Vec::new();
    for _ in 0..8 {
        let progress = fixture.progress(ProgressSelection::All, budget(0)).await;
        quiet_visits.push((progress.actors, progress.hosts));
        quiet &= progress.hosts == 0;
    }
    // Arm before publication, so every actual bank visit while this exact
    // occurrence is Filled is counted, including any producer-scheduled pass.
    {
        let mut observation = observed.lock().unwrap();
        observation.attempt = Some(attempt.registration.actor());
        observation.armed = true;
    }
    let first = attempt.script.enqueue(empty_progress());
    // The AP's actual trigger is the only ready actor. Do not select LP after
    // the real AP Source publishes its first bank fact.
    for _ in 0..64 {
        fixture.progress(ProgressSelection::All, budget(1)).await;
        if attempt.script.published().contains(&first) {
            break;
        }
    }
    // This pre-cut wake starts the finite pass under observation. There are
    // no test notifications after the actual Core commit/index boundary.
    fixture
        .runtime()
        .notify_host_actor(&logical.registration)
        .unwrap();
    for _ in 0..256 {
        fixture
            .progress(
                ProgressSelection::All,
                ProgressBudget {
                    actors: 0,
                    hosts: 1,
                    timers: 0,
                },
            )
            .await;
        if observed.lock().unwrap().partial_busy_cut {
            break;
        }
    }
    let target = observed.lock().unwrap().input;
    // Return the preceding Host claim first. Core's normal notification is
    // now consumed by the NEXT claim, rather than remaining pending inside
    // the same claim and accidentally rescuing the old implementation.
    fixture
        .progress(
            ProgressSelection::All,
            ProgressBudget {
                actors: 64,
                hosts: 0,
                timers: 0,
            },
        )
        .await;
    let committed_empty = target.is_some_and(|input| {
        fixture
            .runtime()
            .input_custody(logical.registration.actor(), input)
            .is_ok_and(|value| value.is_some_and(|value| value.phase == InputPhase::Committed))
            && fixture
                .runtime()
                .effect_inventory(logical.registration.actor(), input, None, 1)
                .is_ok_and(|effects| effects.is_empty())
    });
    observed.lock().unwrap().committed_empty_batch = committed_empty;
    fixture
        .progress(
            ProgressSelection::All,
            ProgressBudget {
                actors: 0,
                hosts: 1,
                timers: 0,
            },
        )
        .await;
    let indexed = observed.lock().unwrap().indexed;
    for _ in 0..32 {
        fixture.progress(ProgressSelection::All, budget(0)).await;
    }
    let reusable = !attempt.occupied().1;
    let retired_before_second = observed.lock().unwrap().retired_without_unrelated_wake;
    let second = attempt.script.enqueue(empty_progress());
    for _ in 0..32 {
        fixture.progress(ProgressSelection::All, budget(1)).await;
        if attempt.script.published().contains(&second) {
            break;
        }
    }
    let second_published = attempt.script.published().contains(&second);
    let (busy_cut, empty_batch, link_visited) = {
        let mut observation = observed.lock().unwrap();
        observation.armed = false;
        (
            observation.completed_busy_pass,
            observation.committed_empty_batch,
            observation.link_visited_busy,
        )
    };
    // Cleanup is a separate phase. Its real gate/result notifications may
    // rescue the red case, but cannot retroactively satisfy the captured cut.
    script.run.open();
    attempt
        .script
        .enqueue(AttemptToLogicalBody::AttemptTerminal(
            AttemptTerminal::Completed,
        ));
    attempt
        .script
        .enqueue(AttemptToLogicalBody::ConvergenceProgress {
            local_exited: true,
            closure: Some(Arc::new(AttemptConvergenceClosure::Evidence(Vec::new()))),
            lifecycle_runtime_filters: None,
        });
    let cut_detail = observed.lock().unwrap().startup_detail.clone();
    fixture.finish().await;
    let oracles = format!(
        "startup_quiescent={startup_quiescent}, quiet={quiet}, quiet_visits={quiet_visits:?}, busy_cut={busy_cut}, empty_batch={empty_batch}, indexed={indexed}, link_visited={link_visited}, reusable={reusable}, retired_before_second={retired_before_second}, second_published={second_published}; startup={startup_detail}; cut={cut_detail}"
    );
    assert!(fixture.runtime().is_empty() && fixture.ledger().is_empty());
    assert!(attempt.exited());
    assert!(
        quiet && startup_quiescent,
        "empty input callbacks and idle visits do not self-wake: {oracles}"
    );
    assert!(
        busy_cut && empty_batch && indexed && link_visited,
        "actual busy-bank Source pass must finish before actual zero-effect commit/index: {oracles}"
    );
    assert!(
        reusable && retired_before_second && second_published,
        "the original nonempty index must return this same bank credit without an unrelated wake: {oracles}"
    );
}

mod handoff_credit;

pub(super) fn wrap_executor(original: Arc<LogicalEffectExecutor>) -> Arc<dyn EffectExecutor> {
    handoff_credit::wrap_executor(original)
}
