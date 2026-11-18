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

//! Actual handoff completion, with result/exit Core indices held downstream.
use super::*;
use novarocks_logical_execution_protocol::effect::ConsumerEffect;
use std::task::Waker;

#[derive(Default)]
struct LaunchState {
    armed: bool,
    available: bool,
    released: bool,
    refused: bool,
    accepted: usize,
    consumer_accepted: bool,
    held: Vec<PreparedEffectJob>,
    wake: Option<Waker>,
    target: Option<InputId>,
    actor: Option<ActorId>,
    activation: Option<AttemptActivationIdentity>,
    parked_full_pass: bool,
    original_full_probe: bool,
    original_indices: usize,
    index_frontier: usize,
    link_before_handoff: bool,
    retirement_ready_before_body: bool,
    retired_before_body: bool,
    loanable_before_body: bool,
}
// Keep every original unlaunched obligation intact if setup itself unwinds.
// This is UNKNOWN custody, never a successful launch/exit/Scope cleanup.
impl Drop for LaunchState {
    fn drop(&mut self) {
        if !self.held.is_empty() {
            let _ = Box::leak(Box::new(std::mem::take(&mut self.held)));
        }
    }
}
#[derive(Default)]
pub(super) struct LaunchGate {
    state: Mutex<LaunchState>,
}
impl LaunchGate {
    pub(super) fn bind_wake(&self, wake: Waker) {
        self.state.lock().unwrap().wake = Some(wake);
    }
    pub(super) fn indexed(&self, inputs: &[InputId]) {
        let mut state = self.state.lock().unwrap();
        if state.armed {
            state.original_indices += inputs.len();
        }
    }
    pub(super) fn visit(
        &self,
        original: &LogicalEffectSource,
        runtime: &ActorRuntime<FrontendActor>,
        visited_link: bool,
        source_pass_complete: bool,
    ) {
        let mut state = self.state.lock().unwrap();
        let (Some(actor), Some(input)) = (state.actor, state.target) else {
            return;
        };
        if state.refused && !state.available && source_pass_complete {
            state.parked_full_pass = true;
            if !state.original_full_probe {
                let endpoint = original
                    .links
                    .endpoint(state.activation.expect("same actual activation"))
                    .expect("same original port endpoint");
                // Full is a real request and registers the original AP waiter.
                state.original_full_probe = endpoint
                    .facts
                    .try_loan(crate::task_execution::host::port::facts::FactRole::Lifecycle)
                    .is_none();
            }
        }
        if state.available
            && state.accepted == 0
            && visited_link
            && runtime.input_retirement_status(actor, input) == Err(ActorTurnError::EffectsRemain)
        {
            // Real Source progress precedes the Host's handoff in this turn.
            // The prior fully quiet pass completed all three original roles.
            state.link_before_handoff = true;
        }
        if state.accepted != 0 && !state.released {
            state.retirement_ready_before_body |=
                runtime.input_retirement_status(actor, input).is_ok();
            state.retired_before_body |= runtime
                .input_custody(actor, input)
                .is_ok_and(|value| value.is_none());
            if state.retired_before_body && !state.loanable_before_body {
                let endpoint = original
                    .links
                    .endpoint(state.activation.expect("same actual activation"))
                    .expect("same original port endpoint");
                // Probe only after actual retirement; no fill or new position.
                state.loanable_before_body = endpoint
                    .facts
                    .try_loan(crate::task_execution::host::port::facts::FactRole::Lifecycle)
                    .is_some();
            }
        }
    }
    fn open_availability(&self) {
        let wake = {
            let mut state = self.state.lock().unwrap();
            state.index_frontier = state.original_indices;
            state.available = true;
            state
                .wake
                .clone()
                .expect("same original host availability registration")
        };
        wake.wake_by_ref();
    }
}

struct HeldLaunchExecutor {
    original: Arc<LogicalEffectExecutor>,
    gate: Arc<LaunchGate>,
}
impl EffectExecutor for HeldLaunchExecutor {
    fn handoff(&self, job: PreparedEffectJob) -> Result<(), PreparedEffectJob> {
        let consumer = matches!(&job.effect, FrontendEffect::Logical(effect)
            if matches!(&effect.body, LogicalHostEffect::Protocol(effect)
                if matches!(effect.body, LogicalEffectBody::Consumer(ConsumerEffect::PublishCompletion))));
        let mut state = self.gate.state.lock().unwrap();
        if !state.armed || state.released {
            drop(state);
            return self.original.handoff(job);
        }
        if !state.available {
            // bind_wake registered the actual original availability waker
            // before this refusal; the full original job stays with Host.
            assert!(state.wake.is_some());
            state.refused = true;
            return Err(job);
        }
        // Acceptance is only transfer into checked custody. CommonHost, not
        // this test, completes the SAME effect ticket after this returns Ok.
        state.consumer_accepted |= consumer;
        state.accepted += 1;
        state.held.push(job);
        Ok(())
    }
}
pub(super) fn wrap_executor(original: Arc<LogicalEffectExecutor>) -> Arc<dyn EffectExecutor> {
    let gate = NEXT.with(|next| {
        next.borrow()
            .as_ref()
            .and_then(|observation| observation.lock().unwrap().handoff.clone())
    });
    match gate {
        Some(gate) => {
            DRAIN.with(|slot| {
                assert!(slot.borrow_mut().replace(original.clone()).is_none());
            });
            Arc::new(HeldLaunchExecutor { original, gate })
        }
        None => original,
    }
}
impl LaunchGate {
    fn release_bodies(&self, original: &dyn EffectExecutor) {
        self.state.lock().unwrap().released = true;
        loop {
            let job = {
                let mut state = self.state.lock().unwrap();
                if state.held.is_empty() {
                    break;
                }
                // The untouched FIFO tail always stays in checked custody.
                state.held.remove(0)
            };
            if let Err(job) = original.handoff(job) {
                // Restore the exact refused head before reporting failure;
                // unwinding cannot drop the still-unlaunched original tail.
                self.state.lock().unwrap().held.insert(0, job);
                panic!("original logical executor refused its retained launch");
            }
        }
    }
}

#[tokio::test]
async fn actual_handoff_completion_returns_same_fact_credit_before_any_job_result_index() {
    let gate = Arc::new(LaunchGate::default());
    let observation = Arc::new(Mutex::new(Observation {
        handoff: Some(gate.clone()),
        ..Observation::default()
    }));
    let mut fixture = FrontendFixture::new();
    let script = LogicalScript::default();
    let start = LogicalScript::start(9361, 0, 3);
    gate.state.lock().unwrap().activation = Some(start.first_attempt.activation);
    NEXT.with(|next| *next.borrow_mut() = Some(observation.clone()));
    let logical = fixture.install_logical(start, script.clone());
    let attempt = fixture
        .activate_attempt(
            &logical,
            logical.start.first_attempt.activation,
            AttemptScript::default(),
        )
        .await;
    for _ in 0..256 {
        let progress = fixture.progress(ProgressSelection::All, budget(64)).await;
        if progress.actors == 0
            && progress.hosts == 0
            && observation.lock().unwrap().startup_quiescent
        {
            break;
        }
    }
    let startup = observation.lock().unwrap().startup_quiescent;
    let mut initially_quiet = true;
    for _ in 0..8 {
        initially_quiet &= fixture
            .progress(ProgressSelection::All, budget(0))
            .await
            .hosts
            == 0;
    }
    gate.state.lock().unwrap().armed = true;
    let action = attempt
        .script
        .enqueue(AttemptToLogicalBody::AttemptTerminal(
            AttemptTerminal::Completed,
        ));
    for _ in 0..64 {
        fixture.progress(ProgressSelection::All, budget(1)).await;
        if attempt.script.published().contains(&action) {
            break;
        }
    }
    let target = fixture
        .runtime()
        .input_inventory(logical.registration.actor(), None, usize::MAX)
        .unwrap()
        .into_iter()
        .find(|input| input.source == attempt.registration.actor())
        .map(|input| input.input);
    {
        let mut state = gate.state.lock().unwrap();
        state.actor = Some(logical.registration.actor());
        state.target = target;
    }
    // Actual LP Core commits the nonempty batch, outside a Host claim.
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
    let nonempty = target.is_some_and(|input| {
        fixture
            .runtime()
            .effect_inventory(logical.registration.actor(), input, None, 32)
            .is_ok_and(|effects| !effects.is_empty())
    });
    let mut blocked_quiet = false;
    for _ in 0..256 {
        let progress = fixture.progress(ProgressSelection::All, budget(0)).await;
        if progress.hosts == 0 {
            blocked_quiet = true;
            break;
        }
    }
    let effects_remain = target.is_some_and(|input| {
        fixture
            .runtime()
            .input_retirement_status(logical.registration.actor(), input)
            == Err(ActorTurnError::EffectsRemain)
    });
    let (refused, parked_full_pass, original_full_probe) = {
        let state = gate.state.lock().unwrap();
        (
            state.refused,
            state.parked_full_pass,
            state.original_full_probe,
        )
    };
    // There are no synthetic Core/index calls or wakes after this single
    // real availability transition. Every accepted body remains unlaunched.
    gate.open_availability();
    let mut completion_quiet = false;
    for _ in 0..256 {
        let progress = fixture.progress(ProgressSelection::All, budget(0)).await;
        if progress.hosts == 0 {
            completion_quiet = true;
            break;
        }
    }
    let reusable = !attempt.occupied().1;
    let (accepted, consumer, link_before, ready, retired, loanable, indices, frontier) = {
        let state = gate.state.lock().unwrap();
        (
            state.accepted,
            state.consumer_accepted,
            state.link_before_handoff,
            state.retirement_ready_before_body,
            state.retired_before_body,
            state.loanable_before_body,
            state.original_indices,
            state.index_frontier,
        )
    };
    // Read the SAME original live executor already held by the test wrapper.
    // This test hook retains that Arc only for this explicit actual drain.
    let executor = DRAIN
        .with(|slot| slot.borrow_mut().take())
        .expect("same original executor drain");
    gate.release_bodies(executor.as_ref());
    script.run.open();
    attempt
        .script
        .enqueue(AttemptToLogicalBody::ConvergenceProgress {
            local_exited: true,
            closure: Some(Arc::new(AttemptConvergenceClosure::Evidence(Vec::new()))),
            lifecycle_runtime_filters: None,
        });
    fixture.finish().await;
    let detail = format!(
        "startup={startup}, quiet={initially_quiet}, nonempty={nonempty}, refused={refused}, blocked_quiet={blocked_quiet}, parked_full_pass={parked_full_pass}, original_full_probe={original_full_probe}, effects_remain={effects_remain}, accepted={accepted}, consumer={consumer}, link_before={link_before}, ready={ready}, retired={retired}, loanable={loanable}, reusable={reusable}, completion_quiet={completion_quiet}, indices={indices}/{frontier}"
    );
    assert!(fixture.runtime().is_empty() && fixture.ledger().is_empty() && attempt.exited());
    assert!(
        startup
            && initially_quiet
            && nonempty
            && refused
            && blocked_quiet
            && parked_full_pass
            && original_full_probe
            && effects_remain,
        "real nonempty input must park behind the original handoff availability: {detail}"
    );
    assert!(
        accepted > 0 && consumer && link_before && indices == frontier && completion_quiet,
        "original handoff completes without any downstream result/exit index: {detail}"
    );
    assert!(
        ready && retired && loanable && reusable,
        "real effect completion must return the same original fact role before any body opens: {detail}"
    );
}
thread_local! {
    static DRAIN: RefCell<Option<Arc<LogicalEffectExecutor>>> = const { RefCell::new(None) };
}
