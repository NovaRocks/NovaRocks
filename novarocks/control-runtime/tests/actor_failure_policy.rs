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

#![cfg(not(loom))]

use novarocks_control_runtime::{
    actor::{Actor, ActorConfig, ActorFailurePolicy, StepContext},
    driver::{ActorRuntime, DriverError, InputPhase},
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    effect::{EffectDescriptor, EffectKind},
    identity::{ActionId, ActorId, DimensionId, Generation, ScopeId, Tick, WorkId},
    mailbox::{InputClass, InputEnvelope},
    scope::{WorkAdmission, WorkKind},
};
use std::{
    panic::panic_any,
    process::{Command, Output},
};

const CHILD_MODE: &str = "NR_CONTROL_RUNTIME_FAILURE_POLICY_CHILD";
fn generation() -> Generation {
    Generation::new(1).unwrap()
}
fn actor() -> ActorId {
    ActorId::new(1).unwrap()
}
fn dimension() -> DimensionId {
    DimensionId::new(1).unwrap()
}
fn resources(n: u64) -> ResourceVector {
    ResourceVector::new([(dimension(), n)]).unwrap()
}
fn config() -> ActorConfig {
    ActorConfig {
        mailbox: [1; 4],
        max_batches: 4,
        max_effects_per_step: 2,
        max_dependencies_per_effect: 2,
        turn_budget: 12,
        max_step_work: 2,
    }
}
#[derive(Debug)]
enum Input {
    Panic,
    HostFailure,
}
struct ControlledActor {
    scope: ScopeHandle,
    parent: WorkId,
    tripwire: bool,
    host_failure_seen: bool,
}
// ScopeHandle has no Debug requirement; actor registration never formats a Core.
impl Actor for ControlledActor {
    type Input = Input;
    type Effect = ();
    fn timer_input(_: Tick) -> Input {
        Input::HostFailure
    }
    fn handle(&mut self, _: Tick, _: InputEnvelope, value: Input, step: &mut StepContext<'_, ()>) {
        match value {
            Input::HostFailure => self.host_failure_seen = true,
            Input::Panic => {
                // A real synchronous reservation is still owned by this
                // uncommitted step. Isolation rolls it back; fatal policy
                // must retain it instead of inventing a commit or exit.
                let guard = self
                    .scope
                    .reserve_next(
                        generation(),
                        ReservationResources::for_work(resources(1)),
                        WorkKind::Business,
                        WorkAdmission::Derived(self.parent),
                    )
                    .unwrap();
                step.reservation(guard)
                    .unwrap_or_else(|_| panic!("same original scope"));
                if self.tripwire {
                    panic_any(PanicDropTripwire);
                }
                panic!("controlled unexpected actor invariant");
            }
        }
    }
    fn next_deadline(&self) -> Option<Tick> {
        None
    }
    fn owner_failed(_: ScopeId, _: Generation, _: InputEnvelope, step: &mut StepContext<'_, ()>) {
        step.effect(
            EffectDescriptor {
                id: ActionId::new(1).unwrap(),
                work: step.failure_work().expect("same original cleanup role"),
                kind: EffectKind::Cleanup,
                dependencies: vec![],
            },
            (),
        )
        .unwrap();
    }
}
struct PanicDropTripwire;
impl Drop for PanicDropTripwire {
    fn drop(&mut self) {
        // An unintended panic-payload Drop must not accidentally look like a
        // successful fatal branch. This is test-only out-of-band panic data,
        // never a rich payload stored inside the pure actor.
        std::process::exit(73);
    }
}
fn child(mode: &str) {
    let ledger = RuntimeLedger::new(resources(2));
    let scope = ledger.open_scope_next(None, generation()).unwrap();
    let (mut failure, mut capacity) = scope
        .reserve_next(
            generation(),
            ReservationResources::for_work(resources(1)),
            WorkKind::Cleanup,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Cleanup)
        .unwrap();
    let runtime = ActorRuntime::new();
    let value = ControlledActor {
        scope: scope.clone(),
        parent: failure.id(),
        tripwire: mode == "fatal",
        host_failure_seen: false,
    };
    if mode == "isolate" {
        runtime
            .register_supervised_next(actor(), scope.clone(), value, config(), &failure)
            .unwrap_or_else(|(error, _)| panic!("default registration: {error:?}"));
    } else {
        runtime
            .register_supervised_next_with_policy(
                actor(),
                scope.clone(),
                value,
                config(),
                &failure,
                ActorFailurePolicy::ProcessFailStop,
            )
            .unwrap_or_else(|(error, _)| panic!("explicit registration: {error:?}"));
    }
    let command = if mode == "host-error" {
        Input::HostFailure
    } else {
        Input::Panic
    };
    let input = runtime
        .reserve_input(actor(), actor(), InputClass::Ordinary)
        .unwrap()
        .fill(command)
        .unwrap_or_else(|_| panic!("actual reserved neutral input"));
    if mode == "fatal" {
        // A broken policy falling back to scope isolation must fail by a
        // distinct normal exit, never unwind a live Inbox and yield a false
        // positive SIGABRT from its strict Drop assertion.
        let returned =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| runtime.poll_one(Tick(1))));
        std::process::exit(if returned.is_ok() { 75 } else { 77 });
    }
    runtime.poll_one(Tick(1)).unwrap();
    if mode == "isolate" {
        assert!(runtime.snapshot(actor()).unwrap().failed);
        assert!(scope.gates().unwrap().failed);
        assert_eq!(
            ledger.used(dimension()),
            1,
            "only the pure unbound step guard rolled back"
        );
        assert_eq!(scope.snapshot().unwrap().work, 1);
        assert_eq!(
            runtime
                .input_custody(actor(), input)
                .unwrap()
                .unwrap()
                .phase,
            InputPhase::AbandonedByOwnerFailure
        );
        let novarocks_control_runtime::driver::FailureSourceState::ClosedPending(supervisor) =
            runtime.failure_source_state(actor()).unwrap()
        else {
            panic!("the actual isolated failure owns its builtin supervisor input");
        };
        assert_ne!(supervisor, input);
        // The triggering request/source can retire while the original
        // supervisor action is still pending. Its own storage is not borrowed
        // by the cleanup which must wait for that request's physical exit.
        runtime.retire_input(actor(), input).unwrap();
        assert!(runtime.input_custody(actor(), input).unwrap().is_none());
        assert_eq!(
            runtime.pending_batches(actor(), None, 2).unwrap(),
            [supervisor]
        );
        let payload = runtime.take_failure_payload(actor()).unwrap().unwrap();
        assert!(payload.downcast_ref::<&str>().is_some());
        drop(payload); // Actual simple panic value is disposed outside all runtime locks.
        let ready = runtime.ready_effects(actor(), supervisor, 2).unwrap();
        assert_eq!(ready.len(), 1);
        let ticket = runtime.take_effect(actor(), supervisor, ready[0]).unwrap();
        runtime.effect_completed(ticket).unwrap();
        runtime.retire_input(actor(), supervisor).unwrap();
    } else {
        assert_eq!(mode, "host-error");
        assert!(!runtime.snapshot(actor()).unwrap().failed);
        assert!(!scope.gates().unwrap().failed);
        assert!(runtime.take_failure_payload(actor()).unwrap().is_none());
    }
    if mode != "isolate" {
        runtime.retire_input(actor(), input).unwrap();
    }
    runtime.close_actor_admission(actor()).unwrap();
    scope.close_external().unwrap();
    while let Some(original_host_turn) = runtime.claim_host_actor() {
        original_host_turn.release(false).unwrap();
    }
    runtime.seal_actor(actor()).unwrap();
    failure.body_exited().unwrap();
    failure.captures_exited().unwrap();
    ledger.reap_scope_ready(scope.id(), 2).unwrap();
    let actual = runtime.remove_actor(actor()).unwrap();
    if mode == "host-error" {
        assert!(actual.host_failure_seen);
    }
    drop(actual);
    capacity
        .release_quota(&capacity.resources().unwrap().quota)
        .unwrap();
    capacity.finish().unwrap();
    ledger.remove_scope(scope.id()).unwrap();
    assert!(runtime.is_empty());
    assert!(ledger.is_empty());
}
#[test]
fn failure_policy_subprocess_entry() {
    let Ok(mode) = std::env::var(CHILD_MODE) else {
        return;
    };
    child(&mode);
}
fn run_child(mode: &str) -> Output {
    Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "failure_policy_subprocess_entry", "--nocapture"])
        .env(CHILD_MODE, mode)
        .output()
        .unwrap()
}
#[test]
fn default_registration_isolates_only_the_original_scope_and_retains_its_failure() {
    let output = run_child("isolate");
    assert!(
        output.status.success(),
        "child {:?}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
}
#[test]
#[cfg(unix)]
fn explicit_process_policy_aborts_without_dropping_the_original_panic_or_isolating_scope() {
    use std::os::unix::process::ExitStatusExt;
    let output = run_child("fatal");
    assert_eq!(
        output.status.signal(),
        Some(6),
        "child {:?}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
}
#[test]
fn a_typed_host_failure_is_an_input_result_not_an_unexpected_process_crash() {
    let output = run_child("host-error");
    assert!(
        output.status.success(),
        "child {:?}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
}
#[test]
fn explicit_policy_still_rejects_a_foreign_original_supervisor_with_equal_numeric_ids() {
    let ledger = RuntimeLedger::new(resources(1));
    let scope = ledger.open_scope_next(None, generation()).unwrap();
    let other = RuntimeLedger::new(resources(1));
    let foreign_scope = other.open_scope_next(None, generation()).unwrap();
    let (mut work, mut capacity) = scope
        .reserve_next(
            generation(),
            ReservationResources::for_work(resources(1)),
            WorkKind::Cleanup,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Cleanup)
        .unwrap();
    let (mut foreign, mut foreign_capacity) = foreign_scope
        .reserve_next(
            generation(),
            ReservationResources::for_work(resources(1)),
            WorkKind::Cleanup,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Cleanup)
        .unwrap();
    assert_eq!(scope.id(), foreign_scope.id());
    assert_eq!(work.id(), foreign.id());
    let runtime = ActorRuntime::new();
    let actor_value = ControlledActor {
        scope: scope.clone(),
        parent: work.id(),
        tripwire: false,
        host_failure_seen: false,
    };
    let (error, original) = runtime
        .register_supervised_next_with_policy(
            actor(),
            scope.clone(),
            actor_value,
            config(),
            &foreign,
            ActorFailurePolicy::ProcessFailStop,
        )
        .unwrap_err();
    assert_eq!(error, DriverError::PublicationTargetMismatch);
    assert_eq!(scope.snapshot().unwrap().storage, 0);
    assert_eq!(foreign_scope.snapshot().unwrap().storage, 0);
    assert_eq!(ledger.used(dimension()), 1);
    assert_eq!(other.used(dimension()), 1);
    drop(original);
    work.body_exited().unwrap();
    work.captures_exited().unwrap();
    foreign.body_exited().unwrap();
    foreign.captures_exited().unwrap();
    capacity
        .release_quota(&capacity.resources().unwrap().quota)
        .unwrap();
    capacity.finish().unwrap();
    foreign_capacity
        .release_quota(&foreign_capacity.resources().unwrap().quota)
        .unwrap();
    foreign_capacity.finish().unwrap();
    ledger.reap_scope_ready(scope.id(), 1).unwrap();
    other.reap_scope_ready(foreign_scope.id(), 1).unwrap();
    scope.close_external().unwrap();
    foreign_scope.close_external().unwrap();
    ledger.remove_scope(scope.id()).unwrap();
    other.remove_scope(foreign_scope.id()).unwrap();
    assert!(runtime.is_empty());
    assert!(ledger.is_empty());
    assert!(other.is_empty());
}
