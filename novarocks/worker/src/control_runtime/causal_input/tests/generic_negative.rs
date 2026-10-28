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

//! Negative tests retain the original bounded custody on genuine Unknown.

use super::*;

fn original_fees(f: &Fixture) -> [u64; 8] {
    [
        Dimension::ActiveTasks,
        Dimension::Work,
        Dimension::Captures,
        Dimension::Inputs,
        Dimension::Batches,
        Dimension::Effects,
        Dimension::Cuts,
        Dimension::Completions,
    ]
    .map(|dimension| f.runtime.ledger.used(dimension.id()))
}

#[test]
fn losing_a_generic_author_keeps_the_original_unknown_without_unused_or_quota_release() {
    // A lost original authority cannot be honestly cleaned up. Retain the same
    // finite fixture inventory, as the existing bound-completion loss test does.
    let mut f = ManuallyDrop::new(fixture_with_author(true));
    let funded = original_fees(&f);
    let cut = f.custody.cut();
    drop(f.author.take().unwrap());

    assert_eq!(f.custody.phase(), ExitRolePhase::Unresolved);
    assert!(f.custody.0.sender.lock().unwrap().is_some());
    assert!(!f.ingress.event_unused(cut).unwrap());
    assert!(f.inbox.deliver_next().unwrap().is_none());
    assert!(!f.custody.actual_exit_ready().unwrap());
    assert!(!f.custody.try_retire().unwrap());
    assert_eq!(original_fees(&f), funded);
    assert!(f.health.failure().is_some());
    assert!(f.pending.0.load(Ordering::SeqCst) > 0);
    let retained = Arc::downgrade(&f.custody.0);
    drop(f.custody.clone());
    assert_eq!(
        retained.upgrade().unwrap().role.phase(),
        ExitRolePhase::Unresolved
    );
}

#[test]
fn a_wrong_task_handoff_retains_the_exact_generic_input_and_original_unknown() {
    let mut f = ManuallyDrop::new(fixture_with_author(true));
    let funded = original_fees(&f);
    let cut = f.custody.cut();
    let wrong_task = task(f.runtime.identity, 2);
    assert_ne!(wrong_task, f.custody.task());
    let author = f.author.take().unwrap();
    let refusal = author.publish(ContextInput::Running { task: wrong_task });

    assert!(matches!(refusal, Err(CausalInputError::HandoffRejected)));
    assert!(
        matches!(f.custody.take_rejected(), Some(ContextInput::Running { task }) if task == wrong_task)
    );
    assert!(f.custody.take_rejected().is_none());
    assert_eq!(f.custody.phase(), ExitRolePhase::Unresolved);
    assert!(!f.ingress.event_unused(cut).unwrap());
    assert!(f.inbox.deliver_next().unwrap().is_none());
    assert!(!f.custody.actual_exit_ready().unwrap());
    assert!(!f.custody.try_retire().unwrap());
    assert_eq!(original_fees(&f), funded);
    assert!(f.health.failure().is_some());
}

#[test]
fn another_real_core_cannot_consume_a_generic_authors_original_core_leg() {
    let mut f = fixture_with_author(true);
    let mut other = fixture_with_author(true);
    // Name the original Task explicitly, so rejection must still distinguish
    // the other actual ingress instead of relying only on numeric Task IDs.
    // This empty alias owns no input, cut or Work in either Core.
    let mut wrong_core = TaskCausalInbox::new(
        other.runtime.runtime.clone(),
        actor(),
        ActorId::new(7002).unwrap(),
        f.custody.task(),
        generation(),
        other.ingress.clone(),
        other.health.clone(),
    );
    let funded = original_fees(&f);
    let other_funded = original_fees(&other);
    f.author.take().unwrap().withdraw().unwrap();
    assert!(f.custody.actual_exit_ready().unwrap());
    assert_eq!(
        f.custody.report().consumption(core()).unwrap(),
        Consumption::Pending
    );
    assert_eq!(
        f.custody.report().consumption(observer()).unwrap(),
        Consumption::Pending
    );

    assert!(matches!(
        f.custody.observe_consumers(&mut wrong_core),
        Err(CausalInputError::Causal(CausalError::WrongTask))
    ));
    assert_eq!(
        f.custody.report().consumption(core()).unwrap(),
        Consumption::Pending
    );
    assert_eq!(
        f.custody.report().consumption(observer()).unwrap(),
        Consumption::Consumed
    );
    assert_eq!(f.custody.phase(), ExitRolePhase::Exited);
    assert!(!f.custody.try_retire().unwrap());
    assert_eq!(original_fees(&f), funded);
    assert_eq!(original_fees(&other), other_funded);
    assert!(wrong_core.is_empty());
    drop(wrong_core);

    // Both original authors really withdraw their own never-sent inputs. Each
    // actual owner consumes its own revocation/report before its cut retires.
    let cut = f.custody.cut();
    assert!(f.custody.observe_consumers(&mut f.inbox).unwrap());
    f.ingress.observe_cut_exit(cut, true, true).unwrap();
    f.retire_cut(cut);
    let other_cut = other.custody.cut();
    other.author.take().unwrap().withdraw().unwrap();
    assert!(other.custody.observe_consumers(&mut other.inbox).unwrap());
    other
        .ingress
        .observe_cut_exit(other_cut, true, true)
        .unwrap();
    other.retire_cut(other_cut);
    assert!(f.health.failure().is_none());
    assert!(other.health.failure().is_none());
    f.finish();
    other.finish();
}

#[test]
fn a_start_permit_dropped_after_scope_failure_withdraws_only_its_original_unused_role() {
    let mut f = fixture_with_author(true);
    let cut = f.custody.cut();
    let start =
        crate::TaskStartPermit::from_admitted_author(f.author.take().unwrap(), generation());
    let funded = original_fees(&f);
    let failed_generation = f.runtime.scope.fail().unwrap();
    assert!(failed_generation > generation());
    drop(start);

    // This is a true unused Running input, not evidence that the failed Task,
    // its loader/preparation bodies or the cut's Source holders have exited.
    assert!(f.ingress.event_unused(cut).unwrap());
    assert!(f.inbox.deliver_next().unwrap().is_none());
    assert_eq!(f.custody.phase(), ExitRolePhase::Exited);
    assert!(f.custody.actual_exit_ready().unwrap());
    assert!(!f.custody.try_retire().unwrap());
    assert_eq!(original_fees(&f), funded);
    assert_eq!(
        f.custody.report().consumption(core()).unwrap(),
        Consumption::Pending
    );
    assert_eq!(
        f.custody.report().consumption(observer()).unwrap(),
        Consumption::Pending
    );
    assert!(
        f.ingress
            .pair_inventory(PairRoute::Cut(cut))
            .unwrap()
            .iter()
            .all(|position| { !position.custody.actual_exit_ready().unwrap() })
    );
    assert!(
        f.runtime
            .jobs
            .values()
            .any(|(work, _)| !work.actual_exit_ready().unwrap())
    );
    let still_failed = f.runtime.scope.snapshot().unwrap();
    assert!(still_failed.failed);
    assert!(!still_failed.join_ready);

    // Only the original unused event's proved revocation and actual author
    // exit authorize its own report legs. Every other original remains held.
    assert!(f.custody.observe_consumers(&mut f.inbox).unwrap());
    f.ingress.observe_cut_exit(cut, true, true).unwrap();
    f.retire_cut(cut);
    assert_eq!(f.custody.phase(), ExitRolePhase::Retired);
    assert!(f.inbox.is_empty());
    assert!(f.runtime.scope.gates().unwrap().failed);
    assert!(!f.runtime.scope.join_ready().unwrap());
    assert_eq!(
        f.runtime.ledger.used(Dimension::ActiveTasks.id()) + 1,
        funded[0]
    );
    assert!(f.health.failure().is_none());
    // The standard fixture now supplies the remaining actual unused Source
    // exits and real job/body/capture exits; scope failure supplied none.
    f.finish();
}

#[test]
fn a_late_start_handoff_after_scope_failure_retains_running_without_core_consumption() {
    let mut f = ManuallyDrop::new(fixture_with_author(true));
    let task = f.custody.task();
    let cut = f.custody.cut();
    let start =
        crate::TaskStartPermit::from_admitted_author(f.author.take().unwrap(), generation());
    let funded = original_fees(&f);
    f.runtime.scope.fail().unwrap();

    // Handoff seals the original Task's prefix without consulting current
    // scope liveness. The paid runtime input has its separate failure fence.
    start.handoff_running().unwrap();
    assert!(!f.ingress.event_unused(cut).unwrap());
    assert_eq!(f.custody.phase(), ExitRolePhase::Exited);
    assert!(f.custody.actual_exit_ready().unwrap());
    assert!(matches!(
        f.inbox.deliver_next(),
        Err(CausalError::Driver(
            novarocks_control_runtime::driver::DriverError::ActorFailed
        ))
    ));
    assert!(!f.inbox.event_core_consumed(cut).unwrap());
    // The actual observer can confirm this original author's physical exit;
    // that does not acknowledge the Running input to the failed Core.
    {
        let original = &mut *f;
        assert!(
            !original
                .custody
                .observe_consumers(&mut original.inbox)
                .unwrap()
        );
    }
    assert_eq!(
        f.custody.report().consumption(observer()).unwrap(),
        Consumption::Consumed
    );
    assert_eq!(
        f.custody.report().consumption(core()).unwrap(),
        Consumption::Pending
    );
    assert!(!f.custody.try_retire().unwrap());
    assert_eq!(original_fees(&f), funded);
    assert!(!f.inbox.is_empty());
    let rejected = f
        .inbox
        .take_rejected(cut)
        .expect("the failed runtime returns this original neutral input");
    assert!(matches!(
        &rejected,
        (None, WorkerMessage::Context(ContextMessage::Command {
            request: None,
            command: ContextInput::Running { task: original_task },
            ..
        })) if *original_task == task
    ));
    assert!(!f.inbox.event_core_consumed(cut).unwrap());
    assert!(f.runtime.scope.gates().unwrap().failed);
    assert!(!f.runtime.scope.join_ready().unwrap());
    // There is no failure-publication-drained proof or S8 takeover here.
    // Retain the exact failed Core input and its same finite owner together;
    // do not fabricate a CompletionExited, abandon or successful cleanup.
    let _retained_original_failure = ManuallyDrop::new((f, rejected));
}

#[test]
fn an_original_business_start_is_refused_after_stop_or_failure_without_fabricating_running() {
    for failed in [false, true] {
        let mut f = fixture_with_role(true, WorkKind::Business);
        let task = f.custody.task();
        let cut = f.custody.cut();
        let permit =
            crate::TaskStartPermit::from_admitted_author(f.author.take().unwrap(), generation());
        let funded = original_fees(&f);
        if failed {
            f.runtime.scope.fail().unwrap();
        } else {
            f.runtime.scope.request_stop().unwrap();
        }
        let (error, permit) = match permit.authorize_running() {
            Err(crate::TaskStartRefusal::Start { error, permit }) => (Some(error), permit),
            Err(crate::TaskStartRefusal::Stopped { permit }) => (None, permit),
            other => panic!(
                "this real stopped/failed Business start must retain its original permit: {other:?}"
            ),
        };
        assert_eq!(permit.identity(), task);
        assert_eq!(f.custody.phase(), ExitRolePhase::Admitted);
        assert_eq!(original_fees(&f), funded);
        assert!(!f.ingress.event_unused(cut).unwrap());
        assert!(f.inbox.deliver_next().unwrap().is_none());
        assert!(!f.custody.actual_exit_ready().unwrap());
        if failed {
            assert!(matches!(
                error,
                Some(ExitRoleError::Runtime(RuntimeError::Scope(
                    novarocks_control_runtime_core::scope::ScopeError::GenerationMismatch
                )))
            ));
        } else {
            assert!(error.is_none());
        }
        // The exact refused original never started. Its explicit withdrawal
        // supplies no Running, stopped-runtime or shared scope exit fact.
        drop(permit);
        assert!(f.ingress.event_unused(cut).unwrap());
        assert!(f.custody.observe_consumers(&mut f.inbox).unwrap());
        f.ingress.observe_cut_exit(cut, true, true).unwrap();
        f.retire_cut(cut);
        assert!(!f.runtime.scope.join_ready().unwrap());
        f.finish();
    }
}
