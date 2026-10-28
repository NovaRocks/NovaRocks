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
