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

//! Passive malformed vectors test the checker only. Real P11b cases obtain
//! CapturedHistory exclusively by draining their actual HistoryCapture.

use super::*;
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    identity::{ActorId, ConsumerId, Tick, WorkId},
};

fn scope(n: u64) -> ScopeId {
    ScopeId::new(n).unwrap()
}
fn reference() -> ScopedWorkRef {
    ScopedWorkRef {
        scope: scope(1),
        work: WorkId::new(7).unwrap(),
    }
}
fn epoch() -> Generation {
    Generation::new(1).unwrap()
}
fn target() -> HistoryActorTarget {
    HistoryActorTarget {
        actor: ActorId::new(11).unwrap(),
        attachment: SlotId::new(3).unwrap(),
        scope_origin: scope(1),
    }
}
fn input() -> InputId {
    InputId::new(4).unwrap()
}
fn vector(facts: Vec<HistoryFact>) -> CapturedHistory {
    CapturedHistory {
        slice: HistorySlice {
            entries: facts
                .into_iter()
                .enumerate()
                .map(|(index, fact)| HistoryEntry {
                    ordinal: index as u64 + 1,
                    now: Tick(0),
                    fact,
                })
                .collect(),
            complete: true,
            remaining: 0,
            error: None,
        },
    }
}
fn work_facts() -> Vec<HistoryFact> {
    let ScopedWorkRef { scope, work } = reference();
    vec![
        HistoryFact::ScopeOpened {
            scope,
            generation: epoch(),
            parent: None,
        },
        HistoryFact::WorkAdmitted {
            scope,
            work,
            parent: None,
        },
        HistoryFact::Started { scope, work },
        HistoryFact::BodyExited { scope, work },
        HistoryFact::CapturesExited { scope, work },
        HistoryFact::WorkRetired { scope, work },
    ]
}

#[test]
fn duplicate_original_start_and_reversed_capture_exit_are_rejected() {
    assert_eq!(
        vector(work_facts())
            .check_work(reference(), WorkExpectation::Retired)
            .unwrap(),
        WorkObservation::Retired
    );
    let mut duplicate = work_facts();
    duplicate.insert(3, duplicate[2].clone());
    assert_eq!(
        vector(duplicate)
            .check_work(reference(), WorkExpectation::Retired)
            .unwrap_err()
            .rule,
        "one original Work start"
    );
    let mut reversed = work_facts();
    reversed.swap(3, 4);
    assert_eq!(
        vector(reversed)
            .check_work(reference(), WorkExpectation::Retired)
            .unwrap_err()
            .rule,
        "BodyExited before CapturesExited"
    );
}

#[test]
fn lossless_unfinished_prefix_requires_explicit_unknown_and_cannot_pass_retirement() {
    let mut facts = work_facts();
    facts.truncate(4);
    let trace = vector(facts);
    trace.check_capture().unwrap();
    assert!(
        trace
            .check_work(reference(), WorkExpectation::Retired)
            .is_err()
    );
    assert_eq!(
        trace
            .check_work(reference(), WorkExpectation::Unknown)
            .unwrap(),
        WorkObservation::Unknown {
            started: true,
            body_exited: true,
            captures_exited: false,
        }
    );
    assert!(
        vector(work_facts())
            .check_work(reference(), WorkExpectation::Unknown)
            .is_err()
    );
    let runtime = ActorRuntime::<FrontendActor>::new();
    let ledger = RuntimeLedger::new(ResourceVector::default());
    assert!(
        trace.check_completed_runtime(&runtime, &ledger).is_err(),
        "empty external owners do not invent missing history retirement"
    );
}

#[test]
fn unused_revoked_report_needs_actual_exits_and_forbids_fake_publication_or_consumers() {
    let ScopedWorkRef { scope, work } = reference();
    let slot = SlotId::new(9).unwrap();
    let facts = vec![
        HistoryFact::WorkAdmitted {
            scope,
            work,
            parent: None,
        },
        HistoryFact::SlotReserved { scope, slot },
        HistoryFact::BodyExited { scope, work },
        HistoryFact::CapturesExited { scope, work },
        HistoryFact::SlotRetired { slot },
        HistoryFact::WorkRetired { scope, work },
    ];
    vector(facts.clone())
        .check_unused_report(reference(), slot)
        .unwrap();
    assert!(
        vector(facts.clone())
            .check_work(reference(), WorkExpectation::Retired)
            .is_err(),
        "unused exit is not Started"
    );
    // Preserve the same genuine unused-work exits. Only the report's
    // original reservation/retirement order is malformed in this vector.
    let mut late_reservation = facts.clone();
    let reserved = late_reservation.remove(1);
    late_reservation.push(reserved);
    assert_eq!(
        vector(late_reservation)
            .check_unused_report(reference(), slot)
            .unwrap_err()
            .rule,
        "original report reservation before retirement"
    );
    for invented in [
        HistoryFact::SlotFilled { slot },
        HistoryFact::CompletionConsumed {
            slot,
            consumer: ConsumerId::new(1).unwrap(),
        },
    ] {
        let mut wrong = facts.clone();
        wrong.insert(4, invented);
        assert!(
            vector(wrong)
                .check_unused_report(reference(), slot)
                .is_err()
        );
    }
}

#[test]
fn original_result_consumption_before_actual_capture_exit_is_rejected() {
    let mut facts = work_facts();
    facts.push(HistoryFact::InputConsumed {
        target: target(),
        input: input(),
    });
    vector(facts.clone())
        .check_result_after_captures(reference(), target(), input())
        .unwrap();
    let consumed = facts.pop().unwrap();
    facts.insert(4, consumed);
    assert_eq!(
        vector(facts)
            .check_result_after_captures(reference(), target(), input())
            .unwrap_err()
            .rule,
        "CapturesExited before original result consumption"
    );
}

#[test]
fn same_numeric_input_of_a_different_attachment_does_not_satisfy_original_disposition() {
    let other = HistoryActorTarget {
        attachment: SlotId::new(19).unwrap(),
        ..target()
    };
    let trace = vector(vec![HistoryFact::InputConsumed {
        target: other,
        input: input(),
    }]);
    assert!(
        trace
            .check_input(target(), input(), InputDisposition::Consumed)
            .is_err()
    );
    trace
        .check_input(target(), input(), InputDisposition::Unknown)
        .unwrap();
    let ambiguous = vector(vec![
        HistoryFact::InputConsumed {
            target: target(),
            input: input(),
        },
        HistoryFact::InputAbandoned {
            target: target(),
            input: input(),
        },
    ]);
    assert!(
        ambiguous
            .check_input(target(), input(), InputDisposition::Consumed)
            .is_err()
    );
    assert!(
        ambiguous
            .check_input(target(), input(), InputDisposition::Abandoned)
            .is_err()
    );
}

fn effect() -> EffectIdentity {
    EffectIdentity {
        target: target(),
        input: input(),
        action: ActionId::new(2).unwrap(),
        actor_generation: epoch(),
        financial_work: reference(),
        author_generation: epoch(),
    }
}
fn effect_fact(identity: EffectIdentity, phase: EffectHistoryPhase) -> HistoryFact {
    HistoryFact::Effect {
        target: identity.target,
        input: identity.input,
        action: identity.action,
        actor_generation: identity.actor_generation,
        financial_work: identity.financial_work,
        author_generation: identity.author_generation,
        phase,
    }
}
fn effect_facts() -> Vec<HistoryFact> {
    vec![
        HistoryFact::BatchCommitted {
            target: target(),
            input: input(),
            actor_generation: epoch(),
        },
        effect_fact(effect(), EffectHistoryPhase::Dispatched),
        effect_fact(effect(), EffectHistoryPhase::Published),
        effect_fact(effect(), EffectHistoryPhase::Completed),
    ]
}

#[test]
fn effect_publication_order_duplicate_phase_and_changed_financial_identity_are_rejected() {
    vector(effect_facts())
        .check_effect(effect(), EffectExpectation::PublishedThenCompleted)
        .unwrap();
    let mut reversed = effect_facts();
    reversed.swap(2, 3);
    assert!(
        vector(reversed)
            .check_effect(effect(), EffectExpectation::PublishedThenCompleted)
            .is_err()
    );
    let mut duplicate = effect_facts();
    duplicate.push(effect_fact(effect(), EffectHistoryPhase::Completed));
    assert_eq!(
        vector(duplicate)
            .check_effect(effect(), EffectExpectation::Completed)
            .unwrap_err()
            .rule,
        "one original effect phase"
    );
    let changed = EffectIdentity {
        financial_work: ScopedWorkRef {
            work: WorkId::new(99).unwrap(),
            ..reference()
        },
        ..effect()
    };
    let mut wrong = effect_facts();
    wrong[3] = effect_fact(changed, EffectHistoryPhase::Completed);
    assert_eq!(
        vector(wrong)
            .check_effect(effect(), EffectExpectation::Completed)
            .unwrap_err()
            .rule,
        "same original effect actor and financial identity"
    );
    let mut unfinished = effect_facts();
    unfinished.pop();
    let trace = vector(unfinished);
    trace
        .check_effect(effect(), EffectExpectation::Unknown)
        .unwrap();
    assert!(
        trace
            .check_effect(effect(), EffectExpectation::Completed)
            .is_err()
    );
}

#[test]
fn actual_child_removal_and_intrinsic_parent_link_exit_precede_parent_removal() {
    let ScopedWorkRef {
        scope: parent,
        work,
    } = reference();
    let child = scope(2);
    let facts = vec![
        HistoryFact::ScopeOpened {
            scope: parent,
            generation: epoch(),
            parent: None,
        },
        HistoryFact::WorkAdmitted {
            scope: parent,
            work,
            parent: None,
        },
        HistoryFact::ScopeOpened {
            scope: child,
            generation: epoch(),
            parent: Some(reference()),
        },
        HistoryFact::ScopeRemoved { scope: child },
        HistoryFact::BodyExited {
            scope: parent,
            work,
        },
        HistoryFact::CapturesExited {
            scope: parent,
            work,
        },
        HistoryFact::WorkRetired {
            scope: parent,
            work,
        },
        HistoryFact::ScopeRemoved { scope: parent },
    ];
    vector(facts.clone())
        .check_child_scope_removed(child, parent)
        .unwrap();
    let mut wrong = facts;
    let removed = wrong.remove(3);
    wrong.push(removed);
    assert!(
        vector(wrong)
            .check_child_scope_removed(child, parent)
            .is_err()
    );
}

#[test]
fn ordinal_gaps_duplicate_ordinals_and_reversed_time_are_not_complete_history() {
    let mut gap = vector(work_facts());
    gap.slice.entries[2].ordinal += 1;
    assert!(gap.check_capture().is_err());
    let mut duplicate = vector(work_facts());
    duplicate.slice.entries[2].ordinal = duplicate.slice.entries[1].ordinal;
    assert!(duplicate.check_capture().is_err());
    let mut reversed = vector(work_facts());
    reversed.slice.entries[0].now = Tick(3);
    assert!(reversed.check_capture().is_err());
}

#[test]
fn actual_bounded_capture_full_and_undrained_buffer_are_rejected() {
    let full = HistoryCapture::new(1);
    let ledger = RuntimeLedger::with_history(ResourceVector::default(), full.clone());
    let original = ledger.open_scope_next(None, epoch()).unwrap();
    original.close_external().unwrap();
    ledger.remove_scope(original.id()).unwrap();
    let trace = CapturedHistory::drain(&full, 64);
    assert!(!trace.slice.complete);
    assert_eq!(
        trace.slice.error,
        Some(novarocks_control_runtime_core::history::HistoryError::Full)
    );
    assert!(trace.check_capture().is_err());
    assert!(
        trace
            .check_work(reference(), WorkExpectation::Unknown)
            .is_err()
    );
    let original = HistoryCapture::new(32);
    let ledger = RuntimeLedger::with_history(ResourceVector::default(), original.clone());
    let first = ledger.open_scope_next(None, epoch()).unwrap();
    let second = ledger.open_scope_next(None, epoch()).unwrap();
    let partial = CapturedHistory::drain(&original, 1);
    assert!(partial.slice.complete && partial.slice.remaining != 0);
    assert_eq!(
        partial.check_capture().unwrap_err().rule,
        "capture not drained"
    );
    for scope in [first, second] {
        scope.close_external().unwrap();
        ledger.remove_scope(scope.id()).unwrap();
    }
    assert!(ledger.is_empty());
}

#[test]
fn actual_unbound_reservation_rollback_is_distinct_from_started_or_unused_bound_exit() {
    use novarocks_control_runtime_core::{
        capacity::ReservationResources,
        scope::{WorkAdmission, WorkKind},
    };
    let capture = HistoryCapture::new(64);
    let ledger = RuntimeLedger::with_history(ResourceVector::default(), capture.clone());
    let scope = ledger.open_scope_next(None, epoch()).unwrap();
    let registration = scope
        .reserve_next(
            epoch(),
            ReservationResources::work_only(),
            WorkKind::Business,
            WorkAdmission::External,
        )
        .unwrap();
    let original = ScopedWorkRef {
        scope: scope.id(),
        work: registration.work_id(),
    };
    drop(registration);
    scope.close_external().unwrap();
    ledger.remove_scope(scope.id()).unwrap();
    let history = CapturedHistory::drain(&capture, 64);
    assert_eq!(
        history
            .check_work(original, WorkExpectation::UnboundRolledBack)
            .unwrap(),
        WorkObservation::UnboundRolledBack
    );
    assert!(
        history
            .check_work(original, WorkExpectation::Retired)
            .is_err()
    );
    assert!(
        history
            .check_work(original, WorkExpectation::UnusedRetired)
            .is_err()
    );
    assert!(
        history
            .check_work(original, WorkExpectation::Unknown)
            .is_err()
    );
    history
        .check_completed_runtime(&ActorRuntime::new(), &ledger)
        .unwrap();
}

#[test]
fn appended_actual_capture_chunks_preserve_original_ordinals_and_drain_status() {
    let capture = HistoryCapture::new(64);
    let ledger = RuntimeLedger::with_history(ResourceVector::default(), capture.clone());
    let scope = ledger.open_scope_next(None, epoch()).unwrap();
    scope.close_external().unwrap();
    let mut trace = CapturedHistory::drain(&capture, 1);
    assert!(trace.check_capture().is_err());
    trace.append_from(&capture, 64);
    trace.check_capture().unwrap();
    ledger.remove_scope(scope.id()).unwrap();
    trace.append_from(&capture, 64);
    trace
        .check_completed_runtime(&ActorRuntime::new(), &ledger)
        .unwrap();
    assert_eq!(trace.clone().entries(), trace.entries());
}
