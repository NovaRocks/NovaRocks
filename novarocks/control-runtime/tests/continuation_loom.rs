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

#![cfg(loom)]

//! Bounded production-algorithm models, not a proof of arbitrary host I/O.
use loom::thread;
use novarocks_control_runtime::{
    completion::ReportSlot,
    continuation::{ContinuationError, ContinuationPhase, ContinuationSlot, InvocationResult},
    transaction::RuntimeLedger,
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    completion::ConsumerKind,
    identity::{ConsumerId, DimensionId, Generation},
    scope::{WorkAdmission, WorkKind},
};
fn model(run: impl Fn() + Send + Sync + 'static) {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(2);
    model.max_branches = 20_000;
    model.check(run);
}
#[test]
fn delayed_continuation_start_and_scope_failure_share_the_actual_generation_gate() {
    use novarocks_control_runtime::transaction::RuntimeError;
    use novarocks_control_runtime_core::scope::ScopeError;
    model(|| {
        let generation = Generation::new(1).unwrap();
        let dimension = DimensionId::new(1).unwrap();
        let resources = ResourceVector::new([(dimension, 1)]).unwrap();
        let ledger = RuntimeLedger::new(resources.clone(), 1, 1);
        let scope = ledger.open_scope_next(None, generation, 1, 1).unwrap();
        let (work, capacity) = scope
            .reserve_next(
                generation,
                resources,
                WorkKind::Settlement,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Settlement)
            .unwrap();
        let owner = ConsumerId::new(1).unwrap();
        let observer = ConsumerId::new(2).unwrap();
        let report = ReportSlot::reserve_next(
            &scope,
            generation,
            work.id(),
            2,
            [
                (
                    owner,
                    ConsumerKind::Owner {
                        scope: scope.id(),
                        generation,
                    },
                ),
                (observer, ConsumerKind::Observer),
            ],
        )
        .unwrap();
        let (slot, token) = ContinuationSlot::<()>::from_admitted(work, capacity, report.clone());
        token
            .register(Box::new(|_| {}))
            .unwrap_or_else(|_| panic!("one original registration"));
        slot.publish_barrier(()).unwrap();
        let running = slot.clone();
        let invoke = thread::spawn(move || running.invoke_in_generation(generation));
        let failing = scope.clone();
        let fail = thread::spawn(move || failing.fail().unwrap());
        let next = fail.join().unwrap();
        match invoke.join().unwrap() {
            Ok(InvocationResult::Exited) => assert_eq!(slot.phase(), ContinuationPhase::Exited),
            Err(ContinuationError::Runtime(RuntimeError::Scope(
                ScopeError::GenerationMismatch,
            ))) => {
                assert_eq!(slot.phase(), ContinuationPhase::Ready);
                assert!(!slot.body_exited());
                assert!(!slot.captures_exited());
                assert!(ledger.unresolved_work().is_empty());
                assert_eq!(
                    slot.invoke_in_generation(next).unwrap(),
                    InvocationResult::Exited
                );
            }
            other => panic!("unexpected generation-gated start: {other:?}"),
        }
        report
            .abandon_failed_owner(owner, &scope, generation)
            .unwrap();
        assert!(!slot.try_retire().unwrap());
        assert_eq!(ledger.used(dimension), 1);
        report.consume(observer).unwrap();
        assert!(slot.try_retire().unwrap());
        ledger.reap_scope_ready(scope.id(), 1).unwrap();
        ledger.remove_scope(scope.id()).unwrap();
        assert!(ledger.is_empty());
    });
}
#[test]
fn actual_invocation_and_owner_abandonment_preserve_the_other_consumer() {
    model(|| {
        let generation = Generation::new(1).unwrap();
        let dimension = DimensionId::new(1).unwrap();
        let resources = ResourceVector::new([(dimension, 1)]).unwrap();
        let ledger = RuntimeLedger::new(resources.clone(), 1, 1);
        let scope = ledger.open_scope_next(None, generation, 1, 1).unwrap();
        let (work, capacity) = scope
            .reserve_next(
                generation,
                resources,
                WorkKind::Settlement,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Settlement)
            .unwrap();
        let owner = ConsumerId::new(1).unwrap();
        let observer = ConsumerId::new(2).unwrap();
        let report = ReportSlot::reserve_next(
            &scope,
            generation,
            work.id(),
            2,
            [
                (
                    owner,
                    ConsumerKind::Owner {
                        scope: scope.id(),
                        generation,
                    },
                ),
                (observer, ConsumerKind::Observer),
            ],
        )
        .unwrap();
        let (slot, token) = ContinuationSlot::<()>::from_admitted(work, capacity, report.clone());
        token
            .register(Box::new(|_| {}))
            .unwrap_or_else(|_| panic!("unique registration"));
        slot.publish_barrier(()).unwrap();
        let running = slot.clone();
        let invoke = thread::spawn(move || running.invoke().unwrap());
        let failing_scope = scope.clone();
        let failed_report = report.clone();
        let fail = thread::spawn(move || {
            failing_scope.fail().unwrap();
            failed_report
                .abandon_failed_owner(owner, &failing_scope, generation)
                .unwrap();
        });
        assert_eq!(invoke.join().unwrap(), InvocationResult::Exited);
        fail.join().unwrap();
        assert!(!slot.try_retire().unwrap());
        assert_eq!(ledger.used(dimension), 1);
        report.consume(observer).unwrap();
        assert!(slot.try_retire().unwrap());
        assert_eq!(ledger.reap_ready(1).unwrap(), 1);
        ledger.remove_scope(scope.id()).unwrap();
        assert!(ledger.is_empty());
    });
}
#[test]
fn dropping_registration_authority_and_actual_unused_exit_are_separate_observations() {
    model(|| {
        let generation = Generation::new(1).unwrap();
        let dimension = DimensionId::new(1).unwrap();
        let resources = ResourceVector::new([(dimension, 1)]).unwrap();
        let ledger = RuntimeLedger::new(resources.clone(), 1, 1);
        let scope = ledger.open_scope_next(None, generation, 1, 1).unwrap();
        let (work, capacity) = scope
            .reserve_next(
                generation,
                resources,
                WorkKind::Settlement,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Settlement)
            .unwrap();
        let report = ReportSlot::reserve_next(&scope, generation, work.id(), 0, []).unwrap();
        let (slot, token) = ContinuationSlot::<()>::from_admitted(work, capacity, report);
        let revoke = thread::spawn(move || drop(token));
        let exiting = slot.clone();
        let exit = thread::spawn(move || exiting.unused_role_exited());
        revoke.join().unwrap();
        match exit.join().unwrap() {
            Ok(InvocationResult::Exited) => (),
            Err(ContinuationError::InvalidPhase(ContinuationPhase::AwaitingRegistration)) => {
                assert!(!slot.body_exited());
                assert!(!slot.captures_exited());
                assert!(!slot.try_retire().unwrap());
                assert_eq!(slot.unused_role_exited().unwrap(), InvocationResult::Exited);
            }
            other => panic!("unexpected role observation: {other:?}"),
        }
        assert_eq!(ledger.used(dimension), 1);
        assert!(slot.try_retire().unwrap());
        assert_eq!(ledger.reap_ready(1).unwrap(), 1);
        scope.close_external().unwrap();
        ledger.remove_scope(scope.id()).unwrap();
        assert!(ledger.is_empty());
    });
}
