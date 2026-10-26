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

//! Models the production custody locks with two preemptions. Host capture
//! disposal is supplied by the original authority, not simulated by a timeout.
use loom::thread;
use novarocks_control_runtime::{
    completion::ReportSlot,
    exit_role::{ExitRole, ExitRolePhase},
    transaction::{RuntimeError, RuntimeLedger},
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    completion::{CompletionError, ConsumerKind},
    identity::{ConsumerId, DimensionId, Generation},
    scope::{WorkAdmission, WorkKind},
};
fn model(run: impl Fn() + Send + Sync + 'static) {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(2);
    model.max_branches = 20_000;
    model.check(run);
}
fn generation() -> Generation {
    Generation::new(1).unwrap()
}
fn dimension() -> DimensionId {
    DimensionId::new(1).unwrap()
}
fn resources() -> ResourceVector {
    ResourceVector::new([(dimension(), 1)]).unwrap()
}

#[test]
fn actual_exit_and_independent_consumption_cannot_reuse_capacity_early() {
    model(|| {
        let ledger = RuntimeLedger::new(resources(), 1, 1);
        let scope = ledger.open_scope_next(None, generation(), 1, 1).unwrap();
        let (work, capacity) = scope
            .reserve_next(
                generation(),
                resources(),
                WorkKind::Cleanup,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        let consumer = ConsumerId::new(1).unwrap();
        let report = ReportSlot::reserve_next(
            &scope,
            generation(),
            work.id(),
            1,
            [(consumer, ConsumerKind::Observer)],
        )
        .unwrap();
        let (role, mut authority) = ExitRole::from_admitted(work, capacity, report.clone());
        authority.start().unwrap();
        scope.close_external().unwrap();
        let finishing = role.clone();
        let exit = thread::spawn(move || {
            authority.exited().unwrap();
            finishing.try_retire().unwrap()
        });
        let consuming = report.clone();
        let consumed = thread::spawn(move || consuming.consume(consumer));
        let retired = exit.join().unwrap();
        match consumed.join().unwrap() {
            Ok(()) => (),
            Err(RuntimeError::Completion(CompletionError::NotFilled)) => {
                report.consume(consumer).unwrap()
            }
            other => panic!("unexpected consumer fact: {other:?}"),
        }
        if !retired {
            assert_eq!(ledger.used(dimension()), 1);
            assert!(role.try_retire().unwrap());
        }
        assert_eq!(role.phase(), ExitRolePhase::Retired);
        assert!(role.body_exited());
        assert!(role.captures_exited());
        assert_eq!(ledger.used(dimension()), 0);
        assert_eq!(ledger.reap_scope_ready(scope.id(), 1).unwrap(), 1);
        ledger.remove_scope(scope.id()).unwrap();
        assert!(ledger.is_empty());
    });
}

#[test]
fn scope_failure_abandons_only_the_owner_leg_and_never_the_original_exit() {
    model(|| {
        let ledger = RuntimeLedger::new(resources(), 1, 1);
        let scope = ledger.open_scope_next(None, generation(), 1, 1).unwrap();
        let (work, capacity) = scope
            .reserve_next(
                generation(),
                resources(),
                WorkKind::Cleanup,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        let owner = ConsumerId::new(1).unwrap();
        let observer = ConsumerId::new(2).unwrap();
        let report = ReportSlot::reserve_next(
            &scope,
            generation(),
            work.id(),
            2,
            [
                (
                    owner,
                    ConsumerKind::Owner {
                        scope: scope.id(),
                        generation: generation(),
                    },
                ),
                (observer, ConsumerKind::Observer),
            ],
        )
        .unwrap();
        let (role, mut authority) = ExitRole::from_admitted(work, capacity, report.clone());
        let exit = thread::spawn(move || {
            authority.start().unwrap();
            authority.exited().unwrap();
        });
        let failing = scope.clone();
        let abandoning = report.clone();
        let fail = thread::spawn(move || {
            failing.fail().unwrap();
            abandoning
                .abandon_failed_owner(owner, &failing, generation())
                .unwrap();
        });
        exit.join().unwrap();
        fail.join().unwrap();
        assert_eq!(role.phase(), ExitRolePhase::Exited);
        assert!(!role.try_retire().unwrap());
        assert_eq!(ledger.used(dimension()), 1);
        assert!(
            report
                .abandon_failed_owner(observer, &scope, generation())
                .is_err()
        );
        report.consume(observer).unwrap();
        assert!(role.try_retire().unwrap());
        assert_eq!(ledger.reap_scope_ready(scope.id(), 1).unwrap(), 1);
        ledger.remove_scope(scope.id()).unwrap();
        assert!(ledger.is_empty());
    });
}
