// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#![expect(
    clippy::disallowed_methods,
    reason = "Integration tests own and join the threads that exercise memory races; see ADR-0218."
)]

mod common;
use common::*;
use novarocks_memory::*;
use std::sync::{Arc, Barrier, mpsc};

#[test]
fn seal_before_activation_refuses_and_activation_before_seal_finishes_without_new_scope() {
    let a = authority(32_768);
    let q = work(&a);
    let sealed_first = q.create_domain(128).unwrap();
    let worker_domain = sealed_first.clone();
    let (sealed_tx, sealed_rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        sealed_rx.recv().unwrap();
        assert!(matches!(
            worker_domain.activate(128, 0),
            Err(CapacityError::Closed { .. })
        ));
    });
    sealed_first.seal();
    sealed_tx.send(()).unwrap();
    worker.join().unwrap();

    let active_first = q.create_domain(128).unwrap();
    let worker_domain = active_first.clone();
    let (active_tx, active_rx) = mpsc::channel();
    let (finish_tx, finish_rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let mut scope = worker_domain.activate(128, 0).unwrap();
        let origin = scope.record_allocation(128);
        active_tx.send(()).unwrap();
        finish_rx.recv().unwrap();
        scope.finish();
        origin
    });
    active_rx.recv().unwrap();
    active_first.seal();
    assert_eq!(q.retire(&exited()), Err(TeardownError::ActiveScope));
    finish_tx.send(()).unwrap();
    let origin = worker.join().unwrap();
    assert!(active_first.activate(0, 0).is_err());
    assert!(!active_first.snapshot().active);
    q.retire(&exited()).unwrap();
    free(origin, 128);
}

#[test]
fn racing_seal_and_activation_choose_one_order_and_leave_no_publish_scope_after_finish() {
    for _ in 0..128 {
        let a = authority(16_384);
        let q = work(&a);
        let domain = q.create_domain(64).unwrap();
        let start = Arc::new(Barrier::new(2));
        let ready = start.clone();
        let worker_domain = domain.clone();
        let worker = std::thread::spawn(move || {
            ready.wait();
            match worker_domain.activate(64, 0) {
                Ok(mut scope) => {
                    let origin = scope.record_allocation(64);
                    scope.finish();
                    Some(origin)
                }
                Err(CapacityError::Closed { .. }) => None,
                Err(error) => panic!("unexpected activation outcome: {error}"),
            }
        });
        start.wait();
        domain.seal();
        let origin = worker.join().unwrap();
        let snapshot = domain.snapshot();
        assert!(snapshot.sealed);
        assert!(!snapshot.active);
        assert_eq!(snapshot.live, if origin.is_some() { 64 } else { 0 });
        assert!(matches!(
            domain.activate(0, 0),
            Err(CapacityError::Closed { .. })
        ));
        q.retire(&exited()).unwrap();
        if let Some(origin) = origin {
            free(origin, 64);
        }
    }
}

#[test]
fn capacity_drain_does_not_revoke_a_scope_paused_between_allocation_hooks() {
    let a = authority(32_768);
    let q = work(&a);
    let domain = q.create_domain(1_024).unwrap();
    let worker_domain = domain.clone();
    let (active_tx, active_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let mut scope = worker_domain.activate(1_024, 0).unwrap();
        active_tx.send(()).unwrap();
        resume_rx.recv().unwrap();
        let origin = scope.record_allocation(1_024);
        scope.finish();
        origin
    });
    active_rx.recv().unwrap();
    let before = a.root().committed_bytes();
    let mut writer = a.take_capacity_writer().unwrap();
    writer.set_capacity(0).unwrap();
    let coverage = loop {
        let coverage = a.maintain(64);
        if coverage.complete {
            break coverage;
        }
    };
    assert_eq!(coverage.deferred_active, 1);
    assert_eq!(domain.snapshot().authorized, 1_024);
    assert_eq!(a.root().committed_bytes(), before);
    resume_tx.send(()).unwrap();
    let origin = worker.join().unwrap();
    assert_eq!(domain.snapshot().live, 1_024);
    assert_eq!(domain.snapshot().debt, 0);
    q.retire(&exited()).unwrap();
    free(origin, 1_024);
}

#[test]
fn child_and_two_ancestors_can_retire_concurrently_without_losing_residual() {
    for _ in 0..64 {
        let a = authority(32_768);
        let group = a
            .create_account(AccountKind::ResourceGroup, ExternalRef::NONE)
            .unwrap();
        let q = group
            .create_child(AccountKind::Work, ExternalRef::NONE)
            .unwrap();
        let task = q
            .create_child(AccountKind::Task, ExternalRef::NONE)
            .unwrap();
        let domain = task.create_domain(256).unwrap();
        let mut scope = domain.activate(256, 0).unwrap();
        let origin = scope.record_allocation(256);
        scope.finish();
        let before = a.pressure_projection();
        let start = Arc::new(Barrier::new(4));
        let handles: Vec<_> = [task.clone(), q.clone(), group.clone()]
            .into_iter()
            .map(|account| {
                let ready = start.clone();
                std::thread::spawn(move || {
                    ready.wait();
                    account.retire(&exited()).unwrap();
                })
            })
            .collect();
        start.wait();
        // Observations concurrent with handoff must use one committed
        // responsibility classification, including prepaid record metadata.
        for _ in 0..8 {
            let sample = a.pressure_projection();
            assert_eq!(sample.root_committed, before.root_committed);
            assert!(
                sample.query_pressure() == 0 || sample.query_pressure() == before.query_pressure()
            );
        }
        for handle in handles {
            handle.join().unwrap();
        }
        assert!(task.is_retired() && q.is_retired() && group.is_retired());
        assert_eq!(task.committed_bytes(), 0);
        assert_eq!(q.committed_bytes(), 0);
        assert_eq!(group.committed_bytes(), 0);
        let after = a.pressure_projection();
        assert_eq!(after.residual_committed, 256 + OWNER_METADATA_BYTES);
        assert_eq!(after.residual_metadata, OWNER_METADATA_BYTES);
        assert_eq!(after.query_committed, 0);
        assert_eq!(after.query_pressure(), 0);
        free(origin, 256);
        drop(domain);
        a.request_maintenance(MaintenanceReason::ExplicitLocalReclaim);
        while !a.maintain(64).complete {}
        assert_eq!(a.pressure_projection().residual_committed, 0);
    }
}

#[test]
fn remote_final_free_racing_maintenance_is_consumed_once_by_a_later_complete_sweep() {
    for _ in 0..64 {
        let a = authority(16_384);
        let baseline = a.root().committed_bytes();
        let q = work(&a);
        let domain = q.create_domain(64).unwrap();
        let mut scope = domain.activate(64, 0).unwrap();
        let origin = scope.record_allocation(64);
        scope.finish();
        q.retire(&exited()).unwrap();
        drop(domain);
        drop(q);
        let start = Arc::new(Barrier::new(2));
        let ready = start.clone();
        let remote = std::thread::spawn(move || {
            ready.wait();
            free(origin, 64);
        });
        a.request_maintenance(MaintenanceReason::ExplicitLocalReclaim);
        start.wait();
        while !a.maintain(1).complete {}
        remote.join().unwrap();
        a.request_maintenance(MaintenanceReason::ExplicitLocalReclaim);
        while !a.maintain(1).complete {}
        assert_eq!(a.pressure_projection().residual_committed, 0);
        assert_eq!(a.root().committed_bytes(), baseline);
        assert!(a.snapshot().root.is_internally_consistent());
    }
}
