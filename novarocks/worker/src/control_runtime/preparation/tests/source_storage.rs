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

//! Future Task roles move once from the original Accepted object's unused inventory.

use super::*;
use novarocks_control_runtime_core::{
    capacity::ReservationResources, owned_storage::OwnedSlotNeed,
};

fn role_storage(roles: usize) -> ReservationResources {
    crate::control_runtime_policy::StorageNeed::role(false)
        .checked_mul(roles)
        .unwrap()
        .resources()
        .unwrap()
}
fn accepted_resources() -> ReservationResources {
    ReservationResources::new(
        ResourceVector::new([
            (Dimension::PreparingTasks.id(), 1),
            (Dimension::PreparingBytes.id(), 10),
        ])
        .unwrap(),
        OwnedSlotNeed {
            work_headers: 1,
            completions: 2,
            captures: 1,
            ..OwnedSlotNeed::default()
        },
    )
    .checked_add(&role_storage(3))
    .unwrap()
}
fn admitted() -> (
    Fixture,
    PreparationCustody,
    PreparationJob,
    Arc<AtomicUsize>,
) {
    let resources = accepted_resources();
    let mut fixture = Fixture {
        ledger: RuntimeLedger::new(resources.quota.clone()),
        scopes: Vec::new(),
    };
    let scope = fixture.scope();
    let (work, mut capacity) = scope
        .reserve_next(
            generation(),
            resources,
            WorkKind::Business,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Business)
        .unwrap();
    let mut report = || {
        ReportSlot::reserve_next(
            &mut capacity,
            generation(),
            2,
            [
                (
                    core(),
                    ConsumerKind::Owner {
                        scope: scope.id(),
                        generation: generation(),
                    },
                ),
                (observer(), ConsumerKind::Observer),
            ],
        )
        .unwrap()
    };
    let reports = [report(), report()];
    let drops = Arc::new(AtomicUsize::new(0));
    let (custody, job) = PreparationCustody::from_admitted(
        PreparationBinding {
            stop: crate::task_port::TaskStopSignal::initial(identity(11)),
            pending: Arc::new(Pending),
            health: Arc::new(ExecutionHealth::default()),
        },
        work,
        capacity,
        reports,
        input(drops.clone()),
    );
    (fixture, custody, job, drops)
}
fn assert_preparing(f: &Fixture, count: u64, bytes: u64) {
    assert_eq!(f.ledger.used(Dimension::PreparingTasks.id()), count);
    assert_eq!(f.ledger.used(Dimension::PreparingBytes.id()), bytes);
}

#[test]
fn source_split_moves_only_original_storage_and_stays_owned_after_preparation_returns() {
    let (fixture, owner, job, drops) = admitted();
    let need = role_storage(2);
    let mut storage = owner.take_task_role_storage(&need).unwrap();
    assert_eq!(storage.resources().unwrap(), need);
    assert_preparing(&fixture, 1, 10);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(matches!(
        owner.take_task_role_storage(&need),
        Err(PreparationStorageSplitError::AlreadyTransferred)
    ));
    assert!(job.dispose());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(owner.publish_whole_exit().unwrap());
    assert!(owner.take_after_exit().unwrap().is_none());
    assert_preparing(&fixture, 1, 10);
    for report in [owner.body_report(), owner.exit_report()] {
        report.consume(core()).unwrap();
        report.consume(observer()).unwrap();
    }
    let mut remainder = owner.take_after_exit().unwrap().unwrap();
    assert_preparing(&fixture, 0, 0);
    assert_eq!(remainder.resources().unwrap(), role_storage(1));
    remainder.finish().unwrap();
    assert_eq!(
        storage.resources().unwrap(),
        need,
        "finishing the original remainder cannot release transferred unused Task roles"
    );
    storage.finish().unwrap();
    drop((storage, remainder, owner));
    fixture.finish();
}

#[test]
fn invalid_and_failed_splits_keep_the_original_storage_and_single_transfer_opportunity() {
    let (fixture, owner, job, drops) = admitted();
    let valid = role_storage(2);
    let mut invalid = vec![
        ReservationResources::default(),
        ReservationResources::work_only(),
    ];
    let mut wrong_profile = valid.clone();
    wrong_profile.owned_slots.completions += 1;
    invalid.push(wrong_profile);
    for dimension in [
        Dimension::PreparingTasks,
        Dimension::PreparingBytes,
        Dimension::Requests,
    ] {
        let mut wrong_quota = valid.clone();
        wrong_quota.quota = ResourceVector::new([(dimension.id(), 1)]).unwrap();
        invalid.push(wrong_quota);
    }
    for need in invalid {
        assert!(matches!(
            owner.take_task_role_storage(&need),
            Err(PreparationStorageSplitError::InvalidResources)
        ));
        assert_preparing(&fixture, 1, 10);
    }
    assert!(matches!(
        owner.take_task_role_storage(&role_storage(4)),
        Err(PreparationStorageSplitError::Runtime(_))
    ));
    let mut storage = owner.take_task_role_storage(&valid).unwrap();
    assert_eq!(storage.resources().unwrap(), valid);
    assert!(job.dispose());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    fixture.release(&owner);
    assert_preparing(&fixture, 0, 0);
    assert_eq!(storage.resources().unwrap(), valid);
    storage.finish().unwrap();
    drop((storage, owner));
    fixture.finish();
}

#[test]
fn preparation_exit_closes_an_unused_source_storage_transfer() {
    let (fixture, owner, job, drops) = admitted();
    assert!(job.dispose());
    assert!(owner.publish_whole_exit().unwrap());
    assert!(matches!(
        owner.take_task_role_storage(&role_storage(2)),
        Err(PreparationStorageSplitError::Unavailable)
    ));
    assert_preparing(&fixture, 1, 10);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    fixture.release(&owner);
    drop(owner);
    fixture.finish();
}
