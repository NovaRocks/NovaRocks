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

//! Source storage is split from one actual original Accepted bundle. These
//! tests create no extra source Work, report or Native execution capability.

use super::*;

fn source_fees(units: u64) -> ResourceVector {
    ResourceVector::new([
        (Dimension::Work.id(), units),
        (Dimension::Completions.id(), units),
        (Dimension::Captures.id(), units),
    ])
    .unwrap()
}
fn accepted_resources() -> ResourceVector {
    ResourceVector::new([
        (Dimension::PreparingTasks.id(), 1),
        (Dimension::PreparingBytes.id(), 10),
        (Dimension::InboundRecords.id(), 1),
        (Dimension::InboundBytes.id(), 9),
    ])
    .unwrap()
    .checked_add(&source_fees(3))
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
        ledger: RuntimeLedger::new(resources.clone(), 4, 1),
        scopes: Vec::new(),
    };
    let scope = fixture.scope();
    // All source fees are paid by this original admission, before binding the
    // actual input. No later reservation supplies duplicate source fees.
    let (work, capacity) = scope
        .reserve_next(
            generation(),
            resources,
            WorkKind::Business,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Business)
        .unwrap();
    let report = || {
        ReportSlot::reserve_next(
            &scope,
            generation(),
            work.id(),
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
fn assert_paid(fixture: &Fixture, resources: &ResourceVector) {
    for dimension in [
        Dimension::PreparingTasks,
        Dimension::PreparingBytes,
        Dimension::InboundRecords,
        Dimension::InboundBytes,
        Dimension::Work,
        Dimension::Completions,
        Dimension::Captures,
    ] {
        assert_eq!(
            fixture.ledger.used(dimension.id()),
            resources.get(dimension.id())
        );
    }
}

#[test]
fn source_split_moves_only_original_storage_and_stays_paid_after_preparation_returns() {
    let (fixture, owner, job, drops) = admitted();
    let original = accepted_resources();
    let source = source_fees(2);
    let mut storage = owner.take_task_source_storage(&source).unwrap();
    assert_eq!(storage.resources().unwrap(), source);
    assert_paid(&fixture, &original);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(matches!(
        owner
            .take_task_source_storage(&source)
            .err()
            .expect("one transfer"),
        PreparationStorageSplitError::AlreadyTransferred
    ));
    assert_paid(&fixture, &original);

    // Dispose the actual never-started job and its original input outside
    // custody locks. The two reports still need their actual consumer legs.
    assert!(job.dispose());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(owner.publish_whole_exit().unwrap());
    assert!(owner.take_after_exit().unwrap().is_none());
    assert_paid(&fixture, &original);
    for report in [owner.body_report(), owner.exit_report()] {
        report.consume(core()).unwrap();
        report.consume(observer()).unwrap();
    }
    let mut remainder = owner.take_after_exit().unwrap().unwrap();
    let remaining = remainder.resources().unwrap();
    assert_eq!(remaining.get(Dimension::PreparingTasks.id()), 0);
    assert_eq!(remaining.get(Dimension::PreparingBytes.id()), 0);
    assert_eq!(remaining.get(Dimension::InboundRecords.id()), 1);
    assert_eq!(remaining.get(Dimension::InboundBytes.id()), 9);
    for dimension in [Dimension::Work, Dimension::Completions, Dimension::Captures] {
        assert_eq!(remaining.get(dimension.id()), 1);
        assert_eq!(fixture.ledger.used(dimension.id()), 3);
    }
    remainder.release(&remaining).unwrap();
    // The preparation can return its own remainder without refunding the
    // caller's same original source storage, which has never issued a holder.
    assert_paid(&fixture, &source);
    storage.release(&source).unwrap();
    drop((storage, remainder, owner));
    fixture.finish();
}

#[test]
fn invalid_and_failed_splits_keep_the_original_storage_and_single_transfer_opportunity() {
    let (fixture, owner, job, drops) = admitted();
    let original = accepted_resources();
    let valid = source_fees(2);
    let mut invalid = vec![
        ResourceVector::default(),
        ResourceVector::new([(Dimension::Work.id(), 1)]).unwrap(),
        ResourceVector::new([
            (Dimension::Work.id(), 1),
            (Dimension::Completions.id(), 2),
            (Dimension::Captures.id(), 1),
        ])
        .unwrap(),
    ];
    for dimension in [
        Dimension::PreparingTasks,
        Dimension::PreparingBytes,
        Dimension::InboundRecords,
        Dimension::InboundBytes,
        Dimension::Requests,
    ] {
        invalid.push(
            valid
                .checked_add(&ResourceVector::new([(dimension.id(), 1)]).unwrap())
                .unwrap(),
        );
    }
    for resources in invalid {
        assert!(matches!(
            owner
                .take_task_source_storage(&resources)
                .err()
                .expect("typed refusal"),
            PreparationStorageSplitError::InvalidResources
        ));
        assert_paid(&fixture, &original);
    }
    assert!(matches!(
        owner
            .take_task_source_storage(&source_fees(4))
            .err()
            .expect("original bundle is too small"),
        PreparationStorageSplitError::Runtime(_)
    ));
    assert_paid(&fixture, &original);
    let mut storage = owner.take_task_source_storage(&valid).unwrap();
    assert_eq!(storage.resources().unwrap(), valid);
    assert_paid(&fixture, &original);
    assert!(job.dispose());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    fixture.release(&owner);
    assert_paid(&fixture, &valid);
    storage.release(&valid).unwrap();
    drop((storage, owner));
    fixture.finish();
}

#[test]
fn preparation_exit_closes_an_unused_source_storage_transfer() {
    let (fixture, owner, job, drops) = admitted();
    assert!(job.dispose());
    assert!(owner.publish_whole_exit().unwrap());
    assert!(matches!(
        owner
            .take_task_source_storage(&source_fees(2))
            .err()
            .expect("preparation exit is already published"),
        PreparationStorageSplitError::Unavailable
    ));
    assert_paid(&fixture, &accepted_resources());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    fixture.release(&owner);
    drop(owner);
    fixture.finish();
}
