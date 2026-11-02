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

use super::super::join_completion::{
    JoinCompletion,
    tests::{Control, error, fixture, participants},
};
use super::*;
use crate::runtime::observable::Observable;
fn collection(count: usize, owner: &dyn InputConstructionOwner) -> Arc<BuildCollection<u64, u64>> {
    let completion = JoinCompletion::new(
        participants(count, 2, 0),
        None,
        Arc::new(Observable::new()),
        owner,
        &Control,
    )
    .unwrap();
    BuildCollection::new(completion, owner, &Control).unwrap()
}
#[test]
fn last_input_only_seals_then_one_finalizer_claims_begin_in_original_slot_order() {
    let (owner, _) = fixture();
    let collection = collection(3, owner.as_ref());
    collection.normal_input(2, 22).unwrap();
    collection.normal_input(0, 0).unwrap();
    assert!(matches!(
        collection.claim_begin(owner.as_ref(), &Control).unwrap(),
        BeginClaim::Pending
    ));
    collection.normal_input(1, 11).unwrap();
    assert!(collection.inputs_sealed().unwrap());
    assert!(!collection.state().unwrap().begin_completed);
    let BeginClaim::Claimed { inputs, mut lease } =
        collection.claim_begin(owner.as_ref(), &Control).unwrap()
    else {
        panic!("sealed inputs")
    };
    assert_eq!(inputs.as_slice(), &[0, 11, 22]);
    assert!(matches!(
        collection.claim_begin(owner.as_ref(), &Control).unwrap(),
        BeginClaim::InProgress
    ));
    assert!(collection.completion.published().unwrap().is_none());
    collection.complete_begin(&mut lease).unwrap();
    collection.publish(Arc::new(5), 0).unwrap();
    assert_eq!(collection.completion.published().unwrap().unwrap().rows, 0);
}
#[test]
fn empty_e_is_exact_empty_but_withdrawn_or_failed_e_never_seals() {
    let (owner, _) = fixture();
    let empty = collection(0, owner.as_ref());
    assert!(empty.inputs_sealed().unwrap());
    let BeginClaim::Claimed { inputs, mut lease } =
        empty.claim_begin(owner.as_ref(), &Control).unwrap()
    else {
        panic!("exact empty")
    };
    assert!(inputs.as_slice().is_empty());
    empty.complete_begin(&mut lease).unwrap();
    let withdrawn = collection(2, owner.as_ref());
    withdrawn.normal_input(0, 9).unwrap();
    withdrawn.withdraw_input_if_pending(1).unwrap();
    assert!(!withdrawn.inputs_sealed().unwrap());
    assert!(matches!(
        withdrawn.claim_begin(owner.as_ref(), &Control).unwrap(),
        BeginClaim::Unavailable
    ));
    let failed = collection(2, owner.as_ref());
    let cause = build_contract("original producer failure");
    failed.fail_input(0, cause.clone());
    assert_eq!(error(failed.claim_begin(owner.as_ref(), &Control)), cause);
}
#[test]
fn duplicate_e_or_lost_begin_is_failure_and_original_begin_cause_wins() {
    let (owner, _) = fixture();
    let duplicate = collection(1, owner.as_ref());
    duplicate.normal_input(0, 1).unwrap();
    let cause = error(duplicate.normal_input(0, 2));
    assert_eq!(error(duplicate.inputs_sealed()), cause);
    let abandoned = collection(0, owner.as_ref());
    let claim = abandoned.claim_begin(owner.as_ref(), &Control).unwrap();
    drop(claim);
    assert!(abandoned.check().is_err());
    assert!(abandoned.completion.published().is_err());
    let original = collection(0, owner.as_ref());
    let BeginClaim::Claimed { inputs, mut lease } =
        original.claim_begin(owner.as_ref(), &Control).unwrap()
    else {
        panic!("begin")
    };
    let cause = build_contract("actual original graph construction refusal");
    assert_eq!(lease.fail(cause.clone()), cause);
    drop((inputs, lease));
    assert_eq!(error(original.check()), cause);
}

#[test]
fn duplicate_begin_completion_latches_and_cannot_publish() {
    let (owner, _) = fixture();
    let c = collection(0, owner.as_ref());
    let BeginClaim::Claimed {
        inputs: _,
        mut lease,
    } = c.claim_begin(owner.as_ref(), &Control).unwrap()
    else {
        panic!("begin")
    };
    c.complete_begin(&mut lease).unwrap();
    let cause = error(c.complete_begin(&mut lease));
    assert_eq!(error(c.publish(Arc::new(8), 0)), cause);
}
