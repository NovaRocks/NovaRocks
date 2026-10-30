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

use super::super::join_completion::tests::{completion, error, fixture};
use super::*;
#[test]
fn tail_ranges_are_unique_and_finish_only_after_last_output_settles() {
    let (owner, _) = fixture();
    let (completion, _) = completion(2, 3, owner.as_ref());
    completion.publish(Arc::new(91), 9).unwrap();
    let ranges = TailRanges::new(
        completion.clone(),
        NonZeroUsize::new(4).unwrap(),
        owner.as_ref(),
        &super::super::join_completion::tests::Control,
    )
    .unwrap();
    assert!(matches!(ranges.claim().unwrap(), TailClaim::Pending));
    completion.normal_after_output(1).unwrap();
    completion.normal_after_output(0).unwrap();
    let mut leases = Vec::new();
    for expected in [0..4, 4..8, 8..9] {
        let TailClaim::Range(lease) = ranges.claim().unwrap() else {
            panic!("tail range")
        };
        assert_eq!(lease.rows(), expected);
        assert_eq!(*lease.artifact(), 91);
        leases.push(lease);
    }
    assert!(matches!(ranges.claim().unwrap(), TailClaim::Pending));
    let mut last = leases.pop().unwrap();
    for lease in &mut leases {
        lease.complete_after_output().unwrap();
    }
    assert!(matches!(ranges.claim().unwrap(), TailClaim::Pending));
    last.complete_after_output().unwrap();
    assert!(matches!(ranges.claim().unwrap(), TailClaim::Finished));
}
#[test]
fn normal_global_withdrawal_retires_without_reissue_or_successful_tail() {
    let (owner, _) = fixture();
    let (completion, handoff) = completion(1, 1, owner.as_ref());
    completion.publish(Arc::new(1), 4).unwrap();
    completion.normal_after_output(0).unwrap();
    let ranges = TailRanges::new(
        completion.clone(),
        NonZeroUsize::new(2).unwrap(),
        owner.as_ref(),
        &super::super::join_completion::tests::Control,
    )
    .unwrap();
    let TailClaim::Range(mut lease) = ranges.claim().unwrap() else {
        panic!("range")
    };
    handoff.unwrap().close_consumer(0);
    lease.withdraw_after_demand_released().unwrap();
    assert!(matches!(ranges.claim().unwrap(), TailClaim::Withdrawn));
    assert!(completion.check().is_ok());
}
#[test]
fn zero_rows_is_true_published_empty_and_failure_before_tail_barrier_stays_failure() {
    let (owner, _) = fixture();
    let (completion, _) = completion(1, 1, owner.as_ref());
    completion.publish(Arc::new(1), 0).unwrap();
    let ranges = TailRanges::new(
        completion.clone(),
        NonZeroUsize::new(3).unwrap(),
        owner.as_ref(),
        &super::super::join_completion::tests::Control,
    )
    .unwrap();
    assert!(matches!(ranges.claim().unwrap(), TailClaim::Pending));
    completion.normal_after_output(0).unwrap();
    assert!(matches!(ranges.claim().unwrap(), TailClaim::Finished));
    let cause = build_contract("actual late root failure");
    completion.fail(cause.clone());
    assert_eq!(error(ranges.claim()), cause);
}

#[test]
fn abandoned_tail_keeps_original_failure_and_never_reissues_range() {
    let (owner, _) = fixture();
    let (completion, _) = completion(1, 1, owner.as_ref());
    completion.publish(Arc::new(1), 5).unwrap();
    completion.normal_after_output(0).unwrap();
    let ranges = TailRanges::new(
        completion.clone(),
        NonZeroUsize::new(2).unwrap(),
        owner.as_ref(),
        &super::super::join_completion::tests::Control,
    )
    .unwrap();
    let TailClaim::Range(mut lease) = ranges.claim().unwrap() else {
        panic!("range")
    };
    let cause = build_contract("actual original tail construction failure");
    assert_eq!(lease.fail(cause.clone()), cause);
    drop(lease);
    assert_eq!(error(ranges.claim()), cause);
    assert_eq!(error(completion.published()), cause);
}
