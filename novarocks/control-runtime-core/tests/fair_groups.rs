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

use novarocks_control_runtime_core::{
    fair_queue::{FairQueueError, FairWorkQueue},
    identity::{ScopeId, WorkId},
};

// A complete neutral grouping value, independent of every actual job owner.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct GroupKey {
    query: u64,
    frontend: u64,
    backend: u64,
}
fn group(query: u64) -> GroupKey {
    GroupKey {
        query,
        frontend: 7,
        backend: 9,
    }
}
type OwnerJob = (ScopeId, WorkId);
fn job(scope: u64, work: u64) -> OwnerJob {
    (ScopeId::new(scope).unwrap(), WorkId::new(work).unwrap())
}

#[test]
fn group_fifo_and_fairness_preserve_complete_owner_keys_with_equal_local_work_ids() {
    let (a, b) = (group(1), group(2));
    let (a1, a2, a3, b1, b2) = (job(11, 1), job(12, 1), job(11, 2), job(21, 1), job(22, 1));
    let mut queue = FairWorkQueue::<GroupKey, OwnerJob>::new(2, 5, 3);
    for (group, work) in [(a, a1), (a, a2), (b, b1), (a, a3), (b, b2)] {
        queue.enqueue(group, work).unwrap();
    }
    assert_eq!(queue.enqueue(a, a2), Err(FairQueueError::DuplicateWork));
    assert_eq!(queue.pop(), Some((a, a1)));
    assert_eq!(queue.pop(), Some((b, b1)));
    assert_eq!(queue.pop(), None);
    assert_eq!(queue.running(a), Some(a1));
    assert_eq!(queue.running(b), Some(b1));
    assert_eq!(queue.finish(a, a2), Err(FairQueueError::WrongRunningWork));
    assert_eq!(queue.remove_queued(a, a1), Err(FairQueueError::UnknownWork));
    assert_eq!(queue.len(), 5);
    queue.finish(a, a1).unwrap();
    queue.finish(b, b1).unwrap();
    assert_eq!(queue.pop(), Some((a, a2)));
    assert_eq!(queue.pop(), Some((b, b2)));
    assert_eq!(queue.pop(), None);
    queue.finish(a, a2).unwrap();
    queue.finish(b, b2).unwrap();
    assert_eq!(queue.pop(), Some((a, a3)));
    queue.finish(a, a3).unwrap();
    assert!(queue.is_empty());
}

#[test]
fn indexed_cancellation_removes_exact_head_middle_and_tail_without_touching_another_group() {
    let (a, b) = (group(1), group(2));
    let a_jobs = [job(10, 1), job(11, 1), job(12, 1), job(13, 1)];
    let b_jobs = [job(20, 1), job(21, 1)];
    let mut queue = FairWorkQueue::<GroupKey, OwnerJob>::new(2, 6, 4);
    for work in a_jobs {
        queue.enqueue(a, work).unwrap();
    }
    for work in b_jobs {
        queue.enqueue(b, work).unwrap();
    }
    assert_eq!(
        queue.remove_queued(a, b_jobs[0]),
        Err(FairQueueError::UnknownWork)
    );
    queue.remove_queued(a, a_jobs[1]).unwrap();
    queue.remove_queued(a, a_jobs[3]).unwrap();
    queue.remove_queued(a, a_jobs[0]).unwrap();
    assert_eq!(queue.len(), 3);
    assert_eq!(queue.pop(), Some((a, a_jobs[2])));
    assert_eq!(queue.pop(), Some((b, b_jobs[0])));
    queue.remove_queued(b, b_jobs[1]).unwrap();
    assert_eq!(queue.pop(), None);
    assert_eq!(queue.running(a), Some(a_jobs[2]));
    assert_eq!(queue.running(b), Some(b_jobs[0]));
    queue.finish(a, a_jobs[2]).unwrap();
    queue.finish(b, b_jobs[0]).unwrap();
    assert!(queue.is_empty());
}

#[test]
fn cancellation_and_body_finish_recover_original_global_group_and_per_group_capacity() {
    let (a, b, c) = (group(1), group(2), group(3));
    let (a1, a2, a3, b1, b2, c1) = (
        job(10, 1),
        job(11, 1),
        job(12, 1),
        job(20, 1),
        job(21, 1),
        job(30, 1),
    );
    let mut queue = FairWorkQueue::<GroupKey, OwnerJob>::new(2, 3, 2);
    queue.enqueue(a, a1).unwrap();
    queue.enqueue(a, a2).unwrap();
    assert_eq!(queue.enqueue(a, a3), Err(FairQueueError::ContextFull));
    queue.enqueue(b, b1).unwrap();
    assert_eq!(queue.enqueue(c, c1), Err(FairQueueError::Full));
    queue.remove_queued(a, a2).unwrap();
    assert_eq!(queue.enqueue(c, c1), Err(FairQueueError::ContextCapacity));
    queue.enqueue(b, b2).unwrap();
    queue.remove_queued(b, b1).unwrap();
    queue.remove_queued(b, b2).unwrap();
    queue.enqueue(c, c1).unwrap();
    assert_eq!(queue.pop(), Some((a, a1)));
    queue.enqueue(a, a2).unwrap();
    queue.finish(a, a1).unwrap();
    assert_eq!(queue.pop(), Some((c, c1)));
    assert_eq!(queue.pop(), Some((a, a2)));
    queue.finish(c, c1).unwrap();
    queue.finish(a, a2).unwrap();
    assert!(queue.is_empty());
    queue.enqueue(group(4), job(40, 1)).unwrap();
    queue.remove_queued(group(4), job(40, 1)).unwrap();
    assert!(queue.is_empty());
}

#[test]
fn full_group_value_and_non_default_key_types_are_preserved_without_projection() {
    #[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
    struct NonDefaultJob(u64);
    let a = group(1);
    let b = GroupKey { frontend: 8, ..a };
    let c = GroupKey { backend: 10, ..a };
    let mut queue = FairWorkQueue::<GroupKey, NonDefaultJob>::new(3, 3, 1);
    let expected = [
        (a, NonDefaultJob(1)),
        (b, NonDefaultJob(2)),
        (c, NonDefaultJob(3)),
    ];
    for (group, work) in expected {
        queue.enqueue(group, work).unwrap();
    }
    for entry in expected {
        assert_eq!(queue.pop(), Some(entry));
    }
    assert_eq!(queue.pop(), None);
    for (group, work) in expected {
        queue.finish(group, work).unwrap();
    }
    assert!(queue.is_empty());
}

#[test]
fn default_api_keeps_scope_local_work_identity_and_zero_capacity_refusals() {
    let (a, b) = (ScopeId::new(1).unwrap(), ScopeId::new(2).unwrap());
    let work = WorkId::new(1).unwrap();
    let mut queue: FairWorkQueue = FairWorkQueue::new(2, 2, 1);
    queue.enqueue(a, work).unwrap();
    queue.enqueue(b, work).unwrap();
    queue.remove_queued(a, work).unwrap();
    assert_eq!(queue.pop(), Some((b, work)));
    queue.finish(b, work).unwrap();
    assert!(queue.is_empty());
    for (groups, jobs, per_group, expected) in [
        (0, 2, 1, FairQueueError::ContextCapacity),
        (2, 0, 1, FairQueueError::Full),
        (2, 2, 0, FairQueueError::ContextFull),
    ] {
        let mut queue = FairWorkQueue::<GroupKey, OwnerJob>::new(groups, jobs, per_group);
        assert_eq!(queue.enqueue(group(1), job(10, 1)), Err(expected));
        assert_eq!(queue.len(), 0);
        assert_eq!(queue.pop(), None);
    }
}
