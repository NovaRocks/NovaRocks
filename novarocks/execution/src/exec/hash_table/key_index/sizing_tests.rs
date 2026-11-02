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

//! Sizing observations, real resource custody and original control failures.
//! Domain identity is not a completed task-coverage proof.

use super::super::sizing::{SizingDomain, SizingDomainKind, SizingSketch};
use super::*;
use crate::runtime::{
    fragment::ExecutionFailureCause, tracked_state_allocator::expression_allocation_host,
};
use novarocks_failure_contract::{FailureClass, FailureProjection, ResourceDimension};
use novarocks_functions::{AggregateStateAllocator, KernelEvaluationControl, KernelFailure};
use novarocks_type_contract::{ValueTypeStorageHold, ValueTypeStorageLifetime};
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

fn failure<T>(result: ExecutionResult<T>) -> ExecutionFailure {
    match result {
        Err(cause) => cause,
        Ok(_) => panic!("the operation must retain its original refusal"),
    }
}

fn fixture(
    values: Vec<Option<i32>>,
    nulls: NullPolicy,
    owner: &dyn InputConstructionOwner,
) -> (KeySpec, LocalHashPolicy, LocalHashBatch) {
    let ty = FunctionValueType::new(DataType::Int32, true);
    let spec = KeySpec::new(std::slice::from_ref(&ty), &[nulls], owner, &|| Ok(())).unwrap();
    let policy = LocalHashPolicy::with_seed(&spec, 601, owner).unwrap();
    let rows: Vec<_> = (0..values.len()).collect();
    let keys = input(&[Arc::new(Int32Array::from(values))], &[ty], &rows, owner);
    let hashes = hash(&keys, &spec, &policy, owner);
    (spec, policy, hashes)
}

fn host(tracker: &Arc<MemTracker>) -> Arc<dyn AggregateStateAllocator> {
    expression_allocation_host(tracker.clone())
}

fn assert_capacity(cause: &ExecutionFailure, installed_limit: i64) {
    let ExecutionFailureCause::Kernel(KernelFailure::Capacity(original)) = cause.cause() else {
        panic!("the real opaque host must preserve its compact capacity cause")
    };
    assert_eq!(cause.failure_fact(), original.failure_fact());
    let fact = cause.failure_fact();
    assert_eq!(fact.class(), FailureClass::Resource);
    let resource = fact
        .resource()
        .expect("the original byte-limit facts survive");
    assert_eq!(resource.dimension, ResourceDimension::Bytes);
    assert_eq!(resource.limit, Some(installed_limit as u64));
    assert!(resource.requested.is_some_and(|n| n > 0));
    assert!(
        resource
            .committed
            .is_some_and(|n| n > installed_limit as u64)
    );
}

#[test]
fn sizing_hash_zero_participates_and_duplicate_occurrences_remain_one_member() {
    let owner = owner();
    let (spec, policy, hashes) = fixture(
        vec![Some(17), None, Some(17)],
        NullPolicy::Unmatchable,
        owner.as_ref(),
    );
    let hashes = hashes.force_hash(0);
    let domain =
        SizingDomain::new(&spec, &policy, SizingDomainKind::WholeInput, owner.as_ref()).unwrap();
    let tracker = MemTracker::new_root("sizing zero hash");
    let mut sketch = SizingSketch::new(&domain, host(&tracker), &CopyControl).unwrap();
    sketch
        .update_occurrences(&hashes, [1], &CopyControl)
        .unwrap();
    assert_eq!(sketch.estimate_observed(&CopyControl).unwrap(), 0);
    sketch
        .update_occurrences(&hashes, [0, 2, 0, 2], &CopyControl)
        .unwrap();
    assert_eq!(sketch.estimate_observed(&CopyControl).unwrap(), 1);
    let payload = sketch.seal(&CopyControl).unwrap();
    assert_eq!(tracker.current() as usize, payload.retained_upper_bytes());
    drop(payload);
    assert_eq!(tracker.current(), 0);

    let (spec, policy, hashes) = fixture(
        vec![Some(2), Some(4), Some(6), Some(2), Some(4)],
        NullPolicy::Equal,
        owner.as_ref(),
    );
    let domain =
        SizingDomain::new(&spec, &policy, SizingDomainKind::WholeInput, owner.as_ref()).unwrap();
    let mut sketch = SizingSketch::new(&domain, host(&tracker), &CopyControl).unwrap();
    sketch
        .update_occurrences(&hashes, (0..3_000).map(|n| n % hashes.len()), &CopyControl)
        .unwrap();
    assert_eq!(sketch.estimate_observed(&CopyControl).unwrap(), 3);
    drop(sketch);
    assert_eq!(tracker.current(), 0);
}

struct MergeCustodyControl {
    source: Arc<MemTracker>,
    target: Arc<MemTracker>,
    payload_bytes: i64,
    target_peak: AtomicI64,
    calls: AtomicUsize,
}
impl KernelEvaluationControl for MergeCustodyControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        assert_eq!(self.source.current(), self.payload_bytes);
        self.target_peak
            .fetch_max(self.target.current(), Ordering::SeqCst);
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("sizing operations never wait")
    }
}

#[test]
fn sizing_same_domain_payload_crosses_real_hosts_without_double_charging_input() {
    let owner = owner();
    let (spec, policy, hashes) = fixture(
        (0..64).map(Some).collect(),
        NullPolicy::Equal,
        owner.as_ref(),
    );
    let domain = SizingDomain::new(
        &spec,
        &policy,
        SizingDomainKind::Partition(3),
        owner.as_ref(),
    )
    .unwrap();
    let shared_domain = domain.clone();
    let source = MemTracker::new_root("sizing source host");
    let target = MemTracker::new_root("sizing target host");
    let mut left = SizingSketch::new(&domain, host(&source), &CopyControl).unwrap();
    left.update_occurrences(&hashes, 0..hashes.len(), &CopyControl)
        .unwrap();
    let expected = left.estimate_observed(&CopyControl).unwrap();
    let source_heap = left.retained_upper_bytes();
    let payload = left.serialize_observed(&CopyControl).unwrap();
    assert!(payload.domain().same_instance(&domain));
    assert_eq!(
        source.current() as usize,
        source_heap + payload.retained_upper_bytes()
    );
    drop(left);
    assert_eq!(source.current() as usize, payload.retained_upper_bytes());
    let mut right = SizingSketch::new(&shared_domain, host(&target), &CopyControl).unwrap();
    assert_eq!(target.current() as usize, right.retained_upper_bytes());
    let control = MergeCustodyControl {
        source: source.clone(),
        target: target.clone(),
        payload_bytes: source.current(),
        target_peak: AtomicI64::new(target.current()),
        calls: AtomicUsize::new(0),
    };
    right.merge_payload(payload, &control).unwrap();
    assert_eq!(source.current(), 0);
    assert!(control.calls.load(Ordering::SeqCst) > 0);
    assert_eq!(right.estimate_observed(&CopyControl).unwrap(), expected);
    assert_eq!(target.current() as usize, right.retained_upper_bytes());
    assert!(control.target_peak.load(Ordering::SeqCst) > target.current());
    drop(right);
    assert_eq!(target.current(), 0);
}

#[test]
fn sizing_independent_equal_domains_and_whole_partition_domains_do_not_merge() {
    let owner = owner();
    let (spec, policy, hashes) = fixture(vec![Some(2)], NullPolicy::Equal, owner.as_ref());
    let original =
        SizingDomain::new(&spec, &policy, SizingDomainKind::WholeInput, owner.as_ref()).unwrap();
    for kind in [SizingDomainKind::WholeInput, SizingDomainKind::Partition(0)] {
        let other = SizingDomain::new(&spec, &policy, kind, owner.as_ref()).unwrap();
        assert!(!original.same_instance(&other));
        let source = MemTracker::new_root("foreign sizing payload");
        let target = MemTracker::new_root("foreign sizing target");
        let mut partial = SizingSketch::new(&original, host(&source), &CopyControl).unwrap();
        partial
            .update_occurrences(&hashes, [0], &CopyControl)
            .unwrap();
        let payload = partial.seal(&CopyControl).unwrap();
        let mut destination = SizingSketch::new(&other, host(&target), &CopyControl).unwrap();
        let cause = failure(destination.merge_payload(payload, &CopyControl));
        assert_value_failure(&cause, ValueViolation::Layout);
        assert_eq!(source.current(), 0);
        assert_eq!(target.current(), 0);
        assert_eq!(failure(destination.estimate_observed(&CopyControl)), cause);
    }
}

#[test]
fn sizing_domain_checks_original_policy_before_funding_and_preserves_arc_request_refusal() {
    let owner = owner();
    let (spec, policy, hashes) = fixture(vec![Some(2)], NullPolicy::Equal, owner.as_ref());
    let (_, foreign, _) = fixture(vec![Some(2)], NullPolicy::Equal, owner.as_ref());
    let cause: ExecutionFailure = ConstructionFailure::Control(
        novarocks_type_contract::CompileControlError::ResourceExhausted,
    )
    .into();
    let refusing = RefusingOwner(owner.as_ref(), cause.clone());
    assert_value_failure(
        &failure(SizingDomain::new(
            &spec,
            &foreign,
            SizingDomainKind::WholeInput,
            &refusing,
        )),
        ValueViolation::Layout,
    );
    assert_eq!(
        failure(SizingDomain::new(
            &spec,
            &policy,
            SizingDomainKind::WholeInput,
            &refusing
        )),
        cause,
    );
    // A foreign policy for the same spec still cannot authorize saved hashes.
    let other_policy = LocalHashPolicy::with_seed(&spec, 601, owner.as_ref()).unwrap();
    let domain = SizingDomain::new(
        &spec,
        &other_policy,
        SizingDomainKind::WholeInput,
        owner.as_ref(),
    )
    .unwrap();
    let tracker = MemTracker::new_root("sizing foreign saved hash");
    let mut sketch = SizingSketch::new(&domain, host(&tracker), &CopyControl).unwrap();
    assert_value_failure(
        &failure(sketch.update_occurrences(&hashes, [0], &CopyControl)),
        ValueViolation::Layout,
    );
    assert_eq!(tracker.current(), 0);
}

struct StopControl {
    calls: AtomicUsize,
    stop: usize,
}
impl StopControl {
    fn counting() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            stop: usize::MAX,
        }
    }
    fn stopping(stop: usize) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            stop,
        }
    }
}
impl KernelEvaluationControl for StopControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        if self.calls.fetch_add(1, Ordering::SeqCst) + 1 == self.stop {
            Err(KernelFailure::DeadlineExceeded)
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("sizing operations never wait")
    }
}

#[test]
fn sizing_post_mutation_control_latches_but_readonly_serialization_refusal_does_not_poison() {
    let owner = owner();
    let (spec, policy, hashes) = fixture(vec![Some(31)], NullPolicy::Equal, owner.as_ref());
    let domain =
        SizingDomain::new(&spec, &policy, SizingDomainKind::WholeInput, owner.as_ref()).unwrap();
    let tracker = MemTracker::new_root("sizing mutation control");
    let mut baseline = SizingSketch::new(&domain, host(&tracker), &CopyControl).unwrap();
    let count = StopControl::counting();
    baseline.update_occurrences(&hashes, [0], &count).unwrap();
    let calls = count.calls.load(Ordering::SeqCst);
    assert!(calls > 2);
    drop(baseline);
    assert_eq!(tracker.current(), 0);
    let mut sketch = SizingSketch::new(&domain, host(&tracker), &CopyControl).unwrap();
    // The final observation closes update_occurrences; the preceding one
    // observes the already-mutated handle after original retained reconciliation.
    let control = StopControl::stopping(calls - 1);
    let cause = failure(sketch.update_occurrences(&hashes, [0], &control));
    assert_eq!(
        cause,
        ExecutionFailure::from(KernelFailure::DeadlineExceeded)
    );
    assert_eq!(control.calls.load(Ordering::SeqCst), calls - 1);
    assert_eq!(tracker.current(), 0);
    assert_eq!(failure(sketch.estimate_observed(&CopyControl)), cause);
    assert_eq!(
        failure(sketch.update_occurrences(&hashes, [0], &CopyControl)),
        cause
    );

    let mut sketch = SizingSketch::new(&domain, host(&tracker), &CopyControl).unwrap();
    sketch
        .update_occurrences(&hashes, [0], &CopyControl)
        .unwrap();
    let live = tracker.current();
    let count = StopControl::counting();
    let payload = sketch.serialize_observed(&count).unwrap();
    let calls = count.calls.load(Ordering::SeqCst);
    assert!(calls > 2);
    drop(payload);
    assert_eq!(tracker.current(), live);
    // The penultimate boundary observes the actual library-produced Vec.
    let control = StopControl::stopping(calls - 1);
    assert_eq!(
        failure(sketch.serialize_observed(&control)),
        ExecutionFailure::from(KernelFailure::DeadlineExceeded)
    );
    assert_eq!(tracker.current(), live);
    assert_eq!(sketch.estimate_observed(&CopyControl).unwrap(), 1);
    drop(sketch.serialize_observed(&CopyControl).unwrap());
    assert_eq!(tracker.current(), live);
    drop(sketch);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn sizing_real_memory_limits_refuse_create_growth_serialize_and_merge_with_refunds() {
    let owner = owner();
    let (spec, policy, hashes) = fixture(
        (0..1_024).map(Some).collect(),
        NullPolicy::Equal,
        owner.as_ref(),
    );
    let domain =
        SizingDomain::new(&spec, &policy, SizingDomainKind::WholeInput, owner.as_ref()).unwrap();

    let tracker = MemTracker::new_root("sizing create limit");
    tracker.install_limit_once(1).unwrap();
    assert_capacity(
        &failure(SizingSketch::new(&domain, host(&tracker), &CopyControl)),
        1,
    );
    assert_eq!(tracker.current(), 0);

    let tracker = MemTracker::new_root("sizing actual growth limit");
    let mut sketch = SizingSketch::new(&domain, host(&tracker), &CopyControl).unwrap();
    let limit = tracker.current();
    assert!(limit > 0);
    tracker.install_limit_once(limit).unwrap();
    let cause = failure(sketch.update_occurrences(&hashes, 0..hashes.len(), &CopyControl));
    assert_capacity(&cause, limit);
    assert_eq!(tracker.current(), 0);
    assert_eq!(failure(sketch.estimate_observed(&CopyControl)), cause);
    drop(sketch);
    assert_eq!(tracker.current(), 0);

    let tracker = MemTracker::new_root("sizing serialization limit");
    let mut sketch = SizingSketch::new(&domain, host(&tracker), &CopyControl).unwrap();
    sketch
        .update_occurrences(&hashes, 0..3, &CopyControl)
        .unwrap();
    let limit = tracker.current();
    assert!(limit > 0);
    tracker.install_limit_once(limit).unwrap();
    assert_capacity(&failure(sketch.serialize_observed(&CopyControl)), limit);
    assert_eq!(tracker.current(), limit);
    assert_eq!(sketch.estimate_observed(&CopyControl).unwrap(), 3);
    drop(sketch);
    assert_eq!(tracker.current(), 0);

    let source = MemTracker::new_root("sizing merge input custody");
    let target = MemTracker::new_root("sizing merge target limit");
    let mut partial = SizingSketch::new(&domain, host(&source), &CopyControl).unwrap();
    partial
        .update_occurrences(&hashes, 0..64, &CopyControl)
        .unwrap();
    let payload = partial.seal(&CopyControl).unwrap();
    assert_eq!(source.current() as usize, payload.retained_upper_bytes());
    let mut destination = SizingSketch::new(&domain, host(&target), &CopyControl).unwrap();
    let limit = target.current();
    assert!(limit > 0);
    target.install_limit_once(limit).unwrap();
    let cause = failure(destination.merge_payload(payload, &CopyControl));
    assert_capacity(&cause, limit);
    assert_eq!(source.current(), 0);
    assert_eq!(target.current(), 0);
    assert_eq!(failure(destination.estimate_observed(&CopyControl)), cause);
    drop(destination);
    assert_eq!(target.current(), 0);
}

#[test]
fn sizing_domain_last_alias_retains_its_original_construction_creator() {
    struct Tag(Arc<AtomicUsize>);
    impl ValueTypeStorageHold for Tag {}
    impl Drop for Tag {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let owner = owner();
    let (spec, policy, hashes) = fixture(vec![Some(4)], NullPolicy::Equal, owner.as_ref());
    let drops = Arc::new(AtomicUsize::new(0));
    let tagged = TaggedCreator {
        original: owner.as_ref(),
        storage: owner
            .storage_lifetime()
            .retaining_observed::<ConstructionFailure>(
                &ValueTypeStorageLifetime::held(Arc::new(Tag(drops.clone()))),
                |layout| owner.request(layout),
            )
            .unwrap(),
    };
    let domain = SizingDomain::new(&spec, &policy, SizingDomainKind::WholeInput, &tagged).unwrap();
    let tracker = MemTracker::new_root("sizing last domain alias");
    let mut sketch = SizingSketch::new(&domain, host(&tracker), &CopyControl).unwrap();
    sketch
        .update_occurrences(&hashes, [0], &CopyControl)
        .unwrap();
    let payload = sketch.seal(&CopyControl).unwrap();
    let last_alias = payload.domain().clone();
    drop(domain);
    drop(tagged);
    drop(hashes);
    drop(policy);
    drop(spec);
    drop(owner);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(payload);
    assert_eq!(tracker.current(), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(last_alias.kind(), SizingDomainKind::WholeInput);
    drop(last_alias);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
