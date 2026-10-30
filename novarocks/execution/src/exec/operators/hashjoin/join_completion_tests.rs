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

//! Protocol tests only. Scalar I/A fixtures do not prove payload/capture wiring.
use super::*;
use crate::runtime::mem_tracker::MemTracker;
use novarocks_functions::{KernelEvaluationControl, KernelFailure};
use std::sync::atomic::{AtomicUsize, Ordering};
pub(crate) struct Control;
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("protocol tests do not sleep")
    }
}
pub(crate) fn fixture() -> (Box<dyn InputConstructionOwner>, Arc<MemTracker>) {
    let tracker = MemTracker::new_root("join protocol proposal original metadata owner");
    let factory=crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),tracker.clone(),
        Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default())).unwrap();
    let layout = crate::exec::chunk::typed_fixture::layout(
        Arc::new(arrow::datatypes::Schema::empty()),
        &[],
        &[],
    );
    let schema = factory.create_compiled_chunk_schema(&layout).unwrap();
    (factory.begin_typed_construction(&schema).unwrap(), tracker)
}
pub(crate) fn participants(inputs: usize, probes: usize, tails: usize) -> JoinParticipants {
    JoinParticipants {
        inputs,
        finalizers: NonZeroUsize::new(3).unwrap(),
        probes,
        tails,
    }
}
pub(crate) fn completion(
    probes: usize,
    tails: usize,
    owner: &dyn InputConstructionOwner,
) -> (Arc<JoinCompletion<u64>>, Option<Arc<LocalExchanger>>) {
    let p = participants(2, probes, tails);
    let handoff = p
        .handoff_producers()
        .unwrap()
        .map(|count| LocalExchanger::new_handoff(count, 1, 4));
    let completion = JoinCompletion::new(
        p,
        handoff.clone(),
        Arc::new(Observable::new()),
        owner,
        &Control,
    )
    .unwrap();
    (completion, handoff)
}
pub(crate) fn error<T>(value: ExecutionResult<T>) -> ExecutionFailure {
    match value {
        Ok(_) => panic!("expected original failure"),
        Err(cause) => cause,
    }
}
#[test]
fn mixed_normal_and_withdrawn_probe_only_releases_demand_without_tail_or_publication() {
    let (owner, _) = fixture();
    let (c, h) = completion(3, 0, owner.as_ref());
    assert!(h.is_none());
    c.normal_after_output(0).unwrap();
    assert!(!c.withdraw_if_pending(0).unwrap());
    c.withdraw(2).unwrap();
    assert_eq!(c.remaining_demand().unwrap(), RemainingDemand::Needed);
    c.normal_after_output(1).unwrap();
    assert_eq!(c.remaining_demand().unwrap(), RemainingDemand::Released);
    assert!(c.published().unwrap().is_none());
    assert!(matches!(
        c.publish(Arc::new(9), 0).unwrap(),
        PublishResult::NotNeeded(_)
    ));
    assert!(c.published().unwrap().is_none());
    assert!(c.tail_status().is_err());
}
#[test]
fn tail_requires_publication_and_every_probe_normal_under_same_mutex() {
    let (owner, _) = fixture();
    let (c, _) = completion(2, 2, owner.as_ref());
    c.normal_after_output(1).unwrap();
    assert!(matches!(c.tail_status().unwrap(), TailStatus::Pending));
    assert!(matches!(
        c.publish(Arc::new(7), 9).unwrap(),
        PublishResult::Installed
    ));
    assert!(matches!(c.tail_status().unwrap(), TailStatus::Pending));
    let mark = Arc::new(AtomicUsize::new(0));
    let written = mark.clone();
    let complete = c.clone();
    let writer = std::thread::spawn(move || {
        written.store(17, Ordering::Relaxed);
        complete.normal_after_output(0).unwrap();
    });
    loop {
        match c.tail_status().unwrap() {
            TailStatus::Pending => std::thread::yield_now(),
            TailStatus::Ready(Published { rows: 9, .. }) => break,
            _ => panic!("normal probe settle"),
        }
    }
    // Join is deliberately AFTER this read: the join itself must not supply
    // the bitmap happens-before edge that this same-mutex test exercises.
    assert_eq!(mark.load(Ordering::Relaxed), 17);
    writer.join().unwrap();
    assert!(!c.withdraw_if_pending(0).unwrap());
}
#[test]
fn partial_handoff_consumer_exit_does_not_shorten_matching_but_all_exit_does() {
    let (owner, _) = fixture();
    let p = participants(2, 2, 2);
    let h = LocalExchanger::new_handoff(4, 2, 4);
    let c = JoinCompletion::<u64>::new(
        p,
        Some(h.clone()),
        Arc::new(Observable::new()),
        owner.as_ref(),
        &Control,
    )
    .unwrap();
    h.close_consumer(0);
    assert_eq!(c.remaining_demand().unwrap(), RemainingDemand::Needed);
    h.close_consumer(1);
    assert_eq!(c.remaining_demand().unwrap(), RemainingDemand::Released);
    assert!(matches!(c.tail_status().unwrap(), TailStatus::Withdrawn));
    let cause: ExecutionFailure = KernelFailure::DeadlineExceeded.into();
    c.fail(cause.clone());
    assert_eq!(error(c.remaining_demand()), cause);
    assert_eq!(error(c.tail_status()), cause);
}
#[test]
fn duplicate_probe_and_publication_cannot_create_additional_success_or_hide_first_error() {
    let (owner, _) = fixture();
    let (c, _) = completion(2, 1, owner.as_ref());
    c.normal_after_output(0).unwrap();
    let cause = error(c.normal_after_output(0));
    assert_eq!(error(c.published()), cause);
    let (other, _) = completion(2, 1, owner.as_ref());
    other.publish(Arc::new(1), 0).unwrap();
    let cause = error(other.publish(Arc::new(2), 0));
    assert_eq!(error(other.tail_status()), cause);
}
#[test]
fn pending_wait_registration_recheck_observes_original_generation_without_sleep() {
    let (owner, _) = fixture();
    let (c, _) = completion(1, 1, owner.as_ref());
    let observable = c.observable();
    let before = observable.generation();
    assert!(matches!(c.tail_status().unwrap(), TailStatus::Pending));
    // Transition can race registration: register then recheck BOTH predicate
    // and original generation. No callback alone is a publication witness.
    c.publish(Arc::new(1), 0).unwrap();
    let seen = Arc::new(AtomicUsize::new(0));
    let calls = seen.clone();
    let subscription = observable.subscribe(Arc::new(move || {
        calls.fetch_add(1, Ordering::Relaxed);
    }));
    assert_ne!(observable.generation(), before);
    c.normal_after_output(0).unwrap();
    assert!(seen.load(Ordering::Relaxed) > 0);
    assert!(matches!(c.tail_status().unwrap(), TailStatus::Ready(_)));
    drop(subscription);
}

#[test]
fn withdrawn_probe_does_not_become_normal_settle_or_ready_tail() {
    let (owner, _) = fixture();
    let (c, _) = completion(2, 1, owner.as_ref());
    c.publish(Arc::new(7), 4).unwrap();
    c.withdraw(0).unwrap();
    c.normal_after_output(1).unwrap();
    assert_eq!(c.remaining_demand().unwrap(), RemainingDemand::Needed);
    assert!(matches!(c.tail_status().unwrap(), TailStatus::Unavailable));
}
