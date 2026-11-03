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

//! Bitmap initialization, address validity and finite creator lifetime.
use super::super::join_completion::tests::{Control, error, fixture};
use super::*;
use crate::exec::{
    chunk::{ArrayNormalizationStep, ArrayPathStep},
    pipeline::operator::ConstructionFailure,
};
use novarocks_functions::{KernelEvaluationControl, KernelFailure};
use std::{alloc::Layout, sync::atomic::AtomicUsize, thread};

fn initializer(
    rows: usize,
    workers: usize,
    owner: &dyn InputConstructionOwner,
) -> Arc<BuildMatchBitmapInit> {
    BuildMatchBitmapInit::new(
        rows,
        NonZeroUsize::new(workers).unwrap(),
        Arc::new(Observable::new()),
        owner,
        &Control,
    )
    .unwrap()
}
fn claim(init: &Arc<BuildMatchBitmapInit>, creator: &dyn InputConstructionOwner) -> InitObligation {
    match init.claim(creator, &Control).unwrap() {
        BitmapInitClaim::Range(lease) => lease,
        _ => panic!("expected a real initialization range"),
    }
}
fn fill(
    init: &Arc<BuildMatchBitmapInit>,
    creator: &dyn InputConstructionOwner,
) -> BuildMatchBitmap {
    loop {
        match init.claim(creator, &Control).unwrap() {
            BitmapInitClaim::Range(mut lease) => {
                loop {
                    match lease.initialize(1, &Control).unwrap() {
                        InitProgress::Progressed => {}
                        InitProgress::Initialized => break,
                        _ => panic!("one real word of turn budget"),
                    }
                }
                lease.complete().unwrap();
            }
            BitmapInitClaim::Finished => break,
            _ => panic!("single worker has no outstanding range or withdrawn demand"),
        }
    }
    match init.try_freeze().unwrap() {
        BitmapFreeze::Ready(bitmap) => bitmap,
        _ => panic!("all original ranges completed"),
    }
}

#[test]
fn exact_word_domain_zero_and_pending_initialization_cannot_escape_as_ready() {
    let (owner, _) = fixture();
    for rows in [0, 1, 63, 64, 65, 129] {
        let init = initializer(rows, 1, owner.as_ref());
        if rows > 0 {
            assert!(matches!(init.try_freeze().unwrap(), BitmapFreeze::Pending));
            let lease = claim(&init, owner.as_ref());
            assert!(matches!(init.try_freeze().unwrap(), BitmapFreeze::Pending));
            drop(lease);
            assert!(init.try_freeze().is_err());
        }
        let init = initializer(rows, 2, owner.as_ref());
        let bitmap = fill(&init, owner.as_ref());
        assert_eq!(bitmap.rows(), rows);
        assert_eq!(bitmap.word_count(), rows / 64 + usize::from(rows % 64 != 0));
        assert_eq!(bitmap.segments.len(), bitmap.word_count().min(2));
        let mut next = 0;
        let mut actual_words = 0;
        for (id, slot) in bitmap.segments.iter().enumerate() {
            let segment = slot.get().unwrap();
            assert_eq!(
                segment.range,
                segment_words(bitmap.word_count(), bitmap.segments.len(), id).unwrap()
            );
            assert_eq!(segment.range.start, next);
            assert_eq!(segment.words.len(), segment.range.len());
            actual_words += segment.words.len();
            next = segment.range.end;
            for word in segment.range.clone() {
                assert_eq!(
                    segment_for_word(bitmap.word_count(), bitmap.segments.len(), word).unwrap(),
                    id
                );
            }
        }
        assert_eq!(actual_words, bitmap.word_count());
        assert_eq!(next, bitmap.word_count());
        assert!(matches!(init.try_freeze().unwrap(), BitmapFreeze::Frozen));
        assert!(matches!(
            init.claim(owner.as_ref(), &Control).unwrap(),
            BitmapInitClaim::Finished
        ));
    }
    let budgeted = initializer(130, 1, owner.as_ref());
    let mut turn = claim(&budgeted, owner.as_ref());
    let calls = CountWork(AtomicUsize::new(0));
    assert_eq!(turn.initialize(0, &calls).unwrap(), InitProgress::Yielded);
    assert_eq!(turn.remaining_words(), 3);
    assert_eq!(calls.0.load(Ordering::Relaxed), 0);
    for remaining in [2, 1, 0] {
        let progress = turn.initialize(1, &calls).unwrap();
        assert_eq!(turn.remaining_words(), remaining);
        assert_eq!(
            progress,
            if remaining == 0 {
                InitProgress::Initialized
            } else {
                InitProgress::Progressed
            }
        );
    }
    assert_eq!(calls.0.load(Ordering::Relaxed), 3);
    assert_eq!(
        turn.initialize(0, &calls).unwrap(),
        InitProgress::Initialized
    );
    assert_eq!(calls.0.load(Ordering::Relaxed), 3);
    let pending_identity = Arc::as_ptr(turn.pending.as_ref().unwrap());
    turn.complete().unwrap();
    let BitmapFreeze::Ready(budgeted_ready) = budgeted.try_freeze().unwrap() else {
        panic!("exact segment completed")
    };
    assert_eq!(
        pending_identity,
        Arc::as_ptr(budgeted_ready.segments[0].get().unwrap())
    );
    let init = initializer(64, 1, owner.as_ref());
    let mut unfinished = claim(&init, owner.as_ref());
    let cause = error(unfinished.complete());
    assert!(error(init.try_freeze()).same_occurrence(&cause));
    drop(unfinished);
    let left = initializer(64, 1, owner.as_ref());
    let right = initializer(64, 1, owner.as_ref());
    let mut left_lease = claim(&left, owner.as_ref());
    let mut right_lease = claim(&right, owner.as_ref());
    // Only this private test can attempt the cross-owner lease substitution.
    // Validation must fail before either allocation is written.
    std::mem::swap(&mut left_lease.obligation, &mut right_lease.obligation);
    assert!(left_lease.initialize(1, &Control).is_err());
    assert!(right_lease.initialize(1, &Control).is_err());
    assert!(left.try_freeze().is_err());
    assert!(right.try_freeze().is_err());
    drop((left_lease, right_lease));
    // This is an address-domain calculation, not a quota or huge allocation.
    assert_eq!(words_for_rows(usize::MAX).unwrap(), usize::MAX / 64 + 1);
    assert!(Layout::array::<AtomicU64>(words_for_rows(usize::MAX).unwrap()).is_ok());
}

#[test]
fn concurrent_disjoint_init_ranges_cover_each_word_once_and_freeze_only_after_barrier() {
    let (owner, _) = fixture();
    let rows = 257 * 64 + 13;
    let init = initializer(rows, 4, owner.as_ref());
    let creator = owner.as_ref();
    let start = Arc::new(std::sync::Barrier::new(4));
    let mut ranges = thread::scope(|scope| {
        let mut tasks = Vec::new();
        for _ in 0..4 {
            let init = Arc::clone(&init);
            let start = start.clone();
            tasks.push(scope.spawn(move || {
                let mut first = Some(claim(&init, creator));
                start.wait(); // Four distinct leases exist before any write.
                let mut claimed = Vec::new();
                loop {
                    match first
                        .take()
                        .map(BitmapInitClaim::Range)
                        .unwrap_or_else(|| init.claim(creator, &Control).unwrap())
                    {
                        BitmapInitClaim::Range(mut lease) => {
                            let range = lease.words();
                            assert!(matches!(init.try_freeze().unwrap(), BitmapFreeze::Pending));
                            loop {
                                let before = lease.remaining_words();
                                let progress = lease.initialize(1, &Control).unwrap();
                                assert_eq!(before - lease.remaining_words(), 1);
                                if progress == InitProgress::Initialized {
                                    break;
                                }
                                assert_eq!(progress, InitProgress::Progressed);
                            }
                            // A repeated call reaches the cursor end, not a second write.
                            assert_eq!(
                                lease.initialize(1, &Control).unwrap(),
                                InitProgress::Initialized
                            );
                            assert!(matches!(init.try_freeze().unwrap(), BitmapFreeze::Pending));
                            lease.complete().unwrap();
                            claimed.push(range);
                        }
                        BitmapInitClaim::Pending | BitmapInitClaim::Finished => break,
                        BitmapInitClaim::Retired => panic!("no withdrawal"),
                    }
                }
                claimed
            }));
        }
        tasks
            .into_iter()
            .flat_map(|task| task.join().unwrap())
            .collect::<Vec<_>>()
    });
    ranges.sort_by_key(|range| range.start);
    let mut next = 0;
    for range in ranges {
        assert_eq!(range.start, next);
        assert!(range.end > range.start);
        next = range.end;
    }
    assert_eq!(next, words_for_rows(rows).unwrap());
    assert_eq!(
        init.state.lock().unwrap().segments.as_ref().unwrap().len(),
        4
    );
    let BitmapFreeze::Ready(bitmap) = init.try_freeze().unwrap() else {
        panic!("all initializer threads returned")
    };
    for row in 0..rows {
        assert!(!bitmap.is_marked(row).unwrap());
    }
}

#[test]
fn shared_atomic_marks_preserve_edges_same_word_updates_and_checked_rows() {
    let (owner, _) = fixture();
    let bitmap = fill(&initializer(130, 1, owner.as_ref()), owner.as_ref());
    thread::scope(|scope| {
        for row in [0, 1, 63, 64, 65, 127, 128, 129] {
            let bitmap = &bitmap;
            scope.spawn(move || {
                bitmap.mark(row).unwrap();
                bitmap.mark(row).unwrap();
            });
        }
    });
    for row in 0..130 {
        assert_eq!(
            bitmap.is_marked(row).unwrap(),
            [0, 1, 63, 64, 65, 127, 128, 129].contains(&row)
        );
    }
    for invalid in [130, usize::MAX] {
        assert!(bitmap.mark(invalid).is_err());
        assert!(bitmap.is_marked(invalid).is_err());
    }
    let empty = fill(&initializer(0, 1, owner.as_ref()), owner.as_ref());
    assert!(empty.mark(0).is_err());
    assert!(empty.is_marked(0).is_err());
}

struct CountWork(AtomicUsize);
impl KernelEvaluationControl for CountWork {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        self.0.fetch_add(units as usize, Ordering::Relaxed);
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("no sleeping")
    }
}
struct CancelAfterTwoWords(AtomicUsize, std::sync::Weak<BuildMatchBitmapInit>);
impl KernelEvaluationControl for CancelAfterTwoWords {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert_eq!(units, 1);
        assert!(
            self.1.upgrade().unwrap().state.try_lock().is_ok(),
            "checkpoint must run outside the shared state lock"
        );
        if self.0.fetch_add(1, Ordering::Relaxed) == 2 {
            Err(KernelFailure::Cancelled)
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("no sleeping")
    }
}
#[test]
fn partial_cancel_and_abandoned_lease_latch_first_cause_and_notify_original_observable() {
    let (owner, _) = fixture();
    let changed = Arc::new(Observable::new());
    let init = BuildMatchBitmapInit::new(
        1024,
        NonZeroUsize::new(2).unwrap(),
        changed.clone(),
        owner.as_ref(),
        &Control,
    )
    .unwrap();
    let mut lease = claim(&init, owner.as_ref());
    let before = changed.generation();
    let cancelled = error(lease.initialize(
        8,
        &CancelAfterTwoWords(AtomicUsize::new(0), Arc::downgrade(&init)),
    ));
    assert_eq!(lease.pending.as_ref().unwrap().words.len(), 2);
    assert!(error(lease.initialize(1, &Control)).same_occurrence(&cancelled));
    assert_eq!(
        lease.pending.as_ref().unwrap().words.len(),
        2,
        "cancelled prefix cannot be rewritten on retry"
    );
    assert!(changed.generation() > before);
    let after_failure = changed.generation();
    assert!(error(init.check()).same_occurrence(&cancelled));
    assert_eq!(
        changed.generation(),
        after_failure,
        "read-only failure recheck must not notify recursively"
    );
    let before_drop = changed.generation();
    drop(lease);
    assert!(changed.generation() > before_drop);
    assert!(error(init.try_freeze()).same_occurrence(&cancelled));
    assert!(
        init.fail(build_contract("later failure"))
            .same_occurrence(&cancelled)
    );
    drop(init); // Partially initialized AtomicU64 has no destructor to run.

    let init = initializer(256, 1, owner.as_ref());
    let lease = claim(&init, owner.as_ref());
    let changed = init.changed.clone();
    let before = changed.generation();
    drop(lease);
    assert!(changed.generation() > before);
    let abandoned = error(init.check());
    assert!(error(init.claim(owner.as_ref(), &Control)).same_occurrence(&abandoned));
    assert!(error(init.try_freeze()).same_occurrence(&abandoned));
}

#[test]
fn explicit_normal_retirement_does_not_certify_partial_words_as_initialized() {
    let (owner, _) = fixture();
    let init = initializer(256, 4, owner.as_ref());
    let mut initialized = claim(&init, owner.as_ref());
    let mut untouched = claim(&init, owner.as_ref());
    assert_eq!(
        initialized.initialize(1, &Control).unwrap(),
        InitProgress::Initialized
    );
    init.retire().unwrap();
    assert_eq!(
        untouched.initialize(1, &Control).unwrap(),
        InitProgress::Retired
    );
    assert_eq!(untouched.pending.as_ref().unwrap().words.len(), 0);
    initialized.retire().unwrap();
    untouched.retire().unwrap();
    drop((initialized, untouched));
    init.check().unwrap();
    assert!(matches!(
        init.claim(owner.as_ref(), &Control).unwrap(),
        BitmapInitClaim::Retired
    ));
    assert!(matches!(init.try_freeze().unwrap(), BitmapFreeze::Retired));
    // Already-frozen data remains a real caller-owned value on withdrawal.
    let init = initializer(1, 1, owner.as_ref());
    let ready = fill(&init, owner.as_ref());
    init.retire().unwrap();
    assert!(matches!(init.try_freeze().unwrap(), BitmapFreeze::Retired));
    assert!(!ready.is_marked(0).unwrap());
}

struct Witness {
    _last: Arc<()>,
    _original: ValueTypeStorageLifetime,
}
impl novarocks_type_contract::ValueTypeStorageHold for Witness {}
struct WitnessOwner {
    actual: Box<dyn InputConstructionOwner>,
    storage: ValueTypeStorageLifetime,
}
impl InputConstructionOwner for WitnessOwner {
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.storage.clone()
    }
    fn request(&self, layout: Layout) -> Result<(), ConstructionFailure> {
        self.actual.request(layout)
    }
    fn observe_opaque_upper_bound(&self, bytes: usize) -> Result<(), ConstructionFailure> {
        self.actual.observe_opaque_upper_bound(bytes)
    }
    fn observe(
        &self,
        occurrence: usize,
        path: &[ArrayPathStep],
        step: ArrayNormalizationStep<'_>,
    ) -> Result<(), ConstructionFailure> {
        self.actual.observe(occurrence, path, step)
    }
}
fn witnessed_owner() -> (WitnessOwner, std::sync::Weak<()>) {
    let (actual, _) = fixture();
    let last = funded_shared(actual.as_ref(), ()).unwrap();
    let weak = Arc::downgrade(&last);
    let storage = ValueTypeStorageLifetime::held(
        funded_shared(
            actual.as_ref(),
            Witness {
                _original: actual.storage_lifetime(),
                _last: last,
            },
        )
        .unwrap(),
    );
    (WitnessOwner { actual, storage }, weak)
}
#[test]
fn ready_retains_last_original_creator_without_retaining_initializer_or_observable() {
    let (directory_owner, directory_last) = witnessed_owner();
    let (first_worker, first_last) = witnessed_owner();
    let (second_worker, second_last) = witnessed_owner();
    let init = initializer(129, 2, &directory_owner);
    let weak_init = Arc::downgrade(&init);
    let weak_observable = Arc::downgrade(&init.changed);
    for creator in [&first_worker, &second_worker] {
        let mut lease = claim(&init, creator);
        loop {
            if lease.initialize(1, &Control).unwrap() == InitProgress::Initialized {
                break;
            }
        }
        lease.complete().unwrap();
    }
    let BitmapFreeze::Ready(ready) = init.try_freeze().unwrap() else {
        panic!("all segments complete")
    };
    let ready = funded_shared(&directory_owner, ready).unwrap();
    let last_bitmap = ready.clone();
    drop((directory_owner, first_worker, second_worker));
    drop(init);
    assert!(weak_init.upgrade().is_none());
    assert!(weak_observable.upgrade().is_none());
    for creator in [&directory_last, &first_last, &second_last] {
        assert!(creator.upgrade().is_some());
    }
    drop(ready);
    last_bitmap.mark(128).unwrap();
    assert!(last_bitmap.is_marked(128).unwrap());
    for creator in [&directory_last, &first_last, &second_last] {
        assert!(creator.upgrade().is_some());
    }
    drop(last_bitmap);
    for creator in [&directory_last, &first_last, &second_last] {
        assert!(creator.upgrade().is_none());
    }
}

#[test]
fn relaxed_bitmap_read_after_original_join_tail_gate_precedes_thread_join() {
    use super::super::join_completion::tests::participants;
    use super::super::join_completion::{JoinCompletion, PublishResult, TailStatus};
    use crate::exec::operators::local_exchanger::LocalExchanger;
    let (owner, _) = fixture();
    let bitmap = funded_shared(
        owner.as_ref(),
        fill(&initializer(129, 2, owner.as_ref()), owner.as_ref()),
    )
    .unwrap();
    let installed = participants(2, 1, 1);
    let handoff =
        LocalExchanger::new_handoff(installed.handoff_producers().unwrap().unwrap(), 1, 4);
    let completion = JoinCompletion::new(
        installed,
        Some(handoff),
        Arc::new(Observable::new()),
        owner.as_ref(),
        &Control,
    )
    .unwrap();
    assert!(matches!(
        completion.publish(bitmap.clone(), 129).unwrap(),
        PublishResult::Installed
    ));
    let probe = completion.clone();
    let marked = bitmap.clone();
    let worker = thread::spawn(move || {
        marked.mark(128).unwrap();
        probe.normal_after_output(0).unwrap();
    });
    loop {
        match completion.tail_status().unwrap() {
            TailStatus::Pending => thread::yield_now(),
            TailStatus::Ready(published) => {
                assert_eq!(published.rows, 129);
                assert!(published.artifact.is_marked(128).unwrap());
                break;
            }
            _ => panic!("only original normal probe completion releases the tail"),
        }
    }
    // Joining the thread is after the Relaxed load; it cannot supply that HB.
    worker.join().unwrap();
}

#[test]
fn normal_retirement_between_last_initialization_and_settle_is_not_failure() {
    let (owner, _) = fixture();
    let init = initializer(129, 2, owner.as_ref());
    let mut lease = claim(&init, owner.as_ref());
    while lease.initialize(1, &Control).unwrap() != InitProgress::Initialized {}
    init.retire().unwrap();
    assert_eq!(lease.settle().unwrap(), InitProgress::Retired);
    drop(lease);
    init.check().unwrap();
    assert!(matches!(init.try_freeze().unwrap(), BitmapFreeze::Retired));
}
