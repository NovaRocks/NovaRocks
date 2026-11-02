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

//! Range ownership and phase barriers without scheduling or resource authority.
//! The caller funds the shared owner and supplies its original invariant cause.
//! Dropping unfinished work returns its lease but never reissues its range or
//! permits publication; the same first failure remains visible to every user.
// Design: ADR-0212 (docs/adr/ADR-0212-shared-join-stages-and-exact-demand-retirement.md)

use std::{
    num::NonZeroUsize,
    ops::Range,
    sync::{Arc, Mutex, MutexGuard},
};

/// A first-cause value protected by its caller's state lock. Failure and
/// phase mutation share that one lock, so no checked claim races a latch.
struct FailureLatch<E> {
    failure: Option<E>,
}
impl<E: Clone> FailureLatch<E> {
    fn new() -> Self {
        Self { failure: None }
    }
    fn check(&self) -> Result<(), E> {
        match &self.failure {
            Some(cause) => Err(cause.clone()),
            None => Ok(()),
        }
    }
    fn latch(&mut self, cause: E) -> E {
        self.failure.get_or_insert(cause).clone()
    }
}

#[derive(Clone, Copy)]
enum Enumeration {
    Dense { total: usize, width: NonZeroUsize },
    Open,
}
struct State<E> {
    generation: usize,
    issued: usize,
    completed: usize,
    inflight: usize,
    next: usize,
    exhausted: bool,
    enumeration: Enumeration,
    failure: FailureLatch<E>,
}
impl<E> State<E> {
    fn barrier(&self) -> bool {
        self.exhausted && self.inflight == 0 && self.completed == self.issued
    }
}

/// The original caller must allocate the Arc through its construction owner.
/// Claims clone that Arc; there is no allocation per range or task lease.
pub(crate) struct PhasedRangeWork<E: Clone, H = ()> {
    state: Mutex<State<E>>,
    abandoned: E,
    overflow: E,
    // Original creator retention outlives all backing held by this owner.
    _retention: H,
}
impl<E: Clone> PhasedRangeWork<E> {
    #[cfg(test)]
    pub(crate) fn new(abandoned: E) -> Self {
        Self::with_retention(abandoned.clone(), abandoned, ())
    }
}
impl<E: Clone, H> PhasedRangeWork<E, H> {
    pub(crate) fn with_retention(abandoned: E, overflow: E, retention: H) -> Self {
        Self {
            state: Mutex::new(State {
                generation: 0,
                issued: 0,
                completed: 0,
                inflight: 0,
                next: 0,
                exhausted: true,
                enumeration: Enumeration::Open,
                failure: FailureLatch::new(),
            }),
            abandoned,
            overflow,
            _retention: retention,
        }
    }
    pub(crate) fn check(&self) -> Result<(), E> {
        self.state().map(|_| ())
    }
    pub(crate) fn latch(&self, cause: E) -> E {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .failure
            .latch(cause)
    }
    fn state(&self) -> Result<MutexGuard<'_, State<E>>, E> {
        let state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                return Err(state.failure.latch(self.abandoned.clone()));
            }
        };
        state.failure.check()?;
        Ok(state)
    }
    fn invalid(&self, state: &mut State<E>) -> E {
        state.failure.latch(self.abandoned.clone())
    }
    fn overflow(&self, state: &mut State<E>) -> E {
        state.failure.latch(self.overflow.clone())
    }
    fn begin_enumeration(&self, enumeration: Enumeration) -> Result<(), E> {
        let mut state = self.state()?;
        if !state.barrier() {
            return Err(self.invalid(&mut state));
        }
        let Some(generation) = state.generation.checked_add(1) else {
            return Err(self.overflow(&mut state));
        };
        *state = State {
            generation,
            issued: 0,
            completed: 0,
            inflight: 0,
            next: 0,
            exhausted: matches!(enumeration, Enumeration::Dense { total: 0, .. }),
            enumeration,
            failure: FailureLatch::new(),
        };
        Ok(())
    }
    pub(crate) fn begin(&self, total: usize, width: NonZeroUsize) -> Result<(), E> {
        self.begin_enumeration(Enumeration::Dense { total, width })
    }
    /// The caller owns specialized enumeration and seals its true end.
    pub(crate) fn begin_open(&self) -> Result<(), E> {
        self.begin_enumeration(Enumeration::Open)
    }
    fn issue(
        self: &Arc<Self>,
        state: &mut State<E>,
        range: Range<usize>,
    ) -> Result<RangeLease<E, H>, E> {
        let ordinal = state.issued;
        let Some(issued) = state.issued.checked_add(1) else {
            return Err(self.overflow(state));
        };
        let Some(inflight) = state.inflight.checked_add(1) else {
            return Err(self.overflow(state));
        };
        state.issued = issued;
        state.inflight = inflight;
        Ok(RangeLease {
            owner: Arc::clone(self),
            generation: state.generation,
            ordinal,
            range,
            settled: false,
        })
    }
    pub(crate) fn claim(self: &Arc<Self>) -> Result<Option<RangeLease<E, H>>, E> {
        let mut state = self.state()?;
        let Enumeration::Dense { total, width } = state.enumeration else {
            return Err(self.invalid(&mut state));
        };
        if state.exhausted {
            return Ok(None);
        }
        let start = state.next;
        let end = start.saturating_add(width.get()).min(total);
        let lease = self.issue(&mut state, start..end)?;
        state.next = end;
        state.exhausted = end == total;
        Ok(Some(lease))
    }
    pub(crate) fn claim_one(self: &Arc<Self>) -> Result<RangeLease<E, H>, E> {
        let mut state = self.state()?;
        if !matches!(state.enumeration, Enumeration::Open) || state.exhausted {
            return Err(self.invalid(&mut state));
        }
        let start = state.issued;
        let Some(end) = start.checked_add(1) else {
            return Err(self.overflow(&mut state));
        };
        self.issue(&mut state, start..end)
    }
    pub(crate) fn seal(&self) -> Result<(), E> {
        let mut state = self.state()?;
        if !matches!(state.enumeration, Enumeration::Open) || state.exhausted {
            return Err(self.invalid(&mut state));
        }
        state.exhausted = true;
        Ok(())
    }
    pub(crate) fn barrier(&self) -> Result<bool, E> {
        Ok(self.state()?.barrier())
    }
    pub(crate) fn inflight(&self) -> Result<usize, E> {
        Ok(self.state()?.inflight)
    }
    fn validate_in(&self, state: &mut State<E>, lease: &RangeLease<E, H>) -> Result<(), E> {
        if !std::ptr::eq(self, Arc::as_ptr(&lease.owner))
            || lease.generation != state.generation
            || lease.ordinal >= state.issued
            || lease.settled
            || state.inflight == 0
            || state.completed >= state.issued
        {
            return Err(self.invalid(state));
        }
        Ok(())
    }
    /// Validate before caller-owned output installation. The caller serializes
    /// that installation with completion; this grants no effect or capacity.
    pub(crate) fn validate(&self, lease: &RangeLease<E, H>) -> Result<(), E> {
        let mut state = self.state()?;
        self.validate_in(&mut state, lease)
    }
    pub(crate) fn complete(&self, lease: &mut RangeLease<E, H>) -> Result<(), E> {
        let mut state = self.state()?;
        self.validate_in(&mut state, lease)?;
        state.inflight -= 1;
        state.completed += 1;
        lease.settled = true;
        Ok(())
    }
}

/// Uniquely owns one claimed range until completion or actual drop.
pub(crate) struct RangeLease<E: Clone, H = ()> {
    owner: Arc<PhasedRangeWork<E, H>>,
    generation: usize,
    ordinal: usize,
    range: Range<usize>,
    settled: bool,
}
impl<E: Clone, H> RangeLease<E, H> {
    pub(crate) fn is_settled(&self) -> bool {
        self.settled
    }
    pub(crate) fn ordinal(&self) -> usize {
        self.ordinal
    }
    pub(crate) fn range(&self) -> Range<usize> {
        self.range.clone()
    }
}
impl<E: Clone, H> Drop for RangeLease<E, H> {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        // Even after a failure or panic the original position is returned only
        // here, when its actual data owner has dropped. It is never reissued.
        let mut state = self.owner.state.lock().unwrap_or_else(|p| p.into_inner());
        if self.generation == state.generation && state.inflight != 0 {
            state.inflight -= 1;
        }
        state.failure.latch(self.owner.abandoned.clone());
        self.settled = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{panic::AssertUnwindSafe, thread};

    fn work() -> Arc<PhasedRangeWork<&'static str>> {
        Arc::new(PhasedRangeWork::new("abandoned"))
    }

    #[test]
    fn concurrent_ranges_are_exact_once_and_barrier_waits_for_every_completion() {
        let work = work();
        work.begin(257, NonZeroUsize::new(7).unwrap()).unwrap();
        thread::scope(|scope| {
            let mut handles = Vec::new();
            for _ in 0..4 {
                let work = Arc::clone(&work);
                handles.push(scope.spawn(move || {
                    let mut ranges = Vec::new();
                    while let Some(mut lease) = work.claim().unwrap() {
                        assert!(!work.barrier().unwrap());
                        ranges.push(lease.range());
                        work.complete(&mut lease).unwrap();
                    }
                    ranges
                }));
            }
            let mut ranges = handles
                .into_iter()
                .flat_map(|h| h.join().unwrap())
                .collect::<Vec<_>>();
            ranges.sort_by_key(|r| r.start);
            let mut next = 0;
            for range in ranges {
                assert_eq!(range.start, next);
                assert!(range.end > range.start);
                next = range.end;
            }
            assert_eq!(next, 257);
        });
        assert!(work.barrier().unwrap());
        assert_eq!(work.inflight().unwrap(), 0);
        work.begin(0, NonZeroUsize::new(1).unwrap()).unwrap();
        assert!(work.barrier().unwrap());
        assert!(work.claim().unwrap().is_none());
    }

    #[test]
    fn open_enumeration_seal_does_not_complete_a_live_lease() {
        let work = work();
        work.begin_open().unwrap();
        let mut first = work.claim_one().unwrap();
        let mut second = work.claim_one().unwrap();
        assert_eq!(first.ordinal(), 0);
        assert_eq!(second.ordinal(), 1);
        work.seal().unwrap();
        work.complete(&mut second).unwrap();
        assert!(!work.barrier().unwrap());
        work.validate(&first).unwrap();
        work.complete(&mut first).unwrap();
        assert!(work.barrier().unwrap());
    }

    #[test]
    fn duplicate_completion_and_foreign_lease_are_failures_not_progress() {
        let work = work();
        work.begin_open().unwrap();
        let mut lease = work.claim_one().unwrap();
        work.complete(&mut lease).unwrap();
        assert_eq!(work.complete(&mut lease), Err("abandoned"));
        assert_eq!(work.barrier(), Err("abandoned"));

        let receiver = self::work();
        let origin = self::work();
        receiver.begin_open().unwrap();
        origin.begin_open().unwrap();
        let lease = origin.claim_one().unwrap();
        assert_eq!(receiver.validate(&lease), Err("abandoned"));
        assert_eq!(origin.inflight().unwrap(), 1);
        drop(lease);
        assert_eq!(origin.check(), Err("abandoned"));
        assert_eq!(origin.state.lock().unwrap().inflight, 0);
    }

    #[test]
    fn original_failure_precedes_drop_and_prevents_a_new_phase() {
        let work = work();
        work.begin_open().unwrap();
        let lease = work.claim_one().unwrap();
        assert_eq!(
            work.latch("original-resource-refusal"),
            "original-resource-refusal"
        );
        drop(lease);
        assert_eq!(work.check(), Err("original-resource-refusal"));
        assert_eq!(work.barrier(), Err("original-resource-refusal"));
        assert_eq!(work.begin_open(), Err("original-resource-refusal"));
        assert_eq!(work.state.lock().unwrap().inflight, 0);
    }

    #[test]
    fn panic_drop_returns_lease_without_reissuing_range_or_crossing_barrier() {
        let work = work();
        work.begin(10, NonZeroUsize::new(3).unwrap()).unwrap();
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let _lease = work.claim().unwrap().unwrap();
            panic!("actual cursor panic");
        }));
        assert!(result.is_err());
        assert_eq!(work.state.lock().unwrap().inflight, 0);
        assert_eq!(work.check(), Err("abandoned"));
        assert!(work.claim().is_err());
        assert_eq!(work.begin_open(), Err("abandoned"));
    }

    #[test]
    fn premature_phase_change_preserves_live_lease_until_actual_drop() {
        let work = work();
        work.begin_open().unwrap();
        let lease = work.claim_one().unwrap();
        assert_eq!(
            work.begin(1, NonZeroUsize::new(1).unwrap()),
            Err("abandoned")
        );
        assert_eq!(work.state.lock().unwrap().inflight, 1);
        drop(lease);
        assert_eq!(work.state.lock().unwrap().inflight, 0);
    }

    #[test]
    fn escaped_lease_keeps_original_creator_and_failure_until_actual_drop() {
        use std::sync::atomic::{AtomicBool, Ordering};
        struct Retention(Arc<AtomicBool>);
        impl Drop for Retention {
            fn drop(&mut self) {
                assert!(!self.0.swap(true, Ordering::SeqCst));
            }
        }
        let released = Arc::new(AtomicBool::new(false));
        let work = Arc::new(PhasedRangeWork::with_retention(
            "abandoned",
            "arithmetic-resource",
            Retention(Arc::clone(&released)),
        ));
        work.begin_open().unwrap();
        let lease = work.claim_one().unwrap();
        assert!(!lease.is_settled());
        work.latch("original-resource-refusal");
        drop(work);
        assert!(!released.load(Ordering::SeqCst));
        assert_eq!(lease.owner.check(), Err("original-resource-refusal"));
        drop(lease);
        assert!(released.load(Ordering::SeqCst));
    }

    #[test]
    fn actual_counter_overflow_preserves_the_callers_distinct_resource_cause() {
        let work = Arc::new(PhasedRangeWork::with_retention(
            "abandoned",
            "arithmetic-resource",
            (),
        ));
        work.state.lock().unwrap().generation = usize::MAX;
        assert_eq!(work.begin_open(), Err("arithmetic-resource"));
        assert_eq!(work.check(), Err("arithmetic-resource"));
    }
}
