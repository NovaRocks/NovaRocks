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

//! One physical-build bitmap, partitioned into disjoint worker-owned segments.
//! Each segment is safely initialized under its private Arc and installed once.
//! Readiness is the original phase barrier, never an Observable notification.

use super::{
    build_state::build_contract,
    join_completion::{arithmetic, funded_shared, funded_vec},
};
use crate::{
    exec::{
        phased_work::{PhasedRangeWork, RangeObligation},
        pipeline::operator::InputConstructionOwner,
    },
    runtime::{
        observable::Observable,
        task::{ExecutionFailure, ExecutionResult},
    },
};
use novarocks_functions::KernelEvaluationControl;
use novarocks_type_contract::ValueTypeStorageLifetime;
use std::{
    num::NonZeroUsize,
    ops::Range,
    sync::{
        Arc, Mutex, MutexGuard, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};

fn words_for_rows(rows: usize) -> ExecutionResult<usize> {
    (rows / u64::BITS as usize)
        .checked_add(usize::from(!rows.is_multiple_of(u64::BITS as usize)))
        .ok_or_else(arithmetic)
}
// The same balanced q/r geometry used by the original Direct shard directory,
// without depending on that KeyIndex-specific storage or its lifecycle.
fn segment_words(words: usize, segments: usize, segment: usize) -> ExecutionResult<Range<usize>> {
    if segments == 0 || segments > words || segment >= segments {
        return Err(build_contract(
            "join bitmap segment is outside its original directory",
        ));
    }
    let q = words / segments;
    let r = words % segments;
    let start = segment
        .checked_mul(q)
        .and_then(|v| v.checked_add(segment.min(r)))
        .ok_or_else(arithmetic)?;
    let count = q
        .checked_add(usize::from(segment < r))
        .ok_or_else(arithmetic)?;
    Ok(start..start.checked_add(count).ok_or_else(arithmetic)?)
}
fn segment_for_word(words: usize, segments: usize, word: usize) -> ExecutionResult<usize> {
    if segments == 0 || segments > words || word >= words {
        return Err(build_contract(
            "join bitmap word is outside its original directory",
        ));
    }
    let q = words / segments;
    let r = words % segments;
    let large = q.checked_add(1).ok_or_else(arithmetic)?;
    let large_end = large.checked_mul(r).ok_or_else(arithmetic)?;
    if word < large_end {
        Ok(word / large)
    } else {
        r.checked_add((word - large_end) / q).ok_or_else(arithmetic)
    }
}
struct Segment {
    words: Vec<AtomicU64>,
    range: Range<usize>,
    // This is the actual claim worker's finite creator, not initializer F0.
    _storage: ValueTypeStorageLifetime,
}
/// One logical bitmap: segment word counts sum to ceil(B/64), not F or Q times
/// that count. Ready retains only real directory/data storage and creators.
pub(crate) struct BuildMatchBitmap {
    segments: Vec<OnceLock<Arc<Segment>>>,
    rows: usize,
    _storage: ValueTypeStorageLifetime,
}
impl BuildMatchBitmap {
    #[cfg(test)]
    pub(crate) fn rows(&self) -> usize {
        self.rows
    }
    pub(crate) fn word_count(&self) -> usize {
        self.rows / u64::BITS as usize + usize::from(!self.rows.is_multiple_of(u64::BITS as usize))
    }
    fn word_and_mask(&self, row: usize) -> ExecutionResult<(&AtomicU64, u64)> {
        if row >= self.rows {
            return Err(build_contract(
                "join bitmap row is outside the physical build domain",
            ));
        }
        let word = row / u64::BITS as usize;
        let id = segment_for_word(self.word_count(), self.segments.len(), word)?;
        let segment = self
            .segments
            .get(id)
            .and_then(OnceLock::get)
            .ok_or_else(|| build_contract("join bitmap segment was not installed"))?;
        let local = word
            .checked_sub(segment.range.start)
            .ok_or_else(arithmetic)?;
        let value = segment.words.get(local).ok_or_else(|| {
            build_contract("join bitmap segment differs from its physical word domain")
        })?;
        Ok((value, 1u64 << (row % u64::BITS as usize)))
    }
    pub(crate) fn mark(&self, row: usize) -> ExecutionResult<()> {
        let (word, mask) = self.word_and_mask(row)?;
        word.fetch_or(mask, Ordering::Relaxed);
        Ok(())
    }
    pub(crate) fn is_marked(&self, row: usize) -> ExecutionResult<bool> {
        let (word, mask) = self.word_and_mask(row)?;
        Ok(word.load(Ordering::Relaxed) & mask != 0)
    }
    // OnceLock publishes initialization. Q -> T visibility of later Relaxed
    // marks still requires JoinCompletion's same-Mutex normal-probe/tail gate.
}
#[derive(Clone, Copy, Eq, PartialEq)]
enum Phase {
    Initializing,
    Retired,
    Frozen,
}
struct State {
    phase: Phase,
    // Fixed O(F) directory. Only a successful original barrier may move it.
    segments: Option<Vec<OnceLock<Arc<Segment>>>>,
}
pub(crate) struct BuildMatchBitmapInit {
    state: Mutex<State>,
    ranges: Arc<PhasedRangeWork<ExecutionFailure, ValueTypeStorageLifetime>>,
    changed: Arc<Observable>,
    rows: usize,
    _storage: ValueTypeStorageLifetime,
}
pub(crate) enum BitmapInitClaim {
    Pending,
    Finished,
    Retired,
    Range(InitObligation),
}
pub(crate) enum BitmapFreeze {
    Pending,
    Retired,
    Frozen,
    Ready(BuildMatchBitmap),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InitProgress {
    Initialized,
    Progressed,
    Yielded,
    Retired,
}
/// Only the original phase can issue this opaque lease. Its pending Arc never
/// escapes before installation; Arc::get_mut is the sole mutable storage port.
pub(crate) struct InitObligation {
    pending: Option<Arc<Segment>>,
    owner: Arc<BuildMatchBitmapInit>,
    obligation: Option<RangeObligation<ExecutionFailure, ValueTypeStorageLifetime>>,
    words: Range<usize>,
}
impl BuildMatchBitmapInit {
    pub(crate) fn new(
        rows: usize,
        workers: NonZeroUsize,
        changed: Arc<Observable>,
        owner: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<Arc<Self>> {
        control.checkpoint(0)?;
        let count = words_for_rows(rows)?.min(workers.get());
        let mut segments = funded_vec(owner, count)?;
        for _ in 0..count {
            control.checkpoint(1)?;
            segments.push(OnceLock::new());
        }
        let ranges = funded_shared(
            owner,
            PhasedRangeWork::with_retention(
                build_contract("join bitmap initialization lease was abandoned"),
                arithmetic(),
                owner.storage_lifetime(),
            ),
        )?;
        // One original segment id per lease; this width grants no CPU budget.
        ranges.begin(count, NonZeroUsize::MIN)?;
        funded_shared(
            owner,
            Self {
                state: Mutex::new(State {
                    phase: Phase::Initializing,
                    segments: Some(segments),
                }),
                ranges,
                changed,
                rows,
                _storage: owner.storage_lifetime(),
            },
        )
    }
    fn state(&self) -> ExecutionResult<MutexGuard<'_, State>> {
        self.state.lock().map_err(|poisoned| {
            drop(poisoned.into_inner());
            self.ranges.latch(build_contract(
                "join bitmap initialization state was poisoned",
            ))
        })
    }
    pub(crate) fn fail(&self, cause: ExecutionFailure) -> ExecutionFailure {
        let notify = self.changed.defer_notify();
        let cause = self.ranges.latch(cause);
        notify.arm();
        cause
    }
    #[cfg(test)]
    pub(crate) fn check(&self) -> ExecutionResult<()> {
        self.ranges.check()
    }
    /// Allocation is charged to this actual claim worker, not initializer F0.
    /// It reserves the segment's backing without an O(words) initialization.
    pub(crate) fn claim(
        self: &Arc<Self>,
        creator: &dyn InputConstructionOwner,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<BitmapInitClaim> {
        let notify = self.changed.defer_notify();
        let result = (|| {
            control
                .checkpoint(0)
                .map_err(|cause| self.ranges.latch(cause.into()))?;
            let mut owned = {
                let state = self.state()?;
                self.ranges.check()?;
                match state.phase {
                    Phase::Retired => return Ok(BitmapInitClaim::Retired),
                    Phase::Frozen => return Ok(BitmapInitClaim::Finished),
                    Phase::Initializing => {}
                }
                let Some(lease) = self.ranges.claim()? else {
                    return Ok(if self.ranges.barrier()? {
                        BitmapInitClaim::Finished
                    } else {
                        BitmapInitClaim::Pending
                    });
                };
                // Keep the raw original lease local until pure geometry is
                // checked. On failure it drops before state is unlocked;
                // the outer deferred notification then runs after both exit.
                let directory = state.segments.as_ref().ok_or_else(|| {
                    self.ranges
                        .latch(build_contract("join bitmap directory is absent"))
                })?;
                let id = lease.range().start;
                let count = words_for_rows(self.rows).map_err(|cause| self.ranges.latch(cause))?;
                let words = segment_words(count, directory.len(), id)
                    .map_err(|cause| self.ranges.latch(cause))?;
                InitObligation {
                    pending: None,
                    owner: Arc::clone(self),
                    obligation: Some(lease),
                    words,
                }
            };
            // Neither allocator callbacks nor word initialization hold state.
            let words =
                funded_vec(creator, owned.words.len()).map_err(|cause| self.ranges.latch(cause))?;
            owned.pending = Some(
                funded_shared(
                    creator,
                    Segment {
                        words,
                        range: owned.words.clone(),
                        _storage: creator.storage_lifetime(),
                    },
                )
                .map_err(|cause| self.ranges.latch(cause))?,
            );
            Ok(BitmapInitClaim::Range(owned))
        })();
        if result.is_err() {
            notify.arm();
        }
        result
    }
    /// Caller must be the original global normal-demand withdrawal author.
    pub(crate) fn retire(&self) -> ExecutionResult<()> {
        let notify = self.changed.defer_notify();
        let result = (|| {
            let mut state = self.state()?;
            self.ranges.check()?;
            state.phase = Phase::Retired;
            Ok(())
        })();
        notify.arm();
        result
    }
    pub(crate) fn try_freeze(&self) -> ExecutionResult<BitmapFreeze> {
        let notify = self.changed.defer_notify();
        let result = (|| {
            let mut state = self.state()?;
            self.ranges.check()?;
            match state.phase {
                Phase::Retired => return Ok(BitmapFreeze::Retired),
                Phase::Frozen => return Ok(BitmapFreeze::Frozen),
                Phase::Initializing => {}
            }
            if !self.ranges.barrier()? {
                return Ok(BitmapFreeze::Pending);
            }
            // complete installs its exact, fully initialized segment before
            // settling its original range. Barrier proves all directory slots
            // are installed; no O(F) re-scan or second directory is required.
            let segments = state.segments.take().ok_or_else(|| {
                self.ranges
                    .latch(build_contract("join bitmap directory was already moved"))
            })?;
            state.phase = Phase::Frozen;
            Ok(BitmapFreeze::Ready(BuildMatchBitmap {
                segments,
                rows: self.rows,
                _storage: self._storage.clone(),
            }))
        })();
        if matches!(&result, Ok(BitmapFreeze::Ready(_)) | Err(_)) {
            notify.arm();
        }
        result
    }
}
impl InitObligation {
    #[cfg(test)]
    pub(crate) fn words(&self) -> Range<usize> {
        self.words.clone()
    }
    /// Actual Vec length is the only cursor. A retired/settled lease has no
    /// pending initialization work; this does not certify that it initialized.
    #[cfg(test)]
    pub(crate) fn remaining_words(&self) -> usize {
        self.pending
            .as_ref()
            .map_or(0, |segment| self.words.len() - segment.words.len())
    }
    pub(crate) fn initialize(
        &mut self,
        quota: usize,
        control: &dyn KernelEvaluationControl,
    ) -> ExecutionResult<InitProgress> {
        let notify = self.owner.changed.defer_notify();
        let result = (|| {
            {
                let state = self.owner.state()?;
                let lease = self.obligation.as_ref().ok_or_else(|| {
                    self.owner
                        .ranges
                        .latch(build_contract("join bitmap lease was already dropped"))
                })?;
                self.owner.ranges.validate(lease)?;
                if state.phase == Phase::Retired {
                    return Ok(InitProgress::Retired);
                }
                if state.phase != Phase::Initializing {
                    return Err(self
                        .owner
                        .ranges
                        .latch(build_contract("join bitmap lease used after freeze")));
                }
            }
            let pending = self.pending.as_mut().ok_or_else(|| {
                self.owner
                    .ranges
                    .latch(build_contract("join bitmap segment is absent"))
            })?;
            let segment = Arc::get_mut(pending).ok_or_else(|| {
                self.owner.ranges.latch(build_contract(
                    "join bitmap pending segment is not uniquely owned",
                ))
            })?;
            let target = self.words.len();
            if segment.range != self.words
                || segment.words.len() > target
                || segment.words.capacity() < target
            {
                return Err(self.owner.ranges.latch(build_contract(
                    "join bitmap segment differs from its original word reservation",
                )));
            }
            if segment.words.len() == target {
                return Ok(InitProgress::Initialized);
            }
            if quota == 0 {
                return Ok(InitProgress::Yielded);
            }
            let count = quota.min(target - segment.words.len());
            let end = segment
                .words
                .len()
                .checked_add(count)
                .ok_or_else(|| self.owner.ranges.latch(arithmetic()))?;
            // This uniquely owned Vec is already reserved for its entire
            // physical segment. Push cannot grow allocation; each turn adds
            // only its original quota. No global write lock or raw address.
            while segment.words.len() < end {
                control
                    .checkpoint(1)
                    .map_err(|cause| self.owner.ranges.latch(cause.into()))?;
                segment.words.push(AtomicU64::new(0));
            }
            Ok(if segment.words.len() == target {
                InitProgress::Initialized
            } else {
                InitProgress::Progressed
            })
        })();
        if result.is_err() {
            notify.arm();
        }
        result
    }
    #[cfg(test)]
    pub(crate) fn complete(&mut self) -> ExecutionResult<()> {
        self.settle_inner(false).map(|_| ())
    }
    pub(crate) fn settle(&mut self) -> ExecutionResult<InitProgress> {
        self.settle_inner(true)
    }
    fn settle_inner(&mut self, allow_retired: bool) -> ExecutionResult<InitProgress> {
        let notify = self.owner.changed.defer_notify();
        let result = (|| {
            let state = self.owner.state()?;
            let lease = self.obligation.as_mut().ok_or_else(|| {
                self.owner
                    .ranges
                    .latch(build_contract("join bitmap lease was already dropped"))
            })?;
            self.owner.ranges.validate(lease)?;
            if state.phase == Phase::Retired && allow_retired {
                drop(state);
                drop(self.pending.take());
                self.owner.ranges.complete(lease)?;
                return Ok(InitProgress::Retired);
            }
            if state.phase != Phase::Initializing {
                return Err(self.owner.ranges.latch(build_contract(
                    "join bitmap completed after normal retirement",
                )));
            }
            let directory = state.segments.as_ref().ok_or_else(|| {
                self.owner
                    .ranges
                    .latch(build_contract("join bitmap directory is absent"))
            })?;
            let id = lease.range().start;
            let count =
                words_for_rows(self.owner.rows).map_err(|cause| self.owner.ranges.latch(cause))?;
            let expected = segment_words(count, directory.len(), id)
                .map_err(|cause| self.owner.ranges.latch(cause))?;
            let segment = self.pending.as_ref().ok_or_else(|| {
                self.owner
                    .ranges
                    .latch(build_contract("join bitmap segment was already installed"))
            })?;
            if segment.range != expected
                || self.words != expected
                || segment.words.len() != expected.len()
            {
                return Err(self.owner.ranges.latch(build_contract(
                    "join bitmap range completed before exact initialization",
                )));
            }
            let slot = directory.get(id).ok_or_else(|| {
                self.owner.ranges.latch(build_contract(
                    "join bitmap segment id is outside the directory",
                ))
            })?;
            let segment = self
                .pending
                .take()
                .expect("private checked pending segment");
            if let Err(segment) = slot.set(segment) {
                self.pending = Some(segment);
                return Err(self.owner.ranges.latch(build_contract(
                    "join bitmap segment was installed more than once",
                )));
            }
            self.owner.ranges.complete(lease)?;
            Ok(InitProgress::Initialized)
        })();
        notify.arm();
        result
    }
    pub(crate) fn retire(&mut self) -> ExecutionResult<()> {
        let notify = self.owner.changed.defer_notify();
        let result = (|| {
            {
                let state = self.owner.state()?;
                if state.phase != Phase::Retired {
                    return Err(self.owner.ranges.latch(build_contract(
                        "join bitmap lease retired while demand remains",
                    )));
                }
                let lease = self.obligation.as_ref().ok_or_else(|| {
                    self.owner
                        .ranges
                        .latch(build_contract("join bitmap lease was already dropped"))
                })?;
                self.owner.ranges.validate(lease)?;
            }
            // Retired is monotonic. Drop partial actual worker backing outside
            // state, before returning its true phase position. It is not Ready.
            drop(self.pending.take());
            self.owner
                .ranges
                .complete(self.obligation.as_mut().expect("private validated lease"))
        })();
        notify.arm();
        result
    }
}
impl Drop for InitObligation {
    fn drop(&mut self) {
        // All safe Vec elements and their actual segment creator exit before
        // the original lease returns its inflight position and notifies.
        drop(self.pending.take());
        if let Some(lease) = self.obligation.take() {
            let unfinished = !lease.is_settled();
            drop(lease);
            if unfinished {
                self.owner.changed.notify_observers();
            }
        }
    }
}
#[cfg(test)]
#[path = "build_match_bitmap_tests.rs"]
mod tests;
