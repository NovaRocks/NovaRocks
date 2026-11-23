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

use super::MAX_RETAINED_PATTERNS;
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::OpaqueAllocationHost;
use crate::pattern_memo::{ControlledPatternMemo, DeterministicPatternFailure as Failure};
use crate::source_custody_direct_tests::Host;
use crate::{
    AggregateStateAllocator, KernelEvaluationControl, KernelFailure, MAX_UNOBSERVED_KERNEL_WORK,
};
use allocator_api2::vec::Vec as HostVec;
use std::{
    alloc::Layout,
    ptr::NonNull,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

type Program = HostVec<u8, HostAggregateAllocator>;
type Memo<'p> = ControlledPatternMemo<'p, Program, &'static str, &'static str>;
const RESIDENT_BYTES: usize = 137;
const OLD_BYTES: usize = 201;
const NEW_BYTES: usize = 203;

// Track actual allocator blocks and reservations with the existing host
// ledger. The adjustable limit lets the same evaluation retry a refusal.
struct Budget {
    host: Host,
    limit: AtomicUsize,
}

impl Budget {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            host: Host::default(),
            limit: AtomicUsize::new(usize::MAX),
        })
    }

    fn blocks_with_size(&self, size: usize) -> usize {
        self.host
            .ledger
            .lock()
            .unwrap()
            .live
            .iter()
            .filter(|(_, layout)| layout.size() == size)
            .count()
    }

    fn live_bytes(&self) -> usize {
        let ledger = self.host.ledger.lock().unwrap();
        ledger.opaque
            + ledger
                .live
                .iter()
                .map(|(_, layout)| layout.size())
                .sum::<usize>()
    }

    fn check(&self, request: usize) -> Result<(), KernelFailure> {
        if request
            <= self
                .limit
                .load(Ordering::Relaxed)
                .saturating_sub(self.live_bytes())
        {
            Ok(())
        } else {
            Err(KernelFailure::ResourceExhausted)
        }
    }
}

impl AggregateStateAllocator for Budget {
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        self.check(layout.size())?;
        self.host.allocate(layout)
    }

    unsafe fn release(&self, ptr: NonNull<u8>, layout: Layout) {
        // SAFETY: This budget only returns blocks from the underlying host.
        unsafe { self.host.release(ptr, layout) };
    }

    fn opaque_allocation_host(&self) -> Option<&dyn OpaqueAllocationHost> {
        Some(self)
    }
}

impl OpaqueAllocationHost for Budget {
    fn reserve_opaque(&self, bytes: usize) -> Result<(), KernelFailure> {
        self.check(bytes)?;
        self.host.reserve_opaque(bytes)
    }

    fn release_opaque(&self, bytes: usize) {
        self.host.release_opaque(bytes);
    }
}

#[derive(Default)]
struct Control {
    stop_at: Option<usize>,
    calls: AtomicUsize,
    cancel: AtomicBool,
}

impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= MAX_UNOBSERVED_KERNEL_WORK);
        let call = self.calls.fetch_add(1, Ordering::Relaxed) + 1;
        if self.stop_at == Some(call) || self.cancel.load(Ordering::Relaxed) {
            Err(KernelFailure::Cancelled)
        } else {
            Ok(())
        }
    }

    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("pattern memo must not wait")
    }
}

fn program(
    host: Arc<Budget>,
    bytes: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Program, KernelFailure> {
    let allocator = HostAggregateAllocator::try_new_with_journal(host)?;
    let mut program = HostVec::new_in(allocator);
    program
        .try_reserve_exact(bytes)
        .map_err(|_| program.allocator().take_failure())?;
    for _ in 0..bytes {
        work.step()?;
        program.push(0x52);
    }
    Ok(program)
}

fn fill<'p>(
    memo: &mut Memo<'p>,
    host: &Arc<Budget>,
    patterns: &'p [String],
    work: &mut EvaluationCheckpoints<'_>,
) {
    for pattern in patterns {
        let pattern = pattern.as_str();
        let compiled = memo
            .get_or_compile(pattern, work, |source, work| {
                assert!(std::ptr::eq(source, pattern));
                Ok(Ok(program(host.clone(), RESIDENT_BYTES, work)?))
            })
            .unwrap()
            .unwrap();
        assert_eq!(compiled.as_slice(), &[0x52; RESIDENT_BYTES]);
    }
}

fn patterns() -> Vec<String> {
    (0..MAX_RETAINED_PATTERNS)
        .map(|i| format!("p{i}"))
        .collect()
}

#[test]
fn regexp_pattern_memo_retains_residents_and_admits_old_and_new_transient_bytes_together() {
    let host = Budget::new();
    let control = Control::default();
    let mut work = EvaluationCheckpoints::new(&control);
    let keys = patterns();
    let mut memo = Memo::try_new(host.clone()).unwrap();
    fill(&mut memo, &host, &keys, &mut work);

    memo.get_or_compile("old", &mut work, |_, work| {
        Ok(Ok(program(host.clone(), OLD_BYTES, work)?))
    })
    .unwrap()
    .unwrap();
    assert_eq!(host.blocks_with_size(RESIDENT_BYTES), MAX_RETAINED_PATTERNS);
    assert_eq!(host.blocks_with_size(OLD_BYTES), 1);
    let previous_live = host.live_bytes();
    let program_metadata = HostAggregateAllocator::metadata_allocation_bytes()
        + crate::aggregate_helper_memory::private_mutex_backing_bytes().unwrap();
    memo.get_or_compile("new", &mut work, |_, work| {
        assert_eq!(host.blocks_with_size(OLD_BYTES), 1);
        assert_eq!(host.blocks_with_size(NEW_BYTES), 0);
        let replacement = program(host.clone(), NEW_BYTES, work)?;
        let residents = host.blocks_with_size(RESIDENT_BYTES);
        let old = host.blocks_with_size(OLD_BYTES);
        let new = host.blocks_with_size(NEW_BYTES);
        assert_eq!((residents, old, new), (MAX_RETAINED_PATTERNS, 1, 1));
        assert_eq!(
            host.live_bytes(),
            previous_live + NEW_BYTES + program_metadata
        );
        Ok(Ok(replacement))
    })
    .unwrap()
    .unwrap();
    assert_eq!(host.blocks_with_size(OLD_BYTES), 0);
    assert_eq!(host.blocks_with_size(NEW_BYTES), 1);
    assert_eq!(host.live_bytes(), previous_live - OLD_BYTES + NEW_BYTES);
    for pattern in &keys {
        memo.get_or_compile(pattern, &mut work, |_, _| {
            panic!("resident pattern was evicted")
        })
        .unwrap()
        .unwrap();
    }
    assert_eq!(host.blocks_with_size(NEW_BYTES), 1);
    drop(memo);
    host.host.zero();
    work.finish().unwrap();
}

#[test]
fn regexp_pattern_memo_outer_refusals_keep_the_published_transient() {
    let host = Budget::new();
    let control = Control::default();
    let mut work = EvaluationCheckpoints::new(&control);
    let keys = patterns();
    let mut memo = Memo::try_new(host.clone()).unwrap();
    fill(&mut memo, &host, &keys, &mut work);
    memo.get_or_compile("old", &mut work, |_, work| {
        Ok(Ok(program(host.clone(), OLD_BYTES, work)?))
    })
    .unwrap()
    .unwrap();

    let previous_live = host.live_bytes();
    let program_metadata = HostAggregateAllocator::metadata_allocation_bytes()
        + crate::aggregate_helper_memory::private_mutex_backing_bytes().unwrap();
    host.limit.store(
        previous_live + program_metadata + NEW_BYTES - 1,
        Ordering::Relaxed,
    );
    assert!(matches!(
        memo.get_or_compile("new", &mut work, |_, work| Ok(Ok(program(
            host.clone(),
            NEW_BYTES,
            work
        )?))),
        Err(KernelFailure::ResourceExhausted)
    ));
    assert_eq!(host.blocks_with_size(OLD_BYTES), 1);
    assert_eq!(host.blocks_with_size(NEW_BYTES), 0);
    assert_eq!(host.live_bytes(), previous_live);
    host.limit.store(usize::MAX, Ordering::Relaxed);
    let cancelled = Control::default();
    let mut cancelled_work = EvaluationCheckpoints::new(&cancelled);
    assert!(matches!(
        memo.get_or_compile("new", &mut cancelled_work, |_, work| {
            let replacement = program(host.clone(), NEW_BYTES, work)?;
            assert_eq!(host.blocks_with_size(OLD_BYTES), 1);
            assert_eq!(host.blocks_with_size(NEW_BYTES), 1);
            cancelled.cancel.store(true, Ordering::Relaxed);
            Ok(Ok(replacement))
        }),
        Err(KernelFailure::Cancelled)
    ));
    assert_eq!(host.blocks_with_size(OLD_BYTES), 1);
    assert_eq!(host.blocks_with_size(NEW_BYTES), 0);

    assert_eq!(
        memo.get_or_compile("old", &mut work, |_, _| panic!(
            "original transient should still be present"
        ))
        .unwrap()
        .unwrap()
        .len(),
        OLD_BYTES
    );
    memo.get_or_compile("new", &mut work, |_, work| {
        Ok(Ok(program(host.clone(), NEW_BYTES, work)?))
    })
    .unwrap()
    .unwrap();
    assert_eq!(host.blocks_with_size(OLD_BYTES), 0);
    assert_eq!(host.blocks_with_size(NEW_BYTES), 1);
    drop(memo);
    host.host.zero();
    work.finish().unwrap();
}

#[test]
fn regexp_pattern_memo_first_table_admission_observes_stop_before_publication() {
    let host = Budget::new();
    let mut memo = Memo::try_new(host.clone()).unwrap();
    let control = Control {
        stop_at: Some(3),
        ..Control::default()
    };
    let mut work = EvaluationCheckpoints::new(&control);
    assert!(matches!(
        memo.get_or_compile("first", &mut work, |_, work| Ok(Ok(program(
            host.clone(),
            OLD_BYTES,
            work
        )?))),
        Err(KernelFailure::Cancelled)
    ));
    assert_eq!(host.blocks_with_size(OLD_BYTES), 0);

    let control = Control::default();
    let mut work = EvaluationCheckpoints::new(&control);
    let compiled = memo
        .get_or_compile("first", &mut work, |_, work| {
            Ok(Ok(program(host.clone(), NEW_BYTES, work)?))
        })
        .unwrap()
        .unwrap();
    assert_eq!(compiled.len(), NEW_BYTES);
    drop(memo);
    host.host.zero();
    work.finish().unwrap();
}

#[test]
fn regexp_pattern_memo_reuses_repeatable_data_and_capability_facts() {
    let host = Budget::new();
    let control = Control::default();
    let mut work = EvaluationCheckpoints::new(&control);
    let data = "[".repeat(5_000);
    let data_copy = data.clone();
    let mut memo = Memo::try_new(host.clone()).unwrap();
    let capability = "a(?=b)";
    assert!(matches!(
        memo.get_or_compile(&data, &mut work, |_, _| Ok(Err(Failure::Data(
            "unclosed class"
        ))))
        .unwrap(),
        Err(Failure::Data("unclosed class"))
    ));
    assert!(matches!(
        memo.get_or_compile(capability, &mut work, |_, _| Ok(Err(Failure::Capability(
            "lookaround"
        ))))
        .unwrap(),
        Err(Failure::Capability("lookaround"))
    ));
    assert!(matches!(
        memo.get_or_compile(&data_copy, &mut work, |_, _| panic!(
            "repeatable data error must not recompile"
        ))
        .unwrap(),
        Err(Failure::Data("unclosed class"))
    ));
    assert!(matches!(
        memo.get_or_compile(capability, &mut work, |_, _| panic!(
            "repeatable capability fact must not recompile"
        ))
        .unwrap(),
        Err(Failure::Capability("lookaround"))
    ));
    drop(memo);
    host.host.zero();
    work.finish().unwrap();
}

#[test]
fn regexp_pattern_memo_checks_for_stop_during_long_hash_and_equal_key_comparison() {
    let host = Budget::new();
    let data = "中".repeat(10_000);
    let same_text_elsewhere = data.clone();
    let mut memo = Memo::try_new(host.clone()).unwrap();

    let control = Control {
        stop_at: Some(2),
        ..Control::default()
    };
    let mut work = EvaluationCheckpoints::new(&control);
    assert!(matches!(
        memo.get_or_compile(&data, &mut work, |_, _| panic!(
            "compilation reached before the hash completed"
        )),
        Err(KernelFailure::Cancelled)
    ));
    assert_eq!(control.calls.load(Ordering::Relaxed), 2);

    let control = Control::default();
    let mut work = EvaluationCheckpoints::new(&control);
    memo.get_or_compile(&data, &mut work, |_, work| {
        Ok(Ok(program(host.clone(), OLD_BYTES, work)?))
    })
    .unwrap()
    .unwrap();
    work.finish().unwrap();

    let control = Control {
        stop_at: Some(2),
        ..Control::default()
    };
    let mut work = EvaluationCheckpoints::new(&control);
    assert!(matches!(
        memo.get_or_compile(&same_text_elsewhere, &mut work, |_, _| panic!(
            "source comparison must complete first"
        )),
        Err(KernelFailure::Cancelled)
    ));
    assert_eq!(control.calls.load(Ordering::Relaxed), 2);
    drop(memo);
    host.host.zero();
}
