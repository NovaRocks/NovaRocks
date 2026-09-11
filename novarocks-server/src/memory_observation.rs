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

//! The shipping process's allocation observer (MEM-1 wave-1 T07).
//!
//! Installs [`CountingAllocator`] as this process's `#[global_allocator]` and
//! states, once at startup, what that observation covers and what it
//! structurally cannot see.
//!
//! # Why the shipping binary installs the wrapper
//!
//! The observation tier is worth nothing in a test that opts into it. Hard
//! governance knows only what an owner declared, so a real deployment can be
//! honest about every account it keeps and still be over its bound; the only
//! number that closes that gap is the bytes the process actually asked the
//! allocator for, and that number exists only if the wrapper is the process
//! allocator of the binary operators run. Declaring it here — in the server
//! *library*, not in `main` — is also what lets it observe allocations made
//! before `main` is entered, because a `#[global_allocator]` static is
//! resolved at link time rather than installed by code.
//!
//! # What a reading here is, and is not
//!
//! It measures requested bytes routed through the Rust global allocator and
//! attributes nothing to any query, task, operator or session: this module
//! keeps no per-work state and asks no owner who a byte belongs to. Only the
//! accounts in the governance tier own attribution.
//!
//! The difference between this reading and the governance tier's known `L` is
//! therefore a diagnostic — "this much of the process is unaccounted" — never
//! an attribution and never an accusation against a particular account. It is
//! also a lower bound: the counters are request sizes rather than physical
//! memory, and the sources they cannot reach at all are enumerated as typed
//! data by [`CoverageDescriptor::blind_spots`] and logged beside the bytes
//! rather than folded into them.
//!
//! # Startup line, and wave 2
//!
//! [`log_installed`] emits exactly one line, whose field names are frozen:
//! `live_bytes`, `allocations`, `covers`, `blind_spots`. It is a statement of
//! coverage at a moment when nothing interesting has been allocated yet, not a
//! measurement — its value is that an operator reading the log knows which
//! sources the process can account for before any query runs.
//!
//! Wave-2 pressure sampling reuses [`snapshot`]: it is allocation-free and
//! lock-free, so a sampler owns its own cadence and its own observed maximum.
//! No peak is kept here, because a peak would need one contended cache line
//! for every allocation in the process.

use std::alloc::System;

use novarocks_memory::observe::{AllocatorSnapshot, CountingAllocator, CoverageDescriptor};

/// This process's allocator: the system allocator, counted.
///
/// `System` stays the allocator that actually serves the memory; the wrapper
/// only adds relaxed atomic arithmetic on a sharded counter array around each
/// call, so installing it changes what the process can report about itself and
/// not how it obtains memory.
// Design: ADR-0148 (docs/adr/ADR-0148-process-memory-capacity-authority.md)
#[global_allocator]
static GLOBAL: CountingAllocator<System> = CountingAllocator::new(System);

/// Reads this process's current allocation counters.
///
/// Exposed as a function rather than by exposing `GLOBAL` so a caller cannot
/// reach the allocator itself and cannot mistake a shard for a thread. The
/// read is allocation-free and lock-free and never panics, so any diagnostic
/// or sampling path may call it at any time; the returned reading is
/// eventually consistent and is a lower bound under concurrency.
pub fn snapshot() -> AllocatorSnapshot {
    GLOBAL.snapshot()
}

/// Returns what a [`snapshot`] of this process covers, and what it cannot see.
///
/// Constant data, not a measurement: the blind spots are a property of
/// wrapping the Rust global allocator, so every consumer states the same
/// coverage instead of paraphrasing it at each call site.
pub fn coverage() -> CoverageDescriptor {
    CoverageDescriptor::RUST_GLOBAL_ALLOCATOR
}

/// Emits the one startup line that records the observer is installed.
///
/// Called once from process initialisation, after logging is ready and before
/// any runtime is built, so the line is the earliest evidence in the log that
/// this process can account for its own allocations at all. The field names
/// are a contract with wave-2 consumers and must not be renamed.
pub fn log_installed() {
    let snapshot = snapshot();
    let coverage = coverage();
    let blind_spots = blind_spot_labels(coverage);
    tracing::info!(
        target: "novarocks::memory_observation",
        live_bytes = snapshot.live_bytes,
        allocations = snapshot.allocations,
        covers = coverage.covered_description(),
        blind_spots = %blind_spots,
        "process allocator observation installed"
    );
}

/// Joins a descriptor's blind-spot labels into one comma-separated field value.
///
/// The stable labels are used rather than the prose descriptions so the field
/// stays a token list a later consumer can split, and the whole set is
/// rendered rather than a count so the log names *which* sources are missing.
/// Building a `String` is fine here: this runs on the startup path, never
/// inside an allocator callback, where allocating would recurse.
fn blind_spot_labels(coverage: CoverageDescriptor) -> String {
    coverage
        .blind_spots()
        .iter()
        .map(|spot| spot.label())
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod tests {
    use novarocks_memory::observe::BlindSpot;

    use super::*;

    #[test]
    fn the_installed_allocator_counts_this_process() {
        let before = snapshot();
        // A heap allocation the optimiser cannot remove, large enough that no
        // small-allocation shortcut can serve it without the allocator.
        let held = std::hint::black_box(vec![0u8; 1 << 20]);
        let after = snapshot();
        assert!(
            after.allocations > before.allocations,
            "installed wrapper counted no allocation: {before:?} then {after:?}"
        );
        assert!(
            after.allocated_total_bytes >= before.allocated_total_bytes + (1 << 20),
            "installed wrapper missed a 1 MiB request: {before:?} then {after:?}"
        );
        drop(held);
    }

    #[test]
    fn the_logged_blind_spots_name_every_source_the_reading_misses() {
        let rendered = blind_spot_labels(coverage());
        let labels: Vec<&str> = rendered.split(',').collect();
        assert_eq!(labels.len(), BlindSpot::ALL.len());
        for spot in BlindSpot::ALL {
            assert!(labels.contains(&spot.label()), "{rendered}");
        }
    }

    #[test]
    fn coverage_describes_the_rust_global_allocator() {
        assert_eq!(coverage(), CoverageDescriptor::RUST_GLOBAL_ALLOCATOR);
        assert!(
            coverage()
                .covered_description()
                .contains("global allocator"),
            "{}",
            coverage().covered_description()
        );
    }
}
