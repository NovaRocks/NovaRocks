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

//! Coverage and blind spots (MEM-1 wave-1 T04).
//!
//! Every allocator report states its coverage and lists what it cannot see:
//! direct `System` calls, custom allocators, native libraries and direct
//! mappings, plus allocator-internal retention and fragmentation. Where a
//! source can be measured its statistics are added as their own line; where it
//! cannot, the value stays unknown rather than being folded into a total.
//!
//! # Why the blind spots are typed data
//!
//! A margin, a fudge factor or a paragraph in a document all fail the same
//! way: the number that reaches an operator looks complete. The wrapper in
//! [`super::allocator`] measures one source honestly and knows several sources
//! it cannot measure at all, so the blind spots are enumerated as a value that
//! travels with the reading — [`BlindSpot::ALL`] — and a report renders them
//! beside the bytes rather than inside them.
//!
//! # Unknown is a value, not a zero
//!
//! Some blind spots become measurable when something else publishes a number:
//! a subsystem's arena reports its own bytes, an allocator build exposes its
//! internal statistics, the operating system reports the resident set. Those
//! arrive as their own [`SourceReading::Measured`] line.
//! [`BlindSpot::measurable_source`] says which blind spot each line answers.
//! What no line ever does is change the allocator's `live_bytes`: a
//! [`CoverageReport`] has no method that sums its readings, because two
//! sources measured by different mechanisms at different instants, with
//! unknown overlap, do not add up. A source nobody can measure stays
//! [`SourceReading::Unknown`] and is displayed as unknown.

use super::allocator::AllocatorSnapshot;

/// Something outside the counted allocator that holds process memory.
///
/// A blind spot is not an error and not an estimate: it is a named reason the
/// counted bytes are a lower bound on what the process holds. Naming them lets
/// a diagnostic say *which* explanation applies instead of asserting that the
/// difference between a reading and reality is noise.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BlindSpot {
    /// Code that calls `std::alloc::System`, `libc::malloc` or another
    /// allocator interface directly instead of going through the process
    /// allocator. Its bytes are never offered to the wrapper.
    DirectSystemAllocation,
    /// A subsystem that satisfies its own requests from an arena, pool or bump
    /// region. Only the region it obtained is visible, and only if it obtained
    /// it through the process allocator; the sub-allocations inside it are not.
    CustomAllocator,
    /// A linked native library, or an embedded runtime such as a JVM, that
    /// allocates through its own allocator entirely.
    NativeLibrary,
    /// Memory taken straight from the operating system — `mmap`, `VirtualAlloc`,
    /// huge pages, file mappings — which never reaches an allocator interface.
    DirectMapping,
    /// The counted allocator's own overhead: each request is rounded up to a
    /// size class, freed pages are retained for reuse rather than returned,
    /// and long-lived churn fragments the heap. The process therefore holds
    /// more than the sum of requested sizes, and by an amount only the
    /// allocator knows.
    AllocatorRetentionAndFragmentation,
}

impl BlindSpot {
    /// Every blind spot of the counted allocator, in report order.
    ///
    /// A slice rather than a builder: the list is a property of the mechanism,
    /// not of a deployment, so no caller can quietly ship a shorter one.
    pub const ALL: &'static [Self] = &[
        Self::DirectSystemAllocation,
        Self::CustomAllocator,
        Self::NativeLibrary,
        Self::DirectMapping,
        Self::AllocatorRetentionAndFragmentation,
    ];

    /// Returns the stable label used in snapshots, diagnostics and metrics.
    pub const fn label(self) -> &'static str {
        match self {
            Self::DirectSystemAllocation => "direct-system-allocation",
            Self::CustomAllocator => "custom-allocator",
            Self::NativeLibrary => "native-library",
            Self::DirectMapping => "direct-mapping",
            Self::AllocatorRetentionAndFragmentation => "allocator-retention-and-fragmentation",
        }
    }

    /// Returns the human explanation shown next to a reading.
    pub const fn description(self) -> &'static str {
        match self {
            Self::DirectSystemAllocation => {
                "Allocations made directly through System or libc bypass the selected process \
                 allocator and are never offered to the counting wrapper."
            }
            Self::CustomAllocator => {
                "A subsystem with its own arena, pool or bump allocator serves requests from a \
                 region it already holds, so its internal allocations are invisible even when \
                 the region itself was counted."
            }
            Self::NativeLibrary => {
                "A linked native library or embedded runtime allocates through its own \
                 allocator, so none of its bytes pass through the counting wrapper."
            }
            Self::DirectMapping => {
                "Memory mapped straight from the operating system never reaches an allocator \
                 interface, so it is absent from the counters entirely."
            }
            Self::AllocatorRetentionAndFragmentation => {
                "The counted allocator rounds requests up to size classes, retains freed pages \
                 for reuse and fragments over time, so resident memory exceeds the sum of \
                 requested sizes by an amount only the allocator knows."
            }
        }
    }

    /// Returns the source that can put a number on this blind spot, when one
    /// exists.
    ///
    /// `None` means no mechanism in this process can quantify it, so it stays
    /// [`SourceReading::Unknown`] and must be read as "unmeasured", not as
    /// zero.
    pub const fn measurable_source(self) -> Option<MeasuredSource> {
        match self {
            Self::DirectSystemAllocation => None,
            Self::CustomAllocator => Some(MeasuredSource::CustomAllocator),
            Self::NativeLibrary => Some(MeasuredSource::NativeLibrary),
            Self::DirectMapping => Some(MeasuredSource::OperatingSystemResident),
            Self::AllocatorRetentionAndFragmentation => Some(MeasuredSource::AllocatorInternals),
        }
    }
}

/// A place a byte count can come from.
///
/// Each source is measured by its own mechanism, over its own scope, at its
/// own instant. They are enumerated so a report can name where a line came
/// from — and so it is obvious why the lines are not summed: the scopes
/// overlap in ways nothing here can resolve. The operating system's resident
/// set, for instance, already contains most of the counted allocator's bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MeasuredSource {
    /// The counting wrapper around the Rust global allocator. Always
    /// measurable: it is this crate's own counter.
    RustGlobalAllocator,
    /// The selected allocator's internal statistics, such as a jemalloc
    /// `stats.resident` reading, when the build exposes them.
    AllocatorInternals,
    /// A subsystem's own published counter for the arena or pool it manages.
    CustomAllocator,
    /// A native library's or embedded runtime's own published counter.
    NativeLibrary,
    /// The operating system's process-level reading, such as the resident set
    /// size or the working set.
    OperatingSystemResident,
}

impl MeasuredSource {
    /// Returns the stable label used in snapshots, diagnostics and metrics.
    pub const fn label(self) -> &'static str {
        match self {
            Self::RustGlobalAllocator => "rust-global-allocator",
            Self::AllocatorInternals => "allocator-internals",
            Self::CustomAllocator => "custom-allocator",
            Self::NativeLibrary => "native-library",
            Self::OperatingSystemResident => "operating-system-resident",
        }
    }
}

/// One line of a coverage report: a source, and either its bytes or the
/// explicit fact that nobody measured it.
///
/// The two states are separate variants rather than an amount that defaults to
/// zero, because "this source holds no bytes" and "nobody can tell how many
/// bytes this source holds" lead to opposite operational decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SourceReading {
    /// The source published a byte count of its own.
    Measured {
        /// Where the number came from.
        source: MeasuredSource,
        /// The bytes that source reports, in its own scope.
        bytes: u64,
    },
    /// The source exists but nothing in this process quantifies it. It is
    /// reported as unknown and never replaced by an estimate.
    Unknown {
        /// Which source is unquantified.
        source: MeasuredSource,
    },
}

impl SourceReading {
    /// Builds a measured line.
    pub const fn measured(source: MeasuredSource, bytes: u64) -> Self {
        Self::Measured { source, bytes }
    }

    /// Builds an unmeasured line.
    pub const fn unknown(source: MeasuredSource) -> Self {
        Self::Unknown { source }
    }

    /// Returns the source this line describes.
    pub const fn source(self) -> MeasuredSource {
        match self {
            Self::Measured { source, .. } | Self::Unknown { source } => source,
        }
    }

    /// Returns the measured bytes, or `None` when the source is unmeasured.
    ///
    /// Deliberately an `Option` rather than a defaulted `u64`: a caller that
    /// wants to display the line must decide what "unknown" looks like, and
    /// cannot arrive at a number by accident.
    pub const fn measured_bytes(self) -> Option<u64> {
        match self {
            Self::Measured { bytes, .. } => Some(bytes),
            Self::Unknown { .. } => None,
        }
    }

    /// Reports whether this line carries a number.
    pub const fn is_measured(self) -> bool {
        matches!(self, Self::Measured { .. })
    }
}

/// What one measurement mechanism covers, and what it does not.
///
/// Held as data — a source, a description of the covered set, and the typed
/// blind-spot list — so a report renders the same statement everywhere instead
/// of each call site paraphrasing it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoverageDescriptor {
    source: MeasuredSource,
    covered_description: &'static str,
    blind_spots: &'static [BlindSpot],
}

impl CoverageDescriptor {
    /// The descriptor of [`super::allocator::CountingAllocator`]: what a
    /// wrapper around the Rust global allocator can and cannot see.
    pub const RUST_GLOBAL_ALLOCATOR: Self = Self {
        source: MeasuredSource::RustGlobalAllocator,
        covered_description: "Requested bytes of every allocation, reallocation and release \
                              routed through the selected Rust global allocator, counted as \
                              request sizes rather than as physical memory.",
        blind_spots: BlindSpot::ALL,
    };

    /// Returns the source this descriptor describes.
    pub const fn source(self) -> MeasuredSource {
        self.source
    }

    /// Returns what the mechanism does cover.
    pub const fn covered_description(self) -> &'static str {
        self.covered_description
    }

    /// Returns everything the mechanism cannot see.
    pub const fn blind_spots(self) -> &'static [BlindSpot] {
        self.blind_spots
    }
}

impl Default for CoverageDescriptor {
    fn default() -> Self {
        Self::RUST_GLOBAL_ALLOCATOR
    }
}

/// An allocator reading, its coverage statement, and any separately measured
/// sources.
///
/// The report is the honest shape of the observation tier's output: one
/// measured number with its name, a list of what that number excludes, and
/// zero or more additional lines that other mechanisms contributed. It offers
/// no total. [`Self::live_bytes`] returns the allocator's own bytes and
/// nothing else, no matter how many readings were added, because adding a
/// resident-set reading to an allocator reading double-counts everything they
/// share, and adding an unknown to anything yields an unknown.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CoverageReport {
    allocator: AllocatorSnapshot,
    readings: Vec<SourceReading>,
}

impl CoverageReport {
    /// Starts a report from one allocator reading.
    pub fn new(allocator: AllocatorSnapshot) -> Self {
        Self {
            allocator,
            readings: Vec::new(),
        }
    }

    /// Adds a line for another source, consuming the report.
    #[must_use]
    pub fn with_reading(mut self, reading: SourceReading) -> Self {
        self.push_reading(reading);
        self
    }

    /// Adds a line for another source.
    ///
    /// The line is kept as its own entry. Nothing is merged, and no existing
    /// number changes.
    pub fn push_reading(&mut self, reading: SourceReading) {
        self.readings.push(reading);
    }

    /// Returns the coverage statement of the allocator reading.
    pub const fn descriptor(&self) -> CoverageDescriptor {
        CoverageDescriptor::RUST_GLOBAL_ALLOCATOR
    }

    /// Returns the allocator reading this report was built from.
    pub const fn allocator(&self) -> &AllocatorSnapshot {
        &self.allocator
    }

    /// Returns the separately measured lines, in the order they were added.
    pub fn readings(&self) -> &[SourceReading] {
        &self.readings
    }

    /// Returns the counted allocator's live bytes, and only those.
    ///
    /// This is the report's single byte figure by design: it is what the
    /// wrapper measured. No reading added through [`Self::push_reading`] can
    /// move it.
    pub const fn live_bytes(&self) -> u64 {
        self.allocator.live_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_blind_spot_has_a_distinct_label_and_an_explanation() {
        let spots = BlindSpot::ALL;
        assert_eq!(spots.len(), 5);
        for (index, spot) in spots.iter().enumerate() {
            assert!(!spot.label().is_empty());
            assert!(spot.description().len() > spot.label().len());
            for other in &spots[index + 1..] {
                assert_ne!(spot.label(), other.label(), "{spot:?} vs {other:?}");
            }
        }
    }

    #[test]
    fn only_the_unquantifiable_blind_spot_has_no_source() {
        for spot in BlindSpot::ALL {
            let source = spot.measurable_source();
            match spot {
                BlindSpot::DirectSystemAllocation => assert_eq!(source, None),
                _ => assert!(source.is_some(), "{spot:?}"),
            }
        }
    }

    #[test]
    fn descriptor_states_its_coverage_and_lists_the_blind_spots() {
        let descriptor = CoverageDescriptor::default();
        assert_eq!(descriptor.source(), MeasuredSource::RustGlobalAllocator);
        assert!(
            descriptor
                .covered_description()
                .contains("Rust global allocator")
        );
        assert_eq!(descriptor.blind_spots(), BlindSpot::ALL);
    }

    #[test]
    fn an_unknown_reading_keeps_no_number() {
        let unknown = SourceReading::unknown(MeasuredSource::NativeLibrary);
        assert!(!unknown.is_measured());
        assert_eq!(unknown.measured_bytes(), None);
        assert_eq!(unknown.source(), MeasuredSource::NativeLibrary);

        let measured = SourceReading::measured(MeasuredSource::AllocatorInternals, 4_096);
        assert!(measured.is_measured());
        assert_eq!(measured.measured_bytes(), Some(4_096));
    }

    #[test]
    fn added_readings_never_change_the_allocator_bytes() {
        let allocator = AllocatorSnapshot {
            live_bytes: 1_024,
            allocated_total_bytes: 4_096,
            deallocated_total_bytes: 3_072,
            allocations: 8,
            reallocations: 1,
            failures: 0,
        };
        let report = CoverageReport::new(allocator)
            .with_reading(SourceReading::measured(
                MeasuredSource::OperatingSystemResident,
                64 * 1_024 * 1_024,
            ))
            .with_reading(SourceReading::unknown(MeasuredSource::CustomAllocator));

        assert_eq!(report.live_bytes(), 1_024);
        assert_eq!(report.allocator().live_bytes, 1_024);
        assert_eq!(report.readings().len(), 2);
        assert_eq!(report.descriptor().blind_spots().len(), 5);
    }
}
