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

//! Physical pressure contracts (MEM-1 wave-1 T03).
//!
//! A pressure sample carries its source, its sampling time, its value and its
//! coverage. Samples from different sources describe overlapping views of one
//! process and must not be added. The core defines the vocabulary; sampling
//! and the actions taken in response belong to the arbitrator.
//!
//! # Why samples must never be added
//!
//! There is only one process and only one pool of physical memory. Each source
//! is a different *view* of it, taken through a different instrument:
//!
//! - the Rust global allocator knows the bytes it handed out, and nothing
//!   about a native library's arena;
//! - the process resident set knows the pages the kernel has mapped, which
//!   includes the allocator's bytes, the library's arena, and the binary
//!   itself;
//! - a container limit knows the charge the cgroup accounts, which is another
//!   whole-process view with its own rules about page cache and shared pages.
//!
//! Add an allocator reading of 6 GiB to a resident-set reading of 8 GiB and
//! you get 14 GiB of memory that does not exist — most of it counted twice.
//! Whoever acts on that number throttles or kills a workload for a condition
//! that was never real.
//!
//! So there is no summation API in this module and there will not be one.
//! Instead [`PressureSample::describes_same_ground_as`] lets a consumer detect
//! that two samples overlap, so the honest operations are available: compare
//! them, prefer the more authoritative one, report them side by side, or take
//! the maximum. What is not available is adding them.
//!
//! # Why the core takes no clock
//!
//! `sampled_at_nanos` is supplied by the caller. The core reads no clock, so
//! it needs no `std::time` behaviour of its own, cannot be made
//! non-deterministic in a test, and cannot disagree with the arbitrator's
//! notion of time. The value must come from a monotonic source: samples are
//! compared for ordering, and a wall clock that steps backwards would make an
//! old reading look like the newest one.

use std::fmt;

/// How much physical pressure the process is under.
///
/// The core defines the three names and nothing else — no thresholds, no
/// actions. What counts as `Elevated` depends on the deployment's headroom
/// budget and on how violently the platform reacts to overcommit, both of
/// which are the arbitrator's to know.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PressureLevel {
    /// Within expectations. Nothing to do.
    Nominal,
    /// Above expectations. Worth reclaiming proactively and worth admitting
    /// new work more carefully.
    Elevated,
    /// Close to a limit the process does not control. The next allocation may
    /// be answered by the kernel rather than by the authority.
    Critical,
}

impl PressureLevel {
    /// Returns the label used in diagnostics.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Nominal => "nominal",
            Self::Elevated => "elevated",
            Self::Critical => "critical",
        }
    }

    /// Classifies an observed byte count against caller-supplied thresholds.
    ///
    /// The thresholds are the caller's policy; the core holds none of its own.
    /// A `critical_at_bytes` below `elevated_at_bytes` is honoured rather than
    /// corrected — the more severe verdict wins — so a misordered pair
    /// produces a conservative answer instead of a silent inversion.
    pub const fn from_thresholds(
        observed_bytes: u64,
        elevated_at_bytes: u64,
        critical_at_bytes: u64,
    ) -> Self {
        if observed_bytes >= critical_at_bytes {
            Self::Critical
        } else if observed_bytes >= elevated_at_bytes {
            Self::Elevated
        } else {
            Self::Nominal
        }
    }

    /// Reports whether this level is at least as severe as `other`.
    pub const fn is_at_least(self, other: Self) -> bool {
        self as u8 >= other as u8
    }
}

impl fmt::Display for PressureLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// Which instrument produced a sample.
///
/// The source is kept beside the value because a byte count is meaningless
/// without knowing what counted it. Two sources reporting different numbers is
/// the normal case, not a contradiction to be resolved by averaging.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PressureSource {
    /// The counting global allocator: bytes Rust code asked for and has not
    /// freed. Blind to everything allocated another way.
    RustAllocator,
    /// The operating system's resident set for this process. Includes every
    /// other source, plus the binary and its mappings.
    ProcessResident,
    /// The charge a container or cgroup accounts against its limit. Another
    /// whole-process view, with its own rules for page cache and shared pages.
    ContainerLimit,
    /// A native library reporting its own arena. Covers that library only.
    NativeLibrary,
    /// A source the process cannot classify. Reported so it is visible rather
    /// than dropped, and trusted for nothing.
    Unknown,
}

impl PressureSource {
    /// Returns the label used in diagnostics.
    pub const fn label(self) -> &'static str {
        match self {
            Self::RustAllocator => "rust-allocator",
            Self::ProcessResident => "process-resident",
            Self::ContainerLimit => "container-limit",
            Self::NativeLibrary => "native-library",
            Self::Unknown => "unknown",
        }
    }
}

impl fmt::Display for PressureSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// What a sample measured, or that it could not measure.
///
/// An unknown reading is a first-class value rather than a zero. Zero means
/// "nothing is allocated", which is a very different claim from "the
/// instrument was unavailable", and collapsing the two is how a monitoring
/// gap becomes a false all-clear.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PressureValue {
    /// A measured byte count.
    Bytes(u64),
    /// The instrument produced no reading this time.
    Unknown,
}

impl PressureValue {
    /// Returns the measured bytes, or `None` when unknown.
    pub const fn bytes(self) -> Option<u64> {
        match self {
            Self::Bytes(bytes) => Some(bytes),
            Self::Unknown => None,
        }
    }

    /// Reports whether the instrument produced a reading.
    pub const fn is_known(self) -> bool {
        matches!(self, Self::Bytes(_))
    }
}

impl fmt::Display for PressureValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bytes(bytes) => write!(f, "{bytes} bytes"),
            Self::Unknown => f.write_str("unknown"),
        }
    }
}

/// What region of the process a sample's value covers.
///
/// This is the field that makes overlap detectable. Two samples may come from
/// different [`PressureSource`]s and still describe the same ground — a
/// resident-set reading and a container charge both cover the whole process —
/// and the ground, not the source, is what decides whether comparing them is
/// meaningful and adding them is nonsense.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CoverageGround {
    /// Bytes handed out by this process's Rust global allocator.
    RustAllocations,
    /// Every page the kernel counts as resident for this process.
    ProcessResidentSet,
    /// Everything a container or cgroup charges to its limit.
    ContainerCharge,
    /// One native library's own arena.
    NativeLibraryArena,
}

impl CoverageGround {
    /// Returns the label used in diagnostics.
    pub const fn label(self) -> &'static str {
        match self {
            Self::RustAllocations => "rust-allocations",
            Self::ProcessResidentSet => "process-resident-set",
            Self::ContainerCharge => "container-charge",
            Self::NativeLibraryArena => "native-library-arena",
        }
    }

    /// Reports whether this ground covers the whole process.
    ///
    /// A whole-process ground contains every other ground, so it overlaps
    /// everything — including another whole-process ground measured by a
    /// different instrument.
    pub const fn is_whole_process(self) -> bool {
        matches!(self, Self::ProcessResidentSet | Self::ContainerCharge)
    }

    /// Reports whether two grounds can describe the same bytes.
    ///
    /// The rule, stated so its assumptions are visible rather than buried in
    /// an arithmetic decision:
    ///
    /// - a ground always overlaps itself;
    /// - a whole-process ground overlaps everything, itself and each other
    ///   included, because both contain all of the process's pages;
    /// - [`Self::RustAllocations`] and [`Self::NativeLibraryArena`] are treated
    ///   as disjoint, because a library that manages its own arena does not go
    ///   through the Rust global allocator and so is not counted by it. A
    ///   library that *does* allocate through the Rust allocator should report
    ///   itself as `RustAllocations`, not as an arena.
    pub const fn overlaps(self, other: Self) -> bool {
        if self.is_whole_process() || other.is_whole_process() {
            return true;
        }
        matches!(
            (self, other),
            (Self::RustAllocations, Self::RustAllocations)
                | (Self::NativeLibraryArena, Self::NativeLibraryArena)
        )
    }
}

impl fmt::Display for CoverageGround {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// What a sample covers, and how completely.
///
/// `partial` is not a quality score, it is a warning about direction: a
/// partial sample is a *lower bound* on its ground. A consumer may treat a
/// partial reading that already exceeds a threshold as decisive, and must not
/// treat one below a threshold as an all-clear.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SampleCoverage {
    /// The region of the process the value covers.
    pub ground: CoverageGround,
    /// Whether the instrument saw only part of that ground.
    pub partial: bool,
}

impl SampleCoverage {
    /// Coverage of the whole named ground.
    pub const fn complete(ground: CoverageGround) -> Self {
        Self {
            ground,
            partial: false,
        }
    }

    /// Coverage of part of the named ground, so the value is a lower bound.
    pub const fn partial(ground: CoverageGround) -> Self {
        Self {
            ground,
            partial: true,
        }
    }

    /// Reports whether two coverages can describe the same bytes.
    ///
    /// Delegates to [`CoverageGround::overlaps`]: whether a value is partial
    /// changes how much of its ground it saw, never which ground that is.
    pub const fn overlaps(&self, other: &Self) -> bool {
        self.ground.overlaps(other.ground)
    }
}

impl fmt::Display for SampleCoverage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.partial {
            write!(f, "{} (partial)", self.ground)
        } else {
            write!(f, "{}", self.ground)
        }
    }
}

/// One reading of physical pressure.
///
/// Four facts travel together because none of them means anything alone: a
/// byte count without its coverage cannot be compared to another, and a
/// coverage without its timestamp cannot be ordered against a later reading.
///
/// There is no method that adds two samples. See the module documentation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PressureSample {
    /// The instrument that produced the reading.
    pub source: PressureSource,
    /// When it was taken, from a caller-supplied monotonic clock.
    ///
    /// The core reads no clock of its own. The value must be monotonic:
    /// samples are ordered against each other, and a wall clock that steps
    /// backwards would make a stale reading look like the newest one.
    pub sampled_at_nanos: u64,
    /// What was measured, or that it could not be.
    pub value: PressureValue,
    /// What the value covers, and how completely.
    pub coverage: SampleCoverage,
}

impl PressureSample {
    /// Builds a sample from its four facts.
    pub const fn new(
        source: PressureSource,
        sampled_at_nanos: u64,
        value: PressureValue,
        coverage: SampleCoverage,
    ) -> Self {
        Self {
            source,
            sampled_at_nanos,
            value,
            coverage,
        }
    }

    /// Builds a sample whose instrument produced no reading.
    ///
    /// Recorded rather than dropped: knowing that a source went silent at a
    /// given time is itself a diagnostic, and far better than an absence a
    /// consumer will read as zero.
    pub const fn unknown(
        source: PressureSource,
        sampled_at_nanos: u64,
        coverage: SampleCoverage,
    ) -> Self {
        Self::new(source, sampled_at_nanos, PressureValue::Unknown, coverage)
    }

    /// Returns the measured bytes, or `None` when the reading is unknown.
    pub const fn bytes(&self) -> Option<u64> {
        self.value.bytes()
    }

    /// Reports whether this sample and `other` can describe the same bytes.
    ///
    /// True means the two readings overlap, so they may be compared but must
    /// never be added: their sum would count shared bytes more than once.
    /// This is the method that exists in place of the addition a consumer
    /// might otherwise write.
    ///
    /// Two samples from the same source always describe the same ground, and
    /// samples from different sources often do too — a resident-set reading
    /// and a container charge both cover the whole process.
    pub const fn describes_same_ground_as(&self, other: &Self) -> bool {
        self.coverage.overlaps(&other.coverage)
    }

    /// Reports whether this sample was taken after `other`.
    pub const fn is_newer_than(&self, other: &Self) -> bool {
        self.sampled_at_nanos > other.sampled_at_nanos
    }

    /// Classifies this sample against caller-supplied thresholds.
    ///
    /// Returns `None` for an unknown reading: a missing measurement is not
    /// evidence of a nominal one, and answering `Nominal` here would turn
    /// every monitoring gap into a false all-clear.
    pub const fn level_from_thresholds(
        &self,
        elevated_at_bytes: u64,
        critical_at_bytes: u64,
    ) -> Option<PressureLevel> {
        match self.value.bytes() {
            Some(bytes) => Some(PressureLevel::from_thresholds(
                bytes,
                elevated_at_bytes,
                critical_at_bytes,
            )),
            None => None,
        }
    }
}

impl fmt::Display for PressureSample {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} at {}ns: {} covering {}",
            self.source, self.sampled_at_nanos, self.value, self.coverage
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allocator_sample(bytes: u64, at_nanos: u64) -> PressureSample {
        PressureSample::new(
            PressureSource::RustAllocator,
            at_nanos,
            PressureValue::Bytes(bytes),
            SampleCoverage::complete(CoverageGround::RustAllocations),
        )
    }

    fn resident_sample(bytes: u64, at_nanos: u64) -> PressureSample {
        PressureSample::new(
            PressureSource::ProcessResident,
            at_nanos,
            PressureValue::Bytes(bytes),
            SampleCoverage::complete(CoverageGround::ProcessResidentSet),
        )
    }

    #[test]
    fn levels_order_by_severity() {
        assert!(PressureLevel::Critical > PressureLevel::Elevated);
        assert!(PressureLevel::Elevated > PressureLevel::Nominal);
        assert!(PressureLevel::Critical.is_at_least(PressureLevel::Nominal));
        assert!(!PressureLevel::Nominal.is_at_least(PressureLevel::Elevated));
        assert_eq!(PressureLevel::Elevated.to_string(), "elevated");
    }

    #[test]
    fn thresholds_are_the_callers_policy_and_the_severe_verdict_wins() {
        assert_eq!(
            PressureLevel::from_thresholds(10, 100, 200),
            PressureLevel::Nominal
        );
        assert_eq!(
            PressureLevel::from_thresholds(100, 100, 200),
            PressureLevel::Elevated
        );
        assert_eq!(
            PressureLevel::from_thresholds(200, 100, 200),
            PressureLevel::Critical
        );
        assert_eq!(
            PressureLevel::from_thresholds(150, 200, 100),
            PressureLevel::Critical,
            "a misordered threshold pair answers conservatively"
        );
    }

    #[test]
    fn an_unknown_value_is_not_a_zero() {
        let silent = PressureSample::unknown(
            PressureSource::NativeLibrary,
            42,
            SampleCoverage::complete(CoverageGround::NativeLibraryArena),
        );
        assert!(!silent.value.is_known());
        assert_eq!(silent.bytes(), None);
        assert_eq!(
            silent.level_from_thresholds(1, 2),
            None,
            "a missing measurement must not read as nominal"
        );
        assert_ne!(silent.value, PressureValue::Bytes(0));
        assert!(silent.to_string().contains("unknown"));
    }

    #[test]
    fn whole_process_grounds_overlap_everything_including_each_other() {
        assert!(
            CoverageGround::ProcessResidentSet.overlaps(CoverageGround::ContainerCharge),
            "two whole-process views count the same pages"
        );
        assert!(CoverageGround::ProcessResidentSet.overlaps(CoverageGround::RustAllocations));
        assert!(CoverageGround::ContainerCharge.overlaps(CoverageGround::NativeLibraryArena));
        assert!(CoverageGround::RustAllocations.overlaps(CoverageGround::ProcessResidentSet));
    }

    #[test]
    fn a_library_arena_is_disjoint_from_the_rust_allocator() {
        assert!(!CoverageGround::RustAllocations.overlaps(CoverageGround::NativeLibraryArena));
        assert!(!CoverageGround::NativeLibraryArena.overlaps(CoverageGround::RustAllocations));
        assert!(CoverageGround::RustAllocations.overlaps(CoverageGround::RustAllocations));
    }

    #[test]
    fn overlapping_samples_are_detectable_instead_of_addable() {
        let allocator = allocator_sample(6 * 1024, 1_000);
        let resident = resident_sample(8 * 1024, 1_100);
        assert!(
            allocator.describes_same_ground_as(&resident),
            "the allocator's bytes are inside the resident set"
        );
        assert!(resident.describes_same_ground_as(&allocator));
        // The honest operations on overlapping readings: order them and
        // compare them. Their sum would be 14 KiB of memory that does not
        // exist.
        assert!(resident.is_newer_than(&allocator));
        assert!(resident.bytes() > allocator.bytes());
    }

    #[test]
    fn partial_coverage_keeps_its_ground_and_marks_a_lower_bound() {
        let partial = PressureSample::new(
            PressureSource::RustAllocator,
            7,
            PressureValue::Bytes(1_024),
            SampleCoverage::partial(CoverageGround::RustAllocations),
        );
        let complete = allocator_sample(4_096, 8);
        assert!(partial.coverage.partial);
        assert!(!complete.coverage.partial);
        assert!(
            partial.describes_same_ground_as(&complete),
            "partial coverage narrows how much was seen, not which ground"
        );
        assert!(partial.to_string().contains("partial"));
    }

    #[test]
    fn sample_times_come_from_the_caller_and_order_the_readings() {
        let earlier = allocator_sample(1, 100);
        let later = allocator_sample(1, 200);
        assert!(later.is_newer_than(&earlier));
        assert!(!earlier.is_newer_than(&later));
        assert!(
            !earlier.is_newer_than(&earlier),
            "equal timestamps are not newer"
        );
    }
}
