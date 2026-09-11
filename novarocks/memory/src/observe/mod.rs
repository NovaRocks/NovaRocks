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

//! Process allocation observation (MEM-1 wave-1 T04).
//!
//! The observation tier measures what hard governance cannot cover. It reports
//! what it covers and what it cannot see, so a difference against known `L` is
//! a diagnostic rather than an attribution.
//!
//! The tier is two modules and no policy:
//!
//! - [`allocator`] wraps the selected Rust global allocator and counts the
//!   requests that pass through it, without attributing a byte to any work.
//! - [`coverage`] states, as typed data, what such a count includes and the
//!   sources it structurally cannot include.
//!
//! Nothing here grants, refuses, waits or reclaims. A reading is evidence for
//! the headroom budget in the governance tier — never a substitute for the
//! accounts that own `L`, `F` and `O`.

pub mod allocator;
pub mod coverage;

pub use allocator::{AllocatorSnapshot, CountingAllocator, SHARD_COUNT};
pub use coverage::{BlindSpot, CoverageDescriptor, CoverageReport, MeasuredSource, SourceReading};
