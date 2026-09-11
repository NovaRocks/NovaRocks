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

//! Fixed-cost measurement for `arrow-buffer` primitives on buffers that no
//! memory pool has claimed.
//!
//! The `pool` feature adds a `Mutex<Option<Box<dyn MemoryReservation>>>` to
//! `Bytes` and `MutableBuffer`, and makes `reallocate`, `truncate`, `resize`
//! and `clear` take that lock even when no reservation exists. This binary
//! measures exactly those paths so the same scenarios can be compared with the
//! feature off (MEM-1 wave-1 T00 baseline) and on (T08), establishing the
//! fixed cost every Arrow buffer in the process pays.
//!
//! This is a measurement harness, not a latency gate: it prints per-scenario
//! medians and p90 and leaves threshold judgement to the plan record.

use std::hint::black_box;
use std::time::{Duration, Instant};

use arrow_buffer::{Buffer, MutableBuffer};

/// Warm-up rounds discarded before sampling.
const WARMUP: usize = 3;
/// Samples collected per scenario; each sample runs one full work unit.
const SAMPLES: usize = 30;
/// Repetition count for the cheap in-place operations.
const OPS: usize = 1_000_000;
/// Payload size for the allocation and slice scenarios.
const PAYLOAD_BYTES: usize = 1024 * 1024;

fn percentile(sorted: &[Duration], fraction: f64) -> Duration {
    assert!(!sorted.is_empty(), "percentile of an empty sample set");
    let last = sorted.len() - 1;
    let position = (fraction * last as f64).round() as usize;
    sorted[position.min(last)]
}

struct Measurement {
    scenario: &'static str,
    unit: &'static str,
    operations: u64,
    median: Duration,
    p90: Duration,
}

fn measure(
    scenario: &'static str,
    unit: &'static str,
    operations: u64,
    mut work: impl FnMut(),
) -> Measurement {
    for _ in 0..WARMUP {
        work();
    }
    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let started = Instant::now();
        work();
        samples.push(started.elapsed());
    }
    samples.sort_unstable();
    Measurement {
        scenario,
        unit,
        operations,
        median: percentile(&samples, 0.5),
        p90: percentile(&samples, 0.9),
    }
}

/// Allocate a fresh `MutableBuffer` and fill it one `u64` at a time.
fn with_capacity_push() {
    let mut buffer = MutableBuffer::with_capacity(PAYLOAD_BYTES);
    let words = PAYLOAD_BYTES / size_of::<u64>();
    for index in 0..words {
        buffer.push(index as u64);
    }
    black_box(buffer.len());
}

/// Grow a small buffer through ten doubling reallocations.
fn reallocate_doubling() {
    let mut buffer = MutableBuffer::with_capacity(1024);
    let mut target = 1024usize;
    for _ in 0..10 {
        target *= 2;
        buffer.reserve(target);
    }
    black_box(buffer.capacity());
}

/// Repeat `truncate` with an index-dependent length, reading the resulting
/// length back every iteration.
///
/// Both the varying argument and the observed result are required: with the
/// `pool` feature off these calls are a plain field store, and a loop of
/// identical dead stores is eliminated entirely by the optimiser, which would
/// make the baseline measure nothing and any later comparison meaningless.
///
/// Note what this scenario does and does not cover. `truncate` returns early
/// when the requested length is above the current one, and the length here
/// only ever decreases, so after the first cycle roughly seven calls in eight
/// return before touching any bookkeeping. It therefore measures the guard,
/// not the lock. `mutable_resize` and `mutable_clear_resize` are the honest
/// per-call bookkeeping measurements.
fn truncate_in_place(buffer: &mut MutableBuffer, len: usize) {
    let mut observed = 0usize;
    for index in 0..OPS {
        buffer.truncate(len - (index & 7));
        observed = observed.wrapping_add(buffer.len());
    }
    black_box(observed);
}

/// Repeat `resize` with an index-dependent length, reading the result back.
fn resize_in_place(buffer: &mut MutableBuffer, len: usize) {
    let mut observed = 0usize;
    for index in 0..OPS {
        buffer.resize(len - (index & 7), 0);
        observed = observed.wrapping_add(buffer.len());
    }
    black_box(observed);
}

/// Repeat `clear` followed by a small in-capacity `resize`, so the cleared
/// length is observable and the pair cannot collapse.
fn clear_in_place(buffer: &mut MutableBuffer) {
    let mut observed = 0usize;
    for index in 0..OPS {
        buffer.clear();
        observed = observed.wrapping_add(buffer.len());
        buffer.resize(1 + (index & 7), 0);
    }
    black_box(observed);
}

/// Slice and drop an immutable buffer, exercising the shared `Bytes` refcount
/// and drop path.
fn slice_drop(buffer: &Buffer) {
    for _ in 0..OPS {
        let sliced = buffer.slice_with_length(0, 4096);
        black_box(sliced.len());
    }
}

/// Clone and drop an immutable buffer.
fn clone_drop(buffer: &Buffer) {
    for _ in 0..OPS {
        let cloned = buffer.clone();
        black_box(cloned.len());
    }
}

fn main() {
    let shared = Buffer::from_vec(vec![0u8; PAYLOAD_BYTES]);
    // The in-place scenarios reuse one buffer so the sampled window measures
    // the bookkeeping path, not a 1 MiB allocation per sample.
    let mut in_place = MutableBuffer::from_len_zeroed(PAYLOAD_BYTES);
    let in_place_len = in_place.len();

    let measurements = vec![
        measure(
            "mutable_with_capacity_push_1mib",
            "1 MiB of u64 pushes",
            (PAYLOAD_BYTES / size_of::<u64>()) as u64,
            with_capacity_push,
        ),
        measure(
            "mutable_reallocate_doubling_10",
            "10 doubling reserves",
            10,
            reallocate_doubling,
        ),
        measure("mutable_truncate", "1e6 truncate calls", OPS as u64, || {
            truncate_in_place(&mut in_place, in_place_len)
        }),
        measure("mutable_resize", "1e6 resize calls", OPS as u64, || {
            resize_in_place(&mut in_place, in_place_len)
        }),
        measure(
            "mutable_clear_resize",
            "1e6 clear+resize pairs",
            2 * OPS as u64,
            || clear_in_place(&mut in_place),
        ),
        measure("buffer_slice_drop", "1e6 slice+drop", OPS as u64, || {
            slice_drop(&shared)
        }),
        measure("buffer_clone_drop", "1e6 clone+drop", OPS as u64, || {
            clone_drop(&shared)
        }),
    ];

    // `MutableBuffer` stores its reservation inline, so its size is the
    // reliable local signal for whether the resolved `arrow-buffer` build has
    // the `pool` feature enabled. A crate-local `cfg!(feature = ...)` cannot
    // observe a dependency's features.
    println!("arrow-buffer fixed cost");
    println!(
        "samples={SAMPLES} warmup={WARMUP} size_of::<MutableBuffer>()={} size_of::<Buffer>()={} size_of::<Mutex<Option<Box<()>>>>()={}",
        size_of::<MutableBuffer>(),
        size_of::<Buffer>(),
        size_of::<std::sync::Mutex<Option<Box<()>>>>()
    );
    println!(
        "{:<34} {:>14} {:>14} {:>12}  {}",
        "scenario", "median_ns", "p90_ns", "ns_per_op", "work unit"
    );
    for measurement in &measurements {
        // With the feature off, the in-place primitives are one field store,
        // so a percentage against them is dominated by the baseline being
        // sub-nanosecond. The per-operation figure is what a reader can use.
        let per_op = measurement.median.as_nanos() as f64 / measurement.operations as f64;
        println!(
            "{:<34} {:>14} {:>14} {:>12.3}  {}",
            measurement.scenario,
            measurement.median.as_nanos(),
            measurement.p90.as_nanos(),
            per_op,
            measurement.unit
        );
    }

    print!("{{\"measurements\":[");
    for (index, measurement) in measurements.iter().enumerate() {
        if index > 0 {
            print!(",");
        }
        print!(
            "{{\"scenario\":\"{}\",\"median_ns\":{},\"p90_ns\":{}}}",
            measurement.scenario,
            measurement.median.as_nanos(),
            measurement.p90.as_nanos()
        );
    }
    println!("]}}");
}
