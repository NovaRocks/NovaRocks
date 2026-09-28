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

//! Standalone reservation supply-load and saturation probe.
//! A fixed, state-dependent work function runs between operation pairs. Run
//! calibration once per target machine, then pass the same work iterations
//! to every candidate process. Parent interactions use a separate mixed mode.

use std::alloc::System;
use std::fs::File;
use std::hint::black_box;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::Instant;

use novarocks_memory::Reservation;
use novarocks_memory::account::TopUpPolicy;
use novarocks_memory::authority::{AuthorityConfig, MemoryAuthority};
use novarocks_memory::ids::{AccountKind, ExternalRef};
use novarocks_memory::observe::CountingAllocator;

#[allow(dead_code)]
#[path = "../src/reservation_protocol.rs"]
mod reservation_protocol;

#[global_allocator]
static ALLOCATOR: CountingAllocator<System> = CountingAllocator::new(System);

const QUANTUM: u64 = 1024 * 1024;
const DELTA: u64 = 64;
const DEFAULT_PAIRS: usize = 100_000;
const WORK_MULTIPLIER: u64 = 6_364_136_223_846_793_005;
const WORK_INCREMENT: u64 = 1_442_695_040_888_963_407;
const GENERATOR_VERSION: &str = "reservation-supply-v3";

#[derive(Default)]
struct PlainMutexLeaf {
    // (free, live, peak_live, closed); matches the leaf's fast-path facts.
    values: Mutex<(u64, u64, u64, bool)>,
    capacity: u64,
}

impl PlainMutexLeaf {
    fn new(capacity: u64) -> Self {
        Self {
            values: Mutex::new((capacity, 0, 0, false)),
            capacity,
        }
    }

    fn grow(&self, bytes: u64) {
        let mut values = self.values.lock().unwrap();
        assert!(!values.3);
        assert!(values.0 >= bytes);
        values.0 -= bytes;
        values.1 += bytes;
        values.2 = values.2.max(values.1);
    }

    fn shrink(&self, bytes: u64) {
        let mut values = self.values.lock().unwrap();
        assert!(values.1 >= bytes);
        values.1 -= bytes;
        values.0 += bytes;
        assert!(values.0 <= self.capacity);
    }
}

#[derive(Clone, Copy)]
enum Candidate {
    Reservation,
    Protocol,
    Mutex,
    None,
}

impl Candidate {
    fn name(self) -> &'static str {
        match self {
            Self::Reservation => "reservation",
            Self::Protocol => "protocol",
            Self::Mutex => "mutex",
            Self::None => "none",
        }
    }

    fn parse(value: &str) -> Self {
        match value {
            "reservation" => Self::Reservation,
            "protocol" => Self::Protocol,
            "mutex" => Self::Mutex,
            "none" => Self::None,
            _ => panic!("unknown candidate: {value}"),
        }
    }
}

struct Args {
    threads: usize,
    pairs: usize,
    work_iters: usize,
    candidate: Option<Candidate>,
    reverse: bool,
    mixed_every: usize,
    large_bytes: u64,
    latencies_file: Option<PathBuf>,
    release_latencies_file: Option<PathBuf>,
    calibrate: bool,
}

struct AtomicProtocolLeaf {
    free: AtomicU64,
    live: AtomicU64,
    peak_live: AtomicU64,
    closed: AtomicBool,
}

impl AtomicProtocolLeaf {
    fn new() -> Self {
        Self {
            free: AtomicU64::new(QUANTUM),
            live: AtomicU64::new(0),
            peak_live: AtomicU64::new(0),
            closed: AtomicBool::new(false),
        }
    }

    fn grow(&self, bytes: u64) {
        assert!(!self.closed.load(Ordering::Acquire));
        assert!(reservation_protocol::take_free_with_retries(&self.free, bytes).0);
        let live = reservation_protocol::add_live(&self.live, bytes);
        let mut observed = self.peak_live.load(Ordering::Relaxed);
        while live > observed {
            match self.peak_live.compare_exchange_weak(
                observed,
                live,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => observed = actual,
            }
        }
    }

    fn shrink(&self, bytes: u64) {
        let free = reservation_protocol::release_live(&self.live, &self.free, bytes);
        assert!(free <= 2 * QUANTUM);
    }
}

fn main() {
    let args = parse_args();
    if args.calibrate {
        calibrate_work();
        return;
    }
    assert!(args.threads > 0, "threads must be positive");
    assert!(args.pairs > 0, "pairs must be positive");
    if args.mixed_every > 0 {
        assert!(
            matches!(
                args.candidate,
                Some(Candidate::Reservation | Candidate::Mutex)
            ),
            "mixed mode measures the reservation or mutex leaf"
        );
        assert!(
            args.large_bytes > 2 * QUANTUM,
            "mixed release samples must exceed the 2*Q return threshold"
        );
    }
    if let Some(candidate) = args.candidate {
        run(candidate, &args);
    } else {
        assert!(
            args.latencies_file.is_none(),
            "select one candidate when writing raw latencies"
        );
        let order = if args.reverse {
            [
                Candidate::Reservation,
                Candidate::Protocol,
                Candidate::Mutex,
                Candidate::None,
            ]
        } else {
            [
                Candidate::None,
                Candidate::Mutex,
                Candidate::Protocol,
                Candidate::Reservation,
            ]
        };
        for candidate in order {
            run(candidate, &args);
        }
    }
}

fn parse_args() -> Args {
    let mut args = Args {
        threads: 1,
        pairs: DEFAULT_PAIRS,
        work_iters: 0,
        candidate: None,
        reverse: false,
        mixed_every: 0,
        large_bytes: 16 * QUANTUM,
        latencies_file: None,
        release_latencies_file: None,
        calibrate: false,
    };
    let mut input = std::env::args().skip(1);
    while let Some(flag) = input.next() {
        match flag.as_str() {
            "--threads" => args.threads = parse_next(&mut input, "threads"),
            "--pairs" => args.pairs = parse_next(&mut input, "pairs"),
            "--work-iters" => args.work_iters = parse_next(&mut input, "work-iters"),
            "--candidate" => {
                args.candidate = Some(Candidate::parse(&input.next().expect("candidate")));
            }
            "--reverse" => args.reverse = true,
            "--mixed-every" => args.mixed_every = parse_next(&mut input, "mixed-every"),
            "--large-bytes" => args.large_bytes = parse_next(&mut input, "large-bytes"),
            "--latencies-file" => {
                args.latencies_file = Some(PathBuf::from(input.next().expect("latencies-file")));
            }
            "--release-latencies-file" => {
                args.release_latencies_file =
                    Some(PathBuf::from(input.next().expect("release-latencies-file")));
            }
            "--calibrate" => args.calibrate = true,
            "--bench" => {}
            "--help" => {
                println!(
                    "reservation_cost [--calibrate] [--threads N] [--pairs N] \
                     [--work-iters N] [--candidate reservation|protocol|mutex|none] \
                     [--mixed-every N --large-bytes N] [--latencies-file PATH] \
                     [--release-latencies-file PATH] [--reverse]"
                );
                std::process::exit(0);
            }
            _ => panic!("unknown argument: {flag}"),
        }
    }
    args
}

fn parse_next<T: std::str::FromStr>(input: &mut impl Iterator<Item = String>, label: &str) -> T {
    input
        .next()
        .unwrap_or_else(|| panic!("missing {label}"))
        .parse()
        .unwrap_or_else(|_| panic!("invalid {label}"))
}

fn run(candidate: Candidate, args: &Args) {
    let threads = args.threads;
    let pairs = args.pairs;
    let capacity = if args.mixed_every > 0 {
        args.large_bytes
            .checked_mul(threads as u64)
            .and_then(|bytes| bytes.checked_mul(4))
            .expect("mixed capacity overflow")
    } else {
        8 * QUANTUM
    };
    let process_bound = capacity.checked_mul(2).expect("process bound overflow");
    let mut config = AuthorityConfig::new(process_bound, capacity, capacity);
    config.top_up = TopUpPolicy::uniform(QUANTUM);
    let authority = MemoryAuthority::new(config).unwrap();
    let sponsor = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .unwrap();
    let leaf = Arc::new(Reservation::new(&sponsor, ExternalRef::from_u128(2)).unwrap());
    drop(leaf.try_grow(QUANTUM).unwrap());
    let metrics_before = leaf.metrics();
    let plain = Arc::new(PlainMutexLeaf::new(if args.mixed_every > 0 {
        capacity
    } else {
        QUANTUM
    }));
    let protocol = Arc::new(AtomicProtocolLeaf::new());
    let start = Arc::new(Barrier::new(threads + 1));
    let mut joins = Vec::with_capacity(threads);
    for thread_index in 0..threads {
        let leaf = Arc::clone(&leaf);
        let plain = Arc::clone(&plain);
        let protocol = Arc::clone(&protocol);
        let start = Arc::clone(&start);
        let work_iters = args.work_iters;
        let mixed_every = args.mixed_every;
        let large_bytes = args.large_bytes;
        joins.push(std::thread::spawn(move || {
            let mut latencies = Vec::with_capacity(pairs);
            let mut release_latencies = Vec::new();
            let mut checksum = thread_index as u64 + 1;
            let mut denied = 0usize;
            start.wait();
            for pair in 0..pairs {
                let delta = if mixed_every > 0 && (pair + 1) % mixed_every == 0 {
                    large_bytes
                } else {
                    DELTA
                };
                let begin = Instant::now();
                match candidate {
                    Candidate::Reservation => match leaf.try_grow(black_box(delta)) {
                        Ok(lease) => {
                            let release_began = Instant::now();
                            drop(lease);
                            if delta > QUANTUM {
                                release_latencies.push(release_began.elapsed().as_nanos() as u64);
                            }
                            if delta > QUANTUM {
                                // Force a real parent return after each large
                                // request so the next one exercises refill.
                                leaf.trim();
                            }
                        }
                        Err(_) if mixed_every > 0 => denied += 1,
                        Err(error) => panic!("unexpected reservation refusal: {error}"),
                    },
                    Candidate::Mutex => {
                        plain.grow(black_box(delta));
                        let release_began = Instant::now();
                        plain.shrink(black_box(delta));
                        if delta > QUANTUM {
                            release_latencies.push(release_began.elapsed().as_nanos() as u64);
                        }
                    }
                    Candidate::Protocol => {
                        protocol.grow(black_box(delta));
                        protocol.shrink(black_box(delta));
                    }
                    Candidate::None => {
                        black_box(delta);
                    }
                }
                latencies.push(begin.elapsed().as_nanos() as u64);
                checksum = burn_work(checksum ^ pair as u64, work_iters);
            }
            (latencies, release_latencies, checksum, denied)
        }));
    }
    let alloc_before = ALLOCATOR.snapshot();
    let began = Instant::now();
    start.wait();
    let mut latencies = Vec::with_capacity(threads * pairs);
    let mut release_latencies = Vec::new();
    let mut checksum = 0u64;
    let mut denied = 0usize;
    for join in joins {
        let (thread_latencies, thread_releases, thread_checksum, thread_denied) =
            join.join().unwrap();
        latencies.extend(thread_latencies);
        release_latencies.extend(thread_releases);
        checksum = checksum.wrapping_add(thread_checksum);
        denied += thread_denied;
    }
    let elapsed = began.elapsed();
    let alloc_after = ALLOCATOR.snapshot();
    let metrics_after = leaf.metrics();
    if let Some(path) = &args.latencies_file {
        let mut writer = BufWriter::new(File::create(path).expect("create latency file"));
        writeln!(writer, "pair,latency_ns").expect("write latency header");
        for (pair, latency) in latencies.iter().enumerate() {
            writeln!(writer, "{pair},{latency}").expect("write latency file");
        }
        writer.flush().expect("flush latency file");
    }
    if let Some(path) = &args.release_latencies_file {
        let mut writer = BufWriter::new(File::create(path).expect("create release latency file"));
        writeln!(writer, "release,latency_ns").expect("write release latency header");
        for (release, latency) in release_latencies.iter().enumerate() {
            writeln!(writer, "{release},{latency}").expect("write release latency file");
        }
        writer.flush().expect("flush release latency file");
    }
    latencies.sort_unstable();
    release_latencies.sort_unstable();
    let percentile =
        |fraction: f64| -> u64 { latencies[((latencies.len() - 1) as f64 * fraction) as usize] };
    let release_percentile = |fraction: f64| -> u64 {
        if release_latencies.is_empty() {
            0
        } else {
            release_latencies[((release_latencies.len() - 1) as f64 * fraction) as usize]
        }
    };
    let logical_threads = std::thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(0);
    let mode = if args.mixed_every > 0 {
        "mixed"
    } else if args.work_iters == 0 {
        "saturated"
    } else {
        "supply"
    };
    println!(
        "kind={} mode={mode} generator_version={GENERATOR_VERSION} code_sha={} code_dirty={} os={} kernel={} arch={} machine={} rustc={} allocator=System logical_threads={} threads={threads} pairs_per_thread={pairs} work_iters={} mixed_every={} large_bytes={} completed_pairs={} denied_pairs={denied} elapsed_ns={} pairs_per_s={:.0} median_ns={} p90_ns={} p99_ns={} p999_ns={} large_release_count={} large_release_median_ns={} large_release_p99_ns={} large_release_p999_ns={} allocation_calls={} allocated_bytes={} deallocated_bytes={} live_bytes={} free_cas_retries={} parent_top_up_calls={} parent_return_calls={} checksum={checksum} latencies_file={} release_latencies_file={}",
        candidate.name(),
        command_output("git", &["rev-parse", "HEAD"]),
        code_dirty(),
        std::env::consts::OS,
        command_output("uname", &["-sr"]),
        std::env::consts::ARCH,
        command_output("hostname", &[]),
        command_output("rustc", &["--version"]),
        logical_threads,
        args.work_iters,
        args.mixed_every,
        args.large_bytes,
        latencies.len(),
        elapsed.as_nanos(),
        (latencies.len() - denied) as f64 / elapsed.as_secs_f64(),
        percentile(0.5),
        percentile(0.9),
        percentile(0.99),
        percentile(0.999),
        release_latencies.len(),
        release_percentile(0.5),
        release_percentile(0.99),
        release_percentile(0.999),
        alloc_after
            .allocations
            .saturating_sub(alloc_before.allocations),
        alloc_after
            .allocated_total_bytes
            .saturating_sub(alloc_before.allocated_total_bytes),
        alloc_after
            .deallocated_total_bytes
            .saturating_sub(alloc_before.deallocated_total_bytes),
        leaf.snapshot().live_bytes,
        metrics_after
            .free_cas_retries
            .saturating_sub(metrics_before.free_cas_retries),
        metrics_after
            .parent_top_up_calls
            .saturating_sub(metrics_before.parent_top_up_calls),
        metrics_after
            .parent_return_calls
            .saturating_sub(metrics_before.parent_return_calls),
        args.latencies_file
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "none".to_string()),
        args.release_latencies_file
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "none".to_string()),
    );
}

fn command_output(program: &str, arguments: &[&str]) -> String {
    Command::new(program)
        .args(arguments)
        .output()
        .ok()
        .filter(|result| result.status.success())
        .map(|result| {
            String::from_utf8_lossy(&result.stdout)
                .trim()
                .replace(' ', "_")
        })
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unavailable".to_string())
}

fn code_dirty() -> bool {
    !Command::new("git")
        .args(["diff", "--quiet", "HEAD", "--"])
        .status()
        .is_ok_and(|status| status.success())
}

#[inline(never)]
fn burn_work(mut state: u64, iterations: usize) -> u64 {
    for _ in 0..iterations {
        state = black_box(state)
            .wrapping_mul(WORK_MULTIPLIER)
            .wrapping_add(WORK_INCREMENT);
    }
    black_box(state)
}

fn measure_work(iterations: usize) -> (u64, u64) {
    const ROUNDS: usize = 2048;
    let mut checksum = 1u64;
    let started = Instant::now();
    for pair in 0..ROUNDS {
        checksum = burn_work(checksum ^ pair as u64, iterations);
    }
    (
        (started.elapsed().as_nanos() / ROUNDS as u128) as u64,
        checksum,
    )
}

fn calibrate_work() {
    for target_us in [1u64, 5, 20] {
        let target_ns = target_us * 1000;
        let mut upper = 1usize;
        while measure_work(upper).0 < target_ns {
            upper = upper.checked_mul(2).expect("work calibration overflow");
        }
        let mut lower = upper / 2;
        while lower + 1 < upper {
            let middle = lower + (upper - lower) / 2;
            if measure_work(middle).0 < target_ns {
                lower = middle;
            } else {
                upper = middle;
            }
        }
        let (observed_ns, checksum) = measure_work(upper);
        println!(
            "calibration_target_us={target_us} work_iters={upper} observed_ns={observed_ns} checksum={checksum}"
        );
    }
}
