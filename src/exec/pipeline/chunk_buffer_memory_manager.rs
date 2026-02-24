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
//! Chunk-buffer memory budget controller.
//!
//! Responsibilities:
//! - Tracks buffered chunk memory usage across pipeline exchange/queue components.
//! - Provides threshold-based admission and wake-up decisions for backpressure control.
//!
//! Key exported interfaces:
//! - Types: `ChunkBufferMemoryManager`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use crate::novarocks_logging::debug;

/// Memory-budget controller for buffered chunks in pipeline exchange queues.
pub struct ChunkBufferMemoryManager {
    max_memory_usage: AtomicI64,
    max_memory_usage_per_driver: i64,
    max_buffered_rows: AtomicI64,
    memory_usage: AtomicI64,
    buffered_num_rows: AtomicI64,
    peak_memory_usage: AtomicI64,
    peak_num_rows: AtomicI64,
    max_input_dop: usize,
    full_events_changed: AtomicBool,
}

impl ChunkBufferMemoryManager {
    pub fn new(max_input_dop: usize, per_driver_mem_limit: i64, max_buffered_rows: i64) -> Self {
        let per_driver = per_driver_mem_limit.max(1);
        let max_memory_usage = per_driver.saturating_mul(max_input_dop as i64);
        if per_driver_mem_limit <= 0 {
            debug!(
                "ChunkBufferMemoryManager: invalid per_driver_mem_limit={}, clamped to 1",
                per_driver_mem_limit
            );
        }
        Self {
            max_memory_usage: AtomicI64::new(max_memory_usage),
            max_memory_usage_per_driver: per_driver,
            max_buffered_rows: AtomicI64::new(max_buffered_rows.max(1)),
            memory_usage: AtomicI64::new(0),
            buffered_num_rows: AtomicI64::new(0),
            peak_memory_usage: AtomicI64::new(0),
            peak_num_rows: AtomicI64::new(0),
            max_input_dop,
            full_events_changed: AtomicBool::new(false),
        }
    }

    pub fn update_memory_usage(&self, memory_delta: i64, row_delta: i64) {
        let prev_full = self.is_full();
        let prev_mem = self.memory_usage.fetch_add(memory_delta, Ordering::Relaxed);
        let prev_rows = self
            .buffered_num_rows
            .fetch_add(row_delta, Ordering::Relaxed);
        let cur_mem = prev_mem.saturating_add(memory_delta);
        let cur_rows = prev_rows.saturating_add(row_delta);
        self.update_peaks(cur_mem, cur_rows);
        let is_full = cur_mem >= self.max_memory_usage.load(Ordering::Relaxed)
            || cur_rows > self.max_buffered_rows.load(Ordering::Relaxed);
        if prev_full != is_full {
            self.full_events_changed.store(true, Ordering::Release);
        }
    }

    pub fn get_memory_limit_per_driver(&self) -> i64 {
        self.max_memory_usage_per_driver
    }

    pub fn get_memory_usage(&self) -> i64 {
        self.memory_usage.load(Ordering::Relaxed)
    }

    pub fn get_peak_memory_usage(&self) -> i64 {
        self.peak_memory_usage.load(Ordering::Relaxed)
    }

    pub fn get_peak_num_rows(&self) -> i64 {
        self.peak_num_rows.load(Ordering::Relaxed)
    }

    pub fn is_full(&self) -> bool {
        let mem = self.memory_usage.load(Ordering::Relaxed);
        let rows = self.buffered_num_rows.load(Ordering::Relaxed);
        mem >= self.max_memory_usage.load(Ordering::Relaxed)
            || rows > self.max_buffered_rows.load(Ordering::Relaxed)
    }

    pub fn is_half_full(&self) -> bool {
        let mem = self.memory_usage.load(Ordering::Relaxed);
        mem.saturating_mul(2) >= self.max_memory_usage.load(Ordering::Relaxed)
    }

    pub fn get_max_input_dop(&self) -> usize {
        self.max_input_dop
    }

    pub fn update_max_memory_usage(&self, max_memory_usage: i64) {
        self.max_memory_usage
            .store(max_memory_usage.max(1), Ordering::Relaxed);
    }

    pub fn update_max_buffered_rows(&self, max_buffered_rows: i64) {
        self.max_buffered_rows
            .store(max_buffered_rows.max(1), Ordering::Relaxed);
    }

    pub fn clear(&self) {
        self.memory_usage.store(0, Ordering::Relaxed);
        self.buffered_num_rows.store(0, Ordering::Relaxed);
    }

    pub fn full_events_changed(&self) -> bool {
        if !self.full_events_changed.load(Ordering::Acquire) {
            return false;
        }
        self.full_events_changed
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }

    fn update_peaks(&self, memory_usage: i64, num_rows: i64) {
        let mem = memory_usage.max(0);
        let rows = num_rows.max(0);
        let mut prev = self.peak_memory_usage.load(Ordering::Relaxed);
        while mem > prev {
            match self.peak_memory_usage.compare_exchange(
                prev,
                mem,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => prev = actual,
            }
        }
        let mut prev_rows = self.peak_num_rows.load(Ordering::Relaxed);
        while rows > prev_rows {
            match self.peak_num_rows.compare_exchange(
                prev_rows,
                rows,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => prev_rows = actual,
            }
        }
    }
}
