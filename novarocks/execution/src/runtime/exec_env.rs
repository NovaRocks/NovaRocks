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
//! Utilities for deriving execution parameters from request values.
//!
//! This mirrors StarRocks BE `ExecEnv` helpers like `calc_pipeline_dop()`.

/// Calculate pipeline degree-of-parallelism (DOP) for a fragment instance.
///
/// StarRocks behavior:
/// - If `pipeline_dop > 0`, use it as-is (from FE session variable).
/// - Otherwise, default to half of configured executor threads.
pub fn calc_pipeline_dop(pipeline_dop: i32) -> i32 {
    if pipeline_dop > 0 {
        return pipeline_dop;
    }

    // Auto mode: use half of configured executor threads
    ((auto_exec_threads() / 2).max(1)) as i32
}

/// Pipeline DOP for fragments whose output is a **write/data sink** (load / insert / export).
///
/// Mirrors StarRocks `BackendResourceStat::getSinkDefaultDOP`: a positive `pipeline_dop` override
/// (from `SET pipeline_dop = N`) wins; otherwise a deliberately lower curve than the compute
/// `calc_pipeline_dop` — `cores/3` for <=24 cores, `min(32, cores/4)` above — because a write
/// fragment runs both query and storage engine work, so it must not use up CPU, and write
/// throughput/concurrency scales sub-linearly while memory grows linearly.
pub fn calc_sink_pipeline_dop(pipeline_dop: i32) -> i32 {
    if pipeline_dop > 0 {
        return pipeline_dop;
    }
    let cores = auto_exec_threads();
    if cores <= 24 {
        ((cores / 3).max(1)) as i32
    } else {
        ((cores / 4).min(32)) as i32
    }
}

/// Configured executor-thread count (defaults to available CPU cores), shared by the DOP curves.
fn auto_exec_threads() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}
