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
use std::future::Future;
use std::sync::{Arc, OnceLock};

use tokio::runtime::{Handle, Runtime};

use crate::common::config::{data_runtime_max_blocking_threads, data_runtime_worker_threads};
use crate::novarocks_logging::info;

const DATA_RUNTIME_THREAD_NAME: &str = "novarocks-data-runtime";
static DATA_RUNTIME: OnceLock<Result<Arc<Runtime>, String>> = OnceLock::new();

pub fn data_runtime() -> Result<&'static Arc<Runtime>, String> {
    match DATA_RUNTIME.get_or_init(|| {
        let worker_threads = data_runtime_worker_threads().max(1);
        let max_blocking_threads = data_runtime_max_blocking_threads().max(1);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(worker_threads)
            .max_blocking_threads(max_blocking_threads)
            .thread_name(DATA_RUNTIME_THREAD_NAME)
            .build()
            .map_err(|e| format!("init data tokio runtime failed: {e}"))?;
        info!(
            worker_threads,
            max_blocking_threads,
            thread_name = DATA_RUNTIME_THREAD_NAME,
            "global data runtime initialized"
        );
        Ok(Arc::new(runtime))
    }) {
        Ok(runtime) => Ok(runtime),
        Err(err) => Err(err.clone()),
    }
}

pub fn data_runtime_handle() -> Result<Handle, String> {
    let runtime = data_runtime()?;
    Ok(runtime.handle().clone())
}

pub fn data_block_on<F>(future: F) -> Result<F::Output, String>
where
    F: Future,
{
    let runtime = data_runtime()?;
    if Handle::try_current().is_ok() {
        // Path A: Caller is on a thread that has a Tokio runtime handle (e.g. a
        // `task::spawn_blocking` closure inside the standalone server's request
        // handler).  `block_in_place` detects we are on a blocking thread (NOT a
        // worker thread) and simply runs the closure directly without suspending
        // any async task.  Using it instead of `Handle::block_on` lets us call
        // `runtime.block_on` on the *separate* data runtime safely.
        //
        // True async task callers (poll-context worker threads) must NOT reach this
        // path — `block_in_place` from a worker would still allow runtime.block_on
        // on the same runtime to deadlock. We rely on the convention that all DDL
        // is dispatched through `task::spawn_blocking`.
        return Ok(tokio::task::block_in_place(|| runtime.block_on(future)));
    }
    Ok(runtime.block_on(future))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn data_runtime_is_singleton_across_threads() {
        let expected_ptr = Arc::as_ptr(data_runtime().expect("get data runtime")) as usize;
        let handles = (0..16)
            .map(|_| {
                thread::spawn(move || {
                    for _ in 0..64 {
                        let ptr = Arc::as_ptr(data_runtime().expect("get data runtime")) as usize;
                        assert_eq!(ptr, expected_ptr);
                    }
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            handle.join().expect("join runtime singleton checker");
        }
    }

    #[test]
    fn data_block_on_runs_outside_runtime() {
        let value = data_block_on(async { 7_i32 }).expect("run with data runtime");
        assert_eq!(value, 7);
    }

    #[test]
    fn data_block_on_via_block_in_place_from_runtime_context() {
        // Exercises the `Handle::try_current().is_ok()` branch — specifically the
        // case where `runtime.block_on` drives the outer async context, which
        // `block_in_place` treats as a blocking thread regardless of whether the
        // call site is a literal `spawn_blocking` closure or a `block_on` call.
        // In production DDL all paths go through `task::spawn_blocking`, but the
        // behaviour of `block_in_place` is identical in both cases.
        let runtime = data_runtime().expect("get data runtime");
        let value = runtime.block_on(async {
            data_block_on(async { 1_u8 }).expect("block_in_place succeeds from runtime context")
        });
        assert_eq!(value, 1_u8);
    }
}
