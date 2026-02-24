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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::Duration;

use threadpool::ThreadPool;

use crate::common::app_config;
use crate::common::config::exchange_io_threads;

pub struct IoTaskContext {
    cancelled: Arc<AtomicBool>,
}

impl IoTaskContext {
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

struct IoTaskCompletion {
    done: AtomicBool,
    cv: Condvar,
    mu: Mutex<()>,
}

impl IoTaskCompletion {
    fn new() -> Self {
        Self {
            done: AtomicBool::new(false),
            cv: Condvar::new(),
            mu: Mutex::new(()),
        }
    }

    fn mark_done(&self) {
        self.done.store(true, Ordering::Release);
        self.cv.notify_all();
    }

    fn wait(&self) {
        if self.done.load(Ordering::Acquire) {
            return;
        }
        let guard = self.mu.lock().expect("io task completion lock");
        let _guard = self
            .cv
            .wait_while(guard, |_| !self.done.load(Ordering::Acquire))
            .expect("io task completion wait");
    }

    fn wait_timeout(&self, timeout: Duration) -> bool {
        if self.done.load(Ordering::Acquire) {
            return true;
        }
        let guard = self.mu.lock().expect("io task completion lock");
        let (guard, _) = self
            .cv
            .wait_timeout_while(guard, timeout, |_| !self.done.load(Ordering::Acquire))
            .expect("io task completion wait");
        drop(guard);
        self.done.load(Ordering::Acquire)
    }
}

#[derive(Clone)]
pub struct IoTaskHandle {
    cancelled: Arc<AtomicBool>,
    completion: Arc<IoTaskCompletion>,
}

impl IoTaskHandle {
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.completion.cv.notify_all();
    }

    pub fn is_finished(&self) -> bool {
        self.completion.done.load(Ordering::Acquire)
    }

    pub fn wait(&self) {
        self.completion.wait();
    }

    pub fn wait_timeout(&self, timeout: Duration) -> bool {
        self.completion.wait_timeout(timeout)
    }
}

pub struct IoExecutor {
    pool: ThreadPool,
}

impl IoExecutor {
    fn new(num_threads: usize) -> Self {
        let threads = num_threads.max(1);
        let pool = ThreadPool::with_name("io_task".to_string(), threads);
        Self { pool }
    }

    pub fn submit<F>(&self, task: F) -> IoTaskHandle
    where
        F: FnOnce(IoTaskContext) + Send + 'static,
    {
        let cancelled = Arc::new(AtomicBool::new(false));
        let completion = Arc::new(IoTaskCompletion::new());
        let ctx = IoTaskContext {
            cancelled: Arc::clone(&cancelled),
        };
        let completion_clone = Arc::clone(&completion);
        let task_cell: Arc<Mutex<Option<Box<dyn FnOnce() + Send + 'static>>>> =
            Arc::new(Mutex::new(Some(Box::new(move || {
                task(ctx);
                completion_clone.mark_done();
            }))));

        // Avoid hidden thread spawning; queue all tasks in the executor.
        let runner = make_runner(task_cell);
        self.pool.execute(runner);
        IoTaskHandle {
            cancelled,
            completion,
        }
    }
}

fn make_runner(
    task_cell: Arc<Mutex<Option<Box<dyn FnOnce() + Send + 'static>>>>,
) -> impl FnOnce() + Send + 'static {
    move || {
        let task = {
            let mut guard = task_cell.lock().expect("io task cell lock");
            guard.take()
        };
        if let Some(task) = task {
            task();
        }
    }
}

static IO_EXECUTOR: OnceLock<IoExecutor> = OnceLock::new();

pub fn io_executor() -> &'static IoExecutor {
    IO_EXECUTOR.get_or_init(|| {
        let threads = exchange_io_threads().max(
            app_config::config()
                .ok()
                .map(|c| c.runtime.actual_exec_threads())
                .unwrap_or_else(|| {
                    std::thread::available_parallelism()
                        .map(|n| n.get())
                        .unwrap_or(1)
                }),
        );
        IoExecutor::new(threads)
    })
}
