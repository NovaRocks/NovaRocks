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

use std::{any::Any, num::NonZeroUsize, time::Instant};

use crate::coordination::{
    BoundedResultDecodeHandle, BoundedResultDecodeOwner, ResultDecodeExecutorConfig,
    ResultDecodeJob,
};

type CpuResult = Box<dyn Any + Send>;

/// Fixed process limits for CPU-bound query preparation work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryCpuExecutorConfig {
    worker_threads: NonZeroUsize,
    queue_capacity: NonZeroUsize,
}

impl QueryCpuExecutorConfig {
    pub const fn new(worker_threads: NonZeroUsize, queue_capacity: NonZeroUsize) -> Self {
        Self {
            worker_threads,
            queue_capacity,
        }
    }

    pub const fn worker_threads(self) -> NonZeroUsize {
        self.worker_threads
    }

    pub const fn queue_capacity(self) -> NonZeroUsize {
        self.queue_capacity
    }
}

/// The Frontend process holds this unique owner for its entire lifetime.
///
/// Query sessions receive only [`QueryCpuExecutor`], so closing or dropping a
/// session cannot close the CPU queue or join its fixed workers.
pub struct QueryCpuExecutorOwner {
    owner: BoundedResultDecodeOwner<CpuResult>,
    executor: QueryCpuExecutor,
}

#[derive(Clone)]
pub struct QueryCpuExecutor {
    handle: BoundedResultDecodeHandle<CpuResult>,
}

/// Fixed process limits for synchronous query command edges.
///
/// This is deliberately separate from [`QueryCpuExecutorConfig`]: command
/// routes may call providers or wait on durable catalog work, so charging them
/// to compiler CPU capacity would let those waits starve pure preparation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryBlockingExecutorConfig {
    worker_threads: NonZeroUsize,
    queue_capacity: NonZeroUsize,
}

impl QueryBlockingExecutorConfig {
    pub const fn new(worker_threads: NonZeroUsize, queue_capacity: NonZeroUsize) -> Self {
        Self {
            worker_threads,
            queue_capacity,
        }
    }

    pub const fn worker_threads(self) -> NonZeroUsize {
        self.worker_threads
    }

    pub const fn queue_capacity(self) -> NonZeroUsize {
        self.queue_capacity
    }
}

/// The process owner for bounded synchronous command edges that have not yet
/// acquired an asynchronous provider contract.
pub struct QueryBlockingExecutorOwner {
    owner: BoundedResultDecodeOwner<CpuResult>,
    executor: QueryBlockingExecutor,
}

#[derive(Clone)]
pub struct QueryBlockingExecutor {
    handle: BoundedResultDecodeHandle<CpuResult>,
}

impl QueryCpuExecutorOwner {
    pub fn try_new(config: QueryCpuExecutorConfig) -> Result<Self, String> {
        let owner = BoundedResultDecodeOwner::try_new(ResultDecodeExecutorConfig::new(
            config.worker_threads(),
            config.queue_capacity(),
        ))
        .map_err(|error| format!("open query CPU executor: {error}"))?;
        let executor = QueryCpuExecutor {
            handle: owner.handle(),
        };
        Ok(Self { owner, executor })
    }

    pub fn executor(&self) -> QueryCpuExecutor {
        self.executor.clone()
    }

    /// Stops new CPU work and waits for this exact fixed worker set.
    pub async fn shutdown_until(&mut self, deadline: Instant) -> Result<(), String> {
        self.owner
            .shutdown_until(deadline)
            .await
            .map_err(|error| format!("shut down query CPU executor: {error}"))
    }

    /// Closes CPU admission when the role has committed to process exit.
    pub fn request_shutdown_for_process_exit(&self) {
        self.owner.request_close();
    }
}

impl QueryCpuExecutor {
    pub async fn run<T, F>(&self, work: F) -> Result<T, String>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        let receipt = self
            .handle
            .submit(ResultDecodeJob::new(move || Box::new(work()) as CpuResult))
            .await
            .map_err(|_| "query CPU executor closed before admitting work".to_owned())?;
        receipt
            .complete()
            .await
            .map_err(|error| format!("query CPU worker failed: {error}"))?
            .downcast::<T>()
            .map(|result| *result)
            .map_err(|_| "query CPU worker returned an invalid result type".to_owned())
    }
}

impl QueryBlockingExecutorOwner {
    pub fn try_new(config: QueryBlockingExecutorConfig) -> Result<Self, String> {
        let owner = BoundedResultDecodeOwner::try_new(ResultDecodeExecutorConfig::new(
            config.worker_threads(),
            config.queue_capacity(),
        ))
        .map_err(|error| format!("open query blocking executor: {error}"))?;
        let executor = QueryBlockingExecutor {
            handle: owner.handle(),
        };
        Ok(Self { owner, executor })
    }

    pub fn executor(&self) -> QueryBlockingExecutor {
        self.executor.clone()
    }

    /// Stops admission and joins the same fixed worker set.
    pub async fn shutdown_until(&mut self, deadline: Instant) -> Result<(), String> {
        self.owner
            .shutdown_until(deadline)
            .await
            .map_err(|error| format!("shut down query blocking executor: {error}"))
    }

    /// Only process teardown may close this executor without waiting.
    pub fn request_shutdown_for_process_exit(&self) {
        self.owner.request_close();
    }
}

impl QueryBlockingExecutor {
    /// Runs one already-classified command edge after bounded admission.
    ///
    /// This is intentionally not a generic statement runner: callers must
    /// keep parser admission, CPU compilation, protocol delivery, and every
    /// adapter-owned synchronous effect in separate closures.
    pub async fn execute<T, F>(&self, work: F) -> Result<T, String>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        let receipt = self
            .handle
            .submit(ResultDecodeJob::new(move || Box::new(work()) as CpuResult))
            .await
            .map_err(|_| "query blocking executor closed before admitting work".to_owned())?;
        receipt
            .complete()
            .await
            .map_err(|error| format!("query blocking worker failed: {error}"))?
            .downcast::<T>()
            .map(|result| *result)
            .map_err(|_| "query blocking worker returned an invalid result type".to_owned())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        num::NonZeroUsize,
        sync::{
            Arc, Condvar, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    };

    use super::{
        QueryBlockingExecutorConfig, QueryBlockingExecutorOwner, QueryCpuExecutorConfig,
        QueryCpuExecutorOwner,
    };

    fn executor(workers: usize, queue: usize) -> QueryCpuExecutorOwner {
        QueryCpuExecutorOwner::try_new(QueryCpuExecutorConfig::new(
            NonZeroUsize::new(workers).expect("nonzero workers"),
            NonZeroUsize::new(queue).expect("nonzero queue"),
        ))
        .expect("open CPU executor")
    }

    #[tokio::test]
    async fn fixed_cpu_workers_bound_concurrent_work() {
        let owner = executor(2, 2);
        let executor = owner.executor();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let mut work = Vec::new();
        for value in 0..4 {
            let gate = Arc::clone(&gate);
            let active = Arc::clone(&active);
            let peak = Arc::clone(&peak);
            let executor = executor.clone();
            work.push(tokio::spawn(async move {
                executor
                    .run(move || {
                        let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(current, Ordering::SeqCst);
                        let (lock, ready) = &*gate;
                        let mut open = lock.lock().expect("gate lock");
                        while !*open {
                            open = ready.wait(open).expect("gate wait");
                        }
                        active.fetch_sub(1, Ordering::SeqCst);
                        value
                    })
                    .await
            }));
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while active.load(Ordering::SeqCst) != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("two fixed CPU workers started");
        let (lock, ready) = &*gate;
        *lock.lock().expect("gate lock") = true;
        ready.notify_all();
        for (expected, work) in work.into_iter().enumerate() {
            assert_eq!(work.await.expect("join").expect("run"), expected);
        }
        assert_eq!(peak.load(Ordering::SeqCst), 2);
        drop(executor);
        let mut owner = owner;
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("shut down CPU workers");
    }

    #[tokio::test]
    async fn fixed_blocking_workers_bound_concurrent_work() {
        let owner = QueryBlockingExecutorOwner::try_new(QueryBlockingExecutorConfig::new(
            NonZeroUsize::new(1).expect("nonzero workers"),
            NonZeroUsize::new(2).expect("nonzero queue"),
        ))
        .expect("open blocking executor");
        let executor = owner.executor();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let mut work = Vec::new();
        for value in 0..2 {
            let gate = Arc::clone(&gate);
            let active = Arc::clone(&active);
            let peak = Arc::clone(&peak);
            let executor = executor.clone();
            work.push(tokio::spawn(async move {
                executor
                    .execute(move || {
                        let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(current, Ordering::SeqCst);
                        let (lock, ready) = &*gate;
                        let mut open = lock.lock().expect("gate lock");
                        while !*open {
                            open = ready.wait(open).expect("gate wait");
                        }
                        active.fetch_sub(1, Ordering::SeqCst);
                        value
                    })
                    .await
            }));
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while active.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("one fixed blocking worker started");
        let (lock, ready) = &*gate;
        *lock.lock().expect("gate lock") = true;
        ready.notify_all();
        for (expected, work) in work.into_iter().enumerate() {
            assert_eq!(work.await.expect("join").expect("run"), expected);
        }
        assert_eq!(peak.load(Ordering::SeqCst), 1);
        drop(executor);
        let mut owner = owner;
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("shut down blocking workers");
    }
}
