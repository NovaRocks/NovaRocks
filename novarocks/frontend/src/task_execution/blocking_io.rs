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

//! Process-wide admission for blocking Connector calls made by the frontend.
//!
//! Connector implementations may expose synchronous split enumeration and
//! credential vending. Those calls run on Tokio's process blocking pool, but
//! the pool's thread cap is not admission: unrelated blocking work can occupy
//! it, and an unbounded number of query attempts can still queue behind it.
//! This owner adds a smaller explicit bound and, within the Connector calls it
//! supervises, keeps protected capacity for credential and lifecycle progress
//! when ordinary split work is saturated. It does not isolate those calls from
//! unrelated users of Tokio's shared blocking pool.

use std::fmt;
use std::sync::{Arc, Mutex};

use tokio::runtime::Handle;
use tokio::sync::Semaphore;

/// The process bounds for frontend Connector blocking calls.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectorBlockingIoBudget {
    total: usize,
    ordinary: usize,
}

impl ConnectorBlockingIoBudget {
    /// Builds a budget with at least one slot reserved for protected progress.
    pub fn try_new(total: usize, ordinary: usize) -> Result<Self, String> {
        if total == 0 {
            return Err("connector blocking-I/O total permits must be nonzero".to_owned());
        }
        if ordinary == 0 {
            return Err("connector blocking-I/O ordinary permits must be nonzero".to_owned());
        }
        if ordinary >= total {
            return Err(
                "connector blocking-I/O ordinary permits must leave protected capacity".to_owned(),
            );
        }
        Ok(Self { total, ordinary })
    }

    pub const fn total(self) -> usize {
        self.total
    }

    pub const fn ordinary(self) -> usize {
        self.ordinary
    }
}

impl Default for ConnectorBlockingIoBudget {
    fn default() -> Self {
        Self::try_new(16, 12).expect("the default Connector blocking-I/O budget is valid")
    }
}

/// Why a submitted blocking call produced no Connector outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ConnectorBlockingIoError {
    detail: String,
}

impl ConnectorBlockingIoError {
    pub(crate) fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for ConnectorBlockingIoError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for ConnectorBlockingIoError {}

/// A submitted call whose result can be polled by an existing serial owner.
pub(crate) struct ConnectorBlockingIoJob<T> {
    outcome: Arc<Mutex<Option<Result<T, ConnectorBlockingIoError>>>>,
}

impl<T> ConnectorBlockingIoJob<T> {
    pub(crate) fn try_take(&self) -> Option<Result<T, ConnectorBlockingIoError>> {
        self.outcome
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
    }
}

/// The one process owner that admits frontend Connector blocking calls.
#[derive(Clone)]
pub(crate) struct ConnectorBlockingIoSupervisor {
    runtime: Handle,
    total: Arc<Semaphore>,
    ordinary: Arc<Semaphore>,
}

impl ConnectorBlockingIoSupervisor {
    pub(crate) fn new(runtime: Handle, budget: ConnectorBlockingIoBudget) -> Self {
        Self {
            runtime,
            total: Arc::new(Semaphore::new(budget.total())),
            ordinary: Arc::new(Semaphore::new(budget.ordinary())),
        }
    }

    /// Submit credential or lifecycle work through the protected lane.
    pub(crate) fn spawn_protected<T, F>(&self, call: F) -> ConnectorBlockingIoJob<T>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        self.spawn(false, call)
    }

    fn spawn<T, F>(&self, ordinary: bool, call: F) -> ConnectorBlockingIoJob<T>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        let outcome = Arc::new(Mutex::new(None));
        let published = Arc::clone(&outcome);
        let total = Arc::clone(&self.total);
        let ordinary_permits = Arc::clone(&self.ordinary);
        self.runtime.spawn(async move {
            let ordinary_permit = if ordinary {
                match ordinary_permits.acquire_owned().await {
                    Ok(permit) => Some(permit),
                    Err(_) => {
                        publish_error(&published, "connector blocking-I/O ordinary lane closed");
                        return;
                    }
                }
            } else {
                None
            };
            let total_permit = match total.acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => {
                    publish_error(&published, "connector blocking-I/O supervisor closed");
                    return;
                }
            };
            let completed = tokio::task::spawn_blocking(move || {
                // Both guards stay in this closure until the synchronous call
                // returns. Cancellation of the async submitter cannot return
                // capacity while Connector code is still running.
                let _ordinary_permit = ordinary_permit;
                let _total_permit = total_permit;
                call()
            })
            .await
            .map_err(|error| ConnectorBlockingIoError {
                detail: format!("connector blocking-I/O worker failed: {error}"),
            });
            *published.lock().unwrap_or_else(|error| error.into_inner()) = Some(completed);
        });
        ConnectorBlockingIoJob { outcome }
    }
}

fn publish_error<T>(slot: &Arc<Mutex<Option<Result<T, ConnectorBlockingIoError>>>>, detail: &str) {
    *slot.lock().unwrap_or_else(|error| error.into_inner()) = Some(Err(ConnectorBlockingIoError {
        detail: detail.to_owned(),
    }));
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    use super::*;

    fn wait<T>(job: &ConnectorBlockingIoJob<T>) -> Result<T, ConnectorBlockingIoError> {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if let Some(outcome) = job.try_take() {
                return outcome;
            }
            assert!(Instant::now() < deadline, "blocking-I/O job did not finish");
            std::thread::yield_now();
        }
    }

    #[test]
    fn budget_requires_total_ordinary_and_protected_capacity() {
        assert!(ConnectorBlockingIoBudget::try_new(0, 0).is_err());
        assert!(ConnectorBlockingIoBudget::try_new(2, 0).is_err());
        assert!(ConnectorBlockingIoBudget::try_new(2, 2).is_err());
        assert_eq!(
            ConnectorBlockingIoBudget::try_new(2, 1).expect("valid budget"),
            ConnectorBlockingIoBudget {
                total: 2,
                ordinary: 1,
            }
        );
    }

    #[test]
    fn protected_work_can_progress_while_ordinary_capacity_is_saturated() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(4)
            .enable_all()
            .build()
            .expect("runtime");
        let supervisor = ConnectorBlockingIoSupervisor::new(
            runtime.handle().clone(),
            ConnectorBlockingIoBudget::try_new(2, 1).expect("budget"),
        );
        let (release, released) = mpsc::channel();
        let (started, first_started) = mpsc::channel();
        let first = supervisor.spawn(true, move || {
            started.send(()).expect("publish first start");
            let _ = released.recv();
        });
        first_started
            .recv_timeout(Duration::from_secs(2))
            .expect("ordinary call did not start");
        let (second_started, observe_second_start) = mpsc::channel();
        let second = supervisor.spawn(true, move || {
            second_started.send(()).expect("publish second start");
        });
        let protected = supervisor.spawn_protected(|| 7_u8);
        assert_eq!(wait(&protected).expect("protected outcome"), 7);
        assert_eq!(
            observe_second_start.try_recv(),
            Err(mpsc::TryRecvError::Empty),
            "the queued ordinary call must not consume protected capacity"
        );

        release.send(()).expect("release first ordinary call");
        wait(&first).expect("first ordinary outcome");
        wait(&second).expect("second ordinary outcome");
        observe_second_start
            .recv_timeout(Duration::from_secs(2))
            .expect("second ordinary call did not start");
    }

    #[test]
    fn permit_is_held_until_the_blocking_call_really_returns() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(4)
            .enable_all()
            .build()
            .expect("runtime");
        let supervisor = ConnectorBlockingIoSupervisor::new(
            runtime.handle().clone(),
            ConnectorBlockingIoBudget::try_new(2, 1).expect("budget"),
        );
        let (release, released) = mpsc::channel();
        let (started, first_started) = mpsc::channel();
        let first = supervisor.spawn(true, move || {
            started.send(()).expect("publish first start");
            let _ = released.recv();
        });
        first_started
            .recv_timeout(Duration::from_secs(2))
            .expect("first call did not start");
        let (second_started, observe_second_start) = mpsc::channel();
        let second = supervisor.spawn(true, move || {
            second_started.send(()).expect("publish second start");
        });
        assert!(second.try_take().is_none());
        assert_eq!(
            observe_second_start.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        );
        release.send(()).expect("release first ordinary call");
        wait(&first).expect("first outcome");
        wait(&second).expect("second outcome");
        observe_second_start
            .recv_timeout(Duration::from_secs(2))
            .expect("second ordinary call did not start");
    }
}
