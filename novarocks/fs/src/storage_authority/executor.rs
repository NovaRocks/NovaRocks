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

//! Where a credential refresh actually runs.
//!
//! CAD-1 D3 does not merely permit prefetch, it requires that a refresh never
//! occupy the thread that asked for material. The reason is specific to this
//! engine: filesystem reads are driven synchronously from scan threads through
//! `block_on`, and vended credentials across a cluster commonly expire at the
//! same moment. A refresh that borrowed its caller's thread could therefore
//! park a whole scan pool at once, turning one slow catalog into a stalled
//! cluster.
//!
//! `spawn_blocking` is the right primitive rather than `spawn`: an acquisition
//! is a synchronous provider call, so putting it on an async worker would move
//! the stall rather than remove it.

use std::sync::Arc;

use tokio::runtime::Handle;

use super::RefreshExecutor;

/// Runs refreshes on the composition-owned runtime's blocking pool.
#[derive(Clone)]
pub struct TokioRefreshExecutor {
    handle: Handle,
}

impl TokioRefreshExecutor {
    pub fn new(handle: Handle) -> Self {
        Self { handle }
    }

    pub fn shared(handle: Handle) -> Arc<dyn RefreshExecutor> {
        Arc::new(Self::new(handle))
    }
}

impl std::fmt::Debug for TokioRefreshExecutor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TokioRefreshExecutor")
            .finish_non_exhaustive()
    }
}

impl RefreshExecutor for TokioRefreshExecutor {
    #[expect(
        clippy::disallowed_methods,
        reason = "Credential renewal belongs to the shared storage-authority service; see ADR-0218."
    )]
    fn execute(&self, job: Box<dyn FnOnce() + Send + 'static>) {
        // The handle is injected, never discovered: this crate does not reach
        // for a current runtime on a connector's behalf.
        self.handle.spawn_blocking(job);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    #[test]
    fn a_refresh_does_not_run_on_the_calling_thread() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("multi-thread runtime");
        let executor = TokioRefreshExecutor::new(runtime.handle().clone());

        let caller = std::thread::current().id();
        let (sender, receiver) = std::sync::mpsc::channel();
        let ran_elsewhere = Arc::new(AtomicBool::new(false));
        let observed = Arc::clone(&ran_elsewhere);

        executor.execute(Box::new(move || {
            observed.store(std::thread::current().id() != caller, Ordering::SeqCst);
            sender.send(()).expect("refresh completion");
        }));

        receiver
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("the refresh must complete");
        assert!(
            ran_elsewhere.load(Ordering::SeqCst),
            "a refresh must never borrow the thread that asked for material"
        );
    }
}
