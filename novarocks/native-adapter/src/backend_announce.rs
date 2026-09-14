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

//! Native transport supervision for one Backend membership advertisement.
//!
//! The Worker owns the one-way drain fact. This adapter only reads that fact
//! while it drives authenticated announcement over the Native process boundary;
//! it does not own membership, task admission, or any Worker lifecycle state.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use novarocks_execution_contract::{BackendProcessDescriptor, BackendReportedState};
use novarocks_proto_codec::membership::{BackendAnnounceRequest, BackendAnnounceResult};
use novarocks_types::NativeEndpoint;
use novarocks_worker::WorkerDrainState;

use crate::{BackendDataRuntime, NativeRpcClient};

const ANNOUNCE_RPC_TIMEOUT: Duration = Duration::from_secs(3);

/// The Native transport supervisor for one immutable Backend process descriptor.
pub struct BackendAnnounceSupervisor {
    stop: Arc<AtomicBool>,
    wake: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
    join: Option<std::thread::JoinHandle<()>>,
    data_runtime: BackendDataRuntime,
    frontend_endpoint: NativeEndpoint,
    descriptor: BackendProcessDescriptor,
}

impl BackendAnnounceSupervisor {
    #[expect(
        clippy::too_many_arguments,
        reason = "The immutable descriptor, Worker drain fact, and retry policy are separate Native boundary facts."
    )]
    pub fn start(
        data_runtime: BackendDataRuntime,
        frontend_endpoint: NativeEndpoint,
        descriptor: BackendProcessDescriptor,
        drain: Arc<WorkerDrainState>,
        interval: Duration,
        initial_backoff: Duration,
        max_backoff: Duration,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let wake = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let thread_stop = Arc::clone(&stop);
        let thread_wake = Arc::clone(&wake);
        let thread_runtime = data_runtime.clone();
        let thread_frontend_endpoint = frontend_endpoint.clone();
        let thread_descriptor = descriptor.clone();
        let join = std::thread::Builder::new()
            .name("backend-announce".to_string())
            .spawn(move || {
                let client =
                    NativeRpcClient::new_native_endpoint(thread_runtime, thread_frontend_endpoint);
                let initial_backoff = initial_backoff.max(Duration::from_millis(1));
                let max_backoff = max_backoff.max(initial_backoff);
                let mut retry_delay = initial_backoff;
                while !thread_stop.load(Ordering::Acquire) {
                    let reported_state = if drain.is_draining() {
                        BackendReportedState::Draining
                    } else {
                        BackendReportedState::Running
                    };
                    let request = BackendAnnounceRequest::new(
                        novarocks_proto_codec::membership::BackendProcessDescriptor::from_contract(
                            thread_descriptor.clone(),
                        ),
                        reported_state,
                    )
                    .expect("backend process descriptor remains validated");
                    let next_delay = announce_once(
                        &client,
                        request,
                        interval,
                        initial_backoff,
                        max_backoff,
                        &mut retry_delay,
                    );
                    let (pending, signal) = &*thread_wake;
                    let mut pending = pending.lock().expect("backend announce wake lock");
                    if !*pending && !thread_stop.load(Ordering::Acquire) {
                        let (next, _) = signal
                            .wait_timeout(pending, next_delay)
                            .expect("backend announce wake wait");
                        pending = next;
                    }
                    *pending = false;
                }
            })
            .expect("spawn backend announce task");
        Self {
            stop,
            wake,
            join: Some(join),
            data_runtime,
            frontend_endpoint,
            descriptor,
        }
    }

    /// Reports the drain the process has already entered before waking the loop.
    pub fn announce_drain(&self) {
        let client = NativeRpcClient::new_native_endpoint(
            self.data_runtime.clone(),
            self.frontend_endpoint.clone(),
        );
        let request = BackendAnnounceRequest::new(
            novarocks_proto_codec::membership::BackendProcessDescriptor::from_contract(
                self.descriptor.clone(),
            ),
            BackendReportedState::Draining,
        )
        .expect("backend process descriptor remains validated");
        match announce_request(&client, request) {
            Ok(BackendAnnounceResult::Accepted { .. }) => {}
            Ok(BackendAnnounceResult::Rejected {
                reason,
                safe_detail,
            }) => {
                tracing::error!(?reason, %safe_detail, "backend drain announce rejected by frontend");
            }
            Err(error) => {
                tracing::warn!(%error, "backend drain announce attempt failed");
            }
        }
        let (pending, signal) = &*self.wake;
        *pending.lock().expect("backend announce wake lock") = true;
        signal.notify_one();
    }

    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let (_, signal) = &*self.wake;
        signal.notify_one();
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

fn announce_once(
    client: &NativeRpcClient,
    request: BackendAnnounceRequest,
    interval: Duration,
    initial_backoff: Duration,
    max_backoff: Duration,
    retry_delay: &mut Duration,
) -> Duration {
    match announce_request(client, request) {
        Ok(BackendAnnounceResult::Accepted { lease_ttl_ms }) => {
            *retry_delay = initial_backoff;
            interval.min(Duration::from_millis(lease_ttl_ms.saturating_div(3).max(1)))
        }
        Ok(BackendAnnounceResult::Rejected {
            reason,
            safe_detail,
        }) => {
            tracing::error!(?reason, %safe_detail, "backend announce rejected by frontend");
            let delay = *retry_delay;
            *retry_delay = retry_delay.saturating_mul(2).min(max_backoff);
            delay
        }
        Err(error) => {
            tracing::warn!(%error, "backend announce attempt failed");
            let delay = *retry_delay;
            *retry_delay = retry_delay.saturating_mul(2).min(max_backoff);
            delay
        }
    }
}

fn announce_request(
    client: &NativeRpcClient,
    request: BackendAnnounceRequest,
) -> Result<BackendAnnounceResult, String> {
    client
        .blocking_announce_backend_with_timeout(request.as_proto().clone(), ANNOUNCE_RPC_TIMEOUT)
        .and_then(|response| {
            BackendAnnounceResult::from_proto(response)
                .map_err(|error| format!("announce_backend response invalid: {error}"))
        })
}
