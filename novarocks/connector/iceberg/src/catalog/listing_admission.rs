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

//! Catalog-generation admission for trusted external SDK listings.

use std::future::Future;
use std::sync::Arc;

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorOperationControl, ConnectorRequestContext,
};
use tokio::sync::Semaphore;

pub(crate) const LISTING_CONCURRENCY: usize = 8;

#[derive(Debug)]
pub(crate) struct ListingAdmission {
    positions: Arc<Semaphore>,
    #[cfg(feature = "mem-1-m07-hms-listing-observe")]
    observer: Arc<super::hms_listing_observer::Observer>,
}

impl Default for ListingAdmission {
    fn default() -> Self {
        Self {
            positions: Arc::new(Semaphore::new(LISTING_CONCURRENCY)),
            #[cfg(feature = "mem-1-m07-hms-listing-observe")]
            observer: Default::default(),
        }
    }
}

impl ListingAdmission {
    #[cfg(feature = "mem-1-m07-hms-listing-observe")]
    pub(crate) fn hms_snapshot(
        &self,
    ) -> Result<super::hms_listing_observer::Snapshot, &'static str> {
        let mut snapshot = self.observer.snapshot()?;
        snapshot.available_positions_sample = Some(self.positions.available_permits());
        Ok(snapshot)
    }

    #[cfg(feature = "mem-1-m07-hms-listing-observe")]
    pub(crate) fn reset_hms_observation_idle(
        &self,
        domain: uuid::Uuid,
        phase: u64,
        sequence: u64,
    ) -> Result<u64, &'static str> {
        self.observer.reset_idle(domain, phase, sequence)
    }

    #[cfg(feature = "mem-1-m07-hms-listing-observe")]
    pub(super) async fn run_hms<T, F: Future<Output = Result<T, ConnectorError>>>(
        &self,
        context: &ConnectorRequestContext,
        operation: super::hms_listing_observer::HmsListingOperation,
        target_sha256: Option<[u8; 32]>,
        make_call: impl FnOnce(super::hms_listing_observer::Invocation) -> F,
    ) -> Result<T, ConnectorError> {
        use super::hms_listing_observer::{ExitSelection, PermitOwner, WrapperExit};
        let invocation = self
            .observer
            .begin(operation, target_sha256, context.deadline());
        // PermitOwner is declared before both wrapper witness and the future.
        // Cancelling/dropping this outer future cannot return the permit first.
        let mut owner = PermitOwner {
            invocation: invocation.clone(),
            permit: None,
        };
        let _wrapper_exit = WrapperExit(invocation.clone());
        let call = make_call(invocation.clone());
        tokio::pin!(call);
        if let Err(error) = context.check_active() {
            invocation.select(ExitSelection::InitialCheck, context);
            return Err(error);
        }
        let deadline = tokio::time::Instant::from_std(context.deadline());
        let permit = tokio::select! {
            biased;
            _ = context.stop().stopped() => { invocation.select(ExitSelection::StopWaiting, context); return Err(cancelled()); },
            _ = tokio::time::sleep_until(deadline) => { invocation.select(ExitSelection::DeadlineWaiting, context); return Err(expired()); },
            permit = self.positions.clone().acquire_owned() => match permit {
                Ok(permit) => permit,
                Err(_) => { invocation.select(ExitSelection::AdmissionClosed, context);
                    return Err(ConnectorError::new(ConnectorErrorKind::Internal, "catalog listing admission was closed")); }
            },
        };
        owner.acquired(permit);
        tokio::select! {
            biased;
            _ = context.stop().stopped() => { invocation.select(ExitSelection::StopAdmitted, context); Err(cancelled()) },
            _ = tokio::time::sleep_until(deadline) => { invocation.select(ExitSelection::DeadlineAdmitted, context); Err(expired()) },
            result = &mut call => {
                invocation.select(if result.is_ok() { ExitSelection::ReadyOk } else { ExitSelection::ReadyErr }, context);
                result
            },
        }
    }

    #[cfg(test)]
    pub(crate) fn available_positions(&self) -> usize {
        self.positions.available_permits()
    }

    /// Source revalidation retains admission until the already-issued SDK call
    /// actually exits. Its caller checks stop/deadline before each subsequent
    /// page or stat request, so waiting never authorizes another source request.
    pub(crate) async fn run_wait_for_exit<T>(
        &self,
        context: &ConnectorRequestContext,
        call: impl Future<Output = Result<T, ConnectorError>>,
    ) -> Result<T, ConnectorError> {
        context.check_active()?;
        let deadline = tokio::time::Instant::from_std(context.deadline());
        let permit = tokio::select! {
            biased;
            _ = context.stop().stopped() => return Err(cancelled()),
            _ = tokio::time::sleep_until(deadline) => return Err(expired()),
            permit = self.positions.clone().acquire_owned() => permit.map_err(|_| ConnectorError::new(ConnectorErrorKind::Internal, "catalog listing admission was closed"))?,
        };
        context.check_active()?;
        let result = {
            tokio::pin!(call);
            tokio::select! {
                biased;
                _ = context.stop().stopped() => {
                    let _ = call.await;
                    Err(cancelled())
                },
                _ = tokio::time::sleep_until(deadline) => {
                    let _ = call.await;
                    Err(expired())
                },
                result = &mut call => {
                    context.check_active()?;
                    result
                },
            }
        };
        drop(permit);
        result
    }

    pub(crate) async fn run<T>(
        &self,
        context: &ConnectorRequestContext,
        call: impl Future<Output = Result<T, ConnectorError>>,
    ) -> Result<T, ConnectorError> {
        context.check_active()?;
        let deadline = tokio::time::Instant::from_std(context.deadline());
        let permit = tokio::select! {
            biased;
            _ = context.stop().stopped() => return Err(cancelled()),
            _ = tokio::time::sleep_until(deadline) => return Err(expired()),
            permit = self.positions.clone().acquire_owned() => permit.map_err(|_| ConnectorError::new(ConnectorErrorKind::Internal, "catalog listing admission was closed"))?,
        };
        let result = {
            // This scope destroys the SDK future before its position can return.
            // A timeout or stop is not itself evidence that the future exited.
            tokio::pin!(call);
            tokio::select! {
                biased;
                _ = context.stop().stopped() => Err(cancelled()),
                _ = tokio::time::sleep_until(deadline) => Err(expired()),
                result = &mut call => result,
            }
        };
        drop(permit);
        result
    }
}

fn cancelled() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Cancelled,
        "catalog listing was cancelled",
    )
}
fn expired() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::DeadlineExceeded,
        "catalog listing absolute deadline elapsed",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::ConnectorStopOwner;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    fn context(stop: &ConnectorStopOwner, deadline: Instant) -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(deadline, stop.view(), 1024, 4096).unwrap()
    }

    struct PendingCall {
        gate: Arc<Semaphore>,
        dropped: Arc<AtomicBool>,
    }
    impl Future for PendingCall {
        type Output = Result<(), ConnectorError>;
        fn poll(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            std::task::Poll::Pending
        }
    }
    impl Drop for PendingCall {
        fn drop(&mut self) {
            assert_eq!(
                self.gate.available_permits(),
                0,
                "SDK future must exit before its admission position returns"
            );
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn source_stop_retains_position_until_the_issued_call_actually_exits() {
        let gate = ListingAdmission {
            positions: Arc::new(Semaphore::new(1)),
            #[cfg(feature = "mem-1-m07-hms-listing-observe")]
            observer: Default::default(),
        };
        let stop = ConnectorStopOwner::new();
        let ctx = context(&stop, Instant::now() + Duration::from_secs(5));
        let polled = Arc::new(AtomicBool::new(false));
        let exited = Arc::new(AtomicBool::new(false));
        let (release, released) = tokio::sync::oneshot::channel::<()>();
        let call = {
            let polled = polled.clone();
            let exited = exited.clone();
            async move {
                polled.store(true, Ordering::SeqCst);
                released.await.unwrap();
                exited.store(true, Ordering::SeqCst);
                Ok(())
            }
        };
        let outcome = gate.run_wait_for_exit(&ctx, call);
        tokio::pin!(outcome);
        assert!(futures::poll!(outcome.as_mut()).is_pending());
        assert!(polled.load(Ordering::SeqCst));
        assert_eq!(gate.positions.available_permits(), 0);

        stop.request_stop();
        // Poll after the selected stop, while the real call remains pending.
        // Dropping the SDK future on stop would return Ready here.
        assert!(futures::poll!(outcome.as_mut()).is_pending());
        assert!(!exited.load(Ordering::SeqCst));
        assert_eq!(gate.positions.available_permits(), 0);

        release.send(()).unwrap();
        assert_eq!(
            outcome.await.unwrap_err().kind(),
            ConnectorErrorKind::Cancelled
        );
        assert!(exited.load(Ordering::SeqCst));
        assert_eq!(gate.positions.available_permits(), 1);
    }

    #[tokio::test]
    async fn deadline_drops_sdk_before_returning_its_position() {
        let gate = ListingAdmission {
            positions: Arc::new(Semaphore::new(1)),
            #[cfg(feature = "mem-1-m07-hms-listing-observe")]
            observer: Default::default(),
        };
        let stop = ConnectorStopOwner::new();
        let dropped = Arc::new(AtomicBool::new(false));
        let call = PendingCall {
            gate: gate.positions.clone(),
            dropped: dropped.clone(),
        };
        let error = gate
            .run(
                &context(&stop, Instant::now() + Duration::from_millis(20)),
                call,
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::DeadlineExceeded);
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(gate.positions.available_permits(), 1);
    }

    #[tokio::test]
    async fn stop_drops_sdk_before_returning_its_position() {
        let gate = ListingAdmission {
            positions: Arc::new(Semaphore::new(1)),
            #[cfg(feature = "mem-1-m07-hms-listing-observe")]
            observer: Default::default(),
        };
        let stop = ConnectorStopOwner::new();
        let ctx = context(&stop, Instant::now() + Duration::from_secs(5));
        let dropped = Arc::new(AtomicBool::new(false));
        let call = PendingCall {
            gate: gate.positions.clone(),
            dropped: dropped.clone(),
        };
        let outcome = gate.run(&ctx, call);
        let cancel = async {
            tokio::task::yield_now().await;
            stop.request_stop();
        };
        let (error, _) = tokio::join!(outcome, cancel);
        assert_eq!(error.unwrap_err().kind(), ConnectorErrorKind::Cancelled);
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(gate.positions.available_permits(), 1);
    }

    #[tokio::test]
    async fn full_admission_never_polls_another_sdk_call() {
        let gate = ListingAdmission::default();
        let occupied = gate
            .positions
            .clone()
            .acquire_many_owned(LISTING_CONCURRENCY as u32)
            .await
            .unwrap();
        let stop = ConnectorStopOwner::new();
        let polled = Arc::new(AtomicBool::new(false));
        let marker = polled.clone();
        let call = async move {
            marker.store(true, Ordering::SeqCst);
            Ok(())
        };
        let error = gate
            .run(
                &context(&stop, Instant::now() + Duration::from_millis(20)),
                call,
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::DeadlineExceeded);
        assert!(!polled.load(Ordering::SeqCst));
        drop(occupied);
        gate.run(
            &context(&stop, Instant::now() + Duration::from_secs(5)),
            async { Ok(()) },
        )
        .await
        .unwrap();
        assert_eq!(gate.positions.available_permits(), LISTING_CONCURRENCY);
    }
}

#[cfg(all(test, feature = "mem-1-m07-hms-listing-observe"))]
#[path = "hms_listing_observer_tests.rs"]
mod hms_observation_tests;
