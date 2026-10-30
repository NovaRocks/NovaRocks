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
#[cfg(feature = "mem-1-m07-hms-listing-observe")]
use std::sync::Mutex;

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorOperationControl, ConnectorRequestContext,
};
use tokio::sync::Semaphore;

pub(crate) const LISTING_CONCURRENCY: usize = 8;

#[derive(Debug)]
pub(crate) struct ListingAdmission {
    positions: Arc<Semaphore>,
    #[cfg(feature = "mem-1-m07-hms-listing-observe")]
    observer: Mutex<Option<HmsObservation>>,
}

/// Observation of a result already published by the original closed source.
/// This grants no cancellation, CPU/I/O admission or new outcome selection.
pub(crate) trait ListingOriginalError: Send + Sync {
    fn selected_error(&self) -> Option<ConnectorError>;
}

impl Default for ListingAdmission {
    fn default() -> Self {
        Self {
            positions: Arc::new(Semaphore::new(LISTING_CONCURRENCY)),
            #[cfg(feature = "mem-1-m07-hms-listing-observe")]
            observer: Mutex::new(None),
        }
    }
}

#[cfg(feature = "mem-1-m07-hms-listing-observe")]
#[derive(Debug)]
struct HmsObservation {
    observer: Arc<super::hms_listing_observer::Observer>,
    cancellation: Option<novarocks_failpoint::scope::ControlCancellation>,
}

impl ListingAdmission {
    #[cfg(all(test, feature = "mem-1-m07-hms-listing-observe"))]
    fn observed_test_fixture() -> Self {
        Self {
            positions: Arc::new(Semaphore::new(LISTING_CONCURRENCY)),
            observer: Mutex::new(Some(HmsObservation {
                observer: Default::default(),
                cancellation: None,
            })),
        }
    }
    #[cfg(feature = "mem-1-m07-hms-listing-observe")]
    pub(crate) fn install_hms_observation(
        &self,
        cancellation: novarocks_failpoint::scope::ControlCancellation,
    ) -> Result<(), &'static str> {
        if !cancellation.is_active() {
            return Err("HMS observation session is closed");
        }
        let mut state = self
            .observer
            .lock()
            .map_err(|_| "HMS observer lock failed")?;
        if state.is_some() {
            return Err("HMS observer is already installed");
        }
        *state = Some(HmsObservation {
            observer: Default::default(),
            cancellation: Some(cancellation),
        });
        Ok(())
    }
    #[cfg(feature = "mem-1-m07-hms-listing-observe")]
    pub(crate) fn close_hms_observation(&self) {
        if let Ok(mut state) = self.observer.lock() {
            *state = None;
        }
    }
    #[cfg(feature = "mem-1-m07-hms-listing-observe")]
    pub(super) fn active_observer(&self) -> Option<Arc<super::hms_listing_observer::Observer>> {
        let mut state = self.observer.lock().ok()?;
        if state
            .as_ref()?
            .cancellation
            .as_ref()
            .is_some_and(|scope| !scope.is_active())
        {
            *state = None;
            return None;
        }
        Some(Arc::clone(&state.as_ref()?.observer))
    }
    #[cfg(all(test, feature = "mem-1-m07-hms-listing-observe"))]
    fn hms_observation_active(&self) -> bool {
        self.active_observer().is_some()
    }

    #[cfg(feature = "mem-1-m07-hms-listing-observe")]
    pub(crate) fn hms_snapshot(
        &self,
    ) -> Result<super::hms_listing_observer::Snapshot, &'static str> {
        let mut snapshot = self
            .active_observer()
            .ok_or("HMS observation is not active")?
            .snapshot()?;
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
        self.active_observer()
            .ok_or("HMS observation is not active")?
            .reset_idle(domain, phase, sequence)
    }

    #[cfg(feature = "mem-1-m07-hms-listing-observe")]
    pub(super) async fn run_hms<T, F: Future<Output = Result<T, ConnectorError>>>(
        &self,
        context: &ConnectorRequestContext,
        observer: Arc<super::hms_listing_observer::Observer>,
        operation: super::hms_listing_observer::HmsListingOperation,
        target_sha256: Option<[u8; 32]>,
        make_call: impl FnOnce(super::hms_listing_observer::Invocation) -> F,
    ) -> Result<T, ConnectorError> {
        use super::hms_listing_observer::{ExitSelection, PermitOwner, WrapperExit};
        let invocation = observer.begin(operation, target_sha256, context.deadline());
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
        self.run_original(context, call, None).await
    }

    pub(crate) async fn run_with_original<T>(
        &self,
        context: &ConnectorRequestContext,
        call: impl Future<Output = Result<T, ConnectorError>>,
        original: &dyn ListingOriginalError,
    ) -> Result<T, ConnectorError> {
        self.run_original(context, call, Some(original)).await
    }

    async fn run_original<T>(
        &self,
        context: &ConnectorRequestContext,
        call: impl Future<Output = Result<T, ConnectorError>>,
        original: Option<&dyn ListingOriginalError>,
    ) -> Result<T, ConnectorError> {
        if let Err(error) = context.check_active() {
            return Err(selected_original_error(original).unwrap_or(error));
        }
        let deadline = tokio::time::Instant::from_std(context.deadline());
        let permit = tokio::select! {
            biased;
            _ = context.stop().stopped() => return Err(selected_original_error(original).unwrap_or_else(cancelled)),
            _ = tokio::time::sleep_until(deadline) => return Err(selected_original_error(original).unwrap_or_else(expired)),
            permit = self.positions.clone().acquire_owned() => permit.map_err(|_| ConnectorError::new(ConnectorErrorKind::Internal, "catalog listing admission was closed"))?,
        };
        let result = {
            // This scope destroys the SDK future before its position can return.
            // A timeout or stop is not itself evidence that the future exited.
            tokio::pin!(call);
            tokio::select! {
                biased;
                // Select at this exact point, while the SAME pinned F still lives.
                // Its Drop cannot publish a later error over the chosen stop.
                _ = context.stop().stopped() => Err(selected_original_error(original).unwrap_or_else(cancelled)),
                _ = tokio::time::sleep_until(deadline) => Err(selected_original_error(original).unwrap_or_else(expired)),
                result = &mut call => result,
            }
        };
        drop(permit);
        result
    }
}

fn selected_original_error(original: Option<&dyn ListingOriginalError>) -> Option<ConnectorError> {
    original.and_then(ListingOriginalError::selected_error)
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
            observer: Mutex::new(Some(HmsObservation {
                observer: Default::default(),
                cancellation: None,
            })),
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
            observer: Mutex::new(Some(HmsObservation {
                observer: Default::default(),
                cancellation: None,
            })),
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
            observer: Mutex::new(Some(HmsObservation {
                observer: Default::default(),
                cancellation: None,
            })),
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

#[cfg(test)]
mod original_outcome_tests {
    use super::*;
    use novarocks_spi::connector::ConnectorStopOwner;
    use std::{
        sync::{
            Mutex,
            atomic::{AtomicBool, Ordering},
        },
        task::{Context, Poll},
        time::{Duration, Instant},
    };

    // This models only the original SourceMutex publication cut. None is a
    // currently loaned body; Some(None) is a returned body without an Err.
    struct Original {
        body: Mutex<Option<Option<ConnectorError>>>,
    }
    impl ListingOriginalError for Original {
        fn selected_error(&self) -> Option<ConnectorError> {
            self.body.lock().unwrap().as_ref().and_then(Clone::clone)
        }
    }
    struct FooterPending {
        original: Arc<Original>,
        published_on_poll: Option<ConnectorError>,
        published_on_drop: Option<ConnectorError>,
        gate: Arc<Semaphore>,
        dropped: Arc<AtomicBool>,
    }
    impl Future for FooterPending {
        type Output = Result<(), ConnectorError>;
        fn poll(self: std::pin::Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.get_mut();
            if let Some(error) = this.published_on_poll.take() {
                *this.original.body.lock().unwrap() = Some(Some(error));
            }
            Poll::Pending
        }
    }
    impl Drop for FooterPending {
        fn drop(&mut self) {
            assert_eq!(
                self.gate.available_permits(),
                LISTING_CONCURRENCY - 1,
                "the same original position must outlive this F Drop"
            );
            if let Some(error) = self.published_on_drop.take() {
                *self.original.body.lock().unwrap() = Some(Some(error));
            }
            self.dropped.store(true, Ordering::SeqCst);
        }
    }
    fn context(stop: &ConnectorStopOwner) -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(5),
            stop.view(),
            1024,
            4096,
        )
        .unwrap()
    }
    #[tokio::test]
    async fn published_original_error_wins_late_stop_before_the_same_future_drop() {
        let admission = ListingAdmission::default();
        let stop = ConnectorStopOwner::new();
        let context = context(&stop);
        let original = Arc::new(Original {
            body: Mutex::new(Some(None)),
        });
        let selected = ConnectorError::from_domain_static(
            ConnectorErrorKind::CorruptData,
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel,
            "selected original listing source",
        );
        let dropped = Arc::new(AtomicBool::new(false));
        let call = FooterPending {
            original: original.clone(),
            published_on_poll: Some(selected.clone()),
            published_on_drop: None,
            gate: admission.positions.clone(),
            dropped: dropped.clone(),
        };
        let mut outcome = Box::pin(admission.run_with_original(&context, call, original.as_ref()));
        assert!(futures::poll!(outcome.as_mut()).is_pending());
        assert_eq!(original.selected_error(), Some(selected.clone()));
        stop.request_stop();
        assert_eq!(
            outcome.await.unwrap_err(),
            selected,
            "the same opaque cause survives the later stop"
        );
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(admission.positions.available_permits(), LISTING_CONCURRENCY);
    }
    #[tokio::test]
    async fn loaned_body_stop_is_not_replaced_by_error_published_during_future_drop() {
        let admission = ListingAdmission::default();
        let stop = ConnectorStopOwner::new();
        let context = context(&stop);
        let original = Arc::new(Original {
            body: Mutex::new(None),
        });
        let late = ConnectorError::new(ConnectorErrorKind::CorruptData, "later returned CPU body");
        let dropped = Arc::new(AtomicBool::new(false));
        let call = FooterPending {
            original: original.clone(),
            published_on_poll: None,
            published_on_drop: Some(late.clone()),
            gate: admission.positions.clone(),
            dropped: dropped.clone(),
        };
        let mut outcome = Box::pin(admission.run_with_original(&context, call, original.as_ref()));
        assert!(futures::poll!(outcome.as_mut()).is_pending());
        assert_eq!(original.selected_error(), None);
        stop.request_stop();
        assert_eq!(
            outcome.await.unwrap_err().kind(),
            ConnectorErrorKind::Cancelled
        );
        assert_eq!(
            original.selected_error(),
            Some(late),
            "this fact was published only AFTER selection"
        );
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(admission.positions.available_permits(), LISTING_CONCURRENCY);
    }
}
