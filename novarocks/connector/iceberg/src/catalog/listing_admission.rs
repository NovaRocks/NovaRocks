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
        }
    }
}

impl ListingAdmission {
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
    use std::pin::Pin;
    use std::sync::Mutex as TestMutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::{Context, Poll};
    use std::time::{Duration, Instant};

    fn context(stop: &ConnectorStopOwner, deadline: Instant) -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(deadline, stop.view(), 1024, 4096).unwrap()
    }

    #[derive(Default)]
    struct SdkProbe {
        started: AtomicUsize,
        live: AtomicUsize,
        peak: AtomicUsize,
        dropped: AtomicUsize,
        positions_at_drop: TestMutex<Vec<usize>>,
    }

    struct SdkLifetime {
        positions: Arc<Semaphore>,
        probe: Arc<SdkProbe>,
    }

    impl SdkLifetime {
        fn new(positions: Arc<Semaphore>, probe: Arc<SdkProbe>) -> Self {
            assert!(positions.available_permits() < LISTING_CONCURRENCY);
            let live = probe.live.fetch_add(1, Ordering::SeqCst) + 1;
            probe.started.fetch_add(1, Ordering::SeqCst);
            probe.peak.fetch_max(live, Ordering::SeqCst);
            assert!(live <= LISTING_CONCURRENCY);
            Self { positions, probe }
        }
    }

    impl Drop for SdkLifetime {
        fn drop(&mut self) {
            let available = self.positions.available_permits();
            assert!(
                available < LISTING_CONCURRENCY,
                "the actual SDK object must exit before its position returns"
            );
            self.probe.positions_at_drop.lock().unwrap().push(available);
            assert!(self.probe.live.fetch_sub(1, Ordering::SeqCst) > 0);
            self.probe.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }

    enum SdkCompletion {
        Pending,
        Ready(Option<ConnectorError>),
        Release(tokio::sync::oneshot::Receiver<()>),
        Panic(Arc<u8>),
    }

    struct SdkCall {
        positions: Arc<Semaphore>,
        probe: Arc<SdkProbe>,
        lifetime: Option<SdkLifetime>,
        completion: SdkCompletion,
    }

    impl Future for SdkCall {
        type Output = Result<(), ConnectorError>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.get_mut();
            // The delegate constructs its SDK object when the admitted body
            // first runs. A waiting outer future is not a live SDK object.
            // Retain that object even after Ready until this future's Drop.
            if this.lifetime.is_none() {
                this.lifetime = Some(SdkLifetime::new(
                    Arc::clone(&this.positions),
                    Arc::clone(&this.probe),
                ));
            }
            match &mut this.completion {
                SdkCompletion::Pending => Poll::Pending,
                SdkCompletion::Ready(error) => Poll::Ready(error.take().map_or(Ok(()), Err)),
                SdkCompletion::Release(released) => Pin::new(released).poll(cx).map(|result| {
                    result.expect("the original SDK release sender remains owned");
                    Ok(())
                }),
                SdkCompletion::Panic(payload) => std::panic::panic_any(Arc::clone(payload)),
            }
        }
    }

    fn sdk_call(
        gate: &ListingAdmission,
        probe: &Arc<SdkProbe>,
        completion: SdkCompletion,
    ) -> SdkCall {
        SdkCall {
            positions: Arc::clone(&gate.positions),
            probe: Arc::clone(probe),
            lifetime: None,
            completion,
        }
    }

    fn assert_idle(gate: &ListingAdmission, probe: &SdkProbe, expected_calls: usize) {
        assert_eq!(gate.positions.available_permits(), LISTING_CONCURRENCY);
        assert_eq!(probe.started.load(Ordering::SeqCst), expected_calls);
        assert_eq!(probe.live.load(Ordering::SeqCst), 0);
        assert_eq!(probe.dropped.load(Ordering::SeqCst), expected_calls);
        assert_eq!(
            probe.positions_at_drop.lock().unwrap().len(),
            expected_calls
        );
    }

    #[tokio::test]
    async fn eight_started_sdk_calls_keep_the_ninth_waiting_then_reuse_the_same_gate() {
        let gate = ListingAdmission::default();
        let stop = ConnectorStopOwner::new();
        let ctx = context(&stop, Instant::now() + Duration::from_secs(5));
        let probe = Arc::new(SdkProbe::default());
        let mut releases = Vec::new();
        let mut calls = Vec::new();
        for _ in 0..=LISTING_CONCURRENCY {
            let (release, released) = tokio::sync::oneshot::channel();
            releases.push(Some(release));
            calls.push(Box::pin(gate.run(
                &ctx,
                sdk_call(&gate, &probe, SdkCompletion::Release(released)),
            )));
        }
        for call in &mut calls {
            assert!(futures::poll!(call.as_mut()).is_pending());
        }
        assert_eq!(gate.positions.available_permits(), 0);
        assert_eq!(probe.started.load(Ordering::SeqCst), 8);
        assert_eq!(probe.live.load(Ordering::SeqCst), 8);
        assert_eq!(probe.peak.load(Ordering::SeqCst), 8);

        releases[0].take().unwrap().send(()).unwrap();
        calls.remove(0).await.unwrap();
        assert_eq!(*probe.positions_at_drop.lock().unwrap(), [0]);
        assert_eq!(probe.live.load(Ordering::SeqCst), 7);
        assert!(futures::poll!(calls.last_mut().unwrap().as_mut()).is_pending());
        assert_eq!(probe.started.load(Ordering::SeqCst), 9);
        assert_eq!(probe.live.load(Ordering::SeqCst), 8);
        assert_eq!(probe.peak.load(Ordering::SeqCst), 8);

        for release in releases.iter_mut().skip(1) {
            release.take().unwrap().send(()).unwrap();
        }
        for call in calls {
            call.await.unwrap();
        }
        assert_idle(&gate, &probe, 9);
        gate.run(&ctx, sdk_call(&gate, &probe, SdkCompletion::Ready(None)))
            .await
            .unwrap();
        assert_idle(&gate, &probe, 10);
        assert_eq!(probe.peak.load(Ordering::SeqCst), 8);
    }

    #[tokio::test]
    async fn ready_sdk_results_drop_before_position_return_and_preserve_the_original_error() {
        let gate = ListingAdmission::default();
        let stop = ConnectorStopOwner::new();
        let ctx = context(&stop, Instant::now() + Duration::from_secs(5));
        let probe = Arc::new(SdkProbe::default());
        gate.run(&ctx, sdk_call(&gate, &probe, SdkCompletion::Ready(None)))
            .await
            .unwrap();
        let original = ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "the original catalog listing refusal",
        );
        let error = gate
            .run(
                &ctx,
                sdk_call(&gate, &probe, SdkCompletion::Ready(Some(original.clone()))),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), original.kind());
        assert_eq!(error.message(), original.message());
        assert_idle(&gate, &probe, 2);
        assert_eq!(*probe.positions_at_drop.lock().unwrap(), [7, 7]);
    }

    #[tokio::test]
    async fn default_deadline_drops_the_polled_sdk_before_its_position_returns() {
        let gate = ListingAdmission::default();
        let stop = ConnectorStopOwner::new();
        let ctx = context(&stop, Instant::now() + Duration::from_millis(100));
        let probe = Arc::new(SdkProbe::default());
        let mut call = Box::pin(gate.run(&ctx, sdk_call(&gate, &probe, SdkCompletion::Pending)));
        assert!(futures::poll!(call.as_mut()).is_pending());
        assert_eq!(probe.live.load(Ordering::SeqCst), 1);
        assert_eq!(gate.positions.available_permits(), 7);
        assert_eq!(
            call.await.unwrap_err().kind(),
            ConnectorErrorKind::DeadlineExceeded
        );
        assert_idle(&gate, &probe, 1);
        assert_eq!(*probe.positions_at_drop.lock().unwrap(), [7]);
    }

    #[tokio::test]
    async fn default_stop_drops_the_polled_sdk_before_its_position_returns() {
        let gate = ListingAdmission::default();
        let stop = ConnectorStopOwner::new();
        let ctx = context(&stop, Instant::now() + Duration::from_secs(5));
        let probe = Arc::new(SdkProbe::default());
        let mut call = Box::pin(gate.run(&ctx, sdk_call(&gate, &probe, SdkCompletion::Pending)));
        assert!(futures::poll!(call.as_mut()).is_pending());
        stop.request_stop();
        assert_eq!(
            call.await.unwrap_err().kind(),
            ConnectorErrorKind::Cancelled
        );
        assert_idle(&gate, &probe, 1);
        assert_eq!(*probe.positions_at_drop.lock().unwrap(), [7]);
    }

    #[tokio::test]
    async fn sdk_poll_panic_keeps_the_original_payload_and_returns_its_position_after_drop() {
        use futures::FutureExt;
        use std::panic::AssertUnwindSafe;

        let gate = ListingAdmission::default();
        let stop = ConnectorStopOwner::new();
        let ctx = context(&stop, Instant::now() + Duration::from_secs(5));
        let probe = Arc::new(SdkProbe::default());
        let payload = Arc::new(17_u8);
        let panic = AssertUnwindSafe(gate.run(
            &ctx,
            sdk_call(&gate, &probe, SdkCompletion::Panic(Arc::clone(&payload))),
        ))
        .catch_unwind()
        .await
        .unwrap_err();
        assert!(Arc::ptr_eq(&payload, &panic.downcast::<Arc<u8>>().unwrap()));
        assert_idle(&gate, &probe, 1);
        assert_eq!(*probe.positions_at_drop.lock().unwrap(), [7]);
        gate.run(&ctx, sdk_call(&gate, &probe, SdkCompletion::Ready(None)))
            .await
            .unwrap();
        assert_idle(&gate, &probe, 2);
    }

    #[tokio::test]
    async fn dropping_the_outer_call_drops_the_actual_sdk_before_returning_its_position() {
        let gate = ListingAdmission::default();
        let stop = ConnectorStopOwner::new();
        let ctx = context(&stop, Instant::now() + Duration::from_secs(5));
        let probe = Arc::new(SdkProbe::default());
        let mut call = Box::pin(gate.run(&ctx, sdk_call(&gate, &probe, SdkCompletion::Pending)));
        assert!(futures::poll!(call.as_mut()).is_pending());
        assert_eq!(probe.live.load(Ordering::SeqCst), 1);
        drop(call);
        assert_idle(&gate, &probe, 1);
        assert_eq!(*probe.positions_at_drop.lock().unwrap(), [7]);
    }

    #[tokio::test]
    async fn waiting_stop_wins_an_elapsed_deadline_without_polling_an_sdk_call() {
        let gate = ListingAdmission::default();
        let occupied = gate
            .positions
            .clone()
            .acquire_many_owned(LISTING_CONCURRENCY as u32)
            .await
            .unwrap();
        let stop = ConnectorStopOwner::new();
        let ctx = context(&stop, Instant::now() + Duration::from_millis(100));
        let probe = Arc::new(SdkProbe::default());
        let mut call = Box::pin(gate.run(&ctx, sdk_call(&gate, &probe, SdkCompletion::Pending)));
        assert!(futures::poll!(call.as_mut()).is_pending());
        tokio::time::sleep_until(ctx.deadline().into()).await;
        stop.request_stop();
        assert_eq!(
            call.await.unwrap_err().kind(),
            ConnectorErrorKind::Cancelled
        );
        drop(occupied);
        assert_idle(&gate, &probe, 0);
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
