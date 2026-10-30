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

//! Request-local delegation to the original action's fixed I/O cells.
//!
//! These receipts carry custody of an existing cell, never Work authority.
//! The frontend drives the same Root's paid proposal outside provider polls.

use super::{ConnectorError, ConnectorRequestContext};
pub use novarocks_resource_contract::{
    HdfsServiceOrigin, NetworkServiceOrigin, SelectedThriftServiceOrigin, SharedDependency,
    ThriftServiceOrigin,
};
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};

/// The original action supplies one finite inventory, sized by the provider's
/// actual maximum concurrent I/O. This port does not create an execution pool.
pub trait ConnectorIoAdmissionPort: Send + Sync {
    /// Bind the exact running generation's pure factory profile before its
    /// read creator is dispatched. This publishes no physical permit.
    fn configure_read_source(
        &self,
        _profile: &super::ConnectorMetadataMaintenanceIoProfile,
    ) -> Result<(), ConnectorError> {
        Err(ConnectorError::new(
            super::ConnectorErrorKind::Unsupported,
            "read source profile is not installed",
        ))
    }
    fn poll_read_source_ready(&self, _cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
        Poll::Ready(Err(ConnectorError::new(
            super::ConnectorErrorKind::Unsupported,
            "read source profile is not installed",
        )))
    }

    fn request_owned(
        &self,
        additional: std::num::NonZeroU64,
    ) -> Result<ConnectorIoRequest, ConnectorError>;
    fn request(&self, dependency: SharedDependency) -> Result<ConnectorIoRequest, ConnectorError>;
    /// Called only after the original publication journal has recorded a real
    /// dispatch obligation. It changes later cleanup requests, not new business.
    fn publication_obligation_started(&self);
    fn retain_payload(&self) -> Result<super::ConnectorPayloadRetentionGuard, ConnectorError>;
}

/// One preallocated original request cell. Only its real owner can publish a
/// grant or refusal; consumers may borrow the verdict and retain that same cell.
/// Read-only progress of the same original in-flight acquisition. The cell
/// retains the original header; that header keeps only weak request/phase
/// references, so authority progress cannot form a request-cell cycle.
pub trait ConnectorIoProgressSource: Send + Sync {
    /// Affirmative original F/context/callback exit, never inferred from absence.
    fn actual_exit_observed(&self) -> bool;
    fn health_cause(self: Arc<Self>) -> Option<Arc<dyn std::error::Error + Send + Sync>>;
    fn poll_original_progress(&self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>>;
}
pub type ConnectorIoCleanupFuture = futures::future::BoxFuture<'static, Result<(), ConnectorError>>;
pub trait ConnectorIoRequestSource: Send + Sync {
    /// Same fixed wake header; it must not retain this request cell.
    fn exit_waker(&self) -> std::task::Waker;
    fn reserve_stream(&self) -> Result<(), ConnectorError>;
    fn stream_closed(&self);
    fn install_stream_cleanup_in(
        &self,
        source: &mut Option<ConnectorIoCleanupFuture>,
    ) -> Result<(), ConnectorError>;
    fn stream_custody_lost(&self);
    /// Store the original cleanup error before any subsequent raw I/O Drop.
    fn record_cleanup_failure(
        &self,
        original: Box<dyn std::any::Any + Send>,
    ) -> Result<(), Box<dyn std::any::Any + Send>>;
    fn attach_original_progress(
        &self,
        source: Arc<dyn ConnectorIoProgressSource>,
    ) -> Result<(), ConnectorError>;
    fn poll_admitted(&self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>>;
    /// Reuse this same live position only after its complete D has been
    /// atomically extended by the original owner. No second permit is awaited.
    fn poll_include_dependency(
        &self,
        dependency: &SharedDependency,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), ConnectorError>>;
}

#[must_use = "Keep the original request while its paid admission is pending"]
pub struct ConnectorIoRequest {
    original: Arc<dyn ConnectorIoRequestSource>,
}
impl ConnectorIoRequest {
    /// Neutral projection of a pre-existing cell. This grants no WC capacity:
    /// poll_admitted still requires the original owner's actual grant.
    pub fn from_original(original: Arc<dyn ConnectorIoRequestSource>) -> Self {
        Self { original }
    }
    /// Silent original receiving seam. The source retains its request while
    /// pending and installs the positive permit before any request/F Drop.
    pub fn poll_admitted_in(
        &self,
        target: &mut Option<ConnectorIoPermit>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), ConnectorError>> {
        if target.is_some() {
            return Poll::Ready(Err(ConnectorError::new(
                super::ConnectorErrorKind::InvalidRequest,
                "I/O permit receiver is occupied",
            )));
        }
        match self.original.poll_admitted(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => {
                *target = Some(ConnectorIoPermit {
                    original: Some(Arc::downgrade(&self.original)),
                    keepalive: Some(Arc::clone(&self.original)),
                    exit_waker: self.original.exit_waker(),
                });
                Poll::Ready(Ok(()))
            }
        }
    }
    pub async fn admitted(self) -> Result<ConnectorIoPermit, ConnectorError> {
        std::future::poll_fn(|cx| self.original.poll_admitted(cx)).await?;
        let exit_waker = self.original.exit_waker();
        Ok(ConnectorIoPermit {
            original: Some(Arc::downgrade(&self.original)),
            keepalive: Some(self.original),
            exit_waker,
        })
    }
}

/// Keep this same alias through actual request F/response/stream destruction.
/// Its Drop exits both cell references before notifying the independent wake
/// header. It performs no release, retry, I/O or capacity publication.
/// The owner can reclaim only after all aliases of that original cell exit.
#[derive(Clone)]
pub struct ConnectorIoPermit {
    original: Option<Weak<dyn ConnectorIoRequestSource>>,
    // Normal raw request/body aliases keep the original cell alive. Nested
    // cleanup aliases instead borrow its already reserved original stream slot.
    keepalive: Option<Arc<dyn ConnectorIoRequestSource>>,
    exit_waker: std::task::Waker,
}
impl ConnectorIoPermit {
    pub fn attach_original_progress(
        &self,
        source: Arc<dyn ConnectorIoProgressSource>,
    ) -> Result<(), ConnectorError> {
        self.original
            .as_ref()
            .and_then(Weak::upgrade)
            .ok_or_else(source_gone)?
            .attach_original_progress(source)
    }
    /// Preserve the same paid responsibility in a returned body/stream wrapper.
    pub fn retain(&self) -> Self {
        self.clone()
    }
    pub fn begin_stream(&self) -> Result<ConnectorIoStreamCustody, ConnectorError> {
        let original = self
            .original
            .as_ref()
            .and_then(Weak::upgrade)
            .ok_or_else(source_gone)?;
        original.reserve_stream()?;
        Ok(ConnectorIoStreamCustody {
            original: Some(original),
        })
    }
    pub fn weak_phase(&self) -> ConnectorIoWeakPhase {
        ConnectorIoWeakPhase {
            original: self.original.clone(),
            exit_waker: self.exit_waker.clone(),
        }
    }
    pub async fn include_dependency(
        &self,
        dependency: &SharedDependency,
    ) -> Result<(), ConnectorError> {
        let original = self
            .original
            .as_ref()
            .and_then(Weak::upgrade)
            .ok_or_else(source_gone)?;
        std::future::poll_fn(|cx| original.poll_include_dependency(dependency, cx)).await
    }
}
impl std::fmt::Debug for ConnectorIoPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _ = &self.original;
        f.write_str("ConnectorIoPermit { original: .. }")
    }
}
impl ConnectorRequestContext {
    pub fn retain_owned_metadata(
        &self,
    ) -> Result<Option<super::ConnectorPayloadRetentionGuard>, ConnectorError> {
        self.io_admission()
            .map(|port| port.upgrade().ok_or_else(source_gone)?.retain_payload())
            .transpose()
    }
    pub fn publication_obligation_started(&self) -> Result<(), ConnectorError> {
        if let Some(port) = self.io_admission() {
            port.upgrade()
                .ok_or_else(source_gone)?
                .publication_obligation_started();
        }
        Ok(())
    }
    pub async fn reserve_owned_metadata(
        &self,
        required_peak_total: std::num::NonZeroU64,
    ) -> Result<(), ConnectorError> {
        if let Some(port) = self.io_admission() {
            let port = port.upgrade().ok_or_else(source_gone)?;
            let _receipt = port.request_owned(required_peak_total)?.admitted().await?;
        }
        Ok(())
    }
    /// No port means an untouched caller, not a governed fallback. The two
    /// metadata statement producers always install their original action port.
    pub async fn admit_selected_io(
        &self,
        dependency: SharedDependency,
    ) -> Result<Option<ConnectorIoPermit>, ConnectorError> {
        if let Some(phase) = self.io_phase() {
            phase.include_dependency(&dependency).await?;
            return Ok(Some(phase.retain()));
        }
        if let Some(phase) = self.io_weak_phase() {
            return phase.include_dependency(&dependency).await.map(Some);
        }
        match self.io_admission() {
            Some(port) => port
                .upgrade()
                .ok_or_else(source_gone)?
                .request(dependency)?
                .admitted()
                .await
                .map(Some),
            None => Ok(None),
        }
    }
}

/// Only the original stream creation can reserve this non-Clone disposition.
/// It never mints capacity; the same source still owns the actual AsyncIo grant.
pub struct ConnectorIoStreamCustody {
    original: Option<Arc<dyn ConnectorIoRequestSource>>,
}
impl ConnectorIoStreamCustody {
    pub fn closed(mut self) {
        let original = self.original.take().unwrap();
        original.stream_closed();
    }
    pub fn install_cleanup_in(
        mut self,
        source: &mut Option<ConnectorIoCleanupFuture>,
    ) -> Result<(), (Self, ConnectorError)> {
        match self
            .original
            .as_ref()
            .unwrap()
            .install_stream_cleanup_in(source)
        {
            Ok(()) => {
                self.original.take();
                Ok(())
            }
            Err(error) => Err((self, error)),
        }
    }
}
impl Drop for ConnectorIoStreamCustody {
    fn drop(&mut self) {
        if let Some(original) = self.original.take() {
            original.stream_custody_lost();
        }
    }
}
#[derive(Clone)]
pub struct ConnectorIoWeakPhase {
    original: Option<Weak<dyn ConnectorIoRequestSource>>,
    exit_waker: std::task::Waker,
}
impl std::fmt::Debug for ConnectorIoWeakPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ConnectorIoWeakPhase { original stream-owned position }")
    }
}
impl ConnectorIoWeakPhase {
    pub fn record_cleanup_failure(
        &self,
        cause: Box<dyn std::any::Any + Send>,
    ) -> Result<(), Box<dyn std::any::Any + Send>> {
        match self.original.as_ref().and_then(Weak::upgrade) {
            Some(source) => source.record_cleanup_failure(cause),
            None => Err(cause),
        }
    }
    async fn include_dependency(
        &self,
        dependency: &SharedDependency,
    ) -> Result<ConnectorIoPermit, ConnectorError> {
        let original = self
            .original
            .as_ref()
            .and_then(Weak::upgrade)
            .ok_or_else(source_gone)?;
        std::future::poll_fn(|cx| original.poll_include_dependency(dependency, cx)).await?;
        Ok(ConnectorIoPermit {
            original: Some(Arc::downgrade(&original)),
            keepalive: None,
            exit_waker: self.exit_waker.clone(),
        })
    }
}
fn source_gone() -> ConnectorError {
    ConnectorError::new(
        super::ConnectorErrorKind::Internal,
        "original governed I/O source is no longer available",
    )
}

impl Drop for ConnectorIoPermit {
    fn drop(&mut self) {
        drop(self.keepalive.take());
        drop(self.original.take());
        self.exit_waker.wake_by_ref();
    }
}
impl Drop for ConnectorIoWeakPhase {
    fn drop(&mut self) {
        drop(self.original.take());
        self.exit_waker.wake_by_ref();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::Future,
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Wake, Waker},
    };
    struct Cell {
        wake: Mutex<Waker>,
        closed: AtomicUsize,
        cleanup: Mutex<Option<ConnectorIoCleanupFuture>>,
    }
    struct ExitObservation {
        original: Weak<Cell>,
        strong: AtomicUsize,
        weak: AtomicUsize,
    }
    impl Wake for ExitObservation {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            // Observation only. Production uniqueness still uses Arc::get_mut.
            self.strong
                .store(self.original.strong_count(), Ordering::SeqCst);
            self.weak
                .store(self.original.weak_count(), Ordering::SeqCst);
        }
    }
    impl ConnectorIoRequestSource for Cell {
        fn exit_waker(&self) -> Waker {
            self.wake.lock().unwrap().clone()
        }
        fn reserve_stream(&self) -> Result<(), ConnectorError> {
            Ok(())
        }
        fn stream_closed(&self) {
            self.closed.fetch_add(1, Ordering::SeqCst);
        }
        fn install_stream_cleanup_in(
            &self,
            source: &mut Option<ConnectorIoCleanupFuture>,
        ) -> Result<(), ConnectorError> {
            let mut target = self.cleanup.lock().unwrap();
            if target.is_some() || source.is_none() {
                return Err(source_gone());
            }
            *target = source.take();
            Ok(())
        }
        fn record_cleanup_failure(
            &self,
            cause: Box<dyn std::any::Any + Send>,
        ) -> Result<(), Box<dyn std::any::Any + Send>> {
            Err(cause)
        }
        fn stream_custody_lost(&self) {
            self.closed.fetch_add(100, Ordering::SeqCst);
        }
        fn attach_original_progress(
            &self,
            _: Arc<dyn ConnectorIoProgressSource>,
        ) -> Result<(), ConnectorError> {
            Ok(())
        }
        fn poll_admitted(&self, _: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
            Poll::Ready(Ok(()))
        }
        fn poll_include_dependency(
            &self,
            _: &SharedDependency,
            _: &mut Context<'_>,
        ) -> Poll<Result<(), ConnectorError>> {
            Poll::Ready(Ok(()))
        }
    }
    fn admitted(cell: &Arc<Cell>) -> ConnectorIoPermit {
        let request = ConnectorIoRequest::from_original(
            Arc::clone(cell) as Arc<dyn ConnectorIoRequestSource>
        );
        let mut future = std::pin::pin!(request.admitted());
        match future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(Ok(p)) => p,
            _ => panic!("carrier fixture admission"),
        }
    }
    #[test]
    fn exit_notification_follows_actual_strong_and_weak_cell_alias_destruction() {
        let cell = Arc::new(Cell {
            wake: Mutex::new(Waker::noop().clone()),
            closed: AtomicUsize::new(0),
            cleanup: Mutex::new(None),
        });
        let seen = Arc::new(ExitObservation {
            original: Arc::downgrade(&cell),
            strong: AtomicUsize::new(0),
            weak: AtomicUsize::new(0),
        });
        *cell.wake.lock().unwrap() = Waker::from(Arc::clone(&seen));
        let permit = admitted(&cell);
        let weak = permit.weak_phase();
        drop(permit);
        assert_eq!(seen.strong.load(Ordering::SeqCst), 1);
        assert_eq!(seen.weak.load(Ordering::SeqCst), 2);
        drop(weak);
        assert_eq!(seen.strong.load(Ordering::SeqCst), 1);
        assert_eq!(seen.weak.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn original_stream_cleanup_moves_once_and_refusal_preserves_both_sources() {
        let cell = Arc::new(Cell {
            wake: Mutex::new(Waker::noop().clone()),
            closed: AtomicUsize::new(0),
            cleanup: Mutex::new(None),
        });
        let permit = admitted(&cell);
        let stream = permit.begin_stream().unwrap();
        let mut first: Option<ConnectorIoCleanupFuture> = Some(Box::pin(async { Ok(()) }));
        assert!(stream.install_cleanup_in(&mut first).is_ok());
        assert!(first.is_none());
        let stream = permit.begin_stream().unwrap();
        let mut second: Option<ConnectorIoCleanupFuture> = Some(Box::pin(async { Ok(()) }));
        let (same, error) = match stream.install_cleanup_in(&mut second) {
            Err(value) => value,
            Ok(()) => panic!("occupied original cleanup"),
        };
        assert!(second.is_some());
        assert!(cell.cleanup.lock().unwrap().is_some());
        drop(error);
        // Explicit original disposition, not cancellation or an absent future.
        drop(cell.cleanup.lock().unwrap().take());
        same.closed();
        assert_eq!(cell.closed.load(Ordering::SeqCst), 1);
    }
}
