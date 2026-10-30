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

//! Same original Cell/Source completion path. No Work grant or network claim
//! is fabricated by this component fixture; real OpenDAL abort is tested in FS.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

struct OriginalCause {
    ordinal: usize,
    drops: Arc<AtomicUsize>,
    panic_on_drop: bool,
}
impl Drop for OriginalCause {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        if self.panic_on_drop {
            std::panic::panic_any(193usize);
        }
    }
}
struct ExitedAbort {
    original: std::sync::Weak<RequestCell>,
    cause: Option<OriginalCause>,
    ordinal: usize,
    actual_exit: Arc<AtomicUsize>,
}
impl Future for ExitedAbort {
    type Output = Result<(), ConnectorError>;
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        let source = self.original.upgrade().unwrap();
        let cause = self.cause.take().expect("same abort polled once");
        if let Err(cause) = source.record_cleanup_failure(Box::new(cause)) {
            std::panic::panic_any(cause);
        }
        Poll::Ready(Err(ConnectorError::new(
            ConnectorErrorKind::Unavailable,
            format!("original abort {} failed", self.ordinal),
        )))
    }
}
impl Drop for ExitedAbort {
    fn drop(&mut self) {
        self.actual_exit.fetch_add(1, Ordering::SeqCst);
    }
}

pub(in crate::query_execution::maintenance) fn two_failures(
    secondary_drop_panic: bool,
) -> (Arc<MetadataIoAdmission>, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let wake = Arc::new(MetadataWake::new());
    let source = Arc::new(MetadataIoAdmission {
        wake: Arc::clone(&wake),
        original: Mutex::new(None),
        payload_retention: OnceLock::new(),
        settlement: AtomicBool::new(false),
        cleanup: Mutex::new(CleanupReceipt::default()),
    });
    let mut cell = Arc::new(RequestCell {
        original: Mutex::new(Some(CellBody::default())),
        stream: Mutex::new(Some(StreamBody::default())),
        progress: Mutex::new(None),
        cleanup_error: Mutex::new(None),
        observer: AtomicWaker::new(),
        root: wake,
    });
    let first = Arc::new(AtomicUsize::new(0));
    let second = Arc::new(AtomicUsize::new(0));
    let exits = Arc::new(AtomicUsize::new(0));
    for ordinal in 1..=2 {
        let future: ConnectorIoCleanupFuture = Box::pin(ExitedAbort {
            original: Arc::downgrade(&cell),
            cause: Some(OriginalCause {
                ordinal,
                drops: if ordinal == 1 {
                    Arc::clone(&first)
                } else {
                    Arc::clone(&second)
                },
                panic_on_drop: ordinal == 2 && secondary_drop_panic,
            }),
            ordinal,
            actual_exit: Arc::clone(&exits),
        });
        cell.reserve_stream().unwrap();
        let mut pending = Some(future);
        cell.install_stream_cleanup_in(&mut pending).unwrap();
        assert!(
            pending.is_none(),
            "actual original cleanup publication moved once"
        );
        assert!(cell.poll_original(&mut Context::from_waker(std::task::Waker::noop())));
        assert_eq!(
            exits.load(Ordering::SeqCst),
            ordinal,
            "actual cleanup F Drop, not terminal/cancelled evidence"
        );
        assert!(
            !cell.stream_clear(),
            "unreceived ordinary error must prevent reuse"
        );
        assert!(source.receive_cleanup_completion(&cell));
        assert!(
            !source.receive_cleanup_completion(&cell),
            "same completion delivered once"
        );
        assert!(cell.cleanup_error.lock().unwrap().is_none());
        assert!(
            cell.stream_clear(),
            "received domain error is actually exited, not a fake Running state"
        );
        assert!(
            Arc::get_mut(&mut cell).is_some(),
            "same original cell aliases truly exited before reuse"
        );
    }
    let receipt = source.cleanup.lock().unwrap();
    assert_eq!(
        receipt.first.as_ref().unwrap().message(),
        "original abort 1 failed"
    );
    assert_eq!(
        receipt
            .original
            .as_ref()
            .unwrap()
            .downcast_ref::<OriginalCause>()
            .unwrap()
            .ordinal,
        1
    );
    assert_eq!(receipt.rejected_drop_panic.is_some(), secondary_drop_panic);
    drop(receipt);
    assert_eq!(first.load(Ordering::SeqCst), 0);
    assert_eq!(second.load(Ordering::SeqCst), 1);
    (source, first, second)
}
#[test]
fn consecutive_exited_abort_errors_are_received_once_and_reuse_same_cell() {
    let (source, first, second) = two_failures(false);
    assert!(source.health_failure().is_none());
    drop(source);
    assert_eq!(first.load(Ordering::SeqCst), 1);
    assert_eq!(second.load(Ordering::SeqCst), 1);
}
#[test]
fn secondary_cause_drop_panic_preserves_primary_and_actual_exit() {
    let (source, first, _) = two_failures(true);
    assert!(source.health_failure().is_some());
    let receipt = source.cleanup.lock().unwrap();
    assert_eq!(
        receipt
            .rejected_drop_panic
            .as_ref()
            .unwrap()
            .downcast_ref::<usize>(),
        Some(&193)
    );
    drop(receipt);
    assert!(
        source
            .request_in(Some(SharedDependency::LocalHost), None)
            .is_err()
    );
    drop(source);
    assert_eq!(first.load(Ordering::SeqCst), 1);
}
