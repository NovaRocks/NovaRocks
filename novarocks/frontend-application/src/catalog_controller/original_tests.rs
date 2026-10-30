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

use super::*;
use crate::state_store::testing::StateStoreHost;
use novarocks_catalog_application::CatalogAttachmentRepository;
use novarocks_state_store_runtime::StateStoreRunPolicy;
use std::{
    future::Future,
    sync::atomic::AtomicUsize,
    task::{Context, Poll, Waker},
    time::Instant as StdInstant,
};

async fn fixture() -> (
    tempfile::TempDir,
    StateStoreHost,
    Arc<FrontendCatalogController>,
) {
    // Existing explicit legacy setup only. The tested work is the Controller's
    // actual Tokio Join ownership, not Catalog or SQLite execution governance.
    let (directory, host, store) = super::tests::open_store().await;
    let repository =
        CatalogAttachmentRepository::open(store.clone(), StateStoreRunPolicy::default())
            .await
            .expect("existing controller repository fixture");
    let (_control, projection, _) = super::tests::projection(repository);
    let controller =
        FrontendCatalogController::new(store, projection, CatalogProjectionConfig::default())
            .expect("existing controller fixture");
    (directory, host, controller)
}
struct TaskExit(Arc<AtomicBool>);
impl Drop for TaskExit {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
#[tokio::test(flavor = "current_thread")]
async fn cancelled_shutdown_and_outer_timeout_reborrow_the_same_installed_join() {
    let (_directory, mut host, controller) = fixture().await;
    let (release, held) = tokio::sync::oneshot::channel::<()>();
    let (entered, running) = tokio::sync::oneshot::channel();
    let exited = Arc::new(AtomicBool::new(false));
    let actual_exit = exited.clone();
    let task = tokio::spawn(async move {
        let _actual_exit = TaskExit(actual_exit);
        let _ = entered.send(());
        let _ = held.await;
    });
    let identity = task.id();
    *controller.worker.lock().await = Some(task);
    tokio::time::timeout(Duration::from_secs(5), running)
        .await
        .expect("actual task starts")
        .unwrap();
    let mut shutdown = Box::pin(controller.shutdown());
    assert!(matches!(
        shutdown
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
    drop(shutdown); // Cancels only this borrowed join waiter.
    {
        let worker = controller
            .worker
            .try_lock()
            .expect("cancelled waiter releases its borrow");
        let same = worker
            .as_ref()
            .expect("same handle remains in original field");
        assert_eq!(same.id(), identity);
        assert!(!same.is_finished());
    }
    assert!(!exited.load(Ordering::Acquire));
    // The gate, not this short timeout, guarantees the task is still running.
    // No timing measurement or second submission is used as exit evidence.
    assert!(
        tokio::time::timeout(Duration::from_millis(1), controller.shutdown())
            .await
            .is_err()
    );
    {
        let worker = controller
            .worker
            .try_lock()
            .expect("outer cancellation releases the same borrow");
        let same = worker
            .as_ref()
            .expect("outer timeout cannot detach the original task");
        assert_eq!(same.id(), identity);
        assert!(!same.is_finished());
    }
    assert!(!exited.load(Ordering::Acquire));
    assert!(controller.worker_failure.lock().unwrap().is_none());
    release
        .send(())
        .expect("same original task was never aborted or replaced");
    controller
        .shutdown()
        .await
        .expect("same task actually joins after release");
    assert!(exited.load(Ordering::Acquire));
    assert!(controller.worker.lock().await.is_none());
    assert!(controller.worker_failure.lock().unwrap().is_none());
    drop(controller);
    host.shutdown(StdInstant::now() + Duration::from_secs(5))
        .await
        .expect("original fixture store closes");
}
#[derive(Debug)]
struct OriginalJoinPanic;
#[tokio::test(flavor = "current_thread")]
async fn failed_original_join_keeps_same_raw_error_without_repoll_or_replacement() {
    let (_directory, mut host, controller) = fixture().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let actual_calls = calls.clone();
    let task = tokio::spawn(async move {
        actual_calls.fetch_add(1, Ordering::AcqRel);
        std::panic::panic_any(OriginalJoinPanic);
    });
    *controller.worker.lock().await = Some(task);
    assert!(controller.shutdown().await.is_err());
    assert!(controller.worker.lock().await.is_none());
    let raw = controller
        .worker_failure
        .lock()
        .unwrap()
        .as_ref()
        .expect("original raw JoinError retained")
        .clone();
    assert!(raw.is_panic());
    assert!(controller.shutdown().await.is_err());
    assert!(controller.worker.lock().await.is_none());
    assert!(Arc::ptr_eq(
        &raw,
        controller.worker_failure.lock().unwrap().as_ref().unwrap()
    ));
    assert_eq!(calls.load(Ordering::Acquire), 1);
    drop(raw);
    drop(controller);
    host.shutdown(StdInstant::now() + Duration::from_secs(5))
        .await
        .expect("original fixture store closes");
}
