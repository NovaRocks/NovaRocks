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
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    task::{Wake, Waker},
    time::Duration,
};

fn owner() -> QueryCpuExecutorOwner {
    QueryCpuExecutorOwner::try_new(QueryCpuExecutorConfig::with_idle_keepalive(
        Duration::from_secs(1),
    ))
    .unwrap()
}
struct Notice(Arc<AtomicUsize>);
impl Drop for Notice {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}
struct PausedWake {
    entered: mpsc::SyncSender<()>,
    release: Mutex<mpsc::Receiver<()>>,
    once: AtomicBool,
}
impl Wake for PausedWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if !self.once.swap(true, Ordering::AcqRel) {
            self.entered.send(()).unwrap();
            self.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
        }
    }
}

#[tokio::test]
async fn same_worker_result_precedes_actual_exit_and_retains_original_custody() {
    let mut owner = owner();
    let executor = owner.executor();
    let drops = Arc::new(AtomicUsize::new(0));
    let custody = Arc::new(Notice(Arc::clone(&drops)));
    let (entered, arrived) = mpsc::sync_channel(1);
    let (release, released) = mpsc::sync_channel(1);
    let wake = Waker::from(Arc::new(PausedWake {
        entered,
        release: Mutex::new(released),
        once: AtomicBool::new(false),
    }));
    let mut cx = Context::from_waker(&wake);
    let mut receipt = None;
    let mut prepared = Some(
        executor
            .prepare_cpu_preserving_in(|| 17usize, &custody, &mut receipt)
            .unwrap(),
    );
    drop(custody);
    assert!(receipt.as_mut().unwrap().poll_result(&mut cx).is_pending());
    executor.publish_cpu_preserving_in(&mut prepared).unwrap();
    assert!(prepared.is_none());
    arrived.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(
        receipt.as_mut().unwrap().poll_result(&mut cx),
        Poll::Ready(Ok(17))
    );
    assert!(
        receipt
            .as_ref()
            .unwrap()
            .poll_actual_exit(&mut cx)
            .is_pending()
    );
    assert_eq!(drops.load(Ordering::Acquire), 0);
    release.send(()).unwrap();
    std::future::poll_fn(|cx| receipt.as_ref().unwrap().poll_actual_exit(cx))
        .await
        .unwrap();
    assert_eq!(drops.load(Ordering::Acquire), 1);
    owner
        .shutdown_until(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
}

#[tokio::test]
async fn close_and_foreign_pool_leave_the_same_unexecuted_prepared_input() {
    let mut first = owner();
    let mut second = owner();
    let executor = first.executor();
    let ran = Arc::new(AtomicUsize::new(0));
    let entered = Arc::clone(&ran);
    let custody = Arc::new(());
    let mut receipt = None;
    let mut prepared = Some(
        executor
            .prepare_cpu_preserving_in(
                move || {
                    entered.fetch_add(1, Ordering::AcqRel);
                },
                &custody,
                &mut receipt,
            )
            .unwrap(),
    );
    assert!(matches!(
        second.executor().publish_cpu_preserving_in(&mut prepared),
        Err(QueryCpuPublishRefusal::ForeignPool)
    ));
    assert!(prepared.is_some());
    assert_eq!(ran.load(Ordering::Acquire), 0);
    first.request_shutdown_for_process_exit();
    assert!(matches!(
        executor.publish_cpu_preserving_in(&mut prepared),
        Err(QueryCpuPublishRefusal::Closed)
    ));
    assert!(prepared.is_some());
    assert_eq!(ran.load(Ordering::Acquire), 0);
    drop(prepared);
    drop(receipt);
    first
        .shutdown_until(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    second
        .shutdown_until(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
}

#[tokio::test]
async fn caught_call_panic_keeps_same_raw_cause_while_worker_really_exits() {
    struct Original(Arc<AtomicUsize>);
    impl Drop for Original {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }
    let mut owner = owner();
    let executor = owner.executor();
    let drops = Arc::new(AtomicUsize::new(0));
    let original = Original(Arc::clone(&drops));
    let custody = Arc::new(());
    let mut receipt = None;
    let mut prepared = Some(
        executor
            .prepare_cpu_preserving_in(
                move || -> () { std::panic::panic_any(original) },
                &custody,
                &mut receipt,
            )
            .unwrap(),
    );
    executor.publish_cpu_preserving_in(&mut prepared).unwrap();
    let failure = std::future::poll_fn(|cx| receipt.as_mut().unwrap().poll_result(cx))
        .await
        .unwrap_err();
    assert!(failure.original_payload_is::<Original>());
    assert_eq!(drops.load(Ordering::Acquire), 0);
    std::future::poll_fn(|cx| receipt.as_ref().unwrap().poll_actual_exit(cx))
        .await
        .unwrap();
    owner
        .shutdown_until(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(drops.load(Ordering::Acquire), 0);
    drop(failure);
    assert_eq!(drops.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn notification_panic_stays_in_original_failed_worker_after_its_join() {
    struct Original(Arc<AtomicUsize>);
    impl Drop for Original {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }
    struct PanicWake(Arc<AtomicUsize>);
    impl Wake for PanicWake {
        fn wake(self: Arc<Self>) {
            std::panic::panic_any(Original(Arc::clone(&self.0)));
        }
    }

    let mut owner = owner();
    let executor = owner.executor();
    let drops = Arc::new(AtomicUsize::new(0));
    let custody = Arc::new(());
    let mut receipt = None;
    let mut prepared = Some(
        executor
            .prepare_cpu_preserving_in(|| 29usize, &custody, &mut receipt)
            .unwrap(),
    );
    let waker = Waker::from(Arc::new(PanicWake(Arc::clone(&drops))));
    assert!(
        receipt
            .as_mut()
            .unwrap()
            .poll_result(&mut Context::from_waker(&waker))
            .is_pending()
    );
    executor.publish_cpu_preserving_in(&mut prepared).unwrap();
    assert_eq!(
        std::future::poll_fn(|cx| receipt.as_ref().unwrap().poll_actual_exit(cx)).await,
        Err(QueryBlockingActualExitFailure::FailedUnproven)
    );
    let original = receipt.as_ref().unwrap().actual_exit_failure().unwrap();
    assert!(original.original_payload_is::<Original>());

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        executor.pool.reap_finished();
        if executor.pool.snapshot().unjoined_starts == 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "original worker join did not exit"
        );
        tokio::task::yield_now().await;
    }
    assert!(
        executor
            .pool
            .state
            .lock()
            .unwrap()
            .workers
            .values()
            .any(|worker| matches!(worker, ElasticWorkerState::Failed { .. }))
    );
    assert!(owner.shutdown_until(Instant::now()).await.is_err());
    drop(receipt);
    drop(original);
    assert_eq!(drops.load(Ordering::Acquire), 0);
    // Explicit test-owner disposition of the failed process-local pool is
    // distinct from a successful shutdown or actual action-exit proof.
    drop(executor);
    drop(owner);
    assert_eq!(drops.load(Ordering::Acquire), 1);
}
