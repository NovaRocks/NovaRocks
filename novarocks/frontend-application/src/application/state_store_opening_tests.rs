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

//! Host close-field composition with the real FA port and QA ordinary worker.
//! The private operation only drops its own resource; it is not SQLite logic.
use super::*;
use futures::task::AtomicWaker;
use novarocks_query_application::cpu::{QueryBlockingExecutorConfig, QueryBlockingExecutorOwner};
use novarocks_state_store_api::{
    InstanceScope, StateStoreBlockingExit as Exit, StateStoreBlockingOrigin as Origin,
    StateStoreBlockingOriginal as Original, StateStoreBlockingPhase as Phase,
    StateStoreBlockingRequest as Request, StateStoreBlockingSlot,
    StateStoreBlockingSourceVocabulary, StateStoreError, StateStoreErrorKind, StateStoreProviderId,
};
use std::{
    num::{NonZeroU64, NonZeroUsize},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    time::Duration,
};
use tokio::sync::Notify;

struct Resource(Arc<AtomicUsize>);
impl Drop for Resource {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
struct Gate {
    open: Mutex<bool>,
    condition: Condvar,
    entered: AtomicBool,
    notice: Notify,
}
impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            open: Mutex::new(false),
            condition: Condvar::new(),
            entered: AtomicBool::new(false),
            notice: Notify::new(),
        })
    }
    fn wait(&self) {
        self.entered.store(true, Ordering::Release);
        self.notice.notify_one();
        let mut open = self.open.lock().unwrap();
        while !*open {
            open = self.condition.wait(open).unwrap();
        }
    }
    fn release(&self) {
        *self.open.lock().unwrap() = true;
        self.condition.notify_all();
    }
    async fn reached(&self) {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let noticed = self.notice.notified();
                tokio::pin!(noticed);
                noticed.as_mut().enable();
                if self.entered.load(Ordering::Acquire) {
                    return;
                }
                noticed.await;
            }
        })
        .await
        .expect("actual ordinary worker reached its rescueable gate");
    }
}
struct Rescue(Arc<Gate>);
impl Drop for Rescue {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct CloseOriginal {
    scope: InstanceScope,
    resource: Mutex<Option<Arc<Resource>>>,
    request: Mutex<Option<Request>>,
    closing: AtomicBool,
    calls: AtomicUsize,
    close_requests: AtomicUsize,
    actual_exits: AtomicUsize,
    completed: AtomicBool,
    failure: Mutex<Option<Failure>>,
    receiver_wake: AtomicWaker,
    // A test observer releases its actual pin only after the original policy
    // has retried. Releasing this pin deliberately sends no Source wake.
    release_pin: Mutex<Option<mpsc::Sender<()>>>,
    health_gate: Option<Arc<Gate>>,
}
impl Original for CloseOriginal {
    fn origin(&self) -> Origin {
        Origin::Instance(self.scope)
    }
    fn requested(&self) -> Option<Request> {
        if self.completed.load(Ordering::Acquire) {
            return None;
        }
        let mut requested = self.request.lock().unwrap();
        if requested.is_some() {
            return *requested;
        }
        let sole = self
            .resource
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|resource| Arc::strong_count(resource) == 1);
        if self.closing.load(Ordering::Acquire) && sole {
            *requested = Some(Request {
                phase: Phase::Close,
                revision: NonZeroU64::new(1).unwrap(),
            });
        }
        *requested
    }
    fn publication_refusal(&self, _: Request) -> Option<Failure> {
        None
    }
    fn before_publish(&self, request: Request) -> Result<(), Failure> {
        assert_eq!(Some(request), *self.request.lock().unwrap());
        Ok(())
    }
    fn run_original_phase(&self, request: Request) {
        assert_eq!(Some(request), *self.request.lock().unwrap());
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(gate) = &self.health_gate {
            // Original health is visible before the real worker returns. Its
            // receiver must not stand in for QA's still-unreceived footer.
            *self.failure.lock().unwrap() = Some(
                StateStoreError::new(
                    StateStoreErrorKind::Internal,
                    "original close fixture failed",
                )
                .into_original_failure(),
            );
            self.receiver_wake.wake();
            gate.wait();
        }
        let resource = self
            .resource
            .lock()
            .unwrap()
            .take()
            .expect("same original resource");
        match Arc::try_unwrap(resource) {
            Ok(resource) => drop(resource),
            Err(resource) => {
                *self.resource.lock().unwrap() = Some(resource);
                *self.failure.lock().unwrap() = Some(
                    StateStoreError::new(
                        StateStoreErrorKind::Internal,
                        "close fixture resource was still pinned",
                    )
                    .into_original_failure(),
                );
            }
        }
    }
    fn receive_result(&self, request: Request, result: Result<(), Failure>) {
        assert_eq!(Some(request), *self.request.lock().unwrap());
        if let Err(cause) = result {
            self.failure.lock().unwrap().get_or_insert(cause);
        }
        self.receiver_wake.wake();
    }
    fn receive_exit(&self, request: Request, exit: Result<Exit, Failure>) {
        assert_eq!(Some(request), *self.request.lock().unwrap());
        match exit {
            Ok(Exit::ActualExited) => {
                self.actual_exits.fetch_add(1, Ordering::SeqCst);
                if self.failure.lock().unwrap().is_none() && self.resource.lock().unwrap().is_none()
                {
                    self.completed.store(true, Ordering::Release);
                }
            }
            Ok(Exit::NeverPublishedReturned) => {
                self.failure.lock().unwrap().get_or_insert_with(|| {
                    StateStoreError::new(
                        StateStoreErrorKind::Internal,
                        "required close did not execute",
                    )
                    .into_original_failure()
                });
            }
            Err(cause) => {
                self.failure.lock().unwrap().get_or_insert(cause);
            }
        }
        self.receiver_wake.wake();
    }
    fn is_complete(&self) -> bool {
        self.completed.load(Ordering::Acquire)
    }
    fn local_failure(&self) -> Option<Failure> {
        self.failure.lock().unwrap().clone()
    }
    fn request_close(&self) {
        self.closing.store(true, Ordering::Release);
        if self.close_requests.fetch_add(1, Ordering::SeqCst) + 1 >= 2 {
            let observer = self.release_pin.lock().unwrap().take();
            if let Some(observer) = observer {
                let _ = observer.send(());
            }
        }
    }
    fn request_interrupt(&self) {
        self.request_close();
    }
}
struct CloseReceiver {
    original: Arc<CloseOriginal>,
    _borrower: Box<dyn StateStoreBlockingSlot>,
}
impl StateStoreOriginalClose for CloseReceiver {
    fn poll_closed(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Failure>> {
        self.original.receiver_wake.register(cx.waker());
        if let Some(cause) = self.original.local_failure() {
            return Poll::Ready(Err(cause));
        }
        if self.original.is_complete() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}
fn fixture(
    health_gate: Option<Arc<Gate>>,
    release_pin: Option<mpsc::Sender<()>>,
) -> (
    QueryBlockingExecutorOwner,
    InstanceClose,
    Arc<CloseOriginal>,
    Arc<Resource>,
    Arc<AtomicUsize>,
) {
    let executor_owner = QueryBlockingExecutorOwner::try_new(QueryBlockingExecutorConfig::new(
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(1).unwrap(),
    ))
    .unwrap();
    let executor = executor_owner.executor();
    let drops = Arc::new(AtomicUsize::new(0));
    let resource = Arc::new(Resource(drops.clone()));
    let original = Arc::new(CloseOriginal {
        scope: InstanceScope::new(),
        resource: Mutex::new(Some(resource.clone())),
        request: Mutex::new(None),
        closing: AtomicBool::new(false),
        calls: AtomicUsize::new(0),
        close_requests: AtomicUsize::new(0),
        actual_exits: AtomicUsize::new(0),
        completed: AtomicBool::new(false),
        failure: Mutex::new(None),
        receiver_wake: AtomicWaker::new(),
        release_pin: Mutex::new(release_pin),
        health_gate,
    });
    let provider = StateStoreProviderId::new("host-close-fixture");
    let vocabulary = StateStoreBlockingSourceVocabulary::for_provider::<
        CloseOriginal,
        CloseOriginal,
        CloseOriginal,
    >(provider);
    let owner = StateStoreOriginalOwner::closing_instance(
        &executor,
        vocabulary,
        original.scope,
        StateStoreRunPolicy::default(),
    )
    .unwrap();
    let context = owner.context(Instant::now() + Duration::from_secs(5));
    let source: Arc<dyn Original> = original.clone();
    let mut borrower = None;
    context.bind_original_in(&source, &mut borrower).unwrap();
    let receiver = Box::new(CloseReceiver {
        original: original.clone(),
        _borrower: borrower.unwrap(),
    });
    let close = InstanceClose {
        owner,
        original: Some(receiver),
        receiver_closed: false,
        receiver_failure: None,
    };
    (executor_owner, close, original, resource, drops)
}
async fn join_worker(owner: &mut QueryBlockingExecutorOwner) {
    owner
        .shutdown_until(Instant::now() + Duration::from_secs(3))
        .await
        .expect("actual ordinary worker joined");
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_pin_release_advances_by_original_owner_backoff_without_source_wake() {
    let (release_tx, release_rx) = mpsc::channel();
    let (mut executor, mut close, original, pin, drops) = fixture(None, Some(release_tx));
    // This is an external observer thread, not another execution worker or
    // queue. It cannot poll Source or wake the borrowed Host close future.
    let observer = std::thread::spawn(move || {
        let retried = release_rx.recv_timeout(Duration::from_secs(1)).is_ok();
        drop(pin);
        retried
    });
    let mut ready = None;
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        std::future::poll_fn(|cx| close.poll_closed(&mut ready, cx)),
    )
    .await;
    let retried = observer.join().unwrap();
    join_worker(&mut executor).await;
    assert!(matches!(result, Ok(Ok(()))), "{result:?}");
    assert!(
        retried,
        "the original owner must install and fire its own backoff"
    );
    assert!(original.close_requests.load(Ordering::SeqCst) >= 2);
    assert_eq!(original.calls.load(Ordering::SeqCst), 1);
    assert_eq!(original.actual_exits.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn early_original_close_failure_is_retained_until_the_same_actual_footer() {
    let gate = Gate::new();
    let _rescue = Rescue(gate.clone());
    let (mut executor, mut close, original, pin, drops) = fixture(Some(gate.clone()), None);
    drop(pin);
    let mut ready = None;
    let mut first = Box::pin(std::future::poll_fn(|cx| close.poll_closed(&mut ready, cx)));
    let early = tokio::select! {
        result=&mut first=>Some(result),
        _=gate.reached()=>None,
    };
    let still_pending = if early.is_none() {
        std::future::poll_fn(|cx| {
            Poll::Ready(std::future::Future::poll(first.as_mut(), cx).is_pending())
        })
        .await
    } else {
        false
    };
    drop(first);
    let retained = close.receiver_failure.clone();
    let origin = original.local_failure();
    let exited_before_release = original.actual_exits.load(Ordering::SeqCst);
    let receiver_still_installed = close.original.is_some();
    gate.release();
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        std::future::poll_fn(|cx| close.poll_closed(&mut ready, cx)),
    )
    .await;
    join_worker(&mut executor).await;
    assert!(
        early.is_none(),
        "receiver error cannot end the Host wait before QA footer: {early:?}"
    );
    assert!(still_pending);
    assert!(receiver_still_installed);
    assert_eq!(exited_before_release, 0);
    assert_eq!(retained, origin);
    assert!(retained.is_some());
    let reported = result
        .expect("footer must wake the original close borrower")
        .expect_err("actual original failure survives");
    assert_eq!(Some(reported), origin);
    assert_eq!(original.actual_exits.load(Ordering::SeqCst), 1);
    assert_eq!(original.calls.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[path = "state_store_opening_tests/sqlite_composition.rs"]
mod sqlite_composition;
