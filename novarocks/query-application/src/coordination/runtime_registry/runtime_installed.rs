// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Registry custody of one FE-runtime registration and one ordered Registry
//! source. Borrowed waiter cancellation never drops the original send future.
use super::*;
use crate::api::logical_runtime::{
    InstalledLogicalExecution as InstalledRuntime, LogicalExit, LogicalFactSource, LogicalFuture,
    LogicalHostFact, LogicalJoinReadiness, LogicalSourceClosed,
};
use std::{
    collections::VecDeque,
    task::{Context, Waker},
};

#[derive(Clone, Debug)]
pub(super) enum RuntimeReplyValue {
    Fact,
    Readiness(LogicalJoinReadiness),
}
pub(super) struct RuntimeReply {
    value: Mutex<Option<Result<RuntimeReplyValue, LogicalSourceClosed>>>,
    changed: futures::task::AtomicWaker,
}
impl RuntimeReply {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            value: Mutex::new(None),
            changed: futures::task::AtomicWaker::new(),
        })
    }
    fn publish(&self, value: Result<RuntimeReplyValue, LogicalSourceClosed>) {
        *self.value.lock().unwrap_or_else(|p| p.into_inner()) = Some(value);
        self.changed.wake();
    }
}
enum Command {
    Fact(LogicalHostFact),
    Readiness(bool),
}
struct Queued {
    command: Command,
    reply: Arc<RuntimeReply>,
}
type SendJob = LogicalFuture<
    'static,
    (
        Box<dyn LogicalFactSource>,
        Result<RuntimeReplyValue, LogicalSourceClosed>,
    ),
>;
struct PendingSend {
    future: SendJob,
    reply: Arc<RuntimeReply>,
}
struct SourceState {
    source: Option<Box<dyn LogicalFactSource>>,
    queue: VecDeque<Queued>,
    pending: Option<PendingSend>,
    polling: bool,
    waiting: bool,
}
struct ExitState {
    value: Option<LogicalExit>,
    polling: bool,
    waiting: bool,
}

#[derive(Default)]
struct NaturalJoinState {
    reply: Option<Arc<RuntimeReply>>,
    waiting_epoch: Option<u64>,
    query_epoch: u64,
    closing: bool,
    protocol_ready: bool,
    ready: bool,
    closed: bool,
    polling: bool,
    waiting: bool,
}

struct RuntimeWake {
    registry: std::sync::Weak<RegistryInner>,
    forward: futures::task::AtomicWaker,
    changed: Notify,
    epoch: AtomicU64,
}
impl std::task::Wake for RuntimeWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if let Some(registry) = self.registry.upgrade() {
            registry.original_exit_ready.store(true, Ordering::Release);
            registry.progress.notify_waiters();
        }
        self.changed.notify_waiters();
        self.forward.wake();
    }
}

/// The installed handle uses a separate observer for protocol-readiness
/// transitions/final exit. Reply delivery and local queue wakes cannot make
/// a negative readiness query busy-poll itself.
struct RuntimeProgressWake(Arc<RuntimeWake>);
impl std::task::Wake for RuntimeProgressWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.epoch.fetch_add(1, Ordering::AcqRel);
        std::task::Wake::wake_by_ref(&self.0);
    }
}

pub(super) struct RuntimeCarrier {
    pub(super) installed: Arc<dyn InstalledRuntime>,
    source: Mutex<SourceState>,
    exit: Mutex<ExitState>,
    natural_join: Mutex<NaturalJoinState>,
    explicit_join: AtomicBool,
    wake: Arc<RuntimeWake>,
    exit_progress: Arc<RuntimeProgressWake>,
}
impl fmt::Debug for RuntimeCarrier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RuntimeCarrier(original registration)")
    }
}

impl RuntimeCarrier {
    pub(super) fn new(
        installed: Arc<dyn InstalledRuntime>,
        source: Box<dyn LogicalFactSource>,
        registry: std::sync::Weak<RegistryInner>,
    ) -> Arc<Self> {
        let wake = Arc::new(RuntimeWake {
            registry,
            forward: futures::task::AtomicWaker::new(),
            changed: Notify::new(),
            epoch: AtomicU64::new(0),
        });
        Arc::new(Self {
            installed,
            source: Mutex::new(SourceState {
                source: Some(source),
                queue: VecDeque::new(),
                pending: None,
                polling: false,
                waiting: false,
            }),
            exit: Mutex::new(ExitState {
                value: None,
                polling: false,
                waiting: false,
            }),
            natural_join: Mutex::new(NaturalJoinState::default()),
            explicit_join: AtomicBool::new(false),
            exit_progress: Arc::new(RuntimeProgressWake(wake.clone())),
            wake,
        })
    }
    fn enqueue(&self, command: Command) -> Arc<RuntimeReply> {
        let reply = RuntimeReply::new();
        self.source
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .queue
            .push_back(Queued {
                command,
                reply: reply.clone(),
            });
        std::task::Wake::wake_by_ref(&self.wake);
        reply
    }
    pub(super) fn enqueue_fact(&self, fact: LogicalHostFact) -> Arc<RuntimeReply> {
        self.enqueue(Command::Fact(fact))
    }
    pub(super) fn enqueue_readiness(&self, close: bool) -> Arc<RuntimeReply> {
        self.enqueue(Command::Readiness(close))
    }

    pub(super) fn poll_reply(
        &self,
        reply: &RuntimeReply,
        cx: &mut Context<'_>,
    ) -> Poll<Result<RuntimeReplyValue, LogicalSourceClosed>> {
        reply.changed.register(cx.waker());
        self.poll_source(cx);
        reply
            .value
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
            .map_or(Poll::Pending, Poll::Ready)
    }

    /// One original source operation per invocation. Polling occurs outside
    /// both Registry and source locks; the loan restores the SAME future.
    pub(super) fn poll_source(&self, cx: &mut Context<'_>) {
        self.wake.forward.register(cx.waker());
        let next = {
            let mut state = self.source.lock().unwrap_or_else(|p| p.into_inner());
            if state.polling {
                state.waiting = true;
                return;
            }
            if let Some(pending) = state.pending.take() {
                state.polling = true;
                Some(pending)
            } else if let Some(queued) = state.queue.pop_front() {
                let mut source = state.source.take().expect("one original Registry source");
                let installed = self.installed.clone();
                let future = Box::pin(async move {
                    let result = match queued.command {
                        Command::Fact(fact) => {
                            source.send(fact).await.map(|()| RuntimeReplyValue::Fact)
                        }
                        Command::Readiness(close) => installed
                            .join_readiness(close)
                            .await
                            .map(RuntimeReplyValue::Readiness),
                    };
                    (source, result)
                });
                state.polling = true;
                Some(PendingSend {
                    future,
                    reply: queued.reply,
                })
            } else {
                None
            }
        };
        let Some(pending) = next else {
            return;
        };
        let mut loan = SourceLoan {
            carrier: self,
            pending: Some(pending),
        };
        let waker = Waker::from(self.wake.clone());
        let mut context = Context::from_waker(&waker);
        if let Poll::Ready((source, result)) = loan
            .pending
            .as_mut()
            .unwrap()
            .future
            .as_mut()
            .poll(&mut context)
        {
            let pending = loan.pending.take().unwrap();
            {
                let mut state = self.source.lock().unwrap_or_else(|p| p.into_inner());
                assert!(state.source.is_none());
                state.source = Some(source);
            }
            // The source is stable before callbacks or the completed future die.
            pending.reply.publish(result);
            drop(pending);
        }
    }

    pub(super) fn poll_exit(&self, cx: &mut Context<'_>) -> Poll<LogicalExit> {
        self.wake.forward.register(cx.waker());
        {
            let mut state = self.exit.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(value) = &state.value {
                return Poll::Ready(value.clone());
            }
            if state.polling {
                state.waiting = true;
                return Poll::Pending;
            }
            state.polling = true;
        }
        let _loan = ExitLoan { carrier: self };
        let waker = Waker::from(self.exit_progress.clone());
        let mut context = Context::from_waker(&waker);
        let result = self.installed.poll_exit(&mut context);
        if let Poll::Ready(value) = &result {
            self.exit.lock().unwrap_or_else(|p| p.into_inner()).value = Some(value.clone());
        }
        result
    }
    pub(super) fn source_idle(&self) -> bool {
        let state = self.source.lock().unwrap_or_else(|p| p.into_inner());
        state.queue.is_empty() && state.pending.is_none() && !state.polling
    }

    pub(super) fn claim_explicit_join(&self) {
        self.explicit_join.store(true, Ordering::Release);
    }
    pub(super) fn explicit_join_claimed(&self) -> bool {
        self.explicit_join.load(Ordering::Acquire)
    }

    /// The Registry service owns one natural-retirement continuation. A role
    /// which only installed the actor may already have exited: it grants no
    /// close authority. Only the core's exact protocol-ready reply does.
    pub(super) fn poll_natural_retirement(
        &self,
        cx: &mut Context<'_>,
        may_close: bool,
    ) -> Poll<Result<(), LogicalSourceClosed>> {
        let state = {
            let mut state = self.natural_join.lock().unwrap_or_else(|p| p.into_inner());
            if state.polling {
                state.waiting = true;
                return Poll::Pending;
            }
            let old = std::mem::take(&mut *state);
            state.polling = true;
            old
        };
        let mut loan = NaturalJoinLoan {
            carrier: self,
            state: Some(state),
        };
        let state = loan.state.as_mut().unwrap();
        if state.ready {
            return Poll::Ready(Ok(()));
        }
        if state.closed {
            return Poll::Ready(Err(LogicalSourceClosed));
        }
        if state.protocol_ready && !state.closing {
            if !may_close || self.explicit_join_claimed() {
                return Poll::Pending;
            }
            state.closing = true;
            self.installed
                .request_close(novarocks_logical_execution_protocol::input::RegistryClose::Retire);
            state.query_epoch = self.wake.epoch.load(Ordering::Acquire);
            state.reply = Some(self.enqueue_readiness(true));
            return Poll::Pending;
        }
        if state.reply.is_none() {
            let epoch = self.wake.epoch.load(Ordering::Acquire);
            if state.waiting_epoch == Some(epoch) {
                return Poll::Pending;
            }
            state.query_epoch = epoch;
            state.reply = Some(self.enqueue_readiness(state.closing));
        }
        let result = std::task::ready!(self.poll_reply(state.reply.as_ref().unwrap(), cx));
        state.reply = None;
        match result {
            Ok(RuntimeReplyValue::Readiness(LogicalJoinReadiness::Ready)) if !state.closing => {
                state.protocol_ready = true;
                if may_close && !self.explicit_join_claimed() {
                    state.closing = true;
                    self.installed.request_close(
                        novarocks_logical_execution_protocol::input::RegistryClose::Retire,
                    );
                    state.query_epoch = self.wake.epoch.load(Ordering::Acquire);
                    state.reply = Some(self.enqueue_readiness(true));
                }
                Poll::Pending
            }
            Ok(RuntimeReplyValue::Readiness(LogicalJoinReadiness::Ready)) => {
                state.ready = true;
                Poll::Ready(Ok(()))
            }
            Ok(RuntimeReplyValue::Readiness(LogicalJoinReadiness::WaitingForConvergence)) => {
                // A false reply is not a reason to query again. Wait for
                // genuine carrier progress rather than self-polling the core.
                state.waiting_epoch = Some(state.query_epoch);
                Poll::Pending
            }
            Err(closed) => {
                state.closed = true;
                Poll::Ready(Err(closed))
            }
            Ok(RuntimeReplyValue::Fact) => unreachable!("typed natural-retirement reply"),
        }
    }

    pub(super) async fn wait_reply(
        &self,
        reply: &RuntimeReply,
    ) -> Result<RuntimeReplyValue, LogicalSourceClosed> {
        loop {
            // Enable before polling so another Registry borrower cannot steal
            // the only forward waker and strand this original ticket.
            let changed = self.wake.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            tokio::select! {
                biased;
                result = std::future::poll_fn(|cx| self.poll_reply(reply, cx)) => return result,
                _ = &mut changed => {},
            }
        }
    }

    pub(super) async fn readiness(
        &self,
        close: bool,
    ) -> Result<LogicalJoinReadiness, LogicalSourceClosed> {
        let reply = self.enqueue_readiness(close);
        match self.wait_reply(&reply).await? {
            RuntimeReplyValue::Readiness(value) => Ok(value),
            RuntimeReplyValue::Fact => unreachable!("original typed readiness reply"),
        }
    }
    pub(super) async fn wait_ingress(self: Arc<Self>) {
        let ready = self.readiness(true).await;
        if ready == Ok(LogicalJoinReadiness::Ready) {
            return;
        }
        // Waiting/closed is never interpreted as readiness. Only this original
        // registration's actual Scope exit can complete the remaining wait.
        loop {
            let changed = self.wake.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            tokio::select! {
                biased;
                _ = std::future::poll_fn(|cx| self.poll_exit(cx)) => return,
                _ = &mut changed => {},
            }
        }
    }
}
struct NaturalJoinLoan<'a> {
    carrier: &'a RuntimeCarrier,
    state: Option<NaturalJoinState>,
}
impl Drop for NaturalJoinLoan<'_> {
    fn drop(&mut self) {
        let wake = {
            let mut original = self
                .carrier
                .natural_join
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let waiting = original.waiting;
            *original = self.state.take().unwrap();
            original.polling = false;
            waiting
        };
        if wake {
            std::task::Wake::wake_by_ref(&self.carrier.wake);
        }
    }
}

struct SourceLoan<'a> {
    carrier: &'a RuntimeCarrier,
    pending: Option<PendingSend>,
}
impl Drop for SourceLoan<'_> {
    fn drop(&mut self) {
        let wake = {
            let mut state = self
                .carrier
                .source
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            assert!(state.pending.is_none());
            state.pending = self.pending.take();
            state.polling = false;
            std::mem::take(&mut state.waiting)
                || (!state.queue.is_empty() && state.pending.is_none())
        };
        if wake {
            std::task::Wake::wake_by_ref(&self.carrier.wake);
        }
    }
}
struct ExitLoan<'a> {
    carrier: &'a RuntimeCarrier,
}
impl Drop for ExitLoan<'_> {
    fn drop(&mut self) {
        let wake = {
            let mut state = self.carrier.exit.lock().unwrap_or_else(|p| p.into_inner());
            state.polling = false;
            std::mem::take(&mut state.waiting)
        };
        if wake {
            std::task::Wake::wake_by_ref(&self.carrier.wake);
        }
    }
}

pub(super) struct RuntimeInstalledEntry {
    pub(super) registration: Arc<LogicalExecutionRegistration>,
    pub(super) actor_id: LogicalExecutionActorId,
    pub(super) carrier: Arc<RuntimeCarrier>,
    pub(super) original_exit: Option<Arc<OriginalLogicalExit>>,
    pub(super) ingress_closed: bool,
    pub(super) internal_health: Option<InternalConsumerHealth>,
    pub(super) activations:
        BTreeMap<QueryExecutionId, novarocks_coordination_contract::AttemptActivationIdentity>,
}
