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

//! Weak caller views of the original standing sources and actual host exit.
use super::{
    application::LogicalHostReplies,
    failure::LogicalInfrastructureFailure,
    inlet::{LogicalIngressCloseObserver, LogicalInletSender},
};
use crate::control_runtime::{actor::FrontendActor, host::HostExit};
use novarocks_control_runtime::actor_turn::{ActorRegistration, ActorRuntime};
use novarocks_logical_execution_protocol::{
    LogicalRequestToken, LogicalSourceId, input::RegistryClose,
};
use novarocks_query_application::api::logical_runtime::{
    InstalledLogicalExecution, LogicalExit, LogicalFactSource, LogicalFuture, LogicalHostFact,
    LogicalJoinReadiness, LogicalSourceClosed, LogicalSourceKind, LogicalSourceRefusal,
};
use std::{
    future::poll_fn,
    num::NonZeroU64,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
    task::{Context, Poll, Waker},
};
use tokio::sync::{Mutex as AsyncMutex, Notify, oneshot};

pub(super) type SenderCell = Arc<AsyncMutex<LogicalInletSender>>;
struct SourceView {
    kind: LogicalSourceKind,
    id: LogicalSourceId,
    original: Weak<AsyncMutex<LogicalInletSender>>,
    taken: bool,
}
struct Probe {
    source: LogicalSourceId,
    token: LogicalRequestToken,
    reply: oneshot::Sender<bool>,
}
struct ProbeFlight {
    close: bool,
    future: LogicalFuture<'static, Result<bool, LogicalSourceClosed>>,
}
struct ReplyState {
    next: Option<NonZeroU64>,
    probe: Option<Probe>,
    registry: Option<(LogicalSourceId, Weak<AsyncMutex<LogicalInletSender>>)>,
    flight: Option<ProbeFlight>,
    polling: bool,
    waiter: Option<Waker>,
    owner_failed_closing: bool,
}
struct ExitState {
    original: Option<LogicalFuture<'static, LogicalExit>>,
    value: Option<LogicalExit>,
    polling: bool,
    waiter: Option<Waker>,
}
#[derive(Clone)]
struct LogicalFailureBinding {
    runtime: ActorRuntime<FrontendActor>,
    registration: ActorRegistration,
}
pub(super) struct LogicalInstalledState {
    sources: Mutex<Vec<SourceView>>,
    replies: Mutex<ReplyState>,
    probe_gate: AsyncMutex<()>,
    exit: Mutex<ExitState>,
    progress_waiter: Mutex<Option<Waker>>,
    failure_actor: Mutex<Option<LogicalFailureBinding>>,
    registry_close: Mutex<Option<LogicalIngressCloseObserver>>,
    pub(super) requested_close: AtomicU8,
    pub(super) retirable: AtomicBool,
    owner_failed_observation_stop: AtomicBool,
    pub(super) stopped: Notify,
    host: Waker,
    failure: Arc<LogicalInfrastructureFailure>,
}
impl LogicalInstalledState {
    pub(super) fn new(host: Waker, failure: Arc<LogicalInfrastructureFailure>) -> Arc<Self> {
        Arc::new(Self {
            sources: Mutex::new(Vec::new()),
            replies: Mutex::new(ReplyState {
                next: NonZeroU64::new(1),
                probe: None,
                registry: None,
                flight: None,
                polling: false,
                waiter: None,
                owner_failed_closing: false,
            }),
            probe_gate: AsyncMutex::new(()),
            exit: Mutex::new(ExitState {
                original: None,
                value: None,
                polling: false,
                waiter: None,
            }),
            progress_waiter: Mutex::new(None),
            failure_actor: Mutex::new(None),
            registry_close: Mutex::new(None),
            requested_close: AtomicU8::new(0),
            retirable: AtomicBool::new(false),
            owner_failed_observation_stop: AtomicBool::new(false),
            stopped: Notify::new(),
            host,
            failure,
        })
    }
    pub(super) fn bind_failure_actor(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        registration: &ActorRegistration,
    ) {
        runtime
            .validate_registration(registration)
            .expect("same original installed logical actor");
        assert!(
            self.failure_actor
                .lock()
                .unwrap()
                .replace(LogicalFailureBinding {
                    runtime: runtime.clone(),
                    registration: registration.clone()
                })
                .is_none()
        );
    }
    pub(super) fn bind_registry_close(&self, original: LogicalIngressCloseObserver) {
        assert!(
            self.registry_close
                .lock()
                .unwrap()
                .replace(original)
                .is_none()
        );
    }
    pub(super) fn bind_source(
        &self,
        kind: LogicalSourceKind,
        id: LogicalSourceId,
        sender: &SenderCell,
    ) {
        let mut sources = self.sources.lock().unwrap();
        assert!(
            !sources.iter().any(|s| s.kind == kind),
            "same original logical source binds once"
        );
        sources.push(SourceView {
            kind,
            id,
            original: Arc::downgrade(sender),
            taken: false,
        });
        drop(sources);
        if kind == LogicalSourceKind::Registry {
            assert!(
                self.replies
                    .lock()
                    .unwrap()
                    .registry
                    .replace((id, Arc::downgrade(sender)))
                    .is_none()
            );
        }
    }
    pub(super) fn bind_exit(self: &Arc<Self>, mut original: HostExit) {
        let weak = Arc::downgrade(self);
        let future = Box::pin(async move {
            // HostExit reports only actual Actor and Scope removal. A closed
            // watch with no true value supplies no invented completion.
            if original.wait().await.is_err() {
                std::future::pending::<()>().await;
            }
            let Some(state) = weak.upgrade() else {
                std::future::pending::<()>().await;
                unreachable!()
            };
            if state.failure.failed() {
                LogicalExit::OwnerFailed(state.failure.error())
            } else {
                LogicalExit::Completed
            }
        });
        assert!(self.exit.lock().unwrap().original.replace(future).is_none());
    }
    pub(super) async fn stopped(&self) {
        let changed = self.stopped.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if !self.retirable.load(Ordering::Acquire)
            && !self.owner_failed_observation_stop.load(Ordering::Acquire)
        {
            changed.await;
        }
    }
    /// The Source has observed the original kernel failure fence. Stop only
    /// passive application observation; this is neither retirable nor exit.
    pub(super) fn stop_observation_after_owner_failure(&self) {
        if !self
            .owner_failed_observation_stop
            .swap(true, Ordering::AcqRel)
        {
            self.stopped.notify_waiters();
        }
    }
    fn poll_probe(&self, cx: &mut Context<'_>) -> Poll<(bool, Result<bool, LogicalSourceClosed>)> {
        let flight = {
            let mut replies = self.replies.lock().unwrap();
            if replies.polling {
                let old = replies.waiter.replace(cx.waker().clone());
                drop(replies);
                drop(old);
                return Poll::Pending;
            }
            let Some(flight) = replies.flight.take() else {
                return Poll::Ready((false, Err(LogicalSourceClosed)));
            };
            replies.polling = true;
            flight
        };
        let mut loan = ProbeLoan {
            owner: self,
            flight: Some(flight),
        };
        match loan.flight.as_mut().unwrap().future.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                let close = loan.flight.as_ref().unwrap().close;
                drop(loan.flight.take());
                if result.is_err() {
                    let abandoned_reply = self.replies.lock().unwrap().probe.take();
                    drop(abandoned_reply);
                }
                Poll::Ready((close, result))
            }
        }
    }
}
struct WeakSource {
    id: LogicalSourceId,
    original: Weak<AsyncMutex<LogicalInletSender>>,
}
impl LogicalFactSource for WeakSource {
    fn id(&self) -> LogicalSourceId {
        self.id
    }
    fn send(
        &mut self,
        fact: LogicalHostFact,
    ) -> LogicalFuture<'_, Result<(), LogicalSourceClosed>> {
        Box::pin(async move {
            let original = self.original.upgrade().ok_or(LogicalSourceClosed)?;
            original.lock().await.send(fact).await
        })
    }
}
impl InstalledLogicalExecution for LogicalInstalledState {
    fn source(
        &self,
        kind: LogicalSourceKind,
    ) -> Result<Box<dyn LogicalFactSource>, LogicalSourceRefusal> {
        let mut sources = self.sources.lock().unwrap();
        let Some(source) = sources.iter_mut().find(|s| s.kind == kind) else {
            return Err(if matches!(kind, LogicalSourceKind::AttemptData(_)) {
                LogicalSourceRefusal::ForeignActivation
            } else {
                LogicalSourceRefusal::Closed
            });
        };
        if source.taken {
            return Err(LogicalSourceRefusal::AlreadyTaken);
        }
        if source.original.strong_count() == 0 {
            return Err(LogicalSourceRefusal::Closed);
        }
        source.taken = true;
        Ok(Box::new(WeakSource {
            id: source.id,
            original: source.original.clone(),
        }))
    }
    fn request_close(&self, close: RegistryClose) {
        let level = match close {
            RegistryClose::Retire => 1,
            RegistryClose::Shutdown => 2,
        };
        let old = self.requested_close.fetch_max(level, Ordering::AcqRel);
        if level > old {
            self.host.wake_by_ref();
        }
    }
    fn join_readiness(
        &self,
        close_ingress: bool,
    ) -> LogicalFuture<'_, Result<LogicalJoinReadiness, LogicalSourceClosed>> {
        Box::pin(async move {
            let _original_probe = self.probe_gate.lock().await;
            loop {
                {
                    let mut replies = self.replies.lock().unwrap();
                    if replies.owner_failed_closing {
                        return Err(LogicalSourceClosed);
                    }
                    if replies.flight.is_none() {
                        let (source, original) =
                            replies.registry.clone().ok_or(LogicalSourceClosed)?;
                        let original = original.upgrade().ok_or(LogicalSourceClosed)?;
                        let n = replies.next.ok_or(LogicalSourceClosed)?;
                        replies.next = n.get().checked_add(1).and_then(NonZeroU64::new);
                        let token = LogicalRequestToken::new(n);
                        let (send, receive) = oneshot::channel();
                        assert!(
                            replies
                                .probe
                                .replace(Probe {
                                    source,
                                    token,
                                    reply: send
                                })
                                .is_none(),
                            "same original Registry reply"
                        );
                        let future = Box::pin(async move {
                            let mut sender = original.lock().await;
                            sender
                                .send(LogicalHostFact::RegistryJoinReadiness {
                                    token,
                                    close_ingress,
                                })
                                .await?;
                            receive.await.map_err(|_| LogicalSourceClosed)
                        });
                        replies.flight = Some(ProbeFlight {
                            close: close_ingress,
                            future,
                        });
                    }
                }
                // This original future stays in Source custody when this
                // borrowed wait is cancelled. A later stronger close first
                // settles that probe, then sends its own exact close request.
                let (original_close, ready) = poll_fn(|cx| self.poll_probe(cx)).await;
                let ready = ready?;
                if original_close == close_ingress {
                    return Ok(if ready {
                        LogicalJoinReadiness::Ready
                    } else {
                        LogicalJoinReadiness::WaitingForConvergence
                    });
                }
            }
        })
    }
    fn begin_owner_failed_registry_close(&self) -> Result<(), LogicalSourceClosed> {
        let binding = self
            .failure_actor
            .lock()
            .unwrap()
            .clone()
            .ok_or(LogicalSourceClosed)?;
        // This real kernel proof is required even if an outer application has
        // already retained a typed error. Error text/health alone cannot fence.
        binding
            .runtime
            .failure_proof_for(&binding.registration)
            .map_err(|_| LogicalSourceClosed)?;
        if self.registry_close.lock().unwrap().is_none() {
            return Err(LogicalSourceClosed);
        }
        self.replies.lock().unwrap().owner_failed_closing = true;
        self.request_close(RegistryClose::Retire);
        Ok(())
    }
    fn poll_owner_failed_registry_closed(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), LogicalSourceClosed>> {
        let flight = {
            let replies = self.replies.lock().unwrap();
            if !replies.owner_failed_closing {
                return Poll::Ready(Err(LogicalSourceClosed));
            }
            replies.flight.is_some() || replies.polling
        };
        if flight {
            let _ = self.poll_probe(cx);
        }
        let replies = self.replies.lock().unwrap();
        if replies.flight.is_some() || replies.polling || replies.probe.is_some() {
            return Poll::Pending;
        }
        drop(replies);
        let original = self
            .registry_close
            .lock()
            .unwrap()
            .clone()
            .ok_or(LogicalSourceClosed);
        match original {
            Ok(original) => original.poll_closed(cx).map(Ok),
            Err(error) => Poll::Ready(Err(error)),
        }
    }
    fn poll_exit(&self, cx: &mut Context<'_>) -> Poll<LogicalExit> {
        // Only an actual false->true readiness transition wakes this waiter.
        // A false registry reply cannot create a service feedback loop.
        let mut wake = Some(cx.waker().clone());
        {
            let mut waiter = self.progress_waiter.lock().unwrap();
            std::mem::swap(&mut *waiter, &mut wake);
        }
        drop(wake);
        let future = {
            let mut state = self.exit.lock().unwrap();
            if let Some(value) = &state.value {
                return Poll::Ready(value.clone());
            }
            if state.polling {
                let old = state.waiter.replace(cx.waker().clone());
                drop(state);
                drop(old);
                return Poll::Pending;
            }
            let Some(future) = state.original.take() else {
                return Poll::Pending;
            };
            state.polling = true;
            future
        };
        let mut loan = ExitLoan {
            owner: self,
            original: Some(future),
        };
        match loan.original.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(value) => {
                self.exit.lock().unwrap().value = Some(value.clone());
                drop(loan.original.take());
                Poll::Ready(value)
            }
        }
    }
}
struct ProbeLoan<'a> {
    owner: &'a LogicalInstalledState,
    flight: Option<ProbeFlight>,
}
impl Drop for ProbeLoan<'_> {
    fn drop(&mut self) {
        let wake = {
            let mut replies = self.owner.replies.lock().unwrap();
            assert!(replies.polling && replies.flight.is_none());
            replies.flight = self.flight.take();
            replies.polling = false;
            replies.waiter.take()
        };
        if let Some(wake) = wake {
            wake.wake();
        }
    }
}
struct ExitLoan<'a> {
    owner: &'a LogicalInstalledState,
    original: Option<LogicalFuture<'static, LogicalExit>>,
}
impl Drop for ExitLoan<'_> {
    fn drop(&mut self) {
        let wake = {
            let mut state = self.owner.exit.lock().unwrap();
            assert!(state.polling && state.original.is_none());
            state.original = self.original.take();
            state.polling = false;
            state.waiter.take()
        };
        if let Some(wake) = wake {
            wake.wake();
        }
    }
}
impl LogicalHostReplies for LogicalInstalledState {
    fn readiness_changed(&self, ready: bool) {
        if ready {
            let wake = self.progress_waiter.lock().unwrap().take();
            if let Some(wake) = wake {
                wake.wake();
            }
        }
    }
    fn registry_reply(&self, source: LogicalSourceId, token: LogicalRequestToken, ready: bool) {
        let pending = {
            let mut replies = self.replies.lock().unwrap();
            if replies
                .probe
                .as_ref()
                .is_none_or(|p| p.source != source || p.token != token)
            {
                None
            } else {
                replies.probe.take()
            }
        };
        match pending {
            Some(pending) => {
                let _ = pending.reply.send(ready);
            }
            None => self.failure.retain(Box::new((source, token, ready))),
        }
    }
    fn snapshot_reply(&self, _: LogicalSourceId, _: LogicalRequestToken) {}
    fn retirable(&self) {
        self.retirable.store(true, Ordering::Release);
        self.stopped.notify_waiters();
        self.host.wake_by_ref();
    }
}

#[cfg(test)]
mod tests;
