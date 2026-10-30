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
use novarocks_state_store_api::{
    InstanceScope, StateStoreBlockingOrigin as Origin, StateStoreOpenRequest,
    StateStoreOriginalClose, StateStoreOriginalOpen, StateStoreProviderDescriptor,
    StateStoreProviderInstance,
};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering},
};

struct OpenInput {
    path: PathBuf,
    request: StateStoreOpenRequest,
}
struct InstanceCloseBody {
    store: Option<Arc<crate::SqliteStateStore>>,
    evidence: Option<Arc<crate::evidence::SqliteCommitEvidence>>,
    lifetime: Option<Arc<crate::SqliteInstanceLifetime>>,
    progressed: bool,
}
struct OpenBody {
    instance_close: Option<InstanceCloseBody>,
    input: Option<OpenInput>,
    result: Option<Result<crate::SqliteStateStore, Failure>>,
    exited: bool,
    transferred: bool,
}
pub(crate) struct OpenOriginal {
    core: Core<OpenBody>,
    descriptor: StateStoreProviderDescriptor,
    origin: Origin,
    closing: AtomicBool,
}
impl OpenOriginal {
    fn close_if_idle(&self) {
        if !self.closing.load(Ordering::Acquire) || !self.core.is_idle() || self.core.is_complete()
        {
            return;
        }
        if matches!(self.origin, Origin::Instance(_)) {
            if self.core.health().is_some() {
                return;
            }
            let ready = lock(&self.core.body).as_ref().is_some_and(|body| {
                let close = body.instance_close.as_ref().unwrap();
                match (&close.store, &close.evidence, &close.lifetime) {
                    (Some(store), _, _) => Arc::strong_count(store) == 1,
                    (None, Some(evidence), _) => Arc::strong_count(evidence) == 1,
                    (None, None, Some(lifetime)) => Arc::strong_count(lifetime) == 1,
                    (None, None, None) => false,
                }
            });
            // Counts only avoid useless work. The same worker's try_unwrap is
            // the last-owner author, including races with external Weak upgrades.
            if ready && let Err(failure) = self.core.queue_if_idle(Phase::Close) {
                self.core.fail(failure);
            }
            return;
        }
        let Some(body) = lock(&self.core.body)
            .as_ref()
            .map(|b| (matches!(b.result, Some(Ok(_))), b.transferred))
        else {
            return;
        };
        if body.0 && !body.1 {
            if let Err(failure) = self.core.queue_if_idle(Phase::Close) {
                self.core.fail(failure);
            }
        } else {
            self.core.complete();
        }
    }
}
impl Original for OpenOriginal {
    fn origin(&self) -> Origin {
        self.origin
    }
    fn requested(&self) -> Option<Request> {
        self.close_if_idle();
        self.core.request()
    }
    fn publication_refusal(&self, request: Request) -> Option<Failure> {
        if self.core.request() != Some(request) {
            return Some(invalid("SQLite opening stop observation is stale"));
        }
        if request.phase == Phase::Close {
            return None;
        }
        if self.closing.load(Ordering::Acquire) {
            return Some(error(
                StateStoreErrorKind::Cancelled,
                "SQLite original opening was stopped",
            ));
        }
        (std::time::Instant::now() >= self.core.execution.deadline()).then(|| {
            error(
                StateStoreErrorKind::DeadlineExceeded,
                "SQLite original opening deadline exceeded",
            )
        })
    }
    fn before_publish(&self, request: Request) -> Result<(), Failure> {
        self.core.prepare(request)
    }
    fn run_original_phase(&self, request: Request) {
        if !self.core.enter(request) {
            return;
        }
        let mut loan = match BodyLoan::take(&self.core.body) {
            Ok(value) => value,
            Err(failure) => {
                self.core.fail(failure);
                return;
            }
        };
        let body = loan.value.as_mut().unwrap();
        match request.phase {
            Phase::Open => {
                let Some(input) = body.input.take() else {
                    self.core
                        .fail(invalid("SQLite opening input was already consumed"));
                    return;
                };
                body.result = Some(
                    if self.closing.load(Ordering::Acquire)
                        || std::time::Instant::now() >= input.request.deadline
                    {
                        Err(StateStoreError::new(
                            StateStoreErrorKind::Cancelled,
                            "SQLite opening was stopped before its original call",
                        ))
                    } else {
                        crate::open_blocking(
                            input.path,
                            input.request.cluster_id,
                            input.request.limits,
                            crate::default_attempt_capacity(),
                        )
                    }
                    .map_err(failure),
                );
            }
            Phase::Close => {
                if let Some(close) = body.instance_close.as_mut() {
                    close.progressed = false;
                    if let Some(store) = close.store.take() {
                        match Arc::try_unwrap(store) {
                            Ok(store) => {
                                drop(store);
                                close.progressed = true;
                            }
                            Err(store) => close.store = Some(store),
                        }
                    } else if let Some(evidence) = close.evidence.take() {
                        match Arc::try_unwrap(evidence) {
                            Ok(evidence) => {
                                // An external Observation can outlive Store.
                                // Keep its same Evidence until this worker is
                                // its last owner. A legacy worker can still
                                // hold the original connection/file bundle.
                                drop(evidence);
                                close.progressed = true;
                            }
                            Err(evidence) => close.evidence = Some(evidence),
                        }
                    } else if let Some(lifetime) = close.lifetime.take() {
                        match Arc::try_unwrap(lifetime) {
                            Ok(lifetime) => {
                                drop(lifetime);
                                close.progressed = true;
                            }
                            Err(lifetime) => close.lifetime = Some(lifetime),
                        }
                    }
                } else {
                    drop(body.result.take());
                    drop(body.input.take());
                }
            }
            _ => self.core.fail(invalid(
                "SQLite opening source cannot execute a transaction phase",
            )),
        }
    }
    fn receive_result(&self, request: Request, result: Result<(), Failure>) {
        self.core.result(request, result);
    }
    fn receive_exit(&self, request: Request, exit: Result<Exit, Failure>) {
        let Some(exit) = self.core.exit(request, exit) else {
            return;
        };
        if matches!(self.origin, Origin::Instance(_)) {
            // Receiving a result never proves a last drop. Preserve a physical
            // failure and its footer; it cannot advance this closing Source.
            if self.core.health().is_some() {
                self.core.notify();
                return;
            }
            let (empty, progressed) = {
                let body = lock(&self.core.body);
                let close = body.as_ref().unwrap().instance_close.as_ref().unwrap();
                (
                    close.store.is_none() && close.evidence.is_none() && close.lifetime.is_none(),
                    close.progressed,
                )
            };
            if exit == Exit::ActualExited && empty {
                self.core.complete();
                self.core.notify();
            } else if exit == Exit::ActualExited && progressed {
                self.core.idle();
            } else {
                // An unpublished call or a concurrent Weak upgrade grants no
                // last-drop fact and no self-waking retry. Host policy polls.
                self.core.idle_silent();
            }
            return;
        }
        if request.phase == Phase::Close {
            if exit == Exit::ActualExited {
                self.core.complete();
            } else {
                self.core
                    .fail(invalid("SQLite instance close was never executed"));
            }
            return;
        }
        let mut loan = match BodyLoan::take(&self.core.body) {
            Ok(value) => value,
            Err(failure) => {
                self.core.fail(failure);
                return;
            }
        };
        let body = loan.value.as_mut().unwrap();
        body.exited = true;
        if exit == Exit::NeverPublishedReturned && body.result.is_none() {
            body.result = Some(Err(self.core.physical_failure().unwrap_or_else(|| {
                error(
                    StateStoreErrorKind::Cancelled,
                    "SQLite opening call was never published",
                )
            })));
        }
        if body.result.is_none() {
            // Raw call failure remains in Core; no returned instance exists.
            self.closing.store(true, Ordering::Release);
        }
        drop(loan);
        self.core.idle();
        self.close_if_idle();
    }
    fn is_complete(&self) -> bool {
        self.core.is_complete()
    }
    fn local_failure(&self) -> Option<Failure> {
        self.core.local_failure()
    }
    fn request_interrupt(&self) {
        self.request_close();
    }
    fn request_close(&self) {
        self.closing.store(true, Ordering::Release);
        self.core.notify();
    }
}
struct Opening {
    original: Arc<OpenOriginal>,
    transferred: bool,
}
impl StateStoreOriginalOpen for Opening {
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Failure>> {
        let step = self.original.core.poll_step(cx);
        if let Poll::Ready(Err(failure)) = step {
            return Poll::Ready(Err(failure));
        }
        let body = lock(&self.original.core.body);
        let Some(body) = body.as_ref() else {
            return Poll::Pending;
        };
        if !body.exited {
            return Poll::Pending;
        }
        match body.result.as_ref() {
            Some(Ok(_)) if self.original.closing.load(Ordering::Acquire) => {
                Poll::Ready(Err(error(
                    StateStoreErrorKind::Cancelled,
                    "SQLite original opening is closing",
                )))
            }
            Some(Ok(_)) => Poll::Ready(Ok(())),
            Some(Err(cause)) => Poll::Ready(Err(cause.clone())),
            None => {
                Poll::Ready(Err(self.original.core.physical_failure().unwrap_or_else(
                    || invalid("SQLite original opening returned no instance"),
                )))
            }
        }
    }
    fn take_instance_in(
        &mut self,
        target: &mut Option<Box<dyn StateStoreProviderInstance>>,
    ) -> Result<(), Failure> {
        if target.is_some() {
            return Err(invalid("SQLite instance receiving slot is occupied"));
        }
        if self.original.core.health().is_some()
            || self.original.core.request().is_some()
            || self.original.closing.load(Ordering::Acquire)
        {
            return Err(invalid("SQLite opening is not ready for instance handoff"));
        }
        let mut loan = BodyLoan::take(&self.original.core.body)?;
        let body = loan.value.as_mut().unwrap();
        if !body.exited || body.transferred || !matches!(body.result, Some(Ok(_))) {
            return Err(invalid(
                "SQLite original instance is absent or was already transferred",
            ));
        }
        let store = body.result.take().unwrap().map_err(failure)?;
        *target = Some(crate::provider::instance_from_store(
            self.original.descriptor,
            store,
        ));
        body.transferred = true;
        self.transferred = true;
        self.original.core.complete();
        Ok(())
    }
}
impl Drop for Opening {
    fn drop(&mut self) {
        if !self.transferred {
            self.original.request_close();
        }
    }
}
pub(crate) fn prepare_open_in(
    path: PathBuf,
    descriptor: StateStoreProviderDescriptor,
    request: StateStoreOpenRequest,
    execution: &Execution,
    target: &mut Option<Box<dyn StateStoreOriginalOpen>>,
) -> Result<(), Failure> {
    if target.is_some() {
        return Err(invalid("SQLite opening receiving slot is occupied"));
    }
    if request.deadline != execution.deadline() {
        return Err(error(
            StateStoreErrorKind::InvalidRequest,
            "SQLite opening deadline differs from its original execution",
        ));
    }
    if crate::is_memory_path(&path) {
        return Err(error(
            StateStoreErrorKind::InvalidConfiguration,
            "SQLite state store requires a persistent file path",
        ));
    }
    let original = Arc::new(OpenOriginal {
        core: Core::new(
            OpenBody {
                instance_close: None,
                input: Some(OpenInput { path, request }),
                result: None,
                exited: false,
                transferred: false,
            },
            execution.clone(),
        ),
        descriptor,
        origin: Origin::Opening(descriptor.id),
        closing: AtomicBool::new(false),
    });
    original.core.queue(Phase::Open)?;
    original
        .core
        .bind(&(original.clone() as Arc<dyn Original>))?;
    *target = Some(Box::new(Opening {
        original: original.clone(),
        transferred: false,
    }));
    original.core.notify();
    Ok(())
}

struct InstanceClosing {
    original: Arc<OpenOriginal>,
}
impl StateStoreOriginalClose for InstanceClosing {
    fn poll_closed(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Failure>> {
        self.original.close_if_idle();
        let progress = self.original.core.poll_step(cx);
        if let Some(failure) = self.original.local_failure() {
            return Poll::Ready(Err(failure));
        }
        if self.original.is_complete() {
            return Poll::Ready(Ok(()));
        }
        match progress {
            Poll::Ready(Err(failure)) => Poll::Ready(Err(failure)),
            _ => Poll::Pending,
        }
    }
}
impl Drop for InstanceClosing {
    fn drop(&mut self) {
        self.original.request_close();
    }
}
pub(crate) fn borrow_instance_close_in(
    original: &Arc<OpenOriginal>,
    execution: &Execution,
    target: &mut Option<Box<dyn StateStoreOriginalClose>>,
) -> Result<(), Failure> {
    if target.is_some() {
        return Err(invalid("SQLite closing receiving slot is occupied"));
    }
    if !matches!(original.origin, Origin::Instance(_)) {
        return Err(invalid(
            "SQLite closing handle requires an instance close source",
        ));
    }
    if !original.core.execution.shares_owner(execution) {
        return Err(invalid(
            "SQLite closing source belongs to another original execution",
        ));
    }
    *target = Some(Box::new(InstanceClosing {
        original: original.clone(),
    }));
    Ok(())
}
pub(crate) fn prepare_instance_close_in(
    descriptor: StateStoreProviderDescriptor,
    scope: InstanceScope,
    store: Arc<crate::SqliteStateStore>,
    lifetime: Arc<crate::SqliteInstanceLifetime>,
    execution: &Execution,
    target: &mut Option<Box<dyn StateStoreOriginalClose>>,
) -> Result<Arc<OpenOriginal>, Failure> {
    if target.is_some() {
        return Err(invalid("SQLite closing receiving slot is occupied"));
    }
    if descriptor.id != crate::SQLITE_STATE_STORE_PROVIDER_ID
        || store.attempts.scope() != scope
        || !Arc::ptr_eq(&store.lifetime, &lifetime)
    {
        return Err(invalid(
            "SQLite closing source differs from its actual instance ownership",
        ));
    }
    let original = Arc::new(OpenOriginal {
        core: Core::new(
            OpenBody {
                instance_close: Some(InstanceCloseBody {
                    evidence: Some(store.evidence.clone()),
                    store: Some(store),
                    lifetime: Some(lifetime),
                    progressed: false,
                }),
                input: None,
                result: None,
                exited: false,
                transferred: false,
            },
            execution.clone(),
        ),
        descriptor,
        origin: Origin::Instance(scope),
        closing: AtomicBool::new(true),
    });
    // The provider still owns its old Store/lifetime aliases here. No last
    // reference is taken before the real port and stable handle are installed.
    original
        .core
        .bind(&(original.clone() as Arc<dyn Original>))?;
    borrow_instance_close_in(&original, execution, target)?;
    Ok(original)
}
