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

//! One governed LIKE source read; column projection and target mutation are
//! separate actual CPU/business stages owned by the caller.

use super::{
    internal_result_cpu::InternalResultCpu, maintenance::io_admission::MetadataIoAdmission,
    metadata_capture::MetadataCapture,
};
use crate::{
    connector::{context_for_planning_pin_typed, mutation::command_error_from_connector},
    query::product_command_service_error,
};
use novarocks_failure_contract::SharedFailureCause;
use novarocks_query_application::{
    api::{CatalogTableReadBinding, CommandContext, CommandError, StatementNativeHealthFailure},
    cancellation::QueryCancellationView,
    session_control::GovernedActionContext,
    session_error::QueryServiceError,
    session_outcome::governed_cancellation_error,
};
use novarocks_spi::connector::{
    ConnectorError, ConnectorTableMetadata, ConnectorTableObjectBinding,
    ConnectorTableObjectCaptureRequest, ConnectorTableObjectSelector, ConnectorTableResolution,
    ProviderMetadataCpuPort,
};
use novarocks_workload_control::{ResultWindowAlias, WorkError};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

type CaptureFuture =
    Pin<Box<dyn Future<Output = Result<ConnectorTableObjectBinding, ConnectorError>> + Send>>;
#[derive(Debug, Default)]
struct Wake(futures::task::AtomicWaker);
impl crate::wake::StatusIntakeWake for Wake {
    fn wake(&self) {
        self.0.wake();
    }
}

#[derive(Default)]
struct Call {
    future: Option<CaptureFuture>,
    result: Option<Result<ConnectorTableObjectBinding, ConnectorError>>,
    started: bool,
    in_poll: bool,
    dropping: bool,
    exited: bool,
}
impl Call {
    fn poll(&mut self, cx: &mut Context<'_>) {
        if self.exited || self.in_poll || self.dropping {
            return;
        }
        let Some(future) = self.future.as_mut() else {
            return;
        };
        self.in_poll = true;
        let result = future.as_mut().poll(cx);
        self.in_poll = false;
        if let Poll::Ready(result) = result {
            // Keep the original returned value before dropping that same F.
            self.result = Some(result);
            self.dropping = true;
            drop(self.future.take());
            self.dropping = false;
            self.exited = true;
        }
    }
}

pub(crate) struct LikeSourceRead {
    binding: CatalogTableReadBinding,
    command: CommandContext,
    window: ResultWindowAlias,
    cpu: InternalResultCpu,
    cancellation: QueryCancellationView,
    runtime: tokio::runtime::Handle,
    wake: Arc<Wake>,
    capture: Arc<MetadataCapture>,
    call: Call,
    io: Option<Arc<MetadataIoAdmission>>,
    completed_io: Option<Arc<MetadataIoAdmission>>,
    port: Option<Arc<dyn ProviderMetadataCpuPort>>,
    setup_attempted: bool,
    bootstrap_ready: bool,
    port_created: bool,
    cpu_closed: bool,
    cpu_in_foreign: bool,
    io_in_foreign: bool,
    business_failure: Option<QueryServiceError>,
    health: Option<StatementNativeHealthFailure>,
    closing: bool,
    closed: bool,
    delivered: bool,
}
impl LikeSourceRead {
    pub(crate) fn try_new(
        binding: CatalogTableReadBinding,
        command: CommandContext,
        cpu: InternalResultCpu,
        cancellation: QueryCancellationView,
        runtime: tokio::runtime::Handle,
    ) -> Result<Self, QueryServiceError> {
        let window = command
            .result_window_alias()
            .ok_or_else(|| work(WorkError::Conflict))?;
        if !window.is_for_scope(command.scope())
            || command.connector_context().io_admission().is_some()
            || command.connector_context().metadata_cpu().is_some()
        {
            return Err(work(WorkError::Conflict));
        }
        Ok(Self {
            binding,
            command,
            window,
            cpu,
            cancellation,
            runtime,
            wake: Arc::new(Wake::default()),
            capture: MetadataCapture::new(
                unproven(
                    "LIKE source metadata capture exit is unproven",
                    Arc::new(WorkError::Conflict),
                ),
                "LIKE source metadata capture receiver is occupied",
                "LIKE source metadata CPU exit is unproven",
            ),
            call: Call::default(),
            io: None,
            completed_io: None,
            port: None,
            setup_attempted: false,
            bootstrap_ready: false,
            port_created: false,
            cpu_closed: false,
            cpu_in_foreign: false,
            io_in_foreign: false,
            business_failure: None,
            health: None,
            closing: false,
            closed: false,
            delivered: false,
        })
    }
    #[cfg(test)]
    pub(crate) fn capture_in_flight_for_test(&self) -> bool {
        self.call.started && !self.call.exited
    }

    fn fail(&mut self, error: QueryServiceError) {
        self.business_failure.get_or_insert(error);
    }
    pub(crate) fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.health
            .clone()
            .or_else(|| self.capture.local_health_failure())
            .or_else(|| self.io.as_ref().and_then(|io| io.health_failure()))
            .or_else(|| {
                self.port
                    .as_ref()
                    .and_then(|port| port.local_failure())
                    .map(|cause| unproven("LIKE source metadata CPU exit is unproven", cause))
            })
            .or_else(|| {
                (self.call.in_poll
                    || self.call.dropping
                    || self.cpu_in_foreign
                    || self.io_in_foreign)
                    .then(|| {
                        unproven(
                            "LIKE source original exit is unproven",
                            Arc::new(WorkError::Conflict),
                        )
                    })
            })
    }
    fn setup(&mut self, owner: &mut GovernedActionContext<'_>) {
        if self.setup_attempted || self.business_failure.is_some() || self.closing {
            return;
        }
        self.setup_attempted = true;
        let result = (|| {
            let dependencies = self
                .binding
                .planning()
                .binding()
                .metadata()
                .read_planning_initial_dependencies()
                .map_err(connector)?;
            let original = owner
                .execution_owner_slot()
                .as_ref()
                .ok_or_else(|| work(WorkError::Conflict))?;
            if !original.is_for_scope(self.command.scope()) {
                return Err(work(WorkError::Conflict));
            }
            self.io = Some(
                MetadataIoAdmission::for_original_source(original, dependencies).map_err(work)?,
            );
            Ok(())
        })();
        if let Err(error) = result {
            self.fail(error);
        }
    }
    fn progress_io(&mut self, owner: Option<&mut GovernedActionContext<'_>>, cx: &mut Context<'_>) {
        let Some(io) = self.io.as_ref() else {
            return;
        };
        if !self.bootstrap_ready && (self.business_failure.is_some() || self.closing) {
            return;
        }
        self.io_in_foreign = true;
        let result = if self.bootstrap_ready {
            io.poll_actual_progress_continuing(cx)
        } else {
            match owner {
                Some(owner) => match owner.execution_owner_slot().as_ref() {
                    Some(original) => io.poll_progress(original, cx),
                    None => Err(WorkError::Conflict),
                },
                None => Err(WorkError::Conflict),
            }
        };
        self.io_in_foreign = false;
        match result {
            Ok(ready) => self.bootstrap_ready |= ready,
            Err(error) => self.fail(work(error)),
        }
    }
    fn start_capture(&mut self) {
        if self.call.started
            || !self.bootstrap_ready
            || self.business_failure.is_some()
            || self.closing
        {
            return;
        }
        let result = (|| {
            // The provider CPU author needs the positive Root domains
            // installed by this I/O Source's first actual grant.
            let port = self
                .cpu
                .planning_metadata_cpu_port(
                    self.command.scope(),
                    self.window.clone(),
                    self.binding.planning(),
                    self.cancellation.clone(),
                    self.runtime.clone(),
                    self.wake.clone(),
                )
                .map_err(work)?;
            self.port = Some(port.clone());
            self.port_created = true;
            let context = context_for_planning_pin_typed(
                self.binding.planning(),
                self.command.connector_context().clone(),
            )
            .map_err(connector)?
            .with_io_admission(
                self.io
                    .as_ref()
                    .expect("same positive LIKE I/O Source")
                    .clone(),
            );
            let request = ConnectorTableObjectCaptureRequest {
                table: self.binding.table().clone(),
                resolution: ConnectorTableResolution::StrictBaseTable,
                selector: ConnectorTableObjectSelector::Current,
                context,
            };
            let original = self.capture.clone();
            let planning = self.binding.planning().clone();
            self.call.future = Some(Box::pin(async move {
                original.capture_with_port(&planning, port, request).await
            }));
            self.call.started = true;
            Ok(())
        })();
        if let Err(error) = result {
            self.fail(error);
        }
    }
    pub(crate) fn poll_read(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<ConnectorTableMetadata, QueryServiceError>> {
        assert!(!self.delivered, "one original LIKE metadata handoff");
        self.wake.0.register(cx.waker());
        if self.local_health_failure().is_some() {
            return Poll::Pending;
        }
        if !self.call.started && self.business_failure.is_none() {
            if !self.window.is_for_scope(owner.scope()) {
                self.fail(work(WorkError::Conflict));
            }
            if let Some(reason) = owner.cancellation().reason() {
                self.fail(governed_cancellation_error(reason));
            }
            if self.closing {
                self.fail(work(WorkError::Released));
            }
        }
        self.setup(owner);
        self.progress_io(Some(owner), cx);
        self.start_capture();
        self.call.poll(cx);
        self.progress_io(Some(owner), cx);
        if self.local_health_failure().is_some() {
            return Poll::Pending;
        }
        if self.call.started && !self.call.exited {
            return Poll::Pending;
        }
        if !self.call.started && self.business_failure.is_none() {
            return Poll::Pending;
        }
        if let Some(Err(error)) = self.call.result.as_ref() {
            if self.business_failure.is_none() {
                self.fail(connector(error.clone()));
            }
        }
        std::task::ready!(self.poll_close(cx));
        self.delivered = true;
        if let Some(error) = self.business_failure.take() {
            return Poll::Ready(Err(error));
        }
        let result = self
            .call
            .result
            .take()
            .expect("actual LIKE capture result precedes handoff");
        match result {
            Ok(binding) => {
                // Capture, CPU and current I/O requests have actually exited.
                // Keep the positive Root payload for the caller's next phase.
                self.completed_io = Some(self.io.take().expect("same positive LIKE I/O Source"));
                Poll::Ready(Ok(binding.metadata))
            }
            Err(error) => Poll::Ready(Err(connector(error))),
        }
    }

    /// Transfer the same positive I/O Source once after successful read delivery.
    /// No request from this read remains active in it.
    pub(crate) fn take_completed_io(&mut self) -> Option<Arc<MetadataIoAdmission>> {
        self.completed_io.take()
    }

    /// Continue the same existing sources even after the borrowed read waiter
    /// is cancelled. Closing never starts a new capture or supplies a new Root.
    pub(crate) fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        self.wake.0.register(cx.waker());
        self.closing = true;
        if self.closed {
            return Poll::Ready(());
        }
        if self.local_health_failure().is_some() {
            return Poll::Pending;
        }
        self.progress_io(None, cx);
        self.call.poll(cx);
        if self.local_health_failure().is_some() || (self.call.started && !self.call.exited) {
            return Poll::Pending;
        }
        // The common owner observes the actual Future and retires its original
        // opaque Source/pin/port receivers. This is separate from CPU exit.
        std::task::ready!(self.capture.poll_closed(cx));
        if self.local_health_failure().is_some() {
            return Poll::Pending;
        }
        if self.port_created && !self.cpu_closed {
            let port = self
                .port
                .as_ref()
                .expect("created port remains in the original Source");
            self.cpu_in_foreign = true;
            let closed = port.poll_closed(cx);
            self.cpu_in_foreign = false;
            match closed {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    self.health = Some(unproven(
                        "LIKE source metadata CPU close is unproven",
                        Arc::new(error),
                    ));
                    return Poll::Pending;
                }
                Poll::Ready(Ok(())) => self.cpu_closed = true,
            }
        }
        if self.local_health_failure().is_some() {
            return Poll::Pending;
        }
        if let Some(io) = self.io.as_ref() {
            self.io_in_foreign = true;
            let withdrawn = if !self.bootstrap_ready && !self.call.started {
                io.withdraw_unstarted_in()
            } else {
                Ok(None)
            };
            self.io_in_foreign = false;
            let withdrawn = match withdrawn {
                Err(error) => {
                    self.health = Some(unproven(
                        "LIKE source I/O withdrawal is unproven",
                        Arc::new(error),
                    ));
                    return Poll::Pending;
                }
                Ok(Some(receipt)) => {
                    if let Some(error) = receipt.into_ordinary_failure() {
                        self.fail(work(error));
                    }
                    true
                }
                Ok(None) => false,
            };
            if !withdrawn {
                self.io_in_foreign = true;
                let quiet = self
                    .io
                    .as_ref()
                    .expect("same LIKE I/O Source")
                    .poll_actual_quiescent_continuing(cx);
                self.io_in_foreign = false;
                match quiet {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(health)) => {
                        self.health = Some(health);
                        return Poll::Pending;
                    }
                    Poll::Ready(Ok(receipt)) => {
                        if let Some(error) = receipt.into_ordinary_failure() {
                            self.fail(work(error));
                        }
                    }
                }
            }
        }
        // No child was created if setup did not reach the pure port constructor
        // or capture install. None/Err is never used as an exit receipt.
        self.closed = true;
        Poll::Ready(())
    }
}
fn work(error: WorkError) -> QueryServiceError {
    product_command_service_error(CommandError::from_failure_cause(SharedFailureCause::new(
        error,
    )))
}
fn connector(error: ConnectorError) -> QueryServiceError {
    product_command_service_error(command_error_from_connector(error))
}
fn unproven(
    message: &'static str,
    cause: Arc<dyn std::error::Error + Send + Sync>,
) -> StatementNativeHealthFailure {
    StatementNativeHealthFailure::actual_exit_unproven(message, cause)
}
