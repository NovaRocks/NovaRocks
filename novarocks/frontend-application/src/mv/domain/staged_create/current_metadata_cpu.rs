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

//! Current borrows the selected metadata CPU author and one original capture.
use crate::query_execution::{
    internal_result_cpu::InternalResultCpu, maintenance::io_admission::MetadataIoAdmission,
};
use crate::task_execution::status_intake::StatusIntakeWake;
use novarocks_query_application::{
    api::StatementNativeHealthFailure, cancellation::QueryCancellationView,
};
use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorError, ConnectorErrorKind,
    ConnectorProcessLocalPayload, ConnectorRequestContext, ConnectorTableObjectBinding,
    ConnectorTableObjectCaptureRequest, ProviderMetadataCpuPort,
};
use novarocks_workload_control::{ResultWindowAlias, WorkError, WorkScope};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll},
};

pub(crate) struct CurrentProjectionMetadataCpu {
    cpu: InternalResultCpu,
    root: WorkScope,
    window: ResultWindowAlias,
    control: Control,
    runtime: tokio::runtime::Handle,
    wake: Arc<dyn StatusIntakeWake>,
}
enum Control {
    Business(QueryCancellationView),
    ExistingObligation(Box<ExistingObligation>),
}
struct ExistingObligation {
    io: Arc<MetadataIoAdmission>,
    context: ConnectorRequestContext,
}
impl CurrentProjectionMetadataCpu {
    pub(crate) fn business(
        cpu: InternalResultCpu,
        root: WorkScope,
        window: ResultWindowAlias,
        cancel: QueryCancellationView,
        runtime: tokio::runtime::Handle,
        wake: Arc<dyn StatusIntakeWake>,
    ) -> Self {
        Self {
            cpu,
            root,
            window,
            control: Control::Business(cancel),
            runtime,
            wake,
        }
    }
    pub(crate) fn existing_obligation(
        cpu: InternalResultCpu,
        root: WorkScope,
        window: ResultWindowAlias,
        io: Arc<MetadataIoAdmission>,
        obligation: ConnectorRequestContext,
        runtime: tokio::runtime::Handle,
        wake: Arc<dyn StatusIntakeWake>,
    ) -> Self {
        Self {
            cpu,
            root,
            window,
            control: Control::ExistingObligation(Box::new(ExistingObligation {
                io,
                context: obligation,
            })),
            runtime,
            wake,
        }
    }
    fn request_context(
        &self,
        request: &ConnectorRequestContext,
    ) -> Result<ConnectorRequestContext, WorkError> {
        match &self.control {
            Control::Business(_) => Ok(request.clone()),
            Control::ExistingObligation(obligation) => {
                let ExistingObligation { io, context } = obligation.as_ref();
                io.check_existing_metadata_obligation_context(&self.root, request)?;
                // Explicitly selected original obligation only. Keep this
                // request's fresh observation and exact selected capabilities.
                Ok(request
                    .for_existing_obligation(context.deadline(), context.stop().clone())
                    .with_necessary_finalization_deadline_cap(context.deadline()))
            }
        }
    }
    pub(crate) fn port_for(
        &self,
        lease: &ConnectorControlPlanningLease,
    ) -> Result<Arc<dyn ProviderMetadataCpuPort>, WorkError> {
        let window = self.window.try_clone()?;
        match &self.control {
            Control::Business(cancel) => self.cpu.planning_metadata_cpu_port(
                &self.root,
                window,
                lease,
                cancel.clone(),
                self.runtime.clone(),
                Arc::clone(&self.wake),
            ),
            Control::ExistingObligation(obligation) => {
                let ExistingObligation { io, context } = obligation.as_ref();
                self.cpu.planning_metadata_settlement_cpu_port(
                    &self.root,
                    window,
                    lease,
                    Arc::clone(io),
                    context.clone(),
                    self.runtime.clone(),
                    Arc::clone(&self.wake),
                )
            }
        }
    }
}

#[derive(Debug)]
pub(crate) enum CaptureFailure {
    Admission(WorkError),
    Provider(ConnectorError),
}
impl CaptureFailure {
    pub(super) fn into_projection(self) -> novarocks_mv_application::readiness::MvProjectionError {
        match self {
            Self::Provider(error) => super::current_projection_connector(error),
            Self::Admission(error) => {
                let cause = novarocks_failure_contract::SharedFailureCause::new(error);
                super::current_projection_conflict(cause.detail()).with_original_failure(cause)
            }
        }
    }
    pub(crate) fn into_application(self) -> crate::mv::domain::application::MvApplicationError {
        use crate::mv::domain::application::{MvApplicationError, MvApplicationErrorKind};
        match self {
            Self::Provider(error) => {
                MvApplicationError::from_connector(MvApplicationErrorKind::Engine, error)
            }
            Self::Admission(error) => {
                let cause = novarocks_failure_contract::SharedFailureCause::new(error);
                MvApplicationError::new(MvApplicationErrorKind::Engine, cause.detail())
                    .with_original_failure(cause)
            }
        }
    }
}

type Raw = Result<(ConnectorTableObjectBinding, Option<i64>), ConnectorError>;
type CaptureFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
struct Inputs {
    lease: ConnectorControlPlanningLease,
    port: Arc<dyn ProviderMetadataCpuPort>,
    request: ConnectorTableObjectCaptureRequest,
}
#[derive(Default)]
struct Call {
    active: bool,
    inputs: Option<Arc<Inputs>>,
    future: Option<CaptureFuture>,
    owner: Option<ConnectorProcessLocalPayload>,
    raw: Option<Raw>,
    in_foreign: bool,
    exited: bool,
    owners_retired: bool,
}
pub(crate) struct CurrentMetadataCapture {
    call: Mutex<Call>,
    health: StatementNativeHealthFailure,
}
#[derive(Debug)]
struct CaptureExitUnproven;
impl std::fmt::Display for CaptureExitUnproven {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MV Current metadata capture exit is unproven")
    }
}
impl std::error::Error for CaptureExitUnproven {}
// The original F owns this temporary loan, and is itself retained by the
// receiving header. Weak receiver edges prevent a header/F custody cycle.
struct OwnerLoan {
    receiver: Weak<CurrentMetadataCapture>,
    owner: Option<ConnectorProcessLocalPayload>,
}
impl Drop for OwnerLoan {
    fn drop(&mut self) {
        let Some(receiver) = self.receiver.upgrade() else {
            // The receiving header itself is being destroyed. Its scheduled
            // job still has the original QA retirement owner; this Drop emits
            // no completion/exit fact and must not panic during another unwind.
            return;
        };
        let mut call = receiver.call.lock().unwrap_or_else(|p| p.into_inner());
        assert!(
            call.owner.is_none(),
            "one original metadata Source receiver"
        );
        call.owner = self.owner.take();
    }
}
struct FutureLoan<'a> {
    receiver: &'a CurrentMetadataCapture,
    future: Option<CaptureFuture>,
}
impl Drop for FutureLoan<'_> {
    fn drop(&mut self) {
        if let Some(future) = self.future.take() {
            let mut call = self.receiver.call.lock().unwrap_or_else(|p| p.into_inner());
            assert!(
                call.future.is_none(),
                "same original capture Future receiver"
            );
            call.future = Some(future);
        }
    }
}
impl CurrentMetadataCapture {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            call: Mutex::new(Call::default()),
            health: StatementNativeHealthFailure::actual_exit_unproven(
                "MV Current metadata capture exit is unproven",
                Arc::new(CaptureExitUnproven),
            ),
        })
    }
    fn install(
        self: &Arc<Self>,
        lease: &ConnectorControlPlanningLease,
        port: Arc<dyn ProviderMetadataCpuPort>,
        request: ConnectorTableObjectCaptureRequest,
    ) -> Result<(), ConnectorError> {
        let inputs = Arc::new(Inputs {
            lease: lease.clone(),
            port,
            request,
        });
        let mut call = self.call.lock().unwrap_or_else(|p| p.into_inner());
        if call.active || call.in_foreign {
            drop(call);
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "MV Current metadata original receiving is occupied",
            ));
        }
        // No factory/foreign poll occurs while this mutex is held.
        let receiver = Arc::downgrade(self);
        let original_inputs = Arc::clone(&inputs);
        let future = Box::pin(async move {
            let mut owner = OwnerLoan {
                receiver: receiver.clone(),
                owner: None,
            };
            let mut original = original_inputs
                .lease
                .binding()
                .metadata()
                .capture_table_object_with_snapshot_cpu_in(
                    original_inputs.request.clone(),
                    &original_inputs.port,
                    &mut owner.owner,
                );
            std::future::poll_fn(|cx| match original.as_mut().poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(raw) => {
                    let receiver = receiver.upgrade().expect("same original Current receiving");
                    let mut call = receiver.call.lock().unwrap_or_else(|p| p.into_inner());
                    assert!(call.raw.is_none(), "one original capture result");
                    call.raw = Some(raw);
                    Poll::Ready(())
                }
            })
            .await;
            // Actual raw result precedes both Provider F and Source return.
            drop(original);
            drop(owner);
        });
        *call = Call {
            active: true,
            inputs: Some(inputs),
            future: Some(future),
            ..Call::default()
        };
        Ok(())
    }
    pub(crate) fn poll_progress(&self, cx: &mut Context<'_>) -> Poll<()> {
        let future = {
            let mut call = self.call.lock().unwrap_or_else(|p| p.into_inner());
            if call.in_foreign {
                return Poll::Pending;
            }
            if !call.active || call.exited {
                return Poll::Ready(());
            }
            let future = call.future.take().expect("same original Current capture F");
            call.in_foreign = true;
            future
        };
        let mut loan = FutureLoan {
            receiver: self,
            future: Some(future),
        };
        match loan.future.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Pending => {
                let mut call = self.call.lock().unwrap_or_else(|p| p.into_inner());
                call.future = loan.future.take();
                call.in_foreign = false;
                Poll::Pending
            }
            Poll::Ready(()) => {
                // raw is already stable. Panic in F Drop retains that raw and
                // sticky unproven exit; the same poisoned F is not repolled.
                drop(loan.future.take());
                let mut call = self.call.lock().unwrap_or_else(|p| p.into_inner());
                call.exited = true;
                call.in_foreign = false;
                Poll::Ready(())
            }
        }
    }
    pub(crate) fn poll_closed(&self, cx: &mut Context<'_>) -> Poll<()> {
        std::task::ready!(self.poll_progress(cx));
        let retired = {
            let mut call = self.call.lock().unwrap_or_else(|p| p.into_inner());
            if call.in_foreign {
                return Poll::Pending;
            }
            if !call.active || call.owners_retired {
                return Poll::Ready(());
            }
            assert!(
                call.exited,
                "actual capture F exit before Source retirement"
            );
            call.in_foreign = true;
            (call.owner.take(), call.inputs.take())
        };
        // All unique SDK/port/lease destructors remain outside the mutex. The
        // actual raw result is still retained if any one of them panics.
        drop(retired);
        let mut call = self.call.lock().unwrap_or_else(|p| p.into_inner());
        call.owners_retired = true;
        call.in_foreign = false;
        Poll::Ready(())
    }
    pub(crate) fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        let (unproven, port) = {
            let call = self.call.lock().unwrap_or_else(|p| p.into_inner());
            (
                call.in_foreign,
                call.inputs.as_ref().map(|i| Arc::clone(&i.port)),
            )
        };
        if unproven {
            return Some(self.health.clone());
        }
        port.and_then(|port| port.local_failure()).map(|cause| {
            StatementNativeHealthFailure::actual_exit_unproven(
                "MV Current metadata CPU source exit is unproven",
                cause,
            )
        })
    }
    pub(crate) async fn capture(
        self: &Arc<Self>,
        lease: &ConnectorControlPlanningLease,
        factory: &CurrentProjectionMetadataCpu,
        mut request: ConnectorTableObjectCaptureRequest,
    ) -> Result<ConnectorTableObjectBinding, CaptureFailure> {
        request.context = factory
            .request_context(&request.context)
            .map_err(CaptureFailure::Admission)?;
        let port = factory.port_for(lease).map_err(CaptureFailure::Admission)?;
        self.install(lease, port, request)
            .map_err(CaptureFailure::Provider)?;
        std::future::poll_fn(|cx| self.poll_closed(cx)).await;
        let raw = {
            let mut call = self.call.lock().unwrap_or_else(|p| p.into_inner());
            assert!(
                call.exited && call.owners_retired && !call.in_foreign,
                "only actual capture exit hands off metadata"
            );
            let raw = call.raw.take().expect("original capture result");
            call.active = false;
            raw
        };
        raw.map(|(binding, _snapshot)| binding)
            .map_err(CaptureFailure::Provider)
    }
}
