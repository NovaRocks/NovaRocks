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
use crate::wake::StatusIntakeWake;
use novarocks_query_application::{
    api::StatementNativeHealthFailure, cancellation::QueryCancellationView,
};
use novarocks_spi::connector::{
    ConnectorControlPlanningPin, ConnectorError, ConnectorRequestContext,
    ConnectorTableObjectBinding, ConnectorTableObjectCaptureRequest, ProviderMetadataCpuPort,
};
use novarocks_workload_control::{ResultWindowAlias, WorkError, WorkScope};
use std::sync::Arc;

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
        lease: &ConnectorControlPlanningPin,
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

pub(crate) type CurrentMetadataCapture = crate::query_execution::metadata_capture::MetadataCapture;

#[derive(Debug)]
struct CaptureExitUnproven;
impl std::fmt::Display for CaptureExitUnproven {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MV Current metadata capture exit is unproven")
    }
}
impl std::error::Error for CaptureExitUnproven {}
pub(crate) fn new_current_metadata_capture() -> Arc<CurrentMetadataCapture> {
    CurrentMetadataCapture::new(
        StatementNativeHealthFailure::actual_exit_unproven(
            "MV Current metadata capture exit is unproven",
            Arc::new(CaptureExitUnproven),
        ),
        "MV Current metadata original receiving is occupied",
        "MV Current metadata CPU source exit is unproven",
    )
}

impl CurrentMetadataCapture {
    pub(crate) async fn capture(
        self: &Arc<Self>,
        lease: &ConnectorControlPlanningPin,
        factory: &CurrentProjectionMetadataCpu,
        mut request: ConnectorTableObjectCaptureRequest,
    ) -> Result<ConnectorTableObjectBinding, CaptureFailure> {
        request.context = factory
            .request_context(&request.context)
            .map_err(CaptureFailure::Admission)?;
        let port = factory.port_for(lease).map_err(CaptureFailure::Admission)?;
        self.capture_with_port(lease, port, request)
            .await
            .map_err(CaptureFailure::Provider)
    }
}
