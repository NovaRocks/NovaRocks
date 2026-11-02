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

//! The original statement's joint admission and actual Native computation.

use super::*;
use crate::query_execution::{
    compiler::PreparedDmlWriteAssembly,
    internal_consumer::{
        decoded::{
            CowComputationOutput, DecodedConsumerRuntime, ReadyCowComputation,
            ReadyStatisticsComputation, StatisticsComputationOutput,
        },
        domain_cpu::DomainCpuCustody,
        write::{ReadyWriteComputation, WriteComputationOutput},
    },
    logical_execution::{
        InitialComputationReservation, OriginalComputationStart, PreparedLogicalComputation,
    },
    maintenance::io_admission::MetadataIoAdmission,
};
use novarocks_query_application::{
    admitted_query_context::QueryResultCapacityBinding,
    api::{
        ComputationReadyToken, ExecutionHandle, ReservedExecutionIdentity,
        StatementNativeHealthFailure,
    },
    session_control::GovernedActionContext,
};
use novarocks_workload_control::{
    WorkError, WorkOwner, WorkScope,
    joint_admission::{
        InternalComputationGrant, JointAdjustmentReceipt, JointAdmission, JointLane,
        RootStageAdmission, RootStageReady,
    },
};
use std::task::{Context, Poll};

pub(crate) struct StatementStageAdmission {
    client_window: Option<novarocks_workload_control::ResultWindowAlias>,
    root: WorkScope,
    peak_bytes: u64,
    admitted: bool,
    capacity_transferred: bool,
    child: Option<JointAdmission>,
    child_receipt: Option<JointAdjustmentReceipt>,
    owner: Option<WorkOwner>,
    owner_admission: Option<RootStageAdmission>,
    owner_ready: Option<RootStageReady>,
    compute: Option<InternalComputationGrant>,
    capacity: Option<QueryResultCapacityBinding>,
    q_wait: Option<novarocks_workload_control::joint_admission::JointProposalSlotWait>,
    busy: bool,
    health: StatementNativeHealthFailure,
}
impl StatementStageAdmission {
    pub(crate) fn new(root: WorkScope, peak_bytes: u64) -> Self {
        Self {
            client_window: None,
            root,
            peak_bytes,
            admitted: false,
            capacity_transferred: false,
            child: None,
            child_receipt: None,
            owner: None,
            owner_admission: None,
            owner_ready: None,
            compute: None,
            capacity: None,
            q_wait: None,
            busy: false,
            health: health(),
        }
    }
    pub(crate) fn with_client_window(
        mut self,
        window: &novarocks_workload_control::ResultWindowAlias,
    ) -> Result<Self, WorkError> {
        if window.class() != novarocks_workload_control::ResultWindowClass::Client
            || !window.is_for_scope(&self.root)
        {
            return Err(WorkError::Conflict);
        }
        self.client_window = Some(window.try_clone()?);
        Ok(self)
    }
    pub(crate) fn poll_admit(
        &mut self,
        io: &MetadataIoAdmission,
        root_owner: &WorkOwner,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), QueryServiceError>> {
        if self.busy {
            return Poll::Pending;
        }
        if !root_owner.is_for_scope(&self.root) {
            return Poll::Ready(Err(internal_error(
                "statement stage has a foreign original Root",
            )));
        }
        if self.admitted {
            return Poll::Ready(Ok(()));
        }
        self.busy = true;
        let polled = self.poll_admit_inner(io, cx);
        self.busy = false;
        polled
    }
    fn poll_admit_inner(
        &mut self,
        io: &MetadataIoAdmission,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), QueryServiceError>> {
        if let Some(wait) = self.q_wait.as_mut() {
            match Pin::new(wait).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => result.map_err(|e| internal_error(e.to_string()))?,
            }
            drop(self.q_wait.take());
        }
        if self.owner.is_none() {
            if self.child.is_none() && self.child_receipt.is_none() {
                match self
                    .root
                    .try_request_joint_child()
                    .map_err(|e| internal_error(e.to_string()))?
                {
                    novarocks_workload_control::joint_admission::JointTryBegin::Started(
                        request,
                    ) => self.child = Some(request),
                    novarocks_workload_control::joint_admission::JointTryBegin::Full(signal) => {
                        self.q_wait = Some(signal.into_wait());
                        let wait = self.q_wait.as_mut().unwrap();
                        match Pin::new(wait).poll(cx) {
                            Poll::Pending => return Poll::Pending,
                            Poll::Ready(result) => {
                                result.map_err(|e| internal_error(e.to_string()))?;
                                drop(self.q_wait.take());
                                cx.waker().wake_by_ref();
                                return Poll::Pending;
                            }
                        }
                    }
                }
            }
            if self.child_receipt.is_none() {
                let batch = match Pin::new(self.child.as_mut().unwrap()).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(r) => r.map_err(|e| internal_error(e.error.to_string()))?,
                };
                self.child_receipt = batch.adjustment;
                if !batch.new_claims.is_empty() {
                    return Poll::Ready(Err(internal_error(
                        "write statement child produced a physical claim",
                    )));
                }
                drop(self.child.take());
            }
            let notice = self
                .child_receipt
                .as_mut()
                .unwrap()
                .install_child_in(&self.root, &mut self.owner)
                .map_err(|e| internal_error(e.to_string()))?;
            notice.notify();
            drop(self.child_receipt.take());
        }
        if self.compute.is_none() {
            if self.owner_admission.is_none() && self.owner_ready.is_none() {
                let source = io
                    .root_stage_source(self.owner.as_ref().unwrap())
                    .map_err(|e| internal_error(e.to_string()))?;
                let source = match self.client_window.as_ref() {
                    Some(window) => source
                        .for_client_window(window)
                        .map_err(|error| internal_error(error.to_string()))?,
                    None => source,
                };
                let mut proposal = match source
                    .try_begin(self.peak_bytes, JointLane::Foreground)
                    .map_err(|e| internal_error(e.to_string()))?
                {
                    novarocks_workload_control::joint_admission::JointTryBegin::Started(
                        proposal,
                    ) => proposal,
                    novarocks_workload_control::joint_admission::JointTryBegin::Full(signal) => {
                        self.q_wait = Some(signal.into_wait());
                        let wait = self.q_wait.as_mut().unwrap();
                        match Pin::new(wait).poll(cx) {
                            Poll::Pending => return Poll::Pending,
                            Poll::Ready(result) => {
                                result.map_err(|e| internal_error(e.to_string()))?;
                                drop(self.q_wait.take());
                                cx.waker().wake_by_ref();
                                return Poll::Pending;
                            }
                        }
                    }
                };
                proposal
                    .end_payload()
                    .map_err(|e| internal_error(e.to_string()))?;
                proposal
                    .end_stage()
                    .map_err(|e| internal_error(e.to_string()))?;
                self.owner_admission = Some(
                    proposal
                        .submit()
                        .map_err(|e| internal_error(e.error.to_string()))?,
                );
            }
            if self.owner_ready.is_none() {
                let ready = match Pin::new(self.owner_admission.as_mut().unwrap()).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(r) => r.map_err(|e| internal_error(e.error.to_string()))?,
                };
                self.owner_ready = Some(ready);
                drop(self.owner_admission.take());
            }
            let notice = io
                .install_stage_in(
                    self.owner_ready.as_mut().unwrap(),
                    self.owner.as_ref().unwrap(),
                    &mut self.compute,
                )
                .map_err(|e| internal_error(e.to_string()))?;
            notice.notify();
            drop(self.owner_ready.take());
        }
        self.admitted = true;
        Poll::Ready(Ok(()))
    }
    fn materialize_internal_capacity(
        &mut self,
        io: &MetadataIoAdmission,
    ) -> Result<(), QueryServiceError> {
        if !self.admitted || self.capacity_transferred {
            return Err(internal_error(
                "statement stage is not admitted for Internal capacity",
            ));
        }
        if self.capacity.is_none() {
            self.capacity = Some(
                io.stage_capacity(self.owner.as_ref().unwrap(), self.compute.as_ref().unwrap())
                    .map_err(|error| internal_error(error.to_string()))?,
            );
        }
        Ok(())
    }
    pub(crate) fn capacity(&self) -> Option<&QueryResultCapacityBinding> {
        self.capacity.as_ref()
    }
    pub(crate) fn install_internal_read_capacity_in(
        &mut self,
        io: &MetadataIoAdmission,
        target: &mut Option<QueryResultCapacityBinding>,
    ) -> Result<(), QueryServiceError> {
        if !self.admitted || self.capacity_transferred {
            return Err(internal_error(
                "Internal read computation was not owned by this stage",
            ));
        }
        io.install_internal_read_stage_capacity_in(
            self.owner
                .as_ref()
                .ok_or_else(|| internal_error("statement stage owner is not installed"))?,
            &mut self.compute,
            target,
        )
        .map_err(|error| internal_error(error.to_string()))?;
        self.capacity_transferred = true;
        Ok(())
    }

    pub(crate) fn install_client_capacity_in(
        &mut self,
        io: &MetadataIoAdmission,
        window: &novarocks_workload_control::ResultWindowAlias,
        target: &mut Option<QueryResultCapacityBinding>,
    ) -> Result<(), QueryServiceError> {
        if !self.admitted || self.capacity_transferred {
            return Err(internal_error(
                "Client computation was not owned by this stage",
            ));
        }
        io.install_client_stage_capacity_in(
            window,
            self.owner
                .as_ref()
                .ok_or_else(|| internal_error("statement stage owner is not installed"))?,
            &mut self.compute,
            target,
        )
        .map_err(|error| internal_error(error.to_string()))?;
        self.capacity_transferred = true;
        // The exact Client computation and its window are now stable in target.
        // This preparation-only alias must not follow unrelated residual work.
        drop(self.client_window.take());
        Ok(())
    }
    pub(crate) fn owner_slot(&mut self) -> &mut Option<WorkOwner> {
        &mut self.owner
    }
    pub(crate) fn take_compute(&mut self) -> Option<InternalComputationGrant> {
        self.compute.take()
    }
    pub(crate) fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.busy.then(|| self.health.clone())
    }
    /// Requires the original launcher/consumer's actual exit, or no launch.
    pub(crate) fn retire_unlaunched(&mut self) {
        drop(self.child.take());
        drop(self.child_receipt.take());
        drop(self.owner_admission.take());
        drop(self.owner_ready.take());
        drop(self.compute.take());
        drop(self.q_wait.take());
        if let Some(owner) = self.owner.take() {
            owner.complete();
        }
        drop(self.client_window.take());
    }
    /// The original stream/result receiver must already own its exact aliases.
    pub(crate) fn release_capacity_alias(&mut self) {
        drop(self.capacity.take());
    }
}

pub(crate) trait StatementComputationOutput: Send + 'static {
    type Ready: Send + 'static;
    fn cpu_custody(&self) -> Arc<DomainCpuCustody>;
    fn take_ready(
        &mut self,
        token: ComputationReadyToken,
    ) -> Result<Self::Ready, QueryExecutionError>;
}
impl StatementComputationOutput for WriteComputationOutput {
    type Ready = ReadyWriteComputation;
    fn cpu_custody(&self) -> Arc<DomainCpuCustody> {
        self.cpu_custody()
    }
    fn take_ready(
        &mut self,
        token: ComputationReadyToken,
    ) -> Result<Self::Ready, QueryExecutionError> {
        self.take_ready(token)
    }
}
impl StatementComputationOutput for CowComputationOutput {
    type Ready = ReadyCowComputation;
    fn cpu_custody(&self) -> Arc<DomainCpuCustody> {
        self.cpu_custody()
    }
    fn take_ready(
        &mut self,
        token: ComputationReadyToken,
    ) -> Result<Self::Ready, QueryExecutionError> {
        self.take_ready(token)
    }
}

impl StatementComputationOutput for StatisticsComputationOutput {
    type Ready = ReadyStatisticsComputation;
    fn cpu_custody(&self) -> Arc<DomainCpuCustody> {
        StatisticsComputationOutput::cpu_custody(self)
    }
    fn take_ready(
        &mut self,
        token: ComputationReadyToken,
    ) -> Result<Self::Ready, QueryExecutionError> {
        StatisticsComputationOutput::take_ready(self, token)
    }
}

pub(crate) trait StatementComputationAssembly: Send + 'static {
    type Output: StatementComputationOutput;
    fn prepare(
        self,
        initial: InitialComputationReservation,
        runtime: DecodedConsumerRuntime,
    ) -> Result<
        (
            PreparedLogicalComputation,
            ReservedExecutionIdentity,
            Self::Output,
        ),
        QueryExecutionError,
    >;
}
impl StatementComputationAssembly for PreparedDmlWriteAssembly {
    type Output = WriteComputationOutput;
    fn prepare(
        self,
        initial: InitialComputationReservation,
        runtime: DecodedConsumerRuntime,
    ) -> Result<
        (
            PreparedLogicalComputation,
            ReservedExecutionIdentity,
            Self::Output,
        ),
        QueryExecutionError,
    > {
        self.prepare_computation(initial, runtime)
    }
}

impl StatementComputationAssembly
    for crate::query_execution::statistics::preparation::PreparedStatisticsComputationAssembly
{
    type Output = StatisticsComputationOutput;
    fn prepare(
        self,
        initial: InitialComputationReservation,
        runtime: DecodedConsumerRuntime,
    ) -> Result<
        (
            PreparedLogicalComputation,
            ReservedExecutionIdentity,
            Self::Output,
        ),
        QueryExecutionError,
    > {
        self.prepare_computation(initial, runtime)
    }
}

pub(crate) struct StatementComputation<A: StatementComputationAssembly> {
    admission: StatementStageAdmission,
    initial: Option<InitialComputationReservation>,
    assembly: Option<A>,
    computation: Option<PreparedLogicalComputation>,
    output: Option<A::Output>,
    original: OriginalComputationStart,
    execution: Option<Result<ExecutionHandle, QueryExecutionError>>,
    ready: Option<<A::Output as StatementComputationOutput>::Ready>,
    exit_receipt: Option<
        Result<novarocks_query_application::api::LogicalExecutionExitReceipt, QueryExecutionError>,
    >,
    close_failure: Option<StatementNativeHealthFailure>,
    ready_confirmed: bool,
    local_exit_observed: bool,
    native_tail_taken: bool,
    busy: bool,
    launcher: Arc<dyn LogicalExecutionLauncher>,
    #[cfg(novarocks_dev_controls)]
    observation: Option<crate::preparation_diagnostics::StatementObservationHandle>,
}
impl<A: StatementComputationAssembly> StatementComputation<A> {
    pub(crate) fn new(
        root: WorkScope,
        initial: InitialComputationReservation,
        launcher: Arc<dyn LogicalExecutionLauncher>,
        #[cfg(novarocks_dev_controls)] observation: Option<
            crate::preparation_diagnostics::StatementObservationHandle,
        >,
    ) -> Self {
        Self::new_with_peak(
            root,
            initial,
            launcher,
            0,
            #[cfg(novarocks_dev_controls)]
            observation,
        )
    }
    pub(crate) fn new_with_peak(
        root: WorkScope,
        initial: InitialComputationReservation,
        launcher: Arc<dyn LogicalExecutionLauncher>,
        peak_bytes: u64,
        #[cfg(novarocks_dev_controls)] observation: Option<
            crate::preparation_diagnostics::StatementObservationHandle,
        >,
    ) -> Self {
        Self {
            admission: StatementStageAdmission::new(root, peak_bytes),
            initial: Some(initial),
            assembly: None,
            computation: None,
            output: None,
            original: OriginalComputationStart::default(),
            execution: None,
            ready: None,
            exit_receipt: None,
            close_failure: None,
            ready_confirmed: false,
            local_exit_observed: false,
            native_tail_taken: false,
            busy: false,
            launcher,
            #[cfg(novarocks_dev_controls)]
            observation,
        }
    }
    pub(crate) fn poll_admit(
        &mut self,
        io: &MetadataIoAdmission,
        root_owner: &WorkOwner,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), QueryServiceError>> {
        match self.admission.poll_admit(io, root_owner, cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                Poll::Ready(result.and_then(|()| self.admission.materialize_internal_capacity(io)))
            }
        }
    }
    pub(crate) fn capacity(&self) -> Option<&QueryResultCapacityBinding> {
        self.admission.capacity()
    }
    /// The original paid negative health cause; cloning conveys no exit fact.
    pub(crate) fn retained_health(&self) -> StatementNativeHealthFailure {
        self.admission.health.clone()
    }
    pub(crate) fn preparation_connector_context(
        &self,
        context: novarocks_spi::connector::ConnectorRequestContext,
        #[cfg(novarocks_dev_controls)] nomination: Option<
            &Arc<novarocks_failpoint::target_binding::StatementWorkNomination>,
        >,
    ) -> Result<
        novarocks_spi::connector::ConnectorRequestContext,
        crate::query_execution::contract::DistributedQueryError,
    > {
        self.initial
            .as_ref()
            .ok_or_else(|| {
                crate::query_execution::contract::DistributedQueryError::new(
                    crate::query_execution::contract::DistributedQueryErrorKind::Rejected,
                    "computation preparation no longer owns its initial reservation",
                )
            })?
            .connector_request_context(
                context,
                #[cfg(novarocks_dev_controls)]
                nomination,
            )
    }

    pub(crate) fn computation_context(
        &self,
        original: &novarocks_query_application::admitted_query_context::QueryExecutionContext,
    ) -> Result<novarocks_query_application::admitted_query_context::QueryExecutionContext, WorkError>
    {
        original.for_internal_computation(
            self.admission
                .capacity()
                .cloned()
                .ok_or(WorkError::Conflict)?,
        )
    }
    pub(crate) fn install_assembly(&mut self, assembly: A) -> Result<(), QueryServiceError> {
        if self.assembly.is_some() || self.initial.is_none() {
            return Err(internal_error(
                "statement computation assembly is already installed or consumed",
            ));
        }
        self.assembly = Some(assembly);
        Ok(())
    }
    pub(crate) fn take_ready(
        &mut self,
    ) -> Option<<A::Output as StatementComputationOutput>::Ready> {
        self.ready.take()
    }
    pub(crate) fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.close_failure
            .clone()
            .or_else(|| self.admission.local_health_failure())
            .or_else(|| self.busy.then(|| self.admission.health.clone()))
    }
    pub(crate) fn poll_ready(
        &mut self,
        runtime: &(
            crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor,
            crate::native::data_runtime::FrontendDataRuntime,
        ),
        io: &Arc<MetadataIoAdmission>,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), QueryServiceError>> {
        if self.busy || self.close_failure.is_some() {
            return Poll::Pending;
        }
        self.busy = true;
        let polled = self.poll_ready_inner(runtime, io, owner, cx);
        self.busy = false;
        polled
    }
    fn poll_ready_inner(
        &mut self,
        runtime: &(
            crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor,
            crate::native::data_runtime::FrontendDataRuntime,
        ),
        io: &Arc<MetadataIoAdmission>,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), QueryServiceError>> {
        if self.ready_confirmed {
            return Poll::Ready(Ok(()));
        }
        if self.initial.is_some() && self.computation.is_none() && self.original.ready.is_none() {
            let root_owner = owner
                .execution_owner_slot()
                .as_ref()
                .ok_or_else(|| internal_error("statement lost its original Root"))?;
            match self.poll_admit(io, root_owner, cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => result?,
            }
            let (blocking, data) = runtime;
            let runtime = DecodedConsumerRuntime {
                binding: self.admission.capacity().unwrap().clone(),
                computation: self.admission.take_compute(),
                cpu: blocking
                    .provider_creator_cpu()
                    .map_err(|e| internal_error(e.to_string()))?,
                runtime: data.clone(),
                cancellation: QueryCancellationView::governed(
                    owner.cancellation().clone(),
                    owner.timeout_ms(),
                ),
            };
            let assembly = self
                .assembly
                .take()
                .expect("one original frozen write statement assembly");
            let (computation, reservation, output) = assembly
                .prepare(self.initial.take().unwrap(), runtime)
                .map_err(governed_query_execution_error)?;
            self.computation = Some(computation);
            self.original.reservation = Some(reservation);
            self.output = Some(output);
        }
        if self.computation.is_some() {
            let capacity = self.admission.capacity().unwrap().clone();
            let tail = self
                .launcher
                .start_computation_in(
                    &mut self.computation,
                    self.admission.owner_slot(),
                    &capacity,
                    io,
                    &mut self.original,
                    #[cfg(novarocks_dev_controls)]
                    self.observation.clone(),
                )
                .map_err(governed_query_execution_error)?;
            tail.finish().map_err(governed_query_execution_error)?;
        }
        if let Some(future) = self.original.ready.as_mut() {
            match future.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => self.execution = Some(result),
            }
            drop(self.original.ready.take());
        }
        if let Some(result) = self.execution.take() {
            match result {
                Err(error) => return Poll::Ready(Err(governed_query_execution_error(error))),
                Ok(execution) => {
                    let token = *execution.ready_token().ok_or_else(|| {
                        internal_error("write statement has no actual ComputationReady")
                    })?;
                    self.ready = Some(
                        self.output
                            .as_mut()
                            .unwrap()
                            .take_ready(token)
                            .map_err(governed_query_execution_error)?,
                    );
                    self.ready_confirmed = true;
                    drop(execution);
                }
            }
        }
        Poll::Ready(Ok(()))
    }

    /// A real Ready and the actual local CPU join permit the next business
    /// phase. The original Native exit observer stays in this same Source.
    pub(crate) fn poll_ready_local_exit(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        if self.busy || self.close_failure.is_some() {
            return Poll::Pending;
        }
        if !self.ready_confirmed {
            return Poll::Ready(Err(self.admission.health.clone()));
        }
        if self.local_exit_observed {
            return Poll::Ready(Ok(()));
        }
        self.busy = true;
        let polled = self.poll_local_cpu_exit(cx);
        self.busy = false;
        if matches!(polled, Poll::Ready(Ok(()))) {
            self.local_exit_observed = true;
        }
        polled
    }
    fn poll_local_cpu_exit(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        if self.local_exit_observed {
            return Poll::Ready(Ok(()));
        }
        if self.output.is_none()
            && (self.original.ready.is_some()
                || self.original.exit.is_some()
                || self.ready_confirmed)
        {
            self.close_failure = Some(self.admission.health.clone());
            return Poll::Pending;
        }
        if let Some(output) = &self.output {
            let cpu = output.cpu_custody();
            cpu.begin_close();
            match cpu.poll_join_closed(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(cause)) => {
                    self.close_failure = Some(StatementNativeHealthFailure::actual_exit_unproven(
                        "statement local computation exit is unproven",
                        cause,
                    ));
                    return Poll::Pending;
                }
                Poll::Ready(Ok(())) => {}
            }
        }
        Poll::Ready(Ok(()))
    }
    pub(crate) fn poll_actual_exit(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        self.poll_refused_exit(cx)
    }
    pub(crate) fn poll_refused_exit(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        if self.busy || self.close_failure.is_some() {
            return Poll::Pending;
        }
        self.busy = true;
        let polled = self.poll_refused_exit_inner(cx);
        self.busy = false;
        polled
    }
    fn poll_refused_exit_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        match self.poll_local_cpu_exit(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(result) => result?,
        }
        self.local_exit_observed = true;
        if let Some(exit) = self.original.exit.as_mut() {
            match exit.poll_exit(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(value) => self.exit_receipt = Some(value),
            }
            drop(self.original.exit.take());
        }
        if self.exit_receipt.as_ref().is_some_and(Result::is_err) {
            // The exact original failure remains in exit_receipt.
            self.close_failure = Some(self.admission.health.clone());
            return Poll::Pending;
        }
        drop(self.computation.take());
        self.admission.retire_unlaunched();
        Poll::Ready(Ok(()))
    }
    /// Only the original failed stage may identify its formation increment.
    /// Its result holders must then actually Drop before this can be retired.
    pub(crate) fn refused_formation_bytes(&self) -> Option<u64> {
        (self.admission.admitted
            && self.local_exit_observed
            && self.original.exit.is_none()
            && !self.exit_receipt.as_ref().is_some_and(Result::is_err)
            && !self.ready_confirmed
            && self.close_failure.is_none())
        .then_some(self.admission.peak_bytes)
    }
    pub(crate) fn release_result_aliases(&mut self) -> Result<(), StatementNativeHealthFailure> {
        if !self.local_exit_observed || self.ready.is_some() {
            return Err(self.admission.health.clone());
        }
        drop(self.output.take());
        drop(self.assembly.take());
        self.admission.release_capacity_alias();
        Ok(())
    }
    /// Install the exact original observer before a fallible domain observation.
    /// Refusal and unwind leave that observer in the caller's stable tail slot.
    pub(crate) fn transfer_native_tail_then<T, E>(
        &mut self,
        target: &mut Option<StatementNativeTail>,
        observe: impl FnOnce() -> Result<T, E>,
    ) -> Result<Result<T, E>, StatementNativeHealthFailure> {
        if target.is_some() {
            return Err(self.admission.health.clone());
        }
        self.release_result_aliases()?;
        *target = Some(self.take_native_tail()?);
        Ok(observe())
    }
    pub(crate) fn take_native_tail(
        &mut self,
    ) -> Result<StatementNativeTail, StatementNativeHealthFailure> {
        if self.native_tail_taken
            || !self.local_exit_observed
            || !self.ready_confirmed
            || self.ready.is_some()
            || self.output.is_some()
            || self.assembly.is_some()
            || self.admission.capacity().is_some()
            || self.original.ready.is_some()
            || self.original.input.is_some()
            || self.original.request.is_some()
            || self.original.reservation.is_some()
            || (self.original.exit.is_none() && self.exit_receipt.is_none())
            || self.busy
            || self.close_failure.is_some()
        {
            return Err(self.admission.health.clone());
        }
        self.native_tail_taken = true;
        Ok(StatementNativeTail {
            exit: self.original.exit.take(),
            receipt: self.exit_receipt.take(),
            health: self.admission.health.clone(),
            busy: false,
            failed: false,
        })
    }
}
/// Same exact original Native observer after the actual local Ready handoff.
/// It carries no computation grant, CPU observer or second task owner.
pub(crate) struct StatementNativeTail {
    exit: Option<novarocks_query_application::api::LogicalExecutionExitObserver>,
    receipt: Option<
        Result<novarocks_query_application::api::LogicalExecutionExitReceipt, QueryExecutionError>,
    >,
    health: StatementNativeHealthFailure,
    busy: bool,
    failed: bool,
}
impl StatementNativeTail {
    pub(crate) fn retained_heap_bytes() -> Result<usize, WorkError> {
        novarocks_type_contract::owned_resources::layout::arc_layout(std::alloc::Layout::new::<
            SourceExitUnproven,
        >())
        .map(|layout| layout.size())
        .map_err(|_| WorkError::ArithmeticOverflow)
    }
    pub(crate) fn poll_exit(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        if self.busy || self.failed {
            return Poll::Pending;
        }
        if self.exit.is_none() && self.receipt.is_none() {
            self.failed = true;
            return Poll::Pending;
        }
        self.busy = true;
        if let Some(exit) = self.exit.as_mut() {
            match exit.poll_exit(cx) {
                Poll::Pending => {
                    self.busy = false;
                    return Poll::Pending;
                }
                Poll::Ready(value) => self.receipt = Some(value),
            }
            drop(self.exit.take());
        }
        self.failed = self.receipt.as_ref().is_some_and(Result::is_err);
        self.busy = false;
        if self.failed {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }
    pub(crate) fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        (self.busy || self.failed).then(|| self.health.clone())
    }
}
#[derive(Debug)]
struct SourceExitUnproven;
impl std::fmt::Display for SourceExitUnproven {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("statement computation poll or Future Drop did not return normally")
    }
}
impl std::error::Error for SourceExitUnproven {}
fn health() -> StatementNativeHealthFailure {
    StatementNativeHealthFailure::actual_exit_unproven(
        "statement computation local exit is unproven",
        Arc::new(SourceExitUnproven),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_execution::{
        internal_consumer::domain_state::InternalDomainState,
        internal_result_cpu::{InternalResultCpuOwner, admitted_internal_fixture},
        statistics::StatisticsRootResultDecoder,
    };
    use novarocks_query_application::cancellation::QueryCancellationSource;
    struct NoNativeLaunch;
    impl LogicalExecutionLauncher for NoNativeLaunch {
        #[cfg(novarocks_dev_controls)]
        fn resolve_statement_observation(
            &self,
            _: &novarocks_failpoint::target_binding::StatementWorkNomination,
        ) -> Option<crate::preparation_diagnostics::StatementObservationHandle> {
            panic!("this fixture cannot resolve a Native statement observation")
        }

        fn start_read_in(
            &self,
            _: &mut Option<crate::query_execution::completion::PreparedLogicalRead>,
            _: &mut Option<WorkOwner>,
            _: &QueryResultCapacityBinding,
            _: &Arc<crate::query::read_action::io_source::ReadMetadataIoSource>,
            _: &mut OriginalComputationStart,
            #[cfg(novarocks_dev_controls)] _: Option<
                crate::preparation_diagnostics::StatementObservationHandle,
            >,
        ) -> Result<
            novarocks_query_application::api::LogicalExecutionStartAttempt,
            QueryExecutionError,
        > {
            panic!("the CPU-exit fixture cannot start a Native read")
        }
        fn reserve_identity(&self) -> Result<ReservedExecutionIdentity, QueryExecutionError> {
            panic!("Local EXPLAIN cannot reserve Native work")
        }
        fn start_computation_in(
            &self,
            _: &mut Option<crate::query_execution::logical_execution::PreparedLogicalComputation>,
            _: &mut Option<WorkOwner>,
            _: &QueryResultCapacityBinding,
            _: &Arc<MetadataIoAdmission>,
            _: &mut OriginalComputationStart,
            #[cfg(novarocks_dev_controls)] _: Option<
                crate::preparation_diagnostics::StatementObservationHandle,
            >,
        ) -> Result<
            novarocks_query_application::api::LogicalExecutionStartAttempt,
            QueryExecutionError,
        > {
            panic!("Local EXPLAIN cannot start Native work")
        }
    }

    struct OriginalCpuOutput {
        cpu: Arc<DomainCpuCustody>,
    }
    impl StatementComputationOutput for OriginalCpuOutput {
        type Ready = ();
        fn cpu_custody(&self) -> Arc<DomainCpuCustody> {
            self.cpu.clone()
        }
        fn take_ready(&mut self, _: ComputationReadyToken) -> Result<(), QueryExecutionError> {
            unreachable!("the actual local-close boundary test does not fabricate a Ready token")
        }
    }
    struct OriginalCpuAssembly;
    impl StatementComputationAssembly for OriginalCpuAssembly {
        type Output = OriginalCpuOutput;
        fn prepare(
            self,
            _: InitialComputationReservation,
            _: DecodedConsumerRuntime,
        ) -> Result<
            (
                PreparedLogicalComputation,
                ReservedExecutionIdentity,
                Self::Output,
            ),
            QueryExecutionError,
        > {
            unreachable!("this test exercises the retained actual CPU observer")
        }
    }
    #[tokio::test(flavor = "current_thread")]
    async fn actual_local_join_is_required_and_does_not_fabricate_a_native_exit_observer() {
        let (_control, root, binding, capacity) = admitted_internal_fixture();
        let mut cpu_owner = InternalResultCpuOwner::try_new_for_test().unwrap();
        let cancellation = QueryCancellationSource::new();
        let cpu =
            DomainCpuCustody::try_new(
                InternalDomainState::Statistics(
                    StatisticsRootResultDecoder::for_test_with_capacity([], &binding),
                ),
                binding.clone(),
                cpu_owner.runtime(),
                crate::native::data_runtime::FrontendDataRuntime::new(
                    tokio::runtime::Handle::current(),
                ),
                cancellation.view(),
            )
            .unwrap();
        // The Ready boundary is already recorded; this test exercises its
        // distinct actual local-CPU join and the absent Native-observer guard.
        let mut helper = StatementComputation::<OriginalCpuAssembly> {
            admission: StatementStageAdmission::new(
                root.owner.scope(),
                crate::query_execution::internal_result_cpu::INTERNAL_PEAK_BYTES,
            ),
            initial: None,
            assembly: None,
            computation: None,
            output: Some(OriginalCpuOutput { cpu: cpu.clone() }),
            original: OriginalComputationStart::default(),
            execution: None,
            ready: None,
            exit_receipt: None,
            close_failure: None,
            ready_confirmed: true,
            local_exit_observed: false,
            native_tail_taken: false,
            busy: false,
            launcher: Arc::new(NoNativeLaunch),
            #[cfg(novarocks_dev_controls)]
            observation: None,
        };
        assert!(helper.release_result_aliases().is_err());
        assert!(helper.output.is_some());
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(matches!(
            helper.poll_ready_local_exit(&mut cx),
            Poll::Ready(Ok(()))
        ));
        assert!(helper.release_result_aliases().is_ok());
        assert!(helper.output.is_none());
        assert!(helper.take_native_tail().is_err());
        cpu.join_closed().await.unwrap();
        drop(helper);
        drop(cpu);
        drop(binding);
        root.owner.complete();
        root.business.release();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        cpu_owner
            .shutdown_until(std::time::Instant::now() + std::time::Duration::from_secs(5))
            .await
            .unwrap();
    }
    #[test]
    fn absent_native_observer_and_absent_receipt_never_report_actual_exit() {
        let mut tail = StatementNativeTail {
            exit: None,
            receipt: None,
            health: health(),
            busy: false,
            failed: false,
        };
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(tail.poll_exit(&mut cx).is_pending());
        assert!(tail.local_health_failure().is_some());
    }
    #[test]
    fn internal_read_capacity_moves_same_stage_grant_through_original_mio() {
        use novarocks_workload_control::joint_admission::{
            JointProgress, JointProgressDriver, ResponsibilityConfig,
        };
        use novarocks_workload_control::{
            ControlStorageConfig, ResultCapacityConfig, WorkloadConfig, WorkloadControl,
        };
        fn drive(driver: &mut JointProgressDriver) {
            for _ in 0..10000 {
                if matches!(driver.step(), JointProgress::Idle(_)) {
                    return;
                }
            }
            panic!("original finite stage driver did not become idle");
        }
        let count = 64;
        let control = WorkloadControl::try_new_counted(WorkloadConfig {
            root_limit: count,
            business_limit: count,
            query_concurrency_limit: count,
            scope_records_limit: count,
            ..WorkloadConfig::default()
        })
        .unwrap()
        .owner;
        let mut config = ResultCapacityConfig::V1;
        config.positions[2] = 1;
        let result_capacity = control.configure_result_capacity(config).unwrap();
        let (_, mut driver) = control
            .configure_responsibility(ResponsibilityConfig {
                work_per_turn: std::num::NonZeroUsize::new(7).unwrap(),
            })
            .unwrap();
        control
            .configure_control_storage(ControlStorageConfig::new(count, &[]))
            .unwrap();
        control.mark_ready().unwrap();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let io = MetadataIoAdmission::for_original_source(&root.owner, Vec::new()).unwrap();
        let mut stage = StatementStageAdmission::new(root.owner.scope(), 0);
        let mut cx = Context::from_waker(std::task::Waker::noop());
        let mut admitted = false;
        for _ in 0..10000 {
            drive(&mut driver);
            if !io.poll_progress(&root.owner, &mut cx).unwrap() {
                continue;
            }
            match stage.poll_admit(&io, &root.owner, &mut cx) {
                Poll::Pending => {}
                Poll::Ready(result) => {
                    result.unwrap();
                    admitted = true;
                    break;
                }
            }
        }
        assert!(
            admitted,
            "the actual Root/MIO stage must admit its real position"
        );
        assert_eq!(result_capacity.snapshot().held_positions, [0, 0, 1, 0]);
        let mut target = Some(QueryResultCapacityBinding::from_root_owner(&root.owner).unwrap());
        assert!(
            stage
                .install_internal_read_capacity_in(&io, &mut target)
                .is_err()
        );
        assert!(stage.compute.is_some() && !stage.capacity_transferred);
        assert!(
            target
                .as_ref()
                .unwrap()
                .window_alias()
                .internal_manifest_binding()
                .unwrap()
                .is_none()
        );
        drop(target.take());
        stage
            .install_internal_read_capacity_in(&io, &mut target)
            .unwrap();
        assert!(stage.compute.is_none() && stage.capacity_transferred);
        let binding = target.as_ref().unwrap();
        assert_eq!(
            binding.class(),
            novarocks_workload_control::ResultWindowClass::Internal
        );
        assert_eq!(
            binding.scope().id(),
            stage.owner.as_ref().unwrap().scope().id()
        );
        assert!(
            binding
                .window_alias()
                .internal_manifest_binding()
                .unwrap()
                .is_some()
        );
        assert!(
            stage
                .install_internal_read_capacity_in(&io, &mut target)
                .is_err()
        );
        // This stage never launched Native work. Its original receiving value
        // actually exits before its positive position and stage owner retire.
        drop(target.take());
        assert_eq!(result_capacity.snapshot().held_positions, [0; 4]);
        stage.retire_unlaunched();
        drive(&mut driver);
        assert!(matches!(
            io.poll_actual_quiescent_continuing(&mut cx),
            Poll::Ready(Ok(_))
        ));
        drop((stage, io));
        root.business.release();
        root.owner.complete();
        drive(&mut driver);
        control.close_admission();
        assert!(control.shutdown().is_ok());
    }

    include!("statement_computation_count_handoff_tests.rs");
}
