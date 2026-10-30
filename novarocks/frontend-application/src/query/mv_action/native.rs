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

//! One MV data computation borrows the existing statement stage author.
//! Preparation and Current never receive this stage's capacity binding.

use crate::query::statement_computation::{StatementComputation, StatementNativeTail};
use crate::query_execution::{
    logical_execution::{InitialComputationReservation, LogicalExecutionLauncher},
    maintenance::io_admission::MetadataIoAdmission,
    mv_native_write::PreparedMvNativeWriteAssembly,
    write_session::ConnectorWriteSession,
};
use novarocks_query_application::{
    api::StatementNativeHealthFailure,
    session_control::GovernedActionContext,
    session_error::{QueryServiceError, QueryServiceErrorKind},
};
use novarocks_workload_control::WorkScope;
use std::{
    sync::Arc,
    task::{Context, Poll},
};

type Runtime = (
    crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor,
    crate::native::data_runtime::FrontendDataRuntime,
);

/// Stored in the same step until its actual Native exit, including failure.
/// A caller's borrowed poll owns no receipt or stage grant.
pub(super) struct MvNativePhase {
    computation: StatementComputation<PreparedMvNativeWriteAssembly>,
    assembly: Option<PreparedMvNativeWriteAssembly>,
    session: Arc<ConnectorWriteSession>,
    tail: Option<StatementNativeTail>,
    selected: Option<Result<(), QueryServiceError>>,
    health: Option<StatementNativeHealthFailure>,
    assembly_installed: bool,
    ready_received: bool,
    ready_local_exit: bool,
}
impl MvNativePhase {
    pub(super) fn new(
        root: WorkScope,
        initial: InitialComputationReservation,
        launcher: Arc<dyn LogicalExecutionLauncher>,
        assembly: PreparedMvNativeWriteAssembly,
        #[cfg(novarocks_dev_controls)] observation: Option<
            crate::preparation_diagnostics::StatementObservationHandle,
        >,
    ) -> Self {
        let session = assembly.write_session().clone();
        Self {
            computation: StatementComputation::new(
                root,
                initial,
                launcher,
                #[cfg(novarocks_dev_controls)]
                observation,
            ),
            assembly: Some(assembly),
            session,
            tail: None,
            selected: None,
            health: None,
            assembly_installed: false,
            ready_received: false,
            ready_local_exit: false,
        }
    }
    pub(super) fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.health
            .clone()
            .or_else(|| self.computation.local_health_failure())
            .or_else(|| {
                self.tail
                    .as_ref()
                    .and_then(StatementNativeTail::local_health_failure)
            })
    }
    fn fail(&mut self, error: QueryServiceError) {
        if self.selected.is_none() {
            self.selected = Some(Err(error));
        }
    }
    pub(super) fn poll_ready(
        &mut self,
        runtime: &Runtime,
        io: &Arc<MetadataIoAdmission>,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), QueryServiceError>> {
        if self.health.is_some() {
            return Poll::Pending;
        }
        if let Some(result) = &self.selected {
            return Poll::Ready(result.clone());
        }
        if !self.assembly_installed {
            let root = owner.execution_owner_slot().as_ref().ok_or_else(|| {
                QueryServiceError::new(
                    QueryServiceErrorKind::Internal,
                    "MV computation lost its original statement Root",
                )
            })?;
            match self.computation.poll_admit(io, root, cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    self.fail(error);
                    return Poll::Ready(self.selected.as_ref().unwrap().clone());
                }
                Poll::Ready(Ok(())) => {}
            }
            if let Err(error) = self
                .computation
                .install_assembly(self.assembly.take().expect("original MV frozen assembly"))
            {
                self.fail(error);
                return Poll::Ready(self.selected.as_ref().unwrap().clone());
            }
            self.assembly_installed = true;
        }
        if !self.ready_received {
            match self.computation.poll_ready(runtime, io, owner, cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    self.fail(error);
                    return Poll::Ready(self.selected.as_ref().unwrap().clone());
                }
                Poll::Ready(Ok(())) => {}
            }
            // This proof was minted by the real shared consumer, after its
            // original accepted segment was deposited in THIS session.
            let ready = self
                .computation
                .take_ready()
                .expect("actual MV ComputationReady");
            if !Arc::ptr_eq(&ready.session, &self.session)
                || !ready.accepted.belongs_to(&self.session)
            {
                self.fail(QueryServiceError::new(
                    QueryServiceErrorKind::Internal,
                    "MV computation Ready belongs to another write session",
                ));
                return Poll::Ready(self.selected.as_ref().unwrap().clone());
            }
            // The exact accepted payload and terminal credential resolver are
            // already held by the original session. No Ready result window
            // may leak into the subsequent publication/Current phase.
            drop(ready);
            self.ready_received = true;
        }
        if !self.ready_local_exit {
            match self.computation.poll_ready_local_exit(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(health)) => {
                    self.health = Some(health);
                    return Poll::Pending;
                }
                Poll::Ready(Ok(())) => {}
            }
            if let Err(health) = self.computation.release_result_aliases() {
                self.health = Some(health);
                return Poll::Pending;
            }
            match self.computation.take_native_tail() {
                Ok(tail) => self.tail = Some(tail),
                Err(health) => {
                    self.health = Some(health);
                    return Poll::Pending;
                }
            }
            self.ready_local_exit = true;
        }
        self.selected = Some(Ok(()));
        Poll::Ready(Ok(()))
    }
    /// Observe only the tail already detached by actual computation Ready.
    /// A still-running computation keeps its normal business polling owner.
    pub(super) fn poll_ready_tail(&mut self, cx: &mut Context<'_>) {
        if let Some(tail) = self.tail.as_mut()
            && let Poll::Ready(Err(health)) = tail.poll_exit(cx)
        {
            self.health.get_or_insert(health);
        }
    }
    /// Selection never asserts remote actual exit. The same step/residual
    /// continues this original observer independently of publication readiness.
    pub(super) fn poll_closed(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        if self.health.is_some() {
            return Poll::Pending;
        }
        let result = match self.tail.as_mut() {
            Some(tail) => tail.poll_exit(cx),
            None => self.computation.poll_refused_exit(cx),
        };
        match result {
            Poll::Ready(Err(health)) => {
                self.health = Some(health.clone());
                Poll::Ready(Err(health))
            }
            result => result,
        }
    }
}
