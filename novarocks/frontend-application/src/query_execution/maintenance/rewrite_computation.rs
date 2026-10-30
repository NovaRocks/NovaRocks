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

//! One frozen rewrite cohort borrows the original common computation owner.
//! This adapter contains no coordinator, publication runner or scheduling policy.

use super::{PreparedDistributedRewriteCohort, io_admission::MetadataIoAdmission};
use crate::query::statement_computation::{
    StatementComputation, StatementComputationAssembly, StatementNativeTail,
};
use crate::query_execution::{
    internal_consumer::{
        decoded::DecodedConsumerRuntime,
        write::{ReadyWriteComputation, WriteComputationOutput},
    },
    logical_execution::{
        InitialComputationReservation, LogicalExecutionLauncher, PreparedLogicalComputation,
    },
    write_session::ConnectorWriteSession,
};
use novarocks_query_application::{
    api::{QueryExecutionError, ReservedExecutionIdentity, StatementNativeHealthFailure},
    session_control::GovernedActionContext,
    session_error::{QueryServiceError, QueryServiceErrorKind},
};
use novarocks_workload_control::{WorkError, WorkScope};
use std::{
    sync::Arc,
    task::{Context, Poll},
};

impl StatementComputationAssembly for PreparedDistributedRewriteCohort {
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

/// An original receiving fact, not another prepared set. The common Write
/// consumer already appended its payload to the same session. There is no
/// conversion into legacy ConnectorWriteSessionCompletion/accumulate.
pub(crate) struct ReadyRewriteCohort {
    _original: ReadyWriteComputation,
}
/// Inline in the original maintenance Action. The same MIO and neutral
/// context are borrowed at poll time; this creates no Root/runtime/permit.
pub(crate) struct RewriteCohortComputation {
    root: WorkScope,
    expected_session: Arc<ConnectorWriteSession>,
    computation: StatementComputation<PreparedDistributedRewriteCohort>,
    ready: Option<ReadyWriteComputation>,
    native_tail: Option<StatementNativeTail>,
    handed_off: bool,
    failure: Option<StatementNativeHealthFailure>,
}
impl RewriteCohortComputation {
    /// The real Stage is admitted before cohort planning. This is the SAME
    /// common computation source; it does not mint a preparation-only P.
    pub(crate) fn empty(
        root: WorkScope,
        initial: InitialComputationReservation,
        launcher: Arc<dyn LogicalExecutionLauncher>,
        expected_session: Arc<ConnectorWriteSession>,
        #[cfg(novarocks_dev_controls)] observation: Option<
            crate::preparation_diagnostics::StatementObservationHandle,
        >,
    ) -> Self {
        Self {
            computation: StatementComputation::new(
                root.clone(),
                initial,
                launcher,
                #[cfg(novarocks_dev_controls)]
                observation,
            ),
            root,
            expected_session,
            ready: None,
            native_tail: None,
            handed_off: false,
            failure: None,
        }
    }
    pub(crate) fn poll_admit(
        &mut self,
        io: &MetadataIoAdmission,
        owner: &novarocks_workload_control::WorkOwner,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), QueryServiceError>> {
        self.computation.poll_admit(io, owner, cx)
    }
    pub(crate) fn preparation_execution(
        &self,
        original: &novarocks_query_application::admitted_query_context::QueryExecutionContext,
    ) -> Result<novarocks_query_application::admitted_query_context::QueryExecutionContext, WorkError>
    {
        self.computation.computation_context(original)
    }
    pub(crate) fn preparation_context(
        &self,
        original: novarocks_spi::connector::ConnectorRequestContext,
        #[cfg(novarocks_dev_controls)] nomination: Option<
            &Arc<novarocks_failpoint::target_binding::StatementWorkNomination>,
        >,
    ) -> Result<
        novarocks_spi::connector::ConnectorRequestContext,
        crate::query_execution::contract::DistributedQueryError,
    > {
        self.computation.preparation_connector_context(
            original,
            #[cfg(novarocks_dev_controls)]
            nomination,
        )
    }
    pub(crate) fn install_prepared(
        &mut self,
        prepared: PreparedDistributedRewriteCohort,
    ) -> Result<(), QueryServiceError> {
        if !Arc::ptr_eq(&self.expected_session, &prepared.write_session) {
            return Err(QueryServiceError::new(
                QueryServiceErrorKind::Internal,
                "Rewrite prepared cohort belongs to another original session",
            ));
        }
        self.computation.install_assembly(prepared)
    }
    /// Success requires the real Ready token and original local CPU exit.
    /// Unrelated Native cleanup stays in the SAME retained observer.
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
        if !owner
            .execution_owner_slot()
            .as_ref()
            .is_some_and(|original| original.is_for_scope(&self.root))
        {
            return Poll::Ready(Err(QueryServiceError::new(
                QueryServiceErrorKind::Internal,
                "rewrite cohort borrowed a foreign original Root owner",
            )));
        }
        if self.failure.is_some() {
            return Poll::Pending;
        }
        if self.handed_off {
            return Poll::Ready(Err(QueryServiceError::new(
                QueryServiceErrorKind::Internal,
                "rewrite cohort Ready was already handed to its original receiver",
            )));
        }
        if self.native_tail.is_some() {
            return Poll::Ready(Ok(()));
        }
        if self.ready.is_none() {
            match self.computation.poll_ready(runtime, io, owner, cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => result?,
            }
            self.ready = self.computation.take_ready();
            let Some(ready) = self.ready.as_ref() else {
                return Poll::Ready(Err(QueryServiceError::new(
                    QueryServiceErrorKind::Internal,
                    "rewrite cohort has no actual Write ComputationReady",
                )));
            };
            if !Arc::ptr_eq(&ready.session, &self.expected_session)
                || !ready.accepted.belongs_to(&self.expected_session)
            {
                self.failure = Some(self.computation.retained_health());
                return Poll::Pending;
            }
        }
        match self.computation.poll_ready_local_exit(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(failure)) => {
                self.failure = Some(failure);
                return Poll::Pending;
            }
            Poll::Ready(Ok(())) => {}
        }
        if let Err(failure) = self.computation.release_result_aliases() {
            self.failure = Some(failure);
            return Poll::Pending;
        }
        match self.computation.take_native_tail() {
            Ok(tail) => self.native_tail = Some(tail),
            Err(failure) => {
                self.failure = Some(failure);
                return Poll::Pending;
            }
        }
        Poll::Ready(Ok(()))
    }
    /// Validate BOTH stable receiving cells before either original value
    /// moves. Next cohort may start after the local handoff; this receiver
    /// retains the same Native tail until actual exit, independent of Ready.
    pub(crate) fn handoff_ready_in(
        &mut self,
        ready: &mut Option<ReadyRewriteCohort>,
        tail: &mut Option<StatementNativeTail>,
    ) -> Result<(), StatementNativeHealthFailure> {
        if self.handed_off
            || self.failure.is_some()
            || ready.is_some()
            || tail.is_some()
            || self.ready.is_none()
            || self.native_tail.is_none()
        {
            return Err(self.computation.retained_health());
        }
        *ready = self.ready.take().map(|original| ReadyRewriteCohort {
            _original: original,
        });
        *tail = self.native_tail.take();
        self.handed_off = true;
        Ok(())
    }
    /// Refusal/cancel retains actual original observers. After handoff this
    /// adapter cannot assert its receiver's exit.
    pub(crate) fn poll_actual_exit(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        if self.failure.is_some() {
            return Poll::Pending;
        }
        if self.handed_off {
            return Poll::Ready(Err(self.computation.retained_health()));
        }
        match self.native_tail.as_mut() {
            Some(tail) => tail.poll_exit(cx),
            None => self.computation.poll_actual_exit(cx),
        }
    }
    pub(crate) fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.failure
            .clone()
            .or_else(|| self.computation.local_health_failure())
            .or_else(|| {
                self.native_tail
                    .as_ref()
                    .and_then(StatementNativeTail::local_health_failure)
            })
    }
}
