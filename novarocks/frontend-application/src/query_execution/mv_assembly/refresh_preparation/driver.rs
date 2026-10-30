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

//! The original MV action retains every pure turn and its actual CPU receipt.
//! The I/O gate runs only after the preceding worker and forwarder have exited.

use super::{async_original::MvRefreshPreparationOriginal, io_gate::MvRefreshPreparationIo};
use crate::query_execution::{
    internal_result_cpu::{InternalResultCpu, MvCpuJob, MvRefreshCpuTurn},
    mv_assembly::activation_cpu::MvWriteActivationOriginal,
};
use crate::task_execution::status_intake::StatusIntakeWake;
use novarocks_query_application::{
    api::StatementNativeHealthFailure, cancellation::QueryCancellationView,
    execution_error::ExecutionFailureInput,
};
use novarocks_workload_control::{ResultWindowAlias, WorkError, WorkScope};
use std::{
    sync::Arc,
    task::{Context, Poll},
};

/// Preserve the real CPU selection separately from a domain preparation error.
/// A failed physical exit is health evidence, never a successful business result.
#[derive(Clone, Debug)]
pub(crate) enum MvTurnFailure {
    Admission(WorkError),
    Cpu(Arc<ExecutionFailureInput>),
    Health(StatementNativeHealthFailure),
}

/// This is the actual selected CPU input, including its site and full fact.
/// It is only supporting health evidence while the original foreign poison is
/// set; it does not claim to recover the physical worker's raw panic payload.
#[derive(Debug)]
struct SelectedCpuFailure(ExecutionFailureInput);
impl std::fmt::Display for SelectedCpuFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}
impl std::error::Error for SelectedCpuFailure {}

enum Original {
    Preparation(Arc<MvRefreshPreparationOriginal>),
    Activation(Arc<MvWriteActivationOriginal>),
}
impl Original {
    fn foreign_exit_unproven(&self) -> bool {
        match self {
            Self::Preparation(original) => original.foreign_exit_unproven(),
            Self::Activation(original) => original.foreign_exit_unproven(),
        }
    }
    fn turn(&self) -> MvRefreshCpuTurn {
        match self {
            Self::Preparation(original) => MvRefreshCpuTurn::Preparation(original.clone()),
            Self::Activation(original) => MvRefreshCpuTurn::Activation(original.clone()),
        }
    }
}

/// Inline in the existing Action. A borrowed poll/close future never owns the
/// original reader, CPU source, worker receipt or raw failure by itself.
pub(crate) struct MvRefreshTurnCustody {
    io: Arc<MvRefreshPreparationIo>,
    cpu: InternalResultCpu,
    root: WorkScope,
    window: ResultWindowAlias,
    cancellation: QueryCancellationView,
    runtime: tokio::runtime::Handle,
    wake: Arc<dyn StatusIntakeWake>,
    original: Option<Original>,
    job: Option<MvCpuJob>,
    marker: Option<Result<(), ExecutionFailureInput>>,
    failure: Option<MvTurnFailure>,
    waiting_io: bool,
    ready: bool,
    closed: bool,
}
impl MvRefreshTurnCustody {
    pub(crate) fn new(
        io: Arc<MvRefreshPreparationIo>,
        cpu: InternalResultCpu,
        root: WorkScope,
        window: ResultWindowAlias,
        cancellation: QueryCancellationView,
        runtime: tokio::runtime::Handle,
        wake: Arc<dyn StatusIntakeWake>,
    ) -> Self {
        Self {
            io,
            cpu,
            root,
            window,
            cancellation,
            runtime,
            wake,
            original: None,
            job: None,
            marker: None,
            failure: None,
            waiting_io: false,
            ready: false,
            closed: false,
        }
    }
    fn install(&mut self, original: Original) -> Result<(), Original> {
        if self.original.is_some()
            || self.job.is_some()
            || self.marker.is_some()
            || self.failure.is_some()
            || self.ready
            || self.closed
        {
            return Err(original);
        }
        self.original = Some(original);
        Ok(())
    }
    /// Rejection returns the same original; it never destroys an occupied turn.
    pub(crate) fn install_preparation(
        &mut self,
        original: Arc<MvRefreshPreparationOriginal>,
    ) -> Result<(), Arc<MvRefreshPreparationOriginal>> {
        if !original.is_for_io(&self.io) {
            return Err(original);
        }
        self.install(Original::Preparation(original))
            .map_err(|original| {
                let Original::Preparation(original) = original else {
                    unreachable!()
                };
                original
            })
    }
    pub(crate) fn install_activation(
        &mut self,
        original: Arc<MvWriteActivationOriginal>,
    ) -> Result<(), Arc<MvWriteActivationOriginal>> {
        self.install(Original::Activation(original))
            .map_err(|original| {
                let Original::Activation(original) = original else {
                    unreachable!()
                };
                original
            })
    }
    fn fail(&mut self, failure: MvTurnFailure) -> Poll<Result<(), MvTurnFailure>> {
        self.failure = Some(failure.clone());
        Poll::Ready(Err(failure))
    }
    pub(crate) fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        if let Some(MvTurnFailure::Health(failure)) = &self.failure {
            return Some(failure.clone());
        }
        self.job
            .as_ref()
            .and_then(|job| job.local_health_failure())
            .or_else(|| self.io.local_health_failure())
            .or_else(|| match self.original.as_ref() {
                Some(Original::Preparation(original)) => original.local_health_failure(),
                _ => None,
            })
    }
    pub(crate) fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), MvTurnFailure>> {
        if let Some(failure) = &self.failure {
            return Poll::Ready(Err(failure.clone()));
        }
        assert!(!self.closed, "closed MV turn cannot prepare new work");
        if self.ready {
            return Poll::Ready(Ok(()));
        }
        assert!(
            self.original.is_some(),
            "MV action installs its original before polling"
        );
        loop {
            if self.waiting_io {
                let Some(Original::Preparation(original)) = self.original.as_ref() else {
                    unreachable!("only the original preparation can await its named I/O")
                };
                if let Some(failure) = original.local_health_failure() {
                    return self.fail(MvTurnFailure::Health(failure));
                }
                std::task::ready!(original.poll_io(cx));
                self.waiting_io = false;
            }
            if self.job.is_none() {
                let source = match self.cpu.provider_creator_source(&self.root) {
                    Ok(source) => source,
                    Err(error) => return self.fail(MvTurnFailure::Admission(error)),
                };
                self.job = Some(self.cpu.submit_mv_refresh_turn(
                    self.original.as_ref().unwrap().turn(),
                    self.window.clone(),
                    source,
                    self.cancellation.clone(),
                    &self.runtime,
                    self.wake.clone(),
                ));
            }
            match self
                .job
                .as_mut()
                .unwrap()
                .poll_finish_in(cx, &mut self.marker)
            {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(cause)) => {
                    let failure = StatementNativeHealthFailure::actual_exit_unproven(
                        "original MV pure turn actual exit is unproven",
                        cause,
                    );
                    return self.fail(MvTurnFailure::Health(failure));
                }
                Poll::Ready(Ok(())) => {}
            }
            // This exact job received its marker and joined its worker and
            // forwarder. Never remove it on cancellation or merely Ready.
            if let Some(Err(failure)) = &self.marker {
                if self.original.as_ref().unwrap().foreign_exit_unproven() {
                    let health = self
                        .job
                        .as_ref()
                        .unwrap()
                        .local_health_failure()
                        .unwrap_or_else(|| {
                            StatementNativeHealthFailure::actual_exit_unproven(
                                "original MV foreign turn remains poisoned after its CPU failure",
                                Arc::new(SelectedCpuFailure(failure.clone())),
                            )
                        });
                    return self.fail(MvTurnFailure::Health(health));
                }
                return self.fail(MvTurnFailure::Cpu(Arc::new(failure.clone())));
            }
            assert!(matches!(self.marker, Some(Ok(()))), "same MV CPU marker");
            self.job.take();
            self.marker.take();
            match self.original.as_ref().unwrap() {
                Original::Activation(_) => {
                    self.ready = true;
                    return Poll::Ready(Ok(()));
                }
                Original::Preparation(original) if original.is_ready() => {
                    self.ready = true;
                    return Poll::Ready(Ok(()));
                }
                Original::Preparation(_) => self.waiting_io = true,
            }
            // A completed named I/O edge makes one subsequent pure turn
            // eligible. It never executes Provider work on the CPU worker.
        }
    }
    pub(crate) fn preparation_original(&self) -> &Arc<MvRefreshPreparationOriginal> {
        assert!(self.ready && self.job.is_none());
        let Some(Original::Preparation(original)) = &self.original else {
            panic!("MV turn is not the original preparation")
        };
        original
    }
    pub(crate) fn activation_original(&self) -> &Arc<MvWriteActivationOriginal> {
        assert!(self.ready && self.job.is_none());
        let Some(Original::Activation(original)) = &self.original else {
            panic!("MV turn is not the original activation")
        };
        original
    }
    /// The Action first receives the phase output, then retires this empty
    /// source. Failed or partially formed turns must keep their original.
    pub(crate) fn retire_transferred(&mut self) -> Result<(), &'static str> {
        if !self.ready || self.job.is_some() || self.marker.is_some() || self.failure.is_some() {
            return Err("MV pure turn has not completed its original transfer");
        }
        self.original.take();
        self.waiting_io = false;
        self.ready = false;
        self.closed = false;
        Ok(())
    }
    /// Drives only already owned work. The original job retains its selected
    /// failure even if its Product waiter stopped before taking a marker.
    pub(crate) fn poll_closed(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        if self.closed {
            return Poll::Ready(Ok(()));
        }
        if let Some(job) = self.job.as_mut() {
            match job.poll_closed(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(cause)) => {
                    let failure = StatementNativeHealthFailure::actual_exit_unproven(
                        "original MV CPU close is unproven",
                        cause,
                    );
                    self.failure = Some(MvTurnFailure::Health(failure.clone()));
                    return Poll::Ready(Err(failure));
                }
                Poll::Ready(Ok(())) => {}
            }
        }
        if let Some(Original::Preparation(original)) = self.original.as_ref() {
            std::task::ready!(original.poll_closed(cx))?;
        }
        // A prior business phase may have transferred its reader while Current
        // cleanup remained active. The SAME strong I/O owner is independent of
        // that reader and remains supervised until its actual full close.
        std::task::ready!(self.io.poll_closed(cx))?;
        // These aliases and the original stay retained until their Action
        // receives any unconsumed domain output and returned access.
        self.closed = true;
        Poll::Ready(Ok(()))
    }
}
