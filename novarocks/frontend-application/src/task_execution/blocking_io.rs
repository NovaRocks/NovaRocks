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

//! Supervision for blocking Connector calls made by the frontend.
//!
//! Original Connector call/source custody. New source consumers borrow their
//! stable fields and use the installed original CPU/QB owners; actual result
//! delivery is distinct from worker exit. Physical work and raw failures stay
//! with the installed original CPU/QB owners and their stable source receipts.

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use novarocks_workload_control::{ResultWindowAlias, WorkError, WorkOwner, WorkScope};

use tokio::runtime::Handle;

mod child_admission;
mod stable_job;
pub(crate) use child_admission::ConnectorBlockingIoAdmission;
pub(crate) use stable_job::ConnectorOperationJob;

/// Why a submitted blocking call produced no Connector outcome.
#[derive(Clone, Debug)]
pub(crate) struct ConnectorBlockingIoError {
    detail: String,
    original: Arc<OriginalFailure>,
}

struct OriginalFailure {
    cause: OriginalFailureCause,
    // Raw cause and backing remain under the same source disposition.
    // Their field order does not establish nested-unwind or actual-exit proof.
    _backing: Box<[ConnectorBlockingIoBacking]>,
}

enum OriginalFailureCause {
    Preserving(novarocks_query_application::cpu::QueryBlockingFailure),
    Admission(WorkError),
    CpuPublication {
        _refusal: novarocks_query_application::cpu::QueryCpuPublishRefusal,
    },
    BlockingReserve {
        _refusal: novarocks_query_application::cpu::QueryBlockingReserveRefusal,
    },
    BlockingPublication {
        _refusal: novarocks_query_application::cpu::QueryBlockingPublishRefusal,
    },
}

impl fmt::Debug for OriginalFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OriginalConnectorBlockingFailure")
    }
}

impl fmt::Display for OriginalFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("original connector blocking-I/O failure")
    }
}
impl std::error::Error for OriginalFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.cause {
            OriginalFailureCause::Preserving(error) => Some(error),
            OriginalFailureCause::Admission(error) => Some(error),
            OriginalFailureCause::CpuPublication { .. }
            | OriginalFailureCause::BlockingReserve { .. }
            | OriginalFailureCause::BlockingPublication { .. } => None,
        }
    }
}
impl ConnectorBlockingIoError {
    /// Alias the existing opaque carrier, without moving or formatting its raw
    /// panic payload and without allocating a second payload wrapper.
    pub(crate) fn health_cause(&self) -> Arc<dyn std::error::Error + Send + Sync> {
        Arc::clone(&self.original) as Arc<dyn std::error::Error + Send + Sync>
    }
}

impl fmt::Display for ConnectorBlockingIoError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for ConnectorBlockingIoError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.original.cause {
            OriginalFailureCause::Preserving(error) => Some(error),
            OriginalFailureCause::Admission(error) => Some(error),
            OriginalFailureCause::CpuPublication { .. }
            | OriginalFailureCause::BlockingReserve { .. }
            | OriginalFailureCause::BlockingPublication { .. } => None,
        }
    }
}

/// One source/call responsibility, derived before dispatch from the original
/// admitted scope. The existing scope-record bound limits these cells; this
/// does not acquire a second business, execution or Connector permit.
pub(crate) struct ConnectorBlockingIoResponsibility(Arc<ResponsibilityCell>);

struct ResponsibilityCell {
    owner: Option<WorkOwner>,
    pending_joins: AtomicUsize,
    _window: Option<ResultWindowAlias>,
}

impl Drop for ResponsibilityCell {
    fn drop(&mut self) {
        let Some(owner) = self.owner.take() else {
            return;
        };
        if *self.pending_joins.get_mut() == 0 {
            // This private child owns no receiver or remote cancellation
            // delivery. Its last backing alias and every original join have
            // exited, so its own pending Cancel is now obsolete.
            owner.complete_after_terminal_cancel_settled();
        } else {
            // An original owner lost before observing its actual job exit
            // has no exit proof. WorkOwner::drop records an orphan, keeping
            // Host drain fail-closed instead of inventing a successful join.
            drop(owner);
        }
    }
}

impl ConnectorBlockingIoResponsibility {
    pub(crate) fn admit(scope: &WorkScope, window: &ResultWindowAlias) -> Result<Self, WorkError> {
        if !window.is_for_scope(scope) {
            return Err(WorkError::Conflict);
        }
        let owner = scope.child_inherited()?;
        let delegated = match window.for_child(&owner.scope()) {
            Ok(window) => window,
            Err(error) => {
                // No call, source or receiver was installed for this child.
                owner.complete_after_terminal_cancel_settled();
                return Err(error);
            }
        };
        Ok(Self(Arc::new(ResponsibilityCell {
            owner: Some(owner),
            pending_joins: AtomicUsize::new(0),
            _window: Some(delegated),
        })))
    }

    pub(crate) fn scope(&self) -> Result<WorkScope, WorkError> {
        self.0
            .owner
            .as_ref()
            .map(WorkOwner::scope)
            .ok_or(WorkError::Released)
    }
    pub(crate) fn window(&self) -> Result<ResultWindowAlias, WorkError> {
        self.0._window.clone().ok_or(WorkError::Released)
    }
    /// Fixed backing alias, retained after all other fields of an enclosing
    /// source/result have been destroyed. It cannot authorize another job.
    pub(crate) fn retain_backing(&self) -> ConnectorBlockingIoBacking {
        ConnectorBlockingIoBacking(Arc::clone(&self.0))
    }

    pub(crate) fn join_pin(&self) -> ConnectorBlockingIoJoinPin {
        ConnectorBlockingIoJoinPin::new(&self.0)
    }
}

#[derive(Clone)]
pub(crate) struct ConnectorBlockingIoBacking(#[allow(dead_code)] Arc<ResponsibilityCell>);

/// Remains in the original header outside the panic-able synchronous apply.
/// Only observing that job's original actual-exit receipt may mark it joined.
pub(crate) struct ConnectorBlockingIoJoinPin {
    cell: Arc<ResponsibilityCell>,
    joined: AtomicBool,
}

impl ConnectorBlockingIoJoinPin {
    fn new(cell: &Arc<ResponsibilityCell>) -> Self {
        cell.pending_joins
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
            .expect("finite original Connector joins cannot overflow");
        Self {
            cell: Arc::clone(cell),
            joined: AtomicBool::new(false),
        }
    }
}

impl Drop for ConnectorBlockingIoJoinPin {
    fn drop(&mut self) {
        if self.joined.load(Ordering::Acquire) {
            let previous = self.cell.pending_joins.fetch_sub(1, Ordering::AcqRel);
            assert!(previous != 0, "original Connector join pin underflow");
        }
    }
}

/// Borrowed access to one original job's fixed primary close-result slot.
/// Installing a cause is not completion: only that job's original join does it.
pub(crate) struct ConnectorSourceCloseResult<'a> {
    failure: &'a Mutex<Option<Arc<novarocks_spi::connector::ConnectorError>>>,
    worker_failure:
        Option<&'a Mutex<Option<novarocks_query_application::cpu::QueryBlockingFailure>>>,
    source_drop_unproven: Option<&'a AtomicBool>,
}
impl<'a> ConnectorSourceCloseResult<'a> {
    #[cfg(test)]
    pub(crate) fn for_fixture(
        failure: &'a Mutex<Option<Arc<novarocks_spi::connector::ConnectorError>>>,
        worker_failure: &'a Mutex<Option<novarocks_query_application::cpu::QueryBlockingFailure>>,
        source_drop_unproven: &'a AtomicBool,
    ) -> Self {
        Self {
            failure,
            worker_failure: Some(worker_failure),
            source_drop_unproven: Some(source_drop_unproven),
        }
    }

    pub(crate) fn record_source_drop_panic(&self, original: Box<dyn std::any::Any + Send>) {
        if let Some(unproven) = self.source_drop_unproven {
            unproven.store(true, Ordering::Release);
        }
        self.record_panic(original);
    }
    pub(crate) fn record_panic(&self, original: Box<dyn std::any::Any + Send>) {
        let failure = novarocks_query_application::cpu::QueryBlockingFailure::panicked(original);
        let rejected = if let Some(slot) = self.worker_failure {
            let mut slot = slot.lock().unwrap_or_else(|e| e.into_inner());
            if slot.is_none() {
                *slot = Some(failure);
                None
            } else {
                Some(failure)
            }
        } else {
            Some(failure)
        };
        // The same first raw cause is stable before any later foreign Drop.
        drop(rejected);
    }
    pub(crate) fn record_error(&self, error: novarocks_spi::connector::ConnectorError) {
        let rejected = {
            let mut first = self
                .failure
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if first.is_none() {
                *first = Some(Arc::new(error));
                None
            } else {
                Some(error)
            }
        };
        // A later opaque cause may itself have a destructor. The primary is
        // already stable; no foreign destructor runs under the result lock.
        drop(rejected);
    }
}

#[derive(Clone)]
struct OriginalExecutors {
    cpu: novarocks_query_application::cpu::QueryCpuExecutor,
    blocking: novarocks_query_application::cpu::QueryBlockingExecutor,
    provider_creator:
        std::sync::OnceLock<crate::query_execution::internal_result_cpu::InternalResultCpu>,
}
/// The one process owner that supervises frontend Connector blocking calls.
#[derive(Clone)]
pub(crate) struct ConnectorBlockingIoSupervisor {
    runtime: Handle,
    executors: Arc<std::sync::OnceLock<OriginalExecutors>>,
}

impl ConnectorBlockingIoSupervisor {
    pub(crate) fn new(runtime: Handle) -> Self {
        Self {
            runtime,
            executors: Arc::new(std::sync::OnceLock::new()),
        }
    }
    /// Only the role composition owner has both original physical views. No
    /// fallback pool is created by a Connector, session or Operator clone.
    pub(crate) fn install_original_executors(
        &self,
        cpu: novarocks_query_application::cpu::QueryCpuExecutor,
        blocking: novarocks_query_application::cpu::QueryBlockingExecutor,
    ) -> Result<(), WorkError> {
        self.executors
            .set(OriginalExecutors {
                cpu,
                blocking,
                provider_creator: std::sync::OnceLock::new(),
            })
            .map_err(|_| WorkError::Conflict)
    }

    /// Same already-created fixed CPU owner; mandatory production composition
    /// calls this before Ready. No fallback pool is constructed by a consumer.
    pub(crate) fn install_provider_creator_cpu(
        &self,
        cpu: crate::query_execution::internal_result_cpu::InternalResultCpu,
    ) -> Result<(), WorkError> {
        self.executors
            .get()
            .ok_or(WorkError::NotReady)?
            .provider_creator
            .set(cpu)
            .map_err(|_| WorkError::Conflict)
    }
    /// Read preparation uses the SAME installed runtime owners. This is no
    /// Provider, catalog-route, byte-budget, or source-readiness permission.
    pub(crate) fn validate_original_read_runtime(&self) -> Result<(), WorkError> {
        let original = self.executors.get().ok_or(WorkError::NotReady)?;
        original.provider_creator.get().ok_or(WorkError::NotReady)?;
        Ok(())
    }
    pub(crate) fn provider_creator_cpu(
        &self,
    ) -> Result<crate::query_execution::internal_result_cpu::InternalResultCpu, WorkError> {
        self.executors
            .get()
            .and_then(|e| e.provider_creator.get())
            .cloned()
            .ok_or(WorkError::NotReady)
    }

    /// The runtime this lane's work is admitted onto.
    ///
    /// Statement preparation runs on its own threads, not on this runtime, so
    /// it needs the handle to await anything that reaches the lane.
    pub(crate) const fn runtime(&self) -> &Handle {
        &self.runtime
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use novarocks_workload_control::{WorkClass, WorkRequest};
    use std::time::{Duration, Instant};

    use super::*;

    #[tokio::test]
    async fn read_runtime_requires_the_actual_original_executors_and_creator() {
        let supervisor = ConnectorBlockingIoSupervisor::new(Handle::current());
        let missing_runtime = matches!(
            supervisor.validate_original_read_runtime(),
            Err(WorkError::NotReady)
        );
        let mut physical = OriginalExecutorsFixture::install(&supervisor);
        let missing_creator = matches!(
            supervisor.validate_original_read_runtime(),
            Err(WorkError::NotReady)
        );
        let mut control = novarocks_workload_control::WorkloadControl::try_new_counted(
            novarocks_workload_control::WorkloadConfig::default(),
        )
        .unwrap();
        let config = novarocks_query_application::cpu::QueryBlockingExecutorConfig::new(
            std::num::NonZeroUsize::new(1).unwrap(),
            std::num::NonZeroUsize::new(1).unwrap(),
        );
        let mut creator =
            crate::query_execution::internal_result_cpu::InternalResultCpuOwner::try_new(
                config,
                &mut control.owner,
            )
            .unwrap();
        supervisor
            .install_provider_creator_cpu(creator.runtime())
            .unwrap();
        let installed = supervisor.validate_original_read_runtime().is_ok();
        drop(supervisor);
        creator
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        physical.shutdown().await;
        control.owner.close_admission();
        let exited = control.owner.shutdown();
        assert!(missing_runtime);
        assert!(missing_creator);
        assert!(installed);
        assert!(exited.is_ok());
    }

    pub(crate) struct OriginalExecutorsFixture {
        cpu: novarocks_query_application::cpu::QueryCpuExecutorOwner,
        blocking: novarocks_query_application::cpu::QueryBlockingExecutorOwner,
    }
    impl OriginalExecutorsFixture {
        pub(crate) async fn shutdown(&mut self) {
            self.cpu
                .shutdown_until(Instant::now() + Duration::from_secs(5))
                .await
                .unwrap();
            self.blocking
                .shutdown_until(Instant::now() + Duration::from_secs(5))
                .await
                .unwrap();
        }
    }
    pub(crate) fn original_executors(
        handle: Handle,
    ) -> (OriginalExecutorsFixture, ConnectorBlockingIoSupervisor) {
        let supervisor = ConnectorBlockingIoSupervisor::new(handle);
        let fixture = OriginalExecutorsFixture::install(&supervisor);
        (fixture, supervisor)
    }
    impl OriginalExecutorsFixture {
        pub(crate) fn install(supervisor: &ConnectorBlockingIoSupervisor) -> Self {
            use novarocks_query_application::cpu::{
                QueryBlockingExecutorConfig, QueryBlockingExecutorOwner, QueryCpuExecutorConfig,
                QueryCpuExecutorOwner,
            };
            let cpu = QueryCpuExecutorOwner::try_new(QueryCpuExecutorConfig::with_idle_keepalive(
                Duration::from_secs(1),
            ))
            .unwrap();
            let blocking = QueryBlockingExecutorOwner::try_new(QueryBlockingExecutorConfig::new(
                std::num::NonZeroUsize::new(1).unwrap(),
                std::num::NonZeroUsize::new(1).unwrap(),
            ))
            .unwrap();
            supervisor
                .install_original_executors(cpu.executor(), blocking.executor())
                .unwrap();
            Self { cpu, blocking }
        }
    }

    pub(crate) fn admitted() -> (
        novarocks_workload_control::WorkloadControl,
        novarocks_workload_control::RootWork,
        novarocks_workload_control::ResultWindowGrant,
    ) {
        admitted_class(novarocks_workload_control::ResultWindowClass::Internal)
    }

    pub(crate) fn admitted_class(
        class: novarocks_workload_control::ResultWindowClass,
    ) -> (
        novarocks_workload_control::WorkloadControl,
        novarocks_workload_control::RootWork,
        novarocks_workload_control::ResultWindowGrant,
    ) {
        use novarocks_workload_control::{ResultCapacityConfig, WorkloadConfig, WorkloadControl};
        let control = WorkloadControl::try_new_counted(WorkloadConfig::default())
            .expect("workload")
            .owner;
        control
            .configure_result_capacity(ResultCapacityConfig::V1)
            .expect("capacity");
        control.mark_ready().expect("ready");
        let (root, window) = control
            .root_admission()
            .try_begin_root_with_result(WorkRequest::new(WorkClass::Management), class)
            .expect("original admitted work");
        (control, root, window)
    }

    pub(crate) fn until(mut predicate: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while !predicate() {
            assert!(Instant::now() < deadline, "original owner did not converge");
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    pub(crate) fn settle_control(control: &novarocks_workload_control::WorkloadControl) {
        while let Some(permit) = control.next_control() {
            permit.acknowledge();
        }
    }
}
