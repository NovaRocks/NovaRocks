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

//! Explicit fixed unit inventory for FE component tests.
//! Identity tests create no Work; statement tests use the SAME supervisor and
//! original Root/FirstBind. Neither form is a production Native profile.

use novarocks_query_application::{
    api::{LogicalExecutionNativePort, QueryExecutionClient},
    coordination::{
        LogicalExecutionSupervisor, LogicalExecutionSupervisorConfig,
        LogicalExecutionSupervisorShutdownError,
    },
};
use novarocks_types::identity::{FrontendProcessId, QueryProcessNamespace};
use novarocks_workload_control::joint_admission::{
    JointAdmissionHandle, JointProgress, JointProgressDriver, ResponsibilityConfig,
};
use novarocks_workload_control::{ControlStorageConfig, WorkloadConfig, WorkloadControl};
use std::{num::NonZeroUsize, sync::Arc, time::Instant};

pub(crate) struct IdentityTestOwner {
    supervisor: Option<LogicalExecutionSupervisor>,
    control: Option<WorkloadControl>,
    driver: Option<JointProgressDriver>,
    admission: JointAdmissionHandle,
    complete: bool,
}
pub(crate) fn process(
    runtime: tokio::runtime::Handle,
    native: Arc<dyn LogicalExecutionNativePort>,
    namespace: QueryProcessNamespace,
    frontend: FrontendProcessId,
    config: LogicalExecutionSupervisorConfig,
) -> (IdentityTestOwner, QueryExecutionClient) {
    process_inner(runtime, native, namespace, frontend, config, None)
}

/// Install the original statement source and result-window author before Ready.
pub(crate) fn process_with_statement_control(
    runtime: tokio::runtime::Handle,
    native: Arc<dyn LogicalExecutionNativePort>,
    namespace: QueryProcessNamespace,
    frontend: FrontendProcessId,
    config: LogicalExecutionSupervisorConfig,
    service: novarocks_query_application::session_control::QueryControlService,
) -> (IdentityTestOwner, QueryExecutionClient) {
    process_inner(runtime, native, namespace, frontend, config, Some(service))
}

fn process_inner(
    runtime: tokio::runtime::Handle,
    native: Arc<dyn LogicalExecutionNativePort>,
    namespace: QueryProcessNamespace,
    frontend: FrontendProcessId,
    config: LogicalExecutionSupervisorConfig,
    statements: Option<novarocks_query_application::session_control::QueryControlService>,
) -> (IdentityTestOwner, QueryExecutionClient) {
    let roles = [LogicalExecutionSupervisor::logical_role_poll_geometry().unwrap()];
    let count = 4;
    let mut storage = ControlStorageConfig::new(count, &roles);
    if statements.is_some() {
        let source = novarocks_query_application::session_control::QueryControlService::statement_action_geometry();
        storage = storage.with_process_source_backing(
            novarocks_workload_control::PollRoleSourceBacking::from_layouts(
                &[
                    source.registry_record,
                    source.source,
                    source.wake,
                    source.observer,
                    source.receipt,
                    source.failure,
                    source.failure,
                ],
                &[source.driver],
            )
            .unwrap(),
        );
    }
    // The same original owner installs the responsibility service and roles;
    // responsibility quantities do not create a second resource budget.
    let mut control = WorkloadControl::try_new_counted(WorkloadConfig {
        root_limit: count,
        business_limit: count,
        query_concurrency_limit: count,
        scope_records_limit: count,
        ..WorkloadConfig::default()
    })
    .unwrap()
    .owner;
    let (admission, driver) = control
        .configure_responsibility(ResponsibilityConfig {
            work_per_turn: NonZeroUsize::new(7).unwrap(),
        })
        .unwrap();
    control.configure_control_storage(storage).unwrap();
    if statements.is_some() {
        control
            .configure_result_capacity(novarocks_workload_control::ResultCapacityConfig::V1)
            .unwrap();
    }
    let (mut supervisor, client) = LogicalExecutionSupervisor::try_new(
        &mut control,
        0,
        NonZeroUsize::new(7).unwrap(),
        runtime,
        native,
        namespace,
        frontend,
        config,
    )
    .unwrap();
    if let Some(service) = statements {
        supervisor
            .install_statement_control(service, NonZeroUsize::new(count).unwrap())
            .unwrap();
    }
    control.mark_ready().unwrap();
    (
        IdentityTestOwner {
            supervisor: Some(supervisor),
            control: Some(control),
            driver: Some(driver),
            admission,
            complete: false,
        },
        client,
    )
}
impl IdentityTestOwner {
    pub(crate) fn control(&self) -> &WorkloadControl {
        self.control.as_ref().unwrap()
    }

    pub(crate) async fn bind_local_root(&mut self, owner: &novarocks_workload_control::WorkOwner) {
        use std::{future::Future, pin::Pin, task::Poll};
        let mut proposal = self.admission.begin_paid_proposal(owner).unwrap();
        proposal
            .begin_first_bind(owner.control_claim_view().unwrap())
            .unwrap();
        proposal
            .push_dependency(&novarocks_resource_contract::SharedDependency::LocalHost)
            .unwrap();
        proposal.end_change().unwrap();
        let mut request = proposal.submit().unwrap();
        let batch = std::future::poll_fn(|cx| {
            if let Poll::Ready(result) = Pin::new(&mut request).poll(cx) {
                return Poll::Ready(result);
            }
            // SAME unique driver, finite proof turn; no second task/driver.
            match self.driver.as_mut().unwrap().step() {
                JointProgress::More => cx.waker().wake_by_ref(),
                JointProgress::Idle(_) => {}
            }
            Pin::new(&mut request).poll(cx)
        })
        .await
        .unwrap();
        assert!(batch.new_claims.is_empty());
        batch
            .adjustment
            .unwrap()
            .install_control(owner, &[])
            .unwrap();
    }

    pub(crate) async fn shutdown_until(
        &mut self,
        deadline: Instant,
    ) -> Result<(), LogicalExecutionSupervisorShutdownError> {
        if self.complete {
            return Ok(());
        }
        self.supervisor
            .as_mut()
            .unwrap()
            .shutdown_until(deadline)
            .await?;
        self.control.as_ref().unwrap().close_admission();
        // Reclaim the actual bank/statement cells only after the SAME root joined.
        loop {
            assert!(
                Instant::now() < deadline,
                "identity fixture reaper deadline exceeded"
            );
            match self.driver.as_mut().unwrap().step() {
                JointProgress::Idle(_) => break,
                JointProgress::More => tokio::task::yield_now().await,
            }
        }
        drop(self.driver.take());
        let owner = self.control.take().unwrap();
        if let Err(failure) = owner.shutdown() {
            let (error, owner) = failure.into_parts();
            self.control = Some(owner);
            panic!("identity fixture kept actual unresolved responsibility: {error}");
        }
        self.complete = true;
        Ok(())
    }
}
impl Drop for IdentityTestOwner {
    fn drop(&mut self) {
        if !self.complete {
            // Failed assertion/cancelled shutdown retains the same owner graph;
            // dropping a test wrapper never fabricates a successful root join.
            std::mem::forget(self.supervisor.take());
            std::mem::forget(self.driver.take());
            std::mem::forget(self.control.take());
        }
    }
}
