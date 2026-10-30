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

//! Install actual responsibility services and the original source bindings.
use super::*;
use novarocks_workload_control::joint_admission::{JointAdmissionHandle, JointProgressDriver};
use novarocks_workload_control::{ControlStorageConfig, ResponsibilityConfig, WorkloadControl};

#[derive(Clone, Debug)]
pub struct FrontendWorkloadCapacityProfile {
    pub(super) inputs: FrontendNativeCapacityInputs,
    pub(super) workload: WorkloadConfig,
    pub(super) result: Option<novarocks_workload_control::ResultCapacityConfig>,
    pub(super) transport_budget: Option<novarocks_native_adapter::FrontendTaskTransportBudget>,
    pub(super) statement_ingress: Option<super::composition::FrontendStatementIngress>,
}
impl FrontendWorkloadCapacityProfile {
    pub(super) fn from_configuration(
        inputs: FrontendNativeCapacityInputs,
        workload: &WorkloadConfig,
        result: Option<novarocks_workload_control::ResultCapacityConfig>,
        transport_budget: Option<novarocks_native_adapter::FrontendTaskTransportBudget>,
        statement_ingress: Option<super::composition::FrontendStatementIngress>,
    ) -> Result<Self, WorkError> {
        workload.validate()?;
        inputs.validate_pool_declarations(workload, inputs.query_blocking_capacity())?;
        Ok(Self {
            inputs,
            workload: workload.clone(),
            result,
            transport_budget,
            statement_ingress,
        })
    }
    pub const fn inputs(&self) -> &FrontendNativeCapacityInputs {
        &self.inputs
    }
    pub const fn policy(&self) -> &FrontendWorkloadCapacityConfig {
        self.inputs.policy()
    }
    pub(crate) fn with_native_sources(
        mut self,
        native: &FrontendDataRuntime,
    ) -> Result<Self, WorkError> {
        self.inputs = self.inputs.with_native_sources(native)?;
        if let Some(budget) = self.transport_budget {
            let source = self.inputs.native.as_ref().ok_or(WorkError::NotReady)?;
            if !native
                .task_transport_supervisor()
                .accepts_transport_budget(budget.into_codec())
                || source.apply_records != budget.max_backend_queued_operations()
                || source.transport.process_items != budget.max_backend_queued_operations()
                || source.transport.process_retained_bytes != budget.max_backend_queued_bytes()
                || source.transport.target_items != budget.max_query_backend_queued_operations()
                || source.transport.control_reserved_items != budget.max_batch_items()
                || source.transport.maximum_batch_encoded_bytes != budget.max_batch_encoded_bytes()
            {
                return Err(WorkError::InvalidConfig(
                    "frontend actual Native source configuration",
                ));
            }
        }
        Ok(self)
    }
    pub(crate) fn validate_pool_declarations(
        &self,
        workload: &WorkloadConfig,
        blocking: QueryBlockingExecutorConfig,
    ) -> Result<(), WorkError> {
        if !self.matches_workload(workload) {
            return Err(WorkError::Conflict);
        }
        self.inputs.validate_pool_declarations(workload, blocking)
    }
    pub(crate) fn capture_sources(
        &self,
        workload: &WorkloadConfig,
        result: FrontendResultCapacityProfile,
        blocking: &QueryBlockingExecutor,
        cpu: &InternalResultCpu,
    ) -> Result<FrontendNativeCapacitySources, WorkError> {
        self.validate_pool_declarations(workload, blocking.configured_capacity())?;
        if self
            .result
            .is_some_and(|expected| expected != result.declared())
        {
            return Err(WorkError::Conflict);
        }
        self.inputs.capture_sources(workload, result, blocking, cpu)
    }
    pub(crate) fn install(
        &self,
        workload: &mut WorkloadControl,
        sources: &FrontendNativeCapacitySources,
    ) -> Result<(JointAdmissionHandle, JointProgressDriver), WorkError> {
        self.inputs
            .validate_pool_declarations(&self.workload, sources.query_blocking)?;
        if self
            .result
            .is_some_and(|expected| expected != sources.result)
        {
            return Err(WorkError::Conflict);
        }
        #[cfg(not(test))]
        if sources.native.is_none() {
            return Err(WorkError::NotReady);
        }
        // Identity validation does not reserve physical positions or payload.
        sources.internal_cpu.validate_workload_source(workload)?;
        let roles=[novarocks_query_application::coordination::LogicalExecutionSupervisor::logical_role_poll_geometry()?];
        let slots = self.policy().scope_limit();
        let pair = workload.configure_responsibility(ResponsibilityConfig {
            work_per_turn: self.policy().turn_budget(),
        })?;
        workload.configure_control_storage(ControlStorageConfig::new(slots, &roles))?;
        Ok(pair)
    }
    fn matches_workload(&self, other: &WorkloadConfig) -> bool {
        self.workload.root_limit == other.root_limit
            && self.workload.business_limit == other.business_limit
            && self.workload.query_concurrency_limit == other.query_concurrency_limit
            && self.workload.preparation_limit == other.preparation_limit
            && self.workload.execution_limit == other.execution_limit
            && self.workload.executions_per_root == other.executions_per_root
            && self.workload.waiting_limit == other.waiting_limit
            && self.workload.waiting_bytes == other.waiting_bytes
            && self.workload.capacity_wait_timeout == other.capacity_wait_timeout
            && self.workload.restarts_per_work == other.restarts_per_work
            && self.workload.old_attempts_per_work == other.old_attempts_per_work
            && self.workload.old_attempts_limit == other.old_attempts_limit
            && self.workload.unknown_creates_limit == other.unknown_creates_limit
            && self.workload.scope_records_limit == other.scope_records_limit
            && self.workload.obligation_records_limit == other.obligation_records_limit
            && self.workload.control_inflight_limit == other.control_inflight_limit
            && self.workload.control_ready_limit == other.control_ready_limit
    }
}

#[cfg(test)]
#[path = "installation_tests.rs"]
mod tests;
#[cfg(test)]
pub(crate) fn original_owner_test_workload() -> WorkloadConfig {
    WorkloadConfig {
        root_limit: 20,
        business_limit: 20,
        query_concurrency_limit: 1,
        preparation_limit: 20,
        execution_limit: 20,
        executions_per_root: 4,
        scope_records_limit: 20,
        ..WorkloadConfig::default()
    }
}
#[cfg(test)]
pub(crate) fn original_owner_test_profile(
    blocking: QueryBlockingExecutorConfig,
) -> FrontendWorkloadCapacityProfile {
    let workload = original_owner_test_workload();
    let policy = FrontendWorkloadCapacityConfig::try_new(
        workload.scope_records_limit,
        NonZeroUsize::new(7).unwrap(),
        Duration::from_millis(25),
    )
    .unwrap();
    FrontendWorkloadCapacityProfile::from_configuration(
        FrontendNativeCapacityInputs::try_new(policy, blocking).unwrap(),
        &workload,
        None,
        None,
        None,
    )
    .unwrap()
}
