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

//! Frontend runtime configuration and facts from the original installed owners.
//!
//! Source identity and lifecycle readiness do not grant resource capacity.

use crate::native::data_runtime::FrontendDataRuntime;
use crate::native::transport_supervisor::NativeTaskOperationCapacityFacts;
use crate::query_execution::internal_result_cpu::{
    FrontendResultCapacityProfile, InternalResultCpu,
};
use novarocks_query_application::cpu::{QueryBlockingExecutor, QueryBlockingExecutorConfig};
use novarocks_workload_control::{WorkError, WorkloadConfig};
use std::num::NonZeroUsize;
use std::time::Duration;

/// Actual global scope inventory and responsibility-service scheduling.
#[derive(Clone, Debug)]
pub struct FrontendWorkloadCapacityConfig {
    scope_limit: usize,
    turn_budget: NonZeroUsize,
    recheck_interval: Duration,
}
impl FrontendWorkloadCapacityConfig {
    pub fn try_new(
        scope_records_limit: usize,
        turn_budget: NonZeroUsize,
        recheck_interval: Duration,
    ) -> Result<Self, WorkError> {
        if scope_records_limit == 0 || recheck_interval.is_zero() {
            return Err(WorkError::InvalidConfig(
                "frontend workload runtime configuration",
            ));
        }
        Ok(Self {
            scope_limit: scope_records_limit,
            turn_budget,
            recheck_interval,
        })
    }
    pub const fn scope_limit(&self) -> usize {
        self.scope_limit
    }
    pub const fn turn_budget(&self) -> NonZeroUsize {
        self.turn_budget
    }
    pub const fn recheck_interval(&self) -> Duration {
        self.recheck_interval
    }
}

#[derive(Clone, Debug)]
pub struct FrontendNativeCapacityInputs {
    policy: FrontendWorkloadCapacityConfig,
    query_blocking: QueryBlockingExecutorConfig,
    native: Option<NativeProcessCapacitySources>,
}
impl FrontendNativeCapacityInputs {
    pub fn try_new(
        policy: FrontendWorkloadCapacityConfig,
        query_blocking: QueryBlockingExecutorConfig,
    ) -> Result<Self, WorkError> {
        Ok(Self {
            policy,
            query_blocking,
            native: None,
        })
    }
    pub const fn policy(&self) -> &FrontendWorkloadCapacityConfig {
        &self.policy
    }
    pub const fn query_blocking_capacity(&self) -> QueryBlockingExecutorConfig {
        self.query_blocking
    }
    pub(crate) fn with_native_sources(
        mut self,
        native: &FrontendDataRuntime,
    ) -> Result<Self, WorkError> {
        let source = native.installed_runtime_sources()?;
        if source.subscription_records != self.policy.scope_limit() {
            return Err(WorkError::InvalidConfig(
                "frontend actual subscription inventory",
            ));
        }
        self.native = Some(source);
        Ok(self)
    }
    pub(crate) fn validate_pool_declarations(
        &self,
        workload: &WorkloadConfig,
        query_blocking: QueryBlockingExecutorConfig,
    ) -> Result<(), WorkError> {
        if workload.scope_records_limit != self.policy.scope_limit()
            || query_blocking != self.query_blocking
        {
            return Err(WorkError::InvalidConfig(
                "frontend actual source configuration",
            ));
        }
        Ok(())
    }
    pub(crate) fn capture_sources(
        &self,
        workload: &WorkloadConfig,
        result: FrontendResultCapacityProfile,
        query_blocking: &QueryBlockingExecutor,
        internal_cpu: &InternalResultCpu,
    ) -> Result<FrontendNativeCapacitySources, WorkError> {
        self.validate_pool_declarations(workload, query_blocking.configured_capacity())?;
        Ok(FrontendNativeCapacitySources {
            native: self.native,
            query_blocking: query_blocking.configured_capacity(),
            result: result.declared(),
            internal_cpu: internal_cpu.clone(),
        })
    }
}

/// These original-owner observations are neither a capacity grant nor a byte quote.
pub(crate) struct FrontendNativeCapacitySources {
    pub(crate) native: Option<NativeProcessCapacitySources>,
    pub(crate) query_blocking: QueryBlockingExecutorConfig,
    pub(crate) result: novarocks_workload_control::ResultCapacityConfig,
    pub(crate) internal_cpu: InternalResultCpu,
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct NativeProcessCapacitySources {
    pub(crate) apply_records: usize,
    pub(crate) subscription_records: usize,
    pub(crate) transport: NativeTaskOperationCapacityFacts,
    #[cfg(test)]
    pub(crate) physical: novarocks_native_adapter::native_transport_admission::AdmissionDimensions,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn global_scope_inventory_is_not_split_into_class_quotas() {
        let budget = NonZeroUsize::new(7).unwrap();
        assert_eq!(
            FrontendWorkloadCapacityConfig::try_new(8192, budget, Duration::from_millis(25))
                .unwrap()
                .scope_limit(),
            8192
        );
        assert!(
            FrontendWorkloadCapacityConfig::try_new(0, budget, Duration::from_millis(25)).is_err()
        );
        assert!(FrontendWorkloadCapacityConfig::try_new(8192, budget, Duration::ZERO).is_err());
    }
    #[test]
    fn one_worker_retains_its_actual_independent_queue_configuration() {
        let config = QueryBlockingExecutorConfig::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(32).unwrap(),
        );
        let policy = FrontendWorkloadCapacityConfig::try_new(
            8192,
            NonZeroUsize::new(7).unwrap(),
            Duration::from_millis(25),
        )
        .unwrap();
        let inputs = FrontendNativeCapacityInputs::try_new(policy, config).unwrap();
        assert_eq!(inputs.query_blocking_capacity(), config);
    }
}
mod installation;
pub use installation::FrontendWorkloadCapacityProfile;
#[cfg(test)]
pub(crate) use installation::{original_owner_test_profile, original_owner_test_workload};
mod composition;
pub use composition::FrontendStatementIngress;
