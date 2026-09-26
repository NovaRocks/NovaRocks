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

//! Worker-local policy for one task registry.

use std::time::Duration;

use novarocks_types::identity::BackendProcessId;

use crate::{
    AdmissionTicketConfig, LeaseBounds, METRIC_PUBLISH_MIN_INTERVAL, OperationWaitCaps,
    RequestHorizon,
};

/// The bounds and budgets one Worker task owner runs with.
///
/// The role composition root supplies the two frozen transport capacities.
/// The Worker retains no dependency on a codec or native transport model.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct TaskExecutionRegistryConfig {
    pub backend_process_id: BackendProcessId,
    pub lease_bounds: LeaseBounds,
    pub wait_caps: OperationWaitCaps,
    pub admission_tickets: AdmissionTicketConfig,
    pub request_horizon: RequestHorizon,
    pub metric_publish_min_interval: Duration,
    pub max_tasks_per_context: usize,
    pub max_active_tasks_per_backend: usize,
    /// Accepted tasks retain a preparation position through physical exit.
    pub max_preparing_tasks: usize,
    /// Queued and running preparations owned by one exact Context.
    pub max_preparing_tasks_per_context: usize,
    /// Conservative bound on accepted creation bodies held by preparation.
    pub max_preparing_bytes: usize,
    /// Maximum number of contexts preparing concurrently.
    pub max_prepare_workers: usize,
    pub retained_task_capacity: usize,
    pub retained_task_max_bytes: usize,
    pub retained_context_capacity: usize,
    pub gone_fence_capacity: usize,
    pub termination_grace: Duration,
    pub gate_poll_interval: Duration,
}

impl TaskExecutionRegistryConfig {
    pub fn for_process(
        backend_process_id: BackendProcessId,
        max_tasks_per_context: usize,
        max_active_tasks_per_backend: usize,
    ) -> Self {
        Self {
            backend_process_id,
            lease_bounds: LeaseBounds::DEFAULT,
            wait_caps: OperationWaitCaps::DEFAULT,
            admission_tickets: AdmissionTicketConfig::DEFAULT,
            request_horizon: RequestHorizon::DEFAULT,
            metric_publish_min_interval: METRIC_PUBLISH_MIN_INTERVAL,
            max_tasks_per_context,
            max_active_tasks_per_backend,
            max_preparing_tasks: max_active_tasks_per_backend,
            max_preparing_tasks_per_context: max_tasks_per_context,
            max_preparing_bytes: 256 * 1024 * 1024,
            max_prepare_workers: 4,
            retained_task_capacity: max_tasks_per_context,
            retained_task_max_bytes: 16 * 1024 * 1024,
            retained_context_capacity: 1024,
            gone_fence_capacity: max_tasks_per_context,
            termination_grace: RequestHorizon::DEFAULT.server_wait(),
            gate_poll_interval: Duration::from_millis(50),
        }
    }

    /// Checks the limits owned by this registry before a backend begins
    /// admitting tasks. No relation between active and retained counts is
    /// implied: they cover different lifecycle phases.
    pub fn validate_capacity_limits(&self) -> Result<(), String> {
        for (name, value) in [
            ("max_tasks_per_context", self.max_tasks_per_context),
            (
                "max_active_tasks_per_backend",
                self.max_active_tasks_per_backend,
            ),
            ("max_preparing_tasks", self.max_preparing_tasks),
            (
                "max_preparing_tasks_per_context",
                self.max_preparing_tasks_per_context,
            ),
            ("max_preparing_bytes", self.max_preparing_bytes),
            ("max_prepare_workers", self.max_prepare_workers),
            ("retained_task_capacity", self.retained_task_capacity),
            ("retained_task_max_bytes", self.retained_task_max_bytes),
            ("retained_context_capacity", self.retained_context_capacity),
            ("gone_fence_capacity", self.gone_fence_capacity),
        ] {
            if value == 0 {
                return Err(format!("task registry capacity {name} must be positive"));
            }
        }
        Ok(())
    }

    /// The completion owner must be able to reserve a slot for every task
    /// admitted by the registry's active-task gate.
    pub fn validate_completion_capacity(&self, completion_capacity: usize) -> Result<(), String> {
        self.validate_capacity_limits()?;
        if completion_capacity != self.max_active_tasks_per_backend {
            return Err(format!(
                "task completion capacity {completion_capacity} must equal active task capacity {}",
                self.max_active_tasks_per_backend
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::TaskExecutionRegistryConfig;
    use novarocks_types::identity::BackendProcessId;

    #[test]
    fn process_policy_uses_only_the_composed_task_capacities() {
        let config = TaskExecutionRegistryConfig::for_process(BackendProcessId::new_v7(), 17, 9);

        assert_eq!(config.max_tasks_per_context, 17);
        assert_eq!(config.max_active_tasks_per_backend, 9);
        assert_eq!(config.retained_task_capacity, 17);
        assert_eq!(config.gone_fence_capacity, 17);
        config.validate_capacity_limits().expect("valid limits");
        config
            .validate_completion_capacity(9)
            .expect("completion capacity matches active task capacity");
    }

    #[test]
    fn zero_capacity_and_mismatched_completion_capacity_are_rejected() {
        let baseline = TaskExecutionRegistryConfig::for_process(BackendProcessId::new_v7(), 17, 9);
        let invalid: [(&str, fn(&mut TaskExecutionRegistryConfig)); 6] = [
            (
                "max_tasks_per_context",
                |config: &mut TaskExecutionRegistryConfig| config.max_tasks_per_context = 0,
            ),
            (
                "max_active_tasks_per_backend",
                |config: &mut TaskExecutionRegistryConfig| config.max_active_tasks_per_backend = 0,
            ),
            (
                "retained_task_capacity",
                |config: &mut TaskExecutionRegistryConfig| config.retained_task_capacity = 0,
            ),
            (
                "retained_task_max_bytes",
                |config: &mut TaskExecutionRegistryConfig| config.retained_task_max_bytes = 0,
            ),
            (
                "retained_context_capacity",
                |config: &mut TaskExecutionRegistryConfig| config.retained_context_capacity = 0,
            ),
            (
                "gone_fence_capacity",
                |config: &mut TaskExecutionRegistryConfig| config.gone_fence_capacity = 0,
            ),
        ];
        for (name, adjust) in invalid {
            let mut config = baseline;
            adjust(&mut config);
            let error = config.validate_capacity_limits().expect_err(name);
            assert!(error.contains(name), "{error}");
        }
        assert!(baseline.validate_completion_capacity(8).is_err());
        assert!(baseline.validate_completion_capacity(10).is_err());
    }
}

/// Static preparation limits owned by the backend Worker. These bound queued
/// and running jobs, independently of active Task and completion limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TaskPreparationLimits {
    per_context: usize,
    tasks: usize,
    bytes: usize,
    workers: usize,
}

impl TaskPreparationLimits {
    pub fn try_new(
        per_context: usize,
        tasks: usize,
        bytes: usize,
        workers: usize,
    ) -> Result<Self, String> {
        if per_context == 0 || tasks == 0 || bytes == 0 || workers == 0 {
            return Err("preparation limits must be nonzero".to_owned());
        }
        if per_context > tasks {
            return Err(
                "per-context preparation task limit must not exceed the backend task limit"
                    .to_owned(),
            );
        }
        if workers > tasks {
            return Err(
                "preparation worker limit must not exceed the backend task limit".to_owned(),
            );
        }
        Ok(Self {
            per_context,
            tasks,
            bytes,
            workers,
        })
    }
    pub const fn per_context(self) -> usize {
        self.per_context
    }
    pub const fn tasks(self) -> usize {
        self.tasks
    }
    pub const fn bytes(self) -> usize {
        self.bytes
    }
    pub const fn workers(self) -> usize {
        self.workers
    }
}

impl Default for TaskPreparationLimits {
    fn default() -> Self {
        Self {
            per_context: 4096,
            tasks: 32768,
            bytes: 256 * 1024 * 1024,
            workers: 4,
        }
    }
}

impl TaskExecutionRegistryConfig {
    pub fn with_preparation_limits(mut self, limits: TaskPreparationLimits) -> Self {
        self.max_preparing_tasks_per_context = limits.per_context();
        self.max_preparing_tasks = limits.tasks();
        self.max_preparing_bytes = limits.bytes();
        self.max_prepare_workers = limits.workers();
        self
    }
}

#[cfg(test)]
mod preparation_limit_tests {
    use super::TaskPreparationLimits;
    #[test]
    fn deployable_preparation_limits_reject_inverted_positions_and_zero_values() {
        for args in [
            (0, 2, 128, 1),
            (1, 0, 128, 1),
            (1, 2, 0, 1),
            (1, 2, 128, 0),
            (3, 2, 128, 1),
            (1, 2, 128, 3),
        ] {
            assert!(
                TaskPreparationLimits::try_new(args.0, args.1, args.2, args.3).is_err(),
                "{args:?}"
            );
        }
        assert!(TaskPreparationLimits::try_new(1, 2, 128, 1).is_ok());
    }
}
