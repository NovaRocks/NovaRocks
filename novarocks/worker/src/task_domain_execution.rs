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

//! Worker-owned task-domain execution policy.

use std::fmt;

use novarocks_execution_contract::{
    DomainProgression, OperationOutcome, TaskDescriptor, TaskDomainReceipt, TaskDomainUpdate,
    TaskFailureCategory,
};

use crate::{
    DomainPolicyRejection, HostRejection, TaskDomains, TaskExecutionHost,
    commit_task_domain_updates, plan_task_domain_updates, task_domain_reaches_execution,
    validate_task_domain_membership,
};

/// The typed operation outcome for a task-domain policy refusal.
#[derive(Clone, Debug)]
pub struct DomainExecutionRejection {
    outcome: OperationOutcome,
    detail: String,
    category: TaskFailureCategory,
}

impl DomainExecutionRejection {
    fn new(
        outcome: OperationOutcome,
        category: TaskFailureCategory,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            outcome,
            detail: detail.into(),
            category,
        }
    }

    pub const fn outcome(&self) -> OperationOutcome {
        self.outcome
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }

    pub const fn category(&self) -> TaskFailureCategory {
        self.category
    }
}

impl fmt::Display for DomainExecutionRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl From<DomainPolicyRejection> for DomainExecutionRejection {
    fn from(rejection: DomainPolicyRejection) -> Self {
        Self::new(
            OperationOutcome::DomainConflict,
            TaskFailureCategory::Protocol,
            rejection.detail(),
        )
    }
}

pub fn validate_task_domain_execution_membership(
    descriptor: &TaskDescriptor,
    updates: &[TaskDomainUpdate],
) -> Result<(), DomainExecutionRejection> {
    validate_task_domain_membership(descriptor, updates).map_err(Into::into)
}

pub fn plan_task_domain_execution_updates(
    descriptor: &TaskDescriptor,
    domains: &TaskDomains,
    updates: &[TaskDomainUpdate],
) -> Result<Vec<DomainProgression>, DomainExecutionRejection> {
    plan_task_domain_updates(descriptor, domains, updates).map_err(Into::into)
}

pub fn commit_task_domain_execution_updates(
    domains: &mut TaskDomains,
    updates: &[TaskDomainUpdate],
    queued: &[Option<u64>],
) -> Result<(Vec<TaskDomainReceipt>, bool), DomainExecutionRejection> {
    commit_task_domain_updates(domains, updates, queued).map_err(Into::into)
}

pub fn apply_task_domain_updates(
    host: &dyn TaskExecutionHost,
    descriptor: &TaskDescriptor,
    domains: &mut TaskDomains,
    updates: &[TaskDomainUpdate],
) -> Result<(Vec<TaskDomainReceipt>, bool), DomainExecutionRejection> {
    let plan = plan_task_domain_execution_updates(descriptor, domains, updates)?;
    let queued = apply_planned_task_domain_updates(
        host,
        descriptor,
        updates,
        &plan,
        #[cfg(novarocks_dev_controls)]
        None,
    )?;
    commit_task_domain_execution_updates(domains, updates, &queued)
}

pub fn apply_planned_task_domain_updates(
    host: &dyn TaskExecutionHost,
    descriptor: &TaskDescriptor,
    updates: &[TaskDomainUpdate],
    plan: &[DomainProgression],
    #[cfg(novarocks_dev_controls)] operation: Option<novarocks_execution_contract::TaskOperationId>,
) -> Result<Vec<Option<u64>>, DomainExecutionRejection> {
    let mut queued = Vec::with_capacity(updates.len());
    for (update, progression) in updates.iter().zip(plan) {
        if task_domain_reaches_execution(update, *progression) {
            #[cfg(novarocks_dev_controls)]
            let result = match operation {
                Some(operation) => {
                    host.apply_task_domain_for_operation(descriptor, update, operation)
                }
                None => host.apply_task_domain(descriptor, update),
            };
            #[cfg(not(novarocks_dev_controls))]
            let result = host.apply_task_domain(descriptor, update);
            match result {
                Ok(depth) => queued.push(depth),
                Err(rejection) => {
                    tracing::warn!(
                        task = %descriptor.identity(),
                        kind = ?update.kind(),
                        progression = ?progression,
                        category = ?rejection.category(),
                        detail = %rejection.detail(),
                        "task domain update refused by the execution host"
                    );
                    return Err(rejection_from_host(rejection));
                }
            }
        } else {
            queued.push(None);
        }
    }
    Ok(queued)
}

fn rejection_from_host(rejection: HostRejection) -> DomainExecutionRejection {
    let outcome = match rejection.category() {
        TaskFailureCategory::ResourceExhausted => OperationOutcome::ResourceExhausted,
        TaskFailureCategory::Protocol
        | TaskFailureCategory::Exchange
        | TaskFailureCategory::Execution
        | TaskFailureCategory::Internal => OperationOutcome::InvalidStateOrRequest,
    };
    DomainExecutionRejection::new(outcome, rejection.category(), rejection.detail().as_str())
}
