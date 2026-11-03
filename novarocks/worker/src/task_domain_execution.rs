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

use novarocks_execution_contract::operation::DomainFailureOutcome;

use novarocks_execution_contract::{
    DomainProgression, OperationOutcome, TaskDescriptor, TaskDomainReceipt, TaskDomainUpdate,
    TaskFailureCategory, TaskOperationId,
};

use crate::{
    DomainPolicyRejection, HostRejection, TaskDomains, TaskExecutionHost,
    commit_task_domain_updates, plan_task_domain_updates, task_domain_reaches_execution,
    validate_task_domain_membership,
};

/// An operation verdict and its actual local refusal. The verdict controls
/// replay policy; it never stands in for the failure's domain facts/source.
#[derive(Clone, Debug)]
pub struct DomainExecutionRejection {
    outcome: DomainFailureOutcome,
    cause: DomainExecutionCause,
}
#[derive(Clone, Debug)]
enum DomainExecutionCause {
    Policy(DomainPolicyRejection),
    Host(HostRejection),
}
impl DomainExecutionRejection {
    pub const fn outcome(&self) -> OperationOutcome {
        self.outcome.outcome()
    }
    pub(crate) const fn domain_failure_outcome(&self) -> DomainFailureOutcome {
        self.outcome
    }
    pub fn detail(&self) -> &str {
        match &self.cause {
            DomainExecutionCause::Policy(policy) => policy.detail(),
            DomainExecutionCause::Host(host) => host.detail().as_str(),
        }
    }
    pub const fn category(&self) -> TaskFailureCategory {
        match &self.cause {
            DomainExecutionCause::Policy(_) => TaskFailureCategory::Protocol,
            DomainExecutionCause::Host(host) => host.category(),
        }
    }
    pub(crate) fn operation_failure(
        &self,
    ) -> novarocks_execution_contract::operation::OperationDomainFailure {
        match &self.cause {
            DomainExecutionCause::Host(host) => host.operation_failure(),
            DomainExecutionCause::Policy(policy) => {
                novarocks_execution_contract::operation::OperationDomainFailure::new(
                    novarocks_execution_contract::failure::FailureProjection::failure_fact(policy),
                    novarocks_execution_contract::SafeDetail::truncating(policy.detail()),
                )
            }
        }
    }
    /// Preparation transfers the original host refusal into the Task owner.
    /// Worker policy diagnostics contain frozen kinds/identities/tokens only;
    /// no split payload or credential material is rendered into this context.
    pub(crate) fn into_host(self) -> HostRejection {
        match self.cause {
            DomainExecutionCause::Host(host) => host,
            DomainExecutionCause::Policy(policy) => {
                let detail = novarocks_execution_contract::SafeDetail::truncating(policy.detail());
                HostRejection::from_domain_with_detail(
                    TaskFailureCategory::Protocol,
                    policy,
                    detail,
                )
            }
        }
    }
}
impl fmt::Display for DomainExecutionRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.detail())
    }
}
impl std::error::Error for DomainExecutionRejection {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match &self.cause {
            DomainExecutionCause::Policy(policy) => policy,
            DomainExecutionCause::Host(host) => host,
        })
    }
}
impl From<DomainPolicyRejection> for DomainExecutionRejection {
    fn from(policy: DomainPolicyRejection) -> Self {
        Self {
            outcome: DomainFailureOutcome::DomainConflict,
            cause: DomainExecutionCause::Policy(policy),
        }
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
    operation: TaskOperationId,
) -> Result<(Vec<TaskDomainReceipt>, bool), DomainExecutionRejection> {
    let plan = plan_task_domain_execution_updates(descriptor, domains, updates)?;
    let queued = apply_planned_task_domain_updates(host, descriptor, updates, &plan, operation)?;
    commit_task_domain_execution_updates(domains, updates, &queued)
}

pub fn apply_planned_task_domain_updates(
    host: &dyn TaskExecutionHost,
    descriptor: &TaskDescriptor,
    updates: &[TaskDomainUpdate],
    plan: &[DomainProgression],
    operation: TaskOperationId,
) -> Result<Vec<Option<u64>>, DomainExecutionRejection> {
    let mut queued = Vec::with_capacity(updates.len());
    for (update, progression) in updates.iter().zip(plan) {
        if task_domain_reaches_execution(update, *progression) {
            let result = host.apply_task_domain(descriptor, update, operation);
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
        TaskFailureCategory::ResourceExhausted => DomainFailureOutcome::ResourceExhausted,
        TaskFailureCategory::Protocol
        | TaskFailureCategory::Unsupported
        | TaskFailureCategory::Exchange
        | TaskFailureCategory::Execution
        | TaskFailureCategory::Internal => DomainFailureOutcome::InvalidStateOrRequest,
    };
    DomainExecutionRejection {
        outcome,
        cause: DomainExecutionCause::Host(rejection),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn domain_projection_execution_host_source_survives_the_domain_boundary() {
        use novarocks_execution::runtime::task::ExecutionFailure;
        use novarocks_execution_contract::{OperationOutcome, TaskFailureCategory};
        use std::error::Error;
        let original =
            ExecutionFailure::from(novarocks_functions::KernelFailure::ResourceExhausted);
        let host =
            crate::HostRejection::from_domain(TaskFailureCategory::ResourceExhausted, original);
        let original_pointer = host
            .source()
            .unwrap()
            .downcast_ref::<ExecutionFailure>()
            .unwrap()
            .cause() as *const _;
        let rejected = super::rejection_from_host(host);
        assert_eq!(rejected.outcome(), OperationOutcome::ResourceExhausted);
        assert!(
            rejected
                .source()
                .unwrap()
                .downcast_ref::<crate::HostRejection>()
                .is_some()
        );
        let returned = rejected.into_host();
        assert_eq!(returned.category(), TaskFailureCategory::ResourceExhausted);
        let actual_pointer = returned
            .source()
            .unwrap()
            .downcast_ref::<ExecutionFailure>()
            .unwrap()
            .cause() as *const _;
        assert_eq!(
            actual_pointer, original_pointer,
            "same retained source, not a diagnostic reconstruction"
        );
    }
}
