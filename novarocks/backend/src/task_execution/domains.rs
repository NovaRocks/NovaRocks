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

//! Native execution adapter for Worker-owned task domain policy.

use std::fmt;

use novarocks_execution_contract::{
    DomainProgression, OperationOutcome, TaskDescriptor, TaskDomainReceipt, TaskDomainUpdate,
};
use novarocks_worker::{
    DomainPolicyRejection, commit_task_domain_updates, plan_task_domain_updates,
    task_domain_reaches_execution, validate_task_domain_membership,
};

pub(super) use novarocks_worker::{InitialDomainKey, TaskDomains};

use super::host::{HostRejection, TaskExecutionHost};

#[derive(Clone, Debug)]
pub(super) struct DomainRejection {
    outcome: OperationOutcome,
    detail: String,
}

impl DomainRejection {
    fn new(outcome: OperationOutcome, detail: impl Into<String>) -> Self {
        Self {
            outcome,
            detail: detail.into(),
        }
    }

    pub(super) const fn outcome(&self) -> OperationOutcome {
        self.outcome
    }

    pub(super) fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for DomainRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl From<DomainPolicyRejection> for DomainRejection {
    fn from(rejection: DomainPolicyRejection) -> Self {
        Self::new(OperationOutcome::DomainConflict, rejection.detail())
    }
}

pub(super) fn validate_membership(
    descriptor: &TaskDescriptor,
    updates: &[TaskDomainUpdate],
) -> Result<(), DomainRejection> {
    validate_task_domain_membership(descriptor, updates).map_err(Into::into)
}

pub(super) fn plan_updates(
    descriptor: &TaskDescriptor,
    domains: &TaskDomains,
    updates: &[TaskDomainUpdate],
) -> Result<Vec<DomainProgression>, DomainRejection> {
    plan_task_domain_updates(descriptor, domains, updates).map_err(Into::into)
}

pub(super) fn commit_updates(
    domains: &mut TaskDomains,
    updates: &[TaskDomainUpdate],
    queued: &[Option<u64>],
) -> Result<(Vec<TaskDomainReceipt>, bool), DomainRejection> {
    commit_task_domain_updates(domains, updates, queued).map_err(Into::into)
}

pub(super) fn apply_updates(
    host: &dyn TaskExecutionHost,
    descriptor: &TaskDescriptor,
    domains: &mut TaskDomains,
    updates: &[TaskDomainUpdate],
) -> Result<(Vec<TaskDomainReceipt>, bool), DomainRejection> {
    let plan = plan_updates(descriptor, domains, updates)?;
    let queued = apply_planned(host, descriptor, updates, &plan)?;
    commit_updates(domains, updates, &queued)
}

pub(super) fn apply_planned(
    host: &dyn TaskExecutionHost,
    descriptor: &TaskDescriptor,
    updates: &[TaskDomainUpdate],
    plan: &[DomainProgression],
) -> Result<Vec<Option<u64>>, DomainRejection> {
    let mut queued = Vec::with_capacity(updates.len());
    for (update, progression) in updates.iter().zip(plan) {
        if task_domain_reaches_execution(update, *progression) {
            match host.apply_task_domain(descriptor, update) {
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

fn rejection_from_host(rejection: HostRejection) -> DomainRejection {
    use novarocks_execution_contract::TaskFailureCategory;

    let outcome = match rejection.category() {
        TaskFailureCategory::ResourceExhausted => OperationOutcome::ResourceExhausted,
        TaskFailureCategory::Protocol
        | TaskFailureCategory::Exchange
        | TaskFailureCategory::Execution
        | TaskFailureCategory::Internal => OperationOutcome::InvalidStateOrRequest,
    };
    DomainRejection::new(outcome, rejection.detail().as_str())
}

pub(super) fn initial_domain_keys(updates: &[TaskDomainUpdate]) -> Vec<InitialDomainKey> {
    novarocks_worker::initial_domain_keys(updates)
}
