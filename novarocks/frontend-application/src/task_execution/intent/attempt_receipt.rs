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

use super::{AckPayload, OperationAcknowledgement, OperationAcknowledgementResult};
use crate::task_execution::operation::{IngressRejection, OperationDispatchResult};
use novarocks_attempt_protocol::{
    failure::{
        AttemptFailureSite, HostFailureKind, HostFailureRef, HostProjectedFailure, ProtocolFailure,
    },
    input::{
        AckPayload as PassiveAck, LocalDispatchResult, OperationObservation, OperationReply,
        OperationReplyResult, SendGeneration,
    },
    operation::IngressRejection as PassiveIngress,
    start::PayloadRef,
};
use novarocks_execution_contract::{TaskOperationId, operation::OperationResult};
use novarocks_query_application::execution_error::ExecutionFailureSite;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AttemptReceiptError {
    WrongOperation,
    WrongReleaseContribution,
    WrongWorkloadFailure,
    InvalidDispatchResult,
}

impl OperationAcknowledgement {
    /// Project the retained original on its executor. The Source supplies the
    /// already-retained error/contribution references and owns the passive
    /// input's backing through its actual retirement.
    pub(crate) fn attempt_observation(
        &self,
        operation: TaskOperationId,
        generation: SendGeneration,
        retained_filter: Option<PayloadRef>,
        retained_workload: Option<HostFailureRef>,
    ) -> Result<OperationObservation, AttemptReceiptError> {
        if self.operation_id() != operation {
            return Err(AttemptReceiptError::WrongOperation);
        }
        if matches!(
            self.payload(),
            AckPayload::Release {
                runtime_filter: Some(_),
                ..
            }
        ) != retained_filter.is_some()
        {
            return Err(AttemptReceiptError::WrongReleaseContribution);
        }
        let workload = match self.dispatch_result() {
            OperationDispatchResult::NonWorkerRejected if self.local_failure().is_none() => {
                self.governance_failure()
            }
            _ => None,
        };
        if workload.is_some() != retained_workload.is_some() {
            return Err(AttemptReceiptError::WrongWorkloadFailure);
        }

        let result = match self.original_result() {
            OperationAcknowledgementResult::Worker(OperationResult::Verdict {
                outcome,
                detail,
                acknowledgement,
            }) => OperationReplyResult::Worker(OperationResult::Verdict {
                outcome: *outcome,
                detail: detail.clone(),
                acknowledgement: acknowledgement
                    .as_ref()
                    .map(|ack| passive_payload(ack, retained_filter)),
            }),
            OperationAcknowledgementResult::Worker(OperationResult::DomainFailure {
                outcome,
                failure,
            }) => OperationReplyResult::Worker(OperationResult::DomainFailure {
                outcome: *outcome,
                failure: failure.clone(),
            }),
            OperationAcknowledgementResult::NonWorker {
                dispatch,
                payload: AckPayload::None,
                detail,
                local_failure,
            } => {
                let result = match dispatch {
                    OperationDispatchResult::IngressRejected(rejection) => {
                        LocalDispatchResult::IngressRejected(match rejection {
                            IngressRejection::WaitingCapacity => PassiveIngress::WaitingCapacity,
                            IngressRejection::BodyLimit => PassiveIngress::BodyLimit,
                            IngressRejection::UnclassifiedCapacity => {
                                PassiveIngress::UnclassifiedCapacity
                            }
                        })
                    }
                    OperationDispatchResult::NonWorkerRejected => {
                        LocalDispatchResult::NonWorkerRejected
                    }
                    OperationDispatchResult::TransportUnknown => {
                        LocalDispatchResult::TransportUnknown
                    }
                    OperationDispatchResult::WorkerReceipt(_) => {
                        return Err(AttemptReceiptError::InvalidDispatchResult);
                    }
                };
                let failure = workload.zip(retained_workload).map(|(error, original)| {
                    let typed =
                        crate::task_execution::error::TaskExecutionError::Workload(error.clone())
                            .preparation_input();
                    HostProjectedFailure {
                        original,
                        kind: HostFailureKind::Workload,
                        class: typed.attempt_class(),
                        site: failure_site(typed.site()),
                    }
                });
                let protocol_failure = local_failure.as_ref().map(|typed| ProtocolFailure {
                    fact: typed.fact(),
                    detail: typed.detail().clone(),
                    site: failure_site(typed.site()),
                });
                OperationReplyResult::Local {
                    result,
                    failure,
                    protocol_failure,
                    detail: detail.clone(),
                }
            }
            OperationAcknowledgementResult::NonWorker { .. } => {
                return Err(AttemptReceiptError::InvalidDispatchResult);
            }
        };
        Ok(OperationObservation {
            generation,
            reply: OperationReply {
                operation_id: operation,
                kind: self.kind(),
                result,
            },
        })
    }
}

fn passive_payload(payload: &AckPayload, retained_filter: Option<PayloadRef>) -> PassiveAck {
    match payload {
        AckPayload::None => PassiveAck::None,
        AckPayload::AdmissionTicket(receipt) => PassiveAck::AdmissionTicket(*receipt),
        AckPayload::Create(receipt) => PassiveAck::Create(receipt.clone()),
        AckPayload::Update(receipt) => PassiveAck::Update(receipt.clone()),
        AckPayload::Participation(receipt) => PassiveAck::Participation(receipt.clone()),
        AckPayload::Quiesce(receipt) => PassiveAck::Quiesce(Arc::new(receipt.clone())),
        AckPayload::Release {
            receipt, outcome, ..
        } => PassiveAck::Release {
            receipt: receipt.clone(),
            outcome: *outcome,
            runtime_filter: retained_filter,
        },
    }
}

pub(in crate::task_execution) fn failure_site(site: ExecutionFailureSite) -> AttemptFailureSite {
    match site {
        ExecutionFailureSite::Task { identity, phase } => {
            AttemptFailureSite::Task { identity, phase }
        }
        ExecutionFailureSite::TaskControl { identity } => {
            AttemptFailureSite::TaskControl { identity }
        }
        ExecutionFailureSite::Operation {
            participation,
            operation_id,
            kind,
        } => AttemptFailureSite::Operation {
            participation,
            operation_id,
            kind,
        },
        ExecutionFailureSite::RootRead { root } => AttemptFailureSite::RootRead { root },
        ExecutionFailureSite::Backend { process } => AttemptFailureSite::Backend { process },
        ExecutionFailureSite::Coordinator => AttemptFailureSite::Attempt,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_execution::error::local_protocol_refusal;
    use novarocks_coordination_contract::AttemptActionId;
    use novarocks_execution_contract::{
        OperationKind, OperationOutcome, SafeDetail,
        operation::{DomainFailureOutcome, OperationDomainFailure},
    };
    use std::num::NonZeroU64;

    fn generation(value: u64) -> SendGeneration {
        SendGeneration::new(AttemptActionId::new(NonZeroU64::new(value).unwrap()))
    }

    #[test]
    fn exact_send_projects_its_original_worker_and_local_branches() {
        let operation = TaskOperationId::new_v7();
        let kind = OperationKind::CreateTask;
        let error = local_protocol_refusal(kind, OperationOutcome::ResourceExhausted);
        let detail = SafeDetail::truncating("original worker failure");
        let domain = OperationDomainFailure::new(error.fact(), detail.clone());
        let worker = OperationAcknowledgement::worker_domain_failure(
            operation,
            kind,
            DomainFailureOutcome::ResourceExhausted,
            domain.clone(),
        );
        let local = OperationAcknowledgement::local_refusal(operation, kind, error.clone());
        let projected = worker
            .attempt_observation(operation, generation(2), None, None)
            .unwrap();
        assert_eq!(projected.generation, generation(2));
        assert_eq!(projected.reply.operation_id, operation);
        assert_eq!(projected.reply.kind, kind);
        assert!(matches!(
            projected.reply.result,
            OperationReplyResult::Worker(OperationResult::DomainFailure {
                outcome: DomainFailureOutcome::ResourceExhausted,
                failure,
            }) if failure == domain
        ));

        let projected = local
            .attempt_observation(operation, generation(3), None, None)
            .unwrap();
        assert_eq!(projected.generation, generation(3));
        assert_eq!(projected.reply.worker_outcome(), None);
        assert!(matches!(
            projected.reply.result,
            OperationReplyResult::Local {
                result: LocalDispatchResult::NonWorkerRejected,
                failure: None,
                protocol_failure: Some(ProtocolFailure {
                    fact, detail, site: AttemptFailureSite::Attempt,
                }),
                ..
            } if fact == error.fact() && detail == *error.detail()
        ));
        assert!(matches!(
            worker.attempt_observation(TaskOperationId::new_v7(), generation(2), None, None),
            Err(AttemptReceiptError::WrongOperation)
        ));
        assert!(matches!(
            worker.attempt_observation(
                operation,
                generation(2),
                None,
                Some(HostFailureRef(NonZeroU64::MIN))
            ),
            Err(AttemptReceiptError::WrongWorkloadFailure)
        ));
        assert!(matches!(
            worker.attempt_observation(
                operation,
                generation(2),
                Some(PayloadRef(NonZeroU64::MIN)),
                None
            ),
            Err(AttemptReceiptError::WrongReleaseContribution)
        ));
    }

    #[test]
    fn ingress_rejection_and_transport_unknown_keep_distinct_send_generations() {
        let operation = TaskOperationId::new_v7();
        let kind = OperationKind::UpdateTask;
        let unknown = OperationAcknowledgement::transport_unknown(operation, kind);
        let refused = OperationAcknowledgement::from_dispatch_result(
            operation,
            kind,
            OperationDispatchResult::IngressRejected(IngressRejection::WaitingCapacity),
            AckPayload::None,
        );
        let unknown = unknown
            .attempt_observation(operation, generation(1), None, None)
            .unwrap();
        let refused = refused
            .attempt_observation(operation, generation(2), None, None)
            .unwrap();
        assert_eq!(unknown.generation, generation(1));
        assert_eq!(refused.generation, generation(2));
        assert_eq!(unknown.reply.worker_outcome(), None);
        assert_eq!(refused.reply.worker_outcome(), None);
        assert!(matches!(
            unknown.reply.result,
            OperationReplyResult::Local {
                result: LocalDispatchResult::TransportUnknown,
                ..
            }
        ));
        assert!(matches!(
            refused.reply.result,
            OperationReplyResult::Local {
                result: LocalDispatchResult::IngressRejected(PassiveIngress::WaitingCapacity),
                ..
            }
        ));
    }
}
