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

//! Immutable neutral Context decisions retained with each exact request.
//! Shared receipts are borrowed by Arc, never reconstructed from later state.

use super::{ContextEffect, DomainRefusal, RequestResult, RequestSettlement, RootDrainVerdict};
use crate::creation::{PayloadId, RequestContext};
use novarocks_control_runtime_core::identity::InputId;
use novarocks_execution_contract::{
    DomainVersion, FinalTaskInfo, OperationOutcome, QueryContextDomainReceipt,
    QuiesceQueryContextReceipt, TaskDomainReceipt, TaskIdentity,
};
use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ContextReplyBody {
    Verdict(RequestResult),
    FinalInfo(Arc<FinalTaskInfo>),
    RootDrain(RootDrainVerdict),
    TaskDomain {
        outcome: OperationOutcome,
        receipts: Arc<Vec<TaskDomainReceipt>>,
    },
    ContextDomain {
        outcome: OperationOutcome,
        receipt: QueryContextDomainReceipt,
    },
    Domain(DomainRefusal),
    ReadPayload {
        version: DomainVersion,
        payload: PayloadId,
        outcome: OperationOutcome,
    },
    Quiesce {
        outcome: OperationOutcome,
        receipt: Arc<QuiesceQueryContextReceipt>,
    },
}
impl ContextReplyBody {
    pub fn matches(&self, result: RequestResult) -> bool {
        self.result() == result
    }
    pub(super) fn result(&self) -> RequestResult {
        match self {
            Self::Verdict(result) => *result,
            Self::FinalInfo(_) => RequestResult::Outcome(OperationOutcome::Accepted),
            Self::RootDrain(verdict) => RequestResult::Outcome(match verdict {
                RootDrainVerdict::Published(_) | RootDrainVerdict::AlreadyTerminal(_) => {
                    OperationOutcome::Accepted
                }
                RootDrainVerdict::Illegal | RootDrainVerdict::VersionExhausted => {
                    OperationOutcome::InvalidStateOrRequest
                }
            }),
            Self::TaskDomain { outcome, .. }
            | Self::ContextDomain { outcome, .. }
            | Self::ReadPayload { outcome, .. }
            | Self::Quiesce { outcome, .. } => RequestResult::Outcome(*outcome),
            Self::Domain(refusal) => RequestResult::Outcome(refusal.outcome.outcome()),
        }
    }
    pub(super) fn from_effect(effect: &ContextEffect) -> Option<(InputId, Self)> {
        match effect {
            ContextEffect::Reply { request, outcome } => {
                Some((*request, Self::Verdict(RequestResult::Outcome(*outcome))))
            }
            ContextEffect::FinalInfoReply { request, info } => {
                Some((*request, Self::FinalInfo(info.clone())))
            }
            ContextEffect::RootDrainReply { request, result } => {
                Some((*request, Self::RootDrain(*result)))
            }
            ContextEffect::TaskDomainReply {
                request,
                outcome,
                receipts,
            } => Some((
                *request,
                Self::TaskDomain {
                    outcome: *outcome,
                    receipts: receipts.clone(),
                },
            )),
            ContextEffect::ContextDomainReply {
                request,
                outcome,
                receipt,
            } => Some((
                *request,
                Self::ContextDomain {
                    outcome: *outcome,
                    receipt: receipt.clone(),
                },
            )),
            ContextEffect::DomainFailureReply { request, refusal } => {
                Some((*request, Self::Domain(refusal.clone())))
            }
            ContextEffect::ReadPayload {
                request,
                version,
                payload,
                outcome,
            } => Some((
                *request,
                Self::ReadPayload {
                    version: *version,
                    payload: *payload,
                    outcome: *outcome,
                },
            )),
            ContextEffect::QuiesceReply {
                request,
                outcome,
                receipt,
            } => Some((
                *request,
                Self::Quiesce {
                    outcome: *outcome,
                    receipt: receipt.clone(),
                },
            )),
            // Subscribe opens no stream here. Only its actual later owner
            // completion can supply a request verdict; this body grants none.
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContextRequestSettlement {
    pub settlement: RequestSettlement,
    pub task: Option<TaskIdentity>,
    pub body: Arc<ContextReplyBody>,
}
impl std::ops::Deref for ContextRequestSettlement {
    type Target = RequestSettlement;
    fn deref(&self) -> &Self::Target {
        &self.settlement
    }
}
impl ContextRequestSettlement {
    pub(super) fn new(
        request: RequestContext,
        task: Option<TaskIdentity>,
        body: Arc<ContextReplyBody>,
    ) -> Self {
        Self {
            settlement: RequestSettlement {
                request,
                result: body.result(),
            },
            task,
            body,
        }
    }
    pub(super) fn verdict(
        request: RequestContext,
        task: Option<TaskIdentity>,
        result: RequestResult,
    ) -> Self {
        Self::new(request, task, Arc::new(ContextReplyBody::Verdict(result)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lease::MonotonicInstant;
    use novarocks_control_runtime_core::identity::InputId;
    use novarocks_execution_contract::{DomainProgression, TaskOperationId, TaskState};
    use std::time::Duration;

    fn operation() -> TaskOperationId {
        let mut bytes = [71; 16];
        bytes[6] = 0x70;
        bytes[8] = 0x80;
        TaskOperationId::try_from_bytes(bytes).unwrap()
    }

    fn input() -> InputId {
        InputId::new(71).unwrap()
    }

    #[test]
    fn root_drain_preserves_typed_verdict_and_its_exact_operation_result() {
        for (verdict, outcome) in [
            (RootDrainVerdict::Published(19), OperationOutcome::Accepted),
            (
                RootDrainVerdict::AlreadyTerminal(TaskState::Finished),
                OperationOutcome::Accepted,
            ),
            (
                RootDrainVerdict::Illegal,
                OperationOutcome::InvalidStateOrRequest,
            ),
            (
                RootDrainVerdict::VersionExhausted,
                OperationOutcome::InvalidStateOrRequest,
            ),
        ] {
            let (op, body) = ContextReplyBody::from_effect(&ContextEffect::RootDrainReply {
                request: input(),
                result: verdict,
            })
            .unwrap();
            assert_eq!(op, input());
            assert!(matches!(body, ContextReplyBody::RootDrain(original) if original == verdict));
            assert!(body.matches(RequestResult::Outcome(outcome)));
            assert!(!body.matches(RequestResult::TimeoutWhileAccepted));
        }
        assert!(ContextReplyBody::from_effect(&ContextEffect::WakeQuiesce).is_none());
    }

    #[test]
    fn domain_receipts_share_the_original_arc_and_payload_reads_keep_the_exact_version() {
        let receipts = Arc::new(vec![TaskDomainReceipt::TaskDynamicFilter {
            accepted_version: Some(DomainVersion::FIRST),
            progression: DomainProgression::Apply,
        }]);
        let (op, body) = ContextReplyBody::from_effect(&ContextEffect::TaskDomainReply {
            request: input(),
            outcome: OperationOutcome::Idempotent,
            receipts: receipts.clone(),
        })
        .unwrap();
        assert_eq!(op, input());
        let ContextReplyBody::TaskDomain {
            receipts: same_receipts,
            ..
        } = &body
        else {
            panic!("the original typed receipt must survive")
        };
        assert!(
            Arc::ptr_eq(same_receipts, &receipts),
            "reply projection never walks or clones the receipt vector"
        );
        assert!(body.matches(RequestResult::Outcome(OperationOutcome::Idempotent)));
        let version = DomainVersion::new(31).unwrap();
        let payload = PayloadId(37);
        let (_, body) = ContextReplyBody::from_effect(&ContextEffect::ReadPayload {
            request: input(),
            version,
            payload,
            outcome: OperationOutcome::Accepted,
        })
        .unwrap();
        assert!(
            matches!(body, ContextReplyBody::ReadPayload { version: same_version, payload: same_payload, outcome: OperationOutcome::Accepted } if same_version == version && same_payload == payload)
        );
        assert!(body.matches(RequestResult::Outcome(OperationOutcome::Accepted)));
    }

    #[test]
    fn settlement_result_is_derived_from_its_same_body_and_keeps_the_original_request() {
        let request = RequestContext {
            token: InputId::new(41).unwrap(),
            operation: operation(),
            deadline: MonotonicInstant::from_origin(Duration::from_nanos(43)),
        };
        let body = Arc::new(ContextReplyBody::Verdict(RequestResult::Outcome(
            OperationOutcome::TerminalRejected,
        )));
        let settlement = ContextRequestSettlement::new(request, None, body.clone());
        assert!(Arc::ptr_eq(&settlement.body, &body));
        assert_eq!(settlement.request, request);
        assert_eq!(settlement.task, None);
        assert_eq!(
            settlement.result,
            RequestResult::Outcome(OperationOutcome::TerminalRejected)
        );
        assert!(settlement.body.matches(settlement.result));
        let timeout =
            ContextRequestSettlement::verdict(request, None, RequestResult::TimeoutWhileAccepted);
        assert_eq!(timeout.request, request);
        assert_eq!(timeout.result, RequestResult::TimeoutWhileAccepted);
        assert!(timeout.body.matches(timeout.result));
        assert!(
            !timeout
                .body
                .matches(RequestResult::Outcome(OperationOutcome::Accepted))
        );
    }
}
