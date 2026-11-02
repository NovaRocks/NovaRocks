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

//! Freeze rich Task proposals before handing neutral facts to the pure core.
//! The original prepaid Source retains each rich cause through actual exit.

use crate::failure_reason::WorkerFailureReason;
use crate::task_diagnostics::TaskSecondaryDiagnostic;
use novarocks_execution::runtime::fragment::ExecutionFailure;
use novarocks_execution_contract::{
    TaskConclusion, TaskConclusionProposal, TaskFailure, TerminationDetail,
    failure::{FailureClass, FailureEvidence, FailureFact, FailureProjection},
};
use std::sync::Arc;

#[derive(Debug)]
struct UnownedStopReport(ExecutionFailure);
impl std::fmt::Display for UnownedStopReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Task failure reporting received a stop observation without an accepted cause")
    }
}
impl std::error::Error for UnownedStopReport {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl FailureProjection for UnownedStopReport {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            FailureClass::Unexpected,
            WorkerFailureReason::UnownedStopReport.reason(),
        )
    }
}

/// An owned local proposal with its boundary projection already evaluated.
/// Only the accepting transaction may install it as the Task's receipt.
pub(crate) struct PreparedTaskConclusion {
    conclusion: TaskConclusion,
    origin: Option<ExecutionFailure>,
    reported_evidence: Option<FailureEvidence>,
    late_diagnostic: Option<Arc<TaskSecondaryDiagnostic>>,
}
impl PreparedTaskConclusion {
    pub(crate) fn new(proposal: TaskConclusionProposal<ExecutionFailure>) -> Self {
        let mut reported_evidence = None;
        let mut late_diagnostic = None;
        let (conclusion, origin) = match proposal {
            TaskConclusionProposal::Succeeded => (TaskConclusion::Succeeded, None),
            TaskConclusionProposal::Canceled(reason) => (
                TaskConclusion::Terminated(TerminationDetail::Canceled(reason)),
                None,
            ),
            TaskConclusionProposal::Aborted(cause) => (
                TaskConclusion::Terminated(TerminationDetail::Aborted(cause)),
                None,
            ),
            TaskConclusionProposal::Failed { cause, phase } => {
                let fact = cause.failure_fact();
                let evidence = fact.evidence();
                reported_evidence = Some(evidence);
                if evidence == FailureEvidence::Originating {
                    late_diagnostic =
                        Some(TaskSecondaryDiagnostic::late_failure(&cause, phase, fact));
                }
                // If another cause has already won this prepared candidate
                // loses. Otherwise it describes the reporting contract breach,
                // preserving the actual observation rather than inventing a
                // normal cancellation or refining a peer placeholder.
                let (cause, fact) = if evidence == FailureEvidence::StopObservation {
                    let cause = ExecutionFailure::from_domain(UnownedStopReport(cause));
                    let fact = cause.failure_fact();
                    (cause, fact)
                } else {
                    (cause, fact)
                };
                (
                    TaskConclusion::Terminated(TerminationDetail::Failed(
                        TaskFailure::new_in_phase(fact, cause.safe_detail_for_fact(fact), phase),
                    )),
                    Some(cause),
                )
            }
        };
        Self {
            conclusion,
            origin,
            reported_evidence,
            late_diagnostic,
        }
    }
    /// Moves only already projected neutral values into the pure protocol.
    /// The caller retains the rich source in prepaid host custody. Rendering
    /// and the old diagnostic record's destruction occur before its cut lock.
    pub(crate) fn into_protocol(
        self,
    ) -> (
        novarocks_worker_protocol::proposal_summary::Proposal,
        Option<ExecutionFailure>,
    ) {
        let diagnostic = self.late_diagnostic.as_ref().map(|record| {
            novarocks_worker_protocol::proposal_summary::Diagnostic {
                phase: record.phase(),
                fact: record.fact(),
                detail: novarocks_execution_contract::SafeDetail::truncating(record.detail()),
                cleanup: false,
            }
        });
        (
            novarocks_worker_protocol::proposal_summary::Proposal {
                conclusion: self.conclusion,
                evidence: self.reported_evidence,
                source: None,
                diagnostic,
            },
            self.origin,
        )
    }
}
