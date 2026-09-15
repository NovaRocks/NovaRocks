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

//! Statement-local publication state shared by Frontend DML families.

use std::fmt;

use novarocks_spi::connector::{
    LakePublicationDisposition, LakePublicationFamily, LakePublicationId,
    LakePublicationMarkerHeader, LakePublicationNextAction, LakePublicationStatementTag,
    LakePublicationTarget, LakePublicationTerminal,
};

/// The only publication phases a DML statement may retain locally.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DmlPublicationPhase {
    PreDispatch,
    DispatchPossible,
    Terminal,
}

impl DmlPublicationPhase {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::PreDispatch => "pre_dispatch",
            Self::DispatchPossible => "dispatch_possible",
            Self::Terminal => "terminal",
        }
    }
}

/// The finalization observation associated with a terminal result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DmlPublicationFinalization {
    NotApplicable,
    Succeeded,
    Failed,
}

impl DmlPublicationFinalization {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::NotApplicable => "not_applicable",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }
}

pub(crate) const fn publication_disposition_name(
    disposition: LakePublicationDisposition,
) -> &'static str {
    match disposition {
        LakePublicationDisposition::KnownUncommitted => "known_uncommitted",
        LakePublicationDisposition::CommitUnknown => "commit_unknown",
        LakePublicationDisposition::KnownCommitted => "known_committed",
    }
}

pub(crate) const fn publication_next_action_name(
    action: LakePublicationNextAction,
) -> &'static str {
    match action {
        LakePublicationNextAction::RetryStatement => "retry_statement",
        LakePublicationNextAction::InspectPublishedState => "inspect_published_state",
    }
}

/// Read-only adjudication outcomes possible after a provisional unknown.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DmlPublicationAdjudicationOutcome {
    KnownCommitted,
    CommitUnknown,
}

/// A one-use capability returned only after a possible external dispatch.
#[derive(Debug)]
pub(crate) struct DmlPublicationAdjudication {
    _private: (),
}

/// Invalid state-machine transitions are programming errors in a family route,
/// not provider publication outcomes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DmlPublicationAttemptError {
    TerminalAlreadyAssigned,
    DispatchAlreadyPossible,
    DispatchAfterTerminal,
    AdjudicationRequiresPossibleDispatch,
    AdjudicationAlreadyConsumed,
    AdjudicationCompletionWithoutCapability,
    InvalidPreDispatchTerminal,
    InvalidDispatchTerminal,
    InvalidCommittedFinalization,
}

impl fmt::Display for DmlPublicationAttemptError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::TerminalAlreadyAssigned => "DML publication terminal is already assigned",
            Self::DispatchAlreadyPossible => "DML publication dispatch is already marked possible",
            Self::DispatchAfterTerminal => "cannot mark dispatch possible after DML terminal",
            Self::AdjudicationRequiresPossibleDispatch => {
                "DML publication adjudication requires possible dispatch"
            }
            Self::AdjudicationAlreadyConsumed => "DML publication adjudication is already consumed",
            Self::AdjudicationCompletionWithoutCapability => {
                "DML publication adjudication completion requires its capability"
            }
            Self::InvalidPreDispatchTerminal => {
                "pre-dispatch DML publication terminal must be known uncommitted"
            }
            Self::InvalidDispatchTerminal => {
                "possible-dispatch DML publication terminal cannot be known uncommitted"
            }
            Self::InvalidCommittedFinalization => {
                "known-committed DML publication terminal requires finalization success or failure"
            }
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for DmlPublicationAttemptError {}

/// A non-persistent publication attempt owned by one admitted DML statement.
///
/// It freezes the user-visible publication identity and admits only one
/// possible-dispatch adjudication. Family-specific handles, receipts, and
/// evidence remain in the family call stack.
#[derive(Debug)]
pub(crate) struct DmlPublicationAttempt {
    header: LakePublicationMarkerHeader,
    target: LakePublicationTarget,
    statement_tag: Option<LakePublicationStatementTag>,
    phase: DmlPublicationPhase,
    adjudication_consumed: bool,
    terminal: Option<LakePublicationTerminal>,
    finalization: Option<DmlPublicationFinalization>,
}

impl DmlPublicationAttempt {
    pub(crate) fn new(
        publication_id: LakePublicationId,
        family: LakePublicationFamily,
        target: LakePublicationTarget,
        statement_tag: Option<LakePublicationStatementTag>,
    ) -> Self {
        Self {
            header: LakePublicationMarkerHeader::new(publication_id, family),
            target,
            statement_tag,
            phase: DmlPublicationPhase::PreDispatch,
            adjudication_consumed: false,
            terminal: None,
            finalization: None,
        }
    }

    pub(crate) const fn phase(&self) -> DmlPublicationPhase {
        self.phase
    }

    pub(crate) const fn header(&self) -> LakePublicationMarkerHeader {
        self.header
    }

    pub(crate) fn target(&self) -> &LakePublicationTarget {
        &self.target
    }

    pub(crate) fn statement_tag(&self) -> Option<&LakePublicationStatementTag> {
        self.statement_tag.as_ref()
    }

    pub(crate) fn terminal(&self) -> Option<&LakePublicationTerminal> {
        self.terminal.as_ref()
    }

    pub(crate) const fn finalization(&self) -> Option<DmlPublicationFinalization> {
        self.finalization
    }

    pub(crate) fn mark_dispatch_possible(&mut self) -> Result<(), DmlPublicationAttemptError> {
        match self.phase {
            DmlPublicationPhase::PreDispatch => {
                self.phase = DmlPublicationPhase::DispatchPossible;
                Ok(())
            }
            DmlPublicationPhase::DispatchPossible => {
                Err(DmlPublicationAttemptError::DispatchAlreadyPossible)
            }
            DmlPublicationPhase::Terminal => Err(DmlPublicationAttemptError::DispatchAfterTerminal),
        }
    }

    /// Maps an outer failure by the frozen dispatch boundary without inspecting
    /// an error string. A caller uses it when no typed provider outcome exists.
    pub(crate) fn terminal_after_outer_failure(
        &mut self,
    ) -> Result<&LakePublicationTerminal, DmlPublicationAttemptError> {
        match self.phase {
            DmlPublicationPhase::PreDispatch => self.assign_terminal(
                LakePublicationDisposition::KnownUncommitted,
                DmlPublicationFinalization::NotApplicable,
            ),
            DmlPublicationPhase::DispatchPossible => self.assign_terminal(
                LakePublicationDisposition::CommitUnknown,
                DmlPublicationFinalization::NotApplicable,
            ),
            DmlPublicationPhase::Terminal => {
                Err(DmlPublicationAttemptError::TerminalAlreadyAssigned)
            }
        }
    }

    pub(crate) fn terminal_pre_dispatch_uncommitted(
        &mut self,
    ) -> Result<&LakePublicationTerminal, DmlPublicationAttemptError> {
        if self.phase != DmlPublicationPhase::PreDispatch {
            return Err(DmlPublicationAttemptError::InvalidPreDispatchTerminal);
        }
        self.assign_terminal(
            LakePublicationDisposition::KnownUncommitted,
            DmlPublicationFinalization::NotApplicable,
        )
    }

    /// Records a typed provider proof that publication did not occur. Unlike
    /// an outer error, this may follow the conservative dispatch boundary:
    /// the provider's explicit outcome, rather than the call boundary, is
    /// what makes retry safe.
    pub(crate) fn terminal_known_uncommitted(
        &mut self,
    ) -> Result<&LakePublicationTerminal, DmlPublicationAttemptError> {
        if self.phase == DmlPublicationPhase::Terminal {
            return Err(DmlPublicationAttemptError::TerminalAlreadyAssigned);
        }
        self.assign_terminal(
            LakePublicationDisposition::KnownUncommitted,
            DmlPublicationFinalization::NotApplicable,
        )
    }

    pub(crate) fn terminal_known_committed(
        &mut self,
        finalization: DmlPublicationFinalization,
    ) -> Result<&LakePublicationTerminal, DmlPublicationAttemptError> {
        if self.phase != DmlPublicationPhase::DispatchPossible {
            return Err(DmlPublicationAttemptError::InvalidDispatchTerminal);
        }
        validate_committed_finalization(finalization)?;
        self.assign_terminal(LakePublicationDisposition::KnownCommitted, finalization)
    }

    pub(crate) fn terminal_commit_unknown(
        &mut self,
    ) -> Result<&LakePublicationTerminal, DmlPublicationAttemptError> {
        if self.phase != DmlPublicationPhase::DispatchPossible {
            return Err(DmlPublicationAttemptError::InvalidDispatchTerminal);
        }
        self.assign_terminal(
            LakePublicationDisposition::CommitUnknown,
            DmlPublicationFinalization::NotApplicable,
        )
    }

    /// Consumes the sole same-statement, read-only adjudication allowance.
    pub(crate) fn begin_adjudication(
        &mut self,
    ) -> Result<DmlPublicationAdjudication, DmlPublicationAttemptError> {
        if self.phase != DmlPublicationPhase::DispatchPossible {
            return Err(DmlPublicationAttemptError::AdjudicationRequiresPossibleDispatch);
        }
        if self.adjudication_consumed {
            return Err(DmlPublicationAttemptError::AdjudicationAlreadyConsumed);
        }
        self.adjudication_consumed = true;
        Ok(DmlPublicationAdjudication { _private: () })
    }

    /// Completes the one allowed adjudication. Its type cannot represent a
    /// negative observation as permission to retry or clean up.
    pub(crate) fn finish_adjudication(
        &mut self,
        _adjudication: DmlPublicationAdjudication,
        outcome: DmlPublicationAdjudicationOutcome,
        finalization: DmlPublicationFinalization,
    ) -> Result<&LakePublicationTerminal, DmlPublicationAttemptError> {
        if self.phase != DmlPublicationPhase::DispatchPossible || !self.adjudication_consumed {
            return Err(DmlPublicationAttemptError::AdjudicationCompletionWithoutCapability);
        }
        match outcome {
            DmlPublicationAdjudicationOutcome::KnownCommitted => {
                validate_committed_finalization(finalization)?;
                self.assign_terminal(LakePublicationDisposition::KnownCommitted, finalization)
            }
            DmlPublicationAdjudicationOutcome::CommitUnknown => self.assign_terminal(
                LakePublicationDisposition::CommitUnknown,
                DmlPublicationFinalization::NotApplicable,
            ),
        }
    }

    fn assign_terminal(
        &mut self,
        disposition: LakePublicationDisposition,
        finalization: DmlPublicationFinalization,
    ) -> Result<&LakePublicationTerminal, DmlPublicationAttemptError> {
        if self.terminal.is_some() {
            return Err(DmlPublicationAttemptError::TerminalAlreadyAssigned);
        }
        let phase = self.phase;
        let next_action = if disposition.do_not_retry() {
            LakePublicationNextAction::InspectPublishedState
        } else {
            LakePublicationNextAction::RetryStatement
        };
        let terminal = LakePublicationTerminal::new(
            self.header,
            self.target.clone(),
            disposition,
            next_action,
            self.statement_tag.clone(),
        );
        self.terminal = Some(terminal);
        self.finalization = Some(finalization);
        self.phase = DmlPublicationPhase::Terminal;
        let terminal = self
            .terminal
            .as_ref()
            .expect("DML terminal was just assigned");
        crate::dml::observability::record_terminal(terminal, phase, finalization);
        Ok(terminal)
    }
}

fn validate_committed_finalization(
    finalization: DmlPublicationFinalization,
) -> Result<(), DmlPublicationAttemptError> {
    match finalization {
        DmlPublicationFinalization::Succeeded | DmlPublicationFinalization::Failed => Ok(()),
        DmlPublicationFinalization::NotApplicable => {
            Err(DmlPublicationAttemptError::InvalidCommittedFinalization)
        }
    }
}

#[cfg(test)]
mod tests {
    use novarocks_spi::connector::{
        LakePublicationFamily, LakePublicationId, LakePublicationStatementTag,
        LakePublicationTarget,
    };

    use super::*;

    fn attempt() -> DmlPublicationAttempt {
        DmlPublicationAttempt::new(
            LakePublicationId::new_v7(),
            LakePublicationFamily::Write,
            LakePublicationTarget::try_new(
                "iceberg".to_string(),
                "db".to_string(),
                Some("table".to_string()),
                None,
            )
            .expect("target"),
            Some(LakePublicationStatementTag::try_new("insert".to_string()).expect("tag")),
        )
    }

    #[test]
    fn pre_dispatch_failure_is_known_uncommitted_and_retryable() {
        let mut attempt = attempt();
        let (disposition, next_action, do_not_retry) = {
            let terminal = attempt
                .terminal_pre_dispatch_uncommitted()
                .expect("pre-dispatch terminal");
            (
                terminal.disposition(),
                terminal.next_action(),
                terminal.do_not_retry(),
            )
        };

        assert_eq!(attempt.phase(), DmlPublicationPhase::Terminal);
        assert_eq!(disposition, LakePublicationDisposition::KnownUncommitted);
        assert_eq!(next_action, LakePublicationNextAction::RetryStatement);
        assert!(!do_not_retry);
        assert_eq!(
            attempt.finalization(),
            Some(DmlPublicationFinalization::NotApplicable)
        );
    }

    #[test]
    fn possible_dispatch_failure_is_unknown_and_never_retryable() {
        let mut attempt = attempt();
        attempt
            .mark_dispatch_possible()
            .expect("mark dispatch possible");
        assert!(matches!(
            attempt.terminal_pre_dispatch_uncommitted(),
            Err(DmlPublicationAttemptError::InvalidPreDispatchTerminal)
        ));
        let terminal = attempt
            .terminal_after_outer_failure()
            .expect("possible-dispatch terminal");

        assert_eq!(
            terminal.disposition(),
            LakePublicationDisposition::CommitUnknown
        );
        assert_eq!(
            terminal.next_action(),
            LakePublicationNextAction::InspectPublishedState
        );
        assert!(terminal.do_not_retry());
    }

    #[test]
    fn terminal_is_single_assignment_and_cannot_be_downgraded() {
        let mut attempt = attempt();
        attempt
            .mark_dispatch_possible()
            .expect("mark dispatch possible");
        let terminal = attempt
            .terminal_known_committed(DmlPublicationFinalization::Failed)
            .expect("committed terminal");

        assert_eq!(
            terminal.disposition(),
            LakePublicationDisposition::KnownCommitted
        );
        assert_eq!(
            attempt.finalization(),
            Some(DmlPublicationFinalization::Failed)
        );
        assert_eq!(
            attempt.terminal_commit_unknown(),
            Err(DmlPublicationAttemptError::InvalidDispatchTerminal)
        );
        assert_eq!(
            attempt.terminal().expect("terminal").disposition(),
            LakePublicationDisposition::KnownCommitted
        );
    }

    #[test]
    fn adjudication_is_available_once_and_negative_remains_unknown() {
        let mut attempt = attempt();
        attempt
            .mark_dispatch_possible()
            .expect("mark dispatch possible");
        let adjudication = attempt.begin_adjudication().expect("first adjudication");
        assert!(matches!(
            attempt.begin_adjudication(),
            Err(DmlPublicationAttemptError::AdjudicationAlreadyConsumed)
        ));
        let terminal = attempt
            .finish_adjudication(
                adjudication,
                DmlPublicationAdjudicationOutcome::CommitUnknown,
                DmlPublicationFinalization::Failed,
            )
            .expect("complete adjudication");

        assert_eq!(
            terminal.disposition(),
            LakePublicationDisposition::CommitUnknown
        );
        assert_eq!(
            attempt.finalization(),
            Some(DmlPublicationFinalization::NotApplicable)
        );
    }

    #[test]
    fn adjudication_exact_positive_preserves_committed_finalization_failure() {
        let mut attempt = attempt();
        attempt
            .mark_dispatch_possible()
            .expect("mark dispatch possible");
        let adjudication = attempt.begin_adjudication().expect("adjudication");
        let terminal = attempt
            .finish_adjudication(
                adjudication,
                DmlPublicationAdjudicationOutcome::KnownCommitted,
                DmlPublicationFinalization::Failed,
            )
            .expect("complete adjudication");

        assert_eq!(
            terminal.disposition(),
            LakePublicationDisposition::KnownCommitted
        );
        assert!(terminal.do_not_retry());
        assert_eq!(
            attempt.finalization(),
            Some(DmlPublicationFinalization::Failed)
        );
    }

    #[test]
    fn known_committed_requires_a_finalization_result() {
        let mut attempt = attempt();
        attempt
            .mark_dispatch_possible()
            .expect("mark dispatch possible");

        assert!(matches!(
            attempt.terminal_known_committed(DmlPublicationFinalization::NotApplicable),
            Err(DmlPublicationAttemptError::InvalidCommittedFinalization)
        ));
        assert_eq!(attempt.phase(), DmlPublicationPhase::DispatchPossible);
        assert!(attempt.terminal().is_none());
    }

    #[test]
    fn cancellation_or_deadline_before_and_after_dispatch_use_the_same_phase_mapping() {
        let mut before_dispatch = attempt();
        let before = before_dispatch
            .terminal_after_outer_failure()
            .expect("before dispatch");

        let mut after_dispatch = attempt();
        after_dispatch
            .mark_dispatch_possible()
            .expect("mark dispatch possible");
        let after = after_dispatch
            .terminal_after_outer_failure()
            .expect("after dispatch");

        assert_eq!(
            before.disposition(),
            LakePublicationDisposition::KnownUncommitted
        );
        assert_eq!(
            after.disposition(),
            LakePublicationDisposition::CommitUnknown
        );
    }

    #[test]
    fn construction_freezes_publication_identity_target_and_statement_tag() {
        let attempt = attempt();

        assert_eq!(attempt.header().family(), LakePublicationFamily::Write);
        assert_eq!(attempt.target().catalog(), "iceberg");
        assert_eq!(attempt.target().namespace(), "db");
        assert_eq!(attempt.target().table(), Some("table"));
        assert_eq!(
            attempt
                .statement_tag()
                .map(LakePublicationStatementTag::as_str),
            Some("insert")
        );
    }
}
