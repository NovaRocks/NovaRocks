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

//! Query-session errors before a protocol adapter maps them to wire codes.

use std::{borrow::Cow, fmt, sync::Arc};

use crate::api::StatementExternalEffectDisposition;
use novarocks_spi::connector::{LakePublicationDisposition, LakePublicationTerminal};
use novarocks_user_error::UserError;

/// Render an execution error at the Query Application boundary. Protocol
/// adapters consume this projection without rebuilding the selected input.
pub fn execution_public_message(error: &crate::api::QueryExecutionError) -> Cow<'_, str> {
    match error.execution_failure() {
        Some(input) => Cow::Owned(input.user_error().to_string()),
        None => Cow::Borrowed(error.message()),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryServiceErrorKind {
    Parse,
    BadDatabase,
    Unsupported,
    PermissionDenied,
    NoSuchSession,
    Interrupted,
    Timeout,
    InvalidValue,
    Unavailable,
    /// The role-local serving lifecycle has irreversibly closed workload admission.
    FrontendDraining,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum QueryServiceErrorBody {
    Message {
        kind: QueryServiceErrorKind,
        message: String,
    },
    User {
        error: UserError,
        message: String,
    },
    Execution {
        input: Box<crate::execution_error::ExecutionFailureInput>,
        user_error: UserError,
        message: String,
    },
}

/// Sparse, value-semantic diagnostics shared by clones of the same error.
/// These facts grant no publication, cancellation or execution authority.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct DiagnosticFacts {
    publication_terminal: Option<LakePublicationTerminal>,
    compile_control: Option<novarocks_type_contract::CompileControlError>,
    compile_provenance: Option<novarocks_type_contract::CompileControlProvenance>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryServiceError {
    failure_cause: Option<novarocks_failure_contract::SharedFailureCause>,
    body: QueryServiceErrorBody,
    diagnostics: Option<Arc<DiagnosticFacts>>,
    external_effect_disposition: Option<StatementExternalEffectDisposition>,
}

impl QueryServiceError {
    pub const FRONTEND_DRAINING_MESSAGE: &'static str =
        "FRONTEND_DRAINING: frontend is draining; retry on another frontend";

    fn diagnostic_facts_mut(&mut self) -> &mut DiagnosticFacts {
        Arc::make_mut(
            self.diagnostics
                .get_or_insert_with(|| Arc::new(DiagnosticFacts::default())),
        )
    }

    fn remove_empty_diagnostics(&mut self) {
        if self.diagnostics.as_deref().is_some_and(|facts| {
            facts.publication_terminal.is_none()
                && facts.compile_control.is_none()
                && facts.compile_provenance.is_none()
        }) {
            self.diagnostics = None;
        }
    }

    pub fn frontend_draining() -> Self {
        Self::new(
            QueryServiceErrorKind::FrontendDraining,
            Self::FRONTEND_DRAINING_MESSAGE,
        )
    }

    pub fn new(kind: QueryServiceErrorKind, message: impl Into<String>) -> Self {
        Self {
            body: QueryServiceErrorBody::Message {
                kind,
                message: message.into(),
            },
            diagnostics: None,
            external_effect_disposition: None,
            failure_cause: None,
        }
    }

    /// Retains the domain descriptor and rendering without inventing a phase.
    pub fn from_user_error(error: UserError) -> Self {
        let message = error.to_string();
        Self {
            body: QueryServiceErrorBody::User { error, message },
            diagnostics: None,
            external_effect_disposition: None,
            failure_cause: None,
        }
    }

    /// Carries the selected execution fact, safe detail and site as one sum.
    /// User-visible rendering is derived here; it grants no new acceptance.
    pub fn from_execution_failure(input: crate::execution_error::ExecutionFailureInput) -> Self {
        let user_error = input.user_error();
        let message = user_error.to_string();
        Self {
            body: QueryServiceErrorBody::Execution {
                input: Box::new(input),
                user_error,
                message,
            },
            diagnostics: None,
            external_effect_disposition: None,
            failure_cause: None,
        }
    }

    /// Preserve the originating domain and use its frozen fact and safe detail.
    pub fn from_failure_cause(cause: novarocks_failure_contract::SharedFailureCause) -> Self {
        use crate::execution_error::{ExecutionFailureInput, ExecutionFailureSite};
        use novarocks_failure_contract::FailureProjection;
        let mut failure = Self::from_execution_failure(ExecutionFailureInput::new(
            cause.failure_fact(),
            novarocks_execution_contract::SafeDetail::truncating(cause.detail()),
            ExecutionFailureSite::Coordinator,
        ));
        failure.failure_cause = Some(cause);
        failure
    }
    /// Reconcile a local source's stop observation with this query owner's
    /// actually accepted cancellation. The caller must pass the same query's
    /// view; the source fact itself carries no Root identity or cancel right.
    /// Keep the source's original fact and raw cause beside the query verdict.
    pub fn reconcile_local_stop_observation(
        mut self,
        cancellation: &crate::cancellation::QueryCancellationView,
    ) -> Self {
        use crate::execution_error::ExecutionFailureSite;
        use novarocks_failure_contract::{FailureEvidence, FailureProjection};

        let eligible = match (&self.body, &self.failure_cause) {
            (QueryServiceErrorBody::Execution { input, .. }, Some(cause)) => {
                matches!(input.site(), ExecutionFailureSite::Coordinator)
                    && input.fact().evidence() == FailureEvidence::StopObservation
                    && cause.failure_fact() == input.fact()
            }
            _ => false,
        };
        if !eligible {
            return self;
        }
        let Some(reason) = cancellation.reason() else {
            return self;
        };
        let input = crate::session_outcome::cancellation_failure_input(reason);
        let user_error = input.user_error();
        self.body = QueryServiceErrorBody::Execution {
            input: Box::new(input),
            message: user_error.to_string(),
            user_error,
        };
        self.render_execution_message();
        self
    }

    pub fn failure_cause(&self) -> Option<&novarocks_failure_contract::SharedFailureCause> {
        self.failure_cause.as_ref()
    }

    pub fn execution_failure(&self) -> Option<&crate::execution_error::ExecutionFailureInput> {
        match &self.body {
            QueryServiceErrorBody::Execution { input, .. } => Some(input.as_ref()),
            QueryServiceErrorBody::Message { .. } | QueryServiceErrorBody::User { .. } => None,
        }
    }

    pub fn with_publication_terminal(
        message: impl Into<String>,
        terminal: LakePublicationTerminal,
    ) -> Self {
        Self {
            body: QueryServiceErrorBody::Message {
                kind: QueryServiceErrorKind::Internal,
                message: message.into(),
            },
            diagnostics: Some(Arc::new(DiagnosticFacts {
                publication_terminal: Some(terminal),
                ..DiagnosticFacts::default()
            })),
            external_effect_disposition: None,
            failure_cause: None,
        }
    }

    /// Keep the selected input and descriptor unchanged while exposing an
    /// independent publication verdict. Only Unknown and KnownCommitted forbid
    /// retry; KnownUncommitted is not, by itself, permission to retry. Provider
    /// and target diagnostics remain in the terminal sidecar.
    pub fn attach_publication_terminal(mut self, terminal: LakePublicationTerminal) -> Self {
        self.diagnostic_facts_mut().publication_terminal = Some(terminal);
        self.render_execution_message();
        self
    }

    fn render_execution_message(&mut self) {
        if let QueryServiceErrorBody::Execution {
            user_error,
            message,
            ..
        } = &mut self.body
        {
            // Both independent knowledge fields render from the same frozen
            // descriptor. Neither attachment may erase the other one's guard.
            *message = user_error.to_string();
            if let Some(terminal) = self
                .diagnostics
                .as_deref()
                .and_then(|facts| facts.publication_terminal.as_ref())
            {
                let disposition = match terminal.disposition() {
                    LakePublicationDisposition::KnownUncommitted => "KnownUncommitted",
                    LakePublicationDisposition::CommitUnknown => "CommitUnknown",
                    LakePublicationDisposition::KnownCommitted => "KnownCommitted",
                };
                use std::fmt::Write;
                let header = terminal.header();
                write!(
                    message,
                    " (lake publication id={} family={} disposition={}",
                    header.publication_id(),
                    header.family().as_str(),
                    disposition,
                )
                .expect("writing to a String cannot fail");
                if terminal.do_not_retry() {
                    message.push_str(" do_not_retry=true; do not retry statement");
                }
                message.push(')');
            }
            if let Some(disposition) = self.external_effect_disposition {
                let label = match disposition {
                    StatementExternalEffectDisposition::KnownUncommitted => "KnownUncommitted",
                    StatementExternalEffectDisposition::CommitUnknown => "CommitUnknown",
                    StatementExternalEffectDisposition::KnownCommitted => "KnownCommitted",
                };
                use std::fmt::Write;
                write!(message, " (external effect disposition={label}")
                    .expect("writing to a String cannot fail");
                if disposition.do_not_retry() {
                    message.push_str(" do_not_retry=true; do not retry statement");
                }
                message.push(')');
            }
        }
    }

    /// Forward only the original action owner's effect knowledge.
    /// A disposition alone grants no domain identity, receipt, or exit proof.
    /// The selected input, descriptor and phase are retained without rewriting.
    pub fn attach_external_effect_disposition(
        mut self,
        disposition: StatementExternalEffectDisposition,
    ) -> Self {
        // This is one original owner's frozen terminal verdict, not a new
        // observation/update channel. Reapplying it cannot duplicate rendering.
        if self.external_effect_disposition.is_some() {
            return self;
        }
        self.external_effect_disposition = Some(disposition);
        if matches!(&self.body, QueryServiceErrorBody::Execution { .. }) {
            self.render_execution_message();
            return self;
        }
        // Preserve the original Message/User behavior. Their text has no
        // frozen execution descriptor to render again.
        let message = match &mut self.body {
            QueryServiceErrorBody::Message { message, .. }
            | QueryServiceErrorBody::User { message, .. } => message,
            QueryServiceErrorBody::Execution { .. } => unreachable!("handled above"),
        };
        let label = match disposition {
            StatementExternalEffectDisposition::KnownUncommitted => "KnownUncommitted",
            StatementExternalEffectDisposition::CommitUnknown => "CommitUnknown",
            StatementExternalEffectDisposition::KnownCommitted => "KnownCommitted",
        };
        use std::fmt::Write;
        write!(message, " (external effect disposition={label}")
            .expect("writing to a String cannot fail");
        if disposition.do_not_retry() {
            message.push_str(" do_not_retry=true; do not retry statement");
        }
        message.push(')');
        self
    }

    pub fn external_effect_disposition(&self) -> Option<StatementExternalEffectDisposition> {
        self.external_effect_disposition
    }

    pub fn with_compile_control(
        mut self,
        cause: Option<novarocks_type_contract::CompileControlError>,
    ) -> Self {
        if cause.is_none()
            && self.diagnostics.as_deref().is_none_or(|facts| {
                facts.compile_control.is_none() && facts.compile_provenance.is_none()
            })
        {
            return self;
        }
        let facts = self.diagnostic_facts_mut();
        facts.compile_provenance = cause.as_ref().map(|cause| cause.provenance());
        facts.compile_control = cause;
        self.remove_empty_diagnostics();
        self
    }
    pub fn from_compile_control(error: novarocks_type_contract::CompileControlError) -> Self {
        use crate::execution_error::{ExecutionFailureInput, ExecutionFailureSite};
        use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};
        use novarocks_type_contract::CompileControlError;

        // A pure compile stop observes its caller, not accepted Query cancellation.
        // Source diagnostics remain in the rich sidecar; only owner-authored safe
        // descriptions enter the client-facing canonical input.
        let detail = match &error {
            CompileControlError::Cancelled => "pure compilation observed a caller stop",
            CompileControlError::DeadlineExceeded => "pure compilation observed a caller deadline",
            CompileControlError::ResourceExhausted => "pure compilation work budget was exhausted",
            CompileControlError::Source(_) => "pure compilation failed in its source domain",
        };
        Self::from_execution_failure(ExecutionFailureInput::new(
            error.failure_fact(),
            SafeDetail::truncating(detail),
            ExecutionFailureSite::Coordinator,
        ))
        .with_compile_control(Some(error))
    }
    /// A historical fact carries its real compiler provenance without keeping
    /// the original Provider tree alive or manufacturing a rich Source error.
    pub fn with_compile_control_provenance(
        mut self,
        provenance: Option<novarocks_type_contract::CompileControlProvenance>,
    ) -> Self {
        if self.compile_control_provenance() == provenance {
            return self;
        }
        self.diagnostic_facts_mut().compile_provenance = provenance;
        self.remove_empty_diagnostics();
        self
    }
    pub fn compile_control_provenance(
        &self,
    ) -> Option<novarocks_type_contract::CompileControlProvenance> {
        self.diagnostics
            .as_deref()
            .and_then(|facts| facts.compile_provenance)
    }
    pub fn compile_control_error(&self) -> Option<novarocks_type_contract::CompileControlError> {
        self.diagnostics
            .as_deref()
            .and_then(|facts| facts.compile_control.clone())
    }

    pub fn kind(&self) -> QueryServiceErrorKind {
        match &self.body {
            QueryServiceErrorBody::Message { kind, .. } => *kind,
            QueryServiceErrorBody::User { error, .. }
                if error.phase() != novarocks_user_error::ErrorPhase::Execution =>
            {
                QueryServiceErrorKind::Parse
            }
            QueryServiceErrorBody::User { .. } => QueryServiceErrorKind::Internal,
            QueryServiceErrorBody::Execution { input, .. } => match input.error_kind() {
                crate::api::QueryExecutionErrorKind::Cancelled => {
                    QueryServiceErrorKind::Interrupted
                }
                crate::api::QueryExecutionErrorKind::DeadlineExceeded => {
                    QueryServiceErrorKind::Timeout
                }
                crate::api::QueryExecutionErrorKind::Rejected => QueryServiceErrorKind::Unavailable,
                crate::api::QueryExecutionErrorKind::InvalidRequest
                | crate::api::QueryExecutionErrorKind::Failed => QueryServiceErrorKind::Internal,
            },
        }
    }

    pub fn message(&self) -> &str {
        match &self.body {
            QueryServiceErrorBody::Message { message, .. }
            | QueryServiceErrorBody::User { message, .. }
            | QueryServiceErrorBody::Execution { message, .. } => message,
        }
    }

    pub fn user_error(&self) -> Option<&UserError> {
        match &self.body {
            QueryServiceErrorBody::User { error, .. } => Some(error),
            QueryServiceErrorBody::Execution { user_error, .. } => Some(user_error),
            QueryServiceErrorBody::Message { .. } => None,
        }
    }

    pub fn publication_terminal(&self) -> Option<&LakePublicationTerminal> {
        self.diagnostics
            .as_deref()
            .and_then(|facts| facts.publication_terminal.as_ref())
    }
}

impl fmt::Display for QueryServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message())
    }
}

impl std::error::Error for QueryServiceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.failure_cause
            .as_ref()
            .map(|cause| cause as &dyn std::error::Error)
            .or_else(|| {
                self.diagnostics
                    .as_deref()
                    .and_then(|facts| facts.compile_control.as_ref())
                    .map(|cause| cause as &dyn std::error::Error)
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compile_control_resource_and_source_keep_their_author_facts_and_rich_sidecars() {
        use novarocks_execution_contract::failure::FailureProjection;
        use novarocks_type_contract::{
            CompileControlError, owned_resources::layout::LayoutResourceError,
        };
        for cause in [
            CompileControlError::ResourceExhausted,
            CompileControlError::from_domain(LayoutResourceError::Arithmetic),
        ] {
            let expected = cause.failure_fact();
            let service = QueryServiceError::from_compile_control(cause.clone());
            let input = service.execution_failure().unwrap();
            assert_eq!(input.fact(), expected);
            assert_eq!(
                (input.descriptor().errno, input.descriptor().sqlstate),
                (1041, "HY000")
            );
            assert_eq!(service.compile_control_error(), Some(cause));
            assert_eq!(
                service.user_error().unwrap().phase(),
                novarocks_user_error::ErrorPhase::Execution
            );
            assert_eq!(service.user_error().unwrap().location(), None);
        }
    }

    #[test]
    fn pure_compile_stop_observations_do_not_claim_accepted_query_cancellation() {
        use novarocks_execution_contract::failure::{FailureEvidence, FailureProjection};
        use novarocks_type_contract::CompileControlError;
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
        ] {
            let expected = cause.failure_fact();
            let service = QueryServiceError::from_compile_control(cause.clone());
            let input = service.execution_failure().unwrap();
            assert_eq!(input.fact(), expected);
            assert_eq!(input.fact().evidence(), FailureEvidence::StopObservation);
            assert_eq!(
                (input.descriptor().errno, input.descriptor().sqlstate),
                (1105, "HY000")
            );
            assert_eq!(
                input.error_kind(),
                crate::api::QueryExecutionErrorKind::Failed
            );
            assert_eq!(service.compile_control_error(), Some(cause));
            assert_eq!(
                service.user_error().unwrap().phase(),
                novarocks_user_error::ErrorPhase::Execution
            );
            assert_eq!(service.user_error().unwrap().location(), None);
        }
    }

    #[test]
    fn typed_error_preserves_kind_and_message() {
        let error = QueryServiceError::new(QueryServiceErrorKind::Timeout, "deadline elapsed");
        assert_eq!(error.kind(), QueryServiceErrorKind::Timeout);
        assert_eq!(error.message(), "deadline elapsed");
        assert!(error.user_error().is_none());
    }

    #[test]
    fn frontend_draining_error_uses_the_fixed_retry_message() {
        let error = QueryServiceError::frontend_draining();
        assert_eq!(error.kind(), QueryServiceErrorKind::FrontendDraining);
        assert_eq!(
            error.message(),
            "FRONTEND_DRAINING: frontend is draining; retry on another frontend"
        );
    }

    #[test]
    fn parser_user_error_is_preserved_without_message_classification() {
        let parser_error = novarocks_parser::parse("SHOW")
            .expect_err("incomplete SHOW command must be a parser error")
            .to_user_error("SHOW");
        let error = QueryServiceError::from_user_error(parser_error.clone());

        assert_eq!(error.kind(), QueryServiceErrorKind::Parse);
        assert_eq!(error.user_error(), Some(&parser_error));
        assert_eq!(error.message(), parser_error.to_string());
    }
}

#[cfg(test)]
mod compile_control_tests {
    use super::*;
    use novarocks_type_contract::CompileControlError;

    #[test]
    fn service_terminal_preserves_control_without_inventing_query_cancellation() {
        for (control, kind) in [
            (
                CompileControlError::Cancelled,
                QueryServiceErrorKind::Internal,
            ),
            (
                CompileControlError::DeadlineExceeded,
                QueryServiceErrorKind::Internal,
            ),
            (
                CompileControlError::ResourceExhausted,
                QueryServiceErrorKind::Unavailable,
            ),
        ] {
            let error = QueryServiceError::from_compile_control(control.clone());
            use novarocks_execution_contract::failure::FailureProjection;
            assert_eq!(error.kind(), kind);
            let input = error.execution_failure().unwrap();
            assert_eq!(input.fact(), control.failure_fact());
            let expected_errno = if control == CompileControlError::ResourceExhausted {
                1041
            } else {
                1105
            };
            assert_eq!(
                (input.descriptor().errno, input.descriptor().sqlstate),
                (expected_errno, "HY000")
            );
            assert_eq!(error.compile_control_error(), Some(control.clone()));
            assert_eq!(
                QueryServiceError::new(kind, control.to_string()).compile_control_error(),
                None
            );
        }
    }
}

#[cfg(test)]
mod publication_message_tests {
    use super::*;
    use crate::{
        api::{QueryExecutionError, QueryExecutionErrorKind},
        execution_error::{ExecutionFailureInput, ExecutionFailureSite},
    };
    use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};
    use novarocks_spi::connector::{
        ConnectorError, ConnectorErrorKind, LakePublicationFamily, LakePublicationId,
        LakePublicationMarkerHeader, LakePublicationNextAction, LakePublicationStatementTag,
        LakePublicationTarget,
    };

    fn selected_input() -> ExecutionFailureInput {
        let source = ConnectorError::new(
            ConnectorErrorKind::Internal,
            "private provider diagnostic must not enter SQL",
        );
        ExecutionFailureInput::new(
            source.failure_fact(),
            SafeDetail::new("writer execution failed").unwrap(),
            ExecutionFailureSite::Coordinator,
        )
    }

    fn publication(disposition: LakePublicationDisposition) -> LakePublicationTerminal {
        LakePublicationTerminal::new(
            LakePublicationMarkerHeader::new(
                LakePublicationId::new_v7(),
                LakePublicationFamily::Write,
            ),
            LakePublicationTarget::try_new(
                "private-catalog".to_string(),
                "private-namespace".to_string(),
                Some("private-table".to_string()),
                Some("private-reference".to_string()),
            )
            .unwrap(),
            disposition,
            if disposition.do_not_retry() {
                LakePublicationNextAction::InspectPublishedState
            } else {
                LakePublicationNextAction::RetryStatement
            },
            Some(LakePublicationStatementTag::try_new("private-tag".to_string()).unwrap()),
        )
    }

    #[test]
    fn diagnostic_updates_preserve_cloned_facts_and_clear_only_requested_fields() {
        use novarocks_failure_contract::SharedFailureCause;
        use novarocks_type_contract::{CompileControlError, CompileControlProvenance};

        let source = SharedFailureCause::from_static_detail(
            ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                "private provider diagnostic",
            ),
            "selected source failed",
        );
        let terminal = publication(LakePublicationDisposition::KnownCommitted);
        let original = QueryServiceError::from_failure_cause(source.clone())
            .with_compile_control(Some(CompileControlError::Cancelled))
            .attach_external_effect_disposition(StatementExternalEffectDisposition::KnownCommitted)
            .attach_publication_terminal(terminal.clone());
        let input = original.execution_failure().unwrap().clone();
        let message = original.message().to_owned();

        let cleared = original.clone().with_compile_control(None);
        assert_eq!(cleared.compile_control_error(), None);
        assert_eq!(cleared.compile_control_provenance(), None);
        assert_eq!(cleared.publication_terminal(), Some(&terminal));
        assert_eq!(cleared.failure_cause(), Some(&source));
        assert_eq!(cleared.execution_failure(), Some(&input));
        assert_eq!(cleared.message(), message);

        let replacement = publication(LakePublicationDisposition::KnownUncommitted);
        let updated = cleared
            .with_compile_control_provenance(Some(CompileControlProvenance::DeadlineExceeded))
            .attach_publication_terminal(replacement.clone())
            .attach_external_effect_disposition(
                StatementExternalEffectDisposition::KnownUncommitted,
            );
        assert_eq!(updated.publication_terminal(), Some(&replacement));
        assert_eq!(updated.compile_control_error(), None);
        assert_eq!(
            updated.compile_control_provenance(),
            Some(CompileControlProvenance::DeadlineExceeded)
        );
        assert_eq!(updated.failure_cause(), Some(&source));
        assert_eq!(updated.execution_failure(), Some(&input));
        assert_eq!(
            updated.external_effect_disposition(),
            Some(StatementExternalEffectDisposition::KnownCommitted)
        );
        assert!(updated.message().contains("disposition=KnownUncommitted)"));
        assert!(updated.message().contains(
            "external effect disposition=KnownCommitted do_not_retry=true; do not retry statement)"
        ));
        assert!(!updated.message().contains("private provider diagnostic"));

        assert_eq!(original.publication_terminal(), Some(&terminal));
        assert_eq!(
            original.compile_control_error(),
            Some(CompileControlError::Cancelled)
        );
        assert_eq!(
            original.compile_control_provenance(),
            Some(CompileControlProvenance::Cancelled)
        );
        assert_eq!(original.message(), message);
        assert_eq!(original.failure_cause(), Some(&source));

        let plain = QueryServiceError::new(QueryServiceErrorKind::Internal, "same failure");
        assert_eq!(plain.clone().with_compile_control(None), plain);
        assert_eq!(plain.clone().with_compile_control_provenance(None), plain);
        assert_eq!(
            plain
                .clone()
                .with_compile_control(Some(CompileControlError::Cancelled))
                .with_compile_control(None),
            plain
        );
        assert_eq!(
            plain
                .clone()
                .with_compile_control_provenance(Some(CompileControlProvenance::DeadlineExceeded))
                .with_compile_control_provenance(None),
            plain
        );
    }

    #[test]
    fn known_uncommitted_preserves_primary_and_renders_independent_terminal() {
        let input = selected_input();
        let expected_user_error = input.user_error();
        let expected_message = "[query.execution.unexpected] writer execution failed";
        let selected = QueryServiceError::from_execution_failure(input.clone());
        assert_eq!(selected.message(), expected_message);
        let terminal = publication(LakePublicationDisposition::KnownUncommitted);
        let service = selected.attach_publication_terminal(terminal.clone());
        let expected_public = format!(
            "{expected_message} (lake publication id={} family=write disposition=KnownUncommitted)",
            terminal.header().publication_id()
        );
        assert_eq!(service.message(), expected_public);
        assert!(!service.message().contains("do_not_retry"));
        assert!(!service.message().contains("do not retry statement"));
        let repeated = service
            .clone()
            .attach_publication_terminal(terminal.clone());
        assert_eq!(repeated.message(), expected_public);
        assert_eq!(service.execution_failure(), Some(&input));
        assert_eq!(service.user_error(), Some(&expected_user_error));
        assert_eq!(service.publication_terminal(), Some(&terminal));
        assert!(!terminal.do_not_retry());
    }

    #[test]
    fn stage_noncommit_wire_message_keeps_source_descriptor_and_negative_fact() {
        use novarocks_failure_contract::SharedFailureCause;
        use novarocks_spi::connector::{ConnectorMutationFailure, ConnectorMutationFailureKind};
        for kind in [
            ConnectorMutationFailureKind::Unavailable,
            ConnectorMutationFailureKind::PermissionDenied,
        ] {
            let original = SharedFailureCause::from_static_detail(
                ConnectorMutationFailure::new(kind, "private provider diagnostic"),
                "stage request failed",
            );
            let terminal = LakePublicationTerminal::new(
                LakePublicationMarkerHeader::new(
                    LakePublicationId::new_v7(),
                    LakePublicationFamily::Ctas,
                ),
                LakePublicationTarget::try_new(
                    "private-catalog".into(),
                    "private-namespace".into(),
                    Some("private-table".into()),
                    None,
                )
                .unwrap(),
                LakePublicationDisposition::KnownUncommitted,
                LakePublicationNextAction::RetryStatement,
                Some(LakePublicationStatementTag::try_new("private-tag".into()).unwrap()),
            );
            let selected = QueryServiceError::from_failure_cause(original.clone());
            let input = selected.execution_failure().unwrap().clone();
            let primary = selected.user_error().unwrap().clone();
            let service = selected.attach_publication_terminal(terminal.clone());
            // These are the exact public message bytes consumed by both MySQL
            // selected-error writers; no adapter reconstructs a terminal label.
            let wire_message = service.message().as_bytes().to_vec();
            let expected = format!(
                "{} (lake publication id={} family=ctas disposition=KnownUncommitted)",
                primary,
                terminal.header().publication_id()
            );
            assert_eq!(wire_message.as_slice(), expected.as_bytes());
            assert_eq!(service.failure_cause(), Some(&original));
            assert_eq!(
                service
                    .failure_cause()
                    .unwrap()
                    .downcast_ref::<ConnectorMutationFailure>()
                    .unwrap()
                    .kind(),
                kind
            );
            assert_eq!(service.execution_failure(), Some(&input));
            assert_eq!(input.fact(), original.failure_fact());
            assert_eq!(service.user_error(), Some(&primary));
            assert_eq!(
                input.descriptor().descriptor.code.as_str(),
                "query.execution.dependency"
            );
            assert_eq!(
                (input.descriptor().errno, input.descriptor().sqlstate),
                (1105, "HY000")
            );
            assert_eq!(service.publication_terminal(), Some(&terminal));
            assert!(service.external_effect_disposition().is_none());
            assert!(service.compile_control_error().is_none());
            assert!(!terminal.do_not_retry());
            assert!(!service.message().contains("do_not_retry"));
            assert!(!service.message().contains("do not retry statement"));
            for private in [
                "private provider diagnostic",
                "private-catalog",
                "private-namespace",
                "private-table",
                "private-tag",
            ] {
                assert!(!service.message().contains(private));
            }
            let repeated = service.attach_publication_terminal(terminal);
            assert_eq!(repeated.message().as_bytes(), wire_message.as_slice());
        }
    }

    #[test]
    fn non_retryable_publication_keeps_selected_input_and_descriptor() {
        for (disposition, name) in [
            (LakePublicationDisposition::KnownCommitted, "KnownCommitted"),
            (LakePublicationDisposition::CommitUnknown, "CommitUnknown"),
        ] {
            let input = selected_input();
            let expected_user_error = input.user_error();
            let terminal = publication(disposition);
            let expected_message = format!(
                "[query.execution.unexpected] writer execution failed (lake publication id={} family=write disposition={} do_not_retry=true; do not retry statement)",
                terminal.header().publication_id(),
                name,
            );
            let service = QueryServiceError::from_execution_failure(input.clone())
                .attach_publication_terminal(terminal.clone());
            assert_eq!(service.message(), expected_message);
            assert_eq!(service.execution_failure(), Some(&input));
            assert_eq!(service.user_error(), Some(&expected_user_error));
            assert_eq!(service.publication_terminal(), Some(&terminal));
            assert!(terminal.do_not_retry());
            assert_eq!(service.kind(), QueryServiceErrorKind::Internal);
            assert_eq!(
                (input.descriptor().errno, input.descriptor().sqlstate),
                (1105, "HY000")
            );
            assert_eq!(
                input.descriptor().descriptor.code.as_str(),
                "query.execution.unexpected"
            );
            assert_eq!(input.detail().as_str(), "writer execution failed");
            for private in [
                "private provider diagnostic",
                "private-catalog",
                "private-namespace",
                "private-table",
                "private-reference",
                "private-tag",
            ] {
                assert!(!service.message().contains(private));
            }
            // The public suffix belongs to this verdict, not the immutable
            // execution detail, and reattachment cannot accumulate suffixes.
            let repeated = service.attach_publication_terminal(terminal);
            assert_eq!(repeated.message(), expected_message);
            assert_eq!(repeated.execution_failure(), Some(&input));
        }
    }

    #[test]
    fn cumulative_effect_and_terminal_survive_both_attachment_orders_and_reattachment() {
        use novarocks_failure_contract::SharedFailureCause;
        let source = SharedFailureCause::from_static_detail(
            ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                "private provider diagnostic",
            ),
            "selected source failed",
        );
        let selected = QueryServiceError::from_failure_cause(source.clone());
        let input = selected.execution_failure().unwrap().clone();
        let primary = selected.user_error().unwrap().clone();
        for (effect, effect_label) in [
            (
                StatementExternalEffectDisposition::KnownUncommitted,
                "KnownUncommitted",
            ),
            (
                StatementExternalEffectDisposition::KnownCommitted,
                "KnownCommitted",
            ),
            (
                StatementExternalEffectDisposition::CommitUnknown,
                "CommitUnknown",
            ),
        ] {
            for (knowledge, label) in [
                (
                    LakePublicationDisposition::KnownUncommitted,
                    "KnownUncommitted",
                ),
                (LakePublicationDisposition::CommitUnknown, "CommitUnknown"),
                (LakePublicationDisposition::KnownCommitted, "KnownCommitted"),
            ] {
                let terminal = publication(knowledge);
                let guard = if knowledge.do_not_retry() {
                    " do_not_retry=true; do not retry statement"
                } else {
                    ""
                };
                let effect_guard = if effect.do_not_retry() {
                    " do_not_retry=true; do not retry statement"
                } else {
                    ""
                };
                let expected = format!(
                    "{primary} (lake publication id={} family=write disposition={label}{guard}) (external effect disposition={effect_label}{effect_guard})",
                    terminal.header().publication_id()
                );
                let effect_first = selected
                    .clone()
                    .attach_external_effect_disposition(effect)
                    .attach_publication_terminal(terminal.clone());
                let terminal_first = selected
                    .clone()
                    .attach_publication_terminal(terminal.clone())
                    .attach_external_effect_disposition(effect);
                for service in [effect_first, terminal_first] {
                    assert_eq!(service.message().as_bytes(), expected.as_bytes());
                    assert_eq!(service.failure_cause(), Some(&source));
                    assert_eq!(service.execution_failure(), Some(&input));
                    assert_eq!(service.user_error(), Some(&primary));
                    assert_eq!(service.publication_terminal(), Some(&terminal));
                    assert_eq!(service.external_effect_disposition(), Some(effect));
                    assert_eq!(input.fact(), source.failure_fact());
                    assert_eq!(
                        input.descriptor().descriptor.code.as_str(),
                        "query.execution.dependency"
                    );
                    assert_eq!(
                        (input.descriptor().errno, input.descriptor().sqlstate),
                        (1105, "HY000")
                    );
                    assert_eq!(
                        service
                            .message()
                            .matches("external effect disposition=")
                            .count(),
                        1
                    );
                    assert_eq!(service.message().matches("lake publication id=").count(), 1);
                    assert_eq!(
                        service.message().matches("do_not_retry=true").count(),
                        usize::from(effect.do_not_retry()) + usize::from(knowledge.do_not_retry())
                    );
                    for private in [
                        "private provider diagnostic",
                        "private-catalog",
                        "private-namespace",
                        "private-table",
                        "private-reference",
                        "private-tag",
                    ] {
                        assert!(!service.message().contains(private));
                    }
                    // The selected owner's terminal effect is immutable;
                    // repeated attachments cannot replace its received fact.
                    let repeated = service
                        .attach_publication_terminal(terminal.clone())
                        .attach_external_effect_disposition(
                            StatementExternalEffectDisposition::KnownUncommitted,
                        );
                    assert_eq!(repeated.message().as_bytes(), expected.as_bytes());
                    assert_eq!(repeated.external_effect_disposition(), Some(effect));
                    assert_eq!(repeated.publication_terminal(), Some(&terminal));
                    assert_eq!(repeated.failure_cause(), Some(&source));
                    assert_eq!(repeated.execution_failure(), Some(&input));
                }
            }
        }
    }

    #[test]
    fn legacy_publication_message_and_streaming_projection_keep_their_contracts() {
        let legacy = QueryServiceError::new(QueryServiceErrorKind::Internal, "legacy terminal")
            .attach_publication_terminal(publication(LakePublicationDisposition::CommitUnknown));
        assert_eq!(legacy.message(), "legacy terminal");
        assert!(legacy.execution_failure().is_none());
        let input = selected_input();
        let streaming = QueryExecutionError::from_execution(input.clone());
        assert_eq!(streaming.message(), "writer execution failed");
        assert_eq!(
            execution_public_message(&streaming),
            "[query.execution.unexpected] writer execution failed"
        );
        assert_eq!(streaming.execution_failure(), Some(&input));
        let application = QueryExecutionError::new(QueryExecutionErrorKind::Failed, "application");
        assert!(matches!(
            execution_public_message(&application),
            Cow::Borrowed("application")
        ));
    }

    fn original_stop_root() -> (
        novarocks_workload_control::WorkloadControl,
        novarocks_workload_control::RootWork,
        crate::cancellation::QueryCancellationView,
    ) {
        use novarocks_workload_control::{
            ResourceConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
        };
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 128,
                per_scope_bytes: 896,
            },
        )
        .unwrap();
        control.mark_ready().unwrap();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let cancellation = crate::cancellation::QueryCancellationView::governed(
            root.owner.scope().cancellation().unwrap(),
            Some(250),
        );
        (control, root, cancellation)
    }

    fn original_stop_cause() -> novarocks_failure_contract::SharedFailureCause {
        novarocks_failure_contract::SharedFailureCause::from_static_detail(
            novarocks_workload_control::WorkError::Cancelled(
                novarocks_workload_control::CancellationReason::Requested,
            ),
            "state store original physical execution failed",
        )
    }

    #[test]
    fn local_stop_requires_actual_root_cancellation_and_preserves_original_source() {
        use novarocks_execution_contract::failure::FailureEvidence;
        use novarocks_workload_control::{CancellationReason, WorkError};
        let (_control, root, cancellation) = original_stop_root();
        let source = original_stop_cause();
        let original = QueryServiceError::from_failure_cause(source.clone());
        let observed = original.execution_failure().unwrap().clone();
        assert_eq!(observed.fact().evidence(), FailureEvidence::StopObservation);
        assert_eq!(
            original
                .clone()
                .reconcile_local_stop_observation(&cancellation),
            original,
        );
        root.owner.cancel(CancellationReason::DeadlineExceeded);
        let expected = crate::session_outcome::cancellation_failure_input(
            cancellation
                .reason()
                .expect("same Root accepted cancellation"),
        );
        let selected = original.reconcile_local_stop_observation(&cancellation);
        assert_eq!(selected.execution_failure(), Some(&expected));
        assert_eq!(selected.kind(), QueryServiceErrorKind::Timeout);
        assert_eq!(selected.failure_cause(), Some(&source));
        let retained = selected.failure_cause().unwrap();
        assert_eq!(retained.failure_fact(), observed.fact());
        assert_eq!(retained.detail(), observed.detail().as_str());
        assert!(matches!(
            retained.downcast_ref::<WorkError>(),
            Some(WorkError::Cancelled(CancellationReason::Requested))
        ));
        assert_eq!(
            selected
                .clone()
                .reconcile_local_stop_observation(&cancellation),
            selected,
            "the accepted query verdict is not another stop observation",
        );
        root.owner.complete_after_terminal_cancel_settled();
        root.business.release();
    }

    #[test]
    fn local_stop_reconciliation_retains_both_knowledge_fields_and_compile_sidecars() {
        use novarocks_workload_control::CancellationReason;
        let (_control, root, cancellation) = original_stop_root();
        root.owner.cancel(CancellationReason::DeadlineExceeded);
        let source = original_stop_cause();
        let terminal = publication(LakePublicationDisposition::KnownUncommitted);
        let original = QueryServiceError::from_failure_cause(source.clone())
            .with_compile_control(Some(
                novarocks_type_contract::CompileControlError::Cancelled,
            ))
            .attach_external_effect_disposition(StatementExternalEffectDisposition::KnownCommitted)
            .attach_publication_terminal(terminal.clone());
        let control = original.compile_control_error();
        let provenance = original.compile_control_provenance();
        let selected = original.reconcile_local_stop_observation(&cancellation);
        assert_eq!(selected.failure_cause(), Some(&source));
        assert_eq!(selected.compile_control_error(), control);
        assert_eq!(selected.compile_control_provenance(), provenance);
        assert_eq!(selected.publication_terminal(), Some(&terminal));
        assert_eq!(
            selected.external_effect_disposition(),
            Some(StatementExternalEffectDisposition::KnownCommitted)
        );
        assert!(selected.message().contains("disposition=KnownUncommitted)"));
        assert!(selected.message().contains(
            "external effect disposition=KnownCommitted do_not_retry=true; do not retry statement)"
        ));
        assert_eq!(selected.message().matches("do_not_retry=true").count(), 1);
        assert!(!selected.message().contains("private-catalog"));
        root.owner.complete_after_terminal_cancel_settled();
        root.business.release();
    }

    #[test]
    fn local_stop_reconciliation_refuses_foreign_site_missing_or_mismatched_source() {
        use novarocks_workload_control::CancellationReason;
        let (_control, root, cancellation) = original_stop_root();
        root.owner.cancel(CancellationReason::DeadlineExceeded);
        let source = original_stop_cause();
        let mut foreign = QueryServiceError::from_execution_failure(ExecutionFailureInput::new(
            source.failure_fact(),
            SafeDetail::truncating(source.detail()),
            ExecutionFailureSite::Backend {
                process: novarocks_types::BackendProcessId::new_v7(),
            },
        ));
        foreign.failure_cause = Some(source.clone());
        let missing = QueryServiceError::from_execution_failure(ExecutionFailureInput::new(
            source.failure_fact(),
            SafeDetail::truncating(source.detail()),
            ExecutionFailureSite::Coordinator,
        ));
        let mut mismatched = QueryServiceError::from_failure_cause(source);
        mismatched.failure_cause = Some(novarocks_failure_contract::SharedFailureCause::new(
            novarocks_workload_control::WorkError::ForeignAuthority,
        ));
        let originating = QueryServiceError::from_failure_cause(
            novarocks_failure_contract::SharedFailureCause::new(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "original business failure",
            )),
        );
        for original in [foreign, missing, mismatched, originating] {
            assert_eq!(
                original
                    .clone()
                    .reconcile_local_stop_observation(&cancellation),
                original,
            );
        }
        root.owner.complete_after_terminal_cancel_settled();
        root.business.release();
    }
}

#[cfg(test)]
mod external_effect_companion_tests {
    use super::*;
    #[test]
    fn external_effect_neither_reauthors_cause_nor_invents_publication_identity() {
        let original = QueryServiceError::from_compile_control(
            novarocks_type_contract::CompileControlError::ResourceExhausted,
        );
        let input = original.execution_failure().unwrap().clone();
        for disposition in [
            StatementExternalEffectDisposition::KnownUncommitted,
            StatementExternalEffectDisposition::CommitUnknown,
            StatementExternalEffectDisposition::KnownCommitted,
        ] {
            let service = original
                .clone()
                .attach_external_effect_disposition(disposition);
            assert_eq!(service.execution_failure(), Some(&input));
            assert_eq!(service.user_error(), original.user_error());
            assert_eq!(service.kind(), original.kind());
            assert_eq!(service.external_effect_disposition(), Some(disposition));
            assert_eq!(service.publication_terminal(), None);
            assert!(
                service
                    .message()
                    .contains(&format!("external effect disposition={disposition:?}"))
            );
            assert!(!service.message().contains("maintenance publication"));
            assert!(!service.message().contains("lake publication"));
            assert_eq!(
                service.message().contains("do not retry statement"),
                disposition.do_not_retry()
            );
            let repeated = service
                .clone()
                .attach_external_effect_disposition(disposition);
            assert_eq!(repeated.message(), service.message());
            assert_eq!(repeated.execution_failure(), Some(&input));
            let message =
                QueryServiceError::new(QueryServiceErrorKind::Internal, "selected failure")
                    .attach_external_effect_disposition(disposition);
            let suffix = if disposition.do_not_retry() {
                " do_not_retry=true; do not retry statement"
            } else {
                ""
            };
            assert_eq!(
                message.message(),
                format!("selected failure (external effect disposition={disposition:?}{suffix})")
            );
            assert_eq!(message.external_effect_disposition(), Some(disposition));
            assert!(message.publication_terminal().is_none());
        }
    }
}
