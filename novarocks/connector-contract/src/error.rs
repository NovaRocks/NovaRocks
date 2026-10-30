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

use std::fmt;

use crate::ConnectorIdentityError;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ConnectorErrorKind {
    InvalidRequest,
    NotFound,
    PermissionDenied,
    Unsupported,
    Cancelled,
    DeadlineExceeded,
    ResourceExhausted,
    /// A caller-control channel carrying its original typed failure. This is
    /// neither a cancellation intent nor a recovery classification.
    Interrupted,
    Unavailable,
    CorruptData,
    Internal,
}

/// Typed classification for a current-table binding rejection.
///
/// A durable caller must distinguish a missing logical target from a target
/// whose name was rebound to another physical object. Neither condition is a
/// transient catalog outage, and treating either as one could make a retry
/// silently attach work to a replacement table.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ConnectorTableObjectBindingFailure {
    /// The logical target now resolves to another physical table object.
    Replaced,
    /// The logical target no longer resolves to a table object.
    Missing,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ConnectorErrorCause {
    Domain {
        kind: ConnectorErrorKind,
        source: novarocks_failure_contract::SharedFailureCause,
    },
    Kind(ConnectorErrorKind),
    TableObjectBinding(ConnectorTableObjectBindingFailure),
    Identity(ConnectorIdentityError),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorError {
    cause: ConnectorErrorCause,
    message: String,
    retryable_before_progress: bool,
    cleanup_context: Option<String>,
    public_failure_detail: Option<novarocks_failure_contract::BoundedDetail>,
}

impl ConnectorError {
    pub fn new(kind: ConnectorErrorKind, message: impl Into<String>) -> Self {
        Self {
            cause: ConnectorErrorCause::Kind(kind),
            message: message.into(),
            retryable_before_progress: false,
            cleanup_context: None,
            public_failure_detail: None,
        }
    }

    /// Preserve a producer-owned domain cause. The operation kind remains
    /// descriptive; neutral classification and quantities come from that cause.
    pub fn from_domain<E>(kind: ConnectorErrorKind, cause: E) -> Self
    where
        E: std::error::Error
            + novarocks_failure_contract::FailureProjection
            + Send
            + Sync
            + 'static,
    {
        let source = novarocks_failure_contract::SharedFailureCause::new(cause);
        Self {
            message: source.detail().to_owned(),
            cause: ConnectorErrorCause::Domain { kind, source },
            retryable_before_progress: false,
            cleanup_context: None,
            public_failure_detail: None,
        }
    }

    /// Closed producers with a fixed public diagnostic keep the exact domain
    /// cause without invoking its Display. Geometry below describes precisely
    /// this construction, not the arbitrary formatting path in from_domain.
    pub fn from_domain_static<E>(kind: ConnectorErrorKind, cause: E, detail: &'static str) -> Self
    where
        E: std::error::Error
            + novarocks_failure_contract::FailureProjection
            + Send
            + Sync
            + 'static,
    {
        let source =
            novarocks_failure_contract::SharedFailureCause::from_static_detail(cause, detail);
        Self {
            message: source.detail().to_owned(),
            cause: ConnectorErrorCause::Domain { kind, source },
            retryable_before_progress: false,
            cleanup_context: None,
            public_failure_detail: None,
        }
    }

    /// First-party storage of from_domain_static: its actual CauseRecord Arc,
    /// plus exactly two owned copies of the same bounded UTF-8 prefix. The
    /// inline E is included once in that record. Its existing pointees and
    /// this inline ConnectorError remain with their original source authors.
    pub fn static_domain_backing_bytes<E>(
        detail: &'static str,
    ) -> Result<usize, novarocks_type_contract::owned_resources::layout::LayoutResourceError> {
        use novarocks_type_contract::owned_resources::layout::{LayoutResourceError, arc_layout};
        let record =
            arc_layout(novarocks_failure_contract::SharedFailureCause::record_layout::<E>())?;
        let text = novarocks_failure_contract::BoundedDetail::truncating_backing_len(detail);
        text.checked_mul(2)
            .and_then(|bytes| record.size().checked_add(bytes))
            .ok_or(LayoutResourceError::Arithmetic)
    }

    /// Preserve the original pure compiler refusal, including a caller-owned
    /// source whose semantic class need not be Control or Resource.
    pub fn compile_interruption(cause: novarocks_type_contract::CompileControlError) -> Self {
        use novarocks_type_contract::CompileControlError;
        let kind = match &cause {
            CompileControlError::Cancelled => ConnectorErrorKind::Cancelled,
            CompileControlError::DeadlineExceeded => ConnectorErrorKind::DeadlineExceeded,
            CompileControlError::ResourceExhausted => ConnectorErrorKind::ResourceExhausted,
            CompileControlError::Source(_) => ConnectorErrorKind::Interrupted,
        };
        Self::from_domain(kind, cause)
    }

    /// Report a terminal, typed current-table binding failure.
    pub fn table_object_binding(
        failure: ConnectorTableObjectBindingFailure,
        message: impl Into<String>,
    ) -> Self {
        Self {
            cause: ConnectorErrorCause::TableObjectBinding(failure),
            message: message.into(),
            retryable_before_progress: false,
            cleanup_context: None,
            public_failure_detail: None,
        }
    }

    pub const fn kind(&self) -> ConnectorErrorKind {
        match &self.cause {
            ConnectorErrorCause::Kind(kind) => *kind,
            ConnectorErrorCause::Domain { kind, .. } => *kind,
            ConnectorErrorCause::Identity(_) => ConnectorErrorKind::InvalidRequest,
            ConnectorErrorCause::TableObjectBinding(
                ConnectorTableObjectBindingFailure::Replaced,
            ) => ConnectorErrorKind::InvalidRequest,
            ConnectorErrorCause::TableObjectBinding(
                ConnectorTableObjectBindingFailure::Missing,
            ) => ConnectorErrorKind::NotFound,
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    /// Explicitly publish this producer-owned message as a bounded diagnostic.
    /// Only an author that knows the message is public may opt in. Kind,
    /// length, and a foreign error's Display are never evidence of that fact.
    pub fn with_public_failure_detail(mut self) -> Self {
        self.public_failure_detail = Some(novarocks_failure_contract::BoundedDetail::truncating(
            &self.message,
        ));
        self
    }

    /// The original producer's public projection, independent of diagnostic
    /// context appended later. Absence never authorizes publishing message().
    pub fn public_failure_detail(&self) -> Option<&str> {
        self.public_failure_detail
            .as_ref()
            .map(novarocks_failure_contract::BoundedDetail::as_str)
    }

    /// Extend a diagnostic without rebuilding its classification, retry or
    /// cleanup facts. Context adds information, never a new failure verdict.
    pub fn map_message(mut self, map: impl FnOnce(String) -> String) -> Self {
        self.message = map(self.message);
        self
    }

    /// The typed current-table binding classification, when this error is one.
    pub const fn table_object_binding_failure(&self) -> Option<ConnectorTableObjectBindingFailure> {
        match self.cause {
            ConnectorErrorCause::TableObjectBinding(failure) => Some(failure),
            _ => None,
        }
    }

    /// Whether this error rejects a durable table binding without a safe retry.
    pub const fn is_table_object_binding_failure(&self) -> bool {
        self.table_object_binding_failure().is_some()
    }

    pub const fn retryable_before_progress(&self) -> bool {
        self.retryable_before_progress
    }

    pub fn with_retryable_before_progress(mut self) -> Self {
        if matches!(self.cause, ConnectorErrorCause::Kind(_)) {
            self.retryable_before_progress = true;
        }
        self
    }

    pub fn with_cleanup_context(mut self, cleanup_context: impl Into<String>) -> Self {
        self.cleanup_context = Some(cleanup_context.into());
        self
    }
}

impl fmt::Display for ConnectorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: {}", self.kind(), self.message)?;
        if let Some(cleanup_context) = &self.cleanup_context {
            write!(formatter, " (cleanup: {cleanup_context})")?;
        }
        Ok(())
    }
}

impl std::error::Error for ConnectorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.cause {
            ConnectorErrorCause::Identity(cause) => Some(cause),
            ConnectorErrorCause::Domain { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl From<ConnectorIdentityError> for ConnectorError {
    fn from(error: ConnectorIdentityError) -> Self {
        Self {
            message: error.to_string(),
            cause: ConnectorErrorCause::Identity(error),
            retryable_before_progress: false,
            cleanup_context: None,
            public_failure_detail: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_context_preserves_every_error_fact() {
        for error in [
            ConnectorError::new(ConnectorErrorKind::Unavailable, "network")
                .with_retryable_before_progress(),
            ConnectorError::new(ConnectorErrorKind::CorruptData, "manifest"),
            ConnectorError::new(ConnectorErrorKind::PermissionDenied, "denied"),
            ConnectorError::table_object_binding(
                ConnectorTableObjectBindingFailure::Replaced,
                "rebound",
            ),
        ] {
            let original = error.with_cleanup_context("owned cleanup");
            let contextualized = original
                .clone()
                .map_message(|message| format!("{message}; context"));
            assert_eq!(contextualized.kind(), original.kind());
            assert_eq!(
                contextualized.retryable_before_progress,
                original.retryable_before_progress
            );
            assert_eq!(contextualized.cleanup_context, original.cleanup_context);
            assert_eq!(
                contextualized.table_object_binding_failure(),
                original.table_object_binding_failure()
            );
            assert_eq!(
                contextualized.message,
                format!("{}; context", original.message)
            );
        }
    }

    #[test]
    fn ordinary_errors_keep_their_retryable_flag() {
        let error = ConnectorError::new(ConnectorErrorKind::Unavailable, "transient")
            .with_retryable_before_progress();
        assert!(error.retryable_before_progress());
    }

    #[test]
    fn table_object_binding_failures_stay_typed_and_non_retryable() {
        for (failure, kind) in [
            (
                ConnectorTableObjectBindingFailure::Replaced,
                ConnectorErrorKind::InvalidRequest,
            ),
            (
                ConnectorTableObjectBindingFailure::Missing,
                ConnectorErrorKind::NotFound,
            ),
        ] {
            let error = ConnectorError::table_object_binding(failure, "table binding rejected")
                .with_retryable_before_progress();
            assert!(error.is_table_object_binding_failure());
            assert_eq!(error.table_object_binding_failure(), Some(failure));
            assert_eq!(error.kind(), kind);
            assert!(!error.retryable_before_progress());
        }
    }
}

/// Connector facts stay separate from SQL login and SQL authorization facts.
impl novarocks_failure_contract::FailureProjection for ConnectorError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{
            FailureClass as Class, FailureFact, FailureReason, domain,
        };
        if let ConnectorErrorCause::Domain { source, .. } = &self.cause {
            return source.failure_fact();
        }
        if let ConnectorErrorCause::Identity(cause) = &self.cause {
            return cause.failure_fact();
        }
        let (class, code) = if let Some(binding) = self.table_object_binding_failure() {
            (
                Class::Dependency,
                match binding {
                    ConnectorTableObjectBindingFailure::Replaced => 20,
                    ConnectorTableObjectBindingFailure::Missing => 21,
                },
            )
        } else {
            return self.kind().failure_fact();
        };
        FailureFact::new(
            class,
            FailureReason::new(
                domain::CONNECTOR,
                std::num::NonZeroU16::new(code).expect("nonzero reason"),
            ),
        )
    }
}

/// A closed kind's fact does not require allocating a diagnostic carrier.
impl novarocks_failure_contract::FailureProjection for ConnectorErrorKind {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{
            FailureClass as Class, FailureFact, FailureReason, domain,
        };
        let (class, code) = match self {
            ConnectorErrorKind::InvalidRequest => (Class::Data, 1),
            ConnectorErrorKind::NotFound => (Class::Dependency, 2),
            ConnectorErrorKind::PermissionDenied => (Class::Dependency, 3),
            ConnectorErrorKind::Unsupported => (Class::Unsupported, 4),
            ConnectorErrorKind::Cancelled => (Class::Control, 5),
            ConnectorErrorKind::DeadlineExceeded => (Class::Control, 6),
            ConnectorErrorKind::ResourceExhausted => (Class::Resource, 7),
            ConnectorErrorKind::Unavailable => (Class::Dependency, 8),
            ConnectorErrorKind::CorruptData => (Class::Data, 9),
            ConnectorErrorKind::Internal => (Class::Unexpected, 10),
            // An interruption descriptor without its required typed source
            // is an invalid construction, not evidence of caller intent.
            ConnectorErrorKind::Interrupted => (Class::Invariant, 11),
        };
        FailureFact::new(
            class,
            FailureReason::new(
                domain::CONNECTOR,
                std::num::NonZeroU16::new(code).expect("nonzero reason"),
            ),
        )
    }
}

#[cfg(test)]
mod failure_projection_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    #[test]
    fn unsupported_retains_its_connector_reason_without_reclassifying_a_domain_source() {
        let error = ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "cancelled dependency refused",
        );
        assert_eq!(error.failure_fact().class(), FailureClass::Unsupported);
        assert_eq!(error.failure_fact().reason().code(), 4);
        let source = ConnectorError::new(ConnectorErrorKind::Unavailable, "unsupported capability");
        let fact = source.failure_fact();
        let wrapper = ConnectorError::from_domain(ConnectorErrorKind::Unsupported, source.clone());
        assert_eq!(wrapper.failure_fact(), fact);
        let preserved = std::error::Error::source(&wrapper)
            .unwrap()
            .downcast_ref::<novarocks_failure_contract::SharedFailureCause>()
            .unwrap();
        assert_eq!(preserved.downcast_ref::<ConnectorError>(), Some(&source));
    }

    #[test]
    fn external_credentials_and_replaced_targets_keep_distinct_dependency_reasons() {
        let credentials =
            ConnectorError::new(ConnectorErrorKind::PermissionDenied, "numeric overflow");
        assert_eq!(credentials.failure_fact().class(), FailureClass::Dependency);
        assert_eq!(credentials.failure_fact().reason().code(), 3);
        let replaced = ConnectorError::table_object_binding(
            ConnectorTableObjectBindingFailure::Replaced,
            "access denied",
        );
        assert_eq!(replaced.failure_fact().reason().code(), 20);
        assert_ne!(replaced.failure_fact(), credentials.failure_fact());
    }
}

#[cfg(test)]
mod identity_source_tests {
    use super::*;
    use novarocks_failure_contract::FailureProjection;
    #[test]
    fn diagnostic_context_keeps_identity_reason_source_and_permanent_rejection() {
        let source = ConnectorIdentityError::InvalidWriteTargetOrdinal;
        let error = ConnectorError::from(source)
            .map_message(|_| "ResourceExhausted: arbitrary diagnostic".into())
            .with_cleanup_context("later cleanup")
            .with_retryable_before_progress();
        assert_eq!(error.failure_fact(), source.failure_fact());
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(!error.retryable_before_progress());
        assert_eq!(
            std::error::Error::source(&error)
                .unwrap()
                .downcast_ref::<ConnectorIdentityError>(),
            Some(&source)
        );
        assert_eq!(error.table_object_binding_failure(), None);
    }
}

#[cfg(test)]
mod domain_source_tests {
    use super::*;
    use novarocks_failure_contract::{
        FailureClass, FailureFact, FailureProjection, FailureReason, domain,
    };
    use std::error::Error;
    #[derive(Debug)]
    struct LocalContractRefusal;
    impl fmt::Display for LocalContractRefusal {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("ResourceExhausted: misleading diagnostic")
        }
    }
    impl Error for LocalContractRefusal {}
    impl FailureProjection for LocalContractRefusal {
        fn failure_fact(&self) -> FailureFact {
            FailureFact::new(
                FailureClass::Invariant,
                FailureReason::new(domain::CONNECTOR, std::num::NonZeroU16::new(99).unwrap()),
            )
        }
    }
    #[test]
    fn compile_interruption_keeps_original_invariant_and_has_no_retry_authority() {
        use novarocks_type_contract::{
            CompileControlError, owned_resources::layout::LayoutResourceError,
        };
        let raw = LayoutResourceError::SourceModel;
        let expected = raw.failure_fact();
        let cause = CompileControlError::from_domain(raw);
        let original = ConnectorError::compile_interruption(cause.clone());
        let contextual = original
            .clone()
            .map_message(|_| "Cancelled: network unavailable".into())
            .with_retryable_before_progress();
        assert_eq!(original.kind(), ConnectorErrorKind::Interrupted);
        assert_eq!(expected.class(), FailureClass::Invariant);
        for error in [&original, &contextual] {
            assert_eq!(error.failure_fact(), expected);
            assert!(!error.retryable_before_progress());
            let actual = error
                .source()
                .unwrap()
                .source()
                .unwrap()
                .downcast_ref::<CompileControlError>()
                .unwrap();
            assert_eq!(actual, &cause);
            assert!(
                actual
                    .source()
                    .unwrap()
                    .source()
                    .unwrap()
                    .downcast_ref::<LayoutResourceError>()
                    .is_some()
            );
        }
    }

    #[test]
    fn domain_source_is_authoritative_across_diagnostic_context_and_clone() {
        let original =
            ConnectorError::from_domain(ConnectorErrorKind::Internal, LocalContractRefusal);
        let clone = original
            .clone()
            .map_message(|_| "network unavailable".into());
        assert_eq!(original.failure_fact(), LocalContractRefusal.failure_fact());
        assert_eq!(clone.failure_fact(), original.failure_fact());
        assert_eq!(clone.kind(), ConnectorErrorKind::Internal);
        let a = original
            .source()
            .unwrap()
            .source()
            .unwrap()
            .downcast_ref::<LocalContractRefusal>()
            .unwrap();
        let b = clone
            .source()
            .unwrap()
            .source()
            .unwrap()
            .downcast_ref::<LocalContractRefusal>()
            .unwrap();
        assert!(std::ptr::eq(a, b));
    }
    #[test]
    fn public_detail_requires_its_original_author_and_survives_private_context() {
        for error in [
            ConnectorError::new(ConnectorErrorKind::Unsupported, "provider refusal"),
            ConnectorError::from_domain(ConnectorErrorKind::Internal, LocalContractRefusal),
            ConnectorError::from_domain_static(
                ConnectorErrorKind::Unavailable,
                LocalContractRefusal,
                "fixed internal diagnostic",
            ),
            ConnectorError::table_object_binding(
                ConnectorTableObjectBindingFailure::Replaced,
                "original table binding refusal",
            ),
        ] {
            assert_eq!(error.public_failure_detail(), None);
            let fact = error.failure_fact();
            let original_message = error.message().to_owned();
            let public = error.with_public_failure_detail();
            assert_eq!(
                public.public_failure_detail(),
                Some(original_message.as_str())
            );
            let contextualized = public
                .clone()
                .map_message(|_| "private later SDK context".into())
                .with_cleanup_context("private cleanup context");
            assert_eq!(contextualized.failure_fact(), fact);
            assert_eq!(
                contextualized.public_failure_detail(),
                public.public_failure_detail()
            );
            assert_eq!(contextualized.message(), "private later SDK context");
        }
    }

    #[test]
    fn explicitly_public_detail_uses_the_existing_utf8_diagnostic_bound() {
        let error = ConnectorError::new(ConnectorErrorKind::Unsupported, "中".repeat(600));
        assert_eq!(error.public_failure_detail(), None);
        let fact = error.failure_fact();
        let public = error.with_public_failure_detail();
        let expected = novarocks_failure_contract::BoundedDetail::truncating(public.message());
        assert_eq!(public.public_failure_detail(), Some(expected.as_str()));
        assert!(
            public.public_failure_detail().unwrap().len()
                <= novarocks_failure_contract::DIAGNOSTIC_MAX_BYTES
        );
        assert_eq!(public.failure_fact(), fact);
    }

    #[test]
    fn fixed_domain_quote_covers_actual_record_and_both_owned_diagnostics() {
        use novarocks_failure_contract::{BoundedDetail, SharedFailureCause};
        use novarocks_type_contract::owned_resources::layout::arc_layout;
        const DETAIL: &str = "fixed caller-safe diagnostic";
        let error = ConnectorError::from_domain_static(
            ConnectorErrorKind::Unavailable,
            LocalContractRefusal,
            DETAIL,
        );
        let ConnectorErrorCause::Domain { source, .. } = &error.cause else {
            panic!("domain source")
        };
        assert!(source.downcast_ref::<LocalContractRefusal>().is_some());
        assert_eq!(error.failure_fact(), LocalContractRefusal.failure_fact());
        assert_eq!(error.message(), DETAIL);
        assert_eq!(source.detail(), DETAIL);
        let actual_record = arc_layout(SharedFailureCause::record_layout::<LocalContractRefusal>())
            .unwrap()
            .size();
        let actual_detail = BoundedDetail::truncating(DETAIL).into_string();
        assert_eq!(actual_detail.capacity(), DETAIL.len());
        assert_eq!(error.message.capacity(), DETAIL.len());
        assert_eq!(
            ConnectorError::static_domain_backing_bytes::<LocalContractRefusal>(DETAIL).unwrap(),
            actual_record + actual_detail.capacity() + error.message.capacity()
        );
        assert!(
            actual_record
                > arc_layout(std::alloc::Layout::new::<LocalContractRefusal>())
                    .unwrap()
                    .size()
        );
    }

    #[test]
    fn kind_projection_retains_all_original_connector_reason_codes() {
        for (kind, class, code) in [
            (ConnectorErrorKind::InvalidRequest, FailureClass::Data, 1),
            (ConnectorErrorKind::NotFound, FailureClass::Dependency, 2),
            (
                ConnectorErrorKind::PermissionDenied,
                FailureClass::Dependency,
                3,
            ),
            (
                ConnectorErrorKind::Unsupported,
                FailureClass::Unsupported,
                4,
            ),
            (ConnectorErrorKind::Cancelled, FailureClass::Control, 5),
            (
                ConnectorErrorKind::DeadlineExceeded,
                FailureClass::Control,
                6,
            ),
            (
                ConnectorErrorKind::ResourceExhausted,
                FailureClass::Resource,
                7,
            ),
            (ConnectorErrorKind::Unavailable, FailureClass::Dependency, 8),
            (ConnectorErrorKind::CorruptData, FailureClass::Data, 9),
            (ConnectorErrorKind::Internal, FailureClass::Unexpected, 10),
            (ConnectorErrorKind::Interrupted, FailureClass::Invariant, 11),
        ] {
            let fact = kind.failure_fact();
            assert_eq!(fact.class(), class);
            assert_eq!(fact.reason().code(), code);
            assert_eq!(
                fact,
                ConnectorError::new(kind, "unchanged ordinary carrier").failure_fact()
            );
        }
    }
}
