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
pub struct ConnectorError {
    kind: ConnectorErrorKind,
    message: String,
    retryable_before_progress: bool,
    cleanup_context: Option<String>,
    table_object_binding_failure: Option<ConnectorTableObjectBindingFailure>,
}

impl ConnectorError {
    pub fn new(kind: ConnectorErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            retryable_before_progress: false,
            cleanup_context: None,
            table_object_binding_failure: None,
        }
    }

    /// Report a terminal, typed current-table binding failure.
    pub fn table_object_binding(
        failure: ConnectorTableObjectBindingFailure,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind: match failure {
                ConnectorTableObjectBindingFailure::Replaced => ConnectorErrorKind::InvalidRequest,
                ConnectorTableObjectBindingFailure::Missing => ConnectorErrorKind::NotFound,
            },
            message: message.into(),
            retryable_before_progress: false,
            cleanup_context: None,
            table_object_binding_failure: Some(failure),
        }
    }

    pub const fn kind(&self) -> ConnectorErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    /// The typed current-table binding classification, when this error is one.
    pub const fn table_object_binding_failure(&self) -> Option<ConnectorTableObjectBindingFailure> {
        self.table_object_binding_failure
    }

    /// Whether this error rejects a durable table binding without a safe retry.
    pub const fn is_table_object_binding_failure(&self) -> bool {
        self.table_object_binding_failure.is_some()
    }

    pub const fn retryable_before_progress(&self) -> bool {
        self.retryable_before_progress
    }

    pub fn with_retryable_before_progress(mut self) -> Self {
        if self.table_object_binding_failure.is_none() {
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
        write!(formatter, "{:?}: {}", self.kind, self.message)?;
        if let Some(cleanup_context) = &self.cleanup_context {
            write!(formatter, " (cleanup: {cleanup_context})")?;
        }
        Ok(())
    }
}

impl std::error::Error for ConnectorError {}

impl From<ConnectorIdentityError> for ConnectorError {
    fn from(error: ConnectorIdentityError) -> Self {
        Self::new(ConnectorErrorKind::InvalidRequest, error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
