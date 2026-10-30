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

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum StateStoreErrorKind {
    InvalidRequest,
    InvalidConfiguration,
    UnsupportedFormat,
    /// The request itself is outside a fixed storage limit. Permanent:
    /// retrying the same request cannot succeed.
    LimitExceeded,
    /// The instance is at its outstanding-attempt ceiling. Transient and
    /// free of write effect, so a caller may back off and retry inside its
    /// own budget. Deliberately distinct from `LimitExceeded`, which is a
    /// permanent property of the request.
    Saturated,
    DeadlineExceeded,
    PreconditionFailed,
    Conflict,
    Transient,
    Corruption,
    ProviderUnavailable,
    Cancelled,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateStoreError {
    kind: StateStoreErrorKind,
    message: &'static str,
    cleanup: Option<Box<StateStoreError>>,
}

impl StateStoreError {
    pub const fn new(kind: StateStoreErrorKind, message: &'static str) -> Self {
        Self {
            kind,
            message,
            cleanup: None,
        }
    }

    pub const fn kind(&self) -> StateStoreErrorKind {
        self.kind
    }

    pub fn with_cleanup_context(mut self, cleanup: StateStoreError) -> Self {
        self.cleanup = Some(Box::new(cleanup));
        self
    }

    pub fn cleanup_context(&self) -> Option<&StateStoreError> {
        self.cleanup.as_deref()
    }
}

impl fmt::Display for StateStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: {}", self.kind, self.message)?;
        if let Some(cleanup) = &self.cleanup {
            write!(formatter, "; cleanup failed: {cleanup}")?;
        }
        Ok(())
    }
}

impl std::error::Error for StateStoreError {}

impl novarocks_failure_contract::FailureProjection for StateStoreError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        let (class, code) = match self.kind {
            StateStoreErrorKind::InvalidRequest => (FailureClass::Data, 1),
            StateStoreErrorKind::InvalidConfiguration => (FailureClass::Invariant, 2),
            StateStoreErrorKind::UnsupportedFormat => (FailureClass::Unsupported, 3),
            StateStoreErrorKind::LimitExceeded => (FailureClass::Resource, 4),
            StateStoreErrorKind::Saturated => (FailureClass::Resource, 5),
            StateStoreErrorKind::DeadlineExceeded => (FailureClass::Control, 6),
            StateStoreErrorKind::PreconditionFailed => (FailureClass::Dependency, 7),
            StateStoreErrorKind::Conflict => (FailureClass::Dependency, 8),
            StateStoreErrorKind::Transient => (FailureClass::Dependency, 9),
            StateStoreErrorKind::Corruption => (FailureClass::Data, 10),
            StateStoreErrorKind::ProviderUnavailable => (FailureClass::Dependency, 11),
            StateStoreErrorKind::Cancelled => (FailureClass::Control, 12),
            StateStoreErrorKind::Internal => (FailureClass::Invariant, 13),
        };
        let reason = FailureReason::new(
            domain::STATE_STORE,
            std::num::NonZeroU16::new(code).unwrap(),
        );
        if matches!(
            self.kind,
            StateStoreErrorKind::Cancelled | StateStoreErrorKind::DeadlineExceeded
        ) {
            // A provider observes its captured stop; it does not accept a new
            // client cancellation or replace a preceding originating failure.
            FailureFact::stop_observation(reason)
        } else {
            FailureFact::new(class, reason)
        }
    }
}
impl StateStoreError {
    /// The provider-authored fact and original error enter one immutable cause.
    /// Its fixed diagnostic never evaluates a foreign formatter.
    pub fn into_original_failure(self) -> crate::StateStoreExecutionFailure {
        let detail = self.message;
        novarocks_failure_contract::SharedFailureCause::from_static_detail(self, detail)
    }
}
