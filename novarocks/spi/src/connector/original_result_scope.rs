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

//! Closed first-party original-result check and retention projection.
//! No allowance, provider credential, wire identity, or measurement is created.

use super::ConnectorPayloadRetentionGuard;
use std::{error::Error, fmt, sync::Arc, time::Instant};

/// Implemented by the caller that already owns the original result binding.
/// Providers cannot obtain an admitted result window from this interface.
pub trait ConnectorOriginalResultScopeCheck: Send + Sync + 'static {
    fn original_deadline(&self) -> Instant;
    /// Original control and Work checks only. Never authorizes allocation.
    fn check_active(&self) -> Result<(), OriginalResultCheckError>;
}

/// Preserve the original cause; presentation never formats an arbitrary source.
pub struct OriginalResultCheckError {
    class: OriginalResultCheckClass,
    source: Box<dyn Error + Send + Sync>,
    fact: novarocks_failure_contract::FailureFact,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OriginalResultCheckClass {
    Control,
    Identity,
    Admission,
    Coverage,
}
impl OriginalResultCheckError {
    pub fn new(
        class: OriginalResultCheckClass,
        source: impl novarocks_failure_contract::FailureProjection + Error + Send + Sync + 'static,
    ) -> Self {
        let fact = source.failure_fact();
        Self {
            class,
            source: Box::new(source),
            fact,
        }
    }
    pub fn class(&self) -> OriginalResultCheckClass {
        self.class
    }
}
impl fmt::Debug for OriginalResultCheckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OriginalResultCheckError")
            .field("class", &self.class)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for OriginalResultCheckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Original result scope check failed: {:?}", self.class)
    }
}
impl Error for OriginalResultCheckError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

impl novarocks_failure_contract::FailureProjection for OriginalResultCheckError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        self.fact
    }
}
impl novarocks_failure_contract::OuterFailureProjection for OriginalResultCheckError {}

#[derive(Clone)]
pub struct ConnectorOriginalResultScope {
    checks: Arc<dyn ConnectorOriginalResultScopeCheck>,
    guard: ConnectorPayloadRetentionGuard,
}
impl ConnectorOriginalResultScope {
    /// Both projections share the supplied neutral retention owner.
    /// The owner must not hold a result activity lease: actual running jobs
    /// retain their activity separately. This creates no new admission.
    pub fn from_original<T: ConnectorOriginalResultScopeCheck>(owner: Arc<T>) -> Self {
        Self {
            checks: owner.clone(),
            guard: ConnectorPayloadRetentionGuard::from_shared(owner),
        }
    }
    pub fn original_deadline(&self) -> Instant {
        self.checks.original_deadline()
    }
    pub fn check_active(&self) -> Result<(), OriginalResultCheckError> {
        self.checks.check_active()
    }

    pub fn retention_guard(&self) -> ConnectorPayloadRetentionGuard {
        self.guard.clone()
    }
    pub fn is_same_original(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.checks, &other.checks)
    }
}
impl fmt::Debug for ConnectorOriginalResultScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ConnectorOriginalResultScope")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_failure_contract::{FailureProjection, SharedFailureCause};

    #[derive(Debug)]
    struct StopCause;
    impl fmt::Display for StopCause {
        fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
            panic!("scope failure projection must not format its original source")
        }
    }
    impl Error for StopCause {}
    impl FailureProjection for StopCause {
        fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
            novarocks_failure_contract::FailureFact::stop_observation(
                novarocks_failure_contract::FailureReason::new(
                    novarocks_failure_contract::domain::CONNECTOR,
                    std::num::NonZeroU16::new(432).unwrap(),
                ),
            )
        }
    }

    #[test]
    fn original_scope_and_cow_boundary_keep_the_original_fact_and_cause() {
        let fact = StopCause.failure_fact();
        let check = OriginalResultCheckError::new(OriginalResultCheckClass::Coverage, StopCause);
        // The check label is a diagnostic location, not a failure classifier.
        assert_eq!(check.failure_fact(), fact);
        let cause = super::super::ConnectorCowBeginCause::OriginalResult(check);
        let error = super::super::ConnectorError::from_domain(
            super::super::ConnectorErrorKind::ResourceExhausted,
            cause,
        );
        assert_eq!(error.failure_fact(), fact);
        let shared = error
            .source()
            .unwrap()
            .downcast_ref::<SharedFailureCause>()
            .unwrap();
        let original = shared
            .downcast_ref::<super::super::ConnectorCowBeginCause>()
            .unwrap();
        assert!(
            original
                .source()
                .unwrap()
                .source()
                .unwrap()
                .is::<StopCause>()
        );
    }
}
