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
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use crate::connector::{ConnectorError, ConnectorErrorKind};

const MAX_DETAIL_BYTES: usize = 512;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ConnectorMaterializationErrorClass {
    InvalidDefinition,
    Authentication,
    Unavailable,
    Timeout,
    ResourceExhausted,
    Internal,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ConnectorMaterializationRetryDisposition {
    Transient,
    UntilDefinitionChanges,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorMaterializationError {
    class: ConnectorMaterializationErrorClass,
    disposition: ConnectorMaterializationRetryDisposition,
    detail: Arc<str>,
}

impl ConnectorMaterializationError {
    pub fn new(
        class: ConnectorMaterializationErrorClass,
        disposition: ConnectorMaterializationRetryDisposition,
        detail: impl AsRef<str>,
    ) -> Self {
        Self {
            class,
            disposition,
            detail: Arc::from(redact_and_bound(detail.as_ref())),
        }
    }

    pub const fn class(&self) -> ConnectorMaterializationErrorClass {
        self.class
    }

    pub const fn disposition(&self) -> ConnectorMaterializationRetryDisposition {
        self.disposition
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl From<ConnectorError> for ConnectorMaterializationError {
    fn from(error: ConnectorError) -> Self {
        let (class, disposition) = match error.kind() {
            ConnectorErrorKind::PermissionDenied => (
                ConnectorMaterializationErrorClass::Authentication,
                ConnectorMaterializationRetryDisposition::UntilDefinitionChanges,
            ),
            ConnectorErrorKind::Unavailable => (
                ConnectorMaterializationErrorClass::Unavailable,
                ConnectorMaterializationRetryDisposition::Transient,
            ),
            ConnectorErrorKind::DeadlineExceeded => (
                ConnectorMaterializationErrorClass::Timeout,
                ConnectorMaterializationRetryDisposition::Transient,
            ),
            ConnectorErrorKind::ResourceExhausted => (
                ConnectorMaterializationErrorClass::ResourceExhausted,
                ConnectorMaterializationRetryDisposition::Transient,
            ),
            ConnectorErrorKind::Cancelled => (
                ConnectorMaterializationErrorClass::Cancelled,
                ConnectorMaterializationRetryDisposition::Transient,
            ),
            ConnectorErrorKind::Internal | ConnectorErrorKind::CorruptData => (
                ConnectorMaterializationErrorClass::Internal,
                ConnectorMaterializationRetryDisposition::Transient,
            ),
            ConnectorErrorKind::InvalidRequest
            | ConnectorErrorKind::NotFound
            | ConnectorErrorKind::Unsupported => (
                ConnectorMaterializationErrorClass::InvalidDefinition,
                ConnectorMaterializationRetryDisposition::UntilDefinitionChanges,
            ),
        };
        Self::new(class, disposition, error.message())
    }
}

impl fmt::Display for ConnectorMaterializationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: {}", self.class, self.detail)
    }
}

impl std::error::Error for ConnectorMaterializationError {}

/// Per-attempt cancellation and deadline facts. The FE scheduler owns when it
/// is cancelled; providers must check the context before and after I/O.
#[derive(Clone, Debug)]
pub struct MaterializationContext {
    deadline: Instant,
    cancelled: Arc<AtomicBool>,
}

impl MaterializationContext {
    pub fn new(deadline: Instant) -> Self {
        Self {
            deadline,
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    pub const fn deadline(&self) -> Instant {
        self.deadline
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub fn check_active(&self) -> Result<(), ConnectorMaterializationError> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(ConnectorMaterializationError::new(
                ConnectorMaterializationErrorClass::Cancelled,
                ConnectorMaterializationRetryDisposition::Transient,
                "connector materialization was cancelled",
            ));
        }
        if Instant::now() >= self.deadline {
            return Err(ConnectorMaterializationError::new(
                ConnectorMaterializationErrorClass::Timeout,
                ConnectorMaterializationRetryDisposition::Transient,
                "connector materialization deadline elapsed",
            ));
        }
        Ok(())
    }
}

fn redact_and_bound(detail: &str) -> String {
    let mut result = detail.replace("password=", "password=[REDACTED]");
    result = result.replace("secret=", "secret=[REDACTED]");
    result = result.replace("token=", "token=[REDACTED]");
    if result.len() > MAX_DETAIL_BYTES {
        result.truncate(MAX_DETAIL_BYTES);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connector_error_mapping_has_a_finite_retry_disposition() {
        let error = ConnectorMaterializationError::from(ConnectorError::new(
            ConnectorErrorKind::Unavailable,
            "temporary remote failure",
        ));
        assert_eq!(
            error.class(),
            ConnectorMaterializationErrorClass::Unavailable
        );
        assert_eq!(
            error.disposition(),
            ConnectorMaterializationRetryDisposition::Transient
        );
    }

    #[test]
    fn materialization_context_fails_closed_after_cancellation() {
        let context =
            MaterializationContext::new(Instant::now() + std::time::Duration::from_secs(1));
        context.cancel();
        assert_eq!(
            context.check_active().unwrap_err().class(),
            ConnectorMaterializationErrorClass::Cancelled
        );
    }
}
