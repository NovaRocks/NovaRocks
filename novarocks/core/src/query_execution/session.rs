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

//! Frontend-owned SQL session boundary consumed by the MySQL wire adapter.
//! Design: ADR-0012 (docs/adr/ADR-0012-frontend-query-session-router.md)
//!
//! The core server owns protocol framing only.  Authentication success opens a
//! frontend session through this port; all request admission, routing and
//! cancellation identity remain with that session.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;

use crate::engine::StatementResult;
use crate::query_execution::cancellation::QueryCancellationReason;
use novarocks_execution::runtime::query_options::QueryOptions;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuerySessionOpenRequest {
    connection_id: u32,
    principal: Arc<str>,
}

/// Connection-local execution settings owned by the frontend session.
///
/// This is intentionally a neutral value object: the runtime representation
/// stays private and is materialized only when core compilation needs it.
#[derive(Clone, Debug, PartialEq)]
pub struct SessionExecutionSettings {
    query_timeout_secs: Option<u64>,
    group_concat_max_len: i64,
    pipeline_dop: Option<i32>,
    runtime_filter_scan_wait_time_ms: Option<i64>,
    runtime_filter_wait_timeout_ms: Option<i32>,
}

impl Default for SessionExecutionSettings {
    fn default() -> Self {
        Self {
            query_timeout_secs: None,
            group_concat_max_len: 1024,
            pipeline_dop: None,
            runtime_filter_scan_wait_time_ms: None,
            runtime_filter_wait_timeout_ms: None,
        }
    }
}

impl SessionExecutionSettings {
    pub fn query_timeout_secs(&self) -> Option<u64> {
        self.query_timeout_secs
    }

    pub fn set_query_timeout_secs(&mut self, seconds: u64) {
        self.query_timeout_secs = (seconds > 0).then_some(seconds);
    }

    /// Keep the session value verbatim; aggregate lowering clamps it to the
    /// supported minimum before execution.
    pub fn set_group_concat_max_len(&mut self, value: i64) {
        self.group_concat_max_len = value;
    }

    pub fn set_pipeline_dop(&mut self, value: i32) {
        self.pipeline_dop = (value > 0).then_some(value);
    }

    pub fn set_runtime_filter_scan_wait_time_ms(
        &mut self,
        value: i64,
    ) -> Result<(), QueryServiceError> {
        if value < 0 {
            return Err(QueryServiceError::new(
                QueryServiceErrorKind::InvalidValue,
                "runtime_filter_scan_wait_time must be non-negative",
            ));
        }
        self.runtime_filter_scan_wait_time_ms = Some(value);
        Ok(())
    }

    pub fn set_runtime_filter_wait_timeout_ms(
        &mut self,
        value: i32,
    ) -> Result<(), QueryServiceError> {
        if value < 0 {
            return Err(QueryServiceError::new(
                QueryServiceErrorKind::InvalidValue,
                "global_runtime_filter_wait_timeout must be non-negative",
            ));
        }
        self.runtime_filter_wait_timeout_ms = Some(value);
        Ok(())
    }

    pub fn query_options(&self) -> QueryOptions {
        QueryOptions {
            group_concat_max_len: Some(self.group_concat_max_len),
            query_timeout: self
                .query_timeout_secs
                .and_then(|value| value.try_into().ok()),
            pipeline_dop: self.pipeline_dop,
            runtime_filter_scan_wait_time_ms: self.runtime_filter_scan_wait_time_ms,
            runtime_filter_wait_timeout_ms: self.runtime_filter_wait_timeout_ms,
            ..Default::default()
        }
    }
}

impl QuerySessionOpenRequest {
    pub fn new(connection_id: u32, principal: impl Into<Arc<str>>) -> Self {
        Self {
            connection_id,
            principal: principal.into(),
        }
    }

    pub const fn connection_id(&self) -> u32 {
        self.connection_id
    }

    pub fn principal(&self) -> &str {
        &self.principal
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
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryServiceError {
    kind: QueryServiceErrorKind,
    message: String,
}

impl QueryServiceError {
    pub fn new(kind: QueryServiceErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub const fn kind(&self) -> QueryServiceErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for QueryServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for QueryServiceError {}

#[async_trait]
pub trait QuerySession: Send + Sync + 'static {
    async fn init_database(&self, schema: &str) -> Result<(), QueryServiceError>;

    async fn execute_batch(&self, sql: &str) -> Result<StatementResult, QueryServiceError>;

    fn cancel_current(&self, reason: QueryCancellationReason);

    fn close(&self);
}

pub trait QuerySessionFactory: Send + Sync + 'static {
    fn open_session(
        &self,
        request: QuerySessionOpenRequest,
    ) -> Result<Arc<dyn QuerySession>, QueryServiceError>;

    fn cancel_all(&self, reason: QueryCancellationReason);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_request_keeps_connection_identity_private_but_readable() {
        let request = QuerySessionOpenRequest::new(42, "alice");
        assert_eq!(request.connection_id(), 42);
        assert_eq!(request.principal(), "alice");
    }

    #[test]
    fn typed_error_preserves_kind_and_message() {
        let error = QueryServiceError::new(QueryServiceErrorKind::Timeout, "deadline elapsed");
        assert_eq!(error.kind(), QueryServiceErrorKind::Timeout);
        assert_eq!(error.message(), "deadline elapsed");
    }

    #[test]
    fn execution_settings_preserve_group_concat_value_before_materializing_options() {
        let mut settings = SessionExecutionSettings::default();
        settings.set_query_timeout_secs(17);
        settings.set_pipeline_dop(4);
        settings
            .set_runtime_filter_scan_wait_time_ms(0)
            .expect("zero is valid");
        assert_eq!(settings.query_timeout_secs(), Some(17));
        settings.set_group_concat_max_len(-1);
        assert_eq!(settings.query_options().group_concat_max_len(), Some(-1));
        let _opaque_runtime_options = settings.query_options();
    }
}
