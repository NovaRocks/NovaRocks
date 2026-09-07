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

//! Failure vocabulary for the frontend's terminal-outcome report ingress.
//!
//! Protocol contract validation is converted at the Frontend boundary. The
//! refusals a backend can be told about when it reports a participant terminal
//! outcome stay in this role-local vocabulary rather than recreating a Core
//! authority, and [`crate::native::report_server`] maps them to the one gRPC
//! status the caller sees.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryTerminalReportErrorCode {
    InvalidManifest,
    Conflict,
    StaleBackend,
    Capacity,
    Terminated,
    Transport,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryTerminalReportError {
    code: QueryTerminalReportErrorCode,
    detail: String,
}

impl QueryTerminalReportError {
    pub fn new(code: QueryTerminalReportErrorCode, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }

    pub fn invalid_manifest(detail: impl Into<String>) -> Self {
        Self::new(QueryTerminalReportErrorCode::InvalidManifest, detail)
    }

    pub const fn code(&self) -> QueryTerminalReportErrorCode {
        self.code
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl std::fmt::Display for QueryTerminalReportError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:?}: {}", self.code, self.detail)
    }
}

impl std::error::Error for QueryTerminalReportError {}

impl From<novarocks_proto_codec::ProtocolError> for QueryTerminalReportError {
    fn from(error: novarocks_proto_codec::ProtocolError) -> Self {
        Self::invalid_manifest(error.detail())
    }
}
