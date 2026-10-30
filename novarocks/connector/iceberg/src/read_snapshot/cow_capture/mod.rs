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

//! Prospective request-layout receipts for the actual COW capture pipeline.

use novarocks_spi::connector::{
    ConnectorCowBeginCause, ConnectorError, ConnectorErrorKind, ConnectorOriginalResultScope,
};
#[derive(Debug)]
pub(crate) enum ReadFailure {
    Semantic(String),
    Original(ConnectorCowBeginCause),
}
impl From<String> for ReadFailure {
    fn from(error: String) -> Self {
        Self::Semantic(error)
    }
}
impl std::fmt::Display for ReadFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Semantic(message) => f.write_str(message),
            Self::Original(_) => f.write_str("original COW capture check failed"),
        }
    }
}
impl ReadFailure {
    pub(crate) fn into_cause(self) -> ConnectorCowBeginCause {
        match self {
            Self::Original(cause) => cause,
            Self::Semantic(message) => {
                ConnectorError::new(ConnectorErrorKind::InvalidRequest, message).into()
            }
        }
    }
}

/// Borrows the original operation; identity does not grant object-graph bytes.
pub(crate) struct CaptureScope<'a> {
    pub(crate) scope: &'a ConnectorOriginalResultScope,
}
impl<'a> CaptureScope<'a> {
    pub(crate) fn new(scope: &'a ConnectorOriginalResultScope) -> Result<Self, ReadFailure> {
        let value = Self { scope };
        value.active()?;
        Ok(value)
    }
    pub(crate) fn active(&self) -> Result<(), ReadFailure> {
        self.scope
            .check_active()
            .map_err(|e| ReadFailure::Original(e.into()))
    }
}
pub(crate) fn active(scope: Option<&CaptureScope<'_>>) -> Result<(), ReadFailure> {
    if let Some(scope) = scope {
        scope.active()?;
    }
    Ok(())
}
