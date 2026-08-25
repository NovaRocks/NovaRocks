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

//! Frontend-local lifecycle failure vocabulary.
//!
//! Protocol contract validation is converted at the Frontend boundary. Query
//! admission, coordinator state, and native report transport failures stay in
//! this role-local vocabulary rather than recreating a Core authority.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryLifecycleErrorCode {
    InvalidManifest,
    Conflict,
    StaleBackend,
    Capacity,
    Terminated,
    Transport,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryLifecycleError {
    code: QueryLifecycleErrorCode,
    detail: String,
}

impl QueryLifecycleError {
    pub fn new(code: QueryLifecycleErrorCode, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }

    pub fn invalid_manifest(detail: impl Into<String>) -> Self {
        Self::new(QueryLifecycleErrorCode::InvalidManifest, detail)
    }

    pub const fn code(&self) -> QueryLifecycleErrorCode {
        self.code
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl std::fmt::Display for QueryLifecycleError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:?}: {}", self.code, self.detail)
    }
}

impl std::error::Error for QueryLifecycleError {}

impl From<novarocks_proto::lifecycle::ContractError> for QueryLifecycleError {
    fn from(error: novarocks_proto::lifecycle::ContractError) -> Self {
        Self::invalid_manifest(error.detail())
    }
}
