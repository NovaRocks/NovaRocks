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

use crate::common::types::UniqueId;
use crate::query_execution::lifecycle::QueryExecutionId;
use crate::runtime::query_context::QueryId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeFragmentCancelRequest {
    query_id: QueryId,
    fragment_instance_ids: Vec<UniqueId>,
    reason: String,
}

impl NativeFragmentCancelRequest {
    pub fn new(
        query_id: QueryId,
        fragment_instance_ids: Vec<UniqueId>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            query_id,
            fragment_instance_ids,
            reason: reason.into(),
        }
    }

    pub const fn query_id(&self) -> QueryId {
        self.query_id
    }

    pub fn fragment_instance_ids(&self) -> &[UniqueId] {
        &self.fragment_instance_ids
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeFragmentIngressError {
    message: String,
}

impl NativeFragmentIngressError {
    pub fn new(error: impl fmt::Display) -> Self {
        Self {
            message: error.to_string(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for NativeFragmentIngressError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for NativeFragmentIngressError {}

pub trait NativeFragmentIngress: Send + Sync + 'static {
    fn ensure_connector_execution_binding(
        &self,
        _execution_id: QueryExecutionId,
        _declaration: novarocks_spi::connector::ConnectorExecutionDeclaration,
        _context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<(), NativeFragmentIngressError> {
        Err(NativeFragmentIngressError::new(
            "connector binding ingress is not configured",
        ))
    }

    fn retire_connector_execution_binding(
        &self,
        _key: novarocks_spi::connector::ConnectorExecutionBindingKey,
    ) -> Result<(), NativeFragmentIngressError> {
        Err(NativeFragmentIngressError::new(
            "connector binding ingress is not configured",
        ))
    }

    fn cancel(
        &self,
        request: NativeFragmentCancelRequest,
    ) -> Result<(), NativeFragmentIngressError>;
}
