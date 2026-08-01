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

//! Backend-local ingress contracts for native fragment control.

use std::fmt;

use novarocks::query_execution::lifecycle::QueryExecutionId;
use novarocks_spi::connector::{
    ConnectorExecutionBindingKey, ConnectorExecutionDeclaration, ConnectorRequestContext,
};
use novarocks_types::QueryId;
use novarocks_types::UniqueId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NativeFragmentCancelRequest {
    query_id: QueryId,
    fragment_instance_ids: Vec<UniqueId>,
    reason: String,
}

impl NativeFragmentCancelRequest {
    pub(crate) fn new(
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
    pub(crate) const fn query_id(&self) -> QueryId {
        self.query_id
    }
    pub(crate) fn fragment_instance_ids(&self) -> &[UniqueId] {
        &self.fragment_instance_ids
    }
    pub(crate) fn reason(&self) -> &str {
        &self.reason
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NativeFragmentIngressError {
    message: String,
}

impl NativeFragmentIngressError {
    pub(crate) fn new(error: impl fmt::Display) -> Self {
        Self {
            message: error.to_string(),
        }
    }
}
impl fmt::Display for NativeFragmentIngressError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for NativeFragmentIngressError {}

pub(crate) trait NativeFragmentIngress: Send + Sync + 'static {
    fn ensure_connector_execution_binding(
        &self,
        execution_id: QueryExecutionId,
        declaration: ConnectorExecutionDeclaration,
        context: ConnectorRequestContext,
    ) -> Result<(), NativeFragmentIngressError>;
    fn retire_connector_execution_binding(
        &self,
        key: ConnectorExecutionBindingKey,
    ) -> Result<(), NativeFragmentIngressError>;
    fn cancel(
        &self,
        request: NativeFragmentCancelRequest,
    ) -> Result<(), NativeFragmentIngressError>;
}
