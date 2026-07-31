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

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use bytes::Bytes;

use super::{
    ConnectorError, ConnectorInstanceId, ConnectorRequestContext, ConnectorTableHandle,
    StatisticsDataVersion,
};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ConnectorNamespaceIdentity {
    pub instance_id: ConnectorInstanceId,
    pub namespace: Arc<str>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ConnectorTableIdentity {
    pub instance_id: ConnectorInstanceId,
    pub namespace: Arc<str>,
    pub table: Arc<str>,
}

#[derive(Clone)]
pub struct ConnectorTableMetadata {
    pub identity: ConnectorTableIdentity,
    pub schema: SchemaRef,
    /// Provider-owned schema identity. This remains deliberately distinct
    /// from the data-version pin used by statistics and scan planning.
    pub version: Option<Bytes>,
    /// Opaque data-version resolved together with this table metadata. Core
    /// must pass this exact pin to both scan and statistics consumers rather
    /// than resolving `latest` a second time.
    pub statistics_data_version: Option<StatisticsDataVersion>,
    pub table: ConnectorTableHandle,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorTableResolution {
    StrictBaseTable,
    ProviderReadAlias,
}

#[derive(Clone)]
pub struct ConnectorNamespaceRequest {
    pub namespace: ConnectorNamespaceIdentity,
    pub context: ConnectorRequestContext,
}

#[derive(Clone)]
pub struct ConnectorTableRequest {
    pub table: ConnectorTableIdentity,
    pub resolution: ConnectorTableResolution,
    pub context: ConnectorRequestContext,
}

#[derive(Clone)]
pub struct ConnectorListTablesRequest {
    pub namespace: ConnectorNamespaceIdentity,
    pub context: ConnectorRequestContext,
}

pub trait ConnectorMetadata: Send + Sync {
    fn instance_id(&self) -> &ConnectorInstanceId;

    fn namespace_exists(&self, request: ConnectorNamespaceRequest) -> Result<bool, ConnectorError>;

    fn table_exists(&self, request: ConnectorTableRequest) -> Result<bool, ConnectorError>;

    fn list_tables(
        &self,
        request: ConnectorListTablesRequest,
    ) -> Result<Vec<ConnectorTableIdentity>, ConnectorError>;

    fn load_table(
        &self,
        request: ConnectorTableRequest,
    ) -> Result<ConnectorTableMetadata, ConnectorError>;
}
