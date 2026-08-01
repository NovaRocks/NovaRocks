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

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind, ConnectorInstanceId};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::STARROCKS_CONTRACT_VERSION;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StarRocksReadPolicy {
    Rpc,
    Direct,
    Auto,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StarRocksTopology {
    SharedData,
    SharedNothing,
    Unknown,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StarRocksRpcTransport {
    BrpcChunk,
    ArrowFlight,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StarRocksSelectedStrategy {
    Rpc { transport: StarRocksRpcTransport },
    SharedDataDirect,
}

#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StarRocksLocalBindingRef(Arc<str>);

impl StarRocksLocalBindingRef {
    pub fn parse(value: impl AsRef<str>) -> Result<Self, ConnectorError> {
        let value = value.as_ref();
        if value.is_empty() || value.len() > 256 || !value.is_ascii() {
            return Err(invalid("invalid StarRocks local binding reference"));
        }
        Ok(Self(Arc::from(value)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for StarRocksLocalBindingRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("StarRocksLocalBindingRef")
            .field(&self.0)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct StarRocksConnectorConfig {
    pub instance_id: ConnectorInstanceId,
    pub read_policy: StarRocksReadPolicy,
    pub rpc_transport: StarRocksRpcTransport,
    pub local_binding: StarRocksLocalBindingRef,
}

impl StarRocksConnectorConfig {
    pub fn new(
        instance_id: ConnectorInstanceId,
        read_policy: StarRocksReadPolicy,
        rpc_transport: StarRocksRpcTransport,
        local_binding: StarRocksLocalBindingRef,
    ) -> Self {
        Self {
            instance_id,
            read_policy,
            rpc_transport,
            local_binding,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StarRocksCapabilitySnapshot {
    pub api_contract_version: u16,
    pub rpc_transports: BTreeSet<StarRocksRpcTransport>,
    pub rpc_ready: bool,
    pub direct_contract_version: Option<u16>,
    pub direct_ready: bool,
}

impl StarRocksCapabilitySnapshot {
    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.api_contract_version != STARROCKS_CONTRACT_VERSION {
            return Err(unsupported("unsupported StarRocks API contract version"));
        }
        if self
            .direct_contract_version
            .is_some_and(|version| version != STARROCKS_CONTRACT_VERSION)
        {
            return Err(unsupported(
                "unsupported StarRocks direct-read contract version",
            ));
        }
        Ok(())
    }

    pub fn rpc_is_ready(&self, transport: StarRocksRpcTransport) -> Result<(), ConnectorError> {
        self.validate()?;
        if !self.rpc_transports.contains(&transport) {
            return Err(unsupported(
                "requested StarRocks RPC transport is not supported",
            ));
        }
        if !self.rpc_ready {
            return Err(unavailable("StarRocks RPC planning is unavailable"));
        }
        Ok(())
    }

    pub fn direct_is_ready(&self) -> Result<(), ConnectorError> {
        self.validate()?;
        if self.direct_contract_version.is_none() {
            return Err(unsupported(
                "StarRocks direct-read contract is not supported",
            ));
        }
        if !self.direct_ready {
            return Err(unavailable("StarRocks direct planning is unavailable"));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct StarRocksResolvedTable {
    pub namespace: Arc<str>,
    pub table: Arc<str>,
    pub schema: SchemaRef,
    pub topology: StarRocksTopology,
    pub schema_version: Bytes,
    pub data_version: Bytes,
    pub capability: StarRocksCapabilitySnapshot,
}

impl StarRocksResolvedTable {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        namespace: impl Into<Arc<str>>,
        table: impl Into<Arc<str>>,
        schema: SchemaRef,
        topology: StarRocksTopology,
        schema_version: Bytes,
        data_version: Bytes,
        capability: StarRocksCapabilitySnapshot,
    ) -> Result<Self, ConnectorError> {
        let namespace = namespace.into();
        let table = table.into();
        if namespace.is_empty() || table.is_empty() {
            return Err(invalid("StarRocks namespace and table must not be empty"));
        }
        if schema_version.is_empty() || data_version.is_empty() {
            return Err(invalid("StarRocks table versions must not be empty"));
        }
        capability.validate()?;
        Ok(Self {
            namespace,
            table,
            schema,
            topology,
            schema_version,
            data_version,
            capability,
        })
    }
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct StarRocksReadAttemptId(Uuid);

impl StarRocksReadAttemptId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
    pub fn from_uuid(value: Uuid) -> Result<Self, ConnectorError> {
        if value.is_nil() {
            return Err(invalid("StarRocks read attempt ID must not be nil"));
        }
        Ok(Self(value))
    }
    pub fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl Default for StarRocksReadAttemptId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for StarRocksReadAttemptId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("StarRocksReadAttemptId")
            .field(&self.0)
            .finish()
    }
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct StarRocksFreezeDigest(pub [u8; 32]);

impl fmt::Debug for StarRocksFreezeDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("StarRocksFreezeDigest")
            .field(&"<redacted>")
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct StarRocksRpcOpaquePayload(Bytes);

impl StarRocksRpcOpaquePayload {
    pub fn new(bytes: Bytes) -> Result<Self, ConnectorError> {
        if bytes.is_empty() {
            return Err(invalid("StarRocks RPC split payload must not be empty"));
        }
        Ok(Self(bytes))
    }
    pub fn as_bytes(&self) -> &Bytes {
        &self.0
    }
}

impl fmt::Debug for StarRocksRpcOpaquePayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StarRocksRpcOpaquePayload")
            .field("len", &self.0.len())
            .finish()
    }
}

#[derive(Clone, Debug)]
pub enum StarRocksStrategySplitPayload {
    Rpc(StarRocksRpcOpaquePayload),
    SharedDataDirect(Bytes),
}

#[derive(Clone, Debug)]
pub struct StarRocksStrategySplit {
    pub split_id: Arc<str>,
    pub payload: StarRocksStrategySplitPayload,
    pub estimated_bytes: Option<u64>,
}

#[derive(Clone)]
pub struct StarRocksSplitPlanningInput {
    pub owner: ConnectorInstanceId,
    pub incarnation: novarocks_spi::connector::ConnectorInstanceIncarnation,
    pub attempt: StarRocksReadAttemptId,
    pub freeze: StarRocksFreezeDigest,
    pub strategy: StarRocksSelectedStrategy,
    pub topology: StarRocksTopology,
    pub namespace: Arc<str>,
    pub table: Arc<str>,
    pub schema_version: Bytes,
    pub data_version: Bytes,
    pub output_schema: SchemaRef,
    pub projection: Vec<usize>,
    pub limit: Option<u64>,
}

impl fmt::Debug for StarRocksSplitPlanningInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StarRocksSplitPlanningInput")
            .field("attempt", &self.attempt)
            .field("strategy", &self.strategy)
            .field("namespace", &self.namespace)
            .field("table", &self.table)
            .field("projection", &self.projection)
            .field("limit", &self.limit)
            .finish_non_exhaustive()
    }
}

pub fn select_read_strategy(
    policy: StarRocksReadPolicy,
    topology: StarRocksTopology,
    capability: &StarRocksCapabilitySnapshot,
    rpc_transport: StarRocksRpcTransport,
) -> Result<StarRocksSelectedStrategy, ConnectorError> {
    match policy {
        StarRocksReadPolicy::Rpc => {
            capability.rpc_is_ready(rpc_transport)?;
            Ok(StarRocksSelectedStrategy::Rpc {
                transport: rpc_transport,
            })
        }
        StarRocksReadPolicy::Direct => {
            if topology != StarRocksTopology::SharedData {
                return Err(unsupported(
                    "StarRocks direct read requires shared-data topology",
                ));
            }
            capability.direct_is_ready()?;
            Ok(StarRocksSelectedStrategy::SharedDataDirect)
        }
        StarRocksReadPolicy::Auto => {
            if topology == StarRocksTopology::SharedData && capability.direct_is_ready().is_ok() {
                return Ok(StarRocksSelectedStrategy::SharedDataDirect);
            }
            capability.rpc_is_ready(rpc_transport)?;
            Ok(StarRocksSelectedStrategy::Rpc {
                transport: rpc_transport,
            })
        }
    }
}

pub(crate) fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}
pub(crate) fn unsupported(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unsupported, message.into())
}
pub(crate) fn unavailable(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn capability(direct_ready: bool, rpc_ready: bool) -> StarRocksCapabilitySnapshot {
        StarRocksCapabilitySnapshot {
            api_contract_version: 1,
            rpc_transports: [StarRocksRpcTransport::BrpcChunk].into(),
            rpc_ready,
            direct_contract_version: Some(1),
            direct_ready,
        }
    }

    #[test]
    fn mode_selection_preserves_the_direct_contract() {
        assert_eq!(
            select_read_strategy(
                StarRocksReadPolicy::Auto,
                StarRocksTopology::SharedData,
                &capability(true, true),
                StarRocksRpcTransport::BrpcChunk
            )
            .unwrap(),
            StarRocksSelectedStrategy::SharedDataDirect
        );
        assert_eq!(
            select_read_strategy(
                StarRocksReadPolicy::Auto,
                StarRocksTopology::SharedNothing,
                &capability(true, true),
                StarRocksRpcTransport::BrpcChunk
            )
            .unwrap(),
            StarRocksSelectedStrategy::Rpc {
                transport: StarRocksRpcTransport::BrpcChunk
            }
        );
        assert_eq!(
            select_read_strategy(
                StarRocksReadPolicy::Direct,
                StarRocksTopology::Unknown,
                &capability(true, true),
                StarRocksRpcTransport::BrpcChunk
            )
            .unwrap_err()
            .kind(),
            ConnectorErrorKind::Unsupported
        );
    }

    #[test]
    fn mode_selection_covers_explicit_and_auto_readiness_cases() {
        assert_eq!(
            select_read_strategy(
                StarRocksReadPolicy::Rpc,
                StarRocksTopology::Unknown,
                &capability(false, true),
                StarRocksRpcTransport::BrpcChunk,
            )
            .unwrap(),
            StarRocksSelectedStrategy::Rpc {
                transport: StarRocksRpcTransport::BrpcChunk
            }
        );
        assert_eq!(
            select_read_strategy(
                StarRocksReadPolicy::Direct,
                StarRocksTopology::SharedData,
                &capability(true, false),
                StarRocksRpcTransport::BrpcChunk,
            )
            .unwrap(),
            StarRocksSelectedStrategy::SharedDataDirect
        );
        assert_eq!(
            select_read_strategy(
                StarRocksReadPolicy::Direct,
                StarRocksTopology::SharedData,
                &capability(false, true),
                StarRocksRpcTransport::BrpcChunk,
            )
            .unwrap_err()
            .kind(),
            ConnectorErrorKind::Unavailable
        );
        assert_eq!(
            select_read_strategy(
                StarRocksReadPolicy::Auto,
                StarRocksTopology::SharedData,
                &capability(false, true),
                StarRocksRpcTransport::BrpcChunk,
            )
            .unwrap(),
            StarRocksSelectedStrategy::Rpc {
                transport: StarRocksRpcTransport::BrpcChunk
            }
        );
        assert_eq!(
            select_read_strategy(
                StarRocksReadPolicy::Auto,
                StarRocksTopology::SharedNothing,
                &capability(false, false),
                StarRocksRpcTransport::BrpcChunk,
            )
            .unwrap_err()
            .kind(),
            ConnectorErrorKind::Unavailable
        );
    }

    #[test]
    fn rpc_payload_debug_never_exposes_its_contents() {
        let payload = StarRocksRpcOpaquePayload::new(Bytes::from_static(b"query-token")).unwrap();
        assert!(!format!("{payload:?}").contains("query-token"));
    }
}
