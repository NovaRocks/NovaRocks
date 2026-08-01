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
use std::time::{Duration, Instant};

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorCancellation, ConnectorError, ConnectorErrorKind, ConnectorExecutionBindingKey,
    ConnectorExecutionDeclaration, ConnectorInstanceDescriptor, ConnectorInstanceId,
    ConnectorInstanceIncarnation, ConnectorProviderId, ConnectorRequestContext,
    MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
};

use crate::proto::novarocks::{
    EnsureConnectorExecutionBindingRequest, RetireConnectorExecutionBindingRequest,
};
use crate::protocol::decode_native_query_execution_id;
use crate::query_execution::lifecycle::QueryExecutionId;

const CONNECTOR_BINDING_CONTEXT_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) fn decode_ensure_request(
    request: EnsureConnectorExecutionBindingRequest,
) -> Result<(QueryExecutionId, ConnectorExecutionDeclaration), ConnectorError> {
    let execution_id = request.execution_id.as_ref().ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector execution binding request is missing execution_id",
        )
    })?;
    let execution_id = decode_native_query_execution_id(execution_id).map_err(|error| {
        ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
    })?;
    let provider_id = ConnectorProviderId::parse(&request.provider_id)?;
    let instance_id = ConnectorInstanceId::parse(&request.instance_id)?;
    let incarnation = decode_incarnation(&request.incarnation)?;
    let declaration = ConnectorExecutionDeclaration::try_new(
        ConnectorInstanceDescriptor {
            provider_id,
            instance_id,
        },
        incarnation,
        Bytes::from(request.declaration_payload),
    )?;
    Ok((execution_id, declaration))
}

pub(crate) fn decode_retire_request(
    request: RetireConnectorExecutionBindingRequest,
) -> Result<ConnectorExecutionBindingKey, ConnectorError> {
    Ok(ConnectorExecutionBindingKey {
        instance_id: ConnectorInstanceId::parse(&request.instance_id)?,
        incarnation: decode_incarnation(&request.incarnation)?,
    })
}

pub(crate) fn install_request_context() -> Result<ConnectorRequestContext, ConnectorError> {
    ConnectorRequestContext::try_new(
        Instant::now() + CONNECTOR_BINDING_CONTEXT_TIMEOUT,
        Arc::new(NotCancelled),
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
}

fn decode_incarnation(bytes: &[u8]) -> Result<ConnectorInstanceIncarnation, ConnectorError> {
    let bytes: [u8; 16] = bytes.try_into().map_err(|_| {
        ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector instance incarnation must contain exactly 16 bytes",
        )
    })?;
    Ok(ConnectorInstanceIncarnation::from_bytes(bytes))
}

struct NotCancelled;

impl ConnectorCancellation for NotCancelled {
    fn is_cancelled(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ensure_request_rejects_invalid_incarnation_length() {
        let error = decode_ensure_request(EnsureConnectorExecutionBindingRequest {
            execution_id: Some(crate::proto::novarocks::QueryExecutionId {
                query_id: Some(crate::proto::common::UniqueId { hi: 7, lo: 9 }),
                attempt_id: 1,
            }),
            provider_id: "iceberg".to_string(),
            instance_id: "catalog.analytics".to_string(),
            incarnation: vec![7; 15],
            declaration_payload: Vec::new(),
        })
        .expect_err("short incarnation must be rejected");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }
}
