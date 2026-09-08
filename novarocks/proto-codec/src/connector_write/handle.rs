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

//! Validation of the provider-neutral writer-handle carrier.

use novarocks_proto_models::connector_write as dto;
use novarocks_spi::connector::{ConnectorCodecCategory, ConnectorEncodedPayload};
use prost::Message;

use super::MAX_WRITER_HANDLE_ENCODED_BYTES;
use crate::{FieldPath, ProtocolError, ProtocolErrorKind};

/// A writer handle whose public envelope is structurally valid and bounded.
///
/// The public codec deliberately retains only the provider-neutral envelope.
/// The installed provider binding validates provider, catalog generation and
/// codec revision before interpreting the private bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedWriterHandle {
    provider_payload: ConnectorEncodedPayload,
}

impl ValidatedWriterHandle {
    pub fn parse(raw: dto::ConnectorWriterHandle, path: FieldPath) -> Result<Self, ProtocolError> {
        let encoded_len = raw.encoded_len();
        if encoded_len > MAX_WRITER_HANDLE_ENCODED_BYTES {
            return Err(ProtocolError::new(
                path,
                ProtocolErrorKind::OutOfRange,
                format!(
                    "writer handle encodes to {encoded_len} bytes, over the hard limit {MAX_WRITER_HANDLE_ENCODED_BYTES}"
                ),
            ));
        }
        let provider_payload = crate::connector_common::decode_embedded_connector_payload(
            raw.provider_payload.as_ref(),
            ConnectorCodecCategory::WriteHandle,
            MAX_WRITER_HANDLE_ENCODED_BYTES,
            path.field("provider_payload"),
        )?;
        Ok(Self { provider_payload })
    }

    pub const fn provider_payload(&self) -> &ConnectorEncodedPayload {
        &self.provider_payload
    }

    pub fn into_provider_payload(self) -> ConnectorEncodedPayload {
        self.provider_payload
    }

    /// Rebuild the canonical public protobuf without retaining a second copy.
    pub fn as_proto(&self) -> dto::ConnectorWriterHandle {
        dto::ConnectorWriterHandle {
            provider_payload: Some(crate::connector_common::encode_connector_payload_message(
                &self.provider_payload,
            )),
        }
    }

    pub fn into_proto(self) -> dto::ConnectorWriterHandle {
        dto::ConnectorWriterHandle {
            provider_payload: Some(crate::connector_common::encode_connector_payload_message(
                &self.provider_payload,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorCodecRevision, ConnectorEnvelopeHeader,
        ConnectorInstanceId, ConnectorProviderId,
    };

    use super::*;

    fn raw(category: ConnectorCodecCategory) -> dto::ConnectorWriterHandle {
        let catalog = ConnectorInstanceId::try_from_canonical("lake").unwrap();
        let payload = ConnectorEncodedPayload::new(
            ConnectorEnvelopeHeader::new(
                ConnectorProviderId::parse("test").unwrap(),
                CatalogHandle::new(catalog, CatalogVersion::from_bytes([7; 32])),
                category,
                ConnectorCodecRevision::try_new(1).unwrap(),
            ),
            Bytes::from_static(b"private"),
        );
        dto::ConnectorWriterHandle {
            provider_payload: Some(crate::connector_common::encode_connector_payload_message(
                &payload,
            )),
        }
    }

    #[test]
    fn retains_only_the_validated_provider_payload() {
        let raw = raw(ConnectorCodecCategory::WriteHandle);
        let validated =
            ValidatedWriterHandle::parse(raw.clone(), FieldPath::root("writer_handle")).unwrap();
        assert_eq!(validated.provider_payload().payload().as_ref(), b"private");
        assert_eq!(validated.as_proto(), raw);
    }

    #[test]
    fn rejects_a_missing_payload() {
        let error = ValidatedWriterHandle::parse(
            dto::ConnectorWriterHandle {
                provider_payload: None,
            },
            FieldPath::root("writer_handle"),
        )
        .unwrap_err();
        assert_eq!(error.path().to_string(), "writer_handle.provider_payload");
    }

    #[test]
    fn rejects_a_payload_for_another_category() {
        let error = ValidatedWriterHandle::parse(
            raw(ConnectorCodecCategory::CommitFragment),
            FieldPath::root("writer_handle"),
        )
        .unwrap_err();
        assert_eq!(
            error.path().to_string(),
            "writer_handle.provider_payload.header"
        );
    }
}
