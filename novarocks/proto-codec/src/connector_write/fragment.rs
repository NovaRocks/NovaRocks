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

//! Validation of the provider-neutral commit-fragment carrier.

use novarocks_proto_models::connector_write as dto;
use novarocks_spi::connector::{ConnectorCodecCategory, ConnectorEncodedPayload};
use prost::Message;

use super::MAX_COMMIT_FRAGMENT_ENCODED_BYTES;
use crate::{FieldPath, ProtocolError, ProtocolErrorKind};

/// A commit fragment whose public envelope is structurally valid and bounded.
///
/// Provider-private artifact semantics are validated only by the exact
/// provider binding selected by the envelope header.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedCommitFragment {
    provider_payload: ConnectorEncodedPayload,
}

impl ValidatedCommitFragment {
    pub fn parse(
        raw: dto::ConnectorCommitFragment,
        path: FieldPath,
    ) -> Result<Self, ProtocolError> {
        let encoded_len = raw.encoded_len();
        if encoded_len > MAX_COMMIT_FRAGMENT_ENCODED_BYTES {
            return Err(ProtocolError::new(
                path,
                ProtocolErrorKind::OutOfRange,
                format!(
                    "commit fragment encodes to {encoded_len} bytes, over the hard limit {MAX_COMMIT_FRAGMENT_ENCODED_BYTES}"
                ),
            ));
        }
        let provider_payload = crate::connector_common::decode_embedded_connector_payload(
            raw.provider_payload.as_ref(),
            ConnectorCodecCategory::CommitFragment,
            MAX_COMMIT_FRAGMENT_ENCODED_BYTES,
            path.field("provider_payload"),
        )?;
        Ok(Self { provider_payload })
    }

    /// Canonical public encoded size used by root-fragment aggregation.
    pub fn encoded_len(&self) -> usize {
        self.as_proto().encoded_len()
    }

    pub const fn provider_payload(&self) -> &ConnectorEncodedPayload {
        &self.provider_payload
    }

    pub fn into_provider_payload(self) -> ConnectorEncodedPayload {
        self.provider_payload
    }

    /// Rebuild the canonical public protobuf without retaining a second copy.
    pub fn as_proto(&self) -> dto::ConnectorCommitFragment {
        dto::ConnectorCommitFragment {
            provider_payload: Some(crate::connector_common::encode_connector_payload_message(
                &self.provider_payload,
            )),
        }
    }

    pub fn into_proto(self) -> dto::ConnectorCommitFragment {
        dto::ConnectorCommitFragment {
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

    fn raw(category: ConnectorCodecCategory) -> dto::ConnectorCommitFragment {
        let catalog = ConnectorInstanceId::try_from_canonical("lake").unwrap();
        let payload = ConnectorEncodedPayload::new(
            ConnectorEnvelopeHeader::new(
                ConnectorProviderId::parse("test").unwrap(),
                CatalogHandle::new(catalog, CatalogVersion::from_bytes([9; 32])),
                category,
                ConnectorCodecRevision::try_new(1).unwrap(),
            ),
            Bytes::from_static(b"artifact"),
        );
        dto::ConnectorCommitFragment {
            provider_payload: Some(crate::connector_common::encode_connector_payload_message(
                &payload,
            )),
        }
    }

    #[test]
    fn retains_only_the_validated_provider_payload() {
        let raw = raw(ConnectorCodecCategory::CommitFragment);
        let validated =
            ValidatedCommitFragment::parse(raw.clone(), FieldPath::root("fragment")).unwrap();
        assert_eq!(validated.provider_payload().payload().as_ref(), b"artifact");
        assert_eq!(validated.encoded_len(), raw.encoded_len());
        assert_eq!(validated.as_proto(), raw);
    }

    #[test]
    fn rejects_a_missing_payload() {
        let error = ValidatedCommitFragment::parse(
            dto::ConnectorCommitFragment {
                provider_payload: None,
            },
            FieldPath::root("fragment"),
        )
        .unwrap_err();
        assert_eq!(error.path().to_string(), "fragment.provider_payload");
    }

    #[test]
    fn rejects_a_payload_for_another_category() {
        let error = ValidatedCommitFragment::parse(
            raw(ConnectorCodecCategory::WriteHandle),
            FieldPath::root("fragment"),
        )
        .unwrap_err();
        assert_eq!(error.path().to_string(), "fragment.provider_payload.header");
    }
}
