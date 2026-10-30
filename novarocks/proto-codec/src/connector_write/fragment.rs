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
use novarocks_spi::connector::{
    ConnectorCodecCategory, ConnectorEncodedPayload, ConnectorPayloadRetentionGuard,
};
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
    encoded_len: usize,
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
        let provider_payload = crate::connector_common::decode_embedded_owned_connector_payload(
            raw.provider_payload,
            ConnectorCodecCategory::CommitFragment,
            MAX_COMMIT_FRAGMENT_ENCODED_BYTES,
            path.field("provider_payload"),
        )?;
        Ok(Self {
            provider_payload,
            encoded_len,
        })
    }

    /// Canonical public encoded size used by root-fragment aggregation.
    pub fn encoded_len(&self) -> usize {
        self.encoded_len
    }

    pub const fn provider_payload(&self) -> &ConnectorEncodedPayload {
        &self.provider_payload
    }

    pub fn into_provider_payload(self) -> ConnectorEncodedPayload {
        self.provider_payload
    }

    /// Keep the exact outer decode buffer receipt with its actual bytes.
    /// A Provider that borrows/clones these Bytes carries the same receipt
    /// through its last payload alias; no other collector buffers are pinned.
    pub fn retain_provider_payload(mut self, receipt: ConnectorPayloadRetentionGuard) -> Self {
        let (header, payload) = self.provider_payload.into_parts();
        self.provider_payload = ConnectorEncodedPayload::new(
            header,
            bytes::Bytes::from_owner(RetainedProviderPayload {
                payload,
                _receipt: receipt,
            }),
        );
        self
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

// Actual payload destruction precedes refunding its original byte receipt.
struct RetainedProviderPayload {
    payload: bytes::Bytes,
    _receipt: ConnectorPayloadRetentionGuard,
}
impl AsRef<[u8]> for RetainedProviderPayload {
    fn as_ref(&self) -> &[u8] {
        &self.payload
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
    fn owned_parse_moves_payload_backing_and_cached_size_without_a_second_copy() {
        let mut raw = raw(ConnectorCodecCategory::CommitFragment);
        raw.provider_payload.as_mut().unwrap().payload = vec![7; 8192];
        let pointer = raw.provider_payload.as_ref().unwrap().payload.as_ptr();
        let encoded_len = raw.encoded_len();
        let validated = ValidatedCommitFragment::parse(raw, FieldPath::root("fragment")).unwrap();
        assert_eq!(validated.provider_payload().payload().as_ptr(), pointer);
        assert_eq!(validated.encoded_len(), encoded_len);
        assert_eq!(validated.encoded_len(), validated.as_proto().encoded_len());
    }

    #[test]
    fn exact_payload_receipt_follows_last_provider_bytes_alias() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        struct OriginalReceipt(Arc<AtomicUsize>);
        impl Drop for OriginalReceipt {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::AcqRel);
            }
        }
        let raw = raw(ConnectorCodecCategory::CommitFragment);
        let pointer = raw.provider_payload.as_ref().unwrap().payload.as_ptr();
        let exits = Arc::new(AtomicUsize::new(0));
        let validated = ValidatedCommitFragment::parse(raw, FieldPath::root("fragment"))
            .unwrap()
            .retain_provider_payload(ConnectorPayloadRetentionGuard::new(OriginalReceipt(
                exits.clone(),
            )));
        assert_eq!(validated.provider_payload().payload().as_ptr(), pointer);
        let provider_alias = validated.provider_payload().payload().clone();
        let last_slice = provider_alias.slice(1..);
        drop((validated, provider_alias));
        assert_eq!(exits.load(Ordering::Acquire), 0);
        assert_eq!(last_slice.as_ref(), b"rtifact");
        drop(last_slice);
        assert_eq!(exits.load(Ordering::Acquire), 1);
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
