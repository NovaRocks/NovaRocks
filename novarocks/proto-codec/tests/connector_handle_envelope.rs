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

use bytes::Bytes;
use novarocks_proto_codec::connector_common::{decode_connector_payload, encode_connector_payload};
use novarocks_spi::connector::{
    CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecErrorKind,
    ConnectorCodecRevision, ConnectorDecodeLimits, ConnectorEncodedPayload,
    ConnectorEnvelopeHeader, ConnectorInstanceId, ConnectorProviderId,
};

fn limits(raw: usize, retained: usize) -> ConnectorDecodeLimits {
    ConnectorDecodeLimits::try_new(raw, retained, 1024, 16, 8).unwrap()
}

fn payload() -> ConnectorEncodedPayload {
    ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            ConnectorProviderId::parse("paimon").unwrap(),
            CatalogHandle::new(
                ConnectorInstanceId::try_from_canonical("lake").unwrap(),
                CatalogVersion::from_bytes([4; 32]),
            ),
            ConnectorCodecCategory::ReadSplit,
            ConnectorCodecRevision::try_new(3).unwrap(),
        ),
        Bytes::from_static(b"private-split"),
    )
}

#[test]
fn public_envelope_round_trips_exact_identity_category_revision_and_payload() {
    let expected = payload();
    let encoded = encode_connector_payload(&expected);
    let decoded = decode_connector_payload(&encoded, limits(encoded.len(), 4096)).unwrap();
    assert_eq!(decoded, expected);
}

#[test]
fn raw_scan_rejects_unknown_duplicate_and_wrong_wire_fields_before_prost_loss() {
    let encoded = encode_connector_payload(&payload());

    let mut unknown = encoded.clone();
    unknown.extend_from_slice(&[0x1a, 0]);
    assert_eq!(
        decode_connector_payload(&unknown, limits(4096, 4096))
            .unwrap_err()
            .kind(),
        ConnectorCodecErrorKind::UnknownField
    );

    let mut duplicate = encoded.clone();
    duplicate.extend_from_slice(&[0x12, 1, b'x']);
    assert_eq!(
        decode_connector_payload(&duplicate, limits(4096, 4096))
            .unwrap_err()
            .kind(),
        ConnectorCodecErrorKind::DuplicateField
    );

    let wrong_wire = [0x08, 0x01];
    assert_eq!(
        decode_connector_payload(&wrong_wire, limits(4096, 4096))
            .unwrap_err()
            .kind(),
        ConnectorCodecErrorKind::InvalidValue
    );
}

#[test]
fn encoded_and_decoded_retained_budgets_are_independent() {
    let encoded = encode_connector_payload(&payload());
    assert_eq!(
        decode_connector_payload(&encoded, limits(encoded.len() - 1, 4096))
            .unwrap_err()
            .kind(),
        ConnectorCodecErrorKind::Capacity
    );
    assert_eq!(
        decode_connector_payload(&encoded, limits(encoded.len(), 1))
            .unwrap_err()
            .kind(),
        ConnectorCodecErrorKind::Capacity
    );
}

#[test]
fn malformed_nested_catalog_and_zero_revision_fail_closed() {
    let mut encoded = encode_connector_payload(&payload());
    let revision_pattern = [0x20, 0x03];
    let position = encoded
        .windows(revision_pattern.len())
        .position(|window| window == revision_pattern)
        .expect("revision field");
    encoded[position + 1] = 0;
    assert_eq!(
        decode_connector_payload(&encoded, limits(4096, 4096))
            .unwrap_err()
            .kind(),
        ConnectorCodecErrorKind::VersionMismatch
    );
}
