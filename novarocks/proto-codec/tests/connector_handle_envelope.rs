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

use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use novarocks_proto_codec::connector_common::{decode_connector_payload, encode_connector_payload};
use novarocks_proto_models::{catalog as catalog_dto, connector_common as dto};
use novarocks_spi::connector::{
    CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecErrorKind,
    ConnectorCodecRevision, ConnectorDecodeContext, ConnectorDecodeLedger, ConnectorDecodeLimits,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorFieldPath, ConnectorInstanceId,
    ConnectorPrivateDecoder, ConnectorProviderId,
};
use prost::Message;

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

fn raw_catalog() -> catalog_dto::CatalogHandle {
    catalog_dto::CatalogHandle {
        catalog_name: "lake".to_string(),
        version: vec![4; 32],
    }
}

fn raw_header() -> dto::ConnectorEnvelopeHeader {
    dto::ConnectorEnvelopeHeader {
        provider_id: "paimon".to_string(),
        catalog: Some(raw_catalog()),
        category: 4,
        codec_revision: 3,
    }
}

fn push_varint(bytes: &mut Vec<u8>, mut value: u64) {
    loop {
        let next = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            bytes.push(next);
            return;
        }
        bytes.push(next | 0x80);
    }
}

fn push_length_delimited(bytes: &mut Vec<u8>, field: u32, value: &[u8]) {
    push_varint(bytes, u64::from((field << 3) | 2));
    push_varint(bytes, value.len() as u64);
    bytes.extend_from_slice(value);
}

fn envelope_with_raw_header(header: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    push_length_delimited(&mut bytes, 1, header);
    push_length_delimited(&mut bytes, 2, b"private-split");
    bytes
}

fn header_with_raw_catalog(catalog: &[u8]) -> Vec<u8> {
    let mut header = Vec::new();
    push_length_delimited(&mut header, 1, b"paimon");
    push_length_delimited(&mut header, 2, catalog);
    push_varint(&mut header, 3 << 3);
    push_varint(&mut header, 4);
    push_varint(&mut header, 4 << 3);
    push_varint(&mut header, 3);
    header
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

#[test]
fn nested_header_and_catalog_reject_unknown_duplicate_missing_and_wrong_wire_fields() {
    let mut unknown_header = raw_header().encode_to_vec();
    push_varint(&mut unknown_header, 5 << 3);
    push_varint(&mut unknown_header, 1);
    assert_eq!(
        decode_connector_payload(
            &envelope_with_raw_header(&unknown_header),
            limits(4096, 4096)
        )
        .unwrap_err()
        .kind(),
        ConnectorCodecErrorKind::UnknownField
    );

    let mut duplicate_header = raw_header().encode_to_vec();
    push_length_delimited(&mut duplicate_header, 1, b"iceberg");
    assert_eq!(
        decode_connector_payload(
            &envelope_with_raw_header(&duplicate_header),
            limits(4096, 4096),
        )
        .unwrap_err()
        .kind(),
        ConnectorCodecErrorKind::DuplicateField
    );

    let mut wrong_wire_header = Vec::new();
    push_length_delimited(&mut wrong_wire_header, 1, b"paimon");
    push_length_delimited(&mut wrong_wire_header, 2, &raw_catalog().encode_to_vec());
    push_length_delimited(&mut wrong_wire_header, 3, b"read-split");
    push_varint(&mut wrong_wire_header, 4 << 3);
    push_varint(&mut wrong_wire_header, 3);
    assert_eq!(
        decode_connector_payload(
            &envelope_with_raw_header(&wrong_wire_header),
            limits(4096, 4096),
        )
        .unwrap_err()
        .kind(),
        ConnectorCodecErrorKind::InvalidValue
    );

    let mut unknown_catalog = raw_catalog().encode_to_vec();
    push_varint(&mut unknown_catalog, 3 << 3);
    push_varint(&mut unknown_catalog, 1);
    assert_eq!(
        decode_connector_payload(
            &envelope_with_raw_header(&header_with_raw_catalog(&unknown_catalog)),
            limits(4096, 4096),
        )
        .unwrap_err()
        .kind(),
        ConnectorCodecErrorKind::UnknownField
    );

    let mut duplicate_catalog = raw_catalog().encode_to_vec();
    push_length_delimited(&mut duplicate_catalog, 1, b"other-lake");
    assert_eq!(
        decode_connector_payload(
            &envelope_with_raw_header(&header_with_raw_catalog(&duplicate_catalog)),
            limits(4096, 4096),
        )
        .unwrap_err()
        .kind(),
        ConnectorCodecErrorKind::DuplicateField
    );

    let mut wrong_wire_catalog = raw_catalog().encode_to_vec();
    push_varint(&mut wrong_wire_catalog, 2 << 3);
    push_varint(&mut wrong_wire_catalog, 7);
    assert_eq!(
        decode_connector_payload(
            &envelope_with_raw_header(&header_with_raw_catalog(&wrong_wire_catalog)),
            limits(4096, 4096),
        )
        .unwrap_err()
        .kind(),
        ConnectorCodecErrorKind::InvalidValue
    );

    for malformed in [
        dto::ConnectorEncodedPayload {
            header: None,
            payload: b"private-split".to_vec(),
        },
        dto::ConnectorEncodedPayload {
            header: Some(dto::ConnectorEnvelopeHeader {
                catalog: None,
                ..raw_header()
            }),
            payload: b"private-split".to_vec(),
        },
        dto::ConnectorEncodedPayload {
            header: Some(dto::ConnectorEnvelopeHeader {
                provider_id: String::new(),
                ..raw_header()
            }),
            payload: b"private-split".to_vec(),
        },
        dto::ConnectorEncodedPayload {
            header: Some(dto::ConnectorEnvelopeHeader {
                catalog: Some(catalog_dto::CatalogHandle {
                    version: Vec::new(),
                    ..raw_catalog()
                }),
                ..raw_header()
            }),
            payload: b"private-split".to_vec(),
        },
    ] {
        assert!(decode_connector_payload(&malformed.encode_to_vec(), limits(4096, 4096)).is_err());
    }
}

#[test]
fn raw_retained_scalar_item_and_depth_budgets_fail_independently() {
    let encoded = encode_connector_payload(&payload());
    let scalar_bytes = "paimon".len() + "lake".len() + 32;
    let cases = [
        ConnectorDecodeLimits::try_new(encoded.len() - 1, usize::MAX, usize::MAX, usize::MAX, 64)
            .unwrap(),
        ConnectorDecodeLimits::try_new(encoded.len(), 1, usize::MAX, usize::MAX, 64).unwrap(),
        ConnectorDecodeLimits::try_new(encoded.len(), usize::MAX, scalar_bytes - 1, usize::MAX, 64)
            .unwrap(),
        ConnectorDecodeLimits::try_new(encoded.len(), usize::MAX, usize::MAX, 7, 64).unwrap(),
        ConnectorDecodeLimits::try_new(encoded.len(), usize::MAX, usize::MAX, usize::MAX, 2)
            .unwrap(),
    ];
    for limits in cases {
        assert_eq!(
            decode_connector_payload(&encoded, limits)
                .unwrap_err()
                .kind(),
            ConnectorCodecErrorKind::Capacity
        );
    }
}

struct StrictSplitDecoder {
    expected: ConnectorEnvelopeHeader,
    payload_parses: AtomicUsize,
}

impl ConnectorPrivateDecoder<u8> for StrictSplitDecoder {
    fn decode_private(
        &self,
        payload: &[u8],
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<u8, novarocks_spi::connector::ConnectorCodecError> {
        context.expected_header().validate_expected(
            self.expected.provider_id(),
            self.expected.catalog(),
            self.expected.category(),
            self.expected.codec_revision(),
        )?;
        self.payload_parses.fetch_add(1, Ordering::SeqCst);
        context.ledger().charge_raw(payload.len())?;
        if payload != [42] {
            return Err(novarocks_spi::connector::ConnectorCodecError::new(
                ConnectorFieldPath::root("read_split").field("private_kind"),
                ConnectorCodecErrorKind::InconsistentFields,
                "public read-split category carries another private value kind",
            ));
        }
        Ok(42)
    }
}

#[test]
fn installed_binding_is_checked_before_private_payload_and_private_kind_conflicts_fail_closed() {
    let expected = payload().header().clone();
    let decoder = StrictSplitDecoder {
        expected: expected.clone(),
        payload_parses: AtomicUsize::new(0),
    };
    let wrong_headers = [
        ConnectorEnvelopeHeader::new(
            ConnectorProviderId::parse("iceberg").unwrap(),
            expected.catalog().clone(),
            expected.category(),
            expected.codec_revision(),
        ),
        ConnectorEnvelopeHeader::new(
            expected.provider_id().clone(),
            CatalogHandle::new(
                ConnectorInstanceId::try_from_canonical("other-lake").unwrap(),
                CatalogVersion::from_bytes([4; 32]),
            ),
            expected.category(),
            expected.codec_revision(),
        ),
        ConnectorEnvelopeHeader::new(
            expected.provider_id().clone(),
            CatalogHandle::new(
                ConnectorInstanceId::try_from_canonical("lake").unwrap(),
                CatalogVersion::from_bytes([5; 32]),
            ),
            expected.category(),
            expected.codec_revision(),
        ),
        ConnectorEnvelopeHeader::new(
            expected.provider_id().clone(),
            expected.catalog().clone(),
            ConnectorCodecCategory::ReadColumn,
            expected.codec_revision(),
        ),
        ConnectorEnvelopeHeader::new(
            expected.provider_id().clone(),
            expected.catalog().clone(),
            expected.category(),
            ConnectorCodecRevision::try_new(4).unwrap(),
        ),
    ];
    for header in wrong_headers {
        let mut ledger = ConnectorDecodeLedger::new(limits(1024, 1024));
        let mut context = ConnectorDecodeContext::new(&header, &mut ledger);
        assert!(decoder.decode_private(b"untrusted", &mut context).is_err());
        assert_eq!(ledger.raw_bytes(), 0);
    }
    assert_eq!(decoder.payload_parses.load(Ordering::SeqCst), 0);

    let mut ledger = ConnectorDecodeLedger::new(limits(1024, 1024));
    let mut context = ConnectorDecodeContext::new(&expected, &mut ledger);
    assert_eq!(
        decoder
            .decode_private(b"wrong-private-kind", &mut context)
            .unwrap_err()
            .kind(),
        ConnectorCodecErrorKind::InconsistentFields
    );
    assert_eq!(decoder.payload_parses.load(Ordering::SeqCst), 1);

    let mut ledger = ConnectorDecodeLedger::new(limits(1024, 1024));
    let mut context = ConnectorDecodeContext::new(&expected, &mut ledger);
    assert_eq!(decoder.decode_private(&[42], &mut context).unwrap(), 42);
    assert_eq!(decoder.payload_parses.load(Ordering::SeqCst), 2);
}
