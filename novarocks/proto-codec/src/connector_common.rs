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

//! Strict outer codec for provider-owned connector payloads.

use bytes::Bytes;
use novarocks_proto_models::{catalog as catalog_dto, connector_common as dto};
use novarocks_spi::connector::{
    CATALOG_VERSION_BYTES, CatalogHandle, CatalogVersion, ConnectorCodecCategory,
    ConnectorCodecError, ConnectorCodecErrorKind, ConnectorCodecRevision, ConnectorDecodeLedger,
    ConnectorDecodeLimits, ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorFieldPath,
    ConnectorInstanceId, ConnectorProviderId,
};
use prost::Message;

use crate::{FieldPath, ProtocolError, ProtocolErrorKind};

pub fn encode_connector_payload(value: &ConnectorEncodedPayload) -> Vec<u8> {
    encode_connector_payload_message(value).encode_to_vec()
}

/// Materialize the public protobuf envelope around provider-private bytes.
/// Purpose-specific carriers use this helper so they never know a provider's
/// private message type.
pub fn encode_connector_payload_message(
    value: &ConnectorEncodedPayload,
) -> dto::ConnectorEncodedPayload {
    let header = value.header();
    dto::ConnectorEncodedPayload {
        header: Some(dto::ConnectorEnvelopeHeader {
            provider_id: header.provider_id().as_str().to_owned(),
            catalog: Some(catalog_dto::CatalogHandle {
                catalog_name: header.catalog().catalog_name().as_str().to_owned(),
                version: header.catalog().version().as_bytes().to_vec(),
            }),
            category: encode_connector_category(header.category()),
            codec_revision: header.codec_revision().get(),
        }),
        payload: value.payload().to_vec(),
    }
}

pub fn decode_connector_payload(
    encoded: &[u8],
    limits: ConnectorDecodeLimits,
) -> Result<ConnectorEncodedPayload, ConnectorCodecError> {
    let root = ConnectorFieldPath::root("connector_payload");
    let mut ledger = ConnectorDecodeLedger::new(limits);
    ledger.charge_raw(encoded.len())?;
    scan_message(encoded, &[(1, 2), (2, 2)], &root)?;
    let raw = dto::ConnectorEncodedPayload::decode(encoded)
        .map_err(|error| invalid(root.clone(), format!("malformed protobuf: {error}")))?;
    raw.header
        .as_ref()
        .ok_or_else(|| missing(root.field("header")))?;
    let encoded_header = extract_length_delimited(encoded, 1, &root)?;
    let header_path = root.field("header");
    scan_message(
        encoded_header,
        &[(1, 2), (2, 2), (3, 0), (4, 0)],
        &header_path,
    )?;
    let encoded_catalog = extract_length_delimited(encoded_header, 2, &header_path)?;
    let catalog_path = header_path.field("catalog");
    scan_message(encoded_catalog, &[(1, 2), (2, 2)], &catalog_path)?;
    finish_owned_connector_payload(raw, &mut ledger)
}

// The raw-wire entry above remains the strict unknown/duplicate-field author.
// A decoded DTO has only its canonical known fields; consume those original
// allocations instead of serializing and decoding them a second time.
fn finish_owned_connector_payload(
    raw: dto::ConnectorEncodedPayload,
    ledger: &mut ConnectorDecodeLedger,
) -> Result<ConnectorEncodedPayload, ConnectorCodecError> {
    let root = ConnectorFieldPath::root("connector_payload");
    let raw_header = raw.header.ok_or_else(|| missing(root.field("header")))?;
    let header_path = root.field("header");
    // The previous canonical wire scan reports the absent field number before
    // any semantic header work. Preserve that public error path/precedence.
    let raw_catalog = raw_header
        .catalog
        .ok_or_else(|| missing(header_path.field("field_2")))?;
    let catalog_path = header_path.field("catalog");
    ledger.charge_items(8)?;
    ledger.check_depth(3)?;
    ledger.charge_scalar(raw_header.provider_id.len())?;
    let provider_id = ConnectorProviderId::parse(&raw_header.provider_id).map_err(|_| {
        invalid(
            header_path.field("provider_id"),
            "invalid provider identity",
        )
    })?;
    ledger.charge_scalar(raw_catalog.catalog_name.len())?;
    ledger.charge_scalar(raw_catalog.version.len())?;
    let catalog_name =
        ConnectorInstanceId::try_from_canonical(&raw_catalog.catalog_name).map_err(|_| {
            invalid(
                catalog_path.field("catalog_name"),
                "invalid catalog identity",
            )
        })?;
    let version: [u8; CATALOG_VERSION_BYTES] = raw_catalog.version.try_into().map_err(|_| {
        invalid(
            catalog_path.field("version"),
            "catalog version must contain exactly 32 bytes",
        )
    })?;
    let category = decode_category(raw_header.category, header_path.field("category"))?;
    let revision = ConnectorCodecRevision::try_new(raw_header.codec_revision)?;
    ledger.charge_retained(
        raw.payload
            .len()
            .saturating_add(raw_header.provider_id.len())
            .saturating_add(raw_catalog.catalog_name.len())
            .saturating_add(CATALOG_VERSION_BYTES),
    )?;
    Ok(ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            provider_id,
            CatalogHandle::new(catalog_name, CatalogVersion::from_bytes(version)),
            category,
            revision,
        ),
        Bytes::from(raw.payload),
    ))
}

/// Validate an envelope that has already been decoded as part of a larger
/// protobuf carrier. Provider payload semantics remain untouched here.
pub fn decode_connector_payload_message(
    raw: dto::ConnectorEncodedPayload,
    limits: ConnectorDecodeLimits,
) -> Result<ConnectorEncodedPayload, ConnectorCodecError> {
    let mut ledger = ConnectorDecodeLedger::new(limits);
    ledger.charge_raw(raw.encoded_len())?;
    finish_owned_connector_payload(raw, &mut ledger)
}

/// Decode one envelope embedded in a purpose-specific carrier and prove that
/// it belongs to the category selected by that carrier. The installed binding
/// performs the later provider/catalog/revision comparison.
pub fn decode_embedded_connector_payload(
    raw: Option<&dto::ConnectorEncodedPayload>,
    expected_category: ConnectorCodecCategory,
    max_encoded_bytes: usize,
    path: FieldPath,
) -> Result<ConnectorEncodedPayload, ProtocolError> {
    // Keep the original borrowed API's rejection before its unavoidable DTO
    // clone. Owning carriers use the move entry below and need no such clone.
    check_embedded_envelope_size(raw, max_encoded_bytes, &path)?;
    decode_embedded_owned_connector_payload(
        raw.cloned(),
        expected_category,
        max_encoded_bytes,
        path,
    )
}

/// Consume an already-owned public envelope, retaining its original payload
/// allocation. The same neutral limits/header/category checks still apply.
/// Raw bytes must first use their carrier's existing wire decoder.
pub fn decode_embedded_owned_connector_payload(
    raw: Option<dto::ConnectorEncodedPayload>,
    expected_category: ConnectorCodecCategory,
    max_encoded_bytes: usize,
    path: FieldPath,
) -> Result<ConnectorEncodedPayload, ProtocolError> {
    check_embedded_envelope_size(raw.as_ref(), max_encoded_bytes, &path)?;
    let raw = raw.expect("the original embedded envelope size check requires presence");
    let envelope_slack = 4096usize;
    let limit = max_encoded_bytes.max(1);
    let limits =
        ConnectorDecodeLimits::try_new(limit, limit.saturating_add(envelope_slack), limit, 16, 8)
            .expect("finite embedded connector limits are valid");
    let decoded = decode_connector_payload_message(raw, limits)
        .map_err(|error| connector_error(path.clone(), error))?;
    if decoded.header().category() != expected_category {
        return Err(ProtocolError::new(
            path.field("header"),
            ProtocolErrorKind::InconsistentFields,
            "connector payload category does not match its public carrier",
        ));
    }
    Ok(decoded)
}

fn check_embedded_envelope_size(
    raw: Option<&dto::ConnectorEncodedPayload>,
    max_encoded_bytes: usize,
    path: &FieldPath,
) -> Result<(), ProtocolError> {
    let raw = raw.ok_or_else(|| {
        ProtocolError::new(
            (*path).clone(),
            ProtocolErrorKind::MissingField,
            "provider payload must be present",
        )
    })?;
    let encoded_len = raw.encoded_len();
    if encoded_len > max_encoded_bytes {
        return Err(ProtocolError::new(
            (*path).clone(),
            ProtocolErrorKind::OutOfRange,
            format!("provider payload exceeds {max_encoded_bytes} encoded bytes"),
        ));
    }
    Ok(())
}

fn connector_error(path: FieldPath, error: ConnectorCodecError) -> ProtocolError {
    let kind = match error.kind() {
        ConnectorCodecErrorKind::MissingField => ProtocolErrorKind::MissingField,
        ConnectorCodecErrorKind::InvalidEnum => ProtocolErrorKind::InvalidEnum,
        ConnectorCodecErrorKind::InvalidValue | ConnectorCodecErrorKind::UnknownField => {
            ProtocolErrorKind::InvalidValue
        }
        ConnectorCodecErrorKind::DuplicateField => ProtocolErrorKind::DuplicateField,
        ConnectorCodecErrorKind::InconsistentFields => ProtocolErrorKind::InconsistentFields,
        ConnectorCodecErrorKind::VersionMismatch => ProtocolErrorKind::VersionMismatch,
        ConnectorCodecErrorKind::CompileControl(cause) => {
            ProtocolErrorKind::CompileControl(cause.clone())
        }
        ConnectorCodecErrorKind::Domain(_)
        | ConnectorCodecErrorKind::Capacity
        | ConnectorCodecErrorKind::Unsupported => return ProtocolError::from_domain(path, error),
    };
    ProtocolError::new(path, kind, error.to_string())
}

/// Project the sole neutral category vocabulary without a private codec or
/// runtime capability. Carrier codecs share this exhaustive conversion.
pub fn encode_connector_category(category: ConnectorCodecCategory) -> i32 {
    match category {
        ConnectorCodecCategory::ReadTable => dto::ConnectorPayloadCategory::ReadTable as i32,
        ConnectorCodecCategory::ReadView => dto::ConnectorPayloadCategory::ReadView as i32,
        ConnectorCodecCategory::ReadColumn => dto::ConnectorPayloadCategory::ReadColumn as i32,
        ConnectorCodecCategory::ReadSplit => dto::ConnectorPayloadCategory::ReadSplit as i32,
        ConnectorCodecCategory::WriteHandle => dto::ConnectorPayloadCategory::WriteHandle as i32,
        ConnectorCodecCategory::CommitFragment => {
            dto::ConnectorPayloadCategory::CommitFragment as i32
        }
    }
}

fn decode_category(
    value: i32,
    path: ConnectorFieldPath,
) -> Result<ConnectorCodecCategory, ConnectorCodecError> {
    decode_connector_category(value).ok_or_else(|| {
        ConnectorCodecError::new(
            path,
            ConnectorCodecErrorKind::InvalidEnum,
            "connector payload category is unknown or unspecified",
        )
    })
}

/// Reject unspecified and unknown categories before any provider lookup.
/// This checks the neutral envelope grammar and grants no private authority.
pub fn decode_connector_category(value: i32) -> Option<ConnectorCodecCategory> {
    match dto::ConnectorPayloadCategory::try_from(value) {
        Ok(dto::ConnectorPayloadCategory::ReadTable) => Some(ConnectorCodecCategory::ReadTable),
        Ok(dto::ConnectorPayloadCategory::ReadView) => Some(ConnectorCodecCategory::ReadView),
        Ok(dto::ConnectorPayloadCategory::ReadColumn) => Some(ConnectorCodecCategory::ReadColumn),
        Ok(dto::ConnectorPayloadCategory::ReadSplit) => Some(ConnectorCodecCategory::ReadSplit),
        Ok(dto::ConnectorPayloadCategory::WriteHandle) => Some(ConnectorCodecCategory::WriteHandle),
        Ok(dto::ConnectorPayloadCategory::CommitFragment) => {
            Some(ConnectorCodecCategory::CommitFragment)
        }
        Ok(dto::ConnectorPayloadCategory::Unspecified) | Err(_) => None,
    }
}

fn scan_message(
    mut input: &[u8],
    fields: &[(u32, u8)],
    path: &ConnectorFieldPath,
) -> Result<(), ConnectorCodecError> {
    let mut seen = 0u64;
    while !input.is_empty() {
        let key = read_varint(&mut input, path)?;
        let field = u32::try_from(key >> 3)
            .map_err(|_| invalid(path.clone(), "protobuf field number is out of range"))?;
        let wire = (key & 7) as u8;
        let index = fields
            .iter()
            .position(|(number, _)| *number == field)
            .ok_or_else(|| {
                ConnectorCodecError::new(
                    path.field(format!("field_{field}")),
                    ConnectorCodecErrorKind::UnknownField,
                    "unknown connector envelope field",
                )
            })?;
        if wire != fields[index].1 {
            return Err(ConnectorCodecError::new(
                path.field(format!("field_{field}")),
                ConnectorCodecErrorKind::InvalidValue,
                "connector envelope field has the wrong protobuf wire type",
            ));
        }
        let bit = 1u64 << index;
        if seen & bit != 0 {
            return Err(ConnectorCodecError::new(
                path.field(format!("field_{field}")),
                ConnectorCodecErrorKind::DuplicateField,
                "connector envelope singular field appears more than once",
            ));
        }
        seen |= bit;
        skip_value(&mut input, wire, path)?;
    }
    Ok(())
}

fn extract_length_delimited<'a>(
    mut input: &'a [u8],
    wanted: u32,
    path: &ConnectorFieldPath,
) -> Result<&'a [u8], ConnectorCodecError> {
    while !input.is_empty() {
        let key = read_varint(&mut input, path)?;
        let field = (key >> 3) as u32;
        let wire = (key & 7) as u8;
        if wire != 2 {
            skip_value(&mut input, wire, path)?;
            continue;
        }
        let length = usize::try_from(read_varint(&mut input, path)?)
            .map_err(|_| invalid(path.clone(), "protobuf length is out of range"))?;
        if input.len() < length {
            return Err(invalid(path.clone(), "truncated length-delimited field"));
        }
        let (value, rest) = input.split_at(length);
        if field == wanted {
            return Ok(value);
        }
        input = rest;
    }
    Err(missing(path.field(format!("field_{wanted}"))))
}

fn skip_value(
    input: &mut &[u8],
    wire: u8,
    path: &ConnectorFieldPath,
) -> Result<(), ConnectorCodecError> {
    match wire {
        0 => {
            read_varint(input, path)?;
        }
        1 => take(input, 8, path)?,
        2 => {
            let length = usize::try_from(read_varint(input, path)?)
                .map_err(|_| invalid(path.clone(), "protobuf length is out of range"))?;
            take(input, length, path)?;
        }
        5 => take(input, 4, path)?,
        _ => return Err(invalid(path.clone(), "unsupported protobuf wire type")),
    }
    Ok(())
}

fn take(
    input: &mut &[u8],
    length: usize,
    path: &ConnectorFieldPath,
) -> Result<(), ConnectorCodecError> {
    if input.len() < length {
        return Err(invalid(path.clone(), "truncated protobuf field"));
    }
    *input = &input[length..];
    Ok(())
}

fn read_varint(input: &mut &[u8], path: &ConnectorFieldPath) -> Result<u64, ConnectorCodecError> {
    let mut value = 0u64;
    for shift in (0..70).step_by(7) {
        let (&byte, rest) = input
            .split_first()
            .ok_or_else(|| invalid(path.clone(), "truncated protobuf varint"))?;
        *input = rest;
        if shift == 63 && byte > 1 {
            return Err(invalid(path.clone(), "protobuf varint overflow"));
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(invalid(path.clone(), "protobuf varint overflow"))
}

fn missing(path: ConnectorFieldPath) -> ConnectorCodecError {
    ConnectorCodecError::new(
        path,
        ConnectorCodecErrorKind::MissingField,
        "required connector envelope field is missing",
    )
}

fn invalid(path: ConnectorFieldPath, detail: impl AsRef<str>) -> ConnectorCodecError {
    ConnectorCodecError::new(path, ConnectorCodecErrorKind::InvalidValue, detail)
}

#[cfg(test)]
mod compile_control_adapter_tests {
    use super::*;
    use novarocks_type_contract::CompileControlError;
    use std::error::Error;

    #[test]
    fn connector_error_adapter_keeps_each_compile_cause_and_both_diagnostic_paths() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let provider = ConnectorCodecError::from(cause.clone())
                .with_path(ConnectorFieldPath::root("columns").index(17));
            let expected_detail = provider.to_string();
            let outer = FieldPath::root("fragment")
                .field("scans")
                .index(3)
                .field("provider_payload");
            let error = connector_error(outer.clone(), provider);
            assert_eq!(error.path(), &outer);
            assert_eq!(
                error.kind(),
                &ProtocolErrorKind::CompileControl(cause.clone())
            );
            assert_eq!(error.detail(), expected_detail);
            assert!(error.detail().contains("columns[17]"));
            assert_eq!(
                error
                    .source()
                    .unwrap()
                    .downcast_ref::<CompileControlError>(),
                Some(&cause)
            );
        }
    }

    #[test]
    fn connector_error_domain_keeps_whole_source_and_both_field_paths() {
        use novarocks_execution_contract::failure::{FailureProjection, SharedFailureCause};
        let raw = novarocks_spi::connector::ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::CorruptData,
            "provider detail",
        );
        let codec =
            ConnectorCodecError::from_domain(ConnectorFieldPath::root("columns").index(17), raw);
        let fact = codec.failure_fact();
        let error = connector_error(FieldPath::root("outer"), codec.clone());
        assert_eq!(error.path().to_string(), "outer");
        assert_eq!(error.failure_fact(), fact);
        let source = error
            .source()
            .unwrap()
            .downcast_ref::<SharedFailureCause>()
            .unwrap();
        assert_eq!(source.downcast_ref::<ConnectorCodecError>(), Some(&codec));
    }

    #[test]
    fn nominal_capacity_and_unsupported_keep_original_fact_quantities_and_source() {
        use novarocks_execution_contract::failure::{
            FailureClass, FailureProjection, SharedFailureCause,
        };
        use novarocks_spi::connector::{
            ConnectorDecodeLedger, ConnectorDecodeLimits, ConnectorFieldPath,
        };
        let mut ledger =
            ConnectorDecodeLedger::new(ConnectorDecodeLimits::try_new(8, 8, 8, 8, 8).unwrap());
        ledger.charge_raw(7).unwrap();
        let known = ledger.charge_raw(2).unwrap_err();
        let unknown = ConnectorCodecError::new(
            ConnectorFieldPath::root("payload").field("header"),
            ConnectorCodecErrorKind::Capacity,
            "pure compilation was cancelled",
        );
        let unsupported = ConnectorCodecError::new(
            ConnectorFieldPath::root("payload").field("header"),
            ConnectorCodecErrorKind::Unsupported,
            "dependency permission denied",
        );
        for original in [known, unknown, unsupported] {
            let fact = original.failure_fact();
            let error = connector_error(FieldPath::root("scan"), original.clone());
            let protocol = &error;
            assert_eq!(protocol.path().to_string(), "scan");
            assert!(matches!(protocol.kind(), ProtocolErrorKind::Domain(_)));
            assert_eq!(protocol.failure_fact(), fact);
            let shared = std::error::Error::source(protocol)
                .unwrap()
                .downcast_ref::<SharedFailureCause>()
                .unwrap();
            assert_eq!(
                shared.downcast_ref::<ConnectorCodecError>(),
                Some(&original)
            );
            if original.kind() == &ConnectorCodecErrorKind::Unsupported {
                assert_eq!(fact.class(), FailureClass::Unsupported);
                assert_eq!(fact.reason().code(), 166);
                assert!(fact.resource().is_none());
            } else {
                assert_eq!(fact.class(), FailureClass::Resource);
                assert_eq!(fact.reason().code(), 167);
                if let Some(resource) = fact.resource() {
                    assert_eq!(
                        (
                            resource.requested,
                            resource.required_growth,
                            resource.committed,
                            resource.limit,
                            resource.live
                        ),
                        (Some(2), Some(2), Some(7), Some(8), None)
                    );
                } else {
                    assert_eq!(original.path().to_string(), "payload.header");
                }
            }
        }
    }

    #[test]
    fn connector_error_adapter_preserves_ordinary_categories_despite_control_spelling() {
        let categories = [
            (
                ConnectorCodecErrorKind::MissingField,
                ProtocolErrorKind::MissingField,
            ),
            (
                ConnectorCodecErrorKind::InvalidEnum,
                ProtocolErrorKind::InvalidEnum,
            ),
            (
                ConnectorCodecErrorKind::InvalidValue,
                ProtocolErrorKind::InvalidValue,
            ),
            (
                ConnectorCodecErrorKind::UnknownField,
                ProtocolErrorKind::InvalidValue,
            ),
            (
                ConnectorCodecErrorKind::DuplicateField,
                ProtocolErrorKind::DuplicateField,
            ),
            (
                ConnectorCodecErrorKind::InconsistentFields,
                ProtocolErrorKind::InconsistentFields,
            ),
            (
                ConnectorCodecErrorKind::VersionMismatch,
                ProtocolErrorKind::VersionMismatch,
            ),
        ];
        for (provider_kind, expected_kind) in categories {
            let provider = ConnectorCodecError::new(
                ConnectorFieldPath::root("payload").field("header"),
                provider_kind,
                "pure compilation was cancelled; pure compilation deadline was exceeded; pure compilation work budget was exhausted",
            );
            let expected_detail = provider.to_string();
            let error = connector_error(FieldPath::root("scan"), provider);
            assert_eq!(error.kind(), &expected_kind);
            assert_eq!(error.detail(), expected_detail);
            assert!(error.source().is_none());
        }
    }

    fn commit_envelope() -> dto::ConnectorEncodedPayload {
        dto::ConnectorEncodedPayload {
            header: Some(dto::ConnectorEnvelopeHeader {
                provider_id: "test".into(),
                catalog: Some(catalog_dto::CatalogHandle {
                    catalog_name: "lake".into(),
                    version: vec![9; 32],
                }),
                category: encode_connector_category(ConnectorCodecCategory::CommitFragment),
                codec_revision: 1,
            }),
            payload: vec![7; 64],
        }
    }

    #[test]
    fn owned_message_validation_preserves_wire_header_limits_and_error_precedence() {
        let valid = commit_envelope();
        let mut cases = vec![valid.clone()];
        let mut missing_header = valid.clone();
        missing_header.header = None;
        cases.push(missing_header);
        let mut missing_catalog = valid.clone();
        let header = missing_catalog.header.as_mut().unwrap();
        header.catalog = None;
        header.provider_id.clear();
        cases.push(missing_catalog);
        let mut bad_version = valid.clone();
        bad_version
            .header
            .as_mut()
            .unwrap()
            .catalog
            .as_mut()
            .unwrap()
            .version
            .pop();
        cases.push(bad_version);
        let mut bad_category = valid.clone();
        bad_category.header.as_mut().unwrap().category = i32::MAX;
        cases.push(bad_category);
        let mut bad_revision = valid.clone();
        bad_revision.header.as_mut().unwrap().codec_revision = 0;
        cases.push(bad_revision);
        let limits = ConnectorDecodeLimits::try_new(8192, 8192, 8192, 16, 8).unwrap();
        for raw in cases {
            let encoded = raw.encode_to_vec();
            let wire = decode_connector_payload(&encoded, limits);
            let owned = decode_connector_payload_message(raw, limits);
            match (wire, owned) {
                (Ok(wire), Ok(owned)) => assert_eq!(wire, owned),
                (Err(wire), Err(owned)) => {
                    assert_eq!(wire.kind(), owned.kind());
                    assert_eq!(wire.path(), owned.path());
                    assert_eq!(wire.to_string(), owned.to_string());
                }
                pair => panic!("wire and owned validation disagree: {pair:?}"),
            }
        }
        let encoded = valid.encode_to_vec();
        let limits = ConnectorDecodeLimits::try_new(encoded.len() - 1, 8192, 8192, 16, 8).unwrap();
        let wire = decode_connector_payload(&encoded, limits).unwrap_err();
        let owned = decode_connector_payload_message(valid, limits).unwrap_err();
        assert_eq!(wire.kind(), &ConnectorCodecErrorKind::Capacity);
        assert_eq!(wire.kind(), owned.kind());
        assert_eq!(wire.path(), owned.path());
    }

    #[test]
    fn both_embedded_entries_keep_size_refusal_before_decoding() {
        let raw = commit_envelope();
        let bound = raw.encoded_len() - 1;
        let path = FieldPath::root("fragment").field("provider_payload");
        let borrowed = decode_embedded_connector_payload(
            Some(&raw),
            ConnectorCodecCategory::CommitFragment,
            bound,
            path.clone(),
        )
        .unwrap_err();
        let owned = decode_embedded_owned_connector_payload(
            Some(raw),
            ConnectorCodecCategory::CommitFragment,
            bound,
            path.clone(),
        )
        .unwrap_err();
        assert_eq!(borrowed.kind(), &ProtocolErrorKind::OutOfRange);
        assert_eq!(borrowed.kind(), owned.kind());
        assert_eq!(borrowed.path(), &path);
        assert_eq!(borrowed.to_string(), owned.to_string());
    }

    #[test]
    fn raw_wire_unknown_and_duplicate_fields_still_use_the_original_strict_scanner() {
        let limits = ConnectorDecodeLimits::try_new(8192, 8192, 8192, 16, 8).unwrap();
        let encoded = commit_envelope().encode_to_vec();
        let mut unknown = encoded.clone();
        unknown.extend_from_slice(&[0x1a, 0]);
        assert_eq!(
            decode_connector_payload(&unknown, limits)
                .unwrap_err()
                .kind(),
            &ConnectorCodecErrorKind::UnknownField
        );
        let mut duplicate = encoded;
        duplicate.extend_from_slice(&[0x12, 0]);
        assert_eq!(
            decode_connector_payload(&duplicate, limits)
                .unwrap_err()
                .kind(),
            &ConnectorCodecErrorKind::DuplicateField
        );
    }

    #[test]
    fn embedded_connector_decode_keeps_existing_missing_and_category_errors() {
        let path = FieldPath::root("fragment").field("scan");
        let missing = decode_embedded_connector_payload(
            None,
            ConnectorCodecCategory::ReadTable,
            1024,
            path.clone(),
        )
        .unwrap_err();
        assert_eq!(missing.path(), &path);
        assert_eq!(missing.kind(), &ProtocolErrorKind::MissingField);
        assert!(missing.source().is_none());

        let payload = ConnectorEncodedPayload::new(
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
        );
        let raw = encode_connector_payload_message(&payload);
        let mismatch = decode_embedded_connector_payload(
            Some(&raw),
            ConnectorCodecCategory::ReadTable,
            1024,
            path.clone(),
        )
        .unwrap_err();
        assert_eq!(mismatch.path(), &path.field("header"));
        assert_eq!(mismatch.kind(), &ProtocolErrorKind::InconsistentFields);
        assert_eq!(
            mismatch.detail(),
            "connector payload category does not match its public carrier"
        );
        assert!(mismatch.source().is_none());
    }
}
