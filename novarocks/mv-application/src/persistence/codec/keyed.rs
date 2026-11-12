// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! MV binding and compact publication codecs. Payloads never carry provider I/O.

use super::*;
use crate::persistence::exact_revision::{decode_fact, encode_fact_parts};
use crate::persistence::identity::{NativeDataVersion, PublicationIdentity};
use novarocks_spi::connector::{ConnectorProviderId, ConnectorSemanticFact};

pub const KEYED_INTERPRETATION_FORMAT_VERSION: u32 = 2;
pub const BINDING_FORMAT_VERSION: u32 = 1;
pub const MAX_DATA_VERSION_BYTES: u16 = 256;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataVersionFormat {
    pub provider: String,
    pub name: String,
    pub version: u16,
    pub max_value_bytes: u16,
}
impl DataVersionFormat {
    /// The bound must come from the provider's format declaration, never the
    /// observed value length or a guessed global maximum.
    pub fn from_fact(
        fact: &ConnectorSemanticFact,
        declared_max: u16,
    ) -> Result<Self, PersistenceCodecError> {
        if fact.declared_max_value_bytes() != Some(declared_max) {
            return malformed("data-version maximum differs from the provider format declaration");
        }
        let value = Self {
            provider: fact.provider().as_str().into(),
            name: fact.format().into(),
            version: fact.version(),
            max_value_bytes: declared_max,
        };
        value.validate()?;
        if fact.value().len() > usize::from(declared_max) {
            return malformed("data-version value exceeds its declared format maximum");
        }
        Ok(value)
    }
    pub fn from_issued_fact(fact: &ConnectorSemanticFact) -> Result<Self, PersistenceCodecError> {
        let maximum = fact.declared_max_value_bytes().ok_or_else(|| {
            PersistenceCodecError::MalformedWire(
                "provider data-version format has no declared maximum".into(),
            )
        })?;
        Self::from_fact(fact, maximum)
    }
    fn validate(&self) -> Result<(), PersistenceCodecError> {
        if self.max_value_bytes == 0 || self.max_value_bytes > MAX_DATA_VERSION_BYTES {
            return malformed("binding data-version maximum must be between 1 and 256");
        }
        let provider = ConnectorProviderId::parse(&self.provider)
            .map_err(|e| PersistenceCodecError::MalformedWire(e.to_string()))?;
        if self.name.is_empty()
            || self.name.len() > 128
            || !self.name.is_ascii()
            || self.version == 0
        {
            return malformed("invalid data-version format identity");
        }
        let _ = provider;
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BindingDocument {
    /// Same order and multiplicity as D, including repeated physical objects.
    pub data_versions: Vec<DataVersionFormat>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyedPublicationDocument {
    pub publication_prepared_at_ms: u64,
    pub publication_id: PublicationIdentity,
    pub definition_revision: DocumentRevision,
    pub interpretation_revision: DocumentRevision,
    pub inputs: Vec<PublicationInput>,
    pub logical_result_rows: Option<u64>,
}

fn malformed<T>(message: impl Into<String>) -> Result<T, PersistenceCodecError> {
    Err(PersistenceCodecError::MalformedWire(message.into()))
}

pub fn encode_keyed_interpretation(
    value: &InterpretationDocument,
) -> Result<EncodedDocument, PersistenceCodecError> {
    let mut canonical = value.clone();
    canonicalize_interpretation(&mut canonical);
    validate_interpretation(&canonical)?;
    let mut dto = interpretation_to_proto(&canonical);
    dto.format_version = Some(KEYED_INTERPRETATION_FORMAT_VERSION);
    encode_message(dto)
}
pub fn decode_keyed_interpretation(
    bytes: &[u8],
) -> Result<InterpretationDocument, PersistenceCodecError> {
    wire::preflight(bytes, wire::Schema::InterpretationDocument)?;
    let dto = proto::InterpretationDocument::decode(bytes)
        .map_err(PersistenceCodecError::ProtobufDecode)?;
    if dto.format_version != Some(KEYED_INTERPRETATION_FORMAT_VERSION) {
        return malformed("keyed interpretation requires format version 2");
    }
    let value = interpretation_from_proto(dto)?;
    if encode_keyed_interpretation(&value)?.as_bytes() != bytes {
        return malformed("non-canonical keyed interpretation");
    }
    Ok(value)
}

pub fn encode_binding(value: &BindingDocument) -> Result<EncodedDocument, PersistenceCodecError> {
    if value.data_versions.len() > u16::MAX as usize {
        return malformed("binding has too many relation occurrences");
    }
    for format in &value.data_versions {
        format.validate()?;
    }
    let dto = proto::BindingDocument {
        format_version: Some(BINDING_FORMAT_VERSION),
        data_versions: value
            .data_versions
            .iter()
            .map(|f| proto::DataVersionFormat {
                provider: Some(f.provider.clone()),
                name: Some(f.name.clone()),
                version: Some(u32::from(f.version)),
                max_value_bytes: Some(u32::from(f.max_value_bytes)),
            })
            .collect(),
    };
    encode_message(dto)
}
pub fn decode_binding(bytes: &[u8]) -> Result<BindingDocument, PersistenceCodecError> {
    wire::preflight(bytes, wire::Schema::BindingDocument)?;
    let dto =
        proto::BindingDocument::decode(bytes).map_err(PersistenceCodecError::ProtobufDecode)?;
    if dto.format_version != Some(BINDING_FORMAT_VERSION) {
        return malformed("unsupported binding format version");
    }
    let value = BindingDocument {
        data_versions: dto
            .data_versions
            .into_iter()
            .map(|f| {
                Ok(DataVersionFormat {
                    provider: required(f.provider, "binding.provider")?,
                    name: required(f.name, "binding.name")?,
                    version: u16::try_from(required(f.version, "binding.version")?).map_err(
                        |_| {
                            PersistenceCodecError::MalformedWire(
                                "binding format version exceeds u16".into(),
                            )
                        },
                    )?,
                    max_value_bytes: u16::try_from(required(
                        f.max_value_bytes,
                        "binding.max_value_bytes",
                    )?)
                    .map_err(|_| {
                        PersistenceCodecError::MalformedWire(
                            "binding format maximum exceeds u16".into(),
                        )
                    })?,
                })
            })
            .collect::<Result<_, PersistenceCodecError>>()?,
    };
    if encode_binding(&value)?.as_bytes() != bytes {
        return malformed("non-canonical binding document");
    }
    Ok(value)
}

pub fn maximum_publication_payload_bytes(
    binding: &BindingDocument,
) -> Result<usize, PersistenceCodecError> {
    let mut bytes = 1usize + 16 + 8 + 1 + 8 + 2;
    if binding.data_versions.len() > u16::MAX as usize {
        return malformed("too many publication occurrences");
    }
    for format in &binding.data_versions {
        format.validate()?;
        bytes = bytes
            .checked_add(2 + usize::from(format.max_value_bytes))
            .ok_or_else(|| {
                PersistenceCodecError::MalformedWire("publication size overflow".into())
            })?;
    }
    Ok(bytes)
}

pub fn encode_publication_payload(
    value: &KeyedPublicationDocument,
    definition: &DefinitionDocument,
    binding: &BindingDocument,
) -> Result<Vec<u8>, PersistenceCodecError> {
    if value.publication_id.as_bytes().len() != 16
        || value.publication_prepared_at_ms > i64::MAX as u64
    {
        return malformed("publication identity or frozen timestamp is invalid");
    }
    if value.inputs.len() != definition.relation_occurrences.len()
        || binding.data_versions.len() != value.inputs.len()
    {
        return malformed("publication occurrence count differs from definition or binding");
    }
    let mut out = Vec::with_capacity(maximum_publication_payload_bytes(binding)?);
    out.push(1);
    out.extend_from_slice(value.publication_id.as_bytes());
    out.extend_from_slice(&(value.publication_prepared_at_ms as i64).to_be_bytes());
    out.push(u8::from(value.logical_result_rows.is_some()));
    if let Some(rows) = value.logical_result_rows {
        out.extend_from_slice(&rows.to_be_bytes());
    }
    out.extend_from_slice(&(value.inputs.len() as u16).to_be_bytes());
    for ((input, occurrence), format) in value
        .inputs
        .iter()
        .zip(&definition.relation_occurrences)
        .zip(&binding.data_versions)
    {
        if input.relation_occurrence_id != occurrence.occurrence_id
            || input.object_id != occurrence.object_id
        {
            return malformed("publication input differs from its exact definition occurrence");
        }
        let fact = decode_fact(input.native_data_version.as_bytes())
            .map_err(|e| PersistenceCodecError::MalformedWire(e.to_string()))?;
        if fact.provider != format.provider
            || fact.format != format.name
            || fact.version != format.version
            || fact.value.len() > usize::from(format.max_value_bytes)
            || fact.value.len() > 256
        {
            return malformed("publication input does not match its binding format");
        }
        out.extend_from_slice(&(fact.value.len() as u16).to_be_bytes());
        out.extend_from_slice(&fact.value);
    }
    Ok(out)
}

pub fn decode_publication_payload(
    bytes: &[u8],
    definition: &DefinitionDocument,
    definition_revision: DocumentRevision,
    interpretation_revision: DocumentRevision,
    binding: &BindingDocument,
) -> Result<KeyedPublicationDocument, PersistenceCodecError> {
    let mut cursor = Cursor { bytes, at: 0 };
    if cursor.take(1)? != [1] {
        return malformed("unknown publication payload layout");
    }
    let publication_id = PublicationIdentity::try_new(cursor.take(16)?.to_vec())?;
    let frozen = i64::from_be_bytes(cursor.take(8)?.try_into().unwrap());
    if frozen < 0 {
        return malformed("negative publication frozen timestamp");
    }
    let logical_result_rows = match cursor.take(1)?[0] {
        0 => None,
        1 => Some(u64::from_be_bytes(cursor.take(8)?.try_into().unwrap())),
        _ => return malformed("invalid publication row-presence byte"),
    };
    let count = usize::from(u16::from_be_bytes(cursor.take(2)?.try_into().unwrap()));
    if count != definition.relation_occurrences.len() || count != binding.data_versions.len() {
        return malformed("publication occurrence count differs from definition or binding");
    }
    let mut inputs = Vec::with_capacity(count);
    for (occurrence, format) in definition
        .relation_occurrences
        .iter()
        .zip(&binding.data_versions)
    {
        format.validate()?;
        let len = usize::from(u16::from_be_bytes(cursor.take(2)?.try_into().unwrap()));
        if len == 0 || len > usize::from(format.max_value_bytes) || len > 256 {
            return malformed("publication input exceeds its declared format maximum");
        }
        let raw = cursor.take(len)?;
        let encoded = encode_fact_parts(&format.provider, &format.name, format.version, raw)
            .map_err(|e| PersistenceCodecError::MalformedWire(e.to_string()))?;
        inputs.push(PublicationInput {
            relation_occurrence_id: occurrence.occurrence_id,
            object_id: occurrence.object_id.clone(),
            native_data_version: NativeDataVersion::try_new(encoded)?,
        });
    }
    if cursor.at != bytes.len() {
        return malformed("trailing publication payload bytes");
    }
    Ok(KeyedPublicationDocument {
        publication_prepared_at_ms: frozen as u64,
        publication_id,
        definition_revision,
        interpretation_revision,
        inputs,
        logical_result_rows,
    })
}
struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}
impl<'a> Cursor<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8], PersistenceCodecError> {
        let end = self
            .at
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| {
                PersistenceCodecError::MalformedWire("truncated publication payload".into())
            })?;
        let out = &self.bytes[self.at..end];
        self.at = end;
        Ok(out)
    }
}

/// Check each protobuf document's canonical structure before DTO conversion.
/// Compact publication structure is checked by `decode_publication_payload`,
/// which also needs the decoded definition and binding facts.
pub fn preflight_keyed_document_set(
    d: &[u8],
    l: &[u8],
    b: &[u8],
    c: &[u8],
) -> Result<(), PersistenceCodecError> {
    for (content, schema) in [
        (d, wire::Schema::DefinitionDocument),
        (l, wire::Schema::InterpretationDocument),
        (b, wire::Schema::BindingDocument),
        (c, wire::Schema::ConfigurationDocument),
    ] {
        wire::preflight(content, schema)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::test_support::MvDocumentFixture;
    use novarocks_spi::connector::ConnectorExactSemanticRevision;
    fn fixture() -> (
        DefinitionDocument,
        BindingDocument,
        KeyedPublicationDocument,
    ) {
        let f = MvDocumentFixture::new();
        let mut inputs = Vec::new();
        let mut formats = Vec::new();
        for d in &f.definition.relation_occurrences {
            let data = ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
                ConnectorProviderId::parse("iceberg").unwrap(),
                &crate::persistence::exact_revision::restore_persisted_object(&d.object_id)
                    .unwrap(),
                Some(17),
            )
            .unwrap();
            formats.push(DataVersionFormat::from_issued_fact(data.data_version()).unwrap());
            let (_, v) =
                crate::persistence::exact_revision::persist_exact_connector_revision(&data)
                    .unwrap();
            inputs.push(PublicationInput {
                relation_occurrence_id: d.occurrence_id,
                object_id: d.object_id.clone(),
                native_data_version: v,
            });
        }
        (
            f.definition,
            BindingDocument {
                data_versions: formats,
            },
            KeyedPublicationDocument {
                publication_prepared_at_ms: 42,
                publication_id: PublicationIdentity::try_new(vec![0x11; 16]).unwrap(),
                definition_revision: DocumentRevision::try_from_bytes(&[1; 32]).unwrap(),
                interpretation_revision: DocumentRevision::try_from_bytes(&[2; 32]).unwrap(),
                inputs,
                logical_result_rows: Some(7),
            },
        )
    }
    #[test]
    fn binding_and_compact_publication_roundtrip_with_exact_fact_identities() {
        let (d, b, p) = fixture();
        let encoded = encode_binding(&b).unwrap();
        assert_eq!(
            hex::encode(encoded.as_bytes()),
            "080112240a07696365626572671215636f6e6e6563746f722d736e617073686f742d69641801200912240a07696365626572671215636f6e6e6563746f722d736e617073686f742d696418012009"
        );
        assert_eq!(decode_binding(encoded.as_bytes()).unwrap(), b);
        let bytes = encode_publication_payload(&p, &d, &b).unwrap();
        assert_eq!(
            hex::encode(&bytes),
            "0111111111111111111111111111111111000000000000002a010000000000000007000200090100000000000000110009010000000000000011"
        );
        assert_eq!(&bytes[..17], &[vec![1], vec![0x11; 16]].concat());
        assert_eq!(&bytes[17..25], &42i64.to_be_bytes());
        assert_eq!(
            decode_publication_payload(
                &bytes,
                &d,
                p.definition_revision,
                p.interpretation_revision,
                &b
            )
            .unwrap(),
            p
        );
    }
    #[test]
    fn compact_publication_rejects_truncation_trailing_presence_and_occurrence_mismatch() {
        let (d, b, p) = fixture();
        let bytes = encode_publication_payload(&p, &d, &b).unwrap();
        for end in 0..bytes.len() {
            assert!(
                decode_publication_payload(
                    &bytes[..end],
                    &d,
                    p.definition_revision,
                    p.interpretation_revision,
                    &b
                )
                .is_err()
            );
        }
        let mut bad = bytes.clone();
        bad.push(0);
        assert!(
            decode_publication_payload(
                &bad,
                &d,
                p.definition_revision,
                p.interpretation_revision,
                &b
            )
            .is_err()
        );
        let mut bad = bytes.clone();
        bad[25] = 2;
        assert!(
            decode_publication_payload(
                &bad,
                &d,
                p.definition_revision,
                p.interpretation_revision,
                &b
            )
            .is_err()
        );
        let mut bad = bytes;
        bad[34..36].copy_from_slice(&0u16.to_be_bytes());
        assert!(
            decode_publication_payload(
                &bad,
                &d,
                p.definition_revision,
                p.interpretation_revision,
                &b
            )
            .is_err()
        );
    }
    #[test]
    fn binding_rejects_unknown_duplicate_and_oversized_format_fields() {
        let (_, mut b, _) = fixture();
        b.data_versions[0].max_value_bytes = 257;
        assert!(encode_binding(&b).is_err());
        let (_, b, _) = fixture();
        let mut bytes = encode_binding(&b).unwrap().into_bytes();
        bytes.extend_from_slice(&[8, 1]);
        assert!(decode_binding(&bytes).is_err());
        assert!(decode_binding(&[8, 2]).is_err());
    }
}
