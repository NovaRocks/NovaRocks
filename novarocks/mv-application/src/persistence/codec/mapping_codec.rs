// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

use std::collections::BTreeSet;

use bytes::Bytes;
use novarocks_spi::connector::ProviderSourceType;
use prost::Message;

use super::{
    EncodedDocument, PersistenceCodecError, encode_message, enum_value, proto, required, wire,
};
use crate::persistence::identity::FieldIdentity;

pub const STORAGE_MAPPING_FORMAT_VERSION: u32 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageMappingDocument {
    pub columns: Vec<StorageColumnMapping>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageColumnMapping {
    pub output_ordinal: u32,
    pub storage_field_id: FieldIdentity,
    pub logical_type_identity: Bytes,
    pub storage_type: ProviderSourceType,
    pub storage_nullable: bool,
    pub provider_storage_declaration: Bytes,
    pub storage_format_version: u32,
    pub encode_identity: Bytes,
    pub decode_identity: Bytes,
    pub content_contract: Bytes,
    pub pushdown: MappingPushdownClass,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MappingPushdownClass {
    Identity,
    OrderPreservingInjective,
    Opaque,
}

pub fn encode_storage_mapping(
    mapping: &StorageMappingDocument,
) -> Result<EncodedDocument, PersistenceCodecError> {
    validate_mapping(mapping)?;
    encode_message(proto::StorageMappingDocument {
        format_version: Some(STORAGE_MAPPING_FORMAT_VERSION),
        columns: mapping.columns.iter().map(encode_column).collect(),
    })
}

pub fn decode_storage_mapping(
    bytes: &[u8],
) -> Result<StorageMappingDocument, PersistenceCodecError> {
    wire::preflight(bytes, wire::Schema::StorageMappingDocument)?;
    let value = proto::StorageMappingDocument::decode(bytes)
        .map_err(PersistenceCodecError::ProtobufDecode)?;
    let version = required(value.format_version, "mapping.format_version")?;
    if version != STORAGE_MAPPING_FORMAT_VERSION {
        return Err(PersistenceCodecError::UnknownFormatVersion {
            document: "mapping",
            version,
        });
    }
    let mapping = StorageMappingDocument {
        columns: value
            .columns
            .into_iter()
            .map(decode_column)
            .collect::<Result<_, _>>()?,
    };
    if encode_storage_mapping(&mapping)?.as_bytes() != bytes {
        return Err(invalid("non-canonical MV storage mapping"));
    }
    Ok(mapping)
}

fn validate_mapping(mapping: &StorageMappingDocument) -> Result<(), PersistenceCodecError> {
    if mapping.columns.is_empty() {
        return Err(invalid("MV storage mapping has no columns"));
    }
    let mut ids = BTreeSet::new();
    for (ordinal, column) in mapping.columns.iter().enumerate() {
        if column.output_ordinal as usize != ordinal
            || !ids.insert(&column.storage_field_id)
            || column.storage_format_version == 0
            || [
                &column.logical_type_identity,
                &column.provider_storage_declaration,
                &column.encode_identity,
                &column.decode_identity,
                &column.content_contract,
            ]
            .into_iter()
            .any(|identity| identity.is_empty())
        {
            return Err(invalid(
                "incomplete, duplicate, or unordered MV storage column binding",
            ));
        }
    }
    Ok(())
}

fn encode_column(column: &StorageColumnMapping) -> proto::StorageColumnMapping {
    proto::StorageColumnMapping {
        output_ordinal: Some(column.output_ordinal),
        storage_field_id: Some(column.storage_field_id.as_bytes().to_vec()),
        logical_type_identity: Some(column.logical_type_identity.to_vec()),
        storage_type_identity: Some(column.storage_type.canonical_bytes()),
        encode_identity: Some(column.encode_identity.to_vec()),
        decode_identity: Some(column.decode_identity.to_vec()),
        content_contract: Some(column.content_contract.to_vec()),
        pushdown: Some(match column.pushdown {
            MappingPushdownClass::Identity => 1,
            MappingPushdownClass::OrderPreservingInjective => 2,
            MappingPushdownClass::Opaque => 3,
        }),
        storage_nullable: Some(column.storage_nullable),
        provider_storage_declaration: Some(column.provider_storage_declaration.to_vec()),
        storage_format_version: Some(column.storage_format_version),
    }
}

fn decode_column(
    column: proto::StorageColumnMapping,
) -> Result<StorageColumnMapping, PersistenceCodecError> {
    let bytes = |value, field| Ok::<_, PersistenceCodecError>(Bytes::from(required(value, field)?));
    Ok(StorageColumnMapping {
        output_ordinal: required(column.output_ordinal, "mapping.output_ordinal")?,
        storage_field_id: FieldIdentity::try_new(required(
            column.storage_field_id,
            "mapping.storage_field_id",
        )?)?,
        logical_type_identity: bytes(
            column.logical_type_identity,
            "mapping.logical_type_identity",
        )?,
        storage_type: ProviderSourceType::from_canonical_bytes(&required(
            column.storage_type_identity,
            "mapping.storage_type_identity",
        )?)
        .map_err(|cause| PersistenceCodecError::MalformedWire(cause.to_string()))?,
        encode_identity: bytes(column.encode_identity, "mapping.encode_identity")?,
        decode_identity: bytes(column.decode_identity, "mapping.decode_identity")?,
        content_contract: bytes(column.content_contract, "mapping.content_contract")?,
        pushdown: enum_value(column.pushdown, "mapping.pushdown", |kind| {
            Some(match kind {
                1 => MappingPushdownClass::Identity,
                2 => MappingPushdownClass::OrderPreservingInjective,
                3 => MappingPushdownClass::Opaque,
                _ => return None,
            })
        })?,
        storage_nullable: required(column.storage_nullable, "mapping.storage_nullable")?,
        provider_storage_declaration: bytes(
            column.provider_storage_declaration,
            "mapping.provider_storage_declaration",
        )?,
        storage_format_version: required(
            column.storage_format_version,
            "mapping.storage_format_version",
        )?,
    })
}

fn invalid(detail: &str) -> PersistenceCodecError {
    PersistenceCodecError::MalformedWire(detail.into())
}
