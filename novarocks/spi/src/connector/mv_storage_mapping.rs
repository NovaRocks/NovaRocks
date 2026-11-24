// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

use bytes::Bytes;
use novarocks_type_contract::ValueTypeRef;

use super::{ConnectorCommittedVersion, ConnectorTableObjectId, ProviderSourceType};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvStorageColumnRequest {
    pub output_ordinal: u32,
    pub field_id: Bytes,
    pub logical_type: ValueTypeRef,
}

/// Immutable declarations issued by the ordinary reader and writer for an
/// exact staged or current field. Logical mappings are checked by the provider.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvStorageColumnDeclaration {
    pub read_type: ValueTypeRef,
    pub write_type: ValueTypeRef,
    pub provider_declaration: Bytes,
    pub storage_format_version: u32,
    pub encode_identity: Bytes,
    pub decode_identity: Bytes,
    pub content_contract: Bytes,
}

impl MvStorageColumnDeclaration {
    pub(crate) fn retained_identity_bytes(&self) -> usize {
        [
            &self.provider_declaration,
            &self.encode_identity,
            &self.decode_identity,
            &self.content_contract,
        ]
        .into_iter()
        .fold(0usize, |n, bytes| n.saturating_add(bytes.len()))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvStoragePushdownClass {
    Identity,
    OrderPreservingInjective,
    Opaque,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvObservedStorageMapping {
    pub output_ordinal: u32,
    pub field_id: Bytes,
    pub logical_type: ValueTypeRef,
    pub storage_type: ProviderSourceType,
    pub storage_nullable: bool,
    pub declaration: MvStorageColumnDeclaration,
    pub pushdown: MvStoragePushdownClass,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvCurrentStorageMapping {
    pub target_object: ConnectorTableObjectId,
    pub target_version: ConnectorCommittedVersion,
    pub columns: Vec<MvObservedStorageMapping>,
}
