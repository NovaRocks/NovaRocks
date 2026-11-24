// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

use bytes::Bytes;
use novarocks_spi::connector::{
    MvObservedStorageMapping, MvStorageColumnRequest, MvStoragePushdownClass,
};
use novarocks_sql::planning::mv::SqlMvCreatePersistenceFacts;
use std::collections::BTreeMap;

use super::codec::{
    InterpretationDocument, MappingPushdownClass, StorageColumnMapping, StorageMappingDocument,
    output_identity,
};
use super::documents::MvDocumentError;
use super::identity::FieldIdentity;

pub fn storage_mapping_requests(
    sql: &SqlMvCreatePersistenceFacts,
    interpretation: &InterpretationDocument,
) -> Result<Vec<MvStorageColumnRequest>, MvDocumentError> {
    let by_output = interpretation
        .outputs
        .iter()
        .map(|output| (&output.output_id, output))
        .collect::<BTreeMap<_, _>>();
    sql.outputs()
        .iter()
        .map(|output| {
            let identity = output_identity(output.output_ordinal(), output.canonical_bytes());
            let physical = by_output.get(&identity).ok_or_else(|| {
                MvDocumentError::Contract("MV output has no interpreted storage field".into())
            })?;
            Ok(MvStorageColumnRequest {
                output_ordinal: output.output_ordinal(),
                field_id: Bytes::copy_from_slice(physical.target_field_id.as_bytes()),
                logical_type: output.value_type().clone(),
            })
        })
        .collect()
}

pub fn build_storage_mapping(
    sql: &SqlMvCreatePersistenceFacts,
    requests: &[MvStorageColumnRequest],
    observed: Vec<MvObservedStorageMapping>,
) -> Result<StorageMappingDocument, MvDocumentError> {
    if sql.outputs().len() != requests.len() || sql.outputs().len() != observed.len() {
        return Err(MvDocumentError::Contract(
            "MV mapping does not cover all SQL outputs".into(),
        ));
    }
    let columns = sql
        .outputs()
        .iter()
        .zip(requests)
        .zip(observed)
        .map(|((output, requested), actual)| {
            if actual.output_ordinal != output.output_ordinal()
                || actual.output_ordinal != requested.output_ordinal
                || actual.field_id != requested.field_id
                || &actual.logical_type != output.value_type()
                || actual.logical_type != requested.logical_type
            {
                return Err(MvDocumentError::Contract(
                    "MV provider mapping differs from the exact output request".into(),
                ));
            }
            let declaration = actual.declaration;
            Ok(StorageColumnMapping {
                output_ordinal: actual.output_ordinal,
                storage_field_id: FieldIdentity::try_new(actual.field_id.to_vec())
                    .map_err(|error| MvDocumentError::Contract(error.to_string()))?,
                logical_type_identity: Bytes::copy_from_slice(output.canonical_bytes()),
                storage_type: actual.storage_type,
                storage_nullable: actual.storage_nullable,
                provider_storage_declaration: declaration.provider_declaration,
                storage_format_version: declaration.storage_format_version,
                encode_identity: declaration.encode_identity,
                decode_identity: declaration.decode_identity,
                content_contract: declaration.content_contract,
                pushdown: match actual.pushdown {
                    MvStoragePushdownClass::Identity => MappingPushdownClass::Identity,
                    MvStoragePushdownClass::OrderPreservingInjective => {
                        MappingPushdownClass::OrderPreservingInjective
                    }
                    MvStoragePushdownClass::Opaque => MappingPushdownClass::Opaque,
                },
            })
        })
        .collect::<Result<_, _>>()?;
    Ok(StorageMappingDocument { columns })
}
