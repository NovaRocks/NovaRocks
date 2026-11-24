// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

use bytes::Bytes;
use novarocks_spi::connector::{ConnectorMutationOperationId, ConnectorSnapshotRecord};
use prost::Message;

use super::{PersistenceCodecError, encode_message, proto, required, wire};
use crate::persistence::{
    identity::{DocumentRevision, PublicationIdentity},
    keyed_documents::{MV_DOCUMENT_OWNER, snapshot_record_frame},
};

pub const RETIREMENT_RECORD_NAME: &str = "retirement";
const FORMAT_VERSION: u32 = 1;

/// One exact retirement, preserving the diagnostic counters from RWM-9.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetirementRecord {
    pub retired_publication: PublicationIdentity,
    pub operation: ConnectorMutationOperationId,
    pub requested: i64,
    pub matched: i64,
    pub insufficient_keys: i64,
    pub content_fingerprint: Option<[u8; 16]>,
}

impl RetirementRecord {
    pub fn encode(&self) -> Result<ConnectorSnapshotRecord, PersistenceCodecError> {
        if self.retired_publication.as_bytes().len() != 16 {
            return Err(malformed("retired publication identity must be 16 bytes"));
        }
        if [self.requested, self.matched, self.insufficient_keys]
            .into_iter()
            .any(|count| count < 0)
        {
            return Err(malformed("negative retirement evidence count"));
        }
        let record = proto::RetirementRecord {
            format_version: Some(FORMAT_VERSION),
            retired_publication: Some(self.retired_publication.as_bytes().to_vec()),
            operation: Some(self.operation.to_bytes().to_vec()),
            requested: Some(self.requested as u64),
            matched: Some(self.matched as u64),
            insufficient_keys: Some(self.insufficient_keys as u64),
            content_fingerprint: self.content_fingerprint.map(|bytes| bytes.to_vec()),
        };
        Ok(ConnectorSnapshotRecord {
            owner: MV_DOCUMENT_OWNER.into(),
            name: RETIREMENT_RECORD_NAME.into(),
            binding: None,
            payload: Bytes::from(encode_message(record)?.into_bytes()),
        })
    }

    pub fn decode(record: &ConnectorSnapshotRecord) -> Result<Self, PersistenceCodecError> {
        if record.owner != MV_DOCUMENT_OWNER
            || record.name != RETIREMENT_RECORD_NAME
            || record.binding.is_some()
        {
            return Err(malformed("invalid MV retirement record envelope"));
        }
        wire::preflight(&record.payload, wire::Schema::RetirementRecord)?;
        let dto = proto::RetirementRecord::decode(record.payload.as_ref())
            .map_err(PersistenceCodecError::ProtobufDecode)?;
        let version = required(dto.format_version, "retirement.format_version")?;
        if version != FORMAT_VERSION {
            return Err(PersistenceCodecError::UnknownFormatVersion {
                document: RETIREMENT_RECORD_NAME,
                version,
            });
        }
        let operation = required(dto.operation, "retirement.operation")?
            .try_into()
            .map_err(|_| malformed("retirement operation identity must be 16 bytes"))?;
        let signed = |value, field| {
            i64::try_from(required(value, field)?)
                .map_err(|_| malformed("retirement count exceeds Int64"))
        };
        let content_fingerprint = dto
            .content_fingerprint
            .map(|bytes| {
                bytes
                    .try_into()
                    .map_err(|_| malformed("retirement fingerprint must be 16 bytes"))
            })
            .transpose()?;
        let value = Self {
            retired_publication: PublicationIdentity::try_new(required(
                dto.retired_publication,
                "retirement.retired_publication",
            )?)?,
            operation: ConnectorMutationOperationId::from_bytes(operation),
            requested: signed(dto.requested, "retirement.requested")?,
            matched: signed(dto.matched, "retirement.matched")?,
            insufficient_keys: signed(dto.insufficient_keys, "retirement.insufficient_keys")?,
            content_fingerprint,
        };
        if value.encode()? != *record {
            return Err(malformed("non-canonical MV retirement record"));
        }
        Ok(value)
    }

    pub fn revision(&self) -> Result<DocumentRevision, PersistenceCodecError> {
        Ok(DocumentRevision::from_canonical_bytes(
            &snapshot_record_frame(&self.encode()?),
        ))
    }
}

fn malformed(message: &'static str) -> PersistenceCodecError {
    PersistenceCodecError::MalformedWire(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_identity_separates_equal_retirement_outcomes() {
        let first = RetirementRecord {
            retired_publication: PublicationIdentity::try_new(vec![2; 16]).unwrap(),
            operation: ConnectorMutationOperationId::from_bytes([1; 16]),
            requested: 100,
            matched: 80,
            insufficient_keys: 10,
            content_fingerprint: Some([6; 16]),
        };
        let g = first.encode().unwrap();
        assert_eq!(RetirementRecord::decode(&g).unwrap(), first);
        assert_eq!(RetirementRecord::decode(&g).unwrap().encode().unwrap(), g);
        let second = RetirementRecord {
            operation: ConnectorMutationOperationId::from_bytes([2; 16]),
            ..first.clone()
        };
        assert_ne!(first.revision().unwrap(), second.revision().unwrap());
        let invalid = RetirementRecord {
            matched: -1,
            ..second
        };
        assert!(invalid.encode().is_err());
    }
}
