// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

use bytes::Bytes;
use novarocks_spi::connector::{ConnectorMutationOperationId, ConnectorSnapshotRecord};
use prost::Message;

use super::{PersistenceCodecError, encode_message, proto, required, wire};
use crate::persistence::{identity::PublicationIdentity, keyed_documents::MV_DOCUMENT_OWNER};

pub const RECOVERY_RECORD_NAME: &str = "recovery";
const FORMAT_VERSION: u32 = 1;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum RecoverySource {
    Invalid {
        publication: PublicationIdentity,
    },
    Pending {
        publication: PublicationIdentity,
        attempt: [u8; 16],
    },
}

/// Evidence of the admitted obligations an explicit rebuild recovered from.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryRecord {
    pub recovered_from: Vec<RecoverySource>,
    pub operation: ConnectorMutationOperationId,
    pub recovered_at_ms: u64,
}

impl RecoveryRecord {
    pub fn encode(&self) -> Result<ConnectorSnapshotRecord, PersistenceCodecError> {
        let mut sources = self.recovered_from.iter().collect::<Vec<_>>();
        sources.sort_unstable();
        if sources.is_empty() || sources.windows(2).any(|window| window[0] == window[1]) {
            return Err(malformed("recovery sources must be nonempty and unique"));
        }
        let recovered_from = sources
            .into_iter()
            .map(|source| {
                let (publication, pending_attempt) = match source {
                    RecoverySource::Invalid { publication } => (publication, None),
                    RecoverySource::Pending {
                        publication,
                        attempt,
                    } => (publication, Some(attempt.to_vec())),
                };
                proto::RecoverySource {
                    publication: Some(publication.as_bytes().to_vec()),
                    pending_attempt,
                }
            })
            .collect();
        let payload = encode_message(proto::RecoveryRecord {
            format_version: Some(FORMAT_VERSION),
            operation: Some(self.operation.to_bytes().to_vec()),
            recovered_at_ms: Some(self.recovered_at_ms),
            recovered_from,
        })?;
        Ok(ConnectorSnapshotRecord {
            owner: MV_DOCUMENT_OWNER.into(),
            name: RECOVERY_RECORD_NAME.into(),
            binding: None,
            payload: Bytes::from(payload.into_bytes()),
        })
    }

    pub fn decode(record: &ConnectorSnapshotRecord) -> Result<Self, PersistenceCodecError> {
        if record.owner != MV_DOCUMENT_OWNER
            || record.name != RECOVERY_RECORD_NAME
            || record.binding.is_some()
        {
            return Err(malformed("invalid MV recovery record envelope"));
        }
        wire::preflight(&record.payload, wire::Schema::RecoveryRecord)?;
        let dto = proto::RecoveryRecord::decode(record.payload.as_ref())
            .map_err(PersistenceCodecError::ProtobufDecode)?;
        let version = required(dto.format_version, "recovery.format_version")?;
        if version != FORMAT_VERSION {
            return Err(PersistenceCodecError::UnknownFormatVersion {
                document: RECOVERY_RECORD_NAME,
                version,
            });
        }
        let operation = required(dto.operation, "recovery.operation")?
            .try_into()
            .map_err(|_| malformed("recovery operation identity must be 16 bytes"))?;
        let recovered_from = dto
            .recovered_from
            .into_iter()
            .map(|source| {
                let publication = PublicationIdentity::try_new(required(
                    source.publication,
                    "recovery.source.publication",
                )?)?;
                match source.pending_attempt {
                    Some(attempt) => Ok(RecoverySource::Pending {
                        publication,
                        attempt: attempt
                            .try_into()
                            .map_err(|_| malformed("recovery pending attempt must be 16 bytes"))?,
                    }),
                    None => Ok(RecoverySource::Invalid { publication }),
                }
            })
            .collect::<Result<Vec<_>, PersistenceCodecError>>()?;
        let value = Self {
            recovered_from,
            operation: ConnectorMutationOperationId::from_bytes(operation),
            recovered_at_ms: required(dto.recovered_at_ms, "recovery.recovered_at_ms")?,
        };
        if value.encode()? != *record {
            return Err(malformed("non-canonical MV recovery record"));
        }
        Ok(value)
    }
}

fn malformed(message: &'static str) -> PersistenceCodecError {
    PersistenceCodecError::MalformedWire(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovery_record_canonically_binds_distinct_obligations_to_the_operation() {
        let invalid = RecoverySource::Invalid {
            publication: PublicationIdentity::try_new(vec![2; 16]).unwrap(),
        };
        let pending = RecoverySource::Pending {
            publication: PublicationIdentity::try_new(vec![1; 16]).unwrap(),
            attempt: [4; 16],
        };
        let mut value = RecoveryRecord {
            recovered_from: vec![invalid.clone(), pending],
            operation: ConnectorMutationOperationId::from_bytes([3; 16]),
            recovered_at_ms: 123,
        };
        let record = value.encode().unwrap();
        assert_eq!(RecoveryRecord::decode(&record).unwrap(), value);
        value.recovered_from.reverse();
        assert_eq!(value.encode().unwrap(), record);
        value.recovered_from.push(invalid);
        assert!(value.encode().is_err());
    }
}
