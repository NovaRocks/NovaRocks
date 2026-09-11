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

//! Confidential query-attempt credential lease protocol values.
//!
//! Only descriptor metadata is manifest/digest material. The SPI confidential
//! envelope owns secret wrapping; this codec only validates and encodes the
//! TLS-only wire carrier.

use novarocks_proto_models::novarocks;
use novarocks_spi::connector::{
    CatalogCredentialMode, CatalogCredentialPurpose, CredentialConsumerRole,
    CredentialLeaseDescriptor, CredentialLeaseId, CredentialLeaseProvider,
    MAX_CREDENTIAL_LEASE_ID_BYTES, MAX_CREDENTIAL_LEASE_PREFIXES,
    MAX_CREDENTIAL_LEASE_SECRET_ENVELOPE_BYTES, MAX_CREDENTIAL_LEASE_SECRET_SCALAR_BYTES,
    MAX_CREDENTIAL_LEASES_PER_QUERY, StorageAccessDomainId, StorageCredentialScopePrefix,
};
use prost::Message;

use crate::catalog::{CatalogSet, decode_catalog_handle, encode_catalog_handle};
use crate::{FieldPath, ProtocolError, ProtocolErrorKind};

pub use novarocks_spi::connector::CredentialLeaseSecretEnvelope;

/// Parses and encodes only the descriptor portion of the credential contract.
pub fn encode_credential_lease_descriptor(
    descriptor: &CredentialLeaseDescriptor,
) -> novarocks::CredentialLeaseDescriptor {
    novarocks::CredentialLeaseDescriptor {
        lease_id: descriptor.lease_id().as_bytes().to_vec(),
        epoch: descriptor.epoch(),
        owner: Some(encode_catalog_handle(descriptor.owner())),
        provider: match descriptor.provider() {
            CredentialLeaseProvider::S3 => novarocks::CredentialLeaseProvider::S3 as i32,
        },
        prefixes: descriptor
            .prefixes()
            .iter()
            .map(|prefix| prefix.as_str().to_owned())
            .collect(),
        not_after_unix_ms: descriptor.not_after_unix_ms(),
        refresh_capable: descriptor.refresh_capable(),
        storage_access_domain_id: descriptor.storage_access_domain_id().as_bytes().to_vec(),
    }
}

pub fn decode_credential_lease_descriptor(
    raw: novarocks::CredentialLeaseDescriptor,
    root: FieldPath,
) -> Result<CredentialLeaseDescriptor, ProtocolError> {
    let lease_id = decode_lease_id(&raw.lease_id, root.clone().field("lease_id"))?;
    if raw.epoch == 0 {
        return Err(invalid(
            root.clone().field("epoch"),
            "credential lease epoch must be nonzero",
        ));
    }
    let owner = raw.owner.ok_or_else(|| {
        missing(
            root.clone().field("owner"),
            "credential lease owner is required",
        )
    })?;
    let owner = decode_catalog_handle(owner, root.clone().field("owner"))?;
    let provider = match novarocks::CredentialLeaseProvider::try_from(raw.provider) {
        Ok(novarocks::CredentialLeaseProvider::S3) => CredentialLeaseProvider::S3,
        _ => {
            return Err(invalid(
                root.clone().field("provider"),
                "credential lease provider must be S3",
            ));
        }
    };
    if raw.prefixes.is_empty() || raw.prefixes.len() > MAX_CREDENTIAL_LEASE_PREFIXES {
        return Err(resource_exhausted(
            root.clone().field("prefixes"),
            "credential lease prefixes must contain 1..=64 entries",
        ));
    }
    let mut prefixes = Vec::with_capacity(raw.prefixes.len());
    for (index, value) in raw.prefixes.iter().enumerate() {
        let prefix = StorageCredentialScopePrefix::try_from_normalized(value).map_err(|error| {
            invalid(
                root.clone().field("prefixes").index(index),
                format!("invalid canonical S3 credential prefix: {error}"),
            )
        })?;
        if prefixes
            .last()
            .is_some_and(|previous: &StorageCredentialScopePrefix| previous >= &prefix)
        {
            return Err(invalid(
                root.clone().field("prefixes").index(index),
                "credential lease prefixes must be strictly sorted and unique",
            ));
        }
        prefixes.push(prefix);
    }
    if raw.not_after_unix_ms == 0 {
        return Err(invalid(
            root.clone().field("not_after_unix_ms"),
            "credential lease expiration must be nonzero",
        ));
    }
    let domain: [u8; 32] = raw.storage_access_domain_id.try_into().map_err(|_| {
        invalid(
            root.clone().field("storage_access_domain_id"),
            "storage access domain id must contain exactly 32 bytes",
        )
    })?;
    CredentialLeaseDescriptor::try_new(
        lease_id,
        raw.epoch,
        owner,
        provider,
        prefixes,
        raw.not_after_unix_ms,
        raw.refresh_capable,
        StorageAccessDomainId::from_bytes(domain),
    )
    .map_err(|error| invalid(root, error.to_string()))
}

/// Validates one ordered descriptor contribution against the exact catalog set
/// frozen in the same participant manifest.
pub fn validate_credential_lease_descriptors(
    descriptors: &[novarocks::CredentialLeaseDescriptor],
    catalog_set: &CatalogSet,
    root: FieldPath,
) -> Result<(), ProtocolError> {
    if descriptors.len() > MAX_CREDENTIAL_LEASES_PER_QUERY {
        return Err(resource_exhausted(
            root.field("credential_lease_descriptors"),
            "credential lease contribution exceeds 64 entries",
        ));
    }
    let catalogs = catalog_set.catalogs()?;
    let mut previous = None;
    for (index, raw) in descriptors.iter().cloned().enumerate() {
        let path = root
            .clone()
            .field("credential_lease_descriptors")
            .index(index);
        let descriptor = decode_credential_lease_descriptor(raw, path.clone())?;
        let current = descriptor.lease_id();
        if previous.is_some_and(|previous: CredentialLeaseId| previous >= current) {
            return Err(invalid(
                path.field("lease_id"),
                "credential lease descriptors must be strictly sorted and unique by lease id",
            ));
        }
        previous = Some(current);
        let owner = catalogs
            .iter()
            .find(|properties| properties.handle() == descriptor.owner())
            .ok_or_else(|| {
                invalid(
                    path.clone().field("owner"),
                    "credential lease owner is not present in the participant catalog set",
                )
            })?;
        let has_vended_data_binding = owner.credential_bindings().iter().any(|binding| {
            binding.purpose() == CatalogCredentialPurpose::ObjectStoreData
                && binding.consumer_role() == CredentialConsumerRole::Backend
                && matches!(binding.mode(), CatalogCredentialMode::Vended)
        });
        if !has_vended_data_binding {
            return Err(invalid(
                path.field("owner"),
                "credential lease owner does not declare vended object-store data credentials",
            ));
        }
    }
    Ok(())
}

/// Decodes one TLS-only confidential wire envelope. Validation happens before
/// its scalar values are transferred to SPI's redacted secret wrapper.
pub fn decode_credential_lease_secret_envelope(
    raw: novarocks::CredentialLeaseSecretEnvelope,
    root: FieldPath,
) -> Result<CredentialLeaseSecretEnvelope, ProtocolError> {
    if raw.encoded_len() > MAX_CREDENTIAL_LEASE_SECRET_ENVELOPE_BYTES {
        return Err(resource_exhausted(
            root,
            "credential lease secret envelope exceeds 256 KiB",
        ));
    }
    let lease_id = decode_lease_id(&raw.lease_id, root.clone().field("lease_id"))?;
    let material = raw.s3.ok_or_else(|| {
        missing(
            root.clone().field("s3"),
            "credential lease S3 material is required",
        )
    })?;
    validate_secret_scalar(
        &material.access_key_id,
        root.clone().field("s3").field("access_key_id"),
    )?;
    validate_secret_scalar(
        &material.secret_access_key,
        root.clone().field("s3").field("secret_access_key"),
    )?;
    validate_secret_scalar(
        &material.session_token,
        root.clone().field("s3").field("session_token"),
    )?;
    CredentialLeaseSecretEnvelope::try_new_from_wire_scalars(
        lease_id,
        raw.epoch,
        material.access_key_id,
        material.secret_access_key,
        material.session_token,
        material.session_token_expires_at_unix_ms,
    )
    .map_err(|error| invalid(root, error.to_string()))
}

/// Encodes one SPI-owned confidential envelope for a previously authenticated
/// TLS lifecycle carrier.
pub fn encode_credential_lease_secret_envelope(
    envelope: &CredentialLeaseSecretEnvelope,
) -> novarocks::CredentialLeaseSecretEnvelope {
    let (access_key_id, secret_access_key, session_token) = envelope.s3_secret_scalars();
    novarocks::CredentialLeaseSecretEnvelope {
        lease_id: envelope.lease_id().as_bytes().to_vec(),
        epoch: envelope.epoch(),
        s3: Some(novarocks::CredentialLeaseS3SecretMaterial {
            access_key_id: access_key_id.to_owned(),
            secret_access_key: secret_access_key.to_owned(),
            session_token: session_token.to_owned(),
            session_token_expires_at_unix_ms: envelope.session_token_expires_at_unix_ms(),
        }),
    }
}

/// Validates exact InitQuery descriptor/value pairing without placing values in
/// a digest. The caller is still responsible for checking the actual native
/// transport is TLS before accepting these values.
pub fn validate_initial_credential_lease_envelopes(
    descriptors: &[novarocks::CredentialLeaseDescriptor],
    envelopes: &[novarocks::CredentialLeaseSecretEnvelope],
    root: FieldPath,
) -> Result<(), ProtocolError> {
    if envelopes.len() > MAX_CREDENTIAL_LEASES_PER_QUERY {
        return Err(resource_exhausted(
            root.field("credential_lease_envelopes"),
            "credential lease envelope contribution exceeds 64 entries",
        ));
    }
    if descriptors.len() != envelopes.len() {
        return Err(invalid(
            root.field("credential_lease_envelopes"),
            "credential lease descriptors and confidential envelopes must have identical cardinality",
        ));
    }
    let mut total_encoded_bytes = 0usize;
    let mut previous = None;
    for (index, raw) in envelopes.iter().cloned().enumerate() {
        total_encoded_bytes = total_encoded_bytes.saturating_add(raw.encoded_len());
        if total_encoded_bytes > MAX_CREDENTIAL_LEASE_SECRET_ENVELOPE_BYTES {
            return Err(resource_exhausted(
                root.clone().field("credential_lease_envelopes"),
                "credential lease secret envelopes exceed 256 KiB",
            ));
        }
        let path = root
            .clone()
            .field("credential_lease_envelopes")
            .index(index);
        let envelope = decode_credential_lease_secret_envelope(raw, path.clone())?;
        if previous.is_some_and(|previous: CredentialLeaseId| previous >= envelope.lease_id()) {
            return Err(invalid(
                path.field("lease_id"),
                "credential lease envelopes must be strictly sorted and unique by lease id",
            ));
        }
        previous = Some(envelope.lease_id());
        let descriptor = descriptors
            .get(index)
            .cloned()
            .ok_or_else(|| invalid(path.clone(), "credential lease descriptor is missing"))?;
        let descriptor = decode_credential_lease_descriptor(
            descriptor,
            root.clone()
                .field("credential_lease_descriptors")
                .index(index),
        )?;
        if !envelope.matches_descriptor(&descriptor) {
            return Err(invalid(
                path,
                "credential lease envelope does not exactly match its descriptor",
            ));
        }
    }
    Ok(())
}

pub fn decode_lease_id(raw: &[u8], root: FieldPath) -> Result<CredentialLeaseId, ProtocolError> {
    let bytes: [u8; MAX_CREDENTIAL_LEASE_ID_BYTES] = raw.try_into().map_err(|_| {
        invalid(
            root.clone(),
            "credential lease id must contain exactly 16 bytes",
        )
    })?;
    CredentialLeaseId::try_from_bytes(bytes).map_err(|error| invalid(root, error.to_string()))
}

pub fn validate_lease_epoch(
    lease_id: &[u8],
    epoch: u64,
    root: FieldPath,
) -> Result<(CredentialLeaseId, u64), ProtocolError> {
    let lease_id = decode_lease_id(lease_id, root.clone().field("lease_id"))?;
    if epoch == 0 {
        return Err(invalid(
            root.field("epoch"),
            "credential lease epoch must be nonzero",
        ));
    }
    Ok((lease_id, epoch))
}

fn validate_secret_scalar(value: &str, root: FieldPath) -> Result<(), ProtocolError> {
    if value.is_empty() || value.len() > MAX_CREDENTIAL_LEASE_SECRET_SCALAR_BYTES {
        return Err(invalid(
            root,
            "credential lease secret scalar must contain 1..=8192 UTF-8 bytes",
        ));
    }
    Ok(())
}

fn invalid(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::InvalidValue, detail)
}

fn missing(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::InvalidValue, detail)
}

fn resource_exhausted(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::Capacity, detail)
}

#[cfg(test)]
mod tests {
    use super::{
        CredentialLeaseSecretEnvelope, decode_credential_lease_descriptor,
        decode_credential_lease_secret_envelope, encode_credential_lease_descriptor,
        encode_credential_lease_secret_envelope, validate_initial_credential_lease_envelopes,
    };
    use crate::FieldPath;
    use novarocks_proto_models::novarocks;
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorInstanceId, CredentialLeaseDescriptor,
        CredentialLeaseId, CredentialLeaseProvider, StorageAccessDomainId,
        StorageCredentialScopePrefix,
    };

    fn descriptor() -> CredentialLeaseDescriptor {
        CredentialLeaseDescriptor::try_new(
            CredentialLeaseId::try_from_bytes([1; 16]).expect("lease"),
            3,
            CatalogHandle::new(
                ConnectorInstanceId::parse("warehouse").expect("catalog"),
                CatalogVersion::from_bytes([7; 32]),
            ),
            CredentialLeaseProvider::S3,
            vec![
                StorageCredentialScopePrefix::try_from_normalized("s3://bucket/data")
                    .expect("prefix"),
            ],
            99,
            true,
            StorageAccessDomainId::from_bytes([8; 32]),
        )
        .expect("descriptor")
    }

    fn envelope(value: &str) -> CredentialLeaseSecretEnvelope {
        CredentialLeaseSecretEnvelope::try_new_from_wire_scalars(
            CredentialLeaseId::try_from_bytes([1; 16]).expect("lease"),
            3,
            "access-canary".to_owned(),
            value.to_owned(),
            "token-canary".to_owned(),
            99,
        )
        .expect("envelope")
    }

    #[test]
    fn descriptor_round_trip_has_no_secret_material() {
        let raw = encode_credential_lease_descriptor(&descriptor());
        let decoded =
            decode_credential_lease_descriptor(raw, FieldPath::root("credential_lease_descriptor"))
                .expect("descriptor");
        assert_eq!(decoded, descriptor());
        assert!(!format!("{decoded:?}").contains("canary"));
    }

    #[test]
    fn envelope_debug_redacts_and_exact_init_pairing_rejects_different_value() {
        let first = envelope("secret-canary-a");
        let second = envelope("secret-canary-b");
        let rendered = format!("{first:?}");
        assert!(!rendered.contains("secret-canary-a"));
        assert!(rendered.contains("[REDACTED]"));
        assert_ne!(first, second);
        let encoded_descriptor = encode_credential_lease_descriptor(&descriptor());
        validate_initial_credential_lease_envelopes(
            &[encoded_descriptor],
            &[encode_credential_lease_secret_envelope(&first)],
            FieldPath::root("init_query_request"),
        )
        .expect("exact pairing");
        let mut mismatched = encode_credential_lease_secret_envelope(&second);
        mismatched.epoch = 4;
        assert!(
            validate_initial_credential_lease_envelopes(
                &[encode_credential_lease_descriptor(&descriptor())],
                &[mismatched],
                FieldPath::root("init_query_request"),
            )
            .is_err()
        );
    }

    #[test]
    fn envelope_rejects_missing_or_oversized_s3_scalars() {
        let error = decode_credential_lease_secret_envelope(
            novarocks::CredentialLeaseSecretEnvelope {
                lease_id: vec![1; 16],
                epoch: 3,
                s3: Some(novarocks::CredentialLeaseS3SecretMaterial {
                    access_key_id: String::new(),
                    secret_access_key: "secret".to_owned(),
                    session_token: "token".to_owned(),
                    session_token_expires_at_unix_ms: 1,
                }),
            },
            FieldPath::root("credential_lease_secret_envelope"),
        )
        .expect_err("empty scalar rejects");
        assert!(error.detail().contains("secret scalar"));
    }
}
