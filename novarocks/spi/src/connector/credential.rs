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

//! Secret-free catalog credential identity and storage authorization domains.
//!
//! Credential values are deliberately absent from this module. Catalog desired
//! state and native wire contracts may carry these bounded identities, while
//! role-local composition remains the only owner of long-lived secret material.

use std::sync::Arc;

use sha2::{Digest, Sha256};

use super::{ConnectorError, ConnectorErrorKind, ConnectorInstanceId, ConnectorProviderId};

pub const MAX_CATALOG_CREDENTIAL_REFERENCE_BYTES: usize = 128;
pub const MAX_CATALOG_CREDENTIAL_BINDINGS: usize = 16;
pub const MAX_CATALOG_NON_SECRET_PROPERTIES: usize = 128;
pub const MAX_CATALOG_NON_SECRET_PROPERTY_KEY_BYTES: usize = 256;
pub const MAX_CATALOG_NON_SECRET_PROPERTY_VALUE_BYTES: usize = 4 * 1024;
pub const MAX_STORAGE_CREDENTIAL_SCOPE_PREFIXES: usize = 64;
pub const MAX_STORAGE_CREDENTIAL_SCOPE_PREFIX_BYTES: usize = 2 * 1024;

const CREDENTIAL_BINDING_DOMAIN: &[u8] = b"novarocks.catalog.credential.bindings.v1\0";
const STORAGE_ACCESS_DOMAIN: &[u8] = b"novarocks.storage.access-domain.v2\0";

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CatalogCredentialPurpose {
    /// Frontend access to a provider's catalog control API.
    CatalogControl,
    /// Backend access to table data for one execution attempt.
    ObjectStoreData,
    /// Frontend access to metadata, manifests, and statistics files.
    ObjectStoreMetadata,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CredentialConsumerRole {
    Frontend,
    Backend,
    FrontendAndBackend,
}

/// Immutable operator-managed identity for one role-local secret value.
///
/// The generation is part of catalog identity. Replacing the value behind the
/// same `(name, generation)` is a deployment contract violation.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StaticCredentialReference {
    name: Arc<str>,
    generation: Arc<str>,
}

impl StaticCredentialReference {
    pub fn try_new(name: &str, generation: &str) -> Result<Self, ConnectorError> {
        Ok(Self {
            name: parse_reference_component("catalog credential name", name)?,
            generation: parse_reference_component("catalog credential generation", generation)?,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn generation(&self) -> &str {
        &self.generation
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CatalogCredentialMode {
    Static(StaticCredentialReference),
    Vended,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CatalogCredentialBinding {
    purpose: CatalogCredentialPurpose,
    consumer_role: CredentialConsumerRole,
    mode: CatalogCredentialMode,
}

impl CatalogCredentialBinding {
    pub fn try_new(
        purpose: CatalogCredentialPurpose,
        consumer_role: CredentialConsumerRole,
        mode: CatalogCredentialMode,
    ) -> Result<Self, ConnectorError> {
        let valid = matches!(
            (&purpose, &consumer_role, &mode),
            (
                CatalogCredentialPurpose::CatalogControl,
                CredentialConsumerRole::Frontend,
                CatalogCredentialMode::Static(_)
            ) | (
                CatalogCredentialPurpose::ObjectStoreData,
                CredentialConsumerRole::Backend,
                CatalogCredentialMode::Static(_) | CatalogCredentialMode::Vended
            ) | (
                CatalogCredentialPurpose::ObjectStoreMetadata,
                CredentialConsumerRole::Frontend,
                CatalogCredentialMode::Static(_)
            )
        );
        if !valid {
            return Err(invalid(
                "catalog credential purpose, role, and mode combination",
            ));
        }
        Ok(Self {
            purpose,
            consumer_role,
            mode,
        })
    }

    pub const fn purpose(&self) -> CatalogCredentialPurpose {
        self.purpose
    }

    pub const fn consumer_role(&self) -> CredentialConsumerRole {
        self.consumer_role
    }

    pub const fn mode(&self) -> &CatalogCredentialMode {
        &self.mode
    }

    pub fn static_reference(&self) -> Option<&StaticCredentialReference> {
        match &self.mode {
            CatalogCredentialMode::Static(reference) => Some(reference),
            CatalogCredentialMode::Vended => None,
        }
    }
}

/// Validate a complete binding set and return its unique canonical order.
pub fn canonicalize_catalog_credential_bindings(
    mut bindings: Vec<CatalogCredentialBinding>,
) -> Result<Vec<CatalogCredentialBinding>, ConnectorError> {
    if bindings.len() > MAX_CATALOG_CREDENTIAL_BINDINGS {
        return Err(exhausted("catalog credential binding set"));
    }
    bindings.sort();
    if bindings
        .windows(2)
        .any(|pair| pair[0].purpose == pair[1].purpose)
    {
        return Err(invalid("duplicate catalog credential purpose"));
    }
    Ok(bindings)
}

/// Stable bytes for catalog definition/version hashing and persistence fixtures.
pub fn canonical_catalog_credential_binding_bytes(
    bindings: &[CatalogCredentialBinding],
) -> Result<Vec<u8>, ConnectorError> {
    let bindings = canonicalize_catalog_credential_bindings(bindings.to_vec())?;
    let mut output = Vec::with_capacity(CREDENTIAL_BINDING_DOMAIN.len() + bindings.len() * 32);
    output.extend_from_slice(CREDENTIAL_BINDING_DOMAIN);
    put_count(&mut output, bindings.len());
    for binding in bindings {
        output.push(match binding.purpose {
            CatalogCredentialPurpose::CatalogControl => 0,
            CatalogCredentialPurpose::ObjectStoreData => 1,
            CatalogCredentialPurpose::ObjectStoreMetadata => 2,
        });
        output.push(match binding.consumer_role {
            CredentialConsumerRole::Frontend => 0,
            CredentialConsumerRole::Backend => 1,
            CredentialConsumerRole::FrontendAndBackend => 2,
        });
        match binding.mode {
            CatalogCredentialMode::Static(reference) => {
                output.push(0);
                put_bytes(&mut output, reference.name.as_bytes());
                put_bytes(&mut output, reference.generation.as_bytes());
            }
            CatalogCredentialMode::Vended => output.push(1),
        }
    }
    Ok(output)
}

/// Provider-declared catalog property that is safe to persist and hash.
///
/// The constructor deliberately accepts string slices rather than a generic
/// string conversion, so redacted secret wrappers cannot be passed by accident.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CatalogNonSecretProperty {
    key: Arc<str>,
    value: Arc<str>,
}

impl CatalogNonSecretProperty {
    pub fn try_new(key: &str, value: &str) -> Result<Self, ConnectorError> {
        if !is_property_key(key) || is_sensitive_property_key(key) {
            return Err(invalid("catalog non-secret property key"));
        }
        if value.is_empty() || value.len() > MAX_CATALOG_NON_SECRET_PROPERTY_VALUE_BYTES {
            return Err(invalid("catalog non-secret property value"));
        }
        if let Ok(url) = url::Url::parse(value)
            && (!url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some())
        {
            return Err(invalid("catalog non-secret property URL"));
        }
        Ok(Self {
            key: Arc::from(key),
            value: Arc::from(value),
        })
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

/// Provider-normalized non-secret prefix scope for a vended credential.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StorageCredentialScopePrefix(Arc<str>);

impl StorageCredentialScopePrefix {
    pub fn try_from_normalized(value: &str) -> Result<Self, ConnectorError> {
        if value.is_empty()
            || value.len() > MAX_STORAGE_CREDENTIAL_SCOPE_PREFIX_BYTES
            || !value.is_ascii()
            || value.bytes().any(|byte| byte.is_ascii_whitespace())
            || value
                .bytes()
                .any(|byte| matches!(byte, b'?' | b'#' | b'\\'))
        {
            return Err(invalid("normalized storage credential scope prefix"));
        }
        let parsed = url::Url::parse(value)
            .map_err(|_| invalid("normalized storage credential scope prefix"))?;
        if parsed.scheme() != "s3"
            || parsed.host_str().is_none_or(str::is_empty)
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.port().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
            || parsed.as_str() != value
        {
            return Err(invalid("normalized storage credential scope prefix"));
        }
        Ok(Self(Arc::from(value)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Complete, source-neutral input for one stable storage authorization domain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogStorageAccessDomainInput {
    provider_id: ConnectorProviderId,
    catalog_name: ConnectorInstanceId,
    config_format_version: u32,
    non_secret_properties: Vec<CatalogNonSecretProperty>,
    storage_scope: StorageAccessScope,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CatalogUncredentialedStorageKind {
    Local,
    Hdfs,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum StorageAccessScope {
    Static {
        purpose: CatalogCredentialPurpose,
        consumer_role: CredentialConsumerRole,
        reference: StaticCredentialReference,
    },
    Vended {
        purpose: CatalogCredentialPurpose,
        consumer_role: CredentialConsumerRole,
        prefixes: Vec<StorageCredentialScopePrefix>,
    },
    Uncredentialed {
        kind: CatalogUncredentialedStorageKind,
        authority: Option<Arc<str>>,
    },
}

impl CatalogStorageAccessDomainInput {
    pub fn try_new(
        provider_id: ConnectorProviderId,
        catalog_name: ConnectorInstanceId,
        config_format_version: u32,
        non_secret_properties: Vec<CatalogNonSecretProperty>,
        storage_binding: CatalogCredentialBinding,
        mut vended_prefixes: Vec<StorageCredentialScopePrefix>,
    ) -> Result<Self, ConnectorError> {
        if !matches!(
            (storage_binding.purpose, storage_binding.consumer_role),
            (
                CatalogCredentialPurpose::ObjectStoreData,
                CredentialConsumerRole::Backend
            ) | (
                CatalogCredentialPurpose::ObjectStoreMetadata,
                CredentialConsumerRole::Frontend
            )
        ) {
            return Err(invalid("storage credential binding"));
        }
        if vended_prefixes.len() > MAX_STORAGE_CREDENTIAL_SCOPE_PREFIXES {
            return Err(exhausted("storage credential scope prefix set"));
        }
        vended_prefixes.sort();
        if vended_prefixes.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(invalid("duplicate storage credential scope prefix"));
        }
        let purpose = storage_binding.purpose;
        let consumer_role = storage_binding.consumer_role;
        let storage_scope = match storage_binding.mode {
            CatalogCredentialMode::Static(reference) if vended_prefixes.is_empty() => {
                StorageAccessScope::Static {
                    purpose,
                    consumer_role,
                    reference,
                }
            }
            CatalogCredentialMode::Vended if !vended_prefixes.is_empty() => {
                StorageAccessScope::Vended {
                    purpose,
                    consumer_role,
                    prefixes: vended_prefixes,
                }
            }
            CatalogCredentialMode::Static(_) => {
                return Err(invalid("static storage binding with vended prefix scope"));
            }
            CatalogCredentialMode::Vended => {
                return Err(invalid("vended storage binding without prefix scope"));
            }
        };

        let mut input = Self::try_new_common(
            provider_id,
            catalog_name,
            config_format_version,
            non_secret_properties,
        )?;
        input.storage_scope = storage_scope;
        Ok(input)
    }

    pub fn try_new_uncredentialed(
        provider_id: ConnectorProviderId,
        catalog_name: ConnectorInstanceId,
        config_format_version: u32,
        non_secret_properties: Vec<CatalogNonSecretProperty>,
        kind: CatalogUncredentialedStorageKind,
        authority: Option<&str>,
    ) -> Result<Self, ConnectorError> {
        let authority = authority.map(parse_storage_authority).transpose()?;
        match kind {
            CatalogUncredentialedStorageKind::Local if authority.is_some() => {
                return Err(invalid("local storage authority"));
            }
            CatalogUncredentialedStorageKind::Hdfs if authority.is_none() => {
                return Err(invalid("HDFS storage authority"));
            }
            CatalogUncredentialedStorageKind::Local | CatalogUncredentialedStorageKind::Hdfs => {}
        }
        let mut input = Self::try_new_common(
            provider_id,
            catalog_name,
            config_format_version,
            non_secret_properties,
        )?;
        input.storage_scope = StorageAccessScope::Uncredentialed { kind, authority };
        Ok(input)
    }

    fn try_new_common(
        provider_id: ConnectorProviderId,
        catalog_name: ConnectorInstanceId,
        config_format_version: u32,
        mut non_secret_properties: Vec<CatalogNonSecretProperty>,
    ) -> Result<Self, ConnectorError> {
        if config_format_version == 0 {
            return Err(invalid("catalog config format version"));
        }
        if non_secret_properties.len() > MAX_CATALOG_NON_SECRET_PROPERTIES {
            return Err(exhausted("catalog non-secret property set"));
        }
        non_secret_properties.sort();
        if non_secret_properties
            .windows(2)
            .any(|pair| pair[0].key == pair[1].key)
        {
            return Err(invalid("duplicate catalog non-secret property key"));
        }
        Ok(Self {
            provider_id,
            catalog_name,
            config_format_version,
            non_secret_properties,
            storage_scope: StorageAccessScope::Uncredentialed {
                kind: CatalogUncredentialedStorageKind::Local,
                authority: None,
            },
        })
    }

    pub fn derive_access_domain(&self) -> StorageAccessDomainId {
        let mut digest = Sha256::new();
        digest.update(STORAGE_ACCESS_DOMAIN);
        hash_bytes(&mut digest, self.provider_id.as_str().as_bytes());
        hash_bytes(&mut digest, self.catalog_name.as_str().as_bytes());
        digest.update(self.config_format_version.to_le_bytes());
        hash_count(&mut digest, self.non_secret_properties.len());
        for property in &self.non_secret_properties {
            hash_bytes(&mut digest, property.key.as_bytes());
            hash_bytes(&mut digest, property.value.as_bytes());
        }
        match &self.storage_scope {
            StorageAccessScope::Static {
                purpose,
                consumer_role,
                reference,
            } => {
                digest.update([0]);
                digest.update([credential_purpose_discriminant(*purpose)]);
                digest.update([consumer_role_discriminant(*consumer_role)]);
                hash_bytes(&mut digest, reference.name.as_bytes());
                hash_bytes(&mut digest, reference.generation.as_bytes());
            }
            StorageAccessScope::Vended {
                purpose,
                consumer_role,
                prefixes,
            } => {
                digest.update([1]);
                digest.update([credential_purpose_discriminant(*purpose)]);
                digest.update([consumer_role_discriminant(*consumer_role)]);
                hash_count(&mut digest, prefixes.len());
                for prefix in prefixes {
                    hash_bytes(&mut digest, prefix.0.as_bytes());
                }
            }
            StorageAccessScope::Uncredentialed { kind, authority } => {
                digest.update([match kind {
                    CatalogUncredentialedStorageKind::Local => 2,
                    CatalogUncredentialedStorageKind::Hdfs => 3,
                }]);
                match authority {
                    Some(authority) => {
                        digest.update([1]);
                        hash_bytes(&mut digest, authority.as_bytes());
                    }
                    None => digest.update([0]),
                }
            }
        }
        StorageAccessDomainId(digest.finalize().into())
    }
}

const fn credential_purpose_discriminant(purpose: CatalogCredentialPurpose) -> u8 {
    match purpose {
        CatalogCredentialPurpose::CatalogControl => 0,
        CatalogCredentialPurpose::ObjectStoreData => 1,
        CatalogCredentialPurpose::ObjectStoreMetadata => 2,
    }
}

const fn consumer_role_discriminant(role: CredentialConsumerRole) -> u8 {
    match role {
        CredentialConsumerRole::Frontend => 0,
        CredentialConsumerRole::Backend => 1,
        CredentialConsumerRole::FrontendAndBackend => 2,
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StorageAccessDomainId([u8; 32]);

impl StorageAccessDomainId {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

fn parse_reference_component(subject: &str, value: &str) -> Result<Arc<str>, ConnectorError> {
    let bytes = value.as_bytes();
    if bytes.is_empty()
        || bytes.len() > MAX_CATALOG_CREDENTIAL_REFERENCE_BYTES
        || !bytes[0].is_ascii_lowercase()
        || !bytes[1..].iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
    {
        return Err(invalid(subject));
    }
    Ok(Arc::from(value))
}

fn is_property_key(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= MAX_CATALOG_NON_SECRET_PROPERTY_KEY_BYTES
        && bytes[0].is_ascii_lowercase()
        && bytes[1..].iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
}

fn is_sensitive_property_key(value: &str) -> bool {
    let compact = value
        .bytes()
        .filter(u8::is_ascii_alphanumeric)
        .map(char::from)
        .collect::<String>();
    [
        "secret",
        "password",
        "token",
        "credential",
        "accesskey",
        "apikey",
        "privatekey",
    ]
    .iter()
    .any(|marker| compact.contains(marker))
}

fn parse_storage_authority(value: &str) -> Result<Arc<str>, ConnectorError> {
    if value.is_empty()
        || value.len() > MAX_STORAGE_CREDENTIAL_SCOPE_PREFIX_BYTES
        || !value.is_ascii()
        || value.bytes().any(|byte| byte.is_ascii_whitespace())
        || value
            .bytes()
            .any(|byte| matches!(byte, b'/' | b'?' | b'#' | b'@'))
    {
        return Err(invalid("storage authority"));
    }
    Ok(Arc::from(value.to_ascii_lowercase()))
}

fn put_count(output: &mut Vec<u8>, count: usize) {
    output.extend_from_slice(&(count as u32).to_le_bytes());
}

fn put_bytes(output: &mut Vec<u8>, value: &[u8]) {
    put_count(output, value.len());
    output.extend_from_slice(value);
}

fn hash_count(digest: &mut Sha256, count: usize) {
    digest.update((count as u32).to_le_bytes());
}

fn hash_bytes(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u32).to_le_bytes());
    digest.update(value);
}

fn invalid(subject: &str) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::InvalidRequest,
        format!("invalid {subject}"),
    )
}

fn exhausted(subject: &str) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        format!("{subject} exceeds configured bounds"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn static_reference(generation: &str) -> StaticCredentialReference {
        StaticCredentialReference::try_new("warehouse-reader", generation).unwrap()
    }

    fn static_data_binding(generation: &str) -> CatalogCredentialBinding {
        CatalogCredentialBinding::try_new(
            CatalogCredentialPurpose::ObjectStoreData,
            CredentialConsumerRole::Backend,
            CatalogCredentialMode::Static(static_reference(generation)),
        )
        .unwrap()
    }

    fn control_binding() -> CatalogCredentialBinding {
        CatalogCredentialBinding::try_new(
            CatalogCredentialPurpose::CatalogControl,
            CredentialConsumerRole::Frontend,
            CatalogCredentialMode::Static(
                StaticCredentialReference::try_new("rest-control", "blue").unwrap(),
            ),
        )
        .unwrap()
    }

    fn metadata_binding() -> CatalogCredentialBinding {
        CatalogCredentialBinding::try_new(
            CatalogCredentialPurpose::ObjectStoreMetadata,
            CredentialConsumerRole::Frontend,
            CatalogCredentialMode::Static(
                StaticCredentialReference::try_new("warehouse-metadata", "blue").unwrap(),
            ),
        )
        .unwrap()
    }

    fn access_input(
        generation: &str,
        properties: Vec<CatalogNonSecretProperty>,
    ) -> CatalogStorageAccessDomainInput {
        CatalogStorageAccessDomainInput::try_new(
            ConnectorProviderId::parse("iceberg").unwrap(),
            ConnectorInstanceId::try_from_canonical("analytics").unwrap(),
            2,
            properties,
            static_data_binding(generation),
            vec![],
        )
        .unwrap()
    }

    #[test]
    fn reference_and_binding_matrix_is_closed() {
        for invalid_value in ["", "Upper", "-leading", "x/y"] {
            assert!(StaticCredentialReference::try_new(invalid_value, "one").is_err());
            assert!(StaticCredentialReference::try_new("valid", invalid_value).is_err());
        }
        let oversized = "x".repeat(129);
        assert!(StaticCredentialReference::try_new(&oversized, "one").is_err());
        assert!(StaticCredentialReference::try_new("valid", &oversized).is_err());
        assert!(
            CatalogCredentialBinding::try_new(
                CatalogCredentialPurpose::CatalogControl,
                CredentialConsumerRole::Backend,
                CatalogCredentialMode::Static(static_reference("one")),
            )
            .is_err()
        );
        assert!(
            CatalogCredentialBinding::try_new(
                CatalogCredentialPurpose::CatalogControl,
                CredentialConsumerRole::Frontend,
                CatalogCredentialMode::Vended,
            )
            .is_err()
        );
        assert!(
            CatalogCredentialBinding::try_new(
                CatalogCredentialPurpose::ObjectStoreData,
                CredentialConsumerRole::Frontend,
                CatalogCredentialMode::Static(static_reference("one")),
            )
            .is_err()
        );
        assert!(
            CatalogCredentialBinding::try_new(
                CatalogCredentialPurpose::ObjectStoreData,
                CredentialConsumerRole::FrontendAndBackend,
                CatalogCredentialMode::Static(static_reference("one")),
            )
            .is_err()
        );
        assert!(
            CatalogCredentialBinding::try_new(
                CatalogCredentialPurpose::ObjectStoreData,
                CredentialConsumerRole::Backend,
                CatalogCredentialMode::Static(static_reference("one")),
            )
            .is_ok()
        );
        assert!(
            CatalogCredentialBinding::try_new(
                CatalogCredentialPurpose::ObjectStoreMetadata,
                CredentialConsumerRole::Frontend,
                CatalogCredentialMode::Static(static_reference("one")),
            )
            .is_ok()
        );
        assert!(
            CatalogCredentialBinding::try_new(
                CatalogCredentialPurpose::ObjectStoreMetadata,
                CredentialConsumerRole::FrontendAndBackend,
                CatalogCredentialMode::Static(static_reference("one")),
            )
            .is_err()
        );
        assert!(
            CatalogCredentialBinding::try_new(
                CatalogCredentialPurpose::ObjectStoreMetadata,
                CredentialConsumerRole::Frontend,
                CatalogCredentialMode::Vended,
            )
            .is_err()
        );
    }

    #[test]
    fn whole_binding_set_is_bounded_unique_and_order_independent() {
        let control = control_binding();
        let data = static_data_binding("blue");
        let left =
            canonical_catalog_credential_binding_bytes(&[data.clone(), control.clone()]).unwrap();
        let right =
            canonical_catalog_credential_binding_bytes(&[control.clone(), data.clone()]).unwrap();
        assert_eq!(left, right);
        assert!(canonicalize_catalog_credential_bindings(vec![data.clone(), data]).is_err());
        assert!(
            canonicalize_catalog_credential_bindings(vec![
                control;
                MAX_CATALOG_CREDENTIAL_BINDINGS + 1
            ])
            .is_err()
        );
    }

    #[test]
    fn canonical_binding_discriminants_preserve_data_and_append_metadata() {
        let purpose_offset = CREDENTIAL_BINDING_DOMAIN.len() + std::mem::size_of::<u32>();
        let data = canonical_catalog_credential_binding_bytes(&[static_data_binding("blue")])
            .expect("data binding bytes");
        let metadata = canonical_catalog_credential_binding_bytes(&[metadata_binding()])
            .expect("metadata binding bytes");

        assert_eq!(data[purpose_offset], 1);
        assert_eq!(metadata[purpose_offset], 2);
    }

    #[test]
    fn access_domain_is_order_independent_but_definition_and_generation_sensitive() {
        let uri = CatalogNonSecretProperty::try_new("uri", "https://catalog.example/v1").unwrap();
        let warehouse = CatalogNonSecretProperty::try_new("warehouse", "s3://warehouse").unwrap();
        let first =
            access_input("blue", vec![warehouse.clone(), uri.clone()]).derive_access_domain();
        let reordered = access_input("blue", vec![uri, warehouse]).derive_access_domain();
        let rotated = access_input(
            "green",
            vec![
                CatalogNonSecretProperty::try_new("uri", "https://catalog.example/v1").unwrap(),
                CatalogNonSecretProperty::try_new("warehouse", "s3://warehouse").unwrap(),
            ],
        )
        .derive_access_domain();
        let reformatted = CatalogStorageAccessDomainInput::try_new(
            ConnectorProviderId::parse("iceberg").unwrap(),
            ConnectorInstanceId::try_from_canonical("analytics").unwrap(),
            3,
            vec![
                CatalogNonSecretProperty::try_new("uri", "https://catalog.example/v1").unwrap(),
                CatalogNonSecretProperty::try_new("warehouse", "s3://warehouse").unwrap(),
            ],
            static_data_binding("blue"),
            vec![],
        )
        .unwrap()
        .derive_access_domain();
        assert_eq!(first, reordered);
        assert_ne!(first, rotated);
        assert_ne!(first, reformatted);
        assert_eq!(
            first.as_bytes(),
            &[
                2, 67, 53, 41, 126, 17, 195, 228, 118, 40, 75, 52, 156, 194, 211, 125, 242, 71,
                160, 209, 154, 69, 176, 118, 34, 81, 160, 130, 132, 245, 154, 85,
            ]
        );
    }

    #[test]
    fn access_domain_retains_the_exact_storage_purpose_and_consumer_role() {
        let data = access_input("blue", vec![]).derive_access_domain();
        let metadata = CatalogStorageAccessDomainInput::try_new(
            ConnectorProviderId::parse("iceberg").unwrap(),
            ConnectorInstanceId::try_from_canonical("analytics").unwrap(),
            2,
            vec![],
            CatalogCredentialBinding::try_new(
                CatalogCredentialPurpose::ObjectStoreMetadata,
                CredentialConsumerRole::Frontend,
                CatalogCredentialMode::Static(static_reference("blue")),
            )
            .unwrap(),
            vec![],
        )
        .unwrap()
        .derive_access_domain();

        assert_ne!(data, metadata);
    }

    #[test]
    fn vended_scope_is_canonical_and_cannot_mix_with_static_mode() {
        let first = StorageCredentialScopePrefix::try_from_normalized("s3://bucket/a/").unwrap();
        let second = StorageCredentialScopePrefix::try_from_normalized("s3://bucket/b/").unwrap();
        let vended = CatalogCredentialBinding::try_new(
            CatalogCredentialPurpose::ObjectStoreData,
            CredentialConsumerRole::Backend,
            CatalogCredentialMode::Vended,
        )
        .unwrap();
        let input = CatalogStorageAccessDomainInput::try_new(
            ConnectorProviderId::parse("iceberg").unwrap(),
            ConnectorInstanceId::try_from_canonical("analytics").unwrap(),
            2,
            vec![],
            vended,
            vec![second, first],
        )
        .unwrap();
        let reversed = CatalogStorageAccessDomainInput::try_new(
            ConnectorProviderId::parse("iceberg").unwrap(),
            ConnectorInstanceId::try_from_canonical("analytics").unwrap(),
            2,
            vec![],
            CatalogCredentialBinding::try_new(
                CatalogCredentialPurpose::ObjectStoreData,
                CredentialConsumerRole::Backend,
                CatalogCredentialMode::Vended,
            )
            .unwrap(),
            vec![
                StorageCredentialScopePrefix::try_from_normalized("s3://bucket/a/").unwrap(),
                StorageCredentialScopePrefix::try_from_normalized("s3://bucket/b/").unwrap(),
            ],
        )
        .unwrap();
        assert_eq!(
            input.derive_access_domain(),
            reversed.derive_access_domain()
        );
        assert!(
            CatalogStorageAccessDomainInput::try_new(
                ConnectorProviderId::parse("iceberg").unwrap(),
                ConnectorInstanceId::try_from_canonical("analytics").unwrap(),
                2,
                vec![],
                static_data_binding("blue"),
                vec![StorageCredentialScopePrefix::try_from_normalized("s3://bucket/").unwrap()],
            )
            .is_err()
        );
    }

    #[test]
    fn secret_like_property_keys_and_unnormalized_prefixes_are_rejected() {
        assert!(CatalogNonSecretProperty::try_new("access_key_id", "not-allowed").is_err());
        assert!(CatalogNonSecretProperty::try_new("aws.s3.access.key", "not-allowed").is_err());
        assert!(CatalogNonSecretProperty::try_new("aws.s3.accesskeyid", "not-allowed").is_err());
        assert!(CatalogNonSecretProperty::try_new("aws.s3.secret.key", "not-allowed").is_err());
        assert!(CatalogNonSecretProperty::try_new("session_token", "not-allowed").is_err());
        assert!(
            CatalogNonSecretProperty::try_new(
                "uri",
                "https://user:secret@catalog.example/v1?token=leak"
            )
            .is_err()
        );
        assert!(StorageCredentialScopePrefix::try_from_normalized("S3://bucket/").is_err());
        assert!(StorageCredentialScopePrefix::try_from_normalized("s3://bucket/?token=x").is_err());
        assert!(
            StorageCredentialScopePrefix::try_from_normalized("s3://user:secret@bucket/a/")
                .is_err()
        );
        assert!(StorageCredentialScopePrefix::try_from_normalized("gs://bucket/a/").is_err());
        assert!(StorageCredentialScopePrefix::try_from_normalized("s3://bucket/a/../b/").is_err());
    }

    #[test]
    fn local_and_hdfs_catalogs_mint_explicit_binding_free_domains() {
        let provider = ConnectorProviderId::parse("iceberg").unwrap();
        let catalog = ConnectorInstanceId::try_from_canonical("analytics").unwrap();
        let properties =
            vec![CatalogNonSecretProperty::try_new("warehouse", "file:///warehouse").unwrap()];
        let local = CatalogStorageAccessDomainInput::try_new_uncredentialed(
            provider.clone(),
            catalog.clone(),
            2,
            properties,
            CatalogUncredentialedStorageKind::Local,
            None,
        )
        .unwrap()
        .derive_access_domain();
        let hdfs = CatalogStorageAccessDomainInput::try_new_uncredentialed(
            provider,
            catalog,
            2,
            vec![
                CatalogNonSecretProperty::try_new("warehouse", "hdfs://namenode:8020/warehouse")
                    .unwrap(),
            ],
            CatalogUncredentialedStorageKind::Hdfs,
            Some("NameNode:8020"),
        )
        .unwrap()
        .derive_access_domain();
        assert_ne!(local, hdfs);
        assert!(
            CatalogStorageAccessDomainInput::try_new_uncredentialed(
                ConnectorProviderId::parse("iceberg").unwrap(),
                ConnectorInstanceId::try_from_canonical("analytics").unwrap(),
                2,
                vec![],
                CatalogUncredentialedStorageKind::Hdfs,
                None,
            )
            .is_err()
        );
    }
}
