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

use std::{collections::BTreeMap, fmt};

use novarocks_secret::SecretValue;
use novarocks_spi::connector::{CatalogCredentialPurpose, StaticCredentialReference};
use novarocks_types::ClusterRole;

pub const MAX_ROLE_LOCAL_CREDENTIAL_ENTRIES: usize = 256;
const MAX_OAUTH_CLIENT_ID_BYTES: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogCredentialMaterialKind {
    S3,
    IcebergRestOauth2,
    IcebergRestBearer,
}

#[derive(Clone, Eq, PartialEq)]
pub struct S3CredentialMaterial {
    access_key_id: SecretValue,
    access_key_secret: SecretValue,
    session_token: Option<SecretValue>,
}

impl S3CredentialMaterial {
    pub fn new(
        access_key_id: SecretValue,
        access_key_secret: SecretValue,
        session_token: Option<SecretValue>,
    ) -> Result<Self, String> {
        require_secret("S3 access key ID", &access_key_id)?;
        require_secret("S3 access key secret", &access_key_secret)?;
        if let Some(session_token) = &session_token {
            require_secret("S3 session token", session_token)?;
        }
        Ok(Self {
            access_key_id,
            access_key_secret,
            session_token,
        })
    }

    pub fn access_key_id(&self) -> &SecretValue {
        &self.access_key_id
    }

    pub fn access_key_secret(&self) -> &SecretValue {
        &self.access_key_secret
    }

    pub fn session_token(&self) -> Option<&SecretValue> {
        self.session_token.as_ref()
    }
}

impl fmt::Debug for S3CredentialMaterial {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("S3CredentialMaterial(<redacted>)")
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct IcebergRestOauth2CredentialMaterial {
    client_id: String,
    client_secret: SecretValue,
}

impl IcebergRestOauth2CredentialMaterial {
    pub fn new(client_id: String, client_secret: SecretValue) -> Result<Self, String> {
        if client_id.is_empty()
            || client_id.len() > MAX_OAUTH_CLIENT_ID_BYTES
            || !client_id.is_ascii()
        {
            return Err("invalid Iceberg REST OAuth2 client ID".to_string());
        }
        require_secret("Iceberg REST OAuth2 client secret", &client_secret)?;
        Ok(Self {
            client_id,
            client_secret,
        })
    }

    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    pub fn client_secret(&self) -> &SecretValue {
        &self.client_secret
    }
}

impl fmt::Debug for IcebergRestOauth2CredentialMaterial {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("IcebergRestOauth2CredentialMaterial(<redacted>)")
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct IcebergRestBearerCredentialMaterial {
    token: SecretValue,
}

impl IcebergRestBearerCredentialMaterial {
    pub fn new(token: SecretValue) -> Result<Self, String> {
        require_secret("Iceberg REST bearer token", &token)?;
        Ok(Self { token })
    }

    pub fn token(&self) -> &SecretValue {
        &self.token
    }
}

impl fmt::Debug for IcebergRestBearerCredentialMaterial {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("IcebergRestBearerCredentialMaterial(<redacted>)")
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum CatalogCredentialMaterial {
    S3(S3CredentialMaterial),
    IcebergRestOauth2(IcebergRestOauth2CredentialMaterial),
    IcebergRestBearer(IcebergRestBearerCredentialMaterial),
}

impl CatalogCredentialMaterial {
    pub const fn kind(&self) -> CatalogCredentialMaterialKind {
        match self {
            Self::S3(_) => CatalogCredentialMaterialKind::S3,
            Self::IcebergRestOauth2(_) => CatalogCredentialMaterialKind::IcebergRestOauth2,
            Self::IcebergRestBearer(_) => CatalogCredentialMaterialKind::IcebergRestBearer,
        }
    }

    pub const fn as_s3(&self) -> Option<&S3CredentialMaterial> {
        match self {
            Self::S3(material) => Some(material),
            Self::IcebergRestOauth2(_) | Self::IcebergRestBearer(_) => None,
        }
    }

    pub const fn as_iceberg_rest_oauth2(&self) -> Option<&IcebergRestOauth2CredentialMaterial> {
        match self {
            Self::IcebergRestOauth2(material) => Some(material),
            Self::S3(_) | Self::IcebergRestBearer(_) => None,
        }
    }

    pub const fn as_iceberg_rest_bearer(&self) -> Option<&IcebergRestBearerCredentialMaterial> {
        match self {
            Self::IcebergRestBearer(material) => Some(material),
            Self::S3(_) | Self::IcebergRestOauth2(_) => None,
        }
    }
}

impl fmt::Debug for CatalogCredentialMaterial {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("CatalogCredentialMaterial")
            .field(&self.kind())
            .field(&"<redacted>")
            .finish()
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CatalogCredentialRegistryKey {
    purpose: CatalogCredentialPurpose,
    reference: StaticCredentialReference,
}

impl CatalogCredentialRegistryKey {
    pub const fn new(
        purpose: CatalogCredentialPurpose,
        reference: StaticCredentialReference,
    ) -> Self {
        Self { purpose, reference }
    }

    pub const fn purpose(&self) -> CatalogCredentialPurpose {
        self.purpose
    }

    pub const fn reference(&self) -> &StaticCredentialReference {
        &self.reference
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogCredentialRegistryEntry {
    key: CatalogCredentialRegistryKey,
    material: CatalogCredentialMaterial,
}

impl CatalogCredentialRegistryEntry {
    pub fn try_new(
        purpose: CatalogCredentialPurpose,
        reference: StaticCredentialReference,
        material: CatalogCredentialMaterial,
    ) -> Result<Self, String> {
        let kind_matches = matches!(
            (purpose, material.kind()),
            (
                CatalogCredentialPurpose::CatalogControl,
                CatalogCredentialMaterialKind::IcebergRestOauth2
                    | CatalogCredentialMaterialKind::IcebergRestBearer
            ) | (
                CatalogCredentialPurpose::ObjectStoreData,
                CatalogCredentialMaterialKind::S3
            ) | (
                CatalogCredentialPurpose::ObjectStoreMetadata,
                CatalogCredentialMaterialKind::S3
            )
        );
        if !kind_matches {
            return Err(format!(
                "credential purpose {purpose:?} does not accept material kind {:?}",
                material.kind()
            ));
        }
        Ok(Self {
            key: CatalogCredentialRegistryKey::new(purpose, reference),
            material,
        })
    }

    pub const fn key(&self) -> &CatalogCredentialRegistryKey {
        &self.key
    }

    pub const fn material(&self) -> &CatalogCredentialMaterial {
        &self.material
    }
}

/// Immutable credential values owned by exactly one deployable application role.
#[derive(Clone, Eq, PartialEq)]
// Design: ADR-0125 (docs/adr/ADR-0125-role-local-catalog-credentials-and-access-domains.md)
pub struct CatalogCredentialRegistry {
    role: ClusterRole,
    entries: BTreeMap<CatalogCredentialRegistryKey, CatalogCredentialMaterial>,
}

impl CatalogCredentialRegistry {
    pub fn try_new(
        role: ClusterRole,
        entries: Vec<CatalogCredentialRegistryEntry>,
    ) -> Result<Self, String> {
        if entries.len() > MAX_ROLE_LOCAL_CREDENTIAL_ENTRIES {
            return Err(format!(
                "role-local credential registry exceeds {MAX_ROLE_LOCAL_CREDENTIAL_ENTRIES} entries"
            ));
        }
        let mut resolved = BTreeMap::new();
        for entry in entries {
            let role_allows_purpose = matches!(
                (role, entry.key.purpose),
                (ClusterRole::Fe, CatalogCredentialPurpose::CatalogControl)
                    | (
                        ClusterRole::Fe,
                        CatalogCredentialPurpose::ObjectStoreMetadata
                    )
                    | (ClusterRole::Be, CatalogCredentialPurpose::ObjectStoreData)
            );
            if !role_allows_purpose {
                return Err(format!(
                    "role {role:?} cannot own {:?} credential `{}` generation `{}`",
                    entry.key.purpose,
                    entry.key.reference.name(),
                    entry.key.reference.generation()
                ));
            }
            if resolved.insert(entry.key.clone(), entry.material).is_some() {
                return Err(format!(
                    "duplicate {:?} credential `{}` generation `{}`",
                    entry.key.purpose,
                    entry.key.reference.name(),
                    entry.key.reference.generation()
                ));
            }
        }
        Ok(Self {
            role,
            entries: resolved,
        })
    }

    pub const fn role(&self) -> ClusterRole {
        self.role
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn keys(&self) -> impl ExactSizeIterator<Item = &CatalogCredentialRegistryKey> {
        self.entries.keys()
    }

    pub fn resolve(
        &self,
        purpose: CatalogCredentialPurpose,
        reference: &StaticCredentialReference,
    ) -> Option<&CatalogCredentialMaterial> {
        self.entries.get(&CatalogCredentialRegistryKey::new(
            purpose,
            reference.clone(),
        ))
    }
}

impl novarocks_connector_iceberg::access_binding::IcebergStaticCredentialResolver
    for CatalogCredentialRegistry
{
    fn resolve_object_store_static(
        &self,
        reference: &StaticCredentialReference,
    ) -> Result<novarocks_fs::ObjectStoreSecretMaterial, novarocks_spi::connector::ConnectorError>
    {
        let material = self
            .resolve(CatalogCredentialPurpose::ObjectStoreData, reference)
            .and_then(CatalogCredentialMaterial::as_s3)
            .ok_or_else(|| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                    "role-local registry has no exact S3 object-store credential binding",
                )
            })?;
        Ok(novarocks_fs::ObjectStoreSecretMaterial {
            access_key_id: material.access_key_id().clone(),
            access_key_secret: material.access_key_secret().clone(),
            session_token: material.session_token().cloned(),
        })
    }

    fn resolve_object_store_metadata_static(
        &self,
        reference: &StaticCredentialReference,
    ) -> Result<novarocks_fs::ObjectStoreSecretMaterial, novarocks_spi::connector::ConnectorError>
    {
        let material = self
            .resolve(CatalogCredentialPurpose::ObjectStoreMetadata, reference)
            .and_then(CatalogCredentialMaterial::as_s3)
            .ok_or_else(|| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                    "role-local registry has no exact S3 metadata credential binding",
                )
            })?;
        Ok(novarocks_fs::ObjectStoreSecretMaterial {
            access_key_id: material.access_key_id().clone(),
            access_key_secret: material.access_key_secret().clone(),
            session_token: material.session_token().cloned(),
        })
    }
}

impl fmt::Debug for CatalogCredentialRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CatalogCredentialRegistry")
            .field("role", &self.role)
            .field("entry_count", &self.entries.len())
            .finish()
    }
}

fn require_secret(subject: &str, value: &SecretValue) -> Result<(), String> {
    if value.is_empty() {
        return Err(format!("{subject} must not be empty"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference(name: &str, generation: &str) -> StaticCredentialReference {
        StaticCredentialReference::try_new(name, generation).unwrap()
    }

    fn s3(value: &str) -> CatalogCredentialMaterial {
        CatalogCredentialMaterial::S3(
            S3CredentialMaterial::new(
                SecretValue::new(value),
                SecretValue::new(format!("{value}-secret")),
                None,
            )
            .unwrap(),
        )
    }

    fn rest_bearer(value: &str) -> CatalogCredentialMaterial {
        CatalogCredentialMaterial::IcebergRestBearer(
            IcebergRestBearerCredentialMaterial::new(SecretValue::new(value)).unwrap(),
        )
    }

    #[test]
    fn exact_generation_lookup_never_falls_back_by_name() {
        let blue = reference("warehouse", "blue");
        let green = reference("warehouse", "green");
        let registry = CatalogCredentialRegistry::try_new(
            ClusterRole::Be,
            vec![
                CatalogCredentialRegistryEntry::try_new(
                    CatalogCredentialPurpose::ObjectStoreData,
                    blue.clone(),
                    s3("blue"),
                )
                .unwrap(),
                CatalogCredentialRegistryEntry::try_new(
                    CatalogCredentialPurpose::ObjectStoreData,
                    green.clone(),
                    s3("green"),
                )
                .unwrap(),
            ],
        )
        .unwrap();
        assert!(
            registry
                .resolve(CatalogCredentialPurpose::ObjectStoreData, &blue)
                .is_some()
        );
        assert!(
            registry
                .resolve(
                    CatalogCredentialPurpose::ObjectStoreData,
                    &reference("warehouse", "missing")
                )
                .is_none()
        );
    }

    #[test]
    fn role_kind_duplicate_and_bounds_are_closed() {
        let data = CatalogCredentialRegistryEntry::try_new(
            CatalogCredentialPurpose::ObjectStoreData,
            reference("warehouse", "blue"),
            s3("blue"),
        )
        .unwrap();
        assert!(CatalogCredentialRegistry::try_new(ClusterRole::Fe, vec![data.clone()]).is_err());
        assert!(CatalogCredentialRegistry::try_new(ClusterRole::Be, vec![data.clone()]).is_ok());
        let metadata = CatalogCredentialRegistryEntry::try_new(
            CatalogCredentialPurpose::ObjectStoreMetadata,
            reference("warehouse-metadata", "blue"),
            s3("metadata"),
        )
        .unwrap();
        assert!(
            CatalogCredentialRegistry::try_new(ClusterRole::Fe, vec![metadata.clone()]).is_ok()
        );
        assert!(CatalogCredentialRegistry::try_new(ClusterRole::Be, vec![metadata]).is_err());
        let control = CatalogCredentialRegistryEntry::try_new(
            CatalogCredentialPurpose::CatalogControl,
            reference("rest", "blue"),
            rest_bearer("control"),
        )
        .unwrap();
        assert!(CatalogCredentialRegistry::try_new(ClusterRole::Be, vec![control]).is_err());
        assert!(
            CatalogCredentialRegistry::try_new(ClusterRole::Be, vec![data.clone(), data.clone()])
                .is_err()
        );
        assert!(
            CatalogCredentialRegistry::try_new(
                ClusterRole::Be,
                vec![data; MAX_ROLE_LOCAL_CREDENTIAL_ENTRIES + 1]
            )
            .is_err()
        );
        assert!(
            CatalogCredentialRegistryEntry::try_new(
                CatalogCredentialPurpose::CatalogControl,
                reference("rest", "blue"),
                s3("wrong-kind")
            )
            .is_err()
        );
    }

    #[test]
    fn iceberg_metadata_resolution_never_falls_back_to_data_credentials() {
        let reference = reference("warehouse", "blue");
        let registry = CatalogCredentialRegistry::try_new(
            ClusterRole::Fe,
            vec![
                CatalogCredentialRegistryEntry::try_new(
                    CatalogCredentialPurpose::ObjectStoreMetadata,
                    reference.clone(),
                    s3("metadata"),
                )
                .unwrap(),
            ],
        )
        .unwrap();

        assert!(
            novarocks_connector_iceberg::access_binding::IcebergStaticCredentialResolver::resolve_object_store_metadata_static(
                &registry,
                &reference,
            )
            .is_ok()
        );
        assert!(
            novarocks_connector_iceberg::access_binding::IcebergStaticCredentialResolver::resolve_object_store_static(
                &registry,
                &reference,
            )
            .is_err()
        );
    }

    #[test]
    fn diagnostics_redact_every_secret_value() {
        let canary = "cca-secret-canary";
        let registry = CatalogCredentialRegistry::try_new(
            ClusterRole::Be,
            vec![
                CatalogCredentialRegistryEntry::try_new(
                    CatalogCredentialPurpose::ObjectStoreData,
                    reference("warehouse", "blue"),
                    s3(canary),
                )
                .unwrap(),
            ],
        )
        .unwrap();
        assert!(!format!("{registry:?}").contains(canary));
        assert!(!format!("{:?}", registry.entries.values().next()).contains(canary));
    }
}
