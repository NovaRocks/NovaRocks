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
use novarocks_spi::connector::{
    CatalogCredentialPurpose, NodeLocalCredentialReference, StaticCredentialReference,
};
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
pub enum CatalogCredentialKeyReference {
    Static(StaticCredentialReference),
    NodeLocal(NodeLocalCredentialReference),
}

impl CatalogCredentialKeyReference {
    pub fn name(&self) -> &str {
        match self {
            Self::Static(reference) => reference.name(),
            Self::NodeLocal(reference) => reference.name(),
        }
    }

    pub fn generation(&self) -> &str {
        match self {
            Self::Static(reference) => reference.generation(),
            Self::NodeLocal(reference) => reference.generation(),
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CatalogCredentialRegistryKey {
    purpose: CatalogCredentialPurpose,
    reference: CatalogCredentialKeyReference,
}

impl CatalogCredentialRegistryKey {
    pub const fn new(
        purpose: CatalogCredentialPurpose,
        reference: StaticCredentialReference,
    ) -> Self {
        Self {
            purpose,
            reference: CatalogCredentialKeyReference::Static(reference),
        }
    }

    pub const fn new_node_local(
        purpose: CatalogCredentialPurpose,
        reference: NodeLocalCredentialReference,
    ) -> Self {
        Self {
            purpose,
            reference: CatalogCredentialKeyReference::NodeLocal(reference),
        }
    }

    pub const fn purpose(&self) -> CatalogCredentialPurpose {
        self.purpose
    }

    pub const fn reference(&self) -> &CatalogCredentialKeyReference {
        &self.reference
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CatalogCredentialDefinition {
    Static(CatalogCredentialMaterial),
    NodeLocal(novarocks_fs::AliyunEcsRamRoleConfig),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogCredentialRegistryEntry {
    key: CatalogCredentialRegistryKey,
    definition: CatalogCredentialDefinition,
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
            ) | (
                // A catalog identity, not a storage credential: the execution
                // node exchanges it for data credentials and never signs an
                // object-store request with it (CAD-1 D1).
                CatalogCredentialPurpose::DataCredentialVending,
                CatalogCredentialMaterialKind::IcebergRestOauth2
                    | CatalogCredentialMaterialKind::IcebergRestBearer
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
            definition: CatalogCredentialDefinition::Static(material),
        })
    }

    pub fn try_new_node_local(
        purpose: CatalogCredentialPurpose,
        reference: NodeLocalCredentialReference,
        config: novarocks_fs::AliyunEcsRamRoleConfig,
    ) -> Result<Self, String> {
        if !matches!(
            purpose,
            CatalogCredentialPurpose::ObjectStoreData
                | CatalogCredentialPurpose::ObjectStoreMetadata
        ) {
            return Err("node-local credentials require an object-store purpose".to_string());
        }
        Ok(Self {
            key: CatalogCredentialRegistryKey::new_node_local(purpose, reference),
            definition: CatalogCredentialDefinition::NodeLocal(config),
        })
    }

    pub const fn key(&self) -> &CatalogCredentialRegistryKey {
        &self.key
    }

    pub const fn material(&self) -> Option<&CatalogCredentialMaterial> {
        match &self.definition {
            CatalogCredentialDefinition::Static(material) => Some(material),
            CatalogCredentialDefinition::NodeLocal(_) => None,
        }
    }
}

/// Immutable credential definitions owned by one deployable application role.
/// Static definitions contain fixed material; node-local definitions contain
/// only the renewable source configuration, never acquired material.
#[derive(Clone, Eq, PartialEq)]
// Design: ADR-0125 (docs/adr/ADR-0125-role-local-catalog-credentials-and-access-domains.md)
pub struct CatalogCredentialRegistry {
    role: ClusterRole,
    entries: BTreeMap<CatalogCredentialRegistryKey, CatalogCredentialDefinition>,
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
                    // Mirror image of CatalogControl: the vending identity is
                    // the execution node's and only the execution node's. A
                    // coordinator that owned one would be authenticating as the
                    // node it dispatches to.
                    | (
                        ClusterRole::Be,
                        CatalogCredentialPurpose::DataCredentialVending
                    )
            );
            if !role_allows_purpose {
                return Err(format!(
                    "role {role:?} cannot own {:?} credential `{}` generation `{}`",
                    entry.key.purpose,
                    entry.key.reference.name(),
                    entry.key.reference.generation()
                ));
            }
            if resolved
                .insert(entry.key.clone(), entry.definition)
                .is_some()
            {
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

    pub fn resolve_node_local(
        &self,
        purpose: CatalogCredentialPurpose,
        reference: &NodeLocalCredentialReference,
    ) -> Option<&novarocks_fs::AliyunEcsRamRoleConfig> {
        match self
            .entries
            .get(&CatalogCredentialRegistryKey::new_node_local(
                purpose,
                reference.clone(),
            )) {
            Some(CatalogCredentialDefinition::NodeLocal(config)) => Some(config),
            Some(CatalogCredentialDefinition::Static(_)) | None => None,
        }
    }

    pub fn resolve(
        &self,
        purpose: CatalogCredentialPurpose,
        reference: &StaticCredentialReference,
    ) -> Option<&CatalogCredentialMaterial> {
        match self.entries.get(&CatalogCredentialRegistryKey::new(
            purpose,
            reference.clone(),
        )) {
            Some(CatalogCredentialDefinition::Static(material)) => Some(material),
            Some(CatalogCredentialDefinition::NodeLocal(_)) | None => None,
        }
    }
}

impl novarocks_connector_iceberg::access_binding::IcebergCredentialResolver
    for CatalogCredentialRegistry
{
    fn resolve_node_local_object_store(
        &self,
        purpose: CatalogCredentialPurpose,
        reference: &NodeLocalCredentialReference,
    ) -> Result<novarocks_fs::AliyunEcsRamRoleConfig, novarocks_spi::connector::ConnectorError>
    {
        self.resolve_node_local(purpose, reference)
            .cloned()
            .ok_or_else(|| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                    "role-local registry has no exact renewable object-store source binding",
                )
            })
    }

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

    fn resolve_data_credential_vending(
        &self,
        reference: &StaticCredentialReference,
    ) -> Result<
        novarocks_connector_iceberg::access_binding::IcebergRestAuthMaterial,
        novarocks_spi::connector::ConnectorError,
    > {
        use novarocks_connector_iceberg::access_binding::IcebergRestAuthMaterial;
        // Role scoping was already enforced when the registry was built: a
        // coordinator cannot own this purpose at all, so a lookup here can only
        // succeed on an execution node.
        match self.resolve(CatalogCredentialPurpose::DataCredentialVending, reference) {
            Some(CatalogCredentialMaterial::IcebergRestOauth2(material)) => {
                Ok(IcebergRestAuthMaterial::Oauth2 {
                    client_id: material.client_id().to_string(),
                    client_secret: material.client_secret().clone(),
                })
            }
            Some(CatalogCredentialMaterial::IcebergRestBearer(material)) => {
                Ok(IcebergRestAuthMaterial::Bearer {
                    token: material.token().clone(),
                })
            }
            _ => Err(novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Unsupported,
                "role-local registry has no exact data-credential-vending identity",
            )),
        }
    }

    fn resolve_catalog_control(
        &self,
        reference: &StaticCredentialReference,
    ) -> Result<
        novarocks_connector_iceberg::access_binding::IcebergRestAuthMaterial,
        novarocks_spi::connector::ConnectorError,
    > {
        use novarocks_connector_iceberg::access_binding::IcebergRestAuthMaterial;
        // An execution node never holds a control identity: the registry
        // refuses the purpose on that role, and the backend projection of a
        // catalog definition never names one. Say so explicitly rather than
        // reporting a missing entry.
        if self.role != ClusterRole::Fe {
            return Err(novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Unsupported,
                "catalog-control identity is resolvable only on the frontend role",
            ));
        }
        // On the coordinator a declared binding without its entry is a
        // deployment error, not an absent identity: continuing would send the
        // catalog session without the authentication the definition asks for.
        match self.resolve(CatalogCredentialPurpose::CatalogControl, reference) {
            Some(CatalogCredentialMaterial::IcebergRestOauth2(material)) => {
                Ok(IcebergRestAuthMaterial::Oauth2 {
                    client_id: material.client_id().to_string(),
                    client_secret: material.client_secret().clone(),
                })
            }
            Some(CatalogCredentialMaterial::IcebergRestBearer(material)) => {
                Ok(IcebergRestAuthMaterial::Bearer {
                    token: material.token().clone(),
                })
            }
            _ => Err(novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                format!(
                    "role-local registry has no catalog-control identity `{}` generation `{}`",
                    reference.name(),
                    reference.generation()
                ),
            )),
        }
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

    fn node_local_entry(
        purpose: CatalogCredentialPurpose,
        name: &str,
        generation: &str,
    ) -> CatalogCredentialRegistryEntry {
        CatalogCredentialRegistryEntry::try_new_node_local(
            purpose,
            NodeLocalCredentialReference::try_new(name, generation).unwrap(),
            novarocks_fs::AliyunEcsRamRoleConfig::try_new(
                "novarocks-ecs",
                vec!["s3://novarocks-ci/development/".to_string()],
            )
            .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn node_local_lookup_is_typed_purpose_and_generation_exact() {
        use novarocks_connector_iceberg::access_binding::IcebergCredentialResolver;
        let source = node_local_entry(
            CatalogCredentialPurpose::ObjectStoreData,
            "warehouse",
            "blue",
        );
        let registry = CatalogCredentialRegistry::try_new(ClusterRole::Be, vec![source]).unwrap();
        let local = NodeLocalCredentialReference::try_new("warehouse", "blue").unwrap();
        let config = registry
            .resolve_node_local_object_store(CatalogCredentialPurpose::ObjectStoreData, &local)
            .unwrap();
        assert_eq!(config.role_name(), "novarocks-ecs");
        assert!(
            registry
                .resolve_node_local(
                    CatalogCredentialPurpose::ObjectStoreData,
                    &NodeLocalCredentialReference::try_new("warehouse", "green").unwrap()
                )
                .is_none()
        );
        assert!(
            registry
                .resolve_node_local(CatalogCredentialPurpose::ObjectStoreMetadata, &local)
                .is_none()
        );
        assert!(
            registry
                .resolve(
                    CatalogCredentialPurpose::ObjectStoreData,
                    &reference("warehouse", "blue")
                )
                .is_none()
        );
        assert!(
            registry
                .resolve_object_store_static(&reference("warehouse", "blue"))
                .is_err()
        );
    }

    #[test]
    fn equal_static_and_node_local_names_never_alias_material() {
        let local = node_local_entry(
            CatalogCredentialPurpose::ObjectStoreData,
            "warehouse",
            "blue",
        );
        let fixed = CatalogCredentialRegistryEntry::try_new(
            CatalogCredentialPurpose::ObjectStoreData,
            reference("warehouse", "blue"),
            s3("static-canary"),
        )
        .unwrap();
        let registry =
            CatalogCredentialRegistry::try_new(ClusterRole::Be, vec![local, fixed]).unwrap();
        assert_eq!(registry.len(), 2);
        assert_eq!(
            registry
                .resolve(
                    CatalogCredentialPurpose::ObjectStoreData,
                    &reference("warehouse", "blue")
                )
                .unwrap()
                .as_s3()
                .unwrap()
                .access_key_id()
                .expose_secret(),
            "static-canary"
        );
        assert_eq!(
            registry
                .resolve_node_local(
                    CatalogCredentialPurpose::ObjectStoreData,
                    &NodeLocalCredentialReference::try_new("warehouse", "blue").unwrap()
                )
                .unwrap()
                .role_name(),
            "novarocks-ecs"
        );
    }

    #[test]
    fn node_local_definitions_cannot_be_static_control_or_vending_material() {
        let config = novarocks_fs::AliyunEcsRamRoleConfig::try_new(
            "novarocks-ecs",
            vec!["s3://novarocks-ci/".to_string()],
        )
        .unwrap();
        let source = node_local_entry(CatalogCredentialPurpose::ObjectStoreData, "source", "blue");
        assert!(source.material().is_none());
        for purpose in [
            CatalogCredentialPurpose::CatalogControl,
            CatalogCredentialPurpose::DataCredentialVending,
        ] {
            assert!(
                CatalogCredentialRegistryEntry::try_new_node_local(
                    purpose,
                    NodeLocalCredentialReference::try_new("source", "blue").unwrap(),
                    config.clone(),
                )
                .is_err()
            );
        }
        let metadata = node_local_entry(
            CatalogCredentialPurpose::ObjectStoreMetadata,
            "metadata",
            "blue",
        );
        let data = node_local_entry(CatalogCredentialPurpose::ObjectStoreData, "data", "blue");
        assert!(
            CatalogCredentialRegistry::try_new(ClusterRole::Fe, vec![metadata.clone()]).is_ok()
        );
        assert!(CatalogCredentialRegistry::try_new(ClusterRole::Be, vec![metadata]).is_err());
        assert!(CatalogCredentialRegistry::try_new(ClusterRole::Be, vec![data.clone()]).is_ok());
        assert!(CatalogCredentialRegistry::try_new(ClusterRole::Fe, vec![data.clone()]).is_err());
        assert!(
            CatalogCredentialRegistry::try_new(ClusterRole::Be, vec![data.clone(), data]).is_err()
        );
    }

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
    fn a_coordinator_resolver_refuses_the_vending_identity_rather_than_improvising() {
        use novarocks_connector_iceberg::access_binding::IcebergCredentialResolver;

        // A coordinator cannot own this purpose at all, so its resolver has
        // nothing to return. Reporting Unsupported is what lets the authority
        // say "this capability cannot renew" instead of failing vaguely at the
        // first read that needs material.
        let coordinator = CatalogCredentialRegistry::try_new(ClusterRole::Fe, vec![]).unwrap();
        let refused = coordinator.resolve_data_credential_vending(&reference("executor", "v1"));
        assert_eq!(
            refused.unwrap_err().kind(),
            novarocks_spi::connector::ConnectorErrorKind::Unsupported
        );
    }

    #[test]
    fn an_execution_node_resolves_its_own_rest_identity_exactly() {
        use novarocks_connector_iceberg::access_binding::{
            IcebergCredentialResolver, IcebergRestAuthMaterial,
        };

        let registry = CatalogCredentialRegistry::try_new(
            ClusterRole::Be,
            vec![
                CatalogCredentialRegistryEntry::try_new(
                    CatalogCredentialPurpose::DataCredentialVending,
                    reference("executor", "v1"),
                    rest_bearer("executor-token"),
                )
                .unwrap(),
            ],
        )
        .unwrap();

        match registry
            .resolve_data_credential_vending(&reference("executor", "v1"))
            .expect("exact identity")
        {
            IcebergRestAuthMaterial::Bearer { token } => {
                assert_eq!(token.expose_secret(), "executor-token");
            }
            other => panic!("unexpected material: {other:?}"),
        }

        // Exactness carries over from the rest of the registry: a different
        // generation is a different identity, never a fallback.
        assert!(
            registry
                .resolve_data_credential_vending(&reference("executor", "v2"))
                .is_err()
        );
    }

    fn rest_oauth2(client_id: &str, secret: &str) -> CatalogCredentialMaterial {
        CatalogCredentialMaterial::IcebergRestOauth2(
            IcebergRestOauth2CredentialMaterial::new(
                client_id.to_string(),
                SecretValue::new(secret),
            )
            .unwrap(),
        )
    }

    fn control_entry(
        name: &str,
        material: CatalogCredentialMaterial,
    ) -> CatalogCredentialRegistryEntry {
        CatalogCredentialRegistryEntry::try_new(
            CatalogCredentialPurpose::CatalogControl,
            reference(name, "v1"),
            material,
        )
        .unwrap()
    }

    #[test]
    fn a_coordinator_resolves_its_catalog_control_identity_exactly() {
        use novarocks_connector_iceberg::access_binding::{
            IcebergCredentialResolver, IcebergRestAuthMaterial,
        };

        let coordinator = CatalogCredentialRegistry::try_new(
            ClusterRole::Fe,
            vec![
                control_entry("oauth", rest_oauth2("coordinator-client", "client-secret")),
                control_entry("bearer", rest_bearer("control-token")),
            ],
        )
        .unwrap();

        match coordinator
            .resolve_catalog_control(&reference("oauth", "v1"))
            .expect("exact OAuth2 identity")
        {
            IcebergRestAuthMaterial::Oauth2 {
                client_id,
                client_secret,
            } => {
                assert_eq!(client_id, "coordinator-client");
                assert_eq!(client_secret.expose_secret(), "client-secret");
            }
            other => panic!("unexpected material: {other:?}"),
        }
        match coordinator
            .resolve_catalog_control(&reference("bearer", "v1"))
            .expect("exact bearer identity")
        {
            IcebergRestAuthMaterial::Bearer { token } => {
                assert_eq!(token.expose_secret(), "control-token");
            }
            other => panic!("unexpected material: {other:?}"),
        }

        // A declared identity the coordinator does not hold is a deployment
        // error, and a different generation is a different identity.
        let missing = coordinator
            .resolve_catalog_control(&reference("bearer", "v2"))
            .expect_err("no fallback by name");
        assert_eq!(
            missing.kind(),
            novarocks_spi::connector::ConnectorErrorKind::InvalidRequest
        );
    }

    #[test]
    fn an_execution_node_refuses_the_catalog_control_identity() {
        use novarocks_connector_iceberg::access_binding::IcebergCredentialResolver;

        // The execution node's own vending identity under the same reference
        // must not answer a control lookup either.
        let execution_node = CatalogCredentialRegistry::try_new(
            ClusterRole::Be,
            vec![
                CatalogCredentialRegistryEntry::try_new(
                    CatalogCredentialPurpose::DataCredentialVending,
                    reference("executor", "v1"),
                    rest_bearer("executor-token"),
                )
                .unwrap(),
            ],
        )
        .unwrap();
        for name in ["executor", "coordinator"] {
            let refused = execution_node
                .resolve_catalog_control(&reference(name, "v1"))
                .expect_err("an execution node has no control identity");
            assert_eq!(
                refused.kind(),
                novarocks_spi::connector::ConnectorErrorKind::Unsupported
            );
            assert!(!refused.to_string().contains("executor-token"));
        }
    }

    #[test]
    fn only_an_execution_node_may_own_the_vending_identity() {
        // CAD-1 D1 makes this the mirror image of CatalogControl. A coordinator
        // holding a vending identity would be authenticating as the node it
        // dispatches to, which is the whole thing the split exists to prevent.
        let entry = || {
            CatalogCredentialRegistryEntry::try_new(
                CatalogCredentialPurpose::DataCredentialVending,
                reference("executor", "v1"),
                rest_bearer("token"),
            )
            .unwrap()
        };
        assert!(CatalogCredentialRegistry::try_new(ClusterRole::Be, vec![entry()]).is_ok());
        let on_coordinator = CatalogCredentialRegistry::try_new(ClusterRole::Fe, vec![entry()]);
        assert!(on_coordinator.is_err());

        // And the reverse still holds: control identity stays coordinator-only.
        let control = CatalogCredentialRegistryEntry::try_new(
            CatalogCredentialPurpose::CatalogControl,
            reference("coordinator", "v1"),
            rest_bearer("token"),
        )
        .unwrap();
        assert!(CatalogCredentialRegistry::try_new(ClusterRole::Be, vec![control]).is_err());
    }

    #[test]
    fn the_vending_identity_is_a_catalog_credential_not_a_storage_one() {
        // It is exchanged for data credentials; it never signs an object-store
        // request. Handing it S3 material would mean somebody intended the
        // wrong thing.
        assert!(
            CatalogCredentialRegistryEntry::try_new(
                CatalogCredentialPurpose::DataCredentialVending,
                reference("executor", "v1"),
                s3("ak"),
            )
            .is_err()
        );
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
            novarocks_connector_iceberg::access_binding::IcebergCredentialResolver::resolve_object_store_metadata_static(
                &registry,
                &reference,
            )
            .is_ok()
        );
        assert!(
            novarocks_connector_iceberg::access_binding::IcebergCredentialResolver::resolve_object_store_static(
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
