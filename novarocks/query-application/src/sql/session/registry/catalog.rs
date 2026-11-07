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

//! Catalog declarations describe the original admitted instance, not a provider-wide default.
use super::*;
use novarocks_spi::connector::{ConnectorInstanceId, ConnectorProviderId};
use uuid::Uuid;

/// A neutral projection of CatalogRuntimeObservation. This is metadata, not a
/// lease, permission or Provider I/O capability. FE supplies the original fact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogPropertyIdentity {
    pub attachment_id: Uuid,
    pub instance_id: ConnectorInstanceId,
    pub provider_id: ConnectorProviderId,
    pub generation: u64,
}

/// The current sealed providers explicitly declare an empty set. Adding a
/// property later replaces this factory at the original composition owner.
pub struct EmptyCatalogPropertyDeclarationFactory {
    provider: ConnectorProviderId,
}
impl EmptyCatalogPropertyDeclarationFactory {
    pub fn new(provider: ConnectorProviderId) -> Self {
        Self { provider }
    }
}
impl CatalogPropertyDeclarationFactory for EmptyCatalogPropertyDeclarationFactory {
    fn provider_id(&self) -> &ConnectorProviderId {
        &self.provider
    }
    fn declarations(
        &self,
        _identity: &CatalogPropertyIdentity,
        _configuration: &[(String, String)],
    ) -> Result<Vec<PropertyDeclaration>, SessionPropertyError> {
        Ok(Vec::new())
    }
}

pub trait CatalogPropertyDeclarationFactory: Send + Sync {
    fn provider_id(&self) -> &ConnectorProviderId;
    /// Configuration is borrowed from the same already validated instance.
    /// The factory is pure and may capture that configuration in declarations.
    fn declarations(
        &self,
        identity: &CatalogPropertyIdentity,
        configuration: &[(String, String)],
    ) -> Result<Vec<PropertyDeclaration>, SessionPropertyError>;
}

#[derive(Clone, Debug)]
pub struct CatalogPropertyMetadata {
    catalog: String,
    identity: Option<CatalogPropertyIdentity>,
    declarations: BTreeMap<String, Arc<PropertyDeclaration>>,
}
impl CatalogPropertyMetadata {
    pub fn local(catalog: impl Into<String>) -> Self {
        Self {
            catalog: catalog.into(),
            identity: None,
            declarations: BTreeMap::new(),
        }
    }
    pub fn catalog(&self) -> &str {
        &self.catalog
    }
    pub fn identity(&self) -> Option<&CatalogPropertyIdentity> {
        self.identity.as_ref()
    }
    pub fn declarations(&self) -> impl Iterator<Item = &PropertyDeclaration> {
        self.declarations.values().map(Arc::as_ref)
    }
    pub fn lookup(&self, name: &str) -> Result<Arc<PropertyDeclaration>, SessionPropertyError> {
        self.declarations
            .get(name)
            .cloned()
            .ok_or_else(|| SessionPropertyError::UnknownProperty {
                name: format!("{}.{name}", self.catalog),
            })
    }
    pub fn prepare_header(
        &self,
        catalog: &str,
        name: &str,
        raw: &str,
    ) -> Result<ValidatedSessionProperty, SessionPropertyError> {
        if self.catalog != catalog {
            return Err(SessionPropertyError::CatalogIdentityMismatch);
        }
        let declaration = self.lookup(name)?;
        let value = decode_header(&declaration, raw).map_err(|error| match error {
            SessionPropertyError::InvalidValue { detail, .. } => {
                SessionPropertyError::InvalidValue {
                    name: format!("{catalog}.{name}"),
                    detail,
                }
            }
            original => original,
        })?;
        Ok(ValidatedSessionProperty {
            declaration,
            value,
            raw: raw.to_owned(),
            scope: SessionPropertyScope::Catalog {
                catalog: self.catalog.clone(),
                identity: self.identity.clone(),
            },
            constant_source: None,
        })
    }
}
impl SessionPropertyRegistry {
    /// The application resolves the original instance/configuration once. No
    /// metadata cache here can accidentally reuse another catalog generation.
    pub fn bind_catalog(
        &self,
        catalog: &str,
        identity: CatalogPropertyIdentity,
        configuration: &[(String, String)],
    ) -> Result<CatalogPropertyMetadata, SessionPropertyError> {
        if identity.instance_id.as_str() != catalog {
            return Err(SessionPropertyError::CatalogIdentityMismatch);
        }
        let values = match self.factories.get(identity.provider_id.as_str()) {
            Some(factory) => factory.declarations(&identity, configuration)?,
            None => {
                return Err(SessionPropertyError::InvalidDeclaration {
                    detail: "Catalog provider has no session property declaration factory",
                });
            }
        };
        if values.iter().any(|value| value.project.is_some()) {
            return Err(SessionPropertyError::ProjectionInvariant);
        }
        Ok(CatalogPropertyMetadata {
            catalog: catalog.to_owned(),
            identity: Some(identity),
            declarations: compose_declarations(values)?,
        })
    }
}

pub enum SessionCatalogPropertyAdmission {
    Absent,
    Unavailable(QueryServiceError),
    Ready(Arc<CatalogPropertyMetadata>),
}
/// This port only observes the existing admission and immutable declarations.
/// It must not materialize a Provider, create a catalog, or acquire a new lease.
pub trait SessionCatalogPropertyPort: Send + Sync {
    fn session_property_catalog(
        &self,
        catalog: &str,
    ) -> Result<SessionCatalogPropertyAdmission, QueryServiceError>;
}
