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

//! Immutable catalog identity and materialization inputs.
//!
//! These values deliberately describe catalog content, not a Frontend process
//! generation, query attempt, or operation owner.  FE derives a version from
//! durable desired state; BE compares the validated value exactly and never
//! recomputes it from provider configuration.

use std::{fmt::Write, sync::Arc};

use uuid::Uuid;

use super::{
    CatalogCredentialBinding, ConnectorError, ConnectorErrorKind, ConnectorInstanceId,
    ConnectorProviderId, canonicalize_catalog_credential_bindings,
};

pub const CATALOG_VERSION_BYTES: usize = 32;
pub const MAX_CATALOGS_PER_QUERY: usize = 256;
pub const MAX_CATALOG_SET_BYTES: usize = 1024 * 1024;
/// A periodic prune is a cluster reachability snapshot, rather than one
/// query's dependency set, so it has a separately bounded larger envelope.
pub const MAX_REACHABLE_CATALOGS_PER_PRUNE: usize = 65_536;
pub const MAX_PRUNE_CATALOG_SET_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_CATALOG_PROPERTIES: usize = 128;
pub const MAX_CATALOG_PROPERTY_KEY_BYTES: usize = 256;
pub const MAX_CATALOG_PROPERTY_VALUE_BYTES: usize = 4 * 1024;

/// Stable content identity for one catalog configuration.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CatalogVersion([u8; CATALOG_VERSION_BYTES]);

impl CatalogVersion {
    pub const fn from_bytes(bytes: [u8; CATALOG_VERSION_BYTES]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; CATALOG_VERSION_BYTES] {
        &self.0
    }

    /// A bounded rendering suitable for diagnostics, never a metric label.
    pub fn short_hex(self) -> String {
        let mut result = String::with_capacity(16);
        for byte in &self.0[..8] {
            write!(&mut result, "{byte:02x}").expect("writing to String cannot fail");
        }
        result
    }
}

/// The exact catalog content a read or write execution artifact uses.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CatalogHandle {
    catalog_name: ConnectorInstanceId,
    version: CatalogVersion,
}

impl CatalogHandle {
    pub const fn new(catalog_name: ConnectorInstanceId, version: CatalogVersion) -> Self {
        Self {
            catalog_name,
            version,
        }
    }

    pub const fn catalog_name(&self) -> &ConnectorInstanceId {
        &self.catalog_name
    }

    pub const fn version(&self) -> CatalogVersion {
        self.version
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CatalogProperty {
    key: Arc<str>,
    value: Arc<str>,
}

impl CatalogProperty {
    pub fn new(key: impl AsRef<str>, value: impl AsRef<str>) -> Result<Self, ConnectorError> {
        let key = key.as_ref();
        if !is_property_key(key) || is_sensitive_property_key(key) {
            return Err(invalid("catalog property key"));
        }
        let value = value.as_ref();
        if value.is_empty() || value.len() > MAX_CATALOG_PROPERTY_VALUE_BYTES {
            return Err(invalid("catalog property value"));
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

/// Bounded, credential-free input used to materialize one BE-local runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogProperties {
    handle: CatalogHandle,
    provider_id: ConnectorProviderId,
    config_format_version: u32,
    execution_properties: Vec<CatalogProperty>,
    credential_bindings: Vec<CatalogCredentialBinding>,
}

impl CatalogProperties {
    pub fn new(
        handle: CatalogHandle,
        provider_id: ConnectorProviderId,
        config_format_version: u32,
        mut execution_properties: Vec<CatalogProperty>,
        credential_bindings: Vec<CatalogCredentialBinding>,
    ) -> Result<Self, ConnectorError> {
        if config_format_version == 0 {
            return Err(invalid("catalog config format version"));
        }
        if execution_properties.len() > MAX_CATALOG_PROPERTIES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "catalog properties exceed configured bounds",
            ));
        }
        execution_properties.sort_by(|left, right| left.key.cmp(&right.key));
        if execution_properties
            .windows(2)
            .any(|pair| pair[0].key == pair[1].key)
        {
            return Err(invalid("duplicate catalog property key"));
        }
        let credential_bindings = canonicalize_catalog_credential_bindings(credential_bindings)?;
        Ok(Self {
            handle,
            provider_id,
            config_format_version,
            execution_properties,
            credential_bindings,
        })
    }

    pub const fn handle(&self) -> &CatalogHandle {
        &self.handle
    }

    pub const fn provider_id(&self) -> &ConnectorProviderId {
        &self.provider_id
    }

    pub const fn config_format_version(&self) -> u32 {
        self.config_format_version
    }

    pub fn execution_properties(&self) -> &[CatalogProperty] {
        &self.execution_properties
    }

    pub fn credential_bindings(&self) -> &[CatalogCredentialBinding] {
        &self.credential_bindings
    }
}

/// The process-local identity of an FE control runtime.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorControlRuntimeId(Uuid);

impl ConnectorControlRuntimeId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }

    pub const fn to_bytes(self) -> [u8; 16] {
        *self.0.as_bytes()
    }
}

impl Default for ConnectorControlRuntimeId {
    fn default() -> Self {
        Self::new()
    }
}

fn is_property_key(value: &str) -> bool {
    value.len() <= MAX_CATALOG_PROPERTY_KEY_BYTES
        && !value.is_empty()
        && value.as_bytes()[0].is_ascii_lowercase()
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'.' | b'-')
        })
}

fn is_sensitive_property_key(value: &str) -> bool {
    ["secret", "password", "token", "credential", "access_key"]
        .iter()
        .any(|marker| value.contains(marker))
}

fn invalid(subject: &str) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::InvalidRequest,
        format!("invalid {subject}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn handle(version: u8) -> CatalogHandle {
        CatalogHandle::new(
            ConnectorInstanceId::try_from_canonical("catalog").unwrap(),
            CatalogVersion::from_bytes([version; CATALOG_VERSION_BYTES]),
        )
    }

    #[test]
    fn catalog_handle_is_name_and_content_version() {
        assert_eq!(handle(1), handle(1));
        assert_ne!(handle(1), handle(2));
        assert_eq!(handle(1).version().short_hex(), "0101010101010101");
    }

    #[test]
    fn properties_sort_and_reject_duplicate_or_secret_keys() {
        let properties = CatalogProperties::new(
            handle(1),
            ConnectorProviderId::parse("iceberg").unwrap(),
            1,
            vec![
                CatalogProperty::new("warehouse", "s3://warehouse").unwrap(),
                CatalogProperty::new("catalog_uri", "http://catalog").unwrap(),
            ],
            vec![],
        )
        .unwrap();
        assert_eq!(properties.execution_properties()[0].key(), "catalog_uri");
        assert!(CatalogProperty::new("password", "leak").is_err());
        assert!(
            CatalogProperties::new(
                handle(1),
                ConnectorProviderId::parse("iceberg").unwrap(),
                1,
                vec![
                    CatalogProperty::new("warehouse", "one").unwrap(),
                    CatalogProperty::new("warehouse", "two").unwrap(),
                ],
                vec![],
            )
            .is_err()
        );
    }

    #[test]
    fn control_runtime_identity_is_not_catalog_version() {
        let control = ConnectorControlRuntimeId::from_bytes([7; 16]);
        assert_eq!(control.to_bytes(), [7; 16]);
        assert_ne!(
            handle(7).version().as_bytes().len(),
            control.to_bytes().len()
        );
    }
}
