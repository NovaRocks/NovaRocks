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

// Design: ADR-0120 (docs/adr/ADR-0120-connector-binding-restart-reconciliation.md)
use std::sync::Arc;

use super::{
    ConnectorError, ConnectorErrorKind, ConnectorInstanceId, ConnectorInstanceIncarnation,
};

const MAX_LOCAL_BINDING_BYTES: usize = 256;

/// The closed provider variant carried by an execution declaration.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ConnectorExecutionProviderKind {
    Iceberg,
    StarRocks,
}

impl ConnectorExecutionProviderKind {
    pub const ALL: [Self; 2] = [Self::Iceberg, Self::StarRocks];

    pub const fn provider_id(self) -> &'static str {
        match self {
            Self::Iceberg => "iceberg",
            Self::StarRocks => "starrocks",
        }
    }
}

/// Immutable identity shared by the FE control and BE execution processes.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorExecutionBindingKey {
    pub instance_id: ConnectorInstanceId,
    pub incarnation: ConnectorInstanceIncarnation,
}

impl ConnectorExecutionBindingKey {
    pub fn instance_id(&self) -> &str {
        self.instance_id.as_str()
    }

    pub fn incarnation(&self) -> [u8; 16] {
        self.incarnation.to_bytes()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ConnectorExecutionBindingProvider {
    Iceberg { access_binding: Arc<str> },
    StarRocks { local_binding: Arc<str> },
}

/// Borrowed, transport-neutral provider facts from a validated execution
/// declaration.  Consumers must match this closed enum rather than infer a
/// provider from an identifier string.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorExecutionDeclarationProvider<'a> {
    Iceberg { access_binding: &'a str },
    StarRocks { local_binding: &'a str },
}

impl ConnectorExecutionDeclarationProvider<'_> {
    pub const fn kind(self) -> ConnectorExecutionProviderKind {
        match self {
            Self::Iceberg { .. } => ConnectorExecutionProviderKind::Iceberg,
            Self::StarRocks { .. } => ConnectorExecutionProviderKind::StarRocks,
        }
    }
}

impl ConnectorExecutionBindingProvider {
    const fn kind(&self) -> ConnectorExecutionProviderKind {
        match self {
            Self::Iceberg { .. } => ConnectorExecutionProviderKind::Iceberg,
            Self::StarRocks { .. } => ConnectorExecutionProviderKind::StarRocks,
        }
    }
}

/// Transport-neutral, validated declaration admitted by connector control.
///
/// Its fields remain private so a provider binding can only be constructed
/// through the bounded constructors below.  Protocol adapters are owned by
/// the FE and BE applications, not by SPI.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorExecutionDeclaration {
    binding_key: ConnectorExecutionBindingKey,
    provider: ConnectorExecutionBindingProvider,
}

impl ConnectorExecutionDeclaration {
    pub fn iceberg(
        instance_id: impl AsRef<str>,
        incarnation: [u8; 16],
        access_binding: impl AsRef<str>,
    ) -> Result<Self, ConnectorError> {
        Self::try_new(
            instance_id.as_ref(),
            incarnation,
            ConnectorExecutionBindingProvider::Iceberg {
                access_binding: bounded_binding(access_binding.as_ref())?,
            },
        )
    }

    pub fn starrocks(
        instance_id: impl AsRef<str>,
        incarnation: [u8; 16],
        local_binding: impl AsRef<str>,
    ) -> Result<Self, ConnectorError> {
        Self::try_new(
            instance_id.as_ref(),
            incarnation,
            ConnectorExecutionBindingProvider::StarRocks {
                local_binding: bounded_binding(local_binding.as_ref())?,
            },
        )
    }

    fn try_new(
        instance_id: &str,
        incarnation: [u8; 16],
        provider: ConnectorExecutionBindingProvider,
    ) -> Result<Self, ConnectorError> {
        Ok(Self {
            binding_key: ConnectorExecutionBindingKey {
                instance_id: ConnectorInstanceId::try_from_canonical(instance_id)?,
                incarnation: ConnectorInstanceIncarnation::from_bytes(incarnation),
            },
            provider,
        })
    }

    pub fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.binding_key
    }

    pub fn provider(&self) -> ConnectorExecutionDeclarationProvider<'_> {
        match &self.provider {
            ConnectorExecutionBindingProvider::Iceberg { access_binding } => {
                ConnectorExecutionDeclarationProvider::Iceberg { access_binding }
            }
            ConnectorExecutionBindingProvider::StarRocks { local_binding } => {
                ConnectorExecutionDeclarationProvider::StarRocks { local_binding }
            }
        }
    }

    pub const fn provider_kind(&self) -> ConnectorExecutionProviderKind {
        self.provider.kind()
    }

    pub const fn provider_id(&self) -> &'static str {
        self.provider_kind().provider_id()
    }

    pub fn iceberg_access_binding(&self) -> Option<&str> {
        match &self.provider {
            ConnectorExecutionBindingProvider::Iceberg { access_binding } => Some(access_binding),
            ConnectorExecutionBindingProvider::StarRocks { .. } => None,
        }
    }

    pub fn starrocks_local_binding(&self) -> Option<&str> {
        match &self.provider {
            ConnectorExecutionBindingProvider::Iceberg { .. } => None,
            ConnectorExecutionBindingProvider::StarRocks { local_binding } => Some(local_binding),
        }
    }
}

impl From<&ConnectorExecutionDeclaration> for ConnectorExecutionBindingKey {
    fn from(declaration: &ConnectorExecutionDeclaration) -> Self {
        declaration.binding_key.clone()
    }
}

fn bounded_binding(value: &str) -> Result<Arc<str>, ConnectorError> {
    if value.is_empty() || value.len() > MAX_LOCAL_BINDING_BYTES || !value.is_ascii() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector execution local binding must be non-empty bounded ASCII",
        ));
    }
    Ok(Arc::from(value))
}

#[cfg(test)]
mod tests {
    use super::{
        ConnectorExecutionDeclaration, ConnectorExecutionDeclarationProvider,
        ConnectorExecutionProviderKind,
    };

    #[test]
    fn constructors_validate_canonical_identity_and_local_binding() {
        assert!(ConnectorExecutionDeclaration::iceberg("MyCatalog", [1; 16], "local").is_err());
        assert!(ConnectorExecutionDeclaration::iceberg("catalog", [1; 16], "").is_err());
        assert!(
            ConnectorExecutionDeclaration::starrocks("catalog", [1; 16], "x".repeat(257)).is_err()
        );
        let declaration =
            ConnectorExecutionDeclaration::iceberg("catalog", [1; 16], "local").unwrap();
        assert_eq!(
            declaration.provider_kind(),
            ConnectorExecutionProviderKind::Iceberg
        );
        assert_eq!(declaration.provider_id(), "iceberg");
        assert!(matches!(
            declaration.provider(),
            ConnectorExecutionDeclarationProvider::Iceberg {
                access_binding: "local"
            }
        ));
    }
}
