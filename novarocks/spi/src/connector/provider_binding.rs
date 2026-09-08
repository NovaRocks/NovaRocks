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

// Design: ADR-0125 (docs/adr/ADR-0125-query-leased-catalog-runtime-and-provider-binding-evidence.md)
use std::sync::Arc;

use super::{
    ConnectorError, ConnectorErrorKind, ConnectorInstanceId, ConnectorProviderId,
    ProviderBindingEpoch,
};

const MAX_LOCAL_BINDING_BYTES: usize = 256;

/// Immutable provider-private identity used to fence FE effects and late
/// materialization. It is not a BE execution identity and never crosses the
/// native fragment or terminal-report wire contracts.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorProviderBindingKey {
    pub instance_id: ConnectorInstanceId,
    pub incarnation: ProviderBindingEpoch,
}

impl ConnectorProviderBindingKey {
    pub fn instance_id(&self) -> &str {
        self.instance_id.as_str()
    }

    pub fn incarnation(&self) -> [u8; 16] {
        self.incarnation.to_bytes()
    }
}

/// A validated provider binding admitted by connector control. The provider
/// identity is open; only its installed provider may interpret `local_binding`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorProviderBinding {
    binding_key: ConnectorProviderBindingKey,
    provider_id: ConnectorProviderId,
    local_binding: Arc<str>,
}

impl ConnectorProviderBinding {
    pub fn try_new(
        provider_id: ConnectorProviderId,
        instance_id: impl AsRef<str>,
        incarnation: [u8; 16],
        local_binding: impl AsRef<str>,
    ) -> Result<Self, ConnectorError> {
        Ok(Self {
            binding_key: ConnectorProviderBindingKey {
                instance_id: ConnectorInstanceId::try_from_canonical(instance_id.as_ref())?,
                incarnation: ProviderBindingEpoch::from_bytes(incarnation),
            },
            provider_id,
            local_binding: bounded_binding(local_binding.as_ref())?,
        })
    }

    pub fn iceberg(
        instance_id: impl AsRef<str>,
        incarnation: [u8; 16],
        access_binding: impl AsRef<str>,
    ) -> Result<Self, ConnectorError> {
        Self::try_new(
            ConnectorProviderId::parse("iceberg")
                .expect("the static Iceberg provider identity is valid"),
            instance_id,
            incarnation,
            access_binding,
        )
    }

    pub fn starrocks(
        instance_id: impl AsRef<str>,
        incarnation: [u8; 16],
        local_binding: impl AsRef<str>,
    ) -> Result<Self, ConnectorError> {
        Self::try_new(
            ConnectorProviderId::parse("starrocks")
                .expect("the static StarRocks provider identity is valid"),
            instance_id,
            incarnation,
            local_binding,
        )
    }

    pub fn binding_key(&self) -> &ConnectorProviderBindingKey {
        &self.binding_key
    }

    pub const fn provider_id(&self) -> &ConnectorProviderId {
        &self.provider_id
    }

    pub fn local_binding(&self) -> &str {
        &self.local_binding
    }

    pub fn iceberg_access_binding(&self) -> Option<&str> {
        (self.provider_id.as_str() == "iceberg").then_some(&self.local_binding)
    }

    pub fn starrocks_local_binding(&self) -> Option<&str> {
        (self.provider_id.as_str() == "starrocks").then_some(&self.local_binding)
    }
}

impl From<&ConnectorProviderBinding> for ConnectorProviderBindingKey {
    fn from(binding: &ConnectorProviderBinding) -> Self {
        binding.binding_key.clone()
    }
}

fn bounded_binding(value: &str) -> Result<Arc<str>, ConnectorError> {
    if value.is_empty() || value.len() > MAX_LOCAL_BINDING_BYTES || !value.is_ascii() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector provider local binding must be non-empty bounded ASCII",
        ));
    }
    Ok(Arc::from(value))
}

#[cfg(test)]
mod tests {
    use super::ConnectorProviderBinding;
    use crate::connector::ConnectorProviderId;

    #[test]
    fn an_open_provider_identity_owns_one_bounded_local_binding() {
        let binding = ConnectorProviderBinding::try_new(
            ConnectorProviderId::parse("paimon").unwrap(),
            "catalog",
            [1; 16],
            "filesystem",
        )
        .unwrap();
        assert_eq!(binding.provider_id().as_str(), "paimon");
        assert_eq!(binding.local_binding(), "filesystem");
        assert!(binding.iceberg_access_binding().is_none());
    }
}
