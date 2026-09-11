// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{fmt, sync::Arc};

use bytes::Bytes;

use super::{ConnectorError, ConnectorErrorKind, ConnectorProviderId, ConnectorTableObjectId};

pub const MAX_CONNECTOR_SEMANTIC_FACT_FORMAT_BYTES: usize = 128;
pub const MAX_CONNECTOR_SEMANTIC_FACT_VALUE_BYTES: usize = 256;

const TABLE_OBJECT_ID_FORMAT: &str = "connector-table-object-id";
const SNAPSHOT_ID_FORMAT: &str = "connector-snapshot-id";
const SEMANTIC_FACT_VERSION_V1: u16 = 1;

/// A provider-issued, process-independent semantic fact.
///
/// Consumers may compare the complete value but cannot construct or interpret
/// one without going through a Connector provider or another SPI-owned
/// observation adapter.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorSemanticFact {
    provider: ConnectorProviderId,
    format: Arc<str>,
    version: u16,
    value: Bytes,
}

impl ConnectorSemanticFact {
    fn try_new(
        provider: ConnectorProviderId,
        format: impl Into<Arc<str>>,
        version: u16,
        value: Bytes,
    ) -> Result<Self, ConnectorError> {
        let format = format.into();
        if format.is_empty()
            || format.len() > MAX_CONNECTOR_SEMANTIC_FACT_FORMAT_BYTES
            || !format.is_ascii()
            || version == 0
            || value.is_empty()
            || value.len() > MAX_CONNECTOR_SEMANTIC_FACT_VALUE_BYTES
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector semantic revision fact is invalid",
            ));
        }
        Ok(Self {
            provider,
            format,
            version,
            value,
        })
    }

    pub const fn provider(&self) -> &ConnectorProviderId {
        &self.provider
    }

    pub fn format(&self) -> &str {
        &self.format
    }

    pub const fn version(&self) -> u16 {
        self.version
    }

    pub const fn value(&self) -> &Bytes {
        &self.value
    }
}

impl fmt::Debug for ConnectorSemanticFact {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectorSemanticFact")
            .field("provider", &self.provider)
            .field("format", &self.format)
            .field("version", &self.version)
            .field("value_len", &self.value.len())
            .finish()
    }
}

/// Stable identity and data-version facts for one exact table observation.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorExactSemanticRevision {
    object_identity: ConnectorSemanticFact,
    data_version: ConnectorSemanticFact,
}

impl ConnectorExactSemanticRevision {
    /// Build the common table-object/snapshot revision used by providers whose
    /// catalog publishes a stable object ID and an exact numeric snapshot.
    pub fn try_from_table_object_and_snapshot(
        provider: ConnectorProviderId,
        object_identity: &ConnectorTableObjectId,
        snapshot_id: Option<i64>,
    ) -> Result<Self, ConnectorError> {
        let object_identity = ConnectorSemanticFact::try_new(
            provider.clone(),
            TABLE_OBJECT_ID_FORMAT,
            SEMANTIC_FACT_VERSION_V1,
            object_identity.as_bytes().clone(),
        )?;
        let mut encoded_snapshot = Vec::with_capacity(9);
        match snapshot_id {
            Some(snapshot_id) => {
                encoded_snapshot.push(1);
                encoded_snapshot.extend_from_slice(&snapshot_id.to_be_bytes());
            }
            None => encoded_snapshot.push(0),
        }
        let data_version = ConnectorSemanticFact::try_new(
            provider,
            SNAPSHOT_ID_FORMAT,
            SEMANTIC_FACT_VERSION_V1,
            Bytes::from(encoded_snapshot),
        )?;
        Ok(Self {
            object_identity,
            data_version,
        })
    }

    pub const fn object_identity(&self) -> &ConnectorSemanticFact {
        &self.object_identity
    }

    pub const fn data_version(&self) -> &ConnectorSemanticFact {
        &self.data_version
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn semantic_revision_distinguishes_object_and_snapshot() {
        let provider = ConnectorProviderId::parse("iceberg").unwrap();
        let object = ConnectorTableObjectId::try_new(Bytes::from_static(b"table-a")).unwrap();
        let other = ConnectorTableObjectId::try_new(Bytes::from_static(b"table-b")).unwrap();
        let first = ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
            provider.clone(),
            &object,
            Some(101),
        )
        .unwrap();
        assert_ne!(
            first,
            ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
                provider.clone(),
                &object,
                Some(102),
            )
            .unwrap()
        );
        assert_ne!(
            first,
            ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
                provider,
                &other,
                Some(101),
            )
            .unwrap()
        );
    }
}
