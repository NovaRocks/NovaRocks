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

use std::{error::Error, fmt};

use bytes::Bytes;

use crate::{CatalogHandle, ConnectorProviderId, ConnectorReadRelationKind};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ConnectorCodecCategory {
    ReadTable,
    ReadView,
    ReadColumn,
    ReadSplit,
    WriteHandle,
    CommitFragment,
    WriteLayout,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ConnectorCodecContractError {
    RevisionMustBeNonZero,
    ProviderMismatch,
    CatalogMismatch,
    CategoryMismatch,
    RevisionMismatch,
}

impl fmt::Display for ConnectorCodecContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::RevisionMustBeNonZero => "connector codec revision must be non-zero",
            Self::ProviderMismatch | Self::CatalogMismatch | Self::CategoryMismatch => {
                "connector envelope header does not match the installed binding"
            }
            Self::RevisionMismatch => {
                "connector codec revision does not match the installed definition"
            }
        })
    }
}

impl Error for ConnectorCodecContractError {}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorCodecRevision(u32);

impl ConnectorCodecRevision {
    pub fn try_new(value: u32) -> Result<Self, ConnectorCodecContractError> {
        if value == 0 {
            return Err(ConnectorCodecContractError::RevisionMustBeNonZero);
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ConnectorEnvelopeHeader {
    provider_id: ConnectorProviderId,
    catalog: CatalogHandle,
    category: ConnectorCodecCategory,
    codec_revision: ConnectorCodecRevision,
}

impl ConnectorEnvelopeHeader {
    pub const fn new(
        provider_id: ConnectorProviderId,
        catalog: CatalogHandle,
        category: ConnectorCodecCategory,
        codec_revision: ConnectorCodecRevision,
    ) -> Self {
        Self {
            provider_id,
            catalog,
            category,
            codec_revision,
        }
    }

    pub const fn provider_id(&self) -> &ConnectorProviderId {
        &self.provider_id
    }

    pub const fn catalog(&self) -> &CatalogHandle {
        &self.catalog
    }

    pub const fn category(&self) -> ConnectorCodecCategory {
        self.category
    }

    pub const fn codec_revision(&self) -> ConnectorCodecRevision {
        self.codec_revision
    }

    pub fn validate_expected<E>(
        &self,
        provider_id: &ConnectorProviderId,
        catalog: &CatalogHandle,
        category: ConnectorCodecCategory,
        codec_revision: ConnectorCodecRevision,
    ) -> Result<(), E>
    where
        E: From<ConnectorCodecContractError>,
    {
        if &self.provider_id != provider_id {
            return Err(ConnectorCodecContractError::ProviderMismatch.into());
        }
        if &self.catalog != catalog {
            return Err(ConnectorCodecContractError::CatalogMismatch.into());
        }
        if self.category != category {
            return Err(ConnectorCodecContractError::CategoryMismatch.into());
        }
        if self.codec_revision != codec_revision {
            return Err(ConnectorCodecContractError::RevisionMismatch.into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ConnectorEncodedPayload {
    header: ConnectorEnvelopeHeader,
    payload: Bytes,
}

impl ConnectorEncodedPayload {
    pub fn new(header: ConnectorEnvelopeHeader, payload: Bytes) -> Self {
        Self { header, payload }
    }

    pub const fn header(&self) -> &ConnectorEnvelopeHeader {
        &self.header
    }

    pub const fn payload(&self) -> &Bytes {
        &self.payload
    }

    pub fn into_parts(self) -> (ConnectorEnvelopeHeader, Bytes) {
        (self.header, self.payload)
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ConnectorReadRelationPayload {
    kind: ConnectorReadRelationKind,
    table: ConnectorEncodedPayload,
    view: ConnectorEncodedPayload,
}

impl ConnectorReadRelationPayload {
    pub const fn new(
        kind: ConnectorReadRelationKind,
        table: ConnectorEncodedPayload,
        view: ConnectorEncodedPayload,
    ) -> Self {
        Self { kind, table, view }
    }

    pub const fn kind(&self) -> ConnectorReadRelationKind {
        self.kind
    }

    pub const fn table(&self) -> &ConnectorEncodedPayload {
        &self.table
    }

    pub const fn view(&self) -> &ConnectorEncodedPayload {
        &self.view
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CatalogVersion, ConnectorInstanceId};

    #[test]
    fn header_validation_preserves_typed_mismatch_reason() {
        let provider = ConnectorProviderId::parse("iceberg").unwrap();
        let catalog = CatalogHandle::new(
            ConnectorInstanceId::try_from_canonical("lake").unwrap(),
            CatalogVersion::from_bytes([1; 32]),
        );
        let revision = ConnectorCodecRevision::try_new(1).unwrap();
        let header = ConnectorEnvelopeHeader::new(
            provider.clone(),
            catalog.clone(),
            ConnectorCodecCategory::ReadTable,
            revision,
        );
        assert_eq!(
            header.validate_expected::<ConnectorCodecContractError>(
                &provider,
                &catalog,
                ConnectorCodecCategory::ReadView,
                revision,
            ),
            Err(ConnectorCodecContractError::CategoryMismatch)
        );
    }
}
