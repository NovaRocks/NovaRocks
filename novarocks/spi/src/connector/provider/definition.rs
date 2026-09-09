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

use std::sync::Arc;

use sha2::{Digest, Sha256};

use crate::connector::{
    ConnectorCodecCategory, ConnectorCodecError, ConnectorCodecErrorKind, ConnectorCodecRevision,
    ConnectorFieldPath, ConnectorProviderId,
};

const MAX_DESCRIPTOR_BYTES: usize = 1024 * 1024;
const MAX_FORMAT_NAME_BYTES: usize = 128;

#[derive(Clone, Debug, Eq, PartialEq)]
// Design: ADR-0137 (docs/adr/ADR-0137-provider-owned-private-connector-wire.md)
pub struct ConnectorCodecDeclaration {
    provider_id: ConnectorProviderId,
    category: ConnectorCodecCategory,
    revision: ConnectorCodecRevision,
    format_name: Arc<str>,
    descriptor: Arc<[u8]>,
    descriptor_sha256: [u8; 32],
}

impl ConnectorCodecDeclaration {
    pub fn try_new(
        provider_id: ConnectorProviderId,
        category: ConnectorCodecCategory,
        revision: ConnectorCodecRevision,
        format_name: impl AsRef<str>,
        descriptor: impl AsRef<[u8]>,
    ) -> Result<Self, ConnectorCodecError> {
        let format_name = format_name.as_ref();
        let descriptor = descriptor.as_ref();
        if format_name.is_empty()
            || format_name.len() > MAX_FORMAT_NAME_BYTES
            || !format_name.is_ascii()
        {
            return Err(invalid(
                "format_name",
                "codec format name must be bounded ASCII",
            ));
        }
        if descriptor.is_empty() || descriptor.len() > MAX_DESCRIPTOR_BYTES {
            return Err(invalid(
                "descriptor",
                "codec descriptor must be non-empty and bounded",
            ));
        }
        let digest: [u8; 32] = Sha256::digest(descriptor).into();
        Ok(Self {
            provider_id,
            category,
            revision,
            format_name: Arc::from(format_name),
            descriptor: Arc::from(descriptor),
            descriptor_sha256: digest,
        })
    }

    pub const fn provider_id(&self) -> &ConnectorProviderId {
        &self.provider_id
    }

    pub const fn category(&self) -> ConnectorCodecCategory {
        self.category
    }

    pub const fn revision(&self) -> ConnectorCodecRevision {
        self.revision
    }

    pub fn format_name(&self) -> &str {
        &self.format_name
    }

    pub fn descriptor(&self) -> &[u8] {
        &self.descriptor
    }

    pub const fn descriptor_sha256(&self) -> &[u8; 32] {
        &self.descriptor_sha256
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderReadCodecDefinitions {
    table: ConnectorCodecDeclaration,
    view: ConnectorCodecDeclaration,
    column: ConnectorCodecDeclaration,
    split: ConnectorCodecDeclaration,
}

impl ProviderReadCodecDefinitions {
    pub fn try_new(
        table: ConnectorCodecDeclaration,
        view: ConnectorCodecDeclaration,
        column: ConnectorCodecDeclaration,
        split: ConnectorCodecDeclaration,
    ) -> Result<Self, ConnectorCodecError> {
        let values = [&table, &view, &column, &split];
        let expected = [
            ConnectorCodecCategory::ReadTable,
            ConnectorCodecCategory::ReadView,
            ConnectorCodecCategory::ReadColumn,
            ConnectorCodecCategory::ReadSplit,
        ];
        validate_group(&values, &expected)?;
        Ok(Self {
            table,
            view,
            column,
            split,
        })
    }

    pub fn declarations(&self) -> [&ConnectorCodecDeclaration; 4] {
        [&self.table, &self.view, &self.column, &self.split]
    }

    pub const fn provider_id(&self) -> &ConnectorProviderId {
        self.table.provider_id()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderWriteCodecDefinitions {
    writer_handle: ConnectorCodecDeclaration,
    commit_fragment: ConnectorCodecDeclaration,
}

impl ProviderWriteCodecDefinitions {
    pub fn try_new(
        writer_handle: ConnectorCodecDeclaration,
        commit_fragment: ConnectorCodecDeclaration,
    ) -> Result<Self, ConnectorCodecError> {
        let values = [&writer_handle, &commit_fragment];
        let expected = [
            ConnectorCodecCategory::WriteHandle,
            ConnectorCodecCategory::CommitFragment,
        ];
        validate_group(&values, &expected)?;
        Ok(Self {
            writer_handle,
            commit_fragment,
        })
    }

    pub fn declarations(&self) -> [&ConnectorCodecDeclaration; 2] {
        [&self.writer_handle, &self.commit_fragment]
    }

    pub const fn provider_id(&self) -> &ConnectorProviderId {
        self.writer_handle.provider_id()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderReadContractDefinition {
    codecs: ProviderReadCodecDefinitions,
}

impl ProviderReadContractDefinition {
    pub const fn new(codecs: ProviderReadCodecDefinitions) -> Self {
        Self { codecs }
    }

    pub const fn codecs(&self) -> &ProviderReadCodecDefinitions {
        &self.codecs
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderWriteContractDefinition {
    codecs: ProviderWriteCodecDefinitions,
}

impl ProviderWriteContractDefinition {
    pub const fn new(codecs: ProviderWriteCodecDefinitions) -> Self {
        Self { codecs }
    }

    pub const fn codecs(&self) -> &ProviderWriteCodecDefinitions {
        &self.codecs
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderContractDefinition {
    provider_id: ConnectorProviderId,
    read: ProviderReadContractDefinition,
    write: Option<ProviderWriteContractDefinition>,
}

impl ProviderContractDefinition {
    pub fn read_only(
        provider_id: ConnectorProviderId,
        read: ProviderReadContractDefinition,
    ) -> Result<Self, ConnectorCodecError> {
        validate_provider(&provider_id, read.codecs().provider_id())?;
        Ok(Self {
            provider_id,
            read,
            write: None,
        })
    }

    pub fn read_write(
        provider_id: ConnectorProviderId,
        read: ProviderReadContractDefinition,
        write: ProviderWriteContractDefinition,
    ) -> Result<Self, ConnectorCodecError> {
        validate_provider(&provider_id, read.codecs().provider_id())?;
        validate_provider(&provider_id, write.codecs().provider_id())?;
        Ok(Self {
            provider_id,
            read,
            write: Some(write),
        })
    }

    pub const fn provider_id(&self) -> &ConnectorProviderId {
        &self.provider_id
    }

    pub const fn read(&self) -> &ProviderReadContractDefinition {
        &self.read
    }

    pub const fn write(&self) -> Option<&ProviderWriteContractDefinition> {
        self.write.as_ref()
    }

    pub fn declarations(&self) -> Vec<&ConnectorCodecDeclaration> {
        let mut result = self.read.codecs().declarations().to_vec();
        if let Some(write) = &self.write {
            result.extend(write.codecs().declarations());
        }
        result
    }
}

fn validate_group(
    values: &[&ConnectorCodecDeclaration],
    expected: &[ConnectorCodecCategory],
) -> Result<(), ConnectorCodecError> {
    let provider_id = values[0].provider_id();
    for (index, (value, category)) in values.iter().zip(expected).enumerate() {
        if value.provider_id() != provider_id {
            return Err(ConnectorCodecError::new(
                ConnectorFieldPath::root("codecs")
                    .index(index)
                    .field("provider_id"),
                ConnectorCodecErrorKind::InconsistentFields,
                "codec group contains more than one provider identity",
            ));
        }
        if value.category() != *category {
            return Err(ConnectorCodecError::new(
                ConnectorFieldPath::root("codecs")
                    .index(index)
                    .field("category"),
                ConnectorCodecErrorKind::InconsistentFields,
                "codec occupies the wrong capability category",
            ));
        }
    }
    Ok(())
}

fn validate_provider(
    expected: &ConnectorProviderId,
    actual: &ConnectorProviderId,
) -> Result<(), ConnectorCodecError> {
    if expected != actual {
        return Err(ConnectorCodecError::new(
            ConnectorFieldPath::root("provider_definition").field("provider_id"),
            ConnectorCodecErrorKind::InconsistentFields,
            "provider definition and codec group identities differ",
        ));
    }
    Ok(())
}

fn invalid(field: &'static str, detail: &'static str) -> ConnectorCodecError {
    ConnectorCodecError::new(
        ConnectorFieldPath::root("codec").field(field),
        ConnectorCodecErrorKind::InvalidValue,
        detail,
    )
}
