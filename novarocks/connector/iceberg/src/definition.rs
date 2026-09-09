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

//! Static Iceberg provider contract.

use novarocks_spi::connector::provider::{
    ConnectorCodecDeclaration, ProviderContractDefinition, ProviderReadCodecDefinitions,
    ProviderReadContractDefinition, ProviderWriteCodecDefinitions, ProviderWriteContractDefinition,
};
use novarocks_spi::connector::{
    ConnectorCodecCategory, ConnectorCodecError, ConnectorCodecRevision, ConnectorProviderId,
};

use crate::{PROVIDER_ID, wire::FILE_DESCRIPTOR_SET};

pub fn iceberg_contract_definition() -> Result<ProviderContractDefinition, ConnectorCodecError> {
    let provider_id = ConnectorProviderId::parse(PROVIDER_ID)
        .expect("the static Iceberg provider identity is valid");
    let read_revision =
        ConnectorCodecRevision::try_new(crate::typed_read::codec::ICEBERG_READ_CODEC_REVISION)
            .expect("the static Iceberg read codec revision is non-zero");
    let write_revision = ConnectorCodecRevision::try_new(crate::wire::write::WRITE_CODEC_REVISION)
        .expect("the static Iceberg write codec revision is non-zero");
    let declaration = |category, revision, format_name| {
        ConnectorCodecDeclaration::try_new(
            provider_id.clone(),
            category,
            revision,
            format_name,
            FILE_DESCRIPTOR_SET,
        )
    };
    ProviderContractDefinition::read_write(
        provider_id.clone(),
        ProviderReadContractDefinition::new(ProviderReadCodecDefinitions::try_new(
            declaration(
                ConnectorCodecCategory::ReadTable,
                read_revision,
                "novarocks.iceberg.read-table.v1",
            )?,
            declaration(
                ConnectorCodecCategory::ReadView,
                read_revision,
                "novarocks.iceberg.read-view.v1",
            )?,
            declaration(
                ConnectorCodecCategory::ReadColumn,
                read_revision,
                "novarocks.iceberg.read-column.v1",
            )?,
            declaration(
                ConnectorCodecCategory::ReadSplit,
                read_revision,
                "novarocks.iceberg.read-split.v1",
            )?,
        )?),
        ProviderWriteContractDefinition::new(ProviderWriteCodecDefinitions::try_new(
            declaration(
                ConnectorCodecCategory::WriteHandle,
                write_revision,
                "novarocks.iceberg.write-handle.v1",
            )?,
            declaration(
                ConnectorCodecCategory::CommitFragment,
                write_revision,
                "novarocks.iceberg.commit-fragment.v1",
            )?,
        )?),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iceberg_contract_uses_one_descriptor_and_revision() {
        let contract = iceberg_contract_definition().expect("static Iceberg contract");
        assert_eq!(contract.provider_id().as_str(), PROVIDER_ID);
        assert!(contract.write().is_some());

        let declarations = contract.declarations();
        assert_eq!(declarations.len(), 6);
        for declaration in declarations {
            assert_eq!(declaration.provider_id(), contract.provider_id());
            assert_eq!(declaration.revision().get(), 1);
            assert_eq!(declaration.descriptor(), FILE_DESCRIPTOR_SET);
            assert_eq!(
                declaration.descriptor_sha256(),
                contract.declarations()[0].descriptor_sha256()
            );
        }
    }
}
