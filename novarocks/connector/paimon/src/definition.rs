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

//! Static Paimon provider contract.

use novarocks_spi::connector::provider::{
    ConnectorCodecDeclaration, ProviderContractDefinition, ProviderReadCodecDefinitions,
    ProviderReadContractDefinition,
};
use novarocks_spi::connector::{
    ConnectorCodecCategory, ConnectorCodecError, ConnectorCodecRevision, ConnectorProviderId,
};

use crate::{PROVIDER_ID, wire::FILE_DESCRIPTOR_SET};

pub const PAIMON_READ_CODEC_REVISION: u32 = 1;

pub fn paimon_contract_definition() -> Result<ProviderContractDefinition, ConnectorCodecError> {
    let provider_id = ConnectorProviderId::parse(PROVIDER_ID)
        .expect("the static Paimon provider identity is valid");
    let revision = ConnectorCodecRevision::try_new(PAIMON_READ_CODEC_REVISION)
        .expect("the static Paimon read codec revision is non-zero");
    let declaration = |category, format_name| {
        ConnectorCodecDeclaration::try_new(
            provider_id.clone(),
            category,
            revision,
            format_name,
            FILE_DESCRIPTOR_SET,
        )
    };
    ProviderContractDefinition::read_only(
        provider_id.clone(),
        ProviderReadContractDefinition::new(ProviderReadCodecDefinitions::try_new(
            declaration(
                ConnectorCodecCategory::ReadTable,
                "novarocks.paimon.read-table.v1",
            )?,
            declaration(
                ConnectorCodecCategory::ReadView,
                "novarocks.paimon.read-view.v1",
            )?,
            declaration(
                ConnectorCodecCategory::ReadColumn,
                "novarocks.paimon.read-column.v1",
            )?,
            declaration(
                ConnectorCodecCategory::ReadSplit,
                "novarocks.paimon.read-split.v1",
            )?,
        )?),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paimon_contract_is_one_read_only_revision_and_descriptor() {
        let contract = paimon_contract_definition().expect("static Paimon contract");
        assert_eq!(contract.provider_id().as_str(), PROVIDER_ID);
        assert!(contract.write().is_none());

        let declarations = contract.read().codecs().declarations();
        assert_eq!(declarations.len(), 4);
        for declaration in declarations {
            assert_eq!(declaration.provider_id(), contract.provider_id());
            assert_eq!(declaration.revision().get(), PAIMON_READ_CODEC_REVISION);
            assert_eq!(declaration.descriptor(), FILE_DESCRIPTOR_SET);
            assert_eq!(
                declaration.descriptor_sha256(),
                contract.read().codecs().declarations()[0].descriptor_sha256()
            );
        }
    }
}
