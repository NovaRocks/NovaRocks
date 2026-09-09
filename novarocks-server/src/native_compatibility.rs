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

// Design: ADR-0121 (docs/adr/ADR-0121-native-compatibility-islands-and-ingress-admission.md)
//! Server-owned static manifest for the Native compatibility contract.

use anyhow::Context;
use novarocks_spi::connector::provider::{ProviderContractDefinition, SealedProviderRegistry};
use novarocks_version::{
    NativeCarrierDeclaration, NativeCompatibilityMaterial,
    derive_repository_native_compatibility_material,
};

/// Builds the one closed carrier manifest for this server binary.
///
/// This is intentionally independent of config and runtime connector state.
pub fn native_carrier_declarations(
    provider_registry: &SealedProviderRegistry,
) -> anyhow::Result<Vec<NativeCarrierDeclaration>> {
    provider_registry
        .definitions()
        .iter()
        .map(native_carrier_from_contract)
        .collect()
}

fn native_carrier_from_contract(
    contract: &ProviderContractDefinition,
) -> anyhow::Result<NativeCarrierDeclaration> {
    let declarations = contract.declarations();
    let first = declarations
        .first()
        .copied()
        .context("provider contract has no private codec declaration")?;
    for declaration in declarations.iter().copied() {
        anyhow::ensure!(
            declaration.provider_id() == contract.provider_id(),
            "provider contract contains a declaration for another provider"
        );
        anyhow::ensure!(
            declaration.revision() == first.revision(),
            "provider contract declarations disagree on private codec revision"
        );
        anyhow::ensure!(
            declaration.descriptor_sha256() == first.descriptor_sha256(),
            "provider contract declarations disagree on private descriptor digest"
        );
    }
    NativeCarrierDeclaration::try_new_with_private_descriptor(
        contract.provider_id().as_str(),
        u64::from(first.revision().get()),
        first.descriptor(),
    )
    .with_context(|| "validate server native carrier declaration")
}

/// Resolves the immutable compatibility material for this binary before role
/// application composition opens listeners or runtime services.
pub fn resolve_native_compatibility_material(
    provider_registry: &SealedProviderRegistry,
    function_catalog_digest: [u8; 32],
    execution_implementation_manifest_digest: [u8; 32],
) -> anyhow::Result<NativeCompatibilityMaterial> {
    let declarations = native_carrier_declarations(provider_registry)?;
    derive_repository_native_compatibility_material(
        declarations,
        function_catalog_digest,
        execution_implementation_manifest_digest,
    )
    .with_context(|| "derive native compatibility material")
}

#[cfg(test)]
mod tests {
    use novarocks_spi::connector::provider::{
        ConnectorCodecDeclaration, ProviderContractDefinition, ProviderReadCodecDefinitions,
        ProviderReadContractDefinition,
    };
    use novarocks_spi::connector::{
        ConnectorCodecCategory, ConnectorCodecRevision, ConnectorProviderId,
    };

    use super::{
        native_carrier_declarations, native_carrier_from_contract,
        resolve_native_compatibility_material,
    };

    fn server_manifest() -> crate::provider_manifest::ServerProviderManifest {
        crate::provider_manifest::ServerProviderManifest::seal().expect("server provider manifest")
    }

    #[test]
    fn static_manifest_contains_exact_private_provider_descriptors() {
        let manifest = server_manifest();
        let declarations =
            native_carrier_declarations(manifest.contracts()).expect("server carrier declarations");
        let declared = declarations
            .iter()
            .map(|declaration| declaration.provider_id())
            .collect::<Vec<_>>();
        assert_eq!(declared, vec!["iceberg", "paimon"]);
        assert_eq!(
            declarations
                .iter()
                .map(|declaration| (declaration.provider_id(), declaration.contract_revision()))
                .collect::<Vec<_>>(),
            vec![("iceberg", 1), ("paimon", 1)]
        );
        assert!(
            declarations
                .iter()
                .all(|declaration| { declaration.private_descriptor_digest() != [0; 32] })
        );
    }

    #[test]
    fn repository_material_is_nonempty_and_uses_the_server_manifest() {
        let manifest = server_manifest();
        let material =
            resolve_native_compatibility_material(manifest.contracts(), [0x31; 32], [0x41; 32])
                .expect("compatibility material");
        let implementation_only_change =
            resolve_native_compatibility_material(manifest.contracts(), [0x31; 32], [0x42; 32])
                .expect("implementation-only compatibility material");

        assert_eq!(
            material.carriers(),
            native_carrier_declarations(manifest.contracts()).unwrap()
        );
        assert_eq!(material.id().to_string().len(), 64);
        assert_ne!(material.id(), implementation_only_change.id());
    }

    #[test]
    fn native_carrier_rejects_provider_declarations_with_different_revisions() {
        let provider = ConnectorProviderId::parse("fixture").expect("provider");
        let declaration = |category, revision| {
            ConnectorCodecDeclaration::try_new(
                provider.clone(),
                category,
                ConnectorCodecRevision::try_new(revision).expect("revision"),
                format!("fixture.{category:?}.{revision}"),
                b"same descriptor".as_slice(),
            )
            .expect("declaration")
        };
        let contract = ProviderContractDefinition::read_only(
            provider.clone(),
            ProviderReadContractDefinition::new(
                ProviderReadCodecDefinitions::try_new(
                    declaration(ConnectorCodecCategory::ReadTable, 1),
                    declaration(ConnectorCodecCategory::ReadView, 2),
                    declaration(ConnectorCodecCategory::ReadColumn, 1),
                    declaration(ConnectorCodecCategory::ReadSplit, 1),
                )
                .expect("codec categories"),
            ),
        )
        .expect("provider contract");

        let error = native_carrier_from_contract(&contract)
            .expect_err("mixed revisions must not enter global compatibility material");
        assert!(
            error
                .to_string()
                .contains("disagree on private codec revision")
        );
    }

    #[test]
    fn native_carrier_rejects_provider_declarations_with_different_descriptors() {
        let provider = ConnectorProviderId::parse("fixture").expect("provider");
        let declaration = |category, descriptor: &'static [u8]| {
            ConnectorCodecDeclaration::try_new(
                provider.clone(),
                category,
                ConnectorCodecRevision::try_new(1).expect("revision"),
                format!("fixture.{category:?}"),
                descriptor,
            )
            .expect("declaration")
        };
        let contract = ProviderContractDefinition::read_only(
            provider.clone(),
            ProviderReadContractDefinition::new(
                ProviderReadCodecDefinitions::try_new(
                    declaration(ConnectorCodecCategory::ReadTable, b"descriptor-a"),
                    declaration(ConnectorCodecCategory::ReadView, b"descriptor-b"),
                    declaration(ConnectorCodecCategory::ReadColumn, b"descriptor-a"),
                    declaration(ConnectorCodecCategory::ReadSplit, b"descriptor-a"),
                )
                .expect("codec categories"),
            ),
        )
        .expect("provider contract");

        let error = native_carrier_from_contract(&contract)
            .expect_err("mixed descriptors must not enter global compatibility material");
        assert!(
            error
                .to_string()
                .contains("disagree on private descriptor digest")
        );
    }
}
