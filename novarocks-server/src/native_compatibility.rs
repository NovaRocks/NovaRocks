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

// Design: ADR-0124 (docs/adr/ADR-0124-native-compatibility-islands-and-ingress-admission.md)
//! Server-owned static manifest for the Native compatibility contract.

use anyhow::Context;
use novarocks_physical_plan::PLAN_CONTRACT_REVISION;
use novarocks_spi::connector::provider::{ProviderContractDefinition, SealedProviderRegistry};
use novarocks_version::{
    NativeCarrierDeclaration, NativeCompatibilityMaterial, StaticPlanInterpreter,
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
/// application composition opens listeners or runtime services. The static
/// plan interpreter is the one this binary composed for every role.
pub fn resolve_native_compatibility_material(
    provider_registry: &SealedProviderRegistry,
    function_catalog_digest: [u8; 32],
    execution_implementation_manifest_digest: [u8; 32],
    static_plan_interpreter: StaticPlanInterpreter,
) -> anyhow::Result<NativeCompatibilityMaterial> {
    let declarations = native_carrier_declarations(provider_registry)?;
    derive_repository_native_compatibility_material(
        declarations,
        function_catalog_digest,
        execution_implementation_manifest_digest,
        PLAN_CONTRACT_REVISION,
        static_plan_interpreter,
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
    use novarocks_version::StaticPlanInterpreter;

    const PLAN_TREE: StaticPlanInterpreter = StaticPlanInterpreter::PlanTree;

    fn server_manifest() -> crate::provider_manifest::ServerProviderManifest {
        crate::provider_manifest::ServerProviderManifest::seal().expect("server provider manifest")
    }

    #[test]
    fn legacy_raw_text_temporal_peer_has_a_distinct_plan_contract_digest() {
        let manifest = server_manifest();
        let current = resolve_native_compatibility_material(
            manifest.contracts(),
            [0x31; 32],
            [0x41; 32],
            PLAN_TREE,
        )
        .unwrap();
        // Catalog declarations and implementation profiles are identical here.
        // Revision 1 admitted raw UTF8 bounds; the normalized contract must not.
        let legacy = novarocks_version::derive_repository_native_compatibility_material(
            native_carrier_declarations(manifest.contracts()).unwrap(),
            [0x31; 32],
            [0x41; 32],
            1,
            PLAN_TREE,
        )
        .unwrap();
        assert_ne!(
            current.plan_contract_revision(),
            legacy.plan_contract_revision()
        );
        assert_ne!(
            current.plan_contract_digest(),
            legacy.plan_contract_digest()
        );
        assert_ne!(current.id(), legacy.id());
        assert_eq!(current.descriptor_digest(), legacy.descriptor_digest());
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
            vec![("iceberg", 3), ("paimon", 1)]
        );
        assert!(
            declarations
                .iter()
                .all(|declaration| { declaration.private_descriptor_digest() != [0; 32] })
        );
    }

    #[test]
    fn iceberg_revision_alone_changes_the_native_compatibility_island() {
        let manifest = server_manifest();
        let declarations = native_carrier_declarations(manifest.contracts()).unwrap();
        let iceberg = manifest
            .contracts()
            .definitions()
            .iter()
            .find(|contract| contract.provider_id().as_str() == "iceberg")
            .expect("sealed Iceberg contract");
        let descriptor = iceberg.declarations()[0].descriptor();
        let changed = declarations
            .iter()
            .map(|declaration| {
                if declaration.provider_id() == "iceberg" {
                    novarocks_version::NativeCarrierDeclaration::try_new_with_private_descriptor(
                        "iceberg",
                        declaration.contract_revision().checked_add(1).unwrap(),
                        descriptor,
                    )
                    .unwrap()
                } else {
                    declaration.clone()
                }
            })
            .collect::<Vec<_>>();
        for (before, after) in declarations.iter().zip(&changed) {
            assert_eq!(before.provider_id(), after.provider_id());
            assert_eq!(
                before.private_descriptor_digest(),
                after.private_descriptor_digest()
            );
            if before.provider_id() != "iceberg" {
                assert_eq!(before, after);
            }
        }
        let derive = |carriers| {
            novarocks_version::derive_repository_native_compatibility_material(
                carriers,
                [0x31; 32],
                [0x41; 32],
                novarocks_physical_plan::PLAN_CONTRACT_REVISION,
                PLAN_TREE,
            )
            .unwrap()
        };
        let original = derive(declarations);
        let revised = derive(changed);
        assert_ne!(original.id(), revised.id());
        assert_eq!(original.descriptor_digest(), revised.descriptor_digest());
        assert_eq!(
            original.function_catalog_digest(),
            revised.function_catalog_digest()
        );
        assert_eq!(
            original.execution_implementation_manifest_digest(),
            revised.execution_implementation_manifest_digest()
        );
        assert_eq!(
            original.plan_contract_digest(),
            revised.plan_contract_digest()
        );
    }

    #[test]
    fn repository_material_is_nonempty_and_uses_the_server_manifest() {
        let manifest = server_manifest();
        let material = resolve_native_compatibility_material(
            manifest.contracts(),
            [0x31; 32],
            [0x41; 32],
            PLAN_TREE,
        )
        .expect("compatibility material");
        let implementation_only_change = resolve_native_compatibility_material(
            manifest.contracts(),
            [0x31; 32],
            [0x42; 32],
            PLAN_TREE,
        )
        .expect("implementation-only compatibility material");

        assert_eq!(
            material.carriers(),
            native_carrier_declarations(manifest.contracts()).unwrap()
        );
        assert_eq!(material.id().to_string().len(), 64);
        assert_eq!(
            material.plan_contract_revision(),
            novarocks_physical_plan::PLAN_CONTRACT_REVISION
        );
        assert_ne!(material.id(), implementation_only_change.id());

        let next_plan_revision = novarocks_physical_plan::PLAN_CONTRACT_REVISION
            .checked_add(1)
            .expect("test requires a next plan contract revision");
        let plan_only_change = novarocks_version::derive_repository_native_compatibility_material(
            native_carrier_declarations(manifest.contracts()).unwrap(),
            [0x31; 32],
            [0x41; 32],
            next_plan_revision,
            PLAN_TREE,
        )
        .expect("plan-only compatibility material");
        assert_ne!(material.id(), plan_only_change.id());
        assert_ne!(
            material.plan_contract_digest(),
            plan_only_change.plan_contract_digest()
        );
        assert_eq!(
            material.descriptor_digest(),
            plan_only_change.descriptor_digest()
        );
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

    #[test]
    fn peer_without_decimal_largeint_pair_rule_has_a_distinct_plan_digest() {
        let manifest = server_manifest();
        let current = resolve_native_compatibility_material(
            manifest.contracts(),
            [0x31; 32],
            [0x41; 32],
            PLAN_TREE,
        )
        .unwrap();
        // Revision 2 has normalized temporal casts but lacks this exact pair.
        let legacy = novarocks_version::derive_repository_native_compatibility_material(
            native_carrier_declarations(manifest.contracts()).unwrap(),
            [0x31; 32],
            [0x41; 32],
            2,
            PLAN_TREE,
        )
        .unwrap();
        assert_ne!(
            current.plan_contract_revision(),
            legacy.plan_contract_revision()
        );
        assert_ne!(
            current.plan_contract_digest(),
            legacy.plan_contract_digest()
        );
        assert_ne!(current.id(), legacy.id());
        assert_eq!(current.descriptor_digest(), legacy.descriptor_digest());
    }
    #[test]
    fn prior_intrinsic_binding_peer_has_a_distinct_plan_contract_digest() {
        let manifest = server_manifest();
        let current = resolve_native_compatibility_material(
            manifest.contracts(),
            [0x31; 32],
            [0x41; 32],
            PLAN_TREE,
        )
        .unwrap();
        // Revision 2 predates intrinsic facts; revision 3 is reserved for the
        // independent Add/Sub semantic root. Both peers lack this closed fact.
        // Equal catalog and implementation digests isolate the plan revision.
        for revision in [2, 3] {
            let prior = novarocks_version::derive_repository_native_compatibility_material(
                native_carrier_declarations(manifest.contracts()).unwrap(),
                [0x31; 32],
                [0x41; 32],
                revision,
                PLAN_TREE,
            )
            .unwrap();
            assert!(current.plan_contract_revision() > revision);
            assert_ne!(current.plan_contract_digest(), prior.plan_contract_digest());
            assert_ne!(current.id(), prior.id());
            assert_eq!(current.descriptor_digest(), prior.descriptor_digest());
            assert_eq!(
                current.function_catalog_digest(),
                prior.function_catalog_digest()
            );
            assert_eq!(
                current.execution_implementation_manifest_digest(),
                prior.execution_implementation_manifest_digest()
            );
        }
    }

    #[test]
    fn peers_without_expression_policy_or_bounded_root_have_distinct_plan_digests() {
        let manifest = server_manifest();
        let current = resolve_native_compatibility_material(
            manifest.contracts(),
            [0x31; 32],
            [0x41; 32],
            PLAN_TREE,
        )
        .unwrap();
        for prior_revision in [4, 5] {
            let prior = novarocks_version::derive_repository_native_compatibility_material(
                native_carrier_declarations(manifest.contracts()).unwrap(),
                [0x31; 32],
                [0x41; 32],
                prior_revision,
                PLAN_TREE,
            )
            .unwrap();
            assert_eq!(
                current.plan_contract_revision(),
                novarocks_physical_plan::PLAN_CONTRACT_REVISION
            );
            assert_eq!(prior.plan_contract_revision(), prior_revision);
            assert_ne!(current.plan_contract_digest(), prior.plan_contract_digest());
            assert_ne!(current.id(), prior.id());
            assert_eq!(current.descriptor_digest(), prior.descriptor_digest());
            assert_eq!(
                current.function_catalog_digest(),
                prior.function_catalog_digest()
            );
            assert_eq!(
                current.execution_implementation_manifest_digest(),
                prior.execution_implementation_manifest_digest()
            );
        }
    }

    /// A candidate binary and a production binary built from one tree share
    /// the descriptor, catalogue, implementation, plan and carrier
    /// components; the interpreter each composed alone puts them on different
    /// islands, while the production identity stays a pure function of its
    /// inputs.
    #[test]
    fn candidate_and_production_binaries_from_one_tree_are_different_islands() {
        let manifest = server_manifest();
        let production = resolve_native_compatibility_material(
            manifest.contracts(),
            [0x31; 32],
            [0x41; 32],
            PLAN_TREE,
        )
        .unwrap();
        let production_again = resolve_native_compatibility_material(
            manifest.contracts(),
            [0x31; 32],
            [0x41; 32],
            PLAN_TREE,
        )
        .unwrap();
        let candidate = resolve_native_compatibility_material(
            manifest.contracts(),
            [0x31; 32],
            [0x41; 32],
            StaticPlanInterpreter::CompiledPackage {
                pure_function_catalog_digest: [0x51; 32],
            },
        )
        .unwrap();

        assert_eq!(production.id(), production_again.id());
        assert_ne!(production.id(), candidate.id());
        assert_ne!(
            production.static_plan_interpreter_digest(),
            candidate.static_plan_interpreter_digest()
        );
        assert_eq!(
            production.descriptor_digest(),
            candidate.descriptor_digest()
        );
        assert_eq!(
            production.function_catalog_digest(),
            candidate.function_catalog_digest()
        );
        assert_eq!(
            production.execution_implementation_manifest_digest(),
            candidate.execution_implementation_manifest_digest()
        );
        assert_eq!(
            production.plan_contract_digest(),
            candidate.plan_contract_digest()
        );
        assert_eq!(production.carriers(), candidate.carriers());
        assert_eq!(production.epoch(), candidate.epoch());
    }
    #[test]
    fn prior_peer_without_to_base64_source_receipt_has_a_distinct_plan_digest() {
        let manifest = server_manifest();
        let current = resolve_native_compatibility_material(
            manifest.contracts(),
            [0x31; 32],
            [0x41; 32],
            PLAN_TREE,
        )
        .unwrap();
        let prior = novarocks_version::derive_repository_native_compatibility_material(
            native_carrier_declarations(manifest.contracts()).unwrap(),
            [0x31; 32],
            [0x41; 32],
            11,
            PLAN_TREE,
        )
        .unwrap();
        assert!(current.plan_contract_revision() > 11);
        assert_eq!(current.descriptor_digest(), prior.descriptor_digest());
        assert_eq!(
            current.function_catalog_digest(),
            prior.function_catalog_digest()
        );
        assert_eq!(
            current.execution_implementation_manifest_digest(),
            prior.execution_implementation_manifest_digest()
        );
        assert_ne!(current.plan_contract_digest(), prior.plan_contract_digest());
        assert_ne!(current.id(), prior.id());
    }
}
