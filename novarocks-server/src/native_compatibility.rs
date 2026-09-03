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
use novarocks_spi::connector::ConnectorProviderBindingKind;
use novarocks_version::{
    NativeCarrierDeclaration, NativeCompatibilityMaterial,
    derive_repository_native_compatibility_material,
};

// Iceberg revision 2 adds `CatalogHandle` to the provider-private distributed
// rewrite attempt artifact. This cannot be inferred from the native IDL.
const NATIVE_CARRIER_MANIFEST: [(&str, u64); 2] = [("iceberg", 2), ("starrocks", 1)];

/// Builds the one closed carrier manifest for this server binary.
///
/// This is intentionally independent of config and runtime connector state.
pub fn native_carrier_declarations() -> anyhow::Result<Vec<NativeCarrierDeclaration>> {
    let declarations = NATIVE_CARRIER_MANIFEST
        .into_iter()
        .map(|(provider_id, contract_revision)| {
            NativeCarrierDeclaration::try_new(provider_id, contract_revision)
                .with_context(|| "validate server native carrier declaration")
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    validate_declared_provider_kinds(&declarations)?;
    Ok(declarations)
}

/// Resolves the immutable compatibility material for this binary before role
/// application composition opens listeners or runtime services.
pub fn resolve_native_compatibility_material(
    function_catalog_digest: [u8; 32],
) -> anyhow::Result<NativeCompatibilityMaterial> {
    let declarations = native_carrier_declarations()?;
    derive_repository_native_compatibility_material(declarations, function_catalog_digest)
        .with_context(|| "derive native compatibility material")
}

fn validate_declared_provider_kinds(
    declarations: &[NativeCarrierDeclaration],
) -> anyhow::Result<()> {
    let expected = ConnectorProviderBindingKind::ALL
        .map(|kind| kind.provider_id())
        .to_vec();
    let actual = declarations
        .iter()
        .map(NativeCarrierDeclaration::provider_id)
        .collect::<Vec<_>>();
    if actual != expected {
        anyhow::bail!(
            "server native carrier manifest does not match ConnectorProviderBindingKind::ALL: actual={actual:?}, expected={expected:?}"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{native_carrier_declarations, resolve_native_compatibility_material};
    use crate::composition::compose_backend_execution_role_binding_factories;
    use novarocks_spi::connector::{CatalogProviderKind, ConnectorProviderBindingKind};

    #[test]
    fn static_manifest_matches_the_closed_provider_enum_and_backend_installers() {
        let declarations = native_carrier_declarations().expect("server carrier declarations");
        let declared = declarations
            .iter()
            .map(|declaration| declaration.provider_id())
            .collect::<Vec<_>>();
        let expected = ConnectorProviderBindingKind::ALL
            .map(|kind| kind.provider_id())
            .to_vec();
        assert_eq!(declared, expected);
        assert_eq!(
            declarations
                .iter()
                .map(|declaration| (declaration.provider_id(), declaration.contract_revision()))
                .collect::<Vec<_>>(),
            vec![("iceberg", 2), ("starrocks", 1)]
        );

        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let config = crate::app_config::NovaRocksConfig::default();
        let factories =
            compose_backend_execution_role_binding_factories(&config, runtime.handle().clone())
                .expect("backend factories");
        let factory_kinds = factories
            .iter()
            .map(|factory| factory.provider_kind())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            factory_kinds,
            CatalogProviderKind::ALL.into_iter().collect()
        );
    }

    #[test]
    fn repository_material_is_nonempty_and_uses_the_server_manifest() {
        let material = resolve_native_compatibility_material([0x31; 32])
            .expect("compatibility material");

        assert_eq!(material.carriers(), native_carrier_declarations().unwrap());
        assert_eq!(material.id().to_string().len(), 64);
    }
}
