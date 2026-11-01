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

//! Production compiled-package composition and pure function support boundary.
//!
//! Complete installed pure-owner coverage decides which definitions are sealed.
//! The exact seal digest participates in Native compatibility; its deterministic
//! exclusions are reported by `--build-info` for capability restoration work.

use std::sync::Arc;

use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_execution::runtime::function_set::SealedExecutionFunctionSet;
use novarocks_functions::{
    ConstantPolicy, EngineFunctionCatalog, EngineFunctionCatalogBuilder, FunctionDefinition,
    InstalledPureKernel, PureEngineFunctionCatalog,
};
use novarocks_native_adapter::backend_application::CompiledStaticPlan;
use novarocks_plan_codec::physical_package_v2::{
    FROZEN_CONSTANT_POLICY, PackageDecodeLimits, PackageEncodeLimits,
};
use novarocks_plan_codec::resource_preflight_v2::FragmentDecodeResourceModel;
use novarocks_spi::connector::ConnectorCodecError;

use crate::provider_manifest::ServerProviderManifest;
use crate::static_plan::CompositionControl;

/// FE and BE share the codec owner's structural constant policy. A deployment
/// cannot silently broaden or narrow the static wire language.
pub(crate) fn frontend_constant_policy(
    configured: ConstantPolicy,
) -> anyhow::Result<ConstantPolicy> {
    anyhow::ensure!(
        configured == FROZEN_CONSTANT_POLICY,
        "runtime.frontend_constant_policy must equal the frozen compiled-package policy; configured={configured:?}; frozen={FROZEN_CONSTANT_POLICY:?}"
    );
    Ok(FROZEN_CONSTANT_POLICY)
}

/// Everything the production backend interpreter is composed from.
pub(crate) struct CompiledPackage {
    decode_limits: PackageDecodeLimits,
    decode_model: Arc<FragmentDecodeResourceModel>,
    constants: ConstantPolicy,
    functions: Arc<PureEngineFunctionCatalog>,
    providers: Arc<PureProviderProgramCatalog<ConnectorCodecError>>,
}

impl CompiledPackage {
    pub(crate) fn compose(
        function_set: &SealedExecutionFunctionSet,
        provider_manifest: &ServerProviderManifest,
    ) -> anyhow::Result<Self> {
        let seal = seal_pure_catalog(function_set.catalog())?;
        tracing::info!(
            installed_definitions = seal.installed_definitions,
            excluded_definitions = seal.excluded.len(),
            pure_function_catalog_digest = %hex::encode(seal.catalog.digest()),
            "compiled-package interpreter composes a sealed pure catalogue"
        );
        tracing::debug!(excluded = ?seal.excluded, "pure catalogue exclusions");
        Ok(Self {
            decode_limits: PackageDecodeLimits::FROZEN,
            decode_model: Arc::new(FragmentDecodeResourceModel::try_new(&CompositionControl)?),
            constants: FROZEN_CONSTANT_POLICY,
            functions: Arc::new(seal.catalog),
            providers: Arc::new(provider_manifest.compose_pure_program_catalog()?),
        })
    }

    pub(crate) fn pure_function_catalog_digest(&self) -> [u8; 32] {
        self.functions.digest()
    }

    /// The frontend freezes packages under the receiver's own admission.
    pub(crate) fn frontend_carrier(
        &self,
    ) -> novarocks_frontend_application::CompiledPackageCarrier {
        novarocks_frontend_application::CompiledPackageCarrier::new(
            self.decode_limits.admission,
            PackageEncodeLimits::FROZEN,
            Arc::clone(&self.decode_model),
            self.decode_limits.wire,
        )
    }

    pub(crate) fn backend_interpreter(&self) -> CompiledStaticPlan {
        CompiledStaticPlan::new(
            Arc::clone(&self.decode_model),
            self.decode_limits.clone(),
            Arc::clone(&self.functions),
            Arc::clone(&self.providers),
            self.constants,
        )
    }
}

/// The production pure catalogue and the definitions outside its support boundary.
pub(crate) struct PureCatalogSeal {
    pub(crate) catalog: PureEngineFunctionCatalog,
    pub(crate) installed_definitions: usize,
    /// A deterministic name, kind and reason for every excluded definition.
    pub(crate) excluded: Vec<PureCatalogExclusion>,
}

/// The support gap for one complete function definition.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub(crate) struct PureCatalogExclusion {
    pub(crate) name: String,
    pub(crate) kind: String,
    pub(crate) reason: String,
}

/// Seals exactly the process definitions whose every overload has an
/// installed pure owner with complete effects.
///
/// Each record is read from the owner the metadata catalogue itself carries;
/// none is authored here, and a definition with any uncovered overload is
/// left out whole rather than sealed partially. Because the records come from
/// the same owners `seal_pure` compares them with, that comparison proves only
/// those owners' consistency. It is not an independent installation manifest:
/// no execution-side producer of one exists yet. The resulting digest differs
/// from the process metadata catalogue's digest whenever anything is left out,
/// so it enters the process compatibility identity through the interpreter
/// component.
pub(crate) fn seal_pure_catalog(
    metadata: &EngineFunctionCatalog,
) -> anyhow::Result<PureCatalogSeal> {
    let mut builder = EngineFunctionCatalogBuilder::new();
    let mut installed = Vec::new();
    let mut installed_definitions = 0;
    let mut excluded = Vec::new();
    for definition in metadata.definitions() {
        match installed_records(metadata, definition) {
            Ok(records) => {
                builder.register(definition.clone()).map_err(|error| {
                    anyhow::anyhow!(
                        "register pure definition {}: {error}",
                        definition.canonical_name()
                    )
                })?;
                installed.extend(records);
                installed_definitions += 1;
            }
            Err(reason) => excluded.push(PureCatalogExclusion {
                name: definition.canonical_name().to_owned(),
                kind: format!("{:?}", definition.kind()),
                reason,
            }),
        }
    }
    excluded.sort_by(|left, right| (&left.name, &left.kind).cmp(&(&right.name, &right.kind)));
    anyhow::ensure!(
        installed_definitions > 0,
        "no process function definition has an installed pure owner"
    );
    let catalog = builder
        .seal_pure(installed)
        .map_err(|error| anyhow::anyhow!("seal the production pure function catalogue: {error}"))?;
    Ok(PureCatalogSeal {
        catalog,
        installed_definitions,
        excluded,
    })
}

fn installed_records(
    metadata: &EngineFunctionCatalog,
    definition: &FunctionDefinition,
) -> Result<Vec<InstalledPureKernel>, String> {
    let declaration = definition
        .binding_declaration()
        .ok_or_else(|| "no binding declaration".to_owned())?;
    declaration
        .validate_complete_effects()
        .map_err(|error| error.to_string())?;
    declaration
        .overloads()
        .iter()
        .map(|overload| {
            let installed = metadata
                .pure_overload_declaration_observed(
                    declaration.function_id(),
                    declaration.kind(),
                    &overload.identity,
                    &CompositionControl,
                )
                .map_err(|error| format!("{}: {error}", overload.identity.as_str()))?;
            Ok(InstalledPureKernel {
                function: declaration.function_id().clone(),
                kind: declaration.kind(),
                implementation: installed.implementation().clone(),
                aggregate_state_format: overload
                    .aggregate
                    .as_ref()
                    .map(|aggregate| aggregate.state_format.clone()),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::composition::compose_process_function_set;
    use novarocks_functions::FunctionKind;

    #[test]
    fn frontend_and_backend_constants_share_one_frozen_policy() {
        assert_eq!(
            frontend_constant_policy(FROZEN_CONSTANT_POLICY).unwrap(),
            PackageDecodeLimits::FROZEN.constant_policy
        );
        let mut wider = FROZEN_CONSTANT_POLICY;
        wider.max_rows += 1;
        assert!(frontend_constant_policy(wider).is_err());
        let mut narrower = FROZEN_CONSTANT_POLICY;
        narrower.max_dictionary_depth -= 1;
        assert!(frontend_constant_policy(narrower).is_err());
    }

    /// The production seal holds exactly the definitions with installed pure
    /// owners: RAND is one, so it is sealed with its owner's own records, and
    /// the seal is a strict subset whose digest is not the metadata digest.
    #[test]
    fn the_seal_is_exactly_the_installed_pure_owners() {
        let function_set = compose_process_function_set().expect("process function set");
        let metadata = function_set.catalog();
        let seal = seal_pure_catalog(metadata).expect("production seal");

        assert!(seal.installed_definitions > 0);
        assert_eq!(
            seal.installed_definitions + seal.excluded.len(),
            metadata.definitions().len(),
            "every process definition is either sealed or reported excluded"
        );
        let rand = seal
            .catalog
            .metadata()
            .definition("rand", FunctionKind::Scalar)
            .expect("RAND has an installed pure owner")
            .binding_declaration()
            .expect("RAND binding");
        for overload in rand.overloads() {
            seal.catalog
                .metadata()
                .pure_overload_declaration_observed(
                    rand.function_id(),
                    rand.kind(),
                    &overload.identity,
                    &CompositionControl,
                )
                .expect("every RAND overload is sealed with its installed implementation");
        }
        for definition in seal.catalog.metadata().definitions() {
            assert!(
                installed_records(metadata, definition).is_ok(),
                "{} was sealed without complete installed coverage",
                definition.canonical_name()
            );
        }
        if !seal.excluded.is_empty() {
            assert_ne!(seal.catalog.digest(), metadata.digest());
        }
        // Both the compatibility digest and the reported support gaps are deterministic.
        let again = seal_pure_catalog(metadata).expect("second seal");
        assert_eq!(again.catalog.digest(), seal.catalog.digest());
        assert_eq!(again.excluded, seal.excluded);
        assert!(
            seal.excluded
                .windows(2)
                .all(|pair| { (&pair[0].name, &pair[0].kind) < (&pair[1].name, &pair[1].kind) })
        );
        assert!(seal.excluded.iter().all(|entry| !entry.reason.is_empty()));
    }
}
