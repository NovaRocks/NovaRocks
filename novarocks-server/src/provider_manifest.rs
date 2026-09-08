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

//! Server-owned provider manifest and role-local factory projection.

use std::num::NonZeroUsize;
use std::sync::Arc;

use anyhow::Context;
use novarocks_connector_iceberg::resources::IcebergMetadataResources;
use novarocks_connector_iceberg::{
    IcebergControlRoleBindingFactory, IcebergExecutionRoleBindingFactory,
};
use novarocks_connector_paimon::role_binding::{
    PaimonControlRoleBindingFactory, PaimonExecutionRoleBindingFactory,
};
use novarocks_spi::connector::provider::{ProviderContractDefinition, SealedProviderRegistry};
use novarocks_spi::connector::{
    ConnectorCodecError, ConnectorControlRoleBindingFactory, ConnectorExecutionRoleBindingFactory,
    ConnectorProviderId,
};
use novarocks_types::ClusterRole;

use crate::app_config::NovaRocksConfig;

type ContractBuilder = fn() -> Result<ProviderContractDefinition, ConnectorCodecError>;
type ControlFactoryBuilder = fn(
    &NovaRocksConfig,
    tokio::runtime::Handle,
) -> anyhow::Result<Arc<dyn ConnectorControlRoleBindingFactory>>;
type ExecutionFactoryBuilder = fn(
    &NovaRocksConfig,
    tokio::runtime::Handle,
) -> anyhow::Result<Arc<dyn ConnectorExecutionRoleBindingFactory>>;

#[derive(Clone, Copy)]
struct ProviderBuilderDefinition {
    contract: ContractBuilder,
    control: Option<ControlFactoryBuilder>,
    execution: Option<ExecutionFactoryBuilder>,
}

struct SealedProviderBuilder {
    provider_id: ConnectorProviderId,
    control: ControlFactoryBuilder,
    execution: ExecutionFactoryBuilder,
}

const PROVIDER_BUILDERS: &[ProviderBuilderDefinition] = &[
    ProviderBuilderDefinition {
        contract: novarocks_connector_iceberg::iceberg_contract_definition,
        control: Some(build_iceberg_control_factory),
        execution: Some(build_iceberg_execution_factory),
    },
    ProviderBuilderDefinition {
        contract: novarocks_connector_paimon::definition::paimon_contract_definition,
        control: Some(build_paimon_control_factory),
        execution: Some(build_paimon_execution_factory),
    },
];

/// One immutable provider contract registry paired with its role-local builders.
///
/// Sealing invokes only contract builders. Role builders are called later by
/// the selected role projection, so FE never constructs BE credentials or
/// execution resources and BE never constructs FE credentials or control
/// resources.
pub struct ServerProviderManifest {
    contracts: SealedProviderRegistry,
    builders: Arc<[SealedProviderBuilder]>,
}

impl ServerProviderManifest {
    pub fn seal() -> anyhow::Result<Self> {
        Self::seal_definitions(PROVIDER_BUILDERS)
    }

    fn seal_definitions(definitions: &[ProviderBuilderDefinition]) -> anyhow::Result<Self> {
        let mut contracts = Vec::with_capacity(definitions.len());
        let mut builders = Vec::with_capacity(definitions.len());
        for definition in definitions {
            let contract = (definition.contract)().context("build provider contract")?;
            let provider_id = contract.provider_id().clone();
            let control = definition.control.ok_or_else(|| {
                anyhow::anyhow!(
                    "provider `{}` has no FE control factory builder",
                    provider_id.as_str()
                )
            })?;
            let execution = definition.execution.ok_or_else(|| {
                anyhow::anyhow!(
                    "provider `{}` has no BE execution factory builder",
                    provider_id.as_str()
                )
            })?;
            contracts.push(contract);
            builders.push(SealedProviderBuilder {
                provider_id,
                control,
                execution,
            });
        }

        let contracts = SealedProviderRegistry::seal(contracts)
            .map_err(|error| anyhow::anyhow!("seal server provider registry: {error}"))?;
        builders.sort_by(|left, right| left.provider_id.cmp(&right.provider_id));
        anyhow::ensure!(
            contracts.definitions().len() == builders.len(),
            "sealed provider contracts and builders disagree on provider count"
        );
        for (contract, builder) in contracts.definitions().iter().zip(&builders) {
            anyhow::ensure!(
                contract.provider_id() == &builder.provider_id,
                "sealed provider contract and builder identities disagree"
            );
        }
        Ok(Self {
            contracts,
            builders: Arc::from(builders),
        })
    }

    pub const fn contracts(&self) -> &SealedProviderRegistry {
        &self.contracts
    }

    pub fn compose_control_factories(
        &self,
        config: &NovaRocksConfig,
        runtime: tokio::runtime::Handle,
    ) -> anyhow::Result<Vec<Arc<dyn ConnectorControlRoleBindingFactory>>> {
        self.builders
            .iter()
            .map(|builder| {
                let factory = (builder.control)(config, runtime.clone()).with_context(|| {
                    format!("compose `{}` FE factory", builder.provider_id.as_str())
                })?;
                validate_factory_identity(
                    "FE control",
                    &builder.provider_id,
                    &factory.provider_id(),
                )?;
                Ok(factory)
            })
            .collect()
    }

    pub fn compose_execution_factories(
        &self,
        config: &NovaRocksConfig,
        runtime: tokio::runtime::Handle,
    ) -> anyhow::Result<Vec<Arc<dyn ConnectorExecutionRoleBindingFactory>>> {
        self.builders
            .iter()
            .map(|builder| {
                let factory = (builder.execution)(config, runtime.clone()).with_context(|| {
                    format!("compose `{}` BE factory", builder.provider_id.as_str())
                })?;
                validate_factory_identity(
                    "BE execution",
                    &builder.provider_id,
                    &factory.provider_id(),
                )?;
                Ok(factory)
            })
            .collect()
    }
}

fn validate_factory_identity(
    role: &str,
    contract_provider_id: &ConnectorProviderId,
    factory_provider_id: &ConnectorProviderId,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        contract_provider_id == factory_provider_id,
        "provider contract `{}` and {role} factory `{}` have different identities",
        contract_provider_id.as_str(),
        factory_provider_id.as_str()
    );
    Ok(())
}

fn build_iceberg_control_factory(
    config: &NovaRocksConfig,
    runtime: tokio::runtime::Handle,
) -> anyhow::Result<Arc<dyn ConnectorControlRoleBindingFactory>> {
    let binding = crate::composition::compose_iceberg_access_template(
        config,
        runtime.clone(),
        ClusterRole::Fe,
    )?;
    let max_inflight = NonZeroUsize::new(config.runtime.catalog_materialization_max_inflight)
        .ok_or_else(|| anyhow::anyhow!("catalog materialization max inflight must be nonzero"))?;
    Ok(Arc::new(IcebergControlRoleBindingFactory::new(
        IcebergMetadataResources::new(binding, runtime),
        max_inflight,
    )))
}

fn build_iceberg_execution_factory(
    config: &NovaRocksConfig,
    runtime: tokio::runtime::Handle,
) -> anyhow::Result<Arc<dyn ConnectorExecutionRoleBindingFactory>> {
    let resources = crate::composition::compose_iceberg_execution_resources(config, runtime)?;
    Ok(Arc::new(IcebergExecutionRoleBindingFactory::new(
        resources,
        novarocks_connector_iceberg::typed_read::page_source_provider::IcebergPageSourceProviderOptions::with_default_budget(),
    )))
}

fn build_paimon_control_factory(
    config: &NovaRocksConfig,
    runtime: tokio::runtime::Handle,
) -> anyhow::Result<Arc<dyn ConnectorControlRoleBindingFactory>> {
    let access = crate::composition::compose_paimon_access_factory(
        config,
        runtime.clone(),
        ClusterRole::Fe,
    )?;
    let max_inflight = NonZeroUsize::new(config.runtime.catalog_materialization_max_inflight)
        .ok_or_else(|| anyhow::anyhow!("catalog materialization max inflight must be nonzero"))?;
    Ok(Arc::new(PaimonControlRoleBindingFactory::new(
        access,
        runtime,
        max_inflight,
    )))
}

fn build_paimon_execution_factory(
    config: &NovaRocksConfig,
    runtime: tokio::runtime::Handle,
) -> anyhow::Result<Arc<dyn ConnectorExecutionRoleBindingFactory>> {
    let access = crate::composition::compose_paimon_access_factory(
        config,
        runtime.clone(),
        ClusterRole::Be,
    )?;
    let max_inflight = NonZeroUsize::new(config.runtime.data_runtime_max_blocking_threads)
        .ok_or_else(|| anyhow::anyhow!("data runtime max blocking threads must be nonzero"))?;
    Ok(Arc::new(PaimonExecutionRoleBindingFactory::new(
        access,
        runtime,
        max_inflight,
    )))
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use novarocks_spi::connector::provider::{
        ConnectorCodecDeclaration, ProviderReadCodecDefinitions, ProviderReadContractDefinition,
    };
    use novarocks_spi::connector::{
        CatalogProperties, ConnectorCodecCategory, ConnectorCodecRevision,
        ConnectorControlRoleBinding, ConnectorExecutionRoleBinding, ConnectorMaterializationError,
        MaterializationContext, NormalizedCatalogProperties,
    };

    use super::*;

    static CONTROL_BUILDS: AtomicUsize = AtomicUsize::new(0);
    static EXECUTION_BUILDS: AtomicUsize = AtomicUsize::new(0);

    fn fixture_contract() -> Result<ProviderContractDefinition, ConnectorCodecError> {
        contract("fixture")
    }

    fn duplicate_fixture_contract() -> Result<ProviderContractDefinition, ConnectorCodecError> {
        contract("fixture")
    }

    fn contract(id: &str) -> Result<ProviderContractDefinition, ConnectorCodecError> {
        let provider_id = ConnectorProviderId::parse(id).expect("fixture provider ID");
        let declaration = |category| {
            ConnectorCodecDeclaration::try_new(
                provider_id.clone(),
                category,
                ConnectorCodecRevision::try_new(1).expect("revision"),
                format!("fixture.{category:?}"),
                b"fixture descriptor".as_slice(),
            )
        };
        ProviderContractDefinition::read_only(
            provider_id.clone(),
            ProviderReadContractDefinition::new(ProviderReadCodecDefinitions::try_new(
                declaration(ConnectorCodecCategory::ReadTable)?,
                declaration(ConnectorCodecCategory::ReadView)?,
                declaration(ConnectorCodecCategory::ReadColumn)?,
                declaration(ConnectorCodecCategory::ReadSplit)?,
            )?),
        )
    }

    struct FixtureFactory {
        provider_id: ConnectorProviderId,
    }

    impl ConnectorControlRoleBindingFactory for FixtureFactory {
        fn provider_id(&self) -> ConnectorProviderId {
            self.provider_id.clone()
        }

        fn normalize_and_validate(
            &self,
            _properties: CatalogProperties,
        ) -> Result<NormalizedCatalogProperties, ConnectorMaterializationError> {
            unreachable!("manifest projection never invokes provider behavior")
        }

        fn materialize(
            &self,
            _properties: NormalizedCatalogProperties,
            _context: MaterializationContext,
        ) -> Pin<
            Box<
                dyn Future<
                        Output = Result<ConnectorControlRoleBinding, ConnectorMaterializationError>,
                    > + Send
                    + 'static,
            >,
        > {
            unreachable!("manifest projection never invokes provider behavior")
        }
    }

    impl ConnectorExecutionRoleBindingFactory for FixtureFactory {
        fn provider_id(&self) -> ConnectorProviderId {
            self.provider_id.clone()
        }

        fn bind(
            &self,
            _properties: &NormalizedCatalogProperties,
        ) -> Result<ConnectorExecutionRoleBinding, ConnectorMaterializationError> {
            unreachable!("manifest projection never invokes provider behavior")
        }
    }

    fn drifted_control_factory(
        _config: &NovaRocksConfig,
        _runtime: tokio::runtime::Handle,
    ) -> anyhow::Result<Arc<dyn ConnectorControlRoleBindingFactory>> {
        Ok(Arc::new(FixtureFactory {
            provider_id: ConnectorProviderId::parse("other").expect("fixture provider ID"),
        }))
    }

    fn fixture_control_factory(
        _config: &NovaRocksConfig,
        _runtime: tokio::runtime::Handle,
    ) -> anyhow::Result<Arc<dyn ConnectorControlRoleBindingFactory>> {
        Ok(Arc::new(FixtureFactory {
            provider_id: ConnectorProviderId::parse("fixture").expect("fixture provider ID"),
        }))
    }

    fn fixture_execution_factory(
        _config: &NovaRocksConfig,
        _runtime: tokio::runtime::Handle,
    ) -> anyhow::Result<Arc<dyn ConnectorExecutionRoleBindingFactory>> {
        Ok(Arc::new(FixtureFactory {
            provider_id: ConnectorProviderId::parse("fixture").expect("fixture provider ID"),
        }))
    }

    fn counted_control_factory(
        config: &NovaRocksConfig,
        runtime: tokio::runtime::Handle,
    ) -> anyhow::Result<Arc<dyn ConnectorControlRoleBindingFactory>> {
        CONTROL_BUILDS.fetch_add(1, Ordering::AcqRel);
        fixture_control_factory(config, runtime)
    }

    fn counted_execution_factory(
        config: &NovaRocksConfig,
        runtime: tokio::runtime::Handle,
    ) -> anyhow::Result<Arc<dyn ConnectorExecutionRoleBindingFactory>> {
        EXECUTION_BUILDS.fetch_add(1, Ordering::AcqRel);
        fixture_execution_factory(config, runtime)
    }

    #[test]
    fn sealing_rejects_a_missing_role_factory() {
        let error = ServerProviderManifest::seal_definitions(&[ProviderBuilderDefinition {
            contract: fixture_contract,
            control: Some(fixture_control_factory),
            execution: None,
        }])
        .err()
        .expect("missing BE factory must fail")
        .to_string();
        assert!(error.contains("no BE execution factory builder"), "{error}");
    }

    #[test]
    fn sealing_rejects_duplicate_provider_contracts() {
        let error = ServerProviderManifest::seal_definitions(&[
            ProviderBuilderDefinition {
                contract: fixture_contract,
                control: Some(fixture_control_factory),
                execution: Some(fixture_execution_factory),
            },
            ProviderBuilderDefinition {
                contract: duplicate_fixture_contract,
                control: Some(fixture_control_factory),
                execution: Some(fixture_execution_factory),
            },
        ])
        .err()
        .expect("duplicate contract must fail")
        .to_string();
        assert!(error.contains("registered more than once"), "{error}");
    }

    #[test]
    fn role_projection_rejects_factory_identity_drift() {
        let manifest = ServerProviderManifest::seal_definitions(&[ProviderBuilderDefinition {
            contract: fixture_contract,
            control: Some(drifted_control_factory),
            execution: Some(fixture_execution_factory),
        }])
        .expect("contract-only sealing must not invoke factories");
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let error = manifest
            .compose_control_factories(&NovaRocksConfig::default(), runtime.handle().clone())
            .err()
            .expect("identity drift must fail")
            .to_string();
        assert!(error.contains("different identities"), "{error}");
    }

    #[test]
    fn role_projection_constructs_only_the_selected_role_resources() {
        CONTROL_BUILDS.store(0, Ordering::Release);
        EXECUTION_BUILDS.store(0, Ordering::Release);
        let manifest = ServerProviderManifest::seal_definitions(&[ProviderBuilderDefinition {
            contract: fixture_contract,
            control: Some(counted_control_factory),
            execution: Some(counted_execution_factory),
        }])
        .expect("manifest");
        assert_eq!(CONTROL_BUILDS.load(Ordering::Acquire), 0);
        assert_eq!(EXECUTION_BUILDS.load(Ordering::Acquire), 0);

        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        manifest
            .compose_control_factories(&NovaRocksConfig::default(), runtime.handle().clone())
            .expect("FE projection");
        assert_eq!(CONTROL_BUILDS.load(Ordering::Acquire), 1);
        assert_eq!(EXECUTION_BUILDS.load(Ordering::Acquire), 0);
    }

    #[test]
    fn native_carriers_and_both_role_projections_share_one_manifest() {
        let manifest = ServerProviderManifest::seal().expect("server provider manifest");
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let config = NovaRocksConfig::default();
        let contract_ids = manifest
            .contracts()
            .definitions()
            .iter()
            .map(|contract| contract.provider_id().as_str().to_owned())
            .collect::<Vec<_>>();
        let carrier_ids =
            crate::native_compatibility::native_carrier_declarations(manifest.contracts())
                .expect("native carriers")
                .into_iter()
                .map(|carrier| carrier.provider_id().to_string())
                .collect::<Vec<_>>();
        let control_ids = manifest
            .compose_control_factories(&config, runtime.handle().clone())
            .expect("FE projection")
            .into_iter()
            .map(|factory| factory.provider_id().as_str().to_owned())
            .collect::<Vec<_>>();
        let execution_ids = manifest
            .compose_execution_factories(&config, runtime.handle().clone())
            .expect("BE projection")
            .into_iter()
            .map(|factory| factory.provider_id().as_str().to_owned())
            .collect::<Vec<_>>();

        assert_eq!(contract_ids, vec!["iceberg", "paimon"]);
        assert_eq!(carrier_ids, contract_ids);
        assert_eq!(control_ids, contract_ids);
        assert_eq!(execution_ids, contract_ids);
    }
}
