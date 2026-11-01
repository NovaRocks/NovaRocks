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
use novarocks_connector_contract::{
    PureProviderManifestEntry, PureProviderProgramCatalog, PureProviderProgramDefinition,
};
use novarocks_connector_iceberg::commit::write_stack::IcebergWriteRecipeCompiler;
use novarocks_connector_iceberg::resources::IcebergMetadataResources;
use novarocks_connector_iceberg::typed_read::codec::IcebergReadRecipeCompiler;
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

use crate::app_config::NovaRocksConfig;
use crate::scan_io::ScanIoServices;
use crate::static_plan::CompositionControl;

type ContractBuilder = fn() -> Result<ProviderContractDefinition, ConnectorCodecError>;
/// The product inputs shared by every registered role-local builder.
/// Observation slots are inert until the role's exact session is authorized.
struct ProviderFactoryInput<'a> {
    config: &'a NovaRocksConfig,
    runtime: tokio::runtime::Handle,
    filesystem: &'a novarocks_fs::FsAccessResources,
    #[cfg(novarocks_dev_controls)]
    test_slots: &'a crate::provider_test_slots::ProviderTestSlots,
}

type ControlFactoryBuilder =
    fn(&ProviderFactoryInput<'_>) -> anyhow::Result<Arc<dyn ConnectorControlRoleBindingFactory>>;
type ExecutionFactoryBuilder = fn(
    &ProviderFactoryInput<'_>,
    &ScanIoServices,
) -> anyhow::Result<Arc<dyn ConnectorExecutionRoleBindingFactory>>;
/// A provider's installed pure program ports, addressed by its sealed
/// contract identity. They compile frozen facts only and bind no instance.
type PureProgramBuilder =
    fn(ConnectorProviderId) -> PureProviderProgramDefinition<ConnectorCodecError>;

type MetadataIoGeometryBuilder = fn() -> Result<
    novarocks_spi::connector::ConnectorMetadataMaintenanceIoGeometry,
    novarocks_spi::connector::ConnectorError,
>;

#[derive(Clone, Copy)]
struct ProviderBuilderDefinition {
    contract: ContractBuilder,
    control: Option<ControlFactoryBuilder>,
    execution: Option<ExecutionFactoryBuilder>,
    // Design: ADR-0200 (docs/adr/ADR-0200-upstream-iceberg-backports-and-provider-suspension.md)
    pure: Option<PureProgramBuilder>,
    metadata_io_geometry: Option<MetadataIoGeometryBuilder>,
    write_io_geometry: Option<MetadataIoGeometryBuilder>,
    read_io_geometry: Option<MetadataIoGeometryBuilder>,
    staged_create_source_bytes:
        Option<fn() -> Result<u64, novarocks_spi::connector::ConnectorError>>,
}

struct SealedProviderBuilder {
    provider_id: ConnectorProviderId,
    control: ControlFactoryBuilder,
    execution: ExecutionFactoryBuilder,
    // Design: ADR-0200 (docs/adr/ADR-0200-upstream-iceberg-backports-and-provider-suspension.md)
    pure: Option<PureProgramBuilder>,
    metadata_io_geometry: Option<MetadataIoGeometryBuilder>,
    write_io_geometry: Option<MetadataIoGeometryBuilder>,
    read_io_geometry: Option<MetadataIoGeometryBuilder>,
    staged_create_source_bytes:
        Option<fn() -> Result<u64, novarocks_spi::connector::ConnectorError>>,
}

// Design: ADR-0139 (docs/adr/ADR-0139-sealed-active-provider-manifest.md)
const PROVIDER_BUILDERS: &[ProviderBuilderDefinition] = &[
    ProviderBuilderDefinition {
        contract: novarocks_connector_iceberg::iceberg_contract_definition,
        control: Some(build_iceberg_control_factory),
        execution: Some(build_iceberg_execution_factory),
        pure: Some(iceberg_pure_program),
        metadata_io_geometry: Some(novarocks_connector_iceberg::metadata_maintenance_io_geometry),
        write_io_geometry: Some(novarocks_connector_iceberg::write_control_io_geometry),
        read_io_geometry: Some(novarocks_connector_iceberg::read_io_geometry),
        staged_create_source_bytes: Some(novarocks_connector_iceberg::catalog_control::staged_create::staged_create_async_source_bytes),
    },
    ProviderBuilderDefinition {
        contract: novarocks_connector_paimon::definition::paimon_contract_definition,
        control: Some(build_paimon_control_factory),
        execution: Some(build_paimon_execution_factory),
        pure: None,
        metadata_io_geometry: None,
        write_io_geometry: None,
        read_io_geometry: None,
        staged_create_source_bytes: None,
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
                pure: definition.pure,
                metadata_io_geometry: definition.metadata_io_geometry,
                write_io_geometry: definition.write_io_geometry,
                read_io_geometry: definition.read_io_geometry,
                staged_create_source_bytes: definition.staged_create_source_bytes,
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

    /// Pure fixed backing of the original active metadata action factory.
    /// This does not bind a catalog generation, route, or runtime capability.
    pub fn metadata_maintenance_io_geometry(
        &self,
    ) -> anyhow::Result<novarocks_spi::connector::ConnectorMetadataMaintenanceIoGeometry> {
        let mut authors = self.builders.iter().filter_map(|builder| {
            builder
                .metadata_io_geometry
                .map(|quote| (&builder.provider_id, quote))
        });
        let (provider, quote) = authors.next().ok_or_else(|| {
            anyhow::anyhow!("server manifest has no metadata I/O geometry author")
        })?;
        anyhow::ensure!(
            authors.next().is_none(),
            "multiple metadata I/O factories require explicit composition"
        );
        quote().with_context(|| format!("quote `{}` metadata I/O factory", provider.as_str()))
    }

    /// Fixed maxima of the actual enabled statement factories. Each original
    /// root owns one statement action; no Provider is inferred from a catalog.
    pub fn statement_io_geometry(
        &self,
    ) -> anyhow::Result<novarocks_spi::connector::ConnectorMetadataMaintenanceIoGeometry> {
        let mut maximum: Option<novarocks_spi::connector::ConnectorMetadataMaintenanceIoGeometry> =
            None;
        for builder in self.builders.iter() {
            for quote in [
                builder.metadata_io_geometry,
                builder.write_io_geometry,
                builder.read_io_geometry,
            ]
            .into_iter()
            .flatten()
            {
                let actual = quote().with_context(|| {
                    format!(
                        "quote `{}` statement I/O factory",
                        builder.provider_id.as_str()
                    )
                })?;
                maximum = Some(match maximum {
                    None => actual,
                    Some(old) => {
                        novarocks_spi::connector::ConnectorMetadataMaintenanceIoGeometry::new(
                            old.max_concurrent_io().max(actual.max_concurrent_io()),
                            old.metadata_bytes().max(actual.metadata_bytes()),
                            old.initial_owned_bytes().max(actual.initial_owned_bytes()),
                        )
                    }
                });
            }
        }
        maximum.ok_or_else(|| {
            anyhow::anyhow!("server manifest has no enabled statement I/O geometry author")
        })
    }

    /// Pure fixed Source backing from each active staged-create implementation.
    /// No generation, SDK runtime, route or catalog session is constructed.
    pub fn staged_create_source_bytes(&self) -> anyhow::Result<u64> {
        let mut maximum = None;
        for builder in self.builders.iter() {
            if let Some(quote) = builder.staged_create_source_bytes {
                let actual = quote().with_context(|| {
                    format!(
                        "quote `{}` staged-create Source backing",
                        builder.provider_id.as_str()
                    )
                })?;
                anyhow::ensure!(actual != 0, "staged-create Source backing must be positive");
                maximum = Some(maximum.map_or(actual, |old: u64| old.max(actual)));
            }
        }
        maximum.ok_or_else(|| anyhow::anyhow!("server manifest has no staged-create Source author"))
    }

    pub fn compose_control_factories(
        &self,
        config: &NovaRocksConfig,
        runtime: tokio::runtime::Handle,
        filesystem: &novarocks_fs::FsAccessResources,
        #[cfg(novarocks_dev_controls)] test_slots: &crate::provider_test_slots::ProviderTestSlots,
    ) -> anyhow::Result<Vec<Arc<dyn ConnectorControlRoleBindingFactory>>> {
        let input = ProviderFactoryInput {
            config,
            runtime,
            filesystem,
            #[cfg(novarocks_dev_controls)]
            test_slots,
        };
        self.builders
            .iter()
            .map(|builder| {
                let factory = (builder.control)(&input).with_context(|| {
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

    /// Composes the installed pure provider program catalogue.
    ///
    /// Active pure builders are checked against their sealed read/write
    /// contracts. Suspended providers retain their contracts and role factories
    /// but do not enter this catalogue. No runtime instance, split or credential
    /// is bound.
    pub fn compose_pure_program_catalog(
        &self,
    ) -> anyhow::Result<PureProviderProgramCatalog<ConnectorCodecError>> {
        let manifest = self
            .contracts
            .definitions()
            .iter()
            .filter(|contract| {
                self.builders.iter().any(|builder| {
                    builder.provider_id == *contract.provider_id() && builder.pure.is_some()
                })
            })
            .map(|contract| {
                PureProviderManifestEntry::new(
                    contract.provider_id().clone(),
                    true,
                    contract.write().is_some(),
                )
            })
            .collect::<Vec<_>>();
        let definitions = self
            .builders
            .iter()
            .filter_map(|builder| builder.pure.map(|pure| pure(builder.provider_id.clone())))
            .collect();
        PureProviderProgramCatalog::try_new(&manifest, definitions, &CompositionControl)
            .map_err(|error| anyhow::anyhow!("compose pure provider program catalogue: {error}"))
    }

    pub fn compose_execution_factories(
        &self,
        config: &NovaRocksConfig,
        runtime: tokio::runtime::Handle,
        filesystem: &novarocks_fs::FsAccessResources,
        scan_io: &ScanIoServices,
        #[cfg(novarocks_dev_controls)] test_slots: &crate::provider_test_slots::ProviderTestSlots,
    ) -> anyhow::Result<Vec<Arc<dyn ConnectorExecutionRoleBindingFactory>>> {
        #[cfg(novarocks_dev_controls)]
        test_slots
            .register_storage(filesystem.storage_authority_registry().clone())
            .map_err(anyhow::Error::msg)?;
        let input = ProviderFactoryInput {
            config,
            runtime,
            filesystem,
            #[cfg(novarocks_dev_controls)]
            test_slots,
        };
        self.builders
            .iter()
            .map(|builder| {
                let factory = (builder.execution)(&input, scan_io).with_context(|| {
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
    input: &ProviderFactoryInput<'_>,
) -> anyhow::Result<Arc<dyn ConnectorControlRoleBindingFactory>> {
    let config = input.config;
    let runtime = input.runtime.clone();
    let metadata_binding = crate::composition::compose_iceberg_metadata_access_template(
        config,
        input.filesystem.clone(),
    )?;
    let max_inflight = NonZeroUsize::new(config.runtime.catalog_materialization_max_inflight)
        .ok_or_else(|| anyhow::anyhow!("catalog materialization max inflight must be nonzero"))?;
    let resources = IcebergMetadataResources::new(metadata_binding, runtime);
    #[cfg(novarocks_dev_controls)]
    let resources = resources.with_test_observation(input.test_slots.iceberg.clone());
    let factory = IcebergControlRoleBindingFactory::new(resources, max_inflight);
    #[cfg(all(novarocks_dev_controls, feature = "mem-1-m07-hms-listing-observe"))]
    let factory = factory.with_hms_listing_observation_slot(input.test_slots.hms.clone());
    Ok(Arc::new(factory))
}

fn build_iceberg_execution_factory(
    input: &ProviderFactoryInput<'_>,
    scan_io: &ScanIoServices,
) -> anyhow::Result<Arc<dyn ConnectorExecutionRoleBindingFactory>> {
    let resources = crate::composition::compose_iceberg_execution_resources(
        input.config,
        input.runtime.clone(),
        input.filesystem.clone(),
        scan_io,
    )?;
    #[cfg(novarocks_dev_controls)]
    let resources = resources.with_test_observation(input.test_slots.iceberg.clone());
    let options = iceberg_execution_reader_options(input.config);
    Ok(Arc::new(IcebergExecutionRoleBindingFactory::new(
        resources, options,
    )))
}

fn iceberg_execution_reader_options(
    config: &NovaRocksConfig,
) -> novarocks_connector_iceberg::typed_read::page_source_provider::IcebergPageSourceProviderOptions
{
    let mut options = novarocks_connector_iceberg::typed_read::page_source_provider::IcebergPageSourceProviderOptions::with_default_budget();
    options.reader_options.coalesce_reads = config.runtime.io_coalesce_read_enable;
    options.reader_options.coalesce_max_bytes = config.runtime.io_coalesce_read_max_buffer_size;
    options.reader_options.coalesce_max_gap = config.runtime.io_coalesce_read_max_distance_size;
    options
}

fn iceberg_pure_program(
    provider: ConnectorProviderId,
) -> PureProviderProgramDefinition<ConnectorCodecError> {
    PureProviderProgramDefinition::new(
        provider,
        Some(Arc::new(IcebergReadRecipeCompiler)),
        Some(Arc::new(IcebergWriteRecipeCompiler)),
    )
}

fn build_paimon_control_factory(
    _input: &ProviderFactoryInput<'_>,
) -> anyhow::Result<Arc<dyn ConnectorControlRoleBindingFactory>> {
    Ok(Arc::new(PaimonControlRoleBindingFactory))
}
fn build_paimon_execution_factory(
    _input: &ProviderFactoryInput<'_>,
    _scan_io: &ScanIoServices,
) -> anyhow::Result<Arc<dyn ConnectorExecutionRoleBindingFactory>> {
    Ok(Arc::new(PaimonExecutionRoleBindingFactory))
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

        fn open_catalog(
            &self,
            _identity: novarocks_spi::connector::CatalogScopeIdentity,
        ) -> Arc<dyn novarocks_spi::connector::ProviderCatalogControlFactory> {
            unreachable!("manifest projection never invokes provider behavior")
        }
    }

    impl novarocks_spi::connector::ProviderCatalogControlFactory for FixtureFactory {
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

    #[test]
    fn sealed_metadata_geometry_is_the_same_iceberg_factory_without_a_role_session() {
        let manifest = ServerProviderManifest::seal().unwrap();
        assert_eq!(
            manifest.metadata_maintenance_io_geometry().unwrap(),
            novarocks_connector_iceberg::metadata_maintenance_io_geometry().unwrap()
        );
    }

    #[test]
    fn staged_create_backing_comes_from_the_same_provider_without_a_role_session() {
        let manifest = ServerProviderManifest::seal().unwrap();
        let actual = manifest.staged_create_source_bytes().unwrap();
        assert_eq!(actual, novarocks_connector_iceberg::catalog_control::staged_create::staged_create_async_source_bytes().unwrap());
        assert!(actual > 0);
    }

    #[test]
    fn statement_geometry_includes_the_same_write_factory_and_recovery_encoder() {
        let manifest = ServerProviderManifest::seal().unwrap();
        let actual = manifest.statement_io_geometry().unwrap();
        let write = novarocks_connector_iceberg::write_control_io_geometry().unwrap();
        let maintenance = novarocks_connector_iceberg::metadata_maintenance_io_geometry().unwrap();
        let iceberg_read = novarocks_connector_iceberg::read_io_geometry().unwrap();
        assert_eq!(
            actual.max_concurrent_io(),
            write
                .max_concurrent_io()
                .max(maintenance.max_concurrent_io())
                .max(iceberg_read.max_concurrent_io())
        );
        assert_eq!(
            actual.metadata_bytes(),
            write
                .metadata_bytes()
                .max(maintenance.metadata_bytes())
                .max(iceberg_read.metadata_bytes())
        );
        assert_eq!(
            actual.initial_owned_bytes(),
            write
                .initial_owned_bytes()
                .max(maintenance.initial_owned_bytes())
                .max(iceberg_read.initial_owned_bytes())
        );
        assert_eq!(
            write.initial_owned_bytes().get(),
            (8 * 1024 * 1024) + (2 * 64 * 1024)
        );
        assert!(actual.initial_owned_bytes() > maintenance.initial_owned_bytes());
    }

    fn drifted_control_factory(
        _input: &ProviderFactoryInput<'_>,
    ) -> anyhow::Result<Arc<dyn ConnectorControlRoleBindingFactory>> {
        Ok(Arc::new(FixtureFactory {
            provider_id: ConnectorProviderId::parse("other").expect("fixture provider ID"),
        }))
    }

    fn fixture_control_factory(
        _input: &ProviderFactoryInput<'_>,
    ) -> anyhow::Result<Arc<dyn ConnectorControlRoleBindingFactory>> {
        Ok(Arc::new(FixtureFactory {
            provider_id: ConnectorProviderId::parse("fixture").expect("fixture provider ID"),
        }))
    }

    fn fixture_execution_factory(
        _input: &ProviderFactoryInput<'_>,
        _scan_io: &ScanIoServices,
    ) -> anyhow::Result<Arc<dyn ConnectorExecutionRoleBindingFactory>> {
        Ok(Arc::new(FixtureFactory {
            provider_id: ConnectorProviderId::parse("fixture").expect("fixture provider ID"),
        }))
    }

    /// A fixture with no pure facet; these cases never compose a catalogue.
    fn fixture_pure_program(
        provider: ConnectorProviderId,
    ) -> PureProviderProgramDefinition<ConnectorCodecError> {
        PureProviderProgramDefinition::new(provider, None, None)
    }

    fn counted_control_factory(
        input: &ProviderFactoryInput<'_>,
    ) -> anyhow::Result<Arc<dyn ConnectorControlRoleBindingFactory>> {
        CONTROL_BUILDS.fetch_add(1, Ordering::AcqRel);
        fixture_control_factory(input)
    }

    fn counted_execution_factory(
        input: &ProviderFactoryInput<'_>,
        scan_io: &ScanIoServices,
    ) -> anyhow::Result<Arc<dyn ConnectorExecutionRoleBindingFactory>> {
        EXECUTION_BUILDS.fetch_add(1, Ordering::AcqRel);
        fixture_execution_factory(input, scan_io)
    }

    #[test]
    fn sealing_rejects_a_missing_role_factory() {
        let error = ServerProviderManifest::seal_definitions(&[ProviderBuilderDefinition {
            contract: fixture_contract,
            control: Some(fixture_control_factory),
            execution: None,
            pure: Some(fixture_pure_program),
            metadata_io_geometry: None,
            write_io_geometry: None,
            read_io_geometry: None,
            staged_create_source_bytes: None,
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
                pure: Some(fixture_pure_program),
                metadata_io_geometry: None,
                write_io_geometry: None,
                read_io_geometry: None,
                staged_create_source_bytes: None,
            },
            ProviderBuilderDefinition {
                contract: duplicate_fixture_contract,
                control: Some(fixture_control_factory),
                execution: Some(fixture_execution_factory),
                pure: Some(fixture_pure_program),
                metadata_io_geometry: None,
                write_io_geometry: None,
                read_io_geometry: None,
                staged_create_source_bytes: None,
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
            pure: Some(fixture_pure_program),
            metadata_io_geometry: None,
            write_io_geometry: None,
            read_io_geometry: None,
            staged_create_source_bytes: None,
        }])
        .expect("contract-only sealing must not invoke factories");
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let filesystem = crate::composition::compose_connector_file_planning_resources(
            &NovaRocksConfig::default(),
            runtime.handle().clone(),
        )
        .expect("explicit FS resources");
        #[cfg(novarocks_dev_controls)]
        let test_slots = crate::provider_test_slots::ProviderTestSlots::default();
        let error = manifest
            .compose_control_factories(
                &NovaRocksConfig::default(),
                runtime.handle().clone(),
                &filesystem,
                #[cfg(novarocks_dev_controls)]
                &test_slots,
            )
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
            pure: Some(fixture_pure_program),
            metadata_io_geometry: None,
            write_io_geometry: None,
            read_io_geometry: None,
            staged_create_source_bytes: None,
        }])
        .expect("manifest");
        assert_eq!(CONTROL_BUILDS.load(Ordering::Acquire), 0);
        assert_eq!(EXECUTION_BUILDS.load(Ordering::Acquire), 0);

        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let filesystem = crate::composition::compose_connector_file_planning_resources(
            &NovaRocksConfig::default(),
            runtime.handle().clone(),
        )
        .expect("explicit FS resources");
        #[cfg(novarocks_dev_controls)]
        let test_slots = crate::provider_test_slots::ProviderTestSlots::default();
        manifest
            .compose_control_factories(
                &NovaRocksConfig::default(),
                runtime.handle().clone(),
                &filesystem,
                #[cfg(novarocks_dev_controls)]
                &test_slots,
            )
            .expect("FE projection");
        assert_eq!(CONTROL_BUILDS.load(Ordering::Acquire), 1);
        assert_eq!(EXECUTION_BUILDS.load(Ordering::Acquire), 0);
    }

    #[test]
    fn native_carriers_and_both_role_projections_share_one_manifest() {
        let manifest = ServerProviderManifest::seal().expect("server provider manifest");
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let filesystem = crate::composition::compose_connector_file_planning_resources(
            &NovaRocksConfig::default(),
            runtime.handle().clone(),
        )
        .expect("explicit FS resources");
        #[cfg(novarocks_dev_controls)]
        let test_slots = crate::provider_test_slots::ProviderTestSlots::default();
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
            .compose_control_factories(
                &config,
                runtime.handle().clone(),
                &filesystem,
                #[cfg(novarocks_dev_controls)]
                &test_slots,
            )
            .expect("FE projection")
            .into_iter()
            .map(|factory| factory.provider_id().as_str().to_owned())
            .collect::<Vec<_>>();
        let scan_io =
            crate::scan_io::ScanIoRuntime::start(&config.runtime).expect("scan I/O runtime");
        let execution_ids = manifest
            .compose_execution_factories(
                &config,
                runtime.handle().clone(),
                &filesystem,
                &scan_io.services(),
                #[cfg(novarocks_dev_controls)]
                &test_slots,
            )
            .expect("BE projection")
            .into_iter()
            .map(|factory| factory.provider_id().as_str().to_owned())
            .collect::<Vec<_>>();

        assert_eq!(contract_ids, vec!["iceberg", "paimon"]);
        assert_eq!(carrier_ids, contract_ids);
        assert_eq!(control_ids, contract_ids);
        assert_eq!(execution_ids, contract_ids);
        #[cfg(novarocks_dev_controls)]
        {
            // Building either role attaches inert slots, without activating a session.
            test_slots
                .iceberg
                .activate(Vec::new())
                .expect("role composition must not activate provider observations");
            test_slots.iceberg.close();
        }
        runtime
            .block_on(scan_io.shutdown())
            .expect("drain BE scan I/O runtime");
    }

    #[test]
    fn iceberg_execution_uses_composed_coalesce_policy() {
        let mut config = NovaRocksConfig::default();
        config.runtime.io_coalesce_read_enable = false;
        config.runtime.io_coalesce_read_max_buffer_size = 2 * 1024 * 1024;
        config.runtime.io_coalesce_read_max_distance_size = 16 * 1024;
        let options = iceberg_execution_reader_options(&config);
        assert!(!options.reader_options.coalesce_reads);
        assert_eq!(options.reader_options.coalesce_max_bytes, 2 * 1024 * 1024);
        assert_eq!(options.reader_options.coalesce_max_gap, 16 * 1024);
    }

    /// Suspended providers keep their wire contracts but install no pure ports.
    #[test]
    fn the_server_manifest_composes_its_pure_program_catalogue() {
        let manifest = ServerProviderManifest::seal().expect("server provider manifest");
        let catalog = manifest
            .compose_pure_program_catalog()
            .expect("pure provider program catalogue");
        assert_eq!(catalog.provider_count(), 1);
        let paimon = manifest
            .builders
            .iter()
            .find(|builder| builder.provider_id.as_str() == "paimon")
            .unwrap();
        assert!(paimon.pure.is_none());
        assert!(paimon.read_io_geometry.is_none());
        let facets = manifest
            .contracts()
            .definitions()
            .iter()
            .map(|contract| {
                (
                    contract.provider_id().as_str().to_owned(),
                    contract.write().is_some(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            facets,
            vec![("iceberg".to_owned(), true), ("paimon".to_owned(), false)]
        );
    }

    /// Installed ports that disagree with the sealed contract fail
    /// composition rather than admitting a provider with a missing facet.
    #[test]
    fn pure_ports_that_disagree_with_their_contract_fail_composition() {
        let manifest = ServerProviderManifest::seal_definitions(&[ProviderBuilderDefinition {
            contract: fixture_contract,
            control: Some(fixture_control_factory),
            execution: Some(fixture_execution_factory),
            pure: Some(fixture_pure_program),
            metadata_io_geometry: None,
            write_io_geometry: None,
            read_io_geometry: None,
            staged_create_source_bytes: None,
        }])
        .expect("manifest");
        let error = manifest
            .compose_pure_program_catalog()
            .err()
            .expect("a read contract without a pure read port must fail")
            .to_string();
        assert!(
            error.contains("installed pure facets differ from the manifest"),
            "{error}"
        );
    }
}
