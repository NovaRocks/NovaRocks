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

//! StarRocks' complete FE and BE role-binding factories.
//!
//! Server startup constructs role-local metadata sources and projects them
//! into [`StarRocksRoleBindingResources`]. This provider-owned factory then
//! resolves an exact durable `local_binding` reference without exposing those
//! resources to catalog state or native wire payloads.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use futures::future::BoxFuture;
use novarocks_spi::connector::{CatalogProperties, ConnectorProviderId};
use novarocks_spi::connector::{
    ConnectorControlRoleBinding, ConnectorControlRoleBindingFactory, ConnectorExecutionRoleBinding,
    ConnectorExecutionRoleBindingFactory, ConnectorMaterializationError,
    ConnectorMaterializationErrorClass, ConnectorMaterializationRetryDisposition,
    MaterializationContext, NormalizedCatalogProperties,
};

use crate::{
    STARROCKS_PROVIDER_ID, StarRocksConnectorConfig, StarRocksControlGeneration,
    StarRocksLocalBindingRef, StarRocksMetadataSource,
};

/// Immutable FE-local resources indexed by the exact local-binding identity.
///
/// The resource object intentionally contains only provider-defined metadata
/// sources. Server configuration, endpoints, and credentials are consumed
/// before this object is constructed and cannot cross this API boundary.
#[derive(Clone)]
pub struct StarRocksRoleBindingResources {
    metadata_sources: BTreeMap<StarRocksLocalBindingRef, Arc<dyn StarRocksMetadataSource>>,
}

impl StarRocksRoleBindingResources {
    pub fn new(
        metadata_sources: BTreeMap<StarRocksLocalBindingRef, Arc<dyn StarRocksMetadataSource>>,
    ) -> Self {
        Self { metadata_sources }
    }

    /// Resolves one exact FE-local metadata source without remote I/O or fallback.
    pub fn resolve(
        &self,
        local_binding: &StarRocksLocalBindingRef,
    ) -> Result<Arc<dyn StarRocksMetadataSource>, StarRocksRoleBindingResourceLookupError> {
        self.metadata_sources
            .get(local_binding)
            .cloned()
            .ok_or_else(|| StarRocksRoleBindingResourceLookupError::NotFound(local_binding.clone()))
    }
}

impl fmt::Debug for StarRocksRoleBindingResources {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StarRocksRoleBindingResources")
            .field(
                "local_bindings",
                &self.metadata_sources.keys().collect::<Vec<_>>(),
            )
            .finish()
    }
}

/// A stable local-resource lookup failure that contains no endpoint or credential.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StarRocksRoleBindingResourceLookupError {
    NotFound(StarRocksLocalBindingRef),
}

impl fmt::Display for StarRocksRoleBindingResourceLookupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound(local_binding) => write!(
                formatter,
                "StarRocks local binding `{}` is not configured on this frontend",
                local_binding.as_str()
            ),
        }
    }
}

impl std::error::Error for StarRocksRoleBindingResourceLookupError {}

/// FE-only StarRocks control factory.
///
/// Metadata I/O stays behind the returned control generation and uses each
/// metadata request's existing context.
#[derive(Clone)]
pub struct StarRocksControlRoleBindingFactory {
    resources: StarRocksRoleBindingResources,
}

impl StarRocksControlRoleBindingFactory {
    pub fn new(resources: StarRocksRoleBindingResources) -> Self {
        Self { resources }
    }
}

impl ConnectorControlRoleBindingFactory for StarRocksControlRoleBindingFactory {
    fn provider_id(&self) -> ConnectorProviderId {
        ConnectorProviderId::parse("starrocks").expect("static provider ID")
    }

    fn normalize_and_validate(
        &self,
        properties: CatalogProperties,
    ) -> Result<NormalizedCatalogProperties, ConnectorMaterializationError> {
        ensure_starrocks(&properties)?;
        starrocks_local_binding(&properties)?;
        NormalizedCatalogProperties::try_new(properties).map_err(invalid_definition)
    }

    fn materialize(
        &self,
        properties: NormalizedCatalogProperties,
        context: MaterializationContext,
    ) -> BoxFuture<'static, Result<ConnectorControlRoleBinding, ConnectorMaterializationError>>
    {
        let resources = self.resources.clone();
        Box::pin(async move {
            context.check_active()?;
            let catalog_properties = properties.as_catalog_properties().clone();
            ensure_starrocks(&catalog_properties)?;
            let local_binding = starrocks_local_binding(&catalog_properties)?;
            let metadata = resources.resolve(&local_binding).map_err(|error| {
                ConnectorMaterializationError::new(
                    ConnectorMaterializationErrorClass::InvalidDefinition,
                    ConnectorMaterializationRetryDisposition::UntilDefinitionChanges,
                    error.to_string(),
                )
            })?;
            let control = StarRocksControlGeneration::try_new(
                StarRocksConnectorConfig::new(
                    catalog_properties.handle().catalog_name().clone(),
                    local_binding,
                ),
                metadata,
            )
            .and_then(|binding| binding.with_catalog_properties(catalog_properties))
            .map_err(ConnectorMaterializationError::from)?;
            context.check_active()?;
            ConnectorControlRoleBinding::try_new(properties, Arc::new(control), None, None)
                .map_err(ConnectorMaterializationError::from)
        })
    }
}

/// BE-only StarRocks factory. It carries no execution capability until a
/// separately accepted StarRocks read execution contract exists.
#[derive(Default)]
pub struct StarRocksExecutionRoleBindingFactory;

impl StarRocksExecutionRoleBindingFactory {
    pub const fn new() -> Self {
        Self
    }
}

impl ConnectorExecutionRoleBindingFactory for StarRocksExecutionRoleBindingFactory {
    fn provider_id(&self) -> ConnectorProviderId {
        ConnectorProviderId::parse("starrocks").expect("static provider ID")
    }

    fn bind(
        &self,
        properties: &NormalizedCatalogProperties,
    ) -> Result<ConnectorExecutionRoleBinding, ConnectorMaterializationError> {
        ensure_starrocks(properties.as_catalog_properties())?;
        ConnectorExecutionRoleBinding::try_new(properties.clone(), None, None)
            .map_err(ConnectorMaterializationError::from)
    }
}

fn ensure_starrocks(properties: &CatalogProperties) -> Result<(), ConnectorMaterializationError> {
    if properties.provider_id().as_str() == "starrocks" {
        return Ok(());
    }
    Err(ConnectorMaterializationError::new(
        ConnectorMaterializationErrorClass::InvalidDefinition,
        ConnectorMaterializationRetryDisposition::UntilDefinitionChanges,
        format!("{STARROCKS_PROVIDER_ID} role binding factory received another provider kind"),
    ))
}

fn starrocks_local_binding(
    properties: &CatalogProperties,
) -> Result<StarRocksLocalBindingRef, ConnectorMaterializationError> {
    const LOCAL_BINDING_PROPERTY: &str = "local_binding";

    let mut matches = properties
        .execution_properties()
        .iter()
        .filter(|property| property.key() == LOCAL_BINDING_PROPERTY);
    let Some(property) = matches.next() else {
        return Err(invalid_definition(
            "StarRocks catalog definition requires a local_binding property",
        ));
    };
    if matches.next().is_some() {
        return Err(invalid_definition(
            "StarRocks catalog definition declares duplicate local_binding properties",
        ));
    }
    StarRocksLocalBindingRef::parse(property.value()).map_err(|error| {
        invalid_definition(format!(
            "invalid StarRocks local_binding catalog property: {error}"
        ))
    })
}

fn invalid_definition(detail: impl Into<String>) -> ConnectorMaterializationError {
    ConnectorMaterializationError::new(
        ConnectorMaterializationErrorClass::InvalidDefinition,
        ConnectorMaterializationRetryDisposition::UntilDefinitionChanges,
        detail.into(),
    )
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;
    use novarocks_spi::connector::{
        CatalogHandle, CatalogProperty, CatalogVersion, ConnectorCancellation, ConnectorError,
        ConnectorInstanceId, ConnectorRequestContext,
    };

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    struct MetadataMustNotRun;

    impl StarRocksMetadataSource for MetadataMustNotRun {
        fn namespace_exists(
            &self,
            _namespace: &str,
            _context: &ConnectorRequestContext,
        ) -> Result<bool, ConnectorError> {
            panic!("control materialization must not execute metadata I/O")
        }

        fn table_exists(
            &self,
            _namespace: &str,
            _table: &str,
            _context: &ConnectorRequestContext,
        ) -> Result<bool, ConnectorError> {
            panic!("control materialization must not execute metadata I/O")
        }

        fn list_tables(
            &self,
            _namespace: &str,
            _context: &ConnectorRequestContext,
        ) -> Result<Vec<String>, ConnectorError> {
            panic!("control materialization must not execute metadata I/O")
        }

        fn load_table(
            &self,
            _namespace: &str,
            _table: &str,
            _context: &ConnectorRequestContext,
        ) -> Result<crate::StarRocksResolvedTable, ConnectorError> {
            panic!("control materialization must not execute metadata I/O")
        }
    }

    fn request_context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            Arc::new(NeverCancelled),
            64 * 1024,
            128 * 1024,
        )
        .expect("request context")
    }

    fn properties(
        instance_id: &str,
        kind: ConnectorProviderId,
        local_binding: Option<&str>,
    ) -> CatalogProperties {
        properties_with_local_bindings(instance_id, kind, local_binding.into_iter().collect())
    }

    fn properties_with_local_bindings(
        instance_id: &str,
        kind: ConnectorProviderId,
        local_bindings: Vec<&str>,
    ) -> CatalogProperties {
        CatalogProperties::new(
            CatalogHandle::new(
                ConnectorInstanceId::parse(instance_id).expect("catalog"),
                CatalogVersion::from_bytes([7; 32]),
            ),
            kind,
            1,
            local_bindings
                .into_iter()
                .map(|value| CatalogProperty::new("local_binding", value).expect("property"))
                .collect(),
            Vec::new(),
        )
        .expect("catalog properties")
    }

    fn control_factory() -> StarRocksControlRoleBindingFactory {
        let sources = ["metadata-blue", "metadata-green"]
            .into_iter()
            .map(|name| {
                (
                    StarRocksLocalBindingRef::parse(name).expect("local binding"),
                    Arc::new(MetadataMustNotRun) as Arc<dyn StarRocksMetadataSource>,
                )
            })
            .collect();
        StarRocksControlRoleBindingFactory::new(StarRocksRoleBindingResources::new(sources))
    }

    #[test]
    fn control_materialization_stamps_the_exact_properties_without_metadata_io() {
        let factory = control_factory();
        let normalized = factory
            .normalize_and_validate(properties(
                "catalog.starrocks",
                ConnectorProviderId::parse("starrocks").expect("static provider ID"),
                Some("metadata-blue"),
            ))
            .expect("normalize StarRocks properties");
        let binding = futures::executor::block_on(factory.materialize(
            normalized.clone(),
            MaterializationContext::new(Instant::now() + Duration::from_secs(1)),
        ))
        .expect("materialize local StarRocks control generation");

        assert_eq!(binding.properties(), &normalized);
        assert_eq!(
            binding.control().catalog_handle().expect("exact handle"),
            normalized.handle()
        );
        assert!(binding.read().is_none());
        assert!(binding.write().is_none());
    }

    #[test]
    fn control_materialization_keeps_two_catalogs_on_their_exact_local_bindings() {
        let factory = control_factory();
        let blue = factory
            .normalize_and_validate(properties(
                "catalog.starrocks-blue",
                ConnectorProviderId::parse("starrocks").expect("static provider ID"),
                Some("metadata-blue"),
            ))
            .expect("normalize blue catalog");
        let green = factory
            .normalize_and_validate(properties(
                "catalog.starrocks-green",
                ConnectorProviderId::parse("starrocks").expect("static provider ID"),
                Some("metadata-green"),
            ))
            .expect("normalize green catalog");

        let blue = futures::executor::block_on(factory.materialize(
            blue,
            MaterializationContext::new(Instant::now() + Duration::from_secs(1)),
        ))
        .expect("materialize blue catalog");
        let green = futures::executor::block_on(factory.materialize(
            green,
            MaterializationContext::new(Instant::now() + Duration::from_secs(1)),
        ))
        .expect("materialize green catalog");

        assert_eq!(
            blue.control()
                .execution_distribution()
                .declaration(&request_context())
                .expect("blue declaration")
                .starrocks_local_binding(),
            Some("metadata-blue")
        );
        assert_eq!(
            green
                .control()
                .execution_distribution()
                .declaration(&request_context())
                .expect("green declaration")
                .starrocks_local_binding(),
            Some("metadata-green")
        );
    }

    #[test]
    fn control_materialization_requires_an_exact_configured_local_binding() {
        let factory = control_factory();
        let missing = factory
            .normalize_and_validate(properties(
                "catalog.starrocks",
                ConnectorProviderId::parse("starrocks").expect("static provider ID"),
                None,
            ))
            .expect_err("local binding is required");
        assert_eq!(
            missing.disposition(),
            ConnectorMaterializationRetryDisposition::UntilDefinitionChanges
        );

        let unknown = factory
            .normalize_and_validate(properties(
                "catalog.starrocks",
                ConnectorProviderId::parse("starrocks").expect("static provider ID"),
                Some("missing"),
            ))
            .expect("the definition is structurally valid");
        let error = match futures::executor::block_on(factory.materialize(
            unknown,
            MaterializationContext::new(Instant::now() + Duration::from_secs(1)),
        )) {
            Ok(_) => panic!("unconfigured local binding must fail closed"),
            Err(error) => error,
        };
        assert_eq!(
            error.disposition(),
            ConnectorMaterializationRetryDisposition::UntilDefinitionChanges
        );
        assert!(
            error
                .to_string()
                .contains("StarRocks local binding `missing` is not configured")
        );
    }

    #[test]
    fn control_normalization_rejects_invalid_local_bindings_and_catalog_properties_reject_duplicates()
     {
        let factory = control_factory();
        let invalid = factory
            .normalize_and_validate(properties_with_local_bindings(
                "catalog.starrocks",
                ConnectorProviderId::parse("starrocks").expect("static provider ID"),
                vec!["not-ascii-\u{4e2d}"],
            ))
            .expect_err("invalid local binding definition must fail closed");
        assert_eq!(
            invalid.disposition(),
            ConnectorMaterializationRetryDisposition::UntilDefinitionChanges
        );

        let duplicate = CatalogProperties::new(
            CatalogHandle::new(
                ConnectorInstanceId::parse("catalog.starrocks").expect("catalog"),
                CatalogVersion::from_bytes([7; 32]),
            ),
            ConnectorProviderId::parse("starrocks").expect("static provider ID"),
            1,
            vec![
                CatalogProperty::new("local_binding", "metadata-blue").expect("property"),
                CatalogProperty::new("local_binding", "metadata-green").expect("property"),
            ],
            Vec::new(),
        )
        .expect_err("duplicate local_binding must not enter a catalog definition");
        assert!(
            duplicate
                .to_string()
                .contains("duplicate catalog property key")
        );
    }

    #[test]
    fn execution_factory_publishes_no_starrocks_capability_until_its_typed_contract_exists() {
        let normalized = NormalizedCatalogProperties::try_new(properties(
            "catalog.starrocks",
            ConnectorProviderId::parse("starrocks").expect("static provider ID"),
            None,
        ))
        .expect("normalized StarRocks properties");

        let binding = StarRocksExecutionRoleBindingFactory::new()
            .bind(&normalized)
            .expect("StarRocks has an explicit capability-free local binding");

        assert_eq!(binding.properties(), &normalized);
        assert!(binding.read().is_none());
        assert!(binding.write().is_none());
    }
}
