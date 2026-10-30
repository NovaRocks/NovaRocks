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

//! Provider-owned Iceberg catalog client construction.
//!
//! The caller supplies execution of the returned futures. This module never
//! discovers a Tokio runtime, so a future control generation can bind all
//! catalog I/O to its role-local runtime.

use std::collections::HashMap;
use std::sync::Arc;

use crate::access_binding::IcebergReadBinding;
use crate::catalog_config::{IcebergCatalogConfiguration, IcebergCatalogKind};

/// One generation's catalog facts. Hadoop and HMS retain their exact SDK read
/// client; REST retains only the provider-owned endpoints and their session.
/// Production has no generic REST SDK facade or second configuration owner.
pub struct IcebergCatalogClient {
    hms: Option<Arc<crate::iceberg_catalog_hms::HmsCatalog>>,
    generic: Option<Arc<dyn crate::iceberg::Catalog>>,
    hadoop: Option<Arc<crate::hadoop_catalog::HadoopFileSystemCatalog>>,
    rest_endpoints: Option<Arc<crate::rest_protocol::endpoints::RestEndpoints>>,
    rest_access_delegation: RestAccessDelegationMode,
}

/// Explicit catalog-definition mode.  REST response presence must never decide
/// whether a generation is allowed to accept vended material.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RestAccessDelegationMode {
    Static,
    Vended,
}

impl IcebergCatalogClient {
    pub(crate) fn hms(&self) -> Option<&Arc<crate::iceberg_catalog_hms::HmsCatalog>> {
        self.hms.as_ref()
    }

    pub fn generic(&self) -> Option<&Arc<dyn crate::iceberg::Catalog>> {
        self.generic.as_ref()
    }

    pub fn hadoop(&self) -> Option<&Arc<crate::hadoop_catalog::HadoopFileSystemCatalog>> {
        self.hadoop.as_ref()
    }

    pub(crate) fn rest_endpoints(
        &self,
    ) -> Option<&Arc<crate::rest_protocol::endpoints::RestEndpoints>> {
        self.rest_endpoints.as_ref()
    }

    pub(crate) const fn rest_access_delegation(&self) -> RestAccessDelegationMode {
        self.rest_access_delegation
    }
}

pub fn build_hadoop_catalog(
    configuration: &IcebergCatalogConfiguration,
    binding: IcebergReadBinding,
    runtime: crate::iceberg::Runtime,
) -> Result<crate::hadoop_catalog::HadoopFileSystemCatalog, String> {
    if configuration.kind != IcebergCatalogKind::Hadoop {
        return Err(format!(
            "build Hadoop Iceberg catalog called for {:?} configuration",
            configuration.kind
        ));
    }
    let file_io =
        crate::fs_io::build_file_io_for_location(&configuration.warehouse_uri, binding.clone());
    Ok(
        crate::hadoop_catalog::HadoopFileSystemCatalog::new_with_binding(
            file_io,
            configuration.warehouse_uri.clone(),
            binding,
            runtime,
        ),
    )
}

pub(crate) const REST_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
pub(crate) const REST_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Read catalog listing admission, connection setup, and ordinary read-phase
/// bounds. The third value is not a shared-client or publication read timeout.
/// This projection grants no capacity and does not make them configurable.
pub const fn frozen_sdk_listing_parameters() -> (usize, std::time::Duration, std::time::Duration) {
    (
        crate::catalog::listing_admission::LISTING_CONCURRENCY,
        REST_CONNECT_TIMEOUT,
        REST_READ_TIMEOUT,
    )
}

/// One REST generation's provider-owned endpoints and single session.
struct RestGeneration {
    endpoints: Arc<crate::rest_protocol::endpoints::RestEndpoints>,
}

/// Build a REST catalog generation for the selected storage-authority mode.
///
/// The generation's FE auth owner, HTTP client and endpoints share one
/// configuration authority, immutable session binding and connection pool.
///
/// A vended generation may authenticate to the REST control plane, but it has
/// no storage capability until a table response contributes an attempt-scoped
/// lease.  Do not install a warehouse storage factory in that mode: deferred
/// responses materialize a FileIO only after the collector has accepted that
/// response-local lease.
async fn build_rest_catalog_with_access_delegation(
    configuration: &IcebergCatalogConfiguration,
    binding: IcebergReadBinding,
    rest_access_delegation: RestAccessDelegationMode,
    runtime: crate::iceberg::Runtime,
    activity: Option<Arc<crate::activity::GenerationActivity>>,
) -> Result<RestGeneration, String> {
    use crate::rest_protocol::wire::{REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE};

    if configuration.kind != IcebergCatalogKind::Rest {
        return Err(format!(
            "build REST Iceberg catalog called for {:?} configuration",
            configuration.kind
        ));
    }
    let uri = configuration.rest_uri.clone().ok_or_else(|| {
        "REST iceberg catalog entry missing rest_uri (CREATE EXTERNAL CATALOG must set `uri`)"
            .to_string()
    })?;
    let mut properties = configuration
        .properties
        .iter()
        .filter(|(key, _)| key != "type")
        .cloned()
        .collect::<HashMap<_, _>>();
    properties.insert(REST_CATALOG_PROP_URI.to_string(), uri);
    if !configuration.warehouse_uri.is_empty() {
        properties.insert(
            REST_CATALOG_PROP_WAREHOUSE.to_string(),
            configuration.warehouse_uri.clone(),
        );
    }
    // Validation runs before any client exists (L5, L15, L26).
    let definition = configuration.rest_persistent_definition()?;
    let identity = binding
        .resolve_catalog_control_identity()
        .map_err(|error| format!("resolve REST catalog-control identity: {error}"))?;
    let binding_declares_vending = binding.declares_data_credential_vending();
    let storage_factory = match rest_access_delegation {
        RestAccessDelegationMode::Static => {
            Some(storage_factory(&configuration.warehouse_uri, binding))
        }
        RestAccessDelegationMode::Vended => None,
    };
    let vending_declared = binding_declares_vending;
    assemble_rest_generation(
        properties,
        definition.server_routing_headers().clone(),
        vending_declared,
        identity,
        storage_factory,
        runtime,
        activity,
    )
    .await
}

/// Assemble one REST generation from its already-validated persistent
/// properties. The endpoints own establishment; no SDK client is constructed.
// Design: ADR-0175 (docs/adr/ADR-0175-iceberg-rest-session-authority-and-mutation-evidence.md)
async fn assemble_rest_generation(
    properties: HashMap<String, String>,
    declared_routing_headers: std::collections::BTreeSet<String>,
    vending_declared: bool,
    identity: Option<crate::access_binding::IcebergRestAuthMaterial>,
    storage_factory: Option<Arc<dyn crate::iceberg::io::StorageFactory>>,
    runtime: crate::iceberg::Runtime,
    activity: Option<Arc<crate::activity::GenerationActivity>>,
) -> Result<RestGeneration, String> {
    let persistent =
        crate::rest_protocol::auth::PersistentRestConfig::from_properties(&properties)?;
    let http = crate::rest_protocol::http::RestHttpClient::new()?;
    let owner = crate::rest_protocol::auth::RestAuthOwner::frontend_observed(
        identity, persistent, http, activity,
    )?;
    let endpoints = Arc::new(crate::rest_protocol::endpoints::RestEndpoints::new(
        Arc::new(properties),
        owner,
        runtime,
        storage_factory,
        declared_routing_headers,
        vending_declared,
    ));
    Ok(RestGeneration { endpoints })
}

/// A vended REST generation built from raw properties, for tests that drive
/// the REST owner against a scripted server.
#[cfg(test)]
pub(crate) async fn build_vended_rest_generation_for_test(
    properties: HashMap<String, String>,
    identity: Option<crate::access_binding::IcebergRestAuthMaterial>,
    runtime: crate::iceberg::Runtime,
) -> Result<Arc<crate::rest_protocol::endpoints::RestEndpoints>, String> {
    let generation = assemble_rest_generation(
        properties,
        Default::default(),
        true,
        identity,
        None,
        runtime,
        None,
    )
    .await?;
    Ok(generation.endpoints)
}

pub async fn build_hms_catalog(
    configuration: &IcebergCatalogConfiguration,
    binding: IcebergReadBinding,
    runtime: crate::iceberg::Runtime,
) -> Result<crate::iceberg_catalog_hms::HmsCatalog, String> {
    use crate::iceberg::CatalogBuilder;
    use crate::iceberg_catalog_hms::{
        HMS_CATALOG_PROP_THRIFT_TRANSPORT, HMS_CATALOG_PROP_URI, HMS_CATALOG_PROP_WAREHOUSE,
        HmsCatalogBuilder, THRIFT_TRANSPORT_BUFFERED, THRIFT_TRANSPORT_FRAMED,
    };

    if configuration.kind != IcebergCatalogKind::Hive {
        return Err(format!(
            "build Hive Iceberg catalog called for {:?} configuration",
            configuration.kind
        ));
    }
    let hms_uri = configuration.hms_uris.clone().ok_or_else(|| {
        "hive iceberg catalog entry missing hms_uris (CREATE EXTERNAL CATALOG must set `hive.metastore.uris`)"
            .to_string()
    })?;
    let mut properties = HashMap::new();
    properties.insert(HMS_CATALOG_PROP_URI.to_string(), hms_uri);
    if !configuration.warehouse_uri.is_empty() {
        properties.insert(
            HMS_CATALOG_PROP_WAREHOUSE.to_string(),
            configuration.warehouse_uri.clone(),
        );
    }
    let framed = configuration
        .properties
        .iter()
        .find(|(key, _)| key == "hive.metastore.thrift.framed")
        .is_some_and(|(_, value)| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        });
    properties.insert(
        HMS_CATALOG_PROP_THRIFT_TRANSPORT.to_string(),
        if framed {
            THRIFT_TRANSPORT_FRAMED.to_string()
        } else {
            THRIFT_TRANSPORT_BUFFERED.to_string()
        },
    );
    HmsCatalogBuilder::default()
        .with_runtime(runtime)
        .with_storage_factory(storage_factory(&configuration.warehouse_uri, binding))
        .load("hms".to_string(), properties)
        .await
        .map_err(|error| format!("build HMS iceberg catalog: {error}"))
}

/// Construct the single concrete client retained by one control generation.
pub async fn build_catalog_client(
    configuration: &IcebergCatalogConfiguration,
    binding: IcebergReadBinding,
    runtime: crate::iceberg::Runtime,
) -> Result<IcebergCatalogClient, String> {
    build_catalog_client_with_rest_access_delegation(
        configuration,
        binding,
        RestAccessDelegationMode::Static,
        runtime,
        None,
    )
    .await
}

/// Build the generation's one catalog client with an explicit credential mode
/// selected from typed catalog properties by the composition root.
pub(crate) async fn build_catalog_client_with_rest_access_delegation(
    configuration: &IcebergCatalogConfiguration,
    binding: IcebergReadBinding,
    rest_access_delegation: RestAccessDelegationMode,
    runtime: crate::iceberg::Runtime,
    activity: Option<Arc<crate::activity::GenerationActivity>>,
) -> Result<IcebergCatalogClient, String> {
    crate::catalog::publication::CatalogEvidencePolicy::from_properties(&configuration.properties)?;
    match configuration.kind {
        IcebergCatalogKind::Hadoop => {
            let hadoop = Arc::new(build_hadoop_catalog(configuration, binding, runtime)?);
            let generic: Arc<dyn crate::iceberg::Catalog> = hadoop.clone();
            Ok(IcebergCatalogClient {
                hms: None,
                generic: Some(generic),
                hadoop: Some(hadoop),
                rest_endpoints: None,
                rest_access_delegation: RestAccessDelegationMode::Static,
            })
        }
        IcebergCatalogKind::Rest => {
            let RestGeneration { endpoints } = build_rest_catalog_with_access_delegation(
                configuration,
                binding,
                rest_access_delegation,
                runtime,
                activity,
            )
            .await?;
            Ok(IcebergCatalogClient {
                hms: None,
                generic: None,
                hadoop: None,
                rest_endpoints: Some(endpoints),
                rest_access_delegation,
            })
        }
        IcebergCatalogKind::Hive => {
            let hms = Arc::new(build_hms_catalog(configuration, binding, runtime).await?);
            Ok(IcebergCatalogClient {
                generic: Some(hms.clone() as Arc<dyn crate::iceberg::Catalog>),
                hms: Some(hms),
                hadoop: None,
                rest_endpoints: None,
                rest_access_delegation: RestAccessDelegationMode::Static,
            })
        }
    }
}

fn storage_factory(
    warehouse_uri: &str,
    binding: IcebergReadBinding,
) -> Arc<dyn crate::iceberg::io::StorageFactory> {
    crate::fs_io::build_storage_factory_for_location(warehouse_uri, binding)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};

    use super::*;

    fn local_binding() -> IcebergReadBinding {
        let runtime = tokio::runtime::Handle::current();
        IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime)),
        )
    }

    #[tokio::test]
    async fn dispatches_hadoop_catalog_from_provider_configuration() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            )],
        )
        .expect("configuration");

        let client = build_catalog_client(
            &configuration,
            local_binding(),
            crate::iceberg::Runtime::try_current().expect("test runtime"),
        )
        .await
        .expect("provider catalog");
        assert!(client.rest_endpoints().is_none());
        assert!(client.hadoop().is_some());
        assert!(Arc::strong_count(client.generic().expect("Hadoop read client")) >= 1);
    }
}
