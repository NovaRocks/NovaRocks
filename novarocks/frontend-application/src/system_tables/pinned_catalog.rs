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

//! Typed catalog listings fenced by one statement-selected generation.
use novarocks_spi::connector::{
    ConnectorControlPlanningPin, ConnectorError, ConnectorErrorKind,
    ConnectorListNamespacesRequest, ConnectorListTablesRequest, ConnectorListViewsRequest,
    ConnectorListingPageSize, ConnectorNamespaceIdentity, ConnectorNamespaceRequest,
    ConnectorOperationControl, ConnectorRequestContext, ConnectorTableIdentity,
    ConnectorTableRequest, ConnectorTableResolution, ConnectorViewIdentity, ConnectorViewRequest,
    DiscoveredRelation, DiscoveryProjection, DiscoveryRelationKind, DiscoveryViewDomainUnavailable,
    ProviderMetadataCpuPort,
};
use std::sync::Arc;

pub(crate) struct PinnedCatalogListing {
    pin: ConnectorControlPlanningPin,
    context: ConnectorRequestContext,
}
impl PinnedCatalogListing {
    pub(crate) fn new(
        pin: ConnectorControlPlanningPin,
        context: ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        let context = crate::connector::context_for_planning_pin_typed(&pin, context)?;
        Ok(Self { pin, context })
    }
    pub(crate) fn pin(&self) -> &ConnectorControlPlanningPin {
        &self.pin
    }
    fn namespace(&self, name: &str) -> ConnectorNamespaceIdentity {
        ConnectorNamespaceIdentity {
            instance_id: self.pin.binding().descriptor().instance_id.clone(),
            namespace: name.into(),
        }
    }
    pub(crate) async fn schema_names(&self) -> Result<Vec<String>, ConnectorError> {
        let instance_id = self.pin.binding().descriptor().instance_id.clone();
        let namespaces = self
            .pin
            .binding()
            .metadata()
            .list_namespaces_async(ConnectorListNamespacesRequest {
                instance_id: instance_id.clone(),
                page_size: ConnectorListingPageSize::DEFAULT,
                context: self.context.clone(),
            })
            .await?;
        if namespaces
            .iter()
            .any(|namespace| namespace.instance_id != instance_id)
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "system namespace listing returned another catalog identity",
            ));
        }
        Ok(namespaces
            .into_iter()
            .map(|namespace| namespace.namespace.to_string())
            .collect())
    }
    pub(crate) async fn schema_exists(&self, schema: &str) -> Result<bool, ConnectorError> {
        self.pin
            .binding()
            .metadata()
            .namespace_exists_async(ConnectorNamespaceRequest {
                namespace: self.namespace(schema),
                context: self.context.clone(),
            })
            .await
    }
    pub(crate) async fn table_names(&self, schema: &str) -> Result<Vec<String>, ConnectorError> {
        let namespace = self.namespace(schema);
        let tables = self
            .pin
            .binding()
            .metadata()
            .list_tables_async(ConnectorListTablesRequest {
                namespace: namespace.clone(),
                page_size: ConnectorListingPageSize::DEFAULT,
                context: self.context.clone(),
            })
            .await?;
        if tables.iter().any(|table| {
            table.instance_id != namespace.instance_id || table.namespace != namespace.namespace
        }) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "system table listing returned another namespace identity",
            ));
        }
        Ok(tables
            .into_iter()
            .map(|table| table.table.to_string())
            .collect())
    }
    pub(crate) fn supports_discovery(&self) -> bool {
        self.pin.binding().metadata().supports_table_discovery()
    }

    pub(crate) fn can_probe_discovery_name(&self, name: &str) -> bool {
        let metadata = self.pin.binding().metadata();
        !metadata.supports_table_discovery() || metadata.can_probe_discovery_name(name)
    }

    /// Observe a requested object under the same pin used for discovery.
    /// A previously listed object stays in that domain if it disappears.
    pub(crate) async fn discover_relation(
        &self,
        schema: &str,
        name: &str,
        kind: Option<DiscoveryRelationKind>,
        projection: DiscoveryProjection,
        cpu: Arc<dyn ProviderMetadataCpuPort>,
    ) -> Result<Option<DiscoveredRelation>, ConnectorError> {
        self.context.original_operation_control().check_active()?;
        let identity = ConnectorTableIdentity {
            instance_id: self.pin.binding().descriptor().instance_id.clone(),
            namespace: schema.into(),
            table: name.into(),
        };
        if kind != Some(DiscoveryRelationKind::View) {
            let observed = self
                .pin
                .binding()
                .metadata()
                .discover_table_async(
                    ConnectorTableRequest {
                        table: identity.clone(),
                        resolution: ConnectorTableResolution::StrictBaseTable,
                        context: self.context.clone(),
                    },
                    projection,
                    Arc::clone(&cpu),
                )
                .await;
            match observed {
                Ok(observed) => {
                    return Self::check_discovery(
                        observed,
                        &identity,
                        DiscoveryRelationKind::Table,
                    );
                }
                Err(error) if error.kind() == ConnectorErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        if kind == Some(DiscoveryRelationKind::Table) {
            return Ok(None);
        }
        let Some(capability) = self.pin.binding().view_metadata() else {
            return Ok(None);
        };
        let observed = capability
            .discover_view_async(
                ConnectorViewRequest {
                    view: ConnectorViewIdentity {
                        instance_id: identity.instance_id.clone(),
                        namespace: identity.namespace.clone(),
                        view: identity.table.clone(),
                    },
                    context: self.context.clone(),
                },
                projection,
                cpu,
            )
            .await;
        match observed {
            Ok(observed) => Self::check_discovery(observed, &identity, DiscoveryRelationKind::View),
            Err(error)
                if error.kind() == ConnectorErrorKind::NotFound
                    || DiscoveryViewDomainUnavailable::is_reported(&error) =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    fn check_discovery(
        observed: DiscoveredRelation,
        expected: &ConnectorTableIdentity,
        kind: DiscoveryRelationKind,
    ) -> Result<Option<DiscoveredRelation>, ConnectorError> {
        if &observed.identity != expected || observed.kind != kind {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "system discovery returned another relation identity",
            ));
        }
        Ok(Some(observed))
    }

    pub(crate) async fn view_names(
        &self,
        schema: &str,
        cpu: Arc<dyn ProviderMetadataCpuPort>,
    ) -> Result<Vec<String>, ConnectorError> {
        let namespace = self.namespace(schema);
        let capability = self.pin.binding().view_metadata().ok_or_else(|| {
            ConnectorError::from_domain(
                ConnectorErrorKind::Unsupported,
                DiscoveryViewDomainUnavailable("list_views"),
            )
        })?;
        let views = capability
            .list_views_async(
                ConnectorListViewsRequest {
                    namespace: namespace.clone(),
                    page_size: ConnectorListingPageSize::DEFAULT,
                    context: self.context.clone(),
                },
                cpu,
            )
            .await?;
        if views.iter().any(|view| {
            view.instance_id != namespace.instance_id || view.namespace != namespace.namespace
        }) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "system view listing returned another namespace identity",
            ));
        }
        Ok(views
            .into_iter()
            .map(|view| view.view.to_string())
            .collect())
    }
}
