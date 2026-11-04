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

//! One exact control lease for an ordinary table/view or historical decision.

use super::{CatalogResolutionError, CatalogResolutionResult, ConnectorQueryTableMaterialization};
use crate::connector::schema_creation::FrontendSchemaLoad;
use novarocks_spi::connector::*;
use novarocks_sql::compiler::{
    CatalogRelationFact, CatalogRelationNeed, CatalogTimeTravelResolution,
};
use novarocks_sql::planning::catalog::{
    SqlTimeTravelSnapshotBindingFacts, SqlViewColumn, SqlViewDefinition,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct ConnectorQueryViewMaterialization {
    pub(crate) definition: Arc<SqlViewDefinition>,
    // Retains the actual observation generation without granting a scan token.
    pub(crate) planning_lease: ConnectorControlPlanningLease,
}

pub enum ConnectorQueryRelationMaterialization {
    Table {
        materialization: Box<ConnectorQueryTableMaterialization>,
        selection: Option<SqlTimeTravelSnapshotBindingFacts>,
    },
    View(ConnectorQueryViewMaterialization),
    SelectionRefused(Box<CatalogRelationFact>),
}

fn begin(
    controls: &dyn ConnectorControlResolver,
    context: ConnectorRequestContext,
    need: &CatalogRelationNeed,
) -> CatalogResolutionResult<(
    ConnectorControlPlanningLease,
    ConnectorTableIdentity,
    ConnectorRequestContext,
)> {
    if !matches!(
        need.target(),
        novarocks_sql::compiler::CatalogLookupTarget::Table { .. }
    ) {
        return Err(CatalogResolutionError::failed(
            "ordinary relation loader cannot answer a metadata target",
        ));
    }
    let relation = need.relation();
    let lease = crate::connector::metadata_binding_typed(controls, &relation.catalog)
        .map_err(CatalogResolutionError::from_connector)?;
    let context = crate::connector::context_for_planning_lease_typed(&lease, context)
        .map_err(CatalogResolutionError::from_connector)?;
    context
        .original_operation_control()
        .check_active()
        .map_err(CatalogResolutionError::from_connector)?;
    let identity = ConnectorTableIdentity {
        instance_id: lease.binding().descriptor().instance_id.clone(),
        namespace: relation.namespace.as_str().into(),
        table: relation.table.as_str().into(),
    };
    Ok((lease, identity, context))
}

fn project_reference(
    need: &CatalogRelationNeed,
    references: ConnectorReadReferenceFacts,
) -> CatalogResolutionResult<CatalogTimeTravelResolution> {
    let facts =
        crate::query_execution::planning::time_travel::project_iceberg_ref_metadata(&references)
            .map_err(CatalogResolutionError::failed)?;
    need.resolve_time_travel(&facts)
        .map_err(|error| CatalogResolutionError::failed(error.to_string()))
}

pub(crate) fn project_view(
    value: ConnectorViewMetadataValue,
    lease: ConnectorControlPlanningLease,
    expected: &ConnectorTableIdentity,
) -> CatalogResolutionResult<ConnectorQueryViewMaterialization> {
    match value.definition.dialect {
        ConnectorViewDialect::StarRocks => {}
    }
    match value.definition.source_format {
        None | Some(ConnectorViewSourceFormat::EffectiveUserSourceV1) => {}
    }
    if value.identity.instance_id != expected.instance_id
        || value.identity.namespace != expected.namespace
        || value.identity.view != expected.table
    {
        return Err(CatalogResolutionError::from_connector(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "view observation returned another relation identity",
        )));
    }
    // Missing catalog has provider-defined meaning only for third-party
    // metadata. The SPI has already refused missing Nova source fields.
    let default_catalog = match (
        &value.definition.default_catalog,
        value.definition.source_format,
    ) {
        (Some(catalog), _) => catalog.clone(),
        (None, None) => expected.instance_id.as_str().into(),
        (None, Some(_)) => {
            return Err(CatalogResolutionError::from_connector(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "versioned view definition is missing its saved catalog",
            )));
        }
    };
    let definition = Arc::new(SqlViewDefinition {
        identity: novarocks_types::naming::TableIdentity::new(
            expected.instance_id.as_str(),
            expected.namespace.as_ref(),
            expected.table.as_ref(),
        ),
        version: value.version,
        raw_sql: value.definition.raw_sql,
        default_catalog,
        default_database: value.definition.default_namespace,
        columns: value
            .definition
            .columns
            .iter()
            .map(|column| SqlViewColumn {
                source_field_name: column.source_field_name.clone(),
                name: column.name.clone(),
                value_type: column.value_type.clone(),
            })
            .collect(),
    });
    definition.validate().map_err(|error| {
        CatalogResolutionError::from_connector(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            error,
        ))
    })?;
    Ok(ConnectorQueryViewMaterialization {
        definition,
        planning_lease: lease,
    })
}

fn no_view_capability() -> CatalogResolutionError {
    CatalogResolutionError::from_connector(ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        "connector generation does not declare view metadata capability",
    ))
}

fn finish_table(
    metadata: ConnectorTableMetadata,
    lease: ConnectorControlPlanningLease,
    selection: Option<SqlTimeTravelSnapshotBindingFacts>,
) -> CatalogResolutionResult<ConnectorQueryRelationMaterialization> {
    let resolution = selection
        .as_ref()
        .map_or(ConnectorTableResolution::StrictBaseTable, |binding| {
            ConnectorTableResolution::HistoricalSnapshot(binding.snapshot_id())
        });
    let materialization =
        super::connector_table_materialization_for_resolution(metadata, lease, resolution)
            .map_err(CatalogResolutionError::failed)?;
    Ok(ConnectorQueryRelationMaterialization::Table {
        materialization: Box::new(materialization),
        selection,
    })
}

pub(crate) fn load_relation(
    controls: &dyn ConnectorControlResolver,
    context: ConnectorRequestContext,
    need: &CatalogRelationNeed,
) -> CatalogResolutionResult<ConnectorQueryRelationMaterialization> {
    let (lease, identity, context) = begin(controls, context, need)?;
    let selection = if need.version().is_some() {
        let references = lease
            .binding()
            .metadata()
            .read_reference_facts(ConnectorReadReferenceFactsRequest {
                table: identity.clone(),
                context: context.clone(),
            })
            .map_err(|error| {
                super::connector_table_resolution_error(error, &identity.namespace, &identity.table)
            })?;
        match project_reference(need, references)? {
            CatalogTimeTravelResolution::Selected(binding) => Some(binding),
            CatalogTimeTravelResolution::Rejected(fact) => {
                return Ok(ConnectorQueryRelationMaterialization::SelectionRefused(
                    fact,
                ));
            }
        }
    } else {
        None
    };
    let resolution = selection
        .as_ref()
        .map_or(ConnectorTableResolution::StrictBaseTable, |binding| {
            ConnectorTableResolution::HistoricalSnapshot(binding.snapshot_id())
        });
    match lease
        .binding()
        .metadata()
        .load_frontend_table(ConnectorTableRequest {
            table: identity.clone(),
            resolution,
            context: context.clone(),
        }) {
        Ok(metadata) => finish_table(metadata, lease, selection),
        Err(error) if selection.is_none() && error.kind() == ConnectorErrorKind::NotFound => {
            let capability = lease
                .binding()
                .view_metadata()
                .ok_or_else(no_view_capability)?;
            let value = crate::connector::schema_creation::load_frontend_view(
                capability.as_ref(),
                ConnectorViewRequest {
                    view: ConnectorViewIdentity {
                        instance_id: identity.instance_id.clone(),
                        namespace: identity.namespace.clone(),
                        view: identity.table.clone(),
                    },
                    context,
                },
            )
            .map_err(|error| {
                super::connector_table_resolution_error(error, &identity.namespace, &identity.table)
            })?;
            project_view(value, lease, &identity).map(ConnectorQueryRelationMaterialization::View)
        }
        Err(error) => Err(super::connector_table_resolution_error(
            error,
            &identity.namespace,
            &identity.table,
        )),
    }
}

pub(crate) async fn load_relation_async(
    controls: &dyn ConnectorControlResolver,
    context: ConnectorRequestContext,
    need: &CatalogRelationNeed,
) -> CatalogResolutionResult<ConnectorQueryRelationMaterialization> {
    let (lease, identity, context) = begin(controls, context, need)?;
    let selection = if need.version().is_some() {
        let references = lease
            .binding()
            .metadata()
            .read_reference_facts_async(ConnectorReadReferenceFactsRequest {
                table: identity.clone(),
                context: context.clone(),
            })
            .await
            .map_err(|error| {
                super::connector_table_resolution_error(error, &identity.namespace, &identity.table)
            })?;
        match project_reference(need, references)? {
            CatalogTimeTravelResolution::Selected(binding) => Some(binding),
            CatalogTimeTravelResolution::Rejected(fact) => {
                return Ok(ConnectorQueryRelationMaterialization::SelectionRefused(
                    fact,
                ));
            }
        }
    } else {
        None
    };
    let resolution = selection
        .as_ref()
        .map_or(ConnectorTableResolution::StrictBaseTable, |binding| {
            ConnectorTableResolution::HistoricalSnapshot(binding.snapshot_id())
        });
    match crate::connector::schema_creation::load_frontend_table_async(
        lease.binding().metadata().as_ref(),
        ConnectorTableRequest {
            table: identity.clone(),
            resolution,
            context: context.clone(),
        },
    )
    .await
    {
        Ok(metadata) => finish_table(metadata, lease, selection),
        Err(error) if selection.is_none() && error.kind() == ConnectorErrorKind::NotFound => {
            let capability = lease
                .binding()
                .view_metadata()
                .ok_or_else(no_view_capability)?;
            let value = crate::connector::schema_creation::load_frontend_view_async(
                capability.as_ref(),
                ConnectorViewRequest {
                    view: ConnectorViewIdentity {
                        instance_id: identity.instance_id.clone(),
                        namespace: identity.namespace.clone(),
                        view: identity.table.clone(),
                    },
                    context,
                },
            )
            .await
            .map_err(|error| {
                super::connector_table_resolution_error(error, &identity.namespace, &identity.table)
            })?;
            project_view(value, lease, &identity).map(ConnectorQueryRelationMaterialization::View)
        }
        Err(error) => Err(super::connector_table_resolution_error(
            error,
            &identity.namespace,
            &identity.table,
        )),
    }
}

#[cfg(test)]
mod tests;
