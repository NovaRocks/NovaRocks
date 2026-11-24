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

//! One exact control planning pin for an ordinary table/view or historical decision.

use super::{CatalogResolutionError, CatalogResolutionResult, ConnectorQueryTableMaterialization};
use crate::connector::schema_creation::FrontendSchemaLoad;
use crate::query_execution::mv_assembly::refresh_preparation::io_gate::MvRefreshPreparationIo;
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
    pub(crate) planning_pin: ConnectorControlPlanningPin,
}

pub enum ConnectorQueryRelationMaterialization {
    Table {
        materialization: Box<ConnectorQueryTableMaterialization>,
        selection: Option<SqlTimeTravelSnapshotBindingFacts>,
    },
    View(ConnectorQueryViewMaterialization),
    SelectionRefused(Box<CatalogRelationFact>),
}

#[derive(Clone)]
enum StatementRelationObservation {
    Table(Arc<HeldTableObservation>),
    View(ConnectorQueryViewMaterialization),
}

struct HeldTableObservation {
    original: ConnectorTableMetadata,
    planning_pin: ConnectorControlPlanningPin,
    context: ConnectorRequestContext,
    complete: tokio::sync::OnceCell<ConnectorTableMetadata>,
}

#[derive(Default)]
pub(in crate::catalog_application) struct StatementRelationObservations {
    names: std::collections::HashMap<
        super::super::query_bindings::QueryTableBindingKey,
        CatalogResolutionResult<StatementRelationObservation>,
    >,
    objects: std::collections::HashMap<
        (ConnectorInstanceId, ConnectorSemanticFact),
        Arc<HeldTableObservation>,
    >,
}

impl StatementRelationObservations {
    fn remember_table(
        &mut self,
        original: ConnectorTableMetadata,
        planning_pin: ConnectorControlPlanningPin,
        context: ConnectorRequestContext,
    ) -> CatalogResolutionResult<StatementRelationObservation> {
        let control = planning_pin.binding();
        let identity = match control
            .metadata()
            .exact_semantic_revision(&original.table, ConnectorReadSelector::Current)
        {
            Ok(revision)
                if revision.object_identity().provider() == &control.descriptor().provider_id =>
            {
                Some((
                    original.identity.instance_id.clone(),
                    revision.object_identity().clone(),
                ))
            }
            Ok(_) => {
                return Err(CatalogResolutionError::failed(
                    "table observation returned a semantic identity from another provider",
                ));
            }
            Err(error) if error.kind() == ConnectorErrorKind::Unsupported => None,
            Err(error) => return Err(CatalogResolutionError::from_connector(error)),
        };
        let hold = || {
            Arc::new(HeldTableObservation {
                original,
                planning_pin,
                context,
                complete: tokio::sync::OnceCell::new(),
            })
        };
        let held = match identity {
            Some(identity) => self.objects.entry(identity).or_insert_with(hold).clone(),
            None => hold(),
        };
        Ok(StatementRelationObservation::Table(held))
    }
}

#[derive(Clone, Copy)]
pub(in crate::catalog_application) enum StatementCatalogIo<'a> {
    Async,
    Governed(&'a MvRefreshPreparationIo),
    Synchronous,
}

pub(in crate::catalog_application) async fn load_statement_relation(
    bindings: &super::super::query_bindings::QueryTableBindingStore,
    controls: &dyn ConnectorControlResolver,
    context: ConnectorRequestContext,
    need: &CatalogRelationNeed,
    complete_first: bool,
    io: StatementCatalogIo<'_>,
) -> CatalogResolutionResult<ConnectorQueryRelationMaterialization> {
    use super::super::query_bindings::QueryTableBindingKey;
    let relation = need.relation();
    let key =
        QueryTableBindingKey::strict_base(&relation.catalog, &relation.namespace, &relation.table);
    let mut state = bindings.observed_relations.lock().await;
    let observation = if let Some(observed) = state.names.get(&key) {
        observed.clone()
    } else {
        bindings
            .require_catalog_observation_allowed()
            .map_err(CatalogResolutionError::failed)?;
        let held_mutation = bindings
            .resolved_lookup(&key)
            .and_then(Result::ok)
            .map(|id| bindings.binding(id))
            .transpose()
            .map_err(CatalogResolutionError::failed)?
            .and_then(|binding| {
                let original = binding.source_metadata.clone()?;
                let pin = binding.admission.exact_planning_pin().ok()?.clone();
                Some((original, pin))
            });
        let observed = match held_mutation {
            Some((original, pin)) => {
                let context = crate::connector::context_for_planning_pin_typed(&pin, context)
                    .map_err(CatalogResolutionError::from_connector)?;
                state.remember_table(original, pin, context)
            }
            None => {
                capture_statement_relation(
                    &mut state,
                    controls,
                    context.clone(),
                    need,
                    complete_first,
                    io,
                )
                .await
            }
        };
        if observed
            .as_ref()
            .err()
            .is_none_or(CatalogResolutionError::is_object_outcome)
        {
            state.names.insert(key, observed.clone());
        }
        observed
    }?;
    match observation {
        StatementRelationObservation::Table(held) => held.project(bindings, need, io).await,
        StatementRelationObservation::View(view) if need.version().is_none() => {
            Ok(ConnectorQueryRelationMaterialization::View(view))
        }
        StatementRelationObservation::View(_) => {
            Err(CatalogResolutionError::from_connector(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "A view cannot be read at a historical table snapshot",
            )))
        }
    }
}

async fn capture_statement_relation(
    state: &mut StatementRelationObservations,
    controls: &dyn ConnectorControlResolver,
    context: ConnectorRequestContext,
    need: &CatalogRelationNeed,
    complete_first: bool,
    io: StatementCatalogIo<'_>,
) -> CatalogResolutionResult<StatementRelationObservation> {
    use crate::connector::schema_creation as schema;
    let (planning_pin, identity, context) = begin(controls, context, need)?;
    let metadata = planning_pin.binding().metadata();
    let request = ConnectorTableRequest {
        table: identity.clone(),
        resolution: ConnectorTableResolution::StrictBaseTable,
        context: context.clone(),
    };
    let loaded = match (io, complete_first) {
        (StatementCatalogIo::Governed(io), true) => {
            io.source_table_metadata(
                &planning_pin,
                context.clone(),
                &identity.namespace,
                &identity.table,
            )
            .await
        }
        (StatementCatalogIo::Governed(io), false) => {
            io.metadata(
                &planning_pin,
                context.clone(),
                &identity.namespace,
                &identity.table,
                ConnectorTableResolution::StrictBaseTable,
            )
            .await
        }
        (StatementCatalogIo::Async, true) => {
            schema::load_frontend_source_table_async(metadata.as_ref(), request).await
        }
        (StatementCatalogIo::Async, false) => {
            schema::load_frontend_table_async(metadata.as_ref(), request).await
        }
        (StatementCatalogIo::Synchronous, true) => metadata
            .prepare_complete_table_observation_sync(request, None)
            .and_then(|prepared| create_prepared_table(metadata.as_ref(), prepared, &context)),
        (StatementCatalogIo::Synchronous, false) => metadata.load_frontend_table(request),
    };
    match loaded {
        Ok(original) if original.identity == identity => {
            state.remember_table(original, planning_pin, context)
        }
        Ok(_) => Err(CatalogResolutionError::from_connector(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "Original table observation returned another name",
        ))),
        Err(error) if error.kind() == ConnectorErrorKind::NotFound => {
            let capability = planning_pin
                .binding()
                .view_metadata()
                .ok_or_else(no_view_capability)?;
            let value = match io {
                StatementCatalogIo::Governed(io) => {
                    io.view_metadata(&planning_pin, identity.clone(), context)
                        .await
                }
                direct => {
                    let request = ConnectorViewRequest {
                        view: ConnectorViewIdentity {
                            instance_id: identity.instance_id.clone(),
                            namespace: identity.namespace.clone(),
                            view: identity.table.clone(),
                        },
                        context,
                    };
                    match direct {
                        StatementCatalogIo::Synchronous => {
                            schema::load_frontend_view(capability.as_ref(), request)
                        }
                        _ => schema::load_frontend_view_async(capability.as_ref(), request).await,
                    }
                }
            }
            .map_err(|error| {
                super::connector_table_resolution_error(error, &identity.namespace, &identity.table)
            })?;
            project_view(value, planning_pin, &identity).map(StatementRelationObservation::View)
        }
        Err(error) => Err(super::connector_table_resolution_error(
            error,
            &identity.namespace,
            &identity.table,
        )),
    }
}

impl HeldTableObservation {
    async fn project(
        &self,
        bindings: &super::super::query_bindings::QueryTableBindingStore,
        need: &CatalogRelationNeed,
        io: StatementCatalogIo<'_>,
    ) -> CatalogResolutionResult<ConnectorQueryRelationMaterialization> {
        use crate::connector::schema_creation as schema;
        let identity = &self.original.identity;
        if need.version().is_none() {
            return finish_table(
                self.original.clone(),
                self.planning_pin.clone(),
                None,
                identity,
            );
        }
        let context = &self.context;
        let metadata = self.planning_pin.binding().metadata();
        let mut observed = self.complete.get().unwrap_or(&self.original);
        let (references, complete) = metadata
            .observed_read_references(observed, context)
            .map_err(CatalogResolutionError::from_connector)?;
        let mut selection = project_reference(need, &references)?;
        let is_timestamp = need.version().is_some_and(|period| {
            period.range_type == novarocks_parser::ast::QueryPeriodRangeType::Timestamp
        });
        let covered = matches!(&selection, CatalogTimeTravelResolution::Selected(binding)
            if references.snapshot_ids().binary_search(&binding.snapshot_id()).is_ok())
            && !is_timestamp;
        if !complete && !covered {
            observed = self
                .complete
                .get_or_try_init(|| async {
                    bindings
                        .require_catalog_observation_allowed()
                        .map_err(CatalogResolutionError::failed)?;
                    let prepared = match io {
                        StatementCatalogIo::Governed(io) => {
                            io.complete_table_observation(
                                &self.planning_pin,
                                &self.original,
                                context.clone(),
                            )
                            .await
                        }
                        StatementCatalogIo::Async => {
                            metadata
                                .complete_table_observation(&self.original, context.clone())
                                .await
                        }
                        StatementCatalogIo::Synchronous => metadata
                            .prepare_complete_table_observation_sync(
                                ConnectorTableRequest {
                                    table: self.original.identity.clone(),
                                    context: context.clone(),
                                    resolution: ConnectorTableResolution::StrictBaseTable,
                                },
                                Some(&self.original),
                            ),
                    }
                    .map_err(CatalogResolutionError::from_connector)?;
                    create_prepared_table(metadata.as_ref(), prepared, context)
                        .map_err(CatalogResolutionError::from_connector)
                })
                .await?;
            let (references, _) = metadata
                .observed_read_references(observed, context)
                .map_err(CatalogResolutionError::from_connector)?;
            selection = project_reference(need, &references)?;
        }
        let selection = match selection {
            CatalogTimeTravelResolution::Selected(selection) => selection,
            CatalogTimeTravelResolution::Rejected(fact) => {
                return Ok(ConnectorQueryRelationMaterialization::SelectionRefused(
                    fact,
                ));
            }
        };
        let projected = schema::with_schema_creation(context, |creation| {
            metadata.create_observed_snapshot(observed, selection.snapshot_id(), context, creation)
        })
        .map_err(schema::connector_schema_error)
        .map_err(CatalogResolutionError::from_connector)?;
        finish_table(
            projected,
            self.planning_pin.clone(),
            Some(selection),
            identity,
        )
    }
}

fn create_prepared_table(
    metadata: &dyn ConnectorMetadata,
    prepared: ConnectorTablePreparation,
    context: &ConnectorRequestContext,
) -> Result<ConnectorTableMetadata, ConnectorError> {
    use crate::connector::schema_creation as schema;
    schema::with_schema_creation(context, |creation| {
        metadata.create_prepared_table(prepared, creation)
    })
    .map_err(schema::connector_schema_error)
}

pub(crate) fn begin(
    controls: &dyn ConnectorControlResolver,
    context: ConnectorRequestContext,
    need: &CatalogRelationNeed,
) -> CatalogResolutionResult<(
    ConnectorControlPlanningPin,
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
    let planning_pin = crate::connector::metadata_binding_typed(controls, &relation.catalog)
        .map_err(CatalogResolutionError::from_connector)?;
    let context = crate::connector::context_for_planning_pin_typed(&planning_pin, context)
        .map_err(CatalogResolutionError::from_connector)?;
    context
        .original_operation_control()
        .check_active()
        .map_err(CatalogResolutionError::from_connector)?;
    let identity = ConnectorTableIdentity {
        instance_id: planning_pin.binding().descriptor().instance_id.clone(),
        namespace: relation.namespace.as_str().into(),
        table: relation.table.as_str().into(),
    };
    Ok((planning_pin, identity, context))
}

pub(crate) fn project_reference(
    need: &CatalogRelationNeed,
    references: &ConnectorReadReferenceFacts,
) -> CatalogResolutionResult<CatalogTimeTravelResolution> {
    let facts =
        crate::query_execution::planning::time_travel::project_iceberg_ref_metadata(references)
            .map_err(CatalogResolutionError::failed)?;
    need.resolve_time_travel(&facts)
        .map_err(|error| CatalogResolutionError::failed(error.to_string()))
}

pub(crate) fn project_view(
    value: ConnectorViewMetadataValue,
    planning_pin: ConnectorControlPlanningPin,
    expected: &ConnectorTableIdentity,
) -> CatalogResolutionResult<ConnectorQueryViewMaterialization> {
    match value.definition.dialect {
        ConnectorViewDialect::Trino => {}
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
    // A standard view without a default catalog resolves within its storing catalog.
    let default_catalog = value
        .definition
        .default_catalog
        .clone()
        .unwrap_or_else(|| expected.instance_id.as_str().into());
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
        planning_pin,
    })
}

pub(crate) fn no_view_capability() -> CatalogResolutionError {
    CatalogResolutionError::from_connector(ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        "connector generation does not declare view metadata capability",
    ))
}

pub(crate) fn finish_table(
    metadata: ConnectorTableMetadata,
    planning_pin: ConnectorControlPlanningPin,
    selection: Option<SqlTimeTravelSnapshotBindingFacts>,
    expected: &ConnectorTableIdentity,
) -> CatalogResolutionResult<ConnectorQueryRelationMaterialization> {
    if &metadata.identity != expected {
        return Err(CatalogResolutionError::from_connector(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "table observation returned another relation identity",
        )));
    }
    let resolution = selection
        .as_ref()
        .map_or(ConnectorTableResolution::StrictBaseTable, |binding| {
            ConnectorTableResolution::HistoricalSnapshot(binding.snapshot_id())
        });
    let materialization =
        super::connector_table_materialization_for_resolution(metadata, planning_pin, resolution)
            .map_err(CatalogResolutionError::failed)?;
    Ok(ConnectorQueryRelationMaterialization::Table {
        materialization: Box::new(materialization),
        selection,
    })
}

#[cfg(test)]
fn load_relation(
    controls: &dyn ConnectorControlResolver,
    context: ConnectorRequestContext,
    need: &CatalogRelationNeed,
) -> CatalogResolutionResult<ConnectorQueryRelationMaterialization> {
    let bindings = super::super::query_bindings::QueryTableBindingStore::try_new()
        .map_err(CatalogResolutionError::failed)?;
    futures::executor::block_on(load_statement_relation(
        &bindings,
        controls,
        context,
        need,
        need.version().is_some(),
        StatementCatalogIo::Synchronous,
    ))
}

#[cfg(test)]
async fn load_relation_async(
    controls: &dyn ConnectorControlResolver,
    context: ConnectorRequestContext,
    need: &CatalogRelationNeed,
) -> CatalogResolutionResult<ConnectorQueryRelationMaterialization> {
    let bindings = super::super::query_bindings::QueryTableBindingStore::try_new()
        .map_err(CatalogResolutionError::failed)?;
    load_statement_relation(
        &bindings,
        controls,
        context,
        need,
        need.version().is_some(),
        StatementCatalogIo::Async,
    )
    .await
}

#[cfg(test)]
mod tests;
