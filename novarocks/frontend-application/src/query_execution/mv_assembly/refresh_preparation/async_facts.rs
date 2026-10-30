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

use super::io_gate::{MvRefreshPreparationIo, provider_error};
use crate::mv::domain::application::{MvApplicationError, MvApplicationErrorKind};
use crate::mv::domain::iceberg_refresh::IcebergMvCorePorts;
use crate::mv::domain::refresh::target::IcebergMvTarget;
use crate::mv::domain::refresh::target_binding::MvTargetBinding;
use crate::mv::domain::storage_observation::{
    MvRefreshBaseObservation, MvSchemaValidationObservation, MvSourceSchemaObservation,
};
use novarocks_spi::connector::*;
use novarocks_types::naming::TableIdentity;

/// Pure/domain failures keep their original category. Provider and repository
/// failures retain the actual typed cause through the same preparation result.
#[derive(Debug)]
pub(crate) struct PreparationError(pub(crate) MvApplicationError);
impl PreparationError {
    pub(crate) fn with_context(self, context: impl std::fmt::Display) -> Self {
        let mut error =
            MvApplicationError::new(self.0.kind(), format!("{context}: {}", self.0.message()))
                .with_compile_control(self.0.compile_control_error())
                .with_unavailable_implementation(self.0.unavailable_implementation().cloned());
        if let Some(cause) = self.0.original_failure() {
            error = error.with_original_failure(cause.clone());
        }
        Self(error)
    }
    pub(crate) fn user(message: impl Into<String>) -> Self {
        Self(MvApplicationError::new(
            MvApplicationErrorKind::InvalidRequest,
            message,
        ))
    }
}
impl std::fmt::Display for PreparationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for PreparationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl From<String> for PreparationError {
    fn from(e: String) -> Self {
        Self::user(e)
    }
}
impl From<&str> for PreparationError {
    fn from(e: &str) -> Self {
        Self::user(e)
    }
}
impl From<MvApplicationError> for PreparationError {
    fn from(e: MvApplicationError) -> Self {
        Self(e)
    }
}
impl From<ConnectorError> for PreparationError {
    fn from(e: ConnectorError) -> Self {
        Self(provider_error(e))
    }
}
impl From<novarocks_sql::compiler::SqlCompileError> for PreparationError {
    fn from(e: novarocks_sql::compiler::SqlCompileError) -> Self {
        Self(MvApplicationError::from_compile(e))
    }
}
impl From<crate::mv::domain::lifecycle::RefreshError> for PreparationError {
    fn from(e: crate::mv::domain::lifecycle::RefreshError) -> Self {
        let result = MvApplicationError::new(
            match e.kind {
                crate::mv::domain::lifecycle::RefreshErrorKind::UserError => {
                    MvApplicationErrorKind::InvalidRequest
                }
            },
            e.message.clone(),
        )
        .with_compile_control(e.compile_control_error())
        .with_unavailable_implementation(e.unavailable_implementation().cloned());
        Self(result)
    }
}

/// Missing planned facts and changed object identity are definition failures.
/// Only a newer snapshot of the same exact source invalidates preparation.
pub(crate) fn require_source_snapshot(
    planned: Option<i64>,
    what: &str,
    named: &str,
) -> Result<i64, PreparationError> {
    planned
        .ok_or_else(|| PreparationError::user(format!("{what} has no pinned snapshot for {named}")))
}

pub(crate) fn validate_source_snapshot(
    occurrence: &novarocks_mv_application::persistence::codec::RelationOccurrence,
    observed_object: &novarocks_mv_application::persistence::identity::ObjectIdentity,
    planned_snapshot: i64,
    observed_snapshot: Option<i64>,
    what: &str,
    named: &str,
) -> Result<(), PreparationError> {
    if observed_object != &occurrence.object_id {
        return Err(PreparationError::user(format!(
            "{what} source object changed for D occurrence {}",
            occurrence.occurrence_id,
        )));
    }
    if observed_snapshot != Some(planned_snapshot) {
        return Err(PreparationError(MvApplicationError::new(
            MvApplicationErrorKind::BindingInvalidated,
            format!("{what} source snapshot changed after planning for {named}"),
        )));
    }
    Ok(())
}

impl MvRefreshPreparationIo {
    pub(crate) async fn table_metadata(
        &self,
        controls: &dyn ConnectorControlResolver,
        table: &TableIdentity,
        context: &ConnectorRequestContext,
    ) -> Result<(ConnectorControlPlanningLease, ConnectorTableMetadata), ConnectorError> {
        let lease = crate::connector::metadata_binding_typed(controls, &table.catalog)?;
        let metadata = self
            .metadata(
                &lease,
                context.clone(),
                &table.namespace,
                &table.table,
                ConnectorTableResolution::StrictBaseTable,
            )
            .await?;
        Ok((lease, metadata))
    }
    pub(crate) async fn target_binding_with_lease(
        &self,
        storage: &dyn MvStorageObservationPort,
        table: &TableIdentity,
        lease: ConnectorControlPlanningLease,
        context: &ConnectorRequestContext,
    ) -> Result<MvTargetBinding, ConnectorError> {
        crate::mv::domain::refresh::target_binding::validate_target_lease(table, &lease)?;
        let metadata = self
            .metadata(
                &lease,
                context.clone(),
                &table.namespace,
                &table.table,
                ConnectorTableResolution::StrictBaseTable,
            )
            .await?;
        crate::mv::domain::refresh::target_binding::complete_target_binding(
            storage, lease, metadata, context,
        )
    }
    pub(crate) async fn target_binding(
        &self,
        source: &IcebergMvCorePorts,
        target: &IcebergMvTarget,
        context: &ConnectorRequestContext,
    ) -> Result<MvTargetBinding, ConnectorError> {
        let table = TableIdentity {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
        };
        let lease =
            crate::connector::metadata_binding_typed(source.connector_control(), &table.catalog)?;
        self.target_binding_with_lease(source.storage_observation(), &table, lease, context)
            .await
    }
    pub(crate) async fn schema(
        &self,
        source: &IcebergMvCorePorts,
        table: &TableIdentity,
        context: &ConnectorRequestContext,
    ) -> Result<MvSourceSchemaObservation, PreparationError> {
        context.original_operation_control().check_active()?;
        let captured = self.captured_sources().require(table)?;
        let (lease, metadata) = if captured.snapshot.is_some() {
            (captured.lease.clone(), captured.metadata()?)
        } else {
            (captured.lease.clone(), captured.observed.clone())
        };
        let observed = crate::mv::domain::storage_observation::observe_source_schema(
            source.storage_observation(),
            &lease,
            &metadata,
            context.clone(),
        )?;
        crate::mv::domain::refresh::observation::validate_observed_schema_locator(
            observed.table(),
            &metadata.identity,
        )?;
        Ok(observed)
    }
    /// Definition admission uses the Current schema from this Root's initial
    /// observation. Reading uses `schema`, which projects the prepared To.
    pub(crate) fn definition_schema(
        &self,
        source: &IcebergMvCorePorts,
        table: &TableIdentity,
        context: &ConnectorRequestContext,
    ) -> Result<MvSourceSchemaObservation, PreparationError> {
        context.original_operation_control().check_active()?;
        let captured = self.captured_sources().require(table)?;
        let observed = crate::mv::domain::storage_observation::observe_source_schema(
            source.storage_observation(),
            &captured.lease,
            &captured.observed,
            context.clone(),
        )?;
        crate::mv::domain::refresh::observation::validate_observed_schema_locator(
            observed.table(),
            &captured.observed.identity,
        )?;
        Ok(observed)
    }
    pub(crate) async fn target_schema(
        &self,
        source: &IcebergMvCorePorts,
        table: &TableIdentity,
        context: &ConnectorRequestContext,
    ) -> Result<MvSchemaValidationObservation, PreparationError> {
        let (lease, metadata) = self
            .table_metadata(source.connector_control(), table, context)
            .await?;
        let observed = crate::mv::domain::storage_observation::observe_schema_validation(
            source.storage_observation(),
            &lease,
            &metadata,
            context.clone(),
        )?;
        crate::mv::domain::refresh::observation::validate_observed_schema_locator(
            observed.table(),
            &metadata.identity,
        )?;
        Ok(observed)
    }
    pub(crate) async fn current_base(
        &self,
        _source: &IcebergMvCorePorts,
        table: &TableIdentity,
        context: &ConnectorRequestContext,
    ) -> Result<MvRefreshBaseObservation, ConnectorError> {
        context.original_operation_control().check_active()?;
        let captured = self.captured_sources().require(table).map_err(|error| {
            ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
        })?;
        Ok(captured.observation.clone())
    }
    pub(crate) async fn current_revision(
        &self,
        _source: &IcebergMvCorePorts,
        table: &TableIdentity,
        context: &ConnectorRequestContext,
    ) -> Result<(MvRefreshBaseObservation, ConnectorExactSemanticRevision), ConnectorError> {
        context.original_operation_control().check_active()?;
        let captured = self.captured_sources().require(table).map_err(|error| {
            ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
        })?;
        Ok((captured.observation.clone(), captured.revision.clone()))
    }
    pub(crate) async fn snapshot_from_revision(
        &self,
        _controls: &dyn ConnectorControlResolver,
        table: &TableIdentity,
        revision: &ConnectorExactSemanticRevision,
        context: &ConnectorRequestContext,
    ) -> Result<i64, ConnectorError> {
        context.original_operation_control().check_active()?;
        let captured = self.captured_sources().require(table).map_err(|error| {
            ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
        })?;
        if revision.object_identity() != captured.revision.object_identity() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "MV historical revision names another captured object",
            ));
        }
        // This is the persisted canonical locator, not an endpoint-presence
        // proof. A missing historical From belongs to typed window admission.
        match revision.canonical_read_point() {
            Some(ConnectorCanonicalReadPoint::Snapshot(Some(id))) => Ok(id),
            _ => Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "MV exact revision has no canonical snapshot read point",
            )),
        }
    }
    /// Pin each object once before source schema or query analysis can observe it.
    /// Only completed facts enter the stock; physical CPU/IO custody stays in this gate.
    pub(crate) async fn capture_refresh_sources(
        &self,
        source: &IcebergMvCorePorts,
        projection: &novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection,
        baseline: &crate::mv::domain::refresh::planning::RefreshStateBaseline,
        full: bool,
        context: &ConnectorRequestContext,
    ) -> Result<(), PreparationError> {
        use super::captured_sources::CapturedRefreshSource;
        use crate::mv::domain::refresh::planning::RefreshStateBaseline;
        use novarocks_spi::connector::read_stack::{
            ConnectorReadChangeWindow, ConnectorReadSourcePreparationRequest, SchemaTableName,
        };
        let RefreshStateBaseline::SnapshotBacked {
            previous_sources, ..
        } = baseline;
        for relation in &projection.facts.definition().relation_occurrences {
            context.original_operation_control().check_active()?;
            let table = TableIdentity {
                catalog: relation.catalog_at_binding.clone(),
                namespace: relation.namespace_at_binding.clone(),
                table: relation.relation_at_binding.clone(),
            };
            let inventory = self.captured_sources();
            let captured = match inventory
                .by_table(&table)
                .or(inventory.by_definition_object(&relation.object_id, &table.catalog)?)
            {
                Some(captured) => {
                    inventory.bind_alias(table.clone(), captured.clone())?;
                    captured
                }
                None => {
                    let lease = crate::connector::metadata_binding_typed(
                        source.connector_control(),
                        &table.catalog,
                    )?;
                    let metadata = self
                        .source_table_metadata(
                            &lease,
                            context.clone(),
                            &table.namespace,
                            &table.table,
                        )
                        .await?;
                    let observed = crate::mv::domain::refresh_io::observe_loaded_refresh_base(
                        source.storage_observation(),
                        &lease,
                        &metadata,
                        &table,
                        context,
                    )?;
                    let revision = lease
                        .binding()
                        .metadata()
                        .exact_semantic_revision(&metadata.table, ConnectorReadSelector::Current)?;
                    if let Some(snapshot) = observed.current_snapshot_id() {
                        let selector = lease
                            .binding()
                            .metadata()
                            .read_selector_from_exact_revision(&metadata.table, &revision)?;
                        if selector != ConnectorReadSelector::SnapshotId(snapshot) {
                            return Err(ConnectorError::new(
                                ConnectorErrorKind::CorruptData,
                                "MV initial source observation and exact revision disagree",
                            )
                            .into());
                        }
                    }
                    if revision.object_identity().value().as_ref()
                        != observed.object_id().as_bytes().as_ref()
                    {
                        return Err(ConnectorError::new(
                            ConnectorErrorKind::CorruptData,
                            "MV initial source observation and exact revision disagree",
                        )
                        .into());
                    }
                    let captured = CapturedRefreshSource::new(
                        table.clone(),
                        lease,
                        metadata,
                        observed.clone(),
                        observed.current_snapshot_id(),
                        revision,
                    );
                    inventory.install(captured)?
                }
            };
            let definition_schema = self.definition_schema(source, &table, context)?;
            crate::mv::domain::refresh::schema_contract::validate_relation_occurrence_schema(
                relation,
                &definition_schema,
            )?;
            let Some(to) = captured.snapshot else {
                continue;
            };
            let previous = if full {
                None
            } else {
                previous_sources
                    .iter()
                    .find(|v| v.occurrence_id.get() == relation.occurrence_id)
            };
            let from = previous.map(|v| &v.semantic_revision);
            if captured.receipt(from).is_some() {
                continue;
            }
            let observed_facts =
                captured
                    .observed
                    .source_read_facts()
                    .cloned()
                    .ok_or_else(|| {
                        ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            "MV initial source observation has no frozen Provider facts",
                        )
                    })?;
            let name = SchemaTableName::try_new(
                captured.table.namespace.clone(),
                captured.table.table.clone(),
            )?;
            let request = match previous {
                Some(previous) => {
                    let from_snapshot =
                        match previous.semantic_revision.canonical_read_point() {
                            Some(ConnectorCanonicalReadPoint::Snapshot(Some(value))) => value,
                            _ => return Err(ConnectorError::new(
                                ConnectorErrorKind::Unsupported,
                                "MV previous exact revision has no canonical snapshot read point",
                            )
                            .into()),
                        };
                    ConnectorReadSourcePreparationRequest::change_window(
                        name,
                        captured.observed.table.clone(),
                        observed_facts,
                        captured.object.clone(),
                        ConnectorReadChangeWindow::new(from_snapshot, to),
                        previous.semantic_revision.clone(),
                        captured.revision.clone(),
                    )
                }
                None => ConnectorReadSourcePreparationRequest::snapshot(
                    name,
                    captured.observed.table.clone(),
                    observed_facts,
                    captured.object.clone(),
                    to,
                    captured.revision.clone(),
                ),
            };
            let receipt = self
                .source_read(source, &captured.lease, request, context.clone())
                .await?;
            captured.install(receipt)?;
        }
        Ok(())
    }

    pub(crate) async fn rebind_definition(
        &self,
        source: &IcebergMvCorePorts,
        projection:&novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection,
        refs: &[TableIdentity],
        target: &IcebergMvTarget,
        retained: Option<&MvSchemaValidationObservation>,
        context: &ConnectorRequestContext,
    ) -> Result<
        (
            novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection,
            String,
        ),
        PreparationError,
    > {
        use crate::mv::domain::refresh::{observation, schema_contract};
        let loaded;
        let target_observation = match retained {
            Some(v) => v,
            None => {
                loaded = self
                    .target_schema(
                        source,
                        &TableIdentity {
                            catalog: target.catalog.clone(),
                            namespace: target.namespace.clone(),
                            table: target.table.clone(),
                        },
                        context,
                    )
                    .await?;
                &loaded
            }
        };
        schema_contract::validate_projection_target(projection, target_observation)?;
        let occurrences = &projection.facts.definition().relation_occurrences;
        if occurrences.len() != refs.len() {
            return Err(PreparationError::user(
                "MV refresh source references do not retain every D occurrence",
            ));
        }
        let mut renames = Vec::new();
        for (occurrence, table) in occurrences.iter().zip(refs) {
            observation::validate_occurrence_locator(occurrence, table)?;
            let observed = self.definition_schema(source, table, context)?;
            renames.extend(schema_contract::validate_relation_occurrence_schema(
                occurrence, &observed,
            )?);
        }
        observation::require_occurrence_rewriter(&renames)?;
        Ok((
            projection.clone(),
            projection.facts.definition().query.effective_sql.clone(),
        ))
    }
    pub(crate) async fn capture_pin(
        &self,
        source: &IcebergMvCorePorts,
        projection:&novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection,
        context: &ConnectorRequestContext,
    ) -> Result<crate::mv::domain::refresh::pin::RefreshSnapshotPin, PreparationError> {
        let mut occurrences =
            Vec::with_capacity(projection.facts.definition().relation_occurrences.len());
        for relation in &projection.facts.definition().relation_occurrences {
            let table = TableIdentity {
                catalog: relation.catalog_at_binding.clone(),
                namespace: relation.namespace_at_binding.clone(),
                table: relation.relation_at_binding.clone(),
            };
            let (observed, revision) = self.current_revision(source, &table, context).await?;
            occurrences.push(
                crate::mv::domain::refresh_pin_adapter::finish_pin_occurrence(
                    relation.occurrence_id,
                    &relation.object_id,
                    table,
                    observed,
                    revision,
                )?,
            );
        }
        crate::mv::domain::refresh::pin::RefreshSnapshotPin::try_from_occurrences(occurrences)
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod source_snapshot_tests {
    use super::{require_source_snapshot, validate_source_snapshot};
    use crate::mv::domain::application::MvApplicationErrorKind;
    use novarocks_mv_application::persistence::identity::ObjectIdentity;
    use novarocks_mv_application::product::MvTarget;

    #[test]
    fn source_snapshot_change_invalidates_only_the_same_object_preparation() {
        let projection = crate::mv::domain::test_admission::sample_keyed_projection(
            MvTarget::from_parts(Some("ice"), "sales", "mv"),
            Some(1),
        );
        let occurrence = &projection.definition().relation_occurrences[0];
        for what in ["MV first-refresh", "MV incremental refresh"] {
            let snapshot = require_source_snapshot(Some(10), what, "ice.sales.orders")
                .expect("original planned snapshot");
            validate_source_snapshot(
                occurrence,
                &occurrence.object_id,
                snapshot,
                Some(10),
                what,
                "ice.sales.orders",
            )
            .expect("the same original source revision remains valid");
            let changed = validate_source_snapshot(
                occurrence,
                &occurrence.object_id,
                snapshot,
                Some(11),
                what,
                "ice.sales.orders",
            )
            .expect_err("a concurrent snapshot invalidates this preparation");
            assert_eq!(changed.0.kind(), MvApplicationErrorKind::BindingInvalidated);
            assert!(changed.0.compile_control_error().is_none());
            assert!(changed.0.original_failure().is_none());
            let missing = require_source_snapshot(None, what, "ice.sales.orders")
                .expect_err("a missing original pin is not a concurrent snapshot change");
            assert_eq!(missing.0.kind(), MvApplicationErrorKind::InvalidRequest);
            let replacement = ObjectIdentity::try_new(vec![9, 8, 7]).expect("replacement identity");
            for current in [Some(10), Some(11)] {
                let replaced = validate_source_snapshot(
                    occurrence,
                    &replacement,
                    snapshot,
                    current,
                    what,
                    "ice.sales.orders",
                )
                .expect_err("object replacement stays refused even when its snapshot differs");
                assert_eq!(replaced.0.kind(), MvApplicationErrorKind::InvalidRequest);
            }
        }
    }
}
