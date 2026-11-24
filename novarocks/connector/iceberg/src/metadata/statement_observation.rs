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

use std::sync::Arc;

use novarocks_connector_contract::PureProviderCompileError;
use novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorProviderBindingKey, ConnectorReadReferenceFacts,
    ConnectorRequestContext, ConnectorTableMetadata, ConnectorTablePreparation,
    ConnectorTableRequest, ConnectorTableResolution,
};

use super::{
    IcebergMetadata, IcebergPreparedTableFacts, TableMetadataProjection,
    read_reference_facts_with_coverage,
};
use crate::catalog::{CatalogTableName, SnapshotLoading};
use crate::catalog_control::statistics::scoped_cpu::scoped_prepared_source::IcebergObservedSourceReadFacts;
use crate::loaded_table::IcebergAttemptTableAccess;

impl IcebergMetadata {
    fn statement_observation<'a>(
        &self,
        original: &'a ConnectorTableMetadata,
        context: &ConnectorRequestContext,
    ) -> Result<&'a IcebergObservedSourceReadFacts, ConnectorError> {
        self.validate_context(context)?;
        self.ensure_owner(&original.identity.instance_id)?;
        let facts = original.source_read_facts().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg table is missing its original source observation",
            )
        })?;
        facts.validate_owner(&ConnectorProviderBindingKey {
            instance_id: self.descriptor.instance_id.clone(),
            incarnation: self.incarnation,
        })?;
        let observation =
            facts.value::<IcebergObservedSourceReadFacts>(self.descriptor.instance_id.as_str())?;
        if !matches!(observation.projection, TableMetadataProjection::Current)
            || observation.access.identifier().namespace().to_url_string()
                != original.identity.namespace.as_ref()
            || observation.access.identifier().name() != original.identity.table.as_ref()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg statement observation requires the original current base table",
            ));
        }
        Ok(observation)
    }

    pub(super) fn observation_references(
        &self,
        original: &ConnectorTableMetadata,
        context: &ConnectorRequestContext,
    ) -> Result<(ConnectorReadReferenceFacts, bool), ConnectorError> {
        let observation = self.statement_observation(original, context)?;
        let complete = observation.snapshots == SnapshotLoading::All;
        Ok((
            read_reference_facts_with_coverage(observation.access.metadata(), context, complete)?,
            complete,
        ))
    }

    pub(super) async fn complete_statement_observation(
        &self,
        original: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorTablePreparation, ConnectorError> {
        let observation = self.statement_observation(original, &context)?;
        let access = &observation.access;
        let complete = if observation.snapshots == SnapshotLoading::All {
            (**access).clone()
        } else {
            let original_location = access
                .metadata_location()
                .filter(|location| !location.is_empty())
                .ok_or_else(|| {
                    frozen_observation_unavailable("the held metadata location is missing")
                })?;
            let resources = self.runtime.resources();
            let binding = resources.planning_binding().for_request(context.clone());
            let loaded = self
                .runtime
                .novarocks_catalog()
                .load_table_for_read_with_snapshots(
                    CatalogTableName::new(
                        original.identity.namespace.as_ref(),
                        original.identity.table.as_ref(),
                    ),
                    binding,
                    SnapshotLoading::All,
                )
                .await
                .map_err(|error| {
                    if error.kind() == ConnectorErrorKind::NotFound {
                        frozen_observation_unavailable("the table no longer resolves by name")
                    } else {
                        error
                    }
                })?;
            if loaded.metadata().uuid() != access.metadata().uuid() {
                return Err(frozen_observation_unavailable(
                    "the name resolves to another table UUID",
                ));
            }
            match loaded
                .metadata_location()
                .filter(|location| !location.is_empty())
            {
                Some(location) if location == original_location => {}
                Some(_) => {
                    return Err(frozen_observation_unavailable(
                        "the metadata location advanced",
                    ));
                }
                None => {
                    return Err(frozen_observation_unavailable(
                        "the supplemental metadata location is missing",
                    ));
                }
            }

            // Only the additional metadata survives this load. Keep the
            // original renewal provenance even if the response vends access.
            (**access)
                .clone()
                .with_complete_metadata(Arc::new(loaded.metadata().clone()))
        };
        let request = ConnectorTableRequest {
            table: original.identity.clone(),
            resolution: ConnectorTableResolution::StrictBaseTable,
            context,
        };
        let facts = self.statement_table_facts(request, &complete, SnapshotLoading::All)?;
        Ok(ConnectorTablePreparation::new(
            self.descriptor.instance_id.clone(),
            facts,
        ))
    }

    pub(super) fn create_statement_snapshot(
        &self,
        original: &ConnectorTableMetadata,
        snapshot_id: i64,
        context: &ConnectorRequestContext,
        creation: &mut ConnectorReadSchemaCreation<'_>,
    ) -> Result<ConnectorTableMetadata, PureProviderCompileError<ConnectorError>> {
        creation.work(1)?;
        let observation = self.statement_observation(original, context)?;
        let request = ConnectorTableRequest {
            table: original.identity.clone(),
            resolution: ConnectorTableResolution::HistoricalSnapshot(snapshot_id),
            context: context.clone(),
        };
        let facts =
            self.statement_table_facts(request, &observation.access, observation.snapshots)?;
        self.create_table_from_observation(facts, creation)
    }

    fn statement_table_facts(
        &self,
        request: ConnectorTableRequest,
        access: &IcebergAttemptTableAccess,
        snapshots: SnapshotLoading,
    ) -> Result<IcebergPreparedTableFacts, ConnectorError> {
        let resources = self.runtime.resources();
        let binding = resources
            .planning_binding()
            .for_request(request.context.clone());
        let loaded = access.reacquired_request_scoped(
            binding,
            resources.catalog_runtime().iceberg_runtime().clone(),
        )?;
        Ok(IcebergPreparedTableFacts {
            table_name: request.table.table.to_string(),
            request,
            loaded,
            metadata_table_type: None,
            snapshots,
            incarnation: self.incarnation,
            generation_owner: Arc::clone(&self.generation_owner),
        })
    }
}

fn frozen_observation_unavailable(reason: &str) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Unavailable,
        format!("Frozen Iceberg table revision cannot be completed: {reason}"),
    )
    .with_retryable_before_progress()
}
