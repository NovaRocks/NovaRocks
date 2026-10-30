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

//! Neutral MV target binding for the refresh / plan / commit call chain.
//!
//! Before SPI-5I the MV apply path threaded
//! `(IcebergCatalogEntry, Arc<dyn Catalog>, IcebergLoadedTable)` through every
//! refresh function and read provider `TableMetadata` directly. This module
//! replaces that triple with a single value that carries only:
//!
//!   - the neutral [`ConnectorTableMetadata`] loaded from one exact generation
//!     (Arrow schema, bounded planning facts, opaque table handle);
//!   - the [`ConnectorControlPlanningLease`] that produced it, retained so
//!     every downstream mutation and write acts on the same generation;
//!   - the [`MvRefreshTargetObservation`] holding the refresh-time snapshot and
//!     ref identity Core legitimately owns.
//!
//! Core never interprets the opaque handle. Physical storage facts a writer
//! needs (table location, sequence numbers, partition spec objects) are
//! deliberately absent — they belong to Provider write preparation.

use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorError, ConnectorErrorKind, ConnectorInstanceId,
    ConnectorRequestContext, ConnectorTableColumnRole, ConnectorTableHandle,
    ConnectorTableIdentity, ConnectorTableMetadata, ConnectorTablePlanningFacts,
};

use novarocks_types::naming::TableIdentity;

use crate::mv::domain::storage_observation::MvRefreshTargetObservation;
use novarocks_mv_application::persistence::schema::MvPartitionContract;

/// One MV target, resolved once against a single provider generation.
///
/// Cloning is cheap enough for the refresh call chain: the metadata's schema is
/// an `Arc`, and the observation's payload is bounded by its own validator.
///
/// Deliberately not `Debug`: `ConnectorTableMetadata` and
/// `ConnectorControlPlanningLease` are not `Debug` precisely so an opaque
/// handle and a live generation cannot end up in a log line.
#[derive(Clone)]
pub struct MvTargetBinding {
    metadata: ConnectorTableMetadata,
    lease: ConnectorControlPlanningLease,
    observation: MvRefreshTargetObservation,
}

impl MvTargetBinding {
    pub(crate) const fn new(
        metadata: ConnectorTableMetadata,
        lease: ConnectorControlPlanningLease,
        observation: MvRefreshTargetObservation,
    ) -> Self {
        Self {
            metadata,
            lease,
            observation,
        }
    }

    /// The exact generation that produced every fact in this binding.
    ///
    /// Downstream mutation and write preparation must reuse this lease rather
    /// than re-resolving `latest`, otherwise a concurrent commit could split
    /// one refresh attempt across two generations.
    pub const fn lease(&self) -> &ConnectorControlPlanningLease {
        &self.lease
    }

    pub const fn metadata(&self) -> &ConnectorTableMetadata {
        &self.metadata
    }

    /// Opaque provider handle. Core passes it through and never decodes it.
    pub const fn handle(&self) -> &ConnectorTableHandle {
        &self.metadata.table
    }

    pub const fn identity(&self) -> &ConnectorTableIdentity {
        &self.metadata.identity
    }

    /// Physical Arrow schema accepted by an MV target write.
    ///
    /// Connector metadata freezes the read schema, which may append synthetic
    /// row-lineage fields such as `_file`, `_pos`, and `_row_id`. Those fields
    /// are query inputs, not declared target fields, and therefore have no
    /// provider field IDs. The planning facts identify them without exposing
    /// provider vocabulary. Hidden ordinary fields remain because MV apply
    /// keys and aggregate state are declared physical target fields.
    pub fn physical_write_schema(&self) -> Result<SchemaRef, String> {
        mv_target_physical_write_schema(
            self.metadata.schema(),
            &self.metadata.planning_facts,
            self.observation.field_ids(),
        )
    }

    /// Preserve the exact target source fields and the same generation's
    /// complete execution declaration as independent write-admission facts.
    pub(crate) fn write_input_fields(
        &self,
    ) -> Result<Arc<[novarocks_spi::connector::ConnectorWriteFieldRequest]>, String> {
        let source = self.physical_write_schema()?;
        let mut fields = Vec::with_capacity(source.fields().len());
        for (ordinal, _) in self.metadata.schema().fields().iter().enumerate() {
            if self
                .metadata
                .planning_facts
                .column_facts()
                .get(ordinal)
                .is_some_and(|fact| fact.role() == ConnectorTableColumnRole::RowLineageSystem)
            {
                continue;
            }
            let declaration =
                crate::connector::connector_write_column(&self.metadata, ordinal, None)?;
            let value_type = declaration
                .declared_value_type()
                .map_err(|error| error.to_string())?;
            let field = source.fields().get(fields.len()).ok_or_else(|| {
                "MV target write field order differs from its complete declarations".to_string()
            })?;
            fields.push(
                novarocks_spi::connector::ConnectorWriteFieldRequest::try_new_typed(
                    field.as_ref().clone(),
                    value_type,
                )
                .map_err(|error| error.to_string())?,
            );
        }
        Ok(fields.into())
    }

    pub(crate) const fn observation(&self) -> &MvRefreshTargetObservation {
        &self.observation
    }

    pub fn table_uuid(&self) -> &str {
        self.observation.table_uuid()
    }

    pub const fn partition(&self) -> &MvPartitionContract {
        self.observation.partition()
    }

    pub const fn current_snapshot_id(&self) -> Option<i64> {
        self.observation.current_snapshot_id()
    }
}

pub(crate) fn validate_target_lease(
    table: &TableIdentity,
    exact_lease: &ConnectorControlPlanningLease,
) -> Result<(), ConnectorError> {
    let expected_instance = ConnectorInstanceId::parse(&table.catalog).map_err(|error| {
        ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
    })?;
    if exact_lease.binding().descriptor().instance_id != expected_instance {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            format!(
                "MV target lease instance does not match table catalog {}",
                table.catalog
            ),
        ));
    }
    Ok(())
}

pub(crate) fn complete_target_binding(
    storage_observation: &dyn novarocks_spi::connector::MvStorageObservationPort,
    exact_lease: ConnectorControlPlanningLease,
    metadata: ConnectorTableMetadata,
    connector_context: &ConnectorRequestContext,
) -> Result<MvTargetBinding, ConnectorError> {
    let observation = crate::mv::domain::storage_observation::observe_refresh_target(
        storage_observation,
        &exact_lease,
        &metadata,
        connector_context.clone(),
    )?;
    Ok(MvTargetBinding::new(metadata, exact_lease, observation))
}

fn mv_target_physical_write_schema(
    read_schema: &arrow::datatypes::Schema,
    planning_facts: &ConnectorTablePlanningFacts,
    field_ids: &[i32],
) -> Result<SchemaRef, String> {
    let column_facts = planning_facts.column_facts();
    if !column_facts.is_empty() && column_facts.len() != read_schema.fields().len() {
        return Err(format!(
            "MV refresh target planning facts cover {} columns but read schema has {}",
            column_facts.len(),
            read_schema.fields().len()
        ));
    }

    let fields = read_schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(ordinal, field)| {
            let fact = column_facts.get(ordinal);
            if fact.is_some_and(|fact| fact.role() == ConnectorTableColumnRole::RowLineageSystem) {
                return None;
            }
            let data_type = fact
                .and_then(|fact| fact.write_target_type())
                .cloned()
                .unwrap_or_else(|| field.data_type().clone());
            Some(Arc::new(field.as_ref().clone().with_data_type(data_type)))
        })
        .collect::<Vec<_>>();
    if fields.len() != field_ids.len() {
        return Err(format!(
            "MV refresh target physical schema has {} fields but observation has {} field IDs",
            fields.len(),
            field_ids.len()
        ));
    }

    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        read_schema.metadata().clone(),
    )))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use novarocks_spi::connector::{
        ConnectorTableColumnPlanningFact, ConnectorTableColumnRole,
        ConnectorTableColumnSemanticKind, ConnectorTableColumnVisibility,
        ConnectorTablePlanningFacts,
    };

    use super::mv_target_physical_write_schema;

    fn fact(
        ordinal: u32,
        visibility: ConnectorTableColumnVisibility,
        role: ConnectorTableColumnRole,
    ) -> ConnectorTableColumnPlanningFact {
        ConnectorTableColumnPlanningFact::new(
            ordinal,
            visibility,
            ConnectorTableColumnSemanticKind::None,
            role,
        )
    }

    #[test]
    fn physical_write_schema_drops_read_only_system_fields_and_preserves_write_facts() {
        let hidden_metadata = HashMap::from([(
            novarocks_spi::connector::CONNECTOR_FIELD_HIDDEN_FROM_SQL.to_string(),
            "true".to_string(),
        )]);
        let read_schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("k1", DataType::Int32, false),
                Field::new("payload", DataType::Binary, true),
                Field::new("__nova_base_row_id", DataType::Int64, false)
                    .with_metadata(hidden_metadata.clone()),
                Field::new("_file", DataType::Utf8, false),
                Field::new("_pos", DataType::Int64, false),
                Field::new("_row_id", DataType::Int64, false),
                Field::new("_last_updated_sequence_number", DataType::Int64, true),
            ],
            HashMap::from([("schema-owner".to_string(), "connector".to_string())]),
        ));
        let facts = ConnectorTablePlanningFacts::try_new(
            &read_schema,
            vec![
                fact(
                    0,
                    ConnectorTableColumnVisibility::Sql,
                    ConnectorTableColumnRole::Ordinary,
                ),
                fact(
                    1,
                    ConnectorTableColumnVisibility::Sql,
                    ConnectorTableColumnRole::Ordinary,
                )
                .with_write_target_type(Some(DataType::LargeBinary)),
                fact(
                    2,
                    ConnectorTableColumnVisibility::Hidden,
                    ConnectorTableColumnRole::Ordinary,
                ),
                fact(
                    3,
                    ConnectorTableColumnVisibility::Hidden,
                    ConnectorTableColumnRole::RowLineageSystem,
                ),
                fact(
                    4,
                    ConnectorTableColumnVisibility::Hidden,
                    ConnectorTableColumnRole::RowLineageSystem,
                ),
                fact(
                    5,
                    ConnectorTableColumnVisibility::Hidden,
                    ConnectorTableColumnRole::RowLineageSystem,
                ),
                fact(
                    6,
                    ConnectorTableColumnVisibility::Hidden,
                    ConnectorTableColumnRole::RowLineageSystem,
                ),
            ],
            vec![],
            vec![],
            vec![],
            &crate::connector::test_request_context(),
        )
        .expect("valid planning facts");

        let physical = mv_target_physical_write_schema(&read_schema, &facts, &[1, 2, 3])
            .expect("derive physical write schema");

        assert_eq!(physical.fields().len(), 3);
        assert_eq!(physical.field(0).name(), "k1");
        assert_eq!(physical.field(1).name(), "payload");
        assert_eq!(physical.field(1).data_type(), &DataType::LargeBinary);
        assert_eq!(physical.field(2).name(), "__nova_base_row_id");
        assert_eq!(physical.field(2).metadata(), &hidden_metadata);
        assert_eq!(
            physical.metadata().get("schema-owner").map(String::as_str),
            Some("connector")
        );
    }

    #[test]
    fn physical_write_schema_fails_closed_when_field_ids_do_not_align() {
        let read_schema = Arc::new(Schema::new(vec![Field::new("k1", DataType::Int32, false)]));

        let error = mv_target_physical_write_schema(
            &read_schema,
            &ConnectorTablePlanningFacts::empty(),
            &[],
        )
        .expect_err("misaligned field IDs must fail");

        assert_eq!(
            error,
            "MV refresh target physical schema has 1 fields but observation has 0 field IDs"
        );
    }
}
