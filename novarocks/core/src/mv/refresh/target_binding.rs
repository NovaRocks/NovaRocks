// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0.

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
    ConnectorControlPlanningLease, ConnectorRequestContext, ConnectorTableColumnRole,
    ConnectorTableHandle, ConnectorTableIdentity, ConnectorTableMetadata,
    ConnectorTablePlanningFacts, ConnectorTableResolution,
};

use novarocks_catalog::identifier::TableIdentity;

use crate::mv::persistence::schema::MvPartitionContract;
use crate::mv::storage_observation::MvRefreshTargetObservation;

/// One MV target, resolved once against a single provider generation.
///
/// Cloning is cheap enough for the refresh call chain: the metadata's schema is
/// an `Arc`, and the observation's payload is bounded by its own validator.
///
/// Deliberately not `Debug`: `ConnectorTableMetadata` and
/// `ConnectorControlPlanningLease` are not `Debug` precisely so an opaque
/// handle and a live generation cannot end up in a log line.
#[derive(Clone)]
pub(crate) struct MvTargetBinding {
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
    pub(crate) const fn lease(&self) -> &ConnectorControlPlanningLease {
        &self.lease
    }

    pub(crate) const fn metadata(&self) -> &ConnectorTableMetadata {
        &self.metadata
    }

    /// Opaque provider handle. Core passes it through and never decodes it.
    pub(crate) const fn handle(&self) -> &ConnectorTableHandle {
        &self.metadata.table
    }

    pub(crate) const fn identity(&self) -> &ConnectorTableIdentity {
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
    pub(crate) fn physical_write_schema(&self) -> Result<SchemaRef, String> {
        mv_target_physical_write_schema(
            &self.metadata.schema,
            &self.metadata.planning_facts,
            self.observation.field_ids(),
        )
    }

    pub(crate) const fn observation(&self) -> &MvRefreshTargetObservation {
        &self.observation
    }

    pub(crate) fn table_uuid(&self) -> &str {
        self.observation.table_uuid()
    }

    pub(crate) const fn schema_id(&self) -> i32 {
        self.observation.schema_id()
    }

    pub(crate) const fn partition(&self) -> &MvPartitionContract {
        self.observation.partition()
    }

    pub(crate) const fn current_snapshot_id(&self) -> Option<i64> {
        self.observation.current_snapshot_id()
    }

    pub(crate) fn snapshot_id_for_ref(&self, ref_name: &str) -> Option<i64> {
        self.observation.snapshot_id_for_ref(ref_name)
    }
}

/// Resolve a target through the explicit MV execution kernel.  This is the
/// kernel-first entry used by frontend-owned MV lifecycle composition.
pub(crate) fn load_mv_target_binding_with_kernel(
    kernel: &crate::engine::domain::MvExecutionKernel,
    table: &TableIdentity,
    connector_context: &ConnectorRequestContext,
) -> Result<MvTargetBinding, String> {
    load_mv_target_binding_with_ports(
        kernel.connector_control().as_ref(),
        kernel.storage_observation().as_ref(),
        table,
        connector_context,
    )
}

/// Resolve an MV target from the exact control and observation ports admitted
/// for a refresh attempt.
///
/// The returned binding retains the planning lease.  A caller must pass that
/// binding through planning and write preparation instead of resolving the
/// catalog's latest connector generation again.
pub(crate) fn load_mv_target_binding_with_ports(
    connector_control: &dyn novarocks_spi::connector::ConnectorControlResolver,
    storage_observation: &dyn crate::mv::storage_observation::MvStorageObservationPort,
    table: &TableIdentity,
    connector_context: &ConnectorRequestContext,
) -> Result<MvTargetBinding, String> {
    let exact_lease =
        crate::connector::acquire_metadata_planning_lease(connector_control, &table.catalog)?;
    load_mv_target_binding_with_lease_and_ports(
        storage_observation,
        table,
        exact_lease,
        connector_context,
    )
}

/// Complete a target binding using a planning lease retained by the caller.
///
/// This preserves atomicity when the caller already acquired the lease while
/// resolving related target facts.
pub(crate) fn load_mv_target_binding_with_lease_and_ports(
    storage_observation: &dyn crate::mv::storage_observation::MvStorageObservationPort,
    table: &TableIdentity,
    exact_lease: ConnectorControlPlanningLease,
    connector_context: &ConnectorRequestContext,
) -> Result<MvTargetBinding, String> {
    let expected_instance = novarocks_spi::connector::ConnectorInstanceId::parse(&table.catalog)
        .map_err(|error| error.to_string())?;
    if exact_lease.binding().descriptor().instance_id != expected_instance {
        return Err(format!(
            "MV target lease instance does not match table catalog {}",
            table.catalog
        ));
    }
    let metadata = crate::connector::metadata_load_connector_table_with_planning_lease(
        &exact_lease,
        connector_context.clone(),
        &table.namespace,
        &table.table,
        ConnectorTableResolution::StrictBaseTable,
    )?;
    let observation = storage_observation
        .observe_refresh_target(&exact_lease, &metadata, connector_context.clone())
        .map_err(|error| {
            format!(
                "observe MV refresh target facts for {}: {error}",
                table.fqn()
            )
        })?;
    Ok(MvTargetBinding::new(metadata, exact_lease, observation))
}

fn mv_target_physical_write_schema(
    read_schema: &SchemaRef,
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
