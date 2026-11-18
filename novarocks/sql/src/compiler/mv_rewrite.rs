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

//! Immutable materialized-view rewrite facts frozen by application admission.
//!
//! The compiler uses this value as data only. Repository enumeration and
//! connector/catalog reads happen before construction, in the application
//! facade, so one statement never observes a changing MV definition set.

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_parser::ast::{
    Expr, FunctionCall, Ident, Literal, LiteralKind, ObjectName, Query, Select, SelectItem,
    SetExpr, TypeName, Visit, WindowSpecification, walk_expr, walk_function_call, walk_object_name,
    walk_query, walk_type_name,
};
use novarocks_spi::connector::{ConnectorExactSemanticRevision, ConnectorTableObjectId};

use crate::binding::SqlTableBindingId;
use crate::catalog::PlannerTableProvider;
use crate::column_id::ColumnRefFactory;
use crate::optimizer::cascades_rules::mv_rewrite::{
    MvRewriteCandidate, descriptor::SpjgDescriptor,
};
use crate::planner::logical::LogicalPlanNode;

/// One syntactic relation in a particular immutable MV definition revision.
/// This is not a query binding, provider read occurrence, or vector offset.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SqlMvRelationOccurrenceId(u32);

impl SqlMvRelationOccurrenceId {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlMvRewritePublicationRelation {
    table_fqn: String,
    revision: ConnectorExactSemanticRevision,
}

impl SqlMvRewritePublicationRelation {
    pub fn new(
        table_fqn: String,
        revision: ConnectorExactSemanticRevision,
    ) -> Result<Self, String> {
        if table_fqn.is_empty() {
            return Err("MV rewrite publication relation has no table identity".to_string());
        }
        Ok(Self {
            table_fqn,
            revision,
        })
    }

    pub fn table_fqn(&self) -> &str {
        &self.table_fqn
    }

    pub const fn revision(&self) -> &ConnectorExactSemanticRevision {
        &self.revision
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlMvRewritePublicationInput {
    occurrence_id: SqlMvRelationOccurrenceId,
    relation: SqlMvRewritePublicationRelation,
}

impl SqlMvRewritePublicationInput {
    pub fn try_new(
        occurrence_id: SqlMvRelationOccurrenceId,
        relation: SqlMvRewritePublicationRelation,
    ) -> Result<Self, String> {
        Ok(Self {
            occurrence_id,
            relation,
        })
    }

    pub const fn occurrence_id(&self) -> SqlMvRelationOccurrenceId {
        self.occurrence_id
    }

    pub const fn relation(&self) -> &SqlMvRewritePublicationRelation {
        &self.relation
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlMvRewriteSelectionFacts {
    publication_id: [u8; 16],
    definition_fingerprint: [u8; 32],
    definition_revision: [u8; 32],
    interpretation_revision: [u8; 32],
    publication_provenance: Arc<str>,
    publication_inputs: Vec<SqlMvRewritePublicationInput>,
    publication_target: SqlMvRewritePublicationRelation,
}

impl SqlMvRewriteSelectionFacts {
    #[cfg(any(test, novarocks_dev_controls))]
    pub fn try_new(
        publication_id: [u8; 16],
        definition_fingerprint: [u8; 32],
        publication_inputs: Vec<String>,
    ) -> Result<Self, String> {
        Self::try_new_for_target(
            publication_id,
            definition_fingerprint,
            publication_inputs,
            "ice.ns.mv".to_string(),
        )
    }

    #[cfg(any(test, novarocks_dev_controls))]
    pub fn try_new_for_target(
        publication_id: [u8; 16],
        definition_fingerprint: [u8; 32],
        publication_inputs: Vec<String>,
        publication_target: String,
    ) -> Result<Self, String> {
        Self::try_new_for_target_with_occurrences(
            publication_id,
            definition_fingerprint,
            publication_inputs
                .into_iter()
                .enumerate()
                .map(|(ordinal, table)| (SqlMvRelationOccurrenceId::new(ordinal as u32), table))
                .collect(),
            publication_target,
        )
    }

    #[cfg(any(test, novarocks_dev_controls))]
    pub fn try_new_for_target_with_occurrences(
        publication_id: [u8; 16],
        definition_fingerprint: [u8; 32],
        publication_inputs: Vec<(SqlMvRelationOccurrenceId, String)>,
        publication_target: String,
    ) -> Result<Self, String> {
        use bytes::Bytes;

        let provider = novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
            .map_err(|error| format!("construct test MV provider: {error}"))?;
        let publication_inputs = publication_inputs
            .into_iter()
            .map(|(occurrence_id, table_fqn)| {
                let object = ConnectorTableObjectId::try_new(Bytes::from(format!(
                    "test-input-{}",
                    occurrence_id.get()
                )))
                .map_err(|error| error.to_string())?;
                SqlMvRewritePublicationInput::try_new(
                    occurrence_id,
                    SqlMvRewritePublicationRelation::new(
                        table_fqn,
                        ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
                            provider.clone(),
                            &object,
                            Some(101),
                        )
                        .map_err(|error| error.to_string())?,
                    )?,
                )
            })
            .collect::<Result<Vec<_>, String>>()?;
        let target_object = ConnectorTableObjectId::try_new(Bytes::from_static(b"test-target"))
            .map_err(|error| error.to_string())?;
        Self::try_new_with_publication(
            publication_id,
            definition_fingerprint,
            [11; 32],
            [12; 32],
            Arc::from("test-provider-provenance"),
            publication_inputs,
            SqlMvRewritePublicationRelation::new(
                publication_target,
                ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
                    provider,
                    &target_object,
                    Some(201),
                )
                .map_err(|error| error.to_string())?,
            )?,
        )
    }

    pub fn try_new_with_publication(
        publication_id: [u8; 16],
        definition_fingerprint: [u8; 32],
        definition_revision: [u8; 32],
        interpretation_revision: [u8; 32],
        publication_provenance: Arc<str>,
        publication_inputs: Vec<SqlMvRewritePublicationInput>,
        publication_target: SqlMvRewritePublicationRelation,
    ) -> Result<Self, String> {
        if publication_id == [0; 16]
            || definition_fingerprint == [0; 32]
            || definition_revision == [0; 32]
            || interpretation_revision == [0; 32]
        {
            return Err("MV rewrite selection identity cannot be zero".to_string());
        }
        if publication_provenance.is_empty() {
            return Err("MV rewrite selection has no provider provenance".to_string());
        }
        if publication_inputs.is_empty() {
            return Err("MV rewrite selection must name every publication input".to_string());
        }
        let mut unique = publication_inputs
            .iter()
            .map(SqlMvRewritePublicationInput::occurrence_id)
            .collect::<Vec<_>>();
        unique.sort_unstable();
        if unique.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err("MV rewrite selection repeats a publication input".to_string());
        }
        Ok(Self {
            publication_id,
            definition_fingerprint,
            definition_revision,
            interpretation_revision,
            publication_provenance,
            publication_inputs,
            publication_target,
        })
    }

    pub(crate) const fn publication_id(&self) -> [u8; 16] {
        self.publication_id
    }

    pub(crate) const fn definition_fingerprint(&self) -> [u8; 32] {
        self.definition_fingerprint
    }

    pub(crate) fn publication_provenance(&self) -> &str {
        &self.publication_provenance
    }

    pub(crate) fn publication_inputs(&self) -> &[SqlMvRewritePublicationInput] {
        &self.publication_inputs
    }

    pub(crate) const fn publication_target(&self) -> &SqlMvRewritePublicationRelation {
        &self.publication_target
    }

    pub(crate) const fn definition_revision(&self) -> [u8; 32] {
        self.definition_revision
    }

    pub(crate) const fn interpretation_revision(&self) -> [u8; 32] {
        self.interpretation_revision
    }
}
use crate::planner::table::ScanSource;

use super::{SqlCompileError, SqlFunctionCatalog, SqlStatisticsPlan, SqlStatisticsSnapshot};

/// Immutable base-snapshot facts submitted by the application to an IMV
/// rewrite snapshot builder.  This is deliberately a value-only boundary:
/// it contains neither a provider table nor a request lifecycle capability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlImvBaseSnapshotFacts {
    occurrence_id: SqlMvRelationOccurrenceId,
    table: novarocks_types::naming::TableIdentity,
    qualifier_at_binding: String,
    snapshot_id: i64,
    table_object_id: ConnectorTableObjectId,
}

impl SqlImvBaseSnapshotFacts {
    pub fn try_new(
        occurrence_id: SqlMvRelationOccurrenceId,
        table: novarocks_types::naming::TableIdentity,
        qualifier_at_binding: String,
        snapshot_id: i64,
        table_object_id: ConnectorTableObjectId,
    ) -> Result<Self, String> {
        if snapshot_id < 0
            || table_object_id.as_bytes().is_empty()
            || qualifier_at_binding.trim().is_empty()
        {
            return Err("IMV base snapshot facts are incomplete".to_string());
        }
        Ok(Self {
            occurrence_id,
            table,
            qualifier_at_binding,
            snapshot_id,
            table_object_id,
        })
    }

    fn into_snapshot(self) -> SqlImvBaseSnapshot {
        SqlImvBaseSnapshot {
            occurrence_id: self.occurrence_id,
            table: self.table,
            qualifier_at_binding: self.qualifier_at_binding,
            snapshot_id: self.snapshot_id,
            table_object_id: self.table_object_id,
        }
    }
}

/// Immutable target-column facts copied from the admitted target schema.
/// Defaults and provider metadata are intentionally absent.
#[derive(Clone, Debug, PartialEq)]
pub struct SqlImvTargetColumnsFacts {
    columns: Arc<[novarocks_types::schema::ColumnDef]>,
}

impl SqlImvTargetColumnsFacts {
    pub fn try_new(columns: Vec<novarocks_types::schema::ColumnDef>) -> Result<Self, String> {
        if columns.is_empty()
            || columns.iter().any(|column| column.name.trim().is_empty())
            || columns.iter().enumerate().any(|(index, column)| {
                columns[..index]
                    .iter()
                    .any(|other| other.name.eq_ignore_ascii_case(&column.name))
            })
        {
            return Err("IMV target column facts are invalid".to_string());
        }
        Ok(Self {
            columns: Arc::from(columns),
        })
    }

    fn into_columns(self) -> Arc<[novarocks_types::schema::ColumnDef]> {
        self.columns
    }
}

/// The value-only foundation of a sealed IMV rewrite snapshot.  Additional
/// contract facts are supplied by the SQL-owned builder; applications can
/// never recover or mutate the resulting planner snapshot.
pub struct SqlImvRewriteSnapshotBuilder {
    target: novarocks_types::naming::TableIdentity,
    target_binding: SqlTableBindingId,
    mv_id: i64,
    base_snapshots: Vec<SqlImvBaseSnapshotFacts>,
    target_columns: Option<SqlImvTargetColumnsFacts>,
    refresh_history: Option<SqlImvRefreshHistoryFacts>,
    schema_contract: Option<SqlImvSchemaContractFacts>,
    aggregate_execution: Option<SqlImvAggregateExecutionFacts>,
}

impl SqlImvRewriteSnapshotBuilder {
    pub fn try_new(
        target: novarocks_types::naming::TableIdentity,
        target_binding: SqlTableBindingId,
        mv_id: i64,
    ) -> Result<Self, String> {
        if mv_id < 0 {
            return Err("IMV rewrite snapshot has an invalid MV identity".to_string());
        }
        Ok(Self {
            target,
            target_binding,
            mv_id,
            base_snapshots: Vec::new(),
            target_columns: None,
            refresh_history: None,
            schema_contract: None,
            aggregate_execution: None,
        })
    }

    pub fn add_base_snapshot(&mut self, base: SqlImvBaseSnapshotFacts) -> Result<(), String> {
        if self
            .base_snapshots
            .iter()
            .any(|existing| existing.occurrence_id == base.occurrence_id)
        {
            return Err(format!(
                "IMV rewrite snapshot has duplicate base {}",
                base.table.fqn()
            ));
        }
        self.base_snapshots.push(base);
        Ok(())
    }

    pub fn target(&self) -> &novarocks_types::naming::TableIdentity {
        &self.target
    }

    pub fn target_binding(&self) -> SqlTableBindingId {
        self.target_binding
    }

    pub fn mv_id(&self) -> i64 {
        self.mv_id
    }

    pub fn base_count(&self) -> usize {
        self.base_snapshots.len()
    }

    pub fn set_target_columns(&mut self, columns: SqlImvTargetColumnsFacts) -> Result<(), String> {
        if self.target_columns.is_some() {
            return Err("IMV rewrite snapshot target columns were submitted twice".to_string());
        }
        self.target_columns = Some(columns);
        Ok(())
    }

    pub fn set_refresh_history(
        &mut self,
        history: SqlImvRefreshHistoryFacts,
    ) -> Result<(), String> {
        if self.refresh_history.is_some() {
            return Err("IMV rewrite snapshot refresh history was submitted twice".to_string());
        }
        self.refresh_history = Some(history);
        Ok(())
    }

    pub fn set_schema_contract(
        &mut self,
        contract: SqlImvSchemaContractFacts,
    ) -> Result<(), String> {
        if self.schema_contract.is_some() {
            return Err("IMV rewrite snapshot schema contract was submitted twice".to_string());
        }
        self.schema_contract = Some(contract);
        Ok(())
    }

    pub fn set_aggregate_execution(
        &mut self,
        layout: SqlImvAggregateExecutionFacts,
    ) -> Result<(), String> {
        if self.aggregate_execution.is_some() {
            return Err("IMV rewrite snapshot aggregate execution was submitted twice".to_string());
        }
        self.aggregate_execution = Some(layout);
        Ok(())
    }

    /// Seal the submitted copied facts.  The returned handle intentionally has
    /// no accessors for the planner snapshot or its private graph vocabulary.
    pub fn build(mut self) -> Result<SqlImvRewriteSnapshotHandle, String> {
        let base_snapshots = self.take_base_snapshots()?;
        let target_columns = self.take_target_columns()?;
        let history = self
            .refresh_history
            .take()
            .ok_or_else(|| "IMV rewrite snapshot has no refresh history facts".to_string())?;
        let schema_contract = self
            .schema_contract
            .take()
            .ok_or_else(|| "IMV rewrite snapshot has no schema contract facts".to_string())?;
        if base_snapshots.len() != schema_contract.inner.bases.len()
            || base_snapshots.iter().zip(&schema_contract.inner.bases).any(
                |(snapshot, contract)| {
                    snapshot.occurrence_id != contract.occurrence_id
                        || snapshot.table.fqn() != contract.table_fqn
                },
            )
        {
            return Err("IMV snapshot/schema facts must cover the same definition occurrences in definition order".to_string());
        }
        let aggregate_execution = self.aggregate_execution.take().map(|facts| facts.inner);
        let snapshot = SqlImvRewriteSnapshot::from_frozen_parts(
            self.target,
            self.target_binding,
            self.mv_id,
            base_snapshots,
            history.previous_snapshot_ids,
            history.previous_table_object_ids,
            history.target_snapshot_id,
            history.target_table_uuid,
            target_columns,
            Arc::new(schema_contract.inner),
            aggregate_execution,
        )?;
        Ok(SqlImvRewriteSnapshotHandle(Arc::new(snapshot)))
    }

    fn take_base_snapshots(&mut self) -> Result<Arc<[SqlImvBaseSnapshot]>, String> {
        if self.base_snapshots.is_empty() {
            return Err("IMV rewrite snapshot has no base table snapshots".to_string());
        }
        Ok(Arc::from(
            std::mem::take(&mut self.base_snapshots)
                .into_iter()
                .map(SqlImvBaseSnapshotFacts::into_snapshot)
                .collect::<Vec<_>>(),
        ))
    }

    fn take_target_columns(&mut self) -> Result<Arc<[novarocks_types::schema::ColumnDef]>, String> {
        self.target_columns
            .take()
            .map(SqlImvTargetColumnsFacts::into_columns)
            .ok_or_else(|| "IMV rewrite snapshot has no target column facts".to_string())
    }
}

/// Opaque sealed IMV rewrite facts.  Cloning this handle only shares the
/// immutable SQL-owned snapshot; it cannot expose or mutate planner state.
#[derive(Clone)]
pub struct SqlImvRewriteSnapshotHandle(Arc<SqlImvRewriteSnapshot>);

impl SqlImvRewriteSnapshotHandle {
    pub fn with_target_file_metadata(
        mut self,
        metadata: [novarocks_types::schema::ColumnDef; 2],
    ) -> Result<Self, String> {
        let [spec, partition] = &metadata;
        if spec.name != "_spec_id"
            || spec.nullable
            || spec.data_type != arrow::datatypes::DataType::Int32
            || partition.name != "_partition"
            || !partition.nullable
            || !matches!(partition.data_type, arrow::datatypes::DataType::Struct(_))
            || self.0.target_file_metadata.is_some()
        {
            return Err("IMV target file metadata differs from its signed scan contract".into());
        }
        Arc::make_mut(&mut self.0).target_file_metadata = Some(Arc::new(metadata));
        Ok(self)
    }

    pub(crate) fn snapshot(&self) -> &Arc<SqlImvRewriteSnapshot> {
        &self.0
    }
}

/// Frozen refresh-history values.  Snapshot pins are identifiers only; table
/// handles, leases and catalog callbacks are deliberately excluded.
pub struct SqlImvRefreshHistoryFacts {
    previous_snapshot_ids: BTreeMap<SqlMvRelationOccurrenceId, i64>,
    previous_table_object_ids: BTreeMap<SqlMvRelationOccurrenceId, ConnectorTableObjectId>,
    target_snapshot_id: Option<i64>,
    target_table_uuid: String,
}

impl SqlImvRefreshHistoryFacts {
    pub fn try_new(
        previous_snapshot_ids: BTreeMap<SqlMvRelationOccurrenceId, i64>,
        previous_table_object_ids: BTreeMap<SqlMvRelationOccurrenceId, ConnectorTableObjectId>,
        target_snapshot_id: Option<i64>,
        target_table_uuid: String,
    ) -> Result<Self, String> {
        if target_snapshot_id.is_some_and(|snapshot_id| snapshot_id < 0)
            || target_table_uuid.trim().is_empty()
            || previous_snapshot_ids
                .iter()
                .any(|(_, snapshot_id)| *snapshot_id < 0)
            || previous_snapshot_ids
                .keys()
                .ne(previous_table_object_ids.keys())
            || previous_table_object_ids
                .iter()
                .any(|(_, object_id)| object_id.as_bytes().is_empty())
        {
            return Err("IMV refresh history facts are invalid".to_string());
        }
        Ok(Self {
            previous_snapshot_ids,
            previous_table_object_ids,
            target_snapshot_id,
            target_table_uuid,
        })
    }
}

/// One base-table snapshot admitted for an incremental MV refresh.
///
/// The compiler identifies a base by its canonical identity and never asks a
/// connector for a newer snapshot.  The application converts its provider
/// lease into this value before calling the compiler.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvBaseSnapshot {
    pub(crate) occurrence_id: SqlMvRelationOccurrenceId,
    pub(crate) table: novarocks_types::naming::TableIdentity,
    pub(crate) qualifier_at_binding: String,
    pub(crate) snapshot_id: i64,
    pub(crate) table_object_id: ConnectorTableObjectId,
}

/// SQL classification of the two physical aggregate-state roles used by the
/// incremental refresh plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlImvAggregateStateRole {
    Single,
    AvgSum,
    AvgCount,
    RetractionCount,
}

/// One visible target output in an aggregate refresh layout.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvAggregateVisibleColumn {
    pub(crate) name: String,
    pub(crate) value_type: novarocks_type_contract::FunctionValueType,
}

/// One physical state column in an aggregate refresh layout.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvAggregateStateColumn {
    pub(crate) name: String,
    pub(crate) value_type: novarocks_type_contract::FunctionValueType,
    pub(crate) visible_source_index: usize,
    pub(crate) aggregate_index: usize,
    pub(crate) function: crate::mv_refresh::AggregateFunctionKind,
    pub(crate) state_role: SqlImvAggregateStateRole,
    pub(crate) count_star: bool,
}

/// SQL-only aggregate IMV layout frozen by application admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvAggregateLayout {
    pub(crate) row_id_column_name: String,
    pub(crate) visible_columns: Vec<SqlImvAggregateVisibleColumn>,
    pub(crate) state_columns: Vec<SqlImvAggregateStateColumn>,
    pub(crate) group_key_source_indexes: Vec<usize>,
    pub(crate) physical_column_names: Vec<String>,
    pub(crate) aggregate_input_types: Vec<Option<arrow::datatypes::DataType>>,
}

/// Aggregate-shape facts required by SQL rewrite construction.  The original
/// persisted SELECT and the application aggregate-state implementation stay
/// outside the compiler boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvAggregateShape {
    pub(crate) group_key_count: usize,
    pub(crate) visible_outputs: Vec<crate::mv_refresh::VisibleAggregateOutput>,
}

/// The aggregate facts admitted for an IMV refresh.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvAggregateExecutionLayout {
    pub(crate) shape: SqlImvAggregateShape,
    pub(crate) layout: SqlImvAggregateLayout,
}

/// SQL-owned lineage kind recorded in an immutable MV contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlImvExpressionKind {
    Column,
    Cast,
    Func,
    Literal,
    Mixed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvQualifiedFieldLineage {
    pub(crate) occurrence_id: SqlMvRelationOccurrenceId,
    pub(crate) table_fqn: String,
    pub(crate) qualifier_at_create: String,
    pub(crate) field_id: bytes::Bytes,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvExpressionLineage {
    pub(crate) kind: SqlImvExpressionKind,
    pub(crate) referenced_base_fields: Vec<SqlImvQualifiedFieldLineage>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvOutputColumnLineage {
    pub(crate) expression: SqlImvExpressionLineage,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvBaseField {
    pub(crate) field_id: bytes::Bytes,
    pub(crate) name_at_create: String,
    pub(crate) data_type: arrow::datatypes::DataType,
    pub(crate) nullable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvBaseContract {
    pub(crate) occurrence_id: SqlMvRelationOccurrenceId,
    pub(crate) table_fqn: String,
    pub(crate) alias_at_create: Option<String>,
    pub(crate) fields: Vec<SqlImvBaseField>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlImvJoinContractKind {
    InnerEquiJoin,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvJoinPredicateLineage {
    pub(crate) left: SqlImvQualifiedFieldLineage,
    pub(crate) right: SqlImvQualifiedFieldLineage,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvJoinContract {
    pub(crate) kind: SqlImvJoinContractKind,
    pub(crate) predicates: Vec<SqlImvJoinPredicateLineage>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlImvAggregateStateRoleContract {
    Single,
    AvgSum,
    AvgCount,
    RetractionCount,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvAggregateStateColumnContract {
    pub(crate) column_name: String,
    pub(crate) type_signature: String,
    pub(crate) role: SqlImvAggregateStateRoleContract,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvAggregateContract {
    pub(crate) state_layout_version: u16,
    pub(crate) row_id_column_name: String,
    pub(crate) state_columns: Vec<SqlImvAggregateStateColumnContract>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvTargetVisibleColumn {
    pub(crate) output_name: String,
    pub(crate) target_field_id: bytes::Bytes,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvHiddenApplyKey {
    pub(crate) column_name: String,
    pub(crate) source: crate::planner::vocabulary::ApplyKeySource,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvBranchContract {
    pub(crate) branch_id_column_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvPartitionContract {
    pub(crate) target_spec_id: i32,
    pub(crate) fields: Vec<SqlImvPartitionField>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvPartitionField {
    pub(crate) partition_field_name: String,
    pub(crate) source_target_field_id: bytes::Bytes,
    pub(crate) transform: SqlImvPartitionTransform,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SqlImvPartitionTransform {
    Identity,
    Year,
    Month,
    Day,
    Hour,
    Bucket { num_buckets: u32 },
    Truncate { width: u32 },
    Void,
}

/// Plan-time, SQL-owned partition derivation facts.  Execution converts this
/// abstract transform into a connector-specific representation after compile.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvPartitionDerivationSpec {
    pub(crate) target_spec_id: i32,
    pub(crate) fields: Vec<SqlImvPartitionDerivationField>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvPartitionDerivationField {
    pub(crate) partition_field_name: String,
    pub(crate) source_target_field_id: bytes::Bytes,
    pub(crate) output_index: usize,
    pub(crate) transform: SqlImvPartitionTransform,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvTargetContract {
    pub(crate) visible_columns: Vec<SqlImvTargetVisibleColumn>,
    pub(crate) hidden_apply_key: SqlImvHiddenApplyKey,
    pub(crate) partition: Option<SqlImvPartitionContract>,
}

/// Immutable SQL projection of the persisted MV schema contract.  Persistence
/// adapters must translate their serialized form before compiler entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvSchemaContract {
    pub(crate) bases: Vec<SqlImvBaseContract>,
    pub(crate) output_columns: Vec<SqlImvOutputColumnLineage>,
    pub(crate) join: Option<SqlImvJoinContract>,
    pub(crate) aggregate: Option<SqlImvAggregateContract>,
    pub(crate) branch: Option<SqlImvBranchContract>,
    pub(crate) target: SqlImvTargetContract,
}

/// SQL-owned expression classification admitted from persisted lineage facts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlImvExpressionKindFacts {
    Column,
    Cast,
    Func,
    Literal,
    Mixed,
}

impl From<SqlImvExpressionKindFacts> for SqlImvExpressionKind {
    fn from(value: SqlImvExpressionKindFacts) -> Self {
        match value {
            SqlImvExpressionKindFacts::Column => Self::Column,
            SqlImvExpressionKindFacts::Cast => Self::Cast,
            SqlImvExpressionKindFacts::Func => Self::Func,
            SqlImvExpressionKindFacts::Literal => Self::Literal,
            SqlImvExpressionKindFacts::Mixed => Self::Mixed,
        }
    }
}

/// A qualified base field recorded at MV creation.  Its identity is copied as
/// strings and an id; it cannot be used to resolve a current provider table.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlImvQualifiedFieldFacts {
    inner: SqlImvQualifiedFieldLineage,
}

impl SqlImvQualifiedFieldFacts {
    pub fn try_new(
        occurrence_id: SqlMvRelationOccurrenceId,
        table_fqn: String,
        qualifier_at_create: String,
        field_id: bytes::Bytes,
    ) -> Result<Self, String> {
        if table_fqn.trim().is_empty()
            || qualifier_at_create.trim().is_empty()
            || field_id.is_empty()
        {
            return Err("IMV qualified field facts are invalid".to_string());
        }
        Ok(Self {
            inner: SqlImvQualifiedFieldLineage {
                occurrence_id,
                table_fqn,
                qualifier_at_create,
                field_id,
            },
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlImvExpressionFacts {
    inner: SqlImvExpressionLineage,
}

impl SqlImvExpressionFacts {
    pub fn try_new(
        kind: SqlImvExpressionKindFacts,
        referenced_base_fields: Vec<SqlImvQualifiedFieldFacts>,
    ) -> Result<Self, String> {
        if referenced_base_fields
            .iter()
            .enumerate()
            .any(|(index, field)| {
                referenced_base_fields[..index].iter().any(|other| {
                    other.inner.occurrence_id == field.inner.occurrence_id
                        && other.inner.field_id == field.inner.field_id
                })
            })
        {
            return Err("IMV expression field-id facts are invalid".to_string());
        }
        Ok(Self {
            inner: SqlImvExpressionLineage {
                kind: kind.into(),
                referenced_base_fields: referenced_base_fields
                    .into_iter()
                    .map(|facts| facts.inner)
                    .collect(),
            },
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlImvOutputColumnFacts {
    inner: SqlImvOutputColumnLineage,
}

impl SqlImvOutputColumnFacts {
    pub fn new(expression: SqlImvExpressionFacts) -> Self {
        Self {
            inner: SqlImvOutputColumnLineage {
                expression: expression.inner,
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlImvBaseFieldFacts {
    inner: SqlImvBaseField,
}

impl SqlImvBaseFieldFacts {
    pub fn try_new(
        field_id: bytes::Bytes,
        name_at_create: String,
        data_type: arrow::datatypes::DataType,
        nullable: bool,
    ) -> Result<Self, String> {
        if field_id.is_empty() || name_at_create.trim().is_empty() {
            return Err("IMV base field facts are invalid".to_string());
        }
        Ok(Self {
            inner: SqlImvBaseField {
                field_id,
                name_at_create,
                data_type,
                nullable,
            },
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlImvBaseContractFacts {
    inner: SqlImvBaseContract,
}

impl SqlImvBaseContractFacts {
    pub fn try_new(
        occurrence_id: SqlMvRelationOccurrenceId,
        table_fqn: String,
        alias_at_create: Option<String>,
        fields: Vec<SqlImvBaseFieldFacts>,
    ) -> Result<Self, String> {
        if table_fqn.trim().is_empty()
            || alias_at_create
                .as_ref()
                .is_some_and(|alias| alias.trim().is_empty())
            || fields.is_empty()
            || fields.iter().enumerate().any(|(index, field)| {
                fields[..index].iter().any(|other| {
                    other.inner.field_id == field.inner.field_id
                        || other.inner.name_at_create == field.inner.name_at_create
                })
            })
        {
            return Err("IMV base contract facts are invalid".to_string());
        }
        Ok(Self {
            inner: SqlImvBaseContract {
                occurrence_id,
                table_fqn,
                alias_at_create,
                fields: fields.into_iter().map(|facts| facts.inner).collect(),
            },
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlImvJoinKindFacts {
    InnerEquiJoin,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlImvJoinPredicateFacts {
    inner: SqlImvJoinPredicateLineage,
}

impl SqlImvJoinPredicateFacts {
    pub fn try_new(
        left: SqlImvQualifiedFieldFacts,
        right: SqlImvQualifiedFieldFacts,
    ) -> Result<Self, String> {
        if left.inner.occurrence_id == right.inner.occurrence_id
            && left.inner.field_id == right.inner.field_id
        {
            return Err("IMV join predicate facts cannot compare one field to itself".to_string());
        }
        Ok(Self {
            inner: SqlImvJoinPredicateLineage {
                left: left.inner,
                right: right.inner,
            },
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlImvJoinContractFacts {
    inner: SqlImvJoinContract,
}

impl SqlImvJoinContractFacts {
    pub fn try_new(
        kind: SqlImvJoinKindFacts,
        predicates: Vec<SqlImvJoinPredicateFacts>,
    ) -> Result<Self, String> {
        if predicates.is_empty() {
            return Err("IMV join contract has no equality predicates".to_string());
        }
        Ok(Self {
            inner: SqlImvJoinContract {
                kind: match kind {
                    SqlImvJoinKindFacts::InnerEquiJoin => SqlImvJoinContractKind::InnerEquiJoin,
                },
                predicates: predicates.into_iter().map(|facts| facts.inner).collect(),
            },
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlImvAggregateStateRoleFacts {
    Single,
    AvgSum,
    AvgCount,
    RetractionCount,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlImvAggregateStateColumnFacts {
    inner: SqlImvAggregateStateColumnContract,
}

impl SqlImvAggregateStateColumnFacts {
    pub fn try_new(
        column_name: String,
        type_signature: String,
        role: SqlImvAggregateStateRoleFacts,
    ) -> Result<Self, String> {
        if column_name.trim().is_empty() || type_signature.trim().is_empty() {
            return Err("IMV aggregate state column facts are invalid".to_string());
        }
        Ok(Self {
            inner: SqlImvAggregateStateColumnContract {
                column_name,
                type_signature,
                role: match role {
                    SqlImvAggregateStateRoleFacts::Single => {
                        SqlImvAggregateStateRoleContract::Single
                    }
                    SqlImvAggregateStateRoleFacts::AvgSum => {
                        SqlImvAggregateStateRoleContract::AvgSum
                    }
                    SqlImvAggregateStateRoleFacts::AvgCount => {
                        SqlImvAggregateStateRoleContract::AvgCount
                    }
                    SqlImvAggregateStateRoleFacts::RetractionCount => {
                        SqlImvAggregateStateRoleContract::RetractionCount
                    }
                },
            },
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlImvAggregateContractFacts {
    inner: SqlImvAggregateContract,
}

impl SqlImvAggregateContractFacts {
    pub fn try_new(
        state_layout_version: u16,
        row_id_column_name: String,
        state_columns: Vec<SqlImvAggregateStateColumnFacts>,
    ) -> Result<Self, String> {
        if state_layout_version == 0
            || row_id_column_name.trim().is_empty()
            || state_columns.is_empty()
            || state_columns.iter().enumerate().any(|(index, column)| {
                state_columns[..index].iter().any(|other| {
                    other
                        .inner
                        .column_name
                        .eq_ignore_ascii_case(&column.inner.column_name)
                })
            })
        {
            return Err("IMV aggregate contract facts are invalid".to_string());
        }
        Ok(Self {
            inner: SqlImvAggregateContract {
                state_layout_version,
                row_id_column_name,
                state_columns: state_columns.into_iter().map(|facts| facts.inner).collect(),
            },
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlImvApplyKeySourceFacts {
    BaseRowId,
    JoinRowKey,
    GroupRowId,
}

impl SqlImvApplyKeySourceFacts {
    /// Decode the stable persisted spelling without exposing SQL planner
    /// vocabulary to the persistence adapter.
    pub fn try_from_persisted_label(label: &str) -> Result<Self, String> {
        match label {
            "BaseRowId" | "BASE_ROW_ID" => Ok(Self::BaseRowId),
            "JoinRowKey" | "JOIN_ROW_KEY" => Ok(Self::JoinRowKey),
            "GroupRowId" | "GROUP_ROW_ID" => Ok(Self::GroupRowId),
            _ => Err("IMV hidden apply-key source is unsupported".to_string()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SqlImvPartitionTransformFacts {
    Identity,
    Year,
    Month,
    Day,
    Hour,
    Bucket { num_buckets: u32 },
    Truncate { width: u32 },
    Void,
}

impl SqlImvPartitionTransformFacts {
    fn into_internal(self) -> Result<SqlImvPartitionTransform, String> {
        match self {
            Self::Identity => Ok(SqlImvPartitionTransform::Identity),
            Self::Year => Ok(SqlImvPartitionTransform::Year),
            Self::Month => Ok(SqlImvPartitionTransform::Month),
            Self::Day => Ok(SqlImvPartitionTransform::Day),
            Self::Hour => Ok(SqlImvPartitionTransform::Hour),
            Self::Bucket { num_buckets } if num_buckets > 0 => {
                Ok(SqlImvPartitionTransform::Bucket { num_buckets })
            }
            Self::Truncate { width } if width > 0 => {
                Ok(SqlImvPartitionTransform::Truncate { width })
            }
            Self::Void => Ok(SqlImvPartitionTransform::Void),
            _ => Err("IMV partition transform facts are invalid".to_string()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlImvPartitionFieldFacts {
    partition_field_name: String,
    source_target_field_id: bytes::Bytes,
    transform: SqlImvPartitionTransformFacts,
}

impl SqlImvPartitionFieldFacts {
    pub fn try_new(
        partition_field_name: String,
        source_target_field_id: bytes::Bytes,
        transform: SqlImvPartitionTransformFacts,
    ) -> Result<Self, String> {
        if partition_field_name.trim().is_empty() || source_target_field_id.is_empty() {
            return Err("IMV partition field facts are invalid".to_string());
        }
        transform.clone().into_internal()?;
        Ok(Self {
            partition_field_name,
            source_target_field_id,
            transform,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlImvPartitionFacts {
    inner: SqlImvPartitionContract,
}

impl SqlImvPartitionFacts {
    pub fn try_new(
        target_spec_id: i32,
        fields: Vec<SqlImvPartitionFieldFacts>,
    ) -> Result<Self, String> {
        if target_spec_id < 0
            || fields.iter().enumerate().any(|(index, field)| {
                fields[..index].iter().any(|other| {
                    other
                        .partition_field_name
                        .eq_ignore_ascii_case(&field.partition_field_name)
                })
            })
        {
            return Err("IMV partition facts are invalid".to_string());
        }
        Ok(Self {
            inner: SqlImvPartitionContract {
                target_spec_id,
                fields: fields
                    .into_iter()
                    .map(|facts| {
                        Ok(SqlImvPartitionField {
                            partition_field_name: facts.partition_field_name,
                            source_target_field_id: facts.source_target_field_id,
                            transform: facts.transform.into_internal()?,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
            },
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlImvTargetVisibleColumnFacts {
    inner: SqlImvTargetVisibleColumn,
}

impl SqlImvTargetVisibleColumnFacts {
    pub fn try_new(output_name: String, target_field_id: bytes::Bytes) -> Result<Self, String> {
        if output_name.trim().is_empty() || target_field_id.is_empty() {
            return Err("IMV target visible-column facts are invalid".to_string());
        }
        Ok(Self {
            inner: SqlImvTargetVisibleColumn {
                output_name,
                target_field_id,
            },
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlImvTargetContractFacts {
    inner: SqlImvTargetContract,
}

impl SqlImvTargetContractFacts {
    pub fn try_new(
        visible_columns: Vec<SqlImvTargetVisibleColumnFacts>,
        hidden_apply_key_column_name: String,
        hidden_apply_key_source: SqlImvApplyKeySourceFacts,
        partition: Option<SqlImvPartitionFacts>,
    ) -> Result<Self, String> {
        if visible_columns.is_empty()
            || hidden_apply_key_column_name.trim().is_empty()
            || visible_columns.iter().enumerate().any(|(index, column)| {
                visible_columns[..index].iter().any(|other| {
                    other
                        .inner
                        .target_field_id
                        .eq(&column.inner.target_field_id)
                        || other
                            .inner
                            .output_name
                            .eq_ignore_ascii_case(&column.inner.output_name)
                })
            })
        {
            return Err("IMV target contract facts are invalid".to_string());
        }
        Ok(Self {
            inner: SqlImvTargetContract {
                visible_columns: visible_columns
                    .into_iter()
                    .map(|facts| facts.inner)
                    .collect(),
                hidden_apply_key: SqlImvHiddenApplyKey {
                    column_name: hidden_apply_key_column_name,
                    source: match hidden_apply_key_source {
                        SqlImvApplyKeySourceFacts::BaseRowId => {
                            crate::planner::vocabulary::ApplyKeySource::BaseRowId
                        }
                        SqlImvApplyKeySourceFacts::JoinRowKey => {
                            crate::planner::vocabulary::ApplyKeySource::JoinRowKey
                        }
                        SqlImvApplyKeySourceFacts::GroupRowId => {
                            crate::planner::vocabulary::ApplyKeySource::GroupRowId
                        }
                    },
                },
                partition: partition.map(|facts| facts.inner),
            },
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlImvBranchContractFacts {
    inner: SqlImvBranchContract,
}

impl SqlImvBranchContractFacts {
    pub fn try_new(branch_id_column_name: String) -> Result<Self, String> {
        if branch_id_column_name.trim().is_empty() {
            return Err("IMV branch contract facts are invalid".to_string());
        }
        Ok(Self {
            inner: SqlImvBranchContract {
                branch_id_column_name,
            },
        })
    }
}

/// Fully validated value-only image of the persisted MV schema contract.
/// The wrapper does not provide a raw-contract accessor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlImvSchemaContractFacts {
    inner: SqlImvSchemaContract,
}

impl SqlImvSchemaContractFacts {
    pub fn try_new(
        bases: Vec<SqlImvBaseContractFacts>,
        output_columns: Vec<SqlImvOutputColumnFacts>,
        join: Option<SqlImvJoinContractFacts>,
        aggregate: Option<SqlImvAggregateContractFacts>,
        branch: Option<SqlImvBranchContractFacts>,
        target: SqlImvTargetContractFacts,
    ) -> Result<Self, String> {
        if bases.is_empty()
            || output_columns.is_empty()
            || bases.iter().enumerate().any(|(index, base)| {
                bases[..index]
                    .iter()
                    .any(|other| other.inner.occurrence_id == base.inner.occurrence_id)
            })
        {
            return Err("IMV schema contract facts are incomplete or duplicate".to_string());
        }
        if join.is_some() && bases.len() < 2 {
            return Err("IMV join contract requires at least two bases".to_string());
        }
        let validate_field = |field: &SqlImvQualifiedFieldLineage| -> Result<(), String> {
            let base = bases
                .iter()
                .find(|base| base.inner.occurrence_id == field.occurrence_id)
                .ok_or_else(|| {
                    "IMV lineage references an unknown definition occurrence".to_string()
                })?;
            if base.inner.table_fqn != field.table_fqn
                || base
                    .inner
                    .alias_at_create
                    .as_ref()
                    .is_some_and(|alias| alias != &field.qualifier_at_create)
                || !base
                    .inner
                    .fields
                    .iter()
                    .any(|known| known.field_id == field.field_id)
            {
                return Err(
                    "IMV lineage differs from its occurrence-qualified schema facts".to_string(),
                );
            }
            Ok(())
        };
        for output in &output_columns {
            for field in &output.inner.expression.referenced_base_fields {
                validate_field(field)?;
            }
        }
        if let Some(join) = &join {
            for predicate in &join.inner.predicates {
                validate_field(&predicate.left)?;
                validate_field(&predicate.right)?;
            }
        }
        Ok(Self {
            inner: SqlImvSchemaContract {
                bases: bases.into_iter().map(|facts| facts.inner).collect(),
                output_columns: output_columns
                    .into_iter()
                    .map(|facts| facts.inner)
                    .collect(),
                join: join.map(|facts| facts.inner),
                aggregate: aggregate.map(|facts| facts.inner),
                branch: branch.map(|facts| facts.inner),
                target: target.inner,
            },
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlImvAggregateVisibleColumnFacts {
    inner: SqlImvAggregateVisibleColumn,
}

impl SqlImvAggregateVisibleColumnFacts {
    pub fn try_new(
        name: String,
        value_type: novarocks_type_contract::FunctionValueType,
    ) -> Result<Self, String> {
        if name.trim().is_empty() {
            return Err("IMV aggregate visible-column facts are invalid".to_string());
        }
        Ok(Self {
            inner: SqlImvAggregateVisibleColumn { name, value_type },
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlImvAggregateExecutionStateColumnFacts {
    inner: SqlImvAggregateStateColumn,
}

impl SqlImvAggregateExecutionStateColumnFacts {
    pub fn try_new(
        name: String,
        value_type: novarocks_type_contract::FunctionValueType,
        visible_source_index: usize,
        aggregate_index: usize,
        function: crate::mv_refresh::AggregateFunctionKind,
        state_role: SqlImvAggregateStateRoleFacts,
        count_star: bool,
    ) -> Result<Self, String> {
        if name.trim().is_empty() {
            return Err("IMV aggregate execution state-column facts are invalid".to_string());
        }
        Ok(Self {
            inner: SqlImvAggregateStateColumn {
                name,
                value_type,
                visible_source_index,
                aggregate_index,
                function,
                state_role: match state_role {
                    SqlImvAggregateStateRoleFacts::Single => SqlImvAggregateStateRole::Single,
                    SqlImvAggregateStateRoleFacts::AvgSum => SqlImvAggregateStateRole::AvgSum,
                    SqlImvAggregateStateRoleFacts::AvgCount => SqlImvAggregateStateRole::AvgCount,
                    SqlImvAggregateStateRoleFacts::RetractionCount => {
                        SqlImvAggregateStateRole::RetractionCount
                    }
                },
                count_star,
            },
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlImvAggregateExecutionFacts {
    inner: SqlImvAggregateExecutionLayout,
}

impl SqlImvAggregateExecutionFacts {
    pub fn try_new(
        group_key_count: usize,
        visible_outputs: Vec<crate::mv_refresh::VisibleAggregateOutput>,
        row_id_column_name: String,
        visible_columns: Vec<SqlImvAggregateVisibleColumnFacts>,
        state_columns: Vec<SqlImvAggregateExecutionStateColumnFacts>,
        group_key_source_indexes: Vec<usize>,
        physical_column_names: Vec<String>,
        aggregate_input_types: Vec<Option<arrow::datatypes::DataType>>,
    ) -> Result<Self, String> {
        if row_id_column_name.trim().is_empty()
            || visible_columns.is_empty()
            || state_columns.is_empty()
            || physical_column_names.is_empty()
            || physical_column_names
                .iter()
                .any(|name| name.trim().is_empty())
        {
            return Err("IMV aggregate execution facts are incomplete".to_string());
        }
        Ok(Self {
            inner: SqlImvAggregateExecutionLayout {
                shape: SqlImvAggregateShape {
                    group_key_count,
                    visible_outputs,
                },
                layout: SqlImvAggregateLayout {
                    row_id_column_name,
                    visible_columns: visible_columns
                        .into_iter()
                        .map(|facts| facts.inner)
                        .collect(),
                    state_columns: state_columns.into_iter().map(|facts| facts.inner).collect(),
                    group_key_source_indexes,
                    physical_column_names,
                    aggregate_input_types,
                },
            },
        })
    }
}

/// Immutable, query-scoped facts consumed by incremental-MV rewrite rules.
///
/// This is the SQL boundary for refresh planning.  It intentionally contains
/// no repository, connector table, metadata payload, lease, callback, or
/// application context.  The persisted schema contract is still carried as a
/// value until its persistence vocabulary moves under the SQL owner; the
/// compiler never uses it to access application state.
#[derive(Clone, Debug)]
pub(crate) struct SqlImvRewriteSnapshot {
    pub(crate) target: novarocks_types::naming::TableIdentity,
    /// Exact request-local target materialization. Every target-state and
    /// target-locator scan produced by the rewrite carries this token, so
    /// preparation cannot silently reacquire a newer target generation.
    pub(crate) target_binding: SqlTableBindingId,
    #[allow(
        dead_code,
        reason = "Retained for staged SQL planner migration consumers and test helpers."
    )]
    pub(crate) mv_id: i64,
    pub(crate) base_snapshots: Arc<[SqlImvBaseSnapshot]>,
    pub(crate) previous_snapshot_ids: BTreeMap<SqlMvRelationOccurrenceId, i64>,
    #[allow(
        dead_code,
        reason = "Retained for staged SQL planner migration consumers and test helpers."
    )]
    pub(crate) previous_table_object_ids:
        BTreeMap<SqlMvRelationOccurrenceId, ConnectorTableObjectId>,
    pub(crate) target_snapshot_id: Option<i64>,
    pub(crate) target_table_uuid: String,
    /// SQL-safe target field facts projected by the application.  This avoids
    /// exposing an Iceberg schema or Iceberg default-literal values to SQL.
    pub(crate) target_columns: Arc<[novarocks_types::schema::ColumnDef]>,
    pub(crate) target_file_metadata: Option<Arc<[novarocks_types::schema::ColumnDef; 2]>>,
    /// SQL projection of the persisted MV planning contract frozen at
    /// admission. Resolving or mutating the serialized contract remains in
    /// the application facade.
    pub(crate) schema_contract: Arc<SqlImvSchemaContract>,
    /// Aggregate shape/layout was derived from the admitted MV definition by
    /// application before compiler entry.  Non-aggregate refreshes use None.
    pub(crate) aggregate_execution: Option<SqlImvAggregateExecutionLayout>,
}

impl SqlImvRewriteSnapshot {
    #[expect(
        clippy::too_many_arguments,
        reason = "The constructor validates each frozen field against occurrence identity before publishing the snapshot; a bundle would obscure those cross-field checks."
    )]
    pub(crate) fn from_frozen_parts(
        target: novarocks_types::naming::TableIdentity,
        target_binding: SqlTableBindingId,
        mv_id: i64,
        base_snapshots: Arc<[SqlImvBaseSnapshot]>,
        previous_snapshot_ids: BTreeMap<SqlMvRelationOccurrenceId, i64>,
        previous_table_object_ids: BTreeMap<SqlMvRelationOccurrenceId, ConnectorTableObjectId>,
        target_snapshot_id: Option<i64>,
        target_table_uuid: String,
        target_columns: Arc<[novarocks_types::schema::ColumnDef]>,
        schema_contract: Arc<SqlImvSchemaContract>,
        aggregate_execution: Option<SqlImvAggregateExecutionLayout>,
    ) -> Result<Self, String> {
        if base_snapshots.is_empty() {
            return Err("IMV rewrite snapshot has no base table snapshots".to_string());
        }
        let ids = base_snapshots
            .iter()
            .map(|base| base.occurrence_id)
            .collect::<std::collections::BTreeSet<_>>();
        if ids.len() != base_snapshots.len()
            || previous_snapshot_ids
                .keys()
                .ne(previous_table_object_ids.keys())
            || previous_snapshot_ids.keys().any(|id| !ids.contains(id))
            || (!previous_snapshot_ids.is_empty() && previous_snapshot_ids.len() != ids.len())
        {
            return Err(
                "IMV refresh history must cover every and only definition occurrence".to_string(),
            );
        }
        for base in base_snapshots.iter() {
            if base.table_object_id.as_bytes().is_empty() {
                return Err(format!(
                    "IMV rewrite snapshot base {} has an empty table UUID",
                    base.table.fqn()
                ));
            }
            if let Some(previous_object_id) = previous_table_object_ids.get(&base.occurrence_id)
                && previous_object_id != &base.table_object_id
            {
                return Err(format!(
                    "base table identity changed for {}; incremental refresh unsafe, rebuild the MV",
                    base.table.fqn()
                ));
            }
        }
        if target_columns.is_empty() {
            return Err("IMV rewrite snapshot target has no SQL column facts".to_string());
        }
        Ok(Self {
            target,
            target_binding,
            mv_id,
            base_snapshots,
            previous_snapshot_ids,
            previous_table_object_ids,
            target_snapshot_id,
            target_table_uuid,
            target_columns,
            target_file_metadata: None,
            schema_contract,
            aggregate_execution,
        })
    }

    pub(crate) fn aggregate_shape_and_layout_for_execution(
        &self,
    ) -> Result<(SqlImvAggregateShape, SqlImvAggregateLayout), String> {
        self.aggregate_execution
            .as_ref()
            .map(|layout| (layout.shape.clone(), layout.layout.clone()))
            .ok_or_else(|| {
                format!(
                    "IMV rewrite snapshot for {} has no aggregate execution layout",
                    self.target.fqn()
                )
            })
    }

    pub(crate) fn base_snapshot_for_occurrence(
        &self,
        occurrence: SqlMvRelationOccurrenceId,
    ) -> Option<&SqlImvBaseSnapshot> {
        self.base_snapshots
            .iter()
            .find(|base| base.occurrence_id == occurrence)
    }
}

#[cfg(any(test, novarocks_dev_controls))]
pub(crate) fn test_target_binding() -> SqlTableBindingId {
    use std::num::{NonZeroU32, NonZeroU64};

    SqlTableBindingId::new(
        crate::binding::SqlTableBindingScopeId::new(NonZeroU64::new(1).unwrap()),
        NonZeroU32::new(1).unwrap(),
    )
}

#[cfg(any(test, novarocks_dev_controls))]
fn test_object_id(value: &str) -> ConnectorTableObjectId {
    ConnectorTableObjectId::try_new(bytes::Bytes::copy_from_slice(value.as_bytes()))
        .expect("test object ID")
}

/// SQL-only incremental IMV fixture for rewrite-rule tests that need an
/// extension payload but do not exercise application persistence conversion.
///
/// The prior and admitted snapshots deliberately describe one exact
/// incremental window. Tests that exercise delta or version rewriting must
/// never rely on a synthetic first-refresh fallback.
#[cfg(any(test, novarocks_dev_controls))]
pub(crate) fn test_incremental_snapshot() -> Arc<SqlImvRewriteSnapshot> {
    let base = novarocks_types::naming::TableIdentity::new("ice", "db", "b");
    let target = novarocks_types::naming::TableIdentity::new("ice", "db", "mv");
    let mut previous_snapshot_ids = BTreeMap::new();
    previous_snapshot_ids.insert(SqlMvRelationOccurrenceId::new(7), 11);
    let mut previous_table_object_ids = BTreeMap::new();
    previous_table_object_ids.insert(
        SqlMvRelationOccurrenceId::new(7),
        test_object_id("object-b"),
    );
    Arc::new(
        SqlImvRewriteSnapshot::from_frozen_parts(
            target,
            test_target_binding(),
            1,
            Arc::from(vec![SqlImvBaseSnapshot {
                occurrence_id: SqlMvRelationOccurrenceId::new(7),
                table: base,
                qualifier_at_binding: "b".to_string(),
                snapshot_id: 22,
                table_object_id: test_object_id("object-b"),
            }]),
            previous_snapshot_ids,
            previous_table_object_ids,
            Some(1),
            "target-uuid".to_string(),
            Arc::from(vec![
                novarocks_types::schema::ColumnDef {
                    name: "k".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                novarocks_types::schema::ColumnDef {
                    name: "__nova_base_row_id".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
            ]),
            Arc::new(SqlImvSchemaContract {
                bases: Vec::new(),
                output_columns: Vec::new(),
                join: None,
                aggregate: None,
                branch: None,
                target: SqlImvTargetContract {
                    visible_columns: Vec::new(),
                    hidden_apply_key: SqlImvHiddenApplyKey {
                        column_name: "__nova_base_row_id".to_string(),
                        source: crate::planner::vocabulary::ApplyKeySource::BaseRowId,
                    },
                    partition: None,
                },
            }),
            None,
        )
        .expect("SQL-only test IMV snapshot"),
    )
}

#[cfg(any(test, novarocks_dev_controls))]
#[allow(
    dead_code,
    reason = "The snapshot-handle fixture remains available to focused SQL MV rewrite tests."
)]
pub(crate) fn test_incremental_snapshot_handle() -> SqlImvRewriteSnapshotHandle {
    SqlImvRewriteSnapshotHandle(test_incremental_snapshot())
}

/// SQL-only scan fixture for rewrite-rule tests.  Test plans must exercise the
/// same tokenized scan vocabulary as production compiler artifacts; connector
/// table metadata belongs to application-owned preparation tests.
#[cfg(any(test, novarocks_dev_controls))]
#[allow(
    dead_code,
    reason = "The generic tokenized scan fixture remains available to focused SQL MV rewrite tests."
)]
pub(crate) fn test_scan_source(kind: crate::planner::table::SqlScanKind) -> ScanSource {
    test_scan_source_for("ice", "db", "b", kind)
}

/// SQL-only scan fixture with an explicit canonical table identity. Tests
/// comparing physical table identity must not collapse unrelated tables into
/// the shared default fixture identity.
#[cfg(any(test, novarocks_dev_controls))]
#[allow(
    dead_code,
    reason = "The explicit-table scan fixture remains available to physical identity rewrite tests."
)]
pub(crate) fn test_scan_source_for(
    catalog: &str,
    namespace: &str,
    table: &str,
    kind: crate::planner::table::SqlScanKind,
) -> ScanSource {
    ScanSource::Sql(
        crate::planner::table::SqlScanSource::new(
            test_target_binding(),
            crate::planner::table::SqlTableIdentity {
                catalog: catalog.to_string(),
                namespace: namespace.to_string(),
                table: table.to_string(),
            },
            kind,
        )
        .with_mv_occurrence(SqlMvRelationOccurrenceId::new(if table == "r" {
            42
        } else {
            7
        })),
    )
}

#[cfg(any(test, novarocks_dev_controls))]
#[allow(
    dead_code,
    reason = "The current-version scan fixture remains available to focused SQL MV rewrite tests."
)]
pub(crate) fn test_data_scan_source() -> ScanSource {
    test_scan_source(crate::planner::table::SqlScanKind::Data {
        version: crate::planner::table::SqlTableVersionSelector::Current,
    })
}

#[cfg(any(test, novarocks_dev_controls))]
#[allow(
    dead_code,
    reason = "The named current-version scan fixture remains available to identity rewrite tests."
)]
pub(crate) fn test_data_scan_source_for(catalog: &str, namespace: &str, table: &str) -> ScanSource {
    test_scan_source_for(
        catalog,
        namespace,
        table,
        crate::planner::table::SqlScanKind::Data {
            version: crate::planner::table::SqlTableVersionSelector::Current,
        },
    )
}

#[cfg(any(test, novarocks_dev_controls))]
#[allow(
    dead_code,
    reason = "The delta scan fixture remains available to incremental SQL MV rewrite tests."
)]
pub(crate) fn test_delta_scan_source(from_snapshot_id: i64, to_snapshot_id: i64) -> ScanSource {
    test_scan_source(crate::planner::table::SqlScanKind::Delta {
        from_snapshot_id,
        to_snapshot_id,
    })
}

/// Build aggregate-refresh facts without persisted records or connector
/// metadata. Rule tests vary these compiler-facing values directly.
#[cfg(any(test, novarocks_dev_controls))]
#[allow(
    dead_code,
    reason = "The aggregate snapshot fixture remains available to aggregate rewrite-rule tests."
)]
pub(crate) fn test_aggregate_snapshot(
    state_columns: Vec<SqlImvAggregateStateColumnContract>,
    partition: Option<SqlImvPartitionContract>,
    branch: Option<SqlImvBranchContract>,
) -> Arc<SqlImvRewriteSnapshot> {
    let mut snapshot = (*test_incremental_snapshot()).clone();
    snapshot.schema_contract = Arc::new(SqlImvSchemaContract {
        bases: vec![SqlImvBaseContract {
            occurrence_id: SqlMvRelationOccurrenceId::new(7),
            table_fqn: "ice.db.b".to_string(),
            alias_at_create: None,
            fields: vec![
                SqlImvBaseField {
                    field_id: bytes::Bytes::from_static(b"field-1"),
                    name_at_create: "k".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                },
                SqlImvBaseField {
                    field_id: bytes::Bytes::from_static(b"field-2"),
                    name_at_create: "v".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: true,
                },
            ],
        }],
        output_columns: Vec::new(),
        join: None,
        aggregate: Some(SqlImvAggregateContract {
            state_layout_version: 1,
            row_id_column_name: "__row_id__".to_string(),
            state_columns: state_columns.clone(),
        }),
        branch,
        target: SqlImvTargetContract {
            visible_columns: vec![
                SqlImvTargetVisibleColumn {
                    output_name: "k".to_string(),
                    target_field_id: bytes::Bytes::from_static(b"field-100"),
                },
                SqlImvTargetVisibleColumn {
                    output_name: "s".to_string(),
                    target_field_id: bytes::Bytes::from_static(b"field-101"),
                },
            ],
            hidden_apply_key: SqlImvHiddenApplyKey {
                column_name: "__row_id__".to_string(),
                source: crate::planner::vocabulary::ApplyKeySource::GroupRowId,
            },
            partition,
        },
    });
    snapshot.aggregate_execution = Some(SqlImvAggregateExecutionLayout {
        shape: SqlImvAggregateShape {
            group_key_count: 1,
            visible_outputs: vec![
                crate::mv_refresh::VisibleAggregateOutput::GroupKey(0),
                crate::mv_refresh::VisibleAggregateOutput::Aggregate(0),
            ],
        },
        layout: SqlImvAggregateLayout {
            row_id_column_name: "__row_id__".to_string(),
            visible_columns: vec![
                SqlImvAggregateVisibleColumn {
                    name: "k".to_string(),
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        arrow::datatypes::DataType::Int64,
                        false,
                    ),
                },
                SqlImvAggregateVisibleColumn {
                    name: "s".to_string(),
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        arrow::datatypes::DataType::Int64,
                        true,
                    ),
                },
            ],
            state_columns: state_columns
                .iter()
                .enumerate()
                .map(|(index, column)| SqlImvAggregateStateColumn {
                    name: column.column_name.clone(),
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        if column.type_signature == "long" {
                            arrow::datatypes::DataType::Int64
                        } else {
                            arrow::datatypes::DataType::Binary
                        },
                        column.role == SqlImvAggregateStateRoleContract::Single,
                    ),

                    visible_source_index: 1,
                    aggregate_index: index,
                    function: crate::mv_refresh::AggregateFunctionKind::Sum,
                    state_role: match column.role {
                        SqlImvAggregateStateRoleContract::Single => {
                            SqlImvAggregateStateRole::Single
                        }
                        SqlImvAggregateStateRoleContract::AvgSum => {
                            SqlImvAggregateStateRole::AvgSum
                        }
                        SqlImvAggregateStateRoleContract::AvgCount => {
                            SqlImvAggregateStateRole::AvgCount
                        }
                        SqlImvAggregateStateRoleContract::RetractionCount => {
                            SqlImvAggregateStateRole::RetractionCount
                        }
                    },
                    count_star: false,
                })
                .collect(),
            group_key_source_indexes: vec![0],
            physical_column_names: state_columns
                .iter()
                .map(|column| column.column_name.clone())
                .collect(),
            aggregate_input_types: state_columns
                .iter()
                .map(|_| Some(arrow::datatypes::DataType::Int64))
                .collect(),
        },
    });
    let mut target_columns = vec![
        novarocks_types::schema::ColumnDef {
            name: "k".to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
        novarocks_types::schema::ColumnDef {
            name: "s".to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: true,
            write_default: None,
            logical_type: None,
        },
        novarocks_types::schema::ColumnDef {
            name: "__row_id__".to_string(),
            data_type: arrow::datatypes::DataType::Utf8,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
    ];
    target_columns.extend(
        state_columns
            .iter()
            .map(|column| novarocks_types::schema::ColumnDef {
                name: column.column_name.clone(),
                data_type: if column.type_signature == "long" {
                    arrow::datatypes::DataType::Int64
                } else {
                    arrow::datatypes::DataType::Binary
                },
                nullable: column.role == SqlImvAggregateStateRoleContract::Single,
                write_default: None,
                logical_type: None,
            }),
    );
    if let Some(branch) = snapshot.schema_contract.branch.as_ref() {
        target_columns.push(novarocks_types::schema::ColumnDef {
            name: branch.branch_id_column_name.clone(),
            data_type: arrow::datatypes::DataType::Int32,
            nullable: false,
            write_default: None,
            logical_type: None,
        });
    }
    snapshot.target_columns = Arc::from(target_columns);
    Arc::new(snapshot)
}

/// SQL-owned join fixture for rewrite rules. It has no persistence, provider,
/// or application-context dependency.
#[cfg(test)]
pub(crate) fn test_join_snapshot(aggregate: bool) -> Arc<SqlImvRewriteSnapshot> {
    let qualified =
        |table_fqn: &str, qualifier_at_create: &str, field_id: u32| SqlImvQualifiedFieldLineage {
            occurrence_id: SqlMvRelationOccurrenceId::new(if qualifier_at_create == "r" {
                42
            } else {
                7
            }),
            table_fqn: table_fqn.to_string(),
            qualifier_at_create: qualifier_at_create.to_string(),
            field_id: bytes::Bytes::from(format!("field-{field_id}")),
        };
    let base_contract = |table_fqn: &str, alias_at_create: &str| SqlImvBaseContract {
        occurrence_id: SqlMvRelationOccurrenceId::new(if alias_at_create == "r" { 42 } else { 7 }),
        table_fqn: table_fqn.to_string(),
        alias_at_create: Some(alias_at_create.to_string()),
        fields: vec![
            SqlImvBaseField {
                field_id: bytes::Bytes::from_static(b"field-1"),
                name_at_create: "k".to_string(),
                data_type: arrow::datatypes::DataType::Int64,
                nullable: false,
            },
            SqlImvBaseField {
                field_id: bytes::Bytes::from_static(b"field-2"),
                name_at_create: "v".to_string(),
                data_type: arrow::datatypes::DataType::Int64,
                nullable: true,
            },
        ],
    };
    let state_columns = vec![
        SqlImvAggregateStateColumnContract {
            column_name: "__agg_state_s".to_string(),
            type_signature: "binary".to_string(),
            role: SqlImvAggregateStateRoleContract::Single,
        },
        SqlImvAggregateStateColumnContract {
            column_name: "__agg_state___ivm_row_count".to_string(),
            type_signature: "long".to_string(),
            role: SqlImvAggregateStateRoleContract::RetractionCount,
        },
    ];
    let schema_contract = Arc::new(SqlImvSchemaContract {
        bases: vec![
            base_contract("ice.db.l", "l"),
            base_contract("ice.db.r", "r"),
        ],
        output_columns: vec![
            SqlImvOutputColumnLineage {
                expression: SqlImvExpressionLineage {
                    kind: SqlImvExpressionKind::Column,
                    referenced_base_fields: vec![qualified("ice.db.l", "l", 1)],
                },
            },
            SqlImvOutputColumnLineage {
                expression: SqlImvExpressionLineage {
                    kind: SqlImvExpressionKind::Column,
                    referenced_base_fields: vec![qualified("ice.db.r", "r", 2)],
                },
            },
        ],
        join: Some(SqlImvJoinContract {
            kind: SqlImvJoinContractKind::InnerEquiJoin,
            predicates: vec![SqlImvJoinPredicateLineage {
                left: qualified("ice.db.l", "l", 1),
                right: qualified("ice.db.r", "r", 1),
            }],
        }),
        aggregate: aggregate.then(|| SqlImvAggregateContract {
            state_layout_version: 1,
            row_id_column_name: "__row_id__".to_string(),
            state_columns: state_columns.clone(),
        }),
        branch: Some(SqlImvBranchContract {
            branch_id_column_name: "__branch_id__".to_string(),
        }),
        target: SqlImvTargetContract {
            visible_columns: vec![
                SqlImvTargetVisibleColumn {
                    output_name: "k".to_string(),
                    target_field_id: bytes::Bytes::from_static(b"field-100"),
                },
                SqlImvTargetVisibleColumn {
                    output_name: "s".to_string(),
                    target_field_id: bytes::Bytes::from_static(b"field-101"),
                },
            ],
            hidden_apply_key: SqlImvHiddenApplyKey {
                column_name: "__row_id__".to_string(),
                source: crate::planner::vocabulary::ApplyKeySource::GroupRowId,
            },
            partition: None,
        },
    });
    let aggregate_execution = aggregate.then(|| SqlImvAggregateExecutionLayout {
        shape: SqlImvAggregateShape {
            group_key_count: 1,
            visible_outputs: vec![
                crate::mv_refresh::VisibleAggregateOutput::GroupKey(0),
                crate::mv_refresh::VisibleAggregateOutput::Aggregate(0),
            ],
        },
        layout: SqlImvAggregateLayout {
            row_id_column_name: "__row_id__".to_string(),
            visible_columns: vec![
                SqlImvAggregateVisibleColumn {
                    name: "k".to_string(),
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        arrow::datatypes::DataType::Int64,
                        false,
                    ),
                },
                SqlImvAggregateVisibleColumn {
                    name: "s".to_string(),
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        arrow::datatypes::DataType::Int64,
                        true,
                    ),
                },
            ],
            state_columns: state_columns
                .iter()
                .enumerate()
                .map(|(aggregate_index, column)| SqlImvAggregateStateColumn {
                    name: column.column_name.clone(),
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        if column.type_signature == "long" {
                            arrow::datatypes::DataType::Int64
                        } else {
                            arrow::datatypes::DataType::Binary
                        },
                        column.role == SqlImvAggregateStateRoleContract::Single,
                    ),

                    visible_source_index: 1,
                    aggregate_index,
                    function: crate::mv_refresh::AggregateFunctionKind::Sum,
                    state_role: match column.role {
                        SqlImvAggregateStateRoleContract::Single => {
                            SqlImvAggregateStateRole::Single
                        }
                        SqlImvAggregateStateRoleContract::AvgSum => {
                            SqlImvAggregateStateRole::AvgSum
                        }
                        SqlImvAggregateStateRoleContract::AvgCount => {
                            SqlImvAggregateStateRole::AvgCount
                        }
                        SqlImvAggregateStateRoleContract::RetractionCount => {
                            SqlImvAggregateStateRole::RetractionCount
                        }
                    },
                    count_star: false,
                })
                .collect(),
            group_key_source_indexes: vec![0],
            physical_column_names: state_columns
                .iter()
                .map(|column| column.column_name.clone())
                .collect(),
            aggregate_input_types: state_columns
                .iter()
                .map(|_| Some(arrow::datatypes::DataType::Int64))
                .collect(),
        },
    });
    let mut target_columns = vec![
        novarocks_types::schema::ColumnDef {
            name: "k".to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
        novarocks_types::schema::ColumnDef {
            name: "s".to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: true,
            write_default: None,
            logical_type: None,
        },
        novarocks_types::schema::ColumnDef {
            name: "__row_id__".to_string(),
            data_type: arrow::datatypes::DataType::Utf8,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
        novarocks_types::schema::ColumnDef {
            name: "__branch_id__".to_string(),
            data_type: arrow::datatypes::DataType::Int32,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
    ];
    target_columns.extend(
        state_columns
            .iter()
            .map(|column| novarocks_types::schema::ColumnDef {
                name: column.column_name.clone(),
                data_type: if column.type_signature == "long" {
                    arrow::datatypes::DataType::Int64
                } else {
                    arrow::datatypes::DataType::Binary
                },
                nullable: column.role == SqlImvAggregateStateRoleContract::Single,
                write_default: None,
                logical_type: None,
            }),
    );
    Arc::new(
        SqlImvRewriteSnapshot::from_frozen_parts(
            novarocks_types::naming::TableIdentity::new("ice", "db", "mv"),
            test_target_binding(),
            42,
            Arc::from(vec![
                SqlImvBaseSnapshot {
                    occurrence_id: SqlMvRelationOccurrenceId::new(7),
                    table: novarocks_types::naming::TableIdentity::new("ice", "db", "l"),
                    qualifier_at_binding: "l".to_string(),
                    snapshot_id: 22,
                    table_object_id: test_object_id("object-l"),
                },
                SqlImvBaseSnapshot {
                    occurrence_id: SqlMvRelationOccurrenceId::new(42),
                    table: novarocks_types::naming::TableIdentity::new("ice", "db", "r"),
                    qualifier_at_binding: "r".to_string(),
                    snapshot_id: 44,
                    table_object_id: test_object_id("object-r"),
                },
            ]),
            BTreeMap::from([
                (SqlMvRelationOccurrenceId::new(7), 11),
                (SqlMvRelationOccurrenceId::new(42), 33),
            ]),
            BTreeMap::from([
                (
                    SqlMvRelationOccurrenceId::new(7),
                    test_object_id("object-l"),
                ),
                (
                    SqlMvRelationOccurrenceId::new(42),
                    test_object_id("object-r"),
                ),
            ]),
            Some(99),
            "uuid-tgt".to_string(),
            Arc::from(target_columns),
            schema_contract,
            aggregate_execution,
        )
        .expect("SQL-only join test snapshot"),
    )
}

#[cfg(test)]
pub(crate) fn test_branch_union_snapshot() -> Arc<SqlImvRewriteSnapshot> {
    let mut snapshot = (*test_aggregate_snapshot(
        vec![
            SqlImvAggregateStateColumnContract {
                column_name: "__agg_state_s".to_string(),
                type_signature: "binary".to_string(),
                role: SqlImvAggregateStateRoleContract::Single,
            },
            SqlImvAggregateStateColumnContract {
                column_name: "__agg_state___ivm_row_count".to_string(),
                type_signature: "long".to_string(),
                role: SqlImvAggregateStateRoleContract::RetractionCount,
            },
        ],
        None,
        Some(SqlImvBranchContract {
            branch_id_column_name: "__branch_id__".to_string(),
        }),
    ))
    .clone();
    snapshot.schema_contract = Arc::new(SqlImvSchemaContract {
        bases: vec![SqlImvBaseContract {
            occurrence_id: SqlMvRelationOccurrenceId::new(7),
            table_fqn: "ice.db.b".to_string(),
            alias_at_create: None,
            fields: vec![
                SqlImvBaseField {
                    field_id: bytes::Bytes::from_static(b"field-1"),
                    name_at_create: "region".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                },
                SqlImvBaseField {
                    field_id: bytes::Bytes::from_static(b"field-2"),
                    name_at_create: "amount".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                },
            ],
        }],
        output_columns: Vec::new(),
        join: None,
        aggregate: snapshot.schema_contract.aggregate.clone(),
        branch: snapshot.schema_contract.branch.clone(),
        target: SqlImvTargetContract {
            visible_columns: vec![
                SqlImvTargetVisibleColumn {
                    output_name: "region".to_string(),
                    target_field_id: bytes::Bytes::from_static(b"field-100"),
                },
                SqlImvTargetVisibleColumn {
                    output_name: "s".to_string(),
                    target_field_id: bytes::Bytes::from_static(b"field-101"),
                },
            ],
            hidden_apply_key: SqlImvHiddenApplyKey {
                column_name: "__row_id__".to_string(),
                source: crate::planner::vocabulary::ApplyKeySource::GroupRowId,
            },
            partition: None,
        },
    });
    if let Some(layout) = snapshot.aggregate_execution.as_mut() {
        layout.layout.visible_columns[0].name = "region".to_string();
        layout.layout.visible_columns[1].name = "s".to_string();
    }
    snapshot.target_columns = Arc::from(vec![
        novarocks_types::schema::ColumnDef {
            name: "region".to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
        novarocks_types::schema::ColumnDef {
            name: "s".to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: true,
            write_default: None,
            logical_type: None,
        },
        novarocks_types::schema::ColumnDef {
            name: "__row_id__".to_string(),
            data_type: arrow::datatypes::DataType::Utf8,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
        novarocks_types::schema::ColumnDef {
            name: "__agg_state_s".to_string(),
            data_type: arrow::datatypes::DataType::Binary,
            nullable: true,
            write_default: None,
            logical_type: None,
        },
        novarocks_types::schema::ColumnDef {
            name: "__agg_state___ivm_row_count".to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
        novarocks_types::schema::ColumnDef {
            name: "__branch_id__".to_string(),
            data_type: arrow::datatypes::DataType::Int32,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
    ]);
    Arc::new(snapshot)
}

/// SQL-only aggregate join fixture whose visible group key follows the
/// branch-union test plans. The base-table and target-state identities remain
/// the same immutable join snapshot facts.
#[cfg(test)]
pub(crate) fn test_region_join_snapshot() -> Arc<SqlImvRewriteSnapshot> {
    let mut snapshot = (*test_join_snapshot(true)).clone();
    Arc::make_mut(&mut snapshot.schema_contract)
        .target
        .visible_columns[0]
        .output_name = "region".to_string();
    if let Some(layout) = snapshot.aggregate_execution.as_mut() {
        layout.layout.visible_columns[0].name = "region".to_string();
    }
    snapshot.target_columns = Arc::from(
        snapshot
            .target_columns
            .iter()
            .cloned()
            .map(|mut column| {
                if column.name.eq_ignore_ascii_case("k") {
                    column.name = "region".to_string();
                }
                column
            })
            .collect::<Vec<_>>(),
    );
    Arc::new(snapshot)
}

/// The maximum number of successfully prepared candidates considered by one
/// statement. Failed or stale definitions do not consume this budget.
pub(crate) const MAX_SUCCESSFUL_MV_REWRITE_CANDIDATES: usize = 16;

/// An optional-rewrite failure recorded by the SQL kernel. The application
/// owns logging policy and may render these diagnostics without handing the
/// compiler an ambient logger.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlMvRewriteDiagnostic {
    pub(crate) mv_id: Option<i64>,
    pub(crate) message: String,
}

pub(crate) struct SqlMvRewritePreparation {
    pub(crate) constant_policy: novarocks_functions::ConstantPolicy,
    pub(crate) candidates: Vec<MvRewriteCandidate>,
    pub(crate) diagnostics: Vec<SqlMvRewriteDiagnostic>,
}

/// One complete immutable source observation. Its unavailable alternative is
/// explicit; object identity and data version can never be supplied separately.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlMvRewriteBaseTableFacts {
    state: SqlMvRewriteBaseTableFactsState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SqlMvRewriteBaseTableFactsState {
    Resolved(ConnectorExactSemanticRevision),
    Unavailable(String),
}

impl SqlMvRewriteBaseTableFacts {
    pub fn resolved(revision: ConnectorExactSemanticRevision) -> Self {
        Self {
            state: SqlMvRewriteBaseTableFactsState::Resolved(revision),
        }
    }

    pub fn unavailable(message: String) -> Self {
        Self {
            state: SqlMvRewriteBaseTableFactsState::Unavailable(message),
        }
    }
}

/// The namespace in which D was analyzed, independent of the querying session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlMvDefinitionResolutionContext {
    default_catalog: String,
    default_namespace: String,
}

impl SqlMvDefinitionResolutionContext {
    pub fn try_new(default_catalog: String, default_namespace: String) -> Result<Self, String> {
        if default_catalog.trim().is_empty() || default_namespace.trim().is_empty() {
            return Err("MV definition resolution context is incomplete".to_string());
        }
        Ok(Self {
            default_catalog,
            default_namespace,
        })
    }
}

/// One D occurrence and the source observations frozen for it. Names address
/// catalog lookups only; neither names nor observation equality merge occurrences.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlMvRewriteSourceOccurrenceFacts {
    occurrence_id: SqlMvRelationOccurrenceId,
    table: novarocks_types::naming::TableIdentity,
    qualifier_at_binding: String,
    published_revision: Option<ConnectorExactSemanticRevision>,
    observed_state: SqlMvRewriteBaseTableFacts,
}

impl SqlMvRewriteSourceOccurrenceFacts {
    pub fn try_new(
        occurrence_id: SqlMvRelationOccurrenceId,
        table: novarocks_types::naming::TableIdentity,
        qualifier_at_binding: String,
        published_revision: Option<ConnectorExactSemanticRevision>,
        observed_state: SqlMvRewriteBaseTableFacts,
    ) -> Result<Self, String> {
        if table.catalog.trim().is_empty()
            || table.namespace.trim().is_empty()
            || table.table.trim().is_empty()
            || qualifier_at_binding.trim().is_empty()
            || matches!(&observed_state.state, SqlMvRewriteBaseTableFactsState::Unavailable(message) if message.trim().is_empty())
        {
            return Err("MV rewrite source occurrence facts are incomplete".to_string());
        }
        Ok(Self {
            occurrence_id,
            table,
            qualifier_at_binding,
            published_revision,
            observed_state,
        })
    }

    pub const fn occurrence_id(&self) -> SqlMvRelationOccurrenceId {
        self.occurrence_id
    }
    pub const fn table(&self) -> &novarocks_types::naming::TableIdentity {
        &self.table
    }
}

/// Immutable D/P inputs for candidate preparation. The exact query input
/// receipts remain a separate query-owned proof, never persisted in this value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlMvRewriteDefinitionFacts {
    mv_id: i64,
    definition_revision: [u8; 32],
    select_query: Query,
    resolution: SqlMvDefinitionResolutionContext,
    storage_engine: String,
    target: Option<novarocks_types::naming::TableIdentity>,
    sources: Vec<SqlMvRewriteSourceOccurrenceFacts>,
    selection: Option<SqlMvRewriteSelectionFacts>,
    selection_unavailable: Option<String>,
}

impl SqlMvRewriteDefinitionFacts {
    pub fn try_new(
        mv_id: i64,
        definition_revision: [u8; 32],
        select_query: Query,
        resolution: SqlMvDefinitionResolutionContext,
        storage_engine: String,
        target: Option<novarocks_types::naming::TableIdentity>,
        sources: Vec<SqlMvRewriteSourceOccurrenceFacts>,
    ) -> Result<Self, String> {
        if mv_id < 0
            || definition_revision == [0; 32]
            || storage_engine.trim().is_empty()
            || sources.is_empty()
        {
            return Err("MV rewrite definition facts are incomplete".to_string());
        }
        let mut ids = std::collections::BTreeSet::new();
        if sources
            .iter()
            .any(|source| !ids.insert(source.occurrence_id))
        {
            return Err("MV rewrite definition repeats a relation occurrence".to_string());
        }
        if target.as_ref().is_some_and(|target| {
            target.catalog.trim().is_empty()
                || target.namespace.trim().is_empty()
                || target.table.trim().is_empty()
        }) {
            return Err("MV rewrite target identity is incomplete".to_string());
        }
        Ok(Self {
            mv_id,
            definition_revision,
            select_query,
            resolution,
            storage_engine,
            target,
            sources,
            selection: None,
            selection_unavailable: None,
        })
    }

    pub fn with_selection_facts(
        mut self,
        selection: SqlMvRewriteSelectionFacts,
    ) -> Result<Self, String> {
        if selection.definition_revision != self.definition_revision {
            return Err(
                "MV rewrite publication belongs to another definition revision".to_string(),
            );
        }
        if self.sources.len() != selection.publication_inputs.len()
            || self
                .sources
                .iter()
                .zip(&selection.publication_inputs)
                .any(|(source, input)| {
                    source.occurrence_id != input.occurrence_id
                        || source.table.fqn() != input.relation.table_fqn
                        || source.published_revision.as_ref() != Some(&input.relation.revision)
                })
        {
            return Err("MV rewrite publication must cover every definition occurrence in definition order with its exact revision".to_string());
        }
        if self
            .target
            .as_ref()
            .is_none_or(|target| target.fqn() != selection.publication_target.table_fqn)
        {
            return Err(
                "MV rewrite publication target differs from the definition target".to_string(),
            );
        }
        self.selection = Some(selection);
        self.selection_unavailable = None;
        Ok(self)
    }

    pub fn with_selection_unavailable(
        mut self,
        message: impl Into<String>,
    ) -> Result<Self, String> {
        let message = message.into();
        if message.trim().is_empty() {
            return Err("MV rewrite selection diagnostic cannot be empty".to_string());
        }
        self.selection = None;
        self.selection_unavailable = Some(message);
        Ok(self)
    }

    fn into_definition(self) -> MvRewriteDefinition {
        self
    }

    pub(super) fn completion_retained_bytes(&self) -> Result<u64, SqlCompileError> {
        let mut bytes = CompletionRetainedBytes::default();
        bytes.add(std::mem::size_of::<Self>());
        bytes.add_query(&self.select_query);
        bytes.add(self.resolution.default_catalog.capacity());
        bytes.add(self.resolution.default_namespace.capacity());
        bytes.add(self.storage_engine.capacity());
        if let Some(target) = &self.target {
            bytes.add_table_identity(target);
        }
        bytes.add_vec_capacity::<SqlMvRewriteSourceOccurrenceFacts>(self.sources.capacity());
        for source in &self.sources {
            bytes.add_table_identity(&source.table);
            bytes.add(source.qualifier_at_binding.capacity());
            if let Some(revision) = &source.published_revision {
                bytes.add_revision(revision);
            }
            match &source.observed_state.state {
                SqlMvRewriteBaseTableFactsState::Resolved(revision) => bytes.add_revision(revision),
                SqlMvRewriteBaseTableFactsState::Unavailable(message) => {
                    bytes.add(message.capacity())
                }
            }
        }
        if let Some(selection) = &self.selection {
            bytes.add(selection.publication_provenance.len());
            bytes.add_vec_capacity::<SqlMvRewritePublicationInput>(
                selection.publication_inputs.capacity(),
            );
            for input in &selection.publication_inputs {
                bytes.add(input.relation.table_fqn.capacity());
                bytes.add_revision(&input.relation.revision);
            }
            bytes.add(selection.publication_target.table_fqn.capacity());
            bytes.add_revision(&selection.publication_target.revision);
        }
        if let Some(message) = &self.selection_unavailable {
            bytes.add(message.capacity());
        }
        bytes.finish()
    }

    pub(super) const fn completion_mv_id(&self) -> i64 {
        self.mv_id
    }

    pub(super) fn completion_base_table_refs(&self) -> Vec<String> {
        self.sources
            .iter()
            .map(|source| source.table.fqn())
            .collect()
    }

    pub(super) fn completion_query_semantics_supported(&self) -> bool {
        // This is the existing default replay policy, not inferred durable
        // metadata. Visible unsupported hints make this optional candidate
        // ineligible before completion requests any of its catalog relations.
        crate::sql_mode::validate_persisted_query_semantics(
            &self.select_query,
            &crate::sql_mode::SqlSemanticSettings::default(),
        )
        .is_ok()
    }

    pub(super) fn completion_catalog_relations(
        &self,
    ) -> Vec<novarocks_types::naming::TableIdentity> {
        self.sources
            .iter()
            .map(|source| source.table.clone())
            .chain(self.target.iter().cloned())
            .collect()
    }
}

/// A checked, conservative memory ledger for parser-owned AST values. Each
/// visited semantic node is charged its inline representation and every
/// owned string is charged by capacity, so deeply nested or sparsely named
/// queries cannot evade the completion budget.
#[derive(Default)]
struct CompletionRetainedBytes {
    bytes: u64,
    overflowed: bool,
}

impl CompletionRetainedBytes {
    fn add_table_identity(&mut self, table: &novarocks_types::naming::TableIdentity) {
        self.add(table.catalog.capacity());
        self.add(table.namespace.capacity());
        self.add(table.table.capacity());
    }

    fn add_revision(&mut self, revision: &ConnectorExactSemanticRevision) {
        for fact in [revision.object_identity(), revision.data_version()] {
            self.add(fact.provider().as_str().len());
            self.add(fact.format().len());
            self.add(fact.value().len());
        }
    }

    fn add(&mut self, bytes: usize) {
        if self.overflowed {
            return;
        }
        let Some(bytes) = u64::try_from(bytes).ok() else {
            self.overflowed = true;
            return;
        };
        let Some(total) = self.bytes.checked_add(bytes) else {
            self.overflowed = true;
            return;
        };
        self.bytes = total;
    }

    fn add_vec_capacity<T>(&mut self, capacity: usize) {
        self.add(capacity.saturating_mul(std::mem::size_of::<T>()));
    }

    fn add_query(&mut self, query: &Query) {
        self.visit_query(query);
    }

    fn add_query_containers(&mut self, query: &Query) {
        self.add_vec_capacity::<novarocks_parser::ast::FunctionSpecification>(
            query.functions.capacity(),
        );
        self.add_vec_capacity::<novarocks_parser::ast::SessionProperty>(
            query.session_properties.capacity(),
        );
        for property in &query.session_properties {
            self.add_vec_capacity::<Ident>(property.name.parts.capacity());
        }
        self.add_order_by_containers(&query.modifiers.order_by);
        if let Some(with) = &query.with {
            self.add_vec_capacity::<novarocks_parser::ast::Cte>(with.ctes.capacity());
            for cte in &with.ctes {
                if let Some(columns) = &cte.columns {
                    self.add_vec_capacity::<Ident>(columns.capacity());
                }
            }
        }
        self.add_set_expr_containers(&query.body);
    }

    #[recursive::recursive]
    fn add_set_expr_containers(&mut self, set_expr: &SetExpr) {
        self.add(std::mem::size_of::<SetExpr>());
        match set_expr {
            SetExpr::Select(select) => self.add_select_containers(select),
            SetExpr::Values(values) => {
                self.add(std::mem::size_of::<novarocks_parser::ast::Values>());
                self.add_vec_capacity::<Expr>(values.rows.capacity());
            }
            SetExpr::Query(_) | SetExpr::Table(_) => {}
            SetExpr::SetOperation(operation) => {
                self.add(std::mem::size_of::<novarocks_parser::ast::SetOperation>());
                self.add_vec_capacity::<SetExpr>(operation.inputs.capacity());
                if let Some(corresponding) = &operation.corresponding {
                    self.add_vec_capacity::<Ident>(corresponding.columns.capacity());
                }
                for input in &operation.inputs {
                    self.add_set_expr_containers(input);
                }
            }
        }
    }

    fn add_select_containers(&mut self, select: &Select) {
        self.add_order_by_containers(&select.modifiers.order_by);
        self.add(std::mem::size_of::<Select>());
        self.add_vec_capacity::<SelectItem>(select.projection.capacity());
        for item in &select.projection {
            match item {
                SelectItem::SingleColumn(_) => {
                    self.add(std::mem::size_of::<novarocks_parser::ast::SingleColumn>())
                }
                SelectItem::AllColumns(columns) => {
                    self.add(std::mem::size_of::<novarocks_parser::ast::AllColumns>());
                    self.add_vec_capacity::<Ident>(columns.aliases.capacity());
                }
            }
        }
        self.add_vec_capacity::<novarocks_parser::ast::WindowDefinition>(select.windows.capacity());
        for window in &select.windows {
            self.add_window_spec_containers(&window.specification);
        }
    }

    fn add_order_by_containers(&mut self, order_by: &Option<novarocks_parser::ast::OrderBy>) {
        if let Some(order_by) = order_by {
            self.add_vec_capacity::<novarocks_parser::ast::SortItem>(order_by.items.capacity());
        }
    }

    fn add_window_spec_containers(&mut self, window: &WindowSpecification) {
        self.add_vec_capacity::<Expr>(window.partition_by.capacity());
        self.add_order_by_containers(&window.order_by);
        if let Some(frame) = &window.window_frame {
            self.add_vec_capacity::<novarocks_parser::ast::MeasureDefinition>(
                frame.measures.capacity(),
            );
            self.add_vec_capacity::<novarocks_parser::ast::SubsetDefinition>(
                frame.subsets.capacity(),
            );
            self.add_vec_capacity::<novarocks_parser::ast::VariableDefinition>(
                frame.variable_definitions.capacity(),
            );
        }
    }

    fn add_window_containers(&mut self, window: &novarocks_parser::ast::Window) {
        if let novarocks_parser::ast::Window::Specification(specification) = window {
            self.add(std::mem::size_of::<WindowSpecification>());
            self.add_window_spec_containers(specification);
        }
    }

    fn finish(self) -> Result<u64, SqlCompileError> {
        if self.overflowed {
            return Err(SqlCompileError::InvalidRequest(
                "materialized-view completion fact memory accounting overflowed".to_string(),
            ));
        }
        Ok(self.bytes)
    }
}

impl Visit for CompletionRetainedBytes {
    fn visit_subset_definition(&mut self, subset: &novarocks_parser::ast::SubsetDefinition) {
        self.add_vec_capacity::<Ident>(subset.identifiers.capacity());
        novarocks_parser::ast::walk_subset_definition(self, subset);
    }

    fn visit_pattern_integer(&mut self, value: &novarocks_parser::ast::PatternInteger) {
        self.add(value.lexeme.capacity());
        novarocks_parser::ast::walk_pattern_integer(self, value);
    }

    fn visit_row_pattern(&mut self, pattern: &novarocks_parser::ast::RowPattern) {
        use novarocks_parser::ast::RowPattern;
        match pattern {
            RowPattern::Alternation { patterns, .. }
            | RowPattern::Concatenation { patterns, .. }
            | RowPattern::Permutation { patterns, .. } => {
                self.add_vec_capacity::<RowPattern>(patterns.capacity())
            }
            RowPattern::Excluded { .. } | RowPattern::Quantified { .. } => {
                self.add(std::mem::size_of::<RowPattern>())
            }
            RowPattern::Variable { .. } | RowPattern::Empty { .. } | RowPattern::Anchor { .. } => {}
        }
        novarocks_parser::ast::walk_row_pattern(self, pattern);
    }

    fn visit_function_specification(
        &mut self,
        function: &novarocks_parser::ast::FunctionSpecification,
    ) {
        self.add_vec_capacity::<novarocks_parser::ast::FunctionParameter>(
            function.parameters.capacity(),
        );
        self.add_vec_capacity::<novarocks_parser::ast::RoutineCharacteristic>(
            function.characteristics.capacity(),
        );
        novarocks_parser::ast::walk_function_specification(self, function);
    }

    fn visit_routine_characteristic(
        &mut self,
        characteristic: &novarocks_parser::ast::RoutineCharacteristic,
    ) {
        use novarocks_parser::ast::RoutineCharacteristic;
        match characteristic {
            RoutineCharacteristic::Comment { comment, .. } => self.add(comment.capacity()),
            RoutineCharacteristic::Properties { properties, .. } => {
                self.add_vec_capacity::<novarocks_parser::ast::RoutineProperty>(
                    properties.capacity(),
                );
            }
            _ => {}
        }
        novarocks_parser::ast::walk_routine_characteristic(self, characteristic);
    }

    fn visit_routine_body(&mut self, body: &novarocks_parser::ast::RoutineBody) {
        if let novarocks_parser::ast::RoutineBody::External { definition, .. } = body {
            self.add(definition.capacity());
        }
        novarocks_parser::ast::walk_routine_body(self, body);
    }

    fn visit_variable_declaration(
        &mut self,
        declaration: &novarocks_parser::ast::VariableDeclaration,
    ) {
        self.add_vec_capacity::<Ident>(declaration.names.capacity());
        novarocks_parser::ast::walk_variable_declaration(self, declaration);
    }

    fn visit_else_clause(&mut self, clause: &novarocks_parser::ast::ElseClause) {
        self.add_vec_capacity::<novarocks_parser::ast::ControlStatement>(
            clause.statements.capacity(),
        );
        novarocks_parser::ast::walk_else_clause(self, clause);
    }

    fn visit_else_if_clause(&mut self, clause: &novarocks_parser::ast::ElseIfClause) {
        self.add_vec_capacity::<novarocks_parser::ast::ControlStatement>(
            clause.statements.capacity(),
        );
        novarocks_parser::ast::walk_else_if_clause(self, clause);
    }

    fn visit_case_statement_when_clause(
        &mut self,
        clause: &novarocks_parser::ast::CaseStatementWhenClause,
    ) {
        self.add_vec_capacity::<novarocks_parser::ast::ControlStatement>(
            clause.statements.capacity(),
        );
        novarocks_parser::ast::walk_case_statement_when_clause(self, clause);
    }

    fn visit_control_statement(&mut self, statement: &novarocks_parser::ast::ControlStatement) {
        use novarocks_parser::ast::{ControlStatement, VariableDeclaration};
        let statements = match statement {
            ControlStatement::Case(node) => {
                self.add_vec_capacity::<novarocks_parser::ast::CaseStatementWhenClause>(
                    node.when_clauses.capacity(),
                );
                None
            }
            ControlStatement::If(node) => {
                self.add_vec_capacity::<novarocks_parser::ast::ElseIfClause>(
                    node.else_if_clauses.capacity(),
                );
                Some(&node.statements)
            }
            ControlStatement::Compound(node) => {
                self.add_vec_capacity::<VariableDeclaration>(node.variable_declarations.capacity());
                Some(&node.statements)
            }
            ControlStatement::Loop(node) => Some(&node.statements),
            ControlStatement::While(node) => Some(&node.statements),
            ControlStatement::Repeat(node) => Some(&node.statements),
            ControlStatement::Return(_)
            | ControlStatement::Assignment(_)
            | ControlStatement::Iterate(_)
            | ControlStatement::Leave(_)
            | ControlStatement::VariableDeclaration(_) => None,
        };
        if let Some(statements) = statements {
            self.add_vec_capacity::<ControlStatement>(statements.capacity());
        }
        novarocks_parser::ast::walk_control_statement(self, statement);
    }

    fn visit_json_path_string(&mut self, path: &novarocks_parser::ast::JsonPathString) {
        self.add(path.value.capacity());
    }
    fn visit_json_path_invocation(&mut self, node: &novarocks_parser::ast::JsonPathInvocation) {
        self.add_vec_capacity::<novarocks_parser::ast::JsonPathParameter>(
            node.path_parameters.capacity(),
        );
        novarocks_parser::ast::walk_json_path_invocation(self, node);
    }
    fn visit_json_object(&mut self, node: &novarocks_parser::ast::JsonObject) {
        self.add_vec_capacity::<novarocks_parser::ast::JsonObjectMember>(node.members.capacity());
        novarocks_parser::ast::walk_json_object(self, node);
    }
    fn visit_json_array(&mut self, node: &novarocks_parser::ast::JsonArray) {
        self.add_vec_capacity::<novarocks_parser::ast::JsonArrayElement>(node.elements.capacity());
        novarocks_parser::ast::walk_json_array(self, node);
    }
    fn visit_json_table(&mut self, node: &novarocks_parser::ast::JsonTable) {
        self.add(std::mem::size_of::<novarocks_parser::ast::JsonTable>());
        self.add_vec_capacity::<novarocks_parser::ast::JsonTableColumn>(node.columns.capacity());
        novarocks_parser::ast::walk_json_table(self, node);
    }
    fn visit_json_table_nested_columns(
        &mut self,
        node: &novarocks_parser::ast::JsonTableNestedColumns,
    ) {
        self.add_vec_capacity::<novarocks_parser::ast::JsonTableColumn>(node.columns.capacity());
        novarocks_parser::ast::walk_json_table_nested_columns(self, node);
    }
    fn visit_json_table_plan_siblings(
        &mut self,
        node: &novarocks_parser::ast::JsonTablePlanSiblings,
    ) {
        self.add_vec_capacity::<novarocks_parser::ast::JsonTableSpecificPlan>(
            node.siblings.capacity(),
        );
        novarocks_parser::ast::walk_json_table_plan_siblings(self, node);
    }

    fn visit_json_table_plan_parent_child(
        &mut self,
        node: &novarocks_parser::ast::JsonTablePlanParentChild,
    ) {
        self.add(std::mem::size_of::<
            novarocks_parser::ast::JsonTableSpecificPlan,
        >());
        novarocks_parser::ast::walk_json_table_plan_parent_child(self, node);
    }

    #[recursive::recursive]
    fn visit_query(&mut self, query: &Query) {
        self.add(std::mem::size_of::<Query>());
        self.add_query_containers(query);
        walk_query(self, query);
    }

    #[recursive::recursive]
    fn visit_expr(&mut self, expression: &Expr) {
        self.add(std::mem::size_of::<Expr>());
        match expression {
            Expr::JsonConstructor(_) => {
                self.add(std::mem::size_of::<novarocks_parser::ast::JsonConstructor>())
            }
            Expr::JsonExists(_) => {
                self.add(std::mem::size_of::<novarocks_parser::ast::JsonExists>())
            }
            Expr::JsonValue(_) => self.add(std::mem::size_of::<novarocks_parser::ast::JsonValue>()),
            Expr::JsonQuery(_) => self.add(std::mem::size_of::<novarocks_parser::ast::JsonQuery>()),
            Expr::JsonSerialize(_) => {
                self.add(std::mem::size_of::<novarocks_parser::ast::JsonSerialize>())
            }
            Expr::JsonObject(_) => {
                self.add(std::mem::size_of::<novarocks_parser::ast::JsonObject>())
            }
            Expr::JsonArray(_) => self.add(std::mem::size_of::<novarocks_parser::ast::JsonArray>()),

            Expr::Dereference(_) => {
                self.add(std::mem::size_of::<novarocks_parser::ast::DereferenceExpr>())
            }
            Expr::Subscript(_) => {
                self.add(std::mem::size_of::<novarocks_parser::ast::SubscriptExpr>())
            }
            Expr::ArrayWildcardSubscript(_) => self.add(std::mem::size_of::<
                novarocks_parser::ast::ArrayWildcardSubscriptExpr,
            >()),
            Expr::Logical(expression) => {
                self.add_vec_capacity::<Expr>(expression.terms.capacity());
            }
            Expr::Predicated(_) => {
                self.add(std::mem::size_of::<novarocks_parser::ast::PredicatedExpr>())
            }
            Expr::InList(expression) => {
                self.add(std::mem::size_of::<novarocks_parser::ast::InListExpr>());
                self.add_vec_capacity::<Expr>(expression.values.capacity());
            }
            Expr::SearchedCase(expression) => {
                self.add(std::mem::size_of::<novarocks_parser::ast::SearchedCaseExpr>());
                self.add_vec_capacity::<novarocks_parser::ast::WhenClause>(
                    expression.when_clauses.capacity(),
                );
            }
            Expr::SimpleCase(expression) => {
                self.add(std::mem::size_of::<novarocks_parser::ast::SimpleCaseExpr>());
                self.add_vec_capacity::<novarocks_parser::ast::WhenClause>(
                    expression.when_clauses.capacity(),
                );
            }
            Expr::AtLocal(_) => self.add(std::mem::size_of::<novarocks_parser::ast::AtLocal>()),
            Expr::AtTimeZone(_) => {
                self.add(std::mem::size_of::<novarocks_parser::ast::AtTimeZone>())
            }
            Expr::Trim(_) => self.add(std::mem::size_of::<novarocks_parser::ast::Trim>()),
            Expr::Overlay(_) => self.add(std::mem::size_of::<novarocks_parser::ast::Overlay>()),
            Expr::IntervalValueExpression(_) => self.add(std::mem::size_of::<
                novarocks_parser::ast::IntervalValueExpression,
            >()),
            Expr::GroupingOperation(node) => {
                self.add(std::mem::size_of::<novarocks_parser::ast::GroupingOperation>());
                self.add_vec_capacity::<Expr>(node.grouping_columns.capacity());
            }
            Expr::MethodCall(node) => {
                self.add(std::mem::size_of::<novarocks_parser::ast::MethodCall>());
                self.add_vec_capacity::<novarocks_parser::ast::CallArgument>(
                    node.arguments.capacity(),
                );
            }
            Expr::StaticMethodCall(node) => {
                self.add(std::mem::size_of::<novarocks_parser::ast::StaticMethodCall>());
                self.add_vec_capacity::<novarocks_parser::ast::CallArgument>(
                    node.arguments.capacity(),
                );
            }
            Expr::CurrentCatalog(_)
            | Expr::CurrentDate(_)
            | Expr::CurrentPath(_)
            | Expr::CurrentSchema(_)
            | Expr::CurrentUser(_)
            | Expr::CurrentTime(_)
            | Expr::CurrentTimestamp(_)
            | Expr::LocalTime(_)
            | Expr::LocalTimestamp(_) => {}
            Expr::Extract(_) => self.add(std::mem::size_of::<novarocks_parser::ast::ExtractExpr>()),
            Expr::If(_) => self.add(std::mem::size_of::<novarocks_parser::ast::IfExpr>()),
            Expr::NullIf(_) => self.add(std::mem::size_of::<novarocks_parser::ast::NullIfExpr>()),
            Expr::Coalesce(node) => {
                self.add(std::mem::size_of::<novarocks_parser::ast::CoalesceExpr>());
                self.add_vec_capacity::<Expr>(node.operands.capacity());
            }
            Expr::Try(_) => self.add(std::mem::size_of::<novarocks_parser::ast::TryExpr>()),
            Expr::Format(node) => {
                self.add(std::mem::size_of::<novarocks_parser::ast::FormatExpr>());
                self.add_vec_capacity::<Expr>(node.arguments.capacity());
            }
            Expr::Cast(_) => self.add(std::mem::size_of::<novarocks_parser::ast::CastExpr>()),
            Expr::Exists(_) => self.add(std::mem::size_of::<novarocks_parser::ast::ExistsExpr>()),
            Expr::Unique(_) => self.add(std::mem::size_of::<novarocks_parser::ast::UniqueExpr>()),
            Expr::WindowOperation(operation) => {
                self.add(std::mem::size_of::<novarocks_parser::ast::WindowOperation>());
                self.add_window_containers(&operation.window);
            }
            Expr::Array(expression) => {
                self.add_vec_capacity::<Expr>(expression.values.capacity());
            }
            Expr::Row(expression) => {
                self.add_vec_capacity::<novarocks_parser::ast::RowField>(
                    expression.fields.capacity(),
                );
            }
            Expr::Lambda(expression) => {
                self.add(std::mem::size_of::<novarocks_parser::ast::LambdaExpr>());
                self.add_vec_capacity::<novarocks_parser::ast::LambdaArgumentDeclaration>(
                    expression.arguments.capacity(),
                );
            }
            Expr::AllRows(_)
            | Expr::Parameter(_)
            | Expr::Identifier(_)
            | Expr::Literal(_)
            | Expr::FunctionCall(_)
            | Expr::Unary(_)
            | Expr::Binary(_)
            | Expr::LegacyParenthesized(_)
            | Expr::LegacyInterval(_)
            | Expr::Subquery(_) => {}
        }
        walk_expr(self, expression);
    }

    fn visit_ident(&mut self, ident: &Ident) {
        self.add(std::mem::size_of::<Ident>());
        self.add(ident.value.capacity());
    }

    fn visit_object_name(&mut self, name: &ObjectName) {
        self.add(std::mem::size_of::<ObjectName>());
        self.add_vec_capacity::<Ident>(name.parts.capacity());
        walk_object_name(self, name);
    }

    fn visit_type_name(&mut self, type_name: &TypeName) {
        self.add(std::mem::size_of::<TypeName>());
        walk_type_name(self, type_name);
    }

    fn visit_generic_data_type(&mut self, node: &novarocks_parser::ast::GenericDataType) {
        self.add_vec_capacity::<novarocks_parser::ast::DataTypeParameter>(
            node.arguments.capacity(),
        );
        novarocks_parser::ast::walk_generic_data_type(self, node);
    }
    fn visit_row_data_type(&mut self, node: &novarocks_parser::ast::RowDataType) {
        self.add_vec_capacity::<novarocks_parser::ast::RowDataTypeField>(node.fields.capacity());
        novarocks_parser::ast::walk_row_data_type(self, node);
    }
    fn visit_date_time_data_type(&mut self, node: &novarocks_parser::ast::DateTimeDataType) {
        if node.precision.is_some() {
            self.add(std::mem::size_of::<novarocks_parser::ast::DataTypeParameter>());
        }
        novarocks_parser::ast::walk_date_time_data_type(self, node);
    }
    fn visit_numeric_parameter(&mut self, node: &novarocks_parser::ast::NumericParameter) {
        self.add(node.spelling.capacity());
    }
    fn visit_array_length(&mut self, node: &novarocks_parser::ast::ArrayLength) {
        self.add(node.spelling.capacity());
    }
    fn visit_simple_interval_qualifier(
        &mut self,
        node: &novarocks_parser::ast::SimpleIntervalQualifier,
    ) {
        if node.precision.is_some() {
            self.add(std::mem::size_of::<novarocks_parser::ast::DataTypeParameter>());
        }
        novarocks_parser::ast::walk_simple_interval_qualifier(self, node);
    }
    fn visit_composite_interval_qualifier(
        &mut self,
        node: &novarocks_parser::ast::CompositeIntervalQualifier,
    ) {
        if node.precision.is_some() {
            self.add(std::mem::size_of::<novarocks_parser::ast::DataTypeParameter>());
        }
        novarocks_parser::ast::walk_composite_interval_qualifier(self, node);
    }
    fn visit_interval_qualifier_field(
        &mut self,
        node: &novarocks_parser::ast::IntervalQualifierField,
    ) {
        if matches!(
            node,
            novarocks_parser::ast::IntervalQualifierField::Second {
                fractional_precision: Some(_)
            }
        ) {
            self.add(std::mem::size_of::<novarocks_parser::ast::DataTypeParameter>());
        }
        novarocks_parser::ast::walk_interval_qualifier_field(self, node);
    }

    fn visit_literal(&mut self, literal: &Literal) {
        self.add(std::mem::size_of::<Literal>());
        match &literal.kind {
            LiteralKind::Long {
                spelling: value, ..
            }
            | LiteralKind::Decimal(value)
            | LiteralKind::String(value) => self.add(value.capacity()),
            LiteralKind::Binary(value) => self.add(value.capacity()),
            LiteralKind::Generic(value) => {
                self.add(value.type_name.capacity());
                self.add(value.value.capacity());
            }
            LiteralKind::Interval(value) => self.add(value.value.capacity()),
            LiteralKind::Null | LiteralKind::Boolean(_) | LiteralKind::Double(_) => {}
        }
        novarocks_parser::ast::walk_literal(self, literal);
    }

    fn visit_function_call(&mut self, call: &FunctionCall) {
        self.add(std::mem::size_of::<FunctionCall>());
        self.add_vec_capacity::<novarocks_parser::ast::CallArgument>(call.arguments.capacity());
        self.add_order_by_containers(&call.order_by);
        if let Some(window) = &call.window {
            self.add_window_containers(window);
        }
        walk_function_call(self, call);
    }

    #[recursive::recursive]
    fn visit_relation(&mut self, relation: &novarocks_parser::ast::Relation) {
        use novarocks_parser::ast;
        self.add(std::mem::size_of::<ast::Relation>());
        match relation {
            ast::Relation::Table(_) | ast::Relation::TableSubquery(_) => {}
            ast::Relation::Aliased(node) => {
                self.add(std::mem::size_of::<ast::AliasedRelation>());
                if let Some(columns) = &node.column_names {
                    self.add_vec_capacity::<Ident>(columns.capacity());
                }
            }
            ast::Relation::Lateral(_) => self.add(std::mem::size_of::<ast::Lateral>()),
            ast::Relation::Unnest(node) => {
                self.add(std::mem::size_of::<ast::Unnest>());
                self.add_vec_capacity::<Expr>(node.expressions.capacity());
            }
            ast::Relation::Join(node) => {
                self.add(std::mem::size_of::<ast::Join>());
                if let Some(ast::JoinCriteria::Using(using)) = &node.criteria {
                    self.add_vec_capacity::<Ident>(using.columns.capacity());
                }
            }
            ast::Relation::Sampled(_) => self.add(std::mem::size_of::<ast::SampledRelation>()),
            ast::Relation::Pivot(node) => {
                self.add(std::mem::size_of::<ast::Pivot>());
                self.add_vec_capacity::<ast::PivotAggregation>(node.aggregations.capacity());
                self.add_vec_capacity::<Expr>(node.pivot_columns.capacity());
                self.add_vec_capacity::<ast::PivotValueGroup>(node.value_groups.capacity());
            }
            ast::Relation::Nearest(_) => self.add(std::mem::size_of::<ast::Nearest>()),
            ast::Relation::TableFunction(node) => {
                self.add(std::mem::size_of::<ast::TableFunctionInvocation>());
                self.add_vec_capacity::<ast::TableFunctionArgument>(node.arguments.capacity());
                self.add_vec_capacity::<Vec<ObjectName>>(node.copartitioning.capacity());
                for group in &node.copartitioning {
                    self.add_vec_capacity::<ObjectName>(group.capacity());
                }
            }
            ast::Relation::LegacyTableFunction(_) => {
                self.add(std::mem::size_of::<ast::legacy::TableFunction>())
            }
            ast::Relation::JsonTable(_) => {}
            ast::Relation::PatternRecognition(node) => {
                self.add(std::mem::size_of::<ast::PatternRecognitionRelation>());
                self.add_vec_capacity::<Expr>(node.partition_by.capacity());
                self.add_order_by_containers(&node.order_by);
                self.add_vec_capacity::<ast::MeasureDefinition>(node.measures.capacity());
                self.add_vec_capacity::<ast::SubsetDefinition>(node.subsets.capacity());
                self.add_vec_capacity::<ast::VariableDefinition>(
                    node.variable_definitions.capacity(),
                );
            }
        }
        ast::walk_relation(self, relation);
    }
    fn visit_table(&mut self, table: &novarocks_parser::ast::Table) {
        self.add(std::mem::size_of::<novarocks_parser::ast::Table>());
        novarocks_parser::ast::walk_table(self, table);
    }
    fn visit_table_subquery(&mut self, node: &novarocks_parser::ast::TableSubquery) {
        self.add(std::mem::size_of::<novarocks_parser::ast::TableSubquery>());
        novarocks_parser::ast::walk_table_subquery(self, node);
    }
    fn visit_group_by(&mut self, group_by: &novarocks_parser::ast::GroupBy) {
        self.add_vec_capacity::<novarocks_parser::ast::GroupingElement>(
            group_by.elements.capacity(),
        );
        for element in &group_by.elements {
            match element {
                novarocks_parser::ast::GroupingElement::Simple(simple) => {
                    self.add_vec_capacity::<Expr>(simple.columns.capacity())
                }
                novarocks_parser::ast::GroupingElement::Sets(sets) => {
                    self.add_vec_capacity::<novarocks_parser::ast::AstVec<Expr>>(
                        sets.sets.capacity(),
                    );
                    for group in &sets.sets {
                        self.add_vec_capacity::<Expr>(group.capacity());
                    }
                }
                novarocks_parser::ast::GroupingElement::Auto(_) => {}
            }
        }
        novarocks_parser::ast::walk_group_by(self, group_by);
    }
    fn visit_pivot_value_group(&mut self, node: &novarocks_parser::ast::PivotValueGroup) {
        self.add_vec_capacity::<Expr>(node.values.capacity());
        novarocks_parser::ast::walk_pivot_value_group(self, node);
    }
    fn visit_table_function_table_argument(
        &mut self,
        node: &novarocks_parser::ast::TableFunctionTableArgument,
    ) {
        if let Some(partition) = &node.partition_by {
            self.add_vec_capacity::<Expr>(partition.capacity());
        }
        self.add_order_by_containers(&node.order_by);
        novarocks_parser::ast::walk_table_function_table_argument(self, node);
    }
    fn visit_descriptor(&mut self, node: &novarocks_parser::ast::Descriptor) {
        self.add_vec_capacity::<novarocks_parser::ast::DescriptorField>(node.fields.capacity());
        novarocks_parser::ast::walk_descriptor(self, node);
    }
}

/// Candidate preparation retains the validated immutable input without
/// introducing a second identity map.
pub(crate) type MvRewriteDefinition = SqlMvRewriteDefinitionFacts;

/// Repository-order-preserving MV definition snapshot for one compiler request.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MvRewriteDefinitionIndex {
    definitions: Vec<MvRewriteDefinition>,
}

impl MvRewriteDefinitionIndex {
    pub fn try_new(definitions: Vec<SqlMvRewriteDefinitionFacts>) -> Result<Self, String> {
        Ok(Self {
            definitions: definitions
                .into_iter()
                .map(SqlMvRewriteDefinitionFacts::into_definition)
                .collect(),
        })
    }

    pub(crate) fn definitions(&self) -> &[MvRewriteDefinition] {
        &self.definitions
    }
}

struct AnalyzedMvRewriteCandidate {
    mv_name: String,
    mv: SpjgDescriptor,
    output_targets: Box<[usize]>,
    mv_scalars: crate::optimizer::scalar::ScalarArena,
    target_database: String,
    target_table: crate::planner::table::TableDef,
    factory_after_analysis: ColumnRefFactory,
    selection: Option<SqlMvRewriteSelectionFacts>,
}

#[expect(
    clippy::large_enum_variant,
    reason = "The inline SQL plan payload avoids an allocation at the compiler handoff boundary."
)]
enum SqlMvRewriteAnalysisEntry {
    Candidate(AnalyzedMvRewriteCandidate),
    Diagnostic(SqlMvRewriteDiagnostic),
    Ignored,
}

pub(crate) struct SqlMvRewriteAnalysis {
    constant_policy: novarocks_functions::ConstantPolicy,
    entries: Vec<SqlMvRewriteAnalysisEntry>,
}

impl SqlMvRewriteAnalysis {
    pub(crate) fn empty(constant_policy: novarocks_functions::ConstantPolicy) -> Self {
        Self {
            constant_policy,
            entries: Vec::new(),
        }
    }
}

pub(super) fn completion_statistics_tables(
    analysis: &SqlMvRewriteAnalysis,
) -> Vec<(String, crate::planner::table::TableDef)> {
    analysis
        .entries
        .iter()
        .filter_map(|entry| match entry {
            SqlMvRewriteAnalysisEntry::Candidate(candidate) => Some((
                candidate.target_database.clone(),
                candidate.target_table.clone(),
            )),
            SqlMvRewriteAnalysisEntry::Diagnostic(_) | SqlMvRewriteAnalysisEntry::Ignored => None,
        })
        .collect()
}

/// Prepare optional MV rewrite candidates from one immutable, repository-order
/// definition index. This is deliberately SQL-owned: application admission
/// freezes definitions and base-table observations, while parse/analyze,
/// descriptor construction and catalog materialization happen before the
/// application freezes request-local statistics. Statistics attachment and
/// warn-and-skip selection are deliberately deferred to the seal phase.
pub(crate) fn analyze_candidates(
    definitions: &MvRewriteDefinitionIndex,
    analyzer_catalog: &dyn PlannerTableProvider,
    current_database: &str,
    logical: &LogicalPlanNode,
    factory: &ColumnRefFactory,
    functions: &dyn SqlFunctionCatalog,
    optimizer_settings: &crate::optimizer::options::SessionOptimizerSettings,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &crate::compiler::SqlCompileControl,
    consumer_requires_semantic_snapshot: bool,
) -> Result<SqlMvRewriteAnalysis, crate::compiler::SqlCompileError> {
    if !optimizer_settings.mv_rewrite_enabled() {
        return Ok(SqlMvRewriteAnalysis::empty(constant_policy));
    }

    let mut query_fqns = Vec::new();
    collect_iceberg_fqns(logical, &mut query_fqns);
    if query_fqns.is_empty() {
        return Ok(SqlMvRewriteAnalysis::empty(constant_policy));
    }

    let mut entries = Vec::with_capacity(definitions.definitions().len());
    let mut candidate_factory = factory.clone();
    let mut materialized_candidates = 0usize;
    for definition in definitions.definitions() {
        control.check()?;
        if materialized_candidates >= MAX_SUCCESSFUL_MV_REWRITE_CANDIDATES {
            entries.push(SqlMvRewriteAnalysisEntry::Diagnostic(
                SqlMvRewriteDiagnostic {
                    mv_id: None,
                    message: format!(
                        "mv rewrite: candidate cap {MAX_SUCCESSFUL_MV_REWRITE_CANDIDATES} reached, rest skipped"
                    ),
                },
            ));
            break;
        }
        if definition.storage_engine != "iceberg"
            || !definition
                .sources
                .iter()
                .any(|base| query_fqns.contains(&base.table.fqn()))
        {
            entries.push(SqlMvRewriteAnalysisEntry::Ignored);
            continue;
        }
        // An optional candidate without a captured semantic contract is
        // ineligible before any analyzer replay; the base query remains valid.
        if consumer_requires_semantic_snapshot {
            entries.push(SqlMvRewriteAnalysisEntry::Diagnostic(SqlMvRewriteDiagnostic {
                mv_id: Some(definition.mv_id),
                message: "mv rewrite: persisted definition is ineligible under GROUP_CONCAT_LEGACY or decimal_overflow_to_double because its semantic settings are not captured".to_string(),
            }));
            continue;
        }
        if let Err(error) = crate::sql_mode::validate_persisted_query_semantics(
            &definition.select_query,
            &crate::sql_mode::SqlSemanticSettings::default(),
        ) {
            entries.push(SqlMvRewriteAnalysisEntry::Diagnostic(
                SqlMvRewriteDiagnostic {
                    mv_id: Some(definition.mv_id),
                    message: format!("mv rewrite: ineligible persisted definition: {error}"),
                },
            ));
            continue;
        }
        let Some(_) = definition.selection.as_ref() else {
            entries.push(SqlMvRewriteAnalysisEntry::Diagnostic(
                SqlMvRewriteDiagnostic {
                    mv_id: Some(definition.mv_id),
                    message: format!(
                        "mv rewrite: skipping frozen candidate without publication proof: {}",
                        definition
                            .selection_unavailable
                            .as_deref()
                            .unwrap_or("selection facts were not supplied")
                    ),
                },
            ));
            continue;
        };
        match build_candidate(
            analyzer_catalog,
            current_database,
            definition,
            &candidate_factory,
            functions,
            constant_policy,
            control,
        ) {
            Ok(Some(candidate)) => {
                candidate_factory = candidate.factory_after_analysis.clone();
                materialized_candidates += 1;
                entries.push(SqlMvRewriteAnalysisEntry::Candidate(candidate));
            }
            Ok(None) => entries.push(SqlMvRewriteAnalysisEntry::Ignored),
            Err(
                error @ (crate::compiler::SqlCompileError::Cancelled
                | crate::compiler::SqlCompileError::DeadlineExceeded
                | crate::compiler::SqlCompileError::ResourceExhausted),
            ) => return Err(error),
            Err(error) => entries.push(SqlMvRewriteAnalysisEntry::Diagnostic(
                SqlMvRewriteDiagnostic {
                    mv_id: Some(definition.mv_id),
                    message: format!("mv rewrite: skipping frozen candidate: {error}"),
                },
            )),
        }
        control.check()?;
    }

    Ok(SqlMvRewriteAnalysis {
        constant_policy,
        entries,
    })
}

pub(crate) fn attach_candidate_statistics(
    analysis: SqlMvRewriteAnalysis,
    statistics_context: &dyn SqlStatisticsSnapshot,
    query_stats: &mut SqlStatisticsPlan,
    main_factory: ColumnRefFactory,
) -> Result<(SqlMvRewritePreparation, ColumnRefFactory), crate::compiler::SqlCompileError> {
    let mut candidates = Vec::new();
    let mut diagnostics = Vec::new();
    let mut factory = main_factory;
    for entry in analysis.entries {
        match entry {
            SqlMvRewriteAnalysisEntry::Candidate(candidate) => {
                factory = candidate.factory_after_analysis;
                let (label, stats) = statistics_context.collect_table_statistics(
                    &candidate.target_database,
                    &candidate.target_table,
                )?;
                let target_stats_ref = query_stats.add_stats(label, stats);
                candidates.push(MvRewriteCandidate {
                    mv_name: candidate.mv_name,
                    mv: candidate.mv,
                    output_targets: candidate.output_targets,
                    mv_scalars: candidate.mv_scalars,
                    target_database: candidate.target_database,
                    target_table: candidate.target_table,
                    target_stats_ref,
                    selection: candidate.selection,
                });
            }
            SqlMvRewriteAnalysisEntry::Diagnostic(diagnostic) => diagnostics.push(diagnostic),
            SqlMvRewriteAnalysisEntry::Ignored => {}
        }
    }
    Ok((
        SqlMvRewritePreparation {
            constant_policy: analysis.constant_policy,
            candidates,
            diagnostics,
        },
        factory,
    ))
}

fn build_candidate(
    analyzer_catalog: &dyn PlannerTableProvider,
    _current_database: &str,
    definition: &MvRewriteDefinition,
    factory: &ColumnRefFactory,
    functions: &dyn SqlFunctionCatalog,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &crate::compiler::SqlCompileControl,
) -> Result<Option<AnalyzedMvRewriteCandidate>, crate::compiler::SqlCompileError> {
    use crate::compiler::SqlCompileError;
    if !definition_is_fresh(definition).map_err(SqlCompileError::Compilation)? {
        return Ok(None);
    }

    let catalog = DefinitionResolutionCatalog {
        inner: analyzer_catalog,
        default_catalog: &definition.resolution.default_catalog,
    };
    let (resolved, ctes, returned) = crate::analyzer::analyze_with_factory_and_function_catalog(
        &definition.select_query,
        &catalog,
        &definition.resolution.default_namespace,
        factory.clone(),
        functions,
        constant_policy,
        control,
    )
    .map_err(SqlCompileError::from)?;
    let mut returned = returned;
    let output_names = crate::analysis::schema::validate_definition_column_names(
        &resolved.output_columns,
        definition.select_query.span,
    )?;
    let analyzed_outputs = resolved.output_columns.clone();
    let mv_logical = crate::planner::plan_query(resolved, ctes, &mut returned, control)?;
    let root_columns = crate::planner::plan_output_columns(&mv_logical)?;
    super::root_output::RootOutputSemantics::bind_analyzed(
        &analyzed_outputs,
        &root_columns,
        &returned,
    )
    .map_err(SqlCompileError::Compilation)?;
    validate_definition_sources(&mv_logical, &definition.sources)
        .map_err(SqlCompileError::Compilation)?;
    let mut mv_scalars =
        crate::optimizer::scalar::ScalarArena::with_constant_policy(constant_policy);
    let mv_opt_expr = crate::planner::optimizer_bridge::logical::try_to_optimizer_expr(
        &mv_logical,
        &mut mv_scalars,
        control,
    )?;
    let mv = SpjgDescriptor::from_opt_expr(&mv_opt_expr, &mut mv_scalars, control)?;
    if mv.joins.is_some() {
        return Ok(None);
    }
    let Some(scan_fqn) = scan_fqn(&mv.table.source) else {
        return Ok(None);
    };
    if !definition
        .sources
        .iter()
        .any(|source| source.table.fqn() == scan_fqn)
    {
        return Err(SqlCompileError::Compilation(format!(
            "mv select resolved to {scan_fqn}, not in recorded base refs"
        )));
    }
    let Some(target) = &definition.target else {
        return Ok(None);
    };
    let target_table = analyzer_catalog
        .resolve_table_for_analysis(Some(&target.catalog), &target.namespace, &target.table)
        .map_err(SqlCompileError::Compilation)?
        .planner;
    let visible_roots = root_columns
        .iter()
        .filter(|column| !column.is_internal)
        .collect::<Vec<_>>();
    if mv.outputs.len() != output_names.len()
        || visible_roots.len() != output_names.len()
        || mv
            .outputs
            .iter()
            .zip(&visible_roots)
            .any(|(output, root)| output.column_id != root.column_id)
    {
        return Err(SqlCompileError::Compilation(
            "MV descriptor changed the analyzed output occurrence binding".into(),
        ));
    }
    let Some(output_targets) = bind_output_targets(&output_names, &target_table) else {
        return Ok(None);
    };
    Ok(Some(AnalyzedMvRewriteCandidate {
        mv_name: target.table.clone(),
        mv,
        output_targets,
        mv_scalars,
        target_database: target.namespace.clone(),
        target_table,
        factory_after_analysis: returned,
        selection: definition.selection.clone(),
    }))
}

fn bind_output_targets(
    names: &[String],
    target: &crate::planner::table::TableDef,
) -> Option<Box<[usize]>> {
    names
        .iter()
        .map(|name| {
            let name = name.to_lowercase();
            let mut matching = target
                .columns
                .iter()
                .enumerate()
                .filter(|(_, column)| column.name.to_lowercase() == name);
            let (ordinal, _) = matching.next()?;
            matching.next().is_none().then_some(ordinal)
        })
        .collect()
}

fn validate_definition_sources(
    plan: &LogicalPlanNode,
    sources: &[SqlMvRewriteSourceOccurrenceFacts],
) -> Result<(), String> {
    fn collect<'a>(
        plan: &'a LogicalPlanNode,
        scans: &mut Vec<&'a crate::planner::payload::PlanScanNode>,
    ) {
        if let crate::planner::logical::LogicalPlanKind::Scan(scan) = &plan.kind {
            scans.push(scan);
        }
        for child in &plan.children {
            collect(child, scans);
        }
    }
    let mut scans = Vec::new();
    collect(plan, &mut scans);
    if scans.len() != sources.len()
        || scans.iter().zip(sources).any(|(scan, source)| {
            let ScanSource::Sql(bound) = &scan.table.source;
            bound.table.catalog != source.table.catalog
                || bound.table.namespace != source.table.namespace
                || bound.table.table != source.table.table
                || scan.alias.as_deref().unwrap_or(&bound.table.table)
                    != source.qualifier_at_binding
        })
    {
        return Err("MV query does not match its ordered definition occurrences".to_string());
    }
    Ok(())
}

/// Apply the definition's name-resolution context without changing its AST or
/// accidentally qualifying CTE references as physical relations.
struct DefinitionResolutionCatalog<'a> {
    inner: &'a dyn PlannerTableProvider,
    default_catalog: &'a str,
}

impl PlannerTableProvider for DefinitionResolutionCatalog<'_> {
    fn resolve_table_version_for_analysis(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
        version: &novarocks_parser::ast::QueryPeriod,
    ) -> Result<crate::catalog::ResolvedAnalyzerTable, crate::analyze_error::AnalyzeError> {
        self.inner.resolve_table_version_for_analysis(
            Some(catalog.unwrap_or(self.default_catalog)),
            database,
            table,
            version,
        )
    }
    fn resolve_view_for_analysis(
        &self,
        catalog: Option<&str>,
        database: &str,
        view: &str,
    ) -> Result<Option<&crate::catalog::SqlViewDefinition>, crate::analyze_error::AnalyzeError>
    {
        self.inner.resolve_view_for_analysis(
            Some(catalog.unwrap_or(self.default_catalog)),
            database,
            view,
        )
    }

    fn view_definition_target(&self) -> Option<&novarocks_types::naming::TableIdentity> {
        self.inner.view_definition_target()
    }

    fn resolve_table_for_analysis(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<crate::catalog::ResolvedAnalyzerTable, String> {
        self.inner.resolve_table_for_analysis(
            Some(catalog.unwrap_or(self.default_catalog)),
            database,
            table,
        )
    }

    fn resolve_quoted_iceberg_metadata(
        &self,
        name: &novarocks_parser::ast::ObjectName,
        catalog: Option<&str>,
        database: &str,
    ) -> Option<crate::catalog::QuotedMetadataTable> {
        self.inner.resolve_quoted_iceberg_metadata(
            name,
            Some(catalog.unwrap_or(self.default_catalog)),
            database,
        )
    }

    fn iceberg_metadata_provider(
        &self,
    ) -> Option<&dyn crate::catalog::IcebergMetadataTableProvider> {
        self.inner
            .iceberg_metadata_provider()
            .map(|_| self as &dyn crate::catalog::IcebergMetadataTableProvider)
    }
}

impl crate::catalog::IcebergMetadataTableProvider for DefinitionResolutionCatalog<'_> {
    fn get_iceberg_metadata_table(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
        metadata_table_type: crate::planning::catalog::MetadataTableKind,
    ) -> Result<crate::catalog::ResolvedAnalyzerTable, String> {
        self.inner
            .iceberg_metadata_provider()
            .ok_or_else(|| "MV definition catalog has no metadata table provider".to_string())?
            .get_iceberg_metadata_table(
                Some(catalog.unwrap_or(self.default_catalog)),
                database,
                table,
                metadata_table_type,
            )
    }
}

fn definition_is_fresh(definition: &MvRewriteDefinition) -> Result<bool, String> {
    for source in &definition.sources {
        let Some(published_revision) = &source.published_revision else {
            return Ok(false);
        };
        match &source.observed_state.state {
            SqlMvRewriteBaseTableFactsState::Resolved(revision) => {
                if revision != published_revision {
                    return Ok(false);
                }
            }
            SqlMvRewriteBaseTableFactsState::Unavailable(error) => {
                return Err(format!(
                    "read frozen source occurrence {} ({}): {error}",
                    source.occurrence_id.get(),
                    source.table.fqn()
                ));
            }
        }
    }
    Ok(true)
}

fn collect_iceberg_fqns(plan: &LogicalPlanNode, output: &mut Vec<String>) {
    if let crate::planner::logical::LogicalPlanKind::Scan(scan) = &plan.kind
        && let Some(fqn) = scan_fqn(&scan.table.source)
        && !output.contains(&fqn)
    {
        output.push(fqn);
    }
    for child in &plan.children {
        collect_iceberg_fqns(child, output);
    }
}

fn scan_fqn(source: &ScanSource) -> Option<String> {
    match source {
        ScanSource::Sql(source) => match source.kind {
            crate::planner::table::SqlScanKind::Data { .. }
            | crate::planner::table::SqlScanKind::FrozenInputSet { .. } => Some(format!(
                "{}.{}.{}",
                source.table.catalog, source.table.namespace, source.table.table
            )),
            _ => None,
        },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    fn test_query(sql: &str) -> Query {
        let statements = novarocks_parser::parse(sql).expect("parse query");
        let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("fixture must be a query");
        };
        query.clone()
    }

    #[test]
    fn unsupported_consumer_semantics_make_only_mv_candidate_ineligible() {
        let catalog = CandidateCatalog::new();
        let (logical, factory) = main_candidate_query(&catalog);
        let resolutions_before = catalog.resolutions.load(Ordering::Acquire);
        let analysis = analyze_candidates(
            &candidate_index(),
            &catalog,
            "db",
            &logical,
            &factory,
            crate::functions::builtin_sql_function_catalog(),
            &crate::optimizer::options::SessionOptimizerSettings::default(),
            crate::constant::test_constant_policy(),
            &crate::compiler::SqlCompileControl::unbounded(),
            true,
        )
        .expect("base query is still admissible");
        assert!(
            analysis
                .entries
                .iter()
                .all(|entry| !matches!(entry, SqlMvRewriteAnalysisEntry::Candidate(_)))
        );
        assert!(analysis.entries.iter().any(|entry| matches!(entry, SqlMvRewriteAnalysisEntry::Diagnostic(d) if d.message.contains("GROUP_CONCAT_LEGACY"))));
        assert_eq!(
            catalog.resolutions.load(Ordering::Acquire),
            resolutions_before,
            "no unsafe definition analysis or target lookup"
        );
    }

    #[test]
    fn stored_legacy_definition_is_ineligible_before_reanalysis() {
        let catalog = CandidateCatalog::new();
        let (logical, factory) = main_candidate_query(&catalog);
        let resolutions_before = catalog.resolutions.load(Ordering::Acquire);
        let mut index = candidate_index();
        index.definitions[0].select_query =
            test_query("SELECT /*+ SET_VAR(sql_mode='GROUP_CONCAT_LEGACY') */ k FROM base");
        let analysis = analyze_candidates(
            &index,
            &catalog,
            "db",
            &logical,
            &factory,
            crate::functions::builtin_sql_function_catalog(),
            &crate::optimizer::options::SessionOptimizerSettings::default(),
            crate::constant::test_constant_policy(),
            &crate::compiler::SqlCompileControl::unbounded(),
            false,
        )
        .unwrap();
        assert!(analysis.entries.iter().any(|entry| matches!(entry, SqlMvRewriteAnalysisEntry::Diagnostic(d) if d.message.contains("ineligible persisted definition"))));
        assert_eq!(
            catalog.resolutions.load(Ordering::Acquire),
            resolutions_before
        );
    }

    #[test]
    fn stored_decimal_definition_is_ineligible_before_reanalysis() {
        let catalog = CandidateCatalog::new();
        let (logical, factory) = main_candidate_query(&catalog);
        let resolutions_before = catalog.resolutions.load(Ordering::Acquire);
        let mut index = candidate_index();
        index.definitions[0].select_query =
            test_query("SELECT /*+ SET_VAR(decimal_overflow_to_double=true) */ k FROM base");
        let analysis = analyze_candidates(
            &index,
            &catalog,
            "db",
            &logical,
            &factory,
            crate::functions::builtin_sql_function_catalog(),
            &crate::optimizer::options::SessionOptimizerSettings::default(),
            crate::constant::test_constant_policy(),
            &crate::compiler::SqlCompileControl::unbounded(),
            false,
        )
        .unwrap();
        assert!(analysis.entries.iter().any(|entry| matches!(entry, SqlMvRewriteAnalysisEntry::Diagnostic(d) if d.message.contains("ineligible persisted definition"))));
        assert_eq!(
            catalog.resolutions.load(Ordering::Acquire),
            resolutions_before
        );
    }

    #[test]
    fn completion_accounting_grows_on_twenty_thousand_real_set_operations() {
        use novarocks_parser::ast::{SetOperation, SetOperator, Values};
        use novarocks_parser::{SYNTHETIC, build};
        use std::mem::size_of;
        fn values(row_capacity: usize, rows_capacity: usize) -> SetExpr {
            let mut row = Vec::with_capacity(row_capacity);
            let expression = build::boolean(true);
            row.push(novarocks_parser::ast::RowField {
                name: None,
                span: expression.span(),
                expression,
            });
            let mut rows = Vec::with_capacity(rows_capacity);
            rows.push(Expr::Row(novarocks_parser::ast::RowExpr {
                fields: row.into(),
                span: SYNTHETIC,
            }));
            SetExpr::Values(Values {
                rows: rows.into(),
                span: SYNTHETIC,
            })
        }
        let mut definition = SqlMvRewriteDefinitionFacts::try_new(
            1,
            [11; 32],
            build::query(values(1, 1)),
            test_resolution(),
            "iceberg".to_string(),
            None,
            vec![test_source(SqlMvRewriteBaseTableFacts::unavailable(
                "not published".to_string(),
            ))],
        )
        .unwrap();
        // Freeze the unchanged definition metadata and original leaf separately.
        // Every newly owned buffer below is measured from its actual capacity.
        let baseline = definition.completion_retained_bytes().unwrap();
        let original_body = std::mem::replace(
            &mut definition.select_query.body,
            novarocks_parser::ast::AstBox::new(values(1, 1)),
        );
        let mut body = original_body.into_inner();
        let mut added_buffer_bytes = 0_u64;
        const DEPTH: usize = 20_000;
        for index in 0..DEPTH {
            let side = values(3 + index % 3, 2 + index % 2);
            let SetExpr::Values(side_values) = &side else {
                unreachable!()
            };
            let Expr::Row(row) = &side_values.rows[0] else {
                unreachable!()
            };
            added_buffer_bytes += (side_values.rows.capacity() * size_of::<Expr>()
                + row.fields.capacity() * size_of::<novarocks_parser::ast::RowField>())
                as u64;
            let mut inputs = Vec::with_capacity(5 + index % 3);
            inputs.push(body);
            inputs.push(side);
            added_buffer_bytes += (inputs.capacity() * size_of::<SetExpr>()) as u64;
            body = SetExpr::SetOperation(SetOperation {
                inputs: inputs.into(),
                operator: if index % 2 == 0 {
                    SetOperator::Union
                } else {
                    SetOperator::Intersect
                },
                distinct: true,
                corresponding: None,
                span: SYNTHETIC,
            });
        }
        definition.select_query.body = novarocks_parser::ast::AstBox::new(body);
        // Each binary operation adds itself and one VALUES sibling. The sibling's
        // boolean has no heap string and is visited once by the real AST walk.
        let inline_per_layer = 2 * size_of::<SetExpr>()
            + size_of::<SetOperation>()
            + size_of::<Values>()
            + 2 * size_of::<Expr>()
            + size_of::<Literal>();
        let expected = baseline + added_buffer_bytes + (DEPTH * inline_per_layer) as u64;
        assert_eq!(definition.completion_retained_bytes().unwrap(), expected);
        // Dropping the real graph also consumes the existing AstVec owning edge.
        drop(definition);
    }

    #[test]
    fn completion_accounting_charges_spare_ast_vector_capacity() {
        const COMPLETION_LIMIT: usize = 32 * 1024 * 1024;
        let mut query = test_query("select 1");
        let item_size = std::mem::size_of::<novarocks_parser::ast::SortItem>();
        let capacity = COMPLETION_LIMIT / item_size + 1;
        let SetExpr::Select(select) = query.body.as_mut() else {
            panic!("SELECT")
        };
        let mut items = Vec::with_capacity(capacity);
        items.push(novarocks_parser::build::sort_item(
            novarocks_parser::build::number("1").unwrap(),
            novarocks_parser::ast::SortOrdering::Ascending,
            novarocks_parser::ast::NullOrdering::Undefined,
        ));
        select.modifiers.order_by = Some(novarocks_parser::build::order_by(items).unwrap());
        let definition = SqlMvRewriteDefinitionFacts::try_new(
            1,
            [11; 32],
            query,
            test_resolution(),
            "iceberg".to_string(),
            None,
            vec![test_source(SqlMvRewriteBaseTableFacts::unavailable(
                "not published".to_string(),
            ))],
        )
        .unwrap();

        assert!(definition.completion_retained_bytes().unwrap() > COMPLETION_LIMIT as u64);
    }

    #[test]
    fn completion_accounting_charges_json_member_capacity() {
        const LIMIT: usize = 32 * 1024 * 1024;
        let mut query = test_query("SELECT 1");
        let SetExpr::Select(select) = query.body.as_mut() else {
            panic!("SELECT")
        };
        let capacity = LIMIT / std::mem::size_of::<novarocks_parser::ast::JsonObjectMember>() + 1;
        select.projection[0] = novarocks_parser::build::item(
            Expr::JsonObject(novarocks_parser::ast::AstBox::new(
                novarocks_parser::ast::JsonObject {
                    members: Vec::with_capacity(capacity),
                    null_on_null: true,
                    unique_keys: false,
                    returned_type: None,
                    output_format: None,
                    span: novarocks_parser::SYNTHETIC,
                },
            )),
            None,
        );
        let definition = SqlMvRewriteDefinitionFacts::try_new(
            1,
            [11; 32],
            query,
            test_resolution(),
            "iceberg".to_string(),
            None,
            vec![test_source(SqlMvRewriteBaseTableFacts::unavailable(
                "not published".to_string(),
            ))],
        )
        .unwrap();
        assert!(definition.completion_retained_bytes().unwrap() > LIMIT as u64);
    }

    struct CandidateCatalog {
        resolutions: AtomicUsize,
    }

    #[test]
    fn partition_facts_accept_unpartitioned_spec() {
        let facts = SqlImvPartitionFacts::try_new(0, Vec::new())
            .expect("unpartitioned Iceberg spec must be valid partition facts");

        assert_eq!(facts.inner.target_spec_id, 0);
        assert!(facts.inner.fields.is_empty());
    }

    impl CandidateCatalog {
        fn new() -> Self {
            Self {
                resolutions: AtomicUsize::new(0),
            }
        }

        fn table(table: &str, binding: u32) -> crate::planner::table::TableDef {
            crate::planner::table::TableDef {
                name: table.to_string(),
                columns: vec![novarocks_types::schema::ColumnDef {
                    name: "k".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                }],
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: crate::planner::table::ScanSource::Sql(
                    crate::planner::table::SqlScanSource::new(
                        SqlTableBindingId::new_for_test(binding),
                        crate::planner::table::SqlTableIdentity {
                            catalog: "iceberg".to_string(),
                            namespace: "db".to_string(),
                            table: table.to_string(),
                        },
                        crate::planner::table::SqlScanKind::Data {
                            version: crate::planner::table::SqlTableVersionSelector::Current,
                        },
                    ),
                ),
            }
        }
    }

    impl PlannerTableProvider for CandidateCatalog {
        fn resolve_table_for_analysis(
            &self,
            catalog: Option<&str>,
            database: &str,
            table: &str,
        ) -> Result<crate::catalog::ResolvedAnalyzerTable, String> {
            if catalog != Some("iceberg") || database != "db" {
                return Err(
                    "candidate lookup used the wrong definition resolution context".to_string(),
                );
            }
            self.resolutions.fetch_add(1, Ordering::AcqRel);
            let binding = match table {
                "base" => 1,
                "mv_target" => 2,
                _ => return Err(format!("unknown candidate table `{table}`")),
            };
            Ok(crate::catalog::ResolvedAnalyzerTable::from_planner(
                catalog,
                database,
                Self::table(table, binding),
            ))
        }
    }

    fn candidate_index() -> MvRewriteDefinitionIndex {
        let selection = SqlMvRewriteSelectionFacts::try_new_for_target(
            [7; 16],
            [9; 32],
            vec!["iceberg.db.base".to_string()],
            "iceberg.db.mv_target".to_string(),
        )
        .unwrap();
        let revision = selection.publication_inputs()[0]
            .relation()
            .revision()
            .clone();
        let source = SqlMvRewriteSourceOccurrenceFacts::try_new(
            SqlMvRelationOccurrenceId::new(0),
            novarocks_types::naming::TableIdentity::new("iceberg", "db", "base"),
            "base".to_string(),
            Some(revision.clone()),
            SqlMvRewriteBaseTableFacts::resolved(revision),
        )
        .unwrap();
        MvRewriteDefinitionIndex::try_new(vec![
            SqlMvRewriteDefinitionFacts::try_new(
                1,
                [11; 32],
                test_query("select k from iceberg.db.base"),
                test_resolution(),
                "iceberg".to_string(),
                Some(novarocks_types::naming::TableIdentity::new(
                    "iceberg",
                    "db",
                    "mv_target",
                )),
                vec![source],
            )
            .expect("candidate definition")
            .with_selection_facts(selection)
            .expect("candidate publication proof"),
        ])
        .expect("candidate index")
    }

    fn main_candidate_query(catalog: &CandidateCatalog) -> (LogicalPlanNode, ColumnRefFactory) {
        let query = test_query("select k from iceberg.db.base");
        let (resolved, ctes, mut factory) = crate::analyzer::analyze_with_function_catalog(
            &query,
            catalog,
            "db",
            crate::functions::builtin_sql_function_catalog(),
            crate::constant::test_constant_policy(),
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .expect("analyze main query");
        let logical = crate::planner::plan_query(
            resolved,
            ctes,
            &mut factory,
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .expect("plan main query");
        (logical, factory)
    }

    fn frozen_definition(state: SqlMvRewriteBaseTableFacts) -> MvRewriteDefinition {
        SqlMvRewriteDefinitionFacts::try_new(
            1,
            [11; 32],
            test_query("select 1"),
            test_resolution(),
            "iceberg".to_string(),
            Some(novarocks_types::naming::TableIdentity::new(
                "iceberg",
                "db",
                "mv_target",
            )),
            vec![test_source(state)],
        )
        .expect("valid frozen definition facts")
        .into_definition()
    }

    fn test_resolution() -> SqlMvDefinitionResolutionContext {
        SqlMvDefinitionResolutionContext::try_new("iceberg".to_string(), "db".to_string()).unwrap()
    }

    fn test_revision(object: &str, snapshot: i64) -> ConnectorExactSemanticRevision {
        ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
            novarocks_spi::connector::ConnectorProviderId::parse("iceberg").unwrap(),
            &test_object_id(object),
            Some(snapshot),
        )
        .unwrap()
    }

    fn test_source(state: SqlMvRewriteBaseTableFacts) -> SqlMvRewriteSourceOccurrenceFacts {
        SqlMvRewriteSourceOccurrenceFacts::try_new(
            SqlMvRelationOccurrenceId::new(7),
            novarocks_types::naming::TableIdentity::new("iceberg", "db", "base"),
            "base".to_string(),
            Some(test_revision("original-object", 42)),
            state,
        )
        .unwrap()
    }

    fn occurrence_definition(
        selection: &SqlMvRewriteSelectionFacts,
    ) -> SqlMvRewriteDefinitionFacts {
        let sources = selection
            .publication_inputs
            .iter()
            .map(|input| {
                let mut source = test_source(SqlMvRewriteBaseTableFacts::resolved(
                    input.relation.revision.clone(),
                ));
                source.occurrence_id = input.occurrence_id;
                source.published_revision = Some(input.relation.revision.clone());
                source
            })
            .collect();
        SqlMvRewriteDefinitionFacts::try_new(
            1,
            [11; 32],
            test_query("select k from iceberg.db.base"),
            test_resolution(),
            "iceberg".to_string(),
            Some(novarocks_types::naming::TableIdentity::new(
                "iceberg",
                "db",
                "mv_target",
            )),
            sources,
        )
        .unwrap()
    }

    fn repeated_publication() -> SqlMvRewriteSelectionFacts {
        SqlMvRewriteSelectionFacts::try_new_for_target_with_occurrences(
            [7; 16],
            [9; 32],
            vec![
                (
                    SqlMvRelationOccurrenceId::new(7),
                    "iceberg.db.base".to_string(),
                ),
                (
                    SqlMvRelationOccurrenceId::new(42),
                    "iceberg.db.base".to_string(),
                ),
            ],
            "iceberg.db.mv_target".to_string(),
        )
        .unwrap()
    }

    #[test]
    fn publication_preserves_non_dense_repeated_relation_occurrences() {
        let publication = repeated_publication();
        let definition = occurrence_definition(&publication)
            .with_selection_facts(publication)
            .unwrap();
        assert_eq!(
            definition
                .sources
                .iter()
                .map(|source| source.occurrence_id.get())
                .collect::<Vec<_>>(),
            vec![7, 42]
        );
        assert!(definition_is_fresh(&definition).unwrap());
        let mut stale = definition.clone();
        stale.sources[1].observed_state =
            SqlMvRewriteBaseTableFacts::resolved(test_revision("changed", 101));
        assert!(!definition_is_fresh(&stale).unwrap());
    }

    #[test]
    fn publication_rejects_missing_extra_swapped_and_foreign_occurrences() {
        let publication = repeated_publication();
        let definition = occurrence_definition(&publication);
        let mut missing = publication.clone();
        missing.publication_inputs.pop();
        assert!(definition.clone().with_selection_facts(missing).is_err());
        let mut extra = publication.clone();
        let mut input = extra.publication_inputs[0].clone();
        input.occurrence_id = SqlMvRelationOccurrenceId::new(99);
        extra.publication_inputs.push(input);
        assert!(definition.clone().with_selection_facts(extra).is_err());
        let mut swapped = publication.clone();
        swapped.publication_inputs.swap(0, 1);
        assert!(definition.clone().with_selection_facts(swapped).is_err());
        let mut foreign = publication.clone();
        foreign.publication_inputs[1].occurrence_id = SqlMvRelationOccurrenceId::new(99);
        assert!(definition.clone().with_selection_facts(foreign).is_err());
        let mut wrong_revision = publication.clone();
        wrong_revision.definition_revision = [13; 32];
        assert!(definition.with_selection_facts(wrong_revision).is_err());
    }

    #[test]
    fn duplicate_occurrence_is_rejected_even_with_distinct_names() {
        assert!(
            SqlMvRewriteSelectionFacts::try_new_for_target_with_occurrences(
                [7; 16],
                [9; 32],
                vec![
                    (SqlMvRelationOccurrenceId::new(7), "ice.db.a".to_string()),
                    (SqlMvRelationOccurrenceId::new(7), "ice.db.b".to_string()),
                ],
                "ice.db.mv".to_string(),
            )
            .is_err()
        );
    }

    #[test]
    fn self_join_field_identity_is_opaque_and_occurrence_qualified() {
        let field = |id| {
            SqlImvQualifiedFieldFacts::try_new(
                SqlMvRelationOccurrenceId::new(id),
                "ice.db.base".to_string(),
                format!("base_{id}"),
                bytes::Bytes::from_static(b"\xffopaque\x00field"),
            )
            .unwrap()
        };
        assert!(SqlImvJoinPredicateFacts::try_new(field(7), field(42)).is_ok());
        assert!(SqlImvJoinPredicateFacts::try_new(field(7), field(7)).is_err());
        assert!(
            SqlImvExpressionFacts::try_new(
                SqlImvExpressionKindFacts::Mixed,
                vec![field(7), field(42)]
            )
            .is_ok()
        );
        assert!(
            SqlImvExpressionFacts::try_new(
                SqlImvExpressionKindFacts::Mixed,
                vec![field(7), field(42), field(7)]
            )
            .is_err()
        );
    }

    #[test]
    fn candidate_and_query_signed_integer_ranges_survive_fold_policy() {
        use crate::compiler::{
            ConversionPlan, FoldRequest, FrozenFoldIntrinsic, FrozenFoldNode, SqlAnalyzeRequest,
            SqlCompiler, SqlConstantEvaluationError, SqlConstantEvaluator, SqlPlannerTableSnapshot,
            SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput,
        };
        use crate::optimizer::rewrite::{
            context::RewriteContext,
            phase::RewritePhase,
            pipeline::{RewritePipeline, RewriteStage},
            rules,
        };
        use std::cell::RefCell;
        use std::rc::Rc;

        struct SignedCastFoldPort(AtomicUsize);
        impl SqlConstantEvaluator for SignedCastFoldPort {
            fn eval_scalar(
                &self,
                request: &FoldRequest,
                control: &dyn novarocks_type_contract::PureCompileControl,
            ) -> Result<Option<novarocks_functions::ConstantValue>, SqlConstantEvaluationError>
            {
                let FrozenFoldNode::Intrinsic(FrozenFoldIntrinsic::Cast {
                    plan: ConversionPlan::Carrier(step),
                }) = &request.kind
                else {
                    return Ok(None);
                };
                let [argument] = request.args.as_slice() else {
                    panic!("the original CAST fold request has one argument");
                };
                if !crate::optimizer::scalar_expr::nonnull_signed_literal_widening(
                    &argument.value_type,
                    &request.result_type,
                ) {
                    return Ok(None);
                }
                assert_eq!(step.target(), &request.result_type);
                assert_eq!(argument.value.value_type(), &argument.value_type);
                let Some(value) = argument.value.signed_integer_observed(
                    novarocks_type_contract::CompilePhase::FunctionSpecialization,
                    control,
                )?
                else {
                    return Ok(None);
                };
                self.0.fetch_add(1, Ordering::SeqCst);
                // This test port models an already admitted lossless signed CAST.
                // The production FoldConstant rule still owns the CV publication.
                Ok(Some(crate::constant::admit_syntax_constant(
                    &crate::analysis::LiteralValue::Int(value),
                    &request.result_type,
                    request.constant_policy,
                    control,
                )?))
            }
        }

        for (disabled, port_absent) in [(false, false), (true, false), (false, true)] {
            for minimum in [98, 99] {
                let evaluator: &'static SignedCastFoldPort =
                    Box::leak(Box::new(SignedCastFoldPort(AtomicUsize::new(0))));
                let catalog = CandidateCatalog::new();
                let snapshot = SqlPlannerTableSnapshot::new(&catalog);
                let mut definitions = candidate_index();
                definitions.definitions[0].select_query =
                    test_query("SELECT k FROM iceberg.db.base WHERE k > 98");
                let mut settings = crate::optimizer::options::SessionOptimizerSettings::default();
                if disabled {
                    settings.disabled_rules.push("FoldConstant".into());
                }
                let control = crate::compiler::SqlCompileControl::unbounded();
                let mut request = SqlAnalyzeRequest::new(
                    SqlStatementInput::parsed_query(Box::new(test_query(&format!(
                        "SELECT k FROM iceberg.db.base WHERE k > {minimum}"
                    )))),
                    crate::compiler::SqlCompileIntent::Query,
                    SqlSessionContext {
                        current_catalog: Some("iceberg".into()),
                        current_database: "db".into(),
                        sql_semantics: crate::sql_mode::SqlSemanticSettings::default(),
                        optimizer_settings: settings,
                    },
                    SqlPlanningEnvironment::Distributed,
                    &snapshot,
                    crate::functions::builtin_sql_function_catalog(),
                    evaluator,
                    Some(&definitions),
                    crate::constant::test_constant_policy(),
                    control.clone(),
                );
                if port_absent {
                    request.constant_evaluator = None;
                }
                let analyzed = SqlCompiler::analyze(request)
                    .unwrap()
                    .into_pending()
                    .unwrap();
                let [SqlMvRewriteAnalysisEntry::Candidate(candidate)] =
                    analyzed.mv_rewrite.entries.as_slice()
                else {
                    panic!("the original published candidate must survive analysis");
                };
                let mut arena = crate::optimizer::scalar::ScalarArena::with_constant_policy(
                    analyzed.constant_policy,
                );
                let plan = crate::planner::optimizer_bridge::logical::try_to_optimizer_expr(
                    &analyzed.logical_plan,
                    &mut arena,
                    &control,
                )
                .unwrap();
                let arena = Rc::new(RefCell::new(arena));
                let mut context = RewriteContext::for_query_with_settings(
                    analyzed.settings.clone(),
                    analyzed.decimal_overflow_policy,
                    &control,
                );
                context.set_root_allow_throw_exception(analyzed.root_allow_throw_exception);
                context.set_function_catalog(Arc::clone(&analyzed.function_catalog));
                context.set_fold_dependency_observer(control.fold_dependency_observer().cloned());
                context.set_scalar_arena(Rc::clone(&arena));
                if let Some(port) = analyzed.constant_evaluator {
                    context.set_constant_evaluator(port);
                }
                let plan = RewritePipeline::from_stages(vec![RewriteStage::new(
                    "ConstantFold",
                    RewritePhase::LogicalNormalize,
                    rules::fold_constant_rules(),
                )])
                .rewrite(plan, &mut context)
                .unwrap();
                drop(context);
                let mut arena = Rc::try_unwrap(arena)
                    .unwrap_or_else(|_| panic!("the test retains one original scalar arena"))
                    .into_inner();
                let query = SpjgDescriptor::from_opt_expr(&plan, &mut arena, &control).unwrap();
                let contained =
                    crate::optimizer::cascades_rules::mv_rewrite::predicate_split::check_containment(
                        &query.predicates,
                        &arena,
                        &candidate.mv.predicates,
                        &candidate.mv_scalars,
                        &query.base_name_of(),
                        &candidate.mv.base_name_of(),
                        &control,
                    )
                    .unwrap();
                assert!(
                    contained.is_some(),
                    "signed range containment must survive minimum={minimum}, FoldConstant disabled={disabled}, port absent={port_absent}"
                );
                let contained = contained.unwrap();
                assert_eq!(contained.compensation.len(), usize::from(minimum > 98));
                if disabled || port_absent {
                    assert_eq!(evaluator.0.load(Ordering::SeqCst), 0);
                } else {
                    assert!(
                        evaluator.0.load(Ordering::SeqCst) >= 1,
                        "the query must consume the original FoldConstant port and publish a typed CV"
                    );
                }
            }
        }
    }

    #[test]
    fn candidate_names_bind_once_to_unique_targets_independent_of_storage_order() {
        let mut target = CandidateCatalog::table("mv_target", 2);
        let mut hidden = target.columns[0].clone();
        hidden.name = "__hidden_apply_key".into();
        target.columns.insert(0, hidden);
        assert_eq!(
            bind_output_targets(&["K".into()], &target)
                .unwrap()
                .as_ref(),
            &[1]
        );
        assert!(bind_output_targets(&["missing".into()], &target).is_none());
        let mut ambiguous = target.columns[1].clone();
        ambiguous.name = "K".into();
        target.columns.push(ambiguous);
        assert!(bind_output_targets(&["k".into()], &target).is_none());

        let mut unicode = target.columns[0].clone();
        unicode.name = "Ä".into();
        target.columns.push(unicode);
        assert_eq!(
            bind_output_targets(&["ä".into()], &target)
                .unwrap()
                .as_ref(),
            &[3]
        );

        let catalog = CandidateCatalog::new();
        let mut definition = candidate_index().definitions[0].clone();
        definition.select_query = test_query("SELECT K FROM iceberg.db.base");
        let candidate = build_candidate(
            &catalog,
            "caller_db",
            &definition,
            &ColumnRefFactory::new(),
            crate::functions::builtin_sql_function_catalog(),
            crate::constant::test_constant_policy(),
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap()
        .expect("original SQL name K binds the stored k column");
        assert_eq!(candidate.output_targets.as_ref(), &[0]);
    }

    #[test]
    fn candidate_uses_definition_resolution_context_not_query_session_namespace() {
        let catalog = CandidateCatalog::new();
        let mut definition = candidate_index().definitions()[0].clone();
        definition.select_query = test_query("select k from base");
        assert!(
            build_candidate(
                &catalog,
                "another_database",
                &definition,
                &ColumnRefFactory::new(),
                crate::functions::builtin_sql_function_catalog(),
                crate::constant::test_constant_policy(),
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap()
            .is_some()
        );
    }

    #[test]
    fn sqlx1_mv_rewrite_definition_index_preserves_application_order() {
        let index = MvRewriteDefinitionIndex::try_new(vec![
            SqlMvRewriteDefinitionFacts::try_new(
                7,
                [11; 32],
                test_query("select 1"),
                test_resolution(),
                "iceberg".to_string(),
                None,
                vec![test_source(SqlMvRewriteBaseTableFacts::unavailable(
                    "not published".to_string(),
                ))],
            )
            .expect("valid first frozen definition"),
            SqlMvRewriteDefinitionFacts::try_new(
                3,
                [11; 32],
                test_query("select 2"),
                test_resolution(),
                "iceberg".to_string(),
                None,
                vec![test_source(SqlMvRewriteBaseTableFacts::unavailable(
                    "not published".to_string(),
                ))],
            )
            .expect("valid second frozen definition"),
        ])
        .expect("valid ordered frozen definitions");

        assert_eq!(
            index
                .definitions()
                .iter()
                .map(|definition| definition.mv_id)
                .collect::<Vec<_>>(),
            vec![7, 3]
        );
    }

    #[test]
    fn sqlx2_mv_frozen_snapshot_and_object_id_decide_candidate_freshness() {
        let fresh = frozen_definition(SqlMvRewriteBaseTableFacts::resolved(test_revision(
            "original-object",
            42,
        )));
        let stale = frozen_definition(SqlMvRewriteBaseTableFacts::resolved(test_revision(
            "original-object",
            43,
        )));
        let recreated = frozen_definition(SqlMvRewriteBaseTableFacts::resolved(test_revision(
            "replacement-object",
            42,
        )));

        assert_eq!(definition_is_fresh(&fresh), Ok(true));
        assert_eq!(definition_is_fresh(&stale), Ok(false));
        assert_eq!(definition_is_fresh(&recreated), Ok(false));
    }

    #[test]
    fn sqlx2_mv_frozen_read_failure_stays_a_warn_and_skip_input() {
        let unavailable = frozen_definition(SqlMvRewriteBaseTableFacts::unavailable(
            "catalog unavailable".to_string(),
        ));

        assert!(matches!(
            definition_is_fresh(&unavailable),
            Err(error) if error.contains("catalog unavailable")
        ));
    }

    #[test]
    fn sqlx2_mv_candidate_limit_is_sixteen_successes() {
        assert_eq!(MAX_SUCCESSFUL_MV_REWRITE_CANDIDATES, 16);
    }

    #[test]
    fn phase_one_materializes_mv_base_and_target_before_statistics_attachment() {
        let catalog = CandidateCatalog::new();
        let (logical, factory) = main_candidate_query(&catalog);
        let mut constant_policy = crate::constant::test_constant_policy();
        constant_policy.max_rows = 37;
        let analysis = analyze_candidates(
            &candidate_index(),
            &catalog,
            "db",
            &logical,
            &factory,
            crate::functions::builtin_sql_function_catalog(),
            &crate::optimizer::options::SessionOptimizerSettings::default(),
            constant_policy,
            &crate::compiler::SqlCompileControl::unbounded(),
            false,
        )
        .expect("analyze candidate before statistics freeze");
        assert_eq!(analysis.constant_policy, constant_policy);
        assert!(analysis.entries.iter().any(|entry| {
            matches!(entry, SqlMvRewriteAnalysisEntry::Candidate(candidate)
                if candidate.mv_scalars.constant_policy() == constant_policy)
        }));
        assert_eq!(
            catalog.resolutions.load(Ordering::Acquire),
            3,
            "main base, candidate base, and candidate target must all materialize in phase one"
        );

        let statistics = crate::planning::dml::DmlStatisticsSnapshot::from_evidence([
            crate::planning::dml::DmlStatisticsEvidence::Missing {
                binding: SqlTableBindingId::new_for_test(1),
                label: "iceberg.db.base".to_string(),
                reason: "base statistics unavailable".to_string(),
            },
            crate::planning::dml::DmlStatisticsEvidence::Missing {
                binding: SqlTableBindingId::new_for_test(2),
                label: "iceberg.db.mv_target".to_string(),
                reason: "target statistics unavailable".to_string(),
            },
        ]);
        let (prepared, _) = attach_candidate_statistics(
            analysis,
            &statistics,
            &mut SqlStatisticsPlan::empty(),
            factory.clone(),
        )
        .expect("typed Missing target statistics remain conservative");
        assert_eq!(prepared.candidates.len(), 1);
        assert_eq!(prepared.constant_policy, constant_policy);
        assert_eq!(
            prepared.candidates[0].mv_scalars.constant_policy(),
            constant_policy
        );
        assert_eq!(catalog.resolutions.load(Ordering::Acquire), 3);

        let analysis = analyze_candidates(
            &candidate_index(),
            &catalog,
            "db",
            &logical,
            &factory,
            crate::functions::builtin_sql_function_catalog(),
            &crate::optimizer::options::SessionOptimizerSettings::default(),
            crate::constant::test_constant_policy(),
            &crate::compiler::SqlCompileControl::unbounded(),
            false,
        )
        .expect("reanalyze candidate for omitted-target check");
        let base_only = crate::planning::dml::DmlStatisticsSnapshot::from_evidence([
            crate::planning::dml::DmlStatisticsEvidence::Missing {
                binding: SqlTableBindingId::new_for_test(1),
                label: "iceberg.db.base".to_string(),
                reason: "base statistics unavailable".to_string(),
            },
        ]);
        let resolutions_before_attachment = catalog.resolutions.load(Ordering::Acquire);
        let error = match attach_candidate_statistics(
            analysis,
            &base_only,
            &mut SqlStatisticsPlan::empty(),
            factory,
        ) {
            Ok(_) => panic!("omitted MV target binding must fail compilation"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("binding is missing"));
        assert_eq!(
            catalog.resolutions.load(Ordering::Acquire),
            resolutions_before_attachment,
            "statistics attachment must not reenter the catalog"
        );
    }

    #[test]
    fn missing_publication_proof_is_diagnostic_and_never_materializes_a_candidate() {
        let catalog = CandidateCatalog::new();
        let (logical, factory) = main_candidate_query(&catalog);
        let mut index = candidate_index();
        index.definitions[0].selection = None;
        index.definitions[0].selection_unavailable =
            Some("MV rewrite target has no published version".to_string());

        let analysis = analyze_candidates(
            &index,
            &catalog,
            "db",
            &logical,
            &factory,
            crate::functions::builtin_sql_function_catalog(),
            &crate::optimizer::options::SessionOptimizerSettings::default(),
            crate::constant::test_constant_policy(),
            &crate::compiler::SqlCompileControl::unbounded(),
            false,
        )
        .expect("optional MV proof failure must preserve the base query");
        let (prepared, _) = attach_candidate_statistics(
            analysis,
            &crate::planning::dml::DmlStatisticsSnapshot::empty(),
            &mut SqlStatisticsPlan::empty(),
            factory,
        )
        .expect("optional MV proof failure needs no target statistics");

        assert!(prepared.candidates.is_empty());
        assert_eq!(prepared.diagnostics.len(), 1);
        assert!(
            prepared.diagnostics[0]
                .message
                .contains("no published version")
        );
        assert_eq!(
            catalog.resolutions.load(Ordering::Acquire),
            1,
            "only the base query may enter catalog analysis"
        );
    }

    #[test]
    fn frozen_mv_rewrite_facts_reject_empty_unavailable_observation() {
        let invalid = SqlMvRewriteSourceOccurrenceFacts::try_new(
            SqlMvRelationOccurrenceId::new(7),
            novarocks_types::naming::TableIdentity::new("iceberg", "db", "base"),
            "base".to_string(),
            None,
            SqlMvRewriteBaseTableFacts::unavailable(String::new()),
        );
        assert!(invalid.is_err());
    }

    #[test]
    fn sealed_snapshot_builder_rejects_incomplete_and_duplicate_base_facts() {
        let target = novarocks_types::naming::TableIdentity {
            catalog: "iceberg".to_string(),
            namespace: "db".to_string(),
            table: "mv".to_string(),
        };
        let base = novarocks_types::naming::TableIdentity {
            catalog: "iceberg".to_string(),
            namespace: "db".to_string(),
            table: "base".to_string(),
        };
        assert!(
            SqlImvBaseSnapshotFacts::try_new(
                SqlMvRelationOccurrenceId::new(7),
                base.clone(),
                "base".to_string(),
                -1,
                test_object_id("object")
            )
            .is_err()
        );

        let mut builder =
            SqlImvRewriteSnapshotBuilder::try_new(target, SqlTableBindingId::new_for_test(1), 7)
                .expect("valid sealed snapshot builder");
        builder
            .add_base_snapshot(
                SqlImvBaseSnapshotFacts::try_new(
                    SqlMvRelationOccurrenceId::new(7),
                    base.clone(),
                    "base".to_string(),
                    42,
                    test_object_id("object"),
                )
                .expect("valid base facts"),
            )
            .expect("first base is accepted");
        assert_eq!(builder.base_count(), 1);
        assert!(
            builder
                .add_base_snapshot(
                    SqlImvBaseSnapshotFacts::try_new(
                        SqlMvRelationOccurrenceId::new(7),
                        base,
                        "base".to_string(),
                        43,
                        test_object_id("other")
                    )
                    .expect("valid duplicate shape"),
                )
                .is_err()
        );
        assert!(SqlImvTargetColumnsFacts::try_new(Vec::new()).is_err());
    }

    #[test]
    fn sealed_snapshot_builder_accepts_complete_value_only_facts() {
        let target = novarocks_types::naming::TableIdentity::new("iceberg", "db", "mv");
        let base = novarocks_types::naming::TableIdentity::new("iceberg", "db", "base");
        let mut builder =
            SqlImvRewriteSnapshotBuilder::try_new(target.clone(), test_target_binding(), 7)
                .expect("builder");
        builder
            .add_base_snapshot(
                SqlImvBaseSnapshotFacts::try_new(
                    SqlMvRelationOccurrenceId::new(7),
                    base,
                    "base".to_string(),
                    42,
                    test_object_id("object-base"),
                )
                .expect("base snapshot"),
            )
            .expect("base accepted");
        builder
            .set_target_columns(
                SqlImvTargetColumnsFacts::try_new(vec![novarocks_types::schema::ColumnDef {
                    name: "k".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                }])
                .expect("target columns"),
            )
            .expect("target columns accepted");
        builder
            .set_refresh_history(
                SqlImvRefreshHistoryFacts::try_new(
                    BTreeMap::new(),
                    BTreeMap::new(),
                    Some(10),
                    "uuid-target".to_string(),
                )
                .expect("history"),
            )
            .expect("history accepted");
        let base_contract = SqlImvBaseContractFacts::try_new(
            SqlMvRelationOccurrenceId::new(7),
            "iceberg.db.base".to_string(),
            None,
            vec![
                SqlImvBaseFieldFacts::try_new(
                    bytes::Bytes::from_static(b"field-1"),
                    "k".to_string(),
                    arrow::datatypes::DataType::Int64,
                    false,
                )
                .expect("base field"),
            ],
        )
        .expect("base contract");
        let output = SqlImvOutputColumnFacts::new(
            SqlImvExpressionFacts::try_new(
                SqlImvExpressionKindFacts::Column,
                vec![
                    SqlImvQualifiedFieldFacts::try_new(
                        SqlMvRelationOccurrenceId::new(7),
                        "iceberg.db.base".to_string(),
                        "base".to_string(),
                        bytes::Bytes::from_static(b"field-1"),
                    )
                    .unwrap(),
                ],
            )
            .expect("output lineage"),
        );
        let target_contract = SqlImvTargetContractFacts::try_new(
            vec![
                SqlImvTargetVisibleColumnFacts::try_new(
                    "k".to_string(),
                    bytes::Bytes::from_static(b"field-1"),
                )
                .expect("visible target"),
            ],
            "__nova_base_row_id".to_string(),
            SqlImvApplyKeySourceFacts::BaseRowId,
            None,
        )
        .expect("target contract");
        builder
            .set_schema_contract(
                SqlImvSchemaContractFacts::try_new(
                    vec![base_contract],
                    vec![output],
                    None,
                    None,
                    None,
                    target_contract,
                )
                .expect("schema contract"),
            )
            .expect("schema contract accepted");
        let sealed = builder.build().expect("complete facts seal");
        assert_eq!(sealed.snapshot().target, target);
    }
}
