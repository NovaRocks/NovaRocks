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

use novarocks_parser::ast::Query;
use novarocks_spi::connector::{ConnectorExactSemanticRevision, ConnectorTableObjectId};

use crate::binding::SqlTableBindingId;
use crate::catalog::PlannerTableProvider;
use crate::column_id::ColumnRefFactory;
use crate::optimizer::cascades_rules::mv_rewrite::{
    MvRewriteCandidate, descriptor::SpjgDescriptor,
};
use crate::planner::logical::LogicalPlanNode;

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
pub struct SqlMvRewriteSelectionFacts {
    publication_id: [u8; 16],
    definition_fingerprint: [u8; 32],
    publication_inputs: Vec<SqlMvRewritePublicationRelation>,
    publication_target: SqlMvRewritePublicationRelation,
}

impl SqlMvRewriteSelectionFacts {
    #[cfg(any(test, feature = "test-support"))]
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

    #[cfg(any(test, feature = "test-support"))]
    pub fn try_new_for_target(
        publication_id: [u8; 16],
        definition_fingerprint: [u8; 32],
        publication_inputs: Vec<String>,
        publication_target: String,
    ) -> Result<Self, String> {
        use bytes::Bytes;

        let provider = novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
            .map_err(|error| format!("construct test MV provider: {error}"))?;
        let publication_inputs = publication_inputs
            .into_iter()
            .enumerate()
            .map(|(ordinal, table_fqn)| {
                let object =
                    ConnectorTableObjectId::try_new(Bytes::from(format!("test-input-{ordinal}")))
                        .map_err(|error| error.to_string())?;
                SqlMvRewritePublicationRelation::new(
                    table_fqn,
                    ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
                        provider.clone(),
                        &object,
                        Some(101),
                    )
                    .map_err(|error| error.to_string())?,
                )
            })
            .collect::<Result<Vec<_>, String>>()?;
        let target_object = ConnectorTableObjectId::try_new(Bytes::from_static(b"test-target"))
            .map_err(|error| error.to_string())?;
        Self::try_new_with_publication(
            publication_id,
            definition_fingerprint,
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
        publication_inputs: Vec<SqlMvRewritePublicationRelation>,
        publication_target: SqlMvRewritePublicationRelation,
    ) -> Result<Self, String> {
        if publication_id == [0; 16] || definition_fingerprint == [0; 32] {
            return Err("MV rewrite selection identity cannot be zero".to_string());
        }
        if publication_inputs.is_empty() {
            return Err("MV rewrite selection must name every publication input".to_string());
        }
        let mut unique = publication_inputs
            .iter()
            .map(|input| input.table_fqn.as_str())
            .collect::<Vec<_>>();
        unique.sort_unstable();
        if unique.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err("MV rewrite selection repeats a publication input".to_string());
        }
        Ok(Self {
            publication_id,
            definition_fingerprint,
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

    pub(crate) fn publication_inputs(&self) -> &[SqlMvRewritePublicationRelation] {
        &self.publication_inputs
    }

    pub(crate) const fn publication_target(&self) -> &SqlMvRewritePublicationRelation {
        &self.publication_target
    }
}
use crate::planner::table::ScanSource;

use super::{SqlFunctionCatalog, SqlStatisticsPlan, SqlStatisticsSnapshot};

/// Immutable base-snapshot facts submitted by the application to an IMV
/// rewrite snapshot builder.  This is deliberately a value-only boundary:
/// it contains neither a provider table nor a request lifecycle capability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlImvBaseSnapshotFacts {
    table: novarocks_types::naming::TableIdentity,
    snapshot_id: i64,
    table_object_id: ConnectorTableObjectId,
}

impl SqlImvBaseSnapshotFacts {
    pub fn try_new(
        table: novarocks_types::naming::TableIdentity,
        snapshot_id: i64,
        table_object_id: ConnectorTableObjectId,
    ) -> Result<Self, String> {
        if snapshot_id < 0 || table_object_id.as_bytes().is_empty() {
            return Err("IMV base snapshot facts are incomplete".to_string());
        }
        Ok(Self {
            table,
            snapshot_id,
            table_object_id,
        })
    }

    fn into_snapshot(self) -> SqlImvBaseSnapshot {
        SqlImvBaseSnapshot {
            table: self.table,
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
        if self.base_snapshots.iter().any(|existing| {
            existing
                .table
                .catalog
                .eq_ignore_ascii_case(&base.table.catalog)
                && existing
                    .table
                    .namespace
                    .eq_ignore_ascii_case(&base.table.namespace)
                && existing.table.table.eq_ignore_ascii_case(&base.table.table)
        }) {
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
    pub(crate) fn snapshot(&self) -> &Arc<SqlImvRewriteSnapshot> {
        &self.0
    }
}

/// Frozen refresh-history values.  Snapshot pins are identifiers only; table
/// handles, leases and catalog callbacks are deliberately excluded.
pub struct SqlImvRefreshHistoryFacts {
    previous_snapshot_ids: BTreeMap<String, i64>,
    previous_table_object_ids: BTreeMap<String, ConnectorTableObjectId>,
    target_snapshot_id: Option<i64>,
    target_table_uuid: String,
}

impl SqlImvRefreshHistoryFacts {
    pub fn try_new(
        previous_snapshot_ids: BTreeMap<String, i64>,
        previous_table_object_ids: BTreeMap<String, ConnectorTableObjectId>,
        target_snapshot_id: Option<i64>,
        target_table_uuid: String,
    ) -> Result<Self, String> {
        if target_snapshot_id.is_some_and(|snapshot_id| snapshot_id < 0)
            || target_table_uuid.trim().is_empty()
            || previous_snapshot_ids
                .iter()
                .any(|(table, snapshot_id)| table.trim().is_empty() || *snapshot_id < 0)
            || previous_table_object_ids.iter().any(|(table, object_id)| {
                table.trim().is_empty() || object_id.as_bytes().is_empty()
            })
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
    pub(crate) table: novarocks_types::naming::TableIdentity,
    pub(crate) snapshot_id: i64,
    pub(crate) table_object_id: ConnectorTableObjectId,
}

/// SQL classification of the two physical aggregate-state roles used by the
/// incremental refresh plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlImvAggregateStateRole {
    Single,
    RetractionCount,
}

/// One visible target output in an aggregate refresh layout.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvAggregateVisibleColumn {
    pub(crate) name: String,
    pub(crate) data_type: arrow::datatypes::DataType,
    pub(crate) nullable: bool,
}

/// One physical state column in an aggregate refresh layout.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvAggregateStateColumn {
    pub(crate) name: String,
    pub(crate) data_type: arrow::datatypes::DataType,
    pub(crate) nullable: bool,
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
    pub(crate) table_fqn: String,
    pub(crate) qualifier_at_create: String,
    pub(crate) field_id: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvExpressionLineage {
    pub(crate) kind: SqlImvExpressionKind,
    pub(crate) referenced_base_field_ids: Vec<i32>,
    pub(crate) referenced_base_fields: Vec<SqlImvQualifiedFieldLineage>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvOutputColumnLineage {
    pub(crate) expression: SqlImvExpressionLineage,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvBaseField {
    pub(crate) field_id: i32,
    pub(crate) name_at_create: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlImvBaseContract {
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
    pub(crate) target_field_id: i32,
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
    pub(crate) source_target_field_id: i32,
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
    pub(crate) source_target_field_id: i32,
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
        table_fqn: String,
        qualifier_at_create: String,
        field_id: i32,
    ) -> Result<Self, String> {
        if table_fqn.trim().is_empty() || qualifier_at_create.trim().is_empty() || field_id < 0 {
            return Err("IMV qualified field facts are invalid".to_string());
        }
        Ok(Self {
            inner: SqlImvQualifiedFieldLineage {
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
        referenced_base_field_ids: Vec<i32>,
        referenced_base_fields: Vec<SqlImvQualifiedFieldFacts>,
    ) -> Result<Self, String> {
        if referenced_base_field_ids
            .iter()
            .any(|field_id| *field_id < 0)
            || referenced_base_field_ids
                .windows(2)
                .any(|ids| ids[0] == ids[1])
        {
            return Err("IMV expression field-id facts are invalid".to_string());
        }
        Ok(Self {
            inner: SqlImvExpressionLineage {
                kind: kind.into(),
                referenced_base_field_ids,
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
    pub fn try_new(field_id: i32, name_at_create: String) -> Result<Self, String> {
        if field_id < 0 || name_at_create.trim().is_empty() {
            return Err("IMV base field facts are invalid".to_string());
        }
        Ok(Self {
            inner: SqlImvBaseField {
                field_id,
                name_at_create,
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
        table_fqn: String,
        alias_at_create: Option<String>,
        fields: Vec<SqlImvBaseFieldFacts>,
    ) -> Result<Self, String> {
        if table_fqn.trim().is_empty()
            || fields.is_empty()
            || fields.iter().enumerate().any(|(index, field)| {
                fields[..index]
                    .iter()
                    .any(|other| other.inner.field_id == field.inner.field_id)
            })
        {
            return Err("IMV base contract facts are invalid".to_string());
        }
        Ok(Self {
            inner: SqlImvBaseContract {
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
        if left
            .inner
            .table_fqn
            .eq_ignore_ascii_case(&right.inner.table_fqn)
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
    source_target_field_id: i32,
    transform: SqlImvPartitionTransformFacts,
}

impl SqlImvPartitionFieldFacts {
    pub fn try_new(
        partition_field_name: String,
        source_target_field_id: i32,
        transform: SqlImvPartitionTransformFacts,
    ) -> Result<Self, String> {
        if partition_field_name.trim().is_empty() || source_target_field_id < 0 {
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
    pub fn try_new(output_name: String, target_field_id: i32) -> Result<Self, String> {
        if output_name.trim().is_empty() || target_field_id < 0 {
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
    #[allow(clippy::too_many_arguments)]
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
                bases[..index].iter().any(|other| {
                    other
                        .inner
                        .table_fqn
                        .eq_ignore_ascii_case(&base.inner.table_fqn)
                })
            })
        {
            return Err("IMV schema contract facts are incomplete or duplicate".to_string());
        }
        if join.is_some() && bases.len() < 2 {
            return Err("IMV join contract requires at least two bases".to_string());
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
        data_type: arrow::datatypes::DataType,
        nullable: bool,
    ) -> Result<Self, String> {
        if name.trim().is_empty() {
            return Err("IMV aggregate visible-column facts are invalid".to_string());
        }
        Ok(Self {
            inner: SqlImvAggregateVisibleColumn {
                name,
                data_type,
                nullable,
            },
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlImvAggregateExecutionStateColumnFacts {
    inner: SqlImvAggregateStateColumn,
}

impl SqlImvAggregateExecutionStateColumnFacts {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        name: String,
        data_type: arrow::datatypes::DataType,
        nullable: bool,
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
                data_type,
                nullable,
                visible_source_index,
                aggregate_index,
                function,
                state_role: match state_role {
                    SqlImvAggregateStateRoleFacts::Single => SqlImvAggregateStateRole::Single,
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
    #[allow(clippy::too_many_arguments)]
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
    pub(crate) previous_snapshot_ids: BTreeMap<String, i64>,
    #[allow(
        dead_code,
        reason = "Retained for staged SQL planner migration consumers and test helpers."
    )]
    pub(crate) previous_table_object_ids: BTreeMap<String, ConnectorTableObjectId>,
    pub(crate) target_snapshot_id: Option<i64>,
    pub(crate) target_table_uuid: String,
    /// SQL-safe target field facts projected by the application.  This avoids
    /// exposing an Iceberg schema or Iceberg default-literal values to SQL.
    pub(crate) target_columns: Arc<[novarocks_types::schema::ColumnDef]>,
    /// SQL projection of the persisted MV planning contract frozen at
    /// admission. Resolving or mutating the serialized contract remains in
    /// the application facade.
    pub(crate) schema_contract: Arc<SqlImvSchemaContract>,
    /// Aggregate shape/layout was derived from the admitted MV definition by
    /// application before compiler entry.  Non-aggregate refreshes use None.
    pub(crate) aggregate_execution: Option<SqlImvAggregateExecutionLayout>,
}

impl SqlImvRewriteSnapshot {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_frozen_parts(
        target: novarocks_types::naming::TableIdentity,
        target_binding: SqlTableBindingId,
        mv_id: i64,
        base_snapshots: Arc<[SqlImvBaseSnapshot]>,
        previous_snapshot_ids: BTreeMap<String, i64>,
        previous_table_object_ids: BTreeMap<String, ConnectorTableObjectId>,
        target_snapshot_id: Option<i64>,
        target_table_uuid: String,
        target_columns: Arc<[novarocks_types::schema::ColumnDef]>,
        schema_contract: Arc<SqlImvSchemaContract>,
        aggregate_execution: Option<SqlImvAggregateExecutionLayout>,
    ) -> Result<Self, String> {
        if base_snapshots.is_empty() {
            return Err("IMV rewrite snapshot has no base table snapshots".to_string());
        }
        for base in base_snapshots.iter() {
            if base.table_object_id.as_bytes().is_empty() {
                return Err(format!(
                    "IMV rewrite snapshot base {} has an empty table UUID",
                    base.table.fqn()
                ));
            }
            if let Some(previous_object_id) = previous_table_object_ids.get(&base.table.fqn())
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

    #[allow(
        dead_code,
        reason = "Retained for staged SQL planner migration consumers and test helpers."
    )]
    pub(crate) fn base_snapshot_for_identity(
        &self,
        table: &novarocks_types::naming::TableIdentity,
    ) -> Option<&SqlImvBaseSnapshot> {
        self.base_snapshots.iter().find(|base| {
            base.table.catalog.eq_ignore_ascii_case(&table.catalog)
                && base.table.namespace.eq_ignore_ascii_case(&table.namespace)
                && base.table.table.eq_ignore_ascii_case(&table.table)
        })
    }

    pub(crate) fn base_snapshot_for_parts(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> Option<&SqlImvBaseSnapshot> {
        self.base_snapshots.iter().find(|base| {
            base.table.catalog.eq_ignore_ascii_case(catalog)
                && base.table.namespace.eq_ignore_ascii_case(namespace)
                && base.table.table.eq_ignore_ascii_case(table)
        })
    }
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn test_target_binding() -> SqlTableBindingId {
    use std::num::{NonZeroU32, NonZeroU64};

    SqlTableBindingId::new(
        crate::binding::SqlTableBindingScopeId::new(NonZeroU64::new(1).unwrap()),
        NonZeroU32::new(1).unwrap(),
    )
}

#[cfg(any(test, feature = "test-support"))]
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
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn test_incremental_snapshot() -> Arc<SqlImvRewriteSnapshot> {
    let base = novarocks_types::naming::TableIdentity::new("ice", "db", "b");
    let target = novarocks_types::naming::TableIdentity::new("ice", "db", "mv");
    let mut previous_snapshot_ids = BTreeMap::new();
    previous_snapshot_ids.insert(base.fqn(), 11);
    let mut previous_table_object_ids = BTreeMap::new();
    previous_table_object_ids.insert(base.fqn(), test_object_id("object-b"));
    Arc::new(
        SqlImvRewriteSnapshot::from_frozen_parts(
            target,
            test_target_binding(),
            1,
            Arc::from(vec![SqlImvBaseSnapshot {
                table: base,
                snapshot_id: 22,
                table_object_id: test_object_id("object-b"),
            }]),
            previous_snapshot_ids,
            previous_table_object_ids,
            Some(1),
            "target-uuid".to_string(),
            Arc::from(vec![novarocks_types::schema::ColumnDef {
                name: "k".to_string(),
                data_type: arrow::datatypes::DataType::Int64,
                nullable: false,
                write_default: None,
                logical_type: None,
            }]),
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

#[cfg(any(test, feature = "test-support"))]
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
#[cfg(any(test, feature = "test-support"))]
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
#[cfg(any(test, feature = "test-support"))]
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
    ScanSource::Sql(crate::planner::table::SqlScanSource::new(
        test_target_binding(),
        crate::planner::table::SqlTableIdentity {
            catalog: catalog.to_string(),
            namespace: namespace.to_string(),
            table: table.to_string(),
        },
        kind,
    ))
}

#[cfg(any(test, feature = "test-support"))]
#[allow(
    dead_code,
    reason = "The current-version scan fixture remains available to focused SQL MV rewrite tests."
)]
pub(crate) fn test_data_scan_source() -> ScanSource {
    test_scan_source(crate::planner::table::SqlScanKind::Data {
        version: crate::planner::table::SqlTableVersionSelector::Current,
    })
}

#[cfg(any(test, feature = "test-support"))]
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

#[cfg(any(test, feature = "test-support"))]
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
#[cfg(any(test, feature = "test-support"))]
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
            table_fqn: "ice.db.b".to_string(),
            alias_at_create: None,
            fields: vec![
                SqlImvBaseField {
                    field_id: 1,
                    name_at_create: "k".to_string(),
                },
                SqlImvBaseField {
                    field_id: 2,
                    name_at_create: "v".to_string(),
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
                    target_field_id: 100,
                },
                SqlImvTargetVisibleColumn {
                    output_name: "s".to_string(),
                    target_field_id: 101,
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
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                },
                SqlImvAggregateVisibleColumn {
                    name: "s".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: true,
                },
            ],
            state_columns: state_columns
                .iter()
                .enumerate()
                .map(|(index, column)| SqlImvAggregateStateColumn {
                    name: column.column_name.clone(),
                    data_type: if column.type_signature == "long" {
                        arrow::datatypes::DataType::Int64
                    } else {
                        arrow::datatypes::DataType::Binary
                    },
                    nullable: column.role == SqlImvAggregateStateRoleContract::Single,
                    visible_source_index: 1,
                    aggregate_index: index,
                    function: crate::mv_refresh::AggregateFunctionKind::Sum,
                    state_role: match column.role {
                        SqlImvAggregateStateRoleContract::Single => {
                            SqlImvAggregateStateRole::Single
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
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn test_join_snapshot(aggregate: bool) -> Arc<SqlImvRewriteSnapshot> {
    let qualified =
        |table_fqn: &str, qualifier_at_create: &str, field_id| SqlImvQualifiedFieldLineage {
            table_fqn: table_fqn.to_string(),
            qualifier_at_create: qualifier_at_create.to_string(),
            field_id,
        };
    let base_contract = |table_fqn: &str, alias_at_create: &str| SqlImvBaseContract {
        table_fqn: table_fqn.to_string(),
        alias_at_create: Some(alias_at_create.to_string()),
        fields: vec![
            SqlImvBaseField {
                field_id: 1,
                name_at_create: "k".to_string(),
            },
            SqlImvBaseField {
                field_id: 2,
                name_at_create: "v".to_string(),
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
                    referenced_base_field_ids: Vec::new(),
                    referenced_base_fields: vec![qualified("ice.db.l", "l", 1)],
                },
            },
            SqlImvOutputColumnLineage {
                expression: SqlImvExpressionLineage {
                    kind: SqlImvExpressionKind::Column,
                    referenced_base_field_ids: Vec::new(),
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
                    target_field_id: 100,
                },
                SqlImvTargetVisibleColumn {
                    output_name: "s".to_string(),
                    target_field_id: 101,
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
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                },
                SqlImvAggregateVisibleColumn {
                    name: "s".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: true,
                },
            ],
            state_columns: state_columns
                .iter()
                .enumerate()
                .map(|(aggregate_index, column)| SqlImvAggregateStateColumn {
                    name: column.column_name.clone(),
                    data_type: if column.type_signature == "long" {
                        arrow::datatypes::DataType::Int64
                    } else {
                        arrow::datatypes::DataType::Binary
                    },
                    nullable: column.role == SqlImvAggregateStateRoleContract::Single,
                    visible_source_index: 1,
                    aggregate_index,
                    function: crate::mv_refresh::AggregateFunctionKind::Sum,
                    state_role: match column.role {
                        SqlImvAggregateStateRoleContract::Single => {
                            SqlImvAggregateStateRole::Single
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
                    table: novarocks_types::naming::TableIdentity::new("ice", "db", "l"),
                    snapshot_id: 22,
                    table_object_id: test_object_id("object-l"),
                },
                SqlImvBaseSnapshot {
                    table: novarocks_types::naming::TableIdentity::new("ice", "db", "r"),
                    snapshot_id: 44,
                    table_object_id: test_object_id("object-r"),
                },
            ]),
            BTreeMap::from([("ice.db.l".to_string(), 11), ("ice.db.r".to_string(), 33)]),
            BTreeMap::from([
                ("ice.db.l".to_string(), test_object_id("object-l")),
                ("ice.db.r".to_string(), test_object_id("object-r")),
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
            table_fqn: "ice.db.b".to_string(),
            alias_at_create: None,
            fields: vec![
                SqlImvBaseField {
                    field_id: 1,
                    name_at_create: "region".to_string(),
                },
                SqlImvBaseField {
                    field_id: 2,
                    name_at_create: "amount".to_string(),
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
                    target_field_id: 100,
                },
                SqlImvTargetVisibleColumn {
                    output_name: "s".to_string(),
                    target_field_id: 101,
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
    pub(crate) candidates: Vec<MvRewriteCandidate>,
    pub(crate) diagnostics: Vec<SqlMvRewriteDiagnostic>,
}

/// One immutable base-table observation frozen by application admission for
/// optional MV rewrite. This is value-only: it has no provider, lease, or
/// catalog capability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlMvRewriteBaseTableFacts {
    state: SqlMvRewriteBaseTableFactsState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SqlMvRewriteBaseTableFactsState {
    Resolved {
        snapshot_id: Option<i64>,
        table_object_id: Option<ConnectorTableObjectId>,
    },
    Unavailable(String),
}

impl SqlMvRewriteBaseTableFacts {
    pub fn resolved(
        snapshot_id: Option<i64>,
        table_object_id: Option<ConnectorTableObjectId>,
    ) -> Self {
        Self {
            state: SqlMvRewriteBaseTableFactsState::Resolved {
                snapshot_id,
                table_object_id,
            },
        }
    }

    pub fn unavailable(message: String) -> Self {
        Self {
            state: SqlMvRewriteBaseTableFactsState::Unavailable(message),
        }
    }

    fn into_state(self) -> MvRewriteBaseTableState {
        match self.state {
            SqlMvRewriteBaseTableFactsState::Resolved {
                snapshot_id,
                table_object_id,
            } => MvRewriteBaseTableState::Resolved {
                snapshot_id,
                table_object_id,
            },
            SqlMvRewriteBaseTableFactsState::Unavailable(message) => {
                MvRewriteBaseTableState::Unavailable(message)
            }
        }
    }
}

/// Immutable persisted-MV facts frozen by application admission. SQL accepts
/// copied repository and connector observations only; it keeps candidate
/// parsing, analysis, and optimizer descriptors private.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlMvRewriteDefinitionFacts {
    mv_id: i64,
    select_query: Query,
    base_table_refs: Vec<String>,
    storage_engine: String,
    target_catalog: Option<String>,
    target_namespace: Option<String>,
    target_table: Option<String>,
    last_refresh_snapshots: BTreeMap<String, i64>,
    last_refresh_table_object_ids: BTreeMap<String, ConnectorTableObjectId>,
    base_table_states: BTreeMap<String, SqlMvRewriteBaseTableFacts>,
    selection: Option<SqlMvRewriteSelectionFacts>,
    selection_unavailable: Option<String>,
}

impl SqlMvRewriteDefinitionFacts {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        mv_id: i64,
        select_query: Query,
        base_table_refs: Vec<String>,
        storage_engine: String,
        target_catalog: Option<String>,
        target_namespace: Option<String>,
        target_table: Option<String>,
        last_refresh_snapshots: BTreeMap<String, i64>,
        last_refresh_table_object_ids: BTreeMap<String, ConnectorTableObjectId>,
        base_table_states: BTreeMap<String, SqlMvRewriteBaseTableFacts>,
    ) -> Result<Self, String> {
        if base_table_states
            .values()
            .any(|state| matches!(&state.state, SqlMvRewriteBaseTableFactsState::Unavailable(message) if message.trim().is_empty()))
        {
            return Err("MV rewrite unavailable base-table fact cannot be empty".to_string());
        }
        Ok(Self {
            mv_id,
            select_query,
            base_table_refs,
            storage_engine,
            target_catalog,
            target_namespace,
            target_table,
            last_refresh_snapshots,
            last_refresh_table_object_ids,
            base_table_states,
            selection: None,
            selection_unavailable: None,
        })
    }

    pub fn with_selection_facts(mut self, selection: SqlMvRewriteSelectionFacts) -> Self {
        self.selection = Some(selection);
        self.selection_unavailable = None;
        self
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
        MvRewriteDefinition {
            mv_id: self.mv_id,
            select_query: self.select_query,
            base_table_refs: self.base_table_refs,
            storage_engine: self.storage_engine,
            target_catalog: self.target_catalog,
            target_namespace: self.target_namespace,
            target_table: self.target_table,
            last_refresh_snapshots: self.last_refresh_snapshots,
            last_refresh_table_object_ids: self.last_refresh_table_object_ids,
            base_table_states: self
                .base_table_states
                .into_iter()
                .map(|(fqn, state)| (fqn, state.into_state()))
                .collect(),
            selection: self.selection,
            selection_unavailable: self.selection_unavailable,
        }
    }
}

/// SQL-private state used after the frozen facts have crossed the application
/// boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
enum MvRewriteBaseTableState {
    Resolved {
        snapshot_id: Option<i64>,
        table_object_id: Option<ConnectorTableObjectId>,
    },
    Unavailable(String),
}

/// Immutable facts required to assess one persisted MV as a rewrite candidate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MvRewriteDefinition {
    pub(crate) mv_id: i64,
    pub(crate) select_query: Query,
    pub(crate) base_table_refs: Vec<String>,
    pub(crate) storage_engine: String,
    pub(crate) target_catalog: Option<String>,
    pub(crate) target_namespace: Option<String>,
    pub(crate) target_table: Option<String>,
    pub(crate) last_refresh_snapshots: BTreeMap<String, i64>,
    pub(crate) last_refresh_table_object_ids: BTreeMap<String, ConnectorTableObjectId>,
    /// Per-base-table reads (including failures) captured while admission
    /// froze this definition. The map is keyed by canonical `cat.ns.tbl`.
    #[expect(
        private_interfaces,
        reason = "The stable SQL shape intentionally carries a crate-private implementation detail."
    )]
    pub(crate) base_table_states: BTreeMap<String, MvRewriteBaseTableState>,
    pub(crate) selection: Option<SqlMvRewriteSelectionFacts>,
    pub(crate) selection_unavailable: Option<String>,
}

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
    entries: Vec<SqlMvRewriteAnalysisEntry>,
}

impl SqlMvRewriteAnalysis {
    pub(crate) fn empty() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

/// Prepare optional MV rewrite candidates from one immutable, repository-order
/// definition index. This is deliberately SQL-owned: application admission
/// freezes definitions and base-table observations, while parse/analyze,
/// descriptor construction and catalog materialization happen before the
/// application freezes request-local statistics. Statistics attachment and
/// warn-and-skip selection are deliberately deferred to the seal phase.
#[expect(
    clippy::too_many_arguments,
    reason = "These are distinct frozen SQL planning facts and grouping them would obscure the compiler boundary."
)]
pub(crate) fn analyze_candidates(
    definitions: &MvRewriteDefinitionIndex,
    analyzer_catalog: &dyn PlannerTableProvider,
    current_database: &str,
    logical: &LogicalPlanNode,
    factory: &ColumnRefFactory,
    functions: &dyn SqlFunctionCatalog,
    optimizer_settings: &crate::optimizer::options::SessionOptimizerSettings,
    control: &crate::compiler::SqlCompileControl,
) -> Result<SqlMvRewriteAnalysis, crate::compiler::SqlCompileError> {
    if !optimizer_settings.mv_rewrite_enabled() {
        return Ok(SqlMvRewriteAnalysis::empty());
    }

    let mut query_fqns = Vec::new();
    collect_iceberg_fqns(logical, &mut query_fqns);
    if query_fqns.is_empty() {
        return Ok(SqlMvRewriteAnalysis::empty());
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
                .base_table_refs
                .iter()
                .any(|base| query_fqns.contains(base))
        {
            entries.push(SqlMvRewriteAnalysisEntry::Ignored);
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
        ) {
            Ok(Some(candidate)) => {
                candidate_factory = candidate.factory_after_analysis.clone();
                materialized_candidates += 1;
                entries.push(SqlMvRewriteAnalysisEntry::Candidate(candidate));
            }
            Ok(None) => entries.push(SqlMvRewriteAnalysisEntry::Ignored),
            Err(error) => entries.push(SqlMvRewriteAnalysisEntry::Diagnostic(
                SqlMvRewriteDiagnostic {
                    mv_id: Some(definition.mv_id),
                    message: format!("mv rewrite: skipping frozen candidate: {error}"),
                },
            )),
        }
        control.check()?;
    }

    Ok(SqlMvRewriteAnalysis { entries })
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
            candidates,
            diagnostics,
        },
        factory,
    ))
}

fn build_candidate(
    analyzer_catalog: &dyn PlannerTableProvider,
    current_database: &str,
    definition: &MvRewriteDefinition,
    factory: &ColumnRefFactory,
    functions: &dyn SqlFunctionCatalog,
) -> Result<Option<AnalyzedMvRewriteCandidate>, String> {
    if definition.last_refresh_snapshots.is_empty() || !definition_is_fresh(definition)? {
        return Ok(None);
    }

    let select = definition.select_query.clone();
    let (resolved, ctes, returned) = crate::analyzer::analyze_with_factory_and_function_catalog(
        &select,
        analyzer_catalog,
        current_database,
        factory.clone(),
        functions,
    )
    .map_err(|error| error.to_string())?;
    let mut returned = returned;
    let mv_logical = crate::planner::plan_query(resolved, ctes, &mut returned)?;
    let mut mv_scalars = crate::optimizer::scalar::ScalarArena::new();
    let mv_opt_expr = crate::planner::optimizer_bridge::logical::try_to_optimizer_expr(
        &mv_logical,
        &mut mv_scalars,
    )?;
    let mv = SpjgDescriptor::from_opt_expr(&mv_opt_expr, &mut mv_scalars)?;
    if mv.joins.is_some() {
        return Ok(None);
    }
    let Some(scan_fqn) = scan_fqn(&mv.table.source) else {
        return Ok(None);
    };
    if !definition.base_table_refs.contains(&scan_fqn) {
        return Err(format!(
            "mv select resolved to {scan_fqn}, not in recorded base refs"
        ));
    }
    let (Some(catalog), Some(namespace), Some(table)) = (
        &definition.target_catalog,
        &definition.target_namespace,
        &definition.target_table,
    ) else {
        return Ok(None);
    };
    let target_table = analyzer_catalog
        .resolve_table_for_analysis(Some(catalog), namespace, table)?
        .planner;
    let mut names = mv
        .outputs
        .iter()
        .map(|output| output.name.as_str())
        .collect::<Vec<_>>();
    names.sort_unstable();
    if names.windows(2).any(|pair| pair[0] == pair[1]) {
        return Ok(None);
    }
    Ok(Some(AnalyzedMvRewriteCandidate {
        mv_name: table.to_string(),
        mv,
        mv_scalars,
        target_database: namespace.to_string(),
        target_table,
        factory_after_analysis: returned,
        selection: definition.selection.clone(),
    }))
}

fn definition_is_fresh(definition: &MvRewriteDefinition) -> Result<bool, String> {
    for base in &definition.base_table_refs {
        let Some(pinned_snapshot) = definition.last_refresh_snapshots.get(base) else {
            return Ok(false);
        };
        match definition.base_table_states.get(base) {
            Some(MvRewriteBaseTableState::Resolved {
                snapshot_id,
                table_object_id,
            }) => {
                if *snapshot_id != Some(*pinned_snapshot) {
                    return Ok(false);
                }
                if let Some(pinned_object_id) = definition.last_refresh_table_object_ids.get(base)
                    && table_object_id.as_ref() != Some(pinned_object_id)
                {
                    return Ok(false);
                }
            }
            Some(MvRewriteBaseTableState::Unavailable(error)) => {
                return Err(format!("read frozen base table {base}: {error}"));
            }
            None => return Err(format!("missing frozen base table state for {base}")),
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
        MvRewriteDefinitionIndex::try_new(vec![
            SqlMvRewriteDefinitionFacts::try_new(
                1,
                test_query("select k from iceberg.db.base"),
                vec!["iceberg.db.base".to_string()],
                "iceberg".to_string(),
                Some("iceberg".to_string()),
                Some("db".to_string()),
                Some("mv_target".to_string()),
                BTreeMap::from([("iceberg.db.base".to_string(), 42)]),
                BTreeMap::new(),
                BTreeMap::from([(
                    "iceberg.db.base".to_string(),
                    SqlMvRewriteBaseTableFacts::resolved(Some(42), None),
                )]),
            )
            .expect("candidate definition")
            .with_selection_facts(
                SqlMvRewriteSelectionFacts::try_new(
                    [7; 16],
                    [9; 32],
                    vec!["iceberg.db.base".to_string()],
                )
                .expect("candidate publication proof"),
            ),
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
        )
        .expect("analyze main query");
        let logical =
            crate::planner::plan_query(resolved, ctes, &mut factory).expect("plan main query");
        (logical, factory)
    }

    fn frozen_definition(state: SqlMvRewriteBaseTableFacts) -> MvRewriteDefinition {
        SqlMvRewriteDefinitionFacts::try_new(
            1,
            test_query("select 1"),
            vec!["iceberg.db.base".to_string()],
            "iceberg".to_string(),
            Some("iceberg".to_string()),
            Some("db".to_string()),
            Some("mv_target".to_string()),
            BTreeMap::from([("iceberg.db.base".to_string(), 42)]),
            BTreeMap::from([(
                "iceberg.db.base".to_string(),
                test_object_id("original-object"),
            )]),
            BTreeMap::from([("iceberg.db.base".to_string(), state)]),
        )
        .expect("valid frozen definition facts")
        .into_definition()
    }

    #[test]
    fn sqlx1_mv_rewrite_definition_index_preserves_application_order() {
        let index = MvRewriteDefinitionIndex::try_new(vec![
            SqlMvRewriteDefinitionFacts::try_new(
                7,
                test_query("select 1"),
                Vec::new(),
                "iceberg".to_string(),
                None,
                None,
                None,
                BTreeMap::new(),
                BTreeMap::new(),
                BTreeMap::new(),
            )
            .expect("valid first frozen definition"),
            SqlMvRewriteDefinitionFacts::try_new(
                3,
                test_query("select 2"),
                Vec::new(),
                "iceberg".to_string(),
                None,
                None,
                None,
                BTreeMap::new(),
                BTreeMap::new(),
                BTreeMap::new(),
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
        let fresh = frozen_definition(SqlMvRewriteBaseTableFacts::resolved(
            Some(42),
            Some(test_object_id("original-object")),
        ));
        let stale = frozen_definition(SqlMvRewriteBaseTableFacts::resolved(
            Some(43),
            Some(test_object_id("original-object")),
        ));
        let recreated = frozen_definition(SqlMvRewriteBaseTableFacts::resolved(
            Some(42),
            Some(test_object_id("replacement-object")),
        ));

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
        let analysis = analyze_candidates(
            &candidate_index(),
            &catalog,
            "db",
            &logical,
            &factory,
            crate::functions::builtin_sql_function_catalog(),
            &crate::optimizer::options::SessionOptimizerSettings::default(),
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .expect("analyze candidate before statistics freeze");
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
        assert_eq!(catalog.resolutions.load(Ordering::Acquire), 3);

        let analysis = analyze_candidates(
            &candidate_index(),
            &catalog,
            "db",
            &logical,
            &factory,
            crate::functions::builtin_sql_function_catalog(),
            &crate::optimizer::options::SessionOptimizerSettings::default(),
            &crate::compiler::SqlCompileControl::unbounded(),
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
            &crate::compiler::SqlCompileControl::unbounded(),
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
        let invalid = SqlMvRewriteDefinitionFacts::try_new(
            1,
            test_query("select 1"),
            Vec::new(),
            "iceberg".to_string(),
            None,
            None,
            None,
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::from([(
                "iceberg.db.base".to_string(),
                SqlMvRewriteBaseTableFacts::unavailable(String::new()),
            )]),
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
            SqlImvBaseSnapshotFacts::try_new(base.clone(), -1, test_object_id("object")).is_err()
        );

        let mut builder =
            SqlImvRewriteSnapshotBuilder::try_new(target, SqlTableBindingId::new_for_test(1), 7)
                .expect("valid sealed snapshot builder");
        builder
            .add_base_snapshot(
                SqlImvBaseSnapshotFacts::try_new(base.clone(), 42, test_object_id("object"))
                    .expect("valid base facts"),
            )
            .expect("first base is accepted");
        assert_eq!(builder.base_count(), 1);
        assert!(
            builder
                .add_base_snapshot(
                    SqlImvBaseSnapshotFacts::try_new(base, 43, test_object_id("other"))
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
                SqlImvBaseSnapshotFacts::try_new(base, 42, test_object_id("object-base"))
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
            "iceberg.db.base".to_string(),
            None,
            vec![SqlImvBaseFieldFacts::try_new(1, "k".to_string()).expect("base field")],
        )
        .expect("base contract");
        let output = SqlImvOutputColumnFacts::new(
            SqlImvExpressionFacts::try_new(SqlImvExpressionKindFacts::Column, vec![1], Vec::new())
                .expect("output lineage"),
        );
        let target_contract = SqlImvTargetContractFacts::try_new(
            vec![
                SqlImvTargetVisibleColumnFacts::try_new("k".to_string(), 1)
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
