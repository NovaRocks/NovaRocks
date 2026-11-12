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

//! Canonical documents frozen at the application-to-SQL rewrite boundary.
//! Runtime handles stay in the caller. Provider identities are never decoded.

use crate::mv::domain::refresh::planning::RefreshBaseRelationOccurrence;
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use bytes::Bytes;
use novarocks_mv_application::persistence::{
    codec::{ApplyKeyKind, DefinitionDocument, ExpressionKind, SourceFieldReference, StateRole},
    exact_revision::{persist_exact_connector_revision, restore_exact_query_revision},
    identity::{DocumentRevision, PartitionSpecVersion},
    keyed_documents::StoredKeyedMvProjection,
    runtime_bindings::{
        MvExactTargetSchemaFacts, MvPhysicalFieldFacts, MvRuntimeBindings,
        reconstruct_keyed_runtime_bindings,
    },
};
use novarocks_spi::connector::{ConnectorExactSemanticRevision, ConnectorTableObjectId};
use novarocks_sql::binding::SqlTableBindingId;
use novarocks_sql::compiler::{
    SqlImvAggregateContractFacts, SqlImvAggregateExecutionFacts,
    SqlImvAggregateExecutionStateColumnFacts, SqlImvAggregateStateColumnFacts,
    SqlImvAggregateStateRoleFacts, SqlImvAggregateVisibleColumnFacts, SqlImvApplyKeySourceFacts,
    SqlImvBaseContractFacts, SqlImvBaseFieldFacts, SqlImvBaseSnapshotFacts,
    SqlImvBranchContractFacts, SqlImvExpressionFacts, SqlImvExpressionKindFacts,
    SqlImvJoinContractFacts, SqlImvOutputColumnFacts, SqlImvPartitionFacts,
    SqlImvQualifiedFieldFacts, SqlImvRefreshHistoryFacts, SqlImvRewriteSnapshotBuilder,
    SqlImvRewriteSnapshotHandle, SqlImvSchemaContractFacts, SqlImvTargetColumnsFacts,
    SqlImvTargetContractFacts, SqlImvTargetVisibleColumnFacts, SqlMvRelationOccurrenceId,
};
use novarocks_sql::planning::mv::SqlMvAggregateCalls;
use novarocks_sql::planning::mv_aggregate_layout::SqlMvAggregatePhysicalLayout;
use novarocks_types::naming::TableIdentity;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// Facts from one provider observation of one D occurrence. Equal logical
/// names never merge two occurrences.
#[derive(Clone, Debug)]
pub struct MvRewriteSourceSnapshot {
    pub occurrence_id: SqlMvRelationOccurrenceId,
    pub snapshot_id: i64,
    pub table_object_id: ConnectorTableObjectId,
    pub semantic_revision: ConnectorExactSemanticRevision,
}

/// Analysis of this exact D under D's resolution context. Join predicates are
/// SQL facts; partition transforms must come from the same provider generation.
#[derive(Clone, Debug)]
pub struct MvRewriteAnalysisFacts {
    pub definition_revision: DocumentRevision,
    pub partition_spec_version: PartitionSpecVersion,
    pub join: Option<SqlImvJoinContractFacts>,
    pub partition: Option<SqlImvPartitionFacts>,
    pub aggregate: Option<MvRewriteAggregateAnalysis>,
}

#[derive(Clone, Debug)]
pub struct MvRewriteAggregateAnalysis {
    pub calls: SqlMvAggregateCalls,
    pub layout: SqlMvAggregatePhysicalLayout,
}

#[derive(Debug)]
pub struct IcebergMvRewriteContext {
    pub target: TableIdentity,
    pub mv_id: i64,
    pub current_catalog: Option<String>,
    pub current_database: String,
    pub mv_definition: Arc<StoredKeyedMvProjection>,
    pub canonical_select_query: Arc<novarocks_parser::ast::Query>,
    /// Ordered, one entry per base scan, in D's own occurrence order.
    pub base_refs: Arc<[RefreshBaseRelationOccurrence]>,
    pub pin: Arc<BTreeMap<SqlMvRelationOccurrenceId, MvRewriteSourceSnapshot>>,
    /// Retained complete history facts for activation; numeric projections
    /// below must never become an alternate source of publication identity.
    pub previous: Arc<[MvRewriteSourceSnapshot]>,
    pub previous_snapshot_ids: BTreeMap<SqlMvRelationOccurrenceId, i64>,
    pub previous_table_object_ids: BTreeMap<SqlMvRelationOccurrenceId, ConnectorTableObjectId>,
    pub target_snapshot_id: Option<i64>,
    pub target_table_uuid: String,
    pub(crate) target_write_fields: Arc<[novarocks_spi::connector::ConnectorWriteFieldRequest]>,
    pub runtime_bindings: MvRuntimeBindings,
    analysis: MvRewriteAnalysisFacts,
    sql_schema: SqlImvSchemaContractFacts,
}

impl IcebergMvRewriteContext {
    /// The exact revision each D occurrence was read at, keyed by D's own
    /// occurrence id. This is what P records as its input watermark, so the
    /// repeated occurrences of one self-joined object stay separate.
    pub fn exact_revisions_by_occurrence(
        &self,
    ) -> BTreeMap<u32, novarocks_spi::connector::ConnectorExactSemanticRevision> {
        self.pin
            .iter()
            .map(|(occurrence_id, source)| (occurrence_id.get(), source.semantic_revision.clone()))
            .collect()
    }

    pub fn from_parts(
        projection: Arc<StoredKeyedMvProjection>,
        current: Vec<MvRewriteSourceSnapshot>,
        previous: Vec<MvRewriteSourceSnapshot>,
        target_table_uuid: String,
        target_arrow_schema: SchemaRef,
        target_write_fields: Arc<[novarocks_spi::connector::ConnectorWriteFieldRequest]>,
        exact_target: MvExactTargetSchemaFacts,
        analysis: MvRewriteAnalysisFacts,
    ) -> Result<Self, String> {
        let facts = &projection.facts;
        if analysis.definition_revision != facts.source_revision().definition_revision
            || analysis.partition_spec_version
                != facts.interpretation().target.partition_spec_version
        {
            return Err("MV rewrite analysis belongs to a different D/L generation".into());
        }
        if target_table_uuid.is_empty() {
            return Err("MV rewrite target UUID is absent".into());
        }
        if target_write_fields.len() != target_arrow_schema.fields().len()
            || target_write_fields
                .iter()
                .zip(target_arrow_schema.fields())
                .any(|(request, field)| {
                    request.execution_type().is_none()
                        || !novarocks_type_contract::arrow_fields_exact(
                            request.field(),
                            field.as_ref(),
                        )
                })
        {
            return Err("MV rewrite target fields lack their exact complete declarations".into());
        }
        let runtime_bindings = reconstruct_keyed_runtime_bindings(facts, &exact_target)?;
        validate_target_schema(&target_arrow_schema, &exact_target)?;
        let pin = validate_source_pins(facts.definition(), current)?;
        let (previous_snapshot_ids, previous_table_object_ids) =
            validate_history(&projection, previous.clone())?;
        for (occurrence, previous_object) in &previous_table_object_ids {
            if pin.get(occurrence).map(|value| &value.table_object_id) != Some(previous_object) {
                return Err("MV rewrite source object changed since publication".into());
            }
        }
        validate_aggregate_analysis(&runtime_bindings, &analysis)?;
        let sql_schema = sql_schema_facts(&projection, &runtime_bindings, &analysis)?;
        let definition = facts.definition();
        let canonical_select_query = Arc::new(
            crate::mv::domain::refresh::definition::read_stored_mv_query(
                &definition.query.effective_sql,
                &crate::mv::domain::refresh::definition::stored_mv_identity(&projection),
            )
            .map_err(|error| error.to_string())?,
        );
        let base_refs = definition
            .relation_occurrences
            .iter()
            .map(|occurrence| RefreshBaseRelationOccurrence {
                occurrence_id: SqlMvRelationOccurrenceId::new(occurrence.occurrence_id),
                table: occurrence_table(occurrence),
            })
            .collect::<Vec<_>>();
        let target = facts.target();
        let target_snapshot_id = facts.metadata_version().snapshot_id();
        Ok(Self {
            target: TableIdentity {
                catalog: target
                    .catalog()
                    .ok_or("MV rewrite target has no catalog")?
                    .to_string(),
                namespace: target.namespace().to_string(),
                table: target.name().to_string(),
            },
            mv_id: projection.mv_id,
            current_catalog: Some(definition.query.resolution.default_catalog.clone()),
            current_database: definition.query.resolution.default_namespace.clone(),
            mv_definition: projection,
            canonical_select_query,
            base_refs: base_refs.into(),
            pin: Arc::new(pin),
            previous: previous.into(),
            previous_snapshot_ids,
            previous_table_object_ids,
            target_snapshot_id,
            target_table_uuid,
            target_write_fields,
            runtime_bindings,
            analysis,
            sql_schema,
        })
    }

    pub fn analysis_facts(&self) -> &MvRewriteAnalysisFacts {
        &self.analysis
    }

    /// The single D occurrence that names `table`.
    ///
    /// A definition may reference the same relation more than once. Those are
    /// distinct occurrences and must never be merged, so a locator-keyed
    /// lookup over a repeated relation is an explicit error rather than an
    /// arbitrary winner.
    /// Pinned current snapshot for one base relation occurrence.
    ///
    /// By occurrence rather than by table: a definition may read one table
    /// twice, and asking for "the pin for this table" would have to pick one
    /// of the two.
    pub(crate) fn pinned_snapshot_id(
        &self,
        base: &RefreshBaseRelationOccurrence,
    ) -> Result<i64, String> {
        self.pin
            .get(&base.occurrence_id)
            .map(|value| value.snapshot_id)
            .ok_or_else(|| format!("MV refresh pin has no entry for {}", base.display()))
    }

    /// Pinned current object identity for one base relation occurrence.
    pub(crate) fn pinned_table_object_id(
        &self,
        base: &RefreshBaseRelationOccurrence,
    ) -> Result<ConnectorTableObjectId, String> {
        self.pin
            .get(&base.occurrence_id)
            .map(|value| value.table_object_id.clone())
            .ok_or_else(|| format!("MV refresh pin has no entry for {}", base.display()))
    }

    /// Published predecessor snapshot for one base relation occurrence.
    pub(crate) fn previous_snapshot_id(
        &self,
        base: &RefreshBaseRelationOccurrence,
    ) -> Result<i64, String> {
        self.previous_snapshot_ids
            .get(&base.occurrence_id)
            .copied()
            .ok_or_else(|| {
                format!(
                    "MV refresh has no published predecessor snapshot for {}",
                    base.display()
                )
            })
    }

    pub(crate) fn previous_revision(
        &self,
        base: &RefreshBaseRelationOccurrence,
    ) -> Result<&ConnectorExactSemanticRevision, String> {
        let mut matches = self
            .previous
            .iter()
            .filter(|source| source.occurrence_id == base.occurrence_id);
        let source = matches
            .next()
            .ok_or_else(|| format!("MV refresh has no exact predecessor for {}", base.display()))?;
        if matches.next().is_some() {
            return Err(format!(
                "MV refresh repeats the exact predecessor for {}",
                base.display()
            ));
        }
        Ok(&source.semantic_revision)
    }

    /// Locator-keyed projections for the legacy publication intent. Both fail
    /// rather than merge when one relation occurs twice.
    /// What each occurrence was pinned at, with the table it names.
    ///
    /// Keyed by occurrence rather than by table: a definition may read one
    /// table twice, and each mention carries its own pin.
    pub(crate) fn pinned_snapshots_by_occurrence(
        &self,
    ) -> Result<BTreeMap<SqlMvRelationOccurrenceId, (TableIdentity, i64)>, String> {
        self.occurrence_keyed(|table, value| (table, value.snapshot_id))
    }

    pub(crate) fn pinned_objects_by_occurrence(
        &self,
    ) -> Result<BTreeMap<SqlMvRelationOccurrenceId, ConnectorTableObjectId>, String> {
        self.occurrence_keyed(|_, value| value.table_object_id.clone())
    }

    fn occurrence_keyed<T>(
        &self,
        project: impl Fn(TableIdentity, &MvRewriteSourceSnapshot) -> T,
    ) -> Result<BTreeMap<SqlMvRelationOccurrenceId, T>, String> {
        let mut values = BTreeMap::new();
        for occurrence in &self.mv_definition.facts.definition().relation_occurrences {
            let table = occurrence_table(occurrence);
            let occurrence_id = SqlMvRelationOccurrenceId::new(occurrence.occurrence_id);
            let pin = self
                .pin
                .get(&occurrence_id)
                .ok_or_else(|| format!("MV refresh pin has no entry for {}", table.fqn()))?;
            if values
                .insert(occurrence_id, project(table.clone(), pin))
                .is_some()
            {
                return Err(format!(
                    "MV definition records occurrence {} more than once",
                    occurrence.occurrence_id
                ));
            }
        }
        Ok(values)
    }

    pub(crate) fn to_sql_rewrite_snapshot_with_target_file_metadata(
        &self,
        target_binding: SqlTableBindingId,
        target_metadata: &novarocks_spi::connector::ConnectorTableMetadata,
    ) -> Result<SqlImvRewriteSnapshotHandle, String> {
        self.to_sql_rewrite_snapshot(target_binding)?
            .with_target_file_metadata(
                crate::query_execution::mv_assembly::iceberg_activation::target_file_metadata(
                    target_metadata,
                )?,
            )
    }

    pub fn to_sql_rewrite_snapshot(
        &self,
        target_binding: SqlTableBindingId,
    ) -> Result<SqlImvRewriteSnapshotHandle, String> {
        let mut builder =
            SqlImvRewriteSnapshotBuilder::try_new(self.target.clone(), target_binding, self.mv_id)?;
        for occurrence in &self.mv_definition.facts.definition().relation_occurrences {
            let id = SqlMvRelationOccurrenceId::new(occurrence.occurrence_id);
            let pin = self
                .pin
                .get(&id)
                .ok_or("MV rewrite occurrence pin is missing")?;
            builder.add_base_snapshot(SqlImvBaseSnapshotFacts::try_new(
                id,
                occurrence_table(occurrence),
                occurrence.qualifier_at_binding.clone(),
                pin.snapshot_id,
                pin.table_object_id.clone(),
            )?)?;
        }
        builder.set_target_columns(SqlImvTargetColumnsFacts::try_new(
            self.target_write_fields
                .iter()
                .map(|field| {
                    let declared = field.execution_type().ok_or_else(|| {
                        "MV target has no complete execution declaration".to_string()
                    })?;
                    novarocks_types::schema::ColumnDef::from_value_type(
                        field.field().name().clone(),
                        declared.clone(),
                        None,
                    )
                    .map_err(|error| error.to_string())
                })
                .collect::<Result<Vec<_>, String>>()?,
        )?)?;
        builder.set_refresh_history(SqlImvRefreshHistoryFacts::try_new(
            self.previous_snapshot_ids.clone(),
            self.previous_table_object_ids.clone(),
            self.target_snapshot_id,
            self.target_table_uuid.clone(),
        )?)?;
        builder.set_schema_contract(self.sql_schema.clone())?;
        if let Some(analysis) = &self.analysis.aggregate {
            builder.set_aggregate_execution(aggregate_execution_facts(analysis)?)?;
        }
        builder.build()
    }
}

pub(super) fn occurrence_table(
    value: &novarocks_mv_application::persistence::codec::RelationOccurrence,
) -> TableIdentity {
    TableIdentity {
        catalog: value.catalog_at_binding.clone(),
        namespace: value.namespace_at_binding.clone(),
        table: value.relation_at_binding.clone(),
    }
}

fn validate_source_pins(
    definition: &DefinitionDocument,
    values: Vec<MvRewriteSourceSnapshot>,
) -> Result<BTreeMap<SqlMvRelationOccurrenceId, MvRewriteSourceSnapshot>, String> {
    let mut pins = BTreeMap::new();
    for value in values {
        if value.snapshot_id < 0 || pins.insert(value.occurrence_id, value).is_some() {
            return Err("MV rewrite source pins contain an invalid or repeated occurrence".into());
        }
    }
    if pins.len() != definition.relation_occurrences.len() {
        return Err("MV rewrite source pins do not cover every D occurrence".into());
    }
    for occurrence in &definition.relation_occurrences {
        let pin = pins
            .get(&SqlMvRelationOccurrenceId::new(occurrence.occurrence_id))
            .ok_or("MV rewrite source pin refers to an unknown occurrence")?;
        if pin.semantic_revision.object_identity().value().as_ref()
            != pin.table_object_id.as_bytes().as_ref()
        {
            return Err("MV rewrite source pin carries conflicting exact object facts".into());
        }
        let (persisted_object, _) = persist_exact_connector_revision(&pin.semantic_revision)
            .map_err(|error| format!("persist MV rewrite source identity: {error}"))?;
        if occurrence.object_id != persisted_object {
            return Err("MV rewrite pinned source object differs from D".into());
        }
    }
    Ok(pins)
}

type RefreshHistory = (
    BTreeMap<SqlMvRelationOccurrenceId, i64>,
    BTreeMap<SqlMvRelationOccurrenceId, ConnectorTableObjectId>,
);

fn validate_history(
    projection: &StoredKeyedMvProjection,
    previous: Vec<MvRewriteSourceSnapshot>,
) -> Result<RefreshHistory, String> {
    let published = match projection.facts.publication() {
        None if previous.is_empty() => {
            return Ok((BTreeMap::new(), BTreeMap::new()));
        }
        None => {
            return Err("NeverPublished MV cannot have refresh history".into());
        }
        Some(published) => published,
    };
    let previous = validate_source_pins(projection.facts.definition(), previous)?;
    let mut snapshots = BTreeMap::new();
    let mut objects = BTreeMap::new();
    for input in &published.inputs {
        let occurrence = SqlMvRelationOccurrenceId::new(input.relation_occurrence_id);
        let value = previous
            .get(&occurrence)
            .ok_or("MV rewrite history omits a publication occurrence")?;
        let expected = restore_exact_query_revision(&input.object_id, &input.native_data_version)
            .map_err(|error| format!("restore MV rewrite history revision: {error}"))?;
        if value.semantic_revision != expected {
            return Err(
                "MV rewrite history does not describe the exact published source revision".into(),
            );
        }
        snapshots.insert(occurrence, value.snapshot_id);
        objects.insert(occurrence, value.table_object_id.clone());
    }
    Ok((snapshots, objects))
}

fn validate_target_schema(
    schema: &SchemaRef,
    exact: &MvExactTargetSchemaFacts,
) -> Result<Vec<Bytes>, String> {
    if schema.fields().len() != exact.fields.len() {
        return Err("MV rewrite exact target fields do not cover its Arrow schema".into());
    }
    let mut ordered = exact.fields.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|field| field.ordinal);
    ordered
        .into_iter()
        .enumerate()
        .map(|(ordinal, field)| {
            let arrow = schema.field(ordinal);
            if usize::try_from(field.ordinal).ok() != Some(ordinal)
                || arrow.name() != &field.name
                || arrow.data_type() != &arrow_type_from_contract_signature(&field.type_signature)?
                || arrow.is_nullable() != field.nullable
            {
                return Err(
                    "MV rewrite target Arrow field differs from its exact provider fact".into(),
                );
            }
            Ok(Bytes::copy_from_slice(field.field_id.as_bytes()))
        })
        .collect()
}

fn sql_lineage(
    definition: &DefinitionDocument,
    field: &SourceFieldReference,
) -> Result<SqlImvQualifiedFieldFacts, String> {
    let occurrence = definition
        .relation_occurrences
        .iter()
        .find(|value| value.occurrence_id == field.occurrence_id)
        .ok_or("MV output references an unknown D occurrence")?;
    SqlImvQualifiedFieldFacts::try_new(
        SqlMvRelationOccurrenceId::new(field.occurrence_id),
        occurrence_table(occurrence).fqn(),
        occurrence.qualifier_at_binding.clone(),
        Bytes::copy_from_slice(field.field_id.as_bytes()),
    )
}

fn sql_schema_facts(
    projection: &StoredKeyedMvProjection,
    bindings: &MvRuntimeBindings,
    analysis: &MvRewriteAnalysisFacts,
) -> Result<SqlImvSchemaContractFacts, String> {
    let definition = projection.facts.definition();
    let interpretation = projection.facts.interpretation();
    let [apply_key] = bindings.apply_key.as_slice() else {
        return Err("SQL IMV rewrite requires one physical apply-key column".into());
    };
    let bases = definition
        .relation_occurrences
        .iter()
        .map(|occurrence| {
            SqlImvBaseContractFacts::try_new(
                SqlMvRelationOccurrenceId::new(occurrence.occurrence_id),
                occurrence_table(occurrence).fqn(),
                Some(occurrence.qualifier_at_binding.clone()),
                occurrence
                    .fields
                    .iter()
                    .map(|field| {
                        SqlImvBaseFieldFacts::try_new(
                            Bytes::copy_from_slice(field.field_id.as_bytes()),
                            field.name_at_binding.clone(),
                            arrow_type_from_contract_signature(&field.type_signature)?,
                            field.nullable,
                        )
                    })
                    .collect::<Result<_, String>>()?,
            )
        })
        .collect::<Result<_, String>>()?;
    let outputs = definition
        .outputs
        .iter()
        .map(|output| {
            Ok(SqlImvOutputColumnFacts::new(
                SqlImvExpressionFacts::try_new(
                    match output.expression.kind {
                        ExpressionKind::Field => SqlImvExpressionKindFacts::Column,
                        ExpressionKind::Literal => SqlImvExpressionKindFacts::Literal,
                        ExpressionKind::Cast => SqlImvExpressionKindFacts::Cast,
                        ExpressionKind::Function => SqlImvExpressionKindFacts::Func,
                        ExpressionKind::Mixed => SqlImvExpressionKindFacts::Mixed,
                    },
                    output
                        .expression
                        .source_fields
                        .iter()
                        .map(|field| sql_lineage(definition, field))
                        .collect::<Result<_, _>>()?,
                )?,
            ))
        })
        .collect::<Result<_, String>>()?;
    let aggregate = if bindings.aggregates.is_empty() {
        None
    } else {
        let Some(analyzed) = &analysis.aggregate else {
            return Err("MV rewrite lacks analyzed aggregate execution facts".into());
        };
        let states = aggregate_states_in_layout_order(bindings, analyzed)?
            .into_iter()
            .map(|(_, state)| {
                SqlImvAggregateStateColumnFacts::try_new(
                    state.physical.name.clone(),
                    state.physical.type_signature.clone(),
                    state_role(state.role),
                )
            })
            .collect::<Result<_, _>>()?;
        // NativeColumnV1 is L's declared encoding, not an old contract version.
        Some(SqlImvAggregateContractFacts::try_new(
            1,
            apply_key.name.clone(),
            states,
        )?)
    };
    let branch = match bindings.branches.first() {
        None => None,
        Some((_, first)) => {
            if bindings
                .branches
                .iter()
                .any(|(_, field)| field.field_id != first.field_id)
            {
                return Err("SQL IMV rewrite requires a shared branch discriminator field".into());
            }
            Some(SqlImvBranchContractFacts::try_new(first.name.clone())?)
        }
    };
    let target = SqlImvTargetContractFacts::try_new(
        definition
            .outputs
            .iter()
            .zip(&bindings.outputs)
            .map(|(output, (_, field))| {
                SqlImvTargetVisibleColumnFacts::try_new(
                    output.name.clone(),
                    Bytes::copy_from_slice(field.field_id.as_bytes()),
                )
            })
            .collect::<Result<_, _>>()?,
        apply_key.name.clone(),
        match interpretation.apply_key.kind {
            ApplyKeyKind::BaseRowId => SqlImvApplyKeySourceFacts::BaseRowId,
            ApplyKeyKind::JoinRowKey => SqlImvApplyKeySourceFacts::JoinRowKey,
            ApplyKeyKind::GroupRowId => SqlImvApplyKeySourceFacts::GroupRowId,
        },
        analysis.partition.clone(),
    )?;
    SqlImvSchemaContractFacts::try_new(
        bases,
        outputs,
        analysis.join.clone(),
        aggregate,
        branch,
        target,
    )
}

fn validate_aggregate_analysis(
    bindings: &MvRuntimeBindings,
    analysis: &MvRewriteAnalysisFacts,
) -> Result<(), String> {
    let Some(aggregate) = &analysis.aggregate else {
        return if bindings.aggregates.is_empty() {
            Ok(())
        } else {
            Err("MV rewrite lacks analyzed aggregate execution facts".into())
        };
    };
    let runtime = aggregate.layout.runtime_layout();
    if runtime.visible_columns().len() != bindings.outputs.len() {
        return Err("MV rewrite aggregate outputs differ from D/L".into());
    }
    for (column, (_, field)) in runtime.visible_columns().iter().zip(&bindings.outputs) {
        validate_physical_column(field, column.name(), column.data_type(), column.nullable())?;
    }
    let [apply_key] = bindings.apply_key.as_slice() else {
        return Err("MV rewrite aggregate requires one physical group row ID".into());
    };
    if aggregate.layout.row_id_column().column().name != apply_key.name {
        return Err("MV rewrite aggregate row ID differs from L".into());
    }
    for (column, state) in aggregate_states_in_layout_order(bindings, aggregate)? {
        validate_physical_column(
            &state.physical,
            column.name(),
            column.data_type(),
            column.nullable(),
        )?;
    }
    Ok(())
}

/// Pair each runtime state column with the L slot that holds it, in the
/// runtime layout's own column order.
///
/// A state slot describes storage -- which column, what type, which role --
/// so the column is what resolves it. That used to go through the aggregate
/// identity instead, on the premise that a slot belongs to one aggregate. It
/// does not: on a UNION view every branch has its own aggregate at an output
/// position and they all write the same column, so the column has several
/// aggregates and exactly one slot. Resolving by column says that directly,
/// and every persisted slot must still be consumed exactly once.
fn aggregate_states_in_layout_order<'a>(
    bindings: &'a MvRuntimeBindings,
    aggregate: &'a MvRewriteAggregateAnalysis,
) -> Result<
    Vec<(
        &'a novarocks_types::mv_aggregate_layout::MvAggregateStateColumn,
        &'a novarocks_mv_application::persistence::runtime_bindings::MvRuntimeStateBinding,
    )>,
    String,
> {
    let mut by_column: BTreeMap<
        &str,
        &novarocks_mv_application::persistence::runtime_bindings::MvRuntimeStateBinding,
    > = BTreeMap::new();
    for binding in &bindings.aggregates {
        for state in &binding.states {
            match by_column.insert(state.physical.name.as_str(), state) {
                None => {}
                // Branch aggregates share the column, so they share the slot
                // that describes it. Two different slots on one column would
                // be two descriptions of the same storage, and nothing could
                // say which one a read is looking at.
                Some(previous) if previous.slot_id == state.slot_id => {}
                Some(_) => {
                    return Err(
                        "MV interpretation describes one physical state column with two slots"
                            .into(),
                    );
                }
            }
        }
    }

    let mut used = BTreeSet::new();
    let mut ordered = Vec::new();
    for column in aggregate.layout.runtime_layout().state_columns() {
        let state = by_column.get(column.name()).ok_or_else(|| {
            format!(
                "MV interpretation has no aggregate state bound to physical column `{}`",
                column.name()
            )
        })?;
        if state_role(state.role) != runtime_state_role(column.state_role()) {
            return Err(format!(
                "MV interpretation binds physical column `{}` to a different state role",
                column.name()
            ));
        }
        used.insert(state.slot_id.clone());
        ordered.push((column, *state));
    }
    let persisted = bindings
        .aggregates
        .iter()
        .flat_map(|value| value.states.iter().map(|state| &state.slot_id))
        .collect::<BTreeSet<_>>();
    if used.len() != persisted.len() {
        return Err("MV rewrite aggregate analysis omits persisted state slots".into());
    }
    Ok(ordered)
}

fn validate_physical_column(
    field: &MvPhysicalFieldFacts,
    name: &str,
    data_type: &DataType,
    nullable: bool,
) -> Result<(), String> {
    let bound = arrow_type_from_contract_signature(&field.type_signature)?;
    if field.name != name
        || !denote_one_provider_type(&bound, data_type)
        || field.nullable != nullable
    {
        return Err(format!(
            "MV rewrite analyzed physical column differs from its L/provider binding: L has \
             name={} type={} nullable={}; the analysis wants name={} type={:?} nullable={}",
            field.name, field.type_signature, field.nullable, name, data_type, nullable
        ));
    }
    Ok(())
}

/// Whether two Arrow types are the same provider type.
///
/// A contract type signature names the provider's type, and several Arrow
/// layouts can carry one of them: Iceberg has a single `binary`, which the
/// connector reads into either binary array width, and the aggregate state
/// column is written as the large one. Comparing the Arrow variants exactly
/// demands a coincidence of layouts the signature never promised, and the
/// aggregate layout's own validators already accept either.
fn denote_one_provider_type(left: &DataType, right: &DataType) -> bool {
    match (left, right) {
        (DataType::Binary | DataType::LargeBinary, DataType::Binary | DataType::LargeBinary) => {
            true
        }
        (DataType::Utf8 | DataType::LargeUtf8, DataType::Utf8 | DataType::LargeUtf8) => true,
        (left, right) => left == right,
    }
}

fn state_role(role: StateRole) -> SqlImvAggregateStateRoleFacts {
    match role {
        StateRole::Single => SqlImvAggregateStateRoleFacts::Single,
        StateRole::AvgSum => SqlImvAggregateStateRoleFacts::AvgSum,
        StateRole::AvgCount => SqlImvAggregateStateRoleFacts::AvgCount,
        StateRole::RetractionCount => SqlImvAggregateStateRoleFacts::RetractionCount,
    }
}
fn runtime_state_role(
    role: novarocks_types::mv_aggregate_layout::MvAggregateStateRole,
) -> SqlImvAggregateStateRoleFacts {
    use novarocks_types::mv_aggregate_layout::MvAggregateStateRole;
    match role {
        MvAggregateStateRole::Single => SqlImvAggregateStateRoleFacts::Single,
        MvAggregateStateRole::AvgSum => SqlImvAggregateStateRoleFacts::AvgSum,
        MvAggregateStateRole::AvgCount => SqlImvAggregateStateRoleFacts::AvgCount,
        MvAggregateStateRole::RetractionCount => SqlImvAggregateStateRoleFacts::RetractionCount,
    }
}
fn aggregate_execution_facts(
    analysis: &MvRewriteAggregateAnalysis,
) -> Result<SqlImvAggregateExecutionFacts, String> {
    let layout = analysis.layout.runtime_layout();
    SqlImvAggregateExecutionFacts::try_new(
        analysis.calls.group_keys.len(),
        analysis.calls.visible_outputs.clone(),
        analysis.layout.row_id_column().column().name.clone(),
        layout
            .visible_columns()
            .iter()
            .map(|column| {
                SqlImvAggregateVisibleColumnFacts::try_new(
                    column.name().to_string(),
                    column.value_type().clone(),
                )
            })
            .collect::<Result<_, _>>()?,
        layout
            .state_columns()
            .iter()
            .map(|column| {
                SqlImvAggregateExecutionStateColumnFacts::try_new(
                    column.name().to_string(),
                    column.value_type().clone(),
                    column.visible_source_index(),
                    column.aggregate_index(),
                    aggregate_function_kind(column.aggregate_kind()),
                    runtime_state_role(column.state_role()),
                    column.count_star(),
                )
            })
            .collect::<Result<_, _>>()?,
        layout.group_key_source_indexes().to_vec(),
        analysis
            .layout
            .physical_columns()
            .iter()
            .map(|column| column.column().name.clone())
            .collect(),
        layout.aggregate_input_types().to_vec(),
    )
}

fn aggregate_function_kind(
    kind: novarocks_types::mv_aggregate_layout::MvAggregateRuntimeKind,
) -> novarocks_sql::planning::mv::AggregateFunctionKind {
    use novarocks_sql::planning::mv::AggregateFunctionKind;
    use novarocks_types::mv_aggregate_layout::MvAggregateRuntimeKind;

    match kind {
        MvAggregateRuntimeKind::Count => AggregateFunctionKind::Count,
        MvAggregateRuntimeKind::Sum => AggregateFunctionKind::Sum,
        MvAggregateRuntimeKind::Avg => AggregateFunctionKind::Avg,
        MvAggregateRuntimeKind::Min => AggregateFunctionKind::Min,
        MvAggregateRuntimeKind::Max => AggregateFunctionKind::Max,
        MvAggregateRuntimeKind::BoolOr => AggregateFunctionKind::BoolOr,
        MvAggregateRuntimeKind::BoolAnd => AggregateFunctionKind::BoolAnd,
        MvAggregateRuntimeKind::CountDistinct => AggregateFunctionKind::CountDistinct,
        MvAggregateRuntimeKind::ApproxCountDistinct => AggregateFunctionKind::ApproxCountDistinct,
    }
}

/// The first UNION ALL branch as a standalone `Query` (keeps the branch's own
/// FROM — a scan, a join, or a fan-in union). Works off the AST so a composed
/// branch is not classified.
pub fn first_union_branch_query(
    query: &novarocks_parser::ast::Query,
) -> Result<novarocks_parser::ast::Query, String> {
    fn first_branch_body(
        body: &novarocks_parser::ast::SetExpr,
    ) -> Result<&novarocks_parser::ast::SetExpr, String> {
        match body {
            novarocks_parser::ast::SetExpr::SetOperation(operation) => {
                first_branch_body(&operation.left)
            }
            novarocks_parser::ast::SetExpr::Query(inner) => first_branch_body(inner.body.as_ref()),
            other => Ok(other),
        }
    }
    let mut branch = query.clone();
    branch.body = Box::new(first_branch_body(query.body.as_ref())?.clone());
    Ok(branch)
}

pub(crate) fn arrow_type_from_contract_signature(type_signature: &str) -> Result<DataType, String> {
    let trimmed = type_signature.trim();
    let lower = trimmed.to_ascii_lowercase();
    Ok(match lower.as_str() {
        "boolean" | "bool" => DataType::Boolean,
        "tinyint" => DataType::Int8,
        "smallint" => DataType::Int16,
        "int" | "integer" => DataType::Int32,
        "long" | "bigint" => DataType::Int64,
        "float" => DataType::Float32,
        "double" => DataType::Float64,
        "date" => DataType::Date32,
        "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "string" | "varchar" | "char" => DataType::Utf8,
        "binary" | "varbinary" => DataType::Binary,
        _ if lower.starts_with("decimal(") => {
            let inner = trimmed
                .strip_prefix("decimal(")
                .or_else(|| trimmed.strip_prefix("DECIMAL("))
                .and_then(|value| value.strip_suffix(')'))
                .ok_or_else(|| format!("invalid decimal type signature `{type_signature}`"))?;
            let mut parts = inner.split(',').map(str::trim);
            let precision = parts
                .next()
                .and_then(|value| value.parse::<u8>().ok())
                .ok_or_else(|| format!("invalid decimal precision in `{type_signature}`"))?;
            let scale = parts
                .next()
                .and_then(|value| value.parse::<i8>().ok())
                .ok_or_else(|| format!("invalid decimal scale in `{type_signature}`"))?;
            DataType::Decimal128(precision, scale)
        }
        _ => {
            return Err(format!(
                "aggregate MV contract input type is unsupported: {type_signature}"
            ));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::domain::test_admission::KeyedProjectionFixture as ProjectionFixture;

    fn projection() -> StoredKeyedMvProjection {
        let object = ConnectorTableObjectId::try_new(Bytes::from_static(&[11])).unwrap();
        let revision = ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
            novarocks_spi::connector::ConnectorProviderId::parse("iceberg").unwrap(),
            &object,
            Some(12),
        )
        .unwrap();
        let (persisted_object, _) = persist_exact_connector_revision(&revision).unwrap();
        let mut fixture = ProjectionFixture::new(
            novarocks_mv_application::product::MvTarget::from_parts(Some("ice"), "sales", "mv"),
            None,
        );
        for occurrence in &mut fixture.definition.relation_occurrences {
            occurrence.object_id = persisted_object.clone();
        }
        StoredKeyedMvProjection {
            mv_id: 17,
            facts: fixture.build().unwrap(),
        }
    }

    fn pin(id: u32) -> MvRewriteSourceSnapshot {
        let object = ConnectorTableObjectId::try_new(Bytes::from_static(&[11])).unwrap();
        MvRewriteSourceSnapshot {
            occurrence_id: SqlMvRelationOccurrenceId::new(id),
            snapshot_id: 12,
            table_object_id: object.clone(),
            semantic_revision: ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
                novarocks_spi::connector::ConnectorProviderId::parse("iceberg").unwrap(),
                &object,
                Some(12),
            )
            .unwrap(),
        }
    }

    #[test]
    fn sparse_repeated_relation_occurrences_remain_distinct() {
        let projection = projection();
        let pins =
            validate_source_pins(projection.facts.definition(), vec![pin(8), pin(7)]).unwrap();
        assert_eq!(pins.len(), 2);
        assert!(pins.contains_key(&SqlMvRelationOccurrenceId::new(7)));
        assert!(pins.contains_key(&SqlMvRelationOccurrenceId::new(8)));
    }

    #[test]
    fn duplicate_and_dense_renumbered_pins_are_rejected() {
        let projection = projection();
        assert!(validate_source_pins(projection.facts.definition(), vec![pin(7), pin(7)]).is_err());
        assert!(validate_source_pins(projection.facts.definition(), vec![pin(0), pin(1)]).is_err());
    }

    #[test]
    fn source_object_replacement_is_not_hidden_by_equal_snapshot() {
        let projection = projection();
        let mut replacement = pin(8);
        replacement.table_object_id =
            ConnectorTableObjectId::try_new(Bytes::from_static(b"replacement")).unwrap();
        assert!(
            validate_source_pins(projection.facts.definition(), vec![pin(7), replacement]).is_err()
        );
    }

    #[test]
    fn never_published_does_not_accept_invented_history() {
        let projection = projection();
        assert!(validate_history(&projection, vec![pin(7), pin(8)]).is_err());
        assert!(
            validate_history(&projection, Vec::new())
                .unwrap()
                .0
                .is_empty()
        );
    }

    #[test]
    fn timezone_signature_is_not_silently_downgraded() {
        assert!(arrow_type_from_contract_signature("timestamptz").is_err());
    }
}
