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

//! Pure completion boundary for one immutable physical plan.
//!
//! The compiler publishes closed, typed value requests. The application
//! answers one exact batch with frozen values and returns the opaque
//! [`SqlCompilation`] to [`SqlCompiler::finish`]. Runtime handles, provider
//! instances, leases, encoders, schedulers and I/O callbacks cannot cross this
//! module's public boundary.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt;

use novarocks_physical_plan::{
    NullOrdering, PlanVersionId, PredicateGuaranteeKind, ProviderColumnReference,
    ProviderReadOccurrenceId, ProviderReadReference, SortDirection, ValueType,
};
use novarocks_spi::connector::read_stack::{
    ConnectorExpression, ConnectorFunctionName, ConnectorReadBinding, ConnectorReadRelationKind,
    ConnectorValueType, Constraint, TupleDomain,
};
use novarocks_spi::connector::{ConnectorCodecCategory, ConnectorEncodedPayload, StatisticsMetric};
use novarocks_type_contract::{BucketLayoutAlgorithm, PartitionHashAlgorithm};
use novarocks_types::naming::TableIdentity;

#[cfg(test)]
use novarocks_physical_plan::PlanBuilder;

use super::{SqlAuthoredPhysicalPlan, SqlCompileError, SqlCompiler};
use crate::binding::SqlTableBindingId;
use crate::catalog::{ResolvedAnalyzerTable, TableLookupMode};
use crate::compiler::mv_rewrite::SqlMvRewriteDefinitionFacts;
use crate::explain::ExplainLevel;
use crate::planner::distributed::build::LoweredSqlPhysicalDraft;
use crate::planner::table::{SqlScanKind, SqlTableVersionSelector};
use crate::planning::catalog::MetadataTableKind;
use crate::planning::dml::DmlStatisticsEvidence;

/// UEA-4 completion limits shared by every physical-plan producer.
pub const DEFAULT_COMPLETION_LIMITS: CompletionLimits = CompletionLimits {
    max_rounds: 4,
    max_distinct_needs: 4096,
    max_exchange_bytes: 32 * 1024 * 1024,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompletionLimits {
    pub max_rounds: u32,
    pub max_distinct_needs: u32,
    /// Maximum bytes simultaneously held by the current Need and incoming Fact batches.
    pub max_exchange_bytes: u64,
}

impl CompletionLimits {
    pub fn try_new(
        max_rounds: u32,
        max_distinct_needs: u32,
        max_exchange_bytes: u64,
    ) -> Result<Self, CompletionProtocolError> {
        if max_rounds == 0 {
            return Err(CompletionProtocolError::InvalidLimit("rounds"));
        }
        if max_distinct_needs == 0 {
            return Err(CompletionProtocolError::InvalidLimit("distinct needs"));
        }
        if max_exchange_bytes == 0 {
            return Err(CompletionProtocolError::InvalidLimit("exchange bytes"));
        }
        Ok(Self {
            max_rounds,
            max_distinct_needs,
            max_exchange_bytes,
        })
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CompletionUsage {
    pub rounds: u32,
    pub distinct_needs: u32,
    /// Bytes held by the currently published Need batch.
    pub exchange_bytes: u64,
}

/// Compiler-minted identity of one need occurrence.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CompileNeedId(u32);

impl CompileNeedId {
    pub const fn get(self) -> u32 {
        self.0
    }

    pub(super) const fn new(value: u32) -> Self {
        Self(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NeedKind {
    CatalogRelation,
    Statistics,
    MaterializedView,
    ProviderRead,
}

/// Exact analyzer lookup surface. Metadata-table lookup is a distinct owner
/// call and never hides in a table-name suffix convention.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogLookupTarget {
    Table { mode: TableLookupMode },
    IcebergMetadata { kind: MetadataTableKind },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogRelationNeed {
    id: CompileNeedId,
    relation: TableIdentity,
    target: CatalogLookupTarget,
}

impl CatalogRelationNeed {
    pub(super) fn try_new(
        id: CompileNeedId,
        relation: TableIdentity,
        target: CatalogLookupTarget,
    ) -> Result<Self, CompletionProtocolError> {
        validate_relation_identity(&relation)?;
        Ok(Self {
            id,
            relation,
            target,
        })
    }

    pub const fn id(&self) -> CompileNeedId {
        self.id
    }

    pub const fn relation(&self) -> &TableIdentity {
        &self.relation
    }

    pub const fn target(&self) -> CatalogLookupTarget {
        self.target
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsNeed {
    id: CompileNeedId,
    binding: SqlTableBindingId,
    metrics: Box<[StatisticsMetric]>,
}

impl StatisticsNeed {
    pub(super) fn try_new(
        id: CompileNeedId,
        binding: SqlTableBindingId,
        metrics: impl Into<Box<[StatisticsMetric]>>,
    ) -> Result<Self, CompletionProtocolError> {
        let metrics = metrics.into();
        if metrics.is_empty() {
            return Err(CompletionProtocolError::InvalidNeed {
                id,
                reason: "statistics need has no metric",
            });
        }
        if metrics.iter().collect::<BTreeSet<_>>().len() != metrics.len() {
            return Err(CompletionProtocolError::InvalidNeed {
                id,
                reason: "statistics need repeats a metric",
            });
        }
        Ok(Self {
            id,
            binding,
            metrics,
        })
    }

    pub const fn id(&self) -> CompileNeedId {
        self.id
    }

    pub const fn binding(&self) -> SqlTableBindingId {
        self.binding
    }

    pub fn metrics(&self) -> &[StatisticsMetric] {
        &self.metrics
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializedViewNeed {
    id: CompileNeedId,
    referenced_relations: Box<[TableIdentity]>,
}

impl MaterializedViewNeed {
    pub(super) fn try_new(
        id: CompileNeedId,
        referenced_relations: impl Into<Box<[TableIdentity]>>,
    ) -> Result<Self, CompletionProtocolError> {
        let referenced_relations = referenced_relations.into();
        for relation in referenced_relations.iter() {
            validate_relation_identity(relation)?;
        }
        if referenced_relations.is_empty() {
            return Err(CompletionProtocolError::InvalidNeed {
                id,
                reason: "materialized-view need has no referenced relation",
            });
        }
        if referenced_relations.iter().collect::<HashSet<_>>().len() != referenced_relations.len() {
            return Err(CompletionProtocolError::InvalidNeed {
                id,
                reason: "materialized-view need repeats a relation",
            });
        }
        Ok(Self {
            id,
            referenced_relations,
        })
    }

    pub const fn id(&self) -> CompileNeedId {
        self.id
    }

    pub fn referenced_relations(&self) -> &[TableIdentity] {
        &self.referenced_relations
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProviderPredicateOccurrenceId(u32);

impl ProviderPredicateOccurrenceId {
    pub const fn get(self) -> u32 {
        self.0
    }

    pub(super) const fn new(value: u32) -> Self {
        Self(value)
    }
}

#[derive(Clone, PartialEq)]
pub struct ProviderReadColumnNeed {
    ordinal: u32,
    name: Box<str>,
    engine_type: ValueType,
    connector_type: ConnectorValueType,
}

impl ProviderReadColumnNeed {
    pub(super) fn try_new(
        ordinal: u32,
        name: impl Into<Box<str>>,
        engine_type: ValueType,
        connector_type: ConnectorValueType,
    ) -> Result<Self, CompletionProtocolError> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(CompletionProtocolError::InvalidProviderReadColumn { ordinal });
        }
        if !novarocks_connector_contract::connector_type_accepts_value_type(
            connector_type,
            &engine_type,
        ) {
            return Err(CompletionProtocolError::ProviderReadColumnTypeMismatch { ordinal });
        }
        Ok(Self {
            ordinal,
            name,
            engine_type,
            connector_type,
        })
    }

    pub const fn ordinal(&self) -> u32 {
        self.ordinal
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn engine_type(&self) -> &ValueType {
        &self.engine_type
    }

    pub const fn connector_type(&self) -> ConnectorValueType {
        self.connector_type
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        ordinal: u32,
        name: impl Into<Box<str>>,
        engine_type: ValueType,
        connector_type: ConnectorValueType,
    ) -> Self {
        Self::try_new(ordinal, name, engine_type, connector_type)
            .expect("test provider column need must be valid")
    }
}

pub(super) use novarocks_connector_contract::connector_type_for_arrow as provider_connector_type_for_engine;

impl fmt::Debug for ProviderReadColumnNeed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderReadColumnNeed")
            .field("ordinal", &self.ordinal)
            .field("name_bytes", &self.name.len())
            .field("nullable", &self.engine_type.nullable())
            .field("connector_type", &self.connector_type)
            .finish()
    }
}

/// Exact value-only operation by which the application must freeze a relation.
///
/// Every variant maps to one connector metadata call. In particular, a change
/// window and a pinned file set cannot collapse into a generic table read.
#[derive(Clone, PartialEq)]
pub enum ProviderReadRelationNeed {
    Data {
        relation: TableIdentity,
        version: ProviderReadVersionNeed,
    },
    FrozenInputSet {
        relation: TableIdentity,
        version: ProviderReadVersionNeed,
    },
    Metadata {
        relation: TableIdentity,
        kind: MetadataTableKind,
        version: ProviderReadVersionNeed,
    },
    Delta {
        relation: TableIdentity,
        from_snapshot_id: i64,
        to_snapshot_id: i64,
    },
    MvTarget {
        relation: TableIdentity,
        target_table_uuid: String,
        target_snapshot_id: Option<i64>,
        use_affected_partitions: bool,
    },
    PinnedFileSet {
        relation: TableIdentity,
    },
    TableExecute {
        relation: TableIdentity,
    },
}

/// Which admission-frozen input one provider read names.
///
/// Timestamp-based selectors are resolved by admission before a read is
/// requested, so the compiler receives only a current or exact snapshot input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderReadVersionNeed {
    Current,
    Snapshot(i64),
}

fn provider_version_need(value: SqlTableVersionSelector) -> ProviderReadVersionNeed {
    match value {
        SqlTableVersionSelector::Current => ProviderReadVersionNeed::Current,
        SqlTableVersionSelector::Snapshot(snapshot_id) => {
            ProviderReadVersionNeed::Snapshot(snapshot_id)
        }
    }
}

impl ProviderReadRelationNeed {
    pub const fn relation(&self) -> &TableIdentity {
        match self {
            Self::Data { relation, .. }
            | Self::FrozenInputSet { relation, .. }
            | Self::Metadata { relation, .. }
            | Self::Delta { relation, .. }
            | Self::MvTarget { relation, .. }
            | Self::PinnedFileSet { relation }
            | Self::TableExecute { relation, .. } => relation,
        }
    }

    pub const fn relation_kind(&self) -> ConnectorReadRelationKind {
        match self {
            Self::Data { .. }
            | Self::FrozenInputSet { .. }
            | Self::PinnedFileSet { .. }
            | Self::MvTarget { .. } => ConnectorReadRelationKind::Table,
            Self::Metadata { .. } => ConnectorReadRelationKind::SystemTable,
            Self::Delta { .. } => ConnectorReadRelationKind::ChangeWindow,
            Self::TableExecute { .. } => ConnectorReadRelationKind::TableExecute,
        }
    }
}

impl fmt::Debug for ProviderReadRelationNeed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let relation = self.relation();
        let mut debug = formatter.debug_struct("ProviderReadRelationNeed");
        debug
            .field("kind", &self.relation_kind())
            .field("catalog_bytes", &relation.catalog.len())
            .field("namespace_bytes", &relation.namespace.len())
            .field("table_bytes", &relation.table.len());
        match self {
            Self::Data { version, .. } | Self::FrozenInputSet { version, .. } => {
                debug.field("version", version);
            }
            Self::Metadata { kind, version, .. } => {
                debug.field("metadata_kind", kind).field("version", version);
            }
            Self::Delta {
                from_snapshot_id,
                to_snapshot_id,
                ..
            } => {
                debug
                    .field("from_snapshot_id", from_snapshot_id)
                    .field("to_snapshot_id", to_snapshot_id);
            }
            Self::MvTarget {
                target_snapshot_id,
                use_affected_partitions,
                ..
            } => {
                debug
                    .field("target_snapshot_id", target_snapshot_id)
                    .field("use_affected_partitions", use_affected_partitions);
            }
            Self::PinnedFileSet { .. } | Self::TableExecute { .. } => {}
        }
        debug.finish()
    }
}

#[derive(Clone, PartialEq)]
pub struct ProviderReadPredicateNeed {
    occurrence: ProviderPredicateOccurrenceId,
    constraint: Constraint<u32>,
}

impl ProviderReadPredicateNeed {
    pub(super) const fn new(
        occurrence: ProviderPredicateOccurrenceId,
        constraint: Constraint<u32>,
    ) -> Self {
        Self {
            occurrence,
            constraint,
        }
    }

    pub const fn occurrence(&self) -> ProviderPredicateOccurrenceId {
        self.occurrence
    }

    pub const fn constraint(&self) -> &Constraint<u32> {
        &self.constraint
    }
}

impl fmt::Debug for ProviderReadPredicateNeed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderReadPredicateNeed")
            .field("occurrence", &self.occurrence)
            .field(
                "summary_columns",
                &self.constraint.summary().columns().count(),
            )
            .field(
                "expression_nodes",
                &provider_expression_node_count(self.constraint.expression()),
            )
            .finish()
    }
}

/// Static provider negotiation input. No runtime provider capability enters it.
#[derive(Clone, PartialEq)]
pub struct ProviderReadNeed {
    id: CompileNeedId,
    occurrence: ProviderReadOccurrenceId,
    binding: SqlTableBindingId,
    relation: ProviderReadRelationNeed,
    columns: Box<[ProviderReadColumnNeed]>,
    filter: Constraint<u32>,
    predicates: Box<[ProviderReadPredicateNeed]>,
    limit: Option<u64>,
}

impl ProviderReadNeed {
    pub(super) fn try_new(
        id: CompileNeedId,
        occurrence: ProviderReadOccurrenceId,
        binding: SqlTableBindingId,
        relation: ProviderReadRelationNeed,
        columns: impl Into<Box<[ProviderReadColumnNeed]>>,
        predicates: impl Into<Box<[ProviderReadPredicateNeed]>>,
        limit: Option<u64>,
    ) -> Result<Self, CompletionProtocolError> {
        let columns = columns.into();
        let predicates = predicates.into();
        validate_provider_relation(id, &relation)?;
        if columns
            .iter()
            .enumerate()
            .any(|(ordinal, column)| column.ordinal as usize != ordinal)
        {
            return Err(CompletionProtocolError::InvalidNeed {
                id,
                reason: "provider read columns are not a dense ordered projection",
            });
        }
        if predicates
            .iter()
            .map(ProviderReadPredicateNeed::occurrence)
            .collect::<BTreeSet<_>>()
            .len()
            != predicates.len()
        {
            return Err(CompletionProtocolError::InvalidNeed {
                id,
                reason: "provider read need repeats a predicate occurrence",
            });
        }
        let filter = combine_provider_predicates(id, &predicates)?;
        validate_provider_constraint(id, &filter, columns.len())?;
        for predicate in predicates.iter() {
            validate_provider_constraint(id, &predicate.constraint, columns.len())?;
        }
        Ok(Self {
            id,
            occurrence,
            binding,
            relation,
            columns,
            filter,
            predicates,
            limit,
        })
    }

    pub const fn id(&self) -> CompileNeedId {
        self.id
    }

    pub const fn occurrence(&self) -> ProviderReadOccurrenceId {
        self.occurrence
    }

    pub const fn binding(&self) -> SqlTableBindingId {
        self.binding
    }

    pub const fn relation(&self) -> &ProviderReadRelationNeed {
        &self.relation
    }

    pub fn columns(&self) -> &[ProviderReadColumnNeed] {
        &self.columns
    }

    /// Exact conjunction offered once to `apply_filter`.
    pub const fn filter(&self) -> &Constraint<u32> {
        &self.filter
    }

    pub fn predicates(&self) -> &[ProviderReadPredicateNeed] {
        &self.predicates
    }

    pub const fn limit(&self) -> Option<u64> {
        self.limit
    }

    /// State the read one SQL-owned program performs.
    ///
    /// A program is a plan no statement described: nothing analyzed it and no
    /// optimizer chose its shape. What it reads is still an ordinary provider
    /// read, negotiated and frozen through exactly the protocol a statement's
    /// scan uses, so it states an ordinary need. The projection it names is
    /// the plan's own scan projection in order, which is why the ordinals are
    /// positions rather than the provider's own numbering -- the provider is
    /// asked for each column by name.
    ///
    /// A program offers no predicate and no limit. Both are answers to what a
    /// statement asked for, and there is no statement here.
    pub fn for_program(
        occurrence: ProviderReadOccurrenceId,
        binding: SqlTableBindingId,
        relation: ProviderReadRelationNeed,
        columns: impl IntoIterator<Item = (Box<str>, ValueType)>,
    ) -> Result<Self, CompletionProtocolError> {
        let id = CompileNeedId::new(0);
        let columns = columns
            .into_iter()
            .enumerate()
            .map(|(ordinal, (name, engine_type))| {
                let ordinal =
                    u32::try_from(ordinal).map_err(|_| CompletionProtocolError::InvalidNeed {
                        id,
                        reason: "program provider read projects more columns than it can address",
                    })?;
                let connector_type =
                    novarocks_connector_contract::connector_type_for_value_type(&engine_type)
                        .ok_or(CompletionProtocolError::InvalidProviderReadColumn { ordinal })?;
                ProviderReadColumnNeed::try_new(ordinal, name, engine_type, connector_type)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Self::try_new(
            id,
            occurrence,
            binding,
            relation,
            columns,
            Box::default(),
            None,
        )
    }

    #[cfg(test)]
    pub(crate) fn exact_projection_for_test(
        binding: SqlTableBindingId,
        relation: ProviderReadRelationNeed,
        columns: impl Into<Box<[ProviderReadColumnNeed]>>,
    ) -> Self {
        Self::try_new(
            CompileNeedId::new(0),
            ProviderReadOccurrenceId::new(0),
            binding,
            relation,
            columns,
            Box::default(),
            None,
        )
        .expect("test provider projection must be valid")
    }
}

fn combine_provider_predicates(
    id: CompileNeedId,
    predicates: &[ProviderReadPredicateNeed],
) -> Result<Constraint<u32>, CompletionProtocolError> {
    let mut summary = TupleDomain::all();
    let mut expressions = Vec::with_capacity(predicates.len());
    let mut assignments = BTreeMap::new();
    for predicate in predicates {
        summary = summary
            .intersect(predicate.constraint.summary())
            .map_err(|_| CompletionProtocolError::InvalidNeed {
                id,
                reason: "provider predicate summaries cannot be combined",
            })?;
        if !predicate.constraint.expression().is_constant_true() {
            expressions.push(predicate.constraint.expression().clone());
        }
        for (name, ordinal) in predicate.constraint.assignments() {
            if assignments
                .insert(name.clone(), *ordinal)
                .is_some_and(|existing| existing != *ordinal)
            {
                return Err(CompletionProtocolError::InvalidNeed {
                    id,
                    reason: "provider predicate variable maps to different projection columns",
                });
            }
        }
    }
    let expression = match expressions.len() {
        0 => ConnectorExpression::constant_true(),
        1 => expressions.pop().expect("one expression is present"),
        _ => ConnectorExpression::Call {
            function: ConnectorFunctionName::try_new("and").map_err(|_| {
                CompletionProtocolError::InvalidNeed {
                    id,
                    reason: "provider conjunction function is invalid",
                }
            })?,
            value_type: ConnectorValueType::Boolean,
            arguments: expressions,
        },
    };
    Constraint::try_new(summary, expression, assignments).map_err(|_| {
        CompletionProtocolError::InvalidNeed {
            id,
            reason: "provider combined filter is invalid",
        }
    })
}

impl fmt::Debug for ProviderReadNeed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderReadNeed")
            .field("id", &self.id)
            .field("occurrence", &self.occurrence)
            .field("binding", &self.binding)
            .field("relation", &self.relation)
            .field("projection_columns", &self.columns.len())
            .field(
                "filter_expression_nodes",
                &provider_expression_node_count(self.filter.expression()),
            )
            .field("predicate_occurrences", &self.predicates.len())
            .field("limit", &self.limit)
            .finish()
    }
}

fn validate_provider_relation(
    id: CompileNeedId,
    relation: &ProviderReadRelationNeed,
) -> Result<(), CompletionProtocolError> {
    validate_relation_identity(relation.relation())?;
    let invalid_snapshot = match relation {
        ProviderReadRelationNeed::Data { version, .. }
        | ProviderReadRelationNeed::FrozenInputSet { version, .. }
        | ProviderReadRelationNeed::Metadata { version, .. } => {
            matches!(version, ProviderReadVersionNeed::Snapshot(snapshot_id) if *snapshot_id < 0)
        }
        ProviderReadRelationNeed::Delta {
            from_snapshot_id,
            to_snapshot_id,
            ..
        } => *from_snapshot_id < 0 || *to_snapshot_id < 0,
        ProviderReadRelationNeed::MvTarget {
            target_snapshot_id, ..
        } => target_snapshot_id.is_some_and(|snapshot| snapshot < 0),
        ProviderReadRelationNeed::PinnedFileSet { .. }
        | ProviderReadRelationNeed::TableExecute { .. } => false,
    };
    if invalid_snapshot {
        return Err(CompletionProtocolError::InvalidNeed {
            id,
            reason: "provider read contains a negative snapshot identifier",
        });
    }
    Ok(())
}

pub(super) fn provider_relation_need_from_sql_scan(
    id: CompileNeedId,
    relation: TableIdentity,
    scan_kind: &SqlScanKind,
) -> Result<ProviderReadRelationNeed, CompletionProtocolError> {
    validate_relation_identity(&relation)?;
    let need = match scan_kind {
        SqlScanKind::Data { version } => ProviderReadRelationNeed::Data {
            relation,
            version: provider_version_need(*version),
        },
        SqlScanKind::FrozenInputSet { version } => ProviderReadRelationNeed::FrozenInputSet {
            relation,
            version: provider_version_need(*version),
        },
        SqlScanKind::Metadata { kind, version } => ProviderReadRelationNeed::Metadata {
            relation,
            kind: *kind,
            version: provider_version_need(*version),
        },
        SqlScanKind::Delta {
            from_snapshot_id,
            to_snapshot_id,
        } => ProviderReadRelationNeed::Delta {
            relation,
            from_snapshot_id: *from_snapshot_id,
            to_snapshot_id: *to_snapshot_id,
        },
        SqlScanKind::PinnedFileSet => ProviderReadRelationNeed::PinnedFileSet { relation },
        SqlScanKind::TableExecute => ProviderReadRelationNeed::TableExecute { relation },
        SqlScanKind::MvTargetState { facts } => ProviderReadRelationNeed::MvTarget {
            relation,
            target_table_uuid: facts.target_table_uuid.clone(),
            target_snapshot_id: facts.target_snapshot_id,
            use_affected_partitions: matches!(
                facts.partition_constraint,
                crate::planner::table::SqlMvTargetStatePartitionConstraint::AffectedPartitionAllowListRequired
            ),
        },
        SqlScanKind::MvTargetLocator { facts } => ProviderReadRelationNeed::MvTarget {
            relation,
            target_table_uuid: facts.target_table_uuid.clone(),
            target_snapshot_id: facts.target_snapshot_id,
            use_affected_partitions: false,
        },
        SqlScanKind::ConnectorRead
        => {
            return Err(CompletionProtocolError::InvalidNeed {
                id,
                reason: "SQL scan kind has no exact provider completion operation",
            });
        }
    };
    validate_provider_relation(id, &need)?;
    Ok(need)
}

fn validate_provider_constraint(
    id: CompileNeedId,
    constraint: &Constraint<u32>,
    projection_len: usize,
) -> Result<(), CompletionProtocolError> {
    let invalid_ordinal = constraint
        .summary()
        .columns()
        .chain(constraint.assignments().values())
        .any(|ordinal| usize::try_from(*ordinal).map_or(true, |value| value >= projection_len));
    if invalid_ordinal {
        return Err(CompletionProtocolError::InvalidNeed {
            id,
            reason: "provider filter references a column outside the projection",
        });
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq)]
pub enum SqlNeedBatch {
    CatalogRelations(Box<[CatalogRelationNeed]>),
    Statistics(Box<[StatisticsNeed]>),
    MaterializedViews(Box<[MaterializedViewNeed]>),
    ProviderReads(Box<[ProviderReadNeed]>),
}

impl SqlNeedBatch {
    pub const fn kind(&self) -> NeedKind {
        match self {
            Self::CatalogRelations(_) => NeedKind::CatalogRelation,
            Self::Statistics(_) => NeedKind::Statistics,
            Self::MaterializedViews(_) => NeedKind::MaterializedView,
            Self::ProviderReads(_) => NeedKind::ProviderRead,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::CatalogRelations(needs) => needs.len(),
            Self::Statistics(needs) => needs.len(),
            Self::MaterializedViews(needs) => needs.len(),
            Self::ProviderReads(needs) => needs.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn ids(&self) -> Box<dyn Iterator<Item = CompileNeedId> + '_> {
        match self {
            Self::CatalogRelations(needs) => Box::new(needs.iter().map(CatalogRelationNeed::id)),
            Self::Statistics(needs) => Box::new(needs.iter().map(StatisticsNeed::id)),
            Self::MaterializedViews(needs) => Box::new(needs.iter().map(MaterializedViewNeed::id)),
            Self::ProviderReads(needs) => Box::new(needs.iter().map(ProviderReadNeed::id)),
        }
    }

    pub(super) fn accounted_bytes(&self) -> Result<u64, CompletionProtocolError> {
        match self {
            Self::CatalogRelations(needs) => checked_sum([
                checked_mul(needs.len(), std::mem::size_of::<CatalogRelationNeed>()),
                checked_sum(
                    needs
                        .iter()
                        .map(|need| table_identity_bytes(&need.relation)),
                ),
            ]),
            Self::Statistics(needs) => checked_sum([
                checked_mul(needs.len(), std::mem::size_of::<StatisticsNeed>()),
                checked_sum(needs.iter().map(|need| {
                    checked_sum([
                        checked_mul(need.metrics.len(), std::mem::size_of::<StatisticsMetric>()),
                        checked_sum(need.metrics.iter().map(statistics_metric_bytes)),
                    ])
                })),
            ]),
            Self::MaterializedViews(needs) => checked_sum([
                checked_mul(needs.len(), std::mem::size_of::<MaterializedViewNeed>()),
                checked_sum(needs.iter().map(|need| {
                    checked_sum([
                        checked_mul(
                            need.referenced_relations.len(),
                            std::mem::size_of::<TableIdentity>(),
                        ),
                        checked_sum(need.referenced_relations.iter().map(table_identity_bytes)),
                    ])
                })),
            ]),
            Self::ProviderReads(needs) => checked_sum([
                checked_mul(needs.len(), std::mem::size_of::<ProviderReadNeed>()),
                checked_sum(needs.iter().map(provider_need_bytes)),
            ]),
        }
    }
}

#[derive(Debug)]
pub enum CatalogRelationOutcome {
    Resolved(Box<ResolvedAnalyzerTable>),
    Missing { reason: Box<str> },
}

#[derive(Debug)]
pub struct CatalogRelationFact {
    id: CompileNeedId,
    relation: TableIdentity,
    target: CatalogLookupTarget,
    outcome: CatalogRelationOutcome,
}

impl CatalogRelationFact {
    pub fn resolved(
        need: &CatalogRelationNeed,
        table: ResolvedAnalyzerTable,
    ) -> Result<Self, CompletionProtocolError> {
        if table.catalog.identity != need.relation {
            return Err(CompletionProtocolError::CatalogRelationMismatch {
                id: need.id,
                expected: Box::new(need.relation.clone()),
                actual: Box::new(table.catalog.identity.clone()),
            });
        }
        if !catalog_lookup_target_matches_table(need, &table) {
            return Err(CompletionProtocolError::CatalogLookupTargetMismatch { id: need.id });
        }
        Ok(Self {
            id: need.id,
            relation: need.relation.clone(),
            target: need.target,
            outcome: CatalogRelationOutcome::Resolved(Box::new(table)),
        })
    }

    pub fn missing(
        need: &CatalogRelationNeed,
        reason: impl Into<Box<str>>,
    ) -> Result<Self, CompletionProtocolError> {
        let reason = reason.into();
        if reason.trim().is_empty() {
            return Err(CompletionProtocolError::EmptyFailureReason { id: need.id });
        }
        Ok(Self {
            id: need.id,
            relation: need.relation.clone(),
            target: need.target,
            outcome: CatalogRelationOutcome::Missing { reason },
        })
    }

    pub const fn id(&self) -> CompileNeedId {
        self.id
    }

    pub const fn relation(&self) -> &TableIdentity {
        &self.relation
    }

    pub const fn target(&self) -> CatalogLookupTarget {
        self.target
    }

    pub const fn outcome(&self) -> &CatalogRelationOutcome {
        &self.outcome
    }
}

fn catalog_lookup_target_matches_table(
    need: &CatalogRelationNeed,
    table: &ResolvedAnalyzerTable,
) -> bool {
    let crate::planner::table::ScanSource::Sql(source) = &table.planner.source;
    if source.table.catalog != need.relation.catalog
        || source.table.namespace != need.relation.namespace
        || source.table.table != need.relation.table
    {
        return false;
    }
    match (need.target, &source.kind) {
        (
            CatalogLookupTarget::IcebergMetadata { kind: expected },
            crate::planner::table::SqlScanKind::Metadata { kind: actual, .. },
        ) => expected == *actual,
        (
            CatalogLookupTarget::Table { .. },
            crate::planner::table::SqlScanKind::Metadata { .. },
        )
        | (CatalogLookupTarget::IcebergMetadata { .. }, _) => false,
        (CatalogLookupTarget::Table { .. }, _) => true,
    }
}

#[derive(Debug)]
pub struct StatisticsFact {
    id: CompileNeedId,
    binding: SqlTableBindingId,
    metrics: Box<[StatisticsMetric]>,
    evidence: DmlStatisticsEvidence,
}

impl StatisticsFact {
    pub fn try_new(
        need: &StatisticsNeed,
        metrics: impl Into<Box<[StatisticsMetric]>>,
        evidence: DmlStatisticsEvidence,
    ) -> Result<Self, CompletionProtocolError> {
        let actual = statistics_binding(&evidence);
        if actual != need.binding {
            return Err(CompletionProtocolError::StatisticsBindingMismatch {
                id: need.id,
                expected: need.binding,
                actual,
            });
        }
        let metrics = metrics.into();
        validate_statistics_metric_coverage(need.id, &need.metrics, &metrics)?;
        if let DmlStatisticsEvidence::Available { evidence, .. } = &evidence {
            let evidence_metrics = evidence.metrics().keys().cloned().collect::<Vec<_>>();
            validate_statistics_metric_coverage(need.id, &need.metrics, &evidence_metrics)?;
        }
        Ok(Self {
            id: need.id,
            binding: need.binding,
            metrics,
            evidence,
        })
    }

    pub const fn id(&self) -> CompileNeedId {
        self.id
    }

    pub const fn binding(&self) -> SqlTableBindingId {
        self.binding
    }

    pub fn metrics(&self) -> &[StatisticsMetric] {
        &self.metrics
    }

    pub const fn evidence(&self) -> &DmlStatisticsEvidence {
        &self.evidence
    }

    pub fn into_evidence(self) -> DmlStatisticsEvidence {
        self.evidence
    }
}

#[derive(Debug)]
pub enum MaterializedViewOutcome {
    Observed(Box<[SqlMvRewriteDefinitionFacts]>),
    Missing { reason: Box<str> },
}

#[derive(Debug)]
pub struct MaterializedViewFact {
    id: CompileNeedId,
    outcome: MaterializedViewOutcome,
}

impl MaterializedViewFact {
    pub fn observed(
        need: &MaterializedViewNeed,
        definitions: impl Into<Box<[SqlMvRewriteDefinitionFacts]>>,
    ) -> Self {
        Self {
            id: need.id,
            outcome: MaterializedViewOutcome::Observed(definitions.into()),
        }
    }

    pub fn missing(
        need: &MaterializedViewNeed,
        reason: impl Into<Box<str>>,
    ) -> Result<Self, CompletionProtocolError> {
        let reason = reason.into();
        if reason.trim().is_empty() {
            return Err(CompletionProtocolError::EmptyFailureReason { id: need.id });
        }
        Ok(Self {
            id: need.id,
            outcome: MaterializedViewOutcome::Missing { reason },
        })
    }

    pub const fn id(&self) -> CompileNeedId {
        self.id
    }

    pub const fn outcome(&self) -> &MaterializedViewOutcome {
        &self.outcome
    }
}

#[derive(Clone, PartialEq)]
pub struct ProviderReadColumnFact {
    request_ordinal: u32,
    column: ProviderColumnReference,
    engine_type: ValueType,
}

impl ProviderReadColumnFact {
    pub fn new(
        request_ordinal: u32,
        column: ProviderColumnReference,
        engine_type: ValueType,
    ) -> Self {
        Self {
            request_ordinal,
            column,
            engine_type,
        }
    }

    pub const fn request_ordinal(&self) -> u32 {
        self.request_ordinal
    }

    pub const fn column(&self) -> &ProviderColumnReference {
        &self.column
    }

    pub const fn engine_type(&self) -> &ValueType {
        &self.engine_type
    }
}

impl fmt::Debug for ProviderReadColumnFact {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderReadColumnFact")
            .field("request_ordinal", &self.request_ordinal)
            .field("nullable", &self.engine_type.nullable())
            .field(
                "provider_payload_bytes",
                &self.column.column_payload.payload().len(),
            )
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderReadPredicateFact {
    occurrence: ProviderPredicateOccurrenceId,
    guarantee: PredicateGuaranteeKind,
}

impl ProviderReadPredicateFact {
    pub const fn new(
        occurrence: ProviderPredicateOccurrenceId,
        guarantee: PredicateGuaranteeKind,
    ) -> Self {
        Self {
            occurrence,
            guarantee,
        }
    }

    pub const fn occurrence(&self) -> ProviderPredicateOccurrenceId {
        self.occurrence
    }

    pub const fn guarantee(&self) -> PredicateGuaranteeKind {
        self.guarantee
    }
}

/// Frozen static provider read contract. Runtime capabilities stay in the
/// frontend sidecar keyed by `sql_binding` and `read.binding`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderReadLimitFact {
    NotRequested,
    Exact(u64),
    Residual(u64),
}

/// Provider-owned physical facts expressed in the read request's ordinal
/// vocabulary. Final-plan [`novarocks_physical_plan::ValueId`] values do not
/// exist while the provider answers a completion need.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderReadProperties {
    pub distribution: ProviderReadDistribution,
    pub ordering: Box<[ProviderReadOrderingKey]>,
}

impl ProviderReadProperties {
    pub fn unconstrained() -> Self {
        Self {
            distribution: ProviderReadDistribution::Unconstrained,
            ordering: Box::new([]),
        }
    }
}

/// Distribution guaranteed by a provider read. Key ordinals address the
/// dense ordered projection in [`ProviderReadRequestBinding::projection`].
/// Broadcast is deliberately absent because a provider read produces each
/// logical row once.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderReadDistribution {
    Unconstrained,
    Singleton,
    RoundRobin,
    Hash {
        keys: Box<[u32]>,
        scheme: ProviderHashPartitionScheme,
    },
    BucketShuffle {
        keys: Box<[u32]>,
        scheme: ProviderBucketPartitionScheme,
    },
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProviderPartitionSpaceToken([u8; 32]);

impl ProviderPartitionSpaceToken {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProviderPartitionCountToken([u8; 32]);

impl ProviderPartitionCountToken {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderPartitionCountDomain {
    pub min: u32,
    pub max: u32,
    pub requires_power_of_two: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderHashPartitionScheme {
    pub space: ProviderPartitionSpaceToken,
    pub count: ProviderPartitionCountToken,
    pub admissible: ProviderPartitionCountDomain,
    pub algorithm: PartitionHashAlgorithm,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderBucketOrdinalDomainProof {
    pub first_ordinal: u32,
    pub ordinal_count: u32,
    pub evidence_digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderBucketPartitionScheme {
    pub space: ProviderPartitionSpaceToken,
    pub bucket_count: u32,
    pub hash: PartitionHashAlgorithm,
    pub layout: BucketLayoutAlgorithm,
    pub ordinal_domain: ProviderBucketOrdinalDomainProof,
}

/// One provider ordering key in request-projection ordinal space.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ProviderReadOrderingKey {
    pub request_ordinal: u32,
    pub direction: SortDirection,
    pub null_ordering: NullOrdering,
}

/// Exact request echoed structurally by a provider response.
#[derive(Clone, PartialEq)]
pub struct ProviderReadRequestBinding {
    relation: ProviderReadRelationNeed,
    projection: Box<[ProviderReadColumnNeed]>,
    filter: Constraint<u32>,
    predicates: Box<[ProviderReadPredicateNeed]>,
    limit: Option<u64>,
}

impl ProviderReadRequestBinding {
    pub fn from_need(need: &ProviderReadNeed) -> Self {
        Self {
            relation: need.relation.clone(),
            projection: need.columns.clone(),
            filter: need.filter.clone(),
            predicates: need.predicates.clone(),
            limit: need.limit,
        }
    }

    pub const fn relation(&self) -> &ProviderReadRelationNeed {
        &self.relation
    }

    pub fn projection(&self) -> &[ProviderReadColumnNeed] {
        &self.projection
    }

    pub const fn filter(&self) -> &Constraint<u32> {
        &self.filter
    }

    pub fn predicates(&self) -> &[ProviderReadPredicateNeed] {
        &self.predicates
    }

    pub const fn limit(&self) -> Option<u64> {
        self.limit
    }
}

impl fmt::Debug for ProviderReadRequestBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderReadRequestBinding")
            .field("relation", &self.relation)
            .field("projection_columns", &self.projection.len())
            .field("predicate_occurrences", &self.predicates.len())
            .field("limit", &self.limit)
            .finish()
    }
}

#[derive(Clone, PartialEq)]
pub struct ProviderReadStaticContract {
    pub sql_binding: SqlTableBindingId,
    pub request: ProviderReadRequestBinding,
    pub read: ProviderReadReference,
    /// Exact provider metadata kind accepted while opening this frozen read.
    pub metadata_kind: Option<novarocks_spi::connector::read_stack::ConnectorReadMetadataKind>,
    pub selection_digest: [u8; 32],
    pub schema: Box<[ProviderReadColumnFact]>,
    pub predicates: Box<[ProviderReadPredicateFact]>,
    pub limit: ProviderReadLimitFact,
    pub provided_properties: ProviderReadProperties,
    pub coverage_evidence: Box<[u8]>,
}

impl fmt::Debug for ProviderReadStaticContract {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderReadStaticContract")
            .field("sql_binding", &self.sql_binding)
            .field("request", &self.request)
            .field("relation_kind", &self.read.relation.kind())
            .field("metadata_kind", &self.metadata_kind)
            .field("schema_columns", &self.schema.len())
            .field("predicate_guarantees", &self.predicates.len())
            .field("limit", &self.limit)
            .field("provided_properties", &self.provided_properties)
            .field("coverage_evidence_bytes", &self.coverage_evidence.len())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, PartialEq)]
pub struct ProviderReadFact {
    id: CompileNeedId,
    occurrence: ProviderReadOccurrenceId,
    binding: SqlTableBindingId,
    contract: ProviderReadStaticContract,
}

impl fmt::Debug for ProviderReadFact {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderReadFact")
            .field("id", &self.id)
            .field("occurrence", &self.occurrence)
            .field("binding", &self.binding)
            .field("outcome", &"complete_frozen_contract")
            .finish()
    }
}

impl ProviderReadFact {
    pub fn negotiated(
        need: &ProviderReadNeed,
        contract: ProviderReadStaticContract,
    ) -> Result<Self, CompletionProtocolError> {
        validate_provider_contract(need, &contract)?;
        Ok(Self {
            id: need.id,
            occurrence: need.occurrence,
            binding: need.binding,
            contract,
        })
    }

    pub const fn id(&self) -> CompileNeedId {
        self.id
    }

    pub const fn binding(&self) -> SqlTableBindingId {
        self.binding
    }

    pub const fn occurrence(&self) -> ProviderReadOccurrenceId {
        self.occurrence
    }

    pub const fn contract(&self) -> &ProviderReadStaticContract {
        &self.contract
    }

    pub fn into_contract(self) -> ProviderReadStaticContract {
        self.contract
    }
}

#[derive(Debug)]
pub enum SqlFactBatch {
    CatalogRelations(Box<[CatalogRelationFact]>),
    Statistics(Box<[StatisticsFact]>),
    MaterializedViews(Box<[MaterializedViewFact]>),
    ProviderReads(Box<[ProviderReadFact]>),
}

impl SqlFactBatch {
    pub const fn kind(&self) -> NeedKind {
        match self {
            Self::CatalogRelations(_) => NeedKind::CatalogRelation,
            Self::Statistics(_) => NeedKind::Statistics,
            Self::MaterializedViews(_) => NeedKind::MaterializedView,
            Self::ProviderReads(_) => NeedKind::ProviderRead,
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::CatalogRelations(facts) => facts.len(),
            Self::Statistics(facts) => facts.len(),
            Self::MaterializedViews(facts) => facts.len(),
            Self::ProviderReads(facts) => facts.len(),
        }
    }

    fn ids(&self) -> Box<dyn Iterator<Item = CompileNeedId> + '_> {
        match self {
            Self::CatalogRelations(facts) => Box::new(facts.iter().map(CatalogRelationFact::id)),
            Self::Statistics(facts) => Box::new(facts.iter().map(StatisticsFact::id)),
            Self::MaterializedViews(facts) => Box::new(facts.iter().map(MaterializedViewFact::id)),
            Self::ProviderReads(facts) => Box::new(facts.iter().map(ProviderReadFact::id)),
        }
    }

    fn accounted_bytes(&self) -> Result<u64, SqlCompileProgressError> {
        match self {
            Self::CatalogRelations(facts) => Ok(checked_sum(facts.iter().map(catalog_fact_bytes))?),
            Self::Statistics(facts) => Ok(checked_sum(facts.iter().map(statistics_fact_bytes))?),
            Self::MaterializedViews(facts) => materialized_view_facts_bytes(facts),
            Self::ProviderReads(facts) => Ok(checked_sum(facts.iter().map(provider_fact_bytes))?),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlDisplayIntent {
    Execute,
    Explain { level: ExplainLevel, analyze: bool },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlDisplayAnnotation {
    key: Box<str>,
    value: Box<str>,
}

impl SqlDisplayAnnotation {
    pub fn try_new(
        key: impl Into<Box<str>>,
        value: impl Into<Box<str>>,
    ) -> Result<Self, CompletionProtocolError> {
        let key = key.into();
        if key.trim().is_empty() {
            return Err(CompletionProtocolError::InvalidDisplayAnnotation);
        }
        Ok(Self {
            key,
            value: value.into(),
        })
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

struct CompletionTracking {
    seen_needs: BTreeSet<CompileNeedId>,
    limits: CompletionLimits,
    usage: CompletionUsage,
}

/// Private, move-only compiler state machine. No variant contains a runtime
/// capability or is visible to an application.
pub(crate) enum CompilerContinuation {
    Catalog(Box<super::SqlCatalogCompletionState>),
    Statistics(Box<super::SqlStatisticsCompletionState>),
    MaterializedView(Box<super::SqlMaterializedViewCompletionState>),
    ProviderRead(Box<super::SqlProviderReadCompletionState>),
    #[cfg(test)]
    Test(NeedKind),
}

impl CompilerContinuation {
    pub(super) fn catalog(state: super::SqlCatalogCompletionState) -> Self {
        Self::Catalog(Box::new(state))
    }

    pub(super) fn statistics(state: super::SqlStatisticsCompletionState) -> Self {
        Self::Statistics(Box::new(state))
    }

    pub(super) fn materialized_view(state: super::SqlMaterializedViewCompletionState) -> Self {
        Self::MaterializedView(Box::new(state))
    }

    pub(super) fn provider_read(state: super::SqlProviderReadCompletionState) -> Self {
        Self::ProviderRead(Box::new(state))
    }

    #[cfg(test)]
    fn test(kind: NeedKind) -> Self {
        Self::Test(kind)
    }
}

/// One private pure-compiler transition. A pending transition cannot contain
/// a lowered draft; only `Ready` owns the source-certified draft to consume.
pub(crate) enum CompilerStep {
    Need {
        batch: SqlNeedBatch,
        continuation: CompilerContinuation,
    },
    Ready {
        version: PlanVersionId,
        draft: Box<LoweredSqlPhysicalDraft>,
        display_intent: SqlDisplayIntent,
        display_annotations: Box<[SqlDisplayAnnotation]>,
    },
    #[cfg(test)]
    TestReady {
        version: PlanVersionId,
        builder: Box<PlanBuilder>,
        display_intent: SqlDisplayIntent,
        display_annotations: Box<[SqlDisplayAnnotation]>,
    },
}

impl CompilerStep {
    pub(super) fn need(batch: SqlNeedBatch, continuation: CompilerContinuation) -> Self {
        Self::Need {
            batch,
            continuation,
        }
    }

    pub(super) fn ready(
        version: PlanVersionId,
        draft: LoweredSqlPhysicalDraft,
        display_intent: SqlDisplayIntent,
        display_annotations: impl Into<Box<[SqlDisplayAnnotation]>>,
    ) -> Self {
        Self::Ready {
            version,
            draft: Box::new(draft),
            display_intent,
            display_annotations: display_annotations.into(),
        }
    }
}

/// Internal seed produced by SQL's owned parse/analyze/build state.
pub struct SqlCompileRequest {
    first_step: CompilerStep,
    limits: CompletionLimits,
}

impl SqlCompileRequest {
    #[cfg(test)]
    fn ready(
        version: PlanVersionId,
        builder: PlanBuilder,
        display_intent: SqlDisplayIntent,
        display_annotations: impl Into<Box<[SqlDisplayAnnotation]>>,
        limits: CompletionLimits,
    ) -> Self {
        Self {
            first_step: CompilerStep::TestReady {
                version,
                builder: Box::new(builder),
                display_intent,
                display_annotations: display_annotations.into(),
            },
            limits,
        }
    }

    pub(super) fn pending(first_step: CompilerStep, limits: CompletionLimits) -> Self {
        Self { first_step, limits }
    }
}

/// Opaque incomplete compilation. Its only semantic read surface is the
/// current need batch; it exposes no builder, plan, encoder or scheduler.
pub struct SqlCompilation {
    needs: SqlNeedBatch,
    continuation: CompilerContinuation,
    tracking: CompletionTracking,
}

impl fmt::Debug for SqlCompilation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqlCompilation")
            .field("needs", &self.needs)
            .field("usage", &self.usage())
            .finish_non_exhaustive()
    }
}

impl SqlCompilation {
    pub const fn needs(&self) -> &SqlNeedBatch {
        &self.needs
    }

    pub const fn usage(&self) -> CompletionUsage {
        self.tracking.usage
    }

    pub const fn limits(&self) -> CompletionLimits {
        self.tracking.limits
    }
}

pub enum SqlCompileProgress {
    Incomplete(SqlCompilation),
    Complete(SqlCompletedPlan),
}

impl SqlCompileProgress {
    pub fn into_complete(self) -> Result<SqlCompletedPlan, SqlCompileProgressError> {
        match self {
            Self::Complete(completed) => Ok(completed),
            Self::Incomplete(_) => Err(CompletionProtocolError::PlanIsIncomplete.into()),
        }
    }
}

enum CompletedPlanSource {
    Authored(SqlAuthoredPhysicalPlan),
    #[cfg(test)]
    StructurallyUntrusted(Box<novarocks_physical_plan::PhysicalPlan>),
}

impl CompletedPlanSource {
    fn plan(&self) -> &novarocks_physical_plan::PhysicalPlan {
        match self {
            Self::Authored(source) => source.plan(),
            #[cfg(test)]
            Self::StructurallyUntrusted(plan) => plan,
        }
    }

    fn into_authored(self) -> SqlAuthoredPhysicalPlan {
        match self {
            Self::Authored(source) => source,
            #[cfg(test)]
            Self::StructurallyUntrusted(_) => {
                panic!("a structural-only test plan has no authored source journal")
            }
        }
    }
}

pub struct SqlCompletedPlan {
    plan: CompletedPlanSource,
    display_intent: SqlDisplayIntent,
    display_annotations: Box<[SqlDisplayAnnotation]>,
}

impl SqlCompletedPlan {
    pub fn plan(&self) -> &novarocks_physical_plan::PhysicalPlan {
        self.plan.plan()
    }

    pub const fn display_intent(&self) -> SqlDisplayIntent {
        self.display_intent
    }

    pub fn display_annotations(&self) -> &[SqlDisplayAnnotation] {
        &self.display_annotations
    }

    /// Project the one exact typed result after optimization and root lowering.
    /// Slot identity is bound later by the sole Native layout; this method
    /// neither admits a child execution nor funds a collector.
    pub fn scalar_schema(
        &self,
    ) -> Result<novarocks_result_contract::ScalarSchema, SqlCompileError> {
        let result = self.plan.plan().result_port().ok_or_else(|| {
            SqlCompileError::InvalidRequest("scalar execution has no result port".into())
        })?;
        result.scalar_schema.clone().ok_or_else(|| SqlCompileError::InvalidRequest(
            "scalar output requires one column with exact semantic facts inside its admitted type profile".into()
        ))
    }

    pub fn into_plan(self) -> SqlAuthoredPhysicalPlan {
        self.plan.into_authored()
    }

    pub fn into_parts(
        self,
    ) -> (
        SqlAuthoredPhysicalPlan,
        SqlDisplayIntent,
        Box<[SqlDisplayAnnotation]>,
    ) {
        (
            self.plan.into_authored(),
            self.display_intent,
            self.display_annotations,
        )
    }
}

impl SqlCompiler {
    /// Advances an owned, no-I/O request until it publishes one exact need
    /// batch or validates and returns the completed plan.
    pub fn start(
        request: SqlCompileRequest,
        control: &super::SqlCompileControl,
    ) -> Result<SqlCompileProgress, SqlCompileProgressError> {
        control.check()?;
        advance_step(
            request.first_step,
            CompletionTracking {
                seen_needs: BTreeSet::new(),
                limits: request.limits,
                usage: CompletionUsage::default(),
            },
            control,
        )
    }

    /// Consumes one incomplete compilation and its exact typed answer batch.
    /// The continuation automatically chooses and advances the next stage.
    pub fn finish(
        compilation: SqlCompilation,
        facts: SqlFactBatch,
        control: &super::SqlCompileControl,
    ) -> Result<SqlCompileProgress, SqlCompileProgressError> {
        control.check()?;
        let SqlCompilation {
            needs,
            continuation,
            mut tracking,
        } = compilation;
        validate_and_account_facts(&needs, &facts, &mut tracking)?;
        let step = match (continuation, facts) {
            (CompilerContinuation::Catalog(state), SqlFactBatch::CatalogRelations(facts)) => {
                super::resume_catalog(*state, facts, control)?
            }
            (CompilerContinuation::Statistics(state), SqlFactBatch::Statistics(facts)) => {
                super::resume_statistics(*state, facts, control)?
            }
            (
                CompilerContinuation::MaterializedView(state),
                SqlFactBatch::MaterializedViews(facts),
            ) => super::resume_materialized_view(*state, facts, control)?,
            (CompilerContinuation::ProviderRead(state), SqlFactBatch::ProviderReads(facts)) => {
                super::resume_provider_read(*state, facts, control)?
            }
            (continuation, facts) => {
                return Err(CompletionProtocolError::FactBatchKindMismatch {
                    expected: continuation.kind(),
                    actual: facts.kind(),
                }
                .into());
            }
        };
        // Need and Fact batches are protocol exchange values. The transition
        // consumes both before publishing the next batch; compiler graph
        // memory belongs to the caller's WorkScope reservation.
        tracking.usage.exchange_bytes = 0;
        advance_step(step, tracking, control)
    }
}

impl CompilerContinuation {
    const fn kind(&self) -> NeedKind {
        match self {
            Self::Catalog(_) => NeedKind::CatalogRelation,
            Self::Statistics(_) => NeedKind::Statistics,
            Self::MaterializedView(_) => NeedKind::MaterializedView,
            Self::ProviderRead(_) => NeedKind::ProviderRead,
            #[cfg(test)]
            Self::Test(kind) => *kind,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompletionResource {
    Rounds,
    DistinctNeeds,
    BatchItems,
    ExchangeBytes,
}

#[derive(Debug)]
pub enum CompletionProtocolError {
    InvalidLimit(&'static str),
    InvalidRelationIdentity(Box<TableIdentity>),
    InvalidNeed {
        id: CompileNeedId,
        reason: &'static str,
    },
    InvalidProviderReadColumn {
        ordinal: u32,
    },
    ProviderReadColumnTypeMismatch {
        ordinal: u32,
    },
    EmptyNeedBatch(NeedKind),
    DuplicateNeed(CompileNeedId),
    NeedWasAlreadyIssued(CompileNeedId),
    ContinuationKindMismatch {
        need: NeedKind,
        continuation: NeedKind,
    },
    FactBatchKindMismatch {
        expected: NeedKind,
        actual: NeedKind,
    },
    FactCountMismatch {
        expected: usize,
        actual: usize,
    },
    DuplicateFact(CompileNeedId),
    UnexpectedFact(CompileNeedId),
    MissingFact(CompileNeedId),
    CatalogRelationMismatch {
        id: CompileNeedId,
        expected: Box<TableIdentity>,
        actual: Box<TableIdentity>,
    },
    CatalogLookupTargetMismatch {
        id: CompileNeedId,
    },
    StatisticsBindingMismatch {
        id: CompileNeedId,
        expected: SqlTableBindingId,
        actual: SqlTableBindingId,
    },
    StatisticsMetricMissing {
        id: CompileNeedId,
        metric: StatisticsMetric,
    },
    StatisticsMetricDuplicate {
        id: CompileNeedId,
        metric: StatisticsMetric,
    },
    StatisticsMetricExtra {
        id: CompileNeedId,
        metric: StatisticsMetric,
    },
    ProviderBindingMismatch {
        id: CompileNeedId,
    },
    ProviderOccurrenceMismatch {
        id: CompileNeedId,
        expected: ProviderReadOccurrenceId,
        actual: ProviderReadOccurrenceId,
    },
    ProviderRequestMismatch {
        id: CompileNeedId,
    },
    ProviderRelationKindMismatch {
        id: CompileNeedId,
    },
    ProviderEnvelopeMismatch {
        id: CompileNeedId,
    },
    ProviderProjectionMismatch {
        id: CompileNeedId,
    },
    ProviderPredicateMismatch {
        id: CompileNeedId,
    },
    ProviderLimitMismatch {
        id: CompileNeedId,
    },
    ProviderPropertyOrdinalOutOfRange {
        id: CompileNeedId,
        ordinal: u32,
        projection_len: usize,
    },
    ProviderPropertyDuplicateOrdinal {
        id: CompileNeedId,
        property: &'static str,
        ordinal: u32,
    },
    ProviderDistributionHasNoKeys {
        id: CompileNeedId,
        distribution: &'static str,
    },
    ProviderPartitionSchemeInvalid {
        id: CompileNeedId,
        reason: &'static str,
    },
    DuplicateMaterializedViewDefinition {
        id: CompileNeedId,
        mv_id: i64,
    },
    MaterializedViewBaseMismatch {
        id: CompileNeedId,
        base: Box<str>,
    },
    EmptyFailureReason {
        id: CompileNeedId,
    },
    InvalidDisplayAnnotation,
    BudgetExceeded {
        resource: CompletionResource,
        limit: u64,
        attempted: u64,
    },
    ResourceCountOverflow(CompletionResource),
    PlanVersionMismatch {
        expected: PlanVersionId,
        actual: PlanVersionId,
    },
    PlanValidation(String),
    PlanIsIncomplete,
}

impl fmt::Display for CompletionProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLimit(resource) => {
                write!(
                    formatter,
                    "completion {resource} limit must be greater than zero"
                )
            }
            Self::InvalidRelationIdentity(identity) => {
                write!(
                    formatter,
                    "invalid catalog relation identity `{}`",
                    identity.fqn()
                )
            }
            Self::InvalidNeed { id, reason } => {
                write!(
                    formatter,
                    "completion need {} is invalid: {reason}",
                    id.get()
                )
            }
            Self::InvalidProviderReadColumn { ordinal } => {
                write!(
                    formatter,
                    "provider read column {ordinal} has an empty name"
                )
            }
            Self::ProviderReadColumnTypeMismatch { ordinal } => write!(
                formatter,
                "provider read column {ordinal} has incompatible engine and connector types"
            ),
            Self::EmptyNeedBatch(kind) => write!(formatter, "{kind:?} completion batch is empty"),
            Self::DuplicateNeed(id) => {
                write!(formatter, "completion batch repeats need {}", id.get())
            }
            Self::NeedWasAlreadyIssued(id) => {
                write!(formatter, "completion need {} was already issued", id.get())
            }
            Self::ContinuationKindMismatch { need, continuation } => write!(
                formatter,
                "{need:?} need batch cannot resume {continuation:?} compiler state"
            ),
            Self::FactBatchKindMismatch { expected, actual } => write!(
                formatter,
                "completion fact batch has kind {actual:?}, expected {expected:?}"
            ),
            Self::FactCountMismatch { expected, actual } => write!(
                formatter,
                "completion fact batch has {actual} items, expected {expected}"
            ),
            Self::DuplicateFact(id) => {
                write!(formatter, "completion response repeats fact {}", id.get())
            }
            Self::UnexpectedFact(id) => write!(
                formatter,
                "completion response contains unexpected fact {}",
                id.get()
            ),
            Self::MissingFact(id) => write!(
                formatter,
                "completion response is missing fact {}",
                id.get()
            ),
            Self::CatalogRelationMismatch {
                id,
                expected,
                actual,
            } => write!(
                formatter,
                "catalog fact {} resolves `{}` instead of `{}`",
                id.get(),
                actual.fqn(),
                expected.fqn()
            ),
            Self::CatalogLookupTargetMismatch { id } => write!(
                formatter,
                "catalog fact {} answers a different lookup target",
                id.get()
            ),
            Self::StatisticsBindingMismatch {
                id,
                expected,
                actual,
            } => write!(
                formatter,
                "statistics fact {} has binding {:?}, expected {:?}",
                id.get(),
                actual,
                expected
            ),
            Self::StatisticsMetricMissing { id, metric } => write!(
                formatter,
                "statistics fact {} is missing requested metric {metric:?}",
                id.get()
            ),
            Self::StatisticsMetricDuplicate { id, metric } => write!(
                formatter,
                "statistics fact {} repeats metric {metric:?}",
                id.get()
            ),
            Self::StatisticsMetricExtra { id, metric } => write!(
                formatter,
                "statistics fact {} contains unrequested metric {metric:?}",
                id.get()
            ),
            Self::ProviderBindingMismatch { id } => write!(
                formatter,
                "provider read fact {} has a different SQL binding",
                id.get()
            ),
            Self::ProviderOccurrenceMismatch {
                id,
                expected,
                actual,
            } => write!(
                formatter,
                "provider read fact {} has occurrence {}, expected {}",
                id.get(),
                actual.get(),
                expected.get()
            ),
            Self::ProviderRequestMismatch { id } => write!(
                formatter,
                "provider read fact {} answers a different negotiation request",
                id.get()
            ),
            Self::ProviderRelationKindMismatch { id } => write!(
                formatter,
                "provider read fact {} has a different relation kind",
                id.get()
            ),
            Self::ProviderEnvelopeMismatch { id } => write!(
                formatter,
                "provider read fact {} contains a payload outside its exact read binding",
                id.get()
            ),
            Self::ProviderProjectionMismatch { id } => write!(
                formatter,
                "provider read fact {} does not exactly answer the ordered projection",
                id.get()
            ),
            Self::ProviderPredicateMismatch { id } => write!(
                formatter,
                "provider read fact {} does not exactly classify every predicate occurrence",
                id.get()
            ),
            Self::ProviderLimitMismatch { id } => write!(
                formatter,
                "provider read fact {} does not exactly classify the requested limit",
                id.get()
            ),
            Self::ProviderPropertyOrdinalOutOfRange {
                id,
                ordinal,
                projection_len,
            } => write!(
                formatter,
                "provider read fact {} property ordinal {ordinal} is outside projection width {projection_len}",
                id.get()
            ),
            Self::ProviderPropertyDuplicateOrdinal {
                id,
                property,
                ordinal,
            } => write!(
                formatter,
                "provider read fact {} repeats request ordinal {ordinal} in {property}",
                id.get()
            ),
            Self::ProviderDistributionHasNoKeys { id, distribution } => write!(
                formatter,
                "provider read fact {} declares {distribution} distribution without keys",
                id.get()
            ),
            Self::ProviderPartitionSchemeInvalid { id, reason } => write!(
                formatter,
                "provider read fact {} has an invalid partition scheme: {reason}",
                id.get()
            ),
            Self::DuplicateMaterializedViewDefinition { id, mv_id } => write!(
                formatter,
                "materialized-view fact {} repeats definition {mv_id}",
                id.get()
            ),
            Self::MaterializedViewBaseMismatch { id, base } => write!(
                formatter,
                "materialized-view fact {} contains unrequested base relation `{base}`",
                id.get()
            ),
            Self::EmptyFailureReason { id } => {
                write!(
                    formatter,
                    "completion fact {} has an empty failure reason",
                    id.get()
                )
            }
            Self::InvalidDisplayAnnotation => {
                formatter.write_str("display annotation key must not be empty")
            }
            Self::BudgetExceeded {
                resource,
                limit,
                attempted,
            } => write!(
                formatter,
                "completion {resource:?} budget is {limit}, attempted {attempted}"
            ),
            Self::ResourceCountOverflow(resource) => {
                write!(formatter, "completion {resource:?} accounting overflowed")
            }
            Self::PlanVersionMismatch { expected, actual } => write!(
                formatter,
                "completed physical plan version {:?} does not match compilation version {:?}",
                actual, expected
            ),
            Self::PlanValidation(error) => write!(formatter, "physical plan is invalid: {error}"),
            Self::PlanIsIncomplete => {
                formatter.write_str("physical plan compilation is incomplete")
            }
        }
    }
}

impl std::error::Error for CompletionProtocolError {}

#[derive(Debug)]
pub enum SqlCompileProgressError {
    Compile(SqlCompileError),
    Protocol(CompletionProtocolError),
}

impl fmt::Display for SqlCompileProgressError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Compile(error) => error.fmt(formatter),
            Self::Protocol(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for SqlCompileProgressError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Compile(source) => Some(source),
            Self::Protocol(source) => Some(source),
        }
    }
}

impl From<SqlCompileError> for SqlCompileProgressError {
    fn from(error: SqlCompileError) -> Self {
        Self::Compile(error)
    }
}

impl From<CompletionProtocolError> for SqlCompileProgressError {
    fn from(error: CompletionProtocolError) -> Self {
        Self::Protocol(error)
    }
}

fn advance_step(
    step: CompilerStep,
    mut tracking: CompletionTracking,
    control: &super::SqlCompileControl,
) -> Result<SqlCompileProgress, SqlCompileProgressError> {
    match step {
        CompilerStep::Need {
            batch,
            continuation,
        } => {
            if batch.kind() != continuation.kind() {
                return Err(CompletionProtocolError::ContinuationKindMismatch {
                    need: batch.kind(),
                    continuation: continuation.kind(),
                }
                .into());
            }
            reserve_need_batch(&mut tracking, &batch)?;
            Ok(SqlCompileProgress::Incomplete(SqlCompilation {
                needs: batch,
                continuation,
                tracking,
            }))
        }
        CompilerStep::Ready {
            version,
            draft,
            display_intent,
            display_annotations,
        } => complete(
            version,
            *draft,
            display_intent,
            display_annotations,
            control,
        ),
        #[cfg(test)]
        CompilerStep::TestReady {
            version,
            builder,
            display_intent,
            display_annotations,
        } => {
            let plan = builder
                .finish_observed(control)
                .map_err(completion_plan_construction_error)?;
            complete_source(
                version,
                CompletedPlanSource::StructurallyUntrusted(Box::new(plan)),
                display_intent,
                display_annotations,
            )
        }
    }
}

fn validate_and_account_facts(
    needs: &SqlNeedBatch,
    facts: &SqlFactBatch,
    tracking: &mut CompletionTracking,
) -> Result<(), SqlCompileProgressError> {
    let expected_kind = needs.kind();
    let actual_kind = facts.kind();
    if expected_kind != actual_kind {
        return Err(CompletionProtocolError::FactBatchKindMismatch {
            expected: expected_kind,
            actual: actual_kind,
        }
        .into());
    }
    let expected = needs.ids().collect::<BTreeSet<_>>();
    let mut actual = BTreeSet::new();
    for id in facts.ids() {
        if !actual.insert(id) {
            return Err(CompletionProtocolError::DuplicateFact(id).into());
        }
        if !expected.contains(&id) {
            return Err(CompletionProtocolError::UnexpectedFact(id).into());
        }
    }
    if facts.len() != expected.len() {
        if let Some(id) = expected.iter().find(|id| !actual.contains(id)) {
            return Err(CompletionProtocolError::MissingFact(*id).into());
        }
        return Err(CompletionProtocolError::FactCountMismatch {
            expected: expected.len(),
            actual: facts.len(),
        }
        .into());
    }

    validate_fact_semantics(needs, facts)?;
    let request_bytes = needs.accounted_bytes()?;
    let fact_bytes = facts.accounted_bytes()?;
    let peak = tracking
        .usage
        .exchange_bytes
        .checked_add(fact_bytes)
        .ok_or(CompletionProtocolError::ResourceCountOverflow(
            CompletionResource::ExchangeBytes,
        ))?;
    check_budget(
        CompletionResource::ExchangeBytes,
        tracking.limits.max_exchange_bytes,
        peak,
    )?;
    tracking.usage.exchange_bytes =
        peak.checked_sub(request_bytes)
            .ok_or(CompletionProtocolError::ResourceCountOverflow(
                CompletionResource::ExchangeBytes,
            ))?;
    Ok(())
}

fn complete(
    version: PlanVersionId,
    draft: LoweredSqlPhysicalDraft,
    display_intent: SqlDisplayIntent,
    display_annotations: Box<[SqlDisplayAnnotation]>,
    control: &super::SqlCompileControl,
) -> Result<SqlCompileProgress, SqlCompileProgressError> {
    let source = draft
        .finish_with_dependency_observer_observed(control)
        .map_err(completion_plan_publication_error)?;
    complete_source(
        version,
        CompletedPlanSource::Authored(source),
        display_intent,
        display_annotations,
    )
}

fn completion_plan_publication_error(
    error: crate::planner::distributed::build::SqlPublicationError,
) -> SqlCompileProgressError {
    match error {
        crate::planner::distributed::build::SqlPublicationError::Construction(error) => {
            completion_plan_construction_error(error)
        }
        crate::planner::distributed::build::SqlPublicationError::Support(error) => {
            SqlCompileProgressError::Compile(error.into_compile_error())
        }
    }
}

fn completion_plan_construction_error(
    error: novarocks_physical_plan::PlanConstructionError,
) -> SqlCompileProgressError {
    match error {
        novarocks_physical_plan::PlanConstructionError::Constants(
            novarocks_physical_plan::ConstantReferenceError::Control(error),
        ) => SqlCompileProgressError::Compile(SqlCompileError::from(error)),
        error => SqlCompileProgressError::Protocol(CompletionProtocolError::PlanValidation(
            error.to_string(),
        )),
    }
}

fn complete_source(
    version: PlanVersionId,
    plan: CompletedPlanSource,
    display_intent: SqlDisplayIntent,
    display_annotations: Box<[SqlDisplayAnnotation]>,
) -> Result<SqlCompileProgress, SqlCompileProgressError> {
    if plan.plan().version() != version {
        return Err(CompletionProtocolError::PlanVersionMismatch {
            expected: version,
            actual: plan.plan().version(),
        }
        .into());
    }
    Ok(SqlCompileProgress::Complete(SqlCompletedPlan {
        plan,
        display_intent,
        display_annotations,
    }))
}

fn reserve_need_batch(
    tracking: &mut CompletionTracking,
    needs: &SqlNeedBatch,
) -> Result<(), CompletionProtocolError> {
    if needs.is_empty() {
        return Err(CompletionProtocolError::EmptyNeedBatch(needs.kind()));
    }
    let mut batch_ids = BTreeSet::new();
    for id in needs.ids() {
        if !batch_ids.insert(id) {
            return Err(CompletionProtocolError::DuplicateNeed(id));
        }
        if tracking.seen_needs.contains(&id) {
            return Err(CompletionProtocolError::NeedWasAlreadyIssued(id));
        }
    }
    let rounds = tracking.usage.rounds.checked_add(1).ok_or(
        CompletionProtocolError::ResourceCountOverflow(CompletionResource::Rounds),
    )?;
    let count = u32::try_from(needs.len()).map_err(|_| {
        CompletionProtocolError::ResourceCountOverflow(CompletionResource::BatchItems)
    })?;
    let distinct = tracking.usage.distinct_needs.checked_add(count).ok_or(
        CompletionProtocolError::ResourceCountOverflow(CompletionResource::DistinctNeeds),
    )?;
    let exchange_bytes = tracking
        .usage
        .exchange_bytes
        .checked_add(needs.accounted_bytes()?)
        .ok_or(CompletionProtocolError::ResourceCountOverflow(
            CompletionResource::ExchangeBytes,
        ))?;
    check_budget(
        CompletionResource::Rounds,
        tracking.limits.max_rounds,
        rounds,
    )?;
    check_budget(
        CompletionResource::BatchItems,
        tracking.limits.max_distinct_needs,
        count,
    )?;
    check_budget(
        CompletionResource::DistinctNeeds,
        tracking.limits.max_distinct_needs,
        distinct,
    )?;
    check_budget(
        CompletionResource::ExchangeBytes,
        tracking.limits.max_exchange_bytes,
        exchange_bytes,
    )?;
    tracking.usage = CompletionUsage {
        rounds,
        distinct_needs: distinct,
        exchange_bytes,
    };
    tracking.seen_needs.extend(batch_ids);
    Ok(())
}

fn validate_fact_semantics(
    needs: &SqlNeedBatch,
    facts: &SqlFactBatch,
) -> Result<(), CompletionProtocolError> {
    match (needs, facts) {
        (SqlNeedBatch::CatalogRelations(needs), SqlFactBatch::CatalogRelations(facts)) => {
            let expected = needs
                .iter()
                .map(|need| (need.id, (&need.relation, need.target)))
                .collect::<BTreeMap<_, _>>();
            for fact in facts.iter() {
                let (expected_relation, expected_target) = expected[&fact.id];
                if expected_relation != &fact.relation {
                    return Err(CompletionProtocolError::CatalogRelationMismatch {
                        id: fact.id,
                        expected: Box::new(expected_relation.clone()),
                        actual: Box::new(fact.relation.clone()),
                    });
                }
                if expected_target != fact.target {
                    return Err(CompletionProtocolError::CatalogLookupTargetMismatch {
                        id: fact.id,
                    });
                }
            }
        }
        (SqlNeedBatch::Statistics(needs), SqlFactBatch::Statistics(facts)) => {
            let expected = needs
                .iter()
                .map(|need| (need.id, need))
                .collect::<BTreeMap<_, _>>();
            for fact in facts.iter() {
                let need = expected[&fact.id];
                let binding = need.binding;
                if fact.binding != binding || statistics_binding(&fact.evidence) != binding {
                    return Err(CompletionProtocolError::StatisticsBindingMismatch {
                        id: fact.id,
                        expected: binding,
                        actual: fact.binding,
                    });
                }
                validate_statistics_metric_coverage(fact.id, &need.metrics, &fact.metrics)?;
                if let DmlStatisticsEvidence::Available { evidence, .. } = &fact.evidence {
                    let evidence_metrics = evidence.metrics().keys().cloned().collect::<Vec<_>>();
                    validate_statistics_metric_coverage(fact.id, &need.metrics, &evidence_metrics)?;
                }
            }
        }
        (SqlNeedBatch::MaterializedViews(needs), SqlFactBatch::MaterializedViews(facts)) => {
            let expected = needs
                .iter()
                .map(|need| (need.id, need))
                .collect::<BTreeMap<_, _>>();
            for fact in facts.iter() {
                let need = expected[&fact.id];
                let requested = need
                    .referenced_relations
                    .iter()
                    .map(TableIdentity::fqn)
                    .collect::<HashSet<_>>();
                let MaterializedViewOutcome::Observed(definitions) = &fact.outcome else {
                    continue;
                };
                let mut mv_ids = HashSet::new();
                for definition in definitions {
                    let mv_id = definition.completion_mv_id();
                    if !mv_ids.insert(mv_id) {
                        return Err(
                            CompletionProtocolError::DuplicateMaterializedViewDefinition {
                                id: fact.id,
                                mv_id,
                            },
                        );
                    }
                    let bases = definition.completion_base_table_refs();
                    if bases.is_empty() {
                        return Err(CompletionProtocolError::MaterializedViewBaseMismatch {
                            id: fact.id,
                            base: Box::from("<empty>"),
                        });
                    }
                    for base in bases {
                        if base.is_empty() || !requested.contains(&base) {
                            return Err(CompletionProtocolError::MaterializedViewBaseMismatch {
                                id: fact.id,
                                base: base.clone().into_boxed_str(),
                            });
                        }
                    }
                }
            }
        }
        (SqlNeedBatch::ProviderReads(needs), SqlFactBatch::ProviderReads(facts)) => {
            let expected = needs
                .iter()
                .map(|need| (need.id, need))
                .collect::<BTreeMap<_, _>>();
            for fact in facts.iter() {
                let need = expected[&fact.id];
                if fact.binding != need.binding {
                    return Err(CompletionProtocolError::ProviderBindingMismatch { id: fact.id });
                }
                if fact.occurrence != need.occurrence {
                    return Err(CompletionProtocolError::ProviderOccurrenceMismatch {
                        id: fact.id,
                        expected: need.occurrence,
                        actual: fact.occurrence,
                    });
                }
                validate_provider_contract(need, &fact.contract)?;
            }
        }
        _ => unreachable!("fact batch kind was validated"),
    }
    Ok(())
}

fn validate_provider_contract(
    need: &ProviderReadNeed,
    contract: &ProviderReadStaticContract,
) -> Result<(), CompletionProtocolError> {
    if contract.sql_binding != need.binding {
        return Err(CompletionProtocolError::ProviderBindingMismatch { id: need.id });
    }
    if contract.request != ProviderReadRequestBinding::from_need(need) {
        return Err(CompletionProtocolError::ProviderRequestMismatch { id: need.id });
    }
    if contract.read.relation.kind() != need.relation.relation_kind() {
        return Err(CompletionProtocolError::ProviderRelationKindMismatch { id: need.id });
    }
    if contract.metadata_kind.is_some()
        != matches!(need.relation, ProviderReadRelationNeed::Metadata { .. })
    {
        return Err(CompletionProtocolError::ProviderRelationKindMismatch { id: need.id });
    }
    let read_binding = &contract.read.binding;
    if read_binding.descriptor().instance_id != *read_binding.catalog_handle().catalog_name()
        || !provider_payload_matches_binding(
            contract.read.relation.table(),
            read_binding,
            ConnectorCodecCategory::ReadTable,
        )
        || !provider_payload_matches_binding(
            contract.read.relation.view(),
            read_binding,
            ConnectorCodecCategory::ReadView,
        )
    {
        return Err(CompletionProtocolError::ProviderEnvelopeMismatch { id: need.id });
    }
    if contract.schema.len() != need.columns.len()
        || contract
            .schema
            .iter()
            .zip(need.columns.iter())
            .any(|(actual, requested)| {
                actual.request_ordinal != requested.ordinal
                    || actual.engine_type != requested.engine_type
            })
    {
        return Err(CompletionProtocolError::ProviderProjectionMismatch { id: need.id });
    }
    if contract.schema.iter().any(|field| {
        !provider_payload_matches_binding(
            &field.column.column_payload,
            read_binding,
            ConnectorCodecCategory::ReadColumn,
        )
    }) {
        return Err(CompletionProtocolError::ProviderEnvelopeMismatch { id: need.id });
    }
    let expected_predicates = need
        .predicates
        .iter()
        .map(ProviderReadPredicateNeed::occurrence)
        .collect::<BTreeSet<_>>();
    let actual_predicates = contract
        .predicates
        .iter()
        .map(ProviderReadPredicateFact::occurrence)
        .collect::<BTreeSet<_>>();
    if actual_predicates.len() != contract.predicates.len()
        || actual_predicates != expected_predicates
    {
        return Err(CompletionProtocolError::ProviderPredicateMismatch { id: need.id });
    }
    let limit_matches = matches!(
        (need.limit, contract.limit),
        (None, ProviderReadLimitFact::NotRequested)
    ) || matches!(
        (need.limit, contract.limit),
        (Some(expected), ProviderReadLimitFact::Exact(actual) | ProviderReadLimitFact::Residual(actual))
            if expected == actual
    );
    if !limit_matches {
        return Err(CompletionProtocolError::ProviderLimitMismatch { id: need.id });
    }
    validate_provider_properties(need, &contract.provided_properties)?;
    Ok(())
}

fn validate_provider_properties(
    need: &ProviderReadNeed,
    properties: &ProviderReadProperties,
) -> Result<(), CompletionProtocolError> {
    let validate_ordinal = |ordinal: u32| {
        let in_range = usize::try_from(ordinal)
            .ok()
            .is_some_and(|ordinal| ordinal < need.columns.len());
        if in_range {
            Ok(())
        } else {
            Err(CompletionProtocolError::ProviderPropertyOrdinalOutOfRange {
                id: need.id,
                ordinal,
                projection_len: need.columns.len(),
            })
        }
    };
    let validate_unique =
        |property: &'static str, ordinals: &[u32]| -> Result<(), CompletionProtocolError> {
            let mut seen = BTreeSet::new();
            for &ordinal in ordinals {
                validate_ordinal(ordinal)?;
                if !seen.insert(ordinal) {
                    return Err(CompletionProtocolError::ProviderPropertyDuplicateOrdinal {
                        id: need.id,
                        property,
                        ordinal,
                    });
                }
            }
            Ok(())
        };

    match &properties.distribution {
        ProviderReadDistribution::Unconstrained
        | ProviderReadDistribution::Singleton
        | ProviderReadDistribution::RoundRobin => {}
        ProviderReadDistribution::Hash { keys, scheme } => {
            if keys.is_empty() {
                return Err(CompletionProtocolError::ProviderDistributionHasNoKeys {
                    id: need.id,
                    distribution: "hash",
                });
            }
            validate_unique("hash distribution", keys)?;
            if scheme.admissible.min == 0
                || scheme.admissible.max < scheme.admissible.min
                || (scheme.admissible.requires_power_of_two
                    && scheme
                        .admissible
                        .min
                        .checked_next_power_of_two()
                        .is_none_or(|first| first > scheme.admissible.max))
            {
                return Err(CompletionProtocolError::ProviderPartitionSchemeInvalid {
                    id: need.id,
                    reason: "hash count domain is empty",
                });
            }
            if keys.iter().any(|ordinal| {
                let ordinal = usize::try_from(*ordinal)
                    .expect("validated provider ordinal always fits usize");
                !scheme
                    .algorithm
                    .supports_partition_key(need.columns[ordinal].engine_type.data_type())
            }) {
                return Err(CompletionProtocolError::ProviderPartitionSchemeInvalid {
                    id: need.id,
                    reason: "hash algorithm does not support a key type",
                });
            }
        }
        ProviderReadDistribution::BucketShuffle { keys, scheme } => {
            if keys.is_empty() {
                return Err(CompletionProtocolError::ProviderDistributionHasNoKeys {
                    id: need.id,
                    distribution: "bucket-shuffle",
                });
            }
            validate_unique("bucket-shuffle distribution", keys)?;
            if scheme.bucket_count == 0
                || scheme.ordinal_domain.first_ordinal != 0
                || scheme.ordinal_domain.ordinal_count != scheme.bucket_count
                || scheme.ordinal_domain.evidence_digest == [0; 32]
            {
                return Err(CompletionProtocolError::ProviderPartitionSchemeInvalid {
                    id: need.id,
                    reason: "bucket ordinal domain is incomplete",
                });
            }
            if keys.iter().any(|ordinal| {
                let ordinal = usize::try_from(*ordinal)
                    .expect("validated provider ordinal always fits usize");
                !scheme
                    .hash
                    .supports_partition_key(need.columns[ordinal].engine_type.data_type())
            }) {
                return Err(CompletionProtocolError::ProviderPartitionSchemeInvalid {
                    id: need.id,
                    reason: "bucket hash algorithm does not support a key type",
                });
            }
        }
    }

    let ordering = properties
        .ordering
        .iter()
        .map(|key| key.request_ordinal)
        .collect::<Vec<_>>();
    validate_unique("ordering", &ordering)
}

fn provider_payload_matches_binding(
    payload: &ConnectorEncodedPayload,
    binding: &ConnectorReadBinding,
    category: ConnectorCodecCategory,
) -> bool {
    payload.header().provider_id() == &binding.descriptor().provider_id
        && payload.header().catalog() == binding.catalog_handle()
        && payload.header().category() == category
}

fn table_identity_bytes(identity: &TableIdentity) -> Result<u64, CompletionProtocolError> {
    checked_sum([
        checked_size(identity.catalog.capacity()),
        checked_size(identity.namespace.capacity()),
        checked_size(identity.table.capacity()),
    ])
}

fn statistics_metric_bytes(metric: &StatisticsMetric) -> Result<u64, CompletionProtocolError> {
    use StatisticsMetric::{AverageSize, Maximum, Minimum, NullCount, RowCount, ThetaNdv};
    match metric {
        RowCount => Ok(0),
        NullCount { column }
        | Minimum { column }
        | Maximum { column }
        | AverageSize { column }
        | ThetaNdv { column } => checked_size(column.len()),
    }
}

fn provider_need_bytes(need: &ProviderReadNeed) -> Result<u64, CompletionProtocolError> {
    provider_request_dynamic_bytes(
        &need.relation,
        &need.columns,
        &need.filter,
        &need.predicates,
    )
}

fn provider_request_dynamic_bytes(
    relation: &ProviderReadRelationNeed,
    columns: &[ProviderReadColumnNeed],
    filter: &Constraint<u32>,
    predicates: &[ProviderReadPredicateNeed],
) -> Result<u64, CompletionProtocolError> {
    let columns = checked_sum([
        checked_mul(columns.len(), std::mem::size_of::<ProviderReadColumnNeed>()),
        checked_sum(columns.iter().map(|column| {
            checked_sum([
                checked_size(column.name.len()),
                data_type_dynamic_bytes(column.engine_type.data_type()),
            ])
        })),
    ])?;
    let predicates = checked_sum([
        checked_mul(
            predicates.len(),
            std::mem::size_of::<ProviderReadPredicateNeed>(),
        ),
        checked_sum(
            predicates
                .iter()
                .map(|predicate| provider_constraint_dynamic_bytes(&predicate.constraint)),
        ),
    ])?;
    checked_sum([
        provider_relation_dynamic_bytes(relation),
        Ok(columns),
        provider_constraint_dynamic_bytes(filter),
        Ok(predicates),
    ])
}

fn provider_relation_dynamic_bytes(
    relation: &ProviderReadRelationNeed,
) -> Result<u64, CompletionProtocolError> {
    let mut bytes = table_identity_bytes(relation.relation())?;
    let variant = match relation {
        ProviderReadRelationNeed::Data { .. }
        | ProviderReadRelationNeed::FrozenInputSet { .. }
        | ProviderReadRelationNeed::Metadata { .. }
        | ProviderReadRelationNeed::Delta { .. }
        | ProviderReadRelationNeed::PinnedFileSet { .. }
        | ProviderReadRelationNeed::TableExecute { .. } => 0,
        ProviderReadRelationNeed::MvTarget {
            target_table_uuid, ..
        } => checked_size(target_table_uuid.len())?,
    };
    bytes = checked_add(bytes, variant)?;
    Ok(bytes)
}

fn provider_constraint_dynamic_bytes(
    constraint: &Constraint<u32>,
) -> Result<u64, CompletionProtocolError> {
    let summary = match constraint.summary().domains() {
        None => 0,
        Some(domains) => checked_sum([
            checked_mul(
                domains.len(),
                std::mem::size_of::<(u32, novarocks_spi::connector::read_stack::Domain)>(),
            ),
            checked_sum(domains.values().map(provider_domain_dynamic_bytes)),
        ])?,
    };
    let assignments = checked_sum([
        checked_mul(
            constraint.assignments().len(),
            std::mem::size_of::<(std::sync::Arc<str>, u32)>(),
        ),
        checked_sum(
            constraint
                .assignments()
                .keys()
                .map(|name| checked_size(name.len())),
        ),
    ])?;
    checked_sum([
        Ok(summary),
        Ok(assignments),
        provider_expression_dynamic_bytes(constraint.expression()),
    ])
}

fn provider_domain_dynamic_bytes(
    domain: &novarocks_spi::connector::read_stack::Domain,
) -> Result<u64, CompletionProtocolError> {
    use novarocks_spi::connector::read_stack::Bound;
    checked_sum([
        checked_mul(
            domain.values().ranges().len(),
            std::mem::size_of::<novarocks_spi::connector::read_stack::Range>(),
        ),
        checked_sum(domain.values().ranges().iter().flat_map(|range| {
            [range.low(), range.high()]
                .into_iter()
                .map(|bound| match bound {
                    Bound::Unbounded => Ok(0),
                    Bound::Inclusive(value) | Bound::Exclusive(value) => {
                        checked_size(value.payload_bytes())
                    }
                })
        })),
    ])
}

fn provider_expression_node_count(expression: &ConnectorExpression) -> usize {
    match expression {
        ConnectorExpression::Constant { .. } | ConnectorExpression::Variable { .. } => 1,
        ConnectorExpression::FieldDereference { target, .. } => {
            1 + provider_expression_node_count(target)
        }
        ConnectorExpression::Call { arguments, .. } => {
            1 + arguments
                .iter()
                .map(provider_expression_node_count)
                .sum::<usize>()
        }
    }
}

fn provider_expression_dynamic_bytes(
    expression: &ConnectorExpression,
) -> Result<u64, CompletionProtocolError> {
    match expression {
        ConnectorExpression::Constant { value, .. } => value
            .as_ref()
            .map_or(Ok(0), |value| checked_size(value.payload_bytes())),
        ConnectorExpression::Variable { name, .. } => checked_size(name.len()),
        ConnectorExpression::FieldDereference { target, .. } => checked_add(
            std::mem::size_of::<ConnectorExpression>() as u64,
            provider_expression_dynamic_bytes(target)?,
        ),
        ConnectorExpression::Call {
            function,
            arguments,
            ..
        } => checked_sum([
            checked_size(function.as_str().len()),
            checked_mul(
                arguments.capacity(),
                std::mem::size_of::<ConnectorExpression>(),
            ),
            checked_sum(arguments.iter().map(provider_expression_dynamic_bytes)),
        ]),
    }
}

fn catalog_fact_bytes(fact: &CatalogRelationFact) -> Result<u64, CompletionProtocolError> {
    let relation = table_identity_bytes(&fact.relation)?;
    let outcome = match &fact.outcome {
        CatalogRelationOutcome::Missing { reason } => checked_size(reason.len())?,
        CatalogRelationOutcome::Resolved(table) => {
            let catalog_columns = checked_sum([
                checked_mul(
                    table.catalog.columns.capacity(),
                    std::mem::size_of::<novarocks_types::schema::ColumnDef>(),
                ),
                checked_sum(table.catalog.columns.iter().map(column_def_bytes)),
                checked_mul(
                    table.catalog.hidden_columns.capacity(),
                    std::mem::size_of::<novarocks_types::schema::ColumnDef>(),
                ),
                checked_sum(table.catalog.hidden_columns.iter().map(column_def_bytes)),
            ])?;
            let planner = table.planner.completion_retained_bytes().ok_or(
                CompletionProtocolError::ResourceCountOverflow(CompletionResource::ExchangeBytes),
            )?;
            checked_sum([
                table_identity_bytes(&table.catalog.identity),
                Ok(catalog_columns),
                Ok(planner),
            ])?
        }
    };
    checked_sum([
        Ok(std::mem::size_of::<CatalogRelationFact>() as u64),
        Ok(relation),
        Ok(outcome),
    ])
}

fn statistics_fact_bytes(fact: &StatisticsFact) -> Result<u64, CompletionProtocolError> {
    use novarocks_spi::connector::{
        StatisticsMetricSource, StatisticsMetricState, StatisticsMetricValue,
    };

    let coverage_bytes = checked_sum([
        checked_mul(fact.metrics.len(), std::mem::size_of::<StatisticsMetric>()),
        checked_sum(fact.metrics.iter().map(statistics_metric_bytes)),
    ])?;
    let dynamic = match &fact.evidence {
        DmlStatisticsEvidence::Missing { label, reason, .. } => checked_sum([
            checked_size(label.capacity()),
            checked_size(reason.capacity()),
        ])?,
        DmlStatisticsEvidence::Fatal { label, failure, .. } => {
            let failure = match failure {
                crate::planning::dml::DmlStatisticsFailure::CorruptEvidence(message) => {
                    checked_size(message.capacity())?
                }
                _ => 0,
            };
            checked_add(checked_size(label.capacity())?, failure)?
        }
        DmlStatisticsEvidence::Available {
            label,
            columns,
            evidence,
            ..
        } => {
            let column_bytes = checked_sum([
                checked_mul(
                    columns.capacity(),
                    std::mem::size_of::<novarocks_types::schema::ColumnDef>(),
                ),
                checked_sum(columns.iter().map(column_def_bytes)),
            ])?;
            let mut metric_bytes = checked_mul(
                evidence.metrics().len(),
                std::mem::size_of::<(StatisticsMetric, StatisticsMetricState)>(),
            )?;
            for (metric, state) in evidence.metrics() {
                metric_bytes = checked_add(metric_bytes, statistics_metric_bytes(metric)?)?;
                let state_bytes = match state {
                    StatisticsMetricState::Available(observation) => {
                        let value = match observation.value() {
                            StatisticsMetricValue::Bytes(bytes) => checked_size(bytes.len())?,
                            _ => std::mem::size_of::<StatisticsMetricValue>() as u64,
                        };
                        let source = match observation.source() {
                            StatisticsMetricSource::Provider(value) => checked_size(value.len())?,
                            _ => 0,
                        };
                        checked_sum([
                            Ok(value),
                            Ok(source),
                            checked_size(observation.basis_version().as_bytes().len()),
                        ])?
                    }
                    StatisticsMetricState::Missing(missing) => checked_size(missing.message.len())?,
                    StatisticsMetricState::Error(error) => checked_size(error.message.len())?,
                };
                metric_bytes = checked_add(metric_bytes, state_bytes)?;
            }
            checked_sum([
                checked_size(label.capacity()),
                Ok(column_bytes),
                checked_size(evidence.data_version().as_bytes().len()),
                checked_size(evidence.evidence_revision().as_bytes().len()),
                Ok(metric_bytes),
            ])?
        }
    };
    checked_sum([
        Ok(std::mem::size_of::<StatisticsFact>() as u64),
        Ok(coverage_bytes),
        Ok(dynamic),
    ])
}

fn materialized_view_facts_bytes(
    facts: &[MaterializedViewFact],
) -> Result<u64, SqlCompileProgressError> {
    let mut total = 0u64;
    for fact in facts {
        total = checked_add(total, std::mem::size_of::<MaterializedViewFact>() as u64)?;
        match &fact.outcome {
            MaterializedViewOutcome::Missing { reason } => {
                total = checked_add(total, checked_size(reason.len())?)?;
            }
            MaterializedViewOutcome::Observed(definitions) => {
                for definition in definitions {
                    total = checked_add(total, definition.completion_retained_bytes()?)?;
                }
            }
        }
    }
    Ok(total)
}

fn provider_fact_bytes(fact: &ProviderReadFact) -> Result<u64, CompletionProtocolError> {
    let contract = &fact.contract;
    let dynamic = {
        let schema = checked_sum([
            checked_mul(
                contract.schema.len(),
                std::mem::size_of::<ProviderReadColumnFact>(),
            ),
            checked_sum(contract.schema.iter().map(|field| {
                checked_sum([
                    provider_column_bytes(&field.column),
                    data_type_dynamic_bytes(field.engine_type.data_type()),
                ])
            })),
        ])?;
        let predicates = checked_mul(
            contract.predicates.len(),
            std::mem::size_of::<ProviderReadPredicateFact>(),
        )?;
        checked_sum([
            provider_request_dynamic_bytes(
                &contract.request.relation,
                &contract.request.projection,
                &contract.request.filter,
                &contract.request.predicates,
            ),
            provider_reference_bytes(&contract.read),
            checked_size(
                contract
                    .metadata_kind
                    .as_ref()
                    .map_or(0, |kind| kind.as_str().len()),
            ),
            Ok(schema),
            Ok(predicates),
            provider_properties_bytes(&contract.provided_properties),
            checked_size(contract.coverage_evidence.len()),
        ])?
    };
    checked_add(std::mem::size_of::<ProviderReadFact>() as u64, dynamic)
}

fn provider_reference_bytes(
    reference: &ProviderReadReference,
) -> Result<u64, CompletionProtocolError> {
    let descriptor = reference.binding.descriptor();
    let catalog = reference.binding.catalog_handle();
    let table = reference.relation.table();
    let view = reference.relation.view();
    checked_sum([
        checked_size(descriptor.provider_id.as_str().len()),
        checked_size(descriptor.instance_id.as_str().len()),
        checked_size(catalog.catalog_name().as_str().len()),
        checked_size(reference.input_version.as_bytes().len()),
        checked_size(table.header().provider_id().as_str().len()),
        checked_size(table.header().catalog().catalog_name().as_str().len()),
        checked_size(table.payload().len()),
        checked_size(view.header().provider_id().as_str().len()),
        checked_size(view.header().catalog().catalog_name().as_str().len()),
        checked_size(view.payload().len()),
    ])
}

fn provider_column_bytes(column: &ProviderColumnReference) -> Result<u64, CompletionProtocolError> {
    let payload = &column.column_payload;
    checked_sum([
        checked_size(payload.header().provider_id().as_str().len()),
        checked_size(payload.header().catalog().catalog_name().as_str().len()),
        checked_size(payload.payload().len()),
    ])
}

fn provider_properties_bytes(
    properties: &ProviderReadProperties,
) -> Result<u64, CompletionProtocolError> {
    let distribution_values = match &properties.distribution {
        ProviderReadDistribution::Hash { keys, .. }
        | ProviderReadDistribution::BucketShuffle { keys, .. } => keys.len(),
        _ => 0,
    };
    checked_sum([
        checked_mul(distribution_values, std::mem::size_of::<u32>()),
        checked_mul(
            properties.ordering.len(),
            std::mem::size_of::<ProviderReadOrderingKey>(),
        ),
    ])
}

fn data_type_dynamic_bytes(
    data_type: &arrow::datatypes::DataType,
) -> Result<u64, CompletionProtocolError> {
    use arrow::datatypes::DataType;
    match data_type {
        DataType::Timestamp(_, timezone) => checked_size(timezone.as_deref().map_or(0, str::len)),
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => field_bytes(field),
        DataType::Struct(fields) => checked_sum([
            checked_mul(
                fields.len(),
                std::mem::size_of::<arrow::datatypes::FieldRef>(),
            ),
            checked_sum(fields.iter().map(|field| field_bytes(field))),
        ]),
        DataType::Union(fields, _) => checked_sum([
            std::alloc::Layout::array::<(i8, arrow::datatypes::FieldRef)>(fields.len())
                .ok()
                .and_then(|payload| {
                    novarocks_type_contract::owned_resources::layout::arc_layout(payload).ok()
                })
                .ok_or(CompletionProtocolError::ResourceCountOverflow(
                    CompletionResource::ExchangeBytes,
                ))
                .and_then(|layout| checked_size(layout.size())),
            checked_sum(fields.iter().map(|(_, field)| field_bytes(field))),
        ]),
        DataType::Dictionary(key, value) => checked_sum([
            checked_mul(2, std::mem::size_of::<DataType>()),
            data_type_dynamic_bytes(key),
            data_type_dynamic_bytes(value),
        ]),
        DataType::RunEndEncoded(run_ends, values) => {
            checked_sum([field_bytes(run_ends), field_bytes(values)])
        }
        _ => Ok(0),
    }
}

fn field_bytes(field: &arrow::datatypes::Field) -> Result<u64, CompletionProtocolError> {
    let metadata =
        checked_sum([
            novarocks_type_contract::owned_resources::arrow_metadata::retained_backing_upper_bound(
                field.metadata().len(),
            )
            .map_err(|_| {
                CompletionProtocolError::ResourceCountOverflow(CompletionResource::ExchangeBytes)
            })
            .and_then(checked_size),
            checked_sum(field.metadata().iter().map(|(key, value)| {
                checked_sum([checked_size(key.len()), checked_size(value.len())])
            })),
        ])?;
    checked_sum([
        Ok(std::mem::size_of::<arrow::datatypes::Field>() as u64),
        checked_size(field.name().len()),
        data_type_dynamic_bytes(field.data_type()),
        Ok(metadata),
    ])
}

fn column_def_bytes(
    column: &novarocks_types::schema::ColumnDef,
) -> Result<u64, CompletionProtocolError> {
    checked_sum([
        checked_size(column.name.capacity()),
        data_type_dynamic_bytes(&column.data_type),
        column
            .write_default
            .as_ref()
            .map_or(Ok(0), column_default_bytes),
        column.logical_type.as_ref().map_or(Ok(0), sql_type_bytes),
    ])
}

fn column_default_bytes(
    value: &novarocks_types::schema::ColumnDefault,
) -> Result<u64, CompletionProtocolError> {
    use novarocks_types::schema::ColumnDefault;
    match value {
        ColumnDefault::String(value) => checked_size(value.capacity()),
        ColumnDefault::Binary(value) | ColumnDefault::Fixed { bytes: value, .. } => {
            checked_size(value.capacity())
        }
        ColumnDefault::Struct(fields) => checked_sum([
            checked_mul(
                fields.capacity(),
                std::mem::size_of::<(String, ColumnDefault)>(),
            ),
            checked_sum(fields.iter().map(|(name, value)| {
                checked_sum([checked_size(name.capacity()), column_default_bytes(value)])
            })),
        ]),
        ColumnDefault::Array(values) => checked_sum([
            checked_mul(values.capacity(), std::mem::size_of::<ColumnDefault>()),
            checked_sum(values.iter().map(column_default_bytes)),
        ]),
        ColumnDefault::Map(entries) => checked_sum([
            checked_mul(
                entries.capacity(),
                std::mem::size_of::<(ColumnDefault, ColumnDefault)>(),
            ),
            checked_sum(entries.iter().map(|(key, value)| {
                checked_sum([column_default_bytes(key), column_default_bytes(value)])
            })),
        ]),
        _ => Ok(0),
    }
}

fn sql_type_bytes(
    value: &novarocks_types::schema::SqlType,
) -> Result<u64, CompletionProtocolError> {
    use novarocks_types::schema::SqlType;
    match value {
        SqlType::Array(element) => checked_sum([
            Ok(std::mem::size_of::<SqlType>() as u64),
            sql_type_bytes(element),
        ]),
        SqlType::Map(key, value) => checked_sum([
            checked_mul(2, std::mem::size_of::<SqlType>()),
            sql_type_bytes(key),
            sql_type_bytes(value),
        ]),
        SqlType::Struct(fields) => checked_sum([
            checked_mul(fields.capacity(), std::mem::size_of::<(String, SqlType)>()),
            checked_sum(fields.iter().map(|(name, value)| {
                checked_sum([checked_size(name.capacity()), sql_type_bytes(value)])
            })),
        ]),
        _ => Ok(0),
    }
}

fn statistics_binding(evidence: &DmlStatisticsEvidence) -> SqlTableBindingId {
    match evidence {
        DmlStatisticsEvidence::Available { binding, .. }
        | DmlStatisticsEvidence::Missing { binding, .. }
        | DmlStatisticsEvidence::Fatal { binding, .. } => *binding,
    }
}

fn validate_statistics_metric_coverage(
    id: CompileNeedId,
    expected: &[StatisticsMetric],
    actual: &[StatisticsMetric],
) -> Result<(), CompletionProtocolError> {
    let mut actual_set = BTreeSet::new();
    for metric in actual {
        if !actual_set.insert(metric) {
            return Err(CompletionProtocolError::StatisticsMetricDuplicate {
                id,
                metric: metric.clone(),
            });
        }
    }
    let expected_set = expected.iter().collect::<BTreeSet<_>>();
    if let Some(metric) = expected_set.difference(&actual_set).next() {
        return Err(CompletionProtocolError::StatisticsMetricMissing {
            id,
            metric: (*metric).clone(),
        });
    }
    if let Some(metric) = actual_set.difference(&expected_set).next() {
        return Err(CompletionProtocolError::StatisticsMetricExtra {
            id,
            metric: (*metric).clone(),
        });
    }
    Ok(())
}

fn validate_relation_identity(identity: &TableIdentity) -> Result<(), CompletionProtocolError> {
    if identity.catalog.trim().is_empty()
        || identity.namespace.trim().is_empty()
        || identity.table.trim().is_empty()
    {
        return Err(CompletionProtocolError::InvalidRelationIdentity(Box::new(
            identity.clone(),
        )));
    }
    Ok(())
}

fn checked_sum(
    values: impl IntoIterator<Item = Result<u64, CompletionProtocolError>>,
) -> Result<u64, CompletionProtocolError> {
    values
        .into_iter()
        .try_fold(0, |total, value| checked_add(total, value?))
}

fn checked_add(left: u64, right: u64) -> Result<u64, CompletionProtocolError> {
    left.checked_add(right)
        .ok_or(CompletionProtocolError::ResourceCountOverflow(
            CompletionResource::ExchangeBytes,
        ))
}

fn checked_mul(count: usize, size: usize) -> Result<u64, CompletionProtocolError> {
    let count = u64::try_from(count).map_err(|_| {
        CompletionProtocolError::ResourceCountOverflow(CompletionResource::ExchangeBytes)
    })?;
    let size = u64::try_from(size).map_err(|_| {
        CompletionProtocolError::ResourceCountOverflow(CompletionResource::ExchangeBytes)
    })?;
    count
        .checked_mul(size)
        .ok_or(CompletionProtocolError::ResourceCountOverflow(
            CompletionResource::ExchangeBytes,
        ))
}

fn checked_size(bytes: usize) -> Result<u64, CompletionProtocolError> {
    u64::try_from(bytes).map_err(|_| {
        CompletionProtocolError::ResourceCountOverflow(CompletionResource::ExchangeBytes)
    })
}

fn check_budget<T>(
    resource: CompletionResource,
    limit: T,
    attempted: T,
) -> Result<(), CompletionProtocolError>
where
    T: Copy + Into<u64> + Ord,
{
    if attempted > limit {
        return Err(CompletionProtocolError::BudgetExceeded {
            resource,
            limit: limit.into(),
            attempted: attempted.into(),
        });
    }
    Ok(())
}

/// Needs a role adapter can be proved against.
///
/// A need is normally minted only by compilation, and that is what keeps a role
/// from answering questions the compiler would never ask. But a role's answer
/// has to be provable on its own - whether an absent relation becomes a fact or
/// an error is a decision with no plan in it - so these mint one directly.
/// They exist only under `test-support`, so no product build can reach them.
#[cfg(novarocks_dev_controls)]
pub mod fixtures {
    use super::{
        CatalogLookupTarget, CatalogRelationNeed, CompileNeedId, CompletionProtocolError,
        MaterializedViewNeed, ProviderReadColumnNeed, ProviderReadNeed, ProviderReadPredicateNeed,
        ProviderReadRelationNeed, SqlTableBindingId, StatisticsMetric, StatisticsNeed,
        TableIdentity,
    };

    pub fn catalog_relation_need(
        id: u32,
        relation: TableIdentity,
        target: CatalogLookupTarget,
    ) -> Result<CatalogRelationNeed, CompletionProtocolError> {
        CatalogRelationNeed::try_new(CompileNeedId::new(id), relation, target)
    }

    pub fn statistics_need(
        id: u32,
        binding: SqlTableBindingId,
        metrics: impl Into<Box<[StatisticsMetric]>>,
    ) -> Result<StatisticsNeed, CompletionProtocolError> {
        StatisticsNeed::try_new(CompileNeedId::new(id), binding, metrics)
    }

    pub fn materialized_view_need(
        id: u32,
        referenced_relations: impl Into<Box<[TableIdentity]>>,
    ) -> Result<MaterializedViewNeed, CompletionProtocolError> {
        MaterializedViewNeed::try_new(CompileNeedId::new(id), referenced_relations)
    }

    pub fn provider_read_need(
        id: u32,
        occurrence: novarocks_physical_plan::ProviderReadOccurrenceId,
        binding: SqlTableBindingId,
        relation: ProviderReadRelationNeed,
        columns: impl Into<Box<[ProviderReadColumnNeed]>>,
        predicates: impl Into<Box<[ProviderReadPredicateNeed]>>,
        limit: Option<u64>,
    ) -> Result<ProviderReadNeed, CompletionProtocolError> {
        ProviderReadNeed::try_new(
            CompileNeedId::new(id),
            occurrence,
            binding,
            relation,
            columns,
            predicates,
            limit,
        )
    }

    pub const fn provider_predicate_occurrence(value: u32) -> super::ProviderPredicateOccurrenceId {
        super::ProviderPredicateOccurrenceId::new(value)
    }

    pub fn provider_read_predicate_need(
        occurrence: super::ProviderPredicateOccurrenceId,
        constraint: super::Constraint<u32>,
    ) -> super::ProviderReadPredicateNeed {
        super::ProviderReadPredicateNeed::new(occurrence, constraint)
    }

    pub fn provider_read_column_need(
        ordinal: u32,
        name: &str,
        engine_type: novarocks_physical_plan::ValueType,
        connector_type: novarocks_spi::connector::read_stack::ConnectorValueType,
    ) -> Result<ProviderReadColumnNeed, CompletionProtocolError> {
        ProviderReadColumnNeed::try_new(ordinal, name, engine_type, connector_type)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arrow::datatypes::DataType;
    use bytes::Bytes;
    use novarocks_physical_plan::{
        Distribution, ExactInputVersion, ExprKind, FragmentBuilder, FragmentId, FragmentSink,
        LiteralValue, NodeKind, OutputPort, PhysicalNode, PhysicalProperties, PipelineDopDomain,
        ResultField, ResultPort, RowMultiplicity, ValueOrigin,
    };
    use novarocks_spi::connector::read_stack::{
        ConnectorReadBinding, ConnectorValue, Domain, TupleDomain,
    };
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
        ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceDescriptor,
        ConnectorInstanceId, ConnectorProviderId, ConnectorReadRelationPayload,
        StatisticsDataVersion, StatisticsEvidence, StatisticsEvidenceRevision,
        StatisticsMetricState, StatisticsMissing, StatisticsMissingKind, StatisticsRowCoverage,
    };

    use super::*;
    use crate::planner::table::{
        ScanSource, SqlMvTargetStatePartitionConstraint, SqlMvTargetStateRowFilter,
        SqlMvTargetStateScan, SqlScanKind, SqlScanSource, SqlTableIdentity, TableDef,
    };

    fn minimal_plan(version: PlanVersionId) -> PlanBuilder {
        let mut fragment = FragmentBuilder::new(FragmentId::new(1));
        let node = fragment.reserve_node_id().unwrap();
        let ty = ValueType::new(DataType::Int64, false);
        let expression = fragment
            .add_expression(node, ty.clone(), ExprKind::Literal(LiteralValue::Int64(1)))
            .unwrap();
        let output = fragment
            .add_value(
                ty.clone(),
                ValueOrigin::NodeOutput {
                    node,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        fragment
            .insert_node_unchecked(PhysicalNode {
                id: node,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: PhysicalProperties {
                    distribution: Distribution::Singleton,
                    row_multiplicity: RowMultiplicity::SingleCopy,
                    ordering: Box::default(),
                },
                output: OutputPort {
                    node,
                    columns: Box::from([output]),
                },
                kind: NodeKind::Values {
                    rows: Box::from([Box::from([expression])]),
                },
            })
            .unwrap();
        let fragment = fragment
            .finish_definition(
                node,
                FragmentSink::Result,
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
                novarocks_physical_plan::StagePlacementPolicy::Singleton,
            )
            .unwrap();
        let mut plan = PlanBuilder::new(version);
        plan.add_fragment(fragment).unwrap();
        plan.set_result_port(ResultPort {
            scalar_schema: None,
            fragment: FragmentId::new(1),
            output: OutputPort {
                node,
                columns: Box::from([output]),
            },
            fields: Box::from([ResultField {
                domain: novarocks_physical_plan::ResultValueDomain::Plain,
                name: "one".into(),
                alias: None,
                value: output,
                ty,
            }]),
        })
        .unwrap();
        plan
    }

    fn catalog_need(id: u32, target: CatalogLookupTarget) -> CatalogRelationNeed {
        CatalogRelationNeed::try_new(
            CompileNeedId::new(id),
            TableIdentity::new("iceberg", "db", "orders"),
            target,
        )
        .unwrap()
    }

    fn catalog_request(need: CatalogRelationNeed, limits: CompletionLimits) -> SqlCompileRequest {
        SqlCompileRequest::pending(
            CompilerStep::need(
                SqlNeedBatch::CatalogRelations(Box::from([need])),
                CompilerContinuation::test(NeedKind::CatalogRelation),
            ),
            limits,
        )
    }

    fn resolved_catalog_table(kind: SqlScanKind) -> ResolvedAnalyzerTable {
        ResolvedAnalyzerTable::from_planner(
            Some("iceberg"),
            "db",
            TableDef {
                name: "orders".to_string(),
                columns: Vec::new(),
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::Sql(SqlScanSource::new(
                    SqlTableBindingId::new_for_test(1),
                    SqlTableIdentity::try_new(
                        "iceberg".to_string(),
                        "db".to_string(),
                        "orders".to_string(),
                    )
                    .unwrap(),
                    kind,
                )),
            },
        )
    }

    fn incomplete(progress: SqlCompileProgress) -> SqlCompilation {
        match progress {
            SqlCompileProgress::Incomplete(compilation) => compilation,
            SqlCompileProgress::Complete(_) => panic!("expected incomplete compilation"),
        }
    }

    fn test_query(sql: &str) -> novarocks_parser::ast::Query {
        let statements = novarocks_parser::parse(sql).expect("parse query");
        let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("fixture must be a query");
        };
        query.clone()
    }

    fn mv_definition(mv_id: i64, base: &str) -> SqlMvRewriteDefinitionFacts {
        let parts = base.split('.').collect::<Vec<_>>();
        SqlMvRewriteDefinitionFacts::try_new(
            mv_id,
            [11; 32],
            test_query("select 1"),
            crate::compiler::SqlMvDefinitionResolutionContext::try_new(
                "iceberg".to_string(),
                "db".to_string(),
            )
            .unwrap(),
            "iceberg".to_string(),
            Some(TableIdentity::new("iceberg", "db", &format!("mv_{mv_id}"))),
            vec![
                crate::compiler::SqlMvRewriteSourceOccurrenceFacts::try_new(
                    crate::compiler::SqlMvRelationOccurrenceId::new(7),
                    TableIdentity::new(parts[0], parts[1], parts[2]),
                    parts[2].to_string(),
                    None,
                    crate::compiler::SqlMvRewriteBaseTableFacts::unavailable(
                        "not published".to_string(),
                    ),
                )
                .unwrap(),
            ],
        )
        .expect("valid MV definition fixture")
    }

    fn connector_binding() -> ConnectorReadBinding {
        let provider_id = ConnectorProviderId::parse("iceberg").unwrap();
        let instance_id = ConnectorInstanceId::parse("lakehouse").unwrap();
        ConnectorReadBinding::new(
            ConnectorInstanceDescriptor {
                provider_id,
                instance_id: instance_id.clone(),
            },
            CatalogHandle::new(instance_id, CatalogVersion::from_bytes([3; 32])),
        )
    }

    fn encoded(
        binding: &ConnectorReadBinding,
        category: ConnectorCodecCategory,
        payload: &[u8],
    ) -> ConnectorEncodedPayload {
        ConnectorEncodedPayload::new(
            ConnectorEnvelopeHeader::new(
                binding.descriptor().provider_id.clone(),
                binding.catalog_handle().clone(),
                category,
                ConnectorCodecRevision::try_new(1).unwrap(),
            ),
            payload.to_vec().into(),
        )
    }

    fn provider_need(
        id: u32,
        ty: DataType,
        predicates: &[u32],
        limit: Option<u64>,
    ) -> ProviderReadNeed {
        provider_need_named(id, "order_key", ty, predicates, limit)
    }

    fn predicate_constraint(value: i64) -> Constraint<u32> {
        Constraint::of_summary(
            TupleDomain::with_column_domains(BTreeMap::from([(
                0,
                Domain::single_value(ConnectorValue::BigInt(value)).unwrap(),
            )]))
            .unwrap(),
        )
    }

    fn provider_need_named(
        id: u32,
        column_name: &str,
        ty: DataType,
        predicates: &[u32],
        limit: Option<u64>,
    ) -> ProviderReadNeed {
        let predicate_needs = predicates
            .iter()
            .map(|occurrence| {
                ProviderReadPredicateNeed::new(
                    ProviderPredicateOccurrenceId::new(*occurrence),
                    predicate_constraint(i64::from(*occurrence)),
                )
            })
            .collect::<Vec<_>>();
        let connector_type = provider_connector_type_for_engine(&ty).unwrap();
        ProviderReadNeed::try_new(
            CompileNeedId::new(id),
            ProviderReadOccurrenceId::new(id),
            SqlTableBindingId::new_for_test(41),
            ProviderReadRelationNeed::Data {
                relation: TableIdentity::new("iceberg", "db", "orders"),
                version: ProviderReadVersionNeed::Current,
            },
            [ProviderReadColumnNeed::try_new(
                0,
                column_name,
                ValueType::new(ty, false),
                connector_type,
            )
            .unwrap()],
            predicate_needs,
            limit,
        )
        .unwrap()
    }

    fn provider_contract(
        need: &ProviderReadNeed,
        payload_sentinel: &[u8],
    ) -> ProviderReadStaticContract {
        let binding = connector_binding();
        ProviderReadStaticContract {
            sql_binding: need.binding(),
            request: ProviderReadRequestBinding::from_need(need),
            read: ProviderReadReference {
                binding: binding.clone(),
                input_version: ExactInputVersion::try_new([9]).unwrap(),
                relation: ConnectorReadRelationPayload::new(
                    need.relation().relation_kind(),
                    encoded(
                        &binding,
                        ConnectorCodecCategory::ReadTable,
                        payload_sentinel,
                    ),
                    encoded(&binding, ConnectorCodecCategory::ReadView, payload_sentinel),
                ),
            },
            metadata_kind: matches!(need.relation(), ProviderReadRelationNeed::Metadata { .. })
                .then(|| {
                    novarocks_spi::connector::read_stack::ConnectorReadMetadataKind::try_new(
                        "$fixture-metadata",
                    )
                    .expect("fixture metadata kind")
                }),
            selection_digest: [8; 32],
            schema: Box::from([ProviderReadColumnFact::new(
                0,
                ProviderColumnReference {
                    column_payload: encoded(
                        &binding,
                        ConnectorCodecCategory::ReadColumn,
                        payload_sentinel,
                    ),
                },
                need.columns()[0].engine_type().clone(),
            )]),
            predicates: need
                .predicates()
                .iter()
                .map(|predicate| {
                    ProviderReadPredicateFact::new(
                        predicate.occurrence(),
                        PredicateGuaranteeKind::Exact,
                    )
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            limit: match need.limit() {
                Some(limit) => ProviderReadLimitFact::Exact(limit),
                None => ProviderReadLimitFact::NotRequested,
            },
            provided_properties: ProviderReadProperties::unconstrained(),
            coverage_evidence: Box::default(),
        }
    }

    fn hash_scheme(seed: u8) -> ProviderHashPartitionScheme {
        ProviderHashPartitionScheme {
            space: ProviderPartitionSpaceToken::from_bytes([seed; 32]),
            count: ProviderPartitionCountToken::from_bytes([seed.wrapping_add(1); 32]),
            admissible: ProviderPartitionCountDomain {
                min: 1,
                max: 8,
                requires_power_of_two: true,
            },
            algorithm: PartitionHashAlgorithm::NativeExchangeV1,
        }
    }

    #[test]
    fn exchange_field_metadata_keeps_text_length_not_spare_capacity() {
        use arrow::datatypes::{DataType, Field};
        let field = |key_capacity, value_capacity| {
            let mut key = String::with_capacity(key_capacity);
            key.push('k');
            let mut value = String::with_capacity(value_capacity);
            value.push('v');
            Field::new("x", DataType::Int32, false)
                .with_metadata(std::collections::HashMap::from([(key, value)]))
        };
        assert_eq!(
            field_bytes(&field(1, 1)).unwrap(),
            field_bytes(&field(64, 96)).unwrap()
        );
        assert_eq!(
            field_bytes(&Field::new("x", DataType::Int32, false)).unwrap(),
            (std::mem::size_of::<Field>() + 1) as u64
        );
    }

    #[test]
    fn exchange_union_pair_padding_and_empty_arc_are_counted() {
        use arrow::datatypes::{DataType, Field, UnionFields, UnionMode};
        let empty = DataType::Union(UnionFields::empty(), UnionMode::Sparse);
        assert_eq!(data_type_dynamic_bytes(&empty).unwrap(), 16);
        let fields: UnionFields = [(
            3,
            std::sync::Arc::new(Field::new("x", DataType::Int32, false)),
        )]
        .into_iter()
        .collect();
        assert_eq!(
            data_type_dynamic_bytes(&DataType::Union(fields, UnionMode::Sparse)).unwrap(),
            (32 + std::mem::size_of::<Field>() + 1) as u64
        );
    }

    #[test]
    fn no_io_plan_completes_during_start() {
        let version = PlanVersionId::try_new([7; 16]).unwrap();
        let request = SqlCompileRequest::ready(
            version,
            minimal_plan(version),
            SqlDisplayIntent::Explain {
                level: ExplainLevel::Verbose,
                analyze: false,
            },
            [SqlDisplayAnnotation::try_new("optimizer", "stable").unwrap()],
            DEFAULT_COMPLETION_LIMITS,
        );
        let completed =
            SqlCompiler::start(request, &crate::compiler::SqlCompileControl::unbounded())
                .unwrap()
                .into_complete()
                .unwrap();
        assert_eq!(completed.plan().version(), version);
        assert_eq!(completed.display_annotations()[0].key(), "optimizer");
        assert!(matches!(
            &completed.plan,
            CompletedPlanSource::StructurallyUntrusted(_)
        ));
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| completed.into_plan()))
                .is_err(),
            "a raw structural test builder cannot publish an authored source owner"
        );
    }

    #[test]
    fn incomplete_exposes_needs_but_not_a_plan() {
        let need = catalog_need(
            1,
            CatalogLookupTarget::Table {
                mode: TableLookupMode::ExplainStats,
            },
        );
        let progress = SqlCompiler::start(
            catalog_request(need, DEFAULT_COMPLETION_LIMITS),
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();
        let SqlCompileProgress::Incomplete(compilation) = progress else {
            panic!("expected catalog need");
        };
        assert!(matches!(
            compilation.needs(),
            SqlNeedBatch::CatalogRelations(needs) if needs.len() == 1
        ));
        assert!(matches!(
            SqlCompileProgress::Incomplete(compilation).into_complete(),
            Err(SqlCompileProgressError::Protocol(
                CompletionProtocolError::PlanIsIncomplete
            ))
        ));
    }

    #[test]
    fn ordinary_and_metadata_catalog_lookups_are_distinct_typed_requests() {
        let table = catalog_need(
            1,
            CatalogLookupTarget::Table {
                mode: TableLookupMode::SchemaOnly,
            },
        );
        let metadata = catalog_need(
            2,
            CatalogLookupTarget::IcebergMetadata {
                kind: MetadataTableKind::Entries,
            },
        );
        assert_ne!(table.target(), metadata.target());
        assert!(matches!(
            metadata.target(),
            CatalogLookupTarget::IcebergMetadata {
                kind: MetadataTableKind::Entries
            }
        ));

        let mut fact = CatalogRelationFact::missing(&metadata, "not found").unwrap();
        fact.target = table.target();
        assert!(matches!(
            validate_fact_semantics(
                &SqlNeedBatch::CatalogRelations(Box::from([metadata.clone()])),
                &SqlFactBatch::CatalogRelations(Box::from([fact])),
            ),
            Err(CompletionProtocolError::CatalogLookupTargetMismatch { .. })
        ));

        let ordinary_fact = resolved_catalog_table(SqlScanKind::ConnectorRead);
        assert!(CatalogRelationFact::resolved(&table, ordinary_fact).is_ok());

        let metadata_fact = resolved_catalog_table(SqlScanKind::Metadata {
            kind: MetadataTableKind::Entries,
            version: crate::planner::table::SqlTableVersionSelector::Current,
        });
        assert!(CatalogRelationFact::resolved(&metadata, metadata_fact).is_ok());

        let wrong_metadata_kind = resolved_catalog_table(SqlScanKind::Metadata {
            kind: MetadataTableKind::Files,
            version: crate::planner::table::SqlTableVersionSelector::Current,
        });
        assert!(matches!(
            CatalogRelationFact::resolved(&metadata, wrong_metadata_kind),
            Err(CompletionProtocolError::CatalogLookupTargetMismatch { .. })
        ));

        let metadata_for_table = resolved_catalog_table(SqlScanKind::Metadata {
            kind: MetadataTableKind::Entries,
            version: crate::planner::table::SqlTableVersionSelector::Current,
        });
        assert!(matches!(
            CatalogRelationFact::resolved(&table, metadata_for_table),
            Err(CompletionProtocolError::CatalogLookupTargetMismatch { .. })
        ));

        let ordinary_for_metadata = resolved_catalog_table(SqlScanKind::ConnectorRead);
        assert!(matches!(
            CatalogRelationFact::resolved(&metadata, ordinary_for_metadata),
            Err(CompletionProtocolError::CatalogLookupTargetMismatch { .. })
        ));

        let mut mismatched_source = resolved_catalog_table(SqlScanKind::ConnectorRead);
        let ScanSource::Sql(source) = &mut mismatched_source.planner.source;
        source.table.table = "other_orders".to_string();
        assert!(matches!(
            CatalogRelationFact::resolved(&table, mismatched_source),
            Err(CompletionProtocolError::CatalogLookupTargetMismatch { .. })
        ));
    }

    #[test]
    fn missing_duplicate_and_wrong_kind_facts_are_rejected_before_resume() {
        let need = catalog_need(
            1,
            CatalogLookupTarget::Table {
                mode: TableLookupMode::SchemaOnly,
            },
        );
        let compilation = incomplete(
            SqlCompiler::start(
                catalog_request(need.clone(), DEFAULT_COMPLETION_LIMITS),
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap(),
        );
        assert!(matches!(
            SqlCompiler::finish(
                compilation,
                SqlFactBatch::CatalogRelations(Box::default()),
                &crate::compiler::SqlCompileControl::unbounded(),
            ),
            Err(SqlCompileProgressError::Protocol(
                CompletionProtocolError::MissingFact(_)
            ))
        ));

        let compilation = incomplete(
            SqlCompiler::start(
                catalog_request(need.clone(), DEFAULT_COMPLETION_LIMITS),
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap(),
        );
        let fact = || CatalogRelationFact::missing(&need, "missing").unwrap();
        assert!(matches!(
            SqlCompiler::finish(
                compilation,
                SqlFactBatch::CatalogRelations(Box::from([fact(), fact()])),
                &crate::compiler::SqlCompileControl::unbounded(),
            ),
            Err(SqlCompileProgressError::Protocol(
                CompletionProtocolError::DuplicateFact(_)
            ))
        ));

        let binding = SqlTableBindingId::new_for_test(1);
        let statistics_need =
            StatisticsNeed::try_new(CompileNeedId::new(1), binding, [StatisticsMetric::RowCount])
                .unwrap();
        let statistics_fact = StatisticsFact::try_new(
            &statistics_need,
            statistics_need.metrics().to_vec(),
            DmlStatisticsEvidence::Missing {
                binding,
                label: "orders".into(),
                reason: "not collected".into(),
            },
        )
        .unwrap();
        let compilation = incomplete(
            SqlCompiler::start(
                catalog_request(need, DEFAULT_COMPLETION_LIMITS),
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap(),
        );
        assert!(matches!(
            SqlCompiler::finish(
                compilation,
                SqlFactBatch::Statistics(Box::from([statistics_fact])),
                &crate::compiler::SqlCompileControl::unbounded(),
            ),
            Err(SqlCompileProgressError::Protocol(
                CompletionProtocolError::FactBatchKindMismatch { .. }
            ))
        ));
    }

    #[test]
    fn statistics_fact_requires_exact_metric_coverage_independent_of_order() {
        let binding = SqlTableBindingId::new_for_test(7);
        let null_count = StatisticsMetric::NullCount {
            column: Arc::from("order_key"),
        };
        let need = StatisticsNeed::try_new(
            CompileNeedId::new(9),
            binding,
            [StatisticsMetric::RowCount, null_count.clone()],
        )
        .unwrap();
        let missing = || DmlStatisticsEvidence::Missing {
            binding,
            label: "orders".into(),
            reason: "not collected".into(),
        };

        let fact = StatisticsFact::try_new(
            &need,
            [null_count.clone(), StatisticsMetric::RowCount],
            missing(),
        )
        .expect("metric coverage is set-based");
        assert_eq!(
            fact.metrics(),
            &[null_count.clone(), StatisticsMetric::RowCount]
        );

        assert!(matches!(
            StatisticsFact::try_new(&need, [StatisticsMetric::RowCount], missing()),
            Err(CompletionProtocolError::StatisticsMetricMissing { metric, .. })
                if metric == null_count
        ));
        let maximum = StatisticsMetric::Maximum {
            column: Arc::from("order_key"),
        };
        assert!(matches!(
            StatisticsFact::try_new(
                &need,
                [StatisticsMetric::RowCount, null_count.clone(), maximum.clone()],
                missing(),
            ),
            Err(CompletionProtocolError::StatisticsMetricExtra { metric, .. })
                if metric == maximum
        ));
        assert!(matches!(
            StatisticsFact::try_new(
                &need,
                [
                    StatisticsMetric::RowCount,
                    null_count.clone(),
                    StatisticsMetric::RowCount,
                ],
                missing(),
            ),
            Err(CompletionProtocolError::StatisticsMetricDuplicate { metric, .. })
                if metric == StatisticsMetric::RowCount
        ));
        assert!(matches!(
            StatisticsFact::try_new(
                &need,
                [StatisticsMetric::RowCount, null_count],
                DmlStatisticsEvidence::Missing {
                    binding: SqlTableBindingId::new_for_test(8),
                    label: "orders".into(),
                    reason: "not collected".into(),
                },
            ),
            Err(CompletionProtocolError::StatisticsBindingMismatch { .. })
        ));
    }

    #[test]
    fn statistics_fact_rejects_available_evidence_metric_drift() {
        let binding = SqlTableBindingId::new_for_test(7);
        let null_count = StatisticsMetric::NullCount {
            column: Arc::from("order_key"),
        };
        let need = StatisticsNeed::try_new(
            CompileNeedId::new(9),
            binding,
            [StatisticsMetric::RowCount, null_count.clone()],
        )
        .unwrap();
        let evidence = StatisticsEvidence::try_new(
            StatisticsDataVersion::try_new(Bytes::from_static(b"data-v1")).unwrap(),
            StatisticsEvidenceRevision::try_new(Bytes::from_static(b"evidence-v1")).unwrap(),
            StatisticsRowCoverage::AllVisibleRows,
            BTreeMap::from([(
                StatisticsMetric::RowCount,
                StatisticsMetricState::Missing(StatisticsMissing {
                    kind: StatisticsMissingKind::NotCollected,
                    message: Arc::from("not collected"),
                }),
            )]),
        )
        .unwrap();

        assert!(matches!(
            StatisticsFact::try_new(
                &need,
                [StatisticsMetric::RowCount, null_count.clone()],
                DmlStatisticsEvidence::Available {
                    binding,
                    label: "orders".into(),
                    columns: Vec::new(),
                    evidence,
                },
            ),
            Err(CompletionProtocolError::StatisticsMetricMissing { metric, .. })
                if metric == null_count
        ));

        let maximum = StatisticsMetric::Maximum {
            column: Arc::from("order_key"),
        };
        let missing_state = || {
            StatisticsMetricState::Missing(StatisticsMissing {
                kind: StatisticsMissingKind::NotCollected,
                message: Arc::from("not collected"),
            })
        };
        let evidence = StatisticsEvidence::try_new(
            StatisticsDataVersion::try_new(Bytes::from_static(b"data-v1")).unwrap(),
            StatisticsEvidenceRevision::try_new(Bytes::from_static(b"evidence-v2")).unwrap(),
            StatisticsRowCoverage::AllVisibleRows,
            BTreeMap::from([
                (StatisticsMetric::RowCount, missing_state()),
                (null_count.clone(), missing_state()),
                (maximum.clone(), missing_state()),
            ]),
        )
        .unwrap();
        assert!(matches!(
            StatisticsFact::try_new(
                &need,
                [StatisticsMetric::RowCount, null_count],
                DmlStatisticsEvidence::Available {
                    binding,
                    label: "orders".into(),
                    columns: Vec::new(),
                    evidence,
                },
            ),
            Err(CompletionProtocolError::StatisticsMetricExtra { metric, .. })
                if metric == maximum
        ));
    }

    #[test]
    fn nested_fact_bytes_are_rejected_before_the_resume_state_can_retain_them() {
        let need = catalog_need(
            1,
            CatalogLookupTarget::Table {
                mode: TableLookupMode::SchemaOnly,
            },
        );
        let limits = CompletionLimits::try_new(1, 1, 256).unwrap();
        let compilation = incomplete(
            SqlCompiler::start(
                catalog_request(need.clone(), limits),
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap(),
        );
        let fact = CatalogRelationFact::missing(&need, "x".repeat(1024)).unwrap();
        assert!(matches!(
            SqlCompiler::finish(
                compilation,
                SqlFactBatch::CatalogRelations(Box::from([fact])),
                &crate::compiler::SqlCompileControl::unbounded(),
            ),
            Err(SqlCompileProgressError::Protocol(
                CompletionProtocolError::BudgetExceeded {
                    resource: CompletionResource::ExchangeBytes,
                    ..
                }
            ))
        ));
    }

    #[test]
    fn oversized_catalog_target_state_is_rejected_before_resume() {
        let need = catalog_need(
            1,
            CatalogLookupTarget::Table {
                mode: TableLookupMode::SchemaOnly,
            },
        );
        let limits = CompletionLimits::try_new(1, 1, 64 * 1024).unwrap();
        let compilation = incomplete(
            SqlCompiler::start(
                catalog_request(need.clone(), limits),
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap(),
        );
        let planner = TableDef {
            name: "orders".to_string(),
            columns: Vec::new(),
            iceberg_row_lineage_metadata_columns: Vec::new(),
            source: ScanSource::Sql(SqlScanSource::new(
                SqlTableBindingId::new_for_test(1),
                SqlTableIdentity::try_new(
                    "iceberg".to_string(),
                    "db".to_string(),
                    "orders".to_string(),
                )
                .unwrap(),
                SqlScanKind::MvTargetState {
                    facts: SqlMvTargetStateScan {
                        target_table_uuid: "x".repeat(1024 * 1024),
                        target_snapshot_id: Some(7),
                        aggregate_state_layout_version: 1,
                        columns: Vec::new(),
                        group_key_names: Vec::new(),
                        aggregate_state_names: Vec::new(),
                        physical_column_names: Vec::new(),
                        row_id_column_name: "_row_id".to_string(),
                        row_filter: SqlMvTargetStateRowFilter::DeltaInputRowIds {
                            row_id_column_name: "_row_id".to_string(),
                            branch_scope: None,
                        },
                        partition_constraint: SqlMvTargetStatePartitionConstraint::Unpartitioned,
                    },
                },
            )),
        };
        let resolved = ResolvedAnalyzerTable::from_planner(Some("iceberg"), "db", planner);
        let fact = CatalogRelationFact::resolved(&need, resolved).unwrap();

        assert!(matches!(
            SqlCompiler::finish(
                compilation,
                SqlFactBatch::CatalogRelations(Box::from([fact])),
                &crate::compiler::SqlCompileControl::unbounded(),
            ),
            Err(SqlCompileProgressError::Protocol(
                CompletionProtocolError::BudgetExceeded {
                    resource: CompletionResource::ExchangeBytes,
                    ..
                }
            ))
        ));
    }

    #[test]
    fn structural_accounting_detects_overflow() {
        assert!(matches!(
            checked_add(u64::MAX, 1),
            Err(CompletionProtocolError::ResourceCountOverflow(
                CompletionResource::ExchangeBytes
            ))
        ));
    }

    #[test]
    fn metadata_kind_is_an_exact_provider_fact_and_only_metadata_reads_carry_it() {
        use novarocks_spi::connector::read_stack::ConnectorReadMetadataKind;
        let data = provider_need(1, DataType::Int64, &[], None);
        let mut wrong_data = provider_contract(&data, b"private");
        wrong_data.metadata_kind = Some(ConnectorReadMetadataKind::try_new("$snapshots").unwrap());
        assert!(matches!(
            ProviderReadFact::negotiated(&data, wrong_data),
            Err(CompletionProtocolError::ProviderRelationKindMismatch { .. })
        ));
        let mut metadata = data.clone();
        metadata.relation = ProviderReadRelationNeed::Metadata {
            relation: TableIdentity::new("iceberg", "db", "orders"),
            kind: MetadataTableKind::Snapshots,
            version: ProviderReadVersionNeed::Current,
        };
        let mut exact = provider_contract(&metadata, b"private");
        exact.metadata_kind = Some(ConnectorReadMetadataKind::try_new("$snapshots").unwrap());
        let fact = ProviderReadFact::negotiated(&metadata, exact.clone()).unwrap();
        assert_eq!(
            fact.contract().metadata_kind.as_ref().unwrap().as_str(),
            "$snapshots"
        );
        let full_bytes = provider_fact_bytes(&fact).unwrap();
        exact.metadata_kind = None;
        assert!(matches!(
            ProviderReadFact::negotiated(&metadata, exact),
            Err(CompletionProtocolError::ProviderRelationKindMismatch { .. })
        ));
        let mut longer = fact.clone();
        longer.contract.metadata_kind =
            Some(ConnectorReadMetadataKind::try_new("$snapshots-extra").unwrap());
        assert_eq!(provider_fact_bytes(&longer).unwrap() - full_bytes, 6);
    }

    #[test]
    fn stale_provider_contract_rejects_changed_request_semantics() {
        let original_need = provider_need(1, DataType::Int64, &[7], Some(10));
        let stale = provider_contract(&original_need, b"provider-private");
        let mut changed_predicate = provider_need(4, DataType::Int64, &[7], Some(10));
        changed_predicate.predicates[0].constraint = predicate_constraint(99);
        changed_predicate.filter = predicate_constraint(99);
        let mut changed_version = provider_need(6, DataType::Int64, &[7], Some(10));
        changed_version.relation = ProviderReadRelationNeed::Data {
            relation: TableIdentity::new("iceberg", "db", "orders"),
            version: ProviderReadVersionNeed::Snapshot(23),
        };
        let mut changed_window = provider_need(7, DataType::Int64, &[7], Some(10));
        changed_window.relation = ProviderReadRelationNeed::Delta {
            relation: TableIdentity::new("iceberg", "db", "orders"),
            from_snapshot_id: 11,
            to_snapshot_id: 19,
        };
        for changed_need in [
            provider_need(2, DataType::Int32, &[7], Some(10)),
            provider_need_named(3, "customer_key", DataType::Int64, &[7], Some(10)),
            changed_predicate,
            provider_need(5, DataType::Int64, &[7], Some(11)),
            changed_version,
            changed_window,
        ] {
            assert!(matches!(
                ProviderReadFact::negotiated(&changed_need, stale.clone()),
                Err(CompletionProtocolError::ProviderRequestMismatch { id })
                if id == changed_need.id()
            ));
        }

        let mut delta = provider_need(8, DataType::Int64, &[7], Some(10));
        delta.relation = ProviderReadRelationNeed::Delta {
            relation: TableIdentity::new("iceberg", "db", "orders"),
            from_snapshot_id: 11,
            to_snapshot_id: 19,
        };
        let delta_contract = provider_contract(&delta, b"provider-private");
        let mut changed_endpoint = delta.clone();
        changed_endpoint.relation = ProviderReadRelationNeed::Delta {
            relation: TableIdentity::new("iceberg", "db", "orders"),
            from_snapshot_id: 11,
            to_snapshot_id: 20,
        };
        assert!(matches!(
            ProviderReadFact::negotiated(&changed_endpoint, delta_contract),
            Err(CompletionProtocolError::ProviderRequestMismatch { id })
                if id == changed_endpoint.id()
        ));
    }

    #[test]
    fn provider_fact_occurrence_must_match_the_exact_need() {
        let need = provider_need(1, DataType::Int64, &[], None);
        let mut fact =
            ProviderReadFact::negotiated(&need, provider_contract(&need, b"provider-private"))
                .unwrap();
        fact.occurrence = ProviderReadOccurrenceId::new(99);

        assert!(matches!(
            validate_fact_semantics(
                &SqlNeedBatch::ProviderReads(Box::from([need])),
                &SqlFactBatch::ProviderReads(Box::from([fact])),
            ),
            Err(CompletionProtocolError::ProviderOccurrenceMismatch {
                expected,
                actual,
                ..
            }) if expected == ProviderReadOccurrenceId::new(1)
                && actual == ProviderReadOccurrenceId::new(99)
        ));
    }

    #[test]
    fn provider_need_builds_one_exact_combined_filter_from_occurrences() {
        let need = provider_need(1, DataType::Int64, &[7, 9], None);
        assert!(need.filter().summary().is_none());
        assert_eq!(need.predicates().len(), 2);
        assert_eq!(
            provider_expression_node_count(need.filter().expression()),
            1
        );

        let out_of_projection = ProviderReadNeed::try_new(
            CompileNeedId::new(2),
            ProviderReadOccurrenceId::new(2),
            SqlTableBindingId::new_for_test(41),
            ProviderReadRelationNeed::Data {
                relation: TableIdentity::new("iceberg", "db", "orders"),
                version: ProviderReadVersionNeed::Current,
            },
            [ProviderReadColumnNeed::try_new(
                0,
                "order_key",
                ValueType::new(DataType::Int64, false),
                ConnectorValueType::BigInt,
            )
            .unwrap()],
            [ProviderReadPredicateNeed::new(
                ProviderPredicateOccurrenceId::new(1),
                Constraint::of_summary(
                    TupleDomain::with_column_domains(BTreeMap::from([(
                        1,
                        Domain::single_value(ConnectorValue::BigInt(7)).unwrap(),
                    )]))
                    .unwrap(),
                ),
            )],
            None,
        );
        assert!(matches!(
            out_of_projection,
            Err(CompletionProtocolError::InvalidNeed {
                reason: "provider filter references a column outside the projection",
                ..
            })
        ));
    }

    #[test]
    fn provider_relation_and_type_requests_fail_closed() {
        let identity = TableIdentity::new("iceberg", "db", "orders");
        assert!(matches!(
            provider_relation_need_from_sql_scan(
                CompileNeedId::new(1),
                identity.clone(),
                &SqlScanKind::Metadata {
                    kind: MetadataTableKind::Entries,
                    version: SqlTableVersionSelector::Snapshot(17),
                },
            )
            .unwrap(),
            ProviderReadRelationNeed::Metadata {
                kind: MetadataTableKind::Entries,
                version: ProviderReadVersionNeed::Snapshot(17),
                ..
            }
        ));
        assert!(matches!(
            provider_relation_need_from_sql_scan(
                CompileNeedId::new(1),
                identity.clone(),
                &SqlScanKind::Delta {
                    from_snapshot_id: 3,
                    to_snapshot_id: 5,
                },
            )
            .unwrap(),
            ProviderReadRelationNeed::Delta {
                from_snapshot_id: 3,
                to_snapshot_id: 5,
                ..
            }
        ));
        let missing_reference = provider_relation_need_from_sql_scan(
            CompileNeedId::new(1),
            identity,
            &SqlScanKind::ConnectorRead,
        );
        assert!(matches!(
            missing_reference,
            Err(CompletionProtocolError::InvalidNeed {
                reason: "SQL scan kind has no exact provider completion operation",
                ..
            })
        ));
        assert!(matches!(
            ProviderReadColumnNeed::try_new(
                0,
                "order_key",
                ValueType::new(DataType::Int64, false),
                ConnectorValueType::Integer,
            ),
            Err(CompletionProtocolError::ProviderReadColumnTypeMismatch { ordinal: 0 })
        ));
    }

    #[test]
    fn provider_accounting_charges_predicate_argument_capacity() {
        fn need_with_argument_capacity(capacity: usize) -> ProviderReadNeed {
            let mut arguments = Vec::with_capacity(capacity);
            arguments.push(ConnectorExpression::constant_true());
            let expression = ConnectorExpression::Call {
                function: ConnectorFunctionName::try_new("identity").unwrap(),
                value_type: ConnectorValueType::Boolean,
                arguments,
            };
            let predicate = ProviderReadPredicateNeed::new(
                ProviderPredicateOccurrenceId::new(1),
                Constraint::try_new(TupleDomain::all(), expression, BTreeMap::new()).unwrap(),
            );
            ProviderReadNeed::try_new(
                CompileNeedId::new(1),
                ProviderReadOccurrenceId::new(1),
                SqlTableBindingId::new_for_test(41),
                ProviderReadRelationNeed::Data {
                    relation: TableIdentity::new("iceberg", "db", "orders"),
                    version: ProviderReadVersionNeed::Current,
                },
                [ProviderReadColumnNeed::try_new(
                    0,
                    "order_key",
                    ValueType::new(DataType::Int64, false),
                    ConnectorValueType::BigInt,
                )
                .unwrap()],
                [predicate],
                None,
            )
            .unwrap()
        }

        let compact = need_with_argument_capacity(1);
        let reserved = need_with_argument_capacity(256);
        let difference =
            provider_need_bytes(&reserved).unwrap() - provider_need_bytes(&compact).unwrap();
        assert_eq!(
            difference,
            ((256 - 1) * std::mem::size_of::<ConnectorExpression>()) as u64
        );
    }

    #[test]
    fn provider_accounting_charges_relation_string_capacity() {
        let compact = provider_need(1, DataType::Int64, &[], None);
        let mut reserved = provider_need(1, DataType::Int64, &[], None);
        let mut table = String::with_capacity(512);
        table.push_str("orders");
        let extra_capacity = table.capacity() - table.len();
        reserved.relation = ProviderReadRelationNeed::Data {
            relation: TableIdentity {
                catalog: "iceberg".to_string(),
                namespace: "db".to_string(),
                table,
            },
            version: ProviderReadVersionNeed::Current,
        };

        assert_eq!(
            provider_need_bytes(&reserved).unwrap() - provider_need_bytes(&compact).unwrap(),
            extra_capacity as u64
        );
    }

    #[test]
    fn materialized_view_observation_rejects_unrequested_base_and_duplicate_id() {
        let need = MaterializedViewNeed::try_new(
            CompileNeedId::new(1),
            [TableIdentity::new("iceberg", "db", "orders")],
        )
        .unwrap();
        let needs = SqlNeedBatch::MaterializedViews(Box::from([need.clone()]));

        let unrequested =
            SqlFactBatch::MaterializedViews(Box::from([MaterializedViewFact::observed(
                &need,
                [mv_definition(7, "iceberg.db.customers")],
            )]));
        assert!(matches!(
            validate_fact_semantics(&needs, &unrequested),
            Err(CompletionProtocolError::MaterializedViewBaseMismatch { .. })
        ));

        let definition = mv_definition(9, "iceberg.db.orders");
        let duplicate =
            SqlFactBatch::MaterializedViews(Box::from([MaterializedViewFact::observed(
                &need,
                [definition.clone(), definition],
            )]));
        assert!(matches!(
            validate_fact_semantics(&needs, &duplicate),
            Err(CompletionProtocolError::DuplicateMaterializedViewDefinition { mv_id: 9, .. })
        ));
    }

    #[test]
    fn provider_fact_debug_does_not_expose_opaque_payload() {
        const SENTINEL: &[u8] = b"opaque-provider-payload-sentinel";
        let need = provider_need(1, DataType::Int64, &[], None);
        let fact = ProviderReadFact::negotiated(&need, provider_contract(&need, SENTINEL)).unwrap();

        let debug = format!("{fact:?}");
        assert!(!debug.contains(std::str::from_utf8(SENTINEL).unwrap()));
        assert!(debug.contains("complete_frozen_contract"));

        let contract_debug = format!("{:?}", fact.contract());
        assert!(!contract_debug.contains(std::str::from_utf8(SENTINEL).unwrap()));
        assert!(!contract_debug.contains("provider_payload_bytes"));

        let sentinel = std::str::from_utf8(SENTINEL).unwrap();
        let column_need = provider_need_named(2, sentinel, DataType::Int64, &[], None);
        assert!(!format!("{column_need:?}").contains(sentinel));
        let relation_need = ProviderReadRelationNeed::Data {
            relation: TableIdentity::new("iceberg", "db", sentinel),
            version: ProviderReadVersionNeed::Current,
        };
        assert!(!format!("{relation_need:?}").contains(sentinel));
    }

    #[test]
    fn provider_contract_rejects_payload_from_another_binding() {
        let need = provider_need(1, DataType::Int64, &[], None);
        let mut contract = provider_contract(&need, b"private");
        let other_provider = ConnectorProviderId::parse("paimon").unwrap();
        let other_instance = ConnectorInstanceId::parse("warehouse").unwrap();
        let other_binding = ConnectorReadBinding::new(
            ConnectorInstanceDescriptor {
                provider_id: other_provider,
                instance_id: other_instance.clone(),
            },
            CatalogHandle::new(other_instance, CatalogVersion::from_bytes([4; 32])),
        );
        contract.schema[0].column.column_payload = encoded(
            &other_binding,
            ConnectorCodecCategory::ReadColumn,
            b"other-private",
        );
        assert!(matches!(
            ProviderReadFact::negotiated(&need, contract),
            Err(CompletionProtocolError::ProviderEnvelopeMismatch { id })
                if id == need.id()
        ));
    }

    #[test]
    fn provider_contract_classifies_every_predicate_occurrence_once() {
        let need = provider_need(1, DataType::Int64, &[5, 7], None);
        let mut missing = provider_contract(&need, b"private");
        missing.predicates = Box::from([ProviderReadPredicateFact::new(
            ProviderPredicateOccurrenceId::new(5),
            PredicateGuaranteeKind::Exact,
        )]);
        assert!(matches!(
            ProviderReadFact::negotiated(&need, missing),
            Err(CompletionProtocolError::ProviderPredicateMismatch { .. })
        ));

        let mut duplicate = provider_contract(&need, b"private");
        duplicate.predicates = Box::from([
            ProviderReadPredicateFact::new(
                ProviderPredicateOccurrenceId::new(5),
                PredicateGuaranteeKind::Exact,
            ),
            ProviderReadPredicateFact::new(
                ProviderPredicateOccurrenceId::new(5),
                PredicateGuaranteeKind::PruningOnly,
            ),
        ]);
        assert!(matches!(
            ProviderReadFact::negotiated(&need, duplicate),
            Err(CompletionProtocolError::ProviderPredicateMismatch { .. })
        ));
    }

    #[test]
    fn provider_properties_reject_out_of_range_and_duplicate_ordinals() {
        let need = provider_need(1, DataType::Int64, &[], None);
        let mut out_of_range = provider_contract(&need, b"private");
        out_of_range.provided_properties = ProviderReadProperties {
            distribution: ProviderReadDistribution::Hash {
                keys: Box::from([1]),
                scheme: hash_scheme(41),
            },
            ordering: Box::default(),
        };
        assert!(matches!(
            ProviderReadFact::negotiated(&need, out_of_range),
            Err(CompletionProtocolError::ProviderPropertyOrdinalOutOfRange {
                ordinal: 1,
                projection_len: 1,
                ..
            })
        ));

        let mut duplicate_distribution = provider_contract(&need, b"private");
        duplicate_distribution.provided_properties = ProviderReadProperties {
            distribution: ProviderReadDistribution::Hash {
                keys: Box::from([0, 0]),
                scheme: hash_scheme(42),
            },
            ordering: Box::default(),
        };
        assert!(matches!(
            ProviderReadFact::negotiated(&need, duplicate_distribution),
            Err(CompletionProtocolError::ProviderPropertyDuplicateOrdinal {
                property: "hash distribution",
                ordinal: 0,
                ..
            })
        ));

        let mut duplicate_ordering = provider_contract(&need, b"private");
        duplicate_ordering.provided_properties = ProviderReadProperties {
            distribution: ProviderReadDistribution::Unconstrained,
            ordering: Box::from([
                ProviderReadOrderingKey {
                    request_ordinal: 0,
                    direction: SortDirection::Ascending,
                    null_ordering: NullOrdering::First,
                },
                ProviderReadOrderingKey {
                    request_ordinal: 0,
                    direction: SortDirection::Descending,
                    null_ordering: NullOrdering::Last,
                },
            ]),
        };
        assert!(matches!(
            ProviderReadFact::negotiated(&need, duplicate_ordering),
            Err(CompletionProtocolError::ProviderPropertyDuplicateOrdinal {
                property: "ordering",
                ordinal: 0,
                ..
            })
        ));
    }
}
