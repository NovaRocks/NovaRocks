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

//! SQL-owned construction for application-admitted frozen connector reads.
//!
//! Applications retain the provider lease, binding store, and execution
//! resolver.  This module owns the only synthetic SQL scan used to carry that
//! admitted read through SQL planning.  In particular, it does not expose the
//! physical planner tree to Core: callers retain only an opaque scan program
//! until a SQL-owned terminal-planning entry consumes it.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use arrow::datatypes::SchemaRef;

use crate::analysis::OutputColumn;
use crate::binding::SqlTableBindingId;
use crate::catalog::ResolvedAnalyzerTable;
use crate::column_id::ColumnRefFactory;
use crate::plan_read::{BoundaryContract, DistributedPlan, FragmentId, PlanScanNode};
pub use crate::planner::payload::SqlScanOccurrence;
use crate::planner::payload::{MvRewriteInputSelection, MvRewriteSelection};
use novarocks_spi::connector::ConnectorReadPurpose;

mod pruning;
mod runtime_filter;
mod static_predicate;

pub use pruning::{
    NativeMinMaxPredicate, NativeMinMaxPredicateValue, native_scan_min_max_predicates,
};
pub use runtime_filter::*;
pub use static_predicate::lower_static_connector_predicates;

/// Immutable execution-preparation facts copied from one sealed distributed
/// plan. These values describe only sealed fragment topology and boundary
/// membership; they contain no planner tree, provider authority, lease, wire
/// payload, or lifecycle state.
#[derive(Clone, Debug)]
pub struct SqlExecutionPreparationFacts {
    topological_fragment_order: Vec<FragmentId>,
    execution_anchor_fragment_id: FragmentId,
    result_fragment_id: Option<FragmentId>,
    producer_fragment_ids: Vec<FragmentId>,
    boundary_contracts: Vec<BoundaryContract>,
}

impl SqlExecutionPreparationFacts {
    pub fn topological_fragment_order(&self) -> &[FragmentId] {
        &self.topological_fragment_order
    }

    pub fn execution_anchor_fragment_id(&self) -> FragmentId {
        self.execution_anchor_fragment_id
    }

    pub fn result_fragment_id(&self) -> Option<FragmentId> {
        self.result_fragment_id
    }

    pub fn producer_fragment_ids(&self) -> &[FragmentId] {
        &self.producer_fragment_ids
    }

    pub fn boundary_contracts(&self) -> &[BoundaryContract] {
        &self.boundary_contracts
    }
}

/// Project immutable coordinator-preparation values from an already sealed
/// plan. The private topology and boundary catalogs remain SQL-owned.
pub fn project_execution_preparation_facts(plan: &DistributedPlan) -> SqlExecutionPreparationFacts {
    let topology = plan.topology();
    SqlExecutionPreparationFacts {
        topological_fragment_order: topology.topological_fragment_order().to_vec(),
        execution_anchor_fragment_id: topology.execution_anchor_fragment_id(),
        result_fragment_id: topology.result_fragment_id(),
        producer_fragment_ids: topology.producer_fragment_ids().to_vec(),
        boundary_contracts: plan.boundaries().contracts().to_vec(),
    }
}

/// Validate a Connector negotiation result against the exact sealed scan it
/// describes. Query preparation uses this to seal residual responsibility
/// without exposing or reconstructing the planner tree.
pub fn scan_predicate_count(plan: &DistributedPlan, node_id: i32) -> Result<usize, String> {
    fn find(node: &crate::plan_read::DistributedNode, node_id: i32) -> Option<usize> {
        if node.node_id == node_id {
            return match &node.payload {
                crate::plan_read::DistributedNodeKind::Scan(scan) => Some(scan.predicates.len()),
                _ => None,
            };
        }
        node.children.iter().find_map(|child| find(child, node_id))
    }

    let mut found = plan
        .fragments()
        .iter()
        .filter_map(|fragment| find(&fragment.root, node_id));
    let count = found
        .next()
        .ok_or_else(|| format!("sealed distributed plan has no scan node {node_id}"))?;
    if found.next().is_some() {
        return Err(format!(
            "sealed distributed plan repeats scan node {node_id}"
        ));
    }
    Ok(count)
}

/// Typed SQL facts that preparation must cover for one sealed scan.
///
/// The application uses this projection to prove that bindings and Connector
/// negotiation outcomes form a complete, exact cover of the plan. It exposes
/// no provider handle or executable callback.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SealedPreparationPlanId(u64);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SealedScanIdentity {
    plan: SealedPreparationPlanId,
    node_id: i32,
}

impl SealedScanIdentity {
    pub const fn node_id(self) -> i32 {
        self.node_id
    }

    pub const fn plan(self) -> SealedPreparationPlanId {
        self.plan
    }
}

/// Query-local seal around one immutable distributed plan.
///
/// The opaque seal distinguishes two independently prepared queries even when
/// their node ids and scan shapes are identical. Clones retain the same seal so
/// observation, Connector negotiation, and final freezing can compare exact
/// scan occurrences without exposing a forgeable numeric occurrence id.
#[derive(Clone, Debug)]
pub struct SealedPreparationPlan {
    id: SealedPreparationPlanId,
    plan: Arc<DistributedPlan>,
}

impl SealedPreparationPlan {
    pub fn seal(plan: DistributedPlan) -> Self {
        static NEXT_PLAN_SEAL: AtomicU64 = AtomicU64::new(1);
        let id = NEXT_PLAN_SEAL.fetch_add(1, Ordering::Relaxed);
        assert!(id != 0, "sealed preparation plan identity exhausted");
        Self {
            id: SealedPreparationPlanId(id),
            plan: Arc::new(plan),
        }
    }

    pub const fn id(&self) -> SealedPreparationPlanId {
        self.id
    }

    pub fn plan(&self) -> &DistributedPlan {
        &self.plan
    }

    pub fn scan_contracts(&self) -> Result<Vec<SealedScanContract>, String> {
        sealed_scan_contracts(self)
    }

    pub fn scan_contract(&self, node_id: i32) -> Result<SealedScanContract, String> {
        self.scan_contracts()?
            .into_iter()
            .find(|scan| scan.node_id() == node_id)
            .ok_or_else(|| format!("sealed distributed plan has no scan node {node_id}"))
    }
}

#[derive(Clone, Debug)]
pub struct SealedScanContract {
    identity: SealedScanIdentity,
    node_id: i32,
    binding: SqlTableBindingId,
    sql_occurrence: SqlScanOccurrence,
    catalog: String,
    namespace: String,
    table: String,
    predicates: usize,
    projected_columns: Vec<OutputColumn>,
    offered_limit: bool,
    mv_rewritten_from: Option<MvRewriteSelection>,
}

fn sealed_scan_contract(
    plan: SealedPreparationPlanId,
    node_id: i32,
    scan: &PlanScanNode,
    offered_limit: bool,
) -> SealedScanContract {
    let facts = scan_preparation_facts(scan);
    let sql_occurrence = SqlScanOccurrence::from_scan(facts.binding(), &scan.columns)
        .expect("sealed SQL scan must retain at least one non-sentinel output column");
    SealedScanContract {
        identity: SealedScanIdentity { plan, node_id },
        node_id,
        binding: facts.binding(),
        sql_occurrence,
        catalog: facts.identity().catalog().to_string(),
        namespace: facts.identity().namespace().to_string(),
        table: facts.identity().table().to_string(),
        predicates: scan.predicates.len(),
        projected_columns: scan.columns.clone(),
        offered_limit,
        mv_rewritten_from: scan.mv_rewritten_from.clone(),
    }
}

impl SealedScanContract {
    pub const fn identity(&self) -> SealedScanIdentity {
        self.identity
    }

    pub const fn node_id(&self) -> i32 {
        self.node_id
    }

    pub const fn predicate_count(&self) -> usize {
        self.predicates
    }

    pub const fn binding(&self) -> SqlTableBindingId {
        self.binding
    }

    pub const fn sql_occurrence(&self) -> SqlScanOccurrence {
        self.sql_occurrence
    }

    pub fn catalog(&self) -> &str {
        &self.catalog
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn projected_columns(&self) -> &[OutputColumn] {
        &self.projected_columns
    }

    pub const fn offered_limit(&self) -> bool {
        self.offered_limit
    }

    /// Return the optimizer-emitted MV rewrite marker for this exact final
    /// scan. The marker is useful only together with this scan's opaque plan
    /// identity; its text alone is not rewrite evidence.
    pub fn mv_rewrite_action(&self) -> Option<SealedMvRewriteAction> {
        self.mv_rewritten_from.as_ref().and_then(|selection| {
            Some(SealedMvRewriteAction {
                target: self.identity,
                source: selection.name().to_string(),
                publication_id: selection.publication_id()?,
                definition_fingerprint: selection.definition_fingerprint()?,
                input_mapping: selection.input_mapping().to_vec(),
            })
        })
    }

    pub const fn was_mv_rewritten(&self) -> bool {
        self.mv_rewritten_from.is_some()
    }
}

/// SQL-owned marker projected from an actual scan in the final optimizer
/// output. Construction is private so application code cannot attach an MV
/// rewrite marker to an arbitrary node or plan seal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedMvRewriteAction {
    target: SealedScanIdentity,
    source: String,
    publication_id: [u8; 16],
    definition_fingerprint: [u8; 32],
    input_mapping: Vec<MvRewriteInputSelection>,
}

impl SealedMvRewriteAction {
    pub const fn target(&self) -> SealedScanIdentity {
        self.target
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub const fn publication_id(&self) -> [u8; 16] {
        self.publication_id
    }

    pub const fn definition_fingerprint(&self) -> [u8; 32] {
        self.definition_fingerprint
    }

    pub fn input_mapping(&self) -> &[MvRewriteInputSelection] {
        &self.input_mapping
    }
}

/// Enumerate every scan occurrence exactly once in sealed plan order.
pub fn sealed_scan_contracts(
    plan: &SealedPreparationPlan,
) -> Result<Vec<SealedScanContract>, String> {
    fn collect(
        plan: SealedPreparationPlanId,
        node: &crate::plan_read::DistributedNode,
        output: &mut Vec<SealedScanContract>,
    ) {
        if let crate::plan_read::DistributedNodeKind::Scan(scan) = &node.payload {
            output.push(sealed_scan_contract(
                plan,
                node.node_id,
                scan,
                node.limit >= 0,
            ));
        }
        for child in &node.children {
            collect(plan, child, output);
        }
    }

    let mut output = Vec::new();
    for fragment in plan.plan().fragments() {
        collect(plan.id(), &fragment.root, &mut output);
    }
    let mut seen = std::collections::BTreeSet::new();
    if let Some(duplicate) = output
        .iter()
        .map(SealedScanContract::node_id)
        .find(|node_id| !seen.insert(*node_id))
    {
        return Err(format!(
            "sealed distributed plan repeats scan node {duplicate}"
        ));
    }
    Ok(output)
}

/// Immutable SQL identity for a synthetic, application-admitted connector
/// scan.  It carries no catalog handle or provider capability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrozenConnectorScanIdentity {
    catalog: String,
    namespace: String,
    table: String,
}

impl FrozenConnectorScanIdentity {
    pub fn try_new(
        catalog: impl Into<String>,
        namespace: impl Into<String>,
        table: impl Into<String>,
    ) -> Result<Self, String> {
        let identity = Self::new(catalog, namespace, table);
        if identity.catalog.is_empty() || identity.namespace.is_empty() || identity.table.is_empty()
        {
            return Err("SQL table identity is incomplete".to_string());
        }
        Ok(identity)
    }

    pub fn new(
        catalog: impl Into<String>,
        namespace: impl Into<String>,
        table: impl Into<String>,
    ) -> Self {
        Self {
            catalog: catalog.into(),
            namespace: namespace.into(),
            table: table.into(),
        }
    }

    pub fn catalog(&self) -> &str {
        &self.catalog
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    fn planner_identity(&self) -> crate::planner::table::SqlTableIdentity {
        crate::planner::table::SqlTableIdentity {
            catalog: self.catalog.clone(),
            namespace: self.namespace.clone(),
            table: self.table.clone(),
        }
    }
}

/// Opaque synthetic scan program for one admitted frozen connector read.
///
/// It can only be consumed by SQL-owned planning entry points.  `scan()` is a
/// read projection used by Core preparation to match the exact binding; it
/// does not permit construction or mutation of the physical planner graph.
#[derive(Clone, Debug)]
pub struct FrozenConnectorScanPlan(crate::planner::physical::PhysicalPlanNode);

impl FrozenConnectorScanPlan {
    pub fn scan(&self) -> &PlanScanNode {
        let crate::planner::physical::PhysicalPlanKind::Scan(scan) = &self.0.kind else {
            unreachable!("frozen connector scan plan is constructed as one scan")
        };
        scan
    }

    pub fn output_column_count(&self) -> usize {
        self.0.output_columns.len()
    }

    /// Attach SQL predicates before the opaque scan program is sealed into a
    /// distributed plan.  This is used for an already admitted frozen source:
    /// Core later retains these as execution residuals rather than negotiating
    /// them against a newer provider generation.
    pub fn with_predicates(mut self, predicates: Vec<crate::plan_read::TypedExpr>) -> Self {
        let crate::planner::physical::PhysicalPlanKind::Scan(scan) = &mut self.0.kind else {
            unreachable!("frozen connector scan plan is constructed as one scan")
        };
        scan.predicates = predicates;
        self
    }

    pub(crate) fn into_physical(self) -> crate::planner::physical::PhysicalPlanNode {
        self.0
    }
}

/// Build the query-local analyzer table for an admitted frozen connector
/// source.  The caller still owns the binding-store lifetime and all provider
/// authority; SQL receives only a token, static identity, and Arrow schema.
pub fn frozen_connector_resolved_analyzer_table(
    identity: &FrozenConnectorScanIdentity,
    input_schema: SchemaRef,
    binding: SqlTableBindingId,
) -> ResolvedAnalyzerTable {
    synthetic_resolved_analyzer_table(
        identity,
        input_schema,
        binding,
        crate::planner::table::SqlScanKind::ConnectorRead,
    )
}

/// Build the query-local analyzer table for one real relation read at one
/// exact version.
///
/// This is the read an internal collection performs: no SQL text named the
/// table, but the relation and its version are both real and stated, so the
/// binding says so rather than presenting the read as a synthetic opaque
/// source. Preparation then resolves it through the ordinary admitted-data
/// lane, pinned to `version_ordinal`.
pub fn pinned_version_resolved_analyzer_table(
    identity: &FrozenConnectorScanIdentity,
    input_schema: SchemaRef,
    binding: SqlTableBindingId,
    version_ordinal: i64,
) -> ResolvedAnalyzerTable {
    synthetic_resolved_analyzer_table(
        identity,
        input_schema,
        binding,
        crate::planner::table::SqlScanKind::Data {
            version: crate::planner::table::SqlTableVersionSelector::Snapshot(version_ordinal),
        },
    )
}

/// Build the query-local analyzer table for a provider-frozen cohort read of a
/// pinned file set.  SQL receives the same token, static identity, and Arrow
/// schema as any synthetic source; the file set itself never enters SQL.
pub fn pinned_file_set_resolved_analyzer_table(
    identity: &FrozenConnectorScanIdentity,
    input_schema: SchemaRef,
    binding: SqlTableBindingId,
) -> ResolvedAnalyzerTable {
    synthetic_resolved_analyzer_table(
        identity,
        input_schema,
        binding,
        crate::planner::table::SqlScanKind::PinnedFileSet,
    )
}

/// Build the query-local analyzer table for one distributed
/// `ALTER TABLE ... EXECUTE` procedure read.  SQL receives the same token,
/// static identity, and Arrow schema as any synthetic source; the frozen group
/// itself never enters SQL.
pub fn table_execute_resolved_analyzer_table(
    identity: &FrozenConnectorScanIdentity,
    input_schema: SchemaRef,
    binding: SqlTableBindingId,
) -> ResolvedAnalyzerTable {
    synthetic_resolved_analyzer_table(
        identity,
        input_schema,
        binding,
        crate::planner::table::SqlScanKind::TableExecute,
    )
}

fn synthetic_resolved_analyzer_table(
    identity: &FrozenConnectorScanIdentity,
    input_schema: SchemaRef,
    binding: SqlTableBindingId,
    kind: crate::planner::table::SqlScanKind,
) -> ResolvedAnalyzerTable {
    let columns = column_defs(&input_schema);
    let planner_identity = identity.planner_identity();
    ResolvedAnalyzerTable::from_planner(
        Some(identity.catalog()),
        identity.namespace(),
        crate::planner::table::TableDef {
            name: identity.table().to_string(),
            columns,
            iceberg_row_lineage_metadata_columns: Vec::new(),
            source: crate::planner::table::ScanSource::Sql(
                crate::planner::table::SqlScanSource::new(binding, planner_identity, kind),
            ),
        },
    )
}

/// Build the SQL-owned analyzer materialization for an admitted terminal write
/// target.  The application retains the provider preparation and exact lease;
/// SQL receives only copied identity, Arrow schema, and a request-scoped
/// binding token.  This is deliberately distinct from a read materialization
/// so a write target cannot be reinterpreted as a connector scan.
pub fn frozen_connector_write_target_resolved_analyzer_table(
    identity: &FrozenConnectorScanIdentity,
    input_schema: SchemaRef,
    binding: SqlTableBindingId,
) -> ResolvedAnalyzerTable {
    let columns = column_defs(&input_schema);
    let planner_identity = identity.planner_identity();
    ResolvedAnalyzerTable::from_planner(
        Some(identity.catalog()),
        identity.namespace(),
        crate::planner::table::TableDef {
            name: identity.table().to_string(),
            columns,
            iceberg_row_lineage_metadata_columns: Vec::new(),
            source: crate::planner::table::ScanSource::Sql(
                crate::planner::table::SqlScanSource::new(
                    binding,
                    planner_identity,
                    crate::planner::table::SqlScanKind::Data {
                        version: crate::planner::table::SqlTableVersionSelector::Current,
                    },
                ),
            ),
        },
    )
}

/// Construct the sole synthetic scan carrier accepted for a frozen connector
/// source.  The physical tree stays opaque outside SQL.
pub fn build_frozen_connector_scan_plan(
    identity: &FrozenConnectorScanIdentity,
    input_schema: &SchemaRef,
    binding: SqlTableBindingId,
) -> FrozenConnectorScanPlan {
    build_synthetic_scan_plan(
        identity,
        input_schema,
        binding,
        crate::planner::table::SqlScanKind::ConnectorRead,
    )
}

/// Construct the synthetic scan carrier for one provider-frozen cohort read of
/// a pinned file set.  The physical tree stays opaque outside SQL.
pub fn build_pinned_file_set_scan_plan(
    identity: &FrozenConnectorScanIdentity,
    input_schema: &SchemaRef,
    binding: SqlTableBindingId,
) -> FrozenConnectorScanPlan {
    build_synthetic_scan_plan(
        identity,
        input_schema,
        binding,
        crate::planner::table::SqlScanKind::PinnedFileSet,
    )
}

/// Construct the synthetic scan carrier for one distributed
/// `ALTER TABLE ... EXECUTE` procedure read.  The physical tree stays opaque
/// outside SQL.
pub fn build_table_execute_scan_plan(
    identity: &FrozenConnectorScanIdentity,
    input_schema: &SchemaRef,
    binding: SqlTableBindingId,
) -> FrozenConnectorScanPlan {
    build_synthetic_scan_plan(
        identity,
        input_schema,
        binding,
        crate::planner::table::SqlScanKind::TableExecute,
    )
}

fn build_synthetic_scan_plan(
    identity: &FrozenConnectorScanIdentity,
    input_schema: &SchemaRef,
    binding: SqlTableBindingId,
    kind: crate::planner::table::SqlScanKind,
) -> FrozenConnectorScanPlan {
    let mut factory = ColumnRefFactory::new();
    let mut output_columns = Vec::with_capacity(input_schema.fields().len());
    for field in input_schema.fields() {
        output_columns.push(OutputColumn {
            column_id: factory.create(
                None,
                field.name().to_string(),
                field.data_type().clone(),
                field.is_nullable(),
            ),
            name: field.name().to_string(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
            is_internal: false,
        });
    }
    let table = crate::planner::table::TableDef {
        name: identity.table().to_string(),
        columns: column_defs(input_schema),
        iceberg_row_lineage_metadata_columns: Vec::new(),
        source: crate::planner::table::ScanSource::Sql(crate::planner::table::SqlScanSource::new(
            binding,
            identity.planner_identity(),
            kind,
        )),
    };
    FrozenConnectorScanPlan(crate::planner::physical::PhysicalPlanNode {
        kind: crate::planner::physical::PhysicalPlanKind::Scan(PlanScanNode {
            database: identity.namespace().to_string(),
            table,
            alias: None,
            columns: output_columns.clone(),
            predicates: Vec::new(),
            required_columns: None,
            variant_columns: Vec::new(),
            mv_rewritten_from: None,
        }),
        children: Vec::new(),
        output_columns,
        stats: crate::planner::physical::PhysicalPlanStats {
            output_row_count: 0.0,
            row_count_confidence: crate::planner::physical::PlannerConfidence::Fallback,
            column_statistics: HashMap::new(),
            cost_estimate: None,
            broadcast_decision: None,
        },
        probe_runtime_filters: Vec::new(),
    })
}

/// Compare a sealed/read-only scan projection with the exact application
/// binding that admitted a frozen connector source.
pub fn matches_frozen_connector_scan(
    scan: &PlanScanNode,
    binding: SqlTableBindingId,
    identity: &FrozenConnectorScanIdentity,
) -> bool {
    matches_synthetic_scan(
        scan,
        binding,
        identity,
        &crate::planner::table::SqlScanKind::ConnectorRead,
    )
}

/// Compare a sealed/read-only scan projection with the exact application
/// binding that admitted a pinned-file-set cohort read.
pub fn matches_pinned_file_set_scan(
    scan: &PlanScanNode,
    binding: SqlTableBindingId,
    identity: &FrozenConnectorScanIdentity,
) -> bool {
    matches_synthetic_scan(
        scan,
        binding,
        identity,
        &crate::planner::table::SqlScanKind::PinnedFileSet,
    )
}

/// Compare a sealed/read-only scan projection with the exact application
/// binding that admitted a table-execute procedure read.
pub fn matches_table_execute_scan(
    scan: &PlanScanNode,
    binding: SqlTableBindingId,
    identity: &FrozenConnectorScanIdentity,
) -> bool {
    matches_synthetic_scan(
        scan,
        binding,
        identity,
        &crate::planner::table::SqlScanKind::TableExecute,
    )
}

fn matches_synthetic_scan(
    scan: &PlanScanNode,
    binding: SqlTableBindingId,
    identity: &FrozenConnectorScanIdentity,
    kind: &crate::planner::table::SqlScanKind,
) -> bool {
    let crate::planner::table::ScanSource::Sql(source) = &scan.table.source;
    &source.kind == kind && source.binding == binding && source.table == identity.planner_identity()
}

/// The execution-relevant category of one sealed SQL scan. This is a copied
/// routing label, not a planner source or provider request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlScanPreparationCategory {
    ConnectorRead,
    AdmittedPinnedFileSet,
    AdmittedTableExecute,
    AdmittedData,
    AdmittedFrozenCurrent,
    AdmittedFrozenSnapshot,
    FrozenTimestampWithoutAdmittedSnapshot,
    AdmittedMetadata,
    Delta,
    MvTargetState,
    MvTargetLocator,
}

/// Immutable catalog identity copied from a sealed SQL scan.
///
/// The fields remain private so callers can report and compare identity, but
/// cannot reconstruct the tokenized scan source that carried it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlScanPreparationIdentity {
    catalog: String,
    namespace: String,
    table: String,
}

impl SqlScanPreparationIdentity {
    pub fn catalog(&self) -> &str {
        &self.catalog
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn fqn(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.namespace, self.table)
    }
}

/// One copied immutable change window from a sealed delta scan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SqlScanPreparationDeltaWindow {
    from_snapshot_id: i64,
    to_snapshot_id: i64,
}

impl SqlScanPreparationDeltaWindow {
    pub fn from_snapshot_id(&self) -> i64 {
        self.from_snapshot_id
    }

    pub fn to_snapshot_id(&self) -> i64 {
        self.to_snapshot_id
    }
}

/// Copied immutable MV target facts needed only to select an already admitted
/// query-local materialization. It contains no MV plan, provider handle, or
/// lifecycle state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlMvTargetScanPreparationFacts {
    target_table_uuid: String,
    target_snapshot_id: Option<i64>,
    use_affected_partitions: bool,
}

impl SqlMvTargetScanPreparationFacts {
    pub fn target_table_uuid(&self) -> &str {
        &self.target_table_uuid
    }

    pub fn target_snapshot_id(&self) -> Option<i64> {
        self.target_snapshot_id
    }

    pub fn use_affected_partitions(&self) -> bool {
        self.use_affected_partitions
    }
}

/// Opaque execution facts projected from one sealed SQL scan.
///
/// Core may recover its paired request-local provider authority by the binding
/// token and select a route by category. It cannot inspect or reconstruct the
/// SQL scan graph, optimizer tree, wire state, or connector lease.
#[derive(Clone, Debug)]
pub struct SqlScanPreparationFacts {
    category: SqlScanPreparationCategory,
    binding: SqlTableBindingId,
    identity: SqlScanPreparationIdentity,
    frozen_snapshot_id: Option<i64>,
    frozen_timestamp_millis: Option<i64>,
    delta_window: Option<SqlScanPreparationDeltaWindow>,
    metadata_table_kind: Option<crate::planner::table::SqlMetadataTableKind>,
    mv_target: Option<SqlMvTargetScanPreparationFacts>,
    connector_read_purpose: ConnectorReadPurpose,
    refresh_projected_names: Option<Vec<String>>,
}

impl SqlScanPreparationFacts {
    pub fn category(&self) -> SqlScanPreparationCategory {
        self.category
    }

    pub fn binding(&self) -> SqlTableBindingId {
        self.binding
    }

    pub fn identity(&self) -> &SqlScanPreparationIdentity {
        &self.identity
    }

    pub fn frozen_snapshot_id(&self) -> Option<i64> {
        self.frozen_snapshot_id
    }

    pub fn frozen_timestamp_millis(&self) -> Option<i64> {
        self.frozen_timestamp_millis
    }

    pub fn delta_window(&self) -> Option<SqlScanPreparationDeltaWindow> {
        self.delta_window
    }

    /// Which system relation an `AdmittedMetadata` scan reads.
    ///
    /// The `$suffix` spelling is stripped before these facts exist, so this is
    /// the only place the kind survives. Without it the connector cannot be
    /// asked for the relation at all: it recognizes a system relation by its
    /// suffix and nothing else.
    pub fn metadata_table_kind(&self) -> Option<crate::planner::table::SqlMetadataTableKind> {
        self.metadata_table_kind
    }

    pub fn mv_target(&self) -> Option<&SqlMvTargetScanPreparationFacts> {
        self.mv_target.as_ref()
    }

    pub fn connector_read_purpose(&self) -> ConnectorReadPurpose {
        self.connector_read_purpose
    }

    pub fn refresh_projected_names(&self) -> Option<&[String]> {
        self.refresh_projected_names.as_deref()
    }

    pub fn source_kind_label(&self) -> &'static str {
        match self.category {
            SqlScanPreparationCategory::ConnectorRead => "SqlConnectorRead",
            SqlScanPreparationCategory::AdmittedPinnedFileSet => "SqlPinnedFileSet",
            SqlScanPreparationCategory::AdmittedTableExecute => "SqlTableExecute",
            SqlScanPreparationCategory::AdmittedData => "SqlData",
            SqlScanPreparationCategory::AdmittedFrozenCurrent
            | SqlScanPreparationCategory::AdmittedFrozenSnapshot
            | SqlScanPreparationCategory::FrozenTimestampWithoutAdmittedSnapshot => {
                "SqlFrozenInputSet"
            }
            SqlScanPreparationCategory::AdmittedMetadata => "SqlMetadata",
            SqlScanPreparationCategory::Delta => "SqlDelta",
            SqlScanPreparationCategory::MvTargetState => "SqlMvTargetState",
            SqlScanPreparationCategory::MvTargetLocator => "SqlMvTargetLocator",
        }
    }

    pub fn source_context(&self) -> String {
        match self.delta_window {
            Some(window) => format!(
                "SqlDelta from_snapshot_id={} to_snapshot_id={}",
                window.from_snapshot_id, window.to_snapshot_id
            ),
            None => self.source_kind_label().to_string(),
        }
    }
}

/// Project exactly the immutable facts needed by application scan preparation
/// from a sealed plan node. SQL owns all raw source and MV scan vocabulary;
/// callers receive only copied values and a request-local binding token.
pub fn scan_preparation_facts(scan: &PlanScanNode) -> SqlScanPreparationFacts {
    let crate::planner::table::ScanSource::Sql(source) = &scan.table.source;
    let identity = SqlScanPreparationIdentity {
        catalog: source.table.catalog.clone(),
        namespace: source.table.namespace.clone(),
        table: source.table.table.clone(),
    };
    let mut facts = SqlScanPreparationFacts {
        category: SqlScanPreparationCategory::ConnectorRead,
        binding: source.binding,
        identity,
        frozen_snapshot_id: None,
        frozen_timestamp_millis: None,
        delta_window: None,
        metadata_table_kind: None,
        mv_target: None,
        connector_read_purpose: ConnectorReadPurpose::Query,
        refresh_projected_names: None,
    };
    match &source.kind {
        crate::planner::table::SqlScanKind::ConnectorRead => {}
        crate::planner::table::SqlScanKind::PinnedFileSet => {
            facts.category = SqlScanPreparationCategory::AdmittedPinnedFileSet;
        }
        crate::planner::table::SqlScanKind::TableExecute => {
            facts.category = SqlScanPreparationCategory::AdmittedTableExecute;
        }
        crate::planner::table::SqlScanKind::Data { .. } => {
            facts.category = SqlScanPreparationCategory::AdmittedData;
        }
        crate::planner::table::SqlScanKind::FrozenInputSet {
            version: crate::planner::table::SqlTableVersionSelector::Current,
        } => {
            facts.category = SqlScanPreparationCategory::AdmittedFrozenCurrent;
        }
        crate::planner::table::SqlScanKind::FrozenInputSet {
            version: crate::planner::table::SqlTableVersionSelector::Snapshot(snapshot_id),
        } => {
            facts.category = SqlScanPreparationCategory::AdmittedFrozenSnapshot;
            facts.frozen_snapshot_id = Some(*snapshot_id);
        }
        crate::planner::table::SqlScanKind::FrozenInputSet {
            version: crate::planner::table::SqlTableVersionSelector::TimestampMillis(timestamp),
        } => {
            facts.category = SqlScanPreparationCategory::FrozenTimestampWithoutAdmittedSnapshot;
            facts.frozen_timestamp_millis = Some(*timestamp);
        }
        crate::planner::table::SqlScanKind::Metadata { kind, .. } => {
            facts.category = SqlScanPreparationCategory::AdmittedMetadata;
            facts.metadata_table_kind = Some(*kind);
        }
        crate::planner::table::SqlScanKind::Delta {
            from_snapshot_id,
            to_snapshot_id,
        } => {
            facts.category = SqlScanPreparationCategory::Delta;
            facts.delta_window = Some(SqlScanPreparationDeltaWindow {
                from_snapshot_id: *from_snapshot_id,
                to_snapshot_id: *to_snapshot_id,
            });
        }
        crate::planner::table::SqlScanKind::MvTargetState { facts: target } => {
            facts.category = SqlScanPreparationCategory::MvTargetState;
            facts.connector_read_purpose = ConnectorReadPurpose::MvTargetState;
            facts.mv_target = Some(SqlMvTargetScanPreparationFacts {
                target_table_uuid: target.target_table_uuid.clone(),
                target_snapshot_id: target.target_snapshot_id,
                use_affected_partitions: matches!(
                    target.partition_constraint,
                    crate::planner::table::SqlMvTargetStatePartitionConstraint::AffectedPartitionAllowListRequired
                ),
            });
            facts.refresh_projected_names = Some(target_state_projected_names(target));
        }
        crate::planner::table::SqlScanKind::MvTargetLocator { facts: target } => {
            facts.category = SqlScanPreparationCategory::MvTargetLocator;
            facts.connector_read_purpose = ConnectorReadPurpose::MvTargetLocator;
            facts.mv_target = Some(SqlMvTargetScanPreparationFacts {
                target_table_uuid: target.target_table_uuid.clone(),
                target_snapshot_id: target.target_snapshot_id,
                use_affected_partitions: false,
            });
            facts.refresh_projected_names = Some(target_locator_projected_names(target));
        }
    }
    facts
}

fn target_state_projected_names(
    target: &crate::planner::table::SqlMvTargetStateScan,
) -> Vec<String> {
    let mut names = Vec::new();
    push_unique_name(&mut names, &target.row_id_column_name);
    for name in target
        .group_key_names
        .iter()
        .chain(target.aggregate_state_names.iter())
    {
        push_unique_name(&mut names, name);
    }
    if let crate::planner::table::SqlMvTargetStateRowFilter::DeltaInputRowIds {
        branch_scope: Some(scope),
        ..
    } = &target.row_filter
    {
        push_unique_name(&mut names, &scope.branch_id_column_name);
    }
    for name in ["_file", "_pos", "_row_id", "_last_updated_sequence_number"] {
        push_unique_name(&mut names, name);
    }
    names
}

fn target_locator_projected_names(
    target: &crate::planner::table::SqlMvTargetLocatorScan,
) -> Vec<String> {
    let mut names = vec![target.apply_key_column.clone()];
    if let Some(branch_id_column) = &target.branch_id_column {
        push_unique_name(&mut names, branch_id_column);
    }
    for name in ["_file", "_pos", "_row_id", "_last_updated_sequence_number"] {
        push_unique_name(&mut names, name);
    }
    names
}

fn push_unique_name(names: &mut Vec<String>, name: &str) {
    if !names
        .iter()
        .any(|existing| existing.eq_ignore_ascii_case(name))
    {
        names.push(name.to_string());
    }
}

fn column_defs(input_schema: &SchemaRef) -> Vec<novarocks_types::schema::ColumnDef> {
    input_schema
        .fields()
        .iter()
        .map(|field| novarocks_types::schema::ColumnDef {
            name: field.name().to_string(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
            write_default: None,
            logical_type: None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    use super::{
        FrozenConnectorScanIdentity, SqlScanPreparationCategory, build_frozen_connector_scan_plan,
        frozen_connector_resolved_analyzer_table, matches_frozen_connector_scan,
        project_execution_preparation_facts, scan_preparation_facts,
    };
    use crate::binding::SqlTableBindingId;
    use crate::plan_read::DistributedNodeKind;

    fn binding() -> SqlTableBindingId {
        SqlTableBindingId::new_for_test(7)
    }

    #[test]
    fn frozen_identity_rejects_incomplete_write_target_facts() {
        let error = FrozenConnectorScanIdentity::try_new("ice", "analytics", "")
            .expect_err("terminal write identity must be complete");
        assert!(error.contains("incomplete"));
    }

    #[test]
    fn frozen_scan_program_preserves_only_static_identity_and_binding() {
        let identity = FrozenConnectorScanIdentity::new("__frozen", "operation", "cohort_7");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let plan = build_frozen_connector_scan_plan(&identity, &schema, binding());

        assert_eq!(plan.output_column_count(), 1);
        assert!(matches_frozen_connector_scan(
            plan.scan(),
            binding(),
            &identity
        ));
        assert!(!matches_frozen_connector_scan(
            plan.scan(),
            SqlTableBindingId::new_for_test(8),
            &identity,
        ));
    }

    #[test]
    fn frozen_analyzer_table_carries_the_same_sql_identity() {
        let identity = FrozenConnectorScanIdentity::new("__frozen", "operation", "cohort_7");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let _resolved = frozen_connector_resolved_analyzer_table(&identity, schema, binding());
    }

    #[test]
    fn scan_preparation_facts_copy_only_sealed_scan_admission_data() {
        use crate::test_support::{NativeScanFixture, native_scan_plan};

        let facts_for = |fixture| {
            let plan = native_scan_plan(fixture).expect("sealed scan fixture");
            let scan = plan
                .fragments()
                .iter()
                .find_map(|fragment| match &fragment.root.payload {
                    DistributedNodeKind::Scan(scan) => Some(scan),
                    _ => None,
                })
                .expect("fixture has one scan");
            scan_preparation_facts(scan)
        };

        let delta = facts_for(NativeScanFixture::DeltaForPreparedBinding);
        assert_eq!(delta.category(), SqlScanPreparationCategory::Delta);
        assert_eq!(
            delta
                .delta_window()
                .expect("delta window")
                .from_snapshot_id(),
            6
        );
        assert_eq!(
            delta.delta_window().expect("delta window").to_snapshot_id(),
            7
        );

        let timestamp = facts_for(NativeScanFixture::FrozenTimestamp);
        assert_eq!(
            timestamp.category(),
            SqlScanPreparationCategory::FrozenTimestampWithoutAdmittedSnapshot
        );
        assert_eq!(timestamp.frozen_timestamp_millis(), Some(1_704_067_200_000));

        let target = facts_for(NativeScanFixture::RefreshMvTargetState);
        assert_eq!(target.category(), SqlScanPreparationCategory::MvTargetState);
        assert_eq!(
            target.connector_read_purpose(),
            novarocks_spi::connector::ConnectorReadPurpose::MvTargetState
        );
        assert!(
            !target
                .mv_target()
                .expect("target facts")
                .use_affected_partitions()
        );
        assert_eq!(
            target.refresh_projected_names().expect("target projection"),
            [
                "bound_order_id",
                "_file",
                "_pos",
                "_row_id",
                "_last_updated_sequence_number"
            ]
        );
    }

    #[test]
    fn execution_preparation_facts_copy_only_sealed_topology_and_boundaries() {
        use crate::test_support::{NativePreparationFixture, native_preparation_plan};

        let result = native_preparation_plan(NativePreparationFixture::ResultOutput)
            .expect("sealed result fixture");
        let result_facts = project_execution_preparation_facts(&result);
        assert_eq!(result_facts.topological_fragment_order(), [7]);
        assert_eq!(result_facts.execution_anchor_fragment_id(), 7);
        assert_eq!(result_facts.result_fragment_id(), Some(7));
        assert!(result_facts.producer_fragment_ids().is_empty());
        assert_eq!(result_facts.boundary_contracts().len(), 1);
    }
}
