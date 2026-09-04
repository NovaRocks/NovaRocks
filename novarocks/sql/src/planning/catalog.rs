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

//! Narrow catalog and view-analysis handoffs.
//!
//! This module carries catalog materialization and typed view-analysis facts;
//! it does not expose a parser or planner implementation tree.

use std::collections::{BTreeMap, BTreeSet};

use crate::analyze_error::AnalyzeError;
use crate::planner::table::TableDef;
use novarocks_parser::ast::{Query, TableVersion};
#[cfg(test)]
use novarocks_parser::ast::{SetExpr, Statement, TableFactor};
use novarocks_types::naming::TableIdentity;
use novarocks_types::schema::{CatalogTable, ColumnDef};

/// The frozen column contract for one metadata relation.
///
/// SQL owns it because no typed column binding carries a type: the connector
/// names a metadata relation's columns and this states what they are. It is
/// re-exported through the planning surface so an application catalog can
/// analyze the same columns the typed reader produces, rather than whatever
/// shape a provider's own alias materialization happens to report.
pub use crate::analyzer::iceberg_metadata::{MetadataColumn, metadata_table_schema};
pub use crate::catalog::memory::PlannerMemoryCatalog;
pub use crate::catalog::{
    IcebergMetadataTableProvider, PlannerTableProvider, ResolvedAnalyzerTable, TableLookupMode,
};
/// SQL metadata-relation vocabulary needed by application catalog admission.
/// This is a value-only DTO; it carries neither a table definition nor a
/// provider handle.
pub use crate::planner::table::SqlMetadataTableKind as MetadataTableKind;

/// The kind of a named Iceberg snapshot reference copied from an admitted
/// connector metadata observation.  This stays a value fact: it carries no
/// provider object, catalog handle, or metadata lease.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlTimeTravelReferenceKind {
    Branch,
    Tag,
}

/// One immutable snapshot-log entry copied from an admitted connector
/// metadata observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlTimeTravelSnapshotLogFacts {
    snapshot_id: i64,
    timestamp_millis: i64,
}

impl SqlTimeTravelSnapshotLogFacts {
    pub fn new(snapshot_id: i64, timestamp_millis: i64) -> Self {
        Self {
            snapshot_id,
            timestamp_millis,
        }
    }
}

/// One immutable named-reference fact copied from an admitted connector
/// metadata observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlTimeTravelNamedReferenceFacts {
    name: String,
    kind: SqlTimeTravelReferenceKind,
    snapshot_id: i64,
}

impl SqlTimeTravelNamedReferenceFacts {
    pub fn try_new(
        name: String,
        kind: SqlTimeTravelReferenceKind,
        snapshot_id: i64,
    ) -> Result<Self, String> {
        if name.is_empty() {
            return Err("iceberg time travel: named reference cannot be empty".to_string());
        }
        Ok(Self {
            name,
            kind,
            snapshot_id,
        })
    }
}

/// Validated immutable snapshot-reference facts consumed by the SQL
/// time-travel resolver.  Applications can only copy already-admitted
/// metadata into this value; SQL keeps its internal metadata representation
/// private.
pub struct SqlTimeTravelReferenceMetadataFacts {
    snapshot_ids: BTreeSet<i64>,
    snapshot_log: Vec<SqlTimeTravelSnapshotLogFacts>,
    named_references: Vec<SqlTimeTravelNamedReferenceFacts>,
    current_snapshot_id: Option<i64>,
}

impl SqlTimeTravelReferenceMetadataFacts {
    pub fn try_new(
        snapshot_ids: Vec<i64>,
        snapshot_log: Vec<SqlTimeTravelSnapshotLogFacts>,
        named_references: Vec<SqlTimeTravelNamedReferenceFacts>,
        current_snapshot_id: Option<i64>,
    ) -> Result<Self, String> {
        let mut unique_snapshot_ids = BTreeSet::new();
        for snapshot_id in snapshot_ids {
            if !unique_snapshot_ids.insert(snapshot_id) {
                return Err(
                    "iceberg time travel: snapshot facts contain duplicate snapshot IDs"
                        .to_string(),
                );
            }
        }

        Self::from_parts(
            unique_snapshot_ids,
            snapshot_log,
            named_references,
            current_snapshot_id,
        )
    }

    fn from_parts(
        snapshot_ids: BTreeSet<i64>,
        snapshot_log: Vec<SqlTimeTravelSnapshotLogFacts>,
        named_references: Vec<SqlTimeTravelNamedReferenceFacts>,
        current_snapshot_id: Option<i64>,
    ) -> Result<Self, String> {
        if current_snapshot_id.is_some_and(|snapshot_id| !snapshot_ids.contains(&snapshot_id)) {
            return Err(
                "iceberg time travel: current snapshot is not listed in snapshot facts".to_string(),
            );
        }

        let mut seen_log_entries = BTreeSet::new();
        for entry in &snapshot_log {
            if !snapshot_ids.contains(&entry.snapshot_id) {
                return Err(
                    "iceberg time travel: snapshot log references an unknown snapshot".to_string(),
                );
            }
            if !seen_log_entries.insert((entry.timestamp_millis, entry.snapshot_id)) {
                return Err(
                    "iceberg time travel: snapshot facts contain duplicate snapshot-log entries"
                        .to_string(),
                );
            }
        }

        let mut seen_reference_names = BTreeSet::new();
        for reference in &named_references {
            if !snapshot_ids.contains(&reference.snapshot_id) {
                return Err(
                    "iceberg time travel: named reference points to an unknown snapshot"
                        .to_string(),
                );
            }
            if !seen_reference_names.insert(reference.name.as_str()) {
                return Err(
                    "iceberg time travel: snapshot facts contain duplicate named references"
                        .to_string(),
                );
            }
        }

        Ok(Self {
            snapshot_ids,
            snapshot_log,
            named_references,
            current_snapshot_id,
        })
    }

    fn as_iceberg_ref_metadata(&self) -> crate::analyzer::iceberg_ref::SqlIcebergRefMetadata {
        use crate::analyzer::iceberg_ref::{
            IcebergRefKind, SqlIcebergNamedRef, SqlIcebergSnapshotLog,
        };

        crate::analyzer::iceberg_ref::SqlIcebergRefMetadata::new(
            self.snapshot_ids.iter().copied(),
            self.snapshot_log
                .iter()
                .map(|entry| SqlIcebergSnapshotLog {
                    snapshot_id: entry.snapshot_id,
                    timestamp_ms: entry.timestamp_millis,
                })
                .collect(),
            self.named_references
                .iter()
                .map(|entry| {
                    (
                        entry.name.clone(),
                        SqlIcebergNamedRef {
                            snapshot_id: entry.snapshot_id,
                            kind: match entry.kind {
                                SqlTimeTravelReferenceKind::Branch => IcebergRefKind::Branch,
                                SqlTimeTravelReferenceKind::Tag => IcebergRefKind::Tag,
                            },
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>(),
            self.current_snapshot_id,
        )
    }
}

/// Sealed result of resolving one parsed time-travel clause against validated
/// immutable snapshot facts.  SQL exposes only the frozen snapshot identity
/// needed for query-local rewrite; ref metadata remains internal.
pub struct SqlTimeTravelSnapshotBindingFacts {
    snapshot_id: i64,
}

impl SqlTimeTravelSnapshotBindingFacts {
    pub const fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }
}

/// Resolve a parsed `FOR VERSION/TIMESTAMP AS OF` clause from copied,
/// validated snapshot-reference facts.  Unknown requested snapshots and refs
/// fail closed in SQL before Core creates a synthetic table identity.
pub fn resolve_time_travel_snapshot_binding(
    version: &TableVersion,
    metadata: &SqlTimeTravelReferenceMetadataFacts,
    fully_qualified_name: &str,
) -> Result<SqlTimeTravelSnapshotBindingFacts, AnalyzeError> {
    let metadata = metadata.as_iceberg_ref_metadata();
    let binding = crate::analyzer::iceberg_ref::resolve_read_binding(
        version,
        &metadata,
        fully_qualified_name,
    )?;
    Ok(SqlTimeTravelSnapshotBindingFacts {
        snapshot_id: binding.snapshot_id,
    })
}

/// Immutable provider-neutral facts for one connector read admitted by the
/// application boundary.  This contains no connector handle or lease: those
/// remain paired with the binding token in the application request store.
pub struct ConnectorReadTableFacts {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub columns: Vec<ColumnDef>,
    pub iceberg_row_lineage_metadata_columns: Vec<ColumnDef>,
    pub schema: arrow::datatypes::SchemaRef,
    pub binding: crate::binding::SqlTableBindingId,
    pub selector: novarocks_spi::connector::ConnectorReadSelector,
    pub planning_facts: novarocks_spi::connector::ConnectorTablePlanningFacts,
}

/// Immutable SQL materialization facts for one admission-frozen IMV target
/// locator. Provider handles, leases, and scan materializations remain in
/// Core's request-local binding store; SQL receives only the copied locator
/// identity and the opaque binding token.
pub struct SqlMvTargetLocatorTableFacts {
    catalog: String,
    namespace: String,
    table: String,
    target_table_uuid: String,
    target_snapshot_id: Option<i64>,
    apply_key_column: String,
    branch_id_column: Option<String>,
    binding: crate::binding::SqlTableBindingId,
}

impl SqlMvTargetLocatorTableFacts {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        catalog: String,
        namespace: String,
        table: String,
        target_table_uuid: String,
        target_snapshot_id: Option<i64>,
        apply_key_column: String,
        branch_id_column: Option<String>,
        binding: crate::binding::SqlTableBindingId,
    ) -> Result<Self, String> {
        for (label, value) in [
            ("catalog", &catalog),
            ("namespace", &namespace),
            ("table", &table),
            ("target table UUID", &target_table_uuid),
            ("apply key column", &apply_key_column),
        ] {
            if value.is_empty() {
                return Err(format!("IMV target locator {label} cannot be empty"));
            }
        }
        if branch_id_column.as_ref().is_some_and(String::is_empty) {
            return Err("IMV target locator branch ID column cannot be empty".to_string());
        }
        Ok(Self {
            catalog,
            namespace,
            table,
            target_table_uuid,
            target_snapshot_id,
            apply_key_column,
            branch_id_column,
            binding,
        })
    }
}

/// Opaque SQL analyzer materialization for an IMV target locator.
pub struct MvTargetLocatorTableMaterialization {
    resolved: crate::catalog::ResolvedAnalyzerTable,
}

impl MvTargetLocatorTableMaterialization {
    pub fn into_resolved_table(self) -> crate::catalog::ResolvedAnalyzerTable {
        self.resolved
    }
}

/// Materialize a frozen IMV target locator without exposing the SQL scan graph
/// to application code.
pub fn materialize_mv_target_locator_table(
    facts: SqlMvTargetLocatorTableFacts,
) -> MvTargetLocatorTableMaterialization {
    use crate::planner::table::{
        ScanSource, SqlMvTargetLocatorScan, SqlScanKind, SqlScanSource, SqlTableIdentity,
    };

    let planner = TableDef {
        name: facts.table.clone(),
        columns: Vec::new(),
        iceberg_row_lineage_metadata_columns: Vec::new(),
        source: ScanSource::Sql(SqlScanSource::new(
            facts.binding,
            SqlTableIdentity {
                catalog: facts.catalog.clone(),
                namespace: facts.namespace.clone(),
                table: facts.table,
            },
            SqlScanKind::MvTargetLocator {
                facts: SqlMvTargetLocatorScan {
                    target_table_uuid: facts.target_table_uuid,
                    target_snapshot_id: facts.target_snapshot_id,
                    apply_key_column: facts.apply_key_column,
                    branch_id_column: facts.branch_id_column,
                },
            },
        )),
    };
    MvTargetLocatorTableMaterialization {
        resolved: crate::catalog::ResolvedAnalyzerTable::from_planner(
            Some(&facts.catalog),
            &facts.namespace,
            planner,
        ),
    }
}

/// Opaque SQL materialization for one admitted connector read.  The embedded
/// analyzer relation is intentionally inaccessible outside this crate.
pub struct ConnectorReadTableMaterialization {
    resolved: crate::catalog::ResolvedAnalyzerTable,
    frozen_snapshot_id: Option<i64>,
}

impl ConnectorReadTableMaterialization {
    pub fn frozen_snapshot_id(&self) -> Option<i64> {
        self.frozen_snapshot_id
    }

    pub fn into_resolved_table(self) -> crate::catalog::ResolvedAnalyzerTable {
        self.resolved
    }
}

/// Copied identity facts from an opaque SQL materialization.
///
/// This is deliberately not a planner table or scan source. Consumers can
/// compare a candidate identity or form a stable digest, but cannot recreate
/// the SQL graph that carried it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlCatalogIdentityFacts {
    catalog: String,
    namespace: String,
    table: String,
}

impl SqlCatalogIdentityFacts {
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

    pub fn matches(&self, catalog: &str, namespace: &str, table: &str) -> bool {
        self.catalog == catalog && self.namespace == namespace && self.table == table
    }
}

/// Copied relation facts required to request connector statistics for an
/// already admitted table. This does not reveal the analyzer relation, scan
/// source, or any provider authority, so it cannot be used to rebuild a SQL
/// graph or reopen a catalog lookup.
#[derive(Clone, Debug, PartialEq)]
pub struct SqlCatalogStatisticsFacts {
    label: String,
    columns: Vec<ColumnDef>,
}

impl SqlCatalogStatisticsFacts {
    pub fn label(&self) -> &str {
        &self.label
    }

    pub fn columns(&self) -> &[ColumnDef] {
        &self.columns
    }
}

/// Materialize immutable connector metadata facts as one SQL analyzer table.
/// The request-local binding is the only route from this table to later scan
/// preparation, so this constructor cannot recreate or replace an admission.
pub fn materialize_connector_read_table(
    facts: ConnectorReadTableFacts,
) -> Result<ConnectorReadTableMaterialization, String> {
    use crate::planner::table::{
        ScanSource, SqlScanKind, SqlScanSource, SqlTableIdentity, SqlTableVersionSelector, TableDef,
    };

    let (scan_kind, frozen_snapshot_id) = match facts.selector {
        novarocks_spi::connector::ConnectorReadSelector::Current => (
            SqlScanKind::Data {
                version: SqlTableVersionSelector::Current,
            },
            None,
        ),
        novarocks_spi::connector::ConnectorReadSelector::SnapshotId(snapshot_id) => (
            SqlScanKind::FrozenInputSet {
                version: SqlTableVersionSelector::Snapshot(snapshot_id),
            },
            Some(snapshot_id),
        ),
        novarocks_spi::connector::ConnectorReadSelector::TimestampMicros(timestamp_micros) => {
            return Err(format!(
                "connector read selector timestamp {timestamp_micros} must resolve to a snapshot before SQL materialization"
            ));
        }
    };
    let ukfk_facts = crate::planner::table::SqlUkFkTableFacts::from_connector_planning_facts(
        &facts.schema,
        &facts.planning_facts,
    );
    let planner = TableDef {
        name: facts.table.clone(),
        columns: facts.columns,
        iceberg_row_lineage_metadata_columns: facts.iceberg_row_lineage_metadata_columns,
        source: ScanSource::Sql(
            SqlScanSource::new(
                facts.binding,
                SqlTableIdentity {
                    catalog: facts.catalog.clone(),
                    namespace: facts.namespace.clone(),
                    table: facts.table,
                },
                scan_kind,
            )
            .with_ukfk_facts(ukfk_facts),
        ),
    };
    Ok(ConnectorReadTableMaterialization {
        resolved: crate::catalog::ResolvedAnalyzerTable::from_planner(
            Some(&facts.catalog),
            &facts.namespace,
            planner,
        ),
        frozen_snapshot_id,
    })
}

/// Attach one application-reserved token to a local SQL relation.
///
/// Local catalog relations have no connector authority, but their scan source
/// must still carry the exact request-local token selected by the application
/// binding store. The planner table and scan source remain SQL-private.
pub fn attach_binding_to_local_materialization(
    mut materialization: crate::catalog::ResolvedAnalyzerTable,
    binding: crate::binding::SqlTableBindingId,
) -> crate::catalog::ResolvedAnalyzerTable {
    let identity = &materialization.catalog.identity;
    materialization.planner.source =
        crate::planner::table::ScanSource::Sql(crate::planner::table::SqlScanSource::new(
            binding,
            crate::planner::table::SqlTableIdentity {
                catalog: identity.catalog.clone(),
                namespace: identity.namespace.clone(),
                table: identity.table.clone(),
            },
            crate::planner::table::SqlScanKind::ConnectorRead,
        ));
    materialization
}

/// Verify that an opaque materialization carries the application-reserved
/// binding token. Any mismatch is a fail-closed admission error.
pub fn validate_materialization_binding(
    materialization: &crate::catalog::ResolvedAnalyzerTable,
    binding: crate::binding::SqlTableBindingId,
) -> Result<(), String> {
    if table_binding_id(materialization) == binding {
        Ok(())
    } else {
        Err(
            "catalog materialization produced a SQL scan with a different request binding"
                .to_string(),
        )
    }
}

/// Copy only catalog identity facts needed by application-side validation and
/// stable digest construction. The analyzer relation and SQL scan graph stay
/// inaccessible outside SQL.
pub fn materialization_identity_facts(
    materialization: &crate::catalog::ResolvedAnalyzerTable,
) -> SqlCatalogIdentityFacts {
    let identity = &materialization.catalog.identity;
    SqlCatalogIdentityFacts {
        catalog: identity.catalog.clone(),
        namespace: identity.namespace.clone(),
        table: identity.table.clone(),
    }
}

/// Copy the only SQL relation facts needed by the application statistics
/// resolver. Connector lease, table handle, version pin, and resolver stay
/// application-owned; planner and analyzer internals remain in SQL.
pub fn materialization_statistics_facts(
    materialization: &crate::catalog::ResolvedAnalyzerTable,
) -> SqlCatalogStatisticsFacts {
    SqlCatalogStatisticsFacts {
        label: materialization.catalog.identity.fqn(),
        columns: materialization.planner.columns.clone(),
    }
}

/// Build the SQL-owned analyzer relation for an already admitted metadata
/// table. Application code supplies only immutable identity/schema facts and
/// the request-local binding token; the planner graph remains internal.
pub fn resolved_metadata_table(
    catalog: &str,
    namespace: &str,
    table: &str,
    metadata_table_type: MetadataTableKind,
    columns: Vec<ColumnDef>,
    iceberg_row_lineage_metadata_columns: Vec<ColumnDef>,
    binding: crate::binding::SqlTableBindingId,
) -> crate::catalog::ResolvedAnalyzerTable {
    use crate::planner::table::{
        ScanSource, SqlScanKind, SqlScanSource, SqlTableIdentity, SqlTableVersionSelector,
    };

    let planner = TableDef {
        name: table.to_string(),
        columns,
        iceberg_row_lineage_metadata_columns,
        source: ScanSource::Sql(SqlScanSource::new(
            binding,
            SqlTableIdentity {
                catalog: catalog.to_string(),
                namespace: namespace.to_string(),
                table: table.to_string(),
            },
            SqlScanKind::Metadata {
                kind: metadata_table_type,
                version: SqlTableVersionSelector::Current,
            },
        )),
    };
    crate::catalog::ResolvedAnalyzerTable::from_planner(Some(catalog), namespace, planner)
}

/// Resolve one local-catalog entry into SQL's opaque analyzer materialization.
/// The application may hold the catalog service, but only SQL may inspect its
/// table definition or create an analyzer relation from it.
pub fn resolve_local_catalog_table(
    catalog: &PlannerMemoryCatalog,
    database: &str,
    table: &str,
) -> Result<crate::catalog::ResolvedAnalyzerTable, String> {
    let planner = catalog.get(database, table)?;
    Ok(crate::catalog::ResolvedAnalyzerTable::from_planner(
        Some("default_catalog"),
        database,
        planner,
    ))
}

/// Read the neutral catalog schema carried by an opaque analyzer
/// materialization. This never exposes the SQL planner table or scan graph.
pub fn catalog_table(materialization: &crate::catalog::ResolvedAnalyzerTable) -> CatalogTable {
    materialization.catalog.clone()
}

/// Read durable local-catalog schema facts without exposing the SQL planner
/// entry used to store them.
pub fn local_catalog_table(
    catalog: &PlannerMemoryCatalog,
    database: &str,
    table: &str,
) -> Result<CatalogTable, String> {
    let planner = catalog.get(database, table)?;
    Ok(local_catalog_table_from_planner(database, planner))
}

/// Return the request-local binding token carried by an opaque analyzer
/// materialization.  Application validation may compare this value, but it
/// cannot inspect or mutate the SQL scan source.
pub fn table_binding_id(
    materialization: &crate::catalog::ResolvedAnalyzerTable,
) -> crate::binding::SqlTableBindingId {
    match &materialization.planner.source {
        crate::planner::table::ScanSource::Sql(source) => source.binding,
    }
}

/// Return the frozen snapshot only when this table is an admitted frozen-input
/// scan. All other SQL scan forms intentionally collapse to `None`.
pub fn frozen_input_snapshot_id(
    materialization: &crate::catalog::ResolvedAnalyzerTable,
) -> Option<i64> {
    match &materialization.planner.source {
        crate::planner::table::ScanSource::Sql(source) => match source.kind {
            crate::planner::table::SqlScanKind::FrozenInputSet {
                version: crate::planner::table::SqlTableVersionSelector::Snapshot(snapshot_id),
            } => Some(snapshot_id),
            _ => None,
        },
    }
}

fn local_catalog_table_from_planner(database: &str, planner: TableDef) -> CatalogTable {
    CatalogTable {
        identity: TableIdentity::new("default_catalog", database, &planner.name),
        columns: planner.columns,
        hidden_columns: planner.iceberg_row_lineage_metadata_columns,
    }
}

/// Register one closed connector-read relation for an application behavior
/// test. The fixture keeps the `TableDef` and scan-source construction inside
/// SQL; callers supply only catalog-visible schema facts.
#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub fn register_test_connector_read_table(
    catalog: &mut PlannerMemoryCatalog,
    database: &str,
    table: &str,
    columns: Vec<ColumnDef>,
) -> Result<(), String> {
    catalog.register(
        database,
        TableDef {
            name: table.to_string(),
            columns,
            iceberg_row_lineage_metadata_columns: Vec::new(),
            source: crate::planner::table::test_sql_scan_source(
                crate::planner::table::SqlScanKind::ConnectorRead,
            ),
        },
    )
}

/// Test-only sealed local table facts for Core behavior tests that validate
/// catalog-visible schema changes. The planner table and scan source remain
/// private to SQL; callers cannot construct or inspect either raw type.
#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub struct SqlTestDeltaTableFacts(TableDef);

#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub fn test_delta_table_facts(
    columns: Vec<ColumnDef>,
    iceberg_row_lineage_metadata_columns: Vec<ColumnDef>,
) -> SqlTestDeltaTableFacts {
    SqlTestDeltaTableFacts(TableDef {
        name: "test_delta_table".to_string(),
        columns,
        iceberg_row_lineage_metadata_columns,
        source: crate::planner::table::test_sql_scan_source(
            crate::planner::table::SqlScanKind::FrozenInputSet {
                version: crate::planner::table::SqlTableVersionSelector::Snapshot(1),
            },
        ),
    })
}

#[cfg(any(test, feature = "test-support"))]
impl SqlTestDeltaTableFacts {
    #[doc(hidden)]
    pub fn columns(&self) -> &[ColumnDef] {
        &self.0.columns
    }

    #[doc(hidden)]
    pub fn iceberg_row_lineage_metadata_columns(&self) -> &[ColumnDef] {
        &self.0.iceberg_row_lineage_metadata_columns
    }

    #[doc(hidden)]
    pub fn push_iceberg_row_lineage_metadata_column(&mut self, column: ColumnDef) {
        self.0.iceberg_row_lineage_metadata_columns.push(column);
    }
}

/// One visible output column of a validated external view definition.
#[derive(Clone, Debug, PartialEq)]
pub struct ViewOutputColumn {
    pub name: String,
    pub data_type: arrow::datatypes::DataType,
    pub nullable: bool,
}

/// Analyze a view query using only the immutable table-provider contract.
/// Catalog application retains the provider and any connector authority.
pub fn analyze_view_query(
    query: &Query,
    provider: &dyn PlannerTableProvider,
    database: &str,
    functions: &dyn crate::compiler::SqlFunctionCatalog,
) -> Result<Vec<ViewOutputColumn>, String> {
    let (resolved, _ctes, _factory) =
        crate::analyzer::analyze_with_function_catalog(query, provider, database, functions)
            .map_err(|error| format!("analyze view definition failed: {error}"))?;
    Ok(resolved
        .output_columns
        .into_iter()
        .filter(|column| !column.is_internal)
        .map(|column| ViewOutputColumn {
            name: column.name,
            data_type: column.data_type,
            nullable: column.nullable,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::sync::Arc;

    use super::*;
    use crate::planner::table::{ScanSource, SqlScanKind};

    fn test_binding_allocator() -> crate::binding::SqlTableBindingAllocator {
        crate::binding::SqlTableBindingAllocator::try_new(
            NonZeroU64::new(17).expect("test scope is nonzero"),
        )
        .expect("test allocator")
    }

    fn connector_read_materialization(
        binding: crate::binding::SqlTableBindingId,
    ) -> crate::catalog::ResolvedAnalyzerTable {
        materialize_connector_read_table(ConnectorReadTableFacts {
            catalog: "ice".to_string(),
            namespace: "analytics".to_string(),
            table: "orders".to_string(),
            columns: vec![ColumnDef {
                name: "order_id".to_string(),
                data_type: arrow::datatypes::DataType::Int64,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: Vec::new(),
            schema: Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("order_id", arrow::datatypes::DataType::Int64, false),
            ])),
            binding,
            selector: novarocks_spi::connector::ConnectorReadSelector::Current,
            planning_facts: novarocks_spi::connector::ConnectorTablePlanningFacts::empty(),
        })
        .expect("materialize connector test facts")
        .into_resolved_table()
    }

    fn parsed_time_travel_clause(sql: &str) -> TableVersion {
        let mut statements = novarocks_parser::parse(sql).expect("parse time travel");
        let [statement] = statements.as_mut_slice() else {
            panic!("expected exactly one statement");
        };
        let Statement::Query(query) = statement else {
            panic!("expected query statement");
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select query");
        };
        let Some(table_with_joins) = select.from.first() else {
            panic!("expected FROM relation");
        };
        let TableFactor::Table {
            version: Some(version),
            ..
        } = &table_with_joins.relation
        else {
            panic!("expected table version clause");
        };
        version.clone()
    }

    fn time_travel_metadata() -> SqlTimeTravelReferenceMetadataFacts {
        SqlTimeTravelReferenceMetadataFacts::try_new(
            vec![10, 20],
            vec![
                SqlTimeTravelSnapshotLogFacts::new(10, 1_700_000_000_000),
                SqlTimeTravelSnapshotLogFacts::new(20, 1_700_000_001_000),
            ],
            vec![
                SqlTimeTravelNamedReferenceFacts::try_new(
                    "dev".to_string(),
                    SqlTimeTravelReferenceKind::Branch,
                    20,
                )
                .expect("valid named reference"),
            ],
            Some(20),
        )
        .expect("valid snapshot facts")
    }

    #[test]
    fn test_catalog_fixture_registers_only_catalog_visible_schema_facts() {
        let mut catalog = PlannerMemoryCatalog::default();
        catalog
            .create_database("analytics")
            .expect("create database");
        register_test_connector_read_table(
            &mut catalog,
            "analytics",
            "orders",
            vec![ColumnDef {
                name: "order_id".to_string(),
                data_type: arrow::datatypes::DataType::Int64,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
        )
        .expect("register sealed connector fixture");
        assert!(catalog.get("analytics", "orders").is_ok());
    }

    #[test]
    fn sqlx4a_catalog_binding_facts_are_opaque_and_fail_closed() {
        let mut allocator = test_binding_allocator();
        let original = allocator.allocate().expect("original binding");
        let replacement = allocator.allocate().expect("replacement binding");
        let materialization = connector_read_materialization(original);

        let identity = materialization_identity_facts(&materialization);
        assert_eq!(identity.fqn(), "ice.analytics.orders");
        assert!(identity.matches("ice", "analytics", "orders"));
        assert!(!identity.matches("ice", "analytics", "customers"));
        validate_materialization_binding(&materialization, original)
            .expect("original binding validates");
        assert!(validate_materialization_binding(&materialization, replacement).is_err());

        let local = attach_binding_to_local_materialization(materialization, replacement);
        validate_materialization_binding(&local, replacement)
            .expect("local materialization carries the replacement token");
        assert!(validate_materialization_binding(&local, original).is_err());
        assert_eq!(materialization_identity_facts(&local), identity);
    }

    #[test]
    fn mv_target_locator_materialization_preserves_frozen_identity_and_binding() {
        let mut allocator = test_binding_allocator();
        let binding = allocator.allocate().expect("target binding");
        let facts = SqlMvTargetLocatorTableFacts::try_new(
            "ice".to_string(),
            "analytics".to_string(),
            "mv_orders".to_string(),
            "target-uuid".to_string(),
            Some(42),
            "__nova_apply_key".to_string(),
            Some("__nova_branch_id".to_string()),
            binding,
        )
        .expect("valid target locator facts");
        let materialization = materialize_mv_target_locator_table(facts).into_resolved_table();

        assert_eq!(
            materialization_identity_facts(&materialization).fqn(),
            "ice.analytics.mv_orders"
        );
        validate_materialization_binding(&materialization, binding)
            .expect("target locator retains frozen binding");
        let ScanSource::Sql(source) = &materialization.planner.source;
        let SqlScanKind::MvTargetLocator { facts } = &source.kind else {
            panic!("target locator must retain its specialized scan kind");
        };
        assert_eq!(facts.target_table_uuid, "target-uuid");
        assert_eq!(facts.target_snapshot_id, Some(42));
        assert_eq!(facts.apply_key_column, "__nova_apply_key");
        assert_eq!(facts.branch_id_column.as_deref(), Some("__nova_branch_id"));
    }

    #[test]
    fn mv_target_locator_facts_fail_closed_for_incomplete_identity() {
        let binding = test_binding_allocator().allocate().expect("target binding");
        for (catalog, namespace, table, uuid, apply_key) in [
            (
                "",
                "analytics",
                "mv_orders",
                "target-uuid",
                "__nova_apply_key",
            ),
            ("ice", "", "mv_orders", "target-uuid", "__nova_apply_key"),
            ("ice", "analytics", "", "target-uuid", "__nova_apply_key"),
            ("ice", "analytics", "mv_orders", "", "__nova_apply_key"),
            ("ice", "analytics", "mv_orders", "target-uuid", ""),
        ] {
            assert!(
                SqlMvTargetLocatorTableFacts::try_new(
                    catalog.to_string(),
                    namespace.to_string(),
                    table.to_string(),
                    uuid.to_string(),
                    None,
                    apply_key.to_string(),
                    None,
                    binding,
                )
                .is_err()
            );
        }
        assert!(
            SqlMvTargetLocatorTableFacts::try_new(
                "ice".to_string(),
                "analytics".to_string(),
                "mv_orders".to_string(),
                "target-uuid".to_string(),
                None,
                "__nova_apply_key".to_string(),
                Some(String::new()),
                binding,
            )
            .is_err()
        );
    }

    #[test]
    fn statistics_facts_copy_only_label_and_columns() {
        let mut allocator = test_binding_allocator();
        let binding = allocator.allocate().expect("binding");
        let materialization = connector_read_materialization(binding);

        let facts = materialization_statistics_facts(&materialization);
        assert_eq!(facts.label(), "ice.analytics.orders");
        assert_eq!(facts.columns().len(), 1);
        assert_eq!(facts.columns()[0].name, "order_id");
    }

    #[test]
    fn time_travel_binding_resolves_parsed_snapshot_and_named_ref() {
        let metadata = time_travel_metadata();

        let snapshot = resolve_time_travel_snapshot_binding(
            &parsed_time_travel_clause("SELECT id FROM t FOR VERSION AS OF 10"),
            &metadata,
            "ice.analytics.t",
        )
        .expect("known snapshot resolves");
        assert_eq!(snapshot.snapshot_id(), 10);

        let named_ref = resolve_time_travel_snapshot_binding(
            &parsed_time_travel_clause("SELECT id FROM t FOR VERSION AS OF 'dev'"),
            &metadata,
            "ice.analytics.t",
        )
        .expect("known named reference resolves");
        assert_eq!(named_ref.snapshot_id(), 20);

        let timestamp = resolve_time_travel_snapshot_binding(
            &parsed_time_travel_clause("SELECT id FROM t FOR SYSTEM_TIME AS OF 1700000000500"),
            &metadata,
            "ice.analytics.t",
        )
        .expect("snapshot log resolves timestamp");
        assert_eq!(timestamp.snapshot_id(), 10);
    }

    #[test]
    fn time_travel_facts_reject_duplicate_and_unknown_snapshot_links() {
        let Err(duplicate) = SqlTimeTravelReferenceMetadataFacts::try_new(
            vec![10, 10],
            Vec::new(),
            Vec::new(),
            None,
        ) else {
            panic!("duplicate snapshots must fail closed");
        };
        assert!(duplicate.contains("duplicate snapshot IDs"));

        let Err(unknown_reference) = SqlTimeTravelReferenceMetadataFacts::try_new(
            vec![10],
            Vec::new(),
            vec![
                SqlTimeTravelNamedReferenceFacts::try_new(
                    "dev".to_string(),
                    SqlTimeTravelReferenceKind::Branch,
                    11,
                )
                .expect("nonempty reference name"),
            ],
            None,
        ) else {
            panic!("unknown named-reference snapshot must fail closed");
        };
        assert!(unknown_reference.contains("unknown snapshot"));

        let Err(unknown_current) = SqlTimeTravelReferenceMetadataFacts::try_new(
            vec![10],
            Vec::new(),
            Vec::new(),
            Some(11),
        ) else {
            panic!("unknown current snapshot must fail closed");
        };
        assert!(unknown_current.contains("current snapshot is not listed"));
    }

    #[test]
    fn time_travel_binding_rejects_unknown_requested_snapshot() {
        let Err(err) = resolve_time_travel_snapshot_binding(
            &parsed_time_travel_clause("SELECT id FROM t FOR VERSION AS OF 999"),
            &time_travel_metadata(),
            "ice.analytics.t",
        ) else {
            panic!("unknown requested snapshot must fail closed");
        };
        assert!(err.message().contains("snapshot 999 not found"));
    }
}
