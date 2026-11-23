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

use std::collections::HashMap;

use arrow::datatypes::SchemaRef;

use crate::analysis::OutputColumn;
use crate::binding::SqlTableBindingId;
use crate::catalog::ResolvedAnalyzerTable;
use crate::column_id::ColumnRefFactory;
use crate::planner::payload::PlanScanNode;
pub use crate::planner::payload::SqlScanOccurrence;

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
    pub fn with_predicates(mut self, predicates: Vec<crate::analysis::TypedExpr>) -> Self {
        let crate::planner::physical::PhysicalPlanKind::Scan(scan) = &mut self.0.kind else {
            unreachable!("frozen connector scan plan is constructed as one scan")
        };
        scan.predicates = predicates;
        self
    }

    /// Address this scan by the occurrence its frozen read was accounted for
    /// under, and hand back the physical tree.
    pub(crate) fn finalize_provider_read_occurrence(
        mut self,
        occurrence: novarocks_physical_plan::ProviderReadOccurrenceId,
    ) -> Result<crate::planner::physical::PhysicalPlanNode, String> {
        let crate::planner::physical::PhysicalPlanKind::Scan(scan) = self.0.kind else {
            unreachable!("frozen connector scan plan is constructed as one scan")
        };
        self.0.kind = crate::planner::physical::PhysicalPlanKind::Scan(
            scan.finalize_provider_read_occurrence(occurrence)?,
        );
        Ok(self.0)
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
    columns: Vec<novarocks_types::schema::ColumnDef>,
    binding: SqlTableBindingId,
) -> ResolvedAnalyzerTable {
    synthetic_resolved_analyzer_table_from_columns(
        identity,
        columns,
        binding,
        crate::planner::table::SqlScanKind::PinnedFileSet,
    )
}

fn synthetic_resolved_analyzer_table(
    identity: &FrozenConnectorScanIdentity,
    input_schema: SchemaRef,
    binding: SqlTableBindingId,
    kind: crate::planner::table::SqlScanKind,
) -> ResolvedAnalyzerTable {
    let columns = column_defs(&input_schema);
    synthetic_resolved_analyzer_table_from_columns(identity, columns, binding, kind)
}

fn synthetic_resolved_analyzer_table_from_columns(
    identity: &FrozenConnectorScanIdentity,
    columns: Vec<novarocks_types::schema::ColumnDef>,
    binding: SqlTableBindingId,
    kind: crate::planner::table::SqlScanKind,
) -> ResolvedAnalyzerTable {
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
    input_columns: &[novarocks_types::schema::ColumnDef],
    binding: SqlTableBindingId,
) -> Result<FrozenConnectorScanPlan, String> {
    build_synthetic_scan_plan_from_columns(
        identity,
        input_columns,
        binding,
        crate::planner::table::SqlScanKind::PinnedFileSet,
    )
}

fn build_synthetic_scan_plan(
    identity: &FrozenConnectorScanIdentity,
    input_schema: &SchemaRef,
    binding: SqlTableBindingId,
    kind: crate::planner::table::SqlScanKind,
) -> FrozenConnectorScanPlan {
    let mut factory = ColumnRefFactory::new();
    let output_columns = input_schema
        .fields()
        .iter()
        .map(|field| {
            let value_type = novarocks_type_contract::FunctionValueType::new(
                field.data_type().clone(),
                field.is_nullable(),
            );
            OutputColumn {
                column_id: factory.create(None, field.name().clone(), value_type.clone()),
                name: field.name().clone(),
                value_type,
                is_internal: false,
            }
        })
        .collect();
    finish_synthetic_scan_plan(
        identity,
        column_defs(input_schema),
        output_columns,
        binding,
        kind,
    )
}

fn build_synthetic_scan_plan_from_columns(
    identity: &FrozenConnectorScanIdentity,
    input_columns: &[novarocks_types::schema::ColumnDef],
    binding: SqlTableBindingId,
    kind: crate::planner::table::SqlScanKind,
) -> Result<FrozenConnectorScanPlan, String> {
    let mut factory = ColumnRefFactory::new();
    let mut output_columns = Vec::with_capacity(input_columns.len());
    for column in input_columns {
        let value_type = column
            .declared_value_type()
            .map_err(|error| error.to_string())?;
        output_columns.push(OutputColumn {
            column_id: factory.create(None, column.name.clone(), value_type.clone()),
            name: column.name.clone(),
            value_type,
            is_internal: false,
        });
    }
    Ok(finish_synthetic_scan_plan(
        identity,
        input_columns.to_vec(),
        output_columns,
        binding,
        kind,
    ))
}

fn finish_synthetic_scan_plan(
    identity: &FrozenConnectorScanIdentity,
    columns: Vec<novarocks_types::schema::ColumnDef>,
    output_columns: Vec<OutputColumn>,
    binding: SqlTableBindingId,
    kind: crate::planner::table::SqlScanKind,
) -> FrozenConnectorScanPlan {
    let table = crate::planner::table::TableDef {
        name: identity.table().to_string(),
        columns,
        iceberg_row_lineage_metadata_columns: Vec::new(),
        source: crate::planner::table::ScanSource::Sql(crate::planner::table::SqlScanSource::new(
            binding,
            identity.planner_identity(),
            kind,
        )),
    };
    FrozenConnectorScanPlan(crate::planner::physical::PhysicalPlanNode {
        kind: crate::planner::physical::PhysicalPlanKind::Scan(
            PlanScanNode {
                database: identity.namespace().to_string(),
                table,
                alias: None,
                columns: output_columns.clone(),
                predicates: Vec::new(),
                required_columns: None,
                variant_columns: Vec::new(),
                mv_rewritten_from: None,
            }
            .into(),
        ),
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

fn matches_synthetic_scan(
    scan: &PlanScanNode,
    binding: SqlTableBindingId,
    identity: &FrozenConnectorScanIdentity,
    kind: &crate::planner::table::SqlScanKind,
) -> bool {
    let crate::planner::table::ScanSource::Sql(source) = &scan.table.source;
    &source.kind == kind && source.binding == binding && source.table == identity.planner_identity()
}
