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

//! Synthetic/local query preparation for time-travel, delta scans, MV helpers,
//! statement schema lookup, and catalog-service table invalidation.
//! Ordinary SELECT external tables resolve through the query catalog materializer.

use crate::catalog_application::query_catalog::CatalogServiceSource;
use crate::catalog_application::resolver::{CatalogAdmission, resolve_table_target};
use crate::query_execution::kernels::{
    DmlExecutionKernel, MvExecutionKernel, QueryPreparationKernel,
};
use novarocks_catalog::schema::ColumnDef;
use novarocks_spi::connector::{ConnectorReadReferenceFacts, ConnectorReadReferenceKind};
#[cfg(test)]
use novarocks_sql::planning::catalog::SqlTestDeltaTableFacts;
use novarocks_sql::planning::catalog::{
    SqlTimeTravelNamedReferenceFacts, SqlTimeTravelReferenceKind,
    SqlTimeTravelReferenceMetadataFacts, SqlTimeTravelSnapshotLogFacts,
    resolve_time_travel_snapshot_binding,
};
use novarocks_sql::syntax::ObjectName;

#[cfg(test)]
#[derive(Clone, Debug)]
pub(crate) struct IcebergFileForQuery {
    pub(crate) path: String,
    pub(crate) size: i64,
    pub(crate) record_count: Option<i64>,
    pub(crate) partition_spec_id: Option<i32>,
    pub(crate) partition_key: Option<String>,
    pub(crate) first_row_id: Option<i64>,
    pub(crate) data_sequence_number: Option<i64>,
    pub(crate) change_op: Option<i8>,
    pub(crate) row_id_allow_list: Option<std::collections::BTreeSet<i64>>,
}

#[cfg(test)]
pub(crate) fn delete_temp_iceberg_file_for_query(
    path: String,
    size: i64,
    record_count: Option<i64>,
    change_op: Option<i8>,
) -> IcebergFileForQuery {
    IcebergFileForQuery {
        path,
        size,
        record_count,
        partition_spec_id: None,
        partition_key: None,
        first_row_id: None,
        data_sequence_number: None,
        change_op,
        row_id_allow_list: None,
    }
}

/// Project provider metadata into the immutable facts required by SQL
/// time-travel analysis.  This conversion is intentionally application-owned:
/// the compiler never receives an Iceberg `TableMetadata` object.
fn project_iceberg_ref_metadata(
    facts: &ConnectorReadReferenceFacts,
) -> Result<SqlTimeTravelReferenceMetadataFacts, String> {
    let refs = facts
        .named_references()
        .iter()
        .map(|reference| {
            let kind = match reference.kind {
                ConnectorReadReferenceKind::Branch => SqlTimeTravelReferenceKind::Branch,
                ConnectorReadReferenceKind::Tag => SqlTimeTravelReferenceKind::Tag,
            };
            SqlTimeTravelNamedReferenceFacts::try_new(
                reference.name.to_string(),
                kind,
                reference.snapshot_id,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    SqlTimeTravelReferenceMetadataFacts::try_new(
        facts.snapshot_ids().to_vec(),
        facts
            .snapshot_log()
            .iter()
            .map(|entry| {
                SqlTimeTravelSnapshotLogFacts::new(entry.snapshot_id, entry.timestamp_millis)
            })
            .collect(),
        refs,
        facts.current_snapshot_id(),
    )
}

// ---------------------------------------------------------------------------
// Time-travel (FOR VERSION/TIMESTAMP AS OF) AST rewrite
// ---------------------------------------------------------------------------

/// Returns true if the query contains any `TableFactor::Table` node with a
/// `version: Some(...)` clause. Used as a cheap pre-check before cloning.
pub fn has_time_travel_refs(query: &sqlparser::ast::Query) -> bool {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            if has_time_travel_in_set_expr(cte.query.body.as_ref()) {
                return true;
            }
        }
    }
    has_time_travel_in_set_expr(query.body.as_ref())
}

fn has_time_travel_in_set_expr(expr: &sqlparser::ast::SetExpr) -> bool {
    match expr {
        sqlparser::ast::SetExpr::Select(select) => {
            for tw in &select.from {
                if has_time_travel_in_factor(&tw.relation) {
                    return true;
                }
                for join in &tw.joins {
                    if has_time_travel_in_factor(&join.relation) {
                        return true;
                    }
                }
            }
            false
        }
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            has_time_travel_in_set_expr(left) || has_time_travel_in_set_expr(right)
        }
        sqlparser::ast::SetExpr::Query(q) => has_time_travel_in_set_expr(q.body.as_ref()),
        _ => false,
    }
}

fn has_time_travel_in_factor(factor: &sqlparser::ast::TableFactor) -> bool {
    match factor {
        sqlparser::ast::TableFactor::Table { version, .. } => version.is_some(),
        sqlparser::ast::TableFactor::Derived { subquery, .. } => {
            has_time_travel_in_set_expr(subquery.body.as_ref())
        }
        _ => false,
    }
}

/// The exact leaf ports required to rewrite `FOR VERSION/TIMESTAMP AS OF`.
///
/// This deliberately omits query execution, statistics and any application
/// aggregate.  A caller can therefore use the same rewriter from query, DML
/// or MV preparation without recovering an application facade.
pub(crate) trait TimeTravelResolver: CatalogAdmission {
    fn connector_control(&self) -> &dyn novarocks_spi::connector::ConnectorControlResolver;
}

macro_rules! impl_kernel_time_travel_resolver {
    ($kernel:ty) => {
        impl TimeTravelResolver for $kernel {
            fn connector_control(&self) -> &dyn novarocks_spi::connector::ConnectorControlResolver {
                self.connector_control().as_ref()
            }
        }
    };
}

impl_kernel_time_travel_resolver!(QueryPreparationKernel);
impl_kernel_time_travel_resolver!(DmlExecutionKernel);
impl_kernel_time_travel_resolver!(MvExecutionKernel);

/// Walk the query AST in-place and rewrite each `TableFactor::Table` that has
/// a `version: Some(...)` clause:
///
/// 1. Resolve `version` → `snapshot_id` via `resolve_read_binding`.
/// 2. Encode a synthetic, query-local analyzer identity for that snapshot.
///    the query catalog materializer memoizes the corresponding exact binding and
///    planning lease in its request-local binding store.
/// 3. Rewrite the `TableFactor::Table`:
///    - Replace `name` with `<catalog>.<namespace>.<synthetic name>` so the
///      provider resolves the overlay rather than shared local catalog state.
///    - Clear `version` (set to `None`).
///    - Preserve any existing alias; if none, set `alias` = original table name
///      so that `SELECT t.col FROM t FOR VERSION AS OF ...` resolves `t.col`.
///
/// Tables without a version clause are left untouched.
pub fn rewrite_time_travel_refs(
    resolver: &impl TimeTravelResolver,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &mut sqlparser::ast::Query,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), String> {
    // Walk CTEs
    if let Some(with) = &mut query.with {
        for cte in &mut with.cte_tables {
            rewrite_time_travel_in_set_expr(
                resolver,
                current_catalog,
                current_database,
                cte.query.body.as_mut(),
                connector_context,
            )?;
        }
    }
    rewrite_time_travel_in_set_expr(
        resolver,
        current_catalog,
        current_database,
        query.body.as_mut(),
        connector_context,
    )
}

fn rewrite_time_travel_in_set_expr(
    resolver: &impl TimeTravelResolver,
    current_catalog: Option<&str>,
    current_database: &str,
    expr: &mut sqlparser::ast::SetExpr,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), String> {
    match expr {
        sqlparser::ast::SetExpr::Select(select) => {
            for tw in &mut select.from {
                rewrite_time_travel_in_factor(
                    resolver,
                    current_catalog,
                    current_database,
                    &mut tw.relation,
                    connector_context,
                )?;
                for join in &mut tw.joins {
                    rewrite_time_travel_in_factor(
                        resolver,
                        current_catalog,
                        current_database,
                        &mut join.relation,
                        connector_context,
                    )?;
                }
            }
            Ok(())
        }
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            rewrite_time_travel_in_set_expr(
                resolver,
                current_catalog,
                current_database,
                left.as_mut(),
                connector_context,
            )?;
            rewrite_time_travel_in_set_expr(
                resolver,
                current_catalog,
                current_database,
                right.as_mut(),
                connector_context,
            )
        }
        sqlparser::ast::SetExpr::Query(q) => rewrite_time_travel_in_set_expr(
            resolver,
            current_catalog,
            current_database,
            q.body.as_mut(),
            connector_context,
        ),
        _ => Ok(()),
    }
}

fn rewrite_time_travel_in_factor(
    resolver: &impl TimeTravelResolver,
    current_catalog: Option<&str>,
    current_database: &str,
    factor: &mut sqlparser::ast::TableFactor,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), String> {
    match factor {
        sqlparser::ast::TableFactor::Table {
            name,
            version,
            alias,
            ..
        } if version.is_some() => {
            let version_clause = version.take().expect("checked is_some above");

            // Extract name parts for our ObjectName lookup
            let parts: Vec<String> = name
                .0
                .iter()
                .filter_map(|p| match p {
                    sqlparser::ast::ObjectNamePart::Identifier(ident) => {
                        Some(ident.value.to_ascii_lowercase())
                    }
                    _ => None,
                })
                .collect();

            if parts.is_empty() {
                return Err("iceberg time travel: table name has no identifier parts".to_string());
            }

            // Reject the combination of branch/tag suffix with FOR VERSION/TIMESTAMP AS OF.
            if let Some(last) = parts.last() {
                for prefix in &["branch_", "tag_"] {
                    if let Some(ref_name) = last.strip_prefix(prefix)
                        && !ref_name.is_empty()
                    {
                        return Err(format!(
                            "iceberg ref: branch suffix '.{}_{}' conflicts with FOR VERSION AS OF clause",
                            prefix.trim_end_matches('_'),
                            ref_name,
                        ));
                    }
                }
            }

            let our_name = ObjectName { parts };
            let target =
                resolve_table_target(resolver, &our_name, current_catalog, current_database)?;

            if target.backend_name != "iceberg" {
                return Err(format!(
                    "iceberg time travel: table '{}' is not an Iceberg table; time travel is only supported for Iceberg",
                    our_name.parts.last().expect("checked nonempty table name")
                ));
            }

            let fqn = format!("{}.{}.{}", target.catalog, target.namespace, target.table);
            // Time-travel fact resolution is catalog admission.  Resolve all
            // facts from one exact control generation; the synthetic table is
            // subsequently admitted by the query-local materializer.
            let lease = crate::connector::acquire_metadata_planning_lease(
                resolver.connector_control(),
                &target.catalog,
            )?;
            let facts = crate::connector::metadata_read_reference_facts_with_planning_lease(
                lease,
                connector_context.clone(),
                &target.namespace,
                &target.table,
            )?;
            let metadata = project_iceberg_ref_metadata(&facts)?;
            let binding = resolve_time_travel_snapshot_binding(&version_clause, &metadata, &fqn)?;
            let snapshot_id = binding.snapshot_id();

            // A time-travel identity is query-local.  The synthetic name is
            // only an analyzer key; its exact snapshot table and planning
            // lease remain in the binding store and never enter the shared
            // in-memory catalog.
            let synthetic_table_name = format!("__sqlx1_tt_{}_{}", target.table, snapshot_id);

            // Rewrite the AST node in-place:
            // - Set alias to original table name if user didn't specify one
            // - Replace name with the synthetic name resolved against the target namespace
            // - version is already cleared (we took it above)
            if alias.is_none() {
                // Infer the original table alias from the last non-catalog part of the name
                let original_leaf = our_name
                    .parts
                    .last()
                    .expect("checked nonempty table name")
                    .to_string();
                *alias = Some(sqlparser::ast::TableAlias {
                    name: sqlparser::ast::Ident::new(original_leaf),
                    columns: vec![],
                    explicit: false,
                });
            }

            // Route the synthetic analyzer identity through the canonical
            // connector catalog so the query catalog materializer resolves the
            // query-local binding above instead of consulting global state.
            *name = sqlparser::ast::ObjectName(vec![
                sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::new(
                    target.catalog.clone(),
                )),
                sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::new(
                    target.namespace.clone(),
                )),
                sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::new(
                    synthetic_table_name,
                )),
            ]);

            Ok(())
        }
        sqlparser::ast::TableFactor::Table { .. } => Ok(()),
        sqlparser::ast::TableFactor::Derived { subquery, .. } => rewrite_time_travel_in_set_expr(
            resolver,
            current_catalog,
            current_database,
            subquery.body.as_mut(),
            connector_context,
        ),
        _ => Ok(()),
    }
}

/// Resolve statement-level connector schema facts without registering a
/// concrete scan in the shared local catalog.  `ANALYZE` only needs columns;
/// it must not leave a provider table or file carrier visible to a later SQL
/// request.
pub(crate) fn external_schema_columns_for_statement(
    resolver: &impl TimeTravelResolver,
    current_catalog: Option<&str>,
    current_database: &str,
    name: &ObjectName,
) -> Result<Option<Vec<ColumnDef>>, String> {
    let target = resolve_table_target(resolver, name, current_catalog, current_database)?;
    if target.backend_name != "iceberg" {
        // Non-Iceberg sources are already represented in the local catalog.
        return Ok(None);
    }
    // Time-travel identities live only in a query binding store and are never
    // valid statement-level catalog objects.
    if is_synthetic_time_travel_table(&target.table) {
        return Err("time-travel identities are not valid ANALYZE targets".to_string());
    }

    let materialization =
        crate::query_execution::planning::catalog_materializer::load_connector_table_materialization_with_lease(
            resolver.connector_control(),
            crate::connector::connector_request_context(
                None,
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            )?,
            &target.catalog,
            &target.namespace,
            &target.table,
        )
        .map_err(|err| {
            format!(
                "load iceberg table {}.{}.{} failed: {err}",
                target.catalog, target.namespace, target.table
            )
        })?;
    Ok(Some(materialization.columns))
}

/// Returns true if `table_name` was produced by a time-travel rewriter.
/// SQLX-1 query-local overlays use `__sqlx1_tt_<table>_<snapshot_id>`;
/// recognize the legacy form as well so old statement materialization cannot
/// mistake either identity for a durable catalog object.
fn is_synthetic_time_travel_table(table_name: &str) -> bool {
    if let Some(encoded) = table_name.strip_prefix("__sqlx1_tt_")
        && let Some((base, snapshot)) = encoded.rsplit_once('_')
    {
        return !base.is_empty() && snapshot.parse::<i64>().is_ok();
    }
    if let Some(at_pos) = table_name.rfind("__at_") {
        let suffix = &table_name[at_pos + "__at_".len()..];
        !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit() || c == '-')
    } else {
        false
    }
}

/// Remove a durable local catalog relation after DDL replaces or drops it.
/// Query-local overlays never call this helper: they are scoped to their
/// binding store and are not registered in the shared catalog in the first
/// place.
pub(crate) fn drop_local_table_registration_if_exists(
    source: &impl CatalogServiceSource,
    namespace: &str,
    table: &str,
) -> Result<(), String> {
    let mut guard = source
        .catalog_service()
        .local()
        .write()
        .map_err(|error| format!("standalone catalog write lock: {error}"))?;
    match guard.drop_table(namespace, table) {
        Ok(()) => Ok(()),
        Err(error) if error.contains("unknown") => Ok(()),
        Err(error) => Err(format!("drop local table metadata: {error}")),
    }
}

#[cfg(test)]
fn validate_delta_file_change_ops(data_files: &[IcebergFileForQuery]) -> Result<Vec<i8>, String> {
    data_files
        .iter()
        .enumerate()
        .map(|(idx, file)| {
            let op = file.change_op.ok_or_else(|| {
                format!(
                    "iceberg delta source file {} ({}) missing {}",
                    idx,
                    file.path,
                    novarocks_execution::exec::change_op::CHANGE_OP_COLUMN
                )
            })?;
            novarocks_execution::exec::change_op::validate_change_op_value(op)?;
            Ok(op)
        })
        .collect()
}

/// Test-only stand-in for the provider-owned frozen data-file rows that
/// `stamp_delta_table_def_change_ops` writes into. Core must not name a
/// provider crate type here, and the stamping contract only depends on the
/// frozen row count plus each row's change-op slot, so this minimal shape is
/// the whole surface under test.
#[cfg(test)]
struct FrozenFileChangeOp {
    ivm_change_op: Option<i8>,
}

#[cfg(test)]
fn stamp_delta_table_def_change_ops(
    table_facts: &mut SqlTestDeltaTableFacts,
    files: &mut [FrozenFileChangeOp],
    change_ops: &[i8],
) -> Result<(), String> {
    if table_facts.columns().iter().any(|col| {
        col.name
            .eq_ignore_ascii_case(novarocks_execution::exec::change_op::CHANGE_OP_COLUMN)
    }) {
        return Err(format!(
            "iceberg delta source base table already has reserved column {}",
            novarocks_execution::exec::change_op::CHANGE_OP_COLUMN
        ));
    }
    if table_facts
        .iceberg_row_lineage_metadata_columns()
        .iter()
        .any(|col| {
            col.name
                .eq_ignore_ascii_case(novarocks_execution::exec::change_op::CHANGE_OP_COLUMN)
        })
    {
        return Err(format!(
            "iceberg delta source metadata already contains reserved column {}",
            novarocks_execution::exec::change_op::CHANGE_OP_COLUMN
        ));
    }

    let field = novarocks_execution::exec::change_op::change_op_field();
    table_facts.push_iceberg_row_lineage_metadata_column(ColumnDef {
        name: field.name().clone(),
        data_type: field.data_type().clone(),
        nullable: field.is_nullable(),
        write_default: None,
        logical_type: None,
    });

    if files.len() != change_ops.len() {
        return Err(format!(
            "iceberg delta source file count mismatch: frozen input has {}, input has {}",
            files.len(),
            change_ops.len()
        ));
    }
    for (file, op) in files.iter_mut().zip(change_ops.iter().copied()) {
        file.ivm_change_op = Some(op);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::query_execution::planning::time_travel::IcebergFileForQuery;
    use novarocks_sql::planning::catalog::test_delta_table_facts;

    fn test_data_file() -> super::FrozenFileChangeOp {
        super::FrozenFileChangeOp {
            ivm_change_op: None,
        }
    }

    fn file(change_op: Option<i8>) -> IcebergFileForQuery {
        IcebergFileForQuery {
            path: "file:///tmp/data.parquet".to_string(),
            size: 10,
            record_count: Some(1),
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: None,
            change_op,
            row_id_allow_list: None,
        }
    }

    #[test]
    fn delta_table_builder_rejects_untagged_file() {
        let err = super::validate_delta_file_change_ops(&[file(None)])
            .expect_err("untagged delta file must fail");

        assert!(err.contains("__change_op"));
        assert!(err.contains("missing"));
    }

    #[test]
    fn delta_table_builder_rejects_invalid_change_op() {
        let err = super::validate_delta_file_change_ops(&[file(Some(0))])
            .expect_err("invalid delta file must fail");

        assert!(err.contains("__change_op"));
        assert!(err.contains("invalid value 0"));
    }

    #[test]
    fn delta_table_builder_stamps_s3_files_and_adds_virtual_column() {
        let mut table_facts = test_delta_table_facts(vec![], vec![]);
        let mut files = vec![test_data_file()];

        super::stamp_delta_table_def_change_ops(&mut table_facts, &mut files, &[1]).expect("stamp");

        assert_eq!(
            table_facts
                .iceberg_row_lineage_metadata_columns()
                .iter()
                .map(|col| (col.name.as_str(), &col.data_type, col.nullable))
                .collect::<Vec<_>>(),
            vec![("__change_op", &arrow::datatypes::DataType::Int8, false)]
        );
        assert_eq!(files[0].ivm_change_op, Some(1));
    }

    #[test]
    fn delta_table_builder_preserves_row_lineage_metadata_and_adds_change_op() {
        let mut table_facts = test_delta_table_facts(
            vec![],
            vec![
                novarocks_catalog::schema::ColumnDef {
                    name: "_file".to_string(),
                    data_type: arrow::datatypes::DataType::Utf8,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                novarocks_catalog::schema::ColumnDef {
                    name: "_pos".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                novarocks_catalog::schema::ColumnDef {
                    name: "_row_id".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                novarocks_catalog::schema::ColumnDef {
                    name: "_last_updated_sequence_number".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
            ],
        );
        let mut files = vec![test_data_file()];

        super::stamp_delta_table_def_change_ops(&mut table_facts, &mut files, &[-1])
            .expect("stamp");

        assert_eq!(
            table_facts
                .iceberg_row_lineage_metadata_columns()
                .iter()
                .map(|col| (col.name.as_str(), &col.data_type, col.nullable))
                .collect::<Vec<_>>(),
            vec![
                ("_file", &arrow::datatypes::DataType::Utf8, false),
                ("_pos", &arrow::datatypes::DataType::Int64, false),
                ("_row_id", &arrow::datatypes::DataType::Int64, false),
                (
                    "_last_updated_sequence_number",
                    &arrow::datatypes::DataType::Int64,
                    false,
                ),
                ("__change_op", &arrow::datatypes::DataType::Int8, false),
            ]
        );
        assert_eq!(files[0].ivm_change_op, Some(-1));
    }

    #[test]
    fn delta_table_builder_accepts_empty_iceberg_storage() {
        // The IVM-A1 delta source `stamp_delta_table_def_change_ops`
        // requires the base table to be backed by an admitted connector read
        // handle. An empty connector snapshot legitimately produces an opaque
        // connector read whose split plan is empty;
        // ensure that path round-trips correctly when stamping with an
        // empty change-op slice.
        let mut table_facts = test_delta_table_facts(vec![], vec![]);
        let mut files = Vec::new();

        super::stamp_delta_table_def_change_ops(&mut table_facts, &mut files, &[])
            .expect("stamp empty delta over empty iceberg storage");

        assert_eq!(
            table_facts
                .iceberg_row_lineage_metadata_columns()
                .iter()
                .map(|col| (col.name.as_str(), &col.data_type, col.nullable))
                .collect::<Vec<_>>(),
            vec![("__change_op", &arrow::datatypes::DataType::Int8, false)]
        );
        assert!(files.is_empty());
    }
}
