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

use crate::catalog_application::resolver::{CatalogAdmission, resolve_table_target};
use crate::query_execution::kernels::{DmlExecutionKernel, QueryPreparationKernel};
use novarocks_parser::ast::{Ident, ObjectName as ParserObjectName, Query, Relation, SetExpr};
use novarocks_parser::recursion::GuardedFuture;
use novarocks_spi::connector::{ConnectorReadReferenceFacts, ConnectorReadReferenceKind};
use novarocks_sql::analyze_error::AnalyzeError;
#[cfg(test)]
use novarocks_sql::planning::catalog::SqlTestDeltaTableFacts;
use novarocks_sql::planning::catalog::{
    SqlTimeTravelNamedReferenceFacts, SqlTimeTravelReferenceKind,
    SqlTimeTravelReferenceMetadataFacts, SqlTimeTravelSnapshotLogFacts,
    resolve_time_travel_snapshot_binding,
};
use novarocks_sql::semantic::ObjectName;

type TimeTravelRewriteFuture<'a> =
    GuardedFuture<dyn std::future::Future<Output = Result<(), TimeTravelRewriteError>> + Send + 'a>;

/// Keeps SQL analysis facts distinct from catalog and connector failures until
/// the frontend still has the original statement source for user rendering.
#[derive(Debug)]
pub(crate) enum TimeTravelRewriteError {
    Engine(String),
    Analyze(AnalyzeError),
    Source(novarocks_failure_contract::SharedFailureCause),
}

impl From<crate::catalog_application::resolver::TargetResolutionError> for TimeTravelRewriteError {
    fn from(error: crate::catalog_application::resolver::TargetResolutionError) -> Self {
        match error {
            crate::catalog_application::resolver::TargetResolutionError::Message(message) => {
                Self::Engine(message)
            }
            crate::catalog_application::resolver::TargetResolutionError::Unsupported(error) => {
                Self::Source(novarocks_failure_contract::SharedFailureCause::new(error))
            }
        }
    }
}
impl From<String> for TimeTravelRewriteError {
    fn from(error: String) -> Self {
        Self::Engine(error)
    }
}

impl From<AnalyzeError> for TimeTravelRewriteError {
    fn from(error: AnalyzeError) -> Self {
        Self::Analyze(error)
    }
}

#[cfg(test)]
#[derive(Clone, Debug)]
#[allow(
    dead_code,
    reason = "The time-travel fixture retains optional file facts so cross-module tests can model every overlay shape."
)]
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
#[allow(
    dead_code,
    reason = "The test helper preserves construction of temporary Iceberg delete-file overlays."
)]
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
pub(crate) fn project_iceberg_ref_metadata(
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

/// Returns true if the query contains any `Relation::Table` node with a
/// `version: Some(...)` clause. Used as a cheap pre-check before cloning.
pub fn has_time_travel_refs(query: &Query) -> bool {
    if let Some(with) = &query.with {
        for cte in &with.ctes {
            if has_time_travel_in_query(cte.query.as_ref()) {
                return true;
            }
        }
    }
    has_time_travel_in_set_expr(query.body.as_ref())
}

fn has_time_travel_in_query(query: &Query) -> bool {
    if let Some(with) = &query.with
        && with
            .ctes
            .iter()
            .any(|cte| has_time_travel_in_query(cte.query.as_ref()))
    {
        return true;
    }
    has_time_travel_in_set_expr(query.body.as_ref())
}

fn has_time_travel_in_set_expr(expr: &SetExpr) -> bool {
    match expr {
        SetExpr::Select(select) => select.from.as_ref().is_some_and(has_time_travel_in_factor),
        SetExpr::SetOperation(operation) => {
            operation.inputs.iter().any(has_time_travel_in_set_expr)
        }
        SetExpr::Query(subquery) => {
            let q = &subquery.query;
            has_time_travel_in_query(q)
        }
        _ => false,
    }
}

fn has_time_travel_in_factor(factor: &Relation) -> bool {
    match factor {
        Relation::Table(table) => table.query_period.is_some(),
        Relation::Aliased(alias) => has_time_travel_in_factor(&alias.relation),
        Relation::Join(join) => {
            has_time_travel_in_factor(&join.left) || has_time_travel_in_factor(&join.right)
        }
        Relation::TableSubquery(node) => has_time_travel_in_query(&node.query),
        Relation::Lateral(node) => has_time_travel_in_query(&node.query),
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

/// Walk the query AST in-place and rewrite each `Relation::Table` that has
/// a `version: Some(...)` clause:
///
/// 1. Resolve `version` → `snapshot_id` via `resolve_read_binding`.
/// 2. Encode a synthetic, query-local analyzer identity for that snapshot.
///    the query catalog materializer memoizes the corresponding exact binding and
///    planning pin in its request-local binding store.
/// 3. Rewrite the `Relation::Table`:
///    - Replace `name` with `<catalog>.<namespace>.<synthetic name>` so the
///      provider resolves the overlay rather than shared local catalog state.
///    - Clear `version` (set to `None`).
///    - Preserve any existing alias; if none, set `alias` = original table name
///      so that `SELECT t.col FROM t FOR VERSION AS OF ...` resolves `t.col`.
///
/// Tables without a version clause are left untouched.
pub(crate) fn rewrite_time_travel_refs_async<'a>(
    resolver: &'a (impl TimeTravelResolver + Sync),
    current_catalog: Option<&'a str>,
    current_database: &'a str,
    query: &'a mut Query,
    connector_context: &'a novarocks_spi::connector::ConnectorRequestContext,
) -> TimeTravelRewriteFuture<'a> {
    GuardedFuture::from_pin(Box::pin(async move {
        // Walk CTEs
        if let Some(with) = &mut query.with {
            for cte in &mut with.ctes {
                rewrite_time_travel_in_query_async(
                    resolver,
                    current_catalog,
                    current_database,
                    cte.query.as_mut(),
                    connector_context,
                )
                .await?;
            }
        }
        rewrite_time_travel_in_set_expr_async(
            resolver,
            current_catalog,
            current_database,
            query.body.as_mut(),
            connector_context,
        )
        .await
    }))
}

fn rewrite_time_travel_in_query_async<'a>(
    resolver: &'a (impl TimeTravelResolver + Sync),
    current_catalog: Option<&'a str>,
    current_database: &'a str,
    query: &'a mut Query,
    connector_context: &'a novarocks_spi::connector::ConnectorRequestContext,
) -> TimeTravelRewriteFuture<'a> {
    GuardedFuture::from_pin(Box::pin(async move {
        if let Some(with) = &mut query.with {
            for cte in &mut with.ctes {
                rewrite_time_travel_in_query_async(
                    resolver,
                    current_catalog,
                    current_database,
                    cte.query.as_mut(),
                    connector_context,
                )
                .await?;
            }
        }
        rewrite_time_travel_in_set_expr_async(
            resolver,
            current_catalog,
            current_database,
            query.body.as_mut(),
            connector_context,
        )
        .await
    }))
}

fn rewrite_time_travel_in_set_expr_async<'a>(
    resolver: &'a (impl TimeTravelResolver + Sync),
    current_catalog: Option<&'a str>,
    current_database: &'a str,
    expr: &'a mut SetExpr,
    connector_context: &'a novarocks_spi::connector::ConnectorRequestContext,
) -> TimeTravelRewriteFuture<'a> {
    GuardedFuture::from_pin(Box::pin(async move {
        match expr {
            SetExpr::Select(select) => {
                if let Some(relation) = &mut select.from {
                    rewrite_time_travel_in_factor_async(
                        resolver,
                        current_catalog,
                        current_database,
                        relation,
                        connector_context,
                    )
                    .await?;
                }
                Ok(())
            }
            SetExpr::SetOperation(operation) => {
                for input in &mut operation.inputs {
                    rewrite_time_travel_in_set_expr_async(
                        resolver,
                        current_catalog,
                        current_database,
                        input,
                        connector_context,
                    )
                    .await?;
                }
                Ok(())
            }
            SetExpr::Query(subquery) => {
                let q = &mut subquery.query;
                rewrite_time_travel_in_query_async(
                    resolver,
                    current_catalog,
                    current_database,
                    q.as_mut(),
                    connector_context,
                )
                .await
            }
            _ => Ok(()),
        }
    }))
}

fn rewrite_time_travel_in_factor_async<'a>(
    resolver: &'a (impl TimeTravelResolver + Sync),
    current_catalog: Option<&'a str>,
    current_database: &'a str,
    factor: &'a mut Relation,
    connector_context: &'a novarocks_spi::connector::ConnectorRequestContext,
) -> TimeTravelRewriteFuture<'a> {
    rewrite_time_travel_in_relation_async(
        resolver,
        current_catalog,
        current_database,
        factor,
        false,
        connector_context,
    )
}

// Consume only the real FROM/Table context. The shared semantic view owns
// metadata classification; the raw remainder preserves its original suffix.
fn time_travel_table_lookup(table: &novarocks_parser::ast::Table) -> (Vec<String>, Option<String>) {
    let parts = novarocks_sql::TableNameView::new(&table.name)
        .parts()
        .collect::<Vec<_>>();
    let suffix = parts
        .last()
        .zip(table.name.parts.last())
        .and_then(|(base, raw)| {
            raw.value
                .strip_prefix(*base)
                .filter(|suffix| !suffix.is_empty())
                .map(str::to_owned)
        });
    (
        parts.into_iter().map(str::to_ascii_lowercase).collect(),
        suffix,
    )
}

fn rewrite_time_travel_in_relation_async<'a>(
    resolver: &'a (impl TimeTravelResolver + Sync),
    current_catalog: Option<&'a str>,
    current_database: &'a str,
    factor: &'a mut Relation,
    has_alias: bool,
    connector_context: &'a novarocks_spi::connector::ConnectorRequestContext,
) -> TimeTravelRewriteFuture<'a> {
    GuardedFuture::from_pin(Box::pin(async move {
        match factor {
            Relation::Table(table) if table.query_period.is_some() => {
                if table.branch.is_some() {
                    return Err(
                        "iceberg time travel: branch-qualified relations are not supported"
                            .to_string()
                            .into(),
                    );
                }
                let version_clause = table.query_period.take().expect("checked is_some above");
                let (parts, metadata_suffix) = time_travel_table_lookup(table);
                let name = &mut table.name;

                if parts.is_empty() {
                    return Err("iceberg time travel: table name has no identifier parts"
                        .to_string()
                        .into());
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
                        )
                        .into());
                        }
                    }
                }

                let name_span = name.span;
                let our_name = ObjectName { parts };
                let target =
                    resolve_table_target(resolver, &our_name, current_catalog, current_database)?;

                if target.provider_id.as_str() != "iceberg" {
                    return Err(format!(
                    "iceberg time travel: table '{}' is not an Iceberg table; time travel is only supported for Iceberg",
                    our_name
                        .parts
                        .last()
                        .expect("checked nonempty table name")
                        .as_str()
                )
                .into());
                }

                let fqn = format!("{}.{}.{}", target.catalog, target.namespace, target.table);
                // Time-travel fact resolution is catalog admission.  Resolve all
                // facts from one exact control generation; the synthetic table is
                // subsequently admitted by the query-local materializer.
                let lease = crate::connector::acquire_metadata_planning_pin(
                    resolver.connector_control(),
                    &target.catalog,
                )?;
                let facts =
                    crate::connector::metadata_read_reference_facts_with_planning_pin_async(
                        lease,
                        connector_context.clone(),
                        &target.namespace,
                        &target.table,
                    )
                    .await?;
                let metadata = project_iceberg_ref_metadata(&facts)?;
                let binding =
                    resolve_time_travel_snapshot_binding(&version_clause, &metadata, &fqn)?;
                let snapshot_id = binding.snapshot_id();

                // A time-travel identity is query-local.  The synthetic name is
                // only an analyzer key; its exact snapshot table and planning
                // lease remain in the binding store and never enter the shared
                // in-memory catalog.
                let mut synthetic_table_name =
                    format!("__sqlx1_tt_{}_{}", target.table, snapshot_id);
                if let Some(suffix) = metadata_suffix {
                    // The old AST retained its metadata field while the base was
                    // rewritten. Keep that same classified suffix on the full name.
                    synthetic_table_name.push_str(&suffix);
                }

                // Rewrite the AST node in-place:
                // - Set alias to original table name if user didn't specify one
                // - Replace name with the synthetic name resolved against the target namespace
                // - version is already cleared (we took it above)
                let inferred_alias = (!has_alias).then(|| Ident {
                    value: our_name
                        .parts
                        .last()
                        .expect("checked nonempty table name")
                        .clone(),
                    quoted: false,
                    span: name_span,
                });

                // Route the synthetic analyzer identity through the canonical
                // connector catalog so the query catalog materializer resolves the
                // query-local binding above instead of consulting global state.
                *name = ParserObjectName {
                    parts: vec![target.catalog, target.namespace, synthetic_table_name]
                        .into_iter()
                        .map(|value| Ident {
                            value,
                            quoted: false,
                            span: name_span,
                        })
                        .collect(),
                    span: name_span,
                };

                if let Some(alias) = inferred_alias {
                    // The wrapper owns this real, already rewritten relation.
                    *factor = novarocks_parser::build::alias(factor.clone(), alias, None);
                }

                Ok(())
            }
            Relation::Table(_) => Ok(()),
            Relation::TableSubquery(node) => {
                rewrite_time_travel_in_query_async(
                    resolver,
                    current_catalog,
                    current_database,
                    &mut node.query,
                    connector_context,
                )
                .await
            }
            Relation::Lateral(node) => {
                rewrite_time_travel_in_query_async(
                    resolver,
                    current_catalog,
                    current_database,
                    &mut node.query,
                    connector_context,
                )
                .await
            }
            Relation::Aliased(alias) => {
                rewrite_time_travel_in_relation_async(
                    resolver,
                    current_catalog,
                    current_database,
                    &mut alias.relation,
                    true,
                    connector_context,
                )
                .await
            }
            Relation::Join(join) => {
                rewrite_time_travel_in_relation_async(
                    resolver,
                    current_catalog,
                    current_database,
                    &mut join.left,
                    false,
                    connector_context,
                )
                .await?;
                rewrite_time_travel_in_relation_async(
                    resolver,
                    current_catalog,
                    current_database,
                    &mut join.right,
                    false,
                    connector_context,
                )
                .await
            }
            _ => Ok(()),
        }
    }))
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
    table_facts.push_iceberg_row_lineage_metadata_column(novarocks_types::schema::ColumnDef {
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
    fn time_travel_lookup_preserves_ordinary_identity_and_qualified_metadata_refuses_period() {
        use novarocks_parser::ast::{Relation, SetExpr};
        use novarocks_spi::connector::{ConnectorProviderBinding, ConnectorProviderId};
        use novarocks_sql::compiler::{
            CatalogQualification, CatalogQualifications, SqlCapturedCatalogPreparation,
            SqlCompileControl, SqlCompileError,
        };

        let sql = r#"SELECT * FROM ice.db."Orders$files" FOR VERSION AS OF 42"#;
        let query = novarocks_parser::parser::parse_query(sql).unwrap();
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("SELECT");
        };
        let Some(Relation::Table(table)) = &select.from else {
            panic!("Table");
        };
        let before = table.clone();
        let (parts, suffix) = super::time_travel_table_lookup(table);
        assert_eq!(parts, ["ice", "db", "orders$files"]);
        assert_eq!(suffix, None);
        let authored = table.name.parts.last().unwrap();
        assert_eq!(authored.value, "Orders$files");
        assert!(authored.quoted);
        assert_eq!(
            table, &before,
            "lookup keeps original full name, quote, period and spans"
        );
        let period_span = table.query_period.as_ref().unwrap().span;

        let control = SqlCompileControl::unbounded();
        let Err(SqlCompileError::Compilation(message)) = SqlCapturedCatalogPreparation::try_new(
            query.clone(),
            Some("ice"),
            "db",
            control.clone(),
        ) else {
            panic!(
                "without an Iceberg observation, the complete name goes through ordinary resolution"
            );
        };
        assert_eq!(message, "unsupported identifier `Orders$files`");

        let provider = ConnectorProviderId::parse("iceberg").unwrap();
        let binding = ConnectorProviderBinding::try_new(
            provider.clone(),
            "ice",
            [73; 16],
            "time-travel-test",
        )
        .unwrap()
        .binding_key()
        .clone();
        let mut qualifications = CatalogQualifications::default();
        qualifications
            .record(
                binding.instance_id.clone(),
                CatalogQualification::Ready { provider, binding },
            )
            .unwrap();
        let Err(SqlCompileError::Analyze(error)) =
            SqlCapturedCatalogPreparation::try_new_with_qualifications(
                query,
                Some("ice"),
                "db",
                control,
                qualifications,
            )
        else {
            panic!("qualified metadata must reject time travel at the original period");
        };
        assert_eq!(
            error.kind(),
            novarocks_sql::analyze_error::AnalyzeErrorKind::UnsupportedQueryShape
        );
        assert_eq!(
            error.message(),
            "time travel is not supported on metadata relations"
        );
        assert_eq!(error.span(), Some(period_span));
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
                novarocks_types::schema::ColumnDef {
                    name: "_file".to_string(),
                    data_type: arrow::datatypes::DataType::Utf8,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                novarocks_types::schema::ColumnDef {
                    name: "_pos".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                novarocks_types::schema::ColumnDef {
                    name: "_row_id".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                novarocks_types::schema::ColumnDef {
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
