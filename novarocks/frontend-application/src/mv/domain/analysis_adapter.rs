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

//! Stateful materialized-view analysis and display adapter.

use arrow::datatypes::DataType;

use crate::mv::domain::analysis::{MvAnalysis, prepare_mv_select_for_catalog_provider};
use crate::mv::domain::application::MvShowStatement;
use crate::mv::domain::lifecycle::MvListRow;
use crate::mv::domain::model::MvStorageEngine;
use novarocks_mv_application::persistence::codec::{ConfigurationDocument, RefreshPolicy};
use novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection;
use novarocks_mv_application::readiness::MvListedManageability;
use novarocks_query_application::api::QueryResult;

/// Lightweight projection of the iceberg base table that
/// `validate_ivm_primary_key` needs. Built once at the top of `create_mv`
/// from the loaded iceberg table; passing this struct keeps validation
/// pure and easy to unit-test.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BaseColumnDescriptor {
    pub name: String,
    pub data_type: DataType,
    /// Uppercased SQL type as the analyzer/iceberg-schema mapper produced
    /// it (e.g. `BIGINT`, `STRING`, `DECIMAL(18,2)`, `ARRAY<STRING>`).
    pub sql_type: String,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BaseTableDescriptor {
    pub format_version: i32,
    pub columns: Vec<BaseColumnDescriptor>,
}

/// Validate that a parsed `PRIMARY KEY (col, ...)` clause on a CREATE
/// MATERIALIZED VIEW statement satisfies the IVM Phase-2 contract:
///
/// 1. The base table is iceberg format-version 2.
/// 2. Every PK column exists on the base table.
/// 3. Every PK column is NOT NULL on the base table.
/// 4. Every PK column has a hashable scalar type.
///
/// Errors fail fast in declared column order — the first mismatch wins.
/// Returns `Ok(())` on success and discards the PK list (PR-1 does not
/// persist it; PR-3 will).
pub(crate) fn validate_ivm_primary_key(
    pk_columns: &[String],
    base: &BaseTableDescriptor,
) -> Result<(), String> {
    // Messages are byte-identical to the provider's ChangeError Display, which
    // this used to borrow purely to render them; the only caller already
    // discarded the typed error via to_string().
    if base.format_version != 2 && base.format_version != 3 {
        return Err(format!(
            "iceberg base table format-version {} is not supported; IVM requires v2 or v3",
            base.format_version
        ));
    }
    for pk in pk_columns {
        let col = base
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(pk))
            .ok_or_else(|| {
                format!("PRIMARY KEY column `{pk}` does not exist on the iceberg base table")
            })?;
        if col.nullable {
            return Err(format!(
                "PRIMARY KEY column `{}` must be NOT NULL on the iceberg base table",
                col.name
            ));
        }
        if !is_hashable_pk_type(&col.sql_type) {
            return Err(format!(
                "PRIMARY KEY column `{}` has unsupported type `{}`; only hashable scalar types are allowed",
                col.name, col.sql_type
            ));
        }
    }
    Ok(())
}

/// Hashable scalar-type predicate for IVM Phase-2 PRIMARY KEY columns.
/// Accepts: BIGINT, INT, SMALLINT, TINYINT, STRING, VARCHAR, DATE,
/// DATETIME, DECIMAL (with or without precision/scale).
/// Rejects: BOOLEAN, FLOAT, DOUBLE, ARRAY, MAP, STRUCT, JSON.
fn is_hashable_pk_type(sql_type: &str) -> bool {
    let upper = sql_type.to_ascii_uppercase();
    let head = upper.split(['(', '<']).next().unwrap_or("").trim();
    matches!(
        head,
        "BIGINT"
            | "INT"
            | "INTEGER"
            | "SMALLINT"
            | "TINYINT"
            | "STRING"
            | "VARCHAR"
            | "CHAR"
            | "DATE"
            | "DATETIME"
            | "TIMESTAMP"
            | "DECIMAL"
    )
}

/// The pure row author used by the closed SHOW CPU turn.
pub(crate) struct MvShowResultRows {
    table: novarocks_query_application::api::LocalTableBuilder,
}
pub(crate) struct MvShowRowSelection {
    pub(crate) read_dependencies: bool,
    dependency_budget: usize,
    display: String,
}
impl MvShowResultRows {
    pub(crate) fn new() -> Result<Self, String> {
        Ok(Self {
            table: mv_table_builder()?,
        })
    }
    pub(crate) fn begin_row(
        &self,
        listed: &novarocks_mv_application::readiness::ListedKeyedMvProjection,
        entrance: Option<&novarocks_mv_application::management::ManagementEntrance>,
        current_catalog: Option<&str>,
        stmt: &MvShowStatement,
        storage_filter: Option<MvStorageEngine>,
    ) -> Result<Option<MvShowRowSelection>, String> {
        let projection = &listed.loaded.projection;
        if !matches_show_filter(projection, current_catalog, stmt, storage_filter) {
            return Ok(None);
        }
        let manageability = manageability_parts(&listed.manageability, entrance, projection);
        let row_prefix = mv_row_bytes(
            projection,
            0,
            manageability.0.len().saturating_add(manageability.1.len()),
        );
        let bound = novarocks_query_application::api::LocalResultBound::V1;
        bound
            .admit(
                self.table.rows().saturating_add(1),
                self.table.bytes().saturating_add(row_prefix),
            )
            .map_err(|e| format!("SHOW MATERIALIZED VIEWS: {e}"))?;
        admit_mv_row(
            projection,
            0,
            manageability.0.len().saturating_add(manageability.1.len()),
            self.table.rows(),
            self.table.bytes(),
            bound,
            bound.bytes,
        )?;
        let row_workspace_overhead = std::mem::size_of::<MvListRow>() + 16 * 64;
        let dependency_budget = (bound.bytes - self.table.bytes() - row_prefix).min(
            bound
                .bytes
                .saturating_sub(row_prefix.saturating_add(row_workspace_overhead)),
        );
        let mut display = String::with_capacity(manageability.0.len() + manageability.1.len());
        display.push_str(manageability.0);
        display.push_str(manageability.1);
        Ok(Some(MvShowRowSelection {
            read_dependencies: matches!(&listed.manageability, MvListedManageability::Manageable),
            dependency_budget,
            display,
        }))
    }
    pub(crate) fn append(
        &mut self,
        projection: &StoredKeyedMvProjection,
        selection: MvShowRowSelection,
        dependencies: &[novarocks_mv_application::persistence::dependency::StoredMvDependency],
    ) -> Result<(), String> {
        let dependencies = render_dependency_display(dependencies, selection.dependency_budget)?;
        let bound = novarocks_query_application::api::LocalResultBound::V1;
        admit_mv_row(
            projection,
            dependencies.len(),
            selection.display.len(),
            self.table.rows(),
            self.table.bytes(),
            bound,
            bound.bytes,
        )?;
        let row = list_row_from_projection(projection, dependencies, selection.display);
        append_mv_row(&mut self.table, &row)
    }
    pub(crate) fn finish(self) -> Result<QueryResult, String> {
        self.table
            .finish()
            .map_err(|e| format!("build SHOW MATERIALIZED VIEWS batch failed: {e}"))
    }
}

/// What SHOW prints for one target's manageability.
///
/// The reason is carried through rather than summarised: an operator seeing
/// READ_ONLY has to know whether it is a restart barrier they can retire or
/// another deployment's target they cannot.
fn manageability_parts<'a>(
    manageability: &'a MvListedManageability,
    entrance: Option<&novarocks_mv_application::management::ManagementEntrance>,
    projection: &StoredKeyedMvProjection,
) -> (&'static str, &'a str) {
    use novarocks_mv_application::management::MvManagementPhase;

    match manageability {
        MvListedManageability::ReadOnly(reason) => return ("READ_ONLY: ", reason),
        MvListedManageability::Unavailable(reason) => return ("UNAVAILABLE: ", reason),
        MvListedManageability::Manageable => {}
    }
    // Readiness says this process may read the target. Whether it may write
    // it is the entrance's answer, and the two diverge exactly where it
    // matters: a target whose owner was just handed away is still a sound
    // query candidate while it is no longer this process's to refresh.
    let Some(entrance) = entrance else {
        return ("", "MANAGEABLE");
    };
    match entrance.management_phase(&projection.facts.source_revision().target) {
        MvManagementPhase::Manageable => ("", "MANAGEABLE"),
        MvManagementPhase::Managing => ("", "MANAGING"),
        // A target this entrance has never observed is not one it has closed:
        // the projection is installed and nothing here holds it.
        MvManagementPhase::NotObserved => ("", "MANAGEABLE"),
        other => ("READ_ONLY: ", other.as_str()),
    }
}

#[cfg(test)]
fn manageability_display(
    manageability: &MvListedManageability,
    entrance: Option<&novarocks_mv_application::management::ManagementEntrance>,
    projection: &StoredKeyedMvProjection,
) -> String {
    let (prefix, reason) = manageability_parts(manageability, entrance, projection);
    format!("{prefix}{reason}")
}

fn base_tables_bytes(projection: &StoredKeyedMvProjection) -> usize {
    let occurrences = &projection.facts.definition().relation_occurrences;
    occurrences.iter().fold(
        occurrences.len().saturating_sub(1).saturating_mul(2),
        |total, relation| {
            total
                .saturating_add(relation.catalog_at_binding.len())
                .saturating_add(relation.namespace_at_binding.len())
                .saturating_add(relation.relation_at_binding.len())
                .saturating_add(2)
        },
    )
}

fn base_tables_display(projection: &StoredKeyedMvProjection) -> String {
    let mut output = String::with_capacity(base_tables_bytes(projection));
    for (index, relation) in projection
        .facts
        .definition()
        .relation_occurrences
        .iter()
        .enumerate()
    {
        if index > 0 {
            output.push_str(", ");
        }
        output.push_str(&relation.catalog_at_binding);
        output.push('.');
        output.push_str(&relation.namespace_at_binding);
        output.push('.');
        output.push_str(&relation.relation_at_binding);
    }
    output
}

fn mv_row_bytes(
    projection: &StoredKeyedMvProjection,
    dependency_bytes: usize,
    manageability_bytes: usize,
) -> usize {
    use novarocks_query_application::api::LocalResultBound;
    let facts = &projection.facts;
    let configuration = facts.configuration();
    let (refresh_time, refresh_rows) = match facts.publication() {
        None => (0, 0),
        Some(published) => (
            published.publication_prepared_at_ms.to_string().len(),
            published
                .logical_result_rows
                .map_or(0, |n| n.to_string().len()),
        ),
    };
    // Numeric text here has a fixed <=20-byte bound; variable-size strings
    // and joined names remain borrowed until the whole row is admitted.
    [
        facts.target().name().len(),
        facts.target().namespace().len(),
        MvStorageEngine::Iceberg.as_sql_str().len(),
        match configuration.refresh_policy {
            RefreshPolicy::Manual => "DEFERRED_MANUAL".len(),
            RefreshPolicy::AsyncOnChange => "ASYNC_ON_CHANGE".len(),
            RefreshPolicy::AsyncInterval => "ASYNC_INTERVAL".len(),
        },
        refresh_time,
        refresh_rows,
        base_tables_bytes(projection),
        facts.definition().query.effective_sql.len(),
        dependency_bytes,
        if configuration.paused { 4 } else { 5 },
        0,
        0,
        configuration
            .max_staleness_ms
            .map_or(0, |n| n.to_string().len()),
        refresh_status_for_configuration(configuration).len(),
        0,
        manageability_bytes,
    ]
    .into_iter()
    .fold(0usize, |sum, n| {
        sum.saturating_add(LocalResultBound::cell_bytes(n))
    })
}

fn admit_mv_row(
    projection: &StoredKeyedMvProjection,
    dependency_bytes: usize,
    manageability_bytes: usize,
    rows: usize,
    bytes: usize,
    bound: novarocks_query_application::api::LocalResultBound,
    workspace_bytes: usize,
) -> Result<(), String> {
    let row_bytes = mv_row_bytes(projection, dependency_bytes, manageability_bytes);
    bound
        .admit(rows.saturating_add(1), bytes.saturating_add(row_bytes))
        .map_err(|e| format!("SHOW MATERIALIZED VIEWS: {e}"))?;
    // One owned domain row is the temporary conversion workspace, not a
    // second retained result list. Include its headers/allocation allowance.
    if row_bytes
        .saturating_add(std::mem::size_of::<MvListRow>())
        .saturating_add(16 * 64)
        > workspace_bytes
    {
        return Err("SHOW MATERIALIZED VIEWS exceeds its row workspace bound".into());
    }
    Ok(())
}

fn matches_show_filter(
    projection: &StoredKeyedMvProjection,
    current_catalog: Option<&str>,
    stmt: &MvShowStatement,
    storage_filter: Option<MvStorageEngine>,
) -> bool {
    // Iceberg is the sole admitted MV storage provider. Catalog aliases are
    // target names, not provider identities.
    if storage_filter.is_some_and(|filter| filter != MvStorageEngine::Iceberg) {
        return false;
    }
    let target = projection.facts.target();
    if current_catalog.is_some_and(|catalog| {
        target
            .catalog()
            .is_none_or(|target_catalog| !target_catalog.eq_ignore_ascii_case(catalog))
    }) {
        return false;
    }
    !stmt
        .database
        .as_deref()
        .is_some_and(|database| !target.namespace().eq_ignore_ascii_case(database))
}

fn list_row_from_projection(
    projection: &StoredKeyedMvProjection,
    dependencies: String,
    manageability: String,
) -> MvListRow {
    let facts = &projection.facts;
    let target = facts.target();
    let definition = facts.definition();
    let configuration = facts.configuration();
    let (last_refresh_time, last_refresh_rows) = match facts.publication() {
        None => (None, None),
        Some(published) => {
            let publication = published;
            (
                // This is P's frozen publication-fact time, not provider
                // commit completion or Accelerator insertion time.
                Some(publication.publication_prepared_at_ms.to_string()),
                // SHOW reports logical result rows only. Unknown is NULL,
                // never filled from physical storage or processed input rows.
                publication.logical_result_rows.map(|rows| rows.to_string()),
            )
        }
    };
    MvListRow {
        manageability,
        name: target.name().to_string(),
        database: target.namespace().to_string(),
        storage_engine: MvStorageEngine::Iceberg.as_sql_str().to_string(),
        refresh_mode: match configuration.refresh_policy {
            RefreshPolicy::Manual => "DEFERRED_MANUAL",
            RefreshPolicy::AsyncOnChange => "ASYNC_ON_CHANGE",
            RefreshPolicy::AsyncInterval => "ASYNC_INTERVAL",
        }
        .to_string(),
        last_refresh_time,
        last_refresh_rows,
        base_tables: base_tables_display(projection),
        select_text: definition.query.effective_sql.clone(),
        dependencies,
        refresh_paused: configuration.paused.to_string(),
        next_refresh_time: None,
        last_scheduler_error: None,
        max_staleness_ms: configuration
            .max_staleness_ms
            .map(|value| value.to_string()),
        refresh_state: refresh_status_for_configuration(configuration),
        retry_after_time: None,
    }
}

fn refresh_status_for_configuration(configuration: &ConfigurationDocument) -> String {
    if configuration.paused {
        return "PAUSED".to_string();
    }
    if matches!(configuration.refresh_policy, RefreshPolicy::Manual) {
        "MANUAL".to_string()
    } else {
        "PENDING".to_string()
    }
}

fn render_dependency_display(
    dependencies: &[novarocks_mv_application::persistence::dependency::StoredMvDependency],
    maximum: usize,
) -> Result<String, String> {
    use novarocks_mv_application::dependency::MvDependencyObjectType;
    let bytes = dependencies.iter().fold(
        dependencies.len().saturating_sub(1).saturating_mul(2),
        |total, dependency| {
            let object = &dependency.upstream;
            total
                .saturating_add(
                    object
                        .catalog
                        .as_ref()
                        .map_or(0, |c| c.len().saturating_add(1)),
                )
                .saturating_add(object.database_or_namespace.len())
                .saturating_add(1)
                .saturating_add(object.name.len())
                .saturating_add(
                    if object.object_type == MvDependencyObjectType::MaterializedView {
                        3
                    } else {
                        0
                    },
                )
        },
    );
    if bytes > maximum {
        return Err("SHOW MATERIALIZED VIEWS dependency display exceeds its row byte bound".into());
    }
    let mut output = String::with_capacity(bytes);
    for (index, dependency) in dependencies.iter().enumerate() {
        if index > 0 {
            output.push_str(", ");
        }
        let object = &dependency.upstream;
        if object.object_type == MvDependencyObjectType::MaterializedView {
            output.push_str("mv:");
        }
        if let Some(catalog) = &object.catalog {
            output.push_str(catalog);
            output.push('.');
        }
        output.push_str(&object.database_or_namespace);
        output.push('.');
        output.push_str(&object.name);
    }
    Ok(output)
}

/// Analyze an MV SELECT against an already-admitted query-local table provider.
///
/// The provider is built by the query-assembly owner that admitted the
/// request: the request-local catalog snapshot, the exact connector control
/// lease, and the catalog-application admission gate are all frozen into it
/// before analysis starts. This adapter contributes MV SELECT preparation and
/// analysis only; it never acquires catalog or connector authority itself.
pub fn analyze_mv_select_with_provider(
    current_catalog: Option<&str>,
    provider: &dyn novarocks_sql::planning::catalog::PlannerTableProvider,
    current_database: &str,
    query: &novarocks_parser::ast::Query,
    functions: &dyn novarocks_sql::compiler::SqlFunctionCatalog,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: novarocks_sql::compiler::SqlCompileControl,
) -> Result<MvAnalysis, novarocks_sql::compiler::SqlCompileError> {
    let catalog = novarocks_sql::compiler::SqlPlannerTableSnapshot::new(provider);
    analyze_mv_select_with_captured_catalog(
        current_catalog,
        &catalog,
        current_database,
        query,
        functions,
        constant_policy,
        control,
    )
}

/// Pure analysis against the caller's already captured catalog. No lookup can
/// obtain live Provider metadata while this author runs on the closed CPU.
pub(crate) fn analyze_mv_select_with_captured_catalog(
    current_catalog: Option<&str>,
    catalog: &dyn novarocks_sql::compiler::SqlCatalogSnapshot,
    current_database: &str,
    query: &novarocks_parser::ast::Query,
    functions: &dyn novarocks_sql::compiler::SqlFunctionCatalog,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: novarocks_sql::compiler::SqlCompileControl,
) -> Result<MvAnalysis, novarocks_sql::compiler::SqlCompileError> {
    let prepared =
        prepare_mv_select_for_catalog_provider(query, current_catalog, current_database)?;
    let refresh_input = novarocks_sql::compiler::analyze_mv_refresh_input(
        novarocks_sql::compiler::SqlMvRefreshAnalysisContext {
            query: Box::new(prepared.query_for_analysis().clone()),
            current_database: current_database.to_string(),
            catalog,
            functions,
            constant_policy,
            control,
        },
    )?;
    let output_columns = refresh_input.analysis_facts().output_columns;
    Ok(MvAnalysis {
        resolved_refs: prepared.resolved_refs().to_vec(),
        output_columns,
        refresh_input,
    })
}

fn mv_table_builder() -> Result<novarocks_query_application::api::LocalTableBuilder, String> {
    const COLUMNS: &[(&str, bool)] = &[
        ("Name", false),
        ("Database", false),
        ("StorageEngine", false),
        ("RefreshMode", false),
        ("LastRefreshTime", true),
        ("LastRefreshRows", true),
        ("BaseTables", false),
        ("SelectText", false),
        ("Dependencies", false),
        ("RefreshPaused", false),
        ("NextRefreshTime", true),
        ("LastSchedulerError", true),
        ("MaxStalenessMs", true),
        ("RefreshState", false),
        ("RetryAfterTime", true),
        ("Manageability", false),
    ];
    novarocks_query_application::api::LocalTableBuilder::try_new(
        COLUMNS,
        novarocks_query_application::api::LocalResultBound::V1,
    )
    .map_err(|e| format!("build SHOW MATERIALIZED VIEWS batch failed: {e}"))
}

fn append_mv_row(
    table: &mut novarocks_query_application::api::LocalTableBuilder,
    row: &MvListRow,
) -> Result<(), String> {
    table
        .push_row(&[
            Some(row.name.as_str()),
            Some(row.database.as_str()),
            Some(row.storage_engine.as_str()),
            Some(row.refresh_mode.as_str()),
            row.last_refresh_time.as_deref(),
            row.last_refresh_rows.as_deref(),
            Some(row.base_tables.as_str()),
            Some(row.select_text.as_str()),
            Some(row.dependencies.as_str()),
            Some(row.refresh_paused.as_str()),
            row.next_refresh_time.as_deref(),
            row.last_scheduler_error.as_deref(),
            row.max_staleness_ms.as_deref(),
            Some(row.refresh_state.as_str()),
            row.retry_after_time.as_deref(),
            Some(row.manageability.as_str()),
        ])
        .map_err(|e| format!("build SHOW MATERIALIZED VIEWS batch failed: {e}"))
}

#[cfg(test)]
pub(crate) fn build_mv_rows_result(rows: &[MvListRow]) -> Result<QueryResult, String> {
    let mut table = mv_table_builder()?;
    for row in rows {
        append_mv_row(&mut table, row)?;
    }
    table
        .finish()
        .map_err(|e| format!("build SHOW MATERIALIZED VIEWS batch failed: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::domain::test_admission::KeyedProjectionFixture as ProjectionFixture;
    use novarocks_mv_application::product::MvTarget;

    fn fixture(snapshot_id: Option<i64>) -> ProjectionFixture {
        ProjectionFixture::new(
            MvTarget::from_parts(Some("lake_alias"), "analytics", "orders_mv"),
            snapshot_id,
        )
    }

    fn stored(fixture: ProjectionFixture) -> StoredKeyedMvProjection {
        StoredKeyedMvProjection {
            mv_id: 42,
            facts: fixture.build().expect("valid document projection"),
        }
    }

    #[test]
    fn show_preserves_definition_occurrences_and_exact_target_names() {
        let projection = stored(fixture(Some(201)));
        let row = list_row_from_projection(
            &projection,
            "ice.sales.orders".to_string(),
            "MANAGEABLE".to_string(),
        );

        assert_eq!(row.name, "orders_mv");
        assert_eq!(row.database, "analytics");
        assert_eq!(row.storage_engine, "iceberg");
        assert_eq!(
            projection
                .facts
                .definition()
                .relation_occurrences
                .iter()
                .map(|occurrence| occurrence.occurrence_id)
                .collect::<Vec<_>>(),
            vec![7, 8],
        );
        assert_eq!(row.base_tables, "ice.sales.orders, ice.sales.orders");
        assert_eq!(
            row.select_text,
            projection.facts.definition().query.effective_sql,
        );
        assert_eq!(row.dependencies, "ice.sales.orders");
    }

    #[test]
    fn show_reports_publication_prepared_time_and_logical_rows_only() {
        let mut fixture = fixture(Some(201));
        let publication = fixture.publication.as_mut().expect("published fixture");
        publication.publication_prepared_at_ms = 1_700_000_001_234;
        publication.logical_result_rows = Some(7);
        let projection = stored(fixture);
        let row = list_row_from_projection(&projection, String::new(), "MANAGEABLE".to_string());

        // P freezes this time before commit; it is neither provider commit
        // completion nor D's creation time or an Accelerator insertion time.
        assert_ne!(
            projection.facts.definition().created_at_ms,
            1_700_000_001_234,
        );
        assert_eq!(row.last_refresh_time.as_deref(), Some("1700000001234"),);
        assert_eq!(row.last_refresh_rows.as_deref(), Some("7"));
        let Some(_published) = projection.facts.publication() else {
            panic!("expected a published projection");
        };
    }

    #[test]
    fn show_keeps_unknown_logical_rows_null_despite_other_row_statistics() {
        let mut fixture = fixture(Some(201));
        let publication = fixture.publication.as_mut().expect("published fixture");
        publication.logical_result_rows = None;
        let row =
            list_row_from_projection(&stored(fixture), String::new(), "MANAGEABLE".to_string());

        assert!(row.last_refresh_time.is_some());
        assert_eq!(row.last_refresh_rows, None);
    }

    #[test]
    fn show_preserves_published_zero_logical_rows() {
        let mut fixture = fixture(Some(201));
        let publication = fixture.publication.as_mut().expect("published fixture");
        publication.logical_result_rows = Some(0);
        let row =
            list_row_from_projection(&stored(fixture), String::new(), "MANAGEABLE".to_string());

        assert_eq!(row.last_refresh_rows.as_deref(), Some("0"));
    }

    #[test]
    fn show_never_published_has_no_refresh_time_or_row_count() {
        let projection = stored(fixture(None));
        let row = list_row_from_projection(&projection, String::new(), "MANAGEABLE".to_string());

        assert!(projection.facts.definition().created_at_ms > 0);
        assert_eq!(row.last_refresh_time, None);
        assert_eq!(row.last_refresh_rows, None);
        assert_eq!(row.next_refresh_time, None);
        assert_eq!(row.last_scheduler_error, None);
        assert_eq!(row.retry_after_time, None);
    }

    #[test]
    fn show_reads_refresh_policy_and_pause_from_configuration() {
        for (policy, interval, mode, state) in [
            (RefreshPolicy::Manual, None, "DEFERRED_MANUAL", "MANUAL"),
            (
                RefreshPolicy::AsyncOnChange,
                None,
                "ASYNC_ON_CHANGE",
                "PENDING",
            ),
            (
                RefreshPolicy::AsyncInterval,
                Some(60_000),
                "ASYNC_INTERVAL",
                "PENDING",
            ),
        ] {
            for paused in [false, true] {
                let mut fixture = fixture(None);
                fixture.configuration.refresh_policy = policy;
                fixture.configuration.refresh_interval_ms = interval;
                fixture.configuration.paused = paused;
                fixture.configuration.max_staleness_ms = Some(123);
                let row = list_row_from_projection(
                    &stored(fixture),
                    String::new(),
                    "MANAGEABLE".to_string(),
                );

                assert_eq!(row.refresh_mode, mode);
                assert_eq!(row.refresh_paused, paused.to_string());
                assert_eq!(row.refresh_state, if paused { "PAUSED" } else { state });
                assert_eq!(row.max_staleness_ms.as_deref(), Some("123"));
            }
        }
    }

    #[test]
    fn show_filters_use_projection_target_not_definition_resolution() {
        let projection = stored(fixture(None));
        let matching = MvShowStatement {
            database: Some("ANALYTICS".to_string()),
        };
        assert!(matches_show_filter(
            &projection,
            Some("LAKE_ALIAS"),
            &matching,
            Some(MvStorageEngine::Iceberg),
        ));
        assert!(!matches_show_filter(
            &projection,
            Some("ice"),
            &matching,
            None,
        ));
        assert!(!matches_show_filter(
            &projection,
            None,
            &MvShowStatement {
                database: Some("sales".to_string()),
            },
            None,
        ));
        assert!(!matches_show_filter(
            &projection,
            None,
            &matching,
            Some(MvStorageEngine::StarRocks),
        ));
    }
}

#[cfg(test)]
mod manageability_tests {
    use super::*;

    use crate::mv::domain::test_admission::KeyedProjectionFixture as ProjectionFixture;

    fn projection() -> StoredKeyedMvProjection {
        StoredKeyedMvProjection {
            mv_id: 42,
            facts: ProjectionFixture::new(
                novarocks_mv_application::product::MvTarget::from_parts(Some("ice"), "db", "mv"),
                Some(1),
            )
            .build()
            .expect("valid document projection"),
        }
    }

    #[test]
    fn a_manageable_target_says_so_plainly() {
        assert_eq!(
            manageability_display(&MvListedManageability::Manageable, None, &projection()),
            "MANAGEABLE"
        );
    }

    #[test]
    fn a_read_only_target_carries_the_reason_it_cannot_be_refreshed() {
        let shown = manageability_display(
            &MvListedManageability::ReadOnly(
                "a previous incarnation may still have an effect in flight".to_string(),
            ),
            None,
            &projection(),
        );

        assert!(shown.starts_with("READ_ONLY: "), "{shown}");
        assert!(
            shown.contains("previous incarnation"),
            "an operator has to tell a restart barrier from another deployment's target: {shown}"
        );
    }

    #[test]
    fn a_quarantined_target_is_listed_with_the_reason_it_is_not_trusted() {
        let shown = manageability_display(
            &MvListedManageability::Unavailable("catalog discovery read failed".to_string()),
            None,
            &projection(),
        );

        assert_eq!(shown, "UNAVAILABLE: catalog discovery read failed");
    }

    #[test]
    fn a_target_the_entrance_has_closed_is_not_reported_manageable() {
        use novarocks_mv_application::management::{
            DeploymentOwner, ManagementEntrance, ProcessIncarnation,
        };

        let entrance = ManagementEntrance::new(
            DeploymentOwner::parse("deployment-a").expect("owner"),
            ProcessIncarnation::parse("inc-a").expect("incarnation"),
        );
        entrance.begin_stopping();

        let shown = manageability_display(
            &MvListedManageability::Manageable,
            Some(&entrance),
            &projection(),
        );

        assert_eq!(
            shown, "READ_ONLY: STOPPING",
            "readiness says the projection is readable; only the entrance knows it is not writable"
        );
    }

    #[test]
    fn every_listed_row_has_a_manageability_column() {
        let row = MvListRow {
            name: "mv".to_string(),
            database: "db".to_string(),
            storage_engine: "iceberg".to_string(),
            refresh_mode: "DEFERRED_MANUAL".to_string(),
            last_refresh_time: None,
            last_refresh_rows: None,
            base_tables: String::new(),
            select_text: String::new(),
            dependencies: String::new(),
            refresh_paused: "false".to_string(),
            next_refresh_time: None,
            last_scheduler_error: None,
            max_staleness_ms: None,
            refresh_state: "IDLE".to_string(),
            retry_after_time: None,
            manageability: "READ_ONLY: closed after restart".to_string(),
        };

        let result = build_mv_rows_result(std::slice::from_ref(&row)).expect("one row");
        assert!(
            result
                .columns
                .iter()
                .any(|column| column.name() == "Manageability"),
            "SHOW has to report why a listed MV cannot be refreshed"
        );
    }
}

#[cfg(test)]
mod compile_control_tests {
    use super::analyze_mv_select_with_provider;
    use novarocks_sql::compiler::{SqlCancellationObservation, SqlCompileControl, SqlCompileError};
    use std::sync::Arc;

    struct Cancelled(bool);
    impl SqlCancellationObservation for Cancelled {
        fn is_cancelled(&self) -> bool {
            self.0
        }
    }

    struct NoLookupAfterControl;
    impl novarocks_sql::planning::catalog::PlannerTableProvider for NoLookupAfterControl {
        fn resolve_table_for_analysis(
            &self,
            _catalog: Option<&str>,
            _database: &str,
            _table: &str,
        ) -> Result<novarocks_sql::planning::catalog::ResolvedAnalyzerTable, String> {
            panic!("entry control failure must precede catalog materialization")
        }
    }

    #[test]
    fn aggregate_preanalysis_preserves_original_request_entry_control() {
        let query =
            crate::mv::domain::refresh::definition::parse_mv_select_query("SELECT sum(x) FROM t")
                .unwrap();
        let provider = NoLookupAfterControl;
        for deadline in [false, true] {
            let control = SqlCompileControl::new(
                deadline.then(std::time::Instant::now),
                Arc::new(Cancelled(!deadline)),
            );
            let result = analyze_mv_select_with_provider(
                Some("default_catalog"),
                &provider,
                "default_db",
                &query,
                novarocks_sql::compiler::builtin_sql_function_catalog(),
                crate::application::test_constant_policy(),
                control,
            );
            assert!(matches!(
                (deadline, result),
                (false, Err(SqlCompileError::Cancelled))
                    | (true, Err(SqlCompileError::DeadlineExceeded))
            ));
        }
    }
}

#[cfg(test)]
mod bounded_mv_row_tests {
    use super::*;
    use crate::mv::domain::test_admission::KeyedProjectionFixture as ProjectionFixture;
    use novarocks_mv_application::product::MvTarget;
    use novarocks_query_application::api::LocalResultBound;

    fn projection() -> StoredKeyedMvProjection {
        let mut fixture =
            ProjectionFixture::new(MvTarget::from_parts(Some("ice"), "sales", "mv"), Some(1));
        fixture.definition.query.effective_sql = "select a deliberately long SQL text".into();
        fixture.configuration.max_staleness_ms = Some(1234);
        StoredKeyedMvProjection {
            mv_id: 7,
            facts: fixture.build().unwrap(),
        }
    }

    #[test]
    fn borrowed_mv_row_size_matches_all_cells_before_copying() {
        let projection = projection();
        let reason = MvListedManageability::ReadOnly("readmission is pending".into());
        let (prefix, text) = manageability_parts(&reason, None, &projection);
        let dependencies = "ice.sales.base, mv:ice.sales.upstream";
        let expected = mv_row_bytes(&projection, dependencies.len(), prefix.len() + text.len());
        let row =
            list_row_from_projection(&projection, dependencies.into(), format!("{prefix}{text}"));
        let mut builder = mv_table_builder().unwrap();
        append_mv_row(&mut builder, &row).unwrap();
        assert_eq!(builder.bytes(), expected);
        assert_eq!(base_tables_bytes(&projection), row.base_tables.len());
    }

    #[test]
    fn mv_row_refuses_whole_and_cumulative_limits_before_owned_row_construction() {
        let projection = projection();
        let bytes = mv_row_bytes(&projection, 3, 10);
        let bound = LocalResultBound {
            rows: 2,
            bytes,
            columns: 16,
        };
        admit_mv_row(&projection, 3, 10, 0, 0, bound, 128 * 1024).unwrap();
        assert!(admit_mv_row(&projection, 3, 10, 0, 1, bound, 128 * 1024).is_err());
        assert!(admit_mv_row(&projection, 3, 10, 2, 0, bound, 128 * 1024).is_err());
        assert!(
            admit_mv_row(&projection, 3, 10, 0, 0, bound, 1)
                .unwrap_err()
                .contains("workspace")
        );
    }
}

#[cfg(test)]
mod bounded_dependency_display_tests {
    use super::render_dependency_display;
    use novarocks_mv_application::dependency::{
        MvDependencyObjectRef, MvDependencyObjectType, MvDependencyStorageEngine,
    };
    use novarocks_mv_application::persistence::dependency::StoredMvDependency;

    #[test]
    fn display_preserves_mv_prefixes_and_separators_under_the_exact_join_bound() {
        let rows = [
            MvDependencyObjectType::Table,
            MvDependencyObjectType::MaterializedView,
            MvDependencyObjectType::Unclassified,
        ]
        .into_iter()
        .map(|object_type| StoredMvDependency {
            downstream_mv_id: 1,
            occurrence_id: 0,
            upstream_object_id: Default::default(),
            created_at_ms: 0,
            upstream: MvDependencyObjectRef {
                catalog: Some("ice".into()),
                database_or_namespace: "sales".into(),
                name: "base".into(),
                object_type,
                storage_engine: MvDependencyStorageEngine::Unclassified,
            },
        })
        .collect::<Vec<_>>();
        let expected = rows
            .iter()
            .map(|row| row.upstream.display_name())
            .collect::<Vec<_>>()
            .join(", ");
        assert_eq!(
            render_dependency_display(&rows, expected.len()).unwrap(),
            expected
        );
        assert!(
            render_dependency_display(&rows, expected.len() - 1)
                .unwrap_err()
                .contains("row byte bound")
        );
        assert_eq!(render_dependency_display(&[], 0).unwrap(), "");
    }
}

#[cfg(test)]
mod captured_create_analysis_tests {
    use super::*;
    #[test]
    fn create_aggregate_analysis_consumes_exact_captured_facts_without_live_lookup() {
        use novarocks_sql::compiler::{
            CatalogRelationFact, SqlCapturedCatalogPreparation, SqlCompileControl,
        };
        use novarocks_sql::planning::catalog::{
            ConnectorReadTableFacts, materialize_connector_read_table,
        };
        let query = crate::mv::domain::refresh::definition::parse_mv_select_query(
            "SELECT count(*) AS n FROM ice.sales.orders",
        )
        .unwrap();
        let preparation = SqlCapturedCatalogPreparation::try_new(
            query.clone(),
            Some("ice"),
            "sales",
            SqlCompileControl::unbounded(),
        )
        .unwrap();
        let [need] = preparation.needs() else {
            panic!("one exact original relation");
        };
        let mut bindings = novarocks_sql::binding::SqlTableBindingAllocator::new_unique().unwrap();
        let table = materialize_connector_read_table(ConnectorReadTableFacts {
            catalog: "ice".into(),
            namespace: "sales".into(),
            table: "orders".into(),
            columns: vec![
                novarocks_types::schema::ColumnDef::from_value_type(
                    "id".into(),
                    novarocks_type_contract::FunctionValueType::new(
                        arrow::datatypes::DataType::Int64,
                        true,
                    ),
                    None,
                )
                .unwrap(),
            ],
            iceberg_row_lineage_metadata_columns: Vec::new(),
            schema: std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, true),
            ])),
            binding: bindings.allocate().unwrap(),
            selector: novarocks_spi::connector::ConnectorReadSelector::Current,
            planning_facts: novarocks_spi::connector::ConnectorTablePlanningFacts::empty(),
        })
        .unwrap();
        let fact = CatalogRelationFact::resolved(need, table.into_resolved_table()).unwrap();
        let captured = preparation.capture(&[fact]).unwrap();
        let analysis = analyze_mv_select_with_captured_catalog(
            Some("ice"),
            &captured,
            "sales",
            &query,
            novarocks_sql::compiler::builtin_sql_function_catalog(),
            crate::application::test_constant_policy(),
            SqlCompileControl::unbounded(),
        )
        .unwrap();
        assert_eq!(analysis.output_columns[0].name, "n");
        assert_eq!(
            analysis.resolved_refs,
            vec![crate::mv::domain::analysis::ResolvedTableRef::Iceberg {
                catalog: "ice".into(),
                namespace: "sales".into(),
                table: "orders".into(),
            }]
        );
        assert_eq!(
            analysis
                .refresh_input
                .create_persistence_facts()
                .unwrap()
                .relation_occurrences()
                .len(),
            1
        );
        assert!(
            captured
                .provider()
                .resolve_table_for_analysis(Some("ice"), "other", "orders")
                .is_err()
        );
    }
}
