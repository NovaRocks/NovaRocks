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

//! Stateful materialized-view analysis adapter.

use arrow::datatypes::DataType;

use crate::mv::domain::analysis::{MvAnalysis, prepare_mv_select_for_catalog_provider};
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
        let query = crate::mv::domain::refresh::definition::read_stored_mv_query(
            "SELECT sum(x) FROM t",
            &novarocks_types::naming::TableIdentity::new("default_catalog", "default_db", "mv"),
        )
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
        let query = crate::mv::domain::refresh::definition::read_stored_mv_query(
            "SELECT count(*) AS n FROM ice.sales.orders",
            &novarocks_types::naming::TableIdentity::new("ice", "sales", "mv"),
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
