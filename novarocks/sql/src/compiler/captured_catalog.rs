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

//! SQL-owned catalog capture for pure analysis over exact observed facts.

use super::{
    CatalogRelationFact, CatalogRelationNeed, SqlCatalogSnapshot, SqlCompileControl,
    SqlCompileError,
    completion_catalog::{CatalogCompletionState, FactBackedCatalog},
};
use crate::catalog::PlannerTableProvider;

/// The original SQL prepass declares every SchemaOnly observation. This source
/// contains no application provider, loader, callback or execution authority.
pub struct SqlCapturedCatalogPreparation {
    catalog: CatalogCompletionState,
    control: SqlCompileControl,
}

impl SqlCapturedCatalogPreparation {
    pub fn try_new(
        query: novarocks_parser::ast::Query,
        current_catalog: Option<&str>,
        current_database: &str,
        control: SqlCompileControl,
    ) -> Result<Self, SqlCompileError> {
        control.check()?;
        let catalog = CatalogCompletionState::try_new(
            query,
            current_catalog,
            current_database,
            crate::catalog::TableLookupMode::SchemaOnly,
            0,
        )?
        .with_exchange_budget(super::DEFAULT_COMPLETION_LIMITS)?;
        control.check()?;
        Ok(Self { catalog, control })
    }

    pub fn try_new_definition(
        query: novarocks_parser::ast::Query,
        catalog: &str,
        database: &str,
        target: Option<novarocks_types::naming::TableIdentity>,
        control: SqlCompileControl,
    ) -> Result<Self, SqlCompileError> {
        control.check()?;
        let catalog = CatalogCompletionState::try_new(
            query,
            Some(catalog),
            database,
            crate::catalog::TableLookupMode::SchemaOnly,
            0,
        )?
        .with_definition_context(target, &control)?
        .with_exchange_budget(super::DEFAULT_COMPLETION_LIMITS)?;
        Ok(Self { catalog, control })
    }

    pub fn continue_catalog(
        mut self,
        facts: Box<[CatalogRelationFact]>,
    ) -> Result<Self, SqlCompileError> {
        self.catalog.accept_facts(facts, &self.control)?;
        Ok(self)
    }

    /// Analyze the original definition after every exact dependency is known.
    /// The source header precedes the public alias list and is never rewritten
    /// from that list or from planner display labels.
    pub fn analyze_view_definition(
        self,
        aliases: &[novarocks_parser::ast::Ident],
        functions: &dyn super::SqlFunctionCatalog,
        policy: super::ConstantPolicy,
    ) -> Result<Box<[crate::catalog::SqlViewColumn]>, SqlCompileError> {
        let database = self.catalog.definition_database()?;
        self.control.check()?;
        let catalog = self.catalog.completed_catalog()?;
        let semantics = crate::sql_mode::SqlSemanticSettings::default();
        crate::sql_mode::validate_persisted_query_semantics_typed(self.catalog.query(), &semantics)
            .map_err(SqlCompileError::from)?;
        let query =
            crate::sql_mode::normalize_concat_query(self.catalog.query().clone(), &semantics)
                .map_err(SqlCompileError::from)?;
        let functions = functions.snapshot_for_scalar_presence();
        let (resolved, _, factory) = crate::analyzer::analyze_with_function_catalog(
            &query,
            &catalog,
            database,
            functions.as_ref(),
            policy,
            &self.control,
        )
        .map_err(SqlCompileError::from)?;
        let names = crate::analysis::schema::validate_definition_column_names(
            &resolved.output_columns,
            aliases,
            query.span,
        )?;
        crate::planning::mv::validate_persistable_output(&resolved, &factory)
            .map_err(SqlCompileError::Compilation)?;
        let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
            &self.control,
            novarocks_type_contract::CompilePhase::Validate,
        )?;
        let mut columns = Vec::with_capacity(names.len());
        for (source, name) in resolved
            .output_columns
            .iter()
            .filter(|field| !field.is_internal)
            .zip(names)
        {
            work.step()?;
            columns.push(crate::catalog::SqlViewColumn {
                source_field_name: source.field_name.as_deref().map(std::sync::Arc::from),
                name: name.into(),
                value_type: source.value_type.clone(),
            });
        }
        work.finish()?;
        Ok(columns.into_boxed_slice())
    }

    pub fn needs(&self) -> &[CatalogRelationNeed] {
        self.catalog.needs()
    }

    /// Use the existing exact completion validator. A missing, duplicate or
    /// foreign answer cannot become a best-effort catalog or a live lookup.
    pub fn capture(
        &self,
        facts: &[CatalogRelationFact],
    ) -> Result<SqlCapturedCatalog, SqlCompileError> {
        self.control.check()?;
        let catalog = self.catalog.fact_catalog(facts)?;
        self.control.check()?;
        Ok(SqlCapturedCatalog { catalog })
    }
}

/// An immutable SQL catalog whose only possible answers are captured values.
/// It can run on the original CPU source without entering an external loader.
pub struct SqlCapturedCatalog {
    catalog: FactBackedCatalog,
}

impl SqlCapturedCatalog {
    pub fn provider(&self) -> &dyn PlannerTableProvider {
        &self.catalog
    }
}

impl SqlCatalogSnapshot for SqlCapturedCatalog {
    fn planner_table_provider(&self) -> &dyn PlannerTableProvider {
        self.provider()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::ResolvedAnalyzerTable;
    use crate::planner::table::{
        ScanSource, SqlScanKind, SqlScanSource, SqlTableIdentity, SqlTableVersionSelector, TableDef,
    };

    fn query(sql: &str) -> novarocks_parser::ast::Query {
        let statements = novarocks_parser::parse(sql).unwrap();
        let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("query fixture")
        };
        query.clone()
    }

    #[test]
    fn captured_schema_catalog_feeds_real_mv_analysis_without_a_loader() {
        let source = SqlCapturedCatalogPreparation::try_new(
            query("SELECT 1 AS amount FROM ice.sales.orders"),
            Some("ice"),
            "sales",
            SqlCompileControl::unbounded(),
        )
        .unwrap();
        let [need] = source.needs() else {
            panic!("the actual SQL prepass must request the exact source table")
        };
        assert_eq!(need.relation().fqn(), "ice.sales.orders");
        let identity = need.relation();
        let table = TableDef {
            name: identity.table.clone(),
            columns: Vec::new(),
            iceberg_row_lineage_metadata_columns: Vec::new(),
            source: ScanSource::Sql(SqlScanSource::new(
                crate::binding::SqlTableBindingId::new_for_test(1),
                SqlTableIdentity::try_new(
                    identity.catalog.clone(),
                    identity.namespace.clone(),
                    identity.table.clone(),
                )
                .unwrap(),
                SqlScanKind::Data {
                    version: SqlTableVersionSelector::Current,
                },
            )),
        };
        let fact = CatalogRelationFact::resolved(
            need,
            ResolvedAnalyzerTable::from_planner(
                Some(&identity.catalog),
                &identity.namespace,
                table,
            ),
        )
        .unwrap();
        let captured = source.capture(&[fact]).unwrap();
        let actual =
            super::super::analyze_mv_refresh_input(super::super::SqlMvRefreshAnalysisContext {
                query: Box::new(query("SELECT 1 AS amount FROM ice.sales.orders")),
                current_database: "sales".into(),
                catalog: &captured,
                functions: crate::functions::builtin_sql_function_catalog(),
                constant_policy: crate::constant::test_constant_policy(),
                control: SqlCompileControl::unbounded(),
            });
        assert!(actual.is_ok());
        // The same value-only provider cannot supply an unrequested namespace.
        assert!(
            captured
                .provider()
                .resolve_table_for_analysis(Some("ice"), "audit", "orders")
                .unwrap_err()
                .contains("was not requested")
        );
        assert!(
            matches!(source.capture(&[]), Err(SqlCompileError::Compilation(message))
            if message.contains("did not answer every exact request"))
        );
    }
}

#[cfg(test)]
mod view_definition_tests {
    use super::*;
    use crate::analyze_error::AnalyzeErrorKind;
    use novarocks_parser::ast::{Statement, ViewStatement};

    fn definition(sql: &str) -> Result<Box<[crate::catalog::SqlViewColumn]>, SqlCompileError> {
        let mut statements = novarocks_parser::parse(sql).unwrap();
        let Statement::View(ViewStatement::Create(create)) = statements.remove(0) else {
            panic!("view definition fixture")
        };
        let source = SqlCapturedCatalogPreparation::try_new_definition(
            create.query,
            "iceberg",
            "db",
            None,
            SqlCompileControl::unbounded(),
        )?;
        assert!(source.needs().is_empty());
        source.analyze_view_definition(
            &create.columns,
            crate::functions::builtin_sql_function_catalog(),
            crate::constant::test_constant_policy(),
        )
    }

    #[test]
    fn public_definition_aliases_do_not_rewrite_the_original_optional_header_or_types() {
        let columns = definition("CREATE VIEW v(PublicName, SmallValue) AS WITH t AS (SELECT 1 AS x) SELECT x AS MiXeD, CAST(2 AS SMALLINT) FROM t").unwrap();
        assert_eq!(columns[0].source_field_name.as_deref(), Some("MiXeD"));
        assert_eq!(columns[0].name.as_ref(), "PublicName");
        assert_eq!(columns[1].source_field_name, None);
        assert_eq!(columns[1].name.as_ref(), "SmallValue");
        assert_eq!(
            columns[1].value_type.data_type(),
            &arrow::datatypes::DataType::Int16
        );
        assert!(columns[1].value_type.nullable());
    }

    #[test]
    fn definition_terminal_keeps_width_then_original_ordinal_name_error_order() {
        for (sql, expected) in [
            (
                "CREATE VIEW v(x) AS SELECT 1, 2",
                AnalyzeErrorKind::MismatchedColumnAliases,
            ),
            (
                "CREATE VIEW v AS SELECT 1 AS A, 2 AS a, 3",
                AnalyzeErrorKind::DuplicateColumnName,
            ),
            (
                "CREATE VIEW v AS SELECT 1",
                AnalyzeErrorKind::MissingColumnName,
            ),
            (
                "CREATE VIEW v(A,a) AS SELECT 1, 2",
                AnalyzeErrorKind::DuplicateColumnName,
            ),
        ] {
            let Err(SqlCompileError::Analyze(error)) = definition(sql) else {
                panic!("expected original typed definition failure: {sql}")
            };
            assert_eq!(error.kind(), expected, "{sql}");
            assert!(error.span().is_some());
        }
    }

    #[test]
    fn set_definition_uses_the_original_first_branch_name_and_type() {
        let columns =
            definition("CREATE VIEW v AS SELECT 1 AS FirstName UNION ALL SELECT 2").unwrap();
        assert_eq!(columns[0].source_field_name.as_deref(), Some("FirstName"));
        assert_eq!(columns[0].name.as_ref(), "FirstName");
        assert_eq!(
            columns[0].value_type.data_type(),
            &arrow::datatypes::DataType::Int64
        );
    }

    #[test]
    fn definition_terminal_rejects_an_ordinary_shadowing_capture_context() {
        let source = SqlCapturedCatalogPreparation::try_new(
            {
                let Statement::View(ViewStatement::Create(create)) =
                    novarocks_parser::parse("CREATE VIEW v AS SELECT 1 AS x")
                        .unwrap()
                        .remove(0)
                else {
                    panic!("fixture")
                };
                create.query
            },
            Some("iceberg"),
            "db",
            SqlCompileControl::unbounded(),
        )
        .unwrap();
        assert!(matches!(
            source.analyze_view_definition(
                &[],
                crate::functions::builtin_sql_function_catalog(),
                crate::constant::test_constant_policy()
            ),
            Err(SqlCompileError::InvalidRequest(_))
        ));
    }
}
