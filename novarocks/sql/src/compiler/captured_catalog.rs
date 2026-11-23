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
    description: Option<Box<DescriptionCapture>>,
}

struct DescriptionCapture {
    session: super::SqlSessionContext,
    parameters: Box<[novarocks_parser::Span]>,
}

impl SqlCapturedCatalogPreparation {
    pub fn try_new(
        query: novarocks_parser::ast::Query,
        current_catalog: Option<&str>,
        current_database: &str,
        control: SqlCompileControl,
    ) -> Result<Self, SqlCompileError> {
        Self::try_new_with_qualifications(
            query,
            current_catalog,
            current_database,
            control,
            super::CatalogQualifications::default(),
        )
    }

    pub fn try_new_with_qualifications(
        query: novarocks_parser::ast::Query,
        current_catalog: Option<&str>,
        current_database: &str,
        control: SqlCompileControl,
        qualifications: super::CatalogQualifications,
    ) -> Result<Self, SqlCompileError> {
        Self::try_new_qualified_catalog(
            query,
            current_catalog,
            current_database,
            None,
            control,
            qualifications,
            &crate::sql_mode::SqlSemanticSettings::default(),
        )
    }

    /// Requests only the schema facts consulted by the effective statement.
    /// Unknown query-period selectors are rejected before any lookup is published.
    pub fn try_new_description(
        query: novarocks_parser::ast::Query,
        parameters: &super::SqlPreparedParameters,
        session: &super::SqlSessionContext,
        control: SqlCompileControl,
        qualifications: super::CatalogQualifications,
    ) -> Result<Self, SqlCompileError> {
        control.check()?;
        if !parameters.is_description() {
            return Err(SqlCompileError::InvalidRequest(
                "Description requires the unbound parameter input".into(),
            ));
        }
        let query = parameters.complete_query(query)?;
        let mut preparation = Self::try_new_qualified_catalog(
            query,
            session.current_catalog.as_deref(),
            &session.current_database,
            None,
            control,
            qualifications,
            &session.sql_semantics,
        )?;
        preparation.description = Some(Box::new(DescriptionCapture {
            session: session.clone(),
            parameters: parameters.positions().into(),
        }));
        Ok(preparation)
    }

    pub fn try_new_definition(
        query: novarocks_parser::ast::Query,
        catalog: &str,
        database: &str,
        target: Option<novarocks_types::naming::TableIdentity>,
        control: SqlCompileControl,
    ) -> Result<Self, SqlCompileError> {
        Self::try_new_definition_with_qualifications(
            query,
            catalog,
            database,
            target,
            control,
            super::CatalogQualifications::default(),
        )
    }

    pub fn try_new_definition_with_qualifications(
        query: novarocks_parser::ast::Query,
        catalog: &str,
        database: &str,
        target: Option<novarocks_types::naming::TableIdentity>,
        control: SqlCompileControl,
        qualifications: super::CatalogQualifications,
    ) -> Result<Self, SqlCompileError> {
        Self::try_new_qualified_catalog(
            query,
            Some(catalog),
            database,
            Some(target),
            control,
            qualifications,
            &crate::sql_mode::SqlSemanticSettings::default(),
        )
    }

    fn try_new_qualified_catalog(
        query: novarocks_parser::ast::Query,
        current_catalog: Option<&str>,
        current_database: &str,
        definition: Option<Option<novarocks_types::naming::TableIdentity>>,
        control: SqlCompileControl,
        qualifications: super::CatalogQualifications,
        sql_semantics: &crate::sql_mode::SqlSemanticSettings,
    ) -> Result<Self, SqlCompileError> {
        control.check()?;
        let catalog = CatalogCompletionState::try_new_with_qualifications(
            query,
            &[],
            &[],
            current_catalog,
            current_database,
            crate::catalog::TableLookupMode::SchemaOnly,
            0,
            sql_semantics,
            qualifications,
        )?;
        let catalog = if let Some(target) = definition {
            catalog.with_definition_context(target, &control)?
        } else {
            catalog
        }
        .with_exchange_budget(super::DEFAULT_COMPLETION_LIMITS)?;
        control.check()?;
        Ok(Self {
            catalog,
            control,
            description: None,
        })
    }

    pub fn continue_catalog(
        mut self,
        facts: Box<[CatalogRelationFact]>,
    ) -> Result<Self, SqlCompileError> {
        self.catalog.accept_facts(facts, &self.control)?;
        Ok(self)
    }

    /// Analyze using the completed SchemaOnly capture and, for a write
    /// source, the original target assignment declaration supplied by its owner.
    pub fn describe(
        self,
        parameters: &super::SqlPreparedParameters,
        session: &super::SqlSessionContext,
        functions: &dyn super::SqlFunctionCatalog,
        policy: super::ConstantPolicy,
        write_assignments: Option<&super::SqlWriteAssignments>,
        materialized_constants: Option<super::SqlExpressionConstants>,
    ) -> Result<super::SqlPreparedDescription, SqlCompileError> {
        self.describe_inner(
            parameters,
            session,
            functions,
            policy,
            write_assignments,
            materialized_constants,
            false,
        )
    }

    /// Analyze a CTAS source and validate its persistent output declaration.
    /// Source analysis needs no optimization, Provider read, or writer request.
    pub fn describe_ctas_source(
        self,
        parameters: &super::SqlPreparedParameters,
        session: &super::SqlSessionContext,
        functions: &dyn super::SqlFunctionCatalog,
        policy: super::ConstantPolicy,
    ) -> Result<super::SqlPreparedDescription, SqlCompileError> {
        self.describe_inner(parameters, session, functions, policy, None, None, true)
    }

    #[expect(clippy::too_many_arguments, reason = "Original description facts")]
    fn describe_inner(
        self,
        parameters: &super::SqlPreparedParameters,
        session: &super::SqlSessionContext,
        functions: &dyn super::SqlFunctionCatalog,
        policy: super::ConstantPolicy,
        write_assignments: Option<&super::SqlWriteAssignments>,
        materialized_constants: Option<super::SqlExpressionConstants>,
        ctas_source: bool,
    ) -> Result<super::SqlPreparedDescription, SqlCompileError> {
        self.control.check()?;
        let capture = self.description.as_deref().ok_or_else(|| {
            SqlCompileError::InvalidRequest("Description requires its own schema capture".into())
        })?;
        if !parameters.is_description()
            || capture.parameters.as_ref() != parameters.positions()
            || capture.session != *session
        {
            return Err(SqlCompileError::InvalidRequest(
                "Description input differs from its captured statement".into(),
            ));
        }
        let catalog = self.catalog.completed_catalog()?;
        let functions = functions.snapshot_for_scalar_presence();
        let materialized = materialized_constants
            .map(|constants| {
                super::materialized_values::MaterializedValues::try_new_expressions(
                    self.catalog.query(),
                    constants,
                    &self.control,
                )
            })
            .transpose()?;
        crate::analyzer::describe_prepared_query(
            self.catalog.query(),
            &catalog,
            &session.current_database,
            functions.as_ref(),
            &session.sql_semantics,
            policy,
            &self.control,
            write_assignments,
            materialized.as_ref(),
            parameters,
            ctas_source,
        )
    }

    /// Analyze the original definition after every exact dependency is known.
    /// Every public view name comes from the query's analyzed visible fields.
    pub fn analyze_view_definition(
        self,
        functions: &dyn super::SqlFunctionCatalog,
        sealed_functions: std::sync::Arc<novarocks_functions::PureEngineFunctionCatalog>,
        policy: super::ConstantPolicy,
    ) -> Result<Box<[crate::catalog::SqlViewColumn]>, SqlCompileError> {
        let database = self.catalog.definition_database()?;
        self.control.check()?;
        let catalog = self.catalog.completed_catalog()?;
        let semantics = crate::sql_mode::SqlSemanticSettings::default();
        crate::sql_mode::validate_persisted_query_semantics_typed(self.catalog.query(), &semantics)
            .map_err(SqlCompileError::from)?;
        let query = self.catalog.query().clone();
        let functions = functions.snapshot_for_scalar_presence();
        let table_sequence = super::table_sequence::TableSequencePass::SchemaOnly(sealed_functions);
        let (resolved, _, factory) =
            crate::analyzer::analyze_with_write_assignments_and_sql_semantics(
                &query,
                &catalog,
                database,
                functions.as_ref(),
                &semantics,
                policy,
                &self.control,
                None,
                None,
                false,
                None,
                Some(&table_sequence),
            )
            .map_err(SqlCompileError::from)?;
        let names = crate::analysis::schema::validate_definition_column_names(
            &resolved.output_columns,
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
                name: name.into(),
                value_type: crate::analyzer::view_value_type(&source.value_type, &self.control)?,
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
        let query = &novarocks_parser::parser::parse_query(sql).unwrap();
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
    use novarocks_parser::ast::TrinoStatement;

    fn definition(sql: &str) -> Result<Box<[crate::catalog::SqlViewColumn]>, SqlCompileError> {
        let statement = novarocks_parser::parser::parse_statement(sql).unwrap();
        let TrinoStatement::CreateView(create) = statement else {
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
            crate::functions::builtin_sql_function_catalog(),
            novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
            crate::constant::test_constant_policy(),
        )
    }

    #[test]
    fn public_definition_uses_query_names_and_keeps_types_without_null_promises() {
        let columns = definition("CREATE VIEW v AS WITH t AS (SELECT 1 AS x) SELECT x AS PublicName, CAST(2 AS SMALLINT) AS SmallValue FROM t").unwrap();
        assert_eq!(columns[0].name.as_ref(), "PublicName");
        assert_eq!(columns[1].name.as_ref(), "SmallValue");
        assert_eq!(
            columns[1].value_type.data_type(),
            &arrow::datatypes::DataType::Int16
        );
        assert!(columns[1].value_type.nullable());
    }

    #[test]
    fn table_sequence_view_declares_columns_without_sampling_the_saved_argument() {
        let columns = definition(
            "CREATE VIEW v AS SELECT sequential_number AS n FROM TABLE(sequence(stop => 3 + CAST(floor(random()) AS BIGINT)))",
        )
        .unwrap();
        assert_eq!(columns[0].name.as_ref(), "n");
        assert_eq!(
            columns[0].value_type.data_type(),
            &arrow::datatypes::DataType::Int64
        );
        let rejected = definition(
            "CREATE VIEW v AS SELECT sequential_number AS n FROM TABLE(sequence(stop => (SELECT 3)))",
        );
        assert!(matches!(rejected, Err(SqlCompileError::Analyze(error))
            if error.kind() == AnalyzeErrorKind::ExpressionNotConstant));
    }

    #[test]
    fn public_view_types_widen_nested_nullability_without_erasing_domains() {
        use arrow::datatypes::{DataType, Field};
        use novarocks_type_contract::{FunctionValueType, ValueKind, ValueLogicalType};
        let source = FunctionValueType::new(
            DataType::Struct(vec![Field::new("a", DataType::Int32, false)].into()),
            false,
        );
        let public =
            crate::analyzer::view_value_type(&source, &SqlCompileControl::unbounded()).unwrap();
        assert!(!source.nullable());
        let ValueKind::Struct(source_fields) = source.value_type().kind() else {
            panic!("source Struct declaration")
        };
        assert!(!source_fields[0].value_type().nullable());
        assert!(public.nullable());
        let ValueKind::Struct(fields) = public.value_type().kind() else {
            panic!("public Struct declaration")
        };
        assert_eq!(fields[0].name(), "a");
        assert!(fields[0].value_type().nullable());
        assert_eq!(
            public.data_type(),
            &DataType::Struct(vec![Field::new("a", DataType::Int32, true)].into())
        );
        let columns = definition("CREATE VIEW v AS SELECT CAST([] AS ARRAY<json>) AS js").unwrap();
        assert!(columns[0].value_type.nullable());
        let ValueKind::List(item) = columns[0].value_type.value_type().kind() else {
            panic!("ARRAY declaration")
        };
        assert!(item.nullable());
        assert_eq!(item.kind(), &ValueKind::Nominal(ValueLogicalType::Json));
    }

    #[test]
    fn definition_terminal_keeps_original_ordinal_name_error_order() {
        for (sql, expected) in [
            (
                "CREATE VIEW v AS SELECT 1 AS A, 2 AS a, 3",
                AnalyzeErrorKind::DuplicateColumnName,
            ),
            (
                "CREATE VIEW v AS SELECT 1",
                AnalyzeErrorKind::MissingColumnName,
            ),
            (
                "CREATE VIEW v AS SELECT 1 AS A, 2 AS a",
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
        assert_eq!(columns[0].name.as_ref(), "FirstName");
        assert_eq!(
            columns[0].value_type.data_type(),
            &arrow::datatypes::DataType::Int32
        );
    }

    #[test]
    fn definition_terminal_rejects_an_ordinary_shadowing_capture_context() {
        let source = SqlCapturedCatalogPreparation::try_new(
            {
                let TrinoStatement::CreateView(create) =
                    novarocks_parser::parser::parse_statement("CREATE VIEW v AS SELECT 1 AS x")
                        .unwrap()
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
                crate::functions::builtin_sql_function_catalog(),
                novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
                crate::constant::test_constant_policy()
            ),
            Err(SqlCompileError::InvalidRequest(_))
        ));
    }
}
