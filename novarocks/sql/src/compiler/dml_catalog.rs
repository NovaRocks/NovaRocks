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

//! Pure catalog observation and analysis for a prepared DML write source.

use super::{
    CatalogRelationFact, CatalogRelationNeed, ConstantPolicy, RootDistributionRequirement,
    SqlAnalyzeRequest, SqlAnalyzedQuery, SqlCompileControl, SqlCompileError, SqlCompileIntent,
    SqlCompiler, SqlConstantEvaluator, SqlFunctionCatalog, SqlPlannerTableSnapshot,
    SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput, SqlStatementInputKind,
    completion_catalog::CatalogCompletionState,
};

/// Owns the original prepared syntax while the application observes the exact
/// catalog facts. No application catalog or provider callback crosses this boundary.
pub struct SqlDmlCatalogPreparation {
    statement: SqlStatementInput,
    catalog: CatalogCompletionState,
    session: SqlSessionContext,
    control: SqlCompileControl,
    intent: SqlCompileIntent,
    root_output_declaration: Option<Vec<novarocks_type_contract::FunctionValueType>>,
}

impl SqlDmlCatalogPreparation {
    pub fn try_new(
        statement: SqlStatementInput,
        session: SqlSessionContext,
        control: SqlCompileControl,
        root_distribution: RootDistributionRequirement,
    ) -> Result<Self, SqlCompileError> {
        Self::try_new_for_intent(
            statement,
            session,
            control,
            SqlCompileIntent::IcebergWrite { root_distribution },
            None,
        )
    }

    /// The same catalog prepass for the closed DML computation families. An
    /// internal COW read retains its complete signed root declaration here.
    pub fn try_new_for_intent(
        statement: SqlStatementInput,
        session: SqlSessionContext,
        control: SqlCompileControl,
        intent: SqlCompileIntent,
        root_output_declaration: Option<Vec<novarocks_type_contract::FunctionValueType>>,
    ) -> Result<Self, SqlCompileError> {
        if !matches!(
            &intent,
            SqlCompileIntent::IcebergWrite { .. }
                | SqlCompileIntent::ChangeStreamWrite
                | SqlCompileIntent::DmlInternalRead
        ) || (root_output_declaration.is_some() && intent != SqlCompileIntent::DmlInternalRead)
        {
            return Err(SqlCompileError::InvalidRequest(
                "DML catalog preparation requires its exact write or internal-read contract".into(),
            ));
        }
        control.check()?;
        let query = match &statement.kind {
            SqlStatementInputKind::ParsedQuery(query)
            | SqlStatementInputKind::MaterializedValuesQuery { query, .. } => (**query).clone(),
            _ => {
                return Err(SqlCompileError::InvalidRequest(
                    "DML catalog preparation requires the original prepared query input".into(),
                ));
            }
        };
        let query = crate::sql_mode::normalize_concat_query(query, &session.sql_semantics)
            .map_err(SqlCompileError::from)?;
        let catalog = CatalogCompletionState::try_new(
            query,
            session.current_catalog.as_deref(),
            &session.current_database,
            crate::catalog::TableLookupMode::SchemaOnly,
            0,
        )?;
        // Validate the producer's structural sites against the actual query
        // retained by the original prepass before requesting external facts.
        if let SqlStatementInputKind::MaterializedValuesQuery { source, .. } = &statement.kind {
            source.loan(catalog.query(), &control)?;
        }
        control.check()?;
        Ok(Self {
            statement,
            catalog,
            session,
            control,
            intent,
            root_output_declaration,
        })
    }

    pub fn needs(&self) -> &[CatalogRelationNeed] {
        self.catalog.needs()
    }

    pub fn analyze(
        self,
        facts: &[CatalogRelationFact],
        functions: &dyn SqlFunctionCatalog,
        evaluator: &'static dyn SqlConstantEvaluator,
        policy: ConstantPolicy,
    ) -> Result<SqlAnalyzedQuery, SqlCompileError> {
        self.control.check()?;
        let catalog = self.catalog.fact_catalog(facts)?;
        let snapshot = SqlPlannerTableSnapshot::new(&catalog);
        let query = Box::new(self.catalog.query().clone());
        let statement = match self.statement.kind {
            SqlStatementInputKind::MaterializedValuesQuery { source, .. } => {
                // Keep the original pool and ordinal. The analysis borrow is
                // rebuilt from structural sites on this actual owned AST.
                source.loan(&query, &self.control)?;
                SqlStatementInput {
                    kind: SqlStatementInputKind::MaterializedValuesQuery { query, source },
                }
            }
            SqlStatementInputKind::ParsedQuery(_) => SqlStatementInput::parsed_query(query),
            _ => {
                return Err(SqlCompileError::InvalidRequest(
                    "DML catalog preparation lost its original prepared query input".into(),
                ));
            }
        };
        let request = SqlAnalyzeRequest::new(
            statement,
            self.intent,
            self.session,
            SqlPlanningEnvironment::Distributed,
            &snapshot,
            functions,
            evaluator,
            None,
            policy,
            self.control,
        );
        let request = match self.root_output_declaration {
            Some(declaration) => request.with_root_output_declaration(declaration)?,
            None => request,
        };
        SqlCompiler::analyze(request)?.into_pending()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::ExprKind;
    use crate::catalog::ResolvedAnalyzerTable;
    use crate::planner::logical::{LogicalPlanKind, LogicalPlanNode};
    use crate::planner::table::{
        ScanSource, SqlScanKind, SqlScanSource, SqlTableIdentity, SqlTableVersionSelector, TableDef,
    };
    use novarocks_functions::ConstantValue;
    use novarocks_type_contract::{CompilePhase, FunctionValueType, ValueLogicalType};
    use std::sync::Arc;

    fn query(sql: &str) -> novarocks_parser::ast::Query {
        let parsed = novarocks_parser::parse(sql).unwrap();
        let [novarocks_parser::ast::Statement::Query(query)] = parsed.as_slice() else {
            panic!("query fixture")
        };
        query.clone()
    }

    fn session() -> SqlSessionContext {
        SqlSessionContext {
            current_catalog: Some("ice".into()),
            current_database: "db".into(),
            optimizer_settings: super::super::SessionOptimizerSettings::default(),
            sql_semantics: crate::sql_mode::SqlSemanticSettings::default(),
        }
    }

    fn prepare(statement: SqlStatementInput) -> SqlDmlCatalogPreparation {
        SqlDmlCatalogPreparation::try_new(
            statement,
            session(),
            SqlCompileControl::unbounded(),
            RootDistributionRequirement::Any,
        )
        .unwrap()
    }

    fn analyze(
        source: SqlDmlCatalogPreparation,
        facts: &[CatalogRelationFact],
    ) -> Result<SqlAnalyzedQuery, SqlCompileError> {
        source.analyze(
            facts,
            crate::functions::builtin_sql_function_catalog(),
            super::super::noop_constant_evaluator(),
            crate::constant::test_constant_policy(),
        )
    }

    #[test]
    fn analysis_consumes_the_exact_table_fact_and_has_no_catalog_fallback() {
        let input = || SqlStatementInput::parsed_query(Box::new(query("SELECT 1 FROM orders")));
        let source = prepare(input());
        assert_eq!(source.needs().len(), 1);
        let need = &source.needs()[0];
        assert_eq!(need.relation().fqn(), "ice.db.orders");
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
        let table = ResolvedAnalyzerTable::from_planner(
            Some(&identity.catalog),
            &identity.namespace,
            table,
        );
        let fact = CatalogRelationFact::resolved(need, table).unwrap();
        assert!(analyze(source, &[fact]).is_ok());
        assert!(matches!(analyze(prepare(input()), &[]),
            Err(SqlCompileError::Compilation(message))
                if message.contains("did not answer every exact request")));
        let source = prepare(input());
        let fact =
            CatalogRelationFact::missing(&source.needs()[0], "original table fact is missing")
                .unwrap();
        assert!(matches!(analyze(source, &[fact]), Err(error)
            if error.to_string().contains("original table fact is missing")));
    }

    fn variant(control: &SqlCompileControl) -> ConstantValue {
        let ty = FunctionValueType::try_with_logical_type(
            arrow::datatypes::DataType::LargeBinary,
            false,
            ValueLogicalType::Variant,
        )
        .unwrap();
        let bytes =
            novarocks_types::value::variant_encode::encode_json_text_to_variant_bytes("null")
                .unwrap();
        ConstantValue::from_binary(
            Arc::new(arrow::datatypes::Field::new(
                "",
                ty.data_type().clone(),
                false,
            )),
            ty,
            &bytes,
            crate::constant::test_constant_policy(),
            CompilePhase::Validate,
            control,
        )
        .unwrap()
    }

    fn constants(node: &LogicalPlanNode) -> Vec<&ConstantValue> {
        let mut found = Vec::new();
        if let LogicalPlanKind::Values(values) = &node.kind {
            for expr in values.rows.iter().flatten() {
                if let ExprKind::Constant(value) = &expr.kind {
                    found.push(value);
                }
            }
        }
        for child in &node.children {
            found.extend(constants(child));
        }
        found
    }

    fn assert_original_constant(analyzed: &SqlAnalyzedQuery, original: &ConstantValue) {
        let retained = constants(&analyzed.logical_plan);
        assert_eq!(retained.len(), 1);
        assert_eq!(
            retained[0].pool().backing_identity(),
            original.pool().backing_identity()
        );
        assert_eq!(
            retained[0].try_binary().unwrap(),
            original.try_binary().unwrap()
        );
        assert_eq!(retained[0].value_type(), original.value_type());
        assert!(
            retained[0].try_binary().unwrap().is_some(),
            "encoded JSON null must not become SQL NULL"
        );
    }

    #[test]
    fn expression_constant_retains_original_pool_through_catalog_prepass_and_analysis() {
        let control = SqlCompileControl::unbounded();
        let value = variant(&control);
        let input = SqlStatementInput::query_with_expression_constants(
            Box::new(query("VALUES (NULL)")),
            vec![super::super::SqlExpressionConstant::new(
                Vec::new(),
                super::super::SqlConstantExpressionSite::ValuesCell { row: 0, column: 0 },
                value.clone(),
            )]
            .into(),
            &control,
        )
        .unwrap();
        let source = prepare(input);
        assert!(source.needs().is_empty());
        let analyzed = analyze(source, &[]).unwrap();
        assert_original_constant(&analyzed, &value);
    }

    #[test]
    fn complete_materialized_rows_retain_the_original_checked_values() {
        let control = SqlCompileControl::unbounded();
        let value = variant(&control);
        let mut query = query("SELECT v.payload FROM (VALUES (0)) AS v(payload)");
        let novarocks_parser::ast::SetExpr::Select(select) = query.body.as_mut() else {
            panic!("SELECT fixture")
        };
        let novarocks_parser::ast::TableFactor::Derived { subquery, .. } =
            &mut select.from[0].relation
        else {
            panic!("derived fixture")
        };
        let novarocks_parser::ast::SetExpr::Values(values) = subquery.body.as_mut() else {
            panic!("VALUES fixture")
        };
        values.rows.clear();
        let input = SqlStatementInput::materialized_values_query(
            Box::new(query),
            super::super::SqlMaterializedValuesSite::SelectFromDerived { from: 0 },
            vec![vec![value.clone()].into_boxed_slice()].into(),
            &control,
        )
        .unwrap();
        let analyzed = analyze(prepare(input), &[]).unwrap();
        assert_original_constant(&analyzed, &value);
    }

    #[test]
    fn internal_read_preserves_its_signed_root_declaration_through_async_catalog_preparation() {
        let declaration = vec![novarocks_type_contract::FunctionValueType::new(
            arrow::datatypes::DataType::Int16,
            true,
        )];
        let source = SqlDmlCatalogPreparation::try_new_for_intent(
            SqlStatementInput::parsed_query(Box::new(query("SELECT 1 AS __nr_sel_0"))),
            session(),
            SqlCompileControl::unbounded(),
            SqlCompileIntent::DmlInternalRead,
            Some(declaration.clone()),
        )
        .unwrap();
        assert!(source.needs().is_empty());
        assert_eq!(source.root_output_declaration.as_ref(), Some(&declaration));
        let analyzed = analyze(source, &[]).expect("signed internal read analyzes");
        assert_eq!(analyzed.intent, SqlCompileIntent::DmlInternalRead);
        let output = crate::planner::plan_output_columns(&analyzed.logical_plan).unwrap();
        assert_eq!(output.len(), 1);
        // The declaration changes the original integer width. Analysis may
        // retain a stronger non-NULL promise; every other type fact stays exact.
        assert_eq!(
            output[0].value_type.data_type(),
            &arrow::datatypes::DataType::Int16
        );
        assert_eq!(
            output[0].value_type.with_nullable(true).unwrap(),
            declaration[0]
        );
    }

    #[test]
    fn change_stream_preserves_its_compile_intent_and_refuses_internal_read_declarations() {
        let source = SqlDmlCatalogPreparation::try_new_for_intent(
            SqlStatementInput::parsed_query(Box::new(query("SELECT 1"))),
            session(),
            SqlCompileControl::unbounded(),
            SqlCompileIntent::ChangeStreamWrite,
            None,
        )
        .unwrap();
        assert_eq!(
            analyze(source, &[]).unwrap().intent,
            SqlCompileIntent::ChangeStreamWrite
        );
        assert!(
            SqlDmlCatalogPreparation::try_new_for_intent(
                SqlStatementInput::parsed_query(Box::new(query("SELECT 1"))),
                session(),
                SqlCompileControl::unbounded(),
                SqlCompileIntent::ChangeStreamWrite,
                Some(vec![novarocks_type_contract::FunctionValueType::new(
                    arrow::datatypes::DataType::Int64,
                    false
                )]),
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_unprepared_sql_instead_of_reparsing_an_application_statement() {
        assert!(matches!(
            SqlDmlCatalogPreparation::try_new(
                SqlStatementInput::sql("SELECT 1"),
                session(),
                SqlCompileControl::unbounded(),
                RootDistributionRequirement::Any,
            ),
            Err(SqlCompileError::InvalidRequest(_))
        ));
    }
}
