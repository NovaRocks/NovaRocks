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

//! Logical EXPLAIN's pure catalog observation and original analysis terminal.

use super::{
    CatalogRelationFact, CatalogRelationNeed, ExplainLevel, SqlAnalyzeRequest, SqlCompileControl,
    SqlCompileError, SqlCompileIntent, SqlCompiler, SqlConstantEvaluator, SqlFunctionCatalog,
    SqlPlannerTableSnapshot, SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput,
    completion_catalog::CatalogCompletionState,
};

/// This source describes exact facts to observe. It is not a completed
/// physical plan and cannot encode a package or grant execution capacity.
pub struct SqlLogicalExplainCatalog {
    catalog: CatalogCompletionState,
    session: SqlSessionContext,
    control: SqlCompileControl,
    root_session_admitted: bool,
}

impl SqlLogicalExplainCatalog {
    pub fn try_new(
        query: novarocks_parser::ast::Query,
        session: SqlSessionContext,
        control: SqlCompileControl,
    ) -> Result<Self, SqlCompileError> {
        Self::try_new_for_input(
            SqlStatementInput::parsed_query(Box::new(query)),
            session,
            control,
        )
    }

    pub fn try_new_for_input(
        input: SqlStatementInput,
        session: SqlSessionContext,
        control: SqlCompileControl,
    ) -> Result<Self, SqlCompileError> {
        Self::try_new_for_input_with_qualifications(
            input,
            session,
            control,
            super::CatalogQualifications::default(),
        )
    }

    pub fn try_new_for_input_with_qualifications(
        input: SqlStatementInput,
        session: SqlSessionContext,
        control: SqlCompileControl,
        qualifications: super::CatalogQualifications,
    ) -> Result<Self, SqlCompileError> {
        control.check()?;
        let (query, root_session_admitted) = match input.kind {
            super::SqlStatementInputKind::ParsedQuery(query) => (*query, false),
            super::SqlStatementInputKind::AdmittedSessionQuery(query) => (*query, true),
            _ => {
                return Err(SqlCompileError::InvalidRequest(
                    "logical EXPLAIN requires a parsed query".into(),
                ));
            }
        };
        let query = if root_session_admitted {
            crate::sql_mode::normalize_concat_query_with_semantics(query, &session.sql_semantics)
        } else {
            crate::sql_mode::normalize_concat_query(query, &session.sql_semantics)
                .map_err(SqlCompileError::from)?
        };
        let catalog = CatalogCompletionState::try_new_with_qualifications(
            query,
            &[],
            &[],
            session.current_catalog.as_deref(),
            &session.current_database,
            crate::catalog::TableLookupMode::ExplainStats,
            0,
            &session.sql_semantics,
            qualifications,
        )?
        .with_exchange_budget(super::DEFAULT_COMPLETION_LIMITS)?;
        Ok(Self {
            catalog,
            session,
            control,
            root_session_admitted,
        })
    }

    pub fn continue_catalog(
        mut self,
        facts: Box<[CatalogRelationFact]>,
    ) -> Result<Self, SqlCompileError> {
        self.catalog.accept_facts(facts, &self.control)?;
        Ok(self)
    }

    pub fn needs(&self) -> &[CatalogRelationNeed] {
        self.catalog.needs()
    }

    pub fn finish(
        self,
        facts: &[CatalogRelationFact],
        functions: &dyn SqlFunctionCatalog,
        evaluator: &'static dyn SqlConstantEvaluator,
        policy: novarocks_functions::ConstantPolicy,
        environment: SqlPlanningEnvironment,
        level: ExplainLevel,
    ) -> Result<Vec<String>, SqlCompileError> {
        self.control.check()?;
        let facts = self.catalog.fact_catalog(facts)?;
        let snapshot = SqlPlannerTableSnapshot::new(&facts);
        let query = Box::new(self.catalog.query().clone());
        let input = if self.root_session_admitted {
            SqlStatementInput::query_with_admitted_session(query)
        } else {
            SqlStatementInput::parsed_query(query)
        };
        let request = SqlAnalyzeRequest::new(
            input,
            SqlCompileIntent::LogicalOnly,
            self.session,
            environment,
            &snapshot,
            functions,
            evaluator,
            None,
            policy,
            self.control.clone(),
        );
        SqlCompiler::analyze(request)?
            .into_complete()?
            .into_explain_lines(level, true, &self.control)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(sql: &str) -> novarocks_parser::ast::Query {
        let query = &mut novarocks_parser::parser::parse_query(sql).unwrap();
        query.clone()
    }
    fn session() -> SqlSessionContext {
        SqlSessionContext {
            sql_semantics: crate::sql_mode::SqlSemanticSettings::default(),
            current_catalog: Some("ice".to_string()),
            current_database: "db".to_string(),
            optimizer_settings: super::super::SessionOptimizerSettings::default(),
        }
    }

    #[test]
    fn logical_catalog_terminal_keeps_the_original_logical_analysis_result() {
        let functions = crate::functions::builtin_sql_function_catalog();
        let control = SqlCompileControl::unbounded();
        let source =
            SqlLogicalExplainCatalog::try_new(query("SELECT 1 AS a"), session(), control.clone())
                .unwrap();
        assert!(source.needs().is_empty());
        let actual = source
            .finish(
                &[],
                functions,
                super::super::noop_constant_evaluator(),
                crate::constant::test_constant_policy(),
                SqlPlanningEnvironment::Distributed,
                ExplainLevel::Normal,
            )
            .unwrap();
        let catalog = crate::planning::catalog::PlannerMemoryCatalog::default();
        let snapshot = SqlPlannerTableSnapshot::new(&catalog);
        let original = SqlAnalyzeRequest::new(
            SqlStatementInput::parsed_query(Box::new(query("SELECT 1 AS a"))),
            SqlCompileIntent::LogicalOnly,
            session(),
            SqlPlanningEnvironment::Distributed,
            &snapshot,
            functions,
            super::super::noop_constant_evaluator(),
            None,
            crate::constant::test_constant_policy(),
            control.clone(),
        );
        let expected = SqlCompiler::analyze(original)
            .unwrap()
            .into_complete()
            .unwrap()
            .into_explain_lines(ExplainLevel::Normal, true, &control)
            .unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn logical_explain_carries_admitted_session_through_catalog_and_final_analysis() {
        let sql = "WITH SESSION decimal_overflow_to_double = true AND true SELECT 1 AS a";
        let query = novarocks_parser::parser::parse_query(sql).unwrap();
        let mut effective = session();
        effective.sql_semantics = effective
            .sql_semantics
            .with_decimal_overflow_to_double(true);
        let logical = SqlLogicalExplainCatalog::try_new_for_input(
            SqlStatementInput::query_with_admitted_session(Box::new(query)),
            effective,
            SqlCompileControl::unbounded(),
        )
        .unwrap();
        assert!(logical.needs().is_empty());
        let lines = logical
            .finish(
                &[],
                crate::functions::builtin_sql_function_catalog(),
                super::super::noop_constant_evaluator(),
                crate::constant::test_constant_policy(),
                SqlPlanningEnvironment::Distributed,
                ExplainLevel::Normal,
            )
            .unwrap();
        assert!(!lines.is_empty());
    }

    #[test]
    fn logical_catalog_terminal_requires_the_exact_declared_observation() {
        let functions = crate::functions::builtin_sql_function_catalog();
        let source = SqlLogicalExplainCatalog::try_new(
            query("SELECT * FROM orders"),
            session(),
            SqlCompileControl::unbounded(),
        )
        .unwrap();
        assert_eq!(source.needs().len(), 1);
        assert!(
            source
                .finish(
                    &[],
                    functions,
                    super::super::noop_constant_evaluator(),
                    crate::constant::test_constant_policy(),
                    SqlPlanningEnvironment::Distributed,
                    ExplainLevel::Normal
                )
                .is_err()
        );
    }
}
