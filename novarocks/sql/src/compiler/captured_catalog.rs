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
        )?;
        control.check()?;
        Ok(Self { catalog, control })
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
