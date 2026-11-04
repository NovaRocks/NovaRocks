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

use super::{SystemCatalog, SystemCatalogInputRequirement};
use crate::system_catalog_rewrite::{
    INFORMATION_SCHEMA_DB, SystemCatalogFacts, SystemCatalogFactsPort,
};
use novarocks_sql::compiler::{
    CatalogLookupTarget, CatalogRelationNeed, SqlCompileControl, SqlCompileError,
};
use novarocks_sql::planning::catalog::{SqlInlineColumn, SqlInlineRelation};
use std::sync::Arc;

/// The same role-installed readers and registry used by ordinary system reads.
/// This value supplies one relation; it never walks or rewrites a view body.
#[derive(Clone)]
pub struct SystemCatalogRelations {
    facts: Arc<dyn SystemCatalogFactsPort>,
    catalog: Arc<dyn SystemCatalog>,
}
impl SystemCatalogRelations {
    pub fn new(facts: Arc<dyn SystemCatalogFactsPort>, catalog: Arc<dyn SystemCatalog>) -> Self {
        Self { facts, catalog }
    }
    pub fn facts_port(&self) -> &Arc<dyn SystemCatalogFactsPort> {
        &self.facts
    }
    pub fn system_catalog(&self) -> &Arc<dyn SystemCatalog> {
        &self.catalog
    }
    pub fn required_inputs(
        &self,
        need: &CatalogRelationNeed,
    ) -> Option<SystemCatalogInputRequirement> {
        if !need.is_definition_context()
            || !matches!(need.target(), CatalogLookupTarget::Table { .. })
            || !need
                .relation()
                .namespace
                .eq_ignore_ascii_case(INFORMATION_SCHEMA_DB)
        {
            return None;
        }
        self.catalog
            .required_inputs(&need.relation().namespace, &need.relation().table)
    }
    /// Pure materialization runs in the original caller CPU turn. Checked
    /// Arrow pools retain the actual values, exact type and source owner;
    /// they are never re-admitted as printed SQL literals.
    pub fn materialize(
        &self,
        need: &CatalogRelationNeed,
        facts: SystemCatalogFacts,
        policy: novarocks_sql::compiler::ConstantPolicy,
        control: &SqlCompileControl,
    ) -> Result<Option<Arc<SqlInlineRelation>>, SqlCompileError> {
        novarocks_type_contract::PureCompileControl::checkpoint(
            control,
            novarocks_type_contract::CompilePhase::Validate,
            0,
        )?;
        if self.required_inputs(need).is_none() {
            return Ok(None);
        }
        if facts.catalog_name != need.relation().catalog {
            return Err(SqlCompileError::InvalidRequest(
                "system catalog facts name another requested catalog".into(),
            ));
        }
        let inputs = super::SystemCatalogInputs {
            catalog_name: &facts.catalog_name,
            schema_names: &facts.schema_names,
            table_names: &facts.table_names,
        };
        let Some(data) = self
            .catalog
            .resolve(&need.relation().namespace, &need.relation().table, &inputs)
            .map_err(SqlCompileError::Compilation)?
        else {
            return Ok(None);
        };
        let columns = data
            .columns
            .iter()
            .map(|column| SqlInlineColumn::from_catalog_column(column, control))
            .collect::<Result<Vec<_>, _>>()?;
        let mut batches = Vec::with_capacity(data.batches.len());
        for batch in &data.batches {
            novarocks_type_contract::PureCompileControl::checkpoint(
                control,
                novarocks_type_contract::CompilePhase::Validate,
                0,
            )?;
            if batch.num_columns() != columns.len() {
                return Err(SqlCompileError::InvalidRequest(
                    "system catalog batch width differs from its declaration".into(),
                ));
            }
            let schema = batch.schema();
            let mut pools = Vec::with_capacity(columns.len());
            for (ordinal, column) in columns.iter().enumerate() {
                novarocks_type_contract::PureCompileControl::checkpoint(
                    control,
                    novarocks_type_contract::CompilePhase::Validate,
                    0,
                )?;
                let pool = novarocks_physical_plan::ConstantPool::try_new(
                    Arc::clone(&schema.fields()[ordinal]),
                    column.value_type.clone(),
                    batch.column(ordinal).to_data(),
                    policy,
                    novarocks_type_contract::CompilePhase::Validate,
                    control,
                )?;
                pools.push(pool);
            }
            batches.push(pools.into_boxed_slice());
        }
        SqlInlineRelation::try_new(
            columns.into_boxed_slice(),
            batches.into_boxed_slice(),
            control,
        )
        .map(Arc::new)
        .map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::system_catalog::SystemCatalogService;
    use novarocks_spi::connector::ConnectorRequestContext;
    struct UnusedFacts;
    impl SystemCatalogFactsPort for UnusedFacts {
        fn local_system_catalog_facts(&self) -> Result<SystemCatalogFacts, String> {
            panic!("pure materialization cannot read facts")
        }
        fn external_system_catalog_facts(
            &self,
            _: &ConnectorRequestContext,
            _: &str,
            _: bool,
        ) -> Result<Option<SystemCatalogFacts>, String> {
            panic!("pure materialization cannot read facts")
        }
    }
    fn preparation(sql: &str) -> novarocks_sql::compiler::SqlCapturedCatalogPreparation {
        let statements = novarocks_parser::parse(sql).unwrap();
        let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("query")
        };
        novarocks_sql::compiler::SqlCapturedCatalogPreparation::try_new_definition(
            query.clone(),
            "ice",
            "db",
            None,
            SqlCompileControl::unbounded(),
        )
        .unwrap()
    }
    #[test]
    fn original_system_registry_materializes_the_same_full_declaration_for_empty_and_nonempty_rows()
    {
        let pair = SystemCatalogRelations::new(
            Arc::new(UnusedFacts),
            Arc::new(SystemCatalogService::with_defaults()),
        );
        let mut declarations = Vec::new();
        for schemas in [vec![], vec!["db".into()]] {
            let preparation = preparation(
                "SELECT catalog_name, schema_name, sql_path FROM information_schema.schemata",
            );
            let [need] = preparation.needs() else {
                panic!("one need")
            };
            assert_eq!(
                pair.required_inputs(need),
                Some(SystemCatalogInputRequirement::Schemas)
            );
            let data = pair
                .materialize(
                    need,
                    SystemCatalogFacts {
                        catalog_name: "ice".into(),
                        schema_names: schemas,
                        table_names: vec![],
                    },
                    crate::completed_plan_fixture::test_constant_policy(),
                    &SqlCompileControl::unbounded(),
                )
                .unwrap()
                .unwrap();
            let fact = novarocks_sql::compiler::CatalogRelationFact::inline(need, data).unwrap();
            declarations.push(
                preparation
                    .continue_catalog(vec![fact].into_boxed_slice())
                    .unwrap()
                    .analyze_view_definition(
                        &[],
                        novarocks_sql::compiler::builtin_sql_function_catalog(),
                        crate::completed_plan_fixture::test_constant_policy(),
                    )
                    .unwrap(),
            );
        }
        assert_eq!(declarations[0], declarations[1]);
        assert!(!declarations[0][0].value_type.nullable());
        assert!(declarations[0][2].value_type.nullable());
        assert_eq!(
            declarations[0][2].value_type.data_type(),
            &arrow::datatypes::DataType::Utf8
        );
    }
}
