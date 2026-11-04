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

//! Exact system relations retain the original statement memo and CPU owner.
use super::*;
use novarocks_query_application::system_catalog::{
    SystemCatalogInputRequirement, SystemCatalogRelations,
};
use novarocks_sql::compiler::{
    CatalogRelationFact, CatalogRelationNeed, SqlCompileControl, SqlCompileError,
};
use novarocks_sql::planning::catalog::SqlInlineRelation;

pub(super) struct DefinitionInputs {
    pub(super) context: novarocks_spi::connector::ConnectorRequestContext,
    pub(super) policy: novarocks_functions::ConstantPolicy,
    pub(super) control: SqlCompileControl,
}
impl CatalogServiceMaterializer<'_> {
    pub(super) fn system_source(
        &self,
        need: &CatalogRelationNeed,
    ) -> Option<(&SystemCatalogRelations, SystemCatalogInputRequirement)> {
        // Exact generated overlays retain their existing author and precedence.
        let relation = need.relation();
        if self
            .query_overlay(
                Some(&relation.catalog),
                &relation.namespace,
                &relation.table,
            )
            .is_some()
        {
            return None;
        }
        let source = self.system_relations.as_ref()?;
        source.required_inputs(need).map(|inputs| (source, inputs))
    }
    fn system_inputs(&self) -> Result<&DefinitionInputs, SqlCompileError> {
        self.definition_inputs.as_ref().ok_or_else(|| SqlCompileError::Compilation(
            "definition system relation requires its original request, constant policy and control".into()))
    }
    fn system_memo(
        &self,
        need: &CatalogRelationNeed,
    ) -> Option<Result<Option<Arc<SqlInlineRelation>>, SqlCompileError>> {
        self.bindings
            .system_relations
            .lock()
            .expect("statement system relation memo")
            .get(need.relation())
            .cloned()
    }
    fn system_answer(
        need: &CatalogRelationNeed,
        result: Result<Option<Arc<SqlInlineRelation>>, SqlCompileError>,
    ) -> Result<Option<CatalogRelationFact>, SqlCompileError> {
        result?
            .map(|data| {
                CatalogRelationFact::inline(need, data)
                    .map_err(|e| SqlCompileError::Compilation(e.to_string()))
            })
            .transpose()
    }
    fn retain_system(
        &self,
        need: &CatalogRelationNeed,
        result: Result<Option<Arc<SqlInlineRelation>>, SqlCompileError>,
    ) -> Result<Option<CatalogRelationFact>, SqlCompileError> {
        let mut memo = self
            .bindings
            .system_relations
            .lock()
            .expect("statement system relation memo");
        match memo.entry(need.relation().clone()) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(result.clone());
            }
            std::collections::hash_map::Entry::Occupied(_) => {
                return Err(SqlCompileError::Compilation(
                    "serialized system source attempted another observation".into(),
                ));
            }
        }
        drop(memo);
        Self::system_answer(need, result)
    }
    fn system_historical(
        need: &CatalogRelationNeed,
    ) -> Option<Result<Option<CatalogRelationFact>, SqlCompileError>> {
        need.version().map(|_| {
            CatalogRelationFact::inline_time_travel_unsupported(need)
                .map(Some)
                .map_err(|e| SqlCompileError::Compilation(e.to_string()))
        })
    }
    fn system_admission(
        &self,
        need: &CatalogRelationNeed,
    ) -> CatalogResolutionResult<Option<novarocks_catalog_application::CatalogRuntimeObservation>>
    {
        if need.relation().catalog == "default_catalog" {
            Ok(None)
        } else {
            self.require_catalog_admission(&need.relation().catalog)
        }
    }
    pub(super) fn system_relation_fact(
        &self,
        need: &CatalogRelationNeed,
    ) -> Option<Result<Option<CatalogRelationFact>, SqlCompileError>> {
        let (source, required) = self.system_source(need)?;
        Some((|| {
            let admission = self
                .system_admission(need)
                .map_err(CatalogResolutionError::into_compile)?;
            if let Some(result) = Self::system_historical(need) {
                return result;
            }
            if let Some(memo) = self.system_memo(need) {
                return Self::system_answer(need, memo);
            }
            self.bindings
                .require_catalog_observation_allowed()
                .map_err(SqlCompileError::Compilation)?;
            let input = self.system_inputs()?;
            let result = source
                .facts_port()
                .definition_system_catalog_facts(
                    &input.context,
                    &need.relation().catalog,
                    required == SystemCatalogInputRequirement::Tables,
                )
                .and_then(|facts| {
                    facts
                        .map(|facts| source.materialize(need, facts, input.policy, &input.control))
                        .transpose()
                        .map(Option::flatten)
                });
            let verified = self
                .verify_catalog_admission(&need.relation().catalog, admission.as_ref())
                .map_err(CatalogResolutionError::into_compile);
            let fact = self.retain_system(need, result)?;
            verified?;
            Ok(fact)
        })())
    }
    pub(crate) async fn system_relation_fact_on_cpu(
        &self,
        need: &CatalogRelationNeed,
        cpu: &crate::query_execution::completion_facts::FrontendPlanningCpuSource,
    ) -> Option<Result<Option<CatalogRelationFact>, SqlCompileError>> {
        let (source, required) = self.system_source(need)?;
        Some(
            async {
                let admission = self
                    .system_admission(need)
                    .map_err(CatalogResolutionError::into_compile)?;
                if let Some(result) = Self::system_historical(need) {
                    return result;
                }
                if let Some(memo) = self.system_memo(need) {
                    return Self::system_answer(need, memo);
                }
                self.bindings
                    .require_catalog_observation_allowed()
                    .map_err(SqlCompileError::Compilation)?;
                let input = self.system_inputs()?;
                let observed = source
                    .facts_port()
                    .definition_system_catalog_facts_async(
                        &input.context,
                        &need.relation().catalog,
                        required == SystemCatalogInputRequirement::Tables,
                    )
                    .await;
                let result = match observed {
                    Ok(Some(facts)) => cpu
                        .system_relation(
                            source.clone(),
                            need.clone(),
                            facts,
                            input.policy,
                            input.control.clone(),
                        )
                        .await
                        .map_err(|health| {
                            // Raw health remains in the original CPU/custody, which
                            // must close before a statement can finish. This
                            // port cannot manufacture a domain projection for
                            // an opaque process-health cause.
                            SqlCompileError::Compilation(health.detail().into())
                        })?,
                    Ok(None) => Ok(None),
                    Err(error) => Err(error),
                };
                let verified = self
                    .verify_catalog_admission(&need.relation().catalog, admission.as_ref())
                    .map_err(CatalogResolutionError::into_compile);
                let fact = self.retain_system(need, result)?;
                verified?;
                Ok(fact)
            }
            .await,
        )
    }
    pub(super) async fn original_system_relation_fact(
        &self,
        need: &CatalogRelationNeed,
        io: &crate::query_execution::mv_assembly::refresh_preparation::io_gate::MvRefreshPreparationIo,
    ) -> Option<
        Result<Option<CatalogRelationFact>, crate::mv::domain::application::MvApplicationError>,
    > {
        use crate::mv::domain::application::MvApplicationError;
        let (source, required) = self.system_source(need)?;
        Some(async {
            let admission = if need.relation().catalog == "default_catalog" { None } else { self.original_catalog_admission(&need.relation().catalog)? };
            if let Some(result) = Self::system_historical(need) { return result.map_err(MvApplicationError::from_compile); }
            if let Some(memo) = self.system_memo(need) { return Self::system_answer(need, memo).map_err(MvApplicationError::from_compile); }
            self.bindings.require_catalog_observation_allowed().map_err(crate::query_execution::mv_assembly::refresh_preparation::io_gate::contract_error)?;
            let input = self.system_inputs().map_err(MvApplicationError::from_compile)?;
            let observed = io.system_catalog_facts(source.clone(), input.context.clone(), need.relation().catalog.clone(), required == SystemCatalogInputRequirement::Tables).await;
            let result = observed.and_then(|facts| facts.map(|facts| source.materialize(need, facts, input.policy, &input.control)).transpose().map(Option::flatten));
            let current = if need.relation().catalog == "default_catalog" { None } else { self.original_catalog_admission(&need.relation().catalog)? };
            let fact = self.retain_system(need, result).map_err(MvApplicationError::from_compile)?;
            if current != admission { return Err(crate::query_execution::mv_assembly::refresh_preparation::io_gate::contract_error("catalog attachment generation changed while acquiring system facts")); }
            Ok(fact)
        }.await)
    }
}

#[cfg(test)]
mod tests;
