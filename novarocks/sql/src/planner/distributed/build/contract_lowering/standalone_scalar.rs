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

//! Required scalar computation uses the original emission and unit authors.
//! This owner contains no plan topology, live registry or capacity grant.
use super::super::unit_source::{StandaloneSourceBrand, UnitDefinitions};
use super::scalar_emission::{ScalarEmission, ScalarStorage, StandaloneScalarStorage};
use super::*;

pub(crate) struct StandaloneScalarEmission {
    unit: Arc<novarocks_expression_contract::BoundExprSet>,
    root: RootOrdinal,
    constants: novarocks_physical_plan::ConstantPools,
    parameters: SemanticParameters,
    functions: Arc<dyn crate::compiler::SqlFunctionCatalog>,
    policy: novarocks_constant_contract::ConstantPolicy,
    brand: StandaloneSourceBrand,
    journal: SqlLogicalSourceJournal,
}
impl StandaloneScalarEmission {
    pub(crate) fn unit(&self) -> &Arc<novarocks_expression_contract::BoundExprSet> {
        &self.unit
    }
    pub(crate) fn root(&self) -> RootOrdinal {
        self.root
    }
    pub(crate) fn constants(&self) -> &novarocks_physical_plan::ConstantPools {
        &self.constants
    }
    pub(crate) fn parameters(&self) -> &SemanticParameters {
        &self.parameters
    }
    pub(crate) fn function_catalog(&self) -> &Arc<dyn crate::compiler::SqlFunctionCatalog> {
        &self.functions
    }
    pub(crate) fn constant_policy(&self) -> novarocks_constant_contract::ConstantPolicy {
        self.policy
    }
    pub(crate) fn value_type(&self) -> &novarocks_type_contract::FunctionValueType {
        let root = self
            .unit
            .root(self.root)
            .expect("the completed standalone root was checked");
        &self
            .unit
            .definition(root.definition)
            .expect("the completed root definition was checked")
            .ty
    }
    fn source_loan(&self) -> SqlUnitSourceLoan<'_> {
        SqlUnitSourceLoan {
            site: ScalarSourceSite::Standalone(self.brand.clone()),
            definitions: UnitDefinitions::from_unit(
                ScalarUnitOwner::Standalone(self.brand.clone()),
                &self.unit,
            ),
            constants: &self.constants,
            parameters: &self.parameters,
            journal: &self.journal,
            functions: &self.functions,
        }
    }
}

/// A genuine draft source, retained until the one common finisher consumes it.
struct StandaloneDraft {
    brand: StandaloneSourceBrand,
    draft: novarocks_expression_contract::BoundExprSetBuilder,
    events: Vec<ExprId>,
    constants: constant_emission::SqlConstantEmission,
    lambda_scope: Vec<LoweringLambdaScope>,
    journal: SqlLogicalSourceJournal,
    functions: Arc<dyn crate::compiler::SqlFunctionCatalog>,
    policy: novarocks_constant_contract::ConstantPolicy,
    allow_throw_exception: bool,
    root_used: bool,
}
impl StandaloneDraft {
    fn new(
        functions: Arc<dyn crate::compiler::SqlFunctionCatalog>,
        allow_throw_exception: bool,
        policy: novarocks_constant_contract::ConstantPolicy,
    ) -> Self {
        Self {
            brand: StandaloneSourceBrand::new(),
            draft: novarocks_expression_contract::BoundExprSetBuilder::new(
                novarocks_expression_contract::UnitLimits {
                    expression_semantic_depth: novarocks_physical_plan::PlanLimits::FROZEN
                        .expression_semantic_depth,
                },
            ),
            events: Vec::new(),
            constants: constant_emission::SqlConstantEmission::new(),
            lambda_scope: Vec::new(),
            journal: SqlLogicalSourceJournal::new(),
            functions,
            policy,
            allow_throw_exception,
            root_used: false,
        }
    }
    fn emission<'emission, 'control>(
        &'emission mut self,
        control: &'control dyn PureCompileControl,
        work: &'emission mut CompileCheckpoints<'control>,
    ) -> ScalarEmission<'emission, 'control> {
        ScalarEmission {
            root_allow_throw_exception: self.allow_throw_exception,
            constant_policy: self.policy,
            root_allow_throw_exception_used: &mut self.root_used,
            functions: &self.functions,
            control,
            work,
            constant_emission: &mut self.constants,
            lambda_scope: &mut self.lambda_scope,
            storage: ScalarStorage::Standalone(StandaloneScalarStorage {
                brand: &self.brand,
                draft: &mut self.draft,
                events: &mut self.events,
            }),
            call_sources: &mut self.journal,
        }
    }
    fn rebind_nullability<'control>(
        &mut self,
        control: &'control dyn PureCompileControl,
        work: &mut CompileCheckpoints<'control>,
    ) -> Result<(), ContractLoweringError> {
        let mut dirty = BTreeSet::new();
        loop {
            let mut changed = false;
            let mut events = Vec::new();
            for &definition in &self.events {
                work.flush()?;
                events
                    .try_reserve(1)
                    .map_err(|_| CompileControlError::ResourceExhausted)?;
                events.push(definition);
                work.step()?;
            }
            for definition in events {
                if self.emission(control, work).rebind_definition(
                    definition,
                    |dependency| dirty.contains(&dependency),
                    |_| false,
                )? {
                    dirty.insert(definition);
                    work.step()?;
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
        work.flush()?;
        Ok(())
    }
}

pub(crate) fn lower_required_constant_unit(
    expression: &TypedExpr,
    functions: Arc<dyn crate::compiler::SqlFunctionCatalog>,
    allow_throw_exception: bool,
    policy: novarocks_constant_contract::ConstantPolicy,
    control: &dyn PureCompileControl,
) -> Result<StandaloneScalarEmission, ContractLoweringError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    work.flush()?;
    let mut source = StandaloneDraft::new(functions, allow_throw_exception, policy);
    work.step()?;
    // These are original lowering-frontier failures, before finish_draft's
    // ordinary footer. In particular, a source Limit must remain primary.
    let definition = source
        .emission(control, &mut work)
        .lower_expression(expression, &BTreeMap::new())?;
    let root = source.emission(control, &mut work).root_definition(
        definition,
        novarocks_type_contract::EvaluationDemand::Value,
        novarocks_expression_contract::RootScope::AllPorts,
    )?;
    let result = (|| {
        source.rebind_nullability(control, &mut work)?;
        let parameters = scalar_emission::freeze_semantic_parameters(
            source.root_used,
            source.allow_throw_exception,
            BTreeMap::new(),
        )?;
        let site = ScalarSourceSite::Standalone(source.brand.clone());
        let loan = SqlUnitSourceLoan {
            site: site.clone(),
            definitions: UnitDefinitions::from_builder(site.owner(), &source.draft),
            constants: source.constants.pools(),
            parameters: &parameters,
            journal: &source.journal,
            functions: &source.functions,
        };
        let presence = source.functions.snapshot_for_scalar_presence();
        unit_authorship::admit_scalar_unit_sources_observed(
            &loan,
            &source.draft,
            presence.as_ref(),
            control,
            &mut work,
        )?;
        for (address, entry) in &source.journal.expression_entries {
            work.step()?;
            if address.site != site {
                return Err(ContractLoweringError::InvalidFunctionBinding {
                    detail: "standalone scalar journal has a foreign source brand".into(),
                });
            }
            let request = neutral_requests::transfer_expression_request_observed(
                entry,
                &mut work,
                &mut |value, work| source.constants.register(value, value.value_type(), work),
            )?;
            source.draft.request(address.definition, request)?;
            work.step()?;
        }
        let authored = unit_authorship::finish_scalar_unit_observed(
            site,
            source.draft,
            || Ok(Vec::new()),
            source.constants.pools(),
            &parameters,
            &source.journal,
            &source.functions,
            policy,
            control,
            &mut work,
        )?;
        Ok(StandaloneScalarEmission {
            unit: authored.unit,
            root,
            constants: source.constants.into_pools(),
            parameters,
            functions: source.functions,
            policy,
            brand: source.brand,
            journal: source.journal,
        })
    })();
    if let Err(ContractLoweringError::Control(cause)) = &result {
        return Err(ContractLoweringError::Control(cause.clone()));
    }
    work.finish()?;
    result
}

#[cfg(test)]
#[path = "standalone_scalar/tests.rs"]
pub(super) mod tests;
