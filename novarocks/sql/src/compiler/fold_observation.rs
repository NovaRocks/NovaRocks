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

//! Borrowed dependency facts at the original single-node folding invocation.
//! Observation owns no binding, provenance, constant backing, or capacity grant.

use super::{FoldRequest, SqlConstantEvaluationError, SqlConstantEvaluator};
use crate::binding::SqlFunctionBinding;
use novarocks_functions::ConstantValue;
use novarocks_type_contract::PureCompileControl;

/// Positive source classification from the original optimizer node.
/// Intrinsics have no function identity; absence is never interpreted as a name.
#[derive(Clone, Copy, Debug)]
pub enum SqlFoldDependencySource<'a> {
    Function(&'a SqlFunctionBinding),
    Intrinsic,
}

impl<'a> SqlFoldDependencySource<'a> {
    pub(crate) fn from_node(node: &'a super::FrozenFoldNode) -> Self {
        match node {
            super::FrozenFoldNode::Call(call) => Self::Function(call.binding()),
            super::FrozenFoldNode::Intrinsic(_) => Self::Intrinsic,
        }
    }
}

/// One synchronous loan of the exact original binding and already-admitted CVs.
/// Function selection is borrowed before the original node can be replaced.
#[derive(Clone, Copy, Debug)]
pub struct SqlFoldDependencyInput<'a> {
    pub source: SqlFoldDependencySource<'a>,
    pub request: &'a FoldRequest,
}

/// The evaluator outcome, before the optimizer's original intern/publication.
/// ProducedConstant does not claim the completed plan has been published.
#[derive(Clone, Copy, Debug)]
pub enum SqlFoldEvaluationOutcome<'a> {
    ProducedConstant(&'a ConstantValue),
    Declined,
    Error(&'a SqlConstantEvaluationError),
}

/// Request-owned observer. The evaluator itself remains process-lifetime/stateless.
pub trait SqlFoldDependencyObserver: Send + Sync {
    /// Admit the complete observation receipt before the original calculation.
    fn before_fold_dependency_observed(
        &self,
        input: SqlFoldDependencyInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), novarocks_type_contract::CompileControlError>;
    /// Only complete the already-admitted receipt; no new fallible work is allowed.
    fn after_fold_dependency_observed(
        &self,
        input: SqlFoldDependencyInput<'_>,
        outcome: SqlFoldEvaluationOutcome<'_>,
    );

    /// Borrow the complete source only after its original publication succeeds.
    /// This loans no kernel coverage, resource grant, or statement-success proof.
    /// The host admits its receipt work; only original compile control may refuse.
    /// Existing observers add no work or checkpoints by default.
    fn observe_published_source_observed(
        &self,
        _source: &super::SqlAuthoredPhysicalPlan,
        _control: &dyn PureCompileControl,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        Ok(())
    }
    /// Borrow the original completed required scalar source. This is not an
    /// optional fold, a fragment publication, or statement-success evidence.
    /// Default observers add no work or checkpoints.
    fn observe_required_scalar_source_observed(
        &self,
        _source: &super::SqlAuthoredScalarUnit,
        _control: &dyn PureCompileControl,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        Ok(())
    }
}

/// Short-lived calculator/observer loan. Neither enters IR or published plans.
pub(crate) struct SqlFoldEvaluatorLoan<'a> {
    calculator: &'static dyn SqlConstantEvaluator,
    observer: Option<&'a dyn SqlFoldDependencyObserver>,
    catalog: Option<&'a dyn super::SqlFunctionCatalog>,
}
impl<'a> SqlFoldEvaluatorLoan<'a> {
    pub(crate) fn with_catalog(mut self, catalog: &'a dyn super::SqlFunctionCatalog) -> Self {
        self.catalog = Some(catalog);
        self
    }
    pub(crate) fn new(
        calculator: &'static dyn SqlConstantEvaluator,
        observer: Option<&'a dyn SqlFoldDependencyObserver>,
    ) -> Self {
        Self {
            calculator,
            observer,
            catalog: None,
        }
    }
}
impl SqlConstantEvaluator for SqlFoldEvaluatorLoan<'_> {
    fn admit_fold_parent_observed(
        &self,
        binding: &SqlFunctionBinding,
        lifecycle: novarocks_functions::PureCallLifecycle,
        control: &dyn PureCompileControl,
    ) -> Result<(), novarocks_functions::FunctionBindingError> {
        if let Some(catalog) = self.catalog {
            catalog.admit_bound_lifecycle_observed(binding.resolved(), lifecycle, control)?;
            catalog.admit_authored_environment_observed(binding, control)?;
        }
        self.calculator
            .admit_fold_parent_observed(binding, lifecycle, control)
    }
    fn eval_scalar(
        &self,
        request: &FoldRequest,
        control: &dyn PureCompileControl,
    ) -> Result<Option<ConstantValue>, SqlConstantEvaluationError> {
        self.calculator.eval_scalar(request, control)
    }
    fn before_fold_dependency_observed(
        &self,
        input: SqlFoldDependencyInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        self.calculator
            .before_fold_dependency_observed(input, control)?;
        if let Some(observer) = self.observer {
            observer.before_fold_dependency_observed(input, control)?;
        }
        Ok(())
    }
    fn after_fold_dependency_observed(
        &self,
        input: SqlFoldDependencyInput<'_>,
        outcome: SqlFoldEvaluationOutcome<'_>,
    ) {
        if let Some(observer) = self.observer {
            observer.after_fold_dependency_observed(input, outcome);
        }
        self.calculator
            .after_fold_dependency_observed(input, outcome);
    }
}

/// Preserve the original evaluator result and its exact first failure.
/// Start may refuse before eval; completion only fills the admitted receipt.
pub(crate) fn evaluate_fold_dependency_observed(
    evaluator: &dyn SqlConstantEvaluator,
    input: SqlFoldDependencyInput<'_>,
    control: &dyn PureCompileControl,
) -> Result<Option<ConstantValue>, SqlConstantEvaluationError> {
    evaluator.before_fold_dependency_observed(input, control)?;
    let result = evaluator.eval_scalar(input.request, control);
    let outcome = match &result {
        Ok(Some(value)) => SqlFoldEvaluationOutcome::ProducedConstant(value),
        Ok(None) => SqlFoldEvaluationOutcome::Declined,
        Err(error) => SqlFoldEvaluationOutcome::Error(error),
    };
    evaluator.after_fold_dependency_observed(input, outcome);
    result
}
