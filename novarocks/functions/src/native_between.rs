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

//! Checked original operation expansion; numeric and Boolean computation
//! remain in the existing comparison and ordered Boolean owners.
use crate::{ComparisonPrepareError, PreparedComparisonRecipe, ScopedExpressionEffects};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, ExpressionEffectContext, FunctionValueType,
    NativeBetweenPlan, PureCompileControl, ValueLogicalType,
};
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedNativeBetweenRecipe {
    plan: NativeBetweenPlan,
    lower: PreparedComparisonRecipe,
    upper: PreparedComparisonRecipe,
}
impl PreparedNativeBetweenRecipe {
    pub fn try_new(
        plan: NativeBetweenPlan,
        operand: &FunctionValueType,
        low: &FunctionValueType,
        high: &FunctionValueType,
        result: &FunctionValueType,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ComparisonPrepareError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let outcome = (|| {
            crate::kernel_input::validate_type_observed(result, &mut work)
                .map_err(ComparisonPrepareError::Kernel)?;
            work.flush()?;
            let lower = PreparedComparisonRecipe::try_new(plan.lower(), operand, low, control)?;
            work.flush()?;
            let upper = PreparedComparisonRecipe::try_new(plan.upper(), operand, high, control)?;
            work.flush()?;
            if *result.data_type() != arrow_schema::DataType::Boolean
                || result.logical_type() != ValueLogicalType::Physical
                || ((lower.nullable_result() || upper.nullable_result()) && !result.nullable())
            {
                return Err(ComparisonPrepareError::TypeMismatch);
            }
            work.step()?;
            Ok(Self { plan, lower, upper })
        })();
        if outcome
            .as_ref()
            .err()
            .is_some_and(|e: &ComparisonPrepareError| e.control_error().is_some())
        {
            return outcome;
        }
        work.finish()?;
        outcome
    }
    pub fn plan(&self) -> NativeBetweenPlan {
        self.plan
    }
    pub fn lower(&self) -> &PreparedComparisonRecipe {
        &self.lower
    }
    pub fn upper(&self) -> &PreparedComparisonRecipe {
        &self.upper
    }
    pub fn own_effects(&self, context: ExpressionEffectContext) -> ScopedExpressionEffects {
        ScopedExpressionEffects::pure_value(context)
    }
}
