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

//! The only author of call control shapes, call claims and per-use effect summaries.
//! SQL lowering, folding and tests all delegate here. Source-journal pairing stays with SQL.

mod controls;
mod effects;
mod error;

#[cfg(test)]
mod tests;

pub use controls::author_call_controls;
pub use effects::author_unit_calls;
pub use error::CallAuthorError;

use crate::EvaluationEnvironment;
use novarocks_constant_contract::ConstantPolicy;
use novarocks_expression_contract::{BoundFunction, ExprId, ExpressionCallClaim};
use novarocks_type_contract::{DecimalOverflowPolicy, ExpressionUseId};
use novarocks_functions::{
    FunctionBindingSelection, FunctionResultType, PureEngineFunctionCatalog,
    ScopedExpressionEffects,
};
use std::{collections::BTreeMap, sync::Arc};

/// Neutral inputs shared by both authorship steps. The decimal map is read only
/// after the unit is sealed; control shapes do not consult it.
pub struct CallAuthorInputs<'a> {
    pub environment: EvaluationEnvironment<'a>,
    pub constant_policy: ConstantPolicy,
    pub functions: &'a PureEngineFunctionCatalog,
    pub decimal_overflow: &'a BTreeMap<ExprId, DecimalOverflowPolicy>,
}

/// Claims consumed by `SealedUnit::finish`, plus the summary of every use.
pub struct UnitCallAuthorship {
    pub claims: BTreeMap<ExpressionUseId, ExpressionCallClaim>,
    pub summaries: BTreeMap<ExpressionUseId, ScopedExpressionEffects>,
}

/// Rebuild the selected signature from the definition the unit already stores.
/// Compilation and authorship share this projection; it does not resolve a name.
pub(crate) fn scalar_binding_selection(function: &BoundFunction) -> Arc<FunctionBindingSelection> {
    Arc::new(FunctionBindingSelection {
        overload: function.overload.clone(),
        argument_types: function.argument_types.clone(),
        result_type: FunctionResultType::Scalar(function.result_type.clone()),
        aggregate: None,
    })
}
